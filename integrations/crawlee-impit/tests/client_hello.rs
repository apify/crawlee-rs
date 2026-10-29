//! Verifies impersonation on the wire: a local TCP listener captures the TLS ClientHello each
//! transport sends and compares cipher suites, extensions and ALPN.
//!
//! This runs against 127.0.0.1 on purpose: remote fingerprint echo services only see the TLS of
//! whatever proxy sits in between, while a local listener sees exactly what the client sent.

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use std::sync::Arc;

use crawlee::Url;
use crawlee::http_client::{
    FingerprintBrowser, HttpRequest, ReqwestTransport, SessionFingerprint, Transport, TransportOptions,
};
use crawlee_impit::{Browser, ImpitTransport};

#[derive(Debug, PartialEq)]
struct ClientHello {
    cipher_suites: Vec<u16>,
    extensions: Vec<u16>,
    alpn: Vec<String>,
    /// `supported_groups`, and the groups of `key_share`, GREASE left out.
    groups: Vec<u16>,
    key_shares: Vec<u16>,
    /// The bytes of the TLS record.
    len: usize,
}

fn is_grease(value: u16) -> bool {
    value & 0x0f0f == 0x0a0a && (value >> 8) == (value & 0xff)
}

fn parse_client_hello(data: &[u8]) -> ClientHello {
    let u16_at = |i: usize| u16::from_be_bytes([data[i], data[i + 1]]);
    assert_eq!(data[0], 0x16, "TLS handshake record");
    assert_eq!(data[5], 0x01, "ClientHello");
    // record header (5) + handshake header (4) + client version (2) + random (32)
    let mut pos = 5 + 4 + 2 + 32;
    pos += 1 + data[pos] as usize; // session id
    let suites_len = u16_at(pos) as usize;
    let cipher_suites = (0..suites_len / 2).map(|i| u16_at(pos + 2 + i * 2)).collect();
    pos += 2 + suites_len;
    pos += 1 + data[pos] as usize; // compression methods
    let extensions_end = pos + 2 + u16_at(pos) as usize;
    pos += 2;

    let mut extensions = Vec::new();
    let mut alpn = Vec::new();
    let (mut groups, mut key_shares) = (Vec::new(), Vec::new());
    while pos + 4 <= extensions_end {
        let (kind, len) = (u16_at(pos), u16_at(pos + 2) as usize);
        if kind == 10 {
            // supported_groups: list length (2), then the groups.
            groups = (0..u16_at(pos + 4) as usize / 2).map(|i| u16_at(pos + 6 + i * 2)).collect();
        }
        if kind == 51 {
            // key_share: list length (2), then group (2) and length-prefixed key of each share.
            let mut p = pos + 6;
            while p < pos + 4 + len {
                key_shares.push(u16_at(p));
                p += 4 + u16_at(p + 2) as usize;
            }
        }
        if kind == 16 {
            // ALPN: list length (2), then length-prefixed protocol names.
            let mut p = pos + 6;
            while p < pos + 4 + len {
                let name_len = data[p] as usize;
                alpn.push(String::from_utf8_lossy(&data[p + 1..p + 1 + name_len]).into_owned());
                p += 1 + name_len;
            }
        }
        extensions.push(kind);
        pos += 4 + len;
    }
    groups.retain(|group| !is_grease(*group));
    key_shares.retain(|group| !is_grease(*group));
    ClientHello { cipher_suites, extensions, alpn, groups, key_shares, len: data.len() }
}

async fn capture(transport: impl Transport + 'static) -> ClientHello {
    capture_with(transport, TransportOptions::default()).await
}

async fn capture_with(transport: impl Transport + 'static, options: TransportOptions) -> ClientHello {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("https://127.0.0.1:{}/", listener.local_addr().unwrap().port())).unwrap();
    let client = tokio::spawn(async move {
        // The handshake cannot complete; only the ClientHello matters.
        let _ = transport.fetch(HttpRequest::get(url), &options).await;
    });

    let (mut socket, _) = listener.accept().await.unwrap();
    let mut data = vec![0u8; 5];
    socket.read_exact(&mut data).await.unwrap();
    let record_len = u16::from_be_bytes([data[3], data[4]]) as usize;
    data.resize(5 + record_len, 0);
    socket.read_exact(&mut data[5..]).await.unwrap();
    drop(socket);
    client.abort();
    parse_client_hello(&data)
}

#[tokio::test]
async fn impit_sends_browser_client_hellos() {
    let reqwest = capture(ReqwestTransport::new()).await;
    let chrome = capture(ImpitTransport::with_browser(Browser::Chrome)).await;
    let firefox = capture(ImpitTransport::with_browser(Browser::Firefox)).await;
    // Extensions that identify the browser; plain rustls sends none of them.
    const RECORD_SIZE_LIMIT: u16 = 28;
    const DELEGATED_CREDENTIALS: u16 = 34;
    const ALPS: [u16; 2] = [17513, 17613];
    const ENCRYPTED_CLIENT_HELLO: u16 = 65037;

    // Chrome: GREASE cipher suite, ALPS and (GREASE) ECH.
    assert!(chrome.cipher_suites.iter().copied().any(is_grease), "{chrome:?}");
    assert!(chrome.extensions.iter().any(|e| ALPS.contains(e)), "{chrome:?}");
    assert!(chrome.extensions.contains(&ENCRYPTED_CLIENT_HELLO), "{chrome:?}");

    // Firefox: no GREASE, but delegated credentials and a record size limit.
    assert!(!firefox.cipher_suites.iter().copied().any(is_grease), "{firefox:?}");
    assert!(firefox.extensions.contains(&DELEGATED_CREDENTIALS), "{firefox:?}");
    assert!(firefox.extensions.contains(&RECORD_SIZE_LIMIT), "{firefox:?}");

    // reqwest (plain rustls) has none of these markers.
    assert!(!reqwest.cipher_suites.iter().copied().any(is_grease), "{reqwest:?}");
    for marker in [RECORD_SIZE_LIMIT, DELEGATED_CREDENTIALS, ENCRYPTED_CLIENT_HELLO, ALPS[0], ALPS[1]] {
        assert!(!reqwest.extensions.contains(&marker), "{reqwest:?}");
    }

    // All of them offer HTTP/2 first.
    for hello in [&chrome, &firefox, &reqwest] {
        assert_eq!(hello.alpn, ["h2", "http/1.1"]);
    }
}

/// Without a browser of its own, the transport impersonates the browser of the session's
/// fingerprint.
#[tokio::test]
async fn the_session_fingerprint_picks_the_browser() {
    const DELEGATED_CREDENTIALS: u16 = 34;
    let firefox = Arc::new(SessionFingerprint { browser: Some(FingerprintBrowser::Firefox), ..Default::default() });
    let options = TransportOptions { fingerprint: Some(firefox), ..Default::default() };
    let hello = capture_with(ImpitTransport::new(), options).await;
    assert!(!hello.cipher_suites.iter().copied().any(is_grease), "{hello:?}");
    assert!(hello.extensions.contains(&DELEGATED_CREDENTIALS), "{hello:?}");
}

/// Chrome offers the post-quantum hybrid X25519MLKEM768 first, with a 1,216-byte key share. Without
/// it (not in Crawlee for JS), the ClientHello offers X25519, P-256 and P-384 with one 32-byte
/// X25519 key share, as got-scraping's Chrome profile does: about 1.2 KB less per connection.
#[tokio::test]
async fn chrome_without_the_post_quantum_key_exchange() {
    const X25519_MLKEM768: u16 = 0x11ec;
    const X25519: u16 = 0x001d;
    const SECP256R1: u16 = 0x0017;
    const SECP384R1: u16 = 0x0018;
    let chrome = capture(ImpitTransport::with_browser(Browser::Chrome)).await;
    assert_eq!(chrome.groups, [X25519_MLKEM768, X25519, SECP256R1, SECP384R1], "{chrome:?}");
    assert_eq!(chrome.key_shares, [X25519_MLKEM768, X25519], "{chrome:?}");

    let classic = capture(ImpitTransport::with_browser(Browser::Chrome).without_post_quantum_key_exchange()).await;
    assert_eq!(classic.groups, [X25519, SECP256R1, SECP384R1], "{classic:?}");
    assert_eq!(classic.key_shares, [X25519], "{classic:?}");
    let sorted = |hello: &ClientHello| {
        let mut extensions: Vec<u16> = hello.extensions.iter().copied().filter(|e| !is_grease(*e)).collect();
        extensions.sort_unstable();
        extensions
    };
    assert_eq!(sorted(&classic), sorted(&chrome), "otherwise Chrome's");
    assert!(chrome.len - classic.len > 1200, "{} -> {} bytes", chrome.len, classic.len);
}
