//! Verifies impersonation on the wire: a local TCP listener captures the TLS ClientHello each
//! transport sends and compares cipher suites, extensions and ALPN.
//!
//! This runs against 127.0.0.1 on purpose: remote fingerprint echo services only see the TLS of
//! whatever proxy sits in between, while a local listener sees exactly what the client sent.

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use crawlee::Url;
use crawlee::http_client::{HttpRequest, ReqwestTransport, Transport, TransportOptions};
use crawlee_impit::{Browser, ImpitTransport};

#[derive(Debug, PartialEq)]
struct ClientHello {
    cipher_suites: Vec<u16>,
    extensions: Vec<u16>,
    alpn: Vec<String>,
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
    while pos + 4 <= extensions_end {
        let (kind, len) = (u16_at(pos), u16_at(pos + 2) as usize);
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
    ClientHello { cipher_suites, extensions, alpn }
}

async fn capture(transport: impl Transport + 'static) -> ClientHello {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("https://127.0.0.1:{}/", listener.local_addr().unwrap().port())).unwrap();
    let client = tokio::task::spawn_local(async move {
        // The handshake cannot complete; only the ClientHello matters.
        let _ = transport.fetch(HttpRequest::get(url), &TransportOptions::default()).await;
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

#[tokio::test(flavor = "local")]
async fn impit_sends_browser_client_hellos() {
    let reqwest = capture(ReqwestTransport::new()).await;
    let chrome = capture(ImpitTransport::new(Browser::Chrome)).await;
    let firefox = capture(ImpitTransport::new(Browser::Firefox)).await;
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
