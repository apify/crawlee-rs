//! Response bodies: bytes plus the encoding they are in, decoded lazily and without copying when
//! they already are valid UTF-8 (the common case).

use std::cell::OnceCell;

use bytes::Bytes;
use encoding_rs::{Encoding, UTF_8};

/// A response body.
#[derive(Debug)]
pub struct Body {
    bytes: Bytes,
    encoding: &'static Encoding,
    /// `None`: the bytes (minus a UTF-8 BOM) are valid UTF-8 and are borrowed as they are.
    decoded: OnceCell<Option<String>>,
}

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

impl Body {
    pub fn new(bytes: Bytes, encoding: &'static Encoding) -> Self {
        Body { bytes, encoding, decoded: OnceCell::new() }
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    fn utf8_payload(&self) -> &[u8] {
        self.bytes.strip_prefix(UTF8_BOM).unwrap_or(&self.bytes)
    }

    /// The body as text. Decoded once; valid UTF-8 is borrowed from the bytes without a copy.
    pub fn text(&self) -> &str {
        let decoded = self.decoded.get_or_init(|| {
            if self.encoding == UTF_8 && simdutf8::basic::from_utf8(self.utf8_payload()).is_ok() {
                None
            } else {
                // `decode` sniffs a BOM first and replaces malformed sequences with U+FFFD.
                Some(self.encoding.decode(&self.bytes).0.into_owned())
            }
        });
        match decoded {
            Some(text) => text,
            None => simdutf8::basic::from_utf8(self.utf8_payload()).expect("validated when first decoded"),
        }
    }
}

/// Resolves an encoding label with the WHATWG Encoding Standard rules (the ones browsers use).
///
/// Crawlee for JS resolves labels with `iconv-lite`, which differs for a few labels: most notably,
/// `latin1` / `iso-8859-1` mean windows-1252 here, as in browsers.
pub fn encoding_for_label(label: &str) -> Option<&'static Encoding> {
    Encoding::for_label(label.trim().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_is_borrowed_and_bom_stripped() {
        let body = Body::new(Bytes::from_static("\u{FEFF}héllo".as_bytes()), UTF_8);
        assert_eq!(body.text(), "héllo");
        assert!(body.decoded.get().unwrap().is_none(), "no copy for valid UTF-8");
    }

    #[test]
    fn legacy_encodings_are_decoded() {
        let win1250 = encoding_for_label("windows-1250").unwrap();
        let (bytes, _, _) = win1250.encode("Příliš žluťoučký kůň");
        let body = Body::new(Bytes::from(bytes.into_owned()), win1250);
        assert_eq!(body.text(), "Příliš žluťoučký kůň");
    }

    #[test]
    fn invalid_utf8_is_replaced() {
        let body = Body::new(Bytes::from_static(b"ok \xFF end"), UTF_8);
        assert_eq!(body.text(), "ok \u{FFFD} end");
    }
}
