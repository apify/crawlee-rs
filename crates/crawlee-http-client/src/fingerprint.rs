//! [`SessionFingerprint`]: the browser-like profile a session impersonates.

use serde_json::{Map, Value};

/// Browser family. Clients that impersonate browsers (impit) pick their TLS profile from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FingerprintBrowser {
    Chrome,
    Firefox,
    Safari,
    Edge,
}

/// Platform hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FingerprintPlatform {
    Windows,
    Macos,
    Linux,
    Android,
    Ios,
}

/// Device class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FingerprintDevice {
    Desktop,
    Mobile,
}

macro_rules! names {
    ($ty:ty { $($variant:ident => $name:literal),* $(,)? }) => {
        impl $ty {
            /// The name used in the saved session state, as in Crawlee for JS.
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $name),* }
            }

            pub fn from_name(name: &str) -> Option<Self> {
                match name { $($name => Some(Self::$variant),)* _ => None }
            }
        }
    };
}

names!(FingerprintBrowser { Chrome => "chrome", Firefox => "firefox", Safari => "safari", Edge => "edge" });
names!(FingerprintPlatform {
    Windows => "windows",
    Macos => "macos",
    Linux => "linux",
    Android => "android",
    Ios => "ios",
});
names!(FingerprintDevice { Desktop => "desktop", Mobile => "mobile" });

/// Identifies the browser-like profile a session impersonates, so that its requests look
/// consistent (`SessionFingerprint` in Crawlee for JS).
///
/// The fields are hints: each HTTP client applies what it can. The impit transport impersonates
/// the browser; the reqwest transport ignores the fingerprint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SessionFingerprint {
    pub browser: Option<FingerprintBrowser>,
    pub platform: Option<FingerprintPlatform>,
    pub device: Option<FingerprintDevice>,
}

impl SessionFingerprint {
    /// The fingerprint as JSON, without the fields that are not set.
    pub fn to_json(&self) -> Value {
        let mut map = Map::new();
        if let Some(browser) = self.browser {
            map.insert("browser".into(), browser.as_str().into());
        }
        if let Some(platform) = self.platform {
            map.insert("platform".into(), platform.as_str().into());
        }
        if let Some(device) = self.device {
            map.insert("device".into(), device.as_str().into());
        }
        Value::Object(map)
    }

    /// A fingerprint from [`to_json`](Self::to_json). `None` when `value` is not an object; fields
    /// with unknown values are left unset.
    pub fn from_json(value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        let field = |name: &str| object.get(name).and_then(Value::as_str);
        Some(SessionFingerprint {
            browser: field("browser").and_then(FingerprintBrowser::from_name),
            platform: field("platform").and_then(FingerprintPlatform::from_name),
            device: field("device").and_then(FingerprintDevice::from_name),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_round_trip() {
        let fingerprint = SessionFingerprint {
            browser: Some(FingerprintBrowser::Edge),
            platform: Some(FingerprintPlatform::Macos),
            device: None,
        };
        assert_eq!(fingerprint.to_json(), json!({ "browser": "edge", "platform": "macos" }));
        assert_eq!(SessionFingerprint::from_json(&fingerprint.to_json()), Some(fingerprint));
        assert_eq!(
            SessionFingerprint::from_json(&json!({ "browser": "opera", "device": "mobile" })),
            Some(SessionFingerprint { device: Some(FingerprintDevice::Mobile), ..Default::default() })
        );
        assert_eq!(SessionFingerprint::from_json(&Value::Null), None);
    }
}
