//! [`Configuration`]: global settings, with the environment variables and defaults of Crawlee for JS.
//!
//! Values are resolved in this order: values set in code, then `CRAWLEE_*` environment
//! variables, then `./crawlee.json` (camelCase keys, like in JS), then the defaults.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq)]
pub struct Configuration {
    /// `CRAWLEE_STORAGE_DIR`, default `./storage`.
    pub storage_dir: PathBuf,
    /// `CRAWLEE_PERSIST_STORAGE`, default `true`: store data on disk (otherwise in memory).
    pub persist_storage: bool,
    /// `CRAWLEE_PURGE_ON_START`, default `true`: empty the default (and aliased) storages when a
    /// crawler starts.
    pub purge_on_start: bool,
    /// `CRAWLEE_PERSIST_STATE_INTERVAL_MILLIS`, default 60 s.
    pub persist_state_interval: Duration,
    /// Interval of `SystemInfo` events, default 1 s.
    pub system_info_interval: Duration,
    /// `CRAWLEE_AVAILABLE_MEMORY_RATIO`, default 0.25: share of the system memory the crawler may use.
    pub available_memory_ratio: f64,
    /// `CRAWLEE_MEMORY_MBYTES`: fixed memory limit in MiB; overrides `available_memory_ratio`.
    pub memory_mbytes: Option<u64>,
    /// CPU usage ratio above which the CPU counts as overloaded, default 0.95.
    pub max_used_cpu_ratio: f64,
    /// `CRAWLEE_CONTAINERIZED`: whether to read limits from cgroups (auto-detected when unset).
    pub containerized: Option<bool>,
    /// `CRAWLEE_INTERNAL_TIMEOUT` (milliseconds): the timeout of a crawler's own operations
    /// (`internalTimeoutMillis`). When unset, crawlers use twice their request handler timeout,
    /// but at least 5 minutes.
    pub internal_timeout: Option<Duration>,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            storage_dir: PathBuf::from("./storage"),
            persist_storage: true,
            purge_on_start: true,
            persist_state_interval: Duration::from_millis(60_000),
            system_info_interval: Duration::from_millis(1_000),
            available_memory_ratio: 0.25,
            memory_mbytes: None,
            max_used_cpu_ratio: 0.95,
            containerized: None,
            internal_timeout: None,
        }
    }
}

/// The subset of `crawlee.json` Crawlee reads.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct FileConfiguration {
    storage_dir: Option<PathBuf>,
    persist_storage: Option<bool>,
    purge_on_start: Option<bool>,
    persist_state_interval_millis: Option<u64>,
    system_info_interval_millis: Option<u64>,
    available_memory_ratio: Option<f64>,
    memory_mbytes: Option<u64>,
    max_used_cpu_ratio: Option<f64>,
    containerized: Option<bool>,
    internal_timeout_millis: Option<u64>,
}

/// `'0'` and `'false'` (any case) are false; any other non-empty value is true, as in JS.
fn parse_bool(value: &str) -> bool {
    !matches!(value.trim().to_ascii_lowercase().as_str(), "0" | "false")
}

impl Configuration {
    /// Defaults overridden by `./crawlee.json` and then by the environment.
    pub fn from_env() -> Self {
        Self::from_sources(|name| std::env::var(name).ok(), std::fs::read_to_string("crawlee.json").ok())
    }

    /// Defaults overridden by `file` (the text of a `crawlee.json`) and then by the variables
    /// `env` returns. An SDK that adds its own aliases of the `CRAWLEE_*` variables resolves
    /// them in `env`.
    pub fn from_sources(env: impl Fn(&str) -> Option<String>, file: Option<String>) -> Self {
        let mut config = Configuration::default();

        if let Some(file) = file.and_then(|text| serde_json::from_str::<FileConfiguration>(&text).ok()) {
            if let Some(v) = file.storage_dir {
                config.storage_dir = v;
            }
            if let Some(v) = file.persist_storage {
                config.persist_storage = v;
            }
            if let Some(v) = file.purge_on_start {
                config.purge_on_start = v;
            }
            if let Some(v) = file.persist_state_interval_millis {
                config.persist_state_interval = Duration::from_millis(v);
            }
            if let Some(v) = file.system_info_interval_millis {
                config.system_info_interval = Duration::from_millis(v);
            }
            if let Some(v) = file.available_memory_ratio {
                config.available_memory_ratio = v;
            }
            config.memory_mbytes = file.memory_mbytes.or(config.memory_mbytes);
            if let Some(v) = file.max_used_cpu_ratio {
                config.max_used_cpu_ratio = v;
            }
            config.containerized = file.containerized.or(config.containerized);
            if let Some(v) = file.internal_timeout_millis {
                config.internal_timeout = Some(Duration::from_millis(v));
            }
        }

        // Empty environment variables count as unset.
        let var = |name: &str| env(name).filter(|value| !value.trim().is_empty());
        if let Some(v) = var("CRAWLEE_STORAGE_DIR") {
            config.storage_dir = PathBuf::from(v);
        }
        if let Some(v) = var("CRAWLEE_PERSIST_STORAGE") {
            config.persist_storage = parse_bool(&v);
        }
        if let Some(v) = var("CRAWLEE_PURGE_ON_START") {
            config.purge_on_start = parse_bool(&v);
        }
        if let Some(v) = var("CRAWLEE_PERSIST_STATE_INTERVAL_MILLIS").and_then(|v| v.trim().parse().ok()) {
            config.persist_state_interval = Duration::from_millis(v);
        }
        if let Some(v) = var("CRAWLEE_AVAILABLE_MEMORY_RATIO").and_then(|v| v.trim().parse().ok()) {
            config.available_memory_ratio = v;
        }
        // 0 means "not set", as in JS.
        if let Some(v) = var("CRAWLEE_MEMORY_MBYTES").and_then(|v| v.trim().parse::<u64>().ok()) {
            config.memory_mbytes = (v > 0).then_some(v);
        }
        if let Some(v) = var("CRAWLEE_CONTAINERIZED") {
            config.containerized = Some(parse_bool(&v));
        }
        if let Some(v) = var("CRAWLEE_INTERNAL_TIMEOUT").and_then(|v| v.trim().parse().ok()) {
            config.internal_timeout = Some(Duration::from_millis(v));
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precedence_env_over_file_over_defaults() {
        let file = r#"{ "storageDir": "/from/file", "purgeOnStart": false, "memoryMbytes": 512 }"#;
        let env = |name: &str| match name {
            "CRAWLEE_STORAGE_DIR" => Some("/from/env".to_owned()),
            "CRAWLEE_PERSIST_STORAGE" => Some("FALSE".to_owned()),
            "CRAWLEE_MEMORY_MBYTES" => Some(String::new()),
            _ => None,
        };
        let config = Configuration::from_sources(env, Some(file.to_owned()));
        assert_eq!(config.storage_dir, PathBuf::from("/from/env"));
        assert!(!config.persist_storage);
        assert!(!config.purge_on_start, "from crawlee.json");
        assert_eq!(config.memory_mbytes, Some(512), "empty env var is ignored");
        assert_eq!(config.available_memory_ratio, 0.25);
        assert_eq!(config.internal_timeout, None);
    }

    #[test]
    fn internal_timeout() {
        let file = r#"{ "internalTimeoutMillis": 1500 }"#;
        let config = Configuration::from_sources(|_| None, Some(file.to_owned()));
        assert_eq!(config.internal_timeout, Some(Duration::from_millis(1500)));
        let env = |name: &str| (name == "CRAWLEE_INTERNAL_TIMEOUT").then(|| "2500".to_owned());
        let config = Configuration::from_sources(env, Some(file.to_owned()));
        assert_eq!(config.internal_timeout, Some(Duration::from_millis(2500)));
    }
}
