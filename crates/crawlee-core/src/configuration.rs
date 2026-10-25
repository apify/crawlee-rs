//! [`Configuration`]: global settings, with the environment variables and defaults of Crawlee for JS.
//!
//! Values are resolved in this order: values set in code, then `CRAWLEE_*` environment
//! variables, then `./crawlee.json` (camelCase keys, like in JS), then the defaults.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::storage::dataset_buffer::DatasetBufferOptions;

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
    /// Buffered dataset writes, an extra of crawlee-rs: `None` (the default) pushes every item
    /// right away, as Crawlee for JS does. Datasets opened through [`Services`](crate::Services)
    /// collect pushes and store them together, as the options say (see
    /// [`dataset_buffer`](crate::storage::dataset_buffer)).
    ///
    /// `CRAWLEE_DATASET_BUFFER` (`1`/`true` enables, `0`/`false` disables) turns it on with the
    /// defaults, which `CRAWLEE_DATASET_BUFFER_STAGES` (`FROM_ITEM:LINGER,...`, such as
    /// `10:1s,100:2s`) and `CRAWLEE_DATASET_BUFFER_MAX_BYTES` change; in `crawlee.json`,
    /// `datasetBuffer`, `datasetBufferStages` and `datasetBufferMaxBytes`.
    pub dataset_buffer: Option<DatasetBufferOptions>,
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
            dataset_buffer: None,
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
    dataset_buffer: Option<bool>,
    dataset_buffer_stages: Option<String>,
    dataset_buffer_max_bytes: Option<usize>,
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
        // The settings of buffered dataset writes, applied once it is known whether they are on.
        let mut buffer_enabled = None;
        let mut buffer_stages = None;
        let mut buffer_max_bytes = None;

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
            buffer_enabled = file.dataset_buffer;
            buffer_stages = file.dataset_buffer_stages;
            buffer_max_bytes = file.dataset_buffer_max_bytes;
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
        if let Some(v) = var("CRAWLEE_DATASET_BUFFER") {
            buffer_enabled = Some(parse_bool(&v));
        }
        buffer_stages = var("CRAWLEE_DATASET_BUFFER_STAGES").or(buffer_stages);
        if let Some(v) = var("CRAWLEE_DATASET_BUFFER_MAX_BYTES").and_then(|v| v.trim().parse().ok()) {
            buffer_max_bytes = Some(v);
        }
        if buffer_enabled == Some(true) {
            let mut options = DatasetBufferOptions::default();
            match buffer_stages.as_deref().map(DatasetBufferOptions::parse_stages) {
                Some(Ok(stages)) => options = options.with_stages(stages),
                Some(Err(err)) => tracing::warn!("{err}; the default stages are used"),
                None => {}
            }
            if let Some(max_bytes) = buffer_max_bytes {
                options = options.with_max_bytes(max_bytes);
            }
            config.dataset_buffer = Some(options);
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
        assert_eq!(config.dataset_buffer, None, "buffered dataset writes are off by default");
    }

    #[test]
    fn dataset_buffer_from_the_environment_and_crawlee_json() {
        use crate::storage::dataset_buffer::BufferStage;

        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |name: &str| vars.iter().find(|(n, _)| *n == name).map(|(_, v)| (*v).to_owned())
        };
        let config = Configuration::from_sources(env(&[("CRAWLEE_DATASET_BUFFER", "1")]), None);
        assert_eq!(config.dataset_buffer, Some(DatasetBufferOptions::default()));

        let config = Configuration::from_sources(
            env(&[
                ("CRAWLEE_DATASET_BUFFER", "true"),
                ("CRAWLEE_DATASET_BUFFER_STAGES", "20:500ms,200:3s,1000:5"),
                ("CRAWLEE_DATASET_BUFFER_MAX_BYTES", "2000000"),
            ]),
            None,
        );
        let options = config.dataset_buffer.unwrap();
        assert_eq!(
            options.stages,
            [
                BufferStage::new(20, Duration::from_millis(500)),
                BufferStage::new(200, Duration::from_secs(3)),
                BufferStage::new(1000, Duration::from_secs(5)),
            ]
        );
        assert_eq!(options.max_bytes, 2_000_000);

        // Tuning alone does not turn it on, and an invalid list keeps the default stages.
        let config = Configuration::from_sources(env(&[("CRAWLEE_DATASET_BUFFER_STAGES", "1:1s")]), None);
        assert_eq!(config.dataset_buffer, None);
        let config = Configuration::from_sources(
            env(&[("CRAWLEE_DATASET_BUFFER", "1"), ("CRAWLEE_DATASET_BUFFER_STAGES", "soon")]),
            None,
        );
        assert_eq!(config.dataset_buffer, Some(DatasetBufferOptions::default()));

        let file = r#"{ "datasetBuffer": true, "datasetBufferStages": "5:2s", "datasetBufferMaxBytes": 1000 }"#;
        let config = Configuration::from_sources(|_| None, Some(file.to_owned()));
        let options = config.dataset_buffer.unwrap();
        assert_eq!((options.stages, options.max_bytes), (vec![BufferStage::new(5, Duration::from_secs(2))], 1000));
        // The environment wins over the file.
        let config = Configuration::from_sources(env(&[("CRAWLEE_DATASET_BUFFER", "0")]), Some(file.to_owned()));
        assert_eq!(config.dataset_buffer, None);
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
