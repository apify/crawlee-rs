//! Crawl statistics. Field names of [`FinalStatistics`] follow `FinalStatistics` of Crawlee for JS.
//!
//! [`Statistics`] is a [`PersistedState`]: the crawler saves it under
//! `CRAWLEE_CRAWLER_STATISTICS_{id}` in the default key-value store, in the record format of
//! Crawlee for JS, and picks it up again when a crawl resumes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use crawlee_core::recoverable_state::{BoxError, PersistedState};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Live counters, updated concurrently by request tasks.
#[derive(Debug)]
pub struct Statistics {
    id: String,
    times: Mutex<Times>,
    /// The instance start was taken from a saved record, so `start()` keeps it.
    restored: AtomicBool,
    succeeded: AtomicU64,
    failed: AtomicU64,
    retries: AtomicU64,
    skipped: AtomicU64,
    succeeded_millis: AtomicU64,
    failed_millis: AtomicU64,
    min_millis: AtomicU64,
    max_millis: AtomicU64,
    detail: Mutex<Detail>,
}

#[derive(Debug, Default)]
struct Times {
    /// Unix milliseconds the runtime is counted from. A resumed crawl moves it back by the
    /// runtime of the previous runs, so the runtime keeps growing across restarts.
    instance_start_millis: Option<i64>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
struct Detail {
    /// `retry_histogram[n]` = requests that finished after `n` retries.
    retry_histogram: Vec<u64>,
    status_codes: BTreeMap<u16, u64>,
    errors: BTreeMap<String, u64>,
    retry_errors: BTreeMap<String, u64>,
}

impl Default for Statistics {
    fn default() -> Self {
        Statistics::with_id("0")
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Groups errors by their message with digits masked, so `timeout after 1234ms` and
/// `timeout after 99ms` land in the same bucket.
fn error_group(message: &str) -> String {
    let first_line = message.lines().next().unwrap_or_default();
    let mut out = String::with_capacity(first_line.len().min(200));
    let mut last_was_digit = false;
    for c in first_line.chars().take(200) {
        if c.is_ascii_digit() {
            if !last_was_digit {
                out.push('_');
            }
            last_was_digit = true;
        } else {
            out.push(c);
            last_was_digit = false;
        }
    }
    out
}

/// The saved record, with the fields in the order Crawlee for JS writes them. Values that are
/// `Infinity` in JS until something finished are `null`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatisticsRecord {
    requests_succeeded: u64,
    requests_failed: u64,
    requests_retries: u64,
    requests_failed_per_minute: Option<u64>,
    requests_succeeded_per_minute: Option<u64>,
    request_min_duration_millis: Option<u64>,
    request_max_duration_millis: u64,
    request_total_failed_duration_millis: u64,
    request_total_succeeded_duration_millis: u64,
    crawler_started_at: Option<String>,
    crawler_finished_at: Option<String>,
    stats_persisted_at: String,
    crawler_runtime_millis: u64,
    crawler_last_start_timestamp: i64,
    request_retry_histogram: Vec<Option<u64>>,
    stats_id: String,
    request_avg_failed_duration_millis: Option<u64>,
    request_avg_succeeded_duration_millis: Option<u64>,
    request_total_duration_millis: u64,
    requests_total: u64,
    requests_with_status_code: BTreeMap<String, u64>,
    errors: Value,
    retry_errors: Value,
}

fn parse_time(value: Option<&str>) -> Result<Option<DateTime<Utc>>, BoxError> {
    let time = value.map(DateTime::parse_from_rfc3339).transpose()?;
    Ok(time.map(|time| time.with_timezone(&Utc)))
}

impl Statistics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Statistics saved under `CRAWLEE_CRAWLER_STATISTICS_{id}`.
    pub fn with_id(id: impl Into<String>) -> Self {
        Statistics {
            id: id.into(),
            times: Mutex::new(Times::default()),
            restored: AtomicBool::new(false),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            succeeded_millis: AtomicU64::new(0),
            failed_millis: AtomicU64::new(0),
            min_millis: AtomicU64::new(u64::MAX),
            max_millis: AtomicU64::new(0),
            detail: Mutex::new(Detail::default()),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The key the statistics are saved under.
    pub fn persist_state_key(&self) -> String {
        format!("CRAWLEE_CRAWLER_STATISTICS_{}", self.id)
    }

    pub fn start(&self) {
        let now = Utc::now();
        let mut times = self.times.lock();
        if !self.restored.load(Ordering::Acquire) || times.instance_start_millis.is_none() {
            times.instance_start_millis = Some(now.timestamp_millis());
        }
        // A restored record keeps the start of the run it belongs to.
        times.started_at.get_or_insert(now);
        times.finished_at = None;
    }

    pub fn finish(&self) {
        self.times.lock().finished_at = Some(Utc::now());
    }
    pub fn record_success(&self, duration: Duration, retry_count: u32) {
        self.succeeded.fetch_add(1, Ordering::Relaxed);
        self.record_duration(duration, &self.succeeded_millis);
        self.record_retries(retry_count);
    }

    pub fn record_failure(&self, duration: Duration, retry_count: u32) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.record_duration(duration, &self.failed_millis);
        self.record_retries(retry_count);
    }

    pub fn record_skipped(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self, message: &str) {
        self.retries.fetch_add(1, Ordering::Relaxed);
        *self.detail.lock().retry_errors.entry(error_group(message)).or_default() += 1;
    }

    pub fn record_error(&self, message: &str) {
        *self.detail.lock().errors.entry(error_group(message)).or_default() += 1;
    }

    pub fn record_status_code(&self, status: u16) {
        *self.detail.lock().status_codes.entry(status).or_default() += 1;
    }

    fn record_duration(&self, duration: Duration, total: &AtomicU64) {
        let ms = millis(duration);
        total.fetch_add(ms, Ordering::Relaxed);
        self.min_millis.fetch_min(ms, Ordering::Relaxed);
        self.max_millis.fetch_max(ms, Ordering::Relaxed);
    }

    fn record_retries(&self, retry_count: u32) {
        let mut detail = self.detail.lock();
        let index = retry_count as usize;
        if detail.retry_histogram.len() <= index {
            detail.retry_histogram.resize(index + 1, 0);
        }
        detail.retry_histogram[index] += 1;
    }

    pub fn requests_succeeded(&self) -> u64 {
        self.succeeded.load(Ordering::Relaxed)
    }

    pub fn requests_failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// Requests finished either way (skipped ones included).
    pub fn requests_finished(&self) -> u64 {
        self.requests_succeeded() + self.requests_failed() + self.skipped.load(Ordering::Relaxed)
    }

    /// Runtime so far: until `finish()`, or until now while running. It spans every run of a
    /// resumed crawl.
    fn runtime_millis(&self) -> u64 {
        let times = self.times.lock();
        let Some(start) = times.instance_start_millis else {
            return 0;
        };
        let end = times.finished_at.unwrap_or_else(Utc::now).timestamp_millis();
        u64::try_from(end - start).unwrap_or(0)
    }

    pub fn snapshot(&self) -> FinalStatistics {
        let succeeded = self.requests_succeeded();
        let failed = self.requests_failed();
        let runtime_millis = self.runtime_millis();
        let minutes = runtime_millis as f64 / 60_000.0;
        // As in JS: successes per minute are rounded, failures floored.
        let per_minute = |count: u64, round: fn(f64) -> f64| {
            if minutes > 0.0 { round(count as f64 / minutes) as u64 } else { 0 }
        };
        let avg = |total: &AtomicU64, count: u64| {
            if count > 0 { (total.load(Ordering::Relaxed) as f64 / count as f64).round() as u64 } else { 0 }
        };
        let detail = self.detail.lock();

        FinalStatistics {
            requests_succeeded: succeeded,
            requests_failed: failed,
            requests_skipped: self.skipped.load(Ordering::Relaxed),
            requests_total: succeeded + failed,
            requests_retries: self.retries.load(Ordering::Relaxed),
            request_avg_succeeded_duration_millis: avg(&self.succeeded_millis, succeeded),
            request_avg_failed_duration_millis: avg(&self.failed_millis, failed),
            request_min_duration_millis: match self.min_millis.load(Ordering::Relaxed) {
                u64::MAX => 0,
                min => min,
            },
            request_max_duration_millis: self.max_millis.load(Ordering::Relaxed),
            requests_succeeded_per_minute: per_minute(succeeded, f64::round),
            requests_failed_per_minute: per_minute(failed, f64::floor),
            request_total_duration_millis: self.succeeded_millis.load(Ordering::Relaxed)
                + self.failed_millis.load(Ordering::Relaxed),
            crawler_runtime_millis: runtime_millis,
            retry_histogram: detail.retry_histogram.clone(),
            status_codes: detail.status_codes.clone(),
            errors: detail.errors.clone(),
            retry_errors: detail.retry_errors.clone(),
        }
    }

    fn record(&self) -> StatisticsRecord {
        let stats = self.snapshot();
        let times = self.times.lock();
        let non_zero = |value: u64| (value > 0).then_some(value);
        StatisticsRecord {
            requests_succeeded: stats.requests_succeeded,
            requests_failed: stats.requests_failed,
            requests_retries: stats.requests_retries,
            requests_failed_per_minute: Some(stats.requests_failed_per_minute),
            requests_succeeded_per_minute: Some(stats.requests_succeeded_per_minute),
            request_min_duration_millis: (stats.requests_total > 0).then_some(stats.request_min_duration_millis),
            request_max_duration_millis: stats.request_max_duration_millis,
            request_total_failed_duration_millis: self.failed_millis.load(Ordering::Relaxed),
            request_total_succeeded_duration_millis: self.succeeded_millis.load(Ordering::Relaxed),
            crawler_started_at: times.started_at.map(iso),
            crawler_finished_at: times.finished_at.map(iso),
            stats_persisted_at: iso(Utc::now()),
            crawler_runtime_millis: stats.crawler_runtime_millis,
            crawler_last_start_timestamp: times.instance_start_millis.unwrap_or_else(|| Utc::now().timestamp_millis()),
            request_retry_histogram: stats.retry_histogram.iter().map(|&count| Some(count)).collect(),
            stats_id: self.id.clone(),
            // JS computes `Math.round(total / count) || Infinity`, so a zero average is `null` too.
            request_avg_failed_duration_millis: non_zero(stats.request_avg_failed_duration_millis),
            request_avg_succeeded_duration_millis: non_zero(stats.request_avg_succeeded_duration_millis),
            request_total_duration_millis: stats.request_total_duration_millis,
            requests_total: stats.requests_total,
            requests_with_status_code: stats.status_codes.iter().map(|(code, n)| (code.to_string(), *n)).collect(),
            errors: serde_json::to_value(&stats.errors).unwrap_or_default(),
            retry_errors: serde_json::to_value(&stats.retry_errors).unwrap_or_default(),
        }
    }
}

impl Statistics {
    fn restore_record(&self, record: Value) -> Result<(), BoxError> {
        let record: StatisticsRecord = serde_json::from_value(record)?;
        let started_at = parse_time(record.crawler_started_at.as_deref())?;
        let finished_at = parse_time(record.crawler_finished_at.as_deref())?;
        let persisted_at = DateTime::parse_from_rfc3339(&record.stats_persisted_at)?.timestamp_millis();
        let status_codes =
            record.requests_with_status_code.iter().filter_map(|(code, n)| Some((code.parse().ok()?, *n))).collect();

        self.succeeded.store(record.requests_succeeded, Ordering::Relaxed);
        self.failed.store(record.requests_failed, Ordering::Relaxed);
        self.retries.store(record.requests_retries, Ordering::Relaxed);
        self.succeeded_millis.store(record.request_total_succeeded_duration_millis, Ordering::Relaxed);
        self.failed_millis.store(record.request_total_failed_duration_millis, Ordering::Relaxed);
        self.min_millis.store(record.request_min_duration_millis.unwrap_or(u64::MAX), Ordering::Relaxed);
        self.max_millis.store(record.request_max_duration_millis, Ordering::Relaxed);
        *self.detail.lock() = Detail {
            retry_histogram: record.request_retry_histogram.iter().map(|count| count.unwrap_or(0)).collect(),
            status_codes,
            errors: serde_json::from_value(record.errors).unwrap_or_default(),
            retry_errors: serde_json::from_value(record.retry_errors).unwrap_or_default(),
        };
        let mut times = self.times.lock();
        // Rebased so the runtime continues from where the saved run was.
        times.instance_start_millis =
            Some(Utc::now().timestamp_millis() - (persisted_at - record.crawler_last_start_timestamp));
        times.started_at = started_at;
        times.finished_at = finished_at;
        self.restored.store(true, Ordering::Release);
        Ok(())
    }
}

impl PersistedState for Statistics {
    fn to_record(&self) -> Value {
        serde_json::to_value(self.record()).unwrap_or_default()
    }

    /// Restores a record written by crawlee-rs or Crawlee for JS. A record missing any field is
    /// discarded whole with a warning, starting from scratch, as statistics are bookkeeping and not
    /// worth failing a crawl over (JS `#deserializeState`). Error groups are kept only if they have
    /// the flat shape crawlee-rs writes (JS groups errors differently).
    fn restore(&self, record: Value) -> Result<(), BoxError> {
        if let Err(err) = self.restore_record(record) {
            tracing::warn!(
                persist_state_key = %self.persist_state_key(),
                "Received invalid state from Key-value store, starting the statistics from scratch: {err}"
            );
            self.reset();
        }
        Ok(())
    }

    fn reset(&self) {
        for counter in [
            &self.succeeded,
            &self.failed,
            &self.retries,
            &self.skipped,
            &self.succeeded_millis,
            &self.failed_millis,
            &self.max_millis,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        self.min_millis.store(u64::MAX, Ordering::Relaxed);
        *self.detail.lock() = Detail::default();
        *self.times.lock() = Times::default();
        self.restored.store(false, Ordering::Release);
    }
}

/// Statistics returned by `run()`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FinalStatistics {
    pub requests_succeeded: u64,
    pub requests_failed: u64,
    pub requests_skipped: u64,
    pub requests_total: u64,
    pub requests_retries: u64,
    pub request_avg_succeeded_duration_millis: u64,
    pub request_avg_failed_duration_millis: u64,
    pub request_min_duration_millis: u64,
    pub request_max_duration_millis: u64,
    pub requests_succeeded_per_minute: u64,
    pub requests_failed_per_minute: u64,
    pub request_total_duration_millis: u64,
    pub crawler_runtime_millis: u64,
    pub retry_histogram: Vec<u64>,
    pub status_codes: BTreeMap<u16, u64>,
    pub errors: BTreeMap<String, u64>,
    pub retry_errors: BTreeMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_groups() {
        let stats = Statistics::new();
        stats.start();
        stats.record_success(Duration::from_millis(100), 0);
        stats.record_success(Duration::from_millis(300), 2);
        stats.record_failure(Duration::from_millis(50), 3);
        stats.record_error("timed out after 1234 ms");
        stats.record_error("timed out after 99 ms");
        stats.record_status_code(404);
        stats.finish();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.requests_succeeded, 2);
        assert_eq!(snapshot.requests_total, 3);
        assert_eq!(snapshot.request_avg_succeeded_duration_millis, 200);
        assert_eq!(snapshot.request_min_duration_millis, 50);
        assert_eq!(snapshot.retry_histogram, vec![1, 0, 1, 1]);
        assert_eq!(snapshot.errors.get("timed out after _ ms"), Some(&2));
        assert_eq!(snapshot.status_codes.get(&404), Some(&1));
    }

    #[test]
    fn record_has_the_js_shape_and_round_trips() {
        let stats = Statistics::with_id("7");
        stats.start();
        stats.record_success(Duration::from_millis(100), 0);
        stats.record_failure(Duration::from_millis(50), 2);
        stats.record_retry("boom 1");
        stats.record_status_code(200);

        let record = stats.to_record();
        let keys: Vec<&str> = record.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "requestsSucceeded",
                "requestsFailed",
                "requestsRetries",
                "requestsFailedPerMinute",
                "requestsSucceededPerMinute",
                "requestMinDurationMillis",
                "requestMaxDurationMillis",
                "requestTotalFailedDurationMillis",
                "requestTotalSucceededDurationMillis",
                "crawlerStartedAt",
                "crawlerFinishedAt",
                "statsPersistedAt",
                "crawlerRuntimeMillis",
                "crawlerLastStartTimestamp",
                "requestRetryHistogram",
                "statsId",
                "requestAvgFailedDurationMillis",
                "requestAvgSucceededDurationMillis",
                "requestTotalDurationMillis",
                "requestsTotal",
                "requestsWithStatusCode",
                "errors",
                "retryErrors",
            ]
        );
        assert_eq!(record["statsId"], "7");
        assert_eq!(record["requestRetryHistogram"], serde_json::json!([1, 0, 1]));
        assert_eq!(record["requestsWithStatusCode"], serde_json::json!({ "200": 1 }));
        assert!(record["crawlerFinishedAt"].is_null());

        let restored = Statistics::with_id("7");
        restored.restore(record).unwrap();
        restored.start();
        let snapshot = restored.snapshot();
        assert_eq!((snapshot.requests_succeeded, snapshot.requests_failed, snapshot.requests_retries), (1, 1, 1));
        assert_eq!(snapshot.request_min_duration_millis, 50);
        assert_eq!(snapshot.retry_errors.get("boom _"), Some(&1));
        assert_eq!(snapshot.status_codes.get(&200), Some(&1));

        // A record missing a field is not ours: discarded whole, starting from scratch rather than
        // failing the crawl.
        let mut broken = stats.to_record();
        broken.as_object_mut().unwrap().remove("requestsTotal");
        let fresh = Statistics::new();
        fresh.restore(broken).unwrap();
        assert_eq!(fresh.snapshot().requests_succeeded, 0);
    }

    #[test]
    fn empty_record_uses_nulls_for_infinity() {
        let record = Statistics::new().to_record();
        assert!(record["requestMinDurationMillis"].is_null());
        assert!(record["requestAvgSucceededDurationMillis"].is_null());
        assert!(record["crawlerStartedAt"].is_null());
    }
}
