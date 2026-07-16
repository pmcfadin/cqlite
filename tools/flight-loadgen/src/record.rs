//! Per-step aggregation and the `flight-loadgen.step/v1` JSONL record
//! (spec: JSONL requirement; design §JSONL record schema).
//!
//! [`StepAgg`] accumulates one step's outcomes under a memory bound — per-class
//! counts, running row/byte totals, and a single fixed-footprint
//! [`hdrhistogram::Histogram`] over the latencies of `ok` requests only (reset
//! per step, so memory is O(histogram buckets), never O(requests)). At step end
//! it renders a [`StepRecord`], which serialises to exactly one JSONL line.

use std::collections::BTreeMap;

use hdrhistogram::Histogram;
use serde::Serialize;

use crate::classify::Outcome;

/// JSONL schema tag emitted on every record. Bump when the shape changes.
pub const SCHEMA_TAG: &str = "flight-loadgen.step/v1";

/// A memory-bounded accumulator for one ramp step's request outcomes.
///
/// Latencies are recorded in **microseconds** into an auto-resizing 3-sig-fig
/// histogram and reported in **milliseconds** in the record. The histogram is
/// per step (a fresh `StepAgg` per step), so memory never grows with request
/// volume — mirroring the drain-don't-accumulate rule the program enforces.
pub struct StepAgg {
    ok: u64,
    unavailable: u64,
    error: u64,
    error_codes: BTreeMap<String, u64>,
    rows_total: u64,
    bytes_total: u64,
    /// Latencies (microseconds) of `ok` requests only.
    latency_us: Histogram<u64>,
}

impl StepAgg {
    /// A fresh accumulator with an empty auto-resizing latency histogram.
    pub fn new() -> Self {
        Self {
            ok: 0,
            unavailable: 0,
            error: 0,
            error_codes: BTreeMap::new(),
            rows_total: 0,
            bytes_total: 0,
            // 3 significant figures, auto-resizing so the caller never has to
            // pick a max latency up front. Recording never fails for a resizing
            // histogram, so any error here is a construction-time bug.
            latency_us: Histogram::new(3).expect("valid histogram sigfig"),
        }
    }

    /// Record a successful request: its latency, drained row count, and drained
    /// byte count. Only `ok` latencies enter the percentile histogram.
    pub fn record_ok(&mut self, latency_us: u64, rows: u64, bytes: u64) {
        self.ok += 1;
        self.rows_total = self.rows_total.saturating_add(rows);
        self.bytes_total = self.bytes_total.saturating_add(bytes);
        // Auto-resizing histogram: `record` only errs if the value exceeds the
        // (auto-grown) range, which cannot happen here — saturate defensively.
        let _ = self.latency_us.record(latency_us);
    }

    /// Record a classified non-ok outcome. `Outcome::Ok` must go through
    /// [`Self::record_ok`] (it carries latency/rows/bytes); passing it here is a
    /// caller bug and is ignored.
    pub fn record_outcome(&mut self, outcome: &Outcome) {
        match outcome {
            Outcome::Ok => {}
            Outcome::Unavailable => self.unavailable += 1,
            Outcome::Error(code) => {
                self.error += 1;
                *self.error_codes.entry(code.clone()).or_insert(0) += 1;
            }
        }
    }

    /// Fold another accumulator (a per-worker partial) into this one.
    pub fn merge(&mut self, other: &StepAgg) {
        self.ok += other.ok;
        self.unavailable += other.unavailable;
        self.error += other.error;
        self.rows_total = self.rows_total.saturating_add(other.rows_total);
        self.bytes_total = self.bytes_total.saturating_add(other.bytes_total);
        for (code, n) in &other.error_codes {
            *self.error_codes.entry(code.clone()).or_insert(0) += n;
        }
        self.latency_us
            .add(&other.latency_us)
            .expect("compatible histograms (same sigfig, auto-resizing)");
    }

    /// Total classified requests (ok + unavailable + error).
    pub fn total(&self) -> u64 {
        self.ok + self.unavailable + self.error
    }

    /// Successful-request count.
    pub fn ok(&self) -> u64 {
        self.ok
    }

    /// Render the step record. `duration_s` is the measured elapsed wall time of
    /// the step (bounds the sampling window; never perturbs which data was
    /// requested). `qps`/`rows_per_s`/`bytes_per_s` are computed against it;
    /// latency percentiles are over `ok` requests only.
    #[allow(clippy::too_many_arguments)]
    pub fn into_record(
        self,
        round: String,
        endpoint: String,
        ts_unix_ms: u64,
        seed: u64,
        step: usize,
        target_concurrency: usize,
        shape: String,
        duration_s: f64,
    ) -> StepRecord {
        let per_s = |n: u64| {
            if duration_s > 0.0 {
                n as f64 / duration_s
            } else {
                0.0
            }
        };
        let us_to_ms = |us: u64| us as f64 / 1000.0;
        let latency = LatencyMs {
            p50: us_to_ms(self.latency_us.value_at_quantile(0.50)),
            p95: us_to_ms(self.latency_us.value_at_quantile(0.95)),
            p99: us_to_ms(self.latency_us.value_at_quantile(0.99)),
            max: us_to_ms(self.latency_us.max()),
            samples: self.latency_us.len(),
        };
        StepRecord {
            schema: SCHEMA_TAG,
            round,
            endpoint,
            ts_unix_ms,
            seed,
            step,
            target_concurrency,
            shape,
            duration_s,
            requests_ok: self.ok,
            requests_unavailable: self.unavailable,
            requests_error: self.error,
            error_codes: self.error_codes,
            qps: per_s(self.ok),
            rows_per_s: per_s(self.rows_total),
            bytes_per_s: per_s(self.bytes_total),
            rows_total: self.rows_total,
            bytes_total: self.bytes_total,
            latency_ms: latency,
        }
    }
}

impl Default for StepAgg {
    fn default() -> Self {
        Self::new()
    }
}

/// Latency percentiles for a step, in milliseconds, over `ok` requests only.
#[derive(Debug, Clone, Serialize)]
pub struct LatencyMs {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    /// Number of `ok` latency samples the percentiles are computed from.
    pub samples: u64,
}

/// One `flight-loadgen.step/v1` JSONL record — one object per ramp step.
///
/// `qps = requests_ok / duration_s`; latency percentiles are over the step's
/// `ok` requests only. Cross-node distribution is N/A server-direct (a single
/// endpoint) and is intentionally absent rather than fabricated.
#[derive(Debug, Clone, Serialize)]
pub struct StepRecord {
    pub schema: &'static str,
    pub round: String,
    pub endpoint: String,
    pub ts_unix_ms: u64,
    pub seed: u64,
    pub step: usize,
    pub target_concurrency: usize,
    pub shape: String,
    pub duration_s: f64,
    pub requests_ok: u64,
    pub requests_unavailable: u64,
    pub requests_error: u64,
    pub error_codes: BTreeMap<String, u64>,
    pub qps: f64,
    pub rows_per_s: f64,
    pub bytes_per_s: f64,
    pub rows_total: u64,
    pub bytes_total: u64,
    pub latency_ms: LatencyMs,
}

impl StepRecord {
    /// Serialise to a single JSONL line (no trailing newline).
    pub fn to_jsonl(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qps_is_ok_over_duration_and_percentiles_over_ok_only() {
        let mut agg = StepAgg::new();
        // Three ok requests at 1/2/3 ms, plus one shed and one error — the error
        // and shed must NOT enter the latency histogram or the qps numerator.
        agg.record_ok(1_000, 10, 100);
        agg.record_ok(2_000, 20, 200);
        agg.record_ok(3_000, 30, 300);
        agg.record_outcome(&Outcome::Unavailable);
        agg.record_outcome(&Outcome::Error("Internal".to_string()));

        let rec = agg.into_record(
            "r".into(),
            "http://x".into(),
            0,
            42,
            2,
            8,
            "mixed".into(),
            2.0,
        );
        assert_eq!(rec.requests_ok, 3);
        assert_eq!(rec.requests_unavailable, 1);
        assert_eq!(rec.requests_error, 1);
        assert_eq!(rec.error_codes.get("Internal"), Some(&1));
        assert_eq!(rec.rows_total, 60);
        assert_eq!(rec.bytes_total, 600);
        // qps = requests_ok / duration_s = 3 / 2.0
        assert!((rec.qps - 1.5).abs() < 1e-9);
        assert!((rec.rows_per_s - 30.0).abs() < 1e-9);
        // Latency samples = ok count only (3), not 5.
        assert_eq!(rec.latency_ms.samples, 3);
        assert!(rec.latency_ms.p50 >= 1.0 && rec.latency_ms.p50 <= 3.0);
        assert!(rec.latency_ms.max >= 2.9 && rec.latency_ms.max <= 3.1);
    }

    #[test]
    fn record_is_valid_jsonl_with_required_fields() {
        let agg = StepAgg::new();
        let rec = agg.into_record(
            "round1".into(),
            "http://h:8815".into(),
            7,
            42,
            0,
            1,
            "full".into(),
            1.0,
        );
        let line = rec.to_jsonl().expect("serialize");
        assert!(!line.contains('\n'), "a record is a single JSONL line");
        let v: serde_json::Value = serde_json::from_str(&line).expect("parse back");
        for field in [
            "schema",
            "target_concurrency",
            "shape",
            "duration_s",
            "requests_ok",
            "requests_unavailable",
            "requests_error",
            "qps",
            "rows_per_s",
            "bytes_per_s",
            "rows_total",
            "bytes_total",
            "latency_ms",
        ] {
            assert!(v.get(field).is_some(), "required field {field} present");
        }
        assert_eq!(v["schema"], SCHEMA_TAG);
        for p in ["p50", "p95", "p99", "max"] {
            assert!(v["latency_ms"].get(p).is_some(), "latency {p} present");
        }
    }

    #[test]
    fn merge_folds_partials() {
        let mut a = StepAgg::new();
        a.record_ok(1_000, 1, 10);
        a.record_outcome(&Outcome::Error("X".into()));
        let mut b = StepAgg::new();
        b.record_ok(4_000, 2, 20);
        b.record_outcome(&Outcome::Unavailable);
        a.merge(&b);
        assert_eq!(a.ok(), 2);
        assert_eq!(a.total(), 4);
        let rec = a.into_record("".into(), "".into(), 0, 0, 0, 2, "mixed".into(), 1.0);
        assert_eq!(rec.rows_total, 3);
        assert_eq!(rec.bytes_total, 30);
        assert_eq!(rec.requests_unavailable, 1);
        assert_eq!(rec.requests_error, 1);
        assert_eq!(rec.latency_ms.samples, 2);
    }
}
