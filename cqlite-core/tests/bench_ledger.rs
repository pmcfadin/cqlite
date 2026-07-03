//! Pure unit tests for the shared `bench_ledger` module (Issue #1566, Epic A / A5):
//! record serialization + ledger-path resolution. The module lives under `benches/`
//! (a bench-support module included by the harness benches via `#[path]`) and is not
//! compiled by `cargo test` on its own; including it here — the same `#[path]`
//! pattern the benches use — compiles it into this integration crate so its surface
//! can be tested with a real test harness (Cargo builds bench targets with
//! `cfg(test)` set, so inline `#[cfg(test)]` tests cannot live in the module itself).

#[path = "../benches/bench_ledger/mod.rs"]
mod bench_ledger;

use serial_test::serial;
use std::path::PathBuf;

/// Path resolution: the `CQLITE_BENCH_LEDGER` env override wins when set/non-empty;
/// otherwise the default is anchored under the crate's `../target/profiling/`.
/// Serialized because it mutates the process-global `CQLITE_BENCH_LEDGER` env var.
#[test]
#[serial]
fn ledger_path_prefers_env_then_default() {
    std::env::remove_var("CQLITE_BENCH_LEDGER");
    let default = bench_ledger::ledger_path();
    assert!(
        default.ends_with("target/profiling/history.jsonl"),
        "default must be <manifest>/../target/profiling/history.jsonl, got {default:?}"
    );

    std::env::set_var("CQLITE_BENCH_LEDGER", "/tmp/cqlite-a5-ledger-test.jsonl");
    assert_eq!(
        bench_ledger::ledger_path(),
        PathBuf::from("/tmp/cqlite-a5-ledger-test.jsonl")
    );

    // Empty/whitespace env falls back to the default.
    std::env::set_var("CQLITE_BENCH_LEDGER", "   ");
    assert_eq!(bench_ledger::ledger_path(), default);
    std::env::remove_var("CQLITE_BENCH_LEDGER");
}

/// Record serialization + append: each metric becomes exactly one standalone JSON
/// line with the six unified-schema fields, a batch shares one ts/commit, and a
/// second call appends (never truncates).
#[test]
#[serial]
fn append_metrics_writes_one_json_line_per_metric() {
    let dir = tempfile::TempDir::new().expect("temp dir for ledger");
    let path = dir.path().join("history.jsonl");

    std::env::set_var("CQLITE_BENCH_LEDGER", &path);
    std::env::set_var("GIT_COMMIT", "deadbeefcafef00d");
    bench_ledger::append_metrics(
        "unit_bench",
        &[
            ("p50", 100.0, "ns"),
            ("p99", 400.0, "ns"),
            ("ratio", 4.0, "ratio"),
        ],
    )
    .expect("append batch 1");
    // A second call appends (never truncates).
    bench_ledger::append_metrics("unit_bench", &[("p50", 110.0, "ns")]).expect("append batch 2");
    std::env::remove_var("GIT_COMMIT");
    std::env::remove_var("CQLITE_BENCH_LEDGER");

    let contents = std::fs::read_to_string(&path).expect("read ledger");
    let lines: Vec<&str> = contents.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 4, "3 + 1 metric records, got: {contents}");

    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        assert!(v.get("ts").and_then(|t| t.as_u64()).is_some(), "ts: {line}");
        assert_eq!(v.get("bench").and_then(|b| b.as_str()), Some("unit_bench"));
        assert_eq!(
            v.get("commit").and_then(|c| c.as_str()),
            Some("deadbeefcafef00d"),
            "commit: {line}"
        );
        assert!(
            v.get("metric").and_then(|m| m.as_str()).is_some(),
            "metric: {line}"
        );
        assert!(
            v.get("value").and_then(|x| x.as_f64()).is_some(),
            "value: {line}"
        );
        assert!(
            v.get("unit").and_then(|u| u.as_str()).is_some(),
            "unit: {line}"
        );
    }

    // The first three records (one append_metrics call) share a single ts.
    let b1: Vec<u64> = lines[..3]
        .iter()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["ts"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert!(
        b1.windows(2).all(|w| w[0] == w[1]),
        "one append_metrics call must stamp a single ts across its batch"
    );
}
