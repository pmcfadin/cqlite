//! Tests for the mixed-load tail-latency harness (Issue #1563, Epic A / A2).
//!
//! Includes the shared harness module and the fixtures loader via `#[path]`
//! (both resolve to `crate::…`, the same pattern the benches use). Two tiers:
//!
//! - **Pure** (always run, no dataset): the percentile math and the
//!   report-ratio/JSON logic.
//! - **Dataset** (`cli-helpers`, skip-not-fail when the fixture is absent): the
//!   self-assertion that mixed-load p99 is bounded by `K` × the scan-free p99,
//!   and a deliberately-loose determinism smoke test. These gate on **ratios**,
//!   never wall-clock absolutes (issue guardrail), and panic loudly (via the
//!   harness setup guards) when the fixture is present but yields 0 rows.

#[path = "../benches/fixtures/mod.rs"]
mod fixtures;

#[path = "../benches/tail_latency/mod.rs"]
mod harness;

// ---------------------------------------------------------------------------
// Pure unit tests (no dataset dependency)
// ---------------------------------------------------------------------------

#[test]
fn percentile_nearest_rank_on_known_vector() {
    // 1..=100: nearest-rank p50 = value at ceil(0.50*100)=50 -> 50;
    // p99 = ceil(0.99*100)=99 -> 99; p999 = ceil(0.999*100)=100 -> 100.
    let v: Vec<u128> = (1..=100).collect();
    assert_eq!(harness::percentile_ns(&v, 50.0), 50);
    assert_eq!(harness::percentile_ns(&v, 99.0), 99);
    assert_eq!(harness::percentile_ns(&v, 99.9), 100);

    // Works on an unsorted input (the function sorts a copy).
    let mut shuffled = v.clone();
    shuffled.reverse();
    assert_eq!(harness::percentile_ns(&shuffled, 50.0), 50);
    assert_eq!(harness::percentile_ns(&shuffled, 99.0), 99);
}

#[test]
fn percentile_empty_sample_is_zero() {
    assert_eq!(harness::percentile_ns(&[], 50.0), 0);
    assert_eq!(harness::percentile_ns(&[], 99.9), 0);
}

#[test]
fn tailstats_invariant_p50_le_p99_le_p999() {
    let v: Vec<u128> = vec![5, 1, 9, 3, 7, 2, 8, 4, 6, 10];
    let s = harness::TailStats::from_latencies(&v);
    assert!(s.p50 <= s.p99, "p50 {} !<= p99 {}", s.p50, s.p99);
    assert!(s.p99 <= s.p999, "p99 {} !<= p999 {}", s.p99, s.p999);
}

#[test]
fn report_ratios_and_json_shape() {
    let mixed = harness::TailStats {
        p50: 100,
        p99: 400,
        p999: 800,
    };
    let scan_free = harness::TailStats {
        p50: 90,
        p99: 100,
        p999: 150,
    };
    let report = harness::HarnessReport::new(mixed, scan_free);

    // p99_over_p50 = mixed.p99 / mixed.p50 = 400/100 = 4.0
    assert!((report.p99_over_p50 - 4.0).abs() < 1e-9);
    // p99_mixed_over_scan_free = mixed.p99 / scan_free.p99 = 400/100 = 4.0
    assert!((report.p99_mixed_over_scan_free - 4.0).abs() < 1e-9);

    // JSON contains both stat blocks and the ratios (spec field names).
    let json = report.to_json();
    for field in [
        "mixed",
        "scan_free",
        "p50",
        "p99",
        "p999",
        "p99_over_p50",
        "p99_mixed_over_scan_free",
    ] {
        assert!(json.contains(field), "JSON missing `{field}`: {json}");
    }
}

#[test]
fn append_ledger_writes_one_json_line_with_required_keys() {
    // Exercises the persisted-ledger surface (spec R4) without a dataset: build a
    // report, append it twice to a temp ledger, and assert each append is exactly
    // one JSON-lines record carrying ts + commit + both stat blocks + ratios.
    let report = harness::HarnessReport::new(
        harness::TailStats {
            p50: 100,
            p99: 400,
            p999: 800,
        },
        harness::TailStats {
            p50: 90,
            p99: 100,
            p999: 150,
        },
    );

    let dir = tempfile::TempDir::new().expect("temp dir for ledger");
    let path = dir.path().join("tail-latency-history.jsonl");

    // GIT_COMMIT override keeps the record deterministic (no reliance on a repo).
    std::env::set_var("GIT_COMMIT", "deadbeefcafef00d");
    harness::append_ledger(&path, &report).expect("append ledger record 1");
    harness::append_ledger(&path, &report).expect("append ledger record 2");
    std::env::remove_var("GIT_COMMIT");

    let contents = std::fs::read_to_string(&path).expect("read ledger");
    let lines: Vec<&str> = contents.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "expected 2 JSON-lines records, got: {contents}"
    );

    // Each line is a standalone JSON object with the required keys.
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).expect("record is valid JSON");
        assert!(v.get("ts").and_then(|t| t.as_u64()).is_some(), "ts: {line}");
        assert_eq!(
            v.get("commit").and_then(|c| c.as_str()),
            Some("deadbeefcafef00d"),
            "commit: {line}"
        );
        for key in ["mixed", "scan_free"] {
            let block = v
                .get(key)
                .unwrap_or_else(|| panic!("missing {key}: {line}"));
            for stat in ["p50", "p99", "p999"] {
                assert!(
                    block.get(stat).and_then(|s| s.as_u64()).is_some(),
                    "{key}.{stat}: {line}"
                );
            }
        }
        for ratio in ["p99_over_p50", "p99_mixed_over_scan_free"] {
            assert!(
                v.get(ratio).and_then(|r| r.as_f64()).is_some(),
                "{ratio}: {line}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Dataset tests (require cli-helpers + the present BIG fixture)
// ---------------------------------------------------------------------------

/// Bound for the mixed-load convoy: point-read p99 under the background scan must
/// be at most `K` × the scan-free baseline p99.
///
/// `K` is chosen GENEROUSLY above the convoy ratio measured on `main` so the test
/// is green today; it tightens as the C2/F1/F3 tail fixes land. Measured on main
/// (2026-07, local run against the BIG `test_basic.simple_table` fixture):
/// p99_mixed/p99_scan_free ≈ 1.28 (mixed p99 25583 ns vs scan-free p99 19959 ns).
/// The convoy is mild on fast local hardware with this small fixture; it is worse
/// on slower/loaded shared runners and under the heavier loads the audit
/// describes. `K = 12` (~9× the measured ratio) leaves ample headroom for
/// shared-runner tail noise while still catching a gross convoy regression. This
/// blocking test is intentionally the loosest safety net; the advisory
/// `tail-latency-gate.json` `p99_mixed_over_scan_free` max is a tighter watch line.
#[cfg(feature = "cli-helpers")]
const K: f64 = 12.0;

#[cfg(feature = "cli-helpers")]
#[test]
fn mixed_p99_bounded_by_k_times_baseline() {
    use fixtures::ReadFixture;

    if !fixtures::fixture_present(&ReadFixture::SIMPLE) {
        eprintln!("tail_latency test: SIMPLE fixture absent — skipping (fetch datasets)");
        return;
    }

    let report = harness::run(ReadFixture::SIMPLE).expect("harness run on present fixture");

    // Structural invariants in both blocks (rows>0 is enforced via setup's no-panic).
    assert!(report.mixed.p50 <= report.mixed.p99 && report.mixed.p99 <= report.mixed.p999);
    assert!(
        report.scan_free.p50 <= report.scan_free.p99
            && report.scan_free.p99 <= report.scan_free.p999
    );

    let bound = K * (report.scan_free.p99 as f64);
    assert!(
        (report.mixed.p99 as f64) <= bound,
        "mixed p99 {} exceeded K({K}) * scan_free p99 {} = {bound} \
         (p99_mixed_over_scan_free = {:.2})",
        report.mixed.p99,
        report.scan_free.p99,
        report.p99_mixed_over_scan_free
    );
}

#[cfg(feature = "cli-helpers")]
#[test]
fn scan_free_determinism_within_wide_tolerance() {
    use fixtures::ReadFixture;

    if !fixtures::fixture_present(&ReadFixture::SIMPLE) {
        eprintln!("tail_latency test: SIMPLE fixture absent — skipping (fetch datasets)");
        return;
    }

    let (db, sql) = harness::setup(&ReadFixture::SIMPLE);
    let a = harness::run_point_read_stream(&db, &sql, harness::MEASURED_N, harness::WARMUP);
    let b = harness::run_point_read_stream(&db, &sql, harness::MEASURED_N, harness::WARMUP);

    let sa = harness::TailStats::from_latencies(&a);
    let sb = harness::TailStats::from_latencies(&b);

    assert!(sa.p50 <= sa.p99 && sa.p99 <= sa.p999);
    assert!(sb.p50 <= sb.p99 && sb.p99 <= sb.p999);

    // Deliberately loose: two consecutive scan-free p50s agree within 4x either
    // direction. This proves the harness is not wildly nondeterministic without
    // gating on wall-clock noise (issue guardrail).
    let lo = sa.p50.min(sb.p50) as f64;
    let hi = sa.p50.max(sb.p50) as f64;
    assert!(
        hi <= 4.0 * lo.max(1.0),
        "scan-free p50 drift too large across two runs: {} vs {} (>4x)",
        sa.p50,
        sb.p50
    );
}
