//! Issue #1696 (AH3, roborev F2): an out-of-range `direct_io_memory_fraction` is
//! REJECTED at the PUBLIC open boundaries, not silently clamped.
//!
//! # Why this test exists rather than a `validate()` unit test
//!
//! The range rule first landed inside `Config::validate`, which nothing on the
//! public read path called: `Database::open` and `SSTableReader::open` both went
//! straight to `resolve_disk_access_mode`, which CLAMPS — `<= 0.0`, NaN and the
//! infinities to the `0.5` default, anything above `1.0` to `1.0`. So
//! `direct_io_memory_fraction = 2.0` passed through the documented database-open
//! API still ran as `1.0`, and the operator was never told: the exact defect the
//! rule exists to fix, with a green guard test beside it.
//!
//! By this repo's wiring-evidence rule a feature is done only when its public
//! surface exercises it — a named surface, a call chain, and an end-to-end test.
//! Every case here therefore goes through `Database::open` or
//! `SSTableReader::open`, never through `validate()`.
//!
//! # The decisions this pins
//!
//! * `1.0` is LEGAL ("all of RAM" is a coherent ceiling).
//! * `0.0` is REJECTED and is NOT read as "never use direct I/O": a zero
//!   threshold makes every nonempty file EXCEED it, so `Auto` would escalate
//!   everything to direct I/O. The value reads as "never" and behaves as
//!   "always", and CQLite does not guess which was meant (issue #28). "Never" is
//!   spelled `DiskAccessMode::Mmap`/`Buffered`; "always" is `Direct`.
//! * A subnormal / tiny positive fraction is LEGAL and honoured LITERALLY (its
//!   threshold rounds to 0 bytes, so every nonempty file uses direct I/O) — a
//!   real, if degenerate, fraction, unlike `0.0` whose plain reading contradicts
//!   its behaviour.
//! * NaN and both infinities are REJECTED.

use cqlite_core::config::{Config, DiskAccessMode};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::Database;
use std::sync::Arc;
use tempfile::TempDir;

/// Every value that must be REJECTED, with the reason it is illegal.
fn illegal_fractions() -> Vec<(f64, &'static str)> {
    vec![
        (2.0, "above 1.0: was silently pinned at 1.0"),
        (
            1.000_000_1,
            "just above 1.0: the boundary must be closed, not approximate",
        ),
        (0.0, "zero: reads as \"never\", behaves as \"always\""),
        (-1.0, "negative: was silently replaced by the 0.5 default"),
        (
            f64::NAN,
            "NaN: every ordered comparison against it is false",
        ),
        (f64::INFINITY, "+inf: not a fraction of anything"),
        (f64::NEG_INFINITY, "-inf: not a fraction of anything"),
    ]
}

/// Every value that must be ACCEPTED, with what it means.
fn legal_fractions() -> Vec<(f64, &'static str)> {
    vec![
        (0.5, "the default: half of RAM"),
        (
            1.0,
            "all of RAM — a coherent ceiling, so the range is closed here",
        ),
        (
            f64::MIN_POSITIVE,
            "smallest normal: degenerate but unambiguous, honoured literally",
        ),
        (
            5e-324,
            "smallest subnormal: still a real fraction, not clamped",
        ),
    ]
}

fn config_with_fraction(fraction: f64) -> Config {
    let mut config = Config::default();
    // `Auto` is the mode the fraction steers, so this is the configuration in
    // which the knob is actually consulted.
    config.storage.disk_access_mode = DiskAccessMode::Auto;
    config.storage.direct_io_memory_fraction = fraction;
    config
}

/// `Database::open` — the documented database-open API — REJECTS every illegal
/// fraction, naming the knob.
#[tokio::test]
async fn database_open_rejects_an_out_of_range_fraction() {
    for (fraction, why) in illegal_fractions() {
        let temp = TempDir::new().expect("temp dir");
        let error = Database::open(temp.path(), config_with_fraction(fraction))
            .await
            .err()
            .unwrap_or_else(|| {
                panic!(
                    "Database::open MUST reject direct_io_memory_fraction = {fraction} \
                     ({why}); silently clamping it is the #1696 AH3 defect"
                )
            });
        let message = error.to_string();
        assert!(
            message.contains("direct_io_memory_fraction"),
            "the error must NAME the knob so the operator can fix it \
             (fraction {fraction}, {why}): {message}"
        );
    }
}

/// The same surface ACCEPTS every legal fraction — the fix must reject nonsense
/// without rejecting the range's own endpoints.
#[tokio::test]
async fn database_open_accepts_every_legal_fraction() {
    for (fraction, meaning) in legal_fractions() {
        let temp = TempDir::new().expect("temp dir");
        Database::open(temp.path(), config_with_fraction(fraction))
            .await
            .unwrap_or_else(|e| {
                panic!("direct_io_memory_fraction = {fraction} ({meaning}) is LEGAL: {e}")
            });
    }
}

/// `SSTableReader::open` is reachable WITHOUT a `Database`, so it enforces the
/// rule itself — and before any file I/O, since a config error needs no bytes.
///
/// The subject is a file that is deliberately NOT a valid SSTable: with a legal
/// fraction the open fails for a DIFFERENT reason (control below), so a
/// configuration error here can only have come from the config check running
/// first. That keeps this case hermetic — no corpus fixture, no 0-row silent skip.
#[tokio::test]
async fn sstable_reader_open_rejects_an_out_of_range_fraction_before_reading() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("nb-1-big-Data.db");
    std::fs::write(&path, b"not an sstable").expect("write subject file");

    for (fraction, why) in illegal_fractions() {
        let config = config_with_fraction(fraction);
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("platform for a config whose only defect is the fraction"),
        );
        let error = SSTableReader::open(&path, &config, platform)
            .await
            .err()
            .unwrap_or_else(|| {
                panic!("SSTableReader::open MUST reject fraction = {fraction} ({why})")
            });
        let message = error.to_string();
        assert!(
            message.contains("direct_io_memory_fraction"),
            "the reader boundary must reject the CONFIG, not fall through to a \
             parse error (fraction {fraction}, {why}): {message}"
        );
    }

    // Control: with a LEGAL fraction the same file fails for its own reason, so
    // the assertions above are about the fraction and nothing else.
    let config = config_with_fraction(0.5);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let control = SSTableReader::open(&path, &config, platform).await;
    let control_message = control.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        !control_message.contains("direct_io_memory_fraction"),
        "a legal fraction must never produce the fraction error: {control_message}"
    );
}

/// The clamp itself is KEPT as defense in depth, so removing it is not what fixed
/// this — the rejection at the boundary is. Asserted by observing that a legal
/// fraction of `1.0` (the value the old clamp produced for `2.0`) opens fine,
/// while `2.0` no longer reaches the resolver at all.
#[tokio::test]
async fn two_point_zero_no_longer_silently_becomes_one_point_zero() {
    let temp = TempDir::new().expect("temp dir");
    Database::open(temp.path(), config_with_fraction(1.0))
        .await
        .expect("1.0 is the value the old clamp substituted, and it is legal");

    let temp = TempDir::new().expect("temp dir");
    assert!(
        Database::open(temp.path(), config_with_fraction(2.0))
            .await
            .is_err(),
        "2.0 must be an ERROR, not a silent 1.0"
    );
}
