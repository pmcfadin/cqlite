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
//! Every case here therefore goes through a real open boundary — `Database::open`,
//! `StorageEngine::open`, `StorageEngine::open_with_sstables`,
//! `SSTableManager::new`, `SSTableManager::new_from_discovered_paths`,
//! `SSTableReader::open` — never through `validate()`.
//!
//! # The discovery boundaries, and why they are the DANGEROUS ones (roborev r3 F2)
//!
//! On the engine/manager boundaries the unvalidated failure mode was not a clamp.
//! Discovery treats a per-file reader-open error as best-effort — it logs and
//! skips — so a fraction the readers reject fails EVERY reader open and the open
//! reports SUCCESS with ZERO SSTables. A silent empty result is strictly worse
//! than a wrong-but-visible value, and it is asserted directly by
//! `an_invalid_fraction_is_an_error_not_a_successful_open_with_zero_sstables`,
//! whose control demonstrates the swallowing mechanism rather than assuming it.
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

/// The config check runs before ANY filesystem call, proven by a path that does
/// not exist (#1696 roborev r2 F4).
///
/// The case above used a file that DOES exist, so it could not tell "validated
/// before file I/O" from "validated after the metadata read succeeded" — and the
/// second was the truth: `open_inner` called `tokio::fs::metadata` first, so a
/// missing or unreadable file MASKED the invalid config behind an I/O error and
/// the caller was told about the wrong problem. With a nonexistent path the two
/// orders give different errors, so this pins the order rather than restating it.
#[tokio::test]
async fn sstable_reader_open_reports_the_config_error_not_the_missing_file() {
    let temp = TempDir::new().expect("temp dir");
    let missing = temp.path().join("nb-1-big-Data.db");
    assert!(!missing.exists(), "the subject path must NOT exist");

    for (fraction, why) in illegal_fractions() {
        let config = config_with_fraction(fraction);
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("platform for a config whose only defect is the fraction"),
        );
        let error = SSTableReader::open(&missing, &config, platform)
            .await
            .err()
            .unwrap_or_else(|| {
                panic!("SSTableReader::open MUST reject fraction = {fraction} ({why})")
            });
        let message = error.to_string();
        assert!(
            message.contains("direct_io_memory_fraction"),
            "a nonexistent file must NOT mask the config error \
             (fraction {fraction}, {why}): {message}"
        );
    }

    // Control: with a LEGAL fraction the same missing path fails as an I/O
    // error, so the assertions above report the config check running first and
    // not merely that every open of this path fails.
    let config = config_with_fraction(0.5);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let control = SSTableReader::open(&missing, &config, platform).await;
    let control_message = control.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        !control_message.is_empty() && !control_message.contains("direct_io_memory_fraction"),
        "a legal fraction on a missing file must fail for the FILE's reason: \
         {control_message}"
    );
}

/// `StorageEngine::open` — public, and reachable without a `Database` — REJECTS
/// every illegal fraction (#1696 roborev r3 F2).
///
/// Before this it validated nothing, and the failure mode was not "the fraction
/// gets clamped": discovery treats a per-file reader-open error as best-effort
/// (log and skip), so a fraction the readers reject fails EVERY reader open and
/// the engine reports SUCCESS with ZERO SSTables. A silent empty result is worse
/// than a clamp — the caller is told nothing at all.
#[tokio::test]
async fn storage_engine_open_rejects_an_out_of_range_fraction() {
    use cqlite_core::storage::StorageEngine;

    for (fraction, why) in illegal_fractions() {
        let temp = TempDir::new().expect("temp dir");
        let config = config_with_fraction(fraction);
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("platform for a config whose only defect is the fraction"),
        );
        let error = StorageEngine::open(
            temp.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("StorageEngine::open MUST reject fraction = {fraction} ({why})"));
        assert!(
            error.to_string().contains("direct_io_memory_fraction"),
            "the error must NAME the knob (fraction {fraction}, {why}): {error}"
        );
    }

    // Control: a legal fraction opens fine on the same empty directory, so the
    // rejections above are about the fraction and not about the path.
    let temp = TempDir::new().expect("temp dir");
    let config = config_with_fraction(0.5);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    StorageEngine::open(
        temp.path(),
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("0.5 is the default and must open");
}

/// Same for `StorageEngine::open_with_sstables`, the pre-discovered-paths
/// boundary (#1696 roborev r3 F2).
#[tokio::test]
async fn storage_engine_open_with_sstables_rejects_an_out_of_range_fraction() {
    use cqlite_core::storage::StorageEngine;

    let temp = TempDir::new().expect("temp dir");
    let table_dir = temp.path().join("ks/table-0123456789abcdef");
    std::fs::create_dir_all(&table_dir).expect("table dir");

    for (fraction, why) in illegal_fractions() {
        let config = config_with_fraction(fraction);
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("platform for a config whose only defect is the fraction"),
        );
        let error = StorageEngine::open_with_sstables(
            temp.path().join("storage").as_path(),
            vec![table_dir.clone()],
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("open_with_sstables MUST reject fraction = {fraction} ({why})"));
        assert!(
            error.to_string().contains("direct_io_memory_fraction"),
            "the error must NAME the knob (fraction {fraction}, {why}): {error}"
        );
    }

    // Control: legal fraction, same directories, opens.
    let config = config_with_fraction(1.0);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    StorageEngine::open_with_sstables(
        temp.path().join("storage").as_path(),
        vec![table_dir],
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("a legal fraction must open");
}

/// The SILENT-OMISSION scenario stated as an assertion (#1696 roborev r3 F2): an
/// invalid fraction must be a config ERROR, never a successful open reporting
/// fewer SSTables than are on disk.
///
/// # What the control establishes, measured rather than assumed
///
/// The subject directory holds one discoverable `*-Data.db`. With a LEGAL
/// fraction the open succeeds and `sstable_count == 1` — the reader is registered
/// (opens are lazy, so unparseable bytes do not fail registration). So this
/// directory is a case where a caller SHOULD see one SSTable.
///
/// Now the hazard: `SSTableReader::open` rejects an illegal fraction (pinned by
/// `sstable_reader_open_rejects_an_out_of_range_fraction_before_reading` above),
/// and discovery treats a per-file reader-open error as best-effort — it LOGS AND
/// SKIPS. Without a check at this boundary the invalid fraction therefore turns
/// that 1 into a 0 and still returns `Ok`: the SSTable is present on disk and the
/// caller is told nothing. Hence the assertion below is not merely "it errors" but
/// "it does not succeed with a reduced count", and the failure message reports the
/// count it saw.
#[tokio::test]
async fn an_invalid_fraction_is_an_error_not_a_successful_open_with_zero_sstables() {
    use cqlite_core::storage::StorageEngine;

    let temp = TempDir::new().expect("temp dir");
    let table_dir = temp.path().join("ks/table-0123456789abcdef");
    std::fs::create_dir_all(&table_dir).expect("table dir");
    std::fs::write(table_dir.join("nb-1-big-Data.db"), b"not an sstable").expect("subject");

    // Control FIRST, because it establishes the hazard rather than just passing:
    // with a legal fraction this directory yields ONE discovered SSTable, so a
    // later open reporting zero would be a silent omission and not an empty
    // directory.
    let config = config_with_fraction(0.5);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let engine = StorageEngine::open_with_sstables(
        temp.path().join("storage").as_path(),
        vec![table_dir.clone()],
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("a legal fraction must open");
    let stats = engine.stats().await.expect("stats");
    assert_eq!(
        stats.sstables.sstable_count, 1,
        "the control must find the SSTable that IS there — otherwise a later \
         zero would prove nothing about omission"
    );

    // Now the same shape with an invalid fraction: an ERROR, not Ok-with-zero.
    let config = config_with_fraction(2.0);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let outcome = StorageEngine::open_with_sstables(
        temp.path().join("storage2").as_path(),
        vec![table_dir],
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await;
    match outcome {
        Ok(engine) => {
            let count = engine
                .stats()
                .await
                .map(|s| s.sstables.sstable_count)
                .unwrap_or_default();
            panic!(
                "an invalid fraction must be REJECTED; instead the open succeeded \
                 reporting {count} SSTables where the control found 1 — a silent \
                 omission the caller cannot detect"
            );
        }
        Err(e) => assert!(
            e.to_string().contains("direct_io_memory_fraction"),
            "the error must name the knob, not blame the data: {e}"
        ),
    }
}

/// `SSTableManager::new` is public too, so it enforces the rule itself rather
/// than trusting whichever engine happened to construct it (#1696 roborev r3 F2).
#[tokio::test]
async fn sstable_manager_constructors_reject_an_out_of_range_fraction() {
    use cqlite_core::storage::sstable::SSTableManager;

    let temp = TempDir::new().expect("temp dir");
    std::fs::create_dir_all(temp.path().join("base")).expect("base dir");

    for (fraction, why) in illegal_fractions() {
        let config = config_with_fraction(fraction);
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("platform for a config whose only defect is the fraction"),
        );
        let error = SSTableManager::new(
            temp.path().join("base").as_path(),
            &config,
            platform.clone(),
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("SSTableManager::new MUST reject {fraction} ({why})"));
        assert!(
            error.to_string().contains("direct_io_memory_fraction"),
            "the error must NAME the knob (fraction {fraction}, {why}): {error}"
        );

        let error = SSTableManager::new_from_discovered_paths(
            temp.path().join("base").as_path(),
            vec![temp.path().join("base")],
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("new_from_discovered_paths MUST reject {fraction} ({why})"));
        assert!(
            error.to_string().contains("direct_io_memory_fraction"),
            "the error must NAME the knob (fraction {fraction}, {why}): {error}"
        );
    }

    // Control: a legal fraction builds both.
    let config = config_with_fraction(0.5);
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableManager::new(
        temp.path().join("base").as_path(),
        &config,
        platform.clone(),
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("a legal fraction must construct");
    SSTableManager::new_from_discovered_paths(
        temp.path().join("base").as_path(),
        vec![temp.path().join("base")],
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("a legal fraction must construct");
}
