//! Issue #4194 blocker #1 — the BTI corroboration gate's SUPPRESSION PATHS
//! (owner ruling 2026-10-02: option (a), fail closed).
//!
//! The review identified three ways the FULL-mode BTI identity cross-check
//! (`bti_partition_identity_mismatch`) can fail to run while a
//! `PendingLocation` still awaits resolution — i.e. three ways "no mismatch
//! was reported" could be read as "the leaves agree with Data.db" when in
//! fact nothing looked. This file covers each, and where a path turns out to
//! be UNREACHABLE end-to-end it says so with a test that would FAIL if that
//! ever changed, rather than leaving a silent gap.
//!
//! A separate file from `issue_4194_verify_location.rs` purely for the
//! file-size ratchet: that file is at 1494 of its 1500-line budget.
//!
//! Why these are integration tests and the CORROBORATED direction is not:
//! a BTI location is, with the current check set, always `Unresolved`
//! end-to-end — see `verify_tests.rs`'s `bti_gate_*` pair, which pins both
//! directions at the seam and records the measurement behind that claim.

#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "lz4"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{
    verify_sstable, PartitionResolution, VerifyErrorClass, VerifyMode,
    BOUNDARY_SOURCE_UNREADABLE_CAUSE, BTI_IDENTITY_UNCORROBORATED,
};
use cqlite_core::Config;

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::resolve_table_generation_dir;

/// `test_da/wide_table`'s generation directory. Force-added to git (full BTI
/// component set), so its absence is a BROKEN CHECKOUT and must fail closed
/// UNCONDITIONALLY — never a `CQLITE_REQUIRE_FIXTURES`-gated skip (#3220).
fn wide_table_dir() -> PathBuf {
    resolve_table_generation_dir("test_da", "wide_table").unwrap_or_else(|why| {
        panic!(
            "COMMITTED clean source test_da.wide_table is absent: {why}. Its *.db binaries are \
             git-tracked, so this is a BROKEN CHECKOUT, not an unfetched dataset."
        )
    })
}

/// The corruption corpus's `compression_info_bad_offset` fixture, resolved
/// over the same candidate roots as the datasets corpus. `None` (with a loud
/// SKIP) when the corpus is not present, hard failure under
/// `CQLITE_REQUIRE_FIXTURES=1`.
fn compression_info_bad_offset_dir() -> Option<PathBuf> {
    let require = std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1")
        .unwrap_or(false);
    let rel = "corruption/test_comp_corrupt/compression_info_bad_offset";
    for root in datasets_root::sstables_root_candidates() {
        // `sstables_root_candidates` yields `<base>/sstables`; the corruption
        // corpus is its sibling.
        if let Some(base) = root.parent() {
            let candidate = base.join(rel);
            if candidate.join("nb-1-big-CompressionInfo.db").is_file() {
                return Some(candidate);
            }
        }
    }
    assert!(
        !require,
        "CQLITE_REQUIRE_FIXTURES=1 but the {rel} corpus fixture is absent"
    );
    eprintln!("SKIP: {rel} corpus fixture absent; set CQLITE_REQUIRE_FIXTURES=1 to enforce.");
    None
}

fn copy_generation(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create staging dir");
    for e in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        let name = e.file_name();
        let as_str = name.to_string_lossy();
        // Reference sidecars are not SSTable components; copying them is
        // harmless but they confuse the TOC presence check.
        if as_str.ends_with(".jsonl") || as_str.ends_with(".db.txt") {
            continue;
        }
        if e.path().is_file() {
            std::fs::copy(e.path(), dst.join(&name)).expect("copy fixture component");
        }
    }
}

async fn run_verify(dir: &Path) -> cqlite_core::storage::sstable::verify::VerifyReport {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    verify_sstable(dir, VerifyMode::Full, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable({}): {e}", dir.display()))
}

/// Every location in `report`, with its finding's class — a location-bearing
/// report is the premise of every case here, so this asserts non-emptiness.
fn locations(
    report: &cqlite_core::storage::sstable::verify::VerifyReport,
) -> Vec<(VerifyErrorClass, PartitionResolution)> {
    let out: Vec<(VerifyErrorClass, PartitionResolution)> = report
        .findings
        .iter()
        .filter_map(|f| f.location.as_ref().map(|l| (f.class, l.partitions.clone())))
        .collect();
    assert!(
        !out.is_empty(),
        "this case requires at least one location-bearing finding, or it measures nothing: {:#?}",
        report.findings
    );
    out
}

/// Stage `wide_table` and truncate its `Data.db` so that chunk offsets past
/// the new EOF are out of bounds.
fn stage_truncated_wide_table(prefix: &str, keep: u64) -> (tempfile::TempDir, PathBuf) {
    let staging = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create staging temp dir");
    let staged = staging.path().join("da-2-bti");
    copy_generation(&wide_table_dir(), &staged);
    let data = staged.join("da-2-bti-Data.db");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&data)
        .expect("open staged Data.db");
    f.set_len(keep).expect("truncate staged Data.db");
    (staging, staged)
}

// ---------------------------------------------------------------------------
// PATH 3 — the SAME-EVENT case, and the sharpest of the three.
//
// `ChunkOffsetOutOfBounds` is pushed in `check_compression_info` ALONGSIDE the
// very `PendingLocation` that needs resolving (verify.rs). That same finding
// then sets `compression_metadata_corrupt`, which skips the whole scan block —
// so ONE corruption event both CREATES the location and DISABLES the only
// guard on it. Pre-ruling this reported `Resolved` with a full partition list.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn path3_chunk_offset_out_of_bounds_disables_its_own_guard_and_refuses() {
    // 491 is `wide_table`'s chunk[2] offset, so chunks 2.. are all past EOF
    // while chunks 0-1 remain intact.
    let (staging, staged) = stage_truncated_wide_table("cqlite-4194-path3-", 491);
    let report = run_verify(&staged).await;

    let locs = locations(&report);
    let oob: Vec<&PartitionResolution> = locs
        .iter()
        .filter(|(class, _)| *class == VerifyErrorClass::ChunkOffsetOutOfBounds)
        .map(|(_, p)| p)
        .collect();
    assert!(
        !oob.is_empty(),
        "expected a located ChunkOffsetOutOfBounds finding: {:#?}",
        report.findings
    );
    for partitions in oob {
        assert_eq!(
            *partitions,
            PartitionResolution::Unresolved(BTI_IDENTITY_UNCORROBORATED.to_string()),
            "the corruption that created this location also skipped the scan that is the only \
             check on the trie's leaf identities, so the trie is UNCORROBORATED and must be \
             refused by name"
        );
    }
    drop(staging);
}

// ---------------------------------------------------------------------------
// PATH 1 — a DIRECT finding against the boundary source.
//
// Already distrusted before this change, via the component/class union
// predicate. The property under test is the ORDERING: a direct finding must
// still report its own, MORE SPECIFIC cause rather than being flattened into
// the new corroboration cause. Getting this backwards would be a real
// regression in diagnosability — "Partitions.db is corrupt" and "the trie was
// never cross-checked" are different operator actions.
//
// MEASURED: this case is green with AND without the corroboration gate, BY
// DESIGN. It is an ORDERING guard, not gate coverage — do not read it as the
// latter. The gate itself is red-verified by `path3` below (pre-gate it
// returned `Resolved{[00000001, 00000002, 00000003]}`) and by the seam pair in
// `verify_tests.rs`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn path1_direct_boundary_finding_keeps_its_own_more_specific_cause() {
    let (staging, staged) = stage_truncated_wide_table("cqlite-4194-path1-", 491);
    // Truncate Rows.db to 0 as well: `check_bti_structure` then reports
    // against a boundary COMPONENT, which the union predicate catches.
    let rows = staged.join("da-2-bti-Rows.db");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&rows)
        .expect("open staged Rows.db");
    f.set_len(0).expect("truncate staged Rows.db");

    let report = run_verify(&staged).await;
    let boundary_findings: Vec<&cqlite_core::storage::sstable::verify::VerifyFinding> = report
        .findings
        .iter()
        .filter(|f| f.component == "Rows.db" || f.component == "Partitions.db")
        .collect();
    assert!(
        !boundary_findings.is_empty(),
        "this case requires a DIRECT finding against a boundary component: {:#?}",
        report.findings
    );

    for (class, partitions) in locations(&report) {
        assert_eq!(
            partitions,
            PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE_CAUSE.to_string()),
            "a direct boundary-source finding must keep its OWN cause; flattening it into the \
             corroboration cause would tell the operator to re-run a cross-check when the real \
             action is to repair {class:?}'s component"
        );
    }
    drop(staging);
}

// ---------------------------------------------------------------------------
// PATH 2 — `compression_metadata_corrupt` via a PARSE failure: UNREACHABLE,
// asserted rather than assumed.
//
// The review named this as a third, independent path: a BTI footer-flip plus
// a corrupt `CompressionInfo.db` skips the cross-check with no `Data.db`
// damage at all. It is now unreachable END-TO-END, because a
// `CompressionInfo.db` that fails to PARSE yields no `PendingLocation` in the
// first place: `check_compression_info` returns `CompressionState::Unreadable`
// (the I3 tri-state fix in this same PR), so neither chunk check runs, so
// there is nothing to resolve and `finalize_locations` is never called.
//
// So this is a test that the path produces NOTHING, not a test that it
// refuses. If a future change ever makes a location coexist with a
// parse-failed `CompressionInfo.db`, this FAILS and forces the gate to be
// re-examined for that shape — which is the point of asserting unreachability
// instead of quietly omitting the case. The MECHANISM (cross-check skipped =>
// refuse) is covered regardless by `bti_gate_uncorroborated_refuses_by_name`,
// which drives the gate directly and does not care why the scan was skipped.
//
// Like `path1`, this is green with AND without the gate by design: it asserts
// an ABSENCE (no location exists), which the gate cannot affect.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn path2_unparseable_compression_info_yields_no_location_to_resolve() {
    let Some(bad_ci) = compression_info_bad_offset_dir() else {
        return;
    };

    let staging = tempfile::Builder::new()
        .prefix("cqlite-4194-path2-")
        .tempdir()
        .expect("create staging temp dir");
    let staged = staging.path().join("da-2-bti");
    copy_generation(&wide_table_dir(), &staged);
    // A CompressionInfo.db that fails to PARSE (ascending-order violation),
    // over an otherwise untouched BTI generation.
    std::fs::copy(
        bad_ci.join("nb-1-big-CompressionInfo.db"),
        staged.join("da-2-bti-CompressionInfo.db"),
    )
    .expect("overlay the unparseable CompressionInfo.db");

    let report = run_verify(&staged).await;
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::CompressionInfoCorrupt),
        "the staged fixture must actually report CompressionInfoCorrupt: {:#?}",
        report.findings
    );
    let located: Vec<&cqlite_core::storage::sstable::verify::VerifyFinding> = report
        .findings
        .iter()
        .filter(|f| f.location.is_some())
        .collect();
    assert!(
        located.is_empty(),
        "DECLARED UNREACHABLE: an unparseable CompressionInfo.db must produce NO located \
         finding, because `CompressionState::Unreadable` runs neither chunk check. A location \
         here means this suppression path became reachable and needs its own refusal \
         assertion: {located:#?}"
    );
    drop(staging);
}
