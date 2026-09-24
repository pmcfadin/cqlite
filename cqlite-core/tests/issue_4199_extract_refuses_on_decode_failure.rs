//! Issue #4199 (spec R6.1) — a decode failure in the REQUESTED partition
//! refuses the WHOLE `extract` run: nothing published under `--out`, and the
//! refusal names the generation/offset the mutation targeted.
//!
//! # Oracle (#3042)
//!
//! Reuses `corrupt_byte_fixture`'s `ClusteringTextLiteral` mutation (issue
//! #3782) — a REAL Cassandra 5.0 fixture (`test_basic.composite_key_table`)
//! staged twice, pristine and with exactly one decompressed byte flipped
//! inside a clustering value, CRC recomputed so the corruption is invisible
//! to integrity checks. The same technique `issue_4196_salvage_partition_atomicity.rs`
//! uses to identify its needle partition from the CONTROL copy's own
//! `Index.db` positions.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::write_engine::extract_split::{
    extract_partitions, ExtractOptions, RefusalReason, Selection,
};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod corrupt_fixture;
#[path = "support/extract_split_fixture.rs"]
mod fixture;

use corrupt_fixture::{FIX_KS, FIX_TABLE};
use fixture::{require_fixtures_strict, table_schema};

#[tokio::test]
async fn extract_refuses_on_a_decode_failure_in_the_requested_partition() {
    let Some(fixture_dir) = datasets_root::sstables_root_for_table(FIX_KS, FIX_TABLE)
        .map(|root| datasets_root::table_generation_dirs(&root, FIX_KS, FIX_TABLE))
        .and_then(|dirs| dirs.into_iter().next())
    else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {FIX_KS}.{FIX_TABLE} is absent; {}",
                datasets_root::describe_search(FIX_KS, FIX_TABLE)
            );
        }
        eprintln!("[issue_4199] {FIX_KS}.{FIX_TABLE} fixture absent (dataset not fetched); skipping");
        return;
    };

    let staged = corrupt_fixture::stage_control_and_mutated(&fixture_dir, "extract-refuse-4199");
    let control_positions = corrupt_fixture::index_partition_positions(&staged.control_dir);
    let (needle_key, _needle_pos) = control_positions
        .iter()
        .rfind(|(_, pos)| *pos <= staged.mutated_offset)
        .cloned()
        .expect("a partition containing the mutated offset must exist");

    let schema = table_schema();
    let temp = TempDir::new().expect("tempdir");
    let out_dir = temp.path().join("out");
    let options = ExtractOptions {
        out_dir: out_dir.clone(),
        raw: false,
    };
    let report = extract_partitions(
        &staged.mutated_dir,
        Selection::Key(needle_key),
        &schema,
        options,
    )
    .await
    .expect("extract_partitions must not hard-Err -- a decode failure is a classified refusal");

    let refused = report
        .refused
        .as_ref()
        .expect("a decode failure on the REQUESTED partition must refuse the whole run");
    assert_eq!(refused.reason, RefusalReason::PartitionDecodeFailed);
    assert!(
        report.generations_written.is_empty(),
        "nothing may be published under --out on a refusal; got {:?}",
        report.generations_written
    );
    assert!(
        !out_dir.exists() || std::fs::read_dir(&out_dir).unwrap().next().is_none(),
        "--out must hold no Data.db on a refusal"
    );
}
