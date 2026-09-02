//! Issue #3782 — the point/full differential over a CORRUPT fixture.
//!
//! The parent lane compares the two forced read paths over WELL-FORMED fixtures,
//! where they agree. That is precisely why it could not see #3782: before the fix
//! the two arms agreed *by both truncating*, so a fixture differing from a real
//! Cassandra SSTable by ONE decompressed byte silently dropped 77 of 100 rows on
//! both arms and the differential stayed green. Agreement is necessary and not
//! sufficient — WHAT the arms agree on matters.
//!
//! # What this case asserts
//!
//! 1. The FULL read path now REFUSES at least one partition of the corrupt
//!    fixture, with the decode error's kind preserved (it names the offending
//!    clustering column), instead of returning a short result set.
//! 2. Neither arm ever FABRICATES: any partition either arm still answers `Ok`
//!    for must return exactly the pristine fixture's rows for that partition.
//!
//! # DECLARED GAP — point and full do NOT yet agree here (#3782)
//!
//! The POINT arm still answers `Ok` with a SHORT row set for the damaged
//! partition. That is not an oversight in the fix: the BIG-promoted and BTI point
//! readers use the tolerant row-decode break as their *chunk-straddle protocol*
//! — `bti_point.rs`'s "a DIFFERENT decoded key means the window tail was
//! truncated mid-partition … pull the next chunk" — so refusing there would
//! break a legitimate, load-bearing control flow rather than fix a defect. They
//! decode a chunk-covering WINDOW, not a proven-complete buffer, so the
//! `with_complete_buffer` contract the stitched read path uses does not apply.
//! Closing it needs the point readers bounded to the target partition's own
//! extent first; that is a separate change to a different subsystem, and it is
//! declared here rather than left to be rediscovered from a green suite.
//!
//! Assertion 2 is written so it stays TRUE after that follow-up (a refusing arm
//! simply never enters the `Ok` branch), so this case does not pin the gap.

use std::collections::BTreeMap;
use std::path::Path;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::Config;

#[path = "../support/corrupt_clustering_fixture.rs"]
mod fixture;

use super::datasets_root;

/// Open the fixture at `root` with one forced read path.
async fn open_db(root: &Path, schema: &Path, mode: ReadPathMode) -> cqlite_core::Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{}/", fixture::FIX_KS)),
    })
    .await
    .expect("ingest")
    .database
}

/// The committed fixture directory, resolved per TABLE (#3220).
fn fixture_dir() -> std::path::PathBuf {
    let root = match datasets_root::sstables_root_for_table(fixture::FIX_KS, fixture::FIX_TABLE) {
        Some(r) => r,
        None => panic!(
            "committed fixture {}.{} not found; {}",
            fixture::FIX_KS,
            fixture::FIX_TABLE,
            datasets_root::describe_search(fixture::FIX_KS, fixture::FIX_TABLE)
        ),
    };
    let ks_dir = root.join(fixture::FIX_KS);
    let prefix = format!("{}-", fixture::FIX_TABLE);
    for e in std::fs::read_dir(&ks_dir)
        .expect("read keyspace dir")
        .flatten()
    {
        if e.path().is_dir() && e.file_name().to_string_lossy().starts_with(&prefix) {
            return e.path();
        }
    }
    panic!("fixture directory not found under {ks_dir:?}");
}

/// Render a `uuid` partition-key value as the hyphenated CQL literal
/// (`8-4-4-4-12`).
///
/// `Display` on `Value` renders `UUID(<hex>)`, which is a DIAGNOSTIC form and not
/// valid CQL. Interpolating it made every query fail to PARSE — and the case then
/// "passed" having compared nothing, which is the vacuous-pass shape this repo's
/// fixture doctrine exists to remove. Measured, not guessed: with `Display` this
/// case reported every one of 100 partitions erroring on the UNFIXED tree.
fn uuid_literal(v: &cqlite_core::types::Value) -> Option<String> {
    let cqlite_core::types::Value::Uuid(bytes) = v else {
        return None;
    };
    let h = hex::encode(bytes);
    Some(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

/// Row set → an ORDER-INSENSITIVE multiset of per-row renderings, each a
/// sorted-by-column-name `Debug` of the row's values (the parent lane's
/// `normalize`, sorted so a subset comparison is order-independent).
fn normalize(rows: &[QueryRow]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let sorted: BTreeMap<&str, String> = row
                .values
                .iter()
                .map(|(k, v)| (k.as_ref(), format!("{v:?}")))
                .collect();
            format!("{sorted:?}")
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn full_read_refuses_a_corrupt_partition_and_neither_arm_fabricates() {
    let schema =
        datasets_root::schema_path(fixture::SCHEMA_FILE).expect("committed CQL schema (#3148)");
    let staged = fixture::stage_control_and_mutated(&fixture_dir(), "ptfull");

    let control = open_db(&staged.control_root, &schema, ReadPathMode::Full).await;
    let discovery = control
        .execute(&format!(
            "SELECT partition_key FROM {}.{}",
            fixture::FIX_KS,
            fixture::FIX_TABLE
        ))
        .await
        .expect("control discovery SELECT")
        .rows;
    let mut keys: Vec<String> = discovery
        .iter()
        .filter_map(|r| r.values.get("partition_key").and_then(uuid_literal))
        .collect();
    keys.sort();
    keys.dedup();
    assert!(
        !keys.is_empty(),
        "0-rows-when-present: the control fixture must yield partition keys"
    );

    let point = open_db(&staged.mutated_root, &schema, ReadPathMode::Point).await;
    let full = open_db(&staged.mutated_root, &schema, ReadPathMode::Full).await;

    let mut full_errored = 0usize;
    let mut named_the_column = false;
    for key in &keys {
        let sql = format!(
            "SELECT * FROM {}.{} WHERE partition_key = {key}",
            fixture::FIX_KS,
            fixture::FIX_TABLE
        );
        let expected = normalize(
            &control
                .execute(&sql)
                .await
                .unwrap_or_else(|e| panic!("the pristine fixture must read partition {key}: {e}"))
                .rows,
        );
        assert!(
            !expected.is_empty(),
            "0-rows-when-present: control partition {key} yielded no rows"
        );

        match full.execute(&sql).await {
            Err(e) => {
                full_errored += 1;
                named_the_column |= e.to_string().contains("clustering_key2");
            }
            Ok(r) => assert_eq!(
                normalize(&r.rows),
                expected,
                "the FULL path answered partition {key} with a set that is not the pristine one"
            ),
        }

        if let Ok(r) = point.execute(&sql).await {
            let got = normalize(&r.rows);
            let fabricated: Vec<&String> = got.iter().filter(|x| !expected.contains(x)).collect();
            assert!(
                fabricated.is_empty(),
                "the POINT path FABRICATED rows for partition {key}: {fabricated:?}"
            );
        }
    }

    assert!(
        full_errored > 0,
        "the corrupt fixture must make the FULL read path refuse at least one of the {} \
         partitions; all of them read cleanly, which is the #3782 silent-truncation shape",
        keys.len()
    );
    assert!(
        named_the_column,
        "the refusal must carry the DECODE error's own kind (naming clustering_key2), not a \
         generic end-of-partition collapse — that preservation is the whole of #3782 AC1"
    );
}
