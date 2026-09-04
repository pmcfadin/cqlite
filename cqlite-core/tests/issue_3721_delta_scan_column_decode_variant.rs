//! Issue #3721 — the delta-scan row loop must not ERASE `Error::ColumnDecode`.
//!
//! # The defect this pins
//!
//! `block_emit.rs`'s `parse_block_emit_delta` row loop answered every row-parse
//! `Err` — `Error::ColumnDecode` included — with
//!
//! ```ignore
//! return Err(Error::corruption(format!(
//!     "delta-scan: row parse error in partition {} at offset {} in {}.{}: {}", ..
//! )));
//! ```
//!
//! This is NOT the swallow the rest of issue #3721 is about: the path errors, and no
//! partial result is served. What it destroyed is the one thing that makes a
//! per-column failure actionable — the MATCHABLE variant. `Error::ColumnDecode`
//! exists precisely so callers can tell a per-column decode failure apart from any
//! other corruption without inspecting message text (which issue #28 forbids), and
//! two of them do: `column_decode_error::is_column_decode` (the
//! end-of-partition-vs-decode-failure decision) and `indexed_walk_falls_back` (the
//! windowed callers' retraction of the index optimization). Wrapping the error in
//! `Error::Corruption` left every caller above this loop with a string.
//!
//! # The oracle
//!
//! REAL Cassandra-written data (`test_signed_coll.signed_int_collections`, BIG
//! `nb`), with only the DECLARED type varied — `m`'s key declared `BIGINT` while the
//! on-disk cell paths are 4-byte `int` keys, so the FIRST element of `m` fails to
//! decode. Identical provocation to
//! `issue_3721_column_decode_error_surface.rs`; what differs is the READ PATH, which
//! is the whole point: `scan_delta` is a public surface that does not reach
//! `Database::execute`, so nothing in that lane could observe this loop.
//!
//! The control is the same schema with `m`'s key declared correctly: the delta scan
//! must complete, so "the error variant survives" is attributable to the one
//! declared type and not to a broken lane.

#![cfg(all(feature = "delta-scan", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::error::ErrorCategory;
use cqlite_core::schema::parse_cql_schema;
use cqlite_core::storage::sstable::reader::delta_scan::scan_delta;
use cqlite_core::Error;

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

const KEYSPACE: &str = "test_signed_coll";
const TABLE: &str = "signed_int_collections";

/// The fixture's real shape — the control.
const CORRECT: &str = "\
CREATE TABLE IF NOT EXISTS test_signed_coll.signed_int_collections (
    id INT PRIMARY KEY,
    s  SET<INT>,
    m  MAP<INT, TEXT>
);
";

/// `m`'s KEY declared `BIGINT` (8 bytes) against 4-byte on-disk `int` cell paths.
const WRONG_KEY: &str = "\
CREATE TABLE IF NOT EXISTS test_signed_coll.signed_int_collections (
    id INT PRIMARY KEY,
    s  SET<INT>,
    m  MAP<BIGINT, TEXT>
);
";

/// Resolve the fixture directory, or FAIL — never skip (issue #3220): the fixture is
/// force-added to git, so absence is a resolution defect.
fn fixture_dir() -> PathBuf {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "{KEYSPACE}.{TABLE} is COMMITTED to git and must resolve in every checkout \
             (issue #3220) — {}.\n  remedy: git restore --source=HEAD -- \
             test-data/datasets/sstables",
            describe_search(KEYSPACE, TABLE)
        )
    });
    let ks = root.join(KEYSPACE);
    std::fs::read_dir(&ks)
        .unwrap_or_else(|e| panic!("read {}: {e}", ks.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .unwrap_or_else(|| panic!("no {TABLE}-* directory under {}", ks.display()))
}

fn schema_for(body: &str) -> cqlite_core::schema::TableSchema {
    let schema = parse_cql_schema(body).expect("parse scratch schema");
    assert_eq!(schema.table, TABLE, "scratch schema must declare {TABLE}");
    schema
}

/// Drain the delta scan, returning the first `Err` it delivers (if any).
async fn drain(body: &str) -> Result<usize, Error> {
    let (mut rx, _summary) = scan_delta(fixture_dir(), schema_for(body), 16);
    let mut delivered = 0usize;
    while let Some(item) = rx.recv().await {
        match item {
            Ok(_) => delivered += 1,
            Err(e) => return Err(e),
        }
    }
    Ok(delivered)
}

/// Control: the correct declaration completes and delivers records, so the lane
/// really exercises the row loop under test.
#[tokio::test]
async fn control_correct_declaration_completes() {
    let delivered = drain(CORRECT)
        .await
        .expect("the delta scan must complete with the fixture's real schema");
    assert!(
        delivered > 0,
        "the control must deliver at least one delta record — otherwise the WRONG_KEY \
         case could 'fail' without the row loop ever running (0-rows-when-present)"
    );
}

/// The subject: the failure must arrive as the MATCHABLE `Error::ColumnDecode`,
/// not as an `Error::Corruption` string that has swallowed the discriminant.
#[tokio::test]
async fn column_decode_variant_survives_the_delta_scan_row_loop() {
    let err = drain(WRONG_KEY)
        .await
        .expect_err("a mis-declared map key must fail the delta scan");

    let Error::ColumnDecode {
        column,
        column_type,
        source,
        ..
    } = &err
    else {
        panic!(
            "the delta-scan row loop must PRESERVE Error::ColumnDecode — a caller that \
             matches on the variant (is_column_decode / indexed_walk_falls_back) can do \
             nothing with a re-wrapped string. Got: {err:?}"
        );
    };
    assert_eq!(column, "m", "the error must name the failing column");
    assert!(
        column_type.to_ascii_lowercase().contains("bigint"),
        "the reported type must name the DISPATCH type the decode ran on; got `{column_type}`"
    );
    assert!(
        !source.to_string().is_empty(),
        "the underlying decode failure must be preserved as the error `source`"
    );
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
}
