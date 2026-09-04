//! Issue #3809 Finding 1 — a ROW DELETION must never be emitted having LOST the
//! clustering identity of the row it deletes.
//!
//! ## The property
//!
//! On the per-element compaction read path a pure row tombstone is surfaced as
//! `CompactionRowData::Tombstone { clustering }`, where `clustering` is rebuilt in
//! schema order from the clustering pseudo-cells the row decoder surfaces (#912).
//! When a clustering column's pseudo-cell was NOT recovered, the builder used to
//! `clustering.clear()` and emit `clustering: []` — SILENTLY, on the WRITE path.
//!
//! An EMPTY clustering is not a harmless degradation there: downstream
//! `extract_clustering_key_from_compaction` maps it to `None`, so the row joins
//! the `None` reconcile bucket — the bucket that ALSO holds the partition's
//! STATIC row — its `deletion_time` becomes that whole group's row deletion, and
//! every cell in the bucket at or below that timestamp is shadowed. The row
//! deletion then either disappears (resurrecting the row it was meant to delete)
//! or is written with no clustering prefix and no `IS_STATIC` flag: a structurally
//! invalid row on a clustered table.
//!
//! The same is true of a row deletion that COEXISTS with cells that survived it
//! (issue #932, emitted as `Live { row_deletion: Some(..) }`): that arm's
//! clustering key is read from the row's `simple` cells and an incomplete one maps
//! to `None` identically, so the invariant covers EVERY non-static row carrying a
//! row deletion (review round 2 of this issue). A row with NO row deletion is
//! deliberately outside it — see the boundary note at the call site in
//! `row_decoder/compaction.rs`.
//!
//! ## Cassandra authority (pinned `cassandra-5.0.8` — never a working tree)
//!
//! A clustering prefix is ARITY-TOTAL, so "fewer values than the table declares"
//! is not a shape Cassandra can write:
//!
//! * `db/Clustering.java` — `Serializer.serialize` asserts
//!   `clustering.size() == types.size()`; `deserialize` reads EXACTLY
//!   `types.size()` values.
//! * `db/rows/UnfilteredSerializer.java` — `deserializeTombstonesOnly` builds
//!   `BTreeRow.emptyDeletedRow(clustering, deletion)`, i.e. a row tombstone always
//!   carries a FULL clustering. A row carries ONE clustering prefix, written
//!   before `row_size`, the `deletion` VInt pair (flag `0x10`) and the cell data
//!   (field order tabulated in
//!   `docs/sstables-definitive-guide/chapters/05-data-db-format.md`, "Row
//!   Structure"), so a row whose deletion coexists with surviving cells carries
//!   the same FULL clustering.
//!
//! So a short clustering on a non-static clustered row is malformed input, and the
//! only faithful response is a REFUSAL.
//!
//! ## The two cases where an EMPTY clustering is CORRECT (and must not red)
//!
//! * A **STATIC row**: `db/Clustering.java` distinguishes `Clustering.EMPTY` from
//!   `Clustering.STATIC_CLUSTERING` by `kind()` (`Clustering.java:102,124`) — a
//!   distinction CQLite's `Vec<(String, Value)>` cannot express, so `[]` is how a
//!   static row's clustering is represented here.
//! * A table with **no clustering columns**: `[]` is the complete and only
//!   clustering such a table has.
//!
//! Both are asserted in the in-crate scalar tests named in the next section (NOT
//! in this file), because a guard that reds on correct input is the guard that
//! gets waived.
//!
//! ## What THIS file covers, and what it does not
//!
//! The refusal decision is `CompactionRowData::require_tombstone_clustering_identity`
//! — `pub(crate)` (#3366), called by the private per-element builder
//! (`row_decoder/compaction.rs`). Its SCALAR cases (the refusal, the two exempt
//! shapes, the accepted arities) are therefore pinned IN-CRATE, in
//! `cqlite-core/src/storage/sstable/reader/compaction_row_tombstone_identity_tests.rs`,
//! and WHICH ARMS CALL IT — the pure tombstone, the #932 coexistence, and the
//! unvalidated no-row-deletion boundary — in
//! `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/compaction_build_identity_tests.rs`
//! (a child module of the builder, which is private).
//!
//! This file holds the BYTE-LEVEL case only: real Cassandra-written bytes must
//! keep their clustering identity through the compaction read, and the guard must
//! not red on them. It cannot drive the guard to FIRE, because the decoder's
//! `parse_clustering_prefix` is itself arity-total (it pushes exactly
//! `schema.clustering_keys.len()` values or returns `Err`), so a non-static
//! clustered row can only fail EARLIER — the guard is defence in depth at the
//! point where the loss would become an emitted row.
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=/data/datasets cargo test -p cqlite-core \
//!   --features cli-helpers --test issue_3809_tombstone_clustering_identity
//! ```

#[cfg(feature = "cli-helpers")]
#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_tomb";
const TABLE: &str = "static_with_tombstones";

// ===========================================================================
// End-to-end on REAL Cassandra-written bytes: the guard must not red, and the
// clustering identity must survive the compaction read.
// ===========================================================================

#[cfg(feature = "cli-helpers")]
mod fixture {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::platform::Platform;
    use cqlite_core::schema::TableSchema;
    use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::types::Value;
    use cqlite_core::Config;

    use super::{datasets_root, KEYSPACE, TABLE};

    /// `test_tomb.static_with_tombstones` (`test-data/schemas/tombstone-parity.cql`
    /// Table 8, real Apache Cassandra 5.0 `nb`, single flush) holds ONE partition
    /// `pk=1` whose `ck=2` row is a PURE row tombstone. Its committed sstabledump
    /// golden (`nb-1-big-Data.db.jsonl`) reads:
    ///
    /// ```json
    /// {"type": "row", "clustering": [2],
    ///  "deletion_info": {"marked_deleted": "2021-01-02T00:00:00Z", ...},
    ///  "cells": []}
    /// ```
    ///
    /// so the clustering identity Cassandra wrote is `[2]` — NOT empty — and
    /// `markedForDeleteAt` is `2021-01-02T00:00:00Z`.
    const TOMBSTONED_CK: i32 = 2;
    /// `2021-01-02T00:00:00Z` in microseconds (epoch second 1_609_545_600).
    const MARKED_DELETED_MICROS: i64 = 1_609_545_600_000_000;

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cqlite-core has a workspace parent")
            .to_path_buf()
    }

    /// Resolved PER TABLE (#3220). The fixture is a COMMITTED binary, so an absent
    /// one is a broken checkout and FAILS — it is never a skip.
    fn fixture_root() -> PathBuf {
        datasets_root::sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
            panic!(
                "no candidate root holds the COMMITTED fixture {KEYSPACE}.{TABLE}; \
                 candidates: {}",
                datasets_root::describe_roots()
            )
        })
    }

    fn table_dir() -> PathBuf {
        let ks_dir = fixture_root().join(KEYSPACE);
        std::fs::read_dir(&ks_dir)
            .unwrap_or_else(|e| panic!("fixture keyspace dir unreadable {ks_dir:?}: {e}"))
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| {
                p.is_dir()
                    && p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
            })
            .unwrap_or_else(|| panic!("committed fixture dir {TABLE}-* absent under {ks_dir:?}"))
    }

    /// The pure row tombstone Cassandra wrote for `ck=2` must reach the compaction
    /// read carrying its FULL clustering identity, and the #3809 guard must not red
    /// on this well-formed partition.
    ///
    /// # What this case does NOT cover (#3809 review)
    ///
    /// It gives the `is_static` exemption ZERO byte-level coverage. This
    /// partition's static row is a LIVE `static_block` (golden:
    /// `{"type":"static_block","cells":[{"name":"stat_col","value":
    /// "surviving_static",...}]}`, no `deletion_info`), so it is built as
    /// `CompactionRowData::Live` and never reaches the tombstone branch the guard
    /// sits in. Nor is that gap fixable from this corpus: of the 166 committed
    /// `*-Data.db.jsonl` goldens, 9 carry a `static_block` and NONE carries one
    /// with `deletion_info` — consistent with CQL offering no way to write a
    /// row-level deletion of the static row (a `DELETE ... WHERE pk=?` is a
    /// PARTITION deletion). So the `is_static` exemption is pinned at the SCALAR
    /// level only, in `compaction_row_tombstone_identity_tests.rs`, and a
    /// contrived non-Cassandra fixture was deliberately NOT synthesised for it
    /// (#3042: an oracle for an on-disk shape must be Cassandra-written bytes).
    ///
    /// Oracle: the committed Cassandra 5.0.8 `sstabledump` golden quoted above
    /// (`clustering: [2]`, `cells: []`), plus `Clustering.java`'s arity assert.
    /// Every expectation is derived from Cassandra output, never from CQLite's own
    /// prior behaviour.
    #[tokio::test]
    async fn a_cassandra_written_row_tombstone_keeps_its_clustering_identity() {
        let res = ingest(IngestionConfig {
            schema_paths: vec![workspace_root()
                .join("test-data")
                .join("schemas")
                .join("tombstone-parity.cql")],
            data_dir: fixture_root(),
            version_hint: None,
            core_config: Config::default(),
            table_directory_filter: Some(KEYSPACE.to_string()),
        })
        .await
        .expect("ingest of the committed tombstone-parity schema must succeed");
        let schemas: Vec<TableSchema> = res
            .schema_registry
            .read()
            .await
            .list_schemas(None)
            .await
            .expect("list_schemas");
        let schema = schemas
            .into_iter()
            .find(|s| s.table == TABLE)
            .unwrap_or_else(|| panic!("the committed CQL declares {TABLE}"));
        assert_eq!(
            schema.clustering_keys.len(),
            1,
            "the fixture table declares exactly one clustering column (ck)"
        );

        let data_path = std::fs::read_dir(table_dir())
            .expect("fixture dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .expect("committed Data.db");
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("open the committed fixture");

        let rows = reader
            .iterate_all_partitions_for_compaction(Some(&schema))
            .await
            .expect(
                "the compaction read of a well-formed Cassandra fixture must NOT be \
                 refused — the #3809 guard may only fire on malformed input",
            );
        assert!(!rows.is_empty(), "the fixture has one partition with rows");

        let tombstones: Vec<&CompactionRowData> = rows
            .iter()
            .map(|r| &r.row_data)
            .filter(|d| matches!(d, CompactionRowData::Tombstone { .. }))
            .collect();
        assert_eq!(
            tombstones.len(),
            1,
            "the golden holds exactly one pure row tombstone (ck=2); got {tombstones:?}"
        );
        match tombstones[0] {
            CompactionRowData::Tombstone {
                clustering,
                deletion_time,
                ..
            } => {
                assert_eq!(
                    clustering.as_slice(),
                    [("ck".to_string(), Value::Integer(TOMBSTONED_CK))],
                    "the tombstone must carry the clustering Cassandra wrote for it \
                     (sstabledump golden: clustering [2]); an empty clustering is the \
                     #3809 identity loss"
                );
                assert_eq!(
                    *deletion_time, MARKED_DELETED_MICROS,
                    "markedForDeleteAt must match the golden's 2021-01-02T00:00:00Z"
                );
            }
            other => panic!("filtered to Tombstone, got {other:?}"),
        }
    }
}
