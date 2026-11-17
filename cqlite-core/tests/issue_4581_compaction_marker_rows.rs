//! Issue #4581 — compaction of rows whose only content is the primary-key liveness MARKER.
//!
//! Before this fix CQLite's compaction dropped such a row (`INSERT INTO t (pk) VALUES (..)`), and a
//! marker strictly newer than a surviving row deletion lost to the deletion. The writer would also
//! have rewritten an emitted marker-only row as ts=0 with no TTL. The rule is now the one
//! Cassandra 5.0.8 applies, whether a merge serves a read or a compaction:
//!
//! | rule | Cassandra 5.0.8 (`cassandra-5.0.8`) | pinned here by |
//! |---|---|---|
//! | marker-only row is kept, with its real ts/TTL/LDT | `Row.java:778`, `BTreeRow.java:464` | `live_marker`, `ttl_within_grace` |
//! | expired marker inside gc_grace is kept verbatim | `DeletionPurger.java:31-34` | `ttl_within_grace`, `clustered_markers` |
//! | expired marker past gc_grace is purged | `DeletionPurger.java:31-34`, `PurgeFunction.java:40-43` | `ttl_past_grace`, `clustered_markers_gc` |
//! | marker covered by a deletion (`ts <= markedForDeleteAt`) dies BEFORE the deletion is purged | `Row.java:762`, `DeletionTime.java:173` | `partition_del*`, `clustered_markers*` |
//! | marker newer than a REGULAR row deletion keeps BOTH | `Row.java:754` (`isShadowedBy` = shadowable only) | `clustered_markers` pk 2 |
//! | expiring supersedes live-forever at equal ts; later LDT wins | `LivenessInfo.java:219-232` | `ttl_within_grace` pk 3, 4, 5 |
//!
//! ORACLE: Cassandra's OWN output. Every table is written by Cassandra 5.0.8, compacted by
//! `nodetool compact` on the same node, and both the INPUT SSTables and Cassandra's COMPACTED
//! output are committed (`test-data/fixtures/issue_4581`, generator
//! `generate-issue-4581-compaction-marker-rows.sh`, which documents every row). CQLite compacts
//! the Cassandra-written inputs at the instant Cassandra compacted (`compaction-instant.json`) and
//! must reproduce the output: row by row (liveness ts/TTL/expiry, row and partition deletions,
//! cells) and, because every table is uncompressed, BYTE FOR BYTE in Data.db.

#![cfg(all(feature = "state_machine", feature = "write-support", feature = "lz4"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, KWayMerger, MergeEntry, MergeStep, RowData,
};
use cqlite_core::types::Value;

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_markerrows";
const SCHEMA: &str = "issue-4581-compaction-marker-rows.cql";

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4581")
}

fn schema_of(table: &str) -> TableSchema {
    let path = datasets_root::schema_path(SCHEMA).expect("committed CQL schema");
    let cql = std::fs::read_to_string(path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {KEYSPACE}.{table} "))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = KEYSPACE.to_string();
    t
}

fn table_dir(keyspace_dir: &str, table: &str) -> PathBuf {
    let root = fixture_root().join(keyspace_dir);
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("{}: {e}", root.display()))
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{table}-")))
        })
        .collect();
    assert_eq!(
        dirs.len(),
        1,
        "{keyspace_dir}/{table}: expected ONE dir, got {dirs:?}"
    );
    dirs.pop().unwrap()
}

/// Committed `*-Data.db` files of a directory, NEWEST generation first (run index 0 wins ties).
fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut data: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("fixture dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
        .collect();
    assert!(!data.is_empty(), "{}: no committed Data.db", dir.display());
    data.sort_by_key(|p| generation_of(p));
    data.reverse();
    data
}

/// `nb-<gen>-big` of an `nb-<gen>-big-Data.db` path.
fn stem_of(p: &Path) -> String {
    let n = p.file_name().and_then(|n| n.to_str()).expect("file name");
    n.trim_end_matches("-Data.db").to_string()
}

fn assert_bytes_equal(table: &str, component: &str, ours: &Path, cassandra: &Path) {
    let (a, b) = (
        std::fs::read(ours).unwrap_or_else(|e| panic!("{}: {e}", ours.display())),
        std::fs::read(cassandra).unwrap_or_else(|e| panic!("{}: {e}", cassandra.display())),
    );
    let hex = |b: &[u8]| {
        b.iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert!(
        a == b,
        "{table}: {component} differs from Cassandra's compacted output (first diff at byte {:?})\n\
         cqlite    ({} B): {}\ncassandra ({} B): {}",
        a.iter().zip(&b).position(|(x, y)| x != y),
        a.len(),
        hex(&a),
        b.len(),
        hex(&b)
    );
}

/// Whether a table's Data.db/Index.db/Summary.db/Digest are compared byte for byte.
///
/// Only `byte_parity` is: the other tables hit THREE encoding gaps of the compaction writer that
/// have nothing to do with the marker and predate #4581 — (1) the output header lists the schema's
/// regular columns while Cassandra's lists the UNION OF THE INPUT HEADERS' columns
/// (`SerializationHeader.make`, `SerializationHeader.java:92-97`), which flips `HAS_ALL_COLUMNS`
/// on rows of a table whose inputs never wrote a regular column; (2) `HAS_ALL_COLUMNS` is withheld
/// from a row whose cells include a tombstone; (3) the min-TTL encoding baseline excludes an input
/// whose min TTL is 0, where Cassandra's `updateTTL(sstable.getMinTTL())` includes it. Their rows,
/// liveness, deletions and cells are asserted equal regardless.
enum ByteParity {
    Exact,
    Skip(&'static str),
}

fn generation_of(p: &Path) -> u64 {
    p.file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.split('-').nth(1))
        .and_then(|g| g.parse().ok())
        .expect("generation in nb-<gen>-big-Data.db")
}

// ---------------------------------------------------------------------------------------------
// Normalised row model, built from BOTH sides.
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Marker {
    ts_micros: i64,
    ttl: Option<i64>,
    expires_at_secs: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Cell {
    name: String,
    /// `Some(text)` for a live cell, `None` for a tombstone.
    value: Option<String>,
    ts_micros: i64,
    /// Tombstone `localDeletionTime` (seconds).
    ldt_secs: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    marker: Option<Marker>,
    /// `(markedForDeleteAt µs, localDeletionTime s)`.
    deletion: Option<(i64, i64)>,
    cells: Vec<Cell>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Partition {
    deletion: Option<(i64, i64)>,
    /// clustering value (0 for a clustering-less table) -> row.
    rows: BTreeMap<i64, Row>,
}

type Model = BTreeMap<i64, Partition>;

/// `1970-01-01T00:00:00.002500Z` -> µs; `2026-11-16T23:40:46Z` -> µs. sstabledump prints a
/// fractional second only when non-zero.
fn iso_micros(s: &str) -> i64 {
    let dt = chrono::DateTime::parse_from_rfc3339(s).unwrap_or_else(|e| panic!("{s}: {e}"));
    dt.timestamp() * 1_000_000 + i64::from(dt.timestamp_subsec_micros())
}

fn iso_secs(s: &str) -> i64 {
    iso_micros(s) / 1_000_000
}

fn cassandra_model(jsonl: &Path) -> Model {
    let text = std::fs::read_to_string(jsonl).expect("sstabledump jsonl");
    let mut model = Model::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("json line");
        let key: i64 = v["partition"]["key"][0]
            .as_str()
            .expect("partition key")
            .parse()
            .expect("int key");
        let p = model.entry(key).or_default();
        if let Some(d) = v["partition"].get("deletion_info") {
            p.deletion = Some((
                iso_micros(d["marked_deleted"].as_str().unwrap()),
                iso_secs(d["local_delete_time"].as_str().unwrap()),
            ));
        }
        for r in v["rows"].as_array().expect("rows") {
            assert_eq!(r["type"], "row", "only plain rows are expected: {r}");
            let ck = r
                .get("clustering")
                .map_or(0, |c| c[0].as_i64().expect("int clustering"));
            let mut row = Row::default();
            if let Some(l) = r.get("liveness_info") {
                row.marker = Some(Marker {
                    ts_micros: iso_micros(l["tstamp"].as_str().unwrap()),
                    ttl: l.get("ttl").map(|t| t.as_i64().unwrap()),
                    expires_at_secs: l.get("expires_at").map(|e| iso_secs(e.as_str().unwrap())),
                });
            }
            if let Some(d) = r.get("deletion_info") {
                row.deletion = Some((
                    iso_micros(d["marked_deleted"].as_str().unwrap()),
                    iso_secs(d["local_delete_time"].as_str().unwrap()),
                ));
            }
            let row_ts = row.marker.as_ref().map(|m| m.ts_micros);
            for c in r["cells"].as_array().expect("cells") {
                // sstabledump omits a cell's `tstamp` when it equals the row's liveness timestamp.
                let ts_micros = c
                    .get("tstamp")
                    .map(|t| iso_micros(t.as_str().unwrap()))
                    .or(row_ts)
                    .expect("cell timestamp");
                let ldt_secs = c
                    .get("deletion_info")
                    .map(|d| iso_secs(d["local_delete_time"].as_str().unwrap()));
                row.cells.push(Cell {
                    name: c["name"].as_str().unwrap().to_string(),
                    value: c.get("value").map(|x| x.as_str().unwrap().to_string()),
                    ts_micros,
                    ldt_secs,
                });
            }
            p.rows.insert(ck, row);
        }
    }
    model
}

fn pk_of(e: &MergeEntry) -> i64 {
    let b: [u8; 4] = e.key.key.as_slice().try_into().expect("int partition key");
    i64::from(i32::from_be_bytes(b))
}

fn cqlite_model(data: &Path, schema: &TableSchema) -> Model {
    let mut merger = KWayMerger::new(vec![data.to_path_buf()], schema).expect("reopen output");
    let mut model = Model::new();
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => return model,
            MergeStep::Partition { rows, .. } => {
                for e in rows {
                    let p = model.entry(pk_of(&e)).or_default();
                    if let Some(d) = e.partition_deletion {
                        p.deletion = Some((d.0, i64::from(d.1)));
                        continue;
                    }
                    let ck = e
                        .clustering_key
                        .as_ref()
                        .map_or(0, |k| match &k.columns[0].1 {
                            Value::Integer(i) => i64::from(*i),
                            other => panic!("clustering value {other:?}"),
                        });
                    let ck_names: Vec<String> = e
                        .clustering_key
                        .as_ref()
                        .map(|k| k.columns.iter().map(|(n, _)| n.clone()).collect())
                        .unwrap_or_default();
                    let mut row = Row::default();
                    let lv = e.row_liveness;
                    if lv.has_marker {
                        row.marker = Some(Marker {
                            ts_micros: lv.marker_timestamp.expect("marker timestamp"),
                            ttl: lv.ttl_seconds.map(i64::from),
                            expires_at_secs: lv.expires_at_seconds,
                        });
                    }
                    row.deletion = e.row_deletion.map(|(t, l)| (t, i64::from(l)));
                    match e.row_data {
                        RowData::Tombstone {
                            deletion_time,
                            local_deletion_time,
                        } => row.deletion = Some((deletion_time, i64::from(local_deletion_time))),
                        RowData::Live { cells } => {
                            for c in cells.into_iter().filter(|c| !ck_names.contains(&c.column)) {
                                let (value, ldt_secs) = match &c.value {
                                    Value::Tombstone(t) => (None, Some(t.local_deletion_time)),
                                    Value::Text(s) => {
                                        (Some(String::from_utf8_lossy(s).into_owned()), None)
                                    }
                                    other => panic!("unexpected cell value {other:?}"),
                                };
                                row.cells.push(Cell {
                                    name: c.column,
                                    value,
                                    ts_micros: c.timestamp,
                                    ldt_secs,
                                });
                            }
                        }
                    }
                    p.rows.insert(ck, row);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The oracle comparison.
// ---------------------------------------------------------------------------------------------

struct Instant {
    now_secs: i64,
    gc_grace_seconds: i64,
}

fn compaction_instant(dir: &Path) -> Instant {
    let v: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("compaction-instant.json")).expect("instant"),
    )
    .expect("json");
    Instant {
        now_secs: v["compaction_now_secs"].as_i64().expect("now"),
        gc_grace_seconds: v["gc_grace_seconds"].as_i64().expect("gc_grace"),
    }
}

/// Compact the committed Cassandra-written inputs of `table` and compare with the Cassandra-
/// compacted output. Returns the model so a caller can pin table-specific facts too.
async fn compare_with_cassandra(
    table: &str,
    expected_marker_only: usize,
    bytes: ByteParity,
) -> Model {
    let schema = schema_of(table);
    let inputs = data_files(&table_dir(KEYSPACE, table));
    let cass_dir = table_dir(&format!("{KEYSPACE}_compacted"), table);
    let cass_data = data_files(&cass_dir);
    assert_eq!(
        cass_data.len(),
        1,
        "{table}: Cassandra's output is ONE SSTable"
    );
    let at = compaction_instant(&cass_dir);

    let out = tempfile::tempdir().expect("tempdir");
    let report = compact_sstables(
        inputs,
        out.path(),
        &schema,
        9,
        Some(at.now_secs - at.gc_grace_seconds),
        Some(at.now_secs),
        true,
    )
    .await
    .expect("compaction");
    let ours = cqlite_model(&report.output.data_path, &schema);
    let theirs = cassandra_model(&PathBuf::from(format!("{}.jsonl", cass_data[0].display())));

    assert_eq!(
        ours, theirs,
        "{table}: CQLite's compaction differs from `nodetool compact`"
    );
    let marker_only = theirs
        .values()
        .flat_map(|p| p.rows.values())
        .filter(|r| r.marker.is_some() && r.cells.is_empty())
        .count();
    assert_eq!(
        marker_only, expected_marker_only,
        "{table}: the oracle must hold the marker-only rows this test is about"
    );

    match bytes {
        ByteParity::Exact => {
            // Uncompressed on both sides: Data.db, Index.db, Summary.db and Digest.crc32 are
            // compared byte for byte (the Statistics.db / Filter.db bookkeeping is not, as in
            // `issue_1387_*`).
            for component in ["Data.db", "Index.db", "Summary.db", "Digest.crc32"] {
                let ours = report
                    .output
                    .data_path
                    .with_file_name(format!("{}-{component}", stem_of(&report.output.data_path)));
                let cass =
                    cass_data[0].with_file_name(format!("{}-{component}", stem_of(&cass_data[0])));
                assert_bytes_equal(table, component, &ours, &cass);
            }
        }
        ByteParity::Skip(_why) => {}
    }
    theirs
}

/// A live marker-only row (pk 1, 2, 7), a live TTL'd marker (pk 5: ttl 864000, LDT kept verbatim),
/// a marker older than a cell (pk 6: marker@1000, cell@2000 — NOT promoted to 2000), a marker
/// beside a cell tombstone (pk 4), and a plain row (pk 3).
#[tokio::test]
async fn live_marker_only_rows_are_kept_with_their_real_liveness() {
    let m = compare_with_cassandra(
        "live_marker",
        4,
        ByteParity::Skip("cell tombstone HAS_ALL_COLUMNS, TTL baseline"),
    )
    .await;
    let ttl_row = &m[&5].rows[&0];
    let marker = ttl_row.marker.as_ref().expect("marker");
    assert_eq!((marker.ts_micros, marker.ttl), (1000, Some(864_000)));
    assert!(
        marker.expires_at_secs.is_some(),
        "the marker's LDT is preserved"
    );
    assert_eq!(m[&6].rows[&0].marker.as_ref().unwrap().ts_micros, 1000);
    assert_eq!(m[&2].rows[&0].marker.as_ref().unwrap().ts_micros, 2000);
}

/// Expired markers inside gc_grace are kept VERBATIM (ts/TTL/LDT), not turned into tombstones, and
/// the `LivenessInfo.supersedes` ties resolve as Cassandra does (an expiring marker beats a
/// live-forever one at an equal timestamp; of two expiring ones the later LDT wins).
#[tokio::test]
async fn expired_markers_inside_gc_grace_are_kept_verbatim() {
    let m = compare_with_cassandra("ttl_within_grace", 5, ByteParity::Skip("TTL baseline")).await;
    assert_eq!(m[&3].rows[&0].marker.as_ref().unwrap().ttl, Some(5));
    for pk in [1, 4, 5] {
        assert_eq!(
            m[&pk].rows[&0].marker.as_ref().unwrap().ttl,
            Some(3),
            "pk {pk}"
        );
    }
    assert_eq!(
        m[&2].rows[&0].marker.as_ref().unwrap().ttl,
        None,
        "newer live-forever wins"
    );
}

/// Expired markers past gc_grace are purged (pk 1, and pk 3 with its cell); a marker outlives its
/// purged cell tombstone (pk 4).
#[tokio::test]
async fn expired_markers_past_gc_grace_are_purged() {
    let m = compare_with_cassandra("ttl_past_grace", 2, ByteParity::Exact).await;
    assert_eq!(m.keys().copied().collect::<Vec<_>>(), vec![2, 4]);
    assert!(
        m[&4].rows[&0].cells.is_empty(),
        "the cell tombstone is purged, the marker stays"
    );
}

/// A partition deletion at an EQUAL timestamp deletes the marker (pk 1, 4); an older marker is
/// deleted (pk 3); a strictly newer marker survives beside the partition deletion (pk 2).
#[tokio::test]
async fn partition_deletion_vs_marker() {
    let m = compare_with_cassandra("partition_del", 2, ByteParity::Skip("header columns")).await;
    for pk in [1, 3, 4] {
        assert!(
            m[&pk].deletion.is_some() && m[&pk].rows.is_empty(),
            "pk {pk}"
        );
    }
    assert!(m[&2].deletion.is_some() && m[&2].rows[&0].marker.is_some());
}

/// The partition deletion is purged — AFTER it shadowed the marker, so no ghost row is left (pk 1).
#[tokio::test]
async fn partition_deletion_is_purged_after_shadowing_the_marker() {
    let m = compare_with_cassandra("partition_del_gc", 1, ByteParity::Skip("header columns")).await;
    assert_eq!(m.keys().copied().collect::<Vec<_>>(), vec![2]);
}

/// ROW deletions (clustered table): pk 1 — the deletion covers the older marker; pk 2 — a marker
/// strictly NEWER than the row deletion keeps the row live AND Cassandra keeps the (regular,
/// non-shadowable) deletion too; pk 4 — an expired marker inside grace.
#[tokio::test]
async fn row_deletion_vs_marker() {
    let m =
        compare_with_cassandra("clustered_markers", 3, ByteParity::Skip("header columns")).await;
    assert!(m[&1].rows[&1].marker.is_none() && m[&1].rows[&1].deletion.is_some());
    let r = &m[&2].rows[&1];
    assert!(
        r.marker.is_some() && r.deletion.is_some(),
        "newer marker AND the deletion survive"
    );
}

/// A row deletion that is purged AFTER shadowing its marker leaves nothing (pk 1); a newer marker
/// outlives its purged older deletion (pk 2); an expired marker past grace removes the whole
/// clustered key-only row (pk 3).
#[tokio::test]
async fn purged_row_deletion_leaves_no_ghost_row() {
    let m = compare_with_cassandra(
        "clustered_markers_gc",
        2,
        ByteParity::Skip("header columns"),
    )
    .await;
    assert_eq!(m.keys().copied().collect::<Vec<_>>(), vec![2, 4]);
}

/// BYTE parity of the marker-only row encoding itself: two marker-only rows (one reconciled across
/// generations, marker@1000 vs @2000) beside one row that writes `v`.
#[tokio::test]
async fn marker_only_rows_are_byte_identical_to_cassandras_compaction() {
    let m = compare_with_cassandra("byte_parity", 2, ByteParity::Exact).await;
    assert_eq!(m[&2].rows[&0].marker.as_ref().unwrap().ts_micros, 2000);
}

// ---------------------------------------------------------------------------------------------
// MarkerMode (#4581, audit caveats 1 and 2): the compaction path is authoritative about the
// marker, INCLUDING "no marker". Cassandra serializes `LivenessInfo.EMPTY` (no HAS_TIMESTAMP,
// `UnfilteredSerializer.serializeRowBody` sets the flags only when `!pkLiveness.isEmpty()`) for a
// row that never had a marker (UPDATE) and for one whose marker was purged (`BTreeRow.purge`).
// ---------------------------------------------------------------------------------------------

/// UPDATE-created rows carry NO marker and compaction must not derive one from the cells: pk 1
/// (plain UPDATE across two generations), pk 2 (live `UPDATE ... USING TTL 864000`: the cell keeps
/// its ts/TTL/LDT and there is no marker for it to outlive), pk 3 (the cell EXPIRED within grace:
/// Cassandra turns it into a cell tombstone and writes still no marker), pk 4 (INSERT control: the
/// marker@1000 stays beside the cell@2000).
#[tokio::test]
async fn update_created_rows_get_no_derived_marker() {
    let m = compare_with_cassandra(
        "update_rows",
        0,
        // The rows are equal; the bytes differ only in the two pre-existing TTL encoding gaps (the
        // live TTL cell's timestamp/LDT baseline, and the expired cell's tombstone
        // `HAS_ALL_COLUMNS`/timestamp), never in the row flags: no `HAS_TIMESTAMP`. The TTL-free
        // `update_rows_gc` and `purged_marker` tables ARE byte-exact.
        ByteParity::Skip("TTL baseline, expired-cell tombstone encoding"),
    )
    .await;
    for pk in [1, 2, 3] {
        assert!(
            m[&pk].rows[&0].marker.is_none(),
            "pk {pk}: UPDATE row has no marker"
        );
    }
    let ttl_cell = &m[&2].rows[&0].cells[0];
    assert_eq!(ttl_cell.value.as_deref(), Some("b"));
    assert!(
        m[&3].rows[&0].cells[0].value.is_none(),
        "pk 3: expired cell -> tombstone"
    );
    assert_eq!(m[&4].rows[&0].marker.as_ref().unwrap().ts_micros, 1000);
}

/// Past gc_grace an expired `UPDATE ... USING TTL` leaves NO ghost row (pk 1: no cell, no marker
/// derived from the dead cell's TTL); the plain UPDATE (pk 2) and the TTL UPDATE overwritten by a
/// newer plain UPDATE (pk 3) are marker-less rows holding just their cell.
#[tokio::test]
async fn expired_update_ttl_row_leaves_no_ghost_marker() {
    let m = compare_with_cassandra("update_rows_gc", 0, ByteParity::Exact).await;
    assert_eq!(m.keys().copied().collect::<Vec<_>>(), vec![2, 3]);
    for pk in [2, 3] {
        let r = &m[&pk].rows[&0];
        assert!(r.marker.is_none() && r.cells.len() == 1, "pk {pk}: {r:?}");
    }
}

/// `INSERT ... USING TTL` then a plain `UPDATE`, compacted past gc_grace: the expired marker is
/// PURGED, the data survives, and the output row has NO marker (pk 1 with data in the INSERT, pk 2
/// with a marker-only INSERT); an expired marker-only row vanishes (pk 3); a live marker-only row
/// stays (pk 4).
#[tokio::test]
async fn purged_marker_beside_surviving_data_is_not_rederived() {
    let m = compare_with_cassandra("purged_marker", 1, ByteParity::Exact).await;
    assert_eq!(m.keys().copied().collect::<Vec<_>>(), vec![1, 2, 4]);
    for pk in [1, 2] {
        let r = &m[&pk].rows[&0];
        assert!(
            r.marker.is_none(),
            "pk {pk}: the purged marker is not re-derived"
        );
        assert_eq!(r.cells[0].value.as_deref(), Some("n"));
        assert_eq!(r.cells[0].ts_micros, 2000);
    }
}
