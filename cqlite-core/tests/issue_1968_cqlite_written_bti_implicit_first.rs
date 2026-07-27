//! Issue #1968's IMPLICIT-FIRST-BLOCK branch, exercised end-to-end over a
//! **CQLite-written** BTI `Rows.db` (roborev finding on issue #3002).
//!
//! ## Why this lane exists
//!
//! `resolve_bti_clustering_seek_window` treats a `None` from `rows_floor_block` as
//! the implicit-first-block signal and decodes from the partition body start
//! (`body_start_rel = 0`), so the earliest clustering rows are never dropped.
//!
//! With the #3002 root-base fix, a **Cassandra-written** trie can no longer reach
//! that branch: `RowIndexWriter.add` indexes block 0 under `ByteComparable.EMPTY`,
//! parked as the ROOT node's own payload, and nothing sorts below the empty key — so
//! every bound floors to a STORED block-0 entry. The `test_da/wide_table` lane
//! (`issue_1968_bti_open_lower_bound.rs`) therefore now covers the STORED-floor path
//! for every one of its queries.
//!
//! The `None` branch is nevertheless load-bearing, because it is the branch a
//! **CQLite-written** row index still takes: CQLite's `RowsTrieWriter` emits
//! `block_count` separators whose FIRST is the first row's real clustering key (a
//! NON-empty separator), and fail-closed refuses the empty one — the known write gap
//! tracked as issue #3045. A bound sorting below that first separator floors to
//! `None`. Without this lane the branch that keeps CQLite's own output readable would
//! have no read-path coverage at all — only synthetic unit-level floor walks.
//!
//! ## What it proves
//!
//! 1. PREMISE (asserted, not assumed): the emitted `Rows.db`'s first separator is
//!    NON-empty, and the floor walk over the emitted root really returns `None` for
//!    the open-lower sentinel — i.e. these queries take the `None` branch.
//! 2. WIRING: through `Database::execute` (→ `SelectExecutor` →
//!    `scan_partition_clustering` → `bti_clustering_row_window`), an open lower bound
//!    (`ck < N`) and a closed lower bound BELOW the first separator return EXACTLY
//!    the matching rows, in ascending clustering order, and report
//!    `AccessPath::ClusteringSlice` — a narrowed window rooted at rel 0, never the
//!    unnarrowed `PartitionLookup` fallback and never a dropped first block.
//!
//! The fixture is written by this test (no dataset dependency), so there is nothing
//! to skip: a 0-row read here is always a failure.

#![cfg(all(
    feature = "write-support",
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::collections::HashMap;
use std::path::Path;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::QueryRow;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::bti::{
    iterate_rows_in_bti_trie, lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry,
    rows_floor_block_for_test, BtiPartitionLocation,
};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "test_1968w";
const TBL: &str = "wide";
/// Clustering rows in the wide partition (ck = 0..WIDE_ROWS-1).
const WIDE_ROWS: i32 = 200;
/// ~2 KiB per row, so WIDE_ROWS rows span several 64 KiB row-index blocks (the
/// row index must hold >= 2 blocks or there is no window to narrow).
const PAYLOAD_LEN: usize = 2048;
/// The wide partition key; `NARROW_PK` stays a direct `DataOffset` leaf.
const WIDE_PK: i32 = 1;
const NARROW_PK: i32 = 2;

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  payload text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
    )
}

fn table_schema() -> TableSchema {
    let col = |name: &str, ty: &str| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: name == "payload",
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![col("pk", "int"), col("ck", "int"), col("payload", "text")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn row(pk: i32, ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::text("x".repeat(PAYLOAD_LEN)),
        }],
        ts,
        None,
    )
}

/// Write a `da`-format BTI SSTable into `data_dir/<ks>/<table>/`: one WIDE partition
/// (a real multi-block `Rows.db` row index) plus one narrow partition. Returns the
/// emitted `Rows.db` bytes and `Partitions.db` bytes for the premise assertions.
async fn write_bti_fixture(data_dir: &Path) -> (Vec<u8>, Vec<u8>) {
    let schema = table_schema();
    let mut writer =
        SSTableWriter::with_format(data_dir.to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .expect("BTI writer");

    let mut partitions: Vec<(i32, Vec<i32>)> =
        vec![(WIDE_PK, (0..WIDE_ROWS).collect()), (NARROW_PK, vec![0])];
    // The writer requires partitions in token order.
    partitions.sort_by_key(|(pk, _)| {
        row(*pk, 0, 1)
            .decorated_key(&schema)
            .expect("decorated key")
            .token
    });
    for (pk, cks) in &partitions {
        let muts: Vec<Mutation> = cks
            .iter()
            .map(|ck| row(*pk, *ck, 1_000_000 + *ck as i64))
            .collect();
        let key = muts[0].decorated_key(&schema).expect("decorated key");
        writer.write_partition(key, muts).expect("write partition");
    }
    let info = writer.finish().await.expect("finish BTI SSTable");

    let rows_db =
        std::fs::read(info.rows_path.clone().expect("Rows.db path")).expect("read Rows.db");
    let partitions_db = std::fs::read(info.partitions_path.clone().expect("Partitions.db path"))
        .expect("read Partitions.db");
    assert!(
        !rows_db.is_empty(),
        "fixture invariant: a {WIDE_ROWS}-row partition must produce a non-empty Rows.db"
    );
    (rows_db, partitions_db)
}

/// PREMISE: the emitted row index really drives the `None` (implicit-first) branch.
///
/// Asserted, never assumed — it is the whole reason this lane covers a different
/// branch than the Cassandra-fixture lane. If CQLite's writer ever adopts Cassandra's
/// canonical `ByteComparable.EMPTY` block-0 separator (issue #3045), this fails
/// LOUDLY: the branch would then need a synthetic non-empty-first-separator trie to
/// stay covered, rather than silently losing its only read-path test again.
fn assert_floor_walk_takes_the_implicit_first_branch(rows_db: &[u8], partitions_db: &[u8]) {
    let mut cur = std::io::Cursor::new(partitions_db.to_vec());
    let location = lookup_raw_key_in_bti_partitions_db(&mut cur, &WIDE_PK.to_be_bytes())
        .expect("Partitions.db lookup must succeed")
        .expect("the wide partition key must be present");
    let rows_offset = match location {
        BtiPartitionLocation::RowsOffset(off) => off as usize,
        BtiPartitionLocation::DataOffset(off) => {
            panic!("the wide partition must carry a RowsOffset, got DataOffset({off})")
        }
    };
    let header = resolve_rows_db_entry(rows_db, rows_offset).expect("row-index entry must resolve");
    let root = header
        .require_trie_root()
        .expect("issue #3002: the written root must pass structural validation");
    assert!(
        header.block_count >= 2,
        "fixture invariant: the wide partition must span >= 2 row-index blocks (got {}), \
         else there is no window to narrow",
        header.block_count
    );

    let entries = iterate_rows_in_bti_trie(rows_db, root).expect("traverse the written row index");
    assert!(
        !entries.is_empty() && !entries[0].0.is_empty(),
        "PREMISE: CQLite's writer must emit a NON-empty first separator (issue #3045); \
         a Cassandra-canonical empty block-0 separator would make the floor walk return \
         a STORED block-0 entry instead, moving this lane off the implicit-first branch — \
         re-point it at a synthetic non-empty-first-separator trie if that lands"
    );
    assert_eq!(
        rows_floor_block_for_test(rows_db, root, b"").expect("floor walk must succeed"),
        None,
        "PREMISE: the open-lower sentinel must floor to `None` over a CQLite-written \
         row index — that `None` IS the #1968 implicit-first signal the queries below \
         exercise"
    );
}

/// Write the fixture, then open it through the production ingestion path.
async fn setup() -> (TempDir, Database) {
    let temp = TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let schema_path = temp.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    let (rows_db, partitions_db) = write_bti_fixture(&data_dir).await;
    assert_floor_walk_takes_the_implicit_first_branch(&rows_db, &partitions_db);

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest the CQLite-written BTI fixture");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "the fixture schema must load"
    );
    (temp, result.database)
}

fn cks(rows: &[QueryRow]) -> Vec<i32> {
    rows.iter()
        .map(|r| match r.values.get("ck") {
            Some(Value::Integer(v)) => *v,
            other => panic!("ck decoded as {other:?}"),
        })
        .collect()
}

/// Run one slice and return `(returned cks, access path)`, asserting ascending
/// clustering order before any sorting (an out-of-order window stitch must not be
/// hidden by the comparison).
async fn slice(db: &Database, where_clause: &str) -> (Vec<i32>, Option<AccessPath>) {
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE {where_clause}"
        ))
        .await
        .unwrap_or_else(|e| panic!("`{where_clause}` must succeed: {e}"));
    let got = cks(&res.rows);
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "`{where_clause}` must return rows in strictly ascending ck order; got {got:?}"
    );
    (got, res.metadata.access_path.clone())
}

/// Issue #1968 over a CQLite-WRITTEN row index: an OPEN lower bound (`ck < N`) sorts
/// below the non-empty first separator, so the floor walk yields `None` and the decode
/// must start at the partition body — returning ck=0..N-1 through a NARROWED window
/// (`ClusteringSlice`), never dropping the first block and never falling back to an
/// unnarrowed partition read.
#[tokio::test]
async fn cqlite_written_open_lower_bound_keeps_implicit_first_block() {
    let (_temp, db) = setup().await;

    // Anti-vacuous: the written partition must read back in full first.
    let full = db
        .execute(&format!(
            "SELECT pk, ck FROM {KS}.{TBL} WHERE pk = {WIDE_PK}"
        ))
        .await
        .expect("full partition read must succeed");
    assert_eq!(
        full.rows.len(),
        WIDE_ROWS as usize,
        "fixture invariant: pk={WIDE_PK} must hold {WIDE_ROWS} clustering rows (0 rows is a \
         read-path FAILURE)"
    );

    for upper in [3i32, 20, 64] {
        let (got, path) = slice(&db, &format!("pk = {WIDE_PK} AND ck < {upper}")).await;
        assert_eq!(
            got,
            (0..upper).collect::<Vec<i32>>(),
            "Issue #1968 (CQLite-written row index): `ck < {upper}` must return ck=0..={} — \
             the implicit-first `None` branch must decode from the partition body start, or \
             the earliest clustering rows are dropped",
            upper - 1
        );
        assert_eq!(
            path,
            Some(AccessPath::ClusteringSlice),
            "Issue #1968 (CQLite-written row index): `ck < {upper}` must still engage a \
             NARROWED clustering window (the implicit-first branch keeps an END-narrowed \
             window rooted at rel 0), not the unnarrowed PartitionLookup fallback"
        );
    }
}

/// The same branch via a CLOSED lower bound that sorts BELOW the first separator
/// (`ck >= -5`, the fixture's smallest ck is 0): also `None` from the floor walk, so
/// the first block must survive here too.
#[tokio::test]
async fn cqlite_written_closed_lower_bound_below_first_separator_keeps_first_block() {
    let (_temp, db) = setup().await;

    let (got, path) = slice(&db, &format!("pk = {WIDE_PK} AND ck >= -5 AND ck < 20")).await;
    assert_eq!(
        got,
        (0..20).collect::<Vec<i32>>(),
        "Issue #1968 (CQLite-written row index): a closed lower bound BELOW the first \
         separator must keep the first block's rows (ck=0..=19)"
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #1968 (CQLite-written row index): the below-first-separator slice must \
         engage the clustering slice"
    );
}
