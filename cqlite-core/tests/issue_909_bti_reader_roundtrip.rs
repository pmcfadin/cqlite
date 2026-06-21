//! Integration tests for issue #909 (epic #872): SSTableReader discovery for the
//! canonical `da` BTI format + a full writer -> reader roundtrip.
//!
//! These prove that an SSTable produced by [`SSTableWriter`] with
//! [`SSTableFormat::Bti`] (a real `da-*-bti-*` component set with NO
//! Index.db/Summary.db) can be:
//!
//! 1. DISCOVERED by [`SSTableReader::open`]: the `da` descriptor routes the
//!    reader onto the BTI partition-trie path (`Partitions.db` + `Rows.db`),
//!    never the BIG Index.db/Summary.db path.
//! 2. FULL-SCANNED back: every written row is returned with its values intact —
//!    for BOTH a NARROW partition (direct `DataOffset`) and a WIDE partition
//!    (whose payload is a positive `RowsOffset` resolved through `Rows.db`).
//! 3. POINT-LOOKED-UP back: a `get(partition_key)` resolves through the
//!    Partitions.db trie (O(log n)) for BOTH the narrow partition (DataOffset)
//!    and the wide partition (RowsOffset -> `resolve_rows_db_entry` -> Data.db
//!    position), returning the partition's row.
//!
//! The wide partition (200 rows x ~2 KiB) spans >= 2 column-index blocks, so the
//! writer emits a non-empty `Rows.db` and stores a positive `RowsOffset` in the
//! partition trie. This is the exact path #909 enables on the read side.
//!
//! All tests require the `write-support` feature.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{
    ClusteringColumn, ClusteringOrder, Column, KeyColumn, SchemaRegistry, SchemaRegistryConfig,
    TableSchema,
};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::{RowKey, TableId as ReaderTableId, Value};
use cqlite_core::Config;
use tempfile::TempDir;
use tokio::sync::RwLock;

/// wide(pk int, ck int, payload text, PRIMARY KEY (pk, ck)).
fn wide_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "wide".to_string(),
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
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

/// One row of a partition: pk/ck ints + a payload of the given size. A ~2 KiB
/// payload over 200 rows comfortably exceeds two 64 KiB column-index blocks,
/// making the partition WIDE (positive `RowsOffset`).
fn row(pk: i32, ck: i32, payload_len: usize, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "wide"),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text("x".repeat(payload_len)),
        }],
        ts,
        None,
    )
}

/// Number of rows in the WIDE partition. Sized so total bytes >> 2 blocks.
const WIDE_ROWS: i32 = 200;
/// Per-row payload size for the wide partition (~2 KiB).
const WIDE_PAYLOAD: usize = 2048;

/// Partition key that is NARROW (1 small row -> direct DataOffset).
const NARROW_PK: i32 = 1;
/// Partition key that is WIDE (>= 2 column-index blocks -> RowsOffset).
const WIDE_PK: i32 = 2;

/// Write a BTI SSTable containing one NARROW partition (pk=1, 1 small row) and
/// one WIDE partition (pk=2, 200 x ~2 KiB rows), then relocate the produced
/// components into a Cassandra-canonical `test_ks/wide-<uuid>/` directory so the
/// reader extracts the table name (`wide`) from the parent dir exactly as it does
/// for real `da` fixtures. Returns the relocated `SSTableInfo`.
async fn write_mixed_bti(dir: &Path) -> cqlite_core::storage::sstable::writer::SSTableInfo {
    let schema = wide_schema();
    let mut writer =
        SSTableWriter::with_format(dir.to_path_buf(), 1, &schema, 16, SSTableFormat::Bti).unwrap();

    // Build per-partition mutation lists.
    let narrow: Vec<Mutation> = vec![row(NARROW_PK, 0, 8, 1_000_000)];
    let wide: Vec<Mutation> = (0..WIDE_ROWS)
        .map(|ck| row(WIDE_PK, ck, WIDE_PAYLOAD, 2_000_000 + ck as i64))
        .collect();

    // Partitions must be written in token order.
    let mut partitions: Vec<(i32, Vec<Mutation>)> = vec![(NARROW_PK, narrow), (WIDE_PK, wide)];
    partitions.sort_by_key(|(pk, _)| row(*pk, 0, 1, 1).decorated_key(&schema).unwrap().token);

    for (_pk, muts) in partitions {
        let key = muts[0].decorated_key(&schema).unwrap();
        writer.write_partition(key, muts).unwrap();
    }

    let info = writer.finish().await.unwrap();
    relocate_to_canonical_dir(dir, info)
}

/// Relocate a writer-produced SSTable from `dir/test_ks/wide/` to the
/// Cassandra-canonical `dir/test_ks/wide-<uuid>/` layout and rewrite the paths in
/// `info`. The reader derives `table_name` by stripping the `-<uuid>` suffix from
/// the parent directory, so the canonical layout is required for the table-id
/// guard in the point-lookup path to resolve to `wide`.
fn relocate_to_canonical_dir(
    dir: &Path,
    info: cqlite_core::storage::sstable::writer::SSTableInfo,
) -> cqlite_core::storage::sstable::writer::SSTableInfo {
    let src_table_dir = dir.join("test_ks").join("wide");
    let dst_table_dir = dir
        .join("test_ks")
        .join("wide-00000000000000000000000000000001");
    std::fs::create_dir_all(&dst_table_dir).unwrap();
    for entry in std::fs::read_dir(&src_table_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        std::fs::rename(entry.path(), dst_table_dir.join(&name)).unwrap();
    }

    // Rewrite each path in `info` to point at the relocated directory.
    let remap = |p: std::path::PathBuf| -> std::path::PathBuf {
        let file = p.file_name().unwrap();
        dst_table_dir.join(file)
    };
    let remap_opt = |p: Option<std::path::PathBuf>| p.map(remap);

    cqlite_core::storage::sstable::writer::SSTableInfo {
        data_path: remap(info.data_path),
        index_path: remap_opt(info.index_path),
        filter_path: remap(info.filter_path),
        summary_path: remap_opt(info.summary_path),
        stats_path: remap(info.stats_path),
        compression_info_path: remap_opt(info.compression_info_path),
        partitions_path: remap_opt(info.partitions_path),
        rows_path: remap_opt(info.rows_path),
        toc_path: remap(info.toc_path),
        digest_path: remap(info.digest_path),
        ..info
    }
}

/// Open the writer-produced Data.db via `SSTableReader::open` with a schema
/// registry seeded with `schema` so point lookups can resolve types.
async fn open_reader(data_path: &Path, schema: &TableSchema) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let registry = SchemaRegistry::new(
        SchemaRegistryConfig::default(),
        platform.clone(),
        config.clone(),
    )
    .await
    .unwrap();
    registry
        .register_schema(schema.clone(), cqlite_core::schema::SchemaSource::Manual)
        .await
        .unwrap();

    let mut reader = SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap();
    reader.set_schema_registry(Arc::new(RwLock::new(registry)));
    reader
}

/// Extract the `payload` column text from a row `Value::Map`.
fn payload_of(value: &Value) -> Option<String> {
    if let Value::Map(entries) = value {
        for (k, v) in entries {
            if let (Value::Text(name), Value::Text(text)) = (k, v) {
                if name == "payload" {
                    return Some(text.clone());
                }
            }
        }
    }
    None
}

/// Extract the clustering value (`ck`) from a row `Value::Map`.
///
/// The two reader decode paths name the clustering column differently: the
/// whole-section scan decode (`parse_block_with_cell_metadata`) uses the schema
/// name `ck`, while the point-lookup decode (`parse_block_emit`) uses the
/// synthetic `clustering_key`. Both carry the same integer value, so we accept
/// either name (the column-naming difference is orthogonal to #909's RowsOffset
/// resolution being exercised here).
fn ck_of(value: &Value) -> Option<i32> {
    if let Value::Map(entries) = value {
        for (k, v) in entries {
            if let Value::Text(name) = k {
                if name == "ck" || name == "clustering_key" {
                    if let Value::Integer(i) = v {
                        return Some(*i);
                    }
                }
            }
        }
    }
    None
}

/// AC#1 + AC#2: discovery routes a `da` SSTable onto the BTI trie path, and a
/// FULL SCAN reads back every row of BOTH the narrow (DataOffset) and wide
/// (RowsOffset -> Rows.db) partitions with values intact.
#[tokio::test]
async fn bti_writer_reader_full_scan_roundtrip() {
    let dir = TempDir::new().unwrap();
    let info = write_mixed_bti(dir.path()).await;
    let schema = wide_schema();

    // The wide partition must have produced a NON-empty Rows.db (RowsOffset path)
    // and there must be no Index.db/Summary.db (BTI discovery precondition).
    let rows_db = std::fs::read(info.rows_path.clone().unwrap()).unwrap();
    assert!(
        !rows_db.is_empty(),
        "the wide partition must produce a non-empty Rows.db (positive RowsOffset path)"
    );
    assert!(info.index_path.is_none(), "BTI must have no Index.db");
    assert!(info.summary_path.is_none(), "BTI must have no Summary.db");

    let reader = open_reader(&info.data_path, &schema).await;

    // Discovery routed to BTI: the reader loaded Partitions.db (proven by the
    // BTI scan path returning rows below — the BIG index path is absent).
    let table_id = ReaderTableId::from("test_ks.wide");
    let rows = reader
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .unwrap();

    // 1 narrow row + 200 wide rows = 201 rows total.
    assert_eq!(
        rows.len(),
        (WIDE_ROWS as usize) + 1,
        "full scan must return all rows from BOTH partitions (1 narrow + {} wide)",
        WIDE_ROWS
    );

    let narrow_payload = "x".repeat(8);
    let wide_payload = "x".repeat(WIDE_PAYLOAD);

    // Narrow partition: exactly one row with the small payload.
    let narrow_rows: Vec<&Value> = rows
        .iter()
        .filter(|(_k, v)| payload_of(v).as_ref() == Some(&narrow_payload))
        .map(|(_k, v)| v)
        .collect();
    assert_eq!(
        narrow_rows.len(),
        1,
        "narrow partition must contribute exactly one row"
    );

    // Wide partition: 200 rows, each with the ~2 KiB payload, covering ck 0..200.
    let mut wide_cks: Vec<i32> = rows
        .iter()
        .filter(|(_k, v)| payload_of(v).as_ref() == Some(&wide_payload))
        .filter_map(|(_k, v)| ck_of(v))
        .collect();
    wide_cks.sort_unstable();
    assert_eq!(
        wide_cks.len(),
        WIDE_ROWS as usize,
        "wide partition must contribute all {} rows via the Rows.db RowsOffset path",
        WIDE_ROWS
    );
    let expected: Vec<i32> = (0..WIDE_ROWS).collect();
    assert_eq!(
        wide_cks, expected,
        "wide partition rows must cover ck 0..{} exactly",
        WIDE_ROWS
    );
}

/// AC#3: a POINT LOOKUP resolves through the Partitions.db trie for BOTH the
/// narrow partition (direct DataOffset) and the wide partition (positive
/// RowsOffset resolved through Rows.db). Neither falls through to a sequential
/// scan, and both return the partition's row.
#[tokio::test]
async fn bti_writer_reader_point_lookup_roundtrip() {
    let dir = TempDir::new().unwrap();
    let info = write_mixed_bti(dir.path()).await;
    let schema = wide_schema();
    let reader = open_reader(&info.data_path, &schema).await;
    let table_id = ReaderTableId::from("test_ks.wide");

    let scans_before = SSTableReader::scan_for_key_call_count();

    // --- NARROW partition (pk=1): direct DataOffset ---
    let narrow_key = RowKey::from(NARROW_PK.to_be_bytes().to_vec());
    let narrow = reader
        .get(&table_id, &narrow_key)
        .await
        .unwrap()
        .expect("narrow partition (DataOffset) must be found via the BTI trie");
    assert_eq!(
        payload_of(&narrow),
        Some("x".repeat(8)),
        "narrow point lookup must return the narrow partition's row"
    );

    // --- WIDE partition (pk=2): positive RowsOffset -> Rows.db -> data_position ---
    let wide_key = RowKey::from(WIDE_PK.to_be_bytes().to_vec());
    let wide = reader
        .get(&table_id, &wide_key)
        .await
        .unwrap()
        .expect("wide partition (RowsOffset) must be found via the BTI trie + Rows.db");
    // The point lookup returns the FIRST row of the partition (ck=0).
    assert_eq!(
        payload_of(&wide),
        Some("x".repeat(WIDE_PAYLOAD)),
        "wide point lookup must return a wide-partition row (resolved through Rows.db)"
    );
    assert_eq!(
        ck_of(&wide),
        Some(0),
        "wide point lookup returns the partition's first clustering row (ck=0)"
    );

    // Neither lookup fell through to a sequential scan: BTI resolves entirely via
    // the Partitions.db trie (+ Rows.db for the wide partition).
    let scans_after = SSTableReader::scan_for_key_call_count();
    assert_eq!(
        scans_after, scans_before,
        "BTI point lookups must NOT fall through to scan_for_key (trie + Rows.db only)"
    );

    // A key that is absent returns None (trie has no path), still no scan.
    let absent_key = RowKey::from(9999i32.to_be_bytes().to_vec());
    let absent = reader.get(&table_id, &absent_key).await.unwrap();
    assert!(absent.is_none(), "an absent partition key must return None");
    assert_eq!(
        SSTableReader::scan_for_key_call_count(),
        scans_before,
        "an absent BTI lookup must not trigger a sequential scan either"
    );
}

/// AC#4: the BTI partition-trie point lookup resolves BOTH the narrow
/// (DataOffset) and wide (RowsOffset) partition keys to an uncompressed Data.db
/// offset — proving #909's RowsOffset → Rows.db → `data_position` resolution at
/// the primitive level (independent of the higher-level `get()` decode). The
/// narrow partition resolves to the first partition (offset 0); the wide
/// partition's positive RowsOffset is resolved through `resolve_rows_db_entry` to
/// its real Data.db position.
#[tokio::test]
async fn bti_trie_resolves_both_offset_kinds() {
    let dir = TempDir::new().unwrap();
    let info = write_mixed_bti(dir.path()).await;
    let schema = wide_schema();
    let reader = open_reader(&info.data_path, &schema).await;

    let narrow_key = NARROW_PK.to_be_bytes().to_vec();
    let wide_key = WIDE_PK.to_be_bytes().to_vec();

    let narrow_off = reader
        .lookup_partition_via_bti_trie(&narrow_key)
        .unwrap()
        .expect("narrow key must resolve to a direct Data.db offset");
    let wide_off = reader
        .lookup_partition_via_bti_trie(&wide_key)
        .unwrap()
        .expect("wide key must resolve via RowsOffset -> Rows.db -> Data.db position");

    // The two partitions occupy distinct Data.db positions (different offsets).
    assert_ne!(
        narrow_off, wide_off,
        "narrow and wide partitions must resolve to distinct Data.db offsets"
    );

    // An absent key has no trie path.
    let absent = reader
        .lookup_partition_via_bti_trie(&9999i32.to_be_bytes())
        .unwrap();
    assert!(absent.is_none(), "absent key must have no trie path");
}
