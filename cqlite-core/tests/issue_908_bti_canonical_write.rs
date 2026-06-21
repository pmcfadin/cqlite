//! Integration tests for issue #908 (epic #872): Cassandra-canonical BTI write.
//!
//! These prove the BTI writer emits a true `da`-format BTI component set instead
//! of the phase-1 `nb-*-big-*` hybrid:
//!
//! 1. Component filenames use the `da` version letter and `bti` format segment
//!    (`da-<gen>-bti-<Component>`), parsed back via `SsTableDescriptor`.
//! 2. A BTI SSTable has `Data.db` + `Partitions.db` and a TOC, but **no**
//!    `Index.db` and **no** `Summary.db`.
//! 3. `TOC.txt` lists exactly the BTI component set (Data, Partitions, Rows,
//!    Filter, Statistics, Digest, TOC) — no Index/Summary — and self-references
//!    TOC.txt. Rows.db is now emitted (#910), even when 0 bytes for a
//!    narrow-only table (matching the real `da` fixtures).
//! 4. The default BIG writer is unchanged (still `nb-*-big-*` with Index/Summary).
//!
//! All tests require the `write-support` feature.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

fn int_pk_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn int_mutation(id: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "t"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// List the component filenames present in the table directory.
fn component_filenames(table_dir: &Path) -> Vec<String> {
    std::fs::read_dir(table_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect()
}

async fn write_bti(dir: &Path, gen: u64) -> cqlite_core::storage::sstable::writer::SSTableInfo {
    let schema = int_pk_schema();
    let mut writer =
        SSTableWriter::with_format(dir.to_path_buf(), gen, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    let mut keyed: Vec<_> = (0..6)
        .map(|i| {
            let m = int_mutation(i, &format!("name{i}"), 1_000_000 + i as i64);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    writer.finish().await.unwrap()
}

/// AC#1 + AC#2: every emitted BTI component uses the `da-<gen>-bti-<Component>`
/// descriptor and the directory contains Data.db + Partitions.db but neither
/// Index.db nor Summary.db.
#[tokio::test]
async fn bti_components_use_da_bti_descriptor_and_omit_big_index() {
    let dir = TempDir::new().unwrap();
    let info = write_bti(dir.path(), 1).await;

    let table_dir = dir.path().join("test_ks").join("t");
    let names = component_filenames(&table_dir);
    assert!(!names.is_empty(), "BTI SSTable should produce components");

    // Every component parses as a `da`/`bti` descriptor (version letter + format
    // segment in the correct order per SsTableDescriptor::parse).
    for name in &names {
        let desc = SsTableDescriptor::parse_filename(name)
            .unwrap_or_else(|e| panic!("component {name:?} is not a valid descriptor: {e}"));
        assert_eq!(
            desc.version, "da",
            "component {name:?} must use `da` version"
        );
        assert_eq!(
            desc.format,
            SsTableFormat::Bti,
            "component {name:?} must use `bti` format segment"
        );
        assert_eq!(desc.sstable_id, "1", "generation must be the id segment");
        assert!(
            name.starts_with("da-1-bti-"),
            "component {name:?} must start with da-1-bti-"
        );
    }

    // Required BTI components exist with the canonical names.
    assert!(names.iter().any(|n| n == "da-1-bti-Data.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Partitions.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Rows.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Filter.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Statistics.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Digest.crc32"));
    assert!(names.iter().any(|n| n == "da-1-bti-TOC.txt"));

    // No BIG-only components. Rows.db IS now emitted (#910); for this narrow
    // table it is a 0-byte component, matching the real `da` fixtures.
    assert!(
        !names.iter().any(|n| n.contains("Index.db")),
        "BTI must not emit Index.db, got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n.contains("Summary.db")),
        "BTI must not emit Summary.db, got {names:?}"
    );

    // SSTableInfo reflects the omission.
    assert!(
        info.index_path.is_none(),
        "BTI SSTableInfo.index_path must be None"
    );
    assert!(
        info.summary_path.is_none(),
        "BTI SSTableInfo.summary_path must be None"
    );
    assert!(
        info.partitions_path.is_some(),
        "BTI SSTableInfo.partitions_path must be Some"
    );
    assert!(
        info.rows_path.is_some(),
        "BTI SSTableInfo.rows_path must be Some (#910)"
    );
    // Reported paths use the canonical descriptor.
    assert_eq!(
        info.data_path.file_name().unwrap().to_str().unwrap(),
        "da-1-bti-Data.db"
    );
    assert_eq!(
        info.toc_path.file_name().unwrap().to_str().unwrap(),
        "da-1-bti-TOC.txt"
    );
}

/// AC#3: TOC.txt lists exactly the BTI component set and self-references TOC.txt.
#[tokio::test]
async fn bti_toc_lists_exact_component_set() {
    let dir = TempDir::new().unwrap();
    let info = write_bti(dir.path(), 7).await;

    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    let listed: std::collections::BTreeSet<&str> = toc
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();

    let expected: std::collections::BTreeSet<&str> = [
        "Data.db",
        "Partitions.db",
        "Rows.db",
        "Filter.db",
        "Statistics.db",
        "Digest.crc32",
        "TOC.txt",
    ]
    .into_iter()
    .collect();

    assert_eq!(
        listed, expected,
        "BTI TOC must list exactly the canonical component set (Data, Partitions, Rows, \
         Filter, Statistics, Digest, TOC) — no Index/Summary"
    );
    // Explicit self-reference + explicit exclusions/inclusions.
    assert!(toc.contains("TOC.txt"), "TOC must self-reference");
    assert!(!toc.contains("Index.db"), "TOC must not list Index.db");
    assert!(!toc.contains("Summary.db"), "TOC must not list Summary.db");
    assert!(toc.contains("Rows.db"), "TOC must list Rows.db (#910)");
}

/// AC#4: the default BIG writer is unchanged — `nb-*-big-*` with Index/Summary,
/// no Partitions.db, and the TOC still lists Index/Summary.
#[tokio::test]
async fn big_default_format_unchanged() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();

    let mut writer = SSTableWriter::new(dir.path().to_path_buf(), 1, &schema).unwrap();
    let m = int_mutation(1, "alice", 1_000_000);
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let table_dir = dir.path().join("test_ks").join("t");
    let names = component_filenames(&table_dir);

    // BIG descriptor on every component.
    for name in &names {
        let desc = SsTableDescriptor::parse_filename(name).unwrap();
        assert_eq!(desc.version, "nb", "BIG component {name:?} must use `nb`");
        assert_eq!(desc.format, SsTableFormat::Big);
        assert!(name.starts_with("nb-1-big-"), "got {name:?}");
    }

    assert!(names.iter().any(|n| n == "nb-1-big-Index.db"));
    assert!(names.iter().any(|n| n == "nb-1-big-Summary.db"));
    assert!(
        !names.iter().any(|n| n.contains("Partitions.db")),
        "BIG must not emit Partitions.db"
    );

    assert!(info.index_path.is_some(), "BIG must report Index.db path");
    assert!(
        info.summary_path.is_some(),
        "BIG must report Summary.db path"
    );
    assert!(info.partitions_path.is_none());

    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    assert!(toc.contains("Index.db"));
    assert!(toc.contains("Summary.db"));
    assert!(!toc.contains("Partitions.db"));
    assert!(!toc.contains("Rows.db"), "BIG must not list Rows.db");
    assert!(info.rows_path.is_none(), "BIG must report no Rows.db path");
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #910: Rows.db within-partition row-index roundtrip + empty-table handling
// ─────────────────────────────────────────────────────────────────────────────

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder};
use cqlite_core::storage::sstable::bti::{
    iterate_rows_in_bti_trie, lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry,
    BtiPartitionLocation,
};
use std::io::Cursor;

/// Schema: wide(pk int, ck int, payload text, PRIMARY KEY (pk, ck)).
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
        dropped_columns: HashMap::new(),
    }
}

/// One row of a wide partition: pk/ck ints + a ~2 KiB payload so a few hundred
/// rows comfortably exceed two 64 KiB column-index blocks.
fn wide_row(pk: i32, ck: i32, ts: i64) -> Mutation {
    let payload = "x".repeat(2048);
    Mutation::new(
        TableId::new("test_ks", "wide"),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(
            cqlite_core::storage::write_engine::mutation::ClusteringKey::single(
                "ck",
                Value::Integer(ck),
            ),
        ),
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text(payload),
        }],
        ts,
        None,
    )
}

/// AC (#910): a BTI SSTable with a WIDE partition emits a non-empty Rows.db; the
/// wide partition resolves through Partitions.db → RowsOffset → resolve_rows_db_entry
/// → iterate_rows_in_bti_trie, while a NARROW partition stays a direct DataOffset.
#[tokio::test]
async fn bti_wide_partition_resolves_through_rows_db() {
    let dir = TempDir::new().unwrap();
    let schema = wide_schema();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    // pk=1: WIDE (200 rows × ~2 KiB ≈ 400 KiB → >= 2 blocks).
    // pk=2: NARROW (1 small row → 1 block → direct DataOffset).
    let mut partitions: Vec<(i32, Vec<i32>)> = vec![(1, (0..200).collect()), (2, vec![0])];

    // Determine token order for the two partition keys.
    partitions.sort_by_key(|(pk, _)| {
        let m = wide_row(*pk, 0, 1_000_000);
        m.decorated_key(&schema).unwrap().token
    });

    for (pk, cks) in &partitions {
        let mut muts: Vec<Mutation> = cks
            .iter()
            .map(|ck| wide_row(*pk, *ck, 1_000_000 + *ck as i64))
            .collect();
        // All mutations share the partition key; write_partition takes one key.
        let key = muts[0].decorated_key(&schema).unwrap();
        // Sort by ck for clustering order (writer also sorts, but be explicit).
        muts.sort_by_key(|m| match &m.clustering_key {
            Some(ck) => match &ck.columns[0].1 {
                Value::Integer(v) => *v,
                _ => 0,
            },
            None => 0,
        });
        writer.write_partition(key, muts).unwrap();
    }

    let info = writer.finish().await.unwrap();

    // Rows.db must exist and be NON-empty (pk=1 is wide).
    let rows_path = info.rows_path.clone().expect("Rows.db path");
    let rows_db = std::fs::read(&rows_path).unwrap();
    assert!(
        !rows_db.is_empty(),
        "a wide partition must produce a non-empty Rows.db"
    );

    let partitions_db = std::fs::read(info.partitions_path.clone().unwrap()).unwrap();

    // pk=1 (wide) → RowsOffset; resolve + traverse yields >= 2 ascending blocks.
    let raw_pk1 = 1i32.to_be_bytes().to_vec();
    let mut cur = Cursor::new(partitions_db.clone());
    let loc1 = lookup_raw_key_in_bti_partitions_db(&mut cur, &raw_pk1)
        .unwrap()
        .expect("pk=1 found");
    let rows_offset = match loc1 {
        BtiPartitionLocation::RowsOffset(o) => o as usize,
        BtiPartitionLocation::DataOffset(o) => {
            panic!("pk=1 must be wide (RowsOffset); got DataOffset({o})")
        }
    };
    let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("resolve pk=1 entry");
    assert!(
        header.block_count >= 2,
        "wide partition must span >= 2 blocks; got {}",
        header.block_count
    );
    let entries =
        iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse pk=1 row index");
    assert_eq!(
        entries.len() as u32,
        header.block_count,
        "traversal must yield block_count blocks"
    );
    // Separators ascending; block offsets strictly increasing.
    for w in entries.windows(2) {
        assert!(w[0].0 <= w[1].0, "separators must be ascending");
        assert!(
            w[0].1.data_offset < w[1].1.data_offset,
            "block offsets must be strictly increasing"
        );
    }
    // First block separator is ck=0 (OSS50 sign-flipped int = 0x8000_0000).
    assert_eq!(
        entries[0].0,
        0x8000_0000u32.to_be_bytes().to_vec(),
        "first separator must be ck=0"
    );

    // pk=2 (narrow) → DataOffset, NOT a RowsOffset.
    let raw_pk2 = 2i32.to_be_bytes().to_vec();
    let mut cur2 = Cursor::new(partitions_db);
    let loc2 = lookup_raw_key_in_bti_partitions_db(&mut cur2, &raw_pk2)
        .unwrap()
        .expect("pk=2 found");
    assert!(
        matches!(loc2, BtiPartitionLocation::DataOffset(_)),
        "narrow partition pk=2 must resolve to a direct DataOffset, got {loc2:?}"
    );

    // TOC lists Rows.db.
    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    assert!(toc.contains("Rows.db"), "TOC must list Rows.db");
}

/// Finding 2 (roborev #908): an EMPTY BTI SSTable (no partitions) cannot produce
/// a readable Partitions.db (no 8-byte root footer) — the writer must REFUSE to
/// publish it with a clear error rather than emit an unreadable `da` artifact.
#[tokio::test]
async fn bti_empty_sstable_is_refused() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();
    let writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();
    // No partitions written.
    let result = writer.finish().await;
    assert!(
        result.is_err(),
        "an empty BTI SSTable must be refused (unreadable Partitions.db otherwise)"
    );
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("empty BTI SSTable"),
        "error must explain the empty-BTI refusal; got: {msg}"
    );
}

/// Schema: wide(pk int, ck int, payload text, PRIMARY KEY (pk, ck)) with the
/// clustering column's order set by `order` (ASC or DESC). Used by the reversed
/// byte-comparable separator regression below.
fn wide_schema_with_order(order: ClusteringOrder) -> TableSchema {
    let mut s = wide_schema();
    s.clustering_keys[0].order = order;
    s
}

/// Schema: wide2(pk int, ck1 int ASC, ck2 int DESC, payload text). Exercises a
/// MIXED-order clustering key (the 0x40 framing byte stays un-inverted while the
/// DESC component's bytes are complemented).
fn wide_mixed_schema() -> TableSchema {
    let mut s = wide_schema();
    s.table = "wide2".to_string();
    s.clustering_keys = vec![
        ClusteringColumn {
            name: "ck1".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        },
        ClusteringColumn {
            name: "ck2".to_string(),
            data_type: "int".to_string(),
            position: 1,
            order: ClusteringOrder::Desc,
        },
    ];
    s.columns = vec![
        Column {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        },
        Column {
            name: "ck1".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        },
        Column {
            name: "ck2".to_string(),
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
    ];
    s
}

/// One wide-partition row carrying a single-component clustering `ck` against an
/// explicit `table`/schema (mirrors `wide_row` but lets the table name vary).
fn wide_row_for(table: &str, pk: i32, ck: i32, ts: i64) -> Mutation {
    let payload = "x".repeat(2048);
    Mutation::new(
        TableId::new("test_ks", table),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(
            cqlite_core::storage::write_engine::mutation::ClusteringKey::single(
                "ck",
                Value::Integer(ck),
            ),
        ),
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text(payload),
        }],
        ts,
        None,
    )
}

/// Regression for the roborev MEDIUM finding: a WIDE partition on a `CLUSTERING
/// ORDER BY (ck DESC)` table must produce ASCENDING OSS50 separator bytes (via
/// the reversed byte-comparable transform) so the Rows.db row-index trie is
/// emitted and the partition resolves through a positive `RowsOffset` — it must
/// NOT silently fall back to a direct `DataOffset`.
#[tokio::test]
async fn bti_wide_desc_partition_resolves_through_rows_db() {
    let dir = TempDir::new().unwrap();
    let schema = wide_schema_with_order(ClusteringOrder::Desc);
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    // Single wide partition pk=1 with 200 rows × ~2 KiB => >= 2 column-index
    // blocks. The writer sorts rows into clustering (DESC) order internally.
    let cks: Vec<i32> = (0..200).collect();
    let muts: Vec<Mutation> = cks
        .iter()
        .map(|ck| wide_row_for("wide", 1, *ck, 1_000_000 + *ck as i64))
        .collect();
    let key = muts[0].decorated_key(&schema).unwrap();
    writer.write_partition(key, muts).unwrap();

    let info = writer.finish().await.unwrap();

    let rows_db = std::fs::read(info.rows_path.clone().expect("Rows.db path")).unwrap();
    assert!(
        !rows_db.is_empty(),
        "a wide DESC partition must produce a non-empty Rows.db (no DataOffset fallback)"
    );

    let partitions_db = std::fs::read(info.partitions_path.clone().unwrap()).unwrap();
    let raw_pk1 = 1i32.to_be_bytes().to_vec();
    let mut cur = Cursor::new(partitions_db);
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &raw_pk1)
        .unwrap()
        .expect("pk=1 found");
    let rows_offset = match loc {
        BtiPartitionLocation::RowsOffset(o) => o as usize,
        BtiPartitionLocation::DataOffset(o) => panic!(
            "wide DESC partition MUST resolve through RowsOffset, not DataOffset({o}) \
             (reversed byte-comparable separators were rejected -> silent fallback)"
        ),
    };

    let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("resolve DESC entry");
    assert!(
        header.block_count >= 2,
        "wide partition must span >= 2 blocks; got {}",
        header.block_count
    );
    let entries =
        iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse DESC row index");
    assert_eq!(entries.len() as u32, header.block_count);

    // Separators ascending (trie requirement) AND block offsets strictly
    // increasing (physical write order). The DESC encoding makes these agree.
    for w in entries.windows(2) {
        assert!(
            w[0].0 < w[1].0,
            "DESC separators must be strictly ascending bytes; got {:02x?} then {:02x?}",
            w[0].0,
            w[1].0
        );
        assert!(
            w[0].1.data_offset < w[1].1.data_offset,
            "block offsets must be strictly increasing"
        );
    }

    // First block's first row is the LARGEST ck (DESC writes descending). For
    // ck in 0..200, that is ck=199 => reversed byte-comparable of (sign-flip
    // int 199) = complement(80 00 00 C7) = 7F FF FF 38.
    let expected_first = (0x8000_0000u32 ^ 199)
        .to_be_bytes()
        .iter()
        .map(|b| 0xFF ^ *b)
        .collect::<Vec<u8>>();
    assert_eq!(
        entries[0].0, expected_first,
        "first DESC separator must be reversed byte-comparable of the largest ck (199)"
    );
}

/// Mixed ASC/DESC clustering: a wide partition on (ck1 int ASC, ck2 int DESC)
/// must still resolve through `RowsOffset` with ascending separator bytes.
#[tokio::test]
async fn bti_wide_mixed_order_partition_resolves_through_rows_db() {
    let dir = TempDir::new().unwrap();
    let schema = wide_mixed_schema();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    // Single wide partition pk=1: vary ck1 across a handful of values, ck2 across
    // many, with ~2 KiB payloads so total >> 2 blocks.
    let payload = "y".repeat(2048);
    let mut muts: Vec<Mutation> = Vec::new();
    for ck1 in 0..4i32 {
        for ck2 in 0..60i32 {
            muts.push(Mutation::new(
                TableId::new("test_ks", "wide2"),
                PartitionKey::single("pk", Value::Integer(1)),
                Some(
                    cqlite_core::storage::write_engine::mutation::ClusteringKey::new(vec![
                        ("ck1".to_string(), Value::Integer(ck1)),
                        ("ck2".to_string(), Value::Integer(ck2)),
                    ]),
                ),
                vec![CellOperation::Write {
                    column: "payload".to_string(),
                    value: Value::Text(payload.clone()),
                }],
                1_000_000,
                None,
            ));
        }
    }
    let key = muts[0].decorated_key(&schema).unwrap();
    writer.write_partition(key, muts).unwrap();

    let info = writer.finish().await.unwrap();
    let rows_db = std::fs::read(info.rows_path.clone().expect("Rows.db path")).unwrap();
    assert!(
        !rows_db.is_empty(),
        "wide mixed-order partition must produce a non-empty Rows.db"
    );

    let partitions_db = std::fs::read(info.partitions_path.clone().unwrap()).unwrap();
    let raw_pk1 = 1i32.to_be_bytes().to_vec();
    let mut cur = Cursor::new(partitions_db);
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &raw_pk1)
        .unwrap()
        .expect("pk=1 found");
    let rows_offset = match loc {
        BtiPartitionLocation::RowsOffset(o) => o as usize,
        BtiPartitionLocation::DataOffset(o) => {
            panic!(
                "mixed-order wide partition MUST resolve through RowsOffset, got DataOffset({o})"
            )
        }
    };
    let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("resolve mixed entry");
    assert!(header.block_count >= 2);
    let entries =
        iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse mixed row index");
    for w in entries.windows(2) {
        assert!(
            w[0].0 < w[1].0,
            "mixed-order separators must be strictly ascending bytes"
        );
    }
}

/// A narrow-only BTI SSTable still publishes (valid Partitions.db) and emits a
/// 0-byte Rows.db listed in the TOC — exactly matching the real
/// `simple_table`/`collection_table`/`ttl_table` `da-2-bti-Rows.db` fixtures.
#[tokio::test]
async fn bti_narrow_only_emits_zero_byte_rows_db() {
    let dir = TempDir::new().unwrap();
    let info = write_bti(dir.path(), 3).await;
    let rows_path = info.rows_path.clone().expect("Rows.db path");
    let rows_db = std::fs::read(&rows_path).unwrap();
    assert!(
        rows_db.is_empty(),
        "a narrow-only BTI SSTable must emit a 0-byte Rows.db; got {} bytes",
        rows_db.len()
    );
    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    assert!(
        toc.contains("Rows.db"),
        "0-byte Rows.db must still be in TOC"
    );
}
