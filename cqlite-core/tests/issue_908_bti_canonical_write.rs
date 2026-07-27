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
            value: Value::text(name.to_string()),
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

/// Read a Cassandra unsigned VInt (`DataOutputPlus.writeUnsignedVInt`) from
/// `bytes[*pos..]`, advancing `pos`. Hand-rolled ON PURPOSE: the assertion path of
/// [`assert_trie_index_entry_base_is_canonical`] must not share code with the
/// production writer/reader pair it is checking (issue #3002).
fn read_unsigned_vint_independently(bytes: &[u8], pos: &mut usize) -> u64 {
    let first = bytes[*pos];
    *pos += 1;
    let extra = first.leading_ones() as usize;
    // Data bits kept in the first byte: 7 - extra (0 once extra >= 7).
    let mask: u64 = if extra >= 7 {
        0
    } else {
        (1u64 << (7 - extra)) - 1
    };
    let mut value = (first as u64) & mask;
    for _ in 0..extra {
        value = (value << 8) | bytes[*pos] as u64;
        *pos += 1;
    }
    value
}

/// Read a Cassandra signed (ZigZag) VInt independently of production code.
fn read_signed_vint_independently(bytes: &[u8], pos: &mut usize) -> i64 {
    let u = read_unsigned_vint_independently(bytes, pos);
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// Issue #3002, writer side: decode the emitted `TrieIndexEntry` at `rows_offset`
/// BY HAND (`u16` key length, key bytes, unsigned-vint data position, signed-vint
/// root delta) and assert the SIGNED root delta is measured from the canonical base
/// `rows_offset + 2 + key_length` — the position immediately AFTER
/// `writeWithShortLength`, which is where cassandra-5.0.8
/// `BtiTableWriter.IndexWriter.append` captures `basePosition`.
///
/// The structural invariant used as the oracle needs no production helper: the
/// writer serializes each partition's row-index trie IMMEDIATELY before that
/// partition's entry and the trie ROOT is the LAST node written (children first,
/// parent after — `write_row_node`), so the root node's serialized bytes must END
/// exactly at `rows_offset`. A 2-low base points 2 bytes short of a node boundary,
/// which this check rejects. The root is additionally asserted payload-CAPABLE
/// (ordinals 1/3, the `SingleNoPayload` variants, structurally cannot carry one —
/// that is how the fixture-side #3002 defect lost the block-0 payload).
fn assert_trie_index_entry_base_is_canonical(
    rows_db: &[u8],
    rows_offset: usize,
    expected_key: &[u8],
) {
    // [u16 key_length][key bytes]
    let key_length = u16::from_be_bytes([rows_db[rows_offset], rows_db[rows_offset + 1]]) as usize;
    assert_eq!(
        key_length,
        expected_key.len(),
        "entry's u16 key length must match the partition key length"
    );
    let key = &rows_db[rows_offset + 2..rows_offset + 2 + key_length];
    assert_eq!(key, expected_key, "entry must carry the raw partition key");

    let mut pos = rows_offset + 2 + key_length;
    // [data position : unsigned vint]
    let _data_position = read_unsigned_vint_independently(rows_db, &mut pos);
    // [trieRoot - base : SIGNED vint]
    let root_delta = read_signed_vint_independently(rows_db, &mut pos);

    let base = rows_offset + 2 + key_length;
    let root = (base as i64 + root_delta) as usize;
    assert!(
        root < rows_offset,
        "the trie root {root} must lie in the trie region BELOW the entry at {rows_offset}"
    );

    // The root node's serialized extent must end exactly at the entry start.
    let extent_ends_at_entry = |node_offset: usize| -> bool {
        let header = rows_db[node_offset];
        let ordinal = header >> 4;
        // Sparse ordinals 5/7/8/9 carry 1/2/3/5-byte backward pointers;
        // layout = [header][count][count transition bytes][count pointers].
        let ptr_bytes = match ordinal {
            5 => 1usize,
            7 => 2,
            8 => 3,
            9 => 5,
            _ => return false,
        };
        let count = rows_db[node_offset + 1] as usize;
        node_offset + 2 + count + count * ptr_bytes == rows_offset
    };

    let header = rows_db[root];
    let ordinal = header >> 4;
    assert!(
        ordinal != 1 && ordinal != 3,
        "the resolved root byte 0x{header:02x} must be a payload-CAPABLE node type \
         (ordinals 1/3 are SingleNoPayload and structurally cannot carry a payload)"
    );
    assert!(
        extent_ends_at_entry(root),
        "the resolved root at {root} (base {base} + delta {root_delta}) must be the LAST \
         trie node written before the entry at {rows_offset}, i.e. its serialized bytes \
         must end exactly there; header byte 0x{header:02x}"
    );
    // The root's single transition is the OSS50 `0x40` NEXT_COMPONENT byte shared by
    // every separator (issue #3002), so the root has exactly one child.
    assert_eq!(
        rows_db[root + 1],
        1,
        "the root indexes one shared first separator byte, so its fan-out must be 1"
    );
    assert_eq!(
        rows_db[root + 2],
        0x40,
        "the root's only transition must be the NEXT_COMPONENT byte 0x40"
    );

    // The PRE-#3002 base (`rows_offset + key_length`, 2 bytes low) does NOT describe
    // a node ending at the entry start — so this check really discriminates the two.
    let pre_fix_root = root - 2;
    assert!(
        !extent_ends_at_entry(pre_fix_root),
        "the pre-#3002 2-low base would resolve to {pre_fix_root}, which must NOT satisfy \
         the node-boundary invariant (else this assertion could not detect a writer \
         regression)"
    );
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
            value: Value::Text(payload.into()),
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
    // ---------------------------------------------------------------------
    // WRITER-SIDE base, decoded INDEPENDENTLY of production code (issue #3002).
    // `resolve_rows_db_entry` + the writer moved in LOCKSTEP, so a round-trip
    // through the reader would pass just as well with the OLD (2-low) base. This
    // block therefore decodes the emitted `TrieIndexEntry` by hand and checks the
    // resulting root against a structural invariant of the emitted bytes.
    // ---------------------------------------------------------------------
    assert_trie_index_entry_base_is_canonical(&rows_db, rows_offset, &raw_pk1);

    let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("resolve pk=1 entry");
    assert!(
        header.block_count >= 2,
        "wide partition must span >= 2 blocks; got {}",
        header.block_count
    );
    let entries =
        iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse pk=1 row index");
    // KNOWN GAP (write parity, tracked as follow-up work off issue #3002) — pinned
    // deliberately, NOT fixed here (fixing it would change emitted bytes):
    //
    //   CQLite's row-index writer emits `block_count` separators, the first being the
    //   FIRST ROW'S clustering key (ck=0 below). Apache Cassandra 5.0's
    //   `RowIndexWriter.add` instead indexes block 0 under `ByteComparable.EMPTY`
    //   (stored as the trie ROOT node's own payload) and appends a trailing separator
    //   in `complete()`, so a Cassandra-written trie holds `blockCount + 1`
    //   separators — see `cqlite-core/tests/issue_3002_bti_rows_root_base.rs`, which
    //   pins exactly that shape on the real `da` fixture. So the count identity
    //   asserted here (`entries.len() == block_count`) is CQLite's shape, not
    //   Cassandra's.
    //
    // Second-order consequences of the gap (all consequences of the missing empty
    // block-0 separator, none of them fixed here):
    //   1. A CQLite-written wide partition has a NON-empty first separator, so a
    //      clustering bound below it floors to `None` — the #1968 implicit-first
    //      branch in `reader/data_access/bti.rs` must therefore live indefinitely,
    //      even though it is unreachable for Cassandra-written tries.
    //   2. A spec-conformant Cassandra 5.0 `RowIndexReader.separatorFloor` over a
    //      CQLite-written `Rows.db` finds NO block-0 entry, so for a clustering key
    //      below the first stored separator the partition's earliest clustering rows
    //      are unreachable through the row index by that reader.
    //   3. The writer consequently REFUSES an empty separator outright rather than
    //      mis-encoding it under transition byte `0x00` (`insert_row` returns
    //      `Error::InvalidInput`; see `partitions_writer_tests.rs`), because the
    //      canonical position — the root node's payload — is not expressible by
    //      `build_row_trie`.
    assert_eq!(
        entries.len() as u32,
        header.block_count,
        "traversal must yield block_count blocks (CQLite's shape; Cassandra's canonical \
         trie holds blockCount + 1 — see the KNOWN GAP note above)"
    );
    // Separators ascending; block offsets strictly increasing.
    for w in entries.windows(2) {
        assert!(w[0].0 <= w[1].0, "separators must be ascending");
        assert!(
            w[0].1.data_offset < w[1].1.data_offset,
            "block offsets must be strictly increasing"
        );
    }
    // First block separator is ck=0: the `0x40 NEXT_COMPONENT` byte every OSS50
    // component carries (issue #3002) + the sign-flipped int (0x8000_0000).
    let mut expected_first = vec![0x40u8];
    expected_first.extend_from_slice(&0x8000_0000u32.to_be_bytes());
    assert_eq!(entries[0].0, expected_first, "first separator must be ck=0");

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
            value: Value::Text(payload.into()),
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
    // Same KNOWN GAP as the ASC case above (follow-up off issue #3002): CQLite emits
    // `block_count` separators led by the first row's clustering key, where Cassandra
    // emits `blockCount + 1` led by the root-payload `ByteComparable.EMPTY` block-0
    // separator.
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
    // ck in 0..200, that is ck=199 => the UN-inverted `0x40 NEXT_COMPONENT` framing
    // byte (emitted by the comparator, not the type — issue #3002) followed by the
    // reversed byte-comparable of (sign-flip int 199) = complement(80 00 00 C7) =
    // 7F FF FF 38.
    let mut expected_first = vec![0x40u8];
    expected_first.extend(
        (0x8000_0000u32 ^ 199)
            .to_be_bytes()
            .iter()
            .map(|b| 0xFF ^ *b),
    );
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
                    value: Value::text(payload.clone()),
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
