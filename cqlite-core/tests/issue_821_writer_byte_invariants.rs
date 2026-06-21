//! Issue #821 (Epic #817): writer-side byte invariants.
//!
//! Verifies two findings from the garbage-free-compaction differential audit:
//!
//! * **Finding #2 — `previousUnfilteredSize`.** The per-unfiltered `prev_size`
//!   vint written inside each row body must equal the byte distance from the
//!   start of the previous unfiltered, *including that previous item's own
//!   prev_size vint length* (it is part of the chain). The first unfiltered in a
//!   partition uses the partition-header byte size as its prev_size (empirically
//!   confirmed against a real Cassandra "nb" SSTable — see the module test
//!   `finding2_real_cassandra_first_row_prev_size_equals_header`). A static row
//!   hard-codes `prev_size = 0` AND is **not** treated as the "previous
//!   unfiltered": its bytes still count toward the running position, so the first
//!   regular row after a static row carries `header_size + static_row_size`
//!   (= its own in-partition offset), NOT the static row's size alone. Both facts
//!   are anchored against real Cassandra "nb" SSTables.
//!
//! * **Finding #16 — 64-bit offsets.** Every in-partition and data-file offset
//!   in the writer path must be `u64`/`i64`. A 32-bit offset wraps negative past
//!   2 GiB and corrupts block offsets. This is verified by round-tripping an
//!   over-2-GiB Data.db offset through the three writer-path offset encoders
//!   (`IndexWriter` data-position vint, `PartitionsTrieWriter` BTI leaf, and the
//!   raw `encode_unsigned` used for in-partition offsets) and asserting the byte
//!   value decodes back to the full 64-bit value with no truncation.
//!
//! All assertions are byte-level (value-asserting, not count-only) per the
//! issue #719 doctrine.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::data_writer::DataWriter;
use cqlite_core::storage::sstable::writer::partitions_writer::PartitionsTrieWriter;
use cqlite_core::storage::sstable::writer::stats_writer::StatisticsMetadata;
use cqlite_core::storage::sstable::writer::IndexWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;

// Row-header flag constants (mirror data_writer.rs).
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
const EXTENDED_IS_STATIC: u8 = 0x01;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Deterministic stats baselines so temporal deltas are small, single-byte
/// vints (keeps the manual byte walk simple).
fn stats() -> StatisticsMetadata {
    let mut s = StatisticsMetadata::new();
    s.min_timestamp = 1_000_000;
    s.min_ttl = 0;
    s.min_local_deletion_time = 0;
    s
}

/// int PK, single regular text column. No clustering, no statics.
fn simple_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue821".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// int PK, int clustering, one static text column + one regular text column.
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue821".to_string(),
        table: "s".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![cqlite_core::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: cqlite_core::schema::ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "sdata".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "rdata".to_string(),
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

/// Read a Cassandra unsigned vint; returns (value, bytes_consumed).
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    assert!(extra < 8, "vint with 8 extension bytes not expected here");
    let mask: u64 = if extra >= 8 {
        0
    } else {
        0xFFu64 >> (extra + 1)
    };
    let mut value = (first as u64) & mask;
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, extra + 1)
}

/// 4-byte big-endian int partition key bytes for `id = n`.
fn int_key_bytes(n: i32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Partition-header byte size for an int PK (4-byte key):
/// 2 (u16 key-length prefix) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
const INT_PK_HEADER_SIZE: u64 = 2 + 4 + 4 + 8;

/// Walk one unfiltered starting at `flags` byte `pos`. Returns
/// (flags, ext_flags, prev_size, total_unfiltered_byte_len, next_pos).
///
/// Handles rows with/without a clustering prefix and static rows. The clustering
/// prefix of a non-static row precedes `row_size`; for the int-clustering schema
/// it is a 1-byte header (0x00 = present) + 4 value bytes = 5 bytes.
fn walk_unfiltered(
    data: &[u8],
    pos: usize,
    has_clustering: bool,
    clustering_prefix_len: usize,
) -> (u8, Option<u8>, u64, usize, usize) {
    let start = pos;
    let mut p = pos;
    let flags = data[p];
    p += 1;
    let mut ext = None;
    if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
        ext = Some(data[p]);
        p += 1;
    }
    let is_static = ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0);
    // Non-static rows carry a clustering prefix before row_size; static rows do not.
    if has_clustering && !is_static {
        p += clustering_prefix_len;
    }
    let (row_size, rs_len) = read_vuint(data, p);
    p += rs_len;
    let (prev_size, _ps_len) = read_vuint(data, p);
    // row_size counts the body (prev_size vint + remaining body). The total
    // unfiltered length is (flags + ext + clustering + row_size_vint + row_size).
    let body_end = p + row_size as usize;
    let total = body_end - start;
    (flags, ext, prev_size, total, body_end)
}

// ---------------------------------------------------------------------------
// Finding #2 — previousUnfilteredSize
// ---------------------------------------------------------------------------

/// Empirical anchor: a real Cassandra 5.0 "nb" SSTable sets the FIRST row's
/// prev_size to the partition-header byte size (NOT 0). This pins the
/// convention CQLite must match, independent of CQLite's own writer.
///
/// `test_basic.uncompressed_table` (no compression, no static columns):
/// partition key is a 16-byte UUID, so the header = 2 + 16 + 12 = 30, and the
/// first row at Data.db offset 30 carries prev_size = 30.
#[test]
fn finding2_real_cassandra_first_row_prev_size_equals_header() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => r,
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT unset; skipping real-SSTable anchor");
            return;
        }
    };
    let path = std::path::Path::new(&root).join(
        "sstables/test_basic/uncompressed_table-6aedb7a0a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(_) => {
            eprintln!("real Data.db not fetched ({path:?}); skipping anchor");
            return;
        }
    };

    // Header: [u16 key_len=16][16 key][i32 LDT=MAX][i64 mfda=MIN] = 30 bytes.
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    assert_eq!(key_len, 16, "expected 16-byte UUID partition key");
    let header_size = 2 + key_len + 12; // = 30
    assert_eq!(header_size, 30);

    // First row begins at offset == header_size (no static columns in this table).
    let row_pos = header_size;
    let flags = data[row_pos];
    // 0x24 = HAS_TIMESTAMP | HAS_ALL_COLUMNS; in particular no extended/static.
    assert_eq!(flags & ROW_HAS_EXTENDED_FLAGS, 0, "first row is not static");
    let (_row_size, rs_len) = read_vuint(&data, row_pos + 1);
    let (prev_size, _) = read_vuint(&data, row_pos + 1 + rs_len);
    assert_eq!(
        prev_size, header_size as u64,
        "real Cassandra: first row prev_size must equal the partition-header byte size"
    );
}

/// Empirical anchor for the STATIC case, against a real Cassandra "nb" SSTable
/// (`test_basic.static_columns_table`, Snappy-compressed): the static row in the
/// first partition carries prev_size = 0, and the following regular row carries
/// prev_size = header_size + static_row_size (NOT the static row size alone).
#[test]
fn finding2_real_cassandra_static_row_zero_and_regular_is_header_plus_static() {
    use cqlite_core::storage::sstable::chunk_decompressor::create_decompressor_from_file;
    use std::io::Cursor;

    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => r,
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT unset; skipping real-SSTable static anchor");
            return;
        }
    };
    let dir = std::path::Path::new(&root)
        .join("sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9");
    let data = match std::fs::read(dir.join("nb-1-big-Data.db")) {
        Ok(d) => d,
        Err(_) => {
            eprintln!("real static Data.db not fetched; skipping anchor");
            return;
        }
    };
    let mut dec = match create_decompressor_from_file(&dir.join("nb-1-big-CompressionInfo.db")) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("could not load CompressionInfo ({e:?}); skipping anchor");
            return;
        }
    };
    let mut cur = Cursor::new(&data);
    let raw = dec.read_all_data(&mut cur).unwrap();

    let key_len = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    let header_size = (2 + key_len + 12) as u64; // UUID PK → 2 + 16 + 12 = 30

    // Static row (first unfiltered). Clustering type is TIMESTAMP → 8 value bytes.
    let ts_prefix = 1 + 8; // 1 clustering header byte + 8 value bytes
    let (sflags, sext, sprev, sstatic_total, after_static) =
        walk_unfiltered(&raw, header_size as usize, true, ts_prefix);
    assert_ne!(sflags & ROW_HAS_EXTENDED_FLAGS, 0);
    assert_eq!(sext, Some(EXTENDED_IS_STATIC));
    assert_eq!(sprev, 0, "real Cassandra static row prev_size must be 0");

    // First regular row after the static row.
    let (_rflags, _rext, rprev, _rtotal, _next) =
        walk_unfiltered(&raw, after_static, true, ts_prefix);
    assert_eq!(
        rprev,
        header_size + sstatic_total as u64,
        "real Cassandra: regular row prev_size = header + static_row_size (its in-partition offset)"
    );
    // And explicitly NOT the static row's size alone.
    assert_ne!(rprev, sstatic_total as u64);
}

/// A normal row that is the FIRST unfiltered in a partition (no statics):
/// CQLite must write prev_size == partition-header size.
#[test]
fn finding2_first_normal_row_prev_size_is_header_size() {
    let schema = simple_schema();
    let mut w = DataWriter::new(stats());

    let key = DecoratedKey::new(1, int_key_bytes(1));
    let m = Mutation::new(
        TableId::new("issue821", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("alice".to_string()),
        }],
        2_000_000,
        None,
    );

    w.write_partition(&key, &[m], &schema, None, &[]).unwrap();
    let bytes = w.finish().unwrap();

    // First row begins right after the 18-byte header (no statics in schema).
    let (_flags, _ext, prev_size, _total, _next) =
        walk_unfiltered(&bytes, INT_PK_HEADER_SIZE as usize, false, 0);
    assert_eq!(
        prev_size, INT_PK_HEADER_SIZE,
        "first normal row prev_size must equal the partition-header byte size (got {prev_size})"
    );
}

/// Two normal rows in one partition (no statics): the second row's prev_size
/// must equal the FIRST row's full serialized byte length (chain advances,
/// including the first row's own prev_size vint).
#[test]
fn finding2_second_normal_row_prev_size_is_first_row_total() {
    // Use a clustering schema so we can place two distinct rows in one partition.
    let schema = clustering_schema();
    let mut w = DataWriter::new(stats());

    let key = DecoratedKey::new(1, int_key_bytes(1));
    let mk = |ck: i32, v: &str| {
        Mutation::new(
            TableId::new("issue821", "c"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(ck))),
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text(v.to_string()),
            }],
            2_000_000,
            None,
        )
    };

    w.write_partition(&key, &[mk(1, "one"), mk(2, "two")], &schema, None, &[])
        .unwrap();
    let bytes = w.finish().unwrap();

    // int clustering prefix = 1 header byte + 4 value bytes = 5 bytes.
    let cprefix = 5usize;
    let (_f1, _e1, prev1, total1, next1) =
        walk_unfiltered(&bytes, INT_PK_HEADER_SIZE as usize, true, cprefix);
    assert_eq!(
        prev1, INT_PK_HEADER_SIZE,
        "first row prev_size must be the header size (got {prev1})"
    );

    let (_f2, _e2, prev2, _total2, _next2) = walk_unfiltered(&bytes, next1, true, cprefix);
    assert_eq!(
        prev2, total1 as u64,
        "second row prev_size must equal the first row's full serialized length (got {prev2}, expected {total1})"
    );
}

/// A static row hard-codes prev_size == 0 AND is skipped by the prev-size
/// chain: it is never treated as the "previous unfiltered". The first regular
/// row after the static row therefore measures its prev_size from the partition
/// start, i.e. `header_size + static_row_size` (= the regular row's absolute
/// in-partition offset). This is exactly what real Cassandra "nb" SSTables do
/// (anchored by `test_basic.static_columns_table`: header 30 + static 16 →
/// regular-row prev_size 46), NOT the static row's size alone (which is what a
/// naive "previous unfiltered size" chain would emit).
#[test]
fn finding2_static_row_hardcodes_zero_and_does_not_advance_chain() {
    let schema = static_schema();
    let mut w = DataWriter::new(stats());

    let key = DecoratedKey::new(1, int_key_bytes(1));
    // One mutation: writes the static column AND a regular cell for clustering 7.
    let m = Mutation::new(
        TableId::new("issue821", "s"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![
            CellOperation::Write {
                column: "sdata".to_string(),
                value: Value::Text("static-val".to_string()),
            },
            CellOperation::Write {
                column: "rdata".to_string(),
                value: Value::Text("row-val".to_string()),
            },
        ],
        2_000_000,
        None,
    );

    w.write_partition(&key, &[m], &schema, None, &[]).unwrap();
    let bytes = w.finish().unwrap();

    // The static row is the first unfiltered after the 18-byte header.
    let (sflags, sext, sprev, sstatic_total, after_static) =
        walk_unfiltered(&bytes, INT_PK_HEADER_SIZE as usize, true, 5);
    assert_ne!(
        sflags & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static row must set HAS_EXTENDED_FLAGS"
    );
    assert_eq!(
        sext,
        Some(EXTENDED_IS_STATIC),
        "static row must set IS_STATIC in extended flags"
    );
    assert_eq!(
        sprev, 0,
        "static row must hard-code prev_size = 0 (got {sprev})"
    );
    assert!(sstatic_total > 0);

    // The static row is NOT the "previous unfiltered" for the chain, but its
    // bytes still count toward the running in-partition position. So the first
    // regular row's prev_size = header + static_row_size = its own offset from
    // the partition start. Crucially this is NOT the static row's size alone.
    let expected_regular_prev = INT_PK_HEADER_SIZE + sstatic_total as u64;
    let (rflags, _rext, rprev, _rtotal, _next) = walk_unfiltered(&bytes, after_static, true, 5);
    assert_eq!(
        rflags & ROW_HAS_EXTENDED_FLAGS,
        0,
        "the unfiltered after the static row must be a normal (non-static) row"
    );
    assert_ne!(
        rprev, sstatic_total as u64,
        "regression guard: the regular row must NOT carry the static row's size \
         alone — that would mean the static row wrongly advanced the chain as the \
         previous unfiltered"
    );
    assert_eq!(
        rprev, expected_regular_prev,
        "first regular row after a static row must carry header+static_size \
         (= its in-partition offset); got {rprev}, expected {expected_regular_prev} \
         (header {INT_PK_HEADER_SIZE} + static {sstatic_total})"
    );
}

/// int PK, int clustering, one regular text column `v`.
fn clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue821".to_string(),
        table: "c".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![cqlite_core::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: cqlite_core::schema::ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Finding #16 — 64-bit offsets
// ---------------------------------------------------------------------------

/// A Data.db offset just past 2 GiB. As an i32 this wraps negative; the writer
/// path must treat it as a full u64/i64.
const OFFSET_OVER_2GIB: u64 = (1u64 << 31) + 12_345; // 2_147_495_993

/// Sanity: the offset really would be corrupted by a 32-bit narrowing.
#[test]
fn finding16_offset_would_wrap_negative_as_i32() {
    assert!(OFFSET_OVER_2GIB > i32::MAX as u64);
    let truncated = OFFSET_OVER_2GIB as u32 as i32;
    assert!(
        truncated < 0,
        "an i32 narrowing of a >2GiB offset must wrap negative (proves the hazard)"
    );
}

/// `IndexWriter` writes the Data.db position as an unsigned vint from a `u64`.
/// A >2 GiB offset must round-trip through the serialized Index.db bytes.
#[test]
fn finding16_index_writer_data_offset_roundtrips_over_2gib() {
    let mut iw = IndexWriter::new();
    let key = DecoratedKey::new(42, int_key_bytes(42));
    iw.add_partition(&key, OFFSET_OVER_2GIB).unwrap();
    let bytes = iw.finish().unwrap();

    // Entry layout: [u16 key_len=4][4 key bytes][position: unsigned vint]...
    let key_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    assert_eq!(key_len, 4);
    let pos_off = 2 + key_len;
    let (decoded, _len) = read_vuint(&bytes, pos_off);
    assert_eq!(
        decoded, OFFSET_OVER_2GIB,
        "Index.db data position must survive >2GiB without 32-bit truncation"
    );
}

/// The raw in-partition / Data.db offset vint encoder (`encode_unsigned`, the
/// same helper the writer uses for IndexInfo `offset` and Index.db positions)
/// must round-trip a >2 GiB value.
#[test]
fn finding16_in_partition_offset_vint_roundtrips_over_2gib() {
    use cqlite_core::storage::serialization::vint::encode_unsigned;
    let mut buf = Vec::new();
    encode_unsigned(OFFSET_OVER_2GIB, &mut buf);
    let (decoded, _len) = read_vuint(&buf, 0);
    assert_eq!(
        decoded, OFFSET_OVER_2GIB,
        "in-partition offset vint must survive >2GiB without truncation"
    );
}

/// The BTI `PartitionsTrieWriter` leaf encodes the Data.db offset as
/// `~position` SizedInts. A >2 GiB offset must round-trip through the serialized
/// trie leaf payload as a full 64-bit value.
#[test]
fn finding16_bti_partition_leaf_data_offset_roundtrips_over_2gib() {
    let mut pw = PartitionsTrieWriter::new();
    pw.add_partition(&int_key_bytes(7), OFFSET_OVER_2GIB);
    let trie = pw.finish().unwrap();
    assert!(!trie.is_empty(), "trie must be non-empty for one partition");

    // The leaf payload is `[header][hash_byte][SizedInts(position)]` where
    // position = !data_offset (sign-negated). Scan the trie for a leaf whose
    // decoded direct offset == OFFSET_OVER_2GIB. We locate it by trying every
    // possible SizedInts window: a leaf header has high nibble 0 (PayloadOnly,
    // ordinal 0) and low nibble = FLAG_HAS_HASH_BYTE(8) + (n-1) for n position
    // bytes. We decode every candidate and assert at least one yields the
    // expected offset, proving no truncation occurred.
    let found = scan_bti_leaf_for_offset(&trie, OFFSET_OVER_2GIB);
    assert!(
        found,
        "BTI partition leaf must encode the >2GiB Data.db offset without truncation"
    );
}

/// Decode SizedInts: Cassandra's variable-width big-endian signed integer where
/// the sign is taken from the top bit of the first byte (sign-extended).
fn decode_sized_int_be(bytes: &[u8]) -> i64 {
    assert!(!bytes.is_empty() && bytes.len() <= 8);
    let neg = bytes[0] & 0x80 != 0;
    let mut v: i64 = if neg { -1 } else { 0 };
    for &b in bytes {
        v = (v << 8) | b as i64;
    }
    v
}

/// Scan the serialized BTI trie for a PayloadOnly leaf whose decoded direct
/// offset (`!position`) equals `want`.
fn scan_bti_leaf_for_offset(trie: &[u8], want: u64) -> bool {
    // The last 8 bytes are the root-offset footer; leaves live before it.
    let body = &trie[..trie.len().saturating_sub(8)];
    for i in 0..body.len() {
        let header = body[i];
        // Leaf = PayloadOnly (high nibble 0); low nibble carries payloadBits.
        if header & 0xF0 != 0 {
            continue;
        }
        let payload_bits = header & 0x0F;
        // FLAG_HAS_HASH_BYTE = 8; position byte count = payloadBits - 8 + 1.
        if payload_bits < 8 {
            continue;
        }
        let n = (payload_bits - 8 + 1) as usize; // 1..=8
                                                 // [header][hash_byte][n position bytes]
        let pos_start = i + 2;
        if pos_start + n > body.len() {
            continue;
        }
        let pos_bytes = &body[pos_start..pos_start + n];
        let position = decode_sized_int_be(pos_bytes);
        // Direct Data.db offset is the bitwise-NOT of position.
        let offset = !position;
        if offset >= 0 && offset as u64 == want {
            return true;
        }
    }
    false
}
