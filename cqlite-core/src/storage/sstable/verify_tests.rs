//! Unit tests for [`super`] — `verify.rs`'s check helpers, error-class codes
//! and component resolution.
//!
//! Carried in a sibling file wired via
//! `#[cfg(test)] #[path = "verify_tests.rs"] mod tests;`. `verify.rs` is long
//! past the 800-line source threshold (campsite rule, epic #1116/#1135) and
//! this change had to add to it; moving ~665 lines of tests out brings the
//! production file BELOW its own pre-change line count, so the file-size
//! ratchet is satisfied outright rather than opted out of. `#[path]` keeps
//! this a CHILD module of `verify`, so `use super::*` still reaches its
//! private items unchanged.

use super::*;

#[test]
fn error_class_codes_are_stable() {
    assert_eq!(VerifyErrorClass::DigestMismatch.code(), "DigestMismatch");
    assert_eq!(
        VerifyErrorClass::ChunkOffsetOutOfBounds.code(),
        "ChunkOffsetOutOfBounds"
    );
    assert_eq!(
        VerifyErrorClass::BtiRootPointerCorrupt.code(),
        "BtiRootPointerCorrupt"
    );
    // issue #1282: the two new classes must expose stable codes.
    assert_eq!(
        VerifyErrorClass::OutOfOrderKeyOrRow.code(),
        "OutOfOrderKeyOrRow"
    );
    assert_eq!(
        VerifyErrorClass::InvalidLocalDeletionTime.code(),
        "InvalidLocalDeletionTime"
    );
    // issue #1414: the unsupported-compression-feature class must be stable.
    assert_eq!(
        VerifyErrorClass::UnsupportedCompressionFeature.code(),
        "UnsupportedCompressionFeature"
    );
}

#[test]
fn unsupported_compression_feature_classified_distinctly() {
    // issue #1414: a zstd dictionary rejection reaches the scan classifier as
    // `Error::UnsupportedFormat` and MUST map to the dedicated
    // `UnsupportedCompressionFeature` class — never the truncation/bit-flip
    // `ChunkDecompressionError` nor the checksum `DigestMismatch`.
    let dict_err = Error::UnsupportedFormat(
        "zstd dictionary compression (Dictionary_ID=1234) is unsupported for chunk 0 at offset 0x0"
            .to_string(),
    );
    assert_eq!(
        classify_scan_error_class(&dict_err),
        VerifyErrorClass::UnsupportedCompressionFeature
    );
    assert_ne!(
        classify_scan_error_class(&dict_err),
        VerifyErrorClass::ChunkDecompressionError
    );
    assert_ne!(
        classify_scan_error_class(&dict_err),
        VerifyErrorClass::DigestMismatch
    );

    // Regression guard: a genuine plain-decode failure (truncation/bit-flip)
    // stays `ChunkDecompressionError` — the new class must not swallow it.
    let decode_err = Error::InvalidFormat(
        "Zstd decompression failed for chunk 0 at offset 0x0: corrupted input".to_string(),
    );
    assert_eq!(
        classify_scan_error_class(&decode_err),
        VerifyErrorClass::ChunkDecompressionError
    );

    // A chunk inline-CRC mismatch also stays `ChunkDecompressionError`.
    let crc_err = Error::Corruption("Data.db chunk 0 CRC32 mismatch".to_string());
    assert_eq!(
        classify_scan_error_class(&crc_err),
        VerifyErrorClass::ChunkDecompressionError
    );
}

#[test]
fn non_compression_unsupported_format_falls_through_to_generic() {
    // roborev (issue #1414): the compression-specific class is reserved for
    // compression-related `UnsupportedFormat`. A NON-compression `UnsupportedFormat`
    // reaching this scan classifier (e.g. a hypothetical future decode-path feature
    // rejection) MUST NOT be mislabeled as an unsupported compression feature — it
    // falls through to the generic `RowScanFailed`.
    let non_compression = Error::UnsupportedFormat(
        "tuple element type not yet supported for chunk 0 at offset 0x0".to_string(),
    );
    assert_eq!(
        classify_scan_error_class(&non_compression),
        VerifyErrorClass::RowScanFailed
    );
    assert_ne!(
        classify_scan_error_class(&non_compression),
        VerifyErrorClass::UnsupportedCompressionFeature
    );

    // Every real compression-related producer still earns the compression class,
    // regardless of the exact wording: the "not compiled in" build-config path…
    let not_compiled = Error::UnsupportedFormat("Zstd support not compiled in".to_string());
    assert_eq!(
        classify_scan_error_class(&not_compiled),
        VerifyErrorClass::UnsupportedCompressionFeature
    );
    // …and the unknown/unsupported algorithm path.
    let unknown_algo =
        Error::UnsupportedFormat("Unknown compression algorithm: BogusCompressor".to_string());
    assert_eq!(
        classify_scan_error_class(&unknown_algo),
        VerifyErrorClass::UnsupportedCompressionFeature
    );
}

/// End-to-end wiring (issue #1414): a REAL trained-dictionary zstd frame,
/// driven through the shipped `ChunkDecompressor`, must surface the typed
/// `Error::UnsupportedFormat` that `classify_scan_error_class` maps to
/// `UnsupportedCompressionFeature` — proving the reader error and the verify
/// class agree end to end (not just on a hand-written message).
#[cfg(feature = "zstd")]
#[test]
fn dictionary_frame_wires_reader_error_to_unsupported_class() {
    use crate::parser::header::CassandraVersion;
    use crate::storage::sstable::chunk_decompressor::ChunkDecompressor;
    use crate::storage::sstable::compression_info::CompressionInfo;
    use std::io::Cursor;

    let plaintext =
        b"cqlite|zstd|dictionary|row=verify|table=zstd_dictionary_table|value=payload-7".to_vec();
    let samples: Vec<Vec<u8>> = (0..1024u32)
        .map(|i| format!("cqlite|zstd|dictionary|row={i}|value={}", i % 37).into_bytes())
        .collect();
    let dict = zstd::dict::from_samples(&samples, 4 * 1024).expect("train zstd dictionary");
    let dict_frame = zstd::bulk::Compressor::with_dictionary(3, &dict)
        .expect("dictionary compressor")
        .compress(&plaintext)
        .expect("dictionary-compress chunk");

    // Cassandra chunk framing: [compressed payload][4-byte BE CRC32].
    let mut image = dict_frame.clone();
    image.extend_from_slice(&crc32fast::hash(&dict_frame).to_be_bytes());

    let info = CompressionInfo {
        algorithm: "ZstdCompressor".to_string(),
        option_pairs: vec![],
        chunk_length: plaintext.len() as u32,
        max_compressed_length: i32::MAX as u32,
        data_length: plaintext.len() as u64,
        chunk_offsets: vec![0],
    };
    let mut dec =
        ChunkDecompressor::new(info, CassandraVersion::V5_0Release).expect("build decompressor");
    let err = dec
        .decompress_chunk_by_index(&mut Cursor::new(image), 0)
        .expect_err("dictionary frame must be rejected");

    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "reader must reject with UnsupportedFormat; got: {err}"
    );
    assert_eq!(
        classify_scan_error_class(&err),
        VerifyErrorClass::UnsupportedCompressionFeature,
        "verify must classify the reader's dictionary rejection as \
         UnsupportedCompressionFeature; got err: {err}"
    );
}

#[test]
fn mode_labels() {
    assert_eq!(VerifyMode::Quick.as_str(), "quick");
    assert_eq!(VerifyMode::Full.as_str(), "full");
    assert_ne!(VerifyMode::Quick, VerifyMode::Full);
}

#[test]
fn real_component_recognition_excludes_sidecars() {
    assert!(is_real_component("Data.db"));
    assert!(is_real_component("Statistics.db"));
    assert!(is_real_component("CompressionInfo.db"));
    assert!(is_real_component("TOC.txt"));
    assert!(is_real_component("Digest.crc32"));
    // sidecar / reference goldens are NOT components
    assert!(!is_real_component("Data.db.jsonl"));
    assert!(!is_real_component("Statistics.db.txt"));
    assert!(!is_real_component("CompressionInfo.db.txt"));
    assert!(!is_real_component("README.md"));
}

#[test]
fn report_summary_line_distinguishes_ok_and_fail() {
    let ok = VerifyReport {
        directory: PathBuf::from("/x"),
        base_name: "nb-1-big".to_string(),
        format: SsTableFormat::Big,
        mode: VerifyMode::Full,
        findings: vec![],
        toc_components: vec![],
        rows_scanned: Some(3),
    };
    assert!(ok.is_ok());
    assert!(ok.summary_line().contains("VERIFY OK"));

    let fail = VerifyReport {
        directory: PathBuf::from("/x"),
        base_name: "nb-1-big".to_string(),
        format: SsTableFormat::Big,
        mode: VerifyMode::Full,
        findings: vec![VerifyFinding::new(
            VerifyErrorClass::DigestMismatch,
            "Digest.crc32",
            "boom",
        )],
        toc_components: vec![],
        rows_scanned: None,
    };
    assert!(!fail.is_ok());
    assert_eq!(fail.primary_class(), Some(VerifyErrorClass::DigestMismatch));
    assert!(fail.summary_line().contains("VERIFY FAIL"));
    assert!(fail.summary_line().contains("DigestMismatch"));
}

// ---- BTI partition identity cross-check (issue #1103) ------------------
//
// These exercise `bti_partition_identity_mismatch` over RESOLVED leaves: each
// leaf carries its emitted byte-comparable prefix plus a payload resolved back
// to a raw partition key (an inline raw key for a `RowsOffset` leaf, or a
// Data.db position for a `DataOffset` leaf). The Data.db side is the
// `(position, raw_key)` set from the scan.

use crate::storage::sstable::bti::parser::encode_partition_key_for_bti_trie;

/// Build the path-compressed trie key for a raw partition key: the
/// byte-comparable `[0x40 ++ token]` key truncated to its first `prefix_len`
/// bytes, mirroring how a real Patricia trie stores only the shortest
/// distinguishing prefix.
fn trie_key_prefix(raw: &[u8], prefix_len: usize) -> Vec<u8> {
    encode_partition_key_for_bti_trie(raw)[..prefix_len].to_vec()
}

/// A `RowsOffset`-style leaf: authoritative inline raw key + matching Data.db
/// position, with a 2-byte emitted prefix (what `test_da/wide_table` does).
fn inline_leaf(raw: &[u8], data_position: u64) -> BtiResolvedLeaf {
    BtiResolvedLeaf {
        prefix: trie_key_prefix(raw, 2),
        inline_raw_key: Some(raw.to_vec()),
        data_position,
    }
}

/// A `DataOffset`-style leaf: no inline key, resolved purely via its Data.db
/// position, with a 2-byte emitted prefix derived from the key it *should*
/// resolve to (so the prefix/payload-consistency check passes when healthy).
fn data_offset_leaf(prefix_from: &[u8], data_position: u64) -> BtiResolvedLeaf {
    BtiResolvedLeaf {
        prefix: trie_key_prefix(prefix_from, 2),
        inline_raw_key: None,
        data_position,
    }
}

/// The Data.db scan side: distinct partition keys, each at a synthetic
/// monotonically-increasing position (0, 100, 200, ...).
fn data_partitions(keys: &[Vec<u8>]) -> Vec<(u64, Vec<u8>)> {
    keys.iter()
        .enumerate()
        .map(|(i, k)| (i as u64 * 100, k.clone()))
        .collect()
}

#[test]
fn identity_check_passes_for_inline_rows_leaves() {
    // Healthy wide-table shape: every leaf resolves to its inline raw key,
    // matching the Data.db key at the same position.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let leaves: Vec<BtiResolvedLeaf> = data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
    assert_eq!(bti_partition_identity_mismatch(&leaves, &data), None);
}

#[test]
fn identity_check_passes_for_data_offset_leaves() {
    // Healthy small-partition shape (`da-2-bti`): leaves carry only a Data.db
    // position; the raw key is resolved through the position map.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let leaves: Vec<BtiResolvedLeaf> = data
        .iter()
        .map(|(pos, k)| data_offset_leaf(k, *pos))
        .collect();
    assert_eq!(bti_partition_identity_mismatch(&leaves, &data), None);
}

#[test]
fn identity_check_detects_inline_payload_pointing_at_wrong_partition() {
    // The exact reviewer scenario for a `RowsOffset` leaf: the leaf's emitted
    // prefix is unchanged but its INLINE raw key (the payload) is rewritten to
    // a partition NOT present in Data.db. Same leaf count, wrong identity.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let mut leaves: Vec<BtiResolvedLeaf> =
        data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
    // Keep the emitted prefix; rewrite the inline raw key to pk=99.
    leaves[0].inline_raw_key = Some(99u32.to_be_bytes().to_vec());
    assert!(
        bti_partition_identity_mismatch(&leaves, &data).is_some(),
        "an inline payload pointing at a partition absent from Data.db must be flagged"
    );
}

#[test]
fn identity_check_detects_data_offset_payload_pointing_at_wrong_partition() {
    // The reviewer scenario for a `DataOffset` leaf: the leaf's emitted prefix
    // is unchanged but its Data.db position payload is rewritten to point at a
    // DIFFERENT partition's start. The resolved key then no longer matches the
    // partition the prefix encodes.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let mut leaves: Vec<BtiResolvedLeaf> = data
        .iter()
        .map(|(pos, k)| data_offset_leaf(k, *pos))
        .collect();
    // Leaf 0's prefix still encodes pk=1, but its position now points at pk=2.
    leaves[0].data_position = data[1].0;
    let detail = bti_partition_identity_mismatch(&leaves, &data)
        .expect("a DataOffset payload pointing at the wrong partition must be flagged");
    // It is caught by the prefix/payload-consistency check (the resolved key's
    // encoding no longer starts with the leaf's prefix) OR the multiset compare.
    assert!(
        detail.contains("inconsistent") || detail.contains("identities"),
        "unexpected detail: {detail}"
    );
}

#[test]
fn identity_check_detects_data_offset_payload_pointing_at_non_partition() {
    // A `DataOffset` flipped to a byte position that is NOT a partition start
    // resolves to no key at all.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let mut leaves: Vec<BtiResolvedLeaf> = data
        .iter()
        .map(|(pos, k)| data_offset_leaf(k, *pos))
        .collect();
    leaves[0].data_position = 37; // not any partition start
    let detail = bti_partition_identity_mismatch(&leaves, &data)
        .expect("a DataOffset pointing at a non-partition position must be flagged");
    assert!(detail.contains("not a decoded partition start"));
}

#[test]
fn identity_check_detects_same_count_wrong_keys_via_multiset() {
    // Same leaf count as Data.db and every leaf is individually well-formed
    // (valid key, valid in-map position, consistent prefix) — but the trie
    // resolves the SAME partition three times instead of {1,2,3}. Only the
    // multiset comparison catches this; it is the core of issue #1103.
    let data_keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&data_keys);
    // Three leaves all resolving to partition 1 (key + position from data[0]).
    let leaves: Vec<BtiResolvedLeaf> = (0..3).map(|_| inline_leaf(&data[0].1, data[0].0)).collect();
    let detail = bti_partition_identity_mismatch(&leaves, &data)
        .expect("same-count wrong-identity must be flagged");
    assert!(
        detail.contains("identities") || detail.contains("time(s)"),
        "expected a multiset-identity mismatch, got: {detail}"
    );
}

#[test]
fn identity_check_detects_inline_leaf_with_corrupt_data_position() {
    // Reviewer (roborev #1431): a `RowsOffset` leaf whose INLINE key is valid
    // and present in Data.db but whose recorded Data.db position points at a
    // non-partition offset must be flagged — a BTI read would seek to the wrong
    // partition even though the inline key looks fine.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let mut leaves: Vec<BtiResolvedLeaf> =
        data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
    // Keep the valid inline key; corrupt only the recorded Data.db position.
    leaves[0].data_position = 9999; // not any partition start
    let detail = bti_partition_identity_mismatch(&leaves, &data).expect(
        "an inline leaf with a valid key but a non-partition data position must be flagged",
    );
    assert!(detail.contains("not a decoded partition start"));
}

#[test]
fn identity_check_detects_one_swapped_key() {
    // Two keys match, one is wrong — the minimal wrong-root that a count check
    // cannot see.
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    let mut leaves: Vec<BtiResolvedLeaf> =
        data.iter().map(|(pos, k)| inline_leaf(k, *pos)).collect();
    // Replace leaf 0 with a key (pk=99) absent from Data.db, including its
    // prefix, and a position that is not a partition start.
    leaves[0] = inline_leaf(&99u32.to_be_bytes(), 10_000);
    assert!(bti_partition_identity_mismatch(&leaves, &data).is_some());
}

// ---- Check 8: key/row order + partition-level LDT (issue #1282) --------

use crate::util::cassandra_murmur3::cassandra_murmur3_token;

/// Build the on-disk-ordered partition list the classifier consumes, sorting
/// the supplied keys by their real Murmur3 `(token, key)` order so the "in
/// order" input mirrors what a healthy Cassandra SSTable produces.
fn ordered_partitions(keys: &[Vec<u8>]) -> Vec<(Vec<u8>, Option<i32>)> {
    let mut v: Vec<Vec<u8>> = keys.to_vec();
    v.sort_by_key(|k| (cassandra_murmur3_token(k), k.clone()));
    v.into_iter().map(|k| (k, None)).collect()
}

#[test]
fn order_ldt_clean_partitions_produce_no_findings() {
    let keys: Vec<Vec<u8>> = (1u32..=6).map(|i| i.to_be_bytes().to_vec()).collect();
    let partitions = ordered_partitions(&keys);
    assert!(
        classify_order_and_ldt(&partitions, true).is_empty(),
        "in-token-order partitions with live LDT must produce zero findings"
    );
}

#[test]
fn order_ldt_detects_out_of_order_partition_keys() {
    // Take the correctly-ordered set and swap the first two, forcing a
    // descending (token, key) step Cassandra's verifier rejects.
    let keys: Vec<Vec<u8>> = (1u32..=6).map(|i| i.to_be_bytes().to_vec()).collect();
    let mut partitions = ordered_partitions(&keys);
    partitions.swap(0, 1);
    let findings = classify_order_and_ldt(&partitions, true);
    assert!(
        findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow),
        "swapping two partitions must be flagged OutOfOrderKeyOrRow, got {:?}",
        findings
    );
}

#[test]
fn order_ldt_detects_duplicate_partition_token_as_out_of_order() {
    // Equal (token, key) is NOT strictly greater → out of order.
    let k = 7u32.to_be_bytes().to_vec();
    let partitions = vec![(k.clone(), None), (k, None)];
    let findings = classify_order_and_ldt(&partitions, true);
    assert!(findings
        .iter()
        .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow));
}

#[test]
fn order_ldt_flags_negative_ldt_on_signed_nb_form() {
    // A deleted partition (Some(ldt)) with a negative ldt on the SIGNED (nb)
    // form is corrupt — Cassandra's DeletionTime/Verifier rejects it.
    let mut partitions = ordered_partitions(&[1u32.to_be_bytes().to_vec()]);
    partitions[0].1 = Some(-1);
    let findings = classify_order_and_ldt(&partitions, /*signed_ldt=*/ true);
    assert!(
        findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::InvalidLocalDeletionTime),
        "negative nb localDeletionTime must be flagged, got {:?}",
        findings
    );
}

#[test]
fn order_ldt_does_not_flag_far_future_ldt_on_unsigned_oa_form() {
    // On the UNSIGNED (oa/da) form a value in [2^31, 2^32) is a legitimate
    // far-future deletion time carried as a negative i32 — it MUST NOT be
    // flagged. This is the no-heuristic guard: the format, not the sign, decides.
    let mut partitions = ordered_partitions(&[1u32.to_be_bytes().to_vec()]);
    partitions[0].1 = Some(-1); // == 0xFFFFFFFF unsigned == far-future seconds
    let findings = classify_order_and_ldt(&partitions, /*signed_ldt=*/ false);
    assert!(
        !findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::InvalidLocalDeletionTime),
        "far-future unsigned oa/da LDT must NOT be flagged, got {:?}",
        findings
    );
}

#[test]
fn order_ldt_positive_deletion_time_is_clean() {
    // A normal positive epoch-seconds partition tombstone is valid on both forms.
    let mut partitions = ordered_partitions(&[1u32.to_be_bytes().to_vec()]);
    partitions[0].1 = Some(1_700_000_000); // ~2023, valid
    assert!(classify_order_and_ldt(&partitions, true).is_empty());
    assert!(classify_order_and_ldt(&partitions, false).is_empty());
}

// ---- Check 8 ROW half: clustering-row order (issue #1282 follow-up) -----

use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use crate::types::Value;
use std::collections::HashMap;

fn schema_one_ck(order: ClusteringOrder) -> TableSchema {
    TableSchema {
        keyspace: "issue_1282".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order,
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

fn ck_int(n: i32) -> Vec<Value> {
    vec![Value::Integer(n)]
}

#[test]
fn clustering_order_ascending_rows_are_clean() {
    let schema = schema_one_ck(ClusteringOrder::Asc);
    let partitions = vec![(0usize, vec![ck_int(1), ck_int(2), ck_int(3)])];
    assert!(
        classify_clustering_row_order(&partitions, &schema).is_empty(),
        "in-order ASC clustering rows must produce no findings"
    );
}

#[test]
fn clustering_order_out_of_order_row_is_flagged() {
    // Row 3 comes before row 2 on disk under ASC — corrupt.
    let schema = schema_one_ck(ClusteringOrder::Asc);
    let partitions = vec![(0usize, vec![ck_int(1), ck_int(3), ck_int(2)])];
    let findings = classify_clustering_row_order(&partitions, &schema);
    assert!(
        findings
            .iter()
            .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow),
        "an out-of-order clustering row must be flagged OutOfOrderKeyOrRow, got {:?}",
        findings
    );
}

#[test]
fn clustering_order_duplicate_row_is_flagged() {
    // Equal consecutive clustering keys are NOT strictly increasing → corrupt.
    let schema = schema_one_ck(ClusteringOrder::Asc);
    let partitions = vec![(0usize, vec![ck_int(5), ck_int(5)])];
    let findings = classify_clustering_row_order(&partitions, &schema);
    assert!(findings
        .iter()
        .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow));
}

#[test]
fn clustering_order_respects_desc_ordering() {
    let schema = schema_one_ck(ClusteringOrder::Desc);
    // DESC on disk stores clustering values descending; 3,2,1 is IN ORDER.
    let ok = vec![(0usize, vec![ck_int(3), ck_int(2), ck_int(1)])];
    assert!(
        classify_clustering_row_order(&ok, &schema).is_empty(),
        "descending rows under DESC clustering order must be clean"
    );
    // Ascending 1,2,3 is OUT OF ORDER under DESC.
    let bad = vec![(0usize, vec![ck_int(1), ck_int(2), ck_int(3)])];
    assert!(
        classify_clustering_row_order(&bad, &schema)
            .iter()
            .any(|f| f.class == VerifyErrorClass::OutOfOrderKeyOrRow),
        "ascending rows under a DESC clustering column must be flagged"
    );
}

#[test]
fn identity_check_detects_count_mismatch() {
    let keys: Vec<Vec<u8>> = (1u32..=3).map(|i| i.to_be_bytes().to_vec()).collect();
    let data = data_partitions(&keys);
    // Only two leaves recovered from the trie (undercount).
    let leaves: Vec<BtiResolvedLeaf> = data
        .iter()
        .take(2)
        .map(|(pos, k)| inline_leaf(k, *pos))
        .collect();
    let detail =
        bti_partition_identity_mismatch(&leaves, &data).expect("undercount must be flagged");
    assert!(detail.contains("2 partition keys"));
    assert!(detail.contains("3 distinct partitions"));
}

// ---- Finding 1 (roborev round 2): tolerate reader filename shapes -------
//
// `SSTableReader::open` does not enforce a "-Data.db" suffix and still opens a
// file whose name it cannot map (it just skips siblings). A reader that opened
// MUST get an `IntegrityCheckResult` from `perform_integrity_check`, not an
// `Err`, so `build_component_set` (the resolution the integrity check ultimately
// drives) must never reject on the suffix.

#[test]
fn build_component_set_matches_reader_base_name_for_non_data_db_name() {
    // A name that does NOT end in "-Data.db" but that the reader's own
    // base-name derivation accepts must resolve to the SAME base name the
    // reader uses for sibling lookup — never an Err (issue #1283, roborev).
    let p = PathBuf::from("/dir/nb-7-big-Statistics.db");
    let set = build_component_set(std::slice::from_ref(&p), p.clone())
        .expect("reader-accepted non-Data.db name must not error");
    assert_eq!(
        Some(set.base_name),
        extract_sstable_base_name(&p),
        "verify base name must match SSTableReader::open's base-name derivation"
    );
}

#[test]
fn build_component_set_degrades_on_unmappable_name() {
    // A name the reader can open but that neither ends in "-Data.db" nor maps
    // via the reader's derivation degrades to the filename minus ".db" (verify
    // what we can) rather than erroring.
    let p = PathBuf::from("/dir/weird.db");
    let set = build_component_set(&[], p.clone()).expect("must degrade, not error");
    assert_eq!(set.base_name, "weird");
    assert_eq!(set.data_path, p);
}

#[test]
fn build_component_set_standard_name_still_resolves_canonically() {
    let p = PathBuf::from("/dir/nb-3-big-Data.db");
    let set =
        build_component_set(std::slice::from_ref(&p), p.clone()).expect("standard name resolves");
    assert_eq!(set.base_name, "nb-3-big");
}

// ---- Finding 2 (roborev round 2): relative Data.db path, empty parent ---
//
// A relative, directory-less filename yields an EMPTY parent from
// `Path::parent()` (Some(""), not None). `generation_dir` must normalize that
// to "." so sibling components are scanned in the current directory — matching
// where `SSTableReader::open` found the file.

#[test]
fn generation_dir_normalizes_empty_parent_to_current_dir() {
    // Relative bare filename opened from the SSTable dir as cwd: empty parent → ".".
    assert_eq!(
        generation_dir(Path::new("nb-1-big-Data.db")),
        Path::new("."),
        "a relative directory-less Data.db must resolve against the current directory"
    );
    // Absolute path keeps its real parent.
    assert_eq!(
        generation_dir(Path::new("/x/y/nb-1-big-Data.db")),
        Path::new("/x/y")
    );
    // Relative path WITH a directory component keeps that directory.
    assert_eq!(
        generation_dir(Path::new("sub/nb-1-big-Data.db")),
        Path::new("sub")
    );
}

// ---------------------------------------------------------------------------
// I4 — a failed `Data.db` stat must be a typed finding, never a silent 0.
//
// Tested at this private seam because the public `verify_sstable` surface
// cannot reach it: `read_dir_files` filters on `is_file()`, so a generation
// whose `Data.db` cannot be stat'ed never resolves at all and the function
// returns `Err` before any check runs. The reachable production case is a
// TOCTOU window — a concurrent compaction unlinking the generation between
// component resolution and Check 5b — which a test cannot schedule. The seam
// is where the behavior lives, so the seam is where it is pinned (same
// rationale as `classify_table_dir_entries`'s injectable-entry tests in
// `cqlite-cli`'s sweep suite).
// ---------------------------------------------------------------------------

/// A `ComponentSet` whose `Data.db` path does not exist, with `CRC.db`
/// present — the exact shape Check 5b sees after a mid-verify unlink.
fn component_set_with_unstattable_data_db(dir: &Path) -> ComponentSet {
    let base_name = "nb-1-big".to_string();
    let crc_path = dir.join("nb-1-big-CRC.db");
    // Content is irrelevant: the fix must return BEFORE `CrcDb::open`. It
    // must merely EXIST, or Check 5b takes its absent-CRC.db early return and
    // the case proves nothing.
    std::fs::write(&crc_path, [0u8; 8]).expect("write CRC.db stand-in");
    let mut present = BTreeMap::new();
    present.insert("CRC.db".to_string(), crc_path);
    ComponentSet {
        base_name,
        format: SsTableFormat::Big,
        present,
        data_path: dir.join("nb-1-big-Data.db"), // deliberately absent
    }
}

#[tokio::test]
async fn i4_unstattable_data_db_is_a_typed_finding_not_a_zero_logical_length() {
    let dir = std::env::temp_dir().join(format!(
        "cqlite-4194-i4-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let components = component_set_with_unstattable_data_db(&dir);

    let mut findings: Vec<VerifyFinding> = Vec::new();
    let mut pending: Vec<PendingLocation> = Vec::new();
    check_uncompressed_crc_db(&dir, &components, &mut findings, &mut pending).await;
    let _ = std::fs::remove_dir_all(&dir);

    // The stat failure is NAMED, and attributed to Data.db — not to CRC.db.
    // Pre-fix `data_len` silently became 0, which (a) made every CRC.db look
    // oversized for a "0-byte" Data.db, so the only finding produced was an
    // `UncompressedChunkCrcMismatch` blaming `CRC.db` for a Data.db problem,
    // and (b) would have become `PendingLocation::logical_len == 0`, collapsing
    // the last partition's extent to empty so it could never be reported for
    // any damage in the file — blocker #2's silent drop by another route.
    assert_eq!(
        findings.len(),
        1,
        "expected exactly one finding for the stat failure: {findings:#?}"
    );
    assert_eq!(findings[0].class, VerifyErrorClass::MissingComponent);
    assert_eq!(
        findings[0].component, "Data.db",
        "the failure is Data.db's, not CRC.db's: {:#?}",
        findings[0]
    );
    assert!(
        findings[0].detail.contains("cannot stat Data.db"),
        "the cause must be named: {}",
        findings[0].detail
    );
    assert!(
        pending.is_empty(),
        "no location may be emitted from an unmeasured logical length; got {} \
         (PendingLocation is not Debug, so the count is the observable)",
        pending.len()
    );
}

// ---------------------------------------------------------------------------
// #4194 blocker #1, option (a): the BTI corroboration gate, BOTH directions.
//
// These two cases are the ONLY place the CORROBORATED branch is reachable, and
// that is a measured fact about the current check set, not a convenience:
//
//   * A BTI `PendingLocation` can only come from `check_inline_chunk_crc` (a
//     chunk CRC/decompression failure) or from `ChunkOffsetOutOfBounds`.
//   * The read path validates the SAME chunk CRC during decompression, so any
//     corruption that produces the first ALSO fails `full_row_scan_partitions`
//     — measured directly: flipping a bit in chunk 0's stored TRAILING CRC32
//     (leaving the payload intact, to try to keep the scan healthy) still
//     fails the scan, because the reader checks that CRC too.
//   * `ChunkOffsetOutOfBounds` sets `compression_metadata_corrupt`, which
//     skips the scan outright.
//
// So end-to-end, a BTI location is now ALWAYS `Unresolved` — the accepted cost
// of option (a), and precisely what the 0.19 corroboration-state follow-up
// exists to recover. Pinning the positive branch here is therefore not
// optional: without it the gate could be stuck-false and no test in the
// repository would notice.
//
// `finalize_locations`'s BTI arm reads its leaves from the `bti_leaves`
// ARGUMENT and performs no file I/O at all (only the BIG arm opens
// `Index.db`), so the directory below never needs real components.
// ---------------------------------------------------------------------------

fn bti_component_set(dir: &Path) -> ComponentSet {
    ComponentSet {
        base_name: "da-2-bti".to_string(),
        format: SsTableFormat::Bti,
        present: BTreeMap::new(),
        data_path: dir.join("da-2-bti-Data.db"),
    }
}

fn pending_at(finding_index: usize, logical_len: u64) -> PendingLocation {
    PendingLocation {
        finding_index,
        component: "Data.db".to_string(),
        byte_offset: 0,
        byte_len: 16,
        chunk_index: Some(0),
        damaged_logical: (0, 16),
        logical_len,
        logical_len_source: LogicalLenSource::Declared,
        anchor: crate::storage::sstable::verify_location::PhysicalAnchor::DamagedExtent,
    }
}

/// `findings[0]` must be a class/component that does NOT itself distrust the
/// boundary source, so the corroboration gate is the only thing being
/// measured (`ChunkDecompressionError` on `Data.db` is exactly the real shape).
fn chunk_finding() -> VerifyFinding {
    VerifyFinding::new(
        VerifyErrorClass::ChunkDecompressionError,
        "Data.db",
        "staged chunk failure".to_string(),
    )
}

async fn resolve_one_bti_location(
    leaves: &[BtiResolvedLeaf],
    corroborated: bool,
) -> PartitionResolution {
    let dir = std::env::temp_dir().join(format!(
        "cqlite-4194-bti-gate-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let components = bti_component_set(&dir);
    let mut findings = vec![chunk_finding()];
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));

    verify_location::finalize_locations(
        &dir,
        &components,
        &mut findings,
        vec![pending_at(0, 4096)],
        Some(leaves),
        None,
        corroborated,
        platform,
    )
    .await;
    let _ = std::fs::remove_dir_all(&dir);

    findings[0]
        .location
        .as_ref()
        .expect("the pending location must have been written back")
        .partitions
        .clone()
}

#[tokio::test]
async fn bti_gate_uncorroborated_refuses_by_name() {
    // `inline_leaf` is this suite's existing `RowsOffset`-shaped helper: an
    // authoritative inline raw key, so resolution needs no scan position map
    // and the corroboration gate is the ONLY thing under measurement.
    let leaves = vec![inline_leaf(b"k0", 0)];
    let res = resolve_one_bti_location(&leaves, false).await;
    match res {
        PartitionResolution::Unresolved(cause) => assert_eq!(
            cause, BTI_IDENTITY_UNCORROBORATED,
            "an uncorroborated BTI source must refuse under its OWN cause, not a generic one"
        ),
        PartitionResolution::Resolved { keys, .. } => panic!(
            "an uncorroborated BTI trie must never resolve: a corruption that keeps a leaf's \
             prefix while rewriting its payload resolves confidently to the WRONG key, and the \
             identity cross-check is the only thing that can see it. Got {keys:?}"
        ),
    }
}

#[tokio::test]
async fn bti_gate_corroborated_still_resolves() {
    // The gate is a GATE, not an unconditional refusal: when the cross-check
    // did run and agreed, the leaves are trusted exactly as before. If this
    // ever starts failing, option (a) has become a blanket BTI refusal and the
    // `corroborated` flag is dead.
    let leaves = vec![inline_leaf(b"k0", 0)];
    let res = resolve_one_bti_location(&leaves, true).await;
    match res {
        PartitionResolution::Resolved { keys, truncated } => {
            assert_eq!(truncated, 0);
            assert_eq!(
                keys.iter().map(|k| k.key_hex.as_str()).collect::<Vec<_>>(),
                vec!["6b30"],
                "the corroborated path must still name the intersecting partition"
            );
        }
        PartitionResolution::Unresolved(cause) => panic!(
            "corroborated BTI leaves must still resolve; refusing here would make the \
             corroboration flag dead and option (a) a blanket refusal: {cause}"
        ),
    }
}
