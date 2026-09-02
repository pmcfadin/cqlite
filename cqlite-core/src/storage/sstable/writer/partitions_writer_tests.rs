//! Unit tests for [`super`] — the BTI `Partitions.db` / `Rows.db` trie
//! writers. Split out of `partitions_writer.rs` to respect the campsite
//! file-size rule (epic #1135); included via `#[path]` so `use super::*`
//! still reaches the parent module's private + `#[cfg(test)]` items
//! (issue #1679).

use super::*;
use crate::storage::sstable::bti::sized_ints;
use crate::storage::sstable::bti::{
    encode_partition_key_for_bti_trie, lookup_raw_key_in_bti_partitions_db, BtiPartitionLocation,
};
use std::io::Cursor;

/// `sized_ints_non_zero_size` must agree with the reader's `non_zero_size`.
#[test]
fn sized_int_size_matches_reader() {
    let values = [
        0i64,
        1,
        -1,
        127,
        -128,
        128,
        -129,
        255,
        -256,
        32767,
        -32768,
        32768,
        -32769,
        i64::MAX,
        i64::MIN,
        !0i64,
        !63i64,
        !125i64,
        !1000i64,
        !1_000_000i64,
        !300_000_000_000i64,
    ];
    for v in values {
        assert_eq!(
            sized_ints_non_zero_size(v),
            sized_ints::non_zero_size(v),
            "size mismatch for {v}"
        );
    }
}

/// A written SizedInt round-trips through the reader's `read`.
#[test]
fn sized_int_write_read_roundtrip() {
    for v in [0i64, !0i64, !63i64, !125i64, !1_000_000i64, i64::MIN] {
        let n = sized_ints_non_zero_size(v);
        let mut buf = Vec::new();
        write_sized_int_be(&mut buf, v, n);
        assert_eq!(buf.len(), n);
        let mut cur = Cursor::new(buf);
        let got = sized_ints::read(&mut cur, n).unwrap();
        assert_eq!(got, v, "SizedInt roundtrip failed for {v}");
    }
}

/// Build a trie from raw keys, then look every key back up through the
/// reader and assert the resolved Data.db offset matches.
fn assert_roundtrip(keys_and_offsets: &[(Vec<u8>, u64)]) {
    let mut w = PartitionsTrieWriter::new();
    for (k, off) in keys_and_offsets {
        w.add_partition(k, *off);
    }
    let bytes = w.finish().expect("finish trie");
    assert!(bytes.len() >= 8, "trie must include 8-byte footer");

    for (k, expected) in keys_and_offsets {
        let mut cur = Cursor::new(bytes.clone());
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, k)
            .expect("lookup")
            .unwrap_or_else(|| panic!("key {k:?} not found in written trie"));
        match loc {
            BtiPartitionLocation::DataOffset(got) => assert_eq!(
                got, *expected,
                "key {k:?}: expected DataOffset({expected}) got DataOffset({got})"
            ),
            BtiPartitionLocation::RowsOffset(r) => {
                panic!("key {k:?}: phase-1 writer must emit DataOffset, got RowsOffset({r})")
            }
        }
    }
}

/// Finding 1 (roborev #908): the partition leaf hash byte must be the
/// **canonical** Cassandra value — `(byte) DecoratedKey.filterHashLowerBits()`,
/// i.e. the low 8 bits of `h2` (the second 64-bit Murmur3 word) — NOT the
/// byte-comparable token's high byte that the phase-1 placeholder emitted.
///
/// These expected bytes are read directly from the real, Cassandra-produced
/// `da-2-bti-Partitions.db` fixture at
/// `test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11`.
/// Its three PayloadOnly leaves store:
///   UUID 2222… → hash byte 0x24 (Data.db offset 0)
///   UUID 1111… → hash byte 0x22 (Data.db offset 63)
///   UUID 3333… → hash byte 0xf4 (Data.db offset 125)
/// (hexdump: `08 24 ff | 08 22 c0 | 08 f4 82 …`).
#[test]
fn canonical_filter_hash_byte_matches_real_bti_fixture() {
    // (raw partition key bytes, expected canonical hash byte from the fixture)
    let vectors: [(Vec<u8>, u8); 3] = [
        (vec![0x22u8; 16], 0x24),
        (vec![0x11u8; 16], 0x22),
        (vec![0x33u8; 16], 0xf4),
    ];
    for (raw_key, expected) in vectors {
        let got = filter_hash_byte(&raw_key);
        assert_eq!(
            got, expected,
            "canonical hash byte mismatch for key {raw_key:02x?}: \
                 expected 0x{expected:02x} (from real da-2-bti-Partitions.db), got 0x{got:02x}"
        );
    }

    // And confirm the placeholder it replaced (token/h1 high byte) would have
    // produced *different*, non-canonical values — guarding against a regression
    // that reintroduces the token-derived byte.
    for (raw_key, expected) in [(vec![0x22u8; 16], 0x90u8), (vec![0x11u8; 16], 0xbc)] {
        let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&raw_key);
        let placeholder = (((token as u64) ^ 0x8000_0000_0000_0000u64) >> 56) as u8;
        assert_eq!(placeholder, expected, "placeholder reference value drifted");
        assert_ne!(
            placeholder,
            filter_hash_byte(&raw_key),
            "canonical hash byte must differ from the old token-derived placeholder"
        );
    }
}

/// The canonical hash byte the writer emits round-trips: building a trie that
/// includes these partitions yields leaf payloads whose first byte equals the
/// canonical hash byte (decoded straight from the serialized bytes).
#[test]
fn written_leaf_hash_byte_is_canonical() {
    let raw_key = vec![0x22u8; 16];
    let mut w = PartitionsTrieWriter::new();
    w.add_partition(&raw_key, 0);
    let bytes = w.finish().expect("finish trie");
    // The first written node is the only leaf (PayloadOnly). Its layout is
    // [header=0x08][hash_byte][SizedInts position…]; header 0x08 = payloadBits 8.
    assert_eq!(
        bytes[0], 0x08,
        "expected PayloadOnly leaf with payloadBits=8"
    );
    assert_eq!(
        bytes[1],
        filter_hash_byte(&raw_key),
        "serialized leaf hash byte must be the canonical value"
    );
    assert_eq!(bytes[1], 0x24, "canonical hash byte for UUID 2222… is 0x24");
}

/// Issue #1681 (S4): each partition key is hashed with Murmur3 exactly ONCE.
/// Pre-fold this read 2N (128 for 64 keys: token + filter hash) and FAILED.
/// The counter is thread-local, so `add_partition` (synchronous) records only
/// this thread's hashes — deterministic under parallel test runs.
#[test]
fn one_murmur3_per_partition_key() {
    use crate::util::cassandra_murmur3::{murmur3_call_count, reset_murmur3_call_count};

    reset_murmur3_call_count();
    let mut w = PartitionsTrieWriter::new();
    for i in 0u64..64 {
        let mut k = vec![0u8; 16];
        k[0..8].copy_from_slice(&i.to_be_bytes());
        w.add_partition(&k, i);
    }
    assert_eq!(
        murmur3_call_count(),
        64,
        "expected 1 murmur3 per key (got {}); pre-#1681 this was 128 (2N)",
        murmur3_call_count()
    );
}

#[test]
fn empty_trie_is_empty_bytes() {
    let w = PartitionsTrieWriter::new();
    assert!(w.finish().unwrap().is_empty());
}

#[test]
fn single_partition_roundtrip() {
    assert_roundtrip(&[(vec![0x11u8; 16], 0)]);
}

#[test]
fn three_uuid_partitions_roundtrip() {
    assert_roundtrip(&[
        (vec![0x11u8; 16], 63),
        (vec![0x22u8; 16], 0),
        (vec![0x33u8; 16], 125),
    ]);
}

/// Issue #1678: interior partition raw keys must NOT be retained. Only the two
/// boundary raw keys (min/max trie key) are kept for the first/last-key region.
/// On `main` (per-entry `raw_key`) the accessor reported `N × keylen`; here it
/// is bounded by first+last only.
#[test]
fn interior_raw_keys_are_not_retained() {
    let mut w = PartitionsTrieWriter::new();
    for i in 0u64..1000 {
        let mut k = vec![0u8; 16];
        k[0..8].copy_from_slice(&i.to_be_bytes());
        w.add_partition(&k, i);
    }
    // Only the first and last (16 bytes each) may be retained.
    assert!(
        w.retained_raw_key_bytes() <= 32,
        "retained {} raw-key bytes; expected <= 32 (first+last only)",
        w.retained_raw_key_bytes()
    );
}

/// Issue #1678: the boundary raw keys tracked incrementally by min/max trie
/// key must equal what `entries.first()/last()` would yield after the sort, so
/// `finish` emits an unchanged first/last-key region. Partitions are added in a
/// deliberately scrambled order (not token order) to prove first/last follow
/// the trie-key sort, not insertion order.
#[test]
fn boundary_raw_keys_match_sorted_first_last() {
    let raws: Vec<Vec<u8>> = [0x33u8, 0x11, 0x88, 0x22, 0x55, 0x44]
        .iter()
        .map(|b| vec![*b; 16])
        .collect();
    let mut w = PartitionsTrieWriter::new();
    for (i, r) in raws.iter().enumerate() {
        w.add_partition(r, i as u64);
    }

    // Oracle: sort the same keys by trie key and take first/last raw bytes.
    let mut sorted: Vec<(_, Vec<u8>)> = raws
        .iter()
        .map(|r| (encode_partition_key_for_bti_trie(r), r.clone()))
        .collect();
    sorted.sort_by_key(|a| a.0);
    let expect_first = sorted.first().map(|(_, r)| r.clone()).unwrap();
    let expect_last = sorted.last().map(|(_, r)| r.clone()).unwrap();

    assert_eq!(
        w.first_raw.as_ref().map(|(_, k)| k.clone()),
        Some(expect_first),
        "first boundary raw key must be the min-trie-key partition"
    );
    assert_eq!(
        w.last_raw.as_ref().map(|(_, k)| k.clone()),
        Some(expect_last),
        "last boundary raw key must be the max-trie-key partition"
    );
}

#[test]
fn large_offsets_roundtrip() {
    assert_roundtrip(&[
        (vec![0xA1u8; 16], 1_000_000),
        (vec![0xB2u8; 16], 300_000_000_000),
        (vec![0xC3u8; 16], 5),
    ]);
}

#[test]
fn many_partitions_roundtrip() {
    // Exercise multi-level fan-out and varied token bytes.
    let mut data = Vec::new();
    for i in 0u64..200 {
        let mut key = vec![0u8; 16];
        key[0..8].copy_from_slice(&i.to_be_bytes());
        key[8..16].copy_from_slice(&(i.wrapping_mul(2654435761)).to_be_bytes());
        data.push((key, i * 37));
    }
    assert_roundtrip(&data);
}

// ── Issue #1679: single-sweep incremental emitter byte-identity ─────────

/// Construct a partition trie entry directly (bypassing the Murmur3
/// transform) so a test can pin an exact trie shape.
fn entry_for(key: [u8; 9], hash_byte: u8, offset: u64) -> PartitionTrieEntry {
    PartitionTrieEntry {
        key,
        hash_byte,
        payload: PartitionPayload::DataOffset(offset),
    }
}

/// Sort + de-duplicate exactly as [`PartitionsTrieWriter::finish`] does
/// before emitting.
fn sorted_unique(mut entries: Vec<PartitionTrieEntry>) -> Vec<PartitionTrieEntry> {
    entries.sort_by_key(|a| a.key);
    entries.dedup_by(|a, b| a.key == b.key);
    entries
}

/// The incremental single-sweep emitter (#1679) must produce byte-identical
/// output — and the same root offset — as the reference whole-tree post-order
/// walk over the same sorted, unique entries.
fn assert_emit_matches_reference(entries: &[PartitionTrieEntry]) {
    let ref_root = build_trie(entries);
    let mut ref_buf = Vec::new();
    let ref_off = write_node(&ref_root, &mut ref_buf).expect("reference write");

    let mut new_buf = Vec::new();
    let new_off = emit_partitions_trie(entries, &mut new_buf).expect("incremental emit");

    assert_eq!(
        new_off, ref_off,
        "root offset must match the reference whole-tree walk"
    );
    assert_eq!(
        new_buf,
        ref_buf,
        "incremental emitter must be byte-identical to the reference walk \
             (len new={} ref={})",
        new_buf.len(),
        ref_buf.len()
    );
}

/// A tiny deterministic PRNG (SplitMix64) so byte-identity is exercised over
/// pseudo-random key shapes without a rand dependency.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[test]
fn emit_matches_reference_single_entry() {
    let entries = sorted_unique(vec![entry_for([0x40, 1, 2, 3, 4, 5, 6, 7, 8], 0xAB, 42)]);
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_diverge_last_byte() {
    // Two keys sharing the first 8 bytes (LCP 8) → same depth-8 node, two
    // leaf children.
    let entries = sorted_unique(vec![
        entry_for([0x40, 1, 2, 3, 4, 5, 6, 7, 0x10], 0x01, 0),
        entry_for([0x40, 1, 2, 3, 4, 5, 6, 7, 0x20], 0x02, 63),
    ]);
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_diverge_first_byte() {
    // Two keys differing at byte 0 (LCP 0) → root fans out to two full
    // depth chains.
    let entries = sorted_unique(vec![
        entry_for([0x10, 0, 0, 0, 0, 0, 0, 0, 0], 0x01, 0),
        entry_for([0x90, 0, 0, 0, 0, 0, 0, 0, 0], 0x02, 100),
    ]);
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_all_same_prefix() {
    // Many keys sharing the first 8 bytes; the depth-8 node accumulates a
    // large (but < 256) sparse fan-out of leaves.
    let entries = sorted_unique(
        (0u8..200)
            .map(|b| entry_for([0x40, 7, 7, 7, 7, 7, 7, 7, b], b, b as u64 * 3))
            .collect(),
    );
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_dense_at_depth_8() {
    // Exactly 256 children at the depth-8 node ⇒ a Dense node (Sparse cannot
    // encode 256). Exercises the incremental Dense chooser.
    let entries = sorted_unique(
        (0u16..=255)
            .map(|b| entry_for([0x40, 1, 2, 3, 4, 5, 6, 7, b as u8], b as u8, b as u64 * 17))
            .collect(),
    );
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_dense_at_interior_depth() {
    // 256 divergent branches at an INTERIOR depth (byte 4): each branch is a
    // distinct chain to its own leaf, so the depth-4 node becomes Dense while
    // deeper nodes are Sparse single-child chains.
    let entries = sorted_unique(
        (0u16..=255)
            .map(|b| entry_for([0x40, 1, 2, 3, b as u8, 0, 0, 0, 0], b as u8, b as u64))
            .collect(),
    );
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_maximally_divergent() {
    // Keys that diverge as early as possible across the whole token range.
    let entries = sorted_unique(
        (0u16..=255)
            .map(|b| {
                let mut k = [0u8; 9];
                k[0] = 0x40;
                k[1] = b as u8;
                k[8] = b as u8;
                entry_for(k, b as u8, b as u64 * 5)
            })
            .collect(),
    );
    assert_emit_matches_reference(&entries);
}

#[test]
fn emit_matches_reference_pseudorandom_shapes() {
    for (seed, count) in [(1u64, 3usize), (2, 17), (3, 200), (4, 2000), (5, 5000)] {
        let mut state = seed;
        let entries = sorted_unique(
            (0..count)
                .map(|_| {
                    let mut k = [0u8; 9];
                    k[0] = 0x40;
                    let token = splitmix64(&mut state);
                    k[1..9].copy_from_slice(&token.to_be_bytes());
                    let hb = splitmix64(&mut state) as u8;
                    let off = splitmix64(&mut state) % 1_000_000;
                    entry_for(k, hb, off)
                })
                .collect(),
        );
        assert_emit_matches_reference(&entries);
    }
}

#[test]
fn emit_matches_reference_realistic_murmur_keys() {
    // Keys produced by the real BTI encoding for varied raw partition keys.
    let entries = sorted_unique(
        (0u64..500)
            .map(|i| {
                let mut raw = vec![0u8; 16];
                raw[0..8].copy_from_slice(&i.to_be_bytes());
                raw[8..16].copy_from_slice(&i.wrapping_mul(2654435761).to_be_bytes());
                let key = encode_partition_key_for_bti_trie(&raw);
                entry_for(key, filter_hash_byte(&raw), i * 13)
            })
            .collect(),
    );
    assert_emit_matches_reference(&entries);
}

/// Pinned golden: the FULL `finish()` output for the three canonical UUID
/// partitions from the real `da-2-bti` fixture. Guards the entire
/// `Partitions.db` byte layout (trie nodes + first/last-key region + 24-byte
/// footer) against any drift in the incremental emitter (#1679). Captured
/// from the pre-#1679 implementation; MUST stay byte-identical.
#[test]
fn finish_three_uuid_partitions_matches_pinned_golden() {
    let mut w = PartitionsTrieWriter::new();
    w.add_partition(&[0x11u8; 16], 63);
    w.add_partition(&[0x22u8; 16], 0);
    w.add_partition(&[0x33u8; 16], 125);
    let bytes = w.finish().expect("finish");
    // Full Partitions.db layout (trie nodes + first/last-key region +
    // 24-byte footer) for the three canonical UUID partitions. The
    // `emit_matches_reference_*` tests independently prove the trie-node
    // bytes equal the pre-#1679 whole-tree walk; this pins the entire
    // `finish()` output against layout drift.
    const GOLDEN_HEX: &str = "0824ff50016a03500120045001910450011e045001f9045001a4045001d5040822c0500162035001340450014d045001640450013804500162045001820408f4825001b803500136045001c804500105045001ba04500142045001d904500390bcf942230450014008001022222222222222222222222222222222001033333333333333333333333333333333000000000000006900000000000000030000000000000065";
    let golden = decode_hex(GOLDEN_HEX);
    assert_eq!(
        bytes,
        golden,
        "Partitions.db bytes drifted from the pinned golden.\n got: {}\nwant: {}",
        encode_hex(&bytes),
        GOLDEN_HEX
    );
}

fn decode_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn encode_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn duplicate_key_is_rejected() {
    let mut w = PartitionsTrieWriter::new();
    // Identical raw key bytes ⇒ identical token ⇒ identical trie key.
    w.add_partition(&[0x55u8; 16], 0);
    w.add_partition(&[0x55u8; 16], 100);
    assert!(w.finish().is_err());
}

/// Finding 1 (issue #766 review): an internal node whose fan-out covers all
/// 256 possible transition bytes must serialize (as a Dense node) and round-
/// trip through the reader. Previously the serializer rejected count == 256.
///
/// We construct the trie directly so we can guarantee a full 256-byte fan-out
/// at one node, independent of Murmur3 token distribution.
#[test]
fn full_256_fanout_internal_node_serializes_and_roundtrips() {
    use std::collections::BTreeMap;

    // Build a two-level trie: root has one child byte 0xFF leading to an
    // internal node with all 256 transition bytes, each pointing at a leaf.
    let mut inner_children: BTreeMap<u8, TrieBuildNode> = BTreeMap::new();
    for b in 0u16..=255 {
        inner_children.insert(
            b as u8,
            TrieBuildNode::Leaf {
                hash_byte: b as u8,
                payload: PartitionPayload::DataOffset((b as u64) * 17),
            },
        );
    }
    let inner = TrieBuildNode::Internal {
        children: inner_children,
    };
    let mut root_children: BTreeMap<u8, TrieBuildNode> = BTreeMap::new();
    root_children.insert(0xFF, inner);
    let root = TrieBuildNode::Internal {
        children: root_children,
    };

    let bytes = serialize_trie(&root).expect("256-fan-out node must serialize");

    // Walk the trie via the reader's node parser for each terminal byte,
    // following root[0xFF] then inner[b], and confirm the resolved leaf
    // payload offset matches what we wrote.
    for b in 0u16..=255 {
        let key = [0xFFu8, b as u8];
        let loc = lookup_key_in_trie(&bytes, &key)
            .unwrap_or_else(|| panic!("byte {b} not found in 256-fan-out trie"));
        assert_eq!(
            loc,
            (b as u64) * 17,
            "byte {b}: wrong Data.db offset resolved"
        );
    }
}

/// Resolve a raw trie key (the byte-comparable bytes traversed from the
/// root) to its leaf's decoded Data.db offset, using the production BTI
/// parser/lookup path. `key` is the already-encoded trie key (no Murmur3
/// transform applied), so `lookup_partition_in_bti_file` walks it directly.
fn lookup_key_in_trie(bytes: &[u8], key: &[u8]) -> Option<u64> {
    use crate::storage::sstable::bti::lookup_partition_in_bti_file;
    let mut cur = Cursor::new(bytes.to_vec());
    match lookup_partition_in_bti_file(&mut cur, key).ok()?? {
        BtiPartitionLocation::DataOffset(o) => Some(o),
        BtiPartitionLocation::RowsOffset(_) => None,
    }
}

// ── Rows.db writer (#910) ────────────────────────────────────────────

/// The unsigned-VInt writer must be the exact inverse of the reader's
/// `read_unsigned_vint_from_slice` for a wide spread of values, including
/// the 1-/2-/.../9-byte boundaries.
#[test]
fn unsigned_vint_roundtrips_through_reader() {
    use crate::storage::sstable::bti::parser::read_unsigned_vint_from_slice_for_test as read_u;
    let values: [u64; 20] = [
        0,
        1,
        63,
        64,
        127,
        128,
        255,
        256,
        16_383,
        16_384,
        65_535,
        65_536,
        1_000_000,
        300_000_000_000,
        (1u64 << 35) - 1,
        1u64 << 35,
        (1u64 << 49) - 1,
        1u64 << 49,
        u64::MAX - 1,
        u64::MAX,
    ];
    for v in values {
        let mut buf = Vec::new();
        write_unsigned_vint(&mut buf, v);
        let (got, n) = read_u(&buf).expect("read");
        assert_eq!(got, v, "unsigned vint roundtrip failed for {v}: {buf:02x?}");
        assert_eq!(n, buf.len(), "consumed all bytes for {v}");
    }
}

/// The signed-VInt (ZigZag) writer must be the exact inverse of the reader's
/// `read_signed_vint_from_slice` for positive, negative and zero deltas.
#[test]
fn signed_vint_roundtrips_through_reader() {
    use crate::storage::sstable::bti::parser::read_signed_vint_from_slice_for_test as read_s;
    for v in [
        0i64,
        1,
        -1,
        10,
        -10,
        127,
        -128,
        1000,
        -1000,
        i32::MIN as i64,
        i64::MAX,
        i64::MIN,
    ] {
        let mut buf = Vec::new();
        write_signed_vint(&mut buf, v);
        let (got, _n) = read_s(&buf).expect("read");
        assert_eq!(got, v, "signed vint roundtrip failed for {v}: {buf:02x?}");
    }
}

/// A wide partition's Rows.db trie must round-trip through the production
/// reader: `resolve_rows_db_entry` recovers the trie root + metadata, and
/// `iterate_rows_in_bti_trie` yields the exact separators + block offsets we
/// wrote, in ascending order.
#[test]
fn rows_db_single_wide_partition_roundtrips() {
    use crate::storage::sstable::bti::{iterate_rows_in_bti_trie, resolve_rows_db_entry};

    // int clustering separators ck = 8,16,24 → OSS50 sign-flipped 4 bytes.
    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let blocks = vec![
        RowIndexBlock {
            separator_key: sep(8),
            block_offset: 16_512,
            open_marker: None,
        },
        RowIndexBlock {
            separator_key: sep(16),
            block_offset: 33_024,
            open_marker: None,
        },
        RowIndexBlock {
            separator_key: sep(24),
            block_offset: 49_536,
            open_marker: None,
        },
    ];

    let raw_pk = 1i32.to_be_bytes().to_vec();
    let mut w = RowsTrieWriter::new();
    w.add_partition_row_index(&raw_pk, 0, blocks.clone(), None);
    let (rows_db, offsets) = w.finish().expect("finish Rows.db");
    assert_eq!(offsets.len(), 1);
    let rows_offset = offsets[0] as usize;

    // Feeding RowsOffset directly as a trie root must FAIL (it is the entry).
    assert!(
        iterate_rows_in_bti_trie(&rows_db, rows_offset).is_err(),
        "RowsOffset is a TrieIndexEntry, not a trie root"
    );

    // resolve_rows_db_entry recovers the header.
    let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("resolve entry");
    assert_eq!(header.data_position, 0);
    assert_eq!(header.block_count, blocks.len() as u32);
    assert_eq!(header.partition_deletion, None, "LIVE sentinel → None");

    // Traversal from the recovered root yields our separators + offsets.
    let entries = iterate_rows_in_bti_trie(
        &rows_db,
        // Issue #3002: the writer's own root must PASS structural validation.
        header
            .trie_root
            .expect("the written root must validate structurally")
            .offset(),
    )
    .expect("traverse from root");
    assert_eq!(entries.len(), blocks.len());
    for (got, expected) in entries.iter().zip(blocks.iter()) {
        assert_eq!(got.0, expected.separator_key, "separator key");
        assert_eq!(got.1.data_offset, expected.block_offset, "block offset");
        assert_eq!(got.1.open_marker, None);
    }
}

/// Multiple wide partitions concatenate in Rows.db; each RowsOffset resolves
/// to its OWN trie root and metadata (no cross-talk between partitions).
#[test]
fn rows_db_multiple_wide_partitions_roundtrip() {
    use crate::storage::sstable::bti::{iterate_rows_in_bti_trie, resolve_rows_db_entry};

    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let mk = |base: u64| {
        vec![
            RowIndexBlock {
                separator_key: sep(8),
                block_offset: base + 16_512,
                open_marker: None,
            },
            RowIndexBlock {
                separator_key: sep(16),
                block_offset: base + 33_024,
                open_marker: None,
            },
        ]
    };

    let mut w = RowsTrieWriter::new();
    w.add_partition_row_index(&1i32.to_be_bytes(), 0, mk(0), None);
    w.add_partition_row_index(&2i32.to_be_bytes(), 700_000, mk(0), None);
    w.add_partition_row_index(&3i32.to_be_bytes(), 1_400_000, mk(0), None);
    let (rows_db, offsets) = w.finish().expect("finish");
    assert_eq!(offsets.len(), 3);

    let data_positions = [0u64, 700_000, 1_400_000];
    for (i, &ro) in offsets.iter().enumerate() {
        let header = resolve_rows_db_entry(&rows_db, ro as usize).expect("resolve");
        assert_eq!(
            header.data_position, data_positions[i],
            "partition {i} data position"
        );
        assert_eq!(header.block_count, 2);
        let entries = iterate_rows_in_bti_trie(
            &rows_db,
            // Issue #3002: the writer's own root must PASS structural validation.
            header
                .trie_root
                .expect("the written root must validate structurally")
                .offset(),
        )
        .expect("traverse");
        assert_eq!(entries.len(), 2, "partition {i} block count");
        assert_eq!(entries[0].0, sep(8));
        assert_eq!(entries[1].0, sep(16));
    }
}

/// An empty RowsTrieWriter yields 0-byte Rows.db with no offsets — exactly
/// what Cassandra emits for a narrow-only BTI SSTable (verified against the
/// real `simple_table`/`collection_table`/`ttl_table` 0-byte `da-2-bti-Rows.db`
/// fixtures), and the reader accepts it.
#[test]
fn rows_db_empty_writer_is_zero_bytes() {
    let w = RowsTrieWriter::new();
    let (bytes, offsets) = w.finish().expect("finish empty");
    assert!(bytes.is_empty(), "empty Rows.db must be 0 bytes");
    assert!(offsets.is_empty());
}

/// A wide partition with an open-marker block round-trips the DeletionTime.
#[test]
fn rows_db_open_marker_roundtrips() {
    use crate::storage::sstable::bti::{iterate_rows_in_bti_trie, resolve_rows_db_entry};
    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let blocks = vec![
        RowIndexBlock {
            separator_key: sep(8),
            block_offset: 16_512,
            open_marker: Some((1_700_000_000, 1_700_000_000_000_000)),
        },
        RowIndexBlock {
            separator_key: sep(16),
            block_offset: 33_024,
            open_marker: None,
        },
    ];
    let mut w = RowsTrieWriter::new();
    w.add_partition_row_index(&7i32.to_be_bytes(), 0, blocks.clone(), None);
    let (rows_db, offsets) = w.finish().expect("finish");
    let header = resolve_rows_db_entry(&rows_db, offsets[0] as usize).expect("resolve");
    let entries = iterate_rows_in_bti_trie(
        &rows_db,
        // Issue #3002: the writer's own root must PASS structural validation.
        header
            .trie_root
            .expect("the written root must validate structurally")
            .offset(),
    )
    .expect("traverse");
    assert_eq!(
        entries[0].1.open_marker,
        Some((1_700_000_000, 1_700_000_000_000_000))
    );
    assert_eq!(entries[1].1.open_marker, None);
}

/// Partition-level deletion (non-LIVE) round-trips through the TrieIndexEntry.
#[test]
fn rows_db_partition_deletion_roundtrips() {
    use crate::storage::sstable::bti::resolve_rows_db_entry;
    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let blocks = vec![
        RowIndexBlock {
            separator_key: sep(8),
            block_offset: 16_512,
            open_marker: None,
        },
        RowIndexBlock {
            separator_key: sep(16),
            block_offset: 33_024,
            open_marker: None,
        },
    ];
    let mut w = RowsTrieWriter::new();
    w.add_partition_row_index(&9i32.to_be_bytes(), 0, blocks, Some((1234, 5678)));
    let (rows_db, offsets) = w.finish().expect("finish");
    let header = resolve_rows_db_entry(&rows_db, offsets[0] as usize).expect("resolve");
    assert_eq!(header.partition_deletion, Some((1234, 5678)));
}

/// A non-ascending separator set is rejected (the trie cannot encode it and
/// DFS expects strict order).
#[test]
fn rows_db_rejects_non_ascending_separators() {
    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let blocks = vec![
        RowIndexBlock {
            separator_key: sep(16),
            block_offset: 16_512,
            open_marker: None,
        },
        RowIndexBlock {
            separator_key: sep(8),
            block_offset: 33_024,
            open_marker: None,
        },
    ];
    let mut w = RowsTrieWriter::new();
    w.add_partition_row_index(&1i32.to_be_bytes(), 0, blocks, None);
    assert!(w.finish().is_err());
}

/// An EMPTY separator (`ByteComparable.EMPTY`, Cassandra's canonical block-0
/// separator) is REFUSED, not mis-encoded (roborev finding on #3002). Its
/// canonical home is the trie ROOT node's own payload, which this builder cannot
/// express; the previous "defensive" fallback filed it under transition byte
/// `0x00`, which reads back as the separator `[0x00]` — silently wrong bytes where
/// an error belongs.
#[test]
fn rows_db_rejects_empty_separator_instead_of_misencoding_it() {
    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let blocks = vec![
        // Block 0 under ByteComparable.EMPTY (what a canonical Cassandra
        // `RowIndexWriter.add` stores as the root payload).
        RowIndexBlock {
            separator_key: Vec::new(),
            block_offset: 7,
            open_marker: None,
        },
        RowIndexBlock {
            separator_key: sep(8),
            block_offset: 16_512,
            open_marker: None,
        },
    ];
    let mut w = RowsTrieWriter::new();
    w.add_partition_row_index(&1i32.to_be_bytes(), 0, blocks, None);
    let err = w
        .finish()
        .expect_err("an empty row-index separator must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("empty") && msg.contains("root"),
        "the error must name the empty separator and the root-payload gap, got: {msg}"
    );
}

/// WRITER ↔ READER COUPLING (roborev #3002): every row-index ROOT this writer emits
/// must have a node ordinal OUTSIDE the set `validate_rows_trie_root` rejects.
///
/// That rejection (`SINGLE_NOPAYLOAD_4`/`_12`) is NOT a format invariant — a
/// payload-less root with a single close child is a legal trie node, and
/// `TrieNode.typeFor` would choose exactly that encoding for it. The reader is safe
/// only because `write_row_node` unconditionally routes internal nodes through
/// `write_sparse`, and nothing else couples that choice to the reader's accepted set.
/// So assert it here: if a future single-child size optimisation lands in the writer,
/// this test FAILS instead of silently making every row index CQLite writes
/// unreadable by CQLite's own reader (a full-partition fallback on every clustering
/// slice, plus a `BtiTrieCorrupt` finding per partition from `verify`).
#[test]
fn rows_db_root_ordinal_stays_in_readers_accepted_set() {
    use crate::storage::sstable::bti::{
        resolve_rows_db_entry, rows_root_rejected_root_ordinals_for_test,
    };

    let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
    let rejected = rows_root_rejected_root_ordinals_for_test();

    // Both root fan-outs: a SINGLE-child root (every `int` clustering separator
    // shares the leading 0x80 byte, so this is the ordinary shape — and the exact one
    // a size optimisation would compress into SINGLE_NOPAYLOAD_*), and a multi-child
    // root.
    let shapes: [(&str, Vec<Vec<u8>>); 2] = [
        ("single-child root", vec![sep(8), sep(16), sep(24)]),
        (
            "multi-child root",
            vec![vec![0x10, 0x01], vec![0x20, 0x02], vec![0x30, 0x03]],
        ),
    ];
    for (label, separators) in shapes {
        let blocks: Vec<RowIndexBlock> = separators
            .iter()
            .enumerate()
            .map(|(i, s)| RowIndexBlock {
                separator_key: s.clone(),
                block_offset: (i as u64 + 1) * 64,
                open_marker: None,
            })
            .collect();
        let mut w = RowsTrieWriter::new();
        w.add_partition_row_index(&1i32.to_be_bytes(), 0, blocks, None);
        let (rows_db, offsets) = w.finish().expect("finish");
        let header =
            resolve_rows_db_entry(&rows_db, offsets[0] as usize).expect("entry must resolve");
        let root = header
            .trie_root
            .unwrap_or_else(|e| panic!("{label}: the written root must validate: {e}"))
            .offset();
        let ordinal = rows_db[root] >> 4;
        assert!(
            !rejected.contains(&ordinal),
            "{label}: the emitted root's node ordinal {ordinal} is in the READER's rejected \
             set {rejected:?} — `validate_rows_trie_root` would refuse every root this \
             writer emits. Either keep routing internal nodes through `write_sparse` or \
             relax the reader IN LOCKSTEP (issue #3002)."
        );
    }
}

/// A partition leaf with a positive RowsOffset payload decodes back to the
/// SAME RowsOffset via the reader (`BtiPartitionLocation::RowsOffset`).
#[test]
fn partition_leaf_rows_offset_roundtrips() {
    let raw_key = vec![0x11u8; 16];
    let mut w = PartitionsTrieWriter::new();
    w.add_partition_with_payload(&raw_key, PartitionPayload::RowsOffset(242));
    let bytes = w.finish().expect("finish");

    let mut cur = Cursor::new(bytes);
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &raw_key)
        .expect("lookup")
        .expect("found");
    match loc {
        BtiPartitionLocation::RowsOffset(o) => assert_eq!(o, 242),
        BtiPartitionLocation::DataOffset(o) => {
            panic!("expected RowsOffset(242), got DataOffset({o})")
        }
    }
}

/// An EMPTY partition key is written at the ring MINIMUM, so the reader — which
/// derives its probe token from `cassandra_murmur3_token` — finds it (issue #3633).
///
/// This is a genuine WRITER/READER DIFFERENTIAL, not a self-consistent
/// round-trip: the two sides reach the minimum token by different code paths
/// (the writer's inlined `is_empty()` guard vs. the reader's helper call), and
/// the VALUE is pinned to the Cassandra oracle in
/// `cassandra_murmur3::tests::empty_partition_key_token_is_cassandra_minimum`.
/// Before the writer carried the guard it stored this leaf at
/// `normalize(h1) == 0` while the reader probed at `i64::MIN`, so the lookup
/// returned `None` — this test fails without the fix.
#[test]
fn empty_partition_key_is_written_at_cassandra_minimum_token() {
    let mut w = PartitionsTrieWriter::new();
    w.add_partition_with_payload(b"", PartitionPayload::DataOffset(7));
    // A non-empty sibling, so this is not a degenerate single-leaf trie.
    w.add_partition_with_payload(b"a", PartitionPayload::DataOffset(9));
    let bytes = w.finish().expect("finish");

    let mut cur = Cursor::new(bytes);
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, b"")
        .expect("lookup must not error")
        .expect("the empty key must be found at the MINIMUM token");
    match loc {
        BtiPartitionLocation::DataOffset(o) => assert_eq!(o, 7),
        BtiPartitionLocation::RowsOffset(o) => {
            panic!("expected DataOffset(7), got RowsOffset({o})")
        }
    }

    // The non-empty sibling is unaffected by the guard.
    let mut cur = Cursor::new(w2_bytes());
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, b"a")
        .expect("lookup must not error")
        .expect("the non-empty key must still be found");
    match loc {
        BtiPartitionLocation::DataOffset(o) => assert_eq!(o, 9),
        BtiPartitionLocation::RowsOffset(o) => {
            panic!("expected DataOffset(9), got RowsOffset({o})")
        }
    }
}

/// Rebuild the same two-leaf trie as above (a `finish()` consumes the writer).
fn w2_bytes() -> Vec<u8> {
    let mut w = PartitionsTrieWriter::new();
    w.add_partition_with_payload(b"", PartitionPayload::DataOffset(7));
    w.add_partition_with_payload(b"a", PartitionPayload::DataOffset(9));
    w.finish().expect("finish")
}
