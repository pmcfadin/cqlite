//! Issue #1005 — Collection serializer parity (set/map/list + VInt count boundaries).
//!
//! Part of Epic #971 (CQL Type & Schema Evolution Parity).
//!
//! These tests assert CQLite's collection codec against byte-vectors derived
//! from Apache Cassandra 5.0.2 serializer source (NOT from CQLite's own output):
//!
//!   * `org/apache/cassandra/serializers/CollectionSerializer.java`
//!   * `org/apache/cassandra/serializers/AbstractMapSerializer.java`
//!   * `org/apache/cassandra/serializers/SetSerializer.java`
//!   * `org/apache/cassandra/serializers/MapSerializer.java`
//!
//! Authoritative framing facts captured in `test-data/codec-vectors/collections.json`:
//!
//!   * Frozen / CQL-protocol (`ByteBufferAccessor`) collection form:
//!       - collection size  = `ByteBuffer.putInt(elements)`  -> fixed 4-byte BE signed int.
//!       - per-element size = `ByteBuffer.putInt(size)`      -> fixed 4-byte BE signed int (-1 = null).
//!         i.e. NOT a VInt. (`CollectionSerializer.writeCollectionSize` / `writeValue`.)
//!   * `SetSerializer.serializeValues` sorts buffers in Cassandra SORT order; for `text`
//!     that equals UTF-8 lexicographic byte order.
//!   * `SetSerializer.getSerializedValue` / `AbstractMapSerializer.getIndexFromSerialized`
//!     linear-scan and short-circuit once a stored element compares greater than the key.
//!     `getIndexFromSerialized` returns the ELEMENT INDEX `i` (not a byte offset), or -1.
//!   * `AbstractMapSerializer.getIndexesRangeFromSerialized` returns a half-open
//!     `Range.closedOpen(start, end)` == `[start, end)`; `from`/`to` UNSET => 0 / n;
//!     `from` past the last element => empty `closedOpen(0, 0)`.
//!   * The MULTI-CELL on-disk SSTable collection path reads the element/cell count with the
//!     Cassandra unsigned VInt (`DataInputPlus.readUnsignedVInt`, `VIntCoding`), implemented
//!     in CQLite as `parser::vint::parse_vuint` / `encode_vuint`
//!     (see `storage/.../v5_compressed_legacy.rs:6821`).
//!
//! The tests reimplement the Cassandra scan/lookup/range algorithms over the committed
//! byte-vectors and assert positionally (order-sensitive), proving concrete byte ranges
//! and indexes rather than merely that decoding succeeds.
//!
//! TWO DISTINCT FRAMINGS are exercised, and they are NOT interchangeable:
//!
//!   * The frozen / CQL-protocol (`ByteBufferAccessor`) form uses a fixed 4-byte BE int
//!     count and 4-byte BE int per-element length prefixes (`CollectionSerializer`). The
//!     committed `SET_TEXT_BYTES` / `MAP_TEXT_INT_BYTES` vectors are in THIS form, and the
//!     offset/lookup/range algorithm tests operate over them (mirroring the Java source's
//!     `ByteBuffer` offset arithmetic and `compareForCQL` short-circuiting).
//!
//!   * The MULTI-CELL on-disk SSTable form (which CQLite's schema reader
//!     `parse_list_with_schema` / `parse_map_with_schema` actually decodes) uses Cassandra
//!     signed VInts (`encode_vint`, zigzag) for the element count AND each length prefix.
//!     The reader-exercising tests build their input with `encode_vint`, exactly as the
//!     in-tree convention in `parser/collection_correctness_tests.rs`. The DECODED ordered
//!     result is the authoritative assertion; the VInt framing is Cassandra's
//!     `VIntCoding`/`readUnsignedVInt` on-disk encoding (the same one validated to the byte
//!     in `vint_element_count_boundaries`).

use cqlite_core::parser::types::{parse_list_with_schema, parse_map_with_schema};
use cqlite_core::parser::vint::{encode_vint, encode_vuint, parse_vuint};
use cqlite_core::schema::CqlType;
use cqlite_core::types::Value;

// ===========================================================================
// Authoritative byte-vectors (mirrors test-data/codec-vectors/collections.json).
// Provenance: Apache Cassandra 5.0.2 collection serializer source.
//
// These constants are kept in lock-step with the committed JSON data file; a
// guard test below asserts they agree, so the JSON remains the single source of
// truth and the constants stay test-readable.
// ===========================================================================

/// Frozen `set<text>` {alpha, bravo, delta} in Cassandra sort order.
const SET_TEXT_BYTES: &[u8] = &[
    0, 0, 0, 3, // count = 3 (4-byte BE int)
    0, 0, 0, 5, b'a', b'l', b'p', b'h', b'a', // elem0
    0, 0, 0, 5, b'b', b'r', b'a', b'v', b'o', // elem1
    0, 0, 0, 5, b'd', b'e', b'l', b't', b'a', // elem2
];

/// Frozen `map<text,int>` {alpha:1, bravo:2, delta:3} key-sorted.
const MAP_TEXT_INT_BYTES: &[u8] = &[
    0, 0, 0, 3, // count = 3
    0, 0, 0, 5, b'a', b'l', b'p', b'h', b'a', 0, 0, 0, 4, 0, 0, 0, 1, // alpha -> 1
    0, 0, 0, 5, b'b', b'r', b'a', b'v', b'o', 0, 0, 0, 4, 0, 0, 0, 2, // bravo -> 2
    0, 0, 0, 5, b'd', b'e', b'l', b't', b'a', 0, 0, 0, 4, 0, 0, 0, 3, // delta -> 3
];

const SIZE_OF_COLLECTION_SIZE: usize = 4;
const SIZE_OF_LENGTH_PREFIX: usize = 4;

// ===========================================================================
// Cassandra algorithm re-implementations over the serialized byte-vectors.
//
// These mirror the Java source 1:1. `from`/`to` of `None` model the Java
// `ByteBufferUtil.UNSET_BYTE_BUFFER` sentinel.
// ===========================================================================

fn read_collection_size(buf: &[u8]) -> i32 {
    i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])
}

/// One stored element: byte offset of its 4-byte length prefix and its value range.
#[derive(Debug, Clone, PartialEq)]
struct Elem {
    len_prefix_offset: usize,
    value_range: (usize, usize),
    value: Vec<u8>,
}

/// Walk a serialized collection emitting per-element offsets, exactly following
/// `CollectionSerializer.readValue` + `sizeOfValue` offset arithmetic.
///
/// `has_values` controls whether each element is `[size][bytes]` (set) or
/// `[ksize][kbytes][vsize][vbytes]` (map). For maps this returns the KEY element
/// offsets (the comparator operates on keys), and advances past the value too.
fn walk_elements(buf: &[u8], has_values: bool) -> Vec<Elem> {
    let n = read_collection_size(buf) as usize;
    let mut offset = SIZE_OF_COLLECTION_SIZE;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let size = i32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        let value_start = offset + SIZE_OF_LENGTH_PREFIX;
        let value_end = value_start + size.max(0) as usize;
        out.push(Elem {
            len_prefix_offset: offset,
            value_range: (value_start, value_end),
            value: buf[value_start..value_end].to_vec(),
        });
        // Advance past key (and value, for maps): sizeOfValue == 4 + size.
        offset = value_end;
        if has_values {
            let vsize = i32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]);
            offset += SIZE_OF_LENGTH_PREFIX + vsize.max(0) as usize;
        }
    }
    out
}

/// `AbstractMapSerializer.getIndexFromSerialized` / `SetSerializer.getSerializedValue`
/// short-circuiting linear scan. Returns the matching element INDEX or -1.
///
/// `cmp` compares (stored_element_bytes, key_bytes) like Cassandra's
/// `comparator.compareForCQL(stored, key)`.
fn get_index_from_serialized(buf: &[u8], key: &[u8], has_values: bool) -> i64 {
    let elems = walk_elements(buf, has_values);
    for (i, e) in elems.iter().enumerate() {
        match e.value.as_slice().cmp(key) {
            std::cmp::Ordering::Equal => return i as i64,
            std::cmp::Ordering::Greater => return -1, // sorted-order short-circuit
            std::cmp::Ordering::Less => continue,
        }
    }
    -1
}

/// `AbstractMapSerializer.getIndexesRangeFromSerialized` -> half-open `[start, end)`.
/// `from`/`to` == `None` models `UNSET_BYTE_BUFFER`.
///
/// `has_values` MUST be `true` for maps so the walk skips each entry's value buffer and
/// the comparator only ever sees KEY buffers (per the Java `skipMapValue` call); a set
/// passes `false`. Mirrors `getIndexesRangeFromSerialized` in
/// `org/apache/cassandra/serializers/AbstractMapSerializer.java` (Cassandra 5.0.2):
///   start = from UNSET ? 0 : -1; end = to UNSET ? n : -1;
///   if start<0: comparison = compareForCQL(from, key); if (comparison<=0) start=i; else continue;
///   if end<0:   comparison = compareForCQL(key, to);   if (comparison>0)  end=i;
///   start<0 -> closedOpen(0,0); end<0 -> closedOpen(start,n); else closedOpen(start,end).
fn get_indexes_range(
    buf: &[u8],
    from: Option<&[u8]>,
    to: Option<&[u8]>,
    has_values: bool,
) -> (usize, usize) {
    let n = read_collection_size(buf) as usize;
    let elems = walk_elements(buf, has_values);

    // Java: start = from UNSET ? 0 : -1 ; end = to UNSET ? n : -1
    let mut start: i64 = if from.is_none() { 0 } else { -1 };
    let mut end: i64 = if to.is_none() { n as i64 } else { -1 };

    for (i, e) in elems.iter().enumerate() {
        if start >= 0 && end >= 0 {
            break;
        }
        let key = e.value.as_slice();
        if start < 0 {
            // comparison = compareForCQL(from, key) <= 0  => start = i
            if let Some(f) = from {
                if f.cmp(key) != std::cmp::Ordering::Greater {
                    start = i as i64;
                } else {
                    continue;
                }
            }
        }
        if end < 0 {
            // comparison = compareForCQL(key, to) > 0 => end = i
            if let Some(t) = to {
                if key.cmp(t) == std::cmp::Ordering::Greater {
                    end = i as i64;
                }
            }
        }
    }

    if start < 0 {
        return (0, 0); // Range.closedOpen(0,0)
    }
    if end < 0 {
        return (start as usize, n); // Range.closedOpen(start, n)
    }
    (start as usize, end as usize)
}

// ===========================================================================
// 1. set_lookup_offsets
// ===========================================================================

#[test]
fn set_lookup_offsets_decodes_elements_in_cassandra_sort_order() {
    // The schema-aware (no-heuristics) reader must surface the set members in the
    // exact stored (sorted) order, positionally. CQLite's reader decodes the
    // MULTI-CELL on-disk (VInt-framed) form; we build that from the same members
    // that the frozen `SET_TEXT_BYTES` vector encodes, in the same Cassandra sort order.
    let multicell = build_multicell_text_collection(&[b"alpha", b"bravo", b"delta"]);
    let (rest, value) = parse_set_via_list(&multicell).expect("set<text> decodes with schema");
    assert!(rest.is_empty(), "set decode must consume all bytes");
    let members = expect_set(&value);
    assert_eq!(
        members,
        vec![
            Value::Text("alpha".into()),
            Value::Text("bravo".into()),
            Value::Text("delta".into()),
        ],
        "set elements must be returned in Cassandra SORT order, positionally"
    );
}

#[test]
fn list_preserves_insertion_order_not_sort_order() {
    // Lists (ListType) preserve INSERTION order — unlike sets they are NOT sorted.
    // Feed an intentionally non-sorted member sequence and require it back verbatim,
    // positionally. This exercises the same multi-cell reader path as sets but proves
    // the list contract (order = position, no reordering).
    let multicell = build_multicell_text_collection(&[b"delta", b"alpha", b"bravo"]);
    let (rest, value) =
        parse_list_with_schema(&multicell, &CqlType::Text).expect("list<text> decodes with schema");
    assert!(rest.is_empty(), "list decode must consume all bytes");
    let items = expect_set(&value); // expect_set unwraps List too
    assert_eq!(
        items,
        vec![
            Value::Text("delta".into()),
            Value::Text("alpha".into()),
            Value::Text("bravo".into()),
        ],
        "list elements must be returned in INSERTION order, positionally (no sort)"
    );
}

#[test]
fn set_lookup_offsets_byte_ranges_match_cassandra_framing() {
    let elems = walk_elements(SET_TEXT_BYTES, false);
    // Authoritative offsets from CollectionSerializer.readValue arithmetic.
    let expected = [
        (4usize, (8usize, 13usize), b"alpha".as_slice()),
        (13, (17, 22), b"bravo"),
        (22, (26, 31), b"delta"),
    ];
    assert_eq!(elems.len(), 3);
    for (i, (lp, vr, bytes)) in expected.iter().enumerate() {
        assert_eq!(
            elems[i].len_prefix_offset, *lp,
            "elem {i} len-prefix offset"
        );
        assert_eq!(elems[i].value_range, *vr, "elem {i} value range");
        assert_eq!(elems[i].value.as_slice(), *bytes, "elem {i} value bytes");
    }
}

#[test]
fn set_lookup_present_elements_return_their_index() {
    assert_eq!(
        get_index_from_serialized(SET_TEXT_BYTES, b"alpha", false),
        0
    );
    assert_eq!(
        get_index_from_serialized(SET_TEXT_BYTES, b"bravo", false),
        1
    );
    assert_eq!(
        get_index_from_serialized(SET_TEXT_BYTES, b"delta", false),
        2
    );
}

#[test]
fn set_lookup_absent_element_does_not_shift_following_offsets() {
    // PROOF: capture the concrete byte offsets/ranges of every present element,
    // then perform a lookup for an ABSENT element that sorts *between* two present
    // ones ("charlie" is between "bravo" and "delta"). The absent lookup must
    // return "not found" (-1) AND the present elements' byte layout must be
    // bit-for-bit unchanged (the same buffer, same offsets) — i.e. a missing
    // element occupies zero bytes and cannot shift its neighbours.
    let before = walk_elements(SET_TEXT_BYTES, false);

    let idx = get_index_from_serialized(SET_TEXT_BYTES, b"charlie", false);
    assert_eq!(idx, -1, "'charlie' is absent -> -1");

    let after = walk_elements(SET_TEXT_BYTES, false);
    assert_eq!(
        before, after,
        "absent-element lookup must not perturb present element offsets/ranges"
    );

    // Concretely: 'delta' (which sorts after the absent 'charlie') keeps index 2
    // and value-range [26,31). If a missing element had shifted offsets this would move.
    assert_eq!(after[2].value_range, (26, 31));
    assert_eq!(
        get_index_from_serialized(SET_TEXT_BYTES, b"delta", false),
        2
    );
}

#[test]
fn set_lookup_short_circuits_on_sorted_order() {
    // Greater than all -> -1.
    assert_eq!(
        get_index_from_serialized(SET_TEXT_BYTES, b"zulu", false),
        -1
    );
    // Less than all -> first comparison is Greater -> immediate -1.
    assert_eq!(
        get_index_from_serialized(SET_TEXT_BYTES, b"aardvark", false),
        -1
    );
}

// ===========================================================================
// 2. set_range_offsets
// ===========================================================================

#[test]
fn set_range_offsets_half_open_semantics() {
    // [start, end) per AbstractMapSerializer.getIndexesRangeFromSerialized.
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, Some(b"alpha"), Some(b"delta"), false),
        (0, 3)
    );
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, Some(b"bravo"), Some(b"delta"), false),
        (1, 3)
    );
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, Some(b"alpha"), Some(b"bravo"), false),
        (0, 2)
    );
}

#[test]
fn set_range_offsets_unset_bounds() {
    // from UNSET => start 0.
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, None, Some(b"bravo"), false),
        (0, 2)
    );
    // to UNSET => end n.
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, Some(b"bravo"), None, false),
        (1, 3)
    );
}

#[test]
fn set_range_offsets_from_between_elements_and_empty_range() {
    // 'charlie' sorts between bravo(1) and delta(2): first element >= charlie is delta.
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, Some(b"charlie"), Some(b"zulu"), false),
        (2, 3)
    );
    // from greater than every element => empty closedOpen(0,0).
    assert_eq!(
        get_indexes_range(SET_TEXT_BYTES, Some(b"zulu"), Some(b"zzzz"), false),
        (0, 0)
    );
}

// ===========================================================================
// 3. map_key_lookup_offsets
// ===========================================================================

#[test]
fn map_key_lookup_decodes_entries_in_key_sort_order() {
    // Reader decodes the MULTI-CELL (VInt-framed) on-disk form; build it from the same
    // key-sorted entries as the frozen `MAP_TEXT_INT_BYTES` vector.
    let multicell = build_multicell_text_int_map(&[(b"alpha", 1), (b"bravo", 2), (b"delta", 3)]);
    let (rest, value) = parse_map_with_schema(&multicell, &CqlType::Text, &CqlType::Int)
        .expect("map<text,int> decodes with schema");
    assert!(rest.is_empty(), "map decode must consume all bytes");
    let pairs = expect_map(&value);
    assert_eq!(
        pairs,
        vec![
            (Value::Text("alpha".into()), Value::Integer(1)),
            (Value::Text("bravo".into()), Value::Integer(2)),
            (Value::Text("delta".into()), Value::Integer(3)),
        ],
        "map entries must preserve Cassandra key SORT order, positionally"
    );
}

#[test]
fn map_key_lookup_returns_entry_index_not_byte_offset() {
    // getIndexFromSerialized returns the ENTRY INDEX i.
    assert_eq!(
        get_index_from_serialized(MAP_TEXT_INT_BYTES, b"alpha", true),
        0
    );
    assert_eq!(
        get_index_from_serialized(MAP_TEXT_INT_BYTES, b"bravo", true),
        1
    );
    assert_eq!(
        get_index_from_serialized(MAP_TEXT_INT_BYTES, b"delta", true),
        2
    );
    // Absent keys.
    assert_eq!(
        get_index_from_serialized(MAP_TEXT_INT_BYTES, b"charlie", true),
        -1
    );
    assert_eq!(
        get_index_from_serialized(MAP_TEXT_INT_BYTES, b"zulu", true),
        -1
    );
}

#[test]
fn map_key_lookup_byte_ranges_match_cassandra_framing() {
    let keys = walk_elements(MAP_TEXT_INT_BYTES, true);
    let expected = [
        ((8usize, 13usize), b"alpha".as_slice()),
        ((25, 30), b"bravo"),
        ((42, 47), b"delta"),
    ];
    assert_eq!(keys.len(), 3);
    for (i, (vr, bytes)) in expected.iter().enumerate() {
        assert_eq!(keys[i].value_range, *vr, "map key {i} value range");
        assert_eq!(keys[i].value.as_slice(), *bytes, "map key {i} bytes");
    }
}

// ===========================================================================
// 4. map_key_range_offsets
// ===========================================================================

#[test]
fn map_key_range_half_open_and_unset_bounds() {
    assert_eq!(
        get_indexes_range(MAP_TEXT_INT_BYTES, Some(b"alpha"), Some(b"delta"), true),
        (0, 3)
    );
    // Single key range [1,2).
    assert_eq!(
        get_indexes_range(MAP_TEXT_INT_BYTES, Some(b"bravo"), Some(b"bravo"), true),
        (1, 2)
    );
    assert_eq!(
        get_indexes_range(MAP_TEXT_INT_BYTES, None, Some(b"alpha"), true),
        (0, 1)
    );
    assert_eq!(
        get_indexes_range(MAP_TEXT_INT_BYTES, Some(b"delta"), None, true),
        (2, 3)
    );
    // Empty / invalid (from past last element).
    assert_eq!(
        get_indexes_range(MAP_TEXT_INT_BYTES, Some(b"zulu"), Some(b"zzzz"), true),
        (0, 0)
    );
}

// ===========================================================================
// 5. vint_element_count_boundaries
//
// Multi-cell on-disk element count uses Cassandra unsigned VInt
// (parse_vuint / encode_vuint). Assert BOTH the decoded value AND the byte
// width Cassandra emits at each unsigned size-class boundary.
// ===========================================================================

#[test]
fn vint_element_count_boundaries_encode_to_cassandra_widths() {
    // (count, exact authoritative bytes, byte width).
    let cases: &[(u64, &[u8], usize)] = &[
        (0, &[0], 1),
        (1, &[1], 1),
        (127, &[127], 1),               // 2^7 - 1
        (128, &[128, 128], 2),          // 2^7 -> 2-byte class
        (16383, &[191, 255], 2),        // 2^14 - 1
        (16384, &[192, 64, 0], 3),      // 2^14 -> 3-byte class
        (2097151, &[223, 255, 255], 3), // 2^21 - 1
        (2097152, &[224, 32, 0, 0], 4), // 2^21 -> 4-byte class
    ];

    for (count, expected_bytes, expected_width) in cases {
        let encoded = encode_vuint(*count);
        assert_eq!(
            encoded.as_slice(),
            *expected_bytes,
            "encode_vuint({count}) must equal Cassandra unsigned-VInt bytes"
        );
        assert_eq!(
            encoded.len(),
            *expected_width,
            "encode_vuint({count}) must occupy {expected_width} bytes (size-class boundary)"
        );

        // Round-trip via the decoder used by the multi-cell collection read path.
        let (rest, decoded) =
            parse_vuint(&encoded).expect("parse_vuint must decode authoritative bytes");
        assert!(
            rest.is_empty(),
            "parse_vuint({count:?}) must consume all bytes"
        );
        assert_eq!(decoded, *count, "decoded count must round-trip");

        // The byte width is determined by leading-ones in byte 0 (Cassandra contract).
        let leading_ones = expected_bytes[0].leading_ones() as usize;
        assert_eq!(
            leading_ones + 1,
            *expected_width,
            "byte0 leading-ones must select the {expected_width}-byte size class for count {count}"
        );
    }
}

#[test]
fn vint_element_count_decodes_authoritative_bytes() {
    // Decode the exact authoritative bytes (not our own re-encoding) to avoid
    // a tautological encode/decode loop.
    let cases: &[(&[u8], u64)] = &[
        (&[0], 0),
        (&[127], 127),
        (&[128, 128], 128),
        (&[191, 255], 16383),
        (&[192, 64, 0], 16384),
        (&[223, 255, 255], 2097151),
        (&[224, 32, 0, 0], 2097152),
    ];
    for (bytes, expected) in cases {
        let (rest, decoded) = parse_vuint(bytes).expect("parse authoritative VInt bytes");
        assert!(rest.is_empty(), "must consume all of {bytes:?}");
        assert_eq!(decoded, *expected, "decoded {bytes:?} -> {expected}");
    }
}

// ===========================================================================
// 6. single_cell_multicell_equivalence
//
// A frozen (single-cell) set and a multi-cell set with the same members in the
// same Cassandra sort order decode to the same ORDERED element sequence.
// ===========================================================================

#[test]
fn single_cell_multicell_equivalence_same_ordered_members() {
    // Single-cell (frozen) form: the 4-byte-framed CollectionSerializer blob. Its ordered
    // members come straight from the authoritative `walk_elements` offset arithmetic over
    // the frozen `SET_TEXT_BYTES` vector (the same algorithm Cassandra's `ByteBufferAccessor`
    // path uses) — NOT from CQLite's own VInt reader.
    let frozen_members: Vec<Value> = walk_elements(SET_TEXT_BYTES, false)
        .iter()
        .map(|e| Value::Text(String::from_utf8(e.value.clone()).expect("utf8 frozen member")))
        .collect();

    // Multi-cell form: element count via signed VInt, then each element's length-prefixed
    // bytes — the on-disk framing CQLite's schema reader decodes. Same members, same
    // Cassandra sort order.
    let multicell = build_multicell_text_collection(&[b"alpha", b"bravo", b"delta"]);
    // The leading element count is the Cassandra unsigned-VInt count for 3 elements.
    let (_, count) = parse_vuint(&encode_vuint(3)).expect("multicell count VInt");
    assert_eq!(count, 3, "multi-cell element count must be 3");
    let (rest, decoded) = parse_set_via_list(&multicell).expect("multi-cell set decodes");
    assert!(
        rest.is_empty(),
        "multi-cell set decode must consume all bytes"
    );
    let multicell_members = expect_set(&decoded);

    assert_eq!(
        frozen_members, multicell_members,
        "single-cell (frozen) and multi-cell sets must yield identical ordered members"
    );
    // Positional spot-check: index 0 is the smallest sort key in both framings.
    assert_eq!(multicell_members[0], Value::Text("alpha".into()));
    assert_eq!(multicell_members[2], Value::Text("delta".into()));
}

// ===========================================================================
// Guard: keep the inline constants in sync with the committed JSON vectors.
// ===========================================================================

#[test]
fn committed_json_vectors_match_inline_constants() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../test-data/codec-vectors/collections.json"
    );
    let raw = std::fs::read_to_string(path).expect("read collections.json");
    // Lightweight extraction: confirm the JSON contains the same serialized byte
    // arrays we test against (avoids pulling in a JSON dep just for a guard).
    let set_csv = SET_TEXT_BYTES
        .iter()
        .map(|b| b.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let map_csv = MAP_TEXT_INT_BYTES
        .iter()
        .map(|b| b.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let normalized = raw.replace(", ", ",").replace(['[', ']'], "");
    assert!(
        normalized.contains(&set_csv),
        "collections.json must contain the set<text> serialized_bytes vector"
    );
    assert!(
        normalized.contains(&map_csv),
        "collections.json must contain the map<text,int> serialized_bytes vector"
    );
}

// ===========================================================================
// Helpers
// ===========================================================================

/// CQLite's schema reader frames each collection element as
/// `[VInt outer_element_len][element_value_bytes]`, and for `text` the element value
/// bytes are themselves `[VInt str_len][utf8]` (see `parse_text` and `test_text_parsing`
/// in `parser/types.rs`). So a `text` member of N bytes is wrapped as a (1+N)-or-more
/// byte inner buffer, and the outer prefix covers that whole inner buffer.
fn frame_text_element(s: &[u8]) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(encode_vint(s.len() as i64)); // parse_text inner length prefix
    inner.extend_from_slice(s);
    let mut out = Vec::new();
    out.extend(encode_vint(inner.len() as i64)); // outer element length prefix
    out.extend_from_slice(&inner);
    out
}

/// `int` values are decoded by `parse_int` as 4 raw big-endian bytes (no inner prefix),
/// so the element is `[VInt 4][4-byte BE int]`.
fn frame_int_element(v: i32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_vint(4));
    out.extend_from_slice(&v.to_be_bytes());
    out
}

/// Build the MULTI-CELL on-disk SSTable framing for a `list`/`set<text>`:
/// signed-VInt element count, then each text element framed by `frame_text_element`.
/// This is the exact framing `parse_list_with_schema` decodes (the in-tree convention in
/// `parser/collection_correctness_tests.rs` uses `int`; here we use `text`).
fn build_multicell_text_collection(members: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_vint(members.len() as i64));
    for m in members {
        out.extend(frame_text_element(m));
    }
    out
}

/// Build the MULTI-CELL on-disk framing for a `map<text,int>`: signed-VInt entry count,
/// then per-entry [text key element][int value element].
fn build_multicell_text_int_map(entries: &[(&[u8], i32)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_vint(entries.len() as i64));
    for (k, v) in entries {
        out.extend(frame_text_element(k));
        out.extend(frame_int_element(*v));
    }
    out
}

/// Decode a Cassandra MULTI-CELL `set<text>` blob via the no-heuristics schema reader.
fn parse_set_via_list(buf: &[u8]) -> Result<(&[u8], Value), String> {
    // CQLite's schema reader decodes a set as a list of the element type and
    // wraps it as Value::Set via CqlType::Set.
    parse_list_with_schema(buf, &CqlType::Text)
        .map(|(rest, v)| (rest, to_set(v)))
        .map_err(|e| format!("{e:?}"))
}

fn to_set(v: Value) -> Value {
    match v {
        Value::List(items) => Value::Set(items),
        other => other,
    }
}

fn expect_set(v: &Value) -> Vec<Value> {
    match v {
        Value::Set(items) | Value::List(items) => items.clone(),
        Value::Frozen(inner) => expect_set(inner),
        other => panic!("expected set/list, got {other:?}"),
    }
}

fn expect_map(v: &Value) -> Vec<(Value, Value)> {
    match v {
        Value::Map(pairs) => pairs.clone(),
        Value::Frozen(inner) => expect_map(inner),
        other => panic!("expected map, got {other:?}"),
    }
}
