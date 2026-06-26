# Collection serializer codec vectors (`collections.json`)

Issue #1005 (Epic #971 — CQL Type & Schema Evolution Parity).

Cassandra-derived byte-vectors for set / map / list collection serialization and
unsigned-VInt element-count boundary decoding. Consumed by
`cqlite-core/tests/issue_1005_collection_serializer_vectors.rs`.

> Do **not** add `primitives.json` here — that file is owned by sibling issue #1004,
> which shares this directory.

## Provenance

Encodings are grounded in Apache Cassandra 5.0.2 serializer source (fetched from
`github.com/apache/cassandra` tag `cassandra-5.0.2`; equivalent to the
`cassandra:5.0.2` Docker image). The `_provenance` block in `collections.json`
lists the exact Java files and quotes the framing contracts. Expected indexes,
byte ranges, and VInt widths are derived from the **Java source**, never from
CQLite's own output.

Authoritative files:

- `org/apache/cassandra/serializers/CollectionSerializer.java`
- `org/apache/cassandra/serializers/AbstractMapSerializer.java`
- `org/apache/cassandra/serializers/SetSerializer.java`
- `org/apache/cassandra/serializers/MapSerializer.java`
- `org/apache/cassandra/serializers/ListSerializer.java`
- `org/apache/cassandra/utils/VIntCoding.java`

## Two framings (NOT interchangeable)

1. **Frozen / CQL-protocol (`ByteBufferAccessor`)** — `CollectionSerializer`:
   fixed **4-byte big-endian signed int** collection size and per-element length
   prefix (`-1` == null). The `serialized_bytes` arrays in `collections.json` use
   this form. The offset/lookup/range tests run Cassandra's `ByteBuffer` offset
   arithmetic and `compareForCQL` short-circuiting over these bytes and assert
   concrete byte ranges and element indexes.

2. **Multi-cell on-disk SSTable** — Cassandra **signed VInts** (`VIntCoding`,
   zigzag; `encode_vint`/`parse_vint`) for the element count and each element
   length prefix; the element COUNT field decodes via the unsigned VInt
   (`readUnsignedVInt` / `parse_vuint`). This is the form CQLite's no-heuristics
   schema reader (`parse_list_with_schema` / `parse_map_with_schema`) actually
   decodes. For `text` elements the length-prefixed bytes are themselves a
   `parse_text` buffer (`[VInt str_len][utf8]`); `int` elements are 4 raw BE bytes.
   The reader-decode tests rebuild the same logical members in this framing and
   assert the decoded ordered result.

`single_cell_multicell_equivalence` proves both framings yield identical ordered
members.

## What is asserted (order-sensitive)

- `set_lookup_offsets` — set decodes in Cassandra SORT order; per-element byte
  ranges; `getIndexFromSerialized` returns the element INDEX (or -1); an ABSENT
  element (`charlie`, sorts between `bravo`/`delta`) returns -1 and does **not**
  shift the byte offsets/indexes of following present elements (proved with
  asserted ranges before/after).
- `set_range_offsets` — `getIndexesRangeFromSerialized` half-open `[start,end)`;
  UNSET bounds (`from` → 0, `to` → n); `from` past last element → empty `(0,0)`.
- `map_key_lookup_offsets` — map keys/values key-sorted; entry INDEX (not byte
  offset); key byte ranges; absent-key short-circuit. The walk skips each entry's
  value buffer so the comparator only sees KEY buffers (`skipMapValue`).
- `map_key_range_offsets` — half-open key ranges incl. single-key `[1,2)`, UNSET
  bounds, invalid/empty range.
- `vint_element_count_boundaries` — unsigned-VInt element counts at
  `0,1,127,128,16383,16384,2097151,2097152`; asserts the decoded count AND the
  exact authoritative encoded bytes AND the size-class byte width (via byte-0
  leading-ones) at each boundary.
- `single_cell_multicell_equivalence` — frozen single-cell vs multi-cell sets
  decode to the same ordered members.
- `list_insertion_order` — lists preserve INSERTION order (not sorted).

## Regenerating

The vectors are static and human-auditable (hand-encoded from the Cassandra
contracts). To re-confirm an encoding, decode the `serialized_bytes`/`encoded_bytes`
with the cited Java serializer (or `nodetool`/`sstabledump` on a fixture), or
re-derive the VInt widths from `VIntCoding`. Keep the inline constants in the test
in lock-step with this file — `committed_json_vectors_match_inline_constants`
guards that the JSON contains the same serialized byte arrays.
