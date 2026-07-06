# Design — bti-zero-copy-slice-walk (C3)

## Context

BTI point lookup resolves a partition key to a `Data.db` offset by descending the `Partitions.db`
Patricia trie. The trie bytes are loaded once at reader open into `bti_partitions_db: Arc<Vec<u8>>`
(`reader/types.rs:339`). Three wastes (see proposal) sit on the per-lookup hot path. This change
removes all three without touching the on-disk `da` format or the resolved offsets.

## Decision 1 — Zero-copy slice lookup entry points

`lookup_partition_in_bti_file<R: Read + Seek>` seeks to `End(-8)`, reads the root offset, then
`read_exact`s the whole trie into a fresh `Vec<u8>`. When the buffer is ALREADY resident (the
production path wraps `Arc<Vec<u8>>` in a `Cursor`), that copy is pure waste.

Add, in a new `bti/parser/slice_walk.rs` module (keeps the over-threshold `partitions.rs` from
growing — campsite/epic #1116):

- `lookup_partition_in_bti_slice(file_bytes: &[u8], encoded_key: &[u8]) -> BtiResult<Option<BtiPartitionLocation>>`
  — validates `len >= 8`, reads the big-endian root offset from the last 8 bytes, and walks
  `&file_bytes[..len-8]` in place via the existing `walk_bti_trie`. No allocation proportional to
  trie size.
- `lookup_raw_key_in_bti_partitions_slice(file_bytes: &[u8], raw_key: &[u8]) -> ...` — encodes the key
  (`encode_partition_key_for_bti_trie`) and delegates.

The `Read + Seek` entry (`lookup_partition_in_bti_file` / `lookup_raw_key_in_bti_partitions_db`) is
kept unchanged for stream/test callers. Production callers
(`lookup_partition_via_bti_trie`, `bti_clustering_row_window`) switch to the slice API on
`partitions_db.as_slice()`.

**Byte-parity:** the footer parse and the trie walk are identical to the stream path; a shared test
asserts the slice API and the stream API return identical `BtiPartitionLocation` for the synthetic
trie and the real `test_da` fixture (offsets 0/63/125), and for a miss.

## Decision 2 — Zero-alloc child descent (`find_child_offset`)

`find_next_child_offset` calls `parse_bti_node_for_traversal` → `parse_bti_node`, which allocates the
node's full child table just to read one pointer. Replace its body with a new zero-alloc
`slice_walk::find_child_offset(trie_data, node_offset, search_byte) -> BtiResult<Option<usize>>` that,
per the node's high-nibble ordinal, decodes ONLY the pointer for `search_byte` directly from the byte
slice:

- **PayloadOnly (0):** no children → `None`.
- **Single (1/2/3/4):** compare the single transition byte; on match, child = `node_offset - delta`
  where delta is the 4-bit low nibble (1), 1 byte (2), 12-bit (3), or 2 bytes (4), via
  `saturating_sub` (matches `parse_single_node`).
- **Sparse (5/6/7/8/9):** read `count`, scan the `count` transition bytes for `search_byte`; on match,
  read that index's delta (1/12-bit/2/3/5 bytes) and return `node_offset - delta`.
- **Dense (10/11/12/13/14/15):** if `start_byte <= search_byte < start_byte + range_len`, read the
  slot's delta (12-bit / 2 / 3 / 4 / 5 / 8 bytes); a delta of `0` is the "no transition" sentinel
  → `None`, otherwise child = `node_offset - delta`.

The child pointer arithmetic (`saturating_sub`, the Dense `delta == 0` sentinel, the `read_be_unsigned`
and `read_12bit_packed` readers) is REUSED from `node_decode` so the resolved child offsets are
bit-identical to `parse_bti_node` + `find_child`. Structural errors (node offset out of bounds, node
truncated before its pointer area) return `Err` exactly as `parse_bti_node` does — no silent `None`.
`find_next_child_offset` becomes a thin delegate, so both the slice walk and the retained stream walk
get the zero-alloc descent. A crafted-node agreement test asserts `find_child_offset` equals
`parse_bti_node(...).find_child(...)` for every ordinal, including the Dense offset-0 / gap cases.

## Decision 3 — Single walk per point read

The prune (`might_contain_partition`) and the seek (`scan_single_partition_clustering`) both call
`lookup_partition_via_bti_trie(key)`, descending the same trie twice. Reuse the prune's resolution:
`lookup_partition_via_bti_trie` memoizes its most-recent `(partition_key, resolved Option<u64>)` in a
reader-local `std::sync::Mutex<Option<(Box<[u8]>, Option<u64>)>>` slot (same pattern as the existing
`verified_uncompressed_chunks` mutex on the reader; the lock is held only for a cheap compare/clone,
NEVER across I/O or an `await`). On a hit for the identical key the resolved offset is returned WITHOUT
re-descending the trie and WITHOUT bumping `TRIE_WALKS`; the presence-oracle ordering and the
`READ_BLOOM_CHECKS` / `READ_PARTITION_LOOKUP` observability semantics are preserved (a memo hit still
records a presence decision — it IS one, just cached).

**Why a memo, not a threaded parameter:** threading the location through `might_contain_partition` →
`scan_partition_clustering` → `scan_single_partition_clustering` would net-grow three files already
over the campsite size threshold (`mod.rs`, `data_access/bti.rs`) and add a hot-path signature change;
the memo delivers the identical observable outcome (`TRIE_WALKS == 1`) confined to the under-threshold
`partition_lookup.rs` + a single reader field in `types.rs`. The memoized value is a pure function of
the immutable trie and the key, so a stale slot (different key, or a concurrent read) merely misses and
re-walks — never a wrong result. This is NOT the B4 cross-lookup key cache.

## Correctness / no-heuristics guardrails

- Resolved offsets are byte-identical to the current implementation (pinned `test_da` 0/63/125 + full
  `da` corpus parity).
- Node decode stays per `TrieNode.java`; ambiguous/truncated decode is `Err`, not a guess.
- The BTI trie remains the authoritative presence oracle (bloom deliberately skipped for BTI).

## Measurement (A4/A5)

- `TRIE_WALKS` (A5, `work-counters` feature) == 1 for a single-candidate BTI point read (was 2).
- Alloc counter (A4 dhat lane): a BTI point lookup allocates no buffer proportional to trie size
  (the whole-`Partitions.db` copy and the per-node child tables are gone).
