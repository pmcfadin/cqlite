## Context

Finding G3 of the read-path audit: partition-location logic is duplicated across four components with
their own entry points, so B4 (key cache) and C5 (range short-circuit) were bolted onto each path
separately and the BIG presence helpers drifted. The risk profile of this consolidation is dominated by
(a) silently changing an offset/negative/error on any path, and (b) flattening the deliberate per-format
bloom asymmetry. The design therefore centres on a thin format-tagged façade that RE-USES the existing,
verified resolution methods rather than rewriting them, plus a parity test written against the legacy
paths BEFORE migration.

## The PartitionLocator façade

New inherent method on `SSTableReader` (in `reader/partition_locator.rs`):

    async fn locate(&self, key: &[u8]) -> Result<Option<(u64, u32)>>

Resolution order, written ONCE:

1. **C5 range short-circuit** — `partition_key_out_of_range(key)` (`range_short_circuit.rs`). Out of the
   authoritative `[first_key, last_key]` Summary bound (Cassandra Murmur3 token order, inclusive) ⇒
   record one short-circuit and return `Ok(None)`. No-op when no Summary bound (BTI). Written once in
   the façade instead of at each caller.
2. **Format dispatch** on `bti_partitions_db.is_some()`:
   - **BTI (`da`)** — `lookup_partition_via_bti_trie(key)` (trie walk; B4 cache + C3 memo + C4 encode
     already inside). Returns `Some((offset, 0))` (Rows.db/Data.db offset; BTI records no partition
     size) or `None` (authoritative absence). Bloom filter is NOT consulted — the trie is the
     authoritative oracle (#831/#909).
   - **BIG (`nb`/uncompressed)** — `lookup_partition_with_index(key)` (raw-key `Index.db` map; B4 cache
     already inside). Returns `Some((offset, size))` (writer emits `size=0`) or `None`.

`locate_encoded(key, &[u8;9])` is the candidate-prune variant: BTI uses
`lookup_partition_via_bti_trie_encoded` (C4 one-hash-per-read hoist); BIG ignores the encoding (bloom is
raw-keyed) and calls `lookup_partition_with_index`.

The façade returns ONLY the partition offset+size. It deliberately does NOT decide "present" — the
caller keeps its per-format post-`None` handling (see below), because `None` means different things per
format.

## Per-format ordering preserved bit-for-bit (the load-bearing invariant)

- **BIG bloom-first** stays in `big_get_with_resolution` (`big_point.rs`): bloom pre-check FIRST
  (definite miss ⇒ `None` before any Index.db probe), then `locate` for the offset, then the
  chunk-targeted decode, then — critically — the `Index.db`-miss ⇒ `scan_for_key` fallback (#1572
  truncated-index correctness). `locate` replaces only the inline `lookup_partition_with_index` call;
  the bloom pre-check and the miss-fallback branch are untouched.
- **BTI bloom-skip** stays in `bti_point_lookup` (`bti.rs`): no bloom pre-check; `locate` (trie) decides
  presence; a trie miss is authoritative absent. `READ_BLOOM_CHECKS` is emitted exactly once inside the
  trie descent (guardrail at `partition_lookup.rs:268-289`) — moving code must not double-count.

Because `None` from `locate` is "index/trie did not resolve", and BIG treats that as inconclusive
(fall back to scan) while BTI treats it as authoritative absent, the branch logic MUST stay at the
caller. The façade is a resolver, not a presence oracle.

## Where B4 and C5 live

- **C5** — hoisted into `locate` (step 1), replacing the current call site at `get_with_resolution:210`.
  One implementation, both formats.
- **B4** — already inside `lookup_partition_with_index` and `lookup_partition_via_bti_trie{,_encoded}`,
  which the façade calls; no relocation needed, and both formats reach it only through the façade after
  migration.

## Entry points deleted

`get_with_spec_readers`, `get_with_schema_context`, `lookup_partition_with_schema_context` (and, if it
becomes orphaned, `compute_partition_key_digest_with_schema`) — proven to have zero production callers
(only in-crate + `tests/` references). The tests that call them are re-pointed at `locate` / `get`, or
deleted where they only exercised the dead helper.

## index_reader / bti split plan (campsite #1116)

- `index_reader.rs` (1019 LOC) → keep the `IndexReader` struct + public API in `index_reader/mod.rs`;
  move the nom parse tree (`parse_index_data_with_summary`, `parse_all_partition_keys_with_summary`, the
  header/entry parsers around `:140-186` and `:320-358`) into `index_reader/parse.rs`. This is also the
  future bounded-mode seam (see below).
- `reader/data_access/bti.rs` (1443 LOC) → move the point-lookup decoders
  (`bti_point_lookup`, `bti_decompress_and_parse_target`, chunk helpers) into
  `data_access/bti_point.rs`, leaving the seek/clustering paths in `bti.rs`. Both touched files end
  smaller than they started.

## Future bounded-mode seam (NOT built)

The whole-`Index.db` materialization at `index_reader.rs:140-186` (`read_to_end`) and `:337-340` builds
the full `key_lookup` map at open. Because `locate` is now the single BIG resolution chokepoint, a
future Summary-bounded on-disk resolver — binary-search the `Index.db` within the Summary-designated
window via `ReadAt` instead of materializing — can be added as an alternate BIG strategy selected at
open by a partition-count/size threshold, behind `locate`, with zero caller changes. This change only
KEEPS that seam open; it does not implement bounded mode (deferred to an Epic G follow-up per owner
decision).

## Test strategy

The parity test is written FIRST, against the legacy paths, before any migration: for every partition
key (present + absent + boundary) in the BIG and BTI fixtures, capture the legacy
`lookup_partition_with_index` / `lookup_partition_via_bti_trie` result, then assert `locate` returns the
identical `(offset, size)` / `None`. Counter-delta assertions pin bloom ordering (BIG bloom-first zero
Index probes on a definite miss; BTI single bloom-check emission) and B4/C5 (cache hit ⇒ zero re-probe;
out-of-range ⇒ one short-circuit, zero downstream work).
