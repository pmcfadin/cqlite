# Design — hoist-candidate-key-rehash (C4, issue #1575)

## Context

`WHERE pk = ?` against a BTI table prunes the reader snapshot to the candidate generations
that admit the key before decoding any Data.db bytes. The prune calls
`SSTableReader::might_contain_partition(raw_key)` per candidate. For BTI that routes to
`lookup_partition_via_bti_trie` → `encode_partition_key_for_bti_trie(raw_key)`, whose Murmur3
token + byte-comparable encoding is a pure function of the key. Recomputing it per candidate
wastes N-1 hashes on an N-generation fan-out.

C3 (#1574) already added a reader-local same-key memo so a SINGLE candidate's prune+seek
descends the trie once. But the memo is per-reader: N distinct generations each miss their
own memo and each re-encode the key. The encoding, unlike the trie walk, is
SSTable-independent, so it should be computed once per read and threaded to every candidate.

## Decisions

### Decision 1 — Pre-encoded lookup entry point, hoisted at the manager

Add `SSTableReader::might_contain_partition_encoded(raw_key, encoded: &[u8; 9])` and
`lookup_partition_via_bti_trie_encoded(raw_key, encoded)`, which accept a PRE-ENCODED
byte-comparable key and skip `encode_partition_key_for_bti_trie`. Both share the same private
`bti_trie_resolve(raw_key, encoded)` body as the raw-key entry point (which now encodes once
then delegates), so there is a single trie-descent implementation — the memo, the presence
counters, the `TRIE_WALKS` accounting, and the resolved offset are byte-identical to the
raw-key path.

`SSTableManager::prune_candidates(readers, raw_key)` computes the encoding EXACTLY ONCE
(guarded by `readers.iter().any(is_bti)` so a pure-BIG table pays nothing) and reuses it for
every candidate. The three prune sites call this helper. A BIG reader ignores the `encoded`
argument and runs its raw-key bloom check, so mixed/non-BTI sets are correct.

Alternative rejected: caching the encoding on the reader. That is a cross-lookup cache (Epic
B/B4) with lifetime/invalidation concerns; hoisting to one local variable per read is
minimal and self-evidently correct.

### Decision 2 — `KEY_HASH_CALLS` counter (A5 pattern, zero-overhead release)

Add `KEY_HASH_CALLS` to `read_work_counters` following the existing issue #1566 pattern:
unconditional `record_key_hash()` free function whose body is `#[cfg(any(test, feature =
"work-counters"))]` (a no-op, not even an atomic, in release), incremented at the single
Murmur3 site inside `encode_partition_key_for_bti_trie`. This makes the hoist provable — a
multi-generation fan-out records 1, and the retained per-candidate path records N — the
no-heuristics way (observe the work, not just the result). No production call site is
`#[cfg]`-gated, so release codegen is unchanged.

### Decision 3 — Wiring evidence keyed on distinct-memo readers

The RED-on-main proof opens N INDEPENDENT `SSTableReader`s on the real BTI
`test_da/simple_table` fixture (each with its own empty C3 memo — a faithful stand-in for N
generations). Cloning one `Arc<Reader>` N times would share one memo and coalesce the
encode, masking the fan-out; distinct readers do not. The pre-C4 path records
`KEY_HASH_CALLS == N`; the hoisted path records 1, with byte-identical prune decisions. A
public-`Database`-API point read additionally proves the manager wiring hashes once.

## Risks / invariants

- **Parity (no-heuristics).** `lookup_partition_in_bti_slice(slice, encoded)` is the exact
  zero-copy walker C3 introduced; feeding it `encode_partition_key_for_bti_trie(raw_key)` is
  identical to the raw-key `lookup_raw_key_in_bti_partitions_slice(slice, raw_key)` it
  replaces on the prune path (the latter just encodes then calls the former). Pinned
  `test_da` offsets and 33-table `da` parity are unchanged.
- **Presence-oracle ordering preserved.** BTI still branches to the trie before any
  bloom/Index.db path; the trie miss stays definitive absence; `READ_BLOOM_CHECKS` /
  `READ_PARTITION_LOOKUP` emission is unchanged.

## Deferred (remaining C4 work, out of this change)

The audit's C4 line also calls for replacing the first-targeted-read whole-trie DFS successor
enumeration (`partition_lookup.rs::bti_partition_offsets`, memoized in a `OnceLock`) with
LOCAL successor resolution (a next-greater trie walk, O(depth)) and single-DFS concurrency
hardening. This is deferred: it is BTI-oracle-sensitive (a wrong seek end-bound silently
truncates a partition read), and a correct next-greater walk spans all six node-type families
(Single/Sparse/Dense variants) — a substantial, higher-risk change that warrants its own
focused review and golden coverage, exceeding this MINIMAL hash-hoist change. The current
enumeration remains memoized and correct; the scaling/concurrency rewrite is carried as
follow-up C4 work.
