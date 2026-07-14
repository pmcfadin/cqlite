# Lazy Summary-guided partition index for BIG open (#2412)

## Milestone
0.15 — the cqlite-trino latency/throughput/operations theme (epic #2403, Lane 1).
**Design-driven — OpenSpec + Seam 1 required before any implementation.** This is the "real fix (b)"
from the owner-approved #2385 analysis; the stopgap "(a)" (kill the O(N²) build, retire the redundant
second parse, capacity hints) already shipped in v0.14.1 (PR #2402). Oracle-driven correctness stays
pinned by the existing physical-dump + query-semantics parity nets; this change redesigns *how the BIG
partition index is opened and walked*, which is a structural/design decision, hence OpenSpec.

## Problem (measured)
BIG `SSTableReader::open` still **eagerly parses the ENTIRE `Index.db` into a resident in-memory index**.
`IndexReader::open_with_summary_cancellable` does `file.read_to_end(..)` then parses every partition
entry into `IndexData.partition_entries` (a `Vec`), which the reader pins for its whole lifetime:

- **Cold open cost is linear per generation** — one full `Index.db` parse per generation on every cold
  open (R10 field: 10.7s cold on 2 generations × ~1.96M partitions; the #2385 stopgap removed the
  quadratic and the double parse but the O(N) parse and its resident map remain).
- **~500MB resident per generation** at field scale (~1.58M partitions), pinned for the process lifetime
  by the flight `WarmTableRegistry` (which caches `Arc<SSTableReader>` per generation, #2310). Warm-set
  memory therefore scales with `Σ partitions across all warm generations`, not with the working set.

Cassandra's own model is lazy and CQLite already matches it on ONE path: **BIG** open reloads the
write-time `Summary.db` (O(n/128) sampled entries) and `getPosition` binary-searches the sample then
walks ≤ one `min_index_interval` of `Index.db`; **BTI** open reads three longs from the file tail and
walks the trie on demand. **CQLite's BTI path is already lazy** (flat byte load + on-demand trie DFS)
and sidesteps this entirely — the BIG path is the outlier. `Summary.db` is already parsed today
(`SummaryReader`, used for token-range iteration and the C5 first/last-key short-circuit) but is NOT yet
used to guide `Index.db` access; it lacks a find-by-key search.

## Goal
Make the BIG partition index Cassandra-BIG-lazy:

- **Open O(summary):** load `Summary.db` only (O(n/128) sampled entries). No `Index.db` scan at open.
- **Point lookup O(log n + interval):** binary-search the summary → read + parse ONE `Index.db`
  interval (≤ `min_index_interval` entries) → resolve the partition. `rows_scanned` (index entries
  touched) bounded by one summary interval.
- **Scans summary-guided streaming:** iterate `Index.db` forward from a summary-guided start offset,
  never materializing the whole index; integrates with the #2361 streaming full-index walk and its
  `(token, key)` order guard.
- **Resident memory ≈ summary only:** the warm registry pins the summary (~sampled entries), not the
  full partition map — restart survival then needs no on-disk cache serialization.

## Non-goals
- **No BTI (`da`/`oa`) changes.** BTI is already lazy; this change is BIG (`na`/`nb`) only.
- **No cross-restart cache persistence / daemon posture.** Serializing parsed caches to disk to survive
  restarts is **consciously rejected** (recorded on epic #2403): the chosen "no state recreation on
  startup" strategy is to make *open cheap*, not to persist caches. Not pursued unless field data after
  this fix demands it.
- **No compaction semantics change.** Compaction consumers keep full-ring, non-range-scoped walks.
- **No `Value` decode / comparator / ordering change** — byte-parity is inviolable.
- **No pre-`na` format support** introduced or revisited (version floor unchanged).
- **No change to the public `Database`/`QueryRow`/flight `do_get` result contract** — same rows, same
  bytes; only how they are located changes.

## The #2413 interplay (design MUST take a position — see design.md §E)
#2413 (token-range pushdown into the per-SSTable partition walk, oracle-driven) redesigns the SAME walk
this change owns. The design (§E) lays out both options and **recommends Option A (this change subsumes
#2413)**: a range-bounded walk falls out of summary-guided forward iteration naturally (binary-search the
summary to the split's range start; stop at range end). The owner decides at Seam 1. If Option B is
chosen instead, #2413 lands standalone first on the existing resident index and this change preserves
its range-pushdown pin.

## Doctrine impact
- **No-heuristics (#28) reinforced:** every offset/interval boundary comes from `Summary.db` /
  `Index.db` structure — no guessed boundaries. Absent/corrupt `Summary.db` → a documented, **counted**
  one-time linear `Index.db` scan (the authoritative full parse, recorded as a distinct FellBack full
  parse), never a silent guess (design §A recommends this over a hard error).
- CLAUDE.md / website `agents-developing/`: no doctrine text change; add a one-line note to the format
  debugging / source-map page describing the lazy BIG open once implemented (in-change, per the
  keep-doctrine-current rule).

## Definition of done
`scripts/agent-gate.sh` full PASS (SUMMARY recorded) + spec-auditor **C** PASS (every requirement
`satisfied` with a public-surface test) + roborev clean; `RUSTFLAGS="-D warnings"` clean; no
`unwrap()`/`expect()` in library code; physical-dump + query-semantics parity + flight `do_get` cold+warm
e2e green. Then `openspec archive`.
