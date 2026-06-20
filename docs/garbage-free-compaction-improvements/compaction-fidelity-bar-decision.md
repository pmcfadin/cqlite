# Compaction Fidelity Bar — Decision (Issue #818)

> **Status: DECIDED 2026-06-19.** This is the gating decision for Epic #817. Every other
> child issue (#819, #821–#825) is sized by the tiers below. When a finding's verdict
> changes, update the table here and the **`Verdict (#818)`** column in
> [`cqlite-findings-and-applicability.md`](./cqlite-findings-and-applicability.md) — update the
> separate **`Status`** column only when the verification state itself changes (e.g.
> `VERIFY` → `HOLDS`/`DIVERGENT`).

## The bar: a three-tier model (not flat "byte-identical vs semantic")

The original framing in #818 — *byte-identical* (A) vs *semantic-correctness + readable* (B) —
is rejected. The binary choice mislabels findings: it would call the verified #4/#21 tie-break
"cosmetic" and skip it, but that tie-break changes *which value survives a merge*, i.e. it
changes query results. The bar must separate **which data wins** from **the exact bytes on disk**.

CQLite compaction output is held to three tiers:

### Tier 1 — MUST: Cassandra-readable (validity)

Output is a structurally valid SSTable that **a real Cassandra 5.0 node loads and reads without
error**. Anything that corrupts the index, ordering, framing, or component set fails this tier.

**Stricter scope — shipped to live cluster (decided 2026-06-19).** CQLite-compacted SSTables
may be loaded by a real Cassandra node, not only re-read by CQLite/sstabledump. Tier-1 validity
therefore includes a real node's *load path* requirements, which #819 must enforce as an
explicit checklist:

- Correct **generation numbers** / file naming for the target format.
- **TOC.txt** lists exactly the components present; **Digest.crc32** matches Data.db.
- **Component completeness**: every component a node expects on load is present and internally
  consistent (Data, Index/Partitions+Rows, Statistics, Filter, CompressionInfo, Summary as
  applicable).
- Index/Summary offsets, partition ordering, and clustering ordering are correct (a wrong
  order makes a node's binary search mis-read the table).

### Tier 2 — MUST: resolution-equivalent (data fidelity)

For any input, the set of **surviving** cells/tombstones **and their values** equals Cassandra's
merge result — same LWW outcome, tombstone supremacy, tie-breaks, shadowing, and purge
decisions. "The same data wins, so the same query answers come back." Get the wrong survivor and
CQLite silently returns different data than Cassandra; that is pure correctness, and it is the
line worth holding.

### Tier 3 — NON-GOAL: byte-identical serialization

Exact bytes of `Data.db` / `Statistics.db` / `Filter.db` are **not** a product requirement.
Pursue byte-identity only **opportunistically**, as a *testing convenience* where it is cheap
(it makes the differential harness a simple `diff`), never as a goal in itself.

**Why byte-identity is a non-goal.** Nothing downstream diffs CQLite's output against Cassandra's
*bytes* — a node reads any valid SSTable regardless of encoding choices. Full byte-identity is
also largely infeasible: `Statistics.db` embeds streaming-histogram state whose byte layout
depends on merge order (see [`tombstone-histogram-spool-proposal.md`](./tombstone-histogram-spool-proposal.md)
— even Cassandra's *own* changes there break byte-identity), and `Filter.db` / compression
chunking add more non-load-bearing variance. Chasing those bytes would burn the epic on
differences that never affect a query result.

## What "pass" means for the differential harness (#819)

The harness's **gate** is **logical merge equivalence**, not raw bytes: walk both outputs
partition-by-partition / cell-by-cell and assert identical surviving tuples (Tier 2). The
equivalence tuple must include **all read/merge-affecting metadata**, not just the value — two
expiring cells with the same value and write-timestamp but different TTL/expiry produce
different future query results and later compaction outcomes, so a value-only tuple would let a
real divergence pass. Compare, per surviving unfiltered:

- clustering key (and row vs static-row vs range-tombstone-marker kind);
- column identifier **and complex-cell path** (collection key / UDT field / list TimeUUID);
- raw value bytes;
- write timestamp;
- **TTL and local deletion time** (expiring-cell liveness);
- cell/row **deletion info** (cell tombstone, row deletion time + local deletion time);
- **range-tombstone bound/boundary** markers and their deletion times;
- **complex (collection/UDT) deletion** metadata.

Plus a Tier-1 load-path check (above). A raw-byte `diff` of `Data.db` / `Statistics.db` is kept
as an **optional secondary signal for debugging only** — reported per-component with byte
offsets, never a gate.

## Reclassification of findings under this bar

Each `VERIFY` / `DIVERGENT` row from
[`cqlite-findings-and-applicability.md`](./cqlite-findings-and-applicability.md) Part 1 is now
classified **fix-required** vs **document-only**:

| Finding | Tier | Verdict | Reason |
|---|---|---|---|
| #4/#21 equal-ts value tie-break | 2 | **FIX** | Match Cassandra's "greater raw value bytes win". Changes surviving value → changes query results. |
| #13/#3 tombstone > expiring at equal ts; strict flags | 2 | **FIX** | Wrong survivor / invalid flag combos. |
| #14/#17 complex-deletion shadow-before-purge / supersede | 2 | **FIX** | Wrong survivor for multi-cell columns. |
| #18 complex-column merge **pairing** by path comparator | 2 | **FIX pairing** | Wrong pairing → wrong survivor. Output *order* bytes are Tier-3, skip. |
| #10 DESC empty-vs-valued clustering order | 1 | **FIX** | Wrong clustering order makes a node's binary search mis-read the SSTable. |
| #16 64-bit offsets (>2 GiB) | 1 | **FIX** | Wrapped offset corrupts the index. |
| #22 header-driven static/columns (not schema) | 2 | **FIX** | Wrong column framing → dropped/misaligned data. |
| #12 column-subset encoding (sparse rows) | 1/2 | **FIX correctness incl. boundary** | Node must decode it. The exact `<64`/`≥64` mode selection (the 64-column boundary) is **decode-critical** — wrong side produces a row a real node mis-parses — so it is Tier-1/2 FIX, **not** Tier-3. Only purely cosmetic layout that does not change how a reader decodes is Tier-3. |
| #23 AlwaysPresentFilter handling | 1 | **FIX** | Read robustness; must not crash on `fp_chance = 1.0`. |
| #25 RT-marker / complex-deletion size vint (long domain) | 1 | **FIX** | Wrong size vint → reader mis-parses. |
| #2 previousUnfilteredSize | 3 | **DOCUMENT / best-effort** | Written but skipped by all current readers. Fix only if cheap or if we add reverse iteration. Note: under "shipped to live cluster", confirm a real node does not validate it on load; if it does, this promotes to Tier-1 FIX. |
| #5/#6 BTI block boundaries | 1 (when BTI write lands) | **DEFER** | CQLite write path is BIG-format; overlaps epic #762. |
| #24 Accord nowInSec→gcBefore | — | **N/A** | No Accord in CQLite. |
| #26 counter merge | — | **N/A-yet** | CQLite doesn't merge counters. |

**Net effect.** Most findings still get fixed — but for the *right reason* (validity or data
fidelity). We explicitly stop chasing `Statistics.db` / `Filter.db` byte-identity and
serialization order (Tier 3). #2 `previousUnfilteredSize` drops from "must" to "best-effort"
(verify-and-document), pending the live-node-load-path check noted above — that is the main
scope saving.

## #4/#21 ruling (explicit)

**FIX.** `reconcile_cluster` must match Cassandra's `Cells.resolveRegular`: at equal timestamp
with both cells live, keep the cell whose **raw serialized value bytes** are strictly greater
under unsigned lexicographic comparison (skipping the vint length prefix), **not** file/run
order. The existing reproduction test
`test_real_merger_value_tiebreak_diverges_from_cassandra` (`merge.rs`) must flip to a
**convergence** test once the fix lands. Implementation note: the fix needs each cell's raw
Cassandra value bytes available at reconcile time (serialize-for-compare, or carry raw bytes
through the merge) — a bounded but real plumbing change; #820 characterizes it across non-text
types.

## Implementation note for the harness (#819)

Because the bar is "shipped to live cluster", #819's Tier-1 check is not satisfied by
"sstabledump can read it" alone — it must assert the real-node load-path checklist above. Keep
the raw-byte component diff as a debug aid; gate on logical equivalence + load-path validity.
