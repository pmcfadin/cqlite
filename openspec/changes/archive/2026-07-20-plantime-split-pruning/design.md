# Design — plantime-split-pruning

## Context

`CqliteFlightSplitManager.getSplits(txn, session, table, dynamicFilter, Constraint)` today ignores its
`Constraint`. It calls `sidecar.tokenRangeReplicas(keyspace)` → ~48 read-replica ranges, validates ring
coverage, and `buildSplits` emits **one split per range** unconditionally. The pushed-down predicate is
carried on each split's ticket (`FlightTicketJson`) and applied server-side (best-effort, post-decode).
Trino delivers simple full-PK point reads as a `TupleDomain` in `constraint.getSummary()` (expression is
`TRUE`); `applyFilter` already translates equality/IN/range domains (`PredicateTreeTranslator`), and
`PrimaryKeyExtractor` already yields partition-key column names + order from the DDL.

The server already makes exactly the "is this a full-PK point read" decision (`point_read::detect_route`
→ `PartitionPointRead`/`MultiPartitionPointRead`/`Scan`) and prunes by token at DoGet time
(`token_in_half_open_range`). This change moves that decision **earlier** — to plan time — so the ~48×
fan-out never happens for a bound-PK query.

## Design calls (the real decisions)

### 1. Token-computation source — port Murmur3 to Java (chosen) vs a server round-trip

There is **no Murmur3 in Java today**. Two options:

- **(A · chosen) Port the token computation to Java.** Add `Murmur3Token` mirroring the Rust authority
  `cassandra_murmur3_token` (`cqlite-core/src/util/cassandra_murmur3.rs`) plus the canonical
  partition-key byte layout from `PartitionKey::to_bytes` (`storage/write_engine/mutation.rs`):
  single-component = the raw serialized value bytes; multi-component = for each component
  `[len:u16 BE][value bytes][0x00]` concatenated. Compute the token(s) at plan time, no RPC.
- **(B · rejected) Add a Sidecar/Flight RPC** that returns the covering range for a key. Rejected: adds a
  network hop on the hot path we're trying to shorten, introduces a new endpoint + failure mode, and still
  needs the Java-side value→bytes serialization anyway. The token math is small, deterministic, and
  already has a byte-exact Rust oracle to test against.

**Why A is safe:** the computation is pure and testable against the Rust implementation with shared
vectors. Correctness risk (a wrong token drops rows — see §4) is contained by a differential test that
compares pruned vs forced-unpruned execution, and by fail-safe defaults.

### 2. Partitioner metadata under no-heuristics — assume Murmur3 explicitly, fail-safe otherwise

The AC requires the partitioner come from metadata, "never inferred from data," and unknown → no pruning.
**Reality:** Sidecar exposes no partitioner field anywhere; the entire ring machinery
(`validateRingCoverage`, `SidecarModels`, token parsing) already **hard-assumes `Murmur3Partitioner`** and
treats range bounds as signed-64-bit Murmur3 tokens.

Design: make the assumption **explicit and centralized**. A single `Partitioner` resolution point returns
`Murmur3` when the cluster is Murmur3 (the current and only supported case) and `Unsupported` otherwise;
pruning is gated on `Murmur3`. If/when a partitioner name becomes available from Sidecar metadata, that
resolver reads it; until then it returns the documented ring assumption. **This is not data inference** —
the token is computed from schema-declared partition-key columns against a declared (assumed) partitioner,
and any deviation disables pruning and is logged. This keeps the change honest about the existing
assumption rather than inventing a partitioner field the platform doesn't expose. Surfaced to the owner as
a decision (below), because "assume Murmur3 explicitly" is a doctrine-adjacent call.

### 3. Full-PK-bound detection + IN handling — reuse the summary TupleDomain

Read `constraint.getSummary().getDomains()` in `getSplits`. A query is **fully bound** iff **every**
partition-key column (by `PrimaryKeyExtractor` name/order, case-folded per `KeyColumn`) is present with a
domain that is either a **single value** (equality) or a **discrete set** (IN) — and null is not allowed.

- **Equality on all PK columns** → one typed key tuple → one token → keep the 1 covering range.
- **IN over full PKs:** take the **Cartesian product** of each PK column's discrete-set values (for a
  single-column PK this is just the set; for composite PKs, IN is delivered per-column so the product is
  the set of full keys Trino will read), compute a token per full key, and keep the **union** of covering
  ranges (**deduped**). Never fewer ranges than the union — if the product is empty or any factor is not a
  clean discrete set, fall back to full fan-out.
- **Anything else** (partial PK, a PK column with a range/unbounded domain, non-PK predicates only,
  null-allowed) → no pruning.

The typed values come from the `Domain`'s Trino type + `PrimaryKeyExtractor` column mapping, serialized to
partition-key bytes by the same `to_bytes` layout the token function consumes.

### 4. Correctness posture — split elimination is load-bearing, so fail-safe is mandatory

Server-side predicate filtering is a pure optimization (a missed filter returns extra rows Trino then
drops). **Split elimination is different: a wrongly-pruned split silently drops real rows.** Therefore:

- Every uncertainty → **no pruning** (full fan-out is always correct).
- The **differential test** (pruned vs forced-unpruned, identical rows/values/order) is the primary guard,
  mirroring the point-vs-full pattern (`point_vs_full_differential.rs`, `issue_2398` token-pushdown).
- The pruning path must be **toggleable** so the differential test can force the unpruned baseline (a
  session property or config flag, e.g. `cqlite.split_pruning_enabled`, default on).
- Token membership reuses the **exact** `(start, end]` half-open + wraparound semantics already in
  `validateRingCoverage` / server `token_in_half_open_range` — no second convention.

## Wiring evidence (definition of done)

The public surface is `ConnectorSplitManager.getSplits`. Wiring is proven by an end-to-end test through the
real Trino query path where a fully-bound-PK point read produces a `SplitSource` yielding **exactly one**
split (→ one DoGet to one pod), asserted via split count / a pruning counter on the public surface — not a
helper-only unit test.

## Risks & mitigations

- **Byte-layout drift from Rust** → shared test vectors pinning Java `Murmur3Token` output against
  `cassandra_murmur3_token` for representative single/composite keys and known-token fixtures.
- **Type coverage gaps** (a PK CQL type whose Java→bytes serialization is unimplemented) → that type
  simply doesn't prune (fail-safe), and is logged; not a correctness hazard.
- **Ring gaps/overlap** already fail-closed via `validateRingCoverage`; pruning runs after that guard.

## Open decision for the owner (Seam 1)

**Partitioner assumption:** proceed with the explicit "assume `Murmur3Partitioner`; unknown → no pruning"
resolver (§2), matching the ring's existing hard assumption, since Sidecar exposes no partitioner field —
OR block on first adding a partitioner-name metadata source. Recommendation: **proceed with the explicit
assumption** (it's the honest encoding of today's reality, fully fail-safe, and unblocks the ~48× win);
file a follow-up to surface partitioner name from Sidecar when that metadata lands.
