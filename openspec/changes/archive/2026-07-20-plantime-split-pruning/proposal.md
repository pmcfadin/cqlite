# plantime-split-pruning

## Why

The v0.15.0 milestone soak (#2661) capped at ~39 qps @ 32 threads with p50 798ms — the latency was
almost entirely client-side queue wait against a throughput ceiling (server-side ~2ms per read). Read-only
triage of the Trino/Flight connector (#2679) found the largest single lever:

`CqliteFlightSplitManager.getSplits` receives Trino's pushed-down `Constraint` but **never uses it to
prune token ranges** — the predicate is only carried on each split's ticket and applied server-side. As a
result **every** query, including a single-partition point read, emits **one split per read-replica token
range**. On the standing rig (3 nodes, `num_tokens 16` → ~48 ranges) a point read fans out **~48 DoGets
across all 3 pods to fetch a single partition**. Point reads don't concentrate load — they multiply it ~48×.

- **Milestone:** 0.15 — cqlite-trino latency/throughput/operations theme (epic #2403), Lane-B goals
  B1/B2 (`docs/architecture/performance-goals-2026-07.md`).
- **Routing:** design-driven. Token-computation source (no Java Murmur3 exists today), partitioner
  metadata plumbing under the no-heuristics mandate, IN-list handling, and the correctness harness are
  real design calls — hence OpenSpec, not a bare oracle-driven fix.

## What Changes

When Trino's pushed-down constraint **fully binds the partition key** — an equality domain on every
partition-key column, or a discrete-set (IN) domain over full keys — the split manager computes the
Murmur3 token(s) at plan time and emits splits **only for the covering token range(s)**. A single-key
point read becomes **1 DoGet to 1 pod**. Anything less than a fully-bound partition key (range scan,
partial PK, non-PK predicates, null-allowed domains, an unsupported/unknown partitioner) keeps the
current full fan-out.

Concretely:

- Read `constraint.getSummary()` (a `TupleDomain`) inside `getSplits` (currently ignored) — this is where
  Trino delivers simple full-PK point reads (the expression is `TRUE`; the equality lives in the summary).
- Add a **Java Murmur3 token computation** (`Murmur3Token`) that mirrors the Rust authority
  `cqlite-core/src/util/cassandra_murmur3.rs` (`cassandra_murmur3_token`) **and** the canonical
  partition-key byte serialization from `cqlite-core/src/storage/write_engine/mutation.rs`
  (`PartitionKey::to_bytes`: single-component = raw value bytes; multi-component =
  `[len:u16 BE][bytes][0x00]` per component).
- Cross-reference the summary's per-column equality domains against
  `PrimaryKeyExtractor.extract(ddl).partitionKey()` (names + order) and the column CQL types (from the
  column handles / Arrow schema) to build the typed partition-key value tuple(s).
- Filter `buildSplits`' emitted ranges to those whose `(start, end]` half-open interval (with the existing
  wraparound convention) contains a computed token — reusing the token-membership logic already encoded in
  `validateRingCoverage` / the server's `token_in_half_open_range`.
- **Fail-safe by construction:** any doubt (partial PK, a PK column absent or not a single-value/discrete
  domain, null-allowed, a value that cannot be typed/serialized, an unknown/non-Murmur3 partitioner) → **no
  pruning, full fan-out** — never fewer splits than correctness requires.

## Non-goals

- **Server-side predicate application is unchanged.** This change only reduces which splits are emitted;
  the ticket predicate and server-side filtering stay exactly as-is. Pushdown remains an optimization, not
  an enforcement contract (the `applyFilter` honesty contract is preserved).
- **No partial-PK or clustering-key pruning.** Only fully-bound partition keys prune. Clustering
  predicates, range scans, and partial PKs keep full fan-out.
- **No new partitioner support.** Only `Murmur3Partitioner` (the ring's existing hard assumption) prunes.
  A non-Murmur3 or unknown partitioner disables pruning; adding ByteOrdered/RandomPartitioner is out of scope.
- **No change to token-range topology discovery.** The Sidecar `token-range-replicas` source and the ~48
  ranges are unchanged; we filter them, we don't re-derive them.
- **Field verification is report-only, next round** (per-pod DoGet collapse, warm point p50, qps @ 32
  threads off ~39). Not gated by this change's merge.

## Impact

- **Affected code (Java connector, `trino-connector/`):**
  `CqliteFlightSplitManager.getSplits`/`buildSplits` (consume the constraint + prune), a new
  `Murmur3Token` + partition-key byte-serializer, plumbing typed PK values from `applyFilter`'s
  summary/`PrimaryKeyExtractor`.
- **No Rust changes** — the server already prunes by token at DoGet time; this pushes the same decision
  earlier so the fan-out never happens.
- **Doctrine:** exercises **no-heuristics** (token from schema-declared PK columns + assumed-Murmur3
  ring, never inferred from data; unknown partitioner → no pruning) and **wiring-evidence** (an end-to-end
  test through the real Trino query path shows a point read served by a single DoGet).
