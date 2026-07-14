# Design — read-path forcing knob + differential lane (#1918)

## Anchors on `main`
- `classify_partition_lookup(predicates, schema) -> PartitionLookupOutcome`
  (`select_executor/lookup.rs:92`). Outcome is `Targeted(Vec<u8>)` / `MultiTargeted(Vec<Vec<u8>>)` /
  `Fallback(FallbackReason)`.
- Five call sites consume it: `execute.rs` metadata WRITETIME/TTL (`:497`), schemaless seek (`:481`),
  materializing (`:624`); `streaming.rs` (`:97`); `stream_agg.rs` (`:188`).
- `AccessPath` closed enum + process-global `record`/`last`/`reset` probe (`query/access_path.rs`);
  `FallbackReason` closed set. `honest_targeted_path(target, engaged)` already demotes a claimed
  target to `FallbackFullScan{TombstonesBuildNoPrune}` when the storage call did not actually prune.
- `QueryConfig` in `config.rs`; the `CQLITE_TTL_NOW_OVERRIDE_SECS` reader seam + the
  `query_semantics_oracle_parity.rs` lane are the model for a pinned-`now`, fail-closed differential.

## Chosen design: one forcing gate over the outcome, honest AccessPath, distinct fail-closed error

### Knob surface
`ReadPathMode { Auto, Point, Full }`. Resolution order, read **once** into a `OnceLock`:
1. explicit `QueryConfig.forced_read_path: Option<ReadPathMode>` (programmatic/`Database` config), else
2. `CQLITE_READ_PATH` env (`auto|point|full`, case-insensitive), else
3. `Auto`.
An unrecognized env value is a **loud startup-time error**, never silently `Auto` (a typo'd knob that
silently no-ops would defeat the knob's purpose). `Auto` short-circuits before any allocation, so the
unset path is byte-for-byte today plus one relaxed atomic load.

### The single gate (no per-site fork)
A `fn apply_forcing(outcome: PartitionLookupOutcome, mode: ReadPathMode) -> Result<ForcedOutcome>`
wraps the classifier's return exactly once, applied at all five sites via a shared helper so no site
re-implements policy:
- `Auto` → pass the outcome through unchanged.
- `Full` → replace any `Targeted`/`MultiTargeted` with a forced full scan; record
  `AccessPath::FallbackFullScan{ reason: FallbackReason::ForcedFullScan }` (a NEW distinct variant, so
  "forced" is never confused with an organic fallback). Rows are produced by the same full-scan +
  reconciliation code `auto` uses when it falls back — identical semantics.
- `Point` → **fail closed**: if the outcome is `Fallback(r)`, OR the classified target cannot be run as
  a genuinely partition-targeted lookup at this surface (an unwired metadata-`IN` fan-out, or a
  `tombstones`-build no-prune where `engaged == false`), return
  `Error::ForcedReadPathUnavailable { forced: "point", reason }` naming the concrete
  `FallbackReason`/surface. Never a silent full scan.

`Point` fail-closed is defined against the *actually executed* path (engaged), not just the
classification, so a classification that names a target but degrades at execution still errors — that
is the "remove all doubt" contract. Because `engaged` is only known after the storage call, the point
gate checks classification up front (fast fail for `Fallback`) and converts a post-call
`engaged == false` into the same distinct error.

### Why this beat the alternatives
- **Beat per-site forcing** (a check at each of the five call sites): five copies of the policy is the
  exact bug farm the honest-path work already fought; a single wrapper keeps one contract.
- **Beat "silent fallback in point mode"** (the retired #942 sketch's `point = error only if
  unclassifiable`): silent execution-time degradation is what the knob exists to expose. Fail closed on
  the *executed* path, not just classification.
- **Beat overloading the value set with `sequential`/`summary-guided`** (reader index-resolution
  strategy, post-#2412): that is an orthogonal reader-internal axis, not a routing decision
  `classify_partition_lookup` owns; folding it in would make one knob mean two things. Deferred as an
  OWNER FORK below.

## Differential-equality lane
A new integration test target `point_vs_full_differential.rs` drives a corpus query matrix (single-key
`=`, `IN`, clustering-pushdown, WRITETIME/TTL variants; includes multi-generation, tombstone, and TTL
fixtures — the divergence classes #1741 hid) **twice per query** — once under forced `point`, once
under forced `full` — and asserts normalized (rows, values, order) equality. It is a
**query-semantics-class** oracle (CQLite-vs-CQLite): TTL `now` is pinned via
`CQLITE_TTL_NOW_OVERRIDE_SECS`, never wall-clock. A query that is not point-eligible is exercised only
under `full` vs `auto` (both full-scan) so the corpus stays exhaustive without asserting a
fail-closed error as a "divergence". The lane is verified by seeding a divergence (temporarily breaking
one path locally must turn it red — regression-test-verification doctrine).

### Lane placement (recommended, with justification)
Fold the target into the **existing `integration-tests` gate tier** (fail-closed under
`CQLITE_REQUIRE_FIXTURES=1`, SKIP-loud when the corpus is absent), NOT a new standalone heavyweight
gate component. Justification vs the #1825 gate-cost budget:
- It reuses the already-fetched corpus and adds no new gate stage/slot — running each small-corpus
  query twice is cheap relative to a new component's fixed setup + a new SUMMARY line.
- The query-semantics oracle is a *separate* component because it is CQLite-vs-Cassandra with its own
  committed fixtures; this lane is CQLite-vs-CQLite over the same corpus the integration tier already
  loads, so it belongs in that tier.
- **Alternative (owner fork):** a dedicated `point-vs-full-differential` component mirroring
  `query-semantics-oracle` if the owner wants an independent SUMMARY line / independent SKIP visibility.
  Costs one more gate slot; recommended only if independent reporting is valued over budget.

## Observability
The forced choice is recorded through the existing `AccessPath` probe (`record`/`last`), so
`--explain` and tests read it with no new mechanism. Surfacing the `AccessPath` label (+ a "forced"
marker) in the CLI `--explain` output (`query.rs`) is in scope **if cheap**; if it grows the file past
the ratchet or needs its own UX pass, it splits to a follow-up (tracked in tasks.md).

## Risks
- **`Point` over-strictness**: erroring on a genuinely-eligible query that degrades only on the
  `tombstones` build. Mitigation: the error names the concrete reason; the differential lane runs on the
  default build; `tombstones`-build behavior is documented.
- **Forced-full != organic-full ordering**: `full` must reuse the exact full-scan+merge path `auto`
  falls back to, so multi-partition ordering matches. Mitigation: the differential lane asserts order.

## OWNER FORKS
1. **Reader index-resolution axis** — should the knob (or a second knob) also force `sequential` vs
   post-#2412 `summary-guided` BIG-index iteration? Excluded here (orthogonal layer). Fork: separate
   change, compound value, or leave reader-internal.
2. **Lane placement** — fold into `integration-tests` (recommended) vs a dedicated gate component.
3. **`--explain` forced marker** — surface now (if cheap) vs split to a follow-up.
