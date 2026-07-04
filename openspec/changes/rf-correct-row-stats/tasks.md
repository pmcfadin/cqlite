# Tasks: rf-correct-row-stats (#1336)

All work is Java, `trino-connector/` only. No Rust, no wire, no config changes. Fail-first TDD:
write each failing test before the code that satisfies it. Surfaces are named per task
(wiring-evidence).

## 1. Uniform replica-count derivation (pure helper)
- [ ] 1.1 Add a pure static helper (suggested: `uniformReplicaCount(TokenRangeReplicasResponse, String localDatacenter)` beside `replicaHosts` in `CqliteFlightMetadata.java`) returning `OptionalInt`: the distinct scoped read-replica count iff identical (≥1) across all ranges, else empty. Reuse the exact DC-scoping rule of `replicaHosts` (`localDatacenter` when set, else all DCs).
- [ ] 1.2 Tests (new `UniformReplicaCountTest` or extend `AggregateNodeStatsTest`): uniform RF=3 → 3; multi-DC dc1=3/dc2=2 with `localDatacenter=dc1` → 3, unset → 5; non-uniform ranges → empty; zero-replica range → empty; duplicate replica entries within a range are deduped. Fixtures use real `SidecarModels` parsing.

## 2. Logical row count in `getTableStatistics` (surface: `ConnectorMetadata.getTableStatistics`)
- [ ] 2.1 Failing test first: RF=3, three hosts × 200 rows → expect `ROW_COUNT = 200` (not 600) through `CqliteFlightMetadata.getTableStatistics` end to end (injected fetch functions, real `TableStats` JSON decode) — the issue's acceptance criterion.
- [ ] 2.2 Implement the non-aggregated branch: fetch `tokenRangeReplicas` + `fetchTableStats`, require uniform `R` and `stats.complete()`, report `Estimate.of((double) liveRows / R)`; wrap the whole derivation so any `RuntimeException`/timeout → `TableStatistics.empty()` (mirror `groupRatioForGate`'s degrade posture).
- [ ] 2.3 Memoize per `(keyspace, table)` in the metadata instance; test: repeated calls → one fetch (counting stub).
- [ ] 2.4 Fail-closed tests: non-uniform ranges → `empty()`; `complete=false` → `empty()`; Sidecar throws → `empty()` and no exception escapes.
- [ ] 2.5 Update `CqliteFlightTableStatisticsTest`: non-aggregated pinned case moves from always-`empty()` to logical-count-when-grounded + `empty()`-when-not; aggregated pins (global → 1, grouped → `empty()`) unchanged.

## 3. Unchanged-gate proof
- [ ] 3.1 Run the full existing connector suite; `EstimateGroupRatioTest`, `PrimaryKeyExtractorTest`, `TableStatsTest`, split-manager tests must pass **unmodified**.

## 4. Docs
- [ ] 4.1 Add a stats-semantics note to `docs/flight-trino/PLAN.md` (scan path dedupes by token range; stats path de-replicates by uniform replica-count division; fail-closed conditions; transient-replication + replica-divergence caveats).

## 5. Quality stages (gate → C → roborev)
- [ ] 5.1 `./gradlew test` in `trino-connector/` green (the Java suite is the blast-radius test set).
- [ ] 5.2 `scripts/agent-gate.sh --lite` per fix round; FULL `scripts/agent-gate.sh` ONCE before merge (`CQLITE_DATASETS_ROOT` → main repo's `test-data/datasets`); paste the `==== AGENT-GATE SUMMARY ====` block verbatim.
- [ ] 5.3 `spec-auditor` (C) anchored to `openspec/changes/rf-correct-row-stats/specs/**` — every requirement `satisfied` with a public-surface test as evidence.
- [ ] 5.4 roborev clean (`/roborev-review-branch --base origin/main`); pre-empt the recurring-findings checklist (esp. float division edge cases and no-heuristics).
- [ ] 5.5 PR referencing #1336; merge-on-green per autonomy model; then `flow-finalize` (archive change, remove worktree, close issue).
