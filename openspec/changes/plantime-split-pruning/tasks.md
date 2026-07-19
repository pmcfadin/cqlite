# Tasks — plantime-split-pruning

## 1. Java Murmur3 token computation (mirrors Rust authority)

- [ ] 1.1 Add `Murmur3Token` in `trino-connector/src/main/java/in/mcfad/cqlite/flight/` implementing
      `cassandra_murmur3_x64_128` + `cassandra_murmur3_token` + `i64::MIN → i64::MAX` normalization,
      ported from `cqlite-core/src/util/cassandra_murmur3.rs`. Surface exercised: `Murmur3Token.token(byte[])`.
- [ ] 1.2 Add the canonical partition-key byte serializer (single-component = raw value bytes;
      multi-component = `[len:u16 BE][bytes][0x00]` per component) mirroring
      `PartitionKey::to_bytes` (`storage/write_engine/mutation.rs`). Map CQL/Trino types → value bytes;
      an unsupported type returns "cannot serialize" (→ caller disables pruning).
- [ ] 1.3 Unit test `Murmur3TokenTest` with **shared vectors** pinning Java output to the Rust
      `cassandra_murmur3_token` for single-column and composite keys across common CQL types, incl. the
      normalized `i64::MIN` case. (Spec: "Java Murmur3 matches the Rust authority".)

## 2. Full-PK-bound detection from the constraint summary

- [ ] 2.1 In `getSplits`, read `constraint.getSummary().getDomains()` (currently ignored at
      `CqliteFlightSplitManager.java:48`). Resolve the partition-key columns via
      `PrimaryKeyExtractor.extract(handle.ddl()).partitionKey()` (case-folded per `KeyColumn`).
- [ ] 2.2 Classify: fully-bound-equality (every PK column a single-value domain), fully-bound-IN (every PK
      column a discrete set → Cartesian product of full keys), or not-bound (→ no pruning). Null-allowed,
      range, or missing PK column ⇒ not-bound. Surface exercised: a `BoundPartitionKeys` helper returning
      the typed key tuples or empty.
- [ ] 2.3 Resolve the partitioner (explicit "assume Murmur3; unknown → disable" resolver, §2 of design);
      non-Murmur3/unknown ⇒ no pruning, logged.

## 3. Prune the emitted splits

- [ ] 3.1 Compute token(s) for the bound key tuple(s); dedupe. Filter `buildSplits`' ranges to those whose
      `(start, end]` half-open interval (reuse the wraparound/token-membership semantics from
      `validateRingCoverage` / server `token_in_half_open_range`) contains a computed token. Emit the union.
- [ ] 3.2 Add a toggle (`cqlite.split_pruning_enabled` session property / config, default on) so pruning
      can be forced off for the differential baseline.
- [ ] 3.3 Fail-safe: any exception, empty enumeration, or "cannot serialize" ⇒ full fan-out (never fewer
      splits than correct). Log the skip reason.

## 4. Tests (verify the spec)

- [ ] 4.1 `CqliteFlightSplitManagerTest`: fully-bound single PK over a multi-range fixture → exactly 1
      covering split; partial/absent/range PK → unchanged full fan-out. (Spec req 1.)
- [ ] 4.2 IN-list over full PKs → deduped union of covering ranges; two keys sharing a range collapse to
      one split; never fewer than the union. (Spec req 2.)
- [ ] 4.3 Unknown/non-Murmur3 partitioner → no pruning (logged); un-serializable PK value → no pruning.
      (Spec req 3.)
- [ ] 4.4 Connector-level **differential** test: pruned vs forced-unpruned returns identical rows/values/
      order for a point read and an IN-list; pruned emits ≤ splits. (Spec req 5, point-vs-full style.)
- [ ] 4.5 **Wiring/e2e**: fully-bound-PK point read through the public `getSplits` `SplitSource` yields
      exactly one split, asserted via split count / pruning counter on the public surface. (Spec req 6.)

## 5. Quality gates

- [ ] 5.1 `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).
- [ ] 5.2 Review-first: `rust-reviewer` (Java changes still get a review pass) + roborev on the lite-green diff.
- [ ] 5.3 Full `scripts/agent-gate.sh` PASS once (flow-closer) — confirm the connector Gradle build/tests
      run in the gate; if the connector is not in the gate's component set, run its Gradle test suite and
      record the result in the PR alongside the gate SUMMARY.
- [ ] 5.4 **C** intent audit (`spec-auditor` anchored to `openspec/changes/plantime-split-pruning/specs/**`).
- [ ] 5.5 Final roborev clean; merge-on-green; `openspec archive`.

## 6. Docs / follow-ups

- [ ] 6.1 File a follow-up issue to surface the partitioner name from Sidecar metadata (so the resolver
      reads it instead of assuming Murmur3) — cross-ref this change.
- [ ] 6.2 Note the report-only field-verification round (#2661 soak: per-pod DoGet collapse, warm point
      p50, qps @ 32 threads) as a follow-up — not gated by this merge.
