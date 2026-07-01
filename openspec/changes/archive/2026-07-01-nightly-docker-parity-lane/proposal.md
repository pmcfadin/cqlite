# Proposal: nightly-docker-parity-lane

> Milestone: M7 / Cassandra Byte-for-Byte Parity Program (epic #966 → #974 Public Claims and CI Gates).
> Issue: #1025. Routing: **design-driven** (a new scheduled CI lane + its runner script + manifest tier
> wiring + a generated report; no new oracle bytes, no SSTable read/write code changes) → OpenSpec.

## Why

The parity program already defines a `nightly_docker` tier in the tier-contract
(`docs/development/parity-ci-tiers.md`): "Scheduled (nightly) verification that regenerates or
re-validates fixtures inside a real Cassandra Docker image — catching drift the committed goldens
cannot." Today that tier is **honored only as scattered, single-purpose schedules** — the live
differential compaction byte-tier rides `compaction-parity.yml`'s nightly cron, BTI `da` read-back rides
`e2e-readback.yml`, and the sstableloader / `nodetool refresh` live read-backs ride
`cassandra-validation.yml` and `e2e-readback.yml`. There is:

- **No single nightly lane** that runs the heavier live-Cassandra differential checks together, so the
  release checklist's "recent `nightly_docker` pass" item has no one workflow to cite.
- **No Bloom false-positive-rate (FPR) gate.** The FPR check exists only as a `manual_debug`,
  report-only test (`filter_db_statistical_false_positive_rate_slow`, gated behind
  `CQLITE_FILTER_FPR_SLOW=1`) that asserts a generous saturation ceiling and never compares the measured
  FPR to Cassandra's configured `bloom_filter_fp_chance`. Epic #974 explicitly requires the Nightly
  Docker tier to include this check.
- **No nightly report** that distinguishes hard failures (no-false-negative violations, BTI/compaction
  logical divergence) from advisory byte-tier gaps, even though epic #974's acceptance criteria demand
  exactly that distinction.

These checks are too slow/heavy for per-PR CI (they build pinned Cassandra source and/or stand up a live
container), which is precisely what the `nightly_docker` tier is for. This issue creates the dedicated
lane, fits it to the existing tier contract, and wires the FPR check into it.

## What Changes

- **New scheduled workflow** `.github/workflows/nightly-docker-parity.yml` (trigger: `on: schedule`
  nightly + `workflow_dispatch`). It stands up pinned Cassandra 5.0.2 by **reusing the existing
  `test-data/scripts` Docker machinery** and runs, as a single citable lane, the `nightly_docker`-tier
  scenarios the manifest already tags plus the new Bloom FPR check.
- **New runner script** `test-data/scripts/nightly-docker-parity.sh` that drives the lane locally and in
  CI (one command), invoking the existing generators/tests rather than duplicating them, and emitting a
  per-scenario result + a Bloom FPR summary.
- **Bloom FPR gate** wired into the lane: deserialize real Cassandra `Filter.db` fixtures, assert **no
  false negatives** for keys Cassandra wrote, and report/enforce a configured FPR threshold (gating
  policy is an owner fork — see design.md; the no-false-negative property is always hard-fail).
- **Manifest tier wiring**: point the relevant `nightly_docker` scenarios' `ci.workflow` at the new
  lane, and promote `cass.filter_db.statistical_false_positive_rate` from `manual_debug` into the lane
  with the agreed gating posture. Regenerate `docs/reports/cassandra-test-parity.md` so the report
  reflects the new owning workflow.
- **Nightly report**: the lane publishes a report (GitHub step summary + uploaded artifact) that
  separates **hard failures** from **advisory byte-tier gaps**, and on failure retains Cassandra logs,
  CQLite logs, fixture metadata, per-scenario JSONL diffs, the Bloom FPR summary, and reproduction
  commands.

Non-goals (see Out of scope below) — this does NOT add or change byte-fixture generation, does NOT
implement failure-to-issue automation (#1028), and does NOT replace per-PR required-parity lanes.

## Capabilities

### Modified Capabilities
- `parity-ci-tiers`: extend the existing capability with requirements that bind the `nightly_docker` tier
  to a concrete dedicated lane, define the Bloom FPR gate's behavior and the hard-fail vs. advisory
  reporting contract, and require the manifest tier wiring + report regeneration. This sharpens the
  abstract `nightly_docker` contract (already specced) into a verifiable lane without changing the other
  tiers' contracts.

## Impact

- **CI (new)**: `.github/workflows/nightly-docker-parity.yml` (scheduled + dispatch).
- **Scripts (new)**: `test-data/scripts/nightly-docker-parity.sh` (lane runner; reuses
  `e2e-cassandra-readback.sh`, `gen-wide-bti.sh`, `compaction-parity/`, and the existing Filter.db test).
- **Tests (reused, possibly extended)**: `cqlite-core/tests/sstable_parity_filter_db_test.rs`
  (FPR/no-false-negative), `cqlite-core/tests/issue_911_bti_sstabledump_parity.rs` (BTI `da`), the
  compaction-parity Gradle harness (`compaction-parity/`).
- **Manifest**: `test-data/cassandra-parity-manifest.yml` — `ci.workflow` for `nightly_docker` scenarios;
  promote `cass.filter_db.statistical_false_positive_rate` into the lane.
- **Report**: regenerate `docs/reports/cassandra-test-parity.md` (`cassandra-parity report`).
- **Out of scope**: byte-fixture generation or corpus regeneration changes (that is
  `exhaustive_regeneration` / #1026); failure-to-issue automation (#1028); promoting compaction byte
  parity to a required gate (stays advisory until the byte tier is stable, per epic #974); changing the
  required-parity per-PR lanes (#1024); any SSTable reader/writer code change.
