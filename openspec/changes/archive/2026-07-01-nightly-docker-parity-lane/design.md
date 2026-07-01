# Design: nightly-docker-parity-lane

> Issue #1025 (epic #974). Grounded in the existing parity lanes
> (`compaction-parity.yml`, `e2e-readback.yml`, `cassandra-validation.yml`,
> `live-cell-compaction-parity.yml`), the tier contract
> (`docs/development/parity-ci-tiers.md`), and the manifest
> (`test-data/cassandra-parity-manifest.yml`).

## Context: what already exists (do not duplicate)

- **Tier contract.** `docs/development/parity-ci-tiers.md` already defines `nightly_docker`: scheduled,
  live-Cassandra-backed, allows `canonical_semantic` + `byte_for_byte`, non-blocking for in-flight PRs
  but a failure files/updates a tracking issue and blocks release, artifact retention ≥ 30 days, and the
  release checklist already has a "recent `nightly_docker` pass" item.
- **Live-Cassandra machinery (reuse).** `test-data/scripts/e2e-cassandra-readback.sh` stands up
  Cassandra 5.0.2 via `test-data/docker/docker-compose-cassandra5.yml` (start-clean.sh + EXIT-trap
  cleanup) and drives write → flush → export → `nodetool refresh` / `sstableloader` → sstabledump +
  cqlsh SELECT. `gen-wide-bti.sh` generates BTI (`da`) fixtures. The differential compaction harness
  lives in `compaction-parity/` and builds pinned Cassandra source (cassandra-5.0.2) via
  `bootstrap-cassandra.sh`, with a `gradle test` LOGICAL tier (hard gate) and a `gradle byteParity`
  BYTE tier (non-blocking, already declared `nightly_docker`).
- **Bloom checks (reuse + promote).** `cqlite-core/tests/sstable_parity_filter_db_test.rs` already has a
  strict, fail-closed `filter_db_strict_parameters_and_no_false_negative` test (over BIG fixtures, keys
  from Index.db) and a `filter_db_statistical_false_positive_rate_slow` test (50k absent-key probes,
  fixed seed, reports measured FPR, currently asserts only a < 0.50 saturation ceiling, gated behind
  `CQLITE_FILTER_FPR_SLOW=1`). Manifest scenario `cass.filter_db.statistical_false_positive_rate` is
  `status: planned`, `ci.tier: manual_debug` — its `next_step` is exactly "assert the measured FPR tracks
  the configured fp_chance".
- **Existing nightly crons (avoid collision).** 02:30 (e2e-readback), 04:17, 04:31, 04:53
  (live-cell-compaction), 05:23, 05:43 (compaction byte tier). New lane uses **06:11 UTC** (off the rush,
  after the source-build-heavy 05:xx jobs so a shared Cassandra-source cache is warm).

## Decision D1 — One dedicated aggregating lane (not N more crons, not a per-PR job)

**Decision.** Add a single `.github/workflows/nightly-docker-parity.yml` that runs the
`nightly_docker`-tier payload together (live read-back, BTI `da` parity, differential compaction logical
parity, Bloom FPR) and publishes one report.

**Alternatives beaten:**
- *Leave it scattered across the four existing nightly crons.* Rejected: the release checklist needs a
  single citable "recent `nightly_docker` pass", and epic #974 wants one nightly report that separates
  hard failures from advisory gaps. Four independent crons cannot produce that.
- *Make it a per-PR required check.* Rejected by the tier contract — `nightly_docker` is scheduled and
  non-blocking for in-flight PRs precisely because it builds Cassandra source / stands up a container
  (too slow for every PR). The per-PR LOGICAL compaction gate and `required_parity` lanes already cover
  PRs.
- *Replace the existing nightly crons.* Rejected: `compaction-parity.yml` byte tier and
  `cassandra-validation.yml` ingest-path coverage serve distinct decision records (#728, #1016). The new
  lane **calls into** the same scripts/harness rather than deleting them; it is the aggregation point.
  (Reducing cron duplication later is a follow-up, out of scope here.)

## Decision D2 — Stand up live Cassandra by reusing the script machinery (not a GH service container)

**Decision.** The runner script `test-data/scripts/nightly-docker-parity.sh` reuses
`e2e-cassandra-readback.sh` + `docker-compose-cassandra5.yml` for the live read-back legs and
`compaction-parity/scripts/bootstrap-cassandra.sh` for the differential compaction leg, exactly as the
existing nightly workflows do.

**Alternatives beaten:**
- *GitHub Actions `services:` Cassandra container.* Rejected: the repo's machinery already manages
  compose lifecycle + EXIT-trap cleanup + artifact capture, is the local-repro path, and pins the image
  in one place (`docker-compose-cassandra5.yml`). A `services:` block would fork the pin and lose the
  local-repro parity.
- *A brand-new bespoke Docker setup.* Rejected — duplicates pinned-version management and drifts from the
  fixture generators that produce the goldens this lane re-validates.

## Decision D3 — Bloom FPR gate: no-false-negative is ALWAYS hard-fail; statistical FPR threshold is configurable + an owner fork on gating

**Decision.** The lane runs two Bloom legs:
1. **No-false-negative (hard-fail, always).** Reuse `filter_db_strict_parameters_and_no_false_negative`:
   for every Cassandra-written `Filter.db`, decode it, and assert "maybe present" for every key Cassandra
   wrote (raw key bytes from Index.db). A single false negative fails the lane. This is a `p0_data_loss`
   correctness property and is never advisory.
2. **Statistical FPR threshold.** Promote `filter_db_statistical_false_positive_rate_slow` into the lane:
   probe a large absent-key sample and compare the measured FPR against the configured
   `bloom_filter_fp_chance` within a documented statistical tolerance. **Whether a threshold breach
   hard-fails or is advisory at first is an owner fork (F4 below).** Recommended default: **advisory
   (report-only) on first landing**, promoted to hard-fail once larger-cardinality fixtures make the
   measured FPR track `fp_chance` reliably (the scenario's own `next_step`).

**Alternatives beaten:**
- *Make the whole Bloom check advisory.* Rejected: a false negative is data loss (`p0_data_loss`) — the
  tier contract forbids smoke-only proof for a P0 data-loss path. No-false-negative must hard-fail.
- *Hard-fail the statistical FPR immediately.* Rejected for first landing: the committed fixtures are
  tiny (single-long bitsets) where analytic FPR is dominated by quantization, so a tight threshold would
  flake. Start advisory, tighten when the fixtures grow (recorded as the manifest `scope.next_step`).

## Decision D4 — Two-class gating + report (hard failures vs. advisory byte-tier gaps)

**Decision.** The lane classifies each leg as **hard-fail** or **advisory**, fails the workflow iff any
hard-fail leg fails, and always publishes a report (GitHub step summary + uploaded artifact) with a
per-scenario table marking class + outcome and a Bloom FPR summary section.

- **Hard-fail legs:** no-false-negative Bloom membership; BTI `da` sstabledump logical parity;
  differential compaction LOGICAL parity; live read-back semantic equivalence.
- **Advisory legs (`continue-on-error`):** differential compaction BYTE tier (the #842 north star, still
  non-blocking per epic #974 until the byte tier is stable); statistical FPR threshold (per D4/F4 default).

This mirrors `compaction-parity.yml`'s existing LOGICAL-hard / BYTE-advisory split and the tier
contract's "non-blocking for in-flight PRs, failure blocks release" posture.

**Alternative beaten:** *one pass/fail bit.* Rejected — epic #974 explicitly requires the nightly report
to "distinguish hard failures from advisory byte-tier gaps".

## Decision D5 — Failure artifacts + manifest/report feedback

**Decision.** On failure the lane uploads (≥ 30-day retention per the tier contract): Cassandra container
logs, CQLite logs, fixture metadata (pinned version + git SHA), per-scenario JSONL diffs, the Bloom FPR
summary, and a reproduction-commands block (the exact `nightly-docker-parity.sh` invocation + each
underlying command). The relevant `nightly_docker` scenarios' `ci.workflow` is repointed at the new lane
and `docs/reports/cassandra-test-parity.md` is regenerated (`cassandra-parity report`) so the manifest
report reflects the owning workflow. The lane is the citable evidence for the release checklist's "recent
`nightly_docker` pass".

**Alternative beaten:** *logs-only, no report feedback.* Rejected — the release checklist and the
manifest report must be able to point at this lane; otherwise "recent nightly pass" is unverifiable.

## Decision D6 — Scope to the manifest's `nightly_docker` scenarios (no new fixtures)

**Decision.** The lane's payload is exactly the scenarios the manifest already tags `nightly_docker`
(live read-back via loader/refresh, BTI `da` write/read, differential compaction) plus the promoted FPR
scenario. It does NOT regenerate the full corpus or create new byte fixtures (that is
`exhaustive_regeneration` / #1026).

The issue lists illustrative manifest IDs (`nightly.live_readback.sstableloader`,
`nightly.live_readback.nodetool_refresh`, `nightly.compaction_tombstone_ttl`, `nightly.bti_sstabledump`,
`nightly.bloom_filter_fpr`). The repo's actual tagged scenarios are
`cass.write_load_path.live_readback.semantic_only`, `cass.bti_big_version_matrix.bti_da_write_read`,
`cass.compaction.*` (byte tier), and the to-be-promoted `cass.filter_db.statistical_false_positive_rate`
+ `cass.filter_db.no_false_negative_membership`. Tasks wire these real IDs (not invent the illustrative
ones); any rename is an owner-confirmable detail, not a behavior change.

## Product forks for the OWNER (Seam 1) — recommended defaults in brackets

- **F1 — Schedule cadence.** Nightly vs. weekly, and CI-minute cost (this lane builds Cassandra source
  and/or stands up a container — roughly compaction-parity + e2e-readback combined, ~tens of minutes).
  *[Recommended: nightly at 06:11 UTC, matching the existing nightly parity cadence; revisit to weekly
  only if CI-minute spend is a concern.]*
- **F2 — Cassandra version(s).** 5.0.0 vs. 5.0.2. The corpus and every existing live lane are pinned to
  **5.0.2** (`CASSANDRA_REF: cassandra-5.0.2`, git SHA `f278f677…`). *[Recommended: 5.0.2 only, matching
  the pinned corpus; a second version is a separate matrix expansion.]*
- **F3 — Does the lane block anything, or is it a pure monitoring signal?** The tier contract says
  non-blocking for in-flight PRs but blocks **release** via the checklist. *[Recommended: non-blocking
  for PRs; blocks release through the existing checklist item; failure surfaces in the report (and, once
  #1028 lands, files a tracking issue).]*
- **F4 — Statistical Bloom FPR threshold: hard-fail or advisory at first?** *[Recommended: ADVISORY at
  first (report measured FPR vs. configured `fp_chance`), promote to hard-fail once larger-cardinality
  fixtures land. No-false-negative is ALWAYS hard-fail regardless of F4.]*
- **F5 — Compaction BYTE tier in this lane: stays advisory?** *[Recommended: YES, advisory
  (`continue-on-error`), matching `compaction-parity.yml` and epic #974 ("do not promote compaction byte
  parity to required until the byte tier is implemented and stable").]*
