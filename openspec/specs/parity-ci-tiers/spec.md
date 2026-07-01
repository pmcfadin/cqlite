# parity-ci-tiers Specification

## Purpose
TBD - created by archiving change parity-ci-tier-contracts. Update Purpose after archive.
## Requirements
### Requirement: Each parity CI tier has a documented public contract

The project SHALL maintain a single source-of-truth document
(`docs/development/parity-ci-tiers.md`) that defines, for every CI tier in the manifest enum
(`fast_pr`, `required_parity`, `nightly_docker`, `exhaustive_regeneration`, `manual_debug`), all of:
its purpose, the `evidence.type` values it accepts, its skip policy, its failure policy, its
artifact-retention expectations, and its promotion rules. The document MUST exist before downstream
CI-gate work (#1023–#1026) relies on it.

#### Scenario: Every tier is fully specified
- **WHEN** the tier-contract document is reviewed against the five manifest tier names
- **THEN** each of the five tiers has a section defining purpose, allowed evidence types, skip policy, failure policy, artifact expectations, and promotion rules
- **AND** no tier name from the manifest enum is missing a section

#### Scenario: Promotion rules are explicit
- **WHEN** a reader needs to know how a scenario moves from a weaker tier to a stronger one
- **THEN** the document states the promotion rule for that transition (e.g. `fast_pr` → `required_parity`, `required_parity` → `nightly_docker`/`exhaustive_regeneration`)

### Requirement: The contract distinguishes smoke, canonical-semantic, and byte-for-byte gates

The tier-contract document SHALL explicitly classify each gate's strength as one of **smoke**,
**canonical-semantic**, or **byte-for-byte**, and SHALL state which `evidence.type` values map to each
strength so a reader cannot conflate a smoke check with byte-for-byte proof.

#### Scenario: Gate strengths are named and mapped
- **WHEN** the document describes the evidence a tier accepts
- **THEN** it labels that evidence as smoke, canonical-semantic, or byte-for-byte
- **AND** it states that smoke evidence alone cannot satisfy a P0 data-loss scenario without an explicit recorded gap

### Requirement: A release checklist gates public parity claims

The project SHALL maintain a release checklist (`docs/development/parity-release-checklist.md`) that
blocks broad public parity claims unless all required gates are demonstrably green. The checklist MUST
require: manifest lint green; `required_parity` green on the release commit; a recent `nightly_docker`
pass; a recent `exhaustive_regeneration` pass for release candidates; and the absence of any unqualified
"same tests as Cassandra" claim. It MUST link the Cassandra test index, the assessment report, and the
generated parity report.

#### Scenario: Checklist enumerates the required green gates
- **WHEN** a release manager opens the checklist before publishing parity claims
- **THEN** the checklist lists each required gate (manifest lint, required_parity on the release commit, recent nightly_docker, recent exhaustive_regeneration for RCs) as an explicit check item
- **AND** it includes a check that forbids unqualified "same tests as Cassandra" claims

#### Scenario: Checklist links the evidence sources
- **WHEN** a reviewer follows the checklist
- **THEN** it links to `docs/cassandra_test_index.md`, the assessment report, and the generated parity report

### Requirement: CI validates manifest tier names against the documented enum

CI SHALL fail when a tier name used in `test-data/cassandra-parity-manifest.yml` is not part of the
documented tier enum, and SHALL fail when the documented tier set, the manifest schema enum, and the
`cassandra-parity` code enum disagree. This check MUST run in a fast PR path and MUST NOT require Docker,
live Cassandra, or downloaded dataset assets.

#### Scenario: Unknown tier name fails the gate
- **WHEN** a manifest scenario declares a `ci.tier` value not present in the documented enum
- **THEN** the `cassandra-parity` check exits non-zero and names the offending scenario ID and the invalid tier value

#### Scenario: Doc and code enum drift fails the gate
- **WHEN** the tier set listed in `docs/development/parity-ci-tiers.md` differs from `enums::CI_TIER` or the manifest schema enum
- **THEN** the cross-check exits non-zero and reports which tier is present in one source but missing in another

#### Scenario: The check runs without heavy dependencies
- **WHEN** the tier-name validation runs in the fast PR CI path
- **THEN** it completes without Docker, live Cassandra, or downloaded dataset binaries

### Requirement: A dedicated scheduled workflow implements the nightly_docker tier

The project SHALL provide a single dedicated CI workflow that implements the `nightly_docker` tier as a
scheduled, live-Cassandra-backed lane. The workflow MUST be triggered on a schedule (not per-PR) and MUST
also support manual dispatch. It MUST stand up the pinned Cassandra version by reusing the existing
`test-data/scripts` Docker machinery (compose stack and/or pinned-source bootstrap) rather than forking
the version pin. It MUST run the `nightly_docker`-tier payload — live read-back (loader/refresh
validation), BTI (`da`) sstabledump parity, and differential compaction logical parity — together in one
lane that the release checklist's "recent `nightly_docker` pass" item can cite.

#### Scenario: Lane runs on a schedule and on manual dispatch
- **WHEN** the nightly schedule fires or a maintainer manually dispatches the workflow
- **THEN** the dedicated `nightly_docker` workflow runs
- **AND** it does not run automatically on ordinary pull requests

#### Scenario: Lane reuses the existing Cassandra Docker machinery
- **WHEN** the lane stands up live Cassandra
- **THEN** it invokes the existing `test-data/scripts` machinery (the Cassandra 5.0.2 compose stack and/or the pinned-source bootstrap) rather than defining a new, separately-pinned Cassandra setup
- **AND** the Cassandra version used is the same pin the committed parity corpus was generated against

#### Scenario: Lane runs the live read-back, BTI, and differential compaction payload together
- **WHEN** the lane executes
- **THEN** it runs the live read-back (loader/refresh) validation, the BTI (`da`) sstabledump parity check, and the differential compaction logical-parity check within the single workflow run
- **AND** the run is citable as one "recent `nightly_docker` pass" for the release checklist

### Requirement: The lane includes a Bloom false-positive-rate check with a hard no-false-negative gate

The lane SHALL include a Bloom-filter check that deserializes real Cassandra `Filter.db` fixtures. For
every key Cassandra wrote, the check MUST assert the filter reports "maybe present"; a single false
negative MUST hard-fail the lane (this is a P0 data-loss property and is never advisory). The check MUST
additionally compute the measured false-positive rate against a deterministic absent-key sample and
report it compared to the configured `bloom_filter_fp_chance`. Whether a measured-FPR threshold breach
hard-fails or is advisory MUST be a configured policy that the lane documents; the no-false-negative
property MUST hard-fail regardless of that policy.

#### Scenario: A false negative hard-fails the lane
- **WHEN** the Bloom check finds any key that Cassandra wrote for which the deserialized `Filter.db` reports "not present"
- **THEN** the lane fails (hard failure), independent of any FPR-threshold policy

#### Scenario: Measured FPR is reported against the configured fp_chance
- **WHEN** the Bloom check completes its absent-key probing
- **THEN** the lane reports the measured false-positive rate per fixture alongside the configured `bloom_filter_fp_chance`
- **AND** the report states whether the FPR threshold is currently advisory or hard-failing

### Requirement: The nightly report distinguishes hard failures from advisory byte-tier gaps

The lane SHALL publish a report (a CI step summary and an uploaded artifact) that, for every leg it runs,
records whether the leg is **hard-fail** or **advisory** and its outcome. The lane MUST fail the workflow
if and only if at least one hard-fail leg fails; advisory legs (e.g. the differential compaction byte
tier, and the statistical FPR threshold while it remains advisory) MUST NOT, on their own, fail the
workflow. The report MUST include a Bloom FPR summary section.

#### Scenario: Advisory byte-tier gap does not fail the lane
- **WHEN** an advisory leg (e.g. the compaction byte tier) diverges but every hard-fail leg passes
- **THEN** the lane's workflow outcome is success
- **AND** the report marks that leg as advisory with its divergence recorded

#### Scenario: A hard-fail leg fails the lane
- **WHEN** any hard-fail leg fails (no-false-negative Bloom, BTI `da` logical parity, differential compaction logical parity, or live read-back semantic equivalence)
- **THEN** the lane's workflow outcome is failure
- **AND** the report marks that leg as a hard failure

#### Scenario: The report separates the two classes
- **WHEN** a reader opens the nightly report
- **THEN** each leg is labeled hard-fail or advisory with its outcome
- **AND** the report contains a Bloom FPR summary section

### Requirement: The lane retains failure diagnostics and reproduction commands

On a failing run the lane SHALL upload diagnostics sufficient to triage and reproduce the failure,
retained for at least the `nightly_docker` tier's retention window. The uploaded set MUST include
Cassandra container logs, CQLite logs, fixture metadata (the pinned Cassandra version and git SHA),
per-scenario JSONL diffs, the Bloom FPR summary, and a reproduction-commands block.

#### Scenario: Failure artifacts are uploaded with reproduction commands
- **WHEN** the lane fails
- **THEN** it uploads Cassandra logs, CQLite logs, fixture metadata (Cassandra version + git SHA), per-scenario JSONL diffs, and the Bloom FPR summary
- **AND** the artifacts include the exact commands to reproduce the lane locally

### Requirement: The manifest binds nightly_docker scenarios to the dedicated lane

The Cassandra parity manifest SHALL name the dedicated nightly lane as the `ci.workflow` for the
`nightly_docker`-tier scenarios it carries (live read-back, BTI `da`, differential compaction byte tier),
and the statistical Bloom FPR scenario SHALL be promoted out of `manual_debug` into the lane with the
documented gating posture. The generated parity report (`docs/reports/cassandra-test-parity.md`) MUST be
regenerated so it reflects the new owning workflow, and the manifest MUST continue to pass
`cassandra-parity lint` and `tier-contract-check`.

#### Scenario: Tagged nightly scenarios name the dedicated lane
- **WHEN** a `nightly_docker` scenario backed by the dedicated lane is inspected in the manifest
- **THEN** its `ci.workflow` points at the dedicated nightly Docker parity workflow

#### Scenario: The Bloom FPR scenario is promoted into the lane
- **WHEN** the statistical Bloom FPR manifest scenario is inspected
- **THEN** its `ci.tier` is `nightly_docker` (not `manual_debug`) and it names the dedicated lane as its `ci.workflow`

#### Scenario: Manifest stays lint-clean and the report is regenerated
- **WHEN** `cassandra-parity lint`, `cassandra-parity tier-contract-check`, and `cassandra-parity report --check` run after the wiring
- **THEN** all exit zero and the generated report reflects the dedicated lane as the owning workflow

