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

