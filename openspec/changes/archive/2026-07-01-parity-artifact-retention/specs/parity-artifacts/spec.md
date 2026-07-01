## ADDED Requirements

### Requirement: A uniform failure-artifact record schema exists and is versioned

The project SHALL define a single JSON schema for a parity failure-artifact record
(`failure-artifact.json`) that every strict and live parity surface emits on failure. The record MUST
carry a `schema_version`, the failing manifest `scenario_id`, the emitting `lane`, the manifest `tier`,
the `evidence_type`, the list of `artifacts_compared`, a `provenance` object, a list of `diffs` pointers,
and a `repro_bundle` pointer. The `tier` and `evidence_type` values MUST be drawn from the enums already
defined in `test-data/cassandra-parity-manifest.schema.json`.

#### Scenario: A failure record validates against the schema
- **GIVEN** a `failure-artifact.json` written by any parity surface
- **WHEN** it is validated against the failure-artifact JSON schema
- **THEN** validation passes only if `schema_version`, `scenario_id`, `lane`, `tier`, `evidence_type`, `artifacts_compared`, `provenance`, `diffs`, and `repro_bundle` are all present
- **AND** `tier` is one of the manifest tier enum values and `evidence_type` is one of the manifest evidence-type enum values

#### Scenario: A record missing a required field is rejected
- **GIVEN** a `failure-artifact.json` with `scenario_id` omitted
- **WHEN** it is validated against the schema
- **THEN** validation fails and names the missing `scenario_id` field

### Requirement: The provenance block records the full reproduction context

The `provenance` object in a failure-artifact record SHALL include the Cassandra version, the Cassandra
source ref/git-sha, the dataset SHA256, the fixture path, the component list, the exact command line that
was run, and pointers to captured `stdout` and `stderr`. The Cassandra version/git-sha MUST be comparable
to the manifest's `cassandra_source` pin.

#### Scenario: Provenance fields are captured on failure
- **GIVEN** a `required_parity` byte check fails for scenario `cass.compression_checksum.digest_crc32_byte_for_byte_parity`
- **WHEN** the failure-artifact record is written
- **THEN** its `provenance` contains the Cassandra version, source ref/git-sha, dataset SHA256, fixture path, component list, command line, and `stdout`/`stderr` pointers
- **AND** the recorded Cassandra version/git-sha equals the manifest `cassandra_source` pin for that run

### Requirement: Failure artifacts live in a bundle keyed by manifest scenario id

Each failed scenario SHALL produce a bundle directory at
`<root>/parity-failures/<tier>/<scenario_id>/` containing the `failure-artifact.json` record, the
captured `stdout.txt`/`stderr.txt`, a `diffs/` directory, and a `repro/` directory. The directory name
MUST be the manifest `scenario_id` so a red gate maps mechanically to its `cass.*` scenario.

#### Scenario: A failed scenario writes a scenario-id-keyed bundle
- **GIVEN** scenario `cass.data_db_decode.row_framing_parity` fails in a parity run
- **WHEN** the run finishes
- **THEN** a directory `parity-failures/required_parity/cass.data_db_decode.row_framing_parity/` exists
- **AND** it contains `failure-artifact.json`, `stdout.txt`, `stderr.txt`, a `diffs/` directory, and a `repro/` directory

#### Scenario: A passing scenario writes no failure bundle
- **GIVEN** scenario `cass.data_db_decode.row_framing_parity` passes
- **WHEN** the run finishes
- **THEN** no `parity-failures/required_parity/cass.data_db_decode.row_framing_parity/` directory is produced

### Requirement: Byte-for-byte failures preserve byte, offset, and checksum diffs plus a component inventory

When a `byte_for_byte` scenario fails, its bundle `diffs/` directory SHALL contain, for each compared
component, a byte diff (first differing byte with a hex window), an offset diff, a checksum summary
(SHA-256 per component for both engines), and a component inventory (expected vs actual component set).
The record's `diffs[]` MUST point at each of these, with `kind` values `byte_diff`, `offset_diff`,
`checksum_diff`, and `component_inventory`.

#### Scenario: Byte failure preserves all four diff kinds
- **GIVEN** a `byte_for_byte` Data.db comparison fails for scenario `cass.compaction_merge.differential_compaction_parity`
- **WHEN** the failure bundle is written
- **THEN** `diffs/` contains a per-component byte diff, an offset diff, a `checksums.txt` summary, and a `component_inventory.txt`
- **AND** the record's `diffs[]` includes entries with `kind` `byte_diff`, `offset_diff`, `checksum_diff`, and `component_inventory` whose `path` values resolve inside the bundle

### Requirement: Canonical-semantic failures preserve normalized and raw JSONL

When a `canonical_semantic` scenario fails, its bundle `diffs/` directory SHALL contain the normalized
JSONL diff and BOTH raw source JSONL files (the Cassandra `reference.jsonl` and the CQLite
`candidate.jsonl`). The record's `diffs[]` MUST include an entry with `kind` `jsonl_diff`.

#### Scenario: Canonical-semantic failure preserves normalized diff and raw JSONL
- **GIVEN** a `canonical_semantic` JSONL comparison fails for scenario `cass.data_db_decode.sstable_parity_data_db_jsonl`
- **WHEN** the failure bundle is written
- **THEN** `diffs/` contains a normalized `jsonl.diff`, a raw `reference.jsonl`, and a raw `candidate.jsonl`
- **AND** the record's `diffs[]` includes an entry with `kind` `jsonl_diff`

### Requirement: The reproduction bundle lets a maintainer rerun the failing check

Each failure bundle SHALL contain a `repro/` directory with the exact reproduction command line
(`command.sh`), reproduction `INSTRUCTIONS.md`, and an `inputs/` record identifying the fixture(s) by
path plus dataset SHA256 (it MUST NOT require permanently storing the full regenerated dataset). The
record's `repro_bundle` field MUST point at this directory.

#### Scenario: Repro bundle names the command and fixture inputs
- **GIVEN** any parity failure bundle for scenario `cass.statistics_metadata.statistics_db_strict_parity`
- **WHEN** a maintainer opens `repro/`
- **THEN** it contains `command.sh` with the exact comparison command, an `INSTRUCTIONS.md`, and an `inputs/` record naming the fixture path(s) and dataset SHA256
- **AND** the record's `repro_bundle` resolves to that `repro/` directory

### Requirement: The manifest expresses failure artifacts as typed descriptors

The manifest schema SHALL express a scenario's failure artifacts as typed descriptors of the form
`artifact.<tier>.<kind>` (replacing free-text `evidence.failure_artifacts` strings). The defined
descriptor ids include `artifact.required_parity.byte_diff`, `artifact.required_parity.offset_diff`,
`artifact.required_parity.checksum_diff`, `artifact.nightly_docker.live_logs`,
`artifact.exhaustive_regeneration.audit_report`, and `artifact.manual_debug.reproduction_bundle`. A
descriptor's `<tier>` segment MUST equal the scenario's `ci.tier`, and its `<kind>` MUST be a diff/bundle
kind the scenario's `evidence_type` is allowed to emit.

#### Scenario: Descriptor tier must match the scenario tier
- **GIVEN** a scenario whose `ci.tier` is `required_parity`
- **WHEN** it declares an artifact descriptor `artifact.nightly_docker.live_logs`
- **THEN** `cassandra-parity lint` fails, reporting the tier mismatch between the descriptor and `ci.tier`

#### Scenario: Descriptor kind must match the evidence type
- **GIVEN** a scenario whose `evidence_type` is `canonical_semantic`
- **WHEN** it declares `artifact.required_parity.byte_diff`
- **THEN** `cassandra-parity lint` fails, reporting that a byte-diff descriptor is not valid for a canonical-semantic scenario

#### Scenario: A valid descriptor passes lint
- **GIVEN** a `byte_for_byte` scenario whose `ci.tier` is `required_parity`
- **WHEN** it declares `artifact.required_parity.byte_diff` and `artifact.required_parity.checksum_diff`
- **THEN** `cassandra-parity lint` accepts the descriptors

### Requirement: Retention windows are documented per tier and enforced by tier minimum

The project SHALL document a single retention policy table by tier and enforce it: every parity workflow
that uploads failure artifacts MUST set `retention-days` at or above the minimum for the tier(s) of the
scenarios it gates. The minimums are at least: `required_parity` 14 days, `nightly_docker` 30 days,
`exhaustive_regeneration` 90 days (the exact durations are owner-confirmed policy). A lint/audit check
parses each parity workflow's upload step and fails when its `retention-days` is below its tier minimum.

#### Scenario: A workflow below its tier retention minimum fails the check
- **GIVEN** a `required_parity` parity workflow whose `upload-artifact` step sets `retention-days: 7`
- **WHEN** the retention check runs
- **THEN** it fails, naming the workflow and the required minimum (>= 14 days for `required_parity`)

#### Scenario: A workflow meeting its tier retention minimum passes
- **GIVEN** an `exhaustive_regeneration` workflow whose `upload-artifact` step sets `retention-days: 90`
- **WHEN** the retention check runs
- **THEN** it passes for that workflow

### Requirement: Existing parity workflows upload bundles under the shared layout

The existing parity workflows SHALL upload their failure artifacts as a uniformly-named artifact
(`parity-failures-<workflow>`) whose contents are the `parity-failures/**` bundle tree keyed by scenario
id. This applies to `sstabledump-parity-gate.yml` and `compaction-parity.yml` among others. The upload
MUST occur on failure (`if: always()` or equivalent) so a red run produces the triage bundle.

#### Scenario: sstabledump-parity-gate uploads the shared bundle
- **GIVEN** `sstabledump-parity-gate.yml` runs and at least one `required_parity` scenario fails
- **WHEN** the workflow reaches its upload step
- **THEN** it uploads an artifact named `parity-failures-sstabledump-parity-gate` containing `parity-failures/**`
- **AND** the upload step runs even though the test step failed

#### Scenario: compaction-parity uploads the shared bundle
- **GIVEN** `compaction-parity.yml` runs and a byte-parity scenario fails
- **WHEN** the workflow reaches its upload step
- **THEN** it uploads an artifact named `parity-failures-compaction-parity` containing the scenario-id-keyed `parity-failures/**` tree

### Requirement: All four parity lanes emit conforming records

Every lane in the parity program SHALL emit a `failure-artifact.json` conforming to the schema with the
lane's correct `tier` when a scenario it covers fails. This covers all four lanes: `required_parity`,
`nightly_docker`, `exhaustive_regeneration`, and `manual_debug`. The `nightly_docker` lane MUST include a
`live_log` diff entry, and the `exhaustive_regeneration` lane MUST include an `audit_report` diff entry,
reflecting their manifest descriptors.

#### Scenario: Nightly Docker failure includes a live log
- **GIVEN** the `nightly_docker` lane fails a live comparison
- **WHEN** its failure-artifact record is written
- **THEN** the record's `tier` is `nightly_docker` and its `diffs[]` includes an entry with `kind` `live_log`

#### Scenario: Exhaustive regeneration failure includes the audit report
- **GIVEN** the `exhaustive_regeneration` lane's corpus audit fails
- **WHEN** its failure-artifact record is written
- **THEN** the record's `tier` is `exhaustive_regeneration` and its `diffs[]` includes an entry with `kind` `audit_report`
