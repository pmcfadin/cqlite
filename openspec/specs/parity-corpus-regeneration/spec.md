# parity-corpus-regeneration Specification

## Purpose
TBD - created by archiving change exhaustive-regeneration-lane. Update Purpose after archive.
## Requirements
### Requirement: The exhaustive regeneration lane runs on manual dispatch and a slow schedule

The project SHALL provide a CI workflow at the `exhaustive_regeneration` tier that is triggerable via
`workflow_dispatch` and also runs on a scheduled cadence suitable for slow, full-corpus generation. The
lane MUST NOT run on ordinary pull requests and MUST NOT gate the fast or required PR paths.

#### Scenario: Lane is manually dispatchable
- **WHEN** a maintainer triggers the lane from the GitHub Actions UI
- **THEN** the workflow accepts a `workflow_dispatch` event and starts a regeneration run
- **AND** no pull-request trigger is configured for the workflow

#### Scenario: Lane runs on a slow schedule
- **WHEN** the configured schedule cadence elapses
- **THEN** the workflow fires automatically on its `schedule:` cron without manual action
- **AND** the cadence is slower than the nightly Docker lane (e.g. weekly), reflecting the cost of full-corpus generation

### Requirement: Each regeneration run records dataset provenance

The lane SHALL record, for every run, the Cassandra version and source ref/git-sha used, the Docker image
tag, the exact generator commands invoked, the produced dataset asset name, and the SHA256 of that asset.
The recorded Cassandra version/ref MUST be checkable against the manifest's pinned `cassandra_source`.

#### Scenario: Provenance fields are captured
- **WHEN** a regeneration run completes
- **THEN** the run's provenance record contains the Cassandra version, source ref/git-sha, Docker image tag, the generator commands invoked, the dataset asset name, and the asset SHA256
- **AND** the provenance record is included in the run's report artifact

#### Scenario: Provenance is comparable to the manifest pin
- **WHEN** the audit reads the run's provenance record
- **THEN** it compares the recorded Cassandra version/ref/git-sha against the manifest's `cassandra_source` (and `evidence.cassandra_version`/`cassandra_git_sha`) pin
- **AND** the audit fails if the regenerated corpus was produced from a Cassandra version/ref that the manifest does not declare

### Requirement: The lane regenerates the full storage-format matrix, test_deltas, and corruption fixtures

The lane SHALL regenerate the Cassandra-generated parity corpus by invoking the existing generation scripts:
the storage-format matrix (`nb`/`oa`/`da`/`big`/`bti`) via `regenerate-datasets.sh`
(`exhaustive.regenerate.all_formats`), the `test_deltas` delete-bearing fixtures via `generate-deltas.sh`
(`exhaustive.regenerate.test_deltas`), and the corruption fixtures via `generate-corruption-corpus.sh`
(`exhaustive.regenerate.corruption_fixtures`).

#### Scenario: Format matrix is regenerated
- **WHEN** the lane runs the `exhaustive.regenerate.all_formats` step
- **THEN** it invokes `regenerate-datasets.sh` to rebuild the `nb`/`oa`/`da` corpus across the storage-format matrix
- **AND** the regenerated dataset directories are available to the audit step

#### Scenario: Delta fixtures are regenerated
- **WHEN** the lane runs the `exhaustive.regenerate.test_deltas` step
- **THEN** it invokes `generate-deltas.sh` to rebuild the `test_deltas` keyspace fixtures

#### Scenario: Corruption fixtures are regenerated
- **WHEN** the lane runs the `exhaustive.regenerate.corruption_fixtures` step
- **THEN** it invokes `generate-corruption-corpus.sh` to rebuild the corruption-fixture corpus

### Requirement: Corruption fixture generation covers every required component type

The corruption-fixture regeneration step SHALL produce at least one corrupted-component fixture for each of:
Data.db, Index.db, Summary.db, Statistics.db, CompressionInfo.db, TOC.txt, and Digest.crc32.

#### Scenario: All seven component types are covered
- **WHEN** the corruption-fixture step completes
- **THEN** the regenerated corruption corpus includes a fixture targeting each of Data.db, Index.db, Summary.db, Statistics.db, CompressionInfo.db, TOC.txt, and Digest.crc32
- **AND** the audit fails if any of those seven component types has no corruption fixture

### Requirement: The audit compares the regenerated component inventory against expected manifest entries

The lane SHALL run a corpus audit (`exhaustive.audit.manifest_coverage` and
`exhaustive.audit.generated_references`) that compares the regenerated component inventory against the
expected entries in `test-data/cassandra-parity-manifest.yml`, reusing the existing `cassandra-parity`
manifest model and high-relevance classifier.

#### Scenario: Inventory is diffed against the manifest
- **WHEN** the audit runs after regeneration
- **THEN** it enumerates the regenerated component inventory and the manifest's expected component/reference entries
- **AND** it reports any divergence between the two as a named finding (component or reference identifier)

### Requirement: The audit fails on missing references, stale references, unclassified high-relevance files, and unexpected component changes

The corpus audit SHALL exit non-zero when any of the following holds: a manifest reference is missing from
the regenerated corpus; a manifest reference is stale (no regenerated component matches it); a
high-relevance Cassandra file from `docs/cassandra_test_index.md` is unclassified in the manifest; or a
regenerated component's presence/checksum inventory changes unexpectedly relative to the expected manifest
entry set.

#### Scenario: Missing reference fails the audit
- **WHEN** the manifest references a fixture path or component that the regenerated corpus does not contain
- **THEN** the audit exits non-zero and names the missing reference

#### Scenario: Stale reference fails the audit
- **WHEN** the manifest references a component that no regenerated corpus file matches
- **THEN** the audit exits non-zero and names the stale reference

#### Scenario: Unclassified high-relevance file fails the audit
- **WHEN** a high-relevance Cassandra test file in `docs/cassandra_test_index.md` is not referenced by any manifest scenario
- **THEN** the audit exits non-zero and names the unclassified high-relevance file

#### Scenario: Unexpected component change fails the audit
- **WHEN** a regenerated component appears, disappears, or has a checksum that diverges from the expected manifest entry set without a corresponding manifest update
- **THEN** the audit exits non-zero and names the unexpected component change

### Requirement: The lane emits a report artifact and never auto-commits regenerated datasets

The lane SHALL upload a single report artifact containing the provenance record, the audit report, and the
generator logs. It MUST NOT commit regenerated dataset binaries back to the repository and MUST NOT publish
the dataset asset to a release.

#### Scenario: Report artifact is uploaded
- **WHEN** a regeneration run finishes (pass or fail)
- **THEN** the lane uploads a report artifact containing the provenance record, audit report, and generator logs

#### Scenario: No auto-commit of regenerated datasets
- **WHEN** the lane regenerates the corpus
- **THEN** the workflow performs no `git commit`/`git push` of regenerated dataset binaries
- **AND** the workflow does not publish the dataset asset to a GitHub release

