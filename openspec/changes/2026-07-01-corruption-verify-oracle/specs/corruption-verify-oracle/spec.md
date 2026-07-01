# Spec: corruption-verify-oracle (core)

## ADDED Requirements

### Requirement: Full-fixture-directory verdict binding

The corruption corpus generator SHALL bind each captured Cassandra verdict to a deterministic hash of the
**entire** fixture directory, in addition to the mutated-component hash, so that a change to any component
in the directory invalidates the binding rather than only a change to the mutated component.

#### Scenario: Non-mutated component drift invalidates the binding

- **GIVEN** a fixture directory whose captured verdict is bound to `verdict_captured_for_dir_sha256`
- **WHEN** any component file in that directory changes (including a copied, non-mutated component)
- **THEN** the recomputed full-directory hash no longer matches `verdict_captured_for_dir_sha256`
- **AND** the parity test fails fatally with a re-capture instruction, rather than silently trusting a
  stale verdict.

#### Scenario: Manifest records both bindings

- **GIVEN** the generated `corruption-manifest.yml`
- **WHEN** a fixture entry is emitted
- **THEN** it carries both `verdict_captured_for_dir_sha256` (whole directory) and
  `verdict_captured_for_sha256` (mutated component).

### Requirement: MODE-1 corruption-verify parity test (fatal, fail-closed)

The parity test `cqlite-core/tests/sstable_parity_corruption_verify.rs` SHALL assert that CQLite
`verify_sstable` produces the same verdict as the captured Cassandra `sstableverify` verdict for every
corruption class, and SHALL validate the full-directory binding for byte-reproducible fixtures. The test
SHALL fail closed when the fixture set is empty or absent.

#### Scenario: CQLite verdict matches captured Cassandra verdict per corruption class

- **GIVEN** a fixture with a captured Cassandra `sstableverify` verdict for its corruption class
- **WHEN** CQLite `verify_sstable` runs against that fixture directory
- **THEN** CQLite's verdict (detected / not-detected and error class) matches the captured verdict.

#### Scenario: Byte-stable fixture tree matches its bound directory hash

- **GIVEN** a fixture marked `verdict_byte_stable: yes`
- **WHEN** the parity test recomputes the full-directory hash
- **THEN** it equals `verdict_captured_for_dir_sha256`; any mismatch is a fatal failure.

#### Scenario: Empty or absent fixture set fails closed

- **GIVEN** no corruption fixtures are present on disk (e.g. datasets not fetched)
- **WHEN** the parity test runs
- **THEN** it fails (does not silently pass on zero fixtures).

### Requirement: PR-time committed-binding guard is not carried in the corruption-parity PR lane

The `compression-corruption-parity.yml` PR lane SHALL NOT run a pre-regeneration committed-binding guard,
because the corruption `.db` binaries are gitignored and absent on a pull_request, making such a guard
structurally unable to validate non-byte-reproducible fixtures without false-failing. Enforcement of
committed-vs-generated oracle drift is deferred to the nightly regeneration lane (issue #1373, epic #1360).

#### Scenario: Corruption-parity PR checks are green on the retained core

- **GIVEN** the retained core (full-dir binding + MODE-1 parity test) with the PR-time guard steps removed
- **WHEN** the corruption-parity and dependency-isolation PR checks run
- **THEN** they pass, and no step false-fails on the two non-byte-reproducible BTI fixtures.
