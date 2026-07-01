# Tasks: parity-artifact-retention

> Implementation tasks for the LATER implement phase. Each task names the surface it exercises.
> Do NOT start these during the proposal/design phase. Owner OPEN QUESTIONS in `design.md` must be
> resolved (retention durations, presence-gating, migration strategy, repro-bundle weight, live-log
> scope) before the schema/lint tasks are finalized.

## 1. Schema + record model

- [x] 1.1 Add `test-data/parity-failure-artifact.schema.json` (the failure-artifact record schema).
      Surface: schema file + a schema-validation unit test in `tools/cassandra-parity`.
- [x] 1.2 Add a Rust model + emitter for the record in `tools/cassandra-parity` (or a shared lib the
      Rust parity tests can call). Surface: `cassandra-parity` library API + round-trip unit test
      (emit → validate against 1.1's schema).
- [x] 1.3 Define the `diffs[].kind` enum (`byte_diff`/`offset_diff`/`checksum_diff`/`jsonl_diff`/
      `component_inventory`/`live_log`/`audit_report`) in `enums.rs` next to `CI_TIER`. Surface: enum
      cross-check test mirroring the existing tier-enum cross-check.

## 2. Bundle layout + emission from the Rust required_parity checks

- [x] 2.1 Emit a scenario-id-keyed bundle (`parity-failures/<tier>/<scenario_id>/`) from the Rust
      byte/offset/checksum/JSONL parity checks on failure. Surface: `cqlite-core` parity test harness
      helper (`cqlite-core/tests/parity_bundle/mod.rs`, `FailureBundle`, wrapping the Wave 1
      `cassandra_parity::failure_artifact` emitter); an integration test that forces a mismatch and
      asserts the bundle + record exist
      (`cqlite-core/tests/issue_1027_parity_failure_bundle.rs::byte_mismatch_emits_scenario_id_keyed_bundle`,
      `::emitted_record_validates_against_wave1_schema`).
- [x] 2.2 Byte_for_byte diffs: write per-component `byte-diff.txt`, `offset-diff.txt`, `checksums.txt`,
      `component_inventory.txt`. Surface: `FailureBundle::byte_for_byte_component` + body formatters;
      evidence test `::byte_mismatch_emits_all_four_diff_kinds` (all four files + matching `diffs[]`
      entries whose paths resolve inside the bundle).
- [x] 2.3 Canonical_semantic diffs: write `jsonl.diff` + raw `reference.jsonl` + `candidate.jsonl`.
      Surface: `FailureBundle::jsonl`; evidence test `::jsonl_mismatch_emits_jsonl_diff_and_raw_sources`
      (three files + `jsonl_diff` entry).
- [x] 2.4 Repro bundle: write `repro/command.sh`, `repro/INSTRUCTIONS.md`, `repro/inputs/` (paths +
      dataset SHA256, no full dataset copy). Surface: shared emitter (`write_repro`); evidence test
      `::repro_bundle_names_command_and_fixture_inputs` (`repro_bundle` resolves; inputs names fixture
      path + dataset SHA256; asserts no dataset copy). Passing-writes-no-bundle covered by
      `::passing_scenario_writes_no_bundle`.

## 3. Compaction (Java harness) alignment

- [ ] 3.1 Map the existing `compaction-parity/build/parity-artifacts-<task>/<Class>.<method>/` bundle
      onto the shared `parity-failures/<tier>/<scenario_id>/` layout + emit `failure-artifact.json`.
      Surface: `compaction-parity` Gradle harness + a harness test that a forced byte mismatch produces
      the conforming bundle.
- [ ] 3.2 Live-cell compaction lane (`live-cell-compaction-parity.yml`) emits a `live_log` diff entry.
      Surface: that workflow's harness output.

## 4. Manifest descriptors + lint

- [x] 4.1 Extend `test-data/cassandra-parity-manifest.schema.json`: define the `artifact.<tier>.<kind>`
      descriptor family (per the design table) and migrate `evidence.failure_artifacts` per the owner's
      chosen migration strategy. Surface: manifest schema + `cassandra-parity lint`.
- [x] 4.2 `cassandra-parity lint`: descriptor `<tier>` must equal `ci.tier`; `<kind>` must be valid for
      `evidence_type`. Surface: `lint.rs` + lint unit tests for the mismatch + valid cases.
- [x] 4.3 Convert the existing `failure_artifacts` free-text entries in
      `test-data/cassandra-parity-manifest.yml` to typed descriptors (scope per owner OPEN QUESTION 3).
      Surface: the manifest YAML; `cassandra-parity lint` green afterward.

## 5. Retention policy doc + enforcement

- [x] 5.1 Promote the tier-contract §"Artifact retention" bullets into a single enforced retention
      table in `docs/development/parity-ci-tiers.md` (durations per owner OPEN QUESTION 1). Surface: the
      doc + the tier-contract-check if it can cover the table.
- [x] 5.2 Add a retention check (in `cassandra-parity` or a workflow-lint step) that parses each parity
      workflow's `upload-artifact` `retention-days` against its tier minimum. Surface: the check +
      a unit test over a below-minimum and an at-minimum fixture workflow.

## 6. Wire the existing workflows

- [x] 6.1 `sstabledump-parity-gate.yml`: upload `parity-failures/**` as `parity-failures-sstabledump-parity-gate`,
      `if: always()`, retention 14 (required_parity minimum). Surface: the workflow.
- [x] 6.2 `compaction-parity.yml`: upload `parity-failures-compaction-parity` from the scenario-id tree,
      retention 30 (lane also gates nightly_docker); also raised the existing
      `compaction-parity-reports` upload 14→30. Surface: the workflow.
- [x] 6.3 `compression-corruption-parity.yml` (90, exhaustive_regeneration), `cql-type-parity.yml` (30),
      `tombstone-ttl-parity.yml` (30): shared `parity-failures-<basename>` upload, `if: always()`.
      Also added `live-cell-compaction-parity.yml` (14, required_parity). Surface: each workflow.
- [x] 6.4 `exhaustive-regeneration.yml`: added a shared `parity-failures-exhaustive-regeneration`
      upload (audit_report bundle) at 90-day retention alongside the existing report artifact.
- [x] 6.5 Nightly Docker lanes (`cassandra-validation.yml`, `e2e-readback.yml`): raised retention
      14→30 and added the shared `parity-failures-<basename>` upload (live_log bundle), retention 30.
      Wired `retention-check` into CI in `cassandra-parity.yml` (fail-closed enforcement).

## 7. Docs + cross-links

- [x] 7.1 Added `docs/development/parity-failure-artifacts.md` (record schema, bundle layout,
      per-evidence-type contents, descriptor family, upload+retention) and cross-linked it from
      `docs/development/parity-ci-tiers.md`. The `agents-developing/` website mirror is maintained
      separately (issue #1022) and points back to this canonical page.
- [x] 7.2 Referenced failure bundles by scenario id in `docs/development/parity-release-checklist.md`
      (a near-release red-gate triage section cites `parity-failures/<tier>/<scenario_id>/`).

## 8. Closing gates (the standard quality bar)

- [ ] 8.1 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 8.2 `openspec validate parity-artifact-retention --strict` clean (already required to merge).
- [ ] 8.3 spec-auditor (C) PASS — every requirement `satisfied` with a public-surface test as evidence.
- [ ] 8.4 roborev clean (`--base origin/main`).
