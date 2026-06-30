# Proposal: exhaustive-regeneration-lane

> Milestone: Cassandra Byte-for-Byte Parity Program (epic #966 → #974 Public Claims and CI Gates).
> Issue: #1026 (child of #974). Routing: **design-driven** (CI/test-infra + audit tooling; no new
> oracle bytes, no parser/compaction fix) → OpenSpec. Sibling: #1025 (nightly Docker parity lane),
> built on the tier contract landed by #1022 (`docs/development/parity-ci-tiers.md`).

## Why

The parity manifest already declares an `exhaustive_regeneration` CI tier and assigns many P0 scenarios
to it (`test-data/cassandra-parity-manifest.yml`), and the tier contract
(`docs/development/parity-ci-tiers.md` §`exhaustive_regeneration`) promises "the full, expensive
regeneration of the entire fixture corpus across the storage-format matrix … run for release candidates."
But **nothing actually runs that tier**. There is no workflow that regenerates the corpus, and there is no
automated audit that the committed corpus + manifest still describe what a fresh Cassandra would produce.
The generation scripts exist in isolation (`regenerate-datasets.sh`, `generate-deltas.sh`,
`generate-corruption-corpus.sh`, and the `generate-*-parity.sh` family) but are never invoked as a tier,
and the corpus-coverage audit (`cassandra-parity coverage`) is not wired to a regeneration run. As a
result the strongest, broadest gate the program advertises is unbacked: a release could cite a "recent
exhaustive_regeneration pass" that never existed (the release checklist, `parity-release-checklist.md`,
requires one for RCs).

This lane is the last CI-gate child of epic #974. It makes the `exhaustive_regeneration` tier real:
a scheduled + manually-dispatchable workflow that regenerates the Cassandra-generated datasets, records
their provenance, and audits the regenerated component inventory against the manifest — emitting a report
artifact, and deliberately **not** committing regenerated binaries.

## What Changes

- **New workflow `.github/workflows/exhaustive-regeneration.yml`** (CI tier `exhaustive_regeneration`),
  triggered by `workflow_dispatch` and a slow scheduled cadence (e.g. weekly cron) — never on PRs.
  It orchestrates the existing generation scripts (`regenerate-datasets.sh` for the `nb`/`oa`/`da`/`big`/`bti`
  format matrix, `generate-deltas.sh` for `test_deltas`, `generate-corruption-corpus.sh` for the
  corruption fixtures), runs the corpus audit, uploads a single **report artifact**, and does **not**
  commit anything back to the repo.
- **New provenance record** captured per run: Cassandra version/ref/git-sha, the Docker image used,
  the exact generator commands invoked, the produced dataset asset name, and the SHA256 of that asset —
  anchored to the manifest's existing `cassandra_source` + `evidence.cassandra_version`/`cassandra_git_sha`
  fields so the run's provenance is comparable to what the manifest claims.
- **New corpus-audit surface** (a `cassandra-parity` subcommand, e.g. `corpus-audit`, reusing the existing
  `coverage` high-relevance classifier and manifest model) that compares the **regenerated component
  inventory** against the **expected manifest entries** and fails on: missing references, stale references,
  unclassified high-relevance Cassandra files, and unexpected component changes.
- **Manifest entries covered/asserted by the lane:** `exhaustive.regenerate.all_formats`,
  `exhaustive.regenerate.test_deltas`, `exhaustive.regenerate.corruption_fixtures`,
  `exhaustive.audit.manifest_coverage`, `exhaustive.audit.generated_references` — each a regeneration or
  audit step the workflow exercises.
- **Doctrine update:** cross-link the lane from the `exhaustive_regeneration` section of
  `docs/development/parity-ci-tiers.md` and note the lane + audit command in `CLAUDE.md` (same-change
  doctrine rule), so the gate that backs the release-checklist requirement is discoverable.

## Capabilities

### New Capabilities
- `parity-corpus-regeneration`: The scheduled/dispatchable lane that regenerates the Cassandra-generated
  parity corpus across the storage-format matrix, records each run's provenance (version/ref, Docker
  image, commands, asset name, SHA256), audits the regenerated inventory against the manifest (missing/
  stale references, unclassified high-relevance files, unexpected component changes), and emits a report
  artifact without auto-committing regenerated datasets.

### Modified Capabilities
<!-- None: this introduces a new lane capability; it does not change requirements of an existing spec. -->

## Impact

- **CI (new)**: `.github/workflows/exhaustive-regeneration.yml` — `workflow_dispatch` + slow `schedule:`,
  uploads report artifact only.
- **Tooling**: `tools/cassandra-parity` — add a `corpus-audit` subcommand reusing `coverage` + the manifest
  model; existing `lint`/`coverage`/`report`/`tier-contract-check` unchanged.
- **Scripts**: orchestrates existing `test-data/scripts/regenerate-datasets.sh`, `generate-deltas.sh`,
  `generate-corruption-corpus.sh`, `package_datasets.sh` (asset name) — no rewrite of generators.
- **Doctrine**: `docs/development/parity-ci-tiers.md` (`exhaustive_regeneration` section) + `CLAUDE.md`
  get a cross-link to the lane and audit command.

## Non-goals (out of scope)

- **Blocking normal PRs.** The lane never runs on PRs and never gates the fast/required PR path; it is
  scheduled + manual only.
- **Manually reviewing every generated byte diff.** The audit asserts inventory/provenance against the
  manifest; it does not require a human to eyeball each byte-level diff of every regenerated component.
- **Publishing dataset assets.** The lane produces and SHA256-stamps a dataset asset and emits a report,
  but does not upload/publish a GitHub release asset (that stays with `publish_datasets.sh` / a release).
