# Design: exhaustive-regeneration-lane

## Context

The parity program already has the *pieces* of exhaustive regeneration but never assembles them into a
tier:

- **Generators** exist as standalone scripts: `test-data/scripts/regenerate-datasets.sh` reproduces the
  `nb`/`oa`/`da` keyspaces (the storage-format matrix, `cassandra:5.0.2` image), `generate-deltas.sh`
  builds the `test_deltas` delete-bearing fixtures, `generate-corruption-corpus.sh` builds the
  single-byte-mutation corruption fixtures (Data.db, Index.db, Summary.db, Statistics.db,
  CompressionInfo.db, TOC.txt, Digest.crc32), and the `generate-*-parity.sh` family covers compression/
  tombstone/cql-type/write-load/compaction.
- **Provenance fields** already live in the manifest: `cassandra_source.{repo,ref,sha}` and per-scenario
  `evidence.{cassandra_version,cassandra_git_sha,artifacts}`.
- **Audit logic** already exists: `tools/cassandra-parity` has `coverage` (high-relevance classification
  → `unclassified-high` errors under `--strict`, reading `docs/cassandra_test_index.md`), `lint`,
  `report`, and `tier-contract-check`. The manifest model (`model.rs`) and the format/schema are stable.
- **The tier contract** (`docs/development/parity-ci-tiers.md` §`exhaustive_regeneration`) already promises
  this lane's behavior (RC-wide regeneration, `byte_for_byte`/`canonical_semantic` evidence, >= 90-day
  retention, terminal tier).

What is missing is (a) a workflow that *runs* the generators on a slow cadence, (b) a record of each
run's provenance, and (c) an audit that diffs the regenerated inventory against the manifest. The change
is CI + a small audit subcommand + doctrine cross-links; it adds no oracle bytes and changes no parser.

## Goals / Non-Goals

**Goals:**
- A `workflow_dispatch` + scheduled lane that regenerates all three corpus families (format matrix,
  `test_deltas`, corruption fixtures) and runs the audit, emitting one report artifact.
- A per-run provenance record (Cassandra version/ref, Docker image, generator commands, asset name,
  SHA256) that the audit can compare against the manifest's declared `cassandra_source`.
- A corpus audit that fails on missing references, stale references, unclassified high-relevance files,
  and unexpected component changes.
- The lane never commits regenerated datasets and never blocks a PR.

**Non-Goals:**
- Blocking PRs; reviewing every byte diff by hand; publishing dataset assets (the three out-of-scope items).
- Rewriting the generator scripts or the manifest schema.

## Decisions

**D1. Reuse the existing generator scripts; do NOT build a monolithic new generator.**
The lane is a thin workflow that invokes `regenerate-datasets.sh` (format matrix), `generate-deltas.sh`
(`test_deltas`), and `generate-corruption-corpus.sh` (corruption fixtures) in sequence, then runs the
audit. *Alternative considered:* a single new "regenerate-everything" Rust/shell tool that re-implements
generation. *Rejected* — it would duplicate audited, deterministic scripts (the corruption generator has a
documented determinism contract), double the maintenance surface, and risk drift from the scripts the rest
of the program already trusts.

**D2. The audit is a new `cassandra-parity corpus-audit` subcommand reusing `coverage` + the manifest
model — not a new standalone tool and not shell `grep`.**
`corpus-audit` takes the regenerated corpus root + the manifest + the test index and reports the four
failure classes. It reuses `coverage::analyze` for the unclassified-high-relevance check and the existing
`model::Manifest` parse for reference resolution. *Alternative considered:* a bash script doing `find` +
`grep` over the corpus. *Rejected* as brittle and untestable; the Rust tool can be unit-tested with
clean/drifted fixtures the way `tier-contract-check` is (§3 below mirrors that pattern).

**D3. Four audit failure classes have precise, file-level definitions.**
- *Missing reference:* a fixture path/component the manifest references (`scenario.fixtures.datasets`,
  `evidence.artifacts` component files, `cassandra.files`) that is absent from the regenerated corpus.
- *Stale reference:* a manifest reference that no regenerated component matches (the inverse — the manifest
  points at something the fresh corpus no longer produces).
- *Unclassified high-relevance Cassandra file:* a `docs/cassandra_test_index.md` "High-relevance" entry not
  referenced by any manifest scenario (the existing `coverage --strict` failure, re-used verbatim).
- *Unexpected component change:* a regenerated component whose presence/SHA256 inventory diverges from the
  expected manifest entry set (a component appearing/disappearing, or a recorded checksum changing) without
  a corresponding manifest update.

**D4. Provenance is recorded as a structured run record compared against the manifest's `cassandra_source`.**
The lane writes a small JSON/YAML provenance block (Cassandra version + ref + git-sha, Docker image tag,
the exact generator commands run, the `package_datasets.sh` asset name, and the asset's SHA256) into the
report artifact. The audit asserts the recorded Cassandra version/ref matches the manifest's pinned
`cassandra_source.{ref,sha}` (and `evidence.cassandra_version`), so a silent version bump is caught.
*Alternative considered:* relying on workflow logs only. *Rejected* — logs are not a durable, diffable
record and cannot be asserted by the audit.

**D5. Scheduled cadence = weekly (not nightly), plus `workflow_dispatch`.**
Full corpus regeneration across the format matrix + deltas + corruption + a Cassandra container build is
the program's most expensive job; the tier contract calls it "the full, expensive regeneration … run for
release candidates," distinct from the *nightly* Docker lane (#1025). A weekly cron (e.g. `cron: '0 6 * * 0'`)
keeps a "recent exhaustive_regeneration pass" fresh for the release checklist without paying nightly cost,
and `workflow_dispatch` lets an RC trigger it on demand. *Alternative considered:* nightly — *rejected* as
redundant with #1025's nightly Docker lane and wastefully expensive. (Final cron value is an owner question
below.)

**D6. Report-artifact only; no auto-commit, no publish.**
The lane uploads one artifact (provenance record + audit report + generator logs) via `actions/upload-artifact`
and stops. It does not `git commit` regenerated `*.db` binaries (they are gitignored anyway) and does not
call `publish_datasets.sh`. *Alternative considered:* auto-committing refreshed JSONL goldens. *Rejected* —
silently committing regenerated fixtures would bypass review and is explicitly out of scope; a human opens a
follow-up PR if the audit shows the corpus should change.

## Risks / Trade-offs

- **[Expensive/slow run could be chronically skipped]** → the tier contract already warns that a chronically
  skipped exhaustive run invalidates the "recent pass" release requirement; the weekly cron + the
  release-checklist gate are the mitigation, not this change's job to enforce per-run.
- **[Audit false-positives on legitimate corpus growth]** → "unexpected component change" is defined against
  the *manifest's expected entry set*, so an intended new fixture lands with its manifest entry in the same
  PR; the audit only fires when the corpus and manifest disagree.
- **[Non-determinism in generators causing spurious SHA diffs]** → the corruption generator has a documented
  determinism contract; `regenerate-datasets.sh`/`generate-deltas.sh` use fixed row counts and single-flush
  SSTables. The audit compares *inventory + presence + manifest-recorded checksums*, not a brittle full-asset
  SHA equality, so timestamp/UUID churn in non-pinned fixtures does not red the lane.
- **[Audit tool drift from manifest schema]** → `corpus-audit` reuses the existing `model::Manifest` parser
  and `coverage` classifier, so it tracks the schema the rest of the tool already enforces.

## Migration Plan

Additive only — a new workflow, a new `cassandra-parity` subcommand + its unit tests, a provenance record
format, and doctrine cross-links. Reverting removes the lane and subcommand with no runtime impact on CQLite
itself (the generators and manifest are unchanged).

## Open Questions — RESOLVED (owner, 2026-06-29, Seam 1)

- **Cron cadence value.** RESOLVED → **weekly `cron: '0 6 * * 0'`** (Sun 06:00 UTC) + `workflow_dispatch`.
- **Provenance record format/location.** RESOLVED → **artifact only, JSON**. The provenance block is a JSON
  document inside the uploaded report artifact; it is NOT written to a tracked repo path. The release
  checklist cites the artifact's run URL.
- **Audit strictness on RC vs scheduled runs.** RESOLVED → **hard-fail always**. An "unexpected component
  change" is a hard non-zero exit on every run (scheduled + dispatch); a legitimate corpus change must land
  via a manifest-update PR to turn the lane green. No RC-only / report-but-pass mode.
