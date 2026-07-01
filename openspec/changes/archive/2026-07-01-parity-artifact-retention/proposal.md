# Proposal: parity-artifact-retention

> Milestone: Cassandra Byte-for-Byte Parity Program (epic #966 → #974 Public Claims and CI Gates).
> Issue: #1027 (child of #974). Routing: **design-driven** (CI/test-infra + manifest/audit tooling;
> no new oracle bytes, no parser/compaction/decode fix) → OpenSpec. Builds on the tier contract
> landed by #1022 (`docs/development/parity-ci-tiers.md`) and the exhaustive-regeneration lane (#1026).

## Why

The parity program already declares *where* failure artifacts should be retained — the manifest's
`evidence.failure_artifacts` field exists and every `required_parity`/`nightly_docker`/
`exhaustive_regeneration` scenario fills it in — and the tier contract
(`docs/development/parity-ci-tiers.md`) already states per-tier retention *recommendations*
(`required_parity` >= 14d, `nightly_docker` >= 30d, `exhaustive_regeneration` >= 90d). But there is
**no consistent, machine-checkable schema for what a failure artifact actually contains, where it
lives, or how it is named**, and the retention recommendations are not enforced anywhere.

Concretely, today:

- `evidence.failure_artifacts` is a free-text `array of strings` in the manifest schema
  (`test-data/cassandra-parity-manifest.schema.json`). Entries are prose ("panic diff: cqlite-recomputed
  Digest.crc32 payload vs Cassandra reference …"), not paths and not a structured record. Two scenarios
  with the same kind of failure describe their artifacts differently.
- Each parity workflow uploads artifacts under a **lane-private** name, path glob, and retention with no
  shared convention: `sstabledump-parity-gate.yml` → `parity-test-results` / 30d;
  `compaction-parity.yml` → `compaction-parity-reports` / 14d;
  `exhaustive-regeneration.yml` → `exhaustive-regeneration-report` / 90d. None is keyed by the manifest
  scenario ID, so given a red gate you cannot mechanically map the uploaded bundle back to the
  `cass.*` scenario that failed.
- The Java compaction harness already produces a **good** per-scenario forensic bundle
  (`compaction-parity/build/parity-artifacts-<task>/<Class>.<method>/` with `inputs/`,
  `cassandra-output/`, `cqlite-output/`, `commands.txt`, stdout/stderr, normalized JSONL,
  `checksums.txt`, `byte-diff.txt`). This is a de-facto template, but it is harness-specific and the
  Rust-side `required_parity` checks (byte/offset/checksum/JSONL panics) do not emit anything
  comparable — a panic message is all you get.
- The retention windows in the tier contract are prose recommendations; no lint or audit asserts that
  a lane's `retention-days` matches its tier, and no doc names the *single* retention policy.

Result: when a parity gate goes red, triage depends on the specific lane's ad-hoc output rather than a
uniform "open the failure bundle for scenario `cass.X.Y`, read the schema'd record." Issue #1027 closes
that gap with (1) a uniform failure-artifact **record schema**, (2) a shared on-disk + upload **layout
keyed by manifest scenario ID**, and (3) a **documented + enforced retention policy by tier**.

## What changes

- Define a uniform **failure-artifact record** (`failure-artifact.json`) schema: lane/tier, manifest
  scenario id, evidence type, artifacts compared, diff-payload pointers, reproduction bundle, and
  provenance (Cassandra version/ref/sha, dataset SHA, fixture path, component list, command line,
  stdout/stderr pointers).
- Define a **shared bundle layout + naming** keyed by manifest scenario id, with per-evidence-type
  required contents (byte_for_byte → byte/offset/checksum diff + component inventory; canonical_semantic
  → normalized JSONL diff + raw source JSONL; smoke → load log; partial → record the gap).
- Wire the existing parity workflows (`sstabledump-parity-gate.yml`, `compaction-parity.yml`,
  `live-cell-compaction-parity.yml`, `compression-corruption-parity.yml`, `exhaustive-regeneration.yml`,
  and the nightly Docker lane) to upload bundles under the shared layout/name.
- Promote `evidence.failure_artifacts` from free-text to a manifest **artifact descriptor** family
  (`artifact.required_parity.byte_diff`, `artifact.required_parity.offset_diff`,
  `artifact.required_parity.checksum_diff`, `artifact.nightly_docker.live_logs`,
  `artifact.exhaustive_regeneration.audit_report`, `artifact.manual_debug.reproduction_bundle`) and
  validate it in `cassandra-parity lint`.
- Document the retention policy as a single source (extend `docs/development/parity-ci-tiers.md`
  retention section into an enforced table) and add a lint/audit check that a lane's `retention-days`
  satisfies its tier minimum.

## Non-goals

- **Not** changing what counts as a parity pass or the gate-strength semantics
  (byte_for_byte / canonical_semantic / smoke / partial are defined by #1022 and stay as-is).
- **Not** adding new parity surfaces, new scenarios, or new Cassandra coverage.
- **Not** fixing any individual parity mismatch (out of scope per the issue).
- **Not** automatic GitHub issue creation on failure, and **not** permanent storage of full regenerated
  datasets (both explicitly out of scope per the issue).
- **Not** changing the manifest's tier enum, capability enum, or evidence-type enum.

## Doctrine impact

- Extends `docs/development/parity-ci-tiers.md` (the §"Artifact retention" bullets become a single
  enforced policy table) and is referenced by `docs/development/parity-release-checklist.md` (a release
  cites failure bundles by scenario id when a near-release gate goes red).
- Mirrors to the agent-developer site beside the gate-contract page (issue #1022 cross-link), so the
  failure-artifact schema is discoverable next to the tier contracts.
- No change to the no-heuristics mandate, the gate contract, or the wiring-evidence doctrine.
