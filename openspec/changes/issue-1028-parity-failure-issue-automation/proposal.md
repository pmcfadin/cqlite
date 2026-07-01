## Why

CQLite runs many manifest-backed Cassandra parity lanes — `compression-corruption-parity`, `cql-type-parity`,
`tombstone-ttl-parity`, `live-cell-compaction-parity`, `compaction-parity`, the `sstabledump-parity-gate`,
and `exhaustive-regeneration` (epic #974 / parity-ci-tiers doctrine). When one of these **fails on a
scheduled or main-branch run** (not a PR), there is **no automation to turn that failure into a tracked
issue**: the only existing `github-script` steps merely *comment on the triggering PR*, and the parity-ci-tiers
doctrine explicitly promises that a `nightly_docker` failure "files/updates a tracking issue and blocks
release until resolved" — a promise nothing currently keeps. Failures therefore rely on a human noticing a
red scheduled run, and repeated failures of the same scenario produce no deduplicated signal.

This change adds a GitHub Actions automation that converts recurring parity-CI failures into **deduplicated
GitHub issues**, grouped by a **stable failure fingerprint** (manifest scenario ID + workflow + test target +
fixture/component path + normalized failure class). It reuses the repo's proven dedup idiom (an HTML-comment
marker in the issue body + a label-scoped `gh issue list` lookup, exactly as `scripts/delivery-telemetry.py
retro` and the `<!-- MGR:… -->` / `<!-- RETRO:… -->` markers already do) and the `project-board-sync.yml`
token-guard idiom (absent token → `::notice::` no-op, never reds CI).

- **Milestone:** maintenance / parity-program (epic #974). **Design-driven** — there is no Cassandra
  byte-oracle here; the fingerprint algorithm, dedup mechanism, tier-policy, no-auto-close policy, and
  manifest modeling all have real latitude. Hence OpenSpec + Seam 1.
- Adds a new `parity-failure-automation` capability and new manifest scenarios under the existing
  `cli_reporting` capability.
- **No impact** on the no-heuristics mandate (no SSTable decoding), no new library/binding surface, no memory
  budget impact. It is CI tooling.

## What Changes

- **New workflow `.github/workflows/parity-failure-issue.yml`.** Triggered by `workflow_run` completion of the
  scheduled/main parity lanes (and `workflow_dispatch`). On a failed run it downloads the lane's failure
  artifacts, computes a stable fingerprint per failing scenario, and **creates or updates** a deduplicated
  tracking issue. It runs as a **separate, non-gating** job: issue filing never turns a failing parity run
  green, and never masks a red one.
- **Structured failure-summary artifact.** The parity lanes emit a small machine-readable
  `parity-failures.json` (scenario ID, workflow, test target, component/fixture path, normalized failure
  class) so the fingerprint is computed from structured fields rather than scraped Markdown. Where a lane
  cannot yet emit it, the automation falls back to a documented degraded parse and says so in the run summary
  (never silently fingerprints nothing).
- **Dedup by body-marker + label.** Each tracking issue carries `<!-- PARITY-FAIL:<fingerprint> -->` in its
  body and a `parity-failure` label; the automation lists open `parity-failure` issues and updates the
  matching one (new failure summary comment + latest-run link) instead of opening a duplicate.
- **Five new manifest scenarios** under `cli_reporting` recording this automation as mirrored tooling
  (`evidence.type: smoke`, `risk: tooling_only`), modeled on the existing
  `cass.cli_reporting.parity_manifest_lint_and_report` precedent; report regenerated; lint green.
- **Doctrine:** the parity-ci-tiers page's "nightly_docker failure files/updates a tracking issue" line gains
  a real implementation reference.

## Non-goals

- **Not fixing any underlying parity failure** — this is signal routing only.
- **No auto-close.** Per doctrine, an issue is never auto-closed without a separately-designed green-run
  confirmation policy. A subsequent green run posts a "resolved on run …" comment (and may de-emphasize the
  marker), but closing stays manual / out of scope here.
- **PR lanes stay comment-only.** Fork and same-repo PR parity failures continue to comment on the PR (existing
  behavior); they do NOT file issues (avoids one issue per PR). Only scheduled/main (non-PR) lane failures file.
- No replacement of human triage/severity assignment; no severity inference.

## Impact

- New capability spec `parity-failure-automation`; new `.github/workflows/parity-failure-issue.yml`; a small
  failure-summary emitter wired into the parity lanes; 5 new `cli_reporting` manifest scenarios + regenerated
  report; a doctrine cross-reference.
- Risk class: `tooling_only` — no correctness/data path touched. Validation is `smoke` (the workflow's
  fingerprint + dedup logic is unit-tested via injected fixtures, mirroring `delivery-telemetry.py`'s
  `--open-issues-json` testability).

## Decisions for the owner (recommended defaults baked into the spec — confirm or adjust at approval)

1. **Scenario IDs** → `cass.cli_reporting.failure_issue_{required_parity,nightly_docker,exhaustive_regeneration,bloom_fpr,dedup}` (fits the `cass.…` ID regex + existing `cli_reporting` enum; the issue's `automation.failure_issue.*` names would violate the schema). *Recommended: yes, no schema change.*
2. **Fingerprint** → SHA-256 over normalized `scenario_id|workflow|test_target|component_path|failure_class`, with the parity lanes emitting `parity-failures.json`. *Recommended.*
3. **Dedup** → body-marker `<!-- PARITY-FAIL:<fingerprint> -->` + `parity-failure` label (the repo's proven RETRO/MGR pattern). *Recommended over per-fingerprint labels (which proliferate).*
4. **Which tiers file** → scheduled/main `nightly_docker` + `exhaustive_regeneration` file/update issues; `required_parity` already blocks the build and PR lanes comment-only. *Recommended.*
5. **No-auto-close** → green run comments "resolved", never closes automatically. *Recommended.*
6. **Token** → built-in `GITHUB_TOKEN` + `permissions: {issues: write, contents: read}` with the board-sync absent-token guard. *Recommended (no new secret needed).*
