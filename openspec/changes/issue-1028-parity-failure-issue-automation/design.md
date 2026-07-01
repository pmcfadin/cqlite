## Context

There is no existing failure→issue automation in `.github/` (only PR-comment steps), so this is greenfield
tooling. The design reuses two proven repo patterns: the `project-board-sync.yml` token-guard (absent token →
`::notice::` no-op, never reds CI) and the `scripts/delivery-telemetry.py retro` dedup-by-marker idiom
(`<!-- RETRO:cat -->` body markers + label-scoped `gh issue list` + `--open-issues-json` for testability).
The normalized failure-class vocabulary already exists as `VerifyErrorClass` (stable `.code()` strings).

## Decisions

### D1 — Trigger via `workflow_run`, file only for scheduled/main (non-PR) failures
**Chosen:** a single `parity-failure-issue.yml` triggered by `workflow_run: {workflows: [<parity lanes>],
types: [completed]}` + `workflow_dispatch`. It acts only when `workflow_run.conclusion == 'failure'` AND the
run's event was `schedule`/`push`(main)/`workflow_dispatch` — NOT `pull_request`. **Beat:** embedding an
issue-filing step in each lane (duplicated logic across 6+ workflows, and risks gating the lane); and
`pull_request`-triggered filing (one issue per PR = spam — PR lanes already comment on the PR).

### D2 — Structured `parity-failures.json` artifact, with a documented degraded fallback
**Chosen:** the parity lanes emit a small `parity-failures.json` array of `{scenario_id, workflow, test_target,
component_path, failure_class}` (failure_class normalized to the `VerifyErrorClass.code()` vocabulary where
applicable, else a lane-defined stable code). The automation downloads it from the failed run's artifacts.
Where a lane has not yet been wired to emit it, the automation falls back to parsing the lane's
`parity_summary.md`/logs and **records in the run summary that it used the degraded path** (never silently
fingerprints an empty set — zero-failures-parsed on a failed run is itself surfaced). **Beat:** scraping
Markdown as the primary source (brittle, and the fingerprint inputs — scenario↔test-target↔component — are
scattered across manifest + text).

### D3 — Fingerprint = SHA-256 of normalized, ordered fields
**Chosen:** `fingerprint = sha256("v1|" + scenario_id + "|" + workflow + "|" + test_target + "|" +
component_path + "|" + failure_class)`, each field lower-cased and trimmed; a leading `v1|` version tag so the
scheme can evolve without colliding. The first 12 hex chars are used in the marker/title for readability.
**Beat:** hashing the raw log line (unstable across runs — timestamps, paths, counts drift, exactly the
non-determinism that bit #1236's sha-binding).

### D4 — Dedup by body-marker + `parity-failure` label; update, never duplicate
**Chosen:** each tracking issue body contains `<!-- PARITY-FAIL:<fingerprint> -->` and carries the
`parity-failure` label (+ the parent epic `#974` reference + scenario ID + tier + artifact links + repro
command + latest failure summary). The automation runs `gh issue list --label parity-failure --state open
--json number,body --limit 500`, substring-matches the marker, and **updates** the match (adds a dated failure
comment + refreshes the latest-run link) instead of creating a new issue; it warns loudly if the 500 cap is
hit (no silent truncation). **Beat:** a `parity-failure:<fingerprint>` label per fingerprint (label
proliferation; GitHub label limits). This mirrors `delivery-telemetry.py retro` exactly.

### D5 — Non-gating, fail-open, never auto-close
**Chosen:** the job is independent of the parity lane's pass/fail; it only reacts to a completed run's
conclusion. Token absent → `::notice::` + skip, exit 0 (board-sync idiom). A failure to file an issue must not
fail the parity result (and cannot make a red run green). On a subsequent **green** run for a fingerprint, the
automation posts a "resolved on run <url>" comment on the open issue but does **not** close it (doctrine:
no auto-close without a green-run policy). **Beat:** auto-closing on first green (flaky lanes would
close/reopen churn); gating the parity lane on issue-filing success.

### D6 — Model as `cli_reporting` / `tooling_only` mirrored manifest scenarios
**Chosen:** five scenarios `cass.cli_reporting.failure_issue_*` (`status: mirrored`, `capability:
cli_reporting`, `risk: tooling_only`, `cassandra.{category: other, relevance: low, files:["n/a — CQLite-native
CI tooling"]}`, `evidence.{type: smoke, artifacts:[generated_report, logs]}`, `ci.tier: fast_pr`, `scope: {}`),
modeled byte-for-byte on `cass.cli_reporting.parity_manifest_lint_and_report`. **Beat:** the issue's proposed
`automation.failure_issue.*` IDs, which violate the manifest `id` regex `^cass\.…` and would force schema +
`enums.rs` + lint changes for no benefit.

## Risks / Trade-offs

- **`workflow_run` artifact availability** — the automation depends on the failed lane having uploaded its
  failure artifact even on failure (several lanes already `upload-artifact` on failure / `if: always()`); lanes
  that don't get the degraded-parse path + a surfaced note until wired. Tracked in tasks.
- **Fingerprint stability vs over-coalescing** — too-coarse fields merge distinct failures; too-fine spam. The
  v1 field set is a starting point; the `v1|` tag lets it evolve. Surfaced for owner confirmation.
- **Smoke-only validation** — correctness of the fingerprint/dedup logic is unit-tested with injected
  fixtures (`--open-issues-json`-style), not a live failing CI run; the live path is exercised opportunistically
  via `workflow_dispatch`.

## Migration / Rollout

Additive, ships safe without `PROJECTS_TOKEN` (uses `GITHUB_TOKEN`). New workflow + a small JSON emitter in the
lanes + 5 manifest scenarios + a doctrine cross-ref. No API/CLI/binding change.
