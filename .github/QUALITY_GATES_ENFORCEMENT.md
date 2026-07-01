# Quality Gates Enforcement

This document describes the enforcement policy for CQLite quality gates during
the CI runtime overhaul. The detailed tier contract lives in
`docs/ci/ci-tier-policy.md`.

## Enforcement Rules

- Branch protection must require only stable aggregate checks.
- The required PR gate must run on every pull request without path filters.
- Heavy or surface-specific workflows belong in targeted, nightly, or release
  tiers, not global branch protection.
- Required check names must be changed deliberately and documented as a
  migration rename.
- `.github/branch-protection.json` is the applyable source of truth for required
  status checks.
- `.github/setup-branch-protection.js` must load that JSON file instead of
  maintaining a second list.
- Do not change `.github/branch-protection.json` to require a new status until
  the producing workflow exists and has been proven on a PR.

## Required PR Gate

The target globally required status check after #1364 is:

```text
Required PR Gate / required
```

Issue #1364 owns adding `.github/workflows/pr-gate.yml` to produce that status
and updating `.github/branch-protection.json`. Do not apply real GitHub branch
protection requiring this status until the workflow exists and has run at least
once.

The required PR gate must include:

```bash
cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings
```

This Clippy command is a hard gate. A green `cargo test` run is not sufficient
for `cqlite-core` changes.

## Retired Global Requirements

These legacy contexts remain in `.github/branch-protection.json` during Wave 1
so the checked-in config does not require a missing workflow:

- `CI / test`
- `CI: Core Library (minimal) / m1-core-validation`
- `CI: Core Library (minimal) / sstabledump-parity-m1`
- `CI: SSTableDump Parity Gate / sstabledump-parity`

They should be retired as global branch-protection requirements when #1364
lands. They may continue to run as targeted, nightly, or release checks, but
they must not be reintroduced as globally required PR checks after the migration.

## Quality-Gates Workflow

`.github/workflows/quality-gates.yml` is a nightly and manually runnable
coordination workflow for this migration. It must not call missing workflows. If
it later delegates to another workflow, that workflow must exist and expose
`workflow_call`.

The quality-gates workflow may run essential validation such as:

- Cargo metadata checks.
- `cargo check --package cqlite-core --all-features`.
- `cargo fmt --all -- --check`.
- `cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings`.

It is not the globally required PR gate unless it is explicitly changed to meet
the Required PR tier contract and branch protection is updated to its aggregate
status name.

## Nightly And Release Gates

Nightly deep validation is the backstop for reduced PR fan-out. Deep workflows
must expose `workflow_dispatch`, use staggered schedules, upload triage artifacts
with at least 30 days of retention, and write a step summary that names the next
artifact or log to inspect.

Release readiness is enforced by policy, not by branch protection. The release
owner must collect the full release gate evidence listed in
`docs/ci/ci-tier-policy.md`: full parity, ingestion/readback, supported matrices,
coverage, performance, and publish dry-runs. Publish workflows must not be
scheduled, and nightly validation must not publish packages, images, or release
assets.

## Setup

To inspect the checked-in branch protection configuration:

```bash
ruby -e 'require "json"; JSON.parse(File.read(".github/branch-protection.json")); puts "branch protection JSON ok"'
node -e 'const cfg = require("./.github/branch-protection.json"); console.log(cfg.required_status_checks.contexts)'
```

To apply branch protection after the required workflow exists:

```bash
node .github/setup-branch-protection.js
```

Do not use the setup script as part of this policy-only migration unless the
repository is ready to enforce the configured required status.

## Emergency Handling

Quality gate failures should be fixed, not bypassed. If GitHub Actions itself is
unavailable, maintainers may use local validation evidence while waiting for CI
to recover, but branch protection policy must not be weakened in repo config as
a workaround.
