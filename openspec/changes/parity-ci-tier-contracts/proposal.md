# Proposal: parity-ci-tier-contracts

> Milestone: Cassandra Byte-for-Byte Parity Program (epic #966 → #974 Public Claims and CI Gates).
> Issue: #1022. Routing: **design-driven** (process/contract + docs; no new parity tests, no oracle bytes) → OpenSpec.

## Why

The Cassandra parity program already enumerates five CI tiers (`fast_pr`, `required_parity`,
`nightly_docker`, `exhaustive_regeneration`, `manual_debug`) in the manifest schema and the
`cassandra-parity` linter, but **nowhere defines what each tier promises** — which evidence types it
accepts, when it may skip, how it fails, what artifacts it must preserve, and how a scenario is promoted
to a stronger tier. As a result "parity" claims are not anchored to a gate contract, and there is no
release gate stopping a broad public parity claim from shipping on a commit where the required gates were
never green. This is the contract layer the rest of epic #974 (#1023 claim lint, #1024 gate hardening,
#1025 nightly Docker, #1026 exhaustive regen) builds on, so it must land first.

## What Changes

- **New source-of-truth doc** `docs/development/parity-ci-tiers.md` defining, for each of the five tiers:
  the tier's purpose, allowed `evidence.type` values, skip policy, failure policy, artifact retention
  expectations, and promotion rules (when/how a scenario moves to a stronger tier). It explicitly
  distinguishes **smoke**, **canonical-semantic**, and **byte-for-byte** gates.
- **New release checklist** `docs/development/parity-release-checklist.md` that blocks broad public
  parity claims unless: manifest lint is green, `required_parity` is green on the release commit, a recent
  `nightly_docker` pass exists, a recent `exhaustive_regeneration` pass exists for release candidates, and
  there are no unqualified "same tests as Cassandra" claims. It links the Cassandra test index, the
  assessment report, and the generated parity report.
- **CI cross-check**: a `cassandra-parity` lint rule + a fast-PR CI step that validates the tier names
  used in the manifest against the **documented** enum (doc ↔ schema ↔ `enums::CI_TIER` must agree), so
  the doc cannot silently drift from the code enum.
- **Doctrine update**: cross-link the new tier contract from `CLAUDE.md` and the website
  `agents-developing/` gate-contract page so the gate contract and the parity tiers are discoverable
  together (same-change doctrine rule).

Non-goals (see Out of scope below) — no new parity tests, no parity-failure fixes, no GitHub releases.

## Capabilities

### New Capabilities
- `parity-ci-tiers`: The public contract for each Cassandra parity CI tier (evidence types, skip/failure
  policy, artifact retention, promotion rules) and the release checklist that gates public parity claims,
  plus the doc↔enum validation that keeps the contract honest.

### Modified Capabilities
<!-- None: this introduces a new contract capability; it does not change requirements of an existing spec. -->

## Impact

- **Docs (new)**: `docs/development/parity-ci-tiers.md`, `docs/development/parity-release-checklist.md`.
- **Tooling**: `tools/cassandra-parity` (linter) — add a doc-enum cross-check command/rule; existing
  `lint` enum check stays.
- **CI**: a fast-PR workflow step invoking the cross-check (no Docker, no datasets, no live Cassandra).
- **Doctrine**: `CLAUDE.md` + website `agents-developing/` gate-contract page get a cross-link.
- **Out of scope**: adding new parity tests; fixing parity failures found by gates; creating GitHub
  releases; changing the tier enum values themselves.
