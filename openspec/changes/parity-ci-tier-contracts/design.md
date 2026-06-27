# Design: parity-ci-tier-contracts

## Context

The parity program already has the *mechanism* for tiers: the manifest schema enumerates
`fast_pr | required_parity | nightly_docker | exhaustive_regeneration | manual_debug`, the
`cassandra-parity` linter validates `ci.tier` against `enums::CI_TIER`, and several workflows
(`cassandra-parity.yml`, `compaction-parity.yml`, `tombstone-ttl-parity.yml`, …) implement individual
gates. What is missing is the *contract*: a public definition of what each tier promises. Downstream
epic-#974 issues (#1023 claim lint, #1024 gate hardening, #1025 nightly Docker, #1026 exhaustive regen)
need this contract as their reference, so it lands first. The change is documentation + a small linter
cross-check; it adds no parity tests and changes no tier values.

## Goals / Non-Goals

**Goals:**
- One authoritative doc per the spec's four requirements: tier contracts, gate-strength classification,
  release checklist, and a CI cross-check that keeps doc ↔ schema ↔ code enums in agreement.
- The cross-check runs in fast PR CI with zero heavy dependencies (no Docker/datasets/live Cassandra).
- Discoverable from existing doctrine (CLAUDE.md + website gate-contract page).

**Non-Goals:**
- No new parity tests; no fixing of parity failures; no GitHub releases.
- No change to the five tier enum values themselves.

## Decisions

**D1. Two separate docs, not one.** `parity-ci-tiers.md` (the contract) and
`parity-release-checklist.md` (the gate) serve different readers and change cadences — the contract is
stable reference; the checklist is run per release. *Alternative considered:* one combined doc — rejected
because the checklist would be buried and harder to copy into a release issue.

**D2. The documented enum is sourced from a machine-readable list, not free prose.** The tier doc carries
a fenced, parseable tier list (e.g. a small table or code block) that the cross-check reads, so "the
documented enum" is a real artifact the linter can diff against `enums::CI_TIER` and the schema's enum —
satisfying the spec's drift scenario. *Alternative considered:* validating only manifest-vs-code enum
(the linter already does this) — rejected because #1022 explicitly requires validating against the
*documented* enum, so the doc must be a checkable source.

**D3. Cross-check lives in `cassandra-parity` as a new subcommand (e.g. `tier-contract-check`), wired
into a fast-PR workflow step.** Reuses the existing tool, its enum constants, and its no-Docker/no-dataset
property. *Alternative considered:* a standalone shell/CI grep — rejected as brittle and untestable.

**D4. Gate-strength taxonomy maps to existing `evidence.type` values.** smoke ↔ `smoke`;
canonical-semantic ↔ `canonical_semantic`; byte-for-byte ↔ `byte_for_byte`; `partial`/`out_of_scope` are
documented as non-proving. This anchors the taxonomy to the schema already in use rather than inventing
new labels.

## Risks / Trade-offs

- **[Doc/enum drift despite the check]** → the cross-check is the mitigation; it fails CI on any
  divergence between doc, schema, and `enums::CI_TIER`, and is itself unit-tested with a passing fixture
  and a drifted fixture.
- **[Parser brittleness reading the doc enum]** → keep the documented enum in a single, strictly-formatted
  block (table or fenced list) with a stable shape; the check errors clearly if it cannot locate/parse it.
- **[Scope creep into actually enforcing the release checklist in CI]** → out of scope; the checklist is a
  human gate doc here. Automated claim-lint enforcement is #1023.

## Migration Plan

Additive only — new docs, a new linter subcommand, a new fast-PR CI step, and cross-links. No rollback
concern; reverting the change removes the docs/check with no runtime impact on CQLite itself.

## Open Questions

- None blocking. (Whether the checklist later becomes machine-enforced is tracked by #1023, not here.)
