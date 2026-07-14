# Adopt the round-N standard metrics template

## Why

The AWS field team runs a validation round every build handoff and posts the verdict on the round
tracker (#2367). Round-over-round comparability is currently ad hoc: each report picks its own metric
set, so a regression can hide in a metric that one round measured and the next omitted. In their
round-9 report (#2367, section "Proposed standard metrics") the field proposed a **14-point
per-round reporting standard**, grouped by what each metric protects, and offered to formalize it
("Happy to formalize as a checklist template if useful"). The round-10 build comment already says
"#2399 template welcome".

This change formalizes that 14-point standard as **committed doctrine** — a single canonical template
with each metric classified as a hard pass/fail **gate** item (A/B: correctness + hang/liveness) or a
**tracked number** (C/D: throughput + hygiene), pre-filled with the round-9 baseline so the next round
is directly comparable. Where a report item is cheap to mirror in-repo, the template links the existing
local pin (so a regression that class already covers can't ship silently between field rounds) and
names the single new local mirror this change adds.

## What changes

- **Canonical template artifact** — a committed doc under `docs/development/` enumerating all 14
  field-proposed metrics grouped A/B/C/D, each explicitly marked `GATE` (pass/fail, blocks the round
  verdict) or `TRACKED` (recorded number, non-blocking), with the round-9 measured baseline pre-filled
  per metric for comparability.
- **Round-tracker entry point** — a lightweight GitHub issue template
  (`.github/ISSUE_TEMPLATE/round-tracker.yml`) that seeds a new round tracker from the canonical doc so
  each round starts pre-populated (the doc stays the source of truth; the issue template references it).
- **Local mirrors, where cheap** — the template cross-links the metrics that already have in-repo pins
  (B5 index-parses-delta: #2370 + #2383 + #2385 suites; B7 cancellation-reclaim: #2383 cancel pins) and
  adds **one new local mirror**: a D12 snapshot-leak assertion in the testbed E2E (no leaked `cqlite-`
  snapshots remain after a run). C9 loadtest gating is delegated to #2377 (driver `--gate` mode), which
  the template references rather than re-implements.
- **Offer-back** — post the finalized template on the round-10 tracker (#2367 round channel) for field
  adoption.
- **Doctrine cross-links** — the validation-playbook / pm-operating-loop reference the new template so
  it is discoverable as the round-reporting standard.

## Non-goals

- **Not** re-implementing the B5/B7 pins that already exist — this change references them.
- **Not** building the C9 loadtest `--gate` mode — that is #2377's scope; the template only points at it.
- **Not** changing `scripts/agent-gate.sh` (the pre-PR gate of record) — the field "round gate" (A/B
  pass/fail) is a separate, field-validation concept and does not alter the agent gate contract.
- **Not** fixing the round-9 findings themselves (single-node fan-out #2397, warm-scan setup #2398,
  cold-parse #2385) — the template only standardizes how they are measured and reported.
- **Not** automating the full field round in CI — the round runs against a live 3-node cluster; only the
  cheap, locally-reproducible items get in-repo mirrors.

## Doctrine impact

New reporting doctrine. `docs/development/pm-operating-loop.md` and the validation-playbook page gain a
cross-link to the template. No change to the agent-gate contract, no-heuristics mandate, or the version
floor. Milestone: M7 (perf validation + v1.0) — this is the round-over-round instrument that guards the
v1.0 field-parity claims.
