## Why

The July 2026 platform observability audit
(`docs/reports/platform-observability-audit-2026-07-01.md`, finding **AI6**, epic
#1686 "observability honesty") found that `cqlite-core` emits log events through
**two different facades**:

- **`log`** — ~600 event call sites (`log::warn!`, `log::info!`, `log::debug!`,
  …) across ~70 source files, including the **issue-#586 "never silently swallow"
  corruption warning**.
- **`tracing`** — spans (already the standard for structured span instrumentation).

The two look interchangeable but are not. A library consumer who installs **only
a `tracing` subscriber** — the modern default — and does **not** also wire the
`tracing-log` `LogTracer` bridge **silently loses every `log` event**, including
the corruption warning that issue #586 explicitly promised would never be
swallowed. The failure is invisible: no panic, no error, just missing diagnostics
at the exact moment (a corrupt SSTable) they matter most.

**Routing: design-driven.** This is a crate-wide facade/doctrine decision with
public-observability consequences for embedders, not an oracle-driven
parse-correctness fix, so it is captured as an OpenSpec change per the
spec-driven doctrine. The change **encodes a doctrine** ("one event facade:
`tracing`") that reviewers (including roborev) can then enforce.

Milestone: **0.14**. Priority: **P0** (a promised-visible corruption warning is
silently droppable today). Epic: #1686 (AI6).

## What Changes

- **Adopt one event facade: `tracing`.** Migrate every `log::{warn,info,debug,error,trace}!`
  event site in `cqlite-core/src` to the `tracing::` equivalent (a mechanical,
  message-preserving sweep). Spans are already `tracing` and are untouched.
- **Add a grep-guard test** so the mix cannot creep back: a test asserting zero
  `log::{warn,info,debug,error,trace}!` event macros remain in `cqlite-core/src`.
  It FAILS on today's tree (proving the migration is real).
- **Add a bridge-less capture test**: emit the #586-class corruption warning and
  assert a **`tracing`-only subscriber with NO `LogTracer` installed** receives
  it. This FAILS on `main` (the event is dropped today) and passes after the
  migration — the acceptance oracle for the whole change.
- **Drop the `log` dependency from `cqlite-core`** once no event site remains
  (verified by build), OR document precisely why it must stay (e.g. a transitive
  re-export) if it cannot be removed.

### Explicitly out of scope (do NOT)

- **Do not migrate spans** — they are already `tracing`.
- **Do not change any message CONTENT** — the data-safe message wording is owned
  by AG5 (#1694, already landed). This change is facade-only: `log::warn!(X)` →
  `tracing::warn!(X)` with X byte-identical.
- **Do not touch the CLI's fmt layer** — the CLI keeps its subscriber; only the
  core crate's emission facade changes.
