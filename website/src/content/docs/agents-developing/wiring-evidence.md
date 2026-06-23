---
title: Wiring Evidence
description: A feature is done when the intended public surface exercises it — not when a helper passes unit tests in isolation. How to name the public surface and prove the call chain reaches it. (Issues #949/#963)
sidebar:
  label: Wiring evidence
  order: 6
---

A capability is **done** when the intended USER-FACING surface exercises it — not when a
low-level helper passes unit tests in isolation. Issue #949 shipped a partition-lookup
helper with green tests while the public `SELECT` path never called it. The unit tests
were honest; the feature was inert. Issue #963 codifies the fix: every feature/perf issue
must name its public surface and prove the call chain reaches the new code.

## The rule

**Green unit tests for a helper are not sufficient when the issue names a user-visible
behavior. Prove the public surface exercises the new code.**

A reviewer must be able to answer "yes" to all three:

1. **What is the public surface?** One of: CQL `execute` (one-shot / `--query`), streaming
   API, prepared statements / bind params, a CLI subcommand or flag, REPL/TUI, or a
   language binding (Python/Node).
2. **What is the call chain?** The path from that surface down to the new code, named
   explicitly — e.g. `SELECT execute → QueryEngine::plan → SSTableReader::seek_partition
   → new helper`.
3. **Where is the end-to-end test?** A test that drives the public surface (not the
   helper) and asserts the new behavior is observable.

## Public surfaces in CQLite

| Surface | Entry point |
|---------|-------------|
| CQL one-shot | `cqlite --query "SELECT ..."` / `--execute` |
| Streaming | `execute_streaming` / streaming iterators |
| Prepared / bind | `execute_with_params`, prepared statements |
| CLI subcommands | `delta-export`, `export-sstable`, write subcommands, etc. |
| REPL / TUI | `cqlite repl`, `cqlite tui` |
| Python bindings | `db.execute(...)`, `db.export_parquet(...)` |
| Node.js bindings | `db.executeNative(...)`, `db.executeStreaming(...)` |

See [Key source paths](/cqlite/agents-developing/source-map/) for where each surface is wired.

## Fallback paths

Many optimizations have a fallback (full scan when a fast path can't apply). When a
fallback exists, the e2e test must **distinguish the new path from the fallback** —
otherwise a passing test proves nothing about the new code. Use one of:

- **Work counters** — assert partitions/rows/blocks read dropped (the fast path did less work).
- **Output difference** — the new path returns a result the old path could not.
- **Explicit assertion** — instrument the fallback so the test can assert it did NOT run.

A faster helper that the public path never calls does NOT close a perf issue.

## Intentionally internal or feature-flagged work

Some code is deliberately not yet user-reachable — staged behind a feature flag, or an
internal building block for a later issue. That is fine, but it must be **declared**, not
silently shipped as if it were a finished feature:

- Say so in the PR's *Public Surface & Wiring Evidence* section.
- Name the feature flag (if any) and document it in the feature-flags table.
- Link the follow-up issue that will wire the capability to a public surface.
- Expect `scripts/audit-inert-surfaces.sh` to flag it; explain the findings rather than
  hiding them.

An undeclared stub is a process failure; a declared, tracked, flagged building block is
normal staged work.

## For reviewers: reject helper-only implementations

When an issue asks for **user-visible behavior**, a PR that only adds and unit-tests a
helper is **incomplete** — request changes. Specifically, reject (or send back) a PR when:

- The issue names a public surface but no test drives that surface.
- The only tests call the helper directly; nothing proves the surface reaches it.
- A new public method validates inputs then returns a default/empty/`None` (a
  validation-only shell) with no path to real behavior.
- A perf claim rests on a microbenchmark of a helper, with no evidence the public path
  takes the faster route (no work-counter or surface-level bench delta).
- `_params` / bind values are accepted by a public method and then ignored.

The author's options are: wire the surface and add the e2e test, OR explicitly reclassify
the work as intentionally internal / feature-flagged (see above) with a tracking issue.
"Unit tests are green" is not a rebuttal.

## Audit helper

`scripts/audit-inert-surfaces.sh` is a lightweight, advisory grep for the textual tells of
an unwired surface: `TODO: Implement`, `unimplemented!()`, ignored `_params`, `#[ignore]`
tests, and validation-only empty returns. It is a HEURISTIC, not a gate — exit code `2`
means "review these," not "this failed." Real verification is the end-to-end test.

```bash
scripts/audit-inert-surfaces.sh            # scan all tracked Rust sources
scripts/audit-inert-surfaces.sh --diff     # only files changed vs origin/main
scripts/audit-inert-surfaces.sh cqlite-core/src   # scan a specific path
```

For each hit, confirm the public surface reaches the new code and an e2e test exercises
it, or declare the surface intentionally internal.

## Checklist

Before claiming a feature/perf issue is done:

- [ ] Named the intended public surface(s).
- [ ] Wrote the call chain from surface → new code.
- [ ] Added an end-to-end test that drives the public surface and asserts the behavior.
- [ ] If a fallback exists, the test distinguishes the new path from the fallback.
- [ ] No new public API is a stub / placeholder / validation-only shell.
- [ ] Ran `scripts/audit-inert-surfaces.sh`; explained or wired any findings.
- [ ] Ran the full gate ([Gate contract](/cqlite/agents-developing/gate-contract/)) and pasted its summary block.
