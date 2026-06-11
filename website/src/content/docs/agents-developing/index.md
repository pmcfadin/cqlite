---
title: "For Agents: Developing CQLite"
description: Contributor doctrine, gate contracts, and development workflows for AI agents working on CQLite itself.
sidebar:
  label: Overview
  order: 0
---

# For Agents: Developing CQLite

This section documents the contributor doctrine, gate contracts, and development
workflows for AI agents (and humans) working on CQLite itself.

> **Content arriving in W7.** This placeholder marks the section structure.
> Full pages distilled from `CLAUDE.md`, `.claude/skills/`, and the orchestration
> doctrine (issue #719) will be published as part of issue W7 in epic #733.
> Once W7 lands, `CLAUDE.md` will slim to pointers at these pages.

## What you'll find here (W7 onwards)

- **Gate contract** — what `scripts/agent-gate.sh` checks and what "passing" means
- **No-heuristics mandate** — authoritative metadata only, no guessing (issue #28)
- **Test data fetching** — how to get real SSTable binaries for integration tests
- **Key source paths** — where to find parsers, writers, query engine, bindings
- **sstabledump validation playbook** — how to run and interpret parity checks
- **Orchestration doctrine** — spawn-preflight, inline review, stop-gate patterns
- **Feature flags** — what each flag enables and when to use it
- **Commit and PR conventions** — branch naming, commit message format, PR body

## The gate contract (preview)

Every agent-submitted change must pass `scripts/agent-gate.sh` before opening a PR.
The gate runs:

1. `cargo fmt` — formatting check
2. `cargo clippy -D warnings` — lint check (must be zero warnings)
3. Core tests with `cli-helpers` feature
4. Integration tests
5. Write-support tests
6. CLI tests
7. Minimal-features build (no query engine)
8. Smoke tests

The gate emits a machine-checkable summary block. Paste that block verbatim in
your PR report — "all tests pass" prose does not count.

## Source

This section is built from `CLAUDE.md` and `.claude/skills/` in the
[CQLite repository](https://github.com/pmcfadin/cqlite). The source files are
the authoritative reference until W7 publishes these pages.
