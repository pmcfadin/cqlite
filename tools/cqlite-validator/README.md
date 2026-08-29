# `cqlite-validator` — manual dev tool (NOT CI-wired)

**Status: manual developer tool.** No CI workflow, no script, and no live doc invokes this crate
or its `cqlite-validator` binary — verified by census in issue #1716 (epic #1688, audit finding
AK5). It is therefore **not** in the workspace `default-members`, so a bare `cargo build` at the
repo root does not compile it.

```bash
cargo run -p cqlite-validator -- --help     # build + run on demand
cargo build -p cqlite-validator             # build only
cargo test  -p cqlite-validator             # this crate's own tests (it currently has none)
```

It is still a workspace **member**, so the agent gate's
`cargo clippy --workspace --all-targets --all-features` lints it under `-D warnings` — dropping out
of `default-members` costs build time, never lint coverage.

## What it does

A standalone SSTable validation CLI: `test` (self-check), `file <path>` (validate one component)
and `dir <path>` (walk a Cassandra data directory). It predates the `cqlite` CLI's own validation
surface. The archived quick-start that used to drive it is
`docs/archive/user-guides-superseded/UAT_QUICK_START.md` — archived, not live.

## Before you delete it

Retained deliberately: issue #1716 permits deletion only for a tool that **duplicates a live gate
check entirely**, and this one is not wired into any gate lane, so there is nothing to compare it
against. Batch-deletion of dead tooling is epic #1688 decision #9 — take it there, not here.
