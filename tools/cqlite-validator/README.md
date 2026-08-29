# `cqlite-validator` — manual dev tool (NOT CI-wired)

**Status: manual developer tool.** No CI workflow, no script, and no live doc invokes this crate
or its `cqlite-validator` binary — verified by census in issue #1716 (epic #1688, audit finding
AK5).

```bash
cargo run -p cqlite-validator -- --help     # build + run on demand
cargo build -p cqlite-validator             # build only
cargo test  -p cqlite-validator             # this crate's own tests (it currently has none)
```

A bare `cargo build` at the repo root does not compile this crate — but **not because of
anything #1716 changed**: this workspace has a root package (`cqlite`), so cargo's default member
set is *that package alone*, and every `tools/` crate is compiled only by an explicit
`--workspace` or `-p`. See the `default-members` note in the root `Cargo.toml` before "optimizing"
that.

It is a workspace **member**, and stays one, so the agent gate's
`cargo clippy --workspace --all-targets --all-features` lints it under `-D warnings`. Being
unwired costs it no lint coverage.

## What it does

A standalone SSTable validation CLI: `test` (self-check), `file <path>` (validate one component)
and `dir <path>` (walk a Cassandra data directory). It predates the `cqlite` CLI's own validation
surface. The archived quick-start that used to drive it is
`docs/archive/user-guides-superseded/UAT_QUICK_START.md` — archived, not live.

## Its tests run only when you touch it

No CI job or gate component runs workspace-wide tests, so this crate's own unit tests do not
execute on an unrelated change — but the agent gate's `--lite` blast-radius maps a touched path to
its package, so **editing anything in this directory (this README included) makes `--lite` run
`cargo test -p cqlite-validator --lib`**. Expect latent failures the first time that happens: on #1716,
touching `tools/format-validator/README.md` ran that crate's tests for the first time and one had
never been correct. Run them yourself first with `cargo test -p cqlite-validator`.

## Before you delete it

Retained deliberately: issue #1716 permits deletion only for a tool that **duplicates a live gate
check entirely**, and this one is not wired into any gate lane, so there is nothing to compare it
against. Batch-deletion of dead tooling is epic #1688 decision #9 — take it there, not here.
