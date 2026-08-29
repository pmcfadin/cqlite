# `memory-safety-runner` — manual dev tool (NOT CI-wired, and currently NOT EXECUTABLE)

**Status: manual developer tool.** No CI workflow, no script, and no live doc references this
crate — verified by census in issue #1716 (epic #1688, audit finding AK5).

## Read this first: there is no binary

This crate is **library-only**. It declares no `[[bin]]` target and no other crate depends on it,
so — unlike the other manual tools in `tools/` — **`cargo run -p memory-safety-runner` does not
work**, and nothing in the repository can currently execute `MemorySafetyRunner`. It is reachable
only by its own unit tests, or by code you write against it:

```bash
cargo test  -p memory-safety-runner    # its 2 unit tests
cargo build -p memory-safety-runner    # type-check / lint the library
```

To actually run the wrapped tools you would have to add a `[[bin]]` here (or call the library from
a test). That is deliberately left undone — see "Before you delete it" below.

A bare `cargo build` at the repo root does not compile this crate — but **not because of
anything #1716 changed**: this workspace has a root package (`cqlite`), so cargo's default member
set is *that package alone*, and every `tools/` crate is compiled only by an explicit
`--workspace` or `-p`. See the `default-members` note in the root `Cargo.toml` before "optimizing"
that.

It is a workspace **member**, and stays one, so the agent gate's
`cargo clippy --workspace --all-targets --all-features` lints it under `-D warnings`. Being
unwired costs it no lint coverage.

## What it does

Wraps three external memory-safety checkers behind one `MemorySafetyRunner` type: **Miri**,
**Valgrind** and **AddressSanitizer**, each probed for availability first
(`run_miri_tests`, `run_valgrind_tests`, `run_asan_tests`, `run_stress_tests`,
`run_all_available_tests`, `print_available_tools`). Std-only, no dependencies.

## Before you delete it

Retained deliberately, and **not** wired into the gate on purpose. Issue #1716 allows wiring a tool
in only when it checks something no existing lane does — this one qualifies on subject matter
(nothing in CQLite's stable gate runs Miri/Valgrind/ASan) but **Miri and ASan require a nightly
toolchain**, which `docs/development/ci-toolchain-policy.md` keeps out of the stable gate (the same
reason `fuzz/` is its own excluded workspace). Deletion is permitted by #1716 only for a tool that
duplicates a live gate check entirely, which this does not. Both the "wire it up" and the "delete
it" calls belong to epic #1688 (decision #9) — take them there, not here.
