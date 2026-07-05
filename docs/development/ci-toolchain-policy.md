# CI toolchain policy (issue #1990)

**Single source of truth for the Rust toolchain is `rust-toolchain.toml`** (currently
`channel = "1.88.0"`). CI must match what contributors build locally. This page records
which workflows honor that pin and the one deliberate exception.

## Why this exists

GitHub's `stable` channel advances continuously (it reached rustc 1.96.1). Two scheduled
lanes had hardcoded `toolchain: stable`, which **overrode** the repo pin and drifted CI onto
a newer compiler than the pin:

- **Compaction Parity** failed building `cqlite-cli --features write-support`: rustc ≥ 1.96
  overflows the default query-depth limit (128) computing the async-future layout of
  `tui::events::run_tui<B>()` — `error: queries overflow the depth limit!`. This is a large
  async event loop, not a real type-recursion bug.
- **Coverage Gate** failed because `cargo install cargo-tarpaulin` **built from source** on the
  drifted toolchain and did not compile.

Both were toolchain drift, not source-logic regressions.

## Policy

1. **Every product-validation lane honors `rust-toolchain.toml`.** How to do that depends on
   the setup action (their semantics differ):
   - **`actions-rust-lang/setup-rust-toolchain@v1`** — **omit** the `toolchain:` input. With no
     input it reads `rust-toolchain.toml`. Passing `toolchain: stable` overrides the pin file
     (this is what broke Compaction Parity).
   - **`./.github/actions/setup-rust-ci`** (local composite) — **omit** `toolchain:`. Its default
     is already the pin (`1.88.0`).
   - **`dtolnay/rust-toolchain`** — this action **cannot** read `rust-toolchain.toml`; it requires
     a toolchain via the `@<ref>` tag or the `toolchain:` input. Pin it **explicitly**:
     `uses: dtolnay/rust-toolchain@1.88.0`. (In-repo `cargo` invocations already prefer the pin
     file over `rustup default`, but the explicit ref avoids installing an unused `stable` and
     pins out-of-repo `cargo install` steps too.)

2. **Coverage tools install as PREBUILT pinned binaries, never `cargo install` from source.**
   Use `taiki-e/install-action@v2` with `tool: cargo-tarpaulin@<version>` (and `cargo-llvm-cov`).
   A prebuilt binary cannot be broken by a toolchain/edition drift and runs against the pinned
   toolchain.

3. **Exactly ONE lane tracks latest `stable`:** the advisory
   [`future-rust-canary.yml`](../../.github/workflows/future-rust-canary.yml). It builds the
   workspace + `cqlite-cli --features write-support` + clippy on latest `stable`, on a **weekly
   schedule + `workflow_dispatch` only** (never `pull_request`/`push`), so it can never be a
   PR-required check. A red run is an early-warning signal to investigate forward-incompat
   before bumping the pin — not a merge blocker.

4. **Source is forward-compatible:** `cqlite-cli`'s crate roots (`src/lib.rs`, `src/main.rs`)
   carry `#![recursion_limit = "256"]` so the crate builds on 1.88.0 **and** 1.96+.

## Lane classification

| Category | Toolchain | Workflows |
|----------|-----------|-----------|
| Honors pin (omit `toolchain:` → reads `rust-toolchain.toml`) | 1.88.0 | `compaction-parity`, `observability-gate`, `cassandra-validation`, `perf-regression`, `e2e-readback`, `ci-minimal-features`, `coverage-baseline`; `sstabledump-parity-gate`, `pr-gate` (via `setup-rust-ci` default) |
| Honors pin (explicit `@1.88.0` — dtolnay can't read the pin file) | 1.88.0 | `ci`, `gate`, `cassandra-parity`, `compression-corruption-parity`, `cql-type-parity`, `tombstone-ttl-parity`, `smoke-tests`, `delta-roundtrip`, `flight-ci`, `live-cell-compaction-parity`, `quality-gates`, `exhaustive-regeneration`, `docs-site`, `node-ci`, `python-ci`, `coverage` |
| Already pinned (pre-existing) | 1.88.0 | `nightly-docker-parity` |
| **Advisory stable canary (the ONE exception)** | latest `stable` | `future-rust-canary` |
| Nightly toolchain (legitimately needs nightly) | `nightly` | `fuzz` |
| Release / publish artifacts (deliberately track `stable`; out of scope) | `stable` | `release`, `api-docs`, `python-release`, `node-release` |

Release/publish lanes build shipping artifacts and intentionally use `stable` (some `rm -f
rust-toolchain.toml`); they are **not** product-validation lanes. If we later want release
artifacts pinned too, that is a separate change.

## Bumping the pin — lockstep checklist

Several places hardcode the pinned version as a **literal** and MUST be bumped in lockstep
with `rust-toolchain.toml`, or CI silently drifts from the pin again. Exact files to touch
on the next pin bump:

1. `rust-toolchain.toml` — update `channel` (the source of truth).
2. `.github/actions/setup-rust-ci/action.yml` — the composite action hardcodes `1.88.0` as
   its `toolchain` input **default**, which its `rustup toolchain install` + `rustup
   default` step consumes. Every workflow that calls it without a `toolchain:` input
   (e.g. `sstabledump-parity-gate.yml`, `pr-gate.yml`) inherits this default — bump it.
3. Every literal `dtolnay/rust-toolchain@<old>` ref — grep
   `dtolnay/rust-toolchain@` under `.github/workflows/` and bump each `@1.88.0`
   (these action refs cannot read the pin file; see policy rule 1).
4. `nightly-docker-parity.yml` — passes an explicit `toolchain: 1.88.0` to `setup-rust-ci`.
5. Re-check the prebuilt coverage-tool pins (`cargo-tarpaulin@<ver>`,
   `cargo-llvm-cov@<ver>` in `coverage.yml` / `coverage-baseline.yml`) still run on the
   new toolchain.

Lanes that **omit** `toolchain:` (`actions-rust-lang/setup-rust-toolchain@v1` callers)
follow the pin file automatically — no action needed for those.

Watch `future-rust-canary` beforehand — it is the early-warning lane for the next stable.
(No CI check enforces this lockstep today; this checklist is the doc-only guard, per the
lead's scope call on #1990.)
