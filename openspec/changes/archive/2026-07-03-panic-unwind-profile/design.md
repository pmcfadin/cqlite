## Context

`panic` is a Cargo *profile* setting, not a per-package one: `std` is monomorphized per panic strategy,
so an entire final artifact and its whole dependency graph must be compiled with one strategy. You
cannot mix abort and unwind in one binary. Today the workspace `[profile.release]` sets
`panic = "abort"` and the two binding cdylibs inherit it, which makes PyO3's/napi-rs's built-in
`catch_unwind`→exception firewall dead code.

## Decisions

### D1 — Mechanism: a named `release-unwind` profile (chosen)
Add `[profile.release-unwind] inherits = "release"; panic = "unwind"` and build the two binding
artifacts with it; CLI/core keep plain `--release`.

- **Beat: edit `[profile.release]` to unwind.** Rejected — would flip CLI/core to unwind too, contrary
  to the owner decision and the DoD ("`[profile.release]` still `panic="abort"`").
- **Beat: a per-target/per-package panic override.** Impossible — Cargo has no per-package `panic`; it
  is profile-scoped.
- **Beat: an env/`RUSTFLAGS=-Cpanic=unwind` override at build time.** Rejected as the primary mechanism
  — brittle, invisible in the build definition, and easy to lose; `RUSTFLAGS` is the documented
  *fallback* (see D3) only if a toolchain rejects `--profile`.

`inherits = "release"` preserves `lto`, `codegen-units = 1`, `strip`; only `panic` changes. This is the
Cargo-supported, minimal-surface mechanism the issue prescribes.

### D2 — Guard: a hermetic build-definition assertion runnable in the gate (chosen)
Add a fail-closed check that reads the binding build definitions (`python-release.yml` maturin args,
`bindings/python/pyproject.toml` `[tool.maturin]`, `bindings/node/package.json` build script,
`node-release.yml`) and fails if any binding build uses `--release` or omits `--profile
release-unwind`. It fails closed when a definition is missing/unparseable.

- **Beat: a workflow-only step.** Rejected as the sole guard — it wouldn't fire until CI on a release
  workflow, long after the regression merged; a locally-runnable guard catches the revert pre-push.
- **Beat: a runtime abort-safety test in this change.** Rejected — that is #1437's harness (explicit
  non-goal here). The end-to-end runtime proof lands when #1437 (HELD behind this) runs against a
  `release-unwind`-built artifact.

Implementation note (for the implementer, not a spec constraint): the guard can be a small script under
`scripts/tests/` invoked by the gate, or a Rust test that reads the definition files — whichever
integrates cleanly with `scripts/agent-gate.sh`. It must be deterministic and offline (no build).

### D3 — napi-rs `--profile` plumbing, with a preserved-invariant fallback
napi-rs v3 (`@napi-rs/cli ^3.5.1`) supports `--profile`. If a given toolchain/version rejects
`--profile release-unwind` for one binding, the invariant to preserve is: **the shipped wheel and npm
prebuild are compiled `panic = "unwind"` while CLI/core stay abort.** Any equivalent mechanism
(`CARGO_PROFILE`, `--cargo-flags "--profile release-unwind"`, or `RUSTFLAGS=-Cpanic=unwind`) that
achieves that — and that the guard in D2 can still verify — is acceptable.

## Risks / tradeoffs

- **Binary size / perf:** unwind adds landing-pad tables → modest size increase and near-zero
  steady-state perf cost. Quantified as a deliverable (recorded in the PR), not assumed.
- **Guard brittleness vs build-file churn:** the guard asserts on build-definition text, so it must
  match the real flag the tool consumes (maturin `--profile`, napi cargo profile). Anchored to the
  specific keys/args, not a broad grep, to avoid false negatives.
