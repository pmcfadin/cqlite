## Why

The bindings (`bindings/python` PyO3, `bindings/node` napi-rs) are the surface most users touch, and
both are shipped built with the **workspace release profile, which sets `panic = "abort"`**
(`Cargo.toml:156-160`). PyO3 and napi-rs each ship a `catch_unwind`→exception firewall at the FFI
boundary that turns a Rust panic into a catchable `CqliteError` — but that firewall is **dead code under
abort**: unwinding never happens, so the firewall never runs. A panic raised inside `cqlite-core` while
a scan runs through a binding's `block_on(...)` therefore **aborts the whole host process** (kills the
Python interpreter / Node process) instead of raising an exception. The parser audit
(`docs/reports/bindings-ffi-performance-audit-2026-07-01.md`) confirmed core has real corrupt-input
panic paths (ZigZag lengths, BTI byte-depth cap), so a user pointing either binding at a
corrupt/truncated SSTable can have their process killed outright today.

`[profile.dev]` (`Cargo.toml:169-179`, `panic = "unwind"` at `:177`) is why debug builds don't
reproduce the abort — but wheels and npm prebuilds are built `--release`
(`.github/workflows/python-release.yml:96`; `bindings/node/package.json:29`), so the shipped artifacts
abort. There is **no binding-level profile override**: the cdylibs inherit `panic = "abort"` from the
workspace release profile.

**Decision (DECIDED 2026-07-01, owner-approved):** ship the two binding cdylib artifacts (wheels + npm
prebuilds) built `panic = "unwind"` so the PyO3/napi `catch_unwind` firewall re-activates and a core
panic becomes a catchable `CqliteError`. `panic = "abort"` stays for the CLI/core.

- **Milestone:** Release: bug-clear (pre-v0.13) — P0 abort-safety. **Design-driven** (build-profile
  architecture + a fail-closed CI-artifact guard; touches release workflows and build config — latitude
  in the profile mechanism and where the guard lives). No Cassandra SSTable format oracle is decoded here.
- Adds a new `binding-panic-firewall` capability.

## What Changes

- **New `release-unwind` Cargo profile.** Add a workspace profile that `inherits = "release"` and flips
  only `panic = "unwind"` (keeping `lto`, `codegen-units = 1`, `strip`). `panic` is a *profile* setting
  — the whole artifact + its dependency graph must compile with one strategy, so a per-package override
  is impossible; a named profile is the supported mechanism.
- **Repoint the two binding builds to `release-unwind`.** Python wheels: maturin `--profile
  release-unwind` (replacing `--release`) in `.github/workflows/python-release.yml`. Node prebuilds:
  the cargo `release-unwind` profile in `bindings/node/package.json` build script (and
  `.github/workflows/node-release.yml` if it invokes the build differently).
- **Fail-closed CI-artifact guard.** Add a check that FAILS if a shipped binding artifact would be
  built with abort — i.e. asserts the binding build definitions carry `--profile release-unwind` and do
  NOT use `--release`. This guard runs locally (agent-gate reachable) so a regression to abort is caught
  before shipping, not in production.
- **Recorded size/perf delta.** Build each binding artifact both ways and record (a) cdylib byte size
  and (b) a representative scan micro-benchmark (abort vs unwind) in the PR description.

## Non-goals

- **Does NOT build the abort-safety harness.** `bindings/python/tests/test_abort_safety.py` and
  `bindings/node/__test__/abort-safety.test.js` are owned by **#1437** (HELD behind this issue). This
  change makes that harness pass; it does not create it.
- **Does NOT change `[profile.release]`.** CLI and core stay `panic = "abort"`.
- **Does NOT eliminate the underlying core panic paths.** Parser panic elimination (epic H,
  `#1438`/`#1439` and the parser audit) is complementary and out of scope here — the firewall converts
  panics to exceptions; it does not remove them.
- **No WASM binding profile** (M6, not yet shipped).

## Doctrine impact

None required in CLAUDE.md prose; the invariant "binding cdylibs ship `panic = "unwind"`; CLI/core ship
`panic = "abort"`" is enforced by the fail-closed guard (the guard IS the doctrine, in executable form).
