# Tasks

## 1. Add the `release-unwind` profile
- [ ] 1.1 In workspace `Cargo.toml`, after `[profile.release]`, add `[profile.release-unwind]` with
      `inherits = "release"` and `panic = "unwind"` (no other keys). Surface exercised: `Cargo.toml`
      profile table. Leave `[profile.release]` `panic = "abort"` untouched.

## 2. Repoint the binding builds
- [ ] 2.1 Python wheels: change `.github/workflows/python-release.yml` maturin-action `args` from
      `--release --out dist` to `--profile release-unwind --out dist`. Verify
      `bindings/python/pyproject.toml` `[tool.maturin]` does not re-pin `--release`.
- [ ] 2.2 Node prebuilds: change `bindings/node/package.json` `build` script to build with the
      `release-unwind` cargo profile (napi `--profile release-unwind`, or the version-appropriate
      `--cargo-flags`/`CARGO_PROFILE` mechanism per design D3). Update
      `.github/workflows/node-release.yml` if it drives the build differently.

## 3. Fail-closed guard (binding-panic-firewall)
- [ ] 3.1 Add a deterministic, offline guard (script under `scripts/tests/` invoked by the gate, or a
      Rust test) that FAILS if either binding build definition uses `--release` / omits
      `--profile release-unwind`, and fails closed on a missing/unparseable definition. Surface
      exercised: the guard invocation + the four build definitions it reads.
- [ ] 3.2 Wire the guard so `scripts/agent-gate.sh` runs it (a gate component or an existing test target).

## 4. Measure + record the delta
- [ ] 4.1 Build each binding artifact both ways (`--release` vs `--profile release-unwind`); record
      cdylib byte size for each. Surface: `ls -l` the `.so`/`.node`/wheel.
- [ ] 4.2 Run a representative scan micro-benchmark abort vs unwind (reuse an existing bindings perf
      test). Record both numbers in the PR description.

## 5. Quality gates (definition of done)
- [ ] 5.1 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 5.2 `RUSTFLAGS="-D warnings"` clean; no new `unwrap()`/`expect()` in library code.
- [ ] 5.3 Spec-auditor (C) PASS against `openspec/changes/panic-unwind-profile/specs/**` — every
      requirement satisfied with a public-surface/guard test as evidence.
- [ ] 5.4 roborev clean (`--base origin/main --agent codex`).
- [ ] 5.5 File-size ratchet respected (no touched file grows past threshold without ack).

## Notes
- Do NOT build the abort-safety harness (`test_abort_safety.py` / `abort-safety.test.js`) — owned by
  #1437, HELD behind this issue. This change makes that harness pass; it does not create it.
- Merge is HELD only in the reverse direction (#1437 waits on this); this change (#1440) merges on green.
