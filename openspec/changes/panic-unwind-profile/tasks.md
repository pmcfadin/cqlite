# Tasks

## 1. Add the `release-unwind` profile
- [x] 1.1 In workspace `Cargo.toml`, after `[profile.release]`, add `[profile.release-unwind]` with
      `inherits = "release"` and `panic = "unwind"` (no other keys). Surface exercised: `Cargo.toml`
      profile table. Leave `[profile.release]` `panic = "abort"` untouched.

## 2. Repoint the binding builds
- [x] 2.1 Python wheels: change `.github/workflows/python-release.yml` maturin-action `args` from
      `--release --out dist` to `--profile release-unwind --out dist`. Verify
      `bindings/python/pyproject.toml` `[tool.maturin]` does not re-pin `--release`.
- [x] 2.2 Node prebuilds: change `bindings/node/package.json` `build` script to build with the
      `release-unwind` cargo profile (napi `--profile release-unwind` — confirmed working via the gate's
      node-bindings component, which runs `napi build --platform --profile release-unwind` and PASSED).
      `.github/workflows/node-release.yml` drives the build via `npm run build` (the package.json
      script), so no separate change was needed there.

## 3. Fail-closed guard (binding-panic-firewall)
- [x] 3.1 Add a deterministic, offline guard (`scripts/tests/test_binding_unwind_profile.sh`) that FAILS
      if any binding build definition uses `--release` / omits `--profile release-unwind`, and fails
      closed on a missing/unparseable definition. Includes an inline negative-path self-check
      (compliant/missing/abort/empty fixtures). Surface exercised: the guard invocation + the four
      build definitions it reads.
- [x] 3.2 Wire the guard so `scripts/agent-gate.sh` runs it — added the `binding-unwind-profile`
      component (hard FAIL, offline).

## 4. Measure + record the delta
- [x] 4.1 Built each binding cdylib both ways (`--release` vs `--profile release-unwind`); recorded
      byte size for each (see PR notes). macOS arm64: Node 9,197,504 -> 11,413,120 (+24.1%);
      Python 9,233,808 -> 11,433,040 (+23.8%).
- [ ] 4.2 Run a representative scan micro-benchmark abort vs unwind — NOT measured in the impl
      environment (maturin/napi runtimes unavailable offline; the freshly-built cdylibs are not
      loadable as installed modules). Lead to capture the scan micro-benchmark for the PR body.

## 5. Quality gates (definition of done)
- [x] 5.1 `scripts/agent-gate.sh` PASS — AGENT-GATE SUMMARY block pasted in the handoff.
- [x] 5.2 `RUSTFLAGS="-D warnings"` clean (gate clippy PASS); no new `unwrap()`/`expect()` in library
      code (no library code changed — profile/config/guard only).
- [ ] 5.3 Spec-auditor (C) PASS against `openspec/changes/panic-unwind-profile/specs/**` — run by lead.
- [ ] 5.4 roborev clean (`--base origin/main --agent codex`) — run by lead.
- [x] 5.5 File-size ratchet respected (gate file-size PASS).

## Notes
- Do NOT build the abort-safety harness (`test_abort_safety.py` / `abort-safety.test.js`) — owned by
  #1437, HELD behind this issue. This change makes that harness pass; it does not create it.
- Merge is HELD only in the reverse direction (#1437 waits on this); this change (#1440) merges on green.
