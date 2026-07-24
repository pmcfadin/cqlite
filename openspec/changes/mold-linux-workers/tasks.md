# Tasks — mold on Linux agent workers (issue #2859)

## 1. Bootstrap: detection + install (surface: `scripts/bootstrap-agent-machine.sh`)
- [x] 1.1 Linux branch: detect `mold`; `ok` line with version when present
- [x] 1.2 Install via detected package manager (apt/dnf/yum/pacman) when missing; `warn`
      with cost estimate when no manager matches (advisory, never fails the run)
- [x] 1.3 Link probe: verify resolved C compiler accepts `-fuse-ld=mold` (cc, then clang
      fallback) before any config write

## 2. Bootstrap: managed cargo config block (surface: `~/.cargo/config.toml` writer in bootstrap)
- [x] 2.1 Write delimited `BEGIN/END cqlite-mold` block with both Linux target triples;
      `linker = "clang"` variant when only clang passed the probe
- [x] 2.2 Idempotent replace-the-block on re-run; preserve all content outside markers
- [x] 2.3 Never touch repo `.cargo/config.toml`; Darwin branch performs none of this

## 3. Gate stamp (surface: `scripts/agent-gate.sh` `accelerators:` line)
- [x] 3.1 Linux: emit `mold=linked | present-unconfigured | absent` per spec semantics
- [x] 3.2 Darwin: output byte-identical to pre-change (no token)

## 4. Tests (surfaces: `scripts/tests/test_bootstrap_agent_machine.sh`, `scripts/tests/test_agent_gate_summary.sh`)
- [x] 4.1 Bootstrap self-tests: Linux present/missing/no-manager/probe-fail paths; Darwin
      no-op; idempotent re-run; unrelated-config preservation (simulated HOME + stubbed
      `uname`/`command -v`)
- [x] 4.2 Gate summary self-test: three mold states on Linux; Darwin unchanged

## 5. Measurement (one-time, pre-merge; recorded on issue/PR)
- [ ] 5.1 A/B on one EC2 worker on the post-#2856 toolchain: full gate + one `--lite`
      round wall-clock, with/without mold, both numbers posted to #2859/PR
      (EXTERNAL — cannot run from a macOS dev box; pending an EC2 Linux worker)

## 6. Docs (same change)
- [x] 6.1 `docs/development/gate-ops.md`: mold accelerator + stamp states
- [x] 6.2 fleet-runbook: Linux worker provisioning step + one-time sccache cold rebuild note

## 7. Quality gates
- [ ] 7.1 `--lite` green each round (summary-file redirect)
- [ ] 7.2 rust-reviewer + roborev on the lite-green diff (review-first)
- [ ] 7.3 flow-closer: ONE full gate of record → C intent audit (spec-auditor vs this
      change's specs/**) → final roborev → merge-on-green → finalize
