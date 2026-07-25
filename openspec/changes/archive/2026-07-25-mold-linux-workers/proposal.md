# Bootstrap mold linker on Linux agent workers (issue #2859)

## Why

EC2 Linux agent workers link with the slowest option available. On the pinned 1.88.0
toolchain the Linux default is GNU bfd; after #2856 (rust-1.97.1 bump) x86_64-linux gets
lld by default, but **aarch64/Graviton workers get nothing**, and mold outperforms lld on
both arches. Linking is the one build cost sccache cannot cache — every `--lite` round and
full gate re-links every test binary (`debug = true`) from scratch, so on a warm worker
link time is a large slice of remaining wall-clock. Upstream reference point: bfd→lld
alone measured ~7× link / ~40% e2e incremental (rust-lang 1.90 release notes); mold
improves on lld and covers both Linux target triples.

macOS is explicitly out of scope: mold is Linux-only (the "sold" macOS fork is
discontinued/commercial) and Apple's ld-prime is already the fastest linker there.

## What changes

1. `scripts/bootstrap-agent-machine.sh` (Linux branch only): detect `mold`, install via the
   native package manager where possible, else warn with a cost estimate — mirroring the
   existing sccache/nextest accelerator `ok/warn` pattern.
2. Bootstrap writes a **managed block** in the per-machine `~/.cargo/config.toml` with
   `[target.x86_64-unknown-linux-gnu]` + `[target.aarch64-unknown-linux-gnu]` sections that
   route linking through mold — only after a link-probe proves the toolchain supports it
   (fail-safe: never write a config that would break linking). Idempotent; unrelated user
   config preserved; repo config untouched.
3. `scripts/agent-gate.sh` `accelerators:` line stamps mold state on Linux (present/absent/
   configured) — degradation is visible, not silent, same contract as sccache.
4. One-time A/B measurement on an EC2 worker (full gate + one `--lite` round, with/without
   mold) recorded on the issue/PR before merge (tasks.md; not a durable requirement).

## Routing

Design-driven (process/tooling) — OpenSpec change `mold-linux-workers`, capability
`agent-fleet-runtime`. Design shape pre-agreed with owner in-session 2026-07-24.

## Non-goals

- No macOS behavior change of any kind (bootstrap Darwin branch and gate output on Darwin
  stay byte-identical).
- No change to the repo-committed `.cargo/config.toml` and no change to GitHub-hosted CI
  runners (they don't have mold; per-machine config keeps them on their defaults).
- No Cranelift / codegen-backend work (nightly-only; blocked on arm64-macOS ABI — tracked
  separately if ever).
- No toolchain version change (that is #2856).
- No `RUSTFLAGS` injection via supervisor env (rejected in design.md).

## Doctrine impact

Same-change updates: `docs/development/gate-ops.md` (accelerator: mold) and
`docs/development/fleet-runbook.md` (Linux worker provisioning + one-time sccache cold
rebuild after enabling, since rustflags change cache keys). No CLAUDE.md contract change
(the gate summary `accelerators:` contract already exists; this adds a token on Linux).
