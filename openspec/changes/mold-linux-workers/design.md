# Design — mold on Linux agent workers (issue #2859)

## Decision 1: wiring mechanism — per-machine `~/.cargo/config.toml` managed block (CHOSEN)

Bootstrap writes an explicitly-delimited managed block:

```toml
# BEGIN cqlite-mold (managed by scripts/bootstrap-agent-machine.sh — do not edit inside)
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-fuse-ld=mold"]

[target.aarch64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
# END cqlite-mold
```

Idempotency = replace-the-block: on re-run the block is regenerated in place; everything
outside the markers is preserved byte-for-byte. Only the two Linux triples are named, so
the block is inert on any other host arch even if copied around.

**Fail-safe probe before writing:** bootstrap first proves the system C compiler accepts
mold (`cc -fuse-ld=mold -Wl,--version` on a trivial link, gcc ≥ 12.1 or clang). If the
probe fails with `cc` but `clang` is present and passes, the block adds
`linker = "clang"` per triple. If no compiler passes the probe, bootstrap WARNS and writes
nothing — a machine must never end up with a config that breaks linking.

### Beat (rejected alternatives)

- **`RUSTFLAGS` env exported by the supervisor** — invisible to interactive sessions on the
  same box (two different sccache key spaces on one machine: every attended/unattended
  switch is a cold rebuild), and silently lost when a worker is launched outside the
  supervisor. Config-file wiring is seen identically by every cargo invocation.
- **Repo-committed `.cargo/config.toml`** — target-scoped sections would also apply to
  GitHub-hosted CI runners, which don't have mold → hard link failure on every CI job, or
  else a guard that reintroduces per-machine divergence anyway. Per-machine keeps CI on its
  defaults (lld post-#2856 on x86_64).
- **`mold -run <cmd>` wrapper** — must wrap every invocation site (gate, supervisor, ad-hoc
  cargo, IDE); any missed site silently loses the speedup. Config wiring has one site.

## Decision 2: install path — native package manager, warn-only fallback (CHOSEN)

Detect apt/dnf/yum/pacman and install `mold` when missing (AL2023, Ubuntu 22.04+, Fedora
all package it). No package manager match → `warn` with the estimated cost (mirrors the
sccache "MISSING — ~X% slower" message) and skip; bootstrap stays advisory and idempotent,
never fails the machine. **Beat:** building mold from source (heavy C++ build, toolchain
deps on a fresh worker — not worth it when every target distro packages it).

## Decision 3: gate stamp semantics — three states, Linux-only token (CHOSEN)

The `accelerators:` line gains a `mold=` token on Linux hosts only:

- `mold=linked` — binary present AND the managed block is active in the resolved cargo config
- `mold=present-unconfigured` — binary present, no managed block (bootstrap not re-run)
- `mold=absent` — binary missing

Darwin output is byte-identical to today (no token) — macOS has no mold and adding a
permanent `n/a` token would churn every existing summary parser/fixture for zero signal.
**Beat:** a boolean present/absent (hides the "installed but not wired" failure mode, which
is exactly the silent degradation the accelerators contract exists to surface).

## Interaction with #2856 (rust-1.97.1)

Independent but ordered-after for measurement honesty: the A/B runs on the post-bump
toolchain so the recorded delta is mold-vs-lld (x86_64) and mold-vs-bfd (aarch64) — what
the fleet will actually run. Target-level `rustflags` in cargo config are additive over the
toolchain's default linker choice; mold takes precedence via `-fuse-ld`.

## Decision 4: RUSTFLAGS precedence — keep rustflags, add a fourth `overridden` state (CHOSEN)

Cargo applies a config `target.<triple>.rustflags` ONLY when no higher-precedence flag
source is set; a non-empty environment `RUSTFLAGS` **suppresses** the config value
entirely (it does not merge). So on a worker that exports a global `RUSTFLAGS`, the managed
block's `-fuse-ld=mold` is silently inert and a bare `mold=linked` stamp would LIE. We keep
the config-`rustflags` mechanism (it is the one wiring seen identically by every cargo
invocation — see Decision 1) and make the dishonesty impossible instead: the gate stamps a
fourth state `mold=overridden` whenever a non-empty `RUSTFLAGS` is present at stamp time and
the managed block is active. The fleet rule (fleet-runbook, same change) is therefore:
**never export a global `RUSTFLAGS` on a worker** — scope it per-command (as the gate's own
clippy/minimal-build components already do). A wrapper-linker (`[target].linker = mold`)
would not be RUSTFLAGS-suppressible, but it is rejected for now: it needs mold's own
`ld`-compatible driver wiring and diverges from the trivial `-fuse-ld` probe, and the
`overridden` stamp fully closes the honesty gap at zero extra machinery.

## Known cost

Adding rustflags changes sccache cache keys → one cold rebuild per machine at enablement.
Documented in fleet-runbook (same change).
