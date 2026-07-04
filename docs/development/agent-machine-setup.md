# New-machine agent setup (10 minutes)

Setup for a machine that will run CQLite agent work (workers, the manager, or a
solo `flow-lead`). It gets the throughput-sprint accelerators on, the datasets in
place, and `gh`/roborev configured so a local gate run predicts CI.

## One command

```bash
bash scripts/bootstrap-agent-machine.sh          # check everything; print any install commands
bash scripts/bootstrap-agent-machine.sh --yes    # also auto-run brew/cargo installs + dataset fetch
```

Idempotent and safe to re-run. macOS + Linux. It **never installs without `--yes`** —
without it, it prints the exact command for each gap. It verifies, in order:

1. **Rust toolchain** (`cargo`).
2. **Gate accelerators** (issue #1848) — `sccache`, `cargo-nextest`, modern bash
   (≥4.3). Detection mirrors the gate's own `ACCEL_*` block, so the script and
   `scripts/agent-gate.sh` can never disagree.
3. **`gh` auth + the `project` scope** — the board is the sole dispatch authority
   (Path A, #1886). Missing scope → `gh auth refresh -s project`.
4. **roborev** — installed, and this machine's *configured* agent resolves.
5. **Datasets** + `CQLITE_DATASETS_ROOT` guidance.
6. **Health check** — runs the gate's fmt smoke and prints the authoritative
   `accelerators:` line.

## Accelerators: what the SUMMARY line means

Every gate SUMMARY (full **and** `--lite`) carries one machine-checkable line:

```
accelerators: sccache=on nextest=on lanes=on
```

- **`sccache`** — cross-worktree compile cache (~25.6% faster fresh builds).
- **`nextest`** — parallel `core-tests` (the gate's long pole).
- **`lanes`** — parallel gate components (needs bash ≥4.3 for `wait -n`).

States: **`on`** (detected & used) · **`absent`** (missing → the gate prints a loud
`WARN:` with the install command) · **`off`** (intentionally disabled via
`CQLITE_DISABLE_SCCACHE` / `CQLITE_DISABLE_NEXTEST` / `AGENT_GATE_JOBS=1`; no warn) ·
**`lanes=serial`** (degraded by bash <4.3). If you see `absent`, install the tool —
a machine can otherwise run ~3x slower with no signal. Install commands:

```bash
brew install sccache cargo-nextest bash     # macOS
cargo install sccache cargo-nextest         # Linux (bash via the distro package manager)
```

## Datasets and CQLITE_DATASETS_ROOT

The `*-Data.db` binaries are gitignored and live only in the **main checkout**, never
in worktrees (`git worktree add` does not copy them). Fetch once, then always point
`CQLITE_DATASETS_ROOT` at the **main checkout**, even when running the gate inside a
worktree:

```bash
bash test-data/scripts/fetch-datasets.sh
export CQLITE_DATASETS_ROOT=/path/to/main/checkout/test-data/datasets
```

## roborev: local config, never a pinned agent

roborev runs with **this machine's configured agent** — read from `.roborev.toml`
(commonly `codex`; `roborev config list` shows the resolved values). Run it with **no
`--agent`/`--model` flags**; it uses your local setup. Explicit `--agent`/`--model` is
a **per-machine troubleshooting override only** — reach for it when the bootstrap warns
that your configured agent did not resolve (fix the local config, or override for one
run). Do not treat any specific agent (claude-code, codex, …) as doctrine.

## Multi-machine: the claim protocol in two sentences

The GitHub Project board `Status` field is the sole dispatch authority; a session
**claims** an issue by pushing its `issue-<N>-<slug>` branch to origin (the
cross-machine lock, since assignee `@me` is identical for one user on two machines),
then setting assignee `@me` + `Status=In Progress` and re-reading to confirm it won the
race. Because the lock is server-side, a second machine that finds an existing claim
branch `git fetch`es it to resume rather than colliding.

## Full doctrine

The published contributor doctrine — gate contract, delivery pipeline, accelerators,
claim protocol — is the source of truth:
<https://pmcfadin.github.io/cqlite/agents-developing/>.
