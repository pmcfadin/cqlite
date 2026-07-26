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
3. **`gh` auth + board access** — the board is the sole dispatch authority
   (Path A, #1886). The verdict is a **read-only functional probe** of the board, not
   a `project` scope-string match (#2942 — see the deltas below); the scope check
   survives only as a cheap pre-filter. Missing scope → `gh auth refresh -s project`.
   Scopes are read from the **active account's** stanza only, and the probe runs as
   `CQLITE_PROJECT_ACCOUNT` — the same account `flow-board` forces active before every
   board op — switching back afterwards so a *check* never leaves your active account
   changed. The output names the account it measured. Point it elsewhere with
   `CQLITE_PROJECT_OWNER` / `CQLITE_PROJECT_NUMBER` / `CQLITE_PROJECT_ACCOUNT`.
   It also checks **git push credentials** (#2942) — a *separate* credential path from
   `gh`. Under `--yes` it configures one, **scoped to the origin host**, and the token
   value is never written to disk.
4. **roborev** — installed, and this machine's *configured* agent resolves.
5. **Datasets** + `CQLITE_DATASETS_ROOT` guidance.
6. **Health check** — runs the gate's fmt smoke and prints the authoritative
   `accelerators:` line.

## Three deltas that fail with a message pointing away from the cause (#2942)

Each of these cost a worker a diagnosis round-trip on a Linux box. They are listed by
the **message you will actually see**, because none of the three reads as its real cause.
The bootstrap now checks the first two and fails loudly; the third is a hand-typed footgun.

| You see | Real cause | Working form |
|---|---|---|
| `fatal: could not read Username for 'https://github.com'` | `gh` is authenticated but **git is not** — they are separate credential paths. `scripts/flow/claim.sh` + `claim-heartbeat.sh` push with plain git on 10+ call sites, so the claim protocol itself does not work. | `gh auth setup-git`, or `bash scripts/bootstrap-agent-machine.sh --yes` (configures an origin-host-scoped helper that dereferences `$GH_TOKEN` at call time). |
| `gh project …` fails for a missing **`read:org`** scope on a token whose scopes DO include `project` | A scope match is evidence about a token, not about the operation. `gh project item-edit` needs `read:org`; the equivalent GraphQL mutation does not. | Widen the token (`gh auth refresh -s read:org`), **or** do board writes through the `updateProjectV2ItemFieldValue` GraphQL mutation — it succeeds with the same token. |
| `stale info` from a **bare** `git push --force-with-lease`, even when local and remote refs demonstrably match | The bare form leases against a remote-tracking ref your checkout may never have fetched. | Always the explicit CAS form: `git push --force-with-lease=<ref>:<sha>` (what the flow scripts already use). |

**A note on *which account* any of this is about.** `gh auth status` prints one stanza
**per logged-in account**, and the active one is not guaranteed first — so a plain grep
of its output can report a different account's scopes than the one your commands will
actually use. The documented instance is in `.claude/skills/flow-board/SKILL.md`: gh's
active account silently flips to an EMU account lacking `project`, and every board write
then degrades to labels **silently**. When diagnosing a board problem, always establish
the active account first (`gh auth status` → `Active account: true`), and remember
`flow-board` forces `CQLITE_PROJECT_ACCOUNT` active before each board op.

**Claim verdicts encode this too (#2942).** An unauthenticated push now reports
`CLAIM: ERROR reason=auth … (NOT retryable …)` and names the fix, for `claim.sh`'s
`claim`, `adopt`, `release` and `smoke`. It used to report `reason=infra … (transient —
retry)`, which sent workers into a retry loop on a machine fault that can never
self-clear. `reason=infra (transient — retry)` still means what it says: a real blip,
retry it. **Scope:** this classification covers `claim.sh` only —
`claim-heartbeat.sh` surfaces git's raw error on its pushes and does not classify.

**The `--yes` credential helper reads `$GH_TOKEN` from the environment.** That is
deliberate (no secret on disk), but it means the helper only works in shells where
`GH_TOKEN` is exported — a systemd/cron worker started without it will still fail every
push. For unattended workers prefer `gh auth setup-git`.

## Accelerators: what the SUMMARY line means

Every gate SUMMARY (full **and** `--lite`) carries one machine-checkable line:

```
accelerators: sccache=on nextest=on lanes=on sccache-health=ok
```

- **`sccache`** — cross-worktree compile cache (~25.6% faster fresh builds).
- **`nextest`** — parallel `core-tests` (the gate's long pole).
- **`lanes`** — parallel gate components (needs bash ≥4.3 for `wait -n`).
- **`mold`** (issue #2859, **Linux only** — no token on macOS) — the fast linker,
  wired via a per-machine `~/.cargo/config.toml` managed block. On Linux the line
  gains a trailing `mold=linked | overridden | present-unconfigured | absent` token:
  `overridden` means a global `RUSTFLAGS` is suppressing the wired flags (don't
  export one on a worker); `present-unconfigured` means mold is installed but not
  wired — re-run bootstrap.

States: **`on`** (detected & used) · **`absent`** (missing → the gate prints a loud
`WARN:` with the install command) · **`off`** (intentionally disabled via
`CQLITE_DISABLE_SCCACHE` / `CQLITE_DISABLE_NEXTEST` / `AGENT_GATE_JOBS=1`; no warn) ·
**`lanes=serial`** (degraded by bash <4.3). If you see `absent`, install the tool —
a machine can otherwise run ~3x slower with no signal. Install commands:

```bash
brew install sccache cargo-nextest bash          # macOS (mold is Linux-only)
cargo install sccache cargo-nextest              # Linux (bash + mold via the distro package manager)
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
**claims** an issue with `bash scripts/flow/claim.sh claim <N>`, which atomically creates
the slugless ref `refs/claims/issue-<N>` on origin — THE cross-machine lock, arbitrated
server-side (assignee `@me` is identical for one user on two machines, and the
`issue-<N>-<slug>` branch is only PR plumbing, never the lock) — then sets assignee `@me`
+ `Status=In Progress`. A second machine picking up a reaped/dead claim adopts it via
compare-and-swap — `bash scripts/flow/claim.sh adopt <N> --expect <current-sha>` — not a
bare `git fetch`, so a resurrected original holder loses the lease and detects it at once.
When the claim ref is FREE but an `issue-<N>-*` branch still exists on origin (a resumed,
parked, or reaped issue, or a merged-but-undeleted branch), `claim` refuses with
`reason=legacy-branch-lock` and prints the one sanctioned resume:
`bash scripts/flow/claim.sh adopt <N> --expect none --reason <why>` (#2945) — git's empty
lease, so the create is still server-arbitrated and the claim commit records who + why.
That command is printed only at `open-prs=0`; with an open PR (or an unreadable PR list)
the refusal says `remediation=withheld open-prs=<n>` — confirm ownership before resuming.

## Full doctrine

The published contributor doctrine — gate contract, delivery pipeline, accelerators,
claim protocol — is the source of truth:
<https://pmcfadin.github.io/cqlite/agents-developing/>.
