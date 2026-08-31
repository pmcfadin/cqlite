# New-machine agent setup (10 minutes)

Setup for a machine that will run CQLite agent work (workers, the manager, or a
solo `flow-lead`). It gets the throughput-sprint accelerators on, the datasets in
place, and `gh`/roborev configured so a local gate run predicts CI.

## One command

```bash
bash scripts/bootstrap-agent-machine.sh          # check everything; print any install commands
bash scripts/bootstrap-agent-machine.sh --yes    # also auto-run brew/cargo installs + dataset fetch
bash scripts/bootstrap-agent-machine.sh --fix-credentials --fix-gate-pin --strict
                                                 # image/launcher preflight (#3369, #3414): wire git push
                                                 # credentials AND persist the single-gate pin, then exit 1
                                                 # on any [warn]. Two narrow repairs, nothing else — no
                                                 # toolchain installs, so verify stays a verification step.
```

**Both repair flags are needed, and omitting `--fix-gate-pin` reproduces #3414.** A launched box
gets exactly one bootstrap invocation, and `--fix-credentials` alone leaves
`CQLITE_GATE_MAX_CONCURRENCY` unpersisted — so every gate on that box silently resolves the #1825
concurrency cap from the default formula and admits co-tenants, which is the defect #3414 exists to
remove. `--fix-gate-pin` performs section 5b's `/etc/environment` write (idempotent; it never
rewrites a value someone set deliberately) and the run then VERIFIES it by probing a fresh session.
Check the `gate-pin:` line in the output, and the gate's own
`cpu-budget: ... max-concurrency=N(pinned)` on the first summary that box produces.

`--strict` is what the agent-ami profile's `verify.run` uses. Without it the script always
exits 0 (so it composes into setup scripts) and the ONLY failure signal is the literal
string `All checks green.` in its stdout — a signal a caller can forget to check.

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
   `gh`. Under `--yes` (or `--fix-credentials`) it configures one, **scoped to the origin
   host**, and the token value is never written to disk. That check is a *configuration*
   probe (`git credential fill`, which never contacts the network): it proves a helper
   ANSWERS, never that a push would succeed. The push claim belongs to 3b below.
   The **fallback** repair — the `$GH_TOKEN`-dereferencing helper, used only when
   `gh auth setup-git` yields no usable credential — is additionally gated on **token
   authority for that host**: `gh auth token --hostname <push host>` must return a token
   **equal to** the `$GH_TOKEN`/`$GITHUB_TOKEN` this run would install. **A successful
   login for the host is deliberately NOT sufficient** — on a box authenticated to both
   `github.com` and a GitHub Enterprise host, a login check passes for both and the
   github.com token would be handed to the enterprise host. The host itself is read from
   local git config (`git remote get-url --push`), so a typo, a leftover fork/mirror
   `pushurl` or a stale `insteadOf` would otherwise configure git to hand a real
   credential to a host nobody intended, during a preflight the launcher runs
   automatically. Anything other than an exact match — no token for that host, a different
   token, an unanswerable `gh`, an empty answer — ⇒ the repair is **refused**, one `[warn]`
   names the host (never a token value), and `--strict` exits 1. `gh auth setup-git` itself
   stays unconditional: it makes `gh` the helper, and `gh` decides, per host, what it will
   answer for.
3b. **git PUSH capability** (#3369) — the section above validates *configuration*
   (`git credential fill` never contacts the network); this one performs **the
   operation**, delegating to `scripts/flow/claim.sh smoke` to create, read back and
   delete a throwaway `refs/claims/smoke-<commit-sha>` ref. It runs **after** the credential
   fix, so it measures the machine as the fix left it. The verdict is **three-valued**
   and prints one greppable line:

   | Line | Meaning |
   |---|---|
   | `[ok]   git-push: VERIFIED (refs/claims/* create+ls-remote+delete on 'origin')` | affirmatively measured — the claim protocol works here |
   | `[warn] git-push: FAILED (...)` | the operation did not complete. For a credential fault the line names it and gives the #2942 remedy; for anything else bootstrap **quotes `claim.sh`'s own verdict verbatim** rather than guessing a cause — including `reason=cleanup-unverified`, where the cleanup delete exited nonzero so delete capability is unproven (`release` deletes `refs/claims/issue-<N>`) and the ref may survive; the quoted line carries the `ls-remote` check |
   | `[warn] git-push: UNMEASURED (...)` | no remote / unreachable / no `timeout` to bound it / no verdict — capability is **UNKNOWN, not ok** |
   | `[warn] git-push: OPT-OUT (--skip-push-probe)` | deliberately not measured; still a warning, so it can never buy a green |

   UNMEASURED is a warning on purpose: an unmeasured capability must never inherit the
   permissive branch. `--skip-push-probe` is for offline boxes and hermetic tests, and is
   **not** `--skip-smoke` (which skips the final *gate* run).

   **Cost:** measuring the operation means performing it — two extra network round trips
   and a transient `refs/claims/smoke-<commit-sha>` ref created and deleted on the shared
   origin, on **every** run of this script. An **observed** cleanup failure FAILS the
   probe (`reason=cleanup-unverified` — the delete exited nonzero, so whether the ref
   survives is unknown), so it can never pass quietly. A run **interrupted before
   cleanup** is the residual: it leaves no verdict at all and can still strand a ref.
   List them with `git ls-remote origin 'refs/claims/smoke-*'` and delete with
   `git push origin --delete refs/claims/smoke-<commit-sha>` — always safe.
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
| `git push` fails on a box where `gh auth status` is green **and** `git ls-remote origin HEAD` succeeds | Same shape, one subsystem over: a read is evidence about reachability, not about the write. `claim.sh claim <N>` — the first thing a lane does — is a `git push`, so the box looks healthy and no lane can start (#3369). | `bash scripts/bootstrap-agent-machine.sh --fix-credentials`, then read its `git-push:` line. That check is the direct application of the doctrine sentence below: it PERFORMS the push instead of inferring it. |
| `git push has NO credentials for <host> and this run REFUSED to configure any` under `--fix-credentials` | The push host was resolved from local git config, and `gh auth token --hostname <host>` did not return the token this environment holds — gh has none for that host, or a **different** one (the reachable case: a github.com token on a box that also has a GitHub Enterprise host). A login for the host is not enough; the run would otherwise have configured git to hand `$GH_TOKEN` to a host that token does not belong to. Fail-closed by design (#3369). | If the host is right: `gh auth login --hostname <host>`, then re-run `--fix-credentials` — preferably **without** `$GH_TOKEN` set, so `gh auth setup-git` supplies that host's own token. If it is wrong: `git remote set-url --push origin <the intended url>` — check for a leftover fork/mirror `pushurl` or a stale `insteadOf`. |
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
- **`perf`** (issue #3249, **Linux only** — no token on macOS) — can this box be
  profiled *at all*? Read free from `/proc/sys/kernel/{perf_event_paranoid,kptr_restrict}`
  (never a `perf` exec, so the gate pays nothing for it). Values:
  **`ok`** (unprivileged per-CPU profiling **and** kernel symbols available) ·
  **`paranoid-<N>`** (`perf_event_paranoid = N >= 1`
  forbids **CPU-wide** events, so the `perf stat -C <cpu>` the measurement doctrine
  mandates is DENIED — a *permission* verdict, not a missing capability; images ship
  `4`, and Debian/Ubuntu's extra `>= 3` level denies unprivileged perf *entirely*) · **`kptr-restricted`** (paranoid is fine but `kptr_restrict != 0`, so kernel
  frames resolve to bare addresses — a silent attribution loss) · **`absent`** (the
  `/proc` controls are not present, e.g. a container — tune the host) ·
  **`unknown`** (present but unparseable; never guessed). Anything but `ok` on a box
  you intend to measure means **re-run `bash scripts/bootstrap-agent-machine.sh --yes`**,
  which installs `/etc/sysctl.d/99-cqlite-perf.conf` and then *verifies* it by running
  `perf stat -C 0 -e cycles`. Rationale (`-1`, not `1`), the BPF-still-needs-sudo
  caveat, and the single-tenant security posture: `docs/development/fleet-runbook.md`.

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

## roborev: the sanctioned wrapper, agent + model always explicit

Every review goes through the fail-closed wrapper, never the bare CLI (#2964):

```bash
bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol \
  [--repo /abs/path/to/checkout] [--base origin/main]     # Claude: --agent claude-code --model claude-opus-5
```

**Both `--agent` and `--model` are ALWAYS required** — the wrapper rejects a missing one
as a usage error, because one alone inherits the mismatched `.roborev.toml`-pinned model
and fails as a silent-looking review outage. **Push the branch first**; the wrapper asserts
that and FAILs otherwise. Three direct-CLI forms are **NON-SANCTIONED**:
`roborev review --branch` **without an explicit `--repo`** (from a worktree it resolves
against the ROOT checkout and reviews the base commit, reporting clean having reviewed
NOTHING), the two-positional commit-range form (its range base is git's empty tree), and a
single-SHA review (it reviews ONE COMMIT, not the branch — partial, with a sha that equals
HEAD, so no sha check catches it). `--repo` is what makes `--branch` correct: the wrapper
reviews the RANGE `<base>..HEAD` and asserts both endpoints from the job record, so
`reviewed-sha:` is a range and `job-record:` reports the record's completeness. Any
non-PASS terminal `RESULT`, `NOTHING-TO-REVIEW` included, is a failed round and a blocked
merge, never a clean pass. Why: CLAUDE.md + the `agents-developing/roborev-findings` page.

**Check per box which reviewer is actually usable: `roborev check-agents`.** A resolved
`.roborev.toml` value is not a working reviewer — on this fleet box (roborev v0.61.2, daemon
healthy) only `codex` passes; `claude-code` fails with `OAuth session expired and could not
be refreshed`. That is exactly why "this machine's configured agent" was bad guidance: name
the agent + model explicitly, choosing one `check-agents` reports healthy.

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
`reason=legacy-branch-lock detail=<branches> claim-ref=free resume=documented-procedure` —
a diagnosis that names the blocking branch and confirms the claim ref itself is free.
The one sanctioned resume is
`bash scripts/flow/claim.sh adopt <N> --expect none --reason resume-legacy-branch-lock:branch-outlived-claim`
(#2945) — git's empty lease, so the create is still server-arbitrated and the claim commit
records who + why (a placeholder reason like `<why>`, or one still carrying an unsubstituted
`<…>`, is refused — so the `--reason` above is shown already substituted). It is deliberately **not
printed** by the refusal: a printed line gets run literally, and an older-fleet worker locks
with the BRANCH while holding no claim ref, so the empty-lease adopt would succeed against an
actively-worked lane. Establish abandonment yourself first — `bash scripts/flow/claim-heartbeat.sh
should-reap <machine>` (the same test `flow-board`'s reaper applies: age > 4h AND no open PR AND
pid-dead-if-local), plus the board `Status` and the branch/PR author — and never hand-craft a
claim commit.

## Full doctrine

The published contributor doctrine — gate contract, delivery pipeline, accelerators,
claim protocol — is the source of truth:
<https://pmcfadin.github.io/cqlite/agents-developing/>.
