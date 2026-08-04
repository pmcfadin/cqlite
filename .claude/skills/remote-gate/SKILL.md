---
name: remote-gate
description: Use when a full agent-gate.sh run should happen on a remote agent-ami worker instead of this laptop — the gate of record for a PR, a bisect, a fixture regeneration, or several branches gated in parallel. Returns only the AGENT-GATE SUMMARY block, never a log stream.
---

# remote-gate — run the gate of record on a remote worker

The full gate takes 15–25 minutes and this machine serializes it
(`CQLITE_GATE_MAX_CONCURRENCY=1`), so a local gate blocks every other gate. A
remote `agent-ami` worker has the toolchain, the datasets, and a warm sccache
already on its `/data` volume, so it can run the identical gate while your laptop
stays free — and several branches can gate at once on separate boxes.

**The worker runs the gate. No agent runs on the worker.** That is deliberate: a
headless `claude -p` process is turn-scoped and kills its own pending background
tasks, so a box whose whole job is a 25-minute gate cannot own one. See
`docs/agent-ami-remote-worker-trial-2026-07-25.md`, finding F9 — it burned four
runs and ~8 hours of idle billing before the cause was identified. Do not put an
agent on the box.

## When to use this

- **The gate of record** for a PR (flow-closer step 1), when you want the laptop back.
- **Several branches at once** — one box each, genuinely parallel.
- **Long single-shot compute**: a bisect, a fixture regeneration, a parallel sweep.

**Do NOT** use a remote worker for the issue endgame loop (gate → review → fix →
re-gate). Review tooling cannot authenticate on a worker (trial F8: `roborev` →
`claude-code` "Not logged in", `codex` 401), so review stays local. Remote
compute, local orchestration.

## Preconditions

```bash
agent-ami list                 # is a worker already up?
```

If none, ask the owner before launching one — it bills (~$0.25/hr spot,
~$0.85 on-demand for `c7i.4xlarge`). `agent-ami up` is deliberately NOT in the
permission allowlist; launching and terminating are the owner's calls.

**Always address a box by its instance ID (`i-0abc…`), never by the `#` index.**
Indices renumber when any box is terminated, so a stale index silently targets a
different machine (trial F11) — a data-loss vector, not a nit.

## Running the gate

The branch must be **pushed to origin** — the worker fetches from GitHub, it does
not see your working tree.

```bash
git push -u origin <branch>

agent-ami ssh <id> -- bash -lc 'set -e
  cd /home/ubuntu/workspace/repo
  git fetch --prune origin
  git checkout -B <branch> origin/<branch>
  AGENT_GATE_SUMMARY_FILE=/tmp/gate.txt CQLITE_GATE_MAX_CONCURRENCY=1 \
    bash scripts/agent-gate.sh > /tmp/gate.log 2>&1 < /dev/null || true
  cat /tmp/gate.txt'
```

This blocks for 15–25 minutes and returns the complete
`==== AGENT-GATE SUMMARY ====` block — start marker → `RESULT:` → end marker.
That block is the gate's verdict and the only gate text you keep.

**Never pull `/tmp/gate.log` into context.** It is megabytes. If the summary shows
a failing component, ask for that component's lines only:

```bash
agent-ami ssh <id> -- grep -A 30 '<component-name>' /tmp/gate.log | head -60
```

`|| true` before `cat` is required: `agent-gate.sh` exits non-zero on FAIL, and
without it `set -e` would skip the summary you need in order to know *what*
failed.

## Reporting

Report the `RESULT:` line and the failing component names. Do not paste the whole
summary block into the conversation unless the owner asks — the point of this
skill is that gate churn does not accumulate in anyone's context.

```
gate on i-0abc (issue-2876-scan-read-plane-split): PASS 29/29 @ 57c9a90 (18m42s)
```

## Gating several branches at once

One box per branch, backgrounded so they overlap:

```bash
for b in issue-2876-foo issue-2877-bar; do
  ( agent-ami ssh <id-for-$b> -- bash -lc "..." > /tmp/gate-$b.txt 2>&1 ) &
done
wait
```

Then read each `/tmp/gate-*.txt`. Match box to branch by instance ID; do not
assume ordering.

## Verify the box is sane first (cheap, catches the known traps)

```bash
agent-ami ssh <id> -- ls /data/datasets                              # datasets on the durable volume
agent-ami ssh <id> -- git -C /home/ubuntu/workspace/repo status --short
```

The second must be clean. Deleted files under `test-data/datasets/` mean
`fetch-datasets.sh` wiped git-tracked fixtures (cqlite #2878) — the gate will
fail on pristine `main` and it is not your branch's fault. Stop and report it.

## Never

- **Never `agent-ami down` a box with unpushed work.** Check first:
  `agent-ami ssh <id> -- git -C /home/ubuntu/workspace/repo log --branches --not --remotes --oneline`.
  Empty output means everything is on a remote. A terminated box takes its disk
  with it, and a completed gate-passing fix was nearly lost this way (trial F1).
- **Never trust a box's self-report over `origin`.** The authoritative state is
  the pushed branch and the summary file (trial P2).
- **Never leave a box you launched unaccounted for.** Report every instance ID
  still running when you end your turn. `agent-ami volumes` shows retained
  volumes, which bill (~$24/mo each) until deleted.
