# gate-tree-integrity — a mid-run worktree mutation SHALL invalidate the gate artifact (issue #2926)

## Milestone

Maintenance / delivery-harness hardening (no product milestone). **Design-driven**: there is no
external oracle for "what a certification artifact must prove". The remedy has real latitude
(fail-closed stamp vs. worktree snapshot vs. advisory lock), it changes the gate contract that every
agent reads, and it touches the doctrine pages — exactly the shape `openspec/config.yaml` routes to
a proposal rather than to a bare issue + pinned test.

## Why

`scripts/agent-gate.sh` stamps the SUMMARY's `commit:` line from HEAD **at summary-write time**:

```
scripts/agent-gate.sh:4888   # full gate
scripts/agent-gate.sh:3703   # --lite
scripts/agent-gate.sh:3923   # --delta
SUMMARY_META+=("commit: $(git rev-parse --short HEAD) branch: ... dirty: ...")
```

Nothing anywhere in the script reads HEAD or any tree state at gate **start** (verified: the only
`git rev-parse` calls in the file are the three stamps above, the `--delta` anchor resolution at
:3818, the push-signal at :4926, and two `--verify` base-ref probes). So a run whose worktree is
modified while it executes emits a SUMMARY that attributes **mixed-tree results to the final
commit** — formally indistinguishable from a legitimate certification.

Field incident, 2026-07-26, gating PR #2916 (#2914): a full gate started 17:21:32 against HEAD
`4686c37`; a review-fix round committed `116d0b9` into the same worktree mid-run. It was killed and
happened to leave `RESULT: INCOMPLETE`, so the fail-safe held **by timing luck**. Had it completed
it would have emitted `commit: 116d0b9 … RESULT: PASS` for a tree most components never compiled.

This is reachable **without violating the one-worker rule (#1930)**: a single lead legitimately runs
a `flow-closer` (gating) and a fixer (editing) that overlap on one worktree. #1582's retro covers the
*collision*; nothing covers the resulting artifact being silently wrong. Because the full gate is
*the only run that counts* (#719), a mixed-tree run wearing the final commit's SHA launders an
uncertified tree into an apparently-certified one, and nothing downstream re-checks it.

The blast radius is wider than the `commit:` line. Three components derive their own *scope* from
git mid-run — `file-size` (:3256–3268), `--lite` blast-radius selection (:3356–3368) and `--delta`
fail-closed path classification (:3881–3882) — so a mid-run commit can silently change **which
tests a run even selects**, not just which code it compiled.

## What changes

- `scripts/agent-gate.sh`: capture a **tree identity** (HEAD + a content-sensitive digest of every
  uncommitted tracked change and every untracked non-ignored file) at gate START, re-capture at each
  component boundary and at terminal emit, and **FAIL CLOSED** on mismatch with a named
  `tree-integrity: FAIL (tree-mutated-midrun; …)` line. A mutated run can never emit `RESULT: PASS`.
- `scripts/agent-gate.sh`: stamp `tree-start:` and `tree-end:` provenance lines (HEAD short-sha +
  dirty flag + digest) into **every** SUMMARY block — full, `--lite`, `--delta`, `--only` — and
  `tree-start:` into the startup `INCOMPLETE` sentinel, so even a killed gate records the tree it
  began on.
- Applies to `--lite` and `--delta` as well as the full gate (see `design.md` §4 for the reasoning);
  helper/self-test emission modes stamp a synthetic identity.
- **No bypass**: no environment variable turns a mutated run green (`design.md` §6).
- New discriminating self-test `scripts/tests/test_agent_gate_tree_integrity.sh`, wired into the
  `tooling-tests` component.
- Doctrine in the same change: `CLAUDE.md` gate section + the
  `website/src/content/docs/agents-developing/gate-contract.md` page state that a mid-run tree
  mutation invalidates the run and that a closer MUST check `tree-integrity:` before trusting a
  summary.

## Non-goals

- **Not** a worktree snapshot / detached-copy gate, and **not** an advisory worktree lock (the
  issue's other two suggested remedies). Both are larger and neither makes the *artifact*
  self-evidently invalid, which is the property that actually failed here. Rejected with reasons in
  `design.md` §2.
- **Not** a fix for the #2908 poll-predicate hazard (`grep -q 'RESULT:'` matching the `INCOMPLETE`
  placeholder). This change is explicitly constrained not to regress it and recommends the joint
  `.running`-sentinel fix, but that fix stays #2908's.
- **Not** a change to `--locked`/`--frozen` on the gate's cargo invocations (a follow-up; see
  `design.md` §5, the lockfile carve-out).
- No change to the `#2874` summary-integrity mechanism, the concurrency cap, component selection, or
  the SKIP semantics of any component.
- No production (`cqlite-*` crate) code changes at all.

## Impact

- **No-heuristics mandate**: unaffected — no decode path is touched.
- **Public binding surfaces (Python/Node/CLI)**: unaffected.
- **<128MB memory budget**: unaffected.
- **Gate contract**: two new provenance lines and one new named failure class in the SUMMARY block.
  The block's terminal line stays `RESULT: PASS|FAIL|PARTIAL|INCOMPLETE` — no parser that keys on
  `RESULT:` or on the start/end markers changes behaviour.
- **Cost**: ~17 ms per capture measured in a 4,346-file checkout (`design.md` §3), i.e. well under a
  second added to a 40–60 minute gate.
