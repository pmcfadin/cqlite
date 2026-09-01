# Design — merge-result certification (issue #3680, slice 2 of #3650)

All line numbers below were **re-verified against `HEAD = ef083e2bf`**. The numbers in issue #3680's own
scope section were mapped on the slice-1 lane and are **all stale**: `agent-gate.sh` is **18817** lines,
not "14k", and **PR #3561 has already merged** (merge commit `21448663c`, an ancestor of this branch), so
the "in flight" warning is moot — but it is what moved them. The corrected map is in D1.

## D1 — The verified integration map

| thing | issue claims | actual |
|---|---|---|
| mode `case "${1:-}"` | :2678 | **:5828** (`esac` :6056) |
| marker pair + `SUMMARY_MODE_LINE` | :2835-2851 | **:6064-6078** |
| summary filename defaults | :2897-2913 | **:6744-6763** |
| `_emit_terminal_summary` | :4111 | **:8289** |
| dispatch | :13265 | **:17805** / **:17814** |
| usage table | :698-745 | **:748-806** |
| `premerge-assert.sh` residual 3 | :99-116 | **:100-117** (+119-137) |
| `premerge-assert.sh` success scope | :679-684 | **:1012-1034** |
| `test_premerge_assert.sh` Case 39 | :842-867 | **:1310-1364** |
| `CLAUDE.md` | :1052-1062 | **:1734-1743** |
| `.claude/agents/flow-closer.md` | :210-216 | **:262-269** |
| `.claude/skills/flow-address/SKILL.md` | :76 | **:74-76** |

Three structural facts the numbers alone do not carry, and each one shapes the mode:

- **Mode flags must be argument 1.** `case "${1:-}"` has no outer loop; only `--delta` runs an inner
  `while`/`shift`. `--merge-result` therefore takes its options the way `--delta` does or not at all.
- **Full is the DEFAULT arm and emits no `MODE:` line**; lite and delta are `if`/`elif` overrides at
  :6067-6078. A fourth mode extends that chain.
- **`_emit_terminal_summary` is a thin no-clobber forwarder, not a formatter.** Markers reach the block
  through the globals `SUMMARY_START_MARKER`/`SUMMARY_END_MARKER`/`SUMMARY_MODE_LINE`, read inside
  `emit_summary` (:7969, block :7983-8001) and `_integrity_fail_block` (:8119). Setting the three globals
  is the whole of the mode's summary wiring; nothing is threaded through call arguments.

## D2 — The composition is a SYNTHETIC COMMIT, and that is forced by two existing guards

```
tree=$(git merge-tree --write-tree <origin/main> <branch-head>)      # rc 0 and a tree sha, or conflicts
synth=$(git commit-tree "$tree" -p <origin/main> -p <branch-head> -m …)
git worktree add --detach <scratch> "$synth"
```

Measured on this lane (git 2.43.0): `merge-tree --write-tree` returns rc 0 and a tree sha; `commit-tree`
yields a commit whose tree **is** that tree; a scratch checkout is **143M** excluding `target/` and
`.git`, and `sccache` is on PATH so the build is warm-ish rather than cold.

**Why a commit and not a dirty checkout — this is a constraint, not a preference.**
`premerge-assert.sh` enforces `dirty: no` **affirmatively** (`assert_clean_tree` :700-755, `= no` at
:733), and #2926 captures `dirty` in the start identity and re-compares it at **every** component
boundary (`_assert_tree_integrity` :8555, from `record_result` :9174). A composition applied to a
worktree without committing reads `dirty: yes` and is refused twice over. Two parents rather than one
because the commit is honest about what was composed, and because it is what makes D5 work.

**A conflict is not a verdict.** If `merge-tree` reports conflicts the merge result does not exist; the
mode refuses with a named cause and stamps no certification. `premerge-assert.sh` then refuses for want
of one. Nothing degrades to a pass.

## D3 — Legibility falls out of the mechanism, not out of a label (AC3)

The issue makes legibility a hard requirement because the misread that nearly merged #3616 on #3580's
verdict was someone reading `RESULT: PASS` as covering what lands. A label alone would be the same class
of protection that failed there. Instead the two directions are each refused **structurally**:

- **A merge-result block cannot be pasted as a branch-head certification.** `commit:`/`tree-start:` are
  derived only from the tree capture, so they name the **synthetic commit** — which can never equal the
  certified PR head. Case A's `assert_covers` (:634-646) therefore fails on it by construction, before
  any marker is consulted.
- **A branch-head block cannot be pasted as a merge-result certification**, because the new leg requires
  the merge-result marker and `MODE: merge-result` **affirmatively**, the way Case B already requires
  `MODE: delta` (:876-882).

On top of that mechanism, and not in place of it: a distinct marker pair, a distinct `MODE:` line, and
keys naming `origin/main`'s tip, the branch head, and the synthetic commit.

## D4 — Four integration points the issue does not list, each silent if missed

1. **`scripts/gate-liveness.sh` hard-codes a CLOSED dialect regex** —
   `^==== (END )?AGENT-GATE( LITE| DELTA)? SUMMARY ====$` at :523, :528, :539, :540, :564, :685, plus an
   opener/closer dialect-match check at :567-570. A fourth header is **unreadable** to the liveness
   reader, which is how a lane tells a running gate from a reaped one (#3473). It must learn the dialect.
2. **`scripts/tests/test_agent_gate_component_set.sh:2415-2503`** censuses every `emit_summary` /
   `_emit_terminal_summary` call site. A new emit site lands in `GAP` and **FAILs `tooling-tests`**
   unless it stamps `COMPONENT_SET_LINE` or carries `# component-set-exempt: <reason>`.
3. **`premerge-assert.sh:441-443`** names `LITE_S`/`DELTA_S`/`DELTA_E` for by-name refusal. The
   merge-result markers join that list, or a merge-result block is silently accepted as a full block.
4. **`agent-gate.sh:17413-17415`** — `--anchor-summary-file` must contain the full marker and must not
   contain the LITE or DELTA marker. Same addition, same reason.

Two defaults are already correct and are **left alone deliberately**, both because they are positive
enumerations that a new mode falls outside of:

- `acquire_gate_slot` (:17748) exempts `LITE`/`DELTA`/`ONLY`/`CQLITE_GATE_DISABLE_CAP` only ⇒ the new
  mode **queues** under the #1825 cap. Correct: it runs real components.
- `_component_set_strict()` (:5280) is `[ -z "$ONLY" ] && [ "$LITE" -eq 0 ]` — `DELTA` is strict by
  falling through the negative test ⇒ the new mode is **fail-closed** on #3544 skew by default.

And one consequence worth stating rather than discovering: `REPO_ROOT` is `$PWD` after
`cd "$(dirname "$0")/.."` (:887-888), so a gate run in the scratch worktree runs **that worktree's
composed copy** of `agent-gate.sh`. That is desirable — it is the script that will be on `main` — and it
is also why the mode cannot be a wrapper that re-enters the lane's own copy.

## D5 — AC1, the recursion: the answer is the slice-1 advisory applied to the composed base

**SEAM 1 — the owner's call. What follows is a recommendation with its measurement, not a ruling.**

A merge-result gate composed against `origin/main` at time *T* is stale in exactly the same way once
main reaches *T+1*. The natural answer — slice 1 named it and declined to rule on it — is to apply the
same predicate one level down. What makes it more than symmetry is that **it needs no new code at all**:

`merge-base(origin/main, <synthetic>)` **is the composed base**, because the synthetic commit carries
`origin/main`-at-composition-time as a parent. So running the **unmodified** slice-1 advisory with the
synthetic commit as its subject answers the recursion exactly.

Verified end to end on this lane, non-degenerate (a genuine off-main branch head with a real diff),
composed against `origin/main~10`:

```
BASE-STALENESS: base 9da235a6cb9c48b78e019a359e8851ad8d007e09  <- == the composed base
BASE-STALENESS: behind 10 commits
BASE-STALENESS: diff-paths 12
BASE-STALENESS: blast-radius 9 RECOGNISED of 10 commits behind
BASE-STALENESS: verdict STALE-RECOGNISED            (exit 4)
```

**The precondition, stated because a first probe fell foul of it.** The property holds because a PR head
is **not an ancestor of `origin/main`**. An earlier probe used an on-main commit as the "branch head";
`merge-base` collapsed to that commit and the scan reported `diff-paths 0`. That is a **fixture
artifact, not a mechanism limit** — but it is exactly the shape of a test that passes for the wrong
reason, so the spec conditions the requirement on it and pins it with a case.

**Why it terminates rather than regresses.** Same predicate at both levels, so there is no second
definition to drift. Each re-gate strictly advances the composed base toward current `main`, and the
staling subset is blast-radius churn — **measured at 35% of commits behind** in slice 1, not 100% — so
the fixed point is reachable rather than a race the lane can never win.

**Alternatives, and why they are worse:**

| option | objection |
|---|---|
| a **time** bound (valid *N* minutes) | time is not the property. One bad commit inside the window defeats it, and a quiet six hours forces a re-gate that nothing required. |
| a **commit-distance** bound (≤ *K* behind) | count is not causation. PR #3362's culprit was **1 of 107**; any *K* > 1 admits it and any *K* ≤ 1 blocks everything. |
| **always re-derive at merge time** | maximally sound and operationally unusable: `main` moved **12× in 4 hours** the night #3650 was filed, and a full gate is far longer than the gap between commits — the lane can livelock. |
| **blast-radius recursion (recommended)** | inherits slice 1's declared non-exhaustiveness (gap 1 of 2, the dependency closure). Enforcement makes that gap *blocking*, which raises the cost of a false negative without changing its existence. Declared, not hidden. |

## D6 — AC1b, the git version floor: measure by DOING, declare the scope, add no parse

**SEAM 1 — the owner's call.** Slice 1 recorded a repo-wide git version floor as belonging to this issue
"as a precondition of *enforcement*". Two floors are actually in play: **>= 2.38** for
`merge-tree --write-tree`, and slice 1's declared **>= 2.36** no-fetch scope for `GIT_NO_LAZY_FETCH`.

**Recommendation: no repo-wide floor and no `git --version` parse.** Three reasons, in order of weight:

1. **The dependency already fails closed.** On git < 2.38, `merge-tree --write-tree` fails, the mode
   refuses with a named cause, no certification is stamped, and `premerge-assert.sh` refuses for want of
   one. There is no permissive branch to fall into — which is the only thing a floor would buy.
2. **Measure by doing, not by parsing a version string.** This repo's own precedent: `claim.sh smoke`
   measures git push capability **by performing the push**, and reports `VERIFIED`/`FAILED`/`UNMEASURED`
   rather than trusting a credential-helper answer. Exercising the capability is strictly better evidence
   than a version comparison, and it cannot drift from what the code actually needs.
3. **Slice 1's own reversal ruled on this exact trade.** Its round 5 built version and promisor detection
   and the owner **deleted it**, on the #3549 R10 precedent: *where a scenario is unreachable, declare the
   scope in code, in operator-visible text and in the PR body rather than build machinery for it.*
   Measured on this fleet: git **2.43.0**, no promisor markers. Building the parse here would reinstate
   what the owner removed one slice ago.

**What it costs, stated plainly:** the failure on an old-git host is a refusal naming a git command, not
a sentence naming a version floor. That is a worse *diagnostic* and an equally safe *verdict*. The floor
is therefore **declared** — in the mode's header, in one operator-visible output line, and in doctrine —
so the sentence exists where a reader will look, without a probe deciding anything.

**The counter-argument, so the owner can weigh it rather than take my framing:** enforcement changes the
stakes. A declared-but-undetected precondition under a *non-blocking advisory* costs nothing when
violated; under a *merge blocker* a reader may be owed a clearer statement than "git command failed". An
owner who weighs the diagnostic higher than the precedent should rule for an explicit floor, and the
honest cost of that is roughly what slice 1 deleted.

## D7 — Where enforcement attaches, and what it must never do

The new leg runs in `premerge-assert.sh` and consumes the advisory it already invokes at :385-386 — which
today captures `adv_rc` and **prints it without branching** (:398-401), delegating 4/5 semantics to this
change in prose. The leg is fail-closed on the closed token set:

| advisory | requirement |
|---|---|
| `0` / `NO-STALENESS-RECOGNISED` | merge-result certification **not** required (the owner's blast-radius ruling) |
| `4` / `STALE-RECOGNISED` | merge-result certification **REQUIRED**; absent ⇒ refuse |
| `5` / `UNMEASURED` | **treated as STALE** ⇒ certification REQUIRED; absent ⇒ refuse |
| `3` / usage, or any unrecognised value | **treated as STALE** — never a pass derived from the absence of a bad signal |

The last row is the one that matters most and is the whole reason slice 1 wrote the contract into its own
header: this repo's most-repeated defect shape is a multi-state signal whose unmeasured state inherits
the permissive branch. A test must **red if `5` is treated as fresh**, and the closed token set must
reject an unplanned value by **exact token match**, not a prefix test — `PASS*` accepts
`PASSthisNeverRan` (CLAUDE.md, the roborev verdict-scan precedent).

## D8 — Adopt PR #3842's shape rather than race it

PR #3842 (#3752) is open, +1995/−19, adding two fail-closed legs to `premerge-assert.sh` as sourced
helpers plus a structural `gh --json` scanner, and editing three of the five disclaimer locations this
change must touch. It is the same file, the same kind of leg, at the same time. This change therefore
**adopts its shape** — one sourced helper per leg, a closed verdict-token set matched token-exactly,
every could-not-measure state non-passing, the positional contract unchanged — and rebases onto it when
it lands. Convergent structure is what keeps the conflict textual rather than semantic.
