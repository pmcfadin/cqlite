# #3751 merge-time notice — the ONE re-certification visit for in-flight lanes

Prepared before the merge, posted to every open PR at merge time. This discharges tasks 6.1/6.2: the
owner's rollout condition was that whichever of #3751/#3752 merged SECOND must not force a second sweep,
and the coordination lead confirmed (2026-09-02) that with #3752's issue absorbed into #3162 and PR #3842
orphaned, **#3751's landing is the one sweep in-flight lanes pay.**

Post verbatim as a PR comment on each open PR. It is deliberately short: a notice nobody reads is not a
notice, and the remedy has to be copy-pasteable.

---

## 🔧 `premerge-assert.sh` gained a REQUIRED flag — one-line change to your closer's call

`scripts/flow/premerge-assert.sh` now requires `--c-verdict`, and **omitting it is exit 3** (a usage
failure, not a silent pass). If your lane is mid-endgame, your existing call needs one flag added:

```bash
# before
bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>]

# after — add this, and nothing else
bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>] \
    --c-verdict AUTO
```

**Use `AUTO` unless you have a captured verdict line.** `AUTO` measures from the certified tree whether
the C intent audit applies to your branch — it diffs `openspec/changes/` (excluding `archive/**`) between
the merge-base with `origin/main` and your certified sha. Two outcomes:

- **Your branch touches no OpenSpec change** ⇒ `c-verdict: NOT-APPLICABLE (no openspec change on branch)`,
  and nothing else changes for you. Most oracle-driven bug fixes land here.
- **Your branch carries an OpenSpec change** ⇒ C is REQUIRED, and the merge refuses until a verdict is
  recorded. Record it with:

```bash
bash scripts/flow/review-stage.sh open c --issue <N> --agent spec-auditor   # BEFORE spawning the auditor
#   → prints the report path AND a paste-ready clause; put that clause in the spawn prompt verbatim
bash scripts/flow/review-stage.sh verdict c --issue <N>                     # after it reports
```

Only `PASS` and `AUTHOR-PERFORMED` proceed. `AUTHOR-PERFORMED` prints under its own
`PREMERGE: C-VERDICT` line and is never folded into `PREMERGE: OK`, so a reader can see the audit was
done by the diff's author.

### Three things that will save you a confusing refusal

1. **`AUTO` is measured, not asserted.** There is deliberately no spelling of the flag that means "not
   applicable" — a supplied file can only carry a review-stage verdict token. Inapplicability is reachable
   only through `AUTO`'s measurement, and any failure to measure (no git, no `origin/main`, the certified
   commit absent) is `UNMEASURED`, which is treated as **REQUIRED**.
2. **Run it from the repository root, or use a recent build.** The routing pathspec is now root-anchored
   (`:(top)`); before that fix, invoking from a subdirectory measured an empty diff and mis-classified a
   design-routed branch as NOT-APPLICABLE.
3. **A stage opened before a later commit will not certify it.** The stage record binds the HEAD it was
   opened at, so if you commit after recording C, re-open at the new commit
   (`open c --issue <N> --agent spec-auditor --force`) and re-run the audit. That is deliberate: a C audit
   of an older tree may not certify a newer one, which is the gate-of-record rule applied to intent.

### If you hit something else

`review-stage.sh --help` and `premerge-assert.sh --help` are authoritative and live in the same file as
the code, so they cannot drift from it. Every refusal names its own cause and carries a remedy line —
if one does not, that is a bug worth filing, not a puzzle worth solving.

Background: #3751 (four read-only review subagents idled without reporting in one delivery — an absent
review must never read as a clean one). Declared residual, lead-accepted: **#3929, whose scope is the
TOCTOU WINDOW between a check and the open that follows it, and nothing wider** — a symlinked or
unsearchable path COMPONENT needs no race and is refused outright (#3751 round 20).
