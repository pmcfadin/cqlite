# #3229 AC2 — the live probe procedure (run AFTER this change merges)

This is the AC2 demonstration, recorded as **prose** rather than as an executable, and it is a
**post-merge** step. Both facts follow from one property of roborev, stated first because it is the
whole reason this file is not a `.sh`.

## Why this is post-merge, and why it is not an executable

**A `.roborev.toml` change cannot take effect for its own review.** roborev's daemon binds a
repository by its `repos.root_path` — the **ROOT checkout** — and resolves `exclude_patterns` from
**that** checkout's `.roborev.toml`, not from the linked worktree the wrapper is invoked in. Under
1:1:1:1 every issue runs in a worktree, so while this change is unmerged the root checkout still
carries the pre-change blanket `['docs/**', '*.md']`, and *that* is the set applied to this PR's own
review. The narrowed set becomes effective only once the change lands on `main` and the root checkout
is updated.

That has a sharp consequence for the original plan. An **executable committed under root `docs/`** —
which the earlier `probe-census-exclusion.sh` was, deliberately, as a self-demonstrating specimen —
is swallowed by the root checkout's `docs/**` for as long as the change is unmerged. The wrapper's
`census-exclusion:` check therefore FAILs, **correctly**, and no amount of re-running changes it:

```
census-exclusion: FAIL (1/7 code census paths excluded:
  docs/reports/3229-artifacts/probe-census-exclusion.sh by 'docs/**' [root-config])
```

A pre-merge demonstration of the narrowing was thus a **deadlock**, not a test: the specimen that
proves the fix is the specimen the unfixed configuration eats. So the executable was removed from the
branch and its procedure kept here, to be run once the ordering allows it. The requirement is not
dropped — it is rescheduled, and this file is where the result gets recorded.

**A daemon RESTART is also required.** The daemon **snapshots config at start**, so an edit to the
root checkout's `.roborev.toml` does not take effect for a running daemon — the one observed during
this work had **4d15h** uptime. Independently corroborated by #3234, whose single daemon restart
happened to precede every config edit it made and never followed one, which is why that investigation
measured `exclude_patterns` as having "no observable effect". So after the merge: update the root
checkout, **restart the daemon**, and only then run the probe.

Both of these properties have now cost real rounds — (1) a `census-exclusion: PASS` that certified a
config roborev never read, (2) a whole investigation's null result. They are not theoretical.

This is the same shape as the `required`-check property already recorded in CLAUDE.md — *`required`
evaluates the aggregator **and** the registry from the PR's **BASE** ref, so a registry/aggregator
change lands only after it merges.* Stated generally, and this is the form to remember:

> **Any PR whose subject is a config the daemon (or gate) reads from root cannot certify itself.**

When you change a config, workflow, or registry that a verifier reads from somewhere other than your
branch, assume your own PR is evaluated under the OLD version, and plan the demonstration for after
the merge.

## The PRIMARY evidence is a real PR, not this probe

**Do not treat this procedure as AC2's primary evidence.** The first post-merge PR that happens to
carry an executable under `docs/` demonstrates the fix end to end for free — #3234 ships harnesses
now, #3096's successor will, #3249's artifacts may — and that is *strictly better* evidence than a
probe written to pass: it proves the fix on a diff **nobody shaped for it**.

- **AC2's record** = that PR's `census-exclusion:` + `prompt-content:` lines, pasted into #3229.
- **This procedure** = the documented **fallback**, for when no such PR arrives promptly or the
  natural evidence is ambiguous.

### The named trigger — an unowned post-merge obligation is not an obligation

Post-merge intentions decay. #3232 existed only as prose in #3100's close; #3103 shipped while its
producer stayed uncommitted, after which three separate issues rebuilt a corpus. So this obligation
carries mechanism, not goodwill:

1. On merge, **#3229 goes to `In Review`, NOT `Done`** — `Done` auto-closes the issue and the
   obligation would vanish with it.
2. The PR is finalized and delivery telemetry is stamped as usual. Neither waits on this.
3. #3229 flips to **`Done` only once the AC2 evidence is posted** on the issue.
4. If the demonstration has not happened **within a few days**, it is **filed as a tracked issue** —
   never left to live in a comment thread.

## Procedure

Run from a real issue worktree on its own branch, with its commits pushed, **after** this change is on
`main` and the root checkout has been updated (`git -C <root> pull`):

```bash
# 0. Confirm the ROOT checkout carries the NARROWED set — this is the precondition.
grep -n exclude_patterns "$(git rev-parse --path-format=absolute --git-common-dir)/../.roborev.toml"
cd /path/to/cqlite-wt/issue-<N>            # a branch whose diff carries a docs/ executable
git push                                    # an unpushed commit is itself an empty-diff cause

# 1. The SANCTIONED invocation, unmodified: --agent AND --model, an explicit absolute
#    --repo (what makes --branch correct from a worktree), transcript to a log.
bash scripts/flow/roborev-review.sh \
  --agent codex --model claude-opus-5 \
  --repo "$(pwd -P)" --base origin/main \
  --log "${TMPDIR:-/tmp}/probe-3229.log" | tee "${TMPDIR:-/tmp}/probe-3229-summary.txt"

# 2. Extract exactly the lines AC2 asks to be recorded.
grep -E '^(census|code-free|census-exclusion|prompt-content|reviewed-sha|job|tokens|RESULT): ' \
  "${TMPDIR:-/tmp}/probe-3229-summary.txt"
```

Use `scripts/flow/roborev-review.sh` directly — there is no probe wrapper to maintain, and the
sanctioned invocation is the thing under test.

## Expected values

| Line | Expected |
|---|---|
| `census:` | the branch's `git diff --numstat --no-renames origin/main...HEAD` counts |
| `code-free:` | `PASS` — a `docs/` path prefix never makes a program documentation |
| `census-exclusion:` | `PASS (<n>/<n> code census paths survive the effective exclusion set; corroboration: OK)` |
| `prompt-content:` | `PASS (<n>/<n> code census paths present)` |
| `reviewed-sha:` | the RANGE `<base40>..<head40>`, head endpoint = branch HEAD |
| `tokens:` | above the mechanism's floor — see below |

`census-exclusion: PASS` **and** `prompt-content: PASS (<n>/<n>)` together are the demonstration: the
first says the configuration would not swallow the `docs/` executables, the second says the reviewer
actually received them. Either alone is insufficient.

A `RESULT` of `FINDINGS`/`FAIL` because the reviewer found real issues is **not** a probe failure: the
probe is about **scope** (did the reviewer receive the code?), not about the verdict.

If the probe diff touches a lockfile or cache path (`Cargo.lock`, `go.sum`, `pnpm-lock.yaml`, …), expect
`census-exclusion: PASS` (the guard does not model roborev's compiled-in deny-list) followed by
`prompt-content: FAIL` naming that path. That is the **declared residual**, not a probe failure — see the
residual section in `scripts/flow/roborev-review.sh --help` and issue #3278. Keep such paths out of the
probe diff if you want an unambiguous demonstration.

## Reading the token line — the mechanism's thresholds, not a memorised band

Judge the token triple against **the wrapper's own thresholds**, which are what any verdict is
actually computed from:

- **`input` ≥ `ROBOREV_VACUITY_MIN_INPUT_TOKENS` (25,000)** — the floor is anchored on the *highest
  observed vacuous run* (18,801), with headroom. Below it, tier 2 FAILs.
- **`cached` > 0** — a vacuous run measured exactly 0 cached.
- **`output` is ADVISORY ONLY, never a failure condition.** A genuine **clean** review emits roughly
  **20–60** output tokens, which is *indistinguishable* from the vacuous baseline's 53–56. Output
  therefore cannot be a realness test on its own, in either direction. This is already documented at
  `scripts/flow/roborev-review-checks.sh:328`.

**Do not treat 398k–649k input as a threshold.** Those figures were *observed on large diffs* and are
diff-size dependent. A real, substantive round measured during this work was
`input=118514 cached=88320 output=5954` on a ~90k-character prompt, with two substantive findings
citing real code — unambiguously genuine, and far below that band. An absolute floor set from
large-diff observations would falsely flag legitimate small diffs.

The **vacuous signature** to recognise is the shape, not a magnitude: `input` below the 25k floor with
`cached == 0` and a few dozen output tokens in a handful of seconds. PR #3222 measured
**15,443 in / 89 out** beside `prompt-content: FAIL (136/136 code census paths absent)`.

## The second, independent assertion — root anchoring

`website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` MUST be **PRESENT**
in the prompt actually sent:

```bash
roborev show <job> --prompt | grep -F 'website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json'
```

Why: the configured `docs/reports/*-artifacts/**/*.json` contains an interior `/`, so roborev's
`FormatExcludeArgs` passes it **VERBATIM** — it is ROOT-ANCHORED at this repo's top-level `docs/` and
cannot match a **nested** `docs` directory. The probe's path is chosen so the two candidate readings
DISAGREE about it: `**/docs/reports/*-artifacts/**/*.json` (the incorrect `**/`-prefixed form) matches
it, the root-anchored form does not. Its survival is therefore evidence, not decoration.

- **Present** ⇒ the disassembly-recovered algorithm is confirmed live.
- **ABSENT** ⇒ the port is **FALSIFIED**. That blocks, and is not an acceptable outcome to merely
  record: both the pattern list in `.roborev.toml` and the ported construction in
  `scripts/flow/roborev-review-oracles.sh` rest on this root-anchoring result.

This file is retained on the branch deliberately: unlike an executable under root `docs/`, a `.json`
under a **nested** `docs` directory is not swallowed by the configured set, so it does not deadlock
pre-merge and is live evidence either way.

**Round 9 relocated it, because it had gone vacuous.** It previously sat at
`website/src/content/docs/_3229-root-anchoring-probe.json` and discriminated against the pre-round-6
pattern `docs/**/*.json`. Round 6's directory-scoping (⑦a) removed that pattern, after which **no**
configured pattern matched the old path under **either** reading — so it survived unconditionally and
proved nothing. Discrimination was re-established with the guard's OWN port
(`roborev_format_exclude_args` + the same `git diff --name-only -- <exclude pathspecs>` survivor
query), run in both directions against a must-be-EXCLUDED control (`docs/reports/3229-artifacts/*.json`)
and a must-SURVIVE control (`scripts/flow/*.sh`). Do NOT use hand-rolled `git ls-files` +
`:(exclude,glob)` pathspecs as the oracle here: measured on this issue, an exclude pathspec combined
with a **literal file** pathspec returns 0 unconditionally, and `git ls-files -- 'website/'
':(exclude,glob)*.md'` returned 0 of 95 files — an answer that would have manufactured a config defect
that does not exist. **If a future round changes `exclude_patterns` again, re-run the discrimination
check**: a probe whose discriminating pattern has been deleted is vacuous evidence, and vacuous
evidence reads exactly like real evidence.

## Version pinning

Everything here is pinned to **`roborev v0.61.2`** — the ported `FormatExcludeArgs` and the measured
`config get` behaviour. Re-run this procedure and re-verify the port after any roborev version bump: an
upstream change would silently invalidate it while every summary block still read `PASS`.
