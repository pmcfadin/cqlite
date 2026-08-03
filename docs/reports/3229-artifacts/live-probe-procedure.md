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

**A daemon restart may also be required.** The daemon observed during this work had **4d15h** uptime,
and it could not be established whether it re-reads the root path's config **per job** or **caches it
at startup**. So after the merge, if the probe still shows the old behaviour, restart the daemon
before concluding anything about the configuration.

This is the same shape as the `required`-check property already recorded in CLAUDE.md — *`required`
evaluates the aggregator **and** the registry from the PR's **BASE** ref, so a registry/aggregator
change lands only after it merges.* Both are cases of **a change to the machinery that governs a PR's
own verification not applying to that PR**. Recognising the shape is the point: when you change a
config, workflow, or registry that a gate reads from somewhere other than your branch, assume your own
PR is evaluated under the OLD version and plan the demonstration for after the merge.

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
bash scripts/flow/roborev-review-scripts_placeholder 2>/dev/null || true   # (see next line)
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

If `census-exclusion:` reads `NOTICE (… excluded by a roborev built-in …)`, that is expected and
non-failing whenever the diff touches a lockfile or cache path — see the FAIL-vs-NOTICE rationale in
`scripts/flow/roborev-review.sh --help`.

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

`website/src/content/docs/_3229-root-anchoring-probe.json` MUST be **PRESENT** in the prompt actually
sent:

```bash
roborev show <job> --prompt | grep -F 'website/src/content/docs/_3229-root-anchoring-probe.json'
```

Why: `docs/**/*.json` contains an interior `/`, so roborev's `FormatExcludeArgs` passes it **VERBATIM**
— it is ROOT-ANCHORED at this repo's top-level `docs/` and cannot match a **nested** `docs` directory.

- **Present** ⇒ the disassembly-recovered algorithm is confirmed live.
- **ABSENT** ⇒ the port is **FALSIFIED**. That blocks, and is not an acceptable outcome to merely
  record: both the pattern list in `.roborev.toml` and the ported construction in
  `scripts/flow/roborev-review-oracles.sh` rest on this root-anchoring result.

This file is retained on the branch deliberately: unlike an executable under root `docs/`, a `.json`
under a **nested** `docs` directory survives **both** the old and the new configuration, so it does not
deadlock pre-merge and is live evidence either way.

## Version pinning

Everything here is pinned to **`roborev v0.61.2`** — the ported `FormatExcludeArgs`, the extracted
built-in deny-list, and the measured `config get` behaviour. Re-run this procedure and re-verify both
lists after any roborev version bump: an upstream change would silently invalidate them while every
summary block still read `PASS`.
