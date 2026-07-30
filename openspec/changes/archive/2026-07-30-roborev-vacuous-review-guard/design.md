# Design: fail-closed roborev vacuous-review guard (issue #2964)

## Context

`roborev` is the pipeline's code-review stage. Its verdict is a **merge condition**: `flow-implement`
runs it review-first on the lite-green diff (#2086), and `flow-closer` runs a final confirmation pass
before arming `gh pr merge --auto` (#2084/#2667). "roborev clean" is therefore load-bearing — if it can
be satisfied without a review having happened, the pipeline merges unreviewed code with no red anywhere.

**FOUR** confirmed triggers produce exactly that (evidence table in `proposal.md`; T4 was found by this
change's own live probe, in round 5):

| # | Trigger | Enqueued `git_ref` | Detectable by a SHA check? |
|---|---------|--------------------|----------------------------|
| 1 | worktree + `--branch` **without `--repo`** resolves against the ROOT checkout (on `main`) | `origin/main` | **yes** |
| 2 | two-positional commit-range form anchors the range at git's EMPTY TREE | `4b825dc6…..<head>` | **yes** |
| 3 | code-free (docs-only) diff structurally discarded | correct SHA | **NO** |
| 4 | single-SHA form reviews ONE COMMIT, not the branch | `<head40>` — *correct*, and still partial | **NO** (it EQUALS HEAD) |

T4 is the one the issue's own AC2 prescribed, and the nastiest of the four: the enqueued sha is exactly
branch HEAD, so every sha-equality check passes while the reviewer saw only the last commit. On a
17-commit branch it delivered 3 of 5 code files.

### The measured invocation matrix (round 5, real daemon)

One branch (17 commits, census 27 files = 22 markdown + 5 code), four invocation forms, measuring the
enqueued `git_ref` AND which files appear as `diff --git` headers in the prompt actually sent:

| form | enqueued `git_ref` | code files in prompt |
|---|---|---|
| `--branch --base <base> --repo <abs>` | `<base40>..<head40>` | **5/5 — SANCTIONED** |
| `--since <base> --repo <abs>` | `<base40>..<head40>` | 5/5 (byte-identical prompt) |
| `<base> <head>` (two positionals) | `4b825dc6…`(git EMPTY-TREE)`..<head40>` | 3/5 BROKEN |
| `<sha>` (single commit) | `<head40>` | 3/5 PARTIAL — one commit |

Three conclusions this document exists to preserve: **(1)** `--repo` is what makes `--branch` correct from
a worktree (with it, roborev reported "17 commits since origin/main"), so the defect was never `--branch`
— it was `--branch` *without* `--repo`; **(2)** the single-SHA form is a partial review reported as a
complete one, so we implement AC2's INTENT (reviewed content must match the requested range) rather than
its letter; **(3)** all 22 markdown files were ABSENT from the prompt while all 5 code files were present
— roborev **excludes non-code paths from the diff it constructs**, which is the mechanism behind T3 (for a
markdown-only diff the constructed diff is genuinely empty, so "contains no code changes to review" is a
truthful report of an empty input, not a malfunction) and the reason `prompt-content` checks the CODE
subset of the census.

Two structural facts constrain every option:

- **`roborev` is an external binary** (`/usr/local/bin/roborev`), not vendored in this repo. We cannot
  change its resolution logic or its exit codes.
- **A vacuous verdict is textually identical to a genuine clean one** at the top level ("No issues
  found"). Distinguishing them requires evidence obtained OUTSIDE the reviewer's own narration.

So the guard must be a **CQLite-side wrapper**, and it must judge the reviewer's claims against
something computed **locally**, not against the reviewer's own prose.

### The thesis the implementation converged on

The first cut was *prose-primary*: match the reviewer's "no code changes" sentence, with token
thresholds corroborating. Four rounds of review found that this is the wrong axis, for a reason worth
recording: **prose matching can only see the half of the defect space where the reviewer TELLS you it
saw nothing.** The other half — a reviewer that never received the diff, a job that never finished, a
provider 400, a `failed` status — emits no phrase at all, and "no bad phrase" was being read as proof
of a review. Every one of those reached `RESULT: PASS`.

The final architecture is therefore **DETERMINISTIC-PRIMARY**. The verdict is carried by checks judged
against data the wrapper obtains itself:

| Check | Its own oracle | Which half it covers |
|---|---|---|
| `push-assert` | the REMOTE, via `git ls-remote` | the reviewer can only see what the remote has |
| `census-check` | our own `git diff --numstat --no-renames` | what MUST be reviewed |
| `code-free` | our own classification of that census | T3, before a review is even enqueued |
| `sha-assert` | the job record's structured `git_ref` (BOTH range endpoints) | T1/T2/T4 — the wrong or partial scope was reviewed |
| `review-completed` | job `status` + an allow-list of terminal verdict markers | a review actually FINISHED |
| `prompt-content` | our census's own paths inside the prompt SENT | the reviewer never GOT the diff |

Prose (`vacuity-tier1`) and token accounting (`vacuity-tier2`) sit on top as **corroboration**. A PASS
now requires POSITIVE evidence; it is never inferred from the absence of a bad phrase.

## Recommended design (as built)

### `scripts/flow/roborev-review.sh` — the ONLY sanctioned roborev invocation

Flags: `--agent` and `--model` (**both required**), `--repo <path>` (default: `git rev-parse
--show-toplevel` of `$PWD`, absolutised), `--base <ref>` (default `origin/main`), `--log <path>`.
An option given an EMPTY value is a usage error, never a silent fallback to the default — `--repo ""`
falling back to `$PWD` is exactly how a caller reviews a repository it did not name. Every step is
ordered and fail-closed; the first hard failure stops the run, emits the summary block, and exits
non-zero.

Implementation is **five files**, split for size hygiene and one responsibility each:

- `scripts/flow/roborev-review.sh` (752 lines) — flags, identity, invocation, the range/job-record asserts,
  the block.
- `scripts/flow/roborev-review-oracles.sh` (227) — **sourced**: `roborev_push_assert` + `roborev_census`
  (including the code-free classification). These two are the change's whole thesis in code, and both
  learned the "never trust a local proxy" lesson the hard way (below).
- `scripts/flow/roborev-review-checks.sh` (342) — **sourced**: the five per-review checks
  (`review-completed`, `prompt-content`, `findings`/`roborev-exit`, tier 1, tier 2). Split out when the
  wrapper hit 998 lines. Division of labour: the ORACLES file answers *what must be reviewed, and is it
  even reviewable*, from data we obtain ourselves; the CHECKS file answers *did a review of that actually
  happen*, from the job record and the transcript.
- `scripts/flow/roborev-job-facts.py` (203) — JSON decoding of the job record: `git_ref`, `status`,
  `model`/`requested_model`, `verdict`, the prompt, and the token counts with their three-state extraction.
- `scripts/tests/test_roborev_review_guard.sh` (1628) — the hermetic regression check.

Both sourced paths are resolved from `BASH_SOURCE` (never `$PWD` — the wrapper runs from arbitrary
worktrees), and the wrapper **FAILs CLOSED** when either is missing OR truncated (the test is that every
required function is actually defined). Both are validated **BEFORE the review is invoked**, so a broken
installation costs no review — even though the checks file's functions are not called until after the job
facts exist. A silently absent helper would leave every key it owns reading `SKIP`/`PASS` beside a
`RESULT: PASS`, which is a worse failure than any this guard was built to catch.

**Step 1 — Resolve identity.** Repo root (**absolute** — `roborev --repo` must never receive a relative
path), branch, full 40-char HEAD. `--repo` is always passed explicitly; the wrapper never relies on
`roborev` inferring the repo from `$PWD` (that inference is trigger 1). `symbolic-ref -q` /
`rev-parse --verify --quiet` are used deliberately: `rev-parse --abbrev-ref HEAD` ECHOES the literal
string `HEAD` in a repo with no commits, so a `|| fallback` concatenated onto real-looking values and
the no-commit guard never fired.

**Step 2 — Push assert (AC3), asking the REMOTE.** `git ls-remote --heads <remote> <branch>` is the
oracle; the remote comes from the branch's configured upstream, else `origin`. Four distinct, correctly
attributed failures: detached HEAD, `ls-remote` failure (**infra/auth — explicitly NOT "never
pushed"**, since `git` and `gh` are separate credential paths, #2942), branch absent on the remote, and
remote tip ≠ HEAD (naming the unpushed commits, or the divergence). This runs **before** the census so
the operator gets the actionable cause ("push your commits") rather than a downstream vacuity FAIL.

**There is no local mirror-ref fast path** — see *Alternatives* 7/8: the fleet's narrow fetch refspec
means the mirror ref never exists, and a cached one survives a force-push or a branch deletion.

**Step 3 — Local diff census: the oracle.** `git diff --numstat --no-renames <base>...HEAD` → files,
lines added, lines removed, and the list of paths. `--no-renames` is deliberate: with rename detection a
renamed file renders as the composite `dir/{old => new}.rs`, which is not a real path and so could never
be found in the reviewer's prompt (step 6b matches literal paths). Three distinguishable failure
states, none of which may alias to "genuinely empty":

- `FAIL (base '<ref>' unresolvable)` — a narrow-refspec clone that never fetched the base.
- `FAIL (git diff failed)` — the measurement never happened.
- `NOTHING-TO-REVIEW` (exit `3`) — genuinely 0 files, no review enqueued, explicitly **not** a pass and
  not recordable as "roborev clean".

"We could not tell" must never render as "there is nothing to look at".

**Step 3b — Code-free census: a DETERMINISTIC FAIL, pre-enqueue.** roborev structurally discards a
code-free diff, so a census that is entirely prose cannot be certified by roborev **at all** — and that
is a property of OUR census, measured locally, so it must not depend on the reviewer admitting it. The
previous revision computed this classification and used it only for attribution WORDING, so a docs-only
diff could reach `RESULT: PASS` whenever the verdict happened not to carry the phrase — violating this
change's own spec requirement and CLAUDE.md rule (4). Classification is by **file EXTENSION**
(`md markdown mdx txt rst adoc`) with a path assist limited to **extensionless** files under
`openspec/ docs/ website/ .claude/`. An earlier revision treated everything under `docs/`, `.github/`
or `.claude/` as prose, which misclassifies `docs/foo.py` and `.github/workflows/*.yml` — and now that
code-free is a FAIL condition, a false code-free classification is a **false FAIL**.

**Step 4 — Invoke over the CENSUS RANGE with an explicit repo (AC2's intent).**

```
roborev review --branch --base <base> --repo <abs-repo> --agent <a> --model <m> --wait
```

This reviews the RANGE `<base>..HEAD` — exactly the census — per the matrix above. **NEVER `--branch`
WITHOUT `--repo`** (T1). **NEVER the two-positional range form** (T2, empty-tree base). **NEVER a single
sha** (T4, one commit). This is a deliberate, recorded departure from AC2's literal text, which prescribed
the single-sha form; the AC's *intent* — the reviewed content must match the requested range — is what the
wrapper implements and asserts. Both `--agent` and `--model` are always passed — the wrapper refuses to run
with only one, preserving the #2433/#3037 trap (`--agent claude-code` alone inherits `review_model` from
`.roborev.toml` and fails as a silent-looking outage). Enforcing it in the wrapper converts that outage
into a usage error at the call site. The transcript goes to `--log`; stdout stays reserved for the block.

**Step 5 — Reviewed-RANGE assert (AC2's intent): the STRUCTURED field is the only oracle.** The job
record's `git_ref` for the sanctioned form is `<base40>..<head40>`, and **BOTH endpoints** are compared
full-sha to full-sha against the census range — strictly stronger than the single-sha equality it
replaces (it proves the scope neither stops short of the tip nor starts elsewhere). `reviewed-sha:`
therefore carries a RANGE, not a sha. Four failure shapes:
`FAIL (reviewed range does not match <base>...HEAD)` naming the offending endpoint (an empty-tree base is
called out as the two-positional signature); `FAIL (single-commit record, not the census range)` — which
fires **even when the single sha EQUALS HEAD**, because `prompt-content` matches PATHS, so a review of only
the last of several commits touching one file passes every path check while the earlier changes go
unreviewed; `FAIL (reviewed-sha does not match head-sha)` for a single sha that is not HEAD (attributed to
the base ref where it equals it); and `FAIL (job record unavailable — reviewed range unverifiable)`.

The stdout `Enqueued job <N> for <sha>` line is **demoted all the way to the carrier of the job id**: for
a RANGE review it announces only the range BASE, so it can establish nothing about HEAD, and the wrapper
therefore FAILS CLOSED when the record is unavailable rather than falling back to a check that verifies
nothing. Its absence is still a hard FAIL, because every structured query needs that id. Parsing stays
defensive: lower-cased before matching (so an upper-case announcement cannot survive the match and then
fall out of field extraction as garbage handed to `roborev show`), a **7**-hex-char floor (4 was loose
enough that a 4-char prefix satisfied the old assert), both fields validated, and with several
announcements the LAST is the effective enqueue with the multiplicity recorded as a NOTICE.

**Step 5a — `job-record:`, its own key.** Four asserts plus `model:` depend on the structured record, so
its completeness is reported rather than inferred: `PASS` / `PASS (no token accounting in the record)` /
`DEGRADED (incomplete after <n>s: <fields>)` / `SKIP`. `DEGRADED` is deliberately NON-failing — the
dependent asserts publish their own verdicts (notably `sha-assert: FAIL (job record unavailable …)`), so
nothing is silently weakened. **TWO SOURCES OF DIFFERENT SHAPE** are consulted and a source counts only
when it yields the fields the asserts need: `roborev show <job> --json` returns the **REVIEW** row (id,
agent, prompt — but no `git_ref`/`status`/`token_usage`) and NESTS the JOB row under a `job` key, while
`roborev list --json` returns the JOB row directly. Both rows answer to the same id, so returning the
first id match handed back the poorer row; the extractor now prefers an id match that actually carries
`git_ref`. **There was never an async/durability problem** — an earlier round diagnosed one from exactly
this wrong-row read, and that diagnosis is retracted here: with the nested row read as a first-class source
the record is complete in ONE read, so the bounded poll is a **5×1s sanity retry** (down from 45×1s), not a
wait. Shortening it can only make the record MORE likely to read `DEGRADED` — the fail-closed direction.

**Step 5b — `model:`.** `requested_model` ≠ `model` is surfaced as a **loud NOTICE, not a FAIL**: a
model-alias resolution is legitimate, so a mismatch is not by itself evidence of a bad review, and an
always-red guard is the failure mode that gets guards bypassed (this change hit that twice). Review
integrity is carried by the deterministic checks; this line exists so a substitution can never happen
SILENTLY.

**Step 6a — `review-completed:` — POSITIVE evidence a review HAPPENED.** Required before PASS is
reachable: the job `status` must not be a value other than `done`, AND the transcript must carry a
**terminal verdict marker** from an allow-list (a severity-tagged findings marker, or the clean shape:
"no issues found" AND a `Summary:` line). Absence ⇒ FAIL CLOSED. This is the inverse of the original
logic and closes the worst defect found in review: a transcript showing only "waiting for job N", a
`400 … model is not supported`, or `status: failed (provider timeout)` each contain no vacuous phrase
and all three used to PASS. When `status` is unavailable, completion may rest on the marker alone — with
a NOTICE naming it as the weaker of the two signals, never a silent upgrade to `done`.

**Step 6b — `prompt-content:` — DETERMINISTIC, threshold-free.** Reads the prompt actually sent to the
agent (the job record's `prompt`, else `roborev show <job> --prompt`) and looks for the **CODE subset** of
our census's paths in it. This is the deterministic complement of tier 1: tier 1 catches "the reviewer GOT
the diff and discarded it"; this catches "the reviewer never GOT the diff". Three properties, each of which
replaced a reproduced defect:

- **The CODE subset, not every path** — roborev excludes non-code paths from the diff it builds (measured
  22 markdown absent / 5 code present), so requiring all 27 would false-FAIL every documentation-touching
  branch, i.e. most of them.
- **Every code path, no sampling cap** — the former `PROMPT_CONTENT_MAX_PATHS_CHECKED=40` even-sampling
  bound was a hole: a partial prompt naming just the sampled files passed. Exact-header matching is cheap
  even for a 500-file diff, so the cap is gone.
- **Both header SIDES, compared whole-line** — the census runs `--no-renames` (a rename = two paths) while
  the reviewer's diff may have rename detection ON (one `diff --git a/old b/new` header). Same-path-only
  matching FALSELY REJECTED every review containing a detected rename; collecting the path set from both
  sides reconciles the two behaviours without weakening exact-header strictness to a substring test (a
  substring is satisfied by any incidental mention — including this wrapper quoting a path in a comment).

**An unretrievable (whitespace-only) prompt now FAILS** — `FAIL (prompt unretrievable — no evidence any
diff was delivered)`. The former non-failing `UNAVAILABLE` was a round-6 BLOCKER: with a non-empty code
census it allowed PASS with no authoritative evidence any diff reached the reviewer, which contradicts the
wrapper's whole purpose. It is not an always-red risk either — the prompt is measurably retrievable from
the record's `prompt` field AND from `roborev show <job> --prompt`, so an empty one is a real anomaly.
Note the value shapes: `PASS (n/n code census paths present)` vs
`FAIL (k/n code census paths absent …)` — same denominator, opposite numerator sense, so a grep-based
reader must read the word, not the ratio.

**Step 6c — `findings:` and `roborev-exit:`.** roborev **exits non-zero when it REPORTS FINDINGS**. The
original wording called every non-zero exit a reviewer malfunction, which is dangerous in the OPPOSITE
direction from the vacuity bug: an agent told the reviewer broke will retry or bypass instead of FIXING
the findings. So the exit is split — `FINDINGS (exit N)` when the review ran (structured `status` is the
authority) versus `ERROR (exit N)` when the reviewer itself failed — and the findings state gets its own
key (`NONE` / `PRESENT` / `PRESENT (n)` / `UNKNOWN` / `INCONSISTENT (…)` / `SKIP`). Both still force
`RESULT: FAIL` (a review with open findings is not "roborev clean"), but the FINDINGS message says
explicitly: the review is genuine, do not retry, do not bypass, triage and fix.

The PRESENT/NONE answer is derived from the **structured `verdict` field**, with the exit code as fallback,
because tier 1 is GATED on it: a regex over the whole transcript let an incidental or quoted `[Low]` set
`PRESENT` and thereby EXEMPT a genuinely vacuous verdict from tier 1's hard failure. Prose is consulted
only inside the FINDINGS BLOCK (a `Findings` heading/label through to a **line-initial** `Summary`
heading/label — matched mid-sentence, the terminator closed the block early and under-counted). The `(n)`
COUNT stays best-effort prose parsing reported for a human; the PRESENT/NONE/INCONSISTENT distinction is
the load-bearing part. A contradiction — a clean structured verdict, or a zero exit, beside in-block
severity markers — is `INCONSISTENT (verdict clean, n findings marker(s))` /
`INCONSISTENT (exit 0, n findings marker(s))`: both FAIL the run, and being neither `PRESENT` nor `NONE`
neither can exempt tier 1.

**Step 6d — `vacuity-tier1:` — AUTHORITATIVE, but ANCHORED and GATED.** The reviewer's own summary
claiming no code changes, against a census we measured as non-empty, is T3 and must FAIL, not merely
note. But the naive form false-FAILs: matched anywhere in the transcript, a genuine review that merely
QUOTED the phrase was failed as vacuous — and this change's own diff carries the phrase in five-plus
files. Two properties make the strict version safe:

1. **Anchoring to the whole summary BLOCK** — from a `Summary` HEADING or a `Summary:` label anywhere on a
   line, through to the next heading or EOF; never arbitrary finding bodies. No such region ⇒
   `UNAVAILABLE`. The earlier region — only the LINES containing `Summary:` — was a round-6 BLOCKER: the
   real format is `## Summary` / blank / prose, so the region held the heading and none of the prose, and a
   vacuous clean review whose "no code changes" sentence sat under the heading reported **PASS** — the
   exact defect the wrapper exists to stop. The block form is a strict superset of the line form, so the
   older single-line `No issues found. Summary: …` shape stays covered.
2. **Gating on `findings:`** — `NONE` ⇒ the reviewer is CLAIMING CLEANLINESS, so the phrase is a vacuity
   claim ⇒ **HARD FAIL**. `UNKNOWN` ⇒ **HARD FAIL** too: an unknowable state must never DISARM the
   check. `PRESENT*` ⇒ the reviewer demonstrably analysed the diff, so the phrase is discussion ⇒
   advisory `NOTICE`, which does not fail the run.

Why the relaxation is recorded rather than silent: the systemic cost of a false FAIL here is agents
learning to **WAIVE tier-1 FAILs**, which restores the original defect wholesale. A guard that cries
wolf is a guard that gets bypassed.

**Step 6e — `vacuity-tier2:` — corroborating, fail-closed only, and now actually alive.** The tier was
**permanently `UNAVAILABLE` on every real run** before this fix: the real payload's `token_usage` is a
**JSON-ENCODED STRING** (it must be decoded TWICE) and the output count is `total_output_tokens`, not
`output_tokens` — so a single decode read no counts, reported a non-failing `UNAVAILABLE`, and the guard
silently was not there. Hence three explicit states in `roborev-job-facts.py`:

| state | meaning | wrapper |
|---|---|---|
| `absent` | no token field at all | `UNAVAILABLE` (legitimate build difference) |
| `parsed` | counts readable | evaluate the thresholds |
| `unparseable` | a token field IS present, no alias resolved | **`FAIL` (drift)** |

`unparseable` is a FAIL, not a notice, precisely because that is how the tier was disarmed. The remedy
in the message is "add the alias to `INPUT/CACHED/OUTPUT_TOKEN_KEYS`", never "waive it". A drift FAIL
costs one re-run; a silently disarmed guard costs an unreviewed merge.

Calibration, and the two deliberate changes from the original spec:

```
VACUOUS baseline:  17,333–18,801 input, 0 cached, 21–56 output, 8s   (jobs 4651/4658/4659)
GENUINE, SMALL:    67,387 input, 43,520 cached, 2,232 output, 68s    (20 files, +2279)
GENUINE, LARGE:    398k–649k input, 314k–554k cached, 5.0k–6.3k out  (jobs 4652/4654/4656)
```

- `ROBOREV_VACUITY_MIN_INPUT_TOKENS = 25000` — **lowered from the originally specified 50,000**, and
  re-anchored on the **vacuous ceiling** rather than the genuine band, because the genuine band scales
  with diff size. 25,000 is ~1.33× above the highest vacuous run (18,801) and ~2.7× below the smallest
  genuine one (67,387). The original 50,000 was only 1.35× below that 67k run — one modestly smaller
  genuine diff away from an always-red guard.
- **The output-token floor was DROPPED as a FAIL condition** (kept as an advisory NOTICE at 200). A
  genuine CLEAN review and a vacuous one emit near-identical output: both are "No issues found" plus
  one sentence (~20–60 tokens; vacuous measured 21–56). The counts COLLIDE, so an output floor would
  false-FAIL precisely the case we care most about — a real review that is legitimately clean. It cannot
  discriminate, so it is reported and never asserted.
- `cached_input_tokens == 0` **stays** a FAIL, with its false-positive caveat documented (a genuinely
  cold cache can report 0). It is an accepted trade in the fail-closed direction, affordable now that
  `prompt-content` gives a deterministic primary check — tier 2 is no longer the only thing standing
  between us and a vacuous pass. Wall time is deliberately NOT asserted (host-dependent, #2642).

**Step 7 — Emit a compact, machine-greppable summary block.** One field per line — a reader greps a
single `^<key>: ` anchor, never a column offset. The as-built block, in contract order (pinned by
`scripts/tests/test_roborev_review_guard.sh`):

```
==== ROBOREV REVIEW SUMMARY ====
repo: <abs>
branch: <name>
base: <ref>
head-sha: <sha40>|-
reviewed-sha: <base40>..<head40>   |  <sha40> (only if the record reports a single commit)  |  -
job: <N>|-
model: <m> | <m> (SUBSTITUTED — requested '<r>') | <m> (UNCONFIRMED — no model field in the job record) | -
census: <F> file(s), +<A>/-<D>   |  -
tokens: input=<i> cached=<c> output=<o|unknown>   |  UNAVAILABLE
push-assert:      PASS | SKIP | FAIL (detached HEAD) | FAIL (ls-remote failed: infra/auth)
                                    | FAIL (branch absent on remote <r>) | FAIL (unpushed commits)
census-check:     PASS | SKIP | FAIL (git diff failed) | FAIL (base '<ref>' unresolvable) | FAIL (empty census)
code-free:        PASS | SKIP | FAIL (code-free census: n/n files are documentation/specification text)
job-record:       PASS | PASS (no token accounting in the record) | SKIP
                                    | DEGRADED (incomplete after <n>s: <fields>)      <-- NON-failing
sha-assert:       PASS | SKIP | FAIL (roborev not on PATH) | FAIL (no parseable enqueue announcement)
                                    | FAIL (unparseable enqueue announcement)
                                    | FAIL (reviewed range does not match <base>...HEAD)
                                    | FAIL (single-commit record, not the census range)
                                    | FAIL (reviewed-sha does not match head-sha)
                                    | FAIL (job record unavailable — reviewed range unverifiable)
review-completed: PASS | SKIP | FAIL (transcript unreadable) | FAIL (job status '<s>' is not done)
                                    | FAIL (no terminal verdict marker)
prompt-content:   PASS (n/n code census paths present)
                                    | FAIL (k/n code census paths absent from the prompt)
                                    | FAIL (prompt unretrievable — no evidence any diff was delivered)
                                    | SKIP                                  <-- NO passing UNAVAILABLE
vacuity-tier1:    PASS | FAIL (vacuous verdict vs non-empty census)
                                    | NOTICE (phrase present in a findings-bearing review) | UNAVAILABLE | SKIP
vacuity-tier2:    PASS | FAIL (vacuous token signature)
                                    | FAIL (token accounting present but unparseable — drift) | UNAVAILABLE | SKIP
findings:         NONE | PRESENT | PRESENT (n) | UNKNOWN | SKIP
                                    | INCONSISTENT (verdict clean, n findings marker(s))
                                    | INCONSISTENT (exit 0, n findings marker(s))
roborev-exit:     PASS | FINDINGS (exit N) | ERROR (exit N) | SKIP
log: <transcript path>
RESULT: PASS|FAIL|NOTHING-TO-REVIEW
```

Note `sha-assert: FAIL (roborev not on PATH)`: an absent binary is a pre-enqueue fail-closed condition
reported under the assert that would have consumed the announcement. Every per-check key participates in
ONE scan: a value starting `FAIL` / `FINDINGS` / `ERROR` / `INCONSISTENT` fails the run;
`PASS*` / `SKIP` / `UNAVAILABLE` / `NOTICE*` / `DEGRADED*` never do (NOTICE is the advisory tier's value;
DEGRADED reports an incomplete job record whose consequences are carried by the dependent asserts). A key
whose step was never reached reads `SKIP` — never a blank, and never mistakable for a pass.

Exit codes: `0` PASS, `1` FAIL, `3` NOTHING-TO-REVIEW, `2` **usage error**. Exit 2 is deliberately NOT a
verdict: it emits **no** summary block at all (a loud `ERROR:` naming the missing option, before any repo
identity is resolved and before anything is enqueued), because a `RESULT:` line for a run that never
happened would alias a usage error onto one of the three real outcomes — recreating the very
indistinguishability this change exists to eliminate. `--help` (exit 0) is likewise not a verdict. An
unexpected mid-run abort is caught by an `EXIT` trap that still emits the block with `RESULT: FAIL`: a
run that died without a verdict must never look like a run that was never made. Modeled on the gate's
summary-file contract: **an agent retains only this block, never raw roborev stdout**. The block name is
distinct from `AGENT-GATE SUMMARY` / `… LITE …` / `… DELTA …` so neither can be pasted as the other.

**Step 8 — Hygiene.** No wall-clock threshold asserts in the regression test's correctness path (#2642,
mechanized by `roborev-lints`); no unquoted interpolation of external output into a shell command;
`shellcheck 0.10.0` clean at info level with `-x` across all three shell files (the `-x` matters — it is
what resolves the sourced-fragment variables across the source boundary).

### Docs-only diffs cannot be roborev-certified

Trigger 3 is not a bug we can route around: roborev **structurally discards a code-free diff**. So the
sanctioned position is explicit — a docs/spec-only diff **cannot be certified by roborev at all**. The
wrapper FAILs it deterministically at `code-free:` **before enqueuing anything** (the reviewer's prose is
not involved), and the sanctioned substitute is **verification against primary sources**, recorded in the
PR body. For #2950 that was `git show cassandra-5.0.8:<path>`. A docs-only PR must **never** record
"roborev clean".

This dovetails with existing doctrine rather than fighting it: CLAUDE.md already treats a docs-only diff
specially for the gate (the #3042 CITE-AND-WAIVE rule). The parallel is deliberate — a docs-only diff
cannot be *certified* by a tool whose subject is compiled code, in either the gate's or roborev's case.

### Call-site migration — SIXTEEN surfaces, three obligation classes

Every roborev invocation in the agent surfaces and the fleet-facing doctrine routes through the wrapper,
and bare `--branch` becomes non-sanctioned prose. The surfaces are NOT homogeneous, and conflating them
would state an obligation four of them cannot satisfy:

- **Review-round sites (4)** — run a round themselves; must also state push-first and
  non-PASS-is-a-failed-round: `.claude/skills/flow-implement/SKILL.md` (the review-first step — the
  primary call site; *previously* documented the now **NON-SANCTIONED, historical**
  `roborev review --branch --base origin/main --agent codex --model gpt-5.6-sol --wait`),
  `.claude/agents/flow-closer.md` (the final confirmation pass — the **merge-gating** call site;
  *previously* documented a `/roborev-review-branch` form that does not exist),
  `.claude/skills/flow-address/SKILL.md` (the post-comment re-review), and `.claude/commands/worker.md`
  (the fleet's **unattended entry point**, which runs the implement loop's review step itself).
- **Prescribing sites (5)** — name the wrapper without running a round in-line:
  `.claude/agents/flow-lead.md` (stage table + doctrine bullet),
  `.claude/skills/ci-cd-validation/SKILL.md` + `.claude/skills/ci-cd-validation/merge-process.md`
  (the merge-readiness definition), `.claude/skills/flow-activate/SKILL.md` (the `tasks.md` it authors),
  and `.claude/commands/manager.md` (what "roborev clean" means for the workers it dispatches).
- **Non-invoking surfaces (4)** — no roborev invocation at all; each must SAY so and point at the
  wrapper: `.claude/skills/flow-finalize/SKILL.md` (the telemetry `--roborev-findings` counter + what
  "roborev clean" means in the ledger), `.claude/agents/rust-reviewer.md` (pre-roborev self-check
  classes — and it additionally flags a reintroduced bare `--branch`/range form as a **BLOCKER**),
  `.claude/agents/sstable-developer.md` and `.claude/agents/test-validator.md` (the `roborev-lints`
  lite component + never-invoke-directly).
- **Non-`.claude` doctrine surfaces (3)** — `website/src/content/docs/agents-developing/delivery-pipeline.md`,
  `docs/development/pm-operating-loop.md`, `docs/development/agent-machine-setup.md`. These were the
  most important discovery of the migration sweep: each carried the **INVERSE** instruction — "roborev
  follows this machine's configured agent … run it with no `--agent`/`--model` flags", with
  `delivery-pipeline.md` calling explicit agent/model **"never doctrine"**. Leaving them would have left
  the fleet's *published* guidance prescribing exactly the invocation this change forbids.

Plus the two AC4 doctrine surfaces (CLAUDE.md and the `roborev-findings` page) = **18 files** referencing
the wrapper. `.claude/hooks/issue-gate.sh` is deliberately out of scope: it already documents that no
hook path runs roborev at all (#2671) and has no invocation to migrate.

### Regression check + gate wiring

`scripts/tests/test_roborev_review_guard.sh` (1628 lines) — **hermetic**: a **stub `roborev`** first on
`PATH` replays the recorded real outputs (enqueue lines, verdict text, and the `show --json` payload
*including* the doubly-encoded `token_usage` string with `total_output_tokens`, and the REVIEW row that
NESTS the job row under a `job` key — reproducing those shapes verbatim is what keeps tier 2 and the
job-record read honest), driven against throwaway `git init` fixtures each with its own
local bare `origin`. Fixture modes cover the fleet's real topologies: wide/`narrow` refspec, behind,
`narrow-upstream` (a remote named `upstream`), `unreachable` (ls-remote fails), `no-base`,
`deleted-remote` (mirror ref equals HEAD, branch deleted), docs-only, mixed, workflow-yaml, and
`renamed` (a rename the census sees as two paths). No network,
no real roborev, no datasets, no cargo. **329 assertions; deliberate wrapper mutations killed 27/27 in the
round-5 batch and 15/15 in the round-6 batch** (the one survivor found there — the nested-job-row read —
is now pinned by case x10). A fixture-integrity guard fails if a narrow-refspec fixture ever grows a
feature mirror ref — otherwise it would silently stop testing the condition it exists for. python3 absence
is a loud SKIP for the structured-payload cases, never a silent pass. The tally line deliberately does NOT
start with `RESULT:`, a token that belongs to the gate's summary contract and the wrapper's own block.

**Wiring (concrete).** Both registration points in `scripts/agent-gate.sh` are used:

- `run_roborev_lints_cmd()` (component `roborev-lints`) is in **both** `LITE_COMPONENTS` and
  `COMPONENTS`, and already chains `check-workflow-injection.sh && check-no-wallclock-asserts.sh`. The
  guard test is appended there — this is what makes a regression **FAIL the fast `--lite` loop** rather
  than cost a review round, and it is thematically exact: `roborev-lints` is the component that
  mechanizes roborev-related delivery costs (#2656).
- `run_tooling_tests()` (the full-gate `tooling-tests` component) runs it too, following the
  `test_check_dockerfile_rust_pin.sh` / `test_check_skill_flag_tables.sh` guard pattern (FAIL the
  component on non-zero, with a named failure line).

**Live worktree probe (documented, not gate-run).** From a real `issue-<N>-*` worktree with a pushed
commit and the root checkout on `main`, run the wrapper and confirm the reviewed scope COVERS the worktree
HEAD: with the sanctioned range invocation `reviewed-sha:` is `<base40>..<head40>`, so the assertion is on
the range's HEAD endpoint (plus `sha-assert: PASS`, which is only reachable when both endpoints match), and
a `reviewed-sha` that is the base alone means the explicit `--repo` did not defeat the root-checkout
resolution. Only this probe can prove the real external binary honours the explicit `--repo`; it needs
network + a live reviewer, so it lives in the wrapper's `--help` text (so procedure and implementation
cannot drift) and the doctrine page, with an instruction to re-run it after any roborev version bump.

**Known residual (recorded, not hidden).** The `--help` probe text still phrases step 3 as
`reviewed-sha == head-sha (prefix match)`, wording that predates the range form and which the pinned
regression case asserts verbatim; the corrected RANGE phrasing has landed on the doctrine page. Both should
converge on the range form in a follow-up touch of `scripts/flow/roborev-review.sh` + its test — it is a
documentation-only staleness (no assert depends on it), but it is exactly the kind of drift this section
exists to name.

### Doctrine (ships in this change)

CLAUDE.md's roborev-invocation bullet (*Agent-Team Conventions*) and
`website/src/content/docs/agents-developing/roborev-findings.md` both state: the wrapper is the only
sanctioned invocation; **verify the reviewed SCOPE against the census range**; `"contains no code changes to
review"` on a non-empty diff is a **HARD FAIL**; docs-only diffs cannot be roborev-certified — plus the
exit-code contract and "any non-PASS `RESULT`, `NOTHING-TO-REVIEW` included, is a blocked merge".

**The three measured corrections land on every surface that states the rule** (CLAUDE.md,
`roborev-findings.md`, `delivery-pipeline.md`, `docs/development/pm-operating-loop.md`,
`docs/development/agent-machine-setup.md`), because the previous wording FORBADE the form now known to be
correct: (a) the non-sanctioned form is `--branch` **WITHOUT** an explicit `--repo`, not `--branch` as such;
(b) the single-SHA form reviews ONE COMMIT — a fourth vacuity class, and the form AC2 literally asked for;
(c) roborev EXCLUDES non-code paths from the diff it builds, which is why a markdown-only diff yields a
genuinely empty input (a truthful "no code changes" report), why `prompt-content` checks the CODE subset,
and why the deterministic pre-enqueue `code-free:` FAIL is the right answer. The block's documented keys
gain `job-record:` and the corrected `prompt-content:` values, and the live-probe section is restated in the
range form. The page also
gains a row in its **mechanized-in-`--lite`** table for the new guard (a mechanized class absent from
that table gets hand-checked forever). Publication acceptance follows CLAUDE.md's rule: **grep the
served page for a new distinctive phrase**; a `200` is not proof (observed CDN staleness ≈3 minutes).

## Why the token thresholds do NOT violate the no-heuristics mandate (#28)

Worth stating plainly, because "thresholds" reads like a heuristic at a glance. The no-heuristics
mandate governs **on-disk TYPE/format inference from byte patterns in the SSTable read path** — never
guessing a CQL type or a format version from data content instead of authoritative metadata (schema,
else `Statistics.db`). It says nothing about, and is not weakened by, **review-tooling liveness
detection**, which is a property of our own CI process, not of Cassandra's on-disk format.

Three structural properties keep this clean anyway, and the final architecture strengthens all three:

1. **The verdict is carried by DETERMINISTIC, threshold-free checks** compared against locally obtained
   oracles — the review-tooling analogue of "authoritative metadata only". Token accounting is the
   third-ranked signal, not the primary one.
2. **Tier 2's only possible action is to FAIL CLOSED.** It can never manufacture a pass, relax another
   check's FAIL, or infer that a review *did* happen. Its worst-case error is a false FAIL, whose cost
   is one re-run — bounded, visible and self-correcting, and preferable to the status quo, whose failure
   mode is merging unreviewed code. The `cached == 0` clause is the most false-positive-prone term and
   is an accepted, deliberate trade, recorded so a future recalibration knows it was a choice.
3. **The thresholds are evidenced, bounded and named**, with the measured table cited beside them, and
   every failure prints observed-vs-threshold — the opposite of an unexplained magic number. Where a
   threshold could NOT discriminate (output tokens), it was **dropped** rather than kept for
   appearances.

## Alternatives considered (and why the recommendation beat them)

1. **Register worktrees with `roborev repo` so `--branch` resolves correctly.** *Rejected:* there **is
   no `add` subcommand** — repos self-register on first use, so there is nothing to call. Even if there
   were, it would fix only T1: T2, T3 and T4 would still return an indistinguishable "No
   issues found".
2. **Patch/fork `roborev` upstream.** *Rejected for this change:* the binary is outside this repo's
   control and the fleet is exposed on **every** issue right now — a guard we own ships today. Noted as
   a possible **upstream follow-up** (worktree-aware `--branch`; a non-zero exit when a code-free diff
   is discarded). The wrapper stays correct and cheap after such a fix lands.
3. **Run roborev from the root checkout instead of the worktree.** *Rejected:* it breaks 1:1:1:1
   isolation and requires **commandeering the shared root** — explicitly forbidden (a closer that
   switched the shared root's branch once stranded root off `main` and broke every session on the box).
   It also serializes concurrent lanes onto one checkout.
4. **PROSE MATCHING AS THE PRIMARY SIGNAL** (the original design). *Tried and REJECTED with evidence:*
   it only sees the half of the defect space where the reviewer *tells* you it saw nothing. A job that
   never finished, a provider `400`, a `status: failed`, and a reviewer that never received the diff all
   emit **no phrase at all** — and all four reached `RESULT: PASS`, because absence of a phrase was
   treated as proof of a review. Replaced by `review-completed` (positive evidence) + `prompt-content`
   (deterministic "did it GET the diff") with the prose check retained, anchored and gated, as
   corroboration.
5. **Token-threshold-only detection.** *Rejected as a primary signal:* non-deterministic and
   recalibrating with every model/prompt change (`gpt-5.6-sol` is codex's own moving default, not a
   config pin — it already shifted once on a version bump). Demoted to a corroborating tier where a
   drifted threshold can only cost a re-run.
6. **An OUTPUT-token floor as a FAIL condition** (originally specified at 200). *Tried and REJECTED with
   evidence:* a genuine CLEAN review and a vacuous one emit near-identical output counts (~20–60 for
   both; vacuous measured 21–56), so the signal cannot discriminate them and the floor would false-FAIL
   precisely the case that matters most. Kept as an advisory NOTICE only.
7. **A local mirror-ref (`refs/remotes/origin/<branch>`) push assert.** *Tried and REJECTED with
   evidence:* CQLite clones carry a NARROW fetch refspec
   (`+refs/heads/main:refs/remotes/origin/main`), so a feature branch's mirror ref is **never** created
   however often the branch is pushed. The first implementation false-FAILed **100% of the fleet** —
   which would have made the only sanctioned invocation unusable and pushed agents straight back to the
   bare `--branch` form. `git ls-remote` asks the REMOTE, which is the authority.
8. **A mirror-ref FAST PATH in front of `ls-remote`** (short-circuit when the cached ref happens to
   equal HEAD). *Tried and REJECTED with evidence:* a cached ref survives a **force-push or an outright
   deletion** of the remote branch, so it can equal HEAD while the remote no longer has the commit —
   enqueueing a review of a commit the reviewer cannot fetch, i.e. a vacuous-review setup. `ls-remote`
   costs ~1s, and not trusting local proxies is this wrapper's whole point. Removed entirely.
9. **Trust the verdict text.** *Rejected:* that is precisely the defect. "No issues found" is emitted
   identically whether 650k tokens of real diff were reviewed or zero bytes were.
10. **Detect the empty diff by asking roborev what it saw** (e.g. parse the job's reported diffstat).
    *Rejected:* it re-derives the answer from the same untrusted source. The census must be computed on
    our side with `git`, or the check is circular — the same reason a CQLite `file:line` is never format
    authority.
11. **Treat a `requested_model` ≠ `model` substitution as a FAIL.** *Rejected:* roborev legitimately
    resolves model aliases, so this would be red on healthy runs, and an always-red guard is the failure
    mode that gets guards bypassed (hit twice in this change's own rounds). Surfaced as a loud NOTICE
    instead, so a substitution can never be SILENT while integrity stays carried by the deterministic
    checks.
12. **Treat a present-but-unparseable token payload as a NOTICE.** *Rejected:* that is precisely how the
    tier was silently disarmed for its entire life before this fix (a double-encoded string and a
    renamed output field degraded it to a non-failing `UNAVAILABLE` while the real counts were the
    vacuous baseline). A drift FAIL costs one re-run after a one-line alias addition; a silently
    disarmed guard costs an unreviewed merge.
13. **The SINGLE-SHA invocation** (`roborev review <sha> --repo <abs>`) — *the form issue #2964's own AC2
    prescribes*. **Tried and REJECTED with measured evidence:** it enqueues `git_ref = <head40>`, i.e. ONE
    COMMIT, and delivered **3 of 5** census code files on a 17-commit branch. Every sha-equality check
    passes (the sha IS branch HEAD), so it is a PARTIAL review reported as a complete one — a fourth
    vacuity class. Worse, `prompt-content` matches PATHS, so when several commits touch the same file a
    review of only the last one satisfies every path check. Replaced by the range form, and the wrapper now
    FAILs a single-commit record even when it equals HEAD. AC2's intent is honoured; its letter is not.
14. **The TWO-POSITIONAL range form** (`roborev review <base> <head>`). **Tried and REJECTED with measured
    evidence:** the enqueued range base is git's **EMPTY-TREE** hash `4b825dc6…`, not `<base>`, and it
    delivered 3/5 code files. The empty-tree base is now called out by name in the mismatch message as
    this form's signature.
15. **`--branch` WITHOUT an explicit `--repo`** (the original defect, and — importantly — the thing the
    first draft of this doctrine banned *categorically*). **Rejected as an invocation, but the categorical
    ban was itself WRONG and is retracted here:** measured, `--branch` **with** `--repo <abs>` reports "17
    commits since origin/main" and delivers 5/5 code files, and is now the SANCTIONED form. The defect was
    always the MISSING `--repo` (which lets roborev resolve against the ROOT checkout, normally on the base
    branch). A doctrine that forbids `--branch` outright forbids the correct invocation — which is why the
    narrowing is propagated to every surface that states the rule.
16. **`prompt-content: UNAVAILABLE` as a NON-FAILING value for an unretrievable prompt.** *Tried and
    REJECTED (round-6 BLOCKER):* with a non-empty code census it let a run reach PASS with **no**
    authoritative evidence any diff reached the reviewer — a pass resting on nothing, which is the very
    thing this wrapper exists to prevent. It is also not a plausible always-red risk: the prompt is
    measurably retrievable from the job record's `prompt` field AND from `roborev show <job> --prompt`. Now
    `FAIL (prompt unretrievable — no evidence any diff was delivered)`.
17. **SAME-PATH-ONLY matching of `diff --git` headers in `prompt-content`.** *Tried and REJECTED (round-6
    BLOCKER):* our census runs `--no-renames` (a rename = two paths) while the reviewer's diff may have
    rename detection ON (one `a/old b/new` header), so same-path matching **falsely rejected every review
    containing a detected rename** — a false FAIL on ordinary work, which is how guards get waived. Fixed
    by collecting the path set from BOTH header sides and comparing whole-line; exact-header strictness is
    retained (no weakening to a substring test, which any incidental mention would satisfy).
18. **A tier-1 verdict region of only the LINES CONTAINING `Summary:`.** *Tried and REJECTED (round-6
    BLOCKER):* the real reviewer format is a `## Summary` HEADING followed by a blank line and the prose, so
    the region captured the heading and none of the sentence — and a **vacuous clean review whose "no code
    changes" claim sat under the heading reported PASS**, the exact defect the wrapper exists to stop. The
    region is now the whole summary BLOCK (heading or label → next heading/EOF), a strict superset of the
    line form.
19. **Diagnosing the incomplete job record as an ASYNCHRONOUS write** (and polling 45×1s for it). *Tried
    and REJECTED as a MISDIAGNOSIS:* the record was never late. `roborev show <job> --json` returns the
    REVIEW row and nests the JOB row under a `job` key, and the extractor was matching the outer row, which
    carries no `git_ref`/`status`/`token_usage`. Reading the nested row as a first-class source makes the
    record complete in ONE read; the poll is now a 5×1s **sanity retry**. Recorded because the wrong
    diagnosis had already been written into prose, and a "known" cause that is false costs the next reader
    more than an open question.
