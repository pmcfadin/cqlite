# roborev review contract — the merge-gate wrapper

Deep reference for `scripts/flow/roborev-review.sh`. `CLAUDE.md` holds the **rules**; this file holds
the **mechanism and the history** — why each guard exists, what it measured, and which
false-PASS/false-FAIL it closed. Moved verbatim out of `CLAUDE.md` (#4092) so a 670-line postmortem
stops loading on every agent turn.

## Live open work in this subsystem

- **#3252 (P1, OPEN)** — roborev cannot certify a large diff on this fleet: above `max_prompt_size`
  the inline diff is swapped for a snapshot-file pointer, and `prompt-content:` then FAILs with text
  identical to the T1 worktree-bug signature. The guard is right; its diagnosis misleads. Token
  accounting is the tell.
- **#3283** — configured-exclusion modelling, deferred by owner ruling.

---

- **roborev invocation — `scripts/flow/roborev-review.sh` is the ONLY sanctioned call, and it requires
  BOTH `--agent` and `--model` (#2964/#2433/#3037).**
  `bash scripts/flow/roborev-review.sh --agent <agent> --model <model> [--repo <abs-path>] [--base <ref>] [--log <path>]`
  — codex is `--agent codex --model gpt-5.6-sol`; Claude is `--agent claude-code --model claude-opus-5`.
  `--repo` defaults to the toplevel of `$PWD` (resolved absolute), `--base` to `origin/main`. Retain ONLY
  its `==== ROBOREV REVIEW SUMMARY ====` block (header deliberately distinct from all three
  `AGENT-GATE *SUMMARY` blocks so neither can be pasted as the other), never the transcript — that goes
  to the `log:` path named in the block. Exit `0` PASS / `1` FAIL / `3` NOTHING-TO-REVIEW / `2` usage
  error; **any** non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed review round and
  a blocked merge, never "roborev clean". **"ROBOREV CLEAN" MEANS NO UNADDRESSED FINDINGS, NOT "THE TOOL
  PRINTED ZERO" (#3626)** — a LEAD-DEFERRED finding is re-reported by every later round, so the (correct,
  unwaivable) affirmative-`NONE` rule below blocked such a merge FOREVER; the route past it is a
  `roborev-defer: findings` authorization reported as `findings: DEFERRED (…)`, never `NONE`, and every
  OTHER non-PASS verdict still blocks exactly as before. Four rules: **(1)** the NON-SANCTIONED direct forms are
  `--branch` **WITHOUT** an explicit `--repo` (from a worktree it resolves against the ROOT checkout),
  the two-positional commit-range form (its range base is git's EMPTY TREE), and a SINGLE-SHA review (it
  covers ONE COMMIT, certifying a multi-commit branch from its last commit alone). `--repo` is what makes
  `--branch` correct, so the wrapper reviews the RANGE `--branch --base <base> --repo <abs>` — measured
  5/5 census code files delivered, vs 3/5 for the other two. **(2)** The **reviewed RANGE must be VERIFIED
  against `<base>...HEAD`** — the wrapper asserts BOTH endpoints from the **job record's structured
  fields** (`roborev list/show --json`; `git_ref` is `<base40>..<head40>`, echoed in `reviewed-sha:`
  beside a `job-record:` completeness key), with the stdout `Enqueued job <N> for <sha>` line DEMOTED to
  the job-id carrier: for a range review it names only the BASE, so an unavailable record FAILs rather
  than falling back to prose that verifies nothing. A range that does not match, a SINGLE-COMMIT record
  (even one equal to HEAD), or a base-equal scope **aborts the round** — base-equality is the signature of
  the worktree bug. **The expected RANGE BASE is the MERGE-BASE, never the base ref's TIP (#3392)**:
  `<base>...HEAD` *is* `merge-base(<base>, HEAD)..HEAD`, so an assert that expected the tip FAILED
  DETERMINISTICALLY on a CORRECT review of any branch whose `main` had advanced past its branch point —
  i.e. almost every branch not just rebased. It was misdiagnosed as a race **twice** (the falsifying
  control: `origin/main` recorded before AND after a failing round, unmoved). The tip is still read, for
  the T1 root-checkout signature alone, and the block now prints an informational
  `assert-base: <merge-base> (merge-base of <base> and HEAD; <base> tip <sha>)` so the two can never be
  confused in a pasted block. The absence waiver's `base=` field is bound to that same merge-base —
  copy it from `assert-base:`, not from `base:`. **(3)** `"contains no code changes to review"` on a
  NON-EMPTY diff is a **HARD FAIL**, never a pass. **(4)** A docs-only (code-free) diff **cannot be
  roborev-certified at all** — and "docs-only" means a **CODE-FREE CENSUS as the wrapper classifies it,
  NEVER a `docs/` path prefix** (#3229). The mechanism, stated correctly: **roborev drops exactly what
  its configured `exclude_patterns` pathspecs match — it makes NO code/non-code judgement.** The measured
  22-markdown-absent / 5-code-present split happened because `*.md` is CONFIGURED, not because the
  reviewer recognised prose, so for prose-only the constructed diff is genuinely EMPTY and that verdict is
  a truthful report of an empty input, not a malfunction. The wrapper's
  deterministic pre-enqueue `code-free:` check fails it before any review is enqueued, and
  `prompt-content:` therefore asserts the CODE subset of the census (an unretrievable prompt FAILs — there
  is no passing `UNAVAILABLE` there). The sanctioned substitute is
  primary-source verification recorded in the PR (e.g. `git show cassandra-5.0.8:<path>`), and no
  docs-only change may ever record "roborev clean".
  **The same mechanism cuts the other way, and did**: a configured `docs/**` discarded 33 EXECUTABLE
  measurement-harness files on PR #3222 — the `docs/reports/*-artifacts/` harnesses this repo ships **by
  convention are reviewed CODE**, so a PR carrying them is NOT a docs-only change and MUST be
  roborev-certified. The deny-list is now narrowed to `*.md` plus artifact extensions **scoped to
  artifact-bearing DIRECTORIES** (measured after the narrowing: 71 `docs/` executables reach the reviewer,
  0 markdown does, and nothing outside `docs/` is newly excluded). **NOTHING PREDICTS THE EXCLUSION SET
  PRE-ENQUEUE.** A `census-exclusion:` key that did — a bash port of roborev's `git.FormatExcludeArgs` over
  a TOML parse of three config sources — was built on #3229 and **REMOVED by owner ruling, deferred to
  #3283**: its false-PASS count was *increasing* across review rounds (1, 1, 2, 3), and two of the last
  round's three defects lived in code the two preceding fix rounds had just introduced. **A guard with
  known documented false-PASSes is worse than no guard, because it invites reliance it cannot support.**
  So a path the reviewer did not receive surfaces AFTER the review, under `prompt-content:`, fail-closed,
  with a cause that names the symptom rather than the mechanism — **if `prompt-content:` FAILs, suspect
  `.roborev.toml` first.** The class-level lesson, recorded for #3283: **a port is a second
  implementation, and a second implementation's correctness is only knowable by differential testing
  against the original** — the oracle re-derived Go's trim rules in bash and was tested against a *model*
  of Go, not against Go, so its NBSP divergence (Go's `unicode.IsSpace` trims U+00A0; bash trims do not)
  was unfindable by care. The narrowing's asymmetry is deliberate — **noise, never blindness** — but that claim is SCOPED, and the
  scope is the whole content of it: it holds for **inert dumps** (`.txt`/`.log`/`.err`), where exclusion
  costs only **noise** (a new artifact *directory* is re-admitted to review prompts, a token cost, while
  the swallow direction can only ever fail loudly). For a **code-bearing format**
  (`.json`/`.html`/`.svg`) exclusion is **BLINDNESS**, because such a file can be **functional
  configuration under any path**. So exclusion of code-bearing formats **MUST be scoped by directory,
  never by extension alone**. **This asymmetry was first written unqualified and THIS CHANGE falsified
  it (#3229):** an extension sweep across ALL of `docs/` was retired because `docs/**/*.json` hid
  `docs/observability/grafana/dashboards/cqlite-overview.json` — the gate's own `kit-dashboard-drift`
  component guards that dashboard, so the extension-wide form hid from the reviewer a file the gate
  treats as correctness-bearing — from the reviewer's diff *and* classified it code-free, i.e.
  unreviewable by construction; `docs/reports/delivery-telemetry.schema.json` went the same way. The
  durable generalisation: **an extension describes a FORMAT; a directory records an INTENT** — someone
  decided that tree holds artifacts — so a directory is the better proxy for "generated". So the
  patterns are `<artifact-dir-glob>/**/*.<ext>` over exactly four directories
  (`docs/reports/*-artifacts/`, `docs/round-artifacts/`, `docs/**/jfr-reports/`,
  `docs/sstables-definitive-guide/diagrams/`) and everything else under `docs/` is **reviewed**. Still
  extension-scoped *within* each directory, never a blanket `<dir>/**` — those directories hold the
  executable harnesses that ARE the census `docs/**` swallowed. The census-side mirror
  (`CODE_FREE_ARTIFACT_EXTENSIONS` / `CODE_FREE_ARTIFACT_DIR_GLOBS`) and the committed `.roborev.toml` are
  the same fact written twice and are **maintained BY HAND** — add an extension or a directory in both, in
  one edit. There is deliberately **no automated drift assert**: the one that existed depended on the
  removed TOML parser and went with it, so drift surfaces the slow way, as a `prompt-content:` FAIL on
  someone's report PR, until #3283 lands a guard whose own correctness is establishable. That gap is a
  **known reduction in coverage**, accepted, not argued away.
  **The verdict split follows ONE rule — apply it to any call of this shape without asking: FAIL where
  the author can act; NOTICE where only the information is actionable; never silence.** `NOTICE` stays
  outside the wrapper's failing-capable scan (`FAIL|FINDINGS|ERROR|INCONSISTENT`) because `vacuity-tier1:`
  needs it as an advisory.
  **NEITHER HALF OF ROBOREV'S EXCLUSION SET IS MODELLED (#3283 configured, #3278 compiled-in).** Beyond
  `exclude_patterns`, roborev appends a hard-coded lockfile/cache deny-list (`**/Cargo.lock`, `**/go.sum`,
  `**/pnpm-lock.yaml`, `**/.cache/**`, …) that no configuration can switch off. Modelling either half was
  built and then **DELETED on #3229**, and **subtraction cannot introduce a false PASS** — with nothing
  predicted, nothing is excused. So the residual, stated rather than left to be rediscovered: **a path
  roborev excludes by either half is silently dropped from the reviewer's diff, nothing names it
  pre-enqueue, and `prompt-content:` FAILs on its absence.** That **fails CLOSED** — the cost is a
  diagnostic whose stated cause names the symptom, not the mechanism. `prompt-content:` accordingly expects
  **every** census code path and subtracts nothing: no key is licensed to tell another which paths to skip.
  Also: **`prompt-content:` never prints a `0/0` PASS** — a key with no subject has no verdict to give.
  **`prompt-content:` ASKS ONE QUESTION, AND THERE IS NO DELIVERY CLASSIFIER (#3312, owner ruling (4)).** Are
  the census **CODE** paths present in the prompt the reviewer was sent? **Present ⇒ PASS. Absent ⇒ FAIL,
  unconditionally**, whatever caused it. The wrapper used to infer HOW roborev delivered the diff — inlined,
  or by a path to a **transient** `.roborev/roborev-snapshot-<id>/` file it deletes before `--wait` returns,
  or the delegated tier that ships neither and tells the reviewer to run git itself — and that inference
  produced **four consecutive High-severity false verdicts, in both directions**: a header set consulted
  before an oversize marker (a delegated review PASSing on repository-quoted headers), a candidate outliving
  its block, a real inline delivery under an unrecognised heading producing no evidence, and a block opener
  keyed on heading text that roborev treats as caller **data**. The instances differed; **the cause did not** —
  roborev's prompt embeds repository-controlled content (project guidelines/`AGENTS.md`, additional context,
  previous-review bodies) at column zero, indistinguishable from roborev's own text, so structure inferred
  from it is spoofable both ways. **No terminating marker exists** (the only structural one was roborev's
  fenced diff, and repository content can contain fences too), so the owner deleted the inference rather than
  patch a fifth instance. Gone with it: block detection, heading parsing, fence evidence, `mixed-delivery`,
  candidate lifetime, the snapshot/delegated distinction, the lexical path binding, snapshot-path extraction,
  the three `snapshot-*` keys, this key's `NOTICE` verdict and its exemption — so the **affirmation backstop
  again has no per-key escape hatch**. Every one of the four Highs is now *unexpressible*.
  **THE ACCEPTED COST, stated because it is real: a snapshot-delivered diff and a vacuous review that
  received NOTHING are IDENTICAL to the machine** — neither has census paths in its prompt, so both FAIL.
  What distinguishes them is a **human plus the review's token accounting** (genuine: 398k–649k input /
  314k–554k cached; vacuous baseline ~18.7k / 0). That trade was chosen over a machine guessing from
  injectable text.
  **THAT COST IS TRUE AT REVIEW TIME, AND FALSE AFTER THE FACT ONLY FOR A HUMAN READING THE STORED
  RECORD — IT STAYS TRUE OF THE MACHINE, AND MUST (#3654).** The prompt roborev SENT is RETAINED in the job record and retrievable
  later — `roborev show <id> --prompt` — even though the snapshot file it names is transient and
  already deleted, and a delivery-by-path prompt says so in its own words under `### Combined
  Diff`. That is the **DIRECT ARTIFACT** — roborev's ACTUAL prompt rather than a statistic about it —
  **IT IS NOT SELF-AUTHENTICATING, AND THE
  TRUST PROPERTIES RUN THE OPPOSITE WAY FROM THE OBVIOUS READING:** roborev's prompt EMBEDS
  repository-controlled content at positions indistinguishable from roborev's own text, so a
  reviewed branch can carry text MIMICKING that delivery wording — a human in the loop is not a
  channel separation, it is the same shared channel with a slower parser — so the prompt is read for
  the STRUCTURAL fact it reports, never as proof of its own provenance. The **token accounting is
  DAEMON-RECORDED BUT NOT INDEPENDENT, and it does not establish delivery either**: the RECORD is
  authentic (the branch cannot rewrite it), but the counts measure THE PROMPT, and the prompt embeds
  repository-controlled content — so a branch influences their MAGNITUDE without forging anything.
  That bites where the counts are used: the vacuous baseline is ~18.7k input / 0 cached, so padding
  non-diff prompt content can make a review that received NO diff look token-rich. **NEITHER SIGNAL
  ESTABLISHES PROVENANCE, and they are NOT INDEPENDENT** — both are functions of the same
  repository-influenced prompt. **Which evidence a waiver should rest on is an OPEN QUESTION,
  tracked as #3826** — nothing here recommends one signal over the other, or any ordering between
  them (an earlier revision of this paragraph asserted the counts were
  unwritable-and-therefore-corroborating; that was false in the half that mattered). **It resurrects nothing of the deleted delivery classifier,
  and that distinction is load-bearing rather than a caveat:** the classifier read injectable prompt
  text AT DECISION TIME to produce an AUTOMATED verdict, while this is a HUMAN reading a STORED
  record as evidence for a HAND-GRANTED waiver, so the direct PARSER exploit is gone: nothing in
  the wrapper parses the prompt for delivery mode, and nothing may be added that does. **THAT IS
  ALL IT BUYS** — the human is IN the path, not outside it, so spoofed repository-controlled prompt
  text can still mislead an authorizer into issuing the marker, and the marker is what makes
  `--recheck-job` pass. That exposure is **#3826**'s subject and is NOT settled here.
  **AND `job=` IS DAEMON-SCOPED, WHICH NOBODY HAD WRITTEN DOWN (#3654).** Each box runs its own
  roborev daemon with its own database and its own sequential ids, so **two boxes can legitimately
  present the SAME id for DIFFERENT reviews** — measured: `job=265` on two lanes 50 minutes apart,
  different ranges, branches and token counts, both correct — and the coordination lead read the
  repetition as a collision and WITHHELD a valid waiver. The failure is symmetric and the other
  direction is worse: a lead who therefore treats `job=` as uninformative discards the one field
  binding an authorization to a REVIEW rather than to a RANGE. So **verify the record's `git_ref`,
  never the id alone** — `roborev show <id> --json | jq '.job | {id, git_ref, branch, status,
  token_usage}'`, because `show` NESTS those fields under `.job`, while for
  `roborev list --json --limit <n> --repo <abs> --branch <branch>`
  **`--limit` is REQUIRED READING and must be RAISED until the job appears or the returned row
  count STOPS GROWING** — it defaults to 50 (measured, v0.61.2), so an older job is outside the
  window and the query yields nothing though the record exists, an absence indistinguishable from
  "no such job" and exactly the reach a waiver argument needs; an empty result at a limit that is
  still growing the row count is UNMEASURED, not an answer. And
  `list`'s default branch filter follows the **`--repo` PATH's CURRENT HEAD**, not the branch your
  shell is standing in — so name `--branch` whenever that checkout is not on the job's branch, which
  is exactly the `--recheck-job` case. An earlier revision of this line named the cwd's branch and
  was FALSE; the evidence offered for it — a `null` from a `--repo` sitting on `main` — is explained
  identically by both readings, so it could never have separated them. The measurement that does:
  from cwd `/tmp`, which is not a git repository at all, `--repo <lane>` returns that lane's rows. **Read `.job`, never a `show` payload's TOP-LEVEL
  `id`**: that is the REVIEW row's own sequence and need not be the job you asked for (measured over
  ten records — asking for 9 returns `id=8`, `job_id=9`, `job.id=9`), so a top-level jq manufactures
  the very "is this the right review?" doubt the check exists to remove. The wrapper is unaffected —
  `find_job` matches `id`/`job_id`/`job` and then prefers the object carrying `git_ref` — so this is
  a trap for the human running the check by hand, not a live false `STALE`. **A LOCAL ROW COUNT IS NOT EVIDENCE OF
  UNIQUENESS**: `roborev list … | jq '[.[] | select(.id==N)] | length'` returns `1` whether or not
  another box holds that id, because `list` only ever sees the LOCAL daemon — and `0` when the row
  fell outside the `--limit` window, so the count says nothing about the window it was taken over — another probe whose
  output is IDENTICAL under the two states it claims to separate (the `RESULT: INCOMPLETE` launch
  sentinel read as a verdict; a gate run dir found by `ls -t`; `mergeable: MERGEABLE` on a
  marker-bearing merge commit) — and it gave both lanes the right answer for a reason that did not
  hold. Use `git_ref`. **AND WHAT THE `git_ref` CHECK SETTLES IS SCOPED TO ONE DAEMON — the rest is
  NOT CLAIMED.** It settles that the id names the review you think it does *on this daemon*. It does
  NOT settle the cross-box case: two daemons can hold the same id for the SAME `git_ref` range, so a
  waiver authorized against machine A's review can be accepted by `--recheck-job` against machine B's
  DIFFERENT review of that range, and **no local lookup can detect it** — `roborev list` only ever
  sees the local daemon, while the marker travels through GITHUB. So same-range cross-daemon
  collisions remain UNPROTECTED; that is declared, not closed. **Whether the block should NAME the
  issuing daemon — and the cross-box question that comes with it — is tracked as #3825, together with
  the marker-grammar question it raises.**
  **THE ABSENCE WAIVER — the break-glass, its four constraints, and why the documentation is not the
  credential (#3312 job 23).** The **OWNER or the coordination LEAD** may excuse an absence FAIL with a
  **dedicated, column-zero line** of a PR comment:
  `roborev-waive: prompt-content-absent base=<40-hex> head=<40-hex> job=<id> reason=<why>`.
  **(a)** Human-authorized, never self-applied: a worker or closer may post **one** REQUEST comment —
  carrying the token accounting — and may never waive its own PR. **(b) Bound to the WHOLE REVIEW SCOPE**,
  not just the head: **base AND head AND job**, all required and all verified, because the authorizer
  judged **one** review — so a push, a different base *or a re-run* each need a fresh waiver, and one
  persistent comment can no longer excuse a later **vacuous** review at the same head. **(c)** It excuses
  the **ABSENCE verdict ONLY** — never any other cause — and the block still records what was absent, the
  authorizer, the bound scope and the reason, under a **distinct `WAIVED` token** (so nobody grepping
  `prompt-content: PASS` reads a waived run as certified) beside a `waiver:` key that names the state even
  when nothing was granted (`NONE`/`STALE`/`MALFORMED`/`UNAVAILABLE`, each leaving the FAIL). **(d)** The
  request carries the token accounting and the authorizer checks it.
  **AND THE LOOP HAS TO CLOSE, WHICH IT DID NOT (#3312 job 24).** The waiver binds a JOB, but the
  operator only learns the job id — and the token accounting — from the FINISHED run, and re-running the
  wrapper to apply a fresh waiver **enqueues a different job**, so the waiver was instantly `STALE`. As
  first built the break-glass was a **dead letter**: no sequence of actions got a legitimate absence past
  the gate. The fix is **not** to loosen the binding (dropping `job=` reopens the hole where one comment
  waives a later *vacuous* review at the same base+head) but to add
  **`--recheck-job <id>`**: re-decide THAT job's verdict, enqueueing nothing. The job is named
  **explicitly**, never resolved from base+head, or a re-run could inherit a waiver written for a
  different review. **A recheck inherits nothing**: `sha-assert` re-compares the record's `git_ref`
  against this base and head, the record's own review text becomes the transcript so
  `review-completed`, both vacuity tiers and `findings` are re-asserted from it (no review text ⇒ empty
  transcript ⇒ fail-closed), and `roborev-exit` reports `SKIP` rather than claiming an exit status for a
  process that never ran. The block declares **`MODE: recheck (job <id> …; NO review was enqueued)`** and
  **`recheck-of: <id>`** as its first keys — the way the gate declares `MODE: lite` — so a recheck PASS is
  legitimate but can never be pasted as evidence of a *fresh* review. Demonstrated end to end: absence
  FAIL → waiver naming that base/head/job → recheck ⇒ `WAIVED` + `RESULT: PASS`, with zero reviewer
  invocations; and a recheck of a *different* job stays `STALE`.
  **REQUEST A WAIVER ONLY WHEN THE HEAD IS FINAL — pushed, conflict-free, post-gate, and REVIEWED AT
  THAT SHA (#3460).** The binding above is `base` AND `head` AND `job`, each compared for EXACT
  EQUALITY and each against a DIFFERENT value: `head` against the run's own `HEAD_SHA`
  (`git rev-parse HEAD`, assigned ONCE before mode dispatch — NO path derives head from the job record,
  `--recheck-job` included), `base` against `RANGE_BASE_SHA`, which is the **MERGE-BASE and NOT the base
  ref's tip** (#3392 — copy it from the block's `assert-base:` line, NEVER from `base:`), and `job`
  against this run's job id. So **any commit landing after the request makes the grant unapplicable** —
  `waiver: STALE`, and the FAIL stands. The order is therefore: push every local commit → rebase or
  resolve until the PR is not `CONFLICTING` → gate of record → **a roborev confirmation pass ON THAT
  FINAL SHA** → only then request the waiver, naming THAT round's triple. **`--recheck-job` is not an
  escape**: #3392 stabilised the BASE comparison against a moving `main`, and nothing can make a HEAD
  binding survive a genuine content change. **The confirmation pass is the step that gets skipped, and
  skipping it has its own failure shape**: #3367's gated sha had never been reviewed at all (round 25
  reviewed `6f5fc2b7c` and two commits landed after it), and #2605 was the same — so the final sha needs
  its OWN round, and THAT round's job id is the one the waiver must name. **THE TRAP CATCHES A LANE
  DOING THE CAREFUL THING, which is why it is written down rather than left to judgement**: the absence
  diagnostic prints `base … head … job …`, those values are CORRECT at the moment it prints them, and
  **the failing block itself says nothing about a push invalidating them** — only `--help` does ("a
  push, a different base or a re-run each need a fresh one"), which is not where a lane reading a FAIL
  is looking. So copying the verified triple straight into a request is simultaneously the obvious
  action and the wrong one whenever anything is still going to move the head. Measured cost: THREE
  independent lanes on ONE day (2026-08-28) — #1705/PR #3382 (grant received, then a conflict with
  just-merged #1701 had to be resolved), #1699/PR #3403 (the triple was exact and the PR was
  `CONFLICTING` at the same moment), #3248/PR #3455 (fixes committed but unpushed, AND `CONFLICTING`) —
  each spending an authorization on code that would not merge, and asking the authorizer to judge a
  review that no longer described the diff. **DO NOT LOOSEN THE BINDING TO MAKE THIS EASIER**: all three
  instances are the binding WORKING, and a waiver riding to a later review is the hole #3312 exists to
  close. **TWO OF THE FOUR CONDITIONS ARE MECHANIZED AND TWO ARE NOT — KNOW WHICH.** *Pushed* is:
  `push-assert` FAILs the round before any review is enqueued (`FAIL (unpushed commits)` when the remote
  branch exists and local is ahead; `FAIL (branch absent on remote <remote>)` when it was never pushed —
  four spellings, one verdict). *Reviewed-at-that-sha* is: `sha-assert` FAILs when the record's range
  head ≠ local `HEAD`, and the head binding catches it again at waiver time. But **`mergeable` /
  `CONFLICTING` appears NOWHERE** in the wrapper, the waiver scanner or `premerge-assert.sh`, and
  **NOTHING correlates the reviewed sha with a gate of record** — so a pushed, reviewed,
  still-`CONFLICTING` head passes every check and yields a triple that dies on the rebase. Mechanizing
  those two, and splitting the staleness VERDICT TOKEN (`STALE` for all three causes today, though its
  DETAIL already names the diverged field and both values) into `head moved` / `base moved` /
  `job mismatch`, is **#3827** — whose demonstration is **circular rather than impossible**: every
  sanctioned invocation is the branch's own `scripts/flow/roborev-review.sh`, so the round reviewing a
  wrapper change RUNS the changed wrapper (the same property #3544 records for `agent-gate.sh`, and
  NOT the read-from-root self-certification bar of #3229).
  **The marker is decided by ONE anchored pattern, and the reason is trimmed BEFORE it is judged**, so
  field order and value boundaries are enforced and `reason=TODO ` / whitespace-only reasons are refused
  like their untrimmed forms — per-field extraction had enforced neither.
  **AND THREE LAYERS STOP THE ARTIFACT BECOMING THE CREDENTIAL — the sharpest instance of this issue's
  recurring shape.** The first version accepted the marker *anywhere* inside a comment whose newlines had
  been flattened, and the absence diagnostic **printed a complete marker carrying the live sha** — so
  pasting the summary block into a PR comment, *the documented practice throughout this repo*, authorized
  the next run (RED-verified: the pasted block produced `prompt-content: WAIVED … RESULT: PASS`). A quoted
  example or a waiver *request* self-granted the same way. It is the same defect as prose inside a diff
  naming its own oracle, which is why the census matcher is column-zero anchored. Now: **(1)** comment line
  boundaries are preserved and the marker must **BE** the line — an indented, `>`-quoted, bulleted or
  mid-sentence copy is inert; **(2)** placeholder reasons are refused (an unsubstituted `<…>`, or a bare
  `why`/`todo`/`tbd` — `claim.sh`'s rule), so a pasted **template** reads `MALFORMED`; **(3)** no emitted
  diagnostic carries **any part** of the marker — not even the prefix — and points at `--help` instead.
  **THE UMBRELLA LESSON OF THIS ISSUE, and the most durable thing in it: CONTROL AND DATA MUST NOT SHARE A
  CHANNEL WHEN THE DATA IS ATTACKER-CONTROLLED (#3312).** Four separate High-severity defects were the same
  shape, and each individual fix worked while the family kept regenerating — because the shape was never
  named in one place. The instances, in the order they were found:
  1. **Prose inside a diff naming its own oracle.** A census path quoted in the reviewed text could satisfy
     the check that the reviewer *received* that path — which is why the matcher is anchored at COLUMN ZERO
     (every unified-diff body line carries a leading `+`/`-`/space/`@`/`\`, so body content cannot pose as
     a header).
  2. **The wrapper's own diagnostic printing a valid waiver marker** — an artifact that DESCRIBED the escape
     hatch BECAME it, because summary blocks get pasted into PR comments as a matter of course. Fixed by
     three layers: an anchored dedicated line, placeholder-reason refusal, and a diagnostic that emits no
     part of the marker at all.
  3. **Repository text reproducing roborev's delivery-block markers.** Delivery mode was inferred from
     prompt text that embeds project guidelines / `AGENTS.md`, so repo content could move a review into an
     uncertified mode in either direction. No terminating marker existed, so the owner deleted the
     *classifier* rather than patch a fifth instance of it.
  4. **A comment body forging its own author record.** Comments were flattened into one stream with an
     in-band author delimiter, so an unauthorized commenter could name an allowlisted login inside their own
     body and defeat the allowlist with one control character. Fixed by parsing `gh --json` STRUCTURALLY —
     author and body stay separate FIELDS of one object — so there is no delimiter to forge.
  **The generalisation to apply elsewhere:** when a decision is made from a stream that carries both your
  markers and someone else's payload, the fix is to REMOVE the shared channel (structured data, a separate
  field, a distinct file), not to choose a rarer delimiter — a rarer delimiter is still forgeable, and each
  narrowing only postpones the next instance. Where the channel genuinely cannot be separated, anchor the
  control tokens somewhere the payload provably cannot reach (column zero of a diff), and say in code that
  this is what the anchor is for.
  **THE FIFTH VARIATION, AND HOW THE CLASS WAS FINALLY CLOSED (#3312 job 29): AN AUTHORIZATION MUST BE THE
  SOLE NONBLANK CONTENT OF ITS PR COMMENT.** Leading/trailing blank lines are fine; anything else — prose, a
  code fence, a quote, an HTML tag, a second sentence — means the comment is **not** an authorization.
  **FOUR RECOGNISERS WERE TRIED AND SUPERSEDED**, each correct about the case in front of it, and they are
  named here so nobody reintroduces Markdown parsing thinking it was an oversight:
  (1) accept the marker **anywhere** in the comment ⇒ a quoted example granted;
  (2) require it to **be its own line** (column-zero anchor) ⇒ defeated indented, `>`-quoted, bulleted and
  mid-sentence copies, but not fences;
  (3) **skip fenced regions** ⇒ a fence preserves column zero, so a quoted example inside one granted;
  (4) **track fence open/close state** ⇒ a ```` ```bash ```` line *inside* a fence is CONTENT, not a closing
  delimiter, so the state desynchronised and a later marker granted — and HTML `<pre>`/`<code>` was never
  covered at all.
  Every one asked *"is this line DATA or CONTROL?"* of a grammar the **comment author controls**, which has
  unbounded ways to say "this is data" — so the list of recognisers never closes. **That is this issue's own
  umbrella lesson applied to itself: remove the shared channel, do not pick a rarer delimiter.** Parsing
  Markdown to separate data from control *is* sharing a channel. The sole-content rule removes it and is
  decidable **without parsing anything**: no quoting construct can be the only thing in a comment, because
  every quoting construct requires additional content.
  **Cost, and why it is arguably an improvement:** the authorizer posts a comment containing only the marker
  and puts commentary in a **separate** comment — the token accounting already lives inside `reason=`, so
  nothing is lost, and an authorization *should* be a clean unambiguous act rather than a sentence buried in
  prose. **A comment with other content is ignored silently, not reported malformed:** someone documenting
  the form (this repository's own threads do) never attempted an authorization, and `MALFORMED` would be a
  false accusation reprinted on every later run. A **marker-only** comment with bad fields is still
  `MALFORMED`. The `NONE` cause teaches both rules — sole content **and** top-level.
  **THE WAIVER'S THREAT MODEL, STATED WITH ITS LIMITS (#3312) — and the triage rule that goes with it.**
  Five consecutive review rounds landed in this one authorization path (marker anchoring, scope binding,
  author authorization, the parse channel, the enforcer path). Every fix was right, and the pattern predicts
  more — so the boundary is recorded to stop the next finding being patched instead of triaged.
  **A HOSTILE INVOKER IS OUT OF SCOPE, by construction.** Whoever runs the wrapper can edit it, replace the
  scanner file beside it, shadow `gh`/`python3` on `PATH`, or skip the wrapper and hand-write a
  `==== ROBOREV REVIEW SUMMARY ==== … RESULT: PASS` block into the PR. **No check inside a process defends
  against the party that controls the process**, and pretending otherwise is the false-assurance shape this
  issue exists to remove. The merge gate's real protection against a hostile *worker* is the **audit trail
  plus a human reading the PR**, not the wrapper.
  **WHAT IT DOES DEFEND, which is why the layers were worth building:** (1) **parties who do not control the
  invocation** — this is a public repository, anyone can comment, and a failing block *prints* base/head/job,
  so the allowlist + anchored marker + structured author association are what stop a stranger; (2)
  **accident and drift**, the larger category in practice — a pasted block, a quoted example, a stale waiver
  riding to a later review, a re-run inheriting an authorization meant for another job, an unsubstituted
  placeholder. Every fix in this path landed in (1) or (2).
  **THE TRIAGE RULE:** *"the INVOKER can bypass this"* ⇒ **out of model — record it, do not patch it**;
  *"a NON-INVOKER can bypass this"* or *"this can be bypassed BY ACCIDENT"* ⇒ **defect**. Same-host actors
  able to write these scripts or roborev's database are **invoker-class**, not third parties.
  **CHEAP HARDENING REMAINS WORTH IT** where an invoker could reach the same end another way: dropping the
  scanner-path env override cost nothing and closes contexts where the environment is influenced while
  files are not (a workflow injecting a variable). "Theoretically redundant" never justifies leaving a hole
  a non-invoker or an accident can walk through.
  **TWO RESIDUALS INSIDE THE MODEL, named rather than implied:** the marker is read from **top-level PR
  comments only**, and **THE MOST PROBABLE MISPLACEMENT IS THE PR'S LINKED ISSUE THREAD (#3759)** — that is
  where lane/lead coordination lives — followed by a review body and a review-thread reply. None of the
  three is read, so a marker posted there is silently not applied (the run reports `waiver: NONE` and the
  FAIL stands — fail-closed, but it reads as "my waiver was ignored"). **Measured on PR #3710:** the lead
  granted BOTH markers, field-perfect and sole-content, from an allowlisted author — on **issue #3544**;
  both keys read `NONE`, which is textually identical to "the lead refused", and position 1 of a six-PR
  serial queue idled ~8 hours and blocked five lanes. **SINCE #3759 THE LINKED-ISSUE CASE IS DIAGNOSED,
  NOT GRANTED:** when the PR-side scan returns `none`, the PR's linked issue(s) — resolved from the
  structured `closingIssuesReferences` relation, NEVER from the mutable PR body — are scanned with the
  SAME scanner and the SAME base/head/job/allowlist, and a marker there that WOULD have been accepted by
  the channel is reported `waiver: MISPLACED (found on linked issue #N …)` naming the issue and the
  remedy. **`MISPLACED` GRANTS NOTHING — not partially, not with a notice — and the FAIL STANDS**: only a
  marker on the PULL REQUEST grants, and moving it there is a HUMAN act by the authorizer, never
  something the tool does. A `NONE` now also DECLARES whether that probe ran (checked / bounded /
  no linked issue / could-not-check), so "checked and it is not there either" and "never checked" can no
  longer read alike. **LEAD-SIDE PROCEDURE, the other half of the fix: after posting either marker,
  verify with `gh pr view <PR> --json comments` that the line is ON THE PR — a grant is only granted once
  it is readable by the scanner that reads it.**
  And **an authorized human can authorize carelessly** — pre-authorizing a job id, or waiving without
  checking the token accounting. Nothing mechanical detects either; the control is the permanent,
  attributable comment, which is why a substantive reason is required and recorded verbatim.
  **AND A SECOND, EQUALLY TRANSFERABLE RULE FROM THE SAME ISSUE: THE CONSTRAINED PARTY MUST NOT CHOOSE ITS
  OWN ENFORCER (#3312 job 27).** Hardening a check while leaving its *invocation* configurable moves the hole
  rather than closing it. Concretely: the waiver allowlist was deliberately hard-coded and asserted
  non-env-derived — *"an override is settable by the party the allowlist constrains"* — and then the **scanner
  that enforces that allowlist** was made env-settable (`WAIVER_SCAN_TOOL`), so an invoker could point it at a
  script printing `state=granted` and pass with **no authorized comment anywhere**. The protection had moved
  outward and been left open. The enforcer is now resolved from the wrapper's own directory with no override
  and no `${…:-…}` fallback, and the structural assert covers the **invocation** as well as the value.
  **Corollary for tests:** a case needing a different enforcer **substitutes the artifact** in its own scratch
  copy of the tree — never a path variable. A test-only seam is one more thing a real invoker can set, so the
  harness assert forbids reintroducing one (with the needle split so the guard cannot match its own line).
  **WHO MAY GRANT: AN EXPLICIT AUTHOR ALLOWLIST (#3312 job 25) — and the correction that produced it.**
  The comment author used to be *recorded but never authorized*, so on this **public** repository ANY
  commenter could copy the `base`/`head`/`job` values out of the failing block (they are printed in it)
  and make the merge gate pass. The residual had been written as *"we cannot distinguish the owner from
  the worker on a shared `GH_TOKEN`"*, which conflated **cannot enforce perfectly** with **cannot enforce
  at all** — so absence of a perfect check became absence of ANY check, the same permissive shape this
  issue is about. Now: the author must be on `ROBOREV_WAIVER_AUTHORS`, hard-coded in
  `roborev-review-oracles.sh` — **not** a config file and **not** env-overridable, because an override is
  settable by the party it constrains and one visible location keeps "who may grant" inside the diff a
  reviewer already reads. A well-formed marker naming this exact review from a non-allowlisted author
  reports **`waiver: UNAUTHORIZED (...)`** — distinct from `MALFORMED`, because the marker was fine and
  the author was not.
  **THE RESIDUAL SURVIVES, NARROWED TO WHAT IS TRUE:** the worker, the closer and the owner all post
  through the SAME login on this fleet, so nothing here can tell **which allowlisted human** posted a
  comment. "Only the owner or the coordination lead may GRANT; a worker may only REQUEST" is therefore
  **process-enforced with an audit trail** at that level — never a claim that authorship is unverifiable
  in general. **An unenforceable claim gets scoped to what is enforceable, never dropped whole.**
  **The hang and race classes are NOT REACHABLE because nothing is read — that is weaker than "fixed", and only
  it is true.** The predicate-family rule survives as **doctrine, not code** (the helper and its lint were
  deleted with the probes they served, since a lint with an empty subject set greens vacuously): **every
  `test`/`[` file predicate is two-valued, so it must collapse "cannot tell" onto one of its answers — and it
  always picks the permissive one.** If a filesystem probe ever returns to that code, this rule obligates the
  three-valued helper (`verified-absent` / `present` / `unreadable`) to return with it.
  **That is ONE SHAPE, found repeatedly on #3229, so it is now a RULE: a positive verdict requires an
  AFFIRMATIVE MEASUREMENT.** The shape is *a multi-state signal where only the BAD states are tested, so
  every unknown/unmeasured state inherits the PERMISSIVE branch* — a three-state signal took the permissive
  excusal path; an `UNAVAILABLE` corroboration state reached a `PASS` and **enqueued** (the code's own
  comment said the binary was the only oracle that could tell "no key recognised" from "nothing
  configured", then never required it to have *answered*); a `${end:-$start}` default degraded a failed
  `awk` bound to a 1-line scan. Those instances lived in a subsystem since deleted; **the shape is the
  lesson, and it was never theirs** — it was in the wrapper's own terminal verdict scan, which predates
  them all. **AND `findings:` WAS THE SAME SHAPE, ONE KEY OVER (#3564).** `findings:` is not one of the
  six affirmation keys — its affirmative value is `NONE`, not `PASS` — and it was documented as merely
  CORROBORATING, which read as "guarded elsewhere" when it was guarded NOWHERE: `PRESENT` is in the
  closed grammar's NON-FAILING set, so the only thing failing a findings-bearing run was the
  NEIGHBOURING key `roborev-exit: FINDINGS (exit 1)`. On `--recheck-job` **no reviewer runs**, so
  `roborev-exit` is legitimately `SKIP` — and the run emitted `findings: PRESENT (3)` beside
  `RESULT: PASS`, a **false PASS in a merge gate** (measured on #3473 round 3), on the ONE path an
  authorized waiver must travel, letting a waiver scoped to `prompt-content` ABSENCE excuse findings
  nobody excused. Now a would-be PASS requires `findings:` to reduce token-exactly to `NONE` **in every
  mode including recheck**, and that requirement is **NOT waivable**. Fixed in the verdict scan and
  deliberately NOT in `roborev-exit`: `SKIP` is the TRUE statement about a recheck, and making a key
  claim a failure it never observed trades one false statement for another. Second half, the part that
  keeps the break-glass alive: a recheck of a record with no structured `verdict` field used to read
  `UNKNOWN` (its branch was keyed on the reviewer's exit code, and there is no reviewer), which would
  have false-FAILed EVERY clean recheck — so a recheck now re-asserts findings from the record's own
  review text — but ONLY in the direction prose can actually evidence. **PROSE CAN EVIDENCE FINDINGS;
  IT CANNOT EVIDENCE CLEANLINESS**, so a marker in a findings block yields `PRESENT` while its ABSENCE
  yields `UNKNOWN`, never `NONE`. `NONE` is reachable only from the record's STRUCTURED `verdict`
  letter. **Two review rounds each found a review SHAPE the previous recogniser missed** — a HEADERLESS
  findings review (no `Findings` heading, which `review-completed` deliberately accepts), then a
  findings BLOCK with no recognised severity marker — and the class provably does not close, because
  `review-completed` accepts a bare `## Summary` heading as a completed review: a findings review whose
  findings are prose is then INDISTINGUISHABLE from a clean one, whose real text is
  `No issues found.\n\nSummary: …` with no `Findings` heading either. That is #3312's lesson applied
  one directory over: **REMOVE THE CHANNEL, do not pick a rarer delimiter** — a recogniser over
  author-controlled prose never closes. **And it costs nothing, measured rather than assumed**:
  `roborev show --json` SYNTHESISES the verdict letter from the `reviews.verdict_bool` column for every
  observed record (`P` clean / `F` findings; `review_jobs` has no verdict column), so a real clean
  recheck takes the structured path and the break-glass is intact, and the verdict-less branch is
  defensive for a payload shape nothing observed emits. **The generalisation to carry elsewhere: DELEGATING A KEY'S FAILURE TO ITS NEIGHBOUR IS A
  LATENT FALSE PASS** — the coupling is invisible while one event populates both keys and evaporates in
  the first mode where it does not, so ask of every key *what fails the run if THIS key alone goes bad*.
  **And a fail-closed argument for a `${VAR:-default}` is only valid for the consumers that existed when
  it was written**: the `block_marker_count` `:-0` was audited as strict because `NONE` was the STRICT
  direction for `vacuity-tier1:`, and a new consumer for which `NONE` is PERMISSIVE inverted it silently
  — no default can fix that (`0` and *unmeasurable* are one value). **The resolution is not a better
  default or a second signal but a REMOVED CONSUMER**: `NONE` is unreachable from a marker count at all
  (only the structured verdict yields it), so nothing derives a permissive verdict from that `0` and the
  original argument holds unchanged. An intermediate version of this fix DID add a separate
  `block_measured` flag; it went away with the prose reconstruction it guarded, and this sentence
  described it for one round after it was deleted — caught by the C audit. **A doctrine line naming a
  mechanism is a claim about code, and it decays exactly like a comment: re-grep the symbol.**
  **AND THE UNWAIVABLE RULE MADE ONE MERGE UNOBTAINABLE, WHICH IS ITS OWN DEFECT CLASS (#3626).** #3586's
  requirement is right, and it interacted with a fact nobody designed for: **roborev re-reports a
  LEAD-DEFERRED finding on every later round.** So once a lead defers a finding — as a nit, as a batched
  follow-up, or by explicit ruling — `findings: PRESENT (n)` persists, `RESULT` stays `FAIL`, and *"any
  non-PASS terminal RESULT is a blocked merge"* blocks that merge **forever**: neither escape hatch applies
  (the absence waiver excuses `prompt-content` ABSENCE only, by design, and a correct `--recheck-job` of a
  findings-bearing job re-reports the same `FAIL`). Measured on PR #3572 job 262: two findings, **ZERO
  new** — both already filed (#3602, #3613) and both already lead-deferred — 5.9M input / 5.7M cached
  tokens, every deterministic key PASS, and the merge required an out-of-band lead comment. The lane the
  fix protects is the one that behaved CORRECTLY: it refused to arm `--auto` over a `FAIL`, refused to fix
  the deferrals to manufacture a green, refused a waiver that does not apply, and asked the owner instead.
  **A rule that punishes the correct behaviour will not survive contact.** So *"roborev clean"* is
  redefined as **NO UNADDRESSED FINDINGS**, and the distinction is made MECHANICAL rather than a matter of
  lead memory: a **second marker**, `roborev-defer: findings issues=<N>[,<N>...] count=<n> base=<40-hex>
  head=<40-hex> job=<id> reason=<why>`, travels the **absence waiver's channel** (top-level PR comment,
  column-zero, **sole nonblank content**, hard-coded `ROBOREV_WAIVER_AUTHORS` allowlist, structured
  `gh --json` author parsing, applied via `--recheck-job`, placeholder reasons refused, no part of the
  marker in any diagnostic) and inherits those rules **BY CALL** — the same scanner, kind selected
  explicitly — never by copy, because a second implementation of a channel rule is a second place for it
  to diverge and a divergence there is an authorization bypass. **There is deliberately NO flag, NO file
  in the worktree and NO env var**, each of which would hand the constrained party the power to satisfy
  its own constraint (#3312's corollary). **AND IT INHERITS THE MISPLACEMENT RESIDUAL BY CALL TOO
  (#3759)**: the deferral marker is read from **top-level PR comments only**, and the **MOST PROBABLE
  MISPLACEMENT IS THE PR'S LINKED ISSUE THREAD**, ahead of a review body and a review-thread reply —
  measured on PR #3710, where both markers were granted field-perfect on issue #3544 and both keys
  read `NONE`. Since #3759 that case is DIAGNOSED, not granted: a would-have-been-accepted marker on a
  linked issue reports `deferral: MISPLACED (found on linked issue #N …)` with the remedy, while
  `findings:` stays exactly as measured — **never `DEFERRED`** (that would be the grant) and **never
  `NONE`** (that would read as a clean review) — and `RESULT` stays `FAIL`. **`MISPLACED` GRANTS
  NOTHING**; only a marker on the PULL REQUEST grants. The rendering claims the marker *would have
  been accepted by the channel*, NOT that it *would have granted*: the network **issue-disposition
  legs are deliberately not run issue-side** and still apply once the marker is on the PR, because a
  diagnostic that overstates what it measured is what stops the next person looking. As with the
  waiver, **verify with `gh pr view <PR> --json comments` that the line is on the PR after posting
  it.** **The match is AFFIRMATIVE, which is what makes this a match
  and not a mute button**: `count=` must EQUAL the observed findings count and `issues=` must be
  non-empty, so a PRE-AUTHORIZATION written before the findings were read fails on a mismatch, and **any
  new finding at the same head raises the observed count and fails** — that is how the UNDEFERRED set is
  computed without a per-finding identity roborev's prose does not provide, and **no such identity is
  reconstructed from that prose** (the class #3564 closed by REMOVING prose reconstruction stays closed).
  `issues=` records that the finding is **TRACKED**, and THE ISSUE-STATE LEG is what enforces it:
  each number must be an **OPEN** GitHub issue, asked **FOUR-VALUED** — only a payload affirmatively
  naming that number **and an OPEN state** is `present` and may grant; an issue GitHub answers does not
  exist is `ISSUE-ABSENT`; an issue GitHub answers is CLOSED is `ISSUE-CLOSED`; an issue whose existence
  could NOT BE ASKED (no `gh`, no auth, a network/API failure, an unparseable payload, or any diagnostic
  that does not say the issue is missing) is `ISSUE-UNVERIFIABLE`. They are **textually distinct**
  because they are different operator actions ("that issue number is wrong" / "that issue is closed" /
  "this box cannot reach GitHub"), and **`gh issue view` EXITS 1 FOR BOTH THE FIRST AND THE LAST**
  (measured, gh 2.98.0) — so an exit-code-only test is the two-valued predicate that always picks
  the permissive answer and would grant over issues nobody confirmed exist. Unrecognised ⇒ could-not-ask,
  and a could-not-ask is NEVER read as verified.
  **AND "RETRIEVABLE" WAS NOT ENOUGH, WHICH IS WHY THE CHECK IS STRONGER THAN THE CONDITION THAT ASKED
  FOR IT (#3626 round 3).** `gh issue view` returns the number and **exits 0 for a CLOSED issue**, so a
  number-only test made "the finding is tracked" satisfiable by an issue closed as a duplicate three
  weeks ago: `present` ⇒ `GRANTED` ⇒ `RESULT: PASS`, the finding permanently untracked while the block
  asserted it was filed. The condition said *retrievable* and closed-is-retrievable satisfies the letter
  — but three separate statements of this leg claim it enforces **not-dropped**, so the claim was made
  TRUE rather than weakened to match a weaker implementation. **The generalisable ruling: when the
  implementation satisfies the LETTER of a condition and contradicts the PROPERTY every statement of it
  claims, strengthen the implementation — do not narrow three claims.** A false refusal here is
  recoverable (reopen it, or file a fresh tracking issue) and is the fail-closed direction.
  **The disposition backstop COUNTS VERIFICATIONS PERFORMED; it does not test the string.** It was
  `[ -z "$ISSUES" ]` — a non-emptiness test standing in for a verification test — and `ISSUES=","`
  passes it, splits into ZERO words, runs the loop body never, and returns with the state still
  `granted`: a `DEFERRED` ⇒ `PASS` with not one `gh issue view` executed. Unreachable only because the
  `issues=` PATTERN forbade that value, i.e. **exactly the upstream dependency a backstop must not
  have**. Now the count of verifications must EQUAL the count of declared comma-separated fields.
  **A PR-BODY LINK WAS ALSO REQUIRED, AND THAT LEG IS DELETED — DO NOT REINSTATE IT (#3626, lead
  ruling).** An earlier revision demanded each number also appear as a local, visible `#N` in the PR
  BODY (`PR-UNLINKED` otherwise), with recognisers for `owner/repo#N`, `#Nsuffix`, fences, code spans and
  HTML comments. **The reason it is gone is NOT the bypasses: a PR body is EDITABLE AT ANY TIME BY ANYONE
  WITH WRITE ACCESS, WITH NO PER-EDIT ATTRIBUTION, while a top-level comment is PERMANENT AND
  ATTRIBUTABLE.** So it was the WEAKER artifact and would stay weaker **even if Markdown parsed
  trivially** — an authorization the constrained party can silently rewrite after it is granted evidences
  nothing; the recogniser problem was a SYMPTOM. The requirement's own wording invited it, too: "name
  where the finding went" invited a PROSE SCAN when the property wanted is that the finding is TRACKED.
  The census, kept because it is the evidence the class does not close (Markdown-handling references in
  that one predicate went **0 → 11** over two rounds): round 1 closed five shapes (cross-repository,
  `#Nsuffix`, fenced block, HTML comment, single-backtick span); round 2 found **two more** — a
  multi-backtick span and an explicit `[#N](url)` link — with GFM autolinks, reference-style links, raw
  HTML, entity refs and nested emphasis unhandled by any generation and the 4-space indent already a
  declared residual. #3312 (*remove the shared channel, do not pick a rarer delimiter*) and #3229's owner
  ruling (*a guard with known documented false-PASSes is worse than no guard*) both apply, and
  **subtraction cannot introduce a false PASS**: with nothing predicted about the body, nothing is
  excused by it. Any future strengthening must come from an **immutable or attributed** artifact (a
  structured GitHub relation, or the authorization comment itself), never from parsing the mutable body
  of the PR under review. It reports
  a **distinct token** — `findings: DEFERRED (<n>, issues=#…, authorized @<login>, job <id>)`, **NEVER
  `NONE`** (which stays reachable only from the record's structured verdict letter, so nobody grepping
  `findings: NONE` reads a deferred run as clean) — beside a `deferral:` key that speaks even when
  nothing was granted (`NONE`/`STALE`/`MALFORMED`/`UNAUTHORIZED`/`COUNT-MISMATCH`/`ISSUE-ABSENT`/
  `ISSUE-CLOSED`/`ISSUE-UNVERIFIABLE`/`UNAVAILABLE`, each leaving the FAIL). A marker **attempt** is the
  stem plus whitespace **or end-of-line**, so a marker-only comment that is exactly the stem is
  `MALFORMED`, never a fail-quiet `NONE`. Three field rules, both kinds, one parser: `base=`/`head=` are
  **exactly 40 hex** (an abbreviated sha is `MALFORMED`, never `STALE` — it names THIS review in a
  spelling the form forbids, and an authorizer sent to re-check *which review* finds nothing wrong); a
  recorded `reason` keeps its internal whitespace **VERBATIM** (only the BLOCK boundary renders a
  control character as a visible escape, because the property required is one line per value, not
  collapsed whitespace); and a `reason` may **not contain either marker stem** — refused, not escaped,
  since **the structural assert covers the CODE while a RUNTIME value can inject what no source scan
  sees, so an invariant over OUTPUT needs a check on the OUTPUT PATH**. **AND THAT RULE IS OVER EVERY
  EMITTED VALUE, NOT OVER THE `reason` — fixing the field and not the class cost a review round
  (roborev job 230).** The reason is the field an authorizer CHOOSES, so refusing it removes that class;
  a keyword also arrives through fields nobody chooses — an unauthorized commenter's **GitHub login**
  (which `UNAUTHORIZED` must report to say who was refused), **`gh issue view`'s stdout/stderr** (which
  reach `deferral:` as an `ISSUE-UNVERIFIABLE` cause), the allowlist, and whatever a future key
  interpolates. So each process neutralises the keywords at its **ONE emit boundary** (`safe_value` in
  the scanner; `roborev_safe_line` in the wrapper, already the gate for every block value and every
  DETAILS line), never per interpolation site — a per-site escape is a list to keep complete. There the
  value is **REDACTED, not refused**: it is an identity or a diagnostic the run must still report.
  Only where the keyword is **not continued by another letter** — a longer word is a different word,
  exactly as `roborev-defer: findingsfoo` is — because the scanner's own FILE NAME embeds a keyword and
  is printed by the fail-closed `waiver: UNAVAILABLE (… tool: <path>)` cause an operator has to read.
  It is **display-only, which is the whole safety argument**: every authorization decision is made on
  the RAW value before any renderer runs, so two boundaries can only redact differently, never grant —
  acceptable where two marker PARSERS would not be, since a parser decides and a renderer does not.
  Deliberately **not** a security layer: a login admits letters, digits and hyphens and NOT colons or
  spaces, so it cannot hold a full stem, and an emitted line begins `deferral: UNAUTHORIZED (`, which
  the sole-content rule refuses. **`findings: UNKNOWN` and `SKIP` are NOT
  deferrable in any mode**: those states were never ESTABLISHED, and a pass may not rest on a state that
  could not be read. **The two authorizations stay SEPARATELY SCOPED and neither falls back to the
  other** — an absence waiver confers no authority over `findings:`, a findings deferral none over
  `prompt-content:` — because collapsing them would let a delivery-artifact waiver excuse a real defect;
  a run may legitimately carry both, each under its own key. `DEFERRED` is a value of the **closed**
  verdict grammar, non-failing **only** on the single coupled granted state that the grammar scan and the
  findings gate both read — one state, not two, so they cannot drift into two opinions about whether one
  authorization was granted. **AND THAT ADMISSION IS CONFINED TO ONE KEY, `findings:`, BY CONSTRUCTION**
  (roborev job 225): the scan carries each key's NAME beside its value and admits the token for
  `findings` alone, and the deterministic-key affirmation backstop carries **no** `DEFERRED` arm and
  reads the state not at all. The confinement was first left resting on an ABSENCE — no other key
  happened to emit the token — which is #3564's lesson verbatim, so ask of every key *what fails the run
  if THIS key alone goes bad*. The contrast with the absence waiver is the reason: **a waiver authorizes a
  PROPERTY** (an absence) that only one key can ever report, so its provenance IS the whole test and it is
  correctly not key-scoped; **a deferral authorizes a NAMED SET OF FINDINGS** and says nothing about
  whether the reviewer's diff arrived or the reviewed range matched, so an unconfined admission would let
  ONE authorization excuse a check NOBODY authorized. Relatedly, **no emitted diagnostic reproduces any
  part of either marker — not even its prefix**: the MALFORMED detail used to quote the whole required
  form and is interpolated into the summary key, so the block printed a fillable authorization beside a
  live base/head/job while a comment beside the interpolation asserted it never did. So: never derive a pass from the ABSENCE of a bad signal; where an oracle is the SOLE evidence
  for a claim and could not be consulted the verdict is NON-PASSING and its text names what was
  unverifiable; key a permissive branch on the AFFIRMATIVE value (`= OK`), never on `!= <bad>`; and where a
  signal genuinely SHOULD be permissive, record the reason IN CODE at the branch. The wrapper's verdict
  scan is therefore a CLOSED grammar (unrecognised value ⇒ FAIL) plus a backstop that no PASS may carry a
  verdict-carrying key that is not affirmatively `PASS` — a `SKIP` means the check never ran, which is the
  vacuous pass itself. **Both are RETAINED after the oracle that surfaced them was deleted**, because they
  are properties of every remaining key, and leaving the terminal verdict permissive again would leave the
  wrapper worse than we found it. **And the closure must not itself be a prefix test**: `PASS*` accepts
  `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, i.e. the guard against unplanned values would
  check a *spelling* rather than a *state* — the same shape one level down. So each value is reduced to its
  **verdict TOKEN** (up to the first space) and matched **EXACTLY**.
  **Paths are normalised ONCE, at the census, and that boundary is the fix for SIX blockers (#3229).**
  Rounds 2–4 of review produced six, and every one was a path-normalisation defect in a *different*
  consumer, because normalisation was scattered. Now the census reads `git diff --numstat -z` (and the
  survivor set `--name-only -z`), so paths arrive **RAW**, and RAW is the single representation used for
  classification, comparison and display; the one quoted-path decoder survives for the reviewer's prompt
  alone, with exactly one caller — the canonical matcher `roborev_diff_header_has_path`, which every
  consumer must ask rather than parsing headers itself. It reads every shape git emits: unquoted,
  **space-bearing** (`diff --git a/a b.txt b/a b.txt` — this repo tracks 40 space-bearing paths under
  `docs/`), **C-quoted** (`diff --git "a/\303\251.txt" "b/…"`), and the **MIXED** shape a rename produces
  (`diff --git a/<ascii> "b/<quoted>"`). Two measured costs of getting this wrong, in both directions: the
  census classifying a *quoted* spelling read `docs/é notes.md` as extension `md"` and called PROSE **code**,
  so the configured `*.md` legitimately removed it from the reviewer's diff while `prompt-content:`
  demanded it there ⇒ a **false FAIL** on an ordinary docs+code branch (reproduced against the tracked
  `docs/research/CQLite Writes (M5) — …md`); and a
  newline-delimited path set with `grep -Fxq` membership made a path's first line "prove" its presence ⇒ a
  genuine **false PASS**. A key that reds on correct input is the key agents learn to waive; a key that
  greens on absent input is worse. The invariant is asserted **structurally** in
  `scripts/tests/test_roborev_review_guard.sh` (no path-reading `git diff` without `-z`; the decoder called
  only from the matcher), because behavioural cases only cover the shapes someone already thought of.
  **A `.roborev.toml` change cannot certify itself (#3229) — three properties, one generalization:**
  **(1)** roborev's daemon binds a repository by its **`repos.root_path`** and reads **that ROOT
  checkout's** `.roborev.toml` — a *worktree* `.roborev.toml` edit is **invisible** to it, so under
  1:1:1:1 the file you edited is not the file your review applies. **(2)** The daemon **snapshots config
  at start**, so an edit needs a **daemon restart** to take effect. **(3) Generalized: any PR whose
  subject is a config the daemon (or a gate) reads from root cannot certify itself** — the same shape as
  `required` evaluating the aggregator and registry from the PR's **BASE** ref (below). Plan the
  demonstration for **after** the merge. Both (1) and (2) have cost real rounds: (1) produced a
  since-removed key's `PASS (7/7 survive)` about a config roborev never read, caught only by the
  pre-existing `prompt-content: FAIL (1/7 absent)` — **defence in depth paid out in the direction nobody
  plans for, and it is why `prompt-content:` is the layer that stayed**; (2) made #3234 measure `exclude_patterns` as having
  "no observable effect" (its single daemon restart preceded every config edit and never followed one).
  The durable lesson from that pairing: when the newer, cleverer guard and the older, dumber one disagree,
  **the one that measures what actually happened wins** — which is why the descope kept `prompt-content:`
  and dropped the predictor.
  Push first: an unpushed implementation commit is
  itself an empty-diff cause, and the wrapper asserts the push and FAILs otherwise. **Why:** FOUR
  confirmed paths make roborev report clean having reviewed NOTHING (or only part), and a vacuous pass is
  TEXTUALLY IDENTICAL to a genuine one — (T1) from a worktree, `--branch` without `--repo` resolves
  against the ROOT checkout (normally on `main`) and enqueues the BASE commit: enqueued `39900e4db`
  (= origin/main) while branch HEAD was `4e7ab591e`; (T2) the two-positional range form anchors the range
  at git's EMPTY TREE (`4b825dc6…`); (T3) a diff every path of which the configured
  `exclude_patterns` match is SILENTLY DISCARDED even with the right SHA and the right `--repo` — a
  code-free diff by default, and under a mis-scoped pattern like `docs/**` an EXECUTABLE one too — so
  **SHA verification alone is insufficient**; (T4) a single-SHA review covers
  ONE COMMIT — a PARTIAL review whose enqueued sha EQUALS HEAD, so no sha check can see it (this is the
  form #2964's own AC2 asked for; the wrapper implements the AC's intent instead).
  Token accounting is the tell: genuine reviews
  398k–649k input / 314k–554k cached / 5.0k–6.3k output over ~2m30s, vs the vacuous baseline 18.7k input
  / 0 cached / 53–56 output in 8s. Real cost: on #2950 two vacuous runs "passed"; re-run correctly
  against the real SHA, the SAME diff produced TWO REAL BLOCKERS. 1:1:1:1 puts EVERY issue in a worktree
  and `flow-closer`'s final pass is a MERGE GATE — so this could merge unreviewed code fleet-wide.
  Reviewer-selection trap: `--agent claude-code` alone still inherits `review_model = 'gpt-5.6-sol'` from
  `.roborev.toml` (the repo pin overrides your global `~/.roborev/config.toml`) — an OpenAI model name
  Claude cannot serve, which fails as a silent review failure that looks like an outage; historically
  mirrored (codex-on-a-ChatGPT-account hard-`400 'opus' model is not supported`). Hence the wrapper
  enforces both. `gpt-5.6-sol` is **codex's own built-in default, not a config pin** — there is no
  `~/.codex/config.toml` on the worker boxes; the bare `codex` default moved `gpt-5.5` → `gpt-5.6-sol` in
  the 0.142.5 → 0.145.0 upgrade, so a version bump can silently move it again. `codex --version` + a bare
  `codex exec` header is how you check what it actually resolves to.
- **Scoping a review (`exclude_patterns`) is a ROOT-checkout operation (#3229/#3234).** The daemon binds
  the repo via `repos.root_path` and reads the **ROOT checkout's** `.roborev.toml`, so editing it inside a
  worktree is a silent no-op that looks exactly like "`exclude_patterns` doesn't work" — and `roborev
  config get` answers differently depending on cwd. Edit the root checkout's file and restart the daemon.
