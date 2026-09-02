#!/usr/bin/env bash
# roborev-review.sh — THE ONLY SANCTIONED roborev INVOCATION (issue #2964).
#
# WHY THIS EXISTS
# ---------------
# `roborev`'s verdict is a MERGE CONDITION (flow-implement review-first #2086;
# flow-closer's confirmation pass before arming `gh pr merge --auto` #2084/#2667).
# A vacuous review — one where roborev reviewed NOTHING — is textually identical to
# a genuine clean one ("No issues found"), so "roborev clean" can be satisfied
# without a review having happened and the pipeline merges unreviewed code with no
# red anywhere. FOUR confirmed triggers (the fourth, the partial review, was found by
# this wrapper's own live probe and was not anticipated by issue #2964):
#
#   1. `--branch` WITHOUT an explicit `--repo`, from inside a git WORKTREE, resolves
#      against the ROOT checkout (which normally sits on `main`), so the enqueued
#      commit is `origin/main` and the branch's own change is never seen. An explicit
#      `--repo` IS what makes `--branch` correct (measured: it then reported
#      "17 commits since origin/main" and recorded `<base40>..<head40>`), so the
#      sanctioned form is `--branch --base <base> --repo <abs>` and it is `--branch`
#      WITHOUT `--repo` that is NON-SANCTIONED. (Registering the worktree is not an
#      option and is not needed: `roborev repo` has no `add` subcommand — repos
#      self-register on first use.)
#   2. the two-positional commit-RANGE form (`roborev review <a> <b>`) bases the diff
#      on git's EMPTY-TREE hash (measured `git_ref` =
#      `4b825dc642cb6eb9a060e54bf8d69288fbee4904..<head>`), so the reviewer sees a
#      fraction of the real change. => NON-SANCTIONED.
#   3. the single-SHA form (`roborev review <sha>`) reviews ONE COMMIT, not the
#      branch: on a multi-commit branch it certifies the branch from its last commit
#      alone — a PARTIAL review reported as a complete one. => NON-SANCTIONED.
#   4. a code-free (docs/spec-prose-only) diff is DISCARDED and still reported as
#      "No issues found" — with the CORRECT sha enqueued, so no sha check can catch
#      it. Mechanism MEASURED, and it is NOT a code/non-code judgement: **roborev
#      drops exactly what its configured `exclude_patterns` pathspecs match.** On a
#      census of 22 markdown + 5 code files the prompt carried diff headers for
#      exactly the 5 code files because `*.md` is CONFIGURED, so a prose-only diff
#      leaves nothing to review. => a docs-only diff — meaning a CODE-FREE CENSUS,
#      never a `docs/` path prefix — CANNOT be roborev-certified at all; the
#      sanctioned substitute is primary-source verification recorded in the PR (e.g.
#      `git show cassandra-5.0.8:<path>`). The same mechanism cuts the other way and
#      did: under a configured `docs/**` it discarded 33 EXECUTABLE harness files on
#      PR #3222 (#3229), which is why this repo's `.roborev.toml` now excludes ARTIFACT
#      EXTENSIONS inside artifact-bearing directories and never a blanket `docs/**`,
#      and why `prompt-content:` matches the CODE census against the prompt the
#      reviewer was actually given.
#
# This wrapper judges the reviewer's claims against a LOCALLY COMPUTED `git` diff
# census — never against the reviewer's own prose — and fails closed.
#
# Corollary the wrapper itself had to learn: EVERY oracle here must be the
# authoritative source, never a local proxy. The push assert asks the REMOTE via
# `git ls-remote` because CQLite clones carry a narrow fetch refspec under which a
# feature branch's `refs/remotes/origin/<branch>` mirror ref is never created — so
# reading the mirror produced a 100%-reproducible false FAIL that would have pushed
# agents back to the bare `--branch` form this wrapper exists to replace. Likewise
# `<base>` is a mirror ref: if it does not resolve, the run FAILs closed rather
# than reporting an empty census.
#
# USAGE
# -----
#   scripts/flow/roborev-review.sh --agent <agent> --model <model> \
#       [--repo <path>] [--base <ref>] [--log <path>] [--help]
#
#   --agent   REQUIRED. Reviewer agent (e.g. `codex`, `claude-code`).
#   --model   REQUIRED. Reviewer model (e.g. `gpt-5.6-sol`, `claude-opus-5`).
#             BOTH are required on purpose: `--agent codex` alone INHERITS
#             `review_model` from the repo's `.roborev.toml`, and a mismatched
#             agent/model pair hard-400s as a SILENT-LOOKING review OUTAGE
#             (#2433/#3037). Refusing here converts that outage into a usage error
#             at the call site, before anything is enqueued.
#   --repo    Target repository. Default: `git rev-parse --show-toplevel` of $PWD,
#             resolved to an ABSOLUTE path (roborev --repo must never get a
#             relative path). Always passed explicitly — never let roborev infer
#             the repo from $PWD; that inference IS trigger 1.
#   --base    Base ref for the census. Default: `origin/main`.
#   --log     Raw roborev transcript path. Default: under ${TMPDIR:-/tmp}, and
#             always NAMED in the summary block. STDOUT carries the summary block;
#             a caller retains ONLY that block, NEVER the transcript.
#
# OUTPUT CONTRACT
# ---------------
# Exactly one block, terminal `RESULT:` last, header distinct from all three
# agent-gate summary headers so neither can ever be pasted as the other:
#
#   ==== ROBOREV REVIEW SUMMARY ====
#   repo: / branch: / base: / head-sha: / reviewed-sha: / assert-base: / job: /
#   model: / census: /
#   tokens: / push-assert: / census-check: / code-free: / job-record: /
#   sha-assert: / review-completed: / prompt-content: /
#   vacuity-tier1: / vacuity-tier2: / findings: / [deferral:] / roborev-exit: / log:
#   RESULT: PASS|FAIL|NOTHING-TO-REVIEW
#
# EVERY value is ONE LINE, guaranteed (#3229 blocker 2). Diff-derived text reaches these
# values — `prompt-content:` names the census paths absent from the prompt — and a census path is
# attacker-controlled, so a NEWLINE-bearing filename could otherwise make a value span
# lines and inject its own `key:` lines, up to a forged `RESULT: PASS`. Every value goes
# through `emit_kv` and every DETAILS line through `finish`, both of which neutralise
# control characters into visible escapes (`\n`, `\r`, `\t`, else `\ooo`). See
# `roborev_safe_line` below.
#
# Per-check values, as built:
#   push-assert / census-check / code-free / review-completed  PASS | FAIL (...) | SKIP
#   job-record        PASS | PASS (no token accounting in the record) |
#                     DEGRADED (incomplete after <n> retries: <fields>) | SKIP
#   sha-assert        PASS | FAIL (...) | SKIP
#                     Asserts BOTH endpoints of the reviewed range against the CENSUS range,
#                     whose base is the MERGE-BASE of `<base>` and HEAD — `<base>...HEAD` is
#                     `merge-base(<base>, HEAD)..HEAD`, never `<base-tip>..HEAD` (#3392). The
#                     base ref's TIP is still read, for the T1 root-checkout signature alone.
#   prompt-content    PASS (<k>/<n> code census paths present) |
#                     FAIL (<k>/<n> code census paths absent from the prompt) |
#                     FAIL (no code census path was checkable — a 0/0 is never a pass) |
#                     FAIL (prompt unretrievable — ...) |
#                     WAIVED (<k>/<n> code census paths absent — authorized by @<login> for <sha>) | SKIP
#                     ONE QUESTION, NO CLASSIFIER (owner ruling (4), #3312): are the CODE census paths
#                     present in the prompt the reviewer was sent? PRESENT is a PASS; ABSENT is a FAIL,
#                     unconditionally — whatever caused it. The wrapper used to infer roborev's delivery
#                     MODE from prompt text (inline / snapshot path / delegated tier); four consecutive
#                     review rounds each found a High-severity false verdict in that inference, whose one
#                     cause was reading structure out of text that embeds repository-controlled content,
#                     so the inference is DELETED rather than patched again. THE ACCEPTED COST: a
#                     snapshot-delivered diff and a vacuous review that received nothing are IDENTICAL
#                     to the machine. A human plus the review's token accounting distinguishes them.
#   waiver            GRANTED (author=@<login> sha=<40-hex> reason=<why>) | NONE (...) | STALE (...) |
#                     MALFORMED (...) | UNAVAILABLE (...)
#                     PRESENT ONLY WHEN THE ABSENCE BRANCH RAN, so it is absent rather than
#                     placeholdered on a run that had nothing to waive. INFORMATIONAL: it is not in the
#                     verdict scan and cannot make anything pass by itself. The waiver is a DEDICATED,
#                     column-zero line of a PR comment binding base AND head AND job (see --help for the
#                     exact form, which is deliberately not repeated in any emitted diagnostic), granted by the
#                     OWNER or the coordination LEAD (a worker or closer may only REQUEST one, and must
#                     include the token accounting). It is SHA-BOUND — a push invalidates it — and it
#                     excuses the ABSENCE verdict ONLY, never any other cause. AUTHORSHIP IS
#                     PROCESS-ENFORCED WITH AN AUDIT TRAIL, NOT MECHANICALLY VERIFIED: worker, closer
#                     and owner share one GitHub login on this fleet, so no check here can tell WHICH
#                     ALLOWLISTED human posted a comment. The author IS authorized against an explicit
#                     allowlist (ROBOREV_WAIVER_AUTHORS) — a public repository prints base/head/job in
#                     the failing block, so without it any commenter could grant a waiver.
#   vacuity-tier1     PASS | FAIL (vacuous verdict vs non-empty census) |
#                     NOTICE (phrase present in a findings-bearing review) |
#                     UNAVAILABLE | SKIP        (ADVISORY when it is a NOTICE)
#   vacuity-tier2     PASS | FAIL (...) | UNAVAILABLE | SKIP
#   findings          NONE | PRESENT [(<n>)] | INCONSISTENT (...) | UNKNOWN | SKIP |
#                     DEFERRED (<n>, issues=#<N>[,#<N>...], authorized @<login>, job <id>)
#                     ONLY an affirmative `NONE` permits a PASS, IN EVERY MODE including
#                     `--recheck-job`, and that requirement is NOT WAIVABLE (#3564). It is
#                     enforced in step 7 on its own terms rather than by the neighbouring
#                     `roborev-exit` key, which is legitimately `SKIP` on a recheck.
#                     THE ONE OTHER PERMITTED VALUE IS `DEFERRED` (#3626): "roborev clean"
#                     means NO UNADDRESSED FINDINGS, not "the tool printed zero", and a
#                     LEAD-DEFERRED finding is re-reported by every later round — so the
#                     affirmative-`NONE` rule, correct in itself, blocked such a merge
#                     forever. `DEFERRED` rides ONLY on an authorized, affirmatively matched
#                     deferral (see `deferral` below); it is NEVER `NONE`, so nobody grepping
#                     `findings: NONE` reads a deferred run as clean.
#   deferral          GRANTED (author=@<login> issues=<N>,<N> count=<n> scope=base=<…>
#                     head=<…> job=<id> reason=<why>) | NONE (...) | STALE (...) |
#                     MALFORMED (...) | UNAUTHORIZED (...) | COUNT-MISMATCH (...) |
#                     ISSUE-ABSENT (...) | ISSUE-CLOSED (...) | ISSUE-UNVERIFIABLE (...) |
#                     UNAVAILABLE (...)
#                     PRESENT ONLY WHEN THE FINDINGS BRANCH HAD A DEFERRAL TO LOOK FOR (a
#                     `--recheck-job` over an affirmatively measured `PRESENT (n)`), so it is
#                     absent rather than placeholdered otherwise. INFORMATIONAL, exactly like
#                     `waiver:`: it is not in the verdict scan and cannot make anything pass
#                     by itself. A deferral is a DEDICATED, column-zero line that is the SOLE
#                     NONBLANK CONTENT of a TOP-LEVEL PR comment from an author on
#                     ROBOREV_WAIVER_AUTHORS, binding base AND head AND job AND the finding
#                     COUNT AND the filed issue numbers, each of which must be an OPEN
#                     issue GitHub confirms — four-valued, so an issue that does not exist
#                     (ISSUE-ABSENT), one GitHub says is CLOSED (ISSUE-CLOSED) and one whose
#                     existence could not be ASKED (ISSUE-UNVERIFIABLE) are separate
#                     non-granting states and none reads as verified. The PR BODY is not
#                     consulted at all: a body is editable AT ANY TIME by anyone with write
#                     access with no per-edit attribution, while a comment is permanent and
#                     attributable (#3626, see --help for the exact form, which is
#                     deliberately not repeated in any emitted diagnostic). It is granted
#                     by the OWNER or the coordination LEAD; a worker may only REQUEST one.
#                     SEPARATELY SCOPED FROM THE WAIVER, and that separation is load-bearing:
#                     an absence waiver confers NO authority over `findings:`, a findings
#                     deferral confers NONE over `prompt-content:`, and neither falls back to
#                     the other — collapsing them would let a delivery-artifact waiver excuse
#                     a real defect. `findings: UNKNOWN` and `SKIP` are NOT deferrable in any
#                     mode: those states were never ESTABLISHED, and a pass may not rest on a
#                     state that could not be read.
#   roborev-exit      PASS | FINDINGS (exit N) | ERROR (exit N) | SKIP
#   model             <model> | <model> (SUBSTITUTED — requested '<r>') |
#                     <model> (UNCONFIRMED — no model field in the job record) | -
#   census            `<N> file(s), +<A>/-<D>` | -
#   tokens            `input=<n> cached=<n> output=<n>` | UNAVAILABLE
# FINDINGS means the reviewer RAN and reported findings (a GENUINE review to triage and
# fix); ERROR means the reviewer itself failed; INCONSISTENT means a "clean" signal is
# contradicted by markers in the findings block.
#
# THE VERDICT SCAN: a key fails the run when its value starts with FAIL, FINDINGS,
# ERROR or INCONSISTENT. PASS, SKIP, UNAVAILABLE, NOTICE and DEGRADED never do —
# NOTICE is tier 1's advisory value, and DEGRADED reports an incomplete job record
# whose consequences are carried by the dependent asserts (which fail on their own
# terms). THE GRAMMAR IS CLOSED: the non-failing set is an ALLOW-LIST (PASS, SKIP,
# NOTICE, UNAVAILABLE, DEGRADED, and findings:'s NONE/PRESENT/UNKNOWN), so a value
# outside it — an empty string, a state a future check invents — is an UNRECOGNISED
# VERDICT and FAILS. Testing only the bad states would let every unplanned one inherit
# the permissive branch, which is the general shape of three separate defects found on
# #3229 (see step 7).
# GRAMMAR RECOGNITION IS NOT A PASS, and conflating the two was #3564: `PRESENT` is
# RECOGNISED here — it is a documented findings state, not a typo — and that is all this
# scan says about it. Whether a value may ride to `RESULT: PASS` is decided by the two
# AFFIRMATIVE gates beside it in step 7: `findings:` must read exactly `NONE`, and every
# deterministic key must read exactly `PASS`.
#
# THE RULE THAT GOVERNS EVERY KEY HERE, and the one to apply when adding another:
# **A POSITIVE VERDICT REQUIRES AN AFFIRMATIVE MEASUREMENT.** Never derive a pass from
# the absence of a bad signal. Where an oracle is the SOLE evidence for a claim and it
# could not be consulted, the verdict is NON-PASSING, and its text must distinguish
# "we could not check" from "nothing was wrong" — naming what was unverifiable and what
# would have verified it. Where a signal has more than two states, key the permissive
# branch on the AFFIRMATIVE value (`= OK`), never on "not the bad one" (`!= DIVERGED`),
# so an unknown state fails closed. Where a signal genuinely SHOULD be permissive (an
# oracle that only cross-checks something already measured — `corroboration:` with
# patterns parsed), say so IN CODE with the reason, so the next reader does not have to
# re-derive it and the next edit does not silently widen it.
#
# WHICH CHECKS CARRY THE VERDICT. The DETERMINISTIC ones, each judged against data we
# obtained ourselves: `push-assert` (the remote, via ls-remote), `census-check` (our
# own git diff), `code-free` (our own census classification), `sha-assert` (the job
# record's git_ref, asserting BOTH endpoints of the reviewed range against the census
# range), `review-completed` (job status + an allow-list of terminal verdict markers),
# `prompt-content` (the CODE subset of our census inside the prompt actually sent). Prose matching (`vacuity-tier1`) and token accounting (`vacuity-tier2`)
# CORROBORATE; tier 1 can only ever raise a NOTICE. `base:`, `head-sha:`, `reviewed-sha:`,
# `assert-base:`, `census:`, `tokens:`, `waiver:` and `deferral:` are INFORMATIONAL — they are in neither
# the verdict-grammar scan nor the affirmation loop (both enumerate the verdict-carrying keys
# by name), so none of them can make a run pass or fail on its own.
#
# EXIT CODES (exactly three outcomes plus a usage code)
#   0  PASS               — a review demonstrably HAPPENED against branch HEAD with
#                           no vacuity signal. PASS requires POSITIVE evidence; it is
#                           never inferred from the absence of a bad phrase.
#   1  FAIL               — any failed check: push-assert, census-check, code-free,
#                           sha-assert, review-completed, prompt-content,
#                           vacuity-tier1, vacuity-tier2, findings (INCONSISTENT), or
#                           roborev-exit (FINDINGS or ERROR). Each names itself under
#                           its own key. NOT reportable as "roborev clean".
#   3  NOTHING-TO-REVIEW  — the census is genuinely empty; NO review was enqueued.
#                           DISTINCT from PASS by exit code alone, so a caller can
#                           never mistake "nothing to review" for "reviewed clean".
#   2  usage error        — a missing/invalid option, detected before any repo
#                           identity is resolved and before anything is enqueued.
#                           This path prints `ERROR: ...` and NO summary block: no
#                           review was attempted, so there is no verdict to state,
#                           and emitting a `RESULT:` line would alias a usage error
#                           onto one of the three real outcomes.
#
# LIVE WORKTREE PROBE (documented, NOT gate-run — needs network + a live reviewer)
# -------------------------------------------------------------------------------
# The hermetic regression check (scripts/tests/test_roborev_review_guard.sh) pins
# every guard against a stub reviewer. Only the live probe can show the REAL binary
# honours the explicit `--repo` from inside a worktree. Its full procedure and the
# summary-block values it must produce are in the `--help` output (the wrapper's
# usage documentation) — run `roborev-review.sh --help` rather than duplicating it
# here, so the two can never drift apart.
set -euo pipefail

# --- Vacuity signals: two DETERMINISTIC checks, then ADVISORY token accounting --
#
# The load-bearing checks are threshold-free and judged against our own locally
# computed census:
#   * verdict-text-vs-census (tier 1) catches "the reviewer GOT the diff and
#     DISCARDED it" — the code-free-diff trigger T3.
#   * prompt-content catches "the reviewer NEVER GOT the diff" — triggers T1/T2 and
#     any future variant of them, since it reads the prompt actually sent and looks
#     for our census's own file paths in it.
# Together they cover both halves deterministically. Token accounting is a THIRD,
# ADVISORY-corroborating signal that can only ever fail CLOSED.
#
# MEASURED EVIDENCE (issue #2964, real jobs on the fleet):
#   VACUOUS baseline:  17,333-18,801 input, 0 cached, 21-56 output, 8s wall
#                      (jobs 4651 / 4658 / 4659 — reproducible)
#   GENUINE, SMALL:    67,387 input, 43,520 cached, 2,232 output, 68s wall
#                      (job 1 on this branch: 20 files, +2279)
#   GENUINE, LARGE:    398k-649k input, 314k-554k cached, 5.0k-6.3k output
#                      (jobs 4652 / 4654 / 4656)
#
# INPUT FLOOR is anchored on the VACUOUS CEILING, not the genuine band: the
# genuine band scales with diff size, so a floor derived from it false-FAILs small
# genuine reviews. 25,000 sits ~1.33x above the highest observed vacuous run
# (18,801) and ~2.7x below the smallest observed genuine run (67,387). The
# original 50,000 was only 1.35x below that 67k run — one modestly smaller genuine
# diff away from an always-red guard, which is the failure mode that gets a guard
# bypassed (cf. the mirror-ref push-assert regression).
#
# OUTPUT TOKENS ARE ADVISORY ONLY — NEVER a FAIL condition. A genuine CLEAN review
# and a vacuous one emit nearly IDENTICAL output: both are "No issues found." plus
# a one-sentence summary (~20-60 tokens; the vacuous baseline measured 21-56). The
# counts therefore COLLIDE, and any output floor would false-FAIL precisely the
# case we care most about — a real review that is legitimately clean. Reported for
# the operator, never asserted.
#
# CACHED == 0 stays a FAIL condition. It is the most false-positive-prone term
# (a genuinely cold cache can report 0), retained deliberately in the fail-closed
# direction — acceptable now that prompt-content gives a deterministic primary
# check, so tier 2 is no longer the only thing standing between us and a vacuous
# pass. Wall time is deliberately NOT asserted (host-dependent — #2642).
ROBOREV_VACUITY_MIN_INPUT_TOKENS=25000
# Advisory only (see above): reported next to the observed value, never a FAIL.
ROBOREV_VACUITY_ADVISORY_MIN_OUTPUT_TOKENS=200

# (The former PROMPT_CONTENT_MAX_PATHS_CHECKED sampling cap is gone: every code path is
# now checked against its exact `diff --git` header, cheap even for a 500-file diff, and
# sampling was a hole — a partial prompt naming just the sampled files passed.)

# Job-record read retries. There is NO asynchronous write to wait out (an earlier
# diagnosis to that effect was wrong and is retracted): `roborev show <id> --json`
# nests the job row under a "job" key while `roborev list --json` carries the same
# fields at top level, and the extractor was matching the outer REVIEW row. With the
# nested row preferred the record is complete in ONE read, so this is a short SANITY
# retry (transient read failure, not-yet-terminal status): 5 x 1s. It still fails
# closed if the record genuinely cannot be read.
# Overridable ONLY as a timing knob (the hermetic self-test shortens it). Shortening
# it can never weaken a check: fewer polls can only make the record MORE likely to be
# reported DEGRADED, which is the fail-closed direction.
JOB_RECORD_POLL_ATTEMPTS=${ROBOREV_JOB_RECORD_POLL_ATTEMPTS:-5}
JOB_RECORD_POLL_INTERVAL_SECS=${ROBOREV_JOB_RECORD_POLL_INTERVAL_SECS:-1}


PROGNAME=$(basename "$0")
# Resolve helpers from THIS FILE's directory (BASH_SOURCE), never $PWD: the wrapper
# is invoked from arbitrary worktrees and a $PWD-relative lookup would silently miss.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ORACLES_FILE="$SCRIPT_DIR/roborev-review-oracles.sh"
CHECKS_FILE="$SCRIPT_DIR/roborev-review-checks.sh"
JOB_FACTS_TOOL="$SCRIPT_DIR/roborev-job-facts.py"

usage() {
  cat <<EOF
$PROGNAME — the only sanctioned roborev invocation (issue #2964)

Usage:
  $PROGNAME --agent <agent> --model <model> [--repo <path>] [--base <ref>]
                     [--log <path>] [--help]

Options:
  --agent <agent>  REQUIRED reviewer agent (codex, claude-code, ...).
  --model <model>  REQUIRED reviewer model (gpt-5.6-sol, claude-opus-5, ...).
                   Both are required: supplying only one inherits a mismatched
                   model from .roborev.toml and fails as a silent-looking outage.
  --repo <path>    Target repo (default: git toplevel of \$PWD, absolutised).
  --base <ref>     Census base ref (default: origin/main).
  --log <path>     Raw transcript path (default: under \${TMPDIR:-/tmp}).
  --help           This text.

Outcome: one '==== ROBOREV REVIEW SUMMARY ====' block on stdout, terminal
RESULT: PASS|FAIL|NOTHING-TO-REVIEW. Exit 0=PASS, 1=FAIL, 3=NOTHING-TO-REVIEW,
2=usage error. Retain the block, never the transcript.

Sanctioned invocation (measured, issue #2964 round 5):
  roborev review --branch --base <base> --repo <abs> --agent <a> --model <m> --wait
which reviews the RANGE <base>..HEAD, i.e. exactly the census. Non-sanctioned:
'--branch' WITHOUT an explicit --repo (resolves against the ROOT checkout from a
worktree); the two-positional commit-range form (anchors the range at git's EMPTY
TREE); and a single-sha review (covers ONE COMMIT, so it certifies a branch from
its last commit alone).
A docs-only diff cannot be roborev-certified at all — record primary-source
verification in the PR instead of "roborev clean". "docs-only" means a CODE-FREE
CENSUS as code-free: classifies it, NEVER a 'docs/' path prefix: the measurement
harnesses this repo ships under docs/reports/*-artifacts/ are executable code that
IS reviewed, so a PR carrying them is not a docs-only change (issue #3229).

HOW ROBOREV DROPS PATHS, stated correctly because the old claim was FALSIFIED:
roborev drops exactly what its exclusion pathspecs match (the repo/global
exclude_patterns plus a compiled-in lockfile/cache deny-list) and makes NO
code/non-code judgement of its own. A markdown-only diff arrives EMPTY because
'*.md' is CONFIGURED, not because a reviewer recognised prose — and the same
mechanism cuts the other way: a configured 'docs/**' discarded 33 EXECUTABLE
harness files on PR #3222 (#3229). NOTHING HERE PREDICTS THAT SET. There is no
pre-enqueue key reconciling the census against it: the oracle that tried was
removed (#3283 for the configured half, #3278 for the built-ins) because it
produced false-PASSes faster than review rounds closed them. Consequence: a path
the reviewer did not receive surfaces AFTER the review, under prompt-content:,
whose cause names the symptom rather than the mechanism. Fail-closed, never green
— but if prompt-content: FAILs, SUSPECT .roborev.toml first.

ONE QUESTION, NO DELIVERY CLASSIFIER (issue #3312, owner ruling (4)): prompt-content:
asks only whether the CODE census paths are present in the prompt the reviewer was
sent. PRESENT is a PASS. ABSENT is a FAIL, unconditionally, whatever caused it.

This wrapper used to infer HOW roborev delivered the diff — inlined in the prompt, or
written to a transient file whose path the prompt names, or the delegated tier that
ships neither and tells the reviewer to run git itself. Four consecutive review rounds
each found a High-severity false verdict in that inference, in BOTH directions, and
every one had the same cause: it read structure out of prompt text, and roborev's
prompt embeds repository-controlled content (project guidelines, AGENTS.md sections,
previous-review bodies) at column zero, indistinguishable from roborev's own. No
terminating marker exists — the only structural one was roborev's fenced diff, and
repository content can contain fences too — so the inference was DELETED rather than
patched a fifth time. Block detection, heading parsing, fence evidence, mixed-delivery,
candidate lifetime and the snapshot/delegated distinction are all gone with it.

THE ACCEPTED COST, stated because it is real: a diff roborev delivered BY PATH and a
vacuous review that received NOTHING are IDENTICAL to the machine — both have no
census paths in the prompt, so both FAIL. What distinguishes them is a HUMAN plus the
review's token accounting (genuine reviews measured 398k-649k input / 314k-554k cached;
the vacuous baseline is ~18.7k input / 0 cached).

THAT SENTENCE IS TRUE AT REVIEW TIME, AND FALSE AFTER THE FACT ONLY FOR A HUMAN READING
THE STORED RECORD — IT STAYS TRUE OF THE MACHINE, AND MUST (#3654). The prompt roborev
SENT is retained in the job record and can be retrieved later, even though the snapshot
file it names is transient and long deleted:

    roborev show <id> --prompt

Under '### Combined Diff' a delivery-by-path prompt carries roborev's own wording — 'Diff
too large to include inline ... Read the diff from: <path>'. That is the DIRECT ARTIFACT:
roborev's ACTUAL prompt rather than a statistic about it.

WHAT THE TWO SIGNALS CAN AND CANNOT ESTABLISH. THE PROMPT IS NOT SELF-AUTHENTICATING:
roborev's prompt EMBEDS repository-controlled content — project guidelines, AGENTS.md,
additional context, previous-review bodies — at positions indistinguishable from roborev's
own text, so a reviewed branch can carry text MIMICKING that delivery wording and an
authorizer would read it as roborev's. A human in the loop is not a channel separation; it
is the same shared channel with a slower parser. So the prompt reports a STRUCTURAL fact
and is never proof of its own provenance. THE TOKEN ACCOUNTING IS DAEMON-RECORDED BUT NOT
INDEPENDENT: the RECORD is authentic — the daemon writes the counts and the reviewed branch
cannot rewrite them — but their VALUE measures THE PROMPT, and the prompt embeds
repository-controlled content, so a branch influences their MAGNITUDE without forging
anything. That bites exactly where the counts are used — the vacuous baseline is about
18.7k input / 0 cached, so padding non-diff prompt content can make a review that never
received the diff look token-rich. So NEITHER SIGNAL ESTABLISHES PROVENANCE, and the two
are NOT INDEPENDENT: both are functions of the same repository-influenced prompt.

WHICH EVIDENCE A WAIVER SHOULD REST ON IS AN OPEN QUESTION, TRACKED AS #3826. Nothing
here recommends one signal over the other, or any ordering between them.

THIS RESURRECTS NOTHING OF THE DELETED DELIVERY CLASSIFIER, and the distinction is
load-bearing rather than a caveat. The classifier failed because it inferred delivery
MODE from injectable prompt text AT DECISION TIME, to produce an AUTOMATED verdict —
roborev's prompt embeds repository-controlled content, so that inference was spoofable
in both directions. What is described here is a HUMAN reading a STORED record as
evidence for a HAND-GRANTED waiver, so the direct PARSER exploit is gone: nothing in
this wrapper parses the prompt for delivery mode, and nothing may be added that does.
THAT IS ALL IT BUYS. The human is IN the path, not outside it — spoofed
repository-controlled prompt text can mislead an authorizer into issuing the marker, and
the marker is what makes '--recheck-job' pass. That exposure is #3826's subject and is
NOT settled here.

THE WAIVER, therefore: the OWNER or the coordination LEAD may excuse an absence FAIL
with a PR comment that carries this as a DEDICATED LINE, at column zero, all four
fields present:

    roborev-waive: prompt-content-absent base=<40-hex> head=<40-hex> job=<id> reason=<why>

AND THEN APPLY IT WITH A RECHECK, which is what closes the loop:

    bash scripts/flow/roborev-review.sh --repo <abs> --recheck-job <id> \
      --agent <agent> --model <model>

A recheck RE-DECIDES that job's verdict and ENQUEUES NOTHING. It exists because the
waiver names a job, the operator only learns the job id (and the token accounting)
from the FINISHED run, and re-running the wrapper would enqueue a DIFFERENT job —
making the fresh waiver instantly STALE. Without it the mechanism was a dead letter.
The job is named EXPLICITLY, never resolved from base+head: a resolver would let a
re-run inherit a waiver written for a different review, which is the hole the job
binding closes.

A recheck INHERITS NOTHING from the original run. sha-assert re-compares the record's
git_ref against this base and head — 'this base' being the MERGE-BASE of --base and HEAD,
the base of the range under review, which the block names under assert-base: (#3392); the record's own review text becomes the
transcript, so review-completed, both vacuity tiers and findings are re-asserted from
it (a record with no review text leaves the transcript empty, which fails closed);
roborev-exit reports SKIP rather than claiming an exit status for a process that did
not run. The block declares 'MODE: recheck (job <id> …; NO review was enqueued)' and
'recheck-of: <id>' as its first keys, the way the gate declares MODE: lite, so a
recheck PASS can never be pasted as evidence of a fresh review.

A worker or a closer may REQUEST one — one comment, including the token accounting —
and may never apply it to its own PR.

IT IS BOUND TO THE WHOLE REVIEW SCOPE, not just the head: base AND head AND job are all
required and all verified. The base= field is the base OF THE REVIEWED RANGE — the
merge-base of the --base ref and HEAD, which the block prints under the assert-base: key — and NOT
the tip of the base ref (#3392). Copy it from the assert-base: line of the failing block,
never from the base: line; the two name the same commit only while the branch is not behind
its base, and binding to the tip made a waiver go STALE the instant the base ref advanced,
which is what made this mechanism a dead letter under fleet load. The authorizer's judgment under (d) was about ONE review and
its token accounting, so the waiver may not outlive it — a push, a different base or a
re-run each need a fresh one. A marker missing any field is MALFORMED, never granted.

JOB IDS ARE PER-DAEMON, NOT GLOBAL, SO VERIFY THE RECORD'S git_ref AND NEVER THE ID
ALONE (#3654). Every fleet box runs its own roborev daemon with its own database and its
own sequential ids, so two boxes can legitimately present the SAME id for DIFFERENT
reviews — measured: 'job=265' on two lanes 50 minutes apart, different ranges, different
branches, different token counts, both correct. A repeated id is therefore NOT evidence
of a collision; reading it as one cost a valid waiver a round. The check that settles it:

    roborev show <id> --json | jq '.job | {id, git_ref, branch, status, token_usage}'
    roborev list --json --limit 200 --repo <abs-repo> --branch <branch> | jq '.[] | select(.id==<id>) | {id, git_ref, branch}'

git_ref MUST equal the marker's <base40>..<head40>. FOUR TRAPS IN THOSE COMMANDS, all
measured on roborev v0.61.2: (1) 'show <id> --json' NESTS git_ref/status/token_usage
under '.job', so a top-level jq over that payload prints nulls for all of them — a check
whose output cannot show what it claims;
(2) 'roborev list' defaults its branch filter to the CURRENT HEAD OF THE --repo PATH —
NOT to the branch your shell is standing in (measured: from a cwd that is not a git
repository at all, '--repo <lane>' returns that lane's branch's rows) — so pass --branch
explicitly whenever that checkout is not on the job's branch, which is exactly the
'--recheck-job' case, or a correct query returns null; (3) the TOP-LEVEL 'id' of a 'show' payload is the REVIEW
row's own sequence and is NOT necessarily the job you asked for — measured over ten
records, asking for 9 returns id=8 with job_id=9 and job.id=9. So read '.job' (or
'job_id'), never the top-level 'id', or the answer manufactures exactly the "is this the
right review?" doubt this section exists to remove. The wrapper is unaffected: its
extractor matches id/job_id/job and then PREFERS the object carrying git_ref, so it lands
on the job row either way — this is a trap for the HUMAN running the check by hand.
(4) 'roborev list' returns a BOUNDED WINDOW of the most recent rows — '--limit' defaults
to 50 (measured: 'roborev list --help', v0.61.2) — so an older job is simply not in a
default read and the query yields NOTHING though the record exists —
an absence indistinguishable from 'no such job', which breaks this whole procedure for
exactly the reviews a waiver argument reaches back to. So pass '--limit' EXPLICITLY and
RAISE it until the job appears, or until the returned row count STOPS GROWING (at which
point the window covers the whole local table and the record really is absent). An empty
result at a limit that is still growing the row count is UNMEASURED, not an answer. This
is also what the row count below cannot survive: a count of 1 says nothing about the
window it was taken over.

AND A LOCAL ROW COUNT IS NOT EVIDENCE OF UNIQUENESS. This, which reads like a collision
check, is not one:

    roborev list --json ... | jq '[.[] | select(.id==<id>)] | length'    # never more than 1

'roborev list' only ever sees the LOCAL daemon, so it returns 1 whether or not another
box holds the same id — and 0 when the row fell outside the '--limit' window, which is
not a collision answer either. It is structurally incapable of detecting the cross-box collision
it appears to rule out — a probe whose output is IDENTICAL under the two states it claims
to separate, the same class as reading the gate's 'RESULT: INCOMPLETE' launch sentinel as
a verdict, or locating a gate run directory with 'ls -t'. Run on both of the 'job=265'
lanes, it gave the right answer for a reason that did not hold. Use git_ref.

WHAT THE git_ref CHECK SETTLES, AND WHAT IS NOT CLAIMED (#3654). It settles that the id
names the review you think it does ON THIS DAEMON. It does NOT settle the cross-box
question: two daemons can hold the SAME id for the SAME git_ref range, so a waiver
authorized against machine A's review can be accepted by '--recheck-job' against machine
B's DIFFERENT review of that range — and no local lookup can detect it, because
'roborev list' only ever sees the LOCAL daemon. The marker travels through GITHUB while
'--recheck-job' reads the local daemon, so nothing here binds an authorization to one
BOX. That residual is NOT CLAIMED and NOT closed here: closing it is #3825, which carries
the 'job-machine:' key and the marker-grammar question that comes with it.

THREE THINGS STOP THE DOCUMENTATION BECOMING THE CREDENTIAL. (1) The marker must BE the
line: an indented, '>'-quoted, bulleted or mid-sentence copy does not match, so pasting
a block or quoting an example grants nothing. (2) A placeholder reason is refused — an
unsubstituted '<...>' or a bare 'why'/'todo'/'tbd' — so a pasted TEMPLATE (including the
line above) reads MALFORMED. (3) The absence-FAIL diagnostic prints NO part of the
marker; it points here instead. Summary blocks get pasted into PR comments as a matter
of course, and before this a pasted block silently authorized the next run.

It excuses the ABSENCE verdict ONLY: any other cause (an unretrievable prompt, a 0/0
census, a failed sha assert, a review that never completed) is reached on a different
path and is untouched. The block then reports prompt-content: WAIVED (...) — a DISTINCT
token, so no reader grepping 'prompt-content: PASS' mistakes it for a certification —
beside a waiver: key recording the authorizer, the bound scope, the reason and the
absent paths. Never silence.

THREAT MODEL, STATED WITH ITS LIMITS. A HOSTILE INVOKER IS OUT OF SCOPE: whoever
runs this wrapper can edit it, replace the scanner beside it, shadow gh on PATH, or
skip it entirely and hand-write a summary block into the PR. No check inside a
process defends against the party controlling that process. The merge gate's real
protection against a hostile worker is the audit trail plus a human reading the PR.

What the waiver DOES defend: (1) parties who do NOT control the invocation — anyone
can comment on a public PR, and the failing block prints base/head/job, so the
allowlist, the anchored marker and the structured author association are what stop a
stranger granting one; (2) accident and drift — a pasted block, a quoted example, a
stale waiver riding to a later review, a re-run inheriting someone else's
authorization, an unsubstituted placeholder.

TRIAGE: a finding of the form "the invoker can bypass this" is OUT OF MODEL and
should be recorded rather than patched. "A non-invoker can bypass this", or "this can
be bypassed by accident", is a DEFECT. Same-host actors who can write these scripts
are invoker-class, not third parties.

SHAPE AND CHANNEL, both load-bearing:

  * THE MARKER MUST BE THE SOLE NONBLANK CONTENT OF ITS COMMENT. Leading and trailing
    blank lines are fine; anything else — prose, a code fence, a quote, an HTML tag, a
    second sentence — means the comment is not an authorization. Put commentary in a
    SEPARATE comment; the token accounting belongs inside reason= anyway. This replaced
    four successive Markdown recognisers (anywhere-in-comment, column-zero anchor,
    fence skipping, fence-state tracking): deciding "data or control?" inside a grammar
    the author controls is unbounded, and no quoting construct can be the ONLY thing in
    a comment, so quoting cannot grant.
  * THE COMMENT MUST BE TOP-LEVEL. Markers inside a review body or a review-thread
    reply are not read (fail-closed, but it looks like the waiver was ignored).

THE AUTHOR MUST BE ON AN EXPLICIT ALLOWLIST (see ROBOREV_WAIVER_AUTHORS in
roborev-review-oracles.sh). This is a PUBLIC repository and a failing block PRINTS the
base, head and job, so without that check any commenter could copy them and make the
merge gate pass. A comment from an author outside the allowlist reports
'waiver: UNAUTHORIZED (...)' and grants nothing. The list is hard-coded in the wrapper
rather than read from a config or an env var: an override would be settable by the very
party it constrains, and one visible location keeps "who may grant" in the same diff a
reviewer already reads.

BEYOND THAT, AUTHORSHIP IS PROCESS-ENFORCED WITH AN AUDIT TRAIL, NOT MECHANICALLY
VERIFIED — and that residual is now narrow: on this fleet the worker, the closer and the
owner all post through the SAME login, so no check here can tell WHICH ALLOWLISTED HUMAN
posted a comment. "Only the owner or the coordination lead may GRANT; a worker may only
REQUEST" therefore rests on process and on the comment being permanently attributable.
The earlier, broader claim ("authorship cannot be verified at all") is what justified
having NO author check, which is how any commenter could grant one — an unenforceable
claim gets SCOPED to what is true, never dropped whole.

THE FINDINGS DEFERRAL (issue #3626) — A SECOND, SEPARATELY SCOPED AUTHORIZATION.
"roborev clean" means NO UNADDRESSED FINDINGS, not "the tool printed zero". A
lead-deferred finding is re-reported by every later round, so 'findings: PRESENT (n)'
persists and the affirmative-NONE requirement — correct in itself — blocked such a
merge FOREVER (measured on PR #3572 job 262: two findings, ZERO new, both already
filed and both already lead-deferred). The OWNER or the coordination LEAD may record
that deferral with a PR comment carrying this as a DEDICATED LINE, at column zero,
every field present:

    roborev-defer: findings issues=<N>[,<N>...] count=<n> base=<40-hex> head=<40-hex> job=<id> reason=<why>

AND THEN APPLY IT WITH A RECHECK, exactly as the absence waiver is applied:

    bash scripts/flow/roborev-review.sh --repo <abs> --recheck-job <id> \
      --agent <agent> --model <model>

count= IS THE AFFIRMATIVE HALF OF THE BINDING, and it is why this is a match rather
than a mute button. A job is a completed review and its findings do not change, so
job= already fixes the finding SET; requiring the declared count to EQUAL the observed
one means a PRE-AUTHORIZATION (written before the findings were read) fails on a
mismatch instead of passing silently, and ANY NEW finding at the same head raises the
observed count and fails too. That is how the UNDEFERRED set is computed without a
per-finding identity, which roborev's prose does not provide — and no such identity is
reconstructed from that prose, because a recogniser over author-controlled text is the
class #3564 closed by REMOVING prose reconstruction.

issues= RECORDS THAT THE FINDING IS TRACKED, and each number must be an OPEN GitHub
issue — asked of GitHub, and FOUR-VALUED: 'the issue does not exist' (ISSUE-ABSENT),
'GitHub says it is CLOSED' (ISSUE-CLOSED) and 'this box could not ask'
(ISSUE-UNVERIFIABLE) are separate non-granting states, because gh issue view exits 1
for the first and third and 0 for the second, and only two of the three are ANSWERS. A
deferral naming an issue that does not exist is a dropped finding wearing a link, and
one naming an issue closed as a duplicate three weeks ago is the same thing with a
better disguise — so OPEN is required, which is deliberately STRONGER than
'retrievable'. A false refusal is recoverable (reopen it, or file a fresh tracking
issue and re-authorize) and is the fail-closed direction.

NO PR-BODY LINK IS REQUIRED, DELIBERATELY (#3626). An earlier version also demanded a
visible #<N> reference in the PR BODY. It was removed, not fixed: a PR body is EDITABLE
AT ANY TIME BY ANYONE WITH WRITE ACCESS, WITH NO PER-EDIT ATTRIBUTION, whereas a
top-level comment is PERMANENT AND ATTRIBUTABLE — so the body was the weaker artifact,
and would stay weaker even if Markdown parsed trivially. Its Markdown recognisers
leaked in two successive review rounds (multi-backtick code spans, explicit links,
and more unhandled) for the same reason every recogniser over author-controlled text
leaks; the census and the argument are recorded at the deleted site in
scripts/flow/roborev-waiver-scan.py.
The ruling needs no second artifact: the authorization comment is permanent,
attributable and in the PR.

WHAT IT REPORTS: 'findings: DEFERRED (<n>, issues=#…, authorized @<login>, job <id>)'
— a DISTINCT token, NEVER 'NONE', so no reader grepping for a clean review finds a
deferred one — beside a 'deferral: GRANTED (...)' key recording the author, the issue
numbers, the count, the bound scope and the reason VERBATIM. Every non-granting state
speaks under its own name and leaves the FAIL: NONE / STALE / MALFORMED / UNAUTHORIZED
/ COUNT-MISMATCH / ISSUE-ABSENT / ISSUE-CLOSED / ISSUE-UNVERIFIABLE / UNAVAILABLE.

NOT DEFERRABLE, IN ANY MODE: 'findings: UNKNOWN' and 'findings: SKIP'. Those values
mean the findings state was never ESTABLISHED — we cannot count what we cannot see, and
a deferral over one would be a pass resting on a state we could not read.

SEPARATELY SCOPED FROM THE ABSENCE WAIVER, and that is a constraint rather than an
accident: distinct marker keywords, distinct summary keys, distinct verdict tokens, and
NEITHER reads the other's marker or falls back to it. An absence waiver confers no
authority over findings; a findings deferral confers none over prompt-content. A run may
legitimately carry both, each granted on its own marker. Collapsing them would let a
delivery-artifact waiver excuse a real defect.

THE CHANNEL, THE ALLOWLIST, THE PLACEHOLDER REFUSAL, THE CREDENTIAL RULE AND THE THREAT
MODEL ARE THE WAIVER'S, INHERITED BY CALL: sole nonblank content of a TOP-LEVEL comment,
an author on ROBOREV_WAIVER_AUTHORS, a substantive reason (no '<...>', no bare
why/todo/tbd), no part of the form in any emitted diagnostic, one enforcer resolved from
the wrapper's own directory with no override. There is deliberately NO flag, NO file in
the worktree and NO environment variable by which a deferral can be asserted: each would
hand the reviewed party the power to satisfy its own constraint.

LIVE WORKTREE PROBE (documented, NOT gate-run: needs network + a live reviewer).
Only this probe can show the REAL binary honours the explicit --repo from inside
a worktree; the gate's hermetic check uses a stub reviewer.
  1. Confirm the ROOT checkout sits on main (what makes the trigger reproducible):
       git -C <root> rev-parse --abbrev-ref HEAD    # => main
  2. From a real issue worktree on its own branch, with its commit PUSHED:
       cd /path/to/cqlite-wt/issue-<N>
       $PROGNAME --agent codex --model gpt-5.6-sol
  3. In the emitted block assert the reviewed SCOPE covers the worktree, remembering
     that reviewed-sha is a RANGE '<base40>..<head40>', never a single sha:
       - head-sha == the worktree branch HEAD (git rev-parse HEAD);
       - sha-assert: PASS;
       - reviewed-sha ENDS IN that same head-sha, and its base endpoint (before '..')
         == git merge-base <base> HEAD — i.e. the reviewed range IS <base>...HEAD.
         NOT git rev-parse <base>: the tip and the merge-base are the same commit only
         while the branch is not behind <base> (#3392). assert-base: names the sha the
         assert used, and the <base> tip beside it, so the two are never confused;
       - reviewed-sha is NOT the base ref alone, and its head endpoint is NOT the base
         sha: either means the review never reached the worktree's own commits, which
         is the root-checkout resolution this probe exists to rule out;
       - prompt-content: PASS with the full code census covered;
       - census matching 'git diff --numstat -z --no-renames <base>...HEAD';
       - job-record: PASS and tokens above both thresholds.
     RESULT is PASS (exit 0) only when the review is also finding-free; a review with
     open findings correctly reports FINDINGS and exits 1 — that is not a probe
     failure, and the scope assertions above are what the probe is for.
  4. Record the observed head-sha/reviewed-sha/assert-base/job/census/tokens in the PR body,
     and re-run the probe after any roborev version bump.
EOF
}

# shellcheck disable=SC2016 # the backticks in these messages are prose, not expansions
die_usage() { # die_usage <message>
  printf 'ERROR: %s\n' "$1" >&2
  printf 'ERROR: run `%s --help` for the option contract.\n' "$PROGNAME" >&2
  exit 2
}

# --- argument parsing ----------------------------------------------------------
AGENT=""
MODEL=""
REPO_ARG=""
BASE="origin/main"
# ===== RECHECK MODE (#3312 job 24): RE-EVALUATE A COMPLETED JOB, ENQUEUE NOTHING =====
# WHY IT EXISTS, and it is a workflow defect rather than a feature request: the absence waiver is bound
# to `base+head+job` — which is what makes it unable to outlive the review its authorizer judged — but
# the operator learns the JOB ID and the token accounting FROM the completed run, and re-running the
# wrapper to apply a freshly posted waiver ENQUEUES A NEW JOB, so the waiver was instantly STALE. As
# built, the mechanism was a dead letter: no sequence of actions got a legitimate absence past the gate.
# `--recheck-job <id>` closes the loop by re-deciding the verdict FOR THAT JOB without reviewing again.
# The binding is NOT loosened (dropping `job=` would reopen the hole where one persistent comment waives
# a later VACUOUS review at the same base+head); the loop is closed instead.
#
# NOTHING IS ASSUMED BECAUSE IT PASSED ONCE: every assert is re-run against the job record — the range
# (sha-assert), completion (from the record's own review text), the vacuity tiers, the token accounting
# and prompt-content. A recheck can therefore FAIL where the original run passed, and does not inherit
# anything from it.
RECHECK_JOB=""
LOG_ARG=""

# An option supplied with an EMPTY value is a usage error, never a silent fallback
# to the default: `--repo ""` falling back to $PWD is exactly how a caller ends up
# reviewing a repository it did not name.
need_value() { # need_value <option> <argc> <value>
  [ "$2" -ge 2 ] || die_usage "$1 requires a value"
  [ -n "$3" ] || die_usage "$1 was given an empty value (an empty value is never a default)"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --agent) need_value --agent $# "${2:-}"; AGENT="$2"; shift 2 ;;
    --model) need_value --model $# "${2:-}"; MODEL="$2"; shift 2 ;;
    --repo)  need_value --repo  $# "${2:-}"; REPO_ARG="$2"; shift 2 ;;
    --base)  need_value --base  $# "${2:-}"; BASE="$2"; shift 2 ;;
    --log)   need_value --log   $# "${2:-}"; LOG_ARG="$2"; shift 2 ;;
    --recheck-job)
      need_value --recheck-job $# "${2:-}"
      case "$2" in
        ''|*[!0-9]*) die_usage "--recheck-job takes a numeric roborev job id, got '$2'" ;;
      esac
      RECHECK_JOB="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) die_usage "unknown option '$1'" ;;
  esac
done

if [ -z "$AGENT" ] && [ -z "$MODEL" ]; then
  die_usage "missing required options --agent AND --model (both are required)"
fi
[ -n "$AGENT" ] || die_usage "missing required option --agent (--model alone leaves the reviewer agent inherited from .roborev.toml)"
[ -n "$MODEL" ] || die_usage "missing required option --model (--agent alone inherits review_model from .roborev.toml and hard-400s as a silent-looking review outage)"

# --- identity resolution (absolute repo, branch, full 40-char HEAD) ------------
if [ -n "$REPO_ARG" ]; then
  [ -d "$REPO_ARG" ] || die_usage "--repo path is not a directory: $REPO_ARG"
  REPO=$(git -C "$REPO_ARG" rev-parse --show-toplevel 2>/dev/null) \
    || die_usage "--repo is not inside a git repository: $REPO_ARG"
else
  REPO=$(git rev-parse --show-toplevel 2>/dev/null) \
    || die_usage "\$PWD is not inside a git repository and no --repo was given"
fi
REPO=$(cd "$REPO" && pwd -P)

# `rev-parse --abbrev-ref HEAD` / `rev-parse HEAD` both ECHO the literal string
# "HEAD" and exit non-zero in a repo with no commits, so the `|| printf` fallbacks
# concatenated onto real-looking values and the no-commit guard never fired.
# `symbolic-ref -q` and `rev-parse --verify --quiet` fail SILENTLY instead.
BRANCH=$(git -C "$REPO" symbolic-ref --short -q HEAD || printf 'HEAD')
HEAD_SHA=$(git -C "$REPO" rev-parse --verify --quiet HEAD || printf '')

if [ -n "$LOG_ARG" ]; then
  LOG="$LOG_ARG"
else
  log_slug=$(printf '%s' "$BRANCH" | tr -c 'A-Za-z0-9._-' '-')
  LOG="${TMPDIR:-/tmp}/roborev-review-${log_slug}-${HEAD_SHA:0:8}-$$.log"
fi
mkdir -p "$(dirname "$LOG")"
: >"$LOG"

# --- summary state -------------------------------------------------------------
REVIEWED_SHA="-"
JOB="-"
MODEL_LINE="-"
CENSUS="-"
TOKENS="UNAVAILABLE"
# Populated by roborev_census (in the sourced oracles file); declared here so the
# array always exists even if that oracle ever returns before filling it.
# shellcheck disable=SC2034 # read in roborev-review-oracles.sh, not here
census_files=0
# shellcheck disable=SC2034 # read in roborev-review-oracles.sh, not here
census_non_code_files=0
census_paths=()
census_code_paths=()
# The paths whose recorded mode `git ls-tree` could not measure at all (#3229). Declared here
# for the same reason as the arrays above: the fail-closed check reads `${#...[@]}`, and an
# array that does not exist would abort under `set -u` — or, worse, be treated as empty.
census_unmeasurable_paths=()
census_unmeasurable_detail=()
PUSH_ASSERT="SKIP"
CENSUS_CHECK="SKIP"
CODE_FREE="SKIP"
JOB_RECORD="SKIP"
SHA_ASSERT="SKIP"
# The POSITIVE "a review actually happened" assert. Absence of a vacuous phrase is
# NOT evidence a review occurred: a transcript that only says "Waiting for job N to
# complete...", or "Error: 400 the requested model is not supported", or "status:
# failed (provider timeout)" contains no vacuous phrase at all — and every one of
# them used to reach RESULT: PASS. Positive evidence (job status done AND a terminal
# verdict marker from an ALLOW-list) is now required before PASS is reachable.
REVIEW_COMPLETED="SKIP"
PROMPT_CONTENT="SKIP"
# The waiver record for the absence branch (owner ruling (4), #3312). Empty means the branch never
# ran — the census paths were present — so the key is ABSENT from the block rather than placeholdered.
WAIVER_REPORT=""
# The findings-deferral record (#3626). Empty means the findings branch never had a deferral to look
# for — a fresh review, or a findings state that is not an affirmatively measured `PRESENT (n)` — so
# the key is ABSENT from the block rather than placeholdered with a lookup that never happened.
DEFERRAL_REPORT=""
TIER1="SKIP"
TIER2="SKIP"
FINDINGS="SKIP"
# The reviewer process's OWN status, under its own greppable key: a caller retains
# only the block and reads it by grepping the per-check keys, so without this key a
# non-zero roborev exit shows up as every check reading PASS beside a RESULT: FAIL
# — the one failure cause a grep-based reader could not attribute. Values:
#   PASS            process exited 0
#   FINDINGS (exit N)  the review RAN and reported findings — a GENUINE review whose
#                   findings must be triaged, NOT a reviewer malfunction
#   ERROR (exit N)  the reviewer itself failed (status not done / no review body)
#   SKIP            a push/census/PATH failure exited before the process ran
ROBOREV_EXIT="SKIP"
RESULT="FAIL"
DETAILS=()
EMITTED=0

# ============ THE OUTPUT NEUTRALISATION BOUNDARY (#3229 round 5, blocker 2) ============
# NO PATH MAY REACH A SUMMARY VALUE UN-NEUTRALISED, and this is the ONE place that is
# enforced — at the single emit boundary, not per interpolation site.
#
# WHY. The block is LINE-ORIENTED and safety-critical: every reader (flow-closer, the
# flow-* skills, this repo's own guard suite) retains only the block and greps it by
# `^<key>: `, `^RESULT: ` deciding whether a merge proceeds. Diff-derived text reaches
# those values — `prompt-content:` names each census path absent from the prompt, and the
# FAIL DETAILS prose names paths — and a census path is
# ATTACKER-CONTROLLED: it is whatever a PR branch chose to track. A filename containing a
# NEWLINE therefore let a value SPAN LINES and introduce arbitrary `key:` lines, up to a
# FORGED `RESULT: PASS`, into the block whose whole purpose is to be trusted.
#
# WHY CENTRALLY, AND NOT AT EACH SITE. A per-site escape is a list to keep complete, and
# the next value that grows a path interpolation silently reopens the hole. Every value in
# the block goes through `emit_kv`, and every DETAILS line goes through `finish`, so the
# property is TOTAL and holds for keys that do not exist yet. The regression suite asserts
# it structurally against `emit_summary` itself, so a raw `printf 'key: %s\n'` FAILs.
#
# WHAT IT DOES. Control characters are replaced with VISIBLE ESCAPES (`\n`, `\r`, `\t`,
# else `\ooo` octal). Quotes, backslashes and spaces are deliberately LEFT ALONE: the block
# names swallowed paths by their REAL BYTES (a path with a literal `"` must still read as
# one — `docs/.../odd "q" name.sh`, pinned by case (cx6b)), and no non-control byte can
# start a new line or a new `key:`.
#
# THE DECLARED RESIDUAL, stated rather than implied: the rendering is NOT reversible — a
# path holding the two literal bytes `\` `n` renders identically to one holding a newline.
# That is display fidelity, not a safety property, and the guarantee this boundary makes is
# exactly "no value spans a line and no `key:` can be introduced". A caller needing the
# exact bytes reads them from git, not from a summary block.
#
# SECOND PROPERTY, ADDED AT THE SAME BOUNDARY FOR THE SAME REASON (roborev job 230): NO EMITTED
# VALUE CARRIES AN AUTHORIZATION KEYWORD. The block's standing invariant is that no emitted
# diagnostic carries any part of a marker form, not even its prefix (#3312 job 23), because
# summary blocks get pasted into PR comments as a matter of course. `judge_reason` in
# roborev-waiver-scan.py refuses a stem-bearing REASON, but a marker keyword can reach a
# diagnostic through several OTHER externally-sourced fields this process does not control — a
# GitHub login (`waiver:`/`deferral: UNAUTHORIZED (... its author '@<login>' ...)`), `gh issue
# view`'s stdout and stderr (`deferral: ... ($errtext)`), and any future value that grows an
# interpolation. Fixing them one at a time is the per-site list this boundary exists to abolish,
# so the denylist goes HERE, where every block value and every DETAILS line already passes.
#
# WHAT THE THREAT IS AND IS NOT, so nobody later mistakes this for a closed BYPASS or reopens it
# as one. NOT a bypass: a GitHub login admits letters, digits and hyphens and NOT colons or
# spaces, so a login can contain `roborev-defer` but can never contain a full stem
# (`roborev-defer: findings`); and an emitted line begins `deferral: UNAUTHORIZED (`, which is
# not a stem, so the scanner's `sole_marker_line` `startswith` test refuses it (verified with a
# positive control). It IS a spec-conformance and invariant-coverage defect: the invariant is
# stated absolutely, and a rule with an exception for "the layers below catch it anyway" decays
# the next time a layer moves. Deliberately NOT a security-grade escaping layer — a two-token
# denylist, and it must not grow into one.
#
# DISPLAY ONLY, WHICH IS THE SAFETY ARGUMENT: every authorization decision (allowlist, scope,
# count, retrievability) is made on the RAW value long before it reaches this renderer, and
# nothing downstream re-parses a rendered value as an authorization. So this can never move a
# verdict, and if this spelling ever diverges from the scanner's `MARKER_KEYWORD` the only
# possible effect is redacting differently — never granting. That is why the same rule at two
# emit boundaries is acceptable where two marker PARSERS would not be: a parser decides, a
# renderer does not.
#
# PURE BASH, NO SUBSHELL: explicit bracket classes give the case-insensitive match without
# `sed` (which would take a command substitution's exit status inside a `set -e` function) and
# without toggling `nocasematch`. The pattern is also written so this source line does not
# itself contain a literal marker keyword.
#
# The replacement token deliberately carries no part of either marker form, so redacting can
# never produce something a later paste could fill in. It is spelled IDENTICALLY to the
# scanner's `MARKER_KEYWORD_REDACTION`, so one redaction reads the same wherever it surfaces.
ROBOREV_MARKER_REDACTION='[authorization-keyword-redacted]'
roborev_safe_line() { # roborev_safe_line <text> -> sets _rx_safe
  local s="$1" out="" i n ch _redacted=""
  _rx_safe="$s"
  case "$s" in
    *[[:cntrl:]]*)
      n=${#s}
      for ((i = 0; i < n; i++)); do
        ch="${s:$i:1}"
        case "$ch" in
          $'\n') out+='\n' ;;
          $'\r') out+='\r' ;;
          $'\t') out+='\t' ;;
          [[:cntrl:]]) printf -v ch '\\%03o' "'$ch"; out+="$ch" ;;
          *) out+="$ch" ;;
        esac
      done
      _rx_safe="$out"
      ;;
  esac
  # AFTER the control-character rendering and on BOTH paths, because the guarantee is over the
  # text that is actually EMITTED — not over an intermediate form, and not only over the values
  # that happened to need escaping.
  #
  # A LONGER WORD IS A DIFFERENT WORD, which is the file's own rule for `roborev-defer:
  # findingsfoo` applied to the renderer. The keyword is redacted only where it is NOT continued
  # by another letter, and that boundary is LOAD-BEARING, not cosmetic: the scanner's own file
  # name, `roborev-waiver-scan.py`, is printed by the fail-closed `waiver: UNAVAILABLE (... tool:
  # <path>)` diagnostic (case wv31), and an operator has to read that path to fix the state. A
  # blanket substring redaction mangles it, and a diagnostic that cannot be acted on is the defect
  # this repository's cause strings exist to avoid. Rebuilt through BASH_REMATCH rather than
  # `${var//pat/repl}` because a pattern substitution cannot express "or end of line" and would
  # CONSUME the following character it must preserve.
  #
  # THE DECLARED RESIDUAL: a keyword embedded inside a longer word (`roborev-waiverfoo`) is left
  # alone, so such a value still shows the keyword as a substring. That is deliberate — it carries
  # no marker FORM (the form needs the keyword, then its kind, then the fields), and the test
  # helper `assert_no_marker_form` greps the bare keyword UNANCHORED, i.e. stricter than this
  # renderer, so any case that ever emits one is caught there rather than silently rendered.
  local _pre _m
  while [[ $_rx_safe =~ [rR][oO][bB][oO][rR][eE][vV]-([wW][aA][iI][vV][eE]|[dD][eE][fF][eE][rR])([^a-zA-Z]|$) ]]; do
    _m="${BASH_REMATCH[0]}"
    _pre="${_rx_safe%%"$_m"*}"
    _redacted="$_redacted$_pre$ROBOREV_MARKER_REDACTION${BASH_REMATCH[2]}"
    _rx_safe="${_rx_safe#*"$_m"}"
  done
  _rx_safe="$_redacted$_rx_safe"
}

emit_kv() { # emit_kv <key> <value> — the ONLY way a value enters the block
  roborev_safe_line "$2"
  printf '%s: %s\n' "$1" "$_rx_safe"
}

emit_summary() {
  printf '==== ROBOREV REVIEW SUMMARY ====\n'
  # MODE IS DECLARED THE WAY THE GATE DECLARES `MODE: lite` (#3312 job 24): a recheck's PASS is
  # legitimate — the review it re-decides was genuine and a human authorized the absence — but it must
  # never be pasteable as evidence of a FRESH review, so the block says which job it re-decided and that
  # nothing was enqueued. Emitted as the FIRST key INSIDE the block, so it travels with any paste of it.
  if [ -n "${RECHECK_JOB:-}" ]; then
    emit_kv 'MODE' "recheck (job $RECHECK_JOB re-decided from its job record; NO review was enqueued — not evidence of a fresh review)"
    emit_kv 'recheck-of' "$RECHECK_JOB"
  fi
  emit_kv 'repo' "$REPO"
  emit_kv 'branch' "$BRANCH"
  emit_kv 'base' "$BASE"
  emit_kv 'head-sha' "${HEAD_SHA:--}"
  emit_kv 'reviewed-sha' "$REVIEWED_SHA"
  # ===== WHICH BASE THE RANGE ASSERT COMPARED AGAINST (#3392) =====
  # INFORMATIONAL, exactly like `census:`/`tokens:`/`waiver:` — it is NOT in the verdict-grammar
  # scan and NOT in the affirmation loop (both enumerate the verdict-carrying keys by name), so it
  # can never make anything pass or fail on its own; `sha-assert:` alone carries that verdict.
  # It exists because the two candidate bases are INDISTINGUISHABLE in a pasted block otherwise:
  # a reader comparing `reviewed-sha`'s base endpoint against `base:` has no way to tell a
  # merge-base from the ref tip, which is exactly the confusion that let the tip-vs-merge-base
  # defect survive two misdiagnoses. Both shas are printed, always, so their (in)equality is
  # visible rather than inferred. Placed beside `head-sha:`/`reviewed-sha:`, the other endpoints.
  emit_kv 'assert-base' "${RANGE_BASE_SHA:--} (merge-base of $BASE and HEAD; $BASE tip ${BASE_TIP_SHA:--})"
  emit_kv 'job' "$JOB"
  emit_kv 'model' "$MODEL_LINE"
  emit_kv 'census' "$CENSUS"
  emit_kv 'tokens' "$TOKENS"
  emit_kv 'push-assert' "$PUSH_ASSERT"
  emit_kv 'census-check' "$CENSUS_CHECK"
  emit_kv 'code-free' "$CODE_FREE"
  emit_kv 'job-record' "$JOB_RECORD"
  emit_kv 'sha-assert' "$SHA_ASSERT"
  emit_kv 'review-completed' "$REVIEW_COMPLETED"
  emit_kv 'prompt-content' "$PROMPT_CONTENT"
  # THE WAIVER RECORD (owner ruling (4), #3312). INFORMATIONAL, exactly like `census:`/`tokens:` — it
  # is NOT in the verdict scan and cannot make anything pass on its own; `prompt-content:` alone
  # carries that verdict. Emitted ONLY when the absence branch ran and therefore had a waiver to look
  # for: in the PASS case it has no subject, and a `-` placeholder would imply a lookup that never
  # happened. It records the state even when no waiver exists, because "your marker names the wrong
  # sha" is the diagnostic a human needs, and a waived FAIL must never be silent about who waived it.
  if [ -n "${WAIVER_REPORT:-}" ]; then
    emit_kv 'waiver' "$WAIVER_REPORT"
  fi
  emit_kv 'vacuity-tier1' "$TIER1"
  emit_kv 'vacuity-tier2' "$TIER2"
  emit_kv 'findings' "$FINDINGS"
  # THE DEFERRAL RECORD (#3626). INFORMATIONAL, exactly like `waiver:`/`census:`/`tokens:` — it is NOT
  # in the verdict scan and cannot make anything pass on its own; `findings:` alone carries that
  # verdict, and its `DEFERRED` token is admitted only on the coupled granted state read in step 7.
  # Emitted ONLY when the findings branch had a deferral to look for, and then it states its own state
  # even when nothing was granted: "your marker names the wrong job", "the count moved" and "there is
  # no marker" are different operator actions, and a bare FAIL distinguishes none of them.
  if [ -n "${DEFERRAL_REPORT:-}" ]; then
    emit_kv 'deferral' "$DEFERRAL_REPORT"
  fi
  emit_kv 'roborev-exit' "$ROBOREV_EXIT"
  emit_kv 'log' "$LOG"
  emit_kv 'RESULT' "$RESULT"
}

finish() { # finish <PASS|FAIL|NOTHING-TO-REVIEW> <exit-code>
  RESULT="$1"
  # DETAILS go through the SAME neutralisation as the block's values: they are printed to
  # the same stdout a reader greps for `^RESULT: `, so a newline-bearing path in a DETAILS
  # line could forge a verdict just as well as one in a value.
  local _d
  for _d in ${DETAILS[@]+"${DETAILS[@]}"}; do
    roborev_safe_line "$_d"
    printf '%s\n' "$_rx_safe"
  done
  EMITTED=1
  emit_summary
  exit "$2"
}

# Emit a block on EVERY exit path, including an unexpected `set -e` abort: a run
# that dies without a verdict must never look like a run that was never made.
# shellcheck disable=SC2317 # invoked indirectly, by `trap on_exit EXIT` below
on_exit() {
  local rc=$?
  # C⁗ (#3312): there is no watcher, no background process and no temporary artefact to clean up here —
  # nothing is read, so nothing outlives the wrapper. The stop/cleanup hooks that used to live here are
  # deleted with the observer they served.
  if [ "$EMITTED" -eq 0 ]; then
    printf 'ERROR: the wrapper terminated unexpectedly (exit %s) before reaching a verdict.\n' "$rc"
    RESULT="FAIL"
    EMITTED=1
    emit_summary
    [ "$rc" -ne 0 ] || rc=1
    exit "$rc"
  fi
}
trap on_exit EXIT

if [ -z "$HEAD_SHA" ]; then
  DETAILS+=("ERROR: cannot resolve HEAD in $REPO — there is no commit to review.")
  finish FAIL 1
fi

# --- the local oracles (sourced) ----------------------------------------------
# Resolved from BASH_SOURCE, never $PWD, so the wrapper works from any directory.
# FAIL CLOSED if the file is missing or does not define both functions: an absent
# oracles file would silently turn the push assert and the census into no-ops, which
# is a worse failure than any this guard was built to catch.
if [ ! -f "$ORACLES_FILE" ]; then
  DETAILS+=("ERROR: the required oracles file '$ORACLES_FILE' is missing, so the push assert and the diff census cannot run. Failing closed rather than proceeding with those checks silently disabled — reinstall/restore scripts/flow/roborev-review-oracles.sh.")
  finish FAIL 1
fi
# shellcheck source-path=SCRIPTDIR
# shellcheck source=roborev-review-oracles.sh
. "$ORACLES_FILE"
if [ "$(type -t roborev_push_assert)" != function ] || [ "$(type -t roborev_census)" != function ]; then
  DETAILS+=("ERROR: '$ORACLES_FILE' did not define roborev_push_assert and roborev_census, so the push assert and the diff census cannot run. Failing closed — the file is truncated or corrupt.")
  finish FAIL 1
fi
roborev_push_assert
roborev_census
# IS THE TOOL EVEN INSTALLED — asked HERE, before any enqueue-side setup (#3229 round-10).
# It used to sit after that setup, which meant that on a box with no `roborev` at all the
# FIRST failure reported belonged to a downstream oracle that could not answer — a
# MISATTRIBUTED cause, sending the reader to investigate configuration when the actionable
# fact is that the binary is missing (the same error push-assert deliberately avoids when it
# refuses to call an auth failure "never pushed"). Deliberately AFTER the census and
# `code-free:`, which are pure git/classification facts whose causes are more actionable
# still, and BEFORE the checks-file validation and the enqueue, so a box with no binary
# costs no review round.
if ! command -v roborev >/dev/null 2>&1; then
  SHA_ASSERT="FAIL (roborev not on PATH)"
  DETAILS+=("ERROR: 'roborev' is not on PATH, so the review cannot be performed and the census cannot be certified. Failing closed rather than reporting a pass.")
  finish FAIL 1
fi

# --- the per-review checks (sourced) ------------------------------------------
# Same contract as the oracles file: resolved from BASH_SOURCE, and FAIL CLOSED if it is
# missing or incomplete — an absent checks file would silently turn all five checks into
# no-ops while every key still read PASS. Validated HERE, before any review is
# enqueued, so a broken installation costs no review; the functions are CALLED further
# down (steps 6a-6e), once the job facts exist.
if [ ! -f "$CHECKS_FILE" ]; then
  DETAILS+=("ERROR: the required checks file '$CHECKS_FILE' is missing, so review-completed, prompt-content, findings and both vacuity tiers cannot run. Failing closed rather than proceeding with those checks silently disabled — restore scripts/flow/roborev-review-checks.sh.")
  finish FAIL 1
fi
# shellcheck source-path=SCRIPTDIR
# shellcheck source=roborev-review-checks.sh
. "$CHECKS_FILE"
for roborev_required_check in roborev_check_review_completed roborev_check_prompt_content \
  roborev_check_findings roborev_check_tier1 roborev_check_tier2 roborev_check_findings_deferral; do
  if [ "$(type -t "$roborev_required_check")" != function ]; then
    DETAILS+=("ERROR: '$CHECKS_FILE' did not define $roborev_required_check, so that check cannot run. Failing closed — the file is truncated or corrupt.")
    finish FAIL 1
  fi
done

# --- step 4: invoke over the CENSUS RANGE + an EXPLICIT absolute repo (AC2) ----
# (`roborev` on PATH was asserted above, before any enqueue-side setup, so its absence is
# reported as the absent binary rather than as an oracle that would not answer.)

# THE SANCTIONED FORM — `--branch --base <base> --repo <abs>` — reviews the RANGE
# `<base>..HEAD`, i.e. exactly the census. Determined EMPIRICALLY against the real
# daemon (issue #2964, round 5); the three candidates measured on a 17-commit branch
# whose census was 27 files (22 markdown + 5 code):
#
#   FORM                                              enqueued git_ref            code files in prompt
#   --branch --base <base> --repo <abs>               <base40>..<head40>          5/5   <-- WINNER
#   --since <base> --repo <abs>                       <base40>..<head40>          5/5   (identical)
#   <base> <head> (two positionals)                   <EMPTY-TREE>..<head40>      3/5   BROKEN
#   <sha> (the previously sanctioned single-sha form)  <head40>                    3/5   PARTIAL
#
# The single-sha form reviews ONE COMMIT. On any multi-commit branch it certified the
# branch while the reviewer had seen only the last commit — a FOURTH vacuity class
# (a partial review reported as a full one) that the issue never anticipated. The
# issue's AC2 literally prescribes that form, so this fixes its INTENT (the reviewed
# content must match the requested range), not its letter.
#
# `--repo` is what makes `--branch` correct from a worktree: with it, roborev
# reported "17 commits since origin/main"; the original defect was `--branch`
# WITHOUT `--repo`, which resolves against the ROOT checkout. NEVER the
# two-positional range form (it anchors the range at git's EMPTY TREE).
# The transcript goes to the log; stdout stays reserved for the summary block.
#
# NOTHING IS ARMED AROUND THIS CALL, and that absence is the design (#3312, owner ruling (4)). When
# roborev delivers a large diff by writing it to a file, it DELETES that file when the review finishes
# — measured: it is gone before this very `--wait` returns — and `roborev show` cannot hand it back (no
# `--diff`). Every attempt to hold on to it, and then every attempt to classify the delivery from the
# prompt text instead, produced defects in the machinery rather than in the verdict; both are deleted.
# `prompt-content:` now asks one question of the prompt itself and an absence is a FAIL a human may
# waive. So there is no watcher, no capture directory, no classifier state and nothing to clean up
# here — only the review call.
# THE ENQUEUE IS THE ONE THING A RECHECK MUST NOT DO, so it is guarded here — the single place the
# reviewer is ever invoked — rather than by the caller remembering not to. Asserted structurally.
if [ -n "$RECHECK_JOB" ]; then
  REVIEW_RC=0
  RECHECK_ACTIVE=1
  : >"$LOG"
else
  # ===== THE ENQUEUE IS PINNED TO THE RESOLVED MERGE-BASE, NOT THE SYMBOLIC REF (#3392) =====
  # `--base` used to receive the SYMBOLIC `$BASE`, so roborev re-resolved the mirror ref ITSELF and
  # computed its own merge-base. If the ref moved between the census and this call, roborev reviewed a
  # DIFFERENT range than the census measured — and the only thing that noticed was `sha-assert`, AFTER a
  # full-price review had been spent. That is the residual second-order race the issue names, and
  # detecting it later is not the same as not having it.
  #
  # Passing the RESOLVED sha makes the range IMMUTABLE across all four consumers — census, enqueue,
  # assert, waiver scope — so the divergence is UNEXPRESSIBLE rather than caught. The property that makes
  # this exact rather than approximate: with the merge-base as the base, `base..HEAD` and `base...HEAD`
  # denote the SAME range (merge-base(merge-base, HEAD) is the merge-base), so pinning it cannot change
  # what is reviewed — there is no longer a two-dot/three-dot semantics gap for the two sides to disagree
  # across.
  #
  # VERIFIED AGAINST THE REAL BINARY (roborev v0.61.2), not assumed: a raw 40-hex sha is accepted by
  # `--base` and recorded as the range base. Measured on a throwaway repo whose main had advanced past
  # the branch point (merge-base d6b806a…, branch head 0b226fa…, main tip 399abca…):
  #   --base <merge-base sha>  -> "1 commits since d6b806a…", git_ref d6b806a…..0b226fa…, job_type range
  #   --base origin/main       -> "1 commits since origin/main", git_ref d6b806a…..0b226fa…  (IDENTICAL)
  # i.e. both forms record the same merge-base-anchored range — which is also the direct measurement
  # that roborev anchors at the MERGE-BASE and never at the ref tip.
  #
  # The block still reports `base: $BASE` — the operator asked for a symbolic ref and the block must not
  # misreport what was requested — while `assert-base:` carries the resolved sha this call used.
  #
  # A STRUCTURAL BACKSTOP, deliberately unreachable through the normal ordering (the census resolves
  # `RANGE_BASE_SHA` and `finish`es on failure, before this point): an EMPTY value here would be handed to
  # roborev as no `--base` at all, silently re-enabling its base AUTO-DETECTION and reviewing a range
  # nothing verified. The point of a backstop is not to depend on an upstream check still being there.
  if [ -z "${RANGE_BASE_SHA:-}" ]; then
    DETAILS+=("ERROR: the resolved range base is empty at the enqueue, so the reviewed range would be whatever roborev auto-detected rather than the census range. This is a defect in the wrapper's own ordering (the census resolves it and fails closed before this point), not something to fix in the branch under review. Failing closed; no review was enqueued.")
    finish FAIL 1
  fi
  set +e
  roborev review --branch \
    --base "$RANGE_BASE_SHA" \
    --repo "$REPO" \
    --agent "$AGENT" \
    --model "$MODEL" \
    --wait >"$LOG" 2>&1
  REVIEW_RC=$?
  set -e
fi

# --- step 5: reviewed-RANGE assert (AC2) — STRUCTURED data is the oracle -------
#
# The job record's `git_ref` field is a FULL 40-char sha recorded by roborev itself,
# so it is compared full-sha to full-sha. The stdout `Enqueued job <N> for <sha>`
# line is a CROSS-CHECK ONLY — parsing a tool's prose is the weaker source whenever a
# structured one exists — but its ABSENCE is still a hard failure, because it carries
# the job id every structured query needs.
#
# The announcement is parsed DEFENSIVELY: lower-cased first (so an upper-case
# announcement cannot survive the match and then fall out of the field extraction as
# garbage that gets handed to `roborev show`), the sha floor is 7 hex chars (4 was
# loose enough that a 4-char prefix satisfied the assert), and both fields are
# validated before use. When several announcements are present the LAST one is the
# effective enqueue, and the multiplicity is recorded.
# RECHECK: THE JOB ID COMES FROM THE FLAG, EXPLICITLY (#3312 job 24). Explicit rather than "resolve the
# latest completed job for this base+head", deliberately: the waiver names ONE job because the authorizer
# judged ONE review, and a resolver would let a re-run silently become the subject of a waiver written for
# a different review — the very hole the job binding closes. The enqueue announcement is the ENQUEUE's own
# cross-check, so it is skipped here (there was no enqueue) while every RECORD-derived assert still runs:
# `sha-assert` below still compares the record's git_ref against THIS base and head.
if [ -n "$RECHECK_JOB" ]; then
  JOB="$RECHECK_JOB"
  ANNOUNCE_COUNT=0
  ANNOUNCE="recheck"
  ANNOUNCED_SHA=""
  announce_ok=1
else
ANNOUNCE_COUNT=$({ grep -ociE 'enqueued job [0-9]+ for [0-9a-f]{7,40}' "$LOG" 2>/dev/null || printf 0; } | tail -1)
# shellcheck disable=SC2018,SC2019 # ASCII-only on purpose: this normalises a HEX sha,
# and the POSIX classes would make the transform locale-dependent for no benefit.
ANNOUNCE=$({ tr 'A-Z' 'a-z' <"$LOG" 2>/dev/null || printf ''; } | grep -oE 'enqueued job [0-9]+ for [0-9a-f]{7,40}' | tail -1 || printf '')
ANNOUNCED_SHA=""
announce_ok=0
if [ -z "$ANNOUNCE" ]; then
  SHA_ASSERT="FAIL (no parseable enqueue announcement)"
  DETAILS+=("ERROR: sha-assert: the transcript contains no parseable 'Enqueued job <N> for <sha>' line (with a sha of at least 7 hex chars), so neither the job record nor the reviewed sha can be located and the review is UNVERIFIABLE. That is a failure, never a skipped check. Transcript: $LOG")
else
  JOB=$(printf '%s' "$ANNOUNCE" | sed -E 's/^enqueued job ([0-9]+).*/\1/')
  ANNOUNCED_SHA=$(printf '%s' "$ANNOUNCE" | sed -E 's/.* for ([0-9a-f]+)$/\1/')
  case "$JOB" in
    ''|*[!0-9]*)
      SHA_ASSERT="FAIL (unparseable enqueue announcement)"
      DETAILS+=("ERROR: sha-assert: the enqueue announcement '$ANNOUNCE' did not yield a numeric job id, so nothing can be queried about the job. Failing closed rather than passing a malformed id to roborev.")
      JOB="-"
      ;;
    *) announce_ok=1 ;;
  esac
  if [ "$announce_ok" -eq 1 ]; then
    case "$ANNOUNCED_SHA" in
      *[!0-9a-f]*|'')
        SHA_ASSERT="FAIL (unparseable enqueue announcement)"
        DETAILS+=("ERROR: sha-assert: the enqueue announcement '$ANNOUNCE' did not yield a hex sha. Failing closed.")
        announce_ok=0
        ;;
    esac
  fi
  # `${ANNOUNCE_COUNT:-0}` is DIAGNOSTIC-ONLY and gates nothing but a NOTICE about
  # multiplicity, so its permissive default cannot weaken a verdict (#3229 round-10 sweep):
  # the announcement's own PRESENCE is asserted above and FAILs closed, and the reviewed range
  # is verified from the structured job record, not from this count.
  if [ "${ANNOUNCE_COUNT:-0}" -gt 1 ]; then
    DETAILS+=("NOTICE: sha-assert: the transcript carries $ANNOUNCE_COUNT enqueue announcements; the LAST one (job $JOB) is the effective enqueue and is the one asserted.")
  fi
fi
fi

# --- structured job facts (extracted by scripts/flow/roborev-job-facts.py) -----
# Diagnostics live beside the transcript; `log:` names the base path.
# RECORD_OUTPUT_FILE holds the review text the JOB RECORD carries. In recheck mode it BECOMES the
# transcript, so `review-completed`, the vacuity tiers and `findings` are re-asserted from the record
# rather than inherited from a run that is not happening (#3312 job 24).
FACTS_FILE="$LOG.facts"
RECORD_OUTPUT_FILE="$LOG.record-output"
PROMPT_FILE="$LOG.prompt"
: >"$FACTS_FILE"
: >"$PROMPT_FILE"

extract_job_facts() { # extract_job_facts <job> <json> <facts-out> <prompt-out> [<review-out>]
  command -v python3 >/dev/null 2>&1 || return 1
  [ -f "$JOB_FACTS_TOOL" ] || return 1
  [ -n "$2" ] || return 1
  printf '%s' "$2" | python3 "$JOB_FACTS_TOOL" "$1" "$3" "$4" ${5:+"$5"} 2>/dev/null
}

fact() { sed -n "s/^$1=//p" "$FACTS_FILE" | head -1; }

# TWO PAYLOAD SHAPES, NOT AN ASYNC WRITE (issue #2964; the round-5 "the job record is
# written asynchronously" diagnosis was WRONG and is retracted — there is no durability
# problem and no write race). The fields were always present, one level down:
#   * `roborev list --json` returns JOB rows with git_ref / status / model /
#     requested_model / token_usage / verdict at TOP level.
#   * `roborev show <id> --json` returns a REVIEW row — agent, closed, created_at, id,
#     job, job_id, output, prompt, uuid, verdict_bool — that NESTS the job row under a
#     `job` key. Its `job_id` (and the nested `job.id`) equal the job asked for, so a
#     first-match lookup returned the OUTER row, which carries none of those fields;
#     that looked like an empty record and silently weakened FOUR asserts at once on a
#     NORMAL run (sha-assert fell back to prose, review-completed to the transcript
#     alone, tier 2 to UNAVAILABLE, model to UNCONFIRMED).
#     ITS TOP-LEVEL `id` IS THE REVIEW ROW'S OWN SEQUENCE AND NEED NOT EQUAL THE JOB
#     (#3654). Measured over records 1-10 on v0.61.2: six agree, and two PAIRS swap —
#     asking for 8 returns `id=9`, asking for 9 returns `id=8`, likewise 6/7 — while
#     `job_id` and `job.id` name the requested job in every one of the ten. This
#     comment used to assert the top-level `id` equalled the job, which
#     contradicted the help text one file over; doctrine decays exactly like a comment.
# roborev-job-facts.py now prefers an id match that carries `git_ref`, so the record is
# complete in ONE read. The loop below is therefore a short SANITY RETRY (it covers a
# transient read failure and a not-yet-terminal status), never a wait on a write race.
record_required_present() {
  local status
  [ -n "$(fact git_ref)" ] || return 1
  status=$(fact status)
  case "$status" in "done"|"failed") return 0 ;; *) return 1 ;; esac
}

# Token accounting is DESIRABLE, not required — a build may legitimately report none,
# and waiting out the whole bound for it would cost the bound on every such run. It
# gets ONE grace poll after the required fields land.
record_complete() {
  record_required_present || return 1
  [ "$(fact token_state)" = "parsed" ] || return 1
  return 0
}

# TWO SOURCES, DIFFERENT SHAPES — try both and keep the one that actually answers
# (MEASURED, issue #2964 round 5). `roborev show <job> --json` returns the REVIEW row:
# a parseable object carrying id/agent/prompt but NO git_ref, NO status, NO verdict and
# NO token_usage. `roborev list --json` returns the JOB row, which has all of them.
# Accepting the first payload that merely PARSED meant the richer source was never
# consulted, and the record looked permanently incomplete — which read as an async lag
# and silently downgraded sha-assert, tier 2 and model on every real run. So a source
# only counts when it yields the fields the asserts require.
read_job_record() { # read_job_record <job> -> populates FACTS_FILE / PROMPT_FILE
  local payload best_facts="$FACTS_FILE.candidate"
  : >"$best_facts"
  for payload in show list; do
    local json=""
    case "$payload" in
      show) json=$(roborev show "$1" --json 2>/dev/null || printf '') ;;
      # ===== THIS READ IS BRANCH-SCOPED BY DEFAULT, AND DELIBERATELY UNCHANGED (#3654) =====
      # `roborev list` filters by branch, and its DEFAULT follows the CURRENT HEAD OF THE `--repo`
      # PATH — not the branch the invoking shell is standing in (measured: from a cwd that is not a
      # git repository at all, `--repo <lane>` returns that lane's branch's rows, and the same
      # `--repo` run from another lane returns those same rows). It is correct here BY CONSTRUCTION
      # for the ordinary case — the wrapper enqueued the review for the `--repo` checkout's own
      # branch — and it must NOT grow a `--branch` here: `sha-assert` depends on this read, and
      # changing what it selects is a separate question from #3654.
      list) json=$(roborev list --json --limit 50 --repo "$REPO" 2>/dev/null || printf '') ;;
    esac
    extract_job_facts "$1" "$json" "$FACTS_FILE" "$PROMPT_FILE" "$RECORD_OUTPUT_FILE" || continue
    if record_required_present; then
      rm -f "$best_facts"
      return 0
    fi
    # Keep the poorer payload only as a last resort, so nothing is LOST if no source is
    # complete, but never let it shadow a source that has the required fields.
    cp "$FACTS_FILE" "$best_facts"
  done
  if [ -s "$best_facts" ]; then
    cp "$best_facts" "$FACTS_FILE"
  fi
  rm -f "$best_facts"
  return 1
}

# REQUIRED to stop polling: the fields without which an assert cannot run at all.
if [ "$announce_ok" -eq 1 ]; then
  record_polls=0
  token_grace_used=0
  while : ; do
    read_job_record "$JOB" || true
    if record_complete; then break; fi
    # Required fields present but tokens absent: spend ONE grace poll, then accept.
    if record_required_present; then
      if [ "$token_grace_used" -eq 1 ]; then break; fi
      token_grace_used=1
    fi
    if [ "$record_polls" -ge "$JOB_RECORD_POLL_ATTEMPTS" ]; then break; fi
    record_polls=$((record_polls + 1))
    sleep "$JOB_RECORD_POLL_INTERVAL_SECS"
  done
  if record_complete; then
    JOB_RECORD="PASS"
    if [ "$record_polls" -gt 0 ]; then
      DETAILS+=("NOTICE: job-record: the record read complete only on retry $record_polls of $JOB_RECORD_POLL_ATTEMPTS. This is a transient read, NOT an asynchronous write — the job row is present from enqueue; 'roborev list --json' carries its fields at top level and 'roborev show <id> --json' nests them under a 'job' key.")
    fi
  elif record_required_present; then
    JOB_RECORD="PASS (no token accounting in the record)"
  else
    missing=""
    [ -n "$(fact git_ref)" ] || missing="$missing git_ref"
    [ -n "$(fact status)" ] || missing="$missing status"
    [ "$(fact token_state)" = "parsed" ] || missing="$missing token_usage"
    JOB_RECORD="DEGRADED (incomplete after ${JOB_RECORD_POLL_ATTEMPTS} retries:${missing:- none})"
    DETAILS+=("NOTICE: job-record: DEGRADED — after ${JOB_RECORD_POLL_ATTEMPTS} retries at ${JOB_RECORD_POLL_INTERVAL_SECS}s the job record for '$JOB' is still missing:${missing:- nothing}. The dependent asserts below report their own verdicts; nothing here is silently weakened.")
  fi
  # The prompt may not be carried in the JSON payload; ask for it directly.
  if [ ! -s "$PROMPT_FILE" ]; then
    roborev show "$JOB" --prompt >"$PROMPT_FILE" 2>/dev/null || : >"$PROMPT_FILE"
  fi
fi

# ===== RECHECK: THE RECORD'S OWN REVIEW TEXT IS THE TRANSCRIPT (#3312 job 24) =====
# A recheck has no transcript of its own, and it must not be allowed to inherit the original run's
# verdicts either. So the record's review text is copied into `$LOG` and every text-based check runs
# against it unchanged. If the record carries no review text the file stays EMPTY, which
# `review-completed` reads as "no terminal verdict marker" — a FAIL. That is the intended direction: a
# job whose completion cannot be re-established is not recheckable.
if [ -n "$RECHECK_JOB" ]; then
  if [ -s "$RECORD_OUTPUT_FILE" ]; then
    cat "$RECORD_OUTPUT_FILE" >"$LOG"
  else
    : >"$LOG"
    DETAILS+=("NOTICE: recheck: the job record for '$JOB' carries no review text, so the transcript-based checks below have nothing to re-assert against and will fail closed. A recheck never inherits the original run's verdicts.")
  fi
fi

JOB_GIT_REF=$(fact git_ref | tr 'A-F' 'a-f')
JOB_STATUS=$(fact status)
JOB_MODEL=$(fact model)
JOB_REQUESTED_MODEL=$(fact requested_model)
JOB_HAS_TOKEN_DATA=$(fact has_token_data)
JOB_VERDICT=$(fact verdict)
TOKEN_STATE=$(fact token_state)
TOK_IN=$(fact input_tokens)
TOK_CACHED=$(fact cached_input_tokens)
TOK_OUT=$(fact output_tokens)

# The sanctioned form reviews a RANGE, so the job record's `git_ref` is
# `<base40>..<head40>` and BOTH endpoints are asserted — strictly stronger than the
# old single-sha equality. The stdout announcement for a range review names only the
# range BASE ("Enqueued job N for <base>"), so it can no longer verify that HEAD was
# reviewed at all: when the record is unavailable this assert FAILS rather than
# falling back to a check that verifies nothing. (A single-sha `git_ref` is still
# accepted, for a roborev build that reports one.)
#
# ===== THE EXPECTED RANGE BASE IS `RANGE_BASE_SHA`, THE MERGE-BASE (#3392) =====
# `<base>...HEAD` is `merge-base(<base>, HEAD)..HEAD`, which is the range the census
# measures and the range roborev reviews. Comparing the record's base endpoint against
# the base REF'S TIP instead — as this did — made a CORRECT review FAIL deterministically
# for every branch whose base had advanced since the branch point, i.e. for almost every
# branch that had not just been rebased. It was misdiagnosed as a race twice; the
# controlled measurement (base ref recorded before AND after the review, unmoved, assert
# failed anyway) is what killed that hypothesis. So the comparison is now like-for-like.
#
# `BASE_TIP_SHA` DOES NOT DISAPPEAR, and that is AC2: the single-sha branch below needs
# the TIP, because the T1 trap it detects is a `--branch` review resolved against the
# ROOT checkout, which enqueues the base ref's TIP. Each variable is used where its own
# meaning is the subject, and neither is a stand-in for the other.
if [ "$announce_ok" -eq 1 ]; then
  case "$JOB_GIT_REF" in
    *..*)
      range_base="${JOB_GIT_REF%%..*}"
      range_head="${JOB_GIT_REF##*..}"
      REVIEWED_SHA="$JOB_GIT_REF"
      if [ "$range_head" = "$HEAD_SHA" ] && [ "$range_base" = "$RANGE_BASE_SHA" ]; then
        SHA_ASSERT="PASS"
      else
        SHA_ASSERT="FAIL (reviewed range does not match ${BASE}...HEAD)"
        DETAILS+=("ERROR: sha-assert: the reviewed range '$JOB_GIT_REF' does not match the census range: expected base '$RANGE_BASE_SHA' (the merge-base of $BASE and HEAD, which is what '${BASE}...HEAD' diffs from) and head '$HEAD_SHA' (job $JOB).")
        if [ "$range_base" != "$RANGE_BASE_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the range BASE is '$range_base', not '$RANGE_BASE_SHA'. An empty-tree base (4b825dc6...) is the signature of the non-sanctioned two-positional commit-range form. A base equal to the TIP of '$BASE' ($BASE_TIP_SHA) instead of its merge-base with HEAD would mean the review scope was anchored at the ref tip, which is NOT the branch diff — the reviewed range must be merge-base..HEAD.")
        fi
        if [ "$range_head" != "$HEAD_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the range HEAD is '$range_head', not branch HEAD '$HEAD_SHA' — the reviewed scope stops short of the branch tip.")
        fi
      fi
      ;;
    ?*)
      REVIEWED_SHA="$JOB_GIT_REF"
      if [ "$JOB_GIT_REF" = "$HEAD_SHA" ]; then
        # FAIL CLOSED on a single-commit record even when it equals HEAD (codex, round
        # 5): a single-commit review covers ONE commit and prompt-content matches
        # PATHS, so when several commits touch the SAME file a review of only the last
        # one passes every path check while the earlier changes go unreviewed. The
        # sanctioned invocation always records a range; a single sha means something
        # else ran.
        SHA_ASSERT="FAIL (single-commit record, not the census range)"
        DETAILS+=("ERROR: sha-assert: the job record reports a SINGLE commit ('$JOB_GIT_REF'), not the census range. It equals branch HEAD, but a single-commit review covers only that commit: when several commits touch the same file, path-based checks cannot tell the difference and the earlier changes go unreviewed. The sanctioned invocation records a '<base>..<head>' range — something else produced this job.")
      else
        SHA_ASSERT="FAIL (reviewed-sha does not match head-sha)"
        DETAILS+=("ERROR: sha-assert: the job record's git_ref '$JOB_GIT_REF' does not equal branch HEAD '$HEAD_SHA' (job $JOB).")
        # ===== T1, AND WHY THE TIP IS STILL READ HERE (AC2, #3392) =====
        # A `--branch` review resolved against the ROOT checkout enqueues the base ref's
        # TIP, so THE TIP is the sha this signature is about — it is not interchangeable
        # with the merge-base. But the merge-base is the base of the range that SHOULD have
        # been reviewed, so an equality with EITHER is the same "no branch change was
        # reviewed" signature and both are reported, naming WHICH one matched. On a freshly
        # rebased branch they are the same commit and the message says so; when they differ,
        # which one matched is the diagnostic (tip => the root-checkout resolution;
        # merge-base => a review anchored at the branch point, still reviewing nothing).
        # Either way this FAILs — nothing here becomes permissive.
        if [ "$JOB_GIT_REF" = "$BASE_TIP_SHA" ] && [ "$JOB_GIT_REF" = "$RANGE_BASE_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the reviewed sha EQUALS the base ref '$BASE' — which here is BOTH its tip and its merge-base with HEAD ($BASE_TIP_SHA; this branch is not behind '$BASE') — so NO branch change was reviewed. That equality is the signature of a '--branch' review resolved against the ROOT checkout instead of this worktree.")
        elif [ "$JOB_GIT_REF" = "$BASE_TIP_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the reviewed sha EQUALS the TIP of the base ref '$BASE' ($BASE_TIP_SHA) — NO branch change was reviewed. That equality is the signature of a '--branch' review resolved against the ROOT checkout instead of this worktree. (The base of the range that SHOULD have been reviewed is the merge-base, $RANGE_BASE_SHA.)")
        elif [ "$JOB_GIT_REF" = "$RANGE_BASE_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the reviewed sha EQUALS the MERGE-BASE of '$BASE' and HEAD ($RANGE_BASE_SHA) — the branch point itself, so NO branch change was reviewed: every commit under review is a DESCENDANT of it. (The tip of '$BASE' is $BASE_TIP_SHA.)")
        else
          DETAILS+=("ERROR: sha-assert: the reviewed sha matches NEITHER endpoint (head '$HEAD_SHA', range base '$RANGE_BASE_SHA' — the merge-base of '$BASE' and HEAD, whose tip is $BASE_TIP_SHA).")
        fi
      fi
      ;;
    *)
      SHA_ASSERT="FAIL (job record unavailable — reviewed range unverifiable)"
      REVIEWED_SHA="-"
      DETAILS+=("ERROR: sha-assert: the job record carries no 'git_ref' after polling (job-record: $JOB_RECORD), so the reviewed RANGE cannot be verified. The stdout announcement names only the range BASE ('$ANNOUNCED_SHA') for a range review, so prose cannot establish that branch HEAD was reviewed — this fails closed rather than accepting a check that verifies nothing. The job row is present from enqueue — 'roborev list --json' carries git_ref at top level and 'roborev show <job> --json' nests it under a 'job' key — so an absent git_ref means the record could not be READ, not that it was not yet written; re-run, and if it persists check the daemon ('roborev status').")
      ;;
  esac
fi

# --- model-substitution check (review integrity) ------------------------------
# A NOTICE, not a FAIL, deliberately: roborev legitimately canonicalises/resolves a
# model alias, so a mismatch is not by itself evidence of a bad review — and an
# always-red guard is the failure mode that gets guards bypassed. Review integrity is
# carried by the deterministic checks; this line exists so a substitution can never
# happen SILENTLY.
if [ -n "$JOB_MODEL" ]; then
  if [ -n "$JOB_REQUESTED_MODEL" ] && [ "$JOB_REQUESTED_MODEL" != "$JOB_MODEL" ]; then
    MODEL_LINE="$JOB_MODEL (SUBSTITUTED — requested '$JOB_REQUESTED_MODEL')"
    DETAILS+=("NOTICE: model: the job ran '$JOB_MODEL' but '$JOB_REQUESTED_MODEL' was requested — a MODEL SUBSTITUTION. Recorded as a loud NOTICE rather than a FAIL (an alias resolution is legitimate), but confirm the substituted model is one you accept for a merge-gating review.")
  else
    MODEL_LINE="$JOB_MODEL"
  fi
else
  MODEL_LINE="$MODEL (UNCONFIRMED — no model field in the job record)"
fi

# --- steps 6a-6e: the per-review checks (defined in the sourced checks file) ---
roborev_check_review_completed
roborev_check_prompt_content
roborev_check_findings
roborev_check_tier1
roborev_check_tier2
# AFTER BOTH TIERS, DELIBERATELY (#3626): `vacuity-tier1` GATES on `findings:` reading `PRESENT*`, so
# rewriting that value before it runs would move a correct advisory NOTICE to a HARD FAIL. A deferral
# changes what the VERDICT does with an established findings state; it must not change what any other
# check saw.
roborev_check_findings_deferral

# --- step 7: the verdict ------------------------------------------------------
# The findings COUNT is best-effort (it counts severity markers in the transcript);
# the PRESENT/NONE/UNKNOWN distinction is the load-bearing part — tier 1 is gated on it.
#
# Every per-check key participates in ONE scan. A key fails the run when its value
# starts with FAIL, FINDINGS, ERROR or INCONSISTENT; PASS / SKIP / UNAVAILABLE /
# NOTICE / DEGRADED never do (NOTICE is tier 1's non-failing value; DEGRADED reports
# an incomplete job record, whose consequences are carried by the dependent asserts).
#
# ====== THE GRAMMAR IS CLOSED: NO PASS WITHOUT AN AFFIRMATIVE VALUE (#3229 round-10) ======
# THE GENERAL DEFECT this closes, of which several instances were found on this issue (each
# in a subsystem since deleted — #3283/#3278 — but the defect was NOT theirs: it was HERE,
# in this scan, and it predates every one of them): **a multi-state signal where only the BAD
# states are tested, so every unknown or unmeasured state inherits the PERMISSIVE branch.**
# A point fix per instance is the wrong response; the shape has to be closed where it is
# structural. This closure is therefore RETAINED after the exclusion oracle that surfaced it
# was removed: it is a property of the wrapper's terminal verdict for EVERY key, and leaving
# the verdict permissive again would leave the wrapper worse than we found it.
#
# This scan WAS that shape, at the wrapper's single most consequential decision point: it
# tested four failing prefixes and let EVERYTHING ELSE fall through to `finish PASS 0`. So a
# key holding a value nobody planned — an EMPTY string because a check aborted before
# assigning it, a state name a future check introduces, a typo in an assignment, a value from
# a checks file that returned early — reached PASS. The absence of a bad word is not evidence
# of a good outcome, which is the same epistemic error every other fix on this issue removes.
#
# So the grammar is CLOSED and keyed POSITIVELY: a value must MATCH a recognised non-failing
# form to be non-failing. The recognised set is exactly the states this block documents —
# PASS / SKIP / NOTICE / UNAVAILABLE / DEGRADED, plus `findings:`'s own NONE / PRESENT /
# UNKNOWN — and anything else is an UNRECOGNISED VERDICT, which fails closed and names
# itself. UNKNOWN is recognised deliberately: `findings: UNKNOWN` is a documented value, not a
# typo — and recognition is all this scan grants it. It no longer RELIES on being "unreachable
# unless `roborev-exit:` is already `ERROR (exit N)`, which fails on its own terms": that was a
# statement about a NEIGHBOURING key, and #3564 is what happens when a key's failure is delegated
# to its neighbour — on `--recheck-job` the neighbour is legitimately `SKIP` and the delegation
# evaporates. `findings:` now carries its own affirmative gate below, so `UNKNOWN` fails on ITS OWN
# terms; `vacuity-tier1:` additionally treats it as claiming cleanliness.
#
# ====== MATCHED ON THE VERDICT TOKEN, EXACTLY — NEVER AS A PREFIX GLOB ======
# The scan's earlier form matched `PASS*` / `FAIL*` etc. as PREFIX globs, and a prefix glob
# reopens, in the closure itself, exactly the hole the closure exists to shut: `PASS` is a
# prefix of `PASSthisNeverRan` and of `PASS-MEASUREMENT-DID-NOT-HAPPEN`, so a value that
# merely BEGINS with a recognised token — a typo, a concatenation, a state a future check
# names `PASS-PENDING` — was accepted as that token and rode to `RESULT: PASS`. The closure
# would then be checking a spelling, not a state.
#
# So the value is reduced to its VERDICT TOKEN — everything before the first space — and that
# token is compared EXACTLY. Every documented value in this block is either the bare token
# (`PASS`, `SKIP`, `UNAVAILABLE`) or `TOKEN (detail…)` (`FAIL (empty census)`,
# `PASS (2/2 code census paths present)`, `DEGRADED (incomplete after 3 retries: …)`), so the
# token is well defined for every one of them, and a value with anything ELSE glued to the
# token is UNRECOGNISED and fails closed. That is strictly stronger than the prefix form in
# both arms: a `FAILED (…)` variant no longer matches the failing arm by prefix either — it
# lands in `*)`, which also fails, so nothing becomes permissive by tightening.
#
# The failing-token `case` is kept as a SEPARATE statement: it is the one the regression suite
# extracts and asserts against (NOTICE must stay outside it), and the positive arm is an
# ADDITION rather than a rewrite of a scan whose set is pinned.
failed=0
unrecognised=""
ungranted_deferral=""
misplaced_deferral=""
# ====== ONE COUPLED STATE FOR THE DEFERRAL, READ BY BOTH GATES THAT NEED IT (#3626) ======
# `DEFERRED` is a NEW value of the closed grammar below, and it is non-failing ONLY when the deferral
# oracle affirmatively GRANTED. The grammar scan and the `findings:` gate must therefore read ONE
# state, not two: two independent tests of "was it granted?" are two things that can drift apart, and
# the drift would be an authorization bypass rather than an inconsistency. So the whole admission is
# decided HERE, once, and each gate asks this variable. (The deterministic-key affirmation backstop
# reads it NOT AT ALL — see the comment at its `case`: the coupling was right, its SCOPE was not.)
#
# EVERY TERM IS AFFIRMATIVE, and every one of them is required (#3586). It is not enough that the
# oracle said `granted`: the provenance must be COMPLETE (an authorizer, a reason, at least one filed
# issue), the SCOPE must equal this run's own base/head/job — the same equality the absence waiver's
# admission asserts — the declared count must equal the count this run OBSERVED, and the mode must be
# `--recheck-job`, the only path an authorization can travel. A `DEFERRED` produced by some future code
# path that measured nothing therefore cannot ride to a PASS.
#
# AND THE ADMISSION IS CONFINED TO ONE KEY, `findings:`, BY CONSTRUCTION (roborev job 225). An earlier
# version of this block admitted `DEFERRED` from this state FOR ANY KEY, on the reasoning that the
# PROVENANCE is what matters and that a key-scoped test has to be re-argued whenever a key is added.
# That reasoning is wrong HERE, and the difference is worth stating because it is the opposite of the
# absence waiver's: a waiver authorizes a PROPERTY (an absence) that only one key can ever report,
# while a deferral authorizes a NAMED SET OF FINDINGS — nothing a lead writes in a deferral marker
# says anything about whether the reviewer's diff arrived, whether the push landed or whether the
# reviewed range matched. So an unconfined admission let ONE authorization excuse a check NOBODY
# authorized, and the only thing standing between it and a false PASS was that no other key HAPPENS
# to emit the token. That is #3564's lesson exactly — delegating a key's failure to its neighbour is
# a latent false pass — so the question asked here is "what fails the run if THIS key alone goes
# bad", and the answer must not be "nothing, because today nothing else says DEFERRED".
# The confinement is therefore structural: the scan below carries the KEY NAME beside each value and
# admits `DEFERRED` only for `findings`, and the deterministic-key affirmation loop carries no
# `DEFERRED` arm at all.
deferral_admits=0
if [ "${ROBOREV_DEFERRAL_STATE:-}" = "granted" ] \
  && [ -n "${RECHECK_JOB:-}" ] \
  && [ -n "${ROBOREV_DEFERRAL_AUTHOR:-}" ] \
  && [ -n "${ROBOREV_DEFERRAL_REASON:-}" ] \
  && [ -n "${ROBOREV_DEFERRAL_ISSUES:-}" ] \
  && [ -n "${ROBOREV_DEFERRAL_OBSERVED_COUNT:-}" ] \
  && [ "${ROBOREV_DEFERRAL_COUNT:-}" = "${ROBOREV_DEFERRAL_OBSERVED_COUNT:-}" ] \
  && [ "${ROBOREV_DEFERRAL_SCOPE:-}" = "base=${RANGE_BASE_SHA:-} head=${HEAD_SHA:-} job=${JOB:-}" ]; then
  deferral_admits=1
fi
# EACH ENTRY CARRIES ITS KEY NAME, `<key>=<value>`, because one arm of the grammar below is
# key-scoped (`DEFERRED`, admitted for `findings` alone) and a scan over bare values cannot express
# that. The split is the same one the affirmation loop uses: the key name is everything before the
# first `=` (no key contains one), the value is the rest, so a value containing `=` is unaffected.
for scan_keyed in "push-assert=$PUSH_ASSERT" "census-check=$CENSUS_CHECK" "code-free=$CODE_FREE" \
  "sha-assert=$SHA_ASSERT" "review-completed=$REVIEW_COMPLETED" "prompt-content=$PROMPT_CONTENT" \
  "vacuity-tier1=$TIER1" "vacuity-tier2=$TIER2" "findings=$FINDINGS" "roborev-exit=$ROBOREV_EXIT"; do
  scan_key="${scan_keyed%%=*}"
  verdict="${scan_keyed#*=}"
  # The VERDICT TOKEN: the value up to its first space. An empty or all-detail value yields a
  # token that matches nothing, which is the intended fail-closed outcome.
  verdict_token="${verdict%% *}"
  case "$verdict_token" in FAIL|FINDINGS|ERROR|INCONSISTENT) failed=1 ;; esac
  # The POSITIVE arm. An `*)` that FAILS is what makes the grammar closed — the whole point
  # is that an unplanned value must not inherit the non-failing branch.
  case "$verdict_token" in
    FAIL|FINDINGS|ERROR|INCONSISTENT) ;;
    # ===== `DEFERRED`: ONLY ON `findings:`, AND ONLY WHEN AFFIRMATIVELY GRANTED (#3626) =====
    # TWO independent requirements, each with its own diagnostic, because they are different defects
    # with different remedies. (i) The KEY must be `findings` — a deferral authorizes a named set of
    # FINDINGS and confers no authority over any other check, so a `DEFERRED` anywhere else is a
    # wrapper defect, not something the branch under review can fix. (ii) The oracle must have
    # affirmatively GRANTED, so a fabricated token that measured nothing cannot ride to a PASS.
    # Ordered (i) then (ii) deliberately: a misplaced token is a defect whether or not an
    # authorization exists, and reporting it as "ungranted" would send the reader to look for a
    # marker that would not have helped.
    DEFERRED)
      if [ "$scan_key" != findings ]; then
        failed=1
        misplaced_deferral="${misplaced_deferral:+$misplaced_deferral; }$scan_key: '$verdict'"
      elif [ "$deferral_admits" -ne 1 ]; then
        failed=1
        ungranted_deferral="${ungranted_deferral:+$ungranted_deferral; }'$verdict'"
      fi
      ;;
    PASS|WAIVED|SKIP|NOTICE|UNAVAILABLE|DEGRADED|NONE|PRESENT|UNKNOWN) ;;
    *)
      failed=1
      unrecognised="${unrecognised:+$unrecognised; }'$verdict'"
      ;;
  esac
done
# ====== AND A PASS NEEDS EVERY DETERMINISTIC KEY TO HAVE AFFIRMATIVELY PASSED ======
# The grammar check above closes "an unplanned value inherits the non-failing branch". This
# closes the neighbouring case: a value that is IN the grammar and non-failing, but is not a
# measurement — `SKIP`, i.e. "this check never ran". The six keys below are the ones that
# CARRY the verdict (each judged against data the wrapper obtained itself), and on a PASS
# every one of them must be an affirmative `PASS` — no exceptions, and there is deliberately
# no exemption mechanism. (One existed briefly, for a key allowed a `NOTICE` because a
# remedy-less swallow was a measurement with a stated residual; both that key and its
# exemption are gone — #3283/#3278 — so the backstop is uniform, which is STRICTER, never
# weaker.)
#
# MATCHED ON THE VERDICT TOKEN, EXACTLY, for the same reason the grammar scan above is: a
# `PASS*` prefix glob would accept `PASSthisNeverRan` as an affirmative pass, i.e. the
# backstop against unmeasured keys would itself be satisfiable by a value that measured
# nothing. The token is the value up to its first space, compared exactly.
# `vacuity-tier1/2` and `findings:` are EXCLUDED FROM THIS LOOP, for two DIFFERENT reasons
# that #3564 showed must not be stated as one. `vacuity-tier1/2` CORROBORATE, and
# `UNAVAILABLE` is a documented, legitimate value for them on a clean run. `findings:` is
# excluded only because its affirmative value is `NONE` rather than `PASS`, so it cannot
# satisfy this loop's uniform test — it is NOT unguarded: it has its own affirmative gate
# IMMEDIATELY ABOVE, which is stricter than this loop (no `WAIVED` is admitted there).
# Reading the old wording as "findings only corroborates" is what left the recheck path
# able to PASS beside `findings: PRESENT (3)`.
#
# WHY IT IS NOT REDUNDANT with the checks-file validation: that validation proves the five
# functions EXIST, not that each reached its assignment. A check that returned early — an
# aborted helper, a `return` added inside a new branch, a sourced file that defines a function
# whose body changed — leaves its key at the initial `SKIP` and, before this, the run PASSED
# with a key that had measured nothing. "PASS requires POSITIVE evidence" is the wrapper's
# stated contract (see EXIT CODES above); this is the contract enforced rather than intended.
#
# ====== AND `findings:` MUST BE AFFIRMATIVELY `NONE` FOR A PASS, IN EVERY MODE ======
# (#3564.) `findings:` reports whether the REVIEW found anything, and until this existed it could
# not fail the run ON ITS OWN: `PRESENT` is in the grammar's non-failing set above, so the only
# thing failing a findings-bearing run was the NEIGHBOURING key `roborev-exit: FINDINGS (exit 1)`.
# That coupling held for a fresh review and broke exactly where it mattered most. On
# `--recheck-job` NO REVIEWER PROCESS RUNS, so `roborev-exit` is legitimately `SKIP` — and with the
# failing signal gone, a recheck of a job whose record carries findings emitted
# `findings: PRESENT (3)` beside `RESULT: PASS`. Measured on #3473's round-3 recovery (job 160):
# a FALSE PASS IN A MERGE GATE, and the wrapper's own documented affirmation backstop did not catch
# it because `findings` is deliberately excluded from the loop below.
#
# This is this repository's named recurring defect, in the one place it is most expensive: **A
# POSITIVE VERDICT REQUIRES AN AFFIRMATIVE MEASUREMENT** — never derive a pass from the ABSENCE of
# a bad signal. So the requirement is stated POSITIVELY and on `findings`'s OWN terms: its
# affirmative value is `NONE`, not `PASS`. That is why this is its OWN statement and not a per-key
# affirmative token inside the loop below — a key-scoped special case there is the shape that has
# to be re-argued every time a key is added, and the loop's uniform "every key must read PASS" is
# the property that makes it a backstop. `PRESENT`, `UNKNOWN` and the initial `SKIP` all fail here;
# `INCONSISTENT` already fails the grammar scan, and this is a second, independent reason it cannot
# pass.
#
# MATCHED ON THE VERDICT TOKEN, EXACTLY, for the same reason both scans above are: `PRESENT (3)`
# must reduce to `PRESENT`, and a `NONE-BUT-UNMEASURED` variant must NOT satisfy a `NONE*` prefix.
#
# THE FIX BELONGS HERE, NOT IN `roborev-exit`. `SKIP` is the TRUE statement about a recheck — the
# reviewer genuinely did not run — so making that key claim a failure it did not observe would
# trade one false statement for another.
#
# DELIBERATELY NOT WAIVABLE, and this is the sharpest reason the gate has to be here. The absence
# waiver (#3312) excuses `prompt-content` ABSENCE **only**, and `--recheck-job` is the ONLY path a
# waiver can travel (a re-run enqueues a different job and stales it) — so a waiver-bearing recheck
# is PRECISELY the run this must still fail. Admitting `WAIVED` here would let one authorization
# excuse findings no human agreed to excuse.
#
# Evaluated only on a would-be PASS, and BEFORE the affirmation loop: where both this and the
# structural backstop would fire, OPEN FINDINGS are what the reader must act on, and a wrapper
# defect reported over them would bury it.
# ====== AND `DEFERRED` IS THE ONE OTHER VALUE THAT MAY RIDE, ON AN AUTHORIZED DEFERRAL (#3626) ======
# "roborev clean" means NO UNADDRESSED FINDINGS, not "the tool printed zero". A lead-deferred finding
# is re-reported by every later round, so the affirmative-`NONE` requirement -- correct in itself --
# blocked such a merge FOREVER, and the lane that behaved correctly (refused to manufacture a green,
# asked instead) was the one it punished. So the terminal verdict is gated on the UNDEFERRED set: the
# `NONE` requirement is unchanged, and a `DEFERRED` findings value additionally rides ONLY on
# `deferral_admits` -- the single coupled state above, which requires the authorization's scope, its
# author, its reason, its filed issues and its declared count to all match what this run measured. It
# is NOT a waiver of the findings requirement: nothing here admits `PRESENT`, `UNKNOWN` or `SKIP`, and
# `findings:` never reports `NONE` on account of a deferral. And this is the ONLY key the mechanism can
# reach: the grammar scan above admits the token for `findings` alone, and the affirmation backstop
# below admits it for nothing.
findings_deferred=0
if [ "$deferral_admits" -eq 1 ] && [ "${FINDINGS%% *}" = DEFERRED ]; then
  findings_deferred=1
fi
if [ "$failed" -eq 0 ] && [ "${FINDINGS%% *}" != NONE ] && [ "$findings_deferred" -ne 1 ]; then
  failed=1
  DETAILS+=("ERROR: findings: this run would have PASSED while 'findings:' reads '$FINDINGS' — and only an affirmative 'NONE' certifies that the review found nothing. A review with OPEN FINDINGS is not \"roborev clean\", whatever the neighbouring keys say: on --recheck-job no reviewer process runs, so 'roborev-exit' is legitimately SKIP and CANNOT be the thing that fails a findings-bearing run (#3564). If this is PRESENT, triage the findings in the review record ($LOG), fix them, then push and re-review. 'UNKNOWN' or 'SKIP' means the findings state was never ESTABLISHED, which fails closed for the same reason — a pass may not rest on a state we could not read. This requirement is NOT waivable in any mode: the absence waiver excuses prompt-content absence only. THE ONE OTHER ROUTE PAST A 'PRESENT' IS A LEAD-AUTHORIZED DEFERRAL (#3626), which is a SEPARATE authorization on its own marker: it reports 'findings: DEFERRED (...)' beside a 'deferral: GRANTED (...)' key, never NONE, and it requires the authorized count to EQUAL the observed count with every deferred finding filed as an issue GitHub can retrieve. Deferral state for this run: ${DEFERRAL_REPORT:-<not looked for: a deferral is decided only on --recheck-job, over an affirmatively measured PRESENT (n)>}.")
fi
# Evaluated ONLY when the run would otherwise PASS, deliberately: on an already-failing run
# every non-affirmative key has its own diagnostic under its own name, and repeating them here
# would bury the actionable cause under a structural one.
if [ "$failed" -eq 0 ]; then
  not_affirmed=""
  for keyed in "push-assert=$PUSH_ASSERT" "census-check=$CENSUS_CHECK" "code-free=$CODE_FREE" \
    "sha-assert=$SHA_ASSERT" \
    "review-completed=$REVIEW_COMPLETED" "prompt-content=$PROMPT_CONTENT"; do
    det_key="${keyed%%=*}"
    det_value="${keyed#*=}"
    case "${det_value%% *}" in
      PASS) continue ;;
      # ===== `WAIVED`: A HUMAN-AUTHORIZED ABSENCE, GATED ON ITS OWN PROVENANCE =====
      # (Owner ruling (4), #3312.) This is NOT the per-key escape hatch that used to live here — that
      # one admitted a `NOTICE` for one named key in one machine-inferred mode, and the inference it
      # rested on is deleted. This admits a token that only exists when a human named THIS head sha in
      # a PR comment, and it is gated on the PROVENANCE being complete rather than on which key
      # carries it: an authorizer, the certified sha and a reason must all be present, so a `WAIVED`
      # produced by a future code path that measured nothing cannot ride to a PASS. Not gated on
      # `det_key`, deliberately: a key-scoped exemption is the shape that has to be re-argued every
      # time a key is added, and the provenance test is the property that actually matters.
      WAIVED)
        if [ -n "${ROBOREV_WAIVER_AUTHOR:-}" ] && [ -n "${ROBOREV_WAIVER_REASON:-}" ] \
          && [ "${ROBOREV_WAIVER_SCOPE:-}" = "base=${RANGE_BASE_SHA:-} head=${HEAD_SHA:-} job=${JOB:-}" ] \
          && [ "${ROBOREV_WAIVER_STATE:-}" = "granted" ]; then continue; fi
        ;;
      # ===== AND THERE IS DELIBERATELY NO `DEFERRED` ARM HERE (#3626, roborev job 225) =====
      # One existed, admitting the token from `deferral_admits` for any of these six keys, described as
      # a "structural backstop for a future key that acquires the token". It was the opposite: a
      # findings deferral says nothing about whether the reviewer's diff arrived or the reviewed range
      # matched, so admitting it here let ONE authorization excuse a check nobody authorized, and the
      # only thing preventing that was that no other key HAPPENS to emit `DEFERRED` (#3564: delegating
      # a key's failure to its neighbour is a latent false pass). `findings:` is not in this loop, and
      # the grammar scan above admits `DEFERRED` for `findings` alone — so a deterministic key holding
      # the token now fails there, by key name, with its own diagnostic. Do not add an arm back.
    esac
    not_affirmed="${not_affirmed:+$not_affirmed; }$det_key: '$det_value'"
  done
  if [ -n "$not_affirmed" ]; then
    failed=1
    DETAILS+=("ERROR: verdict-affirmation: this run reached the PASS branch with a VERDICT-CARRYING key that never affirmatively passed — $not_affirmed. A PASS must rest on POSITIVE evidence from every deterministic check (push-assert, census-check, code-free, sha-assert, review-completed, prompt-content); a non-failing value that is not a measurement — 'SKIP' above all, which means the check NEVER RAN — is exactly the vacuous pass this wrapper exists to prevent, and it is textually indistinguishable from a genuine one. Failing closed. This is a structural backstop, so its cause is a defect in the wrapper or its sourced files (a check that returned before assigning its key), NOT something to fix in the branch under review.")
  fi
fi
if [ -n "$misplaced_deferral" ]; then
  DETAILS+=("ERROR: verdict-grammar: a per-check key OTHER THAN 'findings:' reports a DEFERRED state — $misplaced_deferral. A lead-authorized deferral (#3626) defers a NAMED SET OF FINDINGS and confers authority over the 'findings:' key and nothing else: it says nothing about whether the reviewer's diff arrived (prompt-content), whether the branch was pushed, or whether the reviewed range matched this base and head, so it may not excuse any of them. Admitting it elsewhere would let ONE authorization excuse a check NOBODY authorized. This holds even when a deferral WAS granted, and it is not waivable. Failing closed: the cause is a defect in the wrapper or its sourced files — a check that assigned DEFERRED to its own key — NOT something to fix in the branch under review. An absence of prompt-content evidence has its OWN separate authorization (the #3312 waiver, reported as 'prompt-content: WAIVED'); every other key must simply pass.")
fi
if [ -n "$ungranted_deferral" ]; then
  DETAILS+=("ERROR: verdict-grammar: a per-check key reports a DEFERRED state that the deferral oracle did not affirmatively GRANT: $ungranted_deferral. DEFERRED is non-failing ONLY on a complete, matching authorization — a top-level PR comment from an allowlisted author, whose SOLE NONBLANK CONTENT names THIS base, head and job, whose authorized count EQUALS the count this run observed, and each of whose named issues is an OPEN issue GitHub confirms — and only on --recheck-job. Deferral state for this run: ${DEFERRAL_REPORT:-<none looked for>}. Failing closed: a DEFERRED token that no authorization backs is indistinguishable from an authorized one to every reader of this block, which is the false-assurance shape this wrapper exists to prevent.")
fi
if [ -n "$unrecognised" ]; then
  DETAILS+=("ERROR: verdict-grammar: a per-check key holds a value outside the block's documented grammar: $unrecognised. Every key must report one of FAIL / FINDINGS / ERROR / INCONSISTENT (failing) or PASS / WAIVED / SKIP / NOTICE / UNAVAILABLE / DEGRADED / NONE / PRESENT / UNKNOWN (non-failing), or DEFERRED (non-failing ONLY on an affirmatively granted deferral). An unrecognised value means a check did not reach an assignment (an early return, an aborted helper), introduced a state this scan has never judged, or glued extra characters onto a recognised token (the token is matched EXACTLY, up to the value's first space, so 'PASSthisNeverRan' is unrecognised rather than a pass) — so the run FAILs closed rather than letting the unplanned value inherit the non-failing branch. An EMPTY value ('') is this same defect with nothing to print. Fix the check that produced it; do not add the value to the recognised set without deciding what it MEANS for the verdict.")
fi

if [ "$failed" -eq 0 ]; then
  finish PASS 0
fi
finish FAIL 1
