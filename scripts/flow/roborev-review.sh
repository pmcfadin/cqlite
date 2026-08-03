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
#      PR #3222 (#3229), which is why `census-exclusion:` now reconciles the census
#      against the configured set BEFORE anything is enqueued.
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
#   repo: / branch: / base: / head-sha: / reviewed-sha: / job: / model: / census:
#   tokens: / push-assert: / census-check: / code-free: / census-exclusion: /
#   job-record: / sha-assert: / review-completed: / prompt-content: /
#   vacuity-tier1: / vacuity-tier2: / findings: / roborev-exit: / log:
#   RESULT: PASS|FAIL|NOTHING-TO-REVIEW
#
# Per-check values, as built:
#   push-assert / census-check / code-free / review-completed  PASS | FAIL (...) | SKIP
#   census-exclusion  PASS (<k>/<n> code census paths survive the effective exclusion
#                     set; corroboration: OK|NOTICE|UNAVAILABLE) |
#                     PASS (no exclusion patterns configured; <k>/<n> ... survive the
#                     <b> roborev v0.61.2 built-in exclude(s); corroboration: ...) |
#                     NOTICE (<k>/<n> ... survive ...; <m> code census path(s) excluded
#                     by a roborev built-in: <path> by '<pat>' [roborev-builtin]; ...) |
#                     FAIL (<m>/<n> code census paths excluded: <path> by '<pat>'
#                     [<source>], ...) |
#                     FAIL (exclusion set unreadable: ...) |
#                     FAIL (trailing-slash pattern ...) |
#                     FAIL (exclusion set drift: ...) | SKIP (<cause>)
#   job-record        PASS | PASS (no token accounting in the record) |
#                     DEGRADED (incomplete after <n> retries: <fields>) | SKIP
#   sha-assert        PASS | FAIL (...) | SKIP
#   prompt-content    PASS (<k>/<n> code census paths present)
#                     [ (+<b> not expected: excluded by a roborev built-in — ...) ] |
#                     FAIL (<k>/<n> code census paths absent from the prompt) |
#                     FAIL (no code census path was checkable — a 0/0 is never a pass) |
#                     FAIL (prompt unretrievable — ...) | SKIP
#                     Paths are compared NORMALISED (both sides through
#                     roborev_unquote_path) and EVERY header shape git emits is
#                     recognised: unquoted, space-bearing, and C-quoted.
#   vacuity-tier1     PASS | FAIL (vacuous verdict vs non-empty census) |
#                     NOTICE (phrase present in a findings-bearing review) |
#                     UNAVAILABLE | SKIP        (ADVISORY when it is a NOTICE)
#   vacuity-tier2     PASS | FAIL (...) | UNAVAILABLE | SKIP
#   findings          NONE | PRESENT [(<n>)] | INCONSISTENT (...) | UNKNOWN | SKIP
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
# terms).
#
# WHICH CHECKS CARRY THE VERDICT. The DETERMINISTIC ones, each judged against data we
# obtained ourselves: `push-assert` (the remote, via ls-remote), `census-check` (our
# own git diff), `code-free` (our own census classification), `census-exclusion` (our
# census vs the configured exclusion set, matched by git — #3229), `sha-assert` (the job
# record's git_ref, asserting BOTH endpoints of the reviewed range against the census
# range), `review-completed` (job status + an allow-list of terminal verdict markers),
# `prompt-content` (the CODE subset of our census inside the prompt actually sent). Prose matching (`vacuity-tier1`) and token accounting (`vacuity-tier2`)
# CORROBORATE; tier 1 can only ever raise a NOTICE.
#
# EXIT CODES (exactly three outcomes plus a usage code)
#   0  PASS               — a review demonstrably HAPPENED against branch HEAD with
#                           no vacuity signal. PASS requires POSITIVE evidence; it is
#                           never inferred from the absence of a bad phrase.
#   1  FAIL               — any failed check: push-assert, census-check, code-free,
#                           census-exclusion, sha-assert, review-completed, prompt-content,
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

census-exclusion: (PRE-ENQUEUE, immediately after code-free: in the fixed key
order) reconciles the CODE census against the EFFECTIVE exclusion set by porting
roborev v0.61.2's git.FormatExcludeArgs and letting GIT match.
roborev drops exactly what its pathspecs match and makes NO code/non-code judgement,
which is why this is computed, not assumed (#3229: a configured 'docs/**' discarded
33 executable harness files on PR #3222).
THE EFFECTIVE SET IS A UNION OF FOUR THINGS, and a swallow in ANY of them FAILs:
  1. the wrapper's --repo checkout .roborev.toml   [worktree-config|repo-config]
  2. the ROOT checkout's .roborev.toml        [root-config]  <-- when --repo is a
     LINKED WORKTREE. roborev's daemon binds a repo by its repos.root_path, i.e. the
     ROOT checkout, and reads THAT file: a narrowed worktree config does NOT override
     it, and reading only (1) reported '7/7 survive' on a branch whose real review
     came back 'prompt-content: FAIL (1/7 absent)'.
  3. ~/.roborev/config.toml                   [global-config]
  4. roborev's own BUILT-IN excludes          [roborev-builtin]  <-- NOT configurable:
     the hard-coded lockfile/cache deny-list (**/Cargo.lock, **/go.sum,
     **/package-lock.json, ... **/.beads/**, **/.cache/**), extracted from the pinned
     v0.61.2 binary. A Cargo.lock in your diff IS dropped from the reviewer's copy.
Every value line names the SOURCE of the pattern responsible, because with several
files in play "excluded by 'docs/**'" does not say which file to edit.
THE UNIFYING RULE, which decides every value below:
  **FAIL where the author can act; NOTICE where only the information is actionable;
    never silence.**
It is not three ad-hoc calls but one rule applied three times:
  - a CONFIGURED pattern (sources 1-3) swallowing census code => FAIL. The remedy is a
    one-token edit to a NAMED file; act before paying for a review round.
  - a PINNED built-in (source 4) swallowing SOME census code => NOTICE. There IS no fix:
    the deny-list is compiled in, with no opt-out and no negation form. A check that
    fires on a legitimate change (a routine Cargo.lock touch) with no remedy available
    is a check that gets DISABLED, and a disabled census-exclusion is #3229 all over
    again.
  - a PINNED built-in swallowing the WHOLE code census => FAIL. Not an exception to the
    NOTICE above but the SAME rule reaching a case it does not cover: with nothing left,
    the reviewer receives an EMPTY prompt, so any verdict certifies NOTHING — the very
    condition code-free: already FAILs pre-enqueue for a prose-only census. The remedy
    is code-free's: verify another way and record it in the PR. Left as a NOTICE it let
    prompt-content: print 'PASS (0/0 code census paths present)' and the block read
    'RESULT: PASS' — a vacuous pass textually identical to a genuine one (MEASURED on a
    Cargo.lock + README.md fixture). Any lockfile-only dependency bump reaches it, and
    code-free: does not catch it because a '.lock' extension classifies as CODE.
  - the LIVE built-in set DIVERGING from the pinned 24 => FAIL. This one HAS a remedy
    (re-extract, update the pin, judge the new built-in) and it is a MECHANISM change,
    which the v0.61.2 pin already obliges us to catch on upgrade. A NOTICE here would
    silently absorb an upgrade that began excluding '*.rs' or 'scripts/**', with the
    failure looking like normal operation.
"never silence" is the third clause and it is load-bearing: an UNOBSERVABLE built-in
set reads 'built-in-set: UNAVAILABLE' in the value line, never an unstated assumption
of agreement. Every value therefore ends with built-in-set: OK|DIVERGED|UNAVAILABLE.
Both FAIL causes outrank the NOTICE, and EVERY cause present is named. Values:
  PASS (<k>/<n> code census paths survive ...; corroboration: OK|NOTICE|UNAVAILABLE;
       built-in-set: OK|UNAVAILABLE)
  PASS (no exclusion patterns configured; <k>/<n> ... survive the <b> roborev
       v0.61.2 built-in exclude(s); ...)  absent key/file or an empty list, and only
       once 'roborev config get' has CORROBORATED that nothing is configured
  NOTICE (<k>/<n> ... survive ...; <m> code census path(s) excluded by a roborev
       built-in: <path> by '<pattern>' [roborev-builtin]; ...)  PARTIAL swallow only
       (a total one is the FAIL below); NON-FAILING (NOTICE is
       outside the verdict scan's FAIL*|FINDINGS*|ERROR*|INCONSISTENT* set) but the
       paths ARE named: a clean verdict does not cover them, and prompt-content: is
       told not to expect them
  FAIL (0/<n> code census paths survive ...; ALL <n> code census path(s) excluded by a
       roborev built-in, so the reviewer would receive an EMPTY diff: ...)  the TOTAL
       swallow — uncertifiable, exactly as a code-free census is
  FAIL (roborev built-in exclude set DIVERGED from the pinned v0.61.2 set: <delta>)
       DIFF-INDEPENDENT — the mechanism moved under us; re-extract and re-pin
  FAIL (<m>/<n> code census paths excluded: <path> by '<pattern>' [<source>], ...)
  FAIL (exclusion set unreadable: <cause>) present but unparseable, an unknown TOML
                                          escape, or an unresolvable ROOT checkout —
                                          DISTINCT from 'no exclusion patterns
                                          configured'
  FAIL (trailing-slash pattern '<p>/' from <source> resolves RECURSIVE ...)
                                          diff-independent
  FAIL (exclusion set drift: '<pattern>' reported by roborev config get ...)  also
       how a key spelling this parser does not recognise — but roborev DOES honour —
       is caught, instead of reading as a green 'nothing configured'
  SKIP (<cause>)                          the step was not reached
A FAIL naming a config source is a CONFIGURATION defect: fix the named file — do not
investigate the reviewer or prompt-content:. A NOTICE naming [roborev-builtin] is not
fixable: roborev will never show a reviewer that path, so verify it some other way.
A FAIL naming a DIVERGED built-in set is fixable HERE: re-extract the deny-list with
  LC_ALL=C grep -a -o ':(exclude,glob)[^ ]*' "$(command -v roborev)"
then update ROBOREV_BUILTIN_EXCLUDES + ROBOREV_BUILTIN_PATHSPEC_LITERALS in
scripts/flow/roborev-review-oracles.sh and JUDGE the new built-in.
Re-verify BOTH the port and the built-in list on any roborev version bump.

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
         == git rev-parse <base> — i.e. the reviewed range IS <base>...HEAD;
       - reviewed-sha is NOT the base ref alone, and its head endpoint is NOT the base
         sha: either means the review never reached the worktree's own commits, which
         is the root-checkout resolution this probe exists to rule out;
       - prompt-content: PASS with the full code census covered;
       - census matching 'git diff --numstat --no-renames <base>...HEAD';
       - job-record: PASS and tokens above both thresholds.
     RESULT is PASS (exit 0) only when the review is also finding-free; a review with
     open findings correctly reports FINDINGS and exits 1 — that is not a probe
     failure, and the scope assertions above are what the probe is for.
  4. Record the observed head-sha/reviewed-sha/job/census/tokens in the PR body,
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
# CODE census paths that `census-exclusion:` determined a ROBOREV BUILT-IN exclude drops
# (never a configured pattern — that direction FAILs pre-enqueue and never gets here).
# `prompt-content:` subtracts these, because their absence from the prompt is a
# deterministic property of roborev's compiled-in deny-list, not evidence about the
# reviewer — and re-reporting a known absence as a discovery would red a routine
# Cargo.lock touch under a second key with no remedy behind either.
CENSUS_BUILTIN_EXCLUDED=()
PUSH_ASSERT="SKIP"
CENSUS_CHECK="SKIP"
CODE_FREE="SKIP"
# The census-vs-configured-exclusion reconciliation (#3229). An explicit SKIP CAUSE,
# never a blank: this key's whole job is to say whether the configured exclusion set
# would swallow the census's CODE paths, and a blank value would read as a pass.
CENSUS_EXCLUSION="SKIP (not reached)"
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

emit_summary() {
  printf '==== ROBOREV REVIEW SUMMARY ====\n'
  printf 'repo: %s\n' "$REPO"
  printf 'branch: %s\n' "$BRANCH"
  printf 'base: %s\n' "$BASE"
  printf 'head-sha: %s\n' "${HEAD_SHA:--}"
  printf 'reviewed-sha: %s\n' "$REVIEWED_SHA"
  printf 'job: %s\n' "$JOB"
  printf 'model: %s\n' "$MODEL_LINE"
  printf 'census: %s\n' "$CENSUS"
  printf 'tokens: %s\n' "$TOKENS"
  printf 'push-assert: %s\n' "$PUSH_ASSERT"
  printf 'census-check: %s\n' "$CENSUS_CHECK"
  printf 'code-free: %s\n' "$CODE_FREE"
  # Immediately after `code-free:` — mirroring the pre-enqueue evaluation order, and
  # part of the block's FIXED key contract (#3229).
  printf 'census-exclusion: %s\n' "$CENSUS_EXCLUSION"
  printf 'job-record: %s\n' "$JOB_RECORD"
  printf 'sha-assert: %s\n' "$SHA_ASSERT"
  printf 'review-completed: %s\n' "$REVIEW_COMPLETED"
  printf 'prompt-content: %s\n' "$PROMPT_CONTENT"
  printf 'vacuity-tier1: %s\n' "$TIER1"
  printf 'vacuity-tier2: %s\n' "$TIER2"
  printf 'findings: %s\n' "$FINDINGS"
  printf 'roborev-exit: %s\n' "$ROBOREV_EXIT"
  printf 'log: %s\n' "$LOG"
  printf 'RESULT: %s\n' "$RESULT"
}

finish() { # finish <PASS|FAIL|NOTHING-TO-REVIEW> <exit-code>
  RESULT="$1"
  if [ "${#DETAILS[@]}" -gt 0 ]; then
    printf '%s\n' "${DETAILS[@]}"
  fi
  EMITTED=1
  emit_summary
  exit "$2"
}

# Emit a block on EVERY exit path, including an unexpected `set -e` abort: a run
# that dies without a verdict must never look like a run that was never made.
# shellcheck disable=SC2317 # invoked indirectly, by `trap on_exit EXIT` below
on_exit() {
  local rc=$?
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
if [ "$(type -t roborev_check_census_exclusion)" != function ]; then
  DETAILS+=("ERROR: '$ORACLES_FILE' did not define roborev_check_census_exclusion, so the census could not be reconciled against roborev's configured exclusion set. Failing closed — the file is truncated or corrupt.")
  finish FAIL 1
fi

roborev_push_assert
roborev_census
# Reconcile the CODE census against the EFFECTIVE `exclude_patterns` (#3229). Placed
# HERE — after the census/`code-free:` classification it consumes, and BEFORE the
# checks-file validation and the enqueue — so a configuration that would swallow census
# code costs no review round, and reports itself under its own key rather than
# surfacing later as `prompt-content:` (which would send the reader to investigate the
# reviewer for a defect entirely in configuration).
roborev_check_census_exclusion

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
  roborev_check_findings roborev_check_tier1 roborev_check_tier2; do
  if [ "$(type -t "$roborev_required_check")" != function ]; then
    DETAILS+=("ERROR: '$CHECKS_FILE' did not define $roborev_required_check, so that check cannot run. Failing closed — the file is truncated or corrupt.")
    finish FAIL 1
  fi
done

# --- step 4: invoke over the CENSUS RANGE + an EXPLICIT absolute repo (AC2) ----
if ! command -v roborev >/dev/null 2>&1; then
  SHA_ASSERT="FAIL (roborev not on PATH)"
  DETAILS+=("ERROR: 'roborev' is not on PATH, so the review cannot be performed and the census cannot be certified. Failing closed rather than reporting a pass.")
  finish FAIL 1
fi

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
set +e
roborev review --branch \
  --base "$BASE" \
  --repo "$REPO" \
  --agent "$AGENT" \
  --model "$MODEL" \
  --wait >"$LOG" 2>&1
REVIEW_RC=$?
set -e

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
  if [ "${ANNOUNCE_COUNT:-0}" -gt 1 ]; then
    DETAILS+=("NOTICE: sha-assert: the transcript carries $ANNOUNCE_COUNT enqueue announcements; the LAST one (job $JOB) is the effective enqueue and is the one asserted.")
  fi
fi

# --- structured job facts (extracted by scripts/flow/roborev-job-facts.py) -----
# Diagnostics live beside the transcript; `log:` names the base path.
FACTS_FILE="$LOG.facts"
PROMPT_FILE="$LOG.prompt"
: >"$FACTS_FILE"
: >"$PROMPT_FILE"

extract_job_facts() { # extract_job_facts <job> <json> <facts-out> <prompt-out>
  command -v python3 >/dev/null 2>&1 || return 1
  [ -f "$JOB_FACTS_TOOL" ] || return 1
  [ -n "$2" ] || return 1
  printf '%s' "$2" | python3 "$JOB_FACTS_TOOL" "$1" "$3" "$4" 2>/dev/null
}

fact() { sed -n "s/^$1=//p" "$FACTS_FILE" | head -1; }

# TWO PAYLOAD SHAPES, NOT AN ASYNC WRITE (issue #2964; the round-5 "the job record is
# written asynchronously" diagnosis was WRONG and is retracted — there is no durability
# problem and no write race). The fields were always present, one level down:
#   * `roborev list --json` returns JOB rows with git_ref / status / model /
#     requested_model / token_usage / verdict at TOP level.
#   * `roborev show <id> --json` returns a REVIEW row — agent, closed, created_at, id,
#     job, job_id, output, prompt, uuid, verdict_bool — that NESTS the job row under a
#     `job` key. Its own `id` equals the job id, so a first-id-match lookup returned
#     the OUTER row, which carries none of those fields; that looked like an empty
#     record and silently weakened FOUR asserts at once on a NORMAL run (sha-assert
#     fell back to prose, review-completed to the transcript alone, tier 2 to
#     UNAVAILABLE, model to UNCONFIRMED).
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
      list) json=$(roborev list --json --limit 50 --repo "$REPO" 2>/dev/null || printf '') ;;
    esac
    extract_job_facts "$1" "$json" "$FACTS_FILE" "$PROMPT_FILE" || continue
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
if [ "$announce_ok" -eq 1 ]; then
  case "$JOB_GIT_REF" in
    *..*)
      range_base="${JOB_GIT_REF%%..*}"
      range_head="${JOB_GIT_REF##*..}"
      REVIEWED_SHA="$JOB_GIT_REF"
      if [ "$range_head" = "$HEAD_SHA" ] && [ "$range_base" = "$BASE_SHA" ]; then
        SHA_ASSERT="PASS"
      else
        SHA_ASSERT="FAIL (reviewed range does not match ${BASE}...HEAD)"
        DETAILS+=("ERROR: sha-assert: the reviewed range '$JOB_GIT_REF' does not match the census range: expected base '$BASE_SHA' ($BASE) and head '$HEAD_SHA' (job $JOB).")
        if [ "$range_base" != "$BASE_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the range BASE is '$range_base', not '$BASE_SHA'. An empty-tree base (4b825dc6...) is the signature of the non-sanctioned two-positional commit-range form.")
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
        if [ "$JOB_GIT_REF" = "$BASE_SHA" ]; then
          DETAILS+=("ERROR: sha-assert: the reviewed sha EQUALS the base ref '$BASE' ($BASE_SHA) — NO branch change was reviewed. That equality is the signature of a '--branch' review resolved against the ROOT checkout instead of this worktree.")
        else
          DETAILS+=("ERROR: sha-assert: the reviewed sha matches NEITHER endpoint (head '$HEAD_SHA', base '$BASE' $BASE_SHA).")
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

# --- step 7: the verdict ------------------------------------------------------
# The findings COUNT is best-effort (it counts severity markers in the transcript);
# the PRESENT/NONE/UNKNOWN distinction is the load-bearing part — tier 1 is gated on it.
#
# Every per-check key participates in ONE scan. A key fails the run when its value
# starts with FAIL, FINDINGS, ERROR or INCONSISTENT; PASS / SKIP / UNAVAILABLE /
# NOTICE / DEGRADED never do (NOTICE is tier 1's non-failing value; DEGRADED reports
# an incomplete job record, whose consequences are carried by the dependent asserts).
failed=0
for verdict in "$PUSH_ASSERT" "$CENSUS_CHECK" "$CODE_FREE" "$CENSUS_EXCLUSION" "$SHA_ASSERT" \
  "$REVIEW_COMPLETED" "$PROMPT_CONTENT" "$TIER1" "$TIER2" "$FINDINGS" "$ROBOREV_EXIT"; do
  case "$verdict" in FAIL*|FINDINGS*|ERROR*|INCONSISTENT*) failed=1 ;; esac
done

if [ "$failed" -eq 0 ]; then
  finish PASS 0
fi
finish FAIL 1
