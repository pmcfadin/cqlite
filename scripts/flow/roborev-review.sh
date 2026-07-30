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
# red anywhere. Three confirmed triggers:
#
#   1. bare `--branch` from inside a git WORKTREE resolves against the ROOT
#      checkout (which normally sits on `main`), so the enqueued commit is
#      `origin/main` and the branch's own change is never seen. Worktrees are not
#      in `roborev repo list`, and `roborev repo` has NO `add` subcommand — repos
#      self-register on first use — so there is nothing to register and no way to
#      make the bare form worktree-correct. => bare `--branch` is NON-SANCTIONED.
#   2. the two-positional commit-RANGE form (`roborev review <a> <b>`) has been
#      OBSERVED enqueueing a commit that is NEITHER endpoint. => NON-SANCTIONED.
#   3. a code-free (docs/spec/workflow-only) diff is STRUCTURALLY DISCARDED and
#      still reported as "No issues found" — with the CORRECT sha enqueued, so no
#      sha check can catch it. => a docs-only diff CANNOT be roborev-certified at
#      all; the sanctioned substitute is primary-source verification recorded in
#      the PR (e.g. `git show cassandra-5.0.8:<path>`).
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
#   repo: / branch: / base: / head-sha: / reviewed-sha: / job: / census: / tokens:
#   push-assert: / census-check: / sha-assert: / vacuity-tier1: / vacuity-tier2:
#   roborev-exit: / log:
#   RESULT: PASS|FAIL|NOTHING-TO-REVIEW
#
# Per-check values are PASS | FAIL | SKIP | UNAVAILABLE (a FAIL may carry a
# parenthesised reason). `census:` is `<N> files, +<A>/-<D>`; `tokens:` is
# `input=<n> cached=<n> output=<n>` or `UNAVAILABLE`.
#
# EXIT CODES (exactly three outcomes plus a usage code)
#   0  PASS               — reviewed, sha verified, no vacuity signal.
#   1  FAIL               — any failed check: push-assert, census-check, sha-assert,
#                           vacuity-tier1, vacuity-tier2, or roborev-exit (the
#                           reviewer process's own status). Each names itself under
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
# every guard against a stub reviewer. Only this probe can prove the REAL external
# binary honours the explicit `--repo` from inside a worktree. Procedure:
#
#   1. Confirm the ROOT checkout sits on `main` (that is what makes trigger 1
#      reproducible):  git -C <root> rev-parse --abbrev-ref HEAD   # => main
#   2. From a real issue worktree on its own branch, with its commit PUSHED:
#        cd /path/to/cqlite-wt/issue-<N>
#        scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol
#   3. Read ONLY the emitted summary block and assert:
#        head-sha:      == the WORKTREE branch HEAD (git rev-parse HEAD)
#        reviewed-sha:  == head-sha (prefix match)     <-- the probe's point
#        reviewed-sha:  != `git rev-parse origin/main` <-- trigger 1 defeated
#        sha-assert:    PASS
#        census:        matches `git diff --numstat origin/main...HEAD`
#        tokens:        input/cached/output all above the thresholds below
#        RESULT:        PASS   (exit 0)
#      A `reviewed-sha` equal to `origin/main` means the explicit-repo invocation
#      did NOT defeat the root-checkout resolution and the wrapper must be fixed
#      before any verdict from it is trusted.
#   4. Record the observed head-sha / reviewed-sha / job / census / tokens in the
#      PR body as the live evidence. Re-run after any roborev version bump.
set -euo pipefail

# --- Tier-2 vacuity thresholds (named constants; measured evidence below) -------
#
# MEASURED EVIDENCE (issue #2964, real jobs on the fleet):
#   GENUINE reviews:  398k-649k input, 314k-554k cached, 5.0k-6.3k output,
#                     2m25s-2m45s wall  (jobs 4652 / 4654 / 4656)
#   VACUOUS baseline: 18,700-18,801 input, 0 cached, 53-56 output, 8s wall
#                     (jobs 4658 / 4659 — reproducible)
#   KNOWN-EMPTY diff: 17,333 input, 21 output  (job 4651)
#
# So 50,000 input sits ~2.7x above the vacuous ceiling (18,801) and ~8x below the
# genuine floor (398k); 200 output sits ~3.6x above the vacuous ceiling (56) and
# ~25x below the genuine floor (5,067). Wall time is deliberately NOT asserted
# (host-dependent — #2642).
#
# Tier 2 is CORROBORATING and can ONLY fail closed: it can never manufacture a
# PASS, never relax a tier-1 FAIL, and its unavailability is stamped visibly
# rather than skipped silently. Recalibrating means editing these two constants
# and this evidence block together.
ROBOREV_VACUITY_MIN_INPUT_TOKENS=50000
ROBOREV_VACUITY_MIN_OUTPUT_TOKENS=200

# Code-free (non-code) census classification. A census consisting ENTIRELY of
# these extensions/prefixes is structurally discarded by roborev (trigger 3), so
# the tier-1 failure is attributed to the code-free-diff condition specifically.
CODE_FREE_EXTENSIONS="md markdown mdx txt rst adoc"
CODE_FREE_PREFIXES="openspec/ docs/ website/ .github/ .claude/"

PROGNAME=$(basename "$0")

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

Non-sanctioned forms this wrapper replaces: bare 'roborev review --branch'
(resolves against the ROOT checkout from a worktree) and the two-positional
commit-range form (observed enqueueing a commit that is neither endpoint).
A docs-only diff cannot be roborev-certified at all — record primary-source
verification in the PR instead of "roborev clean".

LIVE WORKTREE PROBE (documented, NOT gate-run: needs network + a live reviewer).
Only this probe can show the REAL binary honours the explicit --repo from inside
a worktree; the gate's hermetic check uses a stub reviewer.
  1. Confirm the ROOT checkout sits on main (what makes the trigger reproducible):
       git -C <root> rev-parse --abbrev-ref HEAD    # => main
  2. From a real issue worktree on its own branch, with its commit PUSHED:
       cd /path/to/cqlite-wt/issue-<N>
       $PROGNAME --agent codex --model gpt-5.6-sol
  3. In the emitted block assert: head-sha == the worktree branch HEAD;
     reviewed-sha == head-sha (prefix match); reviewed-sha != git rev-parse
     origin/main; sha-assert: PASS; census matching
     'git diff --numstat origin/main...HEAD'; tokens above both thresholds;
     RESULT: PASS (exit 0). A reviewed-sha equal to origin/main means the
     explicit-repo invocation did NOT defeat the root-checkout resolution.
  4. Record the observed head-sha/reviewed-sha/job/census/tokens in the PR body,
     and re-run the probe after any roborev version bump.
EOF
}

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

BRANCH=$(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null || printf 'HEAD')
HEAD_SHA=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || printf '')

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
CENSUS="-"
TOKENS="UNAVAILABLE"
PUSH_ASSERT="SKIP"
CENSUS_CHECK="SKIP"
SHA_ASSERT="SKIP"
TIER1="SKIP"
TIER2="SKIP"
# The reviewer process's OWN exit status, under its own greppable key: a caller
# retains only the block and reads it by grepping the per-check keys, so without
# this key a non-zero roborev exit shows up as every check reading PASS beside a
# RESULT: FAIL — the one failure cause a grep-based reader could not attribute.
# SKIP until the process actually runs (a push/census/PATH failure exits earlier).
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
  printf 'census: %s\n' "$CENSUS"
  printf 'tokens: %s\n' "$TOKENS"
  printf 'push-assert: %s\n' "$PUSH_ASSERT"
  printf 'census-check: %s\n' "$CENSUS_CHECK"
  printf 'sha-assert: %s\n' "$SHA_ASSERT"
  printf 'vacuity-tier1: %s\n' "$TIER1"
  printf 'vacuity-tier2: %s\n' "$TIER2"
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

# --- step 2: push assert (AC3) — before the census, so the operator gets the ---
# --- actionable cause ("push your commits") rather than a downstream vacuity ---
#
# THE ORACLE IS THE REMOTE, NOT THE LOCAL MIRROR (#2964 follow-up). The obvious
# implementation — read `refs/remotes/<remote>/<branch>` — is WRONG on this fleet
# and was a 100%-reproducible false FAIL: CQLite clones carry a NARROW fetch
# refspec
#     remote.origin.fetch = +refs/heads/main:refs/remotes/origin/main
# so `refs/remotes/origin/*` only ever holds `origin/main` (+ `origin/HEAD`). A
# remote-tracking ref for a feature branch is NEVER created there, no matter how
# many times the branch is pushed — so "mirror ref absent" says NOTHING about
# whether the branch is pushed. `git ls-remote` asks the REMOTE, which is the
# authoritative answer, exactly as the census asks `git` locally rather than
# believing the reviewer's prose. A local proxy is never authority.
if [ "$BRANCH" = "HEAD" ]; then
  PUSH_ASSERT="FAIL (detached HEAD)"
  DETAILS+=("ERROR: push-assert: $REPO is on a detached HEAD, so there is no branch to assert against. Check out the issue branch. No review was enqueued.")
  finish FAIL 1
fi

# Prefer the branch's CONFIGURED upstream remote; fall back to `origin`.
REMOTE=$(git -C "$REPO" config --get "branch.$BRANCH.remote" 2>/dev/null || printf '')
[ -n "$REMOTE" ] || REMOTE=origin

# FAST PATH (optional, saves a remote round trip): a mirror ref that EXISTS and
# already equals HEAD is proof enough. Its ABSENCE proves nothing — fall through.
MIRROR_SHA=$(git -C "$REPO" rev-parse --verify --quiet "refs/remotes/$REMOTE/$BRANCH" || printf '')
if [ "$MIRROR_SHA" = "$HEAD_SHA" ]; then
  PUSH_ASSERT="PASS"
else
  set +e
  LS_OUT=$(git -C "$REPO" ls-remote --heads "$REMOTE" "$BRANCH" 2>&1)
  LS_RC=$?
  set -e
  if [ "$LS_RC" -ne 0 ]; then
    # NOT "never pushed" — a misattributed cause sends people to fix the wrong
    # thing. `git` and `gh` are SEPARATE credential paths (#2942): an
    # authenticated `gh` with an unwired git fails every remote read here.
    PUSH_ASSERT="FAIL (ls-remote failed: infra/auth)"
    DETAILS+=("ERROR: push-assert: 'git ls-remote --heads $REMOTE $BRANCH' exited $LS_RC, so the remote state is UNKNOWN. This is an INFRA/AUTH condition, NOT evidence that the branch is unpushed — do not 'fix' it by pushing. git and gh are separate credential paths (#2942): check network reachability and git's own credentials ('gh auth setup-git'). git said:")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      DETAILS+=("  $line")
    done <<<"$LS_OUT"
    DETAILS+=("ERROR: push-assert: failing closed on an unknown remote state. No review was enqueued.")
    finish FAIL 1
  fi
  REMOTE_SHA=$(printf '%s\n' "$LS_OUT" | awk -v ref="refs/heads/$BRANCH" '$2 == ref { print $1; exit }')
  if [ -z "$REMOTE_SHA" ]; then
    PUSH_ASSERT="FAIL (branch absent on remote $REMOTE)"
    DETAILS+=("ERROR: push-assert: the remote '$REMOTE' has no branch '$BRANCH' (authoritative: git ls-remote) — this branch has never been pushed. The reviewer can only see what the remote has, so an unpushed branch is itself an empty-diff cause. Push it, then re-run. No review was enqueued.")
    finish FAIL 1
  fi
  if [ "$REMOTE_SHA" != "$HEAD_SHA" ]; then
    PUSH_ASSERT="FAIL (unpushed commits)"
    DETAILS+=("ERROR: push-assert: $REMOTE/$BRANCH is at $REMOTE_SHA (authoritative: git ls-remote) but local HEAD is $HEAD_SHA.")
    unpushed=""
    if git -C "$REPO" cat-file -e "${REMOTE_SHA}^{commit}" 2>/dev/null; then
      unpushed=$(git -C "$REPO" log --oneline "$REMOTE_SHA..HEAD" 2>/dev/null || printf '')
    fi
    if [ -n "$unpushed" ]; then
      DETAILS+=("ERROR: push-assert: unpushed commit(s):")
      while IFS= read -r line; do
        [ -n "$line" ] || continue
        DETAILS+=("  $line")
      done <<<"$unpushed"
    else
      DETAILS+=("ERROR: push-assert: local HEAD is not a descendant of the remote tip (or the remote tip is not present locally) — the branch has diverged; reconcile before reviewing.")
    fi
    DETAILS+=("ERROR: push-assert: push the branch before reviewing. No review was enqueued.")
    finish FAIL 1
  fi
  PUSH_ASSERT="PASS"
fi

# --- step 3: the local diff census — THE ORACLE -------------------------------
# `<base>` (default `origin/main`) IS a local mirror ref, so it can be stale or —
# on a narrow-refspec clone that has never fetched — absent. Fail CLOSED: an
# unresolvable base must never be allowed to produce an empty census, which would
# surface as NOTHING-TO-REVIEW and read as "nothing to look at" rather than "we
# could not tell". No implicit `git fetch` is performed on the caller's behalf.
BASE_SHA=""
if ! BASE_SHA=$(git -C "$REPO" rev-parse --verify --quiet "${BASE}^{commit}"); then
  CENSUS_CHECK="FAIL (base '$BASE' unresolvable)"
  DETAILS+=("ERROR: census: base ref '$BASE' does not resolve to a commit in $REPO, so the census — and therefore every vacuity judgement — would be unfounded. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW: an unresolvable base is 'we cannot tell', never 'there is nothing to review'. If '$BASE' is a remote-tracking ref, this clone may have a narrow fetch refspec or have never fetched it; fetch it yourself (the wrapper never fetches behind your back) and re-run. No review was enqueued.")
  finish FAIL 1
fi

NUMSTAT=$(git -C "$REPO" diff --numstat "${BASE}...HEAD" 2>/dev/null || printf '')
census_files=0
census_added=0
census_deleted=0
census_code_free=1
while IFS=$'\t' read -r add del path; do
  [ -n "${path:-}" ] || continue
  if [ "$add" = "-" ]; then add=0; fi
  if [ "$del" = "-" ]; then del=0; fi
  census_files=$((census_files + 1))
  census_added=$((census_added + add))
  census_deleted=$((census_deleted + del))
  # Code-free classification: the file must match a documented non-code extension
  # OR sit under a documented non-code path prefix. One code file flips the whole
  # census to "has code".
  file_non_code=0
  ext="${path##*.}"
  # shellcheck disable=SC2086 # deliberate split of the space-separated constants
  for candidate in $CODE_FREE_EXTENSIONS; do
    if [ "$ext" = "$candidate" ]; then file_non_code=1; fi
  done
  # shellcheck disable=SC2086 # deliberate split of the space-separated constants
  for prefix in $CODE_FREE_PREFIXES; do
    case "$path" in "$prefix"*) file_non_code=1 ;; esac
  done
  if [ "$file_non_code" -eq 0 ]; then census_code_free=0; fi
done <<<"$NUMSTAT"

CENSUS="$census_files files, +$census_added/-$census_deleted"

if [ "$census_files" -eq 0 ]; then
  CENSUS_CHECK="FAIL (empty census)"
  DETAILS+=("NOTHING-TO-REVIEW: the local diff census for '${BASE}...HEAD' is genuinely empty (0 files changed), so no review was enqueued. This is explicitly NOT a pass and MUST NOT be recorded as \"roborev clean\".")
  finish NOTHING-TO-REVIEW 3
fi
CENSUS_CHECK="PASS"

# --- step 4: invoke by EXPLICIT sha + EXPLICIT absolute repo (AC2) ------------
if ! command -v roborev >/dev/null 2>&1; then
  SHA_ASSERT="FAIL (roborev not on PATH)"
  DETAILS+=("ERROR: 'roborev' is not on PATH, so the review cannot be performed and the census cannot be certified. Failing closed rather than reporting a pass.")
  finish FAIL 1
fi

# NEVER a bare `--branch` (trigger 1); NEVER two positional commits (trigger 2).
# The transcript goes to the log; stdout stays reserved for the summary block.
set +e
roborev review "$HEAD_SHA" \
  --repo "$REPO" \
  --agent "$AGENT" \
  --model "$MODEL" \
  --wait >"$LOG" 2>&1
REVIEW_RC=$?
set -e
if [ "$REVIEW_RC" -eq 0 ]; then
  ROBOREV_EXIT="PASS"
else
  ROBOREV_EXIT="FAIL (exit $REVIEW_RC)"
fi

# --- step 5: reviewed-SHA assert (AC2) ----------------------------------------
ANNOUNCE=$(grep -oiE 'enqueued job [0-9]+ for [0-9a-fA-F]{4,40}' "$LOG" | tail -1 || printf '')
if [ -z "$ANNOUNCE" ]; then
  SHA_ASSERT="FAIL (no parseable enqueue announcement)"
  DETAILS+=("ERROR: sha-assert: the transcript contains no parseable 'Enqueued job <N> for <sha>' line, so the reviewed sha is UNVERIFIABLE. That is a failure, never a skipped check. Transcript: $LOG")
else
  JOB=$(printf '%s' "$ANNOUNCE" | sed -E 's/^[Ee]nqueued [Jj]ob ([0-9]+).*/\1/')
  REVIEWED_SHA=$(printf '%s' "$ANNOUNCE" | sed -E 's/.*[Ff]or ([0-9a-fA-F]+)$/\1/' | tr 'A-F' 'a-f')
  if [ "${HEAD_SHA:0:${#REVIEWED_SHA}}" = "$REVIEWED_SHA" ] \
    || [ "${REVIEWED_SHA:0:${#HEAD_SHA}}" = "$HEAD_SHA" ]; then
    SHA_ASSERT="PASS"
  else
    SHA_ASSERT="FAIL (reviewed-sha does not match head-sha)"
    DETAILS+=("ERROR: sha-assert: announced reviewed-sha '$REVIEWED_SHA' does not prefix-match branch HEAD '$HEAD_SHA' (job $JOB).")
    if [ "${BASE_SHA:0:${#REVIEWED_SHA}}" = "$REVIEWED_SHA" ]; then
      DETAILS+=("ERROR: sha-assert: the announced sha EQUALS the base ref '$BASE' ($BASE_SHA) — NO branch change was reviewed. That equality is the signature of the worktree bare-'--branch' resolution trigger, where the review resolves against the ROOT checkout instead of this worktree.")
    else
      DETAILS+=("ERROR: sha-assert: the announced sha matches NEITHER endpoint (head '$HEAD_SHA', base '$BASE' $BASE_SHA) — the signature of the non-sanctioned two-positional commit-range form.")
    fi
  fi
fi

# --- step 6 tier 1: PRIMARY, deterministic, threshold-free (AC1) --------------
# The census is known NON-empty here (an empty one exited above), so a reviewer
# claiming there are no code changes contradicts a locally measured fact.
if grep -qiE 'contains no code changes to review|no code changes' "$LOG"; then
  TIER1="FAIL (vacuous verdict vs non-empty census)"
  DETAILS+=("ERROR: vacuity-tier1: the review output claims there are NO CODE CHANGES to review, but the locally computed census is NON-EMPTY: $CENSUS (${BASE}...HEAD). The reviewer's claim contradicts a fact we measured ourselves, so the change was demonstrably NOT reviewed and this run is NOT reportable as \"roborev clean\".")
  if [ "$census_code_free" -eq 1 ]; then
    DETAILS+=("ERROR: vacuity-tier1: every file in the census is documentation/specification/workflow text, so this is the CODE-FREE-DIFF condition: a code-free diff cannot be certified by roborev at all (roborev structurally discards it). The sanctioned substitute is primary-source verification recorded in the PR (for example 'git show cassandra-5.0.8:<path>' for the source the docs describe) — never \"roborev clean\".")
  fi
else
  TIER1="PASS"
fi

# --- step 6 tier 2: CORROBORATING token accounting; can only FAIL CLOSED ------
json_int() { # json_int <json> <key> -> integer or empty
  local v=""
  v=$(printf '%s' "$1" | tr -d ' \t\n' | grep -oE "\"$2\":-?[0-9]+" | head -1 | sed -E 's/.*://') || v=""
  printf '%s' "$v"
}

has_token_fields() { # has_token_fields <json>
  [ -n "$1" ] || return 1
  printf '%s' "$1" | tr -d ' \t\n' | grep -q '"input_tokens":'
}

select_job_object() { # select_job_object <list-json> <job-id> -> object json or empty
  command -v python3 >/dev/null 2>&1 || { printf ''; return 0; }
  printf '%s' "$1" | python3 -c '
import json, sys
want = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
def walk(node):
    if isinstance(node, dict):
        for key in ("id", "job_id", "job"):
            if key in node and str(node[key]) == want:
                print(json.dumps(node))
                return True
        for value in node.values():
            if walk(value):
                return True
    elif isinstance(node, list):
        for value in node:
            if walk(value):
                return True
    return False
walk(data)
' "$2" 2>/dev/null || printf ''
}

TOKEN_JSON=""
if [ "$JOB" != "-" ]; then
  TOKEN_JSON=$(roborev show "$JOB" --json 2>/dev/null || printf '')
  if ! has_token_fields "$TOKEN_JSON"; then
    LIST_JSON=$(roborev list --json --limit 50 --repo "$REPO" 2>/dev/null || printf '')
    TOKEN_JSON=$(select_job_object "$LIST_JSON" "$JOB")
  fi
fi

TOK_IN=""
TOK_CACHED=""
TOK_OUT=""
if has_token_fields "$TOKEN_JSON" \
  && ! printf '%s' "$TOKEN_JSON" | tr -d ' \t\n' | grep -q '"has_token_data":false'; then
  TOK_IN=$(json_int "$TOKEN_JSON" input_tokens)
  TOK_CACHED=$(json_int "$TOKEN_JSON" cached_input_tokens)
  TOK_OUT=$(json_int "$TOKEN_JSON" output_tokens)
fi

if [ -z "$TOK_IN" ] || [ -z "$TOK_CACHED" ] || [ -z "$TOK_OUT" ]; then
  TOKENS="UNAVAILABLE"
  TIER2="UNAVAILABLE"
  DETAILS+=("NOTICE: vacuity-tier2: UNAVAILABLE — token accounting for job '$JOB' could not be obtained from the installed roborev build (tried 'roborev show <job> --json', then 'roborev list --json'). This is a DEGRADED-SIGNAL notice, never a silent skip: tier 1 still governs, and an unavailable tier 2 can never turn a FAIL into a PASS.")
else
  TOKENS="input=$TOK_IN cached=$TOK_CACHED output=$TOK_OUT"
  tier2_trips=()
  if [ "$TOK_IN" -lt "$ROBOREV_VACUITY_MIN_INPUT_TOKENS" ]; then
    tier2_trips+=("observed input=$TOK_IN < ROBOREV_VACUITY_MIN_INPUT_TOKENS=$ROBOREV_VACUITY_MIN_INPUT_TOKENS")
  fi
  if [ "$TOK_CACHED" -eq 0 ]; then
    tier2_trips+=("observed cached=$TOK_CACHED == 0 (a genuine review reports a non-zero cached-input count; the recorded vacuous baseline reports exactly 0)")
  fi
  if [ "$TOK_OUT" -lt "$ROBOREV_VACUITY_MIN_OUTPUT_TOKENS" ]; then
    tier2_trips+=("observed output=$TOK_OUT < ROBOREV_VACUITY_MIN_OUTPUT_TOKENS=$ROBOREV_VACUITY_MIN_OUTPUT_TOKENS")
  fi
  if [ "${#tier2_trips[@]}" -gt 0 ]; then
    TIER2="FAIL (vacuous token signature)"
    DETAILS+=("ERROR: vacuity-tier2: the token accounting for job '$JOB' carries the vacuous signature against a NON-EMPTY census ($CENSUS):")
    for trip in "${tier2_trips[@]}"; do
      DETAILS+=("  $trip")
    done
  else
    TIER2="PASS"
  fi
fi

# --- step 7: verdict ----------------------------------------------------------
if [ "$REVIEW_RC" -ne 0 ]; then
  DETAILS+=("ERROR: 'roborev review' exited $REVIEW_RC. A non-zero reviewer exit is a fail-closed FAIL — read the transcript at $LOG before treating anything as clean.")
fi

# `roborev-exit` participates in the SAME per-check scan as every other key, so a
# non-zero reviewer exit forces RESULT: FAIL through the documented path rather
# than a special case bolted on beside it.
failed=0
for verdict in "$PUSH_ASSERT" "$CENSUS_CHECK" "$SHA_ASSERT" "$TIER1" "$TIER2" "$ROBOREV_EXIT"; do
  case "$verdict" in FAIL*) failed=1 ;; esac
done

if [ "$failed" -eq 0 ]; then
  finish PASS 0
fi
finish FAIL 1
