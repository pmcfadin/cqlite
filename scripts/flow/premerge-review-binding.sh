#!/usr/bin/env bash
#
# premerge-review-binding.sh — the REVIEW/head binding and the HOLD re-read
# (issue #3752), invoked by scripts/flow/premerge-assert.sh.
#
# ---------------------------------------------------------------------------
# WHAT THIS EXISTS FOR
# ---------------------------------------------------------------------------
# `premerge-assert.sh` binds the merge to the GATE of record at the certified
# head. Nothing bound it to the ROBOREV round. A rebase rewrites the reviewed
# commit, so a PR can truthfully record "roborev: PASS" about a commit that no
# longer exists on the branch being merged.
#
# MEASURED INSTANCE (#3752): PR #3735 held a genuine roborev PASS — job 304 at
# `d3812f59`, `findings: NONE`, 1,069,882 input tokens. The lane then rebased,
# correctly, under a coordination order. After the rebase `git cat-file -t
# d3812f59` reported the object does not exist, and TWO unreviewed commits sat
# after the reviewed content — one of them a semantic rebase-conflict fix in the
# one file that overlapped `main`, i.e. the single most review-worthy commit on
# the branch. Every word of the recorded PASS was true, of a commit that is not
# the one being merged.
#
# ---------------------------------------------------------------------------
# THE LOAD-BEARING TEST IS `merge-base --is-ancestor`, AND IT MUST RUN FIRST
# ---------------------------------------------------------------------------
# This is the `lane-3552` correction on #3752, and it is not a style point. A
# rebase leaves the OLD commit dangling and REFLOG-REACHABLE, so:
#
#     git cat-file -t 344a5ab3e                    -> commit   <-- still valid
#     git merge-base --is-ancestor 344a5ab3e HEAD  -> FAILS    <-- the only arm
#                                                                  that fires
#
# An object-validity-first check therefore gives a REASSURING answer that never
# fires, and a reader stops there. `cat-file -t` appears below ONLY as a
# diagnostic explaining WHY a non-ancestor is a non-ancestor; it is never the
# verdict. That is the same defect class as the thing this guard guards: a
# two-valued probe collapsing `cannot tell` onto the permissive answer.
#
# ---------------------------------------------------------------------------
# OUTPUT CONTRACT (the `base-staleness.sh` anchor, #3650 D2, reused verbatim)
# ---------------------------------------------------------------------------
#   (a) EVERY line, stdout and stderr, begins with the leg's prefix. Nothing
#       here is pasteable or greppable as a certification.
#   (b) EVERY dynamic field goes through `sane`. Git permits newlines in paths
#       and GitHub permits them in comment bodies; unsanitized, one of those
#       emits a SECOND line carrying no prefix, breaking the anchor everything
#       rests on.
#   (c) The verdict appears ONLY on a `verdict ` line, carrying a token from a
#       CLOSED set. Prose goes on `verdict-detail` lines, so the token position
#       can never hold a word. The token is for the READER: the EXIT CODE is the
#       decision, and `premerge-assert.sh` decides on it alone. That is
#       deliberate and is `base-staleness.sh`'s consumer rule — a consumer must
#       treat an UNMEASURED exit as refusing, which is a property of the code,
#       not of a word it matched. Do not couple a caller to this token: an
#       earlier draft of this clause claimed a caller matched it token-exactly
#       and none ever did, which is a comment inviting reliance it cannot
#       support.
#   (d) This file's own STATIC TEMPLATE TEXT carries none of `PASS`, `OK`,
#       `RESULT:` — asserted structurally by
#       scripts/tests/test_premerge_review_binding.sh.
#
#   DECLARED RESIDUAL, the same one base-staleness declares: a repository path
#   or a GitHub login CAN contain a reserved substring and this script prints it
#   verbatim (control characters excepted). The ANCHOR is what makes that
#   harmless — masking the value would mangle it for the reader, and #3312's
#   ruling is to anchor or remove the channel, never to pick a rarer delimiter.
#
# ---------------------------------------------------------------------------
# SUBCOMMANDS AND EXIT CODES
# ---------------------------------------------------------------------------
#   premerge-review-binding.sh review-binding <pr> <repo> <certified-sha-40hex>
#   premerge-review-binding.sh hold-check     <pr> <repo>
#
#   0   review-binding: BOUND or NOT-APPLICABLE      hold-check: NO-HOLD-RECOGNISED
#   4   review-binding: UNBOUND                      hold-check: HOLD-FOUND
#   5   either leg: UNMEASURED
#   3   usage
#
# *** A CONSUMER MUST TREAT 5 / UNMEASURED AS A REFUSAL, NEVER AS A BINDING. ***
# A positive verdict requires a positive measurement. Where the sole oracle
# could not be consulted the verdict is non-passing and its text names what was
# unverifiable; a permissive branch is keyed on the AFFIRMATIVE value, never on
# `!= <bad>`.
#
# ---------------------------------------------------------------------------
# WHAT THIS LEG DOES **NOT** DO — declared, not implied
# ---------------------------------------------------------------------------
#   1. It does NOT derive anything from the recorded BLOCK's terminal verdict.
#      That value is still REPORTED on a `recorded-verdict` line and nothing is
#      derived from it, because the block is attacker- and accident-controlled
#      text. WHAT IS ENFORCED, since roborev job 59 finding 1, is the JOB
#      RECORD's own structured verdict: a job binds only when its record says
#      `clean`, or says `findings` AND an allowlisted human authorized deferring
#      them for that exact base/head/job. An unreadable record verdict never
#      binds. The earlier version of this leg treated a `git_ref` match ALONE as
#      sufficient and declared the verdict a residual — a false-green route in a
#      merge gate, since a block naming a FAILED or in-progress job whose range
#      matched bound the merge.
#      A deferral's `issues=` half IS re-verified here, by CALLING the one
#      shared four-valued oracle (`roborev_issue_retrievability`): only an OPEN
#      issue GitHub confirms may grant, because `gh issue view` exits 0 for a
#      CLOSED issue and a deferral naming an issue closed weeks ago would
#      otherwise bind with the finding permanently untracked.
#      STILL NOT RE-VERIFIED HERE, and declared rather than implied: the
#      deferral marker's `count=` half, which is matched against the findings
#      count OBSERVED BY THE REVIEW. This leg never ran the review and the
#      record carries no count, so there is nothing here to compare against;
#      that half is enforced at review time, by the wrapper. It is the ONLY
#      unverified half, and the emitted block says so.
#   2. It does NOT model roborev's exclusion set, and it does not re-derive the
#      wrapper's own asserts. It answers ONE question: is the commit a recorded
#      review actually covered an ancestor of the tree about to merge, with no
#      reviewable code added after it.
#   3. A HOSTILE INVOKER IS OUT OF THE THREAT MODEL. Whoever runs this can edit
#      it, shadow `gh`/`roborev` on PATH, or skip the assert. What it closes is
#      the NON-INVOKER and the ACCIDENTAL route — a diligent lane with no step
#      in its path telling it the review it recorded is about a commit the
#      rebase deleted.
set -uo pipefail

# ---------------------------------------------------------------------------
# THE DISARM WINDOW — a NAMED COMMITTED CONSTANT with NO env override (#3312).
# ---------------------------------------------------------------------------
# An override is settable by the party it constrains, and "how long does a lead
# disarm stop me for" is exactly what a lane wanting to merge would widen to
# zero. Changing it is a committed diff a reviewer reads.
PREMERGE_DISARM_WINDOW_SECS=1800

# Resolved from THIS script's own directory, with no env override and no
# `${...:-...}` fallback (#3312's second rule: the constrained party must not
# choose its own enforcer). A test needing different behaviour SUBSTITUTES THE
# ARTIFACT in a scratch copy of the tree — never a path variable, which would be
# one more seam a real invoker could set.
OWN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCAN_TOOL="$OWN_DIR/premerge-pr-scan.py"
FACTS_TOOL="$OWN_DIR/roborev-job-facts.py"
CLASSIFY_TOOL="$OWN_DIR/../ci/classify-docs-only.sh"
# The deferral-AUTHORIZATION enforcer and the allowlist it applies. Same rule as
# above: resolved from this script's own directory, no env override, no
# `${...:-...}` fallback. The allowlist is read from the ONE committed
# definition the wrapper uses (`roborev-review-oracles.sh`) rather than restated
# here — two spellings of "who may authorize" is two places for it to drift, and
# a drift there is an authorization bypass.
WAIVER_SCAN_TOOL="$OWN_DIR/roborev-waiver-scan.py"
ORACLES_FILE="$OWN_DIR/roborev-review-oracles.sh"

P=''

# sane <string> — every C0 control character and DEL replaced by a VISIBLE
# escape. The load-bearing half of the anchor (see (b) above). Control
# characters ONLY: the value is otherwise verbatim.
sane() {
  local s="$1" out c i n
  s="${s//$'\r'/'\r'}"
  s="${s//$'\n'/'\n'}"
  s="${s//$'\t'/'\t'}"
  case "$s" in
    *[[:cntrl:]]*) ;;
    *)
      printf '%s' "$s"
      return 0
      ;;
  esac
  out=""
  n=${#s}
  i=0
  while [ "$i" -lt "$n" ]; do
    c="${s:i:1}"
    case "$c" in
      [[:cntrl:]]) out=$(printf '%s\\x%02x' "$out" "'$c") ;;
      *) out="$out$c" ;;
    esac
    i=$((i + 1))
  done
  printf '%s' "$out"
}

say()    { printf '%s %s\n' "$P" "$1"; }
detail() { printf '%s verdict-detail %s\n' "$P" "$(sane "$1")"; }

# verdict <token> — the ONE emitter. The token stands alone on its own line so
# the token position can never hold a word.
verdict() { printf '%s verdict %s\n' "$P" "$1"; }

usage() {
  printf 'PREMERGE: REVIEW-BINDING USAGE — the call is wrong (not a verdict)\n' >&2
  printf 'PREMERGE: REVIEW-BINDING USAGE usage: %s review-binding <pr> <repo> <certified-sha>\n' \
    "$(sane "${0##*/}")" >&2
  printf 'PREMERGE: REVIEW-BINDING USAGE        %s hold-check <pr> <repo>\n' \
    "$(sane "${0##*/}")" >&2
  exit 3
}

# unmeasured <cause...> — exit 5. Prints NO affirmative token, so it can never
# be misread as a binding.
unmeasured() {
  while [ "$#" -gt 0 ]; do
    printf '%s unmeasured-cause %s\n' "$P" "$(sane "$1")"
    shift
  done
  verdict UNMEASURED
  detail "the check could not be performed. A CONSUMER MUST TREAT THIS AS A REFUSAL,"
  detail "never as a binding (#3752); this is not a certification."
  exit 5
}

need_tool() {
  command -v "$1" >/dev/null 2>&1 ||
    unmeasured "the required tool \`$1\` is not on PATH, so the sole oracle for this leg" \
      "could not be consulted. Fix the box and re-run this assert."
}

need_file() {
  [ -f "$1" ] ||
    unmeasured "the required artifact $1 is absent beside this script. It is resolved from" \
      "this script's OWN directory with no override (#3312), so an absent artifact is a" \
      "broken checkout, never a reason to skip the check."
}

# ---------------------------------------------------------------------------
# THE ONE-COMMAND SELF-CHECK (#3752 AC4) — printed with the REAL shas, in the
# CORRECTED order: the ancestor test FIRST, the classify pipe second.
# ---------------------------------------------------------------------------
print_self_check() {
  say "self-check run this by hand to reproduce the verdict above (ancestor test FIRST —"
  say "self-check \`cat-file -t\` is a diagnostic, never the verdict; a rebased commit is"
  say "self-check still reflog-reachable and answers \`commit\`):"
  say "self-check   git merge-base --is-ancestor $(sane "$1") $(sane "$2") \\"
  say "self-check     && git diff --name-only $(sane "$1")..$(sane "$2") \\"
  say "self-check          | bash scripts/ci/classify-docs-only.sh"
}

# classify_paths <from> <to> — 0 when the range adds no reviewable code, 1 when
# it does, 2 when the measurement itself failed.
#
# `diff.renames`/`diff.relative` are PINNED OFF (#3650's rename-symmetry rule).
# Rename detection reports a rename's DESTINATION ONLY, so a rename would hide
# the OLD path from the classifier; and `diff.relative` is INVOKER-controlled —
# set, and a run from a subdirectory strips the prefix, making the answer a
# function of cwd. Both directions here must be conservative, and more paths can
# only push the classifier toward its fail-closed answer.
classify_paths() {
  local paths
  paths=$(git -c diff.renames=false -c diff.relative=false \
    diff --name-only "$1..$2" 2>/dev/null) || return 2
  # FED BY REDIRECTION, NOT A PIPE (#3752, lane-3752 audit). This script runs
  # under `set -o pipefail` and the classifier BREAKS out of its read loop on
  # the first non-docs path, so on a path list larger than the pipe buffer the
  # producer takes SIGPIPE and the PIPELINE reports 141 — turning a perfectly
  # good verdict 1 (`carries code`) into the `*)` arm, i.e. UNMEASURED. That is
  # this leg's own three-valued rule violated by the plumbing: "the consumer
  # decided early" is not "the measurement failed". A here-string has no such
  # race, and the byte stream is identical (both append one newline).
  bash "$CLASSIFY_TOOL" >/dev/null 2>&1 <<<"$paths"
  case "$?" in
    0) return 0 ;;
    1) return 1 ;;
    *) return 2 ;;
  esac
}

# ---------------------------------------------------------------------------
# THE COMPLETE COMMENT THREAD — `--paginate`, AND EVERY PAGE DECODED (#3752,
# roborev job 59 finding 2)
# ---------------------------------------------------------------------------
# `gh pr view --json comments` and `gh issue view --json comments` return a
# BOUNDED connection, not the thread: a persistent column-zero `HOLD:` outside
# the returned window produced a false `NO-HOLD-RECOGNISED`. That is the same
# defect already fixed for the disarm TIMELINE and it was still live for the
# COMMENTS — the artifact a lead actually posts a stop order in.
#
# ONE STREAM SERVES BOTH LEGS. The job-record discovery and the hold scan read
# the SAME normalised payload, so they can never disagree about what the thread
# contains; paginating one and leaving the other bounded would put a job block
# and a stop order on different views of one page range.
#
# THE REST/GraphQL SPELLING DIFFERENCE IS RECONCILED IN `normalize`, NOT HERE:
# `gh api` says `user.login`/`created_at`, `gh pr view --json` says
# `author.login`/`createdAt`, and a consumer reading the wrong one sees every
# author as empty — which silently stops granting deferrals and stops honouring
# an allowlisted release. Fail-closed, and wrong on correct input, so it is
# fixed once at this boundary rather than per consumer.
#
# fetch_thread <out.json> <view-json-fields> <endpoint> <what> — writes a
# normalised payload, or refuses with UNMEASURED. An incompletely-read thread is
# a hold, never a clearance.
fetch_thread() {
  local out="$1" fields="$2" endpoint="$3" what="$4" tmpdir
  tmpdir=$(dirname "$out")
  gh "${GH_VIEW_ARGV[@]}" --json "$fields" >"$out.view" 2>/dev/null ||
    unmeasured "\`gh $what\` failed (auth/network/no-such-subject), so $what could not be read."
  gh api --paginate "$endpoint?per_page=100" >"$out.comments" 2>/dev/null ||
    unmeasured "the comment thread for $what could not be read IN FULL (pagination failed), so" \
      "a stop order or a recorded review outside the first page could not be ruled out." \
      "An incompletely-read thread is a hold, never a clearance."
  python3 "$SCAN_TOOL" normalize "$out.view" "$out.comments" "$out" >/dev/null 2>&1 ||
    unmeasured "the comment thread for $what could not be normalised into one payload, so its" \
      "shape is not one this code recognises. A shape we cannot read is a refusal, never a" \
      "shorter thread — a short comment list is indistinguishable from a quiet one."
}

# ---------------------------------------------------------------------------
# WHO MAY AUTHORIZE A DEFERRAL — read as DATA from the ONE committed definition
# ---------------------------------------------------------------------------
# The allowlist has exactly one home, `roborev-review-oracles.sh`, and it is
# read here as TEXT and never EXECUTED — the same idiom the agent gate uses for
# its `COMPONENTS` baseline, and for the same reason: sourcing a 1600-line file
# to obtain one string imports every function it defines, and executing a file
# to learn a value is a control channel where a data read will do.
#
# It REFUSES LOUDLY on any shape it does not recognise. A parser that guesses
# here would guess about who may authorize a merge, so an unrecognised
# declaration is a named refusal and never an empty allowlist — an empty
# allowlist would silently make every deferral `unauthorized`, which reads as a
# correct refusal while actually meaning the check never ran.
waiver_authors() {
  local line value
  [ -f "$ORACLES_FILE" ] || return 1
  # Column-zero anchored, exactly one line, the committed form.
  line=$(sed -n 's/^ROBOREV_WAIVER_AUTHORS="\([^"]*\)"$/\1/p' "$ORACLES_FILE") || return 1
  # EXACTLY ONE declaration must have matched. Command substitution strips
  # TRAILING newlines, so a single match carries none and two matches carry one
  # between them — an embedded newline therefore means the file holds more than
  # one declaration, and concatenating them would invent an allowlist neither
  # line states.
  #
  # THE PATTERN USES BASH ANSI-C QUOTING, NOT A COMMAND SUBSTITUTION. Written as
  # a substitution around printf, the newline being searched for is stripped by
  # the substitution itself, collapsing the pattern to an empty string that
  # matches EVERY value. That made this function return 1 unconditionally and
  # the authorized-deferral path UNREACHABLE — fail-closed, but a guard that
  # reds on correct input is the guard agents learn to waive, and it surfaced
  # only because a behavioural case demanded the grant.
  case "$line" in
    *$'\n'*) return 1 ;;
  esac
  value="$line"
  [ -n "$value" ] || return 1
  printf '%s' "$value"
}

# record_verdict_class <letter> — the record verdict, THREE-VALUED.
#
# `clean` and `findings` are the two AFFIRMATIVE measurements roborev records
# (`roborev show --json` synthesises the letter from `reviews.verdict_bool`: `P`
# clean, `F` findings). EVERYTHING ELSE — absent, empty, or a letter this code
# has never judged — is `unknown`, and `unknown` NEVER binds. A positive verdict
# requires a positive measurement; inferring `clean` from "no findings signal"
# is deriving a pass from the absence of a bad signal.
record_verdict_class() {
  case "$1" in
    P | p) printf 'clean' ;;
    F | f) printf 'findings' ;;
    *) printf 'unknown' ;;
  esac
}

# deferral_authorized <job> <base> <head> <tmp> — 0 when an allowlisted human
# authorized deferring THIS job's findings, with DEFERRAL_AUTHOR set.
#
# WHY THIS PATH EXISTS AT ALL: roborev RE-REPORTS a lead-deferred finding on
# every later round, so a job record's verdict stays `F` forever once findings
# were found and deferred (#3626). Requiring `clean` with no deferral route
# would make such a merge UNOBTAINABLE — the defect #3626 exists to record, and
# a rule that punishes the correct behaviour will not survive contact.
#
# THE AUTHORIZATION IS RE-VERIFIED HERE, NOT READ OFF THE PR BLOCK. The block is
# the untrusted artifact this whole finding is about; deciding from its
# `findings: DEFERRED` text would be circular. So the SAME scanner the wrapper
# uses is called, on the SAME marker, under all the same channel rules.
#
# THE `issues=` HALF *IS* RE-VERIFIED HERE, AND THE ORACLE IS CALLED, NOT COPIED.
# `roborev_issue_retrievability` (in roborev-review-oracles.sh) is the ONE
# implementation, asked FOUR-VALUED: only a payload affirmatively naming the
# number AND an OPEN state is `present` and may grant; `closed`, `absent` and
# `unverifiable` are textually distinct and NONE of them grants. A second copy
# of an authorization rule is a second place for it to diverge, and a divergence
# there is an authorization bypass (#3626), so it is sourced in a SUBSHELL —
# which also stops the oracles file shadowing this leg's own globals.
#
# WHY IT MATTERS AT MERGE TIME rather than only at review time: `issues=` is what
# records that a deferred finding is TRACKED. `gh issue view` EXITS 0 FOR A CLOSED
# ISSUE, so without the state half a deferral could name an issue closed as a
# duplicate weeks ago and still bind — the finding permanently untracked while
# the block asserted it was filed. An allowlisted human deferring against a
# since-closed issue is an ACCIDENT route, and by #3312's triage rule an accident
# route is a defect, not an out-of-model invoker bypass.
#
# WHAT IS STILL NOT RE-VERIFIED, DECLARED RATHER THAN IMPLIED: the `count=` half. It
# is matched against the findings count OBSERVED BY THE REVIEW, and this leg
# never ran the review — the job record carries a verdict LETTER and no count.
# So the scanner is called in its `findings-deferral-authorization` mode, which
# judges marker form, sole-content, top-level placement, structured authorship,
# the allowlist and the base/head/job scope, and returns the DISTINCT state
# `granted-authorization`. Fabricating a count to satisfy the check would be an
# affirmative assert over an unmeasured value; comparing the marker's count with
# itself would be a tautology. The count is enforced at REVIEW time, where the
# measurement exists.
# issue_state_of <issue> <repo-slug> — prints one of the oracle's four states.
# Sourced in a SUBSHELL: the oracles file is pure definitions, but it also
# assigns globals (REPO, CODE_FREE_*, ROBOREV_*) and this leg has its own `P`,
# `causes` and friends — a subshell makes shadowing structurally impossible
# instead of merely unlikely. A failure to source, or any missing output, is
# `unverifiable`: a could-not-ask is NEVER read as verified.
issue_state_of() {
  local issue="$1" slug="$2" state
  state=$(
    REPO=$(git rev-parse --show-toplevel 2>/dev/null) || REPO="$PWD"
    export REPO
    # shellcheck source=/dev/null
    . "$ORACLES_FILE" >/dev/null 2>&1 || exit 0
    command -v roborev_issue_retrievability >/dev/null 2>&1 || exit 0
    roborev_issue_retrievability "$issue" "$slug" >/dev/null 2>&1 || exit 0
    printf '%s' "${ROBOREV_ISSUE_STATE:-}"
  )
  case "$state" in
    present | closed | absent | unverifiable) printf '%s' "$state" ;;
    *) printf 'unverifiable' ;;
  esac
}

DEFERRAL_AUTHOR=""
DEFERRAL_ISSUE_REFUSAL=""
deferral_authorized() {
  local job="$1" base="$2" head="$3" tmp="$4" repo_slug="$5" allow result state
  DEFERRAL_AUTHOR=""
  [ -f "$WAIVER_SCAN_TOOL" ] || return 1
  allow=$(waiver_authors) || return 1
  [ -n "$allow" ] || return 1
  result=$(python3 "$WAIVER_SCAN_TOOL" findings-deferral-authorization \
    "$base" "$head" "$job" "$allow" <"$tmp/pr.json" 2>/dev/null) || return 1
  state=$(printf '%s\n' "$result" | sed -n 's/^state=//p' | head -1)
  # KEYED ON THE AFFIRMATIVE VALUE, never on `!= <bad>`: a state this code has
  # never judged is not a grant.
  [ "$state" = "granted-authorization" ] || return 1
  DEFERRAL_AUTHOR=$(printf '%s\n' "$result" | sed -n 's/^author=//p' | head -1)

  # ===== EVERY DECLARED ISSUE MUST BE AN OPEN ISSUE GITHUB CONFIRMS =====
  # The backstop COUNTS VERIFICATIONS PERFORMED rather than testing the string,
  # because `issues=","` is non-empty, splits into ZERO words, and would leave a
  # grant standing with not one issue checked — #3626's own lesson, which is
  # exactly the upstream dependency a backstop must not have.
  local issues verified=0 declared=0 num rest st
  issues=$(printf '%s\n' "$result" | sed -n 's/^issues=//p' | head -1)
  if [ -z "$issues" ]; then
    DEFERRAL_ISSUE_REFUSAL="the deferral names NO tracking issue, so nothing records where the findings went"
    return 1
  fi
  rest="$issues"
  while [ -n "$rest" ]; do
    num="${rest%%,*}"
    if [ "$num" = "$rest" ]; then rest=""; else rest="${rest#*,}"; fi
    declared=$((declared + 1))
    case "$num" in
      "" | *[!0-9]*)
        DEFERRAL_ISSUE_REFUSAL="the deferral declares '$(sane "$num")', which is not an issue number"
        return 1
        ;;
    esac
    st=$(issue_state_of "$num" "$repo_slug")
    verified=$((verified + 1))
    case "$st" in
      present) : ;;
      closed)
        DEFERRAL_ISSUE_REFUSAL="ISSUE-CLOSED — GitHub answered that issue #$(sane "$num") is CLOSED, so it does not track a deferred finding"
        return 1
        ;;
      absent)
        DEFERRAL_ISSUE_REFUSAL="ISSUE-ABSENT — GitHub answered that issue #$(sane "$num") DOES NOT EXIST in this repository"
        return 1
        ;;
      *)
        DEFERRAL_ISSUE_REFUSAL="ISSUE-UNVERIFIABLE — whether issue #$(sane "$num") exists and is OPEN could NOT BE ASKED, and a could-not-ask is never read as verified"
        return 1
        ;;
    esac
  done
  # The count of verifications PERFORMED must equal the count DECLARED.
  if [ "$verified" -ne "$declared" ] || [ "$declared" -eq 0 ]; then
    DEFERRAL_ISSUE_REFUSAL="the declared issue list yielded $verified verification(s) for $declared field(s), so it was not fully checked"
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# review-binding
# ---------------------------------------------------------------------------
cmd_review_binding() {
  local pr="$1" repo="$2" certified="$3"
  P='PREMERGE: REVIEW-BINDING'

  case "$certified" in *[!0-9a-f]* | "") usage ;; esac
  [ "${#certified}" -eq 40 ] || usage

  need_tool git
  need_tool gh
  need_tool python3
  need_tool roborev
  need_file "$SCAN_TOOL"
  need_file "$FACTS_TOOL"
  need_file "$CLASSIFY_TOOL"

  # GLOBAL, deliberately: the EXIT trap below fires as the shell unwinds, by
  # which point a `local` may be out of scope and the trap would expand to
  # `rm -rf ""` — a silent non-removal that leaks the directory on every
  # refusal path. Validated BEFORE the trap is installed, so the trap can never
  # run against an empty value either.
  TMPD=$(mktemp -d "${TMPDIR:-/tmp}/premerge-review-binding.XXXXXX" 2>/dev/null) || TMPD=""
  [ -n "$TMPD" ] && [ -d "$TMPD" ] ||
    unmeasured "could not create a scratch directory under ${TMPDIR:-/tmp}."
  trap 'rm -rf "$TMPD"' EXIT
  local tmp="$TMPD"

  git rev-parse --verify --quiet "$certified^{commit}" >/dev/null 2>&1 ||
    unmeasured "the certified sha $certified is not a commit in THIS checkout, so neither" \
      "the PR diff nor the ancestor test can be evaluated here. Run this assert from the" \
      "lane whose branch carries the certified head."

  GH_VIEW_ARGV=(pr view "$pr" --repo "$repo")
  fetch_thread "$tmp/pr.json" baseRefName,body \
    "repos/$repo/issues/$pr/comments" "pr view $pr --repo $repo"

  local base_ref
  base_ref=$(python3 -c 'import json,sys
d=json.load(open(sys.argv[1]))
v=d.get("baseRefName")
print(v if isinstance(v,str) else "")' "$tmp/pr.json" 2>/dev/null) || base_ref=""
  [ -n "$base_ref" ] ||
    unmeasured "the PR payload carries no readable baseRefName, so the PR's own diff range" \
      "cannot be established."

  local merge_base=""
  local candidate
  for candidate in "origin/$base_ref" "$base_ref"; do
    merge_base=$(git merge-base "$candidate" "$certified" 2>/dev/null) && break
    merge_base=""
  done
  [ -n "$merge_base" ] ||
    unmeasured "no merge-base between $(sane "$base_ref") and $certified is resolvable in this" \
      "checkout (try \`git fetch origin\`), so the PR's own diff cannot be classified."

  say "pr-diff range $(sane "$merge_base")..$certified (merge-base with $(sane "$base_ref"))"

  # ---- STEP 1: is this PR reviewable at all? ------------------------------
  # A code-free diff cannot be roborev-certified AT ALL (project doctrine), so
  # demanding a job record here would red correct input — the guard agents learn
  # to waive. This is a DECLARED exemption printed LOUDLY, never a silent skip.
  classify_paths "$merge_base" "$certified"
  case "$?" in
    0)
      say "exemption this PR's own diff is CODE-FREE as scripts/ci/classify-docs-only.sh"
      say "exemption classifies it. A code-free diff cannot be roborev-certified at all, so"
      say "exemption no job record is demanded and NOTHING here is asserted about review"
      say "exemption coverage. The sanctioned substitute is primary-source verification"
      say "exemption recorded in the PR. This is a DECLARED exemption, not a silent skip."
      verdict NOT-APPLICABLE
      detail "no binding was measured, because there is no reviewable code to bind."
      exit 0
      ;;
    1) : ;;
    *)
      unmeasured "the PR's own diff could not be classified (git or the classifier failed)," \
        "so whether this PR needs a roborev record at all is unknown."
      ;;
  esac
  say "pr-diff carries reviewable code, so a roborev record IS required"

  # ---- STEP 2: discover the recorded job(s) -------------------------------
  local scan_out scan_rc
  scan_out=$(python3 "$SCAN_TOOL" jobs "$tmp/pr.json" 2>/dev/null)
  scan_rc=$?
  [ "$scan_rc" -eq 0 ] ||
    unmeasured "the PR payload could not be scanned for roborev records (exit $scan_rc)."

  local line
  while IFS= read -r line; do
    case "$line" in
      recorded-verdict=*)
        # REPORTED ONLY, and this is now a DELIBERATE non-use rather than a
        # residual: the authoritative signal is the JOB RECORD's structured
        # verdict, read in `reviewed_head_of` and judged at the binding site.
        # This line is the BLOCK's self-report — untrusted text — and is kept
        # visible so a mismatch between what a PR claims and what roborev
        # recorded is legible to a human. The two are textually distinct
        # (`recorded-verdict` here, `record verdict` at the binding site) so a
        # pasted log can never be read as the other.
        say "recorded-verdict $(sane "${line#recorded-verdict=}") — the BLOCK's own claim,"
        say "recorded-verdict reported for the reader ONLY. The binding decision below uses"
        say "recorded-verdict the JOB RECORD's structured verdict, never this."
        ;;
    esac
  done <<<"$scan_out"

  local jobs=()
  while IFS= read -r line; do
    case "$line" in job=*) jobs+=("${line#job=}") ;; esac
  done <<<"$scan_out"

  if [ "${#jobs[@]}" -eq 0 ]; then
    say "no-record no \`==== ROBOREV REVIEW SUMMARY ====\` block naming a job id was found on"
    say "no-record this PR's body or its top-level comments, so NOTHING on this PR binds a"
    say "no-record review to the tree about to merge."
    verdict UNBOUND
    detail "REMEDY: run the sanctioned wrapper LAST, after the gate of record and after any"
    detail "rebase — bash scripts/flow/roborev-review.sh --agent <agent> --model <model>"
    detail "--repo \$PWD — and post its \`==== ROBOREV REVIEW SUMMARY ====\` block as a"
    detail "top-level PR comment. A roborev round changes no bytes, so reviewing after"
    detail "gating costs nothing; a rebase changes bytes, so it VOIDS the round before it."
    exit 4
  fi
  say "records $(sane "${#jobs[@]}") roborev job id(s) recorded on this PR: $(sane "${jobs[*]}")"

  # ---- STEP 3: bind each recorded job to the certified head ---------------
  # ANY recorded round that covers the certified head is sufficient: a
  # multi-round PR legitimately leaves rounds 1..n-1 behind the head, and
  # failing on those would red correct input.
  #
  # SO ONE BAD RECORD MUST NOT END THE SCAN, AND THE RESOLUTION RULE IS STATED
  # HERE BESIDE THE CODE (#3752 blocker 2). Refusing on the FIRST unretrievable
  # record contradicts the contract in the paragraph above it — it refuses a PR
  # that DOES carry a later covering round — and a false rationale in a gate
  # artifact is worse than silence, because it is what stops the next person
  # looking. Every record is therefore examined and its failure RECORDED, then:
  #
  #   * a record that PROVES coverage decides the run outright (BOUND); an
  #     unresolved sibling cannot change an answer already proved.
  #   * with no coverage proved, an unresolved record COULD have been the
  #     covering one, so the verdict is UNMEASURED — a refusal naming what was
  #     unreadable, never permissive.
  #   * with no coverage proved and every record READ, nothing was unmeasurable
  #     and the definite refusal is UNBOUND.
  local job bound=0 unclassifiable=0 unclassifiable_base=0 reviewed
  local heads=()
  local unresolved=()
  local findings_unauthorized=0 verdict_unknown=0
  BOUND_NOTE=""

  # result_permits_binding <job> — 0 when this job's RECORD says its review
  # concluded in a state a merge may rest on. Sets RESULT_NOTE for the log.
  #
  # THIS IS THE FIX FOR roborev JOB 59, FINDING 1, and the thing it replaces was
  # a DECLARED RESIDUAL — "the recorded block's terminal verdict is REPORTED and
  # nothing is derived from it". That residual was a false-green route in a merge
  # gate: a `git_ref` match ALONE yielded BOUND, so a block naming an
  # in-progress, FAILED or findings-bearing job whose range happened to match
  # the certified head bound the merge. It is an ACCIDENT route before it is a
  # hostile one — THIS PR'''s own body recorded a job at `RESULT: FAIL`, and a
  # lane pasting its first (failing) round would have certified itself.
  #
  # The verdict is read from the JOB RECORD, never from the PR block: the block
  # is attacker- and accident-controlled text, the record is roborev'''s own
  # structured field. That is #3564'''s rule one directory over — `NONE` is
  # reachable only from the structured verdict letter, never reconstructed from
  # prose.
  result_permits_binding() {
    local j="$1" class
    RESULT_NOTE=""
    class=$(record_verdict_class "$RH_VERDICT")
    case "$class" in
      clean)
        RESULT_NOTE="record verdict is affirmatively CLEAN"
        return 0
        ;;
      findings)
        DEFERRAL_ISSUE_REFUSAL=""
        if deferral_authorized "$j" "$RH_BASE" "$RH_HEAD" "$tmp" "$repo"; then
          # A DISTINCT TOKEN, deliberately: nobody grepping this log for a
          # clean bind can match a deferred one.
          RESULT_NOTE="record verdict is FINDINGS, deferral AUTHORIZED by @$(sane "$DEFERRAL_AUTHOR") (tracking issues VERIFIED OPEN)"
          return 0
        fi
        # The issue-state refusal is NAMED when there was one: "the deferral is
        # not authorized" and "it is authorized but names a CLOSED issue" are
        # different operator actions, and collapsing them would send a lead to
        # re-post a marker that was already fine.
        if [ -n "$DEFERRAL_ISSUE_REFUSAL" ]; then
          RESULT_NOTE="record verdict is FINDINGS and its deferral cannot stand: $DEFERRAL_ISSUE_REFUSAL"
        else
          RESULT_NOTE="record verdict is FINDINGS and no authorized deferral covers this job"
        fi
        return 1
        ;;
      *)
        RESULT_NOTE="record verdict could not be established (status $(sane "${RH_STATUS:-<none>}"))"
        return 1
        ;;
    esac
  }
  for job in ${jobs[@]+"${jobs[@]}"}; do
    # NOT a command substitution. `reviewed_head_of` can refuse, and a refusal
    # inside `$( )` would exit only the SUBSHELL — the caller would read the
    # diagnostic as a sha and carry on. That is the fail-OPEN shape this whole
    # file exists to refuse, so the result travels in a global.
    RH_HEAD=""
    RH_BASE=""
    RH_ERR=""
    RH_VERDICT=""
    RH_STATUS=""
    if ! reviewed_head_of "$job" "$tmp"; then
      say "job $(sane "$job") $(sane "$RH_ERR")"
      unresolved+=("$RH_ERR")
      continue
    fi
    reviewed="$RH_HEAD"
    heads+=("$reviewed")
    say "job $(sane "$job") reviewed head $(sane "$reviewed")"

    # THE LOAD-BEARING TEST, FIRST. See the header.
    if ! git merge-base --is-ancestor "$reviewed" "$certified" >/dev/null 2>&1; then
      local kind
      kind=$(git cat-file -t "$reviewed" 2>/dev/null) || kind="<not a valid object here>"
      say "job $(sane "$job") NOT an ancestor of the certified head. Diagnostic only:"
      say "job $(sane "$job") \`git cat-file -t\` reports $(sane "$kind") — a rebase leaves the"
      say "job $(sane "$job") old commit dangling and reflog-reachable, so object validity"
      say "job $(sane "$job") proves nothing and the ancestor test is the verdict."
      continue
    fi

    # ---- THE BASE HALF, and it is checked BEFORE the equality shortcut ----
    # A `<head~1>..<head>` record satisfies EVERY head test there is, so a base
    # check placed after the shortcut would never run on the one shape it
    # exists for. The expected base is the MERGE-BASE, never the base ref's
    # TIP (#3392): a tip-expecting assert false-FAILs deterministically on any
    # branch whose main advanced, and that was misdiagnosed as a race twice.
    #
    # THE SKIPPED PREFIX IS A COMMIT SET, NOT A PATH DIFF AGAINST THE RECORDED
    # BASE — and getting that wrong is a FALSE FAIL, caught by the sibling
    # suite's end-to-end fixture. A recorded base OFF this branch (the base
    # ref's tip, say) skips NONE of the PR's own commits: none of them is an
    # ancestor of it. What the review actually skipped is the part of the PR
    # range that IS an ancestor of the recorded base, i.e. everything from the
    # merge-base up to `merge-base(recorded-base, certified)`. So that
    # projection is computed first, and a projection at or before the PR's
    # merge-base means the round started at or before this branch's first
    # commit and skipped nothing — which is also what keeps a legitimate
    # SUPERSET review from reading as a gap.
    local review_start
    review_start=$(git merge-base "$RH_BASE" "$certified" 2>/dev/null) || review_start=""
    if [ -z "$review_start" ]; then
      say "job $(sane "$job") reviewed BASE $(sane "$RH_BASE") could not be located"
      say "job $(sane "$job") relative to the certified head in this checkout, so how much"
      say "job $(sane "$job") of the branch the round covered is unknown"
      unclassifiable_base=1
      continue
    fi
    if git merge-base --is-ancestor "$review_start" "$merge_base" >/dev/null 2>&1; then
      :
    else
      classify_paths "$merge_base" "$review_start"
      case "$?" in
        0)
          say "job $(sane "$job") starts after this PR's merge-base, but the omitted"
          say "job $(sane "$job") prefix is prose by scripts/ci/classify-docs-only.sh"
          ;;
        1)
          say "job $(sane "$job") reviewed BASE $(sane "$RH_BASE") is not this PR's"
          say "job $(sane "$job") merge-base, and the prefix it skipped carries unreviewed"
          say "job $(sane "$job") reviewable code. A partial range whose HEAD equals the"
          say "job $(sane "$job") certified sha passes every head test and still leaves"
          say "job $(sane "$job") earlier commits unreviewed, so the head half alone"
          say "job $(sane "$job") cannot bind."
          continue
          ;;
        *)
          say "job $(sane "$job") reviewed BASE $(sane "$RH_BASE") could not be compared"
          say "job $(sane "$job") against this PR's merge-base, so how much of the branch"
          say "job $(sane "$job") the round covered is unknown"
          unclassifiable_base=1
          continue
          ;;
      esac
    fi

    if [ "$reviewed" = "$certified" ]; then
      say "job $(sane "$job") reviewed head EQUALS the certified head"
      if result_permits_binding "$job"; then
        say "job $(sane "$job") $(sane "$RESULT_NOTE")"
        bound=1
        BOUND_NOTE="$RESULT_NOTE"
        break
      fi
      say "job $(sane "$job") but it CANNOT bind: $(sane "$RESULT_NOTE")"
      case "$(record_verdict_class "$RH_VERDICT")" in
        findings) findings_unauthorized=1 ;;
        *) verdict_unknown=1 ;;
      esac
      continue
    fi

    classify_paths "$reviewed" "$certified"
    case "$?" in
      0)
        say "job $(sane "$job") is an ancestor and everything after it is prose by"
        say "job $(sane "$job") scripts/ci/classify-docs-only.sh, so no reviewable code was"
        say "job $(sane "$job") added after the review"
        if result_permits_binding "$job"; then
          say "job $(sane "$job") $(sane "$RESULT_NOTE")"
          bound=1
          BOUND_NOTE="$RESULT_NOTE"
          break
        fi
        say "job $(sane "$job") but it CANNOT bind: $(sane "$RESULT_NOTE")"
        case "$(record_verdict_class "$RH_VERDICT")" in
          findings) findings_unauthorized=1 ;;
          *) verdict_unknown=1 ;;
        esac
        ;;
      1)
        say "job $(sane "$job") is an ancestor, but REVIEWABLE CODE was added after it"
        ;;
      *)
        say "job $(sane "$job") is an ancestor, but the range after it could not be"
        say "job $(sane "$job") classified, so its coverage is unknown"
        unclassifiable=1
        ;;
    esac
  done

  # GUARDED EXPANSION, and this is the site the portability finding names
  # (roborev job 59, finding 4). `heads` is EMPTY exactly when every recorded
  # job record was unretrievable — the run that MUST still reach the documented
  # UNMEASURED verdict below. Under `set -u` on bash 3.2 a bare `"${heads[@]}"`
  # ABORTS on an empty array, so the leg exited with no verdict at all on
  # precisely its fail-closed path: a refusal that never printed its refusal.
  local head
  for head in ${heads[@]+"${heads[@]}"}; do
    print_self_check "$head" "$certified"
  done
  if [ "$bound" -eq 1 ]; then
    verdict BOUND
    detail "a recorded roborev round covers the certified head's reviewable content, and that"
    detail "round's RECORD says its review concluded in a bindable state: $BOUND_NOTE."
    case "$BOUND_NOTE" in
      *AUTHORIZED*)
        detail "THAT BIND RESTS ON AN AUTHORIZED DEFERRAL, not on a clean review. The"
        detail "authorization was re-verified here from the PR's top-level comments (marker"
        detail "form, sole content, column-zero, structured authorship, hard-coded allowlist,"
        detail "base/head/job scope), and EVERY issue its issues= half names was verified to"
        detail "be an OPEN issue GitHub confirms — asked four-valued, so a CLOSED issue, a"
        detail "non-existent one, and one that could NOT BE ASKED are each a refusal and none"
        detail "of them grants. The marker's count= half is NOT re-verified here and is not"
        detail "claimed to be: it is matched against the findings count OBSERVED BY THE"
        detail "REVIEW, which this leg never ran, and it is enforced at review time. That is"
        detail "the ONE unverified half of this authorization, and it is named rather than"
        detail "left for a reader to discover."
        ;;
    esac
    exit 0
  fi

  # Nothing bound. A measurement failure is NOT an absence of coverage, and the
  # two need different operator actions, so each unreadable input is named.
  local causes=()
  if [ "$unclassifiable" -eq 1 ]; then
    causes+=("a recorded round IS an ancestor of the certified head, but the range after it \
could not be classified, so whether reviewable code was added after the review is UNKNOWN.")
  fi
  if [ "$unclassifiable_base" -eq 1 ]; then
    causes+=("a recorded round's BASE half could not be compared against this PR's merge-base, \
so how much of the branch that round actually covered is UNKNOWN.")
  fi
  if [ "$verdict_unknown" -eq 1 ]; then
    causes+=("a recorded round COVERS the certified head, but its job RECORD carries no \
verdict this code can read, so whether that review concluded at all is UNKNOWN. A range match \
alone is not a review: the record must AFFIRMATIVELY say the review finished. Re-run the \
sanctioned wrapper and post the block it prints.")
  fi
  local why
  for why in ${unresolved[@]+"${unresolved[@]}"}; do
    causes+=("$why")
  done
  if [ "${#causes[@]}" -gt 0 ]; then
    causes+=("That is a measurement failure, not an absence of coverage, and the two need \
different actions — so it is reported as its own verdict rather than folded into one.")
    unmeasured ${causes[@]+"${causes[@]}"}
  fi
  # A FINDINGS RECORD WITH NO AUTHORIZED DEFERRAL IS A *MEASURED* REFUSAL, SO IT
  # IS UNBOUND AND NOT UNMEASURED. The distinction is the one this file keeps
  # everywhere else: `unmeasured` means the oracle could not be consulted, and
  # here it WAS — roborev'''s record affirmatively says the review found findings
  # and no allowlisted human authorized deferring them. Folding a definite
  # refusal into UNMEASURED would misdescribe it to the operator, whose action
  # is completely different (triage the findings, or get a deferral authorized —
  # not "fix the box and re-run").
  if [ "$findings_unauthorized" -eq 1 ]; then
    say "unbound a recorded round COVERS the certified head, but its record verdict is"
    say "unbound FINDINGS and no authorized deferral covers it."
    verdict UNBOUND
    detail "REMEDY: either resolve the findings and run a fresh round at this head, or — if the"
    detail "lead has ruled them deferrable — have an authorized human post the findings-deferral"
    detail "marker for THIS base/head/job as the sole content of a top-level PR comment, then"
    detail "re-decide the round with the wrapper's --recheck-job <id>. The marker form is in"
    detail "\`bash scripts/flow/roborev-review.sh --help\`; it is deliberately not printed here."
    exit 4
  fi
  say "unbound none of the recorded roborev rounds covers the certified head."
  verdict UNBOUND
  detail "REMEDY: re-run the sanctioned wrapper at the CURRENT head, AFTER the rebase and"
  detail "AFTER the gate of record, and post its block as a top-level PR comment. A rebase"
  detail "rewrites the reviewed commit, so it VOIDS every round taken before it."
  exit 4
}

# reviewed_head_of <job> <tmp> — sets RH_HEAD and RH_BASE from the job RECORD's
# `git_ref`, or sets RH_ERR and returns 1. BOTH HALVES ARE PART OF THE BINDING.
#
# VALIDATING ONLY THE HEAD HALF REOPENS THE T4 VACUITY CLASS ONE LEVEL DOWN.
# Project doctrine records it: "a SINGLE-SHA review covers ONE COMMIT — a
# PARTIAL review whose enqueued sha EQUALS HEAD, so no sha check can see it."
# A record of `<head~1>..<head>` has a head EQUAL to the certified sha and
# leaves every earlier reviewable commit on the branch unreviewed. The wrapper
# asserts against that at REVIEW time; this leg asserts it at MERGE time, or
# the merge gate is blind to exactly the vacuity the wrapper was built to catch.
#
# DERIVED FROM THE RECORD, NEVER FROM STDOUT (#3752 AC2/#2964). The wrapper's
# `Enqueued job <N> for <sha>` line names, for a RANGE review, only the range
# BASE — so prose cannot establish which head was reviewed. `git_ref` is the
# record's own `<base40>..<head40>`.
#
# Parsing is delegated to roborev-job-facts.py — the SAME parser the wrapper
# uses. A second implementation's correctness is only knowable by differential
# testing against the first (#3229), so there is deliberately only one.
reviewed_head_of() {
  local job="$1" tmp="$2" payload json ref head base
  RH_HEAD=""
  RH_BASE=""
  RH_VERDICT=""
  RH_STATUS=""
  RH_ERR=""
  for payload in show list; do
    case "$payload" in
      show) json=$(roborev show "$job" --json 2>/dev/null || printf '') ;;
      list)
        local top
        top=$(git rev-parse --show-toplevel 2>/dev/null) || top=""
        if [ -n "$top" ]; then
          json=$(roborev list --json --limit 50 --repo "$top" 2>/dev/null || printf '')
        else
          json=$(roborev list --json --limit 50 2>/dev/null || printf '')
        fi
        ;;
    esac
    [ -n "$json" ] || continue
    : >"$tmp/facts"
    # Redirection, not a pipe, for the reason given in `classify_paths`: under
    # `set -o pipefail` a consumer that exits before draining stdin makes the
    # pipeline report the PRODUCER's SIGPIPE, which is indistinguishable here
    # from the parse having failed.
    python3 "$FACTS_TOOL" "$job" "$tmp/facts" "$tmp/prompt" \
      >/dev/null 2>&1 <<<"$json" || continue
    ref=$(sed -n 's/^git_ref=//p' "$tmp/facts" | head -1 | tr 'A-F' 'a-f')
    [ -n "$ref" ] || continue
    # THE RECORD'S OWN STRUCTURED RESULT, from the same parse (roborev job 59,
    # finding 1). `verdict` and `status` are already STRING_FACTS of the shared
    # facts tool, so this needs no second parser. They are read here and JUDGED
    # at the binding site, where the alternative to binding can be named.
    RH_VERDICT=$(sed -n 's/^verdict=//p' "$tmp/facts" | head -1)
    RH_STATUS=$(sed -n 's/^status=//p' "$tmp/facts" | head -1)
    case "$ref" in
      *..*) : ;;
      *)
        RH_ERR="job $job's record carries git_ref '$ref', which is a SINGLE-COMMIT record, not the sanctioned <base40>..<head40> range. A single-sha review covers ONE COMMIT and cannot certify a multi-commit branch, so this fails closed (#2964)."
        return 1
        ;;
    esac
    head="${ref##*..}"
    base="${ref%%..*}"
    case "$head" in
      *[!0-9a-f]* | "")
        RH_ERR="job $job's record carries git_ref '$ref', whose head half is not hex, so the reviewed head cannot be established."
        return 1
        ;;
    esac
    if [ "${#head}" -ne 40 ]; then
      RH_ERR="job $job's record carries git_ref '$ref', whose head half is not a full 40-hex sha, so the reviewed head cannot be established."
      return 1
    fi
    case "$base" in
      *[!0-9a-f]* | "")
        RH_ERR="job $job's record carries git_ref '$ref', whose base half is not hex, so where the review STARTED cannot be established — and the base half is half the binding (#3752)."
        return 1
        ;;
    esac
    if [ "${#base}" -ne 40 ]; then
      RH_ERR="job $job's record carries git_ref '$ref', whose base half is not a full 40-hex sha, so where the review STARTED cannot be established — and the base half is half the binding (#3752)."
      return 1
    fi
    RH_HEAD="$head"
    RH_BASE="$base"
    return 0
  done
  RH_ERR="job $job's record could not be retrieved or carries no git_ref. Neither \`roborev show $job --json\` nor \`roborev list --json\` yielded it, so the reviewed head is unknown — which is a refusal, never a skip (#3752 AC3)."
  return 1
}

# ---------------------------------------------------------------------------
# hold-check
# ---------------------------------------------------------------------------
cmd_hold_check() {
  local pr="$1" repo="$2"
  P='PREMERGE: HOLD-CHECK'

  need_tool gh
  need_tool python3
  need_file "$SCAN_TOOL"

  # GLOBAL for the same reason as the review-binding leg above.
  TMPD=$(mktemp -d "${TMPDIR:-/tmp}/premerge-hold-check.XXXXXX" 2>/dev/null) || TMPD=""
  [ -n "$TMPD" ] && [ -d "$TMPD" ] ||
    unmeasured "could not create a scratch directory under ${TMPDIR:-/tmp}."
  trap 'rm -rf "$TMPD"' EXIT
  local tmp="$TMPD"

  # Read STRUCTURALLY (`--json`, no `--jq` flattening): author and body stay
  # SEPARATE FIELDS of one object, so a comment body can never forge its own
  # author record (#3312 instance 4). The COMMENTS come from the paginated
  # endpoint via `fetch_thread`, because the `--json comments` connection is
  # bounded and a stop order on a later page must be seen.
  GH_VIEW_ARGV=(pr view "$pr" --repo "$repo")
  fetch_thread "$tmp/pr.json" body,closingIssuesReferences \
    "repos/$repo/issues/$pr/comments" "pr view $pr --repo $repo"

  # `--paginate`, AND EVERY PAGE IS DECODED BEFORE ANY VERDICT (#3752 blocker 3).
  # One page of 100 events is not the timeline: on a longer PR a recent
  # `auto_merge_disabled` sits on a LATER page, and a `clear` derived from a
  # signal that was never fully read is the affirmative-measurement rule
  # violated directly — a false clearance on precisely the scenario this leg
  # exists for (#3735 merged three minutes after the lead disarmed it).
  #
  # `gh api --paginate` emits ONE JSON ARRAY PER PAGE, CONCATENATED — not one
  # array — so the scanner decodes a document STREAM, and a stream that cannot
  # be decoded in full is UNMEASURED, read as a hold.
  gh api --paginate "repos/$repo/issues/$pr/timeline?per_page=100" \
    >"$tmp/timeline.json" 2>/dev/null ||
    unmeasured "the PR timeline could not be read IN FULL (pagination failed), so a lead" \
      "disarm inside the ${PREMERGE_DISARM_WINDOW_SECS}s window could not be ruled out." \
      "An incompletely-read timeline is a hold, never a clearance."

  # WHICH ISSUE THREADS THIS PR CLOSES IS A THREE-VALUED SIGNAL, AND THE
  # PERMISSIVE COLLAPSE HERE WAS A FALSE CLEARANCE (#3752, lane-3752 audit).
  # This used to read `issues=$(python3 -c ...) || issues=""`, which folded "the
  # PR closes NO issue" and "which issues it closes could not be read" onto ONE
  # value — the empty list — so a payload this extractor could not parse
  # silently re-read NO issue thread and the leg could still reach
  # NO-HOLD-RECOGNISED with a `HOLD:` sitting on the issue. Note the asymmetry
  # that made it invisible: an unreadable KNOWN issue is already `unmeasured`
  # ten lines down, so only the step that DISCOVERS the list was permissive.
  # The rc is now checked AFFIRMATIVELY, and the extractor REFUSES on any shape
  # it does not recognise rather than skipping the entry (a skipped entry is the
  # same collapse one level in).
  local extra=()
  local issues issue issues_rc
  issues=$(python3 -c 'import json,sys
d=json.load(open(sys.argv[1]))
r=d.get("closingIssuesReferences")
if r is None:
    r=[]
if not isinstance(r,list):
    raise SystemExit("closingIssuesReferences is not a list")
out=[]
for i in r:
    if not isinstance(i,dict):
        raise SystemExit("a closingIssuesReferences entry is not an object")
    n=i.get("number")
    if not isinstance(n,int) or isinstance(n,bool):
        raise SystemExit("a closingIssuesReferences entry carries no integer number")
    out.append(str(n))
print("\n".join(out))' \
    "$tmp/pr.json" 2>/dev/null)
  issues_rc=$?
  [ "$issues_rc" -eq 0 ] ||
    unmeasured "the PR payload's closingIssuesReferences could not be read (exit $issues_rc), so" \
      "WHICH issue threads this PR closes is unknown, and a stop order posted on one of them" \
      "could not be ruled out. Unreadable is a hold, never a clearance."
  while IFS= read -r issue; do
    [ -n "$issue" ] || continue
    # The ISSUE thread is paginated for the same reason as the PR's: a lead
    # stop order posted on the issue this PR closes is exactly the artifact a
    # bounded connection drops.
    GH_VIEW_ARGV=(issue view "$issue" --repo "$repo")
    fetch_thread "$tmp/issue-$issue.json" body \
      "repos/$repo/issues/$issue/comments" "issue view $issue --repo $repo"
    extra+=("$tmp/issue-$issue.json")
    say "thread also re-reading issue #$(sane "$issue"), which this PR closes"
  done <<<"$issues"

  say "window a lead disarm (auto_merge_disabled) counts as a stop order for"
  say "window ${PREMERGE_DISARM_WINDOW_SECS}s — a NAMED COMMITTED CONSTANT with NO env"
  say "window override, because an override is settable by the party it constrains (#3312)."

  local out rc
  out=$(python3 "$SCAN_TOOL" hold "$PREMERGE_DISARM_WINDOW_SECS" "$tmp/pr.json" \
    "$tmp/timeline.json" ${extra+"${extra[@]}"} 2>/dev/null)
  rc=$?

  local line
  while IFS= read -r line; do
    case "$line" in
      event=*) say "event $(sane "${line#event=}")" ;;
      detail=*) detail "${line#detail=}" ;;
    esac
  done <<<"$out"

  case "$rc" in
    0)
      verdict NO-HOLD-RECOGNISED
      detail "a SCAN RESULT, never a clearance: markers are recognised only as column-zero"
      detail "\`HOLD:\` / \`GO:\` / \`RELEASE:\` lines on top-level comments, plus"
      detail "auto_merge_disabled inside the window. Anything said another way is not seen."
      exit 0
      ;;
    4)
      verdict HOLD-FOUND
      detail "REMEDY: do NOT merge. Resolve the stop order with its author. A lead clears it"
      detail "with a column-zero \`GO:\` or \`RELEASE:\` line on the PR or issue thread."
      exit 4
      ;;
    *)
      unmeasured "the thread scan exited $rc, so no stop order could be ruled out."
      ;;
  esac
}

[ "$#" -ge 1 ] || usage
case "$1" in
  review-binding)
    [ "$#" -eq 4 ] || usage
    cmd_review_binding "$2" "$3" "$(printf '%s' "$4" | tr 'A-F' 'a-f')"
    ;;
  hold-check)
    [ "$#" -eq 3 ] || usage
    cmd_hold_check "$2" "$3"
    ;;
  *) usage ;;
esac
