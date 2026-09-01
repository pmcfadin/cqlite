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
#       CLOSED set, matched token-exactly by the caller. Prose goes on
#       `verdict-detail` lines, so the token position can never hold a word.
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
#   1. It does NOT enforce that the recorded roborev block's own terminal
#      verdict is affirmative. That value is REPORTED on a `recorded-verdict`
#      line and nothing is derived from it: an intermediate round legitimately
#      records findings, so a hard check there would red correct input. #3752
#      declares this a residual, out of its own scope.
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

  gh pr view "$pr" --repo "$repo" --json baseRefName,body,comments >"$tmp/pr.json" 2>/dev/null ||
    unmeasured "\`gh pr view $pr --repo $repo\` failed (auth/network/no-such-PR), so the PR's" \
      "own record of its roborev round could not be read."

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
        say "recorded-verdict $(sane "${line#recorded-verdict=}") — reported ONLY. Nothing is"
        say "recorded-verdict derived from it here (#3752 declared residual)."
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
  for job in "${jobs[@]}"; do
    # NOT a command substitution. `reviewed_head_of` can refuse, and a refusal
    # inside `$( )` would exit only the SUBSHELL — the caller would read the
    # diagnostic as a sha and carry on. That is the fail-OPEN shape this whole
    # file exists to refuse, so the result travels in a global.
    RH_HEAD=""
    RH_BASE=""
    RH_ERR=""
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
      bound=1
      break
    fi

    classify_paths "$reviewed" "$certified"
    case "$?" in
      0)
        say "job $(sane "$job") is an ancestor and everything after it is prose by"
        say "job $(sane "$job") scripts/ci/classify-docs-only.sh, so no reviewable code was"
        say "job $(sane "$job") added after the review"
        bound=1
        break
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

  local head
  for head in "${heads[@]}"; do
    print_self_check "$head" "$certified"
  done
  if [ "$bound" -eq 1 ]; then
    verdict BOUND
    detail "a recorded roborev round covers the certified head's reviewable content."
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
  local why
  for why in ${unresolved[@]+"${unresolved[@]}"}; do
    causes+=("$why")
  done
  if [ "${#causes[@]}" -gt 0 ]; then
    causes+=("That is a measurement failure, not an absence of coverage, and the two need \
different actions — so it is reported as its own verdict rather than folded into one.")
    unmeasured "${causes[@]}"
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
  # author record (#3312 instance 4).
  gh pr view "$pr" --repo "$repo" --json body,comments,closingIssuesReferences \
    >"$tmp/pr.json" 2>/dev/null ||
    unmeasured "\`gh pr view $pr --repo $repo\` failed, so the PR thread could not be re-read" \
      "for a stop order. A thread that cannot be read is a hold, never a clearance."

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
    if gh issue view "$issue" --repo "$repo" --json body,comments \
      >"$tmp/issue-$issue.json" 2>/dev/null; then
      extra+=("$tmp/issue-$issue.json")
      say "thread also re-reading issue #$(sane "$issue"), which this PR closes"
    else
      unmeasured "issue #$issue (closed by this PR) could not be read, so a stop order posted" \
        "on the ISSUE thread could not be ruled out. Unreadable is a hold, never a clearance."
    fi
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
