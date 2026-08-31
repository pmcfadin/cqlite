#!/usr/bin/env bash
#
# advertised-collision-scan.sh — report the ADVERTISED COLLISION WINDOW: an issue
# the board offers as `Ready` while a pushed `issue-<N>-*` branch stands on origin
# and NOBODY holds `refs/claims/issue-<N>` (issue #3436, lead deliverable 2, epic
# #2664).
#
# WHY THIS EXISTS
# ---------------
# #3436's first instance was a SILENT collision: two Claude sessions worked one
# issue in one worktree on one box for ~20 minutes, and the second session had to
# guess a lane path to get there. The second instance (#3393, 2026-08-29) is worse
# because it is ADVERTISED. Sequence, every step of it correct in isolation:
#   1. a slice shipped, PR merged;
#   2. `flow-finalize` RELEASED `refs/claims/issue-3393` — proper behaviour;
#   3. the board went back to `Ready` — also proper at that moment;
#   4. the lead re-issued work on the SAME branch, which then ran for 20+ commits
#      holding NO claim ref while the board advertised the issue as available.
# The flow has a release-on-finalize step and had no re-acquire-on-resume step, so
# a well-behaved session doing exactly what doctrine says — read the board, take a
# `Ready` item — would have collided, and the claim ref could not stop it because
# no ref existed.
#
# The state is MACHINE-VISIBLE with no heuristics: THREE facts, ANDed.
#   (1) board `Status = Ready`                (server-side filtered read)
#   (2) a pushed `refs/heads/issue-<N>-*`     (git ls-remote)
#   (3) NO `refs/claims/issue-<N>`            (git ls-remote)
# Nothing here infers, scores or guesses. Each fact is read or the run is
# UNMEASURABLE.
#
# POSITIVE-DETECTION ONLY — IT NEVER EXITS 0
# ------------------------------------------
#   3  at least one row was REPORTED
#   1  no row was reported OR an input could not be measured
#   64 usage error
# There is deliberately NO exit code for "this fleet is clean", following #3393's
# split ruling on `claim-heartbeat.sh dead-lanes`: the fail-open defect family
# there was FIVE instances of a failed probe read as a NEGATIVE ANSWER, and they
# clustered in exactly the exit-0 path a cron reads. So a clean bill of health is
# not this tool's to give. Act on 3; NEVER read 1 as "no collision window exists".
# Every unmeasurable input (gh absent, gh error, an ls-remote failure, a board
# read that cannot be parsed) lands on exit 1 with an `UNMEASURABLE` line NAMING
# what could not be measured — never a silent "none found".
#
# IT REPORTS AND NEVER MUTATES
# ----------------------------
# No ref is created or deleted, no board item is moved, no branch is touched, no
# comment is posted. WHY: only the session on that box knows whether it owns the
# branch — from here, "the lane is yours, re-take the lock" and "a peer's lane was
# abandoned, adopt it" look identical, and they have opposite remedies. Acting on
# that ambiguity would hand a live lane to a second writer, which is the damage
# this issue documents. The remedy is `claim.sh verify <N>` on the box that owns
# the lane, then the documented resume path (see `claim.sh -h`).
#
# HOW THE BOARD IS READ — FILTERED, NEVER A BARE PAGE
# ---------------------------------------------------
# `gh project item-list 1 --owner pmcfadin --query 'status:Ready'`. CLAUDE.md is
# emphatic: this board carries 900+ items and an UNFILTERED `item-list` SILENTLY
# TRUNCATES at the page limit, returning a partial column with no error — which has
# produced wrong "nothing is Ready" reads. A server-side filter is exact. Do NOT
# reach for GraphQL to work around truncation; filter. If the filtered page comes
# back exactly at the limit, this script says so on a `notice=` line, because a
# full page is the one shape that could still be truncated.
#
# LANE-LOCK STATE IS REPORTED VERBATIM, NOT RE-CLASSIFIED
# ------------------------------------------------------
# Each row carries the machine-local lane lock's own words for the lane directory
# (`scripts/flow/lane-lock.sh probe`, read-only: no record, no mutex, no mkdir),
# which is what composes the two locks the issue says know nothing about each
# other. The verdict and liveness token are printed AS THE PROBE PRINTED THEM —
# this script does not re-derive "occupied" or "reclaimable", because lane-lock.sh
# owns that closed verdict set and a second mapping of it would be a second,
# weaker arbiter of the same fact. A probe that cannot run is
# `lane-lock=unmeasured(<cause>)` and never blocks or fails the scan: the lane lock
# is machine-local, so its answer is only meaningful ON the box being scanned.
#
# USAGE
#   advertised-collision-scan.sh [--issue <N>] [--json] [--help]
#     --issue <N>  restrict the scan to ONE issue (the three facts are unchanged)
#     --json       one JSON object per row on stdout, plus a final summary object.
#                  Every STRING value is escaped (see json_str) — remote names, lane
#                  paths, branch lists and failure details are caller- or
#                  filesystem-derived, and one `"` used to emit output that CLAIMED to
#                  be JSON and was not. Counters and `board_page_at_limit` are emitted
#                  as JSON numbers/booleans, unquoted.
#                 KNOWN LIMIT: a value carrying INVALID UTF-8 still reaches the output
#                 raw, and a conforming parser rejects the document (JSON is defined
#                 over Unicode text). Measured, not assumed. Deliberately unfixed —
#                 validating UTF-8 needs either a hand-rolled bash validator or an
#                 interpreter this component must not depend on; see json_str's header.
#
# ENV
#   CLAIM_REMOTE   origin remote name or URL (default: origin) — the same variable
#                  claim.sh uses, so a scan reads the remote the claims live on
#   LANE_ROOT      lane-directory root, passed through to lane-lock.sh
#                  (default /data/lanes)
#
# CONSTRAINTS
#   macOS bash 3.2 compatible (no associative arrays, no mapfile/readarray).
#   `set -euo pipefail`, shellcheck-clean. No python3, no cargo, no writes.
#   Read-only everywhere: `git ls-remote`, `gh project item-list`, `lane-lock.sh
#   probe`.
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"
REMOTE="${CLAIM_REMOTE:-origin}"
BOARD_NUMBER=1
BOARD_OWNER=pmcfadin
# The owner/repo a board row must belong to (#3436, roborev round 14). HARD-CODED, exactly like
# BOARD_OWNER and BOARD_NUMBER above, because this tool is already specific to ONE board and a
# board is specific to its repositories. Deriving it from the remote URL was tried and reverted:
# CLAIM_REMOTE may be a NAME, a URL or a local path (the suite uses a path), so `git remote
# get-url` fails for legitimate inputs — and then every row is undeterminable, which either
# fails the scan closed on valid input or, worse, takes the permissive branch and accepts rows
# from any repository. A constant cannot be undeterminable.
BOARD_EXPECT_REPO="${BOARD_OWNER}/cqlite"
BOARD_LIMIT=100

# Never let a remote read block on an interactive credential prompt: an unattended
# sweep would hang instead of reporting an UNMEASURABLE input (same reason as
# claim.sh).
export GIT_TERMINAL_PROMPT=0

die_usage() { echo "$prog: $*" >&2; exit 64; }
say()       { printf '%s\n' "$*"; }

# ---------------------------------------------------------------------------
# json_str <text> — <text> as a QUOTED, ESCAPED JSON string, quotes included.
#
# EVERY string value emitted under --json goes through this, and nothing else may
# interpolate a value into JSON directly. Remote names (CLAIM_REMOTE), lane paths,
# branch lists and failure details are all caller- or filesystem-derived, so a single
# `"` or `\` produced OUTPUT THAT CLAIMED TO BE JSON AND WAS NOT — the worst failure
# shape for a machine-read field, because the consumer's parse error names the parser
# and not the value.
#
# PURE BASH ON PURPOSE — no python3, no jq. This script's suite runs in the gate's
# `tooling-tests` component BEFORE its python3 gate, and acquiring an interpreter
# dependency to print a string would be a new SKIP surface in a lane that has none.
#
# Escapes, in the only order that is correct: the BACKSLASH first (reversing it would
# escape the escapes this function just added), then the double quote, then the five
# short control escapes, then every remaining C0 control as \u00xx. DEL (0x7f) and
# every byte above it pass through — JSON requires no escape for them, and passing
# UTF-8 bytes through unchanged keeps valid UTF-8 valid. NUL cannot occur: bash cannot
# hold it in a variable.
#
# STATED LIMIT, MEASURED RATHER THAN ASSUMED (#3436). Passing high bytes through keeps
# VALID UTF-8 valid; it does not MAKE invalid UTF-8 valid. A value carrying malformed
# UTF-8 therefore reaches the output raw, and JSON is defined over Unicode text, so a
# conforming consumer rejects the document:
#
#   $ CLAIM_REMOTE="$(printf 'ori\xffgin')" advertised-collision-scan.sh --json
#     -> the 0xFF byte survives into the value; a STRICT UTF-8 decode of the line
#        fails ("invalid start byte"), so no conforming parser accepts it.
#
# It is the same defect class as an unescaped quote, with a rarer trigger, and it is
# NOT fixed here deliberately. The options are a hand-rolled UTF-8 validator in bash or
# a new interpreter dependency, and this suite runs before the gate's python3 gate by
# design (see PURE BASH ON PURPOSE above). Shipping an untested byte-level validator
# into a merge-gating component is the worse trade — the same reasoning that removed
# #3229's `census-exclusion:` predictor and #3400's parse-site lint: a guard with
# unknown false verdicts invites reliance it cannot support.
#
# Worth recording how the limit was confirmed, because the first attempt got it wrong:
# a check that piped the output into a lenient decoder reported "parses OK", because
# the decoder replaced the bad byte before the parse ever ran. Re-running against RAW
# BYTES with a strict decode inverted the result. A JSON-validity test that decodes
# leniently is not a JSON-validity test.
# redact_remote <remote> — a remote for DISPLAY, with URL userinfo stripped.
#
# WHY (#3436, roborev round 7): CLAIM_REMOTE accepts a URL, and this script printed it
# VERBATIM in two `unmeasurable` details and in the text and JSON summaries. A
# credential-bearing HTTPS remote (`https://user:token@host/org/repo.git`) therefore wrote the
# TOKEN into every log, PR comment and CI transcript that carried the output. JSON escaping
# does not help: escaping makes a secret well-formed, not absent.
#
# Only the userinfo component is removed — host, path and scheme are the useful parts of the
# diagnostic, and over-redacting would make "which remote failed" unanswerable. A bare
# filesystem path or an ssh-style `git@host:org/repo` has no userinfo to strip and passes
# through unchanged, which keeps the common cases readable.
redact_remote() {
  case "${1:-}" in
    *://*@*) printf '%s://***@%s\n' "${1%%://*}" "${1#*://*@}" ;;
    *)       printf '%s\n' "${1:-}" ;;
  esac
}

json_str() {
  local LC_ALL=C
  local s="${1:-}" out="" i=0 len c code
  len=${#s}
  while [ "$i" -lt "$len" ]; do
    c="${s:$i:1}"
    i=$((i + 1))
    case "$c" in
      '\')         out="$out"'\\' ;;
      '"')         out="$out"'\"' ;;
      $'\b')       out="$out\b" ;;
      $'\f')       out="$out\f" ;;
      $'\n')       out="$out\n" ;;
      $'\r')       out="$out\r" ;;
      $'\t')       out="$out\t" ;;
      [[:cntrl:]]) code="$(printf '%d' "'$c")"; out="$out$(printf '\\u%04x' "$code")" ;;
      *)           out="$out$c" ;;
    esac
  done
  printf '"%s"' "$out"
}

# unmeasurable <what> <detail> — an input that could not be READ, reported in the
# active output mode and ALWAYS paired with exit 1. It names the input, because
# "none found" and "could not look" are the two answers a positive-detection tool
# must never render identically (#3393).
unmeasurable() {
  if [ "$AS_JSON" -eq 1 ]; then
    printf '{"summary":"advertised-collision","result":"UNMEASURABLE","rows":0,"measured":"no","unmeasurable":%s,"detail":%s}\n' \
      "$(json_str "$1")" "$(json_str "$2")"
  else
    printf 'SCAN: UNMEASURABLE what=%s detail=%s\n' "$1" "$2"
    printf 'SCAN: advertised-collision rows=0 measured=no (positive-detection only: this is NOT a clean bill of health)\n'
  fi
}

print_help() {
  awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2 {sub(/^# ?/,""); print}' "$0"
}

# Resolved from THIS script's own directory with no env override (#3312: the
# constrained party must not choose its own enforcer).
LANE_LOCK_SH=""
_scan_dir="$(cd -- "$(dirname -- "$0")" >/dev/null 2>&1 && pwd)" || _scan_dir=""
[ -z "$_scan_dir" ] || LANE_LOCK_SH="$_scan_dir/lane-lock.sh"
unset _scan_dir

ONLY_ISSUE=""
AS_JSON=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --issue) [ "$#" -ge 2 ] || die_usage "--issue requires a value"
             case "$2" in *[!0-9]*|'') die_usage "--issue requires a numeric issue number (got '$2')" ;; esac
             ONLY_ISSUE="$2"; shift 2 ;;
    --json)  AS_JSON=1; shift ;;
    -h|--help) print_help; exit 0 ;;
    # Reject rather than ignore: an unrecognized flag in a sweep would otherwise
    # silently widen or narrow the scan.
    *) die_usage "unknown argument '$1' (see --help)" ;;
  esac
done

# ---------------------------------------------------------------------------
# FACT (2): pushed issue-<N>-* branches on the remote.
# ONE network call for the whole fleet — never one call per issue.
BRANCH_PAIRS=""     # lines: <issue><TAB><ref>
BRANCH_ISSUES=""    # unique issue numbers, one per line
scan_branches() {
  local raw line ref rest num
  if ! raw="$(git ls-remote --heads "$REMOTE" 'issue-*' 2>/dev/null)"; then
    return 1
  fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    ref="$(printf '%s' "$line" | awk '{print $2}')"
    case "$ref" in refs/heads/issue-*) ;; *) continue ;; esac
    rest="${ref#refs/heads/issue-}"
    num="${rest%%-*}"
    # A lane branch is `issue-<N>-<slug>`; a slugless `issue-<N>` is accepted too,
    # because it advertises the same window. Anything non-numeric is not a lane
    # branch and is skipped rather than guessed at.
    case "$num" in *[!0-9]*|'') continue ;; esac
    BRANCH_PAIRS="${BRANCH_PAIRS}${num}	${ref}
"
    if ! printf '%s\n' "$BRANCH_ISSUES" | grep -qxF "$num"; then
      BRANCH_ISSUES="${BRANCH_ISSUES}${num}
"
    fi
  done <<EOF
$raw
EOF
  return 0
}

# ---------------------------------------------------------------------------
# FACT (3): held claim refs. ONE network call.
CLAIM_ISSUES=""
scan_claims() {
  local raw line ref num
  if ! raw="$(git ls-remote "$REMOTE" 'refs/claims/issue-*' 2>/dev/null)"; then
    return 1
  fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    ref="$(printf '%s' "$line" | awk '{print $2}')"
    case "$ref" in refs/claims/issue-*) ;; *) continue ;; esac
    num="${ref#refs/claims/issue-}"
    case "$num" in *[!0-9]*|'') continue ;; esac
    CLAIM_ISSUES="${CLAIM_ISSUES}${num}
"
  done <<EOF
$raw
EOF
  return 0
}

# ---------------------------------------------------------------------------
# FACT (1): board Status=Ready, SERVER-SIDE FILTERED (see the header).
READY_ISSUES=""
READY_COUNT=0
READY_PAGE_ROWS=0     # RAW rows the page returned, drafts included (see below)
scan_board() {
  local raw line
  command -v gh >/dev/null 2>&1 || return 1
  # THE jq DELIBERATELY DOES NOT FILTER (#3436 FIX 10). The truncation guard has to
  # compare the RAW PAGE LENGTH against the limit, and a jq `select(.content.number !=
  # null)` hid draft rows from the count: a Ready column of exactly 100 containing 3
  # drafts yielded 97, no `notice=board-page-at-limit` fired, and the run printed
  # `measured=yes` for a page that may have been TRUNCATED — and a truncated Ready column
  # can only ever HIDE rows. So every row is emitted (a draft as the literal `null`),
  # READY_PAGE_ROWS counts them all, and the numeric filter happens below in bash, where
  # it belongs. One API call, no second page read.
  if ! raw="$(gh project item-list "$BOARD_NUMBER" --owner "$BOARD_OWNER" \
                 --query 'status:Ready' --format json -L "$BOARD_LIMIT" \
                 --jq '.items[]|[(.content.type // "null"),(.content.repository // "null"),(.content.number // "null")]|join("|")' 2>/dev/null)"; then
    return 1
  fi
  while IFS= read -r line; do
    line="$(printf '%s' "$line" | tr -d ' \r')"
    [ -n "$line" ] || continue
    READY_PAGE_ROWS=$((READY_PAGE_ROWS + 1))
    # A ROW THIS PARSE DOES NOT RECOGNISE IS UNMEASURABLE, NOT A NON-CANDIDATE
    # (#3436, roborev round 5). `*[!0-9]*) continue` silently dropped ANY unexpected value,
    # so gh schema drift or a malformed `--jq` result produced `measured=yes` with rows
    # HIDDEN — the fail-open direction, in the one tool whose whole contract is that it
    # never claims a clean bill of health. Only two shapes are expected: a decimal issue
    # number, and the literal `null` a DRAFT board item yields for `.content.number`.
    # Anything else nonempty means the board read is not the shape this parse was written
    # for, and the honest answer is that the fact could not be measured.
    # A BOARD NUMBER IS NOT AN ISSUE NUMBER ON THIS REMOTE (#3436, roborev round 14). The row
    # used to be `.content.number` alone, so a Ready PULL REQUEST — or an item from ANOTHER
    # repository — collided with the identically-numbered issue here and produced a candidate
    # for a lane that does not exist. The board legitimately carries all three kinds. So the
    # row now carries type and repository too, and only an ISSUE on the SCANNED remote is a
    # candidate. Everything else is RECOGNISED-and-skipped, which keeps round 5's rule intact:
    # only a shape this parse does not know is UNMEASURABLE.
    row_type="${line%%|*}"; rest="${line#*|}"
    row_repo="${rest%%|*}"; row_num="${rest#*|}"
    case "$row_type" in
      DraftIssue) continue ;;                 # a draft: recognised, not a candidate
      PullRequest) continue ;;                # a PR on the board: recognised, not a candidate
      Issue) ;;                               # the only candidate kind
      *)
        BOARD_UNPARSEABLE_ROW="$line"
        return 1 ;;                           # caller maps this to UNMEASURABLE
    esac
    case "$row_num" in
      null) continue ;;
      *[!0-9]*)
        BOARD_UNPARSEABLE_ROW="$line"
        return 1 ;;
    esac
    [ "$row_repo" = "$BOARD_EXPECT_REPO" ] || continue   # another repo's issue: not ours
    READY_ISSUES="${READY_ISSUES}${row_num}
"
    READY_COUNT=$((READY_COUNT + 1))
  done <<EOF
$raw
EOF
  return 0
}

# probe_lane <issue> — ONE read-only lane-lock probe per row, setting
# ROW_LL_STATE (the probe's OWN verdict/liveness, e.g. `HELD/ALIVE`, or
# `unmeasured(<cause>)`) and ROW_LL_DIR (the lane directory the probe named).
# Never fatal and never blocking: the lane lock is machine-local, so its answer is
# only meaningful ON the box being scanned, and a box that cannot answer must not
# suppress a row whose three facts were all read.
ROW_LL_STATE=""
ROW_LL_DIR=""
probe_lane() {
  local issue="$1" out="" line="" verdict="" liveness="" dir="" rc=0
  ROW_LL_STATE=""
  ROW_LL_DIR=""
  if [ -z "$LANE_LOCK_SH" ] || [ ! -f "$LANE_LOCK_SH" ] || [ ! -r "$LANE_LOCK_SH" ]; then
    ROW_LL_STATE="unmeasured(lane-lock-script-unreadable)"; return 0
  fi
  # BOUNDED when `timeout` exists; a missing timeout is not a reason to skip the
  # field, since the probe reads local files only (no git, no gh, no network).
  if command -v timeout >/dev/null 2>&1; then
    out="$(timeout 20 bash "$LANE_LOCK_SH" probe "$issue" 2>/dev/null)" || rc=$?
  else
    out="$(bash "$LANE_LOCK_SH" probe "$issue" 2>/dev/null)" || rc=$?
  fi
  if [ "$rc" -ne 0 ]; then
    ROW_LL_STATE="unmeasured(probe-exit-$rc)"; return 0
  fi
  line="$(printf '%s\n' "$out" | grep '^LANE-LOCK: ' | head -1)" || line=""
  if [ -z "$line" ]; then
    ROW_LL_STATE="unmeasured(probe-output-unrecognised)"; return 0
  fi
  verdict="$(printf '%s\n' "${line#LANE-LOCK: }" | awk '{print $1}')" || verdict=""
  liveness="$(printf '%s\n' "$line" | tr ' ' '\n' | grep '^liveness=' | head -1)" || liveness=""
  # lane-dir is read as THE REST OF THE LINE, never as a space-delimited word: it is the
  # one field that can legitimately contain spaces (a real filesystem path), and
  # lane-lock.sh emits it LAST for exactly this reason. A word scan truncated
  # `/data/my lanes/lane-N` to `/data/my` (#3436 FIX 9b). `holder-lane-dir=`-style keys
  # cannot answer for it: the match is anchored to the line start or a preceding space.
  case "$line" in
    lane-dir=*)    dir="${line#lane-dir=}" ;;
    *" lane-dir="*) dir="${line#*" lane-dir="}" ;;
    *)             dir="" ;;
  esac
  ROW_LL_STATE="${verdict:-unstated}/${liveness#liveness=}"
  [ -n "${liveness}" ] || ROW_LL_STATE="${verdict:-unstated}/unstated"
  ROW_LL_DIR="$dir"
  return 0
}

branches_of() {
  printf '%s' "$BRANCH_PAIRS" | awk -F'\t' -v n="$1" '$1==n{printf "%s%s", sep, $2; sep=","}'
}

# ---------------------------------------------------------------------------
# MEASURE ALL THREE INPUTS. Any failure is UNMEASURABLE + exit 1, naming the
# input — never a "none found" that reads like an all-clear.
if ! scan_branches; then
  unmeasurable "issue-branches" "git ls-remote --heads $(redact_remote "$REMOTE") 'issue-*' FAILED — the pushed-branch fact could not be read (remote unreachable, auth, or no such remote)"
  exit 1
fi
if ! scan_claims; then
  unmeasurable "claim-refs" "git ls-remote $(redact_remote "$REMOTE") 'refs/claims/issue-*' FAILED — the held-claim fact could not be read (remote unreachable, auth, or no such remote)"
  exit 1
fi
if ! scan_board; then
  # NAME THE RIGHT CAUSE. scan_board fails for two unrelated reasons and reporting the
  # wrong one sends the reader to the wrong problem — the same principle as #3436 AC2
  # ("a collision diagnosed generically sends the reader to the wrong problem"). An
  # unrecognised ROW is schema drift or a bad --jq, not an auth/network fault, and
  # telling someone to fix auth when the board answered fine wastes the whole trip.
  if [ -n "${BOARD_UNPARSEABLE_ROW:-}" ]; then
    unmeasurable "board-status" "the board read returned a row this parse does not recognise: '$BOARD_UNPARSEABLE_ROW' (expected a decimal issue number, or the literal 'null' a DRAFT item yields for .content.number). gh ANSWERED — this is schema drift or a changed --jq, NOT an auth or network fault, so do not go looking at credentials. Treated as UNMEASURABLE rather than skipped, because silently dropping unexpected rows would print measured=yes while HIDING collisions"
    exit 1
  fi
  if command -v gh >/dev/null 2>&1; then
    unmeasurable "board-status" "gh project item-list $BOARD_NUMBER --owner $BOARD_OWNER --query status:Ready FAILED — the board fact could not be read (auth/scope/network). Fix auth; never label-dispatch instead"
  else
    unmeasurable "board-status" "gh is not on PATH — the board fact could not be read at all"
  fi
  exit 1
fi

BOARD_UNPARSEABLE_ROW=""   # set by scan_board when a row is not a recognised shape
BOARD_AT_LIMIT=false
if [ "$READY_PAGE_ROWS" -ge "$BOARD_LIMIT" ]; then
  # A page returned exactly at the limit is the one shape that could still be truncated,
  # and a truncated Ready column can only ever HIDE rows. THE QUANTITY IS THE RAW PAGE
  # LENGTH, not the issue-numbered subset: filtering first made the guard fail OPEN on any
  # page whose limit was reached partly by drafts (#3436 FIX 10).
  BOARD_AT_LIMIT=true
  [ "$AS_JSON" -eq 1 ] || \
    say "SCAN: notice=board-page-at-limit page-rows=$READY_PAGE_ROWS ready=$READY_COUNT limit=$BOARD_LIMIT (the Ready column may be truncated; rows below are still true, absences are not)"
fi

# ---------------------------------------------------------------------------
# REPORT. Candidates come from the BRANCH fact (the cheapest of the three to
# intersect), then both remaining facts must hold.
ROWS=0
for num in $(printf '%s\n' "$BRANCH_ISSUES" | grep -E '^[0-9]+$' | sort -n -u); do
  if [ -n "$ONLY_ISSUE" ] && [ "$num" != "$ONLY_ISSUE" ]; then continue; fi
  # FACT (1): the board offers it as Ready.
  if ! printf '%s\n' "$READY_ISSUES" | grep -qxF "$num"; then continue; fi
  # FACT (3): NOBODY holds the claim ref. A held ref means the window is closed.
  if printf '%s\n' "$CLAIM_ISSUES" | grep -qxF "$num"; then continue; fi
  brs="$(branches_of "$num")"
  probe_lane "$num"
  lls="$ROW_LL_STATE"
  lld="$ROW_LL_DIR"
  ROWS=$((ROWS + 1))
  if [ "$AS_JSON" -eq 1 ]; then
    # `issue` is the only unquoted value here and it is provably numeric (the loop
    # source is `grep -E '^[0-9]+$'`); every string goes through json_str.
    printf '{"issue":%s,"board":"Ready","branches":%s,"claim_ref":"absent","lane_lock":%s,"lane_dir":%s}\n' \
      "$num" "$(json_str "$brs")" "$(json_str "$lls")" "$(json_str "$lld")"
  else
    say "COLLISION-WINDOW: issue=$num board=Ready branches=$brs claim-ref=absent lane-lock=$lls lane-dir=${lld:-unknown}"
  fi
done

BRANCH_ISSUE_COUNT="$(printf '%s\n' "$BRANCH_ISSUES" | grep -cE '^[0-9]+$')" || BRANCH_ISSUE_COUNT=0
VERDICT=NONE-REPORTED
[ "$ROWS" -eq 0 ] || VERDICT=FOUND

if [ "$AS_JSON" -eq 1 ]; then
  # rows/ready/branch_issues are counters this script computed, and
  # board_page_at_limit is the literal `true`/`false` — those stay unquoted NUMBERS and
  # BOOLEANS. `remote` is CLAIM_REMOTE, i.e. caller-supplied, so it is escaped like every
  # other string.
  printf '{"summary":"advertised-collision","result":%s,"rows":%s,"ready":%s,"board_page_rows":%s,"branch_issues":%s,"remote":%s,"board_page_at_limit":%s,"measured":%s}\n' \
    "$(json_str "$VERDICT")" "$ROWS" "$READY_COUNT" "$READY_PAGE_ROWS" "$BRANCH_ISSUE_COUNT" "$(json_str "$(redact_remote "$REMOTE")")" "$BOARD_AT_LIMIT" \
    "$(json_str "$([ "$BOARD_AT_LIMIT" = true ] && printf 'no' || printf 'yes')")"
else
  # `measured=` MUST FOLLOW THE COVERAGE, NOT THE EXIT PATH (#3436, roborev round 11). This
  # printed a flat `measured=yes` even when the Ready page came back AT the limit, i.e. exactly
  # the shape that may have been truncated — and a truncated Ready column can only ever HIDE
  # rows, so `rows=0 measured=yes` asserted full board coverage the scan did not have. The JSON
  # branch already carried board-at-limit; the human line did not, and the human line is what a
  # sweep reads. Same rule as everything else here: a positive verdict needs an affirmative
  # measurement, and "could not see the whole board" is not one.
  say "SCAN: advertised-collision rows=$ROWS ready=$READY_COUNT branch-issues=$BRANCH_ISSUE_COUNT remote=$(redact_remote "$REMOTE") board-at-limit=$BOARD_AT_LIMIT measured=$([ "$BOARD_AT_LIMIT" = true ] && printf 'no' || printf 'yes')"
  if [ "$ROWS" -gt 0 ]; then
    say "SCAN: RESULT=FOUND rows=$ROWS (each row is board Ready AND a pushed issue-<N>-* branch AND no refs/claims/issue-<N>. Remedy runs ON the box holding the lane: 'claim.sh verify <N>', then the documented resume path — never an unguarded create)"
  else
    say "SCAN: RESULT=NONE-REPORTED rows=0 (positive-detection only — exit 1, NEVER 0: this tool does not give a clean bill of health, because a lane it cannot see is indistinguishable from one that is not there)"
  fi
fi

# EXIT 3 = reported, EXIT 1 = nothing reported. There is no exit 0 (see header).
[ "$ROWS" -gt 0 ] && exit 3
exit 1
