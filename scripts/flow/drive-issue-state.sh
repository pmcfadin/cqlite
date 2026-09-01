#!/usr/bin/env bash
#
# drive-issue-state.sh — the OWNERSHIP-STAMPED reader/writer for a lane's durable
# `.drive-issue-state.md` marker (issue #3822).
#
# WHY THIS EXISTS
# ---------------
# `/drive-issue` records durable per-lane state in `.drive-issue-state.md` and
# REHYDRATES from it on every invocation (its Delta 3 / Delta 4). Until this script
# existed the marker had no writer and no reader at all: it was prose, produced and
# consumed by an agent's judgement, and it carried NO ownership stamp. A session
# rehydrating in a shared or REUSED worktree therefore adopted whatever plan it found
# — including a PEER session's — because nothing in the file said whose plan it was.
# An instruction to an LLM cannot be tested; a script can, so the marker is mechanized
# here and the doctrine calls this script.
#
# THE IDENTITY AXES, AND WHY THEY ARE NOT ALL FAIL-CLOSED
# ------------------------------------------------------
# Read literally, "verify the stamp against the current machine/session and fail closed
# on a mismatch" would red EVERY LEGITIMATE RESUME: `/drive-issue`'s Delta 3 cron
# re-invoke is a NEW session id in the SAME lane on the SAME issue, and that resume is
# the marker's intended consumer. A guard that reds on correct input is the guard agents
# learn to waive. So the axes are split by whether they are STABLE across a legitimate
# resume:
#
#   FAIL-CLOSED AXES — issue, machine, worktree.
#     Stable across a resume and DISTINCT across lanes, so a mismatch on any of them
#     means this marker is not about this lane's work. Refused, NAMING THE AXIS
#     (`axis=issue` / `axis=machine` / `axis=worktree`), because the operator's next
#     action differs per axis: a foreign issue means the lane is being reused for other
#     work, a foreign machine means the marker travelled with a pushed branch or a
#     copied tree, a foreign worktree means the file was copied.
#
#   SESSION AXIS — recorded (AC1), but NOT fail-closed on its own.
#     A recorded session id equal to the current one (and recordable) is OWNED outright. So
#     is a recorded pid that is ALIVE and EQUAL to our own session pid with an intersecting
#     start window — a pid is unique among live processes, so that process IS us, and
#     without this a session with CLAUDE_PID set but no CLAUDE_CODE_SESSION_ID would refuse
#     its OWN marker as a live peer. It is an affirmative measurement of sameness (stronger
#     than the id string it stands in for), never a permissive fallback.
#     A session difference alone is resolved by the LIVENESS OF THE RECORDED WRITER,
#     three-valued, on `claim-heartbeat.sh dead-lanes`' precedent:
#       writer provably GONE  -> ADOPTABLE. `verify` still exits NON-ZERO: adoption is an
#                                EXPLICIT gesture (`adopt`), never an implicit inheritance,
#                                and the rewritten stamp records the prior session.
#       writer provably ALIVE -> LIVE-PEER. A live peer owns this lane. `adopt` REFUSES
#                                here too: an adopt that ignores liveness is a mute button
#                                for the whole guard.
#       liveness UNMEASURABLE -> LIVENESS-UNKNOWN. Refused. Never permissive: a positive
#                                verdict requires an AFFIRMATIVE MEASUREMENT, and an
#                                unknown is never read as verified.
#
# WHOSE PID, AND WHY AN UNSET CLAUDE_PID IS *UNKNOWN* RATHER THAN *DEAD*
# ---------------------------------------------------------------------
# The liveness question is about the SESSION, so the recorded pid must be the session's:
# `CLAUDE_PID`. When `CLAUDE_PID` is unset the pid is NOT RECORDABLE as a session-liveness
# token and liveness MUST be UNKNOWN. There is deliberately NO `$$` fallback: `$$` is the
# transient bash running this script, which exits immediately, so a marker stamped with it
# would read as DEAD seconds later and make a LIVE PEER adoptable — the exact
# false-permissive this issue exists to close. The same rule covers the start window: an
# unmeasured window is UNKNOWN, never alive and never gone.
#
# PID REUSE is defeated by recording the pid's START WINDOW alongside it and requiring the
# live pid's window to still INTERSECT the recorded one. The window is measured by
# `process_start_window` from the SHARED library `lib/process-liveness.sh` — the same
# three-valued primitives `claim-heartbeat.sh` uses, sourced rather than reimplemented,
# because a second implementation of those review rounds is a second place to lose them.
# It derives the start from ELAPSED time (`ps -o etimes=`), not a wall-clock string, and
# returns an INTERVAL rather than a point; `/proc/<pid>/stat` field 22 would need a
# clock-tick + boot-time conversion, i.e. a second implementation of a solved problem.
#
# THE STAMP DOES NOT SHARE A TEXT CHANNEL WITH THE BODY
# ----------------------------------------------------
# The marker BODY is free-form author/agent prose (stage notes, question text). Identity
# read out of a shared text channel is forgeable — `claim.sh` records the class where a
# value carrying `actor=` shifted the recorded actor — and the standing ruling (CLAUDE.md,
# #3312) is to REMOVE the shared channel, not to pick a rarer delimiter. So:
#
#   * The stamp is a STRICTLY BOUNDED PROLOGUE. The file's FIRST line is the exact
#     `<!-- drive-issue-state:stamp:v1 -->` sentinel; the stamp is the contiguous run of
#     `key: value` lines at column zero immediately after it; it is terminated by the exact
#     `<!-- drive-issue-state:stamp:end -->` sentinel. The reader parses ONLY that prologue
#     and NEVER greps identity out of the body.
#   * The writer REFUSES a body containing either sentinel as a whole line at column zero —
#     REFUSED, not escaped. A structural source assert cannot see what a RUNTIME value
#     injects, so the check belongs on the OUTPUT path.
#   * At READ time a SECOND column-zero occurrence of either sentinel anywhere in the file
#     is its OWN named refusal (DUPLICATE-SENTINEL), so a hand-edited file cannot smuggle a
#     second stamp in behind the first.
#   * Every stamp value except `worktree` is sanitized to ONE token from a closed charset
#     that excludes space, newline, '<', '>', '!' and '=' — so no value can create a line,
#     reproduce a sentinel, or introduce another `key:` pair. `worktree` is recorded
#     VERBATIM (a path must compare exactly, and sanitizing would alias `/a b` onto `/a-b`),
#     and a worktree path carrying a CONTROL character is refused at write time rather than
#     recorded lossily.
#
# OUTPUT CONTRACT (mirrors scripts/flow/base-staleness.sh)
#   (a) EVERY line of a verdict-bearing invocation, stdout AND stderr, begins
#       `DRIVE-STATE: `. `--help` is the one exemption: it emits no verdict line, so no
#       consumer parses it.
#   (b) Every dynamic field is CONTROL-CHARACTER SANITIZED for display. Load-bearing: git
#       and the filesystem PERMIT NEWLINES IN PATHS, and an unsanitized path printed
#       verbatim emits a second line carrying no prefix, breaking the anchor everything
#       else rests on.
#   (c) The verdict appears ONLY on a `DRIVE-STATE: verdict ` line and is a single token
#       from the CLOSED set below; prose goes on `verdict-detail` lines. An unrecognised
#       token is not a thing this script can print — the set is the grammar.
#
# VERDICT TOKENS (closed set)
#   OWNED               this lane, this issue, this session — proceed
#   WRITTEN             the marker was written/replaced
#   ADOPTED             ownership transferred to this session (prior session recorded)
#   SHOWN               fields printed; asserts NOTHING about ownership
#   ABSENT              no marker at all — a legitimate FRESH START, not a refusal
#   UNSTAMPED           a marker exists but carries no stamp prologue (the pre-#3822 shape)
#   MALFORMED           the prologue is unterminated, mis-shaped, duplicated or incomplete
#   DUPLICATE-SENTINEL  a second stamp sentinel at column zero
#   FOREIGN-ISSUE       fail-closed axis mismatch (axis=issue)
#   FOREIGN-MACHINE     fail-closed axis mismatch (axis=machine)
#   FOREIGN-WORKTREE    fail-closed axis mismatch (axis=worktree)
#   ADOPTABLE           session differs and the recorded writer is provably GONE
#   LIVE-PEER           session differs and the recorded writer is provably ALIVE
#   LIVENESS-UNKNOWN    session differs and liveness could NOT be measured
#   ERROR               an I/O or internal failure — nothing was decided
#
# SUBCOMMANDS
#   write  <N> [--stage <s>] [--request-id <r>] [--pr <n>] [--branch <b>]
#              [--body-file <path>] [--actor <id>]
#              Write/replace the marker in the CURRENT worktree. Writing OVER an existing
#              marker first passes the SAME ownership verification `verify` applies — you
#              may not overwrite a live peer's plan either — so a foreign marker refuses
#              with that marker's own verdict. The route past it is `adopt`, never a flag
#              on `write`. Without --body-file an OWNED marker's body is PRESERVED.
#              ONE EXCEPTION — the MIGRATION case: an UNSTAMPED marker (the pre-#3822
#              shape) asserts NO ownership, so `write` REPLACES it, DISCARDS its body (an
#              unstamped plan may belong to any session and is never carried forward) and
#              ANNOUNCES the discard on a verdict-detail line. `verify` still REFUSES an
#              unstamped marker; `write` is the one door, and it is the door the UNSTAMPED
#              refusal names. MALFORMED / DUPLICATE-SENTINEL get no exception: they CLAIM
#              an identity that cannot be READ, which may be a live peer's, so a human
#              moves the file aside deliberately and `write` then takes the ABSENT path.
#   verify <N> [--actor <id>]      the rehydrate gate (see the axes above)
#   adopt  <N> --reason <why> [--actor <id>]
#              The explicit adopt gesture. Resolves the SESSION axis ONLY: a fail-closed
#              axis mismatch still refuses, and so does a live or unmeasurable writer.
#              --reason is REQUIRED and RECORDED. An empty/whitespace reason, one with
#              nothing recordable in it, a bare PLACEHOLDER (`why`, `todo`, `tbd`, …) or one
#              still carrying an UNSUBSTITUTED `<…>` is a USAGE error (64), never a silent
#              `reason=unspecified` — same gate, and same reasons, as claim.sh's.
#   show   <N> print the recorded stamp fields. No ownership verdict.
#   --help     this contract (authoritative)
#
# IDENTITY
#   issue     the issue number the marker is about.
#   machine   CLAIM_MACHINE, else `hostname -s`, SANITIZED — deliberately the SAME notion
#             `claim.sh` records (same env var, same default, same sanitizer), so a lane's
#             claim ref and its state marker cannot disagree about which box holds it.
#             claim.sh cannot be SOURCED (sourcing runs its dispatch), so the definition is
#             mirrored here and the AGREEMENT is pinned BEHAVIOURALLY by
#             scripts/tests/test_drive_issue_state.sh, which extracts claim.sh's own
#             definition and compares the two in one environment.
#   worktree  `pwd -P` — the physical directory holding the marker.
#   session   CLAUDE_CODE_SESSION_ID, sanitized. `unrecorded` when unset, which makes the
#             session axis UNMEASURED and routes to the liveness resolution.
#   actor     --actor, else CLAIM_ACTOR, else `flow` — as claim.sh resolves it. NOTE: the
#             actor is RECORDED but is NOT an ownership axis here; #3810 (claim-actor
#             collision) is a separate, open issue and is not fixed or widened by this file.
#
# ENV
#   CLAIM_MACHINE           machine identity (default `hostname -s`) — shared with claim.sh
#   CLAIM_ACTOR             default actor when --actor is omitted (default: flow)
#   CLAUDE_CODE_SESSION_ID  the current session id (recorded; the session axis)
#   CLAUDE_PID              the SESSION's pid (the liveness token; no $$ fallback)
#   There is deliberately NO env override for the marker path or the start-window slack:
#   a knob that widens the guard is settable by the party the guard constrains.
#
# EXIT CODES
#   0   OWNED / WRITTEN / ADOPTED / SHOWN
#   1   ERROR — I/O or internal failure; nothing was decided
#   3   ABSENT — no marker; a legitimate fresh start (textually AND numerically distinct
#       from every refusal, so a caller can tell "nothing to resume" from "refused")
#   4   FOREIGN-ISSUE / FOREIGN-MACHINE / FOREIGN-WORKTREE
#   5   ADOPTABLE — session differs, writer provably gone; run `adopt`
#   6   LIVE-PEER — a live peer owns this lane; do NOT proceed
#   7   LIVENESS-UNKNOWN — could not measure; do NOT proceed
#   8   UNSTAMPED / MALFORMED / DUPLICATE-SENTINEL
#   64  usage error
#
# CONSTRAINTS
#   macOS bash 3.2 compatible (no associative arrays, no readarray/mapfile). No network, no
#   gh, no git. `set -euo pipefail`, shellcheck-clean.
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"
SCRIPT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

P='DRIVE-STATE:'

MARKER_NAME='.drive-issue-state.md'
STAMP_BEGIN='<!-- drive-issue-state:stamp:v1 -->'
STAMP_END='<!-- drive-issue-state:stamp:end -->'

# Rounding tolerance when intersecting the recorded start window with the live one. Both
# come from the SAME host and the SAME clock (`now - elapsed`, bracketed), so the only
# error to absorb is second-resolution rounding on each side. HARD-CODED with no env
# override on purpose: widening it is exactly what a caller wanting to adopt a live peer's
# lane would do, and the constrained party must not choose its own enforcer.
START_SLACK_SECS=2

# ---------------------------------------------------------------------------
# Output. Contract (a): every line, stdout and stderr, carries the ONE prefix.
# ---------------------------------------------------------------------------
emit()       { printf '%s %s\n' "$P" "$*"; }
note()       { printf '%s note %s\n' "$P" "$*" >&2; }
die_usage()  { printf '%s USAGE %s\n' "$P" "$*" >&2; exit 64; }

# sane <string> — the string with every C0 control character and DEL replaced by '?'.
# Applied to EVERY dynamic field before it is printed (contract (b)). This is the
# load-bearing half of the anchor: a path or a hand-edited field value may contain a
# NEWLINE, and printed verbatim that emits a second line with no `DRIVE-STATE: ` prefix.
# Control characters ONLY are masked; everything else is printed verbatim, because
# mangling a path for the reader buys nothing once the anchor holds.
sane() {
  printf '%s' "${1:-}" | LC_ALL=C tr -c '\040-\176\200-\377' '?'
}

# verdict <TOKEN> — contract (c): the ONE verdict line, one closed-set token, nothing else.
verdict() { printf '%s verdict %s\n' "$P" "$1"; }
detail()  { printf '%s verdict-detail %s\n' "$P" "$*"; }

# refuse <TOKEN> <exit-code> <detail...> — emit a named refusal and exit.
refuse() {
  local token="$1" code="$2"; shift 2
  verdict "$token"
  [ "$#" -eq 0 ] || detail "$@"
  exit "$code"
}

print_help() {
  awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2 {sub(/^# ?/,""); print}' "$0"
}

# TEMP FILES ARE REGISTERED THE MOMENT THEY EXIST, and the trap is installed BEFORE any of
# them is created — otherwise a refusal between `mktemp` and `mv` leaves a stray
# `.drive-issue-state.md.XXXXXX` in the lane, which is exactly the kind of unexplained file
# a later session reads as state. The handlers cover signals too: bash runs no EXIT trap
# for a signal with its default disposition.
TMP_FILES=''
cleanup_tmp() {
  local f
  for f in $TMP_FILES; do [ -z "$f" ] || rm -f "$f"; done
  TMP_FILES=''
}
trap cleanup_tmp EXIT
trap 'cleanup_tmp; exit 130' INT
trap 'cleanup_tmp; exit 143' TERM
trap 'cleanup_tmp; exit 129' HUP

# register_tmp <path> — remember a temp file for cleanup. Paths here are always mktemp
# output (no spaces, no newlines), so a space-separated list is safe.
register_tmp() { TMP_FILES="$TMP_FILES $1"; }

# The shared three-valued process-liveness primitives (#3822). A MISSING library is fatal
# and NAMED — never a silent continue with the predicates undefined, which would make
# every liveness answer an empty string and (worse) could read as "gone".
[ -r "$SCRIPT_HOME/lib/process-liveness.sh" ] || {
  printf '%s ERROR cannot read %s/lib/process-liveness.sh (the shared process-liveness primitives) — NOTHING was measured\n' "$P" "$SCRIPT_HOME" >&2
  exit 1
}
# shellcheck source=lib/process-liveness.sh
. "$SCRIPT_HOME/lib/process-liveness.sh"

# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

# sanitize_field <text> — collapse a free-text value into ONE parseable token. MIRRORS
# claim.sh's function of the same name, deliberately and verbatim in behaviour (see
# IDENTITY in the header for why it is mirrored rather than sourced, and where the
# agreement is pinned). Keeps [A-Za-z0-9._:/#-]; note that space, newline, '=', '<', '>'
# and '!' are all dropped, which is what makes a stamp value structurally unable to create
# a line, a sentinel or another `key:` pair. Collapse -> trim -> cut(120) -> re-trim: the
# cut happens BEFORE the final trim because trimming first can re-introduce the trailing
# separator the trim promised to remove. LC_ALL=C on both tr and sed is load-bearing —
# BSD/macOS tr aborts on non-ASCII input under a UTF-8 locale, and under `set -e` that
# failure inside a command substitution kills the script with no verdict line at all.
sanitize_field() {
  local s
  s="$(printf '%s' "${1:-}" | LC_ALL=C tr -c 'A-Za-z0-9._:/#-' '-' | LC_ALL=C sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"
  s="$(LC_ALL=C printf '%.120s' "$s")"
  s="${s%-}"
  [ -n "$s" ] || s="unspecified"
  printf '%s\n' "$s"
}

this_machine() { sanitize_field "${CLAIM_MACHINE:-$(hostname -s)}"; }

# this_session — the current session id, or the `unrecorded` sentinel. An unrecorded
# session makes the session axis UNMEASURED (never "equal"), which routes to the liveness
# resolution rather than to a false OWNED.
this_session() {
  if [ -n "${CLAUDE_CODE_SESSION_ID:-}" ]; then
    sanitize_field "$CLAUDE_CODE_SESSION_ID"
  else
    printf 'unrecorded\n'
  fi
}

# this_session_pid — the SESSION's pid, or the `unrecordable` sentinel. NO `$$` FALLBACK:
# see the header. `$$` is this transient bash, so recording it would make a live peer
# adoptable seconds later.
this_session_pid() {
  case "${CLAUDE_PID:-}" in
    '' | *[!0-9]*) printf 'unrecordable\n' ;;
    *) if [ "${CLAUDE_PID}" -gt 0 ] 2>/dev/null; then printf '%s\n' "$CLAUDE_PID"
       else printf 'unrecordable\n'; fi ;;
  esac
}

resolve_actor() { sanitize_field "${1:-${CLAIM_ACTOR:-flow}}"; }

require_numeric_issue() {
  case "${1:-}" in
    *[!0-9]* | '') die_usage "$2 requires a numeric issue number (got '$(sane "${1:-<none>}")')" ;;
  esac
}

# ---------------------------------------------------------------------------
# Marker reading. Sets S_* globals. Refuses (and exits) on any structural fault.
# ---------------------------------------------------------------------------
S_issue=''; S_machine=''; S_worktree=''; S_session=''; S_pid=''
S_start_lo=''; S_start_hi=''; S_actor=''; S_ts=''; S_stage=''
S_prior_session=''

marker_path() { printf '%s/%s\n' "$(pwd -P)" "$MARKER_NAME"; }

# count_sentinel <path> <sentinel> — column-zero, WHOLE-LINE occurrences. `grep -Fx` is
# what makes this a column-zero test: a sentinel appearing inside a `key: value` value or
# mid-sentence in the body is not a whole line and cannot pose as a stamp boundary.
count_sentinel() {
  local n
  n="$(LC_ALL=C grep -Fxc -- "$2" "$1" 2>/dev/null || true)"
  case "$n" in '' | *[!0-9]*) n=0 ;; esac
  printf '%s\n' "$n"
}

# read_marker — parse the stamp prologue of the marker in the current worktree.
# Exits with a named refusal on every structural fault; returns 0 with S_* set otherwise.
read_marker() {
  local path; path="$(marker_path)"
  [ -e "$path" ] || refuse ABSENT 3 "no $MARKER_NAME in $(sane "$(pwd -P)") — nothing to resume; this is a legitimate FRESH START, not a refusal"
  [ -f "$path" ] && [ -r "$path" ] || refuse ERROR 1 "$(sane "$path") exists but is not a readable regular file — nothing was decided"

  local first
  first="$(head -1 "$path" 2>/dev/null || true)"
  if [ "$first" != "$STAMP_BEGIN" ]; then
    refuse UNSTAMPED 8 "$(sane "$path") carries NO ownership stamp (its first line is not the stamp sentinel) — this is the pre-#3822 marker shape, whose plan could belong to ANY session on ANY machine, so nothing is READ from it. The route forward is '$prog write <issue>', which SUCCEEDS over an unstamped marker and REPLACES it — DISCARDING its body, because an unstamped plan may belong to any session and is never carried forward. Save anything you need out of the file first."
  fi

  local nb ne
  nb="$(count_sentinel "$path" "$STAMP_BEGIN")"
  ne="$(count_sentinel "$path" "$STAMP_END")"
  if [ "$nb" -ne 1 ] || [ "$ne" -gt 1 ]; then
    refuse DUPLICATE-SENTINEL 8 "$(sane "$path") carries $nb stamp-begin and $ne stamp-end sentinels at column zero (exactly one of each is legal) — a second stamp cannot be told apart from the first, so no identity is read from this file. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)"
  fi
  [ "$ne" -eq 1 ] || refuse MALFORMED 8 "$(sane "$path") has no stamp-end sentinel at column zero — the prologue is unterminated, so its extent (and therefore which lines are identity) is undecidable. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)"

  # Parse the CONTIGUOUS run of `key: value` lines between the two sentinels, and NOTHING
  # else in the file. `IFS=` + `-r` keeps each line byte-exact.
  local line key val ln=0 dup=''
  while IFS= read -r line; do
    ln=$((ln + 1))
    [ "$ln" -eq 1 ] && continue          # the begin sentinel, already verified
    [ "$line" = "$STAMP_END" ] && break
    case "$line" in
      [a-z]*': '*) : ;;
      *) refuse MALFORMED 8 "$(sane "$path") line $ln is inside the stamp prologue but is not a 'key: value' line at column zero — the prologue grammar is closed, so a line it cannot parse is a refusal rather than a guess. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)" ;;
    esac
    key="${line%%: *}"
    val="${line#*: }"
    case "$key" in
      *[!a-z0-9-]*) refuse MALFORMED 8 "$(sane "$path") line $ln has a stamp key outside [a-z0-9-] ('$(sane "$key")'). The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)" ;;
    esac
    [ -n "$val" ] || refuse MALFORMED 8 "$(sane "$path") line $ln records an EMPTY value for '$(sane "$key")'. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)"
    case "$key" in
      issue)                      dup="$S_issue";          S_issue="$val" ;;
      machine)                    dup="$S_machine";        S_machine="$val" ;;
      worktree)                   dup="$S_worktree";       S_worktree="$val" ;;
      session)                    dup="$S_session";        S_session="$val" ;;
      session-pid)                dup="$S_pid";            S_pid="$val" ;;
      session-pid-start-earliest) dup="$S_start_lo";       S_start_lo="$val" ;;
      session-pid-start-latest)   dup="$S_start_hi";       S_start_hi="$val" ;;
      actor)                      dup="$S_actor";          S_actor="$val" ;;
      ts)                         dup="$S_ts";             S_ts="$val" ;;
      stage)                      dup="$S_stage";          S_stage="$val" ;;
      prior-session)              dup="$S_prior_session";  S_prior_session="$val" ;;
      *) dup='' ;;   # forward compatibility: an unrecognised KEY is ignored, but its
                     # SHAPE was still enforced above, so it can never smuggle in a line.
    esac
    # A DUPLICATE identity key would let the PARSER's choice decide identity, which is the
    # forgery shape this whole prologue exists to remove. Refused, not first-wins.
    [ -z "$dup" ] || refuse MALFORMED 8 "$(sane "$path") records '$(sane "$key")' TWICE in the stamp prologue — which occurrence is the identity is undecidable. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)"
  done <"$path"

  local missing=''
  [ -n "$S_issue" ]    || missing="$missing issue"
  [ -n "$S_machine" ]  || missing="$missing machine"
  [ -n "$S_worktree" ] || missing="$missing worktree"
  [ -n "$S_session" ]  || missing="$missing session"
  [ -n "$S_pid" ]      || missing="$missing session-pid"
  [ -n "$S_start_lo" ] || missing="$missing session-pid-start-earliest"
  [ -n "$S_start_hi" ] || missing="$missing session-pid-start-latest"
  [ -n "$S_actor" ]    || missing="$missing actor"
  [ -n "$S_ts" ]       || missing="$missing ts"
  [ -z "$missing" ] || refuse MALFORMED 8 "$(sane "$path") stamp prologue is missing required field(s):$(sane "$missing") — an incomplete stamp is not a weaker stamp, it is no stamp. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)"
  return 0
}

# marker_body — the marker's body (everything AFTER the end sentinel) on stdout.
marker_body() {
  local path; path="$(marker_path)"
  awk -v s="$STAMP_END" 'seen{print} $0==s{seen=1}' "$path"
}

# ---------------------------------------------------------------------------
# Liveness of the RECORDED writer. Sets LIVE_STATE (gone|alive|unknown) + LIVE_DETAIL.
# ---------------------------------------------------------------------------
LIVE_STATE=''; LIVE_DETAIL=''

writer_liveness() {
  LIVE_STATE=unknown; LIVE_DETAIL=''
  local pid="$S_pid" lo="$S_start_lo" hi="$S_start_hi"

  case "$pid" in
    '' | *[!0-9]*)
      LIVE_DETAIL="the stamp records session-pid=$(sane "$pid"), which is not a pid — the writing session's liveness is UNMEASURABLE. It is reported UNKNOWN and NOT 'gone': recording this script's own \$\$ instead would make a LIVE peer read as dead."
      return 0 ;;
  esac
  case "$lo$hi" in
    '' | *[!0-9]*)
      LIVE_DETAIL="the stamp records no measured start window for pid $(sane "$pid") (earliest=$(sane "$lo") latest=$(sane "$hi")), so a pid that is alive NOW cannot be shown to be the process that stamped this marker — PID REUSE is indistinguishable, hence UNKNOWN"
      return 0 ;;
  esac
  if ! ps_usable; then
    LIVE_DETAIL="\`ps\` cannot answer an existence question on this host (it failed even for our own pid), so nothing about pid $(sane "$pid") was measured"
    return 0
  fi

  local presence state cur ce cl
  presence="$(process_presence "$pid")"
  case "$presence" in
    absent)
      LIVE_STATE=gone
      LIVE_DETAIL="pid $(sane "$pid") is absent from the process table and the independent signal probe affirms it (ESRCH) — the writing session is gone"
      return 0 ;;
    unknown)
      LIVE_DETAIL="the process-table probes DISAGREE about pid $(sane "$pid") (our view is not self-consistent — e.g. hidepid, or a transient probe failure), so neither alive nor gone was established"
      return 0 ;;
  esac

  state="$(process_state_class "$pid")"
  case "$state" in
    zombie)
      LIVE_STATE=gone
      LIVE_DETAIL="pid $(sane "$pid") is a ZOMBIE — visible to ps and /proc but not running; the writing session is gone"
      return 0 ;;
    unreadable)
      LIVE_DETAIL="pid $(sane "$pid") is present but its process STATE could not be read — neither a confirmed zombie nor a confirmed live process"
      return 0 ;;
  esac

  cur="$(process_start_window "$pid")"
  if [ -z "$cur" ]; then
    LIVE_DETAIL="pid $(sane "$pid") is running but its start time could not be measured, so it cannot be shown to be the process that stamped this marker"
    return 0
  fi
  ce="${cur%% *}"; cl="${cur##* }"
  # Same process <=> the recorded interval and the live one INTERSECT (both are
  # `now - elapsed` brackets from this host's clock; the slack absorbs per-side rounding).
  if [ "$((ce - START_SLACK_SECS))" -le "$hi" ] && [ "$((cl + START_SLACK_SECS))" -ge "$lo" ]; then
    LIVE_STATE=alive
    LIVE_DETAIL="pid $(sane "$pid") is running and its start window [$ce,$cl] still intersects the recorded [$(sane "$lo"),$(sane "$hi")] — it IS the session that stamped this marker"
  else
    LIVE_STATE=gone
    LIVE_DETAIL="pid $(sane "$pid") is running but its start window [$ce,$cl] is DISJOINT from the recorded [$(sane "$lo"),$(sane "$hi")] — the pid was REUSED, so the writing session is gone and this live process is unrelated"
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Ownership verification, shared by verify / write / adopt.
# `check_ownership <issue> <mode>` where mode = strict (verify/write) | adopt.
# Returns 0 for OWNED. Every other outcome is a refusal that EXITS.
# ---------------------------------------------------------------------------
check_ownership() {
  local issue="$1" mode="$2"
  local machine session
  machine="$(this_machine)"; session="$(this_session)"

  read_marker

  # FAIL-CLOSED AXES first, and they are fail-closed in `adopt` too: adopt resolves the
  # SESSION axis only.
  [ "$S_issue" = "$issue" ] || refuse FOREIGN-ISSUE 4 "axis=issue recorded=$(sane "$S_issue") current=$(sane "$issue") — $(sane "$(marker_path)") is the durable state of a DIFFERENT issue; this lane is being reused. Move or remove it deliberately; it is never adopted for issue $(sane "$issue")."
  [ "$S_machine" = "$machine" ] || refuse FOREIGN-MACHINE 4 "axis=machine recorded=$(sane "$S_machine") current=$(sane "$machine") — this marker was stamped on another box (a copied tree, or a marker that travelled with a branch). Machine identity is not transferable, so it is refused rather than adopted."
  [ "$S_worktree" = "$(pwd -P)" ] || refuse FOREIGN-WORKTREE 4 "axis=worktree recorded=$(sane "$S_worktree") current=$(sane "$(pwd -P)") — this marker was stamped for a different worktree and has been COPIED here; a peer lane's plan is not this lane's."

  # SESSION AXIS. Equal AND recordable => OWNED. Anything else is liveness-resolved.
  if [ "$S_session" = "$session" ] && [ "$session" != unrecorded ]; then
    return 0
  fi

  writer_liveness

  # SAME PROCESS => OWNED, EVEN WHEN THE SESSION ID DOES NOT MATCH. A pid is unique among
  # LIVE processes, so a recorded pid that is alive AND equal to our own session pid (with
  # its start window still intersecting) is not a peer — it IS us. Without this branch a
  # session with CLAUDE_PID set but CLAUDE_CODE_SESSION_ID UNSET wrote a marker and then
  # refused its OWN marker as a LIVE-PEER on the very next command: a guard that reds on
  # correct input, which is the guard agents learn to waive. This is an AFFIRMATIVE
  # measurement of sameness (pid identity + start-window intersection), strictly stronger
  # than the session-id string it stands in for — never a fallback to permissiveness.
  if [ "$LIVE_STATE" = alive ] && [ "$S_pid" = "$(this_session_pid)" ]; then
    return 0
  fi

  local why="axis=session recorded=$(sane "$S_session") current=$(sane "$session"); $LIVE_DETAIL"
  case "$LIVE_STATE" in
    alive)
      refuse LIVE-PEER 6 "$why. A LIVE PEER owns this lane — do NOT proceed and do NOT adopt: an adopt that ignores liveness is a mute button for this guard." ;;
    unknown)
      refuse LIVENESS-UNKNOWN 7 "$why. Liveness was NOT measured, and an unmeasured liveness is never read as verified — refusing. Resolve the ambiguity (identify the session, or clear the marker deliberately) rather than adopting on unproven information." ;;
    gone)
      if [ "$mode" = adopt ]; then
        return 0
      fi
      refuse ADOPTABLE 5 "$why. The recorded writer is provably GONE, so this lane IS adoptable — but adoption is an EXPLICIT gesture: run '$prog adopt $(sane "$issue") --reason <what the resume is>', which rewrites the stamp and records the prior session." ;;
    *)
      refuse ERROR 1 "internal: liveness state '$(sane "$LIVE_STATE")' is not one of gone|alive|unknown — nothing was decided" ;;
  esac
}

# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

# stamp_body_is_safe <body-file> — REFUSE (exit 64) a body carrying either sentinel as a
# whole line at column zero. On the OUTPUT path deliberately: a structural source assert
# cannot see what a RUNTIME value injects. Refused, never escaped — an escaped sentinel is
# a second grammar, and two grammars is how a reader and a writer come to disagree.
assert_body_safe() {
  local f="$1" s
  for s in "$STAMP_BEGIN" "$STAMP_END"; do
    if LC_ALL=C grep -Fxq -- "$s" "$f" 2>/dev/null; then
      die_usage "the body carries a stamp sentinel as a whole line at column zero. It is REFUSED, not escaped: the stamp prologue is the only place identity is read from, and a body line that can pose as a boundary would break that. Remove the line (indent it, or drop the '<!--' comment) and retry. NOTHING was written."
    fi
  done
}

# write_marker <issue> <actor> <body-file|''> [<extra key: value lines>...]
#
# Reports through GLOBALS (WROTE_PATH on success, WRITE_ERR + return 1 on failure) rather
# than through stdout, and is therefore NEVER called inside a command substitution. A
# `refuse` inside `$( )` exits only the SUBSHELL and its verdict line is CAPTURED by the
# caller's variable — so the run would end up with no verdict on stdout and a verdict
# string inside a path. Every emit site in this script is in the main shell for that reason.
WROTE_PATH=''; WRITE_ERR=''
write_marker() {
  local issue="$1" actor="$2" bodyfile="$3"; shift 3
  local wt path tmp ts session pid win lo hi
  wt="$(pwd -P)"
  path="$(marker_path)"

  # The worktree path is recorded VERBATIM (a path must compare EXACTLY; sanitizing would
  # alias '/a b' onto '/a-b' and let two lanes verify each other's markers). A path
  # carrying a CONTROL character cannot be recorded losslessly on one line, so it is
  # refused rather than recorded wrongly.
  case "$wt" in
    *[[:cntrl:]]*) WRITE_ERR="the worktree path contains a control character, so it cannot be recorded on one line as the worktree axis — refusing to write a stamp whose identity would be lossy"; return 1 ;;
    /*) : ;;
    *)  WRITE_ERR="the worktree path '$(sane "$wt")' is not absolute — refusing to write a stamp whose worktree axis cannot be compared"; return 1 ;;
  esac

  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  session="$(this_session)"
  pid="$(this_session_pid)"
  lo=unmeasured; hi=unmeasured
  if [ "$pid" != unrecordable ]; then
    win="$(process_start_window "$pid" || true)"
    if [ -n "$win" ]; then lo="${win%% *}"; hi="${win##* }"; fi
  fi

  tmp="$(mktemp "$path.XXXXXX")" || {
    WRITE_ERR="cannot create a temporary file next to $(sane "$path") — nothing was written"; return 1; }
  register_tmp "$tmp"
  {
    printf '%s\n' "$STAMP_BEGIN"
    printf 'issue: %s\n' "$issue"
    printf 'machine: %s\n' "$(this_machine)"
    printf 'worktree: %s\n' "$wt"
    printf 'session: %s\n' "$session"
    printf 'session-pid: %s\n' "$pid"
    printf 'session-pid-start-earliest: %s\n' "$lo"
    printf 'session-pid-start-latest: %s\n' "$hi"
    printf 'actor: %s\n' "$actor"
    printf 'ts: %s\n' "$ts"
    local kv
    for kv in "$@"; do [ -z "$kv" ] || printf '%s\n' "$kv"; done
    printf '%s\n' "$STAMP_END"
    printf '\n'
    if [ -n "$bodyfile" ] && [ -s "$bodyfile" ]; then cat "$bodyfile"; fi
  } >"$tmp" || { rm -f "$tmp"; WRITE_ERR="failed writing the stamp to $(sane "$tmp") — nothing was replaced"; return 1; }
  mv -f "$tmp" "$path" || { rm -f "$tmp"; WRITE_ERR="failed replacing $(sane "$path")"; return 1; }
  WROTE_PATH="$path"
  return 0
}

# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------
cmd_write() {
  local issue="${1:-}"; shift || true
  require_numeric_issue "$issue" write
  local stage='' request='' pr='' branch='' bodyfile='' actor_raw=''
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --stage)      stage="${2:-}";      shift 2 || die_usage "--stage needs a value" ;;
      --request-id) request="${2:-}";    shift 2 || die_usage "--request-id needs a value" ;;
      --pr)         pr="${2:-}";         shift 2 || die_usage "--pr needs a value" ;;
      --branch)     branch="${2:-}";     shift 2 || die_usage "--branch needs a value" ;;
      --body-file)  bodyfile="${2:-}";   shift 2 || die_usage "--body-file needs a value" ;;
      --actor)      actor_raw="${2:-}";  shift 2 || die_usage "--actor needs a value" ;;
      *) die_usage "write: unknown option '$(sane "$1")'" ;;
    esac
  done
  local actor; actor="$(resolve_actor "$actor_raw")"

  local body_src=''
  if [ -n "$bodyfile" ]; then
    [ -r "$bodyfile" ] || die_usage "--body-file '$(sane "$bodyfile")' is not readable — nothing was written"
    assert_body_safe "$bodyfile"
    body_src="$bodyfile"
  fi

  # Writing OVER an existing marker passes the SAME ownership verification: you may not
  # overwrite a live peer's plan either. ABSENT is the legitimate fresh-start path.
  #
  # ONE EXCEPTION, AND IT IS THE MIGRATION CASE: an UNSTAMPED marker. It asserts NO
  # ownership at all — that is what makes it the pre-#3822 shape — so refusing to replace it
  # protects no identifiable party while BRICKING the only route forward. On rollout EVERY
  # existing lane holds an unstamped marker, so the refusal made the whole marker path a
  # dead letter fleet-wide, with the refusal text naming the very command that refused (the
  # #3312-job-24 shape: a break-glass no sequence of actions can reach). So `write` REPLACES
  # it and DISCARDS its body — silently carrying a foreign plan forward is the defect this
  # issue exists to close — and ANNOUNCES the discard, because a quiet overwrite of someone's
  # notes is unacceptable even where refusing is worse. A MALFORMED or DUPLICATE-SENTINEL
  # marker gets NO such exception: it CLAIMS an identity that merely cannot be read, and an
  # unreadable identity may be a live peer's, so it is moved aside by a human deliberately.
  local carried='' discarded=''
  if [ -e "$(marker_path)" ]; then
    local mpath; mpath="$(marker_path)"
    [ -f "$mpath" ] && [ -r "$mpath" ] || refuse ERROR 1 "$(sane "$mpath") exists but is not a readable regular file — nothing was decided"
    if [ "$(head -1 "$mpath" 2>/dev/null || true)" != "$STAMP_BEGIN" ]; then
      local dl dbytes
      dl="$(LC_ALL=C wc -l <"$mpath" 2>/dev/null | tr -d ' ')"
      dbytes="$(LC_ALL=C wc -c <"$mpath" 2>/dev/null | tr -d ' ')"
      discarded="replaced an UNSTAMPED marker of unknown provenance and DISCARDED its body (${dl:-?} lines, ${dbytes:-?} bytes): an unstamped plan may belong to ANY session, so it is never carried forward"
      # body_src stays as the caller supplied it (empty unless --body-file): the preserve
      # branch below is UNREACHABLE from here, which is the point.
    else
      check_ownership "$issue" strict
      if [ -z "$body_src" ]; then
      # Preserve an OWNED marker's body: `write` updates the stamp and the recorded stage,
      # not the author's notes.
        carried="$(mktemp "${TMPDIR:-/tmp}/drive-issue-body.XXXXXX")" || refuse ERROR 1 "cannot create a temporary file for the carried body"
        register_tmp "$carried"
        marker_body >"$carried"
        assert_body_safe "$carried"
        body_src="$carried"
      fi
    fi
  fi

  # Optional fields are built into a positional list first: an empty one contributes
  # NOTHING (write_marker skips empty extras), so an omitted --stage leaves no `stage:`
  # line rather than an empty-valued one the reader would refuse as MALFORMED.
  local f_stage='' f_request='' f_pr='' f_branch=''
  [ -z "$stage" ]   || f_stage="stage: $(sanitize_field "$stage")"
  [ -z "$request" ] || f_request="request-id: $(sanitize_field "$request")"
  [ -z "$pr" ]      || f_pr="pr: $(sanitize_field "$pr")"
  [ -z "$branch" ]  || f_branch="branch: $(sanitize_field "$branch")"
  if ! write_marker "$issue" "$actor" "$body_src" "$f_stage" "$f_request" "$f_pr" "$f_branch"; then
    [ -z "$carried" ] || rm -f "$carried"
    refuse ERROR 1 "$WRITE_ERR"
  fi
  [ -z "$carried" ] || rm -f "$carried"
  verdict WRITTEN
  [ -z "$discarded" ] || detail "$discarded"
  detail "issue=$(sane "$issue") machine=$(sane "$(this_machine)") worktree=$(sane "$(pwd -P)") session=$(sane "$(this_session)") session-pid=$(sane "$(this_session_pid)") actor=$(sane "$actor") -> $(sane "$WROTE_PATH")"
}

cmd_verify() {
  local issue="${1:-}"; shift || true
  require_numeric_issue "$issue" verify
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor) shift 2 || die_usage "--actor needs a value" ;;
      *) die_usage "verify: unknown option '$(sane "$1")'" ;;
    esac
  done
  check_ownership "$issue" strict
  verdict OWNED
  detail "issue=$(sane "$issue") machine=$(sane "$S_machine") worktree=$(sane "$S_worktree") session=$(sane "$S_session") stage=$(sane "${S_stage:-none}") ts=$(sane "$S_ts") — this lane's marker, this session; resume from the recorded stage"
}

# assert_reason <raw> — claim.sh's `--reason` gate, same rules and same reasons. An
# UNSUBSTITUTED template is refused on the RAW text (before sanitization), because
# `resume:<branch>` sanitizes to a non-sentinel token and would otherwise record an
# unresolved placeholder as the audit reason. These commands are read by agents that run
# printed text LITERALLY.
assert_reason() {
  local raw="$1" tok
  case "$raw" in
    *'<'*'>'*) die_usage "adopt: --reason '$(sane "$raw")' still carries an UNSUBSTITUTED placeholder (<…>) — substitute it, e.g. --reason cron-reinvoke:writer-pid-gone" ;;
  esac
  tok="$(sanitize_field "$raw")"
  if [ "$tok" = unspecified ] || [ "${#tok}" -lt 3 ]; then
    die_usage "adopt: --reason must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '$(sane "$raw")' records as '$(sane "$tok")', which is indistinguishable from no reason at all"
  fi
  case "$(printf '%s' "$tok" | LC_ALL=C tr 'A-Z' 'a-z')" in
    why | reason | todo | tbd | tba | xxx | xxxx | placeholder | fixme | none | foo | bar | baz | n/a)
      die_usage "adopt: --reason '$(sane "$raw")' records as the PLACEHOLDER '$(sane "$tok")' — as uninformative as no reason at all. Say what the resume IS, e.g. --reason cron-reinvoke:writer-pid-gone" ;;
  esac
  printf '%s\n' "$tok"
}

cmd_adopt() {
  local issue="${1:-}"; shift || true
  require_numeric_issue "$issue" adopt
  local reason='' reason_given=0 actor_raw=''
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --reason) reason="${2-}"; reason_given=1; shift 2 || die_usage "--reason needs a value" ;;
      --actor)  actor_raw="${2:-}"; shift 2 || die_usage "--actor needs a value" ;;
      *) die_usage "adopt: unknown option '$(sane "$1")'" ;;
    esac
  done
  # ARGUMENTS ARE VALIDATED BEFORE ANY STATE IS READ, so a placeholder reason is exit 64
  # whatever the lane looks like — a usage error must not depend on the marker's state.
  [ "$reason_given" -eq 1 ] || die_usage "adopt requires --reason saying what the resume IS (it is recorded in the stamp next to who took it), e.g. --reason cron-reinvoke:writer-pid-gone"
  local reason_token actor
  reason_token="$(assert_reason "$reason")"
  actor="$(resolve_actor "$actor_raw")"

  check_ownership "$issue" adopt
  local prior_session="$S_session" prior_pid="$S_pid" prior_ts="$S_ts" stage="$S_stage"

  if [ "$prior_session" = "$(this_session)" ] && [ "$prior_session" != unrecorded ]; then
    verdict ADOPTED
    detail "re-entrant: this session already owns $(sane "$(marker_path)") — nothing to transfer"
    return 0
  fi

  local carried f_stage=''
  carried="$(mktemp "${TMPDIR:-/tmp}/drive-issue-body.XXXXXX")" || refuse ERROR 1 "cannot create a temporary file for the carried body"
  register_tmp "$carried"
  marker_body >"$carried"
  assert_body_safe "$carried"
  [ -z "$stage" ] || f_stage="stage: $stage"
  if ! write_marker "$issue" "$actor" "$carried" "$f_stage" \
      "prior-session: $prior_session" \
      "prior-session-pid: $prior_pid" \
      "prior-ts: $prior_ts" \
      "adopt-reason: $reason_token"; then
    rm -f "$carried"
    refuse ERROR 1 "$WRITE_ERR"
  fi
  rm -f "$carried"
  verdict ADOPTED
  detail "issue=$(sane "$issue") prior-session=$(sane "$prior_session") prior-session-pid=$(sane "$prior_pid") new-session=$(sane "$(this_session)") reason=$(sane "$reason_token") -> $(sane "$WROTE_PATH"); the recorded writer was provably gone: $LIVE_DETAIL"
}

cmd_show() {
  local issue="${1:-}"; shift || true
  require_numeric_issue "$issue" show
  read_marker
  emit "field issue=$(sane "$S_issue")"
  emit "field machine=$(sane "$S_machine")"
  emit "field worktree=$(sane "$S_worktree")"
  emit "field session=$(sane "$S_session")"
  emit "field session-pid=$(sane "$S_pid")"
  emit "field session-pid-start=$(sane "$S_start_lo")..$(sane "$S_start_hi")"
  emit "field actor=$(sane "$S_actor")"
  emit "field ts=$(sane "$S_ts")"
  emit "field stage=$(sane "${S_stage:-none}")"
  [ -z "$S_prior_session" ] || emit "field prior-session=$(sane "$S_prior_session")"
  verdict SHOWN
  detail "fields as recorded for issue $(sane "$issue"); SHOWN asserts NOTHING about ownership — use 'verify' for that"
}

SUB="${1:-}"
case "$SUB" in
  write)  shift; cmd_write  "$@" ;;
  verify) shift; cmd_verify "$@" ;;
  adopt)  shift; cmd_adopt  "$@" ;;
  show)   shift; cmd_show   "$@" ;;
  -h | --help) print_help ;;
  '') die_usage "a subcommand is required: write <N> | verify <N> | adopt <N> --reason <why> | show <N> (see --help)" ;;
  *)  die_usage "unknown subcommand: $(sane "$SUB") (expected write|verify|adopt|show)" ;;
esac
