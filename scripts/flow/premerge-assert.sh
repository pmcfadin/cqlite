#!/usr/bin/env bash
#
# premerge-assert.sh — the #2456 pre-merge SHA guard + the #3465 gate-of-record
# guard, as a script (issues #2668, #3465).
#
# WHY THIS EXISTS
# ---------------
# The flow-closer certifies a SPECIFIC SHA: the exact tree the full gate of
# record and the final roborev pass actually ran on. If the PR's head has
# moved since that certification (a foreign push, a stale un-pushed rebase,
# someone else's commit), then `gh pr merge` would squash a DIFFERENT tree than
# the one the gate covered. That is the 2026-07-14 stale-merge escape on
# #2299/PR #2421: the closer certified a rebased-and-fixed tip locally but never
# pushed it, and `gh pr merge` squashed the PR's stale pre-fix head, transiently
# landing a known data-loss blocker on main. The GitHub required check re-runs
# on push but CANNOT catch a "merge of an old green head" — this SHA assertion
# is the real guard.
#
# #3465 adds the OTHER half. Verifying that the PR head equals a *claimed*
# certified sha never verified that a certified sha EXISTS. PR #3408 merged with
# NO full gate of record at all — 22 `--lite` PASSes and not one
# `scripts/agent-gate.sh` run — because nothing in the merge path ever asked for
# the full `==== AGENT-GATE SUMMARY ====` block. The gate-of-record convention
# was honour-system doctrine; this script is the one point every merge passes
# through, so the convention becomes a mechanism here: a summary file carrying a
# FULL-gate block with `RESULT: PASS`, `tree-integrity: PASS`, and provenance
# (`commit:` + `tree-start:`) matching the certified sha is now REQUIRED.
#
# The gate-summary argument is deliberately REQUIRED, not optional: an optional
# argument would leave the honour system exactly where it is. Omitting it is a
# usage failure (exit 3), which breaks pre-#3465 callers loudly and on purpose.
#
# We parse gh with gh's built-in `--jq` (jq expression run inside gh), so gh's
# JSON serialization is NOT load-bearing — we never read raw JSON with
# sed/regex. The gate summary is parsed by whole-line-anchored marker matching
# after ANSI stripping (#3400: colour survives redirection to a file, and the
# gate's own mandated capture is coloured).
#
# TWO RESIDUALS, STATED RATHER THAN FAKED
# ---------------------------------------
#  1. `run-id:` CANNOT be verified here. The #2874 reader contract says a reader
#     must confirm the summary's `run-id:` matches the run IT launched — this
#     script did not launch the gate, so it has nothing to compare against. It
#     therefore does not look at `run-id:` at all rather than pretend to.
#  2. This assert proves a summary EXISTS claiming a full-gate PASS at this sha
#     with an intact tree. It cannot prove that summary was produced by a
#     genuine gate run rather than hand-written. A HOSTILE INVOKER IS OUT OF THE
#     THREAT MODEL — whoever runs this script controls the process and could
#     edit the script, fake the file, or skip the script entirely; no check
#     inside a process defends against the party that controls the process. What
#     this guard defends is ACCIDENT AND DRIFT, which is the observed failure
#     mode: a diligent worker with no step in its path telling it the gate of
#     record was never run.
#
# USAGE
#   scripts/flow/premerge-assert.sh <pr-number> <certified-sha> <gate-summary-file>
#
# ENVIRONMENT
#   GH_REPO   the target repo (default: pmcfadin/cqlite). `gh` honors GH_REPO
#             natively; we pass --repo explicitly too so the default applies.
#
# EXIT CODES
#   0   gate of record verified + head matches + PR OPEN
#       — prints "PREMERGE: OK <sha>" and "PREMERGE: GATE-OF-RECORD ..."
#   2   no/invalid gate of record, OR head moved (mismatch), OR PR closed/merged
#       — LOUD multi-line refusal
#   3   gh/network/usage failure   — fail closed, never merge on uncertainty
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -euo pipefail

repo="${GH_REPO:-pmcfadin/cqlite}"

usage() {
  printf 'usage: %s <pr-number> <certified-sha> <gate-summary-file>\n' "$(basename "$0")" >&2
  printf '       <gate-summary-file> is REQUIRED: the AGENT_GATE_SUMMARY_FILE of the\n' >&2
  printf '       FULL gate of record (a "==== AGENT-GATE SUMMARY ====" block with\n' >&2
  printf '       RESULT: PASS, tree-integrity: PASS, at the certified sha). See #3465.\n' >&2
}

if [ "$#" -ne 3 ]; then
  usage
  exit 3
fi

pr="$1"
certified="$2"
summary_file="$3"

if [ -z "$pr" ] || [ -z "$certified" ] || [ -z "$summary_file" ]; then
  usage
  exit 3
fi

# Normalize the certified SHA to lowercase and require a full 40-char hex SHA —
# an abbreviated or malformed value can never be safely compared to headRefOid.
certified=$(printf '%s' "$certified" | tr '[:upper:]' '[:lower:]')
case "$certified" in
  *[!0-9a-f]* | "")
    printf 'error: certified SHA must be 40 hex chars (got: %s)\n' "$2" >&2
    usage
    exit 3
    ;;
esac
if [ "${#certified}" -ne 40 ]; then
  printf 'error: certified SHA must be a full 40-char hex SHA (got %d chars: %s)\n' \
    "${#certified}" "$2" >&2
  usage
  exit 3
fi

# ---------------------------------------------------------------------------
# GATE OF RECORD (#3465) — checked FIRST, before any `gh` call. It is offline
# and cheap, and "you have no gate of record" must be reportable without a
# network round trip.
# ---------------------------------------------------------------------------

refuse_no_gate() {
  printf '========================================================\n' >&2
  printf 'PREMERGE: NO-GATE-OF-RECORD — REFUSING TO MERGE\n' >&2
  printf '  summary file: %s\n' "$summary_file" >&2
  printf '  certified sha: %s\n' "$certified" >&2
  while [ "$#" -gt 0 ]; do
    printf '  %s\n' "$1" >&2
    shift
  done
  printf '  The FULL gate is the only run that counts (#719). Run it once,\n' >&2
  printf '  immediately pre-merge, with the mandated redirect:\n' >&2
  printf '    AGENT_GATE_SUMMARY_FILE=<path> bash scripts/agent-gate.sh > gate.log 2>&1\n' >&2
  printf '  then pass <path> as the third argument. See #3465.\n' >&2
  printf '========================================================\n' >&2
  exit 2
}

if [ ! -f "$summary_file" ]; then
  refuse_no_gate "The gate summary file does not exist (or is not a regular file)."
fi
if [ ! -r "$summary_file" ]; then
  refuse_no_gate "The gate summary file exists but is not readable."
fi
if [ ! -s "$summary_file" ]; then
  refuse_no_gate "The gate summary file is EMPTY — nothing was certified."
fi

# Parse the summary by REDIRECTION, never a pipe (#3400: a piped `while read`
# runs in a subshell and its verdict is discarded). One awk pass:
#   * strips ANSI escapes and a trailing CR before matching anything
#   * counts blocks by WHOLE-LINE-EXACT marker equality, never substring —
#     CLAUDE.md, issue threads and PR bodies quote these markers in prose, and
#     "==== END AGENT-GATE SUMMARY ====" CONTAINS the start marker as a substring
#   * also counts LITE/DELTA blocks purely so a refusal can NAME what it found
#     (those headers are distinct by construction: scripts/agent-gate.sh)
#   * emits key=value lines with per-key occurrence COUNTS, so a duplicated key
#     inside one block is refusable rather than silently last-wins
gate_parse=$(awk '
  BEGIN {
    S = "==== AGENT-GATE SUMMARY ===="
    E = "==== END AGENT-GATE SUMMARY ===="
    LS = "==== AGENT-GATE LITE SUMMARY ===="
    DS = "==== AGENT-GATE DELTA SUMMARY ===="
    blocks = 0; lite = 0; delta = 0; open = 0; unterminated = 0
    n_result = 0; n_ti = 0; n_commit = 0; n_ts = 0; n_mode = 0
    v_result = ""; v_ti = ""; v_commit = ""; v_ts = ""; v_dirty = ""
  }
  {
    gsub(/\033\[[0-9;]*[a-zA-Z]/, "")
    sub(/\r$/, "")
  }
  $0 == S { blocks++; if (open == 1) unterminated = 1; open = 1; next }
  $0 == E { if (open == 1) open = 0; next }
  $0 == LS { lite++; next }
  $0 == DS { delta++; next }
  open == 1 {
    if ($1 == "MODE:")           { n_mode++ }
    else if ($1 == "RESULT:")    { n_result++; v_result = $2 }
    else if ($1 == "tree-integrity:") { n_ti++; v_ti = $2 }
    else if ($1 == "tree-start:") { n_ts++; v_ts = $2 }
    else if ($1 == "commit:") {
      n_commit++; v_commit = $2
      for (i = 2; i < NF; i++) if ($i == "dirty:") v_dirty = $(i + 1)
    }
    next
  }
  END {
    if (open == 1) unterminated = 1
    print "blocks=" blocks
    print "lite=" lite
    print "delta=" delta
    print "unterminated=" unterminated
    print "n_mode=" n_mode
    print "n_result=" n_result
    print "n_ti=" n_ti
    print "n_commit=" n_commit
    print "n_ts=" n_ts
    print "v_result=" v_result
    print "v_ti=" v_ti
    print "v_commit=" v_commit
    print "v_ts=" v_ts
    print "v_dirty=" v_dirty
  }
' <"$summary_file") || refuse_no_gate "Could not parse the gate summary file (awk failed)."

blocks=""; lite=""; delta=""; unterminated=""
n_mode=""; n_result=""; n_ti=""; n_commit=""; n_ts=""
v_result=""; v_ti=""; v_commit=""; v_ts=""; v_dirty=""
while IFS='=' read -r gp_k gp_v; do
  case "$gp_k" in
    blocks)       blocks="$gp_v" ;;
    lite)         lite="$gp_v" ;;
    delta)        delta="$gp_v" ;;
    unterminated) unterminated="$gp_v" ;;
    n_mode)       n_mode="$gp_v" ;;
    n_result)     n_result="$gp_v" ;;
    n_ti)         n_ti="$gp_v" ;;
    n_commit)     n_commit="$gp_v" ;;
    n_ts)         n_ts="$gp_v" ;;
    v_result)     v_result="$gp_v" ;;
    v_ti)         v_ti="$gp_v" ;;
    v_commit)     v_commit="$gp_v" ;;
    v_ts)         v_ts="$gp_v" ;;
    v_dirty)      v_dirty="$gp_v" ;;
  esac
done <<GATE_PARSE
$gate_parse
GATE_PARSE

gp_k=""; gp_v=""; gp_v_name=""; gp_label=""
# Every field is keyed on its AFFIRMATIVE value: an unparseable/absent count is
# refused, never treated as "no problem found".
for gp_k in blocks lite delta unterminated n_mode n_result n_ti n_commit n_ts; do
  eval "gp_v=\${$gp_k}"
  case "$gp_v" in
    ''|*[!0-9]*)
      refuse_no_gate "Gate summary parse produced no usable '$gp_k' count — refusing (fail closed)."
      ;;
  esac
done

if [ "$unterminated" != 0 ]; then
  refuse_no_gate \
    "An AGENT-GATE SUMMARY block is UNTERMINATED (no exact '==== END AGENT-GATE SUMMARY ====')." \
    "A truncated summary certifies nothing — the gate may still be running or have died."
fi

if [ "$blocks" -eq 0 ]; then
  refuse_no_gate \
    "The file contains ZERO full-gate blocks (found $lite lite, $delta delta)." \
    "--lite and --delta emit DISTINCT headers and are NOT the gate of record:" \
    "  --lite  is fast iteration; --delta re-certifies a post-full-PASS polish round." \
    "This is the #3408 failure exactly: many lite PASSes, no full gate."
fi

if [ "$blocks" -gt 1 ]; then
  refuse_no_gate \
    "The file contains $blocks full-gate blocks — AMBIGUOUS." \
    "Refusing rather than picking one (a 'take the last block' rule would let a" \
    "stale or foreign run certify this merge). Point at ONE run's summary file."
fi

# Belt for the header separation above: the FULL gate emits NO `MODE:` line;
# --lite and --delta each emit one naming themselves.
if [ "$n_mode" -ne 0 ]; then
  refuse_no_gate \
    "The full-gate block carries a MODE: line — the FULL gate emits none." \
    "This block was produced by (or doctored from) a lite/delta run."
fi

for gp_k in n_result:RESULT n_ti:tree-integrity n_commit:commit n_ts:tree-start; do
  gp_v_name="${gp_k%%:*}"
  gp_label="${gp_k#*:}"
  eval "gp_v=\${$gp_v_name}"
  if [ "$gp_v" -eq 0 ]; then
    refuse_no_gate "The full-gate block has no '$gp_label:' line — it cannot certify anything."
  fi
  if [ "$gp_v" -gt 1 ]; then
    refuse_no_gate "The full-gate block has $gp_v '$gp_label:' lines — AMBIGUOUS, refusing."
  fi
done

# Verdict TOKENS are compared EXACTLY, never by prefix (#3229): a `PASS*` glob
# accepts `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, i.e. it would
# check a SPELLING rather than a STATE. awk already gave us the first
# whitespace-delimited token after the key, so this is a token-exact compare.
if [ "$v_result" != PASS ]; then
  refuse_no_gate \
    "RESULT verdict token is '$v_result', not PASS." \
    "INCOMPLETE is the launch-time liveness SENTINEL, not a verdict (#3041): it is" \
    "written when the gate starts (before the slot is even granted) and overwritten" \
    "only at the terminal emit. Such a summary means still running, queued, or died."
fi

if [ "$v_ti" != PASS ]; then
  refuse_no_gate \
    "tree-integrity verdict token is '$v_ti', not PASS." \
    "A run whose worktree mutated mid-run cannot certify (#2926); PENDING means the" \
    "run never reached its terminal emit, and SKIP means the check never ran."
fi

# The sha comparison. `commit:` carries a 7-char abbreviation and `tree-start:` a
# 12-char one (both `printf '%.Ns'` of the same VERIFIED capture in
# scripts/agent-gate.sh), so "matches the certified sha" cannot be string
# equality against the 40-hex certified sha. Compare each value at ITS OWN exact
# width, using the value's own length — never a glob, never `case $x in $y*)`,
# never a fixed assumed width. BOTH must match: two independent widths off one
# verified capture is materially stronger than one 7-hex compare. A non-hex value
# ("(not captured)", "(capture unavailable — no git worktree)", "selftest",
# "unverified") REFUSES — it is never skipped.
assert_sha_prefix() {
  # $1 = label, $2 = value from the summary
  local label="$1" val="$2" n
  case "$val" in
    ''|*[!0-9a-f]*)
      refuse_no_gate \
        "'$label:' value '$val' is not lowercase hex — nothing verifiable was recorded." \
        "The gate writes a non-hex placeholder when its capture failed or there was no" \
        "git worktree; such a run proves nothing about which tree it executed against."
      ;;
  esac
  n=${#val}
  if [ "$n" -lt 4 ] || [ "$n" -gt 40 ]; then
    refuse_no_gate "'$label:' value '$val' is $n hex chars — outside the 4..40 range."
  fi
  if [ "${certified:0:n}" != "$val" ]; then
    refuse_no_gate \
      "'$label:' value '$val' does not match the certified sha at $n chars." \
      "certified: $certified" \
      "The gate of record ran against a DIFFERENT tree than the one being merged."
  fi
}

assert_sha_prefix commit "$v_commit"
assert_sha_prefix tree-start "$v_ts"

# `dirty:` is REPORTED, not enforced — DELIBERATELY. Failing on `dirty: yes` is
# not in the owner's ruling on #3465 and is not absorbed into this change; a
# follow-up issue will propose it. This is a decision, not an oversight: print it
# so a dirty gate of record is VISIBLE at the merge point.
[ -n "$v_dirty" ] || v_dirty=unknown

# ---------------------------------------------------------------------------
# PR HEAD + STATE (#2456)
# ---------------------------------------------------------------------------

# Fetch head + state in ONE call, extracted by gh's built-in jq into two
# whitespace-separated tokens: "<headRefOid> <state>". Because gh runs the jq
# expression, its JSON serialization (compact vs pretty) is irrelevant. On any
# gh/network failure -> exit 3 (fail closed).
if ! out=$(gh pr view "$pr" --repo "$repo" --json headRefOid,state \
  --jq '.headRefOid + " " + .state' 2>/dev/null); then
  printf '========================================================\n' >&2
  printf 'PREMERGE: GH-FAILURE\n' >&2
  printf '  gh pr view %s --repo %s failed (auth/network/no-such-PR).\n' "$pr" "$repo" >&2
  printf '  Cannot verify the PR head — refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
fi

# Split the two tokens. Empty or malformed --jq output -> exit 3 (fail closed).
actual=$(printf '%s' "$out" | awk '{print $1}')
state=$(printf '%s' "$out" | awk '{print $2}')
actual=$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')

if [ -z "$actual" ] || [ -z "$state" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: GH-FAILURE\n' >&2
  printf '  Could not parse headRefOid/state from gh --jq output.\n' >&2
  printf '  Refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
fi

if [ "$state" != "OPEN" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: NOT-OPEN\n' >&2
  printf '  PR #%s state is "%s" (expected OPEN).\n' "$pr" "$state" >&2
  printf '  The PR is already closed or merged — do NOT merge again.\n' >&2
  printf '========================================================\n' >&2
  exit 2
fi

if [ "$actual" != "$certified" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: STALE-HEAD — REFUSING TO MERGE\n' >&2
  printf '  certified SHA: %s\n' "$certified" >&2
  printf '  actual   head: %s\n' "$actual" >&2
  printf '  head moved since certification — the gate of record no longer\n' >&2
  printf '  covers this PR; re-certify before merge.\n' >&2
  printf '========================================================\n' >&2
  exit 2
fi

printf 'PREMERGE: OK %s\n' "$certified"
printf 'PREMERGE: GATE-OF-RECORD commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s\n' \
  "$v_commit" "$v_ts" "$v_dirty" "$summary_file"
exit 0
