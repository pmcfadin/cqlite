#!/usr/bin/env bash
#
# premerge-assert.sh — the #2456 pre-merge SHA guard, as a script (issue #2668).
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
# This encodes the #2456 rule mechanically so the closer runs it instead of
# remembering it: immediately before `gh pr merge`, assert that the PR is OPEN
# and its headRefOid equals the locally-certified SHA. Refuse (non-zero) on any
# mismatch, on a closed/merged PR, or on a gh/network failure — FAIL CLOSED,
# never "assume ok".
#
# We parse with gh's built-in `--jq` (jq expression run inside gh), so gh's JSON
# serialization is NOT load-bearing — we never read raw JSON with sed/regex.
#
# USAGE
#   scripts/flow/premerge-assert.sh <pr-number> <certified-sha>
#
# ENVIRONMENT
#   GH_REPO   the target repo (default: pmcfadin/cqlite). `gh` honors GH_REPO
#             natively; we pass --repo explicitly too so the default applies.
#
# EXIT CODES
#   0   head matches + PR OPEN     — prints "PREMERGE: OK <sha>"
#   2   head moved (mismatch), OR PR closed/merged — LOUD multi-line refusal
#   3   gh/network/usage failure   — fail closed, never merge on uncertainty
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -euo pipefail

repo="${GH_REPO:-pmcfadin/cqlite}"

usage() {
  printf 'usage: %s <pr-number> <certified-sha>\n' "$(basename "$0")" >&2
}

if [ "$#" -ne 2 ]; then
  usage
  exit 3
fi

pr="$1"
certified="$2"

if [ -z "$pr" ] || [ -z "$certified" ]; then
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
exit 0
