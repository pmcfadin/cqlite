#!/usr/bin/env bash
#
# claim-heartbeat.sh — cross-machine claim liveness via a cheap origin git ref
# (issue #2089, parent epic #2083).
#
# WHY THIS EXISTS
# ---------------
# Cross-machine claims already work via the issue-<N>-* branch lock (pushing
# the branch to origin IS the lock — #1886), but there is no SHARED signal for
# whether the claiming machine is still alive. flow-board's old reaper guessed
# from "no recent commits on the claim branch" — noisy in both directions: a
# long implementation phase with no commits looks abandoned, while a
# push-then-idle machine looks alive. This script gives every active worker a
# lightweight heartbeat: a bare commit object (empty tree; no working-tree or
# branch mutation) pushed to a machine-scoped ref on origin, force-updated on
# every beat. `flow-board` calls `list` to render the fleet view and apply the
# deterministic reap rule (heartbeat age > threshold AND no open PR). This is a
# plain git ref push — NEVER a GitHub API call — so it never touches the
# REST/GraphQL rate-limit buckets #1886/#1930 already worry about.
#
# REF LAYOUT
#   refs/heartbeats/<machine>
#     ONE ref per machine (one worker per machine, issue #1930), force-updated
#     on every `beat` — never a history of commits, just the latest liveness
#     proof. The commit is a root commit (no parent) pointing at the empty
#     tree; its ONLY payload is the commit message:
#       "heartbeat issue=<N> machine=<machine> ts=<ISO8601 UTC>"
#     The commit's author/committer date is also set to `ts` (informational —
#     `list` always parses `ts` FROM THE MESSAGE, the pushed clock, never the
#     commit date, so a re-push through a clock-skewed relay or a rebasing
#     proxy can't silently corrupt the age computation).
#
# THRESHOLD SEMANTICS (documented once, here — flow-board defers to this file)
#   A heartbeat older than 4 hours (default) with NO open PR for its issue is
#   "stale enough to reap" per issue #2089's deterministic rule. 4h
#   comfortably exceeds a normal gate+review round (the full gate alone runs
#   12-25 min; 4h absorbs a lunch break or a long gate-slot queue — see
#   #1825/#1848) while still catching a genuinely dead session well within a
#   work day. This is a REAP GATE, not a liveness SLA: a worker beating at
#   every stage transition (claim/activate/implement/gate/PR) is *usually* far
#   fresher than 4h; the threshold exists to bound the worst case, not to
#   define "healthy". flow-board is the only caller that acts on this — this
#   script only measures and reports age.
#
# USAGE
#   claim-heartbeat.sh beat <issue>      # push a fresh heartbeat for THIS machine
#   claim-heartbeat.sh list              # one line per machine: machine/issue/ts/age
#   claim-heartbeat.sh clear <machine>   # delete a machine's heartbeat ref (reap)
#
# Run from inside the repo (any cwd under the working tree/worktree is fine —
# this never touches the working tree or the current branch).
#
# ENV
#   HEARTBEAT_REMOTE   remote name to push/list/clear against (default: origin)
#   HEARTBEAT_MACHINE  override the machine identity (default: `hostname -s`);
#                      tests use this to simulate multiple machines against one
#                      fake origin from a single clone.
#
# EXIT CODES
#   0  success (including "zero heartbeats" on `list`, and "already absent" on
#      `clear` — both are not errors)
#   1  git operation failed (push/fetch/delete)
#   64 usage error
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[claim-heartbeat] $*" >&2; }

REMOTE="${HEARTBEAT_REMOTE:-origin}"

print_help() {
  awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2 {sub(/^# ?/,""); print}' "$0"
}

# ts_to_epoch <ISO8601 UTC ts> — portable across GNU date and BSD/macOS date.
ts_to_epoch() {
  local ts="$1" epoch
  if epoch=$(date -u -d "$ts" +%s 2>/dev/null); then
    printf '%s\n' "$epoch"
    return 0
  fi
  if epoch=$(date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$ts" +%s 2>/dev/null); then
    printf '%s\n' "$epoch"
    return 0
  fi
  return 1
}

# humanize_age <seconds> — coarse, deterministic bucket (s/m/h/d).
humanize_age() {
  local s="$1"
  [ "$s" -lt 0 ] 2>/dev/null && s=0
  if [ "$s" -lt 60 ]; then
    printf '%ss\n' "$s"
  elif [ "$s" -lt 3600 ]; then
    printf '%sm\n' "$((s / 60))"
  elif [ "$s" -lt 86400 ]; then
    printf '%sh\n' "$((s / 3600))"
  else
    printf '%sd\n' "$((s / 86400))"
  fi
}

cmd_beat() {
  local issue="${1:-}"
  case "$issue" in
    *[!0-9]* | '') die_usage "beat requires a numeric issue number (got '${issue:-<none>}')" ;;
  esac

  local machine ts message empty_tree commit_sha
  machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  message="heartbeat issue=${issue} machine=${machine} ts=${ts}"

  # Empty tree — the heartbeat carries no content, only the commit message.
  # Computed via hash-object (not the well-known SHA-1 constant) so this keeps
  # working under a SHA-256 object format too.
  empty_tree="$(git hash-object -t tree --stdin </dev/null)"

  # A fixed bot identity — never rely on the caller's (or sandbox's) global
  # git config being set, and stamp author/committer date to `ts` so the
  # commit's own metadata agrees with the message (list() still authoritatively
  # parses the message, never the commit date; see header).
  commit_sha="$(
    GIT_AUTHOR_NAME="cqlite-heartbeat" GIT_AUTHOR_EMAIL="heartbeat@cqlite.local" \
      GIT_COMMITTER_NAME="cqlite-heartbeat" GIT_COMMITTER_EMAIL="heartbeat@cqlite.local" \
      GIT_AUTHOR_DATE="$ts" GIT_COMMITTER_DATE="$ts" \
      git commit-tree "$empty_tree" -m "$message"
  )"

  # Force-update: ONE ref per machine (issue #1930 — one worker per machine),
  # so every beat replaces the previous liveness proof rather than growing a
  # history. This never touches the working tree, the index, or the current
  # branch — it is a pure object push against an explicit refspec.
  git push "$REMOTE" --force "${commit_sha}:refs/heartbeats/${machine}"
  note "heartbeat pushed: machine=$machine issue=$issue ts=$ts -> refs/heartbeats/$machine ($commit_sha)"
}

cmd_list() {
  local now_epoch raw
  now_epoch="$(date -u +%s)"
  raw="$(git ls-remote "$REMOTE" 'refs/heartbeats/*' 2>/dev/null || true)"

  if [ -z "$raw" ]; then
    echo "no heartbeats found on $REMOTE"
    return 0
  fi

  printf '%-20s %-8s %-24s %s\n' "MACHINE" "ISSUE" "TS" "AGE"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local refname machine msg issue ts epoch age_h
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    machine="${refname#refs/heartbeats/}"

    if ! git fetch "$REMOTE" "$refname" >/dev/null 2>&1; then
      printf '%-20s %-8s %-24s %s\n' "$machine" "?" "?" "fetch-failed"
      continue
    fi
    msg="$(git log -1 --format=%B FETCH_HEAD 2>/dev/null || true)"
    issue="$(printf '%s' "$msg" | sed -n 's/.*issue=\([0-9][0-9]*\).*/\1/p' | head -1)"
    ts="$(printf '%s' "$msg" | sed -n 's/.*ts=\([^ ]*\).*/\1/p' | head -1)"
    [ -n "$issue" ] || issue="?"
    [ -n "$ts" ] || ts="?"

    if [ "$ts" != "?" ] && epoch="$(ts_to_epoch "$ts" 2>/dev/null)"; then
      age_h="$(humanize_age "$((now_epoch - epoch))")"
    else
      age_h="unknown"
    fi
    printf '%-20s %-8s %-24s %s\n' "$machine" "$issue" "$ts" "$age_h"
  done <<<"$raw"
}

cmd_clear() {
  local machine="${1:-}"
  [ -n "$machine" ] || die_usage "clear requires <machine>"

  if git ls-remote --exit-code "$REMOTE" "refs/heartbeats/${machine}" >/dev/null 2>&1; then
    git push "$REMOTE" --delete "refs/heartbeats/${machine}"
    note "cleared refs/heartbeats/${machine} on $REMOTE"
  else
    note "refs/heartbeats/${machine} already absent on $REMOTE — nothing to clear"
  fi
}

SUBCOMMAND="${1:-}"
case "$SUBCOMMAND" in
  beat)
    shift
    cmd_beat "${1:-}"
    ;;
  list)
    cmd_list
    ;;
  clear)
    shift
    cmd_clear "${1:-}"
    ;;
  -h | --help)
    print_help
    ;;
  "")
    die_usage "a subcommand is required: beat <issue> | list | clear <machine>"
    ;;
  *)
    die_usage "unknown subcommand: $SUBCOMMAND (expected beat|list|clear)"
    ;;
esac
