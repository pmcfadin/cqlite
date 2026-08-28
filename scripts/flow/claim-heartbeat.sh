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
#   refs/machine-claims/<machine>   (issue #2655 / #2499 design)
#     A SUPERVISOR-authored claim ref — the machine-driven complement to the
#     LLM-driven `beat`. `worker-supervisor.sh` (#2090) stamps it at spawn,
#     refreshes it every iteration, and clears it on a clean exit, so claim
#     liveness no longer depends on the worker LLM *remembering* to beat. Same
#     empty-tree/root-commit shape; the message carries the owning PID as well:
#       "claim issue=<N> machine=<machine> pid=<PID> ts=<ISO8601 UTC>"
#     The PID lets a reaper running ON THE CLAIM'S OWN MACHINE add a
#     process-liveness check (a beat-then-crash no longer looks alive for the
#     whole threshold window). A reaper running elsewhere (the project-board-sync
#     CI cron) can't see the PID, so it falls back to age + no-open-PR only.
#     NOTE the namespace is `refs/machine-claims/*`, deliberately DISTINCT from
#     `scripts/flow/claim.sh`'s per-issue LOCK refs `refs/claims/issue-<N>`
#     (#2665) — this is a per-MACHINE liveness proof, not the issue lock, and the
#     reaper must never glob up (let alone delete) the issue-lock refs.
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
#   claim-heartbeat.sh beat <issue>       # push a fresh heartbeat for THIS machine
#   claim-heartbeat.sh list               # one line per machine: machine/issue/ts/age
#   claim-heartbeat.sh clear <machine>    # delete a machine's heartbeat ref (reap)
#
#   claim-heartbeat.sh stamp <issue> [pid]  # push/refresh THIS machine's claim ref
#                                            # (supervisor-authored; pid default $$)
#   claim-heartbeat.sh list-claims          # one line per machine: machine/issue/pid/ts/age
#   claim-heartbeat.sh should-reap <machine> [threshold_secs]
#                                            # exit 0 iff the claim ref is stale
#                                            # (age > threshold, default 14400s/4h)
#                                            # AND its issue has NO open PR AND
#                                            # (pid-dead, when the claim is local);
#                                            # exit 1 = keep (still live / open PR /
#                                            # foreign-pid unknowable); exit 2 = no ref.
#   claim-heartbeat.sh reap <machine>        # delete a machine's claim ref, but
#                                            # REFUSE if its issue has an open PR.
#   claim-heartbeat.sh dead-lanes            # REPORT every claim whose owning
#                                            # process is verifiably gone, with NO
#                                            # age threshold and regardless of an
#                                            # open PR. exit 3 = a dead lane was
#                                            # found; 1 = the measurement was
#                                            # incomplete; 0 = none (incl. zero
#                                            # claims).
#
# WHY `dead-lanes` IS NOT `should-reap` (issue #3393 AC3)
#   `should-reap` is a REAP GATE, and it consults the pid ONLY AFTER age >
#   threshold (4h). So a worker the kernel OOM-killed a minute ago is
#   indistinguishable from a healthy one for four hours, and even then the answer
#   is an exit code nobody is watching. That is the silence #3393 records: three
#   lanes died leaving a clean worktree, a held claim and an open PR, and NOTHING
#   REPORTED IT.
#
#   WHOSE PROCESS, EXACTLY (roborev round 1, High — the first draft of this text
#   overclaimed). The pid in the claim ref is the SUPERVISOR's: `worker-supervisor.sh`
#   stamps `$$`, "the stable per-machine anchor, not a transient worker subprocess"
#   (its own comment). So `DEAD-NO-PROCESS` means THE LANE-OWNING PROCESS IS GONE —
#   the whole session died, which is exactly what #3393's three silent lane deaths
#   did (the kernel killed the tmux scope, taking the supervisor with it).
#
#   NOT COVERED: a WORKER-ONLY KILL under a live supervisor. That is deliberate, not
#   an oversight — the supervisor's job is to recycle workers, so a dead worker beneath
#   a live supervisor is a lane still being driven, and reporting it as dead would page
#   an operator about a system that is working. Do NOT "fix" this by expecting a worker
#   pid here without also changing what `stamp` records: that ref is shared with
#   `should-reap` and the CI reaper, so its meaning is not this command's to redefine.
#   `test_claim_heartbeat.sh` pins the supervisor-pid semantic so this text cannot
#   drift from the mechanism.
#
#   `dead-lanes` answers the other question — "is anything dead RIGHT NOW?" — and
#   differs deliberately on both guards that make the reaper conservative:
#     * NO AGE GATE. A fresh claim with a dead pid is exactly the shape of an OOM
#       kill, so waiting out the threshold would hide the very event we want.
#     * AN OPEN PR DOES NOT SUPPRESS THE REPORT. For the reaper an open PR means
#       KEEP (an unfinished endgame stays owned — #2499). For a report it is the
#       MOST urgent row on the page: a dead process holding an in-flight endgame.
#       So the row is printed and annotated `open-pr=yes`, and reaping is still
#       refused. Reporting and reaping are different acts.
#
#   It is a REPORT, never a mutation: it deletes no ref and flips no board item.
#
#   THREE-VALUED BY CONSTRUCTION. A pid is only checkable on the machine that
#   owns the claim, so the verdict is never folded onto a two-valued alive/dead —
#   the permissive fold ("assume alive") is how a dead lane goes unseen, which is
#   the vacuous-pass shape this repo's doctrine forbids:
#     DEAD-NO-PROCESS   local claim, pid recorded, `kill -0` fails => gone.
#     ALIVE             local claim, pid recorded and still running.
#     UNKNOWN-FOREIGN   claim owned by another machine; its pid is unknowable
#                       from here, so it is reported as neither.
#     UNKNOWN-NO-PID    claim ref carries no pid (pre-#2655 or hand-made).
#   Only DEAD-NO-PROCESS sets the exit code. An UNKNOWN is a gap in what this
#   host can see, and it is printed as such rather than counted either way.
#
#   SCOPE LIMIT, stated because the acceptance criterion asks for more: #3393 AC3
#   describes the operator's scratch check as "worktree present, tmux session
#   absent => DEAD-NO-SESSION". That test cannot be implemented here, because the
#   lane-directory layout (`/data/lanes/lane-<N>`) and the tmux session naming it
#   depends on exist NOWHERE in this repository — not in this script, not in
#   `worker-supervisor.sh`, not in the fleet runbook. Committing a tool that
#   guessed at that convention would silently report nothing on any machine
#   naming its sessions differently, which is a vacuous green wearing a
#   watchdog's clothes. So this implements the same detection keyed on a signal
#   the repository DOES own: the supervisor-authored claim ref and the pid it
#   records (#2655). The remaining half — teaching the fleet a committed lane/
#   session convention — needs an owner decision and is left to #3393.
#
# Run from inside the repo (any cwd under the working tree/worktree is fine —
# this never touches the working tree or the current branch).
#
# ENV
#   HEARTBEAT_REMOTE   remote name to push/list/clear against (default: origin)
#   HEARTBEAT_MACHINE  override the machine identity (default: `hostname -s`);
#                      tests use this to simulate multiple machines against one
#                      fake origin from a single clone.
#   CLAIM_OPEN_PR_CMD  hook used by `should-reap`/`reap`/`clear` to test whether
#                      an issue has an open PR: run as `bash -c "$CMD" _ <issue>`
#                      ($1 = issue number), exit 0 = has open PR. Default lists
#                      open PRs via `gh` and matches the 1:1:1:1 head-branch
#                      convention `issue-<N>-<slug>`. Tests override it to stay
#                      hermetic (no network/gh). ANY gh/network failure is
#                      fail-SAFE: treated as "has open PR" so a transient outage
#                      never reaps a possibly-live claim.
#
# EXIT CODES
#   0  success (including "zero heartbeats" on `list`, and "already absent" on
#      `clear` — both are not errors)
#   1  git operation failed (push/fetch/delete)
#   3  `dead-lanes` only: at least one DEAD-NO-PROCESS lane was reported. A
#      DISTINCT code, not 1: "I found a dead lane" is a successful measurement
#      with a finding, while 1 means the measurement itself failed. A cron that
#      conflated them would page for a network blip and stay silent on a real
#      dead lane. `dead-lanes` also returns 1 when the measurement is INCOMPLETE
#      (the ref listing failed, or a listed ref would not fetch) and no dead lane
#      was found — "I could not tell" must never read as "all clear". A found dead
#      lane outranks incompleteness for the exit code, and the incompleteness is
#      still reported in the text.
#   64 usage error
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[claim-heartbeat] $*" >&2; }

REMOTE="${HEARTBEAT_REMOTE:-origin}"

# Never block on an interactive credential prompt (issue #2942). On a tty-attached
# worker with no git credential helper, the ref pushes below would sit waiting for a
# username forever instead of failing. Prompts off = they fail fast and visibly.
# NOTE: unlike claim.sh, this script does NOT classify auth-vs-transient on its
# pushes — it surfaces git's raw error.
export GIT_TERMINAL_PROMPT=0

# Slack when comparing a process's start time against its claim's `ts` (#3393). `ts` is
# second-resolution and the stamp happens moments AFTER the process starts, so a genuine
# claim always has start <= ts; the slack only absorbs clock granularity/skew, never a
# real recycle (a reused pid is minutes-to-hours newer than the claim).
PID_IDENTITY_SLACK_SECS="${PID_IDENTITY_SLACK_SECS:-60}"

# Default reap threshold: 4h (matches the heartbeat threshold documented above).
DEFAULT_REAP_THRESHOLD_SECS="${DEFAULT_REAP_THRESHOLD_SECS:-14400}"

# issue_has_open_pr <issue> — exit 0 iff the issue has an open linked PR.
# Overridable via CLAIM_OPEN_PR_CMD for hermetic tests (run as `bash -c "$CMD" _
# <issue>`). Default consults `gh`. A gh/network FAILURE is treated fail-SAFE:
# "we could not prove there is NO open PR" -> return "has open PR" (exit 0), so a
# transient outage never causes a reap of a possibly-live claim.
issue_has_open_pr() {
  local issue="$1"
  case "$issue" in
    '' | *[!0-9]*) return 1 ;; # no valid issue number => cannot have an open PR
  esac
  if [ -n "${CLAIM_OPEN_PR_CMD:-}" ]; then
    bash -c "$CLAIM_OPEN_PR_CMD" _ "$issue"
    return $?
  fi
  command -v gh >/dev/null 2>&1 || return 0 # no gh: cannot disprove -> fail-safe keep
  # Detect an open PR via the project's 1:1:1:1 head-branch convention
  # (`issue-<N>-<slug>`) — we list open PRs and keep those whose head starts with
  # `issue-<N>-`. `--limit 1000` is load-bearing: `gh pr list` defaults to a
  # 30-PR window, so on a repo with >30 open PRs a live claim's PR could fall
  # outside the default result set and be read as "no open PR" -> reaped while its
  # endgame is in flight (the exact fail-safe this reaper is built on). Any
  # gh/network FAILURE is fail-SAFE: "could not prove there is NO open PR" ->
  # return 0 (has open PR), so a transient outage never reaps a possibly-live
  # claim.
  local heads
  if ! heads="$(gh pr list --state open --limit 1000 --json headRefName --jq '.[].headRefName' 2>/dev/null)"; then
    return 0 # gh failed -> fail-safe: assume an open PR exists, do not reap
  fi
  printf '%s\n' "$heads" | grep -qE "^issue-${issue}(-|$)"
}

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

# push_liveness_ref <ref> <message> <ts> — commit an empty tree carrying
# <message> and force-push it to <ref> on $REMOTE. Shared by `beat` and `stamp`:
# same root-commit/empty-tree shape, fixed bot identity, author/committer date =
# <ts> so the commit metadata agrees with the message (`list`/`ref_field` still
# authoritatively parse FROM the message, never the commit date; see header).
# Empty tree computed via hash-object (not the SHA-1 constant) so this keeps
# working under a SHA-256 object format too. Never touches the working tree, the
# index, or the current branch — a pure object push against an explicit refspec.
# Echoes the created commit sha on stdout.
push_liveness_ref() {
  local ref="$1" message="$2" ts="$3" empty_tree commit_sha
  empty_tree="$(git hash-object -t tree --stdin </dev/null)"
  commit_sha="$(
    GIT_AUTHOR_NAME="cqlite-heartbeat" GIT_AUTHOR_EMAIL="heartbeat@cqlite.local" \
      GIT_COMMITTER_NAME="cqlite-heartbeat" GIT_COMMITTER_EMAIL="heartbeat@cqlite.local" \
      GIT_AUTHOR_DATE="$ts" GIT_COMMITTER_DATE="$ts" \
      git commit-tree "$empty_tree" -m "$message"
  )"
  git push "$REMOTE" --force "${commit_sha}:${ref}"
  printf '%s\n' "$commit_sha"
}

# ref_msg_field <refname> <key> — fetch <refname> and extract the run of
# non-space chars after `key=` in its commit message. Empty on any failure.
ref_msg_field() {
  local refname="$1" key="$2" msg
  git fetch "$REMOTE" "$refname" >/dev/null 2>&1 || return 0
  msg="$(git log -1 --format=%B FETCH_HEAD 2>/dev/null || true)"
  printf '%s' "$msg" | sed -n "s/.*${key}=\\([^ ][^ ]*\\).*/\\1/p" | head -1
}

cmd_beat() {
  local issue="${1:-}"
  case "$issue" in
    *[!0-9]* | '') die_usage "beat requires a numeric issue number (got '${issue:-<none>}')" ;;
  esac

  local machine ts commit_sha
  machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Force-update: ONE ref per machine (issue #1930 — one worker per machine),
  # so every beat replaces the previous liveness proof rather than growing a
  # history.
  commit_sha="$(push_liveness_ref "refs/heartbeats/${machine}" \
    "heartbeat issue=${issue} machine=${machine} ts=${ts}" "$ts")"
  note "heartbeat pushed: machine=$machine issue=$issue ts=$ts -> refs/heartbeats/$machine ($commit_sha)"
}

# cmd_stamp <issue> [pid] — supervisor-authored claim ref (issue #2655 / #2499).
# Same mechanism as `beat` but writes refs/machine-claims/<machine> and records the
# owning PID so a SAME-machine reaper can add a process-liveness check.
cmd_stamp() {
  local issue="${1:-}" pid="${2:-$$}"
  case "$issue" in
    *[!0-9]* | '') die_usage "stamp requires a numeric issue number (got '${issue:-<none>}')" ;;
  esac
  case "$pid" in
    *[!0-9]* | '') die_usage "stamp pid must be numeric (got '${pid}')" ;;
  esac

  local machine ts commit_sha
  machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  commit_sha="$(push_liveness_ref "refs/machine-claims/${machine}" \
    "claim issue=${issue} machine=${machine} pid=${pid} ts=${ts}" "$ts")"
  note "claim stamped: machine=$machine issue=$issue pid=$pid ts=$ts -> refs/machine-claims/$machine ($commit_sha)"
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

# delete_ref_guarded <ref-namespace> <machine> — shared delete for a machine's
# heartbeat OR claim ref that REFUSES to delete when the ref's issue still has
# an open PR (issue #2655: an open PR means the endgame is unfinished; deleting
# the liveness ref would erase the only signal that this lane is still owned and
# invite a duplicate pickup). A missing ref is a graceful no-op. Returns 0 on
# delete-or-absent, 3 on refuse.
delete_ref_guarded() {
  local namespace="$1" machine="$2"
  local ref="refs/${namespace}/${machine}"

  if ! git ls-remote --exit-code "$REMOTE" "$ref" >/dev/null 2>&1; then
    note "${ref} already absent on $REMOTE — nothing to clear"
    return 0
  fi

  local issue
  issue="$(ref_msg_field "$ref" issue)"
  if [ -n "$issue" ] && issue_has_open_pr "$issue"; then
    note "REFUSING to delete ${ref}: issue #${issue} has an open PR (endgame unfinished; #2655)"
    return 3
  fi

  git push "$REMOTE" --delete "$ref"
  note "cleared ${ref} on $REMOTE"
  return 0
}

cmd_clear() {
  local machine="${1:-}"
  [ -n "$machine" ] || die_usage "clear requires <machine>"
  delete_ref_guarded heartbeats "$machine"
}

cmd_reap() {
  local machine="${1:-}"
  [ -n "$machine" ] || die_usage "reap requires <machine>"
  delete_ref_guarded machine-claims "$machine"
}

cmd_list_claims() {
  local now_epoch raw
  now_epoch="$(date -u +%s)"
  raw="$(git ls-remote "$REMOTE" 'refs/machine-claims/*' 2>/dev/null || true)"

  if [ -z "$raw" ]; then
    echo "no claims found on $REMOTE"
    return 0
  fi

  printf '%-20s %-8s %-10s %-24s %s\n' "MACHINE" "ISSUE" "PID" "TS" "AGE"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local refname machine msg issue pid ts epoch age_h
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    machine="${refname#refs/machine-claims/}"

    if ! git fetch "$REMOTE" "$refname" >/dev/null 2>&1; then
      printf '%-20s %-8s %-10s %-24s %s\n' "$machine" "?" "?" "?" "fetch-failed"
      continue
    fi
    msg="$(git log -1 --format=%B FETCH_HEAD 2>/dev/null || true)"
    issue="$(printf '%s' "$msg" | sed -n 's/.*issue=\([0-9][0-9]*\).*/\1/p' | head -1)"
    pid="$(printf '%s' "$msg" | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | head -1)"
    ts="$(printf '%s' "$msg" | sed -n 's/.*ts=\([^ ]*\).*/\1/p' | head -1)"
    [ -n "$issue" ] || issue="?"
    [ -n "$pid" ] || pid="?"
    [ -n "$ts" ] || ts="?"

    if [ "$ts" != "?" ] && epoch="$(ts_to_epoch "$ts" 2>/dev/null)"; then
      age_h="$(humanize_age "$((now_epoch - epoch))")"
    else
      age_h="unknown"
    fi
    printf '%-20s %-8s %-10s %-24s %s\n' "$machine" "$issue" "$pid" "$ts" "$age_h"
  done <<<"$raw"
}

# cmd_should_reap <machine> [threshold_secs] — the deterministic, FAIL-SAFE reap
# predicate for a claim ref (issue #2655). Exit codes:
#   0  reap it: age > threshold AND issue has NO open PR AND (pid-dead, when the
#      claim belongs to THIS machine — a foreign machine's pid is unknowable, so
#      that clause is skipped there and age + no-open-PR govern).
#   1  keep it: still fresh, OR has an open PR, OR (local) pid is still alive,
#      OR the ts/age is unparseable (never reap on an unknown age).
#   2  no such claim ref.
# Never prints a reap verdict of 0 unless ALL guards agree — a live/open-PR/
# fresh/unknown-age claim is always kept.
cmd_should_reap() {
  local machine="${1:-}" threshold="${2:-$DEFAULT_REAP_THRESHOLD_SECS}"
  [ -n "$machine" ] || die_usage "should-reap requires <machine>"
  case "$threshold" in
    *[!0-9]* | '') die_usage "should-reap threshold must be numeric seconds (got '${threshold}')" ;;
  esac
  local ref="refs/machine-claims/${machine}"

  if ! git ls-remote --exit-code "$REMOTE" "$ref" >/dev/null 2>&1; then
    note "no claim ref ${ref} on $REMOTE"
    return 2
  fi

  local issue pid ts epoch now_epoch age
  issue="$(ref_msg_field "$ref" issue)"
  pid="$(ref_msg_field "$ref" pid)"
  ts="$(ref_msg_field "$ref" ts)"

  # Unparseable/absent age -> KEEP (never reap on an unknown age).
  if [ -z "$ts" ] || ! epoch="$(ts_to_epoch "$ts" 2>/dev/null)"; then
    note "keep ${ref}: unparseable ts ('${ts:-<none>}') — refusing to reap on unknown age"
    return 1
  fi
  now_epoch="$(date -u +%s)"
  age=$((now_epoch - epoch))
  if [ "$age" -le "$threshold" ]; then
    note "keep ${ref}: age ${age}s <= threshold ${threshold}s (fresh)"
    return 1
  fi

  # Open PR -> KEEP (endgame unfinished; the #2499 orphaned-endgame case).
  if [ -n "$issue" ] && issue_has_open_pr "$issue"; then
    note "keep ${ref}: issue #${issue} has an open PR (endgame in flight)"
    return 1
  fi

  # Local claim: add a process-liveness check. A live PID means the worker is
  # still running even if a beat is overdue — never reap it.
  local this_machine
  this_machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  if [ "$machine" = "$this_machine" ] && [ -n "$pid" ]; then
    if kill -0 "$pid" 2>/dev/null; then
      note "keep ${ref}: local pid ${pid} is still alive"
      return 1
    fi
    note "reap ${ref}: age ${age}s > ${threshold}s, no open PR, local pid ${pid} is dead"
    return 0
  fi

  note "reap ${ref}: age ${age}s > ${threshold}s, no open PR (foreign machine — pid not checkable)"
  return 0
}

# process_start_epoch <pid> — epoch seconds at which <pid> started, or EMPTY when it
# cannot be determined. Empty is a THIRD answer and is never folded onto "consistent".
#
# DERIVED FROM ELAPSED TIME, NOT A WALL-CLOCK STRING (roborev round 3, Medium). The
# first cut read `ps -o lstart=` and parsed it with `date -u`, but `lstart` is LOCAL
# wall time with no zone in it, so on any non-UTC host the epoch came out shifted by
# the offset — MEASURED: the same lstart parses 19,800s apart between UTC and
# Asia/Kolkata, which is far past PID_IDENTITY_SLACK_SECS and would falsely declare a
# live supervisor DEAD-PID-REUSED (or mask a real recycle). Elapsed seconds carry no
# timezone at all, so the whole class is gone rather than corrected.
process_start_epoch() {
  local pid="$1" secs now
  now="$(date -u +%s)"
  # `etimes` (elapsed SECONDS) is the direct form.
  secs="$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')"
  case "$secs" in
    '' | *[!0-9]*) secs="" ;;
  esac
  if [ -z "$secs" ]; then
    # Fall back to `etime` ([[DD-]HH:]MM:SS), which POSIX ps provides where `etimes`
    # is absent. Still elapsed, still timezone-free.
    local et d hms h m sec
    et="$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ')"
    [ -n "$et" ] || return 0
    case "$et" in
      *-*) d="${et%%-*}"; hms="${et#*-}" ;;
      *)   d=0;           hms="$et" ;;
    esac
    case "$hms" in
      *:*:*) h="${hms%%:*}"; m="$(printf '%s' "$hms" | cut -d: -f2)"; sec="${hms##*:}" ;;
      *:*)   h=0;            m="${hms%%:*}";                          sec="${hms##*:}" ;;
      *)     return 0 ;;
    esac
    case "$d$h$m$sec" in
      *[!0-9]*) return 0 ;;
    esac
    secs=$(( (10#$d * 86400) + (10#$h * 3600) + (10#$m * 60) + 10#$sec ))
  fi
  printf '%s\n' "$((now - secs))"
}

# process_exists <pid> — exit 0 iff the pid exists, REGARDLESS of ownership.
# `kill -0` is NOT used for this (roborev round 2, Medium): it fails with EPERM for a
# live process owned by another user, which would report a healthy lane as dead. `ps -p`
# answers existence without needing permission to signal.
process_exists() {
  ps -p "$1" >/dev/null 2>&1
}

# open_pr_state <issue> — echo `yes` | `no` | `unknown`. THREE-VALUED on purpose
# (roborev round 2, Low): `issue_has_open_pr` is fail-SAFE for the REAPER — a gh outage
# reads as "has open PR" so nothing is reaped on unproven information — but rendering
# that same guess to an operator as a definite `open-pr=yes` claims an orphaned endgame
# that may not exist. A report must distinguish a confirmed answer from a failed probe.
# `issue_has_open_pr` is deliberately left untouched: its two-valued fail-safe shape is
# correct for the decision it serves.
open_pr_state() {
  local issue="$1" rc
  case "$issue" in
    '' | *[!0-9]*) printf 'unknown\n'; return 0 ;;
  esac
  if [ -n "${CLAIM_OPEN_PR_CMD:-}" ]; then
    set +e
    bash -c "$CLAIM_OPEN_PR_CMD" _ "$issue" >/dev/null 2>&1
    rc=$?
    set -e
    case "$rc" in
      0) printf 'yes\n' ;;
      1) printf 'no\n' ;;
      *) printf 'unknown\n' ;;  # any other status = the probe itself did not answer
    esac
    return 0
  fi
  if ! command -v gh >/dev/null 2>&1; then
    printf 'unknown\n'
    return 0
  fi
  local heads
  set +e
  heads="$(gh pr list --state open --limit 1000 --json headRefName --jq '.[].headRefName' 2>/dev/null)"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    printf 'unknown\n'
    return 0
  fi
  if printf '%s\n' "$heads" | grep -qE "^issue-${issue}(-|$)"; then
    printf 'yes\n'
  else
    printf 'no\n'
  fi
}

# cmd_dead_lanes — REPORT (never mutate) every machine claim whose owning process is
# gone. See the header for what "owning process" means and what this does NOT cover.
#
# EXIT PRECEDENCE (3 outranks 1, deliberately): a found dead lane is ACTIONABLE NOW,
# so it wins the exit code, and any incompleteness is still stated in the text rather
# than lost. With no dead lane, an incomplete measurement is exit 1 — never 0, because
# "I could not tell" must not read as "all clear". That direction matters most during
# exactly the outage in which lanes are dying.
cmd_dead_lanes() {
  [ "$#" -eq 0 ] || die_usage "dead-lanes takes no arguments (got '$1')"

  local raw this_machine dead=0 unreadable=0 lsrc
  this_machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"

  # NO `|| true` HERE (roborev round 1, Medium). Swallowing the status turned an
  # outage into "no claims found" + exit 0 — a pass derived from the ABSENCE of a
  # bad signal. `git ls-remote` exits 0 with EMPTY output when nothing matches, so a
  # genuine empty fleet stays distinguishable from a failure without guessing.
  #
  # stdout and stderr are kept SEPARATE (roborev round 3, Low). `2>&1` merged git/SSH
  # warnings into the ref listing, where each warning line became a bogus "ref" row and
  # printed a spurious UNKNOWN-UNREADABLE — noise that also flipped the exit code to 1
  # on a perfectly healthy fleet. stderr is still captured, but only for the diagnostic.
  local errfile
  errfile="$(mktemp "${TMPDIR:-/tmp}/claim-heartbeat-lsremote.XXXXXX")"
  set +e
  raw="$(git ls-remote "$REMOTE" 'refs/machine-claims/*' 2>"$errfile")"
  lsrc=$?
  set -e
  if [ "$lsrc" -ne 0 ]; then
    note "could not list claim refs on ${REMOTE} (git exited ${lsrc}) — the fleet was NOT measured, so this is NOT 'no dead lanes'. git said: $(tr '\n' ' ' <"$errfile")"
    rm -f "$errfile"
    return 1
  fi
  rm -f "$errfile"

  if [ -z "$raw" ]; then
    echo "no claims found on $REMOTE — no dead lanes (an empty fleet is not a finding)"
    return 0
  fi

  printf '%-20s %-8s %-12s %-18s %s\n' "MACHINE" "ISSUE" "PID" "VERDICT" "DETAIL"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local refname machine msg issue pid ts verdict detail
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    machine="${refname#refs/machine-claims/}"

    if ! git fetch "$REMOTE" "$refname" >/dev/null 2>&1; then
      # An unreadable ref is "we cannot tell about THIS lane", never "this lane is
      # fine" — so it is both printed AND counted toward an incomplete measurement.
      unreadable=$((unreadable + 1))
      printf '%-20s %-8s %-12s %-18s %s\n' "$machine" "?" "?" "UNKNOWN-UNREADABLE" "fetch of $refname failed"
      continue
    fi
    msg="$(git log -1 --format=%B FETCH_HEAD 2>/dev/null || true)"
    issue="$(printf '%s' "$msg" | sed -n 's/.*issue=\([0-9][0-9]*\).*/\1/p' | head -1)"
    pid="$(printf '%s' "$msg" | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | head -1)"
    # The claim's own stamp time — the second half of the pid-identity check below.
    ts="$(printf '%s' "$msg" | sed -n 's/.*ts=\([^ ]*\).*/\1/p' | head -1)"
    # `worker-supervisor.sh` stamps issue "0" when the current iteration's issue is
    # not yet known. That is a PLACEHOLDER, not issue #0: probing for a PR on it
    # would query a number that cannot exist and print a bogus issue in the report.
    if [ -z "$issue" ] || [ "$issue" = "0" ]; then
      issue="?"
    fi

    detail=""
    if [ "$machine" != "$this_machine" ]; then
      # Unknowable BY DESIGN from here, so this does NOT count as an incomplete
      # measurement: counting it would make every multi-machine fleet exit 1 forever
      # and train readers to ignore the code.
      verdict="UNKNOWN-FOREIGN"
      detail="pid not checkable from ${this_machine}"
    elif [ -z "$pid" ]; then
      # A LOCAL claim SHOULD carry a pid, so its absence is a real gap in what this
      # run could determine — counted (roborev round 2, Medium).
      verdict="UNKNOWN-NO-PID"
      detail="claim ref records no pid (pre-#2655 or hand-made)"
      unreadable=$((unreadable + 1))
    elif process_exists "$pid"; then
      # The pid exists. That alone does NOT make it the process that stamped the
      # claim: pids are recycled, and a stale claim naming a reused pid would read
      # ALIVE — the dangerous direction for a monitor whose whole job is spotting a
      # dead lane. The claim's OWN ts settles it with no change to what `stamp`
      # records: a process that started AFTER the claim was stamped cannot be the
      # process that stamped it.
      local pstart cts
      pstart="$(process_start_epoch "$pid")"
      cts=""
      [ -n "$ts" ] && cts="$(ts_to_epoch "$ts" 2>/dev/null || true)"
      if [ -n "$pstart" ] && [ -n "$cts" ] && [ "$pstart" -gt "$((cts + PID_IDENTITY_SLACK_SECS))" ]; then
        verdict="DEAD-PID-REUSED"
        dead=$((dead + 1))
        detail="pid ${pid} started $((pstart - cts))s AFTER the claim was stamped — a different process now holds it; open-pr=$(open_pr_state "$issue")"
      elif [ -n "$pstart" ] && [ -n "$cts" ]; then
        verdict="ALIVE"
        detail="identity=verified (start precedes the claim ts)"
      else
        # NOT "ALIVE" (roborev round 3, Medium). The first cut reported ALIVE with an
        # `identity=unverified` annotation, which still let the run exit 0 — and a
        # recycled pid then reproduces exactly the false-clean this monitor exists to
        # prevent. An unverifiable identity is a gap in what this run could determine,
        # so it gets its own verdict and counts toward an incomplete measurement. An
        # annotation is not a substitute for an exit code: nobody greps the annotation.
        verdict="UNKNOWN-IDENTITY"
        detail="pid ${pid} exists but its start time or the claim ts could not be read — pid reuse NOT excluded"
        unreadable=$((unreadable + 1))
      fi
    else
      verdict="DEAD-NO-PROCESS"
      dead=$((dead + 1))
      detail="open-pr=$(open_pr_state "$issue")"
      case "$detail" in
        *yes) detail="open-pr=yes — ORPHANED ENDGAME (#2499): report it, do NOT reap it" ;;
      esac
    fi
    printf '%-20s %-8s %-12s %-18s %s\n' "$machine" "$issue" "${pid:-none}" "$verdict" "$detail"
  done <<<"$raw"

  if [ "$unreadable" -gt 0 ]; then
    note "INCOMPLETE measurement: ${unreadable} claim ref(s) could not be read, so those lanes were not judged either way"
  fi
  if [ "$dead" -gt 0 ]; then
    note "${dead} dead lane(s) reported — this is a REPORT, nothing was deleted or reaped"
    return 3
  fi
  [ "$unreadable" -eq 0 ] || return 1
  return 0
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
  stamp)
    shift
    cmd_stamp "${1:-}" "${2:-}"
    ;;
  list-claims)
    cmd_list_claims
    ;;
  should-reap)
    shift
    cmd_should_reap "${1:-}" "${2:-}"
    ;;
  reap)
    shift
    cmd_reap "${1:-}"
    ;;
  dead-lanes)
    shift
    cmd_dead_lanes "$@"
    ;;
  -h | --help)
    print_help
    ;;
  "")
    die_usage "a subcommand is required: beat <issue> | list | clear <machine> | stamp <issue> [pid] | list-claims | should-reap <machine> [secs] | reap <machine> | dead-lanes"
    ;;
  *)
    die_usage "unknown subcommand: $SUBCOMMAND (expected beat|list|clear|stamp|list-claims|should-reap|reap|dead-lanes)"
    ;;
esac
