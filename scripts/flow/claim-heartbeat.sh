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
#   claim-heartbeat.sh should-reap <machine> [threshold_secs]           # LEGACY per-machine ref
#   claim-heartbeat.sh should-reap <machine> <issue> <threshold_secs>   # a LANE (all three required)
#                                            # exit 0 iff the claim ref is stale
#                                            # (age > threshold, default 14400s/4h)
#                                            # AND its issue has NO open PR AND
#                                            # (pid-dead, when the claim is local);
#                                            # exit 1 = keep (still live / open PR /
#                                            # foreign-pid unknowable); exit 2 = no ref.
#   claim-heartbeat.sh reap <machine> [issue] # delete a LANE's claim ref, but
#                                            # REFUSE if its issue has an open PR.
#   claim-heartbeat.sh dead-lanes            # REPORT every claim whose owning
#                                            # process is verifiably gone, with NO
#                                            # age threshold and regardless of an
#                                            # open PR. exit 3 = a dead lane was
#                                            # reported; exit 0 = at least one LOCAL
#                                            # lane was measured and none is dead;
#                                            # exit 1 = the measurement was incomplete
#                                            # (which includes zero claim refs, an
#                                            # all-foreign run, and a failed legacy
#                                            # listing). Exit 0 became trustworthy only
#                                            # with per-lane refs (#3393 ruling A) — see
#                                            # the scope limits below for what it does
#                                            # NOT claim.
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
#     DEAD-NO-PROCESS   local claim whose pid is gone from the process table, or is
#                       a ZOMBIE (exited, awaiting reap — it cannot drive a lane).
#     DEAD-PID-REUSED   the pid exists but STARTED AFTER the claim was stamped, so it
#                       cannot be the process that stamped it: the lane owner is gone
#                       and an unrelated process now holds its pid.
#     ALIVE             local claim whose pid exists AND whose start time precedes the
#                       claim ts, i.e. identity established.
#     UNKNOWN-IDENTITY  the pid exists but identity could not be established (start
#                       time or claim ts unreadable, or the start falls inside the
#                       rounding window) — pid reuse is NOT excluded.
#     UNKNOWN-FOREIGN   claim owned by another machine; its pid is unknowable from
#                       here, so it is reported as neither.
#     UNKNOWN-NO-PID    local claim ref carries no pid (pre-#2655 or hand-made).
#     UNKNOWN-STATE     the pid exists but its process state could not be read, so
#                       running-vs-zombie was never established.
#     UNKNOWN-PROBE     `ps` could not answer even for a known-present pid, so nothing
#                       about this pid was established. Especially relevant on the
#                       exhausted hosts this command exists for: a box that cannot fork
#                       cannot run `ps`.
#     UNKNOWN-UNREADABLE  the ref listed but would not fetch.
#   BOTH `DEAD-*` verdicts set the finding code 3. The `UNKNOWN-*` verdicts are gaps in
#   what this run could determine: every one of them EXCEPT `UNKNOWN-FOREIGN` makes the
#   measurement incomplete (exit 1 when nothing dead was found), because a local lane
#   this host could not judge is not a lane this host may call healthy. `UNKNOWN-FOREIGN`
#   is excluded because it is unknowable BY DESIGN and counting it would pin a healthy
#   multi-machine fleet at exit 1 forever.
#
#   TWO SCOPE LIMITS, BOTH STATED RATHER THAN SHIPPED QUIETLY — a monitor whose bounds
#   are undocumented is read as covering more than it does, which is its own false-clean.
#
#   (1) LOCAL-ONLY. A pid is checkable only on the machine that owns the claim, so this
#   reports on THIS machine's lanes and marks foreign ones UNKNOWN-FOREIGN. A run that
#   measured no local lane at all exits 1 saying so, never 0 — otherwise a run from an
#   operator box or CI would report "no dead lanes" about a fleet it never inspected
#   (roborev round 4, High). To sweep a fleet, run it ON each box.
#
#   (2) A LANE THAT NEVER STAMPED IS INVISIBLE — which is what remains after ruling A, and is a
#   much smaller bound than what stood here before. Until #3393's ruling, claims were keyed
#   per MACHINE (`refs/machine-claims/<machine>`) and force-updated every supervisor iteration, so
#   several lanes on one box overwrote each other and a SURVIVING sibling actively MASKED a dead
#   lane: `dead-lanes` reported ALIVE, identity verified, for a box that had just lost a lane. That
#   is why exit 0 was withheld entirely in the interim. Claims are now per LANE
#   (`refs/lane-claims/<machine>/<issue>`, or `p<pid>` while a supervisor's issue is unknown), so a
#   sibling stamps a DIFFERENT ref and a dead lane's ref survives with its dead pid.
#
#   What is still not covered: a lane that stamps nothing at all — run with `CLAIM_CMD=""`, or whose
#   stamps have been failing — cannot be reported, because there is no ref to read. That is why zero
#   claim refs is exit 1 rather than 0, and why exit 0 says "at least one local lane was measured"
#   rather than "this box is healthy". Cross-check the board and open PRs when it matters.
#
#   (2b) A JUST-SPAWNED LANE READS UNKNOWN-IDENTITY FOR A SHORT WHILE, BY DESIGN. The
#   supervisor stamps at spawn, so immediately after a lane starts its process start time and
#   its claim `ts` are within a second or two of each other — inside the rounding band, where
#   identity cannot be established either way. The run therefore reports UNKNOWN-IDENTITY and
#   exits 1 for a perfectly healthy new lane. It resolves on the supervisor's next stamp
#   refresh, after which `ts` has moved forward and the start clearly predates it. This is the
#   accepted cost of refusing to call the band ALIVE (see the band comment below): the
#   alternative claims health it has not established. Expect it on a lane that started
#   seconds ago; treat it as a finding only if it persists across refreshes.
#
#   (3) CLOCK-STEP SENSITIVITY, same root cause, same escalation. Identity compares the
#   claim's wall-clock `ts` against a start epoch reconstructed as `now - elapsed`. Those
#   are the same clock but read at different times, so an NTP step between stamping and
#   inspection shifts the comparison: a backward step can make a reused pid look
#   consistent (false ALIVE), a forward step can make a live supervisor look reused
#   (false DEAD-PID-REUSED). Fixing it properly means recording a stable process identity
#   AT STAMP TIME — boot id plus start ticks — which is the same change to `stamp` that
#   (2) needs, hence the same owner decision. Elapsed time was chosen over parsing
#   `ps -o lstart=` because that string carries no timezone and was measured to be
#   19,800s wrong on a non-UTC host; between the two, a clock STEP is far rarer than a
#   non-UTC host.
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
#   3  MEANING IS PER-SUBCOMMAND (roborev round 7, Low — this was documented as
#      dead-lanes-exclusive, which it never was): for `clear` and `reap` it means the
#      deletion was REFUSED because the issue still has an open PR; for `dead-lanes` it
#      means at least one DEAD-* lane was reported. Both are "the command ran and
#      declined to report success", never a failure of the command itself.
#      For `dead-lanes` specifically: at least one DEAD-* lane was reported. A
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

# Whether this git understands `git fetch --no-write-fetch-head` (landed in git 2.29).
# Computed ONCE: it is a property of the binary, not of a row.
GIT_SUPPORTS_NO_WRITE_FETCH_HEAD=no
_git_ver="$(git --version 2>/dev/null | awk '{print $3}')"
_git_major="${_git_ver%%.*}"
_git_rest="${_git_ver#*.}"
_git_minor="${_git_rest%%.*}"
case "$_git_major$_git_minor" in
  *[!0-9]* | '') : ;;  # unparseable version -> assume unsupported, never guess yes
  *)
    if [ "$_git_major" -gt 2 ] || { [ "$_git_major" -eq 2 ] && [ "$_git_minor" -ge 29 ]; }; then
      GIT_SUPPORTS_NO_WRITE_FETCH_HEAD=yes
    fi
    ;;
esac
unset _git_ver _git_major _git_rest _git_minor

# Rounding tolerance when comparing a process's start time against its claim's `ts`
# (#3393). Both come from the SAME host and the SAME clock — `ts` is written by `date -u`
# at stamp time, and the start epoch is `now - elapsed` — and the stamp necessarily
# happens AFTER the process starts, so a genuine claim always has start <= ts. The only
# error to absorb is second-resolution rounding on each side, so this is 2s, not the 60s
# the first cut used (roborev round 4, Medium: a 60s window is long enough for a real
# pid recycle, and anything inside the window is reported UNKNOWN-IDENTITY rather than
# ALIVE, so widening it buys nothing but lost detection).
PID_IDENTITY_SLACK_SECS="${PID_IDENTITY_SLACK_SECS:-2}"

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
# lane_id_ok <id> — exit 0 iff <id> is a valid lane component (#3393).
#
# TWO SHAPES, deliberately: a bare issue number, or `p<pid>` for a supervisor whose issue is not yet
# known. The placeholder exists because `CLAIM_ISSUE` is cleared on `finalized`, so a supervisor
# finalising issue after issue never knows its issue at spawn time — and a SHARED placeholder (the
# old "0") made every such supervisor on a machine write the same ref, which is the masking ruling A
# removes. `p<pid>` is unique per supervisor and stays out of the numeric issue space, so the
# open-PR guard and the board flip both correctly decline to treat it as an issue.
lane_id_ok() {
  case "$1" in
    '') return 1 ;;
    # `p<pid>` or `p<pid>-<token>`: the token is per-invocation, so pid reuse across a reboot cannot
    # collide two lanes onto one ref (roborev round 3). Hex and dashes only — no path separators.
    p*) case "${1#p}" in '' | *[!0-9a-f-]*) return 1 ;; *) return 0 ;; esac ;;
    *) case "$1" in *[!0-9]*) return 1 ;; *) return 0 ;; esac ;;
  esac
}

# lane_id_is_placeholder <id> — exit 0 iff <id> names no issue (a `p…` lane).
lane_id_is_placeholder() {
  case "$1" in p*) return 0 ;; *) return 1 ;; esac
}

# lane_claim_ref <machine> <issue> — the ref a LANE's claim lives at (#3393, owner ruling A:
# per-lane identity, one ref each).
#
# WHY A NEW NAMESPACE RATHER THAN A SUB-PATH UNDER THE OLD ONE. The obvious move is
# `refs/machine-claims/<machine>/<issue>`, and git forbids it: a ref cannot be both a file and a
# directory, so while any legacy `refs/machine-claims/<machine>` exists the sub-path cannot be
# created at all —
#   fatal: cannot lock ref '...box3/3367': 'refs/machine-claims/box3' exists
# The other single-component option, `<machine>-<issue>`, is AMBIGUOUS: real machine names already
# contain dashes (`ip-172-31-7-163-3367` cannot be split back). So the namespace changes, the last
# component is the issue, and the legacy namespace is left drainable rather than needing a flag day.
lane_claim_ref() {
  printf 'refs/lane-claims/%s/%s\n' "$1" "$2"
}

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
  lane_id_ok "$issue" || die_usage "stamp requires a lane id: an issue number, or p<pid> when the issue is not yet known (got '${issue:-<none>}')"
  case "$pid" in
    *[!0-9]* | '') die_usage "stamp pid must be numeric (got '${pid}')" ;;
  esac

  local machine ts commit_sha ref
  machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  ref="$(lane_claim_ref "$machine" "$issue")"
  commit_sha="$(push_liveness_ref "$ref" \
    "claim issue=${issue} machine=${machine} pid=${pid} ts=${ts}" "$ts")"
  note "claim stamped: machine=$machine issue=$issue pid=$pid ts=$ts -> $ref ($commit_sha)"
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
# delete_ref_guarded <namespace> <suffix> [expected_sha] — <suffix> is everything after
# `refs/<namespace>/`, so it is a bare machine for `refs/heartbeats/*` and `<machine>/<issue>` for a
# lane claim (#3393).
#
# ABSENCE MUST BE CONFIRMED, NOT ASSUMED (roborev round 3, Medium). This treated every failed
# `ls-remote` as "already absent" and returned SUCCESS, so a transient remote or auth failure made
# the supervisor log a successful claim clear, and made CI proceed as though a ref had been reaped,
# while the ref was still there. `--exit-code` gives 2 for a confirmed no-match and something else
# (measured: 128) for an operational failure; only the former is absence.
#
# DELETION TAKES A LEASE when the caller supplies the sha it evaluated (roborev round 3, Medium).
# Reaping was described as atomic and was not: `should-reap` judged one value and the delete removed
# whatever was there NOW, so a supervisor refresh landing in between was destroyed and its board item
# flipped back to Ready under a live lane. `--force-with-lease` makes the delete refuse if the ref
# moved, which is the same compare-and-swap discipline `claim.sh adopt --expect` already uses.
delete_ref_guarded() {
  local namespace="$1" suffix="$2" expected_sha="${3:-}"
  local ref="refs/${namespace}/${suffix}"

  local ls_rc=0
  git ls-remote --exit-code "$REMOTE" "$ref" >/dev/null 2>&1 || ls_rc=$?
  case "$ls_rc" in
    0) : ;;
    2)
      note "${ref} already absent on $REMOTE — nothing to clear"
      return 0
      ;;
    *)
      note "could not determine whether ${ref} exists on $REMOTE (git exited ${ls_rc}; 2 would mean confirmed-absent, so this is an operational failure). NOT reporting a successful clear for a ref that may still be there."
      return 1
      ;;
  esac

  local issue
  issue="$(ref_msg_field "$ref" issue)"
  if [ -n "$issue" ] && issue_has_open_pr "$issue"; then
    note "REFUSING to delete ${ref}: issue #${issue} has an open PR (endgame unfinished; #2655)"
    return 3
  fi

  if [ -n "$expected_sha" ]; then
    if git push "$REMOTE" --force-with-lease="${ref}:${expected_sha}" --delete "$ref"; then
      note "cleared ${ref} on $REMOTE (lease held at ${expected_sha})"
      return 0
    fi
    note "REFUSING to delete ${ref}: the lease at ${expected_sha} was not held — the ref moved, so a live supervisor refreshed it between the verdict and this delete"
    return 4
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

# cmd_reap <machine> [issue] — delete a LANE's claim ref. With <issue> the new per-lane ref is
# deleted; without it the LEGACY per-machine ref is, which is how a pre-ruling ref still gets
# drained (see the legacy note in the header). Refuses under an open PR either way.
cmd_reap() {
  local machine="${1:-}" issue="${2:-}" lease="${3:-}"
  [ -n "$machine" ] || die_usage "reap requires <machine> [issue] [expected_sha]"
  if [ -n "$issue" ]; then
    lane_id_ok "$issue" || die_usage "reap lane id must be an issue number or p<pid> (got '$issue')"
    delete_ref_guarded lane-claims "${machine}/${issue}" "$lease"
  else
    delete_ref_guarded machine-claims "$machine" "$lease"
  fi
}

cmd_list_claims() {
  local now_epoch raw legacy
  now_epoch="$(date -u +%s)"
  # BOTH namespaces (#3393). New claims live at refs/lane-claims/<machine>/<issue>; a legacy
  # refs/machine-claims/<machine> from before the per-lane ruling is still listed so it can be seen
  # and drained — an invisible legacy ref pins its board item at In Progress indefinitely.
  raw="$(git ls-remote "$REMOTE" 'refs/lane-claims/*' 2>/dev/null || true)"
  legacy="$(git ls-remote "$REMOTE" 'refs/machine-claims/*' 2>/dev/null || true)"
  [ -z "$legacy" ] || raw="$(printf '%s\n%s' "$raw" "$legacy")"
  raw="$(printf '%s' "$raw" | sed '/^$/d')"

  if [ -z "$raw" ]; then
    echo "no claims found on $REMOTE"
    return 0
  fi

  printf '%-20s %-8s %-10s %-24s %s\n' "MACHINE" "ISSUE" "PID" "TS" "AGE"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local refname machine msg issue pid ts epoch age_h
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    case "$refname" in
      refs/lane-claims/*)
        # <machine>/<issue> — the issue is the LAST component, which is why the separator is a
        # slash and not a dash (machine names contain dashes).
        machine="${refname#refs/lane-claims/}"
        machine="${machine%/*}"
        ;;
      *) machine="${refname#refs/machine-claims/} (legacy)" ;;
    esac

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
# cmd_should_reap <machine> [threshold_secs] | <machine> <issue> <threshold_secs>
#
# THE TWO-ARGUMENT FORM IS ALWAYS THE LEGACY THRESHOLD; A LANE REQUIRES ALL THREE (roborev round 1,
# Medium). An earlier cut tried to disambiguate `<machine> <N>` at runtime by probing the remote for
# a lane ref named `<N>`, which is worse than it looks: the CI workflow's legacy threshold is
# `14400`, so the moment some lane legitimately carries issue 14400 that call would silently switch
# from "legacy ref, 4h threshold" to "lane 14400, default threshold" and could then delete an
# unrelated legacy ref on the lane's verdict. A grammar whose meaning depends on which refs happen
# to exist is not a grammar. Ambiguity removed rather than resolved: two args is the legacy form,
# three args is a lane, and there is no case where the same call means two things.
cmd_should_reap() {
  local machine="${1:-}" a2="${2:-}" a3="${3:-}"
  local issue="" threshold="$DEFAULT_REAP_THRESHOLD_SECS"
  [ -n "$machine" ] || die_usage "should-reap requires <machine> [threshold_secs], or <machine> <issue> <threshold_secs> for a lane"
  if [ -n "$a3" ]; then
    issue="$a2"; threshold="$a3"
  elif [ -n "$a2" ]; then
    threshold="$a2"
  fi
  case "$threshold" in
    *[!0-9]* | '') die_usage "should-reap threshold must be numeric seconds (got '${threshold}')" ;;
  esac
  if [ -n "$issue" ]; then
    lane_id_ok "$issue" || die_usage "should-reap lane id must be an issue number or p<pid> (got '$issue')"
  fi
  # A PLACEHOLDER LANE IS NEVER AUTOMATICALLY REAPED (roborev round 3, Medium). A `p…` id names no
  # issue, so the open-PR guard has nothing to consult — and a worker can have claimed an issue and
  # opened a PR before its supervisor ever received the marker, or left an auto-merge PR open after
  # `CLAIM_ISSUE` was cleared. Reaping it would delete the claim of a lane with an unfinished
  # endgame, which is the #2499 case the guard exists for. The owning supervisor still clears its own
  # placeholder on clean exit by calling `reap` directly — it knows it is finished; a reaper cannot.
  if [ -n "$issue" ] && lane_id_is_placeholder "$issue"; then
    note "keep refs/lane-claims/${machine}/${issue}: placeholder lane id names no issue, so an open PR cannot be ruled out — never automatically reaped (#3393)"
    return 1
  fi
  local ref
  if [ -n "$issue" ]; then
    ref="$(lane_claim_ref "$machine" "$issue")"
  else
    ref="refs/machine-claims/${machine}"
  fi

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

# process_start_window <pid> — echo `<earliest> <latest>` epoch seconds bracketing when <pid>
# started, or EMPTY when it cannot be determined. Empty is a THIRD answer and is never folded
# onto "consistent".
#
# DERIVED FROM ELAPSED TIME, NOT A WALL-CLOCK STRING (roborev round 3, Medium). The first cut
# read `ps -o lstart=` and parsed it with `date -u`, but `lstart` is LOCAL wall time with no
# zone in it, so on any non-UTC host the epoch came out shifted by the offset — MEASURED: the
# same lstart parses 19,800s apart between UTC and Asia/Kolkata, far past the tolerance, which
# would falsely declare a live supervisor DEAD-PID-REUSED. Elapsed seconds carry no timezone at
# all, so the whole class is gone rather than corrected.
#
# AN INTERVAL, NOT A POINT (roborev round 15, Medium). `start = now - elapsed` needs `now` and
# `elapsed` to refer to the same instant, and they cannot: one is read before the other. The
# first cut sampled `now` BEFORE running `ps`, so a slow `ps` shifted the computed start
# BACKWARD — and a start that looks earlier than it is makes a REUSED pid look like it predates
# the claim, i.e. a false ALIVE. That delay is most likely on exactly the resource-exhausted
# hosts this command exists for. So the query is bracketed and both bounds are returned; the
# caller must decide UNKNOWN when the interval straddles its decision boundary.
process_start_window() {
  local pid="$1" secs t0 t1
  t0="$(date -u +%s)"
  secs="$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')"
  case "$secs" in
    '' | *[!0-9]*) secs="" ;;
  esac
  if [ -z "$secs" ]; then
    # Fall back to `etime` ([[DD-]HH:]MM:SS), which POSIX ps provides where `etimes` is
    # absent. Still elapsed, still timezone-free.
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
  t1="$(date -u +%s)"
  # The elapsed reading was taken at some instant in [t0, t1], so the start lies in
  # [t0 - secs, t1 - secs]. Earliest first.
  printf '%s %s\n' "$((t0 - secs))" "$((t1 - secs))"
}

# ps_usable — exit 0 iff `ps` can be trusted to answer an existence question here.
#
# SELF-VALIDATING, because "nonzero" is not the same as "absent" (roborev round 8, Medium).
# A `ps` that is missing, unsupported, or simply unable to run would otherwise turn every
# claim into DEAD-NO-PROCESS and exit 3 — a fleet-wide false DEAD. That failure mode is not
# hypothetical on the boxes this issue is about: under the memory exhaustion #3393 records,
# a process that cannot fork cannot run `ps` either, so the ONE moment the report matters
# most is when the probe is most likely to fail. So the tool is validated against a pid
# that is certainly present — our own — before any of its answers are believed. This is
# necessary but NOT sufficient; see `process_presence` for why a per-TARGET vote is also
# needed (round 9).
ps_usable() {
  ps -p "$$" >/dev/null 2>&1
}

# signal_probe_class <pid> — echo `present` | `absent` | `denied` | `unknown`.
#
# `kill -0` is the ONE probe here that is not visibility-based, which is why its failure mode
# has to be decoded rather than abstained on (roborev round 10, Medium). EPERM means the
# process EXISTS and is simply not ours; ESRCH means it is gone. Treating both as "no
# opinion" made every remaining voter a VISIBILITY probe — `ps` and `/proc/<pid>` are
# correlated, both hidden by `hidepid=2` — so a different user's live process was unanimously
# "absent" and reported DEAD.
#
# `LC_ALL=C` is load-bearing: the distinction is drawn from the error text, so the message
# has to be in a known language. An unrecognised message is `unknown`, never folded onto
# either answer.
signal_probe_class() {
  local pid="$1" err
  if kill -0 "$pid" 2>/dev/null; then
    printf 'present\n'
    return 0
  fi
  err="$(LC_ALL=C kill -0 "$pid" 2>&1 || true)"
  case "$err" in
    *"not permitted"* | *"Not permitted"* | *"Operation not permitted"*) printf 'denied\n' ;;
    *"No such process"* | *"no such process"*)                           printf 'absent\n' ;;
    *)                                                                   printf 'unknown\n' ;;
  esac
}

# process_presence <pid> — echo `present` | `absent` | `unknown`.
#
# BUILT FROM AGREEING VOTES, because a NEGATIVE answer from one probe is not proof of
# absence (roborev round 9, Medium). `ps -p` exiting nonzero can mean the process is gone,
# but it can equally mean a transient failure under load or that the target is not visible
# to us — and reading that as absence reports a LIVE supervisor as DEAD. Validating `ps`
# against our OWN pid (round 8) was necessary but not sufficient: it proves the tool runs,
# not that it can see THIS target.
#
# THE VOTERS MUST NOT ALL MEASURE THE SAME THING (round 10). `ps -p` and `/proc/<pid>` are
# both VISIBILITY probes and are hidden together by `hidepid=2`, so on their own they can be
# unanimously and confidently wrong about a live process owned by another user. The signal
# probe is the independent one, and its EPERM answer is affirmative evidence of EXISTENCE —
# which is exactly the case the other two get wrong.
#
# Unanimous present => present. Unanimous absent => absent. DISAGREEMENT => unknown: our view
# of the process table is not self-consistent, so nothing is claimed either way.
process_presence() {
  local pid="$1" yes=0 no=0 sig
  if ps -p "$pid" >/dev/null 2>&1; then yes=$((yes + 1)); else no=$((no + 1)); fi
  if [ -d /proc ]; then
    if [ -e "/proc/$pid" ]; then yes=$((yes + 1)); else no=$((no + 1)); fi
  fi
  sig="$(signal_probe_class "$pid")"
  case "$sig" in
    present | denied) yes=$((yes + 1)) ;;   # denied == EPERM == it exists
    absent)           no=$((no + 1)) ;;
    unknown)          : ;;                  # genuinely no opinion; abstains
  esac

  if [ "$yes" -gt 0 ] && [ "$no" -eq 0 ]; then
    printf 'present\n'
  elif [ "$yes" -eq 0 ] && [ "$no" -gt 0 ] && [ "$sig" = absent ]; then
    # ABSENCE REQUIRES THE INDEPENDENT PROBE TO SAY SO (roborev round 15, Medium). Round 10
    # fixed the case where the signal probe answered `denied`, but when it answers `unknown`
    # the only remaining voters are `ps` and `/proc` — which are BOTH visibility probes,
    # hidden together by `hidepid=2`. They can then be unanimously and confidently wrong
    # about a live process, and this function would call it absent: a false DEAD. So a
    # declaration of absence needs the one non-visibility probe to have affirmed it.
    printf 'absent\n'
  else
    printf 'unknown\n'
  fi
}

# process_state_class <pid> — echo `zombie` | `running` | `unreadable`.
#
# THREE-VALUED, and that is the whole point (roborev round 7, Medium). The first cut was a
# two-valued `process_is_zombie` that returned "not a zombie" when the state could not be
# read — after which a readable start time produced ALIVE and exit 0. So an unreadable
# state became a CLEAN result, which is the same "unknown folded onto the permissive
# answer" shape this command exists to avoid, reintroduced one level down in the fix for
# the previous round. The unreadable case must be neither: not `zombie` (a false DEAD on a
# healthy fleet is how a monitor gets ignored) and not `running` (that is the false-clean).
process_state_class() {
  local st
  st="$(ps -o stat= -p "$1" 2>/dev/null | tr -d ' ')"
  case "$st" in
    '') printf 'unreadable\n' ;;
    Z*) printf 'zombie\n' ;;
    *)  printf 'running\n' ;;
  esac
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

  local raw this_machine dead=0 unreadable=0 lsrc local_seen=0 local_listed=0 row=0
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
  raw="$(git ls-remote "$REMOTE" 'refs/lane-claims/*' 2>"$errfile")"
  lsrc=$?
  set -e
  if [ "$lsrc" -ne 0 ]; then
    note "could not list claim refs on ${REMOTE} (git exited ${lsrc}) — the fleet was NOT measured, so this is NOT 'no dead lanes'. git said: $(tr '\n' ' ' <"$errfile")"
    rm -f "$errfile"
    return 1
  fi
  rm -f "$errfile"

  # Legacy per-machine refs are reported too, so a pre-ruling lane is not invisible during the
  # drain (#3393).
  #
  # A FAILURE LISTING THEM MAKES THE MEASUREMENT INCOMPLETE (roborev round 1, Medium). I first wrote
  # this as non-fatal, reasoning that the per-lane listing had already given the run a subject and
  # the legacy set "only ever adds rows". That reasoning is wrong in the direction that matters: if
  # the legacy listing fails while a healthy per-lane lane is measured, a DEAD LEGACY lane is
  # invisible and the run still exits 0 — the same false-clean shape this command guards against
  # everywhere else, reached through the drain path.
  local legacy legacy_rc=0
  legacy="$(git ls-remote "$REMOTE" 'refs/machine-claims/*' 2>/dev/null)" || legacy_rc=$?
  if [ "$legacy_rc" -ne 0 ]; then
    note "could not list LEGACY claim refs on ${REMOTE} (git exited ${legacy_rc}) — a pre-ruling lane could be dead and unseen, so this measurement is INCOMPLETE"
    unreadable=$((unreadable + 1))
    legacy=""
  fi
  [ -z "$legacy" ] || raw="$(printf '%s\n%s' "$raw" "$legacy")"
  raw="$(printf '%s' "$raw" | sed '/^$/d')"

  # REFUSE RATHER THAN CLOBBER (roborev round 13, Medium). Omitting
  # `--no-write-fetch-head` on old git left this monitor overwriting the shared FETCH_HEAD
  # that `list`, `list-claims`, `should-reap` and `reap` still fetch-then-read — so the
  # fallback traded "I cannot measure safely" for "I may corrupt someone else's REAP
  # decision", which is the worse of the two by a wide margin. A monitor may decline to
  # answer; it may not damage the thing it is monitoring.
  if [ "$GIT_SUPPORTS_NO_WRITE_FETCH_HEAD" != yes ]; then
    note "this git cannot fetch without writing FETCH_HEAD (needs 2.29+; found '$(git --version 2>/dev/null || echo unknown)'). Refusing to run rather than clobbering the FETCH_HEAD that list/should-reap/reap read — upgrade git, or run dead-lanes from a checkout with a newer one. NOTHING was measured."
    return 1
  fi

  # NO EARLY `return 0` HERE (roborev round 5, Medium). An empty namespace proves only
  # that no ref exists — it does NOT establish an idle fleet. A lane running with
  # `CLAIM_CMD=""` (stamping deliberately disabled, a documented supervisor option) or one
  # whose stamps have been failing all along looks EXACTLY the same from here, so exiting
  # 0 would report clean about lanes that were never measured. It is also the same
  # condition as the all-foreign case: nothing local was inspected. Both now fall through
  # to the single `local_seen` check at the end, so one rule covers both instead of an
  # early return contradicting it.
  if [ -z "$raw" ]; then
    note "no claim refs exist on ${REMOTE}. That is NOT the same as an idle fleet: a lane running with claim stamping disabled (CLAIM_CMD=\"\"), or one whose stamps have been failing, is indistinguishable from this. Nothing was measured, so this is NOT 'no dead lanes'."
    return 1
  fi

  printf '%-20s %-8s %-12s %-18s %s\n' "MACHINE" "ISSUE" "PID" "VERDICT" "DETAIL"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local refname machine msg issue pid ts verdict detail
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    local lane_issue=""
    case "$refname" in
      refs/lane-claims/*)
        machine="${refname#refs/lane-claims/}"
        lane_issue="${machine##*/}"   # LAST component is the issue; see lane_claim_ref
        machine="${machine%/*}"
        ;;
      *) machine="${refname#refs/machine-claims/}" ;;
    esac

    # FETCH INTO AN INVOCATION-UNIQUE REF, never `FETCH_HEAD` (roborev round 11, Medium).
    # `FETCH_HEAD` is shared per-worktree, so ANY concurrent fetch — and this repository is
    # routinely worked by several sessions in one checkout — can overwrite it between the
    # fetch and the read, making this row report another ref's pid and ts. That is a false
    # ALIVE or a false DEAD attributed to the wrong machine, which is worse than either
    # error alone. The temp ref is unique per process AND per row, and is deleted after use.
    row=$((row + 1))
    local tmpref
    tmpref="refs/tmp/claim-heartbeat/$$-${row}"
    # `--no-write-fetch-head` matters for our NEIGHBOURS, not for us (roborev round 12,
    # Medium). Fetching into a private ref stopped THIS command reading a clobbered
    # FETCH_HEAD, but the fetch still WROTE it — so a concurrent `dead-lanes` became the
    # thing that corrupts `list`, `list-claims`, `should-reap` and `reap`, all of which
    # still fetch-then-read FETCH_HEAD. Making a monitor into the cause of a bad REAP
    # decision would be a poor trade. Guarded by version because the option landed in git
    # 2.29; on older git the flag is omitted rather than failing every row with an
    # unknown-option error whose stated cause would be "fetch failed".
    # Counted from the REFNAME, before any fetch: whether the claim belongs to this machine is
    # known from the ref path, so an unreadable local ref must not be mistaken for "no local
    # claim exists" in the closing diagnostic (roborev round 16, Low).
    [ "$machine" = "$this_machine" ] && local_listed=$((local_listed + 1))
    if ! git fetch --no-write-fetch-head --no-tags "$REMOTE" "+${refname}:${tmpref}" >/dev/null 2>&1; then
      # An unreadable ref is "we cannot tell about THIS lane", never "this lane is
      # fine" — so it is both printed AND counted toward an incomplete measurement.
      unreadable=$((unreadable + 1))
      printf '%-20s %-8s %-12s %-18s %s\n' "$machine" "?" "?" "UNKNOWN-UNREADABLE" "fetch of $refname failed"
      continue
    fi
    msg="$(git log -1 --format=%B "$tmpref" 2>/dev/null || true)"
    git update-ref -d "$tmpref" 2>/dev/null || true
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
      # measurement on its own: counting every foreign row would pin a healthy
      # multi-machine fleet at exit 1 forever and train readers to ignore the code.
      # What DOES matter is whether ANY local lane was measured — see `local_seen` at
      # the end of this function.
      verdict="UNKNOWN-FOREIGN"
      detail="pid not checkable from ${this_machine} — run dead-lanes ON ${machine}"
    else
      # A claim this machine owns: its pid is checkable, so this row is a real
      # measurement and the run is entitled to draw a conclusion from it.
      local_seen=$((local_seen + 1))
      if [ -z "$pid" ]; then
        # A LOCAL claim SHOULD carry a pid, so its absence is a real gap in what this
        # run could determine — counted (roborev round 2, Medium).
        verdict="UNKNOWN-NO-PID"
        detail="claim ref records no pid (pre-#2655 or hand-made)"
        unreadable=$((unreadable + 1))
      else
        # PRESENCE AND STATE ARE EACH READ ONCE (roborev round 10, Medium). The first cut
        # called `process_presence` in up to four successive `elif` guards, so a transient
        # change between them could leave every branch unmatched and fall through to a
        # final `else` that assumed absence — a false death produced by re-measuring. One
        # read, then an explicit dispatch over the three states it can return.
        local presence state
        if ! ps_usable; then
          presence="unknown"
        else
          presence="$(process_presence "$pid")"
        fi

        case "$presence" in
          unknown)
            verdict="UNKNOWN-PROBE"
            detail="the process probes could not agree about pid ${pid} (or ps cannot answer for a known-present pid), so it was NOT judged — repair ps, check /proc visibility, or re-run when the host is not exhausted"
            unreadable=$((unreadable + 1))
            ;;
          absent)
            verdict="DEAD-NO-PROCESS"
            dead=$((dead + 1))
            detail="open-pr=$(open_pr_state "$issue")"
            case "$detail" in
              *yes) detail="open-pr=yes — ORPHANED ENDGAME (#2499): report it, do NOT reap it" ;;
            esac
            ;;
          present)
            state="$(process_state_class "$pid")"
            case "$state" in
              unreadable)
                # In the process table, but running-vs-zombie was never established.
                verdict="UNKNOWN-STATE"
                detail="pid ${pid} exists but its process state could not be read — running vs zombie NOT established"
                unreadable=$((unreadable + 1))
                ;;
              zombie)
                verdict="DEAD-NO-PROCESS"
                dead=$((dead + 1))
                detail="supervisor pid ${pid} is a ZOMBIE (exited, awaiting reap — it cannot drive the lane); open-pr=$(open_pr_state "$issue")"
                ;;
              *)
                # Running. Existence still does NOT make it the process that stamped the
                # claim: pids are recycled, and a stale claim naming a reused pid would read
                # ALIVE — the dangerous direction for a monitor whose whole job is spotting a
                # dead lane. The claim's OWN ts settles it with no change to what `stamp`
                # records: a process that started AFTER the claim was stamped cannot be the
                # process that stamped it.
                local win pstart_min pstart_max cts
                win="$(process_start_window "$pid")"
                pstart_min="${win%% *}"
                pstart_max="${win##* }"
                cts=""
                [ -n "$ts" ] && cts="$(ts_to_epoch "$ts" 2>/dev/null || true)"
                if [ -z "$win" ] || [ -z "$cts" ]; then
                  # NOT "ALIVE" (round 3, Medium): an annotation is not a substitute for an
                  # exit code — nobody greps the annotation.
                  verdict="UNKNOWN-IDENTITY"
                  detail="pid ${pid} exists but its start time or the claim ts could not be read — pid reuse NOT excluded"
                  unreadable=$((unreadable + 1))
                elif [ "$pstart_min" -gt "$((cts + PID_IDENTITY_SLACK_SECS))" ]; then
                  # The EARLIEST the process can have started is still after the claim, so
                  # the whole interval is on the reuse side.
                  verdict="DEAD-PID-REUSED"
                  dead=$((dead + 1))
                  detail="pid ${pid} started at least $((pstart_min - cts))s AFTER the claim was stamped — a different process now holds it; open-pr=$(open_pr_state "$issue")"
                elif [ "$pstart_max" -lt "$((cts - PID_IDENTITY_SLACK_SECS))" ]; then
                  # The LATEST it can have started is still before the claim, so the whole
                  # interval predates it. Using the far bound in each direction is what makes
                  # the measurement delay unable to buy a verdict either way.
                  verdict="ALIVE"
                  detail="identity=verified (start predates the claim ts by more than the ${PID_IDENTITY_SLACK_SECS}s tolerance)"
                else
                  # THE TOLERANCE BAND IS ITS OWN ANSWER, ON BOTH SIDES (rounds 4 and 9).
                  # Within +/- tolerance is exactly where rounding could have produced either
                  # ordering, so it is not evidence of identity and cannot claim health.
                  verdict="UNKNOWN-IDENTITY"
                  detail="pid ${pid} started somewhere in [$((pstart_min - cts)), $((pstart_max - cts))]s relative to the claim ts, straddling the +/-${PID_IDENTITY_SLACK_SECS}s decision boundary — identity NOT established"
                  unreadable=$((unreadable + 1))
                fi
                ;;
            esac
            ;;
        esac
      fi
    fi
    printf '%-20s %-8s %-12s %-18s %s\n' "$machine" "$issue" "${pid:-none}" "$verdict" "$detail"
  done <<<"$raw"

  if [ "$unreadable" -gt 0 ]; then
    note "INCOMPLETE measurement: ${unreadable} claim ref(s) could not be judged either way"
  fi
  if [ "$dead" -gt 0 ]; then
    note "${dead} dead lane(s) reported — this is a REPORT, nothing was deleted or reaped"
    return 3
  fi
  # A RUN THAT INSPECTED NO LOCAL PROCESS CANNOT REPORT A CLEAN FLEET (roborev round 4,
  # High). This command is LOCAL-ONLY — a pid is checkable only on the machine that owns
  # the claim — so run from an operator box or CI, every row is UNKNOWN-FOREIGN and
  # nothing was actually measured. Exiting 0 there says "none found" about a fleet it
  # never looked at, which is the false-clean this whole command exists to prevent.
  # Individual foreign rows still do NOT each count as incomplete: on a healthy
  # multi-machine fleet that would pin the exit code at 1 forever and train readers to
  # ignore it. The distinction is "did I measure ANY local lane", not "is every lane
  # local".
  if [ "$local_seen" -eq 0 ]; then
    # Two different situations, and telling an operator the wrong one sends them to the wrong
    # box (roborev round 16, Low): either no claim here belongs to this machine, or one does
    # and could not be read.
    if [ "$local_listed" -gt 0 ]; then
      note "NOTHING WAS MEASURED: ${local_listed} claim ref(s) on ${REMOTE} DO belong to this machine (${this_machine}) but none could be read, so no lane was judged. This is NOT 'no dead lanes'."
    else
      note "NOTHING WAS MEASURED: claim refs exist on ${REMOTE} but none is owned by this machine (${this_machine}), and a pid is only checkable where it runs. dead-lanes is LOCAL-ONLY — run it ON the suspect box, or check each machine in turn. This is NOT 'no dead lanes'."
    fi
    return 1
  fi
  if [ "$unreadable" -gt 0 ]; then
    return 1
  fi
  # THE CLEAN VERDICT IS SOUND AGAIN, because the layout changed under it (#3393, owner ruling A).
  #
  # It was removed at interim C for a specific reason: claims were keyed per MACHINE and
  # force-updated every supervisor iteration, so after one lane died a surviving sibling's next
  # stamp overwrote the dead lane's pid and this command saw a live, identity-verified pid. Exit 0
  # there was a false clean about the exact scenario the command exists to catch, and documenting
  # that was not a fix — the exit code is what a cron reads.
  #
  # Per-lane refs remove the mechanism rather than mitigate it: a sibling now stamps a DIFFERENT
  # ref, so a dead lane's ref survives with its dead pid and is reported. The masking is gone, so
  # the clean verdict is honest and is restored.
  #
  # WHAT EXIT 0 CLAIMS, EXACTLY — no more than this: at least one LOCAL lane was measured, and none
  # of the local lanes that could be seen is dead. It does NOT claim anything about lanes that never
  # stamped (a lane run with `CLAIM_CMD=""` is still invisible, which is why zero claim refs remains
  # exit 1), nor about other machines (a foreign pid is unknowable, which is why an all-foreign run
  # remains exit 1). Those two guards are what keep this from becoming the old false clean by a
  # different route.
  note "measured ${local_seen} local lane(s); none is dead. Exit 0 means: at least one local lane was measured and none of them is dead — NOT that lanes which never stamped are healthy (they are invisible), and NOT anything about other machines. Run dead-lanes ON each box."
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
    cmd_should_reap "${1:-}" "${2:-}" "${3:-}"
    ;;
  reap)
    shift
    cmd_reap "${1:-}" "${2:-}" "${3:-}"
    ;;
  dead-lanes)
    shift
    cmd_dead_lanes "$@"
    ;;
  -h | --help)
    print_help
    ;;
  "")
    die_usage "a subcommand is required: beat <issue> | list | clear <machine> | stamp <issue> [pid] | list-claims | should-reap <machine> [issue] [secs] | reap <machine> [issue] | dead-lanes"
    ;;
  *)
    die_usage "unknown subcommand: $SUBCOMMAND (expected beat|list|clear|stamp|list-claims|should-reap|reap|dead-lanes)"
    ;;
esac
