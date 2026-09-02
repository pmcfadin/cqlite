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
#     ONE ref per machine — and that is still correct, but NOT for the reason this
#     line used to give. It cited #1930's "one worker per machine", which #3393
#     RETRACTED; a heartbeat is a per-MACHINE liveness proof by its own nature, and
#     stays one-per-machine however many lanes the box runs. (The per-LANE refs are
#     `refs/lane-claims/<machine>/<lane-id>` below.) Force-updated on every `beat` —
#     never a history of commits, just the latest liveness proof. The commit is a root commit (no parent) pointing at the empty
#     tree; its ONLY payload is the commit message:
#       "heartbeat issue=<N> machine=<machine> ts=<ISO8601 UTC>"
#     The commit's author/committer date is also set to `ts` (informational —
#     `list` always parses `ts` FROM THE MESSAGE, the pushed clock, never the
#     commit date, so a re-push through a clock-skewed relay or a rebasing
#     proxy can't silently corrupt the age computation).
#
#   refs/lane-claims/<machine>/<lane-id>   (issue #2655 / #2499 design, per-LANE since #3393)
#     A SUPERVISOR-authored claim ref, ONE PER LANE — the machine-driven complement to the
#     LLM-driven `beat`. `worker-supervisor.sh` (#2090) stamps it at spawn,
#     refreshes it every iteration, and clears it on a clean exit, so claim
#     liveness no longer depends on the worker LLM *remembering* to beat. Same
#     empty-tree/root-commit shape; the message carries the owning PID as well:
#       "claim issue=<N> machine=<machine> pid=<PID> ts=<ISO8601 UTC>"
#     The PID lets a reaper running ON THE CLAIM'S OWN MACHINE add a
#     process-liveness check (a beat-then-crash no longer looks alive for the
#     whole threshold window). A reaper running elsewhere (the project-board-sync
#     CI cron) can't see the PID, so it falls back to age + no-open-PR only.
#     <lane-id> is the issue number, or `p<pid>-<token>` while a supervisor has
#     stamped before its issue is known. ONE REF PER LANE, because a box runs
#     several lanes at once (#3393 retracted the old one-worker-per-machine
#     reading): a single per-machine ref was force-updated by every lane on the
#     box, so siblings overwrote each other and `dead-lanes` could see at most
#     one of them. A separate NAMESPACE rather than `<machine>-<issue>` because
#     git forbids a ref being both a file and a directory, and a machine name
#     may itself contain dashes.
#
#   refs/machine-claims/<machine>   LEGACY, read-only
#     The pre-#3393 per-machine shape. Still ENUMERATED by `list-claims`,
#     `dead-lanes` and the CI reaper so a pre-ruling ref gets drained — an
#     un-enumerated claim ref pins its board item at In Progress indefinitely —
#     and still accepted by `clear`/`reap`/`should-reap` when no lane id is
#     given. Nothing writes it any more.
#
#   refs/tmp/<subcommand>/<pid>-<n>   LOCAL scratch, never pushed
#     Every subcommand that READS a claim fetches it into a private ref here and
#     deletes it immediately, rather than through the shared `FETCH_HEAD` that any
#     concurrent fetch in the same checkout can clobber between the fetch and the
#     read. Nothing outside this script consumes them; a killed run can leave one
#     behind, and `git update-ref -d` on it is always safe.
#
#     NOTE both namespaces are deliberately DISTINCT from
#     `scripts/flow/claim.sh`'s per-issue LOCK refs `refs/claims/issue-<N>`
#     (#2665) — these are liveness proofs, not the issue lock, and the
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
#   claim-heartbeat.sh stamp <lane-id> [pid] # push/refresh THIS LANE's claim ref
#                                            # (<lane-id> = an issue number, or p<pid>-<token>
#                                            #  while the issue is not yet known)
#                                            # (supervisor-authored; pid default $$)
#   claim-heartbeat.sh list-claims          # one line per LANE: machine/lane-id/pid/ts/age
#                                            # (several lines per machine — N lanes per box)
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
#                                            # open PR. POSITIVE DETECTION ONLY in this
#                                            # slice — it NEVER exits 0 (#3393 split).
#                                            # exit 3 = a dead lane was reported;
#                                            # exit 1 = none was reported, which also
#                                            # covers zero claim refs, an all-foreign
#                                            # run and a failed listing. Act on 3;
#                                            # never read 1 as a clean bill of health.
#                                            # A sound clean verdict is possible on
#                                            # per-lane refs and is tracked separately.
#                                            # SUPERVISOR FLEETS ONLY (DESCOPED, #3548) —
#                                            # see SCOPE OF `dead-lanes` below.
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
#   session convention — was left to #3393 and has since been DECIDED, not
#   deferred: the owner DESCOPED it on 2026-09-01 (see the scope note directly
#   below). Do not read this paragraph as an open question; it is settled.
#
# SCOPE OF `dead-lanes`: SUPERVISOR FLEETS ONLY — DESCOPED (owner ruling 2026-09-01 on
# #3548, option C; completes #3393)
#   The subject set is `refs/lane-claims/<machine>/<lane-id>` plus the legacy
#   `refs/machine-claims/<machine>`. The only IN-TREE CALLER that writes either is
#   `scripts/local/worker-supervisor.sh` (through `stamp`) — but `stamp` is a PUBLIC
#   subcommand and can be invoked directly, so a manually stamped fleet legitimately
#   carries refs; the legacy namespace has no current writer at all. EXIT 1 MEANS "NOTHING WAS
#   REPORTED", NEVER a clean bill of health (#3467) — including when the subject set is
#   empty, as it was on this fleet when #3548 was measured (see the runbook below).
#   The two POPULATED namespaces are deliberately NOT read, and both refusals are measured:
#   `refs/claims/issue-<N>` records the TRANSIENT CLAIMING SHELL's pid, never refreshed
#   (measured dead while its lane ran, so it would report healthy lanes dead), and
#   `refs/heartbeats/<machine>` is SINGLE-SLOT PER MACHINE, so N lanes overwrite each other.
#   Being UNENUMERATED is the abstention: neither yields a row or a verdict of any kind.
#   AC4 as a COUNTERFACTUAL: WERE a later change ever to read a NON-REFRESHING carrier, a
#   stale pid there must never yield a `DEAD-*` verdict — it must abstain; `refs/lane-claims/*`
#   is restamped every supervisor iteration, so `DEAD-*` there is correct.
#   Everything else — what the fleet's liveness actually rests on, both board signatures and
#   the measurement itself — is stated ONCE in docs/development/fleet-runbook.md, section
#   "Lane liveness on a supervisor-less `/drive-issue` fleet". It is deliberately not
#   restated here: seven review rounds on #3548 were propagation failures of duplicated prose.
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
# REQUIREMENTS
#   git 2.29+ for every subcommand that READS a ref (`list`, `list-claims`, `clear`,
#   `reap`, `should-reap`, `dead-lanes`). Those fetch into a private ref with
#   `--no-write-fetch-head`, which landed in 2.29; on an older git they REFUSE with a
#   named message and exit 1 rather than fetch without it, because the fetch would
#   overwrite the `FETCH_HEAD` that other tooling in the same checkout reads. `beat` and
#   `stamp` only PUSH and work on any git. There is no fallback and no override: writing
#   someone else's `FETCH_HEAD` can corrupt a REAP decision, which is worse than
#   declining to answer.
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

# EVERY FETCHING SUBCOMMAND NEEDS THIS GUARD, NOT JUST THE NEWEST ONE (roborev round 17,
# Medium). `dead-lanes` refused on a git that cannot fetch privately; `list`,
# `list-claims`, `clear`, `reap` and `should-reap` did not, though they fetch exactly the
# same way. On git < 2.29 they read the unknown-option failure as a per-ROW answer:
# `list`/`list-claims` printed every row `fetch-failed` and still exited 0 — a listing that
# measured NOTHING, reporting success — and the `ref_msg_field` readers behind
# `clear`/`reap`/`should-reap` silently lost the claim metadata their open-PR safeguards
# depend on. One version fact deserves one refusal in one spelling, checked at DISPATCH so
# a subcommand added later cannot forget to ask.
#
# Keyed on the AFFIRMATIVE value (`= yes`), never on `!= no`: an unparseable version sets
# neither, and an unmeasured state must never fall into the permissive branch.
require_private_fetch() {
  [ "$GIT_SUPPORTS_NO_WRITE_FETCH_HEAD" = yes ] && return 0
  note "'$1' needs a git that can fetch without writing FETCH_HEAD (--no-write-fetch-head, git 2.29+; found '$(git --version 2>/dev/null || echo unknown)'). Refusing to run rather than clobbering the shared FETCH_HEAD that other tooling in this checkout may read — upgrade git, or run from a checkout with a newer one. NOTHING was measured."
  return 1
}

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
# issue_number_ok <value> — true only for a positive decimal issue number.
#
# A SHARED PREDICATE, NOT A TIGHTENED SHARED GETTER (roborev round 21, Medium; #3464 family 4).
# `ref_msg_field` stays generic on purpose — `pid` and `ts` are read through it and neither is a
# positive decimal — so the validation lives at the two places an ISSUE is consumed, not in the getter
# every field shares. Tightening the getter would break the callers for which the value is legitimately
# not a number, which is family 4 exactly.
#
# WHY IT EXISTS: `issue_has_open_pr` answers "no open PR" for a NON-NUMERIC issue, because it cannot
# query one — a correct answer to the wrong question. A legacy claim whose message says `issue=abc`
# therefore passed the open-PR safeguard by failing to be checkable, and `should-reap`/`reap` deleted it.
# A value the safeguard cannot USE is not a reason to proceed; it is the same "could not tell" that
# every other branch here treats as KEEP.
# msg_token <message> <field> — the value of the WHOLE `<field>=<value>` token, matched on an EXACT key.
#
# A SUBSTRING KEY IS NOT A KEY (roborev round 26, Low by severity, headline-class by consequence). Both
# readers matched `.*${field}=` , which has no token boundary before the key — so `notissue=42` satisfied
# `issue`, `rapid=123` satisfied `pid`, and `nots=...` satisfied `ts`. A malformed or hand-made claim
# message could therefore SUPPLY a value that the fail-closed parsing was meant to refuse, and a wrong pid
# is PROBED: `dead-lanes` then answers about a different process, which is the same masking failure round
# 25 closed on the value side. Round 25 required the whole VALUE to be well-formed and left the KEY a
# substring match — the same class, one field over, which is the sixth instance of that shape here.
#
# Splitting on whitespace and anchoring `^<field>=` makes the key exact and the value whole in one step,
# with no regex alternation (portable across GNU and BSD sed).
msg_token() {
  local msg="$1" field="$2"
  printf '%s' "$msg" | tr ' \t\n' '\n\n\n' | sed -n "s/^${field}=\\(..*\\)$/\\1/p" | head -1
}

# msg_numeric_field <message> <field> — the field's COMPLETE token, echoed only when the WHOLE token is
# a decimal number; empty otherwise.
#
# A PREFIX MATCH IS NOT A PARSE (roborev round 25, Low by severity, headline-class by consequence). The
# row parsers extracted `\([0-9][0-9]*\)`, which matches the numeric PREFIX of a malformed token: `pid=123x`
# yielded `123` and `issue=42x` yielded `42`. A coerced pid then gets PROBED — so `dead-lanes` reports
# ALIVE or DEAD about a DIFFERENT PROCESS, and a false ALIVE masks exactly the dead lane this command
# exists to find. Silent coercion of a malformed value into a valid-looking one is worse than either
# rejecting it or reporting it unknown.
#
# Extract the whole space-delimited token first, THEN require the entire value to be decimal. Callers
# treat empty as UNKNOWN, which is what they already do for an absent field.
msg_numeric_field() {
  local msg="$1" field="$2" tok
  tok="$(msg_token "$msg" "$field")"
  case "$tok" in
    '' | *[!0-9]*) return 0 ;;   # absent, malformed, or not wholly numeric => UNKNOWN (empty)
  esac
  printf '%s\n' "$tok"
}

issue_number_ok() {
  case "${1:-}" in
    '' | *[!0-9]*) return 1 ;;
    0*) return 1 ;; # `0` and leading-zero forms are not issue numbers
  esac
  return 0
}

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
# ref_msg_field <ref> <field> — read `<field>=<value>` from a liveness ref's commit message.
#
# FAILS CLOSED, AND DOES NOT TOUCH FETCH_HEAD (roborev round 4, Medium). Two defects lived here:
#   * a failed fetch or unreadable message returned EMPTY, and `delete_ref_guarded` reads this to
#     find the issue for its open-PR safeguard — so a transient failure silently SKIPPED the
#     safeguard and deleted a claim whose endgame was unfinished. Absence of an answer was being
#     used as an answer.
#   * it read through shared `FETCH_HEAD`, which any concurrent fetch in this worktree can clobber,
#     so the field could come from ANOTHER ref entirely. Same hazard already fixed in `dead-lanes`,
#     not carried across to here.
# Non-zero exit now means "could not read"; callers must treat that as unknown, never as absent.
ref_msg_field() {
  local ref="$1" field="$2" tmpref msg
  tmpref="refs/tmp/claim-heartbeat-field/$$-${RANDOM}"
  if ! git fetch --no-write-fetch-head --no-tags "$REMOTE" "+${ref}:${tmpref}" >/dev/null 2>&1; then
    return 1
  fi
  msg="$(git log -1 --format=%B "$tmpref" 2>/dev/null)" || { git update-ref -d "$tmpref" 2>/dev/null || true; return 1; }
  git update-ref -d "$tmpref" 2>/dev/null || true
  [ -n "$msg" ] || return 1
  # AN ABSENT FIELD IS ALSO A FAILURE (roborev round 5, Medium). The previous form ended in a
  # pipeline, so a readable-but-MALFORMED message (no `<field>=…` token at all) produced an empty
  # value and exit 0 — and `delete_ref_guarded` reads this to find the issue for its open-PR
  # safeguard, so "no issue" and "message did not say" were indistinguishable and the safeguard was
  # skipped. The fix that made an unreadable FETCH failure fail closed left this second path open.
  local value
  # EXACT key, WHOLE token (roborev round 26) — see msg_token.
  value="$(msg_token "$msg" "$field")"
  [ -n "$value" ] || return 1
  printf '%s' "$value"
}

# lane_id_ok <id> — exit 0 iff <id> is a valid lane component (#3393).
#
# TWO EXACT SHAPES, and the exactness is the point (roborev round 4, Medium). The first cut was a
# loose `p[0-9a-f-]*`, which MEASURABLY accepted `0`, `00`, `pdead` and `p-`:
#   * `0` is the worst of them — it recreates the single shared `refs/lane-claims/<machine>/0` whose
#     collisions are the dead-lane masking this whole change exists to remove. A guard that admits
#     the original defect is not a guard.
#   * `pdead` passed because d, e, a and d are all hex digits, which is a good reminder that a
#     character-class test is not a grammar.
# So: a POSITIVE decimal issue, or exactly `p<decimal-pid>` optionally followed by `-<hex token>`.
lane_id_ok() {
  case "$1" in
    '' | 0 | 0*) return 1 ;;   # no empty, no zero, no leading zero (00 is not an issue)
    p*)
      local rest="${1#p}" pid="" tok=""
      case "$rest" in
        *-*) pid="${rest%%-*}"; tok="${rest#*-}" ;;
        *)   pid="$rest" ;;
      esac
      case "$pid" in '' | *[!0-9]*) return 1 ;; esac
      if [ -n "$tok" ]; then
        case "$tok" in *[!0-9a-f]*) return 1 ;; esac
      else
        case "$rest" in *-*) return 1 ;; esac   # a trailing dash with no token, e.g. `p-`
      fi
      return 0
      ;;
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

cmd_beat() {
  local issue="${1:-}"
  case "$issue" in
    *[!0-9]* | '') die_usage "beat requires a numeric issue number (got '${issue:-<none>}')" ;;
  esac

  local machine ts commit_sha
  machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Force-update: ONE ref per machine, so every beat replaces the previous liveness proof rather
  # than growing a history. NOT justified by #1930's "one worker per machine" (retracted by #3393) —
  # a heartbeat is per-MACHINE by nature and is unaffected by how many lanes the box runs.
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
  # THE SHA IS A FIELD ON STDOUT, NOT A NUMBER INSIDE A SENTENCE (roborev round 19, Medium).
  # `worker-supervisor.sh` needs the sha it just wrote so it can pass it back as a `reap` LEASE and
  # never delete a ref another supervisor has since refreshed. The only other ways to obtain it are
  # to parse the human-readable `note` above — which is #3464 family 5, deciding from a message
  # rather than a field, in the subsystem where that costs someone their work — or to re-read the ref
  # with `ls-remote`, which is slower and weaker, since the value is already known here at the moment
  # of writing. `stamp`'s stdout was empty and nothing parsed it, so this is additive: notes stay on
  # stderr, and stdout is exactly one line, the sha.
  printf '%s\n' "$commit_sha"
}

cmd_list() {
  local now_epoch raw unreadable_rows=0
  now_epoch="$(date -u +%s)"
  # NOT SWALLOWED (#3393 self-sweep). `flow-board` reads this to render the fleet view and decide
  # what is owned, so an origin or auth outage rendering "no heartbeats found" is the same fail-open
  # shape found four times elsewhere in this change. Found by grepping for the class rather than
  # waiting for a fifth review round.
  local ls_rc=0
  raw="$(git ls-remote "$REMOTE" 'refs/heartbeats/*' 2>/dev/null)" || ls_rc=$?
  if [ "$ls_rc" -ne 0 ]; then
    note "could not list heartbeat refs on ${REMOTE} (git exited ${ls_rc}) — this listing would be empty for a reason that is NOT 'no heartbeats exist'"
    return 1
  fi

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

    # A PRIVATE REF, NOT FETCH_HEAD (#3393 self-sweep). `FETCH_HEAD` is shared per-worktree, so any
    # concurrent fetch can clobber it between the fetch and the read and this row would describe
    # ANOTHER ref. Already fixed in `dead-lanes` and `ref_msg_field`; these two readers were missed.
    local tmpref_l
    tmpref_l="refs/tmp/claim-heartbeat-list/$$-${RANDOM}"
    if ! git fetch --no-write-fetch-head --no-tags "$REMOTE" "+${refname}:${tmpref_l}" >/dev/null 2>&1; then
      # A ROW THAT COULD NOT BE MEASURED REACHES THE EXIT STATUS (roborev round 20, Low — the
      # PROPERTY behind census instance 6). Guarding the git version at dispatch closed the CAUSE
      # that instance was found through (git < 2.29) and left the property open: a row fetch failing
      # for network, auth, or a ref deleted mid-run still printed `fetch-failed` and the command
      # still exited 0 — a listing that measured nothing, reporting success. Cause fixed, property
      # violated. The table still renders (an operator wants the rows that DID read), and the exit
      # status now tells a caller the listing is incomplete.
      unreadable_rows=$((unreadable_rows + 1))
      printf '%-20s %-8s %-24s %s\n' "$machine" "?" "?" "fetch-failed"
      continue
    fi
    msg="$(git log -1 --format=%B "$tmpref_l" 2>/dev/null || true)"
    git update-ref -d "$tmpref_l" 2>/dev/null || true
    issue="$(msg_numeric_field "$msg" issue)"
    ts="$(msg_token "$msg" ts)"
    [ -n "$issue" ] || issue="?"
    [ -n "$ts" ] || ts="?"

    if [ "$ts" != "?" ] && epoch="$(ts_to_epoch "$ts" 2>/dev/null)"; then
      age_h="$(humanize_age "$((now_epoch - epoch))")"
    else
      age_h="unknown"
    fi
    printf '%-20s %-8s %-24s %s\n' "$machine" "$issue" "$ts" "$age_h"
  done <<<"$raw"

  if [ "$unreadable_rows" -gt 0 ]; then
    note "${unreadable_rows} heartbeat ref(s) could not be read — this listing is INCOMPLETE; the rows above are the ones that DID read, and a missing machine here is not evidence that it has no heartbeat"
    return 1
  fi
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

  # THE PATH IS AUTHORITATIVE for a per-lane ref (roborev round 4): the issue is in the ref name, so
  # the open-PR guard no longer depends on parsing a commit message at all. Only the legacy shape
  # needs the message, and an unreadable message there is UNKNOWN, not absent — fail closed.
  local issue=""
  case "$namespace/$suffix" in
    lane-claims/*)
      issue="${suffix##*/}"
      lane_id_is_placeholder "$issue" && issue=""   # a placeholder names no issue to check
      ;;
    *)
      if ! issue="$(ref_msg_field "$ref" issue)"; then
        note "REFUSING to delete ${ref}: its claim message could not be read, so an open PR cannot be ruled out (the open-PR safeguard needs the issue)"
        return 5
      fi
      # THE SIBLING SITE, fixed in the same change (roborev round 21 named only `should-reap`). Both
      # read a legacy issue and both feed the same safeguard, so fixing one and leaving the other is
      # the guard-width mistake this branch has already made three times.
      if ! issue_number_ok "$issue"; then
        note "REFUSING to delete ${ref}: its claim message names issue='${issue}', which is not an issue number, so the open-PR safeguard cannot query it"
        return 5
      fi
      ;;
  esac
  if [ -n "$issue" ] && issue_has_open_pr "$issue"; then
    note "REFUSING to delete ${ref}: issue #${issue} has an open PR (endgame unfinished; #2655)"
    return 3
  fi

  if [ -n "$expected_sha" ]; then
    if git push "$REMOTE" --force-with-lease="${ref}:${expected_sha}" --delete "$ref"; then
      note "cleared ${ref} on $REMOTE (lease held at ${expected_sha})"
      return 0
    fi
    # ONLY A GENUINE LEASE MISMATCH IS 4 (roborev round 20, Medium — FAIL-OPEN in a new spelling). A
    # failed push collapsed onto "the lease was not held", so an auth failure, a network blip or a
    # server error all reported OWNERSHIP TRANSFERRED. `worker-supervisor.sh` reads 4 as exactly that
    # and PERMANENTLY DROPS the cleanup entry — and automated reaping deliberately refuses a
    # placeholder lane, so a transient failure leaked a stale ref forever and `dead-lanes` then
    # reported a dead lane that does not exist. The lease fix of the previous round created that leak.
    #
    # So the claim is VERIFIED rather than inferred from the push's failure: re-read the ref and return
    # 4 only when its sha genuinely differs from the lease. Unchanged sha, confirmed-absent, or an
    # unreadable listing are all OPERATIONAL failures (1) and the caller keeps the entry queued.
    local ls_out ls_rc=0 now_sha=""
    ls_out="$(git ls-remote --exit-code "$REMOTE" "$ref" 2>/dev/null)" || ls_rc=$?
    case "$ls_rc" in
      0) now_sha="$(printf '%s' "$ls_out" | awk 'NR==1{print $1}')" ;;
      2)
        # The ref is gone. Somebody else deleted it, so there is nothing left to clean up and nothing
        # was destroyed by us: report success rather than inventing a transfer or an outage.
        note "nothing to delete: ${ref} is already absent on $REMOTE (the leased push failed, and the ref has since gone)"
        return 0
        ;;
      *)
        note "REFUSING to delete ${ref}: the leased push failed AND the ref could not be re-read (git ls-remote exited ${ls_rc}) — whether the lease still holds is UNKNOWN, so this is an operational failure, not a transfer"
        return 1
        ;;
    esac
    if [ -n "$now_sha" ] && [ "$now_sha" != "$expected_sha" ]; then
      note "REFUSING to delete ${ref}: the lease at ${expected_sha} was not held — the ref is now at ${now_sha}, so a live supervisor refreshed it between the verdict and this delete"
      return 4
    fi
    note "REFUSING to delete ${ref}: the leased push failed while the ref is STILL at ${expected_sha:-<none>} — the lease holds, so this is an operational failure (auth, network or server), not a transfer"
    return 1
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
  local now_epoch raw legacy unreadable_rows=0
  now_epoch="$(date -u +%s)"
  # BOTH namespaces (#3393). New claims live at refs/lane-claims/<machine>/<issue>; a legacy
  # refs/machine-claims/<machine> from before the per-lane ruling is still listed so it can be seen
  # and drained — an invisible legacy ref pins its board item at In Progress indefinitely.
  # NEITHER LISTING MAY FAIL SILENTLY (roborev round 4, Low; fourth instance of this class in this
  # change). Both were `|| true`, so an origin or auth outage rendered "no claims found" or a
  # partial table — and this command is what an operator reads to decide whether a lane is owned.
  local lane_rc=0 legacy_rc=0
  raw="$(git ls-remote "$REMOTE" 'refs/lane-claims/*' 2>/dev/null)" || lane_rc=$?
  legacy="$(git ls-remote "$REMOTE" 'refs/machine-claims/*' 2>/dev/null)" || legacy_rc=$?
  if [ "$lane_rc" -ne 0 ] || [ "$legacy_rc" -ne 0 ]; then
    note "could not list claim refs on ${REMOTE} (lane-claims rc=${lane_rc}, machine-claims rc=${legacy_rc}) — this listing would be incomplete or empty for a reason that is NOT 'no claims exist'"
    return 1
  fi
  [ -z "$legacy" ] || raw="$(printf '%s\n%s' "$raw" "$legacy")"
  raw="$(printf '%s' "$raw" | sed '/^$/d')"

  if [ -z "$raw" ]; then
    echo "no claims found on $REMOTE"
    return 0
  fi

  printf '%-20s %-8s %-10s %-24s %s\n' "MACHINE" "ISSUE" "PID" "TS" "AGE"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local refname machine msg issue pid ts epoch age_h lane_from_path=""
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    case "$refname" in
      refs/lane-claims/*)
        # <machine>/<lane-id> — the id is the LAST component, which is why the separator is a
        # slash and not a dash (machine names contain dashes). Shown from the PATH so a placeholder
        # lane is identifiable rather than rendered `?` (roborev round 4).
        machine="${refname#refs/lane-claims/}"
        lane_from_path="${machine##*/}"
        machine="${machine%/*}"
        ;;
      *) machine="${refname#refs/machine-claims/} (legacy)" ;;
    esac

    # A PRIVATE REF, NOT FETCH_HEAD — same reason as in `cmd_list` above (#3393 self-sweep).
    local tmpref_c
    tmpref_c="refs/tmp/claim-heartbeat-listclaims/$$-${RANDOM}"
    if ! git fetch --no-write-fetch-head --no-tags "$REMOTE" "+${refname}:${tmpref_c}" >/dev/null 2>&1; then
      # Same property as `cmd_list` above (roborev round 20, Low): an unmeasured row must reach the
      # exit status, or an outage renders a table of `fetch-failed` rows and still exits 0.
      unreadable_rows=$((unreadable_rows + 1))
      printf '%-20s %-8s %-10s %-24s %s\n' "$machine" "?" "?" "?" "fetch-failed"
      continue
    fi
    msg="$(git log -1 --format=%B "$tmpref_c" 2>/dev/null || true)"
    git update-ref -d "$tmpref_c" 2>/dev/null || true
    issue="$(msg_numeric_field "$msg" issue)"
    pid="$(msg_numeric_field "$msg" pid)"
    ts="$(msg_token "$msg" ts)"
    [ -n "$lane_from_path" ] && issue="$lane_from_path"
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

  if [ "$unreadable_rows" -gt 0 ]; then
    note "${unreadable_rows} claim ref(s) could not be read — this listing is INCOMPLETE; a lane missing from the table above is NOT evidence that it is unowned, which is what an operator would otherwise conclude"
    return 1
  fi
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
  local ref
  if [ -n "$issue" ]; then
    ref="$(lane_claim_ref "$machine" "$issue")"
  else
    ref="refs/machine-claims/${machine}"
  fi

  # ONLY EXIT 2 IS ABSENCE (roborev round 18, Medium — FAIL-OPEN instance 7). `! git ls-remote`
  # collapsed every failure onto "no claim ref … return 2", so an auth failure, a DNS blip or a
  # remote outage reported CONFIRMED ABSENCE for a ref nobody could look at — and a caller acting on
  # 2 concludes there is nothing to own. `--exit-code` distinguishes them: 2 is git's confirmed
  # no-match, anything else is operational. `delete_ref_guarded` already cased this correctly and
  # `should-reap` was simply never brought up to it, which is the same guard-width shape as round 17.
  local ls_rc=0
  git ls-remote --exit-code "$REMOTE" "$ref" >/dev/null 2>&1 || ls_rc=$?
  case "$ls_rc" in
    0) : ;;
    2)
      note "no claim ref ${ref} on $REMOTE"
      return 2
      ;;
    *)
      # KEEP, not "no ref": this is a reap GATE, so the answer to "I could not tell" is the
      # conservative one, and the cause is named rather than swallowed.
      note "keep ${ref}: could not determine whether it exists (git ls-remote exited ${ls_rc} — operational failure, NOT git's no-match status 2). An unverified claim is never confirmed absent."
      return 1
      ;;
  esac
  # A PLACEHOLDER LANE IS NEVER AUTOMATICALLY REAPED (roborev round 3, Medium). A `p…` id names no
  # issue, so the open-PR guard has nothing to consult — and a worker can have claimed an issue and
  # opened a PR before its supervisor ever received the marker, or left an auto-merge PR open after
  # `CLAIM_ISSUE` was cleared. Reaping it would delete the claim of a lane with an unfinished
  # endgame, which is the #2499 case the guard exists for. The owning supervisor still clears its own
  # placeholder on clean exit by calling `reap` directly — it knows it is finished; a reaper cannot.
  #
  # ORDERED AFTER THE EXISTENCE CHECK (roborev round 33, Medium). This rule used to run FIRST, so
  # querying a placeholder that does not exist answered 1 ("keep") instead of the documented 2 ("no
  # ref") — a caller distinguishing "owned" from "absent" got the wrong one of the two, and 1 is the
  # answer that says something is there. The keep-rule is about a ref that EXISTS and cannot be
  # checked against an open PR; it has nothing to say about a ref that is not there.
  if [ -n "$issue" ] && lane_id_is_placeholder "$issue"; then
    note "keep refs/lane-claims/${machine}/${issue}: placeholder lane id names no issue, so an open PR cannot be ruled out — never automatically reaped (#3393)"
    return 1
  fi

  # `pid` is OPTIONAL, and after round 5 that needed saying in code (roborev round 6, Low).
  # `ref_msg_field` now fails when a field is ABSENT — correct for the open-PR safeguard — but under
  # `set -e` an absent `pid` then terminated should-reap outright, so a legacy or foreign claim whose
  # PID is deliberately not required could no longer reach the documented age + open-PR decision. A
  # fail-closed change in one caller became a fail-SHUT regression in another.
  # THE REF PATH OUTRANKS THE MESSAGE, AND AN UNREADABLE LEGACY ISSUE IS KEEP (roborev round 18,
  # Medium — FAIL-OPEN instance 8). Two defects in three lines. `local issue` RESET the caller's
  # lane id, and the message parse then overwrote the one AUTHORITATIVE source of the issue with a
  # best-effort one; `|| issue=""` turned an unreadable message into "no issue", and the open-PR
  # check below is `[ -n "$issue" ] && …`, so an absent issue SKIPPED the #2499 orphaned-endgame
  # safeguard and returned REAP. `delete_ref_guarded`, `list-claims` and `dead-lanes` all prefer the
  # path already — `should-reap` was the one subcommand never brought up to the pattern.
  #
  # Only `pid` is legitimately optional (a foreign machine's PID is unknowable, which is why the
  # predicate is age AND no-open-PR AND pid-dead-IF-LOCAL). `issue` is not: for a per-lane claim it
  # is IN the ref path, and for a legacy claim the message is the only source, so failing to read it
  # means the safeguard cannot run and the answer is KEEP. That is fail-CLOSED without being
  # fail-SHUT (#3464 family 4): the caller still reaches a documented decision, it just reaches the
  # conservative one.
  local lane_issue="$issue" pid ts epoch now_epoch age
  if [ -n "$lane_issue" ]; then
    issue="$lane_issue"
  elif ! issue="$(ref_msg_field "$ref" issue)"; then
    note "keep ${ref}: its claim message could not be read, so its issue is unknown and an open PR cannot be ruled out (#2499). Only pid is optional here."
    return 1
  elif ! issue_number_ok "$issue"; then
    note "keep ${ref}: its claim message names issue='${issue}', which is not an issue number, so the open-PR safeguard cannot query it (#2499). Unusable is the same as unknown here — never a licence to reap."
    return 1
  fi
  pid="$(ref_msg_field "$ref" pid)" || pid=""
  ts="$(ref_msg_field "$ref" ts)" || ts=""

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
  # THE THREE-VALUED PROBE, AND A LOCAL CLAIM WITH NO USABLE PID IS NOT REAPABLE (roborev round 28,
  # Medium). Two defects in one block, both reaping a LIVE lane:
  #
  #   * `kill -0` is TWO-valued here, so every failure read as "dead". But EPERM means the process
  #     EXISTS and simply is not ours — `process_presence` decodes that, and `dead-lanes` has used it
  #     since round 10. `should-reap` is the caller that never got it: guard-width again, with the
  #     harm pointing at the worst possible outcome.
  #   * when `pid` was empty — absent, or malformed and now correctly rejected by the round-25/26
  #     parsing — the `[ -n "$pid" ]` guard was FALSE, so a LOCAL claim fell through to the
  #     foreign-machine branch and was reaped with NO pid check at all, under a message that said
  #     "pid not checkable". The doctrine's predicate is age AND no-open-PR AND pid-dead-IF-LOCAL;
  #     an unsatisfiable third clause must fail the conjunction, not vanish from it.
  local this_machine
  this_machine="${HEARTBEAT_MACHINE:-$(hostname -s)}"
  if [ "$machine" = "$this_machine" ]; then
    if [ -z "$pid" ]; then
      note "keep ${ref}: LOCAL claim with no usable pid, so 'pid-dead-if-local' cannot be established — an unsatisfiable clause fails the predicate rather than dropping out of it"
      return 1
    fi
    # PRESENT IS NOT THE SAME AS ALIVE (roborev round 36, Low). `process_presence` answers a
    # VISIBILITY question, and a ZOMBIE is visible: `ps -p` lists it, `/proc/<pid>` exists, and
    # `kill -0` succeeds. So a zombie supervisor read as `present` and its claim was KEPT
    # INDEFINITELY — while `dead-lanes`, in this same file, has classified exactly that case as
    # DEAD-NO-PROCESS since round 7. The two predicates disagreed about one fact, which is the
    # guard-width shape this command has now met four times: `dead-lanes` gained a check and
    # `should-reap` was never brought up to it.
    #
    # Reusing `process_state_class` rather than reimplementing it is the point — a second
    # implementation of "is this a zombie" is a second thing to keep in step.
    #
    # `unreadable` stays KEEP, deliberately: it is neither a confirmed zombie nor a confirmed live
    # process, and this is a reap GATE, so the unknown takes the conservative branch. PID REUSE is
    # NOT decided here — `dead-lanes` owns that verdict because it needs the claim's own timestamp
    # to bracket a start window, and inferring it inside a reaper that has already passed the age
    # gate would be a second, weaker copy of that reasoning.
    case "$(process_presence "$pid")" in
      absent)
        note "reap ${ref}: age ${age}s > ${threshold}s, no open PR, local pid ${pid} CONFIRMED absent"
        return 0
        ;;
      present)
        case "$(process_state_class "$pid")" in
          zombie)
            note "reap ${ref}: age ${age}s > ${threshold}s, no open PR, local pid ${pid} is a ZOMBIE — visible to ps and /proc but not running (dead-lanes classifies this DEAD-NO-PROCESS)"
            return 0
            ;;
          unreadable)
            note "keep ${ref}: local pid ${pid} is present but its state is UNREADABLE — neither a confirmed zombie nor a confirmed live process, so the reap gate takes the conservative branch"
            return 1
            ;;
          *)
            note "keep ${ref}: local pid ${pid} is still alive"
            return 1
            ;;
        esac
        ;;
      *)
        note "keep ${ref}: local pid ${pid} presence is UNKNOWN (denied or unmeasurable) — not proof it is gone"
        return 1
        ;;
    esac
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
# SUPERVISOR FLEETS ONLY — DESCOPED (#3548, owner ruling 2026-09-01; completes #3393). The
# subject set below is `refs/lane-claims/*` + the legacy `refs/machine-claims/*`, written by
# `worker-supervisor.sh` alone; returning 1 means "NOTHING WAS REPORTED", never a clean bill of
# health (#3467). The populated `refs/claims/issue-<N>` and `refs/heartbeats/<machine>` are NOT
# read here — do not point this at them; the measured reasons and AC4's counterfactual are in
# the header's SCOPE section.
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

  # The git-version refusal that used to live here now runs at DISPATCH, so it covers every
  # fetching subcommand instead of this one alone (roborev round 17, Medium). Keeping a
  # second copy here would be two spellings of one fact, which is how they drift apart —
  # and the drift had already started: the comment below still described a fallback that
  # omits the flag on old git, a design replaced by refusal two rounds earlier.

  # NO EARLY `return 0` HERE (roborev round 5, Medium). An empty namespace proves only
  # that no ref exists — it does NOT establish an idle fleet. A lane running with
  # `CLAIM_CMD=""` (stamping deliberately disabled, a documented supervisor option) or one
  # whose stamps have been failing all along looks EXACTLY the same from here, so exiting
  # 0 would report clean about lanes that were never measured. It is also the same
  # condition as the all-foreign case: nothing local was inspected. Both now fall through
  # to the single `local_seen` check at the end, so one rule covers both instead of an
  # early return contradicting it.
  if [ -z "$raw" ]; then
    # AN EMPTINESS CLAIM NEEDS BOTH LISTINGS TO HAVE ANSWERED (roborev round 17, Low). With
    # the per-lane namespace empty and the LEGACY listing FAILED, `raw` is empty for two
    # different reasons at once, and the message asserted the wrong one: "no claim refs
    # exist" is a statement about a namespace nobody read. The exit code was already 1 and
    # the incompleteness was already noted above, so only the sentence was wrong — but a
    # false sentence in a monitor's output is what an operator acts on.
    if [ "$legacy_rc" -ne 0 ]; then
      note "the per-lane namespace is empty AND the LEGACY listing FAILED (git exited ${legacy_rc}) on ${REMOTE} — so whether any legacy claim exists was never determined. This measurement is INCOMPLETE; it is NOT 'no claim refs exist' and NOT 'no dead lanes'."
      return 1
    fi
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
    # still fetch-then-read a private ref of their own. Making a monitor into the cause of a
    # bad REAP decision would be a poor trade. Unconditional here, and correct because the
    # DISPATCH guard already refused on a git that lacks the option — so this line is only
    # ever reached on a capable git. It is NOT a fallback: an earlier version of this comment
    # claimed the flag is "omitted on older git", which was true of a design replaced two
    # rounds before it and would have failed every row with an unknown-option error reported
    # as "fetch failed" (roborev round 17, Medium).
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
    issue="$(msg_numeric_field "$msg" issue)"
    pid="$(msg_numeric_field "$msg" pid)"
    # The claim's own stamp time — the second half of the pid-identity check below.
    ts="$(msg_token "$msg" ts)"
    # `worker-supervisor.sh` stamps issue "0" when the current iteration's issue is
    # not yet known. That is a PLACEHOLDER, not issue #0: probing for a PR on it
    # would query a number that cannot exist and print a bogus issue in the report.
    # PREFER THE PATH COMPONENT (roborev round 4). The message parse is numeric-only, so a valid
    # PLACEHOLDER lane rendered as `?` — and since placeholders are deliberately never auto-reaped,
    # an operator could see that a lane needed manual cleanup but not WHICH ref to clean. The id is
    # right there in the ref name.
    if [ -n "$lane_issue" ]; then
      issue="$lane_issue"
    elif [ -z "$issue" ] || [ "$issue" = "0" ]; then
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
  # NO CLEAN VERDICT IN THIS SLICE — POSITIVE DETECTION ONLY (#3393 split ruling, 2026-08-29).
  #
  # Per-lane refs DO make a clean verdict sound: a surviving sibling now stamps a DIFFERENT ref, so a
  # dead lane's ref survives with its dead pid and the masking that made exit 0 a lie is gone. That
  # restoration was implemented, reviewed over four rounds, and is deliberately NOT shipped here.
  #
  # Why: the four review rounds on this change produced 5, 3, 4 and 4 findings, every one real, and
  # the FAIL-OPEN family — a failed probe read as a negative answer — clustered in this exit-0 path,
  # five instances in total. The split follows that defect grain. Positive detection is the part that
  # is sound and independently valuable, so it ships; the clean verdict lands separately with the
  # review it needs, because it is the value a cron reads and being wrong there is silent.
  #
  # So this command returns 3 (a dead lane was reported) or 1 (no dead lane was reported), and never
  # 0. Act on 3; never read 1 as a clean bill of health. Restoring exit 0 is tracked in the split
  # issue, which carries the family census and the four-round history rather than restarting from
  # zero.
  note "no dead lane was reported among the $(printf '%s' "$local_seen") local lane(s) measured. This slice is POSITIVE-DETECTION ONLY and never exits 0: a clean verdict needs the exit-0 restoration tracked separately (#3393 split). Exit 1 = nothing found, NOT a clean bill of health."
  return 1
}

SUBCOMMAND="${1:-}"
case "$SUBCOMMAND" in
  beat)
    shift
    cmd_beat "${1:-}"
    ;;
  list)
    require_private_fetch list || exit 1
    cmd_list
    ;;
  clear)
    shift
    require_private_fetch clear || exit 1
    cmd_clear "${1:-}"
    ;;
  stamp)
    shift
    cmd_stamp "${1:-}" "${2:-}"
    ;;
  list-claims)
    require_private_fetch list-claims || exit 1
    cmd_list_claims
    ;;
  should-reap)
    shift
    # Exit 1 here is this subcommand's KEEP, which is the safe answer: a reaper that
    # cannot read the claim must not reap it. The refusal names git as the cause, so
    # "keep because I could not tell" is never silent.
    require_private_fetch should-reap || exit 1
    cmd_should_reap "${1:-}" "${2:-}" "${3:-}"
    ;;
  reap)
    shift
    require_private_fetch reap || exit 1
    cmd_reap "${1:-}" "${2:-}" "${3:-}"
    ;;
  dead-lanes)
    shift
    require_private_fetch dead-lanes || exit 1
    cmd_dead_lanes "$@"
    ;;
  -h | --help)
    print_help
    ;;
  "")
    # BOTH FORMS SPELLED OUT (roborev round 21, Medium). This line advertised
    # `should-reap <machine> [issue] [secs]`, but a TWO-argument call is ALWAYS the legacy
    # `<machine> <threshold_secs>` form — deliberately, so the grammar is unambiguous (pinned by
    # TEST 57) — so an operator following this text ran `should-reap <box> <issue>` and got a verdict
    # about the LEGACY ref with the issue number read as a threshold: a real answer to a different
    # question. The --help block already documented both forms; this one-line summary did not.
    die_usage "a subcommand is required: beat <issue> | list | clear <machine> | stamp <lane-id> [pid] | list-claims | should-reap <machine> [threshold_secs] | should-reap <machine> <issue> <threshold_secs> | reap <machine> [lane-id] [expected_sha] | dead-lanes"
    ;;
  *)
    die_usage "unknown subcommand: $SUBCOMMAND (expected beat|list|clear|stamp|list-claims|should-reap|reap|dead-lanes)"
    ;;
esac
