#!/usr/bin/env bash
#
# lane-lock.sh — machine-local lane-DIRECTORY lock keyed on full PROCESS identity
# (issue #3436, child of epic #2664).
#
# WHY THIS EXISTS
# ---------------
# On 2026-08-28 TWO Claude sessions worked ONE issue in ONE worktree on ONE
# machine for ~20 minutes. Session A claimed #3367 (`refs/claims/issue-3367`) and
# created `/data/lanes/lane-3367`; session B's shell started seven minutes later,
# held NO claim, and committed into A's branch and worktree. A's `git add -A` then
# swept up B's uncommitted work, so a commit landed presenting B's design as A's.
# The ONLY thing that noticed was `agent-gate.sh --lite`'s `tree-integrity`
# (#2926), and it noticed by accident.
#
# WHY `refs/claims/issue-<N>` COULD NOT STOP IT — the whole reason for this file:
#   1. It is a HARD control CROSS-machine (git arbitrates the push server-side) but
#      purely ADVISORY LOCALLY: a session that never runs `claim.sh` simply
#      proceeds into the lane directory, and nothing stops it.
#   2. Even a session that DOES consult it is waved through, because claim.sh's
#      holder identity — and therefore its re-entrancy — is `machine+actor`. Two
#      Claude sessions on one box are BOTH `machine=ip-172-31-7-163 actor=flow`,
#      so the second session's `claim` is indistinguishable from the first
#      session's retry. `machine+actor` is exactly the granularity that CANNOT
#      express "a DIFFERENT process on the SAME box".
# So this lock's identity is the full PROCESS identity, and re-entrancy requires
# ALL FIVE components to match:
#
#   token = <machine>:<actor>:<pid>:<boot-id-short>:<start-ticks>
#
# A same-machine, same-actor, DIFFERENT-live-pid acquire is OCCUPIED. That single
# property is the #3436 fix and MUST NEVER be relaxed back to machine+actor.
#
# IDENTITY DESIGN — boot id + start ticks, and its provenance
# -----------------------------------------------------------
#   boot-id      /proc/sys/kernel/random/boot_id (full value in the record, first 8
#                hex chars in the token). A reboot changes it, which is a definitive
#                statement that every process recorded against the old value is gone.
#   start-ticks  field 22 (`starttime`) of /proc/<pid>/stat — clock ticks SINCE BOOT,
#                so it is immune to wall-clock steps. PARSED ROBUSTLY: field 2
#                (`comm`) is parenthesized and may contain BOTH spaces and
#                parentheses (measured on the fleet: `tmux: server`), so the parse
#                splits on the LAST ')' and indexes from there. `awk '{print $22}'`
#                over the raw line is WRONG and is never used here. Splitting on the
#                LAST ')' is also strictly safer than on the first `") "`, which a
#                comm containing `") "` would break.
#
#   MEASURED ON THIS HOST (ip-172-31-7-163, 2026-08-31), so the design rests on
#   observation rather than on what /proc is supposed to do:
#     * /proc/sys/kernel/random/boot_id reads as bb94898a-7792-44e1-8f38-cdafea462803
#       WITHOUT root — so the identity is available to an ordinary worker.
#     * `flock` is util-linux 2.39.3 at /usr/bin/flock.
#     * the last-')' split agrees with `awk '{print $22}'` on two live pids, and TWO
#       independent reads of pid 3056664 both returned starttime=28412855 — i.e. the
#       value is stable for a live process, which is the whole premise of using it as
#       an identity. After the split, positional 20 is `starttime` and positional 2 is
#       `ppid`, because the remainder begins at field 3 (`state`).
# `scripts/flow/claim-heartbeat.sh` (see its header, "CLOCK-STEP SENSITIVITY")
# documents its own identity check as clock-step sensitive because it reconstructs a
# start epoch as `now - elapsed` and compares it against a wall-clock `ts`: an NTP
# step between stamp and inspection yields a false ALIVE (backward step) or a false
# DEAD-PID-REUSED (forward step). That header names the proper fix — "recording a
# stable process identity AT STAMP TIME — boot id plus start ticks" — and defers it to
# an owner decision. THIS FILE IS THAT DESIGN, for the lane-lock namespace. It does
# not change claim-heartbeat.sh; the two locks are separate namespaces (see SCOPE
# LIMITS).
#
# WHICH PID IS RECORDED — layered, three-valued, never guessed
# ------------------------------------------------------------
#   1. `--pid <pid>`        -> pid-scope=explicit
#   2. `LANE_LOCK_PID` env  -> pid-scope=explicit
#   3. auto-resolve         -> walk the ancestor chain from $$ upward via field 4
#      (ppid) of /proc/<pid>/stat, stopping at pid 1, and take the OUTERMOST
#      ancestor whose /proc/<pid>/cwd resolves to the lane directory or a path
#      inside it. That is the long-lived session process. pid-scope=session.
#
#      MEASURED on the real fleet box (ip-172-31-7-163, 2026-08-31), inside
#      /data/lanes/lane-3436:
#        bash  (cwd=/data/lanes/lane-3436)  <- $$
#        claude(cwd=/data/lanes/lane-3436)  <- SELECTED: the session process
#        tmux: server (cwd=/data/lanes/repo, ppid=1)  <- correctly EXCLUDED
#      Note the tmux server's cwd is the ROOT checkout (/data/lanes/repo), not a lane —
#      which is exactly WHY the "cwd is the lane dir or inside it" rule excludes it.
#      Excluding it is load-bearing, not incidental: ONE tmux server is SHARED by every
#      lane on the box — FOUR concurrent lanes at the time of measurement
#      (lane-3414, lane-3436, lane-3453, lane-3559) — so recording it would make all
#      four locks name the SAME pid, every lane would look mutually alive forever, and
#      no dead lane would ever be reclaimable. That is catastrophic rather than merely
#      wrong: the reclaim path is the only thing standing between a killed session and
#      a permanently stuck lane.
#      (That process's `comm` is also literally `tmux: server` — a space-bearing
#      comm, which is why the /proc/<pid>/stat parse above splits on the last ')'.)
#
#      If the ONLY cwd match is $$ itself -> pid-scope=ephemeral. That is NOT
#      refused: it is recorded, and such a holder's liveness is UNKNOWN-EPHEMERAL,
#      which REFUSES and never auto-reclaims. RATIONALE, and the direction matters:
#      an ephemeral pid is a per-tool-call shell that dies between tool calls, so
#      calling it DEAD would auto-reclaim a lane whose session is very much alive —
#      handing a second writer the exact collision this file exists to prevent. Fail
#      closed toward REFUSING; an operator can always `release --force`.
#      The chain is never followed to pid 1, and a non-cwd-matching ancestor is never
#      recorded as a fallback: an unrelated long-lived ancestor would be a
#      permanently-alive holder.
#
# HOW A STALE LOCK GETS CLEARED, AND BY WHOM — answered BEFORE building it
# ------------------------------------------------------------------------
# Owner ruling on this issue: "A stale lock with no way to distinguish live from stale
# becomes a permanent blocker", and "a guard that never permits work is broken, not
# fail-closed." So, exhaustively:
#   * DEAD-* (boot-id differs, /proc/<pid> absent, pid reused, zombie) — AUTO-RECLAIMED
#     by the next `acquire`. No human, no flag, no command. The reclaim is recorded in
#     the audit log with the previous token and the previous verdict.
#   * A REBOOT CLEARS EVERYTHING. The boot id changes, so every pre-reboot record reads
#     DEAD-REBOOT and is auto-reclaimed on next use: a box restart is a global un-brick.
#   * UNKNOWN-* (an identity that cannot be established at all — a foreign machine, a
#     hand-made or format-drifted record, a host without /proc) — cleared DELIBERATELY,
#     by a human or a reaper, with either
#         lane-lock.sh reclaim <N> --expect <token> --reason <why>   (CAS, recorded)
#         lane-lock.sh release <N> --force                            (unconditional)
#     `release --force` only DELETES, so it needs no identity of its own and works from
#     anywhere, which is what guarantees a stale record is always clearable.
#   * AND NOTHING CREATES AN UN-RE-IDENTIFIABLE RECORD ANY MORE (see
#     require_durable_identity): a write that cannot name a durable owner REFUSES with
#     `reason=unresolved-identity` and writes nothing, which removes the main source of
#     a permanent UNKNOWN-* rather than relying on the clearing paths above.
# Every refusal a stale record can cause names its remedy inline, in one line.
#
# LIVENESS — a CLOSED verdict set; only DEAD-* permits auto-reclaim
# -----------------------------------------------------------------
#   SELF                    the record's token equals ours exactly (re-entrancy)
#   ALIVE                   same boot-id, /proc/<pid> exists, its start-ticks EQUAL
#                           the record's, and its state is not 'Z' (zombie)
#   DEAD-NO-PROCESS         same boot-id, /proc/<pid> absent — or present and a zombie
#   DEAD-PID-REUSED         same boot-id, /proc/<pid> exists, start-ticks DIFFER
#   DEAD-REBOOT             record's boot-id differs from the live one; a reboot
#                           killed every process, so the holder is definitively gone
#   UNKNOWN-FOREIGN         record's machine differs from ours: a pid is not checkable
#   UNKNOWN-EPHEMERAL       record's pid-scope=ephemeral (see above)
#   UNKNOWN-NO-PID          record carries no usable pid
#   UNKNOWN-NO-BOOT-ID      record carries no boot-id (predates the field / hand-made)
#   UNKNOWN-NO-START-TICKS  record carries no start-ticks
#   UNKNOWN-NO-PROC         no /proc on this host: identity cannot be re-checked at all
#   UNKNOWN-UNREADABLE      the record exists but could not be parsed
#   UNKNOWN-STATE           /proc/<pid> exists and start-ticks match, but stat's state
#                           field could not be read, so running-vs-zombie was never
#                           established
#
# ONLY a DEAD-* verdict permits auto-reclaim; EVERY UNKNOWN-* REFUSES. This is
# CLAUDE.md's affirmative-measurement rule: never derive a permissive branch from the
# ABSENCE of a bad signal — key the permissive branch on the AFFIRMATIVE value. An
# unrecognised state is UNKNOWN-*, never ALIVE and never DEAD, so a verdict this file
# cannot establish costs a refusal (recoverable, visible, names the occupant) rather
# than a silent second writer (invisible, and #3436's actual damage).
#
# ATOMICITY
# ---------
# `flock` on `<lock-root>/lane-<N>.flock` is held for the WHOLE read-modify-write of
# every MUTATING subcommand — including acquire's decide-then-write, which is the
# race that matters (two sessions both reading "free" and both writing). The mutex is
# a SEPARATE file from the record so the record can be replaced by rename without
# disturbing the mutex, and so a `flock` open never creates or mutates the record.
# The record itself is written via `mktemp` in the same directory + `mv` (atomic
# rename), so a reader ALWAYS sees a complete record. That is why the read-only
# subcommands (`probe`, `verify`, `status`) take NO lock — `probe` in particular MUST
# NOT, because opening the mutex for flock would CREATE it, and probe must write
# nothing.
# If `flock` is not on PATH the mutating subcommands emit
# `ERROR reason=unsupported detail=flock-unavailable` and exit 1. That is deliberate:
# the fleet is Linux (this whole file is /proc-specific), and an invented fallback
# mutex — a mkdir spinlock, an O_EXCL sentinel — is a second, weaker arbiter that
# would be exercised only on the hosts nobody tests, which is how a lock silently
# stops locking. Fail closed instead.
#
# FILES — IN A SIBLING LOCK ROOT, *NOT* IN THE LANE DIRECTORY
# -----------------------------------------------------------
#   `$LANE_ROOT/.lane-locks/`, keyed by issue:
#
#   lane-<N>.lock     the record: `key=value`, ONE PER LINE. There are NO in-band
#                     delimiters and no multi-field lines — CLAUDE.md #3312: control
#                     and data must not share a channel. Keys: version, issue,
#                     lane-dir, machine, actor, pid, pid-scope, boot-id, start-ticks,
#                     acquired-ts, acquired-epoch, nonce; a reclaim additionally
#                     records reclaimed-from, reclaimed-prev-liveness, reclaimed-ts,
#                     reclaim-reason.
#                     Every value is SANITIZED to one whitespace-free token before it
#                     is written OR compared (the same discipline as claim.sh's
#                     `sanitize_field`, restated here rather than sourced — see
#                     CONSTRAINTS). Without it an `--actor 'flow
#                     pid=1'`-shaped value could forge a holder field and win FALSE
#                     re-entrancy on someone else's lane, which is the failure this
#                     lock exists to make impossible.
#                     Reading parses ONLY `^[a-z-]+=` lines and ignores unknown keys
#                     (forward compatibility); a DUPLICATE key is UNKNOWN-UNREADABLE
#                     — fail closed, because two values for one key means the record
#                     cannot be said to state anything.
#   lane-<N>.flock    the mutex (see ATOMICITY)
#   lane-<N>.log      append-only audit: one line per acquire / reclaim / release
#                     with ts, verdict, token, prev-token, prev-liveness, reason.
#                     NEVER truncated. This is #3436 AC3's "the reclaim is recorded".
#
# WHY THE LOCK IS NOT IN THE LANE DIRECTORY — MEASURED, and it makes AC1 true
# ---------------------------------------------------------------------------
# AC1 is "detects an existing occupant BEFORE writing", i.e. `acquire` must be
# takeable BEFORE `git worktree add` creates the lane. An earlier version of this file
# put the record at `<lane-dir>/.lane-lock` and `mkdir -p`'d the lane directory, which
# makes that order IMPOSSIBLE — `git worktree add` refuses a target that exists AT ALL,
# dotfiles included, and it creates the branch BEFORE failing, leaving a stray branch
# behind. Measured on this host (2026-08-31):
#
#   $ mkdir -p lane-777 && touch lane-777/.lane-lock
#   $ git worktree add lane-777 -b tmp origin/main
#   Preparing worktree (new branch 'tmp')
#   fatal: '.../lane-777' already exists          <- exit 128, and branch `tmp` now EXISTS
#
# So the lock files live in a SIBLING LOCK ROOT keyed by issue, and `acquire` creates
# ONLY that root — it does not create the lane directory at all. Two further properties
# come free and must not be given back: `git worktree remove` (or an `rm -rf` of a
# finished lane) cannot destroy a live lock, and the lock never appears inside a
# worktree whose untracked set agent-gate.sh's `tree-integrity` (#2926) captures.
#
# GITIGNORE / tree-integrity INTERACTION (do not undo)
# ----------------------------------------------------
# The lock root is OUTSIDE any worktree, which is what actually keeps this lock away
# from agent-gate.sh's `tree-integrity` capture (#2926) — that check takes its untracked
# set from `git ls-files --others --exclude-standard`, so a lock file written or
# refreshed mid-gate INSIDE a worktree would trip `tree-mutated-midrun` and VOID a gate
# of record. `.lane-lock*` and `lane-[0-9]*.lock` stay gitignored (see .gitignore,
# #3436) as a BELT, for a pre-move record and for a `LANE_ROOT` pointed inside a
# checkout; the ignore rule is not the thing that makes this safe.
#
# THERE IS NO LOCK-ROOT ENV VAR, AND ONE MAY NEVER BE ADDED (#3312, owner ruling on
# #3436). "The constrained party must not choose its own enforcer. A lock a worker can
# trivially remove or relocate is not a lock." A dedicated lock-root knob is precisely a
# relocation knob: point it at a fresh empty directory and your `acquire` succeeds while
# the real lane's lock still says OCCUPIED, so the lock is defeated by one variable. The
# direct precedent is #3312's `WAIVER_SCAN_TOOL`, removed for this reason after the same
# argument — "theoretically redundant" did not justify leaving it.
#   `LANE_ROOT` is DIFFERENT and stays: it relocates THE WHOLE WORLD — the lane
#   directories AND their locks, consistently — so it is a fleet-layout parameter, and
#   moving it is a different deployment rather than a bypass. A lock-ONLY root would
#   move the ENFORCER while leaving the lanes where they are. That asymmetry is the
#   entire justification, which is why it is written here rather than assumed.
#   The tests isolate by setting `LANE_ROOT`, for the same reason.
#
# LANE DIRECTORY RESOLUTION — the lane is the SUBJECT, not where the lock lives
# ----------------------------------------------------------------------------
#   `--lane-dir <abs path>`, else `${LANE_ROOT:-/data/lanes}/lane-<issue>`.
#   It is recorded as the record's `lane-dir=` field and reported on every verdict
#   line, and it is what the pid ancestor-walk matches cwds against (that is a question
#   about the SESSION, so its semantics are unchanged). The record's own PATH is
#   derived from the LOCK ROOT and the issue number, never from the lane directory.
#   A RELATIVE --lane-dir is a usage error, on CLAUDE.md's CQLITE_SCHEMAS_ROOT
#   precedent: a relative root resolves against each caller's cwd, so two callers
#   would lock two different directories while believing they shared one — the exact
#   class of bug this file is about.
#
# SUBCOMMANDS
#   acquire <N> [--lane-dir <p>] [--actor <id>] [--pid <pid>]
#           take the lane. ACQUIRED / ACQUIRED (re-entrant) / ACQUIRED (reclaimed)
#           / OCCUPIED (exit 2, and it NAMES the occupant — AC2).
#   verify  <N> [--lane-dir <p>] [--actor <id>] [--pid <pid>]
#           exit 0 iff the record holds OUR EXACT token (VERIFY-OK / VERIFY-FAIL).
#   probe   <N> [--lane-dir <p>] [--actor <id>] [--pid <pid>]
#           read-only liveness report: FREE / HELD, always exit 0 for an occupied
#           lane — occupancy is DATA to a probe, not an error. Writes NOTHING (no
#           record, no mutex, no log, and it never creates the lane dir). This is the
#           entry point another tool calls (e.g. claim.sh warning about an occupied
#           lane, #3436 AC5).
#           IT RESOLVES OUR IDENTITY IN COMPARE MODE, so `liveness=SELF` is reachable
#           and MEANS "THIS VERY SESSION HOLDS IT". That distinction is what
#           claim.sh's #3436 AC5/AC6 reporting keys on: SELF ("you already occupy
#           your own lane — re-acquire the claim, the released-then-resumed state")
#           and ALIVE ("a DIFFERENT live process on this box owns that lane — do not
#           touch it") have OPPOSITE remedies, and with no identity to compare
#           against they are textually identical. `--actor`/`--pid` are accepted for
#           the same reason `verify` accepts them; omitted, the session pid is
#           auto-resolved, which is why a caller in the session's own process tree
#           needs no flag at all. A degraded or non-live identity simply cannot match
#           SELF — probe still reports the record's liveness and still exits 0.
#           `reclaimable=` states whether an `acquire` would auto-reclaim, so a caller
#           need not re-implement the DEAD-*/UNKNOWN-* split.
#   release <N> [--lane-dir <p>] [--actor <id>] [--force]
#           delete the record. Without --force it requires our exact token
#           (RELEASE-REFUSED otherwise); --force deletes unconditionally (reaper).
#           Releasing a free lane is RELEASED (already free), exit 0 — idempotent, so
#           cleanup paths are safe to run twice or after a crash.
#   reclaim <N> --expect <token>|none --reason <why> [--lane-dir <p>] [--actor <id>] [--pid <pid>]
#           compare-and-swap adoption, the same discipline as `claim.sh adopt
#           --expect`. --expect <token>: the record's CURRENT token must equal it
#           exactly. --expect none: the record must NOT exist. --expect '' is a usage
#           error. --reason is REQUIRED and is validated exactly like claim.sh's:
#           empty, nothing-recordable ('   ', '---', '…'), a bare placeholder (why,
#           todo, tbd, xxx, …) and any RAW value still carrying an unsubstituted
#           '<…>' (checked BEFORE sanitization, so a copied template fails) are each
#           exit 64.
#   status  [<N>] [--lane-root <p>] [--lock-root <p>] [--lane-dir <p>]
#           render one line per lane lock; with no <N>, enumerate
#           <lock-root>/lane-*.lock. `--lane-root` names the LANE root the lock root is
#           derived from; `--lock-root` names the lock root directly. `--lane-dir` names
#           the SUBJECT lane for a single <N> and no longer locates the record.
#
# ENV
#   LANE_ROOT         lane-directory root (default /data/lanes)
#   LANE_LOCK_MACHINE machine identity (default `hostname -s`). A fleet whose boxes
#                     do NOT have unique short hostnames MUST set this per box, for
#                     the same reason claim.sh's CLAIM_MACHINE says so.
#   LANE_LOCK_ACTOR   default actor when --actor is omitted (default: flow)
#   LANE_LOCK_PID     default recorded pid when --pid is omitted (pid-scope=explicit)
#
# EXIT CODES
#   0   ACQUIRED / ACQUIRED (re-entrant) / ACQUIRED (reclaimed) / VERIFY-OK /
#       FREE / HELD / RELEASED / RECLAIMED / status render
#   2   OCCUPIED / VERIFY-FAIL / RELEASE-REFUSED / RECLAIM-LOST  (a REFUSAL: a real
#       answer about ownership)
#   1   ERROR reason=infra|unsupported|unresolved-identity <detail> —
#       environmental/CORRECTABLE, and NEVER a refusal verdict. A caller must not read
#       exit 1 as "someone else holds it". `unresolved-identity` in particular means
#       "this invocation cannot name a durable owner, so nothing was written" and its
#       text prints the two corrections (cwd inside the lane, or --pid).
#   64  usage error (stderr, no `LANE-LOCK:` line)
#
# OUTPUT CONTRACT
#   Every informative line is ONE line on stdout prefixed `LANE-LOCK: `, verdict word
#   first, then space-separated `key=value` fields. Notes and degradations go to
#   stderr prefixed `[lane-lock] `. (`status` prints one such line per lane.)
#
# SCOPE LIMITS — stated because each one is a real gap, not an oversight
#   * LINUX /proc-SPECIFIC. Every liveness verdict except SELF/UNKNOWN-FOREIGN reads
#     /proc. On a host without /proc the answer is UNKNOWN-NO-PROC, which refuses.
#   * A LANE WHOSE SESSION NEVER ACQUIRED IS INVISIBLE. This is a lock, not a
#     scanner: it can only report occupants that took it. #3436's session B took
#     nothing, so the FIRST session must acquire before writing, and every entry
#     point into a lane must call it. Detecting an un-locked occupant needs a
#     different signal (that is the advertised-collision scan half of #3436).
#   * IT IS MACHINE-LOCAL AND SAYS NOTHING CROSS-MACHINE. Two boxes each hold their
#     own /data/lanes/lane-<N>. Cross-machine arbitration is `refs/claims/issue-<N>`'s
#     job (claim.sh) and this file never contacts a remote, never runs git, and never
#     runs gh — so no verdict here depends on the network.
#   * IT DOES NOT SERIALISE WORK, only lane DIRECTORIES. Two sessions in two
#     directories on one box are outside its remit (#3430's layout question).
#
# CONSTRAINTS
#   macOS bash 3.2 compatible (no associative arrays, no mapfile/readarray).
#   `set -euo pipefail`, shellcheck-clean. No git, no gh, no network, no cargo.
#   Deliberately INDEPENDENT of claim.sh (nothing is sourced): the two locks arbitrate
#   different things, and a shared library would couple a machine-local mutex to a
#   remote-ref protocol. Where a rule is claim.sh's, it is restated and attributed.
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

RECORD_VERSION=1

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[lane-lock] $*" >&2; }
emit()      { echo "LANE-LOCK: $*"; }

# emit_infra <detail…> — a transient/environmental failure. Paired with `return 1`
# per the header's exit-code contract, so a caller never reads it as a refusal.
emit_infra()       { emit "ERROR reason=infra $* (transient — retry)"; }
emit_unsupported() { emit "ERROR reason=unsupported $*"; }

print_help() {
  awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2 {sub(/^# ?/,""); print}' "$0"
}

# ---------------------------------------------------------------------------
# sanitize_field <text> — collapse a free-text value into ONE parseable token.
# Applied to EVERY value that lands in the record OR is compared against one. The
# rule set and its rationale are claim.sh's `sanitize_field` (issue #2945), restated
# here because the two scripts are deliberately independent:
#   * keep [A-Za-z0-9._:/#-]; '=' is NOT kept, so a value can never introduce a new
#     `key=` pair (the forgery vector);
#   * every other run (spaces, newlines, quotes, shell metacharacters) becomes one
#     '-';
#   * collapse -> trim -> cut at 120 -> RE-TRIM. The trim order is part of the
#     contract: cutting after trimming can re-introduce the trailing separator the
#     trim just promised to remove.
#   * LC_ALL=C on both tr and sed: BSD/macOS `tr` aborts with "Illegal byte
#     sequence" on non-ASCII under a UTF-8 locale, and a --reason with an em dash is
#     a likely invocation here; under `set -e` that killed the whole script with no
#     verdict line at all.
sanitize_field() {
  local s
  s="$(printf '%s' "${1:-}" | LC_ALL=C tr -c 'A-Za-z0-9._:/#-' '-' | LC_ALL=C sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"
  s="$(LC_ALL=C printf '%.120s' "$s")"
  s="${s%-}"
  [ -n "$s" ] || s="unspecified"
  printf '%s\n' "$s"
}

# this_machine — machine identity, sanitized ONCE so the written and the compared
# token are always the same value (claim.sh's `this_machine`, #2945 review).
this_machine() { sanitize_field "${LANE_LOCK_MACHINE:-$(hostname -s 2>/dev/null || echo unknown-host)}"; }

# resolve_actor <raw> — the actor is part of the HOLDER IDENTITY, so an unrecordable
# value must not fall back to the `unspecified` sentinel: two distinct-but-
# unrecordable actors would ALIAS onto one identity and the second could satisfy the
# holder gate for a lane it does not own (claim.sh #2945 review, same fail-closed
# direction).
resolve_actor() {
  local tok
  tok="$(sanitize_field "${1:-}")"
  if [ "$tok" = "unspecified" ] || [ "${#tok}" -lt 3 ]; then
    die_usage "--actor must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '${1:-}' records as '$tok', and the actor is part of the HOLDER IDENTITY — two unrecordable actors must never alias onto one identity"
  fi
  printf '%s\n' "$tok"
}

require_numeric_issue() {
  case "${1:-}" in
    *[!0-9]* | '') die_usage "$2 requires a numeric issue number (got '${1:-<none>}')" ;;
  esac
}

# require_abs_path <flag> <value> — path validation happens AT THE ARGUMENT BOUNDARY,
# in the MAIN shell, and must never be left to resolve_lane_dir alone.
#
# WHY (measured, not theoretical): callers resolve the lane dir inside a command
# substitution, and `die_usage`'s `exit 64` there exits only the SUBSHELL. A single
# substitution is still fatal by accident (`set -e` propagates the assignment's
# status), but a NESTED one — `lane="$(lane_real "$(resolve_lane_dir …)")"` — swallows
# it completely: `verify --lane-dir ./lane` printed the usage error to stderr AND THEN
# a `VERIFY-FAIL … lane-dir=` verdict for an empty lane directory, i.e. a refusal
# about a lane nobody named. Validating here removes the reliance on `set -e` reaching
# out of a subshell at all.
require_abs_path() {
  case "${2:-}" in
    /*) ;;
    *) die_usage "$1 must be an ABSOLUTE path (got '${2:-<empty>}'): a relative path resolves against each caller's cwd, so two callers would lock two different directories while believing they shared one" ;;
  esac
}

# ---------------------------------------------------------------------------
# /proc readers
# ---------------------------------------------------------------------------

proc_available() { [ -d /proc/self ]; }

# live_boot_id — the FULL boot id, or "" when it cannot be read.
live_boot_id() {
  [ -r /proc/sys/kernel/random/boot_id ] || return 0
  LC_ALL=C tr -d ' \t\n' < /proc/sys/kernel/random/boot_id 2>/dev/null || true
}

boot_short() { printf '%s\n' "$(printf '%s' "${1:-}" | LC_ALL=C tr -d '-' | LC_ALL=C cut -c1-8)"; }

# proc_stat_field <pid> <n> — field <n> of /proc/<pid>/stat counting from the field
# AFTER comm: n=1 state, n=2 ppid, n=20 starttime (overall field 22).
#
# THE PARSE IS THE POINT. comm (overall field 2) is parenthesized and may contain
# spaces AND parentheses — the fleet's own tmux server reports `(tmux: server)` — so
# `awk '{print $22}'` on the raw line is wrong for any such process. Split on the
# LAST ')' and index from there. Globbing is disabled around the word split because a
# comm could contain '*' (the split happens after the last ')', but the discipline is
# claim.sh's msg_field's and costs nothing).
proc_stat_field() {
  local pid="$1" n="$2" line rest
  [ -r "/proc/$pid/stat" ] || return 1
  IFS= read -r line < "/proc/$pid/stat" 2>/dev/null || return 1
  case "$line" in *')'*) ;; *) return 1 ;; esac
  rest="${line##*)}"
  (
    set -f
    # shellcheck disable=SC2086  # deliberate word split of the post-comm stat fields
    set -- $rest
    [ "$#" -ge "$n" ] || exit 1
    while [ "$n" -gt 1 ]; do shift; n=$((n - 1)); done
    printf '%s\n' "$1"
  )
}

proc_start_ticks() { proc_stat_field "$1" 20; }
proc_state()       { proc_stat_field "$1" 1; }
proc_ppid()        { proc_stat_field "$1" 2; }

# ---------------------------------------------------------------------------
# resolve_pid — layered and three-valued (see the header). Sets REPLY_PID and
# REPLY_PID_SCOPE.
# ---------------------------------------------------------------------------
REPLY_PID=""
REPLY_PID_SCOPE=""
resolve_pid() {
  local lane_real="$1" explicit="${2:-}" purpose="${3:-write}"
  local candidate="" scope=""
  if [ -n "$explicit" ]; then
    candidate="$explicit"; scope=explicit
  elif [ -n "${LANE_LOCK_PID:-}" ]; then
    case "${LANE_LOCK_PID}" in
      *[!0-9]* | '') die_usage "LANE_LOCK_PID must be numeric (got '${LANE_LOCK_PID}')" ;;
    esac
    candidate="${LANE_LOCK_PID}"; scope=explicit
  else
    # Auto-resolve: walk $$ -> ppid -> … and keep the OUTERMOST ancestor whose cwd is
    # the lane directory (or inside it). Stop at pid 1 and never record pid 1 or a
    # non-matching ancestor — see the header for why the shared tmux server must be
    # excluded.
    local p="$$" best="" cwd ppid guard=0
    while [ -n "$p" ] && [ "$p" != "1" ] && [ "$guard" -lt 64 ]; do
      cwd="$(readlink "/proc/$p/cwd" 2>/dev/null || true)"
      case "$cwd" in
        "$lane_real" | "$lane_real"/*) best="$p" ;;
      esac
      ppid="$(proc_ppid "$p" 2>/dev/null || true)"
      [ -n "$ppid" ] || break
      case "$ppid" in *[!0-9]*) break ;; esac
      p="$ppid"
      guard=$((guard + 1))
    done
    if [ -n "$best" ] && [ "$best" != "$$" ]; then
      candidate="$best"; scope=session
    else
      # Only $$ matched (or nothing did): this invocation's own shell, which dies
      # between tool calls. `ephemeral` is REPORTED here and judged by the CALLER:
      #   * a WRITE (acquire/reclaim) REFUSES it — `reason=unresolved-identity`, see
      #     require_durable_identity. Writing such a record BRICKS the lane (below).
      #   * a COMPARE (probe) keeps it and says so on its own line as
      #     `our-identity=UNRESOLVED`, because "I could not tell whether this is you" is
      #     the useful answer from a read-only report and refusing would be useless.
      candidate="$$"; scope=ephemeral
    fi
  fi
  case "$candidate" in
    *[!0-9]* | '') die_usage "--pid must be numeric (got '${candidate:-<none>}')" ;;
  esac
  # An EXPLICIT pid with no /proc entry cannot have a process identity recorded for
  # it, and recording an identity-less holder would create a lock nobody can ever
  # reclaim automatically. Refuse at the argument boundary instead.
  #
  # …but ONLY for purpose=write. `probe` resolves an identity purely to COMPARE (so it
  # can say SELF), records nothing, and is called on the SUCCESS path of another tool
  # (claim.sh): a read-only report must never be able to change its caller's verdict,
  # so for purpose=compare a non-live pid is a NOTE and the comparison simply cannot
  # match SELF. A non-NUMERIC pid stays fatal in both modes — that is unambiguously a
  # caller typo, not an environment state.
  if [ "$scope" = explicit ] && [ "$purpose" = compare ] && proc_available && [ ! -e "/proc/$candidate" ]; then
    note "pid $candidate is not live on this host (no /proc/$candidate); a compare-only identity cannot match the holder's, so SELF is unreachable for this call"
  elif [ "$scope" = explicit ] && [ "$purpose" = write ] && proc_available && [ ! -e "/proc/$candidate" ]; then
    die_usage "--pid $candidate is not a live process on this host (no /proc/$candidate) — a lock records the HOLDER's process identity (boot id + start ticks), which cannot be read for a pid that does not exist"
  fi
  REPLY_PID="$candidate"
  REPLY_PID_SCOPE="$scope"
}

# build_token <machine> <actor> <pid> <boot-short> <start-ticks>
# The five-component identity. An unavailable boot-id / start-ticks records as '-' so
# the token stays parseable and can only ever equal another equally-degraded record's
# (and such a record's liveness is UNKNOWN-*, which refuses).
build_token() {
  printf '%s:%s:%s:%s:%s\n' "$1" "$2" "$3" "${4:--}" "${5:--}"
}

# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------
REC_UNREADABLE=0
REC_ISSUE=""; REC_LANE_DIR=""; REC_MACHINE=""; REC_ACTOR=""; REC_PID=""
REC_PID_SCOPE=""; REC_BOOT_ID=""; REC_START_TICKS=""; REC_ACQUIRED_TS=""
REC_ACQUIRED_EPOCH=""; REC_NONCE=""; REC_VERSION=""

# parse_record <file> — populate the REC_* globals. Returns 1 when the file does not
# exist (no lock), 0 otherwise; an existing-but-unparseable record sets
# REC_UNREADABLE=1 and still returns 0, because "a record is present and says nothing
# intelligible" is a DIFFERENT state from "there is no record" and the two must never
# collapse (the absent case is FREE; the unparseable case REFUSES).
#
# Only `^[a-z-]+=` lines are parsed and unknown keys are IGNORED (forward
# compatibility). A DUPLICATE key is UNREADABLE: two values for one key means the
# record cannot be said to state anything, and picking either one would be a guess.
parse_record() {
  local f="$1" line key val seen=""
  REC_UNREADABLE=0
  REC_ISSUE=""; REC_LANE_DIR=""; REC_MACHINE=""; REC_ACTOR=""; REC_PID=""
  REC_PID_SCOPE=""; REC_BOOT_ID=""; REC_START_TICKS=""; REC_ACQUIRED_TS=""
  REC_ACQUIRED_EPOCH=""; REC_NONCE=""; REC_VERSION=""
  [ -f "$f" ] || return 1
  if [ ! -r "$f" ]; then REC_UNREADABLE=1; return 0; fi
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      *=*) ;;
      *) continue ;;
    esac
    key="${line%%=*}"
    val="${line#*=}"
    case "$key" in
      '' | *[!a-z-]*) continue ;;
    esac
    case " $seen " in
      *" $key "*) REC_UNREADABLE=1; return 0 ;;
    esac
    seen="$seen $key"
    case "$key" in
      version)      REC_VERSION="$val" ;;
      issue)        REC_ISSUE="$val" ;;
      lane-dir)     REC_LANE_DIR="$val" ;;
      machine)      REC_MACHINE="$val" ;;
      actor)        REC_ACTOR="$val" ;;
      pid)          REC_PID="$val" ;;
      pid-scope)    REC_PID_SCOPE="$val" ;;
      boot-id)      REC_BOOT_ID="$val" ;;
      start-ticks)  REC_START_TICKS="$val" ;;
      acquired-ts)  REC_ACQUIRED_TS="$val" ;;
      acquired-epoch) REC_ACQUIRED_EPOCH="$val" ;;
      nonce)        REC_NONCE="$val" ;;
      *) : ;;   # unknown key: ignored on purpose
    esac
  done < "$f"
  if [ "$REC_UNREADABLE" -eq 0 ] && [ -z "$REC_MACHINE" ]; then
    # No machine identity at all: an empty file, a truncated write, or something that
    # is not one of our records. Nothing can be established from it.
    REC_UNREADABLE=1
  fi
  return 0
}

# record_token — the holder token implied by the parsed record.
record_token() {
  build_token "$REC_MACHINE" "$REC_ACTOR" "$REC_PID" "$(boot_short "$REC_BOOT_ID")" "$REC_START_TICKS"
}

# record_liveness <our-token> — print exactly one verdict from the CLOSED set in the
# header. Requires parse_record to have run.
#
# AFFIRMATIVE MEASUREMENT (CLAUDE.md): every branch below reaches ALIVE or a DEAD-*
# only from a signal that was actually READ. Anything unread, unrecognised or
# unreadable lands on an UNKNOWN-*, and only DEAD-* permits auto-reclaim — so an
# unmeasured state can never inherit the permissive answer. Do not "simplify" any of
# these UNKNOWN-* branches into a fallthrough that returns ALIVE or DEAD-*.
record_liveness() {
  local our_token="$1" live_boot rec_token live_ticks state
  if [ "$REC_UNREADABLE" -eq 1 ]; then printf 'UNKNOWN-UNREADABLE\n'; return 0; fi
  rec_token="$(record_token)"
  if [ "$rec_token" = "$our_token" ]; then printf 'SELF\n'; return 0; fi
  if [ "$REC_MACHINE" != "$(this_machine)" ]; then printf 'UNKNOWN-FOREIGN\n'; return 0; fi
  if [ "$REC_PID_SCOPE" = "ephemeral" ]; then printf 'UNKNOWN-EPHEMERAL\n'; return 0; fi
  case "${REC_PID:-}" in
    '' | *[!0-9]*) printf 'UNKNOWN-NO-PID\n'; return 0 ;;
  esac
  if ! proc_available; then printf 'UNKNOWN-NO-PROC\n'; return 0; fi
  live_boot="$(live_boot_id)"
  if [ -z "$live_boot" ]; then
    # We cannot read THIS host's boot id, so the record's boot id cannot be compared
    # and a pid means nothing. Unmeasurable, not dead.
    printf 'UNKNOWN-NO-PROC\n'; return 0
  fi
  if [ -z "$REC_BOOT_ID" ]; then printf 'UNKNOWN-NO-BOOT-ID\n'; return 0; fi
  if [ "$REC_BOOT_ID" != "$live_boot" ]; then
    # A reboot killed every process that existed under the old boot id. This is the
    # one DEAD verdict that needs no pid read at all.
    printf 'DEAD-REBOOT\n'; return 0
  fi
  case "${REC_START_TICKS:-}" in
    '' | *[!0-9]*) printf 'UNKNOWN-NO-START-TICKS\n'; return 0 ;;
  esac
  if [ ! -e "/proc/$REC_PID" ]; then printf 'DEAD-NO-PROCESS\n'; return 0; fi
  if ! live_ticks="$(proc_start_ticks "$REC_PID" 2>/dev/null)" || [ -z "$live_ticks" ]; then
    # stat unreadable. Re-check existence: the process may simply have exited between
    # the two reads (DEAD-NO-PROCESS). If it is still there, running-vs-reused was
    # never established -> UNKNOWN-STATE, which refuses.
    if [ ! -e "/proc/$REC_PID" ]; then printf 'DEAD-NO-PROCESS\n'; else printf 'UNKNOWN-STATE\n'; fi
    return 0
  fi
  if [ "$live_ticks" != "$REC_START_TICKS" ]; then printf 'DEAD-PID-REUSED\n'; return 0; fi
  state="$(proc_state "$REC_PID" 2>/dev/null || true)"
  if [ -z "$state" ]; then
    # start-ticks matched, so this IS the recorded process, but running-vs-zombie was
    # never established. Refuse rather than assume either.
    printf 'UNKNOWN-STATE\n'; return 0
  fi
  if [ "$state" = "Z" ]; then printf 'DEAD-NO-PROCESS\n'; return 0; fi
  printf 'ALIVE\n'
}

# holder_fields — the occupant description shared by every refusal and report line.
# AC2: a collision must NAME the occupant (pid, start time, issue), never fail
# generically — "directory busy" sends the reader to the wrong problem.
holder_fields() {
  local now age="unknown"
  now="$(date -u +%s)"
  case "${REC_ACQUIRED_EPOCH:-}" in
    '' | *[!0-9]*) age="unknown" ;;
    *) age="$((now - REC_ACQUIRED_EPOCH))s" ;;
  esac
  printf 'holder-issue=%s holder-machine=%s holder-actor=%s holder-pid=%s holder-pid-scope=%s holder-start-ticks=%s holder-token=%s acquired-ts=%s age=%s\n' \
    "${REC_ISSUE:-<none>}" "${REC_MACHINE:-<none>}" "${REC_ACTOR:-<none>}" \
    "${REC_PID:-<none>}" "${REC_PID_SCOPE:-<none>}" "${REC_START_TICKS:-<none>}" \
    "$(record_token)" "${REC_ACQUIRED_TS:-<none>}" "$age"
}

# ---------------------------------------------------------------------------
# Writing: mktemp in the SAME directory + mv (atomic rename), so a concurrent
# reader never sees a partial record and the mutex file is never involved.
# ---------------------------------------------------------------------------
write_record() {
  # write_record <record-path> <issue> <lane-dir> <machine> <actor> <pid> <pid-scope>
  #              <boot-id> <start-ticks> [extra-lines…]
  local path="$1" issue="$2" lane="$3" machine="$4" actor="$5" pid="$6" scope="$7"
  local boot="$8" ticks="$9"
  shift 9
  local tmp ts epoch nonce
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  epoch="$(date -u +%s)"
  nonce="$$-${RANDOM}-${RANDOM}-$epoch"
  tmp="$(mktemp "$(dirname "$path")/.lane-lock.tmp.XXXXXX")" || return 1
  {
    printf 'version=%s\n' "$RECORD_VERSION"
    printf 'issue=%s\n' "$issue"
    printf 'lane-dir=%s\n' "$lane"
    printf 'machine=%s\n' "$machine"
    printf 'actor=%s\n' "$actor"
    printf 'pid=%s\n' "$pid"
    printf 'pid-scope=%s\n' "$scope"
    printf 'boot-id=%s\n' "$boot"
    printf 'start-ticks=%s\n' "$ticks"
    printf 'acquired-ts=%s\n' "$ts"
    printf 'acquired-epoch=%s\n' "$epoch"
    printf 'nonce=%s\n' "$(sanitize_field "$nonce")"
    local extra
    for extra in "$@"; do
      [ -n "$extra" ] && printf '%s\n' "$extra"
    done
  } >"$tmp" || { rm -f "$tmp"; return 1; }
  mv -f "$tmp" "$path" || { rm -f "$tmp"; return 1; }
  return 0
}

# append_audit <log> <verdict> <fields…> — append-only, NEVER truncated (#3436 AC3:
# the reclaim is recorded). A failed append is a NOTE, not a verdict change: losing
# an audit line must not turn a granted lock into a refusal (or vice versa).
append_audit() {
  local log="$1"; shift
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$log" 2>/dev/null ||
    note "could not append to audit log $log (lock verdict unaffected)"
}

# ---------------------------------------------------------------------------
# with_lock <mutex-path> <fn> [args…] — hold the flock for the whole read-modify-write
# of a mutating subcommand, then propagate the callee's exit status. The mutex PATH is
# passed in rather than derived from the lane directory: the lock files live in the
# sibling lock root (see the header's "WHY THE LOCK IS NOT IN THE LANE DIRECTORY"), so
# nothing here may write inside a lane.
# ---------------------------------------------------------------------------
with_lock() {
  local flock_file="$1"; shift
  if ! command -v flock >/dev/null 2>&1; then
    emit_unsupported "detail=flock-unavailable mutex=$flock_file (this lock needs flock for its read-modify-write; no fallback mutex is invented on purpose — see the header)"
    return 1
  fi
  if ! exec 9>"$flock_file"; then
    emit_infra "detail=mutex-unopenable path=$flock_file"
    return 1
  fi
  if ! flock 9; then
    emit_infra "detail=flock-failed path=$flock_file"
    exec 9>&-
    return 1
  fi
  local rc=0
  "$@" || rc=$?
  flock -u 9 2>/dev/null || true
  exec 9>&-
  return "$rc"
}

# ---------------------------------------------------------------------------
# Lane-directory resolution
# ---------------------------------------------------------------------------
lane_root() { printf '%s\n' "${LANE_ROOT:-/data/lanes}"; }

# lock_root — where the lock FILES live: a SIBLING of the lane directories, never
# inside one (header: `git worktree add` refuses a target that exists at all, so a lock
# under the lane dir makes acquire-before-worktree-add impossible). Absoluteness is
# validated ONCE in the main shell at startup, not here: this runs inside command
# substitutions, where a `die_usage` exit escapes only the subshell (see
# require_abs_path's measured rationale).
# NO env override, no `${…:-…}` fallback, and no flag on any mutating subcommand: a
# lock-root knob is a relocation knob, and relocating the enforcer defeats the lock (see
# the header's "THERE IS NO LOCK-ROOT ENV VAR"). It is derived from LANE_ROOT alone,
# which moves the lanes and their locks together.
lock_root()   { printf '%s\n' "$(lane_root)/.lane-locks"; }
lock_record() { printf '%s/lane-%s.lock\n'  "$(lock_root)" "$1"; }
lock_mutex()  { printf '%s/lane-%s.flock\n' "$(lock_root)" "$1"; }
lock_audit()  { printf '%s/lane-%s.log\n'   "$(lock_root)" "$1"; }

# resolve_lane_dir <issue> <--lane-dir value or ""> — a RELATIVE --lane-dir is a
# usage error (header: it would resolve differently per caller cwd, so two callers
# would lock two different directories while believing they shared one).
resolve_lane_dir() {
  local issue="$1" opt="${2:-}"
  if [ -n "$opt" ]; then
    case "$opt" in
      /*) printf '%s\n' "$opt" ;;
      *) die_usage "--lane-dir must be an ABSOLUTE path (got '$opt'): a relative lane dir resolves against each caller's cwd, so two callers would lock two different directories while believing they shared one" ;;
    esac
    return 0
  fi
  printf '%s/lane-%s\n' "$(lane_root)" "$issue"
}

# lane_real <path> — the resolved path when it exists, the literal path otherwise
# (acquire/reclaim mkdir first, so they always get the resolved form).
lane_real() {
  local p="$1"
  if [ -d "$p" ]; then ( cd "$p" && pwd -P ); else printf '%s\n' "$p"; fi
}

G_ISSUE=""; G_LANE=""; G_ACTOR=""; G_MACHINE=""; G_PID=""; G_SCOPE=""
G_BOOT=""; G_TICKS=""; G_TOKEN=""; G_RECORD=""; G_LOG=""; G_MUTEX=""

# prepare_identity <issue> <lane-real> <actor-raw> <pid-opt> — resolve OUR identity
# and the file paths. Every value is sanitized here, once, so the token we WRITE and
# the token we COMPARE are always the same value (claim.sh's this_machine rationale).
# prepare_identity <issue> <lane-real> <actor-raw> <pid-opt> [write|compare]
prepare_identity() {
  G_ISSUE="$1"
  G_LANE="$2"
  G_ACTOR="$(resolve_actor "$3")"
  G_MACHINE="$(this_machine)"
  resolve_pid "$G_LANE" "${4:-}" "${5:-write}"
  G_PID="$REPLY_PID"
  G_SCOPE="$REPLY_PID_SCOPE"
  G_BOOT="$(sanitize_field "$(live_boot_id)")"
  [ "$G_BOOT" != "unspecified" ] || G_BOOT=""
  G_TICKS="$(proc_start_ticks "$G_PID" 2>/dev/null || true)"
  case "${G_TICKS:-}" in
    *[!0-9]*) G_TICKS="" ;;
  esac
  if [ -z "$G_BOOT" ] || [ -z "$G_TICKS" ]; then
    note "degraded process identity for pid $G_PID (boot-id='${G_BOOT:-<unreadable>}' start-ticks='${G_TICKS:-<unreadable>}') — this record's liveness will read UNKNOWN-* and will therefore REFUSE rather than auto-reclaim"
  fi
  G_TOKEN="$(build_token "$G_MACHINE" "$G_ACTOR" "$G_PID" "$(boot_short "$G_BOOT")" "$G_TICKS")"
  # The record path is derived from the LOCK ROOT and the issue, never from the lane
  # directory (header). The lane directory is the SUBJECT of the lock, recorded as a
  # field.
  G_RECORD="$(lock_record "$G_ISSUE")"
  G_LOG="$(lock_audit "$G_ISSUE")"
  G_MUTEX="$(lock_mutex "$G_ISSUE")"
}

# require_durable_identity <sub> — REFUSE a write that cannot record a re-identifiable
# owner. Exit 1 (environmental/correctable), NEVER 2 (which asserts something about
# OWNERSHIP), and it WRITES NOTHING — not the record, not the mutex, not the audit line.
#
# WHY REFUSING TO CREATE BEATS REFUSING TO EVALUATE (this reversed an earlier design).
# An `ephemeral` pid is this invocation's own shell, which exits immediately, so the
# record's liveness reads UNKNOWN-EPHEMERAL forever — and every UNKNOWN-* refuses,
# INCLUDING the owning session's own later acquire. A single acquire from outside the
# lane therefore BRICKED the lane on first use: strictly worse than having no lock,
# because it reds on correct input, which is the lane agents learn to waive. The
# situation is real and not a wiring typo — at worktree-CREATION time the session is
# acting from the root checkout and is genuinely not in the lane yet, so no durable
# owner EXISTS to record. Refusing to create the unusable record is the fail-closed
# direction; continuing to EVALUATE an existing UNKNOWN-EPHEMERAL record (legacy or
# hand-made) as a refusal is a different question and is unchanged.
#
# IT IS A CORRECTABLE CONDITION AND THE REFUSAL PRINTS THE CORRECTION, which is what
# keeps it from being the "guard that never permits work" the owner ruling forbids: run
# the acquire with the caller's cwd INSIDE the lane directory, or name the durable
# process with --pid.
require_durable_identity() {
  local sub="$1"
  [ "$G_SCOPE" = "ephemeral" ] || return 0
  emit "ERROR reason=unresolved-identity detail=no-durable-session-process sub=$sub issue=$G_ISSUE pid-scope=$G_SCOPE candidate-pid=$G_PID lane-dir=$G_LANE (NOTHING WAS WRITTEN. This lock records the HOLDER's full process identity, and the only process it could find here is THIS invocation's own shell, which exits when this command returns. A record naming it reads UNKNOWN-EPHEMERAL forever, and every UNKNOWN-* refuses — including YOUR OWN later acquire — so writing it would BRICK the lane rather than lock it. Two corrections, both one line: (1) run this from a shell whose cwd is INSIDE the lane directory, which is what makes the long-lived session process findable — 'cd <lane-dir> && lane-lock.sh $sub $G_ISSUE'; or (2) name the durable process explicitly with '--pid <pid>'. If you are at worktree-CREATION time, do not acquire at all: the session is not in the lane yet, so no durable owner exists — the lock belongs to the session that works IN the lane)"
  return 1
}

self_fields() {
  printf 'token=%s machine=%s actor=%s pid=%s pid-scope=%s' \
    "$G_TOKEN" "$G_MACHINE" "$G_ACTOR" "$G_PID" "$G_SCOPE"
}

# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------
_acquire_locked() {
  local liveness prev_token
  if parse_record "$G_RECORD"; then
    liveness="$(record_liveness "$G_TOKEN")"
    prev_token="$(record_token)"
    case "$liveness" in
      SELF)
        # RE-ENTRANCY REQUIRES ALL FIVE TOKEN COMPONENTS (#3436). A same-machine,
        # same-actor acquire from a DIFFERENT live pid must NOT land here — it is the
        # OCCUPIED case below. claim.sh's machine+actor re-entrancy is precisely why
        # the claim ref could not stop #3436's second local session, so this must
        # NEVER be relaxed to a machine+actor comparison.
        emit "ACQUIRED (re-entrant) issue=$G_ISSUE lane-dir=$G_LANE $(self_fields) record=$G_RECORD"
        return 0
        ;;
      DEAD-*)
        # ONLY a DEAD-* verdict reclaims (#3436 AC3, and the affirmative-measurement
        # rule at record_liveness). The reclaim is RECORDED in the audit log.
        if ! write_record "$G_RECORD" "$G_ISSUE" "$G_LANE" "$G_MACHINE" "$G_ACTOR" "$G_PID" "$G_SCOPE" "$G_BOOT" "$G_TICKS" \
            "reclaimed-from=$prev_token" "reclaimed-prev-liveness=$liveness" \
            "reclaimed-ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            "reclaim-reason=auto-reclaim-dead-holder"; then
          emit_infra "detail=record-write-failed path=$G_RECORD"
          return 1
        fi
        append_audit "$G_LOG" "verdict=ACQUIRED-RECLAIMED issue=$G_ISSUE token=$G_TOKEN prev-token=$prev_token prev-liveness=$liveness reason=auto-reclaim-dead-holder"
        emit "ACQUIRED (reclaimed) issue=$G_ISSUE lane-dir=$G_LANE $(self_fields) prev-liveness=$liveness prev-token=$prev_token record=$G_RECORD"
        return 0
        ;;
      *)
        # ALIVE and every UNKNOWN-* REFUSE. AC2: the refusal NAMES the occupant.
        emit "OCCUPIED issue=$G_ISSUE lane-dir=$G_LANE liveness=$liveness $(holder_fields) our-token=$G_TOKEN"
        return 2
        ;;
    esac
  fi
  if ! write_record "$G_RECORD" "$G_ISSUE" "$G_LANE" "$G_MACHINE" "$G_ACTOR" "$G_PID" "$G_SCOPE" "$G_BOOT" "$G_TICKS"; then
    emit_infra "detail=record-write-failed path=$G_RECORD"
    return 1
  fi
  append_audit "$G_LOG" "verdict=ACQUIRED issue=$G_ISSUE token=$G_TOKEN prev-token=<none> prev-liveness=<none> reason=free-lane"
  emit "ACQUIRED issue=$G_ISSUE lane-dir=$G_LANE $(self_fields) record=$G_RECORD"
  return 0
}

cmd_acquire() {
  local issue="" lane_opt="" actor="${LANE_LOCK_ACTOR:-flow}" pid_opt="" lane
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-dir) [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane_opt="$2"; shift 2 ;;
      --actor)    [ "$#" -ge 2 ] || die_usage "--actor requires a value";    actor="$2";    shift 2 ;;
      --pid)      [ "$#" -ge 2 ] || die_usage "--pid requires a value";      pid_opt="$2";  shift 2 ;;
      -*) die_usage "acquire: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "acquire: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" acquire
  lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  # ONLY the LOCK ROOT is created. The lane directory is deliberately NOT created here:
  # AC1 is "detect an occupant BEFORE writing", so acquire must precede
  # `git worktree add` — and `git worktree add` refuses a target that exists at all
  # (measured; see the header), so creating the lane dir would forbid the very order AC1
  # requires.
  if ! mkdir -p "$(lock_root)" 2>/dev/null; then
    emit_infra "detail=lock-root-uncreatable path=$(lock_root)"
    return 1
  fi
  prepare_identity "$issue" "$lane" "$actor" "$pid_opt"
  # BEFORE the mutex, so a refused acquire creates NOTHING at all (#3436 FIX 5a).
  require_durable_identity acquire || return 1
  with_lock "$G_MUTEX" _acquire_locked
}

# ---------------------------------------------------------------------------
# verify — read-only (no flock: the record is replaced by atomic rename, so a reader
# always sees a complete file).
# ---------------------------------------------------------------------------
cmd_verify() {
  local issue="" lane_opt="" actor="${LANE_LOCK_ACTOR:-flow}" pid_opt="" lane liveness
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-dir) [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane_opt="$2"; shift 2 ;;
      --actor)    [ "$#" -ge 2 ] || die_usage "--actor requires a value";    actor="$2";    shift 2 ;;
      --pid)      [ "$#" -ge 2 ] || die_usage "--pid requires a value";      pid_opt="$2";  shift 2 ;;
      -*) die_usage "verify: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "verify: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" verify
  lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  prepare_identity "$issue" "$lane" "$actor" "$pid_opt"   # sets G_RECORD from the lock root
  if ! parse_record "$G_RECORD"; then
    emit "VERIFY-FAIL issue=$issue lane-dir=$lane reason=no-lock $(self_fields)"
    return 2
  fi
  liveness="$(record_liveness "$G_TOKEN")"
  if [ "$liveness" = "SELF" ]; then
    emit "VERIFY-OK issue=$issue lane-dir=$lane $(self_fields) acquired-ts=${REC_ACQUIRED_TS:-<none>}"
    return 0
  fi
  emit "VERIFY-FAIL issue=$issue lane-dir=$lane reason=not-holder liveness=$liveness $(holder_fields) our-token=$G_TOKEN"
  return 2
}

# ---------------------------------------------------------------------------
# probe — the read-only liveness reporter other tools call (claim.sh's occupied-lane
# warning, #3436 AC5). It WRITES NOTHING: no record, no audit line, and no mutex —
# which is why it must not take the flock (opening the mutex for flock would CREATE
# it). Occupancy is DATA here, not an error, so an occupied lane is exit 0; exit 2 is
# reserved for acquire/verify/release/reclaim.
#
# `liveness=NO-RECORD` on the FREE line is the ABSENCE of a record, deliberately
# outside the closed record-liveness set (which only describes a record that exists).
# `reclaimable=` states whether an `acquire` would auto-reclaim, so a caller does not
# have to re-implement the DEAD-*/UNKNOWN-* split.
# ---------------------------------------------------------------------------
cmd_probe() {
  local issue="" lane_opt="" actor="${LANE_LOCK_ACTOR:-flow}" pid_opt="" lane liveness reclaimable=no
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-dir) [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane_opt="$2"; shift 2 ;;
      --actor)    [ "$#" -ge 2 ] || die_usage "--actor requires a value";    actor="$2";   shift 2 ;;
      --pid)      [ "$#" -ge 2 ] || die_usage "--pid requires a value";      pid_opt="$2"; shift 2 ;;
      -*) die_usage "probe: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "probe: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" probe
  lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  local record; record="$(lock_record "$issue")"
  if ! parse_record "$record"; then
    emit "FREE issue=$issue lane-dir=$lane liveness=NO-RECORD record=absent"
    return 0
  fi
  # A probe RESOLVES OUR IDENTITY — in COMPARE mode, which writes nothing and refuses
  # nothing — so that `SELF` is reachable. It has to be: with an impossible token
  # (which is what this used to pass) our OWN session's lock reports ALIVE, textually
  # indistinguishable from a live LOCAL PEER's, and those two have OPPOSITE remedies —
  # "you already occupy your own lane, re-acquire the claim" vs "a different live
  # process on this box owns that lane, do not touch it". claim.sh's #3436 AC5/AC6
  # reporting keys on exactly that distinction. The identity resolution reuses the
  # ancestor walk, so a caller in the same process tree as the session (claim.sh is)
  # resolves the same session pid and the tokens match with no new flag.
  #
  # Nothing here may write or refuse: no mkdir of the lane dir, no record, no mutex,
  # and the return stays 0 for an occupied lane. A degraded identity (unreadable
  # boot-id/start-ticks, a non-live --pid) simply cannot match SELF, and the record's
  # own liveness is reported unchanged.
  prepare_identity "$issue" "$lane" "$actor" "$pid_opt" compare
  # parse_record above populated the REC_* globals; prepare_identity does not touch
  # them, but re-read anyway so the parse and the comparison are adjacent and no future
  # edit can separate them.
  parse_record "$record" || true
  liveness="$(record_liveness "$G_TOKEN")"
  case "$liveness" in DEAD-*) reclaimable=yes ;; esac
  emit "HELD issue=$issue lane-dir=$lane liveness=$liveness reclaimable=$reclaimable $(holder_fields) our-token=$G_TOKEN record=$record"
  return 0
}

# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------
_release_locked() {
  local force="$1" liveness prev_token
  if ! parse_record "$G_RECORD"; then
    # IDEMPOTENT on purpose: cleanup paths (trap handlers, a supervisor's teardown, a
    # second operator) must be safe to run twice or after a crash.
    emit "RELEASED (already free) issue=$G_ISSUE lane-dir=$G_LANE record=absent"
    return 0
  fi
  prev_token="$(record_token)"
  liveness="$(record_liveness "$G_TOKEN")"
  if [ "$force" != "1" ] && [ "$liveness" != "SELF" ]; then
    emit "RELEASE-REFUSED issue=$G_ISSUE lane-dir=$G_LANE reason=not-holder liveness=$liveness $(holder_fields) our-token=$G_TOKEN"
    return 2
  fi
  if ! rm -f "$G_RECORD"; then
    emit_infra "detail=record-unlink-failed path=$G_RECORD"
    return 1
  fi
  append_audit "$G_LOG" "verdict=RELEASED issue=$G_ISSUE token=$G_TOKEN prev-token=$prev_token prev-liveness=$liveness reason=$([ "$force" = 1 ] && echo forced-release || echo holder-release)"
  if [ "$force" = "1" ] && [ "$liveness" != "SELF" ]; then
    emit "RELEASED issue=$G_ISSUE lane-dir=$G_LANE mode=forced prev-token=$prev_token prev-liveness=$liveness"
  else
    emit "RELEASED issue=$G_ISSUE lane-dir=$G_LANE $(self_fields)"
  fi
  return 0
}

cmd_release() {
  local issue="" lane_opt="" actor="${LANE_LOCK_ACTOR:-flow}" force=0 lane
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-dir) [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane_opt="$2"; shift 2 ;;
      --actor)    [ "$#" -ge 2 ] || die_usage "--actor requires a value";    actor="$2";    shift 2 ;;
      --force)    force=1; shift ;;
      -*) die_usage "release: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "release: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" release
  lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  # No lock root means no record anywhere, and release is IDEMPOTENT — answer without
  # creating the root (a release must not have to make a directory to say "already
  # free"). The check moved from the lane dir to the lock root with the files.
  if [ ! -d "$(lock_root)" ]; then
    emit "RELEASED (already free) issue=$issue lane-dir=$lane record=absent"
    return 0
  fi
  prepare_identity "$issue" "$lane" "$actor" ""
  with_lock "$G_MUTEX" _release_locked "$force"
}

# ---------------------------------------------------------------------------
# reclaim — compare-and-swap adoption, the same discipline as `claim.sh adopt`.
# ---------------------------------------------------------------------------

# validate_reason <raw> — REQUIRED on reclaim. The rule set is claim.sh's `--reason`
# validation (#2945 + its review rounds), restated here rather than sourced (the two
# scripts are deliberately independent). In order:
#   1. An UNSUBSTITUTED '<…>' anywhere in the RAW value is refused BEFORE
#      sanitization. This is the case a post-sanitization check cannot see: the
#      documented `--reason resume-legacy:<branch>` sanitizes to
#      `resume-legacy:-branch`, which is not a sentinel and used to be ACCEPTED,
#      recording an unresolved template as the audit reason. These commands are read
#      by agents that run printed text LITERALLY, so any surviving '<…>' is a caller
#      bug, not a reason.
#   2. It must RECORD something: a value that sanitizes to the `unspecified` sentinel
#      or to fewer than 3 recordable characters ('   ', '---', '…') is
#      indistinguishable from no reason at all.
#   3. A bare PLACEHOLDER token is refused BY NAME. `<why>` sanitizes to `why` — 3
#      recordable chars, so the length gate passes and the record says `reason=why`:
#      exactly as uninformative as the case the gate exists to reject.
# The reason is the ONE field allowed internal '_'-joined text; whitespace sanitizes
# to '-' like every other value, so it stays a single parseable token.
validate_reason() {
  local raw="${1:-}" tok
  case "$raw" in
    *'<'*'>'*)
      die_usage "reclaim: --reason '$raw' still carries an UNSUBSTITUTED placeholder (<…>) — substitute it. e.g. --reason lane-holder-oom-killed-pid-4211 or --reason 'reaped lane, board says Ready'"
      ;;
  esac
  tok="$(sanitize_field "$raw")"
  if [ "$tok" = "unspecified" ] || [ "${#tok}" -lt 3 ]; then
    die_usage "reclaim: --reason must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '$raw' records as '$tok', which is indistinguishable from no reason at all"
  fi
  case "$(printf '%s' "$tok" | LC_ALL=C tr 'A-Z' 'a-z')" in
    why | reason | todo | tbd | tba | xxx | xxxx | placeholder | fixme | none | foo | bar | baz | n/a)
      die_usage "reclaim: --reason '$raw' records as the PLACEHOLDER '$tok' — as uninformative as no reason at all. Say what the reclaim IS, e.g. --reason lane-holder-oom-killed-pid-4211"
      ;;
  esac
  printf '%s\n' "$tok"
}

_reclaim_locked() {
  local expect="$1" reason="$2" liveness prev_token=""
  local have=0
  if parse_record "$G_RECORD"; then have=1; prev_token="$(record_token)"; fi
  if [ "$have" -eq 1 ]; then
    liveness="$(record_liveness "$G_TOKEN")"
  else
    liveness="NO-RECORD"
  fi

  # RE-ENTRANT RECLAIM (mirrors `claim.sh adopt`): the record may already hold OUR
  # exact token. Reporting RECLAIM-LOST there would make a session abandon a lane it
  # demonstrably owns while still holding it. But a VIOLATED compare-and-swap must
  # never be reported as a satisfied one, so when --expect names something the record
  # does NOT hold, the verdict names BOTH values.
  if [ "$liveness" = "SELF" ]; then
    if [ "$expect" = "$prev_token" ]; then
      emit "RECLAIMED (re-entrant) issue=$G_ISSUE lane-dir=$G_LANE $(self_fields) from=$expect"
    else
      emit "RECLAIMED (re-entrant, lease-mismatch expected=$expect actual=$prev_token) issue=$G_ISSUE lane-dir=$G_LANE $(self_fields) — we DO hold the lane, but the compare-and-swap precondition did NOT hold"
    fi
    return 0
  fi

  if [ "$expect" = "none" ]; then
    if [ "$have" -eq 1 ]; then
      emit "RECLAIM-LOST issue=$G_ISSUE lane-dir=$G_LANE expected=none actual=$prev_token liveness=$liveness $(holder_fields)"
      return 2
    fi
  else
    if [ "$have" -eq 0 ]; then
      emit "RECLAIM-LOST issue=$G_ISSUE lane-dir=$G_LANE expected=$expect actual=<none> liveness=$liveness"
      return 2
    fi
    if [ "$prev_token" != "$expect" ]; then
      emit "RECLAIM-LOST issue=$G_ISSUE lane-dir=$G_LANE expected=$expect actual=$prev_token liveness=$liveness $(holder_fields)"
      return 2
    fi
  fi

  if ! write_record "$G_RECORD" "$G_ISSUE" "$G_LANE" "$G_MACHINE" "$G_ACTOR" "$G_PID" "$G_SCOPE" "$G_BOOT" "$G_TICKS" \
      "reclaimed-from=${prev_token:-<none>}" "reclaimed-prev-liveness=$liveness" \
      "reclaimed-ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)" "reclaim-reason=$reason"; then
    emit_infra "detail=record-write-failed path=$G_RECORD"
    return 1
  fi
  append_audit "$G_LOG" "verdict=RECLAIMED issue=$G_ISSUE token=$G_TOKEN prev-token=${prev_token:-<none>} prev-liveness=$liveness reason=$reason"
  emit "RECLAIMED issue=$G_ISSUE lane-dir=$G_LANE $(self_fields) from=$expect prev-token=${prev_token:-<none>} prev-liveness=$liveness reason=$reason"
  return 0
}

cmd_reclaim() {
  local issue="" lane_opt="" actor="${LANE_LOCK_ACTOR:-flow}" pid_opt="" lane
  local expect="" expect_given=0 reason="" reason_given=0 reason_token
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-dir) [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane_opt="$2"; shift 2 ;;
      --actor)    [ "$#" -ge 2 ] || die_usage "--actor requires a value";    actor="$2";    shift 2 ;;
      --pid)      [ "$#" -ge 2 ] || die_usage "--pid requires a value";      pid_opt="$2";  shift 2 ;;
      --expect)   [ "$#" -ge 2 ] || die_usage "--expect requires a value"; expect="$2"; expect_given=1; shift 2 ;;
      --reason)   [ "$#" -ge 2 ] || die_usage "--reason requires a value"; reason="$2"; reason_given=1; shift 2 ;;
      -*) die_usage "reclaim: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "reclaim: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" reclaim
  # An EMPTY --expect '' is a usage error on purpose — the classic `--expect "$TOKEN"`
  # with TOKEN unset. Coercing it to `none` would turn a compare-and-swap into an
  # unconditional take.
  [ "$expect_given" -eq 1 ] || die_usage "reclaim requires --expect <token> (CAS against the CURRENT record) or --expect none (the record must NOT exist)"
  [ -n "$expect" ] || die_usage "reclaim: --expect '' is rejected on purpose — pass the holder token you expect, or the literal 'none'"
  [ "$reason_given" -eq 1 ] || die_usage "reclaim requires --reason saying what the reclaim IS (it is recorded in the record and the audit log next to who took it), e.g. --reason lane-holder-oom-killed-pid-4211"
  reason_token="$(validate_reason "$reason")"
  lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  if ! mkdir -p "$(lock_root)" 2>/dev/null; then
    emit_infra "detail=lock-root-uncreatable path=$(lock_root)"
    return 1
  fi
  prepare_identity "$issue" "$lane" "$actor" "$pid_opt"
  # reclaim WRITES a record, so it is refused on the same terms as acquire — otherwise
  # the break-glass could mint exactly the unreclaimable record acquire now refuses, and
  # the header's "nothing ever creates a record that cannot be re-identified" would be
  # false. `release --force` remains available from anywhere: it only DELETES.
  require_durable_identity reclaim || return 1
  with_lock "$G_MUTEX" _reclaim_locked "$expect" "$reason_token"
}

# ---------------------------------------------------------------------------
# status — read-only render. With no issue it enumerates <lock-root>/lane-*.lock.
# ---------------------------------------------------------------------------
# status_one <record-path> <lane-hint> <issue-hint> — the record is located by the LOCK
# ROOT and the issue; the lane directory is REPORTED, preferring the record's own
# `lane-dir=` field (the subject the holder actually named) over the caller's hint.
status_one() {
  local record="$1" lane_hint="$2" issue_hint="$3" liveness reclaimable=no
  if ! parse_record "$record"; then
    emit "FREE issue=$issue_hint lane-dir=$lane_hint liveness=NO-RECORD record=absent"
    return 0
  fi
  liveness="$(record_liveness '')"
  case "$liveness" in DEAD-*) reclaimable=yes ;; esac
  emit "HELD issue=${REC_ISSUE:-$issue_hint} lane-dir=${REC_LANE_DIR:-$lane_hint} liveness=$liveness reclaimable=$reclaimable $(holder_fields) record=$record"
}

cmd_status() {
  local issue="" root_opt="" lock_opt="" root lock lane base count=0 f
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-root) [ "$#" -ge 2 ] || die_usage "--lane-root requires a value"; require_abs_path --lane-root "$2"; root_opt="$2"; shift 2 ;;
      --lock-root) [ "$#" -ge 2 ] || die_usage "--lock-root requires a value"; require_abs_path --lock-root "$2"; lock_opt="$2"; shift 2 ;;
      --lane-dir)  [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane="$2"; shift 2 ;;
      -*) die_usage "status: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "status: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  root="${root_opt:-$(lane_root)}"
  case "$root" in /*) ;; *) die_usage "--lane-root must be an ABSOLUTE path (got '$root')" ;; esac
  # The lock root is named directly, else derived from the LANE root — the same default
  # the mutating subcommands use, so `status --lane-root X` still reports the locks a
  # `LANE_ROOT=X acquire` took.
  # `--lock-root` exists on `status` ONLY — a read-only render may be pointed at another
  # machine's copied lock root for inspection. No MUTATING subcommand takes it, and
  # there is no env form, because that would be the relocation knob the header refuses.
  if [ -n "$lock_opt" ]; then
    lock="$lock_opt"
  else
    lock="$root/.lane-locks"
  fi
  if [ -n "$issue" ]; then
    require_numeric_issue "$issue" status
    # --lane-dir names the SUBJECT lane only; it no longer locates the record.
    status_one "$lock/lane-$issue.lock" "$(lane_real "${lane:-$root/lane-$issue}")" "$issue"
    return 0
  fi
  if [ ! -d "$lock" ]; then
    emit "STATUS lock-root=$lock lane-root=$root locks=0 detail=lock-root-absent"
    return 0
  fi
  for f in "$lock"/lane-*.lock; do
    [ -f "$f" ] || continue
    base="${f##*/}"; base="${base%.lock}"
    status_one "$f" "$root/$base" "${base#lane-}"
    count=$((count + 1))
  done
  emit "STATUS lock-root=$lock lane-root=$root locks=$count"
  return 0
}

# ---------------------------------------------------------------------------
SUBCOMMAND="${1:-}"
[ "$#" -eq 0 ] || shift
case "$SUBCOMMAND" in
  -h | --help | help) print_help ;;
  acquire) cmd_acquire "$@" ;;
  verify)  cmd_verify "$@" ;;
  probe)   cmd_probe "$@" ;;
  release) cmd_release "$@" ;;
  reclaim) cmd_reclaim "$@" ;;
  status)  cmd_status "$@" ;;
  "") die_usage "a subcommand is required: acquire <N> | verify <N> | probe <N> [--pid <pid>] | release <N> [--force] | reclaim <N> --expect <token>|none --reason <why> | status [<N>]" ;;
  *)  die_usage "unknown subcommand: $SUBCOMMAND (expected acquire|verify|probe|release|reclaim|status)" ;;
esac
