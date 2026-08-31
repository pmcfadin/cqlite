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
#      inside it. pid-scope=cwd-match.
#
#      THE NAME IS DELIBERATE, AND `session` WAS THE WRONG ONE (#3436 FIX 14). This
#      test establishes that an ancestor is WORKING IN that lane. It does NOT establish
#      that the ancestor OUTLIVES the command, and those are different facts: a
#      transient subshell that just `cd`'d into the lane matches, and is outermost among
#      the matches, so it is selected. Labelling it `session` asserted durability that
#      was never measured — the same defect shape this file exists to fix, one level
#      down. Measured cost when it was called `session`: the wired
#      `( cd "$wt" && acquire <N> )` recorded the subshell, which exited on return, the
#      record read DEAD-NO-PROCESS, and a peer was granted the lane by auto-reclaim —
#      an acquire that returns ACQUIRED and protects NOTHING, which is a false clean and
#      strictly worse than a loud refusal. What makes the recorded pid durable is the
#      CALLER's discipline (the session's own cwd is the lane, or an explicit --pid), so
#      the sanctioned wiring is where that guarantee lives; this field only reports what
#      the walk found. `test_lane_lock.sh` pins the durable case by asserting the
#      recorded pid is the suite's own $$ AND that a second acquire is RE-ENTRANT — a
#      transient holder fails both, which is the guard that keeps the wiring honest.
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
#         lane-lock.sh reclaim <N> --expect <lease> --reason <why>   (CAS, recorded;
#                     take <lease> from `probe`'s or `status`'s `lease=` field)
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
#                     Every IDENTITY value is SANITIZED to one whitespace-free token
#                     before it is written OR compared (the same discipline as
#                     claim.sh's `sanitize_field`, restated here rather than sourced —
#                     see CONSTRAINTS). Without it an `--actor 'flow
#                     pid=1'`-shaped value could forge a holder field and win FALSE
#                     re-entrancy on someone else's lane, which is the failure this
#                     lock exists to make impossible.
#                     `lane-dir` IS THE ONE EXCEPTION AND IS WRITTEN RAW (#3436 FIX 9a).
#                     It has to be: it is a real filesystem path, and sanitizing it
#                     would corrupt any path containing a space — reporting a lane
#                     directory that does not exist is its own false clean. This
#                     paragraph previously claimed EVERY value was sanitized, which was
#                     simply untrue of this one field, in the one paragraph a reader
#                     relies on for the anti-forgery property. What holds instead:
#                       * a NEWLINE in `--lane-dir` is REFUSED at the argument boundary
#                         (require_abs_path), because a newline is the only character
#                         that could forge a second `key=` LINE in the record;
#                       * `lane-dir` is not an identity field — it is never compared to
#                         decide ownership (see record_is_self), so a raw value cannot
#                         win re-entrancy;
#                       * a duplicate key still reads UNKNOWN-UNREADABLE, so even a
#                         forged line fails CLOSED rather than granting anything;
#                       * on every EMITTED line it is the LAST field, so a consumer
#                         reads it as the rest of the line rather than as a word (a
#                         space-delimited scan turned `/data/my lanes/lane-5` into
#                         `/data/my`, and claim.sh then reported `no-lane-dir` for a lane
#                         that WAS locked — see claim.sh's msg_rest_field).
#                     Forging the record by hand remains possible and is INVOKER-CLASS
#                     per CLAUDE.md's triage rule: recorded, not fortified against.
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
#   reclaim <N> --expect <lease>|none --reason <why> [--lane-dir <p>] [--actor <id>] [--pid <pid>]
#           (RECLAIMED / RECLAIMED (re-entrant) / RECLAIM-LEASE-MISMATCH — its OWN verdict
#           word, exit 0, for a re-entrant reclaim whose --expect did NOT hold, so a caller
#           matching on the verdict word can see the violated precondition / RECLAIM-LOST)
#           compare-and-swap adoption, the same discipline as `claim.sh adopt
#           --expect`. --expect <lease>: the record's CURRENT lease must equal it
#           EXACTLY. The lease is `<token>#<nonce>` — the RECORD INCARNATION, published as
#           `lease=` by probe/status. It is NOT the holder token: the token is unchanged by
#           a same-process release+reacquire, so a token-based lease let a stale observer
#           overwrite a newly acquired LIVE lock (ABA). Read it from `lease=`, never build it.
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

# require_numeric_issue <value> <subcommand>
#
# DIGITS ARE NOT ENOUGH — THE KEY MUST BE CANONICAL (#3436, roborev round 3). Every path
# this tool derives (record, mutex, audit, default lane dir) interpolates the issue string
# RAW, so `3436` and `03436` are two different mutexes for ONE lane and BOTH acquire —
# concurrent ownership through an alias, which is the defect this file exists to prevent.
# A leading zero is REJECTED rather than normalised: an issue number never legitimately
# carries one, so silently rewriting the caller's key would hide a real caller bug (and a
# rewrite has to be applied at every derivation site, where the next one added would miss
# it). `0` itself is rejected too — there is no issue 0.
require_numeric_issue() {
  case "${1:-}" in
    *[!0-9]* | '') die_usage "$2 requires a numeric issue number (got '${1:-<none>}')" ;;
    0*) die_usage "$2: issue number '${1}' has a leading zero. Every lock path is derived from this string RAW, so '${1}' and '${1#0}' would be two different locks for ONE lane and both would be granted. Use the canonical form '${1#0}'." ;;
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
  # A NEWLINE IS REFUSED, and it is the only character that needs to be (#3436 FIX 9a).
  # The record is `key=value`, ONE PER LINE, and `lane-dir` is the one field written RAW
  # (a real path may contain spaces, so sanitizing it would corrupt it). A newline is
  # therefore the only value that can forge a second `key=` LINE. Spaces, quotes and
  # shell metacharacters are all fine in a path and are deliberately preserved.
  # ($'\n' and NOT "$(printf '\n')": command substitution STRIPS the trailing newline,
  # yielding an EMPTY needle, and `*""*` matches every string — the check would refuse
  # every path. Measured: it refused all of them.)
  case "${2:-}" in
    *$'\n'*)
      die_usage "$1 must not contain a NEWLINE: the lock record is one key=value per line and this value is written raw (a path may legitimately contain spaces), so a newline is the one character that could forge a record line"
      ;;
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
    #
    # SCOPE LIMIT, STATED BECAUSE IT CANNOT BE CLOSED HERE (#3436, roborev round 13).
    # TWO WORKERS SPAWNED BY THE SAME IN-LANE PARENT RESOLVE TO THAT PARENT AND SHARE A
    # TOKEN, so the second is granted `ACQUIRED (re-entrant)`. It is the same shape as the
    # defect this file exists to fix — claim.sh's machine+actor could not express "another
    # process on this box"; ancestry cannot express "two siblings under one in-lane parent"
    # — and it is INHERENT to an ancestry-derived identity, because siblings share every
    # ancestor. No refinement of this walk closes it.
    #
    # THE OBVIOUS GUARD WAS MEASURED AND REJECTED: "refuse if the resolved ancestor has an
    # in-lane child outside my own chain" fires on an ORDINARY single session — measured on
    # a live one, whose resolved ancestor had an in-lane child that was simply its own
    # earlier backgrounded shell. A refusing acquire BRICKS the lane (FIX 5/14), which is
    # strictly worse than the hole: bricking is reliable, the hole is conditional.
    #
    # THE CONTRACT INSTEAD: any integration that launches MORE THAN ONE worker per lane from
    # a shared in-lane parent MUST pass `--pid` (or LANE_LOCK_PID) naming each worker's own
    # durable process. Auto-resolution is a convenience for the one-worker-per-lane case and
    # says so in the record: `pid-scope=cwd-match` marks an ancestry-derived identity,
    # `explicit` a caller-supplied one, and every verdict line carries it via self_fields.
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
      candidate="$best"; scope=cwd-match
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

# record_lease — the RECORD INCARNATION, which is what a compare-and-swap must name.
# `<token>#<nonce>`: the token says WHO holds it, the nonce says WHICH ACQUISITION this is.
# A reclaim lease built from the token alone is an ABA hole (see _reclaim_locked), because
# a release+re-acquire by the same process reproduces the token exactly. The nonce is
# per-write (`$$-RANDOM-RANDOM-epoch`), so it changes on every acquire and every reclaim.
# It is published by `probe`/`status` as `lease=` so a caller never has to build it.
record_lease() { printf '%s#%s\n' "$(record_token)" "${REC_NONCE:-<none>}"; }

# record_token — the holder token implied by the parsed record.
record_token() {
  build_token "$REC_MACHINE" "$REC_ACTOR" "$REC_PID" "$(boot_short "$REC_BOOT_ID")" "$REC_START_TICKS"
}

# record_is_self — is the record OUR identity? A STRUCTURAL comparison of the five
# identity fields against the resolved G_* values, NEVER a comparison of the joined tokens.
#
# #3312's OWN RULE, APPLIED TO THE FILE THAT CITES IT (#3436 FIX 13f): control and data
# must not share a channel, and the remedy is to REMOVE THE SHARED CHANNEL, not to pick a
# rarer delimiter. The token is ':'-joined and `sanitize_field` KEEPS ':', so an actor may
# contain the delimiter — and a chosen `--actor` plus a hand-written record could make a
# joined string equal another session's and win SELF, with nothing downstream to catch it
# (the SELF compare deliberately precedes the numeric validation of start-ticks). That
# exploit is INVOKER-CLASS, so per CLAUDE.md's triage rule it is not a security fix; the
# structural comparison is simply the correct shape, and it costs one function.
#
# The joined form (build_token / record_token) is therefore for DISPLAY and the audit log;
# the five fields are for COMPARISON. Two deliberately different representations — do not
# conflate them again.
#
# It returns FALSE when this call resolved NO identity of its own (G_TOKEN empty, e.g.
# `status`) and when our identity is DEGRADED: without boot-id AND start-ticks the values
# are placeholders and two equally-degraded identities would compare equal, so an
# affirmative match requires affirmative components.
record_is_self() {
  [ -n "${G_TOKEN:-}" ] || return 1
  [ -n "${G_BOOT:-}" ] && [ -n "${G_TICKS:-}" ] || return 1
  [ "${REC_MACHINE:-}" = "$G_MACHINE" ] || return 1
  [ "${REC_ACTOR:-}" = "$G_ACTOR" ] || return 1
  [ "${REC_PID:-}" = "$G_PID" ] || return 1
  [ "${REC_START_TICKS:-}" = "$G_TICKS" ] || return 1
  # THE FULL BOOT ID, not the 8-char display form (#3436, roborev round 7). The short form
  # exists for the display token; comparing it here compared a 32-bit PREFIX of a 128-bit
  # value that is RECORDED IN FULL and sitting right there. A colliding prefix plus a reused
  # pid and start-ticks would have granted re-entrancy to a stale PRE-REBOOT record — and
  # because SELF is tested BEFORE the DEAD-REBOOT branch, that false SELF would have won.
  # This is the same defect as everything else this change fixed: comparing a truncated proxy
  # when the exact value was available. In the identity code written to be exact.
  [ "${REC_BOOT_ID:-}" = "$G_BOOT" ] || return 1
  return 0
}

# record_liveness — print exactly one verdict from the CLOSED set in the header. Requires
# parse_record to have run, and reads OUR identity from the G_* globals (all empty when the
# call resolved none, which makes SELF unreachable by construction — `status` relies on it).
#
# AFFIRMATIVE MEASUREMENT (CLAUDE.md): every branch below reaches ALIVE or a DEAD-*
# only from a signal that was actually READ. Anything unread, unrecognised or
# unreadable lands on an UNKNOWN-*, and only DEAD-* permits auto-reclaim — so an
# unmeasured state can never inherit the permissive answer. Do not "simplify" any of
# these UNKNOWN-* branches into a fallthrough that returns ALIVE or DEAD-*.
record_liveness() {
  local live_boot live_ticks state
  if [ "$REC_UNREADABLE" -eq 1 ]; then printf 'UNKNOWN-UNREADABLE\n'; return 0; fi
  if record_is_self; then printf 'SELF\n'; return 0; fi
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
  printf 'holder-issue=%s holder-machine=%s holder-actor=%s holder-pid=%s holder-pid-scope=%s holder-start-ticks=%s holder-token=%s lease=%s acquired-ts=%s age=%s\n' \
    "${REC_ISSUE:-<none>}" "${REC_MACHINE:-<none>}" "${REC_ACTOR:-<none>}" \
    "${REC_PID:-<none>}" "${REC_PID_SCOPE:-<none>}" "${REC_START_TICKS:-<none>}" \
    "$(record_token)" "$(record_lease)" "${REC_ACQUIRED_TS:-<none>}" "$age"
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
# with_lock <fd> <mutex-path> <fn> [args…] — hold the flock for the whole read-modify-write
# of a mutating subcommand, then propagate the callee's exit status. The mutex PATH is
# passed in rather than derived from the lane directory: the lock files live in the
# sibling lock root (see the header's "WHY THE LOCK IS NOT IN THE LANE DIRECTORY"), so
# nothing here may write inside a lane.
# ---------------------------------------------------------------------------
# dir_mutex <canonical-lane-dir> — a mutex keyed by the LANE DIRECTORY, not by the issue.
#
# WHY THIS EXISTS (#3436, roborev round 8 — a regression I introduced). The record is keyed by
# issue so that `release`/`probe` can find it WITHOUT being told a path (round 2 fix). But
# keying the MUTEX by issue alone means two DIFFERENT issue numbers naming the SAME directory
# take DIFFERENT mutexes and BOTH acquire — measured: `acquire 800 --lane-dir X` and
# `acquire 801 --lane-dir X` both returned ACQUIRED, which is the core collision protection
# bypassed. Before FIX 2 the record lived AT `<lane-dir>/.lane-lock`, so the DIRECTORY was the
# key and this could not happen; moving the lock out (necessary, because `git worktree add`
# refuses any existing target) re-keyed it and opened the alias.
#
# So acquire holds BOTH: this directory mutex (which makes two issues on one directory contend
# for real, closing the RACE) and the per-issue mutex (which keeps the by-issue record path
# working). The filename is the canonical path with `/` mapped to `_`, so it is deterministic,
# needs no hash tool, and is readable in a listing.
#
# RESIDUAL, stated rather than implied: two callers with DIFFERENT `LANE_ROOT` values naming the
# same directory still take different mutexes, because both live under the lock root. That is
# invoker-class (whoever sets LANE_ROOT controls the namespace) and out of the threat model
# per CLAUDE.md's triage rule; the accident this closes is the same-root, different-issue one.
dir_mutex() {
  local canon="$1" key name sum tail_
  key="$(printf '%s' "$canon" | tr '/' '_')"
  name="dir-${key}.flock"
  # BOUND THE FILENAME (#3436, roborev round 13). Flattening the WHOLE canonical path into
  # one component can exceed the filesystem's per-component limit (NAME_MAX, 255 here), and
  # the failure is `mutex-unopenable` on a path that is perfectly VALID — a refusal caused by
  # the key, not by the lane. Measured: this lane is 31 bytes, a deeply nested sanctioned
  # worktree (`.claude/worktrees/issue-<N>-<slug>`) is 125, so it is not triggered today and
  # is reachable. Short paths keep the readable form; only an over-long one falls back.
  #
  # A cksum COLLISION would give two different directories one mutex, i.e. they would contend
  # when they need not. That is over-serialisation, the SAFE direction — the unsafe direction
  # is two directories getting two mutexes when they are one, which is the alias this whole
  # mutex exists to close and which the full canonical path in the key still prevents.
  if [ "${#name}" -gt 255 ]; then
    sum="$(printf '%s' "$canon" | cksum | awk '{print $1}')"
    tail_="$(printf '%s' "$key" | tail -c 180)"
    name="dir-x${sum}-${tail_}.flock"
  fi
  printf '%s/%s\n' "$(lock_root)" "$name"
}

# same_dir_other_issue <issue> <canonical-lane-dir> — prints a live conflicting holder, if any.
# Scans the lock root for a record naming the SAME lane dir under a DIFFERENT issue. A DEAD
# holder does not block (its record is reclaimable and stale), and our OWN issue never blocks
# (that is the ordinary re-entrant/reclaim path).
same_dir_other_issue() {
  local issue="$1" canon="$2" f base other
  for f in "$(lock_root)"/lane-*.lock; do
    [ -f "$f" ] || continue
    base="${f##*/}"; other="${base#lane-}"; other="${other%.lock}"
    [ "$other" = "$issue" ] && continue
    parse_record "$f" || continue
    [ "${REC_LANE_DIR:-}" = "$canon" ] || continue
    case "$(record_liveness '')" in
      DEAD-*) continue ;;
      *) printf '%s|%s|%s\n' "$other" "${REC_PID:-<none>}" "${REC_ACQUIRED_TS:-<none>}"; return 0 ;;
    esac
  done
  return 1
}

# THE DESCRIPTOR IS AN EXPLICIT PARAMETER, AND THAT IS THE FIX FOR A GUARD THAT HELD
# NOTHING (#3436, roborev round 9). This helper NESTS — acquire takes the DIRECTORY mutex
# and then calls itself for the PER-ISSUE mutex — and both used a hardcoded fd 9, so the
# inner `exec 9>` CLOSED the outer descriptor and released the directory lock BEFORE the
# record write. Round 8's directory guard therefore ran, passed its (sequential) test, and
# protected nothing: two issues naming one directory could both scan clean and both
# acquire. bash 4.1's `exec {var}>` would auto-allocate and make the collision
# unrepresentable, but this file is declared macOS-bash-3.2 compatible (see the header), so
# the fd is PASSED — and a reuse is REFUSED rather than silently releasing, because a wrong
# fd is a programming error and the failure mode it replaces was silent.
with_lock() {
  local fd="$1"; shift
  local flock_file="$1"; shift
  case " ${LANE_LOCK_FDS_HELD:-} " in
    *" $fd "*)
      emit_infra "detail=mutex-fd-reused fd=$fd path=$flock_file (a nested with_lock reusing an outer descriptor closes it and SILENTLY RELEASES the outer lock — #3436 round 9. Give the inner lock its own fd.)"
      return 1
      ;;
  esac
  if ! command -v flock >/dev/null 2>&1; then
    emit_unsupported "detail=flock-unavailable mutex=$flock_file (this lock needs flock for its read-modify-write; no fallback mutex is invented on purpose — see the header)"
    return 1
  fi
  if ! eval "exec $fd>\"\$flock_file\""; then
    emit_infra "detail=mutex-unopenable path=$flock_file"
    return 1
  fi
  if ! flock "$fd"; then
    emit_infra "detail=flock-failed path=$flock_file"
    eval "exec $fd>&-"
    return 1
  fi
  LANE_LOCK_FDS_HELD="${LANE_LOCK_FDS_HELD:-} $fd"
  local rc=0
  "$@" || rc=$?
  flock -u "$fd" 2>/dev/null || true
  eval "exec $fd>&-"
  local _f _kept=""
  for _f in ${LANE_LOCK_FDS_HELD:-}; do
    [ "$_f" = "$fd" ] && continue
    _kept="$_kept $_f"
  done
  LANE_LOCK_FDS_HELD="$_kept"
  return "$rc"
}

# ---------------------------------------------------------------------------
# Lane-directory resolution
# ---------------------------------------------------------------------------
# LANE_ROOT IS VALIDATED IN THE MAIN SHELL BEFORE ANY DISPATCH — see
# require_lane_root_abs, called from the entry point. A comment here used to CLAIM that
# validation existed when it did not (roborev round 2), and the gap was a real bypass,
# not a cosmetic one: a RELATIVE LANE_ROOT resolves against each caller's cwd, so two
# sessions naming the SAME absolute lane directory compute DIFFERENT lock roots and BOTH
# acquire — the lock defeated by accident rather than by malice. Same reasoning as
# CLAUDE.md's absolute-only rule for CQLITE_SCHEMAS_ROOT, where a relative value would
# certify one root while the tests read another.
lane_root() { printf '%s\n' "${LANE_ROOT:-/data/lanes}"; }

# require_lane_root_abs — reject a relative or newline-bearing LANE_ROOT at the entry
# point, in the MAIN shell (a die inside a command substitution exits only the subshell;
# see require_abs_path's measured rationale). Unset is fine: the default is absolute.
require_lane_root_abs() {
  [ -n "${LANE_ROOT:-}" ] || return 0
  require_abs_path LANE_ROOT "$LANE_ROOT"
}

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
# lane_real <path> — the physical path, when it can be taken.
#
# #3436 FIX 13h: this used to be `( cd "$p" && pwd -P )` bare. An UNSEARCHABLE directory
# (mode 000, or a permission change under us) makes the `cd` fail, and because every
# caller uses it inside a command substitution, `set -e` aborted the run with a raw
# `cd: Permission denied` and NO `LANE-LOCK:` line at all — breaking the header's promise
# that exit 1 is always `ERROR reason=infra|unsupported`. A caller parsing our output saw
# nothing to parse. The path is a CONVENIENCE (it canonicalises symlinks), never a
# correctness input: the record is found by ISSUE, so falling back to the given path is
# both safe and the honest answer — we report what we were told when we cannot improve on
# it, and we say so on stderr rather than dying.

lane_real() {
  local p="$1" real=""
  # ONE PHYSICAL RESOLVER FOR BOTH CASES (#3436, roborev round 16). Two defects, one cause —
  # `..` was resolved LEXICALLY, and POSIX resolves it AFTER the symlink:
  #
  #   /base/link/../lane   with   link -> /other/sub     names   /other/lane
  #   lexical `..` removal instead yields                        /base/lane
  #
  # so two spellings of ONE directory took TWO directory mutexes and both acquired — the alias
  # hole round 10 closed for a symlinked ANCESTOR, reopened by `..`. Measured both ways:
  # `cd -P` and `realpath -m` agree on /other/lane; `lex_norm_path` returned /base/lane.
  #
  # AND IT WAS NOT ONLY THE NOT-YET-EXISTS BRANCH the review named. The existing-directory
  # branch used a LOGICAL `cd` (then `pwd -P`), which is the same mistake: with both
  # /base/lane and /other/lane present, logical lands on /base/lane and physical on
  # /other/lane. A first test agreed only because bash FALLS BACK to physical when the
  # logical target does not exist — the disagreement is invisible until both paths exist.
  #
  # So: walk the components LEFT TO RIGHT and resolve each existing prefix physically, which
  # is the order the kernel uses. `realpath -m` would do it in one call and is GNU-only; this
  # file is declared macOS bash 3.2 compatible (see the header), so it is done here.
  real="$(_resolve_lane_path "$p")"
  if [ -z "$real" ]; then
    note "could not canonicalise lane dir '$p' (unsearchable or vanished); using the path as given. The record is keyed by ISSUE, so this does not change which lock is read."
    real="$p"
  fi
  printf '%s\n' "$real"
}

# _resolve_lane_path <abs-path> — physical canonicalisation that tolerates a MISSING LEAF.
# Each component is appended in turn; whenever the accumulated prefix exists it is replaced by
# its PHYSICAL form, so a later `..` is taken against the resolved location rather than the
# spelling. Components after the last existing prefix cannot be resolved (they name nothing
# yet) and are appended literally — and a `..` in that tail is genuinely unresolvable, since
# the kernel itself would ENOENT, so it falls back to a lexical parent and says nothing more.
_resolve_lane_path() {
  local p="$1" cur="/" comp next phys had_f=1 oldifs
  case "$p" in /*) ;; *) printf '%s\n' "$p"; return 0 ;; esac
  case "$-" in *f*) had_f=0 ;; esac      # remember the caller's glob setting, restore it below
  set -f                                  # a lane path may legitimately contain a glob char
  oldifs="$IFS"; IFS='/'; set -- $p; IFS="$oldifs"
  for comp in "$@"; do
    [ -n "$comp" ] || continue
    case "$comp" in
      .) continue ;;
      ..)
        if [ -d "$cur" ]; then
          phys="$( cd -P "$cur/.." 2>/dev/null && pwd -P )" || phys=""
          if [ -n "$phys" ]; then cur="$phys"; continue; fi
        fi
        cur="${cur%/*}"; [ -n "$cur" ] || cur="/"
        ;;
      *)
        case "$cur" in */) next="${cur}${comp}" ;; *) next="${cur}/${comp}" ;; esac
        if [ -d "$next" ]; then
          phys="$( cd -P "$next" 2>/dev/null && pwd -P )" || phys=""
          if [ -n "$phys" ]; then cur="$phys"; else cur="$next"; fi
        else
          cur="$next"
        fi
        ;;
    esac
  done
  [ "$had_f" -eq 0 ] || set +f
  printf '%s\n' "$cur"
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
  # DEGRADED IDENTITY IS THE SAME DEFECT AS AN EPHEMERAL ONE, AND THIS CHECKED ONLY THE
  # LATTER (#3436, roborev round 4). The rule is "never WRITE a record that cannot be
  # re-identified" — and a record whose boot-id or start-ticks could not be captured is
  # exactly that: it evaluates `UNKNOWN-*` forever, refuses every later acquire including
  # its own holder's, and is clearable only by `release --force` / `reclaim`. Rejecting
  # `ephemeral` alone left two live paths to it: an explicit `--pid` on a host without
  # `/proc` (deterministic), and a process exiting between argument validation and
  # identity capture (a race). An earlier round ACCEPTED this as a residual, which was
  # inconsistent with the principle adopted one fix later; this closes it.
  if [ -z "${G_BOOT:-}" ] || [ -z "${G_TICKS:-}" ]; then
    emit "ERROR reason=unresolved-identity detail=degraded-process-identity sub=$sub issue=$G_ISSUE pid=$G_PID pid-scope=$G_SCOPE boot-id=${G_BOOT:-<unreadable>} start-ticks=${G_TICKS:-<unreadable>} (NOTHING WAS WRITTEN. A lock records boot-id + /proc/<pid>/stat start-ticks so the holder can be re-identified exactly; neither could be read for this pid, so any record written now would evaluate UNKNOWN-* forever and refuse every later acquire — including yours. Causes: a host without /proc (this lock is Linux-specific), or a pid that exited between validation and capture. Remedies: name a live durable process with '--pid <pid>' on a /proc host, or do not take a lane lock on this platform — the cross-machine control is refs/claims/issue-<N> via claim.sh, which needs no /proc) lane-dir=$G_LANE"
    return 1
  fi
  [ "$G_SCOPE" = "ephemeral" ] || return 0
  emit "ERROR reason=unresolved-identity detail=no-durable-session-process sub=$sub issue=$G_ISSUE pid-scope=$G_SCOPE candidate-pid=$G_PID (NOTHING WAS WRITTEN. This lock records the HOLDER's full process identity, and the only process it could find here is THIS invocation's own shell, which exits when this command returns. A record naming it reads UNKNOWN-EPHEMERAL forever, and every UNKNOWN-* refuses — including YOUR OWN later acquire — so writing it would BRICK the lane rather than lock it. Two corrections, both one line: (1) run this from a shell whose cwd is INSIDE the lane directory, which is what makes the long-lived session process findable — 'cd <lane-dir> && lane-lock.sh $sub $G_ISSUE'; or (2) name the durable process explicitly with '--pid <pid>'. If you are at worktree-CREATION time, do not acquire at all: the session is not in the lane yet, so no durable owner exists — the lock belongs to the session that works IN the lane) lane-dir=$G_LANE"
  return 1
}

# identity_state — did THIS run establish its OWN durable identity? Three-valued, and
# the value is PUBLISHED (probe's `our-identity=`) because a consumer cannot tell SELF
# from a live PEER without it: when our identity is unresolved, our token cannot match
# ANY live holder's, so the record reads ALIVE whether or not the holder is us. Asserting
# "someone else" from that is asserting a positive from the FAILURE to prove its
# opposite, which is the rule this repo keeps relearning (#3436 FIX 7).
#
# A DEGRADED identity is UNRESOLVED even when the pid was given explicitly: without
# boot-id AND start-ticks the token carries '-' placeholders and cannot match a live
# holder's token either, so `explicit` alone is not evidence of anything.
identity_state() {
  if [ -z "$G_BOOT" ] || [ -z "$G_TICKS" ]; then printf 'UNRESOLVED'; return 0; fi
  case "$G_SCOPE" in
    cwd-match) printf 'cwd-match' ;;
    explicit) printf 'explicit' ;;
    *)        printf 'UNRESOLVED' ;;
  esac
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
    liveness="$(record_liveness)"
    prev_token="$(record_token)"
    case "$liveness" in
      SELF)
        # RE-ENTRANCY REQUIRES ALL FIVE TOKEN COMPONENTS (#3436). A same-machine,
        # same-actor acquire from a DIFFERENT live pid must NOT land here — it is the
        # OCCUPIED case below. claim.sh's machine+actor re-entrancy is precisely why
        # the claim ref could not stop #3436's second local session, so this must
        # NEVER be relaxed to a machine+actor comparison.
        # AND THE DIRECTORY MUST MATCH (#3436, roborev round 9). Without this a process
        # holding issue N for directory X gets ACQUIRED (re-entrant) for directory Y
        # while the record still protects X only — so Y is advertised as locked and is
        # not, which is the same alias the directory mutex closes for two issues,
        # reopened for one issue naming two directories.
        if [ "${REC_LANE_DIR:-}" != "$G_LANE" ]; then
          emit "OCCUPIED issue=$G_ISSUE lane-dir=$G_LANE reason=reentrant-lane-dir-mismatch recorded-lane-dir=${REC_LANE_DIR:-<none>} (this issue's lock is held by THIS process for a DIFFERENT directory. The record protects the recorded directory only, so returning re-entrant here would advertise protection this lock does not provide. Release it under this issue first, or take the other directory under its own issue number.)"
          return 2
        fi
        emit "ACQUIRED (re-entrant) issue=$G_ISSUE $(self_fields) record=$G_RECORD lane-dir=$G_LANE"
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
        emit "ACQUIRED (reclaimed) issue=$G_ISSUE $(self_fields) prev-liveness=$liveness prev-token=$prev_token record=$G_RECORD lane-dir=$G_LANE"
        return 0
        ;;
      *)
        # ALIVE and every UNKNOWN-* REFUSE. AC2: the refusal NAMES the occupant.
        emit "OCCUPIED issue=$G_ISSUE liveness=$liveness $(holder_fields) our-token=$G_TOKEN lane-dir=$G_LANE"
        return 2
        ;;
    esac
  fi
  if ! write_record "$G_RECORD" "$G_ISSUE" "$G_LANE" "$G_MACHINE" "$G_ACTOR" "$G_PID" "$G_SCOPE" "$G_BOOT" "$G_TICKS"; then
    emit_infra "detail=record-write-failed path=$G_RECORD"
    return 1
  fi
  append_audit "$G_LOG" "verdict=ACQUIRED issue=$G_ISSUE token=$G_TOKEN prev-token=<none> prev-liveness=<none> reason=free-lane"
  emit "ACQUIRED issue=$G_ISSUE $(self_fields) record=$G_RECORD lane-dir=$G_LANE"
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
  # BOTH mutexes: directory first (so two issues on one directory contend), then per-issue.
  _acquire_under_dir_lock() {
    local conflict
    if conflict="$(same_dir_other_issue "$G_ISSUE" "$G_LANE")"; then
      emit "OCCUPIED issue=$G_ISSUE lane-dir=$G_LANE reason=same-lane-dir-other-issue conflicting-issue=${conflict%%|*} conflicting-pid=$(printf '%s' "$conflict" | cut -d'|' -f2) conflicting-acquired-ts=$(printf '%s' "$conflict" | cut -d'|' -f3) (that DIRECTORY is already locked under a DIFFERENT issue by a live holder. The record is keyed by issue so readers need no path, but the DIRECTORY is what must not have two writers — so this refuses. If the other issue number is the mistake, fix the caller; if that lane is genuinely finished, release or reclaim it under ITS issue number.)"
      return 2
    fi
    with_lock 9 "$G_MUTEX" _acquire_locked
  }
  with_lock 8 "$(dir_mutex "$G_LANE")" _acquire_under_dir_lock
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
  # Same authority rule as `probe` (#3436 FIX 6): if a record exists, ITS lane-dir is the
  # subject — that is the lane the holder was in, so it is the only correct input to the
  # identity walk. Without this, `verify` from a session whose lane is
  # `.claude/worktrees/issue-<N>-<slug>` walked ancestors against
  # `${LANE_ROOT}/lane-<N>` and could never match its own lock.
  # ONE SNAPSHOT, NOT TWO (#3436, roborev round 11). This used to parse the record here to
  # choose the lane and then parse it AGAIN after prepare_identity for the verdict. Both reads
  # hit the SAME path — G_RECORD is lock_record(issue), a pure function of issue and lock root,
  # independent of the lane — so the second was a pure re-read and a TOCTOU window: a concurrent
  # release/reacquire between them yields a verdict whose LANE came from record A and whose
  # HOLDER came from record B, and a record that VANISHED between them leaves the REC_* fields
  # cleared while the code carries on to a confident answer. prepare_identity writes only G_*,
  # so the single parse below survives it.
  local have_rec=0
  parse_record "$(lock_record "$issue")" && have_rec=1
  if [ "$have_rec" -eq 1 ] && [ -n "${REC_LANE_DIR:-}" ]; then
    lane="$REC_LANE_DIR"
  else
    lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  fi
  prepare_identity "$issue" "$lane" "$actor" "$pid_opt"   # sets G_RECORD from the lock root
  if [ "$have_rec" -eq 0 ]; then
    emit "VERIFY-FAIL issue=$issue reason=no-lock $(self_fields) lane-dir=$lane"
    return 2
  fi
  liveness="$(record_liveness)"
  if [ "$liveness" = "SELF" ]; then
    emit "VERIFY-OK issue=$issue $(self_fields) acquired-ts=${REC_ACQUIRED_TS:-<none>} lane-dir=$lane"
    return 0
  fi
  emit "VERIFY-FAIL issue=$issue reason=not-holder liveness=$liveness $(holder_fields) our-token=$G_TOKEN lane-dir=$lane"
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
  local record mismatch=""
  record="$(lock_record "$issue")"
  if ! parse_record "$record"; then
    lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
    emit "FREE issue=$issue liveness=NO-RECORD record=absent lane-dir=$lane"
    return 0
  fi
  # THE RECORD'S OWN `lane-dir` IS AUTHORITATIVE (#3436 FIX 6). The record is found by
  # ISSUE, so a probe needs no lane path — and re-deriving `${LANE_ROOT}/lane-<N>` made
  # every reader describe a directory nobody was working in, because this repo's
  # sanctioned worktrees are `.claude/worktrees/issue-<N>-<slug>` and
  # `~/projects/cqlite-wt/issue-<N>`. The recorded value is also the lane the HOLDER was
  # in, so it is the right subject for the identity walk below, not just for display.
  # A caller-supplied --lane-dir that DISAGREES is reported as information
  # (`lane-dir-mismatch=`) and never silently preferred, and it never changes the
  # liveness verdict.
  if [ -n "${REC_LANE_DIR:-}" ]; then
    lane="$REC_LANE_DIR"
    if [ -n "$lane_opt" ] && [ "$lane_opt" != "$REC_LANE_DIR" ]; then
      mismatch="$lane_opt"
    fi
  else
    lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
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
  # DELIBERATELY NOT RE-READ (#3436, roborev round 11). This line used to be
  # `parse_record "$record" || true`, justified as keeping the parse adjacent to the
  # comparison — but adjacency in the SOURCE bought nothing and cost correctness in TIME: a
  # concurrent release/reacquire between the first parse and this one produced a verdict whose
  # LANE came from the earlier record and whose HOLDER came from the later one, and the
  # `|| true` meant a record that had VANISHED cleared every REC_* field and still reached
  # `record_liveness`, so probe could report HELD or UNKNOWN-FOREIGN about a lock that no longer
  # existed. The first parse is the snapshot; prepare_identity writes only G_*, so it survives.
  liveness="$(record_liveness)"
  case "$liveness" in DEAD-*) reclaimable=yes ;; esac
  # our-pid / our-start-ticks are published ALONGSIDE our-token so a consumer can compare
  # PROCESS identity WITHOUT the actor in it (#3436, roborev round 2). The token carries
  # machine:actor:pid:boot:ticks, so a token difference can mean "a different process" OR
  # merely "the same process under a different actor" — and claim.sh was reading the
  # second as the first, telling a session a peer held a lane its OWN process held. The
  # actor belongs in holder identity for verify/release; it does not belong in the test
  # for "is this a different process". Publishing the fields separately keeps the
  # comparison structural rather than making a caller split the token (#3312: do not
  # reintroduce a delimiter to parse).
  emit "HELD issue=$issue liveness=$liveness reclaimable=$reclaimable $(holder_fields) our-token=$G_TOKEN our-machine=$G_MACHINE our-pid=$G_PID our-start-ticks=${G_TICKS:-<none>} our-identity=$(identity_state) record=$record${mismatch:+ lane-dir-mismatch=$mismatch} lane-dir=$lane"
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
    emit "RELEASED (already free) issue=$G_ISSUE record=absent lane-dir=$G_LANE"
    return 0
  fi
  prev_token="$(record_token)"
  liveness="$(record_liveness)"
  if [ "$force" != "1" ] && [ "$liveness" != "SELF" ]; then
    emit "RELEASE-REFUSED issue=$G_ISSUE reason=not-holder liveness=$liveness $(holder_fields) our-token=$G_TOKEN lane-dir=$G_LANE"
    return 2
  fi
  if ! rm -f "$G_RECORD"; then
    emit_infra "detail=record-unlink-failed path=$G_RECORD"
    return 1
  fi
  append_audit "$G_LOG" "verdict=RELEASED issue=$G_ISSUE token=$G_TOKEN prev-token=$prev_token prev-liveness=$liveness reason=$([ "$force" = 1 ] && echo forced-release || echo holder-release)"
  if [ "$force" = "1" ] && [ "$liveness" != "SELF" ]; then
    emit "RELEASED issue=$G_ISSUE mode=forced prev-token=$prev_token prev-liveness=$liveness lane-dir=$G_LANE"
  else
    emit "RELEASED issue=$G_ISSUE $(self_fields) lane-dir=$G_LANE"
  fi
  return 0
}

cmd_release() {
  local issue="" lane_opt="" actor="${LANE_LOCK_ACTOR:-flow}" force=0 lane pid_opt=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --lane-dir) [ "$#" -ge 2 ] || die_usage "--lane-dir requires a value"; require_abs_path --lane-dir "$2"; lane_opt="$2"; shift 2 ;;
      --actor)    [ "$#" -ge 2 ] || die_usage "--actor requires a value";    actor="$2";    shift 2 ;;
      --force)    force=1; shift ;;
      # --pid: name the holder explicitly. WHY release needs it (#3436, roborev round 2):
      # the holder gate is our exact token, and the auto-resolved pid comes from the cwd
      # walk — so a holder that has MOVED CWD (or whose lane directory has been removed,
      # which is exactly what finalize does) can no longer resolve the identity it locked
      # with, and its own release is refused as not-holder. Reading the record fixes the
      # SUBJECT but cannot fix the IDENTITY. Three sanctioned ways to release, in order of
      # preference: from inside the lane; with --pid naming the durable holder; or --force
      # (the reaper path, which needs no identity at all).
      --pid)      [ "$#" -ge 2 ] || die_usage "--pid requires a value"; pid_opt="$2"; shift 2 ;;
      -*) die_usage "release: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "release: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" release
  # SAME AUTHORITY RULE AS verify/probe (#3436 FIX 6, extended to release on roborev
  # round 2). `release` used to re-derive `${LANE_ROOT}/lane-<N>`, and FIX 6 fixed its
  # two siblings while missing it — so a lock taken for a non-default worktree could not
  # be released without repeating `--lane-dir`, and once that worktree was REMOVED the
  # cwd identity walk had nothing to match, leaving a live stale lock nobody could clear
  # through the normal path. The record is keyed by ISSUE, so its own `lane-dir` is
  # always available and is the only correct subject.
  if parse_record "$(lock_record "$issue")" && [ -n "${REC_LANE_DIR:-}" ]; then
    lane="$REC_LANE_DIR"
  else
    lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  fi
  # No lock root means no record anywhere, and release is IDEMPOTENT — answer without
  # creating the root (a release must not have to make a directory to say "already
  # free"). The check moved from the lane dir to the lock root with the files.
  if [ ! -d "$(lock_root)" ]; then
    emit "RELEASED (already free) issue=$issue record=absent lane-dir=$lane"
    return 0
  fi
  # --force IS THE BREAK-GLASS AND MUST NOT NEED AN IDENTITY (#3436, roborev round 5).
  # `prepare_identity` resolves and VALIDATES actor/pid, and an explicit-or-inherited
  # LANE_LOCK_PID naming a process that is gone is a usage error (exit 64) — so a stale
  # env var defeated the one path documented as unconditional. That documentation is the
  # answer to "how does a stale instance get cleared, and by whom?", so a --force that can
  # refuse makes the answer FALSE, which is worse than the refusal itself. Forced release
  # therefore initialises only the PATHS it needs and skips identity entirely: it deletes
  # the record without comparing holders, which is exactly what a reaper does.
  if [ "$force" -eq 1 ]; then
    G_ISSUE="$issue"; G_LANE="$lane"
    G_RECORD="$(lock_record "$issue")"; G_MUTEX="$(lock_mutex "$issue")"
    G_LOG="$(lock_audit "$issue")"
    # NO IDENTITY RESOLUTION OF ANY KIND HERE — not the pid, and NOT THE ACTOR.
    # roborev round 6 found `resolve_actor` still being called inside this very bypass:
    # it VALIDATES the actor and dies (exit 64) on an unrecordable one, so an invalid
    # INHERITED LANE_LOCK_ACTOR disabled the break-glass exactly as an inherited
    # LANE_LOCK_PID did one round earlier. That was the fourth instance of one shape in
    # this change — fix the site named, miss its sibling — and here the sibling was two
    # lines inside my own fix. So every field is a FIXED PLACEHOLDER, and the whole point
    # is that NOTHING on this path can refuse:
    #   * no resolve_actor  (validates -> can die 64)
    #   * no resolve_pid    (validates liveness -> can die 64)
    #   * no boot-id/start-ticks capture (unreadable -> would refuse)
    # `this_machine` is safe (a plain hostname read with a fallback) but is not needed
    # either: a forced release compares nothing, so recording who forced it belongs in the
    # AUDIT LINE, not in an identity field that only exists to be compared.
    G_TOKEN="<forced-no-identity>"; G_MACHINE="<forced>"
    G_ACTOR="<forced>"; G_PID="<none>"; G_SCOPE="forced"
  else
    prepare_identity "$issue" "$lane" "$actor" "$pid_opt"
  fi
  with_lock 9 "$G_MUTEX" _release_locked "$force"
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
  local expect="$1" reason="$2" liveness prev_token="" prev_lease=""
  local have=0
  if parse_record "$G_RECORD"; then have=1; prev_token="$(record_token)"; prev_lease="$(record_lease)"; fi
  if [ "$have" -eq 1 ]; then
    liveness="$(record_liveness)"
  else
    liveness="NO-RECORD"
  fi

  # RE-ENTRANT RECLAIM (mirrors `claim.sh adopt`): the record may already hold OUR
  # exact token. Reporting RECLAIM-LOST there would make a session abandon a lane it
  # demonstrably owns while still holding it. But a VIOLATED compare-and-swap must
  # never be reported as a satisfied one, so when --expect names something the record
  # does NOT hold, the verdict names BOTH values.
  if [ "$liveness" = "SELF" ]; then
    # THE DIRECTORY MUST MATCH HERE TOO (#3436, roborev round 10). Round 9 added exactly this
    # check to the re-entrant ACQUIRE path and left its sibling here: the reported line was
    # patched and the CLASS was not swept. Without it a holder reclaims its own lease naming
    # --lane-dir Y while the record still protects X — the command reports success for Y,
    # rewrites nothing, and another issue can then acquire Y against a lock that never covered it.
    if [ "${REC_LANE_DIR:-}" != "$G_LANE" ]; then
      emit "OCCUPIED issue=$G_ISSUE lane-dir=$G_LANE reason=reentrant-lane-dir-mismatch recorded-lane-dir=${REC_LANE_DIR:-<none>} (this issue's lock is held by THIS process for a DIFFERENT directory. Reclaiming it under this path would report protection the record does not provide. Reclaim under the recorded directory, or take this one under its own issue number.)"
      return 2
    fi
    # THE LEASE, HERE TOO. This branch compared the TOKEN while the CAS branch below
    # compared the lease, so once `--expect` became a lease the re-entrant SUCCESS path
    # was unreachable: every re-entrant reclaim reported a mismatch. Two comparison sites
    # for one precondition is the bug; both now read `prev_lease`.
    if [ "$expect" = "$prev_lease" ]; then
      emit "RECLAIMED (re-entrant) issue=$G_ISSUE $(self_fields) from=$expect lane-dir=$G_LANE"
    else
      # A DISTINCT VERDICT WORD (#3436 FIX 13i): the FIRST token is what a caller matches
      # on, and both this and a satisfied CAS exit 0 — so calling this one `RECLAIMED` made
      # a VIOLATED --expect invisible to any consumer that does not read prose. The exit
      # code stays 0 on purpose: we demonstrably DO hold the lane, and reporting
      # RECLAIM-LOST would make a session abandon a lane it owns.
      emit "RECLAIM-LEASE-MISMATCH (re-entrant: we DO hold the lane, but the compare-and-swap precondition did NOT hold) issue=$G_ISSUE expected=$expect actual=$prev_lease $(self_fields) lane-dir=$G_LANE"
    fi
    return 0
  fi

  if [ "$expect" = "none" ]; then
    if [ "$have" -eq 1 ]; then
      emit "RECLAIM-LOST issue=$G_ISSUE expected=none actual=$prev_token liveness=$liveness $(holder_fields) lane-dir=$G_LANE"
      return 2
    fi
  else
    if [ "$have" -eq 0 ]; then
      emit "RECLAIM-LOST issue=$G_ISSUE expected=$expect actual=<none> liveness=$liveness lane-dir=$G_LANE"
      return 2
    fi
    # THE LEASE IS THE RECORD INCARNATION, NOT THE PROCESS TOKEN (#3436, roborev round 3).
    # Comparing the token was an ABA hole: machine:actor:pid:boot:ticks is UNCHANGED when
    # the same process releases and re-acquires, so a reclaimer holding a token observed
    # before that cycle matched a record written after it, and overwrote a NEWLY ACQUIRED
    # LIVE lock — two writers, which is the CAS guarantee inverted. claim.sh does not have
    # this hole because git arbitrates on a per-claim commit SHA, unique per acquisition;
    # our equivalent is the per-record `nonce`, which was generated and recorded from the
    # start and simply never reached the lease. `record_lease` binds both, so a lease names
    # one acquisition rather than one process.
    if [ "$prev_lease" != "$expect" ]; then
      emit "RECLAIM-LOST issue=$G_ISSUE expected=$expect actual=$prev_lease liveness=$liveness $(holder_fields) lane-dir=$G_LANE"
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
  emit "RECLAIMED issue=$G_ISSUE $(self_fields) from=$expect prev-token=${prev_token:-<none>} prev-liveness=$liveness reason=$reason lane-dir=$G_LANE"
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
  [ "$expect_given" -eq 1 ] || die_usage "reclaim requires --expect <lease> (CAS against the CURRENT record; take the value from probe/status's lease= field, which is <token>#<nonce> — NOT the holder token, because a token is unchanged by a same-process release+reacquire) or --expect none (the record must NOT exist)"
  [ -n "$expect" ] || die_usage "reclaim: --expect '' is rejected on purpose — pass the holder token you expect, or the literal 'none'"
  [ "$reason_given" -eq 1 ] || die_usage "reclaim requires --reason saying what the reclaim IS (it is recorded in the record and the audit log next to who took it), e.g. --reason lane-holder-oom-killed-pid-4211"
  reason_token="$(validate_reason "$reason")"
  # THE RECORD'S lane-dir IS AUTHORITATIVE HERE TOO (#3436, roborev round 14). FIX 6 made every
  # READER honour it and round 11 rebuilt verify/probe around a single snapshot; release already
  # did. reclaim was the one path left deriving the DEFAULT `$LANE_ROOT/lane-<N>`, so reclaiming
  # a lock taken under a sanctioned custom worktree (`.claude/worktrees/issue-<N>-<slug>`) walked
  # ancestors against a directory nobody was in, could not resolve an identity, and refused the
  # BREAK-GLASS on exactly the lanes that need it most.
  #
  # Swept rather than patched this time: of the six sites deriving a lane, acquire correctly uses
  # the REQUESTED directory (it is creating the lock, and the re-entrant mismatch check covers the
  # disagreement), verify/probe/release already ask the record, and this was the only one left.
  local have_rec=0
  parse_record "$(lock_record "$issue")" && have_rec=1
  if [ "$have_rec" -eq 1 ] && [ -n "${REC_LANE_DIR:-}" ] && [ -z "$lane_opt" ]; then
    lane="$REC_LANE_DIR"
  else
    lane="$(lane_real "$(resolve_lane_dir "$issue" "$lane_opt")")"
  fi
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
  # RECLAIM WRITES A RECORD, SO IT NEEDS THE DIRECTORY MUTEX AND THE CROSS-ISSUE CHECK
  # TOO (#3436, roborev round 9). Taking only the per-issue mutex let a STALE record for
  # issue A be reclaimed AFTER issue B had legitimately acquired that directory — two
  # live holders, deterministically, with no race required. Any path that mints a record
  # must answer the same directory question acquire answers.
  _reclaim_under_dir_lock() {
    local conflict
    if conflict="$(same_dir_other_issue "$G_ISSUE" "$G_LANE")"; then
      emit "OCCUPIED issue=$G_ISSUE lane-dir=$G_LANE reason=same-lane-dir-other-issue conflicting-issue=${conflict%%|*} conflicting-pid=$(printf '%s' "$conflict" | cut -d'|' -f2) conflicting-acquired-ts=$(printf '%s' "$conflict" | cut -d'|' -f3) (that DIRECTORY is held under a DIFFERENT issue by a live holder, so reclaiming this issue's stale record would create a SECOND writer of one directory. Reclaim or release the conflicting issue instead.)"
      return 2
    fi
    with_lock 9 "$G_MUTEX" _reclaim_locked "$expect" "$reason_token"
  }
  with_lock 8 "$(dir_mutex "$G_LANE")" _reclaim_under_dir_lock
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
    emit "FREE issue=$issue_hint liveness=NO-RECORD record=absent lane-dir=$lane_hint"
    return 0
  fi
  # No identity is resolved in `status`, so SELF is unreachable by construction.
  liveness="$(record_liveness)"
  case "$liveness" in DEAD-*) reclaimable=yes ;; esac
  emit "HELD issue=${REC_ISSUE:-$issue_hint} liveness=$liveness reclaimable=$reclaimable $(holder_fields) record=$record lane-dir=${REC_LANE_DIR:-$lane_hint}"
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
    emit "STATUS lock-root=$lock lane-root=$root locks=0 detail=lock-root-absent scope=lock-takers-only (0 means NO LANE RECORDED A LOCK — it does NOT mean no lane is occupied. This tool can only report occupants that CALLED acquire; a session working a lane without taking the lock is invisible to it, which is why claim.sh reports the lane-lock state on every grant. Not a clean bill of health.)"
    return 0
  fi
  for f in "$lock"/lane-*.lock; do
    [ -f "$f" ] || continue
    base="${f##*/}"; base="${base%.lock}"
    status_one "$f" "$root/$base" "${base#lane-}"
    count=$((count + 1))
  done
  # Same disclosure on the non-empty render, and for the same reason: a count of RECORDED
  # locks is not a census of OCCUPIED lanes. Emitted rather than left in the header, because
  # a caveat that lives only where the caveat-hunter looks is not a disclosure to the person
  # who needs it (#3436; the shape lane-3500 found in a census line reading only
  # "not compared").
  if [ "$count" -eq 0 ]; then
    emit "STATUS lock-root=$lock lane-root=$root locks=0 scope=lock-takers-only (0 means NO LANE RECORDED A LOCK — NOT that no lane is occupied. Only occupants that CALLED acquire are visible here. Not a clean bill of health.)"
  else
    emit "STATUS lock-root=$lock lane-root=$root locks=$count scope=lock-takers-only (counts RECORDED locks, not occupied lanes; a session that never called acquire is invisible)"
  fi
  return 0
}

# ---------------------------------------------------------------------------
SUBCOMMAND="${1:-}"
[ "$#" -eq 0 ] || shift
# LANE_ROOT is validated HERE, in the main shell, before any subcommand runs — a
# relative value is a lock BYPASS (two cwds, two lock roots, both acquire), so it must
# fail before anything reads a path. `--help` is exempt: printing usage must work on a
# misconfigured box, and it touches no path.
case "$SUBCOMMAND" in
  -h | --help | help) : ;;
  *) require_lane_root_abs ;;
esac
case "$SUBCOMMAND" in
  -h | --help | help) print_help ;;
  acquire) cmd_acquire "$@" ;;
  verify)  cmd_verify "$@" ;;
  probe)   cmd_probe "$@" ;;
  release) cmd_release "$@" ;;
  reclaim) cmd_reclaim "$@" ;;
  status)  cmd_status "$@" ;;
  "") die_usage "a subcommand is required: acquire <N> | verify <N> | probe <N> [--pid <pid>] | release <N> [--force] | reclaim <N> --expect <lease>|none --reason <why> | status [<N>]" ;;
  *)  die_usage "unknown subcommand: $SUBCOMMAND (expected acquire|verify|probe|release|reclaim|status)" ;;
esac
