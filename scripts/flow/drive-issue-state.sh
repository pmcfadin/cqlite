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
#     A recorded session id equal to the current one (and recordable) is OWNED outright
#     (basis `session`). So is a recorded pid that is ALIVE and EQUAL to our own session pid
#     with an intersecting start window (basis `same-live-pid`) — a pid is unique among live
#     processes, so that process IS us, and without this a session with CLAUDE_PID set but no
#     CLAUDE_CODE_SESSION_ID would refuse its OWN marker as a live peer. It is an affirmative
#     measurement of sameness (stronger than the id string it stands in for), never a
#     permissive fallback. THE TWO BASES ARE CARRIED, NOT COLLAPSED: `verify`/`write` accept
#     both, and `adopt` treats both as ALREADY YOURS (a re-entrant no-op) rather than as
#     something to hand over.
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
#       consumer parses it. THAT COVERS THE EXTERNAL COMMANDS THIS SCRIPT RUNS: every one of
#       them (mktemp, mv, rm, cat, tail, awk, date, wc, tr, hostname, flock, and the sourced liveness
#       library's ps/date/tr/cut) has its stderr either CAPTURED and FOLDED into the anchored
#       message — preferred where the native text names the cause, as with mktemp and mv — or
#       SUPPRESSED where the anchored message already names the failure fully. A native
#       `mktemp: failed to create file via template ...` line carries no prefix, and the
#       failure itself is ALREADY reported through the anchored WRITE_ERR/refuse path, so the
#       native line is pure contract breakage. Cleanup commands additionally carry `|| true`:
#       they run AFTER the verdict, and a failing command in a bash EXIT trap under `set -e`
#       aborts the trap AND replaces the exit status.
#   (b) Every dynamic field is CONTROL-CHARACTER SANITIZED for display. Load-bearing: git
#       and the filesystem PERMIT NEWLINES IN PATHS, and an unsanitized path printed
#       verbatim emits a second line carrying no prefix, breaking the anchor everything
#       else rests on.
#   (c) The verdict appears ONLY on a `DRIVE-STATE: verdict ` line and is a single token
#       from the CLOSED set below; prose goes on `verdict-detail` lines. An unrecognised
#       token is not a thing this script can print — the set is the grammar. EVERY exit
#       carries one, INCLUDING a fatal start-up failure: callers branch on the TOKEN, so a
#       prefixed line with no token is unreadable by every one of them and its `case` falls
#       through — (a) does not imply (c), and only (c) is what a consumer reads. That includes a
#       USAGE error and a SIGNAL: an EXIT-trap backstop emits `verdict ERROR` for any path that
#       would otherwise leave with none, and the INT/TERM/HUP handlers emit exactly one token
#       chosen by COMMIT_PHASE — ERROR before the atomic rename, the run's own success token
#       after it, and a DEFERRED delivery across it. THE EMISSION ITSELF IS A DEFERRAL WINDOW
#       TOO: the line is printed BEFORE the "already emitted" state is committed, and a signal
#       landing between the two is HELD rather than delivered — otherwise both readers see
#       "already emitted" for a line that does not exist yet and the run exits with NO token.
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
#   ERROR               an I/O or internal failure, or an identity axis that could not be
#                       MEASURED (axis=machine / axis=worktree) or could not be recorded
#                       LOSSLESSLY (axis=machine / axis=session) — nothing was decided
#   USAGE               the invocation itself was wrong — nothing was read and nothing written
#
# SUBCOMMANDS
#   write  <N> [--stage <s>] [--request-id <r>] [--pr <n>] [--branch <b>]
#              [--body-file <path>] [--actor <id>]
#              A --body-file is READ EXACTLY ONCE, into a private snapshot, and everything
#              downstream validates and copies THOSE bytes; a read failure is `verdict ERROR`,
#              never a silently EMPTY body.
#              Write/replace the marker in the CURRENT worktree. Writing OVER an existing
#              marker first passes the SAME ownership verification `verify` applies — you
#              may not overwrite a live peer's plan either — so a foreign marker refuses
#              with that marker's own verdict. The route past it is `adopt`, never a flag
#              on `write`. Without --body-file an OWNED marker's body is PRESERVED
#              BYTE-FOR-BYTE, across any number of writes and across an `adopt`: the blank
#              separator between the prologue and the body is the WRITER's, and the reader
#              skips it, so the body neither grows nor shrinks.
#              ONE EXCEPTION — the MIGRATION case: an UNSTAMPED marker (the pre-#3822
#              shape) asserts NO ownership, so `write` REPLACES it, DISCARDS its body (an
#              unstamped plan may belong to any session and is never carried forward) and
#              ANNOUNCES the discard on a verdict-detail line. `verify` still REFUSES an
#              unstamped marker; `write` is the one door, and it is the door the UNSTAMPED
#              refusal names. MALFORMED / DUPLICATE-SENTINEL get no exception: they CLAIM
#              an identity that cannot be READ, which may be a live peer's, so a human
#              moves the file aside deliberately and `write` then takes the ABSENT path.
#              NEITHER DOES A SYMLINK AT THE MARKER PATH — dangling or not. It is classified
#              `not-regular` and refused (`verdict ERROR`), never followed and never replaced:
#              a link is an artifact someone placed deliberately, and `mv` would silently
#              destroy it. A DANGLING one used to classify `absent` (`-e` follows the link),
#              which took the fresh-start path and clobbered it (roborev job 43 K1).
#              An OWNED marker's recorded `stage`/`request-id`/`pr`/`branch` are PRESERVED
#              unless this call overrides them; erasing one is the explicit
#              `--clear stage|request-id|pr|branch` (repeatable). Omitting a flag never
#              erases durable state — dropping the open request id would make the next
#              session re-ask, breaking "one marker, one wait".
#              UNRECOGNISED PROLOGUE KEYS ARE PRESERVED VERBATIM, in order, across every
#              rewrite (`write` and `adopt` alike). The parser accepts a key it does not know
#              FOR FORWARD COMPATIBILITY, and an acceptance whose rewrite path silently DROPPED
#              the field made an OLDER copy of this script DELETE a field a NEWER one had
#              introduced — the same durable-state erasure as a dropped `request-id`. Preserving
#              is chosen over REFUSING to mutate such a marker because refusal would make a
#              newer marker unusable by an older script, bricking every touched lane on a fleet
#              mid-rollout. They are written back byte-for-byte and are never re-sanitized.
#              THE ADOPTION PROVENANCE IS DURABLE STATE TOO and is preserved the same way:
#              `prior-session`, `prior-session-pid`, `prior-ts`, `adopt-reason`. There is no
#              flag and no `--clear` for those four — only a LATER `adopt` replaces them (the
#              newest hand-over is the record), and a fresh write over an ABSENT marker, or
#              the UNSTAMPED migration, INVENTS none. A mandatory, validated `--reason` that
#              the next stage update erases is no audit record at all.
#   verify <N> [--actor <id>]      the rehydrate gate (see the axes above)
#   adopt  <N> --reason <why> [--actor <id>]
#              The explicit adopt gesture. Resolves the SESSION axis ONLY: a fail-closed
#              axis mismatch still refuses, and so does a live or unmeasurable writer.
#              IT REWRITES ONLY WHEN THE RECORDED WRITER IS PROVABLY GONE. Where the marker
#              is ALREADY YOURS — by either basis, the recorded session id being this one OR
#              the recorded pid being THIS live process — it is a re-entrant NO-OP: it says so
#              and changes NOTHING, because rewriting there would record a STILL-RUNNING
#              process as the prior owner and print that it was "provably gone", a false
#              statement in the audit record. The basis comes from the ownership verification
#              itself, never re-derived from the fields by the caller.
#              It RECORDS the provenance of the hand-over — `prior-session`,
#              `prior-session-pid`, `prior-ts`, `adopt-reason` — which then survives later
#              `write`s and is REPLACED (never accumulated) by a later adopt.
#              The body and the durable fields (stage/request-id/pr/branch) SURVIVE: this is
#              THE normal cron-resume path, so dropping them would destroy the open
#              coordination request on every legitimate resume.
#              --reason is REQUIRED and RECORDED. An empty/whitespace reason, one with
#              nothing recordable in it, a bare PLACEHOLDER (`why`, `todo`, `tbd`, …) or one
#              still carrying an UNSUBSTITUTED `<…>` is a USAGE error (64), never a silent
#              `reason=unspecified` — same gate, and same reasons, as claim.sh's.
#   show   <N> print the recorded stamp fields, INCLUDING the adoption provenance when the
#              marker carries it, and any UNRECOGNISED keys this version preserves (on
#              `field unrecognised <key>=<value>` lines) — `show` is the contract's window onto
#              the marker, so it prints every field the reader parses. No ownership verdict.
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
#             IT MUST BE MEASURABLE OR THE RUN REFUSES (`verdict ERROR`, exit 1, detail naming
#             `axis=machine`), on `write`, on `adopt` and on `verify` — the sanitizer maps an
#             empty or unrecordable value onto the `unspecified` PLACEHOLDER, and COMMITTING
#             that placeholder as an identity would (transiently) lock the lane out of its own
#             marker once hostname resolution recovers and (persistently) ALIAS EVERY
#             UNMEASURABLE BOX ONTO ONE OWNER, so lane A's marker would verify as OWNED on
#             machine B. The refusal is at the USE SITE, never in the sanitizer: the shared
#             placeholder is claim.sh's behaviour and stays pinned. Where no marker exists
#             nothing is compared, so `verify`/`adopt` still report ABSENT.
#             IT MUST ALSO BE RECORDABLE **LOSSLESSLY**, and that is the SAME rule one step on
#             (`verdict ERROR`, exit 1, `axis=machine`). The sanitizer maps space -> '-',
#             collapses runs of separators and TRUNCATES at 120 characters, so `build box` and
#             `build-box` record ONE token and two names sharing a 120-character prefix record
#             ONE token — after which two DISTINCT boxes are one owner, which is the same defect
#             as the placeholder wearing different clothes. So the axis is required to ALREADY
#             be what the sanitizer would make of it: recorded != measured is a refusal naming
#             the axis. Again at the USE SITE — the sanitizer is claim.sh's and does not change.
#   worktree  `pwd -P` — the physical directory holding the marker. IT MUST BE MEASURABLE:
#             a failed `pwd -P` (a deleted or unreadable lane directory) is `verdict ERROR`,
#             exit 1, naming `axis=worktree`, on write/verify/adopt AND on `show` — the marker
#             PATH is derived from this axis, so an unmeasurable one would read or write at the
#             filesystem root. Unlike the machine axis it cannot ALIAS (an empty value equals no
#             recorded absolute path), so what this closes is a misattributed diagnostic and a
#             wrong path, not a false OWNED.
#   session   CLAUDE_CODE_SESSION_ID, sanitized. `unrecorded` when unset, which makes the
#             session axis UNMEASURED and routes to the liveness resolution.
#             IT TOO MUST BE RECORDABLE LOSSLESSLY (`verdict ERROR`, exit 1, `axis=session`).
#             The session axis is not fail-closed on a MISMATCH, but an EQUAL session id is
#             OWNED outright, so a lossy one lets two distinct sessions own each other's
#             markers — the machine axis's defect one axis over. `unrecorded` is the ABSENCE of
#             a value, not a lossy one, and is unaffected.
#   actor     --actor, else CLAIM_ACTOR, else `flow` — as claim.sh resolves it. NOTE: the
#             actor is RECORDED but is NOT an ownership axis here; #3810 (claim-actor
#             collision) is a separate, open issue and is not fixed or widened by this file.
#             It is therefore sanitized LOSSILY and DELIBERATELY NOT refused (roborev job 37
#             J1's axis census): nothing compares it, so a collision cannot grant ownership,
#             and refusing `--actor 'flow lead'` would red on input claim.sh accepts — a guard
#             that reds on correct input is the guard agents learn to waive. The same holds for
#             `stage`/`request-id`/`pr`/`branch` and the four `prior-*` provenance fields: they
#             are recorded state, never compared, so lossiness there costs fidelity in the
#             audit record and can never alias one lane onto another.
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
#   1   ERROR — I/O or internal failure, or an unmeasurable machine axis; nothing was decided
#   3   ABSENT — no marker; a legitimate fresh start (textually AND numerically distinct
#       from every refusal, so a caller can tell "nothing to resume" from "refused")
#   4   FOREIGN-ISSUE / FOREIGN-MACHINE / FOREIGN-WORKTREE
#   5   ADOPTABLE — session differs, writer provably gone; run `adopt`
#   6   LIVE-PEER — a live peer owns this lane; do NOT proceed
#   7   LIVENESS-UNKNOWN — could not measure; do NOT proceed
#   8   UNSTAMPED / MALFORMED / DUPLICATE-SENTINEL
#   64  USAGE — usage error
#
# CONSTRAINTS
#   macOS bash 3.2 compatible (no associative arrays, no readarray/mapfile). No network, no
#   gh, no git. `set -euo pipefail`, shellcheck-clean. The MUTATING subcommands additionally
#   require `flock` (util-linux): they refuse without it rather than mutate unserialized, so
#   on a host without flock `write`/`adopt` are unavailable while `verify`/`show` still work.
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"
SCRIPT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

P='DRIVE-STATE:'

MARKER_NAME='.drive-issue-state.md'
STAMP_BEGIN='<!-- drive-issue-state:stamp:v1 -->'
STAMP_END='<!-- drive-issue-state:stamp:end -->'

# How long a mutating subcommand waits for the marker lock. HARD-CODED, no env override
# (the constrained party must not choose its own enforcer); a test that needs a different
# value SUBSTITUTES THE ARTIFACT — it rewrites this line in its own scratch copy of the
# script — which is the repo's rule for exactly this situation.
MARKER_LOCK_WAIT_SECS=30

# Rounding tolerance when intersecting the recorded start window with the live one. Both
# come from the SAME host and the SAME clock (`now - elapsed`, bracketed), so the only
# error to absorb is second-resolution rounding on each side. HARD-CODED with no env
# override on purpose: widening it is exactly what a caller wanting to adopt a live peer's
# lane would do, and the constrained party must not choose its own enforcer.
START_SLACK_SECS=2

# ---------------------------------------------------------------------------
# Output. Contract (a): every line, stdout and stderr, carries the ONE prefix.
# ---------------------------------------------------------------------------
# CONTRACT (c) IS ENFORCED BY A FLAG, NOT BY DISCIPLINE (roborev job 30 G2). Every exit must
# carry exactly ONE `verdict <TOKEN>` line: zero makes a consumer's `case` fall through every
# arm (round 5's F1, at the liveness-library guard), and two lets a consumer read whichever its
# parser picks first. `verdict()` records that it fired, the signal handlers consult the flag so
# they cannot add a second, and the EXIT trap consults it so no path can leave with none.
VERDICT_EMITTED=0
# VERDICT_EMITTING is the CRITICAL-SECTION half of the same contract (roborev job 35 I1): 1 while
# a verdict line is being emitted, so no reader can observe "already emitted" before the line is
# actually on stdout. VERDICT_SIG_PENDING carries a signal that arrived inside that window, held
# until the state is consistent (see verdict/settle_verdict_signal).
VERDICT_EMITTING=0
VERDICT_SIG_PENDING=''
# `--help` is contract (a)'s stated exemption: it emits no verdict line because no consumer
# parses it. It is the ONLY exemption, and it is a flag rather than an inference.
VERDICT_EXEMPT=0

emit()       { printf '%s %s\n' "$P" "$*"; }
note()       { printf '%s note %s\n' "$P" "$*" >&2; }
# A USAGE error is an EXIT, so contract (c) covers it: it carries the `USAGE` token (a member of
# the closed set) as well as the human-readable line. Before job 30 G2 it carried the anchored
# line alone, so `drive-issue-state.sh write <N> --stage` — a plain typo — was unreadable by
# every caller the doctrine tells to branch on the token.
die_usage()  { verdict USAGE; printf '%s USAGE %s\n' "$P" "$*" >&2; exit 64; }

# sane <string> — the string with every C0 control character and DEL replaced by '?'.
# Applied to EVERY dynamic field before it is printed (contract (b)). This is the
# load-bearing half of the anchor: a path or a hand-edited field value may contain a
# NEWLINE, and printed verbatim that emits a second line with no `DRIVE-STATE: ` prefix.
# Control characters ONLY are masked; everything else is printed verbatim, because
# mangling a path for the reader buys nothing once the anchor holds.
# `tr`'s OWN stderr is suppressed: a diagnostic from the tool that exists to PROTECT the
# anchor would be the one line breaking it (roborev job 26 F2).
sane() {
  printf '%s' "${1:-}" 2>/dev/null | LC_ALL=C tr -c '\040-\176\200-\377' '?' 2>/dev/null
}

# verdict <TOKEN> — contract (c): the ONE verdict line, one closed-set token, nothing else.
# It records that it fired so nothing downstream can print a second one, and so the EXIT trap
# can tell "decided" from "left without deciding". NEVER call it inside `$( )`: the flag would
# be set in the subshell and the line captured into a variable (the defect case 17 pins).
#
# THE EMISSION IS A SIGNAL-DEFERRED CRITICAL SECTION (roborev job 35 I1). This was
# `VERDICT_EMITTED=1; printf ...` — the flag committed BEFORE the line was printed — and a signal
# arriving between those two commands produced the one state neither reader can recover from:
# `on_signal` saw "already emitted" and stayed silent, the EXIT-trap backstop saw it and stayed
# silent, and the run exited having printed NO verdict at all, possibly over a COMMITTED write.
# That is contract (c) broken from inside the function installed to enforce it (round 6's G2),
# so the two commands must not be observable in that order:
#   * VERDICT_EMITTING marks the section. Nothing may conclude "already emitted" while it is set.
#   * The line is PRINTED, and only THEN is VERDICT_EMITTED committed. The ordering is asserted
#     structurally by scripts/tests/test_drive_issue_state.sh case 42, because a race cannot be
#     pinned by a timed test; the behavioural half plants the signal AT the window in a scratch
#     copy of this file rather than hoping the scheduler puts it there.
#   * A signal that lands anywhere inside is DEFERRED (recorded, not delivered) and settled the
#     instant the state is consistent — the same shape as the deferral across the atomic rename,
#     and for the same reason: the run reports the outcome it ACHIEVED instead of losing it.
# THE FIRST deferred signal wins: a second one during the same few commands would only overwrite
# an exit code we are already about to deliver, and one exit is all there is to deliver.
verdict() {
  local token="${1:-}"
  VERDICT_EMITTING=1
  printf '%s verdict %s\n' "$P" "$token"
  VERDICT_EMITTED=1
  VERDICT_EMITTING=0
  settle_verdict_signal
}
detail()  { printf '%s verdict-detail %s\n' "$P" "$*"; }

# refuse <TOKEN> <exit-code> <detail...> — emit a named refusal and exit.
refuse() {
  # ARGUMENT-COUNT GUARD BEFORE THE SHIFT (roborev job 30 G3, internal site): a `shift` past the
  # end prints bash's own UNPREFIXED `shift count out of range` under `shift_verbose`/POSIX mode,
  # which breaks contract (a) from inside the function that exists to satisfy contract (c).
  if [ "$#" -lt 2 ]; then
    # ROUTED THROUGH `verdict`, not a hand-rolled flag-then-printf pair (roborev job 35 I1 class
    # sweep): this guard had its own copy of the exact ordering the finding names, and a second
    # emitter is a second place for the window to reappear. `verdict` takes no `shift`, so it is
    # safe to call from the function that exists because a `shift` is not.
    verdict ERROR
    detail "internal: refuse was called with $# argument(s); a token and an exit code are required"
    exit 1
  fi
  local token="$1" code="$2"; shift 2
  verdict "$token"
  [ "$#" -eq 0 ] || detail "$@"
  exit "$code"
}

# THE ONE EXTERNAL CALL WHOSE STDERR IS DELIBERATELY *NOT* SUPPRESSED (roborev job 26 F2).
# `--help` emits no verdict line and no consumer parses it — it is contract (a)'s stated
# exemption — so there is no anchor to protect here, and a HUMAN who asks for the contract and
# gets nothing is better served by awk's own diagnostic than by silence.
print_help() {
  VERDICT_EXEMPT=1
  awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2 {sub(/^# ?/,""); print}' "$0"
}

# TEMP FILES ARE REGISTERED THE MOMENT THEY EXIST, and the trap is installed BEFORE any of
# them is created — otherwise a refusal between `mktemp` and `mv` leaves a stray
# `.drive-issue-state.md.XXXXXX` in the lane, which is exactly the kind of unexplained file
# a later session reads as state. The handlers cover signals too: bash runs no EXIT trap
# for a signal with its default disposition.
# An INDEXED ARRAY, not a space-separated string: `for f in $TMP_FILES` word-SPLITS and
# GLOBS, so a path containing whitespace or a metacharacter would expand into several words
# — and these words are fed to `rm -f`. `TMPDIR` is caller-influenced and shared on this
# fleet, and this very script refuses a newline-bearing worktree path precisely because
# paths here are not tame, so the one destructive command in the file must not depend on the
# shape of a path. (Indexed arrays are bash 3.2; only ASSOCIATIVE arrays need 4.0.)
TMP_FILES=()
cleanup_tmp() {
  local f
  # `${#arr[@]}` is safe under `set -u` for an EMPTY array, whereas `"${arr[@]}"` is not on
  # bash 3.2/4.3 — hence the count guard rather than an unguarded expansion.
  if [ "${#TMP_FILES[@]}" -gt 0 ]; then
    # `2>/dev/null || true`: cleanup is BEST-EFFORT and runs AFTER the verdict, so it may
    # neither emit an unprefixed line nor change the outcome. MEASURED, not assumed — a
    # failing command in a bash EXIT trap under `set -e` aborts the trap and REPLACES the
    # exit status, so a broken `rm` turned a legitimate WRITTEN(0) into an unexplained
    # non-zero (roborev job 26 F2).
    for f in "${TMP_FILES[@]}"; do [ -z "$f" ] || rm -f "$f" 2>/dev/null || true; done
  fi
  TMP_FILES=()
}
# ---------------------------------------------------------------------------
# SIGNAL AND EXIT HANDLING (roborev job 30 G2).
#
# The traps used to be `cleanup_tmp; exit 130|143|129`, which satisfied nothing: no verdict
# token at all, so a consumer branching on the token (drive-issue.md's Delta 4 mandates it) got
# an empty string — the SAME shape as round 5's F1 at the liveness-library guard, one exit path
# over. And they were PHASE-BLIND: a signal arriving after the atomic rename left the marker
# CHANGED while the caller was told nothing whatsoever.
#
# COMMIT_PHASE is set immediately around the one rename that changes durable state:
#   idle       nothing has been committed -> the honest verdict is ERROR, nothing was written
#   committing the rename is in flight -> the signal is DEFERRED across it (below)
#   committed  the rename returned -> the run's own success token is the honest verdict
# COMMIT_VERDICT is the success token the CURRENT mutating subcommand would emit (WRITTEN /
# ADOPTED), set by the subcommand rather than guessed by the handler.
COMMIT_PHASE=idle
COMMIT_VERDICT=''
SIG_PENDING=''

# on_signal <name> <exit-code>
on_signal() {
  local sig="$1" code="$2"
  # THE VERDICT-EMISSION WINDOW COMES FIRST (roborev job 35 I1). While a verdict line is being
  # printed, neither branch below may run: reading VERDICT_EMITTED here is exactly the read that
  # used to observe a flag set before its line existed. Deferred, never dropped — settled by
  # `verdict` itself the moment the state is consistent.
  if [ "$VERDICT_EMITTING" -eq 1 ]; then
    [ -n "$VERDICT_SIG_PENDING" ] || VERDICT_SIG_PENDING="$sig:$code"
    return 0
  fi
  if [ "$COMMIT_PHASE" = committing ]; then
    # DEFERRED, NOT IGNORED. The commit is a single rename; letting it finish costs one syscall
    # and lets the run report the outcome it ACHIEVED instead of an "undetermined". The exit
    # code is still the signal's — settle_pending_signal delivers it once the verdict is out.
    SIG_PENDING="$sig:$code"
    return 0
  fi
  trap '' INT TERM HUP     # no second entry, and therefore no second verdict
  if [ "$VERDICT_EMITTED" -eq 0 ]; then
    if [ "$COMMIT_PHASE" = committed ] && [ -n "$COMMIT_VERDICT" ]; then
      verdict "$COMMIT_VERDICT"
      detail "SIG$sig arrived AFTER the atomic rename COMPLETED: the marker WAS replaced and records what this run assembled. Exiting $code."
    else
      verdict ERROR
      detail "SIG$sig arrived before any atomic rename: NOTHING was written and NOTHING was decided. Exiting $code."
    fi
  fi
  exit "$code"
}
trap 'on_signal INT 130'  INT
trap 'on_signal TERM 143' TERM
trap 'on_signal HUP 129'  HUP

# settle_verdict_signal — deliver a signal that was deferred across the VERDICT EMISSION, now
# that the line is on stdout and VERDICT_EMITTED is committed. Called by `verdict` itself, so
# every emitter gets it without remembering to (roborev job 35 I1).
settle_verdict_signal() {
  [ -n "$VERDICT_SIG_PENDING" ] || return 0
  local sig="${VERDICT_SIG_PENDING%%:*}" code="${VERDICT_SIG_PENDING##*:}"
  VERDICT_SIG_PENDING=''
  trap '' INT TERM HUP
  detail "SIG$sig arrived DURING the verdict emission and was DEFERRED across it, so the verdict above was printed IN FULL rather than lost. Exiting $code."
  exit "$code"
}

# settle_pending_signal — deliver a signal that was deferred across the commit, AFTER the
# verdict is out. Called at the end of every mutating subcommand.
settle_pending_signal() {
  [ -n "$SIG_PENDING" ] || return 0
  local sig="${SIG_PENDING%%:*}" code="${SIG_PENDING##*:}"
  SIG_PENDING=''
  trap '' INT TERM HUP
  detail "SIG$sig arrived DURING the atomic rename and was DEFERRED across it, so the verdict above is the TRUE outcome rather than an 'undetermined'. Exiting $code."
  exit "$code"
}

# THE BACKSTOP. Contract (c) says EVERY exit carries a token, and a rule enforced only by
# reviewing each `exit` is the rule that grows a new exception every round — that is precisely
# how the signal traps came to have none. Any path that leaves without deciding now says so.
# NOTHING IN THIS TRAP MAY FAIL: a failing command in a bash EXIT trap under `set -e` aborts the
# trap AND REPLACES the exit status (measured on roborev job 26 F2).
on_exit() {
  local rc=$?
  # DISARMED FIRST (roborev job 35 I1). The backstop below calls `verdict`, which parks a signal
  # arriving mid-emission and then EXITs to deliver it — from inside the EXIT trap that would
  # skip `cleanup_tmp`. Nothing this trap does needs to be interruptible, so nothing is.
  trap '' INT TERM HUP 2>/dev/null || true
  if [ "$VERDICT_EXEMPT" -eq 0 ] && [ "$VERDICT_EMITTED" -eq 0 ]; then
    verdict ERROR || true
    detail "the run ended at exit $rc without reaching any decision point, so NOTHING was decided and NOTHING was written. This is a defect in $(sane "$prog"), not a state of the marker: report it with the command you ran." || true
  fi
  cleanup_tmp
  return 0
}
trap on_exit EXIT

# register_tmp <path> — remember a temp file for cleanup. Stored as an array ELEMENT, so no
# assumption is made about the path's shape (see TMP_FILES above).
register_tmp() { TMP_FILES+=("$1"); }

# The shared three-valued process-liveness primitives (#3822). A MISSING library is fatal
# and NAMED — never a silent continue with the predicates undefined, which would make
# every liveness answer an empty string and (worse) could read as "gone".
# ANCHORED IS NOT THE SAME AS VERDICT-BEARING (roborev job 26 F1). This guard used to
# printf its own prefixed line and exit 1: contract (a) was satisfied and contract (c) was
# NOT, so the ONE line every caller branches on — the closed-set token on the `verdict `
# line — was absent, and a `case` on that token (which drive-issue.md's Delta 4 mandates)
# fell through every arm on a FATAL failure. `refuse` is defined above precisely so that
# every exit of this script carries a token; there is no exception for a fatal one.
# `-f` AS WELL AS `-r` (roborev job 51's sweep). `.` on a FIFO would BLOCK FOREVER waiting for a
# writer, and `-r` is true for one. The path is repo-owned, so reaching that state means the
# checkout was tampered with — invoker-class, out of the threat model — but the guard is one
# token and a hang here has no verdict and no timeout. Both predicates FOLLOW a symlink, which
# is deliberate: a symlinked checkout is a legitimate layout.
{ [ -f "$SCRIPT_HOME/lib/process-liveness.sh" ] && [ -r "$SCRIPT_HOME/lib/process-liveness.sh" ]; } || \
  refuse ERROR 1 "cannot read $(sane "$SCRIPT_HOME/lib/process-liveness.sh") as a regular file (the shared process-liveness primitives) — NOTHING was measured"
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
  s="$(printf '%s' "${1:-}" 2>/dev/null | LC_ALL=C tr -c 'A-Za-z0-9._:/#-' '-' 2>/dev/null | LC_ALL=C sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//' 2>/dev/null)"
  s="$(LC_ALL=C printf '%.120s' "$s")"
  s="${s%-}"
  [ -n "$s" ] || s="unspecified"
  printf '%s\n' "$s"
}

# THE MACHINE AXIS, RESOLVED ONCE, WITH ITS MEASURABILITY CARRIED ALONGSIDE (roborev job 34 H1).
#
# `sanitize_field` maps an EMPTY value onto the `unspecified` PLACEHOLDER — deliberately, and
# that behaviour is claim.sh's, mirrored here and pinned by case 11, so it is NOT changed. What
# was wrong was COMMITTING the placeholder as an identity: with `hostname -s` failing (or
# printing nothing) and CLAIM_MACHINE unset, the writer stamped `machine: unspecified`. A
# TRANSIENT failure then writes a marker that turns FOREIGN-MACHINE the moment resolution
# recovers (the lane locks itself out), and a PERSISTENT one ALIASES EVERY BOX ONTO ONE OWNER,
# so lane A's marker verifies as OWNED on machine B — the peer-adoption defect this file exists
# to close, arriving through the identity it was supposed to enforce.
#
# So the REFUSAL lives at the USE SITE (`require_machine_axis`), not in the sanitizer: the
# claim.sh agreement is a property of the recorded VALUE, and this is a property of whether the
# value was MEASURED at all. Resolved ONCE and cached, because two calls to `hostname` can give
# two answers and an identity that changes mid-run is no identity.
#
# `hostname`'s stderr is suppressed (roborev job 26 F2): its failure is reported through the
# anchored refusal below, and an unprefixed 'hostname: not found' would break contract (a).
MACHINE_AXIS_VALUE=''
MACHINE_AXIS_STATE=''   # ok | unmeasured | unrecordable | lossy
MACHINE_AXIS_RAW=''
resolve_machine_axis() {
  [ -z "$MACHINE_AXIS_STATE" ] || return 0
  local raw
  # THE MIRRORED EXPRESSION — same env var, same default, same sanitizer as claim.sh's
  # `this_machine` (see IDENTITY in the header; case 11 pins the agreement behaviourally).
  # `|| true` INSIDE the substitution is load-bearing, and it is what the mirrored expression
  # needs HERE that claim.sh's does not need THERE: claim.sh calls it as an ARGUMENT (a failing
  # substitution in a word leaves the command's own status 0), while this is a plain ASSIGNMENT,
  # whose status IS the substitution's — so under `set -e` a failing `hostname` killed the shell
  # before the anchored refusal below could name the axis (measured: the EXIT-trap backstop's
  # generic ERROR, with no axis=machine detail). The value is unchanged either way: empty.
  raw="${CLAIM_MACHINE:-$(hostname -s 2>/dev/null || true)}"
  MACHINE_AXIS_RAW="$raw"
  MACHINE_AXIS_VALUE="$(sanitize_field "$raw")"
  if [ -z "$raw" ]; then
    MACHINE_AXIS_STATE=unmeasured
  elif [ "$MACHINE_AXIS_VALUE" = unspecified ]; then
    # The source said SOMETHING and none of it survives as a token (an all-punctuation
    # CLAIM_MACHINE, or a host whose `tr` is broken, which is the fail-open case 30 used to
    # declare as a residual). Recorded, it is the same alias, so it is refused the same way.
    MACHINE_AXIS_STATE=unrecordable
  elif [ "$MACHINE_AXIS_VALUE" != "$raw" ]; then
    # AN IDENTITY THAT SURVIVES SANITIZATION ONLY IN PART IS AN ALIAS (roborev job 37 J1, the
    # THIRD instance of one family: H1 committed the `unspecified` placeholder for an
    # UNMEASURABLE axis, the round-7 class sweep found the same shape on the worktree axis, and
    # this is a MEASURABLE identity committed LOSSILY). The sanitizer maps space -> '-',
    # collapses runs and TRUNCATES at 120 characters, so `build box` and `build-box` record the
    # SAME token and two boxes sharing a 120-character prefix record the same token — after
    # which lane A's marker verifies as OWNED on machine B, the precise defect this file exists
    # to prevent. Detected by requiring the recorded value to EQUAL the measured one: that one
    # comparison covers charset AND length, and it needs no second copy of the sanitizer's rules.
    MACHINE_AXIS_STATE=lossy
  else
    MACHINE_AXIS_STATE=ok
  fi
}

this_machine() { resolve_machine_axis; printf '%s\n' "$MACHINE_AXIS_VALUE"; }

# THE WORKTREE AXIS, THE SAME SHAPE, FOUND BY SWEEPING THE CLASS (roborev job 34 H1's class).
# H1 is one instance of "a required identity axis silently degrades"; this is the other axis on
# which the degradation is REACHABLE. `pwd -P` FAILS when the lane's working directory has been
# deleted or become unreadable, and three things followed: bash's own UNPREFIXED
# `pwd: error retrieving current directory` line broke contract (a); the assignment's non-zero
# status killed the shell under `set -e`, so the run ended on the EXIT-trap's GENERIC ERROR with
# no axis named; and `marker_path` composed `"/$MARKER_NAME"` — a read (and a would-be write) at
# the FILESYSTEM ROOT, i.e. a marker belonging to nobody being read as this lane's.
# Unlike the machine axis this one cannot ALIAS (an empty value can never equal a recorded
# absolute path, and the writer already refuses a non-absolute worktree), so the defect is a
# misattributed diagnostic and a wrong path — not a false OWNED. It is fixed the same way: one
# resolution, its measurability carried, and a refusal that NAMES THE AXIS at the use site.
WORKTREE_AXIS_VALUE=''
WORKTREE_AXIS_STATE=''   # ok | unmeasured
resolve_worktree_axis() {
  [ -z "$WORKTREE_AXIS_STATE" ] || return 0
  local wd
  # `|| true` for the same reason as the machine axis's, and `2>/dev/null` because a failing
  # `pwd` prints its own unprefixed diagnostic (contract (a)).
  wd="$(pwd -P 2>/dev/null || true)"
  WORKTREE_AXIS_VALUE="$wd"
  case "$wd" in
    /*) WORKTREE_AXIS_STATE=ok ;;
    *)  WORKTREE_AXIS_STATE=unmeasured ;;
  esac
}

this_worktree() { resolve_worktree_axis; printf '%s\n' "$WORKTREE_AXIS_VALUE"; }

# require_worktree_axis — main shell only, same posture as require_machine_axis.
require_worktree_axis() {
  resolve_worktree_axis
  [ "$WORKTREE_AXIS_STATE" = ok ] || refuse ERROR 1 "axis=worktree could NOT BE MEASURED: \`pwd -P\` named no absolute directory (the lane's working directory has probably been deleted or become unreadable). The marker PATH is derived from this axis, so continuing would read or write a marker at the filesystem root that belongs to nobody — nothing was read, nothing was written and nothing was decided. cd into the lane's worktree and re-run."
}

# require_machine_axis — refuse unless this box's own machine identity was MEASURED.
# Called from the main shell ONLY (never inside `$( )`, where the refusal would exit the
# subshell and its verdict line would be captured into a variable — the defect case 17 pins),
# wherever the axis is about to be RECORDED or COMPARED: `write` before any mutation, and
# `check_ownership`, which is verify's and adopt's one door. Where there is no marker at all
# nothing is compared and ABSENT is unaffected.
require_machine_axis() {
  resolve_machine_axis
  case "$MACHINE_AXIS_STATE" in
    ok) return 0 ;;
    unmeasured)
      refuse ERROR 1 "axis=machine could NOT BE MEASURED: CLAIM_MACHINE is unset or empty and \`hostname -s\` produced nothing. The machine axis is fail-closed identity, and recording the 'unspecified' placeholder would ALIAS every unmeasurable box onto ONE owner (this lane's marker would then verify as OWNED on any other box) — so nothing is written, nothing is compared and nothing was decided. Set CLAIM_MACHINE to this box's unique identity, or repair hostname resolution." ;;
    unrecordable)
      refuse ERROR 1 "axis=machine is NOT RECORDABLE: the machine identity resolved to a value that records as the 'unspecified' placeholder (it carries no recordable characters [A-Za-z0-9._:/#-], or the sanitizer itself could not run) and would ALIAS every such box onto ONE owner. Nothing was written, nothing was compared and nothing was decided. Set CLAIM_MACHINE to this box's unique identity." ;;
    lossy)
      refuse ERROR 1 "axis=machine is NOT RECORDABLE LOSSLESSLY: the machine identity '$(sane "$MACHINE_AXIS_RAW")' records as '$(sane "$MACHINE_AXIS_VALUE")', which is a DIFFERENT string. An identity that is normalized before it is compared ALIASES distinct machines onto one owner ('build box' and 'build-box' record the same token; so do two names sharing a 120-character prefix), and this lane's marker would then verify as OWNED on the other box. Nothing was written, nothing was compared and nothing was decided. Set CLAIM_MACHINE to a value that is already one token of [A-Za-z0-9._:/#-] and at most 120 characters." ;;
    *)
      refuse ERROR 1 "internal: machine-axis state '$(sane "$MACHINE_AXIS_STATE")' is not one of ok|unmeasured|unrecordable|lossy — nothing was decided" ;;
  esac
}

# THE SESSION AXIS, RESOLVED ONCE, WITH ITS LOSSLESSNESS CARRIED ALONGSIDE (roborev job 37 J1).
#
# The session axis is NOT fail-closed on a MISMATCH (see the header: a legitimate cron resume is
# a new session id in the same lane, so a literal mismatch check would red every correct resume).
# But an EQUAL session id is OWNED OUTRIGHT — basis `session`, the strongest verdict this script
# emits — so the same lossy-normalization defect the machine axis has lands here with the same
# consequence, one axis over: two DISTINCT session ids that sanitize to one token grant each
# other ownership. The remedy is the same and lives at the USE SITE for the same reason: the
# sanitizer is claim.sh's, pinned by the agreement case, and is not changed.
#
# `unrecorded` is not lossy — it is the ABSENCE of a value, which routes to the liveness
# resolution rather than to any comparison.
SESSION_AXIS_VALUE=''
SESSION_AXIS_STATE=''   # ok | unrecorded | lossy
SESSION_AXIS_RAW=''
resolve_session_axis() {
  [ -z "$SESSION_AXIS_STATE" ] || return 0
  SESSION_AXIS_RAW="${CLAUDE_CODE_SESSION_ID:-}"
  if [ -z "$SESSION_AXIS_RAW" ]; then
    SESSION_AXIS_VALUE='unrecorded'
    SESSION_AXIS_STATE=unrecorded
    return 0
  fi
  SESSION_AXIS_VALUE="$(sanitize_field "$SESSION_AXIS_RAW")"
  if [ "$SESSION_AXIS_VALUE" = "$SESSION_AXIS_RAW" ]; then
    SESSION_AXIS_STATE=ok
  else
    SESSION_AXIS_STATE=lossy
  fi
}

# this_session — the current session id, or the `unrecorded` sentinel. An unrecorded
# session makes the session axis UNMEASURED (never "equal"), which routes to the liveness
# resolution rather than to a false OWNED.
this_session() { resolve_session_axis; printf '%s\n' "$SESSION_AXIS_VALUE"; }

# require_session_axis — main shell only, same posture as require_machine_axis: called wherever
# the session axis is about to be RECORDED or COMPARED.
require_session_axis() {
  resolve_session_axis
  case "$SESSION_AXIS_STATE" in
    ok | unrecorded) return 0 ;;
    lossy)
      refuse ERROR 1 "axis=session is NOT RECORDABLE LOSSLESSLY: CLAUDE_CODE_SESSION_ID '$(sane "$SESSION_AXIS_RAW")' records as '$(sane "$SESSION_AXIS_VALUE")', which is a DIFFERENT string. An equal session id grants OWNED outright, so two distinct session ids that normalize to one token would own each other's markers. Nothing was written, nothing was compared and nothing was decided. Set CLAUDE_CODE_SESSION_ID to a value that is already one token of [A-Za-z0-9._:/#-] and at most 120 characters." ;;
    *)
      refuse ERROR 1 "internal: session-axis state '$(sane "$SESSION_AXIS_STATE")' is not one of ok|unrecorded|lossy — nothing was decided" ;;
  esac
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
# FILESYSTEM ENTRY TYPES (roborev job 51, and the sweep of its class).
#
# WHY A TYPE PROBE EXISTS AT ALL: a `test -e`/`test -r` answers whether a path is THERE, and
# every path this script opens then assumes it is a REGULAR FILE. It is not the same question,
# and the gap is not cosmetic — the three failure modes differ by type:
#
#   FIFO      opening one for write BLOCKS INDEFINITELY until a reader appears. MEASURED: with
#             a FIFO at the lock path, `write` ran until killed (`timeout 10` -> rc 124). In an
#             unattended lane that is a PERMANENT STALL, and it is the worst possible breach of
#             contract (c): not a wrong verdict, NO verdict, forever.
#   device    a char/block device accepts the open and the write, so the run would appear to
#             succeed while the bytes went to the device and the `flock` serialized nothing.
#   socket    opening one fails with ENXIO — a refusal, but under a message naming the wrong
#             cause ("it may be a directory").
#   directory an open-for-write fails cleanly.
#   symlink   FOLLOWED by every predicate except `-L`, so it redirects the open OUTSIDE the lane
#             (roborev job 43 K1, whose fix reached only the symlink half of this same class).
#
# Both helpers are pure `test` builtins: they STAT, they never OPEN, so probing is itself
# incapable of blocking. The fall-through is `unknown`, never `regular` — a type nothing here
# recognises is not licensed to be opened, which is this file's standing rule that a positive
# verdict requires an affirmative measurement.
#
# TWO functions, because the two call sites want different questions and answering the wrong
# one is a defect either way. The LOCK sidecar is script-owned: the entry ITSELF must be
# regular, so a symlink is refused whatever it points at. A caller's `--body-file` is the
# caller's own artifact: a symlink to a real file is ordinary and must keep working, so THERE
# the question is what the path RESOLVES to.
# ---------------------------------------------------------------------------

# path_kind_followed <path> — the type the path RESOLVES to (symlinks followed).
path_kind_followed() {
  local p="$1"
  [ -e "$p" ]  || { printf 'absent\n'; return 0; }
  [ ! -f "$p" ] || { printf 'regular\n'; return 0; }
  [ ! -d "$p" ] || { printf 'directory\n'; return 0; }
  [ ! -p "$p" ] || { printf 'fifo\n'; return 0; }
  [ ! -S "$p" ] || { printf 'socket\n'; return 0; }
  [ ! -b "$p" ] || { printf 'block-device\n'; return 0; }
  [ ! -c "$p" ] || { printf 'char-device\n'; return 0; }
  printf 'unknown\n'
}

# path_kind <path> — the type of the ENTRY, never its target's. The `-L` test comes FIRST
# because every other predicate follows a link, and a DANGLING symlink is `-L` true / `-e`
# false — the shape that made an earlier `-e` probe answer `absent` for an entry that plainly
# exists (roborev job 43 K1).
path_kind() {
  local p="$1"
  [ ! -L "$p" ] || { printf 'symlink\n'; return 0; }
  path_kind_followed "$p"
}

# ---------------------------------------------------------------------------
# Serialization. A verify-then-replace sequence that is not atomic is a guard with a hole:
# two sessions in ONE lane — the exact scenario this file exists for — can BOTH pass
# ownership verification and the second can clobber the first's stamp. The ownership check
# is sound; it just has to be indivisible from the replacement. So every MUTATING subcommand
# holds an exclusive `flock` on a sidecar lockfile across existence-probe -> ownership-verify
# -> body-capture -> replace, and `mv` remains the atomic commit INSIDE it.
#
# READERS (`verify`, `show`) deliberately take no lock: the commit is a rename, so a reader
# sees either the whole old file or the whole new one, never a torn one.
#
# NO flock => REFUSE. An unserialized mutation is an UNMEASURABLE guarantee, and the rule
# throughout this script is that an unknown is never read as satisfied. Consequence, stated
# rather than discovered: this makes the mutating half LINUX/util-linux-only (macOS ships no
# flock). The fleet is Linux; a host without flock gets a named refusal, not a silent race.
# ---------------------------------------------------------------------------
MARKER_LOCK_FD=9

lock_marker() {
  local wt lock
  wt="$(this_worktree)"
  lock="$(marker_path).lock"
  # Probed BEFORE `exec`, because a redirection failure on a bare `exec` terminates a
  # non-interactive shell with bash's own unprefixed diagnostic — which would break the
  # output anchor every consumer rests on, and emit no verdict at all.
  [ -w "$wt" ] || refuse ERROR 1 "the worktree directory $(sane "$wt") is not writable, so the marker lock cannot be taken — nothing was decided and nothing was written"
  # THE LOCK PATH IS SCRIPT-OWNED, SO ANY NON-REGULAR ENTRY THERE IS REFUSED (roborev job 51,
  # and the third round running in which a fix reached one member of its class: job 43 K1 taught
  # this path to refuse a SYMLINK and left every other non-regular type accepted). The FIFO is
  # the one that must never reach the open below — `: >>"$lock"` on a FIFO BLOCKS FOREVER — but
  # the rule is stated over the TYPE rather than over the list of types that hang today: this
  # script owns this path, it did not create what is there, and it neither follows, opens nor
  # replaces someone else's artifact. `absent` is the ordinary case (we create it); `regular` is
  # our own sidecar from a previous run. Everything else, INCLUDING a type this probe cannot
  # name, is a refusal. The symlink half is folded in here rather than kept as a second check,
  # so there is ONE rule and one message to keep true.
  local lkind; lkind="$(path_kind "$lock")"
  case "$lkind" in
    absent | regular) : ;;
    *) refuse ERROR 1 "the marker lock path $(sane "$lock") exists and is NOT a regular file (it is a $lkind) — this script owns that path, so it neither follows, opens nor replaces what someone else put there. Opening a FIFO there would BLOCK FOREVER waiting for a reader (an unattended lane would stall with no verdict at all); a device or socket would serialize NOTHING while appearing to succeed. Inspect the path and remove it deliberately. Nothing was decided and nothing was written." ;;
  esac
  command -v flock >/dev/null 2>&1 || refuse ERROR 1 "flock is not available on this host, so the ownership-verify -> replace sequence cannot be SERIALIZED. Refusing rather than mutating unserialized: two sessions in one lane could both pass verification and one clobber the other's stamp, which is the defect this marker exists to prevent. An unmeasurable guarantee is never read as satisfied."
  # THE `2>/dev/null` COMES FIRST, AND THAT ORDER IS THE WHOLE POINT (roborev job 30, found by
  # sweeping G3's class rather than by the finding). Bash applies redirections LEFT TO RIGHT, so
  # `: >>"$lock" 2>/dev/null` attempts the failing redirection BEFORE stderr is diverted and
  # prints its own UNPREFIXED `bash: <path>: Permission denied` (measured, both orders). A
  # redirection is an external-command diagnostic in every way that matters to contract (a).
  # The type check above has already refused every non-regular entry BY NAME, so what remains
  # here is a permission/quota failure, or an entry that changed type between the probe and this
  # open. It is deliberately NOT removed as redundant: the probe is a stat and this is the act,
  # and a check before an act can only describe a file that no longer has to be the one acted on.
  : 2>/dev/null >>"$lock" || refuse ERROR 1 "cannot create or append to the marker lock file $(sane "$lock") — the lane may not permit creating it, or it was replaced after its type was probed; nothing was decided"
  eval "exec ${MARKER_LOCK_FD}>>\"\$lock\""
  # flock's own stderr is SUPPRESSED rather than captured: capturing it would mean running
  # flock inside a command substitution, i.e. in a subshell, which is not a place to be
  # subtle about which open file description holds the lock. The anchored detail below names
  # both causes instead (roborev job 26 F2).
  flock -w "$MARKER_LOCK_WAIT_SECS" "$MARKER_LOCK_FD" 2>/dev/null || refuse ERROR 1 "another process holds the marker lock $(sane "$lock") (waited ${MARKER_LOCK_WAIT_SECS}s), or it could not be acquired — refusing rather than racing it; nothing was decided. (flock's own diagnostic is suppressed: it carries no DRIVE-STATE: prefix, and an unprefixed line breaks the output anchor every consumer rests on.)"
}

# ---------------------------------------------------------------------------
# Marker reading. Sets S_* globals. Refuses (and exits) on any structural fault.
# ---------------------------------------------------------------------------
S_issue=''; S_machine=''; S_worktree=''; S_session=''; S_pid=''
S_start_lo=''; S_start_hi=''; S_actor=''; S_ts=''; S_stage=''
S_request=''; S_pr=''; S_branch=''
# ADOPTION PROVENANCE (roborev job 26 F3). All FOUR are PARSED, not merely written: they are
# durable state — the audit record of how this lane changed hands — so an ordinary `write` has
# to be able to carry them forward, and carrying forward what is never read is impossible.
S_prior_session=''; S_prior_pid=''; S_prior_ts=''; S_adopt_reason=''
# UNRECOGNISED PROLOGUE KEYS, RETAINED VERBATIM (roborev job 37 J3). See the parser's `*)` arm
# for the ruling; they are carried across `write` and `adopt` as raw `key: value` LINES, in the
# order read, joined by newlines. Empty when the marker carries none.
S_unknown=''

marker_path() { printf '%s/%s\n' "$(this_worktree)" "$MARKER_NAME"; }

# count_sentinel <path> <sentinel> — column-zero, WHOLE-LINE occurrences on stdout, or a
# NON-ZERO return when the scan itself could not be performed. `grep -Fx` is what makes this a
# column-zero test: a sentinel appearing inside a `key: value` value or mid-sentence in the
# body is not a whole line and cannot pose as a stamp boundary.
#
# THREE-VALUED, BECAUSE grep IS (roborev job 30 G1). `grep -c` exits 0 when it matched, 1 when
# it did not, and >1 when the scan COULD NOT BE PERFORMED. The first cut wrote
# `... || true` + `case "$n" in *[!0-9]*) n=0`, collapsing those three onto two and taking the
# PERMISSIVE answer for the error: an unperformed scan counted as ZERO sentinels. With a
# DISPLACED sentinel that made `marker_class` answer `legacy`, and `write`'s MIGRATION path
# then DISCARDED AND REPLACED a file that may be a LIVE PEER's stamped state — the exact defect
# this script exists to prevent, arriving through the one branch allowed to destroy a marker.
# CLAUDE.md's standing rule: a positive verdict requires an AFFIRMATIVE MEASUREMENT, and a pass
# is never derived from the ABSENCE of a bad signal (`1699-find-tristate` lints the sibling
# `[ -z "$(find …)" ]` shape). Callers must therefore branch on the RETURN, never on the text.
count_sentinel() {
  local n rc=0
  n="$(LC_ALL=C grep -Fxc -- "$2" "$1" 2>/dev/null)" || rc=$?
  [ "$rc" -le 1 ] || return 1
  case "$n" in '' | *[!0-9]*) return 1 ;; esac
  printf '%s\n' "$n"
  return 0
}

# count_matching_lines <basic-regexp> — reads STDIN; the same three-valued discipline as
# count_sentinel, for the assembled-prologue field census.
count_matching_lines() {
  local n rc=0
  n="$(LC_ALL=C grep -c -- "$1" 2>/dev/null)" || rc=$?
  [ "$rc" -le 1 ] || return 1
  case "$n" in '' | *[!0-9]*) return 1 ;; esac
  printf '%s\n' "$n"
  return 0
}

# marker_class <path> — echo absent | not-regular | stamped | displaced | legacy | error.
#
# `not-regular` covers a directory, an unreadable regular file, and ANY SYMLINK (dangling or
# not) — see the `-L` note in the body.
#
# ONE CLASSIFIER FOR EVERY CALLER, AND "LEGACY" REQUIRES THE WHOLE FILE TO BE SENTINEL-FREE.
# The first cut inferred "carries no ownership stamp" from the FIRST LINE ALONE, and the
# migration path then discarded and replaced such a file. So a stamped marker with a single
# prepended blank line or comment — a ONE-BYTE mutation — was classified legacy, and A LIVE
# PEER'S STATE WAS OVERWRITTEN: precisely the defect this file exists to close, wearing the
# migration path's clothes. The inference "no stamp at line 1 => no identity asserted" is
# valid ONLY if no sentinel exists ANYWHERE, so `legacy` is now the answer of last resort:
#
#   stamped   line 1 IS the begin sentinel — parse it (dup/grammar checks follow)
#   displaced a sentinel exists at column zero but NOT as a valid prologue opener. The file
#             DOES assert an identity, which merely cannot be READ, and an unreadable
#             identity may be a live peer's => MALFORMED, never migratable.
#   legacy    no sentinel anywhere => genuinely the pre-#3822 shape => UNSTAMPED
#   error     the classification could NOT BE MEASURED (roborev job 30 G1). It is its own
#             class rather than a fall-through to `legacy`, because `legacy` is the ONE class
#             whose handler DESTROYS the file. Every caller must refuse on it.
marker_class() {
  local path="$1" nb ne first rc=0
  # EXISTENCE IS `-e` OR `-L`, AND A SYMLINK IS NEVER A MARKER THIS SCRIPT OWNS (roborev job 43
  # K1). `-e` FOLLOWS the link, so it is FALSE for a DANGLING symlink — an entry that plainly
  # EXISTS — and the classifier answered `absent`, which is the ONE class whose handler in
  # `write` replaces the path without a word. A symlink is a deliberate artifact someone placed,
  # so it is routed through `not-regular` (the refusal that already exists) whether its target
  # exists or not: the `-L` test comes FIRST, before `-f`, because `-f` follows the link and a
  # symlink to a real file would otherwise be indistinguishable from a marker we wrote. That
  # order is what makes BOTH symlink shapes take one refusal rather than two behaviours. Same
  # doctrine as count_sentinel's: a `test` file predicate is two-valued, so it collapses "cannot
  # tell" onto one answer — and unswept it picks the permissive one.
  if [ ! -e "$path" ] && [ ! -L "$path" ]; then printf 'absent\n'; return 0; fi
  [ ! -L "$path" ] || { printf 'not-regular\n'; return 0; }
  { [ -f "$path" ] && [ -r "$path" ]; } || { printf 'not-regular\n'; return 0; }
  first="$(head -1 "$path" 2>/dev/null)" || rc=$?
  [ "$rc" -eq 0 ] || { printf 'error\n'; return 0; }
  if [ "$first" = "$STAMP_BEGIN" ]; then
    printf 'stamped\n'; return 0
  fi
  nb="$(count_sentinel "$path" "$STAMP_BEGIN")" || { printf 'error\n'; return 0; }
  ne="$(count_sentinel "$path" "$STAMP_END")"   || { printf 'error\n'; return 0; }
  if [ "$nb" -gt 0 ] || [ "$ne" -gt 0 ]; then printf 'displaced\n'; else printf 'legacy\n'; fi
}

# read_marker — parse the stamp prologue of the marker in the current worktree.
# Exits with a named refusal on every structural fault; returns 0 with S_* set otherwise.
read_marker() {
  local path cls; path="$(marker_path)"
  cls="$(marker_class "$path")"
  [ "$cls" != absent ] || refuse ABSENT 3 "no $MARKER_NAME in $(sane "$(this_worktree)") — nothing to resume; this is a legitimate FRESH START, not a refusal"
  [ "$cls" != not-regular ] || refuse ERROR 1 "$(sane "$path") exists but is not a readable regular file (a SYMLINK takes this path too, dangling or not: it is an artifact someone placed deliberately, so it is refused rather than followed or replaced) — nothing was decided"
  [ "$cls" != error ] || refuse ERROR 1 "$(sane "$path") exists but could NOT BE CLASSIFIED: reading its first line, or scanning it for column-zero stamp sentinels, FAILED. An unperformed scan is not an absence of sentinels, so nothing is inferred from it — NOTHING was decided and NOTHING was replaced."
  if [ "$cls" = displaced ]; then
    refuse MALFORMED 8 "$(sane "$path") contains a stamp sentinel at column zero but NOT as its first line, so it DOES assert an ownership identity and that identity cannot be READ — it is NOT treated as an unstamped legacy marker and is never replaced for you, because an unreadable identity may be a LIVE PEER's (a single prepended blank line or comment produces this shape). INSPECT it, and if it is genuinely corrupt remove it or move it aside (e.g. 'mv $MARKER_NAME $MARKER_NAME.corrupt'); with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally."
  fi
  if [ "$cls" = legacy ]; then
    refuse UNSTAMPED 8 "$(sane "$path") carries NO ownership stamp (its first line is not the stamp sentinel) — this is the pre-#3822 marker shape, whose plan could belong to ANY session on ANY machine, so nothing is READ from it. The route forward is '$prog write <issue>', which SUCCEEDS over an unstamped marker and REPLACES it — DISCARDING its body, because an unstamped plan may belong to any session and is never carried forward. Save anything you need out of the file first."
  fi

  local nb ne
  nb="$(count_sentinel "$path" "$STAMP_BEGIN")" || refuse ERROR 1 "$(sane "$path") could not be SCANNED for stamp-begin sentinels, so how many identities it claims is unmeasured — NOTHING was decided and NOTHING was replaced."
  ne="$(count_sentinel "$path" "$STAMP_END")" || refuse ERROR 1 "$(sane "$path") could not be SCANNED for stamp-end sentinels, so the extent of its prologue is unmeasured — NOTHING was decided and NOTHING was replaced."
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
      request-id)                 dup="$S_request";        S_request="$val" ;;
      pr)                         dup="$S_pr";             S_pr="$val" ;;
      branch)                     dup="$S_branch";         S_branch="$val" ;;
      prior-session)              dup="$S_prior_session";  S_prior_session="$val" ;;
      prior-session-pid)          dup="$S_prior_pid";      S_prior_pid="$val" ;;
      prior-ts)                   dup="$S_prior_ts";       S_prior_ts="$val" ;;
      adopt-reason)               dup="$S_adopt_reason";   S_adopt_reason="$val" ;;
      # AN UNRECOGNISED KEY IS PRESERVED, NOT MERELY TOLERATED (roborev job 37 J3).
      #
      # THE CHOICE, AND WHY: accepting an unknown key "for forward compatibility" while the
      # rewrite path DROPPED it was incoherent — an OLDER copy of this script would silently
      # DELETE a field a NEWER one introduced, on the first stage update, which is the same
      # durable-state-erasure defect as job 26 F3's dropped adoption provenance. The two
      # coherent options were PRESERVE and REFUSE. PRESERVE wins: refusing would make a NEWER
      # marker unusable by an OLDER script, so a fleet mid-rollout would brick every lane the
      # new version had touched — strictly worse than the loss it prevents — and preservation is
      # what the header's durable-field survival rule already promises everywhere else.
      #
      # SAFE BY CONSTRUCTION, not by trust: the line was read from the prologue, so it is ONE
      # line (no newline can hide in it), its KEY passed the [a-z0-9-] gate, its VALUE is
      # non-empty, and it cannot begin with '<' — so a preserved line can neither pose as a
      # sentinel nor introduce a second grammar. It is written back BYTE-FOR-BYTE; nothing
      # re-sanitizes it, because re-sanitizing a value we did not produce is exactly the lossy
      # normalization J1 closes.
      *) dup=''
         if [ -z "$S_unknown" ]; then S_unknown="$line"; else S_unknown="$S_unknown
$line"; fi ;;
    esac
    # A DUPLICATE identity key would let the PARSER's choice decide identity, which is the
    # forgery shape this whole prologue exists to remove. Refused, not first-wins.
    [ -z "$dup" ] || refuse MALFORMED 8 "$(sane "$path") records '$(sane "$key")' TWICE in the stamp prologue — which occurrence is the identity is undecidable. The route forward is to move the file aside DELIBERATELY (e.g. 'mv $MARKER_NAME $MARKER_NAME.unreadable'): with no marker present this lane takes the ABSENT fresh-start path and a new stamped marker is written normally. It is NOT overwritten for you — unlike an unstamped marker this file CLAIMS an identity, and an identity that cannot be READ may be a live peer's. (This text deliberately names no subcommand: naming one that refuses in THIS state is the dead-letter shape scripts/tests/test_drive_issue_state.sh case 22 forbids.)"
    # THE SUPPRESSOR COMES FIRST (roborev job 37 J2's stat-then-act sweep). `marker_class`
    # established this path a moment ago, and the file can be gone by now — a failed input
    # redirection makes bash print its OWN unprefixed diagnostic, breaking contract (a), and
    # bash applies redirections LEFT TO RIGHT, so `<"$path" 2>/dev/null` would leak it. The
    # failure itself still ends the run through the EXIT-trap backstop's tokened ERROR.
  done 2>/dev/null <"$path"

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

# marker_body — the marker's body on stdout, EXCLUDING the writer-owned blank separator.
#
# THE WRITER OWNS THE SEPARATOR (roborev job 34 H3). `write_marker` ALWAYS emits exactly one
# blank line after the end sentinel, so the canonical file is `<stamp>\n<END>\n\n<body>`. This
# reader must therefore SKIP that one blank line, or the round trip that carries an owned
# marker's body forward (write -> marker_body -> write_marker) adds a blank line on EVERY write:
# measured 1, then 2, then 3 across three successive writes of the same body — unbounded growth
# on a long-running lane, and a body that MUTATES under a contract promising it SURVIVES.
# Exactly one of the two may own it; it is the WRITER, because the writer is what makes the
# separator canonical (a body supplied by --body-file gets one too, so the shape does not depend
# on where the body came from). Do NOT also strip it there, and do not add one here.
# Only the FIRST blank line is skipped, and only when it is the one immediately after the
# sentinel: a body that genuinely BEGINS with a blank line keeps it, because the separator is
# consumed before the body starts.
#
# Returns NON-ZERO when the body could not be read, and its stderr is suppressed: every
# caller must decide what an unreadable body means rather than inherit awk's unprefixed
# diagnostic and an empty result (roborev job 26 F2).
# THE BODY IS COPIED BY BYTE OFFSET, NEVER BY A LINE-ORIENTED TOOL (roborev job 35 I2). This
# used to be `awk ... { print }`, and awk ALWAYS terminates the record it prints — so a body whose
# LAST LINE HAD NO NEWLINE gained one on the next owned write or adoption (measured: a 19-byte
# `no trailing newline` came back as 20 bytes), breaking the byte-for-byte preservation the header
# promises one paragraph up. The class is "body bytes routed through a line-oriented tool", and it
# also covers `$( )` capture, which strips trailing newlines the same way — which is why this
# function writes to STDOUT for the caller to redirect into a file, and why the only other place
# body bytes move (`write_marker`) uses `cat`.
#
# WHY THIS IS BYTE-EXACT: awk is used ONLY to count the bytes of the lines BEFORE the body — the
# prologue, the end sentinel and the writer-owned separator, every one of which is newline
# terminated in any file that HAS a body — and `tail -c +N` then copies raw bytes from that
# offset to EOF, re-terminating nothing. `LC_ALL=C` is load-bearing: awk's `length()` counts
# CHARACTERS, so a multi-byte prologue value under a UTF-8 locale would yield a short offset and
# silently prepend stray bytes to the body.
marker_body() {
  local path off; path="$(marker_path)"
  # Fail-closed: no end sentinel at all (or an unreadable file) is a body that could not be READ,
  # never an empty one — every caller refuses rather than carrying an empty plan forward.
  off="$(LC_ALL=C awk -v s="$STAMP_END" '
    {
      if (state == 1) {
        # The line immediately after the sentinel. The writer ALWAYS emits exactly one blank
        # separator (see write_marker); skip it, and ONLY it, when it is there.
        if ($0 == "") pos += length($0) + 1
        print pos + 1
        done = 1
        exit
      }
      pos += length($0) + 1
      if ($0 == s) state = 1
    }
    END {
      if (done) exit 0
      # The sentinel was the LAST line: the body is empty, and the offset is one past EOF, which
      # `tail -c` answers with nothing. No sentinel at all is a read failure, not an empty body.
      if (state == 1) { print pos + 1; exit 0 }
      exit 3
    }
  ' "$path" 2>/dev/null)" || return 1
  case "$off" in '' | *[!0-9]*) return 1 ;; esac
  [ "$off" -ge 1 ] || return 1
  # `tail`'s stderr is suppressed for contract (a): its native `tail: cannot open ...` carries no
  # prefix, and the non-zero return already routes to each caller's anchored refusal.
  tail -c "+$off" "$path" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Liveness of the RECORDED writer. Sets LIVE_STATE (gone|alive|unknown) + LIVE_DETAIL.
# ---------------------------------------------------------------------------

# epoch_is_canonical <value> — 0 iff <value> is a bounded canonical decimal usable as an
# epoch second in `[ -le ]` and `$(( ))`: digits only, no sign, no leading zeros (so no
# accidental octal in an arithmetic context), 1..12 digits (year 2286 needs 10; 12 leaves
# headroom while staying far inside intmax_t, which is what stops `[` erroring out).
epoch_is_canonical() {
  case "${1:-}" in
    '' | *[!0-9]*) return 1 ;;
    0) return 0 ;;
    0*) return 1 ;;
  esac
  [ "${#1}" -le 12 ] || return 1
  return 0
}

# interval_is_usable <earliest> <latest> — 0 iff both endpoints are canonical AND ordered.
# Sets INTERVAL_WHY to the reason on failure, so the refusal says WHICH property failed
# rather than leaving the reader to guess.
INTERVAL_WHY=''
interval_is_usable() {
  INTERVAL_WHY=''
  if ! epoch_is_canonical "${1:-}"; then
    INTERVAL_WHY="'earliest' is not a bounded canonical epoch second"
    return 1
  fi
  if ! epoch_is_canonical "${2:-}"; then
    INTERVAL_WHY="'latest' is not a bounded canonical epoch second"
    return 1
  fi
  if [ "$1" -gt "$2" ]; then
    INTERVAL_WHY="the interval is INVERTED (earliest > latest), so it describes no instant"
    return 1
  fi
  return 0
}
LIVE_STATE=''; LIVE_DETAIL=''

writer_liveness() {
  LIVE_STATE=unknown; LIVE_DETAIL=''
  local pid="$S_pid" lo="$S_start_lo" hi="$S_start_hi"

  case "$pid" in
    '' | *[!0-9]*)
      LIVE_DETAIL="the stamp records session-pid=$(sane "$pid"), which is not a pid — the writing session's liveness is UNMEASURABLE. It is reported UNKNOWN and NOT 'gone': recording this script's own \$\$ instead would make a LIVE peer read as dead."
      return 0 ;;
  esac
  # THE RECORDED INTERVAL IS VALIDATED BEFORE ANYTHING IS PROBED, AND AN UNUSABLE ONE IS
  # `unknown` — NEVER `gone`. A digit-only test on the two endpoints CONCATENATED was not
  # enough, and the failure mode was the worst available: an INVERTED interval (earliest >
  # latest) or one whose magnitude `[` cannot parse makes the intersection test below ERROR,
  # an errored `[` inside `if A && B` reads as FALSE, and the false branch was `gone` — so a
  # LIVE PEER became adoptable. Unmeasurable must never read as gone; that is the whole
  # liveness axis. Each endpoint is checked SEPARATELY (concatenation let an empty endpoint
  # hide behind a digit-bearing one), as a bounded canonical decimal (no sign, no leading
  # zeros, <= 12 digits so `[ -le ]` and `$(( ))` cannot be handed an out-of-range value),
  # and the interval must be ORDERED.
  if ! interval_is_usable "$lo" "$hi"; then
    LIVE_DETAIL="the stamp's start window for pid $(sane "$pid") is UNUSABLE (earliest=$(sane "$lo") latest=$(sane "$hi"): $INTERVAL_WHY), so a pid that is alive NOW cannot be shown to be the process that stamped this marker — PID REUSE is indistinguishable, hence UNKNOWN and never 'gone'"
    return 0
  fi
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
  # The MEASURED window gets the same treatment as the recorded one: it is derived as
  # `now - elapsed`, so a nonsense `elapsed` yields a nonsense (even negative) endpoint, and
  # an unparseable operand in the comparison below would ERROR — which reads as FALSE, whose
  # branch is `gone`. Same false-permissive, other operand.
  if ! interval_is_usable "$ce" "$cl"; then
    LIVE_DETAIL="pid $(sane "$pid") is running but its MEASURED start window is unusable (earliest=$(sane "$ce") latest=$(sane "$cl"): $INTERVAL_WHY), so it cannot be compared with the recorded one — UNKNOWN, never 'gone'"
    return 0
  fi
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
#
# THE RESULT CARRIES HOW IT WAS ESTABLISHED (roborev job 34 H2). Three DIFFERENT facts reach
# `return 0`, and collapsing them into one undifferentiated success let `cmd_adopt` re-derive
# "is it already mine?" from the session id alone — so a changed or unrecorded session with the
# SAME LIVE PID took the ADOPTION path, rewrote the marker, recorded a STILL-RUNNING process as
# `prior-session-pid` and printed that the writer was "provably gone": a false statement in the
# audit record this script exists to produce, and precisely what LIVE-PEER refuses. So every
# caller reads OWNERSHIP_BASIS rather than re-deriving the distinction from the fields:
#   session         the recorded session id IS this session  -> already yours
#   same-live-pid   the recorded pid is THIS live process     -> already yours
#   adoptable-gone  the recorded writer is provably GONE      -> `adopt` may rewrite (only here)
# `verify` and `write` treat the first two identically and by design (the pid measurement is
# STRONGER than the id string it stands in for), and never see the third: `gone` is a refusal in
# strict mode.
# ---------------------------------------------------------------------------
OWNERSHIP_BASIS=''
check_ownership() {
  local issue="$1" mode="$2"
  local machine session
  # An identity that could not be MEASURED is never read as MATCHING (roborev job 34 H1): with
  # the placeholder on both sides the axis comparison SUCCEEDS, so an unmeasurable box would own
  # every marker recorded by another unmeasurable box. Refused before any comparison.
  require_machine_axis
  require_worktree_axis
  # AND THE SESSION AXIS MUST BE LOSSLESS BEFORE IT IS COMPARED (roborev job 37 J1). An equal
  # session id returns OWNED on the spot, so a lossy one aliases two sessions onto one owner.
  require_session_axis
  machine="$(this_machine)"; session="$(this_session)"

  read_marker

  # FAIL-CLOSED AXES first, and they are fail-closed in `adopt` too: adopt resolves the
  # SESSION axis only.
  [ "$S_issue" = "$issue" ] || refuse FOREIGN-ISSUE 4 "axis=issue recorded=$(sane "$S_issue") current=$(sane "$issue") — $(sane "$(marker_path)") is the durable state of a DIFFERENT issue; this lane is being reused. Move or remove it deliberately; it is never adopted for issue $(sane "$issue")."
  [ "$S_machine" = "$machine" ] || refuse FOREIGN-MACHINE 4 "axis=machine recorded=$(sane "$S_machine") current=$(sane "$machine") — this marker was stamped on another box (a copied tree, or a marker that travelled with a branch). Machine identity is not transferable, so it is refused rather than adopted."
  [ "$S_worktree" = "$(this_worktree)" ] || refuse FOREIGN-WORKTREE 4 "axis=worktree recorded=$(sane "$S_worktree") current=$(sane "$(this_worktree)") — this marker was stamped for a different worktree and has been COPIED here; a peer lane's plan is not this lane's."

  # SESSION AXIS. Equal AND recordable => OWNED. Anything else is liveness-resolved.
  if [ "$S_session" = "$session" ] && [ "$session" != unrecorded ]; then
    OWNERSHIP_BASIS=session
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
    OWNERSHIP_BASIS=same-live-pid
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
        OWNERSHIP_BASIS=adoptable-gone
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
  local f="$1" s rc
  for s in "$STAMP_BEGIN" "$STAMP_END"; do
    rc=0
    LC_ALL=C grep -Fxq -- "$s" "$f" 2>/dev/null || rc=$?
    # THREE-VALUED (roborev job 30 G1): 0 = the body carries a sentinel, 1 = it provably does
    # not, >1 = the scan could not be performed. The last is NOT the second: assuming a body
    # nobody could read carries no sentinel is a pass derived from the ABSENCE of a bad signal.
    case "$rc" in
      0) die_usage "the body carries a stamp sentinel as a whole line at column zero. It is REFUSED, not escaped: the stamp prologue is the only place identity is read from, and a body line that can pose as a boundary would break that. Remove the line (indent it, or drop the '<!--' comment) and retry. NOTHING was written." ;;
      1) : ;;
      *) refuse ERROR 1 "the body file $(sane "$f") could not be SCANNED for stamp sentinels, so whether it carries a line that can pose as a stamp boundary is UNMEASURED — refusing rather than assuming it does not; NOTHING was written." ;;
    esac
  done
}

# assert_assembled_marker <tmp> — validate the FULLY ASSEMBLED file immediately before the
# atomic rename. Sets WRITE_ERR and returns 1 on any fault.
#
# THE CHECK MUST BE INSIDE THE WINDOW IT CERTIFIES. `assert_body_safe` validates the body
# FILE, and the body is READ AGAIN later when the marker is assembled — two different reads
# of a file that can change in between, so the first cannot certify the second. The threat
# model matters here: a hostile INVOKER is out of scope (they can edit the marker directly),
# but this is reachable BY ACCIDENT — an agent's own notes file being rewritten while it
# writes the marker — and "reachable by accident" is a defect. The consequence is the
# dead-letter family again: a marker carrying a stray sentinel is refused by every later
# read, i.e. it BRICKS ITSELF.
#
# Validating the assembled temporary subsumes the race instead of narrowing it: these are
# EXACTLY the bytes that get committed, whatever route the body took to get there. It is a
# second, independent layer — `assert_body_safe` stays, because it gives the common case a
# clear usage error before any work is done.
assert_assembled_marker() {
  local tmp="$1" nb ne first rc=0
  first="$(head -1 "$tmp" 2>/dev/null)" || rc=$?
  if [ "$rc" -ne 0 ]; then
    WRITE_ERR="the assembled marker $(sane "$tmp") could not be READ back before the commit, so the bytes about to replace the marker are unverified — nothing was written"
    return 1
  fi
  if [ "$first" != "$STAMP_BEGIN" ]; then
    WRITE_ERR="internal: the assembled marker's first line is not the stamp sentinel — refusing to commit a file that every later read would refuse"
    return 1
  fi
  nb="$(count_sentinel "$tmp" "$STAMP_BEGIN")" || {
    WRITE_ERR="the assembled marker $(sane "$tmp") could not be SCANNED for stamp-begin sentinels, so it is UNVERIFIED — refusing to commit unverified bytes over the marker; nothing was written"; return 1; }
  ne="$(count_sentinel "$tmp" "$STAMP_END")" || {
    WRITE_ERR="the assembled marker $(sane "$tmp") could not be SCANNED for stamp-end sentinels, so it is UNVERIFIED — refusing to commit unverified bytes over the marker; nothing was written"; return 1; }
  if [ "$nb" -ne 1 ] || [ "$ne" -ne 1 ]; then
    WRITE_ERR="the assembled marker carries $nb stamp-begin and $ne stamp-end sentinels at column zero (exactly one of each is legal) — the body supplied at COMMIT time contains a line that can pose as a stamp boundary. Nothing was written. If a --body-file was named, it changed between validation and assembly, or it carries a sentinel at column zero: remove that line."
    return 1
  fi
  # AND THE REQUIRED FIELDS MUST BE THERE, NON-EMPTY (roborev job 26 F2, found by its own
  # test). The identity values are produced by external helpers — `date`, `tr` inside
  # sanitize_field, `hostname` — and write_marker is called from `if ! write_marker`, which
  # SUPPRESSES `set -e` for its whole subtree, so a helper failing inside a command
  # substitution yielded an EMPTY value and the marker committed anyway. Every later read
  # then refuses it MALFORMED: the lane BRICKS ITSELF, which is the dead-letter family again.
  # Checked HERE because this is the only place that sees the committed bytes, and scoped to
  # the PROLOGUE (a free-form body may legitimately contain a `stage: ...` line).
  local pro k n
  pro="$(awk -v e="$STAMP_END" 'NR==1{next} $0==e{exit} {print}' "$tmp" 2>/dev/null)" || {
    WRITE_ERR="the assembled marker's stamp prologue could not be EXTRACTED from $(sane "$tmp"), so its required fields are UNMEASURED — refusing to commit; nothing was written"; return 1; }
  for k in issue machine worktree session session-pid session-pid-start-earliest \
           session-pid-start-latest actor ts; do
    n="$(printf '%s\n' "$pro" 2>/dev/null | count_matching_lines "^$k: .")" || {
      WRITE_ERR="the assembled marker's stamp prologue could not be SCANNED for the required '$k:' field, so it is UNMEASURED — refusing to commit; nothing was written"; return 1; }
    if [ "$n" -ne 1 ]; then
      WRITE_ERR="the assembled marker records $n non-empty '$k:' line(s) in its stamp prologue (exactly one is required) — an incomplete stamp is not a weaker stamp, it is NO stamp, and every later read would refuse it, bricking this lane. The usual cause is a helper this writer depends on (date, tr, hostname) failing. NOTHING was written."
      return 1
    fi
  done
  return 0
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
  # ARGUMENT-COUNT GUARD BEFORE THE SHIFT (roborev job 30 G3, internal site) — see `refuse`.
  if [ "$#" -lt 3 ]; then
    WRITE_ERR="internal: write_marker was called with $# argument(s); an issue, an actor and a body-file slot are required"
    return 1
  fi
  local issue="$1" actor="$2" bodyfile="$3"; shift 3
  local wt path tmp ts session pid win lo hi
  wt="$(this_worktree)"
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

  # A FAILING `date` is a REFUSAL, not an empty `ts`. Its stderr is suppressed (unprefixed)
  # and its failure is reported through WRITE_ERR: `set -e` cannot be relied on here because
  # every caller invokes this function as `if ! write_marker ...`, which suppresses it for the
  # whole subtree (roborev job 26 F2).
  # THE MACHINE AXIS IS RE-ASSERTED OVER THE BYTES ABOUT TO BE COMMITTED (roborev job 34 H1).
  # The callers refuse an unmeasurable axis before they mutate anything; this is the backstop at
  # the site that ASSEMBLES the stamp, in the same posture as the `ts` completeness check below,
  # because a stamp with a placeholder identity is not a weaker stamp — it is an alias.
  resolve_machine_axis
  [ "$MACHINE_AXIS_STATE" = ok ] || {
    WRITE_ERR="the machine axis is not recordable (state=$(sane "$MACHINE_AXIS_STATE")), so the stamp's \`machine\` field would record a value that is not this box's measured identity — an 'unspecified' placeholder, or a LOSSILY normalized name — and either aliases this box onto another; nothing was written"; return 1; }
  # THE SAME BACKSTOP FOR THE SESSION AXIS (roborev job 37 J1), at the site that ASSEMBLES the
  # stamp: a lossily-recorded session id is an alias, not a weaker stamp.
  resolve_session_axis
  case "$SESSION_AXIS_STATE" in
    ok | unrecorded) : ;;
    *) WRITE_ERR="the session axis is not recordable losslessly (state=$(sane "$SESSION_AXIS_STATE")), so the stamp's \`session\` field would record a value that is not this session's id and could grant OWNED to a different session — nothing was written"; return 1 ;;
  esac
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null)" || {
    WRITE_ERR="cannot read the current time (\`date\` failed), so the stamp's \`ts\` field cannot be recorded — refusing rather than committing a stamp with an empty required field; nothing was written"; return 1; }
  [ -n "$ts" ] || {
    WRITE_ERR="\`date\` exited 0 but produced no timestamp, so the stamp's \`ts\` field cannot be recorded — nothing was written"; return 1; }
  session="$(this_session)"
  pid="$(this_session_pid)"
  lo=unmeasured; hi=unmeasured
  if [ "$pid" != unrecordable ]; then
    win="$(process_start_window "$pid" || true)"
    if [ -n "$win" ]; then lo="${win%% *}"; hi="${win##* }"; fi
  fi

  # mktemp's NATIVE text is CAPTURED and FOLDED into the anchored detail rather than
  # suppressed: "failed to create file via template" names the cause (a full disk, a
  # read-only lane) and the anchored message alone cannot. `2>&1` merges the streams, so a
  # SUCCESS that nevertheless printed something is not trusted as a path either.
  local mkout
  mkout="$(mktemp "$path.XXXXXX" 2>&1)" || {
    WRITE_ERR="cannot create a temporary file next to $(sane "$path") — nothing was written (mktemp: $(sane "$mkout"))"; return 1; }
  tmp="$mkout"
  { [ -n "$tmp" ] && [ -f "$tmp" ]; } || {
    WRITE_ERR="mktemp exited 0 next to $(sane "$path") but named no usable temporary file (it emitted: $(sane "$mkout")) — nothing was written"; return 1; }
  register_tmp "$tmp"
  {
    printf '%s\n' "$STAMP_BEGIN"
    printf 'issue: %s\n' "$issue"
    printf 'machine: %s\n' "$MACHINE_AXIS_VALUE"
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
    # THE WRITER OWNS THIS SEPARATOR — the one blank line between the prologue and the body
    # (roborev job 34 H3). `marker_body` therefore SKIPS it when reading a body back; if both
    # sides emit one, every carry-forward write grows the body by a line. Exactly one owner:
    # change this only together with the reader, and read that function's comment first.
    printf '\n'
    # `cat`'s stderr is SUPPRESSED, not captured: inside a `{ ... } >"$tmp"` group there is
    # nowhere to capture it to, and the group's non-zero status already routes to the
    # anchored WRITE_ERR below (roborev job 26 F2).
    #
    # THE BODY IS ALWAYS READ; IT IS NEVER STAT-GATED (roborev job 37 J2). This used to be
    # `[ -n "$bodyfile" ] && [ -s "$bodyfile" ]`, and the `-s` was a STAT STANDING IN FOR A
    # READ: between that probe and the copy the source can be deleted, truncated, replaced or
    # be a zero-sized nonregular file, and the false branch then contributed NOTHING while the
    # run reported `WRITTEN` — a silently EMPTY body in the one file whose whole job is durable
    # state. It is the same shape as the assembled-marker race: a check placed before the act
    # can only describe a file that no longer has to be the one acted on. Now the read is the
    # act — `cat` runs unconditionally, an empty source contributes nothing (which is what an
    # empty body IS), and a read FAILURE makes the group non-zero and routes to the anchored
    # WRITE_ERR below, so "the body could not be read" can never be committed as "there was no
    # body". The caller-supplied file is additionally SNAPSHOTTED once in cmd_write, which
    # removes the window rather than narrowing it.
    if [ -n "$bodyfile" ]; then cat "$bodyfile" 2>/dev/null; fi
  } 2>/dev/null >"$tmp" || { rm -f "$tmp" 2>/dev/null || true; WRITE_ERR="failed writing the stamp to $(sane "$tmp") — nothing was replaced (the body could not be read, or the temporary file could not be written)"; return 1; }
  # The last thing before the atomic commit, over the committed bytes themselves.
  assert_assembled_marker "$tmp" || { rm -f "$tmp" 2>/dev/null || true; return 1; }
  # THE COMMIT. mv's NATIVE text is CAPTURED and FOLDED for the same reason as mktemp's:
  # "cannot move ... Permission denied" vs "Device or resource busy" are different operator
  # actions. It is captured to a FILE and run as a PLAIN command rather than inside `$( )`,
  # which is the roborev job 30 G2 change and is load-bearing: MEASURED on bash 5.2, a trapped
  # signal arriving while the shell waits for a COMMAND SUBSTITUTION inside a FUNCTION is
  # DISCARDED — the trap never runs at all — while the same signal during a plain command is
  # delivered normally. The one window whose interruption changes durable state must be
  # signal-OBSERVABLE, or the phase machinery below can never see it.
  # CREATED BY `mktemp`, NOT BY A REDIRECTION INTO A DERIVED NAME (roborev job 51's sweep). The
  # commit below redirects into this path, and a redirection bash cannot satisfy prints an
  # UNPREFIXED diagnostic (see lock_marker) — so the path must be proven creatable BEFORE that
  # one command. This used to be `mverrf="$tmp.err"` plus `: >"$mverrf"`, which proves
  # creatability but opens whatever is already at that name: a FIFO there BLOCKS FOREVER, the
  # lock path's defect at a second site. A type probe cannot close it (the probe is a stat, the
  # redirect is the act), but `mktemp` can: it creates with O_EXCL, so it can never open an
  # existing entry of ANY type. That the name is unpredictable makes the race narrow; it does
  # not make it absent, and the fix costs one call.
  local mverrf mverrout mverr=''
  mverrout="$(mktemp "$path.err.XXXXXX" 2>&1)" || {
    rm -f "$tmp" 2>/dev/null || true
    WRITE_ERR="cannot create the commit's diagnostic file next to $(sane "$path") — nothing was written (mktemp: $(sane "$mverrout"))"; return 1; }
  mverrf="$mverrout"
  register_tmp "$mverrf"
  { [ -n "$mverrf" ] && [ -f "$mverrf" ]; } || {
    rm -f "$tmp" 2>/dev/null || true
    WRITE_ERR="mktemp exited 0 for the commit's diagnostic file next to $(sane "$path") but named no usable temporary file (it emitted: $(sane "$mverrout")) — nothing was written"; return 1; }
  COMMIT_PHASE=committing
  if mv -f "$tmp" "$path" >"$mverrf" 2>&1; then
    COMMIT_PHASE=committed
  else
    COMMIT_PHASE=idle
    mverr="$(cat "$mverrf" 2>/dev/null || true)"
    rm -f "$tmp" "$mverrf" 2>/dev/null || true
    WRITE_ERR="failed replacing $(sane "$path")${mverr:+ (mv: $(sane "$mverr"))}"; return 1
  fi
  # RESIDUAL, STATED RATHER THAN CLAIMED AWAY: bash defers a trap until the current command
  # completes, so a signal arriving during the rename runs the handler HERE — with the phase
  # still `committing` — and the deferral above is what makes that report the truth. The window
  # between `mv` returning and the phase assignment above is NOT eliminated, only narrowed to
  # two shell assignments; a signal landing exactly there is reported as a completed write,
  # which is correct, because the rename HAS returned. What is genuinely NOT atomic is the pair
  # (rename, phase update) when the rename FAILS: a pending signal is then dropped in favour of
  # the anchored ERROR verdict above, so the exit status is 1 rather than the signal's.
  rm -f "$mverrf" 2>/dev/null || true
  WROTE_PATH="$path"
  return 0
}

# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------
cmd_write() {
  # COUNT-CHECKED, NOT `shift || true` (roborev job 30 G3): a `shift` past the end prints
  # bash's own UNPREFIXED diagnostic under `shift_verbose`/POSIX mode — both settable from the
  # ENVIRONMENT (`BASHOPTS`) by a caller who never touches this file — which breaks contract
  # (a) before the anchored USAGE line is ever reached.
  local issue="${1:-}"
  [ "$#" -eq 0 ] || shift
  require_numeric_issue "$issue" write
  local stage='' request='' pr='' branch='' bodyfile='' actor_raw='' clears=''
  local prior_session='' prior_pid='' prior_ts='' adopt_reason='' unknown=''
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --stage)      [ "$#" -ge 2 ] || die_usage "--stage needs a value";      stage="$2";      shift 2 ;;
      --request-id) [ "$#" -ge 2 ] || die_usage "--request-id needs a value"; request="$2";    shift 2 ;;
      --pr)         [ "$#" -ge 2 ] || die_usage "--pr needs a value";         pr="$2";         shift 2 ;;
      --branch)     [ "$#" -ge 2 ] || die_usage "--branch needs a value";     branch="$2";     shift 2 ;;
      --body-file)  [ "$#" -ge 2 ] || die_usage "--body-file needs a value";  bodyfile="$2";   shift 2 ;;
      --actor)      [ "$#" -ge 2 ] || die_usage "--actor needs a value";      actor_raw="$2";  shift 2 ;;
      --clear)
        [ "$#" -ge 2 ] || die_usage "--clear needs a field name"
        case "$2" in
          stage | request-id | pr | branch) clears="$clears $2" ;;
          *) die_usage "--clear takes one of stage|request-id|pr|branch (got '$(sane "$2")') — the field set is CLOSED so a typo erases nothing silently" ;;
        esac
        shift 2 ;;
      *) die_usage "write: unknown option '$(sane "$1")'" ;;
    esac
  done
  cleared() { case " $clears " in *" $1 "*) return 0 ;; esac; return 1; }
  local actor; actor="$(resolve_actor "$actor_raw")"

  # BEFORE ANY MUTATION, and before the lock: the fresh-start (ABSENT) path never reaches
  # check_ownership, so this is the site that stops an unmeasurable machine axis being
  # COMMITTED as the 'unspecified' placeholder (roborev job 34 H1).
  require_machine_axis
  require_worktree_axis
  require_session_axis

  # THE CALLER'S BODY FILE IS READ EXACTLY ONCE, INTO A SNAPSHOT WE OWN (roborev job 37 J2).
  # Everything downstream — the sentinel scan, the assembled-marker validation and the copy into
  # the marker — then reads THE SAME BYTES. Validating one file and copying another is what let
  # a body vanish between the two; snapshotting SUBSUMES that window instead of narrowing it,
  # exactly as `assert_assembled_marker` does for the commit. The `-r` probe is kept for the
  # ordinary typo, as a friendly USAGE error before any work — it is NOT the guarantee: the
  # guarantee is that the copy below is an ACT whose failure is checked, not a stat.
  local body_src='' body_snap=''
  if [ -n "$bodyfile" ]; then
    [ -r "$bodyfile" ] || die_usage "--body-file '$(sane "$bodyfile")' is not readable — nothing was written"
    # AND IT MUST BE A REGULAR FILE — the same class as the lock sidecar's, at the one path a
    # CALLER names (roborev job 51's sweep). `-r` is TRUE for a FIFO, and the `cat` below then
    # BLOCKS FOREVER waiting for a writer: MEASURED, `--body-file <a fifo>` ran until killed
    # (`timeout 8` -> rc 124), the same permanent unattended stall as the lock path's. A
    # CHARACTER DEVICE is worse than a stall and was also measured: `--body-file /dev/zero`
    # streams without end into the snapshot, so the failure is a filled disk rather than a hang.
    # The question here is what the path RESOLVES to, not what the entry is: a symlink to a real
    # notes file is an ordinary thing for a caller to pass and keeps working. USAGE rather than
    # ERROR because the path came from the invocation — the same verdict as the `-r` line above,
    # and the documented contract is `--body-file <notes.md>`, a file.
    local bkind; bkind="$(path_kind_followed "$bodyfile")"
    [ "$bkind" = regular ] || die_usage "--body-file '$(sane "$bodyfile")' is not a REGULAR file (it resolves to a $bkind) — reading a FIFO would BLOCK FOREVER waiting for a writer and a device would stream without end, so it is refused rather than opened. Pass a file. NOTHING was written."
    body_snap="$(mktemp "${TMPDIR:-/tmp}/drive-issue-body.XXXXXX" 2>&1)" || refuse ERROR 1 "cannot create a temporary file for the --body-file snapshot under $(sane "${TMPDIR:-/tmp}") — nothing was written (mktemp: $(sane "$body_snap"))"
    { [ -n "$body_snap" ] && [ -f "$body_snap" ]; } || refuse ERROR 1 "mktemp exited 0 for the --body-file snapshot but named no usable temporary file (it emitted: $(sane "$body_snap")) — nothing was written"
    register_tmp "$body_snap"
    # `cat`'s own stderr is suppressed (contract (a): its native `cat: ...: No such file or
    # directory` carries no prefix) and its FAILURE is the refusal — never a silent empty body.
    cat "$bodyfile" 2>/dev/null >"$body_snap" || refuse ERROR 1 "the --body-file $(sane "$bodyfile") could not be READ (it was deleted, replaced or became unreadable after it was named) — refusing rather than committing an EMPTY body over this lane's durable state; nothing was written"
    assert_body_safe "$body_snap"
    body_src="$body_snap"
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
  # From here to the `mv` is ONE critical section (see Serialization above).
  lock_marker

  local carried='' discarded=''
  local mpath mcls
  mpath="$(marker_path)"
  mcls="$(marker_class "$mpath")"
  if [ "$mcls" != absent ]; then
    [ "$mcls" != not-regular ] || refuse ERROR 1 "$(sane "$mpath") exists but is not a readable regular file (a SYMLINK takes this path too, dangling or not: it is an artifact someone placed deliberately, so it is refused rather than followed or replaced) — nothing was written"
    # THE ONE PLACE WHERE A PERMISSIVE MISCLASSIFICATION DESTROYS DATA (roborev job 30 G1):
    # below, `legacy` DISCARDS and REPLACES the file. An unmeasurable classification must
    # therefore never arrive here as `legacy` (it no longer can) and must never be treated as
    # one — it could be a LIVE PEER's stamped marker whose sentinels simply could not be read.
    [ "$mcls" != error ] || refuse ERROR 1 "$(sane "$mpath") exists but could NOT BE CLASSIFIED: reading its first line, or scanning it for column-zero stamp sentinels, FAILED. Refusing rather than taking the UNSTAMPED migration path, which DISCARDS and REPLACES the file: an unmeasurable classification may be a LIVE PEER's stamped marker. NOTHING was written."
    # ONLY the `legacy` class migrates. A `displaced` sentinel means the file DOES assert an
    # identity (see marker_class), so it goes down the ownership path, where read_marker
    # refuses it MALFORMED — never discarded as though it asserted nothing.
    if [ "$mcls" = legacy ]; then
      local dl dbytes
      # `tr`'s stderr is suppressed alongside `wc`'s: an unreadable count degrades to the
      # '?' the message already prints, and neither tool may emit an unprefixed line.
      dl="$(LC_ALL=C wc -l <"$mpath" 2>/dev/null | tr -d ' ' 2>/dev/null || true)"
      dbytes="$(LC_ALL=C wc -c <"$mpath" 2>/dev/null | tr -d ' ' 2>/dev/null || true)"
      discarded="replaced an UNSTAMPED marker of unknown provenance and DISCARDED its body (${dl:-?} lines, ${dbytes:-?} bytes): an unstamped plan may belong to ANY session, so it is never carried forward"
      # body_src stays as the caller supplied it (empty unless --body-file): the preserve
      # branch below is UNREACHABLE from here, which is the point.
    else
      check_ownership "$issue" strict
      # PRESERVE THE RECORDED DURABLE FIELDS unless this call overrides or --clear's them.
      # drive-issue.md's Delta 3 names "stage reached, open request ID, PR/branch" as the
      # durable state, so a `write --stage x` that silently dropped the OPEN REQUEST ID
      # would leave the next session unable to tell which request it is waiting on — it
      # would re-ask, breaking "one marker, one wait", the rule the whole cron design rests
      # on. Erasing a field is therefore an EXPLICIT gesture (`--clear <field>`), never a
      # side effect of omitting a flag.
      [ -n "$stage" ]   || cleared stage      || stage="$S_stage"
      [ -n "$request" ] || cleared request-id || request="$S_request"
      [ -n "$pr" ]      || cleared pr         || pr="$S_pr"
      [ -n "$branch" ]  || cleared branch     || branch="$S_branch"
      # ADOPTION PROVENANCE IS DURABLE STATE TOO (roborev job 26 F3, the same class as job
      # 18's dropped durable fields, reappearing at the adopt fields). `adopt` records WHO
      # held this lane, WHEN, and WHY it was taken; rebuilding the prologue from the write
      # path's field list alone erased all of it on the very next stage update, which makes
      # the mandatory, validated `--reason` worthless. There is no flag and no `--clear` for
      # these: only a LATER `adopt` replaces them (the newest hand-over is the record), and
      # nothing invents them where no adoption happened.
      prior_session="$S_prior_session"; prior_pid="$S_prior_pid"
      prior_ts="$S_prior_ts";           adopt_reason="$S_adopt_reason"
      # AND THE UNRECOGNISED KEYS (roborev job 37 J3) — see the parser's `*)` arm. Only on the
      # OWNED path: the UNSTAMPED migration DISCARDS the file, so it has nothing to carry.
      unknown="$S_unknown"
      if [ -z "$body_src" ]; then
      # Preserve an OWNED marker's body: `write` updates the stamp and the recorded stage,
      # not the author's notes.
        carried="$(mktemp "${TMPDIR:-/tmp}/drive-issue-body.XXXXXX" 2>&1)" || refuse ERROR 1 "cannot create a temporary file for the carried body under $(sane "${TMPDIR:-/tmp}") — nothing was written (mktemp: $(sane "$carried"))"
        { [ -n "$carried" ] && [ -f "$carried" ]; } || refuse ERROR 1 "mktemp exited 0 for the carried body but named no usable temporary file (it emitted: $(sane "$carried")) — nothing was written"
        register_tmp "$carried"
        marker_body 2>/dev/null >"$carried" || refuse ERROR 1 "cannot read the existing marker's body from $(sane "$(marker_path)"), so it cannot be carried forward — refusing rather than silently replacing a plan with an empty one; nothing was written"
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
  local f_ps='' f_pp='' f_pt='' f_ar=''
  [ -z "$prior_session" ] || f_ps="prior-session: $(sanitize_field "$prior_session")"
  [ -z "$prior_pid" ]     || f_pp="prior-session-pid: $(sanitize_field "$prior_pid")"
  [ -z "$prior_ts" ]      || f_pt="prior-ts: $(sanitize_field "$prior_ts")"
  [ -z "$adopt_reason" ]  || f_ar="adopt-reason: $(sanitize_field "$adopt_reason")"
  COMMIT_VERDICT=WRITTEN
  if ! write_marker "$issue" "$actor" "$body_src" "$f_stage" "$f_request" "$f_pr" "$f_branch" \
      "$f_ps" "$f_pp" "$f_pt" "$f_ar" "$unknown"; then
    [ -z "$carried" ] || rm -f "$carried" 2>/dev/null || true
    refuse ERROR 1 "$WRITE_ERR"
  fi
  [ -z "$carried" ] || rm -f "$carried" 2>/dev/null || true
  verdict WRITTEN
  [ -z "$discarded" ] || detail "$discarded"
  detail "issue=$(sane "$issue") machine=$(sane "$(this_machine)") worktree=$(sane "$(this_worktree)") session=$(sane "$(this_session)") session-pid=$(sane "$(this_session_pid)") actor=$(sane "$actor") -> $(sane "$WROTE_PATH")"
  settle_pending_signal
}

cmd_verify() {
  # COUNT-CHECKED, NOT `shift || true` (roborev job 30 G3): a `shift` past the end prints
  # bash's own UNPREFIXED diagnostic under `shift_verbose`/POSIX mode — both settable from the
  # ENVIRONMENT (`BASHOPTS`) by a caller who never touches this file — which breaks contract
  # (a) before the anchored USAGE line is ever reached.
  local issue="${1:-}"
  [ "$#" -eq 0 ] || shift
  require_numeric_issue "$issue" verify
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor) [ "$#" -ge 2 ] || die_usage "--actor needs a value"; shift 2 ;;
      *) die_usage "verify: unknown option '$(sane "$1")'" ;;
    esac
  done
  check_ownership "$issue" strict
  verdict OWNED
  detail "issue=$(sane "$issue") machine=$(sane "$S_machine") worktree=$(sane "$S_worktree") session=$(sane "$S_session") stage=$(sane "${S_stage:-none}") ts=$(sane "$S_ts") basis=$(sane "$OWNERSHIP_BASIS") — this lane's marker, this session; resume from the recorded stage"
}

# assert_reason <raw> — claim.sh's `--reason` gate, same rules and same reasons. An
# UNSUBSTITUTED template is refused on the RAW text (before sanitization), because
# `resume:<branch>` sanitizes to a non-sentinel token and would otherwise record an
# unresolved placeholder as the audit reason. These commands are read by agents that run
# printed text LITERALLY.
REASON_TOKEN=''
assert_reason() {
  local raw="$1" tok
  case "$raw" in
    *'<'*'>'*) die_usage "adopt: --reason '$(sane "$raw")' still carries an UNSUBSTITUTED placeholder (<…>) — substitute it, e.g. --reason cron-reinvoke:writer-pid-gone" ;;
  esac
  tok="$(sanitize_field "$raw")"
  if [ "$tok" = unspecified ] || [ "${#tok}" -lt 3 ]; then
    die_usage "adopt: --reason must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '$(sane "$raw")' records as '$(sane "$tok")', which is indistinguishable from no reason at all"
  fi
  case "$(printf '%s' "$tok" 2>/dev/null | LC_ALL=C tr 'A-Z' 'a-z' 2>/dev/null)" in
    why | reason | todo | tbd | tba | xxx | xxxx | placeholder | fixme | none | foo | bar | baz | n/a)
      die_usage "adopt: --reason '$(sane "$raw")' records as the PLACEHOLDER '$(sane "$tok")' — as uninformative as no reason at all. Say what the resume IS, e.g. --reason cron-reinvoke:writer-pid-gone" ;;
  esac
  REASON_TOKEN="$tok"
}

cmd_adopt() {
  # COUNT-CHECKED, NOT `shift || true` (roborev job 30 G3): a `shift` past the end prints
  # bash's own UNPREFIXED diagnostic under `shift_verbose`/POSIX mode — both settable from the
  # ENVIRONMENT (`BASHOPTS`) by a caller who never touches this file — which breaks contract
  # (a) before the anchored USAGE line is ever reached.
  local issue="${1:-}"
  [ "$#" -eq 0 ] || shift
  require_numeric_issue "$issue" adopt
  local reason='' reason_given=0 actor_raw=''
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --reason) [ "$#" -ge 2 ] || die_usage "--reason needs a value"; reason="$2"; reason_given=1; shift 2 ;;
      --actor)  [ "$#" -ge 2 ] || die_usage "--actor needs a value"; actor_raw="$2"; shift 2 ;;
      *) die_usage "adopt: unknown option '$(sane "$1")'" ;;
    esac
  done
  # ARGUMENTS ARE VALIDATED BEFORE ANY STATE IS READ, so a placeholder reason is exit 64
  # whatever the lane looks like — a usage error must not depend on the marker's state.
  [ "$reason_given" -eq 1 ] || die_usage "adopt requires --reason saying what the resume IS (it is recorded in the stamp next to who took it), e.g. --reason cron-reinvoke:writer-pid-gone"
  local reason_token actor
  # REPORTED THROUGH A GLOBAL, NEVER `$( )`. assert_reason can `die_usage`, and a die/refuse
  # inside a command substitution exits only the SUBSHELL: its `verdict USAGE` line would be
  # CAPTURED into this variable instead of reaching stdout, leaving the run with no verdict at
  # all — exactly the defect case 17 pins for write_marker.
  assert_reason "$reason"
  reason_token="$REASON_TOKEN"
  actor="$(resolve_actor "$actor_raw")"

  # THE AXIS GUARDS RUN BEFORE ANYTHING DERIVES A PATH OR TAKES A LOCK (roborev job 43 K2).
  # `lock_marker` derives its lockfile from `marker_path`, i.e. from the WORKTREE axis, so with
  # that axis unmeasurable `adopt` used to compose a lock path at the FILESYSTEM ROOT and refuse
  # with a generic "the worktree directory / is not writable" — while `write` and `show`, which
  # guard first, refused by name with `axis=worktree`. Same input, two diagnostics depending on
  # the subcommand, and the axis form is the one .claude/commands/drive-issue.md publishes as the
  # ERROR arm of the consumer contract. `check_ownership` re-asserts all three (it is verify's and
  # adopt's one door); these calls are the ones that make the refusal precede the lock, and they
  # are idempotent — each axis resolves once and caches its state.
  require_machine_axis
  require_worktree_axis
  require_session_axis

  lock_marker
  check_ownership "$issue" adopt
  local prior_session="$S_session" prior_pid="$S_pid" prior_ts="$S_ts" stage="$S_stage"
  # THE DURABLE FIELDS SURVIVE AN ADOPT. `adopt` is THE normal cron-resume path (a new
  # session id in the same lane), so dropping `request-id`/`pr`/`branch` here destroyed the
  # open coordination request on EVERY legitimate resume — after which the resuming session
  # cannot tell which request it awaits and re-asks. Ownership transfers; the state does not
  # evaporate.
  local request="$S_request" pr="$S_pr" branch="$S_branch"
  # UNRECOGNISED KEYS SURVIVE AN ADOPT TOO (roborev job 37 J3): a hand-over transfers ownership,
  # it does not discard fields a newer version of this script recorded.
  local unknown="$S_unknown"

  # RE-ENTRANT FOR BOTH OWNERSHIP BASES (roborev job 34 H2). This used to test the SESSION ID
  # alone, which is only ONE of the two ways check_ownership says "already yours" — so the
  # same-live-pid basis fell through to the rewrite below and recorded a running process as the
  # prior owner. The basis is now read from the verification itself rather than re-derived here,
  # and the ONLY basis licensed to rewrite is `adoptable-gone`; `alive` and `unknown` never reach
  # this point (they refuse inside check_ownership) and an unrecognised basis is a refusal, not a
  # rewrite — a positive action must never rest on an unrecognised state.
  case "$OWNERSHIP_BASIS" in
    session | same-live-pid)
      verdict ADOPTED
      detail "re-entrant: this session already owns $(sane "$(marker_path)") (basis=$(sane "$OWNERSHIP_BASIS")) — nothing to transfer, and NOTHING is recorded as a prior owner: the recorded writer is THIS session, not a gone one"
      return 0 ;;
    adoptable-gone) : ;;
    *)
      refuse ERROR 1 "internal: ownership basis '$(sane "$OWNERSHIP_BASIS")' is not one of session|same-live-pid|adoptable-gone — refusing to transfer ownership on an unrecognised basis; nothing was written" ;;
  esac

  local carried f_stage=''
  carried="$(mktemp "${TMPDIR:-/tmp}/drive-issue-body.XXXXXX" 2>&1)" || refuse ERROR 1 "cannot create a temporary file for the carried body under $(sane "${TMPDIR:-/tmp}") — nothing was written (mktemp: $(sane "$carried"))"
  { [ -n "$carried" ] && [ -f "$carried" ]; } || refuse ERROR 1 "mktemp exited 0 for the carried body but named no usable temporary file (it emitted: $(sane "$carried")) — nothing was written"
  register_tmp "$carried"
  marker_body 2>/dev/null >"$carried" || refuse ERROR 1 "cannot read the existing marker's body from $(sane "$(marker_path)"), so it cannot be carried across the adoption — refusing rather than transferring ownership onto an empty plan; nothing was written"
  assert_body_safe "$carried"
  [ -z "$stage" ] || f_stage="stage: $stage"
  local f_request='' f_pr='' f_branch=''
  [ -z "$request" ] || f_request="request-id: $request"
  [ -z "$pr" ]      || f_pr="pr: $pr"
  [ -z "$branch" ]  || f_branch="branch: $branch"
  COMMIT_VERDICT=ADOPTED
  if ! write_marker "$issue" "$actor" "$carried" "$f_stage" "$f_request" "$f_pr" "$f_branch" \
      "prior-session: $prior_session" \
      "prior-session-pid: $prior_pid" \
      "prior-ts: $prior_ts" \
      "adopt-reason: $reason_token" "$unknown"; then
    rm -f "$carried" 2>/dev/null || true
    refuse ERROR 1 "$WRITE_ERR"
  fi
  rm -f "$carried" 2>/dev/null || true
  verdict ADOPTED
  detail "issue=$(sane "$issue") prior-session=$(sane "$prior_session") prior-session-pid=$(sane "$prior_pid") new-session=$(sane "$(this_session)") reason=$(sane "$reason_token") -> $(sane "$WROTE_PATH"); the recorded writer was provably gone: $LIVE_DETAIL"
  settle_pending_signal
}

cmd_show() {
  # COUNT-CHECKED, NOT `shift || true` (roborev job 30 G3): a `shift` past the end prints
  # bash's own UNPREFIXED diagnostic under `shift_verbose`/POSIX mode — both settable from the
  # ENVIRONMENT (`BASHOPTS`) by a caller who never touches this file — which breaks contract
  # (a) before the anchored USAGE line is ever reached.
  local issue="${1:-}"
  [ "$#" -eq 0 ] || shift
  require_numeric_issue "$issue" show
  # `show` asserts nothing about OWNERSHIP, so it needs no machine axis — but it DERIVES THE
  # MARKER PATH from the worktree axis, and an unmeasurable one would make it print a marker
  # from the filesystem root as though it were this lane's.
  require_worktree_axis
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
  emit "field request-id=$(sane "${S_request:-none}")"
  emit "field pr=$(sane "${S_pr:-none}")"
  emit "field branch=$(sane "${S_branch:-none}")"
  # `show` is the contract's WINDOW onto the marker, so it prints every provenance field it
  # parses — printing one of the four and hiding three would make the audit record readable
  # only by opening the file, which is the state this script exists to replace. Each is
  # conditional: a lane that was never adopted has no provenance to show.
  [ -z "$S_prior_session" ] || emit "field prior-session=$(sane "$S_prior_session")"
  [ -z "$S_prior_pid" ]     || emit "field prior-session-pid=$(sane "$S_prior_pid")"
  [ -z "$S_prior_ts" ]      || emit "field prior-ts=$(sane "$S_prior_ts")"
  [ -z "$S_adopt_reason" ]  || emit "field adopt-reason=$(sane "$S_adopt_reason")"
  # AND THE UNRECOGNISED KEYS THIS VERSION PRESERVES (roborev job 37 J3). `show` is the window
  # onto the marker and these are now RETAINED state, so hiding them would make a field this
  # script carries forward readable only by opening the file.
  if [ -n "$S_unknown" ]; then
    local uline
    while IFS= read -r uline; do
      [ -n "$uline" ] || continue
      emit "field unrecognised ${uline%%: *}=$(sane "${uline#*: }")"
    done <<EOF
$S_unknown
EOF
  fi
  verdict SHOWN
  detail "fields as recorded for issue $(sane "$issue"); SHOWN asserts NOTHING about ownership — use 'verify' for that"
}

SUB="${1:-}"
# The four `shift`s below cannot fail: `$SUB` is non-empty only when `$#` >= 1, and an empty
# `$SUB` takes the `''` arm. Stated rather than guarded, because a guard whose condition is
# unreachable is a guard nobody can test (roborev job 30 G3 swept every OTHER site).
case "$SUB" in
  write)  shift; cmd_write  "$@" ;;
  verify) shift; cmd_verify "$@" ;;
  adopt)  shift; cmd_adopt  "$@" ;;
  show)   shift; cmd_show   "$@" ;;
  -h | --help) print_help ;;
  '') die_usage "a subcommand is required: write <N> | verify <N> | adopt <N> --reason <why> | show <N> (see --help)" ;;
  *)  die_usage "unknown subcommand: $(sane "$SUB") (expected write|verify|adopt|show)" ;;
esac
