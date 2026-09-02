#!/usr/bin/env bash
#
# check-object-store-integrity.sh — the #3749 SHARED-OBJECT-STORE integrity sweep.
#
# WHAT QUESTION THIS ANSWERS
# --------------------------
# On this fleet EVERY LANE ON A BOX IS A `git worktree` OF ONE SHARED `.git`
# (measured: `/data/lanes/repo/.git/objects` for lane-3544, lane-3473 and lane-3629
# alike). Git does NOT rehash a packed or loose object against the id it was asked
# for on an ordinary read: it verifies the pack CRC and the zlib stream, which catch
# bit rot and a truncated or torn write, but a whole object whose CONTENT does not
# hash to its own name is returned without complaint. So every consumer of that
# store — including the gate's component-set pre-flight, which reads `origin/main`'s
# committed manifest and HEAD's committed component declaration THROUGH it
# (`scripts/agent-gate.sh`, the `_CS_BASE_OBJ=reused` fast path) — is trusting
# content it never verified.
#
# THE SCOPE OF THIS SCRIPT IS ACCIDENTAL CORRUPTION, AND THAT IS AN OWNER RULING
# (#3749, 2026-09-01), NOT AN OVERSIGHT
# -----------------------------------------------------------------------------
# DELIBERATE peer forgery is INVOKER-CLASS and OUT OF MODEL. Per the #3312 triage
# rule recorded in CLAUDE.md — *same-host actors able to write these scripts are
# invoker-class, not third parties* — a peer lane that wants a false gate PASS can
# simply edit `scripts/agent-gate.sh`, which is cheaper than forging pack data. The
# ruling REJECTED all three hardening alternatives, and the reasons are recorded so
# they are not re-derived:
#   * per-lane full clones — a permanent multi-GB tax on every lane for a threat
#     that is out of model;
#   * per-read rehashing — the fourth carve into one pre-flight, and a permanent
#     cost on every `--lite` round;
#   * removing the object-reuse fast path — a HALF-closure: the ancestry walk and
#     the provenance leg must still read HEAD's COMMITTED content, which has no
#     source other than this store.
# What IS in model is corruption nobody intended: bit rot, a torn pack write, a
# full disk mid-write, a SIGKILLed `git gc`. That is what this sweep closes, and
# `git fsck` is the only thing that answers it, because it REHASHES.
#
# NOT `--connectivity-only`, EVER. `--connectivity-only` walks the reachability
# graph and does NOT rehash object content, so it cannot detect the corruption this
# script exists to find. It would make the sweep fast and vacuous. Do not
# "optimise" it in.
#
# NOT `--strict` either, and for the opposite reason: `--strict` promotes legitimate
# historical warnings (a malformed committer line, a zero-padded file mode) to
# errors, so it would report CORRUPT on a healthy store — the guard operators learn
# to waive.
#
# THE VOCABULARY IS CHOSEN SO THIS CANNOT BE READ AS A CERTIFICATION
# ------------------------------------------------------------------
# The house idiom is `scripts/flow/base-staleness.sh`, and the properties are the
# same ones (CLAUDE.md documents why each is load-bearing):
#   (a) EVERY output line, stdout AND stderr, begins with `OBJECT-STORE: `.
#   (b) Every dynamic field is CONTROL-CHARACTER SANITIZED (newline, CR, other C0,
#       DEL -> a visible escape). GIT PERMITS NEWLINES IN PATHS and an fsck
#       diagnostic quotes paths verbatim, so an unsanitized field emits a line with
#       NO PREFIX AT ALL, breaking the one invariant every consumer and every test
#       rests on. Fields are otherwise kept VERBATIM: an object id or a path that
#       has been masked is useless to the operator who has to act on it.
#   (c) The verdict appears ONLY on an `OBJECT-STORE: verdict ` line, and its token
#       is from the CLOSED set {VERIFIED, CORRUPT, UNMEASURED}. Continuation prose
#       goes on `verdict-detail` lines, so the verdict line's token position can
#       never hold a word.
#   (d) This script's own STATIC TEMPLATE TEXT contains no other verdict
#       vocabulary — asserted STRUCTURALLY over the source file by
#       `scripts/tests/test_check_object_store_integrity.sh`, because that is a
#       provable property while a claim about one sample run is not.
#
# EXIT CODES, AND THE CONSUMER CONTRACT
# -------------------------------------
#   0   VERIFIED   — the sweep RAN TO COMPLETION and reported no corruption.
#   4   CORRUPT    — fsck reported OBJECT/PACK damage (its exit bits 1/4) on TWO
#                    independent walks. The affected object ids are named.
#   5   UNMEASURED — the answer was not obtained: no git, no resolvable object
#                    store, no usable timeout binary, the bound expired, an fsck
#                    failure this script cannot classify, a damage class that did
#                    NOT reproduce, or reachability/ref/commit-graph/multi-pack-index
#                    complaints that did.
#
# REACHABILITY IS ITS OWN NON-PASSING STATE, NEITHER CLEAN NOR CORRUPT. fsck's exit
# bit 2 (ERROR_REACHABLE) fires for a stale reflog entry on a store peer lanes are
# writing — routine on this fleet, and NOT this script's subject — but also for a
# genuinely MISSING object, so it can be demoted to neither. It lands on UNMEASURED
# with a cause that names the class. And NO verdict is fatal on ONE observation:
# see the discriminator at the sweep below.
#   2   usage error — and `--help` exits 2 as well, deliberately: exit 0 MEANS
#                    VERIFIED here, so a run that measured nothing must never
#                    produce it.
#
# *** A CONSUMER MUST NOT READ `UNMEASURED` AS CLEAN. ***
# That is CLAUDE.md's standing rule: never derive a pass from the absence of a bad
# signal; where the sole oracle could not be consulted the verdict is non-passing
# and its text names what was unverifiable. It is stated here and asserted by a
# test, because the shape that keeps recurring in this repo is a multi-state signal
# whose unmeasured state inherits the permissive branch.
#
# THERE IS DELIBERATELY NO KNOB THAT CAN PRODUCE `VERIFIED`. `--timeout` can only
# make the bound tighter or looser, and a tighter bound can only yield UNMEASURED;
# nothing here can be set to manufacture a clean verdict (#3312: an override is
# settable by the party it constrains).
#
# IT MUTATES NOTHING. `git fsck` is read-only, and this script writes no file, no
# ref and no config. Callers that need a throttle keep their own stamp file
# (`scripts/local/worker-supervisor.sh` does).
#
# USAGE
#   scripts/check-object-store-integrity.sh [--repo <path>] [--timeout <secs>]
#   scripts/check-object-store-integrity.sh --help
#
# CALLERS (both go through THIS script — a second implementation would be a second
# place for the verdict to drift):
#   * scripts/bootstrap-agent-machine.sh          — once, at machine onboarding
#   * scripts/local/worker-supervisor.sh          — throttled, per-iteration
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -uo pipefail

# THE GIT ENVIRONMENT IS AN ALLOWLIST, NOT A PIN-TWO-AND-HOPE (#3749 review, and
# CLAUDE.md's recorded ruling for exactly this family, roborev job 276: `env -i`
# plus ONE allowlist for every git call in a verdict-bearing probe, "precisely
# because per-site fixes kept missing new sites").
#
# THE FIRST VERSION EXPORTED TWO VARIABLES AND LET THE REST OF THE CALLER'S
# ENVIRONMENT THROUGH, AND THAT PRODUCED A FALSE `VERIFIED` NAMING THE CORRUPT
# STORE. Reproduced: `GIT_OBJECT_DIRECTORY=<good>/objects` with `--repo <bad>`
# printed `store <bad>/.git/objects` and `verdict VERIFIED`, exit 0 - every emitted
# line affirmatively false, with no signal to either consumer. `GIT_DIR` and
# `GIT_ALTERNATE_OBJECT_DIRECTORIES` reproduce variants of it. It is ACCIDENT-CLASS
# (an exported `GIT_DIR` in the shell that launched the supervisor), which the
# #3312 triage rule says to fix; and the inherited house pattern it copied
# (`base-staleness.sh`) emits a NON-FATAL ADVISORY COUNT, while this one stops a
# supervisor and gates `--strict` onboarding.
#
# So every git call in this script runs under `env -i` with the list below, which
# makes a git environment variable nobody has thought of CLEARED BY DEFAULT rather
# than needing to be discovered. ADMIT only what git needs to REACH the store;
# SET, rather than inherit, anything that decides WHICH objects it reads, WHAT it
# runs, or how it SPEAKS.
#
#   PATH                     ADMITTED. `git`, `nice` and the timeout binary are
#                            invoked by name, and the test suite's hermetic-PATH
#                            cases depend on the caller's PATH being the one in
#                            effect. It cannot redirect the sweep to another
#                            STORE - only to another git, which is the same trust
#                            as `command -v git` above.
#   LC_ALL=C                 SET. Localised diagnostics would change the text the
#                            operator is shown; the class comes from the exit
#                            status, but the evidence lines should not vary by
#                            locale.
#   GIT_NO_LAZY_FETCH=1      SET. In a partial/promisor clone an object read
#                            fetches over the network and WRITES a packfile into
#                            the store this script is auditing. Honoured from
#                            git 2.36.
#   GIT_NO_REPLACE_OBJECTS=1 SET. `refs/replace/*` substitutes objects, so a single
#                            local replacement ref could change which objects the
#                            sweep visits.
#   GIT_CONFIG_GLOBAL,       SET to /dev/null. `~/.gitconfig` and the system config
#   GIT_CONFIG_SYSTEM        can carry `fsck.*` severities, alternates and
#                            `url.*.insteadOf`; HOME is shared on this fleet, so a
#                            peer lane's edit there is not the invoker's.
#   everything else          CLEARED - notably GIT_DIR, GIT_COMMON_DIR,
#                            GIT_OBJECT_DIRECTORY, GIT_ALTERNATE_OBJECT_DIRECTORIES,
#                            GIT_INDEX_FILE, GIT_CEILING_DIRECTORIES,
#                            GIT_CONFIG_COUNT/_KEY_*/_VALUE_*, GIT_CONFIG_PARAMETERS,
#                            GIT_TEMPLATE_DIR, GIT_ALLOW_PROTOCOL, HOME.
#
# git_isolated <cmd...> - run <cmd> with exactly that environment. Used for EVERY
# git call here (the `--git-common-dir` resolution and both fsck passes), because
# an allowlist that reaches only some call sites is the hole it was written to
# close.
git_isolated() {
  "$ENV_BIN" -i \
    PATH="${PATH:-/usr/bin:/bin}" \
    LC_ALL=C \
    GIT_NO_LAZY_FETCH=1 \
    GIT_NO_REPLACE_OBJECTS=1 \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_CONFIG_SYSTEM=/dev/null \
    "$@"
}

P='OBJECT-STORE:'

# sane <string> -> the string with every C0 control character and DEL replaced by a
# VISIBLE escape, on stdout. Applied to EVERY dynamic field (property (b)).
sane() {
  local s="$1" out c i n
  s="${s//$'\r'/'\r'}"
  s="${s//$'\n'/'\n'}"
  s="${s//$'\t'/'\t'}"
  case "$s" in
    *[[:cntrl:]]*) ;;
    *)
      printf '%s' "$s"
      return 0
      ;;
  esac
  out=""
  n=${#s}
  i=0
  while [ "$i" -lt "$n" ]; do
    c="${s:i:1}"
    case "$c" in
      [[:cntrl:]]) out=$(printf '%s\\x%02x' "$out" "'$c") ;;
      *) out="$out$c" ;;
    esac
    i=$((i + 1))
  done
  printf '%s' "$out"
}

# EVERY line here is prefixed too: under property (a) the prefix is THE
# load-bearing invariant, so an unprefixed usage line is a hole in it. `${0##*/}`
# rather than `basename` — an external command whose stderr is not captured here
# would emit an unprefixed diagnostic from the one function whose job is to be
# readable when the call was wrong.
usage() {
  printf '%s USAGE - the call is wrong (this is NOT a measurement verdict)\n' "$P" >&2
  printf '%s USAGE usage: %s [--repo <path>] [--timeout <secs>]\n' \
    "$P" "$(sane "${0##*/}")" >&2
  printf '%s USAGE        %s [--repo <path>] --print-store\n' \
    "$P" "$(sane "${0##*/}")" >&2
  printf '%s USAGE Rehashes the SHARED git object store behind <path> with git fsck\n' "$P" >&2
  printf '%s USAGE and reports whether it is intact (#3749). Read-only; mutates nothing.\n' "$P" >&2
  printf '%s USAGE --print-store resolves and prints the store this run WOULD sweep\n' "$P" >&2
  printf '%s USAGE (one `store <abs-path>` line) and exits 0 WITHOUT sweeping. It is\n' "$P" >&2
  printf '%s USAGE the ONE isolated resolver callers key a throttle/latch on, so no\n' "$P" >&2
  printf '%s USAGE caller has to run its own un-isolated git to name the store.\n' "$P" >&2
  printf '%s USAGE Exits 0 verified / 4 corrupt / 5 unmeasured / 2 usage.\n' "$P" >&2
  printf '%s USAGE A CONSUMER MUST NOT READ EXIT 5 AS CLEAN (nothing was measured).\n' "$P" >&2
  printf '%s USAGE Scope is ACCIDENTAL corruption. Deliberate peer forgery is\n' "$P" >&2
  printf '%s USAGE invoker-class and OUT OF MODEL (#3749 owner ruling, #3312 triage).\n' "$P" >&2
}

# unmeasured <cause...> — exit 5. Prints NO clean signal of any kind, so it can
# never be misread as a completed sweep.
unmeasured() {
  while [ "$#" -gt 0 ]; do
    printf '%s unmeasured-cause %s\n' "$P" "$(sane "$1")"
    shift
  done
  printf '%s verdict UNMEASURED\n' "$P"
  printf '%s verdict-detail the sweep could not be performed. A CONSUMER MUST NOT READ THIS\n' "$P"
  printf '%s verdict-detail AS CLEAN (#3749); it is not a certification.\n' "$P"
  exit 5
}

# --- argument parsing: every unrecognised argument is refused ----------------
REPO="."
BOUND_SECS=600
repo_set=0
bound_set=0
PRINT_STORE=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help)
      usage
      exit 2
      ;;
    --repo)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      [ "$repo_set" -eq 0 ] || { usage; exit 2; }
      REPO="$2"
      repo_set=1
      shift 2
      ;;
    --print-store)
      # RESOLVE-AND-PRINT, NO SWEEP. See the block after the store resolution for what
      # this mode is for and why it deliberately does not require a timeout binary.
      [ "$PRINT_STORE" -eq 0 ] || { usage; exit 2; }
      PRINT_STORE=1
      shift
      ;;
    --timeout)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      [ "$bound_set" -eq 0 ] || { usage; exit 2; }
      # Validated as a POSITIVE integer, never coerced: a bare word would evaluate
      # to 0 in the bound and kill the sweep instantly, which under the
      # classification below is UNMEASURED — a silently self-disabling bound.
      case "$2" in
        '' | *[!0-9]*) usage; exit 2 ;;
      esac
      [ "$2" -ge 1 ] || { usage; exit 2; }
      BOUND_SECS="$2"
      bound_set=1
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

# --- the bound: resolve it BEFORE running anything ---------------------------
#
# THE SWEEP IS BOUNDED, AND AN UNBOUNDABLE HOST DOES NOT GET TO RUN IT. Both
# callers are hang-sensitive: an unbounded fsck can wedge machine onboarding, and
# in the supervisor it sits in the per-iteration preflight path. Refusing is
# UNMEASURED, which is non-passing, so nothing is certified on the strength of a
# probe we declined to run.
#
# The candidate is PROBED for `--kill-after` rather than sniffed by name (the
# idiom, and the reasoning, are bootstrap-agent-machine.sh's): BusyBox and older
# implementations reject the flag, and a selected binary that rejects it would make
# every bounded call fail. SIGTERM-only is ACCEPTED here — unlike a credential
# helper, `git fsck` does not trap or ignore SIGTERM — and the degradation is
# NAMED in the output rather than left silent.
#
# `--print-store` DOES NOT NEED ONE, and is exempted rather than being made to fail for
# a reason that has nothing to do with the question it asks: it runs one `rev-parse`,
# not an fsck, so "this host cannot bound a long walk" says nothing about whether the
# store can be NAMED. Making it depend on `timeout` would leave a caller keying a
# throttle on nothing on exactly the hosts where the sweep is already UNMEASURED.
TIMEOUT_BIN=""
TIMEOUT_KILL_AFTER=0
for _tb_name in timeout gtimeout; do
  [ "$PRINT_STORE" -eq 0 ] || break
  _tb_path="$(command -v "$_tb_name" 2>/dev/null || true)"
  [ -n "$_tb_path" ] || continue
  if "$_tb_path" --kill-after=1 1 true >/dev/null 2>&1; then
    TIMEOUT_BIN="$_tb_path"
    TIMEOUT_KILL_AFTER=1
    break
  fi
  [ -n "$TIMEOUT_BIN" ] || TIMEOUT_BIN="$_tb_path"
done
unset _tb_name _tb_path
BOUND_KILL_GRACE=5

command -v git >/dev/null 2>&1 ||
  unmeasured "git is not on PATH, so the object store cannot be rehashed at all"

# `env` IS A HARD DEPENDENCY BECAUSE THE ISOLATION IS (see git_isolated above), so a
# host without it is UNMEASURED rather than silently downgraded to running git in the
# caller's environment. That downgrade is exactly the defect the allowlist closes: a
# measurement taken under an inherited GIT_OBJECT_DIRECTORY can name one store and
# read another.
ENV_BIN="$(command -v env 2>/dev/null || true)"
[ -n "$ENV_BIN" ] ||
  unmeasured "env is not on PATH, so this run cannot ISOLATE git's environment - and an" \
    "fsck run under an inherited GIT_DIR/GIT_OBJECT_DIRECTORY can report about a" \
    "DIFFERENT store than the one it names. Refusing rather than measuring the wrong thing."

if [ -z "$TIMEOUT_BIN" ] && [ "$PRINT_STORE" -eq 0 ]; then
  unmeasured "no timeout/gtimeout on PATH - refusing to run an UNBOUNDED fsck: both" \
    "callers are hang-sensitive (machine onboarding, and the supervisor's" \
    "per-iteration preflight). Install GNU coreutils and re-run."
fi

# --- resolve the SHARED object store ----------------------------------------
#
# `--git-common-dir`, NEVER `--git-dir`: in a linked worktree `--git-dir` answers
# `<repo>/.git/worktrees/<lane>`, which is the LANE's private administrative
# directory, while the objects every lane on the box shares live under the COMMON
# dir (measured on this fleet: toplevel /data/lanes/lane-NNNN, common dir
# /data/lanes/repo/.git). Sweeping the per-worktree dir would audit the wrong
# thing and report VERIFIED about a store it never read.
if ! GIT_COMMON_DIR_RAW=$(git_isolated git -C "$REPO" rev-parse --git-common-dir 2>/dev/null) ||
  [ -z "$GIT_COMMON_DIR_RAW" ]; then
  unmeasured "git -C $(sane "$REPO") rev-parse --git-common-dir failed: not a git" \
    "repository, or the repository is unreadable"
fi
# The value may be RELATIVE (a plain `.git`), and it is relative to the REPO, not
# to this script's cwd.
if ! GIT_COMMON_DIR=$(cd "$REPO" 2>/dev/null && cd "$GIT_COMMON_DIR_RAW" 2>/dev/null && pwd -P); then
  GIT_COMMON_DIR=""
fi
if [ -z "$GIT_COMMON_DIR" ]; then
  unmeasured "the git common directory ($(sane "$GIT_COMMON_DIR_RAW")) could not be" \
    "canonicalized from $(sane "$REPO") - absent, unreadable, or not a directory"
fi
OBJ_DIR="$GIT_COMMON_DIR/objects"
if [ ! -d "$OBJ_DIR" ] || [ ! -r "$OBJ_DIR" ]; then
  unmeasured "the object store $(sane "$OBJ_DIR") is absent or unreadable, so there is" \
    "nothing this run can rehash"
fi

# --- `--print-store`: the ONE isolated resolver, shared with the callers ------
#
# WHY THIS MODE EXISTS (#3749 review round 2, BLOCKER 2). A caller that throttles or
# latches on the shared store has to NAME it, and naming it means resolving
# `--git-common-dir` — a git call. `scripts/local/worker-supervisor.sh` was doing that
# with a BARE `git`, inheriting the caller's environment, while this script had just
# moved every one of its own git calls under `env -i` + one allowlist. An inherited
# `GIT_DIR`/`GIT_COMMON_DIR` therefore keyed the supervisor's stamp on ANOTHER
# repository, so the real store's sweep was throttled away or its verdict recorded under
# the wrong key: the closed hole re-opened at a site the same round left behind.
#
# The remedy CLAUDE.md records for this family (roborev job 276) is one allowlist
# reaching every site, not a second copy of it. A second copy in the supervisor would be
# a second place for the list to drift, so instead the resolution has exactly ONE
# implementation — the one above, isolated — and callers ASK for it. There is then no
# un-isolated shape available to a future caller: it would have to write a git call of
# its own, which is the thing review looks for.
#
# It prints the same anchored `store <abs>` line the sweep prints, and nothing else:
# a consumer reads that one line and never has to parse a verdict that this mode, by
# construction, does not produce.
if [ "$PRINT_STORE" -eq 1 ]; then
  printf '%s store %s\n' "$P" "$(sane "$OBJ_DIR")"
  exit 0
fi

# --- scratch space (outside the repository: this script writes nothing in it) --
if ! TMPD=$(mktemp -d "${TMPDIR:-/tmp}/object-store-integrity.XXXXXX" 2>/dev/null) ||
  [ -z "$TMPD" ] || [ ! -d "$TMPD" ]; then
  unmeasured "could not create a scratch dir under $(sane "${TMPDIR:-/tmp}")"
fi
trap 'rm -rf "$TMPD" 2>/dev/null' EXIT

printf '%s store %s\n' "$P" "$(sane "$OBJ_DIR")"
printf '%s subject %s (resolved via git rev-parse --git-common-dir, NOT --git-dir)\n' \
  "$P" "$(sane "$REPO")"
if [ "$TIMEOUT_KILL_AFTER" -eq 1 ]; then
  printf '%s bound %ss (hard: SIGTERM then SIGKILL after %ss)\n' "$P" "$BOUND_SECS" "$BOUND_KILL_GRACE"
else
  printf '%s bound %ss (SIGTERM-only: %s does not accept --kill-after; git fsck does not trap SIGTERM)\n' \
    "$P" "$BOUND_SECS" "$(sane "$TIMEOUT_BIN")"
fi

# --- THE SWEEP --------------------------------------------------------------
#
# Full fsck: it REHASHES object content, which is the whole point (see the header
# on why `--connectivity-only` would be vacuous here and `--strict` would be a
# false positive). `--no-dangling` because an unreachable object is ordinary in a
# store that has held reset branches, not corruption. `--no-progress` because
# progress output is not a finding and would pollute the anchored stream.
#
# `nice`d: this is a hygiene sweep on a box that runs up to 4 gates.
#
# COST IS A RANGE WITH CONDITIONS, NOT A NUMBER (#3749 review). This file used to
# quote "19.83s" from a single warm run and derive "~15x headroom" from it; both
# were wrong. Two independent measurement sets on this fleet's shared store
# (366M, one ~220M pack, git 2.43.0) give:
#   * warm page cache, quiet box:      13-24s  (5 runs)
#   * cold-ish cache / concurrent gates: 47-80s (3 runs)
# The sweep is I/O-bound (user time is only 17-19s of an 80s wall), so it is CACHE
# STATE and box load that dominate, and any single number is a measurement of the
# machine's mood rather than of the sweep.
#
# THE 600s DEFAULT BOUND IS SIZED FROM THE TOP OF THAT RANGE: ~7.5x the observed
# cold worst case, not the ~4x that 300s would give. The bound exists to stop a
# HANG, not to police duration - an expired bound is UNMEASURED, which is a page
# nobody can act on, and a bound that fires on a healthy-but-busy box is the guard
# operators learn to waive. WORST-CASE WALL TIME IS 2x THE BOUND, because a
# non-clean first pass is re-run once (see the discriminator below); only the rare
# non-clean path pays it.
FINDING_LIST_LIMIT=40

# git fsck's exit BITMASK, from fsck.h and CONFIRMED against the git in use (2.43.0):
#   1 ERROR_OBJECT   2 ERROR_REACHABLE   4 ERROR_PACK
#   8 ERROR_REFS    16 ERROR_COMMIT_GRAPH  32 ERROR_MULTI_PACK_INDEX
# FSCK_DAMAGE_MASK is this script's subject: object content and pack integrity.
# FSCK_KNOWN_MASK is every bit it can NAME; a status carrying anything else is
# unclassified rather than guessed at (see the ordering note above fsck_pass).
# FSCK_NONMASK_FLOOR is where shell/timeout/die() statuses live and bit-testing stops
# being meaningful: 124-127 are the timeout and exec conventions, 128+N is a signal.
FSCK_DAMAGE_MASK=5
FSCK_KNOWN_MASK=63
FSCK_NONMASK_FLOOR=124

# fsck_pass <tag> - ONE bounded fsck over the shared store. Sets WALK_RC,
# WALK_ELAPSED, WALK_CLASS, WALK_NFIND and writes $TMPD/<tag>.findings (the
# recognised diagnostic lines, verbatim) and $TMPD/<tag>.ids (the 40-hex tokens in
# them).
#
# THE CLASS COMES FROM fsck's EXIT BITMASK, NOT FROM THE TEXT SHAPE OF ITS
# DIAGNOSTICS, AND THAT IS THE #3749 REVIEW'S CORRECTION. `git fsck` returns a
# bitmask: 1 ERROR_OBJECT, 2 ERROR_REACHABLE, 4 ERROR_PACK, 8 ERROR_REFS,
# 16 ERROR_COMMIT_GRAPH. The first version of this script recognised damage from
# `/^error/p` and short-circuited to the fatal branch on any hit - and `error:` is
# ALSO what fsck prints for a reflog entry naming a pruned object, which on a store
# eight lanes are concurrently writing (branch create/delete, fetch, gc) happens
# routinely on a PERFECTLY HEALTHY store. Measured on this fleet: 2 of 4 raw fsck
# runs in one sitting exited 2 with `invalid reflog entry` diagnostics naming a
# DIFFERENT branch each time. Under the old classifier every one of those was a
# `CORRUPT` that pages high, stops the supervisor and fails `--strict` bootstrap.
#
# Only bits 1 and 4 are the subject of this script (an object that failed to
# rehash, a damaged pack). The OTHER known bits are NOT demoted to clean - a
# genuinely missing object also reports ERROR_REACHABLE, so treating reachability
# as clean would convert a real corruption symptom into a pass - they land in their
# own NON-PASSING state (UNMEASURED, with a cause that names the class).
#
# THE MASK DOES NOT END AT 31, AND ASSUMING IT DID DROPPED REAL DAMAGE (#3749 review
# round 2, BLOCKER 3). git 2.43 also defines 32 ERROR_MULTI_PACK_INDEX, so the
# supported mask is 63. The previous version classified only statuses in 1..31 and
# called everything else `unclassified`, which meant 33 (32|1: a multi-pack-index
# complaint PLUS object damage) and 36 (32|4) became UNMEASURED - a FALSE NEGATIVE on
# genuine object corruption, the one direction this whole control exists to prevent.
# MEASURED on git 2.43.0 rather than reasoned from the header: a repo with a truncated
# `multi-pack-index` exits 32, and the same repo with one loose object overwritten
# exits 35 (32|2|1).
#
# THE THREE RULES BELOW, IN THIS ORDER, AND THE ORDER IS THE POINT:
#   1. A status at or above 124 IS NOT A BITMASK. 124/125/126/127 are the
#      timeout/shell conventions (this fsck runs under `timeout`), 128+N is git's
#      `die()` and signal deaths. Testing bits in that range is how `127 & 1` reads as
#      object damage. `unclassified`, never bit-tested.
#   2. THE DAMAGE BITS ARE TESTED INDEPENDENTLY, before any completeness check on the
#      rest of the status. An unrelated bit - one git added after this was written -
#      can then never MASK object or pack damage; it only travels with it.
#   3. Only then, a status carrying a bit OUTSIDE the supported mask is
#      `unclassified`. That is the safe degradation: a bit this script has never heard
#      of means something it cannot name, so it goes to a NON-PASSING state rather
#      than being folded into the class whose remedy would be wrong. Adding a bit to
#      FSCK_KNOWN_MASK is then a wording change, not a correctness fix.
fsck_pass() {
  local tag="$1" rc=0 t0 t1 el
  local out="$TMPD/$tag.out" err="$TMPD/$tag.err" all="$TMPD/$tag.all"
  t0=$(date +%s 2>/dev/null || echo 0)
  if [ "$TIMEOUT_KILL_AFTER" -eq 1 ]; then
    git_isolated nice -n 19 "$TIMEOUT_BIN" --kill-after="$BOUND_KILL_GRACE" "$BOUND_SECS" \
      git --git-dir="$GIT_COMMON_DIR" fsck --no-progress --no-dangling \
      >"$out" 2>"$err" || rc=$?
  else
    git_isolated nice -n 19 "$TIMEOUT_BIN" "$BOUND_SECS" \
      git --git-dir="$GIT_COMMON_DIR" fsck --no-progress --no-dangling \
      >"$out" 2>"$err" || rc=$?
  fi
  t1=$(date +%s 2>/dev/null || echo 0)
  el=$((t1 - t0))
  [ "$el" -ge 0 ] || el=0
  WALK_RC="$rc"
  WALK_ELAPSED="$el"
  cat "$out" "$err" >"$all" 2>/dev/null || : >"$all"

  # The recognised diagnostic shapes, kept for the OPERATOR (the class above is
  # decided by the status). Verified against git 2.43.0 on the planted fixtures the
  # test suite builds:
  #   error: inflate: data stream error ...                 (a torn/rotted object)
  #   error: <sha>: object corrupt or missing: <path>
  #   error: <sha>: hash-path mismatch, found at: <path>    (content != its own name)
  #   missing blob|tree|commit|tag <sha>
  #   broken link from ... to ...
  # Anything containing `corrupt` is included too, wherever git puts it. WARNINGS
  # ARE NOT MATCHED: `warning in commit <sha>: missingSpaceBeforeEmail` is
  # legitimate historical sloppiness and fsck exits 0 on it.
  #
  # A DIAGNOSTIC IS READ WHOLE, NOT LINE BY LINE. git permits newlines in paths and
  # quotes them verbatim, so `sed` would split such a diagnostic in two and the
  # CONTINUATION - which carries the rest of the path the operator has to act on -
  # matches no pattern and would be dropped silently (#3749 review NIT 1). So a
  # line that matches nothing is APPENDED to the previous finding when there is
  # one, and the whole thing is escaped by sane() at print time.
  : >"$TMPD/$tag.findings"
  awk '
    /^error/ || /^missing / || /^broken link/ || /corrupt/ {
      if (have) printf "%s%c", buf, 0
      buf = $0; have = 1; next
    }
    have { buf = buf "\n" $0 }
    END { if (have) printf "%s%c", buf, 0 }
  ' "$all" >"$TMPD/$tag.findings" 2>/dev/null || : >"$TMPD/$tag.findings"
  WALK_NFIND=$(tr -cd '\000' <"$TMPD/$tag.findings" 2>/dev/null | wc -c | tr -d ' ')
  case "$WALK_NFIND" in '' | *[!0-9]*) WALK_NFIND=0 ;; esac

  # The affected object ids: every 40-hex token in the findings, deduped. Extracted
  # FROM the findings so an id can never be reported without the diagnostic that
  # named it.
  : >"$TMPD/$tag.ids"
  tr -c '0-9a-f' '\n' <"$TMPD/$tag.findings" 2>/dev/null |
    awk 'length($0) == 40 { print }' | sort -u >"$TMPD/$tag.ids" 2>/dev/null || : >"$TMPD/$tag.ids"

  if [ "$rc" -eq 0 ]; then
    WALK_CLASS=clean
  elif [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    WALK_CLASS=killed
  elif [ "$rc" -ge "$FSCK_NONMASK_FLOOR" ]; then
    WALK_CLASS=unclassified
  elif [ $((rc & FSCK_DAMAGE_MASK)) -ne 0 ]; then
    WALK_CLASS=damage
  elif [ $((rc & ~FSCK_KNOWN_MASK)) -ne 0 ]; then
    WALK_CLASS=unclassified
  else
    WALK_CLASS=nondamage
  fi
}

# emit_findings <tag> <label> - print the pass's diagnostics (NUL-separated records,
# so a newline inside one is not a record boundary) and, for the fatal branch only,
# the object ids. Bounded, and the overflow is COUNTED rather than dropped.
emit_findings() {
  local tag="$1" label="$2" n=0 rec
  while IFS= read -r -d '' rec; do
    [ -n "$rec" ] || continue
    n=$((n + 1))
    [ "$n" -le "$FINDING_LIST_LIMIT" ] && printf '%s %s %s\n' "$P" "$label" "$(sane "$rec")"
  done <"$TMPD/$tag.findings"
  [ "$n" -gt "$FINDING_LIST_LIMIT" ] &&
    printf '%s %s (+%s further fsck diagnostics, not listed)\n' "$P" "$label" "$((n - FINDING_LIST_LIMIT))"
  return 0
}

# --- PASS 1 ------------------------------------------------------------------
fsck_pass p1
C1="$WALK_CLASS"
RC1="$WALK_RC"
EL1="$WALK_ELAPSED"
N1="$WALK_NFIND"

# 124 = SIGTERM'd at the bound; 137 = it ignored SIGTERM and --kill-after escalated
# to SIGKILL. A KILLED SWEEP IS UNMEASURED, NEVER VERIFIED: it exited without having
# rehashed the rest of the store, and its silence up to that point is the absence of
# a bad signal, not a clean answer. No second pass: a killed pass says nothing to
# reproduce, and re-running would double the wall time of the one case that is
# already too slow.
if [ "$C1" = killed ]; then
  unmeasured "the fsck exceeded its ${BOUND_SECS}s bound and was killed (rc=$RC1) after" \
    "${EL1}s - it never finished rehashing the store, so its silence is NOT a" \
    "clean result. Re-run with a larger --timeout on an idle box."
fi

if [ "$C1" = clean ]; then
  FIRST_WALK_CLEAN=1
else
  FIRST_WALK_CLEAN=0
fi

# --- PASS 2: THE DISCRIMINATOR ----------------------------------------------
#
# WHY A SECOND WALK, AND WHAT IT IS FOR. This control CANNOT DISTINGUISH A
# CONCURRENCY-INDUCED TRANSIENT FROM REAL DAMAGE IN A SINGLE OBSERVATION: the store
# is being written by up to 8 peer lanes while fsck walks it, so a ref that vanishes
# mid-walk, a pack being replaced by gc, or a reflog entry naming a just-pruned
# object all produce diagnostics on a healthy store. What separates the two is
# REPRODUCTION: a transient does not survive a second independent walk, and real
# damage does. So the fatal verdict is affirmative - it rests on the SAME class
# being observed twice - rather than on one observation plus an argument.
#
# IT IS DELIBERATELY NOT A RETRY-UNTIL-CLEAN LOOP. Exactly one re-run, and its
# result can only make the verdict weaker or confirm it, never sweep it away: a
# damage class seen once and not the second time is NOT clean either, it is
# UNMEASURED (a flickering corruption signal is not something to certify).
#
# It is nearly free: only the non-clean path pays a second bound, and on this fleet
# that path is the exception.
if [ "$FIRST_WALK_CLEAN" -eq 0 ]; then
  printf '%s measured pass 1: fsck rc=%s in %ss over %s (full rehash: not --connectivity-only)\n' \
    "$P" "$RC1" "$EL1" "$(sane "$OBJ_DIR")"
  printf '%s note pass 1 was not clean (class=%s, rc=%s, %s diagnostic(s)); re-running once\n' \
    "$P" "$C1" "$RC1" "$N1"
  printf '%s note the second walk is a DISCRIMINATOR, not a retry: a concurrent writer on\n' "$P"
  printf '%s note this shared store produces diagnostics that do not survive a second walk,\n' "$P"
  printf '%s note while real damage does. It is never re-run until it comes back clean.\n' "$P"
  emit_findings p1 note
  fsck_pass p2
  printf '%s measured pass 2: fsck rc=%s in %ss over %s (full rehash: not --connectivity-only)\n' \
    "$P" "$WALK_RC" "$WALK_ELAPSED" "$(sane "$OBJ_DIR")"
  C2="$WALK_CLASS"
  RC2="$WALK_RC"
  EL2="$WALK_ELAPSED"
  N2="$WALK_NFIND"

  if [ "$C2" = killed ]; then
    unmeasured "pass 1 was not clean (class=$C1, rc=$RC1) and the confirming pass exceeded" \
      "its ${BOUND_SECS}s bound (rc=$RC2) after ${EL2}s, so the first observation could" \
      "not be confirmed or dismissed. Re-run with a larger --timeout on an idle box."
  fi

  # BOTH PASSES SAW OBJECT/PACK DAMAGE: the fatal branch, and the only path to it.
  if [ "$C1" = damage ] && [ "$C2" = damage ]; then
    emit_findings p2 finding
    while IFS= read -r oid; do
      [ -n "$oid" ] || continue
      printf '%s object %s\n' "$P" "$(sane "$oid")"
    done <"$TMPD/p2.ids"
    printf '%s measured fsck rc=%s then rc=%s (%ss + %ss) over %s\n' \
      "$P" "$RC1" "$RC2" "$EL1" "$EL2" "$(sane "$OBJ_DIR")"
    printf '%s verdict CORRUPT\n' "$P"
    printf '%s verdict-detail %s fsck diagnostic(s) name damaged or unhashable objects in the\n' "$P" "$N2"
    printf '%s verdict-detail SHARED store, on TWO independent walks (fsck exit bits 1/4 both\n' "$P"
    printf '%s verdict-detail times), so this is damage and not a concurrent writer. Every lane on\n' "$P"
    printf '%s verdict-detail this box reads it, so it can change ANY gate verdict here: do NOT\n' "$P"
    printf '%s verdict-detail certify anything against this checkout.\n' "$P"
    printf '%s verdict-detail REMEDY: stop the lanes on this box, then re-obtain the objects from the\n' "$P"
    printf '%s verdict-detail canonical remote (a fresh clone of pmcfadin/cqlite, or\n' "$P"
    printf '%s verdict-detail `git fetch --force origin` if the damage is confined to fetched packs).\n' "$P"
    printf '%s verdict-detail A LOCAL `git gc`/`git repack` CANNOT REPAIR THIS - it rewrites the same\n' "$P"
    printf '%s verdict-detail damaged content, or refuses. Escalate rather than improvising (#3749).\n' "$P"
    exit 4
  fi

  # A DAMAGE CLASS IN EXACTLY ONE PASS: non-passing, and NOT the fatal branch. It is
  # neither established damage (it did not reproduce) nor a clean store (something
  # named an object as unhashable once).
  if [ "$C1" = damage ] || [ "$C2" = damage ]; then
    emit_findings p2 unmeasured-cause
    unmeasured "an object/pack damage class (fsck exit bit 1 or 4) was observed in ONE of two" \
      "walks and did not reproduce (pass 1 class=$C1 rc=$RC1, pass 2 class=$C2 rc=$RC2)." \
      "That is neither established damage nor a clean store: re-run on an IDLE box," \
      "and if it recurs treat the store as suspect and escalate (#3749)."
  fi

  # A STATUS THIS SCRIPT CANNOT READ AS A BITMASK, in either pass. Its own cause,
  # because the nondamage text below would name a class that was never established:
  # a `die()` or an exec failure says nothing about reachability, and describing it as
  # a reflog problem would send the operator to the wrong remedy.
  if [ "$C1" = unclassified ] || [ "$C2" = unclassified ]; then
    emit_findings p2 unmeasured-cause
    unmeasured "git fsck exited with a status this script cannot read as its error" \
      "bitmask (pass 1 class=$C1 rc=$RC1, pass 2 class=$C2 rc=$RC2). The bits it" \
      "supports are 1|2|4|8|16|32; a status carrying anything else is a die(), an" \
      "exec failure, or a git newer than this classifier. Nothing about the store's" \
      "object content was established, so this is NOT clean. Run the fsck by hand to" \
      "see what it said."
  fi

  # BOTH PASSES NON-CLEAN, NEITHER OBJECT/PACK DAMAGE: reachability, refs,
  # commit-graph or multi-pack-index complaints that survived a second walk. Its OWN
  # non-passing state with its OWN cause text - never the fatal branch (this is not
  # the class this script exists for) and never VERIFIED (a genuinely MISSING object
  # reports exactly this, so reading it as clean would hide real damage). No `object`
  # lines: the 40-hex tokens in a reflog diagnostic name INTACT objects.
  if [ "$C2" != clean ]; then
    emit_findings p2 unmeasured-cause
    unmeasured "git fsck reported reachability/ref/commit-graph/multi-pack-index problems on BOTH walks" \
      "(pass 1 class=$C1 rc=$RC1, pass 2 class=$C2 rc=$RC2) and NO object or pack" \
      "damage. This script's subject is object content, and that question is therefore" \
      "not answered: a missing object reports the same class. Inspect the diagnostics" \
      "above; a stale reflog clears with 'git reflog expire --expire-unreachable=now" \
      "--all', a stale multi-pack-index with 'git multi-pack-index write', and a" \
      "genuinely absent object with neither."
  fi

  # PASS 2 CLEAN, and pass 1 carried no damage class: the first observation did not
  # reproduce. The store gets the affirmative verdict below on the strength of the
  # SECOND walk (a complete rehash that found nothing), and the non-reproducing
  # observation is RECORDED rather than swallowed.
  printf '%s note the pass-1 diagnostics did NOT reproduce on the second walk (pass 2 rc=0),\n' "$P"
  printf '%s note which is the signature of a concurrent writer on this shared store rather\n' "$P"
  printf '%s note than damage. The verdict below rests on the second walk, which completed.\n' "$P"
fi

# --- THE ONE AFFIRMATIVE BRANCH --------------------------------------------
#
# VERIFIED REQUIRES EVIDENCE THE SWEEP RAN AND COMPLETED, not merely that nothing
# bad was printed. The evidence is the fsck PROCESS's OWN exit status 0, and it is
# affirmative for a stated reason: the process ran under a bound whose kill
# statuses (124/137) are distinguishable and are routed to UNMEASURED above, and an
# fsck that could not start, could not read the store or died on a signal cannot
# produce 0. `git fsck` exits 0 only after walking and REHASHING every object it
# was asked to check. So `rc == 0` here means "it finished, and it found nothing" -
# two facts, not one - while every state in which the first fact is unknown has
# already been routed to UNMEASURED.
printf '%s measured fsck rc=0 in %ss over %s (full rehash: not --connectivity-only)\n' \
  "$P" "$WALK_ELAPSED" "$(sane "$OBJ_DIR")"
printf '%s verdict VERIFIED\n' "$P"
printf '%s verdict-detail git fsck ran to completion and reported no damaged objects.\n' "$P"
printf '%s verdict-detail SCOPE: this is a POINT-IN-TIME sweep of ACCIDENTAL corruption, not a\n' "$P"
printf '%s verdict-detail per-read guarantee and not a defence against deliberate forgery, which\n' "$P"
printf '%s verdict-detail is invoker-class and out of model (#3749 owner ruling, #3312 triage).\n' "$P"
exit 0
