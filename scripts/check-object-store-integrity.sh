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

# `--no-reflogs` IS USED, ON THE THIRD WALK ONLY, AND NEVER TO SUPPRESS A COMPLAINT
# (#3749 review round 4, item 1). Using it on the SWEEP was proposed in round 1 as a
# way to make the intermittent reflog false positive go away, MEASURED, and REJECTED
# by the lead — the concurrency transients hit that form too, so it bought nothing and
# would have hidden a real signal. Using it as a DISCRIMINATOR for a complaint that has
# ALREADY reproduced twice is a different operation with the opposite effect: it does
# not decide whether to report, it decides WHICH CAUSE to report, and its answer can
# only make the verdict STRONGER (UNMEASURED -> CORRUPT). Passes 1 and 2 — the sweep
# proper — never carry it, and that is asserted structurally by the test suite.
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
#   4   CORRUPT    — damage established on TWO independent walks. Either OBJECT/PACK
#                    damage (fsck exit bits 1/4), or an object MISSING from the
#                    reachability walk that is still reachable with reflogs EXCLUDED
#                    (bit 2, see the reachability discriminator below). The findings
#                    are named.
#   5   UNMEASURED — the answer was not obtained: no git, no resolvable object
#                    store, no usable timeout binary, the bound expired, an fsck
#                    failure this script cannot classify, a damage class that did
#                    NOT reproduce, or a reachability/ref/commit-graph/multi-pack-index
#                    complaint that reproduced but could not be ATTRIBUTED.
#
# A REACHABILITY COMPLAINT HAS TWO CAUSES AND THEY GET DIFFERENT VERDICTS (#3749
# review round 4, item 1). fsck's exit bit 2 (ERROR_REACHABLE) fires for BOTH:
#   * a stale reflog entry naming an object a peer lane's gc has pruned — routine on
#     a store eight lanes write, and NOT this script's subject; and
#   * an object that is genuinely MISSING while a LIVE ref, the index or HEAD still
#     needs it — which IS corruption of the shared store, in the one direction this
#     whole control exists to prevent.
# Reading the class as "unmeasurable" for both was a FALSE NEGATIVE on real damage:
# UNMEASURED is deliberately non-fatal to the supervisor's loop, so workers kept
# running against a demonstrably damaged store. The two are separated by a THIRD walk
# with `--no-reflogs`: a complaint that survives with the reflogs excluded from the
# reachability roots is not a reflog artefact, and it is CORRUPT. One that clears
# stays UNMEASURED with the reflog remedy. And NO verdict is fatal on ONE
# observation: see the discriminator at the sweep below.
#
# WHAT THE SWEEP'S REACHABILITY ROOTS ACTUALLY ARE, MEASURED RATHER THAN ASSUMED
# (#3749 review round 7). `git --git-dir=<common> fsck` does NOT discard linked
# worktrees: measured on git 2.43.0, with the object removed and the complaint checked
# in both directions, it DOES walk every registered worktree's private `HEAD` (it names
# it `worktrees/<name>/HEAD`), its private INDEX (`missing blob <sha>`), and the HEAD of
# a PRUNABLE worktree whose working directory has been deleted - and all three survive
# `--no-reflogs`, so a missing object needed by one reaches the CORRUPT branch above.
# It does NOT walk a LINKED worktree's per-worktree REFS (`refs/worktree/*`,
# `refs/bisect/*`, `refs/rewritten/*`); those are roots only for an fsck run with THAT
# worktree's own git dir. Measured consequence, and it is the one this control exists to
# prevent: delete an object named only by `refs/worktree/private` in a linked worktree
# and the sweep exits **0 VERIFIED** (the worktree's HEAD reflog echoes the id, and that
# echo CLEARS under `--no-reflogs`, so even the attribution walk reads it as
# reflog-scoped). The MAIN worktree's per-worktree refs live in the common dir itself and
# ARE walked. That gap is closed by the PRIVATE-ROOT PROBE below - a separate, cheap
# question asked before the affirmative branch - and the coverage above is PINNED by the
# test suite rather than believed, so a future git that narrows fsck's worktree
# enumeration reds a case instead of silently shrinking this control.
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

# --- THE BOX-WIDE KEY FOR THIS STORE ----------------------------------------
#
# WHY THE KEY IS COMPUTED HERE AND NOT BY THE CALLER (#3749 review round 5, item 3).
# A caller that throttles or latches on the shared store needs a filesystem-safe,
# INJECTIVE name for it. Round 4 made that name injective over the FLATTENING —
# `<sanitised tail>.<16 hex of sha256(path)>`, because replacing `/` with `_` maps
# `/tmp/a/b/objects` and `/tmp/a_b/objects` onto one name — and then fed the digest the
# value THIS script had already passed through `sane()`. That is a DISPLAY encoding and
# it is LOSSY: a path containing a real newline and one containing the two literal
# characters `\n` render identically, so two different stores shared a throttle stamp AND
# a CORRUPT latch — one suppressing the other's sweep, or stopping every lane on the box
# with the other store's damage. `sane()` exists so a control character cannot break the
# anchored output; it was never reversible and was never an identity.
#
# So the digest is taken from the RAW canonical path, in the one place that has it, and
# the caller receives a finished key. There is then no lossy value left for a caller to
# digest — the same "remove the shape, not the site" move as the isolated resolver below:
# the non-injective form is UNAVAILABLE rather than discouraged.
#
# store_digest <value> -> 16 lowercase hex chars of sha256(value), or nothing (exit 1) on
# a host with no usable digest tool. THREE TOOLS because the two platforms this file
# supports ship different ones: `sha256sum` (GNU coreutils), `shasum -a 256` (macOS, via
# perl), `openssl dgst -sha256` (both). `cksum` is present everywhere and is deliberately
# NOT used: CRC32 is not collision-resistant, and a colliding key is the defect being
# removed. The output is parsed from BOTH ENDS and then VALIDATED AS HEX — first field for
# the coreutils/perl tools, last for openssl (`SHA2-256(stdin)= <hex>` on 3.x, `(stdin)=
# <hex>` on 1.1.x) — so a tool whose output shape is neither FAILS CLOSED instead of
# contributing a garbage key.
store_digest() {
  local v="$1" out='' hex=''
  if command -v sha256sum >/dev/null 2>&1; then
    out=$(printf '%s' "$v" | sha256sum 2>/dev/null || true)
  elif command -v shasum >/dev/null 2>&1; then
    out=$(printf '%s' "$v" | shasum -a 256 2>/dev/null || true)
  elif command -v openssl >/dev/null 2>&1; then
    out=$(printf '%s' "$v" | openssl dgst -sha256 2>/dev/null || true)
  else
    return 1
  fi
  for hex in "${out%% *}" "${out##* }"; do
    case "$hex" in
      '' | *[!0-9a-f]*) continue ;;
    esac
    [ "${#hex}" -ge 32 ] || continue
    printf '%s' "${hex:0:16}"
    return 0
  done
  return 1
}

# store_key <raw-store-path> -> `<sanitised tail>.<digest>`, or nothing (exit 1).
#
# THE TAIL IS READABILITY ONLY AND CARRIES NO IDENTITY: an operator reading `ls /tmp`
# should be able to tell which store a stamp belongs to, while the DIGEST is what makes two
# stores two files. It is the TAIL because the distinguishing part of these paths is the
# end, and the length is checked explicitly because `${v: -40}` yields the EMPTY STRING —
# not the whole value — when the value is shorter than the offset.
store_key() {
  local raw="$1" tail='' digest=''
  [ -n "$raw" ] || return 1
  tail=$(printf '%s' "$raw" | tr -c 'A-Za-z0-9._-' '_')
  [ -n "$tail" ] || return 1
  if [ "${#tail}" -gt 40 ]; then
    tail="${tail:${#tail}-40}"
  fi
  digest=$(store_digest "$raw") || return 1
  case "$digest" in
    '' | *[!0-9a-f]*) return 1 ;;
  esac
  [ "${#digest}" -eq 16 ] || return 1
  printf '%s.%s' "$tail" "$digest"
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
  printf '%s USAGE        %s [--repo <path>] --probe-private-roots\n' \
    "$P" "$(sane "${0##*/}")" >&2
  printf '%s USAGE Rehashes the SHARED git object store behind <path> with git fsck\n' "$P" >&2
  printf '%s USAGE and reports whether it is intact (#3749). Read-only; mutates nothing.\n' "$P" >&2
  printf '%s USAGE --print-store resolves and prints the store this run WOULD sweep\n' "$P" >&2
  printf '%s USAGE (one `store <abs-path>` line) and exits 0 WITHOUT sweeping. It is\n' "$P" >&2
  printf '%s USAGE the ONE isolated resolver callers key a throttle/latch on, so no\n' "$P" >&2
  printf '%s USAGE caller has to run its own un-isolated git to name the store.\n' "$P" >&2
  printf '%s USAGE --probe-private-roots asks the ONE question a common-dir fsck cannot:\n' "$P" >&2
  printf '%s USAGE are the objects named by LINKED worktrees per-worktree refs present?\n' "$P" >&2
  printf '%s USAGE It sweeps nothing and prints `private-root` lines plus one terminal\n' "$P" >&2
  printf '%s USAGE `private-roots` census line. The sweep runs it as a bounded child.\n' "$P" >&2
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
PROBE_PRIVATE_ROOTS=0
# THE PATH THIS SCRIPT RE-INVOKES FOR THE PRIVATE-ROOT PROBE. Captured ONCE, before
# anything can change the working directory, because `$0` is resolved against the cwd in
# effect when bash started. Nothing here cd's on the main path (the store canonicalisation
# cd's inside a `$( )` subshell), so this stays valid - but capturing it is cheaper than
# depending on that staying true.
SELF="$0"
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
      [ "$PROBE_PRIVATE_ROOTS" -eq 0 ] || { usage; exit 2; }
      PRINT_STORE=1
      shift
      ;;
    --probe-private-roots)
      # THE PER-WORKTREE PRIVATE-ROOT PROBE, AS ITS OWN CHILD MODE. It is a mode rather
      # than an inline loop for ONE reason: the whole probe has to be BOUNDED, and a
      # bound needs a single child process to wrap. Inlining it would mean either an
      # UNBOUNDED sequence of git calls in a script whose header says an unboundable host
      # does not get to run it (measured: those calls took 27.9s on this box at load 132),
      # or a shell body passed as a string to `bash -c`, which is neither reviewable nor
      # directly testable. As a mode the body is ordinary code, the suite can drive it
      # head-on, and the sweep pays exactly one bounded stage for it.
      [ "$PROBE_PRIVATE_ROOTS" -eq 0 ] || { usage; exit 2; }
      [ "$PRINT_STORE" -eq 0 ] || { usage; exit 2; }
      PROBE_PRIVATE_ROOTS=1
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
  [ "$PRINT_STORE" -eq 0 ] && [ "$PROBE_PRIVATE_ROOTS" -eq 0 ] || break
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

if [ -z "$TIMEOUT_BIN" ] && [ "$PRINT_STORE" -eq 0 ] && [ "$PROBE_PRIVATE_ROOTS" -eq 0 ]; then
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
# It prints the same anchored `store <abs>` line the sweep prints, plus the box-wide
# `store-key` a caller keys its throttle and latch on — and no verdict, which this mode by
# construction does not produce.
#
# TWO LINES, AND THE SECOND ONE IS THE IDENTITY (#3749 review round 5, item 3). The `store`
# line is passed through `sane()` for the anchored-output invariant, which makes it a
# DISPLAY value and NOT injective: a caller that digested it keyed two different stores onto
# one stamp and one CORRUPT latch. `store-key` is derived from the RAW path here (see
# store_key above) so the caller never has to reconstruct an identity from a rendering. A
# host with no digest tool prints NO key line at all — the caller then has no box-wide key,
# which it announces and handles, rather than being handed a name two stores could share.
if [ "$PRINT_STORE" -eq 1 ]; then
  printf '%s store %s\n' "$P" "$(sane "$OBJ_DIR")"
  if STORE_KEY=$(store_key "$OBJ_DIR"); then
    printf '%s store-key %s\n' "$P" "$STORE_KEY"
  fi
  exit 0
fi

# --- `--probe-private-roots`: THE ROOTS A COMMON-DIR fsck DOES NOT HAVE -------
#
# THE QUESTION, AND WHY IT IS A DIFFERENT ONE FROM THE SWEEP'S. The sweep REHASHES every
# object PRESENT in the store, so object CONTENT is answered store-wide and is not a
# per-worktree question at all. The only worktree-dependent half is PRESENCE: an object
# that is absent cannot be rehashed, and is found only by walking from a root that needs
# it. Measured (header above): a common-dir fsck already walks every worktree's HEAD,
# index and reflogs, and MISSES a LINKED worktree's per-worktree refs. So this probe asks
# exactly the missing half and nothing else.
#
# IT IS NOT AN fsck AND MUST NEVER BECOME ONE. Three measurements rule out the obvious
# shapes, and they are recorded so they are not re-derived:
#   * `git fsck <sha>...` REPLACES the default heads - a missing blob reachable from HEAD
#     went UNDETECTED (rc 0) when an unrelated head was passed - so the private roots can
#     never be appended to the sweep walk;
#   * `git fsck <sha>` still scans the whole object directory, so a separate explicit-head
#     fsck costs a FULL rehash, which is the N-fsck design the #3749 lead ruling rejected;
#   * `git rev-list <missing-root>` dies 128 (`bad object`), so `--missing=print` cannot
#     answer the case that actually fires - the ROOT itself being gone.
# What it does instead is O(refs): list each linked worktree's refs, subtract the common
# ones, and ask `cat-file --batch-check` whether the remainder's targets are in the store.
# Measured on the live 15-worktree store: 0.02s warm.
#
# THE ENUMERATION IS FILESYSTEM-FIRST **BECAUSE THE GIT COMMAND IS FAIL-OPEN**, which is
# the opposite of what one would assume. Measured: `git worktree list --porcelain`
# SILENTLY DROPS a worktree whose admin `gitdir` file is missing - rc 0, no diagnostic,
# the worktree simply absent - so a git-only enumeration answers "clean" about a worktree
# it could not see. `$GIT_COMMON_DIR/worktrees/*` is not a guess about this fleet's
# layout, it is git's OWN administrative directory, and it is a SUPERSET of what the
# command reports. The command is still run, as a CROSS-CHECK in the other direction:
# if it names more linked worktrees than the directory holds, something is being hidden
# from us and that is UNREADABLE, never clean.
#
# WHAT IT DOES NOT ASK, STATED RATHER THAN IMPLIED: the CLOSURE of a private root. It
# asks whether each private ref's TARGET is present, not whether every object in that
# target's history is. An object missing DEEPER in a per-worktree ref's ancestry, and
# named by no common ref, no worktree HEAD, no index and no reflog, is not recognised
# here. That residual is declared rather than closed because closing it needs a graph
# walk from roots whose ancestry is normally shared with the common refs anyway, and the
# walk would be a second bounded stage (see MAX_SWEEP_WALKS).

# _prp_observe <records-file> <census-file> - ONE observation. Writes TAB-separated
# records, one per line:
#     ABSENT<TAB><sha><TAB><worktree><TAB><refname>
#     UNREADABLE<TAB><worktree><TAB><cause>
# and `<roots> <linked-worktrees>` to the census file. Returns 0 always: every failure it
# can name is a record, because a worktree that cannot be inspected is NOT a clean one.
_prp_observe() {
  local out="$1" census="$2"
  local commonrefs="$PRP_TMPD/common.refs" pairs="$PRP_TMPD/wt.pairs"
  local names="$PRP_TMPD/wt.names" priv="$PRP_TMPD/wt.priv" cand="$PRP_TMPD/cand"
  local bc="$PRP_TMPD/bc" miss="$PRP_TMPD/miss"
  local wtdir name nwt=0 nroot=0 gitwt=0
  : >"$out"
  : >"$cand"
  printf '0 0\n' >"$census"

  if ! git_isolated git --git-dir="$GIT_COMMON_DIR" for-each-ref --format='%(refname)' \
    >"$commonrefs" 2>/dev/null; then
    printf 'UNREADABLE\t(common)\tthe common ref list could not be read, so no ref can be told apart from a per-worktree one\n' >>"$out"
    return 0
  fi
  LC_ALL=C sort -u "$commonrefs" -o "$commonrefs" 2>/dev/null ||
    printf 'UNREADABLE\t(common)\tthe common ref list could not be sorted for comparison\n' >>"$out"

  for wtdir in "$GIT_COMMON_DIR"/worktrees/*; do
    [ -d "$wtdir" ] || continue
    nwt=$((nwt + 1))
    name="${wtdir##*/}"
    # A CONTROL CHARACTER IN AN ADMIN NAME IS REFUSED, NOT ESCAPED. These records are
    # line- and TAB-delimited and are INTERSECTED between the two observations, so the
    # name is an IDENTITY here and not a display value (#3749 round 5: a value sanitised
    # for display is not an identity). Refusing keeps every name that is compared RAW.
    case "$name" in
      *[[:cntrl:]]* | *"$(printf '\t')"*)
        printf 'UNREADABLE\t%s\tthe worktree admin directory name carries a control character, so its private refs cannot be reported on one anchored line\n' \
          "$(sane "$name")" >>"$out"
        continue
        ;;
    esac
    if [ ! -r "$wtdir" ] || [ ! -x "$wtdir" ]; then
      printf 'UNREADABLE\t%s\tthe worktree admin directory is not readable/searchable\n' "$name" >>"$out"
      continue
    fi
    if ! git_isolated git --git-dir="$wtdir" for-each-ref --format='%(refname) %(objectname)' \
      >"$pairs" 2>/dev/null; then
      printf 'UNREADABLE\t%s\tits ref list could not be read (git for-each-ref failed against its admin dir)\n' "$name" >>"$out"
      continue
    fi
    # `%(objectname)` is the REF VALUE and reads no object; `%(objecttype)` would, and
    # dies 128 on exactly the missing object this probe exists to find (measured).
    awk 'NF >= 2 { print $1 }' "$pairs" 2>/dev/null | LC_ALL=C sort -u >"$names" 2>/dev/null || : >"$names"
    LC_ALL=C comm -13 "$commonrefs" "$names" >"$priv" 2>/dev/null || : >"$priv"
    awk -v wt="$name" 'NR == FNR { p[$0] = 1; next }
      (NF >= 2 && ($1 in p)) { printf "%s\t%s\t%s\n", $2, wt, $1 }' \
      "$priv" "$pairs" >>"$cand" 2>/dev/null || true
  done

  # THE CROSS-CHECK IN THE FAIL-OPEN DIRECTION (see the block comment above).
  if gitwt=$(git_isolated git --git-dir="$GIT_COMMON_DIR" worktree list --porcelain 2>/dev/null |
    awk '/^worktree /{n++} END{print n + 0}'); then
    case "$gitwt" in '' | *[!0-9]*) gitwt=0 ;; esac
    # minus the main worktree, which `worktree list` always names first.
    [ "$gitwt" -ge 1 ] && gitwt=$((gitwt - 1))
    if [ "$gitwt" -gt "$nwt" ]; then
      printf 'UNREADABLE\t(common)\tgit names %s linked worktree(s) but only %s admin directory(ies) exist under the common dir - one is being hidden from this probe\n' \
        "$gitwt" "$nwt" >>"$out"
    fi
  else
    printf 'UNREADABLE\t(common)\tthe worktree list could not be read as a cross-check on the admin directories\n' >>"$out"
  fi

  nroot=$(awk 'END{print NR + 0}' "$cand" 2>/dev/null || echo 0)
  case "$nroot" in '' | *[!0-9]*) nroot=0 ;; esac
  printf '%s %s\n' "$nroot" "$nwt" >"$census"
  [ "$nroot" -gt 0 ] || return 0

  # ONE batch presence question for every private root. `cat-file --batch-check` prints
  # `<sha> missing` for an object the store does not hold and exits 0 (measured).
  if ! cut -f1 "$cand" 2>/dev/null | LC_ALL=C sort -u |
    git_isolated git --git-dir="$GIT_COMMON_DIR" cat-file --batch-check='%(objectname) %(objecttype)' \
      >"$bc" 2>/dev/null; then
    printf 'UNREADABLE\t(common)\tthe presence check (git cat-file --batch-check) could not be run over %s private root(s)\n' \
      "$nroot" >>"$out"
    return 0
  fi
  awk '$2 == "missing" { print $1 }' "$bc" 2>/dev/null | LC_ALL=C sort -u >"$miss" 2>/dev/null || : >"$miss"
  awk -F'\t' 'NR == FNR { m[$0] = 1; next }
    ($1 in m) { printf "ABSENT\t%s\t%s\t%s\n", $1, $2, $3 }' "$miss" "$cand" >>"$out" 2>/dev/null || true
  return 0
}

# probe_private_roots - TWO independent observations, INTERSECTED, then printed.
#
# THE SECOND OBSERVATION IS THE SAME DISCRIMINATOR THE SWEEP USES, FOR THE SAME REASON.
# Up to eight peer lanes write this store: a lane running `git worktree remove`, or
# deleting a bisect ref, between the enumeration and the presence check makes a root look
# absent when nothing is wrong, and a worktree whose admin dir is being torn down looks
# unreadable for a second. A transient does not survive a second independent
# enumeration; real damage does. Both observations RE-ENUMERATE from scratch - reusing
# the first one's root list would make the second observation answer a stale question.
#
# BOTH OBSERVATIONS RUN INSIDE THIS ONE CHILD PROCESS, which is what keeps the sweep's
# worst case at MAX_SWEEP_WALKS bounded stages: two bounded children would have been a
# fourth stage, and every caller's bound is derived from that number.
probe_private_roots() {
  local o1="$PRP_TMPD/o1" o2="$PRP_TMPD/o2" c1="$PRP_TMPD/c1" c2="$PRP_TMPD/c2"
  local both="$PRP_TMPD/both" nabs=0 nunr=0 nroot=0 nwt=0 kind f1 f2 f3
  _prp_observe "$o1" "$c1"
  _prp_observe "$o2" "$c2"
  LC_ALL=C sort -u "$o1" -o "$o1" 2>/dev/null || : >"$o1"
  LC_ALL=C sort -u "$o2" -o "$o2" 2>/dev/null || : >"$o2"
  LC_ALL=C comm -12 "$o1" "$o2" >"$both" 2>/dev/null || : >"$both"

  while IFS="$(printf '\t')" read -r kind f1 f2 f3; do
    case "$kind" in
      ABSENT)
        nabs=$((nabs + 1))
        printf '%s private-root ABSENT %s %s %s\n' "$P" "$(sane "$f1")" "$(sane "$f2")" "$(sane "$f3")"
        ;;
      UNREADABLE)
        nunr=$((nunr + 1))
        printf '%s private-root UNREADABLE %s %s\n' "$P" "$(sane "$f1")" "$(sane "$f2")"
        ;;
    esac
  done <"$both"

  # The census reports the SECOND observation's counts: it is the one whose enumeration
  # is current at the moment the answer is given.
  read -r nroot nwt <"$c2" 2>/dev/null || { nroot=0; nwt=0; }
  case "$nroot" in '' | *[!0-9]*) nroot=0 ;; esac
  case "$nwt" in '' | *[!0-9]*) nwt=0 ;; esac
  # THE TERMINAL CENSUS LINE IS THE COMPLETENESS MARKER, and the caller requires EXACTLY
  # ONE of them: a probe that died halfway prints records and no census, which is
  # UNMEASURED rather than "nothing was found" (the `.started` idiom, one function over).
  printf '%s private-roots checked=%s worktrees=%s absent=%s unreadable=%s (two observations, intersected)\n' \
    "$P" "$nroot" "$nwt" "$nabs" "$nunr"
  return 0
}

if [ "$PROBE_PRIVATE_ROOTS" -eq 1 ]; then
  if ! PRP_TMPD=$(mktemp -d "${TMPDIR:-/tmp}/object-store-private-roots.XXXXXX" 2>/dev/null) ||
    [ -z "$PRP_TMPD" ] || [ ! -d "$PRP_TMPD" ]; then
    unmeasured "could not create a scratch dir under $(sane "${TMPDIR:-/tmp}") for the" \
      "private-root probe"
  fi
  trap 'rm -rf "$PRP_TMPD" 2>/dev/null' EXIT
  probe_private_roots
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
# operators learn to waive. WORST-CASE WALL TIME IS MAX_SWEEP_WALKS x THE BOUND: a
# non-clean first pass is re-run once (the discriminator below), and a REPRODUCED
# reachability complaint costs one further walk to attribute (#3749 review round 4).
# Only those paths pay it; a clean store takes exactly one walk.
#
# A CALLER MAY WANT A TIGHTER BOUND THAN THIS DEFAULT, AND ONE DOES. This bound is a
# property of the WALK; how long a caller may block is a property of the CALLER.
# scripts/local/worker-supervisor.sh passes a smaller number rather than accepting 600,
# because the sweep runs inside a child process and nothing in that supervisor can read
# its stop file BETWEEN the walks - so its worst case of MAX_SWEEP_WALKS walks is
# deliberately capped at one walk's worth of this default (#3749 review round 3, widened
# for the third walk in round 4). Machine onboarding (bootstrap-agent-machine.sh) keeps
# 600: nobody is waiting on a stop file there. If you change this number, or
# MAX_SWEEP_WALKS, the supervisor's own default is asserted to stay at or below
# BOUND_SECS / MAX_SWEEP_WALKS, so that relation reds rather than drifting silently.
FINDING_LIST_LIMIT=40

# MAX_SWEEP_WALKS - the most bounded CHILD STAGES one invocation can spend, and it is
# declared here as a NUMBER A CALLER CAN READ because the supervisor's own bound is
# derived from it by an asserted relation rather than by someone remembering. Raising it
# raises every caller's worst-case uninterruptible block.
#   1. the sweep;
#   2. the reproduction discriminator, when walk 1 was not clean;
#   3. EITHER the reachability-CAUSE discriminator (`--no-reflogs`, when walks 1 and 2
#      both reported ERROR_REACHABLE and neither reported damage) OR the PRIVATE-ROOT
#      PROBE - never both, and that is why adding the probe did not raise this number.
#
# WHY STAGE 3 IS AN EITHER/OR AND NOT A SUM (#3749 review round 7). Every branch of the
# reachability block exits (CORRUPT, or one of three UNMEASURED causes), so a run that
# spends walk 3 TERMINATES THERE and never reaches the private-root probe; and the probe
# sits immediately before the ONE affirmative branch, which is reachable only when pass 2
# was clean. So the worst case is max(3 fsck walks, 2 fsck walks + 1 probe) = 3 bounded
# stages, unchanged - no caller's bound moves. That is a PLACEMENT property, not an
# arithmetic one, so it is asserted BEHAVIOURALLY by the suite (the 3-walk fixtures must
# emit no `private-root` line at all); moving the probe earlier would silently widen
# every caller's uninterruptible window.
MAX_SWEEP_WALKS=3

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
# ERROR_REACHABLE alone. It is NOT in FSCK_DAMAGE_MASK because the bit does not by
# itself say which of its two causes fired (a pruned object named by a reflog, or an
# object a LIVE ref still needs); the third walk is what attributes it.
FSCK_REACHABLE_BIT=2

# fsck_pass <tag> [--no-reflogs] - ONE bounded fsck over the shared store. Sets
# WALK_RC, WALK_ELAPSED, WALK_CLASS, WALK_NFIND and writes $TMPD/<tag>.findings (the
# recognised diagnostic lines, verbatim) and $TMPD/<tag>.ids (the 40-hex tokens in
# them).
#
# THE SECOND ARGUMENT IS A CLOSED SET OF ONE, AND AN UNRECOGNISED VALUE IS A REFUSAL,
# not a silently ignored typo: `--no-reflogs` belongs to the reachability-CAUSE
# discriminator (walk 3) and to nothing else. Passes 1 and 2 - the sweep proper - pass
# no mode at all, so a future edit that widened the reachability walk into the sweep
# would have to change a call site the test suite reads structurally.
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
# RULE 0, AND IT COMES BEFORE ALL OF THEM: A STATUS IS ONLY READ AS A BITMASK IF WE
# ESTABLISHED, AFFIRMATIVELY, THAT fsck WAS ACTUALLY LAUNCHED AND ITS OUTPUT CAPTURED
# (#3749 review round 3). The shell's status space overlaps fsck's, and a failed
# capture redirection exits 1 - fsck's ERROR_OBJECT bit. See the launch block inside
# fsck_pass: marker absent => WALK_CLASS=launchfail, routed to UNMEASURED by the
# callers and never bit-tested.
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
  local started="$TMPD/$tag.started"
  # The argv is built as an ARRAY (never an unquoted string), so an empty mode cannot
  # word-split and the array is never empty - `"${fargs[@]}"` under `set -u` on bash 3.2
  # is only safe for a non-empty array, and the two base flags guarantee that.
  local -a fargs
  fargs=(--no-progress --no-dangling)
  case "${2:-}" in
    '') ;;
    --no-reflogs) fargs=("${fargs[@]}" --no-reflogs) ;;
    *)
      unmeasured "internal: fsck_pass was called with an unsupported walk mode" \
        "$(sane "${2:-}") - refusing rather than running a walk whose configuration" \
        "this script cannot describe in its own verdict (#3749)."
      ;;
  esac
  rm -f "$started" 2>/dev/null || true
  t0=$(date +%s 2>/dev/null || echo 0)
  # THE LAUNCH IS WRAPPED SO THAT "fsck RAN AND RETURNED A STATUS" CAN BE TOLD APART
  # FROM "WE NEVER GOT AS FAR AS RUNNING IT" (#3749 review round 3, item 2).
  #
  # THE DEFECT THIS EXISTS FOR. The two capture redirections are part of the command,
  # so if opening `$out` or `$err` FAILS - a full scratch filesystem, a `$TMPDIR`
  # reaped by a tmp cleaner mid-run, an unwritable scratch dir - bash never execs
  # anything and the status is **1**. 1 is also fsck's ERROR_OBJECT bit. The
  # classifier below then reads that 1 as object damage, BOTH passes fail the same
  # way, both "reproduce", and the sweep emits **CORRUPT** on a store it never opened.
  # That is round 1's BLOCKER B - a false CORRUPT on a healthy box - coming back
  # through a different door: this time the borrowed bit comes from the shell, not
  # from a concurrent writer.
  #
  # IT CANNOT BE INFERRED FROM THE STATUS, WHICH IS THE WHOLE POINT: fsck's status
  # space and the shell's overlap, so no value of `rc` distinguishes them. The
  # evidence is AFFIRMATIVE instead, and it is the `.started` marker:
  #   * the marker is written INSIDE the redirected group, as its FIRST statement, so
  #     it exists only if bash established BOTH capture redirections - a redirection
  #     error on a compound command means the body does not execute at all (verified
  #     on bash 5.2: the group's status is 1 and nothing inside it runs);
  #   * `$out` and `$err` are required to EXIST afterwards for the same reason.
  # So marker present => the capture was established and control reached the fsck
  # invocation; marker absent => we could not launch or could not capture, and the
  # status is NOT a bitmask. The latter becomes WALK_CLASS=launchfail, which is routed
  # to UNMEASURED by its callers and is never bit-tested.
  #
  # WHAT THIS DOES NOT PROVE, STATED RATHER THAN IMPLIED: the marker proves the
  # redirections took and the group ran, not that `env`/`nice`/`timeout`/`git` were
  # successfully exec'd. That residual is already covered from the other side - an
  # exec failure exits 126/127, which is at or above FSCK_NONMASK_FLOOR and therefore
  # `unclassified` (UNMEASURED), never damage.
  #
  # `2>/dev/null` PRECEDES the capture redirections deliberately (the NIT-4 lesson,
  # one file over): bash applies redirections LEFT TO RIGHT and reports a failure on
  # the stderr in effect at that moment, so without it a failed `>"$out"` would print
  # bash's own UNANCHORED error and break output property (a) for every consumer.
  if [ "$TIMEOUT_KILL_AFTER" -eq 1 ]; then
    (
      true >"$started"
      git_isolated nice -n 19 "$TIMEOUT_BIN" --kill-after="$BOUND_KILL_GRACE" "$BOUND_SECS" \
        git --git-dir="$GIT_COMMON_DIR" fsck "${fargs[@]}"
    ) 2>/dev/null >"$out" 2>"$err" || rc=$?
  else
    (
      true >"$started"
      git_isolated nice -n 19 "$TIMEOUT_BIN" "$BOUND_SECS" \
        git --git-dir="$GIT_COMMON_DIR" fsck "${fargs[@]}"
    ) 2>/dev/null >"$out" 2>"$err" || rc=$?
  fi
  t1=$(date +%s 2>/dev/null || echo 0)
  el=$((t1 - t0))
  [ "$el" -ge 0 ] || el=0
  WALK_RC="$rc"
  WALK_ELAPSED="$el"

  # ASKED BEFORE ANYTHING ELSE READS `rc`, and before any file under $TMPD is written:
  # on this path the scratch dir may be exactly what is broken, so an unguarded
  # `: >"$TMPD/..."` here would itself emit an unanchored bash error. The callers go
  # straight to `unmeasured` on this class without calling emit_findings, so the
  # findings/ids files are deliberately not created.
  if [ ! -f "$started" ] || [ ! -f "$out" ] || [ ! -f "$err" ]; then
    WALK_NFIND=0
    WALK_CLASS=launchfail
    return 0
  fi

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

# LAUNCH/CAPTURE FAILURE: fsck NEVER RAN, so there is nothing to classify and NO SECOND
# PASS (a walk that did not happen has nothing to reproduce, and the second attempt
# would fail on the same broken scratch dir). This is the one branch whose status is
# deliberately NOT read as a bitmask - it is the shell's, not fsck's.
if [ "$C1" = launchfail ]; then
  unmeasured "the fsck could not be LAUNCHED, or its output could not be CAPTURED (pass 1," \
    "shell status $RC1): the scratch capture files under $(sane "$TMPD") were not" \
    "established, so NO fsck status was produced. That status is the SHELL's, and a" \
    "failed redirection exits 1 - which is also fsck's ERROR_OBJECT bit, so reading it" \
    "as a bitmask would report DAMAGE about a store this run never opened (#3749)." \
    "Usual cause: the scratch filesystem is full, unwritable, or was reaped mid-run." \
    "Free space under $(sane "${TMPDIR:-/tmp}") and re-run."
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

  # Same rule on the confirming pass: an unlaunchable second walk confirms and dismisses
  # nothing, and its shell status must not join the bitmask reasoning below.
  if [ "$C2" = launchfail ]; then
    unmeasured "pass 1 was not clean (class=$C1, rc=$RC1) and the confirming pass could not" \
      "be LAUNCHED or CAPTURED (shell status $RC2, not an fsck status), so the first" \
      "observation was neither confirmed nor dismissed. Usual cause: the scratch" \
      "filesystem under $(sane "${TMPDIR:-/tmp}") is full, unwritable, or was reaped" \
      "mid-run. Clear it and re-run (#3749)."
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

  # --- PASS 3: THE REACHABILITY-CAUSE DISCRIMINATOR -------------------------
  #
  # THE FALSE NEGATIVE THIS EXISTS FOR (#3749 review round 4, item 1). ERROR_REACHABLE
  # has two causes and only one of them is benign. A stale reflog entry naming an object
  # a peer lane pruned is routine on this store; an object that is MISSING while a LIVE
  # ref, the index or HEAD still needs it is corruption of exactly the kind this control
  # exists to catch. Both reproduce across walks 1 and 2, so the reproduction
  # discriminator cannot tell them apart, and both used to land on UNMEASURED - which is
  # NON-FATAL to the supervisor's loop by design. So a demonstrably damaged store kept
  # spawning workers, reported as "not measured".
  #
  # HOW THEY ARE SEPARATED. `git fsck --no-reflogs` drops the reflogs from the
  # REACHABILITY ROOTS and keeps everything else, so a complaint that survives it is
  # reachable from a live root. Measured on git 2.43.0, both directions, on real
  # fixtures the suite builds:
  #   * a blob deleted while HEAD's tree still names it: rc 2 WITH reflogs and rc 2
  #     WITHOUT them -> live-reachable, and CORRUPT;
  #   * a commit deleted after `reset --hard`, so only the reflog names it: rc 2 WITH
  #     reflogs and rc 0 WITHOUT -> reflog-scoped, and it stays where it was.
  #
  # IT IS NOT A THIRD OPINION ON WHETHER TO REPORT. The class has already reproduced
  # twice when this runs; this walk decides WHICH CAUSE, and it can only make the
  # verdict stronger. Nothing here can turn a reported complaint into VERIFIED.
  if [ "$C1" = nondamage ] && [ "$C2" = nondamage ] &&
    [ $((RC1 & FSCK_REACHABLE_BIT)) -ne 0 ] && [ $((RC2 & FSCK_REACHABLE_BIT)) -ne 0 ]; then
    printf '%s note both walks report ERROR_REACHABLE (fsck exit bit 2) and NO object/pack\n' "$P"
    printf '%s note damage. That bit covers a stale reflog entry AND an object a live ref\n' "$P"
    printf '%s note still needs, so it is attributed by a third walk with --no-reflogs:\n' "$P"
    printf '%s note a complaint that survives with the reflogs excluded is reachable from a\n' "$P"
    printf '%s note LIVE root and is damage; one that clears was reflog-scoped.\n' "$P"
    fsck_pass p3 --no-reflogs
    C3="$WALK_CLASS"
    RC3="$WALK_RC"
    EL3="$WALK_ELAPSED"
    N3="$WALK_NFIND"
    printf '%s measured pass 3: fsck --no-reflogs rc=%s in %ss over %s (reachability attribution)\n' \
      "$P" "$RC3" "$EL3" "$(sane "$OBJ_DIR")"

    # THE ATTRIBUTION FAILED, so the cause is UNKNOWN - and an unattributed reachability
    # complaint must not be read as either verdict. `killed`, `launchfail` and
    # `unclassified` all mean the third walk produced no usable answer; each keeps the
    # reproduced complaint visible and says which walk went wrong.
    if [ "$C3" = killed ] || [ "$C3" = launchfail ] || [ "$C3" = unclassified ]; then
      emit_findings p2 unmeasured-cause
      unmeasured "git fsck reported reachability problems on BOTH walks (pass 1 rc=$RC1," \
        "pass 2 rc=$RC2) and the confirming --no-reflogs walk produced no usable answer" \
        "(class=$C3 rc=$RC3), so the cause could NOT be attributed: a stale reflog entry" \
        "and an object a LIVE ref still needs report the same bit, and only the second is" \
        "damage. This is NOT clean. Re-run on an idle box; if the third walk keeps failing," \
        "run 'git fsck --no-reflogs' by hand and read what it says (#3749)."
    fi

    # A DAMAGE BIT THAT SHOWS UP FOR THE FIRST TIME IN THE THIRD WALK has not reproduced
    # either: the two sweep walks both said there was none. Same rule as the one-pass
    # damage branch above - neither established damage nor a clean store.
    if [ $((RC3 & FSCK_DAMAGE_MASK)) -ne 0 ]; then
      emit_findings p3 unmeasured-cause
      unmeasured "an object/pack damage class (fsck exit bit 1 or 4) appeared ONLY in the" \
        "third walk (pass 1 rc=$RC1, pass 2 rc=$RC2, pass 3 --no-reflogs rc=$RC3), so it" \
        "did not reproduce across the two sweep walks. That is neither established damage" \
        "nor a clean store: re-run on an IDLE box, and if it recurs treat the store as" \
        "suspect and escalate (#3749)."
    fi

    # THE FATAL BRANCH FOR THIS CLASS: reproduced across walks 1 and 2, and STILL present
    # with the reflogs excluded. Something a live ref, the index or HEAD needs is not in
    # the store.
    if [ $((RC3 & FSCK_REACHABLE_BIT)) -ne 0 ]; then
      emit_findings p3 finding
      printf '%s measured fsck rc=%s then rc=%s then rc=%s --no-reflogs (%ss + %ss + %ss) over %s\n' \
        "$P" "$RC1" "$RC2" "$RC3" "$EL1" "$EL2" "$EL3" "$(sane "$OBJ_DIR")"
      printf '%s verdict CORRUPT\n' "$P"
      printf '%s verdict-detail %s fsck diagnostic(s) name objects MISSING from this box'"'"'s SHARED\n' "$P" "$N3"
      printf '%s verdict-detail store, on THREE walks - and they are still missing with the reflogs\n' "$P"
      printf '%s verdict-detail EXCLUDED from the reachability roots, so a live ref, the index or HEAD\n' "$P"
      printf '%s verdict-detail needs them. That is not a stale reflog and not a concurrent writer.\n' "$P"
      printf '%s verdict-detail Every lane on this box reads this store, so it can change ANY gate\n' "$P"
      printf '%s verdict-detail verdict here: do NOT certify anything against this checkout.\n' "$P"
      printf '%s verdict-detail REMEDY: stop the lanes on this box, then re-obtain the objects from the\n' "$P"
      printf '%s verdict-detail canonical remote (`git fetch --force origin`, or a fresh clone of\n' "$P"
      printf '%s verdict-detail pmcfadin/cqlite). An object that only ever existed locally - an\n' "$P"
      printf '%s verdict-detail unpushed commit - cannot be re-obtained: escalate rather than\n' "$P"
      printf '%s verdict-detail improvising. `git reflog expire` is the remedy for the OTHER cause of\n' "$P"
      printf '%s verdict-detail this bit and does nothing here; a local `git gc`/`git repack` cannot\n' "$P"
      printf '%s verdict-detail repair it either, and may prune what is left (#3749).\n' "$P"
      # NO `object` lines here, deliberately, and the reason is not the same as the reflog
      # branch's. A `broken link from <A> to <B>` diagnostic names the INTACT source as
      # well as the absent target, so labelling every 40-hex token an affected `object`
      # would be a false claim about half of them. The diagnostics above name them in
      # context, which is what the operator needs.
      exit 4
    fi

    # THE COMPLAINT CLEARED WITH REFLOGS EXCLUDED: reflog-scoped. It stays exactly where
    # it was before this discriminator existed - NON-PASSING, with the reflog remedy.
    #
    # WHY IT IS NOT PROMOTED TO VERIFIED, since walks 1 and 2 did rehash every object and
    # reported no damage bit: the affirmative verdict in this script means "an fsck in the
    # configuration this sweep uses ran to completion and found nothing", and that did not
    # happen. A reflog naming an absent object can also be the SHADOW of an object that
    # went missing while nothing live needed it any more, which is a fact about this store
    # an operator is entitled to see. Deriving a pass from a NARROWED question is the
    # shape CLAUDE.md warns about.
    emit_findings p2 unmeasured-cause
    unmeasured "git fsck reported reachability problems on both sweep walks (pass 1 rc=$RC1," \
      "pass 2 rc=$RC2) and the ERROR_REACHABLE bit CLEARED when the reflogs were excluded" \
      "from the reachability roots (pass 3 --no-reflogs rc=$RC3, that bit absent): the" \
      "reachability complaint is REFLOG-SCOPED, so nothing a live ref, the index or HEAD" \
      "needs is missing, and this is NOT the damage class. Any other non-damage bit in" \
      "pass 3's status is named by the diagnostics above." \
      "It is not certified clean either - an fsck in this sweep's own configuration did" \
      "not complete quietly. Clear it with 'git reflog expire --expire-unreachable=now" \
      "--all' on this box's shared repository, then re-run (#3749)."
  fi

  # WHAT IS LEFT: both passes non-clean, neither carrying object/pack damage, and NOT
  # the reproduced-reachability shape the third walk above attributes. So this is a
  # ref, commit-graph or multi-pack-index complaint, or a reachability bit that appeared
  # in only ONE of the two walks (in which case the class itself did not reproduce and
  # there is nothing for walk 3 to attribute). Its OWN non-passing state with its OWN
  # cause text - never the fatal branch (these are not the class this script exists
  # for) and never VERIFIED (the object-content question is not what these bits answer).
  # No `object` lines: the 40-hex tokens in these diagnostics name INTACT objects.
  if [ "$C2" != clean ]; then
    emit_findings p2 unmeasured-cause
    unmeasured "git fsck reported ref/commit-graph/multi-pack-index problems, or a reachability" \
      "complaint that did not reproduce identically, on BOTH walks (pass 1 class=$C1" \
      "rc=$RC1, pass 2 class=$C2 rc=$RC2) and NO object or pack damage. This script's" \
      "subject is object content, and that question is therefore not answered. Inspect" \
      "the diagnostics above; a stale reflog clears with 'git reflog expire" \
      "--expire-unreachable=now --all', a stale multi-pack-index with 'git" \
      "multi-pack-index write', and a genuinely absent object with neither - if an" \
      "object IS absent, re-run: a reachability bit on both walks is attributed" \
      "(#3749)."
  fi

  # PASS 2 CLEAN, and pass 1 carried no damage class: the first observation did not
  # reproduce. The store gets the affirmative verdict below on the strength of the
  # SECOND walk (a complete rehash that found nothing), and the non-reproducing
  # observation is RECORDED rather than swallowed.
  printf '%s note the pass-1 diagnostics did NOT reproduce on the second walk (pass 2 rc=0),\n' "$P"
  printf '%s note which is the signature of a concurrent writer on this shared store rather\n' "$P"
  printf '%s note than damage. The verdict below rests on the second walk, which completed.\n' "$P"
fi

# --- THE PRIVATE-ROOT GATE ON `VERIFIED` ------------------------------------
#
# THE LAST QUESTION BEFORE THE ONLY AFFIRMATIVE VERDICT, and it is placed here for two
# reasons that are both load-bearing. (1) SEMANTICS: `VERIFIED` may not rest on a rehash
# alone when a whole class of roots was not walked, so the probe gates the affirmative
# branch rather than sitting beside it. (2) THE BOUND: every branch of the reachability
# block above exits, so a run that spent a third fsck walk never arrives here - the worst
# case stays at MAX_SWEEP_WALKS bounded child stages and no caller's derived bound moves
# (see MAX_SWEEP_WALKS). Moving this call earlier would silently widen every caller's
# uninterruptible window, which is why the suite asserts the placement behaviourally.
#
# IT IS RUN AS A BOUNDED CHILD OF THIS SAME SCRIPT, under the same isolation and the same
# timeout machinery as an fsck walk: the body is a mode of this file (see
# `--probe-private-roots`), so it is ordinary reviewable code that the suite drives
# head-on, while the caller still pays exactly one boundable process for it.
PRP_OUT="$TMPD/prp.out"
prp_rc=0
if [ ! -r "$SELF" ]; then
  unmeasured "the private-root probe could not be run: this script's own path" \
    "($(sane "$SELF")) is not readable, so the per-worktree roots a common-dir fsck does" \
    "NOT walk (a linked worktree's refs/worktree, refs/bisect, refs/rewritten) were not" \
    "checked. The rehash said nothing about them, so this run is NOT clean (#3749)."
fi
if [ "$TIMEOUT_KILL_AFTER" -eq 1 ]; then
  git_isolated nice -n 19 "$TIMEOUT_BIN" --kill-after="$BOUND_KILL_GRACE" "$BOUND_SECS" \
    bash "$SELF" --repo "$REPO" --probe-private-roots >"$PRP_OUT" 2>&1 || prp_rc=$?
else
  git_isolated nice -n 19 "$TIMEOUT_BIN" "$BOUND_SECS" \
    bash "$SELF" --repo "$REPO" --probe-private-roots >"$PRP_OUT" 2>&1 || prp_rc=$?
fi

# THE TWO CHANNELS MUST AGREE BEFORE EITHER IS ACTED ON (#3749 review round 4, applied to
# the channel this round adds): the child's EXIT STATUS and its terminal census line are
# two independent signals, and a run is only complete when BOTH say so. Exactly one
# census line is required - the `store-key` contract idiom - so a child that died halfway
# through printing records is UNMEASURED and never "nothing was found".
PRP_CENSUS=""
prp_complete=0
if PRP_CENSUS=$(awk '/^OBJECT-STORE: private-roots /{ line = $0; n++ }
  END { if (n == 1) { print line; exit 0 } exit 1 }' "$PRP_OUT" 2>/dev/null); then
  prp_complete=1
fi
PRP_ABSENT=$(awk '/^OBJECT-STORE: private-root ABSENT /{ n++ } END { print n + 0 }' "$PRP_OUT" 2>/dev/null)
PRP_UNREAD=$(awk '/^OBJECT-STORE: private-root UNREADABLE /{ n++ } END { print n + 0 }' "$PRP_OUT" 2>/dev/null)
case "$PRP_ABSENT" in '' | *[!0-9]*) PRP_ABSENT=0 ;; esac
case "$PRP_UNREAD" in '' | *[!0-9]*) PRP_UNREAD=0 ;; esac

# Its own lines are already anchored, and ONLY anchored lines are re-emitted: a child that
# died could put an unanchored shell diagnostic on this stream, and output property (a) is
# what every consumer and every test rests on.
awk '/^OBJECT-STORE: private-root/ { print }' "$PRP_OUT" 2>/dev/null || true

if [ "$prp_rc" -eq 124 ] || [ "$prp_rc" -eq 137 ]; then
  unmeasured "the private-root probe exceeded its ${BOUND_SECS}s bound (rc=$prp_rc), so the" \
    "per-worktree roots a common-dir fsck does NOT walk were not checked. The rehash" \
    "above says nothing about them. Re-run with a larger --timeout on an idle box."
fi
if [ "$prp_complete" -eq 0 ] || [ "$prp_rc" -ne 0 ]; then
  unmeasured "the private-root probe did not complete (child rc=$prp_rc," \
    "census line $([ "$prp_complete" -eq 1 ] && printf 'present' || printf 'ABSENT')): its" \
    "exit status and its terminal census line must AGREE before either is read, and they" \
    "do not. The per-worktree roots a common-dir fsck does NOT walk (a linked worktree's" \
    "refs/worktree, refs/bisect, refs/rewritten) were therefore not checked, so the rehash" \
    "above is not a clean answer about them. Run" \
    "'bash $(sane "${0##*/}") --probe-private-roots' by hand to see what it said (#3749)."
fi
if [ "$PRP_ABSENT" -gt 0 ]; then
  printf '%s measured %s\n' "$P" "$(sane "${PRP_CENSUS#"$P "}")"
  printf '%s verdict CORRUPT\n' "$P"
  printf '%s verdict-detail %s object(s) named by a LINKED WORKTREE'"'"'s per-worktree ref are\n' "$P" "$PRP_ABSENT"
  printf '%s verdict-detail NOT in this box'"'"'s SHARED store, on TWO independent enumerations.\n' "$P"
  printf '%s verdict-detail A common-dir `git fsck` does not walk those refs (it walks every\n' "$P"
  printf '%s verdict-detail worktree'"'"'s HEAD, index and reflogs, and the COMMON refs), so the rehash\n' "$P"
  printf '%s verdict-detail above reported nothing about them: this is exactly the missing-object\n' "$P"
  printf '%s verdict-detail class the reachability discriminator treats as damage, found at a root\n' "$P"
  printf '%s verdict-detail that walk never had. Every lane on this box reads this store, so do NOT\n' "$P"
  printf '%s verdict-detail certify anything against this checkout.\n' "$P"
  printf '%s verdict-detail REMEDY: stop the lanes on this box and re-obtain the objects from the\n' "$P"
  printf '%s verdict-detail canonical remote (`git fetch --force origin`, or a fresh clone of\n' "$P"
  printf '%s verdict-detail pmcfadin/cqlite). If the ref is a leftover (`refs/bisect/*` from an\n' "$P"
  printf '%s verdict-detail abandoned bisect, a tool'"'"'s `refs/worktree/*`) and the object only ever\n' "$P"
  printf '%s verdict-detail existed locally, DELETE THE REF in that worktree rather than improvising\n' "$P"
  printf '%s verdict-detail on the store - but establish which it is first (#3749).\n' "$P"
  exit 4
fi
if [ "$PRP_UNREAD" -gt 0 ]; then
  unmeasured "$PRP_UNREAD registered worktree(s) could not be INSPECTED for their" \
    "per-worktree refs, on both observations (see the private-root lines above). A" \
    "worktree this probe cannot read is not a clean one: a common-dir fsck does not walk" \
    "those refs, so nothing in this run establishes whether the objects they name are" \
    "present. Fix the named admin directory under $(sane "$GIT_COMMON_DIR")/worktrees" \
    "(or prune the worktree with 'git worktree prune') and re-run (#3749)."
fi
printf '%s measured %s\n' "$P" "$(sane "${PRP_CENSUS#"$P "}")"

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
