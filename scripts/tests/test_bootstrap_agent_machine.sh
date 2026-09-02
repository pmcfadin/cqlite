#!/usr/bin/env bash
# Regression test for scripts/bootstrap-agent-machine.sh (issue #1921).
#
# Covers the PURE-CHECK paths only: the bootstrap must, in its default mode
# (no --yes), run all its checks and NEVER install anything — it may only print
# install commands. Fast by design: runs with --skip-smoke so it never invokes
# the multi-minute gate.
#
# Run standalone:   bash scripts/tests/test_bootstrap_agent_machine.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BOOTSTRAP="$SCRIPT_DIR/../bootstrap-agent-machine.sh"

PASS=0
FAIL=0
SKIPS=0
# Every case name that actually REPORTED. Case 15 reads this rather than re-deriving what
# "should" have run: the question is whether a case executed, and the only evidence of that
# is that it announced a verdict.
PIN_RAN_CASES=""
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); PIN_RAN_CASES="$PIN_RAN_CASES
$1"; }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); PIN_RAN_CASES="$PIN_RAN_CASES
$1"; }
# SKIPS ARE NEITHER PASS NOR FAIL, so they must be COUNTED and REPORTED (issue #3414
# review B1). Three end-to-end cases silently became skips when section 5b started adding
# one warning to every sandbox, and the suite still said FAIL=0 — including the case whose
# own comment records that its absence "let a defect through with 102 tests green". A
# total that cannot distinguish "ran and passed" from "did not run" is the proxy-for-a-fact
# shape this issue exists to remove, so the tail now prints SKIP= alongside PASS/FAIL.
# Deliberately NOT a failure in itself: some skips here are honest reports about the HOST
# (no timeout that accepts --kill-after), and a suite that reds on correct input is one
# agents learn to waive. The DRIFT that silently disables cases is caught at its root
# instead, by the base_warns assertion in block 7p.
skip() { printf 'skip - %s\n' "$1"; SKIPS=$((SKIPS + 1)); }

# out_has <text> <grep-args...>: a SIGPIPE-SAFE text predicate, and the reason it exists is
# MEASURED (issue #3727): under `set -o pipefail`, `out_has "$big" PAT` returns
# **141** once the payload exceeds the 64 KiB pipe buffer, because grep -q exits at the first match
# and printf's next write dies — 64 KiB rc=0, 128 KiB rc=141, with the match present in both. A
# full bootstrap run's output crossed that line when section 5b2 was added, which turned PASSING
# cases into failures whose own debug output showed the matching text. A here-string is not a
# pipeline, so grep's own status is the answer and pipefail has nothing to override.
#
# NO `grep -q` PIPELINE PREDICATE REMAINS IN THIS FILE (issue #3727 roborev round 8, f1). The
# round-5 pass converted only sites whose argument was a SIMPLE VARIABLE, and I judged the survivors
# — `"$(scc_slice …)"`, a `git config` read, two two-stage chains, one tiny literal — safe because
# their payloads were small. That judgement was the wrong SHAPE of argument: "demonstrably bounded"
# is a claim about a payload that grows every round, and it had already been falsified once in this
# very file (the four instances that fired did so BECAUSE the output grew). The corrected mechanism
# makes it worse than a size argument: `grep -q` CLOSES the pipe at the first match, so the failure
# is a RACE above bash's ~4 KiB stdio chunk, not a 64 KiB threshold. Conversion is free; a
# measurement that has to be redone whenever the output changes is not. So there is nothing left to
# bound: every predicate reads through out_has.
#
# THE WHOLE FILE IS CONVERTED, and it was done because leaving it declared did not hold: three
# separate cases fired this way across four runs as the output grew (7p-b2 — RED on pristine
# origin/main, 7p-k, 12b-k, then the section-presence loop's CQLITE_DATASETS_ROOT), each reporting
# the opposite of what it measured. 286 `printf … | grep -q` predicates and 2 `push_plain … |
# grep -q` ones route through here, and since round 8 so do the last stragglers (a `git config`
# read, two two-stage `push_plain | grep -E | grep -q` chains, two `$(scc_slice …)` payloads and one
# tiny literal). NOTHING is left "on purpose": the earlier exemption was a size argument about a
# payload that grows every round, and the corrected mechanism is not about size at all — `grep -q`
# CLOSES the pipe at its first match, so the failure is a RACE above bash's ~4 KiB stdio chunk. If a
# new predicate is added, use out_has: the pipeline form is a latent false verdict, not a style
# preference.
out_has() { local __t="$1"; shift; grep -q "$@" <<< "$__t"; }

# --- THIS SUITE IS NOT RUNNABLE AS ROOT, AND SAYS SO UP FRONT (#3414 roborev round 7) --
# OUR OWN REGRESSION, and the second time these three cases have been silently disabled.
# Round 5 made the test seam refuse under EUID 0 (finding S, a real privilege-escalation
# hole). But the refusal is a [warn], and it fires for EVERY sandboxed invocation, so under
# root `base_warns` becomes 2, the baseline assertion below fails, and the three green-path
# cases print `skip` — the exact three that round 2's finding was about. We unskipped them,
# then re-skipped them four rounds later by fixing something else.
#
# Refused UP FRONT rather than per-case: the alternative is threading privilege-dropping
# through test setup, which is how the seam came to need a root guard in the first place. A
# suite that declares "not runnable as root" is honest and cheap; one that runs 190 cases
# of which an unpredictable subset are silently meaningless is neither.
#
# COUNTED, never an `ok` — round 2's finding, and this file now has a hygiene case that
# forbids announcing a skip through ok() anyway.
#
# AND IT IS THE FIRST THING THAT RUNS, BEFORE `mktemp -d` AND BEFORE ANY CASE (#3414
# roborev round 8, lead caution). The manual check that this suite declines under root is
# itself run with `sudo`, so anything executing above the decline would run AS ROOT — and
# a single line up there that resolves CARGO_HOME from /etc/environment, or writes outside
# its own tmpdir, would make the check for a safety property the thing that violates it.
# That is not hypothetical: it is what happened 40 minutes earlier with the cargo config.
# Even the tmpdir matters — created as root it is litter the invoking user cannot remove.
# So the decline precedes everything, and the only host contact above it is now none.
#
# AND IT ASKS BASH, NOT `id` (#3414 roborev round 9). The production guard was moved to the
# readonly $EUID because `id` can be missing, shadowed or malformed; this decline is a
# SAFETY gate, so the same argument applies with more force — an `id` that fails here does
# not merely misreport, it lets the suite continue AS ROOT past the one check whose purpose
# is to stop exactly that. $EUID is set by the shell itself, needs no PATH lookup and no
# fork, and cannot be shadowed. A missing or non-numeric value FAILS CLOSED: unable to tell
# is not permission to proceed.
pin_suite_euid="${EUID-}"
case "$pin_suite_euid" in
  ''|*[!0-9]*)
    printf 'bad  - THE ENTIRE SUITE: cannot determine the effective UID (EUID=%s). Refusing to run: this suite must not execute as root, and a shell that cannot answer that question cannot be trusted to have declined.\n' "${pin_suite_euid:-<unset>}"
    echo
    echo "PASS=$PASS FAIL=$((FAIL + 1)) SKIP=$SKIPS"
    echo "DECLINED: EUID is unavailable, so root could not be ruled out. Run under bash." >&2
    exit 1 ;;
esac
if [ "$pin_suite_euid" = 0 ]; then
  skip "THE ENTIRE SUITE: it drives bootstrap through a test seam that is REFUSED under root (#3414 finding S), so every sandboxed invocation gains a warning and the green-path assertions cannot run. Re-run as an unprivileged user."
  echo
  echo "PASS=$PASS FAIL=$FAIL SKIP=$SKIPS"
  # NONZERO, and this is the THIRD LEVEL at which this same shape has been caught (#3414
  # roborev round 8). Round 2 found a CASE counted as a pass; round 4 found `ok "SKIP …"`;
  # round 7's fix for those turned it into a SUITE-level version — the gate checks only
  # exit status, so `exit 0` here reported `tooling-tests` green having executed nothing at
  # all, including every regression this branch added. A declined suite is not a passing
  # suite. The gate runs as an unprivileged user, so the normal path never reaches this and
  # the cost is zero; running it as root becomes a loud failure instead of a silent green.
  echo "DECLINED: this suite executed NOTHING and is exiting NONZERO so no caller can read it as a pass." >&2
  exit 1
fi

# The sandbox and the shared-state guard are created BEFORE the first case, because the
# guard wraps every bootstrap invocation and case 2 (`--help`) is one of them. Creating it
# later left that invocation calling an undefined "$PIN_BS" — coverage that reads as
# complete while one call site silently is not, which is the shape this guard exists for.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bootstrap-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Baseline for the hermeticity assertion at the end of the suite. Captured as
# mode+mtime+content so a rewrite is caught even if it happens to restore the bytes.
# Named only for the diagnostic below; never stat'd or hashed. The GNU-only `stat -c` /
# `md5sum` snapshot that used to live here is gone with the shared-state observation it
# served (#3414 roborev round 11) — those probes yield empty fields on macOS, where this
# suite runs as part of a MANDATORY gate component.
PIN_SHARED_CARGO=/usr/local/cargo/config.toml

# --- ATTRIBUTABLE, NOT MERELY DETECTED (#3414 roborev round 10) ------------------------
# The tripwire used to snapshot the shared file around the WHOLE suite, so ANY concurrent
# writer — a peer lane's bootstrap, an administrator, or a human at a prompt — made it FAIL
# claiming this suite mutated the file. That is not hypothetical: the lead ran bootstrap by
# hand mid-session and moved that file's mode and mtime; had the suite been running, its
# tripwire would have blamed itself for someone else's shell.
#
# NOT weakened to a notice — a guard that cannot red cannot catch the reintroduced reach it
# exists for. Made ATTRIBUTABLE instead: every bootstrap invocation this suite makes goes
# through the guard below, which snapshots immediately either side of THAT invocation. A
# change observed across a known invocation is attributable to it; a change observed across
# 188 cases and several minutes is not.
#
# A SCRIPT, not a shell function, because several call sites run under `timeout`, which
# execs a binary and cannot invoke a function. Violations are appended to a FILE rather
# than a variable for the same reason plus one more: several invocations run inside `$( )`,
# and a variable assigned in a subshell is discarded — the exact defect round 6 found one
# directory over. A file survives both.
export PIN_SHARED_VIOLATIONS="$tmp/shared-state-violations.log"
: >"$PIN_SHARED_VIOLATIONS"
PIN_BS="$tmp/pin-bs-guard"
cat >"$PIN_BS" <<'PINBS'
#!/usr/bin/env bash
# pin-bs-guard <bootstrap-path> [args...] — run one bootstrap invocation with its
# HOST-REACHING INPUTS asserted, and record a violation if they are not sandboxed.
# stdout/stderr and the exit status pass through untouched: callers capture output and
# assert on rc.
#
# ASSERTS THE INPUTS, DOES NOT OBSERVE THE OUTPUT (#3414 roborev round 11). The previous
# form snapshotted the SHARED /usr/local/cargo/config.toml either side of each invocation.
# That was already attributable in the common case — a change outside every invocation
# window was reported as another writer's, not ours — but it had two defects it could not
# shed: an external write landing INSIDE one of our windows was still recorded as ours (a
# narrower race is a smaller race, not attribution), and `stat -c`/`md5sum` are GNU-only,
# so on macOS both probes yield empty fields, the mutation self-test cannot detect its own
# deliberate write, and a MANDATORY gate component fails on that platform.
#
# The property we actually need is "no invocation can reach the shared path". Bootstrap
# resolves its cargo config as `${CARGO_HOME:-$HOME/.cargo}/config.toml`, so if BOTH
# CARGO_HOME and HOME point inside the suite sandbox, that path is unreachable BY
# CONSTRUCTION — no window, no race, no platform-specific probe, and nothing another
# writer on the box can affect. So assert the inputs.
#
# HONEST RESIDUAL, stated because input-assertion does not cover it: a future bootstrap
# edit that writes an ABSOLUTE path while ignoring CARGO_HOME/HOME would not be caught
# here. That is a different defect (a hardcoded destination rather than an unsandboxed
# caller), and claiming this guard covers it would be the false-assurance shape this whole
# branch is about. #3673 is where a destination-side guard belongs.
# FAIL CLOSED ON AN UNUSABLE SANDBOX ROOT, FIRST. Without this the patterns below read
# `"$PIN_SANDBOX_ROOT"/*`, which with an empty root degenerates to `/*` and matches EVERY
# absolute path — so the guard would silently permit everything it exists to catch. That is
# the permissive-branch-on-an-unmeasured-input shape, inside the guard written to catch that
# shape (#3414, lead self-review). An unusable root is a violation in its own right, never a
# pass: unable to tell is not permission.
_v=""
case "${PIN_SANDBOX_ROOT-}" in
  '') _v="PIN_SANDBOX_ROOT is unset, so the sandbox test would match every absolute path — refusing to certify this invocation" ;;
  /*) ;;
  *) _v="PIN_SANDBOX_ROOT='$PIN_SANDBOX_ROOT' is not an absolute path, so the sandbox test is meaningless" ;;
esac
[ -n "$_v" ] || case "${CARGO_HOME-}" in
  '') _v="CARGO_HOME is unset, so bootstrap would resolve \${HOME}/.cargo — on this fleet HOME-derived or /etc/environment-derived paths reach the SHARED root-owned config" ;;
  "$PIN_SANDBOX_ROOT"/*) ;;
  *) _v="CARGO_HOME='$CARGO_HOME' is outside the suite sandbox ($PIN_SANDBOX_ROOT)" ;;
esac
if [ -z "$_v" ]; then
  case "${HOME-}" in
    '') _v="HOME is unset" ;;
    "$PIN_SANDBOX_ROOT"/*) ;;
    *) _v="HOME='$HOME' is outside the suite sandbox ($PIN_SANDBOX_ROOT)" ;;
  esac
fi
if [ -n "$_v" ]; then
  printf 'invocation: %s\n  unsandboxed input: %s\n' "$*" "$_v" >>"$PIN_SHARED_VIOLATIONS"
  # AND REFUSE TO RUN (roborev job 321, Medium). Recording a violation and then executing
  # anyway lets the damage land BEFORE the suite reports it — and the damage is not
  # hypothetical: an unsandboxed run on this box left /usr/local/cargo/config.toml root-owned
  # 0600 and broke cargo for every other lane, three times (#3673). A guard that observes but
  # does not stop is a log, not a guard. The violation is still recorded, so case 14 reports
  # it exactly as before; what changes is that bootstrap never runs.
  printf 'FATAL: refusing to run bootstrap with unsandboxed input: %s\n' "$_v" >&2
  # IF THIS FIRES ON A SUDO INVOCATION, THE CAUSE IS ALMOST CERTAINLY env_reset, NOT A REAL
  # UNSANDBOXED INPUT. sudo repopulates the environment from /etc/environment, so
  # PIN_SANDBOX_ROOT and PIN_SHARED_VIOLATIONS — both exported by the suite — do NOT survive
  # the boundary, and the guard then fail-closes on an unusable sandbox root while the actual
  # HOME/CARGO_HOME it was handed are perfectly sandboxed. Every `sudo -n env` call site
  # therefore re-passes BOTH vars explicitly. Measured when this refusal was added: 6 root
  # cases broke at once, and the same env_reset drop had ALSO been silently discarding every
  # violation record from a sudo invocation (`>>""`), so that half of the guard had never
  # worked across the boundary at all.
  exit 97
fi
bash "$@"
exit $?
PINBS
chmod +x "$PIN_BS"
PIN_SANDBOX_ROOT="$tmp"
export PIN_SANDBOX_ROOT

# `--help` gets a sandbox like every other invocation. It exits before any section and so
# cannot write — but the guard asserts INPUTS, and excusing an invocation because we
# reasoned it is harmless is how a guard's coverage erodes into a claim. No exemptions.
mkdir -p "$tmp/help-home/.cargo"

# --- TREE IDENTITY, STAMPED AT BOTH ENDS (#3414, shared-worktree incident) -------------
# This suite is run by more than one actor against ONE shared worktree, and a run that
# spans an edit CANNOT BE ATTRIBUTED — its failures may belong to the tree it started on,
# the tree it ended on, or neither. That is not hypothetical twice over: a run died with a
# parse error at a line nobody had touched (bash was still reading the file as it changed),
# and separately a `FAIL=1` followed by four clean runs was read as an intermittent case
# when it was actually one measurement across a moving tree.
#
# Neither was visible in the log. The mtime forensics that found them are not something the
# next reader will think to do, so the run now SAYS what tree it ran on, at both ends, in
# the same shape agent-gate.sh uses (tree-start / tree-end / tree-integrity). A moving tree
# then shows up as itself instead of as a flaky case.
#
# Portable and non-fatal: `git` may be absent or this may not be a checkout, in which case
# the stamp says UNKNOWN rather than pretending. It never fails the suite on its own —
# the point is that the log can be read, not that the tree must be still.
# A COUNT IS NOT A DIGEST (#3414 final roborev, finding CC). The first version recorded
# HEAD plus the NUMBER of dirty paths — unchanged by editing an already-dirty file, and
# unchanged by swapping one dirty path for another. So it would have reported STABLE across
# exactly the shared-worktree edit it was built to expose, and BOTH incidents that motivated
# it began with a file that was ALREADY modified: the instrument was blind to its own
# founding case.
#
# The stamp now carries a digest of the actual content — porcelain status plus the
# working-tree AND index diffs. `git hash-object` rather than `md5sum`/`shasum`: git is
# already a hard requirement two lines up, while `md5sum` is GNU-only, and this suite has
# already shipped one GNU-only probe that broke off Linux. A digest that cannot be computed
# prints UNKNOWN, never an empty string — two empty digests compare EQUAL, which is the
# false-STABLE all over again.
pin_tree_id() {
  local head dig
  if command -v git >/dev/null 2>&1 && git -C "$SCRIPT_DIR" rev-parse --git-dir >/dev/null 2>&1; then
    head=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || printf 'UNKNOWN')
    # UNTRACKED CONTENTS ARE HASHED TOO (#3414 roborev round 14, finding EE). `git status
    # --porcelain` NAMES an untracked file; `git diff` omits its CONTENT — so editing an
    # already-untracked file left the digest unchanged and reported STABLE. The lead
    # preferred narrowing the claim to tracked files; I covered them instead, because the
    # hole is closable in one line and a narrower claim would still have missed a real
    # shared-worktree edit. IGNORED paths stay excluded on purpose (`--exclude-standard`):
    # this lane's own scratch — the verdict file, the follow-up list — changes constantly
    # and hashing it would make every run report MOVED, which is the alarm nobody reads.
    # ...AND THE ENUMERATION IS ROOTED AT THE WORKTREE TOP, NOT AT $SCRIPT_DIR (roborev
    # job 332). `git ls-files` defaults to the CURRENT DIRECTORY, so `-C "$SCRIPT_DIR"`
    # hashed untracked contents only under `scripts/tests` — an untracked file anywhere
    # else could change content and still digest STABLE. That is the SAME silent-omission
    # class as the `xargs -r` defect above, one axis over: portability there, SCOPE here,
    # and in both the digest still LOOKS like it covered them. `status --porcelain` is
    # repo-wide already, so the NAME of such a file was never the gap — only its CONTENT.
    # Measured cost on this lane: 0 untracked non-ignored files, 0.017s.
    local top
    top=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null) || top="$SCRIPT_DIR"
    [ -n "$top" ] || top="$SCRIPT_DIR"
    dig=$( { git -C "$SCRIPT_DIR" status --porcelain 2>/dev/null
             git -C "$SCRIPT_DIR" diff 2>/dev/null
             git -C "$SCRIPT_DIR" diff --cached 2>/dev/null
             # PORTABLE NUL loop, not `xargs -0 -r` (#3414 final roborev). `-r`
             # (--no-run-if-empty) is GNU-only and BSD/macOS xargs REJECTS it, so on a
             # supported macOS host the whole untracked-contents branch failed silently
             # and edits to already-untracked files were reported STABLE. A silent
             # omission inside an integrity digest is the worst possible failure mode:
             # the digest still LOOKS like it covered them.
             while IFS= read -r -d "" _u; do
               git -C "$top" hash-object -- "$_u" 2>/dev/null
             done < <(git -C "$top" ls-files --others --exclude-standard -z 2>/dev/null)
           } | git hash-object --stdin 2>/dev/null )
    case "$dig" in
      ?*) dig=$(printf '%s' "$dig" | cut -c1-12) ;;
      *)  dig=UNKNOWN ;;
    esac
    printf '%s worktree=%s' "$head" "$dig"
  else
    printf 'UNKNOWN worktree=UNKNOWN'
  fi
}
PIN_TREE_START=$(pin_tree_id)
printf 'tree-start: %s  (worktree digest covers tracked diffs + untracked contents; ignored paths excluded)\n' "$PIN_TREE_START"

# --- 1. syntax check (bash -n) ---
if bash -n "$BOOTSTRAP" 2>/dev/null; then
  ok "bootstrap script parses (bash -n)"
else
  bad "bootstrap script has a syntax error"
fi

# --- 2. --help exits 0 and prints usage ---
help_out=$(env HOME="$tmp/help-home" CARGO_HOME="$tmp/help-home/.cargo" "$PIN_BS" "$BOOTSTRAP" --help 2>&1); help_rc=$?
if [ "$help_rc" -eq 0 ] && out_has "$help_out" "bootstrap"; then
  ok "--help exits 0 and prints usage"
else
  bad "--help did not exit 0 / print usage (rc=$help_rc)"
fi

# --- 3. Pure-check run must NOT install. Shadow brew/cargo/roborev with a tripwire
#        on PATH so ANY install attempt is recorded, then assert nothing ran an
#        install subcommand. ---
tripwire="$tmp/tripwire.log"
: >"$tripwire"

# --- GLOBAL git-config isolation (issue #2942) -----------------------------
# The bootstrap's git-credential section READS the configured helper chain and,
# under --yes, WRITES a helper into the user's GLOBAL git config. This self-test
# must never touch (or be perturbed by) the host machine's real credential setup —
# clobbering it would break the live delivery session running on this box. These
# two exports are inherited by EVERY bootstrap child below, so the isolation holds
# for cases added later without remembering to opt in:
#   GIT_CONFIG_GLOBAL   redirects `git config --global` + global reads to a throwaway
#   GIT_CONFIG_NOSYSTEM ignores /etc/gitconfig, so a host-wide helper cannot leak in
# (HOME is sandboxed per case as well; GIT_CONFIG_GLOBAL is the belt to that braces —
# it also covers an XDG_CONFIG_HOME that survives a HOME override.)
export GIT_CONFIG_GLOBAL="$tmp/global-gitconfig"
export GIT_CONFIG_NOSYSTEM=1
: >"$GIT_CONFIG_GLOBAL"

# --- /etc/environment isolation for the single-gate pin (issue #3414) -------
# Section 5b persists CQLITE_GATE_MAX_CONCURRENCY into /etc/environment under --yes,
# and several cases below run bootstrap with --yes. Without this the suite's verdict
# would depend on the host's real system env file — and, on a root-run box, would
# MUTATE it. The section's test seam redirects the write into this sandbox and, under
# the required CQLITE_BOOTSTRAP_TEST_MODE marker, makes that write UNPRIVILEGED, so no
# case here can reach a privileged write at all. Exported ONCE, like GIT_CONFIG_GLOBAL
# above, so a case added later inherits the isolation without remembering to opt in;
# the pin cases below override the FILE per case and leave the marker alone.
export CQLITE_BOOTSTRAP_TEST_MODE=1
export CQLITE_BOOTSTRAP_ENV_FILE="$tmp/etc-environment"
: >"$CQLITE_BOOTSTRAP_ENV_FILE"


# --- CARGO_HOME isolation: THIS SUITE WAS BREAKING cargo FOR THE WHOLE BOX ---------
# The mold section writes `${CARGO_HOME:-$HOME/.cargo}/config.toml`, and on this fleet
# /etc/environment sets CARGO_HOME=/usr/local/cargo — root-owned and SHARED BY EVERY
# USER. So every bootstrap invocation here rewrote the machine-wide cargo config, and a
# peer lane took three spurious red clusters from `could not load Cargo configuration ...
# Permission denied` while it sat root-owned mode 600.
#
# TWO HALVES OF THE CAUSE, both worth recording. The hazard is OLD — that section has
# always written $CARGO_HOME — but this suite makes **37 bootstrap invocations**, so what
# was "occasionally, when someone runs the suite" became constant. We did not introduce
# the bug; we crossed a threshold on someone else's latent one, and a tooling test that
# mutates shared host state is a fleet-wide false-red generator whatever it asserts —
# including inside a gate of record, where it voids a 20-minute certification and invites
# misattribution to the diff under test.
#
# Sandboxed rather than restored-afterwards: a restore leaves a window in which peers red,
# does not survive a killed run, and still clobbers whatever the real file legitimately
# holds. The sibling suite test_perf_capability_bootstrap.sh already pairs
# CARGO_HOME with its sandbox HOME per invocation; this is the same fact from a second
# channel, so a case added later that sets HOME and forgets CARGO_HOME is still contained.
# NOTE the export does NOT cover `sudo` invocations — sudoers' env_reset drops it — so
# those must pass CARGO_HOME explicitly on the command line; see the root cases in block 11.
export CARGO_HOME="$tmp/cargo-home"
mkdir -p "$CARGO_HOME"


# --- REAL-ORIGIN isolation for the push probe (issue #3369) ----------------
# Section 3b now MEASURES push capability by actually pushing a throwaway
# refs/claims/smoke-<commit-sha> ref (scripts/flow/claim.sh smoke). Every case that runs
# "$BOOTSTRAP" IN PLACE therefore has the real checkout as REPO_ROOT and the real
# github.com origin as its remote — with this box's real credentials. Those runs pass
# --skip-push-probe so this suite can NEVER mutate the real origin; the probe's own
# behaviour is covered hermetically in block 7p below, against sandbox remotes only.
# Cases that run a COPY of bootstrap in a fake repo are safe without the flag: the
# probe short-circuits to UNMEASURED when the copy's tree has no scripts/flow/claim.sh
# (mk_fake_repo only installs it on explicit request), so no network call is made.

# --- BOARD env isolation (issue #2942) -------------------------------------
# The board section reads CQLITE_PROJECT_{OWNER,NUMBER,ACCOUNT} and PROJECT_TITLE from
# the environment, and a worker shell commonly EXPORTS them (the fleet exports
# CQLITE_PROJECT_NUMBER). Inheriting them makes this suite's verdict depend on the shell
# it runs in — it silently masked the entire "number not exported" path until a case was
# written for it. Clear them once; every case sets exactly what it means to test.
unset CQLITE_PROJECT_NUMBER CQLITE_PROJECT_OWNER CQLITE_PROJECT_ACCOUNT PROJECT_TITLE

mkshim() {
  # mkshim <name> [dir] [log]: a fake tool that records "install"/"add" invocations
  # and is otherwise a harmless no-op (version/status queries succeed emptily).
  # dir/log default to the shared sandbox + tripwire; a case that needs its OWN
  # install tripwire (7p-f, issue #3369) passes its own and leaves the shared shims
  # every later case depends on untouched.
  local name="$1" dir="${2:-$tmp}" log="${3:-$tripwire}"
  cat >"$dir/$name" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do
  case "\$a" in
    install|add) echo "$name \$*" >>"$log" ;;
  esac
done
exit 0
EOF
  chmod +x "$dir/$name"
}
mkshim brew
mkshim cargo
mkshim roborev
mkshim gh

# Sandbox HOME/CARGO_HOME for these whole-script runs so the Linux mold branch can
# NEVER mutate the host's real ~/.cargo/config when this runs inside tooling-tests
# on a Linux gate box that has mold + a working cc (issue #2859 blocker 2).
host_home="$tmp/host-home"; mkdir -p "$host_home/.cargo"

# Run with the shims FIRST on PATH, default mode (no --yes), skipping the smoke.
run_out=$(PATH="$tmp:$PATH" HOME="$host_home" CARGO_HOME="$host_home/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1); run_rc=$?

if [ "$run_rc" -eq 0 ]; then
  ok "default (no --yes) run exits 0"
else
  bad "default run exited non-zero (rc=$run_rc)"
  printf '%s\n' "$run_out"
fi

if [ -s "$tripwire" ]; then
  bad "default run attempted an install (tripwire):"
  cat "$tripwire"
else
  ok "default run performed NO installs (pure check)"
fi

# --- 4. The run must actually emit its section headers (it ran the checks). ---
for section in "Rust toolchain" "Gate accelerators" "project scope" "roborev" "CQLITE_DATASETS_ROOT" "Notification channel" "Bootstrap summary"; do
  if out_has "$run_out" "$section"; then
    ok "check section present: $section"
  else
    bad "check section MISSING: $section"
  fi
done

# --- 5. Default run must PRINT an install command for a missing tool rather than
#        run it. Force a missing accelerator by running with an empty-ish PATH that
#        still has coreutils but no sccache; assert the guidance line appears. ---
# Reset the tripwire so the no-install assertion below reflects ONLY this run.
: >"$tripwire"
guard_out=$(PATH="$tmp:/usr/bin:/bin" HOME="$host_home" CARGO_HOME="$host_home/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if printf '%s' "$guard_out" | grep -Eq "install sccache:|sccache MISSING"; then
  ok "missing accelerator prints install guidance (does not auto-install)"
else
  bad "missing accelerator did not surface install guidance"
fi
if [ -s "$tripwire" ]; then
  bad "guidance run STILL attempted an install"
else
  ok "guidance run performed NO installs"
fi

# --- 6. mold link accelerator on Linux (issue #2859) ------------------------
# All cases below stub `uname` (to simulate the OS), `mold`, and the C compilers,
# and point HOME/CARGO_HOME at a sandbox so the managed block is written to a
# throwaway ~/.cargo/config.toml — never the real one and never the repo's.

mk_stub() {
  # mk_stub <dir> <name> <body>
  local dir="$1" name="$2" body="$3"
  # REMOVE FIRST — never write THROUGH the path. mk_hermetic_bin populates these same dirs
  # with SYMLINKS to the real tools, and `cat >` FOLLOWS a symlink, so stubbing a name that
  # is ALSO hermetically linked has two failure modes and neither is visible: where the link
  # target is not writable (a root-owned /usr/bin tool, and the suite runs unprivileged) the
  # redirect fails, no stub is installed, and every case relying on it passes VACUOUSLY
  # against the REAL tool; where the target IS writable it TRUNCATES THE REAL BINARY.
  # Measured when this bit: `chmod` is the one name in both sets, the stub never installed,
  # and case 11bc read the real chmod's success as its own.
  rm -f "$dir/$name"
  cat >"$dir/$name" <<EOF
#!/usr/bin/env bash
$body
EOF
  chmod +x "$dir/$name"
  # AND FAIL LOUDLY. A harness that cannot install a stub does not produce a failing case,
  # it produces a PASSING one that tested nothing — the exact shape this suite exists to
  # refuse, so it aborts rather than reporting a verdict it did not earn.
  if [ -L "$dir/$name" ] || [ ! -f "$dir/$name" ] || [ ! -x "$dir/$name" ]; then
    printf 'FATAL: mk_stub could not install a real stub at %s/%s (symlink=%s file=%s exec=%s) — refusing to run cases that would pass vacuously against the real tool\n' \
      "$dir" "$name" "$([ -L "$dir/$name" ] && echo yes || echo no)" \
      "$([ -f "$dir/$name" ] && echo yes || echo no)" "$([ -x "$dir/$name" ] && echo yes || echo no)" >&2
    exit 1
  fi
}
# count_begin <file>: number of managed-block BEGIN markers. grep -c already prints
# a count (0 on no match) AND exits 1 — a `|| echo 0` would DOUBLE-print "0\n0", so
# capture the count and default an empty (missing-file) result to 0 instead.
count_begin() {
  local n
  n=$(grep -c '^# BEGIN cqlite-mold' "$1" 2>/dev/null)
  echo "${n:-0}"
}
# Stub gh + roborev + cargo so the (unrelated) auth/agent/toolchain sections stay
# fast and offline during these mold cases, which run bootstrap under the full PATH
# with CARGO_HOME pointed at a throwaway dir (a real `cargo --version` there would
# trigger a multi-minute rustup toolchain provision into the empty CARGO_HOME).
# GH_STUB_TOKEN_BODY — every `gh` stub must answer `gh auth token --hostname github.com`
# with the ENVIRONMENT token, because that is what real gh does: GH_TOKEN/GITHUB_TOKEN take
# precedence for github.com, and gh reports per-host tokens for anything else. §3b's
# fallback repair is gated on that answer MATCHING the token it would install (#3369 FIX L),
# so a stub that stays silent makes every --yes fallback case REFUSE — a test artifact
# rather than behaviour. Any other host correctly gets gh's "no token" failure.
GH_STUB_TOKEN_BODY='if [ "$1" = auth ] && [ "$2" = token ]; then
  want=""; shift 2
  while [ $# -gt 0 ]; do [ "$1" = --hostname ] && { want="$2"; shift; }; shift; done
  case "${want:-github.com}" in
    github.com) echo "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ;;
    *) echo "no oauth token found for $want" >&2; exit 1 ;;
  esac
  exit 0
fi'

stub_net() {
  mk_stub "$1" gh "$GH_STUB_TOKEN_BODY
exit 0"
  mk_stub "$1" roborev 'exit 0'
  mk_stub "$1" cargo '[ "$1" = --version ] && echo "cargo 1.88.0"; exit 0'
}

# mk_hermetic_bin <dir>: a stub-only PATH dir with symlinked coreutils + a Linux
# `uname` stub, so the missing-mold cases (6g/6h) never depend on the host having (or
# NOT having) apt-get/dnf/etc. On a real Linux runner `/usr/bin/apt-get` exists, which
# would otherwise flip the "no supported package manager" assertion and turn the FULL
# gate RED via tooling-tests (#2859 blocker D). No package-manager binaries are linked;
# callers add exactly the ones they intend to detect.
mk_hermetic_bin() {
  local dir="$1" t p
  mkdir -p "$dir"
  for t in bash dirname mktemp grep cp cat sed awk mkdir rm ln mv touch chmod \
           head tail tr sort cut wc stat env git find xargs basename date sleep expr flock \
           timeout gtimeout; do   # BOTH: stock macOS has only gtimeout (GNU coreutils)
    p=$(type -P "$t" 2>/dev/null) || continue
    [ -n "$p" ] && ln -sf "$p" "$dir/$t" 2>/dev/null || true
  done
  mk_stub "$dir" uname 'echo Linux; exit 0'
  stub_net "$dir"  # gh/roborev/cargo stubs — no live network from these cases
}

# scc_stub_body: the shared stub. Reads SCC_STUB_MAX / SCC_STUB_USED / SCC_STUB_LOC /
# SCC_STUB_ISO_LOC / SCC_STUB_LOG from the environment at call time, so one stub serves every
# case (bootstrap's oracle scrubs only SCCACHE_*, BASH_ENV and ENV, so these survive).
scc_stub_body='log=${SCC_STUB_LOG:-}
[ -n "$log" ] && printf "%s\n" "$*" >> "$log"
scc_resolve() {
  v=${SCCACHE_CACHE_SIZE-}
  case "$v" in
    *[!0-9KkMmGgTt]*|""|*[KkMmGgTt]*[KkMmGgTt]*|[KkMmGgTt]*) echo 10737418240; return ;;
  esac
  n=${v%[KkMmGgTt]}; s=${v#"$n"}
  # An UNREPRESENTABLE value falls back to the default — MEASURED on sccache 0.17.0:
  # 999999999999999999999G (21 digits) reads back as 10737418240. Modelled because it is the
  # regression fixture for #3727 round 4 f1, where a shape test accepted such a literal.
  if [ ${#n} -gt 20 ]; then echo 10737418240; return; fi
  case "$s" in
    K|k) m=1024 ;; M|m) m=1048576 ;; G|g) m=1073741824 ;; T|t) m=1099511627776 ;; *) m=1 ;;
  esac
  echo $(( 10#$n * m ))
}
# --start-server FIXES THE CAP FOR THAT SERVER S LIFETIME, from the env it is started with — the
# whole mechanism of #3727 — so the stub records it in a state file and every later production
# read answers from that. Without this, bootstrap becoming the first starter would be
# unobservable in a test.
case "$*" in
  *--start-server*)
    # SCC_STUB_RACE_CAP models a LOST RACE: the start finds nothing, but by the time the cap is
    # read back a CONCURRENT lane has started a server at ITS value, not ours. That is the shape
    # the real fleet produces (several lanes, one server, `--start-server` a no-op against an
    # existing one) — and it is NOT the same as a pre-existing live server, which 12b-g4 covers:
    # the distinguishing fact is that this run DID attempt a start.
    if [ -n "${SCC_STUB_STATE:-}" ]; then
      if [ -n "${SCC_STUB_RACE_CAP:-}" ]; then printf '%s\n' "$SCC_STUB_RACE_CAP" > "$SCC_STUB_STATE"
      else scc_resolve > "$SCC_STUB_STATE"; fi
    fi
    exit 0 ;;
  *--show-stats*) ;;
  *) exit 0 ;;
esac
if [ -n "${SCCACHE_SERVER_PORT:-}" ]; then
  # ISOLATED client, no server: the cap comes from this process s own SCCACHE_CACHE_SIZE and the
  # size is null (measured on sccache 0.17.0). The two RUNNING-server branches below deliberately
  # IGNORE the client env, because that is what a real server does — and it is exactly what the
  # attribution differential measures. A null cache_size is NOT a no-server signal: a running
  # server with an empty cache reports null too (measured), which is why SCC_STUB_USED=null is a
  # legal setting for a RUNNING server.
  printf "{\"stats\":{},\"cache_location\":\"Local disk: \\\"%s\\\"\",\"cache_size\":null,\"max_cache_size\":%s,\"version\":\"0.17.0\"}\n" \
    "${SCC_STUB_ISO_LOC:-${SCCACHE_DIR:-/none}}" "$(scc_resolve)"
elif [ -n "${SCC_STUB_STATE:-}" ] && [ -s "${SCC_STUB_STATE:-}" ]; then
  # A server was STARTED during this run and is enforcing the cap it was started with.
  printf "{\"stats\":{},\"cache_location\":\"Local disk: \\\"%s\\\"\",\"cache_size\":%s,\"max_cache_size\":%s,\"version\":\"0.17.0\"}\n" \
    "${SCC_STUB_LOC:-/data/sccache-stub}" "${SCC_STUB_USED:-1000}" "$(cat "$SCC_STUB_STATE")"
elif [ "${SCC_STUB_MAX:-none}" = none ]; then
  printf "{\"stats\":{},\"cache_location\":\"Local disk: \\\"%s\\\"\",\"cache_size\":null,\"max_cache_size\":%s,\"version\":\"0.17.0\"}\n" \
    "${SCC_STUB_LOC:-/data/sccache-stub}" "$(scc_resolve)"
else
  printf "{\"stats\":{},\"cache_location\":\"Local disk: \\\"%s\\\"\",\"cache_size\":%s,\"max_cache_size\":%s,\"version\":\"0.17.0\"}\n" \
    "${SCC_STUB_LOC:-/data/sccache-stub}" "${SCC_STUB_USED:-1000}" "$SCC_STUB_MAX"
fi
exit 0'

# 6a. mold present + cc passes the probe -> managed block written, both Linux
#     triples, NO linker line (default cc accepts -fuse-ld=mold).
sbA=$(mktemp -d "$tmp/moldA.XXXXXX"); stubA="$tmp/stubA"; mkdir -p "$stubA"
mk_stub "$stubA" uname 'echo Linux; exit 0'
stub_net "$stubA"
mk_stub "$stubA" mold '[ "$1" = --version ] && echo "mold 2.4.0"; exit 0'
mk_stub "$stubA" cc 'exit 0'
outA=$(PATH="$stubA:$PATH" HOME="$sbA" CARGO_HOME="$sbA/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
cfgA="$sbA/.cargo/config.toml"
if out_has "$outA" "Link accelerator: mold"; then
  ok "mold: Linux run emits the mold section"
else
  bad "mold: Linux run did not emit the mold section"
fi
if [ -f "$cfgA" ] \
   && grep -q '^# BEGIN cqlite-mold' "$cfgA" \
   && grep -q '^# END cqlite-mold' "$cfgA" \
   && grep -q '^\[target.x86_64-unknown-linux-gnu\]' "$cfgA" \
   && grep -q '^\[target.aarch64-unknown-linux-gnu\]' "$cfgA" \
   && grep -q 'link-arg=-fuse-ld=mold' "$cfgA"; then
  ok "mold: managed block written with both Linux target triples"
else
  bad "mold: managed block missing expected markers/triples"
  [ -f "$cfgA" ] && { echo "--- config ---"; cat "$cfgA"; echo "--------------"; }
fi
if [ -f "$cfgA" ] && ! grep -q '^linker = ' "$cfgA"; then
  ok "mold: cc-passing probe writes NO linker override"
else
  bad "mold: cc-passing probe unexpectedly wrote a linker override"
fi

# 6b. re-run is byte-idempotent: block appears exactly once and the file is
#     identical to the first run.
firstA=$(cat "$cfgA")
PATH="$stubA:$PATH" HOME="$sbA" CARGO_HOME="$sbA/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe >/dev/null 2>&1
secondA=$(cat "$cfgA")
if [ "$(count_begin "$cfgA")" = 1 ] && [ "$firstA" = "$secondA" ]; then
  ok "mold: re-run idempotent (exactly one block, file byte-identical)"
else
  bad "mold: re-run not idempotent (begin-count=$(count_begin "$cfgA"))"
fi

# 6c. unrelated user config outside the markers is preserved byte-for-byte.
sbC=$(mktemp -d "$tmp/moldC.XXXXXX"); mkdir -p "$sbC/.cargo"
cfgC="$sbC/.cargo/config.toml"
printf '[build]\njobs = 7\n\n[net]\nretry = 9\n' >"$cfgC"
PATH="$stubA:$PATH" HOME="$sbC" CARGO_HOME="$sbC/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe >/dev/null 2>&1
if grep -qx 'jobs = 7' "$cfgC" && grep -qx 'retry = 9' "$cfgC" \
   && grep -qx '\[build\]' "$cfgC" && grep -qx '\[net\]' "$cfgC" \
   && grep -q '^# BEGIN cqlite-mold' "$cfgC"; then
  ok "mold: unrelated user config preserved alongside the appended block"
else
  bad "mold: user config not preserved when appending the block"
  echo "--- config ---"; cat "$cfgC"; echo "--------------"
fi
# Idempotent even with user content present.
firstC=$(cat "$cfgC")
PATH="$stubA:$PATH" HOME="$sbC" CARGO_HOME="$sbC/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe >/dev/null 2>&1
if [ "$firstC" = "$(cat "$cfgC")" ] && [ "$(count_begin "$cfgC")" = 1 ]; then
  ok "mold: re-run with user content stays byte-identical (one block)"
else
  bad "mold: re-run with user content changed the file or duplicated the block"
fi

# 6d. failed link probe (no compiler accepts -fuse-ld=mold) -> warn, write NOTHING.
sbD=$(mktemp -d "$tmp/moldD.XXXXXX"); stubD="$tmp/stubD"; mkdir -p "$stubD"
mk_stub "$stubD" uname 'echo Linux; exit 0'
stub_net "$stubD"
mk_stub "$stubD" mold 'exit 0'
mk_stub "$stubD" cc 'exit 1'
mk_stub "$stubD" clang 'exit 1'
outD=$(PATH="$stubD:$PATH" HOME="$sbD" CARGO_HOME="$sbD/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if out_has "$outD" "link probe FAILED" \
   && [ ! -f "$sbD/.cargo/config.toml" ]; then
  ok "mold: failed link probe warns and writes no linker config"
else
  bad "mold: failed link probe still wrote config or missed the warning"
  [ -f "$sbD/.cargo/config.toml" ] && cat "$sbD/.cargo/config.toml"
fi

# 6e. clang-only variant: cc fails the probe, clang passes -> block sets linker.
sbE=$(mktemp -d "$tmp/moldE.XXXXXX"); stubE="$tmp/stubE"; mkdir -p "$stubE"
mk_stub "$stubE" uname 'echo Linux; exit 0'
stub_net "$stubE"
mk_stub "$stubE" mold 'exit 0'
mk_stub "$stubE" cc 'exit 1'
mk_stub "$stubE" clang 'exit 0'
PATH="$stubE:$PATH" HOME="$sbE" CARGO_HOME="$sbE/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe >/dev/null 2>&1
cfgE="$sbE/.cargo/config.toml"
if [ -f "$cfgE" ] && [ "$(grep -c '^linker = "clang"' "$cfgE")" = 2 ]; then
  ok "mold: clang-only probe writes linker = \"clang\" for both triples"
else
  bad "mold: clang-only probe did not set linker for both triples"
  [ -f "$cfgE" ] && { echo "--- config ---"; cat "$cfgE"; echo "--------------"; }
fi

# 6f. Darwin no-op: mold section skipped, no config written.
sbF=$(mktemp -d "$tmp/moldF.XXXXXX"); stubF="$tmp/stubF"; mkdir -p "$stubF"
mk_stub "$stubF" uname 'echo Darwin; exit 0'
stub_net "$stubF"
mk_stub "$stubF" mold '[ "$1" = --version ] && echo "mold 2.4.0"; exit 0'
mk_stub "$stubF" cc 'exit 0'
outF=$(PATH="$stubF:$PATH" HOME="$sbF" CARGO_HOME="$sbF/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if ! out_has "$outF" "Link accelerator: mold" \
   && [ ! -f "$sbF/.cargo/config.toml" ]; then
  ok "mold: Darwin performs no mold detection/config (no-op)"
else
  bad "mold: Darwin unexpectedly ran the mold section or wrote config"
fi

# 6g. missing mold + a supported package manager -> prints the install command in
#     default (no --yes) mode and installs NOTHING; writes no linker config. Runs in
#     a HERMETIC stub-only PATH (blocker D): the ONLY package manager visible is the
#     apt-get stub we add, regardless of what the host has installed.
sbG=$(mktemp -d "$tmp/moldG.XXXXXX"); stubG="$tmp/stubG"
mk_hermetic_bin "$stubG"
tripG="$stubG/tripwire.log"; : >"$tripG"
mk_stub "$stubG" apt-get "echo \"apt-get \$*\" >>\"$tripG\"; exit 0"
outG=$(PATH="$stubG" HOME="$sbG" CARGO_HOME="$sbG/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if out_has "$outG" "mold MISSING" \
   && out_has "$outG" "install mold:.*apt-get install -y mold" \
   && [ ! -s "$tripG" ] \
   && [ ! -f "$sbG/.cargo/config.toml" ]; then
  ok "mold: missing + apt prints install command, installs nothing, writes no config"
else
  bad "mold: missing+apt path did not print-only (tripwire=$(cat "$tripG" 2>/dev/null))"
  printf '%s\n' "$outG" | grep -i mold
fi

# 6h. missing mold + NO supported package manager -> warn, no config. HERMETIC PATH
#     (blocker D) so no host apt-get/dnf/etc. is visible.
sbH=$(mktemp -d "$tmp/moldH.XXXXXX"); stubH="$tmp/stubH"
mk_hermetic_bin "$stubH"
outH=$(PATH="$stubH" HOME="$sbH" CARGO_HOME="$sbH/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if out_has "$outH" "no supported package manager" \
   && [ ! -f "$sbH/.cargo/config.toml" ]; then
  ok "mold: missing + no package manager warns and writes no config"
else
  bad "mold: missing + no-manager path missed the warn or wrote config"
  printf '%s\n' "$outH" | grep -i mold
fi

# 6j. legacy extension-less ~/.cargo/config (blocker A): the block must be written
#     INTO the existing `config` cargo actually reads — NOT a shadow `config.toml`
#     that cargo would silently prefer, dropping the user's whole config.
sbJ=$(mktemp -d "$tmp/moldJ.XXXXXX"); mkdir -p "$sbJ/.cargo"
printf '[net]\nretry = 4\n' >"$sbJ/.cargo/config"
PATH="$stubA:$PATH" HOME="$sbJ" CARGO_HOME="$sbJ/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe >/dev/null 2>&1
if grep -q '^# BEGIN cqlite-mold' "$sbJ/.cargo/config" \
   && grep -qx 'retry = 4' "$sbJ/.cargo/config" \
   && [ ! -f "$sbJ/.cargo/config.toml" ]; then
  ok "mold: writes into the legacy extension-less ~/.cargo/config (no shadow config.toml)"
else
  bad "mold: legacy config handling wrong (shadow config.toml or lost user config)"
  ls -la "$sbJ/.cargo" 2>/dev/null
fi

# 6k. pre-existing user [target.<triple>-unknown-linux-gnu] OUTSIDE the markers
#     (blocker B): appending our block would be a TOML table redefinition = cargo
#     parse error on every invocation. Bootstrap must WARN and write NOTHING, leaving
#     the file byte-identical.
sbK=$(mktemp -d "$tmp/moldK.XXXXXX"); mkdir -p "$sbK/.cargo"
cfgK="$sbK/.cargo/config.toml"
printf '[target.x86_64-unknown-linux-gnu]\nrustflags = ["-C", "target-cpu=native"]\n' >"$cfgK"
beforeK=$(cat "$cfgK")
outK=$(PATH="$stubA:$PATH" HOME="$sbK" CARGO_HOME="$sbK/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if out_has "$outK" "existing \[target" \
   && [ "$beforeK" = "$(cat "$cfgK")" ] \
   && ! grep -q '^# BEGIN cqlite-mold' "$cfgK"; then
  ok "mold: pre-existing [target.<triple>] section -> warn, file byte-identical, no block"
else
  bad "mold: pre-existing target section not fail-safe (block written or file changed)"
  echo "--- config ---"; cat "$cfgK"; echo "--------------"
fi

# 6l. BOTH config files exist (blocker 1): cargo prefers the extension-less `config`,
#     so the block must land THERE, not in the ignored `config.toml`.
sbL=$(mktemp -d "$tmp/moldL.XXXXXX"); mkdir -p "$sbL/.cargo"
printf '[net]\nretry = 1\n' >"$sbL/.cargo/config"
printf '[net]\nretry = 2\n' >"$sbL/.cargo/config.toml"
tomlL_before=$(cat "$sbL/.cargo/config.toml")
PATH="$stubA:$PATH" HOME="$sbL" CARGO_HOME="$sbL/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe >/dev/null 2>&1
if grep -q '^# BEGIN cqlite-mold' "$sbL/.cargo/config" \
   && ! grep -q '^# BEGIN cqlite-mold' "$sbL/.cargo/config.toml" \
   && [ "$tomlL_before" = "$(cat "$sbL/.cargo/config.toml")" ]; then
  ok "mold: both files present -> block lands in the effective 'config', config.toml untouched"
else
  bad "mold: both-files precedence wrong (block in the ignored config.toml)"
  echo "--- config ---"; cat "$sbL/.cargo/config"; echo "--- config.toml ---"; cat "$sbL/.cargo/config.toml"
fi

# 6m. pre-existing [build] rustflags (blocker 3): our target.rustflags would silently
#     disable it (first-match-wins), so bootstrap must WARN and write NOTHING.
sbM=$(mktemp -d "$tmp/moldM.XXXXXX"); mkdir -p "$sbM/.cargo"
cfgM="$sbM/.cargo/config.toml"
printf '[build]\nrustflags = ["-C", "target-cpu=native"]\n' >"$cfgM"
beforeM=$(cat "$cfgM")
outM=$(PATH="$stubA:$PATH" HOME="$sbM" CARGO_HOME="$sbM/.cargo" \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if out_has "$outM" "existing \[build\] rustflags" \
   && [ "$beforeM" = "$(cat "$cfgM")" ] \
   && ! grep -q '^# BEGIN cqlite-mold' "$cfgM"; then
  ok "mold: pre-existing [build] rustflags -> warn, file byte-identical, no block"
else
  bad "mold: [build] rustflags not fail-safe (block written or file changed)"
  echo "--- config ---"; cat "$cfgM"; echo "--------------"
fi

# 6n. --yes INSTALLS then WIRES (blocker 4): the install stub places `mold` on PATH,
#     and the same run must re-detect it and write the managed block — one --yes run
#     delivers the full acceleration, not just the install. Runs a COPY of bootstrap in
#     a fake repo so the --yes dataset-fetch path is a fast no-op (no such script → no
#     network), never the real fetch-datasets.sh.
nrepo="$tmp/n-repo"; mkdir -p "$nrepo/scripts"
cp "$BOOTSTRAP" "$nrepo/scripts/bootstrap-agent-machine.sh"
sbN=$(mktemp -d "$tmp/moldN.XXXXXX"); mkdir -p "$sbN/.cargo"; stubN="$tmp/stubN"
mk_hermetic_bin "$stubN"
mk_stub "$stubN" cc 'exit 0'
mk_stub "$stubN" sudo 'exec "$@"'   # passthrough so `sudo apt-get …` runs the stub
# apt-get stub: on `install … mold`, drop a real `mold` executable onto PATH.
apt_body='installed=0; for a in "$@"; do [ "$a" = mold ] && installed=1; done; if [ "$installed" = 1 ]; then printf "#!/usr/bin/env bash\n[ \"\$1\" = --version ] && echo \"mold 2.4.0\"\nexit 0\n" > "'"$stubN/mold"'"; chmod +x "'"$stubN/mold"'"; fi; exit 0'
mk_stub "$stubN" apt-get "$apt_body"
PATH="$stubN" HOME="$sbN" CARGO_HOME="$sbN/.cargo" \
  "$PIN_BS" "$nrepo/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke >/dev/null 2>&1
if grep -q '^# BEGIN cqlite-mold' "$sbN/.cargo/config.toml" 2>/dev/null; then
  ok "mold: --yes installs mold then wires the managed block in the same run"
else
  bad "mold: --yes installed but never wired the linker config"
  ls -la "$sbN/.cargo" 2>/dev/null
fi

# 6i. the repo's committed .cargo/config.toml is never touched (blocker 7): run a COPY
#     of bootstrap whose BASH_SOURCE-derived REPO_ROOT is a fake repo that HAS a
#     .cargo/config.toml, with HOME/CARGO_HOME sandboxed elsewhere. The block must go
#     to the per-machine CARGO_HOME and the fake repo config must be byte-identical.
fakerepo="$tmp/fakerepo"; mkdir -p "$fakerepo/scripts" "$fakerepo/.cargo"
cp "$BOOTSTRAP" "$fakerepo/scripts/bootstrap-agent-machine.sh"
repo_cfg="$fakerepo/.cargo/config.toml"
printf '[registries.example]\nindex = "sparse+https://example.invalid/"\n' >"$repo_cfg"
repo_before=$(cat "$repo_cfg")
sbI=$(mktemp -d "$tmp/moldI.XXXXXX"); mkdir -p "$sbI/.cargo"
PATH="$stubA:$PATH" HOME="$sbI" CARGO_HOME="$sbI/.cargo" \
  "$PIN_BS" "$fakerepo/scripts/bootstrap-agent-machine.sh" --skip-smoke >/dev/null 2>&1
if [ "$repo_before" = "$(cat "$repo_cfg")" ] \
   && grep -q '^# BEGIN cqlite-mold' "$sbI/.cargo/config.toml"; then
  ok "mold: repo-committed .cargo/config.toml untouched; block written to per-machine CARGO_HOME"
else
  bad "mold: repo config was mutated OR block did not land in CARGO_HOME"
  echo "--- repo cfg now ---"; cat "$repo_cfg"; echo "--------------------"
fi

# --- 7. git push credentials (issue #2942) ---------------------------------
# `gh` auth and `git` auth are SEPARATE credential paths: an authenticated gh CLI is
# NOT evidence that a raw `git push` can authenticate, and scripts/flow/claim.sh +
# claim-heartbeat.sh push with plain git on 10+ call sites. Every case below runs a
# COPY of bootstrap inside a throwaway git repo with a sandboxed HOME and its OWN
# GIT_CONFIG_GLOBAL, so the credential write under --yes can only ever land in the
# sandbox — never in this machine's real global git config.

# mk_fake_repo <dir> <origin-url>: a throwaway git repo holding a COPY of bootstrap
# at <dir>/scripts/, with `origin` set to <origin-url> and NO repo-local credential
# helper. The copy makes BASH_SOURCE-derived REPO_ROOT resolve to <dir>, so the
# credential probe reads THIS remote/config, never the real checkout's — and the
# --yes dataset fetch is a fast no-op (no test-data/scripts/fetch-datasets.sh here).
# A THIRD argument, `with-claim`, additionally installs scripts/flow/claim.sh — the
# push-capability probe (issue #3369) delegates to it, and short-circuits to UNMEASURED
# without ever touching the network when it is absent. That default is what keeps every
# pre-existing case above from pushing to the real origin: they are unchanged, and their
# copies have no claim.sh, so no case acquires a network call by inheritance.
mk_fake_repo() {
  local dir="$1" url="$2" claim="${3:-}"
  mkdir -p "$dir/scripts"
  cp "$BOOTSTRAP" "$dir/scripts/bootstrap-agent-machine.sh"
  if [ "$claim" = with-claim ]; then
    mkdir -p "$dir/scripts/flow"
    cp "$SCRIPT_DIR/../flow/claim.sh" "$dir/scripts/flow/claim.sh"
  fi
  git -c init.defaultBranch=main init -q "$dir" >/dev/null 2>&1
  [ -n "$url" ] && git -C "$dir" remote add origin "$url" >/dev/null 2>&1
  return 0
}

FAKE_TOKEN='ghp_FAKEtoken2942FAKEtoken2942FAKEtoken'

# 7a. HTTPS origin, NO credential helper anywhere, default (no --yes) mode ->
#     must WARN (never `ok`), print the identifying `could not read Username`
#     symptom + remediation, and write NOTHING.
sb7a=$(mktemp -d "$tmp/cred7a.XXXXXX"); stub7a="$tmp/stub7a"
mk_hermetic_bin "$stub7a"
repo7a="$tmp/repo7a"; mk_fake_repo "$repo7a" "https://github.com/pmcfadin/cqlite.git"
gc7a="$sb7a/gitconfig"   # deliberately absent
out7a=$(PATH="$stub7a" HOME="$sb7a" CARGO_HOME="$sb7a/.cargo" GIT_CONFIG_GLOBAL="$gc7a" \
  GH_TOKEN="" GITHUB_TOKEN="" "$PIN_BS" "$repo7a/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if out_has "$out7a" "git push credentials"; then
  ok "cred: bootstrap emits the git-credential section"
else
  bad "cred: git-credential section MISSING from bootstrap output"
fi
if out_has "$out7a" "\[warn\].*git push" \
   && out_has "$out7a" "could not read Username" \
   && out_has "$out7a" "gh auth setup-git"; then
  ok "cred: no helper -> warn naming the 'could not read Username' symptom + remediation"
else
  bad "cred: no-helper case did not warn with the symptom/remediation"
  printf '%s\n' "$out7a" | grep -i -A3 "credential"
fi
if printf '%s' "$out7a" | grep -Eq '\[ok\].*(git push credentials|git credentials).*(resolve|configured)'; then
  bad "cred: reported OK for git push credentials while no helper is configured"
else
  ok "cred: authenticated gh alone is NOT reported as git push credentials"
fi
if [ ! -f "$gc7a" ]; then
  ok "cred: default (no --yes) run wrote NO global git config"
else
  bad "cred: default run wrote a global git config"; cat "$gc7a"
fi

# 7b. --yes with `gh auth setup-git` a no-op -> falls back to the $GH_TOKEN helper.
#     The config must carry the LITERAL `$GH_TOKEN` (dereferenced at call time) and
#     must NOT contain the token value; nothing the bootstrap wrote may contain it.
sb7b=$(mktemp -d "$tmp/cred7b.XXXXXX"); stub7b="$tmp/stub7b"
mk_hermetic_bin "$stub7b"
gh7b_log="$tmp/gh7b.log"; : >"$gh7b_log"
mk_stub "$stub7b" gh "echo \"\$*\" >>\"$gh7b_log\"
$GH_STUB_TOKEN_BODY
exit 0"   # setup-git succeeds but wires nothing; `auth token` answers as real gh does
repo7b="$tmp/repo7b"; mk_fake_repo "$repo7b" "https://github.com/pmcfadin/cqlite.git"
gc7b="$sb7b/gitconfig"
out7b=$(PATH="$stub7b" HOME="$sb7b" CARGO_HOME="$sb7b/.cargo" GIT_CONFIG_GLOBAL="$gc7b" \
  GH_TOKEN="$FAKE_TOKEN" "$PIN_BS" "$repo7b/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
if grep -q "auth setup-git" "$gh7b_log"; then
  ok "cred: --yes prefers 'gh auth setup-git' first"
else
  bad "cred: --yes never attempted 'gh auth setup-git'"
fi
if [ -f "$gc7b" ] && grep -q 'x-access-token' "$gc7b" && grep -qF 'GH_TOKEN' "$gc7b"; then
  ok "cred: --yes fell back to a helper that dereferences \$GH_TOKEN at call time"
else
  bad "cred: --yes did not configure the \$GH_TOKEN fallback helper"
  [ -f "$gc7b" ] && { echo "--- gitconfig ---"; cat "$gc7b"; echo "-----------------"; }
fi
# The helper MUST be host-scoped. A bare [credential] helper offers the GitHub token
# to every https host git talks to (submodules, cargo/pip git deps, a mistyped clone,
# anything answering 401) — and `gh auth setup-git`, the path this falls back FROM,
# scopes per host, so an unscoped fallback is strictly less safe than the preferred one.
if [ -f "$gc7b" ] \
   && out_has "$(git config --file "$gc7b" --get-all 'credential.https://github.com.helper' 2>/dev/null)" -F 'x-access-token' \
   && ! out_has "$(git config --file "$gc7b" --get-all credential.helper 2>/dev/null)" -F 'x-access-token'; then
  ok "cred: fallback helper is HOST-SCOPED (credential.https://github.com.helper), not a bare credential.helper"
else
  bad "cred: fallback helper is host-UNSCOPED — the token would be offered to every https host"
  [ -f "$gc7b" ] && { echo "--- gitconfig ---"; cat "$gc7b"; echo "-----------------"; }
fi
# The whole point of Decision 2: no file written by the bootstrap holds the secret.
leak7b=$(grep -rlF "$FAKE_TOKEN" "$sb7b" "$gc7b" "$repo7b" 2>/dev/null | head -5)
if [ -z "$leak7b" ]; then
  ok "cred: token VALUE never written to disk by the bootstrap"
else
  bad "cred: token value leaked into: $leak7b"
fi
if printf '%s' "$out7b" | grep -Eq '\[ok\].*git.*credential'; then
  ok "cred: --yes run reports the configured credential path as ok"
else
  bad "cred: --yes run never confirmed a working credential path"
  printf '%s\n' "$out7b" | grep -i -A2 "credential"
fi

# 7c. --yes where `gh auth setup-git` genuinely works -> use it, and do NOT also
#     add the $GH_TOKEN fallback helper (preferred form wins, Decision 2).
sb7c=$(mktemp -d "$tmp/cred7c.XXXXXX"); stub7c="$tmp/stub7c"
mk_hermetic_bin "$stub7c"
mk_stub "$stub7c" gh 'if [ "$1" = auth ] && [ "$2" = setup-git ]; then
  git config --global --add credential.helper "!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=stub-helper-secret; };f"
fi
exit 0'
repo7c="$tmp/repo7c"; mk_fake_repo "$repo7c" "https://github.com/pmcfadin/cqlite.git"
gc7c="$sb7c/gitconfig"
out7c=$(PATH="$stub7c" HOME="$sb7c" CARGO_HOME="$sb7c/.cargo" GIT_CONFIG_GLOBAL="$gc7c" \
  GH_TOKEN="$FAKE_TOKEN" "$PIN_BS" "$repo7c/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
if [ -f "$gc7c" ] && grep -q 'gh-stub' "$gc7c" && ! grep -q 'x-access-token' "$gc7c" \
   && out_has "$out7c" "gh auth setup-git"; then
  ok "cred: a working 'gh auth setup-git' is preferred; no \$GH_TOKEN fallback added"
else
  bad "cred: working setup-git path did not win (fallback added or not reported)"
  [ -f "$gc7c" ] && { echo "--- gitconfig ---"; cat "$gc7c"; echo "-----------------"; }
fi

# 7d. SSH origin -> the https credential helper is irrelevant; report it and write
#     nothing, even under --yes.
sb7d=$(mktemp -d "$tmp/cred7d.XXXXXX"); stub7d="$tmp/stub7d"
mk_hermetic_bin "$stub7d"
repo7d="$tmp/repo7d"; mk_fake_repo "$repo7d" "git@github.com:pmcfadin/cqlite.git"
gc7d="$sb7d/gitconfig"
out7d=$(PATH="$stub7d" HOME="$sb7d" CARGO_HOME="$sb7d/.cargo" GIT_CONFIG_GLOBAL="$gc7d" \
  GH_TOKEN="$FAKE_TOKEN" "$PIN_BS" "$repo7d/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
if out_has "$out7d" -i "SSH" \
   && ! { [ -f "$gc7d" ] && grep -q 'x-access-token' "$gc7d"; }; then
  ok "cred: SSH origin reported as its own credential path; no helper written"
else
  bad "cred: SSH origin case wrote a helper or did not report the SSH path"
  [ -f "$gc7d" ] && cat "$gc7d"
fi

# 7f. FUNCTIONAL confinement of the config 7b actually produced: github.com gets a
#     credential, an unrelated host gets NOTHING. This is the assertion that would
#     have caught a bare [credential] helper regardless of how it was written.
cred_fill_host() {
  # cred_fill_host <config> <host> -> prints the resolved password line, if any
  printf 'protocol=https\nhost=%s\n\n' "$2" \
    | GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=nonexistent-askpass SSH_ASKPASS=nonexistent-askpass \
      GIT_CONFIG_GLOBAL="$1" GIT_CONFIG_NOSYSTEM=1 GH_TOKEN="$FAKE_TOKEN" \
      git -C "$tmp" credential fill 2>/dev/null | grep '^password=.' || true
}
if [ -n "$(cred_fill_host "$gc7b" github.com)" ] \
   && [ -z "$(cred_fill_host "$gc7b" evil.example)" ] \
   && [ -z "$(cred_fill_host "$gc7b" gitlab.com)" ]; then
  ok "cred: helper answers for github.com and NOT for evil.example / gitlab.com"
else
  bad "cred: helper leaks the token to non-origin hosts (or fails for the origin host)"
fi

# 7g. Helper installed but GH_TOKEN absent from the environment — the reachable
#     production case, since --yes writes the helper GLOBALLY and PERSISTENTLY while
#     GH_TOKEN is per-shell (bootstrap interactively, then run the worker from
#     systemd/cron). git treats an empty `password=` as satisfied, so an
#     exit-status-only probe would report ok while every push fails.
sb7g=$(mktemp -d "$tmp/cred7g.XXXXXX"); stub7g="$tmp/stub7g"
mk_hermetic_bin "$stub7g"
repo7g="$tmp/repo7g"; mk_fake_repo "$repo7g" "https://github.com/pmcfadin/cqlite.git"
gc7g="$sb7g/gitconfig"
cp "$gc7b" "$gc7g" 2>/dev/null || :   # the exact helper config --yes produced in 7b
# Guard the guard: an EMPTY $gc7g would satisfy the warn assertion for the wrong
# reason (no helper at all), making this case vacuous.
if ! grep -q 'x-access-token' "$gc7g" 2>/dev/null; then
  bad "cred: 7g precondition FAILED — no helper installed, the warn below would be vacuous"
fi
out7g=$(PATH="$stub7g" HOME="$sb7g" CARGO_HOME="$sb7g/.cargo" GIT_CONFIG_GLOBAL="$gc7g" \
  GH_TOKEN="" GITHUB_TOKEN="" "$PIN_BS" "$repo7g/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if out_has "$out7g" "\[warn\].*git push has NO credentials" \
   && ! printf '%s' "$out7g" | grep -Eq '\[ok\].*git push credentials resolve'; then
  ok "cred: helper present but GH_TOKEN unset -> WARN (a declining helper is not a credential)"
else
  bad "cred: empty-token case reported ok — probe accepted a non-answer"
  printf '%s\n' "$out7g" | grep -i -A2 "git push"
fi

# 7g-ii. The case the `^password=.` check exists for, which nothing covered: a helper
#        that ANSWERS with a literal EMPTY password line. git treats `password=` as
#        satisfied, so `git credential fill` exits 0 — an exit-status-only probe reports
#        a green machine on which every push fails. Our own helper declines instead of
#        emitting empty (7g), so without this case the non-empty check could be reverted
#        to an exit-status check with the suite still fully green.
sb7ge=$(mktemp -d "$tmp/cred7ge.XXXXXX"); stub7ge="$tmp/stub7ge"
mk_hermetic_bin "$stub7ge"
repo7ge="$tmp/repo7ge"; mk_fake_repo "$repo7ge" "https://github.com/pmcfadin/cqlite.git"
gc7ge="$sb7ge/gitconfig"
git config --file "$gc7ge" --add 'credential.https://github.com.helper' \
  '!f(){ test "$1" = get || exit 0; echo username=x-access-token; echo "password="; };f'
# Sanity: git itself considers this helper "satisfied" (exit 0) — that is the trap.
if printf 'protocol=https\nhost=github.com\n\n' \
   | GIT_CONFIG_GLOBAL="$gc7ge" GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0 \
     GIT_ASKPASS=nonexistent-askpass git -C "$tmp" credential fill >/dev/null 2>&1; then
  ok "cred: (precondition) git credential fill EXITS 0 on an empty password — the trap is real"
else
  bad "cred: (precondition) expected git to accept an empty password line"
fi
out7ge=$(PATH="$stub7ge" HOME="$sb7ge" CARGO_HOME="$sb7ge/.cargo" GIT_CONFIG_GLOBAL="$gc7ge" \
  GH_TOKEN="" GITHUB_TOKEN="" "$PIN_BS" "$repo7ge/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if out_has "$out7ge" "\[warn\].*git push has NO credentials" \
   && ! printf '%s' "$out7ge" | grep -Eq '\[ok\].*git push credentials resolve'; then
  ok "cred: a helper answering with an EMPTY password is not accepted as a credential"
else
  bad "cred: empty-password helper reported ok — the probe trusted exit status"
  printf '%s\n' "$out7ge" | grep -i -A2 "git push"
fi

# 7h. A HANGING credential helper must not hang the bootstrap. Neither
#     GIT_TERMINAL_PROMPT nor GIT_ASKPASS governs a helper SUBPROCESS — real cases are
#     a Git Credential Manager device-code/browser flow, credential-cache waiting on a
#     dead daemon socket, and a locked osxkeychain. This matters beyond the operator:
#     section 3 above runs the real bootstrap against the real REPO_ROOT, so
#     `tooling-tests` probes a developer's ACTUAL helper chain and a hang there would
#     stall the gate of record. This is a deadlock/liveness guard, not a latency
#     budget: the helper sleeps 120s, the probe's own bound is 10s, and the outer
#     ceiling is 60s — ~6x slack, so host load can never flip it.
#     Resolution MUST match the script's (timeout || gtimeout): the fleet is macOS,
#     where GNU coreutils installs `gtimeout` — keying this case off `timeout` alone
#     would skip it on the one platform whose hang scenarios (locked osxkeychain, a GCM
#     browser flow) motivated the bound, leaving it uncovered exactly where it matters.
# MIRRORS PRODUCTION'S CANDIDATE LOOP (#3369 review). Two values, two meanings:
#   TIMEOUT_BIN_TEST  — the FIRST present candidate (what 7h/7hm need: "is there one?")
#   TIMEOUT_KILL_TEST — the first present candidate that ACCEPTS --kill-after, which may
#                       be the SECOND one. A watchdog for a fixture that IGNORES SIGTERM
#                       must be able to escalate to SIGKILL: a plain `timeout` aimed at
#                       such a child waits FOREVER, and `tooling-tests` is a full-gate
#                       component where a hang is worse than a failure (nothing reports
#                       it). Resolved by the SAME behavioural probe the bootstrap uses.
# Checking only the FIRST candidate is how the 7p-o portability defect hid: the harness
# and the code under test disagreed about which binary was in play.
TIMEOUT_BIN_TEST=""
TIMEOUT_KILL_TEST=""
for _tbt_name in timeout gtimeout; do
  _tbt_path="$(command -v "$_tbt_name" 2>/dev/null || true)"
  [ -n "$_tbt_path" ] || continue
  [ -n "$TIMEOUT_BIN_TEST" ] || TIMEOUT_BIN_TEST="$_tbt_path"
  if [ -z "$TIMEOUT_KILL_TEST" ] && "$_tbt_path" --kill-after=1 1 true >/dev/null 2>&1; then
    TIMEOUT_KILL_TEST="$_tbt_path"
  fi
done
unset _tbt_name _tbt_path

# mk_no_killafter_timeouts <dir> — stub EVERY timeout candidate the production loop tries
# (`timeout` THEN `gtimeout`) so it cannot escape past the stub to a real binary further
# along PATH. Stubbing only `timeout` made 7p-o pass on this Linux box (no gtimeout) and
# FAIL on stock macOS, where GNU coreutils installs `gtimeout` — a supported fleet
# platform, per bounded()'s own comment. Each stub rejects --kill-after and otherwise
# delegates to the real binary, so what is measured is the SELECTION logic.
mk_no_killafter_timeouts() {
  local dir="$1" real="$2" name
  for name in timeout gtimeout; do
    mk_stub "$dir" "$name" 'for a in "$@"; do case "$a" in --kill-after*)
      echo "'"$name"': unrecognized option '"'"'$a'"'"'" >&2; exit 125 ;;
    esac; done
exec '"$real"' "$@"'
  done
}
if [ -n "$TIMEOUT_BIN_TEST" ]; then
  sb7h=$(mktemp -d "$tmp/cred7h.XXXXXX"); stub7h="$tmp/stub7h"
  mk_hermetic_bin "$stub7h"
  repo7h="$tmp/repo7h"; mk_fake_repo "$repo7h" "https://github.com/pmcfadin/cqlite.git"
  gc7h="$sb7h/gitconfig"
  git config --file "$gc7h" --add 'credential.https://github.com.helper' '!f(){ sleep 120; };f'
  rc7h=0
  "$TIMEOUT_BIN_TEST" 60 env PATH="$stub7h" HOME="$sb7h" CARGO_HOME="$sb7h/.cargo" GIT_CONFIG_GLOBAL="$gc7h" \
    GH_TOKEN="" "$PIN_BS" "$repo7h/scripts/bootstrap-agent-machine.sh" --skip-smoke >/dev/null 2>&1 || rc7h=$?
  if [ "$rc7h" -ne 124 ]; then
    ok "cred: a hanging credential helper is bounded — bootstrap still completes (rc=$rc7h)"
  else
    bad "cred: bootstrap HUNG on a blocking credential helper (killed at the outer ceiling)"
  fi
  # 7h-ii. The same guard on a STOCK-macOS-SHAPED host: GNU coreutils present only as
  #        `gtimeout`, no plain `timeout`. The fleet is macOS and two of the three hang
  #        scenarios are macOS-only, so a bound that resolves `timeout` alone is inert
  #        exactly where it is needed — and a Linux-only CI would never notice.
  sb7hm=$(mktemp -d "$tmp/cred7hm.XXXXXX"); stub7hm="$tmp/stub7hm"
  mk_hermetic_bin "$stub7hm"
  rm -f "$stub7hm/timeout"                          # <- the macOS shape
  ln -sf "$TIMEOUT_BIN_TEST" "$stub7hm/gtimeout"
  repo7hm="$tmp/repo7hm"; mk_fake_repo "$repo7hm" "https://github.com/pmcfadin/cqlite.git"
  gc7hm="$sb7hm/gitconfig"
  git config --file "$gc7hm" --add 'credential.https://github.com.helper' '!f(){ sleep 120; };f'
  rc7hm=0
  "$TIMEOUT_BIN_TEST" 60 env PATH="$stub7hm" HOME="$sb7hm" CARGO_HOME="$sb7hm/.cargo" \
    GIT_CONFIG_GLOBAL="$gc7hm" GH_TOKEN="" \
    "$PIN_BS" "$repo7hm/scripts/bootstrap-agent-machine.sh" --skip-smoke >/dev/null 2>&1 || rc7hm=$?
  if [ "$rc7hm" -ne 124 ]; then
    ok "cred: the hang bound also applies on a gtimeout-only (macOS-shaped) host (rc=$rc7hm)"
  else
    bad "cred: bootstrap HUNG on a gtimeout-only host — the bound is inert on macOS"
  fi
else
  skip "cred: hanging-helper guard needs timeout/gtimeout (neither on this host)"
fi

# 7e. Re-running --yes must not STACK a second copy of the helper. Bootstrap is
#     documented as idempotent, and a credential.helper list that grows every run
#     is a real footgun (git consults each entry in order).
sb7e=$(mktemp -d "$tmp/cred7e.XXXXXX"); stub7e="$tmp/stub7e"
mk_hermetic_bin "$stub7e"
mk_stub "$stub7e" gh "$GH_STUB_TOKEN_BODY
exit 0"   # setup-git is a no-op -> the fallback helper is used
repo7e="$tmp/repo7e"; mk_fake_repo "$repo7e" "https://github.com/pmcfadin/cqlite.git"
gc7e="$sb7e/gitconfig"
for _ in 1 2; do
  PATH="$stub7e" HOME="$sb7e" CARGO_HOME="$sb7e/.cargo" GIT_CONFIG_GLOBAL="$gc7e" \
    GH_TOKEN="$FAKE_TOKEN" "$PIN_BS" "$repo7e/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke >/dev/null 2>&1
done
out7e=$(PATH="$stub7e" HOME="$sb7e" CARGO_HOME="$sb7e/.cargo" GIT_CONFIG_GLOBAL="$gc7e" \
  GH_TOKEN="$FAKE_TOKEN" "$PIN_BS" "$repo7e/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
helper_count=$(grep -c 'x-access-token' "$gc7e" 2>/dev/null); helper_count="${helper_count:-0}"
if [ "$helper_count" = 1 ]; then
  ok "cred: repeated --yes runs keep exactly one credential helper (idempotent)"
else
  bad "cred: helper stacked across re-runs (count=$helper_count)"
  [ -f "$gc7e" ] && cat "$gc7e"
fi
# On the re-run the probe SUCCEEDS, so the verdict comes from the ok branch — and its
# advisories must see the HOST-SCOPED key this script itself writes. A bare
# `credential.helper` lookup would go silent on exactly the config it just created,
# muting the caveat that matters most to a systemd/cron worker.
if out_has "$out7e" 'reads \$GH_TOKEN from the ENVIRONMENT'; then
  ok "cred: env-dependency caveat fires for the HOST-SCOPED helper the script writes"
else
  bad "cred: env-dependency caveat missed a host-scoped helper"
  printf '%s\n' "$out7e" | grep -i -A2 "git push credentials"
fi

# 7i. Same blind spot on the other advisory: a host-scoped helper at REPO-LOCAL scope
#     with no global one must still raise the "a fresh clone won't inherit it" note.
sb7i=$(mktemp -d "$tmp/cred7i.XXXXXX"); stub7i="$tmp/stub7i"
mk_hermetic_bin "$stub7i"
repo7i="$tmp/repo7i"; mk_fake_repo "$repo7i" "https://github.com/pmcfadin/cqlite.git"
git -C "$repo7i" config --local --add 'credential.https://github.com.helper' \
  '!f(){ test "$1" = get || exit 0; echo username=x; echo password=local-only-secret; };f'
out7i=$(PATH="$stub7i" HOME="$sb7i" CARGO_HOME="$sb7i/.cargo" GIT_CONFIG_GLOBAL="$sb7i/gitconfig" \
  GH_TOKEN="" "$PIN_BS" "$repo7i/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if out_has "$out7i" 'REPO-LOCAL scope only'; then
  ok "cred: repo-local-scope note fires for a HOST-SCOPED local helper"
else
  bad "cred: repo-local-scope note missed a host-scoped local helper"
  printf '%s\n' "$out7i" | grep -i -A3 "git push credentials"
fi

# --- 7p. git PUSH capability is MEASURED, not inferred (issue #3369) --------
# Block 7 covers the CONFIGURATION probe (`git credential fill`). That probe, plus
# `gh auth status` and `git ls-remote origin HEAD`, are all READS — and the box that
# motivated #3369 passed every one of them while every `git push` failed, so the
# launcher's preflight certified a machine on which no lane could start. These cases
# pin the probe that performs THE OPERATION (create + read back + delete a throwaway
# refs/claims/smoke-<commit-sha> ref, via scripts/flow/claim.sh smoke) and — just as
# importantly — pin that an UNKNOWN answer is never reported green.
#
# Hermetic in three layers, because a push probe that escaped the sandbox would mutate
# the REAL origin:
#   1. every case runs a COPY of bootstrap in a throwaway repo, so REPO_ROOT is never
#      this checkout;
#   2. every origin is either a local `file://` bare repo or `push-probe.invalid` — the
#      RFC 2606 guaranteed-unresolvable TLD — never a reachable public remote;
#   3. every whole-checkout run elsewhere in this suite passes --skip-push-probe.

# mk_push_repo <dir> <origin-url|""> — a throwaway checkout that reports ZERO warnings
# except the ones a case deliberately provokes. Getting to zero matters: "All checks
# green." is printed only when WARNINGS is 0, so it is the oracle for BOTH "the probe
# verified" and "the probe withheld green"; a sandbox warning for unrelated reasons
# would make every green assertion here vacuous. (The precondition below MEASURES that
# rather than assuming it.)
mk_push_repo() {
  local dir="$1" url="$2"
  mk_fake_repo "$dir" "$url" with-claim
  mkdir -p "$dir/scripts/lib" "$dir/test-data/datasets/sstables/ks/tbl" "$dir/.home/.cargo"
  cp "$SCRIPT_DIR/../lib/gate-notify.sh" "$dir/scripts/lib/gate-notify.sh"
  cp "$SCRIPT_DIR/../perf-capability.sh" "$dir/scripts/perf-capability.sh"
  # agent-gate.sh is staged for SECTION 5b, not for the push probe (issue #3414 review B1).
  # 5b's verdict asks the gate what it will do with the probed value, so a sandbox without
  # it yields `gate-pin: UNMEASURED` — one extra [warn] in EVERY case built on this helper,
  # which pushed base_warns from 1 to 2 and silently turned three end-to-end assertions
  # below (the absolute-green, AC1+AC3 exit-0, and one-warning cases) into `skip`s. Skips
  # are neither pass nor fail, so the suite stayed at FAIL=0 while the case whose own
  # comment records that its absence "let a defect through with 102 tests green" stopped
  # running. Staging it here plus the pinned `sudo` shim in mk_push_bin makes 5b contribute
  # ZERO warnings deterministically — on a pinned host and an unpinned one alike.
  cp "$SCRIPT_DIR/../agent-gate.sh" "$dir/scripts/agent-gate.sh"
  # A PER-SANDBOX system env file carrying the pin, pointed at by run_push. Section 5b's
  # VERIFIED now requires BOTH the file line and a session that sees it (roborev round 2),
  # so without this the sandboxes would take the new NOT-SYSTEM-WIDE branch, base_warns
  # would go back to 2, and the three end-to-end cases would silently skip again — the
  # round-4 defect returning through the round-5 fix. Per-sandbox rather than the
  # suite-wide seam ALSO removes an order-dependence that was already latent: the shared
  # file is appended to by whichever earlier `--yes` case runs first, so what these cases
  # measured depended on suite ordering.
  # SCCACHE_CACHE_SIZE joins the pin for the same reason and with the same history (issue
  # #3727): section 5b2's VERIFIED requires the file line AND a session that sees it AND a
  # running server enforcing the bytes it means, so without this every sandbox here gains a
  # `sccache-cap: FAILED` warn, `base_warns` goes 1 -> 2, and the three green-path cases below
  # silently skip — the drift this file's own comments record FOUR times. The value pairs with
  # mk_push_bin's sudo shim and sccache stub, so 5b2 contributes ZERO warnings deterministically
  # on a capped host and an uncapped one alike.
  printf 'CQLITE_GATE_MAX_CONCURRENCY=1\nSCCACHE_CACHE_SIZE=30G\n' >"$dir/etc-environment"
  : >"$dir/test-data/datasets/sstables/ks/tbl/nb-1-big-Data.db"
}

# mk_push_bare <dir> [pre-receive-body] — a local bare repo that accepts pushes. With a
# body, its pre-receive hook decides the push's fate OFFLINE, which is how the FAILED
# verdicts below are produced deterministically and without a network.
mk_push_bare() {
  local dir="$1" hook="${2:-}"
  git -c init.defaultBranch=main init -q --bare "$dir" >/dev/null 2>&1
  if [ -n "$hook" ]; then
    mkdir -p "$dir/hooks"
    printf '#!/usr/bin/env bash\n%s\n' "$hook" >"$dir/hooks/pre-receive"
    chmod +x "$dir/hooks/pre-receive"
  fi
}

# mk_push_gh <dir> [setup-git-body] — a gh stub that satisfies the auth + board
# sections (so neither contributes a warning) and runs <setup-git-body> on
# `gh auth setup-git`.
mk_push_gh() {
  local dir="$1" setup="${2:-:}"
  cat >"$dir/gh" <<EOF
#!/usr/bin/env bash
case "\$1" in
  auth)
    if [ "\$2" = status ]; then
      echo "github.com"
      echo "  ✓ Logged in to github.com account tester (GH_TOKEN)"
      echo "  - Token scopes: 'gist', 'project', 'read:org', 'repo', 'workflow'"
    elif [ "\$2" = token ]; then
      # As real gh: the environment token IS github.com's token, and any other host has
      # none here (#3369 FIX L gates the fallback repair on this answer).
      want=""; shift 2
      while [ \$# -gt 0 ]; do [ "\$1" = --hostname ] && { want="\$2"; shift; }; shift; done
      case "\${want:-github.com}" in
        github.com) echo "\${GH_TOKEN:-\${GITHUB_TOKEN:-}}" ;;
        *) echo "no oauth token found for \$want" >&2; exit 1 ;;
      esac
      exit 0
    elif [ "\$2" = setup-git ]; then
      $setup
    fi
    exit 0 ;;
  project) echo '{"id":"PVT_stub"}'; exit 0 ;;
  api)     echo '{"data":{"node":{"id":"PVT_stub"}}}'; exit 0 ;;
esac
exit 0
EOF
  chmod +x "$dir/gh"
}

# mk_push_bin <dir> [setup-git-body] — PATH stubs that keep every OTHER section quiet.
# `uname` reports Darwin so the Linux-only mold + perf sections (whose verdicts depend
# on the HOST's kernel settings) cannot leak host state into these cases.
mk_push_bin() {
  local dir="$1" setup="${2:-:}"
  mkdir -p "$dir"
  # LINUX, plus a perf stub — changed by finding DD (#3414 round 14). This sandbox reported
  # Darwin purely to skip the Linux-only perf section. But DD made a non-Linux host
  # permanently NON-PASSING (with no system-wide file there is nothing to correlate a
  # session value against), so Darwin now costs a gate-pin warn instead: `base_warns` went
  # 1 -> 2 and the three green-path cases silently skipped for the FOURTH time. Measured
  # both ways before choosing — Darwin: gate-pin warns; Linux: gate-pin passes and only the
  # perf section warns. So the sandbox becomes Linux and the one section that made Darwin
  # attractive is satisfied directly. It also makes the green-path cases MORE meaningful:
  # "this box certifies green" is now a Linux-only claim, so they must model a Linux box.
  mk_stub "$dir" uname 'echo Linux'
  # perf stat is invoked with CSV output, so the stub emits `<count>,,cycles` on stderr as
  # the real one does — a human-formatted row parses as no-cycles-row (verified against the
  # shipped awk rather than guessed).
  mk_stub "$dir" perf 'case "$*" in *stat*) echo "1234567,,cycles" >&2 ;; esac
exit 0'
  # A REAL-SHAPED sccache (issue #3727): section 5b2 asks it for the value->bytes map through
  # an isolated read AND for the running server's enforced cap, so a bare `exit 0` yields
  # `sccache-cap: UNMEASURED` — one extra warn in every sandbox here. SCC_STUB_MAX pairs with
  # the 30G in mk_push_repo's env file (30 GiB = 32212254720 bytes).
  mk_stub "$dir" sccache "$scc_stub_body"
  mk_stub "$dir" cargo-nextest 'exit 0'
  mk_stub "$dir" cargo 'exit 0'
  mk_stub "$dir" roborev 'exit 0'
  # A `sudo` that stands in for a PAM session on a PINNED box (issue #3414 review B1).
  # Without it these sandboxes fall through to the REAL sudo and the REAL /etc/environment,
  # so section 5b's verdict — and therefore the warning count every case here measures —
  # would depend on whether the HOST running the suite happens to be pinned. That is the
  # host-dependence this file removes everywhere else (GIT_CONFIG_GLOBAL, the board env,
  # the datasets stub); 5b is simply the newest place it could leak in. Section 3b never
  # invokes sudo and the perf section is skipped here (`uname` reports Darwin), so this
  # shim's only subject is 5b.
  # SCCACHE_CACHE_SIZE is injected alongside the pin (issue #3727) so section 5b2's verdict —
  # and therefore the warning count every case here measures — does not depend on whether the
  # HOST running the suite happens to be capped.
  # `-i` is stripped here for the reason mkpinshims records: 5b2 probes the login form too, and an
  # unhandled `-i` would add a warn to every sandbox built on this helper — the base_warns drift.
  # The SAME value is injected for both session types, so 5b2's two-session comparison agrees and
  # this helper stays a one-warning sandbox.
  # SCCACHE_DIR is injected alongside the cap for the reason mksccshims records: 5b2 scrubs the
  # caller's SCCACHE_* before opening the session, so a stub that injects only the cap manufactures a
  # ROUTING disagreement with the invoking context and every sandbox here gains a warn.
  mk_stub "$dir" sudo 'while [ "${1:-}" = "-n" ]; do shift; done
if [ "${1:-}" = "-u" ]; then shift 2; fi
if [ "${1:-}" = "-i" ]; then shift; fi
exec env CQLITE_GATE_MAX_CONCURRENCY=1 SCCACHE_CACHE_SIZE=30G ${SCC_STUB_SESSION_DIR:+SCCACHE_DIR="$SCC_STUB_SESSION_DIR"} "$@"'
  mk_push_gh "$dir" "$setup"
}

# run_push <repo> <bin> <gitconfig> [bootstrap args...] — sets push_out and push_rc in
# the CALLER's shell. Deliberately NOT `out=$(run_push …)`: a command substitution runs
# the function in a subshell, so push_rc would be discarded — and the EXIT CODE is half
# of what these cases assert (--strict). Never --yes: nothing here may install. Always
# --skip-smoke: the gate fmt run is a different subject (and minutes long).
push_rc=0
push_out=""
run_push() {
  local repo="$1" bin="$2" gc="$3"; shift 3
  push_rc=0
  # SCC_STUB_MAX = 30 GiB in bytes: the cap the sccache stub reports as the RUNNING server's,
  # matching the 30G in this sandbox's env file and sudo shim (issue #3727).
  push_out=$(PATH="$bin:$PATH" HOME="$repo/.home" CARGO_HOME="$repo/.home/.cargo" \
    CQLITE_BOOTSTRAP_ENV_FILE="$repo/etc-environment" SCC_STUB_MAX=32212254720 \
    SCCACHE_CACHE_SIZE=30G SCCACHE_DIR="$repo/scc-cache" SCC_STUB_SESSION_DIR="$repo/scc-cache" \
    GIT_CONFIG_GLOBAL="$gc" GIT_CONFIG_NOSYSTEM=1 CLAIM_MACHINE=push-probe-test \
    CODEX_NOTIFY_WEBHOOK='https://ntfy.example.com/t' \
    CQLITE_PROJECT_OWNER=pmcfadin CQLITE_PROJECT_NUMBER=1 \
    "$PIN_BS" "$repo/scripts/bootstrap-agent-machine.sh" --skip-smoke "$@" 2>&1) || push_rc=$?
}
# ANSI colour is stripped with a printf-built ESC, not a `\x1b` escape: BSD sed (the
# fleet's macOS hosts) does not understand \x1b and would silently match nothing.
PUSH_ESC=$(printf '\033')
push_plain()  { printf '%s' "$1" | sed "s/${PUSH_ESC}\[[0-9;]*m//g"; }
# Count only real warn LINES. `grep -cF '[warn]'` also matched the summary's
# "Address the [warn] lines above", making every count one too high — and the counts
# below are the whole basis of the delta assertion.
push_warns()  { push_plain "$1" | grep -cE '^[[:space:]]+\[warn\] '; }
push_green()  { out_has "$1" -F 'All checks green.'; }
push_verdict(){ push_plain "$1" | grep -F 'git-push:'; }

# 7p-a/d. THE POSITIVE CONTROL and the OPT-OUT, measured as a pair against ONE sandbox
#   whose only variable is the flag. Run the opt-out FIRST so its warning count
#   establishes what the sandbox costs before the probe is even considered.
bare7pa="$tmp/bare7pa.git"; mk_push_bare "$bare7pa"
repo7pa="$tmp/repo7pa"; mk_push_repo "$repo7pa" "file://$bare7pa"
bin7pa="$tmp/bin7pa"; mk_push_bin "$bin7pa"
gc7pa="$tmp/gc7pa"; : >"$gc7pa"
run_push "$repo7pa" "$bin7pa" "$gc7pa" --skip-push-probe --strict; out7pd=$push_out; rc7pd=$push_rc
run_push "$repo7pa" "$bin7pa" "$gc7pa" --strict; out7pa=$push_out; rc7pa=$push_rc
base_warns=$(push_warns "$out7pd"); probe_warns=$(push_warns "$out7pa")

if out_has "$out7pa" '\[ok\].*git-push: VERIFIED' \
   && out_has "$out7pa" 'refs/claims/\*'; then
  ok "push: a REAL push (create+ls-remote+delete on a local bare repo) is reported VERIFIED as [ok]"
else
  bad "push: the probe could not report VERIFIED even against a bare repo that accepts pushes"
  push_verdict "$out7pa"
fi
# The delta is host-independent: the ONLY difference between the two runs is the flag.
if [ "$probe_warns" -eq $((base_warns - 1)) ]; then
  ok "push: a VERIFIED probe adds no warning, and --skip-push-probe adds exactly one"
else
  bad "push: warning delta wrong (opt-out=$base_warns verified=$probe_warns)"
fi
if out_has "$out7pd" '\[warn\].*git-push: OPT-OUT (--skip-push-probe)'; then
  ok "push: --skip-push-probe emits a LOUD [warn] OPT-OUT line (it cannot buy a silent pass)"
else
  bad "push: --skip-push-probe was silent or reported ok"
  push_verdict "$out7pd"
fi
if [ "$rc7pd" -ne 0 ] && ! push_green "$out7pd"; then
  ok "push: --skip-push-probe WITHHOLDS 'All checks green.' and fails --strict (rc=$rc7pd)"
else
  bad "push: the opt-out bought a green/zero-exit run (rc=$rc7pd)"
fi
# Absolute-green assertions need the sandbox to be otherwise-warning-free. MEASURE that
# (baseline = exactly the one OPT-OUT warning) instead of assuming it: an exotic host
# that warns for its own reasons must not produce a mystery red here.
# THE BASELINE ITSELF IS AN ASSERTION (issue #3414 review B1). The three cases below are
# guarded on `base_warns -eq 1` and print a `skip` otherwise — which is correct as a safety
# net but SILENT as a signal: when section 5b began emitting an extra warning in every
# sandbox, base_warns went 1 -> 2, all three stopped running, and the suite still reported
# FAIL=0. Asserting the baseline catches that drift at its cause instead of letting it
# disable assertions one by one. If this reds, a section has started warning in the clean
# sandbox; find it before touching the cases below.
if [ "$base_warns" -eq 1 ]; then
  ok "push: the clean sandbox costs exactly ONE warning (the opt-out) — the exit-0/green cases below can run"
else
  bad "push: sandbox baseline drifted to $base_warns warnings — the three end-to-end cases below will SKIP, not fail"
  push_plain "$out7pd" | grep -E '^[[:space:]]+\[warn\] ' | head -4
fi

if [ "$base_warns" -eq 1 ]; then
  if push_green "$out7pa" && [ "$rc7pa" -eq 0 ]; then
    ok "push: VERIFIED yields 'All checks green.' and --strict exits 0 (zero warnings)"
  else
    bad "push: a verified machine did not go green / --strict did not exit 0 (rc=$rc7pa)"
    push_verdict "$out7pa"
  fi
else
  skip "push: absolute-green assertions need an otherwise-clean sandbox (baseline=$base_warns warnings)"
  printf '%s' "$out7pd" | grep -F '[warn]' | sed 's/\x1b\[[0-9;]*m//g' | head -5
fi

# 7p-b. PUSH FAILS, credential-shaped. The bare repo's pre-receive hook speaks the
#   signature claim.sh classifies as auth, so this needs no network and no real token.
bare7pb="$tmp/bare7pb.git"; mk_push_bare "$bare7pb" 'echo "Authentication failed" >&2; exit 1'
repo7pb="$tmp/repo7pb"; mk_push_repo "$repo7pb" "file://$bare7pb"
bin7pb="$tmp/bin7pb"; mk_push_bin "$bin7pb"
gc7pb="$tmp/gc7pb"; : >"$gc7pb"
run_push "$repo7pb" "$bin7pb" "$gc7pb" --strict; out7pb=$push_out; rc7pb=$push_rc
if out_has "$out7pb" '\[warn\].*git-push: FAILED.*AUTHENTICATE' \
   && ! push_green "$out7pb" && [ "$rc7pb" -ne 0 ]; then
  ok "push: a rejected push is a [warn] FAILED naming authentication, green withheld, --strict exits $rc7pb"
else
  bad "push: credential-shaped rejection not reported as FAILED (rc=$rc7pb, green=$(push_green "$out7pb" && echo yes || echo no))"
  push_verdict "$out7pb"
fi
# THIS CASE'S REMOTE IS `file://`, so its subject is the NON-https advice. The assertion
# used to claim "HTTPS auth failure" while driving exactly this file:// remote — it
# passed only because the advice branched on `!= ssh` and swept `other` into the https
# arm, i.e. the test asserted a property it never exercised (#3369 review). The genuine
# https path is 7p-q below.
if out_has "$out7pb" "remote 'origin' is a 'other' remote" \
   && out_has "$out7pb" 'credential helper may not apply' \
   && ! out_has "$(push_plain "$out7pb" | grep -E '^ *fix:')" 'gh auth setup-git'; then
  ok "push: a file:// remote's auth-shaped failure gets protocol-neutral advice, NOT https credential advice"
else
  bad "push: a non-https remote was given https credential advice"
  push_plain "$out7pb" | grep -E 'fix:|remote is a' | head -4
fi

# 7p-b2. PUSH FAILS, namespace-shaped: a rejection git's stderr does NOT identify as a
#   credential fault must not be mislabelled as one — and the DEFAULT (no --strict) run
#   must still exit 0, which is the composability contract this script has always had.
bare7pb2="$tmp/bare7pb2.git"; mk_push_bare "$bare7pb2" 'echo "denied by ref policy" >&2; exit 1'
repo7pb2="$tmp/repo7pb2"; mk_push_repo "$repo7pb2" "file://$bare7pb2"
bin7pb2="$tmp/bin7pb2"; mk_push_bin "$bin7pb2"
gc7pb2="$tmp/gc7pb2"; : >"$gc7pb2"
run_push "$repo7pb2" "$bin7pb2" "$gc7pb2"; out7pb2=$push_out; rc7pb2=$push_rc
# The catch-all QUOTES claim.sh's verdict rather than re-wording it (#3369 review): a
# re-worded catch-all mis-attributed every unrecognised reason code and discarded detail
# claim.sh had just been fixed to report. So the assertion is that the ORIGINAL verdict
# line survives into bootstrap's output, and that bootstrap adds no cause of its own.
# Predicates via out_has (see its note): MEASURED on a pristine origin/main worktree at
# 8cfaea852, this case FAILS — `printf | grep -q` returned 141 while the matching text was present
# (this payload is over 64 KiB, where the race is effectively certain), so the case reported the
# opposite of what it measured. That red is
# on `main` and is NOT caused by this branch's diff; it is converted here because the fix is one
# line of the idiom this file already documents, and leaving a known-false red in place is worse
# than a slightly wider diff.
if out_has "$out7pb2" '\[warn\].*git-push: FAILED' \
   && out_has "$(push_plain "$out7pb2")" '^ *CLAIM: SMOKE-FAIL.*reason=push-rejected' \
   && ! out_has "$out7pb2" 'git-push: FAILED.*AUTHENTICATE'; then
  ok "push: an unrecognised SMOKE-FAIL is QUOTED verbatim (reason survives; no auth mis-attribution)"
else
  bad "push: catch-all re-classified instead of quoting"
  push_verdict "$out7pb2"
fi
if [ "$rc7pb2" -eq 0 ] && ! push_green "$out7pb2"; then
  ok "push: WITHOUT --strict the script still exits 0 despite warnings (contract preserved)"
else
  bad "push: default exit contract broken (rc=$rc7pb2)"
fi

# 7p-c. THE MOST IMPORTANT CASE: an UNKNOWN answer must not inherit the permissive
#   branch. The remote does not exist, so NOTHING was learned about push capability —
#   the verdict must be UNMEASURED, a [warn], and must never be an [ok].
repo7pc="$tmp/repo7pc"; mk_push_repo "$repo7pc" "file://$tmp/no-such-bare.git"
bin7pc="$tmp/bin7pc"; mk_push_bin "$bin7pc"
gc7pc="$tmp/gc7pc"; : >"$gc7pc"
run_push "$repo7pc" "$bin7pc" "$gc7pc"; out7pc=$push_out; rc7pc=$push_rc
if out_has "$out7pc" '\[warn\].*git-push: UNMEASURED' \
   && out_has "$out7pc" 'git-push: UNMEASURED.*UNKNOWN, not ok'; then
  ok "push: an unreachable remote is UNMEASURED, and the text names it as UNKNOWN"
else
  bad "push: unreachable remote did not produce the UNMEASURED verdict"
  push_verdict "$out7pc"
fi
if ! out_has "$out7pc" '\[ok\].*git-push' && ! push_green "$out7pc"; then
  ok "push: an UNMEASURED probe is NEVER [ok] and NEVER green (affirmative-measurement rule)"
else
  bad "push: an unmeasured push capability took the permissive branch"
  push_verdict "$out7pc"
fi

# 7p-c2. The other unmeasurable shape: no origin remote at all.
repo7pc2="$tmp/repo7pc2"; mk_push_repo "$repo7pc2" ""
bin7pc2="$tmp/bin7pc2"; mk_push_bin "$bin7pc2"
gc7pc2="$tmp/gc7pc2"; : >"$gc7pc2"
run_push "$repo7pc2" "$bin7pc2" "$gc7pc2"; out7pc2=$push_out
if out_has "$out7pc2" "\[warn\].*git-push: UNMEASURED.*no 'origin' remote" \
   && ! out_has "$out7pc2" '\[ok\].*git-push'; then
  ok "push: no origin remote -> UNMEASURED [warn], never [ok]"
else
  bad "push: missing origin was not reported as UNMEASURED"
  push_verdict "$out7pc2"
fi

# 7p-c3. THE MUTATION THIS BLOCK EXISTS FOR. The probe delegates to claim.sh, so the
#   permissive branch must be keyed on the AFFIRMATIVE `SMOKE-OK`, never on the ABSENCE
#   of `SMOKE-FAIL`: a probe that produced NO verdict at all (killed, or a claim.sh
#   whose output contract moved) would otherwise be read as success. Verified as a real
#   mutation: rewriting the branch to `! grep SMOKE-FAIL` leaves every OTHER case in
#   this block green, and only this one reds. The remote here is a bare repo that
#   ACCEPTS pushes, so reachability is not what is being measured — the verdict is.
repo7pc3="$tmp/repo7pc3"; bare7pc3="$tmp/bare7pc3.git"
mk_push_bare "$bare7pc3"
mk_push_repo "$repo7pc3" "file://$bare7pc3"
printf '#!/usr/bin/env bash\nexit 0\n' >"$repo7pc3/scripts/flow/claim.sh"   # mute: no verdict
chmod +x "$repo7pc3/scripts/flow/claim.sh"
bin7pc3="$tmp/bin7pc3"; mk_push_bin "$bin7pc3"
gc7pc3="$tmp/gc7pc3"; : >"$gc7pc3"
run_push "$repo7pc3" "$bin7pc3" "$gc7pc3"; out7pc3=$push_out
if out_has "$out7pc3" '\[warn\].*git-push: UNMEASURED.*no SMOKE-OK/SMOKE-FAIL verdict' \
   && ! out_has "$out7pc3" '\[ok\].*git-push'; then
  ok "push: a probe that returns NO verdict is UNMEASURED — success is keyed on SMOKE-OK, not on the absence of failure"
else
  bad "push: a verdict-less probe was not reported UNMEASURED (the permissive branch is keyed on '!= failed')"
  push_verdict "$out7pc3"
fi

# 7p-e. ORDERING: the probe must run AFTER section 3b's credential fix, so what it
#   measures is the machine as the fix LEFT it. Asserted as a SEQUENCE, not as
#   co-presence: the `gh auth setup-git` stub is what makes the push reachable at all
#   (it installs the url.<local>.insteadOf rewrite), so a probe running BEFORE the fix
#   cannot possibly report VERIFIED — and the negative twin below proves it does not.
#   The origin is `push-probe.invalid` (RFC 2606: guaranteed not to resolve), so the
#   unwired run fails offline instead of reaching a real host.
bare7pe="$tmp/bare7pe.git"; order7pe="$tmp/order7pe.log"; : >"$order7pe"
mk_push_bare "$bare7pe" "echo PUSH >>\"$order7pe\"; exit 0"
repo7pe="$tmp/repo7pe"; mk_push_repo "$repo7pe" "https://push-probe.invalid/cqlite.git"
bin7pe="$tmp/bin7pe"
mk_push_bin "$bin7pe" "echo SETUP-GIT >>\"$order7pe\"
      git config --global --add 'credential.https://push-probe.invalid.helper' '!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=wired-by-setup-git; };f'
      git config --global \"url.file://$bare7pe/.insteadOf\" 'https://push-probe.invalid/cqlite.git'"
# Negative twin FIRST: without --fix-credentials nothing wires the machine, so the
# probe must NOT report VERIFIED. Without this, "VERIFIED after the fix" would not
# distinguish a probe that ran after the fix from one that always passes.
gc7pe1="$tmp/gc7pe1"; : >"$gc7pe1"
run_push "$repo7pe" "$bin7pe" "$gc7pe1"; out7pe1=$push_out
twin_log=$(tr '\n' ' ' <"$order7pe")
if ! out_has "$out7pe1" 'git-push: VERIFIED' && [ -z "${twin_log// /}" ]; then
  ok "push: (negative twin) with no credential fix the probe never reaches the remote and never VERIFIES"
else
  bad "push: the unwired machine reported VERIFIED (log=[$twin_log])"
  push_verdict "$out7pe1"
fi
: >"$order7pe"; gc7pe2="$tmp/gc7pe2"; : >"$gc7pe2"
run_push "$repo7pe" "$bin7pe" "$gc7pe2" --fix-credentials; out7pe2=$push_out
order_seq=$(sed 's/[^A-Z-]//g' "$order7pe" | tr '\n' ' ' | tr -s ' ')
if out_has "$out7pe2" '\[ok\].*git-push: VERIFIED' \
   && out_has "$order_seq" '^SETUP-GIT PUSH'; then
  ok "push: the probe runs AFTER the credential fix — observed order [$order_seq], verdict VERIFIED"
else
  bad "push: fix/probe ordering not established (order=[$order_seq])"
  push_verdict "$out7pe2"
fi

# 7p-f. --fix-credentials is NARROW: it wires credentials and installs NOTHING. Turning
#   the launcher's VERIFY step into a full toolchain installer (which reusing --yes
#   would have done) is a far larger change to an external contract than #3369 needs,
#   so the install tripwire must stay empty.
trip7pf="$tmp/tripwire7pf.log"; : >"$trip7pf"
bin7pf="$tmp/bin7pf"
mk_push_bin "$bin7pf" "git config --global --add 'credential.https://push-probe.invalid.helper' '!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=wired-by-setup-git; };f'"
for shim in brew cargo roborev; do mkshim "$shim" "$bin7pf" "$trip7pf"; done
repo7pf="$tmp/repo7pf"; mk_push_repo "$repo7pf" "https://push-probe.invalid/cqlite.git"
gc7pf="$tmp/gc7pf"; : >"$gc7pf"
run_push "$repo7pf" "$bin7pf" "$gc7pf" --fix-credentials; out7pf=$push_out
if grep -qF 'push-probe.invalid' "$gc7pf" 2>/dev/null && [ ! -s "$trip7pf" ]; then
  ok "push: --fix-credentials wires the credential helper and installs NOTHING (tripwire empty)"
else
  bad "push: --fix-credentials was not narrow (helper=$(grep -c . "$gc7pf" 2>/dev/null) tripwire=$(wc -l <"$trip7pf" 2>/dev/null))"
  [ -s "$trip7pf" ] && cat "$trip7pf"
fi

# 7p-j. THE AC1+AC3 END-TO-END CASE — the one whose absence let a defect through with
#   102 tests green. A fresh UNWIRED box (no credential helper, exactly the pinned-AMI
#   state) + `--fix-credentials --strict` must end at exit 0 AND print
#   "All checks green.". The first implementation warned about the missing helper BEFORE
#   repairing it and could not retract that warning, so verify.run FAILED on a box it had
#   just successfully repaired — AC1 and AC3 both defeated, invisibly. The §3b verdict is
#   now emitted once, after the repair, on the FINAL state.
bare7pj="$tmp/bare7pj.git"; mk_push_bare "$bare7pj"
repo7pj="$tmp/repo7pj"; mk_push_repo "$repo7pj" "https://push-probe.invalid/cqlite.git"
bin7pj="$tmp/bin7pj"
mk_push_bin "$bin7pj" "git config --global --add 'credential.https://push-probe.invalid.helper' '!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=wired-by-setup-git; };f'
      git config --global \"url.file://$bare7pj/.insteadOf\" 'https://push-probe.invalid/cqlite.git'"
gc7pj="$tmp/gc7pj"; : >"$gc7pj"   # UNWIRED: no helper, no rewrite, as the image ships
run_push "$repo7pj" "$bin7pj" "$gc7pj" --fix-credentials --strict; out7pj=$push_out; rc7pj=$push_rc
if out_has "$out7pj" '\[ok\].*git credentials WIRED BY THIS RUN' \
   && ! out_has "$out7pj" '\[warn\].*git push has NO credentials' \
   && out_has "$out7pj" '\[ok\].*git-push: VERIFIED'; then
  ok "push: an unwired box repaired by --fix-credentials reports ONE [ok] verdict — no pre-repair warning survives the repair"
else
  bad "push: the repaired box still carries a credential WARNING (verify.run would fail on a box it just fixed)"
  push_plain "$out7pj" | grep -E 'credential|git-push' | head -6
fi
if [ "$base_warns" -eq 1 ]; then
  if [ "$rc7pj" -eq 0 ] && push_green "$out7pj" && [ "$(push_warns "$out7pj")" -eq 0 ]; then
    ok "push: AC1+AC3 end to end — unwired box + --fix-credentials --strict => exit 0 AND 'All checks green.'"
  else
    bad "push: repaired box did not certify (rc=$rc7pj warns=$(push_warns "$out7pj") green=$(push_green "$out7pj" && echo yes || echo no))"
    push_plain "$out7pj" | grep -E '\[warn\]' | head -5
  fi
else
  skip "push: AC1+AC3 exit-0 assertion needs an otherwise-clean sandbox (baseline=$base_warns warnings)"
fi

# 7p-k. AN UNSUCCESSFUL CLEANUP DELETE (#3369 blocker 2). `cmd_smoke` used to emit SMOKE-OK — text and
#   all: "(create + ls-remote + delete verified)" — after only a stderr `note` when the
#   cleanup delete failed. Bootstrap then reported VERIFIED and passed --strict on a
#   machine that had just stranded a ref on the shared origin: a verdict claiming more
#   than it measured, the same shape as the §3b wording defect one layer down.
bare7pk="$tmp/bare7pk.git"
mk_push_bare "$bare7pk" 'zero=0000000000000000000000000000000000000000
while read -r old new ref; do
  if [ "$new" = "$zero" ]; then echo "deletion of $ref denied by policy" >&2; exit 1; fi
done
exit 0'
repo7pk="$tmp/repo7pk"; mk_push_repo "$repo7pk" "file://$bare7pk"
bin7pk="$tmp/bin7pk"; mk_push_bin "$bin7pk"
gc7pk="$tmp/gc7pk"; : >"$gc7pk"
run_push "$repo7pk" "$bin7pk" "$gc7pk" --strict; out7pk=$push_out; rc7pk=$push_rc
if out_has "$out7pk" '\[warn\].*git-push: FAILED' \
   && out_has "$(push_plain "$out7pk")" 'reason=cleanup-unverified' \
   && ! out_has "$out7pk" 'git-push: VERIFIED' \
   && ! push_green "$out7pk" && [ "$rc7pk" -ne 0 ]; then
  ok "push: an unsuccessful cleanup delete is FAILED, never VERIFIED (green withheld, --strict exits $rc7pk)"
else
  bad "push: delete failure was reported as success (rc=$rc7pk)"
  push_verdict "$out7pk"
fi
if out_has "$(push_plain "$out7pk")" "git ls-remote .* refs/claims/smoke-"; then
  ok "push: the quoted verdict reaches the operator with the ls-remote check for the possibly-stranded ref"
else
  bad "push: cleanup-unverified verdict lost its stray-ref guidance in transit"
  push_plain "$out7pk" | grep -E 'CLAIM:|git-push' | head -3
fi
# NO CAUSE MAY BE ATTRIBUTED (#3369 review). One nonzero exit cannot tell a deletion
# policy from a network drop from a post-readback auth failure, so neither claim.sh nor
# bootstrap may name one — and in particular bootstrap must not fall back to credential
# advice, which was the wrong-remedy defect one round earlier.
# Predicates via out_has, NOT `printf | grep -q`: this output is over 64 KiB, where the pipeline
# form's race is effectively certain and the case reports the opposite of what it measured (see
# out_has).
if ! out_has "$out7pk" 'gh auth setup-git' \
   && ! out_has "$out7pk" -- '--fix-credentials' \
   && ! out_has "$out7pk" -i 'ref-deletion policy' \
   && out_has "$(push_plain "$out7pk")" 'no cause is attributed'; then
  ok "push: a failed cleanup attributes NO cause and gives no credential advice — it reports the observation"
else
  bad "push: an unsupportable cause (or credential advice) was attached to a failed cleanup"
  push_plain "$out7pk" | grep -E 'CLAIM:|cause|setup-git' | head -4
fi
# The verdict must come from claim.sh's ANCHORED verdict line AND its exit status, not
# from a substring anywhere in the captured stream. A claim.sh that prints the token in
# prose (or on stderr) and then FAILS must not pass.
printf '#!/usr/bin/env bash\necho "hint: a healthy run prints CLAIM: SMOKE-OK here" >&2\nexit 1\n' \
  >"$repo7pk/scripts/flow/claim.sh"
chmod +x "$repo7pk/scripts/flow/claim.sh"
run_push "$repo7pk" "$bin7pk" "$gc7pk"; out7pk2=$push_out
if out_has "$out7pk2" '\[warn\].*git-push: UNMEASURED' \
   && ! out_has "$out7pk2" '\[ok\].*git-push'; then
  ok "push: the SMOKE-OK token in unanchored prose (plus a nonzero exit) does NOT satisfy the probe"
else
  bad "push: a prose mention of SMOKE-OK was accepted as the verdict"
  push_verdict "$out7pk2"
fi

# 7p-l. ONE REMOTE, RESOLVED ONCE (#3369 review). The credential half used to read
#   `origin`'s FETCH url while the push probe pushed to `${CLAIM_REMOTE:-origin}` and
#   honoured any `pushurl` — so the run could wire and bless host A while pushing to host
#   B. Both divergences are covered: a non-origin CLAIM_REMOTE, and an origin whose
#   pushurl differs from its fetch url.
#
#   (i) CLAIM_REMOTE names a different remote. `origin` here is a local bare repo that
#   needs no credential at all; if the credential half still read `origin`, it would emit
#   the "'other' remote — no credential helper applies" line and never repair anything,
#   and the push to `upstream` would then be unwired.
bare7pl="$tmp/bare7pl.git"; mk_push_bare "$bare7pl"
repo7pl="$tmp/repo7pl"; mk_push_repo "$repo7pl" "file://$tmp/decoy7pl.git"
git -C "$repo7pl" remote add upstream "https://push-probe.invalid/cqlite.git" >/dev/null 2>&1
bin7pl="$tmp/bin7pl"
mk_push_bin "$bin7pl" "git config --global --add 'credential.https://push-probe.invalid.helper' '!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=wired; };f'
      git config --global \"url.file://$bare7pl/.insteadOf\" 'https://push-probe.invalid/cqlite.git'"
gc7pl="$tmp/gc7pl"; : >"$gc7pl"
export CLAIM_REMOTE=upstream      # unset immediately after: it must not leak into later cases
run_push "$repo7pl" "$bin7pl" "$gc7pl" --fix-credentials --strict; out7pl=$push_out; rc7pl=$push_rc
unset CLAIM_REMOTE
if out_has "$out7pl" '\[ok\].*git credentials WIRED BY THIS RUN.*push-probe.invalid' \
   && out_has "$out7pl" "\[ok\].*git-push: VERIFIED.*'upstream'" \
   && ! out_has "$out7pl" "no credential helper applies"; then
  ok "push: with CLAIM_REMOTE set, the credential half and the push probe address the SAME remote"
else
  bad "push: credential half and push probe addressed different remotes (rc=$rc7pl)"
  push_plain "$out7pl" | grep -E 'credential|git-push|remote' | head -6
fi

#   (ii) `origin` with a pushurl that differs from its fetch url. `git push` honours the
#   pushurl, so the credential subject is the PUSH host — reading the fetch url would
#   classify this as a local 'other' remote needing no helper at all.
repo7pl2="$tmp/repo7pl2"; mk_push_repo "$repo7pl2" "file://$tmp/decoy7pl2.git"
git -C "$repo7pl2" remote set-url --push origin "https://push-probe.invalid/cqlite.git" >/dev/null 2>&1
bin7pl2="$tmp/bin7pl2"; mk_push_bin "$bin7pl2"
gc7pl2="$tmp/gc7pl2"; : >"$gc7pl2"
run_push "$repo7pl2" "$bin7pl2" "$gc7pl2"; out7pl2=$push_out
if out_has "$out7pl2" '\[warn\].*git push has NO credentials for push-probe.invalid' \
   && ! out_has "$out7pl2" "no credential helper applies"; then
  ok "push: an origin with a differing pushurl is judged on its PUSH host, not its fetch host"
else
  bad "push: pushurl was ignored — the credential verdict is about the wrong host"
  push_plain "$out7pl2" | grep -E 'credential|remote' | head -5
fi

# 7p-m. THE BOUND MUST ACTUALLY BOUND (#3369 review). `timeout <secs>` sends SIGTERM and
#   then waits, so a child that traps or ignores it runs on forever and the advertised
#   60s bound bounds nothing — in BOOT-PATH code, where a hang is the worst outcome.
#   `bounded` now passes --kill-after, which also makes the probe's rc=137 branch
#   reachable for the first time (it previously anticipated an outcome the wrapper could
#   not produce). The stand-in for a TERM-ignoring git/ssh/credential-manager is a
#   claim.sh that traps TERM: it is `bounded`'s DIRECT child (env execs it), which is the
#   process timeout signals.
#
#   COST: this case necessarily waits out the real 60s bound plus the 5s grace (~65s) —
#   the bound is production behaviour and must not be shrunk to suit a test. Its own
#   outer ceiling is the negative control: WITHOUT --kill-after the bootstrap never
#   returns, the ceiling fires, and rc is 124.
if [ -n "$TIMEOUT_KILL_TEST" ]; then
  bare7pm="$tmp/bare7pm.git"; mk_push_bare "$bare7pm"
  repo7pm="$tmp/repo7pm"; mk_push_repo "$repo7pm" "file://$bare7pm"
  printf '#!/usr/bin/env bash\ntrap "" TERM\nsleep 300\n' >"$repo7pm/scripts/flow/claim.sh"
  chmod +x "$repo7pm/scripts/flow/claim.sh"
  bin7pm="$tmp/bin7pm"; mk_push_bin "$bin7pm"
  gc7pm="$tmp/gc7pm"; : >"$gc7pm"
  hang_start=$(date +%s)
  hang_rc=0
  hang_out=$("$TIMEOUT_KILL_TEST" --kill-after=5 150 env PATH="$bin7pm:$PATH" HOME="$repo7pm/.home" \
    CARGO_HOME="$repo7pm/.home/.cargo" GIT_CONFIG_GLOBAL="$gc7pm" GIT_CONFIG_NOSYSTEM=1 \
    CLAIM_MACHINE=push-probe-test CODEX_NOTIFY_WEBHOOK='https://ntfy.example.com/t' \
    CQLITE_PROJECT_OWNER=pmcfadin CQLITE_PROJECT_NUMBER=1 \
    "$PIN_BS" "$repo7pm/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1) || hang_rc=$?
  hang_elapsed=$(( $(date +%s) - hang_start ))
  # rc 124 OR 137 both mean the OUTER ceiling fired (137 = the watchdog had to SIGKILL
  # bootstrap itself), i.e. the inner bound failed to bound. Either is a failure here.
  if [ "$hang_rc" -ne 124 ] && [ "$hang_rc" -ne 137 ] && out_has "$hang_out" '\[warn\].*git-push: UNMEASURED.*exceeded its 60s bound and was killed' \
     && [ "$hang_elapsed" -lt 120 ]; then
    ok "push: a SIGTERM-ignoring probe child is KILLED at the bound + grace — bootstrap still completes (${hang_elapsed}s) and reports UNMEASURED"
  else
    bad "push: the bound did not bound a TERM-ignoring child (rc=$hang_rc elapsed=${hang_elapsed}s)"
    push_verdict "$hang_out"
  fi
else
  # Deliberately SKIP rather than run unbounded: this fixture ignores SIGTERM, so without
  # a hard-kill-capable watchdog the case could hang the gate forever. A skip that says
  # so is honest; a case that can hang is not.
  skip "push: bound-escalation guard needs a timeout/gtimeout accepting --kill-after (its fixture ignores SIGTERM)"
fi

# 7p-n. SSH REMOTES GET SSH ADVICE (#3369 review). `gh auth setup-git` configures an
#   HTTPS credential helper and cannot affect key-based auth, so printing it for an SSH
#   remote sends the operator to fix something that is not in the path — the same
#   wrong-remedy class as the delete-path advice two rounds ago. It is newly reachable
#   because this change routes SSH origins into the push probe instead of exempting them.
#   The branch is keyed on GIT_ORIGIN_KIND (derived from the remote URL, authoritative by
#   construction), NOT on git's error text: no cause is being classified here.
#   An `ssh` stub supplies the auth-shaped failure, so nothing contacts a real host.
repo7pn="$tmp/repo7pn"; mk_push_repo "$repo7pn" "git@push-probe.invalid:owner/repo.git"
bin7pn="$tmp/bin7pn"; mk_push_bin "$bin7pn"
mk_stub "$bin7pn" ssh 'echo "git@push-probe.invalid: Permission denied (publickey)." >&2; exit 255'
gc7pn="$tmp/gc7pn"; : >"$gc7pn"
run_push "$repo7pn" "$bin7pn" "$gc7pn"; out7pn=$push_out
if out_has "$out7pn" '\[warn\].*git-push: FAILED' \
   && out_has "$out7pn" 'authenticates with your SSH KEY' \
   && out_has "$out7pn" 'ssh-add -l' \
   && ! out_has "$(push_plain "$out7pn" | grep -E '^ *fix:|^ *  *then re-run')" 'gh auth setup-git'; then
  ok "push: an SSH remote's push failure advises SSH keys, NOT 'gh auth setup-git' (which cannot affect key auth)"
else
  bad "push: SSH push failure got https credential advice"
  push_plain "$out7pn" | grep -E 'git-push|fix:|ssh' | head -5
fi

# 7p-s. NOTHING PRINTED MAY CARRY A CREDENTIAL FROM THE REMOTE URL (#3369 review). A
#   remote URL can be `https://user:token@host/…` or `ssh://user:token@host/…`, and this
#   script's output is persisted in ONBOARDING LOGS — so the two advice lines that printed
#   $GIT_ORIGIN_URL verbatim wrote a live credential into a log file. Both sub-cases below
#   put a distinctive secret in the URL and assert it never appears in the output, which is
#   the property; the structural guard in 7p-i covers the sites nobody has written yet.
URL_SECRET='ghp_URLembeddedSECRET3369URLembeddedSEC'

#   (i) An SSH remote whose URL embeds a secret. The `ssh` stub supplies an auth-shaped
#   failure (as in 7p-n) so the advice branch — the code under test — is really reached.
repo7ps="$tmp/repo7ps"; mk_push_repo "$repo7ps" "ssh://cq:$URL_SECRET@push-probe.invalid/owner/repo.git"
bin7ps="$tmp/bin7ps"; mk_push_bin "$bin7ps"
mk_stub "$bin7ps" ssh 'echo "git@push-probe.invalid: Permission denied (publickey)." >&2; exit 255'
gc7ps="$tmp/gc7ps"; : >"$gc7ps"
run_push "$repo7ps" "$bin7ps" "$gc7ps"; out7ps=$push_out
# Guard the guard: without the FAILED verdict + SSH advice the run never reached the lines
# under test, and "the secret is absent" would be true for the wrong reason.
if out_has "$out7ps" '\[warn\].*git-push: FAILED' \
   && out_has "$out7ps" 'authenticates with your SSH KEY'; then
  ok "push: (precondition) 7p-s(i) reaches the FAILED verdict and the SSH advice branch"
else
  bad "push: 7p-s(i) precondition FAILED — the advice branch was not reached, the secret assert would be vacuous"
  push_plain "$out7ps" | grep -E 'git-push|fix:' | head -4
fi
if ! out_has "$out7ps" -F "$URL_SECRET" \
   && out_has "$out7ps" "remote 'origin' is an SSH remote" \
   && out_has "$out7ps" 'remote get-url --push origin'; then
  ok "push: a credential embedded in an SSH remote URL is NEVER printed — the advice names the remote and where to read the URL locally"
else
  bad "push: the remote URL (and its embedded secret) reached the output"
  push_plain "$out7ps" | grep -E 'fix:|URL' | head -4
fi

#   (ii) An https remote whose PASSWORD contains an '@'. The host is printed on many lines,
#   so parsing the userinfo at the FIRST '@' left the tail of the password inside the
#   "host" and logged a credential fragment. git splits an authority at the LAST '@'.
repo7ps2="$tmp/repo7ps2"; mk_push_repo "$repo7ps2" "https://cq:p@${URL_SECRET}@push-probe.invalid/cqlite.git"
bin7ps2="$tmp/bin7ps2"; mk_push_bin "$bin7ps2"
gc7ps2="$tmp/gc7ps2"; : >"$gc7ps2"
run_push "$repo7ps2" "$bin7ps2" "$gc7ps2"; out7ps2=$push_out
if out_has "$out7ps2" 'git push has NO credentials for push-probe.invalid' \
   && ! out_has "$out7ps2" -F "$URL_SECRET" \
   && ! out_has "$out7ps2" '@push-probe.invalid'; then
  ok "push: a password containing '@' does not leak into the printed host (userinfo is split at the LAST '@')"
else
  bad "push: the parsed host carried a credential fragment"
  push_plain "$out7ps2" | grep -E 'push-probe' | head -4
fi

# 7p-o. A `timeout` THAT REJECTS --kill-after MUST NOT BREAK EVERY BOUND (#3369 review).
#   --kill-after is GNU coreutils; BusyBox and older implementations reject it, and a
#   non-GNU `timeout` earlier on PATH than a GNU `gtimeout` would win a first-match-wins
#   lookup. If the selected binary rejects the flag, EVERY bounded call fails — board
#   probe, credential probe, push probe — and --strict then rejects a healthy machine.
#   The stubs reject the flag and otherwise delegate to the real binary, so this measures
#   the SELECTION logic, not a crippled timeout. BOTH candidates are stubbed: stubbing
#   only `timeout` left the loop free to select a real `gtimeout` further along PATH, so
#   the probe ran, no UNMEASURED appeared, and the case FAILED on stock macOS — green
#   here (Linux, no gtimeout) and red on a supported platform (#3369 review).
if [ -n "$TIMEOUT_BIN_TEST" ]; then
  bare7po="$tmp/bare7po.git"; mk_push_bare "$bare7po"
  repo7po="$tmp/repo7po"; mk_push_repo "$repo7po" "file://$bare7po"
  bin7po="$tmp/bin7po"; mk_push_bin "$bin7po"
  mk_no_killafter_timeouts "$bin7po" "$TIMEOUT_BIN_TEST"
  gc7po="$tmp/gc7po"; : >"$gc7po"
  run_push "$repo7po" "$bin7po" "$gc7po" --strict; out7po=$push_out; rc7po=$push_rc
  # TWO properties, and the second is the one that matters most. (1) The flag-rejecting
  # binary is still SELECTED and USED, so the non-mutating probes keep working and the
  # degradation is STATED — without that, every bounded call would fail and --strict would
  # reject a healthy machine. (2) The MUTATING push is REFUSED, because a SIGTERM-only
  # bound provably does not bound a child that ignores SIGTERM and hanging the launcher is
  # worse than a red verdict. Nothing is pushed, green is withheld, --strict exits nonzero.
  refs7po=$(git ls-remote "$bare7po" 'refs/claims/*' 2>/dev/null | wc -l | tr -d ' ')
  if out_has "$out7po" 'does not accept --kill-after' \
     && out_has "$out7po" '\[warn\].*git-push: UNMEASURED.*cannot hard-kill' \
     && ! out_has "$out7po" '\[ok\].*git-push' \
     && [ "${refs7po:-0}" -eq 0 ] && ! push_green "$out7po" && [ "$rc7po" -ne 0 ]; then
    ok "push: a timeout that cannot hard-kill is still used for other probes, but the MUTATING push is REFUSED — nothing pushed, green withheld, --strict exits $rc7po"
  else
    bad "push: a mutating push ran under a bound that cannot hard-kill (rc=$rc7po refs=${refs7po:-0})"
    push_plain "$out7po" | grep -E 'git-push|kill-after' | head -4
  fi
else
  skip "push: --kill-after fallback case needs a real timeout/gtimeout to delegate to"
fi

# 7p-p. A REMOTE WITH SEVERAL PUSH URLs (#3369 review). `git push <remote>` writes to
#   EVERY configured pushurl while `get-url --push` names only the first, so the probe
#   would mutate N destinations and could create the ref on A, fail on B and clean
#   neither. It refuses instead: UNMEASURED, nothing pushed anywhere, green withheld.
bare7pp1="$tmp/bare7pp1.git"; mk_push_bare "$bare7pp1"
bare7pp2="$tmp/bare7pp2.git"; mk_push_bare "$bare7pp2"
repo7pp="$tmp/repo7pp"; mk_push_repo "$repo7pp" "file://$bare7pp1"
git -C "$repo7pp" remote set-url --add --push origin "file://$bare7pp1" >/dev/null 2>&1
git -C "$repo7pp" remote set-url --add --push origin "file://$bare7pp2" >/dev/null 2>&1
bin7pp="$tmp/bin7pp"; mk_push_bin "$bin7pp"
gc7pp="$tmp/gc7pp"; : >"$gc7pp"
run_push "$repo7pp" "$bin7pp" "$gc7pp" --strict; out7pp=$push_out; rc7pp=$push_rc
refs7pp=$(( $(git ls-remote "$bare7pp1" 'refs/claims/*' 2>/dev/null | wc -l) + $(git ls-remote "$bare7pp2" 'refs/claims/*' 2>/dev/null | wc -l) ))
if out_has "$out7pp" '\[warn\].*git-push: UNMEASURED.*2 push URLs' \
   && ! out_has "$out7pp" '\[ok\].*git-push' \
   && [ "$refs7pp" -eq 0 ] && ! push_green "$out7pp" && [ "$rc7pp" -ne 0 ]; then
  ok "push: a multi-push-URL remote is UNMEASURED and NOTHING is pushed to either destination (green withheld)"
else
  bad "push: multi-destination remote was probed anyway (refs created=$refs7pp rc=$rc7pp)"
  push_verdict "$out7pp"
fi

# 7p-q. THE GENUINE HTTPS PATH (#3369 review) — the case 7p-b only claimed to be. The
#   origin really is `https://…` at classification time, so GIT_ORIGIN_KIND is `https`;
#   the `gh auth setup-git` stub then installs the url.<local>.insteadOf rewrite (as in
#   7p-e/7p-j), so the push lands on a local bare repo whose pre-receive speaks the
#   credential signature claim.sh classifies as auth. Offline, no server, no real host —
#   and the advice under test is the one an https box would actually see.
bare7pq="$tmp/bare7pq.git"; mk_push_bare "$bare7pq" 'echo "Authentication failed" >&2; exit 1'
repo7pq="$tmp/repo7pq"; mk_push_repo "$repo7pq" "https://push-probe.invalid/cqlite.git"
bin7pq="$tmp/bin7pq"
mk_push_bin "$bin7pq" "git config --global --add 'credential.https://push-probe.invalid.helper' '!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=wired; };f'
      git config --global \"url.file://$bare7pq/.insteadOf\" 'https://push-probe.invalid/cqlite.git'"
gc7pq="$tmp/gc7pq"; : >"$gc7pq"
run_push "$repo7pq" "$bin7pq" "$gc7pq" --fix-credentials --strict; out7pq=$push_out; rc7pq=$push_rc
# Guard the guard: if the classification were not https, this case would be testing the
# same `other` path as 7p-b and its assertion would be vacuous again.
if out_has "$out7pq" 'push-probe.invalid' \
   && ! out_has "$out7pq" "is a 'other' remote"; then
  ok "push: (precondition) 7p-q's remote really is classified https"
else
  bad "push: 7p-q precondition FAILED — not an https classification, the advice assertion below is vacuous"
fi
if out_has "$out7pq" '\[warn\].*git-push: FAILED.*AUTHENTICATE' \
   && out_has "$out7pq" 'gh auth setup-git' \
   && out_has "$out7pq" -- '--fix-credentials' \
   && out_has "$out7pq" 'contents:write' \
   && [ "$rc7pq" -ne 0 ]; then
  ok "push: a real HTTPS auth failure prints the credential remediation AND the write-scope possibility"
else
  bad "push: https auth failure printed no remediation / omitted the scope line (rc=$rc7pq)"
  push_plain "$out7pq" | grep -E 'git-push|fix:|scopes' | head -5
fi

# 7p-r. THE FALLBACK REPAIR IS GATED ON THE ENVIRONMENT TOKEN BEING AUTHORITATIVE FOR THE
#   PUSH HOST (#3369 review, twice). §3b resolves the host from LOCAL GIT CONFIG (`git
#   remote get-url --push`), then under --fix-credentials installs a helper that
#   dereferences $GH_TOKEN FOR THAT HOST, and §3b-push immediately performs a real push to
#   it — all during a preflight .agent-ami/profile.yaml runs AUTOMATICALLY at every onboard.
#   A typo, a leftover fork/mirror pushurl or a stale `insteadOf` therefore handed a real
#   credential to an unintended host. An invoker who controls the box is out of the threat
#   model; a MISCONFIGURED REMOTE is reachable BY ACCIDENT, which this repo's triage rule
#   makes a defect.
#
#   THE PREDICATE IS TOKEN AUTHORITY, NOT A LOGIN — and this case is built to falsify the
#   weaker one. The first fix asked `gh auth status --hostname <host>`, which answers "does
#   gh hold SOME credential for that host?". The sandbox below is the box where those two
#   facts diverge: gh is authenticated to BOTH github.com and the push host (so the
#   status check PASSES for both) while the push host's own token is a DIFFERENT value from
#   $GH_TOKEN — exactly a github.com token on a machine that also has a GitHub Enterprise
#   host. The preconditions assert that divergence rather than assuming it.
#
#   MEASURED AS A PAIR against one sandbox shape whose ONLY variable is which token gh
#   reports for the push host — without the positive control, "no helper was written"
#   would also be satisfied by a repair that stopped working altogether.
#   Both stubs' `gh auth setup-git` installs ONLY the url.<local>.insteadOf rewrite and NO
#   credential helper, which is what routes the run into the FALLBACK (the branch under
#   test) while keeping the push itself offline on a local bare repo.

# mk_push_gh_tokenhost <dir> <host> <token-gh-holds-for-that-host> <setup-git-body> — like
# mk_push_gh, but modelling a TWO-HOST box:
#   `gh auth status`              succeeds, listing github.com AND <host>
#   `gh auth status --hostname H` succeeds for BOTH of them (the insufficient predicate)
#   `gh auth token --hostname H`  prints $GH_TOKEN for github.com, <token…> for <host>,
#                                 and fails the way real gh does for anything else
mk_push_gh_tokenhost() {
  local dir="$1" host="$2" tok="$3" setup="${4:-:}"
  cat >"$dir/gh" <<EOF
#!/usr/bin/env bash
host_arg() {   # the value of --hostname in "\$@", or ""
  while [ \$# -gt 0 ]; do
    [ "\$1" = --hostname ] && { printf '%s' "\$2"; return 0; }
    shift
  done
}
case "\$1" in
  auth)
    if [ "\$2" = status ]; then
      shift 2; want="\$(host_arg "\$@")"
      case "\$want" in
        ""|github.com|$host) : ;;
        *) echo "You are not logged into any accounts on \$want" >&2; exit 1 ;;
      esac
      echo "github.com"
      echo "  ✓ Logged in to github.com account tester (GH_TOKEN)"
      echo "  - Token scopes: 'gist', 'project', 'read:org', 'repo', 'workflow'"
      echo "$host"
      echo "  ✓ Logged in to $host account tester (keyring)"
      echo "  - Token scopes: 'repo', 'workflow'"
      exit 0
    elif [ "\$2" = token ]; then
      shift 2; want="\$(host_arg "\$@")"
      case "\$want" in
        ""|github.com) printf '%s\\n' "\${GH_TOKEN:-}" ;;
        $host)         printf '%s\\n' '$tok' ;;
        *) echo "no oauth token found for \$want" >&2; exit 1 ;;
      esac
      exit 0
    elif [ "\$2" = setup-git ]; then
      $setup
    fi
    exit 0 ;;
  project) echo '{"id":"PVT_stub"}'; exit 0 ;;
  api)     echo '{"data":{"node":{"id":"PVT_stub"}}}'; exit 0 ;;
esac
exit 0
EOF
  chmod +x "$dir/gh"
}

# The fallback needs a token in the environment or it never reaches the gated branch.
# Saved and restored so no later case inherits it.
gh_tok_was_set=${GH_TOKEN+1}; gh_tok_saved="${GH_TOKEN-}"
export GH_TOKEN="$FAKE_TOKEN"
ENTERPRISE_TOKEN='ghp_ENTERPRISEtoken3369ENTERPRISEtoken33'

# (i) NEGATIVE: gh is logged in to the push host, but that host's token is NOT $GH_TOKEN.
bare7pr="$tmp/bare7pr.git"; mk_push_bare "$bare7pr"
repo7pr="$tmp/repo7pr"; mk_push_repo "$repo7pr" "https://push-probe.invalid/cqlite.git"
bin7pr="$tmp/bin7pr"; mk_push_bin "$bin7pr"
mk_push_gh_tokenhost "$bin7pr" push-probe.invalid "$ENTERPRISE_TOKEN" \
  "git config --global \"url.file://$bare7pr/.insteadOf\" 'https://push-probe.invalid/cqlite.git'"
gc7pr="$tmp/gc7pr"; : >"$gc7pr"
# Guard the guard, and it is the whole point of the case: the WEAKER predicate must PASS
# here (gh really is logged in to the push host) while the token for that host DIFFERS
# from $GH_TOKEN. Without this, the assertions below could be satisfied by a stub that
# simply refuses everything.
pr_status_ok=0; PATH="$bin7pr:$PATH" gh auth status --hostname push-probe.invalid >/dev/null 2>&1 && pr_status_ok=1
pr_tok_host=$(PATH="$bin7pr:$PATH" gh auth token --hostname push-probe.invalid 2>/dev/null)
pr_tok_gh=$(PATH="$bin7pr:$PATH" gh auth token --hostname github.com 2>/dev/null)
if [ "$pr_status_ok" -eq 1 ] && [ "$pr_tok_host" = "$ENTERPRISE_TOKEN" ] && [ "$pr_tok_gh" = "$FAKE_TOKEN" ]; then
  ok "push: (precondition) the two-host gh stub CONFIRMS a login for the push host while holding a DIFFERENT token for it"
else
  bad "push: 7p-r precondition FAILED — stub does not model the two-host box (status=$pr_status_ok host-token-differs=$([ "$pr_tok_host" != "$FAKE_TOKEN" ] && echo yes || echo no))"
fi
run_push "$repo7pr" "$bin7pr" "$gc7pr" --fix-credentials --strict; out7pr=$push_out; rc7pr=$push_rc
helper7pr=$(git config --file "$gc7pr" --get-all 'credential.https://push-probe.invalid.helper' 2>/dev/null | wc -l | tr -d ' ')
tokleak7pr=$(grep -cF "$FAKE_TOKEN" "$gc7pr" 2>/dev/null || true)
if [ "${helper7pr:-0}" -eq 0 ] && [ "${tokleak7pr:-0}" -eq 0 ] \
   && out_has "$out7pr" '\[warn\].*push-probe.invalid.*REFUSED to configure any' \
   && ! push_green "$out7pr" && [ "$rc7pr" -ne 0 ]; then
  ok "push: a token that is NOT authoritative for the push host is NOT configured for it — the refusal warns, green is withheld, --strict exits $rc7pr"
else
  bad "push: a foreign token was configured for the push host (helpers=${helper7pr:-0} rc=$rc7pr)"
  push_plain "$out7pr" | grep -E 'credential|REFUS|git-push' | head -6
fi
# Neither token may appear anywhere in the output: the comparison is in-process, and the
# diagnosis names the HOST, never a secret.
if ! out_has "$out7pr" -F "$FAKE_TOKEN" && ! out_has "$out7pr" -F "$ENTERPRISE_TOKEN"; then
  ok "push: the authority check prints NEITHER token — the refusal names only the host"
else
  bad "push: a token value leaked into the bootstrap output"
fi
# ONE verdict, as everywhere else in §3b: the refusal must not also emit the generic
# "could not configure any" warning, or a single fault would be counted twice.
if [ "$base_warns" -eq 1 ]; then
  if [ "$(push_warns "$out7pr")" -eq 1 ] \
     && ! out_has "$out7pr" 'could NOT configure any'; then
    ok "push: the refusal is exactly ONE warning and names the host, not a generic second verdict"
  else
    bad "push: refusal emitted $(push_warns "$out7pr") warnings (expected 1)"
    push_plain "$out7pr" | grep -E '^[[:space:]]+\[warn\] ' | head -4
  fi
else
  skip "push: one-warning assertion needs an otherwise-clean sandbox (baseline=$base_warns warnings)"
fi

# (ii) POSITIVE CONTROL: the SAME sandbox and the SAME fallback path, the ONLY change being
#      that gh reports $GH_TOKEN as the push host's own token — the repair must still happen.
bare7pr2="$tmp/bare7pr2.git"; mk_push_bare "$bare7pr2"
repo7pr2="$tmp/repo7pr2"; mk_push_repo "$repo7pr2" "https://push-probe.invalid/cqlite.git"
bin7pr2="$tmp/bin7pr2"; mk_push_bin "$bin7pr2"
mk_push_gh_tokenhost "$bin7pr2" push-probe.invalid "$FAKE_TOKEN" \
  "git config --global \"url.file://$bare7pr2/.insteadOf\" 'https://push-probe.invalid/cqlite.git'"
gc7pr2="$tmp/gc7pr2"; : >"$gc7pr2"
run_push "$repo7pr2" "$bin7pr2" "$gc7pr2" --fix-credentials --strict; out7pr2=$push_out; rc7pr2=$push_rc
helper7pr2=$(git config --file "$gc7pr2" --get-all 'credential.https://push-probe.invalid.helper' 2>/dev/null | grep -cF 'x-access-token' || true)
if [ "${helper7pr2:-0}" -ge 1 ] \
   && out_has "$out7pr2" '\[ok\].*git credentials WIRED BY THIS RUN.*push-probe.invalid' \
   && out_has "$out7pr2" '\[ok\].*git-push: VERIFIED' \
   && ! out_has "$out7pr2" 'REFUSED to configure any'; then
  ok "push: (positive control) the AUTHORITATIVE token is repaired exactly as before — helper written, credentials WIRED, push VERIFIED"
else
  bad "push: the authority gate broke the repair on its own host (helpers=${helper7pr2:-0} rc=$rc7pr2)"
  push_plain "$out7pr2" | grep -E 'credential|git-push' | head -6
fi

if [ -n "${gh_tok_was_set:-}" ]; then export GH_TOKEN="$gh_tok_saved"; else unset GH_TOKEN; fi

# 7p-g. `--strict` AND "All checks green." MUST NOT DIVERGE — asserted in BOTH
#   directions. They are two channels for ONE fact: the green string is printed iff
#   WARNINGS is 0, and --strict exits 0 iff WARNINGS is 0. A reviewer proposed keying
#   --strict on a narrower "blocking faults only" counter; that would have made a box
#   with an ADVISORY warning exit 0 from --strict while the unchanged `expect` string
#   still failed the same run — two channels disagreeing, which is worse than either
#   alone. This case exists so the next person to have that idea trips a test.
#
#   The advisory run below is what gives the case teeth: a machine whose push probe
#   VERIFIES but which carries an unrelated advisory warning (no Data.db fixtures).
#   Under a blocking-only --strict it would exit 0 while withholding green.
repo7pg="$tmp/repo7pg"; mk_push_repo "$repo7pg" "file://$bare7pa"
rm -f "$repo7pg/test-data/datasets/sstables/ks/tbl/nb-1-big-Data.db"   # one ADVISORY warn
bin7pg="$tmp/bin7pg"; mk_push_bin "$bin7pg"
gc7pg="$tmp/gc7pg"; : >"$gc7pg"
run_push "$repo7pg" "$bin7pg" "$gc7pg" --strict; out7pg=$push_out; rc7pg=$push_rc
if out_has "$out7pg" '\[ok\].*git-push: VERIFIED' \
   && out_has "$out7pg" 'no \*-Data.db files found' \
   && ! push_green "$out7pg" && [ "$rc7pg" -ne 0 ]; then
  ok "push: an ADVISORY warning withholds green AND fails --strict, even with push VERIFIED (--strict is not blocking-only)"
else
  bad "push: advisory-warning run diverged (rc=$rc7pg, green=$(push_green "$out7pg" && echo yes || echo no))"
  push_verdict "$out7pg"
fi

divergence=0; green_runs=0; nongreen_runs=0
check_divergence() {   # <label> <output> <rc-of-a---strict-run>
  if push_green "$2"; then
    green_runs=$((green_runs + 1))
    [ "$3" -eq 0 ] || { divergence=1; echo "   divergence: $1 printed 'All checks green.' but --strict exited $3"; }
  else
    nongreen_runs=$((nongreen_runs + 1))
    [ "$3" -ne 0 ] || { divergence=1; echo "   divergence: $1 withheld 'All checks green.' but --strict exited 0"; }
  fi
}
check_divergence 7p-a "$out7pa" "$rc7pa"    # verified, clean   -> expect green + 0
check_divergence 7p-d "$out7pd" "$rc7pd"    # opt-out           -> expect no green + nonzero
check_divergence 7p-b "$out7pb" "$rc7pb"    # push FAILED       -> expect no green + nonzero
check_divergence 7p-g "$out7pg" "$rc7pg"    # advisory warning  -> expect no green + nonzero
if [ "$divergence" -eq 0 ] && [ "$green_runs" -ge 1 ] && [ "$nongreen_runs" -ge 1 ]; then
  ok "push: --strict's exit code and 'All checks green.' agree in BOTH directions ($green_runs green, $nongreen_runs non-green runs)"
elif [ "$divergence" -ne 0 ]; then
  bad "push: --strict and the 'All checks green.' string DIVERGED (see the divergence lines above)"
else
  skip "push: divergence check needs both directions (green=$green_runs nongreen=$nongreen_runs on this host)"
fi

# 7p-h. FLAG HYGIENE. --skip-push-probe and --skip-smoke are different subjects (the
#   git push probe vs the gate fmt run) and the name similarity is a live hazard, so
#   both must be documented and each must skip only its own thing. 7p-a ran with
#   --skip-smoke ALONE and still probed; 7p-d ran with BOTH and skipped only the probe.
push_help=$(env HOME="$tmp/help-home" CARGO_HOME="$tmp/help-home/.cargo" "$PIN_BS" "$BOOTSTRAP" --help 2>&1)
if out_has "$push_help" -- '--skip-push-probe' \
   && out_has "$push_help" -- '--skip-smoke' \
   && out_has "$push_help" -- '--fix-credentials' \
   && out_has "$push_help" -- '--strict'; then
  ok "push: --help documents --skip-push-probe, --skip-smoke, --fix-credentials and --strict"
else
  bad "push: --help does not document the new flags"
fi
if out_has "$out7pa" 'git-push: VERIFIED' \
   && out_has "$out7pa" 'skipped (--skip-smoke)' \
   && out_has "$out7pd" 'git-push: OPT-OUT' \
   && out_has "$out7pd" 'skipped (--skip-smoke)'; then
  ok "push: --skip-smoke skips only the gate run; --skip-push-probe skips only the push probe"
else
  bad "push: the two skip flags are not independent"
fi

# 7p-i. STRUCTURAL GUARD: no case in this suite may point the push probe at a REAL
#   remote. Behavioural cases only cover the shapes someone already thought of; this
#   one covers the case NOBODY HAS WRITTEN YET. The probe pushes for real, so a case
#   that gave a claim.sh-bearing sandbox a github.com origin — or a whole-checkout run
#   that forgot --skip-push-probe — would mutate the shared origin from a unit test.
TEST_SELF="$SCRIPT_DIR/$(basename "$0")"
bad_remote=$(grep -n 'mk_push_repo "\$repo' "$TEST_SELF" \
  | grep -vE 'file://|push-probe\.invalid|mk_push_repo "\$repo[a-z0-9]*" ""' || true)
if [ -z "$bad_remote" ]; then
  ok "push: every claim.sh-bearing sandbox points at a local file:// repo, an empty remote, or push-probe.invalid"
else
  bad "push: a sandbox that can PUSH is pointed at a non-local remote:"
  printf '%s\n' "$bad_remote"
fi
# Same shape, third instance: a case that stubs ONE timeout candidate leaves the
# production loop free to select a real one further along PATH. That is what made 7p-o
# pass here and fail on stock macOS, and it is invisible on any host lacking the OTHER
# candidate — so it is asserted structurally, not behaviourally. The needle is built by
# concatenation so this guard cannot match its own source line.
tstub_needle='mk_stub'' "[^"]*" timeout'
lone_tstub=$(grep -nE "$tstub_needle" "$TEST_SELF" || true)
if [ -z "$lone_tstub" ]; then
  ok "push: no case stubs a single timeout candidate — use mk_no_killafter_timeouts (stubs timeout AND gtimeout)"
else
  bad "push: a case stubs one timeout candidate; the production loop can escape to the other:"
  printf '%s\n' "$lone_tstub"
fi
# Same shape, fourth instance, and this one guards the SCRIPT rather than the suite: no
# line the bootstrap EMITS may pass the raw remote URL, which can embed a credential while
# this output is persisted in onboarding logs (#3369 review). Behavioural cases (7p-s)
# cover the two sites that did; this covers the next one somebody adds. The classification
# uses of $GIT_ORIGIN_URL in section 3b are untouched — it is only PRINTING that is banned.
urlprint=$(grep -nE '^[[:space:]]*(info|warn|ok|note|hdr)[[:space:]].*\$GIT_ORIGIN_URL' "$BOOTSTRAP" || true)
if [ -z "$urlprint" ]; then
  ok "push: no emitted line prints the raw remote URL (it can embed a credential, and this output is logged)"
else
  bad "push: an emitted line prints \$GIT_ORIGIN_URL — print the remote NAME + protocol + parsed host instead:"
  printf '%s\n' "$urlprint"
fi
unguarded=$(grep -n 'bash "\$BOOTSTRAP" --skip-smoke' "$TEST_SELF" | grep -v -- '--skip-push-probe' || true)
if [ -z "$unguarded" ]; then
  ok "push: every run against the REAL checkout passes --skip-push-probe (the suite cannot reach the real origin)"
else
  bad "push: a real-checkout run would probe the REAL origin:"
  printf '%s\n' "$unguarded"
fi

# --- 8. Board check is a FUNCTIONAL, READ-ONLY probe (issue #2942) ----------
# The false OK this exists to prevent: a token whose scopes INCLUDE `project` while
# `gh project` still fails for a missing `read:org`, and the equivalent
# `updateProjectV2ItemFieldValue` GraphQL mutation succeeds with the SAME token. A
# scope-string match therefore proves nothing about the operation, and must never be
# the verdict.

# mk_board_gh <dir> <log> <scopes> <missing-scopes|""> <gh-project-rc> <gh-api-rc>
mk_board_gh() {
  local dir="$1" log="$2" scopes="$3" missing="$4" prc="$5" arc="$6"
  local missing_echo=""
  [ -n "$missing" ] && missing_echo="echo \"  ! Missing required token scopes: $missing\""
  cat >"$dir/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$log"
case "\$1" in
  auth)
    if [ "\$2" = status ]; then
      echo "github.com"
      echo "  ✓ Logged in to github.com account tester (GH_TOKEN)"
      echo "  - Token scopes: $scopes"
      $missing_echo
    fi
    exit 0 ;;
  project) exit $prc ;;
  # The GraphQL probe demands a NON-EMPTY project id — `gh api graphql` exits 0 on a
  # query that RESOLVES TO NULL, so an exit code alone would be a false OK. api-rc
  # 'null' simulates exactly that: a clean exit carrying no project.
  api)
    if [ "$arc" = null ]; then exit 0; fi
    [ "$arc" = 0 ] && echo "PVT_kwStubProjectId"
    exit "$arc" ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$dir/gh"
}

# run_board_case <name> <scopes> <missing> <project-rc> <api-rc> -> sets BOARD_OUT/BOARD_LOG
# CQLITE_PROJECT_ACCOUNT is pinned to the stub's account so these cases exercise the
# VERDICT logic with no account switch in play (switching has its own cases below).
run_board_case() {
  local name="$1" scopes="$2" missing="$3" prc="$4" arc="$5"
  local sb stub repo
  sb=$(mktemp -d "$tmp/board-$name.XXXXXX"); stub="$tmp/stub-board-$name"
  mk_hermetic_bin "$stub"
  BOARD_LOG="$tmp/gh-board-$name.log"; : >"$BOARD_LOG"
  mk_board_gh "$stub" "$BOARD_LOG" "$scopes" "$missing" "$prc" "$arc"
  repo="$tmp/repo-board-$name"; mk_fake_repo "$repo" "https://github.com/pmcfadin/cqlite.git"
  BOARD_OUT=$(PATH="$stub" HOME="$sb" CARGO_HOME="$sb/.cargo" GIT_CONFIG_GLOBAL="$sb/gitconfig" \
    CQLITE_PROJECT_ACCOUNT=tester CQLITE_PROJECT_NUMBER=1 \
    GH_TOKEN="" "$PIN_BS" "$repo/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
}

# run_board_auth_case <name> <auth-status-body> [env...] -> BOARD_OUT/BOARD_LOG
# Like run_board_case but the caller supplies the VERBATIM `gh auth status` body, so a
# multi-account host can be modelled exactly. `gh project`/`gh api` always succeed, so
# any non-green verdict is attributable purely to which account's stanza was parsed.
run_board_auth_case() {
  local name="$1" body="$2"; shift 2
  local sb stub repo
  sb=$(mktemp -d "$tmp/bauth-$name.XXXXXX"); stub="$tmp/stub-bauth-$name"
  mk_hermetic_bin "$stub"
  BOARD_LOG="$tmp/gh-bauth-$name.log"; : >"$BOARD_LOG"
  printf '%s\n' "$body" >"$tmp/authbody-$name.txt"
  cat >"$stub/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$BOARD_LOG"
case "\$1" in
  auth) [ "\$2" = status ] && cat "$tmp/authbody-$name.txt"; exit 0 ;;
  project) exit 0 ;;
  api)     echo "PVT_kwStubProjectId"; exit 0 ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$stub/gh"
  repo="$tmp/repo-bauth-$name"; mk_fake_repo "$repo" "https://github.com/pmcfadin/cqlite.git"
  # Caller overrides go through `env`: a VAR=value coming from "$@" is the result of an
  # expansion, so bash would treat it as a COMMAND NAME, not an assignment — the
  # override would silently do nothing and the case would assert against the default.
  BOARD_OUT=$(PATH="$stub" HOME="$sb" CARGO_HOME="$sb/.cargo" GIT_CONFIG_GLOBAL="$sb/gitconfig" \
    CQLITE_PROJECT_NUMBER=1 \
    GH_TOKEN="" env "$@" "$PIN_BS" "$repo/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
}

# 8a. THE false-OK case: `project` scope present, `read:org` missing, `gh project`
#     unusable, GraphQL fine. Today's scope-string check prints an unqualified
#     "board dispatch works" here — that verdict must be impossible.
run_board_case falseok "'project', 'repo', 'workflow'" "'read:org'" 1 0
if out_has "$BOARD_OUT" "board dispatch works"; then
  bad "board: scope-present-but-gh-project-unusable STILL prints 'board dispatch works'"
else
  ok "board: scope present + gh project unusable -> no unqualified 'board dispatch works'"
fi
if out_has "$BOARD_OUT" "updateProjectV2ItemFieldValue"; then
  ok "board: names the updateProjectV2ItemFieldValue GraphQL write fallback"
else
  bad "board: never named the updateProjectV2ItemFieldValue fallback"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board"
fi
if out_has "$BOARD_OUT" -i "read:org"; then
  ok "board: surfaces the read:org scope gap gh itself reports"
else
  bad "board: did not surface the read:org gap"
fi

# 8b. `gh project` READ works but gh still reports missing required scopes — the
#     write (`item-edit`) can fail. Still not an unqualified success.
run_board_case partial "'project', 'repo', 'workflow'" "'read:org'" 0 0
if ! out_has "$BOARD_OUT" "board dispatch works" \
   && out_has "$BOARD_OUT" "updateProjectV2ItemFieldValue"; then
  ok "board: read-OK + missing required scopes -> qualified verdict naming the fallback"
else
  bad "board: read-OK + missing scopes reported as unqualified success"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board"
fi

# 8c. Fully healthy token: probe succeeds, gh reports no missing scopes -> an ok.
#     The assertion names PROBE-DERIVED text on purpose: a looser `[ok].*board` also
#     matches the old scope-string verdict ("[ok] 'project' scope present — board
#     dispatch works"), so it would pass against the very bug this section exists to
#     catch. In a change about false OKs, a test that passes against the bug is the
#     one thing that must not ship.
run_board_case healthy "'project', 'read:org', 'repo', 'workflow'" "" 0 0
if printf '%s' "$BOARD_OUT" | grep -Eq "\[ok\].*board #1 \(pmcfadin\) reachable.*read probe OK"; then
  ok "board: healthy token reports ok with a PROBE-derived verdict"
else
  bad "board: healthy token did not produce a probe-derived ok verdict"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board #"
fi

# 8d. Unreachable board (both probes fail) -> a loud warn, never a scope-based pass.
run_board_case unreachable "'project', 'repo', 'workflow'" "" 1 1
if printf '%s' "$BOARD_OUT" | grep -Eq '\[warn\].*board' \
   && ! out_has "$BOARD_OUT" "board dispatch works"; then
  ok "board: both probes failing -> warn (scope match never rescues the verdict)"
else
  bad "board: unreachable board did not warn"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board"
fi

# 8f. GraphQL exits 0 but resolves to NO project (wrong owner kind / wrong number).
#     An exit-code-only probe would call that reachable — it must not.
run_board_case nullproject "'project', 'repo', 'workflow'" "" 1 null
if printf '%s' "$BOARD_OUT" | grep -Eq '\[warn\].*(UNREACHABLE|BOTH probes failed)'; then
  ok "board: GraphQL exit 0 with a null project counts as a FAILED probe, not reachable"
else
  bad "board: null-project GraphQL reply was treated as a working fallback"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board #"
fi

# 8g. READ-ONLY project access ('read:project', no 'project') with a clean probe and
#     no gh-reported missing scopes. Board WRITES — the whole dispatch loop — still
#     fail, so an unqualified ok as the section's LAST word would be a false OK even
#     though an earlier line warned about the scope.
run_board_case readonlyscope "'read:project', 'repo', 'workflow'" "" 0 0
if printf '%s' "$BOARD_OUT" | grep -Eq '\[ok\].*board #1.*reachable'; then
  bad "board: read-only project scope still printed an unqualified 'reachable' ok"
elif printf '%s' "$BOARD_OUT" | grep -Eq "\[warn\].*board READ works.*'project' WRITE scope is MISSING"; then
  ok "board: read-only project scope -> READ-works warn naming the missing WRITE scope"
else
  bad "board: read-only project scope produced neither the ok nor the expected warn"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "board"
fi

# --- 8h. The verdict must be attributed to the ACTIVE account ----------------
# `gh auth status` prints one stanza PER logged-in account and the active one is not
# guaranteed first, so a whole-output grep can read a DIFFERENT account's scopes than
# the one every gh call uses. This repo documents the exact hazard
# (.claude/skills/flow-board/SKILL.md): the active account silently flips to an EMU
# account lacking `project`, and board writes then degrade SILENTLY. Both cases below
# are built so a whole-output grep gives the WRONG verdict.

# 8h-i. ACTIVE account is clean; a NON-active account reports missing scopes. A
#       whole-output grep sees that stray line and wrongly qualifies the verdict.
run_board_auth_case active-clean 'github.com
  ✓ Logged in to github.com account other-emu (keyring)
  - Active account: false
  - Token scopes: '"'"'project'"'"', '"'"'repo'"'"'
  ! Missing required token scopes: '"'"'read:org'"'"'
  ✓ Logged in to github.com account pmcfadin (keyring)
  - Active account: true
  - Token scopes: '"'"'project'"'"', '"'"'read:org'"'"', '"'"'repo'"'"''
if printf '%s' "$BOARD_OUT" | grep -Eq "\[ok\].*board #1.*reachable as 'pmcfadin'"; then
  ok "board: a NON-active account's missing-scopes line does not qualify the verdict"
else
  bad "board: verdict read a non-active account's stanza"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "board\|account"
fi

# 8h-ii. A NON-active account listed FIRST has 'project'; the ACTIVE one does not. A
#        whole-output `grep 'Token scopes:' | head -1` picks the wrong stanza and would
#        greenlight a machine whose dispatch writes all fail.
run_board_auth_case active-noproject 'github.com
  ✓ Logged in to github.com account other-emu (keyring)
  - Active account: false
  - Token scopes: '"'"'project'"'"', '"'"'read:org'"'"', '"'"'repo'"'"'
  ✓ Logged in to github.com account pmcfadin (keyring)
  - Active account: true
  - Token scopes: '"'"'read:project'"'"', '"'"'repo'"'"''
if ! printf '%s' "$BOARD_OUT" | grep -Eq '\[ok\].*board #1.*reachable' \
   && out_has "$BOARD_OUT" "'project' scope MISSING on gh account 'pmcfadin'"; then
  ok "board: scopes are read from the ACTIVE stanza, not the first one printed"
else
  bad "board: scopes were read from a non-active (first-listed) account"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "scope\|board #"
fi

# 8h-iii. The operator must be able to see WHICH account the verdict is about.
if out_has "$BOARD_OUT" "measuring gh account 'pmcfadin'"; then
  ok "board: names the account the verdict is about"
else
  bad "board: verdict does not name the account it measured"
fi

# --- 8i. Probe the account board dispatch actually uses ----------------------
# flow-board forces CQLITE_PROJECT_ACCOUNT active before EVERY board op. Probing as
# whatever happens to be active measures a different identity: with an EMU account
# active, bootstrap would shout "board UNREACHABLE — a session must STOP" about a
# machine where flow-board switches and works fine. Mirroring the switch is required —
# and because it mutates real gh state, the operator's account must be RESTORED.
mk_switch_gh() {
  # mk_switch_gh <dir> <log> <statefile> <acctA> <acctB>  (acctA starts active)
  local dir="$1" log="$2" state="$3" a="$4" b="$5"
  printf '%s' "$a" >"$state"
  cat >"$dir/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$log"
cur=\$(cat "$state" 2>/dev/null)
case "\$1" in
  auth)
    case "\$2" in
      status)
        echo "github.com"
        for acct in $a $b; do
          echo "  ✓ Logged in to github.com account \$acct (keyring)"
          if [ "\$acct" = "\$cur" ]; then echo "  - Active account: true"
          else echo "  - Active account: false"; fi
          echo "  - Token scopes: 'project', 'read:org', 'repo'"
        done
        exit 0 ;;
      switch)
        shift 2
        while [ \$# -gt 0 ]; do
          [ "\$1" = --user ] && printf '%s' "\$2" >"$state"
          shift
        done
        exit 0 ;;
      *) exit 0 ;;
    esac ;;
  project) exit 0 ;;
  api)     echo "PVT_kwStubProjectId"; exit 0 ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$dir/gh"
}

sb8i=$(mktemp -d "$tmp/board8i.XXXXXX"); stub8i="$tmp/stub8i"
mk_hermetic_bin "$stub8i"
log8i="$tmp/gh8i.log"; : >"$log8i"; state8i="$tmp/gh8i.state"
mk_switch_gh "$stub8i" "$log8i" "$state8i" other-emu pmcfadin   # EMU active at start
repo8i="$tmp/repo8i"; mk_fake_repo "$repo8i" "https://github.com/pmcfadin/cqlite.git"
out8i=$(PATH="$stub8i" HOME="$sb8i" CARGO_HOME="$sb8i/.cargo" GIT_CONFIG_GLOBAL="$sb8i/gitconfig" \
  CQLITE_PROJECT_NUMBER=1 GH_TOKEN="" "$PIN_BS" "$repo8i/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if grep -q -- 'auth switch --user pmcfadin' "$log8i"; then
  ok "board: switches to CQLITE_PROJECT_ACCOUNT before probing (mirrors flow-board)"
else
  bad "board: never switched to the board account — probes a different identity than dispatch uses"
fi
if grep -q -- 'auth switch --user other-emu' "$log8i" && [ "$(cat "$state8i")" = other-emu ]; then
  ok "board: RESTORES the operator's active account after the probe (a check must not mutate)"
else
  bad "board: left the active account switched to '$(cat "$state8i")' — a check mutated host state"
fi
if printf '%s' "$out8i" | grep -Eq '\[ok\].*board #1.*reachable' ; then
  ok "board: reports reachable for the account dispatch actually uses"
else
  bad "board: did not reach a green verdict after switching to the board account"
  printf '%s\n' "$out8i" | grep -i -A2 "board #"
fi

# 8j. With an env token, gh ignores the keyring and `gh auth switch` cannot change the
#     identity — attempting it would be theatre, and mutating host state for a no-op
#     is worse than not trying.
sb8j=$(mktemp -d "$tmp/board8j.XXXXXX"); stub8j="$tmp/stub8j"
mk_hermetic_bin "$stub8j"
log8j="$tmp/gh8j.log"; : >"$log8j"; state8j="$tmp/gh8j.state"
mk_switch_gh "$stub8j" "$log8j" "$state8j" other-emu pmcfadin
repo8j="$tmp/repo8j"; mk_fake_repo "$repo8j" "https://github.com/pmcfadin/cqlite.git"
out8j=$(PATH="$stub8j" HOME="$sb8j" CARGO_HOME="$sb8j/.cargo" GIT_CONFIG_GLOBAL="$sb8j/gitconfig" \
  CQLITE_PROJECT_NUMBER=1 GH_TOKEN="$FAKE_TOKEN" "$PIN_BS" "$repo8j/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if ! grep -q -- 'auth switch' "$log8j" && [ "$(cat "$state8j")" = other-emu ]; then
  ok "board: an env token suppresses the switch entirely (no pointless host mutation)"
else
  bad "board: attempted an account switch while GH_TOKEN was in force"
fi
if out_has "$out8j" "from GH_TOKEN in the environment"; then
  ok "board: names the env token as the identity source"
else
  bad "board: did not disclose that the identity came from GH_TOKEN"
fi

# --- 8k. CQLITE_PROJECT_NUMBER unset is a DISPATCH BLOCKER -------------------
# flow-board reads `${CQLITE_PROJECT_NUMBER:-}` and STOPs when it is empty. A bootstrap
# that defaulted the number to a guess would print a green "board reachable" on a box
# where every flow-* skill refuses to dispatch — the same false green, one layer out.
# 8k-i: the board is discoverable by title -> warn naming the exact export line.
sb8k=$(mktemp -d "$tmp/board8k.XXXXXX"); stub8k="$tmp/stub8k"
mk_hermetic_bin "$stub8k"
jqp=$(type -P jq 2>/dev/null) && ln -sf "$jqp" "$stub8k/jq"
cat >"$stub8k/gh" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  auth) [ "$2" = status ] && { echo "github.com"
        echo "  ✓ Logged in to github.com account tester (keyring)"
        echo "  - Active account: true"
        echo "  - Token scopes: 'project', 'read:org', 'repo'"; }; exit 0 ;;
  project)
    [ "$2" = list ] && { echo '{"projects":[{"title":"CQLite Delivery","number":7}]}'; exit 0; }
    exit 0 ;;
  api) echo "PVT_kwStubProjectId"; exit 0 ;;
  *)   exit 0 ;;
esac
EOF
chmod +x "$stub8k/gh"
repo8k="$tmp/repo8k"; mk_fake_repo "$repo8k" "https://github.com/pmcfadin/cqlite.git"
out8k=$(PATH="$stub8k" HOME="$sb8k" CARGO_HOME="$sb8k/.cargo" GIT_CONFIG_GLOBAL="$sb8k/gitconfig" \
  CQLITE_PROJECT_ACCOUNT=tester GH_TOKEN="" "$PIN_BS" "$repo8k/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if printf '%s' "$out8k" | grep -Eq '\[ok\].*board #.*reachable'; then
  bad "board: unexported CQLITE_PROJECT_NUMBER still produced a green 'reachable' verdict"
elif out_has "$out8k" 'CQLITE_PROJECT_NUMBER is NOT exported'; then
  ok "board: unexported CQLITE_PROJECT_NUMBER is reported as a dispatch blocker"
else
  bad "board: unexported CQLITE_PROJECT_NUMBER neither warned nor blocked the green verdict"
  printf '%s\n' "$out8k" | grep -i -A2 "board"
fi
if [ -n "$jqp" ]; then
  if out_has "$out8k" 'export CQLITE_PROJECT_NUMBER=7'; then
    ok "board: discovers the number by title and prints the exact export line"
  else
    bad "board: did not resolve the board by title / print the export line"
    printf '%s\n' "$out8k" | grep -i "PROJECT_NUMBER"
  fi
else
  skip "board: title discovery needs jq (absent on this host)"
fi

# 8k-ii: not discoverable -> point at setup-project-board.sh, still no green.
run_board_auth_case nonumber 'github.com
  ✓ Logged in to github.com account pmcfadin (keyring)
  - Active account: true
  - Token scopes: '"'"'project'"'"', '"'"'read:org'"'"', '"'"'repo'"'"'' CQLITE_PROJECT_NUMBER=
if ! printf '%s' "$BOARD_OUT" | grep -Eq '\[ok\].*board #.*reachable' \
   && out_has "$BOARD_OUT" 'setup-project-board.sh'; then
  ok "board: unresolvable board number -> no green, points at setup-project-board.sh"
else
  bad "board: unresolvable board number did not block the green verdict"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "board"
fi

# 8l. The account restore must be armed by a TRAP, not just an inline block: two
#     network calls sit between the switch and the restore, so an interrupt or a
#     supervisor SIGTERM in that window would strand the operator's active account.
if grep -q "trap 'restore_board_account' EXIT" "$BOOTSTRAP" \
   && grep -q "trap 'restore_board_account; exit 130' INT" "$BOOTSTRAP" \
   && grep -q "trap 'restore_board_account; exit 143' TERM" "$BOOTSTRAP"; then
  ok "board: account restore is armed on EXIT/INT/TERM, not only the happy path"
else
  bad "board: no EXIT/INT/TERM trap arming the account restore"
fi
# ...and the probes it brackets must be BOUNDED, so the window cannot hang open.
if grep -q 'bounded 20 gh project view' "$BOOTSTRAP" \
   && grep -q 'bounded 20 gh api graphql' "$BOOTSTRAP"; then
  ok "board: both probes inside the switch/restore bracket are time-bounded"
else
  bad "board: an unbounded probe sits between the account switch and its restore"
fi

# 8e. The probe is READ-ONLY: across EVERY board case above, the bootstrap must never
#     have invoked a board-mutating gh call. The glob covers all three log families —
#     the identity-switching cases most of all, where a mutating call would matter most.
mutating=$(cat "$tmp"/gh-board-*.log "$tmp"/gh-bauth-*.log "$tmp"/gh8i.log "$tmp"/gh8j.log 2>/dev/null \
  | grep -Ei 'item-edit|item-add|item-delete|item-archive|--field|mutation' | head -5)
if [ -z "$mutating" ]; then
  ok "board: probe never invoked a mutating gh/board operation"
else
  bad "board: probe issued a MUTATING call: $mutating"
fi

# --- 9. Notification channel (issue #3119) ----------------------------------
# The notify dep used to be an out-of-band, hand-patched /usr/local/bin binary
# that bootstrap never mentioned. It is now a repo-owned contract, and bootstrap
# must (a) assert the CAPABILITY by running the wrapper's own self-test rather
# than checking that a file exists, (b) RECORD the pinned contract version, and
# (c) never prescribe the swallowed `--category` shape.
if grep -q 'NOTIFY_LIB=.*scripts/lib/gate-notify.sh' "$BOOTSTRAP" \
   && grep -q '\$NOTIFY_LIB" --self-test' "$BOOTSTRAP"; then
  ok "notify: bootstrap asserts the CAPABILITY via the wrapper's self-test"
else
  bad "notify: bootstrap does not run the wrapper's self-test (existence check only?)"
fi
if out_has "$run_out" 'notify contract v'; then
  ok "notify: bootstrap RECORDS the pinned contract version"
else
  bad "notify: bootstrap did not record a pinned notify contract version"
fi
if ! grep -q 'agent-notify --category' "$BOOTSTRAP"; then   # notify-flag-allow
  ok "notify: bootstrap never prescribes the swallowed --category shape"
else
  bad "notify: bootstrap prescribes the swallowed --category shape"
fi
# The section is informational: a machine with no notify target must still finish.
if out_has "$run_out" 'Notification channel' && [ "$run_rc" -eq 0 ]; then
  ok "notify: the section is informational — the run still exits 0"
else
  bad "notify: the section is not informational (rc=$run_rc)"
fi
# A notify target may carry URL userinfo (https://user:token@host/topic). Printing
# it would leak a credential into a terminal or a CI log (roborev finding), so the
# reported value must name the HOST only.
redact_out=$(PATH="$tmp:$PATH" HOME="$host_home" CARGO_HOME="$host_home/.cargo" \
  CODEX_NOTIFY_WEBHOOK='https://alice:s3cr3t-token@ntfy.example.com/private-topic' \
  "$PIN_BS" "$BOOTSTRAP" --skip-smoke --skip-push-probe 2>&1)
if out_has "$redact_out" 'notify target configured' \
   && ! out_has "$redact_out" -E 's3cr3t-token|alice:'; then
  ok "notify: URL userinfo is redacted from the reported target"
else
  bad "notify: the reported target leaked URL userinfo"
  printf '%s\n' "$redact_out" | grep -i 'notify target' | head -2
fi

# --- 10. The notify CAPABILITY assert is HONOURED, not merely present ---------
# THE gap this closes (issue #3119 AC5, found by the C intent audit). Case 9 above
# greps the SCRIPT TEXT for a `--self-test` call and the run output for a pin line.
# Neither observes the `ok`-vs-`warn` DISTINCTION, so the auditor's mutation —
# `if selftest_out=$(… --self-test); true; then ok …` plus a genuinely broken wrapper —
# left this suite 77 PASS / 0 FAIL. That is this very issue reproduced one layer up:
# the TEST was accepting the mere EXISTENCE of a probe call as evidence that the
# capability is verified. These cases assert the VERDICT.
#
# Mechanism: bootstrap resolves REPO_ROOT from its own location, so each case gets a
# throwaway tree holding a copy of the script plus the wrapper we want it to probe.
mknotifyroot() { # mknotifyroot <dir> <good|broken>
  local dir="$1" mode="$2"
  mkdir -p "$dir/scripts/lib"
  cp "$BOOTSTRAP" "$dir/scripts/bootstrap-agent-machine.sh"
  if [ "$mode" = good ]; then
    cp "$SCRIPT_DIR/../lib/gate-notify.sh" "$dir/scripts/lib/gate-notify.sh"
  else
    # A REAL contract violation, caught by the wrapper's own validator: the FAIL
    # payload carries the PASS tag — a red gate paging as a routine success.
    # Heredoc body must be UNINDENTED: `<<'MUT'` is literal, so leading spaces reach
    # python as indentation and raise IndentationError — which would red on STAGING
    # instead of on the verdict under test (observed while proving this very case).
    python3 - "$SCRIPT_DIR/../lib/gate-notify.sh" "$dir/scripts/lib/gate-notify.sh" <<'MUT'
import re, sys
s = open(sys.argv[1]).read()
# Match on the FUNCTION NAME, not on the tag literal: a literal-based mutation is a
# no-op when the source is ALREADY broken (e.g. while proving this case against a
# mutated tree), which would again red on staging rather than on the verdict.
out = re.sub(r'^_gate_notify_tag\(\).*$',
             "_gate_notify_tag() { printf 'white_check_mark\\n'; }",
             s, count=1, flags=re.M)
open(sys.argv[2], "w").write(out)
raise SystemExit(0 if out != s else 1)
MUT
    [ $? -eq 0 ] || return 1
  fi
  return 0
}
runnotifyroot() { # runnotifyroot <dir> [env assignments...]
  local dir="$1"; shift
  env PATH="$tmp:$PATH" HOME="$host_home" CARGO_HOME="$host_home/.cargo" "$@" \
    timeout -s KILL 300 "$PIN_BS" "$dir/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1
}

# (b) POSITIVE twin: a healthy wrapper must be reported VERIFIED.
goodroot="$tmp/notify-good"
if mknotifyroot "$goodroot" good; then
  good_out=$(runnotifyroot "$goodroot" CODEX_NOTIFY_WEBHOOK='https://ntfy.example.com/t')
  if out_has "$good_out" 'notify capability verified' \
     && ! out_has "$good_out" 'notify self-test FAILED'; then
    ok "notify capability: a HEALTHY wrapper is reported verified"
  else
    bad "notify capability: healthy wrapper was not reported verified"
    printf '%s\n' "$good_out" | grep -i 'notify' | head -4
  fi
else
  bad "notify capability: could not stage the healthy wrapper tree"
fi

# (a) NEGATIVE: a genuinely broken wrapper must be reported FAILED, and must NOT be
#     reported verified. This is the assertion the auditor's mutation defeats.
badroot="$tmp/notify-broken"
if mknotifyroot "$badroot" broken; then
  bad_out=$(runnotifyroot "$badroot" CODEX_NOTIFY_WEBHOOK='https://ntfy.example.com/t')
  if out_has "$bad_out" 'notify self-test FAILED' \
     && ! out_has "$bad_out" 'notify capability verified'; then
    ok "notify capability: a BROKEN wrapper is reported FAILED and never verified"
  else
    bad "notify capability: broken wrapper was not surfaced (probe verdict ignored?)"
    printf '%s\n' "$bad_out" | grep -i 'notify' | head -4
  fi
else
  bad "notify capability: could not stage the broken wrapper tree (mutation did not apply)"
fi

# (c) NO TARGET: never exercised on a fleet box, because the ambient
#     CODEX_NOTIFY_WEBHOOK always takes the other branch. Assert the warning, the
#     EXACT export text a reader is told to add, and rc 0 (bootstrap is advisory).
if mknotifyroot "$tmp/notify-notarget" good; then
  notarget_out=$(runnotifyroot "$tmp/notify-notarget" \
    CODEX_NOTIFY_WEBHOOK= CQLITE_NOTIFY_WEBHOOK= CODEX_NOTIFY_NTFY_TOPIC= CQLITE_NOTIFY_TOPIC=)
  notarget_rc=$?
  if [ "$notarget_rc" -eq 0 ] \
     && out_has "$notarget_out" 'no notify target configured' \
     && out_has "$notarget_out" 'CODEX_NOTIFY_WEBHOOK=https://ntfy.sh/<your-topic>' \
     && out_has "$notarget_out" 'silent no-ops on this machine'; then
    ok "notify no-target: warns, prints the exact export line, and still exits 0"
  else
    bad "notify no-target case (rc=$notarget_rc)"
    printf '%s\n' "$notarget_out" | grep -i 'notify' | head -4
  fi
else
  bad "notify no-target: could not stage the tree"
fi

# --- 11. Single-gate pin: the VERDICT is a session PROBE, not a file read (#3414) ---
# The defect this closes, in the section's own words: it reported `ok` from a GREP of
# the shell profile it had just written, or from the value it had INHERITED from its
# own caller. Both were true on every fleet box at once while NO gate could see the
# pin — Ubuntu's stock ~/.bashrc returns early for non-interactive shells — so every
# gate resolved the #1825 cap from the default formula and admitted co-tenants.
#
# These cases assert the VERDICT, in the shape case 10 above established: a NEGATIVE
# that the old code would have passed, its POSITIVE twin, and the degraded states that
# must warn rather than pass. The probe's fresh PAM session is stood in for by a `sudo`
# PATH shim, so nothing here needs sudo, root, or the host's real /etc/environment.

# mkpinshims <dir> <persisted-value|-> : a hermetic PATH whose `sudo` stands in for a
# fresh, profile-free session. `-` = a box where NOTHING is persisted system-wide, so
# the session starts from exactly the environment it was handed and injects nothing —
# which is what makes it able to catch a bootstrap that forgot to scrub its own
# inherited value. A value = a box where the pin IS persisted: the session injects it,
# as pam_env would from /etc/environment.
mkpinshims() {
  local dir="$1" val="$2" t bin
  mk_hermetic_bin "$dir"
  # mk_hermetic_bin links the coreutils the mold/cred cases need; the pin section also
  # needs `id` (to name the probe's runas user), `tee` (the append) and `true` (the
  # `sudo -n true` availability probe — the shim EXECs it, and on a hermetic PATH a
  # missing /usr/bin/true makes that probe look like a sudo that needs a password).
  for t in id tee true; do
    bin=$(type -P "$t" 2>/dev/null) || continue
    [ -n "$bin" ] && ln -sf "$bin" "$dir/$t" 2>/dev/null || true
  done
  if [ "${val#file:}" != "$val" ]; then
    # PAM STAND-IN: read the env file AT SESSION CREATION, exactly as pam_env does, so a
    # write performed earlier in the same bootstrap run is visible to this probe. Presence
    # of the line and its VALUE are separated (grep vs sed) so a present-but-empty line
    # injects an empty value rather than being treated as absent — the distinction the
    # gate's (invalid) classification turns on.
    mk_stub "$dir" sudo "while [ \"\${1:-}\" = \"-n\" ]; do shift; done
if [ \"\${1:-}\" = \"-u\" ]; then shift 2; fi
if [ \"\${1:-}\" = \"-i\" ]; then shift; fi
pam_file='${val#file:}'
if [ -f \"\$pam_file\" ] && grep -Eq '^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=' \"\$pam_file\"; then
  pam_val=\$(sed -n 's/^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=//p' \"\$pam_file\" | head -1)
  exec env CQLITE_GATE_MAX_CONCURRENCY=\"\$pam_val\" \"\$@\"
fi
exec \"\$@\""
  elif [ "$val" = "-" ]; then
    mk_stub "$dir" sudo 'while [ "${1:-}" = "-n" ]; do shift; done
if [ "${1:-}" = "-u" ]; then shift 2; fi
if [ "${1:-}" = "-i" ]; then shift; fi
exec "$@"'
  else
    mk_stub "$dir" sudo "while [ \"\${1:-}\" = \"-n\" ]; do shift; done
if [ \"\${1:-}\" = \"-u\" ]; then shift 2; fi
if [ \"\${1:-}\" = \"-i\" ]; then shift; fi
exec env CQLITE_GATE_MAX_CONCURRENCY=$val \"\$@\""
  fi
}

# runpin <root-dir> <shim-dir> <env-file> [NAME=VALUE...] [--flag...] — one bootstrap
# run. NAME=VALUE arguments become environment; anything starting with `-` becomes a
# bootstrap flag. HOME is per-call so a case can control what the shell profile says.
#
# CQLITE_GATE_MAX_CONCURRENCY IS SCRUBBED FROM EVERY CALL, and a case that wants it set
# passes it explicitly: this suite runs on fleet boxes that export the pin, so an
# inherited value would otherwise decide 11b's verdict instead of the case's own input.
# `env` applies its `-u` options before the NAME=VALUE assignments, so passing it still
# works.
runpin() {
  local root="$1" shims="$2" envfile="$3"; shift 3
  local -a pin_env=() pin_flags=()
  local a
  for a in "$@"; do
    case "$a" in
      -*) pin_flags+=("$a") ;;
      *) pin_env+=("$a") ;;
    esac
  done
  env -u CQLITE_GATE_MAX_CONCURRENCY \
    PATH="$shims" CARGO_HOME="$tmp/pin-cargo" \
    CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$envfile" \
    ${pin_env[@]+"${pin_env[@]}"} \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$root/scripts/bootstrap-agent-machine.sh" \
      --skip-smoke ${pin_flags[@]+"${pin_flags[@]}"} 2>&1
}

pinroot="$tmp/pin-root"
# THE SEAM IS REFUSED UNDER ROOT BY DESIGN (#3414 roborev round 5, HIGH), so this entire
# block is unexercisable as root. Reported as ONE counted `skip` rather than an `ok`:
# announcing a skip through ok() was round 2's finding, and reintroducing it inside the
# block that fixed it would be galling. The capability loss is accepted, not worked around.
if [ "$(id -u)" = 0 ]; then
  skip "gate-pin: the ENTIRE block (the test seam is refused under root, so section 5b cannot be driven here)"
elif ! mknotifyroot "$pinroot" good; then
  bad "gate-pin: could not stage the bootstrap tree"
elif ! cp "$SCRIPT_DIR/../agent-gate.sh" "$pinroot/scripts/agent-gate.sh"; then
  # The verdict asks the GATE what it will do with the probed value (rather than
  # re-deriving the gate's rules inside bootstrap), so the throwaway tree needs a real
  # agent-gate.sh. A tree WITHOUT one is its own case — 11m below.
  bad "gate-pin: could not stage agent-gate.sh into the bootstrap tree"
else
  mkdir -p "$tmp/pin-cargo"
  pin_home_plain="$tmp/pin-home-plain"; mkdir -p "$pin_home_plain/.cargo"

  # 11a. THE CASE. Nothing is persisted, but bootstrap's OWN environment carries the
  #      value — which is the normal state of a re-run on a fleet box. An unscrubbed
  #      probe returns the inherited value and reports the box healthy, i.e. it
  #      certifies exactly the failure this section exists to catch. Must be FAILED.
  shims_none="$tmp/pin-shims-none"; mkpinshims "$shims_none" -
  envf_a="$tmp/pin-env-a"; : >"$envf_a"
  out_a=$(runpin "$pinroot" "$shims_none" "$envf_a" HOME="$pin_home_plain" \
    CQLITE_GATE_MAX_CONCURRENCY=7)
  if out_has "$out_a" 'gate-pin: FAILED' \
     && ! out_has "$out_a" 'gate-pin: VERIFIED'; then
    ok "gate-pin: an INHERITED-but-not-persisted value is FAILED, never VERIFIED (the scrub is honoured)"
  else
    bad "gate-pin: an inherited value was accepted as evidence the box is pinned"
    printf '%s\n' "$out_a" | grep -i 'gate-pin' | head -3
  fi

  # 11b. POSITIVE twin: the pin IS visible to a fresh session (and this run's own
  #      environment does NOT carry it), so VERIFIED must be reachable. Without this,
  #      11a would also pass against a section that can only ever say FAILED.
  shims_one="$tmp/pin-shims-one"; mkpinshims "$shims_one" 1
  envf_b="$tmp/pin-env-b"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_b"
  out_b=$(runpin "$pinroot" "$shims_one" "$envf_b" HOME="$pin_home_plain")
  if out_has "$out_b" 'gate-pin: VERIFIED' \
     && ! out_has "$out_b" 'gate-pin: FAILED'; then
    ok "gate-pin: a pin a fresh profile-free session CAN see is reported VERIFIED"
  else
    bad "gate-pin: a genuinely visible pin was not reported VERIFIED"
    printf '%s\n' "$out_b" | grep -i 'gate-pin' | head -3
  fi

  # 11c. NO SUDO BINARY: no session can be created, so nothing was measured. It must
  #      warn — an unmeasured capability may never inherit the permissive branch.
  shims_nosudo="$tmp/pin-shims-nosudo"; mkpinshims "$shims_nosudo" -; rm -f "$shims_nosudo/sudo"
  envf_c="$tmp/pin-env-c"; : >"$envf_c"
  out_c=$(runpin "$pinroot" "$shims_nosudo" "$envf_c" HOME="$pin_home_plain" \
    CQLITE_GATE_MAX_CONCURRENCY=7)
  if out_has "$out_c" -E '\[warn\].*gate-pin: UNMEASURED' \
     && out_has "$out_c" "no 'sudo' on this box" \
     && ! out_has "$out_c" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: no sudo binary => UNMEASURED as a [warn], never an [ok]"
  else
    bad "gate-pin: a box with no sudo did not report UNMEASURED-as-a-warn"
    printf '%s\n' "$out_c" | grep -i 'gate-pin' | head -3
  fi

  # 11d. SUDO NEEDS A PASSWORD: `sudo -n` never prompts, so the probe cannot run.
  #      Same posture, different cause text — a remedy that names the wrong cause
  #      costs the operator a cycle before they learn it does not apply.
  shims_pw="$tmp/pin-shims-pw"; mkpinshims "$shims_pw" -; mk_stub "$shims_pw" sudo 'exit 1'
  envf_d="$tmp/pin-env-d"; : >"$envf_d"
  out_d=$(runpin "$pinroot" "$shims_pw" "$envf_d" HOME="$pin_home_plain" \
    CQLITE_GATE_MAX_CONCURRENCY=7)
  if out_has "$out_d" -E '\[warn\].*gate-pin: UNMEASURED' \
     && out_has "$out_d" 'will not open a session as' \
     && ! out_has "$out_d" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: a sudo that cannot open a self-session => UNMEASURED as a [warn], with its own cause"
  else
    bad "gate-pin: a password-requiring sudo did not report UNMEASURED-as-a-warn"
    printf '%s\n' "$out_d" | grep -i 'gate-pin' | head -3
  fi

  # 11e. --yes PERSISTS into the system env file, and does so IDEMPOTENTLY. Two runs
  #      must leave exactly one CQLITE_GATE_MAX_CONCURRENCY line: /etc/environment is
  #      a hot, hand-edited file and a bootstrap that appends on every re-run would
  #      grow it without bound.
  envf_e="$tmp/pin-env-e"; : >"$envf_e"
  runpin "$pinroot" "$shims_none" "$envf_e" HOME="$pin_home_plain" --yes >/dev/null 2>&1
  runpin "$pinroot" "$shims_none" "$envf_e" HOME="$pin_home_plain" --yes >/dev/null 2>&1
  pin_e_n=$(grep -c '^CQLITE_GATE_MAX_CONCURRENCY=1$' "$envf_e" 2>/dev/null || true)
  if [ "${pin_e_n:-0}" = 1 ]; then
    ok "gate-pin: --yes persists the pin into the system env file, idempotently across re-runs"
  else
    bad "gate-pin: expected exactly one persisted pin line, got ${pin_e_n:-0}"
    cat "$envf_e"
  fi

  # 11f. AN EXISTING VALUE IS NEVER REWRITTEN. A box deliberately running >1
  #      concurrent gate overrides the pin, and clobbering that back to 1 on the next
  #      bootstrap would be a silent regression of a deliberate operator decision.
  #      The run must also SAY it left the value alone: asserting only that the file is
  #      unchanged is satisfied by a bootstrap that never writes at all, so the case
  #      would pass against the very code this replaces.
  #
  #      SCOPED TO THE PIN LINE, NOT TO THE WHOLE FILE (issue #3727). This used to be
  #      `[ "$(cat "$envf_f")" = "CQLITE_GATE_MAX_CONCURRENCY=4" ]`, which asserts something this
  #      case does not own: that NOTHING ELSE ever writes to the shared sandbox env file. Section
  #      5b2 legitimately appends SCCACHE_CACHE_SIZE under `--yes`, so the equality broke the
  #      moment that feature stopped being inert — and it had been GREEN only because the fleet
  #      cap literal was still an unsubstituted placeholder that bootstrap refused to persist.
  #      Same family as the `base_warns` drift: a case coupled to a NEIGHBOUR section's output,
  #      passing for a reason that had nothing to do with its subject. The property is: exactly
  #      ONE pin assignment, still carrying the operator's 4 — which a rewrite to 1, a duplicate
  #      append, and a deletion all still fail.
  envf_f="$tmp/pin-env-f"; printf 'CQLITE_GATE_MAX_CONCURRENCY=4\n' >"$envf_f"
  out_f=$(runpin "$pinroot" "$shims_none" "$envf_f" HOME="$pin_home_plain" --yes)
  pin_f_n=$(grep -cE '^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=' "$envf_f" 2>/dev/null)
  pin_f_val=$(sed -n 's/^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=//p' "$envf_f" 2>/dev/null | tail -1)
  if [ "${pin_f_n:-0}" = 1 ] && [ "$pin_f_val" = 4 ] \
     && out_has "$out_f" 'already carries a CQLITE_GATE_MAX_CONCURRENCY line — left EXACTLY as it is'; then
    ok "gate-pin: an existing CQLITE_GATE_MAX_CONCURRENCY value is left EXACTLY as it is, and the run says so"
  else
    bad "gate-pin: --yes rewrote a deliberate override (or never looked at the file) — ${pin_f_n:-0} pin assignment(s), value '$pin_f_val'"
    cat "$envf_f"
    printf '%s\n' "$out_f" | grep -i 'gate-pin\|already carries' | head -2
  fi

  # 11g. A file whose last byte is not a newline must not have the pin welded onto its
  #      final line — pam_env would read the join as one malformed entry, i.e. the
  #      write would silently un-persist whatever was already there.
  envf_g="$tmp/pin-env-g"; printf 'FOO=bar' >"$envf_g"
  runpin "$pinroot" "$shims_none" "$envf_g" HOME="$pin_home_plain" --yes >/dev/null 2>&1
  if grep -q '^FOO=bar$' "$envf_g" && grep -q '^CQLITE_GATE_MAX_CONCURRENCY=1$' "$envf_g"; then
    ok "gate-pin: appending to a file with no trailing newline keeps both lines intact"
  else
    bad "gate-pin: the append welded onto the previous line"
    cat -A "$envf_g" | head -3
  fi

  # 11h. PRESENCE IN A PROFILE CAN NO LONGER REPORT SUCCESS. The profile carries the
  #      export AND this run inherits the value — the exact pair of proxies the old
  #      code passed on — while nothing is persisted where a session reads it.
  pin_home_prof="$tmp/pin-home-prof"; mkdir -p "$pin_home_prof/.cargo"
  printf 'export CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$pin_home_prof/.bashrc"
  envf_h="$tmp/pin-env-h"; : >"$envf_h"
  out_h=$(runpin "$pinroot" "$shims_none" "$envf_h" HOME="$pin_home_prof" \
    SHELL=/bin/bash CQLITE_GATE_MAX_CONCURRENCY=7)
  if out_has "$out_h" 'gate-pin: FAILED' \
     && ! out_has "$out_h" -E '\[ok\].*(gate-pin|CQLITE_GATE_MAX_CONCURRENCY)'; then
    ok "gate-pin: a profile that carries the export produces NO success verdict"
  else
    bad "gate-pin: a profile grep (or the inherited value) still bought a success verdict"
    printf '%s\n' "$out_h" | grep -iE 'gate-pin|CQLITE_GATE_MAX_CONCURRENCY' | head -4
  fi

  # 11i. STRUCTURAL, because the behavioural cases above can only cover the branches
  #      someone thought of: section 5b must contain EXACTLY ONE `ok` call, and it
  #      must be the probe's VERIFIED verdict. Any future `ok` added for a file write,
  #      a profile grep or an inherited value reds this immediately.
  # BOUNDED AT 5b2, NOT AT 5c (issue #3727): section 5b2 sits between them and has its own
  # single `ok`, so the old range counted both sections' success verdicts and this guard fired
  # on a correct change. 12b-o below is the same assertion for 5b2.
  pin_section=$(awk '/^# ---- 5b\./,/^# ---- 5b2\./' "$BOOTSTRAP")
  # TWO success verdicts now, and both are ENUMERATED rather than merely counted: the
  # probe's VERIFIED, and the non-Linux NOT-APPLICABLE (an explicit inapplicability, which
  # must be an [ok] so a correctly-configured Mac is not permanently non-passing). Naming
  # them is what keeps this a real guard — a bare count of 2 would let a third `ok` in as
  # soon as someone removed one of these.
  pin_ok_total=$(printf '%s\n' "$pin_section" | grep -cE '^[[:space:]]*ok "' || true)
  pin_ok_named=$(printf '%s\n' "$pin_section" | grep -cE '^[[:space:]]*ok "gate-pin: VERIFIED [(]' || true)
  # ONE success verdict again (#3414 round 14): the non-Linux `ok` was deleted, because on a
  # platform with no system-wide file to correlate against no verdict that reports a state
  # is available. A second `ok` reappearing here means someone re-added an exemption.
  if [ -n "$pin_section" ] && [ "${pin_ok_total:-0}" = 1 ] && [ "${pin_ok_named:-0}" = 1 ]; then
    ok "gate-pin: section 5b's ONLY success verdict is VERIFIED (no platform exemption)"
  else
    bad "gate-pin: section 5b has ${pin_ok_total:-0} ok() call(s), ${pin_ok_named:-0} of them a named verdict"
  fi

  # 11j. The OPT-OUT is loud and NON-PASSING: a switch that returned `ok` would be a
  #      way to buy a vacuous green, which is the failure mode this section removes.
  envf_j="$tmp/pin-env-j"; : >"$envf_j"
  out_j=$(env PATH="$shims_one" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo" \
    CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$envf_j" \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
      --skip-smoke --skip-gate-pin 2>&1)
  if out_has "$out_j" -E '\[warn\].*gate-pin: OPT-OUT' \
     && ! out_has "$out_j" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: --skip-gate-pin is a [warn] OPT-OUT that can never buy a green"
  else
    bad "gate-pin: the opt-out did not report as a non-passing OPT-OUT"
    printf '%s\n' "$out_j" | grep -i 'gate-pin' | head -3
  fi

  # 11l. VISIBLE IS NOT HONOURED. A value the gate DISCARDS (non-numeric/empty) or
  #      silently CLAMPS (<1) is a pin in name only: bootstrap would be certifying a cap
  #      the gate does not apply, while the gate's own summary says (invalid)/(clamped)
  #      for the same box. That is this issue's shape one level further out, so it gets
  #      its own non-passing verdict — never VERIFIED, and never the bare FAILED, whose
  #      remedy ("persist the pin") is wrong for a pin that is already there.
  for pin_case in "abc:DISCARDS" "0:silently raises it to 1"; do
    pin_val=${pin_case%%:*}; pin_expect=${pin_case#*:}
    shims_nh="$tmp/pin-shims-nh-$pin_val"; mkpinshims "$shims_nh" "$pin_val"
    envf_nh="$tmp/pin-env-nh-$pin_val"; printf 'CQLITE_GATE_MAX_CONCURRENCY=%s\n' "$pin_val" >"$envf_nh"
    out_nh=$(runpin "$pinroot" "$shims_nh" "$envf_nh" HOME="$pin_home_plain")
    if out_has "$out_nh" 'gate-pin: NOT-HONOURED' \
       && out_has "$out_nh" "$pin_expect" \
       && ! out_has "$out_nh" -E '\[ok\].*gate-pin'; then
      ok "gate-pin: a visible '$pin_val' the gate does not honour is NOT-HONOURED, never VERIFIED"
    else
      bad "gate-pin: a visible-but-not-honoured '$pin_val' was not surfaced"
      printf '%s\n' "$out_nh" | grep -i 'gate-pin' | head -3
    fi
  done

  # 11m. A value >= 1 is VERIFIED whatever the number: a box deliberately running >1
  #      concurrent gate is legitimate and bootstrap correctly never rewrites it, so the
  #      verdict is "visible AND honoured", not "equal to 1". Without this, 11l would
  #      also pass against a section that only ever accepts the literal 1.
  shims_four="$tmp/pin-shims-four"; mkpinshims "$shims_four" 4
  envf_m="$tmp/pin-env-m"; printf 'CQLITE_GATE_MAX_CONCURRENCY=4\n' >"$envf_m"
  out_m=$(runpin "$pinroot" "$shims_four" "$envf_m" HOME="$pin_home_plain")
  if out_has "$out_m" 'gate-pin: VERIFIED' \
     && out_has "$out_m" 'max-concurrency=4(pinned)'; then
    ok "gate-pin: a deliberate override >1 is VERIFIED and reported as the cap the gate will apply"
  else
    bad "gate-pin: a legitimate >1 override was not VERIFIED"
    printf '%s\n' "$out_m" | grep -i 'gate-pin' | head -3
  fi

  # 11n. THE HONOURING ORACLE IS NOT OPTIONAL. With no agent-gate.sh to consult, the
  #      second half of the question is unanswered — that must be UNMEASURED, never an
  #      assumed pass. (A positive verdict requires an affirmative measurement.)
  nogate="$tmp/pin-root-nogate"
  if mknotifyroot "$nogate" good; then
    envf_n="$tmp/pin-env-n"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_n"
    out_n=$(runpin "$nogate" "$shims_one" "$envf_n" HOME="$pin_home_plain")
    if out_has "$out_n" -E '\[warn\].*gate-pin: UNMEASURED' \
       && out_has "$out_n" 'could not be consulted to confirm' \
       && ! out_has "$out_n" -E '\[ok\].*gate-pin'; then
      ok "gate-pin: a visible pin whose honouring could NOT be checked is UNMEASURED, not VERIFIED"
    else
      bad "gate-pin: an unconsultable gate still produced a verdict about honouring"
      printf '%s\n' "$out_n" | grep -i 'gate-pin' | head -3
    fi
  else
    bad "gate-pin: could not stage the gate-less bootstrap tree"
  fi

  # 11o. THE PAM CASE. The pin IS in the file and a fresh session still does not see it.
  #      That is not a missing pin, and `--yes` cannot fix it — it finds the line already
  #      present and changes nothing, so an operator handed the persist remedy loops.
  #      The two boxes that reach FAILED must be told apart.
  envf_o="$tmp/pin-env-o"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_o"
  out_o=$(runpin "$pinroot" "$shims_none" "$envf_o" HOME="$pin_home_plain")
  if out_has "$out_o" 'gate-pin: FAILED' \
     && out_has "$out_o" 'this is a PAM condition, NOT a missing pin' \
     && out_has "$out_o" 'pam_env' \
     && ! out_has "$out_o" 'fix:  bash scripts/bootstrap-agent-machine.sh --yes'; then
    ok "gate-pin: a present-but-invisible pin is diagnosed as a PAM condition, not handed the persist remedy"
  else
    bad "gate-pin: the two FAILED boxes were not told apart"
    printf '%s\n' "$out_o" | grep -i 'gate-pin\|pam_env\|fix:' | head -4
  fi

  # 11p. ...and the ABSENT-file box is not handed a remedy naming a file it does not
  #      have. A remedy that cannot work on the box it is printed for costs a cycle
  #      before the operator learns that.
  #
  #      THIS CASE USED TO ASSERT THE WRONG HALF (roborev job 332). It ran on the
  #      suite's own LINUX host with the env file merely MISSING, and required the
  #      message "has nowhere to persist it" — which is a MAC's message. On Linux the
  #      missing file is CREATED by --fix-gate-pin, so the case was PINNING A FALSE
  #      DIAGNOSTIC in place: it simulated a Mac by removing a file, and "the file is
  #      absent" is not "this platform has no such file". Split in two, one per state,
  #      because a single case cannot distinguish them by construction.
  #
  #      11p covers the LINUX absent file: creatable, so the remedy must NAME the flag
  #      that creates it and must NOT claim there is nowhere to persist it.
  envf_p="$tmp/pin-env-p-missing"; rm -f "$envf_p"
  out_p=$(runpin "$pinroot" "$shims_none" "$envf_p" HOME="$pin_home_plain")
  if out_has "$out_p" 'gate-pin: FAILED' \
     && [[ $out_p == *'--fix-gate-pin'* ]] \
     && [[ $out_p != *'has nowhere to persist it'* ]]; then
    ok "gate-pin: a LINUX box with no env file is pointed at --fix-gate-pin, which creates it — not told there is nowhere to persist"
  else
    bad "gate-pin: the absent-env-file Linux box got the unmanaged-platform remedy (job 332)"
    printf '%s\n' "$out_p" | grep -i 'gate-pin\|nowhere\|fix:' | head -4
  fi

  # 11p2. ...and the UNMANAGED-PLATFORM box keeps the original ruling: no remedy naming a
  #      file the platform does not have. Same missing file as 11p; the ONLY difference is
  #      `uname`, which is what makes this the state 11p is not. Without this half the fix
  #      above could have deleted the Mac message entirely and still passed.
  shims_mac_p="$tmp/pin-shims-mac-p"; mkpinshims "$shims_mac_p" -
  mk_stub "$shims_mac_p" uname 'echo Darwin'
  envf_p2="$tmp/pin-env-p2-missing"; rm -f "$envf_p2"
  out_p2=$(runpin "$pinroot" "$shims_mac_p" "$envf_p2" HOME="$pin_home_plain")
  if [[ $out_p2 != *'--fix-gate-pin   (this'* ]] \
     && [[ $out_p2 != *'fix:  bash scripts/bootstrap-agent-machine.sh --yes'* ]] \
     && ! out_has "$out_p2" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: an unmanaged-platform box with no env file is never pointed at a file its platform does not read"
  else
    bad "gate-pin: the unmanaged-platform box was handed a create-the-file remedy"
    printf '%s\n' "$out_p2" | grep -i 'gate-pin\|nowhere\|fix:' | head -4
  fi

  # 11q. PERSIST-THEN-PROBE WITHIN ONE RUN — the property `verify.run` depends on.
  #      `--fix-gate-pin` writes the env file and the probe then opens a NEW session, which
  #      reads that file at session creation, so a box that starts unpinned must come out
  #      of the SAME invocation VERIFIED — no --yes, no re-login, no second run. If this
  #      does not hold, putting the flag in .agent-ami/profile.yaml's verify.run buys
  #      nothing. The `sudo` shim here stands in for pam_env by reading the file, so the
  #      case exercises the ORDERING rather than a canned answer.
  envf_q="$tmp/pin-env-q"; : >"$envf_q"
  shims_pam_q="$tmp/pin-shims-pam-q"; mkpinshims "$shims_pam_q" "file:$envf_q"
  out_q=$(runpin "$pinroot" "$shims_pam_q" "$envf_q" HOME="$pin_home_plain" --fix-gate-pin)
  if out_has "$out_q" 'gate-pin: VERIFIED' \
     && grep -q '^CQLITE_GATE_MAX_CONCURRENCY=1$' "$envf_q"; then
    ok "gate-pin: --fix-gate-pin persists AND the same run's probe then sees it (no --yes, no re-login)"
  else
    bad "gate-pin: persist-then-probe did not close within one run"
    printf '%s\n' "$out_q" | grep -i 'gate-pin' | head -3
    cat "$envf_q"
  fi

  # 11q4. AN OVERSIZED VALUE IS NOT "NON-NUMERIC", AND MUST NOT BE TOLD IT IS (roborev job
  #      333, Low). This branch widened `invalid` to include a digit string too large to
  #      represent, and the diagnosis still read "it is empty or non-numeric" while the
  #      remedy said "use a positive integer" — advice `99999999999999999999` HAS ALREADY
  #      TAKEN. A remedy the operator already complies with is worse than none: they find
  #      nothing wrong and re-run into the same verdict. Both halves are asserted, because
  #      naming the cause while leaving the remedy unqualified fixes only the visible half.
  envf_q4="$tmp/pin-env-q4"; printf 'CQLITE_GATE_MAX_CONCURRENCY=99999999999999999999\n' >"$envf_q4"
  shims_pam_q4="$tmp/pin-shims-pam-q4"; mkpinshims "$shims_pam_q4" "file:$envf_q4"
  out_q4=$(runpin "$pinroot" "$shims_pam_q4" "$envf_q4" HOME="$pin_home_plain")
  if out_has "$out_q4" 'gate-pin: NOT-HONOURED' \
     && [[ $out_q4 == *'too large to use as a slot cap'* ]] \
     && [[ $out_q4 != *'it is not a plain decimal integer'* ]] \
     && [[ $out_q4 == *'at most 18 digits'* ]]; then
    ok "gate-pin: an oversized pin is diagnosed BY SIZE and its remedy states the 18-digit bound (not 'use a positive integer')"
  else
    bad "gate-pin: an oversized pin was diagnosed as non-numeric or given advice it already satisfies (job 333)"
    printf '%s\n' "$out_q4" | grep -i 'gate-pin\|fix the VALUE' | head -4
  fi

  # 11q2. A VALID LEADING-ZERO PIN MUST REACH VERIFIED (roborev job 333, Medium). The gate
  #      NORMALISES `08` -> `8(pinned)`; bootstrap compared that normalised N against the RAW
  #      session value, so `8` != `08` demoted a CORRECTLY PERSISTED pin to UNMEASURED and
  #      `--strict` red on a properly pinned box. Introduced by this branch's own octal fix:
  #      normalisation was added to the gate and this comparison was not told about it.
  #
  #      THIS CASE IS ALSO THE ANTI-DRIFT MECHANISM for the canonicaliser bootstrap now
  #      mirrors from the gate. `$pinroot` carries the REAL agent-gate.sh (copied above), so
  #      if the gate's normalisation rule ever changes, this reds rather than the two rules
  #      silently diverging. That is why it is worth a case and not a comment.
  envf_q2="$tmp/pin-env-q2"; printf 'CQLITE_GATE_MAX_CONCURRENCY=08\n' >"$envf_q2"
  shims_pam_q2="$tmp/pin-shims-pam-q2"; mkpinshims "$shims_pam_q2" "file:$envf_q2"
  out_q2=$(runpin "$pinroot" "$shims_pam_q2" "$envf_q2" HOME="$pin_home_plain")
  if out_has "$out_q2" 'gate-pin: VERIFIED' \
     && ! out_has "$out_q2" 'gate-pin: UNMEASURED'; then
    ok "gate-pin: a valid leading-zero pin (08) reaches VERIFIED — the gate's normalisation is not read as oracle drift"
  else
    bad "gate-pin: a correctly persisted 08 was not VERIFIED (job 333 — raw-vs-normalised compare)"
    printf '%s\n' "$out_q2" | grep -i 'gate-pin' | head -3
  fi

  # 11q3. ...and the drift check it must NOT break: the whole point of comparing against our
  #      input is to catch an oracle answering about a DIFFERENT value. Canonicalising both
  #      sides preserves that, and this asserts it — otherwise 11q2 could have been "fixed"
  #      by deleting the comparison, which would pass 11q2 and silently remove a guard.
  #      `08` and `9` are both valid and both pinned, so only the COMPARISON distinguishes
  #      them: the session-visible value is 08 while a BASH_ENV-style pollution makes the
  #      oracle answer 9, and that must NOT read VERIFIED.
  envf_q3="$tmp/pin-env-q3"; printf 'CQLITE_GATE_MAX_CONCURRENCY=08\n' >"$envf_q3"
  shims_pam_q3="$tmp/pin-shims-pam-q3"; mkpinshims "$shims_pam_q3" "file:$envf_q3"
  # a gate whose cpu-budget line always answers 9(pinned), whatever it was handed
  cat > "$shims_pam_q3/../pin-fake-gate.sh" <<'FAKEGATE'
#!/usr/bin/env bash
echo "cpu-budget: wrapper=none ncpu=16 max-concurrency=9(pinned) cores-per-gate=1 build-jobs=1(derived) test-threads=1"
FAKEGATE
  chmod +x "$shims_pam_q3/../pin-fake-gate.sh" 2>/dev/null
  pinroot_q3="$tmp/pin-root-q3"; mkdir -p "$pinroot_q3/scripts"
  cp "$pinroot/scripts/bootstrap-agent-machine.sh" "$pinroot_q3/scripts/" 2>/dev/null
  cp "$shims_pam_q3/../pin-fake-gate.sh" "$pinroot_q3/scripts/agent-gate.sh" 2>/dev/null
  out_q3=$(runpin "$pinroot_q3" "$shims_pam_q3" "$envf_q3" HOME="$pin_home_plain")
  if ! out_has "$out_q3" 'gate-pin: VERIFIED'; then
    ok "gate-pin: an oracle answering about a DIFFERENT value is still refused — canonicalising did not delete the drift check"
  else
    bad "gate-pin: the drift check was lost — a gate answering 9(pinned) for a session showing 08 read VERIFIED"
    printf '%s\n' "$out_q3" | grep -i 'gate-pin' | head -3
  fi

  # 11r. The NEGATIVE twin of 11q, so it cannot pass for the wrong reason: with the SAME
  #      pam-stand-in shim and no repair flag, an unpinned box must still come out FAILED.
  #      Without this, 11q would also pass against a shim that injects unconditionally.
  envf_r="$tmp/pin-env-r"; : >"$envf_r"
  shims_pam_r="$tmp/pin-shims-pam-r"; mkpinshims "$shims_pam_r" "file:$envf_r"
  out_r=$(runpin "$pinroot" "$shims_pam_r" "$envf_r" HOME="$pin_home_plain")
  # `[ ! -s ]` IS a whole-file assertion, and the #3727 sweep checked it deliberately: it survives
  # section 5b2 because BOTH sections refuse to write without an explicit authorisation, and this
  # run passes none. So it now covers 5b2's no-unasked-write rule too — a bonus, but note that
  # adding `--yes`/`--fix-sccache-cap` to THIS case would make it fail for a reason unrelated to
  # the pin.
  if out_has "$out_r" 'gate-pin: FAILED' \
     && ! out_has "$out_r" -E '\[ok\].*gate-pin' \
     && [ ! -s "$envf_r" ]; then
    ok "gate-pin: without a repair flag the same unpinned box stays FAILED and nothing is written"
  else
    bad "gate-pin: the no-flag twin did not stay FAILED (or wrote without being asked)"
    printf '%s\n' "$out_r" | grep -i 'gate-pin' | head -3
  fi

  # 11s. --fix-gate-pin must stay NON-PASSING on a box it cannot certify. An onboarding
  #      instance without passwordless sudo has to red loudly — that is the whole point of
  #      putting the flag behind --strict in verify.run, and `--strict` keys off exactly
  #      this: a [warn] rather than an [ok].
  #
  #      It deliberately does NOT also assert "wrote nothing". Under the test seam the
  #      write is forced UNPRIVILEGED (that is what makes "no env var can steer a
  #      PRIVILEGED write" true), so a broken `sudo` cannot stop the sandbox write and the
  #      file-emptiness half would be asserting a property of the seam, not of the code.
  #      The refuse-to-persist path is covered behaviourally by 11s2 below instead.
  envf_s="$tmp/pin-env-s"; : >"$envf_s"
  out_s=$(runpin "$pinroot" "$shims_pw" "$envf_s" HOME="$pin_home_plain" --fix-gate-pin)
  if ! out_has "$out_s" -E '\[ok\].*gate-pin' \
     && out_has "$out_s" -E '\[warn\].*gate-pin:'; then
    ok "gate-pin: --fix-gate-pin on a box it cannot certify stays non-passing (so --strict exits 1)"
  else
    bad "gate-pin: --fix-gate-pin reported ok on a box without passwordless sudo"
    printf '%s\n' "$out_s" | grep -i 'gate-pin' | head -3
  fi

  # 11s2. ...and a genuine COULD-NOT-PERSIST condition refuses the write, says why, and
  #      still does not pass. An unreadable env file is the one such condition reachable
  #      through the seam: bootstrap will not append blind, because it cannot tell whether
  #      a line is already there and a blind append could duplicate or contradict it.
  #      Root can read a 0000 file, so the case would assert nothing as root — skipped
  #      rather than silently inverted.
  if [ "$(id -u)" = 0 ]; then
    # Through `skip`, NOT `ok` (#3414 roborev round 2): an `ok` here increments PASS and
    # leaves SKIP unchanged — a skip counted as a pass, sitting inside the very accounting
    # added to expose that.
    skip "gate-pin unreadable-env-file case (running as root: 0000 is still readable)"
  else
    envf_s2="$tmp/pin-env-s2"; printf 'FOO=bar\n' >"$envf_s2"; chmod 0000 "$envf_s2"
    # shims_none (a session that injects nothing) is the right stand-in here: the append
    # was refused, so the box IS unpinned and the probe must say so. Using a shim bound to
    # some OTHER case's file would report that file's pin and green this case for a reason
    # that has nothing to do with it.
    out_s2=$(runpin "$pinroot" "$shims_none" "$envf_s2" HOME="$pin_home_plain" --fix-gate-pin)
    chmod 0644 "$envf_s2"
    if out_has "$out_s2" 'cannot read' \
       && out_has "$out_s2" 'gate-pin: FAILED' \
       && ! out_has "$out_s2" -E '\[ok\].*gate-pin' \
       && [ "$(cat "$envf_s2")" = "FOO=bar" ]; then   # whole-file, and safe: the unreadable-file
       # branch precedes every write in BOTH sections, and --fix-gate-pin does not authorise 5b2's
       # (checked in the #3727 sweep — see 11f for the sibling that was NOT safe)
      ok "gate-pin: an unreadable env file refuses the append, says why, and does not pass"
    else
      bad "gate-pin: an unreadable env file was appended to blind (or still passed)"
      printf '%s\n' "$out_s2" | grep -i 'gate-pin\|cannot read' | head -3
      cat "$envf_s2"
    fi
  fi

  # 11t. Contradictory intents do not resolve silently: an explicit --skip-gate-pin beside
  #      --fix-gate-pin is a usage error, and flag ORDER must not change that.
  #      The MESSAGE is asserted too, not just the exit code: an unrecognised flag also
  #      exits 2, so a bare rc check would pass against a build that never learned
  #      --fix-gate-pin at all — a vacuous green in the exact place this case exists.
  for pin_order in "--skip-gate-pin --fix-gate-pin" "--fix-gate-pin --skip-gate-pin"; do
    # shellcheck disable=SC2086
    pin_t_out=$(env PATH="$shims_one" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo" \
      CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$tmp/pin-env-t" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke $pin_order 2>&1)
    pin_t_rc=$?
    if [ "$pin_t_rc" -eq 2 ] && out_has "$pin_t_out" 'contradictory'; then
      ok "gate-pin: '$pin_order' is a usage error naming the contradiction, whatever the order"
    else
      bad "gate-pin: '$pin_order' did not exit 2 naming the contradiction (rc=$pin_t_rc)"
      printf '%s\n' "$pin_t_out" | head -2
    fi
  done

  # 11u. ...but the weaker ENV opt-out yields to an explicit --fix-gate-pin, so a harness
  #      that exports CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 cannot neuter a caller's repair.
  envf_u="$tmp/pin-env-u"; : >"$envf_u"
  shims_pam_u="$tmp/pin-shims-pam-u"; mkpinshims "$shims_pam_u" "file:$envf_u"
  out_u=$(runpin "$pinroot" "$shims_pam_u" "$envf_u" HOME="$pin_home_plain" \
    CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 --fix-gate-pin)
  if out_has "$out_u" 'gate-pin: VERIFIED' \
     && ! out_has "$out_u" 'gate-pin: OPT-OUT'; then
    ok "gate-pin: an explicit --fix-gate-pin overrides the weaker env opt-out"
  else
    bad "gate-pin: the env opt-out neutered an explicit --fix-gate-pin"
    printf '%s\n' "$out_u" | grep -i 'gate-pin' | head -3
  fi


  # 11w. BASH_ENV MUST NOT BE ABLE TO FORGE A PIN (issue #3414 roborev, Medium).
  #      A NON-INTERACTIVE bash sources $BASH_ENV before running its command. On a box
  #      whose sudoers lacks `Defaults env_reset` an inherited BASH_ENV survives into the
  #      probe, and that file can `export CQLITE_GATE_MAX_CONCURRENCY` — so scrubbing the
  #      variable while leaving the mechanism that re-injects it is not a scrub, and the
  #      run reports VERIFIED with nothing persisted anywhere. The `-` shim models exactly
  #      that box: it strips sudo's flags and execs, passing the environment straight
  #      through, which is what a missing env_reset does.
  pin_bashenv="$tmp/pin-bashenv.sh"
  printf 'export CQLITE_GATE_MAX_CONCURRENCY=9\n' >"$pin_bashenv"
  envf_w="$tmp/pin-env-w"; : >"$envf_w"
  out_w=$(runpin "$pinroot" "$shims_none" "$envf_w" HOME="$pin_home_plain" \
    BASH_ENV="$pin_bashenv")
  if out_has "$out_w" 'gate-pin: FAILED' \
     && ! out_has "$out_w" -E '\[ok\].*gate-pin' \
     && ! out_has "$out_w" 'CQLITE_GATE_MAX_CONCURRENCY=9'; then
    ok "gate-pin: a BASH_ENV file exporting the pin cannot forge VERIFIED (BASH_ENV is scrubbed)"
  else
    bad "gate-pin: BASH_ENV injected a pin the box does not have"
    printf '%s\n' "$out_w" | grep -i 'gate-pin' | head -3
  fi

  # 11x. ...and its POSITIVE twin, so 11w cannot pass against a probe that is simply
  #      broken: the same shim and the same BASH_ENV file, but with the pin genuinely
  #      persisted, must still reach VERIFIED. Without this, deleting the probe entirely
  #      would green 11w.
  envf_x="$tmp/pin-env-x"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_x"
  shims_pam_x="$tmp/pin-shims-pam-x"; mkpinshims "$shims_pam_x" "file:$envf_x"
  out_x=$(runpin "$pinroot" "$shims_pam_x" "$envf_x" HOME="$pin_home_plain" \
    BASH_ENV="$pin_bashenv")
  if out_has "$out_x" 'gate-pin: VERIFIED' \
     && out_has "$out_x" 'max-concurrency=1(pinned)'; then
    ok "gate-pin: scrubbing BASH_ENV does not break a genuinely pinned box"
  else
    bad "gate-pin: the BASH_ENV scrub broke the positive path"
    printf '%s\n' "$out_x" | grep -i 'gate-pin' | head -3
  fi

  # 11y. VERIFIED STATES ITS OWN SCOPE (issue #3414 review B2). The probe measures a
  #      PAM-created sudo session; a gate launched from a systemd unit or a container
  #      entrypoint has no PAM in its ancestry and never has /etc/environment applied, so
  #      an unqualified VERIFIED reads as a guarantee the probe cannot give. The verdict
  #      must therefore carry the limit AND name the gate's own cpu-budget token as the
  #      authoritative per-run confirmation — and must not re-assert the attribution it
  #      cannot establish ("came from the system env file").
  # Scoped to the ATTRIBUTION half; the scope-note WORDING is 11z4's subject, so the two
  # cases do not both red on one rewording. What must never come back is the claim the
  # probe cannot establish — that the value "came from the system env file" — since
  # ~/.pam_environment or a sudoers env_file satisfies the observation identically. (What
  # licenses the system-wide claim now is the FILE correlation, not the probe.)
  if ! out_has "$out_x" 'came from the system env file' \
     && out_has "$out_x" 'max-concurrency=N(pinned)'; then
    ok "gate-pin: VERIFIED does not re-assert the unestablishable attribution, and names cpu-budget as authoritative"
  else
    bad "gate-pin: VERIFIED claims an attribution the probe cannot make (or dropped the cpu-budget pointer)"
    printf '%s\n' "$out_x" | grep -iA 2 'gate-pin: VERIFIED' | head -4
  fi

  # 11z. VERIFIED REQUIRES BOTH HALVES (issue #3414 roborev round 2). A session that sees
  #      the value while the system-wide file does NOT carry the line means the value is
  #      arriving from something sudo- or user-specific (a sudoers env_file,
  #      ~/.pam_environment) — real for the session bootstrap opened, absent for every
  #      gate launched outside it. Scoping the TEXT was not enough: the verdict was still
  #      an `ok`, so zero warnings still bought "All checks green." and verify.run still
  #      passed on such a box.
  envf_z="$tmp/pin-env-z"; : >"$envf_z"          # file has NO pin line ...
  out_z=$(runpin "$pinroot" "$shims_one" "$envf_z" HOME="$pin_home_plain")   # ... session DOES see one
  if out_has "$out_z" 'gate-pin: NOT-SYSTEM-WIDE' \
     && ! out_has "$out_z" -E '\[ok\].*gate-pin' \
     && out_has "$out_z" 'sudo- or user-specific source'; then
    ok "gate-pin: a session-visible value with NO line in the system env file is NOT-SYSTEM-WIDE, never VERIFIED"
  else
    bad "gate-pin: a sudo-only value was certified as a system-wide pin"
    printf '%s\n' "$out_z" | grep -i 'gate-pin' | head -3
  fi

  # 11z2. ...and the file half alone is not enough either, which is the ORIGINAL #3414
  #      defect: the line is in the file but no session sees it. Asserting both directions
  #      is what makes "neither half suffices" a tested property rather than a comment.
  #      (11b/11q already cover the both-halves-present positive.)
  envf_z2="$tmp/pin-env-z2"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_z2"
  out_z2=$(runpin "$pinroot" "$shims_none" "$envf_z2" HOME="$pin_home_plain")
  if out_has "$out_z2" 'gate-pin: FAILED' \
     && ! out_has "$out_z2" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: the file half ALONE is not VERIFIED either (the original #3414 defect)"
  else
    bad "gate-pin: a file line with no session visibility was treated as a pin"
    printf '%s\n' "$out_z2" | grep -i 'gate-pin' | head -3
  fi

  # 11z3. An UNREADABLE system env file cannot be correlated, so the run has only half the
  #      evidence the verdict requires — UNMEASURED, never VERIFIED. Root can read a 0000
  #      file, so the case would assert nothing there.
  if [ "$(id -u)" = 0 ]; then
    skip "gate-pin uncorrelatable-file case (running as root: 0000 is still readable)"
  else
    envf_z3="$tmp/pin-env-z3"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_z3"; chmod 0000 "$envf_z3"
    out_z3=$(runpin "$pinroot" "$shims_one" "$envf_z3" HOME="$pin_home_plain")
    chmod 0644 "$envf_z3"
    if out_has "$out_z3" -E '\[warn\].*gate-pin: UNMEASURED' \
       && out_has "$out_z3" 'could not be READ' \
       && ! out_has "$out_z3" -E '\[ok\].*gate-pin'; then
      ok "gate-pin: an unreadable system env file cannot be correlated => UNMEASURED, not VERIFIED"
    else
      bad "gate-pin: an uncorrelatable file still produced a verdict about system-wide scope"
      printf '%s\n' "$out_z3" | grep -i 'gate-pin' | head -3
    fi
  fi

  # 11z4. The scope note must now claim the CORRELATED scope — every PAM stack, not just
  #      the sudo session the probe opened — while still naming the residual it cannot
  #      cover. A note that under-claims after the correlation is as wrong as one that
  #      over-claimed before it.
  # The claim STRENGTHENED with the weaken-only PAM check (roborev round 5): the note used
  # to assert that pam_env reads the file in every stack; it now says those stacks were
  # CHECKED. Asserting the checked wording is the point — the earlier phrasing was a
  # statement about PAM in general, this one is a statement about THIS box.
  # The PAM weaken-signal was DELETED (#3414 round 7 ruling), so the note no longer claims
  # the service stacks were checked — it now states that they are NOT checked here. That
  # is the whole point of the deletion: the note must not assert a scope nothing measured.
  # AND IT MUST DISCLOSE THE TEMPORAL HALF (#3728). pam_env reads the file at SESSION
  # CREATION, so a VERIFIED verdict is about FUTURE sessions: this shell and everything
  # already descended from it — including workers a launcher started earlier — do not have
  # the pin until their sessions are recreated. That was disclosed in the PR body and in
  # #3728 and NOWHERE IN THE EMITTED LINE, which is the only place an operator reads. A
  # caveat that lives where only a caveat-hunter looks is not a disclosure; asserted here
  # so a later edit cannot drop it silently.
  if out_has "$out_x" 'is NOT checked here' \
     && out_has "$out_x" 'created WITHOUT PAM' \
     && out_has "$out_x" 'SESSION CREATION' \
     && out_has "$out_x" 'already descended from it' \
     && out_has "$out_x" 'max-concurrency=N(pinned)'; then
    ok "gate-pin: the scope note states what it did NOT check, that the verdict covers FUTURE sessions only, and names the gate's token as authority"
  else
    bad "gate-pin: the scope note does not match the correlated verdict"
    printf '%s\n' "$out_x" | grep -iA 3 'gate-pin: VERIFIED' | head -4
  fi

  # 11z5. THE NEGATIVE ROW IS INDEPENDENT OF FILE STATE (issue #3414, lead ruling). The
  #      verdict is a conjunction of two measurements, not a file-state precedence: when
  #      the session does NOT see the value, that is an affirmative measurement and the
  #      verdict is FAILED whatever the file says — present, absent, or unreadable. The
  #      rule is that an unmeasurable half may weaken a POSITIVE claim but may never
  #      soften a NEGATIVE one, so an unreadable file must not downgrade a real FAILED to
  #      UNMEASURED. Asserted across all three file states in one loop, because the
  #      individual cases (11w absent, 11z2 present, 11s2 unreadable) each check one row
  #      and none of them states the INVARIANT that binds the three.
  for pin_row in absent present mismatched unreadable; do
    envf_r5="$tmp/pin-env-r5-$pin_row"
    case "$pin_row" in
      absent)     : >"$envf_r5" ;;
      present)    printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_r5" ;;
      # `mismatched` joined the loop with the value check (roborev round 3): the new
      # comparison must not become a way for file state to soften a negative either.
      mismatched) printf 'CQLITE_GATE_MAX_CONCURRENCY=abc\n' >"$envf_r5" ;;
      unreadable) printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_r5"; chmod 0000 "$envf_r5" ;;
    esac
    if [ "$pin_row" = unreadable ] && [ "$(id -u)" = 0 ]; then
      skip "gate-pin negative-row invariant, unreadable file (running as root: 0000 is still readable)"
      continue
    fi
    out_r5=$(runpin "$pinroot" "$shims_none" "$envf_r5" HOME="$pin_home_plain")
    chmod 0644 "$envf_r5" 2>/dev/null || true
    if out_has "$out_r5" 'gate-pin: FAILED' \
       && ! out_has "$out_r5" -E 'gate-pin: (UNMEASURED|NOT-SYSTEM-WIDE|VERIFIED)'; then
      ok "gate-pin: session-cannot-see-it => FAILED with the env file $pin_row (file state cannot soften a negative)"
    else
      bad "gate-pin: file state '$pin_row' changed a NEGATIVE probe's verdict"
      printf '%s\n' "$out_r5" | grep -i 'gate-pin' | head -2
    fi
  done

  # 11aa. THE FILE'S VALUE MUST EQUAL THE SESSION'S, not merely exist (issue #3414 roborev
  #      round 3 — the FOURTH instance of this issue's own defect in this lane, and it was
  #      inside the correlation added to fix the third). File says `abc`, a sudo- or
  #      user-specific source supplies `1`: both halves of "line present AND session sees
  #      it" hold, so the old check said VERIFIED — while every ordinary PAM session gets
  #      `abc`, which the gate discards for its default formula and stamps N(invalid).
  envf_aa="$tmp/pin-env-aa"; printf 'CQLITE_GATE_MAX_CONCURRENCY=abc\n' >"$envf_aa"
  out_aa=$(runpin "$pinroot" "$shims_one" "$envf_aa" HOME="$pin_home_plain")   # session sees 1
  if out_has "$out_aa" 'gate-pin: NOT-SYSTEM-WIDE' \
     && ! out_has "$out_aa" -E '\[ok\].*gate-pin' \
     && out_has "$out_aa" "OVERRIDING the system-wide file"; then
    ok "gate-pin: a file value the session does NOT match is not VERIFIED (presence is not the predicate)"
  else
    bad "gate-pin: a file/session VALUE mismatch was certified"
    printf '%s\n' "$out_aa" | grep -i 'gate-pin' | head -3
  fi

  # 11ab. ...and the equality is a STRING comparison, deliberately not a numeric one. The
  #      gate's own resolver discards `1 ` (trailing space matches *[!0-9]*), so treating
  #      it as equal to `1` here would make bootstrap certify a value the gate rejects —
  #      a second classifier disagreeing with the one that decides, which is the thing
  #      pin_gate_source_for exists to avoid.
  envf_ab="$tmp/pin-env-ab"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1 \n' >"$envf_ab"
  out_ab=$(runpin "$pinroot" "$shims_one" "$envf_ab" HOME="$pin_home_plain")
  if ! out_has "$out_ab" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: '1 ' in the file is not equal to '1' in the session (string equality, as the gate resolves it)"
  else
    bad "gate-pin: a value the gate would DISCARD was normalised into a match"
    printf '%s\n' "$out_ab" | grep -i 'gate-pin' | head -2
  fi

  # 11ac. A NON-LINUX HOST IS ASKED A NARROWER QUESTION, NOT EXEMPTED FROM IT (#3414 final
  #      roborev, finding BB). The earlier contract emitted `ok "NOT-APPLICABLE"` on every
  #      non-Linux host unconditionally, so `--strict` CERTIFIED AN UNPINNED MAC:
  #      inapplicability of the PERSISTENCE STEP stood in for absence of the REQUIREMENT —
  #      this issue's own defect wearing a platform label, in code a full gate had passed.
  #
  #      BOTH DIRECTIONS are asserted, and the second is the one that matters, because it
  #      is the case the old code got wrong: a non-Linux host that sees an honoured pin
  #      earns the scoped `ok`; one that sees nothing must be NON-PASSING.
  shims_mac="$tmp/pin-shims-mac"; mkpinshims "$shims_mac" 1
  mk_stub "$shims_mac" uname 'echo Darwin'
  envf_ac="$tmp/pin-env-ac"; : >"$envf_ac"
  out_ac=$(runpin "$pinroot" "$shims_mac" "$envf_ac" HOME="$pin_home_plain")
  if out_has "$out_ac" -E '\[warn\].*gate-pin: UNMEASURED' \
     && out_has "$out_ac" 'no PAM-read system-wide file to compare it against' \
     && ! out_has "$out_ac" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: a non-Linux host with a session-visible pin is UNMEASURED, never certified"
  else
    bad "gate-pin: a non-Linux host was given a verdict its platform cannot support"
    printf '%s\n' "$out_ac" | grep -i 'gate-pin' | head -3
  fi

  # 11ac2. THE CASE THE OLD CONTRACT GOT WRONG: a non-Linux host whose fresh session sees
  #      NOTHING must be NON-PASSING. Under the previous code this host got an unconditional
  #      `ok`, so `--strict` exited 0 and certified a machine on which no gate is pinned.
  shims_mac_none="$tmp/pin-shims-mac-none"; mkpinshims "$shims_mac_none" -
  mk_stub "$shims_mac_none" uname 'echo Darwin'
  envf_ac2="$tmp/pin-env-ac2"; : >"$envf_ac2"
  out_ac2=$(runpin "$pinroot" "$shims_mac_none" "$envf_ac2" HOME="$pin_home_plain")
  if ! out_has "$out_ac2" -E '\[ok\].*gate-pin' \
     && out_has "$out_ac2" -E '\[warn\].*gate-pin'; then
    ok "gate-pin: an UNPINNED non-Linux host is also NON-PASSING — no platform exemption certifies it"
  else
    bad "gate-pin: an unpinned non-Linux host was certified (the finding-BB defect)"
    printf '%s\n' "$out_ac2" | grep -i 'gate-pin' | head -3
  fi

  # 11ad. ...and the scoping is on PLATFORM, not on "the file is missing". A LINUX box
  #      with no /etc/environment is a genuine anomaly and must stay non-passing; folding
  #      it into 11ac's branch would trade a false red for a false green. Same input as
  #      11ac (no env file) with the only difference being the platform.
  envf_ad="$tmp/pin-env-ad-missing"; rm -f "$envf_ad"
  out_ad=$(runpin "$pinroot" "$shims_one" "$envf_ad" HOME="$pin_home_plain")
  if ! out_has "$out_ad" -E '\[ok\].*gate-pin' \
     && ! out_has "$out_ad" -E 'NOT-APPLICABLE|VERIFIED-NO-SYSTEM-FILE'; then
    ok "gate-pin: a LINUX box with no system env file stays non-passing (scoped by platform, not by file absence)"
  else
    bad "gate-pin: a Linux anomaly was excused as a platform inapplicability"
    printf '%s\n' "$out_ad" | grep -i 'gate-pin' | head -2
  fi

  # 11ae. A QUOTED-BUT-CORRECT FILE MUST STILL VERIFY (issue #3414, lead correction).
  #      pam_env strips surrounding quotes, and quoting IS the convention in this file —
  #      /etc/environment on this fleet opens with PATH="/usr/local/sbin:...". Comparing
  #      the RAW string would make `CQLITE_GATE_MAX_CONCURRENCY="1"` parse as `"1"` while
  #      the session reports `1`, and a properly pinned box would get a non-passing
  #      verdict: red on correct input, produced by the fix for a false green. Both quote
  #      kinds, because pam was MEASURED to treat them alike rather than assumed to.
  for pin_q in '"1"' "'1'" '"1' '1'; do
    envf_ae="$tmp/pin-env-ae"; printf 'CQLITE_GATE_MAX_CONCURRENCY=%s\n' "$pin_q" >"$envf_ae"
    out_ae=$(runpin "$pinroot" "$shims_one" "$envf_ae" HOME="$pin_home_plain")
    if out_has "$out_ae" -E '\[ok\].*gate-pin: VERIFIED'; then
      ok "gate-pin: a file value written as $pin_q still VERIFIES (pam_env's quoting is read, not reinterpreted)"
    else
      bad "gate-pin: a correctly-pinned box written as $pin_q was reported non-passing"
      printf '%s\n' "$out_ae" | grep -i 'gate-pin' | head -2
    fi
  done

  # 11af. ...and de-quoting must not become normalisation. The INTERIOR is untouched, so a
  #      quoted `" 1 "` still mismatches a session reporting `1` — the gate discards ` 1 `
  #      (it matches *[!0-9]*), so calling them equal here would certify a value the gate
  #      rejects. This is the line between reading the file's format and reinterpreting
  #      its content, and it is the half a future "just trim it" refactor would erase.
  envf_af="$tmp/pin-env-af"; printf 'CQLITE_GATE_MAX_CONCURRENCY=" 1 "\n' >"$envf_af"
  out_af=$(runpin "$pinroot" "$shims_one" "$envf_af" HOME="$pin_home_plain")
  if ! out_has "$out_af" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: de-quoting leaves the interior alone — a quoted ' 1 ' still mismatches '1'"
  else
    bad "gate-pin: de-quoting slid into normalisation and certified a value the gate discards"
    printf '%s\n' "$out_af" | grep -i 'gate-pin' | head -2
  fi

  # 11ag. THE GATE ORACLE MUST NOT BE POLLUTABLE EITHER (issue #3414 roborev round 4).
  #      The oracle launches its own fresh non-interactive bash to ask the gate what it
  #      honours, and a non-interactive bash SOURCES $BASH_ENV — so the hole closed for
  #      the probe in round 2 was still open one call site over. Persisted value `abc`,
  #      a valid `1` injected through BASH_ENV: the oracle answers `1(pinned)` about a
  #      value it never saw, and the run certifies a box whose gates will stamp
  #      N(invalid). Two independent defences now: the scrub, and the requirement that
  #      the oracle's resolved N equal the value actually probed.
  pin_bashenv_v="$tmp/pin-bashenv-valid.sh"
  printf 'export CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$pin_bashenv_v"
  envf_ag="$tmp/pin-env-ag"; printf 'CQLITE_GATE_MAX_CONCURRENCY=abc\n' >"$envf_ag"
  shims_ag="$tmp/pin-shims-ag"; mkpinshims "$shims_ag" abc     # session genuinely sees abc
  out_ag=$(runpin "$pinroot" "$shims_ag" "$envf_ag" HOME="$pin_home_plain" \
    BASH_ENV="$pin_bashenv_v")
  if ! out_has "$out_ag" -E '\[ok\].*gate-pin' \
     && out_has "$out_ag" 'gate-pin: NOT-HONOURED'; then
    ok "gate-pin: a BASH_ENV-injected valid value cannot make the ORACLE certify an invalid pin"
  else
    bad "gate-pin: the gate oracle was polluted into certifying a value it never saw"
    printf '%s\n' "$out_ag" | grep -i 'gate-pin' | head -3
  fi

  # 11ah. ...and the SOURCE TOKEN ALONE is not the check: the oracle's resolved N must be
  #      the value we handed it. A `(pinned)` suffix says only that *something* was a
  #      valid pin. Asserted structurally as well as behaviourally, because a future edit
  #      could drop the N comparison while every behavioural case above still passes —
  #      the scrub alone would cover them.
  #
  #      THE NEEDLE MOVED WITH THE COMPARISON (roborev job 333). It used to be the literal
  #      `pin_gate_n" != "$pin_probe_seen`, which the canonical-decimal fix replaced — the
  #      comparison is still there and still compares the resolved N against the probed
  #      value, now with both sides normalised so the gate's own `08`->`8` does not read as
  #      drift. This guard CORRECTLY RED when the expression changed, which is what a
  #      structural guard is for; it is updated rather than deleted, and it still fails if
  #      the comparison is removed altogether. A structural needle is a claim about source
  #      text and decays exactly like a comment: when you change the expression, come here.
  if grep -Fq '[ "$(pin_canon_decimal "$pin_gate_n")" != "$(pin_canon_decimal "$pin_probe_seen")" ]' "$BOOTSTRAP" \
     && grep -q 'bounded 30 env -u BASH_ENV -u ENV' "$BOOTSTRAP"; then
    ok "gate-pin: the oracle is scrubbed AND its resolved N is compared, not just its source token"
  else
    bad "gate-pin: the oracle check lost its scrub or its resolved-value comparison"
  fi

  # 11ai. THE PROFILE APPEND MUST NOT MANUFACTURE A DIVERGENCE (issue #3414 roborev
  #      round 4). On a box deliberately pinned to 4, appending a hardcoded `export …=1`
  #      to the shell profile gives interactive shells 1 while every non-interactive one
  #      gets 4 — this issue's own subject, created by the tool that exists to remove it.
  #      It is SKIPPED rather than value-derived because PAM already delivers the
  #      system-wide value to interactive login shells, so the append could only override
  #      it, and a derived value would go stale on the next edit of the env file.
  pin_home_ai="$tmp/pin-home-ai"; mkdir -p "$pin_home_ai/.cargo"; : >"$pin_home_ai/.bashrc"
  envf_ai="$tmp/pin-env-ai"; printf 'CQLITE_GATE_MAX_CONCURRENCY=4\n' >"$envf_ai"
  shims_ai="$tmp/pin-shims-ai"; mkpinshims "$shims_ai" 4
  out_ai=$(runpin "$pinroot" "$shims_ai" "$envf_ai" HOME="$pin_home_ai" SHELL=/bin/bash --yes)
  if ! grep -q 'CQLITE_GATE_MAX_CONCURRENCY' "$pin_home_ai/.bashrc" \
     && out_has "$out_ai" 'not touching' \
     && out_has "$out_ai" -E '\[ok\].*gate-pin: VERIFIED'; then
    ok "gate-pin: --yes on a box pinned to 4 leaves the profile alone (no manufactured 1-vs-4 divergence)"
  else
    bad "gate-pin: the profile append manufactured a divergence with the system-wide value"
    printf '%s\n' "$out_ai" | grep -i 'gate-pin\|profile\|not touching' | head -3
    cat "$pin_home_ai/.bashrc"
  fi

  # 11aj. ...but where NO system-wide value can be established the append still happens,
  #      so 11ai cannot pass against a build that simply stopped writing profiles.
  #
  #      THE STATE HAS TO BE AN UNWRITABLE ENV FILE, NOT A MISSING ONE — and this case's
  #      premise was invalidated by THIS BRANCH. A missing file used to qualify, because
  #      bootstrap refused to create one, so PAM delivered nothing and the profile was the
  #      only lever left. `--fix-gate-pin`/`--yes` now CREATE it, so on a missing file
  #      bootstrap establishes the system-wide value and then CORRECTLY skips the profile
  #      (that skip is 11ai's whole point). Measured: the run emits `CREATED <file>
  #      carrying CQLITE_GATE_MAX_CONCURRENCY=1` and then `not touching <profile>`.
  #      A file that EXISTS but cannot be written is still genuinely unestablishable —
  #      `the append to <file> FAILED — the pin was NOT persisted` — so the profile is
  #      once again the only lever, and the case tests what it claims to.
  #
  #      (A box that merely STARTS empty is not this case either — `--yes` persists into
  #      it and the append is correctly skipped for the same reason. I discovered that by
  #      writing this case the obvious way first and watching it fail.)
  pin_home_aj="$tmp/pin-home-aj"; mkdir -p "$pin_home_aj/.cargo"; : >"$pin_home_aj/.bashrc"
  envf_aj="$tmp/pin-env-aj-ro"; printf '# no pin here\n' >"$envf_aj"; chmod 0444 "$envf_aj"
  out_aj=$(runpin "$pinroot" "$shims_none" "$envf_aj" HOME="$pin_home_aj" SHELL=/bin/bash --yes)
  chmod 0644 "$envf_aj" 2>/dev/null || true
  if grep -q '^export CQLITE_GATE_MAX_CONCURRENCY=1' "$pin_home_aj/.bashrc"; then
    ok "gate-pin: on a box where no system-wide value CAN be established the profile append still happens"
  else
    bad "gate-pin: the profile append stopped happening even with no system-wide value (11ai would pass vacuously)"
    # DUMP THE SCRIPT OUTPUT, not just the profile: this case previously printed only
    # `.bashrc`, so when the premise above went stale the failure could not show WHY the
    # append was skipped, and it was misdiagnosed as a flake in unrelated code. A failure
    # diagnostic must carry the evidence its own assertion turns on (#3758 nit 7).
    cat "$pin_home_aj/.bashrc"
    printf '%s\n' "$out_aj" | grep -iE 'CREATED|not touching|NOT persisted|gate-pin:' | head -8
    echo "  env-file: mode=$(stat -c %a "$envf_aj" 2>/dev/null) content=[$(cat "$envf_aj" 2>/dev/null | tr '\n' '|')]"
  fi

  # 11ak. THE SEAM MUST NOT STEER A ROOT-PRIVILEGED WRITE (issue #3414 roborev round 5,
  #      HIGH — the SIXTH instance of this issue's defect, and it was inside the safety
  #      guard). The old invariant tested `${#PIN_ROOT[@]} -gt 0`, i.e. "are we going
  #      through sudo" — a proxy for EFFECTIVE PRIVILEGE. Under EUID 0 the array is empty
  #      and `tee -a` is privileged anyway, so an env var could aim a root write at any
  #      absolute path. Asserted at the level the guard now uses: a real root invocation
  #      with the seam set must REFUSE, not write.
  # A sandbox for the ROOT invocations below. It must be cleaned with sudo, because root
  # writing here leaves root-owned files that the suite's own `rm -rf "$tmp"` trap (running
  # as the invoking user) cannot remove — a leak of the same shape as the one being fixed,
  # one directory over.
  pin_root_sandbox="$tmp/pin-root-sandbox"; mkdir -p "$pin_root_sandbox/.cargo"
  pin_seam_probe="$tmp/pin-seam-root-target"; rm -f "$pin_seam_probe"
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    out_ak=$(sudo -n env PIN_SANDBOX_ROOT="$PIN_SANDBOX_ROOT" PIN_SHARED_VIOLATIONS="$PIN_SHARED_VIOLATIONS" CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$pin_seam_probe" \
      HOME="$pin_root_sandbox" CARGO_HOME="$pin_root_sandbox/.cargo" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke --skip-push-probe --yes 2>&1)
    if out_has "$out_ak" 'gate-pin: SKIPPED' \
       && out_has "$out_ak" 'PRIVILEGED write' \
       && [ ! -e "$pin_seam_probe" ]; then
      ok "gate-pin: a ROOT run with the seam set refuses and writes nothing to the env-chosen path"
    else
      bad "gate-pin: the seam was honoured under root (a privileged write could be steered)"
      printf '%s\n' "$out_ak" | grep -i 'gate-pin' | head -2
      ls -l "$pin_seam_probe" 2>/dev/null
    fi
    sudo -n rm -f "$pin_seam_probe" 2>/dev/null || true
    sudo -n rm -rf "$pin_root_sandbox" 2>/dev/null || true; mkdir -p "$pin_root_sandbox/.cargo"
  else
    skip "gate-pin root-seam refusal (no passwordless sudo here to stage a real root invocation)"
  fi

  # 11al. ...and the guard keys on EFFECTIVE PRIVILEGE, not on the sudo-prefix array.
  #      Structural, because the behavioural case above needs root to run at all and a
  #      future edit could revert the predicate while every unprivileged case still passes.
  if grep -q 'PIN_EUID" = 0 \] || \[ -z "\$PIN_EUID" \] || \[ "\${#PIN_ROOT\[@\]}" -gt 0' "$BOOTSTRAP"; then
    ok "gate-pin: the privileged-write invariant tests EUID, not merely the presence of a sudo prefix"
  else
    bad "gate-pin: the privileged-write invariant is back to keying on PIN_ROOT alone"
  fi

  # 11am. THE PROBE SUBJECT IS THE ACCOUNT THAT WILL RUN GATES (roborev round 5). Under
  #      `sudo bash bootstrap`, `id -un` is root — the wrong subject, since a per-user
  #      ~/.pam_environment on the agent account diverges from root's session. An
  #      invoker sudo names but that does not resolve is UNMEASURED, never a silent fall
  #      back to answering about root.
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    out_am=$(sudo -n env PIN_SANDBOX_ROOT="$PIN_SANDBOX_ROOT" PIN_SHARED_VIOLATIONS="$PIN_SHARED_VIOLATIONS" SUDO_USER=cqlite-no-such-account-3414 \
      HOME="$pin_root_sandbox" CARGO_HOME="$pin_root_sandbox/.cargo" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke --skip-push-probe 2>&1)
    if out_has "$out_am" -E '\[warn\].*gate-pin: UNMEASURED' \
       && out_has "$out_am" -E 'does not resolve to an account|INCONSISTENT sudo metadata' \
       && ! out_has "$out_am" -E '\[ok\].*gate-pin'; then
      ok "gate-pin: an unresolvable sudo invoker is UNMEASURED, never answered about root instead"
    else
      bad "gate-pin: an unresolvable invoker fell back to probing the wrong user"
      printf '%s\n' "$out_am" | grep -i 'gate-pin' | head -2
    fi
    out_an=$(sudo -n env PIN_SANDBOX_ROOT="$PIN_SANDBOX_ROOT" PIN_SHARED_VIOLATIONS="$PIN_SHARED_VIOLATIONS" SUDO_USER="$(id -un)" \
      HOME="$pin_root_sandbox" CARGO_HOME="$pin_root_sandbox/.cargo" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke --skip-push-probe 2>&1)
    if out_has "$out_an" "the account that invoked sudo"; then
      ok "gate-pin: a resolvable sudo invoker becomes the probe subject, and the run says so"
    else
      bad "gate-pin: the sudo invoker was not adopted as the probe subject"
      printf '%s\n' "$out_an" | grep -i 'gate-pin\|subject' | head -2
    fi
  else
    skip "gate-pin sudo-invocation-mode cases (no passwordless sudo here)"
  fi

  # 11at. THE PRIVILEGE DECISION MUST NOT GO THROUGH A PATH-RESOLVED BINARY (issue #3414
  #      roborev round 8, HIGH). It used to call `id -u`, so a shadowed or merely MALFORMED
  #      `id` — a busybox variant, a broken PATH — made a ROOT invocation look
  #      unprivileged, and the seam then steered a root `tee -a` at an arbitrary absolute
  #      path: the round-5 High reopened through a different door. Bash's readonly $EUID
  #      cannot be shadowed and costs no fork.
  #
  #      Driven as a REAL root invocation with a lying `id` first on PATH, because that is
  #      the only way to distinguish "we read $EUID" from "we read a binary that agreed".
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    pin_liar="$tmp/pin-liar-bin"; mkdir -p "$pin_liar"
    printf '#!/usr/bin/env bash\necho 1000\n' >"$pin_liar/id"; chmod +x "$pin_liar/id"
    pin_liar_target="$tmp/pin-liar-target"; rm -f "$pin_liar_target"
    out_at=$(sudo -n env PIN_SANDBOX_ROOT="$PIN_SANDBOX_ROOT" PIN_SHARED_VIOLATIONS="$PIN_SHARED_VIOLATIONS" PATH="$pin_liar:$PATH" \
      CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$pin_liar_target" \
      HOME="$pin_root_sandbox" CARGO_HOME="$pin_root_sandbox/.cargo" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke --skip-push-probe --yes 2>&1)
    if out_has "$out_at" 'gate-pin: SKIPPED' && [ ! -e "$pin_liar_target" ]; then
      ok "gate-pin: a lying 'id' on PATH cannot make a ROOT run look unprivileged (the decision reads \$EUID)"
    else
      bad "gate-pin: a shadowed 'id' defeated the root guard — the seam steered a privileged write"
      printf '%s\n' "$out_at" | grep -i 'gate-pin' | head -2
      ls -l "$pin_liar_target" 2>/dev/null
    fi
    sudo -n rm -f "$pin_liar_target" 2>/dev/null || true

    # 11au. SUDO_USER MUST AGREE WITH SUDO_UID (roborev round 8). Trusting the NAME alone
    #      accepts stale metadata — `SUDO_UID=1000 SUDO_USER=root` would probe root and
    #      could report VERIFIED while the agent account differs, which is the
    #      wrong-subject defect the retarget exists to fix, wearing the retarget's clothes.
    out_au=$(sudo -n env PIN_SANDBOX_ROOT="$PIN_SANDBOX_ROOT" PIN_SHARED_VIOLATIONS="$PIN_SHARED_VIOLATIONS" SUDO_UID=1000 SUDO_USER=root \
      HOME="$pin_root_sandbox" CARGO_HOME="$pin_root_sandbox/.cargo" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke --skip-push-probe 2>&1)
    if out_has "$out_au" -E '\[warn\].*gate-pin: UNMEASURED' \
       && out_has "$out_au" 'INCONSISTENT sudo metadata' \
       && ! out_has "$out_au" -E '\[ok\].*gate-pin'; then
      ok "gate-pin: SUDO_USER disagreeing with SUDO_UID is UNMEASURED, not a probe of the wrong account"
    else
      bad "gate-pin: inconsistent sudo metadata was trusted"
      printf '%s\n' "$out_au" | grep -i 'gate-pin' | head -2
    fi

    # 11av. ...and root invoking sudo (SUDO_UID=0) tells us nothing about a gate's account.
    out_av=$(sudo -n env PIN_SANDBOX_ROOT="$PIN_SANDBOX_ROOT" PIN_SHARED_VIOLATIONS="$PIN_SHARED_VIOLATIONS" SUDO_UID=0 SUDO_USER=root \
      HOME="$pin_root_sandbox" CARGO_HOME="$pin_root_sandbox/.cargo" \
      "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 120 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" \
        --skip-smoke --skip-push-probe 2>&1)
    if out_has "$out_av" -E '\[warn\].*gate-pin: UNMEASURED' \
       && out_has "$out_av" 'sudo was invoked BY root'; then
      ok "gate-pin: SUDO_UID=0 is UNMEASURED — root invoking sudo says nothing about a gate's account"
    else
      bad "gate-pin: SUDO_UID=0 was treated as a usable probe subject"
      printf '%s\n' "$out_av" | grep -i 'gate-pin' | head -2
    fi
    sudo -n rm -rf "$pin_root_sandbox" 2>/dev/null || true; mkdir -p "$pin_root_sandbox/.cargo"
  else
    skip "gate-pin privilege-source and sudo-metadata cases (no passwordless sudo here)"
  fi

  # 11aw. UNREADABLE IS NOT ABSENT for the profile append (roborev round 8). The Q fix
  #      keyed on a value having been FOUND, so an unreadable or unparseable env file fell
  #      through to appending the hardcoded `=1` — and if that file already pins 4, we
  #      silently create the divergence Q existed to prevent. An unmeasurable state must
  #      not inherit the permissive branch.
  if [ "$(id -u)" = 0 ]; then
    skip "gate-pin unreadable-file profile-append case (running as root: 0000 is still readable)"
  else
    pin_home_aw="$tmp/pin-home-aw"; mkdir -p "$pin_home_aw/.cargo"; : >"$pin_home_aw/.bashrc"
    envf_aw="$tmp/pin-env-aw"; printf 'CQLITE_GATE_MAX_CONCURRENCY=4\n' >"$envf_aw"; chmod 0000 "$envf_aw"
    out_aw=$(runpin "$pinroot" "$shims_none" "$envf_aw" HOME="$pin_home_aw" SHELL=/bin/bash --yes)
    chmod 0644 "$envf_aw"
    if ! grep -q 'CQLITE_GATE_MAX_CONCURRENCY' "$pin_home_aw/.bashrc" \
       && out_has "$out_aw" 'could not determine what'; then
      ok "gate-pin: an UNREADABLE env file does not get the hardcoded profile export (unreadable is not absent)"
    else
      bad "gate-pin: an unreadable env file fell through to the append, recreating the divergence Q removed"
      printf '%s\n' "$out_aw" | grep -i 'not touching\|could not determine' | head -2
      cat "$pin_home_aw/.bashrc"
    fi
  fi

  # 11ax. NO TIMEOUT UTILITY => NO PROBE RUNS AT ALL (issue #3414 roborev round 10).
  #      `bounded` degrades to running the command DIRECTLY when neither timeout nor
  #      gtimeout exists, and both sudo probes used to execute ABOVE the no-timeout guard —
  #      so a stalled sudo/PAM/NSS lookup hung bootstrap indefinitely while the code
  #      CLAIMED it refuses unbounded session probing. Asserting the verdict alone would
  #      not have caught it: the verdict was already correct, and only the ORDER was wrong.
  #      So this counts sudo invocations with a recording shim — the fact that distinguishes
  #      "refused" from "ran it anyway and then said it refused".
  pin_nt="$tmp/pin-no-timeout"; mkdir -p "$pin_nt"
  for pin_t in bash env grep sed awk head tail tr cut wc stat mktemp dirname basename cat \
               cp mv rm mkdir chmod ln find date id uname nproc tee sort xargs expr sleep; do
    pin_tp=$(type -P "$pin_t" 2>/dev/null) || continue
    [ -n "$pin_tp" ] && ln -sf "$pin_tp" "$pin_nt/$pin_t" 2>/dev/null || true
  done
  pin_nt_trip="$tmp/pin-no-timeout-sudo.log"; : >"$pin_nt_trip"
  mk_stub "$pin_nt" sudo "echo \"sudo \$*\" >>\"$pin_nt_trip\"; exit 0"
  envf_ax="$tmp/pin-env-ax"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_ax"
  # NOT through runpin: that wraps the invocation in `timeout`, which this case has
  # deliberately removed from PATH, so bootstrap would never launch and the assertion would
  # fail for the harness's reason rather than the code's. The outer timeout is invoked by
  # ABSOLUTE path so the bound on the test itself survives while bootstrap's OWN PATH still
  # has none — which is the condition under test.
  pin_ax_timeout=$(command -v timeout 2>/dev/null || true)
  out_ax=$(env PATH="$pin_nt" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo"     CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$envf_ax"     ${pin_ax_timeout:+"$pin_ax_timeout" -s KILL 120}     "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" --skip-smoke --skip-push-probe 2>&1)
  if out_has "$out_ax" -E '\[warn\].*gate-pin: UNMEASURED' \
     && out_has "$out_ax" 'NOTHING was probed' \
     && [ ! -s "$pin_nt_trip" ]; then
    ok "gate-pin: with no timeout utility the section refuses AND invokes sudo zero times"
  else
    bad "gate-pin: a probe ran unbounded (or the refusal was not reported): $(wc -l <"$pin_nt_trip") sudo call(s)"
    printf '%s\n' "$out_ax" | grep -i 'gate-pin' | head -2
    cat "$pin_nt_trip"
  fi

  # 11ay. A BOX THAT PERMITS THE SELF-SESSION BUT NOT UNRESTRICTED ROOT MUST STILL BE
  #      MEASURED (roborev round 10). Probe capability was gated on `sudo -n true`
  #      succeeding as ROOT, but that asks "may I run ANYTHING as root" while the probe
  #      needs only "may I open a session as MYSELF". A narrowly-scoped sudoers rule — or a
  #      box already correctly pinned — was reported sudo-needs-password and failed
  #      --strict on a legitimately configured machine.
  pin_ab="$tmp/pin-shims-selfonly"; mkpinshims "$pin_ab" 1
  mk_stub "$pin_ab" sudo 'args="$*"
case "$args" in
  "-n true")  exit 1 ;;                 # root execution DENIED
esac
while [ "${1:-}" = "-n" ]; do shift; done
if [ "${1:-}" = "-u" ]; then shift 2; fi
exec env CQLITE_GATE_MAX_CONCURRENCY=1 "$@"'
  envf_ay="$tmp/pin-env-ay"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_ay"
  out_ay=$(runpin "$pinroot" "$pin_ab" "$envf_ay" HOME="$pin_home_plain")
  if out_has "$out_ay" -E '\[ok\].*gate-pin: VERIFIED' \
     && ! out_has "$out_ay" 'needs a password'; then
    ok "gate-pin: root execution denied but the self-session permitted still MEASURES (no false red)"
  else
    bad "gate-pin: a box permitting the self-session was failed for lacking unrestricted root"
    printf '%s\n' "$out_ay" | grep -i 'gate-pin' | head -2
  fi

  # 11k. The test seam is FAIL-CLOSED and has NO production fallback: set without its
  #      marker, or relative, it SKIPS the section rather than silently persisting to
  #      the real /etc/environment (the #3249 lesson — a seam that degrades to the
  #      production path certifies the production path by accident).
  # 11ba. A MISSING env file is CREATED under authorisation, and NOT created without it
  #      (#3414 final roborev). Before this, --fix-gate-pin declined to create the file, so a
  #      MINIMAL Linux install — where /etc/environment does not ship — could never be
  #      repaired and failed --strict onboarding forever: a repair flag that cannot repair the
  #      one case it exists for. BOTH directions are asserted because a repair that fires
  #      unasked is as wrong as one that never fires, and the create path was previously
  #      verified BY HAND only, which is the gap this case closes.
  envf_ba="$tmp/pin-env-ba"; rm -f "$envf_ba"
  out_ba=$(env PATH="$shims_one" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo" \
    CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$envf_ba" \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" --skip-smoke --fix-gate-pin 2>&1)
  ba_created=0; [ -s "$envf_ba" ] && grep -q '^CQLITE_GATE_MAX_CONCURRENCY=1$' "$envf_ba" && ba_created=1
  envf_bb="$tmp/pin-env-bb"; rm -f "$envf_bb"
  out_bb=$(env PATH="$shims_one" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo" \
    CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$envf_bb" \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
  if [ "$ba_created" = 1 ] \
     && out_has "$out_ba" 'CREATED' \
     && [ ! -e "$envf_bb" ] \
     && out_has "$out_bb" 'will be CREATED'; then
    ok "gate-pin: a MISSING env file is created under --fix-gate-pin (with the pin in it), and is NOT created without authorisation — which SAYS it would be"
  else
    bad "gate-pin: the create path is wrong (created=$ba_created, unauthorised-file-exists=$([ -e "$envf_bb" ] && echo yes || echo no))"
    printf '%s\n' "$out_ba" | grep -iE 'CREATED|gate-pin' | head -2
    printf '%s\n' "$out_bb" | grep -iE 'CREATED|gate-pin' | head -2
  fi

  # 11bc. THE CREATE IS TWO STEPS, AND ITS FAILURE MUST NOT LEAVE A POPULATED FILE
  #      (roborev job 311, Low). Content-then-mode: a `tee` that succeeded followed by a
  #      mode that could not be established took the failure branch, reported "the pin was
  #      NOT persisted", and left the pin line on disk. The NEXT run then reads a present
  #      CQLITE_GATE_MAX_CONCURRENCY line, treats the pin as persisted, and never repairs
  #      the mode — one run's reported failure becoming the next run's silent success at a
  #      permission nothing chose.
  #
  #      RE-ANCHORED AGAIN (roborev job 329): the post-`ln` read-back-and-rollback this case
  #      used to drive HAS BEEN DELETED, because removing the destination by pathname was a
  #      destructive race (`ln` proves the inode is ours at LINK time, not at REMOVE time). The
  #      mode is now established and verified on the TEMP *before* linking, so the observable
  #      changed: on a mode failure the destination is NEVER CREATED rather than created-then-
  #      removed. The assertion is the same shape — destination absent + failure reported — for
  #      a materially better reason.
  #
  #      DRIVEN THROUGH ITS ORACLE, and the reason is the third instance of premise-staleness
  #      in this block. The first version stubbed `chmod` to fail and forced `umask 077` so
  #      `tee` created 0600. The job-314 fix then made the create atomic — one privileged
  #      `bash -c` with `umask 022` and `set -C` — which establishes 0644 AT CREATION, so a
  #      failing chmod can no longer produce a wrong mode and that route to the state is
  #      closed. The rollback branch is still LIVE in production (a default ACL or an odd
  #      filesystem can yield a mode the readback rejects), so it is exercised by making the
  #      readback itself report a non-0644 mode. Stubbing `stat` is only possible because
  #      mk_stub now removes the hermetic symlink first — before that, this stub would have
  #      silently not installed and the case would have passed against the real `stat`.
  shims_badstat="$tmp/pin-shims-badstat"; mkpinshims "$shims_badstat" 1
  mk_stub "$shims_badstat" stat 'echo 600'
  envf_bc="$tmp/pin-env-bc"; rm -f "$envf_bc"
  out_bc=$(runpin "$pinroot" "$shims_badstat" "$envf_bc" HOME="$pin_home_plain" --fix-gate-pin)
  if [ ! -e "$envf_bc" ] \
     && out_has "$out_bc" 'the pin was NOT persisted' \
     && out_has "$out_bc" 'never created'; then
    ok "gate-pin: a create whose mode cannot be established on the STAGED file never links it, so the destination is untouched (nothing to roll back, hence no rollback race)"
  else
    bad "gate-pin: the failed create left a residue, or did not report rolling it back"
    printf '%s\n' "$out_bc" | grep -iE 'gate-pin|CREATED|REMOVED|persisted' | head -4
    echo "  file-exists=$([ -e "$envf_bc" ] && echo yes || echo no) content=[$(cat "$envf_bc" 2>/dev/null | tr '\n' '|')]"
  fi

  # 11bi. A PRE-LINK FAILURE MUST NOT TOUCH THE DESTINATION (roborev job 328, Medium — a
  #      DESTRUCTIVE defect I introduced with the temp+`ln` rewrite). Exit 5 means the STAGING
  #      write failed, so `ln` never ran and the destination was NEVER ours. The branch used to
  #      `rm -f "$PIN_ENV_FILE"` unconditionally, so a provisioner that created
  #      /etc/environment during our write had ITS file deleted by our "cleanup" — defeating
  #      the exact no-clobber guarantee the rewrite exists for.
  #
  #      DRIVEN BY A READ-ONLY DIRECTORY, which is the one schedulable way to reach exit 5 from
  #      the CLI: the destination is absent (so the create branch IS entered) and the temp
  #      write then fails with EACCES.
  #
  #      WHAT THIS CASE CANNOT DO, stated rather than implied: it cannot schedule the RACE
  #      itself. Entering the create branch requires the destination to be absent, and a peer
  #      creating it mid-write is not something a test can time. So this asserts the reachable
  #      half — the failure is reported and NOTHING is written or removed at the destination —
  #      and the unreachable half rests on the code carrying no destination-`rm` on that path
  #      at all, which is a structural property of a branch that now has none.
  pin_ro_dir="$tmp/pin-ro-dir"; rm -rf "$pin_ro_dir"; mkdir -p "$pin_ro_dir"
  envf_bi="$pin_ro_dir/env"; rm -f "$envf_bi"
  chmod 0555 "$pin_ro_dir"
  out_bi=$(runpin "$pinroot" "$shims_one" "$envf_bi" HOME="$pin_home_plain" --fix-gate-pin)
  chmod 0755 "$pin_ro_dir" 2>/dev/null || true
  if out_has "$out_bi" 'the pin was NOT persisted' \
     && out_has "$out_bi" 'before .* was linked' \
     && [ ! -e "$envf_bi" ]; then
    ok "gate-pin: a staging-write failure reports itself and writes NOTHING at the destination (no destructive cleanup of a path that was never ours)"
  else
    bad "gate-pin: the pre-link failure path did not report cleanly, or touched the destination"
    printf '%s\n' "$out_bi" | grep -iE 'gate-pin|staging|persisted|CREATED' | head -4
    echo "  dest exists=$([ -e "$envf_bi" ] && echo yes || echo no)"
  fi

  # 11bh. AN INVARIANT GUARD, EXPLICITLY *NOT* A DISCRIMINATOR FOR THE LOCK. Two concurrent
  #      runs must leave exactly ONE CQLITE_GATE_MAX_CONCURRENCY line, because pam_env
  #      resolves duplicates by taking the last.
  #
  #      MEASURED, and stated because the first version of this comment claimed the opposite:
  #      this case passes against the pre-lock bootstrap TOO (RED run: PASS=199 FAIL=0, with
  #      the sanity checks confirming 11bh present and `pin_append_env_file` absent). Two
  #      runs started together do enough work before their append that one finishes before
  #      the other reads, so the unlocked implementation serialises by luck and the duplicate
  #      state never materialises. The case therefore pins the INVARIANT — useful against a
  #      future change that reintroduces duplicates in a way that does race — and evidences
  #      NOTHING about job 316's lock.
  #
  #      That makes THREE consecutive failed attempts to discriminate a concurrency fix in
  #      this script (11bg's umask, this case's duplicate count, and the O_EXCL race itself).
  #      The generalisation is worth more than the cases: a fix that narrows a WINDOW is not
  #      observable from a harness that cannot schedule the window, and a test that passes
  #      either way is indistinguishable from coverage until you run it against the defect.
  #      Both concurrency fixes are DECLARED UNCOVERED — see the note at 11bg.
  envf_bh="$tmp/pin-env-bh"; : >"$envf_bh"
  runpin "$pinroot" "$shims_one" "$envf_bh" HOME="$tmp/pin-home-bh1" --fix-gate-pin >/dev/null 2>&1 &
  pin_bh1=$!
  runpin "$pinroot" "$shims_one" "$envf_bh" HOME="$tmp/pin-home-bh2" --fix-gate-pin >/dev/null 2>&1 &
  pin_bh2=$!
  wait "$pin_bh1" 2>/dev/null; wait "$pin_bh2" 2>/dev/null
  bh_lines=$(grep -cE '^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=' "$envf_bh" 2>/dev/null)
  if [ "$bh_lines" = 1 ]; then
    ok "gate-pin: two concurrent runs leave exactly ONE CQLITE_GATE_MAX_CONCURRENCY line (invariant guard; passes with and without the lock)"
  else
    bad "gate-pin: concurrent runs left $bh_lines CQLITE_GATE_MAX_CONCURRENCY lines (pam_env would take the last)"
    cat "$envf_bh"
  fi

  # 11bg WAS HERE AND WAS DELETED, because it passed against the defect it was written for
  #      — the vacuous-case class this block keeps finding, produced this time by me.
  #      It asserted that a create under `umask 077` still comes out 0644, intending to pin
  #      the job-314 fix's "mode established AT CREATION" half. RED-verified against the
  #      pre-fix bootstrap: PASS=199 FAIL=0 — it did not discriminate. The old code was
  #      `tee` (0600 under that umask) followed by a real `chmod 0644` that SUCCEEDS, so both
  #      implementations end at 0644. The difference exists only in the window BETWEEN the
  #      two steps, which is exactly as unobservable from the CLI as the O_EXCL race.
  #
  #      Both halves of that fix are therefore DECLARED UNCOVERED rather than faked: an
  #      implementation-neutral test would have to inject a file between the caller's
  #      `[ ! -e ]` test and the write, and the two implementations share NO step there — so
  #      any test that could fire would be pinning the implementation, not the property. The
  #      remaining option is a source grep for `set -C`, which is nit 5's antipattern
  #      (#3758). The reachable halves ARE covered: 11ba (create happens / does not without
  #      authorisation) and 11bc (rollback when the mode cannot be confirmed).

  # 11bd. A REMEDY MUST BE CHOSEN BY THE FACT THAT DISCRIMINATES IT (roborev job 311, Low).
  #      The verdict `case` dispatches on the GATE's classification of what the SESSION saw,
  #      which says nothing about WHERE that value came from. So a box whose system file is
  #      CORRECT (`=1`) but whose session is overridden by a per-user `abc` reached the
  #      not-honoured branch and was told to "fix the VALUE in <file>" — a file that is
  #      already right. The operator finds nothing wrong and re-runs into the same verdict.
  #      This is #3414's own subject one level down: a remedy keyed on a verdict rather than
  #      on the fact that decides between two remedies.
  envf_bd="$tmp/pin-env-bd"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$envf_bd"
  shims_bd="$tmp/pin-shims-bd"; mkpinshims "$shims_bd" abc
  out_bd=$(runpin "$pinroot" "$shims_bd" "$envf_bd" HOME="$pin_home_plain")
  if out_has "$out_bd" 'gate-pin: NOT-HONOURED' \
     && out_has "$out_bd" 'is OVERRIDING it' \
     && ! out_has "$out_bd" 'fix the VALUE (not the presence)'; then
    ok "gate-pin: a bad SESSION value over a CORRECT system file is diagnosed as an override, not as a bad file"
  else
    bad "gate-pin: the override case was handed the edit-the-system-file remedy"
    printf '%s\n' "$out_bd" | grep -iE 'gate-pin:|OVERRIDING|fix the VALUE' | head -4
  fi

  # 11be. THE NEGATIVE TWIN, so 11bd cannot pass by suppressing the remedy outright: where
  #      the system file REALLY holds the bad value, "edit the VALUE in that file" is the
  #      correct advice and must survive.
  envf_be="$tmp/pin-env-be"; printf 'CQLITE_GATE_MAX_CONCURRENCY=abc\n' >"$envf_be"
  out_be=$(runpin "$pinroot" "$shims_bd" "$envf_be" HOME="$pin_home_plain")
  if out_has "$out_be" 'gate-pin: NOT-HONOURED' \
     && out_has "$out_be" 'fix the VALUE (not the presence)' \
     && ! out_has "$out_be" 'is OVERRIDING it'; then
    ok "gate-pin: where the system file really holds the bad value, the edit-the-file remedy survives"
  else
    bad "gate-pin: the genuine bad-file case lost its remedy to the override branch"
    printf '%s\n' "$out_be" | grep -iE 'gate-pin:|OVERRIDING|fix the VALUE' | head -4
  fi

  # 11bf. VERIFIED DISCLOSES WHAT IT MEASURED (roborev job 311, Medium). Matching values do
  #      not prove the session got the value FROM the system file: a box that also sets it
  #      from a sudoers env_file or ~/.pam_environment to the same value would read VERIFIED
  #      with an /etc/environment no PAM stack loads. That cannot be settled without either
  #      inspecting PAM config (deleted in round 7 — config inspection standing in for
  #      runtime behaviour) or perturbing a live system file, so the CLAIM is scoped in the
  #      output instead of being overstated. Asserted because an unstated limit is
  #      indistinguishable from one nobody noticed.
  envf_bf="$tmp/pin-env-bf"; : >"$envf_bf"
  shims_bf="$tmp/pin-shims-bf"; mkpinshims "$shims_bf" "file:$envf_bf"
  out_bf=$(runpin "$pinroot" "$shims_bf" "$envf_bf" HOME="$pin_home_plain" --fix-gate-pin)
  if out_has "$out_bf" 'gate-pin: VERIFIED' \
     && out_has "$out_bf" 'Agreement is measured; provenance is not'; then
    ok "gate-pin: VERIFIED states that it measured agreement and NOT provenance (the alternate-source residual is disclosed with the verdict)"
  else
    bad "gate-pin: VERIFIED did not disclose the agreement-vs-provenance limit"
    printf '%s\n' "$out_bf" | grep -iE 'VERIFIED|provenance|scope:' | head -4
  fi

  envf_k="$tmp/pin-env-k"; : >"$envf_k"
  # `-u CQLITE_BOOTSTRAP_TEST_MODE` is the point of this half: the marker is exported
  # suite-wide for host safety, and the case is about a seam set WITHOUT it.
  out_k=$(env -u CQLITE_BOOTSTRAP_TEST_MODE \
    PATH="$shims_one" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo" \
    CQLITE_BOOTSTRAP_ENV_FILE="$envf_k" \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
  out_k2=$(env PATH="$shims_one" HOME="$pin_home_plain" CARGO_HOME="$tmp/pin-cargo" \
    CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="relative/env" \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$pinroot/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
  if out_has "$out_k" 'gate-pin: SKIPPED' \
     && out_has "$out_k2" 'gate-pin: SKIPPED' \
     && ! out_has "$out_k" -E '\[ok\].*gate-pin'; then
    ok "gate-pin: the test seam is fail-closed (no marker / relative path => SKIPPED, no fallback)"
  else
    bad "gate-pin: the test seam was honoured without its marker, or accepted a relative path"
    printf '%s\n' "$out_k" | grep -i 'gate-pin' | head -2
    printf '%s\n' "$out_k2" | grep -i 'gate-pin' | head -2
  fi
fi

# 11v. THE COUPLING THAT MOTIVATES THE FLAG. --fix-gate-pin exists because a launched box's
#      ONLY bootstrap invocation is .agent-ami/profile.yaml's verify.run. If that command
#      string ever loses the flag, every new instance silently arrives unpinned again and
#      nothing else in this suite would notice — the flag would still work perfectly and be
#      called by nobody.
pin_profile="$SCRIPT_DIR/../../.agent-ami/profile.yaml"
if [ ! -r "$pin_profile" ]; then
  bad "gate-pin: .agent-ami/profile.yaml is not readable — cannot check verify.run carries --fix-gate-pin"
elif out_has "$(grep -E '^[[:space:]]*run:.*bootstrap-agent-machine\.sh' "$pin_profile")" -- '--fix-gate-pin'; then
  ok "gate-pin: .agent-ami/profile.yaml's verify.run persists the pin on a launched box (--fix-gate-pin)"
else
  bad "gate-pin: verify.run no longer passes --fix-gate-pin — launched boxes will arrive UNPINNED"
  grep -nE '^[[:space:]]*run:.*bootstrap-agent-machine\.sh' "$pin_profile" | head -2
fi

# --- 12b. SECTION 5b2: the sccache cache-size cap (issue #3727) ------------------------
# The twin of the 11* block above, one variable over, and it must be a SEPARATE block for
# the reason section 5b2 is a separate section: the two answer different questions with
# different remedies (`sccache --stop-server` vs editing a value), and folding them would
# make each case's subject ambiguous.
#
# THE STUB IS AN sccache, NOT A SEAM (#3312's corollary, as this file applies it elsewhere):
# a CQLITE_BOOTSTRAP_SCCACHE_BIN variable would be one more thing a real invoker can set, so
# the cases substitute the ARTIFACT on PATH. The stub models the two shapes MEASURED on
# sccache 0.17.0 — an ISOLATED client (SCCACHE_SERVER_PORT set: no server, so the cap comes
# from the client's own SCCACHE_CACHE_SIZE and cache_size is null) and a PRODUCTION read (a
# running server: a fixed cap and an integer cache_size) — and it RECORDS ITS ARGV, so a case
# can assert what was never invoked.
#
# scc_stub_body (the shared sccache stub) is defined with the other PATH-stub helpers near
# the top of this file, because mk_push_bin needs it too.

# mksccshims <dir> <session-value> [no-sccache]
#   <session-value>: `-` = a fresh session sees NOTHING (nothing persisted);
#                    `file:<path>` = the PAM stand-in reads the value out of that env file at
#                    session-creation time, exactly as pam_env does — so a write performed
#                    earlier in the SAME bootstrap run is visible to the probe;
#                    anything else = inject that literal.
#   The pin (CQLITE_GATE_MAX_CONCURRENCY=1) is injected alongside so section 5b reaches its
#   own VERIFIED and its warnings cannot be mistaken for this section's.
mksccshims() {
  local dir="$1" val="$2" mode="${3:-}" t bin
  mk_hermetic_bin "$dir"
  for t in id tee true; do
    bin=$(type -P "$t" 2>/dev/null) || continue
    [ -n "$bin" ] && ln -sf "$bin" "$dir/$t" 2>/dev/null || true
  done
  [ "$mode" = no-sccache ] || mk_stub "$dir" sccache "$scc_stub_body"
  # ONE stub for both session types. It recognises `-i` (the LOGIN form section 5b2 added in round
  # 3) and, by default, answers identically for both — an agreeing box. Three env knobs, read at
  # call time so no call site has to change, drive the disagreeing cases:
  #   SCC_SHIM_LOGIN_VALUE  the value the LOGIN form reports (the profile's value, which on this
  #                         fleet OVERRIDES /etc/environment for a login shell)
  #   SCC_SHIM_LOGIN_DIR    the SCCACHE_DIR the LOGIN form reports (a routing conflict)
  #   SCC_SHIM_LOGIN_FAIL   non-empty: the login form fails, so it cannot be measured
  # Leaving `-i` unhandled would make the stub run `env VAR=x -i bash …`, where GNU env stops
  # option parsing at the first assignment and takes `-i` as the COMMAND (rc 127) — a failure that
  # reads as an unmeasurable login session rather than as a broken stub.
  local scc_pre='[ -n "${SCC_SHIM_SUDO_LOG:-}" ] && echo "sudo $*" >> "$SCC_SHIM_SUDO_LOG"
[ -n "${SCC_SHIM_ENV_LOG:-}" ] && case "$*" in *cqlite-scc-probe*|*"command -v sccache"*|*--show-stats*|*--start-server*|*--stop-server*) { echo "census-ran" >> "$SCC_SHIM_ENV_LOG"; for scc_en in $(compgen -e 2>/dev/null || true); do case "$scc_en" in SCCACHE_*) echo "scc:$scc_en=${!scc_en}" >> "$SCC_SHIM_ENV_LOG" ;; esac; done; echo "marker:${CQLITE_SCRUB_MARKER-<unset>}" >> "$SCC_SHIM_ENV_LOG"; } ;; esac
scc_login=0
while [ "${1:-}" = "-n" ]; do shift; done
if [ "${1:-}" = "-u" ]; then shift 2; fi
if [ "${1:-}" = "-i" ]; then scc_login=1; shift; fi
if [ "$scc_login" = 1 ] && [ -n "${SCC_SHIM_LOGIN_FAIL:-}" ]; then exit 1; fi
scc_extra=()
# A PAM session gets its ROUTING from the same place it gets the cap (on this fleet SCCACHE_DIR is in
# /etc/environment), and section 5b2 SCRUBS the invoking SCCACHE_DIR before opening the session so that
# routing cannot be reported as the session own routing. A stub that injected only the cap would
# therefore report an EMPTY SCCACHE_DIR for both sessions while the invoking context has one — a
# routing disagreement manufactured by the harness. SCC_STUB_SESSION_DIR is passed through a
# non-SCCACHE name precisely because the SCCACHE_* names are the ones being scrubbed.
[ -n "${SCC_STUB_SESSION_DIR:-}" ] && scc_extra+=("SCCACHE_DIR=$SCC_STUB_SESSION_DIR")
if [ "$scc_login" = 1 ]; then
  [ -n "${SCC_SHIM_LOGIN_DIR+set}" ] && scc_extra+=("SCCACHE_DIR=$SCC_SHIM_LOGIN_DIR")
  # SCC_SHIM_LOGIN_BIN prepends a directory to the LOGIN form PATH, so `command -v sccache` inside
  # that context resolves a DIFFERENT binary — the shape of #3727 round 6 f1, where two launch
  # contexts would run different sccache installs.
  [ -n "${SCC_SHIM_LOGIN_BIN:-}" ] && scc_extra+=("PATH=$SCC_SHIM_LOGIN_BIN:$PATH")
elif [ -n "${SCC_SHIM_NONLOGIN_NOBIN:-}" ]; then
  # The NON-LOGIN context resolves NO sccache: the `cargo install` shape, where the binary sits in
  # the user Cargo bin directory and sudo replaces PATH with secure_path. An ABSOLUTE path still
  # executes, which is why the server reads keep working — only `command -v` comes back empty.
  #
  # SCOPED TO THE RESOLUTION PROBE ONLY. Applying it to every non-login sudo call also broke
  # `sudo -n -u <self> true` (the PRIVILEGE probe), which then reported sudo-runas-denied and made
  # the case fail for a reason that had nothing to do with the binary — a harness artifact wearing
  # the verdict it was meant to test.
  case "$*" in
    *"command -v sccache"*) scc_extra+=("PATH=/nonexistent") ;;
  esac
fi'
  if [ "${val#file:}" != "$val" ]; then
    mk_stub "$dir" sudo "$scc_pre
pam_file='${val#file:}'
scc_val=\"\"
if [ -f \"\$pam_file\" ] && grep -Eq '^[[:space:]]*SCCACHE_CACHE_SIZE[[:space:]]*=' \"\$pam_file\"; then
  scc_val=\$(sed -n 's/^[[:space:]]*SCCACHE_CACHE_SIZE[[:space:]]*=//p' \"\$pam_file\" | tail -1)
  if [ \"\$scc_login\" = 1 ] && [ -n \"\${SCC_SHIM_LOGIN_VALUE+set}\" ]; then scc_val=\"\$SCC_SHIM_LOGIN_VALUE\"; fi
  exec env CQLITE_GATE_MAX_CONCURRENCY=1 SCCACHE_CACHE_SIZE=\"\$scc_val\" \${scc_extra[@]+\"\${scc_extra[@]}\"} \"\$@\"
fi
if [ \"\$scc_login\" = 1 ] && [ -n \"\${SCC_SHIM_LOGIN_VALUE+set}\" ]; then
  exec env CQLITE_GATE_MAX_CONCURRENCY=1 SCCACHE_CACHE_SIZE=\"\$SCC_SHIM_LOGIN_VALUE\" \${scc_extra[@]+\"\${scc_extra[@]}\"} \"\$@\"
fi
exec env CQLITE_GATE_MAX_CONCURRENCY=1 \${scc_extra[@]+\"\${scc_extra[@]}\"} \"\$@\""
  elif [ "$val" = "-" ]; then
    mk_stub "$dir" sudo "$scc_pre
if [ \"\$scc_login\" = 1 ] && [ -n \"\${SCC_SHIM_LOGIN_VALUE+set}\" ]; then
  exec env CQLITE_GATE_MAX_CONCURRENCY=1 SCCACHE_CACHE_SIZE=\"\$SCC_SHIM_LOGIN_VALUE\" \${scc_extra[@]+\"\${scc_extra[@]}\"} \"\$@\"
fi
exec env CQLITE_GATE_MAX_CONCURRENCY=1 \${scc_extra[@]+\"\${scc_extra[@]}\"} \"\$@\""
  else
    mk_stub "$dir" sudo "$scc_pre
scc_val=$val
if [ \"\$scc_login\" = 1 ] && [ -n \"\${SCC_SHIM_LOGIN_VALUE+set}\" ]; then scc_val=\"\$SCC_SHIM_LOGIN_VALUE\"; fi
exec env CQLITE_GATE_MAX_CONCURRENCY=1 SCCACHE_CACHE_SIZE=\"\$scc_val\" \${scc_extra[@]+\"\${scc_extra[@]}\"} \"\$@\""
  fi
}

# runscc <script> <shim-dir> <env-file> [NAME=VALUE...] [--flag...] — one bootstrap run.
# BOTH variables are scrubbed from every call: this suite runs on fleet boxes that export
# them, and an inherited value would otherwise decide the verdict instead of the case's input.
#
# THE INVOKING ENVIRONMENT IS NO LONGER COMPARED (#3727, lead retraction after roborev round 9):
# under `sudo bash bootstrap` it is root's, so comparing it false-failed correct boxes. 5b2 now
# compares the two SESSION contexts only. The `SCCACHE_CACHE_SIZE=<v>` arguments the cases pass are
# therefore no longer load-bearing for the verdict — they are kept because they make each case's
# intended invoking environment explicit rather than inherited from whatever box runs the suite, and
# because `env` applies its `-u` options before the NAME=VALUE assignments so passing it is still
# well-defined. 12b-d2 asserts the retraction directly: an inherited value must be IGNORED.
runscc() {
  local script="$1" shims="$2" envfile="$3"; shift 3
  local -a scc_env=() scc_flags=()
  local a
  for a in "$@"; do
    case "$a" in
      -*) scc_flags+=("$a") ;;
      *) scc_env+=("$a") ;;
    esac
  done
  # THE ROUTING IS PINNED, NOT INHERITED. Section 5b2 compares every exported SCCACHE_* across the
  # three contexts, so a host whose own SCCACHE_DIR/SCCACHE_SERVER_PORT differ from the stub sessions'
  # would make every case here report a routing conflict — host state deciding the verdict, which is
  # what this harness removes everywhere else. One fixed value for the invoker, the same one handed to
  # the `sudo` stub through SCC_STUB_SESSION_DIR (a non-SCCACHE name, because 5b2 scrubs SCCACHE_*).
  env -u CQLITE_GATE_MAX_CONCURRENCY -u SCCACHE_CACHE_SIZE -u SCCACHE_SERVER_PORT \
    SCCACHE_DIR="$tmp/scc-session-cache" SCC_STUB_SESSION_DIR="$tmp/scc-session-cache" \
    PATH="$shims" CARGO_HOME="$tmp/pin-cargo" HOME="$tmp/scc-home" \
    CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_ENV_FILE="$envfile" \
    ${scc_env[@]+"${scc_env[@]}"} \
    "${TIMEOUT_BIN_TEST:-timeout}" -s KILL 300 "$PIN_BS" "$script" \
      --skip-smoke ${scc_flags[@]+"${scc_flags[@]}"} 2>&1
}
# scc_slice <output>: JUST section 5b2's block. Every assertion below is made against the
# slice, never the whole run: sections 5b and 3b legitimately warn in some of these sandboxes,
# and a whole-output warn count would make each case's subject ambiguous.
scc_slice() {
  push_plain "$1" | awk '/== sccache cache-size cap/{f=1;next} /^== /{f=0} f'
}
scc_warns() { printf '%s\n' "$1" | grep -cE '^[[:space:]]+\[warn\] '; }
scc_oks()   { printf '%s\n' "$1" | grep -cE '^[[:space:]]+\[ok\] '; }

if [ "$(id -u)" = 0 ]; then
  skip "sccache-cap: the ENTIRE block (the test seam is refused under root, so section 5b2 cannot be driven here)"
elif [ ! -d "$pinroot/scripts" ]; then
  skip "sccache-cap: the ENTIRE block (the staged bootstrap tree from the 11* block is unavailable)"
else
  mkdir -p "$tmp/scc-home/.cargo"
  scc_bs="$pinroot/scripts/bootstrap-agent-machine.sh"
  # A SECOND COPY WITH THE CAP LITERAL SUBSTITUTED. The shipped literal is now a REAL cap (`50G`,
  # derived as a bracket — see .agent-ami/profile.yaml), so bootstrap no longer refuses it. The
  # WRITE cases still substitute the artifact in their own scratch copy rather than reaching for
  # the shipped value (the idiom this repo mandates over a settable seam), so they stay
  # independent of whatever the fleet cap happens to be. The sed is asserted to have matched.
  # NOTE (#3727): the REFUSAL path — bootstrap declining a literal sccache would silently discard
  # — used to be covered incidentally, BY the shipped literal being a placeholder. Now that it is
  # a real value that coverage is gone, so the refusal needs its OWN scratch fixture with a
  # deliberately unusable literal; 12b-m self-retires rather than pass for the wrong reason.
  scc_bs_sub="$tmp/scc-bs-substituted.sh"
  cp "$scc_bs" "$scc_bs_sub"
  sed -i.bak "s/^SCC_ENV_VALUE='[^']*'$/SCC_ENV_VALUE='30G'/" "$scc_bs_sub" 2>/dev/null \
    || sed -i '' "s/^SCC_ENV_VALUE='[^']*'\$/SCC_ENV_VALUE='30G'/" "$scc_bs_sub" 2>/dev/null
  rm -f "$scc_bs_sub.bak"
  if grep -q "^SCC_ENV_VALUE='30G'\$" "$scc_bs_sub"; then
    ok "sccache-cap: the substituted scratch copy carries the test cap literal (the harness's own precondition)"
  else
    bad "sccache-cap: could not substitute SCC_ENV_VALUE in the scratch copy — the write cases below would test nothing"
  fi
  scc_log="$tmp/scc-stub-argv.log"; : >"$scc_log"

  # 12b-a. VERIFIED — the only [ok] this section may ever emit. The file sets the cap, a fresh
  #        profile-free session sees the SAME value, and the RUNNING server enforces exactly the
  #        bytes that value means. Without this case, every negative below would also pass
  #        against a section that can only ever say FAILED.
  scc_shims_v="$tmp/scc-shims-v"
  scc_env_v="$tmp/scc-env-v"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\nSCCACHE_CACHE_SIZE=30G\n' >"$scc_env_v"
  mksccshims "$scc_shims_v" "file:$scc_env_v"
  scc_out_v=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=32212254720 SCC_STUB_LOG="$scc_log")
  scc_sl_v=$(scc_slice "$scc_out_v")
  if out_has "$scc_sl_v" -E '\[ok\].*sccache-cap: VERIFIED' \
     && [ "$(scc_warns "$scc_sl_v")" = 0 ] && [ "$(scc_oks "$scc_sl_v")" = 1 ]; then
    ok "sccache-cap: file + session + RUNNING server agree -> exactly one [ok] VERIFIED and zero [warn]"
  else
    bad "sccache-cap: the positive case did not reach a clean VERIFIED (oks=$(scc_oks "$scc_sl_v") warns=$(scc_warns "$scc_sl_v"))"
    printf '%s\n' "$scc_sl_v" | head -6
  fi
  # The scope note must state what VERIFIED does NOT cover — an unqualified VERIFIED reads as
  # "every gate on this box gets this cap", and the server-startup caveat is the new one.
  # SINCE THE RULING the note declares ONE measured session type and the login shell as a
  # DECLARED RESIDUAL rather than a compared context — and the residual is the assertion that
  # matters, because an unqualified VERIFIED would now cover a launch path nothing measured.
  if out_has "$scc_sl_v" 'scope:.*ONE session type.*NON-LOGIN PAM session' \
     && out_has "$scc_sl_v" 'scope:.*LOGIN shell additionally runs /etc/profile.d' \
     && out_has "$scc_sl_v" 'scope:.*does not measure that context' \
     && out_has "$scc_sl_v" 'scope:.*SERVER at STARTUP' \
     && out_has "$scc_sl_v" 'scope:.*provenance is not' \
     && out_has "$scc_sl_v" 'sccache-cap=<bytes>'; then
    ok "sccache-cap: VERIFIED prints its scope — the ONE session type measured, the login shell DECLARED as unmeasured, server-startup lifetime, unproven provenance, and the gate's own token as per-run authority"
  else
    bad "sccache-cap: the scope note is missing a statement, or still claims a context this section no longer measures"
    printf '%s\n' "$scc_sl_v" | grep 'scope:' | head -4
  fi

  # 12b-b. NOT-HONOURED — the #3727 state itself: the value is persisted, visible and accepted,
  #        and the RUNNING server enforces something else because it predates the value. The
  #        remedy must stop THE SERVER THAT WAS MEASURED and must NOT tell the operator to edit a
  #        value that is already correct (a remedy the operator has already complied with is worse
  #        than none, because it stops them looking).
  #        SINCE roborev job 393 f1 THE REMEDY IS A CONTEXT-CARRYING COMMAND, not the bare
  #        `sccache --stop-server` this case used to pin: the server was located through the PROBED
  #        SESSION's routing, while the operator is root under this script's documented sudo invocation,
  #        so a bare invocation there resolves root's PATH and root's default location — a different
  #        server, or none, with the operator believing they applied it. So the assertion is on the
  #        PROPERTY: it names the probed user, the agreed binary and --stop-server, and it is NOT
  #        the bare form.
  scc_out_nh=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=10737418240 SCC_STUB_LOG="$scc_log")
  scc_sl_nh=$(scc_slice "$scc_out_nh")
  # THE REMEDY TEXT IS GONE (lead ruling req-3727-w4) and this case now asserts its ABSENCE
  # alongside the verdict: the two byte counts ARE the finding, and which server to stop in which
  # context was the interpretation layer the ruling removed. Asserted the other way round so
  # nobody reinstates it by reflex.
  if out_has "$scc_sl_nh" -E '\[warn\].*sccache-cap: NOT-HONOURED' \
     && out_has "$scc_sl_nh" '10737418240 bytes' \
     && ! out_has "$scc_sl_nh" 'remedy' \
     && ! out_has "$scc_sl_nh" -E '\[ok\].*sccache-cap' \
     && ! out_has "$scc_sl_nh" 'fix the VALUE'; then
    ok "sccache-cap: a stale server is NOT-HONOURED naming BOTH byte counts, and prints no remedy text at all"
  else
    bad "sccache-cap: the stale-server state did not report NOT-HONOURED with both byte counts (or an advice layer is back)"
    printf '%s\n' "$scc_sl_nh" | head -6
  fi

  # 12b-c. NOT-SYSTEM-WIDE — visible, accepted and enforced, but NOT coming from the
  #        system-wide file, so a server started outside that source gets sccache's default.
  scc_shims_lit="$tmp/scc-shims-lit"; mksccshims "$scc_shims_lit" 30G
  scc_env_empty="$tmp/scc-env-empty"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$scc_env_empty"
  scc_out_nsw=$(runscc "$scc_bs" "$scc_shims_lit" "$scc_env_empty" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=32212254720 SCC_STUB_LOG="$scc_log")
  scc_sl_nsw=$(scc_slice "$scc_out_nsw")
  if out_has "$scc_sl_nsw" -E '\[warn\].*sccache-cap: NOT-SYSTEM-WIDE' \
     && out_has "$scc_sl_nsw" -- '--fix-sccache-cap' \
     && ! out_has "$scc_sl_nsw" -E '\[ok\].*sccache-cap'; then
    ok "sccache-cap: a value reaching only this session is NOT-SYSTEM-WIDE, remedied by --fix-sccache-cap"
  else
    bad "sccache-cap: a session-only value was not reported as NOT-SYSTEM-WIDE"
    printf '%s\n' "$scc_sl_nsw" | head -6
  fi

  # 12b-d1. FAILED needs ALL THREE contexts blind. Nothing persisted, no shim injection, and the
  #         invoking environment scrubbed by runscc — so no launch path sees a cap and the verdict is
  #         the affirmative FAILED rather than a disagreement.
  scc_shims_none="$tmp/scc-shims-none"; mksccshims "$scc_shims_none" -
  scc_out_f=$(runscc "$scc_bs" "$scc_shims_none" "$scc_env_empty" SCC_STUB_MAX=32212254720 \
    SCC_STUB_LOG="$scc_log")
  scc_sl_f=$(scc_slice "$scc_out_f")
  if out_has "$scc_sl_f" -E '\[warn\].*sccache-cap: FAILED' \
     && ! out_has "$scc_sl_f" -E '\[ok\].*sccache-cap' \
     && out_has "$scc_sl_f" -- '--fix-sccache-cap'; then
    ok "sccache-cap: with every launch context blind the verdict is FAILED, and the remedy names the flag"
  else
    bad "sccache-cap: an unpinned box with no disagreement did not report FAILED"
    printf '%s\n' "$scc_sl_f" | head -6
  fi

  # 12b-d2. AN INHERITED VALUE IS IGNORED, NOT COMPARED (#3727 — the scrub, and the lead's round-9
  #         RETRACTION of the third context). Bootstrap's OWN environment carries a value — the normal
  #         state of a re-run on a fleet box — while nothing is persisted and no session sees it. Two
  #         properties are asserted together, because they used to fight each other: the value must
  #         never certify anything (the scrub, round 1), and it must ALSO not be treated as a launch
  #         context of its own (round 9: under `sudo bash bootstrap` the invoking environment is
  #         ROOT's, so comparing it reported a false CONFLICTING-SOURCES on a correct box). So the
  #         verdict here is the affirmative FAILED — both sessions are blind — and the scope note must
  #         DECLARE that the invoking shell is not compared, so the gap is visible rather than silent.
  scc_out_inh=$(runscc "$scc_bs" "$scc_shims_none" "$scc_env_empty" SCC_STUB_MAX=32212254720 \
    SCCACHE_CACHE_SIZE=30G SCC_STUB_LOG="$scc_log")
  scc_sl_inh=$(scc_slice "$scc_out_inh")
  if ! out_has "$scc_sl_inh" -E '\[ok\].*sccache-cap' \
     && out_has "$scc_sl_inh" -E '\[warn\].*sccache-cap: FAILED' \
     && ! out_has "$scc_sl_inh" 'CONFLICTING-SOURCES'; then
    ok "sccache-cap: an INHERITED-but-not-persisted value neither certifies nor conflicts — the two blind sessions decide, and the verdict is FAILED"
  else
    bad "sccache-cap: an inherited value was accepted as evidence, or was compared as a launch context of its own"
    printf '%s\n' "$scc_sl_inh" | head -6
  fi

  # 12b-e. THE ONE DECLARED AMBIGUITY. A value that resolves to sccache's OWN default cannot be
  #        told apart from one sccache silently DISCARDED, so the verdict is UNMEASURED with the
  #        ambiguity named and the accepted grammar printed — never a guess, and never VERIFIED.
  for scc_amb in 10G 30GiB; do
    scc_shims_amb="$tmp/scc-shims-amb-$scc_amb"; mksccshims "$scc_shims_amb" "$scc_amb"
    scc_env_amb="$tmp/scc-env-amb-$scc_amb"
    printf 'CQLITE_GATE_MAX_CONCURRENCY=1\nSCCACHE_CACHE_SIZE=%s\n' "$scc_amb" >"$scc_env_amb"
    scc_out_amb=$(runscc "$scc_bs" "$scc_shims_amb" "$scc_env_amb" SCCACHE_CACHE_SIZE="$scc_amb" SCC_STUB_MAX=10737418240 SCC_STUB_LOG="$scc_log")
    scc_sl_amb=$(scc_slice "$scc_out_amb")
    if out_has "$scc_sl_amb" -E '\[warn\].*sccache-cap: UNMEASURED' \
       && out_has "$scc_sl_amb" "sccache's OWN default cap" \
       && out_has "$scc_sl_amb" '<digits>\[KkMmGgTt\]' \
       && ! out_has "$scc_sl_amb" -E '\[ok\].*sccache-cap'; then
      ok "sccache-cap: '$scc_amb' resolving to sccache's own default is UNMEASURED with the ambiguity + grammar named"
    else
      bad "sccache-cap: '$scc_amb' did not produce the declared-ambiguity UNMEASURED"
      printf '%s\n' "$scc_sl_amb" | head -6
    fi
  done

  # 12b-f. NO ORACLE, NO VERDICT — AND NOT ONE PRIVILEGED CALL. With no sccache on PATH the
  #        value->bytes map cannot be asked of the tool that owns it, so the answer is UNMEASURED,
  #        never an [ok] and never a bash reimplementation of the grammar.
  #
  #        THE EXPECTED TEXT FOLLOWS THE PRECONDITION, NOT THE BINARY ARM (issue #3727, after the
  #        root-request blocker). This case used to expect 'no launch context resolved an sccache at
  #        all' — the BINARY-resolution arm's wording — because that was the first thing to notice
  #        the absence. The section now notices it as a SECTION-LEVEL PRECONDITION, before privilege
  #        is resolved, which is the whole point of that fix: a run that could never certify anything
  #        must not ask for root. So the case asserts the new wording and, more importantly, the
  #        PROPERTY the fix added — via the `sudo` shim's argv log, that NO sudo call happened at
  #        all. The old phrase was an implementation detail of which arm fired; the property is the
  #        contract. (The binary arm is still reachable and still covered: sccache present but no
  #        session able to resolve it — 12b-f2/12b-f3.)
  scc_shims_nb="$tmp/scc-shims-nb"; mksccshims "$scc_shims_nb" 30G no-sccache
  scc_sudolog_nb="$tmp/scc-sudo-nb.log"; : >"$scc_sudolog_nb"
  scc_out_nb=$(runscc "$scc_bs" "$scc_shims_nb" "$scc_env_v" SCCACHE_CACHE_SIZE=30G \
    SCC_STUB_MAX=32212254720 SCC_SHIM_SUDO_LOG="$scc_sudolog_nb")
  scc_sl_nb=$(scc_slice "$scc_out_nb")
  if out_has "$scc_sl_nb" -E '\[warn\].*sccache-cap: UNMEASURED' \
     && out_has "$scc_sl_nb" "no 'sccache' on this box's PATH" \
     && ! out_has "$scc_sl_nb" -E '\[ok\].*sccache-cap'; then
    ok "sccache-cap: no sccache on PATH -> UNMEASURED naming the missing tool, never an [ok]"
  else
    bad "sccache-cap: an absent oracle did not produce UNMEASURED"
    printf '%s\n' "$scc_sl_nb" | head -6
  fi
  # THE BLOCKER'S OWN PROPERTY, pinned in the suite that owns this section rather than only in the
  # perf suite that caught it: with nothing to measure, 5b2 must make no privileged call.
  #
  # SCOPED TO 5b2'S ATTRIBUTABLE CALLS, AND THAT LIMIT IS THE POINT. Section 5b legitimately probes
  # sudo in this sandbox (the staged tree here HAS agent-gate.sh, unlike the perf suite's minimal
  # one), so a whole-run "no sudo at all" assert reds on correct input — measured: 3 calls, all 5b's.
  # Two of 5b2's five former calls (`sudo -n -u <user> true` and `sudo -n true`) are TEXTUALLY
  # IDENTICAL to 5b's and cannot be attributed from argv, so this asserts the three that CAN be:
  # the two per-context binary resolutions (`command -v sccache`) and the session probe
  # (`cqlite-scc-probe`). If the precondition regresses, those reappear. The whole-run property —
  # no privileged call by ANY section — is the perf suite's, which is where it was caught.
  scc_nb_attrib=$(grep -cE 'cqlite-scc-probe|command -v sccache' "$scc_sudolog_nb" 2>/dev/null)
  if [ "${scc_nb_attrib:-0}" = 0 ]; then
    ok "sccache-cap: with no sccache present, NONE of 5b2's attributable privileged calls happen (no binary resolution, no session probe) — it stops before resolving privilege"
  else
    bad "sccache-cap: a run that could not certify anything still made ${scc_nb_attrib} 5b2-attributable sudo call(s)"
    grep -E 'cqlite-scc-probe|command -v sccache' "$scc_sudolog_nb" | head -3
  fi

  # 12b-g. NO RUNNING SERVER -> UNMEASURED, not a comparison of the value with itself. MEASURED:
  #        with nothing running, --show-stats answers max_cache_size from the CLIENT's own env
  #        and reports cache_size null, so accepting that number would compare the session value
  #        against itself and call the box VERIFIED.
  scc_out_ns=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=none SCC_STUB_LOG="$scc_log")
  scc_sl_ns=$(scc_slice "$scc_out_ns")
  if out_has "$scc_sl_ns" -E '\[warn\].*sccache-cap: UNMEASURED' \
     && out_has "$scc_sl_ns" 'no sccache server is answering' \
     && ! out_has "$scc_sl_ns" -E '\[ok\].*sccache-cap'; then
    ok "sccache-cap: a null cache_size is UNMEASURED — the client's own echo can never certify a cap"
  else
    bad "sccache-cap: a client-side echo with no server running was not refused"
    printf '%s\n' "$scc_sl_ns" | head -6
  fi

  # 12b-f1b. THE AMBIENT PRECONDITION MAY ONLY ANSWER FOR THE CONTEXT IT IS (roborev job 399, f1).
  #          Under the documented `sudo bash <this script>` the ambient PATH is sudo's secure_path,
  #          which omits ~/.cargo/bin — where section 2's own `cargo install sccache` puts the
  #          binary — so the precondition above reported "no sccache on this box" for an installed
  #          tool and --fix-sccache-cap repaired nothing. It is now gated on being the account a
  #          gate runs as: a numeric NON-ZERO EUID.
  #
  #          THE BEHAVIOURAL HALF IS 12b-f ABOVE (this suite runs as an ordinary user, EUID != 0, so
  #          it takes the answering branch and must still refuse without asking for privilege).
  #          THE ROOT HALF IS NOT EXERCISABLE HERE, AND IS LABELLED AS SUCH RATHER THAN DRESSED UP:
  #          `$EUID` is bash's own readonly and this suite is not root, so no fixture can make that
  #          branch execute — a `sudo`/`unshare` fixture would test a different box and a SKIP here
  #          is policed by case 13. So it is asserted STRUCTURALLY over the SHIPPED source: the
  #          absence verdict must be EUID-gated, and the bypass must exist and say why the ambient
  #          PATH is not authoritative. Structural, not behavioural — it proves the gate is written,
  #          never that it fires.
  # $BOOTSTRAP, never $PIN_BS: PIN_BS is this suite's GUARD WRAPPER (it takes the script as its
  # first argument), so reading it yielded an EMPTY slice and the assertion failed for a reason
  # that had nothing to do with the property — measured, first run.
  scc_pre_src=$(sed -n '/^scc_pre_euid=/,/^fi$/p' "$BOOTSTRAP")
  if out_has "$scc_pre_src" -E '^scc_pre_euid="\$\{EUID-\}"$' \
     && out_has "$scc_pre_src" -E "^case \"\\\$scc_pre_euid\" in ''\|\*\[!0-9\]\*\)" \
     && out_has "$scc_pre_src" -E '^  if \[ -n "\$scc_pre_euid" \] && \[ "\$scc_pre_euid" != 0 \]; then$' \
     && out_has "$scc_pre_src" 'no cap to verify' \
     && out_has "$scc_pre_src" 'secure_path' \
     && out_has "$scc_pre_src" 'decided by the SESSION contexts'; then
    ok "sccache-cap: the ambient-absence verdict is gated on a numeric non-zero EUID, and root bypasses it to the session contexts (structural: this suite cannot be root)"
  else
    bad "sccache-cap: the ambient precondition is not EUID-gated — under 'sudo bash bootstrap' a cargo-installed sccache reads as absent and --fix-sccache-cap repairs nothing"
    printf '%s\n' "$scc_pre_src" | head -8
  fi

  # 12b-f2b. TWO PROBES MUST NOT AGREE BY SHARING OUR CONTAMINATION (roborev job 399, f2). The
  #          scrub before each session used to NAME three variables, leaving SCCACHE_REDIS /
  #          SCCACHE_CONF / SCCACHE_WEBDAV_* / any FUTURE SCCACHE_* in the caller's environment. If
  #          sudoers preserves them BOTH probes inherit the same caller-specific routing, agree
  #          because they share our contamination, and the section certifies — or STARTS — a server
  #          no ordinary session will contact: a false VERIFIED, the exact defect 5b2 exists to
  #          catch, in the code that reports it. Now blanket, derived from `compgen -e`, because a
  #          future backend variable is unknowable and an enumerated list goes stale silently.
  #
  #          MEASURED AT THE SUDO BOUNDARY, not inferred from the verdict: the stub censuses the
  #          SCCACHE_* it INHERITED — for MEASUREMENT calls only (a probe, a binary resolution, a
  #          stats read, a start/stop), because those are the calls SCCACHE_* can change. The bare
  #          `sudo … true` privilege probes and the file writes carry the caller's environment and
  #          always will: nothing they run reads it. That scope is set BY ARGV in the stub rather
  #          than left as an absence, and it is the same boundary 12b-f2c asserts over the source —
  #          source-side every measurement call must carry the scrub, boundary-side no measurement
  #          call may see caller routing. Two positive controls, because an empty census is otherwise
  #          indistinguishable from a census that never ran — `census-ran` proves the logger fired,
  #          and an unrelated CQLITE_SCRUB_MARKER must SURVIVE, proving the scrub is SCCACHE_*-scoped
  #          rather than an env wipe that would make the probe answer about nothing.
  scc_envlog="$tmp/scc-sudo-env.log"; : >"$scc_envlog"
  scc_out_scrub=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G \
    SCC_STUB_MAX=32212254720 SCC_SHIM_ENV_LOG="$scc_envlog" CQLITE_SCRUB_MARKER=present \
    SCCACHE_REDIS=redis://poisoned.example/1 SCCACHE_CONF=/caller/poisoned.toml)
  scc_sl_scrub=$(scc_slice "$scc_out_scrub")
  if grep -q '^census-ran$' "$scc_envlog" \
     && grep -q '^marker:present$' "$scc_envlog" \
     && ! grep -q 'poisoned' "$scc_envlog" \
     && out_has "$scc_sl_scrub" -E '\[ok\].*sccache-cap: VERIFIED'; then
    ok "sccache-cap: every caller SCCACHE_* is scrubbed before each session probe (SCCACHE_REDIS/SCCACHE_CONF absent at the sudo boundary), while an unrelated variable survives — so the probes cannot agree by sharing our routing"
  else
    bad "sccache-cap: a caller-only SCCACHE_* reached the session probe (or the census never ran) — two probes could agree on OUR routing and certify a server no gate will use"
    printf '  census: %s\n' "$(grep -c '^' "$scc_envlog" 2>/dev/null || echo 0) line(s)"
    grep -n 'poisoned\|census-ran\|marker:' "$scc_envlog" | head -4
    printf '%s\n' "$scc_sl_scrub" | head -4
  fi

  # 12b-f2c. THE SCRUB IS ASSERTED OVER EVERY SESSION CALL, DERIVED FROM THE SHIPPED SOURCE — not
  #          over the one call site a review happened to name. 12b-f2b's census caught the first fix
  #          being INCOMPLETE: the value probe was scrubbed while the BINARY RESOLUTION and the
  #          shared session runner (server reads and the start — where a caller's SCCACHE_REDIS
  #          redirects the read, not merely the report) still carried three named unsets. A curated
  #          assertion would have passed the incomplete fix, which is the same failure one level up
  #          from the enumerated list itself. So: join continuation lines, take every REAL
  #          `sudo -n -u "$SCC_SELF_USER"` invocation, and require the blanket scrub on each.
  #          ONE EXCUSAL, BY NAME AND WITH ITS REASON: the privilege probe runs `true`, so no
  #          environment can influence it. A floor guards against a refactor greening this vacuously.
  scc_sess_bad=""; scc_sess_n=0; scc_sess_excused=0
  while IFS= read -r scc_sess_line; do
    scc_sess_t=${scc_sess_line#"${scc_sess_line%%[![:space:]]*}"}
    case "$scc_sess_t" in
      '#'*) continue ;;
      *'info "'*|*'warn "'*|*'info '\''*'*) continue ;;
    esac
    scc_sess_n=$((scc_sess_n + 1))
    case "$scc_sess_t" in
      *'sudo -n -u "$SCC_SELF_USER" true'*) scc_sess_excused=$((scc_sess_excused + 1)); continue ;;
    esac
    case "$scc_sess_t" in
      *'SCC_ENV_SCRUB'*) ;;
      *) scc_sess_bad="${scc_sess_bad:+$scc_sess_bad
}  $scc_sess_t" ;;
    esac
  done < <(sed -e ':a' -e '/\\$/N; s/\\\n//; ta' "$BOOTSTRAP" | grep -F 'sudo -n -u "$SCC_SELF_USER"')
  if [ "$scc_sess_n" -ge 4 ] && [ -z "$scc_sess_bad" ]; then
    ok "sccache-cap: all $scc_sess_n session invocations carry the blanket SCCACHE_* scrub ($scc_sess_excused excused by name: the privilege probe runs 'true')"
  else
    bad "sccache-cap: $scc_sess_n session invocation(s) found, and one carries no blanket scrub — the caller's routing reaches a session probe, a read or a start:"
    printf '%s\n' "${scc_sess_bad:-  (no invocation found at all — the derivation broke, which is not a pass)}"
  fi

  # 12b-g2. A FRESH PROVISIONED BOX: NO SERVER YET, AND THE SECTION BECOMES THE FIRST STARTER
  #         (issue #3727 roborev finding 2). This is the case that made every newly launched box
  #         fail `--strict` immediately after correctly persisting the cap: nothing has compiled
  #         yet, so cache_size is null and the cap IN FORCE is genuinely unmeasurable. The fix is
  #         the mechanism itself — the cap is fixed by whichever process starts the server FIRST,
  #         so bootstrap starts it under the persisted value. Asserted three ways: the verdict,
  #         the DECLARATION that this run started it (an [ok] that reads as an independent
  #         observation would be over-read), and the stub's argv.
  scc_state_w="$tmp/scc-stub-state-fresh"; rm -f "$scc_state_w"
  scc_log_fresh="$tmp/scc-stub-argv-fresh.log"; : >"$scc_log_fresh"
  scc_out_fresh=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=none \
    SCC_STUB_STATE="$scc_state_w" SCC_STUB_LOG="$scc_log_fresh" --fix-sccache-cap)
  scc_sl_fresh=$(scc_slice "$scc_out_fresh")
  if out_has "$scc_sl_fresh" -E '\[ok\].*sccache-cap: VERIFIED' \
     && [ "$(scc_warns "$scc_sl_fresh")" = 0 ]; then
    ok "sccache-cap: a fresh box with NO server reaches VERIFIED — the section starts the server under the persisted value instead of failing --strict"
  else
    bad "sccache-cap: a fresh box with no running server did not reach VERIFIED (roborev finding 2)"
    printf '%s\n' "$scc_sl_fresh" | head -8
  fi
  if out_has "$scc_sl_fresh" 'THIS RUN STARTED' \
     && out_has "$scc_sl_fresh" 'scope:.*THIS RUN started it'; then
    ok "sccache-cap: the VERIFIED verdict DECLARES that this run started the server (not an independent observation)"
  else
    bad "sccache-cap: a run that started the server claimed VERIFIED without saying so"
    printf '%s\n' "$scc_sl_fresh" | grep -E 'VERIFIED|scope:' | head -4
  fi
  if grep -q -- '--start-server' "$scc_log_fresh"; then
    ok "sccache-cap: the start is REAL — 'sccache --start-server' appears in the recorded argv"
  else
    bad "sccache-cap: VERIFIED was reached with no --start-server invocation"
    cat "$scc_log_fresh" | head -5
  fi
  # ... and the cap the started server enforces is the one from the FILE, not sccache's default:
  # that is the difference between provisioning and a vacuous pass.
  if [ "$(cat "$scc_state_w" 2>/dev/null)" = 32212254720 ]; then
    ok "sccache-cap: the server this run started was started under the persisted 30G (32212254720 bytes), not the default"
  else
    bad "sccache-cap: the started server got '$(cat "$scc_state_w" 2>/dev/null)' rather than the persisted value's bytes"
  fi

  # 12b-g2b. A RUNNING SERVER WITH AN EMPTY CACHE STILL VERIFIES — the falsifier for the premise
  #          this section shipped for one commit (issue #3727). `"cache_size":null` was read as
  #          "no server is running", and measured against real sccache a server freshly started at
  #          40G on a private port reports cap 42949672960 with size NULL; the two payloads differ
  #          only in their values. Keyed on a null size, the section reported UNMEASURED about a
  #          server whose cap it had just correctly established — and the first version of this
  #          suite was GREEN because the stub encoded the same premise as the code. Attribution is
  #          now a differential (a running server's answer does not move when the client's
  #          SCCACHE_CACHE_SIZE changes; a client's does), and this case pins it.
  scc_log_empty="$tmp/scc-stub-argv-empty.log"; : >"$scc_log_empty"
  scc_out_empty=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=32212254720 \
    SCC_STUB_USED=null SCC_STUB_LOG="$scc_log_empty" --fix-sccache-cap)
  scc_sl_empty=$(scc_slice "$scc_out_empty")
  if out_has "$scc_sl_empty" -E '\[ok\].*sccache-cap: VERIFIED' \
     && [ "$(scc_warns "$scc_sl_empty")" = 0 ] \
     && ! out_has "$scc_sl_empty" 'THIS RUN STARTED' \
     && ! grep -q -- '--start-server' "$scc_log_empty"; then
    ok "sccache-cap: a RUNNING server with an empty cache (cache_size null) VERIFIES as an ALREADY-RUNNING server — no start, no UNMEASURED"
  else
    bad "sccache-cap: a running server with an empty cache was mistaken for no server (the null-size premise is back)"
    printf '%s\n' "$scc_sl_empty" | head -6; cat "$scc_log_empty" | head -3
  fi

  # 12b-g2c. A LOST START RACE IS NOT OWNERSHIP, AND MUST NOT SUPPRESS THE REMEDY (issue #3727
  #          roborev round 10, f2). On this fleet several lanes share one sccache server, so losing
  #          the race is routine: `--start-server` is a no-op against a server that already exists
  #          and the read-back then describes SOMEBODY ELSE'S. Claiming ownership on any successful
  #          read made the run assert it had started a server whose cap it did not choose — and
  #          scc_stale_remedy then called that an sccache-level inconsistency and SUPPRESSED the
  #          stop-server remedy, turning a fixable stale server into "sccache is broken". The stub
  #          models the race: no server at first, and the one that appears after the start enforces
  #          the DEFAULT rather than the requested 30G.
  scc_state_race="$tmp/scc-stub-state-race"; rm -f "$scc_state_race"
  scc_log_race="$tmp/scc-stub-argv-race.log"; : >"$scc_log_race"
  scc_out_race=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G \
    SCC_STUB_MAX=none SCC_STUB_STATE="$scc_state_race" SCC_STUB_RACE_CAP=10737418240 \
    SCC_STUB_LOG="$scc_log_race" --fix-sccache-cap)
  scc_sl_race=$(scc_slice "$scc_out_race")
  if out_has "$scc_sl_race" -E '\[warn\].*sccache-cap: NOT-HONOURED' \
     && out_has "$scc_sl_race" 'a start was attempted' \
     && ! out_has "$scc_sl_race" 'THIS RUN STARTED' \
     && ! out_has "$scc_sl_race" 'sccache-level inconsistency' \
     && ! out_has "$scc_sl_race" -E '\[ok\].*sccache-cap' \
     && grep -q -- '--start-server' "$scc_log_race"; then
    ok "sccache-cap: a LOST start race (the start WAS attempted, another cap answered) is REPORTED as the measured fact and does NOT claim ownership"
  else
    bad "sccache-cap: a lost start race claimed ownership, or the attempt was not reported"
    printf '%s\n' "$scc_sl_race" | head -8
  fi

  # 12b-g3. A DEFAULT RUN STARTS NOTHING. Starting a daemon is a host mutation, and this file's
  #         standing contract is that a run without --yes / a --fix flag mutates nothing — so the
  #         same fresh box stays UNMEASURED, names what is missing, and points at the flag.
  scc_state_d="$tmp/scc-stub-state-default"; rm -f "$scc_state_d"
  scc_log_def="$tmp/scc-stub-argv-default.log"; : >"$scc_log_def"
  scc_out_def=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=none \
    SCC_STUB_STATE="$scc_state_d" SCC_STUB_LOG="$scc_log_def")
  scc_sl_def=$(scc_slice "$scc_out_def")
  if out_has "$scc_sl_def" -E '\[warn\].*sccache-cap: UNMEASURED' \
     && out_has "$scc_sl_def" 'no sccache server is answering' \
     && out_has "$scc_sl_def" -- '--fix-sccache-cap' \
     && ! grep -q -- '--start-server' "$scc_log_def" \
     && [ ! -s "$scc_state_d" ]; then
    ok "sccache-cap: a DEFAULT run starts no server — UNMEASURED naming the flag, and zero host mutation"
  else
    bad "sccache-cap: a default run either started a server or failed to point at the flag"
    printf '%s\n' "$scc_sl_def" | head -6; cat "$scc_log_def" | head -3
  fi

  # 12b-g4. AND IT NEVER RESTARTS A LIVE SERVER. The asymmetry is deliberate: a running server may
  #         have a peer lane's gate compiling against it, so a cap it does not enforce stays
  #         NOT-HONOURED with a remedy for a human to run between gates — bootstrap must not
  #         start (or stop) anything here even under --fix-sccache-cap.
  scc_state_l="$tmp/scc-stub-state-live"; rm -f "$scc_state_l"
  scc_log_live="$tmp/scc-stub-argv-live.log"; : >"$scc_log_live"
  scc_out_live=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=10737418240 \
    SCC_STUB_STATE="$scc_state_l" SCC_STUB_LOG="$scc_log_live" --fix-sccache-cap)
  if out_has "$(scc_slice "$scc_out_live")" -E '\[warn\].*sccache-cap: NOT-HONOURED' \
     && ! grep -qE -- '--start-server|--stop-server' "$scc_log_live" \
     && [ ! -s "$scc_state_l" ]; then
    ok "sccache-cap: a LIVE server with the wrong cap is NOT-HONOURED and is neither started nor stopped (a peer lane may be compiling against it)"
  else
    bad "sccache-cap: a live server was restarted or stopped, or the verdict was not NOT-HONOURED"
    printf '%s\n' "$(scc_slice "$scc_out_live")" | head -6; cat "$scc_log_live" | head -3
  fi

  # 12b-h. THE ISOLATION ASSERT, which is the single most important line in the section: if the
  #        isolated probe is answered by a DIFFERENT sccache, its cap says nothing about our
  #        value and the reading must be discarded rather than trusted.
  scc_out_iso=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=32212254720 \
    SCC_STUB_ISO_LOC=/some/other/servers/cache SCC_STUB_LOG="$scc_log")
  scc_sl_iso=$(scc_slice "$scc_out_iso")
  if out_has "$scc_sl_iso" -E '\[warn\].*sccache-cap: UNMEASURED' \
     && out_has "$scc_sl_iso" 'answered by a DIFFERENT sccache' \
     && ! out_has "$scc_sl_iso" -E '\[ok\].*sccache-cap'; then
    ok "sccache-cap: a foreign sccache answering the isolated probe is DISCARDED, not trusted"
  else
    bad "sccache-cap: the isolation assert did not fire on a foreign cache location"
    printf '%s\n' "$scc_sl_iso" | head -6
  fi

  # 12b-i. AND IT NEVER STOPS A SERVER. Behavioural, from the stub's recorded argv across every
  #        case above: the production server is somebody else's, and a `--stop-server` here
  #        would cost a peer lane's in-flight compile its cache. (This is why the isolated
  #        oracle was built as a READ rather than as the plan's start-a-server design.)
  if [ -s "$scc_log" ] && ! grep -q -- '--stop-server' "$scc_log"; then
    ok "sccache-cap: across every case, section 5b2 invoked sccache $(grep -c '^' "$scc_log") time(s) and NEVER --stop-server"
  elif [ ! -s "$scc_log" ]; then
    bad "sccache-cap: the sccache stub recorded NO invocations — the cases above measured nothing"
  else
    bad "sccache-cap: section 5b2 invoked 'sccache --stop-server' — it must never stop a server it does not own"
    grep -n -- '--stop-server' "$scc_log" | head -3
  fi

  # 12b-j. NON-LINUX IS UNMEASURED, NEVER AN [ok]. The correlation's file half does not exist
  #        there, so a machine-wide cap cannot be told from a sudo- or user-scoped one — and an
  #        `ok "NOT-APPLICABLE"` would let --strict CERTIFY AN UNCAPPED HOST, which is this
  #        issue's own defect wearing a platform label (the mistake #3414 made and removed).
  scc_shims_mac="$tmp/scc-shims-mac"; mksccshims "$scc_shims_mac" 30G
  mk_stub "$scc_shims_mac" uname 'echo Darwin; exit 0'
  scc_out_mac=$(runscc "$scc_bs" "$scc_shims_mac" "$scc_env_v" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=32212254720 SCC_STUB_LOG="$scc_log")
  scc_sl_mac=$(scc_slice "$scc_out_mac")
  if out_has "$scc_sl_mac" -E '\[warn\].*sccache-cap: UNMEASURED' \
     && ! out_has "$scc_sl_mac" -E '\[ok\]'; then
    ok "sccache-cap: a non-Linux host with a session-visible, enforced cap is UNMEASURED, never certified"
  else
    bad "sccache-cap: a non-Linux host produced a success verdict"
    printf '%s\n' "$scc_sl_mac" | head -6
  fi

  # 12b-k. NEVER REWRITES AN EXISTING VALUE, even under --fix-sccache-cap and even when the
  #        existing value differs from this fleet's: a box deliberately running its own cap keeps
  #        it. Asserted byte-for-byte, because "left as is" is a claim about the FILE.
  scc_env_7g="$tmp/scc-env-7g"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\nSCCACHE_CACHE_SIZE=7G\n' >"$scc_env_7g"
  scc_before=$(cat "$scc_env_7g")
  scc_shims_7g="$tmp/scc-shims-7g"; mksccshims "$scc_shims_7g" "file:$scc_env_7g"
  scc_out_7g=$(runscc "$scc_bs_sub" "$scc_shims_7g" "$scc_env_7g" SCCACHE_CACHE_SIZE=7G SCC_STUB_MAX=7516192768 \
    SCC_STUB_LOG="$scc_log" --fix-sccache-cap)
  if [ "$(cat "$scc_env_7g")" = "$scc_before" ]; then
    ok "sccache-cap: --fix-sccache-cap leaves an existing SCCACHE_CACHE_SIZE byte-identical (never rewrites a value)"
  else
    bad "sccache-cap: --fix-sccache-cap rewrote an existing value"
    diff <(printf '%s\n' "$scc_before") "$scc_env_7g" | head -6
  fi
  # ... and that box is VERIFIED at ITS OWN cap, which is what makes the no-rewrite rule safe
  # rather than merely polite.
  scc_sl_7g=$(scc_slice "$scc_out_7g")
  if out_has "$scc_sl_7g" -E '\[ok\].*sccache-cap: VERIFIED.*7G'; then
    ok "sccache-cap: a box on its own 7G cap VERIFIES at that value (the fleet literal is not imposed)"
  else
    bad "sccache-cap: a box with its own cap did not verify at its own value (slice $(printf '%s' "$scc_sl_7g" | wc -c) bytes, whole output $(printf '%s' "$scc_out_7g" | wc -c) bytes)"
    printf '%s\n' "$scc_sl_7g" | head -6
  fi

  # 12b-l. THE WRITE. With a substituted literal and no line in the file, --fix-sccache-cap
  #        persists it, and the SAME RUN's probe then sees it — pam_env reads the file at
  #        session creation, so no reboot and no re-login (the PAM stand-in models exactly that).
  scc_env_w="$tmp/scc-env-w"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$scc_env_w"
  scc_shims_w="$tmp/scc-shims-w"; mksccshims "$scc_shims_w" "file:$scc_env_w"
  scc_out_w=$(runscc "$scc_bs_sub" "$scc_shims_w" "$scc_env_w" SCCACHE_CACHE_SIZE=30G SCC_STUB_MAX=32212254720 \
    SCC_STUB_LOG="$scc_log" --fix-sccache-cap)
  if grep -q '^SCCACHE_CACHE_SIZE=30G$' "$scc_env_w" \
     && grep -q '^# cqlite: sccache object-cache size cap' "$scc_env_w" \
     && out_has "$(scc_slice "$scc_out_w")" -E '\[ok\].*sccache-cap: VERIFIED' \
     && out_has "$(scc_slice "$scc_out_w")" 'resolves to 32212254720 bytes'; then
    ok "sccache-cap: --fix-sccache-cap persists the cap, NAMES the bytes sccache resolves it to, AND the same run's probe verifies it"
  else
    bad "sccache-cap: the write path did not persist + verify in one run"
    echo "--- env file ---"; cat "$scc_env_w"; scc_slice "$scc_out_w" | head -6
  fi
  # The comment goes on its OWN line: pam_env takes a trailing `# …` as part of the value, so an
  # inline comment would make the persisted cap literally `30G  # cqlite: …` — which sccache
  # silently discards.
  if ! grep -q '^SCCACHE_CACHE_SIZE=.*#' "$scc_env_w"; then
    ok "sccache-cap: the persisted line carries NO inline comment (pam_env would read it as part of the value)"
  else
    bad "sccache-cap: the persisted line has an inline comment — pam_env would fold it into the value"
    grep -n 'SCCACHE_CACHE_SIZE' "$scc_env_w"
  fi

  # 12b-m. THE UNUSABLE-LITERAL REFUSAL, ON ITS OWN FIXTURE (issue #3727). Persisting a value
  #        sccache silently discards is worse than persisting nothing: there is no diagnostic
  #        anywhere, and because this section never rewrites an existing value it would be
  #        PERMANENT. This used to be keyed on the SHIPPED literal, which covered the refusal only
  #        INCIDENTALLY — by that literal being an unsubstituted placeholder. Now that the fleet
  #        cap is a real value (`50G`), that coverage would have evaporated, so the case
  #        substitutes the artifact in its own scratch copy and is independent of whatever the
  #        fleet cap becomes.
  #
  #        `50GiB` is chosen deliberately over an obviously-silly string: it is the REAL-WORLD
  #        TRAP — the spelling anyone would reach for — and it is MEASURED to yield sccache's
  #        10 GiB default rather than 50 GiB. A fixture that used `zzz` would prove the guard
  #        rejects garbage while saying nothing about the value that actually gets shipped by
  #        mistake.
  scc_bs_bad="$tmp/scc-bs-unusable.sh"
  cp "$scc_bs" "$scc_bs_bad"
  sed -i.bak "s/^SCC_ENV_VALUE='[^']*'\$/SCC_ENV_VALUE='50GiB'/" "$scc_bs_bad" 2>/dev/null \
    || sed -i '' "s/^SCC_ENV_VALUE='[^']*'\$/SCC_ENV_VALUE='50GiB'/" "$scc_bs_bad" 2>/dev/null
  rm -f "$scc_bs_bad.bak"
  if grep -q "^SCC_ENV_VALUE='50GiB'\$" "$scc_bs_bad"; then
    ok "sccache-cap: the unusable-literal fixture carries 50GiB (the harness's own precondition)"
  else
    bad "sccache-cap: could not plant the unusable literal — the refusal case below would test nothing"
  fi
  scc_env_ph="$tmp/scc-env-ph"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$scc_env_ph"
  scc_out_ph=$(runscc "$scc_bs_bad" "$scc_shims_w" "$scc_env_ph" SCC_STUB_MAX=32212254720 \
    SCC_STUB_LOG="$scc_log" --fix-sccache-cap)
  scc_sl_ph=$(scc_slice "$scc_out_ph")
  if ! grep -q 'SCCACHE_CACHE_SIZE' "$scc_env_ph" \
     && out_has "$scc_sl_ph" 'SILENTLY DISCARD' \
     && out_has "$scc_sl_ph" '50GiB' \
     && ! out_has "$scc_sl_ph" -E '\[ok\].*sccache-cap'; then
    ok "sccache-cap: an unusable cap literal ('50GiB', the real-world trap) is REFUSED and NAMED, never persisted — a discarded line would be permanent and invisible"
  else
    bad "sccache-cap: an unusable cap literal was persisted, or the refusal was silent"
    echo "--- env file ---"; cat "$scc_env_ph"; printf '%s\n' "$scc_sl_ph" | head -4
  fi

  # 12b-m2. THE SHAPE TEST IS NOT THE ORACLE (issue #3727 roborev round 4, f1). A 21-digit literal
  #         plus a suffix passes every shape rule this repo could write and MEASURES as sccache's
  #         10 GiB default — so the shape guard alone would have let `--fix-sccache-cap` persist an
  #         ineffective cap that the section then never rewrites: permanent and invisible, which is
  #         the exact harm the guard exists to prevent. The write is now authorized by the ORACLE, so
  #         this case plants a literal the shape test ACCEPTS and requires the refusal to happen
  #         anyway, and to be attributed to sccache rather than to a shape rule.
  scc_bs_big="$tmp/scc-bs-oversized.sh"
  cp "$scc_bs" "$scc_bs_big"
  sed -i.bak "s/^SCC_ENV_VALUE='[^']*'\$/SCC_ENV_VALUE='999999999999999999999G'/" "$scc_bs_big" 2>/dev/null \
    || sed -i '' "s/^SCC_ENV_VALUE='[^']*'\$/SCC_ENV_VALUE='999999999999999999999G'/" "$scc_bs_big" 2>/dev/null
  rm -f "$scc_bs_big.bak"
  scc_big_val=$(sed -n "s/^SCC_ENV_VALUE='\(.*\)'\$/\1/p" "$scc_bs_big" | head -1)
  # The precondition that makes this case meaningful: the planted literal must be one the SHAPE test
  # would wave through. If it were shape-rejected the case would pass for the wrong reason.
  if [ "$scc_big_val" = '999999999999999999999G' ] \
     && out_has "$scc_big_val" -E '^[0-9]+[KkMmGgTt]$'; then
    ok "sccache-cap: the oversized-literal fixture is planted AND is shape-valid (so only the oracle can refuse it)"
  else
    bad "sccache-cap: could not plant a shape-valid oversized literal — the case below would test nothing"
  fi
  scc_env_big="$tmp/scc-env-big"; printf 'CQLITE_GATE_MAX_CONCURRENCY=1\n' >"$scc_env_big"
  scc_out_big=$(runscc "$scc_bs_big" "$scc_shims_w" "$scc_env_big" SCCACHE_CACHE_SIZE=30G \
    SCC_STUB_MAX=32212254720 SCC_STUB_LOG="$scc_log" --fix-sccache-cap)
  scc_sl_big=$(scc_slice "$scc_out_big")
  if ! grep -q 'SCCACHE_CACHE_SIZE' "$scc_env_big" \
     && out_has "$scc_sl_big" 'SCCACHE ITSELF' \
     && out_has "$scc_sl_big" 'OWN default cap' \
     && ! out_has "$scc_sl_big" -E '\[ok\].*sccache-cap'; then
    ok "sccache-cap: an OVERSIZED but shape-valid literal is refused BY THE ORACLE, not persisted (the second-implementation gap)"
  else
    bad "sccache-cap: an oversized shape-valid literal was persisted, or the refusal was not attributed to sccache"
    echo "--- env file ---"; cat "$scc_env_big"; printf '%s\n' "$scc_sl_big" | head -4
  fi

  # 12b-n. THE OPT-OUT is loud and NON-PASSING — a switch that returned `ok` would be a way to
  #        buy a vacuous green, which is the failure mode this section removes.
  for scc_optout in --skip-sccache-cap env:CQLITE_BOOTSTRAP_SKIP_SCCACHE_CAP=1; do
    if [ "${scc_optout#env:}" != "$scc_optout" ]; then
      scc_out_oo=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" "${scc_optout#env:}" SCC_STUB_MAX=32212254720)
    else
      scc_out_oo=$(runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" SCC_STUB_MAX=32212254720 "$scc_optout")
    fi
    scc_sl_oo=$(scc_slice "$scc_out_oo")
    if out_has "$scc_sl_oo" -E '\[warn\].*sccache-cap: OPT-OUT' \
       && ! out_has "$scc_sl_oo" -E '\[ok\].*sccache-cap'; then
      ok "sccache-cap: $scc_optout is a [warn] OPT-OUT that can never buy a green"
    else
      bad "sccache-cap: $scc_optout did not report as a non-passing OPT-OUT"
      printf '%s\n' "$scc_sl_oo" | head -4
    fi
  done
  # Contradictory intents do not resolve silently: an EXPLICIT skip beside an explicit fix is a
  # usage error (exit 2), while the weaker ENV opt-out loses to an explicit --fix-sccache-cap.
  scc_rc_x=0
  runscc "$scc_bs" "$scc_shims_v" "$scc_env_v" --skip-sccache-cap --fix-sccache-cap >/dev/null 2>&1 || scc_rc_x=$?
  if [ "$scc_rc_x" = 2 ]; then
    ok "sccache-cap: --skip-sccache-cap beside --fix-sccache-cap is a usage error (exit 2), not a silent winner"
  else
    bad "sccache-cap: contradictory flags resolved silently (rc=$scc_rc_x, expected 2)"
  fi
  scc_out_envfix=$(runscc "$scc_bs_sub" "$scc_shims_w" "$scc_env_w" \
    CQLITE_BOOTSTRAP_SKIP_SCCACHE_CAP=1 SCC_STUB_MAX=32212254720 --fix-sccache-cap)
  if ! out_has "$(scc_slice "$scc_out_envfix")" 'OPT-OUT'; then
    ok "sccache-cap: an env opt-out cannot neuter an explicit --fix-sccache-cap"
  else
    bad "sccache-cap: CQLITE_BOOTSTRAP_SKIP_SCCACHE_CAP=1 overrode an explicit --fix-sccache-cap"
  fi
fi

# 12b-o. STRUCTURAL, because the behavioural cases above can only cover the branches someone
#        thought of: section 5b2 must contain EXACTLY ONE `ok` call, and it must be the probe's
#        VERIFIED verdict. Any future `ok` added for a file write, a platform exemption or a
#        visible-but-unenforced value reds this immediately — the twin of 11i for 5b.
scc_section=$(awk '/^# ---- 5b2\./,/^# ---- 5c\./' "$BOOTSTRAP")
scc_ok_total=$(printf '%s\n' "$scc_section" | grep -cE '^[[:space:]]*ok "' || true)
scc_ok_named=$(printf '%s\n' "$scc_section" | grep -cE '^[[:space:]]*ok "sccache-cap: VERIFIED [(]' || true)
if [ -n "$scc_section" ] && [ "${scc_ok_total:-0}" = 1 ] && [ "${scc_ok_named:-0}" = 1 ]; then
  ok "sccache-cap: section 5b2's ONLY success verdict is VERIFIED (no platform exemption, no write-succeeded ok)"
else
  bad "sccache-cap: section 5b2 has ${scc_ok_total:-0} ok() call(s), ${scc_ok_named:-0} of them a named verdict"
fi

# 12b-p. TWO SPELLINGS OF ONE NUMBER IS DRIFT, and this is the only mechanism against it: the
#        fleet cap lives in bootstrap (which persists it) and in .agent-ami/profile.yaml (whose
#        env reaches launcher-created processes). If they disagree, a launched box's session env
#        and its /etc/environment carry different caps and whichever starts the server wins.
scc_profile="$SCRIPT_DIR/../../.agent-ami/profile.yaml"
scc_bs_val=$(sed -n "s/^SCC_ENV_VALUE='\\(.*\\)'\$/\\1/p" "$BOOTSTRAP" | head -1)
scc_prof_val=$(sed -n 's/^[[:space:]]*SCCACHE_CACHE_SIZE:[[:space:]]*"\(.*\)"[[:space:]]*$/\1/p' "$scc_profile" | head -1)
#
#        AGREEMENT IS NOT SUFFICIENT ON ITS OWN, AND THAT GAP WAS REAL (issue #3727 roborev round
#        2, f1): two matching PLACEHOLDERS satisfied an equality test, so the guard greened a
#        fleet cap sccache would silently discard — leaving the only defence a human noticing a
#        TODO. Both halves are now required to be a REAL <digits>[KkMmGgTt] value, so the suite,
#        not a reader, is what stops an unsubstituted placeholder shipping. It is expected to be
#        RED until the measured cap is substituted in BOTH files, and the failure text says so:
#        that red is the mechanism working, not a broken test. (Deliberately the SAME shape check
#        bootstrap applies to its own literal before persisting it — a bare integer is refused
#        too, because sccache reads one as BYTES.)
# scc_lit_ok <literal>: a SHAPE PRE-FILTER, not a grammar oracle — its job is to catch a placeholder
# or an obviously-unusable fleet literal in the two committed files. The AUTHORITATIVE check is
# bootstrap's own oracle at write time (it asks sccache), and 12b-m2 covers the case that motivated
# the split: a 21-digit literal passes every shape rule and MEASURES as sccache's default. The digit
# bound here closes the same gap in this guard, with the same caveat — it is a bound, not a parser.
scc_lit_ok() {
  case "$1" in
    ''|*[!0-9KkMmGgTt]*|*[KkMmGgTt]*[KkMmGgTt]*|[KkMmGgTt]*|*[0-9]) return 1 ;;
  esac
  local __d=${1%[KkMmGgTt]}
  [ "${#__d}" -le 18 ] || return 1
  return 0
}
if [ ! -r "$scc_profile" ]; then
  bad "sccache-cap: .agent-ami/profile.yaml is not readable — cannot check the cap literal or verify.run"
elif ! scc_lit_ok "$scc_bs_val" || ! scc_lit_ok "$scc_prof_val"; then
  bad "sccache-cap: the fleet cap literal is NOT a value sccache accepts — bootstrap says '$scc_bs_val', profile.yaml says '$scc_prof_val'; both must be <digits>[KkMmGgTt] (e.g. 30G). This is EXPECTED to fail while the measured cap is unsubstituted (issue #3727): substitute it in scripts/bootstrap-agent-machine.sh (SCC_ENV_VALUE), .agent-ami/profile.yaml and docs/development/gate-ops.md. Matching placeholders must NOT satisfy this guard — sccache discards them silently and bootstrap then refuses to persist anything"
elif [ "$scc_bs_val" = "$scc_prof_val" ]; then
  ok "sccache-cap: bootstrap's SCC_ENV_VALUE and profile.yaml's SCCACHE_CACHE_SIZE are the SAME, sccache-ACCEPTED literal ('$scc_bs_val')"
else
  bad "sccache-cap: the fleet cap literal DRIFTED — bootstrap says '$scc_bs_val', profile.yaml says '$scc_prof_val'"
fi
# And verify.run must actually pass the flag: a repair nothing calls is a repair that does not
# happen (the same reasoning as the --fix-gate-pin case above).
if [ -r "$scc_profile" ] \
   && out_has "$(grep -E '^[[:space:]]*run:.*bootstrap-agent-machine\.sh' "$scc_profile")" -- '--fix-sccache-cap'; then
  ok "sccache-cap: .agent-ami/profile.yaml's verify.run persists the cap on a launched box (--fix-sccache-cap)"
else
  bad "sccache-cap: verify.run no longer passes --fix-sccache-cap — launched boxes will arrive UNCAPPED"
  grep -nE '^[[:space:]]*run:.*bootstrap-agent-machine\.sh' "$scc_profile" | head -2
fi

# --- 13. NO SKIP MAY BE ANNOUNCED THROUGH ok() (issue #3414 roborev round 2) ----------
# The finding was one `ok "SKIP ..."` — an announcement that incremented PASS and left the
# skip count at 0, i.e. a skip reported as a pass, sitting inside the accounting added the
# round before to expose exactly that. The sweep found a second instance in a sibling
# suite. "If one existed, more may" is the durable half of that finding, so it is a check
# rather than a one-time grep: this scans EVERY suite, so a new one cannot join the habit.
#
# The pattern separates an ANNOUNCEMENT from an ASSERTION ABOUT skips: `ok "SKIP …"` and
# `ok "skip …"` are announcements, while `ok "skip-routing: …"` / `ok "skip-worktree: …"`
# are real passing assertions whose subject happens to be skipping. Matching the former
# and sparing the latter is why this keys on the delimiter after the word, not the word.
# Also asserted host-independently: the root-only case that motivated it never executes on
# an unprivileged host, so a behavioural test could not have covered it here at all.
# COMMENT LINES ARE EXCLUDED, and that is not incidental: the paragraph above QUOTES the
# offending form to explain it, so without this filter the check reds on its own
# documentation — the artifact describing a rule becoming a violation of it. Filter on the
# first non-space character of the matched line, after grep's `file:line:` prefix.
pin_skip_offenders=$(grep -rnE '(^|[^_[:alnum:]])ok "(SKIP|skip)([[:space:]"]|$)' \
  "$SCRIPT_DIR" 2>/dev/null | grep -vE '^[^:]*:[0-9]+:[[:space:]]*#' | head -5)
if [ -z "$pin_skip_offenders" ]; then
  ok "suite hygiene: no test announces a SKIP through ok() (a skip must never increment PASS)"
else
  bad "suite hygiene: a SKIP is announced through ok(), so it counts as a PASS and not as a skip:"
  printf '%s\n' "$pin_skip_offenders"
fi

# --- 13b. THE TRIPWIRE MUST BE ABLE TO FIRE (#3414 roborev round 10) -------------------
# Case 14 asserts the guard found nothing. That is a claim about the WORLD, and on its own
# it is satisfied equally by a guard that works and by a guard that is blind — exactly the
# absence-of-a-bad-signal shape this issue keeps returning to. Nothing in this suite
# legitimately touches the shared file, so the guard's ability to DETECT is otherwise never
# exercised: a plant that neutered it left the suite green.
#
# So the guard is self-tested by PLANTING THE DEFECT IT EXISTS FOR — an invocation whose
# CARGO_HOME is not sandboxed — into a throwaway violations log. Both directions, because
# a guard that fires unconditionally is as useless as one that never fires.
pin_guard_log="$tmp/guard-selftest-violations.log"; : >"$pin_guard_log"
PIN_SHARED_VIOLATIONS="$pin_guard_log" HOME="$tmp/sb-selftest" \
  env -u CARGO_HOME "$PIN_BS" -c 'true' >/dev/null 2>&1
pin_guard_out="$tmp/guard-selftest-outside.log"; : >"$pin_guard_out"
PIN_SHARED_VIOLATIONS="$pin_guard_out" HOME="$tmp/sb-selftest" CARGO_HOME=/usr/local/cargo \
  "$PIN_BS" -c 'true' >/dev/null 2>&1
pin_guard_clean="$tmp/guard-selftest-clean.log"; : >"$pin_guard_clean"
PIN_SHARED_VIOLATIONS="$pin_guard_clean" HOME="$tmp/sb-selftest" CARGO_HOME="$tmp/sb-selftest/.cargo" \
  "$PIN_BS" -c 'true' >/dev/null 2>&1
# ...and the guard's OWN unmeasured-input case: with PIN_SANDBOX_ROOT unset the sandbox
# patterns would degenerate to `/*` and match every absolute path, so a guard that did not
# fail closed here would silently permit everything it exists to catch. Planted with an
# otherwise-PERFECTLY-SANDBOXED pair, so the only thing under test is the missing root.
pin_guard_noroot="$tmp/guard-selftest-noroot.log"; : >"$pin_guard_noroot"
PIN_SHARED_VIOLATIONS="$pin_guard_noroot" HOME="$tmp/sb-selftest" CARGO_HOME="$tmp/sb-selftest/.cargo" \
  env -u PIN_SANDBOX_ROOT "$PIN_BS" -c 'true' >/dev/null 2>&1
if [ -s "$pin_guard_log" ] && [ -s "$pin_guard_out" ] && [ -s "$pin_guard_noroot" ] && [ ! -s "$pin_guard_clean" ]; then
  ok "host hygiene: the guard FIRES on an unset CARGO_HOME, on one outside the sandbox, and on an unusable PIN_SANDBOX_ROOT; and stays quiet on a sandboxed pair"
else
  bad "host hygiene: the input guard cannot fire (or fires unconditionally) — case 14 would be vacuous"
  printf '  unset: %s bytes; outside: %s bytes; no-root: %s bytes; sandboxed: %s bytes\n' \
    "$(wc -c <"$pin_guard_log")" "$(wc -c <"$pin_guard_out")" "$(wc -c <"$pin_guard_noroot")" "$(wc -c <"$pin_guard_clean")"
fi

# --- 14. THIS SUITE MUST NOT TOUCH SHARED HOST STATE -----------------------------------
# Asserted affirmatively rather than by inspection, because the reach is easy to
# reintroduce (a new case that sets HOME and forgets CARGO_HOME, or a `sudo` invocation
# where the exported value is dropped by env_reset) and the cost lands on OTHER lanes as
# unexplained red gates, which is the worst possible place for it to surface.
# ATTRIBUTED BY CONSTRUCTION, not inferred and not observed: the guard asserts each
# invocation's CARGO_HOME and HOME are inside the sandbox, which makes the shared path
# unreachable for bootstrap's `${CARGO_HOME:-$HOME/.cargo}` resolution. No external writer
# can affect this verdict, and it needs no GNU-only probe, so it holds on macOS too.
if [ ! -s "$PIN_SHARED_VIOLATIONS" ]; then
  ok "host hygiene: every bootstrap invocation in this suite ran with sandboxed CARGO_HOME and HOME, so the shared cargo config was unreachable"
else
  bad "host hygiene: a bootstrap invocation in THIS suite ran with an UNSANDBOXED CARGO_HOME or HOME, so it could reach the shared $PIN_SHARED_CARGO — that breaks cargo for every other user on the box"
  cat "$PIN_SHARED_VIOLATIONS"
fi

# --- 14b. GUARD COVERAGE AS A COUNT, NOT A CLAIM (#3414 roborev round 11) --------------
# Case 14 says no invocation escaped the sandbox. That is only as strong as the number of
# invocations actually routed THROUGH the guard: an unwrapped one is invisible to it, so a
# guard covering 49 of 50 sites reports exactly the same green as one covering 50. "Routed
# the direct calls through the wrapper" is a sentence that was true the day it was written
# and unverifiable afterwards; a count is checkable on every run.
#
# Measured from this file's own source: every line that launches bootstrap must do so via
# "$PIN_BS". Excluded, and each for a stated reason rather than by convenience — `bash -n`
# is a syntax check that never executes the script, and a match inside single quotes is a
# grep PATTERN in an assertion about bootstrap's OUTPUT, not an invocation.
pin_cov_total=0; pin_cov_wrapped=0; pin_cov_missing=""
while IFS= read -r pin_cov_line; do
  case "$pin_cov_line" in
    *"bash -n "*) continue ;;
    *"'"*"bootstrap-agent-machine.sh"*"'"*) continue ;;
  esac
  pin_cov_total=$((pin_cov_total + 1))
  case "$pin_cov_line" in
    *'"$PIN_BS"'*) pin_cov_wrapped=$((pin_cov_wrapped + 1)) ;;
    *) pin_cov_missing="${pin_cov_missing:+$pin_cov_missing
}  ${pin_cov_line#"${pin_cov_line%%[![:space:]]*}"}" ;;
  esac
done <<EOF
$(grep -nE '(^|[^-a-zA-Z0-9_])bash [^|;]*bootstrap-agent-machine\.sh|"\$PIN_BS" "\$(BOOTSTRAP|[A-Za-z_][A-Za-z0-9_]*)' "$0" || true)
EOF
if [ "$pin_cov_total" -gt 0 ] && [ -z "$pin_cov_missing" ]; then
  ok "host hygiene: guard coverage is $pin_cov_wrapped/$pin_cov_total bootstrap invocations — none unwrapped"
else
  bad "host hygiene: guard coverage is $pin_cov_wrapped/$pin_cov_total — an unwrapped invocation is INVISIBLE to case 14, which would still report green:"
  printf '%s\n' "$pin_cov_missing"
fi

# --- 15. THE THREE GREEN-PATH CASES MUST HAVE RUN, BY NAME (#3414 roborev round 7) -----
# They have now been silently disabled TWICE by unrelated changes — once when section 5b
# started warning in every sandbox (round 4), once when the seam began refusing under root
# (round 7). Both times the suite reported FAIL=0 and both times the skip count was the
# only trace. The baseline assertion at :1102 catches the CAUSE, and case 13 catches skips
# announced as passes; this catches the EFFECT directly, keyed on the case names, because
# the two prior recurrences arrived through different causes and a third will too.
pin_mustrun_missing=""
for pin_mustrun in \
  "push: VERIFIED yields 'All checks green.' and --strict exits 0 (zero warnings)" \
  "push: AC1+AC3 end to end" \
  "push: the refusal is exactly ONE warning and names the host"; do
  out_has "$PIN_RAN_CASES" -F -- "$pin_mustrun" \
    || pin_mustrun_missing="${pin_mustrun_missing:+$pin_mustrun_missing; }$pin_mustrun"
done
if [ -z "$pin_mustrun_missing" ]; then
  ok "suite: the three green-path cases RAN (not skipped by a warning-count drift)"
else
  bad "suite: green-path case(s) did not run — silently disabled again: $pin_mustrun_missing"
fi

# --- 16. A WHOLESALE DECLINE MUST NOT EXIT 0 (#3414 roborev round 8) -------------------
# Third instance of one shape (case-level pass, `ok "SKIP"`, then suite-level), so it is a
# check rather than a fixed comment. STRUCTURAL, and the limit is stated rather than
# implied: a behavioural test would mean re-running this suite as root FROM INSIDE ITSELF,
# which is recursive and minutes long, so what is asserted here is that every wholesale
# `skip`-then-exit path exits nonzero. The behavioural half is a one-off manual check under
# `sudo`, recorded in the lane verdict file — a structural grep must not be read as a
# behavioural guarantee.
pin_decline_bad=$(awk '
  /^  skip "THE ENTIRE SUITE/ { seen = NR }
  seen && NR > seen && NR <= seen + 8 && /^  exit 0$/ { print NR }
' "$0")
if [ -z "$pin_decline_bad" ] && grep -qE '^  exit 1$' "$0"; then
  ok "suite hygiene: the wholesale-decline path exits NONZERO (a declined suite is not a passing suite)"
else
  bad "suite hygiene: a wholesale decline exits 0 — the gate reads only exit status, so it would certify a suite that ran nothing"
fi

# THE DIGEST SEES UNTRACKED CONTENT ANYWHERE IN THE WORKTREE, NOT JUST UNDER scripts/tests
# (roborev job 332). `git ls-files` defaults to the CURRENT DIRECTORY, so the enumeration
# used to be confined to $SCRIPT_DIR and an untracked file elsewhere could change content
# while the digest reported STABLE — the same silent-omission class as the `xargs -r`
# defect, one axis over.
#
# THE CASE HAS TO CREATE ITS OWN SUBJECT: this lane has ZERO untracked non-ignored files
# (measured), so on this tree the fixed and unfixed forms are INDISTINGUISHABLE and a case
# that merely called pin_tree_id twice would pass either way. It therefore plants a file at
# the WORKTREE TOP (outside $SCRIPT_DIR, which is the whole point), mutates it, and removes
# it — then asserts the digest RETURNED to its original value, which is what proves the
# case cannot destabilise the run's own start/end comparison below. If the case dies before
# cleanup the leftover file flips tree-integrity to MOVED, i.e. it fails LOUDLY rather than
# quietly poisoning the verdict.
# THE PROBE NAME MUST NOT BE GITIGNORED, AND THE FIRST ONE WAS. `.gitignore:79` carries
# `*.tmp`, so a `…-$$.tmp` probe was excluded by `--exclude-standard` — the case would have
# taken the `skip` branch on EVERY run, silently, which is a vacuous case wearing a skip's
# clothes. Caught by the guard below rather than by the suite passing, which is the only
# reason it is not still there. `.txt` is not matched by any rule in this repository; the
# guard stays, because a future .gitignore rule could make it so.
pin_dig_top="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"
pin_dig_probe="$pin_dig_top/pin-digest-probe-$$.txt"
if [ -n "$pin_dig_top" ] && [ -d "$pin_dig_top" ] && ! git check-ignore -q "$pin_dig_probe" 2>/dev/null; then
  pin_dig_before=$(pin_tree_id)
  printf 'a\n' > "$pin_dig_probe" 2>/dev/null
  pin_dig_added=$(pin_tree_id)
  printf 'b\n' > "$pin_dig_probe" 2>/dev/null
  pin_dig_edited=$(pin_tree_id)
  rm -f "$pin_dig_probe"
  pin_dig_after=$(pin_tree_id)
  if [ "$pin_dig_added" != "$pin_dig_before" ] \
     && [ "$pin_dig_edited" != "$pin_dig_added" ] \
     && [ "$pin_dig_after" = "$pin_dig_before" ]; then
    ok "suite hygiene: the tree digest sees a CONTENT change to an untracked file outside scripts/tests (and the probe restored the tree)"
  else
    bad "suite hygiene: an untracked file outside scripts/tests changed content and the digest did not move (job 332)"
    printf '  before=%s added=%s edited=%s after=%s\n' \
      "$pin_dig_before" "$pin_dig_added" "$pin_dig_edited" "$pin_dig_after"
  fi
else
  skip "suite hygiene: cannot plant a NON-IGNORED untracked probe at the worktree top (digest scope unverified — an ignored probe would make this case vacuous)"
fi

PIN_TREE_END=$(pin_tree_id)
printf 'tree-end:   %s\n' "$PIN_TREE_END"
if [ "$PIN_TREE_START" = "$PIN_TREE_END" ]; then
  printf 'tree-integrity: STABLE\n'
else
  # Reported, not failed: the run may still be entirely valid. What it must not do is look
  # like a clean measurement of one tree when it spanned two.
  printf 'tree-integrity: MOVED (%s -> %s) — this run spanned an edit; do not attribute its verdicts to either tree without re-running on a still one\n' \
    "$PIN_TREE_START" "$PIN_TREE_END"
fi

echo
echo "PASS=$PASS FAIL=$FAIL SKIP=$SKIPS"
[ "$FAIL" -eq 0 ]
