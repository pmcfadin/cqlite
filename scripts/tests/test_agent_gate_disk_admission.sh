#!/usr/bin/env bash
# Regression test for issue #3755: the FULL gate's DISK ADMISSION is evaluated at
# LAUNCH and RE-EVALUATED AT SLOT GRANT, using ONE predicate.
#
# THE DEFECT. A full gate admitted with 167G free can sit an hour in the #1825 queue
# and begin building at 30G — the whole queue wait wasted, the build aborting into a
# floor while still HOLDING the slot a peer could have used. An admission test taken
# at launch says nothing about the moment the resource is actually consumed, which is
# slot grant.
#
# WHAT IS EXERCISED, and how it is made evidence rather than a proxy:
#
#   (a) AC5 — a REAL agent-gate.sh process that measures ABOVE the bar at launch,
#       GENUINELY QUEUES behind a peer holding the only slot, and measures BELOW the
#       bar when the slot is granted, must REFUSE: exit non-zero, emit the named
#       `disk-admission: FAIL-CLOSED (#3755)` line + `RESULT: FAIL`, and NEVER BEGIN
#       WORK. The never-began-work half is asserted AFFIRMATIVELY, by a differential:
#       the same harness with readings that stay high DOES reach its work phase and
#       drops the stub's "I am working" marker, and the refusing run never drops it.
#       A bare non-zero exit would prove nothing — an unrelated breakage produces the
#       same exit code — so every negative case here is paired with that control.
#
#   (b) The bar's SOURCE token (default|pinned|invalid|clamped), the #3414
#       `cpu-budget:` idiom: an UNSET variable and a MIS-SET one are different
#       operational facts and `${VAR:-40}` renders them identically.
#
#   (c) UNMEASURED (df absent / df failing / df output unparsable) is DECLARED in the
#       emitted line and NON-FATAL at both moments — never a silent permissive branch.
#
#   (d) The LAUNCH evaluation is ADVISORY: a run reading BELOW the bar at launch and
#       ABOVE it at slot grant PROCEEDS. That asymmetry is deliberate (a low launch
#       reading can be freed by the very peer gate we are about to queue behind), so
#       it is pinned rather than left to be "simplified" later.
#
# HOW THE READINGS ARE DRIVEN — a PATH-shim `df`, never a seam in the shipped gate.
# Doctrine forbids a test-only override in agent-gate.sh (an override is settable by
# the party it constrains). So this test puts a scripted `df` ahead of the real one on
# the child gate's PATH, exactly as the feature-matrix annotation guard puts a
# recording `cargo` there. The gate is UNMODIFIED for testing.
#
# The vehicle is the gate's existing test-only stub mode (CQLITE_GATE_STUB_RUNDIR,
# #1825): it acquires a REAL slot through the REAL acquire_gate_slot, drops a per-PID
# marker while "working", sleeps, and exits without running a component. It is reached
# BEFORE the #3544 component-set pre-flight, so no case here touches the network.
#
# Run standalone:   bash scripts/tests/test_agent_gate_disk_admission.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
SKIP=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
# A control that cannot RUN on this host is reported, never counted as a pass: a green
# derived from a control's absence is the shape this repo's doctrine exists to forbid.
skip() { printf 'skip - %s\n' "$1"; SKIP=$((SKIP + 1)); }

# ---------------------------------------------------------------------------
# PORTABLE DIGEST (roborev job 392) — following `_tree_digest_file`'s in-repo ladder
# rather than inventing one: sha256sum -> shasum -> `git hash-object`, with the same
# rationale, that a guard must never go inert for want of a hashing tool.
#
# THE DEFECT IT REPLACES. `sha256sum` is GNU-only, and macOS with Python — a supported
# configuration — ships `shasum` instead. There, BOTH the before and the after digest came
# out EMPTY and compared EQUAL, so the immutability assertions PASSED having measured
# nothing: a vacuous pass wearing a comparison's clothes, and the same host-dependent
# regression class as round 13's `$HOME` finding. So an unavailable tool is a NAMED REFUSAL
# below, never an empty string.
DA_SHA_TOOL=none
if command -v sha256sum >/dev/null 2>&1;   then DA_SHA_TOOL=sha256sum
elif command -v shasum >/dev/null 2>&1;    then DA_SHA_TOOL=shasum
elif command -v git >/dev/null 2>&1;       then DA_SHA_TOOL=git-hash-object
fi
# da_digest: hash STDIN. Prints the digest and returns 0, or prints nothing and returns 1 —
# so a caller can never mistake "could not measure" for a value.
da_digest() {
  local d
  case "$DA_SHA_TOOL" in
    sha256sum)       d=$(sha256sum | awk '{print $1}') ;;
    shasum)          d=$(shasum -a 256 | awk '{print $1}') ;;
    git-hash-object) d=$(git --no-optional-locks hash-object --no-filters --stdin) ;;
    *) return 1 ;;
  esac
  [ -n "$d" ] || return 1
  printf '%s' "$d"
}

# df_operands <case-label>: the OPERAND of each `df -Pk <path>` the shim received, one per
# line, in call order. The shim answers regardless of what it is asked, so without this the
# suite pins what was RENDERED and not what was MEASURED — and "measure the right
# filesystem" is the property most of this issue has been about.
df_operands() {
  sed -n 's/^call [0-9]*: -Pk //p' "$tmp/$1.dfstate.log" 2>/dev/null
}
# df_operands_all <case-label> <expected>: rc 0 when at least one df call was made AND
# every one of them received <expected>. Zero calls is a FAILURE, not a pass: a run that
# measured nothing must never satisfy "every measurement was of the right path".
df_operands_all() {
  local got n=0 bad=0 o
  got=$(df_operands "$1")
  while IFS= read -r o; do
    [ -n "$o" ] || continue
    n=$((n + 1))
    [ "$o" = "$2" ] || bad=$((bad + 1))
  done <<EOF_OPS
$got
EOF_OPS
  [ "$n" -ge 1 ] && [ "$bad" -eq 0 ]
}

# df_calls <case-label>: how many times the shim was invoked by that run. An integer
# always, never empty — "measured once" and "measured twice" is the fact under test.
df_calls() {
  local n; n=$(cat "$tmp/$1.dfstate" 2>/dev/null || printf '0')
  case "$n" in ''|*[!0-9]*) n=0 ;; esac
  printf '%s' "$n"
}

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP - no python3 on PATH (the #1825 slot cap this test drives needs it)"
  exit 0
fi

# ---------------------------------------------------------------------------
# HOW A HOSTILE PYTHON IS PLANTED (roborev job 416, F2) — AND WHY IT CHANGED.
#
# The shipped probes run `python3 -I -S`, so `PYTHONPATH=<dir>` + `sitecustomize.py`
# — the technique cases AA/AC/AD used — reaches NOTHING. That is the POINT of the
# isolation fix, not an obstacle to testing it: this suite WAS the proof that the
# probes were exploitable.
#
# The replacement CONSTRUCTS the hostile interpreter itself: the prelude is prepended
# to the SHIPPED body IN SOURCE and both are handed to the real interpreter under the
# SHIPPED flags. Two properties the old technique did not have:
#   * the plants now exercise the body under the argv the GATE actually uses, instead
#     of under a non-isolated one that no longer models it;
#   * the shipped argv is left free to be isolated, so Case AF can assert that
#     isolation AS A DIFFERENTIAL rather than as an absence.
# The absolute interpreter path is captured HERE, before any shim can be prepended to
# PATH — a `command -v python3` from inside a shim finds the shim.
# ---------------------------------------------------------------------------
DA_REAL_PY=$(command -v python3 2>/dev/null || printf '/nonexistent/python3')
# ---------------------------------------------------------------------------
# da_farm <dest-root>: build a SYMLINK FARM of the real checkout at <dest-root>, with an
# empty `scripts/` ready for a substituted `agent-gate.sh`.
#
# WHY A FARM AND NOT A LONE COPY. `agent-gate.sh` does `cd "$(dirname "$0")/.."` and takes
# REPO_ROOT from there, so a copy dropped in $TMPDIR resolves REPO_ROOT=/tmp: `cargo
# metadata` fails, `scripts/lib/gate_slot_daemon.py` is missing so the cap never engages,
# and the run never reaches df — a mutant that CANNOT exhibit the behaviour it is the
# control for, i.e. a control that passes for the wrong reason. Measured, twice: case T hit
# it first (zero df calls), and Case AF hit it again (`target-dir-lockfile-stale-or-
# metadata-failed` + `slot daemon missing`).
#
# ONE definition, because two hand-rolled copies of this is two places for it to go subtly
# wrong — and the omission that broke each attempt was a different entry each time.
# Nothing is written into the worktree: a suite that runs inside `tooling-tests` must never
# mutate the tree its own gate is certifying (#2926).
# ---------------------------------------------------------------------------
DA_REPO=$(cd "$SCRIPT_DIR/../.." && pwd)
da_farm() {
  local dest="$1" e b
  mkdir -p "$dest/scripts"
  for e in "$DA_REPO"/* "$DA_REPO"/.[!.]*; do
    [ -e "$e" ] || continue
    b=$(basename "$e"); [ "$b" = scripts ] && continue
    ln -s "$e" "$dest/$b" 2>/dev/null || true
  done
  for e in "$DA_REPO"/scripts/* "$DA_REPO"/scripts/.[!.]*; do
    [ -e "$e" ] || continue
    b=$(basename "$e"); [ "$b" = agent-gate.sh ] && continue
    ln -s "$e" "$dest/scripts/$b" 2>/dev/null || true
  done
  return 0
}
# da_run_probe <prelude-or-empty> <body-file> <target>: run the shipped probe body under
# the shipped isolation flags, with a hostile prelude prepended. Prints the payload.
da_run_probe() {
  local pre="$1" body="$2" tgt="$3"
  if [ -n "$pre" ]; then
    "$DA_REAL_PY" -I -S -c "$(cat "$pre")
$(cat "$body")" "$tgt" 2>/dev/null
  else
    "$DA_REAL_PY" -I -S -c "$(cat "$body")" "$tgt" 2>/dev/null
  fi
}
# ---------------------------------------------------------------------------
# da_py_shim <dir> <prelude-file>: install a PATH `python3` that runs the gate's
# WRITE PROBE under a hostile prelude and passes every other python3 call through.
#
# WHY NOT `PYTHONPATH` + `sitecustomize.py`, which cases AA/AC/AD used: the shipped
# probes now run `python3 -I -S`, so that technique reaches nothing — and that is the
# POINT of the isolation fix, not an obstacle to testing it. This replacement
# CONSTRUCTS the hostile interpreter itself, so the full ability to force any errno is
# retained while the shipped argv stays free to be isolated (which Case AF then
# asserts as a differential). Strictly better coverage, not a workaround.
#
# The probe is recognised by its own artifact marker, NEVER by an argument count: the
# shipped argv gained two flags this round, and a count-keyed shim goes silently inert
# — reporting a green that measured nothing.
# ---------------------------------------------------------------------------
da_py_shim() {
  local dir="$1" prelude="$2"
  mkdir -p "$dir"
  cat > "$dir/python3" <<SHIM
#!/usr/bin/env bash
REAL='$DA_REAL_PY'
PRELUDE='$prelude'
orig=("\$@")
body=""; seen=0
declare -a rest=()
while [ "\$#" -gt 0 ]; do
  if [ "\$1" = -c ]; then
    shift; body="\${1-}"; shift; rest=("\$@"); seen=1; break
  fi
  shift
done
if [ "\$seen" -eq 1 ]; then
  case "\$body" in
    *agent-gate-writeprobe*)
      exec "\$REAL" -I -S -c "\$(cat "\$PRELUDE")
\$body" \${rest[@]+"\${rest[@]}"} ;;
  esac
fi
exec "\$REAL" \${orig[@]+"\${orig[@]}"}
SHIM
  chmod +x "$dir/python3"
}

# da_py_hang_shim <dir>: install a PATH `python3` that HANGS on the write probe and
# delegates everything else, so a real bound fires on exactly the call under test.
#
# ONE definition replacing two hand-rolled copies, each of which keyed on
# `[ $# -eq 3 ] && [ $1 = -c ]`. The shipped argv gained two isolation flags this round, so
# that predicate stopped matching ANYTHING: both shims would have gone SILENTLY INERT and
# their cases would have reported greens while the gate ran its probe normally. Keyed on the
# payload marker instead, which cannot go inert that way.
da_py_hang_shim() {
  local dir="$1"
  mkdir -p "$dir"
  cat > "$dir/python3" <<HANGSHIM
#!/usr/bin/env bash
orig=("\$@")
body=""
while [ "\$#" -gt 0 ]; do
  if [ "\$1" = -c ]; then shift; body="\${1-}"; break; fi
  shift
done
case "\$body" in
  *agent-gate-writeprobe*) sleep 120 ;;
esac
exec "$DA_REAL_PY" \${orig[@]+"\${orig[@]}"}
HANGSHIM
  chmod +x "$dir/python3"
}

# ---------------------------------------------------------------------------
# ENVIRONMENT ISOLATION (roborev job 373, High) — THE ONE NAMED LIST.
#
# THE DEFECT. This suite runs inside `tooling-tests`, i.e. as a CHILD of a gate — and since
# the #3755 pin that parent EXPORTS `CARGO_TARGET_DIR`. It is highest-precedence, so it
# silently overrode exactly the mechanisms the resolution cases exist to exercise: six cases
# failed, INCLUDING the operand-guard control, which reddened a CORRECT gate. Measured:
#   standalone                                   -> 165 passed, 0 failed
#   CARGO_TARGET_DIR=<repo>/target <same suite>  ->  59 passed, 6 FAILED
# Standalone-green is not `tooling-tests`-green. That is the general shape: a suite whose
# verdict depends on how it was launched is not a suite, and this one could only be trusted
# when launched by hand.
#
# A DENY-LIST WAS THE WRONG SHAPE, AND CLOSING ONE AXIS AT A TIME PROVED IT (job 389).
# This suite's isolation has now been incomplete in THREE different directions — the
# variable list, the inherited pin, and `$HOME`. `unset CARGO_HOME` does NOT isolate cargo
# configuration: cargo then reads `$HOME/.cargo/config.toml`, so a caller with a legitimate
# user-level `build.target-dir` made the k-default case resolve elsewhere and this
# GATE-WIRED suite failed as a function of whose box it ran on. Measured, both directions,
# on one planted user-level config:
#   unset CARGO_HOME   -> target_directory = <planted>/user-level     (the defect)
#   CARGO_HOME=<empty> -> target_directory = <repo>/target            (the fix)
#
# EVERYTHING CARGO READS FOR `build.target-dir`, enumerated so the next reader does not have
# to rediscover it one axis at a time:
#   1. env `CARGO_TARGET_DIR`               highest precedence          -> CLEARED below
#   2. env `CARGO_BUILD_TARGET_DIR`         the `[build]` env spelling  -> CLEARED below
#   3. `$CARGO_HOME/config.toml`            CARGO_HOME defaults to `$HOME/.cargo`, which is
#                                           why UNSETTING it isolates nothing -> SET below
#                                           to a fresh EMPTY dir, which cargo reads and
#                                           finds nothing in (no `$HOME` fallback happens
#                                           when CARGO_HOME is set)
#   4. `.cargo/config.toml` in the cwd and EVERY ANCESTOR — NOT isolable without moving the
#                                           cwd, which the gate requires to be the repo. So
#                                           k-default does not HARD-CODE `<repo>/target`; it
#                                           asks cargo, under this same environment, what it
#                                           would resolve unaided. An ancestor config then
#                                           moves BOTH the expectation and the gate, and the
#                                           case still asserts the property that matters:
#                                           the gate resolves what CARGO resolves.
# The positive construction is the durable form; the cleared list below is the part of it
# that has to be expressed as absence.
#
# WHAT IS CLEARED, and why each is here — a case's MEANING depends on it:
#   CARGO_TARGET_DIR / CARGO_BUILD_TARGET_DIR
#       two of the three inputs to cargo's target-dir precedence; cases K/K-low/M/U are
#       ABOUT that precedence, so an inherited one decides the answer before the case does
#   CQLITE_GATE_MIN_FREE_GB
#       the bar's source token is decided from whether it is SET
#   CQLITE_GATE_DISABLE_CAP
#       an inherited 1 turns every queue case into a no-cap case with no other symptom
#   AGENT_GATE_ALLOW_MISSING_FIXTURES
#       flips WHICH early terminal Case J reaches
#   AGENT_GATE_SUMMARY_FILE
#       every case pins its own; scrubbed so a missed one cannot clobber a caller's file
#
# WHAT IS DELIBERATELY *NOT* CLEARED, stated so the omission is a decision and not a gap:
#   AGENT_GATE_PARENT_RUN_ID   under `tooling-tests` these child gates ARE nested, and #2874
#                              wants them to say so; clearing it would make a child claim to
#                              be a top-level gate, which is a lie in the artifact
#   AGENT_GATE_FM_DIR          the feature-matrix sidecar dir; no assertion here depends on
#                              it, and it is the parent's business
#   CARGO_HOME                 deliberately NOT in the cleared list: it is SET (see above),
#                              and `env -u`-ing it would drop the isolation for every child
#                              that does not set its own, sending cargo back to
#                              `$HOME/.cargo`. A case that needs its own still wins, because
#                              its assignment comes after these flags.
#   RUSTFLAGS / RUSTC_WRAPPER / CARGO_INCREMENTAL / CARGO_BUILD_JOBS
#                              build-time only; nothing here builds, and `cargo metadata`
#                              resolution does not read them
#   PATH                       the shims deliberately PREPEND to the caller's
# Derived from the gate's own `export` sweep, not guessed: those four plus PATH and
# CQLITE_DATASETS_ROOT (which every case that cares passes explicitly) are everything
# agent-gate.sh exports.
DA_ISOLATE=(CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR
            CQLITE_GATE_MIN_FREE_GB CQLITE_GATE_DISABLE_CAP
            AGENT_GATE_ALLOW_MISSING_FIXTURES AGENT_GATE_SUMMARY_FILE)
# In-process, so the suite's OWN cargo calls (the Case U no-op demonstration) are clean too.
unset "${DA_ISOLATE[@]}"
# ...and for every child, so a future edit that re-exports one cannot reach a case. Placed
# BEFORE a case's own assignments, which `env` therefore still wins.
DA_ENV_U=()
for _da_v in "${DA_ISOLATE[@]}"; do DA_ENV_U+=(-u "$_da_v"); done

tmp=$(mktemp -d "${TMPDIR:-/tmp}/gate-disk-adm.XXXXXX")
cleanup() { kill $(jobs -p) 2>/dev/null; rm -rf "$tmp"; }
trap cleanup EXIT

# THE POSITIVELY-CONSTRUCTED HALF of the isolation (job 389): a fresh EMPTY cargo home,
# exported so every child inherits it. Cargo reads `$CARGO_HOME/config.toml`, finds nothing,
# and does NOT fall back to `$HOME/.cargo` — which is exactly what `unset CARGO_HOME` failed
# to achieve. Verified not to break the probe: `cargo metadata --no-deps` resolves normally
# under an empty CARGO_HOME (the lockfile is already present, so no registry is consulted),
# and `cargo nextest` still resolves through PATH rather than `$CARGO_HOME/bin`.
DA_CARGO_HOME="$tmp/isolated-cargo-home"
mkdir -p "$DA_CARGO_HOME"
export CARGO_HOME="$DA_CARGO_HOME"

# ---------------------------------------------------------------------------
# The PATH-shim `df`. Each invocation consumes the NEXT line of $DF_SHIM_SCRIPT
# (the last line repeats once exhausted, so a case need only script the readings
# it cares about) and renders it as POSIX `df -Pk` output:
#   * an integer            -> that many AVAILABLE KiB
#   * FAIL                  -> exit 1 (df ran and could not answer)
#   * NOTFOUND              -> exit 127, which is EXACTLY what a shell reports for an
#                              ABSENT command, so this drives the df-unavailable branch
#                              on the same observable a df-less PATH would produce
#   * GARBAGE               -> well-formed columns with a NON-NUMERIC Available
#   * RAW <data-line>       -> that EXACT data line, verbatim, under the standard
#                              header — how the space-bearing / capacity-anchor cases
#                              deliver a payload no field-index parse can read
# A per-run state file keeps the counter, so concurrent runs never share one.
# ---------------------------------------------------------------------------
mkdir -p "$tmp/shim"
# A PATH-shim `cargo` used ONLY by the cases that need the resolved target dir to CHANGE
# between the two measurements. It lives in its own directory so the default cases keep
# using the real cargo (Case K's whole point).
mkdir -p "$tmp/cargoshim"
# NON-`metadata` invocations are delegated to the REAL cargo and consume NO scripted
# line. Found the hard way: the gate's accelerator detection runs `cargo nextest
# --version` at startup, which ate script line 1, so BOTH resolutions read the same
# value and the "subject moved" case silently became a "subject unchanged" case — a
# control that did not control, the third instance of that family on this branch.
_REAL_CARGO=$(command -v cargo 2>/dev/null || printf '/nonexistent/cargo')
cat > "$tmp/cargoshim/cargo" <<CSHIM
#!/usr/bin/env bash
if [ "\${1:-}" != metadata ]; then exec "$_REAL_CARGO" "\$@"; fi
CSHIM
cat >> "$tmp/cargoshim/cargo" <<'CSHIM'
n=$(cat "$CARGO_SHIM_STATE" 2>/dev/null || printf '0')
case "$n" in ''|*[!0-9]*) n=0 ;; esac
n=$((n + 1)); printf '%s' "$n" > "$CARGO_SHIM_STATE"
val=$(sed -n "${n}p" "$CARGO_SHIM_SCRIPT" 2>/dev/null)
[ -n "$val" ] || val=$(tail -n 1 "$CARGO_SHIM_SCRIPT" 2>/dev/null)
printf '{"target_directory":"%s","packages":[],"workspace_members":[],"version":1}\n' "$val"
CSHIM
chmod +x "$tmp/cargoshim/cargo"

cat > "$tmp/shim/df" <<'SHIM'
#!/usr/bin/env bash
n=$(cat "$DF_SHIM_STATE" 2>/dev/null || printf '0')
case "$n" in ''|*[!0-9]*) n=0 ;; esac
n=$((n + 1))
printf '%s' "$n" > "$DF_SHIM_STATE"
printf 'call %s: %s\n' "$n" "$*" >> "$DF_SHIM_STATE.log"
val=$(sed -n "${n}p" "$DF_SHIM_SCRIPT" 2>/dev/null)
[ -n "$val" ] || val=$(tail -n 1 "$DF_SHIM_SCRIPT" 2>/dev/null)
case "$val" in
  FAIL) exit 1 ;;
  NOTFOUND) exit 127 ;;
  'RAW '*)
    printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
    printf '%s\n' "${val#RAW }"
    exit 0 ;;
  GARBAGE)
    printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
    printf '/dev/shim 999999999 1 not-a-number 1%% /shimfs\n'
    exit 0 ;;
esac
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/shim 999999999 1 %s 1%% /shimfs\n' "$val"
SHIM
chmod +x "$tmp/shim/df"

KIB_PER_GIB=1048576
gib_kib() { printf '%s' "$(( $1 * KIB_PER_GIB ))"; }

# df_script <name> <reading...>: write a shim script file, return its path via echo.
df_script() {
  local name="$1"; shift
  local f="$tmp/df-$name.script"
  : > "$f"
  local r
  for r in "$@"; do printf '%s\n' "$r" >> "$f"; done
  printf '%s' "$f"
}

# marker_count <rundir>: how many stub runs are advertising "I am working" right now.
marker_count() {
  local d="$1" c=0 f
  for f in "$d"/holding.*; do
    [ -e "$f" ] && c=$(( c + 1 ))
  done
  printf '%s' "$c"
}

# run_stub_gate <case> <df-script> [env assignments...] -> backgrounds a stub gate.
# Sets, for the caller: RS_PID, RS_RUNDIR, RS_SUMMARY, RS_ERR.
RS_PATH_PREFIX=""
# RS_GATE names the script under test. It defaults to the SHIPPED gate and is overridden
# ONLY by Case AF, which needs a scratch copy with the isolation flags stripped as its
# positive control: a "verdict unchanged" assertion is indistinguishable from a test that
# never reached the code unless the SAME plant is shown to flip a build that lacks the fix.
# A dedicated variable, for the reason RS_PATH_PREFIX is one — `env` applies assignments
# left to right and this function's own come last.
RS_GATE=""
run_stub_gate() {
  local case_name="$1" script="$2"; shift 2
  RS_RUNDIR="$tmp/$case_name.run"; mkdir -p "$RS_RUNDIR"
  RS_SUMMARY="$tmp/$case_name.summary.txt"
  RS_ERR="$tmp/$case_name.err"
  # RS_PATH_PREFIX prepends to the child's PATH. It is a dedicated variable rather than a
  # `PATH=` in "$@" because `env` applies assignments LEFT TO RIGHT and the function's own
  # PATH= comes last, so a caller-supplied one is silently overridden — which is exactly
  # how the k-nocargo case first ran against the REAL cargo and reported a resolution it
  # was written to prove impossible.
  env "${DA_ENV_U[@]}" "$@" \
    PATH="${RS_PATH_PREFIX:+$RS_PATH_PREFIX:}$tmp/shim:$PATH" \
    DF_SHIM_SCRIPT="$script" \
    DF_SHIM_STATE="$tmp/$case_name.dfstate" \
    AGENT_GATE_SUMMARY_FILE="$RS_SUMMARY" \
    CQLITE_GATE_STUB_RUNDIR="$RS_RUNDIR" \
    CQLITE_GATE_POLL_SECS=0.3 \
    bash "${RS_GATE:-$GATE}" >"$tmp/$case_name.out" 2>"$RS_ERR" &
  RS_PID=$!
}

# watch_until_exit <pid> <rundir> <timeout_s>: poll the rundir while <pid> runs, then
# reap it. Sets WX_STATUS (exit status), WX_MARKERS (the MAX number of "I am working"
# markers ever observed — the AFFIRMATIVE evidence for "did this run begin its work
# phase") and WX_TIMEDOUT.
#
# Sets GLOBALS rather than printing: a `$( ... )` capture runs in a SUBSHELL, where
# `wait <pid>` cannot reap a job of the PARENT shell and silently yields 127.
#
# THE TIMEOUT IS REAL — THERE IS NO UNBOUNDED `wait` ON ANY PATH (roborev job 323).
# A version that stopped POLLING at the deadline and then called a bare `wait` would
# hang FOREVER on a deadlocked gate, and this file runs inside `tooling-tests`, i.e. in
# the gate of record for every lane on the fleet. A hang there is worse than a failure:
# it burns the machine-wide slot with no verdict — which is the exact resource-waste
# #3755 exists to remove, reintroduced by its own test. So expiry is detected
# explicitly, the child is terminated, reaped on a BOUNDED path, and reported as a
# DISTINCT TIMEOUT status (124, the `timeout(1)` convention) — never a silent pass and
# never a generic FAIL.
#
# The signal goes to THE PID WE STILL HOLD, never to a process GROUP (roborev job 279):
# once bash has reaped the leader that pgid can be recycled, and on a four-lane box the
# group most likely to inherit it is a PEER LANE'S GATE. And no `wait` is issued after
# the kill: a process wedged in uninterruptible sleep would make even that call
# unbounded, so the reap is a bounded poll and a survivor is left to the EXIT trap.
# DEADLINES ARE GENEROUS ON PURPOSE (120s for a child that normally exits in ~2s). They
# bound a HANG; they are not a performance assertion, and nothing here asserts a duration.
# Six cases still carried a 30s deadline from before the probe grew a `cargo metadata` call
# and a writability probe, and one of them (`e-above`) TIMED OUT on a loaded box while the
# same case passed in the next run — a flake in a suite that runs inside `tooling-tests`,
# where a co-scheduled gate is the normal condition rather than the exception.
WX_STATUS=0
WX_MARKERS=0
WX_TIMEDOUT=0
watch_until_exit() {
  local pid="$1" rundir="$2" timeout="$3"
  local deadline=$(( $(date +%s) + timeout )) max=0 c expired=0 i=0
  WX_TIMEDOUT=0
  while :; do
    c=$(marker_count "$rundir")
    [ "$c" -gt "$max" ] && max="$c"
    kill -0 "$pid" 2>/dev/null || break
    if [ "$(date +%s)" -ge "$deadline" ]; then expired=1; break; fi
    sleep 0.05
  done
  c=$(marker_count "$rundir"); [ "$c" -gt "$max" ] && max="$c"
  WX_MARKERS="$max"
  if [ "$expired" -eq 0 ]; then
    # Bounded by construction: the loop only leaves here once `kill -0` says the child
    # is gone, so bash already holds its status and `wait` returns immediately.
    wait "$pid"; WX_STATUS=$?
    return 0
  fi
  WX_TIMEDOUT=1
  WX_STATUS=124
  kill -TERM "$pid" 2>/dev/null
  i=0
  while [ "$i" -lt 40 ] && kill -0 "$pid" 2>/dev/null; do sleep 0.05; i=$((i + 1)); done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null
    i=0
    while [ "$i" -lt 40 ] && kill -0 "$pid" 2>/dev/null; do sleep 0.05; i=$((i + 1)); done
  fi
  return 0
}

# assert_no_timeout <label>: a TIMEOUT is its own named failure. Called after every
# watch_until_exit, so a hung child is reported as a hang rather than surfacing as a
# confusing cascade of value assertions against a run that never finished.
assert_no_timeout() {
  if [ "$WX_TIMEDOUT" -eq 0 ]; then
    return 0
  fi
  bad "TIMEOUT: $1 — the child gate did not exit within its deadline; it was terminated"
  return 1
}

# grep_line <file> <pattern>: print the first matching line (empty when none).
grep_line() { grep -m1 -E "$2" "$1" 2>/dev/null; }

# ---------------------------------------------------------------------------
# Self-check of the harness's own timeout path (roborev job 323, finding 2).
#
# This case IS the positive control for boundedness: under the pre-fix helper — which
# stopped POLLING at the deadline and then called a bare `wait` — it would hang
# FOREVER, so the fact that this file reaches its final tally at all is the property
# being demonstrated. Deliberately no elapsed-time assertion: "it returned" is the
# observable, and a wall-clock threshold in a correctness path is a flake generator.
# ---------------------------------------------------------------------------
sleep 300 &
_hang_pid=$!
mkdir -p "$tmp/hang.run"
watch_until_exit "$_hang_pid" "$tmp/hang.run" 1
if [ "$WX_TIMEDOUT" -eq 1 ] && [ "$WX_STATUS" -eq 124 ]; then
  ok "harness: a child that outlives its deadline is reported as a DISTINCT TIMEOUT (status 124)"
else
  bad "harness: a hung child was not reported as a timeout (timedout=$WX_TIMEDOUT status=$WX_STATUS)"
fi
if kill -0 "$_hang_pid" 2>/dev/null; then
  bad "harness: the timed-out child is still alive — the deadline terminated nothing"
  kill -KILL "$_hang_pid" 2>/dev/null
else
  ok "harness: the timed-out child was terminated (by pid, never by process group)"
fi
wait "$_hang_pid" 2>/dev/null

HIGH=$(gib_kib 200)
LOW=$(gib_kib 10)

# ===========================================================================
# Case A (AC5): ABOVE the bar at launch, QUEUED behind a peer, BELOW the bar at
# slot grant -> refuses, releases the slot, never begins work.
# ===========================================================================
a_slots="$tmp/a-slots"
peer_script=$(df_script a-peer "$HIGH")
run_stub_gate a-peer "$peer_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=15
a_peer_pid=$RS_PID; a_peer_run=$RS_RUNDIR

# The peer must actually HOLD the only slot before the subject launches, AND must go
# on holding it for longer than the subject's own startup takes on a loaded box — or
# the subject finds a free slot, never queues, and this case silently degrades into
# case B: a green that measured the wrong thing. Hence the generous peer hold; the
# subject exits the moment the slot is granted, so the hold only bounds the wait.
a_peer_holding=0
a_deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$a_deadline" ]; do
  [ "$(marker_count "$a_peer_run")" -ge 1 ] && { a_peer_holding=1; break; }
  sleep 0.1
done
if [ "$a_peer_holding" -eq 1 ]; then
  ok "AC5 setup: peer holds the only slot (N=1) before the subject launches"
else
  bad "AC5 setup: peer never acquired the slot — the subject would not have queued"
fi

subj_script=$(df_script a-subj "$HIGH" "$LOW")
run_stub_gate a-subj "$subj_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=4
a_subj_pid=$RS_PID; a_subj_run=$RS_RUNDIR; a_subj_sum=$RS_SUMMARY; a_subj_err=$RS_ERR

watch_until_exit "$a_subj_pid" "$a_subj_run" 180; a_status=$WX_STATUS; a_markers=$WX_MARKERS
assert_no_timeout "AC5 subject"
wait "$a_peer_pid" 2>/dev/null

if grep -q 'waiting for gate slot' "$a_subj_err" 2>/dev/null; then
  ok "AC5: the subject GENUINELY QUEUED for the slot (not a free-slot fast path)"
else
  bad "AC5: no queue notice on the subject's stderr — it did not queue behind the peer"
fi
if [ "$a_status" -ne 0 ]; then
  ok "AC5: below-bar at slot grant REFUSES (exit $a_status)"
else
  bad "AC5: below-bar at slot grant exited 0 — the gate was admitted into a floor"
fi
if [ "$a_markers" -eq 0 ]; then
  ok "AC2/AC5: the refusing run NEVER began its work phase (0 work markers observed)"
else
  bad "AC2/AC5: the refusing run began working ($a_markers marker(s) observed)"
fi
a_line=$(grep_line "$a_subj_sum" '^disk-admission: ')
case "$a_line" in
  'disk-admission: FAIL-CLOSED (#3755)'*)
    ok "AC4: distinct NAMED outcome in the SUMMARY: ${a_line:0:60}…" ;;
  '') bad "AC3/AC4: no disk-admission: line in the refusal SUMMARY ($a_subj_sum)" ;;
  *)  bad "AC4: refusal SUMMARY carries the wrong verdict: $a_line" ;;
esac
# The shared assembly is IDEMPOTENT: this block's builder passes the line explicitly, so
# a non-dropping assembly would emit it TWICE.
a_count=$(grep -c '^disk-admission: ' "$a_subj_sum" 2>/dev/null || printf '0')
if [ "$a_count" -eq 1 ]; then
  ok "AC3: exactly ONE disk-admission: line in the block (the shared assembly de-duplicates)"
else
  bad "AC3: expected exactly 1 disk-admission: line, found $a_count"
fi
# AC3: value observed, bar applied, and BOTH moments named.
for needle in 'post-slot 10.0GiB' 'bar 40GiB(default)' 'launch 200.0GiB' 'evaluated 2x'; do
  case "$a_line" in
    *"$needle"*) ok "AC3: refusal line states '$needle'" ;;
    *)           bad "AC3: refusal line omits '$needle': $a_line" ;;
  esac
done
case "$a_line" in
  *'slot RELEASED'*) ok "AC2: the refusal line reports the slot RELEASED" ;;
  *)                 bad "AC2: the refusal line does not report a slot release: $a_line" ;;
esac
# AC4: the terminal RESULT stays the pollable FAIL — never a new token that would
# break the mandated `grep -qE 'RESULT: (PASS|FAIL)'` completion probe (#3041).
if grep -qx 'RESULT: FAIL' "$a_subj_sum" 2>/dev/null; then
  ok "AC4: RESULT: FAIL (the #3041 completion probe still fires on a refusal)"
else
  bad "AC4: refusal SUMMARY lacks an exact 'RESULT: FAIL' line"
  grep -E '^RESULT:' "$a_subj_sum" 2>/dev/null
fi
if grep -q '^refusal: disk admission (#3755) — refused at SLOT GRANT;' "$a_subj_sum" 2>/dev/null; then
  ok "AC4: the refusal is NAMED on its own refusal: line, and NAMES THE MOMENT"
else
  bad "AC4: no named 'refusal: disk admission (#3755) — refused at SLOT GRANT' line"
  grep -m1 '^refusal:' "$a_subj_sum" 2>/dev/null
fi
# AC2, behavioural half: the slot is usable by a follow-up run immediately after.
follow_script=$(df_script a-follow "$HIGH")
run_stub_gate a-follow "$follow_script" \
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; f_status=$WX_STATUS; f_markers=$WX_MARKERS
assert_no_timeout "AC2 follow-up run"
if [ "$f_status" -eq 0 ] && [ "$f_markers" -ge 1 ]; then
  ok "AC2: the released slot is immediately usable by a follow-up run"
else
  bad "AC2: follow-up run did not get the slot (exit $f_status, markers $f_markers)"
fi

# ===========================================================================
# Case B (POSITIVE CONTROL): the SAME harness with readings that stay ABOVE the
# bar proceeds past the check and DOES begin work. Without this, case A's
# non-zero exit is not evidence — any breakage produces the same exit code.
# ===========================================================================
b_slots="$tmp/b-slots"
b_peer_script=$(df_script b-peer "$HIGH")
run_stub_gate b-peer "$b_peer_script" \
  CQLITE_GATE_SLOTS_DIR="$b_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=15
b_peer_pid=$RS_PID; b_peer_run=$RS_RUNDIR
b_deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$b_deadline" ]; do
  [ "$(marker_count "$b_peer_run")" -ge 1 ] && break
  sleep 0.1
done
b_subj_script=$(df_script b-subj "$HIGH" "$HIGH")
run_stub_gate b-subj "$b_subj_script" \
  CQLITE_GATE_SLOTS_DIR="$b_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=3
b_subj_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 180; b_status=$WX_STATUS; b_markers=$WX_MARKERS
assert_no_timeout "positive control"
wait "$b_peer_pid" 2>/dev/null
if [ "$b_status" -eq 0 ] && [ "$b_markers" -ge 1 ]; then
  ok "CONTROL: above-bar at BOTH moments proceeds and DOES begin work (exit 0, $b_markers marker(s))"
else
  bad "CONTROL: above-bar run did not proceed (exit $b_status, markers $b_markers)"
fi
b_line=$(grep_line "$b_subj_err" '^agent-gate: disk-admission: ')
case "$b_line" in
  *'disk-admission: PASS'*'evaluated 2x'*'launch 200.0GiB'*'post-slot 200.0GiB'*)
    ok "AC3: the PASS line names BOTH evaluations affirmatively" ;;
  *) bad "AC3: PASS line malformed or missing: ${b_line:-<none>}" ;;
esac

# ===========================================================================
# Case C: the LAUNCH evaluation is ADVISORY — below at launch, above at slot
# grant PROCEEDS (design point: a low launch reading can be freed by the very
# peer we are about to queue behind, so refusing there is a FALSE refusal).
# ===========================================================================
c_script=$(df_script c "$LOW" "$HIGH")
run_stub_gate c "$c_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/c-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
c_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; c_status=$WX_STATUS; c_markers=$WX_MARKERS
assert_no_timeout "launch-advisory case"
if [ "$c_status" -eq 0 ] && [ "$c_markers" -ge 1 ]; then
  ok "LAUNCH ADVISORY: below-at-launch/above-at-grant PROCEEDS (exit 0)"
else
  bad "LAUNCH ADVISORY: a low LAUNCH reading refused the run (exit $c_status)"
fi
c_line=$(grep_line "$c_err" '^agent-gate: disk-admission: ')
case "$c_line" in
  *'launch 10.0GiB(BELOW BAR)'*'post-slot 200.0GiB'*)
    ok "LAUNCH ADVISORY: the low launch reading is DECLARED in the line" ;;
  *) bad "LAUNCH ADVISORY: line does not declare the low launch reading: ${c_line:-<none>}" ;;
esac

# ===========================================================================
# Case D: UNMEASURED is DECLARED and NON-FATAL at both moments.
# ===========================================================================
run_unmeasured_case() {
  local label="$1" reading="$2" why="$3"
  local s; s=$(df_script "$label" "$reading")
  run_stub_gate "$label" "$s" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
  local err=$RS_ERR st mk line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; st=$WX_STATUS; mk=$WX_MARKERS
  assert_no_timeout "$label"
  if [ "$st" -eq 0 ] && [ "$mk" -ge 1 ]; then
    ok "UNMEASURED($why): non-fatal — the run proceeded and began work"
  else
    bad "UNMEASURED($why): the run was refused (exit $st, markers $mk)"
  fi
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"disk-admission: UNMEASURED ($why)"*'NOT APPLIED'*)
      ok "UNMEASURED($why): DECLARED in the line, bar NOT APPLIED" ;;
    *) bad "UNMEASURED($why): not declared: ${line:-<none>}" ;;
  esac
}
run_unmeasured_case d-fail    FAIL    df-failed
run_unmeasured_case d-garbage GARBAGE df-unparsable

# df ABSENT: a shell reports rc 127 for an absent command, so the NOTFOUND reading
# drives exactly the branch a df-less PATH would. The probe must ALSO not leak a
# `command not found` line onto the gate's own stderr — the minimal-PATH case in
# test_agent_gate_summary.sh reads any such line as a missing-tool defect.
d_script=$(df_script d-absent NOTFOUND)
run_stub_gate d-absent "$d_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/d-absent-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
d_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; d_status=$WX_STATUS; d_markers=$WX_MARKERS
assert_no_timeout "df-absent case"
d_line=$(grep_line "$d_err" '^agent-gate: disk-admission: ')
if [ "$d_status" -eq 0 ] && [ "$d_markers" -ge 1 ]; then
  ok "UNMEASURED(df-unavailable): non-fatal — the run proceeded and began work"
else
  bad "UNMEASURED(df-unavailable): refused the run (exit $d_status, markers $d_markers)"
fi
case "$d_line" in
  *'disk-admission: UNMEASURED (df-unavailable)'*'NOT APPLIED'*)
    ok "UNMEASURED(df-unavailable): DECLARED in the line, bar NOT APPLIED" ;;
  *) bad "UNMEASURED(df-unavailable): not declared: ${d_line:-<none>}" ;;
esac
if grep -q 'command not found' "$d_err" 2>/dev/null; then
  bad "UNMEASURED: the probe leaked 'command not found' onto the gate's stderr"
  grep -m3 'command not found' "$d_err"
else
  ok "UNMEASURED: no 'command not found' leaked onto the gate's stderr"
fi

# ===========================================================================
# Case E: the bar's SOURCE token (#3414 idiom). unset|pinned|invalid|clamped.
# ===========================================================================
bar_case() {
  local label="$1" expect="$2"; shift 2
  local s; s=$(df_script "$label" "$HIGH")
  run_stub_gate "$label" "$s" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1 "$@"
  local err=$RS_ERR st mk line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; st=$WX_STATUS; mk=$WX_MARKERS
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"bar $expect"*) ok "bar-source: $label -> 'bar $expect'" ;;
    *)               bad "bar-source: $label expected 'bar $expect', got: ${line:-<none>}" ;;
  esac
}
bar_case e-unset   '40GiB(default)'
bar_case e-pinned  '50GiB(pinned)'   CQLITE_GATE_MIN_FREE_GB=50
bar_case e-frac    '0.5GiB(pinned)'  CQLITE_GATE_MIN_FREE_GB=0.5
bar_case e-empty   '40GiB(invalid)'  CQLITE_GATE_MIN_FREE_GB=
bar_case e-nonnum  '40GiB(invalid)'  CQLITE_GATE_MIN_FREE_GB=abc
bar_case e-neg     '0GiB(clamped)'   CQLITE_GATE_MIN_FREE_GB=-5

# A pinned bar ABOVE the reading refuses even with no contention — the same
# predicate, the same disposition, reached without a queue.
e_script=$(df_script e-above "$HIGH")
run_stub_gate e-above "$e_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/e-above-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=2 CQLITE_GATE_MIN_FREE_GB=500
e_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; e_status=$WX_STATUS; e_markers=$WX_MARKERS
assert_no_timeout "pinned-bar refusal"
if [ "$e_status" -ne 0 ] && [ "$e_markers" -eq 0 ]; then
  ok "bar-source: a PINNED bar above the reading refuses and never begins work"
else
  bad "bar-source: pinned bar 500GiB did not refuse (exit $e_status, markers $e_markers)"
fi
if grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$e_sum" 2>/dev/null; then
  ok "bar-source: the pinned-bar refusal emits the same named line"
else
  bad "bar-source: pinned-bar refusal SUMMARY lacks the named line"
fi

# ===========================================================================
# Case F: --lite is EXEMPT — the cap exempts it, so the admission probe must not
# run for it either (it builds nothing that fills a disk the way a full gate does,
# and it is never queued). Asserted on the probe's own call log: ZERO df calls.
# ===========================================================================
f_state="$tmp/f.dfstate"
f_script=$(df_script f "$HIGH")
env "${DA_ENV_U[@]}" PATH="$tmp/shim:$PATH" DF_SHIM_SCRIPT="$f_script" DF_SHIM_STATE="$f_state" \
  AGENT_GATE_SUMMARY_FILE="$tmp/f.summary.txt" \
  CQLITE_GATE_STUB_RUNDIR="$tmp/f.run" CQLITE_GATE_STUB_SLEEP=1 \
  CQLITE_GATE_SLOTS_DIR="$tmp/f-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  bash "$GATE" --lite >"$tmp/f.out" 2>"$tmp/f.err"
f_calls=$(cat "$f_state" 2>/dev/null || printf '0')
case "$f_calls" in ''|*[!0-9]*) f_calls=0 ;; esac
if [ "$f_calls" -eq 0 ]; then
  ok "exemption: the admission probe made 0 df calls on a non-full-gate run"
else
  bad "exemption: the admission probe ran on a non-full-gate run ($f_calls df call(s))"
fi

# ===========================================================================
# Case G (roborev job 323, finding 1): the df parse is ANCHORED ON THE CAPACITY
# FIELD, so a source name or mount point containing SPACES cannot shift a column
# into $4 and admit a run that is below the bar.
#
# This is a FALSE-PASS class, not a cosmetic one: a shifted $4 lands on the USED
# value, which is large and NUMERIC, so a "is it a number" validation succeeds and
# the gate is ADMITTED BELOW THE FLOOR. Every negative case below therefore carries
# the PRE-FIX PARSE as a POSITIVE CONTROL — the defective one-liner is reproduced
# verbatim against the same payload and must be shown to yield a number that WOULD
# have cleared the bar. A test that merely passes after the fix does not establish
# that the defect was ever reachable.
# ===========================================================================
BAR_KIB=$(gib_kib 40)

# prefix_parse_admits <payload>: the PRE-FIX parse (`awk 'END { print $4 }'`),
# reproduced exactly. Exit 0 when it yields a numeric value at or above the 40GiB
# bar, i.e. when the old code would have ADMITTED this payload.
prefix_parse_admits() {
  local v
  v=$(printf '%s\n' "$1" | awk 'END { print $4 }' 2>/dev/null)
  case "$v" in ''|*[!0-9]*) return 1 ;; esac
  [ "$v" -ge "$BAR_KIB" ]
}

# raw_case <label> <payload> <kind> <expect-substring>
#   kind refuse     -> exit non-zero, 0 work markers, FAIL-CLOSED line in the SUMMARY
#   kind unmeasured -> exit 0, work began, UNMEASURED line on stderr
#   kind pass       -> exit 0, work began, PASS line on stderr
raw_case() {
  local label="$1" payload="$2" kind="$3" expect="$4"
  local s; s=$(df_script "$label" "RAW $payload")
  run_stub_gate "$label" "$s" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=2
  local err=$RS_ERR sum=$RS_SUMMARY st mk line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; st=$WX_STATUS; mk=$WX_MARKERS
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$kind" in
    refuse)
      if [ "$st" -ne 0 ] && [ "$mk" -eq 0 ]; then
        ok "df-anchor/$label: REFUSED and never began work (exit $st)"
      else
        bad "df-anchor/$label: ADMITTED a below-bar payload (exit $st, markers $mk)"
      fi
      if grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$sum" 2>/dev/null; then
        ok "df-anchor/$label: the refusal carries the named line"
      else
        bad "df-anchor/$label: no FAIL-CLOSED line in the SUMMARY"
      fi ;;
    unmeasured)
      if [ "$st" -eq 0 ] && [ "$mk" -ge 1 ]; then
        ok "df-anchor/$label: UNMEASURED is non-fatal — the run proceeded"
      else
        bad "df-anchor/$label: an unparsable payload refused the run (exit $st)"
      fi ;;
    pass)
      if [ "$st" -eq 0 ] && [ "$mk" -ge 1 ]; then
        ok "df-anchor/$label: an above-bar space-bearing payload is ADMITTED (no over-refusal)"
      else
        bad "df-anchor/$label: over-refused a legitimate above-bar payload (exit $st)"
      fi ;;
  esac
  case "$line" in
    *"$expect"*) ok "df-anchor/$label: line states '$expect'" ;;
    *)           bad "df-anchor/$label: line omits '$expect': ${line:-<none>}" ;;
  esac
}

G_SRC='my server:/export vol 999999999 900000000 10485760 90% /data'
G_MNT='/dev/sda1 999999999 900000000 10485760 90% /mnt/my disk'
G_BOTH='my server:/export vol 999999999 900000000 10485760 90% /mnt/my disk'
G_NOCAP='/dev/sda1 999999999 900000000 10485760 - /data'
# A mount PATH ending in `%` is still ONE anchor (`/mnt/50%` does not match
# `^[0-9]+%$`), so it must parse normally — the anchor must not be so eager that an
# ordinary path defeats it.
G_PCTPATH='/dev/sda1 999999999 900000000 10485760 90% /mnt/50%'
# GENUINELY ambiguous: a mount point whose SPACE-SEPARATED tokens include one that IS
# capacity-shaped. Two anchors identify nothing, so the parse must refuse rather than
# pick one — and must NOT fall back to $4, which would reinstate the false pass in
# exactly the payloads that defeat the anchor.
G_TWOCAP='/dev/sda1 999999999 900000000 10485760 90% /mnt/vol 50% spare'
G_HIGH='my server:/export vol 999999999 1 209715200 1% /data'

# The POSITIVE CONTROLS. Without these, the refusals below prove only that something
# refused — not that the old parse would have let these through.
for pl in "$G_SRC" "$G_BOTH"; do
  if prefix_parse_admits "$pl"; then
    ok "df-anchor CONTROL: the PRE-FIX \$4 parse ADMITS this below-bar payload — the defect was reachable"
  else
    bad "df-anchor CONTROL: the PRE-FIX \$4 parse did NOT admit '$pl' — this case does not demonstrate the defect"
  fi
done
# ...and the mount-only payload is the counter-control: $4 is correct there, so that
# case is about the RENDERED mount point, not about admission.
if prefix_parse_admits "$G_MNT"; then
  bad "df-anchor CONTROL: the PRE-FIX parse admits the mount-only payload — the case split is wrong"
else
  ok "df-anchor CONTROL: the PRE-FIX parse reads the mount-only payload correctly (that case tests rendering, not admission)"
fi

raw_case g-space-source "$G_SRC"    refuse     'post-slot 10.0GiB(BELOW BAR)'
raw_case g-space-mount  "$G_MNT"    refuse     'fs /mnt/my disk'
raw_case g-space-both   "$G_BOTH"   refuse     'fs /mnt/my disk'
raw_case g-no-capacity  "$G_NOCAP"  unmeasured 'UNMEASURED (df-unparsable)'
raw_case g-pct-path     "$G_PCTPATH" refuse    'fs /mnt/50%'
raw_case g-two-capacity "$G_TWOCAP" unmeasured 'UNMEASURED (df-unparsable)'
raw_case g-space-ok     "$G_HIGH"   pass       'post-slot 200.0GiB'

# ===========================================================================
# Case H (roborev job 329, finding 1): THE MEASUREMENT IMMEDIATELY PRECEDING THE
# BUILD IS ALWAYS FAIL-CLOSED. A launch measurement is advisory ONLY when a slot
# grant will follow it.
#
# The first draft made post-slot-grant binding and left FIVE paths returning into
# the build with nothing binding in front of them. Two routes are exercised here:
# the cap never engaging (no queue, so the launch reading IS the consumption-moment
# reading), and the daemon dying AFTER the queue (where the launch reading is stale
# by exactly the interval #3755 is about, so it must be RE-TAKEN).
# ===========================================================================

# --- H1: the cap never engages. One reading, and it is BINDING. ---
h1_script=$(df_script h1 "$LOW")
run_stub_gate h1 "$h1_script" \
  CQLITE_GATE_DISABLE_CAP=1 CQLITE_GATE_STUB_SLEEP=2
h1_sum=$RS_SUMMARY; h1_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h1_status=$WX_STATUS; h1_markers=$WX_MARKERS
assert_no_timeout "H1 cap-disabled"
if [ "$h1_status" -ne 0 ] && [ "$h1_markers" -eq 0 ]; then
  ok "H1: cap-inactive + below bar REFUSES and never begins work — the launch reading is BINDING"
else
  bad "H1: a cap-inactive run BUILT below the bar with nothing binding (exit $h1_status, markers $h1_markers)"
fi
h1_line=$(grep_line "$h1_err" '^agent-gate: disk-admission: ')
case "$h1_line" in
  *'FAIL-CLOSED (#3755)'*'evaluated 1x'*'NOT RE-MEASURED'*)
    ok "H1: reported as ONE binding evaluation, not re-measured for the sake of it" ;;
  *) bad "H1: wrong rendering: ${h1_line:-<none>}" ;;
esac
# The PRE-FIX behaviour is exactly the ADVISORY rendering, so its ABSENCE on a binding
# path is the differential: a below-bar cap-inactive run must never render ADVISORY.
case "$h1_line" in
  *ADVISORY*) bad "H1: still renders ADVISORY on a BINDING path — the pre-fix disposition survives" ;;
  *)          ok "H1: no ADVISORY rendering on a binding path (the pre-fix disposition is gone)" ;;
esac
if grep -q '^refusal: disk admission (#3755) — refused at LAUNCH' "$h1_sum" 2>/dev/null; then
  ok "H1: the refusal NAMES the moment it refused at (LAUNCH, not 'post-slot')"
else
  bad "H1: refusal line does not name the LAUNCH moment"
  grep -m1 '^refusal:' "$h1_sum" 2>/dev/null
fi
if [ "$(df_calls h1)" -eq 1 ]; then
  ok "H1: measured exactly ONCE — no queue elapsed, so there is nothing to re-measure"
else
  bad "H1: expected 1 measurement with no queue, got $(df_calls h1)"
fi

# --- H1 CONTROL: same route, above the bar, proceeds. ---
h1c_script=$(df_script h1c "$HIGH")
run_stub_gate h1c "$h1c_script" CQLITE_GATE_DISABLE_CAP=1 CQLITE_GATE_STUB_SLEEP=2
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h1c_status=$WX_STATUS; h1c_markers=$WX_MARKERS
assert_no_timeout "H1 control"
if [ "$h1c_status" -eq 0 ] && [ "$h1c_markers" -ge 1 ]; then
  ok "H1 CONTROL: cap-inactive + above bar PROCEEDS and begins work (the rule does not red correct input)"
else
  bad "H1 CONTROL: cap-inactive above-bar run was refused (exit $h1c_status, markers $h1c_markers)"
fi

# --- H2: the daemon dies AFTER the queue. The stale launch reading must NOT decide. ---
#
# THE INJECTION MUST NOT BE BYPASSABLE BY PRIVILEGE (roborev job 335). The first version
# used `chmod 555` on the slots dir, which a privileged user simply writes through: as
# root the daemon ACQUIRES, the grant-failed path is never taken, and the case fails for
# a reason that has nothing to do with its subject — a control that does not control,
# whose green is nonetheless read as evidence.
#
# Instead `slot.0` is pre-created as a DIRECTORY. The daemon's acquire sweep does
# `os.open(path, O_RDWR|O_CREAT)` on exactly that path (with --slots 1 it is the only
# one it tries) and EISDIR is raised for root and non-root alike; the daemon catches
# only the flock error, so it dies before acquiring. Nothing about the failure depends
# on who is running.
#
# The readings are HIGH then LOW: an implementation that reused the launch reading would
# ADMIT, so the refusal can only have come from the SECOND, fresh measurement.
h2_slots="$tmp/h2-slots"; mkdir -p "$h2_slots/slot.0"
h2_script=$(df_script h2 "$HIGH" "$LOW")
run_stub_gate h2 "$h2_script" \
  CQLITE_GATE_SLOTS_DIR="$h2_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
h2_sum=$RS_SUMMARY; h2_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h2_status=$WX_STATUS; h2_markers=$WX_MARKERS
assert_no_timeout "H2 grant-failed-after-queue"
if grep -q 'slot daemon exited before acquiring' "$h2_err" 2>/dev/null; then
  ok "H2 setup: the run really took the grant-failed-after-queue route"
else
  bad "H2 setup: the grant-failed route was not exercised — this case measured something else"
fi
if [ "$h2_status" -ne 0 ] && [ "$h2_markers" -eq 0 ]; then
  ok "H2: refused on the FRESH post-queue reading (a stale launch reading would have ADMITTED)"
else
  bad "H2: built on a STALE launch reading (exit $h2_status, markers $h2_markers)"
fi
if [ "$(df_calls h2)" -eq 2 ]; then
  ok "H2: measured TWICE — the launch reading was re-taken after the queue"
else
  bad "H2: expected 2 measurements after a queue, got $(df_calls h2)"
fi
h2_line=$(grep_line "$h2_err" '^agent-gate: disk-admission: ')
case "$h2_line" in
  *'evaluated 2x'*'RE-MEASURED after the queue'*)
    ok "H2: the line DECLARES the re-measurement and its cause" ;;
  *) bad "H2: wrong rendering: ${h2_line:-<none>}" ;;
esac
case "$h2_line" in
  *'slot RELEASED'*) bad "H2: claims a slot was RELEASED when none was ever held" ;;
  *'no slot was held'*) ok "H2: the block states honestly that no slot was ever held" ;;
  *) bad "H2: the block says nothing about the slot state: ${h2_line:-<none>}" ;;
esac

# --- H2 CONTROL, the inverse pair: LOW then HIGH on the same route must PROCEED. ---
# Together with H2 this pins that the verdict follows the SECOND reading in BOTH
# directions — a run that simply always refused on this route would pass H2 alone.
h2c_script=$(df_script h2c "$LOW" "$HIGH")
run_stub_gate h2c "$h2c_script" \
  CQLITE_GATE_SLOTS_DIR="$h2_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; h2c_status=$WX_STATUS; h2c_markers=$WX_MARKERS
assert_no_timeout "H2 control"
if [ "$h2c_status" -eq 0 ] && [ "$h2c_markers" -ge 1 ]; then
  ok "H2 CONTROL: low-then-high on the same route PROCEEDS — the verdict follows the FRESH reading, not the stale one"
else
  bad "H2 CONTROL: refused despite a fresh above-bar reading (exit $h2c_status, markers $h2c_markers)"
fi

# ===========================================================================
# Case I (roborev job 329, finding 2): the threshold comparison is FLOATING
# POINT — no `printf %d` conversion, which saturates implementation-dependently
# and, in the busybox direction, ADMITS a filesystem that must be refused.
# ===========================================================================

# --- I1: the awk census. The POSITIVE CONTROL for reachability, and the proof the
#     shipped comparison is correct under every awk this host has. ---
I_BAR_HUGE=8796093022208           # 8 EiB, whose KiB value exceeds INT64_MAX
I_AVAIL=209715200                  # 200 GiB available — must be REFUSED against it
i_admits=0; i_broken=0; i_awks=0
for a in awk gawk mawk nawk "busybox awk"; do
  command -v "${a%% *}" >/dev/null 2>&1 || continue
  i_awks=$((i_awks + 1))
  # The PRE-FIX chain, reproduced verbatim: awk %d, then bash's integer `[ -ge ]`.
  v=$($a -v g="$I_BAR_HUGE" 'BEGIN { printf "%d", (g * 1048576) + 0.5 }' 2>/dev/null)
  if [ "$I_AVAIL" -ge "$v" ] 2>/dev/null; then
    i_admits=$((i_admits + 1))
    printf 'info - pre-fix chain under %-12s -> %%d=%s ADMITS (false PASS)\n' "$a" "$v"
  fi
  # rc 2 == bash could not compare at all: a verdict reached by an ERROR, not a measurement.
  [ "$I_AVAIL" -ge "$v" ] 2>/dev/null; rc=$?
  if [ "$rc" -ge 2 ]; then
    i_broken=$((i_broken + 1))
    printf 'info - pre-fix chain under %-12s -> %%d=%s makes bash [ ] ERROR (rc %s)\n' "$a" "$v" "$rc"
  fi
  # The SHIPPED comparison, same inputs, must be exactly "below the bar" everywhere.
  $a -v k="$I_AVAIL" -v g="$I_BAR_HUGE" 'BEGIN { exit ((k + 0) >= (g * 1048576)) ? 0 : 1 }' </dev/null 2>/dev/null
  if [ $? -eq 1 ]; then
    ok "I1: the shipped float comparison is correct under $a (200GiB is BELOW an 8-EiB bar)"
  else
    bad "I1: the shipped float comparison is WRONG under $a"
  fi
done
if [ "$i_awks" -eq 0 ]; then
  skip "I1 CONTROL: no awk implementation on this host — reachability could not be measured"
elif [ "$i_admits" -gt 0 ] || [ "$i_broken" -gt 0 ]; then
  ok "I1 CONTROL: the PRE-FIX %d chain is defective under $((i_admits + i_broken)) of $i_awks awk(s) here ($i_admits ADMIT, $i_broken error out) — the defect was reachable"
else
  skip "I1 CONTROL: none of this host's $i_awks awk(s) reproduces the %d defect — reachability not demonstrable here"
fi

# --- I2/I3: the accepted bar range is STATED, and an over-range bar is FAIL-CLOSED. ---
#
# Round 5 rejected DISCARDING an over-range bar for the 40GiB default because that loosens
# a bar the operator set high. Right, and incomplete: CLAMPING DOWN loosens it too, just
# less — a requested 2 PiB floor on a multi-PiB filesystem with 1.5 PiB free would PASS
# (roborev job 367). The honest answer is a named refusal, so I2 pins the largest bar that
# IS representable and I3 pins the refusal one step above it.
bar_case i-max '1048576GiB(pinned)' CQLITE_GATE_MIN_FREE_GB=1048576

i3_script=$(df_script i3 "$HIGH")
run_stub_gate i3 "$i3_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/i3-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=2 CQLITE_GATE_MIN_FREE_GB="$I_BAR_HUGE"
i3_sum=$RS_SUMMARY; i3_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 60; i3_status=$WX_STATUS; i3_markers=$WX_MARKERS
assert_no_timeout "I3 over-range bar"
if [ "$i3_status" -ne 0 ] && [ "$i3_markers" -eq 0 ]; then
  ok "I3: an over-range bar REFUSES and never begins work — no substituted floor, high or low"
else
  bad "I3: an over-range bar ADMITTED a 200GiB filesystem (exit $i3_status, markers $i3_markers) — the bar was loosened"
fi
i3_line=$(grep_line "$i3_err" '^agent-gate: disk-admission: ')
case "$i3_line" in
  *'BAR-UNREPRESENTABLE-FAIL-CLOSED (#3755)'*"bar ${I_BAR_HUGE}GiB(out-of-range)"*)
    ok "I3: its OWN verdict token, and the bar is reported AS TYPED rather than as a substitute" ;;
  *"bar 1048576GiB"*)
    bad "I3: the bar was silently CLAMPED DOWN to the maximum — a quieter floor nobody asked for: $i3_line" ;;
  *) bad "I3: expected BAR-UNREPRESENTABLE-FAIL-CLOSED naming the typed bar, got: ${i3_line:-<none>}" ;;
esac
# Three refusal causes, three tokens: free space / fix the directory / fix the variable.
if ! grep -q 'BAR-UNREPRESENTABLE' "$a_subj_sum" 2>/dev/null \
   && grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$a_subj_sum" 2>/dev/null; then
  ok "I3: the below-bar refusal does NOT carry the bar-unrepresentable token (the three causes stay distinct)"
else
  bad "I3: the refusal tokens are not distinct"
fi
if grep -q 'exceeds the largest representable bar' "$i3_err" 2>/dev/null; then
  ok "I3: the remedy names the representable maximum, so the operator knows what to set"
else
  bad "I3: no stderr line naming the representable maximum"
fi
# It refuses BEFORE measuring: a configuration constant cannot be rescued by a reading, and
# the run must not take a slot it was always going to hand back.
if [ "$(df_calls i3)" -eq 0 ]; then
  ok "I3: refused BEFORE any df call — an unusable bar needs no measurement"
else
  bad "I3: measured $(df_calls i3) time(s) before refusing on a bar that no reading could satisfy"
fi

# ===========================================================================
# Case J (roborev job 335, Medium): EVERY full-gate SUMMARY carries the line —
# including the EARLY-TERMINAL paths, which no builder of ours ever reaches.
#
# Omission is the one rendering that must never happen: a block with no line at
# all leaves a reader unable to tell "never probed" from "predates the probe"
# from "somebody forgot a call site", and only the third ships a hole.
#
# Driven through a REAL early-terminal path, not a synthetic block: a real full
# gate (no stub) against a corpus-less CQLITE_DATASETS_ROOT, which exits at a
# preflight before any component. WHICH preflight it hits depends on the host
# (the #3544 component-set pre-flight runs BEFORE the probe and needs the
# network; the #2078 fixture preflight runs AFTER it), so this case MEASURES
# which path ran and then asserts the rendering that path is required to carry —
# never a rendering it hoped for.
# ===========================================================================
j_root="$tmp/j-empty-datasets"; mkdir -p "$j_root/sstables"
j_script=$(df_script j "$HIGH" "$HIGH")
j_sum="$tmp/j.summary.txt"; j_err="$tmp/j.err"
env "${DA_ENV_U[@]}" PATH="$tmp/shim:$PATH" \
  DF_SHIM_SCRIPT="$j_script" DF_SHIM_STATE="$tmp/j.dfstate" \
  AGENT_GATE_SUMMARY_FILE="$j_sum" \
  CQLITE_DATASETS_ROOT="$j_root" \
  CQLITE_GATE_SLOTS_DIR="$tmp/j-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_POLL_SECS=0.3 \
  bash "$GATE" >"$tmp/j.out" 2>"$j_err" &
j_pid=$!
mkdir -p "$tmp/j.norun"
watch_until_exit "$j_pid" "$tmp/j.norun" 900; j_status=$WX_STATUS
assert_no_timeout "J early-terminal full gate"

j_line=$(grep_line "$j_sum" '^disk-admission: ')
j_count=$(grep -c '^disk-admission: ' "$j_sum" 2>/dev/null || printf '0')
case "$j_count" in ''|*[!0-9]*) j_count=0 ;; esac

# Which early terminal did we actually reach? Measured, then asserted against.
j_path=""
grep -q '^missing-fixtures: FAIL-CLOSED' "$j_sum" 2>/dev/null && j_path=post-probe-fixtures
[ -z "$j_path" ] && grep -q '^missing-schemas: FAIL-CLOSED' "$j_sum" 2>/dev/null && j_path=post-probe-schemas
[ -z "$j_path" ] && grep -q '^component-set: FAIL-CLOSED' "$j_sum" 2>/dev/null && j_path=pre-probe-component-set
if [ -n "$j_path" ]; then
  ok "J setup: the run really terminated at an early preflight ($j_path, exit $j_status)"
else
  bad "J setup: no early-terminal marker in the block — this case did not exercise an early-terminal path"
  grep -E '^(RESULT|refusal|component-set|missing-)' "$j_sum" 2>/dev/null | head -4
fi
if [ "$j_count" -eq 1 ]; then
  ok "J: the early-terminal block carries EXACTLY ONE disk-admission: line — the contract has no hole"
else
  bad "J: the early-terminal block carries $j_count disk-admission: lines (the contract says exactly 1)"
fi
case "$j_path" in
  post-probe-*)
    # The probe ran before this preflight, so the block must carry a REAL verdict — which
    # need NOT be PASS. Pinning PASS specifically made this red whenever the admission probe
    # itself was legitimately UNMEASURED (a transient cargo failure is enough), i.e. a false
    # red from a condition unrelated to the property under test. The property is that the
    # verdict EXISTS and names both evaluations; NOT EVALUATED is the only failure.
    case "$j_line" in
      *'NOT EVALUATED'*)
        bad "J: a POST-probe early terminal claims NOT EVALUATED — the verdict existed and was dropped: $j_line" ;;
      *'disk-admission: '*'evaluated 2x'*)
        ok "J: a POST-probe early terminal carries a real verdict, both evaluations named" ;;
      *) bad "J: unexpected rendering on a post-probe early terminal: ${j_line:-<none>}" ;;
    esac ;;
  pre-probe-*)
    # This block genuinely precedes the probe; its honest value names the ordering.
    case "$j_line" in
      *'NOT EVALUATED'*'emitted BEFORE the #3755 probe'*)
        ok "J: a PRE-probe early terminal says so, naming the ordering — not a fabricated verdict" ;;
      *) bad "J: a pre-probe early terminal does not name the ordering: ${j_line:-<none>}" ;;
    esac ;;
esac
# Whatever the path, the block must never claim a verdict the probe did not reach.
case "$j_line" in
  *'INTERNAL (#3755)'*) bad "J: the block reports the probe ran but left no verdict — a defect state was reached" ;;
esac

# ===========================================================================
# Case K (roborev job 341, Medium): the probe measures the filesystem CARGO will
# actually write to, resolved by ASKING CARGO.
#
# The pre-fix subject was `${CARGO_TARGET_DIR:-$REPO_ROOT/target}`. Cargo also
# honours CARGO_BUILD_TARGET_DIR and `[build] target-dir` in a .cargo/config.toml
# (workspace, $CARGO_HOME, or any ancestor). Point either at another volume and the
# guard measures a device the build never touches — a confident, specific, WRONG
# number, which is worse than none because a reader acts on it.
#
# Every below case carries the PRE-FIX resolver evaluated on the SAME input, so the
# defect is shown to have been reachable rather than merely fixed.
# ===========================================================================
# ASKED, NOT HARD-CODED (job 389). This was `<repo>/target`, which is only cargo's answer
# when no configuration says otherwise — and an ANCESTOR `.cargo/config.toml` is the one
# config source this suite cannot isolate. Deriving it from cargo under the SAME isolated
# environment the cases run in makes k-default assert the property that actually matters —
# the gate resolves what CARGO resolves — instead of an assumption about the box. The
# CASE-CHOSEN expectations (k-cargo-td / k-build-td / k-config-td) are unaffected and are
# where a wrong answer is still caught.
# AN UNANSWERABLE ORACLE IS A SKIP, NOT A FAIL — AND NEVER A GUESS (the round-19 flake).
#
# THE DEFECT, identified by exact reproduction rather than by hypothesis. This was ONE
# unbounded, unretried external call at suite start whose failure was a `bad`, so a BRIEF
# transient — a concurrent `git checkout`/rebase rewriting Cargo.toml and Cargo.lock is the
# observed trigger — reddened the suite on a condition with nothing to do with the code
# under test. Signature: `passed: 236  failed: 1  skipped: 1`, reproduced BYTE-IDENTICALLY
# by failing exactly this call (suite-level calls are the metadata invocations WITHOUT
# `--locked`; the gate always passes it, so that is the discriminator).
#
# THE FIX IS NOT A RETRY, which would only pick an arbitrary count and mask the same thing.
# It is that cargo not answering is an UNMEASURABLE PRECONDITION, not evidence of a defect:
# the honest verdict is a named `skip -`, the same disposition as quota and Case Y.
#
# AND IT REMOVES A GUESS THAT WAS ALREADY WRONG IN PRINCIPLE: the old fallback asserted
# k-default against `$REPO/target`, which is only cargo's answer when NO configuration says
# otherwise — and an ancestor `.cargo/config.toml` is exactly the source this suite cannot
# isolate (job 389). So on the boxes where the fallback mattered it could be false, and the
# case would have red for the wrong reason.
K_HAVE_DEFAULT=1
K_PREFIX_DEFAULT=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
  | python3 -c 'import json,sys;sys.stdout.write(json.load(sys.stdin)["target_directory"])' 2>/dev/null)
if [ -n "$K_PREFIX_DEFAULT" ]; then
  ok "target-dir: derived the unconfigured expectation from cargo itself ($K_PREFIX_DEFAULT)"
else
  K_HAVE_DEFAULT=0
  K_PREFIX_DEFAULT=""
  skip "target-dir: cargo could not answer for the unconfigured target dir, so the k-default expectation is UNMEASURABLE here — the cases that need it are skipped by name below, and nothing is asserted against a guess"
fi

# prefix_resolver <env-name> <env-value>: the PRE-FIX subject resolution, reproduced
# exactly — `${CARGO_TARGET_DIR:-$REPO_ROOT/target}` — under the given single override.
prefix_resolver() {
  case "$1" in
    CARGO_TARGET_DIR) printf '%s' "$2" ;;
    *) printf '%s' "$K_PREFIX_DEFAULT" ;;
  esac
}

# k_case <label> <expected-target-dir> <env-name> <env-value> [more env...]
# Runs a real stub gate and asserts the line names <expected-target-dir>.
k_case() {
  local label="$1" expect="$2" envname="$3"; shift 3
  local sc; sc=$(df_script "$label" "$HIGH")
  run_stub_gate "$label" "$sc" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1 "$@"
  local err=$RS_ERR line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"target-dir $expect (via cargo metadata)"*)
      ok "target-dir/$label: resolved to $expect, and the line says HOW" ;;
    *) bad "target-dir/$label: expected 'target-dir $expect (via cargo metadata)', got: ${line:-<none>}" ;;
  esac
  # WHAT WAS MEASURED, not merely what was rendered (roborev job 360, Low).
  if df_operands_all "$label" "$expect"; then
    ok "target-dir/$label: every df -Pk call was made AGAINST $expect ($(df_calls "$label") call(s))"
  else
    bad "target-dir/$label: a df call measured something other than $expect — operands were: $(df_operands "$label" | tr '\n' ' ')"
  fi
  # The differential: what the pre-fix resolver would have picked on this same input. It is
  # stated only when the derivation succeeded, since the pre-fix resolver's answer for the
  # non-env mechanisms IS that value.
  if [ -n "$envname" ] && [ "$K_HAVE_DEFAULT" -eq 1 ]; then
    local was; was=$(prefix_resolver "$envname" "$expect")
    if [ "$was" = "$expect" ]; then
      ok "target-dir/$label CONTROL: the pre-fix resolver also picked $expect (this case is not a differential — it guards against over-correction)"
    else
      ok "target-dir/$label CONTROL: the PRE-FIX resolver picked $was, NOT $expect — it measured the wrong filesystem"
    fi
  fi
}

k_ct="$tmp/k-target-ct"
k_bt="$tmp/k-target-bt"
k_ch="$tmp/k-cargo-home"; mkdir -p "$k_ch"
k_cfg="$tmp/k-target-cfg"
printf '[build]\ntarget-dir = "%s"\n' "$k_cfg" > "$k_ch/config.toml"

if [ "$K_HAVE_DEFAULT" -eq 1 ]; then
  k_case k-default   "$K_PREFIX_DEFAULT" ""                     CQLITE_GATE_POLL_SECS=0.3
else
  skip "target-dir/k-default: skipped — the unconfigured expectation could not be derived (see above)"
fi
k_case k-cargo-td  "$k_ct"  CARGO_TARGET_DIR       CARGO_TARGET_DIR="$k_ct"
k_case k-build-td  "$k_bt"  CARGO_BUILD_TARGET_DIR CARGO_BUILD_TARGET_DIR="$k_bt"
k_case k-config-td "$k_cfg" CARGO_HOME             CARGO_HOME="$k_ch"
# Precedence: env CARGO_TARGET_DIR must beat CARGO_BUILD_TARGET_DIR and the config file.
k_case k-precedence "$k_ct" CARGO_TARGET_DIR \
  CARGO_HOME="$k_ch" CARGO_BUILD_TARGET_DIR="$k_bt" CARGO_TARGET_DIR="$k_ct"

# The measurement must follow the RESOLVED directory, not just be reported beside it: a
# target dir on a filesystem the shim reports as BELOW the bar must REFUSE.
k_low_script=$(df_script k-low "$LOW")
run_stub_gate k-low "$k_low_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/k-low-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=2 CARGO_BUILD_TARGET_DIR="$k_bt"
k_low_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; k_low_status=$WX_STATUS; k_low_markers=$WX_MARKERS
assert_no_timeout "k-low"
if [ "$k_low_status" -ne 0 ] && [ "$k_low_markers" -eq 0 ]; then
  ok "target-dir/k-low: a below-bar reading on the RESOLVED target dir refuses (the verdict follows the resolution)"
else
  bad "target-dir/k-low: did not refuse (exit $k_low_status, markers $k_low_markers)"
fi
if grep -q "target-dir $k_bt (via cargo metadata)" "$k_low_sum" 2>/dev/null; then
  ok "target-dir/k-low: the refusal SUMMARY names the directory it measured"
else
  bad "target-dir/k-low: the refusal SUMMARY does not name the resolved directory"
fi

# RESOLUTION FAILURE is UNMEASURED with a cause naming TARGET-DIR RESOLUTION — distinct
# from a df cause and from a bar cause, because they are three different operator
# actions — and it NEVER falls back to $REPO_ROOT/target, which would reinstate the
# defect in exactly the configurations that trigger it. Driven by a PATH with no cargo,
# which is what an absent cargo really looks like to the probe (rc 127).
mkdir -p "$tmp/k-nocargo-bin"
cat > "$tmp/k-nocargo-bin/cargo" <<'NOCARGO'
#!/usr/bin/env bash
exit 127
NOCARGO
chmod +x "$tmp/k-nocargo-bin/cargo"
k_nc_script=$(df_script k-nocargo "$HIGH")
RS_PATH_PREFIX="$tmp/k-nocargo-bin"
run_stub_gate k-nocargo "$k_nc_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/k-nocargo-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
k_nc_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; k_nc_status=$WX_STATUS; k_nc_markers=$WX_MARKERS
assert_no_timeout "k-nocargo"
k_nc_line=$(grep_line "$k_nc_err" '^agent-gate: disk-admission: ')
case "$k_nc_line" in
  *'UNMEASURED (target-dir-'*)
    ok "target-dir/k-nocargo: resolution failure is UNMEASURED with a cause naming TARGET-DIR resolution" ;;
  *'UNMEASURED (df-'*)
    bad "target-dir/k-nocargo: a resolution failure is reported as a DF failure — wrong operator action: $k_nc_line" ;;
  *) bad "target-dir/k-nocargo: expected a target-dir UNMEASURED cause, got: ${k_nc_line:-<none>}" ;;
esac
# GUARDED: with an empty K_PREFIX_DEFAULT the pattern below would match ANY line, turning a
# negative assertion into an unconditional failure.
if [ "$K_HAVE_DEFAULT" -eq 0 ]; then
  skip "target-dir/k-nocargo: no-fallback check skipped — it compares against the underivable unconfigured target dir"
else
case "$k_nc_line" in
  *"target-dir $K_PREFIX_DEFAULT"*)
    bad "target-dir/k-nocargo: fell back to \$REPO_ROOT/target — the defect is reinstated in exactly the configurations that trigger it" ;;
  *) ok "target-dir/k-nocargo: NO fallback to \$REPO_ROOT/target on a resolution failure" ;;
esac
fi
if [ "$k_nc_status" -eq 0 ] && [ "$k_nc_markers" -ge 1 ]; then
  ok "target-dir/k-nocargo: a resolution failure is NON-FATAL (declared, not un-runnable)"
else
  bad "target-dir/k-nocargo: a resolution failure refused the run (exit $k_nc_status)"
fi
# The df shim must NOT have been consulted: with no subject there is nothing to measure.
if [ "$(df_calls k-nocargo)" -eq 0 ]; then
  ok "target-dir/k-nocargo: df was never called — the probe refuses before measuring an unresolved subject"
else
  bad "target-dir/k-nocargo: df was called $(df_calls k-nocargo) time(s) against an unresolved subject"
fi

# ===========================================================================
# Case L (roborev job 345, Medium): the PROBE and the BUILDS resolve the target
# directory the SAME way — one resolver, one truth.
#
# The probe stopped modelling cargo in round 5, which exposed that
# run_side_component still did, with the very expression the probe had shed. So a
# config-based target dir made the guard measure cargo's directory while several
# large side-lane builds wrote somewhere else entirely.
#
# Asserted against the REAL functions: both bodies are EXTRACTED VERBATIM from the
# shipped agent-gate.sh and executed (the idiom test_cargo_output_parsers.sh uses),
# so unwiring them reds this suite instead of greening it. Only two things are
# substituted, and neither is the subject: `dispatch_component`, replaced by a
# recorder, and `_gate_resolve_target_dir`, scripted — the resolver itself is
# covered against the REAL cargo by Case K, and stubbing it here isolates the
# wiring question this case exists to answer.
# ===========================================================================
l_extract() { awk -v f="^$1\\\(\\\) \\\{$" '$0 ~ f {p=1} p {print} p && /^\}$/ {exit}' "$GATE"; }

for fn in _gate_side_target_base_init run_side_component; do
  if [ -n "$(l_extract "$fn")" ]; then
    ok "side-base: extracted the REAL $fn from the shipped gate"
  else
    bad "side-base: could not extract $fn — this case would be testing nothing"
  fi
done

# l_side_base <resolver-answer> <_DA_TARGET_DIR> <CARGO_TARGET_DIR> -> the base
# run_side_component actually passes, via the real bodies.
l_side_base() {
  local answer="$1" datd="$2" ctd="$3"
  (
    set -uo pipefail
    REPO_ROOT="/repo-root"
    _DA_TARGET_DIR="$datd"
    CARGO_TARGET_DIR="$ctd"
    _GATE_SIDE_BASE=""; _GATE_SIDE_BASE_NOTE=""
    # `$answer`, never `$1`: inside a function body `$1` is THAT FUNCTION's first
    # argument, so the obvious `printf '%s' "$1"` printed the empty string and every
    # scripted answer read as UNRESOLVED — a stub that silently stubbed nothing.
    _L_ANSWER="$answer"
    _gate_resolve_target_dir() { printf '%s' "$_L_ANSWER"; }
    dispatch_component() { printf '%s' "${CARGO_TARGET_DIR%/agent-gate-side/*}"; }
    eval "$(l_extract _gate_side_target_base_init)"
    eval "$(l_extract run_side_component)"
    _gate_side_target_base_init 2>/dev/null
    run_side_component smoke
  ) 2>/dev/null
}
# The PRE-FIX body, reproduced verbatim, for the differential.
l_prefix_base() { printf '%s' "${2:-/repo-root/target}"; }

l_case() {
  local label="$1" expect="$2" resolver="$3" datd="$4" ctd="$5"
  local got; got=$(l_side_base "$resolver" "$datd" "$ctd")
  if [ "$got" = "$expect" ]; then
    ok "side-base/$label: the REAL run_side_component bases on $expect"
  else
    bad "side-base/$label: expected base $expect, got '${got:-<none>}'"
  fi
  local was; was=$(l_prefix_base "$resolver" "$ctd")
  if [ "$was" = "$expect" ]; then
    ok "side-base/$label CONTROL: the pre-fix body also produced $expect (over-correction guard, not a differential)"
  else
    ok "side-base/$label CONTROL: the PRE-FIX body produced $was, NOT $expect — the builds wrote to a filesystem the probe never measured"
  fi
}
# (a) the probe already resolved it: reuse that answer verbatim, never re-ask.
l_case probe-verdict  /cfg-target  'OK /never-asked' /cfg-target ''
# (b) no probe verdict (--only): ask the SAME resolver.
l_case only-mode      /cfg-target  'OK /cfg-target'  ''           ''
# (c) CARGO_TARGET_DIR set: cargo resolves it, and the base follows cargo's answer.
l_case cargo-td       /env-target  'OK /env-target'  ''           /env-target
# (d) resolution FAILS: the legacy modelled base survives HERE and only here.
l_case unresolved     /repo-root/target 'UNRESOLVED target-dir-cargo-unavailable' '' ''

# The behaviour change, stated as an assertion rather than left in prose: with a
# config-based target dir the side base is NO LONGER under the repo.
l_cfg=$(l_side_base 'OK /cfg-target' /cfg-target '')
case "$l_cfg" in
  /repo-root/*) bad "side-base: a config-based target dir still lands under the repo — the disagreement survives" ;;
  *) ok "side-base: with a config-based target dir, side-lane builds are placed under it, not under the repo (declared behaviour change)" ;;
esac
# ...and the placement suffix is unchanged, so nothing else about the side lane moved.
l_full=$(
  ( set -uo pipefail
    REPO_ROOT=/repo-root; _DA_TARGET_DIR=/cfg-target; CARGO_TARGET_DIR=""
    _GATE_SIDE_BASE=""; _GATE_SIDE_BASE_NOTE=""
    _gate_resolve_target_dir() { printf 'OK /cfg-target'; }
    dispatch_component() { printf '%s' "$CARGO_TARGET_DIR"; }
    eval "$(l_extract _gate_side_target_base_init)"; eval "$(l_extract run_side_component)"
    _gate_side_target_base_init 2>/dev/null; run_side_component smoke ) 2>/dev/null)
if [ "$l_full" = "/cfg-target/agent-gate-side/smoke" ]; then
  ok "side-base: the per-component suffix is unchanged (<base>/agent-gate-side/<name>)"
else
  bad "side-base: unexpected per-component path '$l_full'"
fi

# ===========================================================================
# Case M (roborev job 345, Low): a fresh target dir is never paired with a stale
# mount. The target dir is deliberately RE-RESOLVED at slot grant, so if it moved
# during the queue the retained mount describes a different filesystem — and the
# remedy line would send an operator to clean the wrong one.
# ===========================================================================
# NOTE the target dirs below live under $tmp. Since the subject became a bounded
# `mkdir -p` (job 351) an unwritable path such as `/td-A` is legitimately
# `target-dir-uncreatable`, so a fixture using one would measure that instead of the
# subject-moved property this case is about.
m_case() {
  local label="$1" td1="$2" td2="$3" expect_fs="$4" why="$5"
  local cs="$tmp/$label.cargoscript"
  printf '%s\n%s\n' "$td1" "$td2" > "$cs"
  local ds; ds=$(df_script "$label" "$HIGH" FAIL)
  RS_PATH_PREFIX="$tmp/cargoshim"
  run_stub_gate "$label" "$ds" \
    CARGO_SHIM_SCRIPT="$cs" CARGO_SHIM_STATE="$tmp/$label.cargostate" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1
  RS_PATH_PREFIX=""
  local err=$RS_ERR line
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120
  assert_no_timeout "$label"
  line=$(grep_line "$err" '^agent-gate: disk-admission: ')
  case "$line" in
    *"fs $expect_fs;"*) ok "stale-mount/$label: $why" ;;
    *) bad "stale-mount/$label: expected 'fs $expect_fs', got: ${line:-<none>}" ;;
  esac
  case "$line" in
    *"target-dir $td2 "*) ok "stale-mount/$label: the line names the RE-RESOLVED target dir ($td2)" ;;
    *) bad "stale-mount/$label: the line does not name the re-resolved target dir: ${line:-<none>}" ;;
  esac
  # THE OPERANDS MUST TRACK THE RESOLUTION IN FORCE AT EACH MOMENT. When the subject moves
  # mid-run the two df calls must be made against DIFFERENT paths; a run that measured the
  # same filesystem twice while reporting a moved target dir is the exact confusion the
  # rendering assertions above cannot see.
  local o1 o2
  o1=$(df_operands "$label" | sed -n 1p)
  o2=$(df_operands "$label" | sed -n 2p)
  if [ "$o1" = "$td1" ] && [ "$o2" = "$td2" ]; then
    ok "stale-mount/$label: df call 1 measured $td1 and call 2 measured $td2 — each the resolution in force at that moment"
  else
    bad "stale-mount/$label: df operands were '$o1' then '$o2'; expected '$td1' then '$td2'"
  fi
}
# The subject MOVED during the queue -> the mount measured for the old one is dropped.
m_case m-moved   "$tmp/td-A" "$tmp/td-B" unknown \
  "the mount is CLEARED when the re-resolved subject differs (no fresh-dir/stale-mount pairing)"
# CONTROL: the subject is unchanged -> the mount IS retained. Without this, a rule that
# simply always cleared would pass the case above and lose real information.
m_case m-same    "$tmp/td-A" "$tmp/td-A" /shimfs \
  "the mount is RETAINED when the re-resolved subject is PROVEN identical"

# ===========================================================================
# Case N (roborev job 349): the SUBJECT SET declares itself NON-EXHAUSTIVE, on
# every rendering, naming #3886.
#
# This probe measures ONE filesystem. The venv and bindings/node/node_modules are
# not measured, and node_modules is under the REPOSITORY whatever cargo's target
# dir says — a counting-completeness gap split to #3886. A bare
# `disk-admission: PASS` would invite a reader to infer a closure this check does
# not deliver. A declaration nothing tests is a comment, so it is tested here.
# ===========================================================================
n_declares() {
  local label="$1" text="$2"
  case "$text" in
    *'subjects 1 RECOGNISED'*'NON-EXHAUSTIVE'*'(#3886)'*)
      ok "non-exhaustive/$label: declares an AFFIRMATIVE count, its incompleteness, and #3886" ;;
    *) bad "non-exhaustive/$label: missing or malformed declaration: ${text:-<none>}" ;;
  esac
  # The affirmative form is the point: `1 RECOGNISED`, never a bare figure, for the same
  # reason the cfg-gated-subtree census spells `0 RECOGNISED`.
  case "$text" in
    *'subjects 1 RECOGNISED (the cargo-resolved BUILD-OUTPUT filesystem only)'*)
      ok "non-exhaustive/$label: names WHAT the one measured subject is" ;;
    *) bad "non-exhaustive/$label: does not name the measured subject" ;;
  esac
}
# PASS rendering.
n_pass_script=$(df_script n-pass "$HIGH")
run_stub_gate n-pass "$n_pass_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/n-pass-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
n_pass_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "n-pass"
n_declares PASS "$(grep_line "$n_pass_err" '^agent-gate: disk-admission: ')"
# FAIL-CLOSED rendering — a refusal must not lose the declaration.
n_fail_script=$(df_script n-fail "$LOW")
run_stub_gate n-fail "$n_fail_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/n-fail-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
n_fail_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "n-fail"
n_declares FAIL-CLOSED "$(grep_line "$n_fail_sum" '^disk-admission: ')"
# UNMEASURED rendering.
n_unm_script=$(df_script n-unm FAIL)
run_stub_gate n-unm "$n_unm_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/n-unm-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
n_unm_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "n-unm"
n_declares UNMEASURED "$(grep_line "$n_unm_err" '^agent-gate: disk-admission: ')"

# ===========================================================================
# Case O (roborev job 349, Low): the bounded runner's capture TRIPLE is owned and
# released — no `agent-gate-bcap.*` strays.
#
# Every bounded call on the admission path is made from inside a `$( … )`, where
# the runner's lazily-mktemp'd triple was memoized in a subshell and left three
# files behind per resolution — multiplied by every nested gate this suite runs.
# Counted in a PRIVATE TMPDIR so a peer lane on the same box cannot perturb it.
# ===========================================================================
o_tmp="$tmp/o-tmpdir"; mkdir -p "$o_tmp"
o_count() { local c=0 f; for f in "$o_tmp"/agent-gate-bcap.*; do [ -e "$f" ] && c=$((c+1)); done; printf '%s' "$c"; }
# PROVE THE COUNTER DISCRIMINATES before trusting a zero from it — a counter that can
# never see a leak reports "no leak" on a leaking build (four instances of that family on
# this branch already).
: > "$o_tmp/agent-gate-bcap.control"
if [ "$(o_count)" -eq 1 ]; then
  ok "capture-leak CONTROL: the counter SEES a planted bcap file (a zero from it means something)"
else
  bad "capture-leak CONTROL: the counter cannot see a planted bcap file — its zero would prove nothing"
fi
rm -f "$o_tmp/agent-gate-bcap.control"
o_before=$(o_count)
o_script=$(df_script o "$HIGH")
run_stub_gate o "$o_script" \
  TMPDIR="$o_tmp" CQLITE_GATE_SLOTS_DIR="$tmp/o-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_SLEEP=1
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; assert_no_timeout "o"
o_after=$(o_count)
if [ "$o_after" -eq "$o_before" ]; then
  ok "capture-leak: a full-gate run left 0 stray bcap files in its TMPDIR (before=$o_before after=$o_after)"
else
  bad "capture-leak: the run leaked $((o_after - o_before)) bcap file(s) (before=$o_before after=$o_after)"
fi

# ===========================================================================
# Case P (roborev job 349, Medium): `df` is BOUNDED.
#
# A stalled NFS/FUSE mount hangs `df` indefinitely — at the post-grant
# measurement, while the machine-wide slot is HELD. That is #3755's own failure
# recreated inside its fix. The bound firing is reported with a cause DISTINCT
# from a parse failure, because a hang and a bad payload are different operator
# situations. The proof that nothing hangs is that this case returns at all.
# ===========================================================================
mkdir -p "$tmp/p-hangbin"
cat > "$tmp/p-hangbin/df" <<'PHANG'
#!/usr/bin/env bash
# Hangs on the FIRST call only, longer than _GATE_DF_BOUND_SECS; answers normally
# afterwards, so the case costs one bound rather than two.
n=$(cat "$DF_SHIM_STATE" 2>/dev/null || printf '0')
case "$n" in ''|*[!0-9]*) n=0 ;; esac
n=$((n + 1)); printf '%s' "$n" > "$DF_SHIM_STATE"
if [ "$n" -eq 1 ]; then sleep 120; fi
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/shim 999999999 1 209715200 1%% /shimfs\n'
PHANG
chmod +x "$tmp/p-hangbin/df"
p_script=$(df_script p "$HIGH")
RS_PATH_PREFIX="$tmp/p-hangbin"
run_stub_gate p "$p_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/p-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
p_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; p_status=$WX_STATUS; p_markers=$WX_MARKERS
assert_no_timeout "p bounded df"
p_line=$(grep_line "$p_err" '^agent-gate: disk-admission: ')
case "$p_line" in
  *'launch UNMEASURED(df-timeout)'*)
    ok "bounded-df: a hanging df is CUT OFF and reported as df-timeout, distinct from df-failed" ;;
  *'launch UNMEASURED(df-failed)'*)
    bad "bounded-df: a hang is reported as a generic df-failed — a hang and a parse failure are different situations: $p_line" ;;
  *) bad "bounded-df: expected a df-timeout launch reading, got: ${p_line:-<none>}" ;;
esac
case "$p_line" in
  *'post-slot 200.0GiB'*)
    ok "bounded-df CONTROL: the run continued and the SECOND (fast) reading was taken normally" ;;
  *) bad "bounded-df CONTROL: the run did not recover after the bound fired: ${p_line:-<none>}" ;;
esac
if [ "$p_status" -eq 0 ] && [ "$p_markers" -ge 1 ]; then
  ok "bounded-df: the gate did not hang holding the slot — it measured, declared and proceeded"
else
  bad "bounded-df: the run did not complete normally (exit $p_status, markers $p_markers)"
fi

# ===========================================================================
# Case Q (roborev job 351, Medium): a MAIN-ONLY invocation never runs
# `cargo metadata`.
#
# _gate_side_target_base_init used to be called before anything established
# whether a side component had even been selected, so `--only file-size` —
# DOCUMENTED as cargo-free and hermetic, and the shape the nested tooling
# self-tests use — invoked cargo metadata: a delay and a possible Cargo.lock
# write on a path whose contract is that it touches neither.
#
# Asserted from an OBSERVATION (a recording shim), never from a timing or an
# absence nobody measured — and the shim is proved to discriminate first.
# ===========================================================================
mkdir -p "$tmp/q-cargoshim"
_Q_REAL_CARGO=$(command -v cargo 2>/dev/null || printf '/nonexistent/cargo')
cat > "$tmp/q-cargoshim/cargo" <<QSHIM
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "\$CARGO_RECORD"
exec "$_Q_REAL_CARGO" "\$@"
QSHIM
chmod +x "$tmp/q-cargoshim/cargo"
# `grep -c` PRINTS 0 and EXITS 1 when nothing matches, so a `|| printf '0'` fallback
# emits BOTH and the result is the two-line string "0\n0" — which then blows up in
# `[ -eq ]`. Take grep's output and sanitize it; never add a fallback beside it.
q_meta_calls() {
  local n; n=$(grep -c '^metadata' "$1" 2>/dev/null); n="${n%%$'\n'*}"
  case "$n" in ''|*[!0-9]*) n=0 ;; esac
  printf '%s' "$n"
}

# THE SUBJECT SET IS DERIVED, NOT LISTED (roborev job 357). Round 8 fixed the INSTANCE the
# review named — `--only file-size`, a MAIN-lane component — and left the CLASS open: the
# SIDE lane also holds explicitly Cargo-free components, and `--only delivery-telemetry`
# still ran cargo metadata. A finding names an instance; the defect is a class, so the set
# is computed from the gate's OWN classification (_component_lane + _fm_component_class,
# extracted from the shipped file) and a future Cargo-free component joins with no edit here.
q_extract() { sed -n "/^$1() {/,/^}$/p" "$GATE"; }
q_derive_free() {
  (
    eval "$(q_extract _component_lane)"
    eval "$(q_extract _fm_component_class)"
    local c
    for c in $(sed -n 's/^COMPONENTS=(\(.*\))$/\1/p' "$GATE"); do
      case "$(_fm_component_class "$c" 2>/dev/null)" in
        no-cargo) printf '%s\n' "$c" ;;
      esac
    done
  )
}
q_free=$(q_derive_free)
q_free_n=$(printf '%s\n' "$q_free" | grep -c '[^[:space:]]' || true)
q_free_n="${q_free_n%%$'\n'*}"; case "$q_free_n" in ''|*[!0-9]*) q_free_n=0 ;; esac
# A derivation that yields nothing would make every assertion below vacuous.
if [ "$q_free_n" -ge 3 ]; then
  ok "only-cargo-free: derived $q_free_n Cargo-free component(s) from the gate's own classification"
else
  bad "only-cargo-free: the derivation yielded $q_free_n component(s) — too few to be the real set; every assertion below would be vacuous"
fi

# (a) THE SUBJECTS: one --only run per Cargo-free component, MAIN lane and SIDE lane alike.
q_only=0
q_worst=""
for q_c in $q_free; do
  q_rec="$tmp/q-only.$q_c.record"; : > "$q_rec"
  env "${DA_ENV_U[@]}" PATH="$tmp/q-cargoshim:$PATH" CARGO_RECORD="$q_rec" \
    AGENT_GATE_SUMMARY_FILE="$tmp/q-only.$q_c.summary.txt" \
    CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-/nonexistent}" \
    bash "$GATE" --only "$q_c" >"$tmp/q-only.$q_c.out" 2>"$tmp/q-only.$q_c.err"
  q_n=$(q_meta_calls "$q_rec")
  if [ "$q_n" -eq 0 ]; then
    ok "only-cargo-free[$q_c]: 0 'cargo metadata' calls — its Cargo-free contract holds"
  else
    bad "only-cargo-free[$q_c]: invoked cargo metadata $q_n time(s) — a documented Cargo-free path runs Cargo"
    q_only=$((q_only + q_n)); q_worst="$q_c"
  fi
done
q_rec="$tmp/q-only.file-size.record"
# (b) THE DISCRIMINATION CONTROL, run FIRST in spirit and asserted here: the SAME shim on a
#     full gate MUST record metadata calls. Without it, "0" proves only that the shim is
#     inert — the failure mode four earlier controls on this branch actually had.
q_rec2="$tmp/q-full.record"; : > "$q_rec2"
q_full_script=$(df_script q-full "$HIGH")
mkdir -p "$tmp/q-full.run"
env "${DA_ENV_U[@]}" PATH="$tmp/q-cargoshim:$tmp/shim:$PATH" CARGO_RECORD="$q_rec2" \
  DF_SHIM_SCRIPT="$q_full_script" DF_SHIM_STATE="$tmp/q-full.dfstate" \
  AGENT_GATE_SUMMARY_FILE="$tmp/q-full.summary.txt" \
  CQLITE_GATE_STUB_RUNDIR="$tmp/q-full.run" CQLITE_GATE_STUB_SLEEP=1 \
  CQLITE_GATE_SLOTS_DIR="$tmp/q-full-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  bash "$GATE" >"$tmp/q-full.out" 2>"$tmp/q-full.err"
q_full=$(q_meta_calls "$q_rec2")
if [ "$q_full" -ge 1 ]; then
  ok "only-cargo-free CONTROL: the same shim records $q_full 'cargo metadata' call(s) on a full gate — a 0 from it means something"
else
  bad "only-cargo-free CONTROL: the shim recorded NO metadata call even on a full gate — it is inert and the subject assertion below proves nothing"
fi
if [ "$q_only" -eq 0 ]; then
  ok "only-cargo-free: the WHOLE derived class is Cargo-metadata-free, not just the one instance a review named"
else
  bad "only-cargo-free: $q_only metadata call(s) across the class (worst: $q_worst)"
fi
# The claim is precisely about `cargo metadata`. The gate's accelerator detection runs
# `cargo nextest --version` at startup on EVERY invocation, which is pre-existing and not
# this issue's subject; asserting "no cargo at all" would be asserting something false.
if [ "$(wc -l < "$q_rec")" -ge 1 ]; then
  ok "only-cargo-free: the run DID make its pre-existing startup cargo probe — the shim was on PATH and active"
else
  bad "only-cargo-free: no cargo call at all was recorded — the shim was not on the child's PATH"
fi

# ===========================================================================
# Case R (roborev job 351, Medium): the two-valued ancestor walk is GONE.
#
# `test -e` answers 1 for a permission-denied component, a symlink loop and a
# non-directory component exactly as for a genuinely missing path, so the walk
# climbed PAST an inaccessible mount and measured a DIFFERENT filesystem — a
# FALSE ADMISSION, the 1699-find-tristate shape. `mkdir -p` replaces it and
# answers the question the probe actually has: can the build write here.
#
# The fixture uses a NON-DIRECTORY path component, not chmod: ENOTDIR is raised
# for root and non-root alike, so this control cannot be bypassed by privilege
# (the H2 lesson from job 335).
# ===========================================================================
r_file="$tmp/r-not-a-directory"; : > "$r_file"
r_target="$r_file/target"

# THE POSITIVE CONTROL: the PRE-FIX walk, reproduced verbatim, on the same input.
r_walk() {
  local d="$1"
  while [ -n "$d" ] && [ "$d" != "/" ]; do
    [ -e "$d" ] && { printf '%s' "$d"; return 0; }
    d="$(dirname "$d")"
  done
  printf '/'
}
r_would=$(r_walk "$r_target")
if [ "$r_would" = "$r_file" ]; then
  ok "mkdir-subject CONTROL: the PRE-FIX walk resolved to '$r_would' — a plain FILE, not the build directory — and would have measured its filesystem and ADMITTED"
else
  bad "mkdir-subject CONTROL: the pre-fix walk resolved to '$r_would'; this fixture does not demonstrate the defect"
fi

r_script=$(df_script r "$HIGH")
run_stub_gate r "$r_script" \
  CARGO_TARGET_DIR="$r_target" \
  CQLITE_GATE_SLOTS_DIR="$tmp/r-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
r_err=$RS_ERR; r_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; r_status=$WX_STATUS; r_markers=$WX_MARKERS
assert_no_timeout "r uncreatable target dir"
r_line=$(grep_line "$r_err" '^agent-gate: disk-admission: ')
# ---- roborev job 357, Medium: a failure that ESTABLISHES the build cannot write is a
# BINDING REFUSAL, not a non-fatal UNMEASURED. Classifying it "could not tell" bypassed
# admission on exactly the condition this change exists to catch.
if [ "$r_status" -ne 0 ] && [ "$r_markers" -eq 0 ]; then
  ok "cannot-write: an uncreatable target dir REFUSES and never begins work (exit $r_status)"
else
  bad "cannot-write: the run PROCEEDED into a build already known to be impossible (exit $r_status, markers $r_markers)"
fi
case "$r_line" in
  *'UNWRITABLE-FAIL-CLOSED (#3755)'*'UNWRITABLE(ENOTDIR)'*)
    ok "cannot-write: reported under its OWN verdict token, naming the errno that established it" ;;
  *'UNMEASURED'*)
    bad "cannot-write: still classified UNMEASURED — an affirmative 'cannot write' read as 'cannot tell': $r_line" ;;
  *) bad "cannot-write: unexpected rendering: ${r_line:-<none>}" ;;
esac
if grep -qx 'RESULT: FAIL' "$r_sum" 2>/dev/null; then
  ok "cannot-write: RESULT: FAIL (the pollable terminal token, as for a below-bar refusal)"
else
  bad "cannot-write: no exact 'RESULT: FAIL' line in the refusal SUMMARY"
fi
# The two binding causes must be TEXTUALLY distinct — different operator situations,
# different remedies — so a below-bar refusal must NOT carry the unwritable token.
if grep -q '^disk-admission: FAIL-CLOSED (#3755)' "$a_subj_sum" 2>/dev/null \
   && ! grep -q '^disk-admission: UNWRITABLE-FAIL-CLOSED' "$a_subj_sum" 2>/dev/null; then
  ok "cannot-write: the below-bar refusal keeps its own distinct token (the two are not merged)"
else
  bad "cannot-write: the below-bar and unwritable refusals are not textually distinct"
fi
if [ "$(df_calls r)" -eq 0 ]; then
  ok "cannot-write: df was NEVER called — no filesystem other than the subject was measured"
else
  bad "cannot-write: df ran $(df_calls r) time(s) — some other filesystem was measured"
fi
# A REAL EACCES subject, in addition to the ENOTDIR one above, because the finding named
# permission failures explicitly. Skipped rather than faked when running as root, where
# chmod cannot deny us.
if [ "$(id -u)" -eq 0 ]; then
  skip "cannot-write[EACCES]: running as root — chmod cannot produce a real permission denial here"
else
  r_locked="$tmp/r-locked"; mkdir -p "$r_locked"; chmod 500 "$r_locked"
  r_perm_script=$(df_script r-perm "$HIGH")
  run_stub_gate r-perm "$r_perm_script" \
    CARGO_TARGET_DIR="$r_locked/target" \
    CQLITE_GATE_SLOTS_DIR="$tmp/r-perm-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
  r_perm_err=$RS_ERR
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; r_perm_status=$WX_STATUS; r_perm_markers=$WX_MARKERS
  assert_no_timeout "r-perm"
  chmod 700 "$r_locked"
  r_perm_line=$(grep_line "$r_perm_err" '^agent-gate: disk-admission: ')
  case "$r_perm_line" in
    *'UNWRITABLE-FAIL-CLOSED (#3755)'*'UNWRITABLE(EACCES)'*)
      if [ "$r_perm_status" -ne 0 ] && [ "$r_perm_markers" -eq 0 ]; then
        ok "cannot-write[EACCES]: a REAL permission denial refuses and never begins work"
      else
        bad "cannot-write[EACCES]: named the errno but did not refuse (exit $r_perm_status)"
      fi ;;
    *) bad "cannot-write[EACCES]: expected an UNWRITABLE(EACCES) refusal, got: ${r_perm_line:-<none>}" ;;
  esac
fi

# ---- THE OTHER HALF OF THE SPLIT: a failure that establishes NOTHING stays non-fatal.
# Driven by a REAL bound firing, not a simulated status: a python3 shim that hangs ONLY on
# the WRITE PROBE (recognised by its own payload marker) and delegates the metadata parse,
# so the resolution still succeeds and the hang lands exactly on the call under test.
da_py_hang_shim "$tmp/r-hangpy"
r_unm_script=$(df_script r-unm "$HIGH")
RS_PATH_PREFIX="$tmp/r-hangpy"
run_stub_gate r-unm "$r_unm_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/r-unm-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
r_unm_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; r_unm_status=$WX_STATUS; r_unm_markers=$WX_MARKERS
assert_no_timeout "r-unm bounded classifier"
r_unm_line=$(grep_line "$r_unm_err" '^agent-gate: disk-admission: ')
case "$r_unm_line" in
  *'UNMEASURED (target-dir-mkdir-timeout)'*)
    ok "cannot-tell: the bound firing stays UNMEASURED with its own cause — it establishes nothing" ;;
  *'UNWRITABLE'*)
    bad "cannot-tell: a bound timeout was read as an affirmative 'cannot write' — that would red correct runs: $r_unm_line" ;;
  *) bad "cannot-tell: expected UNMEASURED (target-dir-mkdir-timeout), got: ${r_unm_line:-<none>}" ;;
esac
if [ "$r_unm_status" -eq 0 ] && [ "$r_unm_markers" -ge 1 ]; then
  ok "cannot-tell: 'could not tell' is NON-FATAL — the run proceeded, declared"
else
  bad "cannot-tell: an unclassifiable failure refused the run (exit $r_unm_status)"
fi

# THE OTHER HALF: a target dir that simply does not exist yet — the cold-lane case the
# walk existed for — must be CREATED and measured, not refused. This is what stops the
# fix being an over-correction that reds every cold lane.
r_cold="$tmp/r-cold/deep/target"
r_cold_script=$(df_script r-cold "$HIGH")
run_stub_gate r-cold "$r_cold_script" \
  CARGO_TARGET_DIR="$r_cold" \
  CQLITE_GATE_SLOTS_DIR="$tmp/r-cold-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
r_cold_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; r_cold_status=$WX_STATUS
assert_no_timeout "r cold target dir"
r_cold_line=$(grep_line "$r_cold_err" '^agent-gate: disk-admission: ')
case "$r_cold_line" in
  *"disk-admission: PASS"*"target-dir $r_cold "*)
    ok "mkdir-subject: a not-yet-existing target dir is CREATED and measured (the cold-lane case the walk existed for)" ;;
  *) bad "mkdir-subject: a cold target dir was not measured: ${r_cold_line:-<none>}" ;;
esac
if [ -d "$r_cold" ]; then
  ok "mkdir-subject: the accepted side effect is real and asserted — the directory now exists (cargo would create it seconds later anyway)"
else
  bad "mkdir-subject: the target dir was reported measured but does not exist"
fi

# ===========================================================================
# Case T: the shipped gate still PARSES, and the operand assertion above
# DISCRIMINATES.
#
# (a) The admission path embeds a python program inside a single-quoted bash
#     string, so one apostrophe in a comment terminates the quote and breaks the
#     whole script. It happened once while writing this change.
#
# (b) The df shim answers whatever it is asked, so `df_operands_all` is only
#     evidence if it can actually FAIL. Proved against a MUTANT copy of the
#     shipped gate whose production call measures the wrong path — the same
#     substitute-the-artifact idiom the canonical-pin helper uses, never a seam
#     in the shipped script.
# ===========================================================================
if bash -n "$GATE" 2>"$tmp/t-syntax.err"; then
  ok "gate-parses: the shipped agent-gate.sh parses (the embedded python is still quoted correctly)"
else
  bad "gate-parses: the shipped agent-gate.sh does NOT parse: $(head -1 "$tmp/t-syntax.err")"
fi

# THE MUTANT NEEDS A REPO, NOT JUST A FILE. `agent-gate.sh` does `cd "$(dirname "$0")/.."`
# and takes REPO_ROOT from there, so a copy dropped in $TMPDIR resolves REPO_ROOT=/tmp,
# `cargo metadata` fails, and the run never reaches df — which made the first version of
# this control pass for the WRONG REASON (zero df calls rather than a wrong operand). Its
# own zero-call assert is what caught that, which is why it is here. So the mutant lives in
# a SYMLINK FARM of the real checkout: every top-level entry symlinked, `scripts/` rebuilt
# with the mutated gate. Nothing is written into the worktree — a suite that runs inside
# `tooling-tests` must never mutate the tree its own gate is certifying (#2926).
t_root="$tmp/t-mutrepo"
da_farm "$t_root"
t_mutant="$t_root/scripts/agent-gate.sh"
sed 's|df -Pk "$path"|df -Pk /|' "$GATE" > "$t_mutant"
if grep -q 'df -Pk /' "$t_mutant" && ! grep -q 'df -Pk "$path"' "$t_mutant"; then
  ok "operand-guard CONTROL: the mutant gate was built (its production df call measures the WRONG path)"
else
  bad "operand-guard CONTROL: the mutant substitution did not take — the discrimination proof below would be vacuous"
fi
t_target="$tmp/t-target"; mkdir -p "$t_target"
t_script=$(df_script t "$HIGH")
mkdir -p "$tmp/t.run"
env "${DA_ENV_U[@]}" PATH="$tmp/shim:$PATH" \
  DF_SHIM_SCRIPT="$t_script" DF_SHIM_STATE="$tmp/t.dfstate" \
  CARGO_TARGET_DIR="$t_target" \
  AGENT_GATE_SUMMARY_FILE="$tmp/t.summary.txt" \
  CQLITE_GATE_STUB_RUNDIR="$tmp/t.run" CQLITE_GATE_STUB_SLEEP=1 \
  CQLITE_GATE_SLOTS_DIR="$tmp/t-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_POLL_SECS=0.3 \
  bash "$t_mutant" >"$tmp/t.out" 2>"$tmp/t.err"
if [ "$(df_calls t)" -ge 1 ]; then
  ok "operand-guard CONTROL: the mutant DID call df ($(df_calls t) time(s)) — so a red below is a mismatch, not an absence"
else
  bad "operand-guard CONTROL: the mutant made no df call at all; the discrimination proof would be vacuous"
fi
if df_operands_all t "$t_target"; then
  bad "operand-guard: df_operands_all PASSED a gate that measured $(df_operands t | sed -n 1p) instead of $t_target — the assertion cannot see a wrong operand and proves nothing"
else
  ok "operand-guard: df_operands_all REDS on a gate that measures the wrong path (observed operand: $(df_operands t | sed -n 1p))"
fi
# ...and it must not red on the correct one, or it would be a red that means nothing.
if df_operands_all k-build-td "$k_bt"; then
  ok "operand-guard: the same assertion PASSES the unmutated gate — it discriminates in both directions"
else
  bad "operand-guard: the assertion reds the correct gate too"
fi

# ===========================================================================
# Case U (roborev job 367, Medium): the MEASURED filesystem is the USED one —
# the resolved target dir is PINNED as CARGO_TARGET_DIR for every later cargo.
#
# Only the side lane reused the resolution; MAIN-lane cargo re-resolved config
# for itself, so an ancestor .cargo/config.toml or a CARGO_HOME change landing
# after the metadata call sent the build to an UNMEASURED filesystem. Two
# resolutions that have to agree is a TOCTOU hope; one the builds are pinned to
# is structural.
#
# Asserted against the REAL functions, extracted verbatim from the shipped gate.
# ===========================================================================
u_extract() { sed -n "/^$1() {/,/^}$/p" "$GATE"; }
for u_fn in _gate_disk_admission_pin_target_dir _gate_disk_admission_dispose; do
  if [ -n "$(u_extract "$u_fn")" ]; then
    ok "pin: extracted the REAL $u_fn from the shipped gate"
  else
    bad "pin: could not extract $u_fn — this case would be testing nothing"
  fi
done

# u_dispose_pin <target-dir>: run the REAL disposer on an OK measurement and print the
# CARGO_TARGET_DIR it leaves behind. Only the line renderer and the refuser are stubbed,
# and neither is the subject.
u_dispose_pin() {
  (
    set -uo pipefail
    _DA_TARGET_DIR="$1"; _DA_STATE=OK; _DA_WHY=""
    CARGO_TARGET_DIR="SENTINEL-UNSET"
    DISK_ADMISSION_LINE=""
    _gate_disk_admission_line() { DISK_ADMISSION_LINE="line"; }
    _gate_disk_admission_refuse() { printf 'REFUSED'; exit 0; }
    eval "$(u_extract _gate_disk_admission_pin_target_dir)"
    eval "$(u_extract _gate_disk_admission_dispose)"
    _gate_disk_admission_dispose "note" "detail" 2>/dev/null
    printf '%s' "$CARGO_TARGET_DIR"
  ) 2>/dev/null
}
u_got=$(u_dispose_pin /some/resolved/target)
if [ "$u_got" = /some/resolved/target ]; then
  ok "pin: a binding resolution EXPORTS CARGO_TARGET_DIR=<resolved dir> for every later cargo"
else
  bad "pin: expected CARGO_TARGET_DIR=/some/resolved/target, got '${u_got:-<empty>}'"
fi
# CHECK 3 of the brief: pin NOTHING when the resolution itself failed — inventing a target
# dir from a failed resolution would move the build somewhere nobody chose.
u_none=$(u_dispose_pin "")
if [ "$u_none" = SENTINEL-UNSET ]; then
  ok "pin: an UNRESOLVED target dir pins NOTHING — the caller's value is left untouched"
else
  bad "pin: a failed resolution still wrote CARGO_TARGET_DIR='$u_none'"
fi

# CHECK 1 of the brief: DEMONSTRATED, not reasoned. The pinned value came from cargo, so on
# a lane with no target-dir configuration it must be exactly what cargo chooses unaided.
u_unaided=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])' 2>/dev/null)
if [ -n "$u_unaided" ]; then
  u_repinned=$(CARGO_TARGET_DIR="$u_unaided" cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])' 2>/dev/null)
  if [ "$u_repinned" = "$u_unaided" ]; then
    ok "pin: NO-OP in the normal case — pinning cargo's own answer resolves to the same path ($u_unaided)"
  else
    bad "pin: pinning changed the resolution ($u_unaided -> ${u_repinned:-<none>}) — it is not a no-op"
  fi
else
  skip "pin: cargo metadata unavailable here — the no-op demonstration could not run"
fi

# CHECK 2 of the brief: NESTED CHILDREN. A nested gate inherits the exported value; the
# property that must survive is that the child MEASURES what it will BUILD into. Driven for
# real: a stub gate run with CARGO_TARGET_DIR already set, exactly as a nested child sees it.
u_inherited="$tmp/u-inherited-target"; mkdir -p "$u_inherited"
u_script=$(df_script u "$HIGH")
run_stub_gate u "$u_script" \
  CARGO_TARGET_DIR="$u_inherited" \
  CQLITE_GATE_SLOTS_DIR="$tmp/u-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
u_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; u_status=$WX_STATUS
assert_no_timeout "u nested inheritance"
u_line=$(grep_line "$u_err" '^agent-gate: disk-admission: ')
case "$u_line" in
  *"target-dir $u_inherited (via cargo metadata)"*)
    ok "pin/nested: a child that INHERITS the pin measures the inherited directory — measured==used holds for the child too" ;;
  *) bad "pin/nested: the child measured something other than its inherited target dir: ${u_line:-<none>}" ;;
esac
if df_operands_all u "$u_inherited"; then
  ok "pin/nested: and every df call was made against that same inherited directory"
else
  bad "pin/nested: df measured '$(df_operands u | sed -n 1p)', not the inherited $u_inherited"
fi

# ===========================================================================
# Case V (roborev job 373, High): the suite is verified in the environment
# `tooling-tests` GIVES it, not only the one a human launches it in.
#
# A suite that can only be trusted when launched by hand is not trustworthy in
# the component that runs it. So the hostile environment is part of this suite's
# own coverage: an inherited variable must red HERE, not in someone's gate of
# record.
# ===========================================================================
# (a) STRUCTURAL: every gate invocation in this file must go through the isolation list.
#     A behavioural case only covers the variables someone already thought of; this covers
#     an invocation someone adds later.
v_unisolated=$(awk '
  /bash "\$GATE"|bash "\$t_mutant"/ {
    blk = $0
    for (j = NR - 1; j >= 1 && lines[j] ~ /\\$/; j--) blk = lines[j] "\n" blk
    if (blk !~ /DA_ENV_U/) { print NR; n++ }
  }
  { lines[NR] = $0 }
' "$0" | tr '\n' ' ')
if [ -z "$(printf '%s' "$v_unisolated" | tr -d '[:space:]')" ]; then
  ok "env-isolation: every gate invocation in this file routes through the DA_ISOLATE list"
else
  bad "env-isolation: UNISOLATED gate invocation(s) at line(s): $v_unisolated — an inherited variable would reach that case"
fi

# (b) BEHAVIOURAL: poison the SUITE's own environment exactly as a parent gate does, then
#     run a resolution case and require it to measure ITS OWN target dir. This is the
#     reproduction that found the defect, turned into coverage.
v_poison_target="$tmp/v-poison"; mkdir -p "$v_poison_target"
v_case_target="$tmp/v-case"; mkdir -p "$v_case_target"
v_poison_home="$tmp/v-poison-home"; mkdir -p "$v_poison_home"
printf '[build]\ntarget-dir = "%s"\n' "$v_poison_target" > "$v_poison_home/config.toml"
v_run() {
  local label="$1"; shift
  local sc; sc=$(df_script "$label" "$HIGH")
  run_stub_gate "$label" "$sc" \
    CARGO_TARGET_DIR="$v_case_target" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=1
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120
  assert_no_timeout "$label"
  grep_line "$RS_ERR" '^agent-gate: disk-admission: '
}
# ONLY THE VARIABLES THAT ARE *CLEARED* BELONG IN THIS LOOP. CARGO_HOME is no longer one
# of them — it is SET (the positive construction), so poisoning it here would prove nothing
# about the clearing, and worse, this case USED to pass for the wrong reason: `v_run` passes
# an explicit CARGO_TARGET_DIR, which is highest precedence and beats any CARGO_HOME config,
# so the assertion held without the isolation doing anything. The CARGO_HOME/$HOME axis is
# covered honestly by (b2) above, which sets no CARGO_TARGET_DIR at all.
for v_var in CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR; do
  export "$v_var=$v_poison_target"
  v_line=$(v_run "v-${v_var}")
  unset "$v_var"
  case "$v_line" in
    *"target-dir $v_case_target "*)
      ok "env-isolation[$v_var]: an inherited $v_var does NOT decide what the case measures" ;;
    *"target-dir $v_poison_target "*)
      bad "env-isolation[$v_var]: the INHERITED value won — the case measured $v_poison_target, not its own $v_case_target" ;;
    *) bad "env-isolation[$v_var]: unexpected rendering: ${v_line:-<none>}" ;;
  esac
  if df_operands_all "v-${v_var}" "$v_case_target"; then
    ok "env-isolation[$v_var]: and every df call measured the case's own directory"
  else
    bad "env-isolation[$v_var]: df measured '$(df_operands "v-${v_var}" | sed -n 1p)'"
  fi
done

# (b2) THE $HOME AXIS (job 389). `unset CARGO_HOME` sends cargo to `$HOME/.cargo/config.toml`,
#      so a caller with a user-level `build.target-dir` made this GATE-WIRED suite fail as a
#      function of whose box it ran on. Both directions are measured on ONE planted config:
#      the pre-fix environment must resolve the planted dir (proving the hazard is real and
#      this fixture reaches it), and the shipped isolated CARGO_HOME must not.
v_home="$tmp/v-fake-home"; mkdir -p "$v_home/.cargo"
v_user_target="$tmp/v-user-level-target"
printf '[build]\ntarget-dir = "%s"\n' "$v_user_target" > "$v_home/.cargo/config.toml"
v_prefix=$(env -u CARGO_HOME HOME="$v_home" cargo metadata --no-deps --format-version 1 2>/dev/null \
  | python3 -c 'import json,sys;sys.stdout.write(json.load(sys.stdin)["target_directory"])' 2>/dev/null)
# SAME CLASS AS THE k-default DERIVATION: this is a single unretried cargo call, so a
# transient makes it EMPTY. An empty answer means the control could not be MEASURED, which
# is a skip; only a NON-EMPTY answer that is the WRONG path is a real failure of the fixture.
if [ "$v_prefix" = "$v_user_target" ]; then
  ok "env-isolation[\$HOME] CONTROL: with CARGO_HOME UNSET, cargo reads \$HOME/.cargo and resolves $v_user_target — the pre-fix hazard, reproduced"
elif [ -z "$v_prefix" ]; then
  skip "env-isolation[\$HOME] CONTROL: cargo did not answer, so the pre-fix hazard could not be reproduced here — the shipped assertion below is reported but is not a differential"
else
  bad "env-isolation[\$HOME] CONTROL: the pre-fix environment resolved '$v_prefix', not the planted $v_user_target — this fixture does not reach the defect, so the assertion below proves nothing"
fi
# The shipped suite: the same poisoned HOME, but CARGO_HOME isolated. k-default must still
# agree with cargo's unconfigured answer.
v_hg_script=$(df_script v-home "$HIGH")
HOME="$v_home" run_stub_gate v-home "$v_hg_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/v-home-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
v_home_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120
assert_no_timeout "v-home"
v_home_line=$(grep_line "$v_home_err" '^agent-gate: disk-admission: ')
if [ "$K_HAVE_DEFAULT" -eq 0 ]; then
  skip "env-isolation[\$HOME]: skipped — it compares against the underivable unconfigured target dir"
else
case "$v_home_line" in
  *"target-dir $K_PREFIX_DEFAULT "*)
    ok "env-isolation[\$HOME]: a user-level build.target-dir does NOT move the measurement — the isolated CARGO_HOME holds" ;;
  *"target-dir $v_user_target "*)
    bad "env-isolation[\$HOME]: the user-level config WON — this suite would red on any box that has one" ;;
  *) bad "env-isolation[\$HOME]: unexpected rendering: ${v_home_line:-<none>}" ;;
esac
if df_operands_all v-home "$K_PREFIX_DEFAULT"; then
  ok "env-isolation[\$HOME]: and every df call measured that same directory"
else
  bad "env-isolation[\$HOME]: df measured '$(df_operands v-home | sed -n 1p)'"
fi
fi

# (b3) THE POISON CASES MUST NOT DAMAGE THE ISOLATION THEY TEST AGAINST. Found the hard
#      way: an earlier version of the loop above poisoned CARGO_HOME and then `unset` it,
#      which destroyed the suite-wide isolated value for EVERY LATER CASE — (b2) then failed
#      with "the user-level config WON" for a reason that had nothing to do with the shipped
#      code. So the invariant is asserted here, after the poisoning, rather than assumed.
if [ "${CARGO_HOME:-}" = "$DA_CARGO_HOME" ]; then
  ok "env-isolation: the isolated CARGO_HOME survived the poison cases — later cases still run isolated"
else
  bad "env-isolation: a poison case left CARGO_HOME='${CARGO_HOME:-<unset>}' instead of $DA_CARGO_HOME — every case after it runs unisolated"
  export CARGO_HOME="$DA_CARGO_HOME"
fi

# (c) THE OPERAND GUARD MUST DISCRIMINATE IN BOTH DIRECTIONS *UNDER A PIN*. Reddening a
#     CORRECT gate is as broken as greening a wrong one, and that is precisely how this
#     defect surfaced. Re-checked here with the poison in place.
export CARGO_TARGET_DIR="$v_poison_target"
v_ok_line=$(v_run v-guard-correct)
unset CARGO_TARGET_DIR
if df_operands_all v-guard-correct "$v_case_target"; then
  ok "env-isolation: df_operands_all GREENS the correct gate even with a pin exported"
else
  bad "env-isolation: df_operands_all REDS a correct gate under an exported pin — the control misfires"
fi
if df_operands_all t "$t_target"; then
  bad "env-isolation: df_operands_all still greens the MUTANT — it discriminates in neither direction"
else
  ok "env-isolation: ...and still REDS the mutant — both directions, under a pinned environment"
fi

# ===========================================================================
# Case W (roborev job 373, Medium): a NEGATIVE Available is the worst reading
# there is, not an unparsable one.
#
# df legitimately reports a negative Available on an overcommitted filesystem or
# one dipping into reserved blocks. Classifying it unparsable produced a
# non-fatal UNMEASURED and let the build proceed in the most severe low-space
# condition that exists — a false admission at the worst possible moment.
# ===========================================================================
W_NEG=-1048576   # -1 GiB
# THE POSITIVE CONTROL: the PRE-FIX validators, reproduced verbatim, on the same value.
if printf '%s' "$W_NEG" | grep -qE '^[0-9]+$'; then
  bad "negative-avail CONTROL: the pre-fix unsigned pattern ACCEPTED $W_NEG; this case does not demonstrate the defect"
else
  ok "negative-avail CONTROL: the PRE-FIX unsigned pattern REJECTED $W_NEG — it became UNMEASURED, i.e. non-fatal, i.e. admitted"
fi
w_script=$(df_script w "$W_NEG")
run_stub_gate w "$w_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/w-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
w_err=$RS_ERR; w_sum=$RS_SUMMARY
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; w_status=$WX_STATUS; w_markers=$WX_MARKERS
assert_no_timeout "w negative available"
w_line=$(grep_line "$w_err" '^agent-gate: disk-admission: ')
if [ "$w_status" -ne 0 ] && [ "$w_markers" -eq 0 ]; then
  ok "negative-avail: a negative Available REFUSES and never begins work — a binding refusal, not a parse"
else
  bad "negative-avail: the run PROCEEDED on a negative Available (exit $w_status, markers $w_markers)"
fi
case "$w_line" in
  *'FAIL-CLOSED (#3755)'*'-1.0GiB(BELOW BAR)'*)
    ok "negative-avail: read as a MEASUREMENT below the bar, and rendered as the negative it is" ;;
  *'UNMEASURED (df-unparsable)'*)
    bad "negative-avail: still classified unparsable — the worst reading there is, treated as no reading: $w_line" ;;
  *) bad "negative-avail: unexpected rendering: ${w_line:-<none>}" ;;
esac
if grep -qx 'RESULT: FAIL' "$w_sum" 2>/dev/null; then
  ok "negative-avail: RESULT: FAIL in the emitted block"
else
  bad "negative-avail: no exact 'RESULT: FAIL' line"
fi

# ===========================================================================
# Case X (roborev job 389, Low): an over-precise decimal bar cannot round into a
# false admission.
#
# The grammar accepted arbitrarily precise decimals while the comparison runs in
# doubles, so `40.0000000000000000001` converted to exactly 40 and a filesystem
# with EXACTLY 40 GiB free PASSED a floor set strictly above 40 — same direction
# as the round-5 %d saturation. Refused rather than rounded, because the loss
# happens in the TEXT->double conversion, before any arithmetic this code does.
# ===========================================================================
X_EXACT_40_KIB=41943040        # exactly 40 GiB, the reading that must be refused
X_OVERPRECISE=40.0000000000000000001

# THE POSITIVE CONTROL: the shipped comparator, on the raw value, admits it. This is the
# defect itself — the comparison cannot see the difference, which is why the BAR is refused
# upstream instead.
if awk -v k="$X_EXACT_40_KIB" -v g="$X_OVERPRECISE" 'BEGIN { exit ((k+0) >= (g*1048576)) ? 0 : 1 }'; then
  ok "bar-precision CONTROL: the float comparison ADMITS exactly 40GiB against a >40 bar — the defect is real and lives in the text->double conversion"
else
  bad "bar-precision CONTROL: the comparison already refused $X_OVERPRECISE; this case does not demonstrate the defect"
fi

x_run() {
  local label="$1" bar="$2" avail="$3"
  local sc; sc=$(df_script "$label" "$avail")
  run_stub_gate "$label" "$sc" \
    CQLITE_GATE_MIN_FREE_GB="$bar" \
    CQLITE_GATE_SLOTS_DIR="$tmp/$label-slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
    CQLITE_GATE_STUB_SLEEP=2
  X_ERR=$RS_ERR
  watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; X_STATUS=$WX_STATUS; X_MARKERS=$WX_MARKERS
  assert_no_timeout "$label"
  X_LINE=$(grep_line "$X_ERR" '^agent-gate: disk-admission: ')
}

# The over-precise bar REFUSES, before measuring, under its own token.
x_run x-overprecise "$X_OVERPRECISE" "$X_EXACT_40_KIB"
if [ "$X_STATUS" -ne 0 ] && [ "$X_MARKERS" -eq 0 ]; then
  ok "bar-precision: an over-precise bar REFUSES and never begins work"
else
  bad "bar-precision: exactly 40GiB was ADMITTED against a >40 bar (exit $X_STATUS, markers $X_MARKERS)"
fi
case "$X_LINE" in
  *'BAR-TOO-PRECISE-FAIL-CLOSED (#3755)'*)
    ok "bar-precision: its OWN token, distinct from the out-of-range and the measurement refusals" ;;
  *'BAR-UNREPRESENTABLE'*)
    bad "bar-precision: reported as out-of-range — a different operator mistake with a different remedy: $X_LINE" ;;
  *) bad "bar-precision: expected BAR-TOO-PRECISE-FAIL-CLOSED, got: ${X_LINE:-<none>}" ;;
esac
if [ "$(df_calls x-overprecise)" -eq 0 ]; then
  ok "bar-precision: refused BEFORE any df call — an unusable bar needs no measurement"
else
  bad "bar-precision: measured $(df_calls x-overprecise) time(s) before refusing on an unusable bar"
fi
if grep -q "at most 3 decimal places" "$X_ERR" 2>/dev/null; then
  ok "bar-precision: the remedy names the accepted precision"
else
  bad "bar-precision: no stderr line naming the accepted precision"
fi

# THE BOUNDARY, both sides — so the bound is pinned rather than merely present.
x_run x-3dp 40.001 "$X_EXACT_40_KIB"
if [ "$X_STATUS" -ne 0 ] && [ "$X_MARKERS" -eq 0 ]; then
  case "$X_LINE" in
    *'BAR-TOO-PRECISE'*) bad "bar-precision[3dp]: 40.001 was refused as over-precise — the bound is off by one" ;;
    *'FAIL-CLOSED (#3755)'*'bar 40.001GiB(pinned)'*)
      ok "bar-precision[3dp]: 3 decimals are ACCEPTED and applied — exactly 40GiB correctly refuses a 40.001 floor" ;;
    *) bad "bar-precision[3dp]: unexpected rendering: ${X_LINE:-<none>}" ;;
  esac
else
  bad "bar-precision[3dp]: a 40.001 floor ADMITTED exactly 40GiB (exit $X_STATUS)"
fi
x_run x-4dp 40.0001 "$X_EXACT_40_KIB"
case "$X_LINE" in
  *'BAR-TOO-PRECISE-FAIL-CLOSED (#3755)'*)
    ok "bar-precision[4dp]: 4 decimals are REFUSED — the bound is exactly where it is documented" ;;
  *) bad "bar-precision[4dp]: expected a too-precise refusal at 4 decimals, got: ${X_LINE:-<none>}" ;;
esac
# NOT AN OVER-CORRECTION: ordinary fractional bars still work. `0.1` in particular is not
# exactly representable as a double, and refusing it would have reddened correct input —
# which is why the bound is a digit count and not an exact-representability test.
x_run x-half 0.5 "$HIGH"
if [ "$X_STATUS" -eq 0 ] && [ "$X_MARKERS" -ge 1 ]; then
  ok "bar-precision: a 0.5GiB bar still PASSES a spacious filesystem (the bound does not red correct input)"
else
  bad "bar-precision: a legitimate 0.5GiB bar was refused (exit $X_STATUS)"
fi
x_run x-tenth 0.1 "$HIGH"
case "$X_LINE" in
  *'bar 0.1GiB(pinned)'*)
    ok "bar-precision: 0.1GiB — not exactly representable as a double — is ACCEPTED, because a digit bound admits it safely" ;;
  *) bad "bar-precision: 0.1 was not accepted as pinned: ${X_LINE:-<none>}" ;;
esac

# ===========================================================================
# Case Y (roborev job 390, Medium): admission never mutates a TRACKED file, so
# the gate cannot absorb its own write into the certification baseline.
#
# `cargo metadata` without `--locked` is PERMITTED to create or update
# `Cargo.lock`, and this probe runs BEFORE `_tree_recapture_after_slot` — so such
# a write would land before the certification window is captured and be ABSORBED
# rather than caught. `tree-integrity` makes a mid-run mutation fatal; one before
# the recapture is invisible to it.
#
# THE REACHABILITY CONTROL COULD NOT BE PRODUCED ON THIS TOOLCHAIN, and that is
# reported rather than simulated (the same disposition as quota in Case W). See
# the skip below for the four measurements.
# ===========================================================================
y_repo=$(cd "$SCRIPT_DIR/../.." && pwd)

# (a) STRUCTURAL, and on this toolchain it is the assertion that CARRIES the guarantee:
#     since the behavioural control cannot discriminate here, a source assert is the only
#     thing that reds if someone drops `--locked`.
if grep -q 'cargo metadata --no-deps --locked --format-version 1' "$GATE"; then
  ok "lockfile: the shipped metadata probe passes --locked, so it may not write Cargo.lock"
else
  bad "lockfile: the metadata probe does NOT pass --locked — it may create or update Cargo.lock before the certification window opens"
fi
# The cause must be its own string: four operator situations (df / target-dir / bar /
# lockfile), four strings.
if grep -q 'target-dir-lockfile-stale-or-metadata-failed' "$GATE"; then
  ok "lockfile: a --locked refusal has its OWN cause naming the lockfile, distinct from the df, bar and other target-dir causes"
else
  bad "lockfile: no distinct cause naming the lockfile — a stale lockfile would report as a generic probe failure"
fi

# (b) BEHAVIOURAL: the digest of Cargo.lock, and the whole tracked-file census, across a
#     REAL admission run. Asserted from the artifacts themselves, never from the absence of
#     a complaint.
if [ "$DA_SHA_TOOL" = none ]; then
  bad "lockfile: NO digest tool (sha256sum / shasum / git hash-object) — the immutability assertions cannot be made, and must not be reported as passing"
else
  ok "lockfile: digest tool resolved ($DA_SHA_TOOL) via _tree_digest_file's ladder — portable off GNU coreutils"
fi
y_lock_before=$(da_digest < "$y_repo/Cargo.lock" 2>/dev/null) || y_lock_before=""
y_status_before=$( (cd "$y_repo" && git status --porcelain 2>/dev/null) | da_digest 2>/dev/null ) || y_status_before=""
y_script=$(df_script y "$HIGH")
run_stub_gate y "$y_script" \
  CQLITE_GATE_SLOTS_DIR="$tmp/y-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
y_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; y_status=$WX_STATUS
assert_no_timeout "y lockfile immutability"
y_lock_after=$(da_digest < "$y_repo/Cargo.lock" 2>/dev/null) || y_lock_after=""
y_status_after=$( (cd "$y_repo" && git status --porcelain 2>/dev/null) | da_digest 2>/dev/null ) || y_status_after=""
# The run must have actually reached the probe, or "nothing changed" proves nothing.
if [ "$(df_calls y)" -ge 1 ]; then
  ok "lockfile CONTROL: the run reached the admission probe ($(df_calls y) df call(s)) — so an unchanged lockfile means something"
else
  bad "lockfile CONTROL: the run never reached the probe; the immutability assertions below would be vacuous"
fi
if [ -n "$y_lock_before" ] && [ "$y_lock_before" = "$y_lock_after" ]; then
  ok "lockfile: Cargo.lock's DIGEST is unchanged across a full admission run"
elif [ -z "$y_lock_before" ]; then
  bad "lockfile: could not digest $y_repo/Cargo.lock — the assertion could not be made"
else
  bad "lockfile: Cargo.lock CHANGED across admission — the gate mutated a tracked file before its own certification window opened"
fi
if [ -z "$y_status_before" ] || [ -z "$y_status_after" ]; then
  bad "lockfile: the tracked-file census could not be digested (before='${y_status_before:-<none>}' after='${y_status_after:-<none>}') — two empty strings compare EQUAL, which is the vacuous pass this must never be"
elif [ "$y_status_before" = "$y_status_after" ]; then
  ok "lockfile: the whole tracked-file census is unchanged across admission (broader than Cargo.lock, which is the property that matters)"
else
  bad "lockfile: admission changed the tracked-file census — some tracked path was written before the certification window opened"
fi

# (c) THE REACHABILITY CONTROL, reported as unavailable rather than faked. Measured four
#     ways on the pinned toolchain, each with the lockfile ABSENT (the most favourable
#     condition for a write): with and without `--offline`, network available and not.
#     Every one left Cargo.lock absent at rc 0 — `--no-deps` does not write it. So the
#     pre-fix invocation cannot be shown mutating anything HERE, and `--locked` is
#     converting an incidental, undocumented property into an explicit one rather than
#     fixing an observed write.
y_farm="$tmp/y-farm"; mkdir -p "$y_farm"
for y_e in "$y_repo"/* "$y_repo"/.[!.]*; do
  y_b=$(basename "$y_e"); [ "$y_b" = Cargo.lock ] && continue
  ln -s "$y_e" "$y_farm/$y_b" 2>/dev/null || true
done
( cd "$y_farm" && timeout 300 cargo metadata --no-deps --offline --format-version 1 >/dev/null 2>&1 )
if [ -f "$y_farm/Cargo.lock" ]; then
  ok "lockfile CONTROL: the PRE-FIX invocation CREATED Cargo.lock from absent — the defect is reachable and the assertions above are a differential"
else
  skip "lockfile CONTROL: the pre-fix invocation did NOT write Cargo.lock even with the file ABSENT — reachability is not demonstrable on this cargo, so (a) is what carries the guarantee"
fi

# ===========================================================================
# Case Z (roborev job 392, Medium): no verdict and no rendered number varies with
# the locale.
#
# awk converts and formats numbers under LC_NUMERIC. A guard may not have a
# verdict that depends on LANG — the same reasoning that made the mkdir
# classifier report `errno.errorcode` instead of parsing mkdir's message.
#
# No comma-decimal locale is INSTALLED here, so one is BUILT privately with
# `localedef` + `LOCPATH`; where that is impossible the case reports a `skip -`
# naming what could not be exercised, as with quota and Case Y's reachability.
# ===========================================================================
z_loc="$tmp/z-locale"; mkdir -p "$z_loc"
z_have_locale=0
if command -v localedef >/dev/null 2>&1 && [ -f /usr/share/i18n/locales/de_DE ]; then
  if localedef -i de_DE -f UTF-8 "$z_loc/de_DE.UTF-8" >/dev/null 2>&1; then z_have_locale=1; fi
fi
if [ "$z_have_locale" -eq 0 ]; then
  skip "locale: no comma-decimal locale is installed and none could be built (localedef or /usr/share/i18n/locales/de_DE absent) — the locale axis could not be exercised on this host"
else
  ok "locale: built a private comma-decimal locale to test against (none is installed here)"
  # EXPORTED, not passed as a prefix: without it in the environment before the assignment,
  # bash's own setlocale fails and emits a `warning: setlocale` line for every case below —
  # noise in the gate log. The locale is equally active either way (measured).
  export LOCPATH="$z_loc"

  # (a) THE CONTROL: find an awk that ACTUALLY mis-renders under that locale, so a pass
  #     below is a differential and not a property of every awk. Measured across all of
  #     them: mawk emits `200,0`; gawk, nawk and busybox emit `200.0`.
  z_bad_awk=""
  for z_a in mawk awk nawk busybox; do
    command -v "$z_a" >/dev/null 2>&1 || continue
    case "$z_a" in busybox) z_cmd="busybox awk" ;; *) z_cmd="$z_a" ;; esac
    z_out=$(LOCPATH="$z_loc" LC_ALL=de_DE.UTF-8 $z_cmd 'BEGIN{printf "%.1f", 209715200/1048576}' 2>/dev/null)
    if [ "$z_out" = "200,0" ]; then z_bad_awk="$z_cmd"; break; fi
  done
  if [ -n "$z_bad_awk" ]; then
    ok "locale CONTROL: '$z_bad_awk' renders 200,0 under that locale when UNPINNED — the defect is real and this fixture reaches it"
  else
    skip "locale CONTROL: no awk on this host mis-renders under the built locale — the render half is not demonstrable here (the pin is still asserted below)"
  fi

  # (b) THE SHIPPED RENDERER, extracted verbatim, must be immune.
  # THE IN-SHAPE CONTROL. The z_bad_awk probe above used a bare awk command; this proves the
  # locale is active in the EXACT shape case (b) uses — a shell function called with a
  # prefix assignment — so a pass below is the PIN working and not the fixture failing to
  # reach the defect. (Measured: the unpinned form yields 200,0GiB here.)
  if [ -n "$z_bad_awk" ]; then
    mkdir -p "$tmp/z-awkbin-ctl"
    printf '#!/usr/bin/env bash\nexec %s "$@"\n' "$z_bad_awk" > "$tmp/z-awkbin-ctl/awk"
    chmod +x "$tmp/z-awkbin-ctl/awk"
    z_unpinned=$(
      _z_unpinned_render() { awk -v k="$1" 'BEGIN { printf "%.1fGiB", k/1048576 }'; }
      PATH="$tmp/z-awkbin-ctl:$PATH" LC_ALL=de_DE.UTF-8 _z_unpinned_render 209715200
    )
    if [ "$z_unpinned" = "200,0GiB" ]; then
      ok "locale CONTROL: an UNPINNED renderer in the SAME invocation shape emits 200,0GiB — the locale really is active in case (b) below"
    else
      bad "locale CONTROL: the unpinned renderer emitted '$z_unpinned' in this shape, so the locale is NOT active here and case (b) would pass vacuously"
    fi
  fi
  z_render=$(
    eval "$(sed -n '/^_gate_gib_render() {/,/^}$/p' "$GATE")"
    PATH="${z_bad_awk:+$tmp/z-awkbin-ctl:}$PATH" LC_ALL=de_DE.UTF-8 _gate_gib_render 209715200
  )
  if [ "$z_render" = "200.0GiB" ]; then
    ok "locale: the SHIPPED _gate_gib_render emits 200.0GiB under a comma locale (LC_ALL=C is pinned at the site)"
  else
    bad "locale: the shipped renderer emitted '$z_render' under a comma locale — the number in the SUMMARY varies with LANG"
  fi

  # (c) THE SHIPPED COMPARATORS must be immune too. 40.25GiB free against a 40.5GiB bar
  #     must REFUSE (rc 1) whatever the locale; a truncation of the bar to 40 would admit.
  z_cmp=$(
    eval "$(sed -n '/^_gate_disk_admission_clears_bar() {/,/^}$/p' "$GATE")"
    PATH="${z_bad_awk:+$tmp/z-awkbin-ctl:}$PATH" LC_ALL=de_DE.UTF-8 \
      _gate_disk_admission_clears_bar 42204528 40.5; printf '%s' "$?"
  )
  if [ "$z_cmp" = 1 ]; then
    ok "locale: the SHIPPED comparator still refuses 40.25GiB against a 40.5GiB bar under a comma locale"
  else
    bad "locale: the comparator returned rc=$z_cmp — a fractional bar was mis-parsed, which admits a filesystem below the floor"
  fi

  # (d) END TO END, through the implementation that actually mis-renders: a real gate run
  #     with that awk first on PATH and the comma locale active must still PASS and still
  #     print a dot-decimal reading.
  if [ -n "$z_bad_awk" ]; then
    mkdir -p "$tmp/z-awkbin"
    printf '#!/usr/bin/env bash\nexec %s "$@"\n' "$z_bad_awk" > "$tmp/z-awkbin/awk"
    chmod +x "$tmp/z-awkbin/awk"
    z_script=$(df_script z "$HIGH")
    RS_PATH_PREFIX="$tmp/z-awkbin"
    LOCPATH="$z_loc" LC_ALL=de_DE.UTF-8 run_stub_gate z "$z_script" \
      CQLITE_GATE_SLOTS_DIR="$tmp/z-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
    RS_PATH_PREFIX=""
    z_err=$RS_ERR
    watch_until_exit "$RS_PID" "$RS_RUNDIR" 120; z_status=$WX_STATUS; z_markers=$WX_MARKERS
    assert_no_timeout "z locale end-to-end"
    z_line=$(grep_line "$z_err" '^agent-gate: disk-admission: ')
    case "$z_line" in
      *'200.0GiB'*)
        ok "locale: a REAL gate run under a comma locale with '$z_bad_awk' as awk reports 200.0GiB and behaves normally" ;;
      *'200,0GiB'*)
        bad "locale: a real gate run emitted 200,0GiB — the pin does not reach every render site: $z_line" ;;
      *) bad "locale: unexpected rendering under a comma locale: ${z_line:-<none>}" ;;
    esac
    if [ "$z_status" -eq 0 ] && [ "$z_markers" -ge 1 ]; then
      ok "locale: ...and the run proceeded normally (exit 0), so the pin costs nothing"
    else
      bad "locale: the run under a comma locale did not complete normally (exit $z_status)"
    fi
  else
    skip "locale: end-to-end not run — no awk here mis-renders, so it would exercise nothing the extracted-renderer case above does not"
  fi
  unset LOCPATH
fi

# ===========================================================================
# Case AA (roborev job 394, Medium): EVERY OSError from the write probe is
# binding — the errno's membership in a list does not decide fatality.
#
# There used to be an allowlist, so EIO, ESTALE and ENODEV — definitive write
# failures — fell through to non-fatal UNMEASURED and the build proceeded AFTER
# ITS OWN WRITE PROBE HAD FAILED. Driven with a REAL forced errno rather than a
# reasoned one: sitecustomize.py makes os.makedirs raise EIO inside the SHIPPED
# classifier, extracted verbatim from the gate.
# ===========================================================================
aa_cls="$tmp/aa-classifier.py"
sed -n '/^import errno, os, sys$/,/^# END-WRITE-PROBE$/p' "$GATE" > "$aa_cls"
if [ -s "$aa_cls" ] && grep -q 'CANNOT-WRITE' "$aa_cls"; then
  ok "errno-source: extracted the SHIPPED write-probe classifier from the gate"
else
  bad "errno-source: could not extract the classifier — this case would test nothing"
fi
# PLANTED VIA THE ISOLATED ARGV, not PYTHONPATH (roborev job 416): the shipped probe now
# runs `python3 -I -S`, so a sitecustomize reaches nothing — see the da_run_probe header.
cat > "$tmp/aa-prelude.py" <<'AASC'
import os, errno
def boom(*a, **k):
    raise OSError(errno.EIO, "simulated I/O error")
os.makedirs = boom
AASC
aa_got=$(da_run_probe "$tmp/aa-prelude.py" "$aa_cls" "$tmp/aa-target")
# THE POSITIVE CONTROL: the PRE-FIX allowlist, reproduced verbatim, on the SAME errno. It
# must classify EIO as UNCLASSIFIED — i.e. non-fatal — or this case is not a differential.
aa_pre=$(python3 - <<'AAPRE' 2>/dev/null
import errno, sys
B = set()
for n in ("ENOSPC","EDQUOT","EROFS","EACCES","EPERM","ENOTDIR","EEXIST","ELOOP","ENAMETOOLONG"):
    v = getattr(errno, n, None)
    if v is not None: B.add(v)
e = errno.EIO
sys.stdout.write(("CANNOT-WRITE " if e in B else "UNCLASSIFIED ") + errno.errorcode[e])
AAPRE
)
case "$aa_pre" in
  'UNCLASSIFIED EIO')
    ok "errno-source CONTROL: the PRE-FIX allowlist classified EIO as UNCLASSIFIED — non-fatal, so the build proceeded after a failed write probe" ;;
  *) bad "errno-source CONTROL: the pre-fix allowlist gave '$aa_pre'; this case does not demonstrate the defect" ;;
esac
case "$aa_got" in
  'CANNOT-WRITE EIO')
    ok "errno-source: the SHIPPED classifier calls a forced EIO CANNOT-WRITE — the SOURCE of the error decides, not a list" ;;
  'UNCLASSIFIED'*)
    bad "errno-source: an OSError from the write probe is still UNCLASSIFIED ('$aa_got') — the build would proceed after its own probe failed" ;;
  *) bad "errno-source: unexpected classification '$aa_got'" ;;
esac
# And the same for a forced failure of the OPEN/WRITE half, not just the creation.
cat > "$tmp/aa-prelude2.py" <<'AASC2'
import os, errno
_real = os.open
def boom(*a, **k):
    raise OSError(errno.ESTALE, "simulated stale handle")
os.open = boom
AASC2
aa_got2=$(da_run_probe "$tmp/aa-prelude2.py" "$aa_cls" "$tmp/aa-target2")
case "$aa_got2" in
  'CANNOT-WRITE ESTALE')
    ok "errno-source: a forced ESTALE from the OPEN half is binding too (both halves of the probe, not just creation)" ;;
  'CANNOT-WRITE ESTALE LEFTOVER '*)
    bad "errno-source: a stray was DECLARED although open() failed, so no file was ever created — a false statement about an artifact that does not exist: $aa_got2" ;;
  *) bad "errno-source: the open/write half gave '$aa_got2', expected CANNOT-WRITE ESTALE" ;;
esac
# AND THE ARTIFACT CENSUS BACKS IT: the directory must be empty, so the payload above is not
# merely worded right but TRUE. This assertion is here because the first draft of job 398's
# cleanup keyed the declaration on the intended PATH rather than on whether the file was
# CREATED, and claimed a stray that never existed.
aa_files=$(ls -A "$tmp/aa-target2" 2>/dev/null | grep -c 'agent-gate-writeprobe' || true)
aa_files="${aa_files%%$'\n'*}"; case "$aa_files" in ''|*[!0-9]*) aa_files=0 ;; esac
if [ "$aa_files" -eq 0 ]; then
  ok "errno-source: and the directory really is empty — a payload claiming no stray is TRUE, not just well-worded"
else
  bad "errno-source: $aa_files artifact(s) actually present although the payload claimed none"
fi
# NOT AN OVER-CORRECTION: a writable directory must still come back OK.
aa_ok=$(da_run_probe "" "$aa_cls" "$tmp/aa-good")
if [ "$aa_ok" = OK ] && [ -d "$tmp/aa-good" ]; then
  ok "errno-source: a writable target still classifies OK (the rule did not become 'always refuse')"
else
  bad "errno-source: a writable target classified '$aa_ok'"
fi

# ===========================================================================
# Case AB (roborev job 394, Medium): the BLOCKING cap setup happens BEFORE the
# binding measurement, so a slow `mkdir` cannot make the bound reading stale.
#
# Round 329's rule binds the launch reading on cap-inactive paths BECAUSE no time
# has elapsed between it and the build. A blocking `mkdir -p "$CQLITE_GATE_SLOTS_DIR"`
# — a network-backed dir is the obvious case — broke that premise. Fixed by
# ORDER, and the order is asserted from OBSERVED SIDE-EFFECT SEQUENCE, not from
# reading the source: both `mkdir` and `df` are shimmed to append to one log, and
# the slots-dir mkdir must appear BEFORE the first df.
# ===========================================================================
ab_seq="$tmp/ab-sequence.log"; : > "$ab_seq"
mkdir -p "$tmp/ab-bin"
cat > "$tmp/ab-bin/mkdir" <<'ABMK'
#!/usr/bin/env bash
# Log + delay + FAIL only for the slots dir under test; pass everything else through.
for a in "$@"; do
  case "$a" in
    *"$AB_SLOTS_MARKER"*)
      printf 'mkdir-slots\n' >> "$AB_SEQ"
      sleep 2
      exit 1 ;;
  esac
done
exec /bin/mkdir "$@"
ABMK
cat > "$tmp/ab-bin/df" <<'ABDF'
#!/usr/bin/env bash
printf 'df\n' >> "$AB_SEQ"
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/shim 999999999 1 209715200 1%% /shimfs\n'
ABDF
chmod +x "$tmp/ab-bin/mkdir" "$tmp/ab-bin/df"
ab_slots="$tmp/ab-slots-BLOCKY"
RS_PATH_PREFIX="$tmp/ab-bin"
run_stub_gate ab "$(df_script ab "$HIGH")" \
  AB_SEQ="$ab_seq" AB_SLOTS_MARKER="ab-slots-BLOCKY" \
  CQLITE_GATE_SLOTS_DIR="$ab_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
ab_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 180; ab_status=$WX_STATUS; ab_markers=$WX_MARKERS
assert_no_timeout "ab blocking slot dir"
# The fixture must actually have exercised the blocking-failure path.
if grep -q 'cannot create slot dir' "$ab_err" 2>/dev/null && grep -q '^mkdir-slots$' "$ab_seq"; then
  ok "blocking-setup setup: the slots-dir mkdir really was reached, blocked and failed"
else
  bad "blocking-setup setup: the blocking mkdir path was not exercised — the ordering assertion below would prove nothing"
fi
ab_first=$(sed -n 1p "$ab_seq" 2>/dev/null)
if [ "$ab_first" = mkdir-slots ]; then
  ok "blocking-setup: the BLOCKING mkdir ran BEFORE the first df — the bound reading is taken after it, never before"
else
  bad "blocking-setup: the first side effect was '$ab_first', so the measurement precedes the blocking call and the bound reading can be stale"
fi
ab_line=$(grep_line "$ab_err" '^agent-gate: disk-admission: ')
case "$ab_line" in
  *'evaluated 1x'*'NOT RE-MEASURED'*'slot dir uncreatable'*)
    ok "blocking-setup: reported as ONE binding evaluation naming the cap-off cause" ;;
  *) bad "blocking-setup: unexpected rendering: ${ab_line:-<none>}" ;;
esac
if [ "$ab_status" -eq 0 ] && [ "$ab_markers" -ge 1 ]; then
  ok "blocking-setup: an above-bar reading after the blocking failure still PROCEEDS (the order change does not red correct input)"
else
  bad "blocking-setup: the run was refused despite an above-bar reading (exit $ab_status)"
fi
# And the binding half: below-bar on that same blocking-failure path must REFUSE.
: > "$ab_seq"
RS_PATH_PREFIX="$tmp/ab-bin"
cat > "$tmp/ab-bin/df" <<'ABDF2'
#!/usr/bin/env bash
printf 'df\n' >> "$AB_SEQ"
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/shim 999999999 1 10485760 90%% /shimfs\n'
ABDF2
chmod +x "$tmp/ab-bin/df"
run_stub_gate ab-low "$(df_script ab-low "$LOW")" \
  AB_SEQ="$ab_seq" AB_SLOTS_MARKER="ab-slots-BLOCKY" \
  CQLITE_GATE_SLOTS_DIR="$ab_slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=2
RS_PATH_PREFIX=""
watch_until_exit "$RS_PID" "$RS_RUNDIR" 180; ab_low_status=$WX_STATUS; ab_low_markers=$WX_MARKERS
assert_no_timeout "ab-low"
if [ "$ab_low_status" -ne 0 ] && [ "$ab_low_markers" -eq 0 ]; then
  ok "blocking-setup: below-bar on the blocking-failure path REFUSES — the reading taken after the block is BINDING"
else
  bad "blocking-setup: built below the bar after a blocking cap-setup failure (exit $ab_low_status, markers $ab_low_markers)"
fi

# ===========================================================================
# Case AC (roborev job 395, Medium): ONE error boundary around the whole write
# probe — including close() — and a failed unlink DECLARES rather than refuses.
#
# On NFS and quota-enforced filesystems a write error is NOT reported at write();
# it surfaces at close(). fsync narrows that window but does not shut it. With
# close() errors discarded the probe printed OK after a write that never landed.
#
# This was the THIRD consecutive round in this one mechanism (--locked, the errno
# allowlist, now close/unlink), so it is fixed as a CONSOLIDATION: makedirs, the
# isdir check, open, write, fsync and close sit inside a SINGLE try where any
# OSError is CANNOT-WRITE. Branch count went 11 -> 5, which is the check that it
# was consolidated and not carved again.
# ===========================================================================
ac_cls="$tmp/ac-classifier.py"
sed -n '/^import errno, os, sys$/,/^# END-WRITE-PROBE$/p' "$GATE" > "$ac_cls"
if python3 -c "compile(open('$ac_cls').read(),'c','exec')" 2>/dev/null; then
  ok "one-boundary: extracted the SHIPPED classifier and it compiles"
else
  bad "one-boundary: the extracted classifier does not compile — every case below would test nothing"
fi
# STRUCTURAL: exactly ONE `except OSError` in the classifier. A second one means a call site
# has started deciding fatality for itself again, which is the shape this round consolidated.
ac_boundaries=$(grep -c '^except OSError as e:' "$ac_cls" 2>/dev/null); ac_boundaries="${ac_boundaries%%$'\n'*}"
case "$ac_boundaries" in ''|*[!0-9]*) ac_boundaries=0 ;; esac
if [ "$ac_boundaries" -eq 1 ]; then
  ok "one-boundary: exactly ONE 'except OSError' boundary in the probe (no call site decides fatality for itself)"
else
  bad "one-boundary: found $ac_boundaries 'except OSError' clauses — the probe has been re-carved into per-call handling"
fi
# A RAW BRANCH COUNT STOPPED MEASURING THE INVARIANT (job 398). Job 395 asserted `<= 6`
# branches as a proxy for "consolidated, not carved". Job 398 then had to add CLEANUP
# branches — a refusal must unlink before it exits — and the count rose to 13 while the
# property was untouched. A proxy that reds on a correct change is the guard agents learn to
# waive, so the invariant is now encoded directly: EXACTLY ONE handler decides fatality, and
# EVERY other handler is a SWALLOWING cleanup handler whose body is `pass`.
# ENCODE THE PROPERTY, NOT A PROXY FOR IT — AND NOT BY PARSING INDENTATION EITHER. The
# first attempt at this assert counted "handlers whose next statement is not `pass`", which
# read a COMMENT as a statement and counted a handler that RECORDS A DECLARATION (`stray =
# w`) as one that DECIDES A VERDICT. Two different things. What the consolidation actually
# guarantees is: ONE handler decides fatality, ONE reports an internal fault, and the set of
# things this program can SAY is closed. All three are countable without knowing Python.
ad_count() { grep -cF "$1" "$ac_cls" 2>/dev/null | head -1; }
ac_failcalls=$(grep -cE '^ +fail\(e, w\)$' "$ac_cls" 2>/dev/null); ac_failcalls="${ac_failcalls%%$'\n'*}"
case "$ac_failcalls" in ''|*[!0-9]*) ac_failcalls=0 ;; esac
if [ "$ac_failcalls" -eq 1 ]; then
  ok "one-boundary: exactly ONE call site reaches the fatality decision (fail() is invoked from the single OSError boundary and nowhere else)"
else
  bad "one-boundary: $ac_failcalls call sites invoke fail() — fatality is being decided in more than one place again"
fi
ac_unclass=$(ad_count 'verdict("UNCLASSIFIED'); case "$ac_unclass" in ''|*[!0-9]*) ac_unclass=0 ;; esac
if [ "$ac_unclass" -eq 1 ]; then
  ok "one-boundary: exactly ONE internal-fault arm (the non-OSError case), so a cleanup problem can never report as one"
else
  bad "one-boundary: $ac_unclass UNCLASSIFIED emissions, expected 1"
fi
# THE EMIT BOUNDARY IS NOW TWO FUNCTIONS, WHICH IS A STRONGER INVARIANT THAN THE OLD COUNT
# (roborev job 416). Job 395 pinned "exactly 6 `sys.stdout.write(` sites" as a closed payload
# set; the verdict-first restructure routes EVERY emission through `verdict()` (the one verdict
# boundary) or `leftover()` (the one suffix boundary), so the property is now expressible
# directly: exactly TWO raw write sites exist, and they are those two functions. A cleanup arm
# that starts emitting a verdict of its own would raise the count.
ac_says=$(grep -cE 'sys\.stdout\.write\(' "$ac_cls" 2>/dev/null); ac_says="${ac_says%%$'\n'*}"
case "$ac_says" in ''|*[!0-9]*) ac_says=0 ;; esac
if [ "$ac_says" -eq 2 ]; then
  ok "one-boundary: exactly TWO raw stdout writes remain — the ONE verdict boundary and the ONE leftover-suffix boundary"
else
  bad "one-boundary: $ac_says raw stdout write sites, expected 2 (verdict() and leftover()) — an emission has escaped the two boundaries"
fi
# ...and the verdict TOKENS are a closed set of four, all emitted through verdict().
ac_verdicts=$(grep -cE '^ *verdict\("' "$ac_cls" 2>/dev/null); ac_verdicts="${ac_verdicts%%$'\n'*}"
case "$ac_verdicts" in ''|*[!0-9]*) ac_verdicts=0 ;; esac
if [ "$ac_verdicts" -eq 4 ]; then
  ok "one-boundary: the probe emits a CLOSED SET of 4 verdicts (CANNOT-WRITE <code>, CANNOT-WRITE ENOTDIR, UNCLASSIFIED unknown, OK)"
else
  bad "one-boundary: $ac_verdicts verdict() call sites, expected 4 — the verdict set changed and the shell parser may not know about it"
fi
# THE INVARIANT THIS ROUND EXISTS FOR, ASSERTED ON THE SOURCE: every verdict is FLUSHED, and
# the flush lives inside the one boundary rather than at any call site.
if grep -qE '^ *sys\.stdout\.flush\(\)$' "$ac_cls"; then
  ok "one-boundary: the emit boundary FLUSHES — without it a verdict never reaches a caller that kills the probe mid-cleanup"
else
  bad "one-boundary: no explicit flush in the probe — a hung cleanup would discard the verdict again"
fi

# (a) A FORCED close() FAILURE IS BINDING, with the pre-fix behaviour shown emitting OK.
cat > "$tmp/ac-prelude-close.py" <<'ACSC'
import os, errno
_real = os.close
def boom(fd):
    _real(fd)
    raise OSError(errno.EIO, "simulated deferred write error at close")
os.close = boom
ACSC
ac_got=$(da_run_probe "$tmp/ac-prelude-close.py" "$ac_cls" "$tmp/ac-target")
# THE POSITIVE CONTROL: the PRE-FIX shape, reproduced verbatim — close() inside a `finally`
# with its error swallowed — on the SAME forced failure. It must print OK, which is the
# false admission.
cat > "$tmp/ac-prefix-body.py" <<'ACPRE'
import os, sys
p = sys.argv[1]
os.makedirs(p, exist_ok=True)
w = os.path.join(p, "." + os.urandom(12).hex() + ".probe")
fd = None
try:
    fd = os.open(w, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(fd, b"\0")
    os.fsync(fd)
except OSError:
    sys.stdout.write("CANNOT-WRITE"); sys.exit(0)
finally:
    if fd is not None:
        try: os.close(fd)
        except Exception: pass
    try: os.unlink(w)
    except Exception: pass
sys.stdout.write("OK")
ACPRE
ac_pre=$(da_run_probe "$tmp/ac-prelude-close.py" "$tmp/ac-prefix-body.py" "$tmp/ac-pre-target")
if [ "$ac_pre" = OK ]; then
  ok "one-boundary CONTROL: the PRE-FIX shape prints OK on a forced close() failure — a write that never landed, admitted"
else
  bad "one-boundary CONTROL: the pre-fix shape gave '$ac_pre'; this case does not demonstrate the defect"
fi
case "$ac_got" in
  'CANNOT-WRITE EIO')
    ok "one-boundary: the SHIPPED probe calls a forced close() failure CANNOT-WRITE — close is inside the boundary" ;;
  OK*) bad "one-boundary: the shipped probe still printed '$ac_got' after a failed close() — the false admission survives" ;;
  *) bad "one-boundary: unexpected classification '$ac_got'" ;;
esac

# (b) A FORCED unlink() FAILURE STILL ADMITS, AND NAMES THE ARTIFACT. Deliberate: everything
#     before it succeeded, so the filesystem IS writable, which is the only question the
#     probe answers. Refusing would red correct input; the leftover is DECLARED instead.
cat > "$tmp/ac-prelude-unlink.py" <<'ACSU'
import os, errno
def boom(*a, **k):
    raise OSError(errno.EPERM, "simulated immutable directory")
os.unlink = boom
ACSU
ac_ul=$(da_run_probe "$tmp/ac-prelude-unlink.py" "$ac_cls" "$tmp/ac-ul-target")
case "$ac_ul" in
  'OK LEFTOVER '*'.agent-gate-writeprobe')
    ok "one-boundary: a failed unlink AFTER a successful write still answers OK and reports the artifact path" ;;
  'CANNOT-WRITE'*)
    bad "one-boundary: a failed unlink REFUSED the run — the filesystem was proven writable, so this reds correct input: $ac_ul" ;;
  *) bad "one-boundary: unexpected unlink classification '$ac_ul'" ;;
esac
# END TO END: the run admits and the emitted line NAMES the leftover, so it cannot be
# absorbed into the certification baseline unseen.
ac_script=$(df_script ac "$HIGH")
da_py_shim "$tmp/ac-bin" "$tmp/ac-prelude-unlink.py"
RS_PATH_PREFIX="$tmp/ac-bin"
run_stub_gate ac "$ac_script" \
  CARGO_TARGET_DIR="$tmp/ac-e2e-target" \
  CQLITE_GATE_SLOTS_DIR="$tmp/ac-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
# RESET IMMEDIATELY. A leaked prefix silently applies this plant to every LATER case, which
# is the same "silently active / silently inert" failure the argv-count shims had.
RS_PATH_PREFIX=""
ac_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 180; ac_status=$WX_STATUS; ac_markers=$WX_MARKERS
assert_no_timeout "ac unlink end-to-end"
ac_line=$(grep_line "$ac_err" '^agent-gate: disk-admission: ')
if [ "$ac_status" -eq 0 ] && [ "$ac_markers" -ge 1 ]; then
  ok "one-boundary: a leftover artifact does NOT refuse the run (exit 0, work began)"
else
  bad "one-boundary: a leftover artifact refused the run (exit $ac_status, markers $ac_markers)"
fi
case "$ac_line" in
  *'write-probe artifacts LEFT BEHIND ('*"$tmp/ac-e2e-target/."*)
    ok "one-boundary: the emitted line DECLARES the leftovers and names their exact paths" ;;
  *'write-probe artifacts LEFT BEHIND'*)
    bad "one-boundary: the leftovers are declared but no path is named: $ac_line" ;;
  *) bad "one-boundary: the leftover was not declared in the line: ${ac_line:-<none>}" ;;
esac
# ...and a clean run must NOT carry that declaration, or it would be noise rather than a signal.
if grep -q 'write-probe artifacts LEFT BEHIND' "$n_pass_err" 2>/dev/null; then
  bad "one-boundary: a CLEAN run also declares a leftover — the declaration is noise, not a signal"
else
  ok "one-boundary: a clean run carries no leftover declaration (it fires only when there is one)"
fi

# ===========================================================================
# Case AD (roborev job 398): three fixes in the emit path.
#   1  a refusal cleans up before exiting, and never lets cleanup soften the verdict
#   2  `td` survives every POST-resolution failure, so the block stops saying
#      UNRESOLVED about a resolved directory and the target-dir pin stays reachable
#   3  leftovers ACCUMULATE across both evaluations, so a two-probe run declares BOTH
# ===========================================================================
ad_cls="$tmp/ad-classifier.py"
sed -n '/^import errno, os, sys$/,/^# END-WRITE-PROBE$/p' "$GATE" > "$ad_cls"
if python3 -c "compile(open('$ad_cls').read(),'c','exec')" 2>/dev/null; then
  ok "emit-path: extracted the SHIPPED classifier and it compiles"
else
  bad "emit-path: the extracted classifier does not compile — every case below would test nothing"
fi

# --- 1  A REFUSAL MUST NOT LITTER, AND MUST STILL REFUSE ---------------------------
cat > "$tmp/ad-prelude-close.py" <<'ADSC'
import os, errno
_real = os.close
def boom(fd):
    _real(fd)
    raise OSError(errno.EIO, "simulated deferred write error at close")
os.close = boom
ADSC
ad_t1="$tmp/ad-t1"
ad_v1=$(da_run_probe "$tmp/ad-prelude-close.py" "$ad_cls" "$ad_t1")
ad_stray1=$(ls -A "$ad_t1" 2>/dev/null | grep -c 'agent-gate-writeprobe' || true)
ad_stray1="${ad_stray1%%$'\n'*}"; case "$ad_stray1" in ''|*[!0-9]*) ad_stray1=0 ;; esac
if [ "$ad_v1" = "CANNOT-WRITE EIO" ]; then
  ok "refusal-cleanup: the VERDICT is preserved on a post-create failure (CANNOT-WRITE EIO)"
else
  bad "refusal-cleanup: the verdict changed to '$ad_v1' — cleanup must never soften a refusal"
fi
if [ "$ad_stray1" -eq 0 ]; then
  ok "refusal-cleanup: the probe file is REMOVED before the refusal exits (0 strays left)"
else
  bad "refusal-cleanup: the refusal left $ad_stray1 artifact(s) behind — a disk guard adding to the exhaustion it measures"
fi
# THE POSITIVE CONTROL: the PRE-FIX shape, reproduced verbatim — exit straight from the
# error handler with no cleanup — on the SAME forced failure. It must LITTER.
ad_t1p="$tmp/ad-t1-prefix"
cat > "$tmp/ad-prefix-body.py" <<'ADPRE'
import errno, os, sys
p = sys.argv[1]
def fail(e):
    sys.stdout.write("CANNOT-WRITE")
    sys.exit(0)
os.makedirs(p, exist_ok=True)
w = os.path.join(p, "." + os.urandom(12).hex() + ".agent-gate-writeprobe")
try:
    fd = os.open(w, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(fd, b"\0")
    os.fsync(fd)
    os.close(fd)
except OSError as e:
    fail(e)
ADPRE
da_run_probe "$tmp/ad-prelude-close.py" "$tmp/ad-prefix-body.py" "$ad_t1p" >/dev/null 2>&1
ad_strayp=$(ls -A "$ad_t1p" 2>/dev/null | grep -c 'agent-gate-writeprobe' || true)
ad_strayp="${ad_strayp%%$'\n'*}"; case "$ad_strayp" in ''|*[!0-9]*) ad_strayp=0 ;; esac
if [ "$ad_strayp" -ge 1 ]; then
  ok "refusal-cleanup CONTROL: the PRE-FIX shape left $ad_strayp artifact(s) on the same failure — the defect was reachable"
else
  bad "refusal-cleanup CONTROL: the pre-fix shape littered nothing; this case does not demonstrate the defect"
fi
# ...and when cleanup ITSELF fails, the verdict is still the refusal, with the stray named.
cat > "$tmp/ad-prelude-both.py" <<'ADSB'
import os, errno
_real = os.close
def bc(fd):
    _real(fd)
    raise OSError(errno.EIO, "simulated")
def bu(*a, **k):
    raise OSError(errno.EPERM, "simulated")
os.close = bc
os.unlink = bu
ADSB
ad_v2=$(da_run_probe "$tmp/ad-prelude-both.py" "$ad_cls" "$tmp/ad-t2")
case "$ad_v2" in
  'CANNOT-WRITE EIO LEFTOVER '*'.agent-gate-writeprobe')
    ok "refusal-cleanup: cleanup failing too keeps the REFUSAL and names the stray, as a SUFFIX to a complete verdict (symmetrical with OK LEFTOVER)" ;;
  'OK'*) bad "refusal-cleanup: a cleanup problem SOFTENED the refusal to '$ad_v2' — the exact route this must not open" ;;
  *) bad "refusal-cleanup: unexpected payload '$ad_v2'" ;;
esac

# --- 2  `td` SURVIVES A POST-RESOLUTION FAILURE, AND THE PIN STAYS REACHABLE --------
ad_x() { sed -n "/^$1() {/,/^}$/p" "$GATE"; }
# CONTROL: an EMPTY target dir renders the false statement, which is what the dropped field
# produced. Driven through the SHIPPED line renderer.
ad_pre_line=$(
  _DA_EVALUATIONS=1; _DA_LAUNCH_RENDER="x"; _DA_POST_RENDER="y"; _DA_BAR=40; _DA_BAR_SRC=default
  _DA_MOUNT="/m"; _DA_TARGET_DIR=""; _DA_TARGET_NOTE=""; _DA_LEFTOVER_ALL=""; DISK_ADMISSION_LINE=""
  eval "$(ad_x _gate_disk_admission_line)"
  _gate_disk_admission_line "UNMEASURED (target-dir-mkdir-timeout)" "d" >/dev/null 2>&1
  printf '%s' "$DISK_ADMISSION_LINE"
)
case "$ad_pre_line" in
  *'target-dir UNRESOLVED'*)
    ok "td-survives CONTROL: an EMPTY target dir renders 'target-dir UNRESOLVED' — the false statement the dropped field produced" ;;
  *) bad "td-survives CONTROL: an empty target dir did not render UNRESOLVED; the differential below proves less than it claims" ;;
esac
# SHIPPED, end to end: a post-resolution measurement failure (the mkdir classifier bound
# fires) must still name the RESOLVED directory. Driven with a python3 shim that hangs only
# on the 3-argv classifier call, so the cargo resolution itself still succeeds.
da_py_hang_shim "$tmp/ad-hangpy"
ad_tgt="$tmp/ad-resolved-target"
RS_PATH_PREFIX="$tmp/ad-hangpy"
run_stub_gate ad "$(df_script ad "$HIGH")" \
  CARGO_TARGET_DIR="$ad_tgt" \
  CQLITE_GATE_SLOTS_DIR="$tmp/ad-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
ad_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; ad_status=$WX_STATUS; ad_markers=$WX_MARKERS
assert_no_timeout "ad post-resolution failure"
ad_line=$(grep_line "$ad_err" '^agent-gate: disk-admission: ')
case "$ad_line" in
  *'UNMEASURED (target-dir-mkdir-timeout)'*"target-dir $ad_tgt "*)
    ok "td-survives: a post-resolution measurement failure still names the RESOLVED directory" ;;
  *'target-dir UNRESOLVED'*)
    bad "td-survives: the block says UNRESOLVED about a directory cargo DID resolve: $ad_line" ;;
  *) bad "td-survives: unexpected rendering: ${ad_line:-<none>}" ;;
esac
if [ "$ad_status" -eq 0 ] && [ "$ad_markers" -ge 1 ]; then
  ok "td-survives: the run still proceeds as declared UNMEASURED (the fix is reporting + pinning, not a new refusal)"
else
  bad "td-survives: the run was refused (exit $ad_status) — a measurement failure must stay non-fatal"
fi
# THE PIN. Round 367's guarantee is forfeited when _DA_TARGET_DIR is empty, because the
# pinner skips. Asserted against the REAL disposer + pinner, with an UNMEASURED state.
ad_pin=$(
  set -uo pipefail
  _DA_TARGET_DIR="/resolved/dir"; _DA_STATE=UNMEASURED; _DA_WHY=target-dir-mkdir-timeout
  CARGO_TARGET_DIR="SENTINEL"; DISK_ADMISSION_LINE=""
  _gate_disk_admission_line() { DISK_ADMISSION_LINE="line"; }
  _gate_disk_admission_refuse() { printf 'REFUSED'; exit 0; }
  eval "$(ad_x _gate_disk_admission_pin_target_dir)"
  eval "$(ad_x _gate_disk_admission_dispose)"
  _gate_disk_admission_dispose "n" "d" >/dev/null 2>&1
  printf '%s' "$CARGO_TARGET_DIR"
)
if [ "$ad_pin" = /resolved/dir ]; then
  ok "td-survives: an UNMEASURED result with a RESOLVED dir still PINS CARGO_TARGET_DIR (measured-fs-is-used-fs holds on this path)"
else
  bad "td-survives: the pin was skipped on an UNMEASURED result (CARGO_TARGET_DIR='$ad_pin')"
fi

# --- 3  LEFTOVERS ACCUMULATE ACROSS BOTH EVALUATIONS -------------------------------
cat > "$tmp/ad-prelude-unlink.py" <<'ADSU'
import os, errno
def boom(*a, **k):
    raise OSError(errno.EPERM, "simulated immutable directory")
os.unlink = boom
ADSU
da_py_shim "$tmp/ad-two-bin" "$tmp/ad-prelude-unlink.py"
RS_PATH_PREFIX="$tmp/ad-two-bin"
run_stub_gate ad-two "$(df_script ad-two "$HIGH")" \
  CARGO_TARGET_DIR="$tmp/ad-two-target" \
  CQLITE_GATE_SLOTS_DIR="$tmp/ad-two-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
ad2_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 180; ad2_status=$WX_STATUS
assert_no_timeout "ad-two accumulation"
ad2_line=$(grep_line "$ad2_err" '^agent-gate: disk-admission: ')
# The run probes TWICE, so both artifacts must be declared and the count must say 2.
case "$ad2_line" in
  *'evaluated 2x'*'write-probe artifacts LEFT BEHIND (2;'*)
    ok "leftover-accumulates: a two-evaluation run declares BOTH artifacts and counts them" ;;
  *'write-probe artifacts LEFT BEHIND (1;'*)
    bad "leftover-accumulates: only ONE artifact declared on a two-probe run — the launch stray was absorbed silently: $ad2_line" ;;
  *) bad "leftover-accumulates: unexpected rendering: ${ad2_line:-<none>}" ;;
esac
# Both paths must actually appear, not just a count.
ad2_paths=$(printf '%s' "$ad2_line" | grep -o "$tmp/ad-two-target/\.[0-9a-f]*\.agent-gate-writeprobe" | sort -u | grep -c . || true)
ad2_paths="${ad2_paths%%$'\n'*}"; case "$ad2_paths" in ''|*[!0-9]*) ad2_paths=0 ;; esac
if [ "$ad2_paths" -eq 2 ]; then
  ok "leftover-accumulates: TWO DISTINCT paths are named (each probe makes its own random name)"
else
  bad "leftover-accumulates: $ad2_paths distinct path(s) named, expected 2"
fi
if [ "$ad2_status" -eq 0 ]; then
  ok "leftover-accumulates: still non-fatal — a declaration, not a verdict"
else
  bad "leftover-accumulates: leftovers refused the run (exit $ad2_status)"
fi

# ===========================================================================
# Case AE (roborev job 416, F1): ONCE THE VERDICT IS DECIDED, NOTHING THAT RUNS
# LATER MAY WEAKEN IT — not an exception, not a hang, not a bound expiring.
#
# THE DEFECT, and it was INTRODUCED BY ROUND 19's OWN FIX. Job 398 moved cleanup
# AHEAD of the exit so a refusal would not litter, and left the stdout write AFTER
# the unlink. python3's stdout is BLOCK-BUFFERED onto a file (which is what
# `_component_set_bounded` captures into), so nothing reaches the capture until the
# process exits. If that unlink HANGS — a dead NFS mount, a stale handle — the outer
# bound fires, the probe produces NO OUTPUT AT ALL, and the shell reads rc=124 as
# "cannot tell" -> non-fatal UNMEASURED -> THE GATE PROCEEDS ON A FILESYSTEM THAT
# DEFINITIVELY COULD NOT BE WRITTEN. A false admission, which is the exact class this
# whole change exists to remove.
#
# THE FIX HAS TWO HALVES AND BOTH ARE ASSERTED HERE:
#   (1) python: the verdict is written AND FLUSHED before any cleanup runs, so the
#       grammar is `<verdict>[ LEFTOVER <path>]` — a COMPLETE PREFIX plus an optional
#       SUFFIX, never two mutually exclusive leading tokens.
#   (2) shell: a timed-out bounded call whose captured stdout ALREADY carries a
#       definitive REFUSAL is honoured as that refusal — and a PERMISSIVE `OK`
#       recovered the same way is STILL DISCARDED to UNMEASURED.
#
# MEASURED PREMISE for (2), because it is not obvious and the design rests on it:
# `_component_set_bounded` captures the child's stdout into a REGULAR FILE and replays
# it after the child completes, so partial stdout DOES survive a kill —
#   flushed-then-hang, timeout arm: rc=124 out=[CANNOT-WRITE EIO]  (16 bytes)
#   flushed-then-hang, bash arm:    rc=124 out=[CANNOT-WRITE EIO]  (16 bytes)
#   UNflushed-then-hang:            rc=124 out=[]                  (0 bytes)
# The third reading is why half (1) is a PREREQUISITE for half (2) rather than a tidy-up
# beside it: without the explicit flush there is nothing to honour.
# ===========================================================================


# The shipped write-probe body, extracted between its two anchors.
ae_cls="$tmp/ae-classifier.py"
sed -n '/^import errno, os, sys$/,/^# END-WRITE-PROBE$/p' "$GATE" > "$ae_cls"
if [ -s "$ae_cls" ] && grep -q 'agent-gate-writeprobe' "$ae_cls"; then
  ok "verdict-first: extracted the SHIPPED write probe between its declared anchors"
else
  bad "verdict-first: could not extract the write probe (anchors changed?) — every case below would test nothing"
fi

# The hostile prelude: close() reports a deferred write error (the definitive refusal),
# and unlink() HANGS FOREVER (the cleanup that must not be able to discard it).
cat > "$tmp/ae-prelude-refuse.py" <<'AEPR'
import os, errno, time
_c = os.close
def bc(fd):
    _c(fd)
    raise OSError(errno.EIO, "simulated deferred write error at close")
def bu(*a, **k):
    time.sleep(600)
os.close = bc
os.unlink = bu
AEPR
# ...and the permissive counterpart: nothing fails, only the cleanup hangs.
cat > "$tmp/ae-prelude-ok.py" <<'AEPO'
import os, time
def bu(*a, **k):
    time.sleep(600)
os.unlink = bu
AEPO

# (a) HALF ONE, DIRECTLY: with the cleanup hung, does the DEFINITIVE VERDICT reach the
#     caller at all? Run under a real external bound, exactly as the gate does.
# A HOST WITHOUT `timeout` CANNOT RUN THIS PAIR, AND THAT IS REPORTED, NEVER PASSED. The
# subject is what survives a KILL, so the case needs a real external bound; a green derived
# from a control that could not run is the shape this file's own `skip()` exists to forbid.
# (The end-to-end halves (b) and (c) below need no such tool — they use the gate's own
# bounded runner, which has a pure-bash arm.)
DA_HAVE_TIMEOUT=0
command -v timeout >/dev/null 2>&1 && DA_HAVE_TIMEOUT=1
if [ "$DA_HAVE_TIMEOUT" -eq 0 ]; then
  skip "verdict-first: no timeout(1) on this host — the flushed-vs-unflushed differential and its pre-fix control cannot be driven"
  ae_rc=0; ae_got="SKIPPED"; ae_prerc=0; ae_pre="SKIPPED"
else
# Run DIRECTLY, capturing into a file rather than through `$( )`: a substitution runs in
# a SUBSHELL, so an rc recorded into a variable there is discarded — the same trap this
# file records for `watch_until_exit`, and it cost a round here too.
timeout --kill-after=1 5 "$DA_REAL_PY" -I -S -c "$(cat "$tmp/ae-prelude-refuse.py")
$(cat "$ae_cls")" "$tmp/ae-t-refuse" >"$tmp/ae-refuse.payload" 2>/dev/null
ae_rc=$?
ae_got=$(cat "$tmp/ae-refuse.payload" 2>/dev/null)
if [ "$ae_rc" -eq 124 ] || [ "$ae_rc" -eq 137 ]; then
  ok "verdict-first: the hung cleanup really does hit the bound (rc=$ae_rc), so this case exercises the defect's condition"
else
  bad "verdict-first: the planted hang did not reach the bound (rc=$ae_rc) — the case models nothing"
fi
case "$ae_got" in
  'CANNOT-WRITE EIO'*)
    ok "verdict-first: the definitive refusal SURVIVES a hung cleanup ('$ae_got') — the verdict is written and flushed before cleanup runs" ;;
  '')
    bad "verdict-first: rc=$ae_rc with NO OUTPUT AT ALL — the definitive CANNOT-WRITE was discarded by the hung cleanup, so the shell reads 'cannot tell' and ADMITS" ;;
  *) bad "verdict-first: unexpected survivor '$ae_got'" ;;
esac
# THE POSITIVE CONTROL: the PRE-FIX ORDERING, reproduced verbatim — cleanup first, write
# afterwards — on the SAME planted hang. It must produce NOTHING, or the case above
# proves less than it claims.
cat > "$tmp/ae-prefix-body.py" <<'AEPB'
import errno, os, sys
p = sys.argv[1]
os.makedirs(p, exist_ok=True)
w = os.path.join(p, "." + os.urandom(12).hex() + ".agent-gate-writeprobe")
fd = os.open(w, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
try:
    os.write(fd, b"\0")
    os.fsync(fd)
    os.close(fd)
except OSError as e:
    code = errno.errorcode.get(e.errno, "unknown")
    try:
        os.unlink(w)          # round 19's ordering: cleanup BEFORE the write
    except Exception:
        pass
    sys.stdout.write("CANNOT-WRITE " + code)
    sys.exit(0)
AEPB
ae_pre=$(timeout --kill-after=1 5 "$DA_REAL_PY" -I -S -c "$(cat "$tmp/ae-prelude-refuse.py")
$(cat "$tmp/ae-prefix-body.py")" "$tmp/ae-t-prefix" 2>/dev/null); ae_prerc=$?
if [ -z "$ae_pre" ] && { [ "$ae_prerc" -eq 124 ] || [ "$ae_prerc" -eq 137 ]; }; then
  ok "verdict-first CONTROL: the PRE-FIX ordering yields rc=$ae_prerc and an EMPTY payload on the same hang — the defect was reachable"
else
  bad "verdict-first CONTROL: the pre-fix ordering gave rc=$ae_prerc payload '$ae_pre'; this case does not demonstrate the defect"
fi
fi

# (b) HALF TWO, END TO END: the recovered refusal is HONOURED. A REAL gate run whose
#     write probe times out with `CANNOT-WRITE` already in its capture must REFUSE.
da_py_shim "$tmp/ae-bin-refuse" "$tmp/ae-prelude-refuse.py"
RS_PATH_PREFIX="$tmp/ae-bin-refuse"
run_stub_gate ae-refuse "$(df_script ae-refuse "$HIGH")" \
  CARGO_TARGET_DIR="$tmp/ae-e2e-refuse" \
  CQLITE_GATE_SLOTS_DIR="$tmp/ae-refuse-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
ae_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; ae_status=$WX_STATUS; ae_markers=$WX_MARKERS
assert_no_timeout "ae refusal recovered from a timed-out probe"
ae_line=$(grep_line "$ae_err" '^agent-gate: disk-admission: ')
if [ "$ae_status" -ne 0 ] && [ "$ae_markers" -eq 0 ]; then
  ok "verdict-first: a refusal recovered from a TIMED-OUT probe REFUSES the run (exit $ae_status) and no work ever began"
else
  bad "verdict-first: the run PROCEEDED (exit $ae_status, markers $ae_markers) although its own write probe had definitively refused — a false admission"
fi
case "$ae_line" in
  *'UNWRITABLE-FAIL-CLOSED (#3755)'*)
    ok "verdict-first: ...under the UNWRITABLE verdict token, not a generic timeout" ;;
  *'UNMEASURED (target-dir-mkdir-timeout)'*)
    bad "verdict-first: the recovered CANNOT-WRITE was downgraded to UNMEASURED: $ae_line" ;;
  *) bad "verdict-first: unexpected rendering: ${ae_line:-<none>}" ;;
esac

# (c) THE ASYMMETRY, WHICH IS THE POINT: an `OK` recovered from a timed-out probe is
#     STILL DISCARDED. A refusal recovered from a partial write is fail-closed and safe;
#     an ADMISSION recovered the same way would be deriving a pass from a process we
#     killed. So this run must proceed as UNMEASURED — never as PASS.
da_py_shim "$tmp/ae-bin-ok" "$tmp/ae-prelude-ok.py"
RS_PATH_PREFIX="$tmp/ae-bin-ok"
run_stub_gate ae-ok "$(df_script ae-ok "$HIGH")" \
  CARGO_TARGET_DIR="$tmp/ae-e2e-ok" \
  CQLITE_GATE_SLOTS_DIR="$tmp/ae-ok-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
ae2_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; ae2_status=$WX_STATUS; ae2_markers=$WX_MARKERS
assert_no_timeout "ae permissive token recovered from a timed-out probe"
ae2_line=$(grep_line "$ae2_err" '^agent-gate: disk-admission: ')
case "$ae2_line" in
  *'disk-admission: PASS'*)
    bad "verdict-first: an OK recovered from a KILLED probe was honoured as an admission — a pass derived from a process we terminated: $ae2_line" ;;
  *'UNMEASURED (target-dir-mkdir-timeout)'*)
    ok "verdict-first: a PERMISSIVE token recovered from a timed-out probe is DISCARDED to UNMEASURED (the bound may only keep or strengthen a refusal, never soften one)" ;;
  *) bad "verdict-first: unexpected rendering for the permissive-recovery case: ${ae2_line:-<none>}" ;;
esac
if [ "$ae2_status" -eq 0 ] && [ "$ae2_markers" -ge 1 ]; then
  ok "verdict-first: ...and UNMEASURED stays NON-FATAL, so the discard does not red a run whose filesystem was never shown to be bad"
else
  bad "verdict-first: the discard turned into a refusal (exit $ae2_status, markers $ae2_markers) — UNMEASURED must stay non-fatal"
fi


# --- (d) THE SAME DISCARD ON A NON-TIMEOUT EXIT — SWEEP THE CLASS, NOT THE INSTANCE ----
#
# Job 416 named rc=124, and the identical discard sat on every OTHER non-zero arm. After
# the flush the probe still runs cleanup, and a BaseException escaping there — SIGINT
# during the unlink, an interpreter-level fault — exits NON-ZERO with the verdict ALREADY
# IN THE CAPTURE, which the `*) classifier-failed` arm discarded exactly as the timeout arm
# did. A per-status fix leaves that route open, so the honouring is stated over the PROPERTY
# (a complete refusal is present) and not over the STATUS.
cat > "$tmp/ae-prelude-raise.py" <<'AEPX'
import os, errno
_c = os.close
def bc(fd):
    _c(fd)
    raise OSError(errno.EIO, "simulated deferred write error at close")
def bu(*a, **k):
    # A BaseException, so the probe's `except Exception` cleanup guards do NOT catch it and
    # the interpreter exits non-zero — with the verdict already flushed.
    raise KeyboardInterrupt("simulated signal during cleanup")
os.close = bc
os.unlink = bu
AEPX
# First: confirm the condition is real — the body exits NON-ZERO, NOT 124, and the verdict
# is nonetheless in the payload. Without this the case could pass on a timeout instead.
da_run_probe "$tmp/ae-prelude-raise.py" "$ae_cls" "$tmp/ae-t-raise" >"$tmp/ae-raise.payload" 2>/dev/null
ae3_rc=$?
ae3_got=$(cat "$tmp/ae-raise.payload" 2>/dev/null)
if [ "$ae3_rc" -ne 0 ] && [ "$ae3_rc" -ne 124 ] && [ "$ae3_rc" -ne 137 ] && [ "$ae3_got" = "CANNOT-WRITE EIO" ]; then
  ok "verdict-first: a non-timeout failure (rc=$ae3_rc) still carries the flushed refusal — the discard class is wider than the bound"
else
  bad "verdict-first: the non-timeout plant gave rc=$ae3_rc payload '$ae3_got' — this case does not model the wider discard"
fi
# END TO END: the gate must honour it, exactly as it honours the timeout case.
da_py_shim "$tmp/ae-bin-raise" "$tmp/ae-prelude-raise.py"
RS_PATH_PREFIX="$tmp/ae-bin-raise"
run_stub_gate ae-raise "$(df_script ae-raise "$HIGH")" \
  CARGO_TARGET_DIR="$tmp/ae-e2e-raise" \
  CQLITE_GATE_SLOTS_DIR="$tmp/ae-raise-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_PATH_PREFIX=""
ae3_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; ae3_status=$WX_STATUS; ae3_markers=$WX_MARKERS
assert_no_timeout "ae refusal recovered from a non-timeout failure"
ae3_line=$(grep_line "$ae3_err" '^agent-gate: disk-admission: ')
if [ "$ae3_status" -ne 0 ] && [ "$ae3_markers" -eq 0 ]; then
  ok "verdict-first: a refusal recovered from a NON-TIMEOUT failure also REFUSES (exit $ae3_status) and no work ever began"
else
  bad "verdict-first: the run PROCEEDED (exit $ae3_status, markers $ae3_markers) on a non-timeout failure whose capture held a definitive refusal — the discard survives one arm over"
fi
case "$ae3_line" in
  *'UNWRITABLE-FAIL-CLOSED (#3755)'*)
    ok "verdict-first: ...under the UNWRITABLE token, not 'target-dir-mkdir-classifier-failed'" ;;
  *'target-dir-mkdir-classifier-failed'*)
    bad "verdict-first: the recovered CANNOT-WRITE was downgraded on the classifier-failed arm: $ae3_line" ;;
  *) bad "verdict-first: unexpected rendering for the non-timeout recovery case: ${ae3_line:-<none>}" ;;
esac
# AND THE EXCLUSIONS HOLD. `$_CS_REPLAY_RC` (our own read failed => bytes known-unusable)
# and `$_CS_UNBOUNDABLE_RC` (the command was NOT RUN => empty capture) must NOT be
# honoured. Asserted on the SHIPPED source, because neither status is drivable from here
# without a seam in the gate — and a seam is the thing doctrine forbids.
ae_guard=$(sed -n '/^_gate_disk_admission_subject() {/,/^}$/p' "$GATE" \
  | grep -c 'e" -ne "\$_CS_REPLAY_RC" \] && \[ "\$e" -ne "\$_CS_UNBOUNDABLE_RC"')
ae_guard="${ae_guard%%$'\n'*}"; case "$ae_guard" in ''|*[!0-9]*) ae_guard=0 ;; esac
if [ "$ae_guard" -eq 1 ]; then
  ok "verdict-first: the honouring EXCLUDES rc 198 (unusable bytes) and rc 199 (command not run) — recovery is from a measurement CUT SHORT, never from one we could not read"
else
  bad "verdict-first: the honouring no longer excludes the replay/unboundable statuses — a verdict could be recovered from bytes we could not read"
fi

# ===========================================================================
# Case AF (roborev job 416, F2): BOTH python probes run ISOLATED, so environment
# state cannot monkeypatch the os operations the verdict is computed from.
#
# THE DEFECT. Neither probe passed any isolation flag, so both inherited PYTHONPATH
# and auto-loaded `sitecustomize`. THIS SUITE WAS THE PROOF OF EXPLOITABILITY — it is
# exactly how cases AA/AC/AD planted their EIO/ESTALE/close failures. IN MODEL because
# PYTHONPATH can be set SYSTEM-WIDE in `/etc/environment`, which the fleet bootstrap
# writes, so the setter need not be the invoker: a non-invoker route is a defect
# (CLAUDE.md's triage rule), not an invoker-class hazard to be recorded and left.
#
# WHICH FLAG BUYS WHICH PROPERTY — MEASURED on this host (python 3.12.3), because
# NEITHER ALONE IS SUFFICIENT and a comment asserting that without measuring it is a
# guess:
#
#   `-S`  stops `site` running, which is what imports `sitecustomize`. With `-I` ALONE,
#         `site` still runs and the SYSTEM site dirs stay on sys.path (measured:
#         `site imported: True`, with /usr/local/lib/python3.12/dist-packages and
#         /usr/lib/python3/dist-packages present), so a `sitecustomize.py` dropped into
#         a system dir still executes and can patch `os` outright.
#   `-I`  drops PYTHONPATH (via the `-E` it implies) and the user site dir (via `-s`).
#         With `-S` ALONE, PYTHONPATH IS STILL ON sys.path (measured: True), so a
#         planted `json.py` SHADOWS the stdlib — measured end to end, the metadata parse
#         returned an attacker-supplied `target_directory`. (`os` is a FROZEN module in
#         3.12 and cannot be shadowed that way; `json`, which the first probe imports,
#         is ordinary python and can.)
#
# The two flags close two different routes and the PAIR is the fix. Both bodies need
# only stdlib (os, errno, sys, json), verified importable under `-I -S`.
# ===========================================================================

# ---- (a) STRUCTURAL: every python3 this path dispatches is isolated ---------------
# Counted from the SHIPPED source, so a THIRD probe added later without the flags is
# caught by the census rather than by someone noticing.
af_py_calls=$(grep -cE '_component_set_bounded "\$_GATE_[A-Z_]+_BOUND_SECS" python3 ' "$GATE" 2>/dev/null)
af_py_calls="${af_py_calls%%$'\n'*}"; case "$af_py_calls" in ''|*[!0-9]*) af_py_calls=0 ;; esac
af_py_iso=$(grep -cE '_component_set_bounded "\$_GATE_[A-Z_]+_BOUND_SECS" python3 -I -S -c ' "$GATE" 2>/dev/null)
af_py_iso="${af_py_iso%%$'\n'*}"; case "$af_py_iso" in ''|*[!0-9]*) af_py_iso=0 ;; esac
if [ "$af_py_calls" -eq 2 ]; then
  ok "isolation: the disk-admission path dispatches exactly 2 bounded python3 probes (the metadata parse and the write probe)"
else
  bad "isolation: found $af_py_calls bounded python3 probes, expected 2 — one may have arrived unisolated, or this census no longer sees them"
fi
if [ "$af_py_iso" -gt 0 ] && [ "$af_py_iso" -eq "$af_py_calls" ]; then
  ok "isolation: ALL $af_py_iso of them run 'python3 -I -S' (each flag closes a different route; see the header)"
else
  bad "isolation: only $af_py_iso of $af_py_calls bounded python3 probes are isolated — an inherited PYTHONPATH or a sitecustomize can monkeypatch the os operations the verdict is computed from"
fi

# ---- THE POSITIVE CONTROL VEHICLE ------------------------------------------------
# Without it the behavioural assertions below mean NOTHING: a bare "the verdict is
# unchanged" is indistinguishable from a test that never reached the code. So the SAME
# plant is applied to a SCRATCH COPY of the gate with the isolation flags STRIPPED, and
# that copy MUST FLIP.
# A SYMLINK FARM (see da_farm for why a lone copy of the script cannot serve as a
# control here), substituting ONLY the stripped `agent-gate.sh` — so the scratch gate
# differs from the shipped one in EXACTLY the property under test.
af_repo="$tmp/af-unisolated-repo"
af_stripped="$af_repo/scripts/agent-gate.sh"
da_farm "$af_repo"
sed 's/python3 -I -S -c /python3 -c /g' "$GATE" > "$af_stripped"
af_strip_n=$(grep -cE 'python3 -I -S -c ' "$af_stripped" 2>/dev/null)
af_strip_n="${af_strip_n%%$'\n'*}"; case "$af_strip_n" in ''|*[!0-9]*) af_strip_n=0 ;; esac
if [ -s "$af_stripped" ] && [ "$af_strip_n" -eq 0 ] && bash -n "$af_stripped" 2>/dev/null \
   && [ -f "$af_repo/scripts/lib/gate_slot_daemon.py" ] && [ -e "$af_repo/Cargo.lock" ]; then
  ok "isolation CONTROL: built a scratch REPO whose gate has the isolation flags stripped (0 flagged sites left), with a live slot daemon and workspace"
else
  bad "isolation CONTROL: could not build the unisolated scratch repo ($af_strip_n flagged sites left) — the differentials below would prove nothing"
fi

# ---- (b) THE WRITE PROBE: a plant that WOULD refuse the run ----------------------
mkdir -p "$tmp/af-plant-write"
cat > "$tmp/af-plant-write/sitecustomize.py" <<'AFPW'
import os, errno
def boom(*a, **k):
    raise OSError(errno.EIO, "planted: the write probe cannot create the target dir")
os.makedirs = boom
AFPW
# CONTROL FIRST — the unisolated copy must HONOUR the plant and refuse.
RS_GATE="$af_stripped"
run_stub_gate af-w-ctl "$(df_script af-w-ctl "$HIGH")" \
  PYTHONPATH="$tmp/af-plant-write" CARGO_TARGET_DIR="$tmp/af-w-ctl-target" \
  CQLITE_GATE_SLOTS_DIR="$tmp/af-w-ctl-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_GATE=""
af_wc_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; af_wc_status=$WX_STATUS; af_wc_markers=$WX_MARKERS
assert_no_timeout "af write-probe control (unisolated)"
af_wc_line=$(grep_line "$af_wc_err" '^agent-gate: disk-admission: ')
case "$af_wc_line" in
  *'UNWRITABLE-FAIL-CLOSED (#3755)'*)
    ok "isolation CONTROL: the UNISOLATED gate HONOURS a planted sitecustomize and refuses (exit $af_wc_status) — the plant really does flip the verdict" ;;
  *) bad "isolation CONTROL: the unisolated gate did not flip on the plant (exit $af_wc_status): ${af_wc_line:-<none>} — the shipped-gate assertion below proves nothing" ;;
esac

# THE SHIPPED GATE, SAME PLANT, MUST BE UNAFFECTED.
run_stub_gate af-w "$(df_script af-w "$HIGH")" \
  PYTHONPATH="$tmp/af-plant-write" CARGO_TARGET_DIR="$tmp/af-w-target" \
  CQLITE_GATE_SLOTS_DIR="$tmp/af-w-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
af_w_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; af_w_status=$WX_STATUS; af_w_markers=$WX_MARKERS
assert_no_timeout "af write-probe (shipped)"
af_w_line=$(grep_line "$af_w_err" '^agent-gate: disk-admission: ')
case "$af_w_line" in
  *'disk-admission: PASS'*)
    ok "isolation: the SHIPPED gate IGNORES the same plant and admits normally — the write probe is isolated" ;;
  *) bad "isolation: the shipped gate was steered by an inherited PYTHONPATH (exit $af_w_status): ${af_w_line:-<none>}" ;;
esac
if [ "$af_w_status" -eq 0 ] && [ "$af_w_markers" -ge 1 ]; then
  ok "isolation: ...and it reached its work phase, so the plant changed nothing at all"
else
  bad "isolation: the shipped gate did not complete under the plant (exit $af_w_status, markers $af_w_markers)"
fi

# ---- (c) THE METADATA PROBE: a plant that WOULD redirect the measured filesystem --
# This is the route `-S` alone does NOT close, so it is asserted separately rather than
# assumed to follow from (b): a hostile `json.py` on PYTHONPATH shadows the stdlib and
# hands the gate an attacker-chosen `target_directory` — which the gate then MEASURES,
# PINS as CARGO_TARGET_DIR, and builds into.
mkdir -p "$tmp/af-plant-json"
af_fake_td="$tmp/af-planted-target-dir"
cat > "$tmp/af-plant-json/json.py" <<AFPJ
def load(f):
    return {"target_directory": "$af_fake_td"}
def loads(s):
    return {"target_directory": "$af_fake_td"}
AFPJ
# CONTROL FIRST — the unisolated copy must MEASURE THE PLANTED DIRECTORY.
RS_GATE="$af_stripped"
run_stub_gate af-j-ctl "$(df_script af-j-ctl "$HIGH")" \
  PYTHONPATH="$tmp/af-plant-json" \
  CQLITE_GATE_SLOTS_DIR="$tmp/af-j-ctl-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
RS_GATE=""
af_jc_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; af_jc_status=$WX_STATUS
assert_no_timeout "af metadata-probe control (unisolated)"
af_jc_line=$(grep_line "$af_jc_err" '^agent-gate: disk-admission: ')
case "$af_jc_line" in
  *"target-dir $af_fake_td "*)
    ok "isolation CONTROL: the UNISOLATED gate measured the PLANTED target dir — a shadowed stdlib json really does redirect the subject filesystem" ;;
  *) bad "isolation CONTROL: the unisolated gate was not redirected by the shadowed json: ${af_jc_line:-<none>} — the shipped assertion below proves nothing" ;;
esac

# THE SHIPPED GATE, SAME PLANT, MUST RESOLVE THE REAL DIRECTORY.
run_stub_gate af-j "$(df_script af-j "$HIGH")" \
  PYTHONPATH="$tmp/af-plant-json" \
  CQLITE_GATE_SLOTS_DIR="$tmp/af-j-slots" CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_STUB_SLEEP=1
af_j_err=$RS_ERR
watch_until_exit "$RS_PID" "$RS_RUNDIR" 300; af_j_status=$WX_STATUS; af_j_markers=$WX_MARKERS
assert_no_timeout "af metadata-probe (shipped)"
af_j_line=$(grep_line "$af_j_err" '^agent-gate: disk-admission: ')
case "$af_j_line" in
  *"target-dir $af_fake_td "*)
    bad "isolation: the SHIPPED gate measured the PLANTED target dir — the metadata probe is steered by an inherited PYTHONPATH: $af_j_line" ;;
  *'target-dir UNRESOLVED'*)
    bad "isolation: the shipped gate could not resolve a target dir at all under the plant — the fix must not break the probe: $af_j_line" ;;
  *'target-dir /'*)
    ok "isolation: the SHIPPED gate ignores the shadowed stdlib json and resolves the REAL target dir — the metadata probe is isolated too" ;;
  *) bad "isolation: unexpected rendering from the shipped metadata probe: ${af_j_line:-<none>}" ;;
esac
if [ "$af_j_status" -eq 0 ] && [ "$af_j_markers" -ge 1 ]; then
  ok "isolation: ...and that run completed normally, so isolating the metadata probe costs a correct run nothing"
else
  bad "isolation: the shipped gate did not complete with the json plant present (exit $af_j_status, markers $af_j_markers)"
fi

printf '\n%s\n' "-----------------------------------------------"
printf 'passed: %d  failed: %d  skipped: %d\n' "$PASS" "$FAIL" "$SKIP"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
