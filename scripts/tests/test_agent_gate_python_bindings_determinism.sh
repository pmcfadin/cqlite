#!/usr/bin/env bash
# Regression test for issue #1803: the agent-gate `python-bindings` component (and
# the symmetric --lite python tier) intermittently FAILed at pytest import with
# `ModuleNotFoundError: cqlite._cqlite` even though the wheel built and the code
# compiled cleanly — a stale/half-built editable install in the persistent venv
# (a cleaned target/, an interrupted prior run, or a concurrent same-checkout
# gate) that `maturin develop` (exit 0) did not repair. The aggregate gate then
# read FAIL over green code, eroding "the gate is the only run that counts".
#
# The fix routes BOTH call sites through _python_build_verify_venv, which after a
# maturin-exited-0 build VERIFIES `import cqlite._cqlite`, and on a miss self-heals
# EXACTLY ONCE. Only if the module STILL will not import after the rebuild does it
# FAIL — with a DISTINCT message that names a real binding defect, not a transient
# venv miss.
#
# roborev round 2 refined this: (Finding A) `maturin develop` can also fail because
# the build TOOLCHAIN itself is absent (no cargo/rustc on PATH — an offline/
# toolchain gap), which must classify the SAME as a venv/pip setup gap (SKIP in the
# lite tier), not as a real compile error (FAIL) — the function now returns a
# DISTINCT exit 4 for that case, checked BEFORE invoking maturin. (Finding B) the
# self-heal branch no longer `rm -rf`s the SHARED persistent venv (a concurrent
# same-checkout gate reusing it would otherwise race a mid-build/pytest teardown);
# it builds into a PRIVATE per-process "$venv.heal.$$" venv instead and writes that
# path back to an out-file so the caller knows what to activate + clean up.
#
# This test drives that function hermetically via the hidden `--python-build-verify`
# hook with PATH-shadowed python3/pip/maturin/python/cargo/rustc (temp dirs
# prepended to PATH), so it needs NO real maturin build and runs in well under a
# second. It proves: (a) an import miss on attempt 1 that succeeds after a rebuild
# self-heals to PASS (exit 0) INTO A PRIVATE venv, leaving the shared venv's
# sentinel untouched; (b) an import that fails on BOTH attempts FAILs with exit 3 +
# the distinct marker; (c) `maturin develop` failing WITH cargo/rustc present is a
# real compile error (exit 2, FAIL class); (d) `maturin develop` failing because
# cargo/rustc are ABSENT is a toolchain gap (exit 4, SKIP class) — distinct from
# (c); (e) the healthy first-try path does not rebuild; (f) an already-healthy
# persistent venv is REUSED, never torn down.
#
# Run standalone:   bash scripts/tests/test_agent_gate_python_bindings_determinism.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
# Resolve bash by its REAL absolute path from the CURRENT (unshadowed)
# environment, before any scenario overrides PATH — used for the
# toolchain-absent scenario so command resolution never depends on which
# PATH is in effect at invocation time (roborev round 3, Finding 1).
BASH_BIN=$(command -v bash)

DISTINCT_MARKER="did not import after clean-venv rebuild — real binding defect"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-py1803.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# ---- PATH-shadowed stub toolchain --------------------------------------------
# The stubs simulate `maturin develop` (exit 0 unless MATURIN_RC set) and an
# import-verify whose success is driven by an attempt counter + policy, so the
# self-heal control flow is exercised with no real python/maturin.
stub="$tmp/stubbin"
mkdir -p "$stub"

# python3: satisfies `python3 -m venv <dir>` by creating <dir>/bin + an EMPTY
# activate (no PATH mutation → the verify `python -c` stays PATH-shadowed by our
# stub). It deliberately does NOT create <dir>/bin/python, so the reuse guard is
# false and behavior is driven solely by the counter (deterministic) UNLESS a
# scenario pre-populates <venv>/bin/python itself (the reuse scenario below).
# Also increments PBV_TEST_PY3_COUNTER so the reuse scenario can assert `python3
# -m venv` (i.e. a rebuild) was NEVER invoked. PBV_TEST_VENV_FAIL=1 makes venv
# creation itself fail (rc-1 setup-gap scenario, roborev round 3 Finding 2).
cat >"$stub/python3" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "-m" ] && [ "${2:-}" = "venv" ]; then
  if [ "${PBV_TEST_VENV_FAIL:-0}" = 1 ]; then
    exit 1
  fi
  n=0; [ -f "${PBV_TEST_PY3_COUNTER:-/dev/null}" ] && n=$(cat "$PBV_TEST_PY3_COUNTER" 2>/dev/null || echo 0)
  n=$((n + 1)); [ -n "${PBV_TEST_PY3_COUNTER:-}" ] && printf '%s' "$n" >"$PBV_TEST_PY3_COUNTER"
  d="${3:?venv dir}"
  mkdir -p "$d/bin"
  : >"$d/bin/activate"
  exit 0
fi
exit 0
EOF

# pip: succeeds unless PBV_TEST_PIP_FAIL=1 (rc-1 setup-gap scenario, roborev
# round 3 Finding 2 — an alternate trigger for the same "venv/pip toolchain
# setup failed" class, exercised via the `pip install maturin pytest` call).
cat >"$stub/pip" <<'EOF'
#!/usr/bin/env bash
[ "${PBV_TEST_PIP_FAIL:-0}" = 1 ] && exit 1
exit 0
EOF

# maturin: `develop ...` exits ${MATURIN_RC:-0}. A non-zero here is a real BUILD
# failure (contract exit 2), NOT the import miss we self-heal. Also increments
# PBV_TEST_MATURIN_COUNTER so the reuse scenario can assert it ran AT MOST ONCE
# (no needless rebuild-and-rebuild on an already-healthy venv).
cat >"$stub/maturin" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "develop" ]; then
  n=0; [ -f "${PBV_TEST_MATURIN_COUNTER:-/dev/null}" ] && n=$(cat "$PBV_TEST_MATURIN_COUNTER" 2>/dev/null || echo 0)
  n=$((n + 1)); [ -n "${PBV_TEST_MATURIN_COUNTER:-}" ] && printf '%s' "$n" >"$PBV_TEST_MATURIN_COUNTER"
  exit "${MATURIN_RC:-0}"
fi
exit 0
EOF

# python: only ever invoked as `python -c 'import cqlite; import cqlite._cqlite'`
# by _pbv_verify. Increments PBV_TEST_COUNTER and succeeds iff the attempt number
# has reached PBV_TEST_PASS_ON_ATTEMPT (0 = never succeed).
cat >"$stub/python" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "-c" ]; then
  n=0; [ -f "${PBV_TEST_COUNTER:-/dev/null}" ] && n=$(cat "$PBV_TEST_COUNTER" 2>/dev/null || echo 0)
  n=$((n + 1)); printf '%s' "$n" >"${PBV_TEST_COUNTER:?}"
  if [ "${PBV_TEST_PASS_ON_ATTEMPT:-0}" -gt 0 ] && [ "$n" -ge "${PBV_TEST_PASS_ON_ATTEMPT}" ]; then
    exit 0
  fi
  exit 1
fi
exit 0
EOF
chmod +x "$stub"/python3 "$stub"/pip "$stub"/maturin "$stub"/python

# Toolchain stub, in a SEPARATE directory from $stub (Finding A): `command -v
# cargo`/`command -v rustc` just need SOMETHING executable to resolve, so a
# no-op `exit 0` suffices. Kept apart from $stub so the toolchain-absent
# scenario can shadow python3/pip/maturin/python WITHOUT also shadowing
# cargo/rustc — PATH composition alone selects "present" vs "absent".
stub_tc="$tmp/stubbin-toolchain"
mkdir -p "$stub_tc"
printf '#!/usr/bin/env bash\nexit 0\n' >"$stub_tc/cargo"
printf '#!/usr/bin/env bash\nexit 0\n' >"$stub_tc/rustc"
chmod +x "$stub_tc/cargo" "$stub_tc/rustc"

# Curated minimal-coreutils shim (roborev round 3, Finding 1): the ORIGINAL
# toolchain-absent scenario appended the real system `/usr/bin:/bin` to the
# PATH so agent-gate.sh's own startup path (dirname/basename/mkdir, all used
# before + inside the hook) still resolved — but that made "absent" HOST-
# DEPENDENT: a CI image with a distro-packaged cargo/rustc under /usr/bin
# would leak a REAL cargo/rustc into the scenario, silently exercising rc 2
# instead of rc 4 and passing vacuously. Fix: resolve the handful of coreutils
# actually needed (dirname, basename, mkdir, cat) to their REAL absolute paths
# from the CURRENT (real, unshadowed) PATH ONCE, symlink ONLY those into a
# dedicated directory, and build the scenario's PATH from ONLY $stub + this
# directory — no system directory is ever on it, so cargo/rustc can NEVER
# resolve regardless of the host. `bash` is included too: every stub script
# has a `#!/usr/bin/env bash` shebang, and `env` resolves that interpreter
# via the CHILD process's (i.e. our shadowed) PATH, not the caller's real one
# — without a `bash` on this minimal PATH the stubs themselves fail to
# execute ("env: bash: No such file or directory"), which was caught by this
# very scenario misreporting rc 1 instead of rc 4 during development.
stub_coreutils_min="$tmp/stubbin-coreutils-min"
mkdir -p "$stub_coreutils_min"
for _tool in dirname basename mkdir cat bash; do
  _tool_real=$(command -v "$_tool" 2>/dev/null) || {
    echo "FATAL: '$_tool' not found on the real PATH — cannot build the hermetic toolchain-absent scenario" >&2
    exit 1
  }
  ln -s "$_tool_real" "$stub_coreutils_min/$_tool"
done
unset _tool _tool_real

# run_scenario <pass-on-attempt> <maturin-rc> -> sets RC / OUT / COUNTER globals.
# Fresh venv + counter per scenario; PATH-shadowed by the stubs (incl. the
# toolchain stub, so cargo/rustc always resolve here — the toolchain-ABSENT
# case is exercised separately by run_toolchain_scenario, which omits $stub_tc).
run_scenario() {
  local pass_on="$1" maturin_rc="$2"
  local venv="$tmp/venv-$RANDOM$RANDOM"
  local counter="$tmp/counter-$RANDOM$RANDOM"
  rm -rf "$venv"; rm -f "$counter"
  OUT=$(
    PATH="$stub:$stub_tc:$PATH" \
    PBV_TEST_COUNTER="$counter" \
    PBV_TEST_PASS_ON_ATTEMPT="$pass_on" \
    MATURIN_RC="$maturin_rc" \
    bash "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" 2>&1
  )
  RC=$?
  COUNTER=0; [ -f "$counter" ] && COUNTER=$(cat "$counter")
}

# run_reuse_scenario: pre-populate a venv that ALREADY has a usable
# <venv>/bin/python marker (so `_pbv_setup`'s `[ -x "$venv/bin/python" ]` reuse
# guard is TRUE) and where the import verifies on the FIRST attempt. Also drops
# a sentinel file directly in the venv dir so a `rm -rf "$venv"` (the self-heal
# teardown) would be detectable by its absence. Sets RC / OUT / COUNTER (import
# attempts) / PY3_COUNTER (venv-creation calls) / MATURIN_COUNTER / SENTINEL_OK.
run_reuse_scenario() {
  local venv="$tmp/venv-reuse-$RANDOM$RANDOM"
  local counter="$tmp/counter-$RANDOM$RANDOM"
  local py3_counter="$tmp/py3counter-$RANDOM$RANDOM"
  local maturin_counter="$tmp/maturincounter-$RANDOM$RANDOM"
  rm -rf "$venv"; rm -f "$counter" "$py3_counter" "$maturin_counter"
  mkdir -p "$venv/bin"
  : >"$venv/bin/python"; chmod +x "$venv/bin/python"
  : >"$venv/bin/activate"
  : >"$venv/SENTINEL"
  OUT=$(
    PATH="$stub:$stub_tc:$PATH" \
    PBV_TEST_COUNTER="$counter" \
    PBV_TEST_PY3_COUNTER="$py3_counter" \
    PBV_TEST_MATURIN_COUNTER="$maturin_counter" \
    PBV_TEST_PASS_ON_ATTEMPT=1 \
    MATURIN_RC=0 \
    bash "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" 2>&1
  )
  RC=$?
  COUNTER=0; [ -f "$counter" ] && COUNTER=$(cat "$counter")
  PY3_COUNTER=0; [ -f "$py3_counter" ] && PY3_COUNTER=$(cat "$py3_counter")
  MATURIN_COUNTER=0; [ -f "$maturin_counter" ] && MATURIN_COUNTER=$(cat "$maturin_counter")
  SENTINEL_OK=0; [ -f "$venv/SENTINEL" ] && SENTINEL_OK=1
}

# ---- (e) venv REUSE: an already-healthy venv verifies on attempt 1 → NO rebuild,
#     NO `python3 -m venv` (re)creation, `maturin develop` invoked at most once,
#     no self-heal message, and the pre-existing venv (sentinel) survives —
#     preserving the persistent-venv speed optimization (a stated AC).
run_reuse_scenario
if [ "$RC" -eq 0 ]; then
  ok "reuse: already-healthy venv verifies on first attempt → exit 0"
else
  bad "reuse: expected exit 0, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$COUNTER" -eq 1 ]; then
  ok "reuse: exactly ONE import-verify attempt (no rebuild)"
else
  bad "reuse: expected 1 import-verify attempt, got $COUNTER"
fi
if [ "$PY3_COUNTER" -eq 0 ]; then
  ok "reuse: python3 -m venv NEVER invoked (existing venv/bin/python reused, not recreated)"
else
  bad "reuse: expected 0 'python3 -m venv' calls (venv should be reused), got $PY3_COUNTER"
fi
if [ "$MATURIN_COUNTER" -eq 1 ]; then
  ok "reuse: maturin develop invoked exactly ONCE (no needless rebuild-and-rebuild)"
else
  bad "reuse: expected exactly 1 'maturin develop' call, got $MATURIN_COUNTER"
fi
if [ "$SENTINEL_OK" -eq 1 ]; then
  ok "reuse: the pre-existing venv survives (no rm -rf teardown of a healthy venv)"
else
  bad "reuse: the venv sentinel was removed — a healthy venv was torn down unnecessarily"
fi
if printf '%s\n' "$OUT" | grep -qF "self-healing into a private venv"; then
  bad "reuse: unexpectedly emitted the self-heal notice for an already-healthy venv"
else
  ok "reuse: no self-heal notice emitted (nothing to heal)"
fi

# ---- (d) healthy first-try: import verifies immediately → PASS, no rebuild -----
run_scenario 1 0
if [ "$RC" -eq 0 ]; then
  ok "healthy: import verifies on first attempt → exit 0 (persistent venv reused)"
else
  bad "healthy: expected exit 0, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$COUNTER" -eq 1 ]; then
  ok "healthy: exactly ONE import-verify attempt (no unnecessary clean-venv rebuild)"
else
  bad "healthy: expected 1 import-verify attempt (no rebuild), got $COUNTER"
fi

# ---- (a) transient venv miss: fail attempt 1, pass after rebuild → self-heal ---
run_scenario 2 0
if [ "$RC" -eq 0 ]; then
  ok "self-heal: import miss on attempt 1, success after clean-venv rebuild → exit 0"
else
  bad "self-heal: expected exit 0 (self-healed), got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$COUNTER" -eq 2 ]; then
  ok "self-heal: exactly TWO import-verify attempts (one rebuild, healed once)"
else
  bad "self-heal: expected 2 import-verify attempts (heal ONCE), got $COUNTER"
fi
if printf '%s\n' "$OUT" | grep -qF "self-healing into a private venv"; then
  ok "self-heal: emits the self-heal notice"
else
  bad "self-heal: missing the self-heal notice"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi

# ---- (b) real defect: import fails on BOTH attempts → distinct FAIL (exit 3) ---
run_scenario 0 0
if [ "$RC" -eq 3 ]; then
  ok "real-defect: import fails after clean-venv rebuild → exit 3 (distinct code)"
else
  bad "real-defect: expected exit 3, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if printf '%s\n' "$OUT" | grep -qF "$DISTINCT_MARKER"; then
  ok "real-defect: emits the DISTINCT 'real binding defect, not a venv miss' message"
else
  bad "real-defect: missing the distinct binding-defect marker"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$COUNTER" -eq 2 ]; then
  ok "real-defect: exactly TWO import-verify attempts (self-heal tried once, then FAIL)"
else
  bad "real-defect: expected 2 import-verify attempts, got $COUNTER"
fi

# ---- (c) real COMPILE error (cargo/rustc PRESENT, roborev round-2 Finding A):
#     maturin exits non-zero with the build toolchain available → exit 2 (FAIL
#     class), no import-verify attempted. Distinguished from (g) below, where
#     the SAME maturin failure happens because the toolchain is ABSENT.
run_scenario 1 1
if [ "$RC" -eq 2 ]; then
  ok "compile-error: maturin develop non-zero WITH cargo/rustc present → exit 2 (real compile error, FAIL class)"
else
  bad "compile-error: expected exit 2, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$COUNTER" -eq 0 ]; then
  ok "compile-error: NO import-verify attempted (maturin failed before verify)"
else
  bad "compile-error: expected 0 import-verify attempts, got $COUNTER"
fi

# ---- (g) TOOLCHAIN ABSENT (roborev round-2 Finding A, made HERMETIC by round-3
#     Finding 1): no cargo/rustc on PATH at all → exit 4 (SKIP class, same as
#     rc 1), DISTINCT from the real compile-error exit 2 above even though
#     `maturin develop` "fails" in both. The scenario PATH is built ENTIRELY
#     from $stub (python3/pip/maturin/python) + $stub_coreutils_min (the
#     curated dirname/basename/mkdir/cat symlinks) — NEVER a system directory
#     — so cargo/rustc cannot resolve regardless of what the host happens to
#     have installed. A fake HOME additionally blocks the gate's own
#     `$HOME/.cargo/bin` auto-detect from re-injecting a real cargo, and
#     $BASH_BIN (resolved once from the real PATH, before any override) makes
#     the bash invocation itself independent of the scenario's PATH too.
run_toolchain_absent_scenario() {
  local venv="$tmp/venv-tcabsent-$RANDOM$RANDOM"
  local fake_home="$tmp/fakehome-$RANDOM$RANDOM"
  rm -rf "$venv"; mkdir -p "$fake_home"
  OUT=$(
    PATH="$stub:$stub_coreutils_min" \
    HOME="$fake_home" \
    MATURIN_RC=1 \
    "$BASH_BIN" "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" 2>&1
  )
  RC=$?
}
run_toolchain_absent_scenario
if [ "$RC" -eq 4 ]; then
  ok "toolchain-absent: no cargo/rustc resolvable on a PATH built ENTIRELY from our stub dirs → exit 4 (toolchain gap, SKIP class — distinct from exit 2, hermetic regardless of host)"
else
  bad "toolchain-absent: expected exit 4, got $RC (if this is 2, the scenario PATH leaked a REAL cargo/rustc from the host — hermeticity broken)"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi

# ---- (i) VENV/PIP SETUP FAILURE (roborev round 3, Finding 2 — completes the
#     exit-code matrix: 0 healthy / 1 setup-gap / 2 compile-error / 3 import-
#     after-rebuild / 4 toolchain-absent). `python3 -m venv` itself failing
#     (PBV_TEST_VENV_FAIL=1) is a genuine venv-creation toolchain gap →
#     _pbv_setup fails BEFORE the build stage is ever reached → exit 1. No
#     import-verify or maturin invocation should happen.
run_setup_fail_scenario() {
  local venv="$tmp/venv-setupfail-$RANDOM$RANDOM"
  local counter="$tmp/counter-$RANDOM$RANDOM"
  local maturin_counter="$tmp/maturincounter-$RANDOM$RANDOM"
  rm -rf "$venv"; rm -f "$counter" "$maturin_counter"
  OUT=$(
    PATH="$stub:$stub_tc:$PATH" \
    PBV_TEST_COUNTER="$counter" \
    PBV_TEST_MATURIN_COUNTER="$maturin_counter" \
    PBV_TEST_VENV_FAIL=1 \
    MATURIN_RC=0 \
    bash "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" 2>&1
  )
  RC=$?
  COUNTER=0; [ -f "$counter" ] && COUNTER=$(cat "$counter")
  MATURIN_COUNTER=0; [ -f "$maturin_counter" ] && MATURIN_COUNTER=$(cat "$maturin_counter")
}
run_setup_fail_scenario
if [ "$RC" -eq 1 ]; then
  ok "setup-fail: python3 -m venv failing → exit 1 (venv/pip toolchain setup gap)"
else
  bad "setup-fail: expected exit 1, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$MATURIN_COUNTER" -eq 0 ]; then
  ok "setup-fail: maturin develop NEVER invoked (setup failed before the build stage)"
else
  bad "setup-fail: expected 0 'maturin develop' calls, got $MATURIN_COUNTER"
fi
if [ "$COUNTER" -eq 0 ]; then
  ok "setup-fail: NO import-verify attempted (setup failed before verify)"
else
  bad "setup-fail: expected 0 import-verify attempts, got $COUNTER"
fi
# Same class via the OTHER setup call: `pip install maturin pytest` failing
# (venv creation itself succeeds, the dependency install does not).
run_pip_fail_scenario() {
  local venv="$tmp/venv-pipfail-$RANDOM$RANDOM"
  rm -rf "$venv"
  OUT=$(
    PATH="$stub:$stub_tc:$PATH" \
    PBV_TEST_PIP_FAIL=1 \
    MATURIN_RC=0 \
    bash "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" 2>&1
  )
  RC=$?
}
run_pip_fail_scenario
if [ "$RC" -eq 1 ]; then
  ok "setup-fail (pip): pip install failing → exit 1 (same venv/pip toolchain setup gap)"
else
  bad "setup-fail (pip): expected exit 1, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi

# ---- (j) CALLER classification (roborev round 3, Finding 2): the --lite
#     python tier explicitly branches rc 2 (FAIL), rc 3 (FAIL), and rc 4
#     (SKIP) — rc 1 has NO explicit branch, so it MUST fall through to the
#     generic `elif [ "$pbv_rc" -ne 0 ]` catch-all, which is the SKIP branch.
#     This reads the ACTUAL source (not a re-implementation) so it breaks the
#     moment someone adds an explicit — and possibly wrong — rc==1 branch
#     that reclassifies a toolchain setup gap as a code FAIL.
# NOTE: "$python_diff" -eq 1 appears TWICE in agent-gate.sh (a plan-string
# helper AND the real python-tier execution block) — anchor on a comment
# string unique to the REAL block (the "SKIP vs FAIL split" note directly
# above it) so this reliably extracts the right one even if a future edit
# reorders the two.
lite_tier_src=$(awk '/SKIP vs FAIL split \(roborev job 1449, Low\)/{flag=1} flag{print; if (/^  fi$/) exit}' "$GATE")
if [ -z "$lite_tier_src" ]; then
  bad "lite-tier-classification: could not locate the python-tier block in $GATE (source shape changed — update this test)"
elif printf '%s\n' "$lite_tier_src" | grep -qE 'pbv_rc.*-eq 2' \
   && printf '%s\n' "$lite_tier_src" | grep -qE 'pbv_rc.*-eq 3' \
   && printf '%s\n' "$lite_tier_src" | grep -qE 'pbv_rc.*-eq 4' \
   && printf '%s\n' "$lite_tier_src" | grep -qE 'pbv_rc.*-ne 0'; then
  ok "lite-tier-classification: rc 1 has NO explicit branch (only 2/3/4 do), so it falls through the catch-all 'pbv_rc -ne 0' branch → SKIP (source-verified, not re-implemented)"
else
  bad "lite-tier-classification: expected explicit rc==2/rc==3/rc==4 branches plus a catch-all 'pbv_rc -ne 0' branch in the python tier; the source shape has changed"
  echo "------- lite tier source -------"; printf '%s\n' "$lite_tier_src"; echo "---------------------------------"
fi
# The catch-all branch's OWN note text must say SKIPPED, not FAIL — pinning
# that the fallthrough destination is really the SKIP branch, not some other
# unnamed non-zero-handling code path.
if printf '%s\n' "$lite_tier_src" | grep -A2 'pbv_rc.*-ne 0' | grep -q 'SKIP'; then
  ok "lite-tier-classification: the catch-all 'pbv_rc -ne 0' branch is the SKIP branch (not FAIL)"
else
  bad "lite-tier-classification: the catch-all 'pbv_rc -ne 0' branch does not read as SKIP"
fi

# ---- (h) self-heal builds into a PRIVATE venv, never touches the SHARED venv
#     (roborev round-2 Finding B). Pre-populate the "shared" venv dir with a
#     sentinel file; force an import miss on attempt 1 that succeeds after a
#     rebuild (the self-heal path); assert the written-back active-venv path
#     is a DIFFERENT "$venv.heal.*" directory AND that the shared venv's
#     sentinel survives (proving no `rm -rf` of shared mutable state — the
#     race #1803 exists to eliminate, since concurrent same-checkout gates
#     reuse the same shared venv).
run_selfheal_privatevenv_scenario() {
  local venv="$tmp/venv-heal-$RANDOM$RANDOM"
  local counter="$tmp/counter-$RANDOM$RANDOM"
  local out_file="$tmp/outfile-$RANDOM$RANDOM"
  rm -rf "$venv"; rm -f "$counter" "$out_file"
  mkdir -p "$venv"
  : >"$venv/SHARED_SENTINEL"
  OUT=$(
    PATH="$stub:$stub_tc:$PATH" \
    PBV_TEST_COUNTER="$counter" \
    PBV_TEST_PASS_ON_ATTEMPT=2 \
    MATURIN_RC=0 \
    bash "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" "$out_file" 2>&1
  )
  RC=$?
  ACTIVE_VENV=""; [ -f "$out_file" ] && ACTIVE_VENV=$(cat "$out_file")
  SHARED_SENTINEL_OK=0; [ -f "$venv/SHARED_SENTINEL" ] && SHARED_SENTINEL_OK=1
  HEAL_VENV_EXISTS=0; [ -n "$ACTIVE_VENV" ] && [ -d "$ACTIVE_VENV" ] && HEAL_VENV_EXISTS=1
  SHARED_VENV="$venv"
}
run_selfheal_privatevenv_scenario
if [ "$RC" -eq 0 ]; then
  ok "private-heal: self-heals to exit 0"
else
  bad "private-heal: expected exit 0, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
case "$ACTIVE_VENV" in
  "$SHARED_VENV".heal.*)
    ok "private-heal: the written-back active venv is a PRIVATE '\$venv.heal.*' path ($ACTIVE_VENV), not the shared venv" ;;
  *)
    bad "private-heal: expected an active venv matching '\$venv.heal.*', got '$ACTIVE_VENV'" ;;
esac
if [ "$HEAL_VENV_EXISTS" -eq 1 ]; then
  ok "private-heal: the private heal venv directory actually exists on disk"
else
  bad "private-heal: the reported active venv directory does not exist"
fi
if [ "$SHARED_SENTINEL_OK" -eq 1 ]; then
  ok "private-heal: the SHARED venv's sentinel survives (no rm -rf of shared mutable state — race-free)"
else
  bad "private-heal: the shared venv's sentinel was removed — the shared venv was destroyed (the exact race Finding B forbids)"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
