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
# EXACTLY ONCE (rm -rf the venv, recreate, reinstall, rebuild, re-verify). Only if
# the module STILL will not import after a clean-venv rebuild does it FAIL — with a
# DISTINCT message that names a real binding defect, not a transient venv miss.
#
# This test drives that function hermetically via the hidden `--python-build-verify`
# hook with PATH-shadowed python3/pip/maturin/python (a temp dir prepended to PATH),
# so it needs NO real maturin build and runs in well under a second. It proves BOTH
# branches: (a) an import miss on the first attempt that succeeds after a venv
# rebuild self-heals to PASS (exit 0), and (b) an import that fails on BOTH attempts
# FAILs with exit 3 + the distinct marker. It also pins the healthy path (no
# unnecessary rebuild) and the real-build-failure path (exit 2).
#
# Run standalone:   bash scripts/tests/test_agent_gate_python_bindings_determinism.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

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
# false and behavior is driven solely by the counter (deterministic).
cat >"$stub/python3" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "-m" ] && [ "${2:-}" = "venv" ]; then
  d="${3:?venv dir}"
  mkdir -p "$d/bin"
  : >"$d/bin/activate"
  exit 0
fi
exit 0
EOF

# pip: always succeeds (dependency install is not what we are testing).
cat >"$stub/pip" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

# maturin: `develop ...` exits ${MATURIN_RC:-0}. A non-zero here is a real BUILD
# failure (contract exit 2), NOT the import miss we self-heal.
cat >"$stub/maturin" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "develop" ]; then exit "${MATURIN_RC:-0}"; fi
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

# run_scenario <pass-on-attempt> <maturin-rc> -> sets RC / OUT / COUNTER globals.
# Fresh venv + counter per scenario; PATH-shadowed by the stubs.
run_scenario() {
  local pass_on="$1" maturin_rc="$2"
  local venv="$tmp/venv-$RANDOM$RANDOM"
  local counter="$tmp/counter-$RANDOM$RANDOM"
  rm -rf "$venv"; rm -f "$counter"
  OUT=$(
    PATH="$stub:$PATH" \
    PBV_TEST_COUNTER="$counter" \
    PBV_TEST_PASS_ON_ATTEMPT="$pass_on" \
    MATURIN_RC="$maturin_rc" \
    bash "$GATE" --python-build-verify "$venv" "maturin develop --profile dev -m bindings/python/Cargo.toml" 2>&1
  )
  RC=$?
  COUNTER=0; [ -f "$counter" ] && COUNTER=$(cat "$counter")
}

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
if printf '%s\n' "$OUT" | grep -qF "self-healing with a clean-venv rebuild"; then
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

# ---- (c) real build failure: maturin exits non-zero → exit 2, no import-verify -
run_scenario 1 1
if [ "$RC" -eq 2 ]; then
  ok "build-fail: maturin develop non-zero → exit 2 (real build failure, unchanged)"
else
  bad "build-fail: expected exit 2, got $RC"
  echo "------- out -------"; printf '%s\n' "$OUT"; echo "-------------------"
fi
if [ "$COUNTER" -eq 0 ]; then
  ok "build-fail: NO import-verify attempted (maturin failed before verify)"
else
  bad "build-fail: expected 0 import-verify attempts, got $COUNTER"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
