#!/usr/bin/env bash
# Self-test for test-data/scripts/perf-run-contained.sh (issue #3068).
#
# The wrapper exists because an UNCONTAINED cold read of the multi-GB #3068 perf
# corpus hard-hung a swapless host for 75 minutes. That makes its ARGUMENT
# PARSING safety-critical: a silently-misread `--mem` is the difference between
# "the offending process dies" and "the machine dies". In particular a bare `8`
# must NOT be accepted -- systemd would read it as 8 BYTES, so every run would
# look like an instant OOM and hide the real result.
#
# Hermetic: uses the `--check-args` hook, so nothing is ever executed, no sudo,
# no systemd, no cargo, no network, no datasets.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$REPO_ROOT/test-data/scripts/perf-run-contained.sh"

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

# ------------------------------------------------------------------ preflight --
[ -f "$SCRIPT" ] || { echo "FAIL - missing $SCRIPT"; exit 1; }

# The wrapper is invoked directly in runbooks/measurement scripts, so the
# committed mode must be executable (git tracks the x bit).
if [ -x "$SCRIPT" ]; then
  pass "perf-run-contained.sh is executable"
else
  fail "perf-run-contained.sh is NOT executable (chmod +x and commit mode 755)"
fi

# ------------------------------------------------------- accepted mem values --
# Every form systemd MemoryMax accepts, plus the plain byte count.
for good in 8G 512M 1.5G 2Gi 64K 1T 50% max infinity 1073741824 512MB; do
  if out=$(bash "$SCRIPT" --check-args --mem "$good" --swap "$good" -- true 2>&1) &&
     [[ "$out" == "ARGS-OK mem=$good swap=$good cmd=true" ]]; then
    pass "accepts --mem/--swap '$good'"
  else
    fail "rejected valid memory value '$good' (out: $out)"
  fi
done

# --------------------------------------------------------- rejected garbage ---
# Each must exit 2 (usage error) and never fall through to execution.
reject() { # reject <label> <args...>
  local label="$1"; shift
  local out rc
  out=$(bash "$SCRIPT" --check-args "$@" -- true 2>&1); rc=$?
  if [ "$rc" -eq 2 ] && [[ "$out" != ARGS-OK* ]]; then
    pass "rejects $label (exit 2)"
  else
    fail "accepted $label -- expected exit 2, got rc=$rc (out: $out)"
  fi
}

reject "non-numeric --mem"        --mem banana
reject "trailing junk --mem"      --mem 8G8
reject "negative --mem"           --mem -1G
reject "empty --mem"              --mem ""
reject "space-separated --mem"    --mem "8 G"
reject "shell metachars in --mem" --mem '8G;reboot'
reject "bad suffix --mem"         --mem 8Q
reject "non-numeric --swap"       --swap banana
reject "negative --swap"          --swap -2G
reject "unknown flag"             --memory 8G

# A bare integer IS a valid systemd byte count, so it is accepted -- but the
# usage text must warn about the byte-count reading that makes `8` a footgun.
if bash "$SCRIPT" --help 2>&1 | grep -q "byte count"; then
  pass "usage documents that a bare number is a byte count"
else
  fail "usage text does not explain the bare-number/byte-count reading"
fi

# ------------------------------------------------------- structural failures --
out=$(bash "$SCRIPT" --check-args --mem 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "requires a value" <<<"$out"; then
  pass "rejects --mem with no value (no bash 'unbound variable' leak)"
else
  fail "--mem with no value: expected exit 2 + 'requires a value', rc=$rc ($out)"
fi

out=$(bash "$SCRIPT" --check-args --mem 4G -- 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "missing command" <<<"$out"; then
  pass "rejects an empty command after --"
else
  fail "empty command after --: expected exit 2, rc=$rc ($out)"
fi

# Defaults must be the documented 8G/2G, not empty.
if out=$(bash "$SCRIPT" --check-args -- true 2>&1) &&
   [[ "$out" == "ARGS-OK mem=8G swap=2G cmd=true" ]]; then
  pass "defaults are mem=8G swap=2G"
else
  fail "unexpected defaults (out: $out)"
fi

# Args after `--` are passed through verbatim and NOT parsed as wrapper flags.
if out=$(bash "$SCRIPT" --check-args -- cargo run --mem 1 2>&1) &&
   [[ "$out" == "ARGS-OK mem=8G swap=2G cmd=cargo run --mem 1" ]]; then
  pass "post -- arguments are not reinterpreted as wrapper flags"
else
  fail "post -- passthrough broken (out: $out)"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "test_perf_run_contained: ALL PASS"
  exit 0
fi
echo "test_perf_run_contained: $fails FAILURE(S)"
exit 1
