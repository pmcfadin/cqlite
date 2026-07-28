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
# Every FINITE form systemd MemoryMax accepts, plus the plain byte count. Notes
# go to stderr, so only stdout is compared.
for good in 8G 512M 1.5G 2Gi 64K 1T 50% 100% 1073741824 512MB; do
  if out=$(bash "$SCRIPT" --check-args --mem "$good" --swap "$good" -- true 2>/dev/null) &&
     [[ "$out" == "ARGS-OK mem=$good swap=$good cmd=true" ]]; then
    pass "accepts --mem/--swap '$good'"
  else
    fail "rejected valid memory value '$good' (out: $out)"
  fi
done

# `--swap 0` is the legitimate "no swap at all" cap and must stay accepted
# (unlike `--mem 0`, which is a nonsense cap -- see the rejections below).
for goodswap in 0 0G 0%; do
  if out=$(bash "$SCRIPT" --check-args --swap "$goodswap" -- true 2>/dev/null) &&
     [[ "$out" == "ARGS-OK mem=8G swap=$goodswap cmd=true" ]]; then
    pass "accepts --swap '$goodswap' (no swap)"
  else
    fail "rejected valid zero swap cap '$goodswap' (out: $out)"
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

# ------------------------------------------------- UNBOUNDED caps are refused --
# systemd ACCEPTS MemoryMax=max/infinity and it DISABLES the limit, i.e. runs the
# "contained" workload uncontained -- the exact state that livelocked a swapless
# host for 75 minutes. Case-insensitively refused for BOTH caps.
for unbounded in max infinity MAX INFINITY Max Infinity mAx iNfInItY; do
  reject "--mem $unbounded (unbounded cap)"  --mem "$unbounded"
  reject "--swap $unbounded (unbounded cap)" --swap "$unbounded"
done

# Zero / over-100% --mem caps are nonsense, not containment.
reject "zero --mem"               --mem 0
reject "zero --mem (suffixed)"    --mem 0G
reject "zero --mem (percent)"     --mem 0%
reject "over-100% --mem"          --mem 200%
reject "over-100% --swap"         --swap 101%

# A SUFFIXLESS number is BYTES to systemd, so a bare `8` is an 8-BYTE cap: every
# run would look like an instant OOM and hide the real result. Refused outright
# below 1 MiB rather than silently producing a uselessly tiny cap.
reject "bare small integer --mem"   --mem 8
reject "sub-1MiB byte count --mem"  --mem 1024
reject "bare small integer --swap"  --swap 8

# A large suffixless count IS a legitimate byte cap, but the resolved reading is
# echoed so it can never be a silent misunderstanding.
err=$(bash "$SCRIPT" --check-args --mem 1073741824 -- true 2>&1 >/dev/null)
if grep -q "reads it as BYTES" <<<"$err"; then
  pass "suffixless byte count reports its resolved reading"
else
  fail "no byte-count note for a suffixless --mem (err: $err)"
fi

# The usage text must explain the byte-count reading AND the unbounded refusal.
help_out=$(bash "$SCRIPT" --help 2>&1)
if grep -q "BYTE count" <<<"$help_out" || grep -q "byte count" <<<"$help_out"; then
  pass "usage documents that a bare number is a byte count"
else
  fail "usage text does not explain the bare-number/byte-count reading"
fi
if grep -qi "REFUSED" <<<"$help_out" && grep -qi "infinity" <<<"$help_out"; then
  pass "usage documents that max/infinity are refused"
else
  fail "usage text does not document the unbounded-cap refusal"
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
