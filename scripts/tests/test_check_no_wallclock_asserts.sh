#!/usr/bin/env bash
# test_check_no_wallclock_asserts.sh — self-test for the #2642 wall-clock guard.
#
# Proves check-no-wallclock-asserts.sh:
#   1. PASSes on a clean fixture (record-only timing, no threshold assert),
#   2. FAILs on a planted `assert!(elapsed < N)` violation,
#   3. respects the `perf-gate-allow` escape hatch,
#   4. and PASSes on the real, retired correctness test path.
# Hermetic: writes fixtures to a temp dir, no cargo/network/datasets.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD="$REPO_ROOT/scripts/tests/check-no-wallclock-asserts.sh"

if [ ! -x "$GUARD" ] && [ ! -f "$GUARD" ]; then
  echo "FAIL: guard script not found at $GUARD"
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (guard is a no-op without it)"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# 1. clean fixture: records timing, never asserts on it.
cat >"$tmp/clean.rs" <<'RS'
#[test]
fn records_timing() {
    let start = std::time::Instant::now();
    do_work();
    let elapsed = start.elapsed();
    eprintln!("[perf-record] work: {elapsed:?} (not asserted)");
    assert_eq!(result_count(), 3);
}
RS
if ! bash "$GUARD" "$tmp/clean.rs" >/dev/null 2>&1; then
  echo "FAIL: guard flagged a clean record-only fixture"
  bash "$GUARD" "$tmp/clean.rs" || true
  exit 1
fi
echo "OK: clean fixture PASSes"

# 2. planted violation: an elapsed < threshold assert.
cat >"$tmp/bad.rs" <<'RS'
#[test]
fn asserts_on_wallclock() {
    let start = std::time::Instant::now();
    do_work();
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() < 50, "too slow: {:?}", elapsed);
}
RS
if bash "$GUARD" "$tmp/bad.rs" >/dev/null 2>&1; then
  echo "FAIL: guard did NOT trip on a planted wall-clock threshold assert"
  exit 1
fi
echo "OK: planted violation is caught"

# 3. escape hatch: an #[ignore]d perf-lane assert marked perf-gate-allow.
cat >"$tmp/allowed.rs" <<'RS'
#[test]
#[ignore = "perf-only"]
fn opt_in_perf_assert() {
    let start = std::time::Instant::now();
    do_work();
    let elapsed = start.elapsed();
    // perf-gate-allow: #[ignore]d opt-in perf lane, not in the default gate.
    assert!(elapsed.as_millis() < 50, "too slow: {:?}", elapsed);
}
RS
if ! bash "$GUARD" "$tmp/allowed.rs" >/dev/null 2>&1; then
  echo "FAIL: guard ignored the perf-gate-allow escape hatch"
  bash "$GUARD" "$tmp/allowed.rs" || true
  exit 1
fi
echo "OK: perf-gate-allow escape hatch respected"

# 4. the real, retired correctness test path must be clean.
if ! bash "$GUARD" >/dev/null 2>&1; then
  echo "FAIL: the real correctness test path still contains a wall-clock threshold assert"
  bash "$GUARD" || true
  exit 1
fi
echo "OK: real correctness test path is clean"

echo "PASS: check-no-wallclock-asserts self-test"
