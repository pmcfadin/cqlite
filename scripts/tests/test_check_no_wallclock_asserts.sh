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

# 3b. Scan-surface extension (issue #2902): the DEFAULT roots must now cover the
# workspace-root tests/*.rs integration files (the #2720 gap), scanned
# NON-RECURSIVELY. Build a throwaway fake repo so the copied guard derives its
# REPO_ROOT from its own location, then assert the default (no-arg) scan flags a
# top-level tests/*.rs violation but does NOT descend into a subtree.
fakeroot="$tmp/fakerepo"
mkdir -p "$fakeroot/scripts/tests" "$fakeroot/tests/deep"
# Canonicalize to the PHYSICAL path (pwd -P): on macOS $TMPDIR lives under
# /var -> /private/var, and the guard derives REPO_ROOT via a logical `pwd`. If we
# left $fakeroot logical, the guard's ROOTS would be `/var/...` while python's
# os.getcwd() is physical `/private/var/...`, and os.path.relpath would emit a
# `../`-laden path that the containment check below would (correctly) reject as
# an escape. Pinning the physical path keeps relpath fake-repo-relative.
fakeroot="$(cd "$fakeroot" && pwd -P)"
cp "$GUARD" "$fakeroot/scripts/tests/check-no-wallclock-asserts.sh"
cat >"$fakeroot/tests/planted.rs" <<'RS'
#[tokio::test]
async fn planted_top_level() {
    let start = std::time::Instant::now();
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() < 5, "planted top-level violation");
}
RS
cat >"$fakeroot/tests/deep/nested.rs" <<'RS'
#[tokio::test]
async fn planted_deep() {
    let start = std::time::Instant::now();
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() < 5, "planted deep (must NOT be scanned)");
}
RS
fake_guard="$fakeroot/scripts/tests/check-no-wallclock-asserts.sh"
# Run from $fakeroot so the guard's relpath output is fake-repo-relative (used by
# the containment assert below).
default_out="$(cd "$fakeroot" && bash "$fake_guard" 2>&1)" && default_rc=0 || default_rc=$?
if [ "$default_rc" -eq 0 ]; then
  echo "FAIL: default scan did NOT flag a top-level tests/*.rs violation (surface not extended)"
  echo "$default_out"
  exit 1
fi
if ! grep -q 'tests/planted.rs' <<<"$default_out"; then
  echo "FAIL: default scan output did not name the top-level tests/planted.rs offender"
  echo "$default_out"
  exit 1
fi
if grep -q 'nested.rs' <<<"$default_out"; then
  echo "FAIL: default scan RECURSED into a tests/ subtree (must be top-level only, #2902/#2705)"
  echo "$default_out"
  exit 1
fi
echo "OK: default scan covers top-level tests/*.rs and does not recurse into subtrees"

# Positive containment (finding #5): every reported offender must live UNDER the
# fake repo — i.e. the copied guard derived REPO_ROOT from its OWN location, not
# from the real checkout. A REPO_ROOT-derivation regression that scanned the real
# tree would surface paths escaping $fakeroot via `../` (or an absolute path),
# which we reject; each reported path must also resolve to a real file under it.
offenders="$(grep -E '^  [^ ].*:[0-9]+:' <<<"$default_out" || true)"
if [ -z "$offenders" ]; then
  echo "FAIL: containment check found no offender lines to verify (output format changed?)"
  echo "$default_out"
  exit 1
fi
while IFS= read -r offender_line; do
  [ -n "$offender_line" ] || continue
  offender_path="${offender_line#  }"
  offender_path="${offender_path%%:*}"
  case "$offender_path" in
    /* | *../*)
      echo "FAIL: guard reported an offender OUTSIDE the fake repo: '$offender_path' (REPO_ROOT derivation regressed to the real checkout)"
      exit 1
      ;;
  esac
  if [ ! -f "$fakeroot/$offender_path" ]; then
    echo "FAIL: guard-reported offender '$offender_path' does not resolve under \$fakeroot (containment broken)"
    exit 1
  fi
done <<<"$offenders"
echo "OK: every reported offender is contained under the fake repo (REPO_ROOT derived locally)"

# Removing the top-level offender makes the default scan clean even though the deep
# (unscanned) subtree violation remains — proving the non-recursive boundary.
rm -f "$fakeroot/tests/planted.rs"
if ! bash "$fake_guard" >/dev/null 2>&1; then
  echo "FAIL: default scan still failed after removing the top-level offender (a deep subtree file was scanned?)"
  bash "$fake_guard" || true
  exit 1
fi
echo "OK: default scan is clean once the top-level offender is removed (subtree stays deferred)"

# 4. the real, retired correctness test path must be clean.
if ! bash "$GUARD" >/dev/null 2>&1; then
  echo "FAIL: the real correctness test path still contains a wall-clock threshold assert"
  bash "$GUARD" || true
  exit 1
fi
echo "OK: real correctness test path is clean"

echo "PASS: check-no-wallclock-asserts self-test"
