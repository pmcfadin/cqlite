#!/usr/bin/env bash
# Run `cargo test --no-fail-fast <args>` and fail unless at least one test PASSED
# (issue #4420). A suite that compiles but runs 0 tests exits 0 under cargo, so
# the exit status alone cannot tell a green suite from an unmeasured one.
# Usage: run-counted-tests.sh <label> -- <cargo test args...>
set -euo pipefail

label="$1"; shift
[ "${1:-}" = "--" ] && shift

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

rc=0
cargo test --no-fail-fast "$@" 2>&1 | tee "$log" || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "::error::${label}: cargo test failed (exit ${rc})" >&2
  exit "$rc"
fi

# Sum the `passed` counts over every `test result:` line (one per test binary).
passed="$(grep -E '^test result: ok\.' "$log" | sed -E 's/.* ([0-9]+) passed.*/\1/' | awk '{s+=$1} END {print s+0}')"
passed="${passed:-0}"
if [ "$passed" -eq 0 ]; then
  echo "::error::${label}: ran 0 tests; an unmeasured suite is not a passing one" >&2
  exit 1
fi
echo "${label}: ${passed} tests passed"
