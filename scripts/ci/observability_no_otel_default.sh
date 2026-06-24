#!/usr/bin/env bash
# Observability dependency-isolation guard (epic #1031, issue #1043).
#
# Asserts that the DEFAULT cqlite-core build links NO opentelemetry crates, and
# that the OTel stack appears ONLY under `--features observability`. This is the
# enforcement arm of the "zero-cost when off" contract: the helpers in
# `crate::observability` compile to no-ops when the feature is off, and the
# dependency surface must prove it.
#
# Run from the repo root:
#   scripts/ci/observability_no_otel_default.sh
#
# Exits non-zero (and prints the offending lines) if the default build pulls in
# any opentelemetry crate, or if the observability build does NOT.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

fail=0

echo "== cargo tree: DEFAULT build (must contain NO opentelemetry) =="
default_hits="$(cargo tree -p cqlite-core -e features 2>/dev/null | grep -i opentelemetry || true)"
if [[ -n "$default_hits" ]]; then
  echo "FAIL: default cqlite-core build links opentelemetry crates:"
  echo "$default_hits"
  fail=1
else
  echo "OK: default build links no opentelemetry crates."
fi

echo
echo "== cargo tree: --features observability (must contain opentelemetry) =="
obs_hits="$(cargo tree -p cqlite-core --features observability -e features 2>/dev/null | grep -i 'opentelemetry v' || true)"
if [[ -z "$obs_hits" ]]; then
  echo "FAIL: --features observability build does NOT link opentelemetry — wiring broken."
  fail=1
else
  echo "OK: observability build links the OTel stack:"
  echo "$obs_hits" | sed 's/^/    /' | sort -u | head
fi

exit "$fail"
