#!/usr/bin/env bash
# test_udt_rowbuilder_tuple_shape.sh — regression lint guard for issue #1991.
#
# The nb row-builder in test-data/scripts/regenerate-datasets.sh inserts UDT
# values through PREPARED statements. cassandra-driver serializes a UDT bound to
# a prepared statement POSITIONALLY (UserType.serialize_safe reads `val[i]`), so
# a UDT value MUST be a tuple/sequence in declared field order. A dict raises
# `KeyError: 0` on the first field, every row is skipped, 0 rows are inserted and
# the whole exhaustive regeneration aborts (the exact 2026-07 field failure on
# `collections_with_udts`). This is driver-version-independent.
#
# This guard runs WITHOUT Docker or a live Cassandra so a regression (someone
# refactors build_udt_value back to a dict, or a driver bump reintroduces the
# shape mismatch by copy-paste) is caught in the fast tooling-tests lane, not
# weeks later in the weekly cron.
#
# It asserts, by static inspection of build_udt_value():
#   1. it returns a positional `tuple(...)`, and
#   2. it does NOT build a per-field dict for the UDT value.
# It also asserts the per-table zero-row abort is ACTIONABLE: it captures the
# first failing row's traceback (traceback.format_exc()) and prints it on abort.
#
# Backs: issue #1991.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GEN="$REPO_ROOT/test-data/scripts/regenerate-datasets.sh"

fail=0

if [[ ! -f "$GEN" ]]; then
  echo "[lint][FAIL] generator not found: $GEN" >&2
  exit 1
fi

# --- Extract the body of the build_udt_value() Python function ---------------
# From the `def build_udt_value` line to the next top-level `def ` at the same
# (4-space) indentation. Keeps the assertions robust to line-number drift.
udt_fn="$(
  awk '
    /^    def build_udt_value\(/ { grab=1; print; next }
    grab && /^    def [A-Za-z_]+\(/ { exit }
    grab { print }
  ' "$GEN"
)"

if [[ -z "$udt_fn" ]]; then
  echo "[lint][FAIL] could not locate build_udt_value() in $GEN" >&2
  exit 1
fi

# 1. Must return a positional tuple.
if ! grep -Eq 'return[[:space:]]+tuple\(' <<<"$udt_fn"; then
  echo "[lint][FAIL] build_udt_value() must return a positional 'tuple(...)' of" >&2
  echo "  UDT field values in declared order (issue #1991). Prepared-statement" >&2
  echo "  UDT serialization reads fields positionally (val[i]); a dict raises" >&2
  echo "  KeyError: 0 and skips every row." >&2
  fail=1
fi

# 2. Must NOT build a per-field dict for the UDT value (the reintroduced bug):
#    a `result[<name>] = ...` assignment or a `{<key>: sample_val(...)}` /
#    `{... : ...}` dict-comprehension over the fields.
if grep -Eq 'result\[[^]]+\][[:space:]]*=' <<<"$udt_fn" \
   || grep -Eq '\{[^}]*:[^}]*sample_val' <<<"$udt_fn"; then
  echo "[lint][FAIL] build_udt_value() builds a DICT for the UDT value — this is" >&2
  echo "  the issue #1991 regression (KeyError: 0 under prepared inserts). Return" >&2
  echo "  a positional tuple in declared field order instead." >&2
  fail=1
fi

# --- Assert the per-table zero-row abort is ACTIONABLE (issue #1991) ----------
# It must capture the first failure traceback and reference it on the 0-row
# abort, so CI logs alone are enough to diagnose a future breakage.
if ! grep -q 'traceback.format_exc()' "$GEN"; then
  echo "[lint][FAIL] the row-builder must capture the first failing row's" >&2
  echo "  traceback.format_exc() so the 0-row abort is actionable (issue #1991)." >&2
  fail=1
fi
if ! grep -Eq '0 rows inserted.*aborting' "$GEN"; then
  echo "[lint][FAIL] the fail-closed 0-row abort message (naming the table) is" >&2
  echo "  missing from the row-builder (issue #1991)." >&2
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "[lint] UDT row-builder tuple-shape guard FAILED (issue #1991)." >&2
  exit 1
fi

echo "[lint] UDT row-builder tuple-shape guard PASSED: build_udt_value() returns a positional tuple; 0-row abort is actionable."
