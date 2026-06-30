#!/usr/bin/env bash
# test_generator_keyspace_scoping.sh — lint guard for issue #1232.
#
# The test-data fixture generators MUST enumerate their own keyspace's SSTables
# by rooting `find` at the per-keyspace directory ("$SSTABLES_DIR/$KEYSPACE" /
# "$sstables_dir/$KEYSPACE"), NOT by enumerating the WHOLE corpus and filtering
# with a substring match such as `find "$SSTABLES_DIR" ... | grep -z "$KEYSPACE"`.
#
# A substring filter silently breaks the moment a prefix-colliding keyspace is
# committed (e.g. `test_comp` also matches `test_compactionparity`): the
# generator would then walk and dump foreign keyspaces' SSTables.
#
# This guard fails if any generate-*.sh under test-data/scripts/ reintroduces a
# whole-corpus substring enumeration pattern.
#
# Backs: issue #1232.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GEN_DIR="$REPO_ROOT/test-data/scripts"

fail=0

# Anti-pattern: piping a find into `grep -z "$KEYSPACE"` (NUL-delimited substring
# filter over the whole corpus). The correct idiom roots find at the keyspace dir.
while IFS= read -r script; do
  if grep -nE 'grep[[:space:]]+-z[[:space:]]+"\$KEYSPACE"' "$script" >/dev/null 2>&1; then
    echo "[lint][FAIL] $script reintroduces whole-corpus substring enumeration:" >&2
    grep -nE 'grep[[:space:]]+-z[[:space:]]+"\$KEYSPACE"' "$script" >&2
    echo "  Use a path-scoped find rooted at the keyspace dir instead, e.g.:" >&2
    echo "    find \"\$SSTABLES_DIR/\$KEYSPACE\" -type f -name '*-Data.db' -not -name '._*' -print0" >&2
    fail=1
  fi
done < <(find "$GEN_DIR" -maxdepth 1 -type f -name 'generate-*.sh' -print 2>/dev/null | sort)

if [[ "$fail" -ne 0 ]]; then
  echo "[lint] generator keyspace-scoping guard FAILED (issue #1232)." >&2
  exit 1
fi

echo "[lint] generator keyspace-scoping guard PASSED: no whole-corpus substring enumeration."
