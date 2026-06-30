#!/usr/bin/env bash
# test_generator_keyspace_scoping.sh — lint guard for issue #1232.
#
# The test-data fixture generators MUST enumerate their own keyspace's SSTables
# by rooting `find` at the per-keyspace directory ("$SSTABLES_DIR/$KEYSPACE" /
# "$sstables_dir/$KEYSPACE" / "$sstables_dir/test_x"), NOT by enumerating the
# WHOLE corpus and filtering with a NUL-delimited substring match such as
# `find "$SSTABLES_DIR" ... | grep -z "$KEYSPACE"` (or a literal-quoted form,
# e.g. `grep -z 'test_deltas'`).
#
# A substring filter silently breaks the moment a prefix-colliding keyspace is
# committed (e.g. `test_comp` also matches `test_compactionparity`): the
# generator would then walk and dump foreign keyspaces' SSTables.
#
# This guard fails if any generate-*.sh under test-data/scripts/ reintroduces a
# whole-corpus substring enumeration pattern, in either the variable form
# (grep -z "$KEYSPACE") or the literal form (grep -z 'test_x' / "test_x").
# It does NOT flag the legitimate path-scoped idiom `find "$dir/$KEYSPACE" ...`.
#
# Backs: issue #1232.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GEN_DIR="$REPO_ROOT/test-data/scripts"

fail=0

# Anti-pattern (two parts, on the same OR adjacent line because the pipe is often
# wrapped): a `find` rooted at the WHOLE corpus directory — `find "$sstables_dir"`
# or `find "$SSTABLES_DIR"` with NO `/...` path scope after the variable — piped
# into a NUL-delimited keyspace substring filter `grep -z <keyspace>`, where the
# keyspace is a variable ("$KEYSPACE") or a literal ('test_x' / "test_x").
#
# We detect it by joining each script into a single logical stream (so a wrapped
# pipe matches) and looking for the bare-root find immediately feeding a `grep -z`.
#
# - Whole-corpus root, NOT path-scoped:  find "$sstables_dir"   (followed by space,
#   not by `/`).  The correct idiom `find "$sstables_dir/$KEYSPACE"` has a `/`
#   right after the closing quote and is therefore NOT matched.
# - keyspace grep -z filter:  grep -z "$KEYSPACE" | grep -z 'test_x' | grep -z "test_x"
WHOLE_CORPUS_FIND='find[[:space:]]+"\$[Ss][Ss][Tt][Aa][Bb][Ll][Ee][Ss]_[Dd][Ii][Rr]"[[:space:]]'
GREP_Z_KEYSPACE="grep[[:space:]]+-z[[:space:]]+(\"\\\$[A-Za-z_]+\"|'[A-Za-z0-9_]+'|\"[A-Za-z0-9_]+\")"

while IFS= read -r script; do
  # Flatten line-continuations and pipes so a wrapped `find ... \<newline>| grep -z`
  # is matched as one logical command; then scan for the anti-pattern.
  matches="$(
    tr '\n' ' ' < "$script" \
      | grep -oE "${WHOLE_CORPUS_FIND}[^|]*\|[[:space:]]*${GREP_Z_KEYSPACE}" 2>/dev/null || true
  )"
  if [[ -n "$matches" ]]; then
    echo "[lint][FAIL] $script reintroduces whole-corpus substring enumeration:" >&2
    printf '  %s\n' "$matches" >&2
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
