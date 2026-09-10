#!/usr/bin/env bash
#
# Issue #4196 (spec R4.3) — `cqlite salvage` locates partitions ONLY from an
# authoritative boundary source (`Index.db` / the `Partitions.db` trie), and
# NEVER by scanning `Data.db` bytes for a plausible header. This guard greps
# `cqlite-core/src/storage/write_engine/salvage/` for any byte-pattern search
# primitive OUTSIDE test code and FAILs naming the line if one is found.
#
# Registered in the gate's `tooling-tests` component (no cargo/network needed —
# a pure grep, so it always runs).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SALVAGE_DIR="$REPO_ROOT/cqlite-core/src/storage/write_engine/salvage"

if [ ! -d "$SALVAGE_DIR" ]; then
  echo "FAIL - salvage module directory not found: $SALVAGE_DIR"
  exit 1
fi

# Literal byte-pattern search primitives R4.3 forbids in production salvage
# code — the exact "resync by scanning for a plausible header" shape #3928
# measured as inventing partitions out of misaligned bytes.
PATTERNS=("memchr" ".windows(" "find(|" "position(|")

hits=0
while IFS= read -r -d '' file; do
  # Exclude test code: any file under a `tests/` path component (unit tests
  # may legitimately use these primitives to build/verify fixtures).
  case "$file" in
    */tests/*) continue ;;
  esac
  # Track whether the current line is inside a `#[cfg(test)]` module via
  # brace depth from the attribute onward — a lightweight substitute for a
  # real Rust parser, sufficient for excluding an in-file test module.
  #
  # Two hardenings (roborev, issue #4196) against the naive form: (a) the
  # attribute match is ANCHORED to a line that, with whitespace stripped, is
  # EXACTLY `#[cfg(test)]` — a bare substring match would also fire inside a
  # doc comment or string literal mentioning the attribute, silently
  # disabling the guard for the rest of the file (this codebase's module
  # docs routinely quote cfg attributes); (b) if the brace tracker never
  # returns to depth 0 by EOF (an unbalanced/miscounted file, e.g. a brace
  # inside a string or comment), that is a REFUSAL (FAIL), not a silent
  # skip — a guard whose whole job is fail-closed detection must not have a
  # counting failure read as "nothing to report".
  in_test_mod=0
  depth=0
  line_no=0
  while IFS= read -r line; do
    line_no=$((line_no + 1))
    if [ "$in_test_mod" -eq 1 ]; then
      opens=$(grep -o '{' <<<"$line" | wc -l)
      closes=$(grep -o '}' <<<"$line" | wc -l)
      depth=$((depth + opens - closes))
      if [ "$depth" -le 0 ]; then
        in_test_mod=0
      fi
      continue
    fi
    stripped="${line// /}"
    if [ "$stripped" = '#[cfg(test)]' ]; then
      in_test_mod=1
      depth=0
      continue
    fi
    for pat in "${PATTERNS[@]}"; do
      if [[ "$line" == *"$pat"* ]]; then
        echo "FAIL - byte-pattern search primitive '$pat' outside tests: $file:$line_no: $line"
        hits=$((hits + 1))
      fi
    done
  done <"$file"
  if [ "$in_test_mod" -eq 1 ]; then
    echo "FAIL - $file: brace tracker for a #[cfg(test)] block never returned to depth 0 by EOF \
(unbalanced or miscounted braces) — refusing rather than silently trusting the scan"
    hits=$((hits + 1))
  fi
done < <(find "$SALVAGE_DIR" -name '*.rs' -print0)

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found in $SALVAGE_DIR outside tests (spec R4.3)"
  exit 1
fi

echo "ok   - no byte-pattern search primitive in $SALVAGE_DIR outside tests (spec R4.3)"
exit 0
