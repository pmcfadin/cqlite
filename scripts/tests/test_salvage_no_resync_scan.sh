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
#
# SCOPE (roborev, issue #4196, round-12 Low finding): this guard scans ONLY
# `cqlite-core/src/storage/write_engine/salvage/` — the actual byte-touching
# decode primitive salvage's per-partition loop calls,
# `decode_partition_at_offset_for_salvage`
# (`reader/data_access/point_compaction.rs`), is OUTSIDE that directory and
# therefore OUTSIDE this guard's reach. DELIBERATE, not an oversight:
# `point_compaction.rs` is a SHARED point-read file with many OTHER
# functions and their own legitimate byte-pattern-search uses (unrelated to
# salvage's R4.3 "never resync by scanning" mandate) — widening the scan to
# the WHOLE file risks false positives against code this guard has no
# business flagging, and `decode_partition_at_offset_for_salvage` itself
# reads via `read_exact_at`/`pull_chunk_window`'s bounded, offset-driven
# reads (never a byte-pattern SEARCH) per its own construction, so nothing
# in it would trip these patterns regardless. Declared explicitly here
# rather than silently assumed.
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
# roborev, issue #4196, round-14 Low finding: count SCANNED (non-excluded)
# files and require at least one — without this, a guard that matched
# `find ... -name '*.rs'` against a directory holding only excluded
# (`*/tests/*`) files, or none at all (a future reorganization leaving only
# a re-exporting mod.rs under a moved-out production tree), printed the
# SAME "ok" a genuinely swept-and-clean run does. A merge-blocking gate
# component that scanned nothing must not read identically to one that
# scanned everything and found nothing (the affirmative-zero property this
# change already enforces everywhere else — `losses: 0 RECOGNISED`,
# `component findings: 0 RECOGNISED`).
scanned=0
while IFS= read -r -d '' file; do
  # Exclude test code: any file under a `tests/` path component (unit tests
  # may legitimately use these primitives to build/verify fixtures).
  case "$file" in
    */tests/*) continue ;;
  esac
  scanned=$((scanned + 1))
  # Track whether the current line is inside a `#[cfg(test)]` module via
  # brace depth from the attribute onward — a lightweight substitute for a
  # real Rust parser, sufficient for excluding an in-file test module.
  #
  # Three hardenings (roborev, issue #4196) against the naive form: (a) the
  # attribute match is ANCHORED to a line that, with whitespace stripped, is
  # EXACTLY `#[cfg(test)]` — a bare substring match would also fire inside a
  # doc comment or string literal mentioning the attribute, silently
  # disabling the guard for the rest of the file (this codebase's module
  # docs routinely quote cfg attributes); (b) if the brace tracker never
  # returns to depth 0 by EOF (an unbalanced/miscounted file, e.g. a brace
  # inside a string or comment), that is a REFUSAL (FAIL), not a silent
  # skip — a guard whose whole job is fail-closed detection must not have a
  # counting failure read as "nothing to report"; (c) (round 21) the
  # ANCHOR line and the module's OPENING BRACE are not always the SAME
  # line — a blank line, a second attribute (`#[allow(dead_code)]`), a
  # comment, or `mod tests` with its `{` on the NEXT line all carry no net
  # `{` themselves. The naive tracker cleared `in_test_mod` the instant
  # `depth <= 0`, which is TRUE before the opening brace is ever seen (0
  # opens, 0 closes, depth stays 0) — dropping out of test mode
  # immediately and scanning the whole module as production code. Track
  # `seen_open` and only clear `in_test_mod` once depth has gone POSITIVE
  # at least once AND returned to 0.
  in_test_mod=0
  seen_open=0
  depth=0
  line_no=0
  while IFS= read -r line; do
    line_no=$((line_no + 1))
    if [ "$in_test_mod" -eq 1 ]; then
      opens=$(grep -o '{' <<<"$line" | wc -l)
      closes=$(grep -o '}' <<<"$line" | wc -l)
      depth=$((depth + opens - closes))
      if [ "$depth" -gt 0 ]; then
        seen_open=1
      fi
      if [ "$seen_open" -eq 1 ] && [ "$depth" -le 0 ]; then
        in_test_mod=0
      # (round 21) a BRACE-LESS `#[cfg(test)]`-gated item (a `const`, `use`,
      # or `type` alias, never a `mod` block) ends at its own `;` with NO
      # `{` ever appearing — `seen_open` would otherwise never become 1,
      # leaving the tracker stuck until EOF and firing the unbalanced-
      # braces refusal on a perfectly well-formed file. Only reachable
      # while still on the line immediately following the attribute
      # (`depth == 0`, nothing opened yet) — a `;` deep inside an
      # already-open block (e.g. a statement in `mod tests { .. }`) must
      # NOT close tracking early, so this arm is gated on `seen_open == 0`.
      elif [ "$seen_open" -eq 0 ] && [[ "$line" == *';'* ]]; then
        in_test_mod=0
      fi
      continue
    fi
    # (round 21) strip tabs as well as spaces — a tab-indented
    # `#[cfg(test)]` never matched the space-only strip below.
    stripped="${line// /}"
    stripped="${stripped//$'\t'/}"
    if [ "$stripped" = '#[cfg(test)]' ]; then
      in_test_mod=1
      seen_open=0
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

if [ "$scanned" -eq 0 ]; then
  echo "FAIL - 0 production .rs file(s) scanned under $SALVAGE_DIR (every *.rs found was under \
a */tests/* path, or none exist) — a guard that scanned nothing cannot certify a 'no \
byte-pattern search primitive' verdict; this is a REFUSAL, not a pass"
  exit 1
fi

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found in $SALVAGE_DIR outside tests (spec R4.3)"
  exit 1
fi

echo "ok   - $scanned production file(s) scanned, 0 byte-pattern-search hit(s) RECOGNISED in \
$SALVAGE_DIR outside tests (spec R4.3)"
exit 0
