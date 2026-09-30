#!/usr/bin/env bash
#
# Issue #4194 (corruption-locator, spec verify-location L3) — partition
# location resolution reads ONLY the boundary source's own decoded entries
# (`Index.db`'s `PartitionIndexEntry`s, the BTI trie's decoded leaves) and the
# `CompressionInfo.db`/`CRC.db` chunk tables — NEVER by scanning `Data.db`
# bytes for a plausible partition header. This guard greps
# `cqlite-core/src/storage/sstable/verify_location.rs` for any byte-pattern
# search primitive OUTSIDE test code and FAILs naming the line if one is
# found. Modeled on `sstable-salvage`'s `test_salvage_no_resync_scan.sh`
# (issue #4196, spec R4.3), single-file rather than directory-wide.
#
# SCOPE, both declared explicitly (roborev round-1 LOW finding):
# - This guard scans ONLY `verify_location.rs`, not `verify.rs` — the
#   partition-resolution LOGIC (what this spec item governs) lives entirely
#   in `verify_location.rs`; `verify.rs`'s new code (`PendingLocation`,
#   `finalize_locations`) computes byte RANGES from already-decoded
#   `CompressionInfo`/chunk-index/offset fields, never scans `Data.db` bytes,
#   and carries none of the forbidden patterns either (verified by eye at
#   review time) — but is out of this guard's MECHANICAL reach.
# - The `#[cfg(test)]` exclusion skips from the file's FIRST such marker to
#   EOF wholesale, not via brace-counting: `verify_location.rs` has exactly
#   ONE top-level `#[cfg(test)] mod tests { .. }`, at the end of the file
#   (verified at write time), so "skip the tail from the marker" is exact
#   for this file and immune to the brace-counting fragility a `format!`
#   string containing a literal `{{`/`}}` could otherwise trip (the
#   salvage guard's directory-wide brace tracker exists because THAT guard
#   must tolerate a `#[cfg(test)]` block appearing mid-file, in more than
#   one file). If a second `#[cfg(test)]` block is ever added ABOVE the
#   trailing one, this guard would incorrectly treat everything from the
#   FIRST such marker onward as test code — re-adopt the brace-counting form
#   (or split the file) if that ever happens.
#
# WIRING, and why it is not where the salvage guard is: registered in the
# gate's UNSCOPED `roborev-lints` component (`run_roborev_lints_cmd` in
# `scripts/agent-gate.sh`), which runs on every `--lite` round and in the full
# gate regardless of what the diff touches. It is deliberately NOT in
# `tooling-tests`: #4266 made that component diff-scoped
# (`scripts/lib/tooling-tests-scope.sh`) and `cqlite-core/**` is not in its
# declared harness-path set, so a core-only diff — precisely the diff that can
# reintroduce a byte scan into `verify_location.rs` — would SKIP this guard.
# No cargo/python3/network needed (a pure grep over one committed file), which
# is what makes it safe for the fast loop.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TARGET="$REPO_ROOT/cqlite-core/src/storage/sstable/verify_location.rs"

if [ ! -f "$TARGET" ]; then
  echo "FAIL - target file not found: $TARGET"
  exit 1
fi

# Literal byte-pattern search primitives L3 forbids in production location
# code — the exact "resync by scanning for a plausible header" shape
# `sstable-salvage`'s R4.3 guard exists to catch, reused here verbatim.
PATTERNS=("memchr" ".windows(" "find(|" "position(|")

hits=0
allowed=0
line_no=0
in_test_tail=0
while IFS= read -r line; do
  line_no=$((line_no + 1))
  if [ "$in_test_tail" -eq 1 ]; then
    continue
  fi
  stripped="${line// /}"
  stripped="${stripped//$'\t'/}"
  if [ "$stripped" = '#[cfg(test)]' ]; then
    in_test_tail=1
    continue
  fi
  # LINE-LEVEL OPT-OUT (roborev job 102 LOW). Without one this guard was
  # purely lexical with no sanctioned override, unlike its siblings
  # (`injection-lint-allow`, `perf-gate-allow`) -- and it had already DISTORTED
  # production code: `resolve_partitions`'s sortedness debug_assert! was
  # written as `zip(iter().skip(1))`, with a comment apologising for avoiding
  # the clearer `windows(2)`, over a slice of boundary-entry tuples that has
  # nothing to do with `Data.db` bytes. A guard that constrains SYNTAX rather
  # than behaviour needs an escape hatch, or the next legitimate
  # `.position(|e| ..)` over that same slice FAILs --lite with no way out
  # (this guard is wired into the UNSCOPED roborev-lints component).
  #
  # The marker is deliberately narrow: same line, named, and it does not
  # suppress the census -- an allowed line is still COUNTED and reported, so
  # the opt-out is visible rather than silent.
  if [[ "$line" == *"no-resync-scan-allow"* ]]; then
    allowed=$((allowed + 1))
    continue
  fi
  for pat in "${PATTERNS[@]}"; do
    if [[ "$line" == *"$pat"* ]]; then
      echo "FAIL - byte-pattern search primitive '$pat' outside tests: $TARGET:$line_no: $line"
      hits=$((hits + 1))
    fi
  done
done <"$TARGET"

if [ "$in_test_tail" -eq 0 ]; then
  echo "FAIL - $TARGET: no '#[cfg(test)]' marker found — this guard's exact-tail-skip scope \
assumes exactly one, at the end of the file; refusing rather than silently scanning either \
too much or too little"
  exit 1
fi

if [ "$hits" -gt 0 ]; then
  echo "FAIL - $hits byte-pattern-search hit(s) found in $TARGET outside tests (spec L3)"
  exit 1
fi

echo "ok   - $TARGET scanned, 0 byte-pattern-search hit(s) RECOGNISED outside tests, $allowed line(s) RECOGNISED as no-resync-scan-allow (spec L3)"
exit 0
