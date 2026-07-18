#!/usr/bin/env bash
# check-no-wallclock-asserts.sh — the #2369-rule guard (issue #2642).
#
# It FAILs if a wall-clock THRESHOLD assert is (re)introduced into the default
# correctness test path — i.e. an `assert!(<measured elapsed> < <threshold>)`
# that runs under a plain `cargo test`. Wall-clock latency depends on the host,
# contention, and CI load, so such an assert is a latent flake: green locally,
# red on a busy runner, and it can never distinguish a real regression from a
# scheduling hiccup. Timings belong in `[perf-record]` log lines (record, do not
# assert) or a dedicated, host-controlled perf gate — never `cargo test`.
#
# What counts as a violation: an `assert!/assert_eq!/assert_ne!` invocation whose
# body references a time measurement (`.elapsed()`, `.as_millis()`, `.as_secs`,
# `.as_micros()`, `.as_nanos()`, or an identifier `elapsed`/`duration`) AND
# contains a `<` or `>` comparison (a threshold check).
#
# Escape hatch (deliberate, reviewer-visible): put `perf-gate-allow` in the
# assert body or in a comment within the assert span / two lines above it. It is
# for the two cases where a wall-clock bound is NOT an incidental latency budget:
#   1. the assert lives in an `#[ignore]`d opt-in perf lane (out of the default
#      gate), or
#   2. a deadline/timeout FEATURE is the property under test — verifying "returns
#      within budget instead of hanging" REQUIRES a time bound; keep the ceiling
#      generously slacked (>= ~10x the budget) so it is load-immune.
# Every use must carry a one-line rationale. Abusing it to smuggle a "should be
# fast" budget back into the gate reintroduces exactly the flake this guard stops.
#
# SKIP-aware, modelled on the other agent-gate guard scripts: no python3 -> SKIP
# (loud, never a silent PASS), so it is safe to wire into the gate on a stripped
# runner. Deterministic and fail-closed: any matching assert in a scanned file
# exits non-zero and names the offender.
#
# Usage:
#   scripts/tests/check-no-wallclock-asserts.sh [PATH...]
# With no args it scans the default correctness test path (the rust core + CLI
# integration tests). Explicit PATH args (files or dirs) override the roots —
# used by the self-test to point the scanner at a planted fixture.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (needed to scan for wall-clock asserts)"
  exit 0
fi

# Default scan roots: the rust correctness test path retired by #2642. Only
# existing paths are passed on (a stripped checkout may lack one).
declare -a ROOTS=()
if [ "$#" -gt 0 ]; then
  ROOTS=("$@")
else
  # KNOWN COVERAGE BOUNDARY (#2642 review): the automated default scan covers the
  # tests/ trees only. `#[cfg(test)]` inline modules under src/ ALSO compile and run
  # in the default correctness gate (`cargo test --package <pkg>`), so a wall-clock
  # assert there flakes the same way — but a full src/ sweep surfaces a broader set of
  # pre-existing latency asserts across unrelated modules (value_fmt, collection
  # validation, block_io_retry, async_bridge, benchmarks/cassandra5) and false-positives
  # on the CQL type name `duration`, both of which are out of this issue's enumerated
  # scope. That sweep + a tightened time-typed regex is deferred to a follow-up. Scan
  # src/ explicitly by passing the path as an argument.
  for p in \
    "$REPO_ROOT/cqlite-core/tests" \
    "$REPO_ROOT/cqlite-cli/tests"; do
    [ -e "$p" ] && ROOTS+=("$p")
  done
fi

if [ "${#ROOTS[@]}" -eq 0 ]; then
  echo "SKIP: no scan roots present (not a full checkout)"
  exit 0
fi

python3 - "${ROOTS[@]}" <<'PY'
import os, re, sys

roots = sys.argv[1:]

# assert!/assert_eq!/assert_ne! statement: from the macro name to the terminating
# ';'. Non-greedy + DOTALL so multi-line asserts are captured whole. Format
# strings containing a literal ';' are vanishingly rare in this codebase, so
# stopping at the first ';' is a safe, dependency-free approximation.
ASSERT = re.compile(r'assert(?:_eq|_ne)?!\s*\(.*?;', re.DOTALL)
TIME_TOKEN = re.compile(r'\.elapsed\(\)|\.as_millis|\.as_secs|\.as_micros|\.as_nanos|\belapsed\b|\bduration\b')
COMPARE = re.compile(r'[<>]')
ALLOW = 'perf-gate-allow'

def scrub_comments(text):
    """Replace `//`/`//!` line-comment and `/* */` block-comment bodies with
    spaces, preserving newlines and byte offsets so line numbers computed off the
    scrubbed text still match the original. This stops a `//` reviewer note that
    merely QUOTES `assert!(elapsed < N)` from being flagged as a real assert.
    String/char literals are not tracked (a `//` inside a string is treated as a
    comment) — acceptable here: no scanned assert relies on a `//` inside a string
    literal, and false-scrub only risks a missed match, never a false positive."""
    out = list(text)
    i, n = 0, len(text)
    while i < n:
        two = text[i:i + 2]
        if two == '//':
            j = i
            while j < n and text[j] != '\n':
                if text[j] != '\n':
                    out[j] = ' '
                j += 1
            i = j
        elif two == '/*':
            j = i
            while j < n and text[j:j + 2] != '*/':
                if text[j] != '\n':
                    out[j] = ' '
                j += 1
            # blank the closing */ too (if present)
            if j < n:
                out[j] = ' '
                if j + 1 < n and text[j + 1] != '\n':
                    out[j + 1] = ' '
                j += 2
            i = j
        else:
            i += 1
    return ''.join(out)

def rs_files(root):
    if os.path.isfile(root):
        if root.endswith('.rs'):
            yield root
        return
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            if f.endswith('.rs'):
                yield os.path.join(dirpath, f)

violations = []
for root in roots:
    for path in rs_files(root):
        try:
            text = open(path, encoding='utf-8').read()
        except OSError:
            continue
        raw_lines = text.split('\n')
        scrubbed = scrub_comments(text)
        for m in ASSERT.finditer(scrubbed):
            body = m.group(0)
            if not TIME_TOKEN.search(body):
                continue
            if not COMPARE.search(body):
                continue
            start_line = scrubbed.count('\n', 0, m.start()) + 1
            end_line = scrubbed.count('\n', 0, m.end()) + 1
            # allow marker anywhere in the assert span itself...
            span = raw_lines[start_line - 1:end_line]
            # ...or in the contiguous `//` comment block directly above the assert
            # (the scrub blanks comment bodies, so read markers from raw_lines).
            k = start_line - 2  # 0-indexed line directly above
            while k >= 0 and raw_lines[k].lstrip().startswith('//'):
                span.append(raw_lines[k])
                k -= 1
            if any(ALLOW in ln for ln in span):
                continue
            snippet = ' '.join(body.split())
            if len(snippet) > 140:
                snippet = snippet[:137] + '...'
            violations.append((path, start_line, snippet))

if violations:
    print("FAIL: wall-clock threshold assert(s) in the default correctness test path")
    print("      (#2369 rule / issue #2642). Record timings via `[perf-record]`")
    print("      log lines, or move behind an #[ignore] opt-in perf gate.")
    print("      Deliberate & #[ignore]d? mark the assert `perf-gate-allow`.")
    for path, line_no, snippet in violations:
        rel = os.path.relpath(path)
        print(f"  {rel}:{line_no}: {snippet}")
    sys.exit(1)

print("OK: no wall-clock threshold asserts in the scanned correctness test path")
PY
