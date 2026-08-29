#!/usr/bin/env bash
# check-cargo-output-parsers.sh — the colour-immune-at-the-parse-site guard (issue #3400).
#
# It FAILs if a cargo-output parse site in the gate reads from a source that is NOT
# ANSI-stripped. The defect it exists for was live and silent on main for months:
#
#   `check_no_unexpected_zero_tests` (the #2039 zero-tests guard) keyed on the literal
#   text `Running tests/`. Under `CARGO_TERM_COLOR=always` cargo writes
#
#       ESC[1mESC[92m     RunningESC[0m tests/empty.rs (target/debug/deps/empty-…)
#
#   i.e. the RESET LANDS BETWEEN THE STATUS WORD AND THE PAYLOAD, so the literal
#   `Running tests/` never appears. No target was ever associated with a result, the
#   `0 passed` branch's `[ -n "$target" ]` test was always false, and the guard reported
#   OK having judged nothing. MEASURED, both ways, on real cargo output.
#
# TWO measured facts make this a live defect rather than a tty-only curiosity:
#   1. `CARGO_TERM_COLOR=always` SURVIVES redirection to a file (25 ESC bytes in the
#      redirected `always` capture, 0 in the `never` one). The gate's own MANDATED
#      `> gate.log 2>&1` pattern therefore preserves the escapes.
#   2. 18 workflows set `CARGO_TERM_COLOR: always` — including `.github/workflows/gate.yml`,
#      the nightly FULL gate — plus `scripts/local/pre-merge.sh`.
# And one fact that decides the failure DIRECTION per site: `test result:` and
# `running N tests` come from libtest, and cargo does not pass `--color` through to the
# harness, so they are byte-identical under `always` and `never`. Those parses are
# colour-safe TODAY — for a reason that is invisible at the parse site. A parse keyed on
# `warning:` would break (cargo colours it `warningESC[0mESC[1m: …`), which is the
# highest-consequence shape: a warnings-denial that silently stops denying.
#
# ── THE POLICY THIS ENFORCES (issue #3400 AC4) ────────────────────────────────────────
#   1. Parsers are COLOUR-IMMUNE AT THE PARSE SITE. This is the load-bearing rule. A
#      parser whose correctness depends on its caller's environment is "fixed" by
#      inheritance, which is the exact coupling that left the zero-tests guard inert:
#      nothing at the parse said what it depended on, and nothing failed when the
#      dependency was not met.
#   2. `CARGO_TERM_COLOR=never` on parsed invocations is BELT, never the fix. Setting it
#      is welcome; relying on it is not.
#   3. The nightly `gate.yml` KEEPS `CARGO_TERM_COLOR: always`. Colour is a presentation
#      property of a log read by humans; relocating correctness into a workflow file 18
#      files away from the parse is a WORSE coupling than the one being removed.
#
# ── WHAT COUNTS AS A PARSE SITE ───────────────────────────────────────────────────────
# A non-comment line that mentions cargo/libtest output text (`test result:`,
# `Running tests/`, `Running unittests`, `Doc-tests`, `running N tests`, `Compiling`,
# `Finished`, `warning:`, `error:` in a matching context) AND performs a MATCH
# (`==`, `=~`, `case`, `grep`, `sed`, `awk`, `rg`). A bare `echo "… test result: …"`
# message performs no match and is not a parse site.
#
# ── WHAT COUNTS AS AN ANSI-STRIPPED SOURCE ────────────────────────────────────────────
# Exactly three RECOGNISED shapes; anything else is a named FAIL rather than a guess
# (the `check-pub-surface.sh` posture — refuse, never assume):
#   R1  the parse is inside a `while … read` loop whose `done` REDIRECT names a stripped
#       source. A `done` with NO redirect is a FAIL in its own right: a piped
#       `while read` loop runs in a SUBSHELL, so the loop's accumulated verdict variable
#       is discarded on exit and the guard silently PASSES — the identical failure these
#       guards exist to prevent, arriving through their own plumbing (issue #3400 AC3).
#   R2  the parse line references a variable assigned from `_ansi_stripped_log`
#       (`V=$(_ansi_stripped_log …)`), or calls `_ansi_stripped_log` itself.
#   R3  the site carries `cargo-colour-lint-allow` plus a one-line rationale.
#
# ── AN EMPTY SUBJECT SET IS A FAIL, NOT A GREEN ───────────────────────────────────────
# If the scan finds ZERO parse sites, its own pattern has stopped matching (someone
# refactored the parsers out of its reach) and printing `0/0 PASS` would be THE IDENTICAL
# VACUOUS-PASS SHAPE one level up. So zero subjects is a hard FAIL, and success prints an
# AFFIRMATIVE line naming the count actually measured:
#     cargo-output-parsers: N/N parse sites read from an ANSI-stripped source
# (the `schemas:` / `pub-surface:` convention: a pasted summary must show the check RAN).
#
# ── ESCAPE HATCH ──────────────────────────────────────────────────────────────────────
# Put `cargo-colour-lint-allow` in the parse line, or in a comment within TWO lines above
# it, followed by a one-line rationale. A bare placeholder (`why`, `todo`, `tbd`,
# `fixme`) or an unsubstituted `<…>` template is REFUSED as MALFORMED (claim.sh's rule) —
# a marker with nothing recordable in it is not a rationale. The rationale must be at least
# 12 characters after placeholder-stripping — enough to say WHY, not enough to be a word.
#
# ── KNOWN COVERAGE BOUNDARIES, stated rather than implied ──────────────────────────────
#   * Default scan target is `scripts/agent-gate.sh` ONLY. It is the only non-test shell
#     file in the repo that parses cargo output (measured:
#     `grep -rln 'test result:|Running tests/|Running unittests' --include='*.sh'` returns
#     it and one self-test that PLANTS such text as a fixture). Scan another file by
#     passing its path as an argument.
#   * A `#` comment is recognised only as a WHOLE LINE (first non-blank character). Cargo text
#     quoted in an INLINE trailing comment on a line that also performs a match would be read as
#     part of the parse. The consequence is a false FAIL with a visible remedy (move the note to
#     its own line, or mark the site), never a false pass — the direction this guard must err in.
#   * `STRIPPED_VARS` is collected file-globally and is not flow-sensitive: a variable
#     assigned from `_ansi_stripped_log` somewhere and REASSIGNED from a raw source
#     elsewhere would still read as stripped. Bash dataflow analysis is out of scope; the
#     R1/R2/R3-or-FAIL posture is what keeps the guard honest.
#
# SKIP-aware, like the sibling guard scripts: no python3 -> loud `SKIP:`, exit 0, never a
# silent PASS. Deterministic, fail-closed, and it names every offender.
#
# Usage:
#   scripts/ci/check-cargo-output-parsers.sh [PATH...]
# With no args it scans scripts/agent-gate.sh. Explicit PATH args override the scan
# target — used by the self-test to point the scanner at planted fixtures.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (needed to scan cargo-output parse sites)"
  exit 0
fi

declare -a TARGETS=()
if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  TARGETS=("$REPO_ROOT/scripts/agent-gate.sh")
fi

for t in "${TARGETS[@]}"; do
  if [ ! -r "$t" ]; then
    echo "FAIL: scan target '$t' is not readable — a guard that cannot read its subject has"
    echo "      measured nothing and must never report OK (issue #3400)."
    exit 1
  fi
done

python3 - "${TARGETS[@]}" <<'PY'
import os, re, sys

paths = sys.argv[1:]

# Cargo/libtest output text a parser can key on. Deliberately a CURATED list of strings
# that only appear in cargo's own output — not a general "looks like a status word" test.
CARGO_TOKENS = [
    'test result:',
    'Running tests/',
    'Running unittests',
    'Doc-tests',
    'running 0 tests',
    'Compiling ',
    'Finished ',
    'warning:',
    'error[',
]
# A MATCH, as opposed to a mention. `echo "… test result: …"` has no match operator.
MATCH_OP = re.compile(r'==|=~|\bcase\b|\bgrep\b|\bsed\b|\bawk\b|\brg\b')
# V=$(_ansi_stripped_log …)  — the affirmative evidence that V holds a stripped path.
STRIP_ASSIGN = re.compile(r'(?:^|[\s;&|(])([A-Za-z_]\w*)=\"?\$\(\s*_ansi_stripped_log\b')
HELPER = '_ansi_stripped_log'
LOOP_HEADER = re.compile(r'\bwhile\b.*\bread\b')
DONE_LINE = re.compile(r'^\s*done\b')
FUNC_DEF = re.compile(r'^\s*[A-Za-z_]\w*\(\)\s*\{')
CLOSE_BRACE = re.compile(r'^\s*\}\s*$')
ALLOW = 'cargo-colour-lint-allow'
PLACEHOLDER = {'', 'why', 'todo', 'tbd', 'fixme', 'xxx', 'tk', 'n/a'}
UNSUBSTITUTED = re.compile(r'<[^<>]*>')


def is_comment(line):
    s = line.lstrip()
    return s.startswith('#')


def var_refs(text):
    """Every ${VAR} / $VAR name referenced in text."""
    return set(re.findall(r'\$\{?([A-Za-z_]\w*)', text))


def allow_rationale(lines, n):
    """Return (found, rationale_text). The marker may sit on the parse line itself or in
    a comment within the two lines above it."""
    for k in (n, n - 1, n - 2):
        if k < 0 or k >= len(lines):
            continue
        if k != n and not is_comment(lines[k]):
            continue
        idx = lines[k].find(ALLOW)
        if idx < 0:
            continue
        tail = lines[k][idx + len(ALLOW):]
        tail = tail.lstrip(' \t:-–—,.#')
        return True, tail.strip()
    return False, ''


def enclosing_loop_done(lines, n):
    """(in_loop, done_line_index). Walk backward to the nearest structural marker: a
    `while … read` header means we are inside such a loop; a `done`, a function
    definition or a closing brace means we are not."""
    for k in range(n - 1, -1, -1):
        line = lines[k]
        if is_comment(line):
            continue
        if LOOP_HEADER.search(line):
            for j in range(n + 1, len(lines)):
                if DONE_LINE.match(lines[j]):
                    return True, j
            return True, -1
        if DONE_LINE.match(line) or FUNC_DEF.match(line) or CLOSE_BRACE.match(line):
            return False, -1
    return False, -1


def done_redirect(line):
    """Classify a `done …` line's input redirect.
    Returns (kind, expr): kind in {'file', 'herestring', 'procsub', 'none'}."""
    m = re.search(r'done\s*<\s*<\((.*)\)', line)
    if m:
        return 'procsub', m.group(1)
    m = re.search(r'done\s*<<<\s*(.*)$', line)
    if m:
        return 'herestring', m.group(1)
    m = re.search(r'done\s*<\s*(\S+)', line)
    if m:
        return 'file', m.group(1)
    return 'none', ''


violations = []   # (path, lineno, reason, snippet)
sites = 0
allowed = 0

for path in paths:
    try:
        text = open(path, encoding='utf-8').read()
    except OSError as exc:
        violations.append((path, 0, 'unreadable scan target (%s)' % exc, ''))
        continue
    lines = text.split('\n')
    stripped_vars = set(STRIP_ASSIGN.findall(text))

    def source_is_stripped(expr):
        if HELPER in expr:
            return True
        return bool(var_refs(expr) & stripped_vars)

    for n, line in enumerate(lines):
        if is_comment(line):
            continue
        if not any(tok in line for tok in CARGO_TOKENS):
            continue
        if not MATCH_OP.search(line):
            continue
        sites += 1
        lineno = n + 1
        snippet = ' '.join(line.split())
        if len(snippet) > 130:
            snippet = snippet[:127] + '...'

        # R3 — explicit allow, rationale required.
        found, rationale = allow_rationale(lines, n)
        if found:
            bare = rationale.lower().strip(' .')
            if bare in PLACEHOLDER or UNSUBSTITUTED.search(rationale) or len(bare) < 12:
                violations.append((
                    path, lineno,
                    "MALFORMED `%s`: a marker needs a one-line RATIONALE, not a "
                    "placeholder (got %r)" % (ALLOW, rationale), snippet))
            else:
                allowed += 1
            continue

        in_loop, done_idx = enclosing_loop_done(lines, n)
        if in_loop:
            # R1 — the loop's `done` redirect is the parse source.
            if done_idx < 0:
                violations.append((
                    path, lineno,
                    "inside a `while … read` loop with NO terminating `done` the scanner "
                    "could find, so its parse source is unknowable", snippet))
                continue
            kind, expr = done_redirect(lines[done_idx])
            if kind == 'none':
                violations.append((
                    path, lineno,
                    "the enclosing `while … read` loop (line %d `done`) has NO input "
                    "REDIRECT, so it is PIPE-FED: the loop body runs in a SUBSHELL and its "
                    "accumulated verdict is DISCARDED on exit — the guard passes silently. "
                    "Use `done < \"$(%s <log>)\"`-derived redirection (issue #3400 AC3)"
                    % (done_idx + 1, HELPER), snippet))
                continue
            if not source_is_stripped(expr):
                violations.append((
                    path, lineno,
                    "reads a RAW source: the enclosing loop's `done` (line %d) redirects "
                    "from `%s`, which is not derived from `%s`"
                    % (done_idx + 1, expr.strip(), HELPER), snippet))
                continue
            continue

        # R2 — the parse line itself names a stripped source.
        if source_is_stripped(line):
            continue
        violations.append((
            path, lineno,
            "reads a RAW source: neither this line nor an enclosing `while … read` loop "
            "names a value derived from `%s`" % HELPER, snippet))

if violations:
    print("FAIL: cargo-output parse site(s) that do not read from an ANSI-stripped source")
    print("      (issue #3400). Under CARGO_TERM_COLOR=always — set by 18 workflows incl.")
    print("      the nightly gate.yml, and SURVIVING redirection to a file — cargo puts the")
    print("      reset BETWEEN the status word and the payload, so a literal-text parse")
    print("      silently matches nothing. Route the read through `%s`," % HELPER)
    print("      or mark the site `%s <one-line rationale>`." % ALLOW)
    for path, lineno, reason, snippet in violations:
        rel = os.path.relpath(path)
        print("  %s:%s: %s" % (rel, lineno, reason))
        if snippet:
            print("      %s" % snippet)
    sys.exit(1)

if sites == 0:
    print("FAIL: ZERO cargo-output parse sites found in %s." % ', '.join(
        os.path.relpath(p) for p in paths))
    print("      An empty subject set is NOT a pass: it means this guard's own pattern")
    print("      stopped matching (the parsers were renamed, moved or refactored out of")
    print("      reach), and printing `0/0 PASS` would be the identical vacuous-pass shape")
    print("      this guard exists to catch, one level up (issue #3400).")
    print("      Fix the scan pattern, or point the scan at the file that holds the parsers.")
    sys.exit(1)

extra = " (%d via %s)" % (allowed, ALLOW) if allowed else ""
print("cargo-output-parsers: %d/%d parse sites read from an ANSI-stripped source%s"
      % (sites, sites, extra))
PY
