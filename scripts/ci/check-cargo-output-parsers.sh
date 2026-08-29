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
# ── WHAT COUNTS AS A PARSE SITE — ATTRIBUTION TO THE ENCLOSING CONSTRUCT ──────────────
# A non-comment line mentioning cargo/libtest/nextest output text is a CANDIDATE. It is
# deliberately NOT required to carry a match operator itself, and that requirement was the
# lint's own worst defect (roborev B4): a `case` block splits the operator from the pattern
# across lines — `case "$line" in` carries the operator, `*"Running tests/"*)` carries the
# token — so a RAW multi-line parse was INVISIBLE while the affirmative `N/N` line kept
# printing off the single-line sites. A hole shaped exactly like coverage, in the guard whose
# entire purpose is to catch that shape. Measured: a file with one stripped site plus one raw
# `case` parse reported `1/1 parse sites read from an ANSI-stripped source`, exit 0.
#
# Each candidate is resolved to the CONSTRUCT that owns its source, in this order, and the
# order is load-bearing (construct resolution runs BEFORE the mention test, so a `case`
# pattern is judged rather than excused as a quoted string):
#   1. the enclosing `while … read` loop      -> judge its `done` redirect
#   2. the enclosing `case … in` block, when the candidate is one of its PATTERNS
#                                             -> judge the block SUBJECT
#   3. the candidate's own JOINED logical command (backslash continuations are joined first,
#      so `grep -q \` / `"Running tests/" \` / `"$logfile"` is one parse, not three lines)
#   4. a token stored in a VARIABLE            -> REFUSAL (this scanner does not follow
#      variables to their match sites; it will neither skip silently nor accuse falsely)
#   5. a logical command that matches but names no attributable source -> RAW verdict
#   6. a MENTION — an AFFIRMATIVE classification, not a fall-through: the logical command
#      performs no match AND takes no input redirection AND the token sits inside a quoted
#      string, so it can only be DATA. Measured on the shipped agent-gate.sh: the four
#      `emit_summary … "error: …"` argument lines. Not a site, not counted.
#   7. anything else -> UNCLASSIFIED, a FAIL naming what could not be attributed, with text
#      deliberately DISTINCT from the empty-subject-set FAIL so a pasted summary can never
#      confuse "could not classify one site" with "found no sites at all".
#
# Note honestly what the empty-subject-set rule bought here, because it is both halves: given
# the raw `case` site ALONE the pre-fix lint DID exit 1 — via `ZERO parse sites`, the right
# refusal for the wrong reason. That rule is why B4 was a wrong-reason refusal instead of an
# undetectable hole, and it is NOT sufficient, because it evaporates the moment the file holds
# any other site, which every real file does.
#
# The token set is EXHAUSTIVE and is the same list in prose and in code — see
# CARGO_OUTPUT_TOKENS below, which this paragraph must never drift from (roborev B2: the
# header once claimed `error:` and a general `running N tests` while the code carried neither,
# so a raw parser keyed on those was invisible to the lint AND the affirmative `N/N` line still
# printed, because other sites kept the count nonzero — a hole that looked like coverage):
#   cargo status words   `Running `, `Running tests/`, `Running unittests`, `Doc-tests`,
#                        `Compiling `, `Finished ` — ALL COLOURED, all fragile.
#   cargo diagnostics    `warning:`, `warning[`, `error:`, `error[` — coloured with the reset
#                        BEFORE the colon (`warning<ESC>[0m<ESC>[1m: …`). Highest-consequence
#                        shape: a warnings-denial that silently stops denying.
#   libtest              `test result:`, `running <N> tests` (any N, as a regex — the literal
#                        `running 0 tests` alone missed every nonzero form).
#   nextest              `Summary [`, `Starting `, `PASS [`, `FAIL [`.
# Each token carries its own delimiter (a trailing space, `[`, `:` or `/`) so it identifies
# TOOL OUTPUT rather than the English word. MEASURED on the shipped scripts/agent-gate.sh:
# this full set yields the SAME 5 parse sites as the original narrow set — the widening costs
# zero false positives here, so nothing had to be excluded and the header claims the whole list.
#
# ── WHAT COUNTS AS AN ANSI-STRIPPED SOURCE ────────────────────────────────────────────
# Exactly three RECOGNISED shapes; anything else is a named FAIL rather than a guess
# (the `check-pub-surface.sh` posture — refuse, never assume):
#   R1  the parse is inside a `while … read` loop whose `done` redirect reads the CONTENTS
#       of a stripped source. Two FAILs live here, and both are silent passes otherwise:
#         * a `done` with NO redirect is PIPE-FED — the loop body runs in a SUBSHELL, so its
#           accumulated verdict variable is discarded on exit and the guard PASSES having
#           found the problem and thrown it away (issue #3400 AC3);
#         * `done <<< "$src"` names a stripped source and still reads NOTHING: a here-string
#           feeds the loop the FILENAME, one line, so the parser consumes no cargo output at
#           all. Referencing a stripped PATH is not the same as reading its CONTENTS, so the
#           redirect KIND is classified and each kind judged on its own terms — direct file
#           redirection (`< "$src"`) reads contents; a here-string or process substitution
#           must do so EXPLICITLY (`<<< "$(cat "$src")"`, `< <(cat "$src")`). An
#           unclassifiable redirect is a FAIL, never a fall-through to permissive.
#   R2  the parse line reads the CONTENTS of a stripped source — a `< "$src"` redirect, or
#       `$src` as the file operand of a content-reading command (cat/sed/awk/grep/…), or an
#       inline `_ansi_stripped_log` in a content-reading position. The same value-vs-contents
#       distinction applies: `sed … <<< "$src"` and `echo "$src" | grep …` pass the PATH, not
#       the log, and are reported as such rather than accepted for naming a stripped variable.
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

# Cargo/libtest/nextest output text a parser can key on. A CURATED, EXHAUSTIVE list — not a
# general "looks like a status word" test — and it is the SAME list the header paragraph
# states (roborev B2: they disagreed, which left `error:` and every nonzero `running N tests`
# invisible to the lint while the affirmative N/N line kept printing off other sites). Every
# entry carries its own delimiter so it identifies TOOL OUTPUT, not the English word: that is
# why `FAIL \[` and not `FAIL`, `Finished ` and not `Finished`. Regex, not substrings, so
# `running <N> tests` covers every count instead of only the literal zero.
CARGO_OUTPUT_TOKENS = re.compile(
    r'test result:'
    r'|Running tests/'
    r'|Running unittests'
    r'|Running '
    r'|Doc-tests'
    r'|running [0-9]+ tests'
    r'|Compiling '
    r'|Finished '
    r'|warning:'
    r'|warning\['
    r'|error:'
    r'|error\['
    r'|Summary \['
    r'|Starting '
    r'|PASS \['
    r'|FAIL \['
)
# A MATCH, as opposed to a mention. NOTE: this is asked of the enclosing CONSTRUCT (the joined
# logical line, and only after the loop/case resolution above has declined), NEVER of the
# candidate's own physical line — that same-line coupling was defect B4 (see the header).
MATCH_OP = re.compile(r'==|=~|\bcase\b|\bgrep\b|\bsed\b|\bawk\b|\brg\b')
# `case "$X" in` — the block header whose SUBJECT is what a pattern line matches against.
CASE_HEADER = re.compile(r'\bcase\s+(.+?)\s+in\b')
ESAC_LINE = re.compile(r'^\s*esac\b')
# A `case` PATTERN line: `<pattern>) cmds ;;`. Deliberately narrow — it must carry a `)` and
# either a `;;` terminator or be otherwise unassignable — because misreading an ordinary line
# as a case pattern would attribute it to the wrong construct.
CASE_PATTERN = re.compile(r'^\s*\(?\s*[^();=]*\)(?:\s|$)')
# An assignment whose RHS carries the token: `pat="Running tests/"`, `local pat=...`.
TOKEN_ASSIGN = re.compile(r'^\s*(?:local\s+|declare\s+[-\w]*\s+|export\s+|readonly\s+)?'
                          r'[A-Za-z_]\w*=')
ANY_INPUT_REDIR = re.compile(r'(?<![0-9<>])<')
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


def logical_units(lines):
    """Join backslash-continuation lines into LOGICAL commands.

    Required by B4's second half: a parse can be spread over continuation lines
    (`grep -q \\` / `"Running tests/" \\` / `"$logfile"`), so judging a candidate by its own
    physical line reads a pattern operand as if it were a bare string. Returns a list mapping
    each PHYSICAL line index to the joined text of the logical command it belongs to; findings
    are still reported at the candidate's own physical line number."""
    joined = [None] * len(lines)
    i = 0
    while i < len(lines):
        start = i
        parts = [lines[i]]
        while parts[-1].rstrip().endswith('\\') and i + 1 < len(lines):
            i += 1
            parts.append(lines[i])
        text = ' '.join(p.rstrip().rstrip('\\').strip() for p in parts)
        for k in range(start, i + 1):
            joined[k] = text
        i += 1
    return joined


def enclosing_case_subject(lines, n):
    """The SUBJECT of the `case … in` block containing line n, or None.

    A `case` block splits the match operator from the pattern across lines, which is exactly
    why B4's raw site was invisible: `case "$line" in` carried the operator and
    `*"Running tests/"*)` carried the token, so neither line qualified on its own. Walk
    backward to the block header, stopping at any structural marker that proves we are not
    inside one."""
    for k in range(n - 1, -1, -1):
        line = lines[k]
        if is_comment(line):
            continue
        m = CASE_HEADER.search(line)
        if m:
            return m.group(1)
        if (ESAC_LINE.match(line) or DONE_LINE.match(line)
                or FUNC_DEF.match(line) or CLOSE_BRACE.match(line)):
            return None
    return None


def token_in_quotes(line, tok_match):
    """Is the token occurrence inside a quoted string? Counted by quote parity before the
    match, which is exact for the single-line shapes here and errs toward 'not quoted' (i.e.
    toward judging rather than excusing) on anything exotic."""
    prefix = line[:tok_match.start()]
    return (prefix.count('"') % 2 == 1) or (prefix.count("'") % 2 == 1)


# Commands that read a FILE OPERAND'S CONTENTS. `echo`/`printf` are deliberately absent:
# they emit their ARGUMENT, so `echo "$src" | grep …` greps the FILENAME.
CONTENT_READER = (r'\b(?:cat|sed|awk|grep|egrep|fgrep|rg|tr|tail|head|sort|uniq|wc|nl|cut'
                  r'|tac|rev|od|xxd|strings)\b')
# A direct input redirection `< X` — and specifically NOT `<<<` (a here-string, which feeds a
# VALUE) and NOT `< <(` (a process substitution, judged on its command instead).
DIRECT_REDIR = re.compile(r'(?<!<)<(?!<)\s*(?!\()"?\$\{?([A-Za-z_]\w*)')
FILE_OPERAND = re.compile(CONTENT_READER + r'[^|<>]*?"?\$\{?([A-Za-z_]\w*)\}?"?')
INLINE_HELPER_READ = re.compile(r'<\s*"?\$\(\s*' + HELPER + r'\b')


def done_redirect(line):
    """Classify a `done …` line's input redirect.
    Returns (kind, expr): kind in {'file', 'herestring', 'procsub', 'none'}.

    The KIND matters, it is not a formality: only 'file' reads the referenced path's
    CONTENTS by itself. 'herestring' feeds the loop the VALUE of its operand — so
    `done <<< "$src"` hands the parser a one-line filename and it consumes no cargo output
    whatsoever, which is the vacuous pass this whole lint exists to catch. 'procsub' reads
    whatever its command produces, which may or may not be the file. Hence each kind is
    judged separately by the caller, and an unrecognised shape FAILs (closed grammar)."""
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

    def reads_stripped_contents(expr):
        """Does `expr` read the CONTENTS of an ANSI-stripped log?

        The distinction this function exists for: naming a stripped PATH is not reading it.
        `<<< "$src"` and `echo "$src" | …` both reference a stripped variable and both feed a
        one-line FILENAME to the parser, which then matches nothing and reports clean. So a
        stripped variable only counts when it appears in a CONTENT-READING position — the
        target of a direct `< ` redirection, or the file operand of a command that reads its
        operand's contents. ACCEPT only on an affirmative match; there is no permissive
        fall-through."""
        if INLINE_HELPER_READ.search(expr):
            return True
        for m in DIRECT_REDIR.finditer(expr):
            if m.group(1) in stripped_vars:
                return True
        for m in FILE_OPERAND.finditer(expr):
            if m.group(1) in stripped_vars:
                return True
        return False

    joined = logical_units(lines)

    for n, line in enumerate(lines):
        if is_comment(line):
            continue
        tok = CARGO_OUTPUT_TOKENS.search(line)
        if not tok:
            continue
        # A cargo token makes this line a CANDIDATE. It is deliberately NOT required to carry
        # a match operator itself (defect B4): `case` splits the operator from the pattern
        # across lines, so a same-line requirement made every multi-line parse invisible while
        # the affirmative N/N line kept printing off the single-line ones. Mention-vs-match is
        # decided below, from the enclosing construct.
        unit = joined[n] if joined[n] is not None else line
        lineno = n + 1
        snippet = ' '.join(line.split())
        if len(snippet) > 130:
            snippet = snippet[:127] + '...'

        # R3 — explicit allow, rationale required.
        found, rationale = allow_rationale(lines, n)
        if found:
            sites += 1
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
            sites += 1
            # R1 — the loop's `done` redirect is the parse source.
            if done_idx < 0:
                violations.append((
                    path, lineno,
                    "inside a `while … read` loop with NO terminating `done` the scanner "
                    "could find, so its parse source is unknowable", snippet))
                continue
            kind, expr = done_redirect(lines[done_idx])
            if kind not in ('file', 'herestring', 'procsub', 'none'):
                # Closed grammar: an unrecognised redirect kind FAILs. A new shape must be
                # classified deliberately, never inherit the permissive branch.
                violations.append((
                    path, lineno,
                    "the enclosing loop's `done` (line %d) uses an input redirection this "
                    "scanner does not classify (%r), so whether it reads the log's CONTENTS "
                    "is unknown — classify it here or mark the site" % (done_idx + 1, kind),
                    snippet))
                continue
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
            if kind == 'file':
                # A direct `< X` redirection reads X's CONTENTS. Stripped path, done.
                continue
            # 'herestring' / 'procsub': naming a stripped path is NOT reading it. A
            # here-string feeds the loop the VALUE — one line of FILENAME — so the parser
            # consumes no cargo output at all and reports clean, which is precisely the
            # vacuous pass this lint exists to catch. A process substitution reads whatever
            # its command produces. Both must read the contents EXPLICITLY.
            if reads_stripped_contents(expr):
                continue
            violations.append((
                path, lineno,
                "the enclosing loop's `done` (line %d) uses a %s (`%s`) that names a stripped "
                "path WITHOUT READING IT: %s feed the loop the VALUE, so the parser consumes a "
                "one-line FILENAME and matches NOTHING while reporting clean. Read the contents "
                "explicitly (`done < \"$src\"`, `done < <(cat \"$src\")`, "
                "`done <<< \"$(cat \"$src\")\"`)"
                % (done_idx + 1,
                   'here-string' if kind == 'herestring' else 'process substitution',
                   expr.strip(),
                   'here-strings' if kind == 'herestring'
                   else 'process substitutions of a non-reading command'),
                snippet))
            continue

        # R1b — an enclosing `case … in` block, when the candidate is one of its PATTERNS.
        # This is B4's reported shape once the loop resolution above declines: the operator
        # lives on the header and the token on the pattern, so the construct — not the line —
        # is what has a source.
        if CASE_PATTERN.match(line):
            subject = enclosing_case_subject(lines, n)
            if subject is not None:
                sites += 1
                if reads_stripped_contents(subject) or source_is_stripped(subject):
                    # The subject is a stripped value; whether it is a loop read variable or a
                    # command substitution over the stripped log, the pattern matches stripped
                    # text. (A loop read variable would have been resolved by R1 above.)
                    continue
                violations.append((
                    path, lineno,
                    "is a `case` PATTERN whose block subject `%s` is not derived from `%s`, so "
                    "it matches RAW cargo text. The operator and the pattern sit on different "
                    "lines, which is why this must be judged as a construct" % (subject, HELPER),
                    snippet))
                continue

        # R2 — the candidate's own logical command must READ a stripped source, not merely
        # name one. Judged on `unit` (the joined command), never on the physical line: a
        # continuation-line token measured against its fragment sees no source at all, which
        # is B4's second half.
        if reads_stripped_contents(unit):
            sites += 1
            continue
        if source_is_stripped(unit):
            sites += 1
            violations.append((
                path, lineno,
                "names a stripped source but does not READ IT: the reference is in a VALUE "
                "position (a here-string, or an `echo`/`printf` piped into the parser), which "
                "passes the one-line PATH instead of the log's contents — the parser then "
                "matches nothing and reports clean. Use `< \"$src\"` or pass `$src` as the file "
                "operand of the parsing command", snippet))
            continue

        # R4 — a cargo token STORED IN A VARIABLE. This scanner does not follow variables to
        # their match sites, and it says so rather than guessing in either direction: a silent
        # skip would be the B4 hole again, and a RAW verdict would be an accusation it cannot
        # support. REFUSAL, textually distinct from every other cause.
        if TOKEN_ASSIGN.match(unit):
            sites += 1
            violations.append((
                path, lineno,
                "UNRESOLVED (cargo token held in a variable): this scanner does not follow a "
                "variable to the place it is matched, so it cannot tell whether that match "
                "reads a stripped source. Match against a `%s`-derived read at the parse "
                "itself, or mark the site" % HELPER, snippet))
            continue

        # R5 — the logical command performs a MATCH but names no source this scanner can
        # attribute: a raw read is the only reading of it that is safe to report.
        if MATCH_OP.search(unit):
            sites += 1
            violations.append((
                path, lineno,
                "reads a RAW source: this logical command matches cargo output but neither it "
                "nor an enclosing `while … read` loop or `case` block reads the contents of a "
                "value derived from `%s`" % HELPER, snippet))
            continue

        # R6 — a MENTION, and this is an AFFIRMATIVE classification, not a fall-through: the
        # logical command performs no match AND takes no input redirection AND the token sits
        # inside a quoted string, so it can only be DATA (a message, a summary line, a
        # diagnostic). Such a candidate is NOT a parse site and is not counted. Measured on the
        # shipped agent-gate.sh: the four `emit_summary … "error: …"` argument lines.
        if (not MATCH_OP.search(unit) and not ANY_INPUT_REDIR.search(unit)
                and token_in_quotes(line, tok)):
            continue

        # R7 — CLOSED GRAMMAR. Everything else is a candidate this scanner could not attribute
        # to any construct. It FAILs, naming what it could not classify, and its text is
        # deliberately distinct from the empty-subject-set FAIL so a pasted summary can never
        # confuse "I could not classify this one site" with "I found no sites at all".
        sites += 1
        violations.append((
            path, lineno,
            "UNCLASSIFIED cargo-output candidate: this scanner could not attribute the token "
            "to any parse construct it recognises (no enclosing `while … read` loop or `case` "
            "block, no readable source on the line, no match operator, and not a quoted "
            "message), so whether it reads coloured text is UNKNOWN. Route it through `%s` at "
            "the parse, or mark the site with a rationale" % HELPER, snippet))

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
