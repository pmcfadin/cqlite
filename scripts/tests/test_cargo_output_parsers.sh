#!/usr/bin/env bash
# test_cargo_output_parsers.sh — self-test for the #3400 cargo-output colour lint.
#
# Two subjects, because the issue has two halves:
#
#   (A) THE PINNED DEFECT, RED-first. The PRE-FIX shape of the cli-tests zero-tests guard
#       (`check_no_unexpected_zero_tests`, verbatim from main) is run against the SAME
#       zero-test cargo log twice — once coloured, once plain — and asserted to exit 0
#       (silent VACUOUS PASS) on the coloured one and 1 on the plain one. That is the
#       defect, characterised and pinned so it cannot come back unnoticed. The CURRENT
#       shape, EXTRACTED FROM THE SHIPPED scripts/agent-gate.sh, must then exit 1 on BOTH.
#
#   (B) THE STRUCTURAL LINT, scripts/ci/check-cargo-output-parsers.sh: it must red a raw
#       parse site, red a PIPE-FED while-read loop (AC3), red an EMPTY SUBJECT SET rather
#       than print `0/0 PASS`, honour `cargo-colour-lint-allow` only WITH a real
#       rationale, ignore a bare mention that performs no match, and green the shipped
#       agent-gate.sh with an affirmative non-zero count.
#
# FIXTURE PROVENANCE. Every escape sequence below is a REAL ESC byte injected via
# `printf '\033'` — never a hand-typed two-character `\x1b` string, which would make the
# whole suite test nothing. The sequences are transcribed from a `cat -v` capture of real
# `cargo test` output under CARGO_TERM_COLOR=always REDIRECTED TO A FILE (issue #3400
# oracle run), including the decisive detail: the reset lands BETWEEN the status word and
# the payload (`Running<ESC>[0m tests/empty.rs`), so the literal `Running tests/` never
# appears. `running N tests` and `test result:` are libtest text and were measured
# byte-identical under `always` and `never`, so they are written here WITHOUT escapes —
# that asymmetry is itself part of the fixture.
#
# AC3 is asserted both ways: behaviourally (a pipe-fed while-read loop over the very same
# log silently exits 0 because its `bad` accumulator dies with the subshell, while the
# redirect-fed loop exits 1) and structurally (the shipped guard uses `done < "$_parse_src"`,
# and the lint reds a planted pipe-fed site).
#
# Hermetic: temp dir only. No cargo, no datasets, no network, no gh. Runnable standalone.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LINT="$REPO_ROOT/scripts/ci/check-cargo-output-parsers.sh"
GATE="$REPO_ROOT/scripts/agent-gate.sh"

PASSES=0
FAILS=0
ok()  { printf 'ok   - %s\n' "$1"; PASSES=$((PASSES + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAILS=$((FAILS + 1)); }

for f in "$LINT" "$GATE"; do
  if [ ! -r "$f" ]; then
    echo "FAIL: required file not readable: $f"
    exit 1
  fi
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (the lint under test is a no-op without it)"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# ─────────────────────────────────────────────────────────────────────────────────────
# Fixture: a cargo `cargo test` log in which tests/empty.rs runs ZERO tests.
# make_cargo_log <colour|plain> <outfile>
# ─────────────────────────────────────────────────────────────────────────────────────
ESC="$(printf '\033')"

make_cargo_log() {
  local mode="$1" out="$2" pre="" post=""
  if [ "$mode" = colour ]; then
    # Exactly the bytes cargo emits: bold + bright-green on the status word, reset
    # BEFORE the payload. Transcribed from the #3400 `cat -v` capture.
    pre="${ESC}[1m${ESC}[92m"
    post="${ESC}[0m"
  fi
  {
    # cargo status lines — COLOURED under CARGO_TERM_COLOR=always.
    printf '%s    Finished%s `test` profile [unoptimized + debuginfo] target(s) in 0.00s\n' "$pre" "$post"
    printf '%s     Running%s unittests src/lib.rs (target/debug/deps/dw-5b885fbb233ae842)\n' "$pre" "$post"
    # libtest lines — measured UNCOLOURED in both modes (cargo does not pass --color
    # through to the harness), so they are written plain on purpose.
    printf '\nrunning 1 test\ntest tests::it_works ... ok\n\n'
    printf 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n\n'
    printf '%s     Running%s tests/empty.rs (target/debug/deps/empty-679f6c9c5bff9e92)\n' "$pre" "$post"
    printf '\nrunning 0 tests\n\n'
    printf 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n\n'
    printf '%s     Running%s tests/foo.rs (target/debug/deps/foo-1d5149cf0c5ed499)\n' "$pre" "$post"
    printf '\nrunning 1 test\ntest one ... ok\n\n'
    printf 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n\n'
    printf '%s   Doc-tests%s dw\n' "$pre" "$post"
    printf '\nrunning 0 tests\n\n'
    printf 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n'
  } >"$out"
}

make_cargo_log colour "$tmp/zero-colour.log"
make_cargo_log plain  "$tmp/zero-plain.log"

# Non-vacuity of the fixtures themselves: the coloured one must actually carry ESC bytes
# and the plain one must carry none. A suite whose "coloured" fixture is not coloured
# proves nothing (and would green through the defect it is meant to pin).
esc_colour=$(LC_ALL=C tr -cd '\033' <"$tmp/zero-colour.log" | wc -c | tr -d ' ')
esc_plain=$(LC_ALL=C tr -cd '\033' <"$tmp/zero-plain.log" | wc -c | tr -d ' ')
if [ "$esc_colour" -gt 0 ] && [ "$esc_plain" -eq 0 ]; then
  ok "fixture provenance: coloured log carries $esc_colour real ESC bytes, plain log carries 0"
else
  bad "fixture provenance: expected ESC>0 in the coloured log and 0 in the plain one (got $esc_colour / $esc_plain) — the rest of this suite would be vacuous"
fi
# And the decisive property: the literal `Running tests/` is ABSENT from the coloured log.
if grep -q 'Running tests/' "$tmp/zero-plain.log" && ! grep -q 'Running tests/' "$tmp/zero-colour.log"; then
  ok "fixture provenance: the literal 'Running tests/' is present in the plain log and ABSENT in the coloured one (reset lands between status word and payload)"
else
  bad "fixture provenance: 'Running tests/' presence is not split plain/coloured as measured — the fixture does not reproduce the defect"
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (A1) RED-first: the PRE-FIX shape of site 1, verbatim from main. It must PASS
#      (exit 0) on the coloured zero-test log — the silent vacuous pass — and FAIL
#      (exit 1) on the same log uncoloured.
# ─────────────────────────────────────────────────────────────────────────────────────
cat >"$tmp/prefix_guard.sh" <<'PREFIX'
# VERBATIM pre-#3400 shape of scripts/agent-gate.sh's cli-tests zero-tests guard
# (the `'"'"'` blob escaping unwound). Kept here as the PINNED DEFECT, not as live code.
check_no_unexpected_zero_tests() {
  local pass_name="$1" logfile="$2"; shift 2
  local allowed_zero=" $* "
  local bad="" target=""
  while IFS= read -r line; do
    if [[ "$line" == *"Running tests/"* ]]; then
      target=$(printf "%s" "$line" | sed -E "s#.*Running tests/([^[:space:]]+)\.rs.*#\1#")
    elif [[ "$line" == "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"* ]]; then
      if [ -n "$target" ] && [[ "$allowed_zero" != *" $target "* ]]; then
        bad="$bad $target"
      fi
      target=""
    elif [[ "$line" == "test result:"* ]]; then
      target=""
    fi
  done < "$logfile"
  if [ -n "$bad" ]; then
    echo "cli-tests: FAIL-CLOSED —$bad ran 0 tests in $pass_name unexpectedly" >&2
    return 1
  fi
  return 0
}
PREFIX

prefix_rc_colour=0
( set +e; . "$tmp/prefix_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" >/dev/null 2>&1; exit $? ) || prefix_rc_colour=$?
prefix_rc_plain=0
( set +e; . "$tmp/prefix_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-plain.log" >/dev/null 2>&1; exit $? ) || prefix_rc_plain=$?

if [ "$prefix_rc_colour" -eq 0 ]; then
  ok "RED (pinned defect): the PRE-FIX guard exits 0 on the COLOURED zero-test log — the vacuous pass, reproduced"
else
  bad "RED (pinned defect): the PRE-FIX guard exited $prefix_rc_colour (expected 0) on the coloured log — the fixture no longer reproduces the #3400 defect, so the GREEN case below proves nothing"
fi
if [ "$prefix_rc_plain" -eq 1 ]; then
  ok "RED (pinned defect): the PRE-FIX guard exits 1 on the SAME log uncoloured — colour alone flips the verdict"
else
  bad "RED (pinned defect): the PRE-FIX guard exited $prefix_rc_plain (expected 1) on the plain log — it is not detecting the zero-test target at all"
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (A2) GREEN: the CURRENT shape, extracted from the SHIPPED scripts/agent-gate.sh.
#      Extraction is fail-closed: an empty extraction would run a guard that parses
#      nothing and "passes", which is the very shape under test (#1699 round-13 lesson).
# ─────────────────────────────────────────────────────────────────────────────────────
python3 - "$GATE" "$tmp/current_guard.sh" <<'PY'
import re, sys
gate, out = sys.argv[1], sys.argv[2]
lines = open(gate, encoding='utf-8').read().split('\n')


def extract(start_re, end_re):
    for i, l in enumerate(lines):
        if re.match(start_re, l):
            for j in range(i + 1, len(lines)):
                if re.match(end_re, lines[j]):
                    return '\n'.join(lines[i:j + 1])
            break
    return ''


helper = extract(r'^_ansi_stripped_log\(\) \{', r'^\}')
guard = extract(r'^  check_no_unexpected_zero_tests\(\) \{', r'^  \}')
# Unwind the cli-tests `bash -c '…'` blob escaping: '"'"' is a literal single quote.
guard = guard.replace('\'"\'"\'', "'")
if not helper.strip() or 'sed -E' not in helper:
    print('EXTRACT-FAIL: _ansi_stripped_log', file=sys.stderr); sys.exit(2)
if not guard.strip() or 'while IFS= read' not in guard:
    print('EXTRACT-FAIL: check_no_unexpected_zero_tests', file=sys.stderr); sys.exit(2)
if '_ansi_stripped_log' not in guard:
    print('EXTRACT-FAIL: the extracted guard does not call _ansi_stripped_log — it would '
          'parse the raw log and this suite would certify the defect', file=sys.stderr)
    sys.exit(2)
open(out, 'w', encoding='utf-8').write(helper + '\n\n' + guard + '\n')
PY
extract_rc=$?
if [ "$extract_rc" -ne 0 ]; then
  bad "extraction of the shipped guard + _ansi_stripped_log from agent-gate.sh FAILED (rc=$extract_rc) — cannot certify the current shape"
else
  ok "extracted _ansi_stripped_log + check_no_unexpected_zero_tests from the shipped agent-gate.sh"
  if bash -n "$tmp/current_guard.sh" 2>/dev/null; then
    ok "the extracted guard is syntactically valid bash (the blob escaping unwinds cleanly)"
  else
    bad "the extracted guard is not valid bash — the cli-tests blob escaping changed shape"
  fi

  cur_rc_colour=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" >/dev/null 2>&1; exit $? ) || cur_rc_colour=$?
  cur_rc_plain=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-plain.log" >/dev/null 2>&1; exit $? ) || cur_rc_plain=$?

  if [ "$cur_rc_colour" -eq 1 ]; then
    ok "GREEN: the SHIPPED guard exits 1 on the COLOURED zero-test log (the #3400 fix, measured)"
  else
    bad "GREEN: the SHIPPED guard exited $cur_rc_colour (expected 1) on the coloured log — the zero-test protection is inert under CARGO_TERM_COLOR=always"
  fi
  if [ "$cur_rc_plain" -eq 1 ]; then
    ok "GREEN: the SHIPPED guard exits 1 on the plain zero-test log too (colour no longer changes the verdict)"
  else
    bad "GREEN: the SHIPPED guard exited $cur_rc_plain (expected 1) on the plain log"
  fi

  # A guard that reds everything is not a fix. Positive control: the same coloured log
  # with the zero-test target on the allowed-zero list must PASS.
  cur_rc_allowed=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" empty >/dev/null 2>&1; exit $? ) || cur_rc_allowed=$?
  if [ "$cur_rc_allowed" -eq 0 ]; then
    ok "positive control: the SHIPPED guard PASSes the coloured log when 'empty' is on the allowed-zero list (so it parsed the target NAME out of coloured text, not just 'something is wrong')"
  else
    bad "positive control: the SHIPPED guard exited $cur_rc_allowed (expected 0) with 'empty' allowed — it is reporting a failure it cannot attribute, i.e. it did not recover the target name from the coloured banner"
  fi

  # Fail-closed control: an unreadable log must FAIL, never pass having parsed nothing.
  cur_rc_missing=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/does-not-exist.log" >/dev/null 2>&1; exit $? ) || cur_rc_missing=$?
  if [ "$cur_rc_missing" -ne 0 ]; then
    ok "fail-closed control: the SHIPPED guard FAILs on an unreadable log rather than passing having parsed nothing"
  else
    bad "fail-closed control: the SHIPPED guard PASSed on an unreadable log — a guard that consumed no input has measured nothing"
  fi

  # AC3, structurally: the shipped guard must be REDIRECTION-fed, not pipe-fed.
  if grep -q 'done < "\$_parse_src"' "$tmp/current_guard.sh" && ! grep -qE '\|[[:space:]]*while IFS= read' "$tmp/current_guard.sh"; then
    ok "AC3 (structural): the shipped guard reads via REDIRECTION (done < \"\$_parse_src\"), not through a pipe into while-read"
  else
    bad "AC3 (structural): the shipped guard is not redirection-fed — a piped while-read loop runs in a subshell and discards its verdict"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (A3) AC3, behaviourally: WHY the pipe shape is forbidden. Same log, two loops.
# ─────────────────────────────────────────────────────────────────────────────────────
cat >"$tmp/ac3.sh" <<'AC3'
guard_piped() {
  local bad=""
  cat "$1" | while IFS= read -r line; do
    case "$line" in
      "test result: ok. 0 passed; 0 failed; 0 ignored"*) bad="$bad zero" ;;
    esac
  done
  [ -z "$bad" ]
}
guard_redirected() {
  local bad=""
  while IFS= read -r line; do
    case "$line" in
      "test result: ok. 0 passed; 0 failed; 0 ignored"*) bad="$bad zero" ;;
    esac
  done < "$1"
  [ -z "$bad" ]
}
AC3
piped_rc=0
( set +e; . "$tmp/ac3.sh"; guard_piped "$tmp/zero-plain.log"; exit $? ) || piped_rc=$?
redir_rc=0
( set +e; . "$tmp/ac3.sh"; guard_redirected "$tmp/zero-plain.log"; exit $? ) || redir_rc=$?
if [ "$piped_rc" -eq 0 ] && [ "$redir_rc" -eq 1 ]; then
  ok "AC3 (behavioural): over the SAME log the PIPE-fed loop silently exits 0 (subshell discards \$bad) while the REDIRECT-fed loop exits 1"
else
  bad "AC3 (behavioural): expected piped=0 / redirected=1, got piped=$piped_rc / redirected=$redir_rc — the subshell-verdict-loss premise did not reproduce on this bash ($BASH_VERSION)"
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (B) The structural lint.
# ─────────────────────────────────────────────────────────────────────────────────────
lint_out=""
lint_rc=0
run_lint() { # run_lint <path...>
  lint_rc=0
  lint_out=$(bash "$LINT" "$@" 2>&1) || lint_rc=$?
}

# B1 — a RAW parse site (the pre-fix shape) must RED.
cat >"$tmp/f_raw.sh" <<'F'
myguard() {
  while IFS= read -r line; do
    case "$line" in *"Running tests/"*) echo hit ;; esac
  done < "$1"
}
F
run_lint "$tmp/f_raw.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'RAW source'; then
  ok "lint B1: reds a raw parse site (while-read loop redirected from an unstripped log)"
else
  bad "lint B1: expected a non-zero exit naming a RAW source, got rc=$lint_rc"
fi

# B2 — the fixed shape must GREEN, with an affirmative count.
cat >"$tmp/f_stripped.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  while IFS= read -r line; do
    case "$line" in *"Running tests/"*) echo hit ;; esac
  done < "$src"
}
F
run_lint "$tmp/f_stripped.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q '^cargo-output-parsers: 1/1 parse sites read from an ANSI-stripped source$'; then
  ok "lint B2: greens a stripped parse site and prints the affirmative '1/1' line"
else
  bad "lint B2: expected rc=0 and an affirmative '1/1' line, got rc=$lint_rc / '$lint_out'"
fi

# B3 — a PIPE-FED while-read loop is its own FAIL (AC3, structural).
cat >"$tmp/f_piped.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  cat "$src" | while IFS= read -r line; do
    case "$line" in *"Running tests/"*) echo hit ;; esac
  done
}
F
run_lint "$tmp/f_piped.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'SUBSHELL'; then
  ok "lint B3: reds a PIPE-FED while-read loop even though its source IS stripped (the verdict dies with the subshell)"
else
  bad "lint B3: expected a non-zero exit naming the SUBSHELL hazard, got rc=$lint_rc"
fi

# B4 — AN EMPTY SUBJECT SET IS A FAIL, NOT `0/0 PASS`.
cat >"$tmp/f_empty.sh" <<'F'
#!/usr/bin/env bash
# No cargo-output parse sites at all in this file.
myhelper() {
  grep -c '^foo' "$1"
}
F
run_lint "$tmp/f_empty.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'ZERO cargo-output parse sites'; then
  ok "lint B4: an EMPTY SUBJECT SET FAILs (never '0/0 PASS') — the vacuous-pass shape one level up"
else
  bad "lint B4: expected a non-zero exit naming ZERO parse sites, got rc=$lint_rc / '$lint_out'"
fi
# ...and specifically never emits the AFFIRMATIVE verdict line with a zero count. The
# match is anchored on the verdict line, not on the substring `0/0` anywhere in the
# output: the FAIL diagnostic legitimately QUOTES `0/0 PASS` while explaining why it
# refuses to print one, and a substring test would read that explanation as the offence.
if printf '%s\n' "$lint_out" | grep -q '^cargo-output-parsers: 0/0'; then
  bad "lint B4: the lint emitted the affirmative verdict line with a 0/0 count — a figure with no subject behind it"
else
  ok "lint B4: the lint never emits the affirmative verdict line with a 0/0 count"
fi

# B5 — a bare MENTION that performs no match is not a parse site.
cat >"$tmp/f_mention.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  echo "myguard: looking for a 'test result:' line and a 'Running tests/' banner"
  printf '%s\n' "no test result: line was found"
  while IFS= read -r line; do
    case "$line" in *"Running tests/"*) echo hit ;; esac
  done < "$src"
}
F
run_lint "$tmp/f_mention.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q '^cargo-output-parsers: 1/1 '; then
  ok "lint B5: counts 1 site, not 3 — two echo/printf MENTIONS of cargo text perform no match and are not parse sites"
else
  bad "lint B5: expected rc=0 with exactly 1/1 counted, got rc=$lint_rc / '$lint_out'"
fi

# B6 — the escape hatch, WITH a rationale.
cat >"$tmp/f_allow_good.sh" <<'F'
myguard() {
  # cargo-colour-lint-allow this log is written by our own fixture generator, never by cargo
  grep -c "test result:" "$1"
}
F
run_lint "$tmp/f_allow_good.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q 'via cargo-colour-lint-allow'; then
  ok "lint B6: honours cargo-colour-lint-allow with a one-line rationale, and SAYS SO in the affirmative line"
else
  bad "lint B6: expected rc=0 naming the allow count, got rc=$lint_rc / '$lint_out'"
fi

# B7 — a bare placeholder rationale is REFUSED.
cat >"$tmp/f_allow_todo.sh" <<'F'
myguard() {
  # cargo-colour-lint-allow todo
  grep -c "test result:" "$1"
}
F
run_lint "$tmp/f_allow_todo.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'MALFORMED'; then
  ok "lint B7: refuses a bare placeholder rationale ('todo') as MALFORMED"
else
  bad "lint B7: expected a non-zero MALFORMED verdict for a placeholder rationale, got rc=$lint_rc"
fi

# B8 — an UNSUBSTITUTED template rationale is REFUSED (claim.sh's rule).
cat >"$tmp/f_allow_tmpl.sh" <<'F'
myguard() {
  # cargo-colour-lint-allow <one-line rationale goes here>
  grep -c "test result:" "$1"
}
F
run_lint "$tmp/f_allow_tmpl.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'MALFORMED'; then
  ok "lint B8: refuses an unsubstituted '<...>' template rationale as MALFORMED"
else
  bad "lint B8: expected a non-zero MALFORMED verdict for an unsubstituted template, got rc=$lint_rc"
fi

# B9 — an unreadable scan target FAILs (a guard that cannot read its subject measured nothing).
run_lint "$tmp/does-not-exist.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'not readable'; then
  ok "lint B9: FAILs on an unreadable scan target rather than reporting OK"
else
  bad "lint B9: expected a non-zero 'not readable' verdict, got rc=$lint_rc / '$lint_out'"
fi

# B10 — the SHIPPED scripts/agent-gate.sh must green, with a non-zero measured count.
run_lint
if [ "$lint_rc" -eq 0 ]; then
  count=$(printf '%s' "$lint_out" | sed -n 's/^cargo-output-parsers: \([0-9][0-9]*\)\/.*/\1/p' | tail -1)
  if [ -n "$count" ] && [ "$count" -ge 4 ]; then
    ok "lint B10: the shipped agent-gate.sh greens with $count measured parse sites (>= the 4 enumerated on main)"
  else
    bad "lint B10: the shipped scan greened but reported '${count:-<none>}' parse sites — a green with no measured subject is exactly what this lint refuses"
  fi
else
  bad "lint B10: the shipped scripts/agent-gate.sh does NOT pass the lint: $lint_out"
fi

# B11 — the lint must still red the ORIGINAL, unfixed shape when it is planted verbatim
#       beside the fixed one (so a partial revert cannot hide behind the green above).
{
  cat "$tmp/f_stripped.sh"
  echo
  cat "$tmp/prefix_guard.sh"
} >"$tmp/f_mixed.sh"
run_lint "$tmp/f_mixed.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'RAW source'; then
  ok "lint B11: one stripped site does not excuse a raw one in the same file"
else
  bad "lint B11: expected a non-zero RAW-source verdict for the mixed fixture, got rc=$lint_rc"
fi

echo
printf 'passed=%d failed=%d\n' "$PASSES" "$FAILS"
if [ "$FAILS" -gt 0 ]; then
  echo "FAIL: test_cargo_output_parsers self-test"
  exit 1
fi
echo "PASS: test_cargo_output_parsers self-test"
