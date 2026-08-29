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

  # C3: AFFIRMATIVE MEASUREMENT. Every check before the loop only establishes that nothing
  # BAD was seen, and all of them are satisfied by parsing NOTHING — an empty log, a
  # truncated log, or a log whose banners cargo has reformatted runs zero iterations and
  # falls through to `return 0`. That is the vacuous pass #3400 exists to remove, surviving
  # inside the fix for it. Both cases below have a READABLE, NON-EMPTY, successfully
  # normalised log, so they isolate the banner count as the only thing that can red them.
  zero_banner_rc=0
  printf 'some cargo noise\nnothing recognisable here\n' >"$tmp/no-banners.log"
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/no-banners.log" >/dev/null 2>&1; exit $? ) || zero_banner_rc=$?
  if [ "$zero_banner_rc" -ne 0 ]; then
    ok "C3: a NON-EMPTY log with no recognised target banners FAILs (the guard judged no target, so it measured nothing)"
  else
    bad "C3: the guard PASSed a log it found no target banners in — it reported OK having judged nothing, the exact vacuous pass #3400 removes"
  fi
  empty_log_rc=0
  : >"$tmp/empty.log"
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/empty.log" >/dev/null 2>&1; exit $? ) || empty_log_rc=$?
  if [ "$empty_log_rc" -ne 0 ]; then
    ok "C3: an EMPTY log FAILs (zero iterations is zero measurement, not a clean bill of health)"
  else
    bad "C3: the guard PASSed an EMPTY log — zero loop iterations reported as success"
  fi
  # ...and the positive control that keeps C3 from being a reject-everything rule: the real
  # coloured log has banners, so it still reds for the RIGHT reason (a zero-test target) and
  # still PASSes when that target is allowed.
  c3_ctrl_rc=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" empty >/dev/null 2>&1; exit $? ) || c3_ctrl_rc=$?
  if [ "$c3_ctrl_rc" -eq 0 ]; then
    ok "C3: a log WITH banners still PASSes when its zero-test target is allowed (the banner assert is not a blanket reject)"
  else
    bad "C3: the banner assert reds a legitimate log — rc=$c3_ctrl_rc"
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
# (A4) The OTHER shipped parse site: run_arrow_parity_guard_cmd (`arrow-parity-guard`).
#      Its `test result:` parse is colour-SAFE today (libtest text, measured identical
#      both ways) and its failure direction is a FALSE RED, so #3400 routes it through
#      _ansi_stripped_log as BELT. Exercised here with a stub `cargo` so the belt is
#      proven not to have broken the guard in either direction.
# ─────────────────────────────────────────────────────────────────────────────────────
python3 - "$GATE" "$tmp/arrow_guard.sh" <<'ARROWPY'
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
guard = extract(r'^run_arrow_parity_guard_cmd\(\) \{', r'^\}')
if not helper.strip() or not guard.strip():
    print('EXTRACT-FAIL', file=sys.stderr); sys.exit(2)
if '_ansi_stripped_log' not in guard:
    print('EXTRACT-FAIL: run_arrow_parity_guard_cmd does not call _ansi_stripped_log',
          file=sys.stderr)
    sys.exit(2)
open(out, 'w', encoding='utf-8').write(helper + '\n\n' + guard + '\n')
ARROWPY
arrow_extract_rc=$?
if [ "$arrow_extract_rc" -ne 0 ]; then
  bad "extraction of run_arrow_parity_guard_cmd (+ helper) from agent-gate.sh FAILED (rc=$arrow_extract_rc) — the arrow-parity-guard parse is uncertified"
else
  ok "extracted run_arrow_parity_guard_cmd from the shipped agent-gate.sh (it calls _ansi_stripped_log)"
  # Stub `cargo`: emits a COLOURED cargo log. $STUB_PASSED controls the reported count.
  cat >"$tmp/arrow_stub.sh" <<STUB
cargo() {
  printf '%s     Running%s tests/issue_1495.rs (target/debug/deps/x-1)\n' '${ESC}[1m${ESC}[92m' '${ESC}[0m'
  printf '\nrunning %s tests\n\n' "\$STUB_PASSED"
  printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n' "\$STUB_PASSED"
}
STUB
  arrow_rc_ok=0
  ( set +e; . "$tmp/arrow_guard.sh"; STUB_PASSED=3; . "$tmp/arrow_stub.sh"; run_arrow_parity_guard_cmd >/dev/null 2>&1; exit $? ) || arrow_rc_ok=$?
  arrow_rc_zero=0
  ( set +e; . "$tmp/arrow_guard.sh"; STUB_PASSED=0; . "$tmp/arrow_stub.sh"; run_arrow_parity_guard_cmd >/dev/null 2>&1; exit $? ) || arrow_rc_zero=$?
  if [ "$arrow_rc_ok" -eq 0 ]; then
    ok "arrow-parity-guard: exits 0 on a COLOURED cargo log reporting 3 passed (the belt introduced no false red)"
  else
    bad "arrow-parity-guard: exited $arrow_rc_ok (expected 0) on a coloured log reporting 3 passed — FALSE RED"
  fi
  if [ "$arrow_rc_zero" -ne 0 ]; then
    ok "arrow-parity-guard: still FAILs on a COLOURED cargo log reporting 0 passed (the vacuous-skip protection survives)"
  else
    bad "arrow-parity-guard: PASSed a coloured log reporting 0 passed — the vacuous-skip protection is gone"
  fi
  # AC4 part 2: the BELT is applied to the invocation this component owns. Note both cases
  # above ran against a stub `cargo` shell FUNCTION, which the assignment prefix cannot
  # affect — so they prove the STRIP alone carries the correctness, and this assert covers
  # the belt. The prefix must stay a BARE assignment: `env CARGO_TERM_COLOR=never …` execs an
  # external binary and would bypass the stub, silently turning both cases above into real
  # cargo builds. That is why this assert pins the exact spelling.
  if grep -q 'CARGO_TERM_COLOR=never cargo test --package cqlite-core --features arrow' "$tmp/arrow_guard.sh"; then
    ok "AC4 part 2: the arrow-parity-guard cargo invocation carries the CARGO_TERM_COLOR=never belt (and the two cases above, run against a stub cargo the prefix cannot reach, show the STRIP is what actually carries it)"
  else
    bad "AC4 part 2: the arrow-parity-guard cargo invocation is missing the CARGO_TERM_COLOR=never belt"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (A5) The DERIVED SIBLING must be collected. _ansi_stripped_log writes
#      `<log>.ansi-stripped` beside its input, so any caller that cleans only the
#      original leaks a world-readable file per run — and the sibling name is derivable
#      from the log name, which for a minutes-long run is a narrow but real TOCTOU
#      symlink window. Both shipped callers must use a private 0700 mktemp -d and remove
#      it wholesale (issue #3400).
# ─────────────────────────────────────────────────────────────────────────────────────
if grep -qE 'mktemp -d .*agent-gate-cli' "$GATE" && grep -qE 'trap "rm -rf .*_cli_tmp' "$GATE"; then
  ok "A5: cli-tests logs into a private mktemp -d and removes it wholesale (the .ansi-stripped siblings go with it)"
else
  bad "A5: cli-tests does not use a private mktemp -d + rm -rf trap — the derived .ansi-stripped siblings leak into TMPDIR"
fi
if grep -qE '^ *log1=\$\(mktemp\) && log2=\$\(mktemp\)' "$GATE"; then
  bad "A5: cli-tests is back to two bare mktemp files in the shared tmp"
else
  ok "A5: cli-tests no longer creates two bare mktemp files in the shared tmp"
fi
if grep -qE 'tmpd=\$\(mktemp -d\)' "$GATE" && grep -qE 'rm -rf "\$tmpd"' "$GATE"; then
  ok "A5: run_arrow_parity_guard_cmd normalises inside a private mktemp -d and removes it (consistent with the cli-tests caller)"
else
  bad "A5: run_arrow_parity_guard_cmd is not using a private mktemp -d — the two callers disagree"
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
#       A FLOOR, deliberately not an equality: the count is measured, not declared, and it
#       legitimately RISES when a parser is added (PR #3403 adds several) or when the token
#       set widens. What must never happen is it reaching zero, and the lint's own
#       empty-subject FAIL covers that. The floor is the 4 sites hand-enumerated on main
#       (the lint measures 5 — the extra is the `sed -E` that extracts the target name from
#       the `Running tests/` line, a distinct parse expression the hand count folded in).
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

# ─────────────────────────────────────────────────────────────────────────────────────
# (B12) CONTENT vs VALUE: naming a stripped path is NOT reading it (roborev B1, #3400).
#       `done <<< "$src"` references a stripped source and still reads NOTHING — a
#       here-string feeds the loop one line of FILENAME, so the parser consumes no cargo
#       output at all and reports clean. The lint must classify the redirect KIND and
#       judge each on its own terms, with an unclassifiable shape FAILing rather than
#       falling through to permissive. Every accepted shape gets a positive fixture and
#       every rejected shape a negative one, driven from one table so neither side can be
#       quietly dropped.
# ─────────────────────────────────────────────────────────────────────────────────────
# plant_shape <name> <expect: pass|fail> <body-line...>  — wraps the body in a function
# whose source variable IS stripped, so strippedness is never the reason for the verdict.
plant_shape() {
  local name="$1" expect="$2"; shift 2
  local f="$tmp/shape_$name.sh"
  {
    echo 'myguard() {'
    echo '  local src'
    echo '  src=$(_ansi_stripped_log "$1") || return 1'
    printf '%s\n' "$@"
    echo '}'
  } >"$f"
  run_lint "$f"
  if [ "$expect" = pass ]; then
    if [ "$lint_rc" -eq 0 ]; then
      ok "B12 shape '$name': ACCEPTED (reads the stripped log's CONTENTS)"
    else
      bad "B12 shape '$name': expected ACCEPT, got rc=$lint_rc — $lint_out"
    fi
  else
    if [ "$lint_rc" -ne 0 ]; then
      ok "B12 shape '$name': REJECTED (names a stripped path without reading it, or reads a raw one)"
    else
      bad "B12 shape '$name': expected REJECT, the lint PASSED it — a shape that consumes no cargo output must never green"
    fi
  fi
}

# ── ACCEPTED: the redirect/operand actually delivers the file's contents ──
plant_shape direct_redirect pass \
  '  while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done < "$src"'
plant_shape procsub_cat pass \
  '  while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done < <(cat "$src")'
plant_shape herestring_cmdsub pass \
  '  while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done <<< "$(cat "$src")"'
plant_shape operand_of_reader pass \
  '  sed -n "s/^test result: ok\. \([0-9]*\) passed.*/\1/p" "$src"'
plant_shape reader_with_redirect pass \
  '  grep -c "test result:" < "$src"'

# ── REJECTED: every one of these references a STRIPPED path and still reads no log ──
# THE B1 DEFECT ITSELF. Before the fix the lint accepted this because the here-string
# operand named a stripped variable; the loop reads one line of filename.
plant_shape herestring_bare_path fail \
  '  while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done <<< "$src"'
plant_shape procsub_echo fail \
  '  while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done < <(echo "$src")'
plant_shape reader_herestring fail \
  '  grep -c "test result:" <<< "$src"'
plant_shape echo_piped fail \
  '  echo "$src" | grep -c "test result:"'
plant_shape printf_piped fail \
  '  printf "%s\n" "$src" | sed -n "s/^test result: ok\. \([0-9]*\).*/\1/p"'
# ...and the two pre-existing rejection classes, re-asserted through the same table.
plant_shape pipe_fed fail \
  '  cat "$src" | while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done'
plant_shape raw_positional fail \
  '  while IFS= read -r line; do' \
  '    case "$line" in *"Running tests/"*) echo hit ;; esac' \
  '  done < "$1"'

# The B1 rejection must name the CONTENT-vs-VALUE cause, not a generic "raw source" —
# an accurate diagnosis is what makes the finding actionable rather than confusing.
run_lint "$tmp/shape_herestring_bare_path.sh"
if printf '%s' "$lint_out" | grep -q 'PATH-VALUED value'; then
  ok "B12: the here-string rejection names the PATH-VALUED cause (not a generic raw-source message)"
else
  bad "B12: the here-string rejection does not explain that the matched value is a path: $lint_out"
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (B13) TOKEN COVERAGE: the detector's pattern list and its documented claim must be the
#       same set (roborev B2). The header once claimed `error:` and a general
#       `running N tests` while the code carried neither, so a raw parser keyed on those
#       was invisible to the lint — and the affirmative `N/N` line still printed, because
#       other sites kept the count nonzero. That is a hole shaped exactly like coverage.
#
#       Each token gets BOTH directions, and both are load-bearing:
#         * STRIPPED read  -> must PASS with 1/1, which is the only proof the token was
#           DETECTED as a parse site at all;
#         * RAW read       -> must FAIL, which proves it is ENFORCED.
#       The raw direction alone would not discriminate: an undetected token yields ZERO
#       sites, and zero sites is also a FAIL (for a different reason).
# ─────────────────────────────────────────────────────────────────────────────────────
# token_case <name> <literal cargo text to plant>
token_case() {
  local name="$1" text="$2"
  local sf="$tmp/tok_${name}_stripped.sh" rf="$tmp/tok_${name}_raw.sh"
  {
    echo 'myguard() {'
    echo '  local src'
    echo '  src=$(_ansi_stripped_log "$1") || return 1'
    echo "  grep -Fc \"$text\" \"\$src\""
    echo '}'
  } >"$sf"
  {
    echo 'myguard() {'
    echo "  grep -Fc \"$text\" \"\$1\""
    echo '}'
  } >"$rf"
  run_lint "$sf"
  local det_rc=$lint_rc det_out=$lint_out
  run_lint "$rf"
  if [ "$det_rc" -eq 0 ] && printf '%s' "$det_out" | grep -q '^cargo-output-parsers: 1/1 ' \
     && [ "$lint_rc" -ne 0 ]; then
    ok "B13 token '$name' ($text): DETECTED as a parse site (1/1 when stripped) and ENFORCED (reds when raw)"
  else
    bad "B13 token '$name' ($text): stripped rc=$det_rc out='$det_out'; raw rc=$lint_rc — the token is either not in the detector's set or not enforced"
  fi
}

token_case test_result        'test result:'
token_case running_tests      'Running tests/'
token_case running_unittests  'Running unittests src/lib.rs'
token_case running_generic    'Running target/debug/deps/x'
token_case doc_tests          'Doc-tests'
token_case running_n_tests    'running 7 tests'
token_case compiling          'Compiling cqlite-core'
token_case finished           'Finished test profile'
token_case warning_colon      'warning:'
token_case warning_bracket    'warning['
token_case error_colon        'error:'
token_case error_bracket      'error[E0308]'
token_case nextest_summary    'Summary ['
token_case nextest_starting   'Starting 42 tests'
token_case nextest_pass       'PASS ['
token_case nextest_fail       'FAIL ['

# The English word WITHOUT its tool delimiter must NOT be a parse site — that is what keeps
# the widened set from firing on the gate's own prose (measured: the full set yields the same
# 5 sites on the shipped agent-gate.sh as the original narrow one). Zero sites -> FAIL, which
# is how a non-site is observed here.
for word in FAIL PASS Finished Compiling Starting Summary; do
  cat >"$tmp/tok_word_$word.sh" <<F
myguard() {
  grep -Fc "$word" "\$1"
}
F
  run_lint "$tmp/tok_word_$word.sh"
  if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'ZERO cargo-output parse sites'; then
    ok "B13 non-token '$word': the bare English word is NOT a parse site (delimiter-less, so it cannot fire on the gate's own prose)"
  else
    bad "B13 non-token '$word': expected ZERO parse sites, got rc=$lint_rc / '$lint_out' — the token set is matching prose"
  fi
done

# ─────────────────────────────────────────────────────────────────────────────────────
# (B14) CONSTRUCT ATTRIBUTION, not same-line matching (roborev B4 — the sharpest finding
#       of the round, and the one that changed the model).
#
#       The detector used to require a cargo token AND a match operator on the SAME LINE.
#       A `case` block splits them — `case "$line" in` carries the operator,
#       `*"Running tests/"*)` carries the token — so a RAW multi-line parse was INVISIBLE,
#       and the affirmative `N/N` line still printed off the single-line sites: a hole
#       shaped exactly like coverage. MEASURED before the fix: the mixed fixture below
#       reported `1/1 parse sites read from an ANSI-stripped source`, exit 0.
#
#       Note honestly what the empty-subject-set rule did and did not buy. Handed the raw
#       site ALONE, the pre-fix lint DID exit 1 — via `ZERO cargo-output parse sites`, the
#       right refusal for the wrong reason. That rule is what kept this from being a
#       silently undetectable hole, and it is NOT sufficient: it disappears the moment the
#       file contains any other site, which every real file does.
# ─────────────────────────────────────────────────────────────────────────────────────
# The exact reported shape: one stripped site, one raw `case`-based parse.
cat >"$tmp/f_b4_mixed.sh" <<'F'
good() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  while IFS= read -r line; do
    case "$line" in *"test result:"*) echo hit ;; esac
  done < "$src"
}
bad_raw() {
  local logfile="$1" seen=""
  while IFS= read -r line; do
    case "$line" in
      *"Running tests/"*) seen="x" ;;
    esac
  done < "$logfile"
}
F
run_lint "$tmp/f_b4_mixed.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'RAW source'; then
  ok "B14: a RAW multi-line \`case\` parse beside a stripped site is CAUGHT (was invisible: the same-line model reported 1/1 and exit 0)"
else
  bad "B14: the mixed fixture did not red — rc=$lint_rc / '$lint_out'"
fi
if printf '%s\n' "$lint_out" | grep -q '^cargo-output-parsers: '; then
  bad "B14: the lint printed an AFFIRMATIVE line for a file containing a raw site — a finding and a clean verdict must never appear together"
else
  ok "B14: no affirmative line is printed when a site reds (a FAIL never reads as coverage)"
fi
# ...and the raw site ALONE must red for the RIGHT reason now, not via the empty-subject net.
cat >"$tmp/f_b4_alone.sh" <<'F'
bad_raw() {
  local logfile="$1" seen=""
  while IFS= read -r line; do
    case "$line" in
      *"Running tests/"*) seen="x" ;;
    esac
  done < "$logfile"
}
F
run_lint "$tmp/f_b4_alone.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'RAW source' \
   && ! printf '%s' "$lint_out" | grep -q 'ZERO cargo-output parse sites'; then
  ok "B14: the raw \`case\` site alone reds as a RAW SOURCE, not via the empty-subject-set net (right reason, not just the right verdict)"
else
  bad "B14: the lone raw site did not red as a raw source — rc=$lint_rc / '$lint_out'"
fi
# A CASE block NOT inside a loop, over a raw variable, must red on the block SUBJECT.
cat >"$tmp/f_b4_case_nonloop.sh" <<'F'
myguard() {
  local out="$1"
  case "$out" in
    *"Doc-tests"*)
      echo hit
      ;;
  esac
}
F
run_lint "$tmp/f_b4_case_nonloop.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'block subject'; then
  ok "B14: a non-loop \`case\` block over a raw variable reds, naming the BLOCK SUBJECT"
else
  bad "B14: the non-loop case block did not red on its subject — rc=$lint_rc / '$lint_out'"
fi
# ...and the same shape over a STRIPPED read must PASS (so the rule is not reject-everything).
cat >"$tmp/f_b4_case_stripped.sh" <<'F'
myguard() {
  local src out
  src=$(_ansi_stripped_log "$1") || return 1
  out=$(cat "$src")
  case "$(cat "$src")" in
    *"Doc-tests"*)
      echo hit
      ;;
  esac
}
F
run_lint "$tmp/f_b4_case_stripped.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q '^cargo-output-parsers: 1/1 '; then
  ok "B14: the same non-loop \`case\` block over a stripped read is ACCEPTED (1/1) — positive control"
else
  bad "B14: the stripped non-loop case block did not pass — rc=$lint_rc / '$lint_out'"
fi
# A token on a CONTINUATION line of a multi-line command.
cat >"$tmp/f_b4_continuation.sh" <<'F'
myguard() {
  grep -q \
    "Running tests/" \
    "$1"
}
F
run_lint "$tmp/f_b4_continuation.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'RAW source'; then
  ok "B14: a token on a CONTINUATION line is attributed to its joined logical command and reds"
else
  bad "B14: the continuation-line fixture did not red — rc=$lint_rc / '$lint_out'"
fi
cat >"$tmp/f_b4_continuation_ok.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  grep -q \
    "Running tests/" \
    "$src"
}
F
run_lint "$tmp/f_b4_continuation_ok.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q '^cargo-output-parsers: 1/1 '; then
  ok "B14: the same continuation-line command over a stripped source is ACCEPTED (1/1) — positive control"
else
  bad "B14: the stripped continuation fixture did not pass — rc=$lint_rc / '$lint_out'"
fi
# A PATTERN HELD IN A VARIABLE: the scanner does not follow variables, and REFUSES rather
# than guessing in either direction. The refusal text must be distinct from every other cause.
cat >"$tmp/f_b4_var_pattern.sh" <<'F'
myguard() {
  local pat="Running tests/"
  grep -q "$pat" "$1"
}
F
run_lint "$tmp/f_b4_var_pattern.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'UNRESOLVED (cargo token held in a variable)'; then
  ok "B14: a pattern held in a VARIABLE is REFUSED with its own named cause (not silently skipped, not falsely accused of a raw read)"
else
  bad "B14: the variable-held pattern was not refused with a distinct cause — rc=$lint_rc / '$lint_out'"
fi
# The three refusal/failure causes must be TEXTUALLY DISTINCT from the empty-subject-set FAIL,
# so a pasted summary can never confuse "could not classify one site" with "found no sites".
run_lint "$tmp/f_empty.sh"
empty_out="$lint_out"
run_lint "$tmp/f_b4_var_pattern.sh"
if printf '%s' "$empty_out" | grep -q 'ZERO cargo-output parse sites' \
   && ! printf '%s' "$lint_out" | grep -q 'ZERO cargo-output parse sites' \
   && ! printf '%s' "$empty_out" | grep -q 'UNRESOLVED'; then
  ok "B14: the UNRESOLVED refusal and the EMPTY-SUBJECT-SET FAIL are textually distinct (neither message appears in the other's output)"
else
  bad "B14: the refusal and empty-subject-set diagnostics overlap — the two causes could be confused in a pasted summary"
fi
# The mention classification must survive the model change: a quoted message argument on a
# continuation line of a non-matching command is DATA. Measured on the shipped gate: four
# `emit_summary … "error: …"` lines. Reproduced here so the property is pinned, not incidental.
cat >"$tmp/f_b4_message_arg.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  emit_summary ERROR \
    "delta-anchor: none" \
    "error: anchor summary RESULT is not PASS — cannot anchor a delta re-cert"
  grep -c "test result:" "$src"
}
F
run_lint "$tmp/f_b4_message_arg.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q '^cargo-output-parsers: 1/1 '; then
  ok "B14: a quoted MESSAGE argument on a continuation line is DATA, not a parse site (1/1, the real parse only)"
else
  bad "B14: the message-argument fixture was misjudged — rc=$lint_rc / '$lint_out'"
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (B15) THE INVERSION: classify the VALUE, do not enumerate the read forms
#       (roborev C1 + C2, consolidated — and B1 was the same class one round earlier).
#
#       B1 was `done <<< "$src"`. C1 is `grep tok <<< "$(_ansi_stripped_log "$log")"`. C2 is
#       a non-loop `case "$src" in`. Three spellings, ONE conceptual error: treating "names a
#       stripped path" as "reads stripped content". The class count across rounds went 2, 2 —
#       not decreasing — so the MODEL was wrong, not the predicates, and enumerating read
#       forms could only ever find the next spelling.
#
#       MEASURED before the inversion: this fixture pair reported
#       `2/2 parse sites read from an ANSI-stripped source`, exit 0. Both parses run against
#       one line of FILENAME.
#
#       The rule now: matching a cargo token against a PATH-VALUED value is an error in EVERY
#       syntactic position, with no exceptions. A false PASS therefore requires misclassifying
#       a PATH as CONTENT, which is a far smaller surface than "did we enumerate every read
#       form", and everything unresolved lands on the loud side.
# ─────────────────────────────────────────────────────────────────────────────────────
cat >"$tmp/f_c1_inline_herestring.sh" <<'F'
c1_guard() {
  local log="$1"
  grep -c "test result:" <<< "$(_ansi_stripped_log "$log")"
}
F
run_lint "$tmp/f_c1_inline_herestring.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'PATH-VALUED value'; then
  ok "B15/C1: an INLINE-helper here-string (\`<<< \"\$(_ansi_stripped_log …)\"\`) reds as PATH-VALUED (was approved: it parsed the returned FILENAME)"
else
  bad "B15/C1: the inline-helper here-string was not rejected — rc=$lint_rc / '$lint_out'"
fi

cat >"$tmp/f_c2_case_path.sh" <<'F'
c2_guard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  case "$src" in
    *"Running tests/"*)
      echo hit
      ;;
  esac
}
F
run_lint "$tmp/f_c2_case_path.sh"
if [ "$lint_rc" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'PATH-VALUED value'; then
  ok "B15/C2: a non-loop \`case \"\$src\"\` over a PATH-valued variable reds (was approved because \$src came from the helper — but it holds a filename)"
else
  bad "B15/C2: the non-loop case over a path-valued variable was not rejected — rc=$lint_rc / '$lint_out'"
fi

# The two indirections the lead named: the file IS dereferenced, but the MATCH happens at a
# later read this scanner does not follow. They must land UNRESOLVED — loud — never PASS.
cat >"$tmp/f_c2_exec_fd.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  exec 3< "$src"
  while IFS= read -r line <&3; do
    case "$line" in *"Running tests/"*) echo hit ;; esac
  done
  exec 3<&-
}
F
run_lint "$tmp/f_c2_exec_fd.sh"
if [ "$lint_rc" -ne 0 ]; then
  ok "B15: \`exec 3< \"\$src\"\` + a read from fd 3 does NOT pass (the match is at an indirection this scanner does not follow, so it is loud)"
else
  bad "B15: the exec-fd shape PASSED — an unfollowed indirection must never look like a clean read"
fi

cat >"$tmp/f_c2_mapfile.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  mapfile -t lines < "$src"
  printf '%s\n' "${lines[@]}" | grep -c "Running tests/"
}
F
run_lint "$tmp/f_c2_mapfile.sh"
if [ "$lint_rc" -ne 0 ]; then
  ok "B15: \`mapfile -t lines < \"\$src\"\` then matching the array does NOT pass (same unfollowed-indirection rule)"
else
  bad "B15: the mapfile shape PASSED — an unfollowed indirection must never look like a clean read"
fi

# POSITIVE CONTROLS — the inversion must not turn into reject-everything, and these three are
# the shapes the SHIPPED gate actually uses. NOTE on `grep <token> "$src"` with no loop: the
# lead's fixture list asked for this to FAIL or be UNRESOLVED, and it must NOT. `grep PATTERN
# FILE` DEREFERENCES its operand — it matches the file's BYTES, not its name — and that is
# precisely the shipped arrow-parity-guard parse (`sed -n '…' "$stripped"`). Making it red
# would red the gate of record for a correct construct. Dereference is the hinge of the whole
# model: a path in a READ position yields CONTENT; the same path in a VALUE position is C1/C2.
cat >"$tmp/f_c2_grep_operand.sh" <<'F'
myguard() {
  local src
  src=$(_ansi_stripped_log "$1") || return 1
  grep -c "test result:" "$src"
}
F
run_lint "$tmp/f_c2_grep_operand.sh"
if [ "$lint_rc" -eq 0 ] && printf '%s' "$lint_out" | grep -q '^cargo-output-parsers: 1/1 '; then
  ok "B15 control: \`grep <token> \"\$src\"\` with no loop is ACCEPTED — grep DEREFERENCES its operand, and this is the shipped arrow-parity parse shape"
else
  bad "B15 control: \`grep <token> \"\$src\"\` was rejected — that reds the shipped gate for a construct that reads the file correctly (rc=$lint_rc / '$lint_out')"
fi

cat >"$tmp/f_c2_content_var.sh" <<'F'
myguard() {
  local src out
  src=$(_ansi_stripped_log "$1") || return 1
  out=$(cat "$src")
  case "$out" in
    *"Running tests/"*)
      echo hit
      ;;
  esac
}
F
run_lint "$tmp/f_c2_content_var.sh"
if [ "$lint_rc" -eq 0 ]; then
  ok "B15 control: a CONTENT-valued variable (\`out=\$(cat \"\$src\")\`) matched by a non-loop \`case\` is ACCEPTED — the value is the log's text, not its name"
else
  bad "B15 control: the content-valued variable was rejected — rc=$lint_rc / '$lint_out'"
fi

# The SAME variable name, one letter of provenance apart: proof the verdict follows the VALUE'S
# PROVENANCE and not the spelling of the construct.
cat >"$tmp/f_c2_provenance.sh" <<'F'
path_valued() {
  local v
  v=$(_ansi_stripped_log "$1") || return 1
  case "$v" in *"Doc-tests"*) echo hit ;; esac
}
F
run_lint "$tmp/f_c2_provenance.sh"
pv_rc=$lint_rc
cat >"$tmp/f_c2_provenance_ok.sh" <<'F'
content_valued() {
  local p v
  p=$(_ansi_stripped_log "$1") || return 1
  v=$(cat "$p")
  case "$v" in *"Doc-tests"*) echo hit ;; esac
}
F
run_lint "$tmp/f_c2_provenance_ok.sh"
if [ "$pv_rc" -ne 0 ] && [ "$lint_rc" -eq 0 ]; then
  ok "B15: the SAME \`case \"\$v\"\` construct reds when \$v is PATH-valued and passes when it is CONTENT-valued — the verdict follows the value's provenance, not the syntax"
else
  bad "B15: provenance does not decide the verdict (path-valued rc=$pv_rc, content-valued rc=$lint_rc)"
fi

echo
printf 'passed=%d failed=%d\n' "$PASSES" "$FAILS"
if [ "$FAILS" -gt 0 ]; then
  echo "FAIL: test_cargo_output_parsers self-test"
  exit 1
fi
echo "PASS: test_cargo_output_parsers self-test"
