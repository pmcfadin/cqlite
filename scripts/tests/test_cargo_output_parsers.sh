#!/usr/bin/env bash
# test_cargo_output_parsers.sh — the #3400 cargo-output colour guard behaviour test.
#
# SUBJECT: every cargo-output parse site in scripts/agent-gate.sh, exercised as CODE.
#
# WHY IT EXISTS. #1699 introduced `_ansi_stripped_log` and routed three guards through it
# with NO test of its own, so the mechanism the gate's zero-test protection now depends on
# was pinned by nothing: any later refactor could unroute a guard and every suite would stay
# green, because a guard that parses nothing reports nothing wrong. This file is that pin,
# and it measures BEHAVIOUR against the shipped code — each guard is EXTRACTED FROM
# scripts/agent-gate.sh and run, never re-implemented here.
#
# THE PINNED DEFECT, RED-first, in BOTH directions:
#   * VACUOUS PASS (site 1). The PRE-#1699 shape of the cli-tests zero-tests guard
#     (`check_no_unexpected_zero_tests`) is run against the SAME zero-test cargo log twice —
#     once coloured, once plain — and asserted to exit 0 on the coloured one and 1 on the
#     plain one. Colour alone flips a real failure into a silent pass. The CURRENT shape must
#     then exit 1 on BOTH, still PASS when the zero-test target is on the allowed-zero list
#     (proving it recovered the target NAME from coloured text rather than merely erroring),
#     and FAIL closed on an unreadable log, an empty log, a log holding no recognisable
#     output, and a target observed but never judged.
#   * FALSE RED (site 4, A6). `check_declared_test_targets_observed` greps the RAW log for
#     the literal `Running tests/`, which under colour does not exist — so its observed set
#     comes back empty and every declared target is reported unobserved on a healthy run.
#     #3400 routes it through the helper; the control proves a genuinely absent target still
#     FAILs, so the fix restored observation rather than disabling the check.
#   * BELT (site 5, A4). `run_arrow_parity_guard_cmd` is driven against a stub `cargo` that
#     emits coloured output and IGNORES CARGO_TERM_COLOR, which is what shows the STRIP —
#     not the belt — carries the correctness.
#
# A STRUCTURAL LINT over the parse sites was built here and DESCOPED (#3400): its own
# false-PASS count rose across review rounds (2, 2, 3) and two of the last round's three
# defects were inside the two preceding fix rounds, so it was removed under the precedent
# CLAUDE.md records for #3229's `census-exclusion:` key — a guard with known documented
# false-PASSes is worse than no guard, because it invites reliance it cannot support.
# Mechanization is deferred to #3499; the rule stands as doctrine. What remains here is
# behaviour measured against real code, which is why it survived the descope.
#
# FIXTURE PROVENANCE. Every escape sequence below is a REAL ESC byte injected via
# `printf '\033'` — never a hand-typed two-character `\x1b` string, which would make the
# whole suite test nothing. The sequences are transcribed from a `cat -v` capture of real
# `cargo test` output under CARGO_TERM_COLOR=always REDIRECTED TO A FILE (issue #3400
# oracle run), including the decisive detail: the reset lands BETWEEN the status word and
# the payload (`Running<ESC>[0m tests/empty.rs`), so the literal `Running tests/` never
# appears. `running N tests` and `test result:` are libtest text and were measured
# byte-identical under `always` and `never`, so they are written here WITHOUT escapes —
# that asymmetry is itself part of the fixture, and it is why routing the arrow guard is
# belt rather than a bug fix.
#
# AC3 is asserted both ways: behaviourally (a pipe-fed while-read loop over the very same
# log silently exits 0 because its `bad` accumulator dies with the subshell, while the
# redirect-fed loop exits 1) and structurally (the shipped guard reads via
# `done < "$_parse_src"` and contains no pipe into a while-read).
#
# Hermetic: temp dir only. No cargo, no datasets, no network, no gh. Runnable standalone.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE="$REPO_ROOT/scripts/agent-gate.sh"

PASSES=0
FAILS=0
ok()  { printf 'ok   - %s\n' "$1"; PASSES=$((PASSES + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAILS=$((FAILS + 1)); }

if [ ! -r "$GATE" ]; then
  echo "FAIL: required file not readable: $GATE"
  exit 1
fi

# FAIL CLOSED, never `exit 0` -- but be precise about WHICH path this protects, because an earlier
# version of this comment claimed a gate-level hole that does not exist.
#
#   * Through the FULL gate it protects NOTHING: run_tooling_tests checks python3 itself and returns
#     `status=SKIP` for the whole component BEFORE invoking this file, and that `tooling-tests: SKIP`
#     IS visible in the SUMMARY. Repo policy there is deliberate ("no python3 -> SKIP, loud, never a
#     silent PASS") and covers all 15 tests that component runs, so it is not this file's business.
#     Deferred, with the SKIP line's missing detail filed separately.
#   * Through `--delta` it protects a REAL vacuous pass: run_delta_shell_selftests (agent-gate.sh)
#     executes changed `scripts/tests/*.sh` directly with NO python3 guard of its own and maps the
#     exit status straight to PASS/FAIL, so an `exit 0` here would have produced
#     `shell-selftests: PASS` having verified nothing.
#   * On DIRECT invocation it tells a developer on a python3-less box the truth instead of a pass.
#
# So: `exit 1`, and the message names what went unverified rather than claiming the property holds.
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 unavailable, so the guard extraction below could not run and the ANSI-handling"
  echo "      behaviour of check_no_unexpected_zero_tests is UNVERIFIED (issue #3400). Refusing to"
  echo "      exit 0: this file's subject is guards that pass without measuring, so a silent skip"
  echo "      here would be an instance of the defect under test. Remedy: install python3."
  exit 1
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
extract_rc=0
# `|| extract_rc=$?` and not a following `extract_rc=$?` line: under `set -e` a
# failing extraction kills the script outright, so the fail-closed branch below
# would be UNREACHABLE and the run would end with no diagnostic and no summary.
# Demonstrated on a mutated scratch tree, not reasoned about.
python3 - "$GATE" "$tmp/current_guard.sh" <<'PY' || extract_rc=$?
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


# Both are TOP-LEVEL functions in the shipped gate: #1699 promoted the guard out of the
# cli-tests `bash -c` blob so its three callers share ONE implementation, so there is no
# blob escaping left to unwind here. Anchoring at column zero is what makes that true --
# a `^  ` anchor would silently extract nothing and the fail-closed checks below would fire.
helper = extract(r'^_ansi_stripped_log\(\) \{', r'^\}')
guard = extract(r'^check_no_unexpected_zero_tests\(\) \{', r'^\}')
if not helper.strip() or 'sed -E' not in helper:
    print('EXTRACT-FAIL: _ansi_stripped_log', file=sys.stderr); sys.exit(2)
if not guard.strip() or 'while IFS= read' not in guard:
    print('EXTRACT-FAIL: check_no_unexpected_zero_tests', file=sys.stderr); sys.exit(2)
# COMMENT-BLIND (roborev job 146, Low). A bare substring test is satisfied by a COMMENT
# that merely NAMES the helper -- and run_arrow_parity_guard_cmd's comment block names it
# three times -- so the check would report the guard routed after the call was deleted. An
# artifact DESCRIBING the routing would BECOME the evidence for it: #3312's shape, and the
# same reason this repo's alternate-executor scan is `^[^#]*--test`.
if not any('_ansi_stripped_log' in l for l in guard.split('\n')
           if not l.lstrip().startswith('#')):
    print('EXTRACT-FAIL: the extracted guard has no NON-COMMENT call to _ansi_stripped_log '
          '- it parses the raw log and this suite would certify the defect', file=sys.stderr)
    sys.exit(2)
open(out, 'w', encoding='utf-8').write(helper + '\n\n' + guard + '\n')
PY
if [ "$extract_rc" -ne 0 ]; then
  bad "extraction of the shipped guard + _ansi_stripped_log from agent-gate.sh FAILED (rc=$extract_rc) — cannot certify the current shape"
else
  ok "extracted _ansi_stripped_log + check_no_unexpected_zero_tests from the shipped agent-gate.sh"
  if bash -n "$tmp/current_guard.sh" 2>/dev/null; then
    ok "the extracted guard is syntactically valid bash (so the cases below run the real thing)"
  else
    bad "the extracted guard is not valid bash — the shipped function shape changed and this extraction no longer captures it"
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

  # ORPHAN DETECTION, main's round-26 property, pinned here because nothing else pins it.
  # A `Running` banner with NO following `test result:` line means the target was OBSERVED and
  # never JUDGED -- a truncated log, a killed binary, or a result line the parse missed -- and
  # the guard would otherwise return success having silently skipped exactly the target it was
  # asked about. Fixture: the coloured log cut off mid-way, so the last banner has no result.
  # The line is located ANSI-INSENSITIVELY (`Running.*tests/`) because in the coloured log the
  # literal `Running tests/` does not exist -- that asymmetry IS this issue, and counting with
  # the literal here would cut at the wrong place and red for emptiness instead.
  cut_at=$(grep -nE 'Running.*tests/' "$tmp/zero-colour.log" | sed -n '2p' | cut -d: -f1)
  awk -v c="$cut_at" 'NR <= c' "$tmp/zero-colour.log" >"$tmp/orphan.log"
  orphan_banners=$(grep -cE 'Running.*tests/' "$tmp/orphan.log" || true)
  # `empty` is ALLOWED on purpose: the fixture still holds the zero-test `empty` target, so
  # without this the guard would red on the unexpected-zero condition and the assertion would
  # pass whether or not orphan detection exists. Excusing it leaves the DANGLING SECOND BANNER
  # as the only possible cause.
  orphan_rc=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/orphan.log" empty >/dev/null 2>&1; exit $? ) || orphan_rc=$?
  if [ "$orphan_rc" -ne 0 ]; then
    ok "orphan: a log ending on a banner with no following result FAILs — a target observed and not judged is not a measurement"
  else
    bad "orphan: the guard PASSed a log whose last target was observed and never judged"
  fi
  # ...and the fixture must genuinely BE the orphan case, or the assertion above proves nothing:
  # two banners, and the LAST line must be the second banner (nothing follows it to judge it).
  # Keyed on the last line specifically, not on a tail WINDOW: the three-line window ending at
  # the banner still contains the FIRST target's result line, so a window test reported the
  # fixture malformed while it was in fact exactly right.
  if [ "${orphan_banners:-0}" -eq 2 ] && tail -n 1 "$tmp/orphan.log" | grep -qE 'Running.*tests/'; then
    ok "orphan: the fixture is genuinely the dangling case ($orphan_banners banners, the last line IS the second banner)"
  else
    bad "orphan: the fixture is not the dangling case (banners=${orphan_banners:-0}) — the case above would red for the wrong reason"
  fi
  # POSITIVE CONTROL, so orphan detection is not a reject-everything rule: the SAME log with the
  # dangling banner's result restored must PASS. Red there and green here isolates the missing
  # result line as the only thing that changed.
  cp "$tmp/orphan.log" "$tmp/orphan-closed.log"
  printf '\nrunning 1 test\ntest one ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n' >>"$tmp/orphan-closed.log"
  orphan_ctrl_rc=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/orphan-closed.log" empty >/dev/null 2>&1; exit $? ) || orphan_ctrl_rc=$?
  if [ "$orphan_ctrl_rc" -eq 0 ]; then
    ok "orphan (control): the SAME fixture PASSes once the dangling banner gets its result — so the red above is PROVEN to be the orphan, not the fixture"
  else
    bad "orphan (control): the closed fixture reds too (rc=$orphan_ctrl_rc) — the case above cannot be attributed to the orphan"
  fi

  # AC3, structurally: the shipped guard must be REDIRECTION-fed, not pipe-fed.
  # FIXED strings, not regexes (see the note on the blob asserts below): `grep` here is ugrep,
  # and a NEGATIVE assert whose pattern silently fails to match passes VACUOUSLY — which is the
  # defect class this file exists for. The positive half is proven to discriminate by re-running
  # it against a copy with the needle removed.
  if grep -Fq -- 'done < "$_parse_src"' "$tmp/current_guard.sh" \
     && ! grep -Fq -- '| while IFS= read' "$tmp/current_guard.sh"; then
    sed 's/done < "\$_parse_src"//' "$tmp/current_guard.sh" >"$tmp/current_guard_mutated.sh"
    if grep -Fq -- 'done < "$_parse_src"' "$tmp/current_guard_mutated.sh"; then
      bad "AC3 (structural): the redirection check does NOT discriminate — it still matches a guard with that line removed"
    else
      ok "AC3 (structural): the shipped guard reads via REDIRECTION (done < \"\$_parse_src\"), not through a pipe into while-read — and the check is proven to fire"
    fi
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
arrow_extract_rc=0
# `|| arrow_extract_rc=$?` and not a following `arrow_extract_rc=$?` line: under `set -e` a
# failing extraction kills the script outright, so the fail-closed branch below
# would be UNREACHABLE and the run would end with no diagnostic and no summary.
# Demonstrated on a mutated scratch tree, not reasoned about.
python3 - "$GATE" "$tmp/arrow_guard.sh" <<'ARROWPY' || arrow_extract_rc=$?
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
# COMMENT-BLIND: this function's own comment block names the helper three times, so a bare
# substring test greens on a guard whose CALL has been deleted (roborev job 146, Low).
if not any('_ansi_stripped_log' in l for l in guard.split('\n')
           if not l.lstrip().startswith('#')):
    print('EXTRACT-FAIL: run_arrow_parity_guard_cmd has no NON-COMMENT call to '
          '_ansi_stripped_log', file=sys.stderr)
    sys.exit(2)
open(out, 'w', encoding='utf-8').write(helper + '\n\n' + guard + '\n')
ARROWPY
if [ "$arrow_extract_rc" -ne 0 ]; then
  bad "extraction of run_arrow_parity_guard_cmd (+ helper) from agent-gate.sh FAILED (rc=$arrow_extract_rc) — the arrow-parity-guard parse is uncertified"
else
  ok "extracted run_arrow_parity_guard_cmd from the shipped agent-gate.sh (it calls _ansi_stripped_log)"
  # Stub `cargo`: emits a COLOURED cargo log. $STUB_PASSED controls the reported count, and
  # $STUB_COLOUR_RESULT controls whether the `test result:` PAYLOAD LINE is coloured too.
  #
  # That second knob is DELIBERATELY COUNTERFACTUAL and is the discrimination probe. Real
  # libtest does not colour that line -- measured byte-identical under always and never, which
  # is exactly why routing this site is BELT and not a bug fix. But that same fact makes the
  # two cases below UNABLE TO TELL whether the guard parsed the stripped copy or the raw
  # capture: with an uncoloured payload both parse identically, so reverting the routing would
  # keep them green (roborev job 146, Low). Colouring the payload asks the question the belt
  # exists to answer -- "if this line WERE coloured, would the parse survive?" -- and makes the
  # answer observable. Cases (a)/(b) keep the MEASURED uncoloured payload, so the no-false-red
  # and vacuous-skip properties are still asserted against real cargo behaviour.
  cat >"$tmp/arrow_stub.sh" <<STUB
cargo() {
  printf '%s     Running%s tests/issue_1495.rs (target/debug/deps/x-1)\n' '${ESC}[1m${ESC}[92m' '${ESC}[0m'
  printf '\nrunning %s tests\n\n' "\$STUB_PASSED"
  if [ -n "\${STUB_COLOUR_RESULT:-}" ]; then
    printf '%stest result%s: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n' '${ESC}[1m${ESC}[92m' '${ESC}[0m' "\$STUB_PASSED"
  else
    printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n' "\$STUB_PASSED"
  fi
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
  # above ran against a stub `cargo` shell FUNCTION whose coloured output is HARDCODED and
  # which never reads CARGO_TERM_COLOR — so they prove the STRIP alone carries the
  # correctness, independently of the variable, and this assert covers the belt. (A bare
  # assignment prefix IS visible inside a shell function, contrary to an earlier version of
  # this comment; what makes those cases independent of the belt is the stub ignoring the
  # variable, not the prefix failing to reach it.) The prefix must stay a BARE assignment: `env CARGO_TERM_COLOR=never …` execs an
  # external binary and would bypass the stub, silently turning both cases above into real
  # cargo builds. That is why this assert pins the exact spelling.
  # THE DISCRIMINATION PROOF (roborev job 146). Everything above is satisfied by a guard that
  # parses the RAW capture, because the payload it parses is uncoloured either way -- so on its
  # own this section certifies the routing it claims to test only by ASSERTING it structurally,
  # which is the shape this whole file exists to reject. So: build a MUTANT of the extracted
  # guard whose _ansi_stripped_log is the IDENTITY (it hands back the raw path -- exactly what
  # "revert the routing" means), and run BOTH against a log whose `test result:` line IS
  # coloured. Real guard PASSes, mutant FAILs, same input: the difference is the routing and
  # nothing else. If someone deletes the strip, this pair reds.
  #
  # The mutant is built by REPLACING the helper definition, not by editing the guard body, so
  # the guard under test stays byte-identical to the shipped one.
  mut_rc=0
  # `|| mut_rc=$?` and not a following `mut_rc=$?` line: under `set -e` a
  # failing extraction kills the script outright, so the fail-closed branch below
  # would be UNREACHABLE and the run would end with no diagnostic and no summary.
  # Demonstrated on a mutated scratch tree, not reasoned about.
  python3 - "$tmp/arrow_guard.sh" "$tmp/arrow_guard_raw.sh" <<'MUTPY' || mut_rc=$?
import re, sys
src, out = sys.argv[1], sys.argv[2]
text = open(src, encoding='utf-8').read()
lines = text.split('\n')
start = end = None
for i, l in enumerate(lines):
    if re.match(r'^_ansi_stripped_log\(\) \{', l):
        start = i
        for j in range(i + 1, len(lines)):
            if re.match(r'^\}', lines[j]):
                end = j
                break
        break
if start is None or end is None:
    print('MUTATE-FAIL: no _ansi_stripped_log definition to replace', file=sys.stderr)
    sys.exit(2)
identity = ['_ansi_stripped_log() {', '  printf %s "$1"', '}']
open(out, 'w', encoding='utf-8').write('\n'.join(lines[:start] + identity + lines[end + 1:]))
MUTPY
  if [ "$mut_rc" -ne 0 ]; then
    bad "A4 (discrimination): could not build the identity-strip mutant (rc=$mut_rc) — the routing is asserted but not measured"
  else
    arrow_real_coloured=0
    ( set +e; . "$tmp/arrow_guard.sh"; STUB_PASSED=3 STUB_COLOUR_RESULT=1; export STUB_COLOUR_RESULT; . "$tmp/arrow_stub.sh"; run_arrow_parity_guard_cmd >/dev/null 2>&1; exit $? ) || arrow_real_coloured=$?
    arrow_mut_coloured=0
    ( set +e; . "$tmp/arrow_guard_raw.sh"; STUB_PASSED=3 STUB_COLOUR_RESULT=1; export STUB_COLOUR_RESULT; . "$tmp/arrow_stub.sh"; run_arrow_parity_guard_cmd >/dev/null 2>&1; exit $? ) || arrow_mut_coloured=$?
    if [ "$arrow_real_coloured" -eq 0 ] && [ "$arrow_mut_coloured" -ne 0 ]; then
      ok "A4 (discrimination): on a log whose 'test result:' line IS coloured, the SHIPPED guard PASSes and an identity-strip mutant of it FAILs — so the cases above are measuring the ROUTING, not just the fixture"
    else
      bad "A4 (discrimination): expected shipped=0 / identity-strip-mutant=nonzero on a coloured result line, got shipped=$arrow_real_coloured / mutant=$arrow_mut_coloured — reverting the strip would NOT red this suite, so its green does not evidence the routing"
    fi
    # ...and the mutant must not be a reject-everything strawman: on the MEASURED uncoloured
    # payload it must still PASS, which is what makes the red above attributable to the colour.
    arrow_mut_plain=0
    ( set +e; . "$tmp/arrow_guard_raw.sh"; STUB_PASSED=3; unset STUB_COLOUR_RESULT; . "$tmp/arrow_stub.sh"; run_arrow_parity_guard_cmd >/dev/null 2>&1; exit $? ) || arrow_mut_plain=$?
    if [ "$arrow_mut_plain" -eq 0 ]; then
      ok "A4 (discrimination control): the SAME mutant PASSes on the uncoloured payload — so its red above is the COLOUR, not a broken mutant"
    else
      bad "A4 (discrimination control): the mutant reds on the uncoloured payload too (rc=$arrow_mut_plain) — it is broken, and the case above proves nothing"
    fi
  fi
  if grep -Fq -- 'CARGO_TERM_COLOR=never cargo test --package cqlite-core --features arrow' "$tmp/arrow_guard.sh"; then
    ok "AC4 part 2: the arrow-parity-guard cargo invocation carries the CARGO_TERM_COLOR=never belt (and the two cases above, run against a stub cargo that IGNORES CARGO_TERM_COLOR, show the STRIP is what actually carries it)"
  else
    bad "AC4 part 2: the arrow-parity-guard cargo invocation is missing the CARGO_TERM_COLOR=never belt"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (A6) The FALSE-RED direction, at the site #1699 left unrouted:
#      check_declared_test_targets_observed. It greps the log for the literal
#      `Running tests/`, which under CARGO_TERM_COLOR=always does not exist — so its
#      `observed` set comes back EMPTY and every declared target is reported
#      unobserved-and-UNEXPLAINED on a perfectly healthy run. Opposite direction from the
#      zero-tests guard (a red, not a green) and just as wrong. RED-first, then the shipped
#      shape, then a control proving it can still detect a genuinely absent target.
# ─────────────────────────────────────────────────────────────────────────────────────
# The declared set: two targets, neither declaring required-features, so an unobserved one
# has no excuse available and lands in the FAIL bucket. Both DO run in the fixture log.
DECL_META="$(printf 'empty\t\nfoo\t')"

cat >"$tmp/prefix_declared.sh" <<'PREFIXDECL'
# The pre-#3400 OBSERVATION half of check_declared_test_targets_observed, verbatim: it
# greps the RAW logfile. Reduced to the observation + verdict, which is the part colour
# reaches; the excusal machinery is unrelated to this defect and is not reproduced.
prefix_declared() {
  local logfile="$1" meta="$2"
  local observed declared=0 seen=0 bad="" tname rf
  observed=" $(grep -oE 'Running tests/[^[:space:]]+\.rs' "$logfile" \
    | sed -E 's#^Running tests/(.*)\.rs$#\1#' | sort -u | tr '\n' ' ') "
  while IFS=$'\t' read -r tname rf; do
    [ -n "$tname" ] || continue
    declared=$((declared + 1))
    case "$observed" in
      *" $tname "*) seen=$((seen + 1)); continue ;;
    esac
    bad="$bad $tname"
  done <<< "$meta"
  [ -z "$bad" ]
}
PREFIXDECL

pd_colour=0
( set +e; . "$tmp/prefix_declared.sh"; prefix_declared "$tmp/zero-colour.log" "$DECL_META" >/dev/null 2>&1; exit $? ) || pd_colour=$?
pd_plain=0
( set +e; . "$tmp/prefix_declared.sh"; prefix_declared "$tmp/zero-plain.log" "$DECL_META" >/dev/null 2>&1; exit $? ) || pd_plain=$?
if [ "$pd_colour" -ne 0 ] && [ "$pd_plain" -eq 0 ]; then
  ok "A6 RED (pinned defect): the raw-log reconciliation FAILs on the COLOURED log and PASSes on the same log plain — a FALSE RED on a healthy run, caused by colour alone"
else
  bad "A6 RED (pinned defect): expected coloured=nonzero / plain=0, got coloured=$pd_colour / plain=$pd_plain — the fixture no longer reproduces the false-red direction, so the GREEN case below proves nothing"
fi

decl_extract_rc=0
# `|| decl_extract_rc=$?` and not a following `decl_extract_rc=$?` line: under `set -e` a
# failing extraction kills the script outright, so the fail-closed branch below
# would be UNREACHABLE and the run would end with no diagnostic and no summary.
# Demonstrated on a mutated scratch tree, not reasoned about.
python3 - "$GATE" "$tmp/declared_guard.sh" <<'DECLPY' || decl_extract_rc=$?
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
guard = extract(r'^check_declared_test_targets_observed\(\) \{', r'^\}')
if not helper.strip() or not guard.strip():
    print('EXTRACT-FAIL', file=sys.stderr); sys.exit(2)
# COMMENT-BLIND, for the same reason as the other two extractions (roborev job 146).
if not any('_ansi_stripped_log' in l for l in guard.split('\n')
           if not l.lstrip().startswith('#')):
    print('EXTRACT-FAIL: check_declared_test_targets_observed has no NON-COMMENT call to '
          '_ansi_stripped_log - it parses the raw log and reds every coloured run',
          file=sys.stderr)
    sys.exit(2)
open(out, 'w', encoding='utf-8').write(helper + '\n\n' + guard + '\n')
DECLPY
if [ "$decl_extract_rc" -ne 0 ]; then
  bad "A6: extraction of check_declared_test_targets_observed (+ helper) from agent-gate.sh FAILED (rc=$decl_extract_rc) — that parse site is uncertified"
else
  ok "A6: extracted check_declared_test_targets_observed from the shipped agent-gate.sh (it calls _ansi_stripped_log)"
  # GATE_SELF must be readable: the guard fails closed on it before reaching the parse, and
  # this file IS a real readable path, so the alternate-executor half is satisfiable.
  dg_colour=0
  ( set +e; . "$tmp/declared_guard.sh"; GATE_SELF="$GATE" check_declared_test_targets_observed "lane" "$tmp/zero-colour.log" " arrow " "$DECL_META" "" >/dev/null 2>&1; exit $? ) || dg_colour=$?
  dg_plain=0
  ( set +e; . "$tmp/declared_guard.sh"; GATE_SELF="$GATE" check_declared_test_targets_observed "lane" "$tmp/zero-plain.log" " arrow " "$DECL_META" "" >/dev/null 2>&1; exit $? ) || dg_plain=$?
  if [ "$dg_colour" -eq 0 ]; then
    ok "A6 GREEN: the SHIPPED reconciliation PASSes the COLOURED log — it recovered both target NAMES from coloured banners, so the false red is gone"
  else
    bad "A6 GREEN: the SHIPPED reconciliation exited $dg_colour on a healthy COLOURED log — every declared target is being reported unobserved because the literal 'Running tests/' is not there"
  fi
  if [ "$dg_plain" -eq 0 ]; then
    ok "A6 GREEN: it PASSes the plain log too (colour no longer changes the verdict)"
  else
    bad "A6 GREEN: it exited $dg_plain on the plain log"
  fi
  # A reconciliation that passes everything is not a fix. Control: a target that genuinely
  # never ran must still FAIL, on the COLOURED log — so the green above is discrimination,
  # not blanket acceptance.
  dg_absent=0
  ( set +e; . "$tmp/declared_guard.sh"; GATE_SELF="$GATE" check_declared_test_targets_observed "lane" "$tmp/zero-colour.log" " arrow " "$(printf 'empty\t\nnever_built\t')" "" >/dev/null 2>&1; exit $? ) || dg_absent=$?
  if [ "$dg_absent" -ne 0 ]; then
    ok "A6 (control): a declared target absent from the COLOURED log still FAILs — the fix restored observation, it did not disable the check"
  else
    bad "A6 (control): a target that never ran PASSed on the coloured log — the reconciliation now accepts anything"
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
if grep -Fq -- 'mktemp -d "${TMPDIR:-/tmp}/agent-gate-cli.XXXXXX"' "$GATE" \
   && grep -Fq -- 'rm -rf \"$_cli_tmp\"' "$GATE"; then
  ok "A5: cli-tests logs into a private mktemp -d and removes it wholesale (the .ansi-stripped siblings go with it)"
else
  bad "A5: cli-tests does not use a private mktemp -d + rm -rf trap — the derived .ansi-stripped siblings leak into TMPDIR"
fi
if grep -Fq -- 'log1=$(mktemp) && log2=$(mktemp)' "$GATE"; then
  bad "A5: cli-tests is back to two bare mktemp files in the shared tmp"
else
  ok "A5: cli-tests no longer creates two bare mktemp files in the shared tmp"
fi
if grep -Fq -- 'tmpd=$(mktemp -d)' "$GATE" && grep -Fq -- 'rm -rf "$tmpd"' "$GATE"; then
  ok "A5: run_arrow_parity_guard_cmd normalises inside a private mktemp -d and removes it (consistent with the cli-tests caller)"
else
  bad "A5: run_arrow_parity_guard_cmd is not using a private mktemp -d — the two callers disagree"
fi

# ─────────────────────────────────────────────────────────────────────────────────────
# (A7) THE #3625 CENSUS PARSERS ARE PARSE SITES TOO, so they belong in this registry —
#      the question this file exists to answer is "is EVERY cargo-output parse in the
#      shipped gate colour-immune", and a new one absent from it is a hole the whole
#      suite cannot see. Extracted from the shipped gate, like every other site here.
#
#      `Executable ` is a CARGO STATUS WORD and IS coloured (same placement as `Running`:
#      the reset lands between the word and the payload), so the raw parse counts ZERO —
#      and for the census a measured zero means VACUOUS, i.e. a FALSE RED on a healthy
#      `--no-run` lane. `test result:` is libtest's and carries no escapes, which is why
#      routing it is belt; it is routed anyway, for the same reason site 4 is.
#
#      The census's own state machine, the verdict coupling and the AC3 plant live in
#      scripts/tests/test_agent_gate_census.sh. What is asserted HERE is only the property
#      this file owns: colour immunity at the parse site.
# ─────────────────────────────────────────────────────────────────────────────────────
cen_rc=0
python3 - "$GATE" "$tmp/census_parsers.sh" <<'CENSUSPY' || cen_rc=$?
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
lt = extract(r'^_census_libtest_tally\(\) \{', r'^\}')
cp = extract(r'^_census_compile_tally\(\) \{', r'^\}')
meas = extract(r'^_census_measure\(\) \{', r'^\}')
for name, body, needle in (('_ansi_stripped_log', helper, 'sed -E'),
                           ('_census_libtest_tally', lt, 'test result:'),
                           ('_census_compile_tally', cp, 'Executable'),
                           ('_census_measure', meas, '_ansi_stripped_log')):
    if not body.strip() or needle not in body:
        print('EXTRACT-FAIL: %s' % name, file=sys.stderr)
        sys.exit(2)
# COMMENT-BLIND, for the reason recorded at the A2 extraction: a comment NAMING the helper
# would otherwise satisfy a substring test, and _census_measure's comment block names it.
if not any('_ansi_stripped_log' in l for l in meas.split('\n')
           if not l.lstrip().startswith('#')):
    print('EXTRACT-FAIL: _census_measure has no NON-COMMENT call to _ansi_stripped_log - '
          'it would parse the raw log and this suite would certify the defect',
          file=sys.stderr)
    sys.exit(2)
open(out, 'w', encoding='utf-8').write('\n\n'.join((helper, lt, cp)) + '\n')
CENSUSPY
if [ "$cen_rc" -ne 0 ]; then
  bad "A7: extraction of the #3625 census parsers from agent-gate.sh FAILED (rc=$cen_rc) — cannot certify their colour handling"
else
  ok "A7: extracted _census_libtest_tally + _census_compile_tally (and verified _census_measure routes through _ansi_stripped_log from a non-comment line)"
  # The escape bytes are the SAME real ones the provenance cases at the top of this file
  # already proved are real; ESC is that variable.
  {
    printf '%s  Executable%s unittests src/lib.rs (target/debug/deps/dw-1)\n' "${ESC}[1m${ESC}[92m" "${ESC}[0m"
    printf 'test result: ok. 4 passed; 0 failed; 0 ignored\n'
  } > "$tmp/census-colour.log"
  cen_raw=$( set +e; . "$tmp/census_parsers.sh"; _census_compile_tally "$tmp/census-colour.log" )
  cen_stripped=$( set +e; . "$tmp/census_parsers.sh"; s=$(_ansi_stripped_log "$tmp/census-colour.log"); _census_compile_tally "$s" )
  if [ "$cen_raw" = 0 ] && [ "$cen_stripped" = 1 ]; then
    ok "A7a: _census_compile_tally counts 0 'Executable' lines on the COLOURED log and 1 after _ansi_stripped_log — the strip carries the correctness (an unrouted call would report a healthy --no-run lane as having built nothing)"
  else
    bad "A7a: expected raw=0 stripped=1 from the census compile tally, got raw='$cen_raw' stripped='$cen_stripped'"
  fi
  cen_lt=$( set +e; . "$tmp/census_parsers.sh"; s=$(_ansi_stripped_log "$tmp/census-colour.log"); _census_libtest_tally "$s" )
  if [ "$cen_lt" = "4 1" ]; then
    ok "A7b: _census_libtest_tally reads libtest's own uncoloured tally through the same normalised source (4 passed across 1 result line)"
  else
    bad "A7b: expected '4 1' from the census libtest tally, got '$cen_lt'"
  fi
fi

echo
printf 'passed=%d failed=%d\n' "$PASSES" "$FAILS"
if [ "$FAILS" -gt 0 ]; then
  echo "FAIL: test_cargo_output_parsers self-test"
  exit 1
fi
echo "PASS: test_cargo_output_parsers self-test"
