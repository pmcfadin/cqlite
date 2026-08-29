#!/usr/bin/env bash
# test_cargo_output_parsers.sh — the #3400 cargo-output colour guard behaviour test.
#
# SUBJECT: the cargo-output parse sites in scripts/agent-gate.sh, exercised as CODE.
#
# THE PINNED DEFECT, RED-first. The PRE-FIX shape of the cli-tests zero-tests guard
# (`check_no_unexpected_zero_tests`, verbatim from main) is run against the SAME zero-test
# cargo log twice — once coloured, once plain — and asserted to exit 0 (a silent VACUOUS
# PASS) on the coloured one and 1 on the plain one. That is the defect, characterised and
# pinned so it cannot come back unnoticed. The CURRENT shape, EXTRACTED FROM THE SHIPPED
# scripts/agent-gate.sh, must then exit 1 on BOTH, still PASS when the zero-test target is
# on the allowed-zero list (proving it recovered the target NAME from coloured text), and
# FAIL closed on an unreadable log or a log in which it recognised no target banner at all.
#
# A STRUCTURAL LINT over the parse sites was built here and DESCOPED (#3400): its own
# false-PASS count rose across review rounds (2, 2, 3) and two of the last round's three
# defects were inside the two preceding fix rounds, so it was removed under the precedent
# CLAUDE.md records for #3229's `census-exclusion:` key — a guard with known documented
# false-PASSes is worse than no guard, because it invites reliance it cannot support.
# Mechanization is deferred; the rule stands as doctrine. What remains here is behaviour
# measured against real code, which is why it survived the descope.
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

# FAIL CLOSED, never `exit 0`. The component that runs this file sees only an exit code, so a
# dependency-absent `exit 0` is reported as PASS and the SKIP is invisible in the SUMMARY -- the
# exact "a guard reported OK having measured nothing" shape this file exists to prevent, and the
# same rule the sibling tooling-tests selftest states as "never silent PASS". The verdict names
# what could not be verified rather than claiming the property holds.
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
( set +e; . "$tmp/prefix_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" 2 >/dev/null 2>&1; exit $? ) || prefix_rc_colour=$?
prefix_rc_plain=0
( set +e; . "$tmp/prefix_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-plain.log" 2 >/dev/null 2>&1; exit $? ) || prefix_rc_plain=$?

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
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" 2 >/dev/null 2>&1; exit $? ) || cur_rc_colour=$?
  cur_rc_plain=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-plain.log" 2 >/dev/null 2>&1; exit $? ) || cur_rc_plain=$?

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
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" 2 empty >/dev/null 2>&1; exit $? ) || cur_rc_allowed=$?
  if [ "$cur_rc_allowed" -eq 0 ]; then
    ok "positive control: the SHIPPED guard PASSes the coloured log when 'empty' is on the allowed-zero list (so it parsed the target NAME out of coloured text, not just 'something is wrong')"
  else
    bad "positive control: the SHIPPED guard exited $cur_rc_allowed (expected 0) with 'empty' allowed — it is reporting a failure it cannot attribute, i.e. it did not recover the target name from the coloured banner"
  fi

  # Fail-closed control: an unreadable log must FAIL, never pass having parsed nothing.
  cur_rc_missing=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/does-not-exist.log" 2 >/dev/null 2>&1; exit $? ) || cur_rc_missing=$?
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
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/no-banners.log" 2 >/dev/null 2>&1; exit $? ) || zero_banner_rc=$?
  if [ "$zero_banner_rc" -ne 0 ]; then
    ok "C3: a NON-EMPTY log with no recognised target banners FAILs (the guard judged no target, so it measured nothing)"
  else
    bad "C3: the guard PASSed a log it found no target banners in — it reported OK having judged nothing, the exact vacuous pass #3400 removes"
  fi
  empty_log_rc=0
  : >"$tmp/empty.log"
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/empty.log" 2 >/dev/null 2>&1; exit $? ) || empty_log_rc=$?
  if [ "$empty_log_rc" -ne 0 ]; then
    ok "C3: an EMPTY log FAILs (zero iterations is zero measurement, not a clean bill of health)"
  else
    bad "C3: the guard PASSed an EMPTY log — zero loop iterations reported as success"
  fi
  # ...and the positive control that keeps C3 from being a reject-everything rule: the real
  # coloured log has banners, so it still reds for the RIGHT reason (a zero-test target) and
  # still PASSes when that target is allowed.
  c3_ctrl_rc=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" 2 empty >/dev/null 2>&1; exit $? ) || c3_ctrl_rc=$?
  if [ "$c3_ctrl_rc" -eq 0 ]; then
    ok "C3: a log WITH banners still PASSes when its zero-test target is allowed (the banner assert is not a blanket reject)"
  else
    bad "C3: the banner assert reds a legitimate log — rc=$c3_ctrl_rc"
  fi

  # E1: `>= 1` BANNER IS PARTIAL CREDIT. A log TRUNCATED after the first banner satisfies a
  # "at least one target was parsed" test while a later zero-test target is simply ABSENT from
  # the file — the same vacuous pass as an empty log, just needing a partial write instead of
  # no write. The guard must compare its parsed banner count against the number of REQUESTED
  # --test targets. This fixture is the coloured log cut off mid-way: readable, non-empty,
  # normalises fine, ONE banner recognised, TWO requested.
  truncated_rc=0
  # Cut just before the SECOND target banner, so the fixture holds exactly one. The line is
  # located ANSI-INSENSITIVELY (`Running.*tests/`) because in the coloured log the literal
  # `Running tests/` does not exist — that asymmetry is this whole issue, and counting with the
  # literal here is how the first version of this fixture measured 0 banners and would have red
  # for emptiness rather than for the count.
  cut_at=$(grep -nE 'Running.*tests/' "$tmp/zero-colour.log" | sed -n '2p' | cut -d: -f1)
  awk -v c="$cut_at" 'NR < c' "$tmp/zero-colour.log" >"$tmp/truncated.log"
  trunc_banners=$(grep -cE 'Running.*tests/' "$tmp/truncated.log" || true)
  # `empty` is ALLOWED here on purpose: the truncated fixture still holds the zero-test `empty`
  # target, so without this the guard would red on the unexpected-zero-test condition and the
  # assertion below would pass whether or not the banner-count check exists. Excusing `empty`
  # leaves the COUNT mismatch (1 banner recognised, 2 requested) as the SOLE possible cause.
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/truncated.log" 2 empty >/dev/null 2>&1; exit $? ) || truncated_rc=$?
  if [ "$truncated_rc" -ne 0 ]; then
    ok "E1: a log TRUNCATED after the first banner FAILs — the guard judged fewer targets than were requested, which is not a measurement of its subject"
  else
    bad "E1: the guard PASSed a truncated log — one early banner bought credit for targets absent from the file"
  fi
  # ...and the fixture must actually BE the partial case, or the assertion above proves nothing.
  if [ "${trunc_banners:-0}" -ge 1 ] && [ "${trunc_banners:-0}" -lt 2 ] && [ -s "$tmp/truncated.log" ]; then
    ok "E1: the truncated fixture is genuinely PARTIAL (non-empty, $trunc_banners of 2 banners) — so the red above is the COUNT, not emptiness"
  else
    bad "E1: the truncated fixture is not the partial case (banners=${trunc_banners:-0}, size=$( [ -s "$tmp/truncated.log" ] && echo nonempty || echo empty )) — the case above would red for the wrong reason"
  fi
  # POSITIVE CONTROL, so the discrimination is MEASURED and not merely argued by the comment
  # above: the SAME fixture with the requested count set to the 1 banner it actually holds must
  # PASS. Red there and green here isolates the count as the only thing that changed, which is
  # the property under test. Without this, "the red above is the COUNT" is an assertion about
  # the fixture rather than evidence about the guard.
  trunc_ctrl_rc=0
  ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/truncated.log" 1 empty >/dev/null 2>&1; exit $? ) || trunc_ctrl_rc=$?
  if [ "$trunc_ctrl_rc" -eq 0 ]; then
    ok "E1 (control): the SAME truncated fixture PASSes when the requested count is 1 — so the red above is PROVEN to be the count mismatch, not the fixture"
  else
    bad "E1 (control): the truncated fixture reds even when the requested count matches its 1 banner (rc=$trunc_ctrl_rc) — the case above cannot be attributed to the count"
  fi
  # The expected-count argument is itself fail-closed: a guard told to expect nothing is back to
  # accepting whatever the log happened to contain.
  for badcount in "" 0 abc; do
    argrc=0
    ( set +e; . "$tmp/current_guard.sh"; check_no_unexpected_zero_tests "Pass 1" "$tmp/zero-colour.log" "$badcount" empty >/dev/null 2>&1; exit $? ) || argrc=$?
    if [ "$argrc" -ne 0 ]; then
      ok "E1: an expected-target count of '${badcount:-<empty>}' is refused (an empty subject set cannot certify anything)"
    else
      bad "E1: the guard accepted an expected-target count of '${badcount:-<empty>}'"
    fi
  done

  # E1, the other half: a `tee` FAILURE must be surfaced. The callers pipe cargo through tee and
  # historically read only ${PIPESTATUS[0]}, so a full disk or a killed tee left a TRUNCATED log
  # while cargo itself exited 0 — feeding the case above. Demonstrated, then asserted structurally.
  set +e
  true 2>&1 | tee "$tmp/no-such-dir/out.log" >/dev/null 2>&1
  # ONE assignment for the whole array. `ps0=${PIPESTATUS[0]}` is itself a command and RESETS
  # PIPESTATUS, so a second statement reading [1] gets an unbound/empty value — which is how
  # this very test caught the same mistake in the gate.
  e1_ps=("${PIPESTATUS[@]}")
  set -e
  ps0=${e1_ps[0]}; ps1=${e1_ps[1]}
  if [ "$ps0" -eq 0 ] && [ "$ps1" -ne 0 ]; then
    ok "E1 (behavioural): a failing \`tee\` leaves PIPESTATUS[0]=0 and PIPESTATUS[1]=$ps1 — reading only [0] is exactly how a truncated log passes for a successful run"
  else
    bad "E1 (behavioural): expected PIPESTATUS[0]=0 with a nonzero [1], got [0]=$ps0 [1]=$ps1 — the premise did not reproduce on this shell"
  fi
  # STRUCTURAL asserts over the cli-tests blob, as FIXED STRINGS with a discrimination proof.
  #
  # Not a style preference. `grep` on this fleet is ugrep, and a BRE pattern containing `$((`
  # did not match a line it visibly occurs in — so the first version of these asserts was a
  # QUOTING PUZZLE whose verdict I could not reproduce by hand. An assert that greens because
  # its pattern is broken is the exact false-PASS shape this whole issue is about, one level up.
  # So: `grep -F` only, and every needle is then re-checked against a copy of the blob with that
  # needle REMOVED — if the predicate still passes, it is not measuring anything.
  cli_blob_src=$(awk "/cli-tests\\) run_component cli-tests bash -c/{f=1} f{print} f&&/^ *;;\$/{exit}" "$GATE")
  if [ -n "$cli_blob_src" ]; then
    ok "extracted the cli-tests dispatch blob from the shipped gate for structural assertions"
  else
    bad "could not extract the cli-tests dispatch blob — every structural assertion below would pass or fail for no reason"
  fi

  # blob_needs <label> <count> <fixed-string> — the string must occur at least <count> times in
  # the blob, AND the same test must FAIL on a blob with the string stripped out.
  blob_needs() {
    local label="$1" want="$2" needle="$3" got mutated_got
    got=$(printf '%s\n' "$cli_blob_src" | grep -Fc -- "$needle" || true)
    mutated_got=$(printf '%s\n' "${cli_blob_src//"$needle"/}" | grep -Fc -- "$needle" || true)
    if [ "${got:-0}" -ge "$want" ] && [ "${mutated_got:-0}" -eq 0 ]; then
      ok "E1 (structural): $label — found ${got} occurrence(s), and the check is PROVEN to fire (0 on a blob with it removed)"
    elif [ "${got:-0}" -lt "$want" ]; then
      bad "E1 (structural): $label — expected >= $want occurrence(s) of the required text, found ${got:-0}"
    else
      bad "E1 (structural): $label — the check does NOT discriminate: it still reports ${mutated_got} on a blob with the text removed, so its green means nothing"
    fi
  }

  blob_needs "both passes capture the WHOLE PIPESTATUS array in one assignment" 2 \
    '_ps=("${PIPESTATUS[@]}")'
  blob_needs "both passes read the tee half from that captured array" 2 \
    'tee_rc=${_ps[1]}'
  blob_needs "both passes fail closed on a nonzero tee status" 2 \
    'tee_rc" -ne 0'
  blob_needs "Pass 1 derives its expected target count from def_flags" 1 \
    'def_expected=$(( ${#def_flags[@]} / 2 ))'
  blob_needs "Pass 2 derives its expected target count from ws_flags" 1 \
    'ws_expected=$(( ${#ws_flags[@]} / 2 ))'
  # One assert PER PASS, each labelled with only what it measures. A single "each pass" assert
  # keyed on Pass 1's call site alone would let Pass 2 stop passing its derived count while the
  # green kept claiming both -- the stated-scope-exceeds-measured-scope shape this issue is about.
  blob_needs "Pass 1 passes its derived count to the guard" 1 \
    '"$log1" "$def_expected"'
  blob_needs "Pass 2 passes its derived count to the guard" 1 \
    '"$log2" "$ws_expected"'
  # The needle is matched against the RAW blob text, where the single quotes are still in this
  # file's `bash -c` escaped form — so key on the quote-free payload and let the NEGATIVE assert
  # below (no `trap "rm -rf`) establish that the quoting is the deferred kind.
  blob_needs "the EXIT trap removes the private dir by an explicitly-terminated path" 1 \
    'rm -rf -- "$_cli_tmp"'

  # The one-assignment rule, pinned as its own NEGATIVE property: a second statement reading
  # PIPESTATUS is a check that silently never fires, so no caller may reintroduce that shape.
  if printf '%s\n' "$cli_blob_src" | grep -Fq -- 'rc=${PIPESTATUS[0]}; tee_rc=${PIPESTATUS[1]}'; then
    bad "E1: a cli-tests pass reads PIPESTATUS across TWO statements — the first assignment resets the array, so the tee status is empty and its check never fires"
  else
    ok "E1: no cli-tests pass reads PIPESTATUS across two statements (the first assignment would reset the array)"
  fi
  # ...and the DOUBLE-QUOTED trap form must not return: it bakes the literal path into a string
  # bash re-parses, so a TMPDIR carrying a $ or a backtick both RUNS injected syntax and SKIPS
  # cleanup. Both consequences measured on this box before the fix.
  if printf '%s\n' "$cli_blob_src" | grep -Fq -- 'trap "rm -rf'; then
    bad "E1: the cli-tests EXIT trap is DOUBLE-QUOTED again — a TMPDIR containing \$ or a backtick can alter the cleanup command"
  else
    ok "E1: the cli-tests EXIT trap is not double-quoted (expansion is deferred to when the trap runs)"
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
  if grep -Fq -- 'CARGO_TERM_COLOR=never cargo test --package cqlite-core --features arrow' "$tmp/arrow_guard.sh"; then
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
if grep -Fq -- 'mktemp -d "${TMPDIR:-/tmp}/agent-gate-cli.XXXXXX"' "$GATE" \
   && grep -Fq -- 'rm -rf -- "$_cli_tmp"' "$GATE"; then
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

echo
printf 'passed=%d failed=%d\n' "$PASSES" "$FAILS"
if [ "$FAILS" -gt 0 ]; then
  echo "FAIL: test_cargo_output_parsers self-test"
  exit 1
fi
echo "PASS: test_cargo_output_parsers self-test"
