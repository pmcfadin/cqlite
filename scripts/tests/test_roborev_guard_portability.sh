#!/usr/bin/env bash
# PORTABILITY GUARD for the roborev review-guard code path (issue #3296).
#
# WHY THIS FILE EXISTS. `scripts/tests/test_roborev_review_guard.sh` gates a merge (it runs
# inside the gate's `roborev-lints` component, in --lite AND in the full gate of record). At
# `origin/main` it reported `passed: 553  failed: 7` on macOS and `failed: 0` on Linux: three
# GNU-vs-BSD utility differences in the TEST's own scaffolding, not in the wrapper it guards.
# Every hosted CI lane is Linux, so NO hosted signal can see that class — it is found by
# whoever next tries to certify anything locally, as a failure attributed to THEIR diff.
#
# So the regression coverage cannot be "run it on macOS": it must be a DIFFERENTIAL that runs
# on every platform. Two mechanisms, both here:
#
#   (1) STRUCTURAL — the GNU-only constructs that caused #3296 (and their nearest relatives)
#       cannot be reintroduced into the roborev code path. Each pattern carries a POSITIVE
#       CONTROL: a sample violation the pattern must DETECT. A scanner whose regex silently
#       matches nothing is the vacuous pass this repo keeps finding (CLAUDE.md: "never derive a
#       pass from the ABSENCE of a bad signal"), so every pattern is affirmatively measured.
#
#   (2) BEHAVIOURAL under BSD SHIMS — a `sed` that consumes -i's next argument as the backup
#       suffix (BSD semantics) and a `paste` that usage()-errors with no file operand are put
#       first on PATH, and the guard test's OWN helpers — extracted VERBATIM from it, never
#       copied — are exercised against them. Each shim is itself controlled: it must first be
#       shown to REPRODUCE the reported defect, or the differential below proves nothing.
#
# Deliberately TARGETED: it exercises the new helpers and the mutated asserts, NOT all 581
# guard-test cases under shims (that would double a --lite component's runtime for no extra
# information — the full-suite run under both shims was recorded once, by hand, on #3296).
#
# SAME DOCTRINE, DIFFERENT SCOPE as scripts/tests/test_agent_gate_tree_portability.sh (#2926,
# after #2914): behavioural BSD shims for the covered paths plus a static lint for the
# uncovered ones, every lint rule proved discriminating. That file lints the gate's
# tree-integrity functions; this one lints the roborev review-guard path. macOS is a
# FIRST-CLASS host here — the gate carries `Darwin) … taskpolicy` branches and declares a
# /bin/bash 3.2 floor — so this file also avoids bash-4-only constructs and never expands a
# possibly-empty array under `set -u` (a 3.2 unbound-variable abort).
#
# AUTHORITATIVE SOURCES for the three BSD behaviours emulated here (a CQLite file is never
# authority for another program's behaviour):
#   sed -i takes a REQUIRED argument   Apple text_cmds sed/main.c:
#                                      getopt(argc, argv, "EI:ae:f:i:lnru")   <- the `i:`
#   paste needs a file operand         Apple text_cmds paste/paste.c + FreeBSD
#                                      usr.bin/paste/paste.c:
#                                        argc -= optind; argv += optind;
#                                        if (*argv == NULL) usage();
#                                      usage() -> stderr + exit(1), i.e. EMPTY stdout
#   cut is NOT implicated              FreeBSD usr.bin/cut/cut.c: with no operand it reads
#                                      stdin (`if (*argv) ... else fcn(stdin, "stdin")`)
#
# Run standalone:   bash scripts/tests/test_roborev_guard_portability.sh
# Or via the gate:  scripts/agent-gate.sh --lite   (roborev-lints component)
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
GUARD="$SCRIPT_DIR/test_roborev_review_guard.sh"
GATE="$SCRIPT_DIR/../agent-gate.sh"
FLOW_DIR="$SCRIPT_DIR/../flow"

PASS=0
FAIL=0
# SKIPPED is COUNTED and printed in the tally, never absorbed into PASS. A check that could not
# run has no verdict to give: reporting it as a pass is the vacuous pass itself, and reporting it
# as a failure turns a legitimately stripped runner into a red gate (which is #3296's own defect
# one level down). So it is its own third state, loud in the output and named in the tally.
SKIPPED=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
skip() { printf 'SKIP - %s\n' "$1"; SKIPPED=$((SKIPPED + 1)); }

if [ ! -f "$GUARD" ]; then
  printf 'FAIL - the guard test is not at %s — nothing to keep portable\n' "$GUARD"
  exit 1
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/roborev-portability.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# ===========================================================================
# (1) STRUCTURAL: no GNU-only construct in the roborev code path.
#
# The table is EXPLICIT rather than a generic "GNU long option" sweep: a generic sweep
# false-positives on git/gh (whose long options are portable) and a lint agents learn to
# waive is worse than no lint. Add an entry when a new divergence is actually found.
# Comment-only lines are stripped before scanning, so prose ABOUT a construct (including
# this file's own citations, and the guard test's `sed -i` explanation) is not a violation.
# ===========================================================================
SCAN_FILES=(
  "$GUARD"
  "$FLOW_DIR/roborev-review.sh"
  "$FLOW_DIR/roborev-review-checks.sh"
  "$FLOW_DIR/roborev-review-oracles.sh"
  "$FLOW_DIR/roborev-job-facts.py"
)

# Three parallel arrays: the ERE, why it is not portable, and a sample violation the ERE
# MUST detect (the positive control that keeps the pattern honest).
CONSTRUCT_RE=(); CONSTRUCT_WHY=(); CONSTRUCT_SAMPLE=()
add_construct() { CONSTRUCT_RE+=("$1"); CONSTRUCT_WHY+=("$2"); CONSTRUCT_SAMPLE+=("$3"); }

# The patterns whose controls below name them directly are held in NAMED variables rather
# than referenced by table INDEX: an index reference silently retargets when a row is inserted
# above it, which would leave a control asserting about a different rule than it names.
#
# THE OPTION RUN. `-i` does not have to sit next to `sed`: `sed -e 's/a/b/' -i file` reaches the
# same BSD argument-consuming `-i` (it eats `file` as the backup suffix) while an "adjacent
# flags only" regex sees nothing. So the rules skip over an arbitrary run of intervening
# tokens — BUT that run stops dead at a shell metacharacter (| ; & < > ( )), which is what
# keeps `sed 's/x/y/' f | grep -i foo` from being read as a sed `-i`: the `-i` there belongs to
# a DIFFERENT command. This is deliberately a LINE-ORIENTED approximation of shell parsing and
# not a tokeniser; the residual it leaves is stated in full below the table.
_OPT_RUN='([[:space:]]+[^[:space:]|;&<>()]+)*'
# `--in-place` is matched with an OPTIONAL `=SUFFIX` (#3296 round 8): GNU accepts
# `--in-place=.bak`, and requiring whitespace after the option name made that spelling — a long
# option BSD sed does not have AT ALL — invisible to this rule.
RE_SED_INPLACE='(^|[^[:alnum:]_-])sed'"$_OPT_RUN"'[[:space:]]+(-i|--in-place(=[^[:space:]|;&<>()]*)?)([[:space:]]|$)'
# BUNDLED CLUSTERS ending in `i` — the hole the bare `-i` rule above leaves open. BSD getopt
# processes `-Ei` as `-E` then `-i`, and `i` is declared WITH A REQUIRED ARGUMENT (Apple
# text_cmds sed/main.c: `getopt(argc, argv, "EI:ae:f:i:lnru")`), so with nothing left in the
# cluster it consumes the NEXT ARGV entry as the backup suffix — byte for byte the #3296
# defect, reached by a spelling the bare-`-i` regex never sees. `-i.bak` (an ATTACHED suffix)
# is portable and deliberately NOT matched: both seds read it the same way.
#
# The letters BEFORE the trailing `i` are restricted to the ARGUMENT-FREE sed options
# (BSD `n E r a l u`, plus GNU's `s` and `z`). A cluster containing an ARGUMENT-TAKING option
# never reaches an in-place flag at all: in `sed -fi input`, getopt gives `-f` the argument
# "i" (the script file), so there is no `-i` and flagging it was a false positive. Same for
# `-ei`, and for BSD's `-I`/`-i`, which take arguments themselves.
RE_SED_INPLACE_CLUSTER='(^|[^[:alnum:]_-])sed'"$_OPT_RUN"'[[:space:]]+-[nEralusz]+i([[:space:]]|$)'
# PASTE WITH NO FILE OPERAND, in every spelling that reaches BSD's `if (*argv == NULL) usage();`:
#   paste -sd,            bundled flags, nothing after them
#   paste -d ,            the delimiter argument SEPARATED from -d (consumed as the option's
#                         argument by getopt, so it is not an operand either)
#   paste -sd, < input    a REDIRECTION is not an operand — BSD still usage()-errors, and the
#                         `<` is why the option run above excludes redirection characters
# The `-d`-with-separated-argument pair is matched as ONE unit, because a regex cannot express
# "this bare token is the argument of the preceding option" any other way.
_PASTE_DARG='[[:space:]]+-[a-zA-Z]*d[[:space:]]+[^[:space:]|;)&<]+'
_PASTE_OPT='[[:space:]]+-[^[:space:]|;)&<]+'
_PASTE_REDIR='([[:space:]]*<[[:space:]]*[^[:space:]|;)&]+)?'
RE_PASTE_NO_OPERAND='(^|[^[:alnum:]_-])paste('"$_PASTE_DARG"'|'"$_PASTE_OPT"')*'"$_PASTE_REDIR"'[[:space:]]*($|\||\)|;|&)'

add_construct "$RE_SED_INPLACE" \
  "BSD sed's -i takes a REQUIRED suffix argument, so it eats the EXPRESSION and the edit never lands (#3296 cx28/cx29/cx28b/cx28c) — use the guard test's sed_inplace helper" \
  "  sed -i 's/a/b/' \"\$f\""
add_construct "$RE_SED_INPLACE_CLUSTER" \
  "a BUNDLED cluster ending in -i (-Ei, -ni, -nEi, …) reaches the SAME BSD argument-consuming -i as a bare -i, so the edit never lands — use sed_inplace" \
  "  sed -Ei 's/a/b/' \"\$f\""
add_construct '(^|[^[:alnum:]_-])sed[[:space:]]+-i("")' \
  'the empty-suffix spelling -i"" is GNU-only (BSD needs -i "" or no -i at all) — use sed_inplace' \
  '  sed -i"" -e s/a/b/ f'
add_construct "(^|[^[:alnum:]_-])sed[[:space:]]+-i('')" \
  "the empty-suffix spelling -i'' is GNU-only — use sed_inplace" \
  "  sed -i'' -e s/a/b/ f"
add_construct "$RE_PASTE_NO_OPERAND" \
  'a paste with NO FILE OPERAND is empty output + exit 1 on BSD (it usage()-errors instead of reading stdin) — pass an explicit `-`, or extract with awk (#3296 case (j2))' \
  '  order=$(grep -n x f | cut -d: -f2 | paste -sd,)'
add_construct '(^|[^[:alnum:]_-])readlink[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-f' \
  'readlink -f is absent from older BSD readlink — canonicalise with `cd "$p" && pwd -P` (which is what the wrapper does)' \
  '  p=$(readlink -f "$x")'
add_construct '(^|[^[:alnum:]_-])stat[[:space:]]+-c' \
  'stat -c is GNU-only (BSD spells it stat -f)' \
  '  n=$(stat -c %s "$f")'
add_construct '(^|[^[:alnum:]_-])grep[[:space:]]+-[a-zA-Z]*P([[:space:]]|$)' \
  'grep -P (PCRE) is GNU-only — BSD grep has no -P' \
  "  grep -P '\\\\d+' f"
add_construct '(^|[^[:alnum:]_-])date[[:space:]]+(-d[[:space:]]|--date)' \
  'date -d/--date is GNU-only (BSD date uses -r / -v / -j -f)' \
  '  t=$(date -d @1700000000)'
add_construct '(^|[^[:alnum:]_-])(sed|grep)[[:space:]]+-[a-zA-Z]*z([[:space:]]|$)' \
  'sed -z / grep -z (NUL-delimited records) are GNU-only — read `git … -z` output with a shell read loop or awk RS' \
  '  grep -z foo f'
add_construct '\-printf[[:space:]]' \
  'find -printf is GNU-only — use -exec or -print with a shell loop' \
  "  find . -printf '%p\\\\n'"
add_construct '(^|[^[:alnum:]_-])xargs[[:space:]]+(-[a-zA-Z]*r|--)' \
  'xargs -r (and GNU long options) are not in BSD xargs; BSD already skips an empty input line only with -0' \
  '  printf "" | xargs -r rm'
add_construct '(^|[^[:alnum:]_-])base64[[:space:]]+-w' \
  'base64 -w is GNU-only (BSD/macOS base64 has no wrap flag; use -b or fold)' \
  '  base64 -w0 <f'
add_construct '(^|[^[:alnum:]_-])timeout[[:space:]]+[0-9]' \
  'timeout(1) is NOT installed on stock macOS — guard it with `command -v timeout` or restructure' \
  '  timeout 30 some-command'
add_construct '(^|[^[:alnum:]_-])(mapfile|readarray)([[:space:]]|$)|declare[[:space:]]+-A|\$\{[A-Za-z_][A-Za-z_0-9]*,,\}' \
  'bash 4 only — stock macOS /bin/bash is 3.2, so mapfile/readarray/associative arrays/${v,,} can abort the script outright' \
  '  mapfile -t arr <f'

if [ "${#CONSTRUCT_RE[@]}" -ne "${#CONSTRUCT_WHY[@]}" ] ||
  [ "${#CONSTRUCT_RE[@]}" -ne "${#CONSTRUCT_SAMPLE[@]}" ]; then
  bad 'structural: the construct table arrays are not the same length — some pattern has no reason or no positive control'
fi

# ---------------------------------------------------------------------------
# STATED RESIDUAL (an undocumented hole is worse than no guard, because it invites reliance it
# cannot support). This scanner is line-oriented ERE matching over backslash-JOINED logical
# lines. It is deliberately NOT a shell tokeniser: a bash re-implementation of shell word
# splitting would be a second implementation whose own correctness is only knowable by
# differential testing against the first, which is the failure mode CLAUDE.md records for the
# deleted `census-exclusion:` predictor (a false-PASS count that GREW across review rounds).
# So these spellings are KNOWN NOT COVERED, by choice:
#
#   1. A shell metacharacter INSIDE QUOTES within the option run, e.g.
#        sed -e 's/a|b/c/' -i f
#      The run stops at the `|` because nothing here knows it is quoted. Un-quoted, the same
#      `|` really would end the command, and reading it as one is what prevents the
#      `sed … | grep -i …` false positive — the two cases are indistinguishable line-wise.
#   2. The command name reached through a VARIABLE or an alias: `$SED -i f`, `"${SED}" -i f`.
#      (`command sed -i f` and `env sed -i f` ARE caught — the literal name is present.)
#   3. An operand supplied at RUNTIME: `paste -sd, $files` is flagged when $files is empty and
#      not otherwise; the scanner cannot know, so it treats a bare word as an operand (the
#      quiet direction — noise here would be worse than a miss, per the negative controls).
#   4. A construct built by string concatenation or eval (`cmd="sed -"; cmd="$cmd i"`).
#
# Each of these is a MISS, never a false green elsewhere; the behavioural shim differential in
# section (2) is the backstop that catches what the text scan cannot see.
# ---------------------------------------------------------------------------

# The scan body: code only. A construct named in a comment (this repo documents the ones it
# banned) is prose, not an invocation. A line carrying `portability-lint-allow` is exempt —
# the repo's existing escape-marker convention (`injection-lint-allow`, `perf-gate-allow`) —
# so a provably-safe or deliberately-BSD-emulating line has a route that is VISIBLE in the
# diff instead of forcing a rewrite of the lint.
#
# CONTINUATION JOINING: `sed \` + newline + `-i file` is ONE command that no line-oriented ERE
# can see, so logical lines are assembled before matching. The awk below is written to keep the
# output the SAME LENGTH as the input — a joined logical line is emitted at the position of its
# FIRST physical line and each consumed continuation becomes a blank — so `grep -n` line numbers
# still name the real line in the real file. Comment-only and marker-exempt lines are BLANKED
# rather than deleted for the same reason (deleting them, as the previous form did, shifted
# every number after them). A comment line is never joined to the next: `#` in shell comments to
# end of line, so a trailing backslash there continues nothing.
scan_hits() { # scan_hits <ere> <file>
  awk '
    function emit_logical(  i) {
      if (buf ~ /portability-lint-allow/) print ""; else print buf
      for (i = 0; i < pending; i++) print ""
      buf = ""; have = 0; pending = 0
    }
    BEGIN { buf = ""; have = 0; pending = 0 }
    {
      if (have == 0) {
        if ($0 ~ /^[[:space:]]*#/) { print ""; next }
        buf = $0; have = 1; pending = 0
      } else {
        line = $0
        sub(/^[[:space:]]+/, "", line)
        buf = buf " " line
        pending = pending + 1
      }
      if (buf ~ /\\$/) { sub(/\\$/, "", buf); next }
      emit_logical()
    }
    END { if (have == 1) emit_logical() }
  ' "$2" | grep -nE -- "$1" || true
}

for _ci in "${!CONSTRUCT_RE[@]}"; do
  _re="${CONSTRUCT_RE[$_ci]}"
  _why="${CONSTRUCT_WHY[$_ci]}"
  # POSITIVE CONTROL FIRST: the pattern must detect its own sample violation. Without this a
  # typo'd regex would report every file clean, forever.
  printf '%s\n' "${CONSTRUCT_SAMPLE[$_ci]}" >"$tmp/sample-$_ci.sh"
  if [ -n "$(scan_hits "$_re" "$tmp/sample-$_ci.sh")" ]; then
    ok "structural control: the pattern detects its sample violation (${CONSTRUCT_SAMPLE[$_ci]})"
  else
    bad "structural control: the pattern MATCHES NOTHING — it cannot detect '${CONSTRUCT_SAMPLE[$_ci]}', so its clean verdict below is vacuous ($_re)"
    continue
  fi
  _hits=""
  for _f in "${SCAN_FILES[@]}"; do
    if [ ! -f "$_f" ]; then
      bad "structural: scan target missing: $_f"
      continue
    fi
    _fh=$(scan_hits "$_re" "$_f")
    [ -n "$_fh" ] && _hits="$_hits $(basename "$_f"):${_fh%%$'\n'*}"
  done
  if [ -z "$_hits" ]; then
    ok "structural: the roborev code path is free of this construct — $_why"
  else
    bad "structural: GNU-only construct in the roborev code path ($_why):$_hits"
  fi
done

# POSITIVE CONTROLS for the CLUSTER rule beyond its table sample: every bundled spelling that
# reaches BSD's argument-consuming -i must be detected, not just the one in the table. `-Ei` is
# the form that evaded the bare-`-i` rule and is therefore asserted against BOTH rules: the old
# one must MISS it (that is the hole) and the new one must CATCH it.
printf '%s\n' \
  "  sed -Ei 's/a/b/' \"\$f\"" \
  "  sed -ni '\$p' \"\$f\"" \
  "  sed -nEi 's/a/b/' \"\$f\"" \
  "  sed -E -i 's/a/b/' \"\$f\"" >"$tmp/cluster-bad.sh"
# Asserted against the UNION of the two in-place rules, because that union is what the scan
# actually applies: `-E -i` (separated) is the BARE rule's job and `-Ei` (bundled) is the
# cluster rule's, and what matters to a reader of this file is that NO in-place spelling
# escapes the table. Which rule catches which is pinned separately, immediately below.
_cluster_missed=""
while IFS= read -r _cl; do
  [ -n "$_cl" ] || continue
  printf '%s\n' "$_cl" >"$tmp/cluster-one.sh"
  if [ -z "$(scan_hits "$RE_SED_INPLACE_CLUSTER" "$tmp/cluster-one.sh")" ] &&
    [ -z "$(scan_hits "$RE_SED_INPLACE" "$tmp/cluster-one.sh")" ]; then
    _cluster_missed="$_cluster_missed [$_cl]"
  fi
done <"$tmp/cluster-bad.sh"
if [ -z "$_cluster_missed" ]; then
  ok 'structural control: every in-place spelling (-Ei, -ni, -nEi, -E -i) is detected by the in-place rules — no bundled form escapes the table'
else
  bad "structural control: the in-place rules MISS:$_cluster_missed — a scanner with a known hole invites reliance it cannot support"
fi
printf '%s\n' "  sed -Ei 's/a/b/' \"\$f\"" >"$tmp/cluster-one.sh"
if [ -z "$(scan_hits "$RE_SED_INPLACE" "$tmp/cluster-one.sh")" ]; then
  ok 'structural control: `sed -Ei` is (still) invisible to the bare -i rule — which is WHY the cluster rule exists, stated as a measurement rather than an assumption'
else
  bad 'structural control: the bare -i rule now also matches `sed -Ei`; fold the two rules together rather than keeping a redundant one'
fi

# NEGATIVE CONTROLS for the cluster rule: a cluster that does NOT end in `i`, and an ATTACHED
# suffix (portable — both seds read `-i.bak` as -i with suffix ".bak"), must not be reported.
# `-fi`/`-ei` are included because the trailing `i` there is the ARGUMENT of an argument-taking
# option, not an in-place flag: `sed -fi input` reads its script from a file named "i".
printf '%s\n' \
  "  sed -n '2p' \"\$f\"" \
  "  sed -E 's/a/b/' \"\$f\"" \
  "  sed -Ei.bak -e 's/a/b/' \"\$f\"" \
  "  sed -fi input" \
  "  sed -ei 's/a/b/' \"\$f\"" >"$tmp/cluster-ok.sh"
if [ -z "$(scan_hits "$RE_SED_INPLACE_CLUSTER" "$tmp/cluster-ok.sh")" ]; then
  ok 'structural control: a non-`i` cluster (-n, -E), an ATTACHED suffix (-Ei.bak) and an argument-taking cluster (-fi, -ei, where the i is the OPTION ARGUMENT) are not flagged — the rule reds only on the unportable spelling'
else
  bad "structural control: the cluster rule false-positives on a portable sed — a lint that reds on correct input is the lint agents learn to waive: $(scan_hits "$RE_SED_INPLACE_CLUSTER" "$tmp/cluster-ok.sh" | tr '\n' ' ')"
fi

# ---------------------------------------------------------------------------
# CONTROLS FOR THE SPELLINGS THE ADJACENT-FLAGS-ONLY FORM MISSED (roborev round 2). Each named
# form gets its OWN control and each is asserted to FIRE — a rule without a control is a rule
# nobody has tested. Multi-line fixtures are written as separate printf arguments so the
# continuation-joining path is exercised on real newlines, not on an escaped approximation.
# The assertion is against the UNION of the table's rules, because "is this flagged by the
# scan" is the property the guard actually provides.
# ---------------------------------------------------------------------------
scan_any() { # scan_any <file> -> hits from ANY rule in the table
  local _i _h _all=""
  for _i in "${!CONSTRUCT_RE[@]}"; do
    _h=$(scan_hits "${CONSTRUCT_RE[$_i]}" "$1")
    [ -n "$_h" ] && _all="$_all ${_h%%$'\n'*}"
  done
  printf '%s' "$_all"
}
assert_flagged() { # assert_flagged <label> <file>
  if [ -n "$(scan_any "$2")" ]; then
    ok "structural control: $1 is FLAGGED"
  else
    bad "structural control: $1 is NOT flagged — the scan has a hole at this spelling, and a guard with a known-but-hidden miss invites reliance it cannot support"
  fi
}
assert_not_flagged() { # assert_not_flagged <label> <file>
  if [ -z "$(scan_any "$2")" ]; then
    ok "structural control: $1 is correctly NOT flagged"
  else
    bad "structural control: $1 was flagged — a lint that reds on correct input is the lint agents learn to waive:$(scan_any "$2")"
  fi
}

# (a) -i AFTER an intervening option argument. BSD eats `file` as the backup suffix.
printf '%s\n' "  sed -e 's/a/b/' -i file" >"$tmp/sp-optrun.sh"
assert_flagged '`sed -e EXPR -i file` (-i beyond the adjacent option run)' "$tmp/sp-optrun.sh"
# (b) a LINE-BROKEN invocation — invisible to any single-line ERE, hence the joiner.
printf '%s\n' '  sed \' '    -i '"'"'s/a/b/'"'"' file' >"$tmp/sp-break.sh"
assert_flagged '`sed \` + newline + `-i …` (backslash-continued across lines)' "$tmp/sp-break.sh"
printf '%s\n' '  sed -e '"'"'s/a/b/'"'"' \' '    -Ei file' >"$tmp/sp-break2.sh"
assert_flagged 'a line-broken invocation whose continuation carries a BUNDLED -Ei' "$tmp/sp-break2.sh"
# (c) the delimiter argument SEPARATED from -d, with no operand.
printf '%s\n' '  order=$(paste -d ,)' >"$tmp/sp-darg.sh"
assert_flagged '`paste -d ,` (separated delimiter argument, still no file operand)' "$tmp/sp-darg.sh"
# (d) a REDIRECTION is not an operand — BSD usage()-errors just the same.
printf '%s\n' '  order=$(paste -sd, < input)' >"$tmp/sp-redir.sh"
assert_flagged '`paste -sd, < input` (a redirection is not a file operand)' "$tmp/sp-redir.sh"
printf '%s\n' '  order=$(paste -sd, <"$f")' >"$tmp/sp-redir2.sh"
assert_flagged '`paste -sd, <"$f"` (redirection with no space)' "$tmp/sp-redir2.sh"
# (e) the GNU long spelling, absent from BSD sed entirely.
printf '%s\n' "  sed --in-place -e 's/a/b/' file" >"$tmp/sp-long.sh"
assert_flagged '`sed --in-place` (GNU long option; BSD sed has no such flag)' "$tmp/sp-long.sh"

# NEGATIVE CONTROLS for the widened option run — this is where widening can go wrong.
# The first is the important one: an `-i` belonging to a DIFFERENT command after a pipe must
# not be attributed to the sed, which is why the option run stops at a shell metacharacter.
printf '%s\n' '  sed '"'"'s/x/y/'"'"' f | grep -i foo' >"$tmp/sp-pipe.sh"
assert_not_flagged 'a `grep -i` AFTER a pipe (the -i belongs to another command)' "$tmp/sp-pipe.sh"
printf '%s\n' "  sed -e 's/a/b/' file" >"$tmp/sp-noi.sh"
assert_not_flagged 'a multi-option sed with NO in-place flag' "$tmp/sp-noi.sh"
printf '%s\n' '  order=$(paste -d , "$f")' >"$tmp/sp-dargok.sh"
assert_not_flagged '`paste -d , FILE` (separated delimiter argument WITH an operand)' "$tmp/sp-dargok.sh"
printf '%s\n' '  grep -q foo \' '    "$f" | sed -n '"'"'1p'"'"'' >"$tmp/sp-breakok.sh"
assert_not_flagged 'a benign backslash-continued command (joining must not manufacture a hit)' "$tmp/sp-breakok.sh"
# A comment line ending in a backslash continues NOTHING in shell, so it must not swallow the
# next line into a joined logical line — otherwise a comment could mask real code below it.
printf '%s\n' '# sed \' "  echo ok" >"$tmp/sp-cmtbreak.sh"
assert_not_flagged 'a COMMENT ending in a backslash (it must not join the code line beneath it)' "$tmp/sp-cmtbreak.sh"
printf '%s\n' '# prose about sed, ending in a backslash \' "  sed -i 's/a/b/' file" \
  >"$tmp/sp-cmtbreak2.sh"
assert_flagged 'a REAL `sed -i` on the line beneath a backslash-ended COMMENT (joining it into the comment would MASK it)' "$tmp/sp-cmtbreak2.sh"

# The joiner rewrites the stream, so LINE NUMBERS are asserted rather than assumed: a report
# naming the wrong line sends the next reader to the wrong place.
printf '%s\n' '# a comment' '' "  sed -i 's/a/b/' f" '  echo tail' >"$tmp/sp-lineno.sh"
_ln=$(scan_hits "$RE_SED_INPLACE" "$tmp/sp-lineno.sh")
if [ "${_ln%%:*}" = 3 ]; then
  ok 'structural control: a hit is reported at its REAL file line (blanking, not deleting, keeps numbering exact)'
else
  bad "structural control: the hit was reported at line '${_ln%%:*}' but lives at line 3 — the scan's line numbers do not name the real file"
fi

# NEGATIVE CONTROL for the paste pattern, whose ERE is the subtlest of the table: a paste WITH
# an explicit operand is portable and must NOT be reported.
printf '%s\n' '  order=$(grep -n x f | cut -d: -f2 | paste -sd, -)' >"$tmp/paste-ok.sh"
printf '%s\n' '  order=$(paste -sd, "$f")' >>"$tmp/paste-ok.sh"
if [ -z "$(scan_hits "$RE_PASTE_NO_OPERAND" "$tmp/paste-ok.sh")" ]; then
  ok 'structural control: a paste WITH a file operand (`-` or a path) is not flagged'
else
  bad 'structural control: the paste pattern false-positives on a portable paste with an operand — a lint that reds on correct input is the lint agents learn to waive'
fi

# CONTROL for the escape marker, in BOTH directions: it must exempt the line it is on, and it
# must not be a blanket switch (the same sample WITHOUT the marker is still detected above).
printf '%s\n' "  sed -i 's/a/b/' \"\$f\"   # portability-lint-allow: deliberate BSD-emulation control" \
  >"$tmp/allow.sh"
if [ -z "$(scan_hits "$RE_SED_INPLACE" "$tmp/allow.sh")" ]; then
  ok 'structural control: a line marked portability-lint-allow is exempt (a visible, per-line escape)'
else
  bad 'structural control: the portability-lint-allow marker does not exempt its line'
fi

# ===========================================================================
# (2) THE BSD SHIMS, and the controls that prove they reproduce the reported defects.
# ===========================================================================
shim="$tmp/shim"
mkdir -p "$shim"
REAL_SED=$(command -v sed || printf '')
REAL_PASTE=$(command -v paste || printf '')
if [ -z "$REAL_SED" ] || [ -z "$REAL_PASTE" ]; then
  bad 'shim setup: sed/paste not found on PATH — the differential cannot run (this is a failure to measure, not a measurement)'
fi

{
  printf '#!/usr/bin/env bash\n# BSD/macOS sed emulation, -i only (see test_roborev_guard_portability.sh).\n'
  printf 'REAL_SED=%q\n' "$REAL_SED"
  cat <<'SHIM_SED'
set -uo pipefail
# A zero-argument call is handled before any array is built: expanding an EMPTY "${arr[@]}"
# under `set -u` aborts on macOS's /bin/bash 3.2, the floor the gate declares.
[ $# -eq 0 ] && exec "$REAL_SED"
# BSD sed declares -i with a REQUIRED argument, so a SEPARATE `-i` consumes the NEXT argv
# entry as the backup suffix. Rewriting it to GNU's attached form reproduces exactly that
# consumption: `sed -i EXPR FILE` becomes suffix=EXPR, script=FILE, no input file — the edit
# never lands and the exit status is non-zero, which is what macOS does.
out=()
while [ $# -gt 0 ]; do
  if [ "$1" = "-i" ]; then
    shift
    suffix="${1-}"
    [ $# -gt 0 ] && shift
    out+=("-i$suffix")
  else
    out+=("$1")
    shift
  fi
done
exec "$REAL_SED" "${out[@]}"
SHIM_SED
} >"$shim/sed"

{
  printf '#!/usr/bin/env bash\n# BSD/macOS paste emulation (see test_roborev_guard_portability.sh).\n'
  printf 'REAL_PASTE=%q\n' "$REAL_PASTE"
  cat <<'SHIM_PASTE'
set -uo pipefail
# getopt(argc, argv, "d:s") then `if (*argv == NULL) usage();` — flags are consumed (with
# bundling, so `-sd,` is -s plus -d,), and a missing FILE OPERAND is a usage error on stderr
# with exit 1 and NOTHING on stdout.
if [ $# -eq 0 ]; then   # see the sed shim: no empty-array expansion under bash 3.2 + set -u
  printf 'usage: paste [-s] [-d delimiters] file ...\n' >&2
  exit 1
fi
orig=("$@")
# A COUNTER, not an array: `${#arr[@]}` on an array that never received an element is not
# reliably safe under bash 3.2 + set -u either.
n_operands=0
end=0
while [ $# -gt 0 ]; do
  a="$1"
  if [ "$end" -eq 0 ] && [ "$a" = "--" ]; then end=1; shift; continue; fi
  if [ "$end" -eq 0 ] && [ "${a#-}" != "$a" ] && [ "$a" != "-" ]; then
    rest="${a#-}"
    while [ -n "$rest" ]; do
      c="${rest:0:1}"
      rest="${rest:1}"
      case "$c" in
        d) if [ -n "$rest" ]; then rest=""; else shift; fi ;;
        *) ;;
      esac
    done
    shift
    continue
  fi
  n_operands=$((n_operands + 1))
  shift
done
if [ "$n_operands" -eq 0 ]; then
  printf 'usage: paste [-s] [-d delimiters] file ...\n' >&2
  exit 1
fi
exec "$REAL_PASTE" "${orig[@]}"
SHIM_PASTE
} >"$shim/paste"
chmod +x "$shim/sed" "$shim/paste"

SHIM_PATH="$shim:$PATH"

# CONTROL A: the sed shim must reproduce the #3296 defect — non-zero exit AND an UNCHANGED
# file. If it silently worked, every sed_inplace assert below would be vacuous.
printf 'foo\n' >"$tmp/ctl-sed.txt"
if PATH="$SHIM_PATH" sed -i 's/foo/bar/' "$tmp/ctl-sed.txt" 2>/dev/null; then
  bad 'shim control: `sed -i EXPR FILE` SUCCEEDED under the BSD shim — the shim does not emulate BSD, so the differential below would prove nothing'
elif [ "$(cat "$tmp/ctl-sed.txt")" = foo ]; then
  ok 'shim control: under BSD -i semantics `sed -i EXPR FILE` fails AND leaves the file unpatched (the #3296 root cause 1, reproduced on this platform)'
else
  bad "shim control: the file changed despite the failure: $(cat "$tmp/ctl-sed.txt")"
fi

# CONTROL B: the paste shim must reproduce case (j2) — empty stdout, non-zero exit.
_ctl_paste=$(printf 'a\nb\n' | PATH="$SHIM_PATH" paste -sd, 2>/dev/null)
_ctl_paste_rc=$?
if [ "$_ctl_paste_rc" -eq 0 ] || [ -n "$_ctl_paste" ]; then
  bad "shim control: operand-less `paste -sd,` produced '$_ctl_paste' (rc $_ctl_paste_rc) under the BSD shim — the shim does not emulate BSD"
else
  ok 'shim control: under BSD semantics an operand-less `paste -sd,` yields EMPTY stdout + non-zero exit (the #3296 root cause 3, reproduced on this platform)'
fi
printf 'a\nb\n' >"$tmp/ctl-paste.txt"
if [ "$(PATH="$SHIM_PATH" paste -sd, "$tmp/ctl-paste.txt" 2>/dev/null)" = 'a,b' ]; then
  ok 'shim control: the paste shim still passes a legitimate `paste -sd, FILE` through'
else
  bad 'shim control: the paste shim broke a legitimate `paste -sd, FILE` — it emulates something other than BSD'
fi

# ---------------------------------------------------------------------------
# The helpers under test are EXTRACTED VERBATIM from the guard test, never copied: a copy
# would drift, and a drifted copy passing here while the real helper is broken is precisely
# the shape of failure this file exists to remove.
# ---------------------------------------------------------------------------
extract_fn() { # extract_fn <name> -> the function's source text
  awk -v fn="$1" '$0 ~ "^" fn "\\(\\) \\{" { inside = 1 } inside { print } inside && /^\}$/ { exit }' "$GUARD"
}

for _fn in sed_inplace summary_key_order; do
  _src=$(extract_fn "$_fn")
  if [ -z "$_src" ]; then
    bad "extraction: $_fn is not defined in $(basename "$GUARD") — the portable helper was removed or renamed, so nothing below tests it"
    continue
  fi
  if printf '%s\n' "$_src" | bash -n 2>/dev/null; then
    ok "extraction: $_fn was read verbatim from the guard test and parses"
  else
    bad "extraction: the $_fn text read from the guard test does not parse"
    continue
  fi
  eval "$_src"
done

if ! declare -f sed_inplace >/dev/null || ! declare -f summary_key_order >/dev/null; then
  bad 'extraction: the guard test helpers are not available — the differential below cannot run'
else
  # --- sed_inplace, single-line substitution, under BSD -i semantics.
  printf 'alpha\nbeta\n' >"$tmp/one.txt"
  if PATH="$SHIM_PATH" sed_inplace "$tmp/one.txt" 's/^alpha$/ALPHA/' &&
    [ "$(cat "$tmp/one.txt")" = "$(printf 'ALPHA\nbeta')" ]; then
    ok 'sed_inplace: a single-line substitution lands under BSD sed semantics (where `sed -i` would not have)'
  else
    bad "sed_inplace: the substitution did not land under the BSD shim: $(cat "$tmp/one.txt")"
  fi

  # --- sed_inplace, the cx29-shaped MULTI-LINE insert, verified the way cx29 verifies it:
  # the inserted line must be the line IMMEDIATELY AFTER the function header.
  printf 'roborev_check_prompt_content() {\n  local x\n}\n' >"$tmp/multi.sh"
  PATH="$SHIM_PATH" sed_inplace "$tmp/multi.sh" \
    's/^roborev_check_prompt_content() {$/roborev_check_prompt_content() {\
  return 0/'
  _mp=$(grep -A1 '^roborev_check_prompt_content() {$' "$tmp/multi.sh" | sed -n '2p')
  if [ "$_mp" = '  return 0' ]; then
    ok 'sed_inplace: the cx29-shaped two-line replacement lands, with the new line immediately after the header'
  else
    bad "sed_inplace: the multi-line replacement did not land as two lines (line 2 = '$_mp')"
  fi

  # --- AC2 AT THE EDIT SITE: an expression that matches NOTHING must be an ERROR, and the
  # file must be byte-identical. Portability must not have become unconditionality.
  cp "$tmp/one.txt" "$tmp/one.before"
  if PATH="$SHIM_PATH" sed_inplace "$tmp/one.txt" 's/^nothing-matches-this$/x/'; then
    bad 'sed_inplace: a no-op patch returned SUCCESS — a silently unapplied mutation would then be asserted against, which is a probe failing in the direction that looks like success'
  elif cmp -s "$tmp/one.txt" "$tmp/one.before"; then
    ok 'sed_inplace: a no-op patch returns NON-ZERO and leaves the file byte-identical (fail-closed preserved)'
  else
    bad 'sed_inplace: a no-op patch changed the file'
  fi
  # --- MODE PRESERVATION, BOTH DIRECTIONS (#3296 round-6). The cases in the guard test mutate
  # COPIES OF EXECUTABLE FLOW SCRIPTS (roborev-review-checks.sh, mode 755). The first form of the
  # helper ended in `mv scratch original`, and the scratch was created fresh by `>`, so it carried
  # `0666 & ~umask` — no execute bits under ANY umask: measured `-rwxr-xr-x` -> `-rw-rw-r--` on a
  # successful edit. An environment-dependent breakage introduced by a portability fix is the very
  # class this branch closes, so it is pinned here in both directions: a SUCCESSFUL edit and a
  # FAILED (no-change, non-zero) edit must both leave the file executable. `[ -x ]` is POSIX test,
  # not a `stat` format flag (those are the GNU-vs-BSD divergence itself).
  printf '#!/bin/sh\necho alpha\n' >"$tmp/exec-ok.sh"
  chmod 755 "$tmp/exec-ok.sh"
  if [ ! -x "$tmp/exec-ok.sh" ]; then
    bad 'sed_inplace control: the fixture could not be made executable, so mode preservation was NOT MEASURED (a filesystem mounted noexec-ish?)'
  else
    if PATH="$SHIM_PATH" sed_inplace "$tmp/exec-ok.sh" 's/alpha/ALPHA/' &&
      grep -qF 'echo ALPHA' "$tmp/exec-ok.sh"; then
      if [ -x "$tmp/exec-ok.sh" ]; then
        ok 'sed_inplace: a SUCCESSFUL edit preserves the executable bit (the original file is truncated and rewritten, never replaced by a fresh umask-moded file)'
      else
        bad 'sed_inplace: a successful edit LOST the executable bit — mutating a copied flow script would make it non-executable, which is a new platform/umask-dependent breakage'
      fi
    else
      bad 'sed_inplace: the mode-preservation fixture edit did not land, so the executable-bit contract was NOT MEASURED'
    fi
    printf '#!/bin/sh\necho beta\n' >"$tmp/exec-noop.sh"
    chmod 755 "$tmp/exec-noop.sh"
    if PATH="$SHIM_PATH" sed_inplace "$tmp/exec-noop.sh" 's/^nothing-matches-this$/x/'; then
      bad 'sed_inplace: the no-op fixture edit returned SUCCESS, so the FAILED-edit direction of the mode contract was not exercised'
    elif [ -x "$tmp/exec-noop.sh" ]; then
      ok 'sed_inplace: a FAILED (no-change, non-zero) edit also preserves the executable bit'
    else
      bad 'sed_inplace: a FAILED edit LOST the executable bit — the failure path must leave the file exactly as it was'
    fi
  fi

  # And no temp spill: the helper must not leave its scratch file behind on the failure path.
  #
  # AFFIRMATIVE MEASUREMENT (CLAUDE.md: "never derive a pass from the ABSENCE of a bad
  # signal"). The obvious spelling — `[ -z "$(find … 2>/dev/null)" ]` — reads EMPTY OUTPUT as
  # "no spill", so an enumeration that never ran (unreadable dir, a `find` that errored, a
  # mistyped path) reports the same green as a genuinely clean dir. That is the exact vacuous
  # portability pass this file exists to prevent, one level down. So the enumeration is
  # (1) first shown to DETECT a planted spill, and (2) then required to EXIT ZERO before its
  # empty output is allowed to mean anything; its stderr is captured, never discarded.
  #
  # `-maxdepth` is deliberately KEPT: it is NOT a GNU extension — FreeBSD/macOS find(1)
  # documents `-maxdepth n` ("Always true; descend at most n directory levels below the
  # command line arguments"), and scripts/agent-gate.sh already relies on it fleet-wide.
  _spill_err="$tmp/spill-find.err"
  scratch_scan() { find "$tmp" -maxdepth 1 -name '*.sed-inplace.*' 2>"$_spill_err"; }

  _decoy="$tmp/planted.sed-inplace.control"
  : >"$_decoy"
  _ctl_spill=$(scratch_scan); _ctl_spill_rc=$?
  rm -f "$_decoy"
  if [ "$_ctl_spill_rc" -eq 0 ] && printf '%s\n' "$_ctl_spill" | grep -qF 'planted.sed-inplace.control'; then
    ok 'sed_inplace control: the scratch-file enumeration detects a PLANTED spill (so its clean verdict below is a measurement, not an empty pipe)'
  else
    bad "sed_inplace control: the scratch-file enumeration did not find a planted spill (rc $_ctl_spill_rc, stderr: $(tr '\n' ' ' <"$_spill_err")) — every no-spill verdict from it would be vacuous"
  fi

  _spill=$(scratch_scan); _spill_rc=$?
  if [ "$_spill_rc" -ne 0 ]; then
    bad "sed_inplace: the scratch-file enumeration itself FAILED (rc $_spill_rc, stderr: $(tr '\n' ' ' <"$_spill_err")) — a failure to measure is not a measurement, so this is NOT reported as 'no spill'"
  elif [ -n "$_spill" ]; then
    bad "sed_inplace: a .sed-inplace.* scratch file survived the no-op path: $(printf '%s' "$_spill" | tr '\n' ' ')"
  else
    ok 'sed_inplace: no scratch file is left behind on the no-op path (enumeration exited 0, having first been shown to detect a planted spill)'
  fi

  # --- AC2 IN THE CASES: the four cases must STILL verify the mutation landed. Asserted on
  # the guard test's TEXT, because a helper that is fail-closed today does not keep the CASES
  # fail-closed tomorrow. These are the verification forms cx28 / cx29 / cx28b+cx28c use — each
  # now names the state that must be PRESENT after the edit and, where an earlier case's
  # mutation must be gone, the state that must be ABSENT (`sed_inplace_verified`, #3296).
  for _vpair in \
    "'    TIER1=\"MEASUREMENT-DID-NOT-HAPPEN\"' '    TIER1=\"PASS\"'|cx28 verifies its unrecognised-verdict patch landed and replaced the valid value" \
    "= '  return 0'|cx29 verifies its early-return patch is the line after the header" \
    "'    TIER1=\"PASS\"' 'MEASUREMENT-DID-NOT-HAPPEN'|cx29 verifies the cx28 mutation was really restored (present PASS, absent stale value)" \
    "\"    TIER1=\\\"\$_np_value\\\"\" '    TIER1=\"PASS\"'|cx28b/cx28c verify their near-prefix patch landed and replaced the valid value"; do
    _vtext="${_vpair%%|*}"
    _vwhy="${_vpair#*|}"
    if grep -qF -- "$_vtext" "$GUARD"; then
      ok "AC2: $_vwhy"
    else
      bad "AC2: the guard test no longer contains this verification ($_vwhy) — a case that cannot detect an unapplied patch is a regression even when green"
    fi
  done

  # --- DELIBERATELY NOT BUILT: a STRUCTURAL rule that every `sed_inplace`/`sed_inplace_verified`
  # call in the guard test reads the helper's status (owner ruling, #3296 round 6; the #3283
  # disposition pattern).
  #
  # WHAT IT WOULD HAVE CHECKED: that no mutation call site discards the helper's non-zero
  # "nothing changed" status — the defect behind the round-4 finding, where cx29's restore ignored
  # it and the case then passed on cx28's stale value.
  #
  # WHY IT IS GONE: it existed for three review rounds and produced a false PASS in every one
  # (1 -> 2 -> 1 -> 2), each time in code the preceding fix round had just introduced: a
  # `&&`/`||`-anywhere deny-list accepted `true && sed_inplace …` and `sed_inplace …; true || bad`;
  # the allow-list that replaced it accepted `if sed_inplace … || true; then`; and it could not see
  # an unquoted `sed_inplace $file "$expr"` at all. That is not a series of typos, it is the shape
  # of the task: the rule is a bash ERE approximating SHELL GRAMMAR, and the correctness of a
  # second implementation of a grammar is knowable only by differential testing against the real
  # parser — which is out of scope here. CLAUDE.md's #3229 owner ruling applies verbatim: a guard
  # with known documented false-PASSes is worse than no guard, because it invites reliance it
  # cannot support, and subtraction cannot introduce a false PASS.
  #
  # WHAT PROTECTS THE PROPERTY INSTEAD: the four call sites in the guard test are correct today and
  # each is verified BEHAVIOURALLY — every one routes a failed mutation to `bad` (proved by making
  # each expression non-matching and observing the case FAIL at the edit site rather than passing on
  # stale content), `sed_inplace_verified` requires its three affirmative facts at the edit site
  # itself, and the AC2 text pins above still assert each case carries its own post-edit
  # verification. The BSD-shim differential below exercises the helper's fail-closed no-op return
  # and its mode preservation directly.
  #
  # ACCEPTED RESIDUAL, stated plainly and not argued away: a FIFTH call site added later that
  # discards the status is caught only BEHAVIOURALLY — i.e. only if it happens to break a case —
  # and nothing here will name it. Per the ruling this is recorded, not tracked as an issue, and is
  # re-raisable only if it ever bites in practice.

  # --- summary_key_order under the paste shim, plus the RED side of the differential.
  printf 'vacuity-tier2: PASS\nroborev-exit: PASS\nfindings: NONE\nlog: /tmp/x\n' >"$tmp/block.txt"
  if [ "$(PATH="$SHIM_PATH" summary_key_order "$tmp/block.txt" 'vacuity-tier2|roborev-exit|log')" \
    = 'vacuity-tier2,roborev-exit,log' ]; then
    ok 'summary_key_order: the key order is extracted correctly under BSD paste semantics'
  else
    bad "summary_key_order: wrong extraction under the BSD shim: '$(PATH="$SHIM_PATH" summary_key_order "$tmp/block.txt" 'vacuity-tier2|roborev-exit|log')'"
  fi
  # THE OLD-PIPELINE DIFFERENTIAL — the RED side of the fix, and (with the shim controls above)
  # the AUTHORITATIVE half of this file, so a control that can pass for the wrong reason here
  # undermines the whole guard (#3296 round-8 finding 4). The reported case (j2) symptom was
  # `unexpected key order: ` with nothing after the colon; the operand-less paste below is
  # DELIBERATE — it IS the defect being reproduced — and carries a per-line lint marker.
  #
  # EMPTY OUTPUT ALONE IS NOT PROOF THAT `paste` REFUSED. The previous form captured the
  # composed pipeline's stdout, DISCARDED ITS EXIT STATUS, and read `[ -z "$_old" ]` as "BSD
  # paste usage()-errored". But every UNRELATED failure in that pipeline yields the very same
  # empty string — a mistyped fixture path, a grep that matched nothing, a `cut` missing from
  # the shimmed PATH, a quoting error in the `bash -c` text — so the control that is supposed to
  # prove the shim reproduces case (j2) could pass for a reason that has nothing to do with
  # paste. That is CLAUDE.md's shape verbatim: a positive verdict derived from the ABSENCE of a
  # bad signal, with every unmeasured state inheriting the permissive branch.
  #
  # So THREE affirmative facts are required before emptiness may be ATTRIBUTED to the paste
  # stage, and the classifier is itself controlled below against a PLANTED upstream failure:
  #   1. the UPSTREAM `grep | cut` prefix, run alone on the same input under the same PATH,
  #      produces NON-EMPTY output (paste is therefore demonstrably fed real data);
  #   2. the full pipeline's STATUS is non-zero — measured with `set -o pipefail` inside the
  #      probe shell, so a failing paste stage is not masked by the last command's status;
  #   3. and its stdout is EMPTY, which is the reported case (j2) symptom.
  # The reason for each rejection is reported, so a red here names which stage misbehaved.
  _old_why=''
  old_pipeline_reproduces() { # old_pipeline_reproduces <block-file>; sets _old_why
    local _bf="$1" _up _up_rc=0 _full _full_rc=0
    _up=$(PATH="$SHIM_PATH" bash -c 'set -o pipefail; grep -nE "^(vacuity-tier2|roborev-exit|log):" "$1" | cut -d: -f2' _ "$_bf" 2>/dev/null) || _up_rc=$?
    if [ "$_up_rc" -ne 0 ] || [ -z "$_up" ]; then
      _old_why="UPSTREAM: the \`grep | cut\` prefix produced nothing on its own (rc $_up_rc, output '$_up'), so an empty full-pipeline result cannot be attributed to paste at all — a failure to measure, not a reproduction"
      return 1
    fi
    _full=$(PATH="$SHIM_PATH" bash -c 'set -o pipefail; grep -nE "^(vacuity-tier2|roborev-exit|log):" "$1" | cut -d: -f2 | paste -sd,' _ "$_bf" 2>/dev/null) || _full_rc=$? # portability-lint-allow: the operand-less paste IS the #3296 defect being reproduced
    if [ "$_full_rc" -eq 0 ]; then
      _old_why="STATUS: the pipeline SUCCEEDED (rc 0, output '$_full') — BSD paste semantics were not in force on this run"
      return 1
    fi
    if [ -n "$_full" ]; then
      _old_why="OUTPUT: the pipeline failed (rc $_full_rc) but still produced '$_full', so its failure is not the empty-stdout symptom of case (j2)"
      return 1
    fi
    _old_why="upstream alone produced '$(printf '%s' "$_up" | tr '\n' ' ')'; the paste stage then failed (rc $_full_rc) with EMPTY stdout"
    return 0
  }
  if old_pipeline_reproduces "$tmp/block.txt"; then
    ok "differential: the pre-#3296 \`grep | cut | paste\` pipeline FAILS with EMPTY stdout under BSD paste, and the emptiness is ATTRIBUTED to the paste stage rather than assumed — $_old_why (the reported case (j2) symptom, reproduced)"
  else
    bad "differential: case (j2) was NOT reproduced on this platform, so the fix is unverified here — $_old_why"
  fi
  # CONTROL, THE DIRECTION THAT WAS BROKEN: a pipeline failure that is NOT paste's must not be
  # reported as a successful reproduction. Planted as a MISSING INPUT FILE — grep then exits
  # non-zero with empty output, byte for byte the shape the status-discarding form accepted as
  # proof that BSD paste had refused.
  if old_pipeline_reproduces "$tmp/no-such-block-file.txt"; then
    bad 'differential control: a pipeline whose UPSTREAM failed (missing input file) was reported as a successful BSD-paste reproduction — the control passes for the wrong reason, so the differential proves nothing'
  else
    case "$_old_why" in
      UPSTREAM:*)
        ok 'differential control: an unrelated pipeline failure (missing input) is REJECTED and attributed to the upstream stages, not to paste — empty output alone is no longer read as proof' ;;
      *)
        bad "differential control: the unrelated failure was rejected, but for the wrong reason ($_old_why) — the classifier must identify the upstream stages as the cause" ;;
    esac
  fi
  if printf '%s\n' "$(extract_fn summary_key_order)" | grep -qE '(^|[^[:alnum:]_-])paste([[:space:]]|$)'; then
    bad 'summary_key_order: it still shells out to paste'
  else
    ok 'summary_key_order: the extraction has no paste stage at all'
  fi
fi

# ===========================================================================
# (3) AC3: case (f)'s pinned contract still FAILs when it is VIOLATED.
#
# The fix compares the wrapper's CANONICALISED --repo against a canonicalisation computed the
# same way, instead of the literal fixture path (which differs on macOS, where $TMPDIR is
# under /var, a symlink to /private/var). The hazard of such a fix is loosening the match
# until it accepts anything. So the REAL assert block is extracted from the guard test and
# fed four records: the sanctioned one, and three violations that must each still be caught.
#
# The fixture repo is placed UNDER A SYMLINK, which reproduces /var -> /private/var on any
# platform: `$work` and its canonical form differ, so the canonical-to-canonical comparison is
# genuinely exercised here rather than trivially satisfied.
# ===========================================================================
_cf_block=$(awk '/^# >>> BEGIN case-f-invocation-asserts/,/^# <<< END case-f-invocation-asserts/' "$GUARD")
if [ -z "$_cf_block" ] || ! printf '%s\n' "$_cf_block" | grep -q 'work_canon='; then
  bad 'AC3: the case-f-invocation-asserts block could not be extracted from the guard test (markers removed?) — the contract is then untested here'
else
  # FIXTURE SETUP IS ITSELF MEASURED. Every command below was previously allowed to fail
  # silently: a failed `git init`/commit leaves `_canon` EMPTY, and an empty `_canon` still
  # satisfies `"$work" != "$_canon"`, so the probes would then compare the wrapper's contract
  # against an empty expected repo path and report success. That is a positive verdict with no
  # affirmative measurement behind it — the defect this whole file exists to prevent. So the
  # setup is a fail-closed chain, `_canon` must be non-empty, ABSOLUTE, and actually resolve to
  # the fixture, and an unestablished fixture is a loud counted SKIP, never a pass.
  _cf_setup_err=''
  work="$tmp/link/work"
  mkdir -p "$tmp/real/work" || _cf_setup_err='mkdir of the fixture dir failed'
  [ -n "$_cf_setup_err" ] || ln -s "$tmp/real" "$tmp/link" || _cf_setup_err='the symlink that reproduces /var -> /private/var could not be created'
  [ -n "$_cf_setup_err" ] || git init -q -b main "$work" >/dev/null 2>&1 || _cf_setup_err='git init failed'
  [ -n "$_cf_setup_err" ] || git -C "$work" config user.email t@e || _cf_setup_err='git config user.email failed'
  [ -n "$_cf_setup_err" ] || git -C "$work" config user.name t || _cf_setup_err='git config user.name failed'
  [ -n "$_cf_setup_err" ] || printf 'x\n' >"$work/f.txt" || _cf_setup_err='writing the fixture file failed'
  [ -n "$_cf_setup_err" ] || git -C "$work" add f.txt >/dev/null 2>&1 || _cf_setup_err='git add failed'
  [ -n "$_cf_setup_err" ] || git -C "$work" commit -q -m base >/dev/null 2>&1 || _cf_setup_err='git commit failed'
  _canon=''
  if [ -z "$_cf_setup_err" ]; then
    _top=$(git -C "$work" rev-parse --show-toplevel 2>/dev/null) ||
      _cf_setup_err='git rev-parse --show-toplevel failed'
    [ -n "$_top" ] || _cf_setup_err='git rev-parse --show-toplevel returned nothing'
    if [ -z "$_cf_setup_err" ]; then
      _canon=$(cd "$_top" 2>/dev/null && pwd -P) || _cf_setup_err='canonicalising the fixture path failed'
    fi
  fi
  # The canonical path must be non-empty, absolute, AND the same directory as $work — an
  # arbitrary non-empty string would satisfy the "differs from $work" control by accident.
  if [ -z "$_cf_setup_err" ]; then
    case "$_canon" in
      /*) ;;
      *) _cf_setup_err="the canonical path is not absolute: '$_canon'" ;;
    esac
  fi
  if [ -z "$_cf_setup_err" ] && ! [ "$_canon" -ef "$work" ]; then
    _cf_setup_err="the canonical path '$_canon' is not the same directory as the fixture '$work'"
  fi

  if [ -n "$_cf_setup_err" ]; then
    skip "AC3: the case (f) fixture could not be established ($_cf_setup_err) — the canonicalisation contract was NOT MEASURED on this host. Not a pass: the probes below are skipped rather than run against an unestablished fixture."
  elif [ "$work" = "$_canon" ]; then
    bad 'AC3: the symlinked fixture did not produce a path that differs from its canonical form — the canonicalisation is not actually exercised, so a PASS below would be vacuous'
  else
    ok "AC3 control: the fixture path differs from its canonical form ($work vs $_canon), reproducing the macOS /var -> /private/var split"
  fi

  probe_case_f() { # probe_case_f <label> <expect: accept|reject> <recorded invocation line>
    local label="$2" expect="$3" line="$4" p0=$PASS f0=$FAIL dp df
    INVOKED="$tmp/invoked-$1.txt"
    printf '%s\n' "$line" >"$INVOKED"
    eval "$_cf_block" >/dev/null 2>&1
    dp=$((PASS - p0)); df=$((FAIL - f0))
    PASS=$p0; FAIL=$f0
    case "$expect" in
      accept)
        if [ "$dp" -eq 2 ] && [ "$df" -eq 0 ]; then
          ok "AC3: $label is ACCEPTED by the real case (f) asserts"
        else
          bad "AC3: $label should be accepted but the real asserts reported $df failure(s) / $dp pass(es)"
        fi ;;
      reject)
        if [ "$df" -eq 2 ] && [ "$dp" -eq 0 ]; then
          ok "AC3: $label is REJECTED by both real case (f) asserts"
        else
          bad "AC3: $label was NOT rejected by both asserts ($df failure(s) / $dp pass(es)) — the canonicalisation fix loosened the pinned contract"
        fi ;;
    esac
  }

  if [ -n "$_cf_setup_err" ]; then
    skip 'AC3: the four case (f) contract probes (sanctioned / relative / root-checkout / no --repo) were NOT RUN, because the fixture above could not be established'
  else
    _tail='--agent codex --model gpt-5.6-sol --wait'
    probe_case_f sanctioned 'the sanctioned canonical --repo record' accept \
      "review --branch --base origin/main --repo $_canon $_tail"
    probe_case_f relative 'a RELATIVE --repo' reject \
      "review --branch --base origin/main --repo . $_tail"
    probe_case_f rootco 'a ROOT-CHECKOUT --repo' reject \
      "review --branch --base origin/main --repo $REPO_ROOT $_tail"
    probe_case_f norepo 'a --branch review with NO --repo at all' reject \
      "review --branch --base origin/main $_tail"
  fi
fi

# ===========================================================================
# (4) AC5: the #3262 exit-code propagation is NOT weakened — `GUARD-TEST RESULT: FAIL`
# still exits non-zero. The fail-open that returned 0 on a FAIL verdict is what hid three
# of the #3296 failures for weeks, and it is the more expensive of the two defects.
#
# Exercised on the guard test's OWN prologue + OWN tally epilogue (both read out of the real
# file), with one injected failing case in between. Composing it rather than re-running all
# 581 cases keeps this component fast; the text under test is the real text either way.
# ===========================================================================
_pro_end=$(grep -nF "trap 'rm -rf \"\$tmp\"' EXIT" "$GUARD" | head -1 | cut -d: -f1)
_epi_start=$(grep -nF '==== ROBOREV REVIEW GUARD TEST TALLY' "$GUARD" | head -1 | cut -d: -f1)
if [ -z "$_pro_end" ] || [ -z "$_epi_start" ] || [ "$_epi_start" -le "$_pro_end" ]; then
  bad "AC5: could not locate the guard test's prologue/tally epilogue (prologue end '$_pro_end', epilogue start '$_epi_start') — the exit-code contract is untested here"
else
  # The composition is laid out in a MIRROR of the real tree (scripts/tests + scripts/flow with
  # a placeholder wrapper), because the guard test's prologue resolves $SCRIPT_DIR from $0 and
  # exits 1 when ../flow/roborev-review.sh is missing — a composition run from a bare temp dir
  # would fail for that reason instead of the one under test.
  mkdir -p "$tmp/mirror/tests" "$tmp/mirror/flow"
  printf '#!/usr/bin/env bash\n# placeholder: the AC5 composition never invokes the wrapper.\n' \
    >"$tmp/mirror/flow/roborev-review.sh"
  compose_probe() { # compose_probe <out> <middle line>
    {
      awk -v n="$_pro_end" 'NR <= n' "$GUARD"
      printf '%s\n' "$2"
      awk -v n="$_epi_start" 'NR >= n' "$GUARD"
    } >"$1"
  }
  # A COMPOSITION IS A LINE-SLICE OF A FOREIGN FILE, so it can be short-circuited by something
  # that is not the contract under test: a dependency SKIP (the guard test SKIPs its
  # structured-payload cases when python3 is missing — today at a line BELOW this slice, but a
  # preflight is exactly the kind of thing that migrates upward), a missing fixture, any early
  # `exit`. If that happens the probe never reaches the tally epilogue, and classifying it by
  # rc/text alone would report BOTH probes as contract FAILURES — turning a legitimate skip on a
  # stripped runner into a red gate, which is #3296's own failure mode reproduced one level down.
  #
  # So reaching the tally is measured FIRST and is a precondition for any verdict: no tally means
  # the exit-code contract was not measured, and such a run is never reported as a pass.
  #
  # CLOSED GRAMMAR (#3296, roborev blocker 2). "Did not reach the tally" is not one state, it is
  # the WHOLE SPACE of early exits: a dependency skip, a syntax error in the composition, a stray
  # `exit 1`, a prologue that dies silently. Classifying that space as one non-failing SKIP is the
  # CLAUDE.md defect in its purest form — a permissive branch keyed on the ABSENCE of a good
  # signal, so an ACTUALLY BROKEN exit-code contract (or an actually broken composition) reports
  # the same green as a stripped runner. Measured before this fix: injecting a syntax error into
  # the ac5-fail probe, and injecting `exit 1`, each produced `skipped: 1` and
  # `PORTABILITY RESULT: PASS`.
  #
  # So the classifier returns ONE of three ENUMERATED states, and only ONE of them is permissive:
  #   MEASURED      the tally was reached; the rc/text verdict below applies
  #   DEP-SKIP      the ONE permissive cause, AFFIRMATIVELY identified (see the branch)
  #   UNRECOGNISED  everything else -> a counted FAILURE naming what was not recognised
  # An unrecognised state FAILs; it never inherits DEP-SKIP. The classifier is itself controlled
  # below, in all three directions.
  #
  # DEP-SKIP IS ONE NAMED CONDITION, NOT A SHAPE (#3296 round-5 blocker 4). The round-4 form
  # accepted "rc 0 AND some line begins `SKIP -`", so an unrelated earlier skip followed by an
  # accidental `exit 0` would hide a truncated composition as an allowed dependency skip — a
  # deny-list wearing an allow-list's clothes. The permissive branch now requires the ONE
  # supported dependency condition, identified two ways at once:
  #   * a DEDICATED SENTINEL EXIT CODE (77, the long-standing "skipped" convention), distinct from
  #     the guard test's own failure exit (1) and from bash's syntax-error exit (2); and
  #   * the EXACT message line of that condition, read out of the guard test itself (never a
  #     hand-copied duplicate that could drift): the python3 preflight, the only dependency the
  #     guard test declines for. If that message cannot be extracted, the recognised cause is
  #     unidentifiable, which is a FAILURE TO MEASURE and is reported as such — DEP-SKIP then
  #     has no legitimate route at all rather than a loose one.
  # A future prologue skip for a DIFFERENT dependency is therefore UNRECOGNISED until it is added
  # here deliberately (the FAIL message says so) — noise, never blindness.
  _AC5_SKIP_RC=77
  _ac5_dep='python3: present'
  command -v python3 >/dev/null 2>&1 ||
    _ac5_dep='python3: ABSENT (the guard test SKIPs its structured-payload cases without it)'
  # The exact expected line, extracted from the guard test's own printf (not duplicated here).
  _ac5_dep_msg=$(grep -F "printf 'SKIP - no python3:" "$GUARD" | head -1 \
    | sed -e "s/^[[:space:]]*printf '//" -e "s/\\\\n'[[:space:]]*\$//")
  case "$_ac5_dep_msg" in
    'SKIP - no python3:'*) ;;
    *) _ac5_dep_msg='' ;;
  esac
  if [ -n "$_ac5_dep_msg" ]; then
    ok 'AC5 control: the ONE supported dependency-skip message was extracted from the guard test, so the permissive branch is keyed on a real, current condition'
  else
    bad "AC5 control: the guard test's python3 dependency-skip message could not be extracted, so the ONE permissive cause cannot be identified — every early exit will be reported UNRECOGNISED until this contract is updated"
  fi
  _ac5_out=''; _ac5_rc=0; _ac5_state=''; _ac5_cause=''
  run_ac5_probe() { # run_ac5_probe <script> -> _ac5_out / _ac5_rc / _ac5_state / _ac5_cause
    _ac5_out=$(bash "$1" 2>&1); _ac5_rc=$?
    if printf '%s\n' "$_ac5_out" | grep -qF '==== ROBOREV REVIEW GUARD TEST TALLY'; then
      _ac5_state='MEASURED'
      _ac5_cause='the probe reached the guard test tally epilogue'
    elif [ "$_ac5_rc" -eq "$_AC5_SKIP_RC" ] && [ -n "$_ac5_dep_msg" ] &&
      printf '%s\n' "$_ac5_out" | grep -qxF -- "$_ac5_dep_msg"; then
      # THE ONE PERMISSIVE CAUSE, and the reason it is legitimately permissive, recorded here at
      # the branch: the guard test may decline to run on a host that lacks python3, the single
      # dependency it declines for. TWO independent affirmative facts are required — the
      # dedicated sentinel rc (nothing else in the guard test exits 77) and the EXACT declared
      # message line (whole-line match, `grep -xF`, against text read from the guard test itself).
      # A stripped runner is a supported host, so this must not red the gate; anything that cannot
      # show BOTH facts is not this cause and may not borrow its verdict — including an unrelated
      # `SKIP -` line followed by an accidental `exit 0`, which is what the round-4 form accepted.
      _ac5_state='DEP-SKIP'
      _ac5_cause="the probe declined with the sentinel rc $_AC5_SKIP_RC and the exact python3 dependency message"
    elif [ "$_ac5_rc" -eq 0 ]; then
      _ac5_state='UNRECOGNISED'
      _ac5_cause="it exited ZERO before the tally, which is not the sentinel rc $_AC5_SKIP_RC a declared dependency skip must use (a silently truncated prologue, or an unrelated skip followed by a stray \`exit 0\`, looks exactly like this)"
    else
      _ac5_state='UNRECOGNISED'
      _ac5_cause="it exited NON-ZERO (rc $_ac5_rc) before the tally — a syntax error in the composition, a stray \`exit\`, or a prologue that failed; none of those is a dependency skip"
    fi
  }
  # The ONLY route to a SKIP: an affirmatively identified DEP-SKIP. Every other non-MEASURED
  # state is a counted FAILURE whose message names what was not recognised.
  ac5_not_measured() { # ac5_not_measured <label>
    if [ "$_ac5_state" = 'DEP-SKIP' ]; then
      skip "AC5 ($1): the composed probe DECLINED before the guard test's tally epilogue (rc $_ac5_rc; $_ac5_dep; last line: $(printf '%s' "$_ac5_out" | tail -1)) — a declared dependency skip, so the exit-code contract was NOT MEASURED on this host. Not a pass: rerun where the guard test's prologue dependencies are present."
    else
      bad "AC5 ($1): the composed probe exited before the guard test's tally for an UNRECOGNISED reason — $_ac5_cause (state $_ac5_state; rc $_ac5_rc; $_ac5_dep; last line: $(printf '%s' "$_ac5_out" | tail -1)). An unrecognised early exit is a FAILURE, never a skip: it may BE the broken exit-code contract this case exists to catch."
    fi
  }

  compose_probe "$tmp/mirror/tests/ac5-fail.sh" "bad 'AC5 injected failure'"
  compose_probe "$tmp/mirror/tests/ac5-pass.sh" ":"
  # CONTROL FIRST: the ONE supported dependency condition — the guard test's own python3 message,
  # verbatim, plus the sentinel rc — must be classified DEP-SKIP, or the permissive branch is dead
  # code and the red-gate hazard on a stripped runner is unfixed.
  compose_probe "$tmp/mirror/tests/ac5-skip.sh" \
    "printf '%s\\n' '$_ac5_dep_msg'; exit $_AC5_SKIP_RC"
  run_ac5_probe "$tmp/mirror/tests/ac5-skip.sh"
  if [ "$_ac5_state" = 'DEP-SKIP' ]; then
    ok "AC5 control: the ONE supported dependency skip (sentinel rc $_AC5_SKIP_RC + the exact python3 message) is classified DEP-SKIP, so a stripped runner degrades to a loud SKIP instead of a red gate"
  else
    bad "AC5 control: the supported dependency skip was classified $_ac5_state ($_ac5_cause) — the one permissive cause cannot be identified, so a legitimate skip on a stripped runner would red the gate"
  fi

  # THE OTHER DIRECTION, AND IT IS THE BLOCKER (#3296): the permissive branch must be reachable
  # ONLY from that affirmatively identified cause, so its COMPLEMENT is pinned case by case —
  # including the two halves of the signature taken separately, and the round-5 finding (an
  # unrelated `SKIP -` line plus an accidental `exit 0`, which the previous form accepted).
  # `ac5_not_measured` routes UNRECOGNISED to `bad`, so every one of these WOULD red the gate.
  for _ac5_u in \
    "syntax:bad 'AC5 injected failure'; fi" \
    "nonzero-exit:exit 1" \
    "silent-exit-zero:exit 0" \
    "unrelated-skip-line-then-exit-zero:printf 'SKIP - some unrelated earlier skip\\n'; exit 0" \
    "unrelated-skip-line-then-sentinel-rc:printf 'SKIP - some unrelated earlier skip\\n'; exit $_AC5_SKIP_RC" \
    "exact-message-without-sentinel-rc:printf '%s\\n' '$_ac5_dep_msg'; exit 0" \
    "exact-message-as-a-substring-only:printf '%s\\n' 'noise $_ac5_dep_msg noise'; exit $_AC5_SKIP_RC"; do
    _ac5_ulabel="${_ac5_u%%:*}"
    compose_probe "$tmp/mirror/tests/ac5-unrec.sh" "${_ac5_u#*:}"
    run_ac5_probe "$tmp/mirror/tests/ac5-unrec.sh"
    if [ "$_ac5_state" = 'UNRECOGNISED' ]; then
      ok "AC5 control: an early exit that is not the supported dependency condition ($_ac5_ulabel, rc $_ac5_rc) is classified UNRECOGNISED, so it is a counted FAILURE and not a skip"
    else
      bad "AC5 control: an early exit that is not the supported dependency condition ($_ac5_ulabel, rc $_ac5_rc) was classified $_ac5_state — a broken composition, or a broken exit-code contract, would then pass as a skip"
    fi
  done

  run_ac5_probe "$tmp/mirror/tests/ac5-fail.sh"
  if [ "$_ac5_state" != 'MEASURED' ]; then
    ac5_not_measured 'injected failing case'
  elif [ "$_ac5_rc" -ne 0 ] && printf '%s\n' "$_ac5_out" | grep -qF 'GUARD-TEST RESULT: FAIL' &&
    printf '%s\n' "$_ac5_out" | grep -qF 'failed: 1'; then
    ok "AC5: a failing case still exits NON-ZERO (rc $_ac5_rc) with GUARD-TEST RESULT: FAIL"
  else
    bad "AC5: the guard test's tally epilogue did not fail closed (rc $_ac5_rc): $(printf '%s' "$_ac5_out" | tail -3 | tr '\n' ' ')"
  fi

  run_ac5_probe "$tmp/mirror/tests/ac5-pass.sh"
  if [ "$_ac5_state" != 'MEASURED' ]; then
    ac5_not_measured 'clean control'
  elif [ "$_ac5_rc" -eq 0 ] && printf '%s\n' "$_ac5_out" | grep -qF 'GUARD-TEST RESULT: PASS'; then
    ok 'AC5 control: with no failing case the same epilogue exits 0 with GUARD-TEST RESULT: PASS'
  else
    bad "AC5 control: the clean composition did not pass (rc $_ac5_rc) — the injected-failure result above would then mean nothing"
  fi
fi

# The gate must not swallow either script's exit status: `roborev-lints` is where both run in
# --lite and in the full gate of record.
if [ ! -f "$GATE" ]; then
  skip 'wiring: agent-gate.sh not found; the roborev-lints wiring could not be checked'
else
  _lints=$(awk '/^run_roborev_lints_cmd\(\) \{/,/^\}$/' "$GATE")
  if [ -z "$_lints" ]; then
    bad 'wiring: run_roborev_lints_cmd is not defined in agent-gate.sh — this test may not be gate-run at all'
  else
    for _needed in test_roborev_review_guard.sh test_roborev_guard_portability.sh; do
      if printf '%s\n' "$_lints" | grep -qF "$_needed"; then
        ok "wiring: roborev-lints runs $_needed"
      else
        bad "wiring: roborev-lints does not run $_needed — a regression would not FAIL the fast loop"
      fi
    done
    if printf '%s\n' "$_lints" | grep -qE '\|\|[[:space:]]*(true|:)'; then
      bad 'wiring: run_roborev_lints_cmd suppresses a non-zero exit (|| true) — that is the #3262 fail-open again, one level up'
    else
      ok 'wiring: run_roborev_lints_cmd propagates a non-zero exit (no || true)'
    fi
  fi
fi

# The tally line deliberately does NOT start with `RESULT:` — that token belongs to the agent
# gate's summary contract and to the roborev wrapper's own block.
printf '\n==== ROBOREV GUARD PORTABILITY TALLY ====\n'
printf 'passed: %d  failed: %d  skipped: %d\n' "$PASS" "$FAIL" "$SKIPPED"
# A skip does not red the gate — a stripped runner is a supported host — but it is never
# silent: it is counted above and restated here, naming that coverage was reduced on this run.
if [ "$SKIPPED" -ne 0 ]; then
  printf 'NOT MEASURED: %d check(s) SKIPped on this host — see the SKIP lines above for what was not covered\n' "$SKIPPED"
fi
if [ "$FAIL" -ne 0 ]; then
  printf 'PORTABILITY RESULT: FAIL\n'
  exit 1
fi
printf 'PORTABILITY RESULT: PASS\n'
