#!/usr/bin/env bash
# PORTABILITY GUARD for an ENUMERATED set of macOS-sensitive shell files (issues #3296, #3756).
# It began as, and still is, the guard for the roborev review-guard code path; #3756 added
# `scripts/bootstrap-agent-machine.sh` and its suite. The AUTHORITATIVE statement of what a run
# covered is the `==== PORTABILITY LINT SCOPE ====` block it PRINTS — not this comment, and not
# the file's name. See SCOPE OF THIS SCANNER below.
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
# Anchored on REPO_ROOT rather than spelled with `..` segments: these paths are PRINTED in the
# scope declaration below and COMPARED against `git ls-files` output, and a `scripts/tests/../flow`
# spelling is neither readable to a human nor equal to the tracked path (#3756 — an unnormalised
# member silently counted as UNSCANNED, overstating the gap by 4).
GUARD="$SCRIPT_DIR/test_roborev_review_guard.sh"
GATE="$REPO_ROOT/scripts/agent-gate.sh"
FLOW_DIR="$REPO_ROOT/scripts/flow"

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

# THE TEMP DIRECTORY IS VERIFIED BEFORE ANYTHING USES IT, AND BEFORE THE TRAP (#3296 round-12).
# An unchecked `mktemp -d` that fails, or that prints nothing, leaves $tmp EMPTY — and then every
# `"$tmp/x"` below resolves to `/x`. This script does not merely read those paths, it CREATES them
# unconditionally: `mkdir -p "$tmp/real/work"`, `ln -s "$tmp/real" "$tmp/link"`,
# `chmod 000 "$tmp/cnr-noread.sh"`, dozens of `>"$tmp/…"` fixtures. On a privileged runner that is
# root-level file creation, and the files are left behind. It is gate-wired into `roborev-lints`, so
# it runs on every --lite and every full gate.
#
# THE ORDER IS THE POINT, not just the check: installing `trap 'rm -rf "$tmp"' EXIT` while $tmp may
# be empty arms a recursive delete for a path that is not ours, so the verification must come first.
# BOTH facts are required — a NON-EMPTY string AND an actual DIRECTORY — because a `mktemp` that
# emitted a diagnostic on stdout, or a path that vanished, would satisfy the emptiness test alone.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/roborev-portability.XXXXXX") || tmp=''
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'FAIL - mktemp -d did not yield a usable temp directory (got: %s) — refusing to run rather than resolving every "$tmp/..." fixture path under /\n' "${tmp:-<empty>}"
  exit 1
fi
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
#
# SCOPE OF THIS SCANNER — ENUMERATED, NOT EXHAUSTIVE, AND SAID SO IN CODE (#3296 round-8;
# CLAUDE.md: "where a signal genuinely SHOULD be permissive, record the reason IN CODE at the
# branch"). Three consecutive review rounds each produced the same class of finding — "here is
# another spelling the table does not detect" — and the spelling space of a shell command is
# UNBOUNDED: `--in-place=`, `-i''`, `-e X -i`, line continuations, `$SED -i`, `eval`, string
# concatenation. A scanner that implicitly claims completeness therefore generates a finding
# EVERY round, forever. That non-convergence is exactly why the sibling structural lint on this
# branch was DELETED by owner ruling (see the "DELIBERATELY NOT BUILT" record further down this
# file, and CLAUDE.md's #3229 ruling: a guard with known false-PASSes is worse than no guard).
#
# So, stated plainly rather than left to be rediscovered:
#   * WHAT THIS IS: a BEST-EFFORT DRIFT TRIPWIRE over an ENUMERATED set of spellings — the ones
#     that actually caused #3296 plus their nearest measured relatives. Enumerated today:
#     `sed -i EXPR`, `sed -i` beyond an option run, bundled clusters ending in `i` (-Ei/-ni/-nEi),
#     `--in-place` and `--in-place=SUFFIX`, the empty-suffix forms `-i''`/`-i""` (adjacent or
#     beyond an option run), backslash-continued invocations of all of the above; `paste` with no
#     file operand (bare, separated `-d ARG`, and with input/output/fd redirections); plus
#     `readlink -f`, `stat -c`, `grep -P`, `date -d`, `sed -z`/`grep -z`, `find -printf`,
#     `xargs -r`, `base64 -w`, `timeout`, and the bash-4-only constructs.
#   * WHAT IS AUTHORITATIVE, AND OVER EXACTLY WHAT CODE: mechanism (2), the BEHAVIOURAL BSD-shim
#     differential. It does not care how a construct is SPELLED — but it can only speak for the
#     code it actually EXECUTES, and that is a SHORT, NAMED LIST: the guard test's `sed_inplace`,
#     `sed_inplace_verified` and `summary_key_order`, extracted verbatim and run under the BSD
#     `sed -i` / BSD `paste` shims, plus the two bare shim controls. (Sections (3) and (4) also
#     EXECUTE real guard-test text — the `case-f-invocation-asserts` block, and the prologue +
#     tally epilogue — but NOT under the shims, so they are not portability coverage.) Everything
#     else in the scanned files — the whole of the rest of test_roborev_review_guard.sh, all of
#     scripts/flow/roborev-review*.sh, and roborev-job-facts.py — is covered by the ENUMERATED
#     SCANNER ALONE, and so are BOTH bootstrap files added by #3756, which have no behavioural
#     probe in this file at all.
#   * AND THE SUBJECT SET IS ITSELF A COVERAGE CLAIM (#3756). The list below is ENUMERATED, so a
#     file absent from it is not covered however green this run is — which is not hypothetical:
#     `xargs -0 -r` shipped in test_bootstrap_agent_machine.sh's tree-identity digest while THIS
#     FILE carried the `xargs -r` rule verbatim, because that file was never scanned. A full
#     derivation over all tracked `scripts/**/*.sh` was MEASURED and rejected (10 of 15 rules red
#     across ~40 sites, mostly other portability lints' own rule TABLES and deliberate
#     GNU-first/BSD-fallback pairs — a cross-cutting cleanup with its own review surface, which
#     would red `roborev-lints` in every lane's --lite). So the set stays enumerated and DECLARES
#     ITSELF AT RUN TIME rather than claiming a completeness it does not have.
#   * SO THE COVERAGE CLAIM IS NARROW, AND THIS IS ITS HONEST FORM (#3296 round-9 finding 2, which
#     CORRECTS the round-8 wording here — the earlier text called a missed spelling a "BOUNDED
#     FALSE NEGATIVE with a backstop underneath it" without qualification, and that was WRONG):
#       - INSIDE the three executed helpers, a missed spelling IS bounded: the behavioural probe
#         catches the defect whatever the spelling, because it runs the code under BSD semantics.
#       - ANYWHERE ELSE in the scanned files there is NO backstop. An unenumerated spelling
#         introduced there — `$SED -i`, an alias, `eval`, a quoted metacharacter inside the option
#         run — is an UNCOVERED false negative: this file reports the scanned set clean and nothing
#         in it will contradict that. See residual 5 below, which states it as a residual rather
#         than leaving it to be rediscovered. The two bootstrap files are wholly in this second
#         category: nothing in THIS file executes them under a shim.
#   * WHY THIS STILL SURVIVES WHERE THE DELETED LINT DID NOT — and the difference is NOT "it has a
#     backstop everywhere", which is the claim just retracted. It is that the deleted lint's misses
#     were false PASSES about the property it was the SOLE check for, and its false-PASS count GREW
#     across review rounds (1, 1, 2, 3). This table's misses are false NEGATIVES of a tripwire that
#     claims only to be a tripwire; its coverage is stated rather than implied; and every fix to it
#     is additive.
#   * WHAT A NEWLY-DISCOVERED SPELLING MEANS: add a row and a positive control — a cheap ADDITIVE
#     fix. It is NOT evidence that the mechanism is broken, and it is not a reason to attempt a
#     shell tokeniser here (a second implementation of shell grammar, whose correctness is
#     knowable only by differential testing against the first). Extending the differential to one
#     more real call site is the other additive route, and is how `sed_inplace_verified` got here.
# The residual it leaves is enumerated in full under "STATED RESIDUAL" below.
#
# THIS FILE SCANS ITSELF. It runs inside the macOS-sensitive `roborev-lints` gate component and
# is mostly shell scaffolding, so a GNU-only construct added HERE would reproduce the exact
# Linux-green/macOS-red regression the file exists to prevent — and nothing else would see it.
# The self-scan is not vacuous: this file is NOT blanket-exempt. It deliberately CONTAINS the
# banned constructs, in two places only — its BSD-emulation fixtures and its positive controls —
# and each such LINE carries a `portability-lint-allow` marker naming why, so the exemption is
# visible in the diff and every other line of this file is scanned like any other target.
BOOTSTRAP_SH="$REPO_ROOT/scripts/bootstrap-agent-machine.sh"
BOOTSTRAP_TEST="$SCRIPT_DIR/test_bootstrap_agent_machine.sh"
SCAN_FILES=(
  "$GUARD"
  "$SCRIPT_DIR/$(basename "$0")"
  "$FLOW_DIR/roborev-review.sh"
  "$FLOW_DIR/roborev-review-checks.sh"
  "$FLOW_DIR/roborev-review-oracles.sh"
  "$FLOW_DIR/roborev-job-facts.py"
  "$BOOTSTRAP_SH"
  "$BOOTSTRAP_TEST"
)

# THE SCOPE IS DECLARED AT RUN TIME, NOT ONLY IN THIS COMMENT (#3756 AC2). A reader of a green
# run learns which files it covered and — affirmatively, as a MEASURED count and not a constant
# that decays — how many tracked shell scripts it did NOT. `NOT MEASURED` is its own third state:
# a census that could not be taken is never rendered as a number, because a number in a scope
# declaration reads as authority.
_scope_unscanned_line() { # _scope_unscanned_line <repo-root>
  local _root="$1" _tracked _n_tracked=0 _n_unscanned=0 _f _rel _hit
  _tracked=$(cd "$_root" 2>/dev/null && git ls-files 'scripts/*.sh' 'scripts/**/*.sh' 2>/dev/null) || {
    printf 'unscanned: NOT MEASURED (the tracked-script census could not be taken under %s)\n' "$_root"; return 0; }
  [ -n "$_tracked" ] || {
    printf 'unscanned: NOT MEASURED (the tracked-script census returned nothing under %s)\n' "$_root"; return 0; }
  while IFS= read -r _rel; do
    [ -n "$_rel" ] || continue
    _n_tracked=$((_n_tracked + 1))
    _hit=no
    for _f in "${SCAN_FILES[@]}"; do
      [ "$_f" = "$_root/$_rel" ] && _hit=yes
    done
    [ "$_hit" = no ] && _n_unscanned=$((_n_unscanned + 1))
  done <<EOF_SCOPE
$_tracked
EOF_SCOPE
  printf 'unscanned: %d of %d tracked scripts/**/*.sh are NOT scanned by this lint\n' \
    "$_n_unscanned" "$_n_tracked"
}
emit_scope_declaration() {
  local _scope_f
  printf '==== PORTABILITY LINT SCOPE ====\n'
  printf 'This lint is an ENUMERATED subject set, not a derived one. A PASS below says nothing\n'
  printf 'about any file absent from this list.\n'
  for _scope_f in "${SCAN_FILES[@]}"; do
    printf 'scanned:   %s\n' "${_scope_f#"$REPO_ROOT/"}"
  done
  _scope_unscanned_line "$REPO_ROOT"
  printf '================================\n'
}
printf '\n'
emit_scope_declaration
printf '\n'

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
# THE EMPTY-SUFFIX SPELLINGS, `-i""` and `-i''`. GNU sed reads an ATTACHED empty suffix as "edit
# in place, keep no backup"; BSD sed has no such reading (it needs `-i ''` as a separate word, or
# no `-i` at all), so the GNU-only spelling is a portability defect in its own right.
#
# THESE TWO RULES ALSO TAKE THE OPTION RUN (#3296 round-8 finding 2). The first form required
# `-i""` to sit IMMEDIATELY after `sed`, so `sed -e 's/a/b/' -i'' f` — the same defect one option
# further along — was invisible to EVERY rule in this table: measured `. . . .` against the bare
# rule, the cluster rule and both empty-suffix rules before this fix. `--in-place=SUFFIX` needed
# no such repair; it is already reached through the option run above, and both of its positions
# are now pinned by controls below rather than assumed.
#
# AND THEY REQUIRE A TOKEN BOUNDARY AFTER THE CLOSING QUOTES (#3296 round-9 review, a FALSE
# POSITIVE in the two rules above). The rules must distinguish two spellings that differ only in
# what FOLLOWS the quotes, because SHELL QUOTE REMOVAL is what decides which one sed sees:
#   sed -i''      -> argv is `-i`      : a bare in-place flag. BSD declares -i WITH A REQUIRED
#                                        argument, so it eats the next token. NON-PORTABLE, flag it.
#   sed -i''.bak  -> argv is `-i.bak`  : the ATTACHED-SUFFIX form. BOTH seds read this identically,
#                                        and the cluster rule's own comment above already calls the
#                                        attached suffix portable (`-Ei.bak` is deliberately not
#                                        matched). PORTABLE — flagging it reds the gate on correct
#                                        code, and this lint is GATE-WIRED into roborev-lints, so a
#                                        false FAIL here recreates #3296 one level down: "a key that
#                                        reds on correct input is the key agents learn to waive".
# So a match now requires the quotes to END THE TOKEN: whitespace, a shell metacharacter, or
# end-of-line. Measured, before -> after, on the two directions: the four attached-suffix forms
# (`-i''.bak`, `-i"".bak`, either beyond an option run, and at EOL) go from FLAGGED to clean, while
# all seven bare-empty-suffix forms stay FLAGGED — including the three whose boundary is not a
# space (`sed -i''` at EOL, `$(sed -i'' f)` closing on `)`, and `sed -i'';`), which is why the
# boundary set is metacharacters and `$`, not `[[:space:]]` alone. Both directions are pinned by
# permanent controls below; widening the escape would show up there as a missed positive.
_TOK_END='([[:space:]]|$|[|;&<>()])'
RE_SED_INPLACE_EMPTY_DQ='(^|[^[:alnum:]_-])sed'"$_OPT_RUN"'[[:space:]]+-i("")'"$_TOK_END"
RE_SED_INPLACE_EMPTY_SQ='(^|[^[:alnum:]_-])sed'"$_OPT_RUN"'[[:space:]]+-i'"('')$_TOK_END"
# PASTE WITH NO FILE OPERAND, in every spelling that reaches BSD's `if (*argv == NULL) usage();`:
#   paste -sd,            bundled flags, nothing after them
#   paste -d ,            the delimiter argument SEPARATED from -d (consumed as the option's
#                         argument by getopt, so it is not an operand either)
#   paste -sd, < input    a REDIRECTION is not an operand — BSD still usage()-errors, and the
#                         `<` is why the option run above excludes redirection characters
#   paste -sd, >out       an OUTPUT redirection is not an operand either
#   paste -sd, 2>/dev/null   nor is a FILE-DESCRIPTOR redirection
# The `-d`-with-separated-argument pair is matched as ONE unit, because a regex cannot express
# "this bare token is the argument of the preceding option" any other way.
_PASTE_DARG='[[:space:]]+-[a-zA-Z]*d[[:space:]]+[^[:space:]|;)&<]+'
_PASTE_OPT='[[:space:]]+-[^[:space:]|;)&<]+'
# THE REDIRECTION GRAMMAR COVERS ALL THREE DIRECTIONS, AND REPEATS (#3296 round-8 finding 3). The
# first form recognised ONE OPTIONAL INPUT redirection, so `paste -sd, >output`,
# `paste -sd, 2>/dev/null`, `paste -sd, >>out 2>&1` and `paste -sd, <in >out` each had NO file
# operand, diverged on BSD exactly as case (j2) did, and were reported CLEAN (measured: `.` under
# the old grammar, `X` under this one, for all four). A redirection is: an optional fd number, one
# of `<` `>` `>>`, an optional `&` (so `2>&1` is one redirection, not a redirection plus a stray
# operand), and a target; zero or more of them may follow the options. `[0-9]*`/`&?` are what keep
# the fd forms from being read as operands, which is the whole point of the rule.
_PASTE_REDIR_ONE='[[:space:]]*[0-9]*(>>|<|>)&?[[:space:]]*[^[:space:]|;)&]+'
_PASTE_REDIR='('"$_PASTE_REDIR_ONE"')*'
RE_PASTE_NO_OPERAND='(^|[^[:alnum:]_-])paste('"$_PASTE_DARG"'|'"$_PASTE_OPT"')*'"$_PASTE_REDIR"'[[:space:]]*($|\||\)|;|&)'

add_construct "$RE_SED_INPLACE" \
  "BSD sed's -i takes a REQUIRED suffix argument, so it eats the EXPRESSION and the edit never lands (#3296 cx28/cx29/cx28b/cx28c) — use the guard test's sed_inplace helper" \
  "  sed -i 's/a/b/' \"\$f\"" # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct "$RE_SED_INPLACE_CLUSTER" \
  "a BUNDLED cluster ending in -i (-Ei, -ni, -nEi, …) reaches the SAME BSD argument-consuming -i as a bare -i, so the edit never lands — use sed_inplace" \
  "  sed -Ei 's/a/b/' \"\$f\"" # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct "$RE_SED_INPLACE_EMPTY_DQ" \
  'the empty-suffix spelling -i"" is GNU-only (BSD needs -i "" or no -i at all) — use sed_inplace' \
  '  sed -i"" -e s/a/b/ f' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct "$RE_SED_INPLACE_EMPTY_SQ" \
  "the empty-suffix spelling -i'' is GNU-only — use sed_inplace" \
  "  sed -i'' -e s/a/b/ f" # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct "$RE_PASTE_NO_OPERAND" \
  'a paste with NO FILE OPERAND is empty output + exit 1 on BSD (it usage()-errors instead of reading stdin) — pass an explicit `-`, or extract with awk (#3296 case (j2))' \
  '  order=$(grep -n x f | cut -d: -f2 | paste -sd,)' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])readlink[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-f' \
  'readlink -f is absent from older BSD readlink — canonicalise with `cd "$p" && pwd -P` (which is what the wrapper does)' \
  '  p=$(readlink -f "$x")' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])stat[[:space:]]+-c' \
  'stat -c is GNU-only (BSD spells it stat -f)' \
  '  n=$(stat -c %s "$f")' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])grep[[:space:]]+-[a-zA-Z]*P([[:space:]]|$)' \
  'grep -P (PCRE) is GNU-only — BSD grep has no -P' \
  "  grep -P '\\\\d+' f" # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])date[[:space:]]+(-d[[:space:]]|--date)' \
  'date -d/--date is GNU-only (BSD date uses -r / -v / -j -f)' \
  '  t=$(date -d @1700000000)' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])(sed|grep)[[:space:]]+-[a-zA-Z]*z([[:space:]]|$)' \
  'sed -z / grep -z (NUL-delimited records) are GNU-only — read `git … -z` output with a shell read loop or awk RS' \
  '  grep -z foo f' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '\-printf[[:space:]]' \
  'find -printf is GNU-only — use -exec or -print with a shell loop' \
  "  find . -printf '%p\\\\n'" # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
# NAMED because the #3756 bootstrap-scope controls below assert about this rule SPECIFICALLY —
# an index reference would silently retarget when a row is inserted above it.
RE_XARGS_R='(^|[^[:alnum:]_-])xargs[[:space:]]+(-[a-zA-Z]*r|--)'
add_construct "$RE_XARGS_R" \
  'xargs -r (and GNU long options) are not in BSD xargs; BSD already skips an empty input line only with -0' \
  '  printf "" | xargs -r rm' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])base64[[:space:]]+-w' \
  'base64 -w is GNU-only (BSD/macOS base64 has no wrap flag; use -b or fold)' \
  '  base64 -w0 <f' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
# THE DURATION MUST BE A COMPLETE TOKEN (#3756). `[0-9]` alone matched the `2` of
# `command -v timeout 2>/dev/null` — which is not an invocation of timeout(1) at all, it is the
# very GUARD this rule's own message tells you to write. A lint that reds on the remedy it
# recommends is the lint agents learn to waive, and it fired on three real call sites. So the
# duration is now `[0-9]+` plus an optional GNU suffix, and it must END the token: a digit
# followed by `>` or `<` is a REDIRECTION's file descriptor, never a duration.
RE_TIMEOUT_UNGUARDED='(^|[^[:alnum:]_-])timeout[[:space:]]+[0-9]+[smhd]?([[:space:]]|$)'
add_construct "$RE_TIMEOUT_UNGUARDED" \
  'timeout(1) is NOT installed on stock macOS — guard it with `command -v timeout` or restructure' \
  '  timeout 30 some-command' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)
add_construct '(^|[^[:alnum:]_-])(mapfile|readarray)([[:space:]]|$)|declare[[:space:]]+-A|\$\{[A-Za-z_][A-Za-z_0-9]*,,\}' \
  'bash 4 only — stock macOS /bin/bash is 3.2, so mapfile/readarray/associative arrays/case-conversion parameter expansion can abort the script outright' \
  '  mapfile -t arr <f' # portability-lint-allow: the SAMPLE VIOLATION this rule must detect (table data, not an invocation)

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
#   5. THE BACKSTOP DOES NOT COVER THE WHOLE SCANNED SET, AND 1-4 ARE UNCOVERED OUTSIDE IT
#      (#3296 round-9 finding 2 — this bullet CORRECTS an earlier claim made right here, that the
#      shim differential "is the backstop that catches what the text scan cannot see", full stop).
#      It is not, in general. The differential EXECUTES exactly three helpers — `sed_inplace`,
#      `sed_inplace_verified`, `summary_key_order` — under BSD `sed -i` / BSD `paste` semantics.
#      For code INSIDE those three, residuals 1-4 are bounded: the probe runs the code and a defect
#      surfaces as a failing case whatever the spelling. For every OTHER line of the scanned set —
#      the rest of test_roborev_review_guard.sh, all four scripts/flow/roborev-review* files, and
#      BOTH #3756 bootstrap files — the enumerated scanner is the ONLY mechanism in this file, so a
#      spelling from 1-4 introduced THERE is an UNCOVERED false negative: the scan reports the
#      scanned set clean and no probe contradicts it. (The bootstrap pair does have a behavioural
#      probe, but it is test_bootstrap_agent_machine.sh's own cases 6o-6q under a BSD `readlink`
#      shim, it covers ONE call site, and it backstops nothing else in those 7300 lines.)
#      This is a KNOWN REDUCTION IN COVERAGE, accepted and recorded, not argued away. It is NOT
#      closed by adding "parsing-based validation": a bash re-implementation of shell word
#      splitting is a second implementation of a grammar, and a second implementation's correctness
#      is knowable only by differential testing against the original — the failure recorded in
#      CLAUDE.md for the deleted `census-exclusion:` predictor (which was tested against a MODEL of
#      Go rather than against Go) and for the deleted status lint further down this file (a
#      false-PASS count that GREW: 1, 1, 2, 3). The two routes that ARE open are both additive and
#      cheap: extend the differential to one more real call site (which is how
#      `sed_inplace_verified` came to be covered), or add the spelling to the table with a positive
#      control.
#
# Residuals 1-4 are MISSES, never false greens about something else. Residual 5 says where those
# misses have a behavioural backstop underneath them and where they do not.
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
#
# A SCAN THAT COULD NOT RUN IS NOT A CLEAN SCAN (#3296 round-9 finding 1). The first form ended
# `awk … | grep -nE -- "$1" || true`, and that `|| true` swallowed EVERY non-zero status, not just
# grep's "no matches": an unreadable scan target, an `awk` that died, a MALFORMED ERE in the table
# (grep exits 2) — each produced empty output and was reported as "the roborev code path is free
# of this construct". That is the purest form of the defect this whole branch is about, one level
# down: the mechanism that cannot run reports the same green as the mechanism that found nothing.
#
# So the two stages are now measured SEPARATELY and only ONE status means "no findings":
#   * the target must be READABLE before anything is attempted;
#   * the preprocessing awk's own exit status is captured, with its stderr, and any non-zero is an
#     ERROR — never an empty result;
#   * of grep's statuses ONLY 1 means "no matches". 0 means hits; anything else (2 = bad regex or
#     an I/O error) is an ERROR. `grep -c`-style thinking ("no output means clean") is exactly
#     what is being removed here.
# The three states are returned as 0 = hits / 1 = none / 2 = COULD NOT RUN, and state 2 carries a
# named cause in SCAN_ERR. There is deliberately no fourth, permissive state.
SCAN_ERR=''
SCAN_HITS_FILE=''
scan_hits_to() { # scan_hits_to <ere> <file> -> 0 hits (in $SCAN_HITS_FILE) / 1 none / 2 could-not-run
  local _re="$1" _f="$2" _pre="$tmp/scan-pre.txt" _serr="$tmp/scan-stderr.txt" _arc _grc
  SCAN_ERR=''
  SCAN_HITS_FILE="$tmp/scan-hits.txt"
  : >"$SCAN_HITS_FILE"
  if [ ! -f "$_f" ]; then
    SCAN_ERR="the scan target does not exist: $_f"
    return 2
  fi
  if [ ! -r "$_f" ]; then
    SCAN_ERR="the scan target exists but is NOT READABLE: $_f"
    return 2
  fi
  scan_preprocess "$_f" >"$_pre" 2>"$_serr"
  _arc=$?
  if [ "$_arc" -ne 0 ]; then
    SCAN_ERR="the preprocessing awk exited $_arc on $_f (stderr: $(tr '\n' ' ' <"$_serr"))"
    return 2
  fi
  grep -nE -- "$_re" "$_pre" >"$SCAN_HITS_FILE" 2>"$_serr"
  _grc=$?
  case "$_grc" in
    0) return 0 ;;
    1) return 1 ;;
    *)
      SCAN_ERR="grep exited $_grc — neither 0 (matched) nor 1 (no match), so this is an ERROR, not a clean scan — on $_f with ERE '$_re' (stderr: $(tr '\n' ' ' <"$_serr"))"
      return 2 ;;
  esac
}

# scan_found is what every call site uses. It COUNTS the could-not-run state as a failure itself,
# right where the cause is known, and still returns 2 so the caller reports NO verdict: an errored
# scan has no verdict to give, and printing either `ok` or `bad` for it would be a verdict derived
# from an unmeasured state.
scan_found() { # scan_found <ere> <file> -> 0 hits / 1 none / 2 could-not-run (already counted)
  local _rc
  scan_hits_to "$1" "$2"
  _rc=$?
  if [ "$_rc" -eq 2 ]; then
    bad "structural: the scan itself COULD NOT RUN — $SCAN_ERR. A scan that cannot run has no verdict to give, so this is a counted FAILURE and never a clean scan (#3296 round-9)."
  fi
  return "$_rc"
}
scan_first_hit() { head -1 "$SCAN_HITS_FILE"; }
scan_all_hits() { tr '\n' ' ' <"$SCAN_HITS_FILE"; }

scan_preprocess() { # scan_preprocess <file> -> the joined, comment-blanked logical-line stream
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
  ' "$1"
}

# ---------------------------------------------------------------------------
# CONTROLS FOR THE COULD-NOT-RUN STATE ITSELF. A three-state contract is only protection if state 2
# is REACHABLE and COUNTED, so each of its four causes is provoked here and required to be
# classified 2 with a cause that NAMES it — and a clean scan is still required to be state 1, so
# the fix has not turned every scan into an error. Without these the new branches would be dead
# code asserted only in prose, which is the same shape as the defect they close.
# ---------------------------------------------------------------------------
_cnr="$tmp/cnr-clean.sh"
printf '%s\n' '  echo portable' >"$_cnr"
scan_hits_to '(^|[^[:alnum:]_-])stat[[:space:]]+-c' "$_cnr"
if [ $? -eq 1 ]; then
  ok 'scan-status control: a readable target with no match is state 1 (NO FINDINGS) — grep exit 1 is still the one status that means clean'
else
  bad "scan-status control: a clean scan was NOT classified as state 1 (SCAN_ERR: $SCAN_ERR) — the error handling has made every scan an error"
fi
scan_hits_to 'echo' "$_cnr"
if [ $? -eq 0 ] && [ -s "$SCAN_HITS_FILE" ]; then
  ok 'scan-status control: a matching target is state 0 with its hits in $SCAN_HITS_FILE'
else
  bad "scan-status control: a matching target was not classified as state 0 with hits (SCAN_ERR: $SCAN_ERR)"
fi
scan_hits_to 'x' "$tmp/definitely-absent-scan-target.sh"
_cnr_rc=$?
case "$_cnr_rc:$SCAN_ERR" in
  '2:the scan target does not exist'*)
    ok 'scan-status control: a MISSING scan target is state 2 (could-not-run) with a cause naming it — never an empty, clean-looking result' ;;
  *)
    bad "scan-status control: a MISSING scan target was classified rc $_cnr_rc / '$SCAN_ERR' — an absent target must not be reported as a clean scan" ;;
esac
printf '%s\n' '  echo x' >"$tmp/cnr-noread.sh"
chmod 000 "$tmp/cnr-noread.sh" 2>/dev/null
if [ -r "$tmp/cnr-noread.sh" ]; then
  # An affirmatively identified environment limitation (running as root, or a filesystem that
  # ignores mode bits): the file could not be MADE unreadable, so this cause cannot be provoked
  # here. Named, counted, and never reported as a pass.
  skip 'scan-status control: the unreadable-target cause was NOT MEASURED — chmod 000 left the file readable (running as root, or a filesystem that ignores mode bits)'
else
  scan_hits_to 'x' "$tmp/cnr-noread.sh"
  _cnr_rc=$?
  case "$_cnr_rc:$SCAN_ERR" in
    '2:the scan target exists but is NOT READABLE'*)
      ok 'scan-status control: an UNREADABLE scan target is state 2 with a cause naming it (the finding-1 case: `awk … | grep … || true` reported this as a CLEAN SCAN)' ;;
    *)
      bad "scan-status control: an UNREADABLE scan target was classified rc $_cnr_rc / '$SCAN_ERR' — this is the exact case the swallowed exit status reported clean" ;;
  esac
fi
chmod 644 "$tmp/cnr-noread.sh" 2>/dev/null
# A MALFORMED ERE makes grep exit 2. Under the old form that was indistinguishable from "no
# matches", so a typo'd table entry silently certified every file clean, forever.
scan_hits_to '(' "$_cnr"
_cnr_rc=$?
case "$_cnr_rc:$SCAN_ERR" in
  '2:grep exited '*)
    ok 'scan-status control: a MALFORMED ERE (grep exit 2) is state 2 with a cause naming grep’s status — only exit 1 is accepted as "no findings"' ;;
  *)
    bad "scan-status control: a MALFORMED ERE was classified rc $_cnr_rc / '$SCAN_ERR' — a broken pattern would then certify every scanned file clean" ;;
esac
# The PREPROCESSING stage's failure path, provoked by swapping the preprocessor for one that fails
# and restoring the real text afterwards (`declare -f`, bash 3.2 compatible). Corrupting the real
# awk program to test this would be untestable-in-place; this exercises the actual branch.
_real_pre=$(declare -f scan_preprocess)
if [ -z "$_real_pre" ]; then
  bad 'scan-status control: scan_preprocess could not be captured, so the awk-failure branch was NOT MEASURED'
else
  scan_preprocess() { return 3; }
  scan_hits_to 'x' "$_cnr"
  _cnr_rc=$?
  eval "$_real_pre"
  case "$_cnr_rc:$SCAN_ERR" in
    '2:the preprocessing awk exited 3'*)
      ok 'scan-status control: a FAILING preprocessing stage is state 2 with a cause naming its exit status — an awk that dies no longer yields an empty, clean-looking scan' ;;
    *)
      bad "scan-status control: a failing preprocessing stage was classified rc $_cnr_rc / '$SCAN_ERR' — a dead awk would then be reported as a clean scan" ;;
  esac
  # And the restore must have worked, or every scan below runs against a stub.
  scan_hits_to 'echo' "$_cnr"
  if [ $? -eq 0 ]; then
    ok 'scan-status control: the real preprocessor was restored after the injection (the scans below run against the real awk)'
  else
    bad "scan-status control: the preprocessor was NOT restored (SCAN_ERR: $SCAN_ERR) — every scan below would be running against the failing stub"
  fi
fi
# Finally: state 2 must be COUNTED, not merely returned. Measured on the tally itself, with the
# counters restored afterwards so this probe does not red the run it is measuring.
_cnr_p0=$PASS
_cnr_f0=$FAIL
scan_found 'x' "$tmp/definitely-absent-scan-target.sh" >/dev/null 2>&1
_cnr_df=$((FAIL - _cnr_f0))
_cnr_dp=$((PASS - _cnr_p0))
PASS=$_cnr_p0
FAIL=$_cnr_f0
if [ "$_cnr_df" -eq 1 ] && [ "$_cnr_dp" -eq 0 ]; then
  ok 'scan-status control: scan_found COUNTS the could-not-run state as a tally FAILURE (1 failure, 0 passes) — it propagates to the gate rather than being swallowed'
else
  bad "scan-status control: a could-not-run scan produced $_cnr_df failure(s) / $_cnr_dp pass(es) on the tally — it must be exactly one counted FAILURE"
fi

for _ci in "${!CONSTRUCT_RE[@]}"; do
  _re="${CONSTRUCT_RE[$_ci]}"
  _why="${CONSTRUCT_WHY[$_ci]}"
  # POSITIVE CONTROL FIRST: the pattern must detect its own sample violation. Without this a
  # typo'd regex would report every file clean, forever.
  printf '%s\n' "${CONSTRUCT_SAMPLE[$_ci]}" >"$tmp/sample-$_ci.sh"
  scan_found "$_re" "$tmp/sample-$_ci.sh"
  case $? in
    0) ok "structural control: the pattern detects its sample violation (${CONSTRUCT_SAMPLE[$_ci]})" ;;
    1)
      bad "structural control: the pattern MATCHES NOTHING — it cannot detect '${CONSTRUCT_SAMPLE[$_ci]}', so its clean verdict below is vacuous ($_re)"
      continue ;;
    # The could-not-run state is already counted by scan_found; the rule's clean verdict below
    # would be unmeasured, so the rule is skipped over entirely rather than reported either way.
    *) continue ;;
  esac
  _hits=""
  _scan_broke=0
  for _f in "${SCAN_FILES[@]}"; do
    scan_found "$_re" "$_f"
    case $? in
      0) _hits="$_hits $(basename "$_f"):$(scan_first_hit)" ;;
      1) ;;
      *) _scan_broke=1 ;;
    esac
  done
  if [ "$_scan_broke" -eq 1 ]; then
    : # a target could not be scanned: the cause is already a counted FAILURE, and a
      # "free of this construct" verdict over a partially-scanned set would be vacuous.
  elif [ -z "$_hits" ]; then
    ok "structural: the scanned set is free of this construct — $_why"
  else
    bad "structural: GNU-only construct in the scanned set ($_why):$_hits"
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
  "  sed -E -i 's/a/b/' \"\$f\"" >"$tmp/cluster-bad.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
# Asserted against the UNION of the two in-place rules, because that union is what the scan
# actually applies: `-E -i` (separated) is the BARE rule's job and `-Ei` (bundled) is the
# cluster rule's, and what matters to a reader of this file is that NO in-place spelling
# escapes the table. Which rule catches which is pinned separately, immediately below.
_cluster_missed=""
_cluster_broke=0
while IFS= read -r _cl; do
  [ -n "$_cl" ] || continue
  printf '%s\n' "$_cl" >"$tmp/cluster-one.sh"
  # The two rules are consulted with EXPLICIT statuses rather than `! scan_found … && ! scan_found …`:
  # `!` would fold the could-not-run state (2) in with "no match" (1) and report the spelling as
  # MISSED — a verdict about a scan that never ran.
  scan_found "$RE_SED_INPLACE_CLUSTER" "$tmp/cluster-one.sh"
  _c1=$?
  scan_found "$RE_SED_INPLACE" "$tmp/cluster-one.sh"
  _c2=$?
  if [ "$_c1" -eq 2 ] || [ "$_c2" -eq 2 ]; then
    _cluster_broke=1
  elif [ "$_c1" -eq 1 ] && [ "$_c2" -eq 1 ]; then
    _cluster_missed="$_cluster_missed [$_cl]"
  fi
done <"$tmp/cluster-bad.sh"
if [ "$_cluster_broke" -eq 1 ]; then
  : # already counted by scan_found; a MISS/no-MISS verdict here would be unmeasured
elif [ -z "$_cluster_missed" ]; then
  ok 'structural control: every in-place spelling (-Ei, -ni, -nEi, -E -i) is detected by the in-place rules — no bundled form escapes the table'
else
  bad "structural control: the in-place rules MISS:$_cluster_missed — a scanner with a known hole invites reliance it cannot support"
fi
printf '%s\n' "  sed -Ei 's/a/b/' \"\$f\"" >"$tmp/cluster-one.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
scan_found "$RE_SED_INPLACE" "$tmp/cluster-one.sh"
case $? in
  1) ok 'structural control: `sed -Ei` is (still) invisible to the bare -i rule — which is WHY the cluster rule exists, stated as a measurement rather than an assumption' ;;
  0) bad 'structural control: the bare -i rule now also matches `sed -Ei`; fold the two rules together rather than keeping a redundant one' ;;
  *) : ;; # already counted by scan_found
esac

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
scan_found "$RE_SED_INPLACE_CLUSTER" "$tmp/cluster-ok.sh"
case $? in
  1) ok 'structural control: a non-`i` cluster (-n, -E), an ATTACHED suffix (-Ei.bak) and an argument-taking cluster (-fi, -ei, where the i is the OPTION ARGUMENT) are not flagged — the rule reds only on the unportable spelling' ;;
  0) bad "structural control: the cluster rule false-positives on a portable sed — a lint that reds on correct input is the lint agents learn to waive: $(scan_all_hits)" ;;
  *) : ;; # already counted by scan_found
esac

# ---------------------------------------------------------------------------
# CONTROLS FOR THE SPELLINGS THE ADJACENT-FLAGS-ONLY FORM MISSED (roborev round 2). Each named
# form gets its OWN control and each is asserted to FIRE — a rule without a control is a rule
# nobody has tested. Multi-line fixtures are written as separate printf arguments so the
# continuation-joining path is exercised on real newlines, not on an escaped approximation.
# The assertion is against the UNION of the table's rules, because "is this flagged by the
# scan" is the property the guard actually provides.
# ---------------------------------------------------------------------------
# scan_any carries the same three states outward: 0 = some rule hit, 1 = no rule hit, 2 = a scan
# could not run (already counted by scan_found). It ABORTS on state 2 rather than continuing over
# the remaining rules, because a "no rule matched" answer assembled from a partially-executed table
# is exactly the unmeasured-state verdict being removed here.
_scan_any_hits=''
scan_any() { # scan_any <file> -> 0 flagged / 1 clean / 2 could-not-run; hits in $_scan_any_hits
  local _i _any=1
  _scan_any_hits=''
  for _i in "${!CONSTRUCT_RE[@]}"; do
    scan_found "${CONSTRUCT_RE[$_i]}" "$1"
    case $? in
      0) _any=0; _scan_any_hits="$_scan_any_hits $(scan_first_hit)" ;;
      1) ;;
      *) return 2 ;;
    esac
  done
  return "$_any"
}
assert_flagged() { # assert_flagged <label> <file>
  scan_any "$2"
  case $? in
    0) ok "structural control: $1 is FLAGGED" ;;
    1) bad "structural control: $1 is NOT flagged — the scan has a hole at this spelling, and a guard with a known-but-hidden miss invites reliance it cannot support" ;;
    *) : ;; # the cause is already a counted FAILURE; there is no verdict to give here
  esac
}
assert_not_flagged() { # assert_not_flagged <label> <file>
  scan_any "$2"
  case $? in
    1) ok "structural control: $1 is correctly NOT flagged" ;;
    0) bad "structural control: $1 was flagged — a lint that reds on correct input is the lint agents learn to waive:$_scan_any_hits" ;;
    *) : ;; # the cause is already a counted FAILURE; there is no verdict to give here
  esac
}

# (a) -i AFTER an intervening option argument. BSD eats `file` as the backup suffix.
printf '%s\n' "  sed -e 's/a/b/' -i file" >"$tmp/sp-optrun.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`sed -e EXPR -i file` (-i beyond the adjacent option run)' "$tmp/sp-optrun.sh" # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
# (b) a LINE-BROKEN invocation — invisible to any single-line ERE, hence the joiner.
printf '%s\n' '  sed \' '    -i '"'"'s/a/b/'"'"' file' >"$tmp/sp-break.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`sed \` + newline + `-i …` (backslash-continued across lines)' "$tmp/sp-break.sh"
printf '%s\n' '  sed -e '"'"'s/a/b/'"'"' \' '    -Ei file' >"$tmp/sp-break2.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged 'a line-broken invocation whose continuation carries a BUNDLED -Ei' "$tmp/sp-break2.sh"
# (c) the delimiter argument SEPARATED from -d, with no operand.
printf '%s\n' '  order=$(paste -d ,)' >"$tmp/sp-darg.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -d ,` (separated delimiter argument, still no file operand)' "$tmp/sp-darg.sh"
# (d) a REDIRECTION is not an operand — BSD usage()-errors just the same.
printf '%s\n' '  order=$(paste -sd, < input)' >"$tmp/sp-redir.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, < input` (a redirection is not a file operand)' "$tmp/sp-redir.sh"
printf '%s\n' '  order=$(paste -sd, <"$f")' >"$tmp/sp-redir2.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, <"$f"` (redirection with no space)' "$tmp/sp-redir2.sh"
# (d2) OUTPUT and FILE-DESCRIPTOR redirections are not operands either — one control per spelling,
# because an input-only grammar reported every one of these CLEAN (#3296 round-8 finding 3).
printf '%s\n' '  paste -sd, >output' >"$tmp/sp-redir-out.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, >output` (an OUTPUT redirection is not a file operand)' "$tmp/sp-redir-out.sh"
printf '%s\n' '  paste -sd, >>output' >"$tmp/sp-redir-app.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, >>output` (an APPEND redirection is not a file operand)' "$tmp/sp-redir-app.sh"
printf '%s\n' '  paste -sd, 2>/dev/null' >"$tmp/sp-redir-fd.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, 2>/dev/null` (a FILE-DESCRIPTOR redirection is not a file operand)' "$tmp/sp-redir-fd.sh"
printf '%s\n' '  paste -sd, >out 2>&1' >"$tmp/sp-redir-both.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, >out 2>&1` (a RUN of redirections, one of them an fd duplication)' "$tmp/sp-redir-both.sh"
printf '%s\n' '  paste -sd, <in >out' >"$tmp/sp-redir-inout.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`paste -sd, <in >out` (input AND output redirection, still no operand)' "$tmp/sp-redir-inout.sh"
printf '%s\n' '  order=$(paste -sd, >"$out")' >"$tmp/sp-redir-subst.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`order=$(paste -sd, >"$out")` (output redirection inside a command substitution)' "$tmp/sp-redir-subst.sh" # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
# And the NEGATIVE direction for the widened grammar, which is where widening can go wrong: a
# paste that HAS an operand must stay unflagged even when it also redirects.
printf '%s\n' '  paste -sd, "$f" >out' >"$tmp/sp-redir-okfile.sh"
assert_not_flagged '`paste -sd, "$f" >out` (an operand AND a redirection — portable)' "$tmp/sp-redir-okfile.sh"
printf '%s\n' '  paste -sd, - 2>/dev/null' >"$tmp/sp-redir-okdash.sh"
assert_not_flagged '`paste -sd, - 2>/dev/null` (the explicit `-` stdin operand plus an fd redirection)' "$tmp/sp-redir-okdash.sh"
printf '%s\n' '#  paste -sd, >output and paste -sd, 2>/dev/null are both banned' >"$tmp/sp-redir-cmt.sh"
assert_not_flagged 'the redirection spellings named in a COMMENT (prose, not an invocation)' "$tmp/sp-redir-cmt.sh"
# (e) the GNU long spelling, absent from BSD sed entirely.
printf '%s\n' "  sed --in-place -e 's/a/b/' file" >"$tmp/sp-long.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`sed --in-place` (GNU long option; BSD sed has no such flag)' "$tmp/sp-long.sh"
# (f) the long spelling with an ATTACHED `=SUFFIX`, in BOTH positions — adjacent to `sed` and
# beyond an intervening option. A long-option rule that requires WHITESPACE after the option name
# sees neither; this table's does not, and both positions are now pinned by measurement instead of
# left to inspection of the regex (#3296 round-8 finding 2).
printf '%s\n' "  sed --in-place=.bak -e 's/a/b/' file" >"$tmp/sp-long-eq.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`sed --in-place=.bak …` (long option with an ATTACHED suffix, adjacent to sed)' "$tmp/sp-long-eq.sh" # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
printf '%s\n' "  sed -e 's/a/b/' --in-place=.bak file" >"$tmp/sp-long-eq2.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged '`sed -e EXPR --in-place=.bak file` (attached-suffix long option BEYOND an intervening option)' "$tmp/sp-long-eq2.sh" # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
# (g) the EMPTY-SUFFIX spellings beyond the adjacent position. Before the empty-suffix rules were
# given the same option-run handling as the bare `-i` rule, these were invisible to EVERY rule in
# the table — the hole this control now closes and keeps closed.
printf '%s\n' "  sed -e 's/a/b/' -i'' file" >"$tmp/sp-empty-sq.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged "\`sed -e EXPR -i'' file\` (empty SINGLE-quoted suffix beyond the adjacent position)" "$tmp/sp-empty-sq.sh" # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
printf '%s\n' "  sed -e 's/a/b/' -i\"\" file" >"$tmp/sp-empty-dq.sh"
assert_flagged '`sed -e EXPR -i"" file` (empty DOUBLE-quoted suffix beyond the adjacent position)' "$tmp/sp-empty-dq.sh" # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
# (g2) THE OTHER SIDE OF THE SAME TOKEN — a CONCATENATED ATTACHED SUFFIX (#3296 round-9 review
# false positive). `-i''.bak` and `-i"".bak` survive shell quote removal as `-i.bak`, the attached
# form BOTH seds read identically, so they are PORTABLE and must NOT be reported. These are
# PERMANENT negative controls: the rules distinguish the two spellings only by the token boundary
# after the closing quotes, and nothing but a control keeps that boundary in place. They are
# deliberately paired with the (g) positives above — a regex change that widens the escape to make
# these pass would break those, and one that drops the boundary again would break these.
printf '%s\n' "  sed -i''.bak -e 's/a/b/' file" >"$tmp/sp-attach-sq.sh"
assert_not_flagged "\`sed -i''.bak …\` (quote removal yields the PORTABLE attached suffix -i.bak)" "$tmp/sp-attach-sq.sh"
printf '%s\n' "  sed -i\"\".bak -e 's/a/b/' file" >"$tmp/sp-attach-dq.sh"
assert_not_flagged '`sed -i"".bak …` (quote removal yields the PORTABLE attached suffix -i.bak)' "$tmp/sp-attach-dq.sh"
printf '%s\n' "  sed -e 's/a/b/' -i''.bak file" >"$tmp/sp-attach-sq2.sh"
assert_not_flagged "\`sed -e EXPR -i''.bak file\` (portable attached suffix BEYOND an option run — the widened option run must not resurrect the false positive)" "$tmp/sp-attach-sq2.sh"
printf '%s\n' "  sed -i''.bak" >"$tmp/sp-attach-eol.sh"
assert_not_flagged "\`sed -i''.bak\` at end-of-line (the boundary set must not treat the suffix as a token end)" "$tmp/sp-attach-eol.sh"
# And the boundaries that are NOT whitespace must still FLAG the bare empty suffix, or the fix
# above would have bought its precision by losing coverage at end-of-line, before `)` and before `;`.
printf '%s\n' "  sed -i''" >"$tmp/sp-empty-eol.sh"
assert_flagged "a bare \`sed -i''\` at END-OF-LINE (boundary \$)" "$tmp/sp-empty-eol.sh"
printf '%s\n' "  order=\$(sed -i'' f)" >"$tmp/sp-empty-paren.sh" # portability-lint-allow: deliberate fixture: the bare empty-suffix spelling this control must DETECT
assert_flagged "a bare \`sed -i''\` inside a command substitution (boundary \`)\`)" "$tmp/sp-empty-paren.sh"
printf '%s\n' "  sed -i''; echo done" >"$tmp/sp-empty-semi.sh" # portability-lint-allow: deliberate fixture: the bare empty-suffix spelling this control must DETECT
assert_flagged "a bare \`sed -i''\` terminated by a semicolon (boundary \`;\`)" "$tmp/sp-empty-semi.sh"
# The widening must not reach past a shell metacharacter here either: an empty-suffix `-i` on the
# far side of a pipe belongs to a DIFFERENT command.
printf '%s\n' '  sed '"'"'s/x/y/'"'"' f | grep -i'"''"' foo' >"$tmp/sp-empty-pipe.sh"
assert_not_flagged "a \`grep -i''\` AFTER a pipe (the empty-suffix rules stop at the metacharacter too)" "$tmp/sp-empty-pipe.sh"
# And the same spellings in PROSE stay invisible: this repo documents the constructs it bans, so a
# comment naming one is not an invocation.
printf '%s\n' "# prose: sed -e 's/a/b/' -i'' file, and sed --in-place=.bak, are both banned here" \
  >"$tmp/sp-new-cmt.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_not_flagged 'the new in-place spellings named in a COMMENT (prose about a banned construct is not an invocation)' "$tmp/sp-new-cmt.sh"

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
  >"$tmp/sp-cmtbreak2.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
assert_flagged 'a REAL `sed -i` on the line beneath a backslash-ended COMMENT (joining it into the comment would MASK it)' "$tmp/sp-cmtbreak2.sh"

# The joiner rewrites the stream, so LINE NUMBERS are asserted rather than assumed: a report
# naming the wrong line sends the next reader to the wrong place.
printf '%s\n' '# a comment' '' "  sed -i 's/a/b/' f" '  echo tail' >"$tmp/sp-lineno.sh" # portability-lint-allow: deliberate fixture: the unportable spelling this control must DETECT
scan_found "$RE_SED_INPLACE" "$tmp/sp-lineno.sh"
case $? in
  0)
    _ln=$(scan_first_hit)
    if [ "${_ln%%:*}" = 3 ]; then
      ok 'structural control: a hit is reported at its REAL file line (blanking, not deleting, keeps numbering exact)'
    else
      bad "structural control: the hit was reported at line '${_ln%%:*}' but lives at line 3 — the scan's line numbers do not name the real file"
    fi ;;
  1) bad "structural control: the line-numbering fixture produced NO hit at all, so the scan's line numbering was NOT MEASURED — which is a failure, not a pass" ;;
  *) : ;; # already counted by scan_found
esac

# NEGATIVE CONTROL for the paste pattern, whose ERE is the subtlest of the table: a paste WITH
# an explicit operand is portable and must NOT be reported.
printf '%s\n' '  order=$(grep -n x f | cut -d: -f2 | paste -sd, -)' >"$tmp/paste-ok.sh"
printf '%s\n' '  order=$(paste -sd, "$f")' >>"$tmp/paste-ok.sh"
scan_found "$RE_PASTE_NO_OPERAND" "$tmp/paste-ok.sh"
case $? in
  1) ok 'structural control: a paste WITH a file operand (`-` or a path) is not flagged' ;;
  0) bad "structural control: the paste pattern false-positives on a portable paste with an operand — a lint that reds on correct input is the lint agents learn to waive: $(scan_all_hits)" ;;
  *) : ;; # already counted by scan_found
esac

# CONTROL for the escape marker, in BOTH directions: it must exempt the line it is on, and it
# must not be a blanket switch (the same sample WITHOUT the marker is still detected above).
printf '%s\n' "  sed -i 's/a/b/' \"\$f\"   # portability-lint-allow: deliberate BSD-emulation control" \
  >"$tmp/allow.sh"
scan_found "$RE_SED_INPLACE" "$tmp/allow.sh"
case $? in
  1) ok 'structural control: a line marked portability-lint-allow is exempt (a visible, per-line escape)' ;;
  0) bad 'structural control: the portability-lint-allow marker does not exempt its line' ;;
  *) : ;; # already counted by scan_found
esac

# ---------------------------------------------------------------------------
# THE SELF-SCAN, AND THE TWO WAYS IT COULD BE VACUOUS (#3296 round-8 finding 1).
#
# This file is one of the SCAN_FILES: it runs inside the macOS-sensitive `roborev-lints`
# component and is mostly shell scaffolding, so a GNU-only construct added HERE would reproduce
# the exact Linux-green/macOS-red regression it exists to prevent, and nothing else would catch
# it. But it also DELIBERATELY contains the banned constructs — in its BSD-emulation fixtures and
# its positive controls — each exempted by a per-line `portability-lint-allow` marker rather than
# by any blanket rule. A self-scan of a file full of sanctioned violations can go green two ways
# that have nothing to do with the file being clean, so BOTH are measured here rather than
# asserted in prose:
#   (i)  EXEMPT IN EFFECT — this file dropped from SCAN_FILES, or so heavily marked that nothing
#        in it is scanned any more. Controlled by appending an UNMARKED violation to a COPY of
#        this file: it must be FLAGGED.
#   (ii) DECORATIVE MARKERS — if the rules did not in fact match this file's own fixtures, the
#        green would come from the rules MISSING them, not from the exemption doing its job, and
#        the markers would be hiding nothing. Controlled by neutralising the marker token in a
#        copy: the file must then be FLAGGED, i.e. the markers are load-bearing.
# ---------------------------------------------------------------------------
_self="$SCRIPT_DIR/$(basename "$0")"
_self_in_scan=no
for _sf in "${SCAN_FILES[@]}"; do
  [ "$_sf" = "$_self" ] && _self_in_scan=yes
done
if [ "$_self_in_scan" = yes ]; then
  ok 'self-scan: this file is one of the SCAN_FILES — a GNU-only construct added to the portability guard itself FAILs the same component it gates'
else
  bad 'self-scan: this file is NOT in SCAN_FILES — the guard that runs in the macOS-sensitive roborev-lints component does not check its own scaffolding, which is where #3296 lived'
fi
cat "$_self" >"$tmp/self-unmarked.sh"
printf '%s\n' "  sed -i 's/a/b/' \"\$f\"" >>"$tmp/self-unmarked.sh" # portability-lint-allow: writes an UNMARKED violation into the self-scan copy on purpose
assert_flagged 'an UNMARKED violation appended to a COPY of this file (so the self-scan is a real scan, not a blanket exemption)' "$tmp/self-unmarked.sh"
awk '{ gsub(/portability-lint-allow/, "portability-lint-NEUTRALISED"); print }' "$_self" >"$tmp/self-nomarker.sh"
assert_flagged "this file with its exemption markers NEUTRALISED (proving the markers are load-bearing — the rules really do match this file's own deliberate fixtures)" "$tmp/self-nomarker.sh"

# ---------------------------------------------------------------------------
# THE BOOTSTRAP PAIR IS IN SCOPE, AND THE RULE THAT MISSED IT IS PROVED TO FIRE THERE (#3756).
#
# `xargs -0 -r` shipped in test_bootstrap_agent_machine.sh's tree-identity digest and was caught
# by a human reviewer, not by this lint — which has carried the `xargs -r` rule verbatim since
# #3296. The rule was fine; the SUBJECT SET was the gap, and an enumerated set that declares its
# own non-exhaustiveness is honest without being coverage. Both files are in SCAN_FILES now, and
# both halves of that are asserted here rather than assumed:
#   (i)  MEMBERSHIP — dropping either file from SCAN_FILES must FAIL, the same shape as the
#        self-scan above. Without it a future edit could quietly restore the gap.
#   (ii) THE RULE ACTUALLY FIRES THERE — membership proves the file is passed to `grep`, not that
#        the incident's own construct would be caught in it. So the incident construct is PLANTED
#        into a throwaway COPY of each file and the scan must flag it AND NAME it: a bare "some
#        rule matched" is not evidence, since the copy is 3000+ lines of real script and an
#        unrelated rule firing would produce an identical verdict.
# The pristine copies are asserted CLEAN of the same rule first — otherwise the planted verdict
# could be inherited from a pre-existing hit and the plant would prove nothing.
# ---------------------------------------------------------------------------
_bs_i=0
for _bs_f in "$BOOTSTRAP_SH" "$BOOTSTRAP_TEST"; do
  _bs_i=$((_bs_i + 1))
  _bs_name=$(basename "$_bs_f")
  _bs_member=no
  for _sf in "${SCAN_FILES[@]}"; do
    [ "$_sf" = "$_bs_f" ] && _bs_member=yes
  done
  if [ "$_bs_member" = yes ]; then
    ok "bootstrap-scope: $_bs_name is one of the SCAN_FILES — the #3756 gap (a GNU-only idiom this lint already knows about, shipped because the file was never scanned) cannot silently reopen"
  else
    bad "bootstrap-scope: $_bs_name is NOT in SCAN_FILES — this is the #3756 gap, reopened"
    continue
  fi
  if [ ! -f "$_bs_f" ]; then
    bad "bootstrap-scope: $_bs_name does not exist at $_bs_f — the plant control below has no subject, so its verdict would be unearned"
    continue
  fi
  # (a) the pristine file must be CLEAN of the incident rule, or (b) proves nothing.
  scan_found "$RE_XARGS_R" "$_bs_f"
  case $? in
    1) ok "bootstrap-scope: $_bs_name is clean of the \`xargs -r\` rule today — so the planted hit below is attributable to the plant and not inherited" ;; # portability-lint-allow: the rule NAME in a diagnostic string, not an invocation
    0) bad "bootstrap-scope: $_bs_name already matches the \`xargs -r\` rule ($(scan_all_hits)) — fix it; until then the plant control below cannot attribute its hit"; continue ;; # portability-lint-allow: the rule NAME in a diagnostic string, not an invocation
    *) continue ;; # already counted by scan_found
  esac
  # (b) plant the INCIDENT construct into a copy and require the scan to NAME it.
  _bs_copy="$tmp/bootstrap-planted-$_bs_i.sh"
  cat "$_bs_f" >"$_bs_copy"
  # A blank line FIRST: a source file with no trailing newline would otherwise join the plant
  # onto its last physical line, where an enclosing quote or comment could hide it — the plant
  # would go undetected and this control would report a hole that does not exist.
  printf '\n' >>"$_bs_copy"
  printf '%s\n' '  printf "" | xargs -r rm' >>"$_bs_copy" # portability-lint-allow: plants the #3756 incident construct into a THROWAWAY COPY on purpose
  scan_found "$RE_XARGS_R" "$_bs_copy"
  case $? in
    0)
      _bs_hit=$(scan_first_hit)
      case "$_bs_hit" in
        *"xargs -r"*) # portability-lint-allow: the planted construct as a MATCH PATTERN, not an invocation
          ok "bootstrap-scope: the \`xargs -r\` rule FIRES on a copy of $_bs_name and NAMES the planted line ($_bs_hit) — the #3756 incident construct would now red the gate instead of a reviewer" ;; # portability-lint-allow: the rule NAME in a diagnostic string, not an invocation
        *)
          bad "bootstrap-scope: a hit was reported on the planted copy of $_bs_name but it does not name the planted construct ($_bs_hit) — a bare red is not evidence, an unrelated match produces the same verdict" ;;
      esac ;;
    1) bad "bootstrap-scope: the \`xargs -r\` rule does NOT fire on a copy of $_bs_name carrying the #3756 construct — membership without detection is the gap wearing a scan's clothes" ;; # portability-lint-allow: the rule NAME in a diagnostic string, not an invocation
    *) : ;; # already counted by scan_found
  esac
done

# ---------------------------------------------------------------------------
# THE SCOPE DECLARATION IS ASSERTED, NOT JUST PRINTED (#3756 AC2). A declaration nothing
# checks is a comment that happens to reach stdout: delete it, mis-spell a member's path, or
# let the census silently degrade, and no test would notice. Three properties, each the one
# that makes the declaration mean something:
#   (i)   EVERY member is named. A scanned file missing from the declaration understates the
#         scope, which is the safe direction for a reader but still a lie about what ran.
#   (ii)  THE ARITHMETIC IS CONSISTENT. `unscanned` + the `.sh` members of SCAN_FILES must
#         equal the tracked total. This is the control that would have caught the real defect
#         found while writing it: members spelled `scripts/tests/../flow/x.sh` compared
#         unequal to the tracked path, so four scanned files counted as UNSCANNED and the
#         declaration OVERSTATED the gap by 4 while looking entirely plausible.
#   (iii) AN UNTAKEABLE CENSUS SAYS SO. The count is the one number a reader will quote, so
#         "could not measure" must never render as a number — the standing rule against
#         deriving a verdict from the absence of a signal, applied to a scope line.
# ---------------------------------------------------------------------------
emit_scope_declaration >"$tmp/scope.txt" 2>&1
_scope_missing=''
for _scope_f in "${SCAN_FILES[@]}"; do
  grep -qF -- "scanned:   ${_scope_f#"$REPO_ROOT/"}" "$tmp/scope.txt" \
    || _scope_missing="$_scope_missing ${_scope_f#"$REPO_ROOT/"}"
done
if [ -z "$_scope_missing" ]; then
  ok "scope: the run-time declaration names every one of the ${#SCAN_FILES[@]} SCAN_FILES members — a reader of a green run learns exactly what it covered"
else
  bad "scope: the run-time declaration omits scanned file(s):$_scope_missing — a declaration that understates its own subject set is not a scope statement"
fi
_scope_unscanned=$(grep -c '^unscanned: ' "$tmp/scope.txt")
if [ "${_scope_unscanned:-0}" = 1 ]; then
  ok 'scope: exactly one unscanned: line is emitted (a reader has one number to quote, not zero and not several)'
else
  bad "scope: the declaration emitted ${_scope_unscanned:-0} unscanned: lines — it must emit exactly one"
fi
_scope_line=$(grep '^unscanned: ' "$tmp/scope.txt" | head -1)
case "$_scope_line" in
  'unscanned: NOT MEASURED'*)
    skip "scope: the tracked-script census could not be taken on this host, so the arithmetic control below has no subject ($_scope_line)" ;;
  *)
    _scope_n=$(printf '%s\n' "$_scope_line" | awk '{ print $2 }')
    _scope_m=$(printf '%s\n' "$_scope_line" | awk '{ print $4 }')
    # Count the SCAN_FILES members that the census could possibly have enumerated: tracked,
    # under scripts/, and ending .sh. Anything else (roborev-job-facts.py) is outside the
    # census's own subject and must NOT be expected to reduce the unscanned count.
    _scope_sh=0
    for _scope_f in "${SCAN_FILES[@]}"; do
      case "${_scope_f#"$REPO_ROOT/"}" in
        scripts/*.sh) _scope_sh=$((_scope_sh + 1)) ;;
      esac
    done
    if [ "$((_scope_n + _scope_sh))" = "$_scope_m" ]; then
      ok "scope: the census arithmetic is consistent — $_scope_n unscanned + $_scope_sh scanned .sh members = $_scope_m tracked, so no member is being miscounted through a path-spelling mismatch"
    else
      bad "scope: the census arithmetic does NOT close — $_scope_n unscanned + $_scope_sh scanned .sh members != $_scope_m tracked. A member whose path spelling differs from the tracked path counts as UNSCANNED, overstating the gap while reading as plausible"
    fi ;;
esac
# (iii) the untakeable census renders as NOT MEASURED, never as a number.
mkdir -p "$tmp/not-a-repo"
_scope_nm=$(_scope_unscanned_line "$tmp/not-a-repo")
case "$_scope_nm" in
  *'NOT MEASURED'*)
    case "$_scope_nm" in
      *' of '*' tracked '*) bad "scope control: an untakeable census printed NOT MEASURED but ALSO a count ($_scope_nm) — the number is what a reader quotes" ;;
      *) ok 'scope control: a census that CANNOT be taken renders NOT MEASURED and emits no number — an unmeasurable scope is never dressed as a measured one' ;;
    esac ;;
  *) bad "scope control: a census taken outside any repository still produced a numeric scope line ($_scope_nm) — that is a fabricated measurement" ;;
esac

# NEGATIVE CONTROL for the timeout rule (#3756): `command -v timeout` is the REMEDY this rule's
# own message recommends, and the old `[0-9]` form matched the `2` of its `2>/dev/null`. Both
# directions are pinned — the guard must be clean, the real invocation must still be flagged —
# because a false-positive fix that also loses the true positive is not a fix.
printf '%s\n' \
  '  bound=$(command -v timeout 2>/dev/null || true)' \
  '  if [ "$(command -v timeout 2>/dev/null)" != "" ]; then' \
  '  exec 2>/dev/null' >"$tmp/timeout-ok.sh"
scan_found "$RE_TIMEOUT_UNGUARDED" "$tmp/timeout-ok.sh"
case $? in
  1) ok 'structural control: `command -v timeout 2>/dev/null` — the guard this rule RECOMMENDS — is not flagged; a digit followed by `>` is a redirection fd, not a duration' ;;
  0) bad "structural control: the timeout rule flags its own recommended guard — a lint that reds on the remedy it prints is the lint agents learn to waive: $(scan_all_hits)" ;;
  *) : ;; # already counted by scan_found
esac
printf '%s\n' \
  '  timeout 30 some-command' \
  '  timeout 5m other-command' \
  '  timeout 180 bash "$GATE"' >"$tmp/timeout-bad.sh" # portability-lint-allow: deliberate fixtures: the unportable spellings this control must DETECT
scan_found "$RE_TIMEOUT_UNGUARDED" "$tmp/timeout-bad.sh"
case $? in
  0) ok 'structural control: real `timeout <duration> cmd` invocations (bare seconds, a GNU suffix, a longer duration) are still FLAGGED — the false-positive fix did not lose the true positive' ;;
  1) bad 'structural control: the timeout rule no longer detects a real timeout(1) invocation — the false-positive fix narrowed it past its subject' ;;
  *) : ;; # already counted by scan_found
esac

# ===========================================================================
# (2) THE BSD SHIMS, and the controls that prove they reproduce the reported defects.
# ===========================================================================
shim="$tmp/shim"
mkdir -p "$shim"
REAL_SED=$(command -v sed || printf '')
REAL_PASTE=$(command -v paste || printf '') # portability-lint-allow: names the binary for the shim; `command -v` does not invoke paste
if [ -z "$REAL_SED" ] || [ -z "$REAL_PASTE" ]; then
  bad 'shim setup: sed/paste not found on PATH — the differential cannot run (this is a failure to measure, not a measurement)'
fi

{
  printf '#!/usr/bin/env bash\n# BSD/macOS sed emulation, -i only (see test_roborev_guard_portability.sh).\n' # portability-lint-allow: prose inside the shim script this printf writes, not an invocation
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
if PATH="$SHIM_PATH" sed -i 's/foo/bar/' "$tmp/ctl-sed.txt" 2>/dev/null; then # portability-lint-allow: BSD-shim control: this `sed -i` MUST run, to prove the shim reproduces #3296
  bad 'shim control: `sed -i EXPR FILE` SUCCEEDED under the BSD shim — the shim does not emulate BSD, so the differential below would prove nothing' # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
elif [ "$(cat "$tmp/ctl-sed.txt")" = foo ]; then
  ok 'shim control: under BSD -i semantics `sed -i EXPR FILE` fails AND leaves the file unpatched (the #3296 root cause 1, reproduced on this platform)' # portability-lint-allow: the construct is named in this assertion LABEL, not invoked
else
  bad "shim control: the file changed despite the failure: $(cat "$tmp/ctl-sed.txt")"
fi

# CONTROL B: the paste shim must reproduce case (j2) — empty stdout, non-zero exit.
_ctl_paste=$(printf 'a\nb\n' | PATH="$SHIM_PATH" paste -sd, 2>/dev/null) # portability-lint-allow: BSD-shim control: the operand-less paste IS the defect being reproduced
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

# `sed_inplace_verified` is extracted and exercised too (#3296 round-9): it is the helper the
# caller contract PREFERS for every mutate-then-assert case, it wraps `sed_inplace`, and running it
# under the BSD sed shim costs one more name in this list. That widens the behavioural coverage by
# one real call site — which is the only honest way to widen the backstop claim in the scope
# statement above.
for _fn in sed_inplace sed_inplace_verified summary_key_order; do
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

if ! declare -f sed_inplace >/dev/null || ! declare -f sed_inplace_verified >/dev/null ||
  ! declare -f summary_key_order >/dev/null; then
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

  # --- sed_inplace_verified UNDER THE BSD sed SHIM (#3296 round-9): the helper the caller contract
  # prefers, exercised behaviourally rather than only pinned by text. All three of its affirmative
  # facts are covered — the edit landed, the wanted post-edit state is PRESENT, and the state it
  # replaces is GONE — because a helper that returns 0 without checking them would let a case assert
  # against content the edit never produced.
  printf 'TIER1="PASS"\n' >"$tmp/ver.txt"
  if PATH="$SHIM_PATH" sed_inplace_verified "$tmp/ver.txt" \
    's/^TIER1="PASS"$/TIER1="MEASUREMENT-DID-NOT-HAPPEN"/' \
    'TIER1="MEASUREMENT-DID-NOT-HAPPEN"' 'TIER1="PASS"' &&
    grep -qF 'TIER1="MEASUREMENT-DID-NOT-HAPPEN"' "$tmp/ver.txt"; then
    ok 'sed_inplace_verified: a mutation whose wanted state is present and whose replaced state is gone SUCCEEDS under BSD sed semantics'
  else
    bad "sed_inplace_verified: the verified mutation did not land under the BSD shim: $(cat "$tmp/ver.txt")"
  fi
  # The edit LANDS but the wanted state is NOT what was asked for: must be non-zero, or "the edit
  # ran" would be mistaken for "the edit did what I meant".
  printf 'TIER1="PASS"\n' >"$tmp/ver2.txt"
  if PATH="$SHIM_PATH" sed_inplace_verified "$tmp/ver2.txt" \
    's/^TIER1="PASS"$/TIER1="SOMETHING-ELSE"/' 'TIER1="WHAT-THE-CALLER-WANTED"'; then
    bad 'sed_inplace_verified: an edit that landed but produced a DIFFERENT state returned SUCCESS — the wanted-state check is not enforced under BSD semantics'
  else
    ok 'sed_inplace_verified: an edit that lands but does NOT produce the wanted state returns NON-ZERO (the affirmative post-edit fact is enforced, not assumed)'
  fi
  # And the must-be-ABSENT direction: the edit landed and the wanted state is present, but the
  # state that was supposed to be replaced is still in the file.
  printf 'TIER1="PASS"\nTIER1="PASS"\n' >"$tmp/ver3.txt"
  if PATH="$SHIM_PATH" sed_inplace_verified "$tmp/ver3.txt" \
    '1s/^TIER1="PASS"$/TIER1="NEW"/' 'TIER1="NEW"' 'TIER1="PASS"'; then
    bad 'sed_inplace_verified: a mutation that left the state it was supposed to REPLACE still in the file returned SUCCESS — the must-be-absent fact is not enforced'
  else
    ok 'sed_inplace_verified: a mutation that leaves the REPLACED state behind returns NON-ZERO (all three affirmative facts are required, under BSD semantics)'
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
  # setup is a fail-closed chain and `_canon` must be non-empty, ABSOLUTE, and actually resolve
  # to the fixture.
  #
  # MEASURING IT WAS NOT ENOUGH, BECAUSE THE MEASUREMENT'S FAILURE BRANCH WAS A SKIP (#3296
  # round-9 finding 3, a RE-OCCURRENCE in the same place as the round-3 commit titled "AC3 fixture
  # setup must be measured"). Every cause — a broken command, a failed write, a canonicalisation
  # error — became a counted SKIP while the script still exited 0, so a regression in fixture setup
  # SILENTLY DISABLED all four canonical-path contract probes without failing the gate. CLAUDE.md
  # is explicit: a SKIP means the check never ran, which IS the vacuous pass itself. Turning a
  # per-cause measurement into one permissive bucket reproduces the very defect being measured.
  #
  # So the two are now DIFFERENT KINDS OF THING and are classified before the chain runs:
  #   * an ENVIRONMENT LIMITATION — affirmatively identified, by name, by a dedicated CAPABILITY
  #     PROBE, and only for the two conditions a supported host may legitimately lack: `git`, and
  #     a filesystem that can create symlinks. That is the ONLY route to `skip`.
  #   * EVERYTHING ELSE is a FAILURE of this file's own fixture, and goes to `bad`.
  # A cause nobody enumerated is therefore a red, not a silent skip — noise, never blindness.
  # THE TWO CAUSES ARE SEPARATELY ANSWERABLE PROBES (#3296 round-11). They used to be ONE function
  # that returned the FIRST limitation it found, git BEFORE symlinks. So on a host without git the
  # symlink control below asked that composite probe, received the GIT cause, saw a non-empty string
  # and reported the symlink-skip branch "reachable and named" — having never executed the symlink
  # probe at all. A control that claims a measurement it did not perform is this branch's entire
  # subject, so the fix is applied twice over: the probes are SPLIT, so the symlink control can ask
  # the symlink question directly and a git answer is structurally impossible; AND the control
  # asserts that the cause it received IDENTIFIES SYMLINK CREATION, so any future refactor that
  # re-composes them cannot silently satisfy it again. Splitting alone would be undone by such a
  # refactor; the cause assertion alone would still be reading a composite answer.
  #
  # The message text lives in its own function so a control can feed the REAL git cause to the
  # acceptance test without hand-copying a duplicate that could drift out of step with it.
  cf_git_limitation_message() {
    printf 'git is not installed on this host, and the case (f) fixture is a git repository'
  }
  cf_git_limitation() { # -> the NAMED git limitation on stdout, else nothing
    command -v git >/dev/null 2>&1 && return 0
    cf_git_limitation_message
  }
  cf_symlink_limitation() { # cf_symlink_limitation <scratch-dir> -> the NAMED symlink limitation, else nothing
    # `ln -s` needs no existing target, so a dangling link is a sufficient capability probe.
    if ln -s "$1/cf-symlink-probe-target" "$1/cf-symlink-probe" 2>/dev/null &&
      [ -L "$1/cf-symlink-probe" ]; then
      rm -f "$1/cf-symlink-probe"
      return 0
    fi
    printf 'symlinks cannot be created under %s, and the fixture needs one to reproduce the macOS /var -> /private/var split' "$1"
  }
  # The composite remains, for the setup chain, which legitimately wants EITHER cause. It is only
  # the CONTROLS that must not use it.
  cf_env_limitation() { # cf_env_limitation <scratch-dir> -> the first NAMED limitation, else nothing
    local _l
    _l=$(cf_git_limitation)
    if [ -n "$_l" ]; then
      printf '%s' "$_l"
      return 0
    fi
    cf_symlink_limitation "$1"
  }
  # The acceptance test for "this cause really is about symlinks", used by the control below and
  # itself controlled in both directions — a test that accepted everything would re-admit the very
  # defect being fixed, and one that accepted nothing would make the symlink control dead code.
  cf_cause_is_symlink() { # cf_cause_is_symlink <cause> -> 0 only if it identifies symlink creation
    case "$1" in
      'symlinks cannot be created under '*) return 0 ;;
      *) return 1 ;;
    esac
  }
  # THE CLASSIFIER IS CONTROLLED IN BOTH DIRECTIONS, because a skip route that can never fire is
  # dead code, and one that fires on a healthy host would silently disable the probes below.
  _cf_probe_ok=$(cf_env_limitation "$tmp")
  if [ -z "$_cf_probe_ok" ]; then
    ok 'AC3 control: no environment limitation is detected on this host, so the four case (f) contract probes below really RUN (a skip here would be the vacuous pass)'
  else
    skip "AC3 control: an environment limitation is present on this host ($_cf_probe_ok), so the case (f) probes cannot run here"
  fi
  # THE FINDING ITSELF, PINNED: the GIT cause must not be able to satisfy the symlink control. Fed
  # the real git message — read from its function, never a hand-copied duplicate — the acceptance
  # test must REJECT it. This control is privilege- and platform-independent, so it runs everywhere
  # the composite probe's ordering could have hidden an unexercised symlink branch.
  if cf_cause_is_symlink "$(cf_git_limitation_message)"; then
    bad 'AC3 control: the GIT limitation message SATISFIES the symlink acceptance test — on a host without git the symlink-skip branch would be reported measured while never being exercised (#3296 round-11)'
  else
    ok 'AC3 control: the GIT limitation message does NOT satisfy the symlink acceptance test — a git answer can no longer stand in for a symlink measurement'
  fi
  # The ACCEPT direction of the same test, provoked from the REAL probe rather than a literal, by
  # pointing it at a directory that does not exist: `ln -s` cannot succeed there on ANY host, root
  # included, so the symlink skip route is proved reachable-and-named even where the read-only
  # directory control below has to skip.
  _cf_sym_absent=$(cf_symlink_limitation "$tmp/definitely-absent-dir-for-symlink-probe")
  if cf_cause_is_symlink "$_cf_sym_absent"; then
    ok 'AC3 control: the SYMLINK probe, asked directly, returns a cause that identifies SYMLINK CREATION when `ln -s` cannot succeed — the acceptance test is discriminating, not blanket-rejecting, and the skip route is reachable on any host'
  else
    bad "AC3 control: the symlink probe answered '$_cf_sym_absent' where `ln -s` cannot succeed — it must name symlink creation, or the symlink control can never pass and is dead code"
  fi
  mkdir -p "$tmp/cf-ro" 2>/dev/null
  chmod 555 "$tmp/cf-ro" 2>/dev/null
  if ln -s x "$tmp/cf-ro/writable-check" 2>/dev/null; then
    rm -f "$tmp/cf-ro/writable-check"
    skip 'AC3 control: the symlink-capability branch could NOT be provoked on a read-only directory — it still accepted a symlink (running as root, or a filesystem that ignores mode bits), so the realistic filesystem case was not measured on this host'
  else
    _cf_sym_cause=$(cf_symlink_limitation "$tmp/cf-ro")
    if cf_cause_is_symlink "$_cf_sym_cause"; then
      ok 'AC3 control: the SYMLINK capability probe — asked DIRECTLY, never through the composite — DETECTS a filesystem that cannot create the link, and its cause identifies SYMLINK CREATION, so an unrelated git answer can no longer satisfy this control'
    elif [ -z "$_cf_sym_cause" ]; then
      bad 'AC3 control: the symlink-capability probe reported NO limitation on a directory where `ln -s` demonstrably fails — the skip route cannot be reached, so a genuine environment limitation would be reported as a fixture FAILURE'
    else
      bad "AC3 control: the symlink probe answered '$_cf_sym_cause', which does not identify SYMLINK CREATION — a control satisfied by some OTHER cause reports a measurement it never performed (#3296 round-11)"
    fi
  fi
  chmod 755 "$tmp/cf-ro" 2>/dev/null

  _cf_env_limit=$(cf_env_limitation "$tmp")
  _cf_setup_err=''
  work="$tmp/link/work"
  [ -n "$_cf_env_limit" ] || mkdir -p "$tmp/real/work" || _cf_setup_err='mkdir of the fixture dir failed'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || ln -s "$tmp/real" "$tmp/link" || _cf_setup_err='the symlink that reproduces /var -> /private/var could not be created (the capability probe above says this filesystem CAN make symlinks, so this is a fixture failure, not an environment limitation)'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || git init -q -b main "$work" >/dev/null 2>&1 || _cf_setup_err='git init failed'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || git -C "$work" config user.email t@e || _cf_setup_err='git config user.email failed'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || git -C "$work" config user.name t || _cf_setup_err='git config user.name failed'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || printf 'x\n' >"$work/f.txt" || _cf_setup_err='writing the fixture file failed'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || git -C "$work" add f.txt >/dev/null 2>&1 || _cf_setup_err='git add failed'
  [ -n "$_cf_env_limit$_cf_setup_err" ] || git -C "$work" commit -q -m base >/dev/null 2>&1 || _cf_setup_err='git commit failed'
  _canon=''
  if [ -z "$_cf_env_limit$_cf_setup_err" ]; then
    _top=$(git -C "$work" rev-parse --show-toplevel 2>/dev/null) ||
      _cf_setup_err='git rev-parse --show-toplevel failed'
    [ -n "$_top" ] || _cf_setup_err='git rev-parse --show-toplevel returned nothing'
    if [ -z "$_cf_setup_err" ]; then
      _canon=$(cd "$_top" 2>/dev/null && pwd -P) || _cf_setup_err='canonicalising the fixture path failed'
    fi
  fi
  # The canonical path must be non-empty, absolute, AND the same directory as $work — an
  # arbitrary non-empty string would satisfy the "differs from $work" control by accident.
  if [ -z "$_cf_env_limit$_cf_setup_err" ]; then
    case "$_canon" in
      /*) ;;
      *) _cf_setup_err="the canonical path is not absolute: '$_canon'" ;;
    esac
  fi
  if [ -z "$_cf_env_limit$_cf_setup_err" ] && ! [ "$_canon" -ef "$work" ]; then
    _cf_setup_err="the canonical path '$_canon' is not the same directory as the fixture '$work'"
  fi

  if [ -n "$_cf_env_limit" ]; then
    skip "AC3: the case (f) fixture could not be established because of an affirmatively identified environment limitation ($_cf_env_limit) — the canonicalisation contract was NOT MEASURED on this host. Not a pass: the probes below are skipped rather than run against an unestablished fixture."
  elif [ -n "$_cf_setup_err" ]; then
    bad "AC3: the case (f) fixture setup FAILED ($_cf_setup_err). This is a defect in THIS FILE's fixture, not a limitation of the host — the capability probe above found none — so it is a counted FAILURE, never a skip: as a skip it silently disabled all four canonical-path contract probes while the script still exited 0 (#3296 round-9)."
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

  if [ -n "$_cf_env_limit" ]; then
    skip 'AC3: the four case (f) contract probes (sanctioned / relative / root-checkout / no --repo) were NOT RUN, because of the environment limitation named above'
  elif [ -n "$_cf_setup_err" ]; then
    bad 'AC3: the four case (f) contract probes (sanctioned / relative / root-checkout / no --repo) were NOT RUN, because this file’s own fixture setup FAILED — losing all four contract probes is a counted FAILURE, not a skip'
  else
    _tail='--agent codex --model gpt-5.6-sol --wait'
    # A SYNTHETIC 40-hex base (#3392). The wrapper now enqueues the RESOLVED merge-base sha, and the
    # extracted block therefore matches the base by SHAPE (`--base <40-hex> --repo`). This fixture has
    # no `origin/main` to compute a real merge-base from — its subject is the `--repo`
    # canonicalisation — so a shape-conformant literal is what keeps these four probes measuring THAT
    # contract. A symbolic base here would be rejected by the shape match and all four probes would
    # report the wrong thing about `--repo`.
    _cf_base=0123456789abcdef0123456789abcdef01234567
    probe_case_f sanctioned 'the sanctioned canonical --repo record' accept \
      "review --branch --base $_cf_base --repo $_canon $_tail"
    probe_case_f relative 'a RELATIVE --repo' reject \
      "review --branch --base $_cf_base --repo . $_tail"
    probe_case_f rootco 'a ROOT-CHECKOUT --repo' reject \
      "review --branch --base $_cf_base --repo $REPO_ROOT $_tail"
    probe_case_f norepo 'a --branch review with NO --repo at all' reject \
      "review --branch --base $_cf_base $_tail"
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
