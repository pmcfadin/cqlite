#!/usr/bin/env bash
# test_scripts_sigpipe_ratchet.sh — the SELF-TEST WITH TEETH for the #4061 class ratchet,
# scripts/ci/check-sigpipe-sites.sh.
#
# WHY THIS FILE EXISTS. The ratchet's whole value is that a NEW piped-builtin-writer site REDS
# rather than shipping. Nothing about a green ratchet run demonstrates that: a guard that
# enumerates nothing, or whose matcher matches nothing, or that skips an unparsed baseline, reads
# exactly the same as one that works. So every FAILING and every REFUSING path is driven here on
# PLANTED input, and the central cases assert the guard EXITS NON-ZERO **and NAMES THE FILE**.
#
# THE SWAP CASE (24) IS THE ONE TO READ FIRST. The baseline recorded per-file COUNTS only, so
# removing one matching line and adding a DIFFERENT hazardous one left the count unchanged and
# PASSed (roborev job 138). Case 24a proves the fixture's count really is unchanged, so 24b
# cannot be passing for the count reason; case 25 is its control, pinning that pure MOTION still
# passes — the property that keeps the baseline free of line numbers.
#
# NO TRACKED FILE IS EVER MUTATED. Each case gets its own scratch GIT repository under $tmp,
# built by copying the git-tracked scripts/**/*.sh set plus the committed baseline and running
# `git init && git add -A` — because the guard derives its subject set from `git ls-files`, a
# scratch repo is the only way to plant a subject at all. Mutating a tracked file in place
# mid-run would also trip the gate's tree-integrity check (CLAUDE.md).
#
# The planted hazardous lines are ASSEMBLED FROM $PIPE at run time, so this file contains no
# matching line of its own and never appears in the baseline it tests.
#
# Prerequisites: git + awk + standard text tools. No cargo, no python3, no datasets, no network.
# It never SKIPs.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
GUARD_REL="scripts/ci/check-sigpipe-sites.sh"
BASE_REL="scripts/ci/sigpipe-sites-baseline.txt"
MATCHER_REL="scripts/tests/lib/sigpipe-matcher.sh"

# Case floor (CLAUDE.md #3544): a span-replacing edit that silently deletes cases yields a green
# tally over a shrunken suite. ENFORCED. May only go DOWN with a stated reason.
CASE_FLOOR=39

pass=0; fail=0; cases=0
ok()  { cases=$((cases+1)); pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad() { cases=$((cases+1)); fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '     %s\n' "$2"; return 0; }

tmp=$(mktemp -d) || { printf 'FAIL could not mktemp — nothing was tested\n'; exit 1; }
trap 'rm -rf "$tmp"' EXIT

PIPE='|'
# The exact #4061 shape, assembled so it is not a literal in this file.
HAZARD="line=\$(printf '%s\\n' \"\$text\" $PIPE grep -m1 \"^\$k: \") $PIPE$PIPE return 0"
# A SECOND, TEXTUALLY DIFFERENT hazard of the same shape. The swap case (24) needs one match to
# replace another so the COUNT is unchanged and only the content DIGEST moves.
HAZARD2="v=\$(printf '%s\\n' \"\$other\" $PIPE head -1)"
# A syntactically valid 64-hex digest for the baseline-grammar cases, so each one exercises the
# check it names rather than tripping the digest-shape check first.
OKHASH=0000000000000000000000000000000000000000000000000000000000000000

# THE ONE MATCHER, SOURCED — never a second copy (case 20 pins that there is exactly one
# definer). The swap and motion cases must MEASURE the planted file's match count to prove the
# property they claim, and this is the only sanctioned way to do it.
# shellcheck source=scripts/tests/lib/sigpipe-matcher.sh
if ! . "$REPO_ROOT/$MATCHER_REL" 2>/dev/null || ! declare -F sigpipe_violations >/dev/null 2>&1; then
  printf 'FAIL could not source %s — nothing below could measure anything\n' "$MATCHER_REL"
  exit 1
fi
n_matches() { sigpipe_violations "$1" >"$tmp/nm.txt" 2>/dev/null; grep -c . "$tmp/nm.txt" || true; }
first_match_lineno() { sigpipe_violations "$1" >"$tmp/fm.txt" 2>/dev/null; awk -F: 'NR==1{print $1}' "$tmp/fm.txt"; }
match_linenos() { sigpipe_violations "$1" >"$tmp/ml.txt" 2>/dev/null; awk -F: '{ printf "%s ", $1 }' "$tmp/ml.txt"; }

# ---------------------------------------------------------------------------
# 0. The scratch root must be OUTSIDE any git repository, or the "not a git repo" case would
#    silently find an ancestor repo and test nothing. Fail-closed, never skipped.
# ---------------------------------------------------------------------------
if git -C "$tmp" rev-parse --git-dir >/dev/null 2>&1; then
  bad "0 scratch root is outside any git repo" "REFUSING: $tmp is inside a git repository — set TMPDIR elsewhere; case 14 would test nothing"
  printf '\npassed=%d failed=%d cases=%d\n' "$pass" "$fail" "$cases"
  exit 1
else
  ok "0 scratch root ($tmp) is outside any git repository"
fi

# ---------------------------------------------------------------------------
# Build the PRISTINE scratch repo once: the real subject set, the real baseline, the real guard
# and the real matcher — never a model of them.
# ---------------------------------------------------------------------------
PRISTINE="$tmp/pristine"
mkdir -p "$PRISTINE"
git -C "$REPO_ROOT" ls-files -z 'scripts/*.sh' 'scripts/**/*.sh' >"$tmp/list.z" 2>/dev/null || true
n_copied=0
while IFS= read -r -d '' f; do
  mkdir -p "$PRISTINE/$(dirname -- "$f")"
  cp -- "$REPO_ROOT/$f" "$PRISTINE/$f" || break
  n_copied=$((n_copied + 1))
done <"$tmp/list.z"
mkdir -p "$PRISTINE/scripts/ci"
cp -- "$REPO_ROOT/$BASE_REL" "$PRISTINE/$BASE_REL" 2>/dev/null || true
if [ "$n_copied" -ge 100 ] && [ -r "$PRISTINE/$GUARD_REL" ] && [ -r "$PRISTINE/$BASE_REL" ] && [ -r "$PRISTINE/$MATCHER_REL" ]; then
  ok "1 scratch fixture: $n_copied tracked subject(s) + baseline + guard + matcher copied"
else
  bad "1 scratch fixture" "copied only $n_copied subject(s), or the guard/baseline/matcher is missing — the cases below would test nothing"
  printf '\npassed=%d failed=%d cases=%d\n' "$pass" "$fail" "$cases"
  exit 1
fi
# A COMMIT, not just an index: the guard's swap diagnostic recovers the REMOVED line from the
# file at HEAD and declares the difference EXACT only when HEAD reproduces the baseline digest.
# Without a HEAD that branch could never be exercised, and case 24 would only ever see the
# indicative fallback.
git -C "$PRISTINE" init -q >/dev/null 2>&1 \
  && git -C "$PRISTINE" add -A >/dev/null 2>&1 \
  && git -C "$PRISTINE" -c user.email=t@example.invalid -c user.name=t \
       commit -q -m 'scratch fixture' >/dev/null 2>&1 || {
  bad "1b scratch repo" "git init/add failed in $PRISTINE"
  printf '\npassed=%d failed=%d cases=%d\n' "$pass" "$fail" "$cases"
  exit 1
}
ok "1b scratch repo: git index built ($(git -C "$PRISTINE" ls-files 'scripts/*.sh' 'scripts/**/*.sh' | grep -c .) subjects visible to git ls-files)"

mkcase() { # mkcase <name> -> echoes the case dir
  local d="$tmp/$1"
  cp -a "$PRISTINE" "$d"
  printf '%s' "$d"
}
reindex() { git -C "$1" add -A >/dev/null 2>&1; }
run_guard() { # run_guard <dir> [args...] -> writes <dir>/out.txt, echoes rc
  local d="$1"; shift
  local rc=0
  ( cd "$d" && bash "$d/$GUARD_REL" "$@" ) >"$d/out.txt" 2>&1 || rc=$?
  printf '%s' "$rc"
}
# expect <id> <label> <dir> <want-rc> <needle>...  — rc AND every needle must be present.
expect() {
  local id="$1" label="$2" d="$3" want="$4"; shift 4
  local rc missing=""
  rc=$(run_guard "$d")
  local nd
  for nd in "$@"; do
    grep -qF -- "$nd" "$d/out.txt" || missing="$missing [missing: $nd]"
  done
  if [ "$rc" = "$want" ] && [ -z "$missing" ]; then
    ok "$id $label (rc=$rc)"
  else
    bad "$id $label" "expected rc=$want with all needles; got rc=$rc$missing"
  fi
}

# ---------------------------------------------------------------------------
# 2. CLEAN CONTROL. The unmodified tree must PASS, with the AFFIRMATIVE ZERO token — an
#    unmeasured check and a clean one must not read alike (CLAUDE.md).
# ---------------------------------------------------------------------------
d=$(mkcase clean)
expect 2 "clean scratch tree: no increase, affirmative zero" "$d" 0 \
  "0 INCREASE RECOGNISED" "verdict NO-INCREASE" "matcher SELF-CHECK OK" \
  "subjects ENUMERATED" "baseline PARSED" "==== DECLARED SCOPE"

# ---------------------------------------------------------------------------
# 3. THE NEGATIVE CONTROL WITH TEETH (lead's named artifact, #4061 AC4). Plant ONE piped
#    builtin writer into a file the baseline already lists: the guard must FAIL, NAME THAT FILE,
#    quote the offending line, and print the --regenerate remedy. Deterministic; no race.
# ---------------------------------------------------------------------------
VICTIM="scripts/bump-version.sh"
d=$(mkcase increase)
printf '%s\n' "$HAZARD" >>"$d/$VICTIM"
reindex "$d"
expect 3 "PLANTED site in a baseline file REDS and NAMES the file" "$d" 1 \
  "INCREASE: $VICTIM" "grep -m1" "verdict INCREASE" "--regenerate"

# ---------------------------------------------------------------------------
# 4. Same teeth for a file the baseline does NOT list — the rename/new-file path, a named and
#    documented failure mode rather than a mystery red.
# ---------------------------------------------------------------------------
NEWF="scripts/zz-planted-subject.sh"
d=$(mkcase newfile)
{ printf '#!/usr/bin/env bash\n'; printf '%s\n' "$HAZARD"; } >"$d/$NEWF"
reindex "$d"
expect 4 "PLANTED site in a NEW subject REDS and NAMES the file" "$d" 1 \
  "NEW FILE WITH SITES: $NEWF" "verdict INCREASE" "--regenerate"

# ---------------------------------------------------------------------------
# 5. And the converse, or the guard is unusable: a new script with NO sites must PASS. A ratchet
#    that reds on every added file is one agents learn to bypass.
# ---------------------------------------------------------------------------
d=$(mkcase newclean)
{ printf '#!/usr/bin/env bash\n'; printf 'line=$(grep -m1 "^k: " <<<"$text")\n'; } >"$d/scripts/zz-planted-clean.sh"
reindex "$d"
expect 5 "a NEW subject with no sites (herestring form) PASSes" "$d" 0 \
  "0 INCREASE RECOGNISED" "verdict NO-INCREASE"

# ---------------------------------------------------------------------------
# 6-7. A DECREASE and a DELETION are never failures, and both are REPORTED by name.
# ---------------------------------------------------------------------------
d=$(mkcase improved)
{ printf '#!/usr/bin/env bash\n'; printf 'head -1 <<<"$x"\n'; } >"$d/$VICTIM"
reindex "$d"
expect 6 "an IMPROVED file passes and is named" "$d" 0 "IMPROVED: $VICTIM" "verdict NO-INCREASE"

d=$(mkcase gone)
rm -f "$d/$VICTIM"
reindex "$d"
expect 7 "a baseline file that is GONE passes and is named" "$d" 0 \
  "BASELINE FILE GONE: $VICTIM" "verdict NO-INCREASE"

# ---------------------------------------------------------------------------
# 8. THE DOCUMENTED REMEDY MUST WORK, and be the ONLY way an existing site is tolerated:
#    plant, --regenerate, then re-check clean.
# ---------------------------------------------------------------------------
d=$(mkcase regen)
printf '%s\n' "$HAZARD" >>"$d/$VICTIM"
reindex "$d"
regen_rc=0
( cd "$d" && bash "$d/$GUARD_REL" --regenerate ) >"$d/regen.txt" 2>&1 || regen_rc=$?
recheck_rc=$(run_guard "$d")
if [ "$regen_rc" = 0 ] && [ "$recheck_rc" = 0 ] && grep -qE "^$VICTIM 3 [0-9a-f]{64}$" "$d/$BASE_REL" \
   && grep -qF 'verdict REGENERATED' "$d/regen.txt"; then
  ok "8 --regenerate records the planted site (bump-version.sh 2 -> 3) and the re-check is clean"
else
  bad "8 --regenerate round trip" "regen rc=$regen_rc recheck rc=$recheck_rc; baseline entry: $(grep -E "^$VICTIM " "$d/$BASE_REL" || printf '(absent)')"
fi

# ---------------------------------------------------------------------------
# 9-13. EVERY BASELINE REFUSAL PATH. Each is exit 3 with a NAMED cause and a REMEDY — fail-closed,
#    because a ratchet that skips a baseline it cannot read has stopped ratcheting. A CLOSED
#    grammar means anything unrecognised is REFUSED, not skipped.
# ---------------------------------------------------------------------------
d=$(mkcase nobase);   rm -f "$d/$BASE_REL"; reindex "$d"
expect 9 "missing baseline REFUSES (fail-closed)" "$d" 3 "reason: no-baseline" "REMEDY" "verdict REFUSED"

d=$(mkcase ungram);   printf 'this line is not a baseline record\n' >>"$d/$BASE_REL"
expect 10 "an UNRECOGNISED baseline line is REFUSED, not skipped" "$d" 3 "reason: baseline-grammar" "REMEDY"

d=$(mkcase dupe);     printf '%s\n' "$VICTIM 9 $OKHASH" >>"$d/$BASE_REL"
expect 11 "a DUPLICATE baseline entry is REFUSED" "$d" 3 "reason: baseline-duplicate" "$VICTIM"

d=$(mkcase zerocount); printf '%s\n' "$GUARD_REL 0 $OKHASH" >>"$d/$BASE_REL"
expect 12 "a ZERO count is not a record and is REFUSED" "$d" 3 "reason: baseline-grammar"

d=$(mkcase truncated)
{ grep '^#' "$PRISTINE/$BASE_REL"; grep '^scripts/' "$PRISTINE/$BASE_REL" | head -3; } >"$d/$BASE_REL"
expect 13 "a TRUNCATED baseline trips the entry floor" "$d" 3 "reason: baseline-floor" "REMEDY"

d=$(mkcase badpath);  printf '/etc/passwd 3 %s\n' "$OKHASH" >>"$d/$BASE_REL"
expect 13b "a baseline path outside scripts/**/*.sh is REFUSED" "$d" 3 "reason: baseline-grammar"

# The pre-#4061 COUNT-ONLY record. It must be REFUSED, not read as "a count with an unknown
# digest": a two-field entry tolerated silently is exactly the swap-blind state this change
# removed (roborev job 138), reintroduced through the parser.
d=$(mkcase twofield); printf '%s\n' "scripts/zz-two-field.sh 3" >>"$d/$BASE_REL"
expect 13c "a TWO-FIELD (pre-#4061 count-only) baseline record is REFUSED" "$d" 3 \
  "reason: baseline-grammar" "REMEDY"

d=$(mkcase badhash);  printf '%s\n' "scripts/zz-bad-hash.sh 3 not-a-sha256" >>"$d/$BASE_REL"
expect 13d "a digest that is not 64 hex characters is REFUSED" "$d" 3 "reason: baseline-grammar"

# ---------------------------------------------------------------------------
# 14-17. EVERY MEASUREMENT REFUSAL PATH: no repo, no matcher, an INERT matcher, and a subject set
#    too small to mean anything. The inert-matcher case is the one that matters most — it is the
#    state in which a broken guard reports zero sites and reads green.
# ---------------------------------------------------------------------------
d=$(mkcase nogit);    rm -rf "$d/.git"
expect 14 "no git repository REFUSES (the subject set cannot be derived)" "$d" 3 \
  "reason: not-a-git-repo" "REMEDY"

d=$(mkcase nomatcher); rm -f "$d/$MATCHER_REL"; reindex "$d"
expect 15 "a MISSING matcher library REFUSES (nothing was scanned)" "$d" 3 "reason: no-matcher" "REMEDY"

# The stub's DEFINITION is assembled at run time from $MATCHER_FN, so this file never contains
# the literal `<fn>() {` that case 20 counts — a lint that matches its own fixture literals is a
# defect this repo has paid for more than once (see the needle-assembly note in
# scripts/tests/test_agent_gate_summary.sh's portability lint).
MATCHER_FN='sigpipe_violations'
d=$(mkcase inert)
printf '#!/usr/bin/env bash\n%s() { :; }\n' "$MATCHER_FN" >"$d/$MATCHER_REL"; reindex "$d"
expect 16 "an INERT matcher REFUSES instead of reporting zero sites" "$d" 3 "reason: matcher-inert" "REMEDY"

d=$(mkcase fewsubj)
git -C "$d" rm -rq --cached scripts >/dev/null 2>&1
git -C "$d" add "$GUARD_REL" "$MATCHER_REL" >/dev/null 2>&1
expect 17 "a subject set below the non-vacuity floor REFUSES" "$d" 3 "reason: subject-floor" "REMEDY"

# ---------------------------------------------------------------------------
# 18-19. USAGE. An unrecognised argument is exit 2 (never a lenient default), --help is exit 0.
# ---------------------------------------------------------------------------
d=$(mkcase usage)
u_rc=0; ( cd "$d" && bash "$d/$GUARD_REL" --tolerate-everything ) >"$d/u.txt" 2>&1 || u_rc=$?
if [ "$u_rc" = 2 ] && grep -qF 'unrecognised argument' "$d/u.txt"; then
  ok "18 an unrecognised argument exits 2 and names it (no lenient default)"
else
  bad "18 unrecognised argument" "expected rc=2 and a named refusal, got rc=$u_rc"
fi
h_rc=0; ( cd "$d" && bash "$d/$GUARD_REL" --help ) >"$d/h.txt" 2>&1 || h_rc=$?
if [ "$h_rc" = 0 ] && grep -qF -- '--regenerate' "$d/h.txt"; then
  ok "19 --help exits 0 and documents --regenerate"
else
  bad "19 --help" "expected rc=0 documenting --regenerate, got rc=$h_rc"
fi

# ---------------------------------------------------------------------------
# 20. ONE MATCHER, NOT TWO (a hard constraint of #4061). Exactly one tracked file may DEFINE the
#     matcher, and the guard must SOURCE it. A copied regex is the silent-divergence defect
#     CLAUDE.md names, and no test can catch it after the fact.
# ---------------------------------------------------------------------------
git -C "$REPO_ROOT" grep -l -e "$MATCHER_FN() {" -- 'scripts/**' >"$tmp/definers.txt" 2>/dev/null || true
definers=$(grep -c . "$tmp/definers.txt" || true)
if [ "${definers:-0}" -eq 1 ] && grep -qF "$MATCHER_REL" "$REPO_ROOT/$GUARD_REL"; then
  ok "20 exactly ONE tracked file defines the matcher, and the ratchet sources it"
else
  bad "20 ONE matcher" "$definers tracked file(s) define the matcher (want 1: $(tr '\n' ' ' <"$tmp/definers.txt")), or the ratchet does not source $MATCHER_REL"
fi

# ---------------------------------------------------------------------------
# 21. THE SHIPPED TREE. The ratchet must actually hold here and now — this is the assertion the
#     gate consumes, run against the real repository rather than a scratch copy.
# ---------------------------------------------------------------------------
ship_rc=0
( cd "$REPO_ROOT" && bash "$REPO_ROOT/$GUARD_REL" ) >"$tmp/ship.txt" 2>&1 || ship_rc=$?
if [ "$ship_rc" = 0 ] && grep -qF '0 INCREASE RECOGNISED' "$tmp/ship.txt"; then
  ok "21 the SHIPPED tree holds the ratchet ($(grep -oE 'MEASURED [0-9]+ file\(s\)[^,]*, [0-9]+ match\(es\)' "$tmp/ship.txt" | head -1))"
else
  bad "21 the shipped tree" "the ratchet does not hold here (rc=$ship_rc): $(grep -E '^SIGPIPE-SITES: (INCREASE|NEW FILE|REFUSING)' "$tmp/ship.txt" | head -5)"
fi

# ---------------------------------------------------------------------------
# 22-23. THE TWO SITES #4061 CONVERTED, pinned BY NAME. The ratchet catches a revert as an
#     increase; these say WHICH form is correct, so the next reader does not have to re-derive it.
#     Both were `printf '%s\n' ... | head -N`, i.e. WITH a trailing newline, for which a
#     herestring is byte-equivalent. It is NOT equivalent for `printf '%s'`.
# ---------------------------------------------------------------------------
if grep -qF 'head -1 <<<"$GL_OUT"' "$REPO_ROOT/scripts/gate-component-verdict.sh"; then
  ok "22 gate-component-verdict.sh reads the liveness answer via a herestring"
else
  bad "22 gate-component-verdict.sh" "the #4061 site is not the expected \`head -1 <<<\"\$GL_OUT\"\` form"
fi
if grep -qF 'head -4 <<<"$obj_out"' "$REPO_ROOT/scripts/bootstrap-agent-machine.sh"; then
  ok "23 bootstrap-agent-machine.sh reads the object-store lines via a herestring"
else
  bad "23 bootstrap-agent-machine.sh" "the #4061 site is not the expected \`head -4 <<<\"\$obj_out\"\` form"
fi


# ---------------------------------------------------------------------------
# 24. THE SWAP — the regression the aggregate content hash exists for (roborev job 138, triaged
#     BLOCKER). Remove one matching line from a baseline file and add a DIFFERENT hazardous one:
#     the per-file COUNT is unchanged, so the count-only ratchet PASSed and a new defect shipped
#     green. 24a asserts BY CONSTRUCTION that the count really is unchanged — without it 24b
#     could be passing for the count reason and never testing the swap at all.
# ---------------------------------------------------------------------------
d=$(mkcase swap)
sw_before=$(n_matches "$d/$VICTIM")
sw_ln=$(first_match_lineno "$d/$VICTIM")
awk -v skip="$sw_ln" 'NR != skip' "$d/$VICTIM" >"$tmp/swap.body" 2>/dev/null
{ cat "$tmp/swap.body"; printf '%s\n' "$HAZARD2"; } >"$d/$VICTIM"
sw_after=$(n_matches "$d/$VICTIM")
reindex "$d"
sw_rc=$(run_guard "$d")
if [ -n "${sw_before:-}" ] && [ "${sw_before:-0}" -ge 1 ] && [ "$sw_before" = "$sw_after" ]; then
  ok "24a the swap fixture leaves $VICTIM's COUNT UNCHANGED ($sw_before -> $sw_after): only the matched-line SET moved"
else
  bad "24a swap fixture is a real swap" "count went $sw_before -> $sw_after; 24b would then be testing an INCREASE, not a swap"
fi
sw_missing=""
for nd in "SWAP: $VICTIM" "verdict INCREASE" "ADDED:" "REMOVED:" "EXACT" "--regenerate"; do
  grep -qF -- "$nd" "$d/out.txt" || sw_missing="$sw_missing [missing: $nd]"
done
if [ "$sw_rc" = 1 ] && [ -z "$sw_missing" ]; then
  ok "24b a SWAP at an unchanged count REDS, NAMES the file and names the added/removed lines (rc=$sw_rc)"
else
  bad "24b a SWAP REDS and is actionable" "expected rc=1 with all needles; got rc=$sw_rc$sw_missing"
fi

# ---------------------------------------------------------------------------
# 25. MOTION-INVARIANCE CONTROL — the property the whole design rests on, and the reason the
#     digest is taken over SORTED, NORMALISED text with the line numbers DISCARDED. Reorder the
#     matched lines and re-indent one of them so EVERY match's line number moves: the multiset is
#     unchanged, so the ratchet must still PASS. This is what stops a future "fix" from putting
#     line numbers back into the hashed material (#4061 pinned :3329 and it drifted to :5392 in
#     two days).
# ---------------------------------------------------------------------------
d=$(mkcase motion)
mo_before=$(n_matches "$d/$VICTIM")
mo_lines_before=$(match_linenos "$d/$VICTIM")
awk '{ a[NR] = $0 }
     END { print "# a non-matching line, added above every site"
           for (i = NR; i >= 1; i--) print a[i] }' "$d/$VICTIM" >"$tmp/motion.body"
mo_reind=$(first_match_lineno "$tmp/motion.body")
awk -v t="${mo_reind:-0}" 'NR == t { print "        " $0; next } { print }' "$tmp/motion.body" >"$d/$VICTIM"
mo_after=$(n_matches "$d/$VICTIM")
mo_lines_after=$(match_linenos "$d/$VICTIM")
reindex "$d"
mo_rc=$(run_guard "$d")
if [ "$mo_before" = "$mo_after" ] && [ "$mo_lines_before" != "$mo_lines_after" ] \
   && [ "$mo_rc" = 0 ] && grep -qF 'verdict NO-INCREASE' "$d/out.txt" \
   && grep -qF '0 INCREASE RECOGNISED' "$d/out.txt" && ! grep -qF "SWAP: $VICTIM" "$d/out.txt"; then
  ok "25 REORDERED + RE-INDENTED sites (line numbers $mo_lines_before-> $mo_lines_after) still PASS: the digest is motion-proof"
else
  bad "25 motion-invariance" "count $mo_before -> $mo_after, linenos '$mo_lines_before' -> '$mo_lines_after', rc=$mo_rc (want 0 with NO-INCREASE and moved linenos)"
fi

# ---------------------------------------------------------------------------
# 26. A DECREASE NEVER FAILS — and this case pins the ONE DECLARED RESIDUAL as a deliberate
#     choice rather than an accident: the digest changes on ANY removal, so it cannot separate a
#     pure removal from a removal-plus-addition. Here BOTH baseline sites are removed and ONE new
#     hazard is added (2 -> 1). It must PASS and be reported as IMPROVED. A ratchet that reds on
#     a net improvement is one agents route around; closing this would need matched-line TEXT in
#     the baseline, i.e. the curated, motion-sensitive list this design refuses.
# ---------------------------------------------------------------------------
d=$(mkcase netdecrease)
{ printf '#!/usr/bin/env bash\n'; printf '%s\n' "$HAZARD2"; } >"$d/$VICTIM"
nd_before=$(n_matches "$d/$VICTIM")
reindex "$d"
if [ "${nd_before:-0}" -eq 1 ]; then
  expect 26 "a NET DECREASE that also ADDS a line still PASSes (the declared residual)" "$d" 0 \
    "IMPROVED: $VICTIM" "verdict NO-INCREASE"
else
  bad "26 net-decrease fixture" "planted file has $nd_before match(es), expected exactly 1"
fi

# ---------------------------------------------------------------------------
# 27. AN ABSENT DIGEST TOOL IS A NAMED REFUSAL, never a SKIP and never a silent fall back to
#     comparing counts alone — a degraded mode that quietly restores the false PASS would be
#     worse than the bug this change fixes. Driven with a PATH holding git and awk (so the
#     refusal cannot be one of those, and `dirname`, which the guard uses to locate itself) and
#     NEITHER sha256sum NOR shasum.
# ---------------------------------------------------------------------------
d=$(mkcase nodigest)
nodigest_bin="$tmp/nodigest-bin"
mkdir -p "$nodigest_bin"
nd_tools_ok=1
for t in dirname git awk; do
  tp=$(command -v "$t") || nd_tools_ok=0
  [ -n "${tp:-}" ] && ln -s "$tp" "$nodigest_bin/$t" 2>/dev/null || nd_tools_ok=0
done
BASH_BIN="${BASH:-$(command -v bash)}"
dg_rc=0
( cd "$d" && PATH="$nodigest_bin" "$BASH_BIN" "$d/$GUARD_REL" ) >"$d/dg.txt" 2>&1 || dg_rc=$?
if [ "$nd_tools_ok" = 1 ] && [ "$dg_rc" = 3 ] && grep -qF 'reason: no-sha256' "$d/dg.txt" \
   && grep -qF 'REMEDY' "$d/dg.txt" && grep -qF 'verdict REFUSED' "$d/dg.txt"; then
  ok "27 no sha256 tool on PATH REFUSES by name (rc=$dg_rc) — no count-only fallback"
else
  bad "27 absent digest tool" "expected rc=3 naming no-sha256 with a REMEDY; got rc=$dg_rc (git/awk symlinks ok=$nd_tools_ok)"
fi

# ---------------------------------------------------------------------------
# 28. A FAILING MATCHER IS A NAMED REFUSAL, NOT A CLEAN FILE (roborev job 139, triaged BLOCKER).
#     The census ran `sigpipe_violations "$f" >census 2>/dev/null` and IGNORED the status: a
#     failing awk left an EMPTY census, the count computed as 0, and that subject was reported
#     CLEAN. A zero count from an empty stream is indistinguishable from a genuine clean file, so
#     this case FAILED before the fix and is the RED control for the whole producer-status class.
#     RED-VERIFIED, and reproducible: restore the pre-fix guard and re-run this file —
#         git show 34730166c:scripts/ci/check-sigpipe-sites.sh >scripts/ci/check-sigpipe-sites.sh
#         bash scripts/tests/test_scripts_sigpipe_ratchet.sh   # 28b and 30b FAIL, 39 cases run
#     — then restore it. Pre-fix, 28b saw rc=0 with `verdict NO-INCREASE`: the unmeasured victim
#     read CLEAN. 29b passed pre-fix (see its own note), which is why it is labelled a pin.
#
#     HOW THE FAILURE IS FORCED, hermetically: an `awk` shim EARLY on PATH that exits non-zero
#     only when invoked with the victim SUBJECT as a file argument — i.e. exactly the matcher's
#     per-subject scan — and `exec`s the real awk for every other call (the self-check, the
#     counts, the normaliser, the comparison). So the refusal can only come from the scan, and
#     28a proves BY CONSTRUCTION that the shim really fired (a marker file), without which this
#     case could pass while forcing nothing.
# ---------------------------------------------------------------------------
d=$(mkcase matcherfail)
mf_bin="$tmp/matcherfail-bin"; mkdir -p "$mf_bin"
mf_marker="$tmp/matcherfail.fired"
REAL_AWK=$(command -v awk)
cat >"$mf_bin/awk" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do
  case "\$a" in
    $VICTIM|*/$VICTIM) : >"$mf_marker"; exit 1 ;;
  esac
done
exec "$REAL_AWK" "\$@"
EOF
chmod +x "$mf_bin/awk"
mf_rc=0
( cd "$d" && PATH="$mf_bin:$PATH" "${BASH:-$(command -v bash)}" "$d/$GUARD_REL" ) >"$d/mf.txt" 2>&1 || mf_rc=$?
if [ -f "$mf_marker" ] && [ -n "${REAL_AWK:-}" ]; then
  ok "28a the awk shim really FIRED on $VICTIM (marker present): the matcher failure was forced, not assumed"
else
  bad "28a forced matcher failure" "the shim never fired (marker absent) or awk is not on PATH — 28b would be testing nothing"
fi
mf_missing=""
for nd in "reason: matcher-failed" "$VICTIM" "UNKNOWN, not zero" "REMEDY" "verdict REFUSED"; do
  grep -qF -- "$nd" "$d/mf.txt" || mf_missing="$mf_missing [missing: $nd]"
done
if [ "$mf_rc" = 3 ] && [ -z "$mf_missing" ] \
   && ! grep -qF 'verdict NO-INCREASE' "$d/mf.txt" && ! grep -qF '0 INCREASE RECOGNISED' "$d/mf.txt"; then
  ok "28b a FAILING matcher REFUSES by name and NAMES the unmeasured file (rc=$mf_rc) — never CLEAN, never NO-INCREASE"
else
  bad "28b failing matcher" "expected rc=3 naming matcher-failed and $VICTIM with no NO-INCREASE token; got rc=$mf_rc$mf_missing"
fi

# ---------------------------------------------------------------------------
# 29. A FAILING DIGEST TOOL IS A NAMED REFUSAL, not an empty digest fed into a comparison. This
#     path was ALREADY fail-closed before the fix — `_digest_of`'s output was shape-checked
#     against 64 hex characters, and an empty string fails that — so this case is a REGRESSION
#     PIN rather than a RED control: it pins that the refusal survives now that the tool's STATUS
#     is read too, and stops a future "optimisation" that trusts the tool's output. Forced with a
#     `sha256sum` shim that exits non-zero, with a marker proving it ran.
# ---------------------------------------------------------------------------
d=$(mkcase digestfail)
df_bin="$tmp/digestfail-bin"; mkdir -p "$df_bin"
df_marker="$tmp/digestfail.fired"
cat >"$df_bin/sha256sum" <<EOF
#!/usr/bin/env bash
: >"$df_marker"
exit 1
EOF
chmod +x "$df_bin/sha256sum"
df_rc=0
( cd "$d" && PATH="$df_bin:$PATH" "${BASH:-$(command -v bash)}" "$d/$GUARD_REL" ) >"$d/df.txt" 2>&1 || df_rc=$?
if [ -f "$df_marker" ]; then
  ok "29a the sha256sum shim really FIRED (marker present): the digest failure was forced, not assumed"
else
  bad "29a forced digest failure" "the shim never fired (marker absent) — 29b would be testing nothing"
fi
df_missing=""
for nd in "reason: digest-failed" "UNKNOWN" "REMEDY" "verdict REFUSED"; do
  grep -qF -- "$nd" "$d/df.txt" || df_missing="$df_missing [missing: $nd]"
done
if [ "$df_rc" = 3 ] && [ -z "$df_missing" ] && ! grep -qF 'verdict NO-INCREASE' "$d/df.txt"; then
  ok "29b a FAILING digest tool REFUSES by name (rc=$df_rc) — no empty digest reaches the comparison"
else
  bad "29b failing digest tool" "expected rc=3 naming digest-failed with no NO-INCREASE token; got rc=$df_rc$df_missing"
fi

# ---------------------------------------------------------------------------
# 30. THE SAME CLASS ONE STAGE LATER, AND THE SECOND RED CONTROL: the sort that orders the
#     comparison records. Before the fix its status was ignored, so a failed sort left the record
#     file EMPTY, the reader loop found no FAIL record, and a run WITH A PLANTED INCREASE printed
#     NO-INCREASE and exited 0 — a false PASS reachable without touching the matcher at all. That
#     is why the fix is the class and not line 243. Forced with a `sort` shim that fails ONLY for
#     the comparison-record file, so the census normaliser (which also sorts) is untouched.
#     RED-VERIFIED against the pre-fix guard: rc=0, `verdict NO-INCREASE`, with a PLANTED hazard
#     sitting in the tree — the false PASS in full, and not reachable through the matcher.
# ---------------------------------------------------------------------------
d=$(mkcase cmpsortfail)
printf '%s\n' "$HAZARD" >>"$d/$VICTIM"
reindex "$d"
cs_bin="$tmp/cmpsortfail-bin"; mkdir -p "$cs_bin"
cs_marker="$tmp/cmpsortfail.fired"
REAL_SORT=$(command -v sort)
cat >"$cs_bin/sort" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do
  case "\$a" in
    *cmp.unsorted) : >"$cs_marker"; exit 1 ;;
  esac
done
exec "$REAL_SORT" "\$@"
EOF
chmod +x "$cs_bin/sort"
cs_rc=0
( cd "$d" && PATH="$cs_bin:$PATH" "${BASH:-$(command -v bash)}" "$d/$GUARD_REL" ) >"$d/cs.txt" 2>&1 || cs_rc=$?
if [ -f "$cs_marker" ] && [ -n "${REAL_SORT:-}" ]; then
  ok "30a the sort shim really FIRED on the comparison records (marker present)"
else
  bad "30a forced comparison-sort failure" "the shim never fired (marker absent) or sort is not on PATH — 30b would be testing nothing"
fi
cs_missing=""
for nd in "reason: comparison-sort-failed" "NO-INCREASE" "REMEDY" "verdict REFUSED"; do
  grep -qF -- "$nd" "$d/cs.txt" || cs_missing="$cs_missing [missing: $nd]"
done
if [ "$cs_rc" = 3 ] && [ -z "$cs_missing" ] && ! grep -qF 'verdict NO-INCREASE' "$d/cs.txt"; then
  ok "30b a FAILING comparison sort REFUSES by name (rc=$cs_rc) — a planted INCREASE can never read as NO-INCREASE"
else
  bad "30b failing comparison sort" "expected rc=3 naming comparison-sort-failed with no NO-INCREASE VERDICT; got rc=$cs_rc$cs_missing"
fi

printf '\npassed=%d failed=%d cases=%d (floor %d)\n' "$pass" "$fail" "$cases" "$CASE_FLOOR"
if [ "$cases" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL case-floor: ran %d cases, floor is %d — a green tally over a shrunken suite is not a pass\n' "$cases" "$CASE_FLOOR"
  exit 1
fi
[ "$fail" -eq 0 ] || exit 1
exit 0
