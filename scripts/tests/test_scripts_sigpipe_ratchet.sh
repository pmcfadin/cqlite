#!/usr/bin/env bash
# test_scripts_sigpipe_ratchet.sh — the SELF-TEST WITH TEETH for the #4061 class ratchet,
# scripts/ci/check-sigpipe-sites.sh.
#
# WHY THIS FILE EXISTS. The ratchet's whole value is that a NEW piped-builtin-writer site REDS
# rather than shipping. Nothing about a green ratchet run demonstrates that: a guard that
# enumerates nothing, or whose matcher matches nothing, or that skips an unparsed baseline, reads
# exactly the same as one that works. So every FAILING and every REFUSING path is driven here on
# PLANTED input, and the two central cases assert the guard EXITS NON-ZERO **and NAMES THE FILE**.
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
CASE_FLOOR=24

pass=0; fail=0; cases=0
ok()  { cases=$((cases+1)); pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad() { cases=$((cases+1)); fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '     %s\n' "$2"; return 0; }

tmp=$(mktemp -d) || { printf 'FAIL could not mktemp — nothing was tested\n'; exit 1; }
trap 'rm -rf "$tmp"' EXIT

PIPE='|'
# The exact #4061 shape, assembled so it is not a literal in this file.
HAZARD="line=\$(printf '%s\\n' \"\$text\" $PIPE grep -m1 \"^\$k: \") $PIPE$PIPE return 0"

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
git -C "$PRISTINE" init -q >/dev/null 2>&1 && git -C "$PRISTINE" add -A >/dev/null 2>&1 || {
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
if [ "$regen_rc" = 0 ] && [ "$recheck_rc" = 0 ] && grep -q "^$VICTIM 3$" "$d/$BASE_REL" \
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

d=$(mkcase dupe);     printf '%s\n' "$VICTIM 9" >>"$d/$BASE_REL"
expect 11 "a DUPLICATE baseline entry is REFUSED" "$d" 3 "reason: baseline-duplicate" "$VICTIM"

d=$(mkcase zerocount); printf '%s\n' "$GUARD_REL 0" >>"$d/$BASE_REL"
expect 12 "a ZERO count is not a record and is REFUSED" "$d" 3 "reason: baseline-grammar"

d=$(mkcase truncated)
{ grep '^#' "$PRISTINE/$BASE_REL"; grep '^scripts/' "$PRISTINE/$BASE_REL" | head -3; } >"$d/$BASE_REL"
expect 13 "a TRUNCATED baseline trips the entry floor" "$d" 3 "reason: baseline-floor" "REMEDY"

d=$(mkcase badpath);  printf '/etc/passwd 3\n' >>"$d/$BASE_REL"
expect 13b "a baseline path outside scripts/**/*.sh is REFUSED" "$d" 3 "reason: baseline-grammar"

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

d=$(mkcase inert);    printf '#!/usr/bin/env bash\nsigpipe_violations() { :; }\n' >"$d/$MATCHER_REL"; reindex "$d"
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
definers=$(git -C "$REPO_ROOT" grep -l -e 'sigpipe_violations() {' -- 'scripts/**' | grep -c . || true)
if [ "${definers:-0}" -eq 1 ] && grep -qF "$MATCHER_REL" "$REPO_ROOT/$GUARD_REL"; then
  ok "20 exactly ONE tracked file defines the matcher, and the ratchet sources it"
else
  bad "20 ONE matcher" "$definers tracked file(s) define sigpipe_violations (want 1), or the ratchet does not source $MATCHER_REL"
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

printf '\npassed=%d failed=%d cases=%d (floor %d)\n' "$pass" "$fail" "$cases" "$CASE_FLOOR"
if [ "$cases" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL case-floor: ran %d cases, floor is %d — a green tally over a shrunken suite is not a pass\n' "$cases" "$CASE_FLOOR"
  exit 1
fi
[ "$fail" -eq 0 ] || exit 1
exit 0
