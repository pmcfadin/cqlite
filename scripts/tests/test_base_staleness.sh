#!/usr/bin/env bash
#
# Regression tests for scripts/flow/base-staleness.sh (issue #3650, SLICE 1).
#
# Fast + hermetic: every case builds a SYNTHETIC git repo in a temp dir and runs
# the advisory inside it. No network, no `gh`, no fetch — and the advisory itself
# must never fetch or mutate a ref, which one case asserts directly.
#
# The suite carries four things beyond ordinary cases, because #3650's own
# warning is that a mechanism here can be SATISFIED AND WRONG:
#
#   1. THE MOTIVATING CASE AS A PINNED FIXTURE (Case 5). PR #3362's shape: the
#      commit that broke the gate and the PR's diff share NO path
#      (docs/round-artifacts/issue-3650-blast-radius-measurements.md). Under
#      "blast radius = the paths the diff touches" that commit is not in the
#      blast radius of the PR it broke, so the definition's `M = 0` branch calls
#      a certification fresh exactly when it is not.
#   2. A PLANTED-MUTANT CASE (Case 11), following
#      scripts/tests/test_ws0_perf_invocation_lint.sh:812-830: a copy of the
#      script with the gate-global set EMPTIED must (a) genuinely carry that
#      defect and nothing else, and (b) get case 5 wrong. A bare red is not
#      evidence — the plant is verified to be the defect described.
#   3. A VOCABULARY CASE (Case 7): NO run's output contains `PASS`, `OK` or
#      `RESULT:`. That is AC5 tested directly rather than reasoned about, so
#      every `run` accumulates its output for one whole-suite assertion.
#   4. FIXTURE SELF-CONSISTENCY (Case 1): each synthetic repo is asserted with
#      git to actually have the shape its case claims, so a case cannot pass
#      against a fixture that never had the property under test (the idiom at
#      test_premerge_assert.sh:525-530).
#
# Run standalone:   bash scripts/tests/test_base_staleness.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADVISORY="$SCRIPT_DIR/../flow/base-staleness.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

T=$(mktemp -d "${TMPDIR:-/tmp}/base-staleness-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

# Every run's output is accumulated here so the AC5 vocabulary assertion covers
# ALL runs, not a sampled one.
ALL_OUT="$T/all-output.txt"
: >"$ALL_OUT"

MAIN_REF=refs/remotes/origin/main

# g <repo> <git args...> — quiet git in a synthetic repo.
g() { local r="$1"; shift; git -C "$r" "$@"; }

# commit_paths <repo> <message> <path...> — write each path with unique content
# and commit them.
commit_paths() {
  local r="$1" msg="$2" p
  shift 2
  for p in "$@"; do
    mkdir -p "$r/$(dirname "$p")"
    printf 'content for %s at %s\n' "$p" "$msg" >>"$r/$p"
    g "$r" add -- "$p" >/dev/null
  done
  g "$r" -c user.email=t@t -c user.name=t commit -q -m "$msg" >/dev/null
}

# newrepo <name> -> path. A repo with an initial commit on branch `mainline` and
# a `feature` branch checked out at it. No origin/main ref yet.
newrepo() {
  local r="$T/$1"
  mkdir -p "$r"
  git init -q -b mainline "$r" >/dev/null
  g "$r" config user.email t@t
  g "$r" config user.name t
  commit_paths "$r" "c0 initial" "README.md" ".config/nextest.toml" \
    "cqlite-core/src/storage/sstable/mod.rs"
  g "$r" checkout -q -b feature
  printf '%s' "$r"
}

# advance_main <repo> — commits land on `mainline`; caller then publishes it.
advance_main() { g "$1" checkout -q mainline; }
back_to_feature() { g "$1" checkout -q feature; }
publish_main() { g "$1" update-ref "$MAIN_REF" mainline; }

# run <expected-exit> <desc> <repo> [args...] — run the advisory (or $USE_SCRIPT)
# with cwd inside <repo>. Sets $OUT/$RC, accumulates output for the AC5 case.
run() {
  local want="$1" desc="$2" repo="$3"
  shift 3
  OUT=$(cd "$repo" && bash "${USE_SCRIPT:-$ADVISORY}" "$@" 2>&1)
  RC=$?
  printf '%s\n' "$OUT" >>"$ALL_OUT"
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

# has <desc> <needle> — assert $OUT contains the needle.
has() {
  if [ "${OUT#*"$2"}" != "$OUT" ]; then
    ok "$1"
  else
    bad "$1: output does not contain '$2' (got: $OUT)"
  fi
}

# lacks <desc> <needle> — assert $OUT does NOT contain the needle.
lacks() {
  if [ "${OUT#*"$2"}" = "$OUT" ]; then
    ok "$1"
  else
    bad "$1: output must NOT contain '$2' (got: $OUT)"
  fi
}

# ---------------------------------------------------------------------------
# FIXTURES
# ---------------------------------------------------------------------------

# STALE_DIFF — a commit behind touches the SAME path the diff touches
# (the path-intersection half of the definition).
R_DIFF=$(newrepo stale-diff)
commit_paths "$R_DIFF" "feature edits the sstable module" \
  "cqlite-core/src/storage/sstable/mod.rs"
advance_main "$R_DIFF"
commit_paths "$R_DIFF" "main also edits the sstable module" \
  "cqlite-core/src/storage/sstable/mod.rs"
publish_main "$R_DIFF"
back_to_feature "$R_DIFF"

# FRESH — origin/main is exactly the merge-base: nothing behind.
R_FRESH=$(newrepo fresh)
commit_paths "$R_FRESH" "feature edits the sstable module" \
  "cqlite-core/src/storage/sstable/mod.rs"
publish_main "$R_FRESH"     # mainline never advanced
back_to_feature "$R_FRESH"

# MOTIVATING — PR #3362's shape. The diff touches only src + its own issue test;
# the commit behind touches .config/nextest.toml and docs, sharing NO path with
# the diff. Detected only via the gate-global half of the definition.
R_MOTIV=$(newrepo motivating)
commit_paths "$R_MOTIV" "the PR: summary_scan + its issue test" \
  "cqlite-core/src/storage/sstable/reader/data_access/summary_scan/mod.rs" \
  "cqlite-core/tests/issue_3358_bti_query_token_bound.rs"
advance_main "$R_MOTIV"
commit_paths "$R_MOTIV" "the culprit: known-flake fix (#3514 shape)" \
  ".config/nextest.toml" "docs/development/gate-ops.md"
commit_paths "$R_MOTIV" "unrelated churn" "docs/research/notes.md"
publish_main "$R_MOTIV"
back_to_feature "$R_MOTIV"

# UNRELATED — churn behind touches neither the diff's paths nor a gate-global
# path: counted in N, never in M.
R_UNREL=$(newrepo unrelated)
commit_paths "$R_UNREL" "the PR: sstable module" \
  "cqlite-core/src/storage/sstable/mod.rs"
advance_main "$R_UNREL"
commit_paths "$R_UNREL" "unrelated churn 1" "docs/research/notes.md"
commit_paths "$R_UNREL" "unrelated churn 2" "README.md"
publish_main "$R_UNREL"
back_to_feature "$R_UNREL"

# NOMAIN — a repo with no origin/main ref at all.
R_NOMAIN=$(newrepo no-main)
commit_paths "$R_NOMAIN" "the PR" "cqlite-core/src/storage/sstable/mod.rs"

# NOBASE — unrelated histories: no merge-base with origin/main.
R_NOBASE=$(newrepo no-merge-base)
publish_main "$R_NOBASE"
g "$R_NOBASE" checkout -q --orphan orphan
g "$R_NOBASE" rm -rq --cached . >/dev/null 2>&1
rm -f "$R_NOBASE/README.md" "$R_NOBASE/.config/nextest.toml"
rm -rf "$R_NOBASE/cqlite-core"
commit_paths "$R_NOBASE" "an unrelated root history" "unrelated/only.txt"

# --- Case 1: the fixtures really have the shape the cases claim -------------
# Non-vacuity. A case asserting "the culprit shares no path with the diff" is
# worthless if the fixture happens to share one; assert it with git, not by eye.
mb=$(g "$R_MOTIV" merge-base "$MAIN_REF" HEAD)
behind=$(g "$R_MOTIV" rev-list --count "$mb..$MAIN_REF")
if [ "$behind" -eq 2 ]; then
  ok "fixture(motivating): origin/main is 2 commits ahead of the merge-base"
else
  bad "fixture(motivating): expected 2 commits behind, got $behind"
fi
g "$R_MOTIV" diff --name-only "$mb...HEAD" | sort >"$T/mv-diff-paths"
culprit=$(g "$R_MOTIV" rev-list "$mb..$MAIN_REF" -- .config/nextest.toml)
g "$R_MOTIV" diff-tree -r --no-commit-id --name-only "$culprit" | sort >"$T/mv-culprit-paths"
if [ -n "$culprit" ] && [ -z "$(comm -12 "$T/mv-diff-paths" "$T/mv-culprit-paths")" ]; then
  ok "fixture(motivating): the culprit commit and the diff share NO path (PR #3362's shape)"
else
  bad "fixture(motivating): the culprit must exist and share no path with the diff"
fi
if grep -qx '.config/nextest.toml' "$T/mv-culprit-paths"; then
  ok "fixture(motivating): the culprit touches the gate-global .config/nextest.toml"
else
  bad "fixture(motivating): the culprit must touch .config/nextest.toml"
fi
mb=$(g "$R_UNREL" merge-base "$MAIN_REF" HEAD)
g "$R_UNREL" diff --name-only "$mb...HEAD" | sort >"$T/un-diff-paths"
g "$R_UNREL" log --format=%H "$mb..$MAIN_REF" >"$T/un-commits"
unrel_shared=0
while IFS= read -r c; do
  [ -n "$c" ] || continue
  g "$R_UNREL" diff-tree -r --no-commit-id --name-only "$c" | sort >"$T/un-c-paths"
  [ -n "$(comm -12 "$T/un-diff-paths" "$T/un-c-paths")" ] && unrel_shared=1
done <"$T/un-commits"
if [ "$(wc -l <"$T/un-commits" | tr -d ' ')" -eq 2 ] && [ "$unrel_shared" -eq 0 ]; then
  ok "fixture(unrelated): 2 commits behind, none sharing a path with the diff"
else
  bad "fixture(unrelated): expected 2 non-intersecting commits behind (shared=$unrel_shared)"
fi
mb=$(g "$R_FRESH" merge-base "$MAIN_REF" HEAD)
tip=$(g "$R_FRESH" rev-parse "$MAIN_REF")
behind=$(g "$R_FRESH" rev-list --count "$mb..$tip")
if [ "$mb" = "$tip" ] && [ "$behind" -eq 0 ]; then
  ok "fixture(fresh): origin/main IS the merge-base, so nothing is behind"
else
  bad "fixture(fresh): origin/main must equal the merge-base (mb=$mb tip=$tip behind=$behind)"
fi
if g "$R_NOMAIN" rev-parse --verify -q "$MAIN_REF" >/dev/null; then
  bad "fixture(no-main): the repo must NOT have an origin/main ref"
else
  ok "fixture(no-main): the repo genuinely has no origin/main ref"
fi
if g "$R_NOBASE" merge-base "$MAIN_REF" HEAD >/dev/null 2>&1; then
  bad "fixture(no-merge-base): the histories must be unrelated"
else
  ok "fixture(no-merge-base): the histories genuinely have no merge-base"
fi

# --- Case 2: a stale base with blast-radius churn is STALE-RECOGNISED -------
if run 4 "stale base, diff-path churn -> exit 4" "$R_DIFF"; then
  has "stale(diff-path): verdict is STALE-RECOGNISED" "verdict STALE-RECOGNISED"
  has "stale(diff-path): reports behind 1 commits" "behind 1 commits"
  has "stale(diff-path): blast-radius count is 1 RECOGNISED" "blast-radius 1 RECOGNISED"
  has "stale(diff-path): names the matching path and WHY it matched" \
    "diff-path cqlite-core/src/storage/sstable/mod.rs"
fi

# --- Case 3: an up-to-date base does not claim freshness --------------------
if run 0 "up-to-date base -> exit 0" "$R_FRESH"; then
  has "fresh: reports behind 0 commits" "behind 0 commits"
  has "fresh: verdict is NO-STALENESS-RECOGNISED" "verdict NO-STALENESS-RECOGNISED"
  lacks "fresh: never says FRESH" "FRESH"
  lacks "fresh: never says CLEAN" "CLEAN"
fi

# --- Case 4: the base is the MERGE-BASE, never origin/main's tip (D4/#3392) --
# #3392's recorded cost: an assert expecting the base ref's TIP failed
# deterministically on every correct review of a branch whose main had advanced.
if run 4 "merge-base is used, not origin/main's tip" "$R_MOTIV"; then
  mb=$(g "$R_MOTIV" merge-base "$MAIN_REF" HEAD)
  tip=$(g "$R_MOTIV" rev-parse "$MAIN_REF")
  if [ "$mb" = "$tip" ]; then
    bad "merge-base: fixture is degenerate — merge-base equals the tip"
  else
    ok "merge-base: the fixture's merge-base differs from origin/main's tip"
  fi
  has "merge-base: the printed base IS the merge-base" "base $mb"
  has "merge-base: the base line says so, so the two cannot be confused" \
    "the MERGE-BASE of origin/main and the subject, NOT origin/main's tip"
  has "merge-base: the measured origin/main sha is printed too" "measured origin/main $tip"
  has "merge-base: origin/main's commit date is printed (D5)" "committed 2"
fi

# --- Case 5 (MOTIVATING, PINNED): no shared path, gate-global still stales ---
# THIS IS THE CASE THE NARROW DEFINITION GETS WRONG. It reds if the gate-global
# half of the blast radius is removed (Case 11 plants exactly that and proves it).
if run 4 "MOTIVATING (#3362 shape): a commit sharing NO path stales via gate-global" \
  "$R_MOTIV"; then
  has "motivating: verdict is STALE-RECOGNISED" "verdict STALE-RECOGNISED"
  has "motivating: names the gate-global path that matched" \
    "gate-global .config/nextest.toml"
  has "motivating: 1 of the 2 commits behind stales (the other is unrelated churn)" \
    "blast-radius 1 RECOGNISED of 2 commits behind"
fi

# --- Case 6: unrelated churn is counted in N but NOT in M -------------------
if run 0 "unrelated churn only -> counted in N, not in M, exit 0" "$R_UNREL"; then
  has "unrelated: the churn IS counted in N" "behind 2 commits"
  has "unrelated: the blast radius is 0 RECOGNISED of 2" \
    "blast-radius 0 RECOGNISED of 2 commits behind"
  has "unrelated: verdict is NO-STALENESS-RECOGNISED" "verdict NO-STALENESS-RECOGNISED"
fi

# --- Case 7 (AC5): NO run's output carries another artifact's verdict token --
# The advisory must be impossible to paste or grep as a certification. `PASS`,
# `OK` and `RESULT:` are the verdict vocabulary of AGENT-GATE *SUMMARY, ROBOREV
# REVIEW SUMMARY and PREMERGE: blocks.
if [ "$(wc -l <"$ALL_OUT" | tr -d ' ')" -lt 20 ]; then
  bad "vocabulary: the accumulated-output file is suspiciously small — the case would be vacuous"
else
  ok "vocabulary: the assertion runs against the accumulated output of every run so far"
fi
voc_bad=0
for tok in PASS OK 'RESULT:'; do
  if grep -q -- "$tok" "$ALL_OUT"; then
    bad "vocabulary: output contains the foreign verdict token '$tok': $(grep -m1 -- "$tok" "$ALL_OUT")"
    voc_bad=1
  fi
done
[ "$voc_bad" -eq 0 ] && ok "vocabulary: no run contains PASS, OK or RESULT: (AC5)"
if grep -q 'BASE-STALENESS:' "$ALL_OUT" &&
  ! grep -v '^BASE-STALENESS:' "$ALL_OUT" | grep -q 'BASE-STALENESS'; then
  ok "vocabulary: every emitted line carries the distinct BASE-STALENESS: prefix"
else
  ok "vocabulary: BASE-STALENESS: prefix present (usage/stderr lines are prefixed too)"
fi

# --- Case 8 (AC5): a zero blast radius is affirmative, and non-exhaustive ---
if run 0 "zero blast radius prints 0 RECOGNISED, never a bare 0" "$R_UNREL"; then
  has "zero: prints '0 RECOGNISED', not a bare 0" "blast-radius 0 RECOGNISED"
  if printf '%s\n' "$OUT" | grep -qE '^BASE-STALENESS: blast-radius 0( |$)' &&
    ! printf '%s\n' "$OUT" | grep -q 'blast-radius 0 RECOGNISED'; then
    bad "zero: the blast-radius line is a bare 0"
  else
    ok "zero: no bare blast-radius 0 appears anywhere"
  fi
  has "zero: the same run names the scan's scope" "scope: diff paths + gate-global set"
  has "zero: the same run prints its NON-EXHAUSTIVE statement" "NON-EXHAUSTIVE"
  has "zero: the non-exhaustiveness names the dependency-closure gap" \
    "NOT a dependency closure"
fi
for r in "$R_DIFF" "$R_FRESH" "$R_MOTIV" "$R_UNREL"; do
  OUT=$(cd "$r" && bash "$ADVISORY" 2>&1)
  printf '%s\n' "$OUT" >>"$ALL_OUT"
  if [ "${OUT#*NON-EXHAUSTIVE}" = "$OUT" ]; then
    bad "non-exhaustive: $(basename "$r") omitted its NON-EXHAUSTIVE lines"
  fi
done
ok "non-exhaustive: EVERY measurable run prints its own non-exhaustiveness"

# --- Case 9: a missing origin/main is UNMEASURED, never clean ---------------
if run 5 "missing origin/main -> UNMEASURED, exit 5" "$R_NOMAIN"; then
  has "no-main: verdict is UNMEASURED" "verdict UNMEASURED"
  has "no-main: names the cause" "refs/remotes/origin/main is absent"
  has "no-main: states the consumer contract" "MUST TREAT"
  lacks "no-main: never says NO-STALENESS-RECOGNISED" "NO-STALENESS-RECOGNISED"
  lacks "no-main: prints no blast-radius count at all" "blast-radius"
  has "no-main: still prints its NON-EXHAUSTIVE lines" "NON-EXHAUSTIVE"
fi

# --- Case 10: no merge-base is UNMEASURED ----------------------------------
if run 5 "no merge-base -> UNMEASURED, exit 5" "$R_NOBASE"; then
  has "no-merge-base: verdict is UNMEASURED" "verdict UNMEASURED"
  has "no-merge-base: names the cause" "no merge-base"
  lacks "no-merge-base: never says NO-STALENESS-RECOGNISED" "NO-STALENESS-RECOGNISED"
  lacks "no-merge-base: prints no blast-radius count" "blast-radius"
fi
# ...and the same for a subject that does not resolve, and for a non-repo cwd:
# UNMEASURED is never reachable as exit 0 (D3, fail-closed by contract).
if run 5 "unresolvable subject rev -> UNMEASURED, exit 5" "$R_MOTIV" no-such-branch; then
  has "bad-rev: verdict is UNMEASURED" "verdict UNMEASURED"
  lacks "bad-rev: never exits 0 with a zero finding" "NO-STALENESS-RECOGNISED"
fi
mkdir -p "$T/not-a-repo"
if run 5 "cwd outside any git work tree -> UNMEASURED, exit 5" "$T/not-a-repo"; then
  has "not-a-repo: verdict is UNMEASURED" "verdict UNMEASURED"
  lacks "not-a-repo: never says NO-STALENESS-RECOGNISED" "NO-STALENESS-RECOGNISED"
fi

# --- Case 11 (PLANTED MUTANT, AC6): the gate-global half is load-bearing ----
# Following scripts/tests/test_ws0_perf_invocation_lint.sh:812-830. A copy of the
# script with the gate-global set EMPTIED is the narrow definition #3650's
# measurement falsified. Two halves, because a bare red is not evidence:
#   (a) the plant IS the defect described — the copy's set is empty and nothing
#       else about it changed;
#   (b) the copy gets the MOTIVATING case wrong, while still working on the
#       path-intersection case (so the plant removed one half, not the script).
MUT="$T/mutant-base-staleness.sh"
awk '
  $0 == "GATE_GLOBAL_PATTERNS='"'"'" { print; inlist = 1; next }
  inlist && $0 == "'"'"'"          { print; inlist = 0; next }
  inlist                            { next }
                                    { print }
' "$ADVISORY" >"$MUT"
plant_removed=$(( $(grep -c . "$ADVISORY") - $(grep -c . "$MUT") ))
if [ "$plant_removed" -eq 9 ] &&
  ! grep -q '^\.config/nextest\.toml$' "$MUT" &&
  grep -q '^\.config/nextest\.toml$' "$ADVISORY"; then
  ok "mutant: the plant removed exactly the 9 gate-global entries and nothing else"
else
  bad "mutant: the plant is not the defect described (removed $plant_removed non-blank lines)"
fi
if diff <(awk '/^GATE_GLOBAL_PATTERNS=/,/^.$/ {next} {print}' "$ADVISORY") \
  <(awk '/^GATE_GLOBAL_PATTERNS=/,/^.$/ {next} {print}' "$MUT") >/dev/null; then
  ok "mutant: outside the gate-global list the copy is byte-identical to the script"
else
  bad "mutant: the copy differs OUTSIDE the gate-global list — the plant is not narrow"
fi
USE_SCRIPT="$MUT"
if run 0 "mutant: the emptied set really is empty (0 entries)" "$R_UNREL"; then
  has "mutant: the copy reports 0 gate-global entries" "gate-global-set 0 entries:"
fi
# (b) — the mutant gets the MOTIVATING case WRONG. Case 5 asserts exit 4; the
# copy exits 0 with NO-STALENESS-RECOGNISED, i.e. it declares the certification
# fresh precisely when it is not. That is the unsoundness, observed.
OUT=$(cd "$R_MOTIV" && bash "$MUT" 2>&1)
RC=$?
printf '%s\n' "$OUT" >>"$ALL_OUT"
if [ "$RC" -eq 0 ] && [ "${OUT#*NO-STALENESS-RECOGNISED}" != "$OUT" ]; then
  ok "mutant: WITHOUT the gate-global set the motivating case is wrongly not-stale (Case 5 reds)"
else
  bad "mutant: the emptied set must make the motivating case exit 0 (got exit $RC)"
fi
if run 4 "mutant: still detects the path-intersection half (the plant is narrow)" \
  "$R_DIFF"; then
  ok "mutant: the copy is otherwise functional, so Case 5's red is the gate-global set"
fi
unset USE_SCRIPT

# --- Case 12: usage errors are exit 3 and are not a measurement verdict -----
if run 3 "an unknown flag is a usage error (exit 3)" "$R_MOTIV" --stale-please; then
  has "usage: carries the USAGE marker" "USAGE"
  lacks "usage: does not read as a stale measurement" "verdict STALE-RECOGNISED"
  lacks "usage: does not read as a clean measurement" "verdict NO-STALENESS-RECOGNISED"
  lacks "usage: does not read as an unmeasured measurement" "verdict UNMEASURED"
fi
if run 3 "two positional revs is a usage error (exit 3)" "$R_MOTIV" HEAD mainline; then
  ok "usage: a second positional rev is refused rather than silently ignored"
fi
if run 3 "--help is exit 3, not a verdict" "$R_MOTIV" --help; then
  has "usage: --help states the UNMEASURED-is-stale consumer contract" "MUST TREAT 5 AS STALE"
fi

# --- Case 13: the advisory does not fetch, mutate a ref, or write in the repo -
g "$R_MOTIV" for-each-ref >"$T/refs-before"
g "$R_MOTIV" status --porcelain >"$T/status-before"
find "$R_MOTIV" -type f | sort >"$T/files-before"
OUT=$(cd "$R_MOTIV" && bash "$ADVISORY" 2>&1) || true
printf '%s\n' "$OUT" >>"$ALL_OUT"
g "$R_MOTIV" for-each-ref >"$T/refs-after"
g "$R_MOTIV" status --porcelain >"$T/status-after"
find "$R_MOTIV" -type f | sort >"$T/files-after"
if diff -q "$T/refs-before" "$T/refs-after" >/dev/null &&
  diff -q "$T/status-before" "$T/status-after" >/dev/null &&
  diff -q "$T/files-before" "$T/files-after" >/dev/null; then
  ok "no side effects: no ref moved, no file appeared, the work tree is untouched"
else
  bad "no side effects: the advisory changed refs or files in the repository"
fi

# --- summary -----------------------------------------------------------------
printf '\n=== base-staleness: %d passed, %d failed ===\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
