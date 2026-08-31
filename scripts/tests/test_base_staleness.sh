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
#   3. THE ANCHORED OUTPUT GUARANTEE (Case 14, WHOLE SUITE). The original AC5
#      case asserted the ABSOLUTE form — "no run's output contains `PASS`, `OK`
#      or `RESULT:`" — and roborev job 233 (F2) FALSIFIED it: the advisory prints
#      repository-controlled paths verbatim, `test-data/**` is gate-global, and
#      the tracked path `test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK`.
#      That case passed only because the sampled run's matched set happened to
#      exclude such paths — a test passing for the wrong reason. What is asserted
#      now is the ANCHORED form: every nonempty output line of EVERY case, stdout
#      and stderr, begins with `BASE-STALENESS: `; the verdict appears only on a
#      `verdict ` line carrying a CLOSED-SET token; and the script's own STATIC
#      TEMPLATE TEXT carries none of the three tokens, asserted STRUCTURALLY over
#      the source (Case 15) because that property is provable while a claim about
#      one sample run is not. Violations are ACCUMULATED across every case and
#      reported once at the end — the old Case 7 recorded success on BOTH
#      branches of its prefix check (so it could never fail) and ran before the
#      usage cases, 7 of whose 8 lines were unprefixed.
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

# THE SCRATCH DIR IS VALIDATED BEFORE ANY PATH IS BUILT FROM IT (#3650 review
# B5). An unchecked `mktemp` leaves `$T` EMPTY, after which every `"$T/..."` in
# this suite resolves to an ABSOLUTE path at the ROOT — `/all-output.txt` and
# synthetic git repos directly under `/` — which a privileged run would really
# create. Aborting here, BEFORE the trap is installed, also keeps the trap from
# ever running `rm -rf ""`.
if ! T=$(mktemp -d "${TMPDIR:-/tmp}/base-staleness-test.XXXXXX" 2>/dev/null) ||
  [ -z "$T" ] || [ ! -d "$T" ]; then
  printf 'FAIL - could not create a scratch directory under %s: refusing to run, because\n' \
    "${TMPDIR:-/tmp}" >&2
  printf 'FAIL - every path in this suite would resolve under / instead.\n' >&2
  exit 1
fi
trap 'rm -rf "$T"' EXIT

# Every run's output is accumulated here so Case 14's whole-suite assertions
# cover ALL runs, not a sampled one.
ALL_OUT="$T/all-output.txt"
: >"$ALL_OUT"
# Violations are ACCUMULATED to files and reported ONCE at the end (F3): the
# check cannot then be short-circuited by running before a later case, and it
# cannot record success on both branches the way the old Case 7 did.
ANCHOR_BAD="$T/anchor-violations.txt"
VERDICT_BAD="$T/verdict-violations.txt"
: >"$ANCHOR_BAD"
: >"$VERDICT_BAD"

# record_out <tag> — accumulate $OUT and check the ANCHORED invariants on it:
#   D2a  every nonempty line begins with `BASE-STALENESS: ` (stdout AND stderr:
#        every `run` captures with 2>&1)
#   D2c  any `verdict ` line carries a token from the CLOSED set
# Called from run() so no case can forget it, and from the direct invocations too.
record_out() {
  local tag="$1" line tok
  printf '%s\n' "$OUT" >>"$ALL_OUT"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    case "$line" in
      'BASE-STALENESS: '*) ;;
      *) printf '%s\t%s\n' "$tag" "$line" >>"$ANCHOR_BAD" ;;
    esac
    case "$line" in
      'BASE-STALENESS: verdict '*)
        tok=${line#'BASE-STALENESS: verdict '}
        tok=${tok%% *}
        case "$tok" in
          STALE-RECOGNISED | NO-STALENESS-RECOGNISED | UNMEASURED) ;;
          *) printf '%s\t%s\n' "$tag" "$line" >>"$VERDICT_BAD" ;;
        esac
        ;;
    esac
  done <<RECORD_OUT
$OUT
RECORD_OUT
}

# verdict_lines — how many `verdict ` lines $OUT carries (exactly one per
# measurement run; zero for a usage error).
verdict_lines() {
  printf '%s\n' "$OUT" | grep -c '^BASE-STALENESS: verdict ' | tr -d ' '
}

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
  record_out "$desc"
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

# RESERVED — matched paths carrying the VERDICT VOCABULARY of this repo's OTHER
# artifacts, plus a SPACE-bearing and a NEWLINE-bearing path. Every path here is
# under the gate-global `test-data/**`, so each stales via a path the DIFF does
# NOT touch and is therefore printed VERBATIM on a `matched` line. This is the
# fixture the old absolute AC5 claim could not survive: `CI_SMOKE_TEST_USAGE.md`
# is a REAL tracked path in this repo and contains `OK` inside `SMOKE`.
# (`RESULT:` is not fixtured as a path: a colon in a filename is legal here but
# not portably creatable, and D2d covers that token structurally over the source.)
R_RES=$(newrepo reserved-substrings)
commit_paths "$R_RES" "the PR: sstable module" "cqlite-core/src/storage/sstable/mod.rs"
advance_main "$R_RES"
commit_paths "$R_RES" "behind: a path containing OK" \
  "test-data/scripts/CI_SMOKE_TEST_USAGE.md"
commit_paths "$R_RES" "behind: a path containing PASS" \
  "test-data/notes/PASSTHROUGH-fixture.md"
commit_paths "$R_RES" "behind: a SPACE-bearing path" \
  "test-data/notes/a spaced PASS name.md"
# GIT PERMITS NEWLINES IN PATHS. Unsanitized, this path emits a SECOND output
# line with NO prefix at all, breaking the one anchor everything rests on.
NL_PATH=$'test-data/notes/we\nird.md'
commit_paths "$R_RES" "behind: a NEWLINE-bearing path" "$NL_PATH"
publish_main "$R_RES"
back_to_feature "$R_RES"

# RENAME — the PR renames a file the campsite-rule way and a commit behind edits
# the OLD path. The porcelain `git diff` honours `diff.renames` (git's default is
# TRUE since 2.9) and reports the DESTINATION ONLY, while the commit scan's
# plumbing `git diff-tree` reports the old path — so without the pin the old path
# is absent from DIFF_PATHS, the commit matches NEITHER half, and the scan reports
# `blast-radius 0 RECOGNISED` on a base that is genuinely stale. A FAIL-OPEN.
# The renamed file is deliberately NOT gate-global, or the case would pass via the
# other half of the definition and prove nothing.
R_REN=$(newrepo rename)
advance_main "$R_REN"
commit_paths "$R_REN" "c1: the file the PR will rename" "cqlite-core/src/oldname.rs"
g "$R_REN" branch -f feature mainline >/dev/null
g "$R_REN" checkout -q feature
g "$R_REN" mv cqlite-core/src/oldname.rs cqlite-core/src/newname.rs
printf 'the PR also edits the renamed file\n' >>"$R_REN/cqlite-core/src/newname.rs"
g "$R_REN" add -A >/dev/null
g "$R_REN" -c user.email=t@t -c user.name=t commit -q -m "the PR: rename + edit" >/dev/null
advance_main "$R_REN"
commit_paths "$R_REN" "behind: main edits the OLD path" "cqlite-core/src/oldname.rs"
publish_main "$R_REN"
back_to_feature "$R_REN"

# RELATIVE — the same shape as STALE_DIFF, but with `diff.relative=true` set in
# the repo. That config is honoured by porcelain only, so run from a
# SUBDIRECTORY it emits paths with the subdirectory prefix STRIPPED, which can
# never equal the root-relative paths the commit scan reports. It makes `M` a
# function of the INVOKER'S CWD — a config the invoker controls, which is why the
# pin is required rather than defensive.
R_REL=$(newrepo relative-config)
commit_paths "$R_REL" "the PR edits the sstable module" \
  "cqlite-core/src/storage/sstable/mod.rs"
advance_main "$R_REL"
commit_paths "$R_REL" "behind: main edits the same module" \
  "cqlite-core/src/storage/sstable/mod.rs"
publish_main "$R_REL"
back_to_feature "$R_REL"
g "$R_REL" config diff.relative true

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
# RESERVED: the reserved-substring, space-bearing and NEWLINE-bearing paths are
# genuinely tracked. Read NUL-separated — a newline-delimited read of a
# newline-bearing path is precisely the bug under test.
res_ok=0; res_pass=0; res_space=0; res_nl=0
while IFS= read -r -d '' f; do
  case "$f" in *OK*) res_ok=1 ;; esac
  case "$f" in *PASS*) res_pass=1 ;; esac
  case "$f" in *' '*) res_space=1 ;; esac
  case "$f" in *$'\n'*) res_nl=1 ;; esac
done < <(g "$R_RES" ls-tree -r -z --name-only "$MAIN_REF" -- 'test-data')
if [ "$res_ok" -eq 1 ] && [ "$res_pass" -eq 1 ]; then
  ok "fixture(reserved): tracked paths genuinely contain the reserved substrings OK and PASS"
else
  bad "fixture(reserved): the reserved-substring paths are not tracked (ok=$res_ok pass=$res_pass)"
fi
if [ "$res_space" -eq 1 ] && [ "$res_nl" -eq 1 ]; then
  ok "fixture(reserved): a SPACE-bearing and a NEWLINE-bearing path are genuinely tracked"
else
  bad "fixture(reserved): space/newline paths are not tracked (space=$res_space nl=$res_nl)"
fi
# RENAME: git must ACTUALLY detect the rename on the porcelain side, or the case
# would pass for the wrong reason (an undetected rename emits both paths, which
# is the behaviour the pin forces anyway).
mb=$(g "$R_REN" merge-base "$MAIN_REF" HEAD)
g "$R_REN" diff --name-only -M "$mb...HEAD" | sort >"$T/ren-porcelain"
g "$R_REN" -c diff.renames=false diff --name-only "$mb...HEAD" | sort >"$T/ren-pinned"
if ! grep -qx 'cqlite-core/src/oldname.rs' "$T/ren-porcelain" &&
  grep -qx 'cqlite-core/src/newname.rs' "$T/ren-porcelain" &&
  grep -qx 'cqlite-core/src/oldname.rs' "$T/ren-pinned"; then
  ok "fixture(rename): git DOES detect the rename (destination only) and the pin restores the old path"
else
  bad "fixture(rename): the rename is not detected, so the case cannot exercise the asymmetry"
fi
ren_culprit=$(g "$R_REN" rev-list "$mb..$MAIN_REF" -- cqlite-core/src/oldname.rs)
if [ -n "$ren_culprit" ]; then
  ok "fixture(rename): a commit behind genuinely edits the OLD path"
else
  bad "fixture(rename): no commit behind touches cqlite-core/src/oldname.rs"
fi
if [ "$(g "$R_REL" config --get diff.relative)" = true ]; then
  ok "fixture(relative): diff.relative is genuinely set in the fixture repo"
else
  bad "fixture(relative): diff.relative is not set, so the case proves nothing"
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

# --- Case 7 (was the AC5 vocabulary case) — MOVED TO THE END OF THE SUITE ----
# It used to live HERE, and that placement was half of its defect: $ALL_OUT at
# this point holds Cases 2-6 only, so the UNMEASURED cases (9/10), the usage
# cases (12) and every mutant run appended AFTER the grep had already run — a
# reword of unmeasured() introducing a forbidden token would have shipped green,
# while the suite header and spec.md both claimed "any of the stale, non-stale or
# UNMEASURABLE cases". The other half was that its prefix check recorded success
# on BOTH branches (so it could never fail) and asked the wrong question
# ("does a NON-prefixed line mention the prefix"), with an else-branch rationale
# that was simply false — usage() printed 6 unprefixed lines.
# It is now Case 14, after the LAST case, and its absolute-substring half is
# retired as FALSIFIED (see the header). Nothing runs here.

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
  record_out "non-exhaustive sweep $(basename "$r")"
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
# Derived from the shipped list rather than hard-coded, so adding a gate-global
# entry cannot silently make the plant non-narrow (it was a literal `9` and went
# stale the moment `scripts/tests/**` was added).
GG_ENTRIES=$(awk "/^GATE_GLOBAL_PATTERNS='/{f=1;next} f&&/^'$/{f=0} f&&NF" "$ADVISORY" | grep -c .)
if [ "$GG_ENTRIES" -ge 5 ]; then
  ok "mutant: the shipped gate-global list has $GG_ENTRIES entries (derived, not hard-coded)"
else
  bad "mutant: could not derive the gate-global entry count (got '$GG_ENTRIES')"
fi
plant_removed=$(( $(grep -c . "$ADVISORY") - $(grep -c . "$MUT") ))
if [ "$plant_removed" -eq "$GG_ENTRIES" ] &&
  ! grep -q '^\.config/nextest\.toml$' "$MUT" &&
  grep -q '^\.config/nextest\.toml$' "$ADVISORY"; then
  ok "mutant: the plant removed exactly the $GG_ENTRIES gate-global entries and nothing else"
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
record_out "mutant(gate-global emptied) on the motivating fixture"
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
record_out "no-side-effects run"
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

# --- Case 13d: TMPDIR inside the checkout is REFUSED, AND NOTHING IS CREATED --
# `mktemp -d` honours TMPDIR, so the no-write-in-the-repository contract Case 13
# asserts is only as strong as the SCRATCH LOCATION. Point TMPDIR at a directory
# inside the work tree: the run must be UNMEASURED naming TMPDIR, and NOTHING
# must have been created. Both halves matter — an UNMEASURED that still wrote a
# scratch dir would satisfy the verdict and violate the contract.
#
# THE ORDERING HALF NEEDS AN OBSERVATION THAT SURVIVES THE RUN, AND A `find`
# AFTERWARDS IS NOT ONE (#3650 review job 239). Two reasons the obvious form is
# blind: `mktemp -d` creates an empty DIRECTORY (so `find -type f` sees nothing
# even while the defect is live), and the script's EXIT trap removes the scratch
# dir on the unmeasured path too — so a create-then-check ordering leaves no
# residue at all once the process has exited. Verified: the pre-fix script passes
# a directory-inclusive post-run snapshot.
#
# So the ordering is observed AT THE CREATE ITSELF, with a PATH shim recording
# every `mktemp` invocation. On the reject path the fixed script must not call
# `mktemp` AT ALL — the check precedes the create — while the pre-fix ordering
# calls it once, with an in-repository template. The post-run snapshot is kept as
# well (it covers a leftover the trap failed to remove), directory-inclusive
# rather than `-type f`; `.git` internals are excluded because a read-only git
# run may legitimately refresh the index, and the scratch dir the defect creates
# is not under `.git`.
MKTEMP_SHIM_DIR="$T/mktemp-shim"
mkdir -p "$MKTEMP_SHIM_DIR"
REAL_MKTEMP=$(command -v mktemp || true)
if [ -z "$REAL_MKTEMP" ]; then
  bad "tmpdir: no mktemp on PATH — the ordering observation cannot run"
else
  {
    printf '#!/usr/bin/env bash\n'
    # The real mktemp by ABSOLUTE path: the shim dir is first on PATH, so a bare
    # `mktemp` here would re-enter this script forever.
    printf '# Test shim: record the invocation, then delegate to the real mktemp.\n'
    printf 'printf %%s\\\\n "$*" >>"$MKTEMP_CALL_LOG"\n'
    printf 'exec %s "$@"\n' "$REAL_MKTEMP"
  } >"$MKTEMP_SHIM_DIR/mktemp"
  chmod +x "$MKTEMP_SHIM_DIR/mktemp"
fi
TMPDIR_IN_REPO="$R_MOTIV/scratch-inside-the-repo"
mkdir -p "$TMPDIR_IN_REPO"
snap_worktree() { find "$R_MOTIV" -mindepth 1 -not -path "$R_MOTIV/.git/*" | sort >"$1"; }
snap_worktree "$T/tmpdir-entries-before"
MKTEMP_LOG_REJECT="$T/mktemp-calls-reject.txt"
: >"$MKTEMP_LOG_REJECT"
OUT=$(cd "$R_MOTIV" && PATH="$MKTEMP_SHIM_DIR:$PATH" MKTEMP_CALL_LOG="$MKTEMP_LOG_REJECT" \
  TMPDIR="$TMPDIR_IN_REPO" bash "$ADVISORY" 2>&1)
RC=$?
record_out "TMPDIR inside the checkout"
if [ "$RC" -eq 5 ]; then
  ok "tmpdir: an in-repository TMPDIR is UNMEASURED (exit 5), never a measurement"
else
  bad "tmpdir: an in-repository TMPDIR must exit 5, got $RC (output: $OUT)"
fi
has "tmpdir: the verdict token is UNMEASURED" "verdict UNMEASURED"
has "tmpdir: the cause NAMES TMPDIR, so the fix is actionable" "TMPDIR"
has "tmpdir: the cause names the in-repository resolution" "INSIDE the repository"
has "tmpdir: the cause names the work tree as the enclosing root" "the work tree"
REJECT_CALLS=$(wc -l <"$MKTEMP_LOG_REJECT" | tr -d ' ')
if [ "$REJECT_CALLS" -eq 0 ]; then
  ok "tmpdir: NOTHING was created — the refused run never invokes mktemp (the check PRECEDES the create)"
else
  bad "tmpdir: the refused run created a scratch dir in the repository: mktemp was invoked $REJECT_CALLS time(s): $(tr '\n' ' ' <"$MKTEMP_LOG_REJECT")"
fi
snap_worktree "$T/tmpdir-entries-after"
if diff -q "$T/tmpdir-entries-before" "$T/tmpdir-entries-after" >/dev/null; then
  ok "tmpdir: the refused run left NOTHING in the repository (no file AND no directory)"
else
  bad "tmpdir: the refused run left entries in the repository: $(diff "$T/tmpdir-entries-before" "$T/tmpdir-entries-after" | tr '\n' ' ' | head -c 300)"
fi
rm -rf "$TMPDIR_IN_REPO"
# NON-VACUITY: the refusal is caused by the LOCATION, not by TMPDIR being set at
# all. The same run with TMPDIR outside the work tree must still MEASURE — and it
# must invoke the SHIM, or the zero above would be the shim never running rather
# than the create never happening.
TMPDIR_OUTSIDE="$T/scratch-outside"
mkdir -p "$TMPDIR_OUTSIDE"
MKTEMP_LOG_OK="$T/mktemp-calls-measured.txt"
: >"$MKTEMP_LOG_OK"
OUT=$(cd "$R_MOTIV" && PATH="$MKTEMP_SHIM_DIR:$PATH" MKTEMP_CALL_LOG="$MKTEMP_LOG_OK" \
  TMPDIR="$TMPDIR_OUTSIDE" bash "$ADVISORY" 2>&1)
RC=$?
record_out "TMPDIR outside the checkout"
if [ "$RC" -eq 4 ]; then
  ok "tmpdir: an out-of-repo TMPDIR is honoured and still MEASURES (the case is not vacuous)"
else
  bad "tmpdir: an out-of-repo TMPDIR must still measure (exit 4), got $RC (output: $OUT)"
fi
if [ "$(wc -l <"$MKTEMP_LOG_OK" | tr -d ' ')" -eq 1 ]; then
  ok "tmpdir: the shim IS wired — a measuring run invokes mktemp exactly once (so the reject-path zero is real)"
else
  bad "tmpdir: the shim recorded $(wc -l <"$MKTEMP_LOG_OK" | tr -d ' ') mktemp call(s) on a measuring run, wanted 1 — the reject-path zero would be vacuous"
fi

# --- Case 13e: TMPDIR under the GIT COMMON DIR is REFUSED (job 239 half 2) ---
# A toplevel-only check is BLIND in this fleet's standard configuration: every
# lane is a `git worktree`, so `--git-common-dir` is ALWAYS OUTSIDE the lane's
# toplevel (measured on lane-3650: toplevel /data/lanes/lane-3650, common dir
# /data/lanes/repo/.git). A TMPDIR there writes into state EVERY lane on the box
# shares. This case reds against a check that consults only the work tree root.
WT_LINKED="$T/motiv-linked-worktree"
if ! g "$R_MOTIV" worktree add --detach "$WT_LINKED" feature >/dev/null 2>&1; then
  bad "common-dir: could not create the linked-worktree fixture (case cannot run)"
else
  # FIXTURE SELF-CONSISTENCY (the Case 1 idiom): assert with git that this
  # fixture really has the property under test — a common dir OUTSIDE its own
  # toplevel — or the case would pass against a fixture that never had it.
  WT_TOP=$(cd "$WT_LINKED" && git rev-parse --show-toplevel)
  WT_COMMON=$(cd "$WT_LINKED" && cd "$(git rev-parse --git-common-dir)" && pwd -P)
  case "$WT_COMMON/" in
    "$WT_TOP"/*)
      bad "common-dir fixture: the linked worktree's common dir ($WT_COMMON) is INSIDE its toplevel ($WT_TOP) — the case would be vacuous"
      ;;
    *)
      ok "common-dir fixture: the linked worktree's common dir is outside its own toplevel (as every lane on this fleet is)"
      ;;
  esac
  TMPDIR_IN_COMMON="$WT_COMMON/scratch-inside-the-common-dir"
  mkdir -p "$TMPDIR_IN_COMMON"
  find "$TMPDIR_IN_COMMON" -mindepth 1 | sort >"$T/common-entries-before"
  OUT=$(cd "$WT_LINKED" && TMPDIR="$TMPDIR_IN_COMMON" bash "$ADVISORY" 2>&1)
  RC=$?
  record_out "TMPDIR inside the git common dir"
  if [ "$RC" -eq 5 ]; then
    ok "common-dir: a TMPDIR under the git common dir is UNMEASURED (exit 5)"
  else
    bad "common-dir: a TMPDIR under the git common dir must exit 5, got $RC (output: $OUT)"
  fi
  has "common-dir: the verdict token is UNMEASURED" "verdict UNMEASURED"
  has "common-dir: the cause NAMES TMPDIR" "TMPDIR"
  has "common-dir: the cause names the git common directory as the enclosing root" \
    "the git common directory"
  lacks "common-dir: never reports a zero finding" "NO-STALENESS-RECOGNISED"
  find "$TMPDIR_IN_COMMON" -mindepth 1 | sort >"$T/common-entries-after"
  if diff -q "$T/common-entries-before" "$T/common-entries-after" >/dev/null; then
    ok "common-dir: the refused run created NOTHING under the shared git directory"
  else
    bad "common-dir: the refused run created entries under the shared git dir: $(diff "$T/common-entries-before" "$T/common-entries-after" | tr '\n' ' ' | head -c 300)"
  fi
  rm -rf "$TMPDIR_IN_COMMON"
  # NON-VACUITY: the linked worktree itself MEASURES with an out-of-repo TMPDIR,
  # so the refusal above is the LOCATION and not the fixture being unusable.
  OUT=$(cd "$WT_LINKED" && TMPDIR="$TMPDIR_OUTSIDE" bash "$ADVISORY" 2>&1)
  RC=$?
  record_out "linked worktree, TMPDIR outside the repository"
  if [ "$RC" -eq 4 ]; then
    ok "common-dir: the same linked worktree MEASURES with an out-of-repo TMPDIR (not vacuous)"
  else
    bad "common-dir: the linked worktree must measure (exit 4) with an out-of-repo TMPDIR, got $RC (output: $OUT)"
  fi
  g "$R_MOTIV" worktree remove --force "$WT_LINKED" >/dev/null 2>&1 || rm -rf "$WT_LINKED"
fi

# --- Case 13f: an UNRESOLVABLE work tree root is UNMEASURED, never a pass ----
# The scratch check compares against two roots, and if either cannot be resolved
# it must NOT fall back to checking whichever one it got — a check that silently
# narrows its own subject is the permissive-branch shape #3650 refuses. A BARE
# repo is the fixture that separates the two: `git rev-parse --git-dir` SUCCEEDS
# there (so the work-tree probe passes) while `--show-toplevel` fails.
BARE="$T/bare-repo.git"
if ! git init -q --bare "$BARE" >/dev/null 2>&1; then
  bad "bare-repo: could not create the bare-repo fixture (case cannot run)"
else
  if (cd "$BARE" && git rev-parse --git-dir >/dev/null 2>&1) &&
    ! (cd "$BARE" && git rev-parse --show-toplevel >/dev/null 2>&1); then
    ok "bare-repo fixture: --git-dir resolves and --show-toplevel does not (the case is not vacuous)"
  else
    bad "bare-repo fixture: expected a resolvable --git-dir and an unresolvable --show-toplevel"
  fi
  OUT=$(cd "$BARE" && bash "$ADVISORY" 2>&1)
  RC=$?
  record_out "bare repo (unresolvable work tree root)"
  if [ "$RC" -eq 5 ]; then
    ok "bare-repo: an unresolvable work tree root is UNMEASURED (exit 5)"
  else
    bad "bare-repo: an unresolvable work tree root must exit 5, got $RC (output: $OUT)"
  fi
  has "bare-repo: the verdict token is UNMEASURED" "verdict UNMEASURED"
  lacks "bare-repo: never reports a zero finding" "NO-STALENESS-RECOGNISED"
  lacks "bare-repo: prints no blast-radius count" "blast-radius"
fi

# --- Case 12b: the USAGE path needs no external command (#3650 B4) ---------
# The usage block prints the program name, and it used to come from
# `$(basename "$0")` — an external command whose stderr is NOT captured, so a
# missing or failing `basename` emits a diagnostic with NO prefix, breaking D2a's
# anchor from the one function whose job is to be readable when the call is
# wrong. Run with a PATH pointing at an EMPTY DIRECTORY: `--help` exits before
# any git call, and every remaining operation is a bash builtin. `bash` itself is
# invoked by ABSOLUTE path (`$BASH`): with a PATH holding nothing, `bash <script>`
# fails at exec with 127 and emits an unprefixed line of the HARNESS's own
# making — which is how the first draft of this case FAILED for the wrong reason,
# and how its mutant half PASSED for the wrong reason (bash-not-found, not
# basename-not-found, was breaking the anchor).
EMPTY_BIN="$T/empty-bin"
mkdir -p "$EMPTY_BIN"
OUT=$(cd "$R_MOTIV" && PATH="$EMPTY_BIN" "$BASH" "$ADVISORY" --help 2>&1)
RC=$?
record_out "usage with an EMPTY PATH"
if [ "$RC" -eq 3 ]; then
  ok "usage(no PATH): --help still exits 3 with no external command available"
else
  bad "usage(no PATH): expected exit 3, got $RC (got: $OUT)"
fi
if printf '%s\n' "$OUT" | grep -qv '^BASE-STALENESS: '; then
  bad "usage(no PATH): an output line lacks the prefix: $(printf '%s\n' "$OUT" |
    grep -m1 -v '^BASE-STALENESS: ')"
else
  ok "usage(no PATH): every line keeps the prefix with no external command available"
fi
has "usage(no PATH): the program name is still printed" "usage: base-staleness.sh"

# PLANTED MUTANT: the SAME run against a copy that restores `basename` must break
# the anchor, so the case above is a property of the fix and not of an empty PATH
# being harmless. NOT recorded into $ALL_OUT — it is the violation the suite
# forbids.
MUT_BN="$T/mutant-basename.sh"
sed 's/"\$P" "\$(sane "\${0##\*\/}")" >&2/"$P" "$(sane "$(basename "$0")")" >\&2/' \
  "$ADVISORY" >"$MUT_BN"
if bash -n "$MUT_BN" 2>/dev/null && grep -q 'basename "\$0"' "$MUT_BN" &&
  ! grep -q 'sane "${0##\*/}"' "$MUT_BN"; then
  ok "basename-mutant: the plant IS the defect described (basename restored in usage)"
else
  bad "basename-mutant: the plant is not the defect described (syntax or content mismatch)"
fi
MUT_OUT=$(cd "$R_MOTIV" && PATH="$EMPTY_BIN" "$BASH" "$MUT_BN" --help 2>&1)
mut_unpref=$(printf '%s\n' "$MUT_OUT" | grep -m1 -v '^BASE-STALENESS: ')
# The unprefixed line must be ABOUT `basename`, not about anything else: the
# first draft of this case broke the anchor with `bash: No such file or
# directory` and read as proof of a defect it never exercised.
case "$mut_unpref" in
  *basename*)
    ok "basename-mutant: with basename unavailable it emits an UNPREFIXED line NAMING basename" ;;
  '') bad "basename-mutant: the plant did not break the anchor — Case 12b proves nothing" ;;
  *) bad "basename-mutant: the unprefixed line is not about basename: $mut_unpref" ;;
esac

# --- Case 13b: a PARTIAL/PROMISOR clone must not LAZILY FETCH (#3650 B2) ----
# Case 13 proves no side effect in an ORDINARY clone, where there is no fetch to
# trigger. The contract's real hazard is a PARTIAL clone: object access itself
# fetches over the network and WRITES a packfile, with no fetch command anywhere
# in the advisory. The fixture is a `--filter=tree:0 --no-checkout` clone over
# `file://`, so the trees the diff needs are genuinely absent locally while the
# promisor remote is genuinely reachable — which is what makes the measurement
# decisive rather than a demonstration that a broken remote fails.
#
# The observable is the OBJECT STORE FILE COUNT, not the exit code: an
# unreachable remote would also produce UNMEASURED, so only "did the repository
# grow" distinguishes "did not fetch" from "tried and failed".
PROM_SRC="$T/promisor-src"
PROM="$T/promisor-clone"
mkdir -p "$PROM_SRC"
git init -q -b main "$PROM_SRC" >/dev/null
g "$PROM_SRC" config user.email t@t
g "$PROM_SRC" config user.name t
g "$PROM_SRC" config uploadpack.allowFilter true
commit_paths "$PROM_SRC" "c0 initial" "cqlite-core/src/storage/sstable/mod.rs"
g "$PROM_SRC" checkout -q -b feature
commit_paths "$PROM_SRC" "the PR" "cqlite-core/src/storage/sstable/mod.rs"
g "$PROM_SRC" checkout -q main
commit_paths "$PROM_SRC" "behind: gate-global churn" ".config/nextest.toml"
prom_ok=1
if ! git clone -q --no-local --filter=tree:0 --no-checkout \
  "file://$PROM_SRC" "$PROM" >/dev/null 2>&1; then
  bad "promisor: could not build the partial-clone fixture (needs a git with --filter support)"
  prom_ok=0
fi
if [ "$prom_ok" -eq 1 ]; then
  g "$PROM" update-ref refs/heads/feature refs/remotes/origin/feature
  # FIXTURE SELF-CONSISTENCY (Case 1's idiom): it really is a promisor clone, and
  # the subject/base refs really are there. A non-promisor fixture would make the
  # whole case vacuous — nothing to lazily fetch.
  if [ "$(g "$PROM" config --get remote.origin.promisor 2>/dev/null)" = "true" ] &&
    g "$PROM" rev-parse --verify --quiet refs/heads/feature >/dev/null &&
    g "$PROM" rev-parse --verify --quiet "$MAIN_REF" >/dev/null; then
    ok "promisor fixture: the clone is a promisor clone with both refs present"
  else
    bad "promisor fixture: the clone is not a promisor clone (the case would be vacuous)"
    prom_ok=0
  fi
fi
if [ "$prom_ok" -eq 1 ]; then
  prom_before=$(find "$PROM/.git/objects" -type f | wc -l | tr -d ' ')
  if run 5 "promisor clone: a missing object is UNMEASURED, never a lazy fetch" \
    "$PROM" refs/heads/feature; then
    has "promisor: the unmeasurable scan names the git call that failed" \
      "unmeasured-cause git diff --name-only -z"
  fi
  prom_after=$(find "$PROM/.git/objects" -type f | wc -l | tr -d ' ')
  if [ "$prom_before" = "$prom_after" ]; then
    ok "promisor: the object store did NOT grow ($prom_before files before and after)"
  else
    bad "promisor: the advisory LAZILY FETCHED — object files $prom_before -> $prom_after (#3650 B2)"
  fi
  # NON-VACUITY, and it must run LAST because it mutates the fixture: the same
  # object access WITHOUT the guard really does fetch and really does write. Run
  # with the variable explicitly cleared, because this suite's own environment
  # may already carry it.
  if env -u GIT_NO_LAZY_FETCH git -C "$PROM" diff --name-only \
    "$MAIN_REF...refs/heads/feature" >/dev/null 2>&1; then
    prom_probe=$(find "$PROM/.git/objects" -type f | wc -l | tr -d ' ')
    if [ "$prom_probe" -gt "$prom_after" ]; then
      ok "promisor probe: unguarded object access DOES fetch and write ($prom_after -> $prom_probe)"
    else
      bad "promisor probe: unguarded access wrote nothing — the case proves nothing about fetching"
    fi
  else
    bad "promisor probe: unguarded object access failed — the promisor remote is not reachable"
  fi
fi

# --- Case 13c: the `matched` record's SHA FIELD is a real sha (#3650 B3) ----
# The field used to come from an UNCHECKED `git rev-parse --short=9` inside a
# command substitution, whose failure is SWALLOWED — yielding a record with an
# EMPTY sha, i.e. `matched  gate-global <path>`. It is now bash truncation, which
# cannot fail. Asserted against GIT rather than against a shape: the token must
# RESOLVE to a commit in the fixture, and that commit must be one of the commits
# BEHIND, not the subject.
if run 4 "the matched record's sha resolves to a commit behind the base" "$R_MOTIV"; then
  m_sha=$(printf '%s\n' "$OUT" |
    sed -n 's/^BASE-STALENESS: matched \([^ ]*\) .*/\1/p' | head -1)
  case "$m_sha" in
    [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f])
      ok "matched-sha: the field is a 9-hex abbreviation, never the empty string" ;;
    *) bad "matched-sha: expected a 9-hex abbreviation, got '$m_sha' (got: $OUT)" ;;
  esac
  m_full=$(g "$R_MOTIV" rev-parse --verify --quiet "$m_sha^{commit}" 2>/dev/null) || m_full=""
  if [ -n "$m_full" ]; then
    ok "matched-sha: the abbreviation RESOLVES to a commit in the repository"
  else
    bad "matched-sha: '$m_sha' does not resolve to a commit — the field is not a real sha"
  fi
  if [ -n "$m_full" ] && g "$R_MOTIV" rev-list mainline | grep -qx "$m_full" &&
    ! g "$R_MOTIV" rev-list feature | grep -qx "$m_full"; then
    ok "matched-sha: it names a commit BEHIND the base, not the subject side"
  else
    bad "matched-sha: the resolved commit is not one of the commits behind the base"
  fi
fi

# --- Case 14: reserved substrings, spaces and NEWLINES in a matched path ----
# The absolute vocabulary claim ("no run's output contains PASS/OK/RESULT:") was
# FALSIFIED by review: the advisory prints repository-controlled paths verbatim.
# What is asserted instead is that such a path is printed VERBATIM (masking it
# would mangle it for the reader) and that the ANCHOR survives it.
if run 4 "reserved-substring / space / NEWLINE bearing matched paths" "$R_RES"; then
  has "reserved: all four staling commits are counted" \
    "blast-radius 4 RECOGNISED of 4 commits behind"
  has "reserved: an OK-bearing path is printed VERBATIM" \
    "gate-global test-data/scripts/CI_SMOKE_TEST_USAGE.md"
  has "reserved: a PASS-bearing path is printed VERBATIM" \
    "gate-global test-data/notes/PASSTHROUGH-fixture.md"
  has "reserved: a SPACE-bearing path is printed VERBATIM" \
    "gate-global test-data/notes/a spaced PASS name.md"
  has "reserved: the NEWLINE in a path is escaped VISIBLY, not emitted raw" \
    "gate-global test-data/notes/we\nird.md"
  if [ "$(verdict_lines)" -eq 1 ]; then
    ok "reserved: exactly ONE 'verdict ' line, despite paths carrying foreign verdict tokens"
  else
    bad "reserved: expected exactly one 'verdict ' line, got $(verdict_lines)"
  fi
  # The per-line anchor for THIS run specifically (the whole-suite roll-up is
  # Case 16; this one names the case that would break it).
  if printf '%s\n' "$OUT" | grep -qv '^BASE-STALENESS: '; then
    bad "reserved: an output line lacks the prefix: $(printf '%s\n' "$OUT" | grep -m1 -v '^BASE-STALENESS: ')"
  else
    ok "reserved: every line stays prefixed even with a newline-bearing path in the matched set"
  fi
fi

# --- Case 15 (PLANTED MUTANT): the CONTROL-CHAR SANITIZER is load-bearing ---
# A copy with sane() reduced to a pass-through. Two halves, as with Case 11: the
# plant IS the defect described, and it produces the unprefixed line that breaks
# the anchor. This output is deliberately NOT recorded into $ALL_OUT — it is the
# violation the suite exists to forbid.
MUT_SANE="$T/mutant-sane.sh"
awk '
  /^sane\(\) \{$/ { print "sane() { printf %s \"$1\"; }"; skip = 1; next }
  skip && /^\}$/  { skip = 0; next }
  skip            { next }
                  { print }
' "$ADVISORY" | sed "s/printf %s \"\$1\"/printf '%s' \"\$1\"/" >"$MUT_SANE"
if bash -n "$MUT_SANE" 2>/dev/null &&
  ! grep -q 'cntrl' "$MUT_SANE" && grep -q 'cntrl' "$ADVISORY"; then
  ok "sane-mutant: the plant IS the defect described (sane() reduced to a pass-through)"
else
  bad "sane-mutant: the plant is not the defect described (syntax or content mismatch)"
fi
# (a) A CALLER-controlled newline (an unresolvable subject rev) reaches an
# `unmeasured-cause` line through a single printf, so the pass-through emits a
# line with NO PREFIX AT ALL — the anchor broken, observed.
NL_REV=$'no-such\nbranch'
OUT=$(cd "$R_RES" && bash "$MUT_SANE" "$NL_REV" 2>&1)
RC=$?
if [ "$RC" -eq 5 ] && printf '%s\n' "$OUT" | grep -qv '^BASE-STALENESS: '; then
  ok "sane-mutant: WITHOUT sanitization a newline in a field emits an UNPREFIXED line"
else
  bad "sane-mutant: the pass-through must break the anchor (exit $RC, all lines prefixed?)"
fi
# ...and the SHIPPED script, same input: prefixed throughout, newline visible.
if run 5 "a NEWLINE-bearing subject rev stays anchored and is escaped visibly" \
  "$R_RES" "$NL_REV"; then
  has "sanitize: the caller-supplied newline is escaped, not emitted raw" "no-such\nbranch"
  if printf '%s\n' "$OUT" | grep -qv '^BASE-STALENESS: '; then
    bad "sanitize: a newline-bearing rev broke the prefix in the SHIPPED script"
  else
    ok "sanitize: every line stays prefixed with a newline-bearing rev (the mutant reds here)"
  fi
fi
# (b) REPOSITORY-controlled data: on the reserved fixture the pass-through splits
# one matched path across TWO `matched` records, so the listing disagrees with the
# count it reports. The shipped script lists exactly the 4 it counted.
OUT=$(cd "$R_RES" && bash "$MUT_SANE" 2>&1)
mut_matched=$(printf '%s\n' "$OUT" | grep -c '^BASE-STALENESS: matched ')
OUT=$(cd "$R_RES" && bash "$ADVISORY" 2>&1)
record_out "shipped run on the reserved fixture"
ship_matched=$(printf '%s\n' "$OUT" | grep -c '^BASE-STALENESS: matched ')
if [ "$ship_matched" -eq 4 ] && [ "$mut_matched" -gt "$ship_matched" ]; then
  ok "sane-mutant: unsanitized, a newline-bearing PATH inflates the listing ($mut_matched vs $ship_matched matched lines)"
else
  bad "sane-mutant: expected the shipped script to list 4 and the mutant more (got $ship_matched / $mut_matched)"
fi

# --- Case 16 (BLOCKER A): rename symmetry — the fail-open the pin closes -----
if run 4 "a renamed PR path + a commit behind editing the OLD path is STALE" "$R_REN"; then
  has "rename: the OLD path is in the blast radius" \
    "diff-path cqlite-core/src/oldname.rs"
  has "rename: the staling commit is counted" "blast-radius 1 RECOGNISED of 1 commits behind"
fi
# The plant: drop the porcelain pin. It must FAIL OPEN — exit 0, blast-radius 0.
MUT_REN="$T/mutant-renames.sh"
sed 's/^if ! git -c diff\.renames=false -c diff\.relative=false \\$/if ! git \\/' \
  "$ADVISORY" >"$MUT_REN"
if [ "$(diff "$ADVISORY" "$MUT_REN" | grep -c '^[<>]')" -eq 2 ] &&
  ! grep -q 'diff.renames=false' "$MUT_REN" && grep -q 'diff.renames=false' "$ADVISORY"; then
  ok "rename-mutant: the plant removed exactly the porcelain pin and nothing else"
else
  bad "rename-mutant: the plant is not narrow (or the pin's spelling moved)"
fi
USE_SCRIPT="$MUT_REN"
if run 0 "rename-mutant: WITHOUT the pin the stale rename case FAILS OPEN" "$R_REN"; then
  has "rename-mutant: it wrongly reports a zero blast radius" "blast-radius 0 RECOGNISED"
  has "rename-mutant: and wrongly reports no staleness" "verdict NO-STALENESS-RECOGNISED"
fi
if run 4 "rename-mutant: still detects a non-renamed intersection (the plant is narrow)" \
  "$R_DIFF"; then
  ok "rename-mutant: the copy is otherwise functional, so Case 16's red is the pin"
fi
# UNSET BEFORE the shipped case below — leaving it set made the "shipped" relative
# case silently run the MUTANT (it red, which is how this was caught).
unset USE_SCRIPT
# ...and the diff.relative half, which is a config the INVOKER controls.
if run 4 "diff.relative=true + cwd in a SUBDIRECTORY still stales" "$R_REL/cqlite-core/src"; then
  has "relative: the root-relative path is still matched" \
    "diff-path cqlite-core/src/storage/sstable/mod.rs"
fi
USE_SCRIPT="$MUT_REN"
if run 0 "relative-mutant: WITHOUT the pin, cwd + diff.relative FAIL OPEN" \
  "$R_REL/cqlite-core/src"; then
  has "relative-mutant: M becomes a function of the invoker's cwd" "blast-radius 0 RECOGNISED"
fi
unset USE_SCRIPT
if run 4 "the same repo from its ROOT stales with or without the pin (control)" "$R_REL"; then
  ok "relative: the fixture is stale from the repo root, so the subdirectory case is about cwd"
fi

# --- Case 16b (DERIVED, one assertion PER GATE-GLOBAL ENTRY) ----------------
# Case 11 empties the WHOLE list, so it catches wholesale removal — and nothing
# pinned an INDIVIDUAL entry: a mutation sweep found 8 of the 10 silently
# deletable with the suite still green, including `scripts/tests/**`, the entry
# review had just added. The two that did red were covered only INCIDENTALLY, as
# a side effect of the motivating and reserved-substring fixtures.
#
# DERIVED, NOT CURATED (the repo rule behind `legacy-heuristics` computing its
# target set and `flight-tests` its unit set): this case reads
# GATE_GLOBAL_PATTERNS out of the SHIPPED script at run time and synthesizes one
# probe commit per entry, so a FUTURE entry is pinned for free with no test edit.
# That is what makes "the next person needs to find the list" actually hold.
#
# One probe path per recognised shape, chosen so the case proves the shape rather
# than accidentally matching some other way:
#   exact           -> the path itself
#   **/<basename>   -> some/member/<basename>  (in a SUBDIRECTORY, so the `**/`
#                      half is what matches, never the exact-match half)
#   <prefix>/**     -> <prefix>/probe-fixture.txt
# An UNRECOGNISED shape is a FAIL naming it, never a skip.
# TWO ORACLES, RECONCILED FAIL-CLOSED — because ONE derivation cannot pin an
# entry, BY CONSTRUCTION. The first version of this case derived the subject set
# from the script alone, and the mutation sweep showed why that is BLIND: drop an
# entry and the probe for it disappears with it, so the suite loses two `ok`s and
# reports 0 failures. The oracle shared a source with its subject. So the subject
# set is the UNION of:
#   A  the shipped script's own GATE_GLOBAL_PATTERNS (so a NEW entry is probed
#      for free, with no test edit — derive, never curate), and
# DECLARED LIMIT: a COORDINATED deletion from BOTH A and B in one diff is NOT
# caught and goes green (measured). Two oracles that both live in this repo cannot
# see an edit that moves both, so this pins ONE-SIDED drift (a rebase, a cleanup, a
# partial edit), not a deliberate two-sided change. Saying only "a one-sided
# deletion reds" would read as "deletions are caught", which is the
# affirming-completeness-we-lack shape this suite exists to refuse. The control for
# the two-sided case is diff review — both hunks land in the same PR, which is why
# oracle B is a COMMITTED DECLARATION and not a generated artifact.
#   B  the INDEPENDENT committed declaration of the same list in the change's
#      design document, which doctrine already requires to be kept current in the
#      same change.
# An entry deleted from A alone stays in the union, its probe runs, the advisory
# does not stale, and the case REDS — which is the pin. The two sets are also
# reconciled directly, so a drop is reported by name in both directions. This is
# the repo's "same fact written twice, maintained BY HAND" pattern (cf. the
# `.roborev.toml` / census mirror), and it makes the lead's "findable list"
# condition stronger: script and design must AGREE.
GG_LIST=()
while IFS= read -r _e; do
  [ -n "$_e" ] || continue
  GG_LIST+=("$_e")
done < <(awk "/^GATE_GLOBAL_PATTERNS='/{f=1;next} f&&/^'$/{f=0} f&&NF" "$ADVISORY")

# Oracle B. Located by GLOB over both the active and the ARCHIVED openspec paths,
# because `openspec archive` moves the directory; if neither is readable that is a
# FAIL naming the reconciliation (the signal to re-home the oracle), never a
# silent fallback to oracle A alone — which is the blindness this exists to fix.
GG_DESIGN=""
for _cand in "$SCRIPT_DIR"/../../openspec/changes/*/design.md \
  "$SCRIPT_DIR"/../../openspec/changes/archive/*/design.md; do
  [ -f "$_cand" ] || continue
  grep -q 'D1a — What membership ASSERTS' "$_cand" || continue
  GG_DESIGN="$_cand"
  break
done
GG_DOC=()
if [ -n "$GG_DESIGN" ]; then
  while IFS= read -r _e; do
    [ -n "$_e" ] || continue
    GG_DOC+=("$_e")
  done < <(awk '
    /D1a — What membership ASSERTS/ { seen = 1 }
    seen && /^```$/ { fence++; next }
    seen && fence == 1 { for (i = 1; i <= NF; i++) print $i }
    fence >= 2 { exit }
  ' "$GG_DESIGN")
fi
if [ -z "$GG_DESIGN" ]; then
  bad "derived: the INDEPENDENT oracle (design.md's D1a list) could not be located — re-home it; refusing to rely on the script alone"
elif [ "${#GG_DOC[@]}" -eq 0 ]; then
  bad "derived: the independent oracle at $GG_DESIGN yielded ZERO entries — the reconciliation would be vacuous"
else
  ok "derived: the independent oracle (${#GG_DOC[@]} entries) was read from $(basename "$(dirname "$GG_DESIGN")")/design.md"
fi

# FAIL CLOSED on an empty derivation: an empty subject set must be a FAILURE
# naming the derivation, never a vacuous pass — an emptied list is exactly what
# Case 11 plants, so a silent zero here would excuse the defect under test.
if [ "${#GG_LIST[@]}" -eq 0 ]; then
  bad "derived: the GATE_GLOBAL_PATTERNS derivation from $ADVISORY yielded ZERO entries — refusing to pass vacuously"
elif [ "${#GG_LIST[@]}" -ne "$GG_ENTRIES" ]; then
  bad "derived: the entry list (${#GG_LIST[@]}) disagrees with the counted entries ($GG_ENTRIES)"
else
  ok "derived: ${#GG_LIST[@]} gate-global entries derived from the shipped script at run time"
fi

# Reconcile the two, naming any difference in BOTH directions.
printf '%s\n' ${GG_LIST[@]+"${GG_LIST[@]}"} | sort >"$T/gg-script"
printf '%s\n' ${GG_DOC[@]+"${GG_DOC[@]}"} | sort >"$T/gg-doc"
gg_only_script=$(comm -23 "$T/gg-script" "$T/gg-doc" | tr '\n' ' ')
gg_only_doc=$(comm -13 "$T/gg-script" "$T/gg-doc" | tr '\n' ' ')
if [ -z "$gg_only_script" ] && [ -z "$gg_only_doc" ] && [ -s "$T/gg-script" ]; then
  ok "derived: the script's gate-global list and the design document's declaration AGREE exactly"
else
  bad "derived: script/design gate-global lists DISAGREE — script-only:[$gg_only_script] design-only:[$gg_only_doc]"
fi

# The subject set is the UNION, so an entry dropped from either side is still
# PROBED and the probe is what reds.
GG_UNION=()
while IFS= read -r _e; do
  [ -n "$_e" ] || continue
  GG_UNION+=("$_e")
done < <(sort -u "$T/gg-script" "$T/gg-doc")
if [ "${#GG_UNION[@]}" -eq 0 ]; then
  bad "derived: the union of both oracles is EMPTY — refusing to pass vacuously"
else
  ok "derived: probing the UNION of both oracles (${#GG_UNION[@]} entries), so a one-sided deletion still reds"
fi
gg_i=0
# Asserted PER ENTRY, never with a suite-wide `ran > 0` (#3220): a count cannot
# see one entry skipping behind its siblings.
for gg_pat in ${GG_UNION[@]+"${GG_UNION[@]}"}; do
  gg_i=$((gg_i + 1))
  case "$gg_pat" in
    '**/'*) gg_probe="some/member/${gg_pat#**/}" ;;
    *'/**') gg_probe="${gg_pat%/**}/probe-fixture.txt" ;;
    *'*'*)
      bad "derived: entry '$gg_pat' is an UNRECOGNISED shape — the matcher recognises exact, <prefix>/** and **/<basename> only"
      continue
      ;;
    *) gg_probe="$gg_pat" ;;
  esac
  R_GG=$(newrepo "gate-global-$gg_i")
  # The diff touches a path that is NOT gate-global, so the ONLY way this can
  # stale is via the entry under test.
  commit_paths "$R_GG" "the PR: a non-gate-global path" \
    "cqlite-core/src/storage/sstable/mod.rs"
  advance_main "$R_GG"
  commit_paths "$R_GG" "behind: touches only the entry under test" "$gg_probe"
  publish_main "$R_GG"
  back_to_feature "$R_GG"
  if run 4 "derived[$gg_pat]: a commit behind touching only '$gg_probe' stales" "$R_GG"; then
    # `gate-global` and not `diff-path`: the entry is what matched, not the diff.
    has "derived[$gg_pat]: matched as gate-global via '$gg_probe'" \
      "gate-global $gg_probe"
    has "derived[$gg_pat]: exactly the one commit behind stales" \
      "blast-radius 1 RECOGNISED of 1 commits behind"
  fi
done

# --- Case 17 (WHOLE SUITE, was Case 7): the ANCHORED output guarantee -------
# Accumulated across EVERY case above and asserted HERE, after the last one. The
# old placement covered Cases 2-6 only, so the UNMEASURED, usage and mutant runs
# were never inspected.
nonempty=$(grep -c . "$ALL_OUT" | tr -d ' ')
if [ "$nonempty" -lt 150 ]; then
  bad "anchor: only $nonempty accumulated lines — the whole-suite assertion would be weak"
else
  ok "anchor: the whole-suite assertion inspects $nonempty output lines from every case"
fi
cov_missing=""
for needle in 'verdict STALE-RECOGNISED' 'verdict NO-STALENESS-RECOGNISED' \
  'verdict UNMEASURED' 'USAGE'; do
  grep -q "$needle" "$ALL_OUT" || cov_missing="$cov_missing '$needle'"
done
if [ -z "$cov_missing" ]; then
  ok "anchor: the accumulated output covers all THREE verdicts AND the usage path"
else
  bad "anchor: accumulated output missing:$cov_missing — narrower than the suite claims"
fi
if [ -s "$ANCHOR_BAD" ]; then
  bad "anchor: $(grep -c . "$ANCHOR_BAD" | tr -d ' ') line(s) lack the 'BASE-STALENESS: ' prefix; first: $(head -1 "$ANCHOR_BAD")"
else
  ok "anchor: EVERY nonempty line of EVERY case, stdout AND stderr, begins with 'BASE-STALENESS: ' (D2a)"
fi
if [ -s "$VERDICT_BAD" ]; then
  bad "anchor: a 'verdict ' line carries a token outside the closed set; first: $(head -1 "$VERDICT_BAD")"
else
  ok "anchor: every 'verdict ' token is from {STALE-RECOGNISED, NO-STALENESS-RECOGNISED, UNMEASURED} (D2c)"
fi
vl_bad=""
for r in "$R_DIFF" "$R_FRESH" "$R_MOTIV" "$R_UNREL" "$R_RES" "$R_NOMAIN" "$R_NOBASE"; do
  OUT=$(cd "$r" && bash "$ADVISORY" 2>&1)
  record_out "verdict-count sweep $(basename "$r")"
  [ "$(verdict_lines)" -eq 1 ] || vl_bad="$vl_bad $(basename "$r")=$(verdict_lines)"
done
if [ -z "$vl_bad" ]; then
  ok "anchor: every measurement run emits EXACTLY ONE 'verdict ' line (stale, clean and unmeasured)"
else
  bad "anchor: these runs did not emit exactly one 'verdict ' line:$vl_bad"
fi

# --- Case 18 (D2d): the script's own STATIC TEMPLATE TEXT is token-free -----
# The ABSOLUTE substring claim about a RUN is falsified and gone. This is what
# replaces it, and unlike a claim about one sample run it is PROVABLE: the
# script's own literal text — every printf format and every literal it prints —
# carries none of `PASS`, `OK`, `RESULT:`.
#
# Only WHOLE-LINE comments are stripped. That is the CONSERVATIVE direction: a
# trailing-comment strip would have to cut at a `#`, and this file's `#`
# characters live inside printf formats (`(#3392)`) and parameter expansions
# (`${pat#**/}`), so cutting there could TRUNCATE a template and HIDE a token.
# Keeping too much text can only produce a false FAIL, never a false PASS.
grep -v '^[[:space:]]*#' "$ADVISORY" >"$T/advisory-code.txt"
code_lines=$(grep -c . "$T/advisory-code.txt" | tr -d ' ')
all_lines=$(grep -c . "$ADVISORY" | tr -d ' ')
if [ "$code_lines" -lt "$all_lines" ] && [ "$code_lines" -gt 60 ] &&
  grep -q 'verdict NO-STALENESS-RECOGNISED' "$T/advisory-code.txt" &&
  grep -q 'NON-EXHAUSTIVE' "$T/advisory-code.txt" &&
  grep -q 'blast-radius' "$T/advisory-code.txt"; then
  ok "template: the comment-stripped source ($code_lines of $all_lines lines) still holds the output templates"
else
  bad "template: the comment strip left no usable template text ($code_lines of $all_lines) — the case would be vacuous"
fi
tmpl_bad=0
for tok in PASS OK 'RESULT:'; do
  if grep -q -- "$tok" "$T/advisory-code.txt"; then
    bad "template: the script's own static text contains '$tok': $(grep -m1 -- "$tok" "$T/advisory-code.txt")"
    tmpl_bad=1
  fi
done
if [ "$tmpl_bad" -eq 0 ]; then
  ok "template: the script's own STATIC text carries none of PASS, OK, RESULT: (D2d, structural)"
fi

# --- summary -----------------------------------------------------------------
printf '\n=== base-staleness: %d passed, %d failed ===\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
