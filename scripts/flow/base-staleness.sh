#!/usr/bin/env bash
#
# base-staleness.sh — the #3650 SLICE 1 base-staleness ADVISORY.
#
# WHAT QUESTION THIS ANSWERS
# --------------------------
# `premerge-assert.sh` proves the diff has not moved since certification and
# that a full gate of record PASSed on that exact tree. It does NOT prove the
# diff was certified against the `main` it will join: a squash-merge composes
# the diff with main's CURRENT tip, so for any PR whose base is behind main the
# certified tree and the merged tree are DIFFERENT OBJECTS (#3650). Measured on
# #3358/PR #3362: base 2bde26a7c, 107 commits behind, whose head gate FAILed
# `core-tests` only because a known flake's fix (5e08db201, #3514) was on main
# and absent from that base. That was the benign direction; the malign one is a
# PASS at a stale head hiding an interaction with something that landed in
# between.
#
# This script reports the INFORMATION needed to see that: `N` commits behind the
# merge-base, and `M` of those touching the diff's blast radius. It is SLICE 1 of
# #3650 and is NON-BLOCKING: nothing here changes any verdict. The enforcement (a
# gate on the merge RESULT, and `premerge-assert.sh` requiring it fail-closed) is
# slice 2, filed separately.
#
# It is also the mechanization of the standing fleet triage rule — "is the fix
# for this red already on main and merely absent from my base?" — which is why it
# is a command a human can run on its own rather than code inlined into a caller.
#
# BLAST RADIUS = PATH INTERSECTION ∪ A HARD-CODED GATE-GLOBAL SET (D1)
# --------------------------------------------------------------------
# Decided BY MEASUREMENT against the case that produced the issue
# (docs/round-artifacts/issue-3650-blast-radius-measurements.md):
#   * path intersection ALONE is UNSOUND — on PR #3362 the culprit commit and
#     the PR's diff share NO path, so that definition's `M = 0` branch calls a
#     certification fresh exactly when it is not;
#   * "any churn behind the base" is what the owner's ruling refuses — 107 of
#     107 commits, forcing re-gate loops on a repo where main moves 12x in 4h;
#   * intersection ∪ gate-global fires on 28 of 107 (26%): the motivating case
#     is caught -- and named, `matched 5e08db201 gate-global .config/nextest.toml`
#     -- while 74% of the churn on an 8-day-old base still does not stale.
#     That count is THIS SCRIPT's own output, not a figure to be trusted from a
#     comment; re-derive it with
#       bash scripts/flow/base-staleness.sh 4bc6b913a6afc63d2fe7f234152da9b03ea03a89
# The gate-global set is content that can change ANY gate's verdict regardless of
# the diff. It is ONE list in ONE place below, hard-coded, with NO env override —
# #3312's second rule: an override is settable by the party it constrains, and
# "which paths stale my certification" is precisely what a lane wanting to skip a
# re-gate would widen.
#
# THE VOCABULARY IS CHOSEN SO THIS CANNOT BE READ AS A CERTIFICATION (D2)
# ----------------------------------------------------------------------
# No `PASS`, no `OK`, no `RESULT:` appears in ANY run's output — those are the
# verdict vocabulary of `AGENT-GATE *SUMMARY`, `ROBOREV REVIEW SUMMARY` and
# `PREMERGE:` blocks, and this repo's failure mode is someone grepping for one of
# them. Every line carries the distinct prefix `BASE-STALENESS:`. The no-finding
# verdict is `NO-STALENESS-RECOGNISED`, never `FRESH` and never `CLEAN`: it names
# a SCAN RESULT, not a state of the world. `M = 0` prints `0 RECOGNISED`, never a
# bare `0` (precedent: `cfg-gated-subtree gaps: N RECOGNISED`), and every run
# prints its own `NON-EXHAUSTIVE` lines, in the OUTPUT rather than only in docs,
# because the output is what gets pasted.
#
# EXIT CODES, AND THE CONSUMER CONTRACT (D3)
# ------------------------------------------
#   0   NO-STALENESS-RECOGNISED — the scan completed and recognised nothing
#   4   STALE-RECOGNISED — at least one commit behind touches the blast radius
#   5   UNMEASURED — the scan could not be performed (no origin/main, no
#       merge-base, a git invocation failing, an unresolvable subject)
#   3   usage error — which is also what `--help` exits with, deliberately: exit
#       0 MEANS `NO-STALENESS-RECOGNISED` here, so a run that measured nothing at
#       all must never produce it.
#
# *** A CONSUMER MUST TREAT 5 / `UNMEASURED` AS STALE, NEVER AS FRESH. ***
# That is CLAUDE.md's standing rule: never derive a pass from the absence of a
# bad signal; where the sole oracle could not be consulted the verdict is
# non-passing. It is stated here, in the spec, and asserted by a test, because
# the shape that keeps recurring in this repo is a multi-state signal whose
# unmeasured state inherits the permissive branch. `UNMEASURED` is therefore
# never reachable as exit 0 and never prints a blast-radius count at all.
#
# Codes 4/5 are deliberately distinct from 0/2/3, which `premerge-assert.sh`
# already owns: reusing one would make this advisory indistinguishable from its
# caller's verdicts.
#
# THE BASE IS THE MERGE-BASE, NEVER origin/main's TIP (D4)
# -------------------------------------------------------
# #3392 is the recorded cost of getting this wrong: an assert that expected the
# base ref's TIP FAILed deterministically on every correct review of a branch
# whose main had advanced, and was misdiagnosed as a race twice. The merge-base
# actually used is PRINTED, labelled as such, so the two can never be confused
# in a pasted block.
#
# IT DOES NOT FETCH (D5)
# ----------------------
# A verifier with a side effect is a worse verifier. `origin/main` is read as
# found, and its sha AND commit date are printed so a reader can see whether the
# MEASUREMENT is itself stale. A missing `origin/main` is `UNMEASURED`, never a
# silent zero. Fetch first if you want a current answer.
#
# WHAT IS NOT COVERED, DECLARED ON EVERY RUN
# ------------------------------------------
# This is NOT a dependency closure. A commit changing a Rust item this diff
# CALLS, touching neither this diff's paths nor a gate-global path, can still
# change the verdict and is reported as NOT staling. Real false-negative class,
# not closed here, needs rustc dep-info as its information source (the route
# #3366 takes for the public-API question), filed separately.
#
# USAGE
#   scripts/flow/base-staleness.sh [<rev>]     # <rev> defaults to HEAD
#   scripts/flow/base-staleness.sh --help
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -uo pipefail

# ---------------------------------------------------------------------------
# THE GATE-GLOBAL SET — ONE list, ONE place, NO env override (D1/#3312).
#
# Content that can change ANY gate's verdict regardless of the diff. Three entry
# shapes, and nothing else is recognised:
#   exact           an exact repo-relative path
#   <prefix>/**     that subtree
#   **/<basename>   that basename anywhere in the tree
#
# `Cargo.toml`/`Cargo.lock` are written as `**/` forms because the gate builds
# the WORKSPACE: a manifest change in any member moves every gate's verdict, not
# only a root-manifest change. (design.md's list spells them bare; `**/` is the
# faithful reading of "the Cargo manifests", and is a superset, so it cannot
# create a false NO-STALENESS-RECOGNISED.)
# ---------------------------------------------------------------------------
GATE_GLOBAL_PATTERNS='
.config/nextest.toml
rust-toolchain.toml
**/Cargo.toml
**/Cargo.lock
scripts/agent-gate.sh
scripts/ci/**
cqlite-core/tests/support/**
test-data/**
.github/workflows/**
'

# Materialized ONCE into an array: the matcher is called for every path of every
# commit behind, and re-reading a here-doc per call would dominate the runtime
# budget in D9.
GATE_GLOBAL_LIST=()
while IFS= read -r _pat; do
  [ -n "$_pat" ] || continue
  GATE_GLOBAL_LIST+=("$_pat")
done <<EOF
$GATE_GLOBAL_PATTERNS
EOF

P='BASE-STALENESS:'

usage() {
  printf '%s USAGE — the call is wrong (this is NOT a measurement verdict)\n' "$P" >&2
  printf 'usage: %s [<rev>]      # <rev> defaults to HEAD\n' "$(basename "$0")" >&2
  printf '       Reports N commits on origin/main behind <rev>'"'"'s MERGE-BASE with\n' >&2
  printf '       origin/main, and M of those touching the diff'"'"'s blast radius\n' >&2
  printf '       (paths the diff touches + a hard-coded gate-global set).\n' >&2
  printf '       Exits 0 no-staleness-recognised / 4 stale-recognised /\n' >&2
  printf '       5 unmeasured / 3 usage. A CONSUMER MUST TREAT 5 AS STALE.\n' >&2
  printf '       Advisory only (#3650 slice 1): it changes no verdict anywhere.\n' >&2
}

# Non-exhaustiveness is printed on EVERY run, including the unmeasured ones — the
# output is what gets pasted, so the caveat travels with it.
print_non_exhaustive() {
  printf '%s NON-EXHAUSTIVE the blast radius is (paths this diff touches) + (a hard-coded\n' "$P"
  printf '%s NON-EXHAUSTIVE gate-global set). It is NOT a dependency closure: a commit that\n' "$P"
  printf '%s NON-EXHAUSTIVE changes an item this diff CALLS, while touching neither this\n' "$P"
  printf '%s NON-EXHAUSTIVE diff'"'"'s paths nor a gate-global path, can still change a gate'"'"'s\n' "$P"
  printf '%s NON-EXHAUSTIVE verdict and is reported here as NOT staling (#3650 non-goal).\n' "$P"
}

# unmeasured <cause...> — exit 5. Prints NO blast-radius count and NO
# NO-STALENESS-RECOGNISED, so it can never be misread as a zero finding (D3).
unmeasured() {
  while [ "$#" -gt 0 ]; do
    printf '%s unmeasured-cause %s\n' "$P" "$1"
    shift
  done
  print_non_exhaustive
  printf '%s verdict UNMEASURED — the scan could not be performed. A CONSUMER MUST TREAT\n' "$P"
  printf '%s verdict THIS AS STALE, never as fresh (#3650 D3); this is not a certification.\n' "$P"
  exit 5
}

rev=HEAD
rev_set=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help)
      usage
      exit 3
      ;;
    -*)
      usage
      exit 3
      ;;
    *)
      # A SECOND positional is refused, never silently ignored (and the flag is a
      # separate `rev_set` rather than `[ "$rev" != HEAD ]`, which accepted
      # `HEAD <other>` because the first value happened to equal the default).
      if [ "$rev_set" -eq 1 ]; then
        usage
        exit 3
      fi
      rev="$1"
      rev_set=1
      shift
      ;;
  esac
done
if [ -z "$rev" ]; then
  usage
  exit 3
fi

BASE_REF=refs/remotes/origin/main

# Scratch space for the NUL-separated git output (see the -z note below). In
# TMPDIR, never in the repository: this script writes nothing in the repo.
if ! TMPD=$(mktemp -d "${TMPDIR:-/tmp}/base-staleness.XXXXXX"); then
  unmeasured "could not create a scratch dir under ${TMPDIR:-/tmp}"
fi
trap 'rm -rf "$TMPD"' EXIT

# --- resolve the three inputs, each failure being UNMEASURED (never a zero) ---
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  unmeasured "not inside a git work tree (cwd $(pwd))"
fi
if ! subject_sha=$(git rev-parse --verify --quiet "$rev^{commit}" 2>/dev/null) ||
  [ -z "$subject_sha" ]; then
  unmeasured "the subject rev '$rev' does not resolve to a commit"
fi
if ! main_sha=$(git rev-parse --verify --quiet "$BASE_REF^{commit}" 2>/dev/null) ||
  [ -z "$main_sha" ]; then
  unmeasured "$BASE_REF is absent — this script does NOT fetch (#3650 D5); run" \
    "'git fetch origin main' and re-run. An absent base ref is unmeasurable, not clean."
fi
main_date=$(git log -1 --format=%cI "$main_sha" 2>/dev/null)
[ -n "$main_date" ] || main_date=UNMEASURED-DATE

# D4: the MERGE-BASE, never origin/main's tip. #3392 is the recorded cost.
if ! merge_base=$(git merge-base "$main_sha" "$subject_sha" 2>/dev/null) ||
  [ -z "$merge_base" ]; then
  unmeasured "no merge-base between $BASE_REF and '$rev' — unrelated histories" \
    "(or a shallow clone truncating the shared ancestry)."
fi

if ! behind=$(git rev-list --count "$merge_base..$main_sha" 2>/dev/null) ||
  [ -z "$behind" ]; then
  unmeasured "git rev-list --count $merge_base..$main_sha failed"
fi

# The diff's own paths. `-z` is MANDATORY (#3229): this repo tracks 40
# space-bearing paths under docs/, and a path-reading `git diff` without -z
# C-quotes them, so a newline-delimited read misclassifies and mis-compares.
# The diff's own paths. `-z` is MANDATORY (#3229): this repo tracks 40
# space-bearing paths under docs/, and a path-reading `git diff` without -z
# C-quotes them, so a newline-delimited read misclassifies and mis-compares.
#
# The NUL-separated output goes to a FILE and is read by REDIRECTION, never
# through `$( )` — command substitution DISCARDS NUL bytes, which would silently
# collapse every path into one record and defeat `-z` entirely. The file lives in
# TMPDIR: this script never writes in the repository.
if ! git diff --name-only -z "$merge_base...$subject_sha" >"$TMPD/diff-paths" 2>/dev/null; then
  unmeasured "git diff --name-only -z $merge_base...$subject_sha failed"
fi
DIFF_PATHS=()
diff_path_count=0
while IFS= read -r -d '' p; do
  [ -n "$p" ] || continue
  DIFF_PATHS+=("$p")
  diff_path_count=$((diff_path_count + 1))
done <"$TMPD/diff-paths"

# matches_gate_global <path> -> 0 if the path is in the gate-global set. Three
# entry shapes are recognised and NOTHING else: an exact path, `<prefix>/**`, and
# `**/<basename>`.
matches_gate_global() {
  local path="$1" pat bn
  # The count guard is for bash 3.2, where `"${arr[@]}"` on an EMPTY array is an
  # unbound-variable error under `set -u` (the planted-mutant test empties it).
  [ "${#GATE_GLOBAL_LIST[@]}" -eq 0 ] && return 1
  for pat in "${GATE_GLOBAL_LIST[@]}"; do
    case "$pat" in
      '**/'*)
        bn="${pat#**/}"
        [ "$path" = "$bn" ] && return 0
        case "$path" in */"$bn") return 0 ;; esac
        ;;
      *'/**')
        case "$path" in "${pat%/**}"/*) return 0 ;; esac
        ;;
      *)
        [ "$path" = "$pat" ] && return 0
        ;;
    esac
  done
  return 1
}

# matches_diff_paths <path> -> 0 if the path is one the diff itself touches.
matches_diff_paths() {
  local path="$1" i=0
  while [ "$i" -lt "$diff_path_count" ]; do
    [ "${DIFF_PATHS[$i]}" = "$path" ] && return 0
    i=$((i + 1))
  done
  return 1
}

# --- the scan ---------------------------------------------------------------
# One rev-list, then one diff-tree per commit behind (D9: ~1.5s warm on the
# 107-commit case). A pathological N is reported and scanned, never silently
# truncated — a truncated scan would have to be an UNMEASURED, so truncating
# would trade a slow answer for a fail-closed one.
commits=""
if [ "$behind" -gt 0 ]; then
  if ! commits=$(git rev-list "$merge_base..$main_sha" 2>/dev/null); then
    unmeasured "git rev-list $merge_base..$main_sha failed"
  fi
fi

m=0
matched_lines=""
for c in $commits; do
  # `-m --first-parent` so a MERGE commit reports its change against its first
  # parent instead of reporting NOTHING; `--root` so a root commit is not
  # silently empty either. Either silence would understate M.
  if ! git diff-tree -r -z --no-commit-id --name-only -m --first-parent --root "$c" \
    >"$TMPD/commit-paths" 2>/dev/null; then
    unmeasured "git diff-tree failed on commit $c — the scan is INCOMPLETE, so it is" \
      "unmeasurable rather than partially reported (#3650 D9)."
  fi
  hit=""
  why=""
  while IFS= read -r -d '' cp; do
    [ -n "$cp" ] || continue
    if matches_diff_paths "$cp"; then
      hit="$cp"
      why=diff-path
      break
    fi
    if matches_gate_global "$cp"; then
      hit="$cp"
      why=gate-global
      break
    fi
  done <"$TMPD/commit-paths"
  if [ -n "$hit" ]; then
    m=$((m + 1))
    matched_lines="$matched_lines$(git rev-parse --short=9 "$c" 2>/dev/null) $why $hit
"
  fi
done

# --- report ----------------------------------------------------------------
printf '%s subject %s (%s)\n' "$P" "$rev" "$subject_sha"
printf '%s base %s <- the MERGE-BASE of origin/main and the subject, NOT origin/main'"'"'s tip (#3392)\n' \
  "$P" "$merge_base"
printf '%s measured origin/main %s committed %s (this script does NOT fetch)\n' \
  "$P" "$main_sha" "$main_date"
printf '%s behind %s commits (on origin/main, not reachable from the merge-base)\n' "$P" "$behind"
printf '%s diff-paths %s (git diff --name-only -z <merge-base>...<subject>)\n' "$P" "$diff_path_count"
printf '%s gate-global-set %s entries: %s\n' "$P" "${#GATE_GLOBAL_LIST[@]}" "${GATE_GLOBAL_LIST[*]-}"
if [ "$m" -eq 0 ]; then
  # Never a bare `0` (D2): a bare zero in a log reads as a verified all-clear
  # from a scan documented as incomplete.
  printf '%s blast-radius 0 RECOGNISED of %s commits behind (scope: diff paths + gate-global set)\n' \
    "$P" "$behind"
else
  printf '%s blast-radius %s RECOGNISED of %s commits behind (scope: diff paths + gate-global set)\n' \
    "$P" "$m" "$behind"
  n=0
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    n=$((n + 1))
    if [ "$n" -le 20 ]; then
      printf '%s matched %s\n' "$P" "$line"
    fi
  done <<EOF
$matched_lines
EOF
  if [ "$n" -gt 20 ]; then
    printf '%s matched (+%s further staling commits scanned but not listed)\n' "$P" "$((n - 20))"
  fi
fi
print_non_exhaustive
if [ "$m" -gt 0 ]; then
  printf '%s verdict STALE-RECOGNISED — %s of the %s commits behind touch this diff'"'"'s\n' \
    "$P" "$m" "$behind"
  printf '%s verdict blast radius. Advisory only in #3650 slice 1: no verdict changes.\n' "$P"
  exit 4
fi
printf '%s verdict NO-STALENESS-RECOGNISED — a SCAN RESULT, not a state of the world, and\n' "$P"
printf '%s verdict not a certification. See the NON-EXHAUSTIVE lines above (#3650 slice 1).\n' "$P"
exit 0
