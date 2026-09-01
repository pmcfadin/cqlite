#!/usr/bin/env bash
# test_lane_scratch_ignored.sh — guard for the reserved lane-scratch ignore
# namespace `.lane-*` (issue #3760).
#
# WHY THIS EXISTS. `.gitignore` used to cover lane-local agent scratch by
# ENUMERATION (`.drive-issue-state.md`, `.impl-*-verdict.md`, `.review-*.md`,
# `.followup-*.md`, `.c-audit-*.md`). A scratch name nobody had listed is
# untracked-and-VISIBLE, so writing it while a gate of record runs is
# `tree-integrity: FAIL (tree-mutated-midrun)` under #2926 and VOIDS the run.
# Measured cost: 40 minutes on #3414, when `.gate-of-record-sha.txt` was created
# mid-gate. The repair is a reserved namespace so the NEXT name is covered by
# construction; this guard is what keeps that true.
#
# WHAT IT ASSERTS (all against the committed .gitignore):
#   1. novel `.lane-*` names are ignored — including the two names the #3414
#      incident actually wanted, spelled in the new namespace — at repo root,
#      nested, and directory-shaped;
#   2. every LEGACY enumerated scratch name is STILL ignored, so a future
#      tidy-up cannot silently drop one;
#   3. the `.lane-*` SUBTREE is genuinely invisible to
#      `git ls-files --others --exclude-standard` (the enumeration
#      tree-integrity uses) — MEASURED, because omitting a `!.lane-*/`
#      negation is a deliberate choice and the inverse of roborev job 209's;
#   4. the CONTRAST: a path under a `!<path>/`-negated `.agent-gate-*` name IS
#      visible, so the two behaviours are pinned as a deliberate PAIR and a
#      future blanket-negation edit reds here;
#   5. a positive control — a tracked source path is NOT ignored — so the guard
#      is demonstrably capable of failing.
#
# THIS SCRIPT CREATES NO FILES IN THE WORKTREE UNDER TEST. Writing scratch into
# the lane is the very defect being repaired, and this guard runs inside the
# gate of record. Everything happens in a throwaway `mktemp -d` + `git init`
# fixture seeded by COPYING the worktree's `.gitignore`; the fixture is also
# what makes the run hermetic (a box-local `.git/info/exclude` or a global
# core.excludesFile cannot manufacture a false PASS, and every ignore verdict is
# additionally required to have come from the seeded `.gitignore` itself).
#
# Hermetic: no network, no cargo, no datasets, no python3.
#
# Deliberately NOT `set -e` for the verdict: every assertion runs and failures
# are COUNTED, so one break does not hide the rest.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GITIGNORE="$REPO_ROOT/.gitignore"

fails=0
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; fails=$((fails + 1)); }

if [ ! -f "$GITIGNORE" ]; then
  echo "FAIL: no .gitignore at $GITIGNORE"
  exit 1
fi

# ---------------------------------------------------------------- fixture ----
# TERMINAL-XXXXXX template (macOS mktemp substitutes only a trailing run of X's).
fixture="$(mktemp -d "${TMPDIR:-/tmp}/lane-scratch-ignore-XXXXXX")" || {
  echo "FAIL: could not create a temp fixture dir"
  exit 1
}
cleanup() { rm -rf "$fixture"; }
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
trap 'cleanup; exit 129' HUP

# Neutralise every ignore source that is not the file under test, so a PASS can
# only come from the committed .gitignore.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

if ! git -c init.defaultBranch=main init -q "$fixture" >/dev/null 2>&1; then
  echo "FAIL: could not git-init the throwaway fixture at $fixture"
  exit 1
fi
cp "$GITIGNORE" "$fixture/.gitignore"
: >"$fixture/.git/info/exclude"
git -C "$fixture" config core.excludesFile /dev/null

g() { git -C "$fixture" "$@"; }

# check-ignore --no-index: decide purely from the ignore RULES, never from index
# state, and report the matching source:line:pattern so the verdict can be
# attributed to the seeded .gitignore.
ignore_source() { g check-ignore --no-index -v -- "$1" 2>/dev/null; }

# assert_ignored <path> [required-pattern-prefix]
assert_ignored() {
  local path="$1" want_prefix="${2:-}" out src pat
  out="$(ignore_source "$path")"
  if [ -z "$out" ]; then
    fail "'$path' is NOT ignored (a lane writing it mid-gate voids the gate of record)"
    return
  fi
  # format: <source>:<linenum>:<pattern>\t<pathname>
  out="${out%%$'\t'*}"
  src="${out%%:*}"
  pat="${out##*:}"
  if [ "$src" != ".gitignore" ]; then
    fail "'$path' is ignored, but by '$src', not the committed .gitignore"
    return
  fi
  if [ -n "$want_prefix" ] && [ "${pat#"$want_prefix"}" = "$pat" ]; then
    fail "'$path' is ignored by pattern '$pat', not by the reserved '$want_prefix' namespace"
    return
  fi
  pass "'$path' is ignored (.gitignore pattern '$pat')"
}

assert_not_ignored() {
  local path="$1" out
  out="$(ignore_source "$path")"
  if [ -n "$out" ]; then
    fail "positive control broke: '$path' IS ignored (${out%%$'\t'*}) — real source would be invisible"
    return
  fi
  pass "'$path' is NOT ignored (positive control: the guard can fail)"
}

# --- 1. the reserved namespace covers novel scratch names -------------------
# The first two are the names the #3414 incident actually wanted, spelled in the
# new namespace; then a nested path and a directory-shaped one.
for p in \
  ".lane-gate-of-record-sha.txt" \
  ".lane-rescue-wip.patch" \
  "some/dir/.lane-notes.md" \
  ".lane-3760/scratch.txt"; do
  assert_ignored "$p" ".lane-"
done

# --- 2. legacy enumerated scratch names are STILL ignored -------------------
# They stay for compatibility (#3760 sanctions this); pinning them stops a
# future tidy-up silently dropping one while their writers still exist.
for p in \
  ".drive-issue-state.md" \
  ".impl-x-verdict.md" \
  ".review-x.md" \
  ".followup-x.md" \
  ".c-audit-x.md"; do
  assert_ignored "$p"
done

# --- 3. the .lane-* SUBTREE is invisible to the tree-integrity enumeration --
# MEASURED, not assumed: this is the inverse of roborev job 209's assert and is
# the whole justification for omitting a `!.lane-*/` negation.
mkdir -p "$fixture/.lane-foo/deep"
echo scratch >"$fixture/.lane-foo/note.md"
echo scratch >"$fixture/.lane-foo/deep/note.md"
# Control file: proves the enumeration RAN and can see things, so an empty
# result below is a real ignore and not a broken invocation.
echo control >"$fixture/lane-scratch-visible-control.txt"

others="$(g ls-files --others --exclude-standard)"
if ! printf '%s\n' "$others" | grep -qx 'lane-scratch-visible-control.txt'; then
  fail "ls-files --others --exclude-standard did not see the control file — the measurement is void"
else
  pass "ls-files --others --exclude-standard sees the un-ignored control file"
fi
if printf '%s\n' "$others" | grep -q '^\.lane-foo/'; then
  fail ".lane-foo/ contents are VISIBLE to ls-files --others (a mid-gate write there would stamp dirty/tree-mutated)"
  printf '%s\n' "$others" | grep '^\.lane-foo/'
else
  pass ".lane-foo/ subtree is invisible to ls-files --others --exclude-standard (no negation, by design)"
fi

# --- 4. the deliberate CONTRAST: job 209's negated names DO surface ---------
# `/.agent-gate-summary.txt.launch-lock` is followed by `!/...launch-lock/`, so
# the DIRECTORY is re-included and source under it stays visible. Pinning both
# behaviours together means a future blanket `!.lane-*/` edit — or deleting a
# job-209 negation — reds here instead of silently changing one of them.
neg=".agent-gate-summary.txt.launch-lock"
mkdir -p "$fixture/$neg"
echo source >"$fixture/$neg/inner.rs"
others="$(g ls-files --others --exclude-standard)"
if printf '%s\n' "$others" | grep -qx "$neg/inner.rs"; then
  pass "job-209 negated '$neg/' still exposes its contents (deliberate contrast)"
else
  fail "'$neg/inner.rs' is invisible — the job-209 '!<path>/' negation was lost"
fi

# --- 5. positive control: real source is not ignored ------------------------
assert_not_ignored "cqlite-core/src/lib.rs"

# ------------------------------------------------------------------ verdict --
echo
if [ "$fails" -ne 0 ]; then
  echo "FAIL: lane-scratch ignore guard — $fails assertion(s) failed"
  echo "REMEDY: lane-local agent scratch MUST be named '.lane-<name>' and .gitignore"
  echo "        must carry the reserved, UN-ANCHORED namespace entry:"
  echo "            .lane-*"
  echo "        placed with the other lane-scratch entries near the end of .gitignore, with"
  echo "        NO '!.lane-*/' negation (a lane scratch DIRECTORY must be swallowed whole —"
  echo "        re-including it would surface its contents as untracked and stamp"
  echo "        'dirty: yes' / 'tree-mutated-midrun', i.e. reproduce issue #3760)."
  echo "        Verify a name before writing it mid-gate: git check-ignore -v <path>"
  exit 1
fi
echo "PASS: lane-scratch ignore guard (#3760)"
