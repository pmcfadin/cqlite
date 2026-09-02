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
# WHAT IT ASSERTS (all against the WORKING-TREE .gitignore of this checkout —
# deliberately not `git show HEAD:.gitignore`, because the gate certifies the
# working tree, so the working tree is the subject whose ignore rules decide
# whether a mid-run write is visible):
#   1. the reserved NAMESPACE itself: the winning pattern for a `.lane-*` path is
#      textually `.lane-*` — un-anchored and un-negated — plus a GENERATIVE case
#      using a name invented at run time, so the assertion cannot be satisfied by
#      any enumeration of names someone already thought of. Four literal samples
#      (including the two the #3414 incident actually wanted, spelled in the new
#      namespace) cover root, nested and directory-shaped spellings;
#   2. the namespace is not OVER-broad: the near misses `lane-foo` (no dot) and
#      `.lanes-foo` stay visible;
#   3. the reservation's PRECONDITION — no TRACKED file anywhere in the
#      repository lives under a `.lane-*` path. This is the entire safety
#      argument for omitting a `!.lane-*/` negation; unenforced, a later
#      `tools/x/.lane-runtime/mod.rs` would be invisible to
#      `git ls-files --others --exclude-standard`, to `dirty:` and to
#      tree-integrity, re-opening roborev job 209's subtree-wide false-clean;
#   4. every LEGACY enumerated scratch name is STILL ignored, so a future
#      tidy-up cannot silently drop one;
#   5. the `.lane-*` SUBTREE is genuinely invisible to
#      `git ls-files --others --exclude-standard` (the enumeration
#      tree-integrity uses) — MEASURED, because omitting a `!.lane-*/`
#      negation is a deliberate choice and the inverse of roborev job 209's;
#   6. the CONTRAST: a path under a `!<path>/`-negated `.agent-gate-*` name IS
#      visible, so the two behaviours are pinned as a deliberate PAIR and a
#      future blanket-negation edit reds here;
#   7. a positive control — a tracked source path is NOT ignored — so the guard
#      is demonstrably capable of failing.
#
# AN IGNORE VERDICT IS NEVER READ OFF rc 0 OR NON-EMPTY OUTPUT. `git check-ignore
# -v` exits 0 and PRINTS the winning pattern when that pattern is a NEGATION —
# measured: `.gitignore:2:!.review-keep.md` for a path that is NOT ignored — so a
# single appended `!.drive-issue-state.md` would make that name visible while a
# rc-only guard reported PASS. Every ignored-ness verdict here therefore requires
# the winning pattern to be attributed to .gitignore AND not to begin with `!`.
#
# DECLARED RESIDUAL, not closed: this pins the two re-inclusion shapes it names
# (a blanket `!.lane-*/`, and a `.lane-*` subtree that stays swallowed). A LATER,
# NARROW re-inclusion — say `!.lane-bar/` for one specific name — would NOT red
# here. Declaring it is deliberate: enumerating every re-inclusion someone might
# write is the same losing game as the enumeration this issue replaced.
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
# The fixture root MUST lie outside the worktree under test. A box exporting a
# repo-local TMPDIR would otherwise make this guard write its fixture INTO the
# lane mid-gate — the very defect under repair, and the same class as the
# TMPDIR-at/below-the-target hazard already recorded at scripts/agent-gate.sh:596
# (a capture list under the deletion target was eaten by the rm -rf, which then
# read as "nothing to restore"). Refuse rather than fall back: a silent fallback
# to /tmp would hide a misconfigured box that other tooling still trusts.
tmp_root="${TMPDIR:-/tmp}"
tmp_root_abs="$(cd "$tmp_root" 2>/dev/null && pwd -P)"
repo_root_abs="$(cd "$REPO_ROOT" && pwd -P)"
if [ -z "$tmp_root_abs" ]; then
  echo "FAIL: TMPDIR '$tmp_root' does not resolve to a usable directory"
  exit 1
fi
if [ "$tmp_root_abs" = "$repo_root_abs" ] || [ "${tmp_root_abs#"$repo_root_abs"/}" != "$tmp_root_abs" ]; then
  echo "FAIL: TMPDIR '$tmp_root_abs' is at or below the worktree under test ($repo_root_abs)."
  echo "      This guard would then write its fixture INTO the lane — mid-gate that is"
  echo "      'tree-integrity: FAIL (tree-mutated-midrun)' (#2926), the defect #3760 repairs."
  echo "REMEDY: export a TMPDIR outside the repository (e.g. TMPDIR=/tmp) and re-run."
  exit 1
fi

# TERMINAL-XXXXXX template (macOS mktemp substitutes only a trailing run of X's).
fixture="$(mktemp -d "$tmp_root_abs/lane-scratch-ignore-XXXXXX")" || {
  echo "FAIL: could not create a temp fixture dir under '$tmp_root_abs'"
  exit 1
}
cleanup() { rm -rf "$fixture"; }
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
trap 'cleanup; exit 129' HUP

# ------------------------------------------------- git environment allowlist --
# EVERY git call in this file runs under `env -i` PLUS THE ONE ALLOWLIST BELOW.
# Copied in shape from scripts/agent-gate.sh's component-set pre-flight, which
# took this same finding twice (#3544 rounds 13 and 276); round 276's lesson is
# that a per-call-site fix does not reach the sites a LATER change adds, so the
# rule is one wrapper and no bare `git` anywhere in the file.
#
# The exposure is reachable BY ACCIDENT, with no hostile actor: GIT ITSELF
# exports GIT_DIR and GIT_INDEX_FILE to hooks and to `rebase --exec` / `bisect
# run` children, so a guard invoked from one of those inherits them. A foreign
# GIT_INDEX_FILE makes the tracked-file census read ANOTHER index and
# affirmatively confirm that no tracked `.lane-*` path exists; GIT_DIR /
# GIT_WORK_TREE redirect the fixture checks away from the intended repository;
# GIT_CONFIG_COUNT/KEY_*/VALUE_* and GIT_CONFIG_PARAMETERS inject config at
# command-line precedence, which outranks the fixture's own local config.
#
# THE LINE THE ALLOWLIST DRAWS, so a future addition can be judged not argued:
#   ADMIT  what git needs to RUN AT ALL.
#   CLEAR  everything that can change WHICH repository, index, object store,
#          config or template git reads.
# Anything not listed is cleared by `env -i` — that is what makes this closed:
# a git environment variable invented tomorrow is cleared BY DEFAULT rather than
# needing to be discovered here.
#
# NOT ADMITTED, deliberately: HOME (this guard needs no keys, no credentials and
# no global config — nothing it does touches a remote), SSH_*/proxy vars (no
# network), LANG/LC_* (the C locale is the better one for parsing git's output).
# Location-specific values are passed EXPLICITLY per call (`-C <dir>`), never
# inherited.
GIT_ENV=(
  # PATH: find `git` itself.
  "PATH=${PATH:-/usr/bin:/bin}"
  # LC_ALL: stable, parseable output regardless of the caller's locale.
  "LC_ALL=C"
  # TMPDIR: git's own temporary files. The CONTAINMENT-VALIDATED value from
  # above, never the raw inherited one — git's temp files must not land inside
  # the worktree under test either.
  "TMPDIR=$tmp_root_abs"
  # THE NEUTRALISERS, last so nothing above can shadow them. Belt, not the
  # control: `env -i` has already removed HOME, so there is no global config to
  # find; these make the intent explicit and survive a future HOME admission.
  "GIT_CONFIG_GLOBAL=/dev/null"
  "GIT_CONFIG_SYSTEM=/dev/null"
)

# gg — THE git wrapper. Nothing in this file may call `git` directly.
gg() { env -i "${GIT_ENV[@]}" git "$@"; }

if ! gg -c init.defaultBranch=main init -q "$fixture" >/dev/null 2>&1; then
  echo "FAIL: could not git-init the throwaway fixture at $fixture"
  exit 1
fi
cp "$GITIGNORE" "$fixture/.gitignore"
# Belt, not the control (the control is the allowlist above): empty the
# fixture's own info/exclude and point core.excludesFile at /dev/null, so a
# PASS can only come from this checkout's .gitignore.
: >"$fixture/.git/info/exclude"
gg -C "$fixture" config core.excludesFile /dev/null

g() { gg -C "$fixture" "$@"; }

# check-ignore --no-index: decide purely from the ignore RULES, never from index
# state, and report the matching source:line:pattern so the verdict can be
# attributed to this checkout's .gitignore.
#
# THREE-VALUED, on purpose. `git check-ignore` exits 0 (a pattern decided the
# path), 1 (no pattern matched) or 128 (error). Collapsing 128 onto "no match" is
# the `1699-find-tristate` shape: a git without --no-index, an unreadable
# fixture, any failure at all would make the ONE assertion whose job is to prove
# this guard can fail pass VACUOUSLY. So the rc is captured, stderr is kept, and
# anything outside {0,1} is a named FAIL rather than an answer.
IGN_OUT=""      # first line of check-ignore -v output (empty when rc 1)
IGN_ERR=""      # stderr, reported on an unexpected rc
IGN_RC=0
ignore_source() {
  local err_file="$fixture/.git/lane-scratch-check-ignore.err"
  IGN_OUT="$(g check-ignore --no-index -v -- "$1" 2>"$err_file")"
  IGN_RC=$?
  IGN_ERR="$(cat "$err_file" 2>/dev/null)"
  rm -f "$err_file"
  [ "$IGN_RC" -eq 0 ] || [ "$IGN_RC" -eq 1 ]
}

# assert_ignored <path> [exact-required-pattern]
#
# An "ignored" verdict requires ALL of: rc 0, a winning pattern attributed to
# .gitignore, and that pattern NOT being a NEGATION. The last is load-bearing and
# is not deducible from the exit code: git exits 0 and prints the pattern when a
# `!`-pattern wins, so `!.drive-issue-state.md` appended to .gitignore makes that
# name VISIBLE while rc stays 0 — an untracked-and-visible scratch name is
# exactly what voids a gate of record (#2926/#3414).
assert_ignored() {
  local path="$1" want_exact="${2:-}" head src pat
  if ! ignore_source "$path"; then
    fail "'$path': git check-ignore failed (rc $IGN_RC) — verdict UNKNOWN, not 'ignored'${IGN_ERR:+: $IGN_ERR}"
    return
  fi
  if [ "$IGN_RC" -ne 0 ] || [ -z "$IGN_OUT" ]; then
    fail "'$path' is NOT ignored (a lane writing it mid-gate voids the gate of record)"
    return
  fi
  # format: <source>:<linenum>:<pattern>\t<pathname>. Strip the two leading
  # fields from the FRONT rather than taking the last colon-field, so a pattern
  # that itself contains a colon is still reported whole.
  head="${IGN_OUT%%$'\t'*}"
  src="${head%%:*}"
  pat="${head#*:}"
  pat="${pat#*:}"
  if [ "$src" != ".gitignore" ]; then
    fail "'$path' is ignored, but by '$src', not this checkout's .gitignore"
    return
  fi
  case "$pat" in
    '!'*)
      fail "'$path' is NOT ignored: the winning pattern '$pat' is a NEGATION (git still exits 0 and prints it)"
      return
      ;;
  esac
  if [ -n "$want_exact" ] && [ "$pat" != "$want_exact" ]; then
    fail "'$path' is ignored by pattern '$pat', not by the reserved namespace pattern '$want_exact' (an ENUMERATION of sample names is not the namespace)"
    return
  fi
  pass "'$path' is ignored (.gitignore pattern '$pat')"
}

# untracked_enumeration — `git ls-files --others --exclude-standard` in the
# fixture, rc-CHECKED. This is the exact enumeration tree-integrity uses. Its rc
# was previously discarded, so a failing invocation returned an empty list and
# the "subtree is invisible" assert would have PASSED on it, its red delegated to
# the neighbouring control-file assert — the coupling CLAUDE.md records as a
# latent false pass (#3564: ask of every key what fails the run if THIS key alone
# goes bad). Sets OTHERS; returns non-zero when the enumeration itself failed.
OTHERS=""
untracked_enumeration() {
  local rc=0
  OTHERS="$(g ls-files --others --exclude-standard 2>&1)" || rc=$?
  if [ "$rc" -ne 0 ]; then
    fail "git ls-files --others --exclude-standard failed (rc $rc) — the visibility measurement was NOT taken${OTHERS:+: $OTHERS}"
    OTHERS=""
    return 1
  fi
  return 0
}

# assert_not_ignored <path> <why> — a path that MUST stay visible.
assert_not_ignored() {
  local path="$1" why="$2" head pat
  if ! ignore_source "$path"; then
    fail "'$path': git check-ignore failed (rc $IGN_RC) — verdict UNKNOWN, not 'visible'${IGN_ERR:+: $IGN_ERR}"
    return
  fi
  if [ "$IGN_RC" -eq 0 ] && [ -n "$IGN_OUT" ]; then
    head="${IGN_OUT%%$'\t'*}"
    pat="${head#*:}"
    pat="${pat#*:}"
    # A negation WINNING here means the path is visible, which is what we want.
    case "$pat" in
      '!'*)
        pass "'$path' is NOT ignored ($why; re-included by '$pat')"
        return
        ;;
    esac
    fail "'$path' IS ignored by '$head' — $why"
    return
  fi
  pass "'$path' is NOT ignored ($why)"
}

# --- 1. the reserved NAMESPACE, not four names someone thought of -----------
# Each verdict must be attributed to the pattern text `.lane-*` EXACTLY, so
# replacing the wildcard with an enumeration of these very sample names reds
# here instead of passing — an enumeration is the defect this issue removed, and
# a guard satisfied by one would certify #3414 all over again.
# The first two are the names the #3414 incident actually wanted, spelled in the
# new namespace; then a nested path and a directory-shaped one.
LANE_PATTERN='.lane-*'
for p in \
  ".lane-gate-of-record-sha.txt" \
  ".lane-rescue-wip.patch" \
  "some/dir/.lane-notes.md" \
  ".lane-3760/scratch.txt"; do
  assert_ignored "$p" "$LANE_PATTERN"
done

# GENERATIVE case — the one that actually closes the class. The name is invented
# at RUN TIME, so no enumeration of names anyone anticipated can satisfy it; this
# is the property "the NEXT scratch name is covered by construction" stated as a
# test rather than as a comment.
# Extension-free on purpose: a suffix like `.tmp` or `.log` is matched by other
# rules in this .gitignore, so the verdict would no longer be attributable to the
# namespace alone.
generated=".lane-$$-${RANDOM}-${RANDOM}-generated"
assert_ignored "$generated" "$LANE_PATTERN"
assert_ignored "nested/dir/$generated" "$LANE_PATTERN"

# --- 2. the namespace is not OVER-broad -------------------------------------
# Near misses that must stay VISIBLE: a reserved prefix that quietly swallowed
# neighbouring names would hide real files from tree-integrity for the opposite
# reason. `lane-foo` lacks the leading dot; `.lanes-foo` differs after it.
assert_not_ignored "lane-foo" "no leading dot — outside the reserved namespace"
assert_not_ignored ".lanes-foo" "differs after the dot — outside the reserved namespace"

# --- 3. the reservation's PRECONDITION: no TRACKED file under .lane-* -------
# "Source may never live under a `.lane-*` path" is the entire safety argument
# for omitting a `!.lane-*/` negation, and prose enforces nothing. Because the
# pattern is un-anchored and git does not descend into an ignored directory, a
# later `tools/x/.lane-runtime/mod.rs` would be invisible to
# `git ls-files --others --exclude-standard`, to `dirty:` and to tree-integrity
# — roborev job 209's subtree-wide false-clean, re-opened. Census over the REAL
# repository index (read-only). Measured when written: 0 matches, so this costs
# nothing to keep true.
#
# THREE-VALUED, like the check-ignore probe above, and for the same reason. The
# first version of this census ended `... | grep -E '(^|/)\.lane-' || true`: an
# unreadable index produced empty output, grep exited 1, `|| true` swallowed it
# and the guard AFFIRMATIVELY reported that the precondition HOLDS — a positive
# verdict derived from the ABSENCE of a bad signal (the `1699-find-tristate`
# shape), inside the one assertion enforcing the precondition the whole
# no-negation decision rests on. "The precondition is broken" and "I could not
# measure it" are different operator actions and are worded differently below.
#
# The measurement is taken in its OWN step so its exit status is observable, and
# is written to a FILE outside the worktree under test: `-z` output carries NUL
# separators, and bash silently DROPS NUL bytes in a command substitution, which
# would give back the very property `-z` was chosen for. The scan is then a shell
# `case` glob over NUL-delimited records read by REDIRECTION (never a pipe, whose
# subshell would discard the result). Using no `grep` removes grep's own
# rc-2 failure mode rather than adding a branch for it.
census_out="$fixture/.git/lane-scratch-census.z"
census_err="$fixture/.git/lane-scratch-census.err"
census_rc=0
gg -C "$REPO_ROOT" ls-files -z >"$census_out" 2>"$census_err" || census_rc=$?
census_err_text="$(cat "$census_err" 2>/dev/null)"

if [ "$census_rc" -ne 0 ]; then
  fail "the tracked-file census COULD NOT BE TAKEN (git ls-files -z rc $census_rc) — the '.lane-*' reservation is UNVERIFIED, NOT confirmed${census_err_text:+: $census_err_text}"
else
  lane_violations=()
  censused=0
  while IFS= read -r -d '' censused_path; do
    censused=$((censused + 1))
    case "$censused_path" in
      .lane-* | */.lane-*) lane_violations+=("$censused_path") ;;
    esac
  done <"$census_out"

  if [ "$censused" -eq 0 ]; then
    # Zero tracked files is not a clean bill of health, it is an empty subject:
    # the census would then "pass" over nothing at all.
    fail "the tracked-file census saw ZERO tracked files under $REPO_ROOT — the '.lane-*' reservation is UNVERIFIED, NOT confirmed (is this a checkout of the repository?)"
  elif [ "${#lane_violations[@]}" -ne 0 ]; then
    fail "TRACKED file(s) live under a reserved '.lane-*' path — the no-negation decision's precondition is BROKEN"
    # Quoted iteration: a violating path containing a space must be reported as
    # ONE path, not word-split into two bogus ones.
    for censused_path in "${lane_violations[@]}"; do
      printf '  %s\n' "$censused_path"
    done
    echo "  REMEDY: move them out of '.lane-*'. That namespace is reserved for lane-local"
    echo "          scratch; git does not descend into an ignored directory, so source there"
    echo "          is invisible to ls-files --others, to 'dirty:' and to tree-integrity."
  else
    pass "no TRACKED file lives under a reserved '.lane-*' path ($censused tracked files censused; precondition of the no-negation decision)"
  fi
fi
rm -f "$census_out" "$census_err"

# --- 4. legacy enumerated scratch names are STILL ignored -------------------
# They stay for compatibility (#3760 sanctions this); pinning them stops a
# future tidy-up silently dropping one while their writers still exist.
#
# RETIREMENT CONDITION (so this cannot red on correct input): each entry here is
# owed to a WRITER that still emits that name. When a writer migrates to the
# `.lane-*` namespace and the enumerated `.gitignore` line is legitimately
# deleted, DELETE ITS ASSERTION IN THE SAME COMMIT. Removing the line while
# leaving the assertion would red a correct change, which is the guard agents
# learn to waive.
for p in \
  ".drive-issue-state.md" \
  ".impl-x-verdict.md" \
  ".review-x.md" \
  ".followup-x.md" \
  ".c-audit-x.md"; do
  assert_ignored "$p"
done

# --- 5. the .lane-* SUBTREE is invisible to the tree-integrity enumeration --
# MEASURED, not assumed: this is the inverse of roborev job 209's assert and is
# the whole justification for omitting a `!.lane-*/` negation.
mkdir -p "$fixture/.lane-foo/deep"
echo scratch >"$fixture/.lane-foo/note.md"
echo scratch >"$fixture/.lane-foo/deep/note.md"
# Control file: proves the enumeration RAN and can see things, so an empty
# result below is a real ignore and not a broken invocation.
echo control >"$fixture/lane-scratch-visible-control.txt"

if untracked_enumeration; then
  if ! printf '%s\n' "$OTHERS" | grep -qx 'lane-scratch-visible-control.txt'; then
    fail "ls-files --others --exclude-standard did not see the control file — the measurement is void"
  else
    pass "ls-files --others --exclude-standard sees the un-ignored control file"
  fi
  if printf '%s\n' "$OTHERS" | grep -q '^\.lane-foo/'; then
    fail ".lane-foo/ contents are VISIBLE to ls-files --others (a mid-gate write there would stamp dirty/tree-mutated)"
    printf '%s\n' "$OTHERS" | grep '^\.lane-foo/'
  else
    pass ".lane-foo/ subtree is invisible to ls-files --others --exclude-standard (no negation, by design)"
  fi
fi

# --- 6. the deliberate CONTRAST: job 209's negated names DO surface ---------
# `/.agent-gate-summary.txt.launch-lock` is followed by `!/...launch-lock/`, so
# the DIRECTORY is re-included and source under it stays visible. Pinning both
# behaviours together means a future blanket `!.lane-*/` edit — or deleting a
# job-209 negation — reds here instead of silently changing one of them.
neg=".agent-gate-summary.txt.launch-lock"
mkdir -p "$fixture/$neg"
echo source >"$fixture/$neg/inner.rs"
if untracked_enumeration; then
  if printf '%s\n' "$OTHERS" | grep -qx "$neg/inner.rs"; then
    pass "job-209 negated '$neg/' still exposes its contents (deliberate contrast)"
  else
    fail "'$neg/inner.rs' is invisible — the job-209 '!<path>/' negation was lost"
  fi
fi

# --- 7. positive control: real source is not ignored ------------------------
assert_not_ignored "cqlite-core/src/lib.rs" "tracked source must never be invisible"

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
  echo "        The namespace is RESERVED: no tracked source may live under a '.lane-*'"
  echo "        path — that reservation is the precondition the no-negation decision rests on."
  echo "        Verify a name before writing it mid-gate: git check-ignore -v <path>"
  echo "        (rc 0 alone is NOT proof: git exits 0 and prints the pattern when a '!'"
  echo "        negation wins, i.e. when the path is visible.)"
  exit 1
fi
echo "PASS: lane-scratch ignore guard (#3760)"
