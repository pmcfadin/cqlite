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
#   * intersection ∪ gate-global fires on 37 of 107 (35%): the motivating case
#     is caught -- and named, `matched 5e08db201 gate-global .config/nextest.toml`
#     -- while 65% of the churn on an 8-day-old base still does not stale.
#     That count is THIS SCRIPT's own output, not a figure to be trusted from a
#     comment; re-derive it with
#       bash scripts/flow/base-staleness.sh 4bc6b913a6afc63d2fe7f234152da9b03ea03a89
#     MEASURED AT origin/main b1e8598a2 (committed 2026-08-30). The sha is pinned
#     here on purpose: `behind` and therefore the count are functions of where
#     origin/main was, so a bare figure reads as a defect the moment main moves.
#     It was 28 of 107 before `scripts/tests/**` joined the set (see below).
# The gate-global set is content that can change ANY gate's verdict regardless of
# the diff. It is ONE list in ONE place below, hard-coded, with NO env override —
# #3312's second rule: an override is settable by the party it constrains, and
# "which paths stale my certification" is precisely what a lane wanting to skip a
# re-gate would widen.
#
# THE VOCABULARY IS CHOSEN SO THIS CANNOT BE READ AS A CERTIFICATION (D2)
# ----------------------------------------------------------------------
# THE ABSOLUTE FORM OF THIS GUARANTEE WAS FALSIFIED BY REVIEW (roborev job 233,
# finding F2), AND IS RECORDED HERE RATHER THAN QUIETLY SOFTENED. It used to read
# "no `PASS`, no `OK`, no `RESULT:` appears in ANY run's output". That is FALSE:
# this script prints repository-controlled data VERBATIM in its dynamic fields,
# and a repository path may contain any of those substrings. Confirmed against
# this tree: `test-data/**` is a gate-global pattern and the tracked path
# `test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK` (in `SMOKE`), so a
# commit touching it emits `OK` on a `matched` line. Three tracked paths contain
# `OK` today. The original AC5 test passed only because the sampled run's matched
# set happened to exclude them — a test passing for the wrong reason.
#
# THE ANCHORED FORM REPLACES IT, and it is what the tests assert:
#   (a) EVERY output line, stdout AND stderr, begins with `BASE-STALENESS: `.
#   (b) Every dynamic field is CONTROL-CHARACTER SANITIZED (newline, CR, other
#       C0, DEL -> a visible `\n`/`\r`/`\xNN` escape) so (a) cannot be broken by
#       repository-controlled data. GIT PERMITS NEWLINES IN PATHS: unsanitized, a
#       matched path containing one emits a line with NO PREFIX AT ALL, breaking
#       the very anchor everything else rests on. The path is otherwise kept
#       VERBATIM — masking a reserved substring would mangle it for the reader,
#       and #3312's rule is to anchor or remove the channel, never to pick a
#       rarer delimiter.
#   (c) The verdict appears ONLY on a `BASE-STALENESS: verdict ` line, and its
#       token is from the CLOSED set {STALE-RECOGNISED, NO-STALENESS-RECOGNISED,
#       UNMEASURED}. Continuation prose goes on `verdict-detail` lines, so the
#       verdict line's token position can never hold a word.
#   (d) This script's own STATIC TEMPLATE TEXT contains none of `PASS`, `OK`,
#       `RESULT:` — asserted STRUCTURALLY over the source file, which is a
#       provable property, unlike a claim about one sample run.
# DECLARED RESIDUAL: a repository path CAN contain `PASS`/`OK`/`RESULT:`, and
# when it does this script prints it. The anchor is what makes that harmless — a
# grep for a foreign verdict token can land on a path, but every line it can land
# on is visibly a `BASE-STALENESS:` line, and no line of this output can ever be
# mistaken for a line of an `AGENT-GATE`/`ROBOREV`/`PREMERGE` block.
#
# The no-finding verdict is `NO-STALENESS-RECOGNISED`, never `FRESH` and never
# `CLEAN`: it names a SCAN RESULT, not a state of the world. `M = 0` prints
# `0 RECOGNISED`, never a bare `0` (precedent: `cfg-gated-subtree gaps: N
# RECOGNISED`), and every run prints its own `NON-EXHAUSTIVE` lines, in the OUTPUT
# rather than only in docs, because the output is what gets pasted.
#
# EXIT CODES, AND THE CONSUMER CONTRACT (D3)
# ------------------------------------------
#   0   NO-STALENESS-RECOGNISED — the scan completed and recognised nothing
#   4   STALE-RECOGNISED — at least one commit behind touches the blast radius
#   5   UNMEASURED — the scan could not be performed: no origin/main, no
#       merge-base, an unresolvable subject rev, or the failure of a git
#       invocation FEEDING THE MEASUREMENT (`rev-parse` of either ref,
#       `merge-base`, `rev-list`, `diff`, `diff-tree`).
#
#       ONE GIT CALL IS EXCEPTED, AND THE EXCEPTION IS STATED HERE, WHERE THE
#       CLAIM IS MADE (#3650 review B3). This used to read "a git invocation
#       failing", an unqualified absolute the code deliberately violates: the
#       INFORMATIONAL commit date of origin/main (`git log -1 --format=%cI`)
#       degrades to the literal `DATE-UNAVAILABLE` in that one field and does NOT
#       make the run UNMEASURED. It feeds neither N nor M, so a fully MEASURED
#       scan stays measured; injecting the verdict token into a measured run
#       would false-positive a slice-2 consumer grepping `UNMEASURED`; and
#       escalating a cosmetic field to a non-verdict would red the tool on
#       correct input, which is the guard agents learn to waive. An absolute the
#       code violates is the same defect class as the falsified vocabulary claim
#       below, so it is SCOPED here rather than left to be rediscovered.
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
# `GIT_NO_LAZY_FETCH=1` IS WHAT MAKES THAT CLAIM TRUE RATHER THAN INTENDED
# (#3650 review B2). In a PARTIAL/PROMISOR clone — `--filter=blob:none`,
# `--filter=tree:0`, any `remote.origin.promisor=true` — plain OBJECT ACCESS
# lazily fetches over the network and WRITES a packfile into the repository:
# `rev-list`, `diff`, `diff-tree` and `log` all do it, with no fetch command
# anywhere in this file. Measured on git 2.43.0 against a `--filter=tree:0`
# clone: the `git diff <base>...<subject>` call alone took the object store from
# 4 files to 12. Exporting the variable ONCE, before ANY object access, turns
# that into an ordinary git failure, which every call site already routes to
# `UNMEASURED` — the correct verdict for an unmeasurable scan, and a consumer
# must treat it as stale (D3).
#
# THAT GUARANTEE IS SCOPED, DECLARED, AND NOT DETECTED: GIT >= 2.36 ON A
# NON-PROMISOR CLONE (#3650 review R5 F1, reversed by the owner ruling).
# `GIT_NO_LAZY_FETCH` is honoured only from git 2.36, so OUTSIDE that scope — an
# older git in a partial/promisor clone — the object reads below can still fetch
# and write packfiles, and this script neither detects nor reports that. It is a
# DECLARED PRECONDITION, not a checked one, and the `BASE-STALENESS:
# no-fetch-scope` line says so — DECLARED-NOT-VERIFIED — on every run.
#
# THE MEASUREMENT BEHIND THE SCOPE, so a reader can re-check it rather than trust
# it: every lane on this fleet runs git 2.43.0, and no clone here is a promisor
# clone — `git config --get remote.origin.promisor` and
# `git config --get extensions.partialclone` both exit 1 (key absent), and
# neither `objects/info/promisor` nor `objects/pack/*.promisor` exists under
# `--git-common-dir`. Both adverse conditions are therefore ABSENT.
#
# DETECTION FOR THEM WAS BUILT AND DELETED. Round 5 chose "make the absolute
# true" and shipped ~60 lines: a conservative `git --version` parse plus a
# three-valued promisor probe over three sources, refusing as `UNMEASURED`
# outside the supported combination. The owner reversed it on the #3549 R10
# precedent — where a scenario is unreachable, DECLARE the scope in code, in
# operator-visible output and in the PR body rather than carry machinery for a
# state that does not occur. The transferable lesson is at design.md D5:
# CHEAPNESS IS A PROPERTY OF THE IMPLEMENTATION, NOT OF THE DECISION, and it is
# only knowable after you build it. A repo-wide git version FLOOR is a
# project-policy decision and belongs to #3680, as a precondition of
# ENFORCEMENT; it is deliberately not imposed by this advisory.
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
# AMBIENT GIT STATE THAT SILENTLY CHANGES THIS MEASUREMENT — THE PINS, IN ONE
# PLACE (#3650 review R6 F1).
#
# Every entry here is the same defect class: repository- or invoker-controlled
# git state that alters what `merge-base`, `rev-list`, `diff` or `diff-tree`
# REPORT, without failing, so an unpinned input yields a confidently wrong answer
# — and the wrong answer is always the permissive one, a false
# `NO-STALENESS-RECOGNISED`. They were pinned in three separate places, each with
# its own rationale, so the SET of pinned ambient inputs was not visible anywhere.
# It is visible here. Add the next one to this list.
#
#   1. `GIT_NO_LAZY_FETCH=1` (D5/#3650 review B2) — EXPORTED BEFORE ANY OBJECT
#      ACCESS. In a partial/promisor clone, reading objects fetches from the
#      network and WRITES packfiles into the repository, so without this the
#      "never fetches, never writes" contract is an intention rather than a
#      property. A missing object then fails its git call, and every call site
#      below routes a git failure to UNMEASURED. Honoured only from git 2.36, so
#      the contract is SCOPED to git >= 2.36 on a non-promisor clone — declared,
#      not detected; see the header and the `no-fetch-scope` output line.
#   2. `GIT_NO_REPLACE_OBJECTS=1` (#3650 review R6 F1) — EXPORTED BEFORE ANY
#      OBJECT ACCESS. `refs/replace/*` entries are honoured by `merge-base`,
#      `rev-list`, `diff` AND `diff-tree`, so a single local replacement ref can
#      rewrite the ancestry this scan walks or HIDE a blast-radius path from a
#      commit behind. Measured on git 2.43.0 against a synthetic fixture (Case 20
#      in the test suite): with one `git replace` of a commit that touches the
#      gate-global `.config/nextest.toml`, `diff-tree` reported NO paths and the
#      scan emitted `blast-radius 0 RECOGNISED` / `NO-STALENESS-RECOGNISED` —
#      the permissive branch — while the same run with this variable exported
#      reported the path and `STALE-RECOGNISED`. `behind` was 1 either way, so
#      nothing else in the output showed the substitution. Unlike (1) this is
#      honoured by every git that has replacement refs at all, so it needs no
#      version measurement.
#   3. `diff.renames` and `diff.relative`, both pinned OFF with `-c` AT the
#      porcelain `git diff` call, NOT here, and deliberately so: `-c` is
#      per-invocation, and the PLUMBING `diff-tree` scan must stay UNPINNED for
#      rename symmetry. The full argument (and the measurement) is at that call
#      site, which is the only place that can state which side is pinned and why.
#      Named here so the reader of this block knows the set is 3, not 2. (Their
#      exact spelling is deliberately NOT reproduced in this comment: the test
#      suite's rename plant asserts that spelling is ABSENT from the mutant, and a
#      comment carrying it would make that plant check pass for the wrong reason.)
#
# NONE of these is settable by the caller (#3312: an override is settable by the
# party it constrains, and "which ambient state may bend my measurement" is
# precisely what a lane wanting to skip a re-gate would set).
# ---------------------------------------------------------------------------
export GIT_NO_LAZY_FETCH=1
export GIT_NO_REPLACE_OBJECTS=1

# ---------------------------------------------------------------------------
# THE GATE-GLOBAL SET — ONE list, ONE place, NO env override (D1/#3312).
#
# WHAT MEMBERSHIP ASSERTS (the predicate — read this before adding an entry):
#   *** CONTENT AT THIS PATH CAN CHANGE A GATE'S VERDICT INDEPENDENTLY OF THE
#       DIFF UNDER TEST. ***
# Not "is important", not "is shared" — INDEPENDENTLY OF THE DIFF. A path
# qualifies when a commit touching only it can flip ANY lane's full gate from
# PASS to FAIL (or back) while that lane's own diff is unchanged: the test
# runner's config, the toolchain pin, a workspace manifest, the gate script
# itself, the scripts the gate EXECUTES, shared test support, the fixture corpus,
# CI workflow definitions.
#
# TO ADD AN ENTRY: add one line below, in one of the three recognised shapes, and
# state in the commit message which gate COMPONENT it can flip and how you
# MEASURED its selectivity over the commits behind a real base (the method is in
# docs/round-artifacts/issue-3650-blast-radius-measurements.md). An unmeasured
# entry is not justified — the whole point of the set is that it is NARROWER than
# "any churn behind the base", so every entry has to earn its false-positive cost.
#
# THIS LIST IS DECLARED NON-CLOSED, and the output says so on every run. It is a
# curated, measured list of RECOGNISED gate-global content, not an enumeration of
# all such content: a path that is gate-global and absent from it is a
# false-negative this script reports as NOT staling. That is the SECOND declared
# gap, alongside the dependency-closure gap.
#
# Three entry shapes, and nothing else is recognised:
#   exact           an exact repo-relative path
#   <prefix>/**     that subtree
#   **/<basename>   that basename anywhere in the tree
#
# `Cargo.toml`/`Cargo.lock` are written as `**/` forms because the gate builds
# the WORKSPACE: a manifest change in any member moves every gate's verdict, not
# only a root-manifest change. (design.md's list spells them bare; `**/` is the
# faithful reading of "the Cargo manifests", and is a superset, so it cannot
# create a false NO-STALENESS-RECOGNISED.)
#
# `scripts/tests/**` is listed for the same reason `scripts/agent-gate.sh` is:
# the gate does not only READ that roster, it EXECUTES it — `tooling-tests` runs
# ~16 `scripts/tests/*.sh` — so a commit touching only, say,
# scripts/tests/test_worker_supervisor.sh reds EVERY lane's full gate regardless
# of that lane's diff, which is the membership predicate verbatim. Measured with
# THIS script over the same 107-commit base (subject 4bc6b913a, origin/main
# b1e8598a2): it takes the fired count from 28 to 37 of 107, so 9 commits behind
# stale ONLY because of it. Two neighbours were measured and DELIBERATELY NOT
# ADDED because they fire ZERO times there — `deny.toml`, and the 14 loose
# `scripts/*.sh` helpers enumerated as exact entries (both left 37 unchanged) —
# since an entry that has never fired buys only false positives.
# ---------------------------------------------------------------------------
GATE_GLOBAL_PATTERNS='
.config/nextest.toml
rust-toolchain.toml
**/Cargo.toml
**/Cargo.lock
scripts/agent-gate.sh
scripts/ci/**
scripts/tests/**
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

# sane <string> -> the string with every C0 control character and DEL replaced by
# a VISIBLE escape, on stdout. Applied to EVERY dynamic field (D2b).
#
# This is the load-bearing half of the anchor: git PERMITS NEWLINES IN PATHS, so
# an unsanitized path printed verbatim emits a SECOND line carrying no
# `BASE-STALENESS: ` prefix — which breaks the one invariant every consumer and
# every test rests on. It sanitizes CONTROL characters ONLY: the path is
# otherwise kept verbatim, because masking a reserved substring would mangle the
# path for the reader while the anchor already makes it harmless.
#
# The three common cases go through bash parameter substitution; the
# per-character fallback is entered only when a rarer control byte survives, so
# the hot path (one call per matched commit) stays substitution-only.
sane() {
  local s="$1" out c i n
  s="${s//$'\r'/'\r'}"
  s="${s//$'\n'/'\n'}"
  s="${s//$'\t'/'\t'}"
  case "$s" in
    *[[:cntrl:]]*) ;;
    *)
      printf '%s' "$s"
      return 0
      ;;
  esac
  out=""
  n=${#s}
  i=0
  while [ "$i" -lt "$n" ]; do
    c="${s:i:1}"
    case "$c" in
      [[:cntrl:]]) out=$(printf '%s\\x%02x' "$out" "'$c") ;;
      *) out="$out$c" ;;
    esac
    i=$((i + 1))
  done
  printf '%s' "$out"
}

# EVERY line here is prefixed too (F3). Under D2's anchored guarantee the prefix
# is THE load-bearing invariant, so an unprefixed usage line is a hole in it, not
# a cosmetic slip: 7 of these 8 lines used to lack it.
usage() {
  printf '%s USAGE — the call is wrong (this is NOT a measurement verdict)\n' "$P" >&2
  # `${0##*/}` rather than `$(basename "$0")` (#3650 review B4): `basename` is an
  # EXTERNAL command whose stderr is NOT captured here, so if it is missing or
  # fails the shell emits a diagnostic with NO `BASE-STALENESS: ` prefix —
  # breaking D2a's anchor, which is the one invariant every consumer and every
  # test rests on, from the one function whose job is to be readable when the
  # call was wrong. Bash suffix removal needs no subprocess and cannot fail. It
  # still goes through `sane`: `$0` is caller-controlled.
  printf '%s USAGE usage: %s [<rev>]      # <rev> defaults to HEAD\n' \
    "$P" "$(sane "${0##*/}")" >&2
  printf '%s USAGE Reports N commits on origin/main behind <rev>'"'"'s MERGE-BASE with\n' "$P" >&2
  printf '%s USAGE origin/main, and M of those touching the diff'"'"'s blast radius\n' "$P" >&2
  printf '%s USAGE (paths the diff touches + a hard-coded gate-global set).\n' "$P" >&2
  printf '%s USAGE Exits 0 no-staleness-recognised / 4 stale-recognised /\n' "$P" >&2
  printf '%s USAGE 5 unmeasured / 3 usage. A CONSUMER MUST TREAT 5 AS STALE.\n' "$P" >&2
  printf '%s USAGE Advisory only (#3650 slice 1): it changes no verdict anywhere.\n' "$P" >&2
}

# Non-exhaustiveness is printed on EVERY run, including the unmeasured ones — the
# output is what gets pasted, so the caveat travels with it.
# TWO declared gaps, not one. The earlier text named only the dependency closure,
# which affirmed a completeness the gate-global list does not have.
print_non_exhaustive() {
  printf '%s NON-EXHAUSTIVE the blast radius is (paths this diff touches) + (a hard-coded\n' "$P"
  printf '%s NON-EXHAUSTIVE gate-global set). TWO gaps are DECLARED, both false-negative:\n' "$P"
  printf '%s NON-EXHAUSTIVE gap 1 of 2 — NOT a dependency closure: a commit that changes an\n' "$P"
  printf '%s NON-EXHAUSTIVE item this diff CALLS, while touching neither this diff'"'"'s paths\n' "$P"
  printf '%s NON-EXHAUSTIVE nor a gate-global path, can still change a gate'"'"'s verdict and is\n' "$P"
  printf '%s NON-EXHAUSTIVE reported here as NOT staling (#3650 non-goal).\n' "$P"
  printf '%s NON-EXHAUSTIVE gap 2 of 2 — the gate-global set is a CURATED, MEASURED, DECLARED\n' "$P"
  printf '%s NON-EXHAUSTIVE NON-CLOSED list of RECOGNISED gate-global content, never an\n' "$P"
  printf '%s NON-EXHAUSTIVE enumeration of all of it: content that is gate-global and absent\n' "$P"
  printf '%s NON-EXHAUSTIVE from the list is likewise reported here as NOT staling.\n' "$P"
}

# unmeasured <cause...> — exit 5. Prints NO blast-radius count and NO
# NO-STALENESS-RECOGNISED, so it can never be misread as a zero finding (D3).
unmeasured() {
  while [ "$#" -gt 0 ]; do
    printf '%s unmeasured-cause %s\n' "$P" "$(sane "$1")"
    shift
  done
  print_non_exhaustive
  # D2c: the verdict TOKEN stands alone on the one `verdict ` line; prose goes on
  # `verdict-detail` lines, so the token position can never hold a word.
  printf '%s verdict UNMEASURED\n' "$P"
  printf '%s verdict-detail the scan could not be performed. A CONSUMER MUST TREAT THIS AS\n' "$P"
  printf '%s verdict-detail STALE, never as fresh (#3650 D3); this is not a certification.\n' "$P"
  exit 5
}

# --- EVERY SCRATCH READ IS A CHECKED OPEN (#3650 review R6 F2) --------------
#
# `done <"$TMPD/file"` IS AN UNCHECKED REDIRECT, AND THAT IS A FAIL-OPEN. If the
# file cannot be opened, bash does TWO things and both are shapes this script
# exists to refuse: it emits an UNPREFIXED diagnostic — breaking D2a's anchor
# from a line no `sane` call can reach — and the loop body NEVER RUNS, so the
# path set reads as EMPTY, `M` is UNDERCOUNTED and the verdict lands on the
# permissive `NO-STALENESS-RECOGNISED`. Neither half may be reached, so the three
# scratch reads below (`diff-paths`, `commit-paths`, `behind-commits`) open their
# file EXPLICITLY on a numbered fd and check the open.
#
# THE OPEN IS WRAPPED IN A BRACE GROUP so the suppression applies to the SHELL'S
# OWN redirect diagnostic. Measured on bash 5.2: redirections are processed left
# to right, so `exec 3<"$f" 2>/dev/null` prints the diagnostic BEFORE the
# suppression takes effect; and `exec 2>/dev/null 3<"$f"` would silence THIS
# SCRIPT'S stderr for the rest of the run, which would suppress the anchored
# lines too. A brace group does not fork, so the fd persists in the current
# shell, and the group's stderr redirect wraps the failing redirect.
#
# `unmeasured` is the only outcome of a failed open: an unreadable scratch file
# is a scan that did not happen, and a scan that did not happen is never a zero
# finding (D3).
scratch_unreadable() {
  unmeasured "the scratch file $(sane "$1") ($2) could not be opened for reading," \
    "so the loop that reads it would run ZERO times, UNDERCOUNTING the blast" \
    "radius. A scan that did not happen is UNMEASURED, never a zero finding" \
    "(#3650 D3)."
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

# --- the scratch location is VALIDATED BEFORE ANYTHING IS CREATED (#3650 R4) --
#
# THE ORDER IS THE POINT. `mktemp -d` honours TMPDIR, so creating first and
# checking afterwards HAS ALREADY WRITTEN A DIRECTORY IN THE CHECKOUT in exactly
# the case the check exists to prevent — and D5's argument for this tool is that
# a verifier with a side effect is a worse verifier, so the contract is
# load-bearing rather than tidy. The REQUESTED location is canonicalized and
# refused BEFORE `mktemp` runs, so nothing is created on the reject path.
#
# BOTH REPOSITORY ROOTS ARE CHECKED, NOT JUST THE WORK TREE. Every lane on this
# fleet is a `git worktree`, so `--git-common-dir` is ALWAYS outside the lane's
# toplevel (measured on this lane: toplevel /data/lanes/lane-3650, common dir
# /data/lanes/repo/.git) — a TMPDIR under the shared git directory writes into
# state EVERY lane on the box shares, and a toplevel-only check cannot see it.
# "Inside either" is in-repository.
#
# UNRESOLVABLE IS `UNMEASURED`, NEVER A PASS: if the work tree root or the common
# dir cannot be resolved, this does NOT fall back to checking whichever one it
# got. A check that silently narrows its own subject is the permissive-branch
# shape #3650 exists to refuse.
#
# Deliberately NOT relocated silently to /tmp: a tool that quietly ignores the
# environment it was handed is the same defect one layer down, and an UNMEASURED
# naming TMPDIR is actionable.
#
# INTERRUPTION residual, RESTATED for this ordering: the reject path creates
# nothing, so a SIGKILL can no longer strand a scratch dir INSIDE the repository
# (the earlier create-then-check ordering could). What a SIGKILL between
# `mktemp` and exit can still strand is the already-validated, provably
# out-of-repo scratch dir, left for the OS to reap — an unignorable signal cannot
# be cleaned up from inside the process.
#
# `2>/dev/null` throughout: an unredirected tool writing to stderr would emit a
# line with no `BASE-STALENESS: ` prefix, breaking D2a's anchor.

# The work-tree probe comes FIRST: the two roots below are what the scratch check
# compares against, so it cannot be deferred to the input-resolution block.
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  unmeasured "not inside a git work tree (cwd $(sane "$(pwd)"))"
fi
# canon <dir> — canonicalize with `cd`+`pwd -P` (the convention in
# scripts/flow/finalize-cleanup.sh; no `realpath` dependency). EMPTY on failure,
# and every caller treats empty as UNMEASURED rather than as "outside".
canon() { (cd "$1" 2>/dev/null && pwd -P) || true; }

repo_canon=$(canon "$(git rev-parse --show-toplevel 2>/dev/null || true)")
if [ -z "$repo_canon" ]; then
  unmeasured "the work tree root (git rev-parse --show-toplevel) could not be resolved," \
    "so the scratch location cannot be proven outside the repository. That is" \
    "UNMEASURED, never a pass (#3650 D3)."
fi
# `--git-common-dir` may be RELATIVE (a plain `.git`), hence the canonicalize.
common_canon=$(canon "$(git rev-parse --git-common-dir 2>/dev/null || true)")
if [ -z "$common_canon" ]; then
  unmeasured "the git common directory (git rev-parse --git-common-dir) could not be" \
    "resolved, so the scratch location cannot be proven outside the shared git dir." \
    "That is UNMEASURED, never a pass (#3650 D3)."
fi

# --- THE NO-FETCH SCOPE IS DECLARED, NOT DETECTED (#3650, owner ruling) ------
#
# `GIT_NO_LAZY_FETCH=1` is exported at the top of this file and is honoured from
# git 2.36, so D5's "never fetches, never writes" holds for GIT >= 2.36 ON A
# NON-PROMISOR CLONE. Outside that combination — an older git in a
# partial/promisor clone — the object reads below can still fetch over the network
# and write packfiles, and THIS SCRIPT DOES NOT DETECT IT. That is stated here, in
# the header, in the line below, in design.md D5 and in the spec; it is a
# precondition, not an assurance.
#
# Measured on this fleet: git 2.43.0; `git config --get remote.origin.promisor`
# and `git config --get extensions.partialclone` both exit 1 (key absent); no
# `objects/info/promisor` and no `objects/pack/*.promisor` under
# `--git-common-dir`. NEITHER adverse condition occurs here, which is why round
# 5's detection (a `git --version` parse plus a three-valued promisor probe, ~60
# lines) was DELETED on the ruling: a mechanism for an unreachable state. A
# version FLOOR belongs to #3680, as a precondition of ENFORCEMENT.
printf '%s no-fetch-scope DECLARED-NOT-VERIFIED git>=2.36 on a non-promisor clone (D5: this script never fetches and never writes WITHIN that scope; outside it the guarantee is neither held nor detected — #3650)\n' "$P"

# in_repository <canonical-dir> — NAMES the repository root the dir lies inside
# (work tree OR git common dir); prints nothing when it is outside both. The dir
# itself counts as inside (`"$root"/*` matches `"$root/"`).
in_repository() {
  case "$1/" in
    "$repo_canon"/*) printf 'the work tree %s' "$repo_canon" ;;
    "$common_canon"/*) printf 'the git common directory %s' "$common_canon" ;;
  esac
}

tmpdir_req=${TMPDIR:-/tmp}
tmpdir_canon=$(canon "$tmpdir_req")
if [ -z "$tmpdir_canon" ]; then
  unmeasured "the requested scratch root TMPDIR=$(sane "$tmpdir_req") could not be" \
    "resolved (absent, unreadable, or not a directory). Re-run with a TMPDIR that" \
    "exists outside the work tree."
fi
tmpdir_enclosing=$(in_repository "$tmpdir_canon")
if [ -n "$tmpdir_enclosing" ]; then
  unmeasured "the requested scratch root resolves INSIDE the repository:" \
    "$(sane "$tmpdir_canon") is under $(sane "$tmpdir_enclosing"). TMPDIR points into" \
    "the checkout and this script writes nothing in the repo (#3650 D5) — NOTHING WAS" \
    "CREATED. Re-run with a TMPDIR outside the work tree and outside the git dir."
fi

# Scratch space for the NUL-separated git output (see the -z note below), under
# the now-validated root.
if ! TMPD=$(mktemp -d "$tmpdir_canon/base-staleness.XXXXXX" 2>/dev/null); then
  unmeasured "could not create a scratch dir under $(sane "$tmpdir_canon")"
fi
trap 'rm -rf "$TMPD" 2>/dev/null' EXIT

# REVALIDATE WHAT WAS ACTUALLY CREATED. The pre-check answers about the path as
# it resolved a moment ago; a symlink swapped between check and create, or a
# `mktemp` resolving somewhere unexpected, lands the real dir elsewhere. On a
# post-create failure the dir is REMOVED before routing to UNMEASURED, so the
# no-write contract holds on this path too.
tmpd_canon=$(canon "$TMPD")
if [ -z "$tmpd_canon" ]; then
  rm -rf "$TMPD" 2>/dev/null
  unmeasured "the scratch dir $(sane "$TMPD") could not be canonicalized"
fi
tmpd_enclosing=$(in_repository "$tmpd_canon")
if [ -n "$tmpd_enclosing" ]; then
  rm -rf "$TMPD" 2>/dev/null
  unmeasured "the CREATED scratch dir resolves INSIDE the repository:" \
    "$(sane "$tmpd_canon") is under $(sane "$tmpd_enclosing") — the scratch root" \
    "resolved into the checkout between the pre-create check and the create. It has" \
    "been removed; this script writes nothing in the repo (#3650 D5)."
fi

# --- resolve the remaining inputs, each failure being UNMEASURED (never a zero)
if ! subject_sha=$(git rev-parse --verify --quiet "$rev^{commit}" 2>/dev/null) ||
  [ -z "$subject_sha" ]; then
  unmeasured "the subject rev '$(sane "$rev")' does not resolve to a commit"
fi
if ! main_sha=$(git rev-parse --verify --quiet "$BASE_REF^{commit}" 2>/dev/null) ||
  [ -z "$main_sha" ]; then
  unmeasured "$BASE_REF is absent — this script does NOT fetch (#3650 D5); run" \
    "'git fetch origin main' and re-run. An absent base ref is unmeasurable, not clean."
fi
# THE ONE GIT CALL WHOSE FAILURE IS NOT `UNMEASURED`, and the exception is
# declared in the header's EXIT CODES section, in spec.md and in design.md D5 —
# not only here (#3650 review B3): a contract stating an absolute the code
# deliberately violates is the defect, whichever side is "right".
# NOT `UNMEASURED-...`: that token is the verdict word, and injecting it into a
# fully MEASURED run would make a slice-2 consumer grepping for `UNMEASURED`
# false-positive. Keep the verdict vocabulary single-purpose. This field feeds
# neither N nor M, so its absence costs a reader one informational value and
# escalating it would red the tool on correct input.
main_date=$(sane "$(git log -1 --format=%cI "$main_sha" 2>/dev/null)")
[ -n "$main_date" ] || main_date=DATE-UNAVAILABLE

# D4: the MERGE-BASE, never origin/main's tip. #3392 is the recorded cost.
if ! merge_base=$(git merge-base "$main_sha" "$subject_sha" 2>/dev/null) ||
  [ -z "$merge_base" ]; then
  unmeasured "no merge-base between $BASE_REF and '$(sane "$rev")' — unrelated histories" \
    "(or a shallow clone truncating the shared ancestry)."
fi

if ! behind=$(git rev-list --count "$merge_base..$main_sha" 2>/dev/null) ||
  [ -z "$behind" ]; then
  unmeasured "git rev-list --count $merge_base..$main_sha failed"
fi

# The diff's own paths. `-z` is MANDATORY (#3229): this repo tracks 40
# space-bearing paths under docs/, and a path-reading `git diff` without -z
# C-quotes them, so a newline-delimited read misclassifies and mis-compares.
#
# The NUL-separated output goes to a FILE and is read by REDIRECTION, never
# through `$( )` — command substitution DISCARDS NUL bytes, which would silently
# collapse every path into one record and defeat `-z` entirely. The file lives in
# TMPDIR: this script never writes in the repository.
#
# *** THESE TWO PATH SOURCES MUST BE RENAME-SYMMETRIC AND ROOT-RELATIVE ON BOTH
#     SIDES, OR THE `M = 0` BRANCH FAILS OPEN. *** Measured (git 2.43.0, a
# `git mv src/foo.rs src/foo_renamed.rs` plus an edit):
#   * PORCELAIN (`git diff`, this call) HONOURS `diff.renames`, whose git default
#     has been TRUE since 2.9, and emitted the DESTINATION ONLY. Unpinned, a PR
#     that renames a file — routine here, the campsite rule makes splits normal —
#     loses the OLD path from DIFF_PATHS, so a commit behind that edited the old
#     path matches NEITHER half of the blast radius and the scan reports
#     `blast-radius 0 RECOGNISED` while the merge composes content nothing tested.
#   * PORCELAIN also honours `diff.relative`, and this is the LIVE hazard because
#     it is a config the INVOKER controls: with it set and cwd in a subdirectory
#     the same call emitted `foo_renamed.rs` with NO `src/` prefix, which can
#     never equal the root-relative `src/foo.rs` the commit side reports. That
#     makes `M` a function of the invoker'"'"'s cwd, so `--no-relative` is required
#     rather than defensive. BOTH are therefore pinned off HERE.
#   * PLUMBING (`git diff-tree`, the commit scan below) does NOT honour
#     `diff.renames` even when it is FORCED ON: measured, `-c diff.renames=true`
#     still emitted BOTH paths. Rename detection there needs an explicit `-M`.
#     So the plumbing call needs no pin, and this comment deliberately does NOT
#     claim the config reaches it — that claim would be false and would rot.
#     THE PLUMBING-SIDE RISK RUNS THE OTHER WAY: adding `-M` to `diff-tree` to
#     "improve" the commit scan would reintroduce the asymmetry from the opposite
#     direction (destination-only on the commit side). DO NOT ADD `-M` THERE.
# These two are entry 3 of the AMBIENT GIT STATE pin list at the top of this
# file; that block is where the SET of pinned ambient inputs is visible in one
# place (#3650 review R6 F1). They stay pinned HERE, per-invocation, because the
# plumbing side must NOT inherit them.
if ! git -c diff.renames=false -c diff.relative=false \
  diff --name-only -z "$merge_base...$subject_sha" >"$TMPD/diff-paths" 2>/dev/null; then
  unmeasured "git diff --name-only -z $merge_base...$subject_sha failed"
fi
DIFF_PATHS=()
diff_path_count=0
if ! { exec 3<"$TMPD/diff-paths"; } 2>/dev/null; then
  scratch_unreadable "$TMPD/diff-paths" "the paths this diff itself touches"
fi
while IFS= read -r -d '' p; do
  [ -n "$p" ] || continue
  DIFF_PATHS+=("$p")
  diff_path_count=$((diff_path_count + 1))
done <&3
exec 3<&-

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
# One rev-list, then one diff-tree per commit behind. Cost, quoted from D9 with
# BOTH figures AND their pin, because this comment previously read "~1.5s warm on
# the 107-commit case" and cited D9 for a number D9 contradicts: the ~1.5 s was
# COLD (and was measured against the `show`-per-commit shape this call replaced),
# while WARM is 0.43 s, re-measured 2026-08-31 on this lane against a base 110
# commits behind. The pin for the 107-commit base is subject 4bc6b913a,
# origin/main b1e8598a2 — the same pin the gate-global selectivity measurement
# above quotes. A bare number here rots the moment it moves, which is why both
# figures and the pin are named rather than one overwriting the other.
# A pathological N is reported and scanned, never silently
# truncated — a truncated scan would have to be an UNMEASURED, so truncating
# would trade a slow answer for a fail-closed one.
# Written to a FILE and read by redirection, not iterated as an unquoted
# expansion: the header claims shellcheck-clean, and `for c in $commits` was the
# one word-splitting expansion in the file.
: >"$TMPD/behind-commits"
if [ "$behind" -gt 0 ]; then
  if ! git rev-list "$merge_base..$main_sha" >"$TMPD/behind-commits" 2>/dev/null; then
    unmeasured "git rev-list $merge_base..$main_sha failed"
  fi
fi

# How many matched commits are LISTED individually; the rest are summarised. One
# named constant, referenced twice — never a repeated magic literal. The reported
# COUNT is never truncated, only the listing.
MATCHED_LIST_LIMIT=20

m=0
matched_lines=""
if ! { exec 3<"$TMPD/behind-commits"; } 2>/dev/null; then
  scratch_unreadable "$TMPD/behind-commits" "the commits behind the merge-base"
fi
while IFS= read -r c; do
  [ -n "$c" ] || continue
  # `-m --first-parent` so a MERGE commit reports its change against its first
  # parent instead of reporting NOTHING; `--root` so a root commit is not
  # silently empty either. Either silence would understate M.
  # NO `-M` here, deliberately — see the rename-symmetry note on the porcelain
  # call above. `diff-tree` does not rename-detect without it, which is exactly
  # the behaviour the diff side is pinned to match.
  if ! git diff-tree -r -z --no-commit-id --name-only -m --first-parent --root "$c" \
    >"$TMPD/commit-paths" 2>/dev/null; then
    unmeasured "git diff-tree failed on commit $c — the scan is INCOMPLETE, so it is" \
      "unmeasurable rather than partially reported (#3650 D9)."
  fi
  hit=""
  why=""
  # fd 4, not 3: fd 3 is the OUTER loop's own open scratch read.
  if ! { exec 4<"$TMPD/commit-paths"; } 2>/dev/null; then
    scratch_unreadable "$TMPD/commit-paths" "the paths of commit $c"
  fi
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
  done <&4
  exec 4<&-
  if [ -n "$hit" ]; then
    m=$((m + 1))
    # SANITIZED HERE, at capture (D2b). `matched_lines` is newline-delimited, so a
    # path containing a newline would both break the anchor on output AND split
    # into two bogus records on the way in.
    # `${c:0:9}` rather than `git rev-parse --short=9` (#3650 review B3): that
    # subprocess was UNCHECKED and its failure is SWALLOWED by command
    # substitution, so a failing git here produced a record with an EMPTY sha.
    # Bash truncation cannot fail, needs no subprocess in a loop that runs once
    # per matched commit, and `$c` is already a full sha `git rev-list` printed.
    matched_lines="$matched_lines${c:0:9} $why $(sane "$hit")
"
  fi
done <&3
exec 3<&-

# --- report ----------------------------------------------------------------
printf '%s subject %s (%s)\n' "$P" "$(sane "$rev")" "$subject_sha"
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
    if [ "$n" -le "$MATCHED_LIST_LIMIT" ]; then
      printf '%s matched %s\n' "$P" "$line"
    fi
  done <<EOF
$matched_lines
EOF
  if [ "$n" -gt "$MATCHED_LIST_LIMIT" ]; then
    printf '%s matched (+%s further staling commits scanned but not listed)\n' \
      "$P" "$((n - MATCHED_LIST_LIMIT))"
  fi
fi
print_non_exhaustive
# D2c again: one `verdict ` line, one closed-set token, prose on `verdict-detail`.
if [ "$m" -gt 0 ]; then
  printf '%s verdict STALE-RECOGNISED\n' "$P"
  printf '%s verdict-detail %s of the %s commits behind touch this diff'"'"'s blast radius.\n' \
    "$P" "$m" "$behind"
  printf '%s verdict-detail Advisory only in #3650 slice 1: no verdict changes.\n' "$P"
  exit 4
fi
printf '%s verdict NO-STALENESS-RECOGNISED\n' "$P"
printf '%s verdict-detail a SCAN RESULT, not a state of the world, and not a certification.\n' "$P"
printf '%s verdict-detail See the NON-EXHAUSTIVE lines above (#3650 slice 1).\n' "$P"
exit 0
