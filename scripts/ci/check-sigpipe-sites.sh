#!/usr/bin/env bash
# check-sigpipe-sites.sh — THE CLASS RATCHET for the piped-builtin-writer (EPIPE) shape,
# across every git-tracked scripts/**/*.sh. Issue #4061, AC4.
#
# THE DEFECT THIS CLASS CARRIES
# -----------------------------
#   line=$(printf '%s\n' "$text" | grep -m1 "^$k: ")
#
# `grep -m1` (and `head -N`, and `sed -n '…;q'`) EXITS ON ITS FIRST MATCH and closes the read
# end while bash's BUILTIN `printf` may still be writing. bash does not die on SIGPIPE the way
# an external command does — it NARRATES the failed write on stderr — so an unanchored
# `printf: write error: Broken pipe` lands in the middle of a verdict a caller is reading. Under
# `set -o pipefail` it is worse than cosmetic: a SUCCESSFUL match can yield status 141, so a
# trailing `|| return 0` fires on a matched read and the field value is silently dropped. The fix
# is always the same shape: `reader <<<"$text"`, which bash implements with a temp file, so no
# writer is left to take EPIPE at all.
#
# It is LOAD-DEPENDENT — it fires under gate load and not on an idle box — so a green behavioural
# run is not evidence of a fix, and a behavioural test cannot pin it (lead ruling on #4061).
# STRUCTURAL ABSENCE IS THE PROOF. This is the class-wide half of that proof: #3803's
# scripts/tests/test_gate_liveness_no_sigpipe.sh asserts ZERO sites in the one file whose verdicts
# the merge gate consumes; this ratchet asserts NO NEW sites ANYWHERE under scripts/**.
#
# WHY A RATCHET AND NOT A HARD ZERO. Measured on this tree: 120 of 201 git-tracked
# scripts/**/*.sh files contain at least one line matching the rule, 2715 matches in all. Most are
# presumably the shared matcher's DECLARED FALSE-POSITIVE classes (a pipe inside a format string
# or a quoted argument, a pipe in a trailing comment, an unrelated later pipeline on the same
# line) — a count here is a count of SHAPE MATCHES and this guard makes NO claim about how many
# are real hazards. A hard whole-tree FAIL would therefore red on 120 files and demand mass
# restructuring of correct code. So the property asserted is "no WORSE than the committed
# baseline", exactly like the gate's file-size ratchet and the dep-duplicates baseline.
#
# A COUNT ALONE CANNOT SEE A SWAP, SO EACH ENTRY ALSO CARRIES AN AGGREGATE CONTENT HASH
# (roborev job 138, triaged BLOCKER). Remove one matching line and add a DIFFERENT hazardous one
# in the same file and the per-file COUNT is unchanged: a count-only ratchet PASSes and a new
# defect ships green — a false PASS against AC4's literal requirement that "a new
# `printf | grep -m1` / `| head` reds rather than shipping", and the exact asymmetry the shared
# matcher's own header invokes (#3229: a guard that hides defects while reading green is worse
# than no guard). So every entry is "<path> <count> <sha256>", where the digest is taken over
# that file's matched lines NORMALISED (trimmed, internal whitespace runs collapsed to one
# space, LINE NUMBERS DISCARDED) and SORTED (LC_ALL=C). Sorting the normalised multiset is what
# keeps the digest MOTION-PROOF: moving or reordering a matched line inside a file does not
# change the multiset, so the digest is unchanged; a SWAP changes the multiset, so it FAILs.
#
# THE ONE RESIDUAL, DECLARED: a NET DECREASE never FAILs (a ratchet that reds on an improvement
# is one agents route around), and the digest changes on ANY removal, so it cannot separate a
# pure removal from a removal-plus-addition. A change that deletes TWO sites from a file and
# adds ONE is therefore still tolerated. Closing that would need the baseline to carry the
# matched-line TEXT, which is the curated, motion-sensitive list this design deliberately
# refuses. The count-unchanged swap — the shape the defect was reported for — IS caught.
#
# THE BASELINE CARRIES NO LINE NUMBERS, AND THAT IS LOAD-BEARING. #4061's own body pinned
# `scripts/bootstrap-agent-machine.sh:3329`, which had drifted to `:5392` within two days. A
# line-numbered list is a curated list in disguise (lead ruling); PER-FILE COUNTS are stale-proof
# against pure code motion. The SUBJECT SET is likewise derived at run time from `git ls-files`
# and is never curated: a new script is a subject the moment it is added to the index. MEMBERSHIP
# comes from the INDEX and CONTENT from the WORKTREE, so a new site in an already-tracked script
# reds BEFORE it is ever committed, while an untracked scratch file is deliberately not a subject.
#
# THE OPERATIONAL COST, stated up front: because the baseline is a COUNT census over the whole
# tree, a REBASE OR MERGE OF main that adds matching lines in files you never touched moves the
# census and FAILs this guard until the baseline is regenerated. That happened on #4061's own
# branch (main gained 6 files and ~370 matches mid-flight, none of them in this PR's diff). The
# remedy is the documented one and is printed in the failure: check whether the named files are in
# your diff, and if not, regenerate as part of the rebase. Line numbers would make this WORSE, not
# better — any edit above a site moves it.
#
# THE RULE IS NOT IMPLEMENTED HERE. It lives ONCE in scripts/tests/lib/sigpipe-matcher.sh,
# together with its declared false positives and residuals, and is PINNED by #3803's 33 cases.
# This file must never carry a second copy of it (CLAUDE.md: a canonical form implemented twice
# diverges silently).
#
# EXIT STATUS
#   0  no increase (the ratchet holds)
#   1  INCREASE: an existing file grew, or a file absent from the baseline has >=1 match
#   2  usage error
#   3  REFUSED — the measurement could not be made (named cause + remedy printed). Fail-closed:
#      an unmeasurable run is never a pass. There is NO bypass env var and no opt-out; a
#      curated-list escape hatch could only buy a vacuous green.
#
# PREREQUISITES: git, awk, a SHA-256 digest tool (`sha256sum`, else `shasum -a 256` — macOS
# ships the latter), standard text tools. No cargo, no python3, no network, no datasets. It
# therefore NEVER SKIPs — and an ABSENT digest tool is a REFUSAL naming it, never a skip and
# never a silent fall back to comparing counts alone, which would quietly restore precisely the
# false PASS the digest exists to remove.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
MATCHER_LIB="$SCRIPT_DIR/../tests/lib/sigpipe-matcher.sh"
BASELINE="$SCRIPT_DIR/sigpipe-sites-baseline.txt"
REGEN_CMD="bash scripts/ci/check-sigpipe-sites.sh --regenerate"

# Non-vacuity floors (CLAUDE.md #3544 / the existing suite's case 11). A verdict must not be
# reachable from an enumeration that found nothing: "0 increases" over 0 subjects is not a
# measurement. Floors may only go DOWN with a stated reason.
SUBJECT_FLOOR=100        # measured 201 git-tracked scripts/**/*.sh on this tree
BASELINE_ENTRY_FLOOR=60  # measured 120 entries

# The relative-path shape a subject and a baseline path must both have. Closed by design.
PATH_RE='^scripts/[A-Za-z0-9._/-]+\.sh$'

MODE=check
case "${1:-}" in
  ""|--check)   MODE=check ;;
  --regenerate) MODE=regenerate ;;
  -h|--help)
    printf 'usage: check-sigpipe-sites.sh [ --check / --regenerate / --help ]\n'
    printf '  --check       (default) re-measure and compare against %s\n' "scripts/ci/sigpipe-sites-baseline.txt"
    printf '  --regenerate  rewrite that baseline from the current tree (the ONLY documented way\n'
    printf '                to tolerate an existing site)\n'
    exit 0 ;;
  *)
    printf 'check-sigpipe-sites.sh: unrecognised argument: %s\n' "$1" >&2
    printf '  usage: check-sigpipe-sites.sh [ --check / --regenerate / --help ]\n' >&2
    exit 2 ;;
esac

refuse() { # refuse <cause> <what> <remedy>
  printf 'SIGPIPE-SITES: REFUSING (reason: %s): %s\n' "$1" "$2"
  printf 'SIGPIPE-SITES: REMEDY: %s\n' "$3"
  printf 'SIGPIPE-SITES: verdict REFUSED\n'
  exit 3
}

# ---------------------------------------------------------------------------
# THE MATCHER: sourced, never re-implemented. Fail-closed — a missing library means nothing was
# scanned, which must never read as "no sites".
# ---------------------------------------------------------------------------
# shellcheck source=scripts/tests/lib/sigpipe-matcher.sh
if ! . "$MATCHER_LIB" 2>/dev/null || ! declare -F sigpipe_violations >/dev/null 2>&1; then
  refuse "no-matcher" \
    "the shared matcher library is absent, unreadable, or did not define sigpipe_violations ($MATCHER_LIB)" \
    "restore scripts/tests/lib/sigpipe-matcher.sh (#4061). Without it NOTHING was scanned."
fi

# ---------------------------------------------------------------------------
# THE DIGEST TOOL. Resolved once, fail-closed. NOT optional: without it the census could only
# compare counts, which is the reported defect. There is deliberately no count-only fallback.
# ---------------------------------------------------------------------------
DIGEST_CMD=""
if command -v sha256sum >/dev/null 2>&1; then
  DIGEST_CMD="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  DIGEST_CMD="shasum -a 256"
else
  refuse "no-sha256" \
    "neither \`sha256sum\` nor \`shasum\` is on PATH, so the per-file aggregate content hash cannot be computed — and a count-only comparison cannot see a SWAP (one site removed, one added), which is the whole reason the hash exists" \
    "install coreutils (\`sha256sum\`) or perl's \`shasum\`. This guard has no count-only mode, deliberately: a degraded mode that silently restores the false PASS is worse than the bug."
fi

command -v git >/dev/null 2>&1 || refuse "no-git" \
  "git is not on PATH, so the subject set cannot be derived at run time (and a curated path list is forbidden)" \
  "install git. This guard has no other prerequisite and never skips."
command -v awk >/dev/null 2>&1 || refuse "no-awk" \
  "awk is not on PATH and the shared matcher is an awk program" \
  "install awk (POSIX)."

# NORMALISE + SORT the matcher's output into the exact bytes the digest is taken over. The
# `<lineno>:` prefix is DROPPED on purpose (a line-numbered baseline rots on code motion —
# see the header), whitespace is trimmed and collapsed so a re-indent is not a "change", and
# the sort makes the material a SORTED MULTISET so intra-file motion is invisible while a swap
# is not. Lines that normalise to nothing are dropped: they carry no content to hash.
_norm_sorted() { # _norm_sorted <violations-file>  -> normalised, sorted lines on stdout
  awk '{ sub(/^[0-9]+:/, "", $0)
         sub(/^[[:space:]]+/, "", $0); sub(/[[:space:]]+$/, "", $0)
         gsub(/[[:space:]]+/, " ", $0)
         if (length($0) > 0) print }' "$1" | LC_ALL=C sort
}
# The hex digest of a file's bytes. `< file` so the tool never prints a name to strip.
_digest_of() { # _digest_of <file> -> 64 hex chars
  $DIGEST_CMD <"$1" | awk '{ print $1; exit }'
}
HASH_RE='^[0-9a-f]{64}$'

# ---------------------------------------------------------------------------
# MATCHER SELF-CHECK. A census of zero is indistinguishable from a matcher that matches nothing,
# so prove the matcher FIRES on the exact #4061 shape before trusting any count from it. The
# fixture is assembled from $PIPE so THIS FILE contains no matching line of its own.
# ---------------------------------------------------------------------------
_tmp=$(mktemp -d 2>/dev/null) || refuse "no-tmpdir" \
  "\`mktemp -d\` failed, so the matcher self-check fixture cannot be written" \
  "check TMPDIR=${TMPDIR:-/tmp}"
trap 'rm -rf "$_tmp"' EXIT
PIPE='|'
{
  printf '#!/usr/bin/env bash\n'
  printf 'line=$(printf %s "$text" %s grep -m1 "^$k: ") || return 0\n' "'%s\\n'" "$PIPE"
} >"$_tmp/selfcheck.sh"
_sc=$(sigpipe_violations "$_tmp/selfcheck.sh" | grep -c . || true)
[ "${_sc:-0}" -eq 1 ] || refuse "matcher-inert" \
  "the shared matcher reported ${_sc:-0} site(s) in a fixture containing exactly ONE known #4061 site, so no count it produces can be trusted" \
  "fix scripts/tests/lib/sigpipe-matcher.sh and re-run scripts/tests/test_gate_liveness_no_sigpipe.sh (33 cases pin it)"
printf 'SIGPIPE-SITES: matcher SELF-CHECK OK (1 known site found in 1-site fixture)\n'

# ---------------------------------------------------------------------------
# THE SUBJECT CENSUS — derived at run time from the git index, never curated. `git ls-files` and
# not `find`, so untracked scratch files are not subjects.
# ---------------------------------------------------------------------------
cd "$REPO_ROOT" 2>/dev/null || refuse "no-repo-root" \
  "cannot cd to the repository root inferred from this script's location ($REPO_ROOT)" \
  "run this script from inside a checkout"
git rev-parse --git-dir >/dev/null 2>&1 || refuse "not-a-git-repo" \
  "$REPO_ROOT is not a git repository, so the subject set cannot be derived from the index" \
  "run inside a checkout ($REGEN_CMD documents the regeneration path)"

if ! git ls-files -z 'scripts/*.sh' 'scripts/**/*.sh' >"$_tmp/subjects.z" 2>"$_tmp/ls.err"; then
  refuse "git-failed" \
    "\`git ls-files\` exited non-zero: $(tr '\n' ' ' <"$_tmp/ls.err")" \
    "repair the checkout; an unmeasured subject set is never a pass"
fi

# BASH 3.2 THROUGHOUT (macOS is a first-class gate host and ships 3.2): no associative arrays
# anywhere in this file. Every set lives in a sorted temp FILE and the comparison is one awk pass,
# which is also why the output is deterministically ordered.
: >"$_tmp/subjects"
N_SUBJECTS=0
while IFS= read -r -d '' f; do
  [[ "$f" =~ $PATH_RE ]] || refuse "subject-path-shape" \
    "git listed a subject whose path is not the recognised scripts/**/*.sh shape: $f" \
    "either the path grammar in this guard needs widening deliberately, or that file does not belong under scripts/"
  [ -r "$f" ] || refuse "unreadable-subject" \
    "$f is tracked but not readable, so its site count is UNKNOWN, not zero" \
    "restore file permissions, or remove the file from the index"
  printf '%s\n' "$f" >>"$_tmp/subjects"
  N_SUBJECTS=$((N_SUBJECTS + 1))
done <"$_tmp/subjects.z"
printf 'SIGPIPE-SITES: subjects ENUMERATED %d git-tracked scripts/**/*.sh file(s) (floor %d)\n' \
  "$N_SUBJECTS" "$SUBJECT_FLOOR"
[ "$N_SUBJECTS" -ge "$SUBJECT_FLOOR" ] || refuse "subject-floor" \
  "only $N_SUBJECTS subject(s) enumerated, floor is $SUBJECT_FLOOR — a clean verdict over this set would measure nothing" \
  "check that the git index is populated and the pathspecs still match; lower the floor only with a stated reason"

# LINE-BASED reads from here on, and that is SAFE ONLY BECAUSE the enumeration above already
# REFUSED anything outside PATH_RE — which admits no whitespace, so no subject path can contain a
# space or a newline. The census file's "path count" records are likewise whitespace-split by awk
# for the same reason. Widening PATH_RE means revisiting both.
: >"$_tmp/now"
CUR_FILES=0
CUR_SITES=0
while IFS= read -r f; do
  sigpipe_violations "$f" >"$_tmp/v.census" 2>/dev/null
  n=$(grep -c . "$_tmp/v.census" || true)
  [ -n "$n" ] || refuse "count-failed" \
    "the matcher produced no count for $f" \
    "re-run; if it persists, fix scripts/tests/lib/sigpipe-matcher.sh"
  if [ "$n" -gt 0 ]; then
    _norm_sorted "$_tmp/v.census" >"$_tmp/v.census.norm"
    h=$(_digest_of "$_tmp/v.census.norm")
    [[ "$h" =~ $HASH_RE ]] || refuse "digest-failed" \
      "\`$DIGEST_CMD\` produced '$h' rather than a 64-character hex digest for $f, so that file's matched-line SET is UNKNOWN — and an unknown set must never read as an unchanged one" \
      "check that $DIGEST_CMD works on this host; an unmeasured comparison is never a pass"
    printf '%s %s %s\n' "$f" "$n" "$h" >>"$_tmp/now"
    CUR_FILES=$((CUR_FILES + 1))
    CUR_SITES=$((CUR_SITES + n))
  fi
done <"$_tmp/subjects"
printf 'SIGPIPE-SITES: MEASURED %d file(s) with at least one SHAPE MATCH, %d match(es) in total\n' \
  "$CUR_FILES" "$CUR_SITES"
printf 'SIGPIPE-SITES: (a SHAPE MATCH is not a confirmed hazard — see the DECLARED SCOPE block)\n'

# ---------------------------------------------------------------------------
# --regenerate: the ONE documented way an existing site is tolerated.
# ---------------------------------------------------------------------------
if [ "$MODE" = regenerate ]; then
  {
    printf '# scripts/ci/sigpipe-sites-baseline.txt — THE PIPED-BUILTIN-WRITER (EPIPE) SITE RATCHET\n'
    printf '# BASELINE (issue #4061, AC4). GENERATED FILE — do not hand-edit; regenerate with ONE\n'
    printf '# command:\n#\n#   %s\n#\n' "$REGEN_CMD"
    printf '# GRAMMAR (closed — anything else is REFUSED, not skipped):\n'
    printf '#   "<relative-path> <count> <sha256>"  one per git-tracked scripts/**/*.sh file with\n'
    printf '#                              >=1 match; count >= 1; sha256 = 64 lowercase hex chars;\n'
    printf '#                              sorted (LC_ALL=C), no duplicate paths\n'
    printf '#   "#..." comment / blank     ignored\n'
    printf '# THE THIRD FIELD is an AGGREGATE DIGEST OVER MATCHED-LINE TEXT, and carries NO LINE\n'
    printf '# NUMBERS: each matched line is normalised (trimmed, internal whitespace runs collapsed\n'
    printf '# to one space, the matcher\x27s "<lineno>:" prefix discarded), the normalised lines are\n'
    printf '# SORTED (LC_ALL=C), and sha256 is taken over that concatenation. Sorting is what keeps\n'
    printf '# it MOTION-PROOF — moving or reordering a matched line does not change the multiset —\n'
    printf '# while a SWAP (one site removed, one added, count unchanged) DOES change it and FAILs.\n'
    printf '# A count alone could not see that swap; that false PASS is why the field exists.\n'
    printf '# NO LINE NUMBERS, deliberately: #4061 pinned bootstrap-agent-machine.sh:3329 and it had\n'
    printf '# drifted to :5392 within two days. Per-file counts are stale-proof against code motion,\n'
    printf '# and a line-numbered list is a curated list in disguise (lead ruling on #4061).\n'
    printf '# A COUNT IS A COUNT OF SHAPE MATCHES, NOT OF CONFIRMED HAZARDS. The shared matcher\n'
    printf '# (scripts/tests/lib/sigpipe-matcher.sh) is deliberately BROAD and REPORTS six declared\n'
    printf '# classes of correct code; see its header and the run-time DECLARED SCOPE block.\n'
    printf '# THE RATCHET: an increase in a listed file, a CHANGED digest at an unchanged count, or\n'
    printf '# ANY match in a file not listed here, FAILs.\n'
    printf '# A DECREASE never fails — re-run the command above and commit a tighter baseline. That\n'
    printf '# is also the ONE declared residual: the digest changes on any removal, so a net decrease\n'
    printf '# that also ADDS a line is tolerated.\n'
    # Sorted from the census FILE, never `printf ... | sort`: `sort` reads to EOF so there is no
    # EPIPE hazard here, but the shape is one this guard REPORTS, and a guard that must list
    # itself in its own baseline is a guard nobody believes.
    LC_ALL=C sort "$_tmp/now"
  } >"$BASELINE.tmp.$$" || refuse "baseline-write-failed" \
    "could not write $BASELINE.tmp.$$" "check write permissions on scripts/ci/"
  mv -- "$BASELINE.tmp.$$" "$BASELINE" || refuse "baseline-write-failed" \
    "could not move the regenerated baseline into place" "check write permissions on scripts/ci/"
  printf 'SIGPIPE-SITES: REGENERATED %s (%d file(s), %d match(es))\n' "$BASELINE" "$CUR_FILES" "$CUR_SITES"
  printf 'SIGPIPE-SITES: verdict REGENERATED\n'
  exit 0
fi

# ---------------------------------------------------------------------------
# THE BASELINE PARSER — a CLOSED grammar. Every line must match a recognised shape; anything
# unrecognised is REFUSED, never skipped (an ungrammatical baseline silently half-read is how a
# ratchet stops ratcheting).
# ---------------------------------------------------------------------------
[ -e "$BASELINE" ] || refuse "no-baseline" \
  "the baseline $BASELINE does not exist, so no site can be tolerated and nothing can be compared" \
  "$REGEN_CMD"
[ -r "$BASELINE" ] || refuse "unreadable-baseline" \
  "the baseline $BASELINE exists but is not readable" \
  "fix its permissions"

: >"$_tmp/base"
: >"$_tmp/basepaths"
B_ENTRIES=0
B_SITES=0
_lineno=0
while IFS= read -r bline || [ -n "$bline" ]; do
  _lineno=$((_lineno + 1))
  case "$bline" in
    ""|\#*) continue ;;
  esac
  if [[ ! "$bline" =~ ^([^[:space:]]+)[[:space:]]+([0-9]+)[[:space:]]+([^[:space:]]+)$ ]]; then
    refuse "baseline-grammar" \
      "$BASELINE line $_lineno is not a recognised shape: $bline" \
      "$REGEN_CMD (the grammar is '<relative-path> <count> <sha256>', a '#' comment, or a blank line; a two-field record is the pre-#4061 count-only form, which cannot see a SWAP and is therefore no longer accepted)"
  fi
  bpath=${BASH_REMATCH[1]}
  bcount=${BASH_REMATCH[2]}
  bhash=${BASH_REMATCH[3]}
  [[ "$bhash" =~ $HASH_RE ]] || refuse "baseline-grammar" \
    "$BASELINE line $_lineno records '$bhash' for $bpath, which is not 64 lowercase hex characters — a digest this reader cannot compare is not a digest" \
    "$REGEN_CMD"
  [[ "$bpath" =~ $PATH_RE ]] || refuse "baseline-grammar" \
    "$BASELINE line $_lineno names a path that is not the recognised scripts/**/*.sh shape: $bpath" \
    "$REGEN_CMD"
  [ "$bcount" -ge 1 ] || refuse "baseline-grammar" \
    "$BASELINE line $_lineno records a count of $bcount for $bpath — the baseline lists only files WITH sites" \
    "$REGEN_CMD"
  # Duplicate detection without an associative array (bash 3.2): the accumulating file is the
  # set. A fixed-string, whole-line-anchored grep, so a path is never read as a pattern.
  if grep -qxF -- "$bpath" "$_tmp/basepaths" 2>/dev/null; then
    refuse "baseline-duplicate" \
      "$BASELINE names $bpath twice (line $_lineno), so which count binds is undefined" \
      "$REGEN_CMD"
  fi
  printf '%s\n' "$bpath" >>"$_tmp/basepaths"
  printf '%s %s %s\n' "$bpath" "$bcount" "$bhash" >>"$_tmp/base"
  B_ENTRIES=$((B_ENTRIES + 1))
  B_SITES=$((B_SITES + bcount))
done <"$BASELINE"

printf 'SIGPIPE-SITES: baseline PARSED %d entr%s, %d recorded match(es) (floor %d entries)\n' \
  "$B_ENTRIES" "$([ "$B_ENTRIES" -eq 1 ] && printf 'y' || printf 'ies')" "$B_SITES" "$BASELINE_ENTRY_FLOOR"
[ "$B_ENTRIES" -ge "$BASELINE_ENTRY_FLOOR" ] || refuse "baseline-floor" \
  "the baseline parsed only $B_ENTRIES entr(y/ies), floor is $BASELINE_ENTRY_FLOOR — it has been truncated or is not the shipped census" \
  "$REGEN_CMD, or lower the floor with a stated reason if the tree genuinely improved that much"

# ---------------------------------------------------------------------------
# THE COMPARISON. Two FAILing conditions and two non-failing observations, each named.
# ---------------------------------------------------------------------------
# ONE awk pass over the three sets (baseline, census, subjects), emitting TAGGED records that
# bash then renders. awk's own arrays are used here — the bash-3.2 constraint is about bash.
# Output is sorted, so a diagnostic is deterministic run to run.
if ! awk -v base="$_tmp/base" -v now="$_tmp/now" -v subj="$_tmp/subjects" '
    FILENAME == base { b[$1] = $2; bh[$1] = $3; next }
    FILENAME == now  { n[$1] = $2; nh[$1] = $3; next }
    FILENAME == subj { s[$0] = 1;  next }
    END {
      for (p in n) {
        if (!(p in b))            { print "FAIL-NEW " p " " n[p]; continue }
        if (n[p] + 0 > b[p] + 0)  { print "FAIL-INC " p " " n[p] " " b[p]; continue }
        # SAME (or fewer) matches but a DIFFERENT matched-line SET. At an UNCHANGED count that is
        # a SWAP and FAILs. At a LOWER count it is the declared residual: a decrease never fails,
        # and the digest always changes on a removal, so the two are indistinguishable here.
        if (n[p] + 0 == b[p] + 0 && nh[p] != bh[p]) { print "FAIL-SWAP " p " " n[p] " " nh[p] " " bh[p] }
      }
      for (p in b) {
        if (!(p in s))            { print "INFO-GONE " p; continue }
        cur = (p in n) ? n[p] + 0 : 0
        if (cur < b[p] + 0)       { print "INFO-IMPROVED " p " " cur " " b[p] }
      }
    }
  ' "$_tmp/base" "$_tmp/now" "$_tmp/subjects" >"$_tmp/cmp.unsorted" 2>"$_tmp/cmp.err"; then
  refuse "comparison-failed" \
    "the awk comparison of census against baseline exited non-zero: $(tr '\n' ' ' <"$_tmp/cmp.err")" \
    "re-run; an unmeasured comparison is never a pass"
fi
LC_ALL=C sort "$_tmp/cmp.unsorted" >"$_tmp/cmp"

# A DIGEST-ONLY FAILURE WOULD BE UNACTIONABLE, so name the lines that moved. The baseline stores
# no TEXT, so the removed line cannot be recovered FROM the baseline — but it can be recovered
# from the file at HEAD, and when HEAD's normalised matched-line set hashes to exactly the
# baseline's digest the difference reported is EXACT rather than indicative. That is checked, not
# assumed; when it does not hold the reader is TOLD the reference state is unavailable and gets
# the current set in full, never a bare digest mismatch.
_swap_report() { # _swap_report <path> <baseline-hash>
  local sp="$1" sbh="$2" ref_ok=0 hh=""
  sigpipe_violations "$sp" >"$_tmp/s.now" 2>/dev/null
  _norm_sorted "$_tmp/s.now" >"$_tmp/s.now.norm"
  if command -v comm >/dev/null 2>&1 && git show "HEAD:$sp" >"$_tmp/s.head.src" 2>/dev/null; then
    sigpipe_violations "$_tmp/s.head.src" >"$_tmp/s.head" 2>/dev/null
    _norm_sorted "$_tmp/s.head" >"$_tmp/s.head.norm"
    hh=$(_digest_of "$_tmp/s.head.norm")
    if [ "$hh" = "$sbh" ]; then ref_ok=1; fi
  fi
  if [ "$ref_ok" -eq 1 ]; then
    printf '    the baseline digest equals this file AT HEAD, so this difference is EXACT:\n' >>"$_tmp/msg.fail"
    LC_ALL=C comm -13 "$_tmp/s.head.norm" "$_tmp/s.now.norm" >"$_tmp/s.added"
    LC_ALL=C comm -23 "$_tmp/s.head.norm" "$_tmp/s.now.norm" >"$_tmp/s.removed"
    while IFS= read -r sl; do
      [ -n "$sl" ] && printf '    ADDED:   %s\n' "$sl" >>"$_tmp/msg.fail"
    done <"$_tmp/s.added"
    while IFS= read -r sl; do
      [ -n "$sl" ] && printf '    REMOVED: %s\n' "$sl" >>"$_tmp/msg.fail"
    done <"$_tmp/s.removed"
  else
    printf '    the baseline records a DIGEST, not text, and this file at HEAD does not reproduce\n' >>"$_tmp/msg.fail"
    printf '    it (HEAD digest: %s), so the REMOVED line cannot be named. The current matched-line\n' "${hh:-unavailable}" >>"$_tmp/msg.fail"
    printf '    set in full follows; compare it against `git diff -- %s`.\n' "$sp" >>"$_tmp/msg.fail"
    while IFS= read -r sl; do
      [ -n "$sl" ] && printf '    NOW:     %s\n' "$sl" >>"$_tmp/msg.fail"
    done <"$_tmp/s.now.norm"
  fi
}

INCREASED=0
NEWFILES=0
SWAPPED=0
: >"$_tmp/msg.fail"
: >"$_tmp/msg.info"
while IFS= read -r rec; do
  [ -n "$rec" ] || continue
  tag=${rec%% *}
  rest=${rec#* }
  case "$tag" in
    FAIL-NEW)
      rp=${rest%% *}; rn=${rest##* }
      NEWFILES=$((NEWFILES + 1))
      printf 'NEW FILE WITH SITES: %s has %s shape match(es) and is not in the baseline\n' "$rp" "$rn" >>"$_tmp/msg.fail"
      sigpipe_violations "$rp" >"$_tmp/v.txt"
      while IFS= read -r v; do
        [ -n "$v" ] || continue
        printf '    %s:%s\n' "$rp" "$v" >>"$_tmp/msg.fail"
      done <"$_tmp/v.txt"
      ;;
    FAIL-INC)
      rp=${rest%% *}; rtail=${rest#* }; rn=${rtail%% *}; rb=${rtail##* }
      INCREASED=$((INCREASED + 1))
      printf 'INCREASE: %s has %s shape match(es), baseline records %s\n' "$rp" "$rn" "$rb" >>"$_tmp/msg.fail"
      sigpipe_violations "$rp" >"$_tmp/v.txt"
      while IFS= read -r v; do
        [ -n "$v" ] || continue
        printf '    %s:%s\n' "$rp" "$v" >>"$_tmp/msg.fail"
      done <"$_tmp/v.txt"
      ;;
    FAIL-SWAP)
      rp=${rest%% *}; rtail=${rest#* }; rn=${rtail%% *}
      rtail2=${rtail#* }; rhn=${rtail2%% *}; rhb=${rtail2##* }
      SWAPPED=$((SWAPPED + 1))
      printf 'SWAP: %s still has %s shape match(es) — the COUNT is unchanged but the matched-line SET is NOT (digest %s, baseline %s)\n' \
        "$rp" "$rn" "${rhn:0:12}" "${rhb:0:12}" >>"$_tmp/msg.fail"
      _swap_report "$rp" "$rhb"
      ;;
    INFO-GONE)
      printf 'BASELINE FILE GONE: %s is in the baseline but is no longer a tracked subject (fixed, renamed or deleted)\n' "$rest" >>"$_tmp/msg.info"
      ;;
    INFO-IMPROVED)
      rp=${rest%% *}; rtail=${rest#* }; rn=${rtail%% *}; rb=${rtail##* }
      printf 'IMPROVED: %s has %s shape match(es), baseline records %s\n' "$rp" "$rn" "$rb" >>"$_tmp/msg.info"
      ;;
    *)
      refuse "comparison-grammar" \
        "the comparison emitted a record this reader does not recognise: $rec" \
        "fix the awk/reader pair in scripts/ci/check-sigpipe-sites.sh; an unread record is never a pass"
      ;;
  esac
done <"$_tmp/cmp"

# ---------------------------------------------------------------------------
# DECLARED SCOPE — printed on EVERY run, pass or fail. A README nobody opens is not a
# declaration (CLAUDE.md).
# ---------------------------------------------------------------------------
printf '\n==== DECLARED SCOPE (check-sigpipe-sites.sh, #4061 AC4) ====\n'
printf 'guarded:   EVERY git-tracked scripts/**/*.sh (%d subject(s), derived at run time from the\n' "$N_SUBJECTS"
printf '           git index — never a curated path list)\n'
printf 'MEMBERSHIP comes from the INDEX, CONTENT from the WORKTREE. So a new site added to an\n'
printf '           already-tracked script REDS immediately, before any commit; a brand-new script\n'
printf '           becomes a subject the moment it is `git add`ed, which is before any PR. An\n'
printf '           UNTRACKED scratch file is deliberately not a subject.\n'
printf 'THE RULE:  a bash BUILTIN writer (printf/echo) with a pipe after it on the same line is a\n'
printf '           SHAPE MATCH. Implemented ONCE in scripts/tests/lib/sigpipe-matcher.sh and\n'
printf '           PINNED by the 33 cases of scripts/tests/test_gate_liveness_no_sigpipe.sh\n'
printf '           (#3803). No second copy of the rule exists, deliberately.\n'
printf 'THE RATCHET: an INCREASE in a baseline file FAILs; ANY match in a file the baseline does\n'
printf '           not list FAILs; and a CHANGED matched-line SET at an UNCHANGED count FAILs\n'
printf '           (a SWAP). A DECREASE never fails. The baseline is the ONLY way an existing\n'
printf '           site is tolerated, and %s is the only way to change it.\n' "$REGEN_CMD"
printf 'SWAPS ARE DETECTED, and that needs a second field: each entry carries a sha256 over the\n'
printf '           file\x27s matched lines NORMALISED (trimmed, whitespace runs collapsed, the\n'
printf '           matcher\x27s line-number prefix DISCARDED) and SORTED. A count alone cannot see\n'
printf '           one site removed and a different one added — it PASSes, and a new defect ships\n'
printf '           green (#3229: a false PASS is worse than no guard). Sorting the multiset keeps\n'
printf '           the digest motion-proof; a swap changes the multiset and reds.\n'
printf 'THE ONE DECLARED RESIDUAL: a NET DECREASE never fails, and any removal changes the digest,\n'
printf '           so a change that deletes TWO sites and adds ONE is still tolerated. Closing it\n'
printf '           would require the baseline to carry matched-line TEXT — the curated,\n'
printf '           motion-sensitive list this design refuses.\n'
printf 'NO LINE NUMBERS in the baseline (in either field), deliberately: #4061 pinned\n'
printf '           bootstrap-agent-machine.sh:3329 and it had drifted to :5392 in two days.\n'
printf 'A COUNT IS A COUNT OF SHAPE MATCHES, NOT OF CONFIRMED HAZARDS. The matcher is\n'
printf '           deliberately BROAD (#3229: loud false POSITIVES cost noise, false PASSes hide\n'
printf '           defects) and INHERITS its six declared false-positive classes — a pipe inside a\n'
printf '           format string or a quoted argument, a pipe in a trailing comment, an unrelated\n'
printf '           later pipeline on the same line, a quoted option-looking pattern, a run-to-EOF\n'
printf '           reader — and its residuals a/b/c: the scan is LEXICAL and PER-LINE; only\n'
printf '           printf/echo count as writers; and the command word is matched LITERALLY, so\n'
printf '           p\\rintf / $cmd / $(echo printf) are MISSED and are not narrowable to zero.\n'
printf '           Narrowing any of the false positives is issue #3992.\n'
printf 'WHAT THIS GUARD DOES NOT ASSERT: that the baseline sites are safe. They are UNTRIAGED\n'
printf '           (%d file(s) / %d recorded match(es)). It asserts only that the tree gets no\n' "$B_ENTRIES" "$B_SITES"
printf '           WORSE. #4061 converted TWO named sites and deliberately left the rest.\n'
printf 'PREREQUISITES: git + awk + a sha256 tool (%s) + standard text tools. No cargo, no\n' "$DIGEST_CMD"
printf '           python3, no datasets, no network. This guard NEVER SKIPs; an unmeasurable run\n'
printf '           is a REFUSAL (exit 3) — including an absent digest tool, which is NEVER a\n'
printf '           silent fall back to comparing counts alone.\n'
printf '==== END DECLARED SCOPE ====\n\n'

while IFS= read -r m; do
  [ -n "$m" ] || continue
  printf 'SIGPIPE-SITES: %s\n' "$m"
done <"$_tmp/msg.info"

if [ -s "$_tmp/msg.fail" ]; then
  while IFS= read -r m; do
    [ -n "$m" ] || continue
    printf 'SIGPIPE-SITES: %s\n' "$m"
  done <"$_tmp/msg.fail"
  printf 'SIGPIPE-SITES: verdict INCREASE (%d file(s) grew, %d new file(s) with sites, %d file(s) SWAPPED a site at an unchanged count)\n' \
    "$INCREASED" "$NEWFILES" "$SWAPPED"
  printf 'SIGPIPE-SITES: REMEDY: remove the writer — `reader <<<"$text"` instead of\n'
  printf '           `printf %%s\\n "$text" %s reader`. NOTE the herestring is byte-equivalent only\n' "$PIPE"
  printf '           for `printf "%%s\\\\n"`: without a trailing newline a herestring ADDS one, which a\n'
  printf '           downstream `tr -c` then translates. If the site is genuinely correct code (one\n'
  printf '           of the declared false-positive classes above), restructure the line, or accept\n'
  printf '           it deliberately with: %s\n' "$REGEN_CMD"
  printf '           IF origin/main ADVANCED (a rebase or a merge of main) the new sites may not be\n'
  printf '           yours at all: this is a WHOLE-TREE census (counts AND content digests), so any\n'
  printf '           commit that adds, removes or rewords a matching line anywhere under scripts/**\n'
  printf '           moves it. Check whether the named files are in\n'
  printf '           YOUR diff; if they are not, %s and commit the\n' "$REGEN_CMD"
  printf '           regenerated baseline as part of the rebase.\n'
  exit 1
fi

printf 'SIGPIPE-SITES: 0 INCREASE RECOGNISED across %d subject(s) vs %d baseline entr%s\n' \
  "$N_SUBJECTS" "$B_ENTRIES" "$([ "$B_ENTRIES" -eq 1 ] && printf 'y' || printf 'ies')"
printf 'SIGPIPE-SITES: verdict NO-INCREASE\n'
exit 0
