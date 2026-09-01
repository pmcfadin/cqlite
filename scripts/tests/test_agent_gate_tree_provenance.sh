#!/usr/bin/env bash
# PROVENANCE regression test for the #2926 tree-integrity guard — the THIRD tree suite.
#
# WHY THIS FILE EXISTS (#2926 review J1/J2/J3). Review H2 established the contract that a
# block published because the tree MUTATED mid-run must name the identity the run actually
# EXECUTED against (`commit:` = the VERIFIED START, labelled) and carry the post-mutation
# reading on its own labelled `tree-end:` line. That fix was applied to ONE of the guard's
# THREE mutation-detection paths — the component boundary. The TERMINAL path (which
# dominates `--lite`, `--delta` and the full gate's whole post-last-boundary window) and the
# SIDE-lane MARKER path both kept stamping an UNLABELLED post-mutation sha: the exact defect
# H2 exists to prevent, on the paths a reader is most likely to meet it. Nothing caught it
# because the existing terminal case asserted only "did not certify" and never looked at the
# `commit:` line.
#
# So this suite is organised around the property, not the path: EVERY detection path is
# asserted to publish the SAME two labels, and the labelling is pinned to a SINGLE
# assignment site in the gate (phase D) so a FOURTH path cannot diverge again the way the
# third one did. It also covers the two smaller findings that share the "a hand-maintained
# list silently stops covering the code" shape:
#   J2 — the boundary block's component table iterated the FULL gate's COMPONENTS, which
#        does not contain `scoped-tests`, the component --lite/--delta spend their time in;
#   J3 — only the summary file was carved out of the digest, so a caller redirecting the
#        run's own stdout to a non-ignored in-repo path made the gate trip on its own output.
#
# Split from scripts/tests/test_agent_gate_tree_integrity.sh (already ~1870 lines) to keep
# both near the campsite-rule size target — see #1135. Hermetic: every fixture lives under
# one per-run `mktemp -d …XXXXXX`; no network; no repo write outside that namespace; NO
# assertion references elapsed time (#2642).
#
# Run standalone:   bash scripts/tests/test_agent_gate_tree_provenance.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# Never inherit a caller's summary path / parent marker (#2751/#2874 discipline).
unset AGENT_GATE_SUMMARY_FILE
unset AGENT_GATE_PARENT_RUN_ID

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-tree-prov.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

GIT_ID=(-c user.email=gate@example.invalid -c user.name=gate-selftest)
COMMIT_ENV=(GIT_AUTHOR_NAME=gate GIT_AUTHOR_EMAIL=gate@example.invalid
            GIT_COMMITTER_NAME=gate GIT_COMMITTER_EMAIL=gate@example.invalid)

# The two CONTRACT strings, spelled here exactly as the spec pins them (em dashes are
# literal U+2014). Every path is asserted against these same two constants, so "the paths
# agree" is a property of the test, not something each case restates in its own words.
START_LABEL='(VERIFIED START — the identity this run executed against; the tree MUTATED mid-run, see tree-end: for the post-mutation observation)'
END_LABEL='(POST-MUTATION observation — NOT the identity this run executed against)'

# mkrepo_from <name> <gate-file> -> a FAKE checkout running THAT gate. Copying only the
# gate into <root>/scripts/ makes its `cd "$(dirname "$0")/.."` resolve REPO_ROOT to
# <root>, so every capture, mutation and default summary path stays in this run's namespace.
# Taking the gate as a PARAMETER is what lets a mutant gate be exercised end-to-end.
mkrepo_from() {
  local root="$tmp/$1" gate="$2"
  mkdir -p "$root/scripts"
  cp "$gate" "$root/scripts/agent-gate.sh"
  # The DISPOSABLE-CHECKOUT MARKER (#2926 review B5): the mutating self-test hooks refuse
  # to write into any checkout that does not carry it.
  printf 'disposable fixture for scripts/tests/test_agent_gate_tree_provenance.sh\n' \
                       > "$root/.agent-gate-tree-selftest-fixture"
  printf 'hello\n'     > "$root/README.md"
  printf 'lock v1\n'   > "$root/Cargo.lock"
  printf 'target/\n*.log\n.agent-gate-summary.txt\n.agent-gate-lite-summary.txt\n.agent-gate-delta-summary.txt\n' \
                       > "$root/.gitignore"
  ( cd "$root" && git init -q . && git add -A && git "${GIT_ID[@]}" commit -qm init ) >/dev/null 2>&1
  printf '%s\n' "$root"
}
mkrepo() { mkrepo_from "$1" "$GATE"; }

# A stub `cargo` that always succeeds (nothing compiles) and can create an untracked file
# mid-run, which is how the "the carve-out was not widened" control gets a real mutation.
STUBBIN="$tmp/stubbin"; mkdir -p "$STUBBIN"
cat > "$STUBBIN/cargo" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = fmt ] && [ -n "${FAKE_CARGO_CREATE:-}" ]; then
  mkdir -p "$(dirname "$FAKE_CARGO_CREATE")"
  printf 'created mid-run\n' > "$FAKE_CARGO_CREATE"
fi
# …and the TRACKED-file variant: appending to an existing tracked file is how the
# `lockfile-settled` carve-out is reached (cargo re-resolving a stale lockfile).
if [ "${1:-}" = fmt ] && [ -n "${FAKE_CARGO_MUTATE:-}" ]; then
  printf 'lock v2\n' >> "$FAKE_CARGO_MUTATE"
fi
case "${1:-}" in metadata) printf '{"packages":[],"workspace_members":[],"target_directory":"/tmp"}\n' ;; esac
exit 0
STUB
chmod +x "$STUBBIN/cargo"

# gate_replace_line <src> <dst> <exact-code-line> <replacement> — the MUTANT builder.
# Matches the line with its indentation stripped and re-emits the replacement at the same
# indentation, so a mutant is addressed by its CALL FORM, never by how it is indented (the
# #2926 review F2 vacuity). rc 3 when nothing matched, so a mutant that silently changes
# nothing is reported instead of "proving" whatever the unmutated gate already does.
gate_replace_line() {
  TEST_FROM="$3" TEST_TO="$4" awk '
    { l = $0; sub(/^[[:space:]]+/, "", l) }
    l == ENVIRON["TEST_FROM"] {
      n++
      print substr($0, 1, match($0, /[^[:space:]]/) - 1) ENVIRON["TEST_TO"]
      next
    }
    { print }
    END { if (n == 0) exit 3 }
  ' "$1" > "$2"
}

# summary_field <summary> <line-prefix> -> that line, or empty.
summary_line() { grep "^$2" "$1" 2>/dev/null | head -1; }
short7()  { printf '%.7s' "$1"; }
short12() { printf '%.12s' "$1"; }

# run_selftest <repo> <mode> <summary> <out> [env KEY=VAL …] — drive one detection path.
run_selftest() {
  local repo="$1" mode="$2" sum="$3" out="$4"; shift 4
  ( cd "$repo" && PATH="$STUBBIN:$PATH" env ${1+"$@"} \
      AGENT_GATE_SUMMARY_FILE="$sum" AGENT_GATE_TREE_SELFTEST="$mode" \
      bash "$repo/scripts/agent-gate.sh" >"$out" 2>&1 )
}

# assert_mutation_labels <label> <summary> <start-head> <end-head> — THE shared contract
# check, applied identically to every detection path.
assert_mutation_labels() {
  local label="$1" sum="$2" a="$3" b="$4" missing=""
  local cline eline
  cline=$(summary_line "$sum" 'commit: ')
  eline=$(summary_line "$sum" 'tree-end: ')
  case "$cline" in
    "commit: $(short7 "$a") "*) ;;
    *) missing="${missing:+$missing }commit-names-verified-start" ;;
  esac
  case "$cline" in
    *"$(short7 "$b")"*) missing="${missing:+$missing }commit-NAMES-POST-MUTATION-SHA" ;;
  esac
  case "$cline" in
    *"$START_LABEL") ;;
    *) missing="${missing:+$missing }commit-label" ;;
  esac
  case "$eline" in
    "tree-end: $(short12 "$b") "*) ;;
    *) missing="${missing:+$missing }tree-end-names-post-mutation" ;;
  esac
  case "$eline" in
    *"$END_LABEL") ;;
    *) missing="${missing:+$missing }tree-end-label" ;;
  esac
  grep -q '^RESULT: FAIL' "$sum" 2>/dev/null || missing="${missing:+$missing }RESULT-FAIL"
  if [ -z "$missing" ]; then
    ok "$label: commit: names the VERIFIED START $(short7 "$a") (labelled) and tree-end: the post-mutation $(short12 "$b") (labelled)"
    return 0
  fi
  bad "$label: $missing"
  printf '  commit: %s\n  end:    %s\n' "$cline" "$eline"
  return 1
}

echo "=== phase A (J1): EVERY mutation-detection path labels its provenance ========"

# The three paths, driven through the production self-test hooks, each with a real mid-run
# COMMIT so the start and post-mutation identities genuinely differ:
#   boundary — _assert_tree_integrity at a component boundary (the path H2 fixed)
#   terminal — _tree_finalize's authoritative last capture (the J1 defect)
#   side     — a SIDE-lane marker consumed by _apply_tree_integrity_marker after the drain
#              (the THIRD path, which review J1 did not name and which had the same gap)
r_lbl=$(mkrepo label-repo)
for mode in boundary terminal side; do
  sum="$tmp/label-$mode.txt"; out="$tmp/label-$mode.out"
  before=$( cd "$r_lbl" && git rev-parse HEAD )
  run_selftest "$r_lbl" "$mode" "$sum" "$out" "${COMMIT_ENV[@]}" \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md AGENT_GATE_TREE_SELFTEST_COMMIT=1
  after=$( cd "$r_lbl" && git rev-parse HEAD )
  if [ "$before" = "$after" ]; then
    bad "J1/$mode: HEAD did not move — the start-vs-post-mutation distinction was not exercised"
    continue
  fi
  assert_mutation_labels "J1/$mode" "$sum" "$before" "$after"
done

# …and the three blocks agree WORD FOR WORD. Divergent-but-present wording is the failure
# mode a per-path assertion cannot see, and it is what a triager would trip over.
# Compared on the SUFFIX alone (everything from the first ` (` on): the shas, dirty flags
# and digests legitimately differ per run, and the property under test is that the LABEL
# wording does not.
lbl_variants=$(for mode in boundary terminal side; do
                 summary_line "$tmp/label-$mode.txt" 'commit: '   | sed 's/^[^(]* (/(/'
                 summary_line "$tmp/label-$mode.txt" 'tree-end: ' | sed 's/^[^(]* (/(/'
               done | LC_ALL=C sort -u | grep -c .)
if [ "$lbl_variants" = 2 ]; then
  ok "J1: all three detection paths publish the SAME two label strings (2 distinct suffix renderings, not 3+)"
else
  bad "J1: the paths publish $lbl_variants distinct commit:/tree-end: renderings — the labelling has drifted between paths"
  for mode in boundary terminal side; do summary_line "$tmp/label-$mode.txt" 'commit: '; done
fi

# A mutation with NO commit: HEAD does not move, only the digest does. The labels must
# still appear (a same-HEAD content mutation is exactly as uncertifiable), which also
# proves the labelling is not keyed on the sha changing.
sum="$tmp/label-nocommit.txt"; out="$tmp/label-nocommit.out"
run_selftest "$r_lbl" terminal "$sum" "$out" AGENT_GATE_TREE_SELFTEST_MUTATE=README.md
if grep -q "^commit: .*$START_LABEL\$" "$sum" && grep -q "^tree-end: .*$END_LABEL\$" "$sum"; then
  ok "J1: a same-HEAD content mutation at the terminal capture is labelled too (the rule keys on the IDENTITY, not on HEAD moving)"
else
  bad "J1: a digest-only terminal mutation published unlabelled provenance"
  grep -E '^commit:|^tree-end:' "$sum" 2>/dev/null
fi
( cd "$r_lbl" && git checkout -q -- README.md )

# THE CONTROL: an unmutated run must carry NEITHER label — otherwise every assertion above
# is satisfied by a gate that labels unconditionally.
sum="$tmp/label-clean.txt"; out="$tmp/label-clean.out"
run_selftest "$r_lbl" clean "$sum" "$out"
c_head=$( cd "$r_lbl" && git rev-parse HEAD )
if grep -q '^tree-integrity: PASS$' "$sum" \
   && grep -q "^commit: $(short7 "$c_head") " "$sum" \
   && ! grep -q 'VERIFIED START' "$sum" && ! grep -q 'POST-MUTATION' "$sum"; then
  ok "J1 control: an unmutated run names the real current sha with NEITHER label (the labelling is conditional)"
else
  bad "J1 control: the unmutated block is mislabelled or does not name the current sha"
  grep -E '^commit:|^tree-' "$sum" 2>/dev/null
fi

# …and the DISCRIMINATION MUTANT: restore exactly the pre-J1 terminal branch (fail closed
# WITHOUT the shared labelling) and prove the assertion above goes red. This is the defect
# as it actually shipped, reproduced.
mut="$tmp/gate-mutant-terminal-label.sh"
if gate_replace_line "$GATE" "$mut" \
     '_tree_mark_mutation "<terminal>" "$(_tree_fail_reason "$head" "$rendered")"' \
     '_tree_fail_closed "<terminal>" "$(_tree_fail_reason "$head" "$rendered")"'; then
  r_mut=$(mkrepo_from label-mutant-repo "$mut")
  sum="$tmp/label-mutant.txt"; out="$tmp/label-mutant.out"
  before=$( cd "$r_mut" && git rev-parse HEAD )
  run_selftest "$r_mut" terminal "$sum" "$out" "${COMMIT_ENV[@]}" \
    AGENT_GATE_TREE_SELFTEST_MUTATE=README.md AGENT_GATE_TREE_SELFTEST_COMMIT=1
  after=$( cd "$r_mut" && git rev-parse HEAD )
  if [ "$before" = "$after" ]; then
    bad "J1 mutant: HEAD did not move — the mutant case was not exercised"
  # The probe runs in a SUBSHELL: assert_mutation_labels reports through ok/bad, and letting
  # it do so here would add its (expected) failure to this suite's own tally.
  elif ( assert_mutation_labels "J1 mutant (SHOULD FAIL)" "$sum" "$before" "$after" ) >/dev/null 2>&1; then
    bad "J1 mutant: the label assertion STILL PASSES with the pre-fix terminal branch — it cannot fail"
  else
    m_commit=$(summary_line "$sum" 'commit: ')
    case "$m_commit" in
      "commit: $(short7 "$after") "*)
        ok "J1 mutant: the pre-fix terminal branch stamps the UNLABELLED post-mutation sha $(short7 "$after") — the assertion is proved discriminating" ;;
      *)
        ok "J1 mutant: the pre-fix terminal branch fails the label contract (proved discriminating): $m_commit" ;;
    esac
  fi
else
  bad "J1 mutant: the terminal mutation-mark call site was not found — the mutant is vacuous"
fi

# The OVER-labelling near-miss, the mirror image of J1: a capture that could not be
# VALIDATED is fail-closed, but nothing observed a mutation, so the block must NOT claim a
# verified-start/post-mutation split it never saw. (A failing hash tool is how the guard
# reaches that state — see the B1 case in the integrity suite.)
BADHASH="$tmp/badhash"; mkdir -p "$BADHASH"
printf '#!/bin/sh\nexit 3\n' > "$BADHASH/sha256sum"; chmod +x "$BADHASH/sha256sum"
printf '#!/bin/sh\nexit 3\n' > "$BADHASH/shasum";    chmod +x "$BADHASH/shasum"
sum="$tmp/label-badhash.txt"; out="$tmp/label-badhash.out"
( cd "$r_lbl" && PATH="$BADHASH:$STUBBIN:$PATH" env AGENT_GATE_SUMMARY_FILE="$sum" \
    AGENT_GATE_TREE_SELFTEST=terminal AGENT_GATE_TREE_SELFTEST_MUTATE=README.md \
    bash "$r_lbl/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
if [ "$rc" -ne 0 ] && grep -q '^RESULT: FAIL' "$sum" \
   && ! grep -q 'VERIFIED START' "$sum" && ! grep -q 'POST-MUTATION' "$sum"; then
  ok "J1 near-miss: a capture that could not be VALIDATED fails closed WITHOUT claiming a verified-start/post-mutation split (not over-labelled)"
else
  bad "J1 near-miss: the capture-failure block claims a split it never observed (rc=$rc)"
  grep -E '^commit:|^tree-' "$sum" 2>/dev/null
fi
# …and the POSITIVE half of the same contract (#2926 review K4). Asserting only the ABSENCE
# of the two labels leaves the spec's third pinned string — the rendering a capture-failure
# block actually publishes — unverified by construction, the same gap the H2/H5 wording fix
# closed. The spec pins it verbatim: "When no validated terminal capture exists the line
# SHALL read exactly `commit: unverified branch: <branch> dirty: unverified`", so it is
# checked as a WHOLE-LINE equality, exactly the way START_LABEL/END_LABEL are checked above.
#
# Here `<branch>` is `unknown`, and that is the contract too, not a shortcut: the branch NAME
# is read ONCE inside the guarded window (TREE_START_BRANCH, review C1), and a START capture
# that never validated never opened one — naming a branch from a fresh emit-time git call is
# precisely what C1 forbids. The variant below covers `<branch>` carrying a real name.
nm_expect="commit: unverified branch: unknown dirty: unverified"
nm_commit=$(summary_line "$sum" 'commit: ')
if [ "$nm_commit" = "$nm_expect" ]; then
  ok "J1/K4 near-miss: the capture-failure block renders the pinned contract line exactly ('$nm_expect')"
else
  bad "J1/K4 near-miss: the unverified rendering is not the pinned contract line — got '$nm_commit', want '$nm_expect'"
fi

# The variant that exercises the `<branch>` substitution itself: a digest tool that succeeds
# for the START capture and fails for every later one. The start identity is validated (so
# the branch IS read), no mutation is ever observed (so the VERIFIED-START branch of the
# renderer is not taken), and the terminal capture cannot be validated — the exact state the
# pinned line describes, now with a real branch name in it.
ONESHOT="$tmp/oneshot"; mkdir -p "$ONESHOT"
for _tool in sha256sum shasum; do
  { printf '#!/bin/sh\n'
    printf 'n=$(cat "%s/count" 2>/dev/null || echo 0); n=$((n + 1)); printf %%s "$n" > "%s/count"\n' \
      "$ONESHOT" "$ONESHOT"
    printf '[ "$n" -gt 1 ] && exit 3\n'
    printf 'exec %s "$@"\n' "$(command -v "$_tool" 2>/dev/null || echo /bin/false)"
  } > "$ONESHOT/$_tool"
  chmod +x "$ONESHOT/$_tool"
done
printf '0' > "$ONESHOT/count"
sum="$tmp/label-oneshot.txt"; out="$tmp/label-oneshot.out"
os_branch=$( cd "$r_lbl" && git rev-parse --abbrev-ref HEAD 2>/dev/null )
( cd "$r_lbl" && PATH="$ONESHOT:$STUBBIN:$PATH" env AGENT_GATE_SUMMARY_FILE="$sum" \
    AGENT_GATE_TREE_SELFTEST=clean bash "$r_lbl/scripts/agent-gate.sh" >"$out" 2>&1 ); rc=$?
os_expect="commit: unverified branch: $os_branch dirty: unverified"
os_commit=$(summary_line "$sum" 'commit: ')
if [ -n "$os_branch" ] && [ "$os_commit" = "$os_expect" ] && [ "$rc" -ne 0 ] \
   && grep -q '^RESULT: FAIL' "$sum" 2>/dev/null; then
  ok "K4: with a VALIDATED start and an unvalidatable later capture the pinned line carries the real branch ('$os_expect')"
else
  bad "K4: the pinned unverified line did not carry the window's branch — got '$os_commit', want '$os_expect' (rc=$rc)"
fi
( cd "$r_lbl" && git checkout -q -- README.md )

echo "=== phase B (J2): the boundary block's component table covers the RUNNING mode ="

# _tree_mode_components, exercised through the read-only hook in each mode. The full gate's
# COMPONENTS never contains `scoped-tests`; LITE/DELTA are exactly where it runs.
r_mc=$(mkrepo modeset-repo)
mode_components() { # mode_components [gate args…] — the set THIS mode would dispatch
  ( cd "$r_mc" && PATH="$STUBBIN:$PATH" env AGENT_GATE_SUMMARY_FILE="$tmp/mc-sentinel.txt" \
      AGENT_GATE_TREE_SELFTEST=mode-components \
      bash "$r_mc/scripts/agent-gate.sh" ${1+"$@"} 2>/dev/null ) \
    | sed -n 's/^tree-selftest: mode-components=//p' | head -1
}
mc_full=$(mode_components)
mc_lite=$(mode_components --lite)
mc_delta=$(mode_components --delta HEAD --anchor-run-id selftest)
case " $mc_full " in
  *" core-tests "*) case " $mc_full " in
                      *" scoped-tests "*) bad "J2: the FULL set claims scoped-tests, which the full gate does not dispatch" ;;
                      *) ok "J2: the full gate's set is COMPONENTS (core-tests present, scoped-tests absent)" ;;
                    esac ;;
  *) bad "J2: the full gate's component set is not COMPONENTS: '$mc_full'" ;;
esac
if [ "$mc_lite" = "file-size fmt clippy roborev-lints scoped-tests" ]; then
  ok "J2: --lite reports its OWN set, including scoped-tests ($mc_lite)"
else
  bad "J2: --lite's set is '$mc_lite', expected the LITE_COMPONENTS list"
fi
if [ "$mc_delta" = "file-size fmt scoped-tests" ]; then
  ok "J2: --delta reports its OWN set, including scoped-tests ($mc_delta)"
else
  bad "J2: --delta's set is '$mc_delta', expected the DELTA_COMPONENTS list"
fi
# The mutant: hard-wire the lite branch back to COMPONENTS.
mut="$tmp/gate-mutant-modeset.sh"
if gate_replace_line "$GATE" "$mut" 'printf '"'"'%s\n'"'"' "${LITE_COMPONENTS[@]}"' \
                                    'printf '"'"'%s\n'"'"' "${COMPONENTS[@]}"'; then
  r_mcm=$(mkrepo_from modeset-mutant-repo "$mut")
  mcm=$( cd "$r_mcm" && PATH="$STUBBIN:$PATH" env AGENT_GATE_SUMMARY_FILE="$tmp/mcm-sentinel.txt" \
           AGENT_GATE_TREE_SELFTEST=mode-components bash "$r_mcm/scripts/agent-gate.sh" --lite 2>/dev/null \
         | sed -n 's/^tree-selftest: mode-components=//p' | head -1 )
  case " $mcm " in
    *" scoped-tests "*) bad "J2 mutant: the lite set STILL contains scoped-tests with the pre-fix branch — the check cannot fail" ;;
    *) ok "J2 mutant: hard-wiring the lite branch to COMPONENTS drops scoped-tests (proved discriminating)" ;;
  esac
else
  bad "J2 mutant: the LITE branch of _tree_mode_components was not found — the mutant is vacuous"
fi

# …and the SWEEP: a component that RECORDED a verdict under a name no static set carries
# must still appear in the table and be counted. The `tree-selftest` hook is exactly such a
# component, so this is asserted on a real recorded verdict rather than a synthetic one.
r_tbl=$(mkrepo table-repo)
sum="$tmp/table-boundary.txt"; out="$tmp/table-boundary.out"
run_selftest "$r_tbl" boundary "$sum" "$out" AGENT_GATE_TREE_SELFTEST_MUTATE=README.md
# FIVE tokens, from two issues that reached the same conclusion independently: #3625 added
# VACUOUS (a PASS whose measured subject count is zero) and #3402 added OPT-OUT (file-size
# under an engaged CQLITE_ALLOW_FILE_GROWTH=1). A hard-coded subset is WRONG wherever it
# appears: it stops SEEING the very rows it does not name.
# Here the consequence is a guard that REDS ON CORRECT INPUT — this assert compares a COUNT of
# printed rows against `components-completed:`, which counts every recorded verdict whatever
# its token, so a grammar knowing four of five UNDERCOUNTS a healthy block. The equality is
# between two counts, so the GRAMMAR, not the fixture, is what has to be complete.
n_rows=$(grep -cE '^[a-z][a-z0-9-]*: +(PASS|FAIL|SKIP|VACUOUS|OPT-OUT) \([0-9]+s\)' "$sum" 2>/dev/null | tr -d ' ')
n_done=$(sed -n 's/^components-completed: \([0-9]*\) .*/\1/p' "$sum" | head -1)
if grep -qE '^tree-selftest: +PASS \([0-9]+s\)' "$sum"; then
  ok "J2: a recorded verdict whose component no static set names still appears in the table (tree-selftest row present)"
else
  bad "J2: the tree-selftest verdict was dropped from the boundary block's table"
  grep -E '^components-completed:|PASS \(' "$sum" 2>/dev/null
fi
if [ -n "$n_done" ] && [ "$n_done" = "$n_rows" ]; then
  ok "J2: components-completed ($n_done) equals the number of rows the block printed — no undercount"
else
  bad "J2: components-completed is '$n_done' but the block printed $n_rows row(s)"
fi
# #3625 (roborev job 360 finding 2): the boundary block is a MODE, and every mode's
# component rows must carry the SAME two annotations — the #3453 feature matrix and the
# #3625 census — because the whole safety argument of both is "one renderer, so no mode can
# render a block the others do not". This path used to `printf` its rows directly, so a run
# that STOPPED at a boundary emitted a table with neither, and the aggregate `census:` line
# was absent as well. Asserted BEHAVIOURALLY here, on the block a real boundary run emitted,
# because the structural half (in scripts/tests/test_agent_gate_census.sh) cannot see
# whether the call site is reached.
n_annot=$(grep -cE '^[a-z][a-z0-9-]*: +(PASS|FAIL|SKIP|VACUOUS) \([0-9]+s\) +\[.+\]  \{.+\}$' "$sum" 2>/dev/null | tr -d ' ')
if [ "$n_rows" -gt 0 ] && [ "$n_annot" = "$n_rows" ]; then
  ok "J2/#3625: all $n_rows boundary row(s) carry BOTH the feature matrix and the census suffix — the truncated table is the same dialect as a completed one"
else
  bad "J2/#3625: only $n_annot of $n_rows boundary row(s) are fully annotated — this mode renders a block the others do not"
  grep -E '^[a-z][a-z0-9-]*: +(PASS|FAIL|SKIP|VACUOUS) \(' "$sum" 2>/dev/null | head -5
fi
if grep -qE '^census: [0-9]+/[0-9]+ components AFFIRMED a count;.*NON-EXHAUSTIVE' "$sum" 2>/dev/null; then
  ok "J2/#3625: the boundary block carries the aggregate census: line, so a stopped run states what it verified rather than only how far it got"
else
  bad "J2/#3625: the boundary block has no aggregate census: line"
fi
# The mutant: neutralise the sweep's not-yet-printed test so it skips everything.
mut="$tmp/gate-mutant-sweep.sh"
if gate_replace_line "$GATE" "$mut" 'case "$_seen" in *" $_c "*) continue ;; esac' \
                                    'case "$_seen" in *) continue ;; esac'; then
  r_swm=$(mkrepo_from sweep-mutant-repo "$mut")
  sum="$tmp/table-mutant.txt"; out="$tmp/table-mutant.out"
  run_selftest "$r_swm" boundary "$sum" "$out" AGENT_GATE_TREE_SELFTEST_MUTATE=README.md
  if grep -qE '^tree-selftest: +PASS' "$sum"; then
    bad "J2 mutant: the row survives with the sweep disabled — the check cannot fail"
  else
    ok "J2 mutant: disabling the sweep drops the unlisted component's row (proved discriminating)"
  fi
else
  bad "J2 mutant: the sweep guard was not found — the mutant is vacuous"
fi

echo "=== phase C (J3): the run's OWN stdout/stderr target is carved out, nothing more ="

r_fd=$(mkrepo fd-repo)
# The DOCUMENTED invocation redirects to `gate.log`, which `.gitignore` covers — so the
# case that matters is a NON-ignored in-repo target. `gate-out.txt` matches no ignore rule.
( cd "$r_fd" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$tmp/fd-only.txt" \
    bash "$r_fd/scripts/agent-gate.sh" --only fmt >gate-out.txt 2>&1 ); rc=$?
if grep -q '^tree-integrity: PASS$' "$tmp/fd-only.txt" 2>/dev/null && [ "$rc" -eq 3 ]; then
  ok "J3: a run whose stdout/stderr is redirected to a NON-ignored in-repo path still certifies (it does not trip on its own output)"
else
  bad "J3: the run tripped the guard on its own stdout redirect target (rc=$rc)"
  grep -E '^tree-|^RESULT:' "$tmp/fd-only.txt" 2>/dev/null
fi
# The carve-out names THIS run's target and nothing else…
fd_probe="$tmp/fd-probe.out"
( cd "$r_fd" && env AGENT_GATE_SUMMARY_FILE="$tmp/fd-probe-sentinel.txt" \
    AGENT_GATE_TREE_SELFTEST=capture bash "$r_fd/scripts/agent-gate.sh" >gate-out.txt 2>&1 )
cp "$r_fd/gate-out.txt" "$fd_probe"
if [ "$(sed -n 's/^tree-selftest: stdout-rel=//p' "$fd_probe" | head -1)" = gate-out.txt ] \
   && [ "$(sed -n 's/^tree-selftest: stderr-rel=//p' "$fd_probe" | head -1)" = gate-out.txt ]; then
  ok "J3: the carve-out resolves to exactly this run's redirect target (gate-out.txt), for both fds"
else
  bad "J3: the fd carve-out did not resolve the redirect target"
  grep -E 'stdout-rel|stderr-rel' "$fd_probe" 2>/dev/null
fi
# The `2>&1` assumption is a near-miss of its own: stdout and stderr can be redirected to
# DIFFERENT in-repo files, and carving out only fd 1 would leave the run tripping on its own
# stderr log.
( cd "$r_fd" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$tmp/fd-split.txt" \
    bash "$r_fd/scripts/agent-gate.sh" --only fmt >gate-out.txt 2>gate-err.txt ); rc=$?
if grep -q '^tree-integrity: PASS$' "$tmp/fd-split.txt" 2>/dev/null && [ "$rc" -eq 3 ]; then
  ok "J3: SEPARATE in-repo stdout and stderr targets are both carved out (the carve-out does not assume 2>&1)"
else
  bad "J3: a run with split stdout/stderr redirects into the checkout tripped the guard (rc=$rc)"
  grep -E '^tree-integrity:' "$tmp/fd-split.txt" 2>/dev/null
fi
rm -f "$r_fd/gate-out.txt" "$r_fd/gate-err.txt"
# …and NOT to an out-of-repo target, and not to a pipe/tty (nothing is excluded then).
out_probe=$( cd "$r_fd" && env AGENT_GATE_SUMMARY_FILE="$tmp/fd-out-sentinel.txt" \
               AGENT_GATE_TREE_SELFTEST=capture bash "$r_fd/scripts/agent-gate.sh" 2>/dev/null \
             | sed -n 's/^tree-selftest: stdout-rel=//p' | head -1 )
if [ "$out_probe" = "<none>" ]; then
  ok "J3 control: a non-file (pipe) stdout carves out NOTHING — the exclusion is not a blanket one"
else
  bad "J3 control: a piped stdout produced an exclusion '$out_probe'"
fi
rm -f "$r_fd/gate-out.txt"
# …and the exclusion was not WIDENED: an untracked file created mid-run by the component is
# still a fatal mutation, on a run that is redirecting into the checkout.
( cd "$r_fd" && PATH="$STUBBIN:$PATH" FAKE_CARGO_CREATE="$r_fd/vendor/new-file.txt" \
    AGENT_GATE_SUMMARY_FILE="$tmp/fd-widened.txt" \
    bash "$r_fd/scripts/agent-gate.sh" --only fmt >gate-out.txt 2>&1 ); rc=$?
if grep -q 'tree-integrity: FAIL (tree-mutated-midrun;' "$tmp/fd-widened.txt" 2>/dev/null \
   && grep -q 'changed: vendor/new-file.txt' "$tmp/fd-widened.txt" 2>/dev/null && [ "$rc" -ne 0 ]; then
  ok "J3 control: an unrelated untracked file created mid-run is STILL fatal and named (the carve-out was not widened)"
else
  bad "J3 control: the mid-run untracked file was not caught on a run redirecting into the checkout (rc=$rc)"
  grep -E '^tree-integrity:|^RESULT:' "$tmp/fd-widened.txt" 2>/dev/null
fi
rm -rf "$r_fd/vendor" "$r_fd/gate-out.txt"
# The mutant: disable the fd probe (the pre-J3 behaviour) and prove the first case goes red
# — AND that the failure text then names the real cause rather than leaving a reader to
# guess, which is the fallback contract on a host with no /proc.
mut="$tmp/gate-mutant-fd.sh"
if gate_replace_line "$GATE" "$mut" 'if [ -e "/proc/$$/fd/1" ] || [ -L "/proc/$$/fd/1" ]; then' 'if false; then'; then
  r_fdm=$(mkrepo_from fd-mutant-repo "$mut")
  ( cd "$r_fdm" && PATH="$STUBBIN:$PATH" AGENT_GATE_SUMMARY_FILE="$tmp/fd-mutant.txt" \
      bash "$r_fdm/scripts/agent-gate.sh" --only fmt >gate-out.txt 2>&1 ); rc=$?
  if grep -q '^tree-integrity: PASS$' "$tmp/fd-mutant.txt" 2>/dev/null; then
    bad "J3 mutant: the run STILL certifies with the fd carve-out disabled — the case cannot fail"
  elif grep -q 'changed: gate-out.txt' "$tmp/fd-mutant.txt" 2>/dev/null; then
    ok "J3 mutant: with the carve-out disabled the run trips on its own output, naming gate-out.txt (proved discriminating)"
  else
    bad "J3 mutant: the mutant failed for some other reason (rc=$rc)"
    grep -E '^tree-integrity:' "$tmp/fd-mutant.txt" 2>/dev/null
  fi
  if grep -q 'this host cannot name the run.s own stdout/stderr redirect target' "$tmp/fd-mutant.txt" 2>/dev/null; then
    ok "J3: where the fd cannot be named, the FAIL line says so — the real cause is named, never silently excluded"
  else
    bad "J3: the no-/proc fallback hint is missing from the failure text"
    grep '^tree-integrity:' "$tmp/fd-mutant.txt" 2>/dev/null
  fi
else
  bad "J3 mutant: the fd probe guard was not found — the mutant is vacuous"
fi

echo "=== phase D: the labelling has ONE assignment site (a 4th path cannot diverge) ="

# J1 happened because a two-line rule lived at ONE of THREE detection sites and nobody
# noticed the other two. These checks make that structurally impossible to repeat: each
# concern is pinned to a single site, and each is proved discriminating by a mutant that
# adds a SECOND site — the exact shape of the regression.
fn_body() { # <file> <function-name> — indentation-aware (a nested definition is covered too)
  TEST_AWK_F="$2() {" awk '
    { l = $0; sub(/^[[:space:]]+/, "", l) }
    !inf && index(l, ENVIRON["TEST_AWK_F"]) == 1 {
      inf = 1
      pad = substr($0, 1, match($0, /[^[:space:]]/) - 1)
      print
      next
    }
    inf { print; if ($0 == pad "}") inf = 0 }
  ' "$1"
}
code_sites() { # <file> <fixed-string> -> count of comment-stripped code lines containing it
  sed 's/[[:space:]]*#.*$//' "$1" | grep -cF "$2" | tr -d ' '
}
in_fn() { # <file> <fn> <fixed-string>
  fn_body "$1" "$2" | sed 's/[[:space:]]*#.*$//' | grep -qF "$3"
}
# pinned <label> <fixed-string> <owning-fn> — one site, and it is in the owning function.
pinned() {
  local label="$1" needle="$2" fn="$3" n
  n=$(code_sites "$GATE" "$needle")
  if [ "$n" = 1 ] && in_fn "$GATE" "$fn" "$needle"; then
    ok "D: '$needle' has exactly ONE assignment site, inside $fn() ($label)"
    return 0
  fi
  bad "D: '$needle' has $n site(s) and/or is not owned by $fn() — the $label rule can diverge again"
  return 1
}
pinned "fail-closed state" 'TREE_MUTATED=1' _tree_fail_closed
# The COMPONENT-ATTRIBUTED verdict line specifically: `_tree_capture_start` also renders a
# `tree-integrity: FAIL (…)` line for a start capture that never produced an identity, which
# names no component and is not a detection — pinning on the `; detected-after-component:`
# tail addresses the detection verdict exactly, without dragging in the unrelated one.
pinned "fail-closed verdict" '; detected-after-component: $1)"' _tree_fail_closed
pinned "verified-start stamp" 'TREE_COMMIT_SOURCE=start' _tree_label_post_mutation
pinned "post-mutation suffix" 'TREE_END_LINE="$TREE_END_LINE $TREE_POST_MUTATION_SUFFIX"' _tree_label_post_mutation
# The CONTRACT TEXT itself is defined once, so no path can publish a paraphrase.
if [ "$(code_sites "$GATE" "$END_LABEL")" = 1 ]; then
  ok "D: the POST-MUTATION contract text is defined exactly once (TREE_POST_MUTATION_SUFFIX)"
else
  bad "D: the POST-MUTATION contract text appears $(code_sites "$GATE" "$END_LABEL") times — a path can publish a paraphrase"
fi
# …and every detection path reaches the shared labelling.
for pair in "_assert_tree_integrity|mutation" \
            "_apply_tree_integrity_marker|_tree_detection_mark" \
            "_tree_finalize|_tree_mark_mutation" \
            "_tree_boundary_fail|_tree_detection_mark"; do
  pfn=${pair%%|*}; pneedle=${pair#*|}
  if in_fn "$GATE" "$pfn" "$pneedle"; then
    ok "D: $pfn() routes its detection through the shared labelling ('$pneedle')"
  else
    bad "D: $pfn() no longer reaches the shared labelling — it can fail closed WITHOUT labelling"
  fi
done
# The mutant: a SECOND assignment site anywhere makes the pin go red. Appending a fourth
# "detection path" that sets the state itself is precisely the J1 regression.
mut="$tmp/gate-mutant-second-site.sh"
{ cat "$GATE"; printf '_tree_fourth_path() {\n  TREE_MUTATED=1\n  TREE_COMMIT_SOURCE=start\n}\n'; } > "$mut"
if [ "$(code_sites "$mut" 'TREE_MUTATED=1')" = 2 ] \
   && [ "$(code_sites "$mut" 'TREE_COMMIT_SOURCE=start')" = 2 ]; then
  ok "D mutant: a 4th path assigning the state itself produces a SECOND site, which the pin rejects (proved discriminating)"
else
  bad "D mutant: adding a second assignment site did not change the counts — the pin is vacuous"
fi

echo "=== phase E (K1): the mutation diagnostic is unambiguous about WHAT moved ====="

# The `changed:` list and the `lockfile-settled:` detail are SPACE-JOINED, so a path
# containing a space rendered exactly like TWO separate paths (#2926 review K1) — in the one
# artifact a triager reads after a mid-run mutation. Both renderings now escape the space as
# `\s`, the fourth member of the `.report` view's own backslash family (`\\`, `\n`, `\t`).
#
# These cases live in this suite rather than the (already oversized) integrity suite for the
# campsite reason its header states; the capture-side escaping cases are the B6 tab case
# there, and this is the RENDER side of the same property.
r_sp=$(mkrepo space-repo)
sum="$tmp/space-changed.txt"
( cd "$r_sp" && PATH="$STUBBIN:$PATH" FAKE_CARGO_CREATE="$r_sp/two words.txt" \
    AGENT_GATE_SUMMARY_FILE="$sum" bash "$r_sp/scripts/agent-gate.sh" --only fmt \
    >"$tmp/space-changed.out" 2>&1 ); rc=$?
sp_line=$(summary_line "$sum" 'tree-integrity: FAIL')
sp_bad=""
[ "$rc" -ne 0 ] || sp_bad="${sp_bad:+$sp_bad }run-certified"
case "$sp_line" in
  *"changed: two\\swords.txt;"*) ;;
  *) sp_bad="${sp_bad:+$sp_bad }space-not-escaped" ;;
esac
case "$sp_line" in
  *"two words.txt"*) sp_bad="${sp_bad:+$sp_bad }raw-space-still-rendered" ;;
esac
if [ -z "$sp_bad" ]; then
  ok "K1: a changed path containing a SPACE renders as 'two\\swords.txt' — one path, not two"
else
  bad "K1: the changed-path rendering is ambiguous ($sp_bad): '$sp_line'"
fi
rm -f "$r_sp/two words.txt"

# The same property on the OTHER rendering: the `lockfile-settled:` detail, which joins
# `<path> <before>→<after>` triples with the same space.
mkdir -p "$r_sp/vendor dir"
printf 'lock v1\n' > "$r_sp/vendor dir/Cargo.lock"
( cd "$r_sp" && git add -A && git "${GIT_ID[@]}" commit -qm "vendored lockfile" ) >/dev/null 2>&1
sum="$tmp/space-lockfile.txt"
( cd "$r_sp" && PATH="$STUBBIN:$PATH" FAKE_CARGO_MUTATE="$r_sp/vendor dir/Cargo.lock" \
    AGENT_GATE_SUMMARY_FILE="$sum" bash "$r_sp/scripts/agent-gate.sh" --only fmt \
    >"$tmp/space-lockfile.out" 2>&1 )
sp_lock=$(summary_line "$sum" 'tree-integrity: PASS (lockfile-settled')
case "$sp_lock" in
  *"lockfile-settled: vendor\\sdir/Cargo.lock "*)
    ok "K1: the lockfile-settled detail escapes the space too ('vendor\\sdir/Cargo.lock …') — the same helper, not a second convention" ;;
  *)
    bad "K1: the lockfile-settled detail renders an ambiguous path: '$sp_lock'" ;;
esac
( cd "$r_sp" && git checkout -q -- "vendor dir/Cargo.lock" )

# The mutant: restore the pre-fix rendering (the raw path, space and all) and prove the
# assertion above goes red — a one-path list becomes indistinguishable from a two-path one.
mut="$tmp/gate-mutant-render.sh"
if gate_replace_line "$GATE" "$mut" \
     'rendered="${rendered:+$rendered }$(_tree_render_path "$p")"' \
     'rendered="${rendered:+$rendered }$p"'; then
  r_spm=$(mkrepo_from render-mutant-repo "$mut")
  ( cd "$r_spm" && PATH="$STUBBIN:$PATH" FAKE_CARGO_CREATE="$r_spm/two words.txt" \
      AGENT_GATE_SUMMARY_FILE="$tmp/space-mutant.txt" \
      bash "$r_spm/scripts/agent-gate.sh" --only fmt >"$tmp/space-mutant.out" 2>&1 )
  spm_line=$(summary_line "$tmp/space-mutant.txt" 'tree-integrity: FAIL')
  case "$spm_line" in
    *"changed: two words.txt;"*)
      ok "K1 mutant: the pre-fix rendering emits 'changed: two words.txt' — indistinguishable from two paths (proved discriminating)" ;;
    *)
      bad "K1 mutant: the pre-fix rendering did not reproduce the ambiguity — the case is vacuous: '$spm_line'" ;;
  esac
else
  bad "K1 mutant: the changed-path render site was not found — the mutant is vacuous"
fi

echo "=== phase F (K2): oversized-untracked membership is cursor-based AND correct ==="

# Membership in the oversized-untracked set was a linear scan INSIDE the per-path loop, so
# each capture cost O(#untracked × #oversized) — at every component boundary, in every
# SIDE-lane subshell and at the terminal (#2926 review K2). It is now a single forward
# CURSOR, which is only correct because `bigpaths` is an order-preserving SUBSEQUENCE of
# `upaths`. This phase asserts the OUTCOME (the right files, and only those, take the
# size+mtime record) and then breaks each half of that reasoning in turn.
mkbig() { # mkbig <path> <bytes>
  local i=0
  : > "$1"
  while [ "$i" -lt "$2" ]; do printf '0123456789abcdef' >> "$1"; i=$(( i + 16 )); done
}
# k2_fixture <name> <gate> -> a checkout whose UNTRACKED set interleaves oversized and
# under-cap files AROUND an embedded git repo. `git ls-files --others` reports an embedded
# repo as the single DIRECTORY entry `m-embedded/`; handing that to find would make it
# RECURSE and report paths that are not in the untracked list at all, which is exactly the
# desync the probe filter exists to prevent.
k2_fixture() {
  local root; root=$(mkrepo_from "$1" "$2")
  mkbig "$root/a-big.bin" 8192
  printf 'small\n' > "$root/b-small.txt"
  mkdir -p "$root/m-embedded"
  ( cd "$root/m-embedded" && git init -q . ) >/dev/null 2>&1
  mkbig "$root/m-embedded/inner-big.bin" 8192
  mkbig "$root/z-big.bin" 8192
  printf '%s\n' "$root"
}
# k2_capture <repo> <manifest-out> -> the capture's `fallbacks=` count (empty on failure)
k2_capture() {
  ( cd "$1" && env AGENT_GATE_SUMMARY_FILE="$tmp/k2-sentinel.txt" \
      AGENT_GATE_TREE_SELFTEST=capture AGENT_GATE_TREE_HASH_CAP_BYTES=4096 \
      AGENT_GATE_TREE_SELFTEST_MANIFEST_OUT="$2" \
      bash "$1/scripts/agent-gate.sh" 2>/dev/null ) \
    | sed -n 's/^tree-selftest: .*fallbacks=//p' | head -1
}
# k2_value <report> <path> -> the VALUE field of that path's record. The path travels
# through the ENVIRONMENT, never `awk -v`, for the escape-transparency reason the gate's own
# lookups do (#2926 review G2/H4).
k2_value() { TEST_AWK_P="$2" awk -F'\t' '$4 == ENVIRON["TEST_AWK_P"] { print $2; exit }' "$1" 2>/dev/null; }

r_k2=$(k2_fixture k2-repo "$GATE")
m_k2="$tmp/k2-manifest"
k2_fb=$(k2_capture "$r_k2" "$m_k2")
k2_bad=""
case "$(k2_value "$m_k2.report" a-big.bin)" in SIZE:8192:MTIME:*) ;; *) k2_bad="${k2_bad:+$k2_bad }a-big-not-capped" ;; esac
case "$(k2_value "$m_k2.report" z-big.bin)" in SIZE:8192:MTIME:*) ;; *) k2_bad="${k2_bad:+$k2_bad }z-big-not-capped" ;; esac
case "$(k2_value "$m_k2.report" b-small.txt)" in
  SIZE:*|'') k2_bad="${k2_bad:+$k2_bad }small-file-capped-or-missing" ;;
esac
case "$(k2_value "$m_k2.report" m-embedded/)" in NONFILE) ;; *) k2_bad="${k2_bad:+$k2_bad }embedded-repo-record" ;; esac
[ "$k2_fb" = 2 ] || k2_bad="${k2_bad:+$k2_bad }fallbacks=$k2_fb(want 2)"
if [ -z "$k2_bad" ]; then
  ok "K2: the cursor selects EXACTLY the oversized untracked files (a-big, z-big) across an embedded-repo entry, and no other ($k2_fb fallbacks)"
else
  bad "K2: the oversized-untracked membership is wrong ($k2_bad)"
  grep -c . "$m_k2.report" 2>/dev/null
fi

# Mutant 1 — the cursor never reports a hit: nothing takes the size+mtime record, so the
# assertion above cannot pass vacuously on a capture that simply hashes everything.
mut="$tmp/gate-mutant-cursor.sh"
if gate_replace_line "$GATE" "$mut" 'isbig=1; bi=$(( bi + 1 ))' 'isbig=0; bi=$(( bi + 1 ))'; then
  r_k2m=$(k2_fixture k2-cursor-mutant-repo "$mut")
  k2m_fb=$(k2_capture "$r_k2m" "$tmp/k2-cursor-manifest")
  if [ "$k2m_fb" = 0 ]; then
    ok "K2 mutant: with the cursor hit disabled NO file takes the size+mtime record (proved discriminating)"
  else
    bad "K2 mutant: the cursor mutant still reported $k2m_fb fallback(s) — the assertion is vacuous"
  fi
else
  bad "K2 mutant: the cursor hit site was not found — the mutant is vacuous"
fi

# Mutant 2 — the load-bearing half: drop the probe FILTER, so the embedded-repo directory
# reaches find, find recurses into it, and its inner file lands in `bigpaths` between
# a-big.bin and z-big.bin. The cursor then desyncs and z-big.bin — a genuinely oversized
# file — silently stops taking the fallback. This is the case that proves the subsequence
# property the cursor rests on is real and not an assumption.
mut="$tmp/gate-mutant-probe.sh"
if gate_replace_line "$GATE" "$mut" \
     'if [ ! -L "$p" ] && [ -f "$p" ]; then probe+=("$p"); fi' 'probe+=("$p")'; then
  r_k2p=$(k2_fixture k2-probe-mutant-repo "$mut")
  k2p_fb=$(k2_capture "$r_k2p" "$tmp/k2-probe-manifest")
  k2p_z=$(k2_value "$tmp/k2-probe-manifest.report" z-big.bin)
  case "$k2p_z" in
    SIZE:*) bad "K2 mutant: dropping the probe filter changed nothing — the subsequence property is untested (fallbacks=$k2p_fb)" ;;
    *)      ok "K2 mutant: without the probe filter find recurses into the embedded repo and z-big.bin loses its record (fallbacks=$k2p_fb) — the subsequence property is load-bearing" ;;
  esac
else
  bad "K2 mutant: the probe filter site was not found — the mutant is vacuous"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
