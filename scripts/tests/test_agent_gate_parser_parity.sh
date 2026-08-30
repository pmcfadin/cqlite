#!/usr/bin/env bash
# Differential parity test for the jq/python3 parser PAIRS this lane added
# (issue #3522, roborev round 6 G1).
#
# WHAT IT ANSWERS. Several helpers here parse the same input two ways — a jq program and a
# python3 program — because a host may have only one of them. That makes each pair TWO
# IMPLEMENTATIONS OF ONE TRANSFORMATION, and CLAUDE.md records the rule such a pair breaks:
# "a port is a second implementation, and a second implementation's correctness is only
# knowable by differential testing against the ORIGINAL" (#3283 — a bash port of Go's
# exclusion logic whose NBSP divergence was unfindable by care, because it was tested against
# a MODEL of the original rather than the original).
#
# G1 was exactly that: `_jest_json_suite_counts`'s jq branch stripped at the LAST `/__test__/`
# (greedy `^.*`) and its python3 branch at the FIRST (`str.find`). On a python3-only host whose
# CHECKOUT PATH contains a `__test__` directory, every suite was reported missing AND extra —
# a FALSE RED on a correct tree, latent on every box that has jq, so no run here could see it.
#
# So this test does not re-derive what the parsers should output. It drives BOTH branches over
# ONE fixture set and asserts BYTE-IDENTICAL output. That is differential testing against the
# original rather than against a model of it, and it is the only check that can catch a
# divergence nobody thought to predict.
#
# EACH FIXTURE IS ADVERSARIAL ON PURPOSE, and the G1 fixture is the case that was actually
# wrong: a checkout prefix containing `__test__`. A fixture set that only contains healthy
# input would agree under both the broken and the fixed code and would prove nothing.
#
# DEGRADES, IT DOES NOT DEMAND (roborev round 7, H1). With both tools it compares them. With
# ONE it still exercises that one against every fixture via PROPERTY checks — invariants of the
# transformation rather than golden output — and DECLARES, per fixture and in the summary, that
# the differential half did not run. It never requires both tools, because the production
# parsers do not: demanding them turned a valid python3-only host into a mandatory-component
# FAILURE, which is the same false-red class G1 fixed, reintroduced by G1's own test.
# The counts are reported separately and affirmatively, so "0 divergences found" can never be
# read off a run that performed 0 comparisons.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
[ -r "$GATE" ] || { echo "FAIL: cannot read $GATE" >&2; exit 1; }

# TOOL AVAILABILITY, AND WHY THIS IS NOT "REQUIRE BOTH" (roborev round 7, H1).
#
# The first cut of this file demanded BOTH jq and python3 and exited non-zero otherwise. That
# made the MANDATORY tooling-tests component FAIL on a valid python3-only host — because the
# UNUSED jq implementation could not be compared. The production parsers support either tool
# INDEPENDENTLY, so the harness had invented a provisioning requirement the shipped code does
# not have. The irony is the lesson and it is recorded here deliberately: G1 fixed a false red
# that hit python3-only hosts, and the fix introduced a HARD FAIL on single-tool hosts — same
# population, worse outcome, in the guard built to make the gate trustworthy. "A lane that reds
# on correct input is the lane agents learn to waive" (CLAUDE.md) applies to self-tests too.
#
# So the work is split in two, and what ran is DECLARED either way:
#   * PROPERTY checks run against EVERY AVAILABLE implementation, always. They assert
#     invariants of the transformation rather than golden output, so a single-tool host gets
#     REAL COVERAGE rather than a skip — and they are not a weaker consolation prize: the
#     "normalised suite path must not still contain the anchor" property CATCHES THE ACTUAL G1
#     DEFECT on its own, with one tool.
#   * The DIFFERENTIAL comparison runs only when both tools exist, because comparing one
#     implementation against itself is not a comparison.
# NEITHER tool present is a FAIL: nothing can be tested, and the production parsers would fail
# on that host too.
have_jq=0; have_py=0
command -v jq >/dev/null 2>&1 && have_jq=1
command -v python3 >/dev/null 2>&1 && have_py=1
if [ "$have_jq" -ne 1 ] && [ "$have_py" -ne 1 ]; then
  echo "FAIL: neither jq nor python3 is present, so NO parser implementation can be exercised." >&2
  echo "  This is a hard failure rather than a skip: the production parsers need one of the two," >&2
  echo "  so a host with neither cannot run the lane this test covers either." >&2
  exit 1
fi
DIFFERENTIAL=0
[ "$have_jq" -eq 1 ] && [ "$have_py" -eq 1 ] && DIFFERENTIAL=1

# Source the anchor and every named implementation OUT OF THE REAL GATE SCRIPT — never a copy,
# which would pass while the shipped parser rotted.
eval "$(sed -n '/^NB_TEST_ANCHOR=/p' "$GATE")"
[ -n "${NB_TEST_ANCHOR:-}" ] || { echo "FAIL: could not extract NB_TEST_ANCHOR from $GATE" >&2; exit 1; }
for fn in _jest_json_suite_counts_jq _jest_json_suite_counts_py \
          _package_integration_target_ids_jq _package_integration_target_ids_py \
          _package_declared_features_jq _package_declared_features_py; do
  src=$(sed -n "/^$fn() {/,/^}$/p" "$GATE")
  [ -n "$src" ] || { echo "FAIL: could not extract $fn from $GATE — renamed or reshaped; this test must not pass having compared nothing" >&2; exit 1; }
  eval "$src" || { echo "FAIL: extracted $fn does not parse" >&2; exit 1; }
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/parserparity.XXXXXX") || exit 1
trap 'rm -rf "$WORK"' EXIT
PASS=0; FAIL=0
DIFF_N=0     # differential comparisons actually PERFORMED
PROP_N=0     # property checks actually PERFORMED
SKIPPED_N=0  # differential comparisons NOT performed for want of a tool
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
skipped() { printf 'skip - %s\n' "$1"; SKIPPED_N=$((SKIPPED_N + 1)); }

echo "tools: jq=$have_jq python3=$have_py -> differential comparison $([ "$DIFFERENTIAL" -eq 1 ] && echo ENABLED || echo 'DISABLED (only one implementation is runnable here)')"
echo

# ---------------------------------------------------------------------------
# PROPERTY CHECKS — invariants of the transformation, asserted against every AVAILABLE
# implementation. Not golden output: a golden would re-derive the transformation, which is the
# very thing differential testing exists to avoid. These are the checks that give a SINGLE-TOOL
# host real coverage.
# ---------------------------------------------------------------------------
# prop_jest <impl-fn> <label> <json>
prop_jest() {
  local fn="$1" lbl="$2" j="$3" out line nf errs=""
  out=$("$fn" "$j" 2>/dev/null) || { bad "$lbl — implementation exited non-zero"; return; }
  PROP_N=$((PROP_N + 1))
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    nf=$(printf '%s' "$line" | awk -F'\t' '{print NF}')
    [ "$nf" = 2 ] || errs="$errs [$line] has $nf TSV fields, want 2;"
    # THE G1 INVARIANT: a normalised suite path must not still contain the anchor. This alone
    # catches the defect that started all of this, with one tool and no comparison.
    case "${line%%	*}" in
      *"$NB_TEST_ANCHOR"*) errs="$errs [$line] still contains the anchor $NB_TEST_ANCHOR — normalisation did not strip it;" ;;
    esac
    # DELIBERATELY NOT asserted: "the path must be RELATIVE". That is NOT an invariant — for an
    # input carrying no anchor at all both implementations correctly emit the raw absolute name
    # (the `NO anchor in the path` fixture below pins that identical fallback), so the check
    # red on correct output the first time it ran. Removed rather than special-cased: an
    # invariant that needs an exception per fixture is not an invariant, and the anchor-free
    # check above is the one that actually catches G1 — the divergent python3 output was
    # `proj/bindings/node/__test__/value.test.js`, which CONTAINS the anchor.
    case "${line##*	}" in
      ''|*[!0-9]*) errs="$errs [$line] passed-count is not a number;" ;;
    esac
  done <<EOF
$out
EOF
  if [ -z "$errs" ]; then ok "$lbl"; else bad "$lbl —$errs"; fi
}
# prop_ids <impl-fn> <label> <meta> <pkg>
prop_ids() {
  local fn="$1" lbl="$2" meta="$3" pkg="$4" out line nf errs=""
  out=$("$fn" "$meta" "$pkg" 2>/dev/null) || { ok "$lbl (implementation refused, as expected for this input)"; PROP_N=$((PROP_N + 1)); return; }
  PROP_N=$((PROP_N + 1))
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    nf=$(printf '%s' "$line" | awk -F'\t' '{print NF}')
    [ "$nf" = 3 ] || errs="$errs [$line] has $nf TSV fields, want 3;"
    case "$line" in
      *Cargo.toml*) errs="$errs [$line] leaks a manifest path;" ;;
    esac
  done <<EOF
$out
EOF
  if [ -z "$errs" ]; then ok "$lbl"; else bad "$lbl —$errs"; fi
}
# prop_feats <impl-fn> <label> <meta> <pkg> — output must be SORTED and UNIQUE, since the
# consumer subtracts it from a resolved set and relies on a stable order.
prop_feats() {
  local fn="$1" lbl="$2" meta="$3" pkg="$4" out
  out=$("$fn" "$meta" "$pkg" 2>/dev/null) || { ok "$lbl (implementation refused, as expected for this input)"; PROP_N=$((PROP_N + 1)); return; }
  PROP_N=$((PROP_N + 1))
  if [ "$out" = "$(printf '%s' "$out" | sort -u)" ]; then ok "$lbl"; else bad "$lbl — output is not sorted/unique: [$out]"; fi
}

# cmp_pair <name> <jq-fn> <py-fn> <args...> — run both, require byte-identical stdout AND the
# same exit status. Status matters as much as output: one branch failing while the other
# succeeds is a divergence even when the successful one's output looks right.
cmp_pair() {
  local nm="$1" fjq="$2" fpy="$3"; shift 3
  local o1 o2 r1 r2 kind

  # The property KIND is inferred from the implementation's own name, so adding a fixture needs
  # no extra argument and cannot silently skip its property check by omission.
  case "$fjq" in
    _jest_json_suite_counts_*)            kind=jest ;;
    _package_integration_target_ids_*)    kind=ids ;;
    _package_declared_features_*)         kind=feats ;;
    *) bad "$nm — HARNESS ERROR: no property kind known for implementation '$fjq'; refusing to run a comparison with no property coverage behind it"; return ;;
  esac

  # PROPERTIES FIRST, on every AVAILABLE implementation. These run on a single-tool host, and
  # they are what makes such a host covered rather than skipped.
  [ "$have_jq" -eq 1 ] && "prop_$kind" "$fjq" "$nm [prop: jq]" "$@"
  [ "$have_py" -eq 1 ] && "prop_$kind" "$fpy" "$nm [prop: python3]" "$@"

  if [ "$DIFFERENTIAL" -ne 1 ]; then
    # DECLARED, never silent, and counted separately from the comparisons that RAN — see the
    # affirmative summary at the end. A pass line that cannot distinguish "compared and agreed"
    # from "could not compare" is the vacuous-green shape this whole file exists to prevent.
    skipped "$nm — differential comparison NOT PERFORMED (jq=$have_jq python3=$have_py; only one implementation is runnable here). The available implementation WAS exercised by the property checks above."
    return
  fi
  DIFF_N=$((DIFF_N + 1))
  o1=$("$fjq" "$@" 2>/dev/null); r1=$?
  o2=$("$fpy" "$@" 2>/dev/null); r2=$?
  if [ "$r1" -ne "$r2" ]; then
    bad "$nm — EXIT STATUS diverges: jq=$r1 python3=$r2"
    return
  fi
  if [ "$o1" != "$o2" ]; then
    bad "$nm — OUTPUT diverges:
--- jq ---
$o1
--- python3 ---
$o2"
    return
  fi
  ok "$nm (both branches identical; exit=$r1)"
}

# ---------------------------------------------------------------------------
# PAIR 1 — _jest_json_suite_counts. THE G1 CASE IS FIRST.
# ---------------------------------------------------------------------------
# A checkout prefix that itself contains a `__test__` directory. VERIFIED to discriminate: the
# pre-fix pair returned `value.test.js` (jq) vs
# `proj/bindings/node/__test__/value.test.js` (python3) on this exact input.
cat > "$WORK/prefix.json" <<'JSON'
{"testResults":[
 {"name":"/home/dev/__test__/proj/bindings/node/__test__/value.test.js","assertionResults":[{"status":"passed"},{"status":"pending"}]},
 {"name":"/repo/bindings/node/__test__/sub/deep.test.js","assertionResults":[{"status":"passed"},{"status":"passed"}]},
 {"name":"/repo/bindings/node/__test__/a.test.js","assertionResults":[]}
]}
JSON
cmp_pair "jest counts: __test__ INSIDE the checkout prefix (the G1 case)" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/prefix.json"

# The anchor appearing TWICE in its full three-component form — the residual ambiguity the
# longer anchor makes implausible but does not make impossible. Both must resolve it the same
# way (last occurrence), whatever that way is.
cat > "$WORK/twice.json" <<'JSON'
{"testResults":[
 {"name":"/x/bindings/node/__test__/y/bindings/node/__test__/z.test.js","assertionResults":[{"status":"passed"}]}
]}
JSON
cmp_pair "jest counts: the full anchor occurring TWICE" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/twice.json"

# No anchor at all — both must fall back identically rather than one emitting a bare basename.
cat > "$WORK/noanchor.json" <<'JSON'
{"testResults":[{"name":"/somewhere/else/odd.test.js","assertionResults":[{"status":"passed"}]}]}
JSON
cmp_pair "jest counts: NO anchor in the path (identical fallback)" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/noanchor.json"

# Every assertion status jest emits, so the passed-count filter is compared over the whole
# vocabulary rather than just passed/pending.
cat > "$WORK/statuses.json" <<'JSON'
{"testResults":[{"name":"/r/bindings/node/__test__/s.test.js","assertionResults":[
 {"status":"passed"},{"status":"failed"},{"status":"pending"},{"status":"skipped"},
 {"status":"todo"},{"status":"disabled"},{"status":"focused"}]}]}
JSON
cmp_pair "jest counts: all assertion statuses" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/statuses.json"

# Structural edge cases: absent assertionResults, absent testResults, empty testResults.
printf '{"testResults":[{"name":"/r/bindings/node/__test__/n.test.js"}]}' > "$WORK/noassert.json"
cmp_pair "jest counts: assertionResults ABSENT" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/noassert.json"
printf '{"testResults":[]}' > "$WORK/empty.json"
cmp_pair "jest counts: testResults EMPTY" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/empty.json"
printf '{}' > "$WORK/bare.json"
cmp_pair "jest counts: testResults ABSENT" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/bare.json"

# A path with a SPACE, which this repo tracks under docs/ and which a tab-delimited format
# must carry unchanged.
cat > "$WORK/space.json" <<'JSON'
{"testResults":[{"name":"/r/bindings/node/__test__/has space.test.js","assertionResults":[{"status":"passed"}]}]}
JSON
cmp_pair "jest counts: a suite path containing a SPACE" \
  _jest_json_suite_counts_jq _jest_json_suite_counts_py "$WORK/space.json"

# ---------------------------------------------------------------------------
# PAIR 2 — _package_integration_target_ids
# ---------------------------------------------------------------------------
META_BASIC='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","features":{"b":[],"a":[],"default":[]},"targets":[
 {"name":"lib_t","kind":["lib"],"src_path":"/w/p/src/lib.rs"},
 {"name":"file_t","kind":["test"],"src_path":"/w/p/tests/file_t.rs","required-features":[]},
 {"name":"dir_t","kind":["test"],"src_path":"/w/p/tests/dir_t/main.rs","required-features":["x","y"]},
 {"name":"outside_t","kind":["test"],"src_path":"/w/p/other/outside_t.rs"},
 {"name":"abs_t","kind":["test"],"src_path":"/elsewhere/abs_t.rs"}
]}]}'
cmp_pair "target ids: file/dir/outside/absolute src_path shapes" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_BASIC" p

cmp_pair "target ids: package NOT PRESENT (both must fail identically)" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_BASIC" nosuch

META_DUP='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","targets":[]},{"name":"p","manifest_path":"/w/q/Cargo.toml","targets":[]}]}'
cmp_pair "target ids: package name appearing TWICE (ambiguous — both must refuse)" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_DUP" p

META_NOTARGETS='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","targets":[{"name":"l","kind":["cdylib"],"src_path":"/w/p/src/lib.rs"}]}]}'
cmp_pair "target ids: NO test targets (zero is a real answer, not a failure)" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_NOTARGETS" p

META_MISSINGFIELDS='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","targets":[{"name":"t","kind":["test"]}]}]}'
cmp_pair "target ids: src_path ABSENT" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_MISSINGFIELDS" p

META_NOMANIFEST='{"packages":[{"name":"p","targets":[{"name":"t","kind":["test"],"src_path":"/w/p/tests/t.rs"}]}]}'
cmp_pair "target ids: manifest_path ABSENT (empty root)" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_NOMANIFEST" p

META_MULTIKIND='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","targets":[{"name":"t","kind":["test","bench"],"src_path":"/w/p/tests/t.rs"}]}]}'
cmp_pair "target ids: multi-kind target including test" \
  _package_integration_target_ids_jq _package_integration_target_ids_py "$META_MULTIKIND" p

# ---------------------------------------------------------------------------
# PAIR 3 — _package_declared_features. Ordering is the thing at risk: jq's `keys` sorts by
# unicode codepoint and python's sorted() on str does too, but that is asserted here, not
# assumed, and the fixture is deliberately out of order and mixed-case.
# ---------------------------------------------------------------------------
META_FEATS='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","features":{"zeta":[],"Alpha":[],"beta":[],"_under":[],"a-dash":[],"10num":[]},"targets":[]}]}'
cmp_pair "declared features: ORDERING over mixed case/punctuation/digits" \
  _package_declared_features_jq _package_declared_features_py "$META_FEATS" p
cmp_pair "declared features: package NOT PRESENT (both must fail identically)" \
  _package_declared_features_jq _package_declared_features_py "$META_FEATS" nosuch
META_NOFEATS='{"packages":[{"name":"p","manifest_path":"/w/p/Cargo.toml","targets":[]}]}'
cmp_pair "declared features: features table ABSENT (empty is a real answer)" \
  _package_declared_features_jq _package_declared_features_py "$META_NOFEATS" p

# ---------------------------------------------------------------------------
# The REAL workspace, as a final control: fixtures only cover shapes someone thought of.
# ---------------------------------------------------------------------------
if command -v cargo >/dev/null 2>&1; then
  REAL_META=$(cargo metadata --format-version 1 --no-deps --manifest-path "$SCRIPT_DIR/../../Cargo.toml" 2>/dev/null)
  if [ -n "$REAL_META" ]; then
    for pkg in cqlite-ffi-common cqlite-node cqlite-core cqlite-cli cqlite-flight; do
      cmp_pair "REAL metadata: target ids for $pkg" \
        _package_integration_target_ids_jq _package_integration_target_ids_py "$REAL_META" "$pkg"
      cmp_pair "REAL metadata: declared features for $pkg" \
        _package_declared_features_jq _package_declared_features_py "$REAL_META" "$pkg"
    done
  else
    echo "note: cargo metadata unavailable; the real-workspace control did not run (fixtures only)" >&2
  fi
else
  echo "note: cargo absent; the real-workspace control did not run (fixtures only)" >&2
fi

echo
# AFFIRMATIVE ACCOUNTING. "0 divergences found" must never be reachable from "0 comparisons
# performed", so the two are reported as separate numbers and the skipped count is named
# explicitly. A reader can tell, from this line alone, whether the differential half ran.
echo "differential comparisons PERFORMED: $DIFF_N"
echo "property checks PERFORMED:          $PROP_N"
echo "differential comparisons SKIPPED:   $SKIPPED_N$([ "$SKIPPED_N" -gt 0 ] && echo "  (jq=$have_jq python3=$have_py — only one implementation runnable on this host)")"
if [ "$DIFFERENTIAL" -eq 1 ] && [ "$DIFF_N" -eq 0 ]; then
  echo "FAIL - both tools are present but ZERO comparisons ran; a parity claim with no comparison behind it is exactly the vacuous green this file exists to prevent" >&2
  FAIL=$((FAIL + 1))
fi
if [ "$PROP_N" -eq 0 ]; then
  echo "FAIL - ZERO property checks ran, so no implementation was exercised at all" >&2
  FAIL=$((FAIL + 1))
fi
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
