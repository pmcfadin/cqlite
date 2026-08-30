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
# REQUIRES BOTH TOOLS, and SAYS SO rather than asserting parity it could not measure: with only
# one of jq/python3 present the affected pairs are reported UNMEASURED and the run exits 2. A
# green over an unexercised comparison is the vacuous pass this whole issue exists to remove.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
[ -r "$GATE" ] || { echo "FAIL: cannot read $GATE" >&2; exit 1; }

have_jq=0; have_py=0
command -v jq >/dev/null 2>&1 && have_jq=1
command -v python3 >/dev/null 2>&1 && have_py=1
if [ "$have_jq" -ne 1 ] || [ "$have_py" -ne 1 ]; then
  echo "UNMEASURED: this test needs BOTH jq and python3 to compare the two branches" >&2
  echo "  (jq present=$have_jq, python3 present=$have_py). Reporting UNMEASURED rather than" >&2
  echo "  PASS: a parity claim over a comparison that never ran is a vacuous green." >&2
  exit 2
fi

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
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# cmp_pair <name> <jq-fn> <py-fn> <args...> — run both, require byte-identical stdout AND the
# same exit status. Status matters as much as output: one branch failing while the other
# succeeds is a divergence even when the successful one's output looks right.
cmp_pair() {
  local nm="$1" fjq="$2" fpy="$3"; shift 3
  local o1 o2 r1 r2
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
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
