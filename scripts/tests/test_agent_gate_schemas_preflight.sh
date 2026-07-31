#!/usr/bin/env bash
# Regression test for issues #3148 / #3131: the agent-gate fixture preflight must
# validate the COMMITTED CQL schemas root, not just the fetched SSTable corpus, and
# the schemas root must be resolved CHECKOUT-RELATIVE rather than by climbing `..`
# from $CQLITE_DATASETS_ROOT.
#
# POSITIVE CONTROL is the point of this file (#3148 AC (c)). The #3148 gap survived
# because the preflight was only ever observed passing on a good layout: "STATUS: OK"
# was never proven to be a *decision*. So every case below drives a layout the
# preflight must REJECT and asserts the rejection text, alongside the happy path.
#
# Fast + hermetic by design: the FULL-gate cases exit at the preflight (before any
# cargo component), and every dataset/schemas root is a temp dir — no real corpus, no
# network, no Docker.
#
# Run standalone:   bash scripts/tests/test_agent_gate_schemas_preflight.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

# #2751 defense-in-depth: never let an inherited summary path be clobbered — every
# invocation below pins its own.
unset AGENT_GATE_SUMMARY_FILE
# A CQLITE_SCHEMAS_ROOT exported by the caller would silently redirect the "checkout
# default" cases; scrub it so this file tests the committed contract, not the shell.
unset CQLITE_SCHEMAS_ROOT

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-schemas.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# The six canonical .cql the gate's dataset-backed components consume. Kept here as a
# LITERAL list rather than read back from agent-gate.sh: if someone shrinks
# CANONICAL_SCHEMA_FILES, this file must redden, not agree with the shrink.
CANONICAL=(basic-types.cql da-test.cql time-series.cql wide-table-bti.cql collections.cql wide-rows.cql)

# A dataset root whose canonical corpus IS present, so the #2078 corpus guard is
# satisfied and the run reaches the #3148 schemas guard.
ds_corpus="$tmp/ds-corpus"
mkdir -p "$ds_corpus/sstables/test_basic/simple_table-0001"
: >"$ds_corpus/sstables/test_basic/simple_table-0001/nb-1-big-Data.db"

# Hostile schemas roots.
schemas_empty="$tmp/schemas-empty"                 # readable dir, zero fixtures
mkdir -p "$schemas_empty"
schemas_partial="$tmp/schemas-partial"             # SOME fixtures — the case a
mkdir -p "$schemas_partial"                        # directory-existence check misses
: >"$schemas_partial/basic-types.cql"
: >"$schemas_partial/collections.cql"

hook_field() {  # hook_field <field> <output>
  printf '%s\n' "$2" | grep "^$1: " | sed "s/^$1: //"
}

# ---------------------------------------------------------------------------
# 1. Hidden --preflight-schemas hook: the PURE decision, both ways.
# ---------------------------------------------------------------------------
good_out=$(bash "$GATE" --preflight-schemas 2>/dev/null)
if [ "$(hook_field STATUS "$good_out")" = OK ] \
   && [ "$(hook_field ROOT "$good_out")" = "$REPO/test-data/schemas" ] \
   && [ "$(hook_field SOURCE "$good_out")" = "checkout-relative" ]; then
  ok "3148-hook-good: checkout resolves the committed schemas root -> STATUS OK"
else
  bad "3148-hook-good: expected STATUS OK + checkout-relative $REPO/test-data/schemas"
  printf '%s\n' "$good_out"
fi

empty_out=$(CQLITE_SCHEMAS_ROOT="$schemas_empty" bash "$GATE" --preflight-schemas 2>/dev/null)
empty_missing=$(hook_field MISSING "$empty_out")
missing_all=1
for f in "${CANONICAL[@]}"; do
  grep -qw -- "$f" <<<"$empty_missing" || missing_all=0
done
if [ "$(hook_field STATUS "$empty_out")" = FAIL ] && [ "$missing_all" -eq 1 ] \
   && [ "$(hook_field SOURCE "$empty_out")" = "CQLITE_SCHEMAS_ROOT override" ]; then
  ok "3148-hook-empty: schemas-less root -> STATUS FAIL naming all ${#CANONICAL[@]} unreadable .cql"
else
  bad "3148-hook-empty: expected STATUS FAIL listing every canonical .cql"
  printf '%s\n' "$empty_out"
fi

# A directory-EXISTENCE check would pass this root: it exists and holds two of the six.
# Only a per-FILE readability check rejects it, naming exactly the four absentees.
partial_out=$(CQLITE_SCHEMAS_ROOT="$schemas_partial" bash "$GATE" --preflight-schemas 2>/dev/null)
partial_missing=$(hook_field MISSING "$partial_out")
if [ "$(hook_field STATUS "$partial_out")" = FAIL ] \
   && grep -qw -- 'da-test.cql' <<<"$partial_missing" \
   && grep -qw -- 'wide-rows.cql' <<<"$partial_missing" \
   && ! grep -qw -- 'basic-types.cql' <<<"$partial_missing" \
   && ! grep -qw -- 'collections.cql' <<<"$partial_missing"; then
  ok "3148-hook-partial: per-FILE readability rejects a present-but-incomplete root"
else
  bad "3148-hook-partial: expected FAIL naming only the absent files (got '$partial_missing')"
fi

# ---------------------------------------------------------------------------
# 2. The FULL gate FAILS CLOSED, with a marker DISTINGUISHABLE from #2078's.
#    apply_schemas_preflight fires before any cargo component, so this is fast.
# ---------------------------------------------------------------------------
full_fail="$tmp/3148-full-fail.txt"
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
  CQLITE_SCHEMAS_ROOT="$schemas_empty" AGENT_GATE_SUMMARY_FILE="$full_fail" \
  bash "$GATE" >/dev/null 2>&1
full_rc=$?
if [ "$full_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$full_fail" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$full_fail" 2>/dev/null \
   && ! grep -q "^RESULT: PASS" "$full_fail" 2>/dev/null; then
  ok "3148-full-fail: FULL gate FAILs CLOSED on a schemas-less root (marker + RESULT: FAIL, no cargo)"
else
  bad "3148-full-fail: expected non-zero exit + missing-schemas FAIL-CLOSED + RESULT: FAIL (rc=$full_rc)"
  cat "$full_fail" 2>/dev/null
fi

# The two causes must be separable in a pasted block: a schemas failure must NEVER
# stamp #2078's corpus marker (the corpus here is deliberately complete).
if ! grep -q "missing-fixtures:" "$full_fail" 2>/dev/null; then
  ok "3148-marker-distinct: schemas failure carries no missing-fixtures line (#2078 vs #3148 separable)"
else
  bad "3148-marker-distinct: the schemas failure also stamped #2078's missing-fixtures marker"
fi

# AC (b): the failure text names the exact expected ABSOLUTE path and the fix command.
if grep -q "$schemas_empty/basic-types.cql" "$full_fail" 2>/dev/null \
   && grep -q "unset CQLITE_SCHEMAS_ROOT" "$full_fail" 2>/dev/null \
   && grep -q "restore --source=HEAD -- test-data/schemas" "$full_fail" 2>/dev/null; then
  ok "3148-remedy: block names the exact absolute path + both fix commands"
else
  bad "3148-remedy: expected the absolute .cql path and the remedy commands in the block"
  cat "$full_fail" 2>/dev/null
fi

# ---------------------------------------------------------------------------
# 3. AC (g): --lite and --only stay LENIENT (unchanged from #2078's contract).
# ---------------------------------------------------------------------------
lite_block="$tmp/3148-lite.txt"
CQLITE_DATASETS_ROOT="$ds_corpus" CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  AGENT_GATE_SUMMARY_FILE="$lite_block" \
  bash "$GATE" --lite --emit-summary-selftest >/dev/null 2>&1
lite_rc=$?
if [ "$lite_rc" -eq 0 ] \
   && grep -q "AGENT-GATE LITE SUMMARY" "$lite_block" 2>/dev/null \
   && ! grep -q "missing-schemas:" "$lite_block" 2>/dev/null; then
  ok "3148-lite: --lite unaffected by an unreachable schemas root (clean LITE block, no marker)"
else
  bad "3148-lite: --lite must stay lenient (rc=$lite_rc)"
  cat "$lite_block" 2>/dev/null
fi

# The arg dispatch is a single `case "$1"`, so `--only X --preflight-schemas` is not
# expressible; the hook's optional 2nd arg seeds ONLY, exercising the SAME pure
# decision the real --only run consumes. `core-tests` is deliberately a DATASET
# component: even the selection that most needs schemas must stay lenient under --only.
only_status=$(CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  bash "$GATE" --preflight-schemas core-tests 2>/dev/null | grep '^STATUS:' | sed 's/^STATUS: //')
if [ "$only_status" = OK ]; then
  ok "3148-only: --only stays lenient (STATUS OK even with the schemas root unreachable)"
else
  bad "3148-only: expected the --only selection to stay lenient (got '$only_status')"
fi

# ---------------------------------------------------------------------------
# 4. AC (f): the symlink trap is GONE, not papered over.
#
#    `join("..")` is not a lexical parent at the syscall level: the kernel resolves
#    `datasets/..` against the SYMLINK TARGET's parent. So a corpus reached through a
#    symlinked `datasets` used to mis-resolve `datasets/../schemas` silently. The fix
#    removes all `..` climbing, which is only meaningful if the schemas decision is
#    INDEPENDENT of $CQLITE_DATASETS_ROOT — asserted directly here across three
#    datasets layouts (real dir / symlink-to-elsewhere / nonexistent).
# ---------------------------------------------------------------------------
sym_parent="$tmp/sym-parent"
mkdir -p "$sym_parent"
ln -s "$ds_corpus" "$sym_parent/datasets"
indep=1
for layout in "$ds_corpus" "$sym_parent/datasets" "$tmp/does-not-exist/datasets"; do
  st=$(CQLITE_DATASETS_ROOT="$layout" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^STATUS:' | sed 's/^STATUS: //')
  rt=$(CQLITE_DATASETS_ROOT="$layout" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^ROOT:' | sed 's/^ROOT: //')
  { [ "$st" = OK ] && [ "$rt" = "$REPO/test-data/schemas" ]; } || indep=0
done
if [ "$indep" -eq 1 ]; then
  ok "3148-symlink-independence: schemas root is identical for real/symlinked/absent datasets roots"
else
  bad "3148-symlink-independence: the schemas root still varies with CQLITE_DATASETS_ROOT"
fi

# The structural half of AC (f)/(d): no code may reintroduce the `..` climb. Comment
# text is exempt (the doc comments deliberately quote the retired idiom); a real
# expression is a hard failure. `grep -v` on a leading `//`/`#` comment marker after
# the `path:line:` prefix is what makes that distinction.
climbs=$(grep -rn --include='*.rs' 'join("\.\./schemas")' "$REPO" 2>/dev/null \
  | grep -v ':[0-9]*: *//' || true)
if [ -z "$climbs" ]; then
  ok "3148-no-dotdot-climb: zero open-coded join(\"../schemas\") expressions in Rust code"
else
  bad "3148-no-dotdot-climb: an open-coded ../schemas climb was reintroduced:"
  printf '%s\n' "$climbs"
fi

# ---------------------------------------------------------------------------
# 5. AC (d)/(e): ONE shared resolution file, included by every historical site.
# ---------------------------------------------------------------------------
shared="$REPO/test-data/support/fixture_roots.rs"
if [ -f "$shared" ] \
   && grep -q 'pub fn schemas_root_resolved' "$shared" \
   && grep -q 'pub fn datasets_root_if_present' "$shared" \
   && grep -q 'pub fn datasets_root' "$shared"; then
  ok "3148-shared-file: test-data/support/fixture_roots.rs defines the single contract"
else
  bad "3148-shared-file: the shared fixture-roots module is missing or incomplete"
fi

sites_ok=1
for site in \
  cqlite-core/benches/fixtures/mod.rs \
  cqlite-core/tests/dead_cache_delete_tests.rs \
  cqlite-core/tests/observability_correctness.rs \
  cqlite-cli/benches/export_csv.rs
do
  grep -q 'test-data/support/fixture_roots.rs' "$REPO/$site" || { sites_ok=0; echo "   (no include: $site)"; }
done
if [ "$sites_ok" -eq 1 ]; then
  ok "3148-all-sites: all four historical call sites include the shared module"
else
  bad "3148-all-sites: a call site no longer resolves roots through the shared module"
fi

# No second copy of the resolution may reappear WHERE #3148 removed one. Scope: the two
# bench trees plus the three files that carried the divergent `datasets_root()` copies.
# The wider `cqlite-core/tests/**` and `src/**` inline suites keep their own ad-hoc env
# reads — out of scope for #3148 (which names three copies), so asserting over them
# would be a scope claim this change does not make.
dupes=$(grep -rln --include='*.rs' 'env::var("CQLITE_DATASETS_ROOT")' \
  "$REPO/cqlite-core/benches" "$REPO/cqlite-cli/benches" \
  "$REPO/cqlite-core/tests/dead_cache_delete_tests.rs" \
  "$REPO/cqlite-core/tests/observability_correctness.rs" 2>/dev/null || true)
if [ -z "$dupes" ]; then
  ok "3148-no-dupe-root: no bench / migrated test re-reads CQLITE_DATASETS_ROOT directly"
else
  bad "3148-no-dupe-root: a datasets-root resolution copy reappeared:"
  printf '%s\n' "$dupes"
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
