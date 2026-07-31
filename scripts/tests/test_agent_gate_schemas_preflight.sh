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

# A bare `-r` test accepts a DIRECTORY named `basic-types.cql`; the Rust side asks for a
# readable REGULAR file (`readable_file`). Both sides must ask the same question or the
# gate can certify a layout the tests reject (reviewer nit N7 / roborev finding 2).
schemas_dirtrap="$tmp/schemas-dirtrap"
mkdir -p "$schemas_dirtrap"
for f in "${CANONICAL[@]}"; do mkdir -p "$schemas_dirtrap/$f"; done
dirtrap_out=$(CQLITE_SCHEMAS_ROOT="$schemas_dirtrap" bash "$GATE" --preflight-schemas 2>/dev/null)
dirtrap_missing=$(hook_field MISSING "$dirtrap_out")
dirtrap_all=1
for f in "${CANONICAL[@]}"; do
  grep -qw -- "$f" <<<"$dirtrap_missing" || dirtrap_all=0
done
if [ "$(hook_field STATUS "$dirtrap_out")" = FAIL ] && [ "$dirtrap_all" -eq 1 ]; then
  ok "3148-hook-dirtrap: a DIRECTORY named like a .cql is not a readable regular file"
else
  bad "3148-hook-dirtrap: expected FAIL for directories named like the fixtures (got '$dirtrap_missing')"
fi

# ---------------------------------------------------------------------------
# 1b. A RELATIVE CQLITE_SCHEMAS_ROOT is REJECTED, not resolved (blocker B1).
#
#     The gate evaluates a relative override with CWD = REPO_ROOT; cargo runs each test
#     binary with CWD = the PACKAGE dir. Resolving it let the gate stamp
#     `schemas: 6/6 … under packaged/schemas (override)` while the tests silently fell
#     back to the checkout — the SUMMARY certifying root A for a run that used root B,
#     which IS #3148's defect. So the decision must be FAIL, the reported ROOT must be
#     the checkout (never the relative string dressed up as absolute), and the reason
#     must be named.
# ---------------------------------------------------------------------------
rel_out=$(CQLITE_SCHEMAS_ROOT="packaged/schemas" bash "$GATE" --preflight-schemas 2>/dev/null)
if [ "$(hook_field STATUS "$rel_out")" = FAIL ] \
   && [ "$(hook_field ROOT "$rel_out")" = "$REPO/test-data/schemas" ] \
   && [ "$(hook_field SOURCE "$rel_out")" = "CQLITE_SCHEMAS_ROOT override REJECTED" ] \
   && grep -q 'must be an ABSOLUTE path' <<<"$(hook_field REJECT "$rel_out")"; then
  ok "3148-relative-override: a relative CQLITE_SCHEMAS_ROOT is rejected, not resolved"
else
  bad "3148-relative-override: expected FAIL + REJECTED source + the absolute-path reason"
  printf '%s\n' "$rel_out"
fi

# …and it must not smuggle the relative string into the report as an "absolute path".
if ! grep -q 'expected absolute path: packaged/schemas' <<<"$rel_out"; then
  ok "3148-relative-override: no relative path is ever labelled absolute (AC (b))"
else
  bad "3148-relative-override: a relative path was reported as the expected absolute path"
fi

# Every relative shape, not just the bare one; and a blank/whitespace value is NOT an
# override at all (a scripting accident), matching the Rust side's `trim().is_empty()`.
rel_shapes_ok=1
for raw in './schemas' '../schemas' 'a/b/schemas'; do
  st=$(CQLITE_SCHEMAS_ROOT="$raw" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^STATUS:' | sed 's/^STATUS: //')
  [ "$st" = FAIL ] || { rel_shapes_ok=0; echo "   (not rejected: $raw -> $st)"; }
done
for raw in '' '   '; do
  st=$(CQLITE_SCHEMAS_ROOT="$raw" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^STATUS:' | sed 's/^STATUS: //')
  [ "$st" = OK ] || { rel_shapes_ok=0; echo "   (blank treated as an override: '$raw' -> $st)"; }
done
if [ "$rel_shapes_ok" -eq 1 ]; then
  ok "3148-relative-shapes: every relative form rejected; blank/whitespace is not an override"
else
  bad "3148-relative-shapes: relative/blank handling diverges from the Rust resolver"
fi

# The FULL gate must FAIL CLOSED on the relative override too — with its own reason, not
# a misleading "missing files" list (the checkout's fixtures are in fact complete).
rel_full="$tmp/3148-rel-full.txt"
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
  CQLITE_SCHEMAS_ROOT="packaged/schemas" AGENT_GATE_SUMMARY_FILE="$rel_full" \
  bash "$GATE" >/dev/null 2>&1
rel_full_rc=$?
if [ "$rel_full_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$rel_full" 2>/dev/null \
   && grep -q "relative CQLITE_SCHEMAS_ROOT rejected" "$rel_full" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$rel_full" 2>/dev/null \
   && ! grep -q "^schemas: " "$rel_full" 2>/dev/null; then
  ok "3148-relative-full: FULL gate FAILs CLOSED on a relative override and stamps no positive schemas line"
else
  bad "3148-relative-full: expected fail-closed with the relative-override reason (rc=$rel_full_rc)"
  cat "$rel_full" 2>/dev/null
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
# 2b. A POSITIVE line must never assert a check that did not run.
#
#     `_schemas_status` returns OK unconditionally under --only/--lite (leniency, AC (g)),
#     so the OK branch used to stamp `schemas: 6/6 canonical .cql readable under <root>`
#     for a check that NEVER RAN — #3148's own misleading `STATUS: OK`, one mode over. The
#     assertion is therefore on the SUMMARY TEXT the real apply_schemas_preflight stamps,
#     driven through the --preflight-schemas-line hook (a real `--only core-tests` run
#     would spend minutes in cargo before printing anything).
# ---------------------------------------------------------------------------
line_field() { printf '%s\n' "$1" | grep '^SCHEMAS_LINE: ' | sed 's/^SCHEMAS_LINE: //'; }

only_line_out=$(CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  bash "$GATE" --preflight-schemas-line core-tests 2>/dev/null)
only_line_rc=$?
only_line=$(line_field "$only_line_out")
if [ "$only_line_rc" -eq 0 ] \
   && ! grep -q 'readable' <<<"$only_line" \
   && grep -q '^schemas: not checked' <<<"$only_line" \
   && grep -q -- '--only core-tests' <<<"$only_line"; then
  ok "3148-only-no-false-positive: an --only run stamps an explicit 'not checked', never 'N/N readable'"
else
  bad "3148-only-no-false-positive: a lenient --only run must not assert readability (rc=$only_line_rc, line: '$only_line')"
fi

# Same for a RELATIVE override under --only. This ALSO pins the second half of the same
# class: the REJECT branch was not governed by _schemas_status, so it FAILed even a
# lenient --only run — the effectful guard diverging from the pure decision it is
# documented to consume.
rel_only_out=$(CQLITE_SCHEMAS_ROOT="packaged/schemas" \
  bash "$GATE" --preflight-schemas-line core-tests 2>/dev/null)
rel_only_rc=$?
rel_only=$(line_field "$rel_only_out")
if [ "$rel_only_rc" -eq 0 ] && grep -q '^schemas: not checked' <<<"$rel_only"; then
  ok "3148-only-reject-lenient: --only stays lenient for a relative override too (no strict-path drift)"
else
  bad "3148-only-reject-lenient: the reject branch is not governed by the lenient mode check (rc=$rel_only_rc, line: '$rel_only')"
fi

# …and the POSITIVE line must still appear when the check DID run, otherwise the two
# asserts above would be satisfied by simply never stamping anything.
full_line=$(line_field "$(bash "$GATE" --preflight-schemas-line 2>/dev/null)")
if grep -q "^schemas: 6/6 canonical .cql readable under $REPO/test-data/schemas" <<<"$full_line"; then
  ok "3148-full-positive-line: a FULL-mode check that ran stamps the positive N/N readable line"
else
  bad "3148-full-positive-line: expected the positive line for a real check (got '$full_line')"
fi

# ---------------------------------------------------------------------------
# 3. AC (g): --lite and --only stay LENIENT (unchanged from #2078's contract).
# ---------------------------------------------------------------------------
lite_block="$tmp/3148-lite.txt"
CQLITE_DATASETS_ROOT="$ds_corpus" CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  AGENT_GATE_SUMMARY_FILE="$lite_block" \
  bash "$GATE" --lite --emit-summary-selftest >/dev/null 2>&1
lite_rc=$?
# `! grep '^schemas: '` as well as the marker: a LITE block must carry NO schemas line at
# all — neither a failure marker nor a POSITIVE assertion. run_lite always exits before
# apply_schemas_preflight, so SCHEMAS_LINE is never stamped; this pins that, so a future
# call-site move cannot start asserting readability in a mode that never checked it.
if [ "$lite_rc" -eq 0 ] \
   && grep -q "AGENT-GATE LITE SUMMARY" "$lite_block" 2>/dev/null \
   && ! grep -q "missing-schemas:" "$lite_block" 2>/dev/null \
   && ! grep -q "^schemas: " "$lite_block" 2>/dev/null; then
  ok "3148-lite: --lite unaffected by an unreachable schemas root (no schemas line at all)"
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
   && grep -q 'pub fn resolve_schemas_root' "$shared" \
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

# ---------------------------------------------------------------------------
# 6. #3131 items 1-2: fetch-datasets.sh must never report success while leaving a
#    root an operator cannot use, and must PRINT the export line it guarantees.
#    Driven through --verify-only, which performs no download/extraction/removal —
#    so this stays hermetic and never touches the real corpus or the tree.
# ---------------------------------------------------------------------------
FETCH="$REPO/test-data/scripts/fetch-datasets.sh"

# 6a. Hollow root (exists, empty): must FAIL LOUDLY with a remedy — never exit 0.
hollow="$tmp/hollow/datasets"
mkdir -p "$hollow"
hollow_out=$(CQLITE_DATASETS_ROOT="$hollow" bash "$FETCH" --verify-only 2>&1)
hollow_rc=$?
if [ "$hollow_rc" -ne 0 ] \
   && grep -q "does not hold a usable dataset corpus" <<<"$hollow_out" \
   && grep -q "remedy: re-run this script with the pin cleared" <<<"$hollow_out"; then
  ok "3131-hollow-root: an unusable root exits non-zero with a remedy (never a green no-op)"
else
  bad "3131-hollow-root: expected non-zero + remedy text (rc=$hollow_rc)"
  printf '%s\n' "$hollow_out"
fi

# 6a-bis. --verify-only must CREATE NOTHING (blocker B2). The first cut of case 6a
#         pre-`mkdir`ed its hollow root, which made it BLIND to exactly this bug:
#         `canonicalize_dataset_root` runs `mkdir -p "${parent}"` before the mode
#         dispatch, so probing a root under a nonexistent parent silently created that
#         parent and then reported the root unusable. The root here is therefore
#         deliberately NOT pre-created, and the assertion is on the filesystem, not on
#         the message.
absent_parent="$tmp/verify-nomutate"
absent_root="$absent_parent/v4/datasets"
nomutate_out=$(CQLITE_DATASETS_ROOT="$absent_root" bash "$FETCH" --verify-only 2>&1)
nomutate_rc=$?
if [ "$nomutate_rc" -ne 0 ] && [ ! -e "$absent_parent" ]; then
  ok "3131-verify-no-mutation: --verify-only creates nothing, even a missing parent dir"
else
  bad "3131-verify-no-mutation: expected non-zero AND no filesystem mutation (rc=$nomutate_rc, created: $(ls -d "$absent_parent" 2>&1))"
  printf '%s\n' "$nomutate_out"
fi

# 6a-ter. Unrecognized arguments must be REJECTED, not silently ignored (blocker B3).
#         The default path is DESTRUCTIVE (`rm -rf "${DATASET_ROOT}"` before extraction),
#         so `--quiet --verify-only` or any typo previously skipped the probe and reached
#         the rm -rf against the operator's corpus. Asserted with a real, POPULATED root:
#         if the rejection ever regresses, this case would attempt the destructive path,
#         so the surviving fixture is itself part of the assertion.
argsafe_ok=1
argsafe_root="$tmp/argsafe/datasets"
mkdir -p "$argsafe_root/sstables/test_basic/simple_table-0001"
: >"$argsafe_root/sstables/test_basic/simple_table-0001/nb-1-big-Data.db"
for badarg in "--quiet --verify-only" "-verify-only" "--verifyonly" "verify-only" "--Verify-Only"; do
  # shellcheck disable=SC2086  # intentional word-split: some cases pass TWO arguments
  out=$(CQLITE_DATASETS_ROOT="$argsafe_root" bash "$FETCH" $badarg 2>&1)
  rc=$?
  if [ "$rc" -ne 2 ] || ! grep -q "unrecognized argument" <<<"$out"; then
    argsafe_ok=0; echo "   (not rejected with exit 2: '$badarg' -> rc=$rc)"
  fi
done
if [ ! -f "$argsafe_root/sstables/test_basic/simple_table-0001/nb-1-big-Data.db" ]; then
  argsafe_ok=0; echo "   (DESTRUCTIVE path reached: the fixture Data.db was deleted)"
fi
if [ "$argsafe_ok" -eq 1 ]; then
  ok "3131-arg-safety: every unrecognized argument exits 2 before any destructive work"
else
  bad "3131-arg-safety: an unrecognized argument was not fail-closed"
fi

# …and the recognized flag plus --help still work (a fail-closed parser that rejects its
# own flag would be a silent regression of the probe).
help_out=$(bash "$FETCH" --help 2>&1); help_rc=$?
if [ "$help_rc" -eq 0 ] && grep -q -- '--verify-only' <<<"$help_out"; then
  ok "3131-arg-safety: --help documents --verify-only and exits 0"
else
  bad "3131-arg-safety: --help should exit 0 and document the flag (rc=$help_rc)"
fi

# 6b. A root holding the required content must report success AND print the exact
#     `export CQLITE_DATASETS_ROOT=<absolute path>` line it guarantees — the missing
#     half of #3131 item 2 (the pre-fix warm path named no actionable root at all).
good="$tmp/fetch-good/datasets"
wide="$good/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
mkdir -p "$wide" "$good/sstables/test_basic/simple_table-0001"
printf 'synthetic: true\n' >"$good/metadata.yml"
printf '{}\n' >"$wide/nb-2-big-Data.db.jsonl"
for c in nb-2-big-Data.db nb-2-big-Index.db nb-2-big-Digest.crc32 nb-2-big-CompressionInfo.db; do
  : >"$wide/$c"
done
for c in nb-1-big-Data.db nb-1-big-Index.db nb-1-big-Summary.db nb-1-big-Statistics.db; do
  : >"$good/sstables/test_basic/simple_table-0001/$c"
done
good_out=$(CQLITE_DATASETS_ROOT="$good" bash "$FETCH" --verify-only 2>&1)
good_rc=$?
if [ "$good_rc" -eq 0 ] \
   && grep -q "^  export CQLITE_DATASETS_ROOT=$good$" <<<"$good_out" \
   && grep -q "Dataset root VERIFIED" <<<"$good_out"; then
  ok "3131-export-line: a usable root is confirmed and prints its exact export line"
else
  bad "3131-export-line: expected exit 0 + the verbatim export line for $good (rc=$good_rc)"
  printf '%s\n' "$good_out"
fi

# 6b-bis. The export line must be PASTEABLE, not merely printed (roborev job 8, finding
#         3). A root containing a space or a shell metacharacter would, under plain
#         interpolation, print a command that breaks (or does something else) when
#         pasted — so the promise "the exact export line" would be false exactly when it
#         matters. Asserted by EVALUATING the printed line and comparing the resulting
#         variable to the real path: the strongest available statement of "pasteable".
spacey="$tmp/fetch space & meta/datasets"
spacey_wide="$spacey/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
mkdir -p "$spacey_wide" "$spacey/sstables/test_basic/simple_table-0001"
printf 'synthetic: true\n' >"$spacey/metadata.yml"
printf '{}\n' >"$spacey_wide/nb-2-big-Data.db.jsonl"
for c in nb-2-big-Data.db nb-2-big-Index.db nb-2-big-Digest.crc32 nb-2-big-CompressionInfo.db; do
  : >"$spacey_wide/$c"
done
for c in nb-1-big-Data.db nb-1-big-Index.db nb-1-big-Summary.db nb-1-big-Statistics.db; do
  : >"$spacey/sstables/test_basic/simple_table-0001/$c"
done
spacey_line=$(CQLITE_DATASETS_ROOT="$spacey" bash "$FETCH" --verify-only 2>/dev/null \
  | grep '^  export CQLITE_DATASETS_ROOT=' | sed 's/^  //')
spacey_eval=$(
  unset CQLITE_DATASETS_ROOT
  eval "$spacey_line" 2>/dev/null
  printf '%s' "${CQLITE_DATASETS_ROOT:-}"
)
if [ -n "$spacey_line" ] && [ "$spacey_eval" = "$spacey" ]; then
  ok "3131-export-quoting: the printed export line round-trips a path with spaces/metacharacters"
else
  bad "3131-export-quoting: pasting the line does not reproduce the root (line: '$spacey_line' -> '$spacey_eval')"
fi

# 6c. #2878 boundary: this change must NOT have touched the rm -rf /
#     restore_ci_tracked_dataset_files behavior. Both must still be present verbatim,
#     so a future reader can see the sibling defect was left to its own delivery.
if grep -q 'rm -rf "${DATASET_ROOT}"' "$FETCH" \
   && grep -q '\[ -n "${CI:-}" \] || return 0' "$FETCH"; then
  ok "3131-2878-boundary: rm -rf + restore_ci_tracked_dataset_files left untouched (#2878)"
else
  bad "3131-2878-boundary: the #2878-owned behavior was modified by this change"
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
