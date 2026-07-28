#!/usr/bin/env bash
# Self-test for the issue-#3068 perf-corpus generator + manifest writer.
#
# Three properties, all of which were real defects:
#
#   1. TABLES is validated BEFORE any destructive or expensive work. An
#      unvalidated typo (TABLES=medum) used to start a container, generate no
#      tables, and then overwrite the COMMITTED manifest with an empty `tables`
#      array -- silent corruption of a provenance artifact.
#   2. write-perf-corpus-manifest.py REFUSES to emit a manifest with an empty
#      table list, so that corruption cannot happen from any caller.
#   3. Stale-corpus pruning is tightly scoped. It deletes MULTI-GB paths, so it
#      must only ever touch "<selected-table>-<32 hex>" directories that are
#      direct children of $CORPUS_ROOT/sstables/<keyspace> -- never a symlink,
#      never an unrelated name, never something outside the corpus root.
#      Exercised through the generator's --prune-dry-run hook, which enumerates
#      candidates and deletes nothing.
#
# Hermetic: no docker, no sudo, no cassandra, no network, no datasets. The
# generator is only ever invoked with --validate-only / --prune-dry-run, which
# exit before the container is started.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEN="$REPO_ROOT/test-data/scripts/gen-perf-corpus-3068.sh"
MANIFEST_PY="$REPO_ROOT/test-data/scripts/write-perf-corpus-manifest.py"

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$GEN" ] || { echo "FAIL - missing $GEN"; exit 1; }
[ -f "$MANIFEST_PY" ] || { echo "FAIL - missing $MANIFEST_PY"; exit 1; }

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# ------------------------------------------------------- TABLES validation ----
# Valid selections resolve to the expected table set and run nothing.
check_valid() { # check_valid <TABLES> <expected table list>
  local tables="$1" expect="$2" out
  out=$(TABLES="$tables" CORPUS_ROOT="$TMP/corpus" bash "$GEN" --validate-only 2>&1)
  if [ $? -eq 0 ] && grep -q "VALIDATE-OK tables=$expect " <<<"$out"; then
    pass "TABLES=$tables resolves to '$expect'"
  else
    fail "TABLES=$tables: expected 'VALIDATE-OK tables=$expect' (out: $out)"
  fi
}
check_valid both   "medium_700b wide_4kb"
check_valid medium "medium_700b"
check_valid wide   "wide_4kb"

# An invalid TABLES must fail closed, non-zero, with a clear message, and must
# not have written ANYTHING (no stress profiles, no corpus dir, no manifest).
for bad in medum "" MEDIUM "both wide" medium,wide all; do
  workdir="$TMP/bad-$RANDOM"
  mkdir -p "$workdir"
  out=$(TABLES="$bad" CORPUS_ROOT="$workdir/corpus" bash "$GEN" --validate-only 2>&1); rc=$?
  leftovers=$(find "$workdir" -mindepth 1 | wc -l | tr -d ' ')
  if [ "$rc" -ne 0 ] && grep -q "invalid TABLES" <<<"$out" && [ "$leftovers" = "0" ]; then
    pass "rejects TABLES='$bad' before doing any work"
  else
    fail "TABLES='$bad': expected non-zero + 'invalid TABLES' + no writes (rc=$rc, leftovers=$leftovers, out: $out)"
  fi
done

# A relative or empty CORPUS_ROOT is refused (nothing may be deleted relative to
# an unknown cwd).
for badroot in "" "relative/path" "/"; do
  out=$(TABLES=both CORPUS_ROOT="$badroot" bash "$GEN" --validate-only 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "CORPUS_ROOT" <<<"$out"; then
    pass "rejects CORPUS_ROOT='$badroot'"
  else
    fail "CORPUS_ROOT='$badroot': expected non-zero + a CORPUS_ROOT message (rc=$rc, out: $out)"
  fi
done

# ------------------------------------------- manifest writer: empty --table ----
if command -v python3 >/dev/null 2>&1; then
  out=$(python3 "$MANIFEST_PY" --corpus-root "$TMP" --keyspace perf_3068 \
          --image cassandra:5.0.2 --out "$TMP/should-not-exist.json" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "empty" <<<"$out" && [ ! -e "$TMP/should-not-exist.json" ]; then
    pass "manifest writer refuses an empty --table list (writes no file)"
  else
    fail "manifest writer with no --table: expected non-zero + no file (rc=$rc, out: $out)"
  fi

  out=$(python3 "$MANIFEST_PY" --corpus-root "$TMP" --keyspace perf_3068 \
          --image cassandra:5.0.2 --table "medium_700b" \
          --out "$TMP/should-not-exist2.json" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "malformed" <<<"$out"; then
    pass "manifest writer refuses a --table without a directory"
  else
    fail "manifest writer with a bare table name: expected non-zero (rc=$rc, out: $out)"
  fi
else
  echo "skip - python3 unavailable, manifest-writer assertions skipped"
fi

# ------------------------------------------------ prune scope (dry run only) ---
# A realistic corpus keyspace dir plus every kind of neighbour the pruner must
# leave alone.
CORPUS="$TMP/corpus"
KSDIR="$CORPUS/sstables/perf_3068"
OUTSIDE="$TMP/outside-the-corpus"
mkdir -p "$KSDIR" "$OUTSIDE/precious"
UUID_A="8cc9d0708a2711f1a82281d620fbe729"
UUID_B="90c037f08a2711f1a82281d620fbe729"
mkdir -p "$KSDIR/medium_700b-$UUID_A" \
         "$KSDIR/medium_700b-$UUID_B" \
         "$KSDIR/wide_4kb-$UUID_A" \
         "$KSDIR/medium_700b-backup" \
         "$KSDIR/medium_700b-$UUID_A/nested/medium_700b-$UUID_B" \
         "$KSDIR/other_table-$UUID_A"
touch "$KSDIR/medium_700b-$UUID_A-notes.txt"
ln -s "$OUTSIDE/precious" "$KSDIR/medium_700b-${UUID_A//8/a}"

out=$(TABLES=both CORPUS_ROOT="$CORPUS" bash "$GEN" --prune-dry-run 2>&1); rc=$?
would=$(grep '^WOULD-PRUNE ' <<<"$out" | sed 's/^WOULD-PRUNE //' | sort)
expected=$(printf '%s\n' \
  "$KSDIR/medium_700b-$UUID_A" \
  "$KSDIR/medium_700b-$UUID_B" \
  "$KSDIR/wide_4kb-$UUID_A" | sort)

if [ "$rc" -eq 0 ] && [ "$would" = "$expected" ]; then
  pass "prune targets exactly the <selected-table>-<uuid> dirs"
else
  fail "prune candidate set wrong (rc=$rc)
  got:
$would
  expected:
$expected"
fi

# A dry run must delete nothing at all.
for must_exist in \
  "$KSDIR/medium_700b-$UUID_A" "$KSDIR/medium_700b-$UUID_B" "$KSDIR/wide_4kb-$UUID_A" \
  "$KSDIR/medium_700b-backup" "$KSDIR/other_table-$UUID_A" \
  "$KSDIR/medium_700b-$UUID_A-notes.txt" "$OUTSIDE/precious"; do
  [ -e "$must_exist" ] || fail "--prune-dry-run deleted $must_exist"
done
pass "--prune-dry-run deletes nothing"

# Named exclusions: the non-uuid dir, another table's dir, the symlink, the
# nested dir, and the non-directory must never be candidates.
for never in \
  "$KSDIR/medium_700b-backup" \
  "$KSDIR/other_table-$UUID_A" \
  "$KSDIR/medium_700b-${UUID_A//8/a}" \
  "$KSDIR/medium_700b-$UUID_A/nested/medium_700b-$UUID_B" \
  "$KSDIR/medium_700b-$UUID_A-notes.txt" \
  "$OUTSIDE/precious"; do
  if grep -qF "WOULD-PRUNE $never" <<<"$out"; then
    fail "prune would have removed '$never'"
  else
    pass "prune does not target '${never#"$TMP"/}'"
  fi
done

# Only the SELECTED table is pruned.
out_medium=$(TABLES=medium CORPUS_ROOT="$CORPUS" bash "$GEN" --prune-dry-run 2>&1)
if grep -q "WOULD-PRUNE $KSDIR/medium_700b-$UUID_A" <<<"$out_medium" &&
   ! grep -q "WOULD-PRUNE $KSDIR/wide_4kb-" <<<"$out_medium"; then
  pass "TABLES=medium prunes only medium_700b dirs"
else
  fail "TABLES=medium pruned the wrong set (out: $out_medium)"
fi

# A missing corpus keyspace dir is a no-op, not an error.
out=$(TABLES=both CORPUS_ROOT="$TMP/never-generated" bash "$GEN" --prune-dry-run 2>&1); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q WOULD-PRUNE <<<"$out"; then
  pass "no corpus yet: prune is a no-op"
else
  fail "prune on a non-existent corpus root: expected a clean no-op (rc=$rc, out: $out)"
fi

# --------------------------------------- schema.cql capture is wired + fatal ---
# keyspace_ddl / per-table DDL can only be rebuilt offline from a captured
# schema.cql, so the generator must capture one and treat a failure as fatal.
if grep -q 'DESCRIBE KEYSPACE' "$GEN" &&
   grep -q 'cp "\$CORPUS_ROOT/schema.cql" "\$dest/schema.cql"' "$GEN"; then
  pass "generator captures DESCRIBE KEYSPACE and publishes schema.cql per table"
else
  fail "generator does not capture/publish schema.cql (manifest would not be reproducible)"
fi
if grep -A6 'capture_schema() {' "$GEN" | grep -q 'die '; then
  pass "schema capture is fail-closed"
else
  fail "schema capture does not fail closed"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "test_gen_perf_corpus_3068: ALL PASS"
  exit 0
fi
echo "test_gen_perf_corpus_3068: $fails FAILURE(S)"
exit 1
