#!/usr/bin/env bash
# capture-cassandra-verify-verdicts.sh — capture the REAL Apache Cassandra 5.0.2
# `sstableverify --extended --force` verdict for every fixture in the corruption
# corpus (issue #1236, verify-parity layer on top of epic #970 / issue #999).
#
# WHY
# ---
# The parity oracle for `cqlite-core/tests/sstable_parity_corruption_verify.rs`
# must be Cassandra's ACTUAL verdict on each fixture's exact bytes — not a
# verdict hand-encoded from reading Cassandra source (the "trust me" gap this
# layer closes). This tool runs each corrupted fixture (and the clean baseline)
# through a `cassandra:5.0.2` container's standalone offline verifier and prints
# a `clean`/`corrupt` verdict + the message that tripped.
#
# The captured verdicts are then recorded (once) into
# `corruption-manifest.yml`'s per-fixture `cassandra_verdict` /
# `verdict_parity` / `verdict_note` fields (see generate-corruption-corpus.sh).
# CI does NOT re-run this tool: the corrupted binaries regenerate deterministically
# from the committed clean sources + mutation manifest, and the Cassandra verdict
# is captured-and-committed. Re-run this tool only when a fixture's bytes change.
#
# REQUIREMENTS
#   * Docker, with the cassandra:5.0.2 image pullable.
#   * The clean source binaries (test_comp/lz4_table, test_da/wide_table) and the
#     regenerated corruption corpus present under CQLITE_DATASETS_ROOT.
#
# Usage:
#   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
#     bash test-data/scripts/capture-cassandra-verify-verdicts.sh
#
# Backs: issue #1236 (verify-parity). Pins cassandra_git_sha f278f677... (5.0.2).
#
# FAIL-CLOSED (Finding 2 / issue #1236): the only nonzero exit this tool is
# allowed to swallow is `sstableverify`'s — that exit code IS the verdict and is
# captured deliberately (errexit is locally disabled just around that call).
# Every other failure (missing live table dir, docker cp, chown, schema setup)
# is FATAL so a partial/garbage capture can never be silently committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATASETS="${CQLITE_DATASETS_ROOT:-$ROOT/datasets}"
CORPUS="$DATASETS/corruption/test_comp_corrupt"
IMAGE="${CASSANDRA_IMAGE:-cassandra:5.0.2}"
CID="${CASS_VERIFY_CID:-cass-verify-verdicts}"
OUT="${OUT:-/dev/stdout}"

fatal() { echo "[verdicts] FATAL: $*" >&2; exit 1; }

# Always tear the verifier container down, success or failure.
cleanup() { docker rm -f "$CID" >/dev/null 2>&1 || true; }
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || fatal "docker required"
[[ -d "$CORPUS" ]] || fatal "corruption corpus missing: $CORPUS (run generate-corruption-corpus.sh)"

docker rm -f "$CID" >/dev/null 2>&1 || true
echo "[verdicts] starting $IMAGE..."
docker run -d --name "$CID" \
  -e CASSANDRA_DC=dc1 -e MAX_HEAP_SIZE=512M -e HEAP_NEWSIZE=128M \
  "$IMAGE" >/dev/null

echo "[verdicts] waiting for CQL up..."
up=0
for _ in $(seq 1 90); do
  if docker exec "$CID" cqlsh -e "describe cluster" >/dev/null 2>&1; then up=1; break; fi
  sleep 10
done
[[ "$up" -eq 1 ]] || fatal "cassandra did not come up"

# Schemas MUST match the fixture generators EXACTLY so each fixture is verified
# against the same serialization header Cassandra wrote (Finding 1 / issue #1236):
#   test_comp.lz4_table  -> generate-compression-parity.sh
#   test_da.wide_table   -> gen-wide-bti.sh / test-data/schemas/wide-table-bti.cql
#                           (pk int, ck int, payload text, PRIMARY KEY (pk, ck), LZ4)
# Pipe via cqlsh stdin (not -e): a trailing newline after the final ';' makes
# `cqlsh -e` emit a spurious "no viable alternative at input ';'" and exit
# nonzero, which would now (set -e) abort the run even though the tables exist.
docker exec -i "$CID" cqlsh <<'CQL'
CREATE KEYSPACE IF NOT EXISTS test_comp WITH replication={'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS test_comp.lz4_table (pk int, ck int, body text, PRIMARY KEY (pk, ck)) WITH compression={'class':'LZ4Compressor','chunk_length_in_kb':16};
CREATE KEYSPACE IF NOT EXISTS test_da WITH replication={'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS test_da.wide_table (pk int, ck int, payload text, PRIMARY KEY (pk, ck)) WITH compression={'class':'LZ4Compressor'};
CQL

echo "[verdicts] cassandra version: $(docker exec "$CID" cassandra -v 2>/dev/null || echo '?')"
printf "fixture\tks\ttbl\tverdict\tmessage\n" > "$OUT"

run_verify() {
  # $1 srcdir (host) $2 ks $3 tbl $4 label
  local srcdir="$1" ks="$2" tbl="$3" label="$4"
  local livedir
  # A missing live table dir means schema setup failed for this table — FATAL
  # (a verdict captured against a nonexistent table would be meaningless).
  livedir=$(docker exec "$CID" sh -c "ls -d /var/lib/cassandra/data/$ks/${tbl}-* 2>/dev/null | head -1")
  [[ -n "$livedir" ]] || fatal "no live dir for $ks.$tbl (schema setup failed?)"
  docker exec "$CID" sh -c "rm -f $livedir/*-* 2>/dev/null || true"
  # Staging the fixture bytes into the live dir MUST succeed; a failed copy would
  # silently verify the (now-empty) live dir instead of the fixture.
  docker cp "$srcdir/." "$CID:$livedir/" || fatal "docker cp $srcdir -> $ks.$tbl failed"
  docker exec "$CID" sh -c "rm -f $livedir/*.jsonl $livedir/*.db.txt $livedir/*.yml $livedir/*.md $livedir/*.txt.* 2>/dev/null || true"
  docker exec "$CID" chown -R cassandra:cassandra "$livedir" || fatal "chown of $livedir failed"
  local raw rc verdict msg
  # The ONLY place errexit is intentionally relaxed: sstableverify's nonzero exit
  # IS the verdict we want to capture, not a script failure.
  set +e
  raw=$(docker exec "$CID" sh -c "sstableverify -e -f $ks $tbl 2>&1"); rc=$?
  set -e
  if [[ $rc -eq 0 ]] && ! echo "$raw" | grep -qiE 'Corrupt|Exception|Error|mismatch|FAILED'; then
    verdict=clean
  else
    verdict=corrupt
  fi
  msg=$(echo "$raw" | grep -iE 'succeeded|Corrupt|Exception|mismatch|EOF|missing|Invalid' | head -2 | tr '\n\t' '  ')
  printf "%s\t%s\t%s\t%s\t%s\n" "$label" "$ks" "$tbl" "$verdict" "${msg:-rc=$rc}" >> "$OUT"
  echo "[verdicts] $label => $verdict (rc=$rc)" >&2
}

# Clean baseline (whole clean lz4_table generation).
CLEANSRC=$(ls -d "$DATASETS"/sstables/test_comp/lz4_table-* 2>/dev/null | head -1 || true)
[[ -n "$CLEANSRC" ]] && run_verify "$CLEANSRC" test_comp lz4_table CLEAN_BASELINE_lz4

for fx in "$CORPUS"/*/; do
  fx="${fx%/}"; name="$(basename "$fx")"
  ls "$fx"/*-Data.db >/dev/null 2>&1 || continue
  if ls "$fx"/da-*-Data.db >/dev/null 2>&1; then
    run_verify "$fx" test_da wide_table "$name"
  else
    run_verify "$fx" test_comp lz4_table "$name"
  fi
done

# Container teardown is handled by the EXIT trap (cleanup).
echo "[verdicts] done." >&2
