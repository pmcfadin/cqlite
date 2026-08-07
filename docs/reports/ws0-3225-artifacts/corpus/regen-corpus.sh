#!/usr/bin/env bash
# WS0 benchmark corpus regeneration for CQLite issue #3225 §2.
#
# #3217's corpus binaries are gitignored and gone from every box, so the C(N)
# sweep needs the corpus rebuilt to the SAME recipe. This script is a
# path-parameterized adaptation of the committed #3026 generator
# (docs/reports/ws0-3026-artifacts/ws0-corpus/gen-corpus.sh), whose hardcoded
# /home/ubuntu/ws0 root and JDK path made it unrunnable here. Recipe, profile,
# stress arguments and compaction steps are otherwise UNCHANGED — that is the
# whole point: a different recipe would not be comparable to #3217's table.
#
# Recipe (docs/reports/ws0-3217-artifacts/corpus/corpus-provenance.txt):
#   gen-corpus.sh 200000 375 6 96 3 96 16 2 10 8 50000
#   = 200,000 partitions x (seq 2 x event_time 10) = 20 rows/partition
#   -> 3,999,890 rows (cassandra-stress' uniform clustering draws collide slightly,
#      so the achieved count is a hair under the nominal 4,000,000 — the #3217 and
#      #3100 runs both landed on exactly 3,999,890)
#   nb-16-big, ONE SSTable after flush+compact, stock LZ4 / 16 KiB chunks.
#
# Usage:
#   bash regen-corpus.sh [--root <dir>] [--skip-load] [--help]
#
#     --root       corpus root (default $WS0_ROOT, else /data/ws0)
#     --skip-load  skip the cassandra-stress load; re-run flush/compact/measure/stage
#                  against whatever is already in the data dir. Use to resume after a
#                  crash in the measurement or staging phase WITHOUT reloading 4M rows.
#
# Prerequisites, all checked fail-closed before anything is started:
#   - a JDK (JAVA_HOME or /usr/lib/jvm/java-17-openjdk-amd64)
#   - apache-cassandra-5.0.8 unpacked under <root>
#   - a python interpreter with cassandra-driver (<root>/venv/bin/python)
#   - >= 12 GB free on <root>'s filesystem
#
# LONG RUNNING (~30-60 min). Launch detached and poll the log.
set -euo pipefail

WS0_ROOT="${WS0_ROOT:-/data/ws0}"
SKIP_LOAD=0
while [ $# -gt 0 ]; do
  case "$1" in
    --root) [ $# -ge 2 ] || { echo "ERROR: --root needs a value" >&2; exit 2; }; WS0_ROOT="$2"; shift 2 ;;
    --skip-load) SKIP_LOAD=1; shift ;;
    -h|--help) sed -n '2,36p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "ERROR: unrecognized argument '$1'" >&2; exit 2 ;;
  esac
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
SRC026="$REPO_ROOT/docs/reports/ws0-3026-artifacts/ws0-corpus"

CAS="$WS0_ROOT/apache-cassandra-5.0.8"
DATA_ROOT="$WS0_ROOT/cassandra-data"
WORK="$WS0_ROOT/ws0-corpus"
STAGE_ROOT="$WS0_ROOT/ws0-h2h/datasets/sstables"
PY="${WS0_PY:-$WS0_ROOT/venv/bin/python}"

# Recipe constants — do not tune these to make a geometry "match". A divergence
# is reported, never engineered away.
PARTS=200000 PAYLOAD=375 PAYLOAD_POP=6
BLOBA=96 BLOBA_POP=3 BLOBB=96 BLOBB_POP=16
SEQ_CL=2 ET_CL=10 THREADS=8 BATCH=50000

log() { printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { printf '[%s] ERROR: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; exit 1; }

# ------------------------------------------------------------- preflight -----
: "${JAVA_HOME:=/usr/lib/jvm/java-17-openjdk-amd64}"
[ -x "$JAVA_HOME/bin/java" ] || die "no JDK at JAVA_HOME=$JAVA_HOME (set JAVA_HOME)"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
[ -x "$CAS/bin/cassandra" ] || die "Cassandra not unpacked at $CAS"
[ -x "$CAS/bin/nodetool" ] || die "nodetool missing at $CAS/bin/nodetool"
[ -x "$CAS/tools/bin/cassandra-stress" ] || die "cassandra-stress missing at $CAS/tools/bin"
[ -x "$PY" ] || die "no python at $PY (WS0_PY overrides); needs cassandra-driver"
"$PY" -c 'import cassandra' 2>/dev/null || die "$PY cannot import cassandra-driver"
for f in ws0-profile.yaml cql.py measure-sstable.py fullscan.py; do
  [ -f "$SRC026/$f" ] || die "committed input missing: $SRC026/$f"
done
AVAIL_KB="$(df -Pk "$WS0_ROOT" | awk 'NR==2 {print $4}')"
[ "$AVAIL_KB" -ge 12000000 ] || die "only ${AVAIL_KB} KB free on $WS0_ROOT; need >= 12 GB"

mkdir -p "$WORK" "$DATA_ROOT"/{data,commitlog,hints,saved_caches} "$WS0_ROOT/cassandra-logs" "$STAGE_ROOT"
cp -f "$SRC026"/{ws0-profile.yaml,cql.py,measure-sstable.py,fullscan.py} "$WORK/"

log "root=$WS0_ROOT java=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
log "cassandra=$CAS  python=$PY  skip_load=$SKIP_LOAD"

# ------------------------------------------------- cassandra.yaml (paths) ----
# The ONLY deltas from stock are paths + listen/rpc addresses, exactly as
# #3026/#3100/#3217 (docs/reports/ws0-3026-artifacts/ws0-corpus/cassandra.yaml.diff).
YAML="$CAS/conf/cassandra.yaml"
[ -f "$YAML.stock" ] || cp -p "$YAML" "$YAML.stock"
"$PY" - "$YAML.stock" "$YAML" "$DATA_ROOT" <<'PY'
import re, sys
src, dst, data_root = sys.argv[1:4]
s = open(src).read()

# The four directory settings ship COMMENTED OUT in stock 5.0.8 (they default to
# $CASSANDRA_HOME-relative paths — that default is exactly what bit #3100, whose
# corpus landed under the tarball dir). Match either the commented or the
# uncommented spelling and fail closed if neither is found: a silently-unpatched
# path would put the corpus somewhere other than where this script then reads it.
scalars = [
    ('hints_directory',        f'{data_root}/hints'),
    ('commitlog_directory',    f'{data_root}/commitlog'),
    ('saved_caches_directory', f'{data_root}/saved_caches'),
    ('listen_address',         '127.0.0.1'),
    ('rpc_address',            '127.0.0.1'),
]
for key, val in scalars:
    pat = r'(?m)^#?\s*%s:.*$' % re.escape(key)
    s, n = re.subn(pat, f'{key}: {val}', s, count=1)
    if n != 1:
        sys.exit(f"FAIL: key {key!r} matched {n} times in stock cassandra.yaml")

# data_file_directories is a YAML list; replace the whole block, commented or not.
s, n = re.subn(r'(?m)^#\s*data_file_directories:\n(?:#\s+- .*\n)+',
               f'data_file_directories:\n    - {data_root}/data\n', s, count=1)
if n != 1:
    s, n = re.subn(r'(?m)^data_file_directories:\n(?:\s+- .*\n)+',
                   f'data_file_directories:\n    - {data_root}/data\n', s, count=1)
if n != 1:
    sys.exit("FAIL: could not rewrite data_file_directories in stock cassandra.yaml")

for key in ('hints_directory', 'commitlog_directory', 'saved_caches_directory',
            'data_file_directories', 'listen_address', 'rpc_address'):
    if not re.search(r'(?m)^%s:' % re.escape(key), s):
        sys.exit(f"FAIL: {key} is not active (uncommented) after patching")
open(dst, 'w').write(s)
print("cassandra.yaml patched (paths + addresses only)")
PY

# ----------------------------------------------------------- start daemon ----
# MAX_HEAP_SIZE=8G is the one deliberate non-path deviation, same as every prior
# round: stock auto-sizing picks ~15.4 GiB on this 30 GiB no-swap box and gets
# OOM-killed (#3026 recorded that kill).
start_daemon() {
  if "$CAS/bin/nodetool" status >/dev/null 2>&1; then
    log "daemon already up"; return 0
  fi
  log "starting cassandra (MAX_HEAP_SIZE=8G)"
  ( cd "$CAS" && MAX_HEAP_SIZE=8G setsid nohup bin/cassandra -f \
      > "$WS0_ROOT/cassandra-logs/stdout.log" 2>&1 < /dev/null & disown ) || true
  local i
  for i in $(seq 1 60); do
    "$CAS/bin/nodetool" status >/dev/null 2>&1 && { log "daemon up after $((i*5))s"; return 0; }
    sleep 5
  done
  die "cassandra did not come up within 300s; see $WS0_ROOT/cassandra-logs/stdout.log"
}
start_daemon
"$CAS/bin/nodetool" info | grep -i "heap memory" || true

# ------------------------------------------------------------- stress load ---
TAG="p${PARTS}-pl${PAYLOAD}x${PAYLOAD_POP}-ba${BLOBA}x${BLOBA_POP}-bb${BLOBB}x${BLOBB_POP}-cl${SEQ_CL}x${ET_CL}"
PROF="$WORK/.gen-${TAG}.yaml"

if [ "$SKIP_LOAD" -eq 0 ]; then
  "$PY" - "$WORK/ws0-profile.yaml" "$PROF" \
    "$PAYLOAD" "$PAYLOAD_POP" "$BLOBA" "$BLOBA_POP" "$BLOBB" "$BLOBB_POP" "$SEQ_CL" "$ET_CL" <<'PY'
import sys
src, dst, pl, plp, ba, bap, bb, bbp, seqcl, etcl = sys.argv[1:11]
s = open(src).read()
s = s.replace("CAL_PAYLOAD_POP", plp).replace("CAL_PAYLOAD", pl)
s = s.replace("CAL_BLOBA_POP", bap).replace("CAL_BLOBA", ba)
s = s.replace("CAL_BLOBB_POP", bbp).replace("CAL_BLOBB", bb)
s = s.replace("CAL_SEQ_CLUSTER", seqcl).replace("CAL_ET_CLUSTER", etcl)
assert "CAL_" not in s, [l for l in s.splitlines() if "CAL_" in l]
open(dst, "w").write(s)
PY
  log "profile: $PROF"

  log "clean slate: DROP KEYSPACE ws0 + clearsnapshot"
  "$PY" "$WORK/cql.py" "DROP KEYSPACE IF EXISTS ws0" >/dev/null 2>&1 || true
  sleep 2
  "$CAS/bin/nodetool" clearsnapshot --all >/dev/null 2>&1 || true
  rm -rf "$DATA_ROOT/data/ws0" 2>/dev/null || true

  ROWS_PER_PART=$((SEQ_CL * ET_CL))
  log "loading ${PARTS} partitions x ${ROWS_PER_PART} rows (~$((PARTS*ROWS_PER_PART)) nominal rows)"
  STRESS_JVM_OPTS="-Xms512M -Xmx4G"   # NOT exported: nodetool shares JVM_OPTS and would die
  START=1; BATCHNO=0
  while [ "$START" -le "$PARTS" ]; do
    END=$((START + BATCH - 1)); [ "$END" -gt "$PARTS" ] && END=$PARTS
    N=$((END - START + 1)); BATCHNO=$((BATCHNO + 1))
    log "--- batch ${BATCHNO}: partitions ${START}..${END} (n=${N})"
    env JVM_OPTS="$STRESS_JVM_OPTS" \
      "$CAS/tools/bin/cassandra-stress" user "profile=$PROF" 'ops(insert=1)' \
      n="$N" no-warmup cl=ONE -pop "seq=${START}..${END}" -rate "threads=${THREADS}" \
      -node 127.0.0.1 > "$WORK/.stress-${TAG}-b${BATCHNO}.log" 2>&1 \
      || { echo "STRESS FAILED"; tail -40 "$WORK/.stress-${TAG}-b${BATCHNO}.log"; exit 1; }
    grep -E "^Total (partitions|errors)|^Op rate|^Row rate" "$WORK/.stress-${TAG}-b${BATCHNO}.log" | sed 's/^/      /'
    "$CAS/bin/nodetool" flush ws0 events
    free -h | sed -n 2p | sed 's/^/      mem: /'
    START=$((END + 1))
  done
else
  log "--skip-load: reusing the existing ws0.events data"
fi

# --------------------------------------------------- flush + compact to one ---
log "nodetool flush + compact -> exactly ONE SSTable"
"$CAS/bin/nodetool" flush ws0 events
time "$CAS/bin/nodetool" compact ws0 events

# The glob's EMPTY result must not look like a success: `dirname ""` is ".", and "." IS
# a directory, so the -d check below would pass and the failure would surface later as
# a confusing count. Resolve the file first and require it by name.
LARGEST_DATA_DB="$(ls -S "$DATA_ROOT"/data/ws0/events-*/*-Data.db 2>/dev/null | head -1)"
[ -n "$LARGEST_DATA_DB" ] || die "no *-Data.db under $DATA_ROOT/data/ws0/events-* — nothing was written"
SRC_DIR="$(dirname "$LARGEST_DATA_DB")"
[ -d "$SRC_DIR" ] || die "no ws0.events SSTable directory under $DATA_ROOT/data/ws0"
NDATA="$(ls "$SRC_DIR"/*-Data.db | wc -l)"
log "sstable dir: $SRC_DIR   Data.db count = $NDATA"
[ "$NDATA" -eq 1 ] || die "expected exactly 1 Data.db after compact, found $NDATA — the recipe requires a single SSTable"
DATA_DB="$(ls "$SRC_DIR"/*-Data.db)"
GEN_FMT="$(basename "$DATA_DB" | sed 's/-Data\.db$//')"
# The FORMAT is a hard requirement and is asserted: `nb` is the Cassandra 5.0 BIG
# version this whole corpus recipe (and every downstream comparison against #3217)
# assumes, and a different one is a different on-disk layout, not a variation.
case "$GEN_FMT" in
  nb-*-big) ;;
  *) die "SSTable is '$GEN_FMT'; the recipe requires the Cassandra 5.0 BIG format nb-<gen>-big" ;;
esac
# The GENERATION NUMBER is deliberately NOT fatal: it counts flush/compaction events,
# so it moves with the batching of the load and is not a property of the data. It is
# recorded — and compared against #3217 by compare-geometry.py's categorical table,
# which is where a divergence belongs, labelled rather than silently tolerated here.
if [ "$GEN_FMT" != "nb-16-big" ]; then
  log "NOTE: generation is $GEN_FMT, #3217 recorded nb-16-big. The generation number counts"
  log "      flush/compact events, not content; compare-geometry.py labels it explicitly."
fi
log "generation/format: $GEN_FMT   (format nb-*-big asserted; #3217 recorded nb-16-big)"

# ------------------------------------------------------- stage for the sweep --
TABLE_DIR="$STAGE_ROOT/ws0/$(basename "$SRC_DIR")"
rm -rf "$TABLE_DIR"; mkdir -p "$TABLE_DIR"
cp -p "$SRC_DIR"/* "$TABLE_DIR"/
SRC_SHA="$(sha256sum "$DATA_DB" | awk '{print $1}')"
STAGED_SHA="$(sha256sum "$TABLE_DIR/$(basename "$DATA_DB")" | awk '{print $1}')"
[ "$SRC_SHA" = "$STAGED_SHA" ] || die "staged Data.db sha256 differs from source ($STAGED_SHA vs $SRC_SHA)"
log "staged -> $TABLE_DIR  (sha256 verified identical to source)"
log "sha256(Data.db) = $SRC_SHA"

# ------------------------------------------------------------- measurement ---
# Primary row-count oracle: sstablemetadata totalRows (reads the on-disk stats).
ROWS_META="$("$CAS/tools/bin/sstablemetadata" "$DATA_DB" 2>/dev/null | awk -F': *' '/totalRows/ {print $2; exit}')"
[ -n "$ROWS_META" ] || die "could not read totalRows from sstablemetadata"
log "rows (sstablemetadata totalRows) = $ROWS_META"

# INDEPENDENT oracle: a live token-range full scan through the CQL driver — a
# different code path (server-side read, not the writer's stats header).
# `SELECT count(*)` over the whole table server-side-times-out at this size,
# which is why the ring is split (documented in #3026's rerun.sh).
log "independent row-count oracle: fullscan.py 512 token ranges (this takes minutes)"
"$PY" "$WORK/fullscan.py" 512 | tee "$HERE/corpus-fullscan.txt"
ROWS_SCAN="$(awk -F': *' '/SCAN rows counted/ {print $2}' "$HERE/corpus-fullscan.txt" | tr -d ' ')"
# An unparseable oracle is not an agreeing oracle. Without this, an empty ROWS_SCAN
# would flow into the comparison below as "", which is only ever a disagreement — but
# for the wrong reason, and the diagnostic would blame the data instead of the parse.
[ -n "$ROWS_SCAN" ] || die "could not parse 'SCAN rows counted' from $HERE/corpus-fullscan.txt — the independent oracle produced no number"
log "rows (fullscan oracle) = $ROWS_SCAN"
# The two oracles disagreeing is a CORPUS defect, not a note. This used to log a
# '***' line and carry on to exit 0, so a corpus whose independent row count did not
# reproduce would be staged, swept and published as if it had. The report claims these
# two agree exactly; that claim is now enforced by the script that produces them.
if [ "$ROWS_META" != "$ROWS_SCAN" ]; then
  die "ORACLE DISAGREEMENT: sstablemetadata=$ROWS_META fullscan=$ROWS_SCAN. These are independent code paths over the same table; a disagreement means the corpus is not what the recipe describes. Refusing to stage it."
fi
log "row-count oracles agree exactly: $ROWS_META"

"$PY" "$WORK/measure-sstable.py" "$SRC_DIR"/*-CompressionInfo.db "$ROWS_META" \
  | tee "$HERE/corpus-measure.txt"
# These four are COMMITTED artifacts that compare-geometry.py and the report parse.
# '|| true' plus '2>/dev/null' meant a failed tool published an EMPTY file and the run
# still succeeded — absence of evidence rendered as evidence. Each must now produce a
# non-empty file or the run stops.
emit_artifact() { # <out-file> <command...>
  local out="$1"; shift
  "$@" > "$out" || die "failed to produce $out: $* exited non-zero"
  [ -s "$out" ] || die "produced an EMPTY $out — a committed artifact with no content is not a measurement"
}
emit_artifact "$HERE/corpus-sstablemetadata.txt" "$CAS/tools/bin/sstablemetadata" "$DATA_DB"
emit_artifact "$HERE/corpus-tablestats.txt" "$CAS/bin/nodetool" tablestats ws0.events
emit_artifact "$HERE/corpus-tablehistograms.txt" "$CAS/bin/nodetool" tablehistograms ws0.events
emit_artifact "$HERE/corpus-sha-staged.txt" sha256sum "$TABLE_DIR"/*
# The one line every downstream consumer (analyze-3225.py, compare-geometry.py) reads.
SHA_DATA_LINES="$(grep -c -- '-Data\.db$' "$HERE/corpus-sha-staged.txt" || true)"
[ "$SHA_DATA_LINES" -eq 1 ] \
  || die "corpus-sha-staged.txt names $SHA_DATA_LINES *-Data.db line(s); the recipe requires exactly 1"

# ------------------------------------------------------------ stop the node --
# A live Cassandra daemon on the sweep box would compete for CPU with the very
# measurement it produced the corpus for. Stopping it is part of the recipe.
log "stopping cassandra daemon"
CAS_PID="$(pgrep -f 'org.apache.cassandra.service.CassandraDaemon' | head -1 || true)"
if [ -n "$CAS_PID" ]; then
  kill -TERM "$CAS_PID" 2>/dev/null || true
  for _ in $(seq 1 60); do kill -0 "$CAS_PID" 2>/dev/null || break; sleep 2; done
  kill -0 "$CAS_PID" 2>/dev/null && { log "daemon did not stop on TERM; sending KILL"; kill -9 "$CAS_PID"; } || true
fi
pgrep -f 'org.apache.cassandra.service.CassandraDaemon' >/dev/null \
  && die "a CassandraDaemon is STILL running — the sweep box is not quiet" \
  || log "cassandra stopped"

cat <<EOF

================ #3225 CORPUS READY ================
staged dataset dir : $STAGE_ROOT      (serve with: cqlite-flight --data-dir <that>)
staged table dir   : $TABLE_DIR
generation/format  : $GEN_FMT
Data.db count      : $NDATA
sha256(Data.db)    : $SRC_SHA
rows (metadata)    : $ROWS_META
rows (fullscan)    : $ROWS_SCAN
measurements       : $HERE/corpus-measure.txt
====================================================
EOF
