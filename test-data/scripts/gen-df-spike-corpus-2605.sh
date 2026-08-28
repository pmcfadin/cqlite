#!/usr/bin/env bash
# gen-df-spike-corpus-2605.sh — Generate a WIDE, MULTI-GENERATION, OVERLAPPING
# Cassandra 5.0 (nb/BIG, LZ4) corpus for the issue #2605 DataFusion
# TableProvider spike bench.
#
# WHY a bespoke corpus (the committed fixtures cannot answer the question):
#   * The largest committed/local *-Data.db is ~0.6 MB. An engine bench over
#     0.6 MB measures process startup, not scan/merge throughput.
#   * Owner comment M15 requires a WIDE + overlapping-generation shape (not the
#     RF=1 narrow shape) and that BOTH bench arms consume POST-RECONCILIATION
#     batches. Reconciliation only RUNS when several SSTables hold the SAME
#     partition keys at DIFFERENT write timestamps.
#   * A single compacted SSTable (what gen-perf-corpus-3068.sh deliberately
#     produces for the read-plane measurement) would bench a MERGE-FREE path —
#     i.e. measure the wrong thing for this spike. So this script NEVER runs
#     `nodetool compact`, keeps autocompaction disabled throughout, and ASSERTS
#     that >= MIN_DATA_DB Data.db files survive.
#
# Shape produced (defaults):
#   generation 1: WIDE_PARTITIONS=190000 partitions x 10 rows = ~1.9M rows
#                 (the R12 corpus row count), ~4.2 KB/row on disk => ~8 GB
#   generation 2: the FIRST OVERLAP_PCT=30% of the SAME cassandra-stress seed
#                 range, re-inserted => the same ~57000 partition keys rewritten
#                 with NEWER write timestamps => ~2.4 GB, and a merge iterator
#                 that must reconcile newest-wins across generations over the
#                 whole token range.
#   `nodetool flush` after each generation; NO major compaction.
#
# Overlap is MEASURED, not asserted by construction: a fixed 1% token slice is
# probed with `SELECT writetime(body)` after each generation.
#   * row count in the slice must be UNCHANGED by generation 2 (a rewrite adds
#     no rows; had gen2 written NEW partitions the count would rise ~30%), and
#   * the fraction of sampled rows whose writetime lands in the generation-2
#     window is the reported overlap fraction.
# Both are recorded in the manifest and either failing is fatal.
#
# Provenance / oracle grade: every byte is written by real Apache Cassandra
# 5.0.x inside the official image via cassandra-stress — NOT by CQLite. Two
# generation-time-only optimizations (neither changes the SSTable bytes):
#   * keyspace durable_writes = false  -> no commitlog write amplification
#   * nodetool disableautocompaction   -> STCS cannot merge the two generations
#                                          behind our back
#
# Output layout (mirrors CQLITE_DATASETS_ROOT, so CQLITE_DATASETS_ROOT=$CORPUS_ROOT
# works directly):
#   $CORPUS_ROOT/sstables/perf_2605/wide_4kb-<uuid>/nb-*-*.db + schema.cql
#   $CORPUS_ROOT/manifest-2605.json
#
# Unlike gen-perf-corpus-3068.sh this script writes NOTHING into the repo: the
# corpus is multi-GB and lives OUTSIDE it, and no committed manifest/fixture is
# touched. Only this script is committed.
#
# Usage:
#   CORPUS_ROOT=/data/corpus-2605 bash test-data/scripts/gen-df-spike-corpus-2605.sh
#   bash test-data/scripts/gen-df-spike-corpus-2605.sh --validate-only  # run nothing
set -euo pipefail

IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-df2605}"
# `${VAR-default}`, not `${VAR:-default}`: an EXPLICITLY EMPTY CORPUS_ROOT
# (typically a caller's unset variable) must fail validation, never silently
# become the default — this script deletes multi-GB paths under CORPUS_ROOT.
CORPUS_ROOT="${CORPUS_ROOT-/data/corpus-2605}"
KS="perf_2605"
TBL="wide_4kb"
# Declared replication factor. A single-node container can only STORE one
# replica, so RF affects the DDL recorded in the manifest (the M15 "RF=3"
# shape) and not the bytes; the property that makes reconciliation run is the
# generation overlap below, not RF.
RF="${RF:-3}"
# 10 rows/partition (clustering fixed(10)) => rows = partitions * 10.
WIDE_PARTITIONS="${WIDE_PARTITIONS:-190000}"
OVERLAP_PCT="${OVERLAP_PCT:-30}"
STRESS_THREADS="${STRESS_THREADS:-16}"
MAX_HEAP="${MAX_HEAP:-6G}"
HEAP_NEW="${HEAP_NEW:-1600M}"
# Leave headroom: this box also runs cargo builds for the same issue.
CPUS="${CPUS:-10}"
MEM="${MEM:-16g}"
# Fail-closed floor on surviving SSTables (see WHY above).
MIN_DATA_DB="${MIN_DATA_DB:-2}"
# Token slice used for the overlap probe, in percent of the murmur3 ring.
SLICE_PCT="${SLICE_PCT:-1}"
SLICE_LIMIT="${SLICE_LIMIT:-200000}"
NEED_GIB="${NEED_GIB:-30}"
# Plain `docker` works on this fleet box; override for hosts needing sudo.
DOCKER="${DOCKER:-docker}"

STRESS="/opt/cassandra/tools/bin/cassandra-stress"
# -(2^63): the low end of the murmur3 ring. 2^64/100 = 184467440737095516 is
# one percent of its width (hardcoded: 2**64 overflows bash's int64 arithmetic).
TOKEN_LO=-9223372036854775808
PCT_WIDTH=184467440737095516

log() { echo "[gen-df-2605] $*"; }
die() { echo "[gen-df-2605] FATAL: $*" >&2; exit 1; }

VALIDATE_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --validate-only) VALIDATE_ONLY=1; shift ;;
    -h|--help)
      echo "usage: $0 [--validate-only]   (config via env: CORPUS_ROOT, WIDE_PARTITIONS, OVERLAP_PCT, ...)" >&2
      exit 0 ;;
    *) die "unknown argument: $1 (config is via environment variables)" ;;
  esac
done

# ---------------------------------------------------------- input validation --
# BEFORE any destructive or expensive work: a typo must not start a container,
# burn an hour, and then emit a manifest describing a corpus nobody wanted.
GEN2_PARTITIONS=0
validate_inputs() {
  [[ -n "${CORPUS_ROOT// }" ]] || die "CORPUS_ROOT is empty"
  [[ "$CORPUS_ROOT" == /* ]] || die "CORPUS_ROOT must be an absolute path, got '$CORPUS_ROOT'"
  [[ "$(printf '%s' "$CORPUS_ROOT" | sed 's:/*$::')" != "" ]] || die "refusing to use '/' as CORPUS_ROOT"
  [[ -n "${KS// }" && -n "${TBL// }" ]] || die "keyspace/table is empty"
  for v in WIDE_PARTITIONS OVERLAP_PCT RF STRESS_THREADS MIN_DATA_DB SLICE_PCT SLICE_LIMIT NEED_GIB; do
    [[ "${!v}" =~ ^[0-9]+$ ]] || die "$v must be a non-negative integer, got '${!v}'"
  done
  [[ "$WIDE_PARTITIONS" -ge 1000 ]] || die "WIDE_PARTITIONS=$WIDE_PARTITIONS is too small to bench"
  (( OVERLAP_PCT >= 1 && OVERLAP_PCT <= 100 )) || die "OVERLAP_PCT must be 1..100, got $OVERLAP_PCT"
  (( SLICE_PCT >= 1 && SLICE_PCT <= 100 )) || die "SLICE_PCT must be 1..100, got $SLICE_PCT"
  (( MIN_DATA_DB >= 2 )) || die "MIN_DATA_DB must be >= 2 (a 1-SSTable corpus benches a merge-free path)"
  GEN2_PARTITIONS=$(( WIDE_PARTITIONS * OVERLAP_PCT / 100 ))
  [[ "$GEN2_PARTITIONS" -ge 1 ]] || die "generation 2 would be empty"
  log "validated: $KS.$TBL gen1=$WIDE_PARTITIONS parts (~$((WIDE_PARTITIONS * 10)) rows), gen2=$GEN2_PARTITIONS parts (${OVERLAP_PCT}% overlap), RF=$RF, CORPUS_ROOT=$CORPUS_ROOT"
}

preflight_space() {
  local avail_gib
  avail_gib=$(df -BG --output=avail "$(dirname "$CORPUS_ROOT")" | tail -1 | tr -dc '0-9')
  log "free space: ${avail_gib} GiB (need >= ${NEED_GIB} GiB)"
  [[ "${avail_gib:-0}" -ge "$NEED_GIB" ]] || die "insufficient free space under $(dirname "$CORPUS_ROOT")"
}

wait_ready() {
  local max="${1:-90}"
  log "waiting for Cassandra..."
  for i in $(seq 1 "$max"); do
    if $DOCKER exec "$CONTAINER" cqlsh -e "SELECT release_version FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra ready after $((i * 5))s"
      return 0
    fi
    sleep 5
  done
  die "Cassandra not ready after $((max * 5))s"
}

# ------------------------------------------------------------------- profile --
write_profile() {
  mkdir -p "$CORPUS_ROOT"
  # Unquoted heredoc: $KS/$TBL/$RF are interpolated; the YAML itself contains no
  # '$' or backticks.
  cat > "$CORPUS_ROOT/wide.yaml" <<YAML
### cassandra-stress user profile — >= 4 KB/row WIDE shape (issue #2605 spike)
### Field widths match gen-perf-corpus-3068.sh's wide_4kb so the two corpora are
### comparable; only the keyspace and the generation strategy differ.
keyspace: $KS
keyspace_definition: |
  CREATE KEYSPACE IF NOT EXISTS $KS WITH replication = {'class':'SimpleStrategy','replication_factor':$RF} AND durable_writes = false;

table: $TBL
table_definition: |
  CREATE TABLE IF NOT EXISTS $TBL (
    pk text,
    ck int,
    ts timestamp,
    status text,
    name text,
    note text,
    body text,
    payload blob,
    v_int int,
    v_long bigint,
    v_dbl double,
    v_bool boolean,
    PRIMARY KEY (pk, ck)
  ) WITH CLUSTERING ORDER BY (ck ASC)
    AND compression = {'class':'LZ4Compressor','chunk_length_in_kb':16}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

columnspec:
  - name: pk
    size: fixed(20)
    population: uniform(1..1000000000)
  - name: ck
    cluster: fixed(10)
  - name: status
    size: fixed(8)
  - name: name
    size: fixed(24)
  - name: note
    size: fixed(256)
  - name: body
    size: fixed(1400)
  - name: payload
    size: fixed(2400)

insert:
  partitions: fixed(1)
  batchtype: UNLOGGED
  select: fixed(1)/1

queries:
  readpart:
    cql: select * from $TBL where pk = ?
    fields: samerow
YAML
  log "wrote stress profile to $CORPUS_ROOT/wide.yaml"
}

# -------------------------------------------------------------------- driver --
start_container() {
  log "removing any stale container + data dir..."
  $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  sudo -n rm -rf "$CORPUS_ROOT/cassandra-data"
  mkdir -p "$CORPUS_ROOT/cassandra-data"
  # The image runs as uid 999 (cassandra); the bind mount must be writable by it.
  sudo -n chown -R 999:999 "$CORPUS_ROOT/cassandra-data"
  log "starting $IMAGE (heap $MAX_HEAP, cpus $CPUS, mem $MEM)..."
  $DOCKER run -d --name "$CONTAINER" --cpus "$CPUS" --memory "$MEM" \
    -e MAX_HEAP_SIZE="$MAX_HEAP" -e HEAP_NEWSIZE="$HEAP_NEW" \
    -e CASSANDRA_NUM_TOKENS=1 \
    -v "$CORPUS_ROOT/cassandra-data:/var/lib/cassandra" \
    "$IMAGE" >/dev/null
  wait_ready 120
}

create_schema() {
  $DOCKER cp "$CORPUS_ROOT/wide.yaml" "$CONTAINER:/tmp/wide.yaml"
  log "creating schema via a 1-op stress run..."
  $DOCKER exec "$CONTAINER" bash -lc \
    "$STRESS user profile=/tmp/wide.yaml ops\\(insert=1\\) n=1 no-warmup cl=ONE -pop seq=1..1 -rate threads=1 -node 127.0.0.1" \
    > "$CORPUS_ROOT/stress-schema.log" 2>&1 || true
  $DOCKER exec "$CONTAINER" cqlsh -e "DESCRIBE TABLE $KS.$TBL;" >/dev/null 2>&1 \
    || die "schema creation failed (RF=$RF rejected?); see $CORPUS_ROOT/stress-schema.log"
  # Only meaningful once the table exists.
  $DOCKER exec "$CONTAINER" nodetool disableautocompaction "$KS" "$TBL" \
    || die "could not disable autocompaction — STCS would merge the generations"
  log "autocompaction DISABLED for $KS.$TBL"
}

stress_gen() {  # $1 = generation label, $2 = partitions (seeds 1..$2)
  local gen="$1" n="$2" t0 t1
  log "[$gen] inserting seeds 1..$n => $n partitions x 10 rows = $((n * 10)) rows (threads=$STRESS_THREADS)"
  t0=$(date +%s)
  $DOCKER exec "$CONTAINER" bash -lc \
    "$STRESS user profile=/tmp/wide.yaml ops\\(insert=1\\) n=$n no-warmup cl=ONE -pop seq=1..$n -rate threads=$STRESS_THREADS -node 127.0.0.1" \
    > "$CORPUS_ROOT/stress-$gen.log" 2>&1 \
    || die "[$gen] cassandra-stress failed; see $CORPUS_ROOT/stress-$gen.log"
  t1=$(date +%s)
  GEN_SECONDS=$((t1 - t0))
  log "[$gen] load wall time: ${GEN_SECONDS}s"
  grep -E "^Total (partitions|errors)" "$CORPUS_ROOT/stress-$gen.log" || true
  $DOCKER exec "$CONTAINER" nodetool flush "$KS" "$TBL" || die "[$gen] flush failed"
  log "[$gen] flushed"
}

container_table_dir() {
  $DOCKER exec "$CONTAINER" bash -lc "ls -d /var/lib/cassandra/data/$KS/$TBL-* | head -1" | tr -d '\r'
}

data_db_count() {
  local dir="$1"
  $DOCKER exec "$CONTAINER" bash -lc "ls '$dir'/*-Data.db 2>/dev/null | wc -l" | tr -d '\r '
}

# Probe a FIXED 1% token slice: `writetime(body)` for every row in it. Prints
# "<rows> <rows_at_or_after_gen2_start>". Plain SELECT (no COUNT aggregate) so a
# server-side aggregation read timeout cannot turn a real measurement into an
# error, and an exact row count falls out of the same read.
slice_probe() {  # $1 = gen2 start timestamp in microseconds (0 before gen2)
  local since="$1" hi out
  hi=$(( TOKEN_LO + SLICE_PCT * PCT_WIDTH ))
  out=$($DOCKER exec "$CONTAINER" cqlsh --request-timeout=1200 -e \
    "SELECT writetime(body) FROM $KS.$TBL WHERE token(pk) >= $TOKEN_LO AND token(pk) < $hi LIMIT $SLICE_LIMIT;" \
    2>/dev/null) || die "slice probe query failed"
  printf '%s\n' "$out" | awk -v since="$since" '
    $1 ~ /^[0-9]+$/ && NF == 1 { n++; if (since > 0 && $1 + 0 >= since) g2++ }
    END { printf "%d %d\n", n, g2 }'
}

capture_schema() {  # $1 = destination file
  local out="$1" tmp
  tmp="$(mktemp)"
  $DOCKER exec "$CONTAINER" cqlsh -e "DESCRIBE KEYSPACE $KS;" > "$tmp" 2>/dev/null \
    || die "could not DESCRIBE KEYSPACE $KS (schema.cql is required for a reproducible manifest)"
  grep -q "^CREATE KEYSPACE $KS " "$tmp" || die "DESCRIBE KEYSPACE $KS produced no CREATE KEYSPACE"
  grep -q "^CREATE TABLE $KS\." "$tmp" || die "DESCRIBE KEYSPACE $KS produced no CREATE TABLE"
  mv "$tmp" "$out"
  log "captured schema -> $out ($(wc -c <"$out" | tr -d ' ') bytes)"
}

# Per-SSTable min/max write timestamp, so the manifest can attribute each
# Data.db to a generation (max_ts >= gen2 start => it holds generation-2 data).
capture_sstable_timestamps() {  # $1 = container dir, $2 = destination tsv
  local dir="$1" out="$2"
  : > "$out"
  local f base mn mx
  while read -r f; do
    [[ -n "$f" ]] || continue
    base="$(basename "$f")"
    mn=$($DOCKER exec "$CONTAINER" bash -lc "/opt/cassandra/tools/bin/sstablemetadata '$f' 2>/dev/null | awk -F': ' '/^Minimum timestamp/ {print \$2; exit}'" | tr -dc '0-9')
    mx=$($DOCKER exec "$CONTAINER" bash -lc "/opt/cassandra/tools/bin/sstablemetadata '$f' 2>/dev/null | awk -F': ' '/^Maximum timestamp/ {print \$2; exit}'" | tr -dc '0-9')
    printf '%s\t%s\t%s\n' "$base" "${mn:-0}" "${mx:-0}" >> "$out"
  done < <($DOCKER exec "$CONTAINER" bash -lc "ls '$dir'/*-Data.db 2>/dev/null" | tr -d '\r')
  log "captured per-SSTable timestamps -> $out ($(wc -l <"$out" | tr -d ' ') files)"
}

publish() {  # $1 = container sstable dir; echoes the published dir
  local cdir="$1" base host_dir dest
  base="$(basename "$cdir")"
  host_dir="$CORPUS_ROOT/cassandra-data/data/$KS/$base"
  dest="$CORPUS_ROOT/sstables/$KS/$base"
  [[ -d "$host_dir" ]] || die "host bind-mount dir missing: $host_dir"
  mkdir -p "$(dirname "$dest")"
  sudo -n rm -rf "$dest"
  mkdir -p "$dest"
  # Hardlink (same filesystem) — instant, and the corpus survives deletion of
  # the Cassandra data dir. Fall back to a copy across filesystems. sudo: the
  # files are owned by uid 999 and protected_hardlinks blocks linking them.
  sudo -n cp -l "$host_dir"/*.db "$host_dir"/*-TOC.txt "$host_dir"/*-Digest.crc32 "$dest/" 2>/dev/null \
    || sudo -n cp "$host_dir"/* "$dest/"
  sudo -n chown -R "$(id -u):$(id -g)" "$dest"
  echo "$dest"
}

# ---------------------------------------------------------------------- main --
validate_inputs
if [[ "$VALIDATE_ONLY" == 1 ]]; then
  echo "VALIDATE-OK table=$KS.$TBL gen1_partitions=$WIDE_PARTITIONS gen2_partitions=$GEN2_PARTITIONS corpus_root=$CORPUS_ROOT"
  exit 0
fi

preflight_space
write_profile
start_container
create_schema

stress_gen gen1 "$WIDE_PARTITIONS"
GEN1_SECONDS="$GEN_SECONDS"
CDIR="$(container_table_dir)"
[[ -n "$CDIR" ]] || die "could not locate the container's $KS/$TBL directory"
GEN1_DATA_DB=$(data_db_count "$CDIR")
log "[gen1] Data.db count: $GEN1_DATA_DB"

read -r SLICE_ROWS_GEN1 _ < <(slice_probe 0)
log "[gen1] token-slice probe (${SLICE_PCT}% of ring): $SLICE_ROWS_GEN1 rows"
[[ "${SLICE_ROWS_GEN1:-0}" -gt 0 ]] || die "generation 1 wrote no readable rows in the probe slice"
[[ "$SLICE_ROWS_GEN1" -lt "$SLICE_LIMIT" ]] || die "probe slice hit LIMIT=$SLICE_LIMIT — lower SLICE_PCT"

# Everything written from here on carries a strictly newer write timestamp.
GEN2_START_US=$(( $(date +%s) * 1000000 ))
sleep 2
stress_gen gen2 "$GEN2_PARTITIONS"
GEN2_SECONDS="$GEN_SECONDS"
GEN2_DATA_DB=$(data_db_count "$CDIR")
log "[gen2] Data.db count: $GEN2_DATA_DB"

read -r SLICE_ROWS_GEN2 SLICE_ROWS_NEW < <(slice_probe "$GEN2_START_US")
log "[gen2] token-slice probe: $SLICE_ROWS_GEN2 rows, $SLICE_ROWS_NEW of them written by generation 2"

# ------------------------------------------------------------------- asserts --
# (1) The generations must not have been merged away.
[[ "$GEN2_DATA_DB" -ge "$MIN_DATA_DB" ]] \
  || die "only $GEN2_DATA_DB Data.db file(s) survive (need >= $MIN_DATA_DB) — a merge-free corpus benches the wrong path"
# (2) Generation 2 must have REWRITTEN generation-1 partitions, not appended new
#     ones: a rewrite leaves the slice's row count unchanged, disjoint keys would
#     raise it by ~OVERLAP_PCT.
DELTA=$(( SLICE_ROWS_GEN2 - SLICE_ROWS_GEN1 ))
[[ "$DELTA" -lt 0 ]] && DELTA=$(( -DELTA ))
[[ $(( DELTA * 100 )) -le "$SLICE_ROWS_GEN1" ]] \
  || die "probe slice row count moved $SLICE_ROWS_GEN1 -> $SLICE_ROWS_GEN2 (>1%): generation 2 wrote NEW partitions instead of overlapping generation 1"
# (3) Generation-2 rows must actually be visible post-reconciliation.
[[ "${SLICE_ROWS_NEW:-0}" -gt 0 ]] \
  || die "no generation-2 write timestamps visible in the probe slice — reconciliation would have nothing to do"
MEASURED_OVERLAP=$(awk -v a="$SLICE_ROWS_NEW" -v b="$SLICE_ROWS_GEN2" 'BEGIN{printf "%.4f", (b>0)?a/b:0}')
log "MEASURED overlap fraction (post-reconciliation, ${SLICE_PCT}% token slice): $MEASURED_OVERLAP (requested $(awk -v p="$OVERLAP_PCT" 'BEGIN{printf "%.4f", p/100}'))"

capture_schema "$CORPUS_ROOT/schema.cql"
capture_sstable_timestamps "$CDIR" "$CORPUS_ROOT/sstable-timestamps.tsv"
DEST="$(publish "$CDIR" | tail -1)"
cp "$CORPUS_ROOT/schema.cql" "$DEST/schema.cql"
log "published -> $DEST"

# ------------------------------------------------------------------ manifest --
# Written ONLY next to the corpus. Nothing in the repo is touched.
MANIFEST="$CORPUS_ROOT/manifest-2605.json"
CORPUS_ROOT="$CORPUS_ROOT" KS="$KS" TBL="$TBL" IMAGE="$IMAGE" CONTAINER="$CONTAINER" \
DEST="$DEST" MANIFEST="$MANIFEST" RF="$RF" \
GEN1_PARTITIONS="$WIDE_PARTITIONS" GEN2_PARTITIONS="$GEN2_PARTITIONS" \
GEN1_SECONDS="$GEN1_SECONDS" GEN2_SECONDS="$GEN2_SECONDS" \
GEN1_DATA_DB="$GEN1_DATA_DB" GEN2_DATA_DB="$GEN2_DATA_DB" \
GEN2_START_US="$GEN2_START_US" OVERLAP_PCT="$OVERLAP_PCT" \
SLICE_PCT="$SLICE_PCT" SLICE_ROWS_GEN1="$SLICE_ROWS_GEN1" \
SLICE_ROWS_GEN2="$SLICE_ROWS_GEN2" SLICE_ROWS_NEW="$SLICE_ROWS_NEW" \
MEASURED_OVERLAP="$MEASURED_OVERLAP" \
python3 - <<'PY'
import glob, json, os, subprocess, datetime

env = os.environ
root, dest = env["CORPUS_ROOT"], env["DEST"]
gen2_start = int(env["GEN2_START_US"])

ts = {}
tsv = os.path.join(root, "sstable-timestamps.tsv")
if os.path.exists(tsv):
    for line in open(tsv):
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 3:
            ts[parts[0]] = (int(parts[1] or 0), int(parts[2] or 0))

files = []
total = 0
for p in sorted(glob.glob(os.path.join(dest, "*-Data.db"))):
    base = os.path.basename(p)
    size = os.path.getsize(p)
    total += size
    mn, mx = ts.get(base, (None, None))
    files.append({
        "name": base,
        "bytes": size,
        "min_timestamp_us": mn,
        "max_timestamp_us": mx,
        # An SSTable whose newest cell postdates the generation-2 cutover holds
        # generation-2 (overlapping, newest-wins) data.
        "holds_generation_2": (mx is not None and mx >= gen2_start),
    })

all_components = sorted(os.path.basename(p) for p in glob.glob(os.path.join(dest, "*")))
schema_path = os.path.join(dest, "schema.cql")
schema = open(schema_path).read() if os.path.exists(schema_path) else None

def rel(p):
    return os.path.relpath(p, root)

manifest = {
    "issue": 2605,
    "purpose": "wide + overlapping-generation Cassandra 5.0 corpus for the DataFusion TableProvider spike bench",
    "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "generator": "test-data/scripts/gen-df-spike-corpus-2605.sh",
    "image": env["IMAGE"],
    "container": env["CONTAINER"],
    "corpus_root": root,
    "keyspace": env["KS"],
    "table": env["TBL"],
    "declared_replication_factor": int(env["RF"]),
    "sstable_dir": rel(dest),
    "compaction": "SizeTieredCompactionStrategy, autocompaction DISABLED (no major compaction was run)",
    "compression": {"class": "LZ4Compressor", "chunk_length_in_kb": 16},
    "keyspace_ddl": schema,
    "generations": [
        {
            "generation": 1,
            "stress_seed_range": [1, int(env["GEN1_PARTITIONS"])],
            "partitions": int(env["GEN1_PARTITIONS"]),
            "rows": int(env["GEN1_PARTITIONS"]) * 10,
            "rows_per_partition": 10,
            "load_seconds": int(env["GEN1_SECONDS"]),
            "data_db_after_flush": int(env["GEN1_DATA_DB"]),
        },
        {
            "generation": 2,
            "stress_seed_range": [1, int(env["GEN2_PARTITIONS"])],
            "partitions": int(env["GEN2_PARTITIONS"]),
            "rows": int(env["GEN2_PARTITIONS"]) * 10,
            "rows_per_partition": 10,
            "load_seconds": int(env["GEN2_SECONDS"]),
            "data_db_after_flush": int(env["GEN2_DATA_DB"]),
            "overlaps_generation_1": True,
            "note": "same cassandra-stress seed range prefix => same partition keys, newer write timestamps",
            "cutover_timestamp_us": gen2_start,
        },
    ],
    "logical_rows_post_reconciliation": int(env["GEN1_PARTITIONS"]) * 10,
    "logical_partitions_post_reconciliation": int(env["GEN1_PARTITIONS"]),
    "rows_written_total": (int(env["GEN1_PARTITIONS"]) + int(env["GEN2_PARTITIONS"])) * 10,
    "overlap": {
        "requested_fraction": int(env["OVERLAP_PCT"]) / 100.0,
        "measured_fraction": float(env["MEASURED_OVERLAP"]),
        "method": (
            "SELECT writetime(body) over a fixed %s%% murmur3 token slice, before and after "
            "generation 2; measured_fraction = rows with writetime >= cutover / rows in slice"
            % env["SLICE_PCT"]
        ),
        "token_slice_pct": int(env["SLICE_PCT"]),
        "slice_rows_after_gen1": int(env["SLICE_ROWS_GEN1"]),
        "slice_rows_after_gen2": int(env["SLICE_ROWS_GEN2"]),
        "slice_rows_written_by_gen2": int(env["SLICE_ROWS_NEW"]),
        "slice_row_count_unchanged": abs(int(env["SLICE_ROWS_GEN2"]) - int(env["SLICE_ROWS_GEN1"])) * 100
                                     <= int(env["SLICE_ROWS_GEN1"]),
    },
    "data_db_count": len(files),
    "data_db_total_bytes": total,
    "data_db_files": files,
    "all_components": all_components,
    "usage": "CQLITE_DATASETS_ROOT=%s" % root,
}
with open(env["MANIFEST"], "w") as fh:
    json.dump(manifest, fh, indent=2, sort_keys=False)
    fh.write("\n")
print("[gen-df-2605] manifest: %s (%d Data.db, %.2f GiB)"
      % (env["MANIFEST"], len(files), total / 1024.0**3))
PY

log "DONE. Corpus at $CORPUS_ROOT/sstables/$KS/"
log "Use with: CQLITE_DATASETS_ROOT=$CORPUS_ROOT"
log "Container '$CONTAINER' left running for sstabledump/sstablemetadata; remove with: $DOCKER rm -f $CONTAINER"
