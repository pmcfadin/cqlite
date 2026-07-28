#!/usr/bin/env bash
# gen-perf-corpus-3068.sh — Generate a FIELD-SHAPED, LZ4-COMPRESSED, multi-GB
# single-SSTable Cassandra 5.0 (nb/BIG) corpus for issue #3068 read-plane
# performance measurement (scan window / large-I/O).
#
# WHY a bespoke corpus: the committed test-data fixtures are tiny and the
# Phase-0 perf anchor was UNCOMPRESSED, so it never executed the compressed
# read path at all. #3068 needs a corpus that
#   (1) is LZ4-compressed at the Cassandra table default chunk_length_in_kb=16
#       (the compressed scan window is a no-op without real chunks),
#   (2) has a Data.db far larger than a CPU cache and comparable to RAM, so a
#       "cold" scan is genuinely cold and "warm" is a real page-cache state,
#   (3) is ONE SSTable, so a k-way merge cannot pollute the read-plane number.
#
# NOT committed: the corpus itself (multi-GB). It is written OUTSIDE the repo,
# default /home/ubuntu/corpus-3068. Only this script + the emitted manifest are
# committed.
#
# Method: cassandra-stress (ships in the cassandra:5.0.2 image) driving a user
# profile, NOT a cqlsh INSERT loop — an INSERT-per-row .cql file (the shape used
# by gen-wide-big.sh) is hopeless at 12M rows. Two generation-time-only
# optimizations keep the load disk-bound rather than backpressure-bound; NEITHER
# changes the bytes written into the SSTable:
#   * keyspace durable_writes = false  -> no commitlog write amplification
#   * nodetool disableautocompaction   -> no STCS rewrites mid-load; a single
#                                          `nodetool compact` at the end does
#                                          the one merge we actually want.
#
# Output layout (mirrors the repo's CQLITE_DATASETS_ROOT convention, so
# CQLITE_DATASETS_ROOT=$CORPUS_ROOT works directly):
#   $CORPUS_ROOT/sstables/perf_3068/medium_700b-<uuid>/nb-*-*.db
#   $CORPUS_ROOT/sstables/perf_3068/wide_4kb-<uuid>/nb-*-*.db
#   $CORPUS_ROOT/manifest-3068.json    (copied to test-data/perf-corpus-3068-manifest.json)
#
# See docs/development/perf-corpus-and-containment.md — including why every
# measurement against this corpus must go through perf-run-contained.sh.
#
# Usage:
#   bash test-data/scripts/gen-perf-corpus-3068.sh                # both tables
#   MEDIUM_PARTITIONS=1200000 WIDE_PARTITIONS=120000 \
#     CORPUS_ROOT=/home/ubuntu/corpus-3068 bash .../gen-perf-corpus-3068.sh
#   TABLES=medium bash test-data/scripts/gen-perf-corpus-3068.sh   # medium only
set -euo pipefail

IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-perf3068}"
CORPUS_ROOT="${CORPUS_ROOT:-/home/ubuntu/corpus-3068}"
KS="perf_3068"
# Which tables to build: "both" | "medium" | "wide".
TABLES="${TABLES:-both}"
# 10 rows per partition (clustering fixed(10)), so rows = partitions * 10.
# 1.2M partitions * 10 rows * ~718 B/row on disk => ~8.6 GB Data.db.
MEDIUM_PARTITIONS="${MEDIUM_PARTITIONS:-1200000}"
# 120k partitions * 10 rows * ~4.2 KB/row on disk => ~5 GB Data.db.
WIDE_PARTITIONS="${WIDE_PARTITIONS:-120000}"
STRESS_THREADS="${STRESS_THREADS:-16}"
# docker needs sudo on the agent fleet machines; override with DOCKER=docker.
DOCKER="${DOCKER:-sudo -n docker}"
MAX_HEAP="${MAX_HEAP:-8G}"
HEAP_NEW="${HEAP_NEW:-2G}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRESS="/opt/cassandra/tools/bin/cassandra-stress"

log() { echo "[gen-perf-3068] $*"; }
die() { echo "[gen-perf-3068] FATAL: $*" >&2; exit 1; }

# ---------------------------------------------------------------- preflight --
# `nodetool compact` needs transient room for a full second copy of the table.
preflight_space() {
  local need_gib=$1
  local avail_gib
  avail_gib=$(df -BG --output=avail "$(dirname "$CORPUS_ROOT")" | tail -1 | tr -dc '0-9')
  log "free space: ${avail_gib} GiB (need >= ${need_gib} GiB for data + 2x compact headroom)"
  [[ "$avail_gib" -ge "$need_gib" ]] || die "insufficient free space"
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

# ------------------------------------------------------------------ profiles --
write_profiles() {
  mkdir -p "$CORPUS_ROOT"
  cat > "$CORPUS_ROOT/medium.yaml" <<'YAML'
### cassandra-stress user profile — ~700 B/row "medium" field shape (issue #3068)
keyspace: perf_3068
keyspace_definition: |
  CREATE KEYSPACE IF NOT EXISTS perf_3068 WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes = false;

table: medium_700b
table_definition: |
  CREATE TABLE IF NOT EXISTS medium_700b (
    pk text,
    ck int,
    ts timestamp,
    status text,
    name text,
    tags text,
    note text,
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
  - name: tags
    size: fixed(48)
  - name: note
    size: fixed(160)
  - name: payload
    size: fixed(400)

insert:
  partitions: fixed(1)
  batchtype: UNLOGGED
  select: fixed(1)/1

queries:
  readpart:
    cql: select * from medium_700b where pk = ?
    fields: samerow
YAML

  cat > "$CORPUS_ROOT/wide.yaml" <<'YAML'
### cassandra-stress user profile — >= 4 KB/row "wide" field shape (#3068 / #3030)
keyspace: perf_3068
keyspace_definition: |
  CREATE KEYSPACE IF NOT EXISTS perf_3068 WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes = false;

table: wide_4kb
table_definition: |
  CREATE TABLE IF NOT EXISTS wide_4kb (
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
    cql: select * from wide_4kb where pk = ?
    fields: samerow
YAML
  log "wrote stress profiles to $CORPUS_ROOT/{medium,wide}.yaml"
}

# -------------------------------------------------------------------- driver --
start_container() {
  log "removing any stale container + data dir..."
  $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  sudo -n rm -rf "$CORPUS_ROOT/cassandra-data"
  mkdir -p "$CORPUS_ROOT/cassandra-data"
  # The image runs as uid 999 (cassandra); the bind mount must be writable by it.
  sudo -n chown -R 999:999 "$CORPUS_ROOT/cassandra-data"
  log "starting $IMAGE (heap $MAX_HEAP, data bind-mounted at $CORPUS_ROOT/cassandra-data)..."
  $DOCKER run -d --name "$CONTAINER" --cpus 14 --memory 22g \
    -e MAX_HEAP_SIZE="$MAX_HEAP" -e HEAP_NEWSIZE="$HEAP_NEW" \
    -e CASSANDRA_NUM_TOKENS=1 \
    -v "$CORPUS_ROOT/cassandra-data:/var/lib/cassandra" \
    "$IMAGE" >/dev/null
  wait_ready 90
}

stress_run() {  # $1 = profile basename (medium|wide), $2 = partitions
  local prof="$1" n="$2"
  $DOCKER cp "$CORPUS_ROOT/$prof.yaml" "$CONTAINER:/tmp/$prof.yaml"
  log "[$prof] creating schema (1-op run)..."
  $DOCKER exec "$CONTAINER" bash -lc \
    "$STRESS user profile=/tmp/$prof.yaml ops\\(insert=1\\) n=1 no-warmup cl=ONE -pop seq=1..1 -rate threads=1 -node 127.0.0.1" \
    >/dev/null 2>&1 || true
  # Disable autocompaction only AFTER the table exists.
  $DOCKER exec "$CONTAINER" nodetool disableautocompaction "$KS" >/dev/null 2>&1 || true
  log "[$prof] loading $n partitions x 10 rows = $((n * 10)) rows (threads=$STRESS_THREADS)..."
  local t0 t1
  t0=$(date +%s)
  $DOCKER exec "$CONTAINER" bash -lc \
    "$STRESS user profile=/tmp/$prof.yaml ops\\(insert=1\\) n=$n no-warmup cl=ONE -pop seq=1..$n -rate threads=$STRESS_THREADS -node 127.0.0.1" \
    > "$CORPUS_ROOT/stress-$prof.log" 2>&1 \
    || die "[$prof] cassandra-stress failed; see $CORPUS_ROOT/stress-$prof.log"
  t1=$(date +%s)
  log "[$prof] load wall time: $((t1 - t0))s"
  grep -E "^Total (partitions|errors)" "$CORPUS_ROOT/stress-$prof.log" || true
}

finalize_table() {  # $1 = table name
  local tbl="$1"
  log "[$tbl] enabling autocompaction, flush + MAJOR compact (this is the slow part)..."
  $DOCKER exec "$CONTAINER" nodetool enableautocompaction "$KS" "$tbl"
  $DOCKER exec "$CONTAINER" nodetool flush "$KS" "$tbl"
  $DOCKER exec "$CONTAINER" nodetool compact "$KS" "$tbl"
  # A major compaction can leave the old inputs briefly; wait for quiescence.
  for _ in $(seq 1 60); do
    local pending
    pending=$($DOCKER exec "$CONTAINER" nodetool compactionstats 2>/dev/null \
      | awk '/pending tasks:/ {print $3; exit}')
    [[ "${pending:-0}" == "0" ]] && break
    sleep 5
  done

  local dir count
  dir=$($DOCKER exec "$CONTAINER" bash -lc "ls -d /var/lib/cassandra/data/$KS/$tbl-* | head -1" | tr -d '\r')
  count=$($DOCKER exec "$CONTAINER" bash -lc "ls '$dir'/*-Data.db 2>/dev/null | wc -l" | tr -d '\r ')
  log "[$tbl] Data.db count after major compaction: $count"
  [[ "$count" == "1" ]] || die "[$tbl] expected exactly 1 Data.db, found $count (k-way merge would pollute the measurement)"
  echo "$dir"
}

publish_table() {  # $1 = table, $2 = container sstable dir
  local tbl="$1" cdir="$2"
  local host_dir="$CORPUS_ROOT/cassandra-data/data/$KS/$(basename "$cdir")"
  local dest="$CORPUS_ROOT/sstables/$KS/$(basename "$cdir")"
  [[ -d "$host_dir" ]] || die "[$tbl] host bind-mount dir missing: $host_dir"
  mkdir -p "$(dirname "$dest")"
  sudo -n rm -rf "$dest"
  mkdir -p "$dest"
  # Hardlink (same filesystem) — instant, and the corpus survives deletion of
  # the Cassandra data dir. Fall back to a copy across filesystems.
  sudo -n cp -l "$host_dir"/nb-*-*.db "$host_dir"/nb-*-TOC.txt "$host_dir"/nb-*-Digest.crc32 "$dest/" 2>/dev/null \
    || sudo -n cp "$host_dir"/nb-* "$dest/"
  sudo -n chown -R "$(id -u):$(id -g)" "$dest"
  echo "$dest"
}

# ---------------------------------------------------------------------- main --
preflight_space 40
write_profiles
start_container

DIRS=()
if [[ "$TABLES" == "both" || "$TABLES" == "medium" ]]; then
  stress_run medium "$MEDIUM_PARTITIONS"
fi
if [[ "$TABLES" == "both" || "$TABLES" == "wide" ]]; then
  stress_run wide "$WIDE_PARTITIONS"
fi
if [[ "$TABLES" == "both" || "$TABLES" == "medium" ]]; then
  DIRS+=("medium_700b:$(finalize_table medium_700b | tail -1)")
fi
if [[ "$TABLES" == "both" || "$TABLES" == "wide" ]]; then
  DIRS+=("wide_4kb:$(finalize_table wide_4kb | tail -1)")
fi

PUBLISHED=()
for entry in "${DIRS[@]}"; do
  tbl="${entry%%:*}"; cdir="${entry#*:}"
  PUBLISHED+=("$tbl:$(publish_table "$tbl" "$cdir" | tail -1)")
  log "[$tbl] published"
done

# Two manifests, same content: one next to the (uncommitted) corpus, and one at
# the COMMITTED path so a regenerated corpus can be diffed against the recorded
# sha256/row counts. MANIFEST_OUT= disables the in-repo write.
log "writing manifest..."
MANIFEST_OUT="${MANIFEST_OUT-$SCRIPT_DIR/../perf-corpus-3068-manifest.json}"
python3 "$SCRIPT_DIR/write-perf-corpus-manifest.py" \
  --corpus-root "$CORPUS_ROOT" \
  --keyspace "$KS" \
  --image "$IMAGE" \
  --container "$CONTAINER" \
  ${PUBLISHED[@]+"${PUBLISHED[@]/#/--table=}"}
# Copied, not re-generated: a second run would re-hash multiple GB for a
# byte-identical result (the manifest records corpus-root-RELATIVE paths).
if [[ -n "$MANIFEST_OUT" ]]; then
  cp "$CORPUS_ROOT/manifest-3068.json" "$MANIFEST_OUT"
  log "committed-path manifest: $MANIFEST_OUT"
fi

log "DONE. Corpus at $CORPUS_ROOT/sstables/$KS/"
log "Use with: CQLITE_DATASETS_ROOT=$CORPUS_ROOT"
log "Container '$CONTAINER' left running for sstabledump/sstablemetadata; remove with: $DOCKER rm -f $CONTAINER"
