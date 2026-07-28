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
#   $CORPUS_ROOT/sstables/perf_3068/medium_700b-<uuid>/nb-*-*.db + schema.cql
#   $CORPUS_ROOT/sstables/perf_3068/wide_4kb-<uuid>/nb-*-*.db   + schema.cql
#   $CORPUS_ROOT/manifest-3068.json    (copied to test-data/perf-corpus-3068-manifest.json)
#
# schema.cql is `cqlsh DESCRIBE KEYSPACE` captured at generation time and copied
# next to each published SSTable, so the manifest's keyspace/table DDL can be
# regenerated OFFLINE from the corpus alone (no live container needed).
#
# See docs/development/perf-corpus-and-containment.md — including why every
# measurement against this corpus must go through perf-run-contained.sh.
#
# Usage:
#   bash test-data/scripts/gen-perf-corpus-3068.sh                # both tables
#   MEDIUM_PARTITIONS=1200000 WIDE_PARTITIONS=120000 \
#     CORPUS_ROOT=/home/ubuntu/corpus-3068 bash .../gen-perf-corpus-3068.sh
#   TABLES=medium bash test-data/scripts/gen-perf-corpus-3068.sh   # medium only
#   bash .../gen-perf-corpus-3068.sh --validate-only   # validate inputs, run nothing
#   bash .../gen-perf-corpus-3068.sh --prune-dry-run   # + list the stale corpus
#                                                     #   dirs a run WOULD remove
set -euo pipefail

IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-perf3068}"
# `${VAR-default}`, not `${VAR:-default}`: an EXPLICITLY EMPTY CORPUS_ROOT/TABLES
# (typically a caller's unset variable) must fail validation, never silently
# become the default — this script deletes multi-GB paths under CORPUS_ROOT.
CORPUS_ROOT="${CORPUS_ROOT-/home/ubuntu/corpus-3068}"
KS="perf_3068"
# Which tables to build: "both" | "medium" | "wide".
TABLES="${TABLES-both}"
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
# Remove a previously-published <table>-<uuid> dir before publishing the new one.
# PRUNE_STALE=0 keeps them (accepting several multi-GB copies the manifest does
# not describe).
PRUNE_STALE="${PRUNE_STALE:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRESS="/opt/cassandra/tools/bin/cassandra-stress"

log() { echo "[gen-perf-3068] $*"; }
die() { echo "[gen-perf-3068] FATAL: $*" >&2; exit 1; }

VALIDATE_ONLY=0
PRUNE_DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    # Self-test hooks (scripts/tests/test_gen_perf_corpus_3068.sh): validate the
    # inputs / enumerate prune candidates and EXIT — no container, no writes, no
    # deletions.
    --validate-only) VALIDATE_ONLY=1; shift ;;
    --prune-dry-run) VALIDATE_ONLY=1; PRUNE_DRY_RUN=1; shift ;;
    -h|--help)
      echo "usage: $0 [--validate-only|--prune-dry-run]  (config via env: TABLES, CORPUS_ROOT, ...)" >&2
      exit 0 ;;
    *) die "unknown argument: $1 (config is via environment variables)" ;;
  esac
done

# ---------------------------------------------------------- input validation --
# BEFORE any destructive or expensive work: an unvalidated TABLES typo used to
# start a container, generate nothing, and then overwrite the COMMITTED manifest
# with an empty tables array — silent corruption of a provenance artifact.
declare -a SELECTED_TABLES=()
validate_inputs() {
  case "$TABLES" in
    both)   SELECTED_TABLES=(medium_700b wide_4kb) ;;
    medium) SELECTED_TABLES=(medium_700b) ;;
    wide)   SELECTED_TABLES=(wide_4kb) ;;
    *) die "invalid TABLES='$TABLES' (expected one of: both | medium | wide)" ;;
  esac
  [[ -n "${CORPUS_ROOT// }" ]] || die "CORPUS_ROOT is empty"
  [[ "$CORPUS_ROOT" == /* ]] || die "CORPUS_ROOT must be an absolute path, got '$CORPUS_ROOT'"
  [[ "$(printf '%s' "$CORPUS_ROOT" | sed 's:/*$::')" != "" ]] \
    || die "refusing to use '/' as CORPUS_ROOT"
  [[ -n "${KS// }" ]] || die "keyspace is empty"
  log "validated: TABLES=$TABLES -> ${SELECTED_TABLES[*]}; CORPUS_ROOT=$CORPUS_ROOT"
}

# Resolved (symlink-free) corpus keyspace dir; "" when it does not exist yet.
corpus_keyspace_dir() {
  local root
  root="$(cd "$CORPUS_ROOT" 2>/dev/null && pwd -P)" || return 0
  [[ -n "$root" && "$root" != "/" ]] || die "CORPUS_ROOT resolved to '/' — refusing"
  printf '%s/sstables/%s' "$root" "$KS"
}

# Remove PREVIOUS <table>-<uuid> dirs so repeated regenerations cannot leave
# several multi-GB copies of a table while the manifest describes only the last.
#
# Deliberately narrow — this deletes multi-GB paths:
#   * only DIRECT children of $CORPUS_ROOT/sstables/$KS,
#   * only names matching exactly "<selected-table>-<32 hex>" (Cassandra's own
#     "<table>-<UUID-without-dashes>" layout); anything else is left alone,
#   * never a symlink, and never a path whose resolved form is outside that
#     keyspace dir (die, not delete),
#   * never the directory just published ($2), and
#   * never with an empty/relative/"/" corpus root (validate_inputs + here).
prune_stale_table_dirs() {  # $1 = table, $2 = basename to KEEP ("" keeps none)
  local tbl="$1" keep="${2:-}" ks_dir d base real
  [[ -n "${tbl// }" ]] || die "prune: empty table name"
  ks_dir="$(corpus_keyspace_dir)"
  [[ -n "$ks_dir" && -d "$ks_dir" ]] || return 0
  local had_nullglob=0
  shopt -q nullglob && had_nullglob=1
  shopt -s nullglob
  for d in "$ks_dir/$tbl"-*; do
    base="$(basename "$d")"
    [[ -d "$d" ]] || continue
    if [[ -L "$d" ]]; then
      log "[prune] skipping symlink (never followed): $d"
      continue
    fi
    if [[ ! "$base" =~ ^${tbl}-[0-9a-f]{32}$ ]]; then
      log "[prune] skipping '$base' (not a <table>-<uuid> corpus dir)"
      continue
    fi
    [[ -n "$keep" && "$base" == "$keep" ]] && continue
    real="$(cd "$d" && pwd -P)" || die "prune: cannot resolve $d"
    [[ "$real" == "$ks_dir/$base" ]] \
      || die "prune: '$d' resolves OUTSIDE the corpus keyspace dir ($real) — refusing to delete"
    if [[ "$PRUNE_DRY_RUN" == 1 ]]; then
      echo "WOULD-PRUNE $real"
      continue
    fi
    log "[prune] removing stale corpus dir $real"
    sudo -n rm -rf -- "$real"
  done
  [[ "$had_nullglob" == 1 ]] || shopt -u nullglob
}

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

# Capture the live keyspace + table DDL so the manifest's keyspace_ddl / per-table
# ddl can be rebuilt from the corpus alone. FAIL-CLOSED: without it a fresh run
# would emit `keyspace_ddl: null` and the committed manifest would not be
# reproducible by the committed generator.
capture_schema() {  # $1 = destination file
  local out="$1" tmp
  tmp="$(mktemp)"
  $DOCKER exec "$CONTAINER" cqlsh -e "DESCRIBE KEYSPACE $KS;" > "$tmp" 2>/dev/null \
    || die "could not DESCRIBE KEYSPACE $KS (schema.cql is required for a reproducible manifest)"
  grep -q "^CREATE KEYSPACE $KS " "$tmp" \
    || die "DESCRIBE KEYSPACE $KS produced no CREATE KEYSPACE statement"
  grep -q "^CREATE TABLE $KS\." "$tmp" \
    || die "DESCRIBE KEYSPACE $KS produced no CREATE TABLE statement"
  mv "$tmp" "$out"
  log "captured schema -> $out ($(wc -c <"$out" | tr -d ' ') bytes)"
}

publish_table() {  # $1 = table, $2 = container sstable dir
  local tbl="$1" cdir="$2"
  local host_dir="$CORPUS_ROOT/cassandra-data/data/$KS/$(basename "$cdir")"
  local dest="$CORPUS_ROOT/sstables/$KS/$(basename "$cdir")"
  [[ -d "$host_dir" ]] || die "[$tbl] host bind-mount dir missing: $host_dir"
  mkdir -p "$(dirname "$dest")"
  # Drop any earlier generation of THIS table first (see prune_stale_table_dirs
  # for the guards) so the corpus never holds several multi-GB copies.
  if [[ "$PRUNE_STALE" == 1 ]]; then
    prune_stale_table_dirs "$tbl" "$(basename "$cdir")"
  fi
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
# Input validation runs FIRST — before the container, the load, and any deletion.
validate_inputs

if [[ "$VALIDATE_ONLY" == 1 ]]; then
  if [[ "$PRUNE_DRY_RUN" == 1 ]]; then
    for tbl in "${SELECTED_TABLES[@]}"; do
      prune_stale_table_dirs "$tbl" ""
    done
  fi
  echo "VALIDATE-OK tables=${SELECTED_TABLES[*]} corpus_root=$CORPUS_ROOT keyspace=$KS"
  exit 0
fi

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

# Captured once from the live container, copied into every published dir.
capture_schema "$CORPUS_ROOT/schema.cql"

PUBLISHED=()
for entry in "${DIRS[@]}"; do
  tbl="${entry%%:*}"; cdir="${entry#*:}"
  dest="$(publish_table "$tbl" "$cdir" | tail -1)"
  cp "$CORPUS_ROOT/schema.cql" "$dest/schema.cql"
  PUBLISHED+=("$tbl:$dest")
  log "[$tbl] published (with schema.cql)"
done

[[ ${#PUBLISHED[@]} -gt 0 ]] || die "no tables were published — refusing to write a manifest"

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
