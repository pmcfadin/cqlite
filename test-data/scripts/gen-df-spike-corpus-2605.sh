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
#     i.e. measure the wrong thing for this spike.
#   * But merge depth must be the SHAPE THE ISSUE ASKS FOR (~1.9M partitions,
#     TWO generations) and not whatever the flush cadence happened to leave.
#     docs/architecture/throughput-program-2026-07.md:158 records the "2 gens"
#     band as "an ASSUMPTION, NOT A MEASUREMENT (STCS-derived expected-k band)",
#     and BOTH bench arms consume the SAME post-reconciliation batches — so an
#     accidental k=25 merge inflates the SHARED decode+merge floor and thereby
#     SHRINKS the apparent vectorized-exec share, biasing the spike toward
#     "don't promote DataFusion". That bias is the hard kind to notice because
#     it looks conservative. Hence the k-depth control below, and a fail-closed
#     assert on MIN_DATA_DB <= count <= MAX_DATA_DB.
#
# K-DEPTH CONTROL — sequencing is the whole trick:
#   1. load generation 1, flush, then MAJOR-compact it to ONE SSTable. Safe
#      precisely because generation 2 does not exist yet: there is no overlap to
#      destroy.
#   2. THEN load generation 2, flush, and compact ONLY generation 2's files
#      (`nodetool compact --user-defined <gen2 paths>`), identified as "every
#      Data.db that is not the file step 1 produced".
#   3. NEVER a whole-table major compaction after step 1 — that would merge the
#      generations into one SSTable and destroy the overlap this corpus exists
#      to provide.
#   Autocompaction stays DISABLED throughout all of it, so STCS cannot merge the
#   generations behind our back; every compaction here is explicit and scoped.
#   COMPACT_GEN1=0 COMPACT_GEN2=0 MAX_DATA_DB=100 reproduces the deliberate
#   high-k (~25 flush-generation) variant as a merge-depth sensitivity data point.
#
# Shape produced (defaults):
#   generation 1: WIDE_PARTITIONS=190000 partitions x 10 rows = ~1.9M rows
#                 (the R12 corpus row count), ~4.2 KB/row on disk => ~8 GB
#   generation 2: the FIRST OVERLAP_PCT=30% of the SAME cassandra-stress seed
#                 range, re-inserted => the same ~57000 partition keys rewritten
#                 with NEWER write timestamps => ~2.4 GB, and a merge iterator
#                 that must reconcile newest-wins across generations over the
#                 whole token range.
#   `nodetool flush` after each generation, then the scoped compactions above:
#   end state 2 Data.db files (generation 1 | generation 2) with ~30% key overlap.
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
# LZ4 IS LOAD-BEARING, SO IT IS MEASURED, NOT ASSUMED. PHASE_STREAM_DECOMPRESS is
# a real sub-phase of the read pipeline: an UNCOMPRESSED corpus understates decode
# cost and therefore OVERSTATES the vectorized-exec share — corrupting the exact
# number this spike isolates (M15 item 1: separate the decode-to-column delta from
# the vectorized-exec delta). gen-perf-corpus-3068.sh's own header records the
# trap ("the Phase-0 perf anchor was UNCOMPRESSED, so it never executed the
# compressed read path at all"), and R12 itself was LZ4. So before the manifest is
# written this script FAILS CLOSED unless, for EVERY published Data.db, a sibling
# CompressionInfo.db parses (via read-compression-info.py, which reads the
# authoritative written header rather than the DDL) as LZ4Compressor with
# chunk_length = 16 KiB. The measured values are recorded per file in the manifest.
#
# REPLICATION FACTOR IS NOMINAL. RF=3 is DECLARED in the DDL (the M15 "wide +
# RF=3/overlap" shape) but a single-node container can only STORE one replica, so
# real RF=3 is unreachable here. The property this corpus actually exercises is
# cross-SSTable OVERLAP, not replication; the manifest says so in a
# replication_note field rather than letting the number imply otherwise.
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
# DECLARED replication factor — nominal only (see REPLICATION FACTOR above): a
# single-node container stores ONE replica whatever this says, so it affects the
# recorded DDL and not the bytes.
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
# Fail-closed BAND on surviving SSTables (see K-DEPTH CONTROL above): at least 2
# so a merge actually runs, at most 3 so the merge depth is the shape the issue
# asks for rather than the flush cadence's accident.
MIN_DATA_DB="${MIN_DATA_DB:-2}"
MAX_DATA_DB="${MAX_DATA_DB:-3}"
# Scoped compactions that produce the k=2 shape. Both 0 (with MAX_DATA_DB raised)
# reproduces the high-k variant.
COMPACT_GEN1="${COMPACT_GEN1:-1}"
COMPACT_GEN2="${COMPACT_GEN2:-1}"
# Recorded in the manifest so a consumer can tell the variants apart.
CORPUS_VARIANT="${CORPUS_VARIANT:-k2-two-generations}"
# Token slice used for the overlap probe, in percent of the murmur3 ring.
SLICE_PCT="${SLICE_PCT:-1}"
SLICE_LIMIT="${SLICE_LIMIT:-200000}"
NEED_GIB="${NEED_GIB:-30}"
# Plain `docker` works on this fleet box; override for hosts needing sudo.
DOCKER="${DOCKER:-docker}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
  for v in WIDE_PARTITIONS OVERLAP_PCT RF STRESS_THREADS MIN_DATA_DB MAX_DATA_DB SLICE_PCT SLICE_LIMIT NEED_GIB; do
    [[ "${!v}" =~ ^[0-9]+$ ]] || die "$v must be a non-negative integer, got '${!v}'"
  done
  [[ "$WIDE_PARTITIONS" -ge 1000 ]] || die "WIDE_PARTITIONS=$WIDE_PARTITIONS is too small to bench"
  (( OVERLAP_PCT >= 1 && OVERLAP_PCT <= 100 )) || die "OVERLAP_PCT must be 1..100, got $OVERLAP_PCT"
  (( SLICE_PCT >= 1 && SLICE_PCT <= 100 )) || die "SLICE_PCT must be 1..100, got $SLICE_PCT"
  (( MIN_DATA_DB >= 2 )) || die "MIN_DATA_DB must be >= 2 (a 1-SSTable corpus benches a merge-free path)"
  (( MAX_DATA_DB >= MIN_DATA_DB )) || die "MAX_DATA_DB=$MAX_DATA_DB is below MIN_DATA_DB=$MIN_DATA_DB"
  for v in COMPACT_GEN1 COMPACT_GEN2; do
    [[ "${!v}" == "0" || "${!v}" == "1" ]] || die "$v must be 0 or 1, got '${!v}'"
  done
  [[ -n "${CORPUS_VARIANT// }" ]] || die "CORPUS_VARIANT is empty"
  [[ -r "$SCRIPT_DIR/read-compression-info.py" ]] \
    || die "missing $SCRIPT_DIR/read-compression-info.py — the LZ4 verification cannot be skipped"
  GEN2_PARTITIONS=$(( WIDE_PARTITIONS * OVERLAP_PCT / 100 ))
  [[ "$GEN2_PARTITIONS" -ge 1 ]] || die "generation 2 would be empty"
  log "validated: $KS.$TBL gen1=$WIDE_PARTITIONS parts (~$((WIDE_PARTITIONS * 10)) rows), gen2=$GEN2_PARTITIONS parts (${OVERLAP_PCT}% overlap), RF=$RF (nominal), variant=$CORPUS_VARIANT, compact gen1=$COMPACT_GEN1 gen2=$COMPACT_GEN2, k band ${MIN_DATA_DB}..${MAX_DATA_DB}, CORPUS_ROOT=$CORPUS_ROOT"
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

# ------------------------------------------------------------- k-depth control --
# Wait for the compaction queue to drain. A `nodetool compact` returns before the
# obsoleted inputs are unlinked, so a Data.db count taken too early is a lie.
wait_compactions() {
  local pending
  for _ in $(seq 1 720); do
    pending=$($DOCKER exec "$CONTAINER" nodetool compactionstats 2>/dev/null \
      | awk '/pending tasks:/ {print $3; exit}')
    [[ "${pending:-1}" == "0" ]] && { sleep 3; return 0; }
    sleep 5
  done
  die "compactions did not drain within 3600s"
}

# WHOLE-TABLE major compaction. Only ever safe BEFORE generation 2 exists — after
# that it would merge the generations into one SSTable and destroy the overlap.
compact_table_major() {
  log "[compact] MAJOR compaction of $KS.$TBL (generation 1 only — gen2 does not exist yet)"
  local t0 t1
  t0=$(date +%s)
  $DOCKER exec "$CONTAINER" nodetool compact "$KS" "$TBL" || die "major compaction failed"
  wait_compactions
  t1=$(date +%s)
  log "[compact] major compaction took $((t1 - t0))s"
}

# Compaction SCOPED to an explicit file list (generation 2's flush files), so the
# generation-1 SSTable is left untouched and the overlap survives.
compact_user_defined() {  # $@ = absolute in-container Data.db paths
  [[ $# -gt 0 ]] || die "compact_user_defined called with no files"
  log "[compact] user-defined compaction of $# generation-2 file(s)"
  local t0 t1
  t0=$(date +%s)
  $DOCKER exec "$CONTAINER" nodetool compact --user-defined "$@" \
    || die "user-defined compaction failed"
  wait_compactions
  t1=$(date +%s)
  log "[compact] user-defined compaction took $((t1 - t0))s"
}

list_data_db() {  # $1 = container dir; one absolute path per line
  $DOCKER exec "$CONTAINER" bash -lc "ls '$1'/*-Data.db 2>/dev/null" | tr -d '\r'
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

# ------------------------------------------------------- compression verifier --
# LZ4 is load-bearing for this corpus (see header). Verify it from the
# AUTHORITATIVE written component — CompressionInfo.db, parsed by
# read-compression-info.py — not from the DDL, which a later schema change or a
# Cassandra-side clamp could make a lie. Fails closed; also emits a TSV the
# manifest records per file.
verify_compression() {  # $1 = published dir, $2 = destination tsv
  local dest="$1" out="$2" n=0 data ci info comp chunk
  : > "$out"
  shopt -s nullglob
  for data in "$dest"/*-Data.db; do
    ci="${data%-Data.db}-CompressionInfo.db"
    [[ -f "$ci" ]] \
      || die "no CompressionInfo.db beside $(basename "$data") — this corpus is NOT compressed, which would understate decode cost and overstate the vectorized-exec share"
    info="$(python3 "$SCRIPT_DIR/read-compression-info.py" "$ci" --json)" \
      || die "could not parse $(basename "$ci")"
    comp="$(printf '%s' "$info" | python3 -c 'import json,sys; print(json.load(sys.stdin)["compressor"])')"
    chunk="$(printf '%s' "$info" | python3 -c 'import json,sys; print(json.load(sys.stdin)["chunk_length_bytes"])')"
    [[ "$comp" == "LZ4Compressor" ]] || die "$(basename "$data") is compressed with '$comp', expected LZ4Compressor"
    [[ "$chunk" == "16384" ]] || die "$(basename "$data") has chunk_length=$chunk bytes, expected 16384 (16 KiB)"
    printf '%s\t%s\t%s\n' "$(basename "$data")" "$comp" "$chunk" >> "$out"
    n=$((n + 1))
  done
  shopt -u nullglob
  [[ "$n" -gt 0 ]] || die "verify_compression found no Data.db under $dest"
  log "VERIFIED compression: $n/$n Data.db are LZ4Compressor @ chunk_length=16384 B (from CompressionInfo.db, not the DDL)"
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
#
# sstablemetadata prints "Minimum timestamp: 08/28/2026 06:24:07 (1787898247892692)"
# — the microseconds are the LAST integer on the line, and the human-readable
# date in front of them is why this must not be reduced with a blunt digit
# filter (which silently concatenates the date's digits onto the timestamp).
sstable_timestamp() {  # $1 = sstablemetadata output, $2 = Minimum|Maximum
  printf '%s\n' "$1" | awk -v want="$2 timestamp" '
    index($0, want) == 1 { s = $0; gsub(/[^0-9]/, " ", s); n = split(s, a, " "); print a[n]; exit }'
}

capture_sstable_timestamps() {  # $1 = container dir, $2 = destination tsv
  local dir="$1" out="$2"
  : > "$out"
  local f base raw mn mx
  while read -r f; do
    [[ -n "$f" ]] || continue
    base="$(basename "$f")"
    # tr -d '\0': sstablemetadata emits NUL bytes (raw key blobs), which a
    # command substitution would otherwise warn about on every file.
    raw="$($DOCKER exec "$CONTAINER" /opt/cassandra/tools/bin/sstablemetadata "$f" 2>/dev/null | tr -d '\0' || true)"
    mn="$(sstable_timestamp "$raw" Minimum)"
    mx="$(sstable_timestamp "$raw" Maximum)"
    [[ -n "$mn" && -n "$mx" ]] \
      || log "WARNING: could not read min/max timestamp for $base (generation attribution will be null)"
    printf '%s\t%s\t%s\n' "$base" "${mn:-}" "${mx:-}" >> "$out"
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
  echo "VALIDATE-OK table=$KS.$TBL gen1_partitions=$WIDE_PARTITIONS gen2_partitions=$GEN2_PARTITIONS variant=$CORPUS_VARIANT k_band=${MIN_DATA_DB}..${MAX_DATA_DB} corpus_root=$CORPUS_ROOT"
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
GEN1_FLUSH_DATA_DB=$(data_db_count "$CDIR")
log "[gen1] Data.db count after flush: $GEN1_FLUSH_DATA_DB"

# Step 1 of the k-depth control: collapse generation 1 to ONE SSTable while there
# is still no generation 2 to lose.
if [[ "$COMPACT_GEN1" == 1 ]]; then
  compact_table_major
fi
GEN1_DATA_DB=$(data_db_count "$CDIR")
log "[gen1] Data.db count after compaction: $GEN1_DATA_DB"
if [[ "$COMPACT_GEN1" == 1 ]]; then
  [[ "$GEN1_DATA_DB" == "1" ]] \
    || die "[gen1] expected exactly 1 Data.db after the major compaction, found $GEN1_DATA_DB"
fi
# Remembered so generation 2's files can be identified by difference — the only
# way to scope the second compaction without touching generation 1.
GEN1_FILES="$(list_data_db "$CDIR")"

read -r SLICE_ROWS_GEN1 _ < <(slice_probe 0)
log "[gen1] token-slice probe (${SLICE_PCT}% of ring): $SLICE_ROWS_GEN1 rows"
[[ "${SLICE_ROWS_GEN1:-0}" -gt 0 ]] || die "generation 1 wrote no readable rows in the probe slice"
[[ "$SLICE_ROWS_GEN1" -lt "$SLICE_LIMIT" ]] || die "probe slice hit LIMIT=$SLICE_LIMIT — lower SLICE_PCT"

# Everything written from here on carries a strictly newer write timestamp.
GEN2_START_US=$(( $(date +%s) * 1000000 ))
sleep 2
stress_gen gen2 "$GEN2_PARTITIONS"
GEN2_SECONDS="$GEN_SECONDS"
GEN2_FLUSH_DATA_DB=$(data_db_count "$CDIR")
log "[gen2] Data.db count after flush: $GEN2_FLUSH_DATA_DB"

# Step 2: compact ONLY generation 2's flush files. NEVER `nodetool compact <ks>
# <tbl>` here — that would merge the generations and destroy the overlap.
declare -a GEN2_FILES=()
while read -r f; do
  [[ -n "$f" ]] || continue
  grep -Fxq "$f" <<<"$GEN1_FILES" || GEN2_FILES+=("$f")
done < <(list_data_db "$CDIR")
log "[gen2] ${#GEN2_FILES[@]} new file(s) belong to generation 2"
[[ "${#GEN2_FILES[@]}" -ge 1 ]] || die "[gen2] no new SSTable appeared — the second generation did not land"
if [[ "$COMPACT_GEN2" == 1 && "${#GEN2_FILES[@]}" -gt 1 ]]; then
  compact_user_defined "${GEN2_FILES[@]}"
fi
GEN2_DATA_DB=$(data_db_count "$CDIR")
log "[gen2] Data.db count after compaction: $GEN2_DATA_DB"

# Re-measured AFTER both compactions: the point is to prove the overlap SURVIVED
# them, so an earlier measurement would prove nothing about the published bytes.
read -r SLICE_ROWS_GEN2 SLICE_ROWS_NEW < <(slice_probe "$GEN2_START_US")
log "[gen2] token-slice probe: $SLICE_ROWS_GEN2 rows, $SLICE_ROWS_NEW of them written by generation 2"

# ------------------------------------------------------------------- asserts --
# (1) Merge depth must be the shape the issue asks for: at least 2 SSTables so a
#     merge runs at all, at most MAX_DATA_DB so an accidental k=25 does not
#     inflate the shared decode+merge floor (see K-DEPTH CONTROL in the header).
[[ "$GEN2_DATA_DB" -ge "$MIN_DATA_DB" ]] \
  || die "only $GEN2_DATA_DB Data.db file(s) survive (need >= $MIN_DATA_DB) — a merge-free corpus benches the wrong path"
[[ "$GEN2_DATA_DB" -le "$MAX_DATA_DB" ]] \
  || die "$GEN2_DATA_DB Data.db files survive (max $MAX_DATA_DB) — a deeper merge than the issue's 2-generation shape biases the vectorized-exec share downward"
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
# Fails closed if the published bytes are not LZ4 @ 16 KiB chunks.
verify_compression "$DEST" "$CORPUS_ROOT/compression-info.tsv"

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
            # An unreadable timestamp stays None: the manifest must not report a
            # fabricated 0 as a real minimum/maximum.
            mn = int(parts[1]) if parts[1].isdigit() else None
            mx = int(parts[2]) if parts[2].isdigit() else None
            ts[parts[0]] = (mn, mx)

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
