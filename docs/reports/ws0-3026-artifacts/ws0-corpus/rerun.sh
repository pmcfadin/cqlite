#!/usr/bin/env bash
# WS0 end-to-end reproduction (CQLite issue #3026, WS0 of umbrella #3023).
#
# Brings up a stock single-node Cassandra 5.0.8, regenerates the throughput-grade
# corpus, and re-runs the two empirical claim verifications.
#
# Everything lives under /home/ubuntu/ws0/. Nothing here touches /home/ubuntu/workspace/
# (the git checkout) or writes to /data.
set -euo pipefail

CAS=/home/ubuntu/ws0/apache-cassandra-5.0.8
HERE=/home/ubuntu/ws0/ws0-corpus
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# ---------------------------------------------------------------------------
# 0. Prerequisites
# ---------------------------------------------------------------------------
#  - cqlsh is UNUSABLE on this box (it demands Python 3.6-3.11; the box has 3.12).
#    All CQL goes through cql.py, which needs the pip driver:
#        pip3 install --break-system-packages cassandra-driver
#  - passwordless sudo is needed for drop_caches + bpftrace in verify-claims.sh.

# ---------------------------------------------------------------------------
# 1. Config deltas from stock cassandra.yaml  (paths + addresses ONLY)
# ---------------------------------------------------------------------------
#    hints_directory        : /home/ubuntu/ws0/cassandra-data/hints
#    data_file_directories  : [/home/ubuntu/ws0/cassandra-data/data]
#    commitlog_directory    : /home/ubuntu/ws0/cassandra-data/commitlog
#    saved_caches_directory : /home/ubuntu/ws0/cassandra-data/saved_caches
#    listen_address         : 127.0.0.1   (was: localhost)
#    rpc_address            : 127.0.0.1   (was: localhost)
#    Exact diff vs the pristine file: $HERE/cassandra.yaml.diff
#    Pristine copy kept at: $CAS/conf/cassandra.yaml.stock
# NOTHING performance-relevant is changed in cassandra.yaml.

# ---------------------------------------------------------------------------
# 2. Start the node.
# ---------------------------------------------------------------------------
# MAX_HEAP_SIZE is the ONE non-path deviation from stock, and it is deliberate:
# cassandra-env.sh's auto-sizing picks HALF of system RAM here (half=15776MB is
# just under heap_limit=15872MB), i.e. a 15.4 GiB heap on a 30 GiB box with no
# swap. That daemon reached ~17 GB RSS and was chosen by the global OOM killer.
# 8G is close to the 7.5 GiB an operator would expect from the documented
# "1/4 of RAM" rule of thumb, so it is not a throughput-relevant deviation --
# but it IS a deviation and is reported as one.
mkdir -p /home/ubuntu/ws0/cassandra-data/{data,commitlog,hints,saved_caches} /home/ubuntu/ws0/cassandra-logs
cd "$CAS"
MAX_HEAP_SIZE=8G setsid nohup bin/cassandra -f \
  > /home/ubuntu/ws0/cassandra-logs/stdout.log 2>&1 < /dev/null &
disown
# HEAP_NEWSIZE is deliberately NOT set: this build uses G1 (jvm17-server.options
# has -XX:+UseG1GC), and cassandra-env.sh ignores/warns on HEAP_NEWSIZE under G1.

for i in $(seq 1 40); do bin/nodetool status >/dev/null 2>&1 && break; sleep 5; done
bin/nodetool status
bin/nodetool info | grep -i "heap memory"        # expect 8192.00 MB max

# ---------------------------------------------------------------------------
# 3. Generate the corpus.
# ---------------------------------------------------------------------------
# Args: partitions payload_B payload_pop bloba_B bloba_pop blobb_B blobb_pop \
#       seq_cluster et_cluster threads batch_partitions
#
# These exact arguments were reached by calibration (6 measured runs); they land
# 692.70 B/row uncompressed and a 3.535x LZ4 ratio. The knobs that matter:
#   * seq_cluster x et_cluster = 2 x 10 -> 20 rows/partition. cassandra-stress
#     derives regular-column values from (partition, LAST clustering value), so
#     seq_cluster is a value-DUPLICATION factor and a huge lever on the ratio
#     (4 x 5 measured 4.32x; 2 x 10 measured 3.02x at identical row width).
#   * blobb_pop=16 is the fine ratio knob (pop 5 -> 4.09x, 8 -> 3.85x, 16 -> 3.53x).
bash "$HERE/gen-corpus.sh" 200000 375 6 96 3 96 16 2 10 8 50000

# NOTE: gen-corpus.sh's final `SELECT count(*)` over the whole table FAILS on 4M
# rows (server-side range-read timeout). Use these two independent oracles instead:
#   sstablemetadata <Data.db> | grep totalRows      -> 3999890
#   python3 fullscan.py 512                         -> 3999890  (token-range split)

# ---------------------------------------------------------------------------
# 4. Verify geometry + the two claims.
# ---------------------------------------------------------------------------
D=$(dirname "$(ls -S /home/ubuntu/ws0/cassandra-data/data/ws0/events-*/*-Data.db | head -1)")
python3 "$HERE/measure-sstable.py" "$D"/*-CompressionInfo.db 3999890
bash "$HERE/verify-claims.sh"
