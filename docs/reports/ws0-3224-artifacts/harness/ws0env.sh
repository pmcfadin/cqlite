# Harness environment for the #3224 capture — COMMITTED, unlike #3217's.
#
# #3217's llc-run.sh sourced /data/ws0/ws0env.sh, which was never committed: the
# variable NAMES were documented in harness/README.md but the exact staged-corpus
# path was not, so the run was not reproducible from the artefacts alone. This
# file closes that gap. Source it, do not copy values out of it by hand.
#
#   source docs/reports/ws0-3224-artifacts/harness/ws0env.sh
#
# WT is derived, not hardcoded, so this works from any checkout of the branch.

WT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
export WT

export WS0_ROOT=/data/ws0
export WS0_STAGE=/data/ws0/ws0-corpus/sstables
export WS0_FLIGHT_BIN="$WT/target/release/cqlite-flight"
export WS0_LOADGEN_BIN="$WT/target/release/flight-loadgen"
export WS0_TICKET_TPL="$WT/docs/reports/ws0-3100-artifacts/ws0-h2h/ws0-events-template.json"

# Server flags are fixed to #3100's recorded invocation (common.sh defaults
# match these; set explicitly so a reader of this file sees them).
# --batch-size 8192 is an AC2 REQUIREMENT — do not tune it.
export WS0_BATCH_SIZE=8192
export WS0_MAX_BATCH_BYTES=4194304
export WS0_MAX_INFLIGHT_EGRESS_BYTES=12582912
export WS0_MAX_CONCURRENT_SCANS=16
export WS0_ADMISSION_WAIT_TIMEOUT_MS=30000
export WS0_SEED=42

# Both endpoints live on NUMA node 0 (see common.sh's core-set table).
export WS0_NUMA_NODE=0

# The corpus regenerated on this host; see corpus/README.md.
# sha256(Data.db) = b1656ae8c0e45feb30f3da641b8a23c4969d1be43e5f341ef0af6bb3a9b41042
export CORPUS_ROWS=3999890
