#!/usr/bin/env bash
# Cold-vs-warm A/B for issue #2824 (madvise(WILLNEED) on the Auto-mmap scan plane).
#
# WHAT THIS MEASURES, AND WHAT IT DOES NOT
# ----------------------------------------
# Runs `ws0-scan-bench` over the ws0.events performance fixture with the page
# cache DROPPED before pass 1 (the COLD arm) and retained for passes 2..N (the
# WARM arm), once per supplied binary. Two binaries -> a baseline-vs-patched A/B.
#
# It does NOT produce issue #2824's acceptance-criterion number. AC1 asks for
# cold-p99 on a cold **i4i** scan; this script measures whatever host it is run
# on, and the host is recorded in the output for exactly that reason. On a
# c7i.4xlarge it is SUPPORTING evidence for the direction and the warm-regression
# check, never the i4i magnitude.
#
# The fixture is CQLite-written and CQLite-read and is a PERFORMANCE fixture
# only, never a correctness oracle (#3042). It is also UNCOMPRESSED (#1406), so
# it does not exercise the compressed-chunk read path.
#
# Requires: passwordless sudo for /proc/sys/vm/drop_caches (checked, fail-closed).
set -euo pipefail

CORPUS=""
PASSES=3
ROUNDS=3
OUT=""
declare -a BINARIES=()

usage() {
  cat <<'USAGE'
usage: cold-warm-ab.sh --corpus <dir> --out <dir> --bin <label>=<path> [--bin ...]
                       [--passes N] [--rounds N]

  --corpus   ws0.events corpus directory (see tools/ws0-corpus-gen/README.md)
  --out      directory for the recorded artifacts (created)
  --bin      a labelled ws0-scan-bench binary; repeat for each A/B arm
  --passes   scan passes per round; pass 1 is COLD, 2..N are WARM (default 3)
  --rounds   repetitions of the whole cold+warm sequence per binary (default 3)
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --corpus) CORPUS="${2:?--corpus needs a value}"; shift 2 ;;
    --out)    OUT="${2:?--out needs a value}"; shift 2 ;;
    --bin)    BINARIES+=("${2:?--bin needs label=path}"); shift 2 ;;
    --passes) PASSES="${2:?--passes needs a value}"; shift 2 ;;
    --rounds) ROUNDS="${2:?--rounds needs a value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "cold-warm-ab: unrecognized argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$CORPUS" ] || { echo "cold-warm-ab: --corpus is required" >&2; exit 2; }
[ -n "$OUT" ]    || { echo "cold-warm-ab: --out is required" >&2; exit 2; }
[ "${#BINARIES[@]}" -ge 1 ] || { echo "cold-warm-ab: at least one --bin is required" >&2; exit 2; }
[ -d "$CORPUS" ] || { echo "cold-warm-ab: corpus not a directory: $CORPUS" >&2; exit 2; }

# Fail CLOSED on drop_caches: a "cold" arm that silently ran warm is worse than
# no measurement, because it is indistinguishable from a real one in the output.
if ! sudo -n sh -c 'echo 1 > /proc/sys/vm/drop_caches' 2>/dev/null; then
  echo "cold-warm-ab: cannot drop the page cache (passwordless sudo required)." >&2
  echo "cold-warm-ab: refusing to run — a warm run labelled COLD is not a measurement." >&2
  exit 3
fi

mkdir -p "$OUT"

{
  echo "host: $(hostname)"
  echo "instance-type: $(curl -s -m 3 -H "X-aws-ec2-metadata-token: $(curl -s -m 3 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null)" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo UNKNOWN)"
  echo "kernel: $(uname -r)"
  echo "cores: $(nproc)"
  echo "mem-total-kb: $(awk '/MemTotal/{print $2}' /proc/meminfo)"
  echo "corpus: $CORPUS"
  echo "corpus-bytes: $(du -sb "$CORPUS" | cut -f1)"
  echo "passes: $PASSES  (pass 1 = COLD, 2..$PASSES = WARM)"
  echo "rounds: $ROUNDS"
  echo "started-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "NOTE: AC1 asks for a cold i4i scan. This host is recorded above; if it is"
  echo "NOTE: not an i4i, the i4i magnitude is UNMEASURED by this run."
} | tee "$OUT/host.txt"

for entry in "${BINARIES[@]}"; do
  label="${entry%%=*}"
  path="${entry#*=}"
  if [ "$label" = "$entry" ] || [ -z "$label" ] || [ -z "$path" ]; then
    echo "cold-warm-ab: --bin needs label=path, got: $entry" >&2; exit 2
  fi
  [ -x "$path" ] || { echo "cold-warm-ab: not executable: $path" >&2; exit 2; }

  echo "=== arm $label ($path) ==="
  for round in $(seq 1 "$ROUNDS"); do
    sync
    sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    log="$OUT/${label}.round${round}.txt"
    echo "--- $label round $round (page cache dropped) ---"
    "$path" --corpus "$CORPUS" --passes "$PASSES" > "$log" 2>&1 \
      || { echo "cold-warm-ab: arm $label round $round FAILED; see $log" >&2; exit 4; }
    tail -20 "$log"
  done
done

echo "finished-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/host.txt"
echo "artifacts: $OUT"
