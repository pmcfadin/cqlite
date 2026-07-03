#!/usr/bin/env bash
# CQLite profiling orchestrator (docs/profiling.md).
#
# One entry point for the profile → fix → re-measure loop:
#
#   ./scripts/profile.sh baseline [name]      # measure + save criterion baseline (default: base)
#   ./scripts/profile.sh bench [name]         # measure + save baseline (default: current)
#   ./scripts/profile.sh flame [filter] [sec] # CPU flamegraphs via pprof (default: all, 10s each)
#   ./scripts/profile.sh heap                 # dhat heap profile vs the <128 MiB budget
#   ./scripts/profile.sh compare [new] [base] # gate check: new vs base (defaults: current, base)
#   ./scripts/profile.sh report               # ranked bottleneck report + history ledger
#
# Typical loop:
#   ./scripts/profile.sh baseline   # once, on the clean tree
#   ./scripts/profile.sh flame      # find the bottleneck, make a change, then:
#   ./scripts/profile.sh bench && ./scripts/profile.sh compare && ./scripts/profile.sh report

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$ROOT/test-data/datasets}"

# cli-helpers gates the read benches; write-support (a default) gates the write
# benches. Both fixture sets are deterministic (Epic #541).
FEATURES="cli-helpers,write-support"
# The bench targets that make up the profiling surface. m1_performance and
# fixtures_smoke exist but are not part of the gated loop; profile them
# explicitly with `cargo bench` if needed.
BENCH_TARGETS=(--bench read --bench write --bench partition_lookup --bench partition_lookup_scaling --bench concurrent_scan --bench read_while_write)

require_fixtures() {
    if ! compgen -G "$CQLITE_DATASETS_ROOT/sstables/test_basic/simple_table-*/nb-1-big-Data.db" > /dev/null; then
        echo "error: SSTable fixtures missing under $CQLITE_DATASETS_ROOT" >&2
        echo "fetch them first: bash test-data/scripts/fetch-datasets.sh" >&2
        exit 1
    fi
}

cmd="${1:-help}"
case "$cmd" in
    baseline)
        require_fixtures
        name="${2:-base}"
        cargo bench --package cqlite-core --features "$FEATURES" "${BENCH_TARGETS[@]}" \
            -- --save-baseline "$name"
        echo
        echo "baseline '$name' saved under target/criterion/"
        ;;

    bench)
        require_fixtures
        name="${2:-current}"
        cargo bench --package cqlite-core --features "$FEATURES" "${BENCH_TARGETS[@]}" \
            -- --save-baseline "$name"
        echo
        echo "baseline '$name' saved; next: ./scripts/profile.sh compare $name base"
        ;;

    flame)
        require_fixtures
        filter="${2:-}"
        seconds="${3:-10}"
        # --profile-time activates the pprof profiler attached in
        # benches/profiling/mod.rs; criterion skips its normal analysis and
        # samples each selected bench for $seconds instead.
        cargo bench --package cqlite-core --features "$FEATURES" "${BENCH_TARGETS[@]}" \
            -- --profile-time "$seconds" ${filter:+"$filter"}
        echo
        echo "flamegraphs written:"
        find target/criterion -name flamegraph.svg -newermt '-15 minutes' | sort || true
        ;;

    heap)
        require_fixtures
        # bench profile, not release: release strips symbols, which destroys
        # dhat's backtraces (Cargo.toml [profile.release] strip = true).
        cargo run --package cqlite-core --example heap_profile \
            --features cli-helpers,dhat-heap --profile bench
        ;;

    compare)
        new="${2:-current}"
        base="${3:-base}"
        python3 scripts/ci/check_perf_regression.py \
            target/criterion "$new" "$base" cqlite-core/benches/perf-gate.json
        ;;

    report)
        python3 scripts/profile_report.py
        ;;

    *)
        sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
        [ "$cmd" = "help" ] || exit 2
        ;;
esac
