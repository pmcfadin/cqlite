#!/usr/bin/env bash
# Canonical agent gate (issue #719).
#
# This script IS the gate. A builder claiming "the gate passed" must have run
# this script and pasted its summary block verbatim; ad-hoc cargo invocations
# do not count. It exists because epic #646 shipped three false-green reports
# rooted in "which commands count as the gate" ambiguity (feature-gated tests
# silently skipping, filtered runs reported as full runs).
#
# Components mirror the enforced CI gates (.github/workflows/ci.yml,
# ci-minimal-features.yml, python-ci.yml) plus the local smoke suite:
#   fmt                cargo fmt --all --check
#   clippy             RUSTFLAGS="-D warnings" clippy, SCOPED per-package (issue
#                      #1844): whole-workspace lint that deliberately does NOT
#                      compile the source-built DuckDB C++ amalgamation
#                      (cqlite-cli `duckdb-tests`) or the OpenTelemetry/OTLP stack
#                      (`observability`/`observability-testing`) — both are pure
#                      per-gate tax (-D warnings gives clippy a distinct fingerprint,
#                      so no other component reuses them). parquet/arrow stay linted.
#                      Set CQLITE_CLIPPY_FULL=1 to run the historical
#                      `--workspace --all-targets --all-features` matrix instead; the
#                      nightly gate.yml deep-check sets it so the otel/duckdb-inclusive
#                      lint still runs within 24h (coverage moved, not deleted).
#   core-tests         cargo test -p cqlite-core --features cli-helpers (CI skip-list applied)
#   scan-offload-guard cargo test -p cqlite-core --features cli-helpers,scan-offload-probe
#                      --test issue_1143_scan_offload_thread (windowed-scan parse
#                      runs off the async worker pool; probe is feature-gated so
#                      the default core-tests run can't execute it — issue #1143).
#                      Also runs --test issue_1333_scan_scratch_reuse (the
#                      windowed scan's per-partition scratch Vec is reused, not
#                      reallocated per partition — issue #1333) and
#                      --test issue_1589_window_drain_bytes (the scan/compaction
#                      windows advance a cursor + compact once per refill instead
#                      of front-draining per partition — issue #1589); same gate.
#   memory-budget      cargo test -p cqlite-core --features cli-helpers,dhat-heap
#                      --test memory_budget -- --test-threads=1 (issue #1565, Epic
#                      A/A4). dhat allocation/peak-heap regression net over the real
#                      read path: pins today's measured full-scan total-bytes
#                      (~209 MB, ceiling 252 MB) and materializing peak-heap
#                      (~4.9 MB, ceiling 6 MB, also asserted < 128 MiB) as Epic-E
#                      ratchet targets. Requires the dhat-heap feature (installs the
#                      dhat global allocator, confined to this one test binary) and
#                      --test-threads=1 (dhat::Profiler is a process-global
#                      singleton). Dataset-dependent: fails closed on empty (each
#                      test asserts >=1 row before reading dhat stats).
#   integration-tests  cargo test -p cqlite-integration-tests: compile ALL targets
#                      (--no-run, whole package) then run the seven CI-enforced ones
#   format-compat      cargo test -p format-compatibility-tests (the 'oa' format crate;
#                      issue #865 folded it into the workspace so fmt/clippy reach it)
#   write-tests        cargo test -p cqlite-core --features write-support (lib + roundtrip + compaction)
#   cli-tests          cargo test -p cqlite-cli --test unit_tests + (write-support)
#                      write_readback_content_tests (CQL write→read content parity, #1231)
#   python-bindings    maturin develop + pytest bindings/python/tests in a throwaway
#                      venv; SKIPs (never silently PASSes) if python3 is unavailable.
#                      Set RUN_SLOW_TESTS=1 to also run the CLI-parity suite.
#                      The full pytest run includes the #1231 Python write→read
#                      content proof (test_write_readback_content.py), so a core
#                      write-format regression reds a binding content test.
#   node-bindings      napi build + the #1231 Node write→read content proof
#                      (npx jest write-readback-content) in bindings/node; SKIPs
#                      (never silently PASSes) if node/npm is unavailable. Scoped
#                      to the content proof (not full `npm test`) so it stays
#                      fast and corpus-free while still failing closed on a Node
#                      write-path regression (#1255).
#   parity-report      cassandra-parity report --check: FAILs (naming
#                      docs/reports/cassandra-test-parity.md) when the committed
#                      derived report drifts from a fresh render of
#                      test-data/cassandra-parity-manifest.yml. Catches the
#                      single-PR "changed the manifest, forgot to regenerate the
#                      report" case at the local gate, before push (issue #1338).
#                      SKIP-aware (loud, never silent PASS): SKIPs when the
#                      cassandra-parity crate (tools/cassandra-parity) or the
#                      manifest is absent (a minimal checkout). No Docker/datasets
#                      — reads the manifest + committed report only. NOTE: a stale
#                      report can ALSO arise post-merge from a semantic merge race
#                      (two manifest-changing PRs), which no per-PR/local check can
#                      see; that path self-heals via the push-to-main job in
#                      .github/workflows/cassandra-parity.yml (issue #1338).
#   binding-unwind-profile
#                      fail-closed guard (#1440): the shipped Python wheel and
#                      Node prebuild build definitions must select
#                      `--profile release-unwind` (PyO3/napi catch_unwind firewall
#                      active) and never `--release` (abort). Reads the four build
#                      definitions (python-release.yml, pyproject.toml [tool.maturin],
#                      package.json build script, node-release.yml); hard-FAILs on
#                      any abort-built or missing/unparseable definition. Pure
#                      bash/grep/awk — offline, deterministic, no datasets/network.
#   tooling-tests      shell-tooling regression tests (fast, no datasets/network):
#                      scripts/tests/test_agent_gate_summary.sh — proves the
#                      SUMMARY block survives non-foreground capture (#1175). It
#                      only drives `agent-gate.sh --emit-summary-selftest`, which
#                      exits before running any component, so there is no recursion.
#                      SKIP-aware: no python3 -> SKIP (the selftest's truncation
#                      assertion needs a python reader), never silent PASS.
#                      Also runs scripts/tests/test_generator_keyspace_scoping.sh
#                      (#1232) — fails if a generate-*.sh enumerates the whole
#                      SSTable corpus and grep -z filters by keyspace; needs no
#                      python3 so it runs even on the SKIP path, and any failure
#                      hard-FAILs this component. On the python3 path also runs
#                      scripts/tests/test_gate_concurrency_cap.sh (#1825) — proves
#                      the machine-wide full-gate concurrency cap queues at N,
#                      exempts --lite, and releases a slot on SIGKILL (uses the
#                      gate's hermetic stub mode, never real gate work).
#   minimal-build      cargo build -p cqlite-core --no-default-features --features all-compression
#   smoke              bash test-data/scripts/smoke-test-all-tables.sh
#   file-size          campsite-rule ratchet (epic #1116 / #1135): lists changed
#                      .rs files over threshold (800 src / 1500 test, total lines)
#                      and FAILs if a change makes an over-threshold file LARGER.
#                      Override an unavoidable growth with CQLITE_ALLOW_FILE_GROWTH=1.
#
# The integration-tests --no-run sweep, the format-compat component, and the
# python-bindings component close the three blind spots from issue #865: a
# compile break in a non-enumerated test target, a fmt/compile break in the
# (previously workspace-excluded) format-compatibility crate, and Python-only
# regressions (LIMIT 0, SET<TEXT> validation) that shipped "gate PASS".
#
# All components run even after a failure so one run reports everything.
# Exit code 0 iff every component passes. Machine-checkable output: the
# summary block between the AGENT-GATE SUMMARY markers, carrying a per-run
# "run-id:" line and ending in "RESULT: PASS" or "RESULT: FAIL".
#
# Usage:
#   scripts/agent-gate.sh             # full gate (the only run that counts)
#   scripts/agent-gate.sh --lite      # FAST ITERATION gate (issue #1821): runs
#                                     # ONLY file-size + fmt + scoped workspace clippy
#                                     # (-D warnings; same #1844 duckdb/otel-excluded
#                                     # scoping as the full gate) + BLAST-RADIUS-SCOPED tests
#                                     # (the touched package's --lib + the diff's
#                                     # new --test targets; NOT core-tests/write/
#                                     # cli/bindings/parity/smoke). ~1-5 min vs
#                                     # 12-25 min. It is NOT the gate of record and
#                                     # emits a DISTINCT "==== AGENT-GATE LITE
#                                     # SUMMARY ====" block (MODE: lite) so it can
#                                     # never be pasted as the full SUMMARY. The
#                                     # full gate MUST PASS once before merge. Its
#                                     # recovery default is .agent-gate-lite-summary.txt.
#                                     # A bindings/python diff routes scoped-tests to
#                                     # maturin develop + fast pytest (issue #1893), so
#                                     # python-diff rounds cost a maturin compile
#                                     # (seconds warm, ~1-3 min cold).
#   scripts/agent-gate.sh --delta <anchor> [--anchor-run-id <id>]
#                                    [--anchor-summary-file <path>]
#                                     # TEST/DOCS-ONLY RE-CERTIFICATION (issue #1892):
#                                     # after a full-gate PASS at <anchor>, re-certify
#                                     # a diff anchor..HEAD that touches ONLY test files
#                                     # (tests/ dirs, *_test(s).rs, __test__/,
#                                     # bindings/*/tests/) and/or docs (*.md, docs/,
#                                     # website/). FAIL-CLOSED: any production file in
#                                     # the diff REFUSES the re-cert (run the full gate).
#                                     # On pass it runs ONLY file-size + fmt + the diff's
#                                     # changed test targets and emits a DISTINCT
#                                     # "==== AGENT-GATE DELTA SUMMARY ====" block
#                                     # (MODE: delta) that names the gate of record
#                                     # (the full PASS at <anchor>) + the anchor run-id,
#                                     # so it can NEVER be pasted as a full SUMMARY.
#                                     # It is NOT the gate of record; any production
#                                     # change needs a fresh full gate. The nightly
#                                     # gate.yml deep-check is the standing backstop.
#                                     # Record BOTH the anchor's full SUMMARY and this
#                                     # DELTA block in the PR. Recovery default:
#                                     # .agent-gate-delta-summary.txt.
#   scripts/agent-gate.sh --list      # list full-gate components without running
#   scripts/agent-gate.sh --lite-list # list the --lite components without running
#   scripts/agent-gate.sh --delta-list # list the --delta components without running
#   scripts/agent-gate.sh --only fmt,clippy   # debugging aid; output is
#                                     # marked PARTIAL and never counts as the gate
#   scripts/agent-gate.sh --emit-summary-selftest
#                                     # print a representative SUMMARY block
#                                     # through the real emission path (fast, for
#                                     # regression tests — see scripts/tests/) and
#                                     # exit 0; never runs any gate component.
#
# Capturing the gate (issue #1175): the authoritative artifact is the block
# between the AGENT-GATE SUMMARY markers. Under non-foreground capture (a
# `script`/pty, a buffering wrapper, a "drain-until-EOF then write" reader, or a
# backgrounded pipeline) that streamed block can be lost if a gate component
# leaks a descendant that keeps the gate's stdout pipe open: the reader never
# sees EOF, gets killed by a timeout, and discards its in-memory buffer — even
# though the gate exited 0. (Detaching the gate's OWN stdout cannot fix this: a
# leaked child still holds its inherited copy of the pipe write-end open.)
#
# The defense is therefore a recovery path the caller can use WITHOUT reading the
# (possibly-lost) stream:
#   - The gate always writes the complete SUMMARY to a CALLER-KNOWN file whose
#     path the caller chose IN ADVANCE: $AGENT_GATE_SUMMARY_FILE if set, else the
#     stable repo-root default $PWD/.agent-gate-summary.txt (gitignored). A caller
#     can ALWAYS `cat` that file for the complete block even if stdout was 100%
#     lost — no need to parse the stream to learn where the file is. A RELATIVE
#     $AGENT_GATE_SUMMARY_FILE resolves against the caller's CURRENT directory
#     (the gate captures it before it cd's to the repo root); an ABSOLUTE path is
#     used verbatim.
#   - That file is INVALIDATED at startup with a "RESULT: INCOMPLETE" sentinel
#     stamped with this run's run-id, so a stale prior-run summary can never be
#     read as this run's result if the gate exits early or can't write (#1175).
#     Each SUMMARY block carries a "run-id:" line; the recovery file is trusted
#     only when it bears THIS run's run-id, defeating a stale-but-complete file.
#   - It also keeps a copy under $LOG_DIR for the logs bundle.
#
# CONCURRENCY (#1175 roborev): the default $PWD/.agent-gate-summary.txt is
# per-CHECKOUT, not per-run. If you run multiple gates concurrently IN THE SAME
# CHECKOUT, each MUST set a unique $AGENT_GATE_SUMMARY_FILE or they will clobber
# each other's recovery artifact. Separate worktrees are already isolated (each
# has a distinct repo root → a distinct default path); CQLite's normal model
# runs concurrent gates in separate worktrees, so this is a non-issue there. The
# `run-id:` line lets a caller that captured the invocation's run-id confirm it
# is reading the right run; a caller with NO expected run-id whose stream was
# lost cannot disambiguate two same-checkout runs and so MUST use a unique path.
# The streamed copy is best-effort (a plain `cat` of the file). The most robust
# streamed capture is still the foreground redirect:
#   bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
# but if that stream truncates, read the caller-known file — it is always complete.
set -uo pipefail

# Capture the caller's invocation CWD BEFORE we cd to the repo root (#1175
# roborev finding 1). A caller-provided RELATIVE AGENT_GATE_SUMMARY_FILE must
# resolve against the directory the caller ran us from — otherwise the caller
# reads ./gate.summary in its own CWD while the gate wrote <repo>/gate.summary,
# breaking the recovery contract. We resolve the relative path against this
# captured CWD just below, before any further directory change.
INVOCATION_CWD="$PWD"

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

# Agent sandboxes often run with a minimal PATH; pick up rustup's cargo.
if ! command -v cargo >/dev/null 2>&1 && [ -d "$HOME/.cargo/bin" ]; then
  export PATH="$HOME/.cargo/bin:$PATH"
fi

# sccache auto-detect (issue #1822): if sccache is available, use it as the
# rustc wrapper for incremental compilation cache. Each worktree keeps its own
# target/ dir (no lock contention); the shared object cache deduplicates
# compilation across worktrees. Disabled via CQLITE_DISABLE_SCCACHE=1.
# Cache location: $SCCACHE_DIR (default ~/.cache/sccache on Linux,
# ~/Library/Caches/Mozilla.sccache on macOS). Cache size limit:
# $SCCACHE_CACHE_SIZE (default 10 GiB; raise for multi-user builds).
# Measurement (issue #1822): 25.6% speedup on fresh worktrees with warm cache.
#
# Accelerator state (issue #1848): every optional accelerator the gate depends on
# records a state in ACCEL_* — `on` (detected & used), `absent` (NOT installed →
# a LOUD WARN with the one-line install command, so a machine is never silently
# 3x slower again), or `off` (intentionally disabled via CQLITE_DISABLE_*; no
# WARN). The states are stamped into a machine-checkable `accelerators:` line in
# the SUMMARY block so degradation is visible in the pasted block, not just
# scrollback. All WARN/banner text goes to STDERR: hidden hook modes (--classify-*)
# must keep STDOUT empty, and this detection runs before the hook dispatch.
ACCEL_SCCACHE=absent
if [ "${CQLITE_DISABLE_SCCACHE:-0}" = 1 ]; then
  ACCEL_SCCACHE=off
elif command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER=sccache
  export CARGO_INCREMENTAL=0
  ACCEL_SCCACHE=on
  echo "agent-gate: sccache detected; using as RUSTC_WRAPPER with CARGO_INCREMENTAL=0 (#1822)" >&2
else
  echo "agent-gate: WARN: sccache not installed — cross-worktree compile caching DISABLED (~25.6% slower fresh builds); install: brew install sccache (#1848)" >&2
fi
export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO_ROOT/test-data/datasets}"

# cargo-nextest auto-detect (issue #1737): the core-tests execution floor (the
# gate's single dominant cost) runs under `cargo nextest run`, which parallelizes
# across test binaries + cores (typically 2-4x vs serial `cargo test`).
# Auto-detected like sccache: absent on PATH -> the gate falls back to plain
# `cargo test` (identical test set, incl. doctests). Opt out with
# CQLITE_DISABLE_NEXTEST=1. nextest does NOT run doctests, so the nextest path
# additionally runs `cargo test --doc` so doctest coverage is never silently
# dropped (same package/feature selection + same skip).
NEXTEST=0
ACCEL_NEXTEST=absent
if [ "${CQLITE_DISABLE_NEXTEST:-0}" = 1 ]; then
  ACCEL_NEXTEST=off
elif command -v cargo-nextest >/dev/null 2>&1; then
  NEXTEST=1
  ACCEL_NEXTEST=on
  # Banner on STDERR: hidden hook modes (--classify-*, --scoped-test-cmd-noparser)
  # must keep STDOUT empty, and this detection runs before the hook dispatch (same
  # rule the sccache banner above already follows; #1821/#1825).
  echo "agent-gate: cargo-nextest detected ($(cargo nextest --version 2>/dev/null | head -1)); core-tests uses nextest + a cargo test --doc pass (#1737)" >&2
else
  # #1848: absent accelerator → LOUD WARN + one-line install command (STDERR).
  echo "agent-gate: WARN: cargo-nextest not installed — core-tests fall back to serial 'cargo test' (much slower long pole); install: brew install cargo-nextest (#1848)" >&2
fi

# Bounded component parallelism (issue #1737): independent gate components run
# concurrently in a worker pool capped at AGENT_GATE_JOBS, collapsing wall-clock
# toward the core-tests long pole WITHOUT oversubscribing the machine. Multiple
# worktree gates can run at once (and aarch64 emulation raises OOM risk), so this
# per-gate cap composes with the machine-wide bound of #1825. Default:
# min(4, ncpu/2), floor 1. Set AGENT_GATE_JOBS=1 for the legacy strictly
# sequential behavior. Concurrency is corruption-safe: cargo serializes builds on
# the shared target dir via its own advisory lock, sccache dedups the recompiles,
# datasets are read-only, and each component captures its own log + verdict to a
# file (see record_result) so interleaved stdout can never corrupt the
# deterministic end-of-run SUMMARY block.
_ncpu=$( { command -v nproc >/dev/null 2>&1 && nproc; } || sysctl -n hw.ncpu 2>/dev/null || echo 4 )
case "$_ncpu" in *[!0-9]*|'') _ncpu=4 ;; esac
_default_jobs=$(( _ncpu / 2 ))
[ "$_default_jobs" -gt 4 ] && _default_jobs=4
[ "$_default_jobs" -lt 1 ] && _default_jobs=1
AGENT_GATE_JOBS="${AGENT_GATE_JOBS:-$_default_jobs}"
case "$AGENT_GATE_JOBS" in *[!0-9]*|'') AGENT_GATE_JOBS=1 ;; esac
[ "$AGENT_GATE_JOBS" -lt 1 ] && AGENT_GATE_JOBS=1
# The bounded pool relies on `wait -n` (bash 4.3+). On older bash (e.g. macOS's
# stock /bin/bash 3.2) fall back to sequential execution rather than risk a
# busy-poll race; correctness is identical, only wall-clock differs.
#
# #1848: lanes are a gate accelerator too. lanes=on (parallel), lanes=serial
# (degraded by bash <4.3 → LOUD WARN + install command), or lanes=off (component
# parallelism intentionally not in play, e.g. AGENT_GATE_JOBS=1 or a low core
# count; no WARN).
ACCEL_LANES=off
if [ "$AGENT_GATE_JOBS" -gt 1 ]; then
  if [ "${BASH_VERSINFO[0]:-0}" -gt 4 ] || \
     { [ "${BASH_VERSINFO[0]:-0}" -eq 4 ] && [ "${BASH_VERSINFO[1]:-0}" -ge 3 ]; }; then
    ACCEL_LANES=on
  else
    # Banner on STDERR (see nextest note above): hidden hook modes must keep STDOUT
    # empty, and this runs before the hook dispatch — under stock bash 3.2 this
    # branch is always taken, so an stdout banner here corrupted --classify-* output.
    echo "agent-gate: WARN: bash <4.3 lacks 'wait -n' — gate components run SERIALLY (no parallel lanes; AGENT_GATE_JOBS=1); install: brew install bash (#1848)" >&2
    AGENT_GATE_JOBS=1
    ACCEL_LANES=serial
  fi
fi

# accelerators_line: the machine-checkable one-liner stamped into every SUMMARY
# block (full, lite, and the emission selftest). Values: on|absent|off|serial.
# See the ACCEL_* detection above (#1848).
accelerators_line() {
  printf 'accelerators: sccache=%s nextest=%s lanes=%s' \
    "${ACCEL_SCCACHE:-unknown}" "${ACCEL_NEXTEST:-unknown}" "${ACCEL_LANES:-unknown}"
}

# Static-golden mandate (coordinator directive for #1737): the local gate runs
# against STATIC GOLDENS only. The live Docker/Cassandra sstabledump parity tests
# (issue #911, the *_under_cassandra5_sstabledump cases) otherwise fire during
# core-tests whenever a Docker daemon + a cassandra:5.0* image are present, adding
# wall-clock and non-determinism (measured ~10s each on a warm image). We default
# CQLITE_SKIP_DOCKER_TESTS=1 so run_core_tests filters them out; that coverage
# still runs in the nightly/dispatch Docker CI lanes, and setting
# CQLITE_SKIP_DOCKER_TESTS=0 restores them here (when Docker is available).
export CQLITE_SKIP_DOCKER_TESTS="${CQLITE_SKIP_DOCKER_TESTS:-1}"

# --- Authoritative Cargo integration-test target mapping (issue #1821) --------
# roborev finding: a Bash `case` glob such as `*/tests/*.rs` also matches NESTED
# helper/module files (e.g. cqlite-core/tests/write_read_roundtrip/data_multi.rs,
# cqlite-cli/tests/common/mod.rs) that are NOT Cargo `--test` targets. Passing
# such a stem as `--test <stem>` makes --lite FAIL on valid helper-only changes.
# We therefore map a changed .rs file to a `--test` target ONLY via authoritative
# Cargo metadata (each integration target's exact src_path + name + required-features
# — no path/name heuristics). A metadata parser (jq OR python3) is a PREREQUISITE
# for per-`--test`-target selection: without one we cannot learn a target's
# required-features, and emitting a feature-gated target feature-less would make
# --lite FAIL spuriously in a minimal shell env (roborev round-3 finding). So when
# NEITHER jq nor python3 is available we emit NO `--test` targets at all — run_lite
# scopes to the touched packages' `--lib` only (safe: --lib carries no per-target
# required-features) and prints a note pointing at the full gate for integration
# coverage. Hand-parsing Cargo.toml for required-features would just be another
# heuristic, so we deliberately do not. These helpers use no Bash-4-only features
# (no associative arrays), so the whole --lite path runs under macOS's Bash 3.2.

# Emit "<abs_src_path>\t<pkg>\t<testname>\t<required-features>" for every Cargo
# test target (required-features comma-joined, empty when none), or nothing if
# metadata cannot be produced/parsed. A single src_path can appear on MULTIPLE
# lines: the workspace-root package `cqlite` and the `cqlite-integration-tests`
# crate both own the top-level tests/*.rs files, and every owning package's
# target must be runnable (issue #1821 roborev finding 1).
_test_target_index() {
  # Test hook (issue #1821 roborev round 3): force the no-metadata-parser path so
  # the tooling self-test can assert the parser-absent behaviour hermetically,
  # without PATH surgery on jq/python3/cargo.
  [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] && return 0
  local meta
  meta=$(cargo metadata --no-deps --format-version 1 2>/dev/null) || return 0
  [ -n "$meta" ] || return 0
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$meta" | jq -r \
      '.packages[] | .name as $p | .targets[]
       | select(.kind[] == "test")
       | "\(.src_path)\t\($p)\t\(.name)\t\((."required-features" // []) | join(","))"'
  elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$meta" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for p in d["packages"]:
    for t in p["targets"]:
        if "test" in t.get("kind", []):
            feats = ",".join(t.get("required-features") or [])
            print("%s\t%s\t%s\t%s" % (t["src_path"], p["name"], t["name"], feats))
'
  fi
}

# Read changed repo-relative paths on stdin; print "<pkg>|<testname>|<features>"
# for EVERY Cargo `--test` target that a changed path is (features comma-joined,
# possibly empty). A single path may emit MULTIPLE lines when several packages own
# it (root `cqlite` + `cqlite-integration-tests` both own top-level tests/*.rs) —
# all owners are emitted so none is silently dropped (issue #1821 finding 1).
# Nested helper/module files are excluded. Deterministic; Bash 3.2-safe.
#
# Authoritative Cargo metadata (jq OR python3) is REQUIRED: without a parser we
# cannot know a target's required-features, and emitting a feature-gated target
# feature-less would make --lite FAIL spuriously (roborev round-3 finding). So
# when metadata is unavailable this emits NOTHING — the caller (run_scoped_tests)
# then scopes to package --lib only and says so, rather than guessing targets.
classify_test_targets() {
  local index f abs hits
  index=$(_test_target_index)
  # No metadata parser (or metadata unavailable) -> emit no --test targets.
  [ -n "$index" ] || return 0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in *.rs) ;; *) continue ;; esac
    abs="$REPO_ROOT/$f"
    # ALL owning targets (no early exit), "<pkg>|<name>|<features>" per line.
    hits=$(printf '%s\n' "$index" \
      | awk -F'\t' -v p="$abs" '$1 == p { print $2 "|" $3 "|" $4 }')
    [ -n "$hits" ] && printf '%s\n' "$hits"
  done
}

# Emit "<abs_manifest_dir>\t<pkg>\t<has_lib>" for EVERY workspace package, or
# nothing when metadata cannot be produced/parsed. `has_lib` is 1 when the
# package has a library target that `cargo test --lib` can run (a target whose
# kind includes "lib" or "rlib"; a cdylib-only binding crate is 0). This is the
# single authoritative source of package ownership — it covers ALL members
# (core, cli, flight, parity, integration-tests, format-compat, tools/*,
# bindings/*, examples, the workspace-root `cqlite`), so no member can fall
# through a hand-maintained list (issue #1821 recurring roborev finding).
_package_index() {
  # Test hook: force the no-metadata-parser path hermetically (issue #1821).
  [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] && return 0
  local meta
  meta=$(cargo metadata --no-deps --format-version 1 2>/dev/null) || return 0
  [ -n "$meta" ] || return 0
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$meta" | jq -r \
      '.packages[]
       | (.manifest_path | sub("/[^/]+$"; "")) as $dir
       | (if any(.targets[]; (.kind[] == "lib") or (.kind[] == "rlib")) then 1 else 0 end) as $lib
       | "\($dir)\t\(.name)\t\($lib)"'
  elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$meta" | python3 -c '
import json, os, sys
d = json.load(sys.stdin)
for p in d["packages"]:
    dr = os.path.dirname(p["manifest_path"])
    lib = 1 if any(("lib" in t["kind"]) or ("rlib" in t["kind"]) for t in p["targets"]) else 0
    print("%s\t%s\t%d" % (dr, p["name"], lib))
'
  fi
}

# Given the package index (as $1) and changed repo-relative paths on stdin, print
# "<pkg>|<has_lib>" for the workspace package that OWNS each path: the package
# whose manifest directory is the LONGEST prefix of the path. The workspace-root
# package (manifest dir == repo root) is EXCLUDED as a path owner — its directory
# is a prefix of everything, so treating it as an owner would make it a degenerate
# catch-all for docs/scripts/config changes; it still enters the package set via
# test-target ownership when a top-level tests/*.rs it owns changes. Deterministic;
# one owner per path; Bash 3.2-safe (no associative arrays).
_owners_from_index() {
  local index=$1 f abs
  [ -n "$index" ] || return 0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    abs="$REPO_ROOT/$f"
    printf '%s\n' "$index" | awk -F'\t' -v path="$abs" -v root="$REPO_ROOT" '
      $1 == root { next }
      { if (substr(path, 1, length($1) + 1) == $1 "/" && length($1) > bl) { bl = length($1); best = $2 "|" $3 } }
      END { if (best != "") print best }'
  done
}

# Self-test / debug hook: map stdin paths -> "<pkg>|<has_lib>" via metadata-derived
# longest-prefix ownership. Empty when no metadata parser is available.
classify_package_owners() { _owners_from_index "$(_package_index)"; }

# Single source of truth for the no-parser fallback's scoped-test command (issue
# #1821 roborev): when NEITHER jq nor python3 is present we cannot derive package
# ownership from Cargo metadata, so we scope to `cqlite-core --lib`. Crucially this
# RUNS the core lib tests (no `--no-run`) — a compile-only check would give false
# confidence that tests passed. cli-helpers matches the full gate's core-tests.
# Both run_scoped_tests and the --scoped-test-cmd-noparser self-test hook use this.
_scoped_test_cmd_noparser() {
  echo "test -p cqlite-core --lib --features cli-helpers"
}

# The FAST python-binding tier --lite runs for a bindings/python diff (issue #1893)
# INSTEAD of the always-libpython-link-failing `cargo test -p cqlite-py`. cqlite-py
# is a pyo3 cdylib, so a plain `cargo test` on it never links libpython and gave
# --lite ZERO python signal on ~1/3 of binding diffs.
#
# REAL single source of truth (roborev job 1449, Medium): the executor in
# run_scoped_tests `eval`s EXACTLY these two component strings, and
# PYTHON_LITE_TIER_CMD — the plan string --classify-scoped-plan advertises and the
# self-test asserts — is composed from the SAME two components, so the advertised
# plan and the executed command can never drift. Never edit one side alone: change
# a component string and both the plan and the execution change together.
PYTHON_LITE_MATURIN_CMD="maturin develop --profile dev -m bindings/python/Cargo.toml"
PYTHON_LITE_PYTEST_CMD="pytest bindings/python/tests -m 'not slow' -q"
PYTHON_LITE_TIER_CMD="$PYTHON_LITE_MATURIN_CMD && $PYTHON_LITE_PYTEST_CMD"

# Python-tier verdict marker for the LITE SUMMARY block (roborev job 1450, Low):
# when a python-binding diff is in scope, the block itself must say what the tier
# did — especially a SKIP (offline/toolchain), where scoped-tests can read PASS
# while the python diff was NOT validated. A pasted green block that validated
# nothing must be detectable from the block alone, not scrollback. Set by
# run_scoped_tests; rendered by run_lite as a `python-tier:` line. Empty (no line)
# when the diff has no python-binding change.
PYTHON_TIER_NOTE=""

# Read changed repo-relative paths on stdin; emit the deduped set of owning Cargo
# workspace packages (one per line) — the union of path-owners + changed
# --test-target owners, derived from `cargo metadata`. Bash 3.2-safe (no
# associative arrays); empty when no metadata parser is available. Inner helper of
# classify_scoped_plan — THE single routing function consumed by both the --lite
# executor (run_scoped_tests) and the --classify-scoped-plan self-test hook.
_scoped_pkgset() {
  local changed index owners newtests pkgset="" key pkg tpkg
  changed=$(cat)
  index=$(_package_index)
  owners=$(printf '%s\n' "$changed" | _owners_from_index "$index")
  newtests=$(printf '%s\n' "$changed" | classify_test_targets)
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    pkg=${key%%|*}
    [ -n "$pkg" ] || continue
    printf '%s\n' "$pkgset" | grep -qxF "$pkg" || pkgset="${pkgset}${pkg}"$'\n'
  done <<<"$owners"
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    tpkg=${key%%|*}
    [ -n "$tpkg" ] || continue
    printf '%s\n' "$pkgset" | grep -qxF "$tpkg" || pkgset="${pkgset}${tpkg}"$'\n'
  done <<<"$newtests"
  printf '%s' "$pkgset" | awk 'NF'
}

# THE scoped-tests ROUTING function (issue #1893; single-sourced per roborev job
# 1450): map stdin changed paths -> the scoped-tests plan. Emits `rust-pkg: <pkg>`
# for every owning rust workspace package EXCEPT cqlite-py, and `python-tier: <cmd>`
# once when a bindings/python change is present (cqlite-py owns it — a pyo3 cdylib
# whose `cargo test` can never link libpython). Node (cqlite-node) and rust-only
# diffs are untouched. Deterministic; no side effects; does not invoke cargo test.
#
# TWO consumers, one computation: run_scoped_tests (the --lite executor) parses
# these lines to decide what to run, and the hidden `--classify-scoped-plan` hook
# exposes the same lines to the py-route self-tests — so the routing the tests
# assert IS the routing the executor performs, never a parallel copy.
classify_scoped_plan() {
  local pkgset python_diff=0 pkg
  pkgset=$(cat | _scoped_pkgset)
  while IFS= read -r pkg; do
    [ -n "$pkg" ] || continue
    if [ "$pkg" = cqlite-py ]; then python_diff=1; continue; fi
    echo "rust-pkg: $pkg"
  done <<<"$pkgset"
  [ "$python_diff" -eq 1 ] && echo "python-tier: $PYTHON_LITE_TIER_CMD"
  return 0
}

# _delta_is_allowed_path <path> (issue #1892): TRUE (0) iff the path is a TEST file
# or DOCS file per the delta-recert policy allowlist; FALSE (non-0) for everything
# else. FAIL-CLOSED by construction — only an explicit test/docs match is allowed,
# so any src, script, workflow, Cargo.*, or config change falls through to the
# refusal path. Test classes: any `tests/` directory (covers cqlite-*/tests/,
# bindings/*/tests/, top-level tests/), any `__test__/` directory (node jest), and
# `*_test.rs` / `*_tests.rs`. Docs classes: any `*.md`, anything under `docs/`,
# anything under `website/`. Defined before the arg-parse case so the hidden
# --delta-classify hook (and run_delta) can call it. Bash 3.2-safe (case globs).
_delta_is_allowed_path() {
  case "$1" in
    # docs
    *.md) return 0 ;;
    docs/*|*/docs/*) return 0 ;;
    website/*|*/website/*) return 0 ;;
    # tests
    tests/*|*/tests/*) return 0 ;;
    __test__/*|*/__test__/*) return 0 ;;
    *_test.rs|*_tests.rs) return 0 ;;
    *) return 1 ;;
  esac
}

# Hidden self-test hook (issue #1892): read changed repo-relative paths on stdin,
# print "ALLOW <path>" / "REFUSE <path>" per path (fail-closed classification via
# _delta_is_allowed_path), then a final "VERDICT: ALLOW" (all test/docs) or
# "VERDICT: REFUSE" (>=1 production file). Pure function — no git, cargo, or tree
# mutation — so scripts/tests can assert the refusal decision hermetically.
delta_classify_stdin() {
  local f verdict=ALLOW
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if _delta_is_allowed_path "$f"; then
      echo "ALLOW $f"
    else
      echo "REFUSE $f"; verdict=REFUSE
    fi
  done
  echo "VERDICT: $verdict"
}

COMPONENTS=(file-size fmt clippy core-tests tombstones-scan scan-offload-guard memory-budget integration-tests format-compat write-tests cli-tests compaction-byte-parity python-bindings node-bindings delivery-telemetry parity-report binding-unwind-profile tooling-tests minimal-build smoke)
# --lite (issue #1821) runs ONLY this fast subset: file-size ratchet, fmt,
# FULL-workspace clippy (cross-crate API breaks are the cheap-insurance class),
# and blast-radius-scoped tests (the touched package's --lib + the diff's new
# test targets), NOT the full core-tests/write/cli/bindings/parity set. It is the
# FAST ITERATION loop, NOT the gate of record — the full gate must PASS once
# before merge. See run_lite() below.
LITE_COMPONENTS=(file-size fmt clippy scoped-tests)
# --delta (issue #1892): TEST/DOCS-ONLY RE-CERTIFICATION after a full-gate PASS.
# Given an anchor (the commit the full gate PASSed at), it verifies the diff
# anchor..HEAD touches ONLY test files and/or docs (FAIL-CLOSED if any production
# file changed), then re-certifies with this fast subset — file-size + fmt + the
# diff's changed test targets. It is NOT the gate of record: the gate of record
# remains the full agent-gate.sh PASS at the anchor, recorded alongside the delta
# evidence in the PR. The standing backstop is the nightly full run on main
# (.github/workflows/gate.yml deep-check). See run_delta() below.
DELTA_COMPONENTS=(file-size fmt scoped-tests)
ONLY=""
SELFTEST=0
LITE=0
DELTA=0
DELTA_ANCHOR=""
DELTA_ANCHOR_RUN_ID=""
DELTA_ANCHOR_SUMMARY_FILE=""
# Optional base-ref override (issue #1892): run_file_size and run_scoped_tests
# resolve their diff base from this when set, instead of merge-base with main.
# --delta points it at the anchor commit so the ratchet + scoping cover exactly
# the anchor..HEAD test/docs diff. Empty everywhere else (unchanged behavior).
GATE_BASE_OVERRIDE=""
case "${1:-}" in
  --list) printf '%s\n' "${COMPONENTS[@]}"; exit 0 ;;
  # --lite alone runs the fast gate; `--lite --emit-summary-selftest` drives the
  # LITE summary block through the real emission path (for tooling-tests) without
  # running any component.
  --lite) LITE=1; [ "${2:-}" = --emit-summary-selftest ] && SELFTEST=1 ;;
  --lite-list) printf '%s\n' "${LITE_COMPONENTS[@]}"; exit 0 ;;
  --delta-list) printf '%s\n' "${DELTA_COMPONENTS[@]}"; exit 0 ;;
  # --delta <anchor> [--anchor-run-id <id>] [--anchor-summary-file <path>]
  #                  [--emit-summary-selftest]
  # Re-certify a test/docs-only diff anchor..HEAD (issue #1892). The anchor is the
  # commit the full gate PASSed at. The anchor's full-gate run-id is recorded from
  # --anchor-run-id, else read from --anchor-summary-file (which must itself be a
  # FULL-gate PASS block — a lite/delta block cannot anchor a delta re-cert).
  --delta)
    DELTA=1
    DELTA_ANCHOR="${2:?--delta needs an anchor commit/sha (the commit the full gate PASSed at)}"
    shift 2 || true
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --anchor-run-id) DELTA_ANCHOR_RUN_ID="${2:?--anchor-run-id needs a value}"; shift 2 ;;
        --anchor-summary-file) DELTA_ANCHOR_SUMMARY_FILE="${2:?--anchor-summary-file needs a path}"; shift 2 ;;
        --emit-summary-selftest) SELFTEST=1; shift ;;
        *) echo "unknown --delta option: $1" >&2; exit 2 ;;
      esac
    done
    ;;
  # Hidden self-test hook (issue #1821): map stdin paths -> "<pkg>|<testname>"
  # for actual Cargo test targets (nested helpers excluded). No side effects.
  --classify-test-targets) classify_test_targets; exit 0 ;;
  # Hidden self-test hook (issue #1821): map stdin paths -> "<pkg>|<has_lib>"
  # via metadata-derived longest-prefix package ownership. No side effects.
  --classify-package-owners) classify_package_owners; exit 0 ;;
  # Hidden self-test hook (issue #1821 roborev): print the no-parser fallback's
  # scoped-test command so the self-test can assert it RUNS tests (--lib, never
  # --no-run). No side effects; does not invoke cargo.
  --scoped-test-cmd-noparser) echo "cargo $(_scoped_test_cmd_noparser)"; exit 0 ;;
  # Hidden self-test hook (issue #1893): map stdin changed paths -> the scoped-tests
  # PLAN ("rust-pkg: <pkg>" / "python-tier: <cmd>") WITHOUT running cargo/maturin, so
  # the self-test can assert python diffs route to the maturin+pytest tier.
  --classify-scoped-plan) classify_scoped_plan; exit 0 ;;
  # Hidden self-test hook (issue #1892): classify stdin paths as test/docs (ALLOW)
  # or production (REFUSE) and print a final VERDICT. No side effects; no cargo/git.
  --delta-classify) delta_classify_stdin; exit 0 ;;
  --only) ONLY="${2:?--only needs a comma-separated component list}" ;;
  --emit-summary-selftest) SELFTEST=1 ;;
  "") ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

# Summary-block markers + optional MODE line (issue #1821). The DEFAULT (full
# gate) values are the historical literals, so a no-flag run's output is
# byte-for-byte unchanged. --lite swaps in DISTINCT markers plus a MODE line so a
# lite summary can NEVER be mistaken for — or pasted as — the full gate's SUMMARY
# (which remains the only run that counts). Everything that writes/greps the block
# uses these variables; for LITE=0 they equal the old literals exactly.
SUMMARY_START_MARKER="==== AGENT-GATE SUMMARY ===="
SUMMARY_END_MARKER="==== END AGENT-GATE SUMMARY ===="
SUMMARY_MODE_LINE=""
if [ "$LITE" -eq 1 ]; then
  SUMMARY_START_MARKER="==== AGENT-GATE LITE SUMMARY ===="
  SUMMARY_END_MARKER="==== END AGENT-GATE LITE SUMMARY ===="
  SUMMARY_MODE_LINE="MODE: lite (FAST ITERATION — NOT the gate of record; full agent-gate.sh must PASS once before merge)"
elif [ "$DELTA" -eq 1 ]; then
  # DISTINCT delta markers + a MODE line naming the gate of record (issue #1892):
  # a delta summary can NEVER be mistaken for — or pasted as — a full SUMMARY. The
  # gate of record remains the full agent-gate.sh PASS at the anchor.
  SUMMARY_START_MARKER="==== AGENT-GATE DELTA SUMMARY ===="
  SUMMARY_END_MARKER="==== END AGENT-GATE DELTA SUMMARY ===="
  SUMMARY_MODE_LINE="MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor $DELTA_ANCHOR)"
fi

LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate.XXXXXX")
# Per-run nonce (#1175 roborev finding 1): the LOG_DIR is a fresh per-run mktemp
# path, so it uniquely identifies THIS invocation. We stamp it into every SUMMARY
# block as `run-id:` so completeness can be verified for THIS run, never a stale
# prior run's file that happens to still contain an old complete block.
RUN_ID="$LOG_DIR"
# Caller-known summary path (#1175): the caller may pick the path IN ADVANCE via
# AGENT_GATE_SUMMARY_FILE; otherwise we use a stable, documented repo-root default
# the caller can `cat` without parsing stdout. This is THE recovery contract: the
# complete SUMMARY is always at this exact path even if the streamed copy is lost.
# CONCURRENCY (#1175): this default is per-CHECKOUT, shared by every gate run in
# the same $REPO_ROOT. Concurrent same-checkout runs MUST each set a unique
# AGENT_GATE_SUMMARY_FILE or they clobber each other's recovery artifact;
# separate worktrees get distinct repo roots and are already isolated.
# The lite run uses a DISTINCT default recovery filename (issue #1821) so it can
# never clobber the full gate's recovery artifact, and so `cat`-ing the default
# after a lite run can never be misread as the full gate's result.
if [ "$LITE" -eq 1 ]; then
  SUMMARY_FILE="${AGENT_GATE_SUMMARY_FILE:-$REPO_ROOT/.agent-gate-lite-summary.txt}"
elif [ "$DELTA" -eq 1 ]; then
  # DISTINCT delta recovery filename (issue #1892) so a delta run can never clobber
  # the full or lite recovery artifact, and `cat`-ing it can never be misread as
  # the full gate's result.
  SUMMARY_FILE="${AGENT_GATE_SUMMARY_FILE:-$REPO_ROOT/.agent-gate-delta-summary.txt}"
else
  SUMMARY_FILE="${AGENT_GATE_SUMMARY_FILE:-$REPO_ROOT/.agent-gate-summary.txt}"
fi
# Resolve a caller-provided RELATIVE AGENT_GATE_SUMMARY_FILE against the caller's
# original CWD, not the repo root we cd'd into (#1175 roborev finding 1). Absolute
# paths are used verbatim; the unset default above is already absolute.
case "$SUMMARY_FILE" in
  /*) ;; # absolute (incl. the repo-root default) -> use verbatim
  *)  SUMMARY_FILE="$INVOCATION_CWD/$SUMMARY_FILE" ;;
esac
# Keep a copy under the logs bundle for archival.
LOG_SUMMARY_FILE="$LOG_DIR/summary.txt"
declare -a NAMES=() STATUSES=() TIMES=()
OVERALL=PASS

# Set to 1 by emit_summary if the authoritative caller-known summary file could
# NOT be written completely (bad path, perms, disk full, truncated write). The
# final exit logic forces a non-zero / FAIL outcome on this so a green gate can
# never silently lack its promised recovery artifact (#1175 roborev finding 1).
SUMMARY_WRITE_FAILED=0

# Startup invalidation (#1175 roborev finding 2): a stale .agent-gate-summary.txt
# from a PREVIOUS run must never survive into THIS run. If the current run exits
# early (dataset preflight fail, any pre-emit `exit 1`) or can't write later, a
# caller reading the recovery path would otherwise see an OLD complete PASS block
# as if it were this run's result. So, as early as possible — before the dataset
# preflight and before any component — overwrite the caller-known file with an
# INCOMPLETE sentinel stamped with THIS run's run-id. emit_summary replaces it
# with the real block on normal completion. Best-effort: if we cannot write the
# sentinel (unwritable path) we do not abort here; emit_summary's authoritative
# write guard catches an unwritable path at the end and forces a FAIL.
{
  echo "$SUMMARY_START_MARKER"
  echo "run-id: $RUN_ID"
  [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
  echo "RESULT: INCOMPLETE (gate did not finish)"
  echo "$SUMMARY_END_MARKER"
} > "$SUMMARY_FILE" 2>/dev/null || true

# emit_summary <result> [meta-line ...]
#
# Build the canonical SUMMARY block (start marker .. RESULT .. end marker) ONCE
# and write it to the CALLER-KNOWN file with plain redirection (no pipe), so it is
# complete regardless of stdout state — a closed-stdout SIGPIPE can never truncate
# a file written by `>`. The caller chose this path in advance (or knows the
# documented default), so it can recover the complete block without ever reading
# the stream (#1175). After writing, best-effort `cat` it to stdout for the
# foreground/redirect case. That is the whole emission: there is no stdout
# fd-detach, because detaching the gate's own stdout cannot close the pipe copy a
# leaked descendant already inherited. Both the real run and the
# --emit-summary-selftest mode go through this single function.
#
# Authoritative-write guard (#1175 roborev finding 1): if the caller-known file
# cannot be opened/written (bad path, missing parent dir, perms, disk full) or
# ends up incomplete (no end marker), that MUST NOT pass silently — the recovery
# artifact is the whole contract. We still compute and print the correctness
# verdict (least surprising), but we set SUMMARY_WRITE_FAILED=1 and print a LOUD
# warning to STDERR (more likely to survive than stdout under a leaked-child/pty
# capture). The caller's exit logic turns SUMMARY_WRITE_FAILED into a non-zero
# exit so a green gate never silently lacks its summary file.
emit_summary() {
  local result="$1"; shift
  # Write the complete block to the caller-known file FIRST, with plain
  # redirection (no pipe). This is the authoritative artifact and the advertised
  # recovery path. Capture stderr from the redirection so we can report WHY the
  # write failed (e.g. "No such file or directory", "Permission denied").
  # Capture BOTH the redirection's exit status and its stderr. The write rc is the
  # primary signal (#1175 roborev finding 1): a non-zero rc means the `>` could not
  # open/write the file, so we must NOT trust whatever is on disk — it may be a
  # stale prior-run block that survives the non-empty/end-marker checks. We grab
  # the rc of the redirected command group via the trailing `; printf` trick so it
  # is the redirection's status, not the `$(...)` substitution's.
  local write_err write_rc
  write_err=$(
    {
      echo
      echo "$SUMMARY_START_MARKER"
      echo "run-id: $RUN_ID"
      [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE"
      echo "RESULT: $result"
      echo "$SUMMARY_END_MARKER"
    } > "$SUMMARY_FILE" 2>&1
    printf '\037rc=%d' "$?"
  ) || true
  # Split the captured rc sentinel (\037 unit-separator) off the tail.
  write_rc="${write_err##*$'\037'rc=}"
  write_err="${write_err%$'\037'rc=*}"
  case "$write_rc" in (*[!0-9]*|'') write_rc=1 ;; esac

  # Verify the authoritative file: the WRITE must have succeeded (rc 0) AND the
  # file must hold the COMPLETE block FOR THIS RUN — non-empty, end marker present,
  # and stamped with THIS run's run-id. The run-id check is what defeats a stale
  # prior-run file: an unwritable path with an OLD complete PASS block on disk
  # would pass the non-empty + end-marker checks, but its run-id is a DIFFERENT
  # run's, so it is correctly rejected as a failed write (#1175 finding 1).
  local reason=""
  if [ "$write_rc" -ne 0 ]; then
    reason="write failed (rc=$write_rc)${write_err:+: $write_err}"
  elif [ ! -s "$SUMMARY_FILE" ]; then
    reason="${write_err:-file missing or empty}"
  elif ! grep -qF "$SUMMARY_END_MARKER" "$SUMMARY_FILE" 2>/dev/null; then
    reason="incomplete write (end marker missing)${write_err:+: $write_err}"
  elif ! grep -qF "run-id: $RUN_ID" "$SUMMARY_FILE" 2>/dev/null; then
    reason="stale file (run-id of this run not found — write did not land)${write_err:+: $write_err}"
  fi
  if [ -n "$reason" ]; then
    SUMMARY_WRITE_FAILED=1
    # LOUD, on STDERR (survives better than stdout under non-foreground capture).
    echo "⚠️ agent-gate: could not write complete summary file $SUMMARY_FILE ($reason)" >&2
    echo "⚠️ agent-gate: recovery artifact is MISSING — gate result forced to FAIL (#1175)" >&2
  fi

  # The RESULT printed in any fallback block MUST match the process exit. Once the
  # authoritative write failed, the gate's exit logic forces a non-zero exit, so
  # the fallback blocks (log + stdout) must say RESULT: FAIL — never the computed
  # PASS — otherwise a consumer parsing the machine-checkable block sees a FALSE
  # GREEN while the process exits non-zero (#1175 roborev finding 1).
  local emit_result="$result"
  if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
    emit_result=FAIL
  fi

  # Keep a copy in the logs bundle (best-effort; the caller-known file is the
  # contract). NEVER copy a stale/failed caller-known file into the log: when the
  # authoritative write failed, $SUMMARY_FILE may still hold a complete-looking
  # prior-run block (e.g. an old "RESULT: PASS"), and copying it would produce a
  # misleading log artifact for THIS run (#1175 finding 1). Only copy the on-disk
  # file when the write was verified successful; otherwise write THIS run's block
  # (this run's run-id + real RESULT) directly to the log so the artifact always
  # reflects the current run, never a stale one.
  if [ "$SUMMARY_WRITE_FAILED" -eq 0 ]; then
    cp "$SUMMARY_FILE" "$LOG_SUMMARY_FILE" 2>/dev/null || true
  else
    {
      echo
      echo "$SUMMARY_START_MARKER"
      echo "run-id: $RUN_ID"
      [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE (WRITE FAILED — see stderr)"
      echo "RESULT: $emit_result"
      echo "$SUMMARY_END_MARKER"
    } > "$LOG_SUMMARY_FILE" 2>/dev/null || true
  fi

  # Best-effort stream the (already-complete) file to stdout for the
  # foreground/redirect case. If stdout is gone (closed pipe -> SIGPIPE) or a
  # leaked child has starved an until-EOF reader, this may be lost — that is
  # fine: the caller-known file above is always complete. If the file itself is
  # bad we already warned on stderr; fall back to streaming the intended block so
  # the verdict still reaches a foreground caller.
  if [ "$SUMMARY_WRITE_FAILED" -eq 0 ]; then
    cat "$SUMMARY_FILE" 2>/dev/null || true
  else
    {
      echo
      echo "$SUMMARY_START_MARKER"
      echo "run-id: $RUN_ID"
      [ -n "$SUMMARY_MODE_LINE" ] && echo "$SUMMARY_MODE_LINE"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE (WRITE FAILED — see stderr)"
      echo "RESULT: $emit_result"
      echo "$SUMMARY_END_MARKER"
    } 2>/dev/null || true
  fi
}

# --emit-summary-selftest: prove the SUMMARY block survives capture without
# running the (5-8 min) gate. Emits a representative block through the exact
# emit_summary path the real run uses, then exits 0. Used by
# scripts/tests/test_agent_gate_summary.sh.
if [ "$SELFTEST" -eq 1 ]; then
  NAMES=(fmt clippy core-tests smoke)
  STATUSES=(PASS PASS PASS PASS)
  TIMES=(1s 2s 3s 4s)
  meta=(
    "commit: selftest branch: selftest dirty: no"
    "datasets: 0 Data.db files under (selftest)"
    "ci-pins: (selftest)"
    "$(accelerators_line)"
  )
  for i in "${!NAMES[@]}"; do
    meta+=("$(printf '%-18s %s (%s)' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}")")
  done
  emit_summary PASS "${meta[@]}"
  # Even the selftest must not exit 0 if it could not write its summary file —
  # the whole point of the selftest is to prove the recovery artifact is produced.
  [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || exit 1
  exit 0
fi

# record_result <name> <status> <seconds>
# Components may run concurrently in the bounded pool (issue #1737). A backgrounded
# subshell CANNOT mutate the parent's NAMES/STATUSES/TIMES arrays or OVERALL, so
# every component writes its own verdict to a per-component result file; the parent
# reconstructs the summary arrays (in canonical COMPONENTS order) after the pool
# drains. This keeps the SUMMARY block deterministic regardless of finish order.
record_result() { # <name> <status> <seconds>
  printf '%s %s\n' "$2" "$3" > "$LOG_DIR/$1.result"
}

# run_clippy: the `clippy` component's command (issue #1844). By default it runs a
# SCOPED per-package clippy that lints the whole workspace with -D warnings WITHOUT
# compiling two costly, gate-irrelevant artifacts on every run/worktree:
#   * the source-built DuckDB C++ amalgamation (cqlite-cli `duckdb-tests` feature),
#   * the full OpenTelemetry/OTLP stack (`observability`/`observability-testing` on
#     cqlite-core/cli/flight/bindings — both the tonic AND reqwest transports).
# `--workspace --all-features` would enable EVERY feature on EVERY package and pull
# in both. `-D warnings` alone already gives clippy a distinct compile fingerprint,
# so those artifacts are never reused by any other component — pure per-gate tax.
#
# parquet/arrow are NOT excluded: they are reachable in normal builds (cqlite-cli's
# cli-helpers→state_machine→cqlite-core/parquet chain), so they stay linted here.
# ONLY duckdb + otel move to the nightly backstop.
#
# Coverage of the excluded features is NOT deleted — it moves to a nightly full
# matrix: set CQLITE_CLIPPY_FULL=1 to run the historical
# `--workspace --all-targets --all-features` pass instead. `.github/workflows/gate.yml`
# (the nightly deep-check) sets it, so the full otel/duckdb-inclusive lint still runs
# within 24h. The explicit per-package feature lists below can drift as features are
# added; that nightly `--all-features` pass is the backstop that catches any omission.
run_clippy() {
  if [ "${CQLITE_CLIPPY_FULL:-0}" = 1 ]; then
    env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
    return
  fi

  # (1) Whole workspace at all-features, EXCLUDING the five packages that carry the
  #     duckdb/otel optional features. --all-features only turns on features of the
  #     SELECTED packages, so with these excluded no `duckdb-tests`/`observability`
  #     feature is ever activated — and cqlite-core, built here only as a transitive
  #     dependency of the remaining crates, never gets its `observability` feature.
  env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features \
    --exclude cqlite-core --exclude cqlite-cli --exclude cqlite-flight \
    --exclude cqlite-py --exclude cqlite-node || return 1

  # (2) cqlite-core: every feature EXCEPT observability/observability-testing/metrics
  #     (the OpenTelemetry stack). Keep this in sync with cqlite-core/Cargo.toml when
  #     features are added; the nightly CQLITE_CLIPPY_FULL=1 pass is the drift guard.
  env RUSTFLAGS="-D warnings" cargo clippy -p cqlite-core --all-targets --features \
"all-compression,antlr,arrow,bench-internals,benchmarks,ci_zero_tolerance,cli-helpers,deflate,delta-scan,dhat-heap,docker-integration,enhanced-index-validation,events,experimental,extended-index-validation,fuzz,legacy-heuristics,lz4,parquet,pest,scan-offload-probe,snappy,state_machine,test-coverage-tracking,test-infrastructure,test-property-testing,test-quality-gates,test-schema-validation,tombstones,unit-tests-only,wasm,work-counters,write-support,zstd" \
    || return 1

  # (3) cqlite-cli: every feature EXCEPT duckdb-tests + observability. Pulls in
  #     parquet/arrow via state_machine and delta-scan via delta-export, so the
  #     normal-build reachable surface stays linted.
  env RUSTFLAGS="-D warnings" cargo clippy -p cqlite-cli --all-targets --features \
"benchmarks,ci_zero_tolerance,cli-helpers,delta-export,experimental,integration-tests,interactive,state_machine,tui,write-support" \
    || return 1

  # (4) cqlite-flight + the Python/Node bindings at their DEFAULT features (none of
  #     which enable observability), plus cqlite-node's write-support code path. This
  #     lints their real binding/connector surface without linking the otel shim.
  #
  #     INVARIANT (issue #1893): cqlite-py MUST stay in this linted set. --lite's
  #     python tier classifies a venv/pip/maturin toolchain failure as SKIP (not
  #     FAIL) precisely because this clippy pass still COMPILES cqlite-py in the
  #     same lite run — it is the compile backstop that makes the SKIP safe.
  #     Removing cqlite-py here would let a broken bindings/python/src build sail
  #     through an offline --lite green.
  env RUSTFLAGS="-D warnings" cargo clippy --all-targets \
    -p cqlite-flight -p cqlite-py -p cqlite-node --features cqlite-node/write-support \
    || return 1
}

run_component() { # run_component <name> <cmd...>
  local name="$1"; shift
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  echo ">>> [$name] $*"
  start=$(date +%s)
  if "$@" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# python-bindings: build the extension with maturin and run pytest. Unlike the
# Rust components this is SKIP-aware: if there is no usable python3 the component
# records SKIP (loudly, never silently PASS) so a missing toolchain can't mask a
# real Python regression the way it did pre-#865. Anything else (venv/build/test
# failure) is a hard FAIL.
run_python_bindings() {
  local name=python-bindings
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  # Persistent venv under target/ so repeat runs skip the maturin/pytest install.
  local venv="$REPO_ROOT/target/agent-gate-venv"
  echo ">>> [$name] maturin develop + pytest (venv: $venv, RUN_SLOW_TESTS=${RUN_SLOW_TESTS:-0})"
  if RUN_SLOW_TESTS="${RUN_SLOW_TESTS:-0}" bash -c '
      set -euo pipefail
      venv="'"$venv"'"
      [ -x "$venv/bin/python" ] || python3 -m venv "$venv"
      . "$venv/bin/activate"
      pip install --quiet --upgrade pip >/dev/null
      pip install --quiet maturin pytest
      maturin develop -m bindings/python/Cargo.toml
      pytest bindings/python/tests -q' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# node-bindings: build the napi-rs native module and run the #1231 Node
# write→read CONTENT proof. Symmetric to run_python_bindings and SKIP-aware:
# if there is no node/npm on PATH the component records SKIP (loudly, never
# silently PASS) so a missing toolchain can't mask a real Node write-path
# regression. Anything else (install/build/test failure) is a hard FAIL.
#
# Scope (#1255): we run the content proof specifically (npx jest
# write-readback-content) rather than the full `npm test`. The full Node suite
# pulls in corpus-dependent parity/smoke tests and a slow `--release` napi
# build; scoping to the content proof keeps the gate fast and reliable while
# guaranteeing the load-bearing #1231 test executes fail-closed. The content
# test self-generates its SSTables, so it needs no fixture corpus (hence
# node-bindings is NOT in DATASET_COMPONENTS); CQLITE_DATASETS_ROOT is still
# exported defensively for any test that reads it.
run_node_bindings() {
  local name=node-bindings
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no node/npm on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] npm ci + npm run build + jest write-readback-content (#1231)"
  if CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" bash -c '
      set -euo pipefail
      cd "'"$REPO_ROOT"'/bindings/node"
      if [ -f package-lock.json ]; then npm ci; else npm install; fi
      npm run build
      npx jest write-readback-content' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# delivery-telemetry: run the delivery-pipeline telemetry tool's unit tests
# (scripts/tests/test_delivery_telemetry.py) with the stdlib unittest runner.
# SKIP-aware like python-bindings: no python3 -> SKIP (loud, never silent PASS);
# any test failure -> hard FAIL. No third-party deps, no datasets, no network.
run_delivery_telemetry() {
  local name=delivery-telemetry
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] python3 scripts/tests/test_delivery_telemetry.py"
  if python3 "$REPO_ROOT/scripts/tests/test_delivery_telemetry.py" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# compaction-byte-parity: the PR-VISIBLE proxy for the nightly-only Java
# differential byte tier (issue #1405). The two manifest scenarios
# cass.compaction.SSTableRewriterTest.output_component_integrity and
# cass.compaction.harness_byte_tier_artifacts prove Cassandra-vs-CQLite
# byte identity only under `gradle byteParity` (compaction-parity.yml /
# nightly-docker-parity.yml), which fires nightly + on workflow_dispatch — never
# on a PR. A PR could break compaction byte parity and merge green.
#
# This component runs the Rust re-compaction byte-parity SUBSET as the local PR
# proxy: CQLite re-produces the same inputs, runs its own compaction, and diffs
# the output components (Data.db/Index.db/Summary.db/Digest.crc32/CRC.db)
# byte-for-byte against committed Cassandra 5.0.2 compacted references. It does
# NOT replace the nightly Java tier (which diffs the FULL component set over the
# whole scenario matrix from a live Cassandra build) — see
# docs/development/parity-ci-tiers.md for the PR-proxy vs nightly tier contract.
#
# Fixture policy (fail-closed where fixtures are committed, SKIP-aware otherwise):
#   * Group A (issue_1017/1020/1240): references are COMMITTED to git under
#     test_compactionparity/** + test_compactionparityudt/**, so they run under
#     CQLITE_REQUIRE_FIXTURES=1 — an absent/present-but-incomplete committed
#     golden is a hard FAIL, never a silent skip.
#   * Group B (issue_1019): its test_tomb references are fetched-only (not
#     committed), so it runs WITHOUT CQLITE_REQUIRE_FIXTURES — it enforces the
#     byte/header diff when the fixtures are present and cleanly self-skips when
#     they are not (e.g. a checkout that has not fetched test_tomb).
# The whole component SKIPs (loud, never silent PASS) when CQLITE_DATASETS_ROOT
# is unset or the committed test_compactionparity keyspace is absent (a minimal
# checkout). NOT in DATASET_COMPONENTS: it is self-guarding, so it must not trip
# the hard dataset preflight.
run_compaction_byte_parity() {
  local name=compaction-byte-parity
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  local committed_ks="${CQLITE_DATASETS_ROOT:-}/sstables/test_compactionparity"
  if [ -z "${CQLITE_DATASETS_ROOT:-}" ] || [ ! -d "$committed_ks" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (CQLITE_DATASETS_ROOT unset or committed test_compactionparity fixtures absent)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] Rust byte-parity PR proxy for the nightly Java byte tier (#1405)"
  if CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" bash -c '
      set -euo pipefail
      # Group A — committed references, fail-closed (CQLITE_REQUIRE_FIXTURES=1).
      env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT="'"$CQLITE_DATASETS_ROOT"'" \
        cargo test -p cqlite-core --features write-support \
          --test issue_1017_live_cell_compaction_byte_parity \
          --test issue_1020_udt_frozen_compaction_byte_parity \
          --test issue_1240_nested_frozen_collection_udt_parity
      # Group B — fetched-only test_tomb references, skip-aware (no require-fixtures).
      env CQLITE_DATASETS_ROOT="'"$CQLITE_DATASETS_ROOT"'" \
        cargo test -p cqlite-core --features write-support \
          --test issue_1019_static_dropped_collection_compaction_parity' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# parity-report: verify the committed derived parity report is not stale vs its
# source manifest (issue #1338). Renders test-data/cassandra-parity-manifest.yml
# with `cassandra-parity report --check`; PASS when the committed report matches a
# fresh render, FAIL (naming docs/reports/cassandra-test-parity.md) when it drifts.
# This catches the single-PR "edited the manifest, forgot to regenerate" case at
# the local gate, before push — the layer the post-merge self-healing job cannot
# cover. SKIP-aware like delivery-telemetry/python-bindings: when the
# cassandra-parity crate (tools/cassandra-parity) or the manifest is absent (a
# minimal checkout), it records SKIP (loud, never silent PASS) rather than FAIL.
# The manifest source and the tool-crate dir resolve to their repo defaults but are
# overridable (PARITY_REPORT_MANIFEST / PARITY_REPORT_TOOL_DIR) so the component is
# self-testable without mutating the committed tree; the --output target is always
# the canonical committed report (read-only under --check) so a failure always
# names that file. No Docker, no datasets — reads the manifest + committed report.
run_parity_report() {
  local name=parity-report
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local manifest="${PARITY_REPORT_MANIFEST:-test-data/cassandra-parity-manifest.yml}"
  local tool_dir="${PARITY_REPORT_TOOL_DIR:-tools/cassandra-parity}"
  local report="docs/reports/cassandra-test-parity.md"
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if [ ! -f "$manifest" ] || [ ! -d "$tool_dir" ]; then
    status=SKIP
    echo ">>> [$name] SKIP (cassandra-parity tool or manifest unavailable: manifest=$manifest tool=$tool_dir)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] cargo run -q -p cassandra-parity -- report --check ($report)"
  if cargo run -q -p cassandra-parity -- report \
       --manifest "$manifest" --output "$report" --check >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    # A nonzero --check exit is either a genuine render mismatch (the tool prints
    # "report: STALE — ...") or an invalid manifest (lint errors bail before any
    # render). Only the former is fixed by regenerating; mirror the CI heal job's
    # distinction so the advice is not misleading. grep on the captured $log is
    # injection/quoting-safe (fixed pattern, no interpolation).
    if grep -q 'STALE' "$log"; then
      echo "--- [$name] FAILED: $report is STALE vs the manifest."
      echo "    Regenerate: cargo run -p cassandra-parity -- report --manifest $manifest --output $report"
    else
      echo "--- [$name] FAILED: cannot render $report — the manifest is invalid."
      echo "    Fix the manifest lint/validity error before regenerating: $manifest"
    fi
    echo "--- last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# tooling-tests: fast shell-tooling regression tests that have no Rust target and
# no dataset/network needs. Currently scripts/tests/test_agent_gate_summary.sh,
# which verifies the SUMMARY block survives non-foreground capture (#1175), and
# scripts/tests/test_agent_gate_smoke_target_dir.sh, which verifies the smoke step
# resolves the CLI via CARGO_TARGET_DIR (#1247). These two never run the real gate
# components, so wiring them here cannot cause the gate to recurse. Also runs
# scripts/tests/test_agent_gate_parity_report.sh (#1338), which drives nested
# `agent-gate.sh --only parity-report` invocations to assert the SKIP/PASS/FAIL
# outcomes; that nesting is BOUNDED (--only parity-report never selects
# tooling-tests, so it cannot recurse) and the cassandra-parity build is already
# warm from the earlier parity-report component, so it stays cheap. Also runs
# scripts/tests/test_bootstrap_agent_machine.sh (#1921), which proves the
# new-machine bootstrap's pure-check paths never install anything (it runs with
# --skip-smoke, so it never invokes the real gate — no recursion). Also runs
# scripts/tests/test_agent_gate_delta.sh (#1892), which drives the hidden
# --delta-classify hook + --delta entry guards + --delta-...-emit-summary-selftest
# to assert the test/docs-only fail-closed re-cert policy and DISTINCT delta
# markers (hermetic — classification/emission only, never runs cargo). SKIP-aware:
# the summary test's truncation case relies on a python3 reader, so with no
# python3 we record SKIP (loud, never silent PASS); any test failure -> hard FAIL.
run_tooling_tests() {
  local name=tooling-tests
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  : >"$log"

  # generator keyspace-scoping guard (#1232): no python3 needed, always runs. A
  # failure here FAILs the component, mirroring the summary selftest semantics.
  echo ">>> [$name] bash scripts/tests/test_generator_keyspace_scoping.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_generator_keyspace_scoping.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (keyspace-scoping guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # parity-report component self-test (#1338): no python3 needed, always runs. A
  # failure FAILs the component, mirroring the keyspace-scoping guard semantics.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_parity_report.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_parity_report.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (parity-report self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # --delta re-cert self-test (#1892): no python3 needed, always runs (hermetic —
  # classification + entry guards + delta summary emission, no cargo). A failure
  # FAILs the component, mirroring the parity-report/keyspace-scoping guards.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_delta.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_delta.sh" >>"$log" 2>&1; then
    status=FAIL
    echo "--- [$name] FAILED (delta re-cert self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    record_result "$name" "$status" "$((end - start))"
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH; selftest truncation reader needs it)"
    record_result "$name" "$status" 0
    return 0
  fi
  echo ">>> [$name] bash scripts/tests/test_agent_gate_summary.sh; bash scripts/tests/test_agent_gate_smoke_target_dir.sh; bash scripts/tests/test_gate_concurrency_cap.sh; bash scripts/tests/test_bootstrap_agent_machine.sh"
  if bash "$REPO_ROOT/scripts/tests/test_agent_gate_summary.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_agent_gate_smoke_target_dir.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_gate_concurrency_cap.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_bootstrap_agent_machine.sh" >>"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# file-size: the campsite-rule ratchet (epic #1116 / #1135). Two parts:
#   advisory  - list every changed .rs file currently over threshold, as a prompt
#               to split it as part of this work.
#   ratchet   - FAIL if a change makes an over-threshold file LARGER (or pushes a
#               file over). You may edit big files freely; you just cannot grow
#               them without either splitting or acknowledging via the override.
# Metric is TOTAL line count (inline tests included) on purpose: the cost being
# controlled is tokens-to-load when an agent reads the file before editing it.
# Degrades to advisory-only (no ratchet) when the base ref can't be resolved.
SRC_LIMIT=800
TEST_LIMIT=1500
run_file_size() {
  local name=file-size
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local start end status=PASS
  start=$(date +%s)

  # Base ref: an explicit override (issue #1892 --delta uses the anchor commit),
  # else merge-base with the default branch. If none resolves, we can still do the
  # advisory list but not the growth comparison.
  local base="" ref
  if [ -n "${GATE_BASE_OVERRIDE:-}" ]; then
    base="$GATE_BASE_OVERRIDE"
  else
    for ref in origin/main main origin/master master; do
      if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
        base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] && break
      fi
    done
  fi

  # Changed, non-deleted .rs files vs base (committed + working tree). With no
  # base, fall back to changes vs HEAD (uncommitted only).
  local files
  if [ -n "$base" ]; then
    files=$(git diff --name-only --diff-filter=d "$base" -- '*.rs' 2>/dev/null)
  else
    files=$(git diff --name-only --diff-filter=d HEAD -- '*.rs' 2>/dev/null)
  fi

  local -a over=() grew=()
  local f cur lim base_n
  while IFS= read -r f; do
    [ -n "$f" ] && [ -f "$f" ] || continue
    cur=$(wc -l <"$f" | tr -d ' ')
    case "$f" in
      *_test.rs|*_tests.rs|*/tests/*|tests/*|*/benches/*) lim=$TEST_LIMIT ;;
      *) lim=$SRC_LIMIT ;;
    esac
    [ "$cur" -gt "$lim" ] || continue
    over+=("$(printf '%5s/%-4s  %s' "$cur" "$lim" "$f")")
    [ -n "$base" ] || continue
    base_n=$(git show "$base:$f" 2>/dev/null | wc -l | tr -d ' ')
    base_n=${base_n:-0}
    if [ "$cur" -gt "$base_n" ]; then
      grew+=("$(printf '%s: %s -> %s (limit %s)' "$f" "$base_n" "$cur" "$lim")")
    fi
  done <<<"$files"

  echo ">>> [$name] thresholds: src=$SRC_LIMIT test=$TEST_LIMIT (total lines, inline tests included)"
  if [ "${#over[@]}" -eq 0 ]; then
    echo ">>> [$name] no changed .rs files over threshold"
  else
    echo "--- [$name] changed files over threshold (campsite rule — split per epic #1116 / #1135):"
    printf '      %s\n' "${over[@]}"
  fi

  if [ -z "$base" ]; then
    echo ">>> [$name] base ref unavailable — growth ratchet skipped (advisory only)"
  elif [ "${#grew[@]}" -gt 0 ]; then
    if [ "${CQLITE_ALLOW_FILE_GROWTH:-0}" = 1 ]; then
      echo ">>> [$name] ${#grew[@]} over-threshold file(s) grew; ALLOWED via CQLITE_ALLOW_FILE_GROWTH=1:"
      printf '      %s\n' "${grew[@]}"
    else
      status=FAIL
      echo "--- [$name] FAIL: change makes over-threshold file(s) larger."
      echo "    Split per the campsite rule (epic #1116 source / #1135 tests), or, if a split"
      echo "    is genuinely out of scope, re-run with CQLITE_ALLOW_FILE_GROWTH=1 to acknowledge:"
      printf '      %s\n' "${grew[@]}"
    fi
  fi

  end=$(date +%s)
  record_result "$name" "$status" "$((end - start))"
  echo ">>> [$name] $status ($((end - start))s)"
}

# scoped-tests (issue #1821, --lite only): the blast-radius-scoped test component.
# Map each changed path to its cargo package and run ONLY those packages' --lib
# tests plus the diff's new/changed `--test` targets — NOT the full
# core-tests/write/cli/bindings/parity set. Falls back to `cqlite-core --lib` and
# says so when no rust workspace package is in the diff (docs/scripts/bindings-only
# changes). Package detection uses the SAME base-ref resolution as file-size.
#
# Python exception (issue #1893): cqlite-py is a pyo3 cdylib whose
# `cargo test -p cqlite-py` can never link libpython, so a bindings/python diff is
# routed to the fast python tier (maturin develop --profile dev + the not-slow
# pytest tier) instead of the always-failing cargo run. Node (cqlite-node) and
# rust-only diffs are unaffected; a mixed diff runs BOTH the rust-scoped targets
# AND the python tier. See PYTHON_LITE_TIER_CMD / classify_scoped_plan above.
run_scoped_tests() {
  local name=scoped-tests
  local log="$LOG_DIR/$name.log"
  local start end status=PASS
  start=$(date +%s)
  : >"$log"

  local base="" ref
  if [ -n "${GATE_BASE_OVERRIDE:-}" ]; then
    base="$GATE_BASE_OVERRIDE"
  else
    for ref in origin/main main origin/master master; do
      if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
        base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] && break
      fi
    done
  fi

  local changed
  if [ -n "$base" ]; then
    changed=$(printf '%s\n%s\n' \
      "$(git diff --name-only "$base"...HEAD 2>/dev/null)" \
      "$(git diff --name-only HEAD 2>/dev/null)")
  else
    changed=$(git diff --name-only HEAD 2>/dev/null)
  fi

  # Package ownership and per-`--test` scoping REQUIRE an authoritative
  # Cargo-metadata parser (jq or python3). Without one we cannot map a path to its
  # owning workspace member NOR learn a target's required-features (running a
  # feature-gated target feature-less would FAIL --lite spuriously). So when
  # NEITHER is present we scope to `cqlite-core --lib` ONLY and say so — we emit no
  # per-package/per-target selection and reintroduce NO hardcoded path mapping. The
  # AGENT_GATE_TEST_NO_METADATA_PARSER hook forces this branch for the self-test.
  local have_meta_parser=1
  if [ "${AGENT_GATE_TEST_NO_METADATA_PARSER:-0}" = 1 ] || \
     { ! command -v jq >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; }; then
    have_meta_parser=0
    echo ">>> [$name] no jq/python3 — scoping to cqlite-core --lib only; run the full gate for integration-test coverage"
  fi

  # No-parser fallback (issue #1821 roborev): without a metadata parser we do NOT
  # consult pkg_has_lib (it can't know without metadata and would degrade to a
  # compile-only --no-run). Run an explicit, unconditional cqlite-core --lib test
  # that ACTUALLY RUNS the core lib tests, then finish this component and return —
  # the metadata-derived per-package selection below is skipped entirely.
  if [ "$have_meta_parser" -eq 0 ]; then
    local -a args=()
    read -r -a args <<<"$(_scoped_test_cmd_noparser)"
    echo ">>> [$name] cargo ${args[*]}"
    if ! cargo "${args[@]}" >>"$log" 2>&1; then
      status=FAIL
      OVERALL=FAIL
    fi
    if [ "$status" = FAIL ]; then
      echo "--- [$name] FAILED; last 60 lines of $log ---"
      tail -60 "$log"
      echo "--- end of $name output ---"
    fi
    end=$(date +%s)
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
    echo ">>> [$name] $status ($((end - start))s)"
    return
  fi

  # Metadata-derived per-TARGET selection (issue #1821): `pkgindex` is
  # "<manifest_dir>\t<pkg>\t<has_lib>" for every member; `newtests` is every
  # changed --test target ("<pkg>|<testname>|<features>"). Both empty in the
  # no-parser fallback. These drive WHICH --test targets/features run within each
  # routed package — the routing itself (which packages, python tier or not) comes
  # from classify_scoped_plan below.
  local pkgindex="" newtests=""
  if [ "$have_meta_parser" -eq 1 ]; then
    pkgindex=$(_package_index)
    newtests=$(printf '%s\n' "$changed" | classify_test_targets)
  fi

  # has_lib lookup for ANY package name, straight from the metadata index (1 when
  # the package has a lib/rlib target `cargo test --lib` can run, else 0).
  pkg_has_lib() {
    printf '%s\n' "$pkgindex" \
      | awk -F'\t' -v p="$1" '$2 == p { print $3; f = 1; exit } END { if (!f) print 0 }'
  }

  # ROUTING — single source of truth (issue #1893, roborev job 1450): the executor
  # consumes classify_scoped_plan's output — the SAME function the hidden
  # `--classify-scoped-plan` hook exposes and the py-route self-tests assert — so
  # the routing logic (package-set union, cqlite-py exclusion, python-tier flag)
  # exists exactly ONCE. An executor-only edit that re-routed a python diff back to
  # `cargo test -p cqlite-py` is now impossible without also changing the asserted
  # plan. Plan lines: "rust-pkg: <pkg>" and "python-tier: <cmd>".
  local plan line
  plan=$(printf '%s\n' "$changed" | classify_scoped_plan)
  local -a pkgs=()
  local python_diff=0
  while IFS= read -r line; do
    case "$line" in
      "rust-pkg: "*) pkgs+=("${line#rust-pkg: }") ;;
      "python-tier: "*) python_diff=1 ;;
    esac
  done <<<"$plan"
  local scoped_note=""
  [ "${#pkgs[@]}" -gt 0 ] && scoped_note="${pkgs[*]}"
  if [ "$python_diff" -eq 1 ]; then
    scoped_note="${scoped_note:+$scoped_note + }python tier ($PYTHON_LITE_TIER_CMD)"
  fi
  # Fall back to the cqlite-core --lib default ONLY when the diff selected nothing
  # at all — NOT when a python-only diff already routed to the python tier.
  if [ "${#pkgs[@]}" -eq 0 ] && [ "$python_diff" -eq 0 ]; then
    pkgs=(cqlite-core)
    scoped_note="cqlite-core --lib (default; no rust workspace package in the diff)"
  fi
  echo ">>> [$name] blast-radius packages: $scoped_note"

  # Union a comma-list of features into a newline-set (Bash 3.2-safe dedup).
  # The separator is placed BETWEEN elements (not trailing): `add_features` is
  # called via `featset=$(add_features ...)`, and command substitution strips
  # trailing newlines, so a trailing-newline scheme would glue the first element
  # of the next call onto the last existing element (e.g. "write-support" +
  # "delta-export" -> "write-supportdelta-export"). Prepending "$set"+newline
  # only when non-empty keeps every element on its own line regardless.
  add_features() {
    local set=$1 list=$2 x oldifs=$IFS nl
    nl=$'\n'
    IFS=,
    for x in $list; do
      [ -n "$x" ] || continue
      printf '%s\n' "$set" | grep -qxF "$x" || set="${set:+${set}${nl}}${x}"
    done
    IFS=$oldifs
    printf '%s' "$set"
  }

  # Bash 3.2 under `set -u` treats "${pkgs[@]}" of an EMPTY array as unbound, and a
  # python-only diff now legitimately leaves pkgs empty (python tier covers it), so
  # expand with the ${arr[@]+"${arr[@]}"} guard rather than unconditionally.
  local p rest tname feats
  for p in ${pkgs[@]+"${pkgs[@]}"}; do
    local -a args=(test -p "$p")
    local featset=""
    # cqlite-core lib tests need cli-helpers (matches the full gate's core-tests).
    [ "$p" = cqlite-core ] && featset=$(add_features "$featset" cli-helpers)
    # Lib presence comes from Cargo metadata (no src/lib.rs probing). A package
    # with no lib target runs only its changed --test targets (issue #1821).
    local haslib
    haslib=$(pkg_has_lib "$p")
    [ "$haslib" -eq 1 ] && args+=(--lib)
    local -a stems=()
    # Collect every changed --test target this package owns AND union each
    # target's required-features so it is compiled with the features it needs —
    # never invoked feature-less (issue #1821 finding 2).
    while IFS= read -r key; do
      [ -n "$key" ] || continue
      case "$key" in
        "$p|"*)
          rest=${key#*|}          # "<name>|<features>"
          tname=${rest%%|*}
          feats=${rest#*|}        # "" when the target has no required-features
          [ "$feats" = "$rest" ] && feats=""
          stems+=(--test "$tname")
          featset=$(add_features "$featset" "$feats")
          ;;
      esac
    done <<<"$newtests"
    # Bash 3.2 under `set -u` treats "${stems[@]}" of an EMPTY array as unbound,
    # so only expand it when non-empty (count expansion is always safe).
    [ "${#stems[@]}" -gt 0 ] && args+=("${stems[@]}")
    # Pass the unioned required-features (if any) so feature-gated targets
    # (write-support / delta-export / duckdb-tests / ...) actually compile.
    local featjoin
    featjoin=$(printf '%s' "$featset" | awk 'NF{ printf (n++?",":"") $0 }')
    [ -n "$featjoin" ] && args+=(--features "$featjoin")
    # A test-only crate with no changed --test target has nothing runnable to
    # scope to; compile-check it (--no-run) rather than run its whole (slow) suite.
    if [ "$haslib" -eq 0 ] && [ "${#stems[@]}" -eq 0 ]; then
      args+=(--no-run)
    fi
    echo ">>> [$name] cargo ${args[*]}"
    if ! cargo "${args[@]}" >>"$log" 2>&1; then
      status=FAIL
      OVERALL=FAIL
    fi
  done

  # Python tier (issue #1893): the REAL python signal --lite runs for a
  # bindings/python diff instead of the always-libpython-link-failing
  # `cargo test -p cqlite-py`. Reuses the full gate's persistent venv
  # (target/agent-gate-venv). Both phases `eval` the PYTHON_LITE_*_CMD component
  # constants that also compose the advertised PYTHON_LITE_TIER_CMD plan string
  # (roborev job 1449) — plan/executor drift is structurally impossible.
  #
  # SKIP vs FAIL split (roborev job 1449, Low): TOOLCHAIN failures (venv creation,
  # pip install — e.g. offline, or the maturin build environment itself missing) get
  # a loud SKIP-note, never FAIL — --lite must stay usable offline, and a toolchain
  # gap is not a code failure (clippy in this same lite run still compiles cqlite-py,
  # and the full gate's python-bindings component hard-fails). A PYTEST failure is a
  # real code failure and FAILs. NOTE: a python-diff --lite round costs a maturin
  # compile of the extension (seconds warm via the persistent venv + sccache,
  # ~1-3 min cold).
  if [ "$python_diff" -eq 1 ]; then
    if ! command -v python3 >/dev/null 2>&1; then
      echo ">>> [$name] python binding diff but no python3 on PATH — SKIP python tier (run the full gate)"
      PYTHON_TIER_NOTE="python-tier: SKIPPED (no python3 on PATH) — python-binding diff NOT validated by this lite run; run the full gate"
    else
      local venv="$REPO_ROOT/target/agent-gate-venv"
      echo ">>> [$name] python tier: $PYTHON_LITE_TIER_CMD (venv: $venv)"
      if ! RUN_SLOW_TESTS=0 PY_MATURIN_CMD="$PYTHON_LITE_MATURIN_CMD" bash -c '
          set -euo pipefail
          venv="'"$venv"'"
          [ -x "$venv/bin/python" ] || python3 -m venv "$venv"
          . "$venv/bin/activate"
          pip install --quiet --upgrade pip >/dev/null
          pip install --quiet maturin pytest
          eval "$PY_MATURIN_CMD"' >>"$log" 2>&1; then
        echo ">>> [$name] python tier SKIP (venv/pip/maturin toolchain setup failed — offline or toolchain gap, NOT a code failure; see $log; run the full gate when the toolchain is available)"
        PYTHON_TIER_NOTE="python-tier: SKIPPED (toolchain: venv/pip/maturin setup failed — offline?) — python-binding diff NOT validated by this lite run; run the full gate"
      elif RUN_SLOW_TESTS=0 PY_PYTEST_CMD="$PYTHON_LITE_PYTEST_CMD" bash -c '
          set -euo pipefail
          . "'"$venv"'/bin/activate"
          eval "$PY_PYTEST_CMD"' >>"$log" 2>&1; then
        echo ">>> [$name] python tier PASS"
        PYTHON_TIER_NOTE="python-tier: PASS ($PYTHON_LITE_TIER_CMD)"
      else
        status=FAIL
        OVERALL=FAIL
        echo ">>> [$name] python tier FAIL (pytest failure — a real code failure)"
        PYTHON_TIER_NOTE="python-tier: FAIL (pytest failure — a real code failure)"
      fi
    fi
  fi

  if [ "$status" = FAIL ]; then
    echo "--- [$name] FAILED; last 60 lines of $log ---"
    tail -60 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  echo ">>> [$name] $status ($((end - start))s)"
}

# run_lite (issue #1821): the FAST ITERATION gate. Runs file-size + fmt +
# FULL-workspace clippy + blast-radius-scoped tests, emits a DISTINCTLY-labeled
# LITE summary, and EXITS — it never falls through to the full-gate flow below, so
# the no-flag path is completely unchanged. It is NOT the gate of record.
run_lite() {
  echo
  echo "==================================================================="
  echo "  AGENT-GATE --lite  :  FAST ITERATION GATE — *NOT* THE GATE OF RECORD"
  echo "  Runs: file-size + fmt + scoped workspace clippy + blast-radius-scoped tests."
  echo "  It SKIPS core-tests, write/cli, bindings, parity, smoke, etc."
  echo "  Before merge you MUST run the full  scripts/agent-gate.sh  and it must"
  echo "  PASS — its ==== AGENT-GATE SUMMARY ==== block is the ONLY run that counts."
  echo "==================================================================="
  echo

  run_file_size
  run_component fmt cargo fmt --all --check
  run_component clippy run_clippy
  run_scoped_tests

  declare -a SUMMARY_META=()
  SUMMARY_META+=("commit: $(git rev-parse --short HEAD) branch: $(git rev-parse --abbrev-ref HEAD) dirty: $(test -n "$(git status --porcelain)" && echo yes || echo no)")
  SUMMARY_META+=("lite-scope: file-size fmt clippy scoped-tests (full gate NOT run — run it once before merge)")
  # Python-tier verdict marker (roborev job 1450): when a python-binding diff was
  # in scope, the block carries the tier's verdict — a SKIPPED marker makes a
  # "green but validated nothing" block detectable from the block alone.
  [ -n "$PYTHON_TIER_NOTE" ] && SUMMARY_META+=("$PYTHON_TIER_NOTE")
  SUMMARY_META+=("$(accelerators_line)")
  local i
  for i in "${!NAMES[@]}"; do
    SUMMARY_META+=("$(printf '%-18s %s (%s)' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}")")
  done
  emit_summary "$OVERALL" "${SUMMARY_META[@]}"

  if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
    echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2
    exit 1
  fi
  case "$OVERALL" in
    PASS) exit 0 ;;
    *) exit 1 ;;
  esac
}

# run_delta <anchor> (issue #1892): TEST/DOCS-ONLY RE-CERTIFICATION. Verifies the
# diff anchor..HEAD (committed + working tree) touches ONLY test/docs files —
# FAIL-CLOSED, naming any offending production file — then re-certifies with
# file-size + fmt + the diff's changed test targets, emits a DISTINCTLY-labeled
# DELTA summary, and EXITS (never falls through to the full-gate flow). It is NOT
# the gate of record: the gate of record remains the full agent-gate.sh PASS at
# the anchor, with the nightly gate.yml deep-check as the standing backstop.
run_delta() {
  local anchor="$1"
  echo
  echo "==================================================================="
  echo "  AGENT-GATE --delta  :  TEST/DOCS-ONLY RE-CERTIFICATION — *NOT* THE GATE OF RECORD"
  echo "  Anchor (full-gate PASS commit): $anchor"
  echo "  Verifies the diff anchor..HEAD touches ONLY test/docs files, then runs:"
  echo "  file-size + fmt + the changed test targets. It SKIPS clippy, core-tests,"
  echo "  write/cli, bindings, parity, smoke, etc. — those were validated by the"
  echo "  full gate at the anchor, and the nightly gate.yml deep-check re-runs the"
  echo "  FULL gate on main. Record BOTH the anchor's full SUMMARY and this DELTA"
  echo "  block in the PR."
  echo "==================================================================="
  echo

  # Resolve the anchor to a full commit sha. A bad/unknown anchor is a usage error
  # (RESULT: ERROR) — we cannot re-certify a diff against a commit that does not
  # resolve.
  local anchor_sha
  if ! anchor_sha=$(git rev-parse --verify -q "${anchor}^{commit}" 2>/dev/null) || [ -z "$anchor_sha" ]; then
    echo "--- [delta] ERROR: anchor '$anchor' does not resolve to a commit." >&2
    echo "    Pass the commit the full gate PASSed at (a sha, tag, or ref)." >&2
    emit_summary ERROR \
      "delta-anchor: $anchor (UNRESOLVED)" \
      "$(accelerators_line)" \
      "error: anchor does not resolve to a commit — cannot re-certify"
    exit 2
  fi

  # Anchor full-gate run-id: from --anchor-run-id, else read from the anchor
  # summary file if given. The anchor summary file MUST be a FULL-gate PASS block:
  # a lite/delta block cannot anchor a delta re-cert (that would let a fast run
  # masquerade as the gate of record). Refuse loudly if it is not.
  local anchor_run_id="${DELTA_ANCHOR_RUN_ID:-}"
  if [ -z "$anchor_run_id" ] && [ -n "$DELTA_ANCHOR_SUMMARY_FILE" ]; then
    if [ ! -f "$DELTA_ANCHOR_SUMMARY_FILE" ]; then
      echo "--- [delta] ERROR: --anchor-summary-file '$DELTA_ANCHOR_SUMMARY_FILE' not found." >&2
      emit_summary ERROR \
        "delta-anchor: $anchor_sha" \
        "$(accelerators_line)" \
        "error: --anchor-summary-file not found: $DELTA_ANCHOR_SUMMARY_FILE"
      exit 2
    fi
    if ! grep -qF "==== AGENT-GATE SUMMARY ====" "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null \
       || grep -qF "==== AGENT-GATE LITE SUMMARY ====" "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null \
       || grep -qF "==== AGENT-GATE DELTA SUMMARY ====" "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null; then
      echo "--- [delta] ERROR: --anchor-summary-file is not a FULL-gate SUMMARY block." >&2
      echo "    A delta re-cert must anchor to a full agent-gate.sh PASS, not a lite/delta run." >&2
      emit_summary ERROR \
        "delta-anchor: $anchor_sha" \
        "$(accelerators_line)" \
        "error: anchor summary is not a full-gate SUMMARY block (lite/delta cannot anchor a delta)"
      exit 2
    fi
    if ! grep -qE '^RESULT: PASS' "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null; then
      echo "--- [delta] ERROR: --anchor-summary-file did not record RESULT: PASS." >&2
      echo "    A delta re-cert must anchor to a full-gate PASS." >&2
      emit_summary ERROR \
        "delta-anchor: $anchor_sha" \
        "$(accelerators_line)" \
        "error: anchor summary RESULT is not PASS — cannot anchor a delta re-cert"
      exit 2
    fi
    anchor_run_id=$(grep -E '^run-id:' "$DELTA_ANCHOR_SUMMARY_FILE" 2>/dev/null | head -1 | sed 's/^run-id:[[:space:]]*//')
  fi
  [ -n "$anchor_run_id" ] || anchor_run_id="(not provided)"

  # Changed files anchor..HEAD (committed) plus the working tree. --diff-filter=d
  # drops deletions (a deleted path cannot be classified against the tree and is
  # never a production-file regression). Dedup, drop blanks.
  local changed
  changed=$(printf '%s\n%s\n' \
    "$(git diff --name-only --diff-filter=d "$anchor_sha" HEAD 2>/dev/null)" \
    "$(git diff --name-only --diff-filter=d HEAD 2>/dev/null)" \
    | awk 'NF && !seen[$0]++')

  # Partition into allowed (test/docs) and offending (everything else). FAIL-CLOSED.
  local f allowed="" offending=""
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if _delta_is_allowed_path "$f"; then
      allowed="${allowed}${f}"$'\n'
    else
      offending="${offending}${f}"$'\n'
    fi
  done <<<"$changed"

  local n_allowed n_offending
  n_allowed=$(printf '%s' "$allowed" | awk 'NF' | wc -l | tr -d ' ')
  n_offending=$(printf '%s' "$offending" | awk 'NF' | wc -l | tr -d ' ')

  # Build the delta-files meta lines (indented list), or a placeholder when empty.
  local -a file_meta=()
  if [ "$n_allowed" -eq 0 ] && [ "$n_offending" -eq 0 ]; then
    file_meta+=("delta-files (0): (no changes anchor..HEAD)")
  else
    file_meta+=("delta-files ($n_allowed allowed / $n_offending offending):")
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      file_meta+=("      [test/docs] $f")
    done <<<"$allowed"
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      file_meta+=("      [PRODUCTION] $f")
    done <<<"$offending"
  fi

  local -a anchor_meta=(
    "commit: $(git rev-parse --short HEAD) branch: $(git rev-parse --abbrev-ref HEAD) dirty: $(test -n "$(git status --porcelain)" && echo yes || echo no)"
    "delta-anchor: $anchor_sha (full-gate PASS commit)"
    "delta-anchor-run-id: $anchor_run_id"
    "gate-of-record: full agent-gate.sh run at $anchor_sha (this DELTA re-certifies a test/docs-only diff; it is NOT a substitute for the full gate)"
    "nightly-backstop: .github/workflows/gate.yml deep-check re-runs the FULL gate on main (CQLITE_CLIPPY_FULL=1)"
  )

  # FAIL-CLOSED: any production file in the diff refuses the delta re-cert. Name the
  # offending files and tell the caller to run the full gate.
  if [ "$n_offending" -gt 0 ]; then
    echo "--- [delta] REFUSED: the diff anchor..HEAD changes production (non-test/docs) files:" >&2
    printf '      %s\n' $(printf '%s\n' "$offending" | awk 'NF') >&2
    echo "    A production change requires a fresh FULL gate: scripts/agent-gate.sh" >&2
    emit_summary REFUSED \
      "${anchor_meta[@]}" \
      "delta-scope: file-size fmt scoped-tests (NOT RUN — refused before execution)" \
      "$(accelerators_line)" \
      "${file_meta[@]}" \
      "refusal: $n_offending production file(s) changed — a full gate is required (test/docs-only diffs qualify for --delta)"
    [ "$SUMMARY_WRITE_FAILED" -eq 0 ] || { echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2; exit 1; }
    exit 1
  fi

  echo ">>> [delta] diff anchor..HEAD is test/docs-only ($n_allowed file(s)); re-certifying"

  # Re-certify: file-size + fmt + the changed test targets, all scoped to the
  # anchor..HEAD diff (GATE_BASE_OVERRIDE points file-size + scoped-tests at the
  # anchor). run_file_size and run_component write result files; run_scoped_tests
  # appends to NAMES and sets OVERALL on failure.
  GATE_BASE_OVERRIDE="$anchor_sha"
  run_file_size
  run_component fmt cargo fmt --all --check
  run_scoped_tests

  # Reconstruct file-size + fmt verdicts from their result files (so a fmt or
  # file-size FAIL fails the delta and shows in the block), then append the
  # scoped-tests entry run_scoped_tests already pushed onto NAMES.
  local -a DN=() DS=() DT=()
  local c rf st secs
  for c in file-size fmt; do
    rf="$LOG_DIR/$c.result"
    if [ -f "$rf" ]; then
      st=""; secs=""
      read -r st secs < "$rf" || true
      [ -n "$st" ] || { st=FAIL; secs=0; }
      DN+=("$c"); DS+=("$st"); DT+=("${secs}s")
      [ "$st" = FAIL ] && OVERALL=FAIL
    else
      DN+=("$c"); DS+=(FAIL); DT+=("0s"); OVERALL=FAIL
    fi
  done
  # Append the scoped-tests entry run_scoped_tests pushed onto NAMES. Guard the
  # KEYS expansion with a count check: the `"${!arr[@]+"${!arr[@]}"}"` empty-array
  # idiom that works for VALUES does NOT work for the keys form `${!arr[@]}` — bash
  # reads `${!NAMES[@]+...}` as INDIRECT expansion and errors ("invalid variable
  # name") on the array's string contents, aborting run_delta before emit_summary.
  # `${#NAMES[@]}` is set -u-safe even when empty.
  local i
  if [ "${#NAMES[@]}" -gt 0 ]; then
    for i in "${!NAMES[@]}"; do
      DN+=("${NAMES[$i]}"); DS+=("${STATUSES[$i]}"); DT+=("${TIMES[$i]}")
    done
  fi

  declare -a SUMMARY_META=()
  SUMMARY_META+=("${anchor_meta[@]}")
  SUMMARY_META+=("delta-scope: file-size fmt scoped-tests (test/docs-only re-cert; clippy/core/write/cli/bindings/parity/smoke NOT run — see gate-of-record)")
  SUMMARY_META+=("$(accelerators_line)")
  SUMMARY_META+=("${file_meta[@]}")
  for i in "${!DN[@]}"; do
    SUMMARY_META+=("$(printf '%-18s %s (%s)' "${DN[$i]}:" "${DS[$i]}" "${DT[$i]}")")
  done
  emit_summary "$OVERALL" "${SUMMARY_META[@]}"

  if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
    echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2
    exit 1
  fi
  case "$OVERALL" in
    PASS) exit 0 ;;
    *) exit 1 ;;
  esac
}

# ---- Machine-wide full-gate concurrency cap (issue #1825) -------------------
# A cross-process bounded semaphore around the FULL gate of record ONLY. At most N
# full `agent-gate.sh` runs execute machine-wide at once; excess invocations BLOCK
# (queue) for a slot — they NEVER fail from the cap. EXEMPT (never queued):
#   * --lite runs (issue #1821): cheap fmt+clippy+scoped tests, must stay instant.
#   * --only PARTIAL runs: they don't count as the gate AND are used by nested
#     tooling self-tests (a capped parent runs `agent-gate.sh --only ...` as a
#     child), so capping them could self-deadlock the queue.
#   * --emit-summary-selftest / hidden hooks: they exit earlier, never reaching here.
#
# Mechanism (SIGKILL-safe by construction): N slot lockfiles under a SHARED,
# machine-wide (NOT per-checkout) dir, each guarded by a non-blocking fcntl.flock.
# A tiny background daemon (scripts/lib/gate_slot_daemon.py) acquires ONE slot,
# signals us via a ready file, then HOLDS the lock while polling this gate's
# liveness; it releases the slot when the gate exits. Crucially the daemon is a
# SEPARATE process that opens the lock fd AFTER it is forked, so the gate's heavy
# children (cargo, nextest, ...) never inherit the lock -- a SIGKILL of the gate
# frees the slot within one poll interval even while orphaned children run on. (An
# fd held by the gate shell itself would be inherited by cargo and keep the slot
# locked after a SIGKILL, defeating stale-slot reaping -- hence the daemon.)
#
# N default: max(2, floor((ncpu-2)/4)) -- a conservative fraction of cores that
# still lets a couple of gates run on a small box; overridable via
# CQLITE_GATE_MAX_CONCURRENCY. Slots dir: $CQLITE_GATE_SLOTS_DIR (default
# ${TMPDIR:-/tmp}/cqlite-gate-slots). Poll interval: $CQLITE_GATE_POLL_SECS
# (default 2). The cap is skipped (with a loud stderr note) when python3 or the
# daemon is unavailable, and can be force-disabled with CQLITE_GATE_DISABLE_CAP=1.
# Non-interactive callers block cleanly (waiting on the daemon), never spin-fail.

# Resolve N from the default formula + the CQLITE_GATE_MAX_CONCURRENCY override.
_gate_max_concurrency() {
  local dflt=$(( ( _ncpu - 2 ) / 4 ))
  [ "$dflt" -lt 2 ] && dflt=2
  local v="${CQLITE_GATE_MAX_CONCURRENCY:-$dflt}"
  case "$v" in *[!0-9]*|'') v=$dflt ;; esac
  [ "$v" -lt 1 ] && v=1
  printf '%s' "$v"
}

# PID of the background slot daemon (empty when the cap is inactive for this run).
GATE_SLOT_DAEMON_PID=""

# Release the held slot by terminating the daemon (which closes its lock fd). Run
# from the EXIT trap. Guarded to fire ONLY in the main gate shell: a backgrounded
# `( ... ) &` pool subshell also runs the inherited EXIT trap on its own exit, and
# must NOT tear down the parent's slot. BASHPID (bash 4+) differs from $$ inside a
# subshell; on bash 3.2 (no BASHPID, no pool subshells) it defaults equal, so the
# guard is a no-op there and only the real gate exit releases the slot.
# shellcheck disable=SC2329  # invoked indirectly via `trap '_gate_release_slot' EXIT`
_gate_release_slot() {
  [ "${BASHPID:-$$}" = "$$" ] || return 0
  [ -n "${GATE_SLOT_DAEMON_PID:-}" ] || return 0
  kill "$GATE_SLOT_DAEMON_PID" 2>/dev/null || true
  GATE_SLOT_DAEMON_PID=""
}

# Block until this full-gate run holds one of N machine-wide slots, then return so
# the gate proceeds while the daemon keeps the slot held in the background. No-op
# for the exempt run classes above. Fail-open (cap disabled) if python3/daemon are
# missing or the daemon dies before acquiring -- the gate must never be un-runnable
# because of the cap.
acquire_gate_slot() {
  [ "$LITE" -eq 1 ] && return 0
  [ "$DELTA" -eq 1 ] && return 0
  [ -n "$ONLY" ] && return 0
  [ "${CQLITE_GATE_DISABLE_CAP:-0}" = 1 ] && return 0
  if ! command -v python3 >/dev/null 2>&1; then
    echo "agent-gate: python3 unavailable -- full-gate concurrency cap DISABLED (#1825)" >&2
    return 0
  fi
  local n dir poll daemon ready
  n=$(_gate_max_concurrency)
  dir="${CQLITE_GATE_SLOTS_DIR:-${TMPDIR:-/tmp}/cqlite-gate-slots}"
  poll="${CQLITE_GATE_POLL_SECS:-2}"
  daemon="$REPO_ROOT/scripts/lib/gate_slot_daemon.py"
  if [ ! -f "$daemon" ]; then
    echo "agent-gate: slot daemon $daemon missing -- concurrency cap DISABLED (#1825)" >&2
    return 0
  fi
  if ! mkdir -p "$dir" 2>/dev/null; then
    echo "agent-gate: cannot create slot dir $dir -- concurrency cap DISABLED (#1825)" >&2
    return 0
  fi
  ready="$LOG_DIR/gate-slot.ready"
  rm -f "$ready" 2>/dev/null || true
  # Start the background lock-holder for THIS gate (pid $$). It writes $ready once
  # it owns a slot and holds it until this gate exits. Its std fds are detached to
  # /dev/null so this long-lived background child can NEVER hold the gate's stdout
  # pipe open and truncate a streamed SUMMARY under an until-EOF reader (#1175).
  python3 "$daemon" --slots-dir "$dir" --slots "$n" --gate-pid "$$" \
    --ready-file "$ready" --poll-secs "$poll" </dev/null >/dev/null 2>&1 &
  GATE_SLOT_DAEMON_PID=$!
  trap '_gate_release_slot' EXIT
  # Block until the daemon signals acquisition, printing the queued notice ONCE
  # after a short grace (so an immediately-free slot stays quiet). If the daemon
  # dies before acquiring, fail open rather than hang the gate forever.
  local printed=0 waited=0
  while [ ! -f "$ready" ]; do
    if ! kill -0 "$GATE_SLOT_DAEMON_PID" 2>/dev/null; then
      echo "agent-gate: slot daemon exited before acquiring -- cap DISABLED for this run (#1825)" >&2
      GATE_SLOT_DAEMON_PID=""
      return 0
    fi
    if [ "$printed" -eq 0 ] && [ "$waited" -ge 3 ]; then
      echo "waiting for gate slot ($n in use)…" >&2
      printed=1
    fi
    waited=$(( waited + 1 ))
    sleep 0.2
  done
  [ "$printed" -eq 1 ] && echo "agent-gate: gate slot acquired -- proceeding (#1825)" >&2
}

# Test-only stub (issue #1825 concurrency self-test): when CQLITE_GATE_STUB_RUNDIR
# is set, the gate acquires a real slot (subject to the cap + exemptions above),
# advertises "I am working" by dropping a per-PID marker file, sleeps
# CQLITE_GATE_STUB_SLEEP seconds, then exits 0 WITHOUT running any real component.
# This lets scripts/tests/test_gate_concurrency_cap.sh exercise the machine-wide
# semaphore (queueing at N, --lite exemption, SIGKILL slot release) hermetically,
# without running actual gate work. Never triggered in normal use.
if [ -n "${CQLITE_GATE_STUB_RUNDIR:-}" ]; then
  acquire_gate_slot   # self-exempts for --lite / --only
  mkdir -p "$CQLITE_GATE_STUB_RUNDIR" 2>/dev/null || true
  _stub_marker="$CQLITE_GATE_STUB_RUNDIR/holding.$$"
  : > "$_stub_marker" 2>/dev/null || true
  sleep "${CQLITE_GATE_STUB_SLEEP:-2}"
  rm -f "$_stub_marker" 2>/dev/null || true
  exit 0
fi

# --lite (issue #1821): run the fast subset and EXIT before the full-gate flow.
# Kept fully separate from the full-gate execution below so the no-flag path is
# byte-for-byte unchanged. --lite is EXEMPT from the #1825 cap (never queued).
if [ "$LITE" -eq 1 ]; then
  run_lite
fi

# --delta (issue #1892): test/docs-only re-certification. Verifies anchor..HEAD is
# test/docs-only (fail-closed), runs file-size + fmt + changed test targets, and
# EXITS before the full-gate flow. EXEMPT from the #1825 cap (never queued).
if [ "$DELTA" -eq 1 ]; then
  run_delta "$DELTA_ANCHOR"
fi

# Machine-wide full-gate concurrency cap (issue #1825): block here until a slot is
# free, so at most N full gates run at once across worktrees + the root checkout.
# --lite already returned above; --only PARTIAL runs self-exempt inside.
acquire_gate_slot

# file-size runs first and needs no dataset, so it executes before the dataset
# preflight (which exits early when data is missing).
run_file_size

# Components that actually read SSTable datasets (Data.db) at run time. These are
# the only ones the dataset preflight must guard. Wrongly skipping the preflight
# for a dataset-dependent component is the #646 hazard, so this set must stay
# complete.
#   needs datasets: core-tests, tombstones-scan, scan-offload-guard,
#     memory-budget (dhat lane reads real Data.db and fails closed on empty),
#     integration-tests, write-tests, smoke (read Data.db / golden fixtures), and
#     python-bindings — the pytest suite resolves CQLITE_DATASETS_ROOT and calls
#     skip_if_no_datasets() (bindings/python/tests/conftest.py), so with data
#     absent its dataset-backed coverage *silently skips* and the suite can still
#     report PASS. python-bindings is therefore in this set (#1175 finding 2): the
#     preflight must FAIL loudly rather than let a skipped suite pass green — the
#     same #646 failure mode that motivated guarding the Rust dataset suites.
#   dataset-free (deliberately NOT guarded): fmt, clippy, file-size (operate on
#     source text), cli-tests (only the unit_tests.rs target: tempfile-based
#     config/parsing/output tests, no CQLITE_DATASETS_ROOT, no Data.db),
#     parity-report (renders the manifest + diffs the committed report; reads no
#     CQLITE_DATASETS_ROOT, no Data.db — issue #1338),
#     delivery-telemetry + tooling-tests (pure shell/stdlib tool tests; the lone
#     CQLITE_DATASETS_ROOT in test_agent_gate_summary.sh *sets an empty* root to
#     exercise the preflight, it consumes no real data), minimal-build (a cargo
#     build, no tests run), and format-compat. format-compat is excluded (#1175
#     finding 1): its sole target (cargo test -p format-compatibility-tests,
#     tests/format-compatibility) is pure in-memory byte-level format-compliance
#     assertions with hardcoded vectors — it reads no CQLITE_DATASETS_ROOT and no
#     Data.db — so guarding it just made `--only format-compat` falsely fail the
#     preflight when datasets are absent.
DATASET_COMPONENTS="core-tests tombstones-scan scan-offload-guard memory-budget integration-tests write-tests python-bindings smoke"

# selected_needs_datasets: true iff at least one SELECTED component reads datasets.
# With no --only, every component runs, so it's always true. With --only, it's true
# only when the selection intersects DATASET_COMPONENTS — so e.g. `--only
# tooling-tests` or `--only fmt` skips the (dataset-requiring) preflight entirely.
selected_needs_datasets() {
  [ -z "$ONLY" ] && return 0
  local sel comp
  for sel in ${ONLY//,/ }; do
    for comp in $DATASET_COMPONENTS; do
      [ "$sel" = "$comp" ] && return 0
    done
  done
  return 1
}

# Dataset preflight: dataset-dependent components must FAIL loudly when data is
# missing, never silently pass on a skipped suite (the #646 failure mode). Run it
# only when the selected component set actually needs datasets (#1175 finding 2),
# so dataset-free selections like `--only tooling-tests` are not blocked by it.
#
# The find/wc over the dataset mount is computed INSIDE this branch (#1175
# finding 2): a dataset-free selection must not traverse $CQLITE_DATASETS_ROOT at
# all (it can be slow or hang on an unavailable mount). When the preflight is
# skipped, DATA_COUNT stays the placeholder below and feeds the summary directly.
DATA_COUNT="(preflight skipped — no dataset-dependent component selected)"
if selected_needs_datasets; then
  DATA_COUNT=$(find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" 2>/dev/null | wc -l | tr -d ' ')
  if [ "$DATA_COUNT" -eq 0 ]; then
    echo "agent-gate: no Data.db files under $CQLITE_DATASETS_ROOT/sstables" >&2
    echo "agent-gate: fetch them first: bash test-data/scripts/fetch-datasets.sh" >&2
    # Overwrite the caller-known recovery file with a FAIL block stamped with this
    # run's run-id (#1175 finding 2). The startup sentinel already guarantees no
    # stale PASS survives; this makes the early exit explicit for a caller reading
    # the recovery path.
    emit_summary FAIL \
      "preflight: FAIL (no Data.db files under $CQLITE_DATASETS_ROOT/sstables)" \
      "hint: bash test-data/scripts/fetch-datasets.sh"
    exit 1
  fi
else
  echo ">>> dataset preflight: skipped (no selected component needs datasets: --only $ONLY)"
fi

# CI dataset pins, for the CI-parity check (issue #719): local validation must
# target the same asset CI uses.
PIN_FILE=".github/workflows/sstabledump-parity-gate.yml"
PINS=$(grep -E 'DATASET_(TAG|ASSET|SHA256):' "$PIN_FILE" 2>/dev/null | sed 's/^ *//' | tr '\n' ' ' || echo "unavailable")

# ---- issue #1737: nextest core-tests + bounded parallel component pool ----
# core-tests: the 67%-of-wall-clock execution floor. Under nextest it parallelizes
# across test binaries + cores; a separate `cargo test --doc` pass preserves
# doctest coverage nextest does not run. Falls back to plain `cargo test`.
run_core_tests() {
  # Always exclude the legacy blob-fallback test (needs the non-default feature).
  # Under the static-golden mandate (CQLITE_SKIP_DOCKER_TESTS != 0, the gate
  # default) ALSO exclude the live Docker parity tests by name substring; setting
  # CQLITE_SKIP_DOCKER_TESTS=0 restores them. nextest excludes by filter DSL, the
  # cargo fallback by libtest --skip (both keep the doctest pass so no coverage is
  # dropped).
  local nx_filter='not test(test_legacy_format_allows_blob_fallback_with_feature)'
  local -a skip_args=(--skip test_legacy_format_allows_blob_fallback_with_feature)
  if [ "${CQLITE_SKIP_DOCKER_TESTS:-1}" != 0 ]; then
    nx_filter="$nx_filter and not test(under_cassandra5_sstabledump)"
    skip_args+=(--skip under_cassandra5_sstabledump)
  fi
  if [ "$NEXTEST" -eq 1 ]; then
    run_component core-tests bash -c '
      cargo nextest run --package cqlite-core --features cli-helpers -E "$1" &&
      cargo test --doc --package cqlite-core --features cli-helpers -- "${@:2}"' \
      cqlite-agent-gate "$nx_filter" "${skip_args[@]}"
  else
    run_component core-tests cargo test --package cqlite-core --features cli-helpers -- "${skip_args[@]}"
  fi
}

# _pool_selected <name>: honor the --only filter when building the launch list.
_pool_selected() {
  [ -z "$ONLY" ] && return 0
  grep -qw "$1" <<<"${ONLY//,/ }"
}

# dispatch_component <name>: run exactly one gate component with the SAME command,
# package, and feature selection as the historical sequential gate. Each branch
# records its verdict to $LOG_DIR/<name>.result (see record_result), so it is safe
# to run in a backgrounded subshell.
dispatch_component() {
  case "$1" in
    fmt) run_component fmt cargo fmt --all --check ;;
    clippy) run_component clippy run_clippy ;;
    core-tests) run_core_tests ;;
    tombstones-scan) run_component tombstones-scan cargo test --package cqlite-core \
      --features write-support,cli-helpers,tombstones \
      --test issue_1085_tombstones_full_scan_parity ;;
    scan-offload-guard) run_component scan-offload-guard cargo test --package cqlite-core \
      --features cli-helpers,scan-offload-probe \
      --test issue_1143_scan_offload_thread \
      --test issue_1333_scan_scratch_reuse \
      --test issue_1589_window_drain_bytes ;;
    memory-budget) run_component memory-budget cargo test --package cqlite-core \
      --features cli-helpers,dhat-heap \
      --test memory_budget -- --test-threads=1 ;;
    integration-tests) run_component integration-tests bash -c '
  cargo test --package cqlite-integration-tests --no-run &&
  cargo test --package cqlite-integration-tests \
    --test chunked_data_reader_direct_test \
    --test comprehensive_component_integration_tests \
    --test fixture_specific_integration_tests \
    --test golden_path_get_operations_tests \
    --test golden_path_partition_lookup_tests \
    --test golden_path_scan_operations_tests \
    --test golden_path_summary_index_integration_tests' ;;
    format-compat) run_component format-compat cargo test --package format-compatibility-tests ;;
    write-tests) run_component write-tests bash -c '
  cargo test --package cqlite-core --features write-support --lib &&
  cargo test --package cqlite-core --features write-support --test write_read_roundtrip &&
  cargo test --package cqlite-core --features write-support --test compaction_integration' ;;
    cli-tests) run_component cli-tests bash -c '
  cargo test --package cqlite-cli --test unit_tests &&
  cargo test --package cqlite-cli --features write-support --test write_readback_content_tests &&
  cargo test --package cqlite-cli --features write-support --test graceful_shutdown_tests' ;;
    compaction-byte-parity) run_compaction_byte_parity ;;
    python-bindings) run_python_bindings ;;
    node-bindings) run_node_bindings ;;
    delivery-telemetry) run_delivery_telemetry ;;
    parity-report) run_parity_report ;;
    binding-unwind-profile) run_component binding-unwind-profile bash "$REPO_ROOT/scripts/tests/test_binding_unwind_profile.sh" ;;
    tooling-tests) run_tooling_tests ;;
    minimal-build) run_component minimal-build cargo build --package cqlite-core --no-default-features --features all-compression ;;
    smoke) run_component smoke bash -c '
  cargo build --package cqlite-cli --bin cqlite &&
  CQLITE_CLI="${CARGO_TARGET_DIR:-$PWD/target}/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh' ;;
    *) echo "dispatch_component: unknown component $1" >&2; return 2 ;;
  esac
}

# is_side_component / run_side_component: python-bindings and node-bindings are the
# biggest non-core costs and, being separate crates built with binding-specific
# features, would repeatedly invalidate + rebuild cqlite-core in the SHARED target
# dir if run concurrently with the main cargo lane (measured: python-bindings
# ballooned 72s -> 576s under a naive shared-target pool). So they run in a SIDE
# lane with their OWN CARGO_TARGET_DIR, which removes the cross-lane cargo
# feature-thrash and build-lock contention (sccache still dedups the actual
# compiles across target dirs). Nothing else spawns from these dirs.
is_side_component() {
  case "$1" in python-bindings|node-bindings) return 0 ;; *) return 1 ;; esac
}
run_side_component() {
  local base="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
  CARGO_TARGET_DIR="$base/agent-gate-side/$1" dispatch_component "$1"
}

# Track which components were selected and which lane (fail-closed check after lanes drain).
declare -a SELECTED_MAIN=() SELECTED_SIDE=()
SIDE_LANE_EXIT=0

# launch_components: two-lane bounded model (issue #1737). The MAIN lane runs every
# selected non-side cargo component SERIALLY in canonical order (identical build
# profile to the historical sequential gate -- no NEW cross-component thrash), with
# nextest cutting the core-tests long pole. The SIDE lane runs the isolated-target
# binding components concurrently with MAIN. Concurrent heavy processes are bounded
# by AGENT_GATE_JOBS: MAIN takes one slot, the SIDE lane runs up to
# (AGENT_GATE_JOBS - 1) of its components at once (this per-gate cap composes with
# the machine-wide bound of #1825). AGENT_GATE_JOBS=1 (or bash < 4.3) collapses to
# the historical strictly-sequential run. file-size already ran inline before the
# dataset preflight and is skipped here.
launch_components() {
  local -a main_lane=() side_lane=()
  local c
  for c in "${COMPONENTS[@]}"; do
    [ "$c" = file-size ] && continue
    _pool_selected "$c" || continue
    if is_side_component "$c"; then side_lane+=("$c"); SELECTED_SIDE+=("$c")
    else main_lane+=("$c"); SELECTED_MAIN+=("$c"); fi
  done

  # Bash 3.2 under `set -u` treats "${arr[@]}" of an EMPTY array as unbound (fixed
  # in bash 4.4+; #1841 latent bug surfaced by the #1825 concurrency-cap self-test,
  # which runs a nested `--only <one-component>` gate -- exactly the case where
  # main_lane or side_lane is empty). Guard every such expansion below with the
  # `"${arr[@]+"${arr[@]}"}"` idiom, which is a no-op when non-empty and expands to
  # nothing (never unbound) when empty. Same idiom already used for `stems` above.
  if [ "$AGENT_GATE_JOBS" -le 1 ] || [ "${#side_lane[@]}" -eq 0 ]; then
    for c in "${main_lane[@]+"${main_lane[@]}"}"; do dispatch_component "$c"; done
    for c in "${side_lane[@]+"${side_lane[@]}"}"; do run_side_component "$c"; done
    return
  fi

  local side_jobs=$(( AGENT_GATE_JOBS - 1 )); [ "$side_jobs" -lt 1 ] && side_jobs=1
  echo ">>> [pool] MAIN lane (serial, shared target): ${main_lane[*]}"
  echo ">>> [pool] SIDE lane (isolated target, up to $side_jobs concurrent): ${side_lane[*]}"
  # SIDE lane: a background sub-pool capped at side_jobs (each isolated target dir).
  (
    srun=0
    for sc in "${side_lane[@]+"${side_lane[@]}"}"; do
      run_side_component "$sc" &
      srun=$(( srun + 1 ))
      if [ "$srun" -ge "$side_jobs" ]; then wait -n 2>/dev/null || true; srun=$(( srun - 1 )); fi
    done
    wait
  ) &
  local side_pid=$!
  # MAIN lane: serial, foreground (shared target dir, no intra-lane parallelism).
  for c in "${main_lane[@]+"${main_lane[@]}"}"; do dispatch_component "$c"; done
  wait "$side_pid" || SIDE_LANE_EXIT=$?
}

launch_components

# Fail-closed check (issue #1737 roborev): verify all SELECTED components produced result files.
# A component that was selected but has no .result file crashed/exited before record_result,
# which is a fail-OPEN hole. Treat missing results as synthetic FAIL + force overall FAIL.
# Also check the SIDE lane's exit status. Bash-3.2-safe empty-array guard (#1841,
# same hazard as launch_components above): a `--only <main-only-component>` run
# leaves SELECTED_SIDE empty, and vice versa.
for _sc in "${SELECTED_SIDE[@]+"${SELECTED_SIDE[@]}"}"; do
  [ -f "$LOG_DIR/$_sc.result" ] || {
    echo "agent-gate: SIDE-lane component '$_sc' SELECTED but has no result file (crashed/exited early)" >&2
    NAMES+=("$_sc"); STATUSES+=(FAIL); TIMES+=("0s")
    OVERALL=FAIL
  }
done
for _mc in "${SELECTED_MAIN[@]+"${SELECTED_MAIN[@]}"}"; do
  [ -f "$LOG_DIR/$_mc.result" ] || {
    echo "agent-gate: MAIN-lane component '$_mc' SELECTED but has no result file (crashed/exited early)" >&2
    NAMES+=("$_mc"); STATUSES+=(FAIL); TIMES+=("0s")
    OVERALL=FAIL
  }
done
if [ "$SIDE_LANE_EXIT" -ne 0 ]; then
  echo "agent-gate: SIDE lane exited with status $SIDE_LANE_EXIT (subshell failure)" >&2
  OVERALL=FAIL
fi

# Reconstruct the summary arrays from per-component result files (issue #1737):
# the bounded pool ran components in backgrounded subshells that cannot write the
# parent's arrays, so each wrote its verdict to $LOG_DIR/<name>.result. Read them
# back in canonical COMPONENTS order for a deterministic SUMMARY regardless of the
# order components finished; a missing file means the component was not selected.
for _c in "${COMPONENTS[@]}"; do
  _rf="$LOG_DIR/$_c.result"
  [ -f "$_rf" ] || continue
  _st=""; _secs=""
  read -r _st _secs < "$_rf" || true
  NAMES+=("$_c"); STATUSES+=("$_st"); TIMES+=("${_secs}s")
  [ "$_st" = FAIL ] && OVERALL=FAIL
done

declare -a SUMMARY_META=()
SUMMARY_META+=("commit: $(git rev-parse --short HEAD) branch: $(git rev-parse --abbrev-ref HEAD) dirty: $(test -n "$(git status --porcelain)" && echo yes || echo no)")
if selected_needs_datasets; then
  SUMMARY_META+=("datasets: $DATA_COUNT Data.db files under $CQLITE_DATASETS_ROOT")
else
  SUMMARY_META+=("datasets: $DATA_COUNT")
fi
SUMMARY_META+=("ci-pins: $PINS")
SUMMARY_META+=("$(accelerators_line)")
if [ -n "$ONLY" ]; then
  SUMMARY_META+=("mode: PARTIAL (--only $ONLY) - does NOT count as the gate")
  [ "$OVERALL" = "PASS" ] && OVERALL=PARTIAL
fi
for i in "${!NAMES[@]}"; do
  SUMMARY_META+=("$(printf '%-18s %s (%s)' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}")")
done
emit_summary "$OVERALL" "${SUMMARY_META[@]}"

# If we could not produce the authoritative recovery artifact, never report
# green (#1175 finding 1): the correctness verdict above is still printed, but a
# missing summary file forces a non-zero exit so the failure cannot pass silently.
if [ "$SUMMARY_WRITE_FAILED" -ne 0 ]; then
  echo "agent-gate: exiting non-zero because the summary file could not be written (#1175)" >&2
  exit 1
fi

# Exit 0 only for a full-gate PASS; PARTIAL runs exit 3 so they can never be
# scripted into a green gate claim.
case "$OVERALL" in
  PASS) exit 0 ;;
  PARTIAL) exit 3 ;;
  *) exit 1 ;;
esac
