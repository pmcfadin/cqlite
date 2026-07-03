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
#   clippy             RUSTFLAGS="-D warnings" clippy --workspace --all-targets --all-features
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
#                      hard-FAILs this component.
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
#                                     # ONLY file-size + fmt + FULL-workspace clippy
#                                     # (-D warnings) + BLAST-RADIUS-SCOPED tests
#                                     # (the touched package's --lib + the diff's
#                                     # new --test targets; NOT core-tests/write/
#                                     # cli/bindings/parity/smoke). ~1-5 min vs
#                                     # 12-25 min. It is NOT the gate of record and
#                                     # emits a DISTINCT "==== AGENT-GATE LITE
#                                     # SUMMARY ====" block (MODE: lite) so it can
#                                     # never be pasted as the full SUMMARY. The
#                                     # full gate MUST PASS once before merge. Its
#                                     # recovery default is .agent-gate-lite-summary.txt.
#   scripts/agent-gate.sh --list      # list full-gate components without running
#   scripts/agent-gate.sh --lite-list # list the --lite components without running
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
export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO_ROOT/test-data/datasets}"

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

COMPONENTS=(file-size fmt clippy core-tests tombstones-scan scan-offload-guard memory-budget integration-tests format-compat write-tests cli-tests compaction-byte-parity python-bindings node-bindings delivery-telemetry parity-report binding-unwind-profile tooling-tests minimal-build smoke)
# --lite (issue #1821) runs ONLY this fast subset: file-size ratchet, fmt,
# FULL-workspace clippy (cross-crate API breaks are the cheap-insurance class),
# and blast-radius-scoped tests (the touched package's --lib + the diff's new
# test targets), NOT the full core-tests/write/cli/bindings/parity set. It is the
# FAST ITERATION loop, NOT the gate of record — the full gate must PASS once
# before merge. See run_lite() below.
LITE_COMPONENTS=(file-size fmt clippy scoped-tests)
ONLY=""
SELFTEST=0
LITE=0
case "${1:-}" in
  --list) printf '%s\n' "${COMPONENTS[@]}"; exit 0 ;;
  # --lite alone runs the fast gate; `--lite --emit-summary-selftest` drives the
  # LITE summary block through the real emission path (for tooling-tests) without
  # running any component.
  --lite) LITE=1; [ "${2:-}" = --emit-summary-selftest ] && SELFTEST=1 ;;
  --lite-list) printf '%s\n' "${LITE_COMPONENTS[@]}"; exit 0 ;;
  # Hidden self-test hook (issue #1821): map stdin paths -> "<pkg>|<testname>"
  # for actual Cargo test targets (nested helpers excluded). No side effects.
  --classify-test-targets) classify_test_targets; exit 0 ;;
  # Hidden self-test hook (issue #1821): map stdin paths -> "<pkg>|<has_lib>"
  # via metadata-derived longest-prefix package ownership. No side effects.
  --classify-package-owners) classify_package_owners; exit 0 ;;
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
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
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
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
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
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
    return 0
  fi
  echo ">>> [$name] python3 scripts/tests/test_delivery_telemetry.py"
  if python3 "$REPO_ROOT/scripts/tests/test_delivery_telemetry.py" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
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
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
    return 0
  fi
  echo ">>> [$name] cargo run -q -p cassandra-parity -- report --check ($report)"
  if cargo run -q -p cassandra-parity -- report \
       --manifest "$manifest" --output "$report" --check >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    OVERALL=FAIL
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
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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
# warm from the earlier parity-report component, so it stays cheap. SKIP-aware:
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
    OVERALL=FAIL
    echo "--- [$name] FAILED (keyspace-scoping guard); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  # parity-report component self-test (#1338): no python3 needed, always runs. A
  # failure FAILs the component, mirroring the keyspace-scoping guard semantics.
  echo ">>> [$name] bash scripts/tests/test_agent_gate_parity_report.sh"
  if ! bash "$REPO_ROOT/scripts/tests/test_agent_gate_parity_report.sh" >>"$log" 2>&1; then
    status=FAIL
    OVERALL=FAIL
    echo "--- [$name] FAILED (parity-report self-test); last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
    end=$(date +%s)
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
    echo ">>> [$name] $status ($((end - start))s)"
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH; selftest truncation reader needs it)"
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
    return 0
  fi
  echo ">>> [$name] bash scripts/tests/test_agent_gate_summary.sh; bash scripts/tests/test_agent_gate_smoke_target_dir.sh"
  if bash "$REPO_ROOT/scripts/tests/test_agent_gate_summary.sh" >>"$log" 2>&1 &&
     bash "$REPO_ROOT/scripts/tests/test_agent_gate_smoke_target_dir.sh" >>"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
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

  # Base ref: merge-base with the default branch. If none resolves, we can still
  # do the advisory list but not the growth comparison.
  local base="" ref
  for ref in origin/main main origin/master master; do
    if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
      base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] && break
    fi
  done

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
      OVERALL=FAIL
      echo "--- [$name] FAIL: change makes over-threshold file(s) larger."
      echo "    Split per the campsite rule (epic #1116 source / #1135 tests), or, if a split"
      echo "    is genuinely out of scope, re-run with CQLITE_ALLOW_FILE_GROWTH=1 to acknowledge:"
      printf '      %s\n' "${grew[@]}"
    fi
  fi

  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  echo ">>> [$name] $status ($((end - start))s)"
}

# scoped-tests (issue #1821, --lite only): the blast-radius-scoped test component.
# Map each changed path to its cargo package and run ONLY those packages' --lib
# tests plus the diff's new/changed `--test` targets — NOT the full
# core-tests/write/cli/bindings/parity set. Falls back to `cqlite-core --lib` and
# says so when no rust workspace package is in the diff (docs/scripts/bindings-only
# changes). Package detection uses the SAME base-ref resolution as file-size.
run_scoped_tests() {
  local name=scoped-tests
  local log="$LOG_DIR/$name.log"
  local start end status=PASS
  start=$(date +%s)
  : >"$log"

  local base="" ref
  for ref in origin/main main origin/master master; do
    if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
      base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] && break
    fi
  done

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

  # Metadata-derived package ownership (issue #1821): the single authoritative
  # source. `pkgindex` is "<manifest_dir>\t<pkg>\t<has_lib>" for every member;
  # `owners` is the longest-prefix owning package of each changed path
  # ("<pkg>|<has_lib>"); `newtests` is every changed --test target
  # ("<pkg>|<testname>|<features>"). All empty in the no-parser fallback. This
  # replaces the old hardcoded path-prefix `case` and `pkg_dir` maps, which kept
  # missing real members (tools/*, bindings/*, examples, ...); every workspace
  # member is now covered because ownership comes from `cargo metadata`.
  local pkgindex="" owners="" newtests=""
  if [ "$have_meta_parser" -eq 1 ]; then
    pkgindex=$(_package_index)
    owners=$(printf '%s\n' "$changed" | _owners_from_index "$pkgindex")
    newtests=$(printf '%s\n' "$changed" | classify_test_targets)
  fi

  # has_lib lookup for ANY package name, straight from the metadata index (1 when
  # the package has a lib/rlib target `cargo test --lib` can run, else 0).
  pkg_has_lib() {
    printf '%s\n' "$pkgindex" \
      | awk -F'\t' -v p="$1" '$2 == p { print $3; f = 1; exit } END { if (!f) print 0 }'
  }

  # Bash 3.2-safe newline-delimited package set (grep -qxF dedup). Built from BOTH
  # path owners AND the owners of every changed --test target — the latter covers
  # members with no path prefix of their own (notably the workspace-root `cqlite`
  # package, which owns the top-level tests/*.rs targets; issue #1821 finding 1).
  local pkgset="" pkg key tpkg
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

  local -a pkgs=()
  while IFS= read -r pkg; do [ -n "$pkg" ] && pkgs+=("$pkg"); done <<<"$pkgset"
  local scoped_note
  if [ "${#pkgs[@]}" -eq 0 ]; then
    pkgs=(cqlite-core)
    scoped_note="cqlite-core --lib (default; no rust workspace package in the diff)"
  else
    scoped_note="${pkgs[*]}"
  fi
  echo ">>> [$name] blast-radius packages: $scoped_note"

  # Union a comma-list of features into a newline-set (Bash 3.2-safe dedup).
  add_features() {
    local set=$1 list=$2 x oldifs=$IFS
    IFS=,
    for x in $list; do
      [ -n "$x" ] || continue
      printf '%s\n' "$set" | grep -qxF "$x" || set="${set}${x}"$'\n'
    done
    IFS=$oldifs
    printf '%s' "$set"
  }

  local p rest tname feats
  for p in "${pkgs[@]}"; do
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
  echo "  Runs: file-size + fmt + workspace clippy + blast-radius-scoped tests."
  echo "  It SKIPS core-tests, write/cli, bindings, parity, smoke, etc."
  echo "  Before merge you MUST run the full  scripts/agent-gate.sh  and it must"
  echo "  PASS — its ==== AGENT-GATE SUMMARY ==== block is the ONLY run that counts."
  echo "==================================================================="
  echo

  run_file_size
  run_component fmt cargo fmt --all --check
  run_component clippy env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
  run_scoped_tests

  declare -a SUMMARY_META=()
  SUMMARY_META+=("commit: $(git rev-parse --short HEAD) branch: $(git rev-parse --abbrev-ref HEAD) dirty: $(test -n "$(git status --porcelain)" && echo yes || echo no)")
  SUMMARY_META+=("lite-scope: file-size fmt clippy scoped-tests (full gate NOT run — run it once before merge)")
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

# --lite (issue #1821): run the fast subset and EXIT before the full-gate flow.
# Kept fully separate from the full-gate execution below so the no-flag path is
# byte-for-byte unchanged.
if [ "$LITE" -eq 1 ]; then
  run_lite
fi

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

run_component fmt cargo fmt --all --check
run_component clippy env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
run_component core-tests cargo test --package cqlite-core --features cli-helpers -- \
  --skip test_legacy_format_allows_blob_fallback_with_feature
# Issue #1085: the row-collapse bug lived in the `tombstones`-feature scan path,
# which the default gate run (cli-helpers) never compiles. Run the full-scan
# regression test under `tombstones` so a re-introduction can't ship green.
run_component tombstones-scan cargo test --package cqlite-core \
  --features write-support,cli-helpers,tombstones \
  --test issue_1085_tombstones_full_scan_parity
# Issue #1143: the thread-identity guard proves the windowed scan's
# decompress+parse runs OFF the async worker pool. Its probe is gated behind the
# non-default `scan-offload-probe` feature (so the instrumentation never ships in
# normal builds), and the test only compiles under that feature — so the default
# core-tests run never executes it. Run it here with the feature on; a guard that
# doesn't run in CI is not a guard.
run_component scan-offload-guard cargo test --package cqlite-core \
  --features cli-helpers,scan-offload-probe \
  --test issue_1143_scan_offload_thread \
  --test issue_1333_scan_scratch_reuse \
  --test issue_1589_window_drain_bytes
# Issue #1565 (Epic A/A4): dhat allocation/peak-heap regression net over the real
# read path. Compiled only under `dhat-heap` (installs the dhat global allocator
# in its own test binary), single-threaded because dhat::Profiler is a
# process-global singleton. Pins today's measured full-scan total-bytes and
# materializing peak-heap as Epic-E ratchet targets; dataset-dependent and fails
# closed on empty (asserts >=1 row before reading dhat stats).
run_component memory-budget cargo test --package cqlite-core \
  --features cli-helpers,dhat-heap \
  --test memory_budget -- --test-threads=1
# Compile EVERY target in the package first (--no-run, whole package) so a
# new/edited test file that doesn't compile can't hide behind the enumerated
# run-list (issue #865); then execute the seven CI-enforced targets.
run_component integration-tests bash -c '
  cargo test --package cqlite-integration-tests --no-run &&
  cargo test --package cqlite-integration-tests \
    --test chunked_data_reader_direct_test \
    --test comprehensive_component_integration_tests \
    --test fixture_specific_integration_tests \
    --test golden_path_get_operations_tests \
    --test golden_path_partition_lookup_tests \
    --test golden_path_scan_operations_tests \
    --test golden_path_summary_index_integration_tests'
# format-compatibility-tests is now a workspace member (issue #865) so fmt/clippy
# reach it; run its 'oa' format compliance tests here too.
run_component format-compat cargo test --package format-compatibility-tests
run_component write-tests bash -c '
  cargo test --package cqlite-core --features write-support --lib &&
  cargo test --package cqlite-core --features write-support --test write_read_roundtrip &&
  cargo test --package cqlite-core --features write-support --test compaction_integration'
run_component cli-tests bash -c '
  cargo test --package cqlite-cli --test unit_tests &&
  cargo test --package cqlite-cli --features write-support --test write_readback_content_tests'
run_compaction_byte_parity
run_python_bindings
run_node_bindings
run_delivery_telemetry
run_parity_report
# binding-unwind-profile (#1440): fail-closed guard that the shipped Python wheel
# and Node prebuild build definitions select `--profile release-unwind` (so the
# PyO3/napi catch_unwind firewall is active) and never `--release` (abort). Pure
# bash/grep/awk — offline, deterministic, no datasets; a hard FAIL on any
# abort-built or missing/unparseable definition.
run_component binding-unwind-profile bash "$REPO_ROOT/scripts/tests/test_binding_unwind_profile.sh"
run_tooling_tests
run_component minimal-build cargo build --package cqlite-core --no-default-features --features all-compression
# Pin smoke to a binary built from THIS tree. Left to its own devices the
# smoke script prefers any existing target/release/cqlite, however stale —
# the first full gate run caught a May binary failing all test_oa tables
# that current code reads fine.
# Resolve the just-built CLI honoring CARGO_TARGET_DIR (issue #1247): when the
# gate runs from a git worktree sharing a target dir via CARGO_TARGET_DIR, the
# binary lands in "$CARGO_TARGET_DIR/debug", not "$PWD/target/debug". Fall back
# to "$PWD/target" when CARGO_TARGET_DIR is unset.
run_component smoke bash -c '
  cargo build --package cqlite-cli --bin cqlite &&
  CQLITE_CLI="${CARGO_TARGET_DIR:-$PWD/target}/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh'

declare -a SUMMARY_META=()
SUMMARY_META+=("commit: $(git rev-parse --short HEAD) branch: $(git rev-parse --abbrev-ref HEAD) dirty: $(test -n "$(git status --porcelain)" && echo yes || echo no)")
if selected_needs_datasets; then
  SUMMARY_META+=("datasets: $DATA_COUNT Data.db files under $CQLITE_DATASETS_ROOT")
else
  SUMMARY_META+=("datasets: $DATA_COUNT")
fi
SUMMARY_META+=("ci-pins: $PINS")
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
