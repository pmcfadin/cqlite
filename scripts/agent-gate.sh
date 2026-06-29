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
#                      the default core-tests run can't execute it — issue #1143)
#   integration-tests  cargo test -p cqlite-integration-tests: compile ALL targets
#                      (--no-run, whole package) then run the seven CI-enforced ones
#   format-compat      cargo test -p format-compatibility-tests (the 'oa' format crate;
#                      issue #865 folded it into the workspace so fmt/clippy reach it)
#   write-tests        cargo test -p cqlite-core --features write-support (lib + roundtrip + compaction)
#   cli-tests          cargo test -p cqlite-cli --test unit_tests
#   python-bindings    maturin develop + pytest bindings/python/tests in a throwaway
#                      venv; SKIPs (never silently PASSes) if python3 is unavailable.
#                      Set RUN_SLOW_TESTS=1 to also run the CLI-parity suite.
#   tooling-tests      shell-tooling regression tests (fast, no datasets/network):
#                      scripts/tests/test_agent_gate_summary.sh — proves the
#                      SUMMARY block survives non-foreground capture (#1175). It
#                      only drives `agent-gate.sh --emit-summary-selftest`, which
#                      exits before running any component, so there is no recursion.
#                      SKIP-aware: no python3 -> SKIP (the selftest's truncation
#                      assertion needs a python reader), never silent PASS.
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
#   scripts/agent-gate.sh --list      # list components without running
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

COMPONENTS=(file-size fmt clippy core-tests tombstones-scan scan-offload-guard integration-tests format-compat write-tests cli-tests python-bindings delivery-telemetry tooling-tests minimal-build smoke)
ONLY=""
SELFTEST=0
case "${1:-}" in
  --list) printf '%s\n' "${COMPONENTS[@]}"; exit 0 ;;
  --only) ONLY="${2:?--only needs a comma-separated component list}" ;;
  --emit-summary-selftest) SELFTEST=1 ;;
  "") ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

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
SUMMARY_FILE="${AGENT_GATE_SUMMARY_FILE:-$REPO_ROOT/.agent-gate-summary.txt}"
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
  echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: $RUN_ID"
  echo "RESULT: INCOMPLETE (gate did not finish)"
  echo "==== END AGENT-GATE SUMMARY ===="
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
      echo "==== AGENT-GATE SUMMARY ===="
      echo "run-id: $RUN_ID"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE"
      echo "RESULT: $result"
      echo "==== END AGENT-GATE SUMMARY ===="
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
  elif ! grep -q '==== END AGENT-GATE SUMMARY ====' "$SUMMARY_FILE" 2>/dev/null; then
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
      echo "==== AGENT-GATE SUMMARY ===="
      echo "run-id: $RUN_ID"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE (WRITE FAILED — see stderr)"
      echo "RESULT: $emit_result"
      echo "==== END AGENT-GATE SUMMARY ===="
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
      echo "==== AGENT-GATE SUMMARY ===="
      echo "run-id: $RUN_ID"
      local line
      for line in "$@"; do echo "$line"; done
      echo "logs: $LOG_DIR"
      echo "summary-file: $SUMMARY_FILE (WRITE FAILED — see stderr)"
      echo "RESULT: $emit_result"
      echo "==== END AGENT-GATE SUMMARY ===="
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

# tooling-tests: fast shell-tooling regression tests that have no Rust target and
# no dataset/network needs. Currently scripts/tests/test_agent_gate_summary.sh,
# which verifies the SUMMARY block survives non-foreground capture (#1175). That
# test only drives `agent-gate.sh --emit-summary-selftest` (which exits before any
# component runs), so wiring it here cannot cause the gate to recurse. SKIP-aware:
# the test's truncation case relies on a python3 reader, so with no python3 we
# record SKIP (loud, never silent PASS); any test failure -> hard FAIL.
run_tooling_tests() {
  local name=tooling-tests
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH; selftest truncation reader needs it)"
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
    return 0
  fi
  echo ">>> [$name] bash scripts/tests/test_agent_gate_summary.sh"
  if bash "$REPO_ROOT/scripts/tests/test_agent_gate_summary.sh" >"$log" 2>&1; then
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

# file-size runs first and needs no dataset, so it executes before the dataset
# preflight (which exits early when data is missing).
run_file_size

# Components that actually read SSTable datasets (Data.db) at run time. These are
# the only ones the dataset preflight must guard. Wrongly skipping the preflight
# for a dataset-dependent component is the #646 hazard, so this set must stay
# complete. Dataset-free components (fmt, clippy, cli-tests, python-bindings,
# delivery-telemetry, tooling-tests, minimal-build, file-size, format-compat)
# need no preflight. format-compat is excluded (#1175 finding 1): its sole test
# target (cargo test -p format-compatibility-tests, tests/format-compatibility)
# is pure in-memory byte-level format-compliance assertions with hardcoded
# vectors — it reads no CQLITE_DATASETS_ROOT and no Data.db — so guarding it just
# made `--only format-compat` falsely fail the preflight when datasets are absent.
DATASET_COMPONENTS="core-tests tombstones-scan scan-offload-guard integration-tests write-tests smoke"

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
  --test issue_1143_scan_offload_thread
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
run_component cli-tests cargo test --package cqlite-cli --test unit_tests
run_python_bindings
run_delivery_telemetry
run_tooling_tests
run_component minimal-build cargo build --package cqlite-core --no-default-features --features all-compression
# Pin smoke to a binary built from THIS tree. Left to its own devices the
# smoke script prefers any existing target/release/cqlite, however stale —
# the first full gate run caught a May binary failing all test_oa tables
# that current code reads fine.
run_component smoke bash -c '
  cargo build --package cqlite-cli --bin cqlite &&
  CQLITE_CLI="$PWD/target/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh'

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
