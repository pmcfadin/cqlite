#!/usr/bin/env bash
#
# nightly-docker-parity.sh — the single aggregating runner for the
# `nightly_docker` parity tier (issue #1025, epic #974).
#
# It is the ONE command that drives the heavy, live-Cassandra differential
# checks together and publishes a two-class report. It does NOT duplicate any
# per-leg logic — it INVOKES the existing per-leg machinery and aggregates:
#
#   HARD-FAIL legs (a failure fails the whole lane):
#     1. live read-back semantic equivalence   -> test-data/scripts/e2e-cassandra-readback.sh
#     2. BTI (da) sstabledump logical parity    -> cqlite-core issue_911_bti_sstabledump_parity
#     3. differential compaction LOGICAL parity -> compaction-parity/ (gradle test)
#     4. Bloom no-false-negative membership      -> cqlite-core
#          filter_db_strict_parameters_and_no_false_negative
#          (a single false negative is P0 data loss — NEVER advisory; owner fork F4)
#
#   ADVISORY legs (run continue-on-error; never fail the lane on their own):
#     5. differential compaction BYTE tier       -> compaction-parity/ (gradle byteParity)
#          (the #842 north star, non-blocking per epic #974 / owner fork F5)
#     6. statistical Bloom FPR threshold          -> cqlite-core
#          filter_db_statistical_false_positive_rate_slow (CQLITE_FILTER_FPR_SLOW=1)
#          (measured FPR vs configured bloom_filter_fp_chance — ADVISORY at first
#           per owner fork F4; report-only, promoted to hard-fail when larger
#           fixtures land — see manifest cass.filter_db.statistical_false_positive_rate
#           scope.next_step)
#
# Cassandra is pinned to the SAME version the committed parity corpus was
# generated against (5.0.2, cassandra-5.0.2 / git f278f677…). This script does
# NOT introduce a second pin: the live read-back leg reuses
# test-data/docker/docker-compose-cassandra5.yml (image cassandra:5.0.2) and the
# compaction leg reuses compaction-parity/scripts/bootstrap-cassandra.sh
# (CASSANDRA_REF=cassandra-5.0.2).
#
# The exit code is 0 iff every HARD-FAIL leg passed. Advisory leg outcomes are
# recorded in the report but never change the exit code.
#
# Outputs (under $REPORT_DIR, default target/nightly-docker-parity):
#   report.md              two-class per-leg table + Bloom FPR summary + repro cmds
#   fixture-metadata.txt   pinned Cassandra version + git SHA + image
#   bloom-fpr-summary.txt  measured FPR vs configured fp_chance (from leg 6)
#   logs/<leg>.log         captured stdout+stderr for each leg
#   diffs/                 per-scenario JSONL diffs surfaced by the legs
#
# Usage:
#   bash test-data/scripts/nightly-docker-parity.sh [--bin PATH] [--skip-build]
#                                                    [--report-dir DIR]
#                                                    [--skip-compaction] [--skip-live]
#
#   --bin PATH        Pre-built cqlite (write-support) binary (default: build it).
#   --skip-build      Do not cargo build the candidate binary (use --bin / existing).
#   --report-dir DIR  Where to write the report + artifacts (default below).
#   --skip-compaction Skip the compaction legs (3 + 5). For environments without
#                     a JDK/ant Cassandra-source build. The skipped HARD-FAIL leg
#                     is recorded as SKIPPED and does NOT fail the lane (a skipped
#                     leg is honestly reported, never silently green) — this is an
#                     EXPLICIT user skip and stays a legitimate SKIP even under
#                     strict mode.
#   --skip-live       Skip the live read-back leg (1) when no Docker is available.
#                     EXPLICIT user skip — a legitimate SKIP even under strict mode.
#
# ---------------------------------------------------------------------------
# STRICT MODE (issue #1025) — run-or-fail for HARD legs in the real lane.
# ---------------------------------------------------------------------------
# In the REAL scheduled nightly lane every HARD leg must RUN or FAIL: if a HARD
# leg cannot actually execute (failed Cassandra bootstrap, missing Docker image,
# live checks skipped), that is a FAIL — never a non-failing SKIP that lets the
# aggregate runner exit 0. The workflow sets NIGHTLY_DOCKER_STRICT=1 (it has
# Docker/JDK/gradle/Cassandra available), so infra breakage REDS the lane.
#
# Under strict mode a HARD leg recorded SKIPPED for any reason OTHER than an
# EXPLICIT user skip flag (--skip-compaction / --skip-live) is converted to FAIL
# (fail-closed) and propagates to a non-zero exit. Explicit user skips remain a
# legitimate SKIP even in strict mode (the user asked). ADVISORY legs may always
# SKIP. Local/non-lane smoke runs (strict OFF) skip cleanly so the agent-gate
# stays green.
#
# This script is the local-repro path AND what the workflow calls. Reproduce a CI
# failure locally with the exact same command the report prints.
#
set -uo pipefail

# ---------------------------------------------------------------------------
# Pinned Cassandra version — single source of truth lives in the reused
# machinery; restated here only for the fixture-metadata artifact (NOT a new
# pin). MUST match docker-compose-cassandra5.yml (cassandra:5.0.2) and
# bootstrap-cassandra.sh (CASSANDRA_REF=cassandra-5.0.2).
# ---------------------------------------------------------------------------
CASSANDRA_REF="${CASSANDRA_REF:-cassandra-5.0.2}"
CASSANDRA_IMAGE="cassandra:5.0.2"
CASSANDRA_GIT_SHA="f278f6774fc76465c182041e081982105c3e7dbb"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

CQLITE_BIN_OVERRIDE=""
SKIP_BUILD=0
SKIP_COMPACTION=0
SKIP_LIVE=0
REPORT_DIR="${REPORT_DIR:-$ROOT/target/nightly-docker-parity}"

# Strict mode (issue #1025): the real scheduled lane sets NIGHTLY_DOCKER_STRICT=1.
# When on, a HARD leg that SKIPs for any reason OTHER than an explicit user skip
# flag is converted to FAIL (run-or-fail). Truthy = "1"/"true".
STRICT=0
case "${NIGHTLY_DOCKER_STRICT:-0}" in
  1|true|TRUE|yes|on) STRICT=1 ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin) CQLITE_BIN_OVERRIDE="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-compaction) SKIP_COMPACTION=1; shift ;;
    --skip-live) SKIP_LIVE=1; shift ;;
    --report-dir) REPORT_DIR="$2"; shift 2 ;;
    -h|--help) sed -n '2,60p' "$0"; exit 0 ;;
    *) echo "[nightly-docker-parity][ERROR] Unknown arg: $1" >&2; exit 2 ;;
  esac
done

log()  { printf '[nightly-docker-parity] %s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S')" "$*" >&2; }
warn() { printf '[nightly-docker-parity][WARN] %s\n' "$*" >&2; }

LOG_DIR="$REPORT_DIR/logs"
DIFF_DIR="$REPORT_DIR/diffs"
REPORT_MD="$REPORT_DIR/report.md"
FIXTURE_META="$REPORT_DIR/fixture-metadata.txt"
FPR_SUMMARY="$REPORT_DIR/bloom-fpr-summary.txt"
mkdir -p "$LOG_DIR" "$DIFF_DIR"

# ---------------------------------------------------------------------------
# Leg bookkeeping. Parallel arrays keyed by insertion order so the report is
# deterministic. CLASS is HARD or ADVISORY; OUTCOME is PASS/FAIL/SKIPPED.
# ---------------------------------------------------------------------------
LEG_NAMES=()
LEG_CLASSES=()
LEG_OUTCOMES=()
LEG_CMDS=()

HARD_FAIL=0   # set to 1 if any HARD leg FAILs

record_leg() {
  # record_leg <name> <HARD|ADVISORY> <PASS|FAIL|SKIPPED> <repro-cmd> [user-skip]
  #
  # The optional 5th arg, when "user-skip", marks a SKIP that the user explicitly
  # requested via a dispatch/CLI flag (--skip-compaction / --skip-live). Such a
  # SKIP stays a legitimate SKIP even under strict mode. Any OTHER HARD SKIP under
  # strict mode is an infra failure (Docker/gradle/Cassandra unavailable) and is
  # converted to FAIL fail-closed (issue #1025 run-or-fail invariant).
  local name="$1" class="$2" outcome="$3" cmd="$4" user_skip="${5:-}"
  if [[ "$STRICT" -eq 1 && "$class" == "HARD" && "$outcome" == "SKIPPED" \
        && "$user_skip" != "user-skip" ]]; then
    warn "STRICT: HARD leg '$name' SKIPPED for a non-user reason (infra unavailable) \
— converting to FAIL (run-or-fail)."
    outcome="FAIL"
  fi
  LEG_NAMES+=("$name")
  LEG_CLASSES+=("$class")
  LEG_OUTCOMES+=("$outcome")
  LEG_CMDS+=("$cmd")
  if [[ "$class" == "HARD" && "$outcome" == "FAIL" ]]; then
    HARD_FAIL=1
  fi
}

# Run a leg, capture its log, return its exit status (does not itself decide
# pass/fail classification — the caller does, so advisory legs can ignore it).
run_leg() {
  # run_leg <log-basename> <command...>
  local logname="$1"; shift
  log "RUN ${logname}: $*"
  ( "$@" ) >"$LOG_DIR/${logname}.log" 2>&1
  local rc=$?
  log "DONE ${logname}: exit ${rc}"
  return $rc
}

# Surface any sstabledump JSONL diffs a leg produced into the diffs dir so the
# uploaded artifact carries per-scenario diffs (best-effort; never fatal).
collect_diffs() {
  local src="$1" tag="$2"
  [[ -d "$src" ]] || return 0
  mkdir -p "$DIFF_DIR/$tag"
  # Copy normalized JSONL + any *diff* text the harness wrote.
  find "$src" \( -name '*.jsonl' -o -name '*diff*' -o -name '*.txt' \) -type f \
    -exec cp -p {} "$DIFF_DIR/$tag/" \; 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Fixture metadata artifact (pinned version + git SHA + image).
# ---------------------------------------------------------------------------
{
  echo "nightly-docker-parity fixture metadata"
  echo "cassandra_ref:     $CASSANDRA_REF"
  echo "cassandra_image:   $CASSANDRA_IMAGE"
  echo "cassandra_git_sha: $CASSANDRA_GIT_SHA"
  echo "repo_commit:       $(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo 'unknown')"
  echo "generated_at:      $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
} >"$FIXTURE_META"

# ---------------------------------------------------------------------------
# Candidate binary (cqlite with write-support) for the live + compaction legs.
# ---------------------------------------------------------------------------
CQLITE_BIN="$CQLITE_BIN_OVERRIDE"
if [[ -z "$CQLITE_BIN" ]]; then
  CQLITE_BIN="$ROOT/target/debug/cqlite"
fi
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  log "Building cqlite-cli (write-support) candidate binary…"
  # A failed candidate build is a HARD failure: continuing would run the legs
  # against a STALE target/debug/cqlite from a previous build and report a
  # misleading pass (issue #1025). --skip-build is the ONLY path that tolerates a
  # pre-existing binary (the user opted in), and even then we verify it exists
  # below. Abort the whole lane non-zero here so a build break never hides behind
  # a stale binary. (A successful `cargo build` is, by definition, up to date with
  # the sources — exit 0 + the existence check below is the "produced by this run"
  # guarantee; a no-op rebuild of an already-current binary is legitimately fine.)
  if ! ( cd "$ROOT" && cargo build --package cqlite-cli --features write-support ) \
        >"$LOG_DIR/build-cqlite.log" 2>&1; then
    log "[ERROR] candidate binary build FAILED — see logs/build-cqlite.log. Aborting the lane \
(use --skip-build only to deliberately reuse a pre-existing binary)."
    exit 1
  fi
fi

# Verify the binary the legs will actually run exists and is executable BEFORE
# running any leg. A missing binary — e.g. --skip-build with nothing built, or a
# build that wrote nowhere we expect — is a HARD failure, never a leg run against
# a stale/absent binary (issue #1025). With --skip-build this is the ONLY
# tolerated path and even then the binary must exist; without it, a successful
# build must have produced the binary or something is wrong.
if [[ ! -x "$CQLITE_BIN" ]]; then
  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    detail='--skip-build was supplied but no usable binary exists (build it first, or drop --skip-build).'
  else
    detail='the candidate build reported success but did not produce it.'
  fi
  log "[ERROR] selected cqlite binary not found or not executable: $CQLITE_BIN. $detail \
Aborting the lane rather than running legs against a stale/absent binary."
  exit 1
fi
log "Using cqlite binary: $CQLITE_BIN"

# ===========================================================================
# Leg 1 — live read-back semantic equivalence (HARD)
# Reuses the existing e2e-cassandra-readback.sh harness, which stands up
# Cassandra 5.0.2 via docker-compose-cassandra5.yml (start-clean.sh + EXIT-trap
# cleanup) and drives write -> flush -> export -> nodetool refresh -> cqlsh.
# ===========================================================================
LIVE_CMD="bash test-data/scripts/e2e-cassandra-readback.sh --no-build --bin <cqlite>"
if [[ "$SKIP_LIVE" -eq 1 ]]; then
  log "SKIP live read-back leg (--skip-live)"
  record_leg "live_readback_semantic" "HARD" "SKIPPED" "$LIVE_CMD" "user-skip"
elif ! command -v docker >/dev/null 2>&1; then
  warn "docker not available — SKIPPING live read-back leg"
  record_leg "live_readback_semantic" "HARD" "SKIPPED" "$LIVE_CMD"
else
  if E2E_ARTIFACT_DIR="$DIFF_DIR/live_readback" COMPOSE_CMD="${COMPOSE_CMD:-docker compose}" \
       run_leg "live_readback" \
       bash "$ROOT/test-data/scripts/e2e-cassandra-readback.sh" \
         --no-build --bin "$CQLITE_BIN"; then
    record_leg "live_readback_semantic" "HARD" "PASS" "$LIVE_CMD"
  else
    record_leg "live_readback_semantic" "HARD" "FAIL" "$LIVE_CMD"
  fi
  collect_diffs "$DIFF_DIR/live_readback" "live_readback"
fi

# ===========================================================================
# Leg 2 — BTI (da) sstabledump logical parity (HARD)
# Reuses the committed gen-wide-bti.sh golden + the issue_911 parity test.
# ===========================================================================
BTI_CMD="cargo test -p cqlite-core --features write-support --test issue_911_bti_sstabledump_parity"
if run_leg "bti_da_parity" bash -c \
     "cd '$ROOT' && CQLITE_DATASETS_ROOT='${CQLITE_DATASETS_ROOT:-$ROOT/test-data/datasets}' \
      cargo test -p cqlite-core --features write-support \
      --test issue_911_bti_sstabledump_parity"; then
  record_leg "bti_da_sstabledump_parity" "HARD" "PASS" "$BTI_CMD"
else
  record_leg "bti_da_sstabledump_parity" "HARD" "FAIL" "$BTI_CMD"
fi

# ===========================================================================
# Leg 4 — Bloom no-false-negative membership (HARD, ALWAYS — owner fork F4)
# Reuses filter_db_strict_parameters_and_no_false_negative: every key Cassandra
# wrote MUST be reported "maybe present". A single false negative is P0 data
# loss and fails the lane regardless of any FPR-threshold policy.
# (Numbered "Leg 4" to match the design's leg order; ordered here before the
# slower compaction legs so the cheap correctness gate runs early.)
# ===========================================================================
BLOOM_NFN_CMD="cargo test -p cqlite-core --features write-support --test sstable_parity_filter_db_test filter_db_strict_parameters_and_no_false_negative"
if run_leg "bloom_no_false_negative" bash -c \
     "cd '$ROOT' && CQLITE_DATASETS_ROOT='${CQLITE_DATASETS_ROOT:-$ROOT/test-data/datasets}' \
      cargo test -p cqlite-core --features write-support \
      --test sstable_parity_filter_db_test \
      filter_db_strict_parameters_and_no_false_negative -- --nocapture"; then
  record_leg "bloom_no_false_negative" "HARD" "PASS" "$BLOOM_NFN_CMD"
else
  record_leg "bloom_no_false_negative" "HARD" "FAIL" "$BLOOM_NFN_CMD"
fi
# Surface the no-false-negative leg's log as a diff artifact too.
cp -p "$LOG_DIR/bloom_no_false_negative.log" "$DIFF_DIR/" 2>/dev/null || true

# ===========================================================================
# Leg 6 — statistical Bloom FPR (ADVISORY at first — owner fork F4)
# Reuses filter_db_statistical_false_positive_rate_slow under
# CQLITE_FILTER_FPR_SLOW=1 to MEASURE the FPR and report it vs the configured
# bloom_filter_fp_chance. ADVISORY: its outcome never changes the lane's exit
# code while it remains advisory (promote to HARD when larger-cardinality
# fixtures make the measured FPR track fp_chance — see manifest scope.next_step).
# ===========================================================================
FPR_CMD="env CQLITE_FILTER_FPR_SLOW=1 cargo test -p cqlite-core --features write-support --test sstable_parity_filter_db_test filter_db_statistical_false_positive_rate_slow -- --nocapture"
if run_leg "bloom_statistical_fpr" bash -c \
     "cd '$ROOT' && CQLITE_DATASETS_ROOT='${CQLITE_DATASETS_ROOT:-$ROOT/test-data/datasets}' \
      CQLITE_FILTER_FPR_SLOW=1 cargo test -p cqlite-core --features write-support \
      --test sstable_parity_filter_db_test \
      filter_db_statistical_false_positive_rate_slow -- --nocapture"; then
  record_leg "bloom_statistical_fpr (advisory)" "ADVISORY" "PASS" "$FPR_CMD"
else
  record_leg "bloom_statistical_fpr (advisory)" "ADVISORY" "FAIL" "$FPR_CMD"
fi

# Build the Bloom FPR summary artifact from the two Bloom legs' logs. The
# no-false-negative leg prints distinct (hash_count, num_longs) + the aggregate
# present/false-negative/false-positive counts; the statistical leg prints the
# per-fixture measured FPR. Configured fp_chance for the committed corpus is the
# Cassandra default (0.01) unless a fixture overrides it — recorded for context.
{
  echo "Bloom filter FPR summary (nightly-docker-parity)"
  echo "configured bloom_filter_fp_chance (corpus default): 0.01"
  echo "policy: no-false-negative = HARD-FAIL (always); statistical FPR threshold = ADVISORY"
  echo
  echo "== no-false-negative leg (HARD) =="
  grep -E 'distinct \(hash_count|present-keys-probed|false-negatives|FALSE NEGATIVE' \
    "$LOG_DIR/bloom_no_false_negative.log" 2>/dev/null || echo "(no summary lines captured)"
  echo
  echo "== measured per-fixture FPR (ADVISORY) =="
  grep -E '^FPR |measured_fpr=' "$LOG_DIR/bloom_statistical_fpr.log" 2>/dev/null \
    || echo "(no FPR lines captured — leg may have skipped or failed to build)"
} >"$FPR_SUMMARY"

# ===========================================================================
# Leg 3 — differential compaction LOGICAL parity (HARD)
# Leg 5 — differential compaction BYTE tier (ADVISORY)
# Reuses compaction-parity/ exactly as compaction-parity.yml: build the pinned
# Cassandra source via bootstrap-cassandra.sh (CASSANDRA_REF=cassandra-5.0.2),
# then `gradle test` (logical, HARD) and `gradle byteParity` (byte, ADVISORY).
# ===========================================================================
LOGICAL_CMD="cd compaction-parity && gradle --no-daemon test"
BYTE_CMD="cd compaction-parity && gradle --no-daemon byteParity"
if [[ "$SKIP_COMPACTION" -eq 1 ]]; then
  log "SKIP compaction legs (--skip-compaction)"
  record_leg "compaction_logical_parity" "HARD" "SKIPPED" "$LOGICAL_CMD" "user-skip"
  record_leg "compaction_byte_tier (advisory)" "ADVISORY" "SKIPPED" "$BYTE_CMD"
elif ! command -v gradle >/dev/null 2>&1; then
  warn "gradle not available — SKIPPING compaction legs"
  record_leg "compaction_logical_parity" "HARD" "SKIPPED" "$LOGICAL_CMD"
  record_leg "compaction_byte_tier (advisory)" "ADVISORY" "SKIPPED" "$BYTE_CMD"
else
  CASS_SRC=""
  if run_leg "compaction_bootstrap" bash -c \
       "CASSANDRA_REF='$CASSANDRA_REF' bash '$ROOT/compaction-parity/scripts/bootstrap-cassandra.sh'"; then
    CASS_SRC="$(tail -n1 "$LOG_DIR/compaction_bootstrap.log" 2>/dev/null || true)"
  else
    warn "Cassandra source bootstrap FAILED — compaction legs cannot run"
  fi

  if [[ -n "$CASS_SRC" && -d "$CASS_SRC" ]]; then
    # Logical tier (HARD).
    if run_leg "compaction_logical" bash -c \
         "cd '$ROOT/compaction-parity' && CQLITE_CASSANDRA_SRC='$CASS_SRC' \
          CQLITE_BIN='$CQLITE_BIN' gradle --no-daemon test"; then
      record_leg "compaction_logical_parity" "HARD" "PASS" "$LOGICAL_CMD"
    else
      record_leg "compaction_logical_parity" "HARD" "FAIL" "$LOGICAL_CMD"
    fi
    collect_diffs "$ROOT/compaction-parity/build/parity-artifacts-test" "compaction_logical"

    # Byte tier (ADVISORY — continue-on-error; outcome recorded, never gates).
    if run_leg "compaction_byte" bash -c \
         "cd '$ROOT/compaction-parity' && CQLITE_CASSANDRA_SRC='$CASS_SRC' \
          CQLITE_BIN='$CQLITE_BIN' gradle --no-daemon byteParity"; then
      record_leg "compaction_byte_tier (advisory)" "ADVISORY" "PASS" "$BYTE_CMD"
    else
      record_leg "compaction_byte_tier (advisory)" "ADVISORY" "FAIL" "$BYTE_CMD"
    fi
    collect_diffs "$ROOT/compaction-parity/build/parity-artifacts-byteParity" "compaction_byte"
  else
    record_leg "compaction_logical_parity" "HARD" "SKIPPED" "$LOGICAL_CMD"
    record_leg "compaction_byte_tier (advisory)" "ADVISORY" "SKIPPED" "$BYTE_CMD"
  fi
fi

# ===========================================================================
# Two-class report (per-leg class + outcome) + Bloom FPR summary + repro cmds.
# ===========================================================================
emit_report() {
  echo "# Nightly Docker Parity (nightly_docker tier)"
  echo
  echo "Pinned Cassandra: \`$CASSANDRA_REF\` (image \`$CASSANDRA_IMAGE\`, git \`$CASSANDRA_GIT_SHA\`)."
  echo
  echo "The lane fails iff a **hard-fail** leg fails. **Advisory** legs (compaction"
  echo "byte tier, statistical Bloom FPR threshold) never fail the lane on their own."
  echo "The Bloom **no-false-negative** property is ALWAYS hard-fail (P0 data loss)."
  echo
  if [[ "$STRICT" -eq 1 ]]; then
    echo "**Strict mode ON** (\`NIGHTLY_DOCKER_STRICT=1\`): a HARD leg that SKIPs for"
    echo "any reason other than an explicit user skip flag (\`--skip-compaction\` /"
    echo "\`--skip-live\`) is converted to FAIL — HARD legs are run-or-fail in the lane."
  else
    echo "Strict mode OFF: HARD legs may SKIP cleanly when infra is unavailable (local repro)."
  fi
  echo
  echo "## Per-leg results"
  echo
  echo "| Leg | Class | Outcome |"
  echo "|---|---|---|"
  local i
  for i in "${!LEG_NAMES[@]}"; do
    local mark="${LEG_OUTCOMES[$i]}"
    case "$mark" in
      PASS)    mark="PASS ✅" ;;
      FAIL)    mark="FAIL ❌" ;;
      SKIPPED) mark="SKIPPED ⚠️" ;;
    esac
    echo "| ${LEG_NAMES[$i]} | ${LEG_CLASSES[$i]} | ${mark} |"
  done
  echo
  echo "## Bloom FPR summary"
  echo
  echo '```'
  cat "$FPR_SUMMARY" 2>/dev/null || echo "(no FPR summary)"
  echo '```'
  echo
  echo "## Reproduce locally"
  echo
  echo "Run the whole lane:"
  echo '```bash'
  echo "bash test-data/scripts/nightly-docker-parity.sh"
  echo '```'
  echo
  echo "Per-leg commands:"
  echo '```bash'
  for i in "${!LEG_NAMES[@]}"; do
    echo "# ${LEG_NAMES[$i]} (${LEG_CLASSES[$i]})"
    echo "${LEG_CMDS[$i]}"
  done
  echo '```'
  echo
  echo "Fixture metadata: see \`fixture-metadata.txt\`. Per-leg logs: \`logs/\`."
  echo "Per-scenario JSONL diffs: \`diffs/\`."
}

emit_report >"$REPORT_MD"

# Mirror the report into the GitHub step summary when running in Actions.
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  cat "$REPORT_MD" >>"$GITHUB_STEP_SUMMARY"
fi

log "Report written to $REPORT_MD"
cat "$REPORT_MD" >&2

if [[ "$HARD_FAIL" -eq 1 ]]; then
  if [[ "$STRICT" -eq 1 ]]; then
    log "RESULT: FAIL — a hard-fail leg failed or (strict mode) a HARD leg could not run."
  else
    log "RESULT: FAIL — at least one hard-fail leg failed."
  fi
  exit 1
fi
log "RESULT: PASS — all hard-fail legs passed (advisory outcomes recorded only)."
exit 0
