#!/usr/bin/env bash
# Self-test for _brt_partition_targets, the binding-rust-tests lane's integration-target
# partitioner (issue #3522, roborev round 2 C3).
#
# WHAT IT COVERS AND WHY IT EXISTS. The helper splits a package's DERIVED integration
# targets into the ones cargo can run at the lane's feature set and the ones it will
# SILENTLY skip for an unmet `required-features`. Its output is read back with a single
# `read` into three variables, and that read is where C3 lived: with a TAB delimiter, an
# EMPTY LEADING FIELD is silently dropped (tab is IFS whitespace, so runs collapse and
# leading/trailing separators are stripped). The consequence was not cosmetic — the skip
# field would be misread as the runnable ids, the census would announce unrunnable targets
# as EXECUTED, and the observation guard would then fail on an impossible runner id.
#
# That case is UNREACHABLE TODAY (cqlite-node declares zero integration targets and
# cqlite-ffi-common's two require no features), which is exactly why it needs a test: the
# helper's entire purpose is to be correct the moment that stops being true, so its
# correctness cannot be established by running the lane.
#
# Needs NO cargo, NO network and NO datasets: the helper reads only its two arguments, so
# every case is synthetic metadata. Deterministic and fast.
# FAILS CLOSED: an unsourceable helper is a FAIL, never a skip.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
[ -r "$GATE" ] || { echo "FAIL: cannot read $GATE" >&2; exit 1; }

# Source ONLY the function under test, out of the real gate script — never a copy. A copy
# would pass while the shipped helper rotted, which is the drift this whole issue is about.
helper_src=$(sed -n '/^_brt_partition_targets() {/,/^}$/p' "$GATE")
[ -n "$helper_src" ] || { echo "FAIL: could not extract _brt_partition_targets from $GATE — the function was renamed or reshaped; this self-test must not pass having tested nothing" >&2; exit 1; }
eval "$helper_src" || { echo "FAIL: extracted _brt_partition_targets does not parse" >&2; exit 1; }

PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

TAB=$(printf '\t')

# check <name> <meta> <enabled> <want-ids> <want-names> <want-skip-substring-or-EMPTY>
#
# Reads the helper's output exactly as the production caller does — same IFS, same single
# `read` — so a delimiter regression fails HERE rather than in the lane.
check() {
  local name="$1" meta="$2" enabled="$3" wid="$4" wnm="$5" wsk="$6"
  local gid gnm gsk
  IFS=$'\037' read -r gid gnm gsk <<< "$(_brt_partition_targets "$meta" "$enabled")"
  local errs=""
  [ "$gid" = "$wid" ] || errs="$errs ids=[$gid] want=[$wid];"
  [ "$gnm" = "$wnm" ] || errs="$errs names=[$gnm] want=[$wnm];"
  if [ -z "$wsk" ]; then
    [ -z "$gsk" ] || errs="$errs skip=[$gsk] want empty;"
  else
    case "$gsk" in *"$wsk"*) ;; *) errs="$errs skip=[$gsk] want to contain [$wsk];" ;; esac
  fi
  if [ -z "$errs" ]; then ok "$name"; else bad "$name —$errs"; fi
}

# 1) The shape in production today: two targets, no required-features, all runnable.
check "two unconditional targets are all runnable" \
  "dependency_boundary${TAB}dependency_boundary${TAB}
error_contract_table${TAB}error_contract_table${TAB}" \
  "  " "dependency_boundary error_contract_table" "dependency_boundary error_contract_table" ""

# 2) THE C3 CASE: every declared target requires a DISABLED feature, so the runnable
#    fields are BOTH empty and the empty LEADING field must survive the read. With the old
#    TAB delimiter this returned ids=<the skip text>, which the census then printed as
#    executed targets.
check "all targets gated off -> empty ids AND empty names, skip populated (C3)" \
  "gated_a${TAB}gated_a${TAB}observability
gated_b${TAB}gated_b${TAB}observability" \
  " default write-support " "" "" "gated_a(required-features[observability]:off[observability])"

# 3) A single gated target, same property with one record.
check "one gated target -> empty ids, empty names" \
  "only_gated${TAB}only_gated${TAB}dhat-heap" \
  " default " "" "" "only_gated(required-features[dhat-heap]:off[dhat-heap])"

# 4) MIXED: the runnable and skipped halves must not contaminate each other.
check "mixed runnable + gated partitions correctly" \
  "runs${TAB}runs${TAB}
gated${TAB}gated${TAB}parquet" \
  " default " "runs" "runs" "gated(required-features[parquet]:off[parquet])"

# 5) A target whose required-features ARE enabled counts as runnable.
check "required-features that are all enabled -> runnable" \
  "needs_ws${TAB}needs_ws${TAB}write-support" \
  " default write-support " "needs_ws" "needs_ws" ""

# 6) Multiple required-features, only one missing, is still skipped — and the diagnostic
#    names the SPECIFIC missing one rather than the whole list.
check "partially-satisfied required-features -> skipped, naming the missing feature" \
  "multi${TAB}multi${TAB}write-support,observability" \
  " default write-support " "" "" "off[observability]"

# 7) EMPTY metadata (the zero-integration-target package, i.e. cqlite-node today) yields
#    three empty fields and NOT an error — zero is a derived fact, not a failure.
check "empty metadata -> three empty fields" "" " default " "" "" ""

# 8) The RUNNER ID may differ from the target NAME (a directory-style target maps to
#    `<name>/main`). Both fields must be carried independently — conflating them is what
#    _package_integration_target_ids exists to prevent.
check "runner id distinct from target name is preserved in both fields" \
  "dirstyle${TAB}dirstyle/main${TAB}" \
  " default " "dirstyle/main" "dirstyle" ""

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
