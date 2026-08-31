#!/usr/bin/env bash
# Regression test for issue #2657 (epic #2636): the agent-gate SIDE lane must run
# the isolatable non-core components CONCURRENTLY with the shared-target MAIN cargo
# lane, each in its OWN CARGO_TARGET_DIR (no shared cqlite-core target contention),
# WITHOUT changing the MAIN-serial build profile or the SUMMARY block contract.
#
# It drives the REAL gate lane classifier via the hidden `--classify-lanes` hook
# (no cargo/git/network), so it asserts the actual mapping the pool consumes rather
# than a re-implemented copy. It then statically pins the serial-fallback + isolated
# CARGO_TARGET_DIR invariants that make concurrent execution safe and the summary
# deterministic.
#
# Fast + hermetic: never builds cargo, never runs the real gate to completion.
#
# Run standalone:   bash scripts/tests/test_agent_gate_sublanes.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# The isolatable non-core components #2657 moves onto the concurrent SIDE lane, on
# top of the two bindings the #1737 pool already parallelized. tooling-tests is
# DELIBERATELY EXCLUDED (see EXPECTED_MAIN_ONLY): its embedded timing-sensitive shell
# self-tests (test_worker_supervisor.sh exit-latency, #2666) starved under co-scheduled
# SIDE-lane load, so it stays SERIAL on the MAIN lane.
EXPECTED_NEW_SIDE="parity-report delivery-telemetry binding-unwind-profile smoke memory-budget"
EXPECTED_EXISTING_SIDE="python-bindings node-bindings"
# The #1699 feature-matrix lanes. Each builds cqlite-core (or cqlite-flight against it) at a
# feature set that DIVERGES from MAIN's cli-helpers set, which is precisely the shared-target
# thrash shape #2657 documents — so all four belong on SIDE, each in its own CARGO_TARGET_DIR.
EXPECTED_FEATURE_MATRIX_SIDE="flight-tests legacy-heuristics feature-iso-parquet feature-iso-delta-scan"
# The #3522 binding-side Rust lane. SIDE for the SAME class-(a) reason, and the decision
# rests on the lane's own build properties rather than on where the classifier happened to
# put it: it compiles TWO packages whose cqlite-core feature resolutions BOTH diverge from
# MAIN's. cqlite-node takes cqlite-core with `parquet` (plus state_machine, all-compression,
# cli-helpers and the defaults), and MAIN never enables parquet; cqlite-ffi-common takes it
# with `default-features = false`, a third distinct resolution. Building either against
# MAIN's shared target dir is exactly the feature-thrash shape #2657 documents, so its own
# CARGO_TARGET_DIR is a positive benefit, not a formality.
# It is NOT in EXPECTED_MAIN_ONLY because the #2657 exclusion is for LATENCY-SENSITIVE
# components (tooling-tests' exit-latency self-tests starve under co-scheduled load), and
# this lane makes no wall-clock assertion at all — it runs cargo test over unit and
# integration suites.
EXPECTED_BINDING_RUST_SIDE="binding-rust-tests"
# The #3453 all-features lane. SIDE for the same class-(a) reason, MEASURED both ways
# rather than reasoned (see the rationale on _component_lane): `--all-features` is the
# WIDEST cqlite-core feature set any component builds — 42 features including the OTLP
# stack MAIN never enables — so it shares almost no compilation unit with MAIN's
# `--features cli-helpers` build. Sharing MAIN's target dir buys nothing (all-features
# after a warm cli-helpers build measured 104s vs 99s in a virgin dir) while serializing
# ~100s onto the critical path.
EXPECTED_ALL_FEATURES_SIDE="all-features-check"
# Components that MUST stay on the strictly-serial MAIN lane despite being otherwise
# isolatable — tooling-tests is here because its shell self-tests are latency-sensitive.
EXPECTED_MAIN_ONLY="tooling-tests"

# 1) Drive the real classifier. STDOUT must be the "<lane> <component>" mapping and
#    nothing else (the classify hooks keep STDOUT clean; banners go to STDERR).
lanes_out=$("$GATE" --classify-lanes 2>/dev/null) \
  && ok "--classify-lanes hook exits 0" \
  || bad "--classify-lanes hook failed to run"

# Extract the components assigned to each lane.
side_list=$(printf '%s\n' "$lanes_out" | awk '$1=="side"{print $2}')
main_list=$(printf '%s\n' "$lanes_out" | awk '$1=="main"{print $2}')

lane_of() { printf '%s\n' "$lanes_out" | awk -v c="$1" '$2==c{print $1}'; }

# 2) Every newly-isolatable non-core component must be on the SIDE lane.
for c in $EXPECTED_NEW_SIDE; do
  if [ "$(lane_of "$c")" = side ]; then
    ok "$c runs in the concurrent SIDE lane (#2657)"
  else
    bad "$c is NOT on the SIDE lane (lane='$(lane_of "$c")')"
  fi
done

# 3) The pre-existing bindings must STAY on the SIDE lane (no regression of #1737).
for c in $EXPECTED_EXISTING_SIDE; do
  if [ "$(lane_of "$c")" = side ]; then
    ok "$c still runs in the SIDE lane (#1737 preserved)"
  else
    bad "$c regressed OFF the SIDE lane (lane='$(lane_of "$c")')"
  fi
done

# 3b) The #1699 feature-matrix lanes MUST be on the SIDE lane, each for the same
#     divergent-feature-set reason (#2657). Asserted per component, not only via the
#     exact-membership check below, so a FAIL names which lane moved.
for c in $EXPECTED_FEATURE_MATRIX_SIDE; do
  if [ "$(lane_of "$c")" = side ]; then
    ok "$c runs in the SIDE lane with its own CARGO_TARGET_DIR (#1699 divergent feature set)"
  else
    bad "$c is NOT on the SIDE lane (lane='$(lane_of "$c")') — it builds at a feature set that diverges from MAIN's and would thrash the shared target dir (#2657)"
  fi
done

# 3c) The #3522 binding-side Rust lane, asserted per component for the same reason as 3b:
#     a FAIL should name which lane moved rather than only reporting that the set drifted.
for c in $EXPECTED_BINDING_RUST_SIDE; do
  if [ "$(lane_of "$c")" = side ]; then
    ok "$c runs in the SIDE lane with its own CARGO_TARGET_DIR (#3522: cqlite-node pulls cqlite-core with parquet, cqlite-ffi-common with default-features=false — both diverge from MAIN)"
  else
    bad "$c is NOT on the SIDE lane (lane='$(lane_of "$c")') — it compiles cqlite-ffi-common and cqlite-node, whose cqlite-core feature resolutions both diverge from MAIN's, and would thrash the shared target dir (#2657/#3522)"
  fi
done

# 3d) The #3453 all-features lane, asserted per component for the same reason as 3b/3c.
for c in $EXPECTED_ALL_FEATURES_SIDE; do
  if [ "$(lane_of "$c")" = side ]; then
    ok "$c runs in the SIDE lane with its own CARGO_TARGET_DIR (#3453: --all-features is the widest cqlite-core set any component builds and shares almost no unit with MAIN)"
  else
    bad "$c is NOT on the SIDE lane (lane='$(lane_of "$c")') — it builds cqlite-core at --all-features, which shares almost no compilation unit with MAIN's cli-helpers build (#2657/#3453)"
  fi
done

# 4) core-tests (the shared-target long pole) and the guard components that build
#    cqlite-core under MAIN's feature set MUST stay on the strictly-serial MAIN lane
#    — moving them to a concurrent lane is the shared-target thrash #1737 documents.
for c in core-tests write-tests cli-tests integration-tests fmt clippy tombstones-scan; do
  if [ "$(lane_of "$c")" = main ]; then
    ok "$c stays on the strictly-serial MAIN lane (shared cqlite-core target)"
  else
    bad "$c leaked onto the SIDE lane (shared-target thrash risk!)"
  fi
done

# 4b) tooling-tests MUST stay on the strictly-serial MAIN lane (#2657 gate FAIL):
#     its embedded timing-sensitive shell self-tests (test_worker_supervisor.sh
#     exit-latency, #2666) starved under co-scheduled SIDE-lane load. Keeping it
#     serial preserves its latency headroom.
for c in $EXPECTED_MAIN_ONLY; do
  if [ "$(lane_of "$c")" = main ]; then
    ok "$c stays SERIAL on the MAIN lane (timing-sensitive self-tests, #2657)"
  else
    bad "$c leaked onto the SIDE lane — its latency-sensitive self-tests will starve (#2657)"
  fi
done

# 5) The SIDE lane must be exactly the union of the two sets — nothing else silently
#    joined it, so the MAIN build profile is unchanged for every other component.
side_sorted=$(printf '%s\n' $side_list | sort)
expected_side_sorted=$(printf '%s\n' $EXPECTED_NEW_SIDE $EXPECTED_EXISTING_SIDE $EXPECTED_FEATURE_MATRIX_SIDE $EXPECTED_BINDING_RUST_SIDE $EXPECTED_ALL_FEATURES_SIDE | sort)
# The COUNT IS DERIVED from the expected sets, never written as a literal (#3522). It used
# to read "the 11 expected isolatable components", and adding a twelfth made that number
# wrong in a message that is only ever printed on SUCCESS — so the stale figure would have
# been asserted as fact by a passing test. The decomposition below stays as PROSE naming the
# issues, because that is the part a reader needs and the part arithmetic cannot express.
n_expected_side=$(printf '%s\n' $expected_side_sorted | grep -c .)
if [ "$side_sorted" = "$expected_side_sorted" ]; then
  ok "SIDE lane is exactly the $n_expected_side expected isolatable components (2 #1737 bindings + 5 #2657 isolatable + 4 #1699 feature-matrix lanes + 1 #3522 binding-side Rust lane + 1 #3453 all-features lane; tooling-tests excluded)"
else
  bad "SIDE lane membership drifted:
--- got ---
$side_sorted
--- want ---
$expected_side_sorted"
fi

# 6) Static invariant: the SIDE lane runs each component under its OWN
#    CARGO_TARGET_DIR (per-lane target isolation — the whole point of #2657).
if grep -q 'CARGO_TARGET_DIR="$base/agent-gate-side/$1" dispatch_component "$1"' "$GATE"; then
  ok "SIDE lane isolates each component's CARGO_TARGET_DIR (no shared-target contention)"
else
  bad "run_side_component no longer isolates CARGO_TARGET_DIR"
fi

# 7) Static invariant: AGENT_GATE_JOBS=1 (or bash <4.3) collapses to the historical
#    strictly-sequential run — the serial fallback must remain intact.
if grep -q 'if \[ "$AGENT_GATE_JOBS" -le 1 \] || \[ "${#side_lane\[@\]}" -eq 0 \]; then' "$GATE"; then
  ok "serial fallback intact (AGENT_GATE_JOBS<=1 or empty side lane -> sequential)"
else
  bad "serial fallback guard changed/missing"
fi

# 8) Static invariant: the SUMMARY is reconstructed in canonical COMPONENTS order
#    from per-component .result files regardless of lane/finish order, so widening
#    the SIDE lane cannot reorder or drop a summary line (contract unchanged).
if grep -q 'for _c in "${COMPONENTS\[@\]}"; do' "$GATE"; then
  ok "SUMMARY reconstructed in canonical COMPONENTS order (contract unchanged by lane)"
else
  bad "canonical-order SUMMARY reconstruction loop changed/missing"
fi

# 9) is_side_component and _component_lane agree (single source of truth): every
#    component the classifier calls "side" must be a member of the union above.
for c in $side_list; do
  case " $EXPECTED_NEW_SIDE $EXPECTED_EXISTING_SIDE $EXPECTED_FEATURE_MATRIX_SIDE $EXPECTED_BINDING_RUST_SIDE $EXPECTED_ALL_FEATURES_SIDE " in
    *" $c "*) : ;;
    *) bad "classifier put unexpected '$c' on the SIDE lane" ;;
  esac
done
ok "every SIDE-lane member is an accounted-for isolatable component"

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
