#!/usr/bin/env bash
# Regression tests for issue #4268: agent-gate.sh --recertify <anchor-summary-file>
# --components <c1[,c2]> — host-fault re-certification of <=2 components against a
# same-tree-digest full-gate anchor.
#
# Three layers, cheapest first:
#   1. scripts/lib/recert-component-domains.sh: a completeness census (every real
#      COMPONENTS entry has a domain mapped) plus pure classify cases, no git.
#   2. Real `--recertify` invocations against a scratch git fixture (never this
#      checkout), pinned to a LOCAL origin so the #3544 component-set pre-flight
#      never touches the network (mirrors test_agent_gate_delta.sh's
#      copy_gate_with_pin/add_local_origin). Anchors are SYNTHETIC — a hand-built
#      "==== AGENT-GATE SUMMARY ====" text file whose tree-end: sha/digest come
#      from a REAL, cheap `--only file-size` run against the fixture (so the
#      "current tree matches the anchor" check compares against a genuine
#      identity, never a fabricated one) — never a real 39-component gate, which
#      would be far too expensive to run inside tooling-tests.
#   3. Wiring evidence (structural): acquire_gate_slot's existing ONLY-exemption
#      already covers RECERTIFY once ONLY is set; asserted by inspection rather
#      than by an expensive real slot-contention scenario.
#
# Hermetic: no network (LOCAL bare origin only), no real cargo test/build (the one
# real component ever dispatched is `file-size`, which is cargo-free) or datasets.
#
# Run standalone:   bash scripts/tests/test_recertify.sh
set -uo pipefail
unset AGENT_GATE_SUMMARY_FILE 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GATE="$REPO_ROOT/scripts/agent-gate.sh"
DOMAINS_LIB="$REPO_ROOT/scripts/lib/recert-component-domains.sh"
SCOPE_LIB="$REPO_ROOT/scripts/lib/tooling-tests-scope.sh"
# shellcheck source=scripts/tests/lib/agent-gate-canonical-pin.bash
. "$SCRIPT_DIR/lib/agent-gate-canonical-pin.bash"

# add_local_origin <repo> (mirrors test_agent_gate_delta.sh's own helper of the
# same name — that one is a PER-FILE helper, not part of the shared
# agent-gate-canonical-pin.bash lib, so it is reproduced here rather than
# sourced): give a scratch fixture a LOCAL bare `origin` whose path
# agent_gate_pin_canonical_remote already rewrote the fixture's gate copy to
# treat as canonical, so the #3544 component-set pre-flight fetches a REAL
# (path, never network) baseline instead of FAIL-CLOSING on "no origin remote".
add_local_origin() {
  local repo="${1:-}"
  [ -n "$repo" ] && [ -d "$repo" ] \
    || { echo "FATAL: add_local_origin needs an existing fixture dir (got '${1:-}')" >&2; exit 1; }
  git init -q --bare "$repo.origin.git" >/dev/null 2>&1
  git -C "$repo.origin.git" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
  ( cd "$repo" \
      && git remote add origin "$repo.origin.git" \
      && git push -q origin HEAD:refs/heads/main ) >/dev/null 2>&1
}

PASS=0
FAIL=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Sourced AFTER ok/bad (it refuses to load without them). Provides the
# THREE-VALUED file probes W3 uses: an ABSENCE assert spelled `if grep -q
# <forbidden>; then bad; else ok; fi` folds "could not read the subject" onto
# `ok`, i.e. an unreadable file CERTIFIES the property (#3752).
# shellcheck source=scripts/tests/lib/tristate-file-probe.bash
. "$SCRIPT_DIR/lib/tristate-file-probe.bash"

[ -f "$GATE" ] || { echo "FATAL: $GATE not found"; exit 1; }
[ -f "$DOMAINS_LIB" ] || { echo "FATAL: $DOMAINS_LIB not found"; exit 1; }

# ==== Layer 1: domain table + pure classification ============================
declared_components=$(bash "$GATE" --list 2>/dev/null)
n_components=$(printf '%s\n' "$declared_components" | awk 'NF' | wc -l | tr -d ' ')
if [ "$n_components" -lt 30 ]; then
  echo "FATAL: agent-gate.sh --list returned $n_components names (expected ~39) — cannot run the completeness census"
  exit 1
fi

# Completeness census: every real component has a domain mapped (pure, no git).
# Sources SCOPE_LIB (#4266) BEFORE DOMAINS_LIB (roborev finding, Medium: an
# earlier cut sourced DOMAINS_LIB alone here, so this census — and the classify
# cases below — exercised the tooling-tests FALLBACK, not the real array
# production actually loads; the two have since diverged from each other on
# purpose to make that drift impossible to miss again, see DOMAINS_LIB's own
# _recert_harness_patterns comment).
missing=""
while IFS= read -r c; do
  [ -n "$c" ] || continue
  dom=$(
    . "$SCOPE_LIB"
    . "$DOMAINS_LIB"
    _recert_component_domain_patterns "$c"
  )
  [ -n "$dom" ] || missing="${missing:+$missing }$c"
done <<<"$declared_components"
if [ -z "$missing" ]; then
  ok "domain-census: every component in agent-gate.sh's live COMPONENTS ($n_components) has a mapped domain"
else
  bad "domain-census: components with NO mapped domain (fail-closed to 'always diff-touched', but undocumented): $missing"
fi

# Pure classify cases (sourced in a subshell so array/function defs don't leak).
classify_domain() {
  local comp="$1" path="$2" want="$3" desc="$4" got
  got=$(
    . "$SCOPE_LIB"
    . "$DOMAINS_LIB"
    if _recert_component_diff_touched "$comp" "$path"; then echo TOUCHED; else echo CLEAR; fi
  )
  if [ "$got" = "$want" ]; then ok "$desc"; else bad "$desc (got $got, wanted $want)"; fi
}
# A dedicated case proving the fallback's fail-closed rc actually WORKS when
# SCOPE_LIB genuinely is NOT loaded (the one scenario the census/classify
# cases above no longer exercise now that they source it): tooling-tests must
# be treated as unrecognized (any diff "touches" it) rather than falling back
# to a hand-maintained second copy.
fallback_got=$(
  . "$DOMAINS_LIB"
  if _recert_component_diff_touched tooling-tests 'docs/development/dev-cookbook.md'; then echo TOUCHED; else echo CLEAR; fi
)
if [ "$fallback_got" = TOUCHED ]; then
  ok "tooling-tests without SCOPE_LIB loaded: fails closed to unrecognized (always diff-touched), never a stale second copy"
else
  bad "tooling-tests without SCOPE_LIB loaded: expected TOUCHED (fail-closed), got $fallback_got"
fi
classify_domain file-size 'cqlite-core/src/lib.rs' TOUCHED "file-size domain covers cqlite-core/src/**"
classify_domain tooling-tests 'scripts/agent-gate.sh' TOUCHED "tooling-tests domain reuses #4266's harness set"
classify_domain tooling-tests 'cqlite-core/src/lib.rs' CLEAR "tooling-tests domain does NOT cover cqlite-core/src/** (the #4268 round-2-style residual is real, not hidden)"
classify_domain python-bindings 'bindings/python/src/result.rs' TOUCHED "python-bindings domain covers bindings/python/**"
classify_domain python-bindings 'bindings/node/src/row.rs' CLEAR "python-bindings domain does NOT cover bindings/node/**"
classify_domain dep-duplicates 'Cargo.lock' TOUCHED "dep-duplicates domain covers Cargo.lock"
classify_domain dep-duplicates 'docs/development/dev-cookbook.md' CLEAR "dep-duplicates domain does NOT cover docs/**"

# job 106 roborev finding (Medium): four arms named paths that are not the
# component's real subject, leaving them fail-open on exactly the diffs that
# could have caused their own failure. Each pair below proves the REAL subject
# now classifies TOUCHED and the OLD (wrong) mapped path classifies CLEAR.
classify_domain integration-tests 'tests/golden_path_get_operations_tests.rs' TOUCHED "integration-tests domain covers tests/** (the real cqlite-integration-tests crate dir)"
classify_domain integration-tests 'cqlite-integration-tests/src/lib.rs' CLEAR "integration-tests domain does NOT cover cqlite-integration-tests/** (that path has never existed; it was the cargo PACKAGE name, not a repo path)"
classify_domain format-compat 'tests/format-compatibility/src/lib.rs' TOUCHED "format-compat domain covers tests/format-compatibility/** (the real format-compatibility-tests crate)"
classify_domain format-compat 'tools/format-validator/src/main.rs' CLEAR "format-compat domain does NOT cover tools/format-validator/** (an unrelated crate)"
classify_domain kit-dashboard-drift 'easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json' TOUCHED "kit-dashboard-drift domain covers easy-db-lab-kits/** (the dashboard it reads)"
classify_domain kit-dashboard-drift 'cqlite-core/src/observability/catalog.rs' TOUCHED "kit-dashboard-drift domain covers cqlite-core/** (the metric catalog + the kit_dashboard_metric_drift test)"
classify_domain kit-dashboard-drift 'website/content/index.md' CLEAR "kit-dashboard-drift domain does NOT cover website/** (never a real subject)"
classify_domain oom-audit 'xtask/src/main.rs' TOUCHED "oom-audit domain covers xtask/** (cargo run -p xtask -- oom-audit --enforce)"

# TOOLCHAIN-CONFIG CENSUS (#4268 roborev finding, Medium): every repo-root build
# CONFIGURATION file must be diff-touched for EVERY component the table MAPS.
#
# This is a census, not spot cases, because the defect it pins is per-arm
# omission: `.clippy.toml` was in no arm's domain at all, so a PR whose whole
# diff was `.clippy.toml` classified clippy — the one component that file
# parameterises — as CLEAR, and `--recertify` would have accepted a host-fault
# recert of it. The file header's "no entry -> always diff-touched" default does
# NOT save this: it applies only to WHOLLY UNMAPPED components, while a MAPPED
# component gets exactly its listed patterns, so an unlisted path is CLEAR for
# it (fail-OPEN).
#
# A census reports an AFFIRMATIVE count and names its subject set, never a bare
# 0 (an unmeasured census and a clean one must not read alike).
#
# THE PROBES ARE CONCRETE REPO PATHS, NOT THE LIBRARY'S OWN PATTERNS. Two of the
# five entries are directory globs (`.cargo/*`, `.config/*`), and feeding a glob
# back in as the changed PATH would only prove that bash `case` is reflexive —
# it would pass even if the domain had been narrowed to that literal string.
# Drift is caught instead by the count assert below: one probe per pattern,
# pinned to the array's own length, so adding a sixth pattern to the library
# without adding a sixth probe here FAILS rather than silently under-measuring.
TC_PROBE_PATHS='rust-toolchain.toml
.clippy.toml
.rustfmt.toml
.cargo/config.toml
.config/nextest.toml'

tc_report=$(
  . "$SCOPE_LIB"
  . "$DOMAINS_LIB"
  pairs=0
  miss=0
  nprobe=0
  offenders=""
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    nprobe=$((nprobe + 1))
  done <<<"$TC_PROBE_PATHS"
  while IFS= read -r c; do
    [ -n "$c" ] || continue
    # Skip components the table does NOT map: they are fail-closed to "always
    # diff-touched" via an EMPTY domain, so asserting TOUCHED for them would
    # pass for a reason having nothing to do with this group.
    [ -n "$(_recert_component_domain_patterns "$c")" ] || continue
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      pairs=$((pairs + 1))
      _recert_component_diff_touched "$c" "$f" && continue
      miss=$((miss + 1))
      # The offender list is CAPPED at 6: the fail-open shape is per-CONFIG-FILE,
      # so the first few pairs already name the cause, while an uncapped list is
      # ~200 entries of noise in a gate log (measured while writing this case).
      [ "$miss" -le 6 ] && offenders="${offenders:+$offenders }$c($f)"
    done <<<"$TC_PROBE_PATHS"
  done <<<"$declared_components"
  # ONE machine-readable line, so a partially-written report cannot be misread as
  # a clean one: every field is present or the whole line is absent.
  printf 'CENSUS pairs=%d miss=%d nprobe=%d npattern=%d offenders=%s\n' \
    "$pairs" "$miss" "$nprobe" "${#_RECERT_DOM_TOOLCHAIN[@]}" "${offenders:-none}"
)
_tc_field() { awk -v k="$1=" '/^CENSUS /{ i = index($0, k); if (i == 0) exit; s = substr($0, i + length(k)); split(s, a, " "); print a[1]; exit }' <<<"$tc_report"; }
tc_pairs=$(_tc_field pairs)
tc_miss=$(_tc_field miss)
tc_nprobe=$(_tc_field nprobe)
tc_npattern=$(_tc_field npattern)
tc_offenders=$(awk -F'offenders=' '/^CENSUS /{ print $2; exit }' <<<"$tc_report")
case "${tc_pairs:-x}${tc_miss:-x}${tc_nprobe:-x}${tc_npattern:-x}" in
  *[!0-9]*)
    bad "toolchain-census: UNMEASURED — the census produced no usable counts (pairs='${tc_pairs:-}' miss='${tc_miss:-}' nprobe='${tc_nprobe:-}' npattern='${tc_npattern:-}'), so it asserts NOTHING about the domain table" ;;
  *)
    if [ "$tc_nprobe" -eq "$tc_npattern" ]; then
      ok "toolchain-census: $tc_nprobe concrete probe path(s) cover all $tc_npattern _RECERT_DOM_TOOLCHAIN pattern(s) — the census measures the whole group"
    else
      bad "toolchain-census: $tc_nprobe probe path(s) for $tc_npattern _RECERT_DOM_TOOLCHAIN pattern(s) — the library grew (or shrank) and TC_PROBE_PATHS did not, so the census below under-measures the group"
    fi
    if [ "$tc_pairs" -lt 30 ]; then
      bad "toolchain-census: only $tc_pairs (component, config-path) pairs were measured — expected ~5x the mapped component count, so the census did not run over the real table"
    elif [ "$tc_miss" -eq 0 ]; then
      ok "toolchain-census: all $tc_pairs (MAPPED component, repo-root build-config path) pairs classify as diff-touched — 0 fail-open gaps"
    else
      bad "toolchain-census: $tc_miss of $tc_pairs pairs classify as CLEAR — a diff touching only that config file would wrongly make the component recert-ELIGIBLE; first offenders: $tc_offenders"
    fi ;;
esac
# Named spot cases as well, so a reader sees the concrete claim and a future
# census refactor cannot quietly stop asserting it. rust-toolchain.toml is
# pinned for all three components a compiler-version bump most obviously
# invalidates; .clippy.toml and the two directory globs get one each.
classify_domain clippy 'rust-toolchain.toml' TOUCHED "clippy domain covers rust-toolchain.toml (the pinned compiler)"
classify_domain fmt 'rust-toolchain.toml' TOUCHED "fmt domain covers rust-toolchain.toml (the pinned compiler)"
classify_domain core-tests 'rust-toolchain.toml' TOUCHED "core-tests domain covers rust-toolchain.toml (the pinned compiler)"
classify_domain clippy '.clippy.toml' TOUCHED "clippy domain covers .clippy.toml (the file that parameterises it)"
classify_domain core-tests '.config/nextest.toml' TOUCHED "core-tests domain covers .config/* (the nextest profiles it runs under)"
classify_domain clippy '.cargo/config.toml' TOUCHED "clippy domain covers .cargo/* (rustflags/target settings cargo reads implicitly)"

# ==== Layer 2: real --recertify invocations against a scratch fixture ========
TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/recertify_test.XXXXXX") || { echo "FATAL: mktemp failed"; exit 1; }
trap 'rm -rf "$TMPROOT"' EXIT

# build_fixture <dir>: a scratch git repo carrying its OWN copies of
# agent-gate.sh + the three libs it sources, pinned to a LOCAL bare origin (never
# the network) via the shared helper every other agent-gate fixture test uses.
build_fixture() {
  local repo="$1"
  mkdir -p "$repo/scripts/lib" || return 1
  cp "$GATE" "$repo/scripts/agent-gate.sh" || return 1
  cp "$DOMAINS_LIB" "$repo/scripts/lib/recert-component-domains.sh" || return 1
  cp "$SCOPE_LIB" "$repo/scripts/lib/tooling-tests-scope.sh" || return 1
  if [ -f "$REPO_ROOT/scripts/perf-capability.sh" ]; then
    cp "$REPO_ROOT/scripts/perf-capability.sh" "$repo/scripts/perf-capability.sh"
  fi
  agent_gate_pin_canonical_remote "$repo/scripts/agent-gate.sh" "$repo.origin.git" || return 1
  agent_gate_install_components_manifest "$repo/scripts/agent-gate.sh" || return 1
  mkdir -p "$repo/cqlite-core/src" "$repo/scripts/tests"
  echo base >"$repo/cqlite-core/src/lib.rs"
  # The COMMITTED canonical CQL schema fixtures (#3148). --recertify reruns its
  # components in FULL-GATE mode, so apply_schemas_preflight is STRICT for it
  # (#4268 roborev finding, High -- see R13 below) and resolves this root
  # CHECKOUT-RELATIVE from the fixture's own REPO_ROOT (the fixture carries no
  # workspace Cargo.toml, so _gate_checkout_test_data_dir falls back to
  # $REPO_ROOT/test-data). Names are read from the gate's OWN
  # CANONICAL_SCHEMA_FILES rather than retyped, so a change to that list cannot
  # leave this fixture silently one file short. Content is irrelevant: the guard
  # asks only for a READABLE REGULAR FILE (_missing_schema_files).
  mkdir -p "$repo/test-data/schemas" || return 1
  local _bf_canon _bf_f
  _bf_canon=$(awk -F'"' '/^CANONICAL_SCHEMA_FILES=/ { print $2; exit }' "$GATE")
  [ -n "$_bf_canon" ] || { echo "FATAL: could not read CANONICAL_SCHEMA_FILES from $GATE" >&2; return 1; }
  for _bf_f in $_bf_canon; do
    printf '%s\n' "-- fixture stub for $_bf_f" >"$repo/test-data/schemas/$_bf_f" || return 1
  done
  ( cd "$repo" \
      && git init -q -b main . \
      && git config user.email t@cqlite.test && git config user.name cqlite-test \
      && git add -A \
      && git commit -qm base ) || return 1
  add_local_origin "$repo"
  return 0
}

# tree_identity <fixture-dir>: run the fixture's OWN gate `--only file-size` for
# real (cheap: no cargo, file-size is dataset-free) and scrape tree-end:'s
# sha/dirty/digest — the SAME hashing a real full-gate block would carry, so a
# synthetic anchor built from it compares as a genuine tree identity, never a
# fabricated one.
tree_identity() { # <fixture-dir> -> "sha dirty digest" on stdout, or empty on failure
  local repo="$1" out te
  out=$(cd "$repo" && bash scripts/agent-gate.sh --only file-size 2>&1)
  # No pipe into an early-exiting reader (W3): awk takes the FIRST tree-end:
  # line itself, and sed reads the captured value from a herestring.
  te=$(awk '/^tree-end:/ { print; exit }' <<<"$out")
  sed -n 's/^tree-end:[[:space:]]*\([^ ]*\) dirty: \([a-z]*\) digest: \([^ ]*\).*/\1 \2 \3/p' <<<"$te"
}

# _override_status_for <component> <override1=status> ...: bash-3.2-safe
# lookup (roborev finding, High — an earlier cut used `local -A overrides`, an
# associative array, which is bash 4.0+; this repo's floor is stock macOS bash
# 3.2, scripts/agent-gate.sh:15209/:15511 and
# test_agent_gate_summary.sh:826-869 document a prior gate-of-record incident
# from exactly this construct. On bash 3.2, `overrides["core-tests"]=…`
# evaluates the subscript ARITHMETICALLY to 0, so every write landed in the
# SAME slot and every component silently read the LAST override — not a
# warning, a silently wrong fixture). Plain positional args + a linear scan;
# the override list is at most 2 entries per test call, so this is not a
# performance concern.
_override_status_for() {
  local comp="$1"; shift
  local kv
  for kv in "$@"; do
    case "$kv" in
      "$comp="*) printf '%s' "${kv#*=}"; return 0 ;;
    esac
  done
  printf 'PASS'
}

# write_anchor <path> <sha> <digest> [override1=status ...]: a hand-built,
# structurally-valid FULL-gate "==== AGENT-GATE SUMMARY ====" block. Every real
# component defaults to PASS; pass e.g. "core-tests=FAIL" to override one.
write_anchor() {
  local out="$1" sha="$2" digest="$3"; shift 3
  {
    echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: synthetic-anchor-$$"
    echo "commit: $sha branch: main dirty: no"
    echo "tree-start: $sha dirty: no digest: $digest"
    echo "tree-end: $sha dirty: no digest: $digest"
    echo "tree-integrity: PASS"
    while IFS= read -r c; do
      [ -n "$c" ] || continue
      echo "${c}: $(_override_status_for "$c" "$@") (1s)"
    done <<<"$declared_components"
    echo "RESULT: PASS"
    echo "==== END AGENT-GATE SUMMARY ===="
  } >"$out"
}

run_recert() { # <fixture-dir> <anchor-file> <components> -> sets RC, OUT
  OUT=$( (cd "$1" && bash scripts/agent-gate.sh --recertify "$2" --components "$3") 2>&1 )
  RC=$?
}

# run_recert_env <fixture-dir> <anchor-file> <components> <VAR=VALUE>...: the
# same invocation with environment variables seeded, for the #3148 schemas cases
# (R13). `env` rather than exported assignments so nothing can leak into a later
# case in this file.
run_recert_env() {
  local repo="$1" anchor_f="$2" comps="$3"; shift 3
  OUT=$( (cd "$repo" && env "$@" bash scripts/agent-gate.sh --recertify "$anchor_f" --components "$comps") 2>&1 )
  RC=$?
}

fixture="$TMPROOT/wt"
if ! build_fixture "$fixture" >"$TMPROOT/build.log" 2>&1; then
  bad "could not build the layer-2 fixture repo (see $TMPROOT/build.log) — skipping all real-invocation cases"
  cat "$TMPROOT/build.log" 2>/dev/null
else
  ident=$(tree_identity "$fixture")
  f_sha=$(printf '%s' "$ident" | awk '{print $1}')
  f_dirty=$(printf '%s' "$ident" | awk '{print $2}')
  f_digest=$(printf '%s' "$ident" | awk '{print $3}')
  if [ -z "$f_sha" ] || [ -z "$f_digest" ]; then
    bad "could not scrape a real tree identity from the fixture's own --only file-size run — skipping all real-invocation cases"
  else
    anchor="$TMPROOT/anchor.txt"
    write_anchor "$anchor" "$f_sha" "$f_digest"

    # R1: a clean, valid anchor + a zero-diff-domain component (file-size, no .rs
    # changed) -> ACCEPTED, falls through, actually runs file-size for real, and
    # the terminal block certifies.
    run_recert "$fixture" "$anchor" file-size
    if [ "$RC" -eq 0 ] && grep -qF "==== AGENT-GATE RECERT SUMMARY ====" <<<"$OUT"; then
      ok "R1: a valid anchor + eligible component is ACCEPTED (distinct RECERT SUMMARY header present)"
    else
      bad "R1: expected acceptance (rc 0, RECERT SUMMARY header), got rc=$RC"
      echo "------- captured -------"; printf '%s\n' "$OUT"; echo "------------------------"
    fi
    if grep -qE '^recert-verdict: CERTIFIED' <<<"$OUT"; then
      ok "R1: recert-verdict is CERTIFIED"
    else
      bad "R1: expected recert-verdict: CERTIFIED"
    fi
    if grep -qE '^file-size: +PASS' <<<"$OUT"; then
      ok "R1: file-size actually ran for real and reported PASS (not merely validated)"
    else
      bad "R1: file-size's own component row is not PASS — the fall-through into the real gate flow did not execute it"
    fi
    if grep -qE '^RESULT: PASS' <<<"$OUT"; then
      ok "R1: RESULT: PASS"
    else
      bad "R1: expected RESULT: PASS"
    fi

    # R2: missing anchor file -> REFUSED, exit 2, distinct header still present.
    run_recert "$fixture" "$TMPROOT/does-not-exist.txt" file-size
    if [ "$RC" -eq 2 ] && grep -qE '^error: .*not found or unreadable' <<<"$OUT"; then
      ok "R2: a missing anchor file REFUSES (exit 2, named cause)"
    else
      bad "R2: expected exit 2 + a named 'not found' cause, got rc=$RC"
    fi

    # R3: >2 components -> REFUSED.
    run_recert "$fixture" "$anchor" "file-size,fmt,clippy"
    if [ "$RC" -eq 2 ] && grep -qE '^error: .*must name 1 or 2' <<<"$OUT"; then
      ok "R3: >2 requested components REFUSES"
    else
      bad "R3: expected exit 2 + a named '1 or 2' cause, got rc=$RC"
    fi

    # R4: an unknown component name -> REFUSED.
    run_recert "$fixture" "$anchor" "not-a-real-component"
    if [ "$RC" -eq 2 ] && grep -qE '^error: unknown component' <<<"$OUT"; then
      ok "R4: an unknown component name REFUSES"
    else
      bad "R4: expected exit 2 + 'unknown component', got rc=$RC"
    fi

    # R5: a duplicate component named twice -> REFUSED.
    run_recert "$fixture" "$anchor" "file-size,file-size"
    if [ "$RC" -eq 2 ] && grep -qE '^error: duplicate component' <<<"$OUT"; then
      ok "R5: a duplicate component name REFUSES"
    else
      bad "R5: expected exit 2 + 'duplicate component', got rc=$RC"
    fi

    # R6: an anchor with a non-PASS/OPT-OUT OTHER component -> REFUSED (#4268 AC1).
    bad_anchor="$TMPROOT/anchor-bad-other.txt"
    write_anchor "$bad_anchor" "$f_sha" "$f_digest" "clippy=FAIL"
    run_recert "$fixture" "$bad_anchor" file-size
    if [ "$RC" -eq 2 ] && grep -qE "^error: anchor component 'clippy' is not PASS/OPT-OUT" <<<"$OUT"; then
      ok "R6: an anchor whose OTHER component is FAIL (not PASS/OPT-OUT) REFUSES"
    else
      bad "R6: expected exit 2 naming 'clippy' not PASS/OPT-OUT, got rc=$RC"
    fi

    # R6b: OPT-OUT for an other component IS accepted (distinct from PASS, both
    # legal) — proves the check is a SET membership, not a literal-PASS compare.
    optout_anchor="$TMPROOT/anchor-optout.txt"
    write_anchor "$optout_anchor" "$f_sha" "$f_digest" "clippy=OPT-OUT"
    run_recert "$fixture" "$optout_anchor" file-size
    if [ "$RC" -eq 0 ] && grep -qE '^recert-verdict: CERTIFIED' <<<"$OUT"; then
      ok "R6b: an anchor whose OTHER component is OPT-OUT (not PASS) is still ACCEPTED"
    else
      bad "R6b: expected acceptance with an OPT-OUT other-component anchor, got rc=$RC"
    fi

    # R6c: SKIP for an other component is REFUSED (#4268: stricter than the
    # generic PASS/SKIP/OPT-OUT nonfailing set — a SKIP means "not measured",
    # not "reviewed and waived").
    skip_anchor="$TMPROOT/anchor-skip.txt"
    write_anchor "$skip_anchor" "$f_sha" "$f_digest" "clippy=SKIP"
    run_recert "$fixture" "$skip_anchor" file-size
    if [ "$RC" -eq 2 ] && grep -qE "^error: anchor component 'clippy' is not PASS/OPT-OUT \\(got 'SKIP'\\)" <<<"$OUT"; then
      ok "R6c: an anchor whose OTHER component is SKIP (not PASS/OPT-OUT) REFUSES"
    else
      bad "R6c: expected exit 2 naming 'clippy' SKIP, got rc=$RC"
    fi

    # R6d: tooling-tests: SKIP is the ONE declared exception (roborev finding,
    # High: #4266's own diff-scoping makes tooling-tests SKIP a DECIDED,
    # reviewed outcome on the common case — a full gate whose diff touches no
    # harness path — not an unmeasured gap; refusing it here would make
    # --recertify unconditionally unusable on exactly the PRs #4266 speeds
    # up). Every OTHER component's SKIP still refuses (R6c, just above).
    tt_skip_anchor="$TMPROOT/anchor-tt-skip.txt"
    write_anchor "$tt_skip_anchor" "$f_sha" "$f_digest" "tooling-tests=SKIP"
    run_recert "$fixture" "$tt_skip_anchor" file-size
    if [ "$RC" -eq 0 ] && grep -qE '^recert-verdict: CERTIFIED' <<<"$OUT"; then
      ok "R6d: an anchor whose tooling-tests is SKIP (the one declared exception) is still ACCEPTED"
    else
      bad "R6d: expected acceptance with a tooling-tests:SKIP anchor, got rc=$RC"
      echo "------- captured -------"; printf '%s\n' "$OUT"; echo "------------------------"
    fi

    # R7: a dirty anchor tree (dirty: yes) -> REFUSED.
    dirty_anchor="$TMPROOT/anchor-dirty.txt"
    write_anchor "$dirty_anchor" "$f_sha" "$f_digest"
    sed -i.bak 's/^tree-end:.*/tree-end: '"$f_sha"' dirty: yes digest: '"$f_digest"'/' "$dirty_anchor" 2>/dev/null \
      || sed -i '' 's/^tree-end:.*/tree-end: '"$f_sha"' dirty: yes digest: '"$f_digest"'/' "$dirty_anchor"
    run_recert "$fixture" "$dirty_anchor" file-size
    if [ "$RC" -eq 2 ] && grep -qE '^error: anchor tree was dirty' <<<"$OUT"; then
      ok "R7: an anchor with dirty:yes REFUSES"
    else
      bad "R7: expected exit 2 + 'anchor tree was dirty', got rc=$RC"
    fi

    # R8: a mismatched digest (a different tree than the current one) -> REFUSED.
    mismatch_anchor="$TMPROOT/anchor-mismatch.txt"
    write_anchor "$mismatch_anchor" "deadbeef0000" "cafef00dcafe"
    run_recert "$fixture" "$mismatch_anchor" file-size
    if [ "$RC" -eq 2 ] && grep -qE '^error: current tree does not match' <<<"$OUT"; then
      ok "R8: a mismatched anchor tree digest REFUSES"
    else
      bad "R8: expected exit 2 + 'current tree does not match', got rc=$RC"
    fi

    # R9: a stale anchor (backdated mtime > 24h) -> REFUSED.
    stale_anchor="$TMPROOT/anchor-stale.txt"
    write_anchor "$stale_anchor" "$f_sha" "$f_digest"
    if touch -t "$(date -v-2d +%Y%m%d%H%M.%S 2>/dev/null || date -d '2 days ago' +%Y%m%d%H%M.%S 2>/dev/null)" "$stale_anchor" 2>/dev/null; then
      run_recert "$fixture" "$stale_anchor" file-size
      if [ "$RC" -eq 2 ] && grep -qE '^error: anchor summary is stale' <<<"$OUT"; then
        ok "R9: an anchor >24h old REFUSES"
      else
        bad "R9: expected exit 2 + 'anchor summary is stale', got rc=$RC"
      fi
    else
      bad "R9: could not backdate the anchor file's mtime with either BSD or GNU 'touch -t' — case not exercised"
    fi

    # R10: a lite/delta/recert-shaped anchor header REFUSES — this is ALSO the
    # mechanism that makes "a recert cannot follow a recert" hold structurally
    # (#4268's explicit limit): a real RECERT SUMMARY block never carries the
    # literal full header, so it can never itself serve as a valid anchor.
    for hdr_kind in LITE DELTA RECERT; do
      chained_anchor="$TMPROOT/anchor-chained-$hdr_kind.txt"
      write_anchor "$chained_anchor" "$f_sha" "$f_digest"
      sed -i.bak "s/^==== AGENT-GATE SUMMARY ====\$/==== AGENT-GATE ${hdr_kind} SUMMARY ====/" "$chained_anchor" 2>/dev/null \
        || sed -i '' "s/^==== AGENT-GATE SUMMARY ====\$/==== AGENT-GATE ${hdr_kind} SUMMARY ====/" "$chained_anchor"
      sed -i.bak "s/^==== END AGENT-GATE SUMMARY ====\$/==== END AGENT-GATE ${hdr_kind} SUMMARY ====/" "$chained_anchor" 2>/dev/null \
        || sed -i '' "s/^==== END AGENT-GATE SUMMARY ====\$/==== END AGENT-GATE ${hdr_kind} SUMMARY ====/" "$chained_anchor"
      run_recert "$fixture" "$chained_anchor" file-size
      if [ "$RC" -eq 2 ] && grep -qE '^error: anchor is not a full-gate SUMMARY block' <<<"$OUT"; then
        ok "R10: a $hdr_kind-shaped anchor header REFUSES (a recert can never chain off another recert)"
      else
        bad "R10 ($hdr_kind): expected exit 2 + 'not a full-gate SUMMARY block', got rc=$RC"
      fi
    done

    # R11: an --only-shaped anchor (same full header, but carries `mode: PARTIAL`)
    # REFUSES — distinguishes a genuine full run from a PARTIAL one sharing the
    # same literal header.
    partial_anchor="$TMPROOT/anchor-partial.txt"
    write_anchor "$partial_anchor" "$f_sha" "$f_digest"
    printf 'mode: PARTIAL (--only clippy) - does NOT count as the gate\n' >>"$partial_anchor"
    run_recert "$fixture" "$partial_anchor" file-size
    if [ "$RC" -eq 2 ] && grep -qE '^error: anchor is an --only PARTIAL run' <<<"$OUT"; then
      ok "R11: an --only-shaped (mode: PARTIAL) anchor REFUSES"
    else
      bad "R11: expected exit 2 + '--only PARTIAL run', got rc=$RC"
    fi

    # R13: --recertify runs its named components in FULL-GATE MODE, so the #3148
    # COMMITTED-SCHEMAS guard must be STRICT for it — there is deliberately no
    # opt-out (roborev finding, High). `--recertify` implements "full-gate mode"
    # by setting ONLY="$RECERT_COMPONENTS" and falling through into the shared
    # flow, so `apply_schemas_preflight`'s leniency test must read the
    # `_gate_is_strict` PREDICATE (which excepts RECERTIFY) and not the bare
    # `[ -n "$ONLY" ] || [ "$LITE" -ne 0 ]` idiom. With the bare idiom every
    # recert took the LENIENT branch: it stamped `schemas: not checked` and
    # never consulted `_gate_schemas_override_reject` at all — while
    # `_schemas_status`, the PURE decision that function's own header says it
    # consumes, was already `_gate_is_strict`-gated. The effectful guard and the
    # pure decision had drifted apart, which is the exact pairing #3148 built
    # them as one to prevent.
    #
    # PLACED BEFORE R12 DELIBERATELY: R12 adds a commit touching
    # cqlite-core/src/**, which both invalidates $anchor and makes every
    # CORE-domain component (core-tests included) diff-touched, so these cases
    # would refuse at check 8 before ever reaching a schemas verdict.
    #
    # `core-tests` rather than `file-size`, because the schemas pre-flight runs
    # ONLY when the selection needs datasets (`selected_needs_datasets`) —
    # file-size does not, so a file-size recert never reaches the guard and
    # could not distinguish the fix from the bug. The fake corpus below is what
    # keeps that cheap: the #2078 corpus guard (already `_gate_is_strict`-gated)
    # runs FIRST and would otherwise FAIL the run before the schemas guard is
    # reached. core-tests itself then FAILs in ~1s (no real Cargo.toml in the
    # fixture), which is irrelevant here — these cases assert the PRE-FLIGHT's
    # schemas verdict, not the component's.
    rc_ds="$TMPROOT/ds"
    rc_keyspace=$(awk -F'"' '/^CANONICAL_FIXTURE_KEYSPACE=/ { print $2; exit }' "$GATE")
    if [ -z "$rc_keyspace" ]; then
      bad "R13: could not read CANONICAL_FIXTURE_KEYSPACE from $GATE — the schemas cases would run against an unrecognized corpus root"
    else
      mkdir -p "$rc_ds/sstables/$rc_keyspace"
      : >"$rc_ds/sstables/$rc_keyspace/na-1-big-Data.db"

      # (a) the AFFIRMATIVE half: the strict guard RAN and says so. A positive
      # line must never be stamped for a check that did not run (#3148), so its
      # presence is the evidence the guard engaged.
      run_recert_env "$fixture" "$anchor" core-tests "CQLITE_DATASETS_ROOT=$rc_ds"
      if grep -qE '^schemas: [0-9]+/[0-9]+ canonical \.cql readable' <<<"$OUT"; then
        ok "R13a: a recert whose selection needs datasets STAMPS the affirmative 'N/N canonical .cql readable' line (the strict #3148 guard ran)"
      else
        bad "R13a: expected an affirmative 'N/N canonical .cql readable' schemas line on a core-tests recert"
        grep -E '^schemas:|^missing-schemas:' <<<"$OUT" || echo '(no schemas line at all)'
      fi
      if ! grep -qE '^schemas: not checked' <<<"$OUT"; then
        ok "R13b: a recert NEVER stamps the lenient 'schemas: not checked' line (--recertify is not a bare --only)"
      else
        bad "R13b: the recert stamped 'schemas: not checked' — apply_schemas_preflight read the bare ONLY idiom instead of _gate_is_strict, so the guard was SKIPPED"
      fi

      # (b) the REFUSAL half: a RELATIVE CQLITE_SCHEMAS_ROOT is the no-opt-out
      # rejection class (the gate resolves it against REPO_ROOT, cargo against
      # each PACKAGE dir, so the two sides would read DIFFERENT roots). Keyed on
      # the CAUSE text, not merely on a non-zero rc: under the bug this run also
      # ends non-zero, but for the unrelated "no Data.db files" reason.
      run_recert_env "$fixture" "$anchor" core-tests \
        "CQLITE_DATASETS_ROOT=$rc_ds" 'CQLITE_SCHEMAS_ROOT=packaged/schemas'
      if [ "$RC" -ne 0 ] \
         && grep -qE '^missing-schemas: FAIL-CLOSED \(#3148\) — relative CQLITE_SCHEMAS_ROOT rejected' <<<"$OUT" \
         && grep -qE '^RESULT: FAIL' <<<"$OUT"; then
        ok "R13c: a recert with a RELATIVE CQLITE_SCHEMAS_ROOT FAILS CLOSED, naming the rejection (#3148 has no --recertify bypass)"
      else
        bad "R13c: expected a FAIL-CLOSED refusal naming the relative-override rejection, got rc=$RC"
        grep -E '^schemas:|^missing-schemas:|^RESULT:' <<<"$OUT" || echo '(no schemas/RESULT line at all)'
      fi
      if ! grep -qE '^recert-verdict: CERTIFIED' <<<"$OUT"; then
        ok "R13d: the rejected-override recert is NOT certified"
      else
        bad "R13d: a recert whose schemas override was REJECTED still stamped recert-verdict: CERTIFIED"
      fi
    fi

    # R12: a component whose domain intersects the PR's own diff REFUSES, while a
    # component OUTSIDE that domain (dep-duplicates — a CARGO_ANY-only domain,
    # excludes cqlite-core/src/**, and SKIPs fast in this minimal fixture with no
    # real Cargo.toml: an "unmeasurable — cargo tree non-zero" state its OWN
    # design treats as SKIP naming the cause, never a hang or a FAIL) is still
    # ACCEPTED against the SAME diff. NOTE: tooling-tests is deliberately NOT
    # used for the "excluded" half here — --recertify reuses --only's dispatch,
    # and --only ALWAYS bypasses #4266's own tooling-tests scoping by design (a
    # recert of tooling-tests must actually EXECUTE it, which is the whole point
    # of recertifying a component that failed), so it would try to run the real
    # ~80-suite here rather than demonstrate domain exclusion cheaply.
    ( cd "$fixture" && echo 'changed' >>cqlite-core/src/lib.rs \
        && git add -A && git commit -qm "touch core src" ) >/dev/null 2>&1
    ident2=$(tree_identity "$fixture")
    f2_sha=$(printf '%s' "$ident2" | awk '{print $1}')
    f2_digest=$(printf '%s' "$ident2" | awk '{print $3}')
    diff_anchor="$TMPROOT/anchor-diff.txt"
    write_anchor "$diff_anchor" "$f2_sha" "$f2_digest"
    run_recert "$fixture" "$diff_anchor" file-size
    if [ "$RC" -eq 2 ] && grep -qE "^error: 'file-size' is in the PR's own diff domain" <<<"$OUT"; then
      ok "R12a: a component (file-size) whose domain intersects a real cqlite-core/src/** diff REFUSES"
    else
      bad "R12a: expected exit 2 + 'file-size' diff-domain refusal, got rc=$RC"
    fi
    # dep-duplicates has NO domain overlap with the diff, so the ELIGIBILITY
    # preflight must ACCEPT it and fall through into the real dispatch — proven
    # by the ABSENCE of a diff-domain refusal AND the presence of its own
    # component row (whatever that row's status: this fixture has no real
    # Cargo.toml or scripts/ci/check-dep-duplicates.sh, so dep-duplicates itself
    # correctly SKIPs once dispatched — a SEPARATE, already-covered property is
    # R1/R6c's "recert-verdict requires exactly PASS", not re-asserted here).
    run_recert "$fixture" "$diff_anchor" dep-duplicates
    if ! grep -qE "^error: 'dep-duplicates' is in the PR's own diff domain" <<<"$OUT" \
       && grep -qE '^dep-duplicates: ' <<<"$OUT"; then
      ok "R12b: dep-duplicates (CARGO_ANY-only domain, excludes cqlite-core/src) is ELIGIBLE against the SAME diff (preflight accepted, dispatched for real)"
    else
      bad "R12b: expected dep-duplicates to be preflight-ELIGIBLE (no diff-domain refusal) against the core-src diff, got rc=$RC"
      echo "------- captured -------"; printf '%s\n' "$OUT"; echo "------------------------"
    fi

  fi
fi

# ==== Layer 3: wiring evidence (structural, cheap) ============================
# W1: acquire_gate_slot's existing `[ -n "$ONLY" ] && return 0` exemption already
# covers RECERTIFY once run_recertify_preflight sets ONLY — assert the function
# body still contains that line (a regression here would silently start queueing
# every recert behind the full-gate slot cap, defeating the whole point).
# The awk output is CAPTURED FIRST and matched from a herestring, never piped
# into `grep -q` (#4268 roborev finding, Medium; W3 below is the ratchet). Under
# `set -o pipefail` a pipe into an early-exiting reader reports the PRODUCER's
# SIGPIPE, so a CORRECT gate file would red intermittently. The repo's
# scripts/ci/check-sigpipe-sites.sh cannot see this shape: it pattern-matches
# `printf`/`echo` writers only, and the writer here is `awk`.
_w1_body=$(awk '/^acquire_gate_slot\(\) \{/{f=1} f{print; if (/^\}/ && NR>1) exit}' "$GATE")
if grep -qE '^acquire_gate_slot\(\) \{' "$GATE" \
   && grep -qE '\[ -n "\$ONLY" \] && return 0' <<<"$_w1_body"; then
  ok "W1: acquire_gate_slot's ONLY-exemption still covers RECERTIFY (it sets ONLY before falling through)"
else
  bad "W1: could not confirm acquire_gate_slot exempts a non-empty \$ONLY"
fi

# W2: run_recertify_preflight is dispatched BEFORE acquire_gate_slot (fail fast,
# never queues for a slot on a bad anchor).
preflight_ln=$(grep -n 'run_recertify_preflight$' "$GATE" | grep -v '^\s*#' | tail -1 | cut -d: -f1)
# Single awk, not `grep -n … | head -1 | cut` — `head -1` is an early-exiting
# reader, the same SIGPIPE-under-pipefail shape W3 forbids.
slot_ln=$(awk '/^acquire_gate_slot$/ { print NR; exit }' "$GATE")
if [ -n "$preflight_ln" ] && [ -n "$slot_ln" ] && [ "$preflight_ln" -lt "$slot_ln" ]; then
  ok "W2: run_recertify_preflight is dispatched before acquire_gate_slot"
else
  bad "W2: could not confirm run_recertify_preflight runs before acquire_gate_slot (preflight_ln=$preflight_ln slot_ln=$slot_ln)"
fi

# W3: NO VERDICT IN THIS SUITE MAY RIDE ON A PIPE INTO AN EARLY-EXITING READER
# (#4268 roborev finding, Medium). Under `set -o pipefail` a `producer | grep -q`
# (or `| head -1`) reports the PRODUCER's SIGPIPE once the consumer matches and
# exits before the producer has finished writing, so a CORRECT subject reds
# intermittently. The construct itself is the defect and its ABSENCE is the
# property, so this is a structural ratchet over this file, not a behavioural
# case.
#
# IT IS NEEDED IN ADDITION TO scripts/ci/check-sigpipe-sites.sh, not instead of
# it: that ratchet pattern-matches `printf`/`echo` WRITERS only, so it was blind
# to W1's `awk … | grep -qE` (the instance this case was written for) and to the
# three `… | head -1` sites elsewhere in this file. Widening that guard's writer
# set is a separate concern and deliberately NOT attempted here.
#
# The needles are ASSEMBLED FROM PARTS so this guard cannot match its own lines,
# and `code` mode drops whole-line comments so the prose above is not a subject.
# assert_src_absent_fixed is THREE-VALUED: an unreadable subject reports
# UNMEASURED rather than certifying the property (#3752).
_w3_grep='| grep -'
_w3_head='| head -'
assert_src_absent_fixed \
  "W3: no verdict in this suite is derived from a pipe into an early-exiting grep" \
  "W3: a pipe into an early-exiting grep is back — under pipefail such a pipeline reports the PRODUCER's SIGPIPE, so a CORRECT subject reds intermittently (capture into a variable and match with a herestring instead)" \
  "${BASH_SOURCE[0]}" "${_w3_grep}q" code
assert_src_absent_fixed \
  "W3: no verdict in this suite is derived from a pipe into an early-exiting head" \
  "W3: a pipe into 'head -1' is back — same SIGPIPE-under-pipefail hazard as the grep form (use a single awk with 'exit', or capture and match from a herestring)" \
  "${BASH_SOURCE[0]}" "${_w3_head}1" code

printf '\n%s\n' "recertify: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
