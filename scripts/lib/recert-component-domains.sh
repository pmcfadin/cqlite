#!/usr/bin/env bash
# scripts/lib/recert-component-domains.sh — issue #4268
#
# `--recertify` reruns up to 2 host-failed components against a same-digest full
# anchor, but it must REFUSE to recertify a component whose failure the PR's own
# diff could plausibly have CAUSED — that is a code failure, and it needs a full
# gate, not a host-fault workaround (#4268's explicit limit).
#
# This is a BEST-EFFORT, COARSE, PATH-PREFIX classifier — declared as such rather
# than implied otherwise. It answers one question: "does this component's domain
# overlap the changed-path set?" It cannot know whether a change actually affects
# a component's behavior, only whether it is PLAUSIBLE. A component with no entry
# below is fail-closed to "always diff-touched" (see
# _recert_component_domain_patterns' default case) — never silently permissive.
#
# ONE DECLARED PLACE (mirrors #4266's tooling-tests-scope.sh): every component in
# agent-gate.sh's COMPONENTS array should have an entry here, pinned by a
# completeness census in scripts/tests/test_recertify.sh so a newly added gate
# component does not silently inherit an unreviewed domain (it fails closed
# either way, but the census keeps the table honest).
#
# Domains reuse forward-slash `case` globs (`*` matches `/`, same as
# scripts/lib/tooling-tests-scope.sh). CASE PATTERNS ONLY, NEVER A BARE
# EXPANSION: every domain group below is a bash ARRAY, and every use is a
# QUOTED `"${arr[@]}"` expansion — an UNQUOTED `$var` holding a glob character
# (e.g. `*.rs`) undergoes real pathname expansion against the CALLER's current
# directory before `printf`/`case` ever sees it, silently replacing the pattern
# with whatever files happen to match it on disk. A first cut of this file did
# exactly that (`printf '%s\n' $_RECERT_DOM_RUST_ANY`), caught by
# scripts/tests/test_recertify.sh's very first real classify case.

# Shared domain groups. Each is an ARRAY, even a single-pattern one, so every
# consumer can use the SAME `"${name[@]}"` expansion uniformly.
#
# _RECERT_DOM_BASE (roborev finding, Medium): every MAPPED component's own
# driver — `run_<component>` — lives in scripts/agent-gate.sh, and this
# table mapped only the PRODUCT paths a component reads, never the HARNESS
# path that IMPLEMENTS it. A PR editing `run_clippy` (or any other driver
# function) changed nothing under `cqlite-core/**`/`Cargo.*`/etc., so it
# classified as NOT diff-touched for every affected component — exactly the
# "a failure there may be a code defect, not a host fault" case check 8
# exists to refuse. Prepended to every MAPPED component's domain (never to
# the unmapped default, which must stay EMPTY — see
# _recert_component_domain_patterns below). This is still coarse (a
# component-specific `scripts/ci/<guard>.sh` is added per-component below
# where one exists; a bare change to a SIBLING component's driver inside the
# same file still counts as touching THIS one, which is the safe direction to
# be imprecise in).
_RECERT_DOM_BASE=('scripts/agent-gate.sh')
# _RECERT_DOM_TOOLCHAIN (#4268 roborev finding, Medium): the repo-root build
# CONFIGURATION every Rust-executing component's behavior is parameterised by.
# The table mapped PRODUCT paths (and, since _RECERT_DOM_BASE, the driver), but
# none of these — so a PR whose whole diff was `.clippy.toml` (or a Rust-version
# bump in `rust-toolchain.toml`, or a nextest profile change) classified as NOT
# diff-touched for clippy, fmt, core-tests and every other MAPPED component, and
# `--recertify` would have accepted a host-fault recert of exactly the component
# whose behavior that diff changes. That is the fail-OPEN direction.
#
# The file header's "a component with no entry is fail-closed to 'always
# diff-touched'" default does NOT cover this: it applies only to components that
# are wholly UNMAPPED. A MAPPED component gets precisely the patterns listed for
# it, so an unlisted path is CLEAR for it — fail-open, not fail-closed.
#
# PREPENDED TO EVERY MAPPED COMPONENT'S DOMAIN, exactly as _RECERT_DOM_BASE is
# (same site, same reason): enumerating it per-arm would leave the next arm to
# the next person. Coarse in the safe direction — a nextest profile change
# counts as touching `kit-dashboard-drift` too, which only ever REFUSES a recert
# that might have been eligible.
#
# `.cargo/*` and `.config/*` are DIRECTORY GLOBS, not single named files, and
# deliberately so. Both directories exist only to hold build/test configuration
# that is read implicitly by cargo and its subcommands (`.cargo/config.toml`'s
# rustflags/target/registry settings; `.config/nextest.toml`'s profiles), so a
# change to ANY entry under them is a plausible cause of a component's failure.
# Naming today's files instead would silently fail OPEN the moment a second one
# is added — the exact per-arm-omission shape this group exists to close — and
# the cost of the glob is only ever an extra REFUSED recert, never an accepted
# one. (Forward-slash `case` globs: `*` matches `/` here, see the file header.)
_RECERT_DOM_TOOLCHAIN=('rust-toolchain.toml' '.clippy.toml' '.rustfmt.toml' '.cargo/*' '.config/*')
_RECERT_DOM_RUST_ANY=('*.rs')
_RECERT_DOM_CARGO_ANY=('Cargo.toml' 'Cargo.lock' '*/Cargo.toml')
_RECERT_DOM_CORE=('cqlite-core/*')
_RECERT_DOM_CLI=('cqlite-cli/*')
_RECERT_DOM_PY=('bindings/python/*')
_RECERT_DOM_NODE=('bindings/node/*')
_RECERT_DOM_BINDINGS_ANY=('bindings/*')
_RECERT_DOM_FLIGHT=('cqlite-flight/*')
_RECERT_DOM_TOOLS=('tools/*')
_RECERT_DOM_TESTDATA=('test-data/*')
_RECERT_DOM_DOCS_REPORTS=('docs/reports/*')
_RECERT_DOM_DOCS_ANY=('docs/*')
_RECERT_DOM_XTASK=('xtask/*')
_RECERT_DOM_LABKITS=('easy-db-lab-kits/*')
# #4268 roborev finding, Medium (job 117): cqlite-ffi-common is a workspace
# member both bindings/python/Cargo.toml and bindings/node/Cargo.toml depend
# on by path, but appeared in NO domain — a diff touching only it classified
# CLEAR for python-bindings/node-bindings/binding-rust-tests|binding-unwind-
# profile, the same fail-open class as the already-fixed oom-audit->xtask/*,
# memory-budget->cqlite-flight/* and integration-tests->tests/* arms.
_RECERT_DOM_FFI_COMMON=('cqlite-ffi-common/*')

# The #4266 declared harness set, reused verbatim rather than retyped (this file
# is sourced alongside scripts/lib/tooling-tests-scope.sh by agent-gate.sh, so
# the array is already in scope when both are loaded; fall back to a literal
# copy if sourced standalone, e.g. by a test, without that file).
_recert_harness_patterns() {
  # `declare -p` (rather than a direct `${#TOOLING_TESTS_SCOPE_PATTERNS[@]}`
  # expansion) is the set -u-SAFE existence check: bash treats a length
  # expansion on a name that was never declared at all — not even as an empty
  # array — as an unset-parameter reference under `set -u`, so a caller that
  # sources this file standalone (e.g. a self-test, or a future consumer that
  # never loads scripts/lib/tooling-tests-scope.sh) would die here instead of
  # falling through to the literal fallback below.
  if declare -p TOOLING_TESTS_SCOPE_PATTERNS >/dev/null 2>&1 \
     && [ "${#TOOLING_TESTS_SCOPE_PATTERNS[@]}" -gt 0 ]; then
    printf '%s\n' "${TOOLING_TESTS_SCOPE_PATTERNS[@]}"
    return 0
  fi
  # FAIL CLOSED (roborev finding, Medium) — NO second, hand-maintained copy of
  # the #4266 declared set. An earlier cut carried a "literal copy" fallback
  # here that silently drifted 7 patterns behind the real (19-entry) array —
  # `.gitignore`, `CLAUDE.md`, `docs/*`, several `test-data/*` classes,
  # `tools/*`, `cqlite-flight/Dockerfile` and `bindings/node/__test__/*` were
  # all missing from it — so a diff touching only those paths classified
  # `tooling-tests` as CLEAR: fail-OPEN, the one direction this file must
  # never take. This should not happen in production (agent-gate.sh sources
  # tooling-tests-scope.sh before this file — see both files' headers), but a
  # standalone caller (a future consumer, a test) that skips it now gets an
  # honest UNRECOGNIZED rather than a second source of truth to keep in sync.
  # rc 1, no output: the caller (_recert_component_domain_patterns_raw's
  # tooling-tests arm) propagates this exit status directly, so
  # _recert_component_domain_patterns then treats tooling-tests as
  # UNRECOGNIZED and its existing "no domain -> always diff-touched" default
  # applies — never a partial, silently-permissive domain.
  return 1
}

# _recert_component_domain_patterns <component>: print the component's domain
# patterns, one per line — ALWAYS including the two SHARED groups
# (_RECERT_DOM_BASE, the component's own driver file, and _RECERT_DOM_TOOLCHAIN,
# the repo-root build configuration) for a RECOGNIZED component, plus that
# component's specific product/guard paths. Prints NOTHING for an UNRECOGNIZED
# component — the caller (_recert_component_diff_touched) treats "no domain" as
# "match everything" (the fail-closed default), never as "match nothing".
# Delegates the actual per-component list to
# _recert_component_domain_patterns_raw and uses ITS EXIT STATUS (0 =
# recognized, 1 = not) to decide whether to prepend the shared groups — printing
# them unconditionally would give an UNRECOGNIZED component a non-empty (so
# "sometimes touched" instead of "ALWAYS touched") domain, silently weakening
# the fail-closed default.
_recert_component_domain_patterns() {
  local _rdp_specific _rdp_rc
  _rdp_specific=$(_recert_component_domain_patterns_raw "$1"); _rdp_rc=$?
  [ "$_rdp_rc" -eq 0 ] || return 0
  printf '%s\n' "${_RECERT_DOM_BASE[@]}" "${_RECERT_DOM_TOOLCHAIN[@]}"
  # Explicit (roborev finding, Low): without this, the function's own exit
  # status is the `[ -n ... ]` test above, which is FALSE whenever a
  # recognized component's specific list happens to be empty — even though
  # the shared groups were just printed. No current caller reads the rc, but
  # a future `if _recert_component_domain_patterns "$c" >/dev/null; then`
  # (or any `set -e` caller) would silently misread a recognized component
  # as unrecognized.
  [ -n "$_rdp_specific" ] && printf '%s\n' "$_rdp_specific"
  return 0
}

# _recert_component_domain_patterns_raw <component>: the per-component product/
# guard paths ALONE (no base) — rc 0 + the list for a recognized component, rc
# 1 + nothing for an unrecognized one. Every arm expands its groups with a
# QUOTED `"${name[@]}"` — see the file header for why that is load-bearing.
_recert_component_domain_patterns_raw() {
  case "$1" in
    # CARGO_ANY added (job 112 roborev finding, Medium): fmt is class `cargo`
    # and `cargo fmt --all --check`'s subject set is the workspace member
    # list in Cargo.toml — a PR adding a member was CLEAR for it. Over-includes
    # for file-size (no-cargo), the safe direction.
    file-size|fmt)
      printf '%s\n' "${_RECERT_DOM_RUST_ANY[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    clippy)
      printf '%s\n' "${_RECERT_DOM_RUST_ANY[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    roborev-lints)
      printf '%s\n' "${_RECERT_DOM_RUST_ANY[@]}" '.github/*' 'scripts/*' ;;
    # test-data/* (job 110 roborev finding, Medium): added for every member of
    # this arm that DATASET_COMPONENTS (scripts/agent-gate.sh) lists as a real
    # corpus reader — core-tests, tombstones-scan, scan-offload-guard,
    # work-counters-guard, legacy-heuristics, feature-iso-delta-scan,
    # write-tests (memory-budget is also a member, but split into its own arm
    # below, job 112). byte-budget-guard, arrow-parity-guard,
    # feature-iso-parquet, compaction-byte-parity and all-features-check are
    # NOT in DATASET_COMPONENTS, so this over-includes for them — the safe
    # direction this file already takes elsewhere (only ever REFUSES an
    # eligible recert, never admits an ineligible one). bti-multiclustering
    # is split into its own arm below (job 119).
    core-tests|tombstones-scan|scan-offload-guard|work-counters-guard|byte-budget-guard|arrow-parity-guard|legacy-heuristics|feature-iso-parquet|feature-iso-delta-scan|compaction-byte-parity|write-tests|all-features-check)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" ;;
    # memory-budget (job 112 roborev finding, Medium): split out of the shared
    # arm above — its Flight lane runs `cargo test --package cqlite-flight
    # --features dhat-heap`, so a cqlite-flight/src/**-only diff was CLEAR.
    memory-budget)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" "${_RECERT_DOM_FLIGHT[@]}" ;;
    # bti-multiclustering (job 119 roborev finding, Medium): split out of the
    # shared arm above — it runs two dedicated fail-closed control harnesses
    # (scripts/agent-gate.sh:17830-17831), so a diff touching only those two
    # scripts was CLEAR for it.
    bti-multiclustering)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" \
        'scripts/tests/test_point_vs_full_failclosed.sh' 'scripts/tests/test_issue_3358_failclosed.sh' ;;
    # oom-audit (job 106 roborev finding, Medium): split out of the shared
    # core/cargo arm above — it runs `cargo run -p xtask -- oom-audit
    # --enforce` (scripts/agent-gate.sh), so xtask/* is a real subject the
    # shared arm never named.
    oom-audit)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_XTASK[@]}" ;;
    pub-surface)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" 'scripts/ci/check-pub-surface.sh' ;;
    # integration-tests (job 106 roborev finding, Medium): 'cqlite-integration-tests/*'
    # is the cargo PACKAGE name, not a repo path — that crate lives at tests/
    # (tests/Cargo.toml declares `name = "cqlite-integration-tests"`), so the old
    # pattern matched nothing in the tree and left this component fail-open.
    # integration-tests also DATASET_COMPONENTS-mapped (job 110 Medium) — see
    # the test-data/* comment on the shared core arm above.
    integration-tests)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" 'tests/*' "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" ;;
    # format-compat (job 106 roborev finding, Medium): the component runs
    # `cargo test --package format-compatibility-tests`, whose sources are
    # tests/format-compatibility/** — 'tools/format-validator/*' is an
    # unrelated crate and was never a real subject of this component.
    # CARGO_ANY added (job 110 Medium): this arm ran cargo but never carried it.
    format-compat)
      printf '%s\n' 'tests/format-compatibility/*' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    cli-tests|smoke)
      printf '%s\n' "${_RECERT_DOM_CLI[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_TESTDATA[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    query-semantics-oracle)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" 'test-data/query-semantics-oracle.json' "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    # flight-tests is DATASET_COMPONENTS-mapped; flight-query-semantics-oracle
    # is not, but shares this arm — over-including TESTDATA for it is the safe
    # direction (job 110 Medium, both findings: CARGO_ANY + test-data/* gaps).
    flight-query-semantics-oracle|flight-tests)
      printf '%s\n' "${_RECERT_DOM_FLIGHT[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" ;;
    # python-bindings (job 110 Medium): maturin builds the extension via an
    # INDIRECT cargo invocation (scripts/agent-gate.sh's _fm_component_class:
    # 'indirect:maturin') and is DATASET_COMPONENTS-mapped (pytest reads the
    # corpus) — both CARGO_ANY and test-data/* were missing. cqlite-ffi-common
    # added (job 117 Medium): bindings/python/Cargo.toml depends on it by path.
    python-bindings)
      printf '%s\n' "${_RECERT_DOM_PY[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" "${_RECERT_DOM_FFI_COMMON[@]}" ;;
    # node-bindings (job 110 Medium): napi's `npm run build` invokes cargo
    # build INDIRECTLY ('indirect:npm run build (napi)') and is
    # DATASET_COMPONENTS-mapped — same two gaps as python-bindings.
    # cqlite-ffi-common added (job 117 Medium): bindings/node/Cargo.toml
    # depends on it by path too.
    node-bindings)
      printf '%s\n' "${_RECERT_DOM_NODE[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_TESTDATA[@]}" "${_RECERT_DOM_FFI_COMMON[@]}" ;;
    # binding-rust-tests runs cargo directly; binding-unwind-profile does not
    # (_fm_component_class: no-cargo) but shares this arm — over-including is
    # the safe direction (job 110 Medium). cqlite-ffi-common added (job 117
    # Medium): binding-rust-tests runs `test cqlite-ffi-common default-features`
    # directly — the clearest case of all three arms. binding-unwind-profile
    # IS scripts/tests/test_binding_unwind_profile.sh (scripts/agent-gate.sh
    # dispatch), added job 119 Medium — a diff touching only that script was
    # CLEAR for it.
    binding-rust-tests|binding-unwind-profile)
      printf '%s\n' "${_RECERT_DOM_BINDINGS_ANY[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_FFI_COMMON[@]}" \
        'scripts/tests/test_binding_unwind_profile.sh' ;;
    # test_delivery_telemetry*.py added (job 112 roborev finding, Medium): the
    # component runs EVERY scripts/tests/test_delivery_telemetry*.py module
    # (test_delivery_telemetry.py AND test_delivery_telemetry_timeline.py) —
    # a PR editing its own test modules was CLEAR for it.
    delivery-telemetry)
      printf '%s\n' 'scripts/delivery-telemetry.py' 'docs/reports/delivery-telemetry.jsonl' \
        'scripts/tests/test_delivery_telemetry*' ;;
    # CARGO_ANY added (job 112 Medium): runs `cargo run -q -p cassandra-parity`
    # — a root Cargo.lock/Cargo.toml dependency change was CLEAR for it
    # (tools/* only covers tools/cassandra-parity/Cargo.toml).
    parity-report)
      printf '%s\n' "${_RECERT_DOM_TOOLS[@]}" "${_RECERT_DOM_TESTDATA[@]}" "${_RECERT_DOM_DOCS_REPORTS[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    # operator-metrics-doc (job 110 Medium): runs `cargo run -q -p cqlite-core
    # --example gen_operator_metrics_doc` — CARGO_ANY was missing.
    operator-metrics-doc)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_DOCS_ANY[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    # kit-dashboard-drift (job 106 roborev finding, Medium): it reads
    # easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json and runs
    # `cargo test -p cqlite-core --test kit_dashboard_metric_drift`
    # (scripts/agent-gate.sh) — docs/* and website/* are not subjects at all;
    # the real ones are the kit subtree and cqlite-core.
    # CARGO_ANY added (job 110 Medium): runs `cargo test -p cqlite-core --test
    # kit_dashboard_metric_drift`, same gap as the job-106 path fix above.
    kit-dashboard-drift)
      printf '%s\n' "${_RECERT_DOM_LABKITS[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    dep-duplicates)
      printf '%s\n' "${_RECERT_DOM_CARGO_ANY[@]}" 'scripts/ci/check-dep-duplicates.sh' 'scripts/ci/dep-duplicates-baseline.txt' ;;
    features-load-bearing)
      printf '%s\n' "${_RECERT_DOM_CARGO_ANY[@]}" "${_RECERT_DOM_RUST_ANY[@]}" 'scripts/ci/check-features-load-bearing.sh' ;;
    tooling-tests)
      # Propagate _recert_harness_patterns's OWN exit status directly (never
      # fall through to this function's shared `return 0` below) — its
      # fail-closed rc 1 must make THIS arm, and therefore the caller, see
      # tooling-tests as unrecognized too (see that function's own comment).
      _recert_harness_patterns
      return $? ;;
    minimal-build)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    *) return 1 ;;   # unrecognized: caller (_recert_component_domain_patterns)
                      # prints nothing at all -> _recert_component_diff_touched's
                      # fail-closed "matches everything" default applies.
  esac
  return 0
}

# _recert_component_diff_touched <component> <changed-paths-newline-list>: true
# (0) iff <component>'s domain intersects the changed-path set, OR the component
# has no domain entry (fail-closed default). False (1) only when the component
# HAS a domain and NONE of the changed paths match it. The `case "$f" in $pat)`
# below is SAFE unquoted — case patterns undergo parameter expansion but NEVER
# pathname expansion against the filesystem, unlike a bare command-argument
# expansion (see the file header).
_recert_component_diff_touched() {
  local comp="$1" changed="$2" domain f pat
  domain=$(_recert_component_domain_patterns "$comp")
  if [ -z "$domain" ]; then
    return 0   # unmapped -> always diff-touched (fail-closed)
  fi
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    while IFS= read -r pat; do
      [ -n "$pat" ] || continue
      case "$f" in
        $pat) return 0 ;;
      esac
    done <<<"$domain"
  done <<<"$changed"
  return 1
}
