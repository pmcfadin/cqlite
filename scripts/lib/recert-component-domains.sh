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
_RECERT_DOM_WEBSITE=('website/*')

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
# patterns, one per line — ALWAYS including _RECERT_DOM_BASE (the component's
# own driver file) for a RECOGNIZED component, plus that component's specific
# product/guard paths. Prints NOTHING for an UNRECOGNIZED component — the
# caller (_recert_component_diff_touched) treats "no domain" as "match
# everything" (the fail-closed default), never as "match nothing". Delegates
# the actual per-component list to _recert_component_domain_patterns_raw and
# uses ITS EXIT STATUS (0 = recognized, 1 = not) to decide whether to prepend
# the base — printing the base unconditionally would give an UNRECOGNIZED
# component a non-empty (so "sometimes touched" instead of "ALWAYS touched")
# domain, silently weakening the fail-closed default.
_recert_component_domain_patterns() {
  local _rdp_specific _rdp_rc
  _rdp_specific=$(_recert_component_domain_patterns_raw "$1"); _rdp_rc=$?
  [ "$_rdp_rc" -eq 0 ] || return 0
  printf '%s\n' "${_RECERT_DOM_BASE[@]}"
  [ -n "$_rdp_specific" ] && printf '%s\n' "$_rdp_specific"
}

# _recert_component_domain_patterns_raw <component>: the per-component product/
# guard paths ALONE (no base) — rc 0 + the list for a recognized component, rc
# 1 + nothing for an unrecognized one. Every arm expands its groups with a
# QUOTED `"${name[@]}"` — see the file header for why that is load-bearing.
_recert_component_domain_patterns_raw() {
  case "$1" in
    file-size|fmt)
      printf '%s\n' "${_RECERT_DOM_RUST_ANY[@]}" ;;
    clippy)
      printf '%s\n' "${_RECERT_DOM_RUST_ANY[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    roborev-lints)
      printf '%s\n' "${_RECERT_DOM_RUST_ANY[@]}" '.github/*' 'scripts/*' ;;
    core-tests|tombstones-scan|scan-offload-guard|work-counters-guard|byte-budget-guard|arrow-parity-guard|memory-budget|legacy-heuristics|feature-iso-parquet|feature-iso-delta-scan|compaction-byte-parity|bti-multiclustering|write-tests|oom-audit|all-features-check)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    pub-surface)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_CARGO_ANY[@]}" 'scripts/ci/check-pub-surface.sh' ;;
    integration-tests)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" 'cqlite-integration-tests/*' "${_RECERT_DOM_CARGO_ANY[@]}" ;;
    format-compat)
      printf '%s\n' 'tools/format-validator/*' "${_RECERT_DOM_CORE[@]}" ;;
    cli-tests|smoke)
      printf '%s\n' "${_RECERT_DOM_CLI[@]}" "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_TESTDATA[@]}" ;;
    query-semantics-oracle)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" 'test-data/query-semantics-oracle.json' ;;
    flight-query-semantics-oracle|flight-tests)
      printf '%s\n' "${_RECERT_DOM_FLIGHT[@]}" "${_RECERT_DOM_CORE[@]}" ;;
    python-bindings)
      printf '%s\n' "${_RECERT_DOM_PY[@]}" "${_RECERT_DOM_CORE[@]}" ;;
    node-bindings)
      printf '%s\n' "${_RECERT_DOM_NODE[@]}" "${_RECERT_DOM_CORE[@]}" ;;
    binding-rust-tests|binding-unwind-profile)
      printf '%s\n' "${_RECERT_DOM_BINDINGS_ANY[@]}" "${_RECERT_DOM_CORE[@]}" ;;
    delivery-telemetry)
      printf '%s\n' 'scripts/delivery-telemetry.py' 'docs/reports/delivery-telemetry.jsonl' ;;
    parity-report)
      printf '%s\n' "${_RECERT_DOM_TOOLS[@]}" "${_RECERT_DOM_TESTDATA[@]}" "${_RECERT_DOM_DOCS_REPORTS[@]}" ;;
    operator-metrics-doc)
      printf '%s\n' "${_RECERT_DOM_CORE[@]}" "${_RECERT_DOM_DOCS_ANY[@]}" ;;
    kit-dashboard-drift)
      printf '%s\n' "${_RECERT_DOM_DOCS_ANY[@]}" "${_RECERT_DOM_WEBSITE[@]}" ;;
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
