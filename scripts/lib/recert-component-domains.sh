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
# scripts/lib/tooling-tests-scope.sh).

# Shared domain groups, so a broad "this touches core" fact is spelled once.
_RECERT_DOM_RUST_ANY='*.rs'
_RECERT_DOM_CARGO_ANY='Cargo.toml Cargo.lock */Cargo.toml'
_RECERT_DOM_CORE='cqlite-core/*'
_RECERT_DOM_CLI='cqlite-cli/*'
_RECERT_DOM_PY='bindings/python/*'
_RECERT_DOM_NODE='bindings/node/*'
_RECERT_DOM_BINDINGS_ANY='bindings/*'
_RECERT_DOM_FLIGHT='cqlite-flight/*'
_RECERT_DOM_TOOLS='tools/*'
_RECERT_DOM_TESTDATA='test-data/*'
_RECERT_DOM_DOCS_REPORTS='docs/reports/*'
_RECERT_DOM_DOCS_ANY='docs/*'
_RECERT_DOM_WEBSITE='website/*'
# The #4266 declared harness set, reused verbatim rather than retyped (this file is
# sourced alongside scripts/lib/tooling-tests-scope.sh by agent-gate.sh, so the
# array is already in scope when both are loaded; fall back to a literal copy if
# sourced standalone, e.g. by a test, without that file).
_recert_harness_patterns() {
  if [ "${#TOOLING_TESTS_SCOPE_PATTERNS[@]}" -gt 0 ]; then
    printf '%s\n' "${TOOLING_TESTS_SCOPE_PATTERNS[@]}"
  else
    printf '%s\n' 'scripts/*' '.github/*' '.claude/*' '.roborev.toml' \
      'rust-toolchain.toml' 'Cargo.lock' 'Cargo.toml'
  fi
}

# _recert_component_domain_patterns <component>: print the component's domain
# patterns, one per line. Prints NOTHING for an unmapped component — the caller
# (_recert_component_diff_touched) treats "no domain" as "match everything" (the
# fail-closed default), never as "match nothing".
_recert_component_domain_patterns() {
  case "$1" in
    file-size|fmt)                       printf '%s\n' $_RECERT_DOM_RUST_ANY ;;
    clippy)                               printf '%s\n' $_RECERT_DOM_RUST_ANY $_RECERT_DOM_CARGO_ANY ;;
    roborev-lints)                        printf '%s\n' $_RECERT_DOM_RUST_ANY '.github/*' 'scripts/*' ;;
    core-tests|tombstones-scan|scan-offload-guard|work-counters-guard|byte-budget-guard|arrow-parity-guard|memory-budget|legacy-heuristics|feature-iso-parquet|feature-iso-delta-scan|compaction-byte-parity|bti-multiclustering|write-tests|oom-audit|all-features-check|pub-surface)
                                           printf '%s\n' $_RECERT_DOM_CORE $_RECERT_DOM_CARGO_ANY ;;
    integration-tests)                     printf '%s\n' $_RECERT_DOM_CORE 'cqlite-integration-tests/*' $_RECERT_DOM_CARGO_ANY ;;
    format-compat)                         printf '%s\n' 'tools/format-validator/*' $_RECERT_DOM_CORE ;;
    cli-tests|smoke)                       printf '%s\n' $_RECERT_DOM_CLI $_RECERT_DOM_CORE $_RECERT_DOM_TESTDATA ;;
    query-semantics-oracle)                printf '%s\n' $_RECERT_DOM_CORE 'test-data/query-semantics-oracle.json' ;;
    flight-query-semantics-oracle|flight-tests) printf '%s\n' $_RECERT_DOM_FLIGHT $_RECERT_DOM_CORE ;;
    python-bindings)                       printf '%s\n' $_RECERT_DOM_PY $_RECERT_DOM_CORE ;;
    node-bindings)                         printf '%s\n' $_RECERT_DOM_NODE $_RECERT_DOM_CORE ;;
    binding-rust-tests|binding-unwind-profile)
                                           printf '%s\n' $_RECERT_DOM_BINDINGS_ANY $_RECERT_DOM_CORE ;;
    delivery-telemetry)                    printf '%s\n' 'scripts/delivery-telemetry.py' 'docs/reports/delivery-telemetry.jsonl' ;;
    parity-report)                         printf '%s\n' $_RECERT_DOM_TOOLS $_RECERT_DOM_TESTDATA $_RECERT_DOM_DOCS_REPORTS ;;
    operator-metrics-doc)                  printf '%s\n' $_RECERT_DOM_CORE $_RECERT_DOM_DOCS_ANY ;;
    kit-dashboard-drift)                   printf '%s\n' $_RECERT_DOM_DOCS_ANY $_RECERT_DOM_WEBSITE ;;
    dep-duplicates)                        printf '%s\n' $_RECERT_DOM_CARGO_ANY ;;
    features-load-bearing)                 printf '%s\n' $_RECERT_DOM_CARGO_ANY $_RECERT_DOM_RUST_ANY ;;
    tooling-tests)                         _recert_harness_patterns ;;
    minimal-build)                         printf '%s\n' $_RECERT_DOM_CORE $_RECERT_DOM_CARGO_ANY ;;
    *) return 0 ;;   # unmapped: caller treats as "matches everything" (fail-closed)
  esac
}

# _recert_component_diff_touched <component> <changed-paths-newline-list>: true
# (0) iff <component>'s domain intersects the changed-path set, OR the component
# has no domain entry (fail-closed default). False (1) only when the component
# HAS a domain and NONE of the changed paths match it.
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
