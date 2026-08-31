#!/usr/bin/env bash
# gate-feature-matrix.sh — SUMMARY feature-matrix annotation (issue #3453).
#
# OWNER RULING (2026-08-30): "the gate SUMMARY should name the feature matrix each
# component ran so a pasted block states what it certified."
#
# A pasted `==== AGENT-GATE SUMMARY ====` block used to say `core-tests: PASS (412s)`
# and nothing about WHAT was compiled — so the block could not distinguish a run that
# certified the OTLP stack from one that never enabled it (the #3453 defect: 220
# cqlite-core --lib tests execute in pr-gate-core's `--all-features` lane and NOWHERE
# in the gate of record). Every component line now carries the feature matrix it
# ACTUALLY ran under, in every mode (full / lite / delta).
#
# DERIVE, NEVER CURATE — the two mechanisms, and why there are two:
#
#   (1) OBSERVED (the default, and the whole reason this is not a hand-written table).
#       `cargo` and `env` are shell FUNCTIONS here. Every cargo invocation made in the
#       gate's OWN shell therefore routes through _fm_observe_cargo_argv, which reads
#       the package/feature flags out of the REAL argv that is about to execute and
#       appends one descriptor line to $AGENT_GATE_FM_DIR/<component>.features. There
#       is no second copy of a feature list to drift from: the annotation IS the argv.
#       (`env` is wrapped because run_clippy — which #3453 must not touch — invokes
#       `env RUSTFLAGS="-D warnings" cargo clippy …`, and an `env` prefix execs the
#       cargo BINARY, bypassing a `cargo` function.)
#
#   (2) DECLARED-FROM-THE-SAME-VARIABLE, for the six components whose cargo calls live
#       inside a single-quoted `bash -c` body (cli-tests, memory-budget, write-tests,
#       integration-tests, minimal-build, smoke). The functions above are deliberately
#       NOT `export -f`-ed: exporting them would make every bash DESCENDANT record too,
#       so tooling-tests (which runs ~80 nested test scripts, several of them nested
#       agent-gate self-tests) would attribute a nested run's cargo invocations to
#       itself — a false claim in a gate log, which is worse than none. Those six
#       components hoist their feature list into ONE shell variable that is expanded
#       BOTH into the `bash -c` argv AND into _fm_note, so drift is still structurally
#       impossible.
#
# FAIL CLOSED / DECLARE, NEVER A SILENT BLANK. Every component renders SOMETHING:
# the observed sets, or an explicit `no-cargo`, or an explicit `via <driver>: feature
# set NOT observed`, or `UNDECLARED`. A blank annotation is the vacuous-pass shape this
# issue exists to remove, so there is no code path that produces one.
# _fm_component_class is the declaration site, and
# scripts/tests/test_agent_gate_feature_matrix_annotation.sh asserts that EVERY name in
# agent-gate.sh's COMPONENTS array resolves there — a NEW component cannot join the set
# undeclared.
#
# OBSERVATION BEATS DECLARATION. If a component declared `no-cargo` is observed running
# cargo, the observed sets are rendered WITH a `!declared-no-cargo` marker rather than
# the declaration being believed — a mis-declaration self-corrects and is visible.

# _fm_active: recording is enabled only inside a component (AGENT_GATE_FM_COMPONENT is
# set by dispatch_component / run_component). Cargo invocations from the preflight —
# `cargo --version` for ci-pins, the `cargo tree`/`cargo metadata` probes — belong to no
# component and are deliberately unrecorded.
_fm_active() {
  [ -n "${AGENT_GATE_FM_DIR:-}" ] && [ -n "${AGENT_GATE_FM_COMPONENT:-}" ] \
    && [ -d "${AGENT_GATE_FM_DIR:-/nonexistent}" ]
}

_fm_sidecar() { printf '%s/%s.features' "${AGENT_GATE_FM_DIR:-}" "$1"; }

# _fm_note <component> <descriptor>: append one invocation descriptor. Best-effort by
# design — a failed append must never fail the component whose matrix it describes (the
# consequence of a lost append is a visibly incomplete annotation, never a wrong one).
_fm_note() {
  local f
  f=$(_fm_sidecar "$1") || return 0
  printf '%s\n' "$2" >> "$f" 2>/dev/null || true
  return 0
}

# _fm_abbrev_features <csv>: render a feature list at bounded width. Up to 5 features
# print in full; beyond that the count leads and the remainder is named as elided
# ("+N more"), never silently truncated — an abbreviation must not imply a completeness
# it does not have.
_fm_abbrev_features() {
  local csv="$1" n first
  csv=$(printf '%s' "$csv" | tr ' ' ',' | tr -s ',' | sed 's/^,//; s/,$//')
  [ -n "$csv" ] || { printf ''; return 0; }
  n=$(printf '%s' "$csv" | tr ',' '\n' | grep -c .)
  if [ "$n" -le 5 ]; then
    printf '%s' "$csv"
  else
    first=$(printf '%s' "$csv" | cut -d, -f1-3)
    printf '%s:%s,+%s more' "$n" "$first" "$(( n - 3 ))"
  fi
}

# _fm_describe_cargo <argv…>: print a one-line descriptor of a cargo invocation, or
# return 1 for an invocation that compiles/runs nothing (a metadata query: tree,
# metadata, locate-project, --version). $1.. is the argv AFTER the `cargo` word.
_fm_describe_cargo() {
  local sub="" feats="" nodef=0 allf=0 ws=0 excl=0 tok scope featpart
  local -a pkgs=()
  for tok in "$@"; do
    case "$tok" in
      +*|-*) continue ;;
      *) sub="$tok"; break ;;
    esac
  done
  case "$sub" in
    test|build|check|clippy|run|bench|fmt|doc|nextest|rustc) ;;
    *) return 1 ;;
  esac
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -p|--package) pkgs+=("${2:-?}"); shift ;;
      --package=*)  pkgs+=("${1#*=}") ;;
      -p=*)         pkgs+=("${1#*=}") ;;
      --features)   feats="${feats:+$feats,}${2:-}"; shift ;;
      --features=*) feats="${feats:+$feats,}${1#*=}" ;;
      --all-features) allf=1 ;;
      --no-default-features) nodef=1 ;;
      --workspace|--all) ws=1 ;;
      --exclude) excl=$(( excl + 1 )); shift ;;
      --exclude=*) excl=$(( excl + 1 )) ;;
    esac
    shift
  done
  if [ "$ws" -eq 1 ]; then
    scope=workspace
    [ "$excl" -gt 0 ] && scope="workspace(excl $excl)"
  elif [ "${#pkgs[@]}" -gt 0 ]; then
    scope=$(printf '%s+' "${pkgs[@]}"); scope="${scope%+}"
  else
    scope='(default pkg)'
  fi
  if [ "$allf" -eq 1 ]; then
    featpart='--all-features'
  elif [ -n "$feats" ]; then
    featpart="--features $(_fm_abbrev_features "$feats")"
  else
    featpart='default-features'
  fi
  [ "$nodef" -eq 1 ] && featpart="--no-default-features ${featpart}"
  printf '%s %s %s' "$sub" "$scope" "$featpart"
  return 0
}

# _fm_observe_cargo_argv <argv…>: record the invocation about to run. Always returns 0.
_fm_observe_cargo_argv() {
  _fm_active || return 0
  local desc
  desc=$(_fm_describe_cargo "$@" 2>/dev/null) || return 0
  [ -n "$desc" ] || return 0
  _fm_note "$AGENT_GATE_FM_COMPONENT" "$desc"
  return 0
}

# cargo / env wrappers. Both are pass-throughs: they record, then exec the real
# command with the untouched argv and the caller's stdin/stdout/stderr and exit
# status. Recording can never alter what the gate runs.
cargo() {
  _fm_observe_cargo_argv "$@" || true
  command cargo "$@"
}

# The `env` wrapper exists ONLY so that `env VAR=… cargo …` (run_clippy, _deny_warnings)
# is observed. It skips env's own options and leading NAME=VALUE assignments to find the
# command word; anything that is not `cargo` is recorded nowhere and simply passed on.
env() {
  local -a rest=("$@")
  local seen_cmd=0
  while [ "${#rest[@]}" -gt 0 ]; do
    case "${rest[0]}" in
      -i|--ignore-environment|-0|--null) rest=("${rest[@]:1}") ;;
      -u|--unset) rest=("${rest[@]:2}") ;;
      -u*|--unset=*|-C*|--chdir=*|-S*|--split-string=*) rest=("${rest[@]:1}") ;;
      --) rest=("${rest[@]:1}"); seen_cmd=1; break ;;
      -*) rest=("${rest[@]:1}") ;;
      *=*) rest=("${rest[@]:1}") ;;
      *) seen_cmd=1; break ;;
    esac
  done
  if [ "$seen_cmd" -eq 1 ] && [ "${#rest[@]}" -gt 0 ] && [ "${rest[0]}" = cargo ]; then
    _fm_observe_cargo_argv "${rest[@]:1}" || true
  fi
  command env "$@"
}

# ---------------------------------------------------------------------------
# _fm_component_class <name>: the DECLARATION site (see the header). Three classes:
#   cargo             — invokes cargo in the gate's own shell (or records its own sets
#                       from a hoisted variable); the annotation must be OBSERVED, and
#                       its absence renders UNDECLARED.
#   no-cargo          — invokes no cargo at all, anywhere: git/wc, shell guards, python.
#   indirect:<driver> — cargo DOES run, under a driver this observer cannot see, so the
#                       feature set is honestly reported as not observed rather than
#                       guessed at.
# scoped-tests is not in COMPONENTS (run_scoped_tests appends it to NAMES in --lite /
# --delta) but appears in those blocks, so it is classified here too.
_fm_component_class() {
  case "$1" in
    # no-cargo: file-size is git+wc; roborev-lints, pub-surface and
    # binding-unwind-profile run shell guards (verified: no `cargo` in
    # check-workflow-injection.sh, check-no-wallclock-asserts.sh's gate path,
    # check-pub-surface.sh or test_binding_unwind_profile.sh); delivery-telemetry runs a
    # python test.
    file-size|roborev-lints|pub-surface|binding-unwind-profile|delivery-telemetry)
      printf 'no-cargo' ;;
    # indirect: the extension is built by a driver that invokes cargo internally, so no
    # cargo argv passes through this shell. Naming the DRIVER is structural (it is the
    # command the component runs); the feature set is NOT claimed.
    python-bindings) printf 'indirect:maturin' ;;
    node-bindings)   printf 'indirect:npm run build (napi)' ;;
    fmt|clippy|core-tests|tombstones-scan|scan-offload-guard|work-counters-guard) printf 'cargo' ;;
    byte-budget-guard|arrow-parity-guard|memory-budget|integration-tests) printf 'cargo' ;;
    format-compat|write-tests|cli-tests|compaction-byte-parity) printf 'cargo' ;;
    bti-multiclustering|query-semantics-oracle|flight-query-semantics-oracle) printf 'cargo' ;;
    flight-tests|legacy-heuristics|feature-iso-parquet|feature-iso-delta-scan) printf 'cargo' ;;
    binding-rust-tests|oom-audit|parity-report|operator-metrics-doc) printf 'cargo' ;;
    kit-dashboard-drift|tooling-tests|minimal-build|all-features-check|smoke) printf 'cargo' ;;
    scoped-tests) printf 'cargo' ;;
    *) return 1 ;;
  esac
  return 0
}

# _fm_render <component>: the observed sets, deduplicated in first-seen order, identical
# sets collapsed to `desc xN`. Bounded at 6 distinct sets, with the remainder DECLARED
# (`+K more sets`) rather than dropped. Returns 1 when nothing was observed.
_fm_render() {
  local f
  f=$(_fm_sidecar "$1") || return 1
  [ -s "$f" ] || return 1
  awk '
    { if (!($0 in cnt)) { ord[++n] = $0 } ; cnt[$0]++ }
    END {
      if (n == 0) exit 1
      cap = 6
      shown = (n < cap ? n : cap)
      out = ""
      for (i = 1; i <= shown; i++) {
        d = ord[i]
        if (cnt[d] > 1) d = d " x" cnt[d]
        out = out (i > 1 ? " | " : "") d
      }
      if (n > shown) out = out " | +" (n - shown) " more sets"
      print out
    }
  ' "$f"
}

# _fm_annotate <component>: the bracketed suffix appended to a SUMMARY component line.
# NEVER empty — that is the contract (see the header).
_fm_annotate() {
  local class obs
  if ! class=$(_fm_component_class "$1"); then
    # Not declared anywhere: name that, distinctly from a declared-cargo component whose
    # observation is missing. The guard test makes this unreachable for COMPONENTS.
    printf '[UNCLASSIFIED — not declared in _fm_component_class (#3453)]'
    return 0
  fi
  if obs=$(_fm_render "$1") && [ -n "$obs" ]; then
    case "$class" in
      no-cargo) printf '[%s !declared-no-cargo]' "$obs" ;;
      *)        printf '[%s]' "$obs" ;;
    esac
    return 0
  fi
  case "$class" in
    no-cargo)   printf '[no-cargo]' ;;
    indirect:*) printf '[via %s: feature set NOT observed]' "${class#indirect:}" ;;
    *)          printf '[UNDECLARED]' ;;
  esac
  return 0
}

# _fm_summary_line <name> <status> <time>: the ONE renderer for a SUMMARY component
# line, used by all six emit sites (full, lite, two delta sites, the aggregation
# self-test and --emit-summary-selftest) so no mode can render a block the others do
# not. `%-18s` and the `(time)` shape are unchanged — the annotation is appended, so
# every existing prefix/stage-line assertion still matches.
_fm_summary_line() {
  printf '%-18s %s (%s)  %s' "$1:" "$2" "$3" "$(_fm_annotate "$1")"
}

# _fm_note_if_skipped <component> <status>: a SKIP-aware component that bailed before
# any cargo call (no python3, no node/npm, no cargo on PATH) would otherwise render
# UNDECLARED, which reads as a defect in the annotation rather than as what actually
# happened. Record the observation — nothing ran — from the two facts we HAVE (the
# status, and an empty sidecar), never from a guess about why. Declared-no-cargo
# components are left alone so their `[no-cargo]` rendering stays exact.
_fm_note_if_skipped() {
  [ "${2:-}" = SKIP ] || return 0
  _fm_active || return 0
  local class f
  class=$(_fm_component_class "$1" 2>/dev/null) || class=cargo
  [ "$class" = no-cargo ] && return 0
  f=$(_fm_sidecar "$1")
  [ -s "$f" ] && return 0
  _fm_note "$1" "no cargo invoked (component SKIPped)"
  return 0
}
