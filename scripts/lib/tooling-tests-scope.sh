#!/usr/bin/env bash
# scripts/lib/tooling-tests-scope.sh — issue #4266
#
# Diff-scoping decision for the full gate's `tooling-tests` component.
# tooling-tests runs ~80 nested shell self-tests for the delivery harness itself
# (worker supervisor, roborev guard, bootstrap, the gate's own summary/
# component-set tests, and others). Measured in gate-4194f (a full PASS on
# astro-processor): 5056s of ~11450s (44%) of the WHOLE gate. A product PR that
# touches no harness file pays all of it regardless of relevance.
#
# This file is sourced by scripts/agent-gate.sh (mirrors scripts/perf-capability.sh's
# conditional-source pattern) and is ALSO directly `bash`-able as a standalone
# library for scripts/tests/test_tooling_tests_scope.sh, which sources it the same
# way and drives the functions below without invoking the real gate.
#
# THE DECLARED PATH SET LIVES HERE, AND ONLY HERE (#4266 AC5) — one array, so the
# self-test can derive its assertions from THIS file rather than retyping the set
# and drifting from it. Adding a new harness-reaching path class means editing this
# array and nothing else; agent-gate.sh never hard-codes a path pattern of its own.
#
# Patterns are plain bash `case` globs (matched against a repo-relative path with
# forward slashes) — `*` matches `/` in this context, so `scripts/*` already covers
# `scripts/tests/foo.sh` and `scripts/lib/bar.sh`, not just top-level files.
#
# The set is hand-maintained against what run_tooling_tests's ~80 self-tests
# ACTUALLY read as live repository input, not derived structurally. A roborev
# review on this issue asked for structural derivation (walk run_tooling_tests's
# own body for path references and assert each is covered here) as the more
# principled fix, and a SECOND review round caught the first cut both (a) still
# missing several read classes and (b) claiming a "completeness-net self-test"
# existed to catch exactly that, when none did — a declared residual naming a
# non-existent mitigation is worse than an undeclared one, so that claim is
# retracted here rather than repeated: THIS RESIDUAL IS UNMITIGATED. There is no
# automated proof that the list below is exhaustive; it is reviewed by hand each
# time a roborev pass (or any reader) finds a gap, and by construction the
# direction any gap fails is SKIP-when-it-should-RUN (fail-open) — never the
# reverse, since an unmapped path simply falls through the pattern list, not into
# a special "definitely out of scope" branch. Structural derivation is tracked as
# a follow-up (#4266 round-2 findings 1/2), not attempted here.
#
# Classes beyond scripts/**, .github/**, .claude/**, and the workspace/toolchain
# pins: the #2878 fetch-datasets.sh fixture guard and test_check_dataset_manifest.sh
# etc. read `test-data/scripts/**`; several `test-data/*.yml` and
# `test-data/*.env`/`perf-corpus-*` fixtures are read directly by name
# (test_agent_gate_parity_report.sh, test_agent_gate_schemas_preflight.sh,
# test_bti_perf_scan.sh, test_gen_perf_corpus_bti.sh); `test_ws0_canonical_corpus.sh`
# plus the `cargo test -p ws0-corpus-gen` block read `tools/**` and the committed
# corpus-identity pin under `docs/reports/ws0-*-artifacts/**` (subsumed by the
# broader `docs/*` entry below); the #2870 Dockerfile-side half of the Rust-pin
# lockstep guard reads `cqlite-flight/Dockerfile` (the other half,
# `rust-toolchain.toml`, was already declared); test_check_dataset_manifest.sh
# also reads `bindings/node/__test__/**`; `test_gate_detached.sh` derives its
# subjects from the committed `.gitignore`; test_gate_component_verdict.sh greps
# `CLAUDE.md`, `process_improvements.md`, `docs/**` and
# `website/src/content/docs/**` for completion-grammar drift; and
# test_worker_supervisor.sh reads `docs/development/fleet-runbook.md` (also
# subsumed by `docs/*`).
#
# DECLARED RESIDUAL (round-2 finding 5, left as-is rather than restructured under
# the 2-roborev-round cap): the `cargo test -p ws0-corpus-gen` block inside
# run_tooling_tests is that crate's ONLY execution point in the gate of record
# (CLAUDE.md), and that crate exercises the production `SSTableWriter` /
# `Database::execute_streaming` write path — yet `cqlite-core/**` is
# DELIBERATELY OUT of this declared set (a core-only diff must SKIP per #4266
# AC1). A `cqlite-core` write-path change that breaks ws0-corpus-gen's generator
# therefore loses its only gate-of-record signal until the nightly unconditional
# run. Follow-up: move that block to an unscoped component, or accept the
# nightly-only cadence explicitly.
TOOLING_TESTS_SCOPE_PATTERNS=(
  'scripts/*'
  '.github/*'
  '.claude/*'
  '.roborev.toml'
  'rust-toolchain.toml'
  'Cargo.lock'
  'Cargo.toml'
  '.gitignore'
  'CLAUDE.md'
  'process_improvements.md'
  'docs/*'
  'website/src/content/docs/*'
  'test-data/scripts/*'
  'test-data/*.yml'
  'test-data/*.env'
  'test-data/perf-corpus-*'
  'tools/*'
  'cqlite-flight/Dockerfile'
  'bindings/node/__test__/*'
)

# Human-readable rendering of the declared set, fixed wording only (no repository
# content) — safe to interpolate into a component's gate-authored status detail
# (see _record_status_detail's contract in agent-gate.sh: fixed wording plus
# computed VALUES, never a repository-derived path/name). Derived from the array
# above so the two can never drift from each other.
_tooling_tests_scope_declared_text() {
  local IFS=,
  printf '%s' "${TOOLING_TESTS_SCOPE_PATTERNS[*]}"
}

# _tooling_tests_path_in_scope <repo-relative-path>: pure classification, no git.
# True (0) iff the path matches one of the declared harness patterns.
_tooling_tests_path_in_scope() {
  local p="$1" pat
  for pat in "${TOOLING_TESTS_SCOPE_PATTERNS[@]}"; do
    case "$p" in
      $pat) return 0 ;;
    esac
  done
  return 1
}

# _tooling_tests_classify_stdin: read repo-relative changed paths on stdin (one per
# line), print "IN-SCOPE <path>" for each declared-set match, then a final
# "MATCHED: <N>" and "VERDICT: RUN|SKIP" line. Pure — no git, no side effects — so
# scripts/tests can assert the classification hermetically via a hidden CLI hook
# (agent-gate.sh --tooling-tests-classify) without any git fixture at all.
_tooling_tests_classify_stdin() {
  local f n=0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if _tooling_tests_path_in_scope "$f"; then
      printf 'IN-SCOPE %s\n' "$f"
      n=$((n + 1))
    fi
  done
  printf 'MATCHED: %s\n' "$n"
  if [ "$n" -gt 0 ]; then
    printf 'VERDICT: RUN\n'
  else
    printf 'VERDICT: SKIP\n'
  fi
}

# _tooling_tests_resolve_base [<override>]: resolve a diff base the same way
# run_file_size does (GATE_BASE_OVERRIDE, else merge-base HEAD against the first of
# origin/main / main / origin/master / master that resolves). Prints the resolved
# base SHA on stdout and returns 0, or prints nothing and returns 1 when no base
# resolves at all (an unmeasurable diff — the caller fails closed to RUN).
_tooling_tests_resolve_base() {
  local override="${1:-${GATE_BASE_OVERRIDE:-}}"
  if [ -n "$override" ]; then
    if git rev-parse --verify -q "$override" >/dev/null 2>&1; then
      printf '%s\n' "$override"
      return 0
    fi
    return 1
  fi
  local ref base
  for ref in origin/main main origin/master master; do
    if git rev-parse --verify -q "$ref" >/dev/null 2>&1; then
      base=$(git merge-base HEAD "$ref" 2>/dev/null) && [ -n "$base" ] && {
        printf '%s\n' "$base"
        return 0
      }
    fi
  done
  return 1
}

# _tooling_tests_changed_paths <base>: print repo-relative changed paths, unioned
# and deduplicated across THREE legs. Mirrors run_file_size's committed+working-tree
# scope, so a dirty tree touching a harness path still forces RUN. Returns 1
# (prints nothing usable) if ANY of the three underlying git calls fails — the
# caller treats that as unmeasurable and fails closed to RUN.
#
#   1. `git diff <base>` — <base> vs the WORKING TREE (not `<base>..HEAD`: with no
#      `--cached`/`HEAD` argument this already includes any uncommitted, TRACKED
#      change on top of HEAD, so it is a superset of the committed range).
#   2. `git ls-files --others --exclude-standard` — untracked files (leg 1 never
#      reports these; see point 2 below).
#   3. `git diff HEAD` — uncommitted TRACKED changes vs HEAD. Given leg 1 already
#      covers <base>..working-tree, this leg's only ADDITIONAL job is a working-tree
#      REVERT of a change that WAS committed between <base> and HEAD (leg 1 would
#      then show no difference for that path, since base..working-tree nets to
#      identical content) — a case leg 1 alone cannot see.
#
# THREE PROPERTIES A ROBOREV REVIEW ON THIS ISSUE FOUND MISSING FROM THE FIRST
# CUT, each a fail-OPEN (the one direction this scoper must never take):
#
#   1. DELETIONS AND RENAMES. Classification needs only the PATH NAME, never the
#      file's post-change existence — unlike run_file_size, which legitimately
#      excludes deletions (a deleted file cannot grow). run_tooling_tests invokes
#      ~80 self-tests by hard-coded path, so a diff that only DELETES or MOVES
#      `scripts/tests/test_foo.sh` must still be seen: no `--diff-filter`
#      restriction, and `--no-renames` so a rename is reported as delete-old +
#      add-new (both paths visible) rather than collapsed to the destination
#      alone — the same reasoning run_delta's own diff already applies (roborev
#      job 3338 there).
#   2. UNTRACKED FILES. Neither `git diff` invocation reports a brand-new,
#      not-yet-`git add`ed file — so introducing a NEW self-test under
#      `scripts/tests/` would not itself be seen as in-scope, and the new test
#      would never run in the very PR that adds it. `git ls-files --others
#      --exclude-standard` closes this.
#   3. QUOTED/ESCAPED PATHS. `git diff --name-only` C-quotes paths containing
#      non-ASCII or special bytes when `core.quotePath` is on (the default) —
#      e.g. `"scripts/t\303\251st.sh"` — and the leading quote character defeats
#      `case "$p" in scripts/*)`. `-z` (NUL-terminated, unquoted output) removes
#      the quoting entirely, independent of the repo's `core.quotePath` setting.
#      NUL-separated output cannot be read through a `$( )` command substitution
#      (bash strings cannot hold embedded NUL bytes), so each leg is written to a
#      scratch file and read back with `read -r -d ''`.
_tooling_tests_changed_paths() {
  local base="$1" f tmp
  tmp=$(mktemp "${TMPDIR:-/tmp}/tooling-tests-scope.XXXXXX") || return 1
  local -a out=()

  if ! git diff --name-only -z --no-renames "$base" >"$tmp" 2>/dev/null; then
    rm -f "$tmp"; return 1
  fi
  while IFS= read -r -d '' f; do [ -n "$f" ] && out+=("$f"); done <"$tmp"

  : >"$tmp"
  if ! git ls-files --others --exclude-standard -z >"$tmp" 2>/dev/null; then
    rm -f "$tmp"; return 1
  fi
  while IFS= read -r -d '' f; do [ -n "$f" ] && out+=("$f"); done <"$tmp"

  : >"$tmp"
  if ! git diff --name-only -z --no-renames HEAD >"$tmp" 2>/dev/null; then
    rm -f "$tmp"; return 1
  fi
  while IFS= read -r -d '' f; do [ -n "$f" ] && out+=("$f"); done <"$tmp"

  rm -f "$tmp"
  printf '%s\n' "${out[@]+"${out[@]}"}" | awk 'NF' | sort -u
}

# _tooling_tests_scope_decide [<base-override>]: the top-level decision. Sets three
# globals the caller reads (never returns a status — every branch is a deliberate
# outcome, not a failure of this function):
#
#   TOOLING_SCOPE_DECISION   RUN | SKIP
#   TOOLING_SCOPE_CAUSE      one-line free text for the run log (may name the
#                            resolved base; NOT safe to interpolate verbatim into a
#                            SUMMARY status-detail field — see TOOLING_SCOPE_DETAIL)
#   TOOLING_SCOPE_DETAIL     gate-authored text ONLY (fixed wording + counts, no
#                            repository content) — this is what the caller passes to
#                            _record_status_detail so it renders on the SUMMARY row.
#
# FAIL-CLOSED (#4266 AC3): an unresolvable base, or a `git diff` that itself fails,
# decides RUN, never SKIP — the diff could not be measured, so scoping abstains
# rather than guesses.
#
# FORCED-RUN escape hatch for the nightly unconditional lane (#4266 AC4): the
# nightly gate.yml deep-check job runs on `main` itself, where HEAD IS the merge
# base, so the diff against it is empty by construction and this function would
# otherwise SKIP a lane whose entire job is to catch harness drift. Setting
# CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 (set by that workflow step) bypasses scoping
# unconditionally, before any git call.
_tooling_tests_scope_decide() {
  TOOLING_SCOPE_DECISION=RUN
  TOOLING_SCOPE_CAUSE=""
  TOOLING_SCOPE_DETAIL=""

  if [ "${CQLITE_TOOLING_TESTS_ALWAYS_RUN:-0}" = 1 ]; then
    TOOLING_SCOPE_CAUSE="CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 — running unconditionally (the nightly unconditional lane)"
    TOOLING_SCOPE_DETAIL="forced by CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 (nightly unconditional lane)"
    return 0
  fi

  local base
  if ! base=$(_tooling_tests_resolve_base "${1:-}"); then
    TOOLING_SCOPE_CAUSE="cause=no-merge-base — could not resolve a base ref (override, origin/main, main, origin/master, master all failed) — running unconditionally (fail-closed, #4266 AC3)"
    TOOLING_SCOPE_DETAIL="diff base unresolved — running unconditionally (fail-closed)"
    return 0
  fi

  local changed
  if ! changed=$(_tooling_tests_changed_paths "$base"); then
    TOOLING_SCOPE_CAUSE="cause=diff-unreadable — 'git diff' against base $base failed — running unconditionally (fail-closed, #4266 AC3)"
    TOOLING_SCOPE_DETAIL="diff against base could not be read — running unconditionally (fail-closed)"
    return 0
  fi

  local classified
  classified=$(printf '%s\n' "$changed" | _tooling_tests_classify_stdin)
  local matched verdict
  matched=$(printf '%s\n' "$classified" | sed -n 's/^MATCHED: //p')
  verdict=$(printf '%s\n' "$classified" | sed -n 's/^VERDICT: //p')
  matched=${matched:-0}

  if [ "$verdict" = RUN ]; then
    TOOLING_SCOPE_DECISION=RUN
    TOOLING_SCOPE_CAUSE="cause=in-diff — $matched harness path(s) in diff vs $base (declared set: $(_tooling_tests_scope_declared_text))"
    TOOLING_SCOPE_DETAIL="$matched harness path(s) in diff (declared set: $(_tooling_tests_scope_declared_text))"
  else
    TOOLING_SCOPE_DECISION=SKIP
    TOOLING_SCOPE_CAUSE="cause=not-in-diff — 0 harness paths in diff vs $base (declared set: $(_tooling_tests_scope_declared_text)); nightly tooling-tests on main covers drift"
    TOOLING_SCOPE_DETAIL="0 harness paths in diff (declared set: $(_tooling_tests_scope_declared_text)); nightly tooling-tests on main covers drift"
  fi
}
