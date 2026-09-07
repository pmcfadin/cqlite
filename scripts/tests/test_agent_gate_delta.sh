#!/usr/bin/env bash
# Regression test for issue #1892: agent-gate.sh --delta test/docs-only
# RE-CERTIFICATION. After a full-gate PASS at an anchor commit, a diff
# anchor..HEAD that touches ONLY what the re-cert can EXECUTE — rust cargo test
# code (AUTHORITATIVE: a .rs file that IS a Cargo `--test` target scoped-tests
# runs, discovered via cargo metadata — NOT globs; roborev job 3327), python
# binding tests (bindings/python/tests/, run by the #1893 python tier), docs
# (markdown ONLY: *.md anywhere; non-md files under docs/ or website/ are REFUSED),
# node jest tests (bindings/node/__test__/, executed by run_delta_node_tests — issue
# #2081), and shell self-tests (scripts/tests/*.sh, executed by
# run_delta_shell_selftests — issue #2081) — may re-certify with file-size + fmt +
# the changed test targets + those executors; ANYTHING else in the diff FAILs closed
# (a fresh full gate is required), including a .rs that is NOT a --test target (nested
# helper mods, src *_test(s).rs, scripts/*.rs, the workspace-excluded fuzz/ crate) and
# src/Cargo.*/workflows/config/test-data. A node __test__/ delta additionally REFUSES
# up front when the native module is not built (--delta never builds with cargo —
# issue #2081). Because the allow decision is now cargo
# metadata-backed, the ALLOW cases below use REAL existing --test target files
# (invented paths cargo does not know would correctly REFUSE). The delta run
# emits a DISTINCT "==== AGENT-GATE DELTA SUMMARY ====" block (MODE: delta) that
# can never be pasted as a full SUMMARY and names the gate of record (the full
# PASS at the anchor) + the nightly backstop.
#
# Fast + hermetic by design: exercises the load-bearing NEW logic — the
# fail-closed test/docs classification (via the hidden --delta-classify hook),
# the entry-point guards (bad anchor / non-full anchor summary), the DELTA
# summary emission (via --delta ... --emit-summary-selftest), and the git-backed
# fail-closed enumeration (a production->allowed RENAME must REFUSE — roborev job
# 3338 — driven end-to-end in an isolated temp git repo). It NEVER runs cargo (no
# component execution) and NEVER mutates the real working tree.
#
# Run standalone:   bash scripts/tests/test_agent_gate_delta.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# #2751 defense-in-depth: this self-test recursively invokes the REAL gate — the
# temp-repo `--delta` runs below (rename-refuses, node-build-refuse) spawn
# agent-gate.sh WITHOUT an explicit AGENT_GATE_SUMMARY_FILE, so an inherited value
# would make them write the DELTA REFUSED / startup INCOMPLETE block to the
# caller's summary file (the tooling-tests clobber, #2751). Scrub any inherited
# path up front so those nested gates fall back to their OWN (temp-repo) default,
# never clobbering the caller — even when this script is run standalone by an agent
# who has AGENT_GATE_SUMMARY_FILE exported. Per-case invocations below that pin
# AGENT_GATE_SUMMARY_FILE="$tmp/..." set it fresh and are unaffected.
unset AGENT_GATE_SUMMARY_FILE

DELTA_START="==== AGENT-GATE DELTA SUMMARY ===="
DELTA_END="==== END AGENT-GATE DELTA SUMMARY ===="
FULL_START="==== AGENT-GATE SUMMARY ===="
LITE_START="==== AGENT-GATE LITE SUMMARY ===="

PASS=0
FAIL=0
SKIP=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
skip() { printf 'skip - %s\n' "$1"; SKIP=$((SKIP + 1)); }

# Metadata-parser availability (roborev job 3336). The .rs-ALLOW classification is
# cargo-metadata-backed: --delta-classify's _test_target_index parses `cargo
# metadata` with jq OR python3. On a machine with NEITHER, the index is empty and
# EVERY .rs fails closed to REFUSE — which is the correct degraded behavior, but it
# would make the .rs-ALLOW cases below FAIL and hard-fail the gate. So gate every
# metadata-dependent .rs-ALLOW assertion on parser availability: run it for real
# when a parser exists (the normal CI/dev case — full coverage preserved), else SKIP
# it and assert the fail-closed degraded behavior instead. Path-only cases (*.md
# ALLOW, all REFUSE cases, the python-tier gap cases) do NOT depend on a parser and
# run unconditionally.
if command -v jq >/dev/null 2>&1 || command -v python3 >/dev/null 2>&1; then
  METADATA_PARSER=1
else
  METADATA_PARSER=0
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-delta-test.XXXXXX" 2>/dev/null) || tmp=""
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'FAIL - could not create a scratch dir under %s — refusing to run\n' "${TMPDIR:-/tmp}"
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT

# #3637: the fixture runs below invoke real gates with no explicit summary path, so
# each writes its #2874 private summary INSIDE its own per-run LOG_DIR and the gate
# RETAINS that directory by design. Point $TMPDIR at this harness's own scratch root
# so the trap above reclaims them instead of leaving several per run under the shared
# ambient temp. Validated first — this export is a derivation from $tmp.
export TMPDIR="$tmp/tmpdir"
mkdir -p "$TMPDIR" || { printf 'FAIL - could not create the scoped TMPDIR %s\n' "$TMPDIR"; exit 1; }

# shellcheck source=scripts/tests/lib/agent-gate-canonical-pin.bash
. "$SCRIPT_DIR/lib/agent-gate-canonical-pin.bash"

# add_local_origin <repo> (#3544): give a scratch fixture a LOCAL bare `origin` whose
# `main` is the fixture's own current commit. The gate's component-set pre-flight fetches
# origin/main and FAILS CLOSED in the certifying modes (--delta is one) when the baseline
# is unobtainable, so a remote-less fixture would exit at that pre-flight instead of
# reaching the --delta classification these cases are about. A path remote keeps the fetch
# REAL while staying hermetic (no network), and pushing the fixture's own commit makes
# origin/main an ancestor of HEAD with an identical component set — so the pre-flight
# PASSes and the case still measures what it says it measures.
add_local_origin() {
  local repo="${1:-}"
  # An EMPTY/absent path would make the `( cd "$repo" && git remote add … )` below run in the
  # CURRENT tree — `cd ""` succeeds in bash and stays put. Refused loudly instead (the class
  # cost a real `git remote set-url origin` on a live checkout in the component-set suite).
  [ -n "$repo" ] && [ -d "$repo" ] \
    || { echo "FATAL: add_local_origin needs an existing fixture dir (got '${1:-}')" >&2; exit 1; }
  git init -q --bare "$repo.origin.git" >/dev/null 2>&1
  git -C "$repo.origin.git" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
  ( cd "$repo" \
      && git remote add origin "$repo.origin.git" \
      && git push -q origin HEAD:refs/heads/main ) >/dev/null 2>&1
}

# copy_gate_with_pin <repo> (#3544 / roborev job 225): copy the gate into a fixture AND pin
# its canonical-identity literal to the LOCAL origin add_local_origin will create.
#
# The pre-flight validates that `origin` NAMES the canonical upstream
# (`github.com/pmcfadin/cqlite`) before fetching a baseline — `origin` merely EXISTING made
# `git remote set-url origin <anything>` a git-config-shaped opt-out, and a baseline of unknown
# provenance is not a measurement (while the pre-flight still RAN the fetched gate, it admitted
# code as well; #3544 REQ-3544-01 reads the baseline as DATA and the check remains, as defence
# in depth). A LOCAL PATH is therefore deliberately not canonical, and without
# this pin both --delta fixtures below stop at the pre-flight as `remote-not-canonical`
# instead of reaching the REFUSED paths they exist to test. That was the job-225 regression:
# it would have surfaced as a full-gate FAIL under `tooling-tests`, which neither `--lite` nor
# the component-set suite executes. Substituting the ARTIFACT in the fixture's own scratch
# copy is the sanctioned pattern (CLAUDE.md); a settable seam would reopen the hole.
#
# BEFORE THE FIXTURE'S FIRST COMMIT, deliberately: pinning afterwards leaves the gate copy as
# a DIRTY working-tree change, which the node fixture's later `git commit -am` swept into the
# anchor..HEAD diff — so that case REFUSED naming `scripts/agent-gate.sh` instead of the
# unbuilt module, i.e. passed/failed for a reason unrelated to what it tests (measured).
copy_gate_with_pin() {
  local repo="${1:-}"
  [ -n "$repo" ] \
    || { echo "FATAL: copy_gate_with_pin needs a fixture dir" >&2; exit 1; }
  mkdir -p "$repo/scripts"
  cp "$GATE" "$repo/scripts/agent-gate.sh"
  agent_gate_pin_canonical_remote "$repo/scripts/agent-gate.sh" "$repo.origin.git" \
    || { echo "FATAL: could not pin the canonical identity in fixture '$repo'" >&2; exit 1; }
  # …and the component MANIFEST beside it (#3544 REQ-3544-01): the pre-flight reads its
  # baseline as DATA and first asserts the working tree's manifest matches the running
  # COMPONENTS array, so a gate copy without one stops at `manifest-missing` — in `--delta`,
  # a CERTIFYING mode, that is fail-closed and no case below would reach its REFUSED path.
  # BEFORE the fixture's first commit, for the same reason the pin is: a post-commit write
  # leaves the fixture DIRTY and gets swept into an anchor..HEAD diff.
  agent_gate_install_components_manifest "$repo/scripts/agent-gate.sh" \
    || { echo "FATAL: could not install the component manifest in fixture '$repo'" >&2; exit 1; }
}

# assert_verdict <label> <expected> <paths...>: pipe the paths through the hidden
# --delta-classify hook and assert the final VERDICT line equals <expected>.
assert_verdict() {
  local label="$1" expected="$2"; shift 2
  local out verdict
  out=$(printf '%s\n' "$@" | bash "$GATE" --delta-classify 2>/dev/null)
  verdict=$(printf '%s\n' "$out" | grep -E '^VERDICT: ' | head -1 | sed 's/^VERDICT: //')
  if [ "$verdict" = "$expected" ]; then
    ok "$label: VERDICT $verdict (expected $expected)"
  else
    bad "$label: VERDICT '$verdict' (expected $expected)"
    echo "------- classify output -------"; printf '%s\n' "$out"; echo "-------------------------------"
  fi
}

# 1. src-file-in-diff → REFUSE (fail-closed). A production .rs file must refuse,
#    while a REAL --test target file (cargo metadata-backed) stays ALLOW.
assert_verdict "src-file-refuses" REFUSE \
  "cqlite-core/src/storage/sstable/reader.rs" \
  "cqlite-core/tests/write_read_roundtrip.rs"
# The offending src file must be the one marked REFUSE (not the test file).
src_out=$(printf '%s\n' "cqlite-core/src/storage/sstable/reader.rs" "cqlite-core/tests/write_read_roundtrip.rs" \
  | bash "$GATE" --delta-classify 2>/dev/null)
if [ "$METADATA_PARSER" -eq 1 ]; then
  if printf '%s\n' "$src_out" | grep -qxF "REFUSE cqlite-core/src/storage/sstable/reader.rs" \
     && printf '%s\n' "$src_out" | grep -qxF "ALLOW cqlite-core/tests/write_read_roundtrip.rs"; then
    ok "src-file-refuses: the src file is marked REFUSE, the test file ALLOW"
  else
    bad "src-file-refuses: per-file classification wrong"
    echo "------- classify output -------"; printf '%s\n' "$src_out"; echo "-------------------------------"
  fi
else
  skip "SKIP: no jq/python3 metadata parser — .rs ALLOW cases require cargo metadata (src-file-refuses per-file ALLOW)"
  # Degraded fail-closed: with no parser, the src file AND the real --test target
  # .rs both REFUSE (the test-target line can no longer be ALLOW).
  if printf '%s\n' "$src_out" | grep -qxF "REFUSE cqlite-core/src/storage/sstable/reader.rs" \
     && printf '%s\n' "$src_out" | grep -qxF "REFUSE cqlite-core/tests/write_read_roundtrip.rs"; then
    ok "src-file-refuses (no parser): both src and the real --test target .rs REFUSE (fail-closed)"
  else
    bad "src-file-refuses (no parser): expected both paths REFUSE without a metadata parser"
    echo "------- classify output -------"; printf '%s\n' "$src_out"; echo "-------------------------------"
  fi
fi

# 2. test-only → ALLOW (would proceed to emit a delta block). Cover the EXECUTABLE
#    classes with REAL, cargo metadata-known targets: top-level integration-test
#    targets in cqlite-core / cqlite-cli / the workspace root, plus a
#    bindings/python/tests/ file (the #1893 python tier). These are actual files in
#    the workspace, so cargo metadata resolves them to --test targets.
if [ "$METADATA_PARSER" -eq 1 ]; then
  assert_verdict "test-only-allows" ALLOW \
    "cqlite-core/tests/write_read_roundtrip.rs" \
    "cqlite-core/tests/compaction_integration.rs" \
    "cqlite-cli/tests/integration_tests.rs" \
    "tests/cache_metrics_test.rs" \
    "bindings/python/tests/test_parity.py"
else
  skip "SKIP: no jq/python3 metadata parser — .rs ALLOW cases require cargo metadata (test-only-allows)"
  # Degraded fail-closed: without a parser the real --test target .rs files can no
  # longer be resolved, so the set REFUSES (a fresh full gate is required).
  assert_verdict "test-only-allows (no parser, fail-closed)" REFUSE \
    "cqlite-core/tests/write_read_roundtrip.rs" \
    "cqlite-core/tests/compaction_integration.rs" \
    "cqlite-cli/tests/integration_tests.rs" \
    "tests/cache_metrics_test.rs" \
    "bindings/python/tests/test_parity.py"
fi

# 2a. A .rs whose name matches the OLD *_test(s).rs glob but that lives in src/ (or
#     scripts/, or the excluded fuzz/ crate) is NOT a Cargo --test target → REFUSE
#     (roborev job 3327). The old static-glob allow classified these as tests; the
#     authoritative cargo-metadata decision refuses them.
assert_verdict "src-test-suffix-refuses"     REFUSE "cqlite-core/src/storage/reader_test.rs"
assert_verdict "src-tests-suffix-refuses"     REFUSE "cqlite-core/src/query/planner_tests.rs"
assert_verdict "script-tests-suffix-refuses"  REFUSE "scripts/foo_tests.rs"
assert_verdict "fuzz-crate-target-refuses"    REFUSE "fuzz/fuzz_targets/fuzz_vint.rs"
# An invented top-level tests/*.rs path cargo does not know is REFUSED (it is not a
# real --test target); only real target files are allowed.
assert_verdict "unknown-test-target-refuses"  REFUSE "cqlite-core/tests/does_not_exist_1892.rs"

# 2b. FAIL-CLOSED with no cargo-metadata parser (roborev job 3327). Force the
#     no-metadata-parser path via AGENT_GATE_TEST_NO_METADATA_PARSER=1: the
#     test-target index is empty, so NO .rs is allowed — even a REAL --test target
#     file REFUSES, forcing the full gate. (*.md and python tests are path-only, so
#     they stay ALLOW even without metadata.)
no_meta_v=$(printf '%s\n' "cqlite-core/tests/write_read_roundtrip.rs" \
  | AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --delta-classify 2>/dev/null \
  | grep -E '^VERDICT: ' | head -1 | sed 's/^VERDICT: //')
if [ "$no_meta_v" = REFUSE ]; then
  ok "no-metadata-parser: a real --test target .rs REFUSES when metadata is unavailable (fail-closed)"
else
  bad "no-metadata-parser: expected REFUSE with no metadata parser (got '$no_meta_v')"
fi
no_meta_md=$(printf '%s\n' "README.md" \
  | AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --delta-classify 2>/dev/null \
  | grep -E '^VERDICT: ' | head -1 | sed 's/^VERDICT: //')
if [ "$no_meta_md" = ALLOW ]; then
  ok "no-metadata-parser: *.md still ALLOWs (path-only, independent of cargo metadata)"
else
  bad "no-metadata-parser: *.md should ALLOW without metadata (got '$no_meta_md')"
fi

# 2b. node jest tests + shell self-tests → ALLOW (issue #2081): --delta now EXECUTES
#     them (run_delta_node_tests against the already-built native module;
#     run_delta_shell_selftests runs the changed scripts/tests/*.sh verbatim), so a
#     node-test-only or shell-selftest-only polish round re-certifies with --delta
#     instead of a whole new full gate. (The node executor REFUSES up front if the
#     native module is not built — see the node-build-gate cases below.)
assert_verdict "node-test-file-allows" ALLOW \
  "bindings/node/__test__/database.test.js"
assert_verdict "shell-selftest-allows" ALLOW \
  "scripts/tests/test_agent_gate_summary.sh"

# 3. docs-only → ALLOW. The doc allowlist is MARKDOWN ONLY (*.md anywhere,
#    including under docs/ and website/).
assert_verdict "docs-only-allows" ALLOW \
  "README.md" \
  "docs/development/pm-operating-loop.md" \
  "docs/sstables-definitive-guide/README.md"
assert_verdict "top-level-docs-allows" ALLOW "docs/profiling.md"
# Markdown under docs/ and website/ stays ALLOWED via the *.md rule.
assert_verdict "website-md-allows" ALLOW "website/docs/agents-developing/gate-contract.md"
assert_verdict "docs-md-allows" ALLOW "docs/development/pm-operating-loop.md"

# 3b. Deep (non-top-level) docs/ and website/ dirs are PRODUCTION (roborev job
#     1452): only *.md is allowed anywhere, so a hypothetical src/docs/mod.rs or
#     a nested website/ dir must REFUSE.
assert_verdict "deep-docs-dir-refuses" REFUSE \
  "cqlite-core/src/docs/mod.rs"
assert_verdict "deep-website-dir-refuses" REFUSE \
  "tools/website/generate.rs"

# 3c. NON-markdown files under docs/ or website/ → REFUSE (roborev job 3325):
#     the blanket docs/* and website/* allows were removed because no delta
#     component builds/validates config, app code, assets, or data artifacts.
#     Only *.md content is pure documentation the delta can safely pass through.
assert_verdict "website-config-refuses"    REFUSE "website/astro.config.mjs"
assert_verdict "website-package-refuses"    REFUSE "website/package.json"
assert_verdict "website-astro-comp-refuses" REFUSE "website/src/components/Foo.astro"
assert_verdict "website-html-refuses"       REFUSE "website/src/index.html"
assert_verdict "docs-jsonl-data-refuses"    REFUSE "docs/reports/delivery-telemetry.jsonl"

# 4. mixed (test + production) → REFUSE (any production file poisons the delta).
assert_verdict "mixed-refuses" REFUSE \
  "cqlite-core/tests/write_read_roundtrip.rs" \
  "README.md" \
  "cqlite-core/src/lib.rs"

# 5. non-test/docs production classes each REFUSE: Cargo manifests, scripts,
#    workflows, config, and test-DATA (a schema/fixture is not a test *file*).
assert_verdict "cargo-manifest-refuses"  REFUSE "Cargo.toml"
assert_verdict "lockfile-refuses"        REFUSE "Cargo.lock"
assert_verdict "script-refuses"          REFUSE "scripts/agent-gate.sh"
assert_verdict "workflow-refuses"        REFUSE ".github/workflows/gate.yml"
assert_verdict "test-data-refuses"       REFUSE "test-data/schemas/basic-types.cql"

# 5b. DELETIONS flow through the same PATH classifier (roborev job 3323): run_delta
#     no longer drops deletions (--diff-filter=d removed), so a DELETED path is
#     classified by path string exactly like an added/modified one. The classifier
#     itself is path-only (it cannot see add-vs-delete) — which is the point: a
#     deleted production file becomes offending and REFUSES, a deleted docs/test
#     file stays allowed. These assert the classification the deletion path relies on.
assert_verdict "deleted-script-refuses"    REFUSE "scripts/foo.sh"
assert_verdict "deleted-workflow-refuses"  REFUSE ".github/workflows/x.yml"
assert_verdict "deleted-src-refuses"       REFUSE "cqlite-core/src/lib.rs"
assert_verdict "deleted-doc-allows"        ALLOW  "docs/x.md"
assert_verdict "deleted-md-allows"         ALLOW  "some/nested/NOTES.md"

# 5c. NESTED rust test-helper mods under a tests/ dir → REFUSE (roborev jobs 3323 /
#     3327): they are not Cargo integration-test *targets* (run_scoped_tests runs
#     the package --lib + top-level tests/<name>.rs targets, never a nested helper
#     mod), so cargo metadata never resolves them to a --test target and certifying
#     one would be a wiring-evidence gap. A TOP-LEVEL integration-test target stays
#     ALLOWED. These use REAL existing files so the cargo metadata decision is
#     exercised: write_read_roundtrip/type_coverage.rs is a submodule of the
#     write_read_roundtrip target (not itself a target); common/mod.rs and
#     parity_support/mod.rs are shared helper mods.
assert_verdict "nested-helper-mod-refuses" REFUSE "cqlite-core/tests/write_read_roundtrip/type_coverage.rs"
assert_verdict "nested-common-mod-refuses" REFUSE "cqlite-core/tests/common/mod.rs"
assert_verdict "nested-support-mod-refuses" REFUSE "cqlite-core/tests/parity_support/mod.rs"
nested_out=$(printf '%s\n' "cqlite-core/tests/write_read_roundtrip/type_coverage.rs" "cqlite-core/tests/write_read_roundtrip.rs" \
  | bash "$GATE" --delta-classify 2>/dev/null)
if [ "$METADATA_PARSER" -eq 1 ]; then
  assert_verdict "top-level-integration-target-allows" ALLOW "cqlite-core/tests/compaction_integration.rs"
  assert_verdict "top-level-integration-root-allows" ALLOW "tests/cache_metrics_test.rs"
  # Per-file check: in a mixed nested-helper + top-level-target diff, the nested
  # helper is the one marked REFUSE while the top-level target stays ALLOW.
  if printf '%s\n' "$nested_out" | grep -qxF "REFUSE cqlite-core/tests/write_read_roundtrip/type_coverage.rs" \
     && printf '%s\n' "$nested_out" | grep -qxF "ALLOW cqlite-core/tests/write_read_roundtrip.rs"; then
    ok "nested-vs-target: nested helper REFUSE, top-level target ALLOW"
  else
    bad "nested-vs-target: per-file classification wrong"
    echo "------- classify output -------"; printf '%s\n' "$nested_out"; echo "-------------------------------"
  fi
else
  skip "SKIP: no jq/python3 metadata parser — .rs ALLOW cases require cargo metadata (top-level-integration-target-allows / nested-vs-target)"
  # Degraded fail-closed: without a parser even a real top-level --test target
  # REFUSES, so both the nested helper and the top-level target REFUSE.
  assert_verdict "top-level-integration-target-refuses (no parser, fail-closed)" REFUSE "cqlite-core/tests/compaction_integration.rs"
  assert_verdict "top-level-integration-root-refuses (no parser, fail-closed)" REFUSE "tests/cache_metrics_test.rs"
  if printf '%s\n' "$nested_out" | grep -qxF "REFUSE cqlite-core/tests/write_read_roundtrip/type_coverage.rs" \
     && printf '%s\n' "$nested_out" | grep -qxF "REFUSE cqlite-core/tests/write_read_roundtrip.rs"; then
    ok "nested-vs-target (no parser): both nested helper and top-level target REFUSE (fail-closed)"
  else
    bad "nested-vs-target (no parser): expected both paths REFUSE without a metadata parser"
    echo "------- classify output -------"; printf '%s\n' "$nested_out"; echo "-------------------------------"
  fi
fi

# 5d. FAIL-CLOSED python-tier gap (issue #1892, roborev job 3333): --delta ALLOWS
#     bindings/python/tests/* ONLY on the premise that the #1893 python tier RUNS
#     them. --delta runs NO clippy, so if the python tier is SKIPPED (python3
#     missing, or venv/pip/maturin setup failed) while a python test file is in the
#     allowed set, the changed tests were NEVER re-certified and a PASS DELTA block
#     would be an unsound green — run_delta must REFUSE. run_delta consumes the SAME
#     _delta_python_tier_gap decision the hidden --delta-python-gap hook exposes, so
#     these assert the real fail-closed behavior hermetically (no cargo/maturin/git).
assert_python_gap() {  # <label> <expected GAP|OK> <note> <allowed-paths...>
  local label="$1" expected="$2" note="$3"; shift 3
  local got
  got=$(printf '%s\n' "$@" | bash "$GATE" --delta-python-gap "$note" 2>/dev/null | head -1)
  if [ "$got" = "$expected" ]; then
    ok "$label: $got (expected $expected)"
  else
    bad "$label: '$got' (expected $expected)"
  fi
}
# python test in scope + tier SKIPPED (no python3) → GAP → run_delta REFUSES (not PASS).
assert_python_gap "py-skip-no-python3-gaps" GAP \
  "python-tier: SKIPPED (no python3 on PATH) — python-binding diff NOT validated by this lite run; run the full gate" \
  "bindings/python/tests/test_parity.py"
# python test in scope + tier SKIPPED (toolchain: venv/pip/maturin) → GAP.
assert_python_gap "py-skip-toolchain-gaps" GAP \
  "python-tier: SKIPPED (toolchain: venv/pip/maturin setup failed — offline?) — python-binding diff NOT validated by this lite run; run the full gate" \
  "bindings/python/tests/test_parity.py"
# python test in scope + EMPTY note (tier never set) → GAP (fail-closed default).
assert_python_gap "py-empty-note-gaps" GAP "" \
  "bindings/python/tests/test_value_parity.py" \
  "docs/x.md"
# python test in scope + tier PASS → OK: the tier ran, so the delta proceeds normally.
assert_python_gap "py-pass-ok" OK \
  "python-tier: PASS (maturin develop --profile dev && pytest bindings/python/tests -m 'not slow' -q)" \
  "bindings/python/tests/test_parity.py"
# python test in scope + tier FAIL → OK for the GAP check: a pytest FAIL already sets
# OVERALL=FAIL in run_delta and flows through as RESULT: FAIL, not as this refusal.
assert_python_gap "py-fail-not-a-gap" OK \
  "python-tier: FAIL (pytest failure — a real code failure)" \
  "bindings/python/tests/test_parity.py"
# NO python test file in scope (docs/rust-only) + SKIPPED/empty note → OK: the python
# tier is irrelevant, so python3 being absent must NOT block a docs/rust-only delta.
assert_python_gap "docs-rust-only-no-gap-when-skipped" OK \
  "python-tier: SKIPPED (no python3 on PATH)" \
  "README.md" \
  "cqlite-core/tests/write_read_roundtrip.rs"
assert_python_gap "docs-only-no-gap-empty-note" OK "" \
  "docs/development/pm-operating-loop.md"

# 6. DELTA summary emission + marker distinctness. `--delta <anchor>
#    --emit-summary-selftest` drives the DELTA block through the real emission
#    path (no components). It must carry the DISTINCT delta markers + a MODE:
#    delta line, and must NOT contain the full-gate or lite marker lines — so a
#    delta summary can never be pasted as the gate of record.
delta_file="$tmp/delta-selftest.txt"
AGENT_GATE_SUMMARY_FILE="$delta_file" \
  bash "$GATE" --delta HEAD --emit-summary-selftest >"$tmp/delta.log" 2>&1
delta_rc=$?
if [ "$delta_rc" -eq 0 ]; then
  ok "delta-selftest: exit status 0"
else
  bad "delta-selftest: exit status $delta_rc (expected 0)"
fi
if grep -qF "$DELTA_START" "$delta_file" && grep -qF "$DELTA_END" "$delta_file" \
   && grep -q "^MODE: delta" "$delta_file" && grep -q "^RESULT: " "$delta_file"; then
  ok "delta-selftest: distinct DELTA markers + MODE: delta present in caller-known file"
else
  bad "delta-selftest: missing DELTA markers or MODE line (file: $delta_file)"
  echo "------- captured -------"; cat "$delta_file"; echo "------------------------"
fi
# The delta block MUST NOT carry the full-gate or lite marker LINES (grep -x on
# line boundaries: the full/lite starts are not substrings of the delta start).
if grep -qxF "$FULL_START" "$delta_file"; then
  bad "delta-selftest: block also contains the FULL '$FULL_START' line (must not)"
else
  ok "delta-selftest: block does not contain the full-gate SUMMARY marker line"
fi
if grep -qxF "$LITE_START" "$delta_file"; then
  bad "delta-selftest: block also contains the LITE '$LITE_START' line (must not)"
else
  ok "delta-selftest: block does not contain the lite SUMMARY marker line"
fi
# The MODE line must name the gate of record (the full PASS at the anchor).
if grep -q "gate of record = the full agent-gate.sh PASS at anchor" "$delta_file"; then
  ok "delta-selftest: MODE line names the gate of record (anchor)"
else
  bad "delta-selftest: MODE line does not name the gate of record"
  echo "------- captured -------"; cat "$delta_file"; echo "------------------------"
fi

# 7. Bad anchor → RESULT: ERROR, non-zero exit (a delta cannot re-certify against
#    a commit that does not resolve). Redirect the recovery file into scratch.
bad_anchor_file="$tmp/bad-anchor.txt"
AGENT_GATE_SUMMARY_FILE="$bad_anchor_file" \
  bash "$GATE" --delta __no_such_ref_1892__ >/dev/null 2>&1
bad_anchor_rc=$?
if [ "$bad_anchor_rc" -ne 0 ] && grep -q "^RESULT: ERROR" "$bad_anchor_file"; then
  ok "bad-anchor: non-zero exit ($bad_anchor_rc) + RESULT: ERROR"
else
  bad "bad-anchor: expected non-zero exit + RESULT: ERROR (rc=$bad_anchor_rc)"
  echo "------- captured -------"; cat "$bad_anchor_file" 2>/dev/null; echo "------------------------"
fi

# 8. A --anchor-summary-file that is NOT a full-gate PASS block must be REFUSED
#    (a lite/delta/non-PASS run can never anchor a delta re-cert). All three are
#    hermetic: the guard fires before any component runs. Anchor=HEAD resolves.
mk_block() { printf '\n%s\nrun-id: /tmp/agent-gate.ANCHOR\n%s\n%s\n' "$1" "$2" "$3" >"$4"; }
# 8a. lite block anchor → ERROR
lite_anchor="$tmp/anchor-lite.txt"
mk_block "$LITE_START" "RESULT: PASS" "==== END AGENT-GATE LITE SUMMARY ====" "$lite_anchor"
o8a="$tmp/o8a.txt"
AGENT_GATE_SUMMARY_FILE="$o8a" \
  bash "$GATE" --delta HEAD --anchor-summary-file "$lite_anchor" >/dev/null 2>&1
if [ "$?" -ne 0 ] && grep -q "^RESULT: ERROR" "$o8a"; then
  ok "anchor-lite-rejected: a LITE summary cannot anchor a delta (RESULT: ERROR)"
else
  bad "anchor-lite-rejected: a LITE anchor summary was not rejected"
  echo "------- captured -------"; cat "$o8a" 2>/dev/null; echo "------------------------"
fi
# 8b. delta block anchor → ERROR
delta_anchor="$tmp/anchor-delta.txt"
mk_block "$DELTA_START" "RESULT: PASS" "$DELTA_END" "$delta_anchor"
o8b="$tmp/o8b.txt"
AGENT_GATE_SUMMARY_FILE="$o8b" \
  bash "$GATE" --delta HEAD --anchor-summary-file "$delta_anchor" >/dev/null 2>&1
if [ "$?" -ne 0 ] && grep -q "^RESULT: ERROR" "$o8b"; then
  ok "anchor-delta-rejected: a DELTA summary cannot anchor a delta (RESULT: ERROR)"
else
  bad "anchor-delta-rejected: a DELTA anchor summary was not rejected"
  echo "------- captured -------"; cat "$o8b" 2>/dev/null; echo "------------------------"
fi
# 8c. full block but RESULT: FAIL → ERROR (must anchor to a full PASS)
fail_anchor="$tmp/anchor-full-fail.txt"
mk_block "$FULL_START" "RESULT: FAIL" "==== END AGENT-GATE SUMMARY ====" "$fail_anchor"
o8c="$tmp/o8c.txt"
AGENT_GATE_SUMMARY_FILE="$o8c" \
  bash "$GATE" --delta HEAD --anchor-summary-file "$fail_anchor" >/dev/null 2>&1
if [ "$?" -ne 0 ] && grep -q "^RESULT: ERROR" "$o8c"; then
  ok "anchor-full-fail-rejected: a non-PASS full anchor summary is rejected (RESULT: ERROR)"
else
  bad "anchor-full-fail-rejected: a full FAIL anchor summary was not rejected"
  echo "------- captured -------"; cat "$o8c" 2>/dev/null; echo "------------------------"
fi

# 9. Bash 3.2 compatibility: macOS ships /bin/bash 3.2 and the gate is invoked as
#    plain `bash scripts/agent-gate.sh`. The --delta classification + list paths
#    must not use Bash-4-only features.
if [ -x /bin/bash ]; then
  bin_bash_major=$(/bin/bash -c 'echo "${BASH_VERSINFO[0]}"' 2>/dev/null)
  v=$(printf '%s\n' "cqlite-core/src/lib.rs" | /bin/bash "$GATE" --delta-classify 2>/dev/null \
        | grep -E '^VERDICT: ' | sed 's/^VERDICT: //')
  if /bin/bash "$GATE" --delta-list >/dev/null 2>&1 && [ "$v" = REFUSE ]; then
    ok "bash-compat: --delta classification path runs under /bin/bash (major ${bin_bash_major:-?})"
  else
    bad "bash-compat: --delta classification path failed under /bin/bash (major ${bin_bash_major:-?})"
  fi
fi

# 10. GIT-BACKED end-to-end fail-closed REGRESSION (roborev job 3338): a RENAME of
#     a PRODUCTION file to an allowed *.md path must REFUSE — it must NOT slip a
#     green delta. With git rename detection ON (diff.renames), `git diff
#     --name-only A B` collapses a rename to ONLY the destination path, so
#     classifying by the destination alone would ALLOW (green) while hiding the
#     production-file removal. run_delta passes --no-renames to BOTH diff
#     invocations so a rename enumerates as delete-old + add-new; the old
#     production path is then classified and (non-allowed) triggers the fail-closed
#     REFUSE. This drives the REAL run_delta git enumeration (not the
#     --delta-classify hook): the gate hard-cds to its own repo root, so we copy
#     agent-gate.sh into an ISOLATED temp git repo where the rename lives.
#     Hermetic — the REFUSE fires before any component runs (no cargo), and the
#     real working tree is never touched.
rn_repo="$tmp/rename-repo"
mkdir -p "$rn_repo/scripts"
copy_gate_with_pin "$rn_repo"
(
  cd "$rn_repo" \
    && git init -q \
    && git config user.email t@cqlite.test && git config user.name cqlite-test \
    && git config diff.renames true \
    && mkdir -p docs \
    && printf '#!/usr/bin/env bash\necho production\n' > scripts/deploy.sh \
    && git add -A && git commit -qm anchor
) >/dev/null 2>&1 && rn_ok=1 || rn_ok=0
add_local_origin "$rn_repo"   # #3544 component-set pre-flight baseline
if [ "$rn_ok" = 1 ]; then
  rn_anchor=$(cd "$rn_repo" && git rev-parse HEAD 2>/dev/null)
  ( cd "$rn_repo" && git mv scripts/deploy.sh docs/deploy.md && git commit -qm rename ) >/dev/null 2>&1
  rn_out="$tmp/rename.log"
  ( cd "$rn_repo" && bash scripts/agent-gate.sh --delta "$rn_anchor" ) >"$rn_out" 2>&1
  rn_rc=$?
  # Precondition: with rename detection ON, the naive (renames-enabled) diff must
  # collapse the rename to the destination only (old path absent). If the local git
  # does not collapse it, the --no-renames path is not exercisable → SKIP.
  rn_naive=$(cd "$rn_repo" && git diff --name-only "$rn_anchor" HEAD 2>/dev/null)
  if printf '%s\n' "$rn_naive" | grep -qxF "scripts/deploy.sh"; then
    skip "rename-refuses: git did not collapse the rename (detection unavailable) — --no-renames path not exercisable"
  elif [ "$rn_rc" -ne 0 ] \
    && grep -q "^RESULT: REFUSED" "$rn_out" \
    && grep -qF "scripts/deploy.sh" "$rn_out" \
    && ! grep -q "^RESULT: PASS" "$rn_out"; then
    ok "rename-refuses: production->*.md rename REFUSES (old path enumerated via --no-renames), not a green delta"
  else
    bad "rename-refuses: a production->allowed rename did not REFUSE (rc=$rn_rc)"
    echo "------- captured -------"; cat "$rn_out" 2>/dev/null; echo "------------------------"
  fi
else
  skip "rename-refuses: could not set up temp git repo (git unavailable)"
fi

# 11. ISSUE #2081: --delta EXECUTES node __test__/ + scripts/tests/*.sh.
# 11a. Classification: both new classes ALLOW; a mixed diff with ANY production file
#      still REFUSES (fail-closed unchanged).
assert_verdict "node-only-allows"   ALLOW "bindings/node/__test__/database.test.js"
assert_verdict "shell-only-allows"  ALLOW "scripts/tests/test_agent_gate_summary.sh"
mixed_verdict=$(printf '%s\n' "scripts/tests/test_x.sh" "cqlite-core/src/lib.rs" \
  | bash "$GATE" --delta-classify 2>/dev/null | grep -E '^VERDICT: ' | sed 's/^VERDICT: //')
if [ "$mixed_verdict" = REFUSE ]; then
  ok "node/shell + src mixed diff still REFUSES (fail-closed unchanged)"
else
  bad "mixed shell+src diff should REFUSE (got '$mixed_verdict')"
fi

# 11b. Shell self-test EXECUTOR runs the changed scripts (issue #2081). Drive the
#      SAME executor run_delta uses (via the hidden --delta-run-shell hook) against a
#      committed fixture that drops a sentinel — proving it actually RAN — and a
#      variant that exits non-zero to exercise the FAIL path. Hermetic (no cargo/git).
probe="scripts/tests/fixtures/delta_shell_probe.sh"
sentinel="$tmp/shell-probe-ran"
rm -f "$sentinel"
shell_out=$(printf '%s\n' "$probe" \
  | DELTA_SHELL_PROBE_SENTINEL="$sentinel" bash "$GATE" --delta-run-shell 2>/dev/null)
if [ -f "$sentinel" ] && printf '%s\n' "$shell_out" | grep -qxF "shell-selftest: $probe PASS"; then
  ok "shell-executor: changed scripts/tests/*.sh was EXECUTED (sentinel written) and reported PASS"
else
  bad "shell-executor: fixture did not run or did not report PASS"
  echo "------- out -------"; printf '%s\n' "$shell_out"; echo "sentinel: $(test -f "$sentinel" && echo present || echo absent)"; echo "-------------------"
fi
if printf '%s\n' "$probe" | DELTA_SHELL_PROBE_FAIL=1 bash "$GATE" --delta-run-shell >/dev/null 2>&1; then
  bad "shell-executor: a failing self-test script should make the executor exit non-zero"
else
  ok "shell-executor: a failing self-test script makes the executor FAIL (non-zero exit)"
fi
# A non-scripts/tests/*.sh path passed to the executor is a no-op (not matched).
noop_out=$(printf '%s\n' "docs/x.md" | bash "$GATE" --delta-run-shell 2>/dev/null)
if printf '%s\n' "$noop_out" | grep -qxF "shell-selftest: (none)"; then
  ok "shell-executor: non-scripts/tests path is a no-op (nothing executed)"
else
  bad "shell-executor: expected '(none)' for a non-shell path (got '$noop_out')"
fi

# 11c. Node-build GATE (issue #2081, the load-bearing design point): --delta ALLOWS
#      node __test__/ ONLY when the native module is already built — it must NEVER
#      build with cargo. The hidden --delta-node-ready hook exposes the SAME decision
#      run_delta's up-front refusal consumes.
node_ready=$(bash "$GATE" --delta-node-ready 2>/dev/null | head -1)
case "$node_ready" in
  READY|NOT-READY) ok "node-ready: hook reports a definite build state ($node_ready)" ;;
  *) bad "node-ready: unexpected build-state token '$node_ready'" ;;
esac
# 11d. Node-build REFUSAL end-to-end (fail-closed): in an ISOLATED git repo with a
#      node-__test__-only diff and NO built native module, run_delta must REFUSE
#      (before any executor / cargo), naming the not-built reason — never a vacuous
#      green. Mirrors the rename-refuses harness (copies agent-gate.sh into a temp repo).
nd_repo="$tmp/node-refuse-repo"
mkdir -p "$nd_repo/scripts" "$nd_repo/bindings/node/__test__"
copy_gate_with_pin "$nd_repo"
(
  cd "$nd_repo" \
    && git init -q \
    && git config user.email t@cqlite.test && git config user.name cqlite-test \
    && printf 'test("x", () => {});\n' > bindings/node/__test__/probe.test.js \
    && git add -A && git commit -qm anchor
) >/dev/null 2>&1 && nd_ok=1 || nd_ok=0
add_local_origin "$nd_repo"   # #3544 component-set pre-flight baseline
if [ "$nd_ok" = 1 ]; then
  nd_anchor=$(cd "$nd_repo" && git rev-parse HEAD 2>/dev/null)
  ( cd "$nd_repo" && printf 'test("x", () => { expect(1).toBe(1); });\n' > bindings/node/__test__/probe.test.js \
    && git commit -qam edit ) >/dev/null 2>&1
  nd_out="$tmp/node-refuse.log"
  # No *.node under the temp repo's bindings/node → _delta_node_build_ready is FALSE.
  ( cd "$nd_repo" && bash scripts/agent-gate.sh --delta "$nd_anchor" ) >"$nd_out" 2>&1
  nd_rc=$?
  if [ "$nd_rc" -ne 0 ] \
    && grep -q "^RESULT: REFUSED" "$nd_out" \
    && grep -qi "native module is not built" "$nd_out" \
    && ! grep -q "^RESULT: PASS" "$nd_out"; then
    ok "node-build-refuse: a node __test__ delta with an UNBUILT module REFUSES (fail-closed, no cargo)"
  else
    bad "node-build-refuse: expected RESULT: REFUSED naming the unbuilt module (rc=$nd_rc)"
    echo "------- captured -------"; cat "$nd_out" 2>/dev/null; echo "------------------------"
  fi
else
  skip "node-build-refuse: could not set up temp git repo (git unavailable)"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL  skipped: $SKIP"
[ "$FAIL" -eq 0 ]
