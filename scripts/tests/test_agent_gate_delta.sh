#!/usr/bin/env bash
# Regression test for issue #1892: agent-gate.sh --delta test/docs-only
# RE-CERTIFICATION. After a full-gate PASS at an anchor commit, a diff
# anchor..HEAD that touches ONLY what the re-cert can EXECUTE — rust cargo test
# code (AUTHORITATIVE: a .rs file that IS a Cargo `--test` target scoped-tests
# runs, discovered via cargo metadata — NOT globs; roborev job 3327), python
# binding tests (bindings/python/tests/, run by the #1893 python tier), and/or
# docs (markdown ONLY: *.md anywhere; non-md files under docs/ or website/ are
# REFUSED) — may re-certify with file-size + fmt +
# the changed test targets; ANYTHING else in the diff FAILs closed (a fresh full
# gate is required), including a .rs that is NOT a --test target (nested helper
# mods, src *_test(s).rs, scripts/*.rs, the workspace-excluded fuzz/ crate), node
# __test__/ files, and scripts/tests/*.sh, which --delta's components never
# execute (roborev jobs 1452 / 3327). Because the allow decision is now cargo
# metadata-backed, the ALLOW cases below use REAL existing --test target files
# (invented paths cargo does not know would correctly REFUSE). The delta run
# emits a DISTINCT "==== AGENT-GATE DELTA SUMMARY ====" block (MODE: delta) that
# can never be pasted as a full SUMMARY and names the gate of record (the full
# PASS at the anchor) + the nightly backstop.
#
# Fast + hermetic by design: exercises the load-bearing NEW logic — the
# fail-closed test/docs classification (via the hidden --delta-classify hook),
# the entry-point guards (bad anchor / non-full anchor summary), and the DELTA
# summary emission (via --delta ... --emit-summary-selftest). It NEVER runs
# cargo (no component execution) and NEVER mutates the working tree.
#
# Run standalone:   bash scripts/tests/test_agent_gate_delta.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

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

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-delta-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

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

# 2b. NON-EXECUTABLE test classes → REFUSE (roborev job 1452): --delta's
#     components (file-size, fmt, scoped-tests) never run node jest or the shell
#     self-tests, so an ALLOW here would yield a PASS DELTA block for an
#     untested change. Both must fail closed to the full gate.
assert_verdict "node-test-file-refuses" REFUSE \
  "bindings/node/__test__/database.test.js"
assert_verdict "shell-selftest-refuses" REFUSE \
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

echo "----"
echo "passed: $PASS  failed: $FAIL  skipped: $SKIP"
[ "$FAIL" -eq 0 ]
