#!/usr/bin/env bash
# Guard: test-data/scripts/ci-one-shot-smoke.sh must run its WHOLE suite even
# when a test fails (issue #3689).
#
# The defect this pins: run_test/run_error_test wrapped a deliberately-unchecked
# command in `set +e` ... `set -e`. Shell options are GLOBAL, not
# function-scoped, so the trailing `set -e` clobbered main()'s deliberate
# `set +e` ("continue on error to collect all results"). The first `return 1`
# out of run_test then aborted the entire script, so only the FIRST failing
# test was ever reported and every test after it never ran at all.
#
# Measured cost of that on main: the suite ran 2 of 9 tests (json PASS,
# csv FAIL, abort). A second golden (select_simple_table.golden) was stale in
# exactly the same way and stayed invisible for months behind the abort.
#
# This runs the REAL shipped script against a stub CLI -- never a local model of
# it -- because a re-implementation of the harness would only test the model.
# It needs no cargo, no dataset corpus and no network.
#
# It carries its own POSITIVE CONTROL: the same scenario is re-run against a
# scratch copy with the defect planted back in, and that copy MUST abort. A
# guard that cannot be made to fail is not evidence, so if the planted copy
# also completes, this test FAILs and says so.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SUBJECT="${REPO_ROOT}/test-data/scripts/ci-one-shot-smoke.sh"

FAILURES=0
fail() { echo "FAIL: $*" >&2; FAILURES=$((FAILURES + 1)); }
pass() { echo "ok: $*"; }

if [[ ! -f "${SUBJECT}" ]]; then
    echo "FAIL: subject not found: ${SUBJECT}" >&2
    exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT INT TERM HUP

# ---------------------------------------------------------------------------
# Hermetic fixture: stub CLI + fake schema/data dir + goldens.
# ---------------------------------------------------------------------------
mkdir -p "${WORK}/data" "${WORK}/schemas" "${WORK}/goldens" "${WORK}/out"

# validate_environment only checks that a *-Data.db exists; content is unused
# because the CLI is a stub.
touch "${WORK}/data/fake-Data.db"
echo "CREATE TABLE test_basic.simple_table (id uuid PRIMARY KEY);" > "${WORK}/schemas/basic-types.cql"
# Test 5 is skipped unless a collections.cql sits beside the schema.
echo "CREATE TABLE test_collections.collection_table (id uuid PRIMARY KEY);" > "${WORK}/schemas/collections.cql"

cat > "${WORK}/stub-cqlite" <<'STUB'
#!/usr/bin/env bash
# Deterministic stand-in for the cqlite CLI. Reproduces only what the smoke
# harness asserts: exit codes, error substrings, and per-format output.
schema=""; datadir=""; query=""; format=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --schema)   schema="$2";  shift 2 ;;
        --data-dir) datadir="$2"; shift 2 ;;
        --dataset)  datadir="$2"; shift 2 ;;
        --execute)  query="$2";   shift 2 ;;
        --format)   format="$2";  shift 2 ;;
        *) shift ;;
    esac
done

# Error cases the harness drives deliberately.
if [[ ! -f "${schema}" ]]; then
    echo "error: schema file not found: ${schema}" >&2
    echo "error: schema file not found: ${schema}"
    exit 3
fi
if [[ ! -d "${datadir}" ]]; then
    echo "error: data directory not found: ${datadir}"
    exit 4
fi
if [[ "${query}" == *"invalid syntax"* ]]; then
    echo "error: invalid query syntax"
    exit 3
fi

case "${format}" in
    json)  printf '[{"id": 1}]\n' ;;
    csv)   printf 'id\n1\n' ;;
    table) printf ' id \n----\n  1 \n' ;;
    *)     printf 'unknown-format\n' ;;
esac
exit 0
STUB
chmod +x "${WORK}/stub-cqlite"

# Goldens matching the stub, EXCEPT the CSV one, which is deliberately wrong so
# test 2 of 9 fails -- the same position as the real #3689 failure.
printf '[{"id": 1}]\n'      > "${WORK}/goldens/select_simple_json.golden"
printf 'id\nDELIBERATELY-WRONG\n' > "${WORK}/goldens/select_simple_csv.golden"
printf ' id \n----\n  1 \n' > "${WORK}/goldens/select_simple_table.golden"
printf '[{"id": 1}]\n'      > "${WORK}/goldens/select_columns_json.golden"
printf '[{"id": 1}]\n'      > "${WORK}/goldens/select_collections_json.golden"

run_suite() {
    # $1 = script to run, $2 = output dir. Echoes combined output; returns rc.
    #
    # `env -i` + an explicit allowlist, NOT a bare `env` (roborev job 3). A bare
    # `env` inherits the caller's environment, and the subject branches on
    # CQLITE_DATASET: if the invoking shell exports it -- which a lane box
    # routinely does, and which this repo's own docs tell you to export -- the
    # subject silently switches to dataset mode, resolves CQLITE_DATASETS_ROOT
    # to the REAL corpus, and stops exercising the fixture entirely. The guard
    # would then pass or fail for a reason that has nothing to do with #3689,
    # and it would do so only on some machines.
    #
    # Allowlist rather than `env -u CQLITE_DATASET ...` so that a NEW variable
    # the subject learns to read is cleared BY DEFAULT instead of needing to be
    # discovered here (the #3544 rule, one directory over).
    local script="$1" outdir="$2"
    mkdir -p "${outdir}"
    env -i \
        PATH="${PATH}" \
        HOME="${HOME}" \
        CQLITE_CLI="${WORK}/stub-cqlite" \
        CQLITE_SCHEMA="${WORK}/schemas/basic-types.cql" \
        CQLITE_DATA_DIR="${WORK}/data" \
        GOLDEN_DIR="${WORK}/goldens" \
        OUTPUT_DIR="${outdir}" \
        bash "${script}" 2>&1
    return $?
}

strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

# ---------------------------------------------------------------------------
# Case 1 -- THE PROPERTY: a failing test must not abort the suite.
# ---------------------------------------------------------------------------
subject_out="$(run_suite "${SUBJECT}" "${WORK}/out/subject" | strip_ansi)"
subject_rc=$?

if grep -q 'test_select_csv_simple: Output does not match snapshot' <<<"${subject_out}"; then
    pass "fixture drives the intended failure at test 2"
else
    fail "fixture did not produce the intended CSV mismatch -- the scenario is not being exercised"
    echo "--- subject output ---" >&2
    echo "${subject_out}" >&2
fi

# Every test AFTER the failing one must still have run.
for later in test_select_table_simple test_select_columns test_select_collections \
             test_error_invalid_query test_query_nonexistent_table; do
    if grep -q "Running test: ${later}\|${later}: " <<<"${subject_out}"; then
        pass "${later} ran after the earlier failure"
    else
        fail "${later} did NOT run -- the suite aborted early (#3689 regression)"
    fi
done

# The summary block is only reached if the suite did not abort.
tests_run="$(grep -oE 'Tests Run: +[0-9]+' <<<"${subject_out}" | grep -oE '[0-9]+' | tail -1)"
if [[ -z "${tests_run}" ]]; then
    fail "no summary block -- the suite aborted before print_summary (#3689 regression)"
elif [[ "${tests_run}" -lt 9 ]]; then
    fail "summary reports only ${tests_run} tests run; expected 9 (suite aborted early)"
else
    pass "summary reports all ${tests_run} tests run"
fi

# A suite with a failing test must still exit non-zero.
if [[ "${subject_rc}" -eq 0 ]]; then
    fail "suite exited 0 despite a failing test"
else
    pass "suite exited non-zero (${subject_rc}) with a failing test"
fi

# ---------------------------------------------------------------------------
# Case 2 -- POSITIVE CONTROL: plant the defect; the copy MUST abort.
# Without this, a green Case 1 could mean the scenario never failed at all.
# ---------------------------------------------------------------------------
planted="${WORK}/planted-smoke.sh"
cp "${SUBJECT}" "${planted}"
# Reintroduce the exact defect: restore errexit unconditionally.
python3 - "${planted}" <<'PLANT'
import re, sys
p = sys.argv[1]
s = open(p).read()
new_body = 'errexit_restore() {\n    set -e\n}'
s2 = re.sub(r'errexit_restore\(\) \{.*?\n\}', new_body, s, count=1, flags=re.S)
if s2 == s:
    sys.stderr.write("PLANT-FAILED: could not rewrite errexit_restore\n")
    sys.exit(1)
open(p, 'w').write(s2)
PLANT
if [[ $? -ne 0 ]]; then
    fail "could not plant the defect -- the positive control did not run, so Case 1 proves nothing"
else
    planted_out="$(run_suite "${planted}" "${WORK}/out/planted" | strip_ansi)"
    planted_run="$(grep -oE 'Tests Run: +[0-9]+' <<<"${planted_out}" | grep -oE '[0-9]+' | tail -1)"
    if [[ -z "${planted_run}" ]]; then
        pass "positive control: planted defect aborts the suite before the summary"
    else
        fail "positive control did NOT abort (reported ${planted_run} tests run) -- this guard cannot detect the #3689 defect and is not evidence"
    fi
fi

# ---------------------------------------------------------------------------
# Case 3 -- the save/restore pair must be NESTABLE.
#
# main() wraps run_test_suite, which wraps each run_test, so these calls nest
# three deep. A first cut parked the saved state in one global: each inner save
# overwrote it, so main's restore reinstated the INNER state (off) instead of
# the script's initial `set -e`. Caught in review (roborev job 2 on PR #3773).
# The state is therefore passed explicitly and held in a `local` per caller.
# ---------------------------------------------------------------------------

# Unit: errexit_restore must honour its ARGUMENT, in both directions.
# Extracted from the shipped script so unrouting it here reds the suite.
(
    eval "$(sed -n '/^errexit_restore() {/,/^}/p' "${SUBJECT}")"
    if ! declare -F errexit_restore >/dev/null; then
        echo "EXTRACT-FAILED"
        exit 1
    fi
    set +e; errexit_restore on
    case $- in *e*) echo "on->on ok" ;; *) echo "on->on BAD" ;; esac
    set -e; errexit_restore off
    case $- in *e*) echo "off->off BAD" ;; *) echo "off->off ok" ;; esac
    set -e; errexit_restore
    case $- in *e*) echo "noarg BAD" ;; *) echo "noarg ok" ;; esac
) > "${WORK}/restore-unit.txt" 2>&1
unit="$(cat "${WORK}/restore-unit.txt")"
if grep -q 'EXTRACT-FAILED' <<<"${unit}"; then
    fail "could not extract errexit_restore from the shipped script -- its contract is untested"
elif grep -q 'BAD' <<<"${unit}"; then
    fail "errexit_restore does not honour its argument: ${unit}"
else
    pass "errexit_restore honours its argument (on/off/absent)"
fi

# Structural: the saved state must be per-caller, never one shared global --
# that is what makes the nesting safe.
if grep -qE '^\s*(ERREXIT_PREV=|errexit_save\b)' "${SUBJECT}"; then
    fail "the shipped script still uses a shared ERREXIT_PREV/errexit_save -- nested save/restore would clobber it (#3689)"
else
    pass "no shared errexit global in the shipped script"
fi

save_sites="$(grep -cE '^\s*case \$- in \*e\*\) errexit_prev=on ;; esac' "${SUBJECT}")"
local_decls="$(grep -cE '^\s*local errexit_prev=off' "${SUBJECT}")"
restore_calls="$(grep -cE '^\s*errexit_restore "\$\{errexit_prev\}"' "${SUBJECT}")"
if [[ "${save_sites}" -gt 0 && "${save_sites}" -eq "${local_decls}" && "${save_sites}" -eq "${restore_calls}" ]]; then
    pass "all ${save_sites} save sites declare a local and pass it back to errexit_restore"
else
    fail "save/restore sites are not balanced: ${save_sites} saves, ${local_decls} locals, ${restore_calls} explicit restores"
fi

# ---------------------------------------------------------------------------
# Case 4 -- HERMETICITY: an exported CQLITE_DATASET must not change the run.
#
# Positive control for the env leak found in review (roborev job 3). Exporting
# CQLITE_DATASET is normal on a fleet box, and it makes the subject take its
# dataset-mode branch against the real corpus. If the allowlist above ever
# regresses to a bare `env`, this case sees a different run and fails.
# ---------------------------------------------------------------------------
poisoned_out="$(
    export CQLITE_DATASET=test_basic
    export CQLITE_DATASETS_ROOT=/nonexistent/should/be/ignored
    export CQLITE_DATA_DIR=/nonexistent/should/be/ignored
    run_suite "${SUBJECT}" "${WORK}/out/poisoned" | strip_ansi
)"

if grep -q 'Using dataset mode' <<<"${poisoned_out}"; then
    fail "an exported CQLITE_DATASET leaked into the subject -- it ran in dataset mode, not against the fixture (roborev job 3)"
else
    pass "exported CQLITE_DATASET does not leak into the subject"
fi

poisoned_run="$(grep -oE 'Tests Run: +[0-9]+' <<<"${poisoned_out}" | grep -oE '[0-9]+' | tail -1)"
if [[ "${poisoned_run:-0}" == "${tests_run:-x}" ]]; then
    pass "poisoned environment produces the same run (${poisoned_run} tests)"
else
    fail "poisoned environment changed the run: ${poisoned_run:-<none>} tests vs ${tests_run:-<none>} clean"
fi

echo
if [[ "${FAILURES}" -eq 0 ]]; then
    echo "PASS: ci-one-shot-smoke.sh runs its whole suite through a failing test (#3689)"
    exit 0
fi
echo "FAILED: ${FAILURES} check(s)" >&2
exit 1
