#!/usr/bin/env bash
# Comprehensive Smoke Test Script for All Test Tables
#
# Issue #1229: the enforced keyspace set is DISCOVERED dynamically by walking the
# committed corpus under test-data/datasets/sstables/<keyspace>/, NOT hand-typed.
# A newly-committed keyspace is automatically in scope unless it is added to the
# documented SKIP_KEYSPACE_NAMES (excluded) or SKIP_PENDING_KEYSPACES (discovered
# but not executed) below. The skip-set + rationale is the single source of truth
# in test-data/corpus-coverage-policy.md. Discovery is based on directory
# structure (committed), independent of whether *-Data.db binaries are present;
# enforced tables that NEED a Data.db skip-on-absence, but a present-but-empty
# result (0 entries) is still a FAILURE.
#
# Earlier issues that shaped this corpus: #200 (nb), #654/#656 (oa/da), #701
# (test_deltas delete-bearing fixtures).
#
# Tables in SKIP_PENDING_KEYSPACES are discovered and listed, but not run
# through the read-sstable command. They appear explicitly in the summary
# as "SKIP-PENDING" so CI can see them (not silent, not failing).
#
# Test command used for each nb table:
#   cargo run --bin cqlite -- read-sstable <table_dir> --format json
#
# Usage:
#   ./smoke-test-all-tables.sh
#
# Environment Variables:
#   CQLITE_DATASETS_ROOT - Path to datasets directory (default: $PWD/test-data/datasets)
#   CQLITE_CLI           - Path to cqlite binary (optional, will build if not set)
#   OUTPUT_DIR           - Directory for test results (default: ./smoke-test-all-tables-results)

set -euo pipefail
# Production-grade error handling: -e (exit on error), -u (error on unset), -o pipefail (pipeline fails if any command fails)

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test tracking arrays
declare -a PASSED_TABLES=()
declare -a FAILED_TABLES=()
declare -a FAILED_DETAILS=()
declare -a SKIPPED_PENDING_TABLES=()
# Tables whose Data.db is ABSENT in THIS environment's dataset subset. CI ships
# a subset of the full local corpus, so an enforced table dir (committed
# TOC/schema/JSONL) may lack its gitignored Data.db here. Absence => SKIP (not
# FAIL), per the local-only-fixtures-skip-on-presence pattern. A Data.db that
# IS present but yields 0 rows remains a FAILURE.
declare -a SKIPPED_ABSENT_TABLES=()

# Get script directory (resolve symlinks)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Default configuration
DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-${WORKSPACE_ROOT}/test-data/datasets}"
SSTABLES_DIR="${DATASETS_ROOT}/sstables"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/smoke-test-all-tables-results}"

# Issue #1229: the enforced keyspace set is DISCOVERED dynamically by walking
# the committed corpus (test-data/datasets/sstables/<keyspace>/), NOT hand-typed.
# A newly-committed keyspace is automatically in scope unless it is added to
# SKIP_KEYSPACES (documented exclusion) below. The skip-set + rationale is the
# single source of truth in test-data/corpus-coverage-policy.md.
#
# KEYSPACES is populated at runtime by discover_keyspaces() (see below).
KEYSPACES=()

# Skip-set: keyspaces intentionally EXCLUDED from the comprehensive read-parity
# corpus by EXACT name. Each MUST carry a reason (parallel arrays, bash 3.x
# compatible). ALL `system*` keyspaces (system, system_auth, system_schema,
# system_distributed, system_traces, system_views, ...) are excluded separately
# by PREFIX via is_system_keyspace() — do NOT enumerate them here. Mirrored in
# bindings/python/tests/corpus.py (SKIP_KEYSPACES) and
# test-data/corpus-coverage-policy.md.
SKIP_KEYSPACE_NAMES=(
    "test_writeparity" "test_compactionparity" "test_compactionparityudt"
    "test_signed_coll" "test_compaction_tombstone_ttl" "test_comparator_order"
)
SKIP_KEYSPACE_REASONS=(
    "write byte-parity fixtures (dedicated Rust parity tests)"
    "compaction byte-parity fixtures (differential-compaction harness)"
    "compaction-parity UDT fixtures (compaction harness; may be local-only)"
    "signed set/map element-order byte-parity fixtures (dedicated Rust parity test issue_1295_*)"
    "tombstone/TTL compaction byte-parity fixtures (dedicated Rust parity test issue_1387_*)"
    "inet/time multicell-collection ORDERING fixture (dedicated Rust ordering test issue_3790_*); a row-count smoke pass proves nothing about element order"
)

# Return 0 if $1 is a system* keyspace (Cassandra-internal metadata, excluded
# by prefix; not a user-data read-parity target). Mirrors is_system_keyspace()
# in corpus.py and isSystemKeyspace() in parity-utils.js.
is_system_keyspace() {
    [[ "$1" == system* ]]
}

# Skip-pending keyspaces: in-scope (covered by JSONL goldens + the dynamic
# enumeration) but discovered + listed explicitly as SKIP-PENDING rather than
# executed through read-sstable. Reasons differ per keyspace:
#   - test_deltas (#701): Data.db binaries not yet in the published dataset asset.
#   - test_tomb / test_types: delete/tombstone/type-edge parity fixtures that
#     legitimately contain partitions with ZERO live rows (e.g. partition-delete
#     -only, deleted-counter-shadowing). The smoke test's "must emit ≥1 entry"
#     check would mis-flag those valid empty results as failures; these keyspaces
#     are validated by dedicated Rust parity tests (tombstone/TTL + CQL-type),
#     not the read-row-count smoke test.
# Flip an entry to enforced (drop from this list) when its constraint is lifted.
SKIP_PENDING_KEYSPACES=("test_deltas" "test_tomb" "test_types")
# Reason per keyspace (parallel arrays, bash 3.x compatible)
SKIP_PENDING_KEYSPACE_NAMES=("test_deltas" "test_tomb" "test_types")
SKIP_PENDING_KEYSPACE_REASONS=(
    "binaries not in published dataset asset yet (see issue #701 — promote once fetch-datasets.sh pin is bumped)"
    "tombstone parity fixtures with valid zero-live-row partitions; validated by dedicated Rust tombstone/TTL parity tests, not the comprehensive row-count corpus"
    "CQL-type/schema-evolution parity fixtures with valid zero-live-row cases (deleted-counter shadowing); validated by dedicated Rust CQL-type parity tests, not the comprehensive row-count corpus"
)

# Return 0 if $1 is in SKIP_KEYSPACE_NAMES
is_skip_keyspace() {
    local ks="$1" k
    for k in "${SKIP_KEYSPACE_NAMES[@]}"; do
        [[ "$k" == "$ks" ]] && return 0
    done
    return 1
}

# Return 0 if $1 is in SKIP_PENDING_KEYSPACES
is_skip_pending_keyspace() {
    local ks="$1" k
    for k in "${SKIP_PENDING_KEYSPACES[@]}"; do
        [[ "$k" == "$ks" ]] && return 0
    done
    return 1
}

# Committed keyspaces: those with at least one git-tracked file under a table
# dir (Issue #1319/#1312). The classification/enforcement set is the COMMITTED
# corpus, NOT raw live-disk enumeration — an untracked WIP keyspace a concurrent
# session dropped into CQLITE_DATASETS_ROOT (e.g. test_signed_coll, zero tracked
# files) is IGNORED here so it is neither enforced nor flagged by the integrity
# guard. "Committed" is deliberately decoupled from "has a JSONL golden": a
# committed table dir that ships SSTable metadata but is MISSING its golden
# still counts so its absent golden is surfaced loudly (#1229), not silently
# dropped. Populated by compute_committed_keyspaces(); if git is unavailable /
# not a work tree, COMMITTED_KEYSPACES_OK stays 0 and callers fall back to
# treating every discovered keyspace as committed (guard not neutered).
COMMITTED_KEYSPACES=()
# Committed table directories at TABLE granularity (#1319): each entry is
# "keyspace/table-dir" for a dir that owns at least one git-tracked file. Used
# so an untracked WIP table dir under an ALREADY-tracked keyspace is IGNORED,
# not enumerated/enforced. Newline-separated for bash 3.x grep lookups.
COMMITTED_TABLE_DIRS=""
COMMITTED_KEYSPACES_OK=0

compute_committed_keyspaces() {
    COMMITTED_KEYSPACES=()
    COMMITTED_TABLE_DIRS=""
    COMMITTED_KEYSPACES_OK=0
    # The committed corpus is owned by THIS source tree (the repo that contains
    # this script + the corpus-coverage policy), NOT by whatever checkout
    # CQLITE_DATASETS_ROOT points at. A concurrent session can commit WIP
    # fixtures into a *different* checkout's index (e.g. the main repo the
    # datasets root points at) while this branch has not adopted them yet; the
    # classification guard must reflect what THIS branch considers committed.
    # WORKSPACE_ROOT is this script's repo (SCRIPT_DIR/../..).
    local src="${WORKSPACE_ROOT}/test-data/datasets/sstables"
    # Probe that this is a git work tree (a non-repo / missing git is the
    # documented graceful fallback, not an empty committed set).
    if ! git -C "${src}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        return
    fi
    # Do NOT enable committed-corpus filtering yet: rev-parse success only proves
    # this is a work tree, not that any file is actually tracked. If ls-files
    # errors or returns zero records (rev-parse OK but nothing tracked), an
    # empty committed set would treat EVERY keyspace as uncommitted and fail with
    # "no in-scope keyspaces". Mirror the Python/Node fallback: keep
    # COMMITTED_KEYSPACES_OK=0 (treat all discovered as committed) until the loop
    # below has parsed at least one tracked table dir.
    local path ks tabledir seen="" tdseen=""
    # Single git ls-files call rooted at the source tree, NO pathspec so ANY
    # tracked file (Data.db, TOC, Statistics, a JSONL golden, ...) marks the dir
    # committed (#1312 — committed must NOT require a tracked golden, else a
    # committed dir missing its golden is silently dropped instead of failing
    # the coverage check loudly). Path layout is <keyspace>/<table-dir>/<file>;
    # first segment is the keyspace, first two are the committed table dir. -z
    # keeps it NUL-delimited (newline-safe); read NUL records straight from the
    # pipe — capturing NUL output in $(...) strips the NULs.
    while IFS= read -r -d '' path; do
        ks="${path%%/*}"
        [[ -z "${ks}" ]] && continue
        # Skip paths that are not at least <keyspace>/<table-dir>/<file> (e.g. a
        # tracked file directly under sstables/ or under a keyspace dir): they do
        # not identify a committed table dir.
        tabledir="${path%/*}"
        [[ "${tabledir}" == */* ]] || continue
        # A tracked file under a table dir exists -> enable committed-corpus
        # filtering. Until this fires (zero qualifying records / ls-files error),
        # the fallback stays engaged.
        COMMITTED_KEYSPACES_OK=1
        case " ${seen} " in
            *" ${ks} "*) ;;
            *) seen="${seen} ${ks}"; COMMITTED_KEYSPACES+=("${ks}");;
        esac
        # "keyspace/table-dir" = strip the trailing "/file" segment (computed
        # above as ${tabledir}).
        case $'\n'"${tdseen}"$'\n' in
            *$'\n'"${tabledir}"$'\n'*) ;;
            *) tdseen="${tdseen}${tabledir}"$'\n'; COMMITTED_TABLE_DIRS="${COMMITTED_TABLE_DIRS}${tabledir}"$'\n';;
        esac
    done < <(git -C "${src}" ls-files -z 2>/dev/null)
}

# Return 0 if $1 is a committed keyspace (git-tracked file), or if git was
# unavailable (COMMITTED_KEYSPACES_OK=0 => fall back to "all discovered count").
is_committed_keyspace() {
    [[ ${COMMITTED_KEYSPACES_OK} -eq 0 ]] && return 0
    local ks="$1" k
    for k in "${COMMITTED_KEYSPACES[@]}"; do
        [[ "$k" == "$ks" ]] && return 0
    done
    return 1
}

# Return 0 if "keyspace/table-dir" ($1) owns a git-tracked file (TABLE
# granularity, #1319), or if git was unavailable (COMMITTED_KEYSPACES_OK=0 =>
# fall back to treating every discovered table dir as committed). An untracked
# WIP table dir under an already-tracked keyspace returns 1 (IGNORED).
is_committed_table_dir() {
    [[ ${COMMITTED_KEYSPACES_OK} -eq 0 ]] && return 0
    local td="$1"
    case $'\n'"${COMMITTED_TABLE_DIRS}" in
        *$'\n'"${td}"$'\n'*) return 0;;
        *) return 1;;
    esac
}

# Discover the enforced keyspace set by walking the committed corpus.
# In-scope = every COMMITTED keyspace dir (git-tracked file, #1319) minus
# SKIP_KEYSPACE_NAMES minus SKIP_PENDING. Based on directory structure
# (committed), independent of Data.db presence.
discover_keyspaces() {
    KEYSPACES=()
    local dir ks
    while IFS= read -r dir; do
        ks="$(basename "${dir}")"
        is_system_keyspace "${ks}" && continue
        is_committed_keyspace "${ks}" || continue
        is_skip_keyspace "${ks}" && continue
        is_skip_pending_keyspace "${ks}" && continue
        KEYSPACES+=("${ks}")
    done < <(find "${SSTABLES_DIR}" -mindepth 1 -maxdepth 1 -type d | sort)
}

# Get skip reason for a keyspace (bash 3.x compatible, no associative arrays)
get_skip_reason() {
    local ks="$1"
    local i
    for i in "${!SKIP_PENDING_KEYSPACE_NAMES[@]}"; do
        if [[ "${SKIP_PENDING_KEYSPACE_NAMES[$i]}" == "$ks" ]]; then
            echo "${SKIP_PENDING_KEYSPACE_REASONS[$i]}"
            return
        fi
    done
    echo "pending"
}

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $*"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

# Detect timeout command (GNU timeout or macOS gtimeout)
detect_timeout_command() {
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD="timeout 30s"
    elif command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_CMD="gtimeout 30s"  # From homebrew coreutils on macOS
    else
        TIMEOUT_CMD=""
        log_warn "timeout command not found - tests may hang indefinitely"
        log_warn "On macOS, install with: brew install coreutils"
    fi
}

# Validate environment
validate_environment() {
    log_info "Validating environment..."

    if [[ ! -d "${SSTABLES_DIR}" ]]; then
        log_error "SSTables directory not found: ${SSTABLES_DIR}"
        log_error "Set CQLITE_DATASETS_ROOT to the correct path or run from workspace root"
        exit 1
    fi

    # Issue #1319: compute the COMMITTED keyspace set (git-tracked files) so
    # discovery + the integrity guard ignore untracked WIP keyspaces.
    compute_committed_keyspaces

    # Issue #1229: discover the enforced keyspace set from disk.
    discover_keyspaces

    if [[ ${#KEYSPACES[@]} -eq 0 ]]; then
        log_error "No in-scope keyspaces discovered under ${SSTABLES_DIR}"
        log_error "(every keyspace is in the skip-set or skip-pending — check the corpus)"
        exit 1
    fi

    # Integrity guard: every COMMITTED keyspace must be classified — either
    # in-scope (enforced), skip-pending, or in the documented skip-set. A new
    # committed keyspace that is none of these reds the smoke test loudly
    # instead of being silently uncovered while CI reports "100%". Issue #1319:
    # the guard enumerates the COMMITTED corpus (git-tracked files), NOT raw
    # live-disk enumeration, so an untracked WIP keyspace (e.g. test_signed_coll,
    # zero tracked files) is IGNORED — neither enforced nor flagged.
    local unclassified=()
    local dir ks
    while IFS= read -r dir; do
        ks="$(basename "${dir}")"
        if is_system_keyspace "${ks}" || is_skip_keyspace "${ks}" || is_skip_pending_keyspace "${ks}"; then
            continue
        fi
        # Ignore untracked WIP keyspaces (no git-tracked file) — #1319.
        is_committed_keyspace "${ks}" || continue
        # in-scope (enforced) keyspaces are exactly KEYSPACES by construction
        local found=0 k
        for k in "${KEYSPACES[@]}"; do
            [[ "$k" == "$ks" ]] && { found=1; break; }
        done
        [[ ${found} -eq 0 ]] && unclassified+=("${ks}")
    done < <(find "${SSTABLES_DIR}" -mindepth 1 -maxdepth 1 -type d | sort)

    if [[ ${#unclassified[@]} -gt 0 ]]; then
        log_error "Unclassified committed keyspace(s): ${unclassified[*]}"
        log_error "Add them to SKIP_KEYSPACE_NAMES (with a reason) or accept them"
        log_error "as in-scope. See test-data/corpus-coverage-policy.md."
        exit 1
    fi

    # Warn (but do not fail) if skip-pending keyspaces are absent
    for keyspace in "${SKIP_PENDING_KEYSPACES[@]}"; do
        if [[ ! -d "${SSTABLES_DIR}/${keyspace}" ]]; then
            log_warn "Skip-pending keyspace not present (OK): ${keyspace}"
        fi
    done

    log_success "Environment validation passed"
    log_info "  SSTables directory: ${SSTABLES_DIR}"
    log_info "  Discovered in-scope keyspaces: ${KEYSPACES[*]}"
}

# Build or locate CLI binary
setup_cli_binary() {
    if [[ -n "${CQLITE_CLI:-}" ]]; then
        if [[ ! -x "${CQLITE_CLI}" ]]; then
            log_error "CQLITE_CLI is set but not executable: ${CQLITE_CLI}"
            exit 1
        fi
        log_info "Using CLI binary from CQLITE_CLI: ${CQLITE_CLI}"
        return
    fi

    # Try to find built binary first
    local dev_binary="${WORKSPACE_ROOT}/target/debug/cqlite"
    local release_binary="${WORKSPACE_ROOT}/target/release/cqlite"

    if [[ -x "${release_binary}" ]]; then
        CQLITE_CLI="${release_binary}"
        log_info "Using existing release binary: ${CQLITE_CLI}"
        return
    fi

    if [[ -x "${dev_binary}" ]]; then
        CQLITE_CLI="${dev_binary}"
        log_info "Using existing debug binary: ${CQLITE_CLI}"
        return
    fi

    # Build the CLI
    log_info "Building CLI binary..."
    cd "${WORKSPACE_ROOT}"
    local build_output
    build_output=$(mktemp)

    # Build CLI binary and capture output
    if cargo build --package cqlite-cli --bin cqlite --quiet 2>&1 | tee "${build_output}"; then
        CQLITE_CLI="${dev_binary}"
        log_success "CLI binary built successfully: ${CQLITE_CLI}"
        rm -f "${build_output}"
    else
        log_error "Failed to build CLI binary"
        cat "${build_output}"
        rm -f "${build_output}"
        exit 1
    fi
}

# Setup test environment
setup_test_environment() {
    log_info "Setting up test environment..."

    # Validate OUTPUT_DIR is safe (defense in depth)
    if [[ -z "${OUTPUT_DIR}" || "${OUTPUT_DIR}" == "/" || "${OUTPUT_DIR}" == "${HOME}" ]]; then
        log_error "Invalid or unsafe OUTPUT_DIR: ${OUTPUT_DIR}"
        exit 1
    fi

    # Create output directory
    mkdir -p "${OUTPUT_DIR}"

    # Clean previous test results (safely - directory validated above)
    if [[ -d "${OUTPUT_DIR}" ]]; then
        rm -f "${OUTPUT_DIR}"/*.json 2>/dev/null || true
    fi

    log_success "Test environment ready (output: ${OUTPUT_DIR})"
}

# Extract table name from directory (remove UUID suffix)
# Args: table_dir_name
extract_table_name() {
    local dir_name="$1"
    # Remove UUID suffix pattern: -XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    echo "${dir_name}" | sed 's/-[0-9a-f]\{32\}$//'
}

# Discover all table directories in all test keyspaces
# Returns array of "keyspace/table_dir" paths
discover_tables() {
    local tables=()

    for keyspace in "${KEYSPACES[@]}"; do
        local keyspace_dir="${SSTABLES_DIR}/${keyspace}"

        if [[ ! -d "${keyspace_dir}" ]]; then
            log_warn "Keyspace directory not found: ${keyspace_dir}"
            continue
        fi

        # Find all table directories (directories containing Data.db files).
        # Filter to the COMMITTED corpus at TABLE granularity (#1319): an
        # untracked WIP <table>-<uuid>/ dir (no git-tracked file) under an
        # already-tracked keyspace is IGNORED, not enforced.
        while IFS= read -r table_dir; do
            local table_dir_name
            table_dir_name=$(basename "${table_dir}")
            is_committed_table_dir "${keyspace}/${table_dir_name}" || continue
            tables+=("${keyspace}/${table_dir_name}")
        done < <(find "${keyspace_dir}" -maxdepth 1 -type d -name "*-*" | sort)
    done

    printf '%s\n' "${tables[@]}"
}

# Test a single table
# Args: keyspace/table_dir
test_table() {
    local table_path="$1"
    local keyspace
    keyspace=$(dirname "${table_path}")
    local table_dir_name
    table_dir_name=$(basename "${table_path}")
    local table_name
    table_name=$(extract_table_name "${table_dir_name}")

    local full_table_path="${SSTABLES_DIR}/${table_path}"
    local qualified_name="${keyspace}.${table_name}"

    # Find Data.db file
    # Exclude macOS AppleDouble resource fork sidecar files (._*-Data.db) which are
    # 4 KB metadata files that look like SSTables to a naive *-Data.db glob (Issue #481).
    local data_db_file
    data_db_file=$(find "${full_table_path}" -name "*-Data.db" -type f -not -name "._*" | head -1)

    if [[ -z "${data_db_file}" ]]; then
        # Data.db ABSENT => this fixture is not in THIS environment's dataset
        # subset (CI ships a subset; local has the full set). SKIP it — do NOT
        # fail. This keeps smoke robust to any subset while preserving the
        # parity-is-truth rule: a PRESENT Data.db yielding 0 rows still FAILs
        # below. See the local-only-fixtures-skip-on-presence pattern
        # (cf. test_da/wide_table, test_big.wide_partition).
        log_warn "${qualified_name} ... SKIP (no Data.db in this dataset subset)"
        SKIPPED_ABSENT_TABLES+=("${qualified_name}")
        return 0
    fi

    # Find corresponding JSONL file
    local jsonl_file
    jsonl_file=$(find "${full_table_path}" -name "*.jsonl" -type f | head -1)

    local output_file="${OUTPUT_DIR}/${keyspace}_${table_name}.json"
    local exit_code=0

    # Run read-sstable command with Data.db file directly
    # Use timeout if available to prevent hangs, suppress stderr (Issue #129: logs go to stderr)
    set +e
    if [[ -n "${TIMEOUT_CMD}" ]]; then
        ${TIMEOUT_CMD} "${CQLITE_CLI}" read-sstable "${data_db_file}" --format json > "${output_file}" 2>/dev/null
        exit_code=$?
    else
        # No timeout available - run without it
        "${CQLITE_CLI}" read-sstable "${data_db_file}" --format json > "${output_file}" 2>/dev/null
        exit_code=$?
    fi
    set -e

    # Check for timeout (exit code 124 for GNU timeout, 143 for some implementations)
    if [[ -n "${TIMEOUT_CMD}" && ( ${exit_code} -eq 124 || ${exit_code} -eq 143 ) ]]; then
        log_error "${qualified_name} ... FAIL (timeout after 30s)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: Command timed out after 30 seconds")
        return 1
    fi

    # Test 1: Check exit code
    if [[ ${exit_code} -ne 0 ]]; then
        log_error "${qualified_name} ... FAIL (exit code: ${exit_code})"
        FAILED_TABLES+=("${qualified_name}")
        # Store simple failure message (detailed output available in ${output_file})
        FAILED_DETAILS+=("${qualified_name}: Exit code ${exit_code}, see ${output_file}")
        return 1  # Early return on failure
    fi

    # Test 2: Validate output contains JSON (at least one '{')
    set +e
    grep -q '{' "${output_file}"
    local grep_result=$?
    set -e

    if [[ ${grep_result} -ne 0 ]]; then
        log_error "${qualified_name} ... FAIL (no JSON output)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: Output does not contain valid JSON objects")
        return 1
    fi

    # Test 3: Validate we got some data
    # Note: Row count comparison is skipped because JSONL format (sstabledump)
    # represents partitions (one line per partition with nested rows), while
    # read-sstable JSON output represents individual entries. The formats are
    # incompatible for direct line count comparison.
    local entry_count
    set +e
    entry_count=$(grep -c '^  {' "${output_file}")
    local grep_exit=$?
    set -e
    # grep -c returns 1 if no matches, which is fine
    if [[ ${grep_exit} -gt 1 ]]; then
        entry_count=0
    fi

    if [[ ${entry_count} -eq 0 ]]; then
        log_error "${qualified_name} ... FAIL (no entries found in output)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: No entries found in JSON output")
        return 1
    fi

    # Success - table loaded and produced entries
    if [[ -n "${jsonl_file}" && -f "${jsonl_file}" ]]; then
        local partition_count
        set +e
        partition_count=$(wc -l < "${jsonl_file}" | tr -d ' ')
        set -e
        log_success "${qualified_name} ... PASS (${entry_count} entries, ${partition_count} partitions in reference)"
    else
        log_warn "${qualified_name} ... PASS (${entry_count} entries, no JSONL reference)"
    fi

    PASSED_TABLES+=("${qualified_name}")
    return 0
}

# Discover and register skip-pending tables (oa, da)
# These are listed in the summary but not run through read-sstable.
register_skip_pending_tables() {
    for keyspace in "${SKIP_PENDING_KEYSPACES[@]}"; do
        local keyspace_dir="${SSTABLES_DIR}/${keyspace}"
        if [[ ! -d "${keyspace_dir}" ]]; then
            continue
        fi
        while IFS= read -r table_dir; do
            local table_dir_name
            table_dir_name=$(basename "${table_dir}")
            # Ignore untracked WIP table dirs (no git-tracked file) — #1319.
            is_committed_table_dir "${keyspace}/${table_dir_name}" || continue
            local table_name
            table_name=$(extract_table_name "${table_dir_name}")
            local qualified_name="${keyspace}.${table_name}"
            local reason
            reason=$(get_skip_reason "${keyspace}")
            log_warn "${qualified_name} ... SKIP-PENDING (${reason})"
            SKIPPED_PENDING_TABLES+=("${qualified_name} [${reason}]")
        done < <(find "${keyspace_dir}" -maxdepth 1 -type d -name "*-*" | sort)
    done
}

# Run all table tests
run_all_tests() {
    log_info "Discovering test tables..."

    local tables=()
    while IFS= read -r table_path; do
        tables+=("${table_path}")
    done < <(discover_tables)

    local total_tables=${#tables[@]}

    if [[ ${total_tables} -eq 0 ]]; then
        log_error "No test tables discovered in ${SSTABLES_DIR}"
        exit 1
    fi

    log_info "Found ${total_tables} tables across ${#KEYSPACES[@]} keyspaces"
    echo ""

    log_info "Starting table loading tests..."
    echo ""

    # Test each table (continue on failure to test all tables)
    # Temporarily disable errexit for the entire loop to allow failures
    set +e
    for table_path in "${tables[@]}"; do
        test_table "${table_path}" || true  # Continue even if test fails
    done
    set -e

    echo ""
    log_info "Checking skip-pending keyspaces: ${SKIP_PENDING_KEYSPACES[*]} (discovered + listed, not executed)..."
    echo ""
    register_skip_pending_tables

    echo ""
    log_info "All table tests completed"
}

# Print comprehensive test summary
print_summary() {
    local total_tables=$((${#PASSED_TABLES[@]} + ${#FAILED_TABLES[@]}))

    echo ""
    echo "========================================="
    echo "    SMOKE TEST SUMMARY - ALL TABLES"
    echo "========================================="
    echo ""
    echo "  Enforced keyspaces (discovered): ${KEYSPACES[*]}"
    echo "  Skip-pending keyspaces:          ${SKIP_PENDING_KEYSPACES[*]}"
    echo "  Total Enforced Tables Tested:    ${total_tables} (= passed + failed; denominator derived from disk, not hard-coded)"
    echo -e "  ${GREEN}Passed:              ${#PASSED_TABLES[@]}${NC}"

    if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
        echo -e "  ${RED}Failed:              ${#FAILED_TABLES[@]}${NC}"
    else
        echo "  Failed:              ${#FAILED_TABLES[@]}"
    fi

    if [[ ${#SKIPPED_PENDING_TABLES[@]} -gt 0 ]]; then
        echo -e "  ${YELLOW}Skip-pending:        ${#SKIPPED_PENDING_TABLES[@]} (${SKIP_PENDING_KEYSPACES[*]} - discovered but not executed; see corpus-coverage-policy.md)${NC}"
    fi

    if [[ ${#SKIPPED_ABSENT_TABLES[@]} -gt 0 ]]; then
        echo -e "  ${YELLOW}Skipped (no Data.db): ${#SKIPPED_ABSENT_TABLES[@]} (enforced tables whose Data.db is absent in this dataset subset)${NC}"
    fi

    echo ""
    echo "  Output Directory:    ${OUTPUT_DIR}"
    echo ""

    # List failed tables with details if any
    if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
        echo -e "${RED}Failed Tables:${NC}"
        echo ""
        for detail in "${FAILED_DETAILS[@]}"; do
            echo -e "${RED}  • ${detail}${NC}"
        done
        echo ""
    fi

    # List skip-pending tables
    if [[ ${#SKIPPED_PENDING_TABLES[@]} -gt 0 ]]; then
        echo -e "${YELLOW}Skip-Pending Tables (fixtures present, parser not yet wired):${NC}"
        echo ""
        for entry in "${SKIPPED_PENDING_TABLES[@]}"; do
            echo -e "${YELLOW}  • ${entry}${NC}"
        done
        echo ""
    fi

    # List tables skipped because their Data.db is absent in this dataset subset
    if [[ ${#SKIPPED_ABSENT_TABLES[@]} -gt 0 ]]; then
        echo -e "${YELLOW}Skipped Tables (Data.db absent in this dataset subset; enforced where present):${NC}"
        echo ""
        for entry in "${SKIPPED_ABSENT_TABLES[@]}"; do
            echo -e "${YELLOW}  • ${entry}${NC}"
        done
        echo ""
    fi

    # Issue #1312 (fast-follow to #1229): a dataset-dependent test must NEVER
    # report success on an empty dataset. validate_environment() already exits
    # non-zero when NO in-scope keyspaces are discovered (case (a): corpus
    # genuinely absent). By the time we reach here the enforced corpus
    # (${KEYSPACES[*]}) is non-empty by construction, so if NOTHING passed and
    # NOTHING failed then every enforced table was skipped because its Data.db
    # is absent (case (b): a broken/empty dataset asset — every Data.db missing).
    # That is NOT a pass; fail loudly instead of printing "All 0 ... passed".
    # The #1229 per-fixture skip-on-absence (case (c): partial subset) is
    # preserved: as long as at least one enforced Data.db was present and passed,
    # PASSED_TABLES is non-empty and we return success, while any PRESENT
    # Data.db yielding 0 rows still lands in FAILED_TABLES below.
    if [[ ${#FAILED_TABLES[@]} -eq 0 && ${#PASSED_TABLES[@]} -eq 0 ]]; then
        echo -e "${RED}=========================================${NC}"
        echo -e "${RED}  Empty/broken dataset: 0 enforced tables ran${NC}"
        echo -e "${RED}  ${#SKIPPED_ABSENT_TABLES[@]} enforced table(s) were skipped because NO Data.db is present${NC}"
        echo -e "${RED}  Enforced keyspaces (discovered from disk): ${KEYSPACES[*]}${NC}"
        echo -e "${RED}  A dataset-dependent smoke run must not pass with zero present fixtures.${NC}"
        echo -e "${RED}  Fetch the corpus (test-data/scripts/fetch-datasets.sh) or fix the dataset asset.${NC}"
        echo -e "${RED}=========================================${NC}"
        return 1
    elif [[ ${#FAILED_TABLES[@]} -eq 0 ]]; then
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}  All ${#PASSED_TABLES[@]} enforced tables passed smoke test${NC}"
        echo -e "${GREEN}  Enforced keyspaces (discovered from disk): ${KEYSPACES[*]}${NC}"
        echo -e "${GREEN}  Skip-pending: ${SKIP_PENDING_KEYSPACES[*]} (see corpus-coverage-policy.md)${NC}"
        echo -e "${GREEN}=========================================${NC}"
        return 0
    else
        echo -e "${RED}=========================================${NC}"
        echo -e "${RED}  ${#FAILED_TABLES[@]} enforced table(s) failed${NC}"
        echo -e "${RED}=========================================${NC}"
        return 1
    fi
}

# Main execution
main() {
    log_info "CQLite Comprehensive Table Loading Smoke Test"
    log_info "Issue #1229: enforced keyspaces are DISCOVERED from the committed"
    log_info "  corpus (no hand-typed allowlist); skip-set + reasons in"
    log_info "  test-data/corpus-coverage-policy.md"
    echo ""

    detect_timeout_command
    validate_environment
    setup_cli_binary
    setup_test_environment

    echo ""
    log_info "Configuration:"
    log_info "  CLI Binary:         ${CQLITE_CLI}"
    log_info "  Datasets Root:      ${DATASETS_ROOT}"
    log_info "  SSTables Directory: ${SSTABLES_DIR}"
    log_info "  Output Directory:   ${OUTPUT_DIR}"
    log_info "  Enforced Keyspaces: ${KEYSPACES[*]}"
    log_info "  Skip-Pending:       ${SKIP_PENDING_KEYSPACES[*]}"
    echo ""

    # Run all tests (continue on error to collect all results)
    set +e
    run_all_tests
    set -e

    # Print summary and exit with appropriate code
    if print_summary; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
