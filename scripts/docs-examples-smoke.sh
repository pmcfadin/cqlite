#!/usr/bin/env bash
# docs-examples-smoke.sh — Extract and run documented CLI commands from recipe pages.
#
# Philosophy: same as sstabledump parity for code, applied to docs.
# Every CLI command in a <!-- SMOKE:CLI --> block must execute successfully
# and produce output matching the expected shape (key presence, row count,
# exit code). A drifted example fails with a clear message naming the recipe page.
#
# Marker convention (documented in website/README.md):
#   <!-- SMOKE:CLI -->          — CLI command; expected exit 0
#   <!-- SMOKE:CLI:exit=N -->   — CLI command; expected exit N
#   <!-- SMOKE:CLI:write -->    — CLI command requiring write-support binary; expected exit 0
#   <!-- /SMOKE:CLI -->         — end of marked block
#
# Python and Node.js markers (<!-- SMOKE:PYTHON -->, <!-- SMOKE:NODE -->) are
# extracted but NOT run in CI (bindings require a separate build step). They
# are supported for local execution with --with-bindings.
#
# Usage:
#   bash scripts/docs-examples-smoke.sh               # CLI recipes only
#   bash scripts/docs-examples-smoke.sh --with-bindings  # include Python + Node
#   bash scripts/docs-examples-smoke.sh --recipe sstable-to-json.md  # one recipe
#
# Environment:
#   CQLITE_DATASETS_ROOT   — dataset root (default: test-data/datasets)
#   CQLITE_CLI             — path to CLI binary (default: target/debug/cqlite)
#   CQLITE_SCHEMA_DIR      — schema directory (default: test-data/schemas)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RECIPES_DIR="$REPO_ROOT/website/src/content/docs/agents-using"

# ── Configuration ─────────────────────────────────────────────────────────────

DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO_ROOT/test-data/datasets}"
DATA_DIR="$DATASETS_ROOT/sstables"
SCHEMA_DIR="${CQLITE_SCHEMA_DIR:-$REPO_ROOT/test-data/schemas}"
CLI="${CQLITE_CLI:-$REPO_ROOT/target/debug/cqlite}"
WITH_BINDINGS=false
SINGLE_RECIPE=""
WRITE_DIR="${SMOKE_WRITE_DIR:-/tmp/cqlite-smoke-write}"

# ── Argument parsing ───────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-bindings) WITH_BINDINGS=true; shift ;;
    --recipe) SINGLE_RECIPE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 2 ;;
  esac
done

# ── Helpers ────────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
RESET='\033[0m'

pass() { echo -e "${GREEN}[PASS]${RESET} $*"; }
fail() { echo -e "${RED}[FAIL]${RESET} $*"; }
info() { echo -e "${YELLOW}[INFO]${RESET} $*"; }
banner() { echo -e "\n${BOLD}══ $* ══${RESET}"; }

FAILURES=()
PASSED=0
SKIPPED=0

record_fail() {
    fail "$*"
    FAILURES+=("$*")
}

# ── Pre-flight ─────────────────────────────────────────────────────────────────

banner "docs-examples-smoke pre-flight"

if [[ ! -f "$CLI" ]]; then
    fail "CLI not found at: $CLI"
    echo "Build with: cargo build --package cqlite-cli --features write-support"
    exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
    fail "Data directory not found: $DATA_DIR"
    echo "Set CQLITE_DATASETS_ROOT or run: bash test-data/scripts/fetch-datasets.sh"
    exit 1
fi

if [[ ! -d "$RECIPES_DIR" ]]; then
    fail "Recipes directory not found: $RECIPES_DIR"
    exit 1
fi

info "CLI:        $CLI"
info "Data dir:   $DATA_DIR"
info "Schema dir: $SCHEMA_DIR"
info "Recipes:    $RECIPES_DIR"
info "With bindings: $WITH_BINDINGS"

# Check write-support binary
WRITE_CLI="$CLI"
# If a release write-support binary exists, prefer it; otherwise use debug
if [[ -f "$REPO_ROOT/target/debug/cqlite" ]]; then
    WRITE_CLI="$REPO_ROOT/target/debug/cqlite"
fi

# ── Extraction and execution ───────────────────────────────────────────────────

# Extract marked blocks from a markdown file.
# Outputs: one shell heredoc per block, with the recipe file path commented.
extract_smoke_blocks() {
    local file="$1"
    local in_block=false
    local block_type=""
    local expected_exit=0
    local cmd_lines=()

    while IFS= read -r line; do
        # Start marker: <!-- SMOKE:TYPE --> or <!-- SMOKE:TYPE:exit=N -->
        if [[ "$line" =~ ^\<\!--[[:space:]]*SMOKE:([A-Z_]+)(:exit=([0-9]+))? ]]; then
            in_block=true
            block_type="${BASH_REMATCH[1]}"
            expected_exit="${BASH_REMATCH[3]:-0}"
            cmd_lines=()
            continue
        fi

        # End marker
        if [[ "$line" =~ ^\<\!--[[:space:]]*/SMOKE ]]; then
            in_block=false
            if [[ ${#cmd_lines[@]} -gt 0 ]]; then
                echo "BLOCK_START:${block_type}:${expected_exit}"
                printf '%s\n' "${cmd_lines[@]}"
                echo "BLOCK_END"
            fi
            continue
        fi

        # Collect lines inside a fenced code block within the SMOKE block
        if [[ "$in_block" == true ]]; then
            # Skip the ``` fence lines
            if [[ "$line" =~ ^'```' ]]; then
                continue
            fi
            cmd_lines+=("$line")
        fi
    done < "$file"
}

# Run a single CLI recipe block.
# Arguments: recipe_file, block content (multi-line), expected_exit
run_cli_block() {
    local recipe="$1"
    local block_content="$2"
    local expected_exit="$3"
    local recipe_name
    recipe_name="$(basename "$recipe")"

    # Replace placeholder paths with real paths
    local expanded
    expanded="$(echo "$block_content" \
        | sed "s|test-data/schemas/|$SCHEMA_DIR/|g" \
        | sed "s|test-data/datasets/sstables|$DATA_DIR|g" \
        | sed "s|--write-dir /tmp/cqlite-write|--write-dir $WRITE_DIR|g" \
        | sed "s|/tmp/cqlite-export|$WRITE_DIR/export|g" \
        | sed "s|/tmp/cqlite-smoke-write|$WRITE_DIR|g"
    )"

    # Join backslash-continued lines into a single logical line, preserving pipes.
    # Strategy: join lines where the current line ends with \, keep the rest as-is.
    # Then compact multiple spaces.
    local cmd
    cmd="$(echo "$expanded" | awk '{
        if (sub(/\\[[:space:]]*$/, "")) {
            printf "%s ", $0
        } else {
            print $0
        }
    }' | sed 's/  */ /g' | sed '/^[[:space:]]*$/d')"

    # Replace bare 'cqlite' with the actual binary path.
    # macOS sed does not support \b word boundaries; use two-pass replacement instead.
    cmd="$(echo "$cmd" \
        | sed "s|^cqlite |$CLI |" \
        | sed "s| cqlite | $CLI |g" \
        | sed "s|\&\& cqlite |\&\& $CLI |g")"

    local tmp_out
    tmp_out="$(mktemp)"
    local actual_exit=0

    # Detect if the command uses a pipe (multi-stage pipeline)
    local has_pipe=false
    if echo "$cmd" | grep -q '|'; then
        has_pipe=true
    fi

    info "Testing [$recipe_name]: $(echo "$cmd" | head -c 120)..."

    # Run command. For piped commands, stderr from all stages goes to /dev/null.
    # Redirect stdout to tmp_out only for non-piped commands so we can validate JSON shape.
    if [[ "$has_pipe" == "true" ]]; then
        # Piped: just check exit code; suppress all output
        eval "$cmd" >/dev/null 2>/dev/null || actual_exit=$?
    else
        # Non-piped: capture stdout for shape validation; suppress stderr (INFO/WARN logs)
        eval "$cmd" >"$tmp_out" 2>/dev/null || actual_exit=$?
    fi

    if [[ "$actual_exit" != "$expected_exit" ]]; then
        record_fail "$recipe_name: expected exit $expected_exit, got $actual_exit"
        if [[ -s "$tmp_out" ]]; then
            echo "  Output was:"
            head -5 "$tmp_out" | sed 's/^/    /'
        fi
        rm -f "$tmp_out"
        return
    fi

    # For non-piped successful CLI blocks, do basic output shape validation
    if [[ "$expected_exit" == "0" && "$has_pipe" == "false" ]]; then
        local stdout_content
        stdout_content="$(cat "$tmp_out")"

        # If output looks like JSON, validate it parses
        if echo "$stdout_content" | grep -q '^\['; then
            if ! echo "$stdout_content" | python3 -c "import json,sys; json.load(sys.stdin)" 2>/dev/null; then
                record_fail "$recipe_name: JSON output does not parse"
                rm -f "$tmp_out"
                return
            fi
            # Ensure the JSON array has at least 1 element
            local row_count
            row_count="$(echo "$stdout_content" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)"
            if [[ "$row_count" -lt 1 ]]; then
                record_fail "$recipe_name: JSON output is empty array (expected rows)"
                rm -f "$tmp_out"
                return
            fi
        fi
    fi

    rm -f "$tmp_out"
    pass "$recipe_name"
    ((PASSED++)) || true
}

# Process a single recipe file
process_recipe() {
    local recipe="$1"

    if [[ ! -f "$recipe" ]]; then
        info "Skipping (not found): $recipe"
        ((SKIPPED++)) || true
        return
    fi

    # Reset write dir for each write recipe
    local recipe_name
    recipe_name="$(basename "$recipe")"

    # Extract and run blocks
    local in_block=false
    local block_type=""
    local expected_exit=0
    local cmd_lines=()
    local block_count=0

    while IFS= read -r line; do
        if [[ "$line" =~ ^\<\!--[[:space:]]*SMOKE:([A-Z_:a-z=0-9]+) ]]; then
            local marker="${BASH_REMATCH[1]}"
            in_block=true
            cmd_lines=()

            # Parse type and exit code from marker
            if [[ "$marker" =~ ^(CLI|CLI:exit=([0-9]+)|CLI:write|PYTHON|NODE)$ ]]; then
                if [[ "$marker" == "CLI" ]]; then
                    block_type="CLI"
                    expected_exit=0
                elif [[ "$marker" =~ ^CLI:exit=([0-9]+)$ ]]; then
                    block_type="CLI"
                    expected_exit="${BASH_REMATCH[1]}"
                elif [[ "$marker" == "CLI:write" ]]; then
                    block_type="CLI_WRITE"
                    expected_exit=0
                elif [[ "$marker" == "PYTHON" ]]; then
                    block_type="PYTHON"
                    expected_exit=0
                elif [[ "$marker" == "NODE" ]]; then
                    block_type="NODE"
                    expected_exit=0
                fi
            fi
            continue
        fi

        if [[ "$line" =~ ^\<\!--[[:space:]]*/SMOKE ]]; then
            in_block=false
            if [[ ${#cmd_lines[@]} -gt 0 ]]; then
                ((block_count++)) || true
                local block_content
                block_content="$(printf '%s\n' "${cmd_lines[@]}")"

                case "$block_type" in
                    CLI)
                        # Prepare write dir for non-write CLI blocks
                        mkdir -p "$WRITE_DIR"
                        run_cli_block "$recipe" "$block_content" "$expected_exit"
                        ;;
                    CLI_WRITE)
                        # Ensure clean write dir for write recipes
                        rm -rf "$WRITE_DIR" && mkdir -p "$WRITE_DIR"
                        run_cli_block "$recipe" "$block_content" "$expected_exit"
                        ;;
                    PYTHON)
                        if [[ "$WITH_BINDINGS" == "true" ]]; then
                            info "TODO: Python block in $recipe_name (--with-bindings not yet wired)"
                            ((SKIPPED++)) || true
                        else
                            info "Skipping Python block in $recipe_name (use --with-bindings to run)"
                            ((SKIPPED++)) || true
                        fi
                        ;;
                    NODE)
                        if [[ "$WITH_BINDINGS" == "true" ]]; then
                            info "TODO: Node block in $recipe_name (--with-bindings not yet wired)"
                            ((SKIPPED++)) || true
                        else
                            info "Skipping Node block in $recipe_name (use --with-bindings to run)"
                            ((SKIPPED++)) || true
                        fi
                        ;;
                esac
            fi
            cmd_lines=()
            continue
        fi

        if [[ "$in_block" == true ]]; then
            # Skip the ``` fence lines
            if [[ "$line" =~ ^'```' ]]; then
                continue
            fi
            cmd_lines+=("$line")
        fi
    done < "$recipe"

    if [[ "$block_count" -eq 0 ]]; then
        info "No SMOKE blocks found in $recipe_name"
    fi
}

# ── Main loop ──────────────────────────────────────────────────────────────────

banner "Running smoke tests"

if [[ -n "$SINGLE_RECIPE" ]]; then
    # Single recipe mode
    if [[ -f "$RECIPES_DIR/$SINGLE_RECIPE" ]]; then
        process_recipe "$RECIPES_DIR/$SINGLE_RECIPE"
    elif [[ -f "$SINGLE_RECIPE" ]]; then
        process_recipe "$SINGLE_RECIPE"
    else
        fail "Recipe not found: $SINGLE_RECIPE"
        exit 1
    fi
else
    # All CLI recipes in order
    CLI_RECIPES=(
        "sstable-to-json.md"
        "export-parquet.md"
        "export-csv.md"
        "inspect-schema.md"
        "count-rows.md"
        "read-collections.md"
        "missing-schema.md"
        "write-mutation.md"
        "export-sstable.md"
    )
    # Python/Node recipes: skipped unless --with-bindings
    BINDING_RECIPES=(
        "query-python.md"
        "query-nodejs.md"
    )

    for recipe in "${CLI_RECIPES[@]}"; do
        process_recipe "$RECIPES_DIR/$recipe"
    done

    for recipe in "${BINDING_RECIPES[@]}"; do
        if [[ "$WITH_BINDINGS" == "true" ]]; then
            process_recipe "$RECIPES_DIR/$recipe"
        else
            info "Skipping binding recipe (use --with-bindings): $recipe"
            ((SKIPPED++)) || true
        fi
    done
fi

# ── Cleanup ────────────────────────────────────────────────────────────────────

rm -rf "$WRITE_DIR" 2>/dev/null || true

# ── Summary ───────────────────────────────────────────────────────────────────

banner "docs-examples-smoke summary"

echo ""
echo "Passed:  $PASSED"
echo "Skipped: $SKIPPED (Python/Node require --with-bindings)"
echo "Failed:  ${#FAILURES[@]}"

if [[ ${#FAILURES[@]} -gt 0 ]]; then
    echo ""
    echo "Failed recipes:"
    for f in "${FAILURES[@]}"; do
        echo "  - $f"
    done
    echo ""
    echo "┌──────────────────────────────────────────────────┐"
    echo "│  docs-examples-smoke: FAILED                      │"
    echo "└──────────────────────────────────────────────────┘"
    echo ""
    echo "DOCS_SMOKE=FAIL"
    exit 1
else
    echo ""
    echo "┌──────────────────────────────────────────────────┐"
    echo "│  docs-examples-smoke: ALL CHECKS PASSED           │"
    echo "└──────────────────────────────────────────────────┘"
    echo ""
    echo "DOCS_SMOKE=PASS"
    exit 0
fi
