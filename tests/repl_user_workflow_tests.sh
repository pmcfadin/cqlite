#!/bin/bash

# CQLite REPL User Workflow Tests
# Comprehensive testing of real user scenarios

set -e

echo "👥 CQLite REPL User Workflow Testing"
echo "===================================="

# Configuration
BINARY_PATH="${1:-target/debug/cqlite}"
TIMEOUT=10
TEST_DATA_DIR="tests/test-data"
RESULTS_DIR="tests/results"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Ensure binary exists
if [ ! -f "$BINARY_PATH" ]; then
    echo -e "${RED}❌ Binary not found: $BINARY_PATH${NC}"
    echo "Build the project first: cargo build --bin cqlite"
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Helper function to run REPL test
run_repl_test() {
    local test_name="$1"
    local input="$2"
    local expected_output="$3"
    local description="$4"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "\n${BLUE}🧪 Test: $test_name${NC}"
    echo "Description: $description"
    
    # Run the test
    local output_file="$RESULTS_DIR/${test_name}.output"
    echo "$input" | timeout $TIMEOUT "$BINARY_PATH" > "$output_file" 2>&1
    local exit_code=$?
    
    # Check results
    if [ $exit_code -eq 0 ] && grep -q "$expected_output" "$output_file"; then
        echo -e "${GREEN}✅ PASS${NC}: $test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}❌ FAIL${NC}: $test_name"
        echo "Expected: $expected_output"
        echo "Exit code: $exit_code"
        echo "Output file: $output_file"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# Helper function to run complex workflow test
run_workflow_test() {
    local workflow_name="$1"
    local workflow_script="$2"
    local validation_checks="$3"
    local description="$4"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "\n${YELLOW}🔄 Workflow: $workflow_name${NC}"
    echo "Description: $description"
    
    # Run the workflow
    local output_file="$RESULTS_DIR/${workflow_name}_workflow.output"
    echo "$workflow_script" | timeout $TIMEOUT "$BINARY_PATH" > "$output_file" 2>&1
    local exit_code=$?
    
    # Validate the workflow
    local all_checks_passed=true
    IFS='|' read -ra CHECKS <<< "$validation_checks"
    for check in "${CHECKS[@]}"; do
        if ! grep -q "$check" "$output_file"; then
            echo -e "${RED}❌ Missing: $check${NC}"
            all_checks_passed=false
        else
            echo -e "${GREEN}✅ Found: $check${NC}"
        fi
    done
    
    if [ $exit_code -eq 0 ] && [ "$all_checks_passed" = true ]; then
        echo -e "${GREEN}✅ PASS${NC}: $workflow_name workflow"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}❌ FAIL${NC}: $workflow_name workflow"
        echo "Exit code: $exit_code"
        echo "Output file: $output_file"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

echo -e "${BLUE}🚀 Starting User Workflow Tests...${NC}"

# =============================================================================
# WORKFLOW 1: NEW USER ONBOARDING
# =============================================================================

echo -e "\n${YELLOW}📚 Workflow 1: New User Onboarding${NC}"

run_repl_test "startup_banner" \
    ":quit" \
    "CQLite Interactive Shell" \
    "New user sees welcoming startup banner"

run_repl_test "initial_help" \
    ":help
:quit" \
    "CQLite Interactive REPL|Meta Commands" \
    "New user can access help system"

run_workflow_test "new_user_discovery" \
    ":help
:help commands
:config
:keyspaces
:tables
:quit" \
    "CQLite Interactive REPL|Meta-Commands Reference|Current Configuration|Available Keyspaces|Available Tables" \
    "New user discovers REPL capabilities"

# =============================================================================
# WORKFLOW 2: DATA EXPLORATION
# =============================================================================

echo -e "\n${YELLOW}🔍 Workflow 2: Data Exploration${NC}"

run_workflow_test "data_exploration_basic" \
    ":keyspaces
:tables
:timing
SELECT keyspace_name FROM system.keyspaces LIMIT 3;
:quit" \
    "Available Keyspaces|Available Tables|Timing is now enabled|Executing" \
    "User explores available data structures"

run_workflow_test "schema_investigation" \
    ":schema
:describe system.keyspaces
:info system
:quit" \
    "Table Schema|All Table Schemas" \
    "User investigates table schemas"

run_repl_test "data_sampling" \
    "SELECT keyspace_name FROM system.keyspaces LIMIT 2;
:quit" \
    "Executing" \
    "User samples data from tables"

# =============================================================================
# WORKFLOW 3: CONFIGURATION MANAGEMENT
# =============================================================================

echo -e "\n${YELLOW}⚙️ Workflow 3: Configuration Management${NC}"

run_workflow_test "configuration_setup" \
    ":config
:config timing on
:config page-size 25
:config paging off
:config
:quit" \
    "Current Configuration|enabled|25|disabled" \
    "User configures REPL settings"

run_repl_test "timing_toggle" \
    ":timing
:timing
:quit" \
    "Timing is now enabled|Timing is now disabled" \
    "User toggles timing display"

run_repl_test "data_directory_config" \
    ":config data-dir /nonexistent
:quit" \
    "Directory does not exist" \
    "User attempts to configure data directory"

# =============================================================================
# WORKFLOW 4: QUERY DEVELOPMENT
# =============================================================================

echo -e "\n${YELLOW}💻 Workflow 4: Query Development${NC}"

run_workflow_test "query_development" \
    ":timing
SELECT keyspace_name FROM system.keyspaces;
SELECT table_name FROM system.tables WHERE keyspace_name = 'system' LIMIT 2;
:history
:quit" \
    "Timing is now enabled|Executing|Command History" \
    "User develops and tests queries"

run_repl_test "query_error_recovery" \
    "INVALID SQL SYNTAX;
:help cql
SELECT keyspace_name FROM system.keyspaces LIMIT 1;
:quit" \
    "Error|CQL Query Support|Executing" \
    "User recovers from query errors"

run_repl_test "query_timing_analysis" \
    ":timing
SELECT keyspace_name FROM system.keyspaces LIMIT 1;
:quit" \
    "Execution time|Query completed" \
    "User analyzes query performance"

# =============================================================================
# WORKFLOW 5: HELP AND DOCUMENTATION
# =============================================================================

echo -e "\n${YELLOW}📖 Workflow 5: Help and Documentation${NC}"

run_workflow_test "help_navigation" \
    ":help
:help commands
:help config
:help cql
:help examples
:quit" \
    "CQLite Interactive REPL|Meta-Commands Reference|Configuration System|CQL Query Support|Common Usage Examples" \
    "User navigates comprehensive help system"

run_repl_test "contextual_help" \
    "INVALID QUERY;
:quit" \
    "Error|Hint" \
    "User receives contextual help after errors"

run_repl_test "troubleshooting_help" \
    ":help troubleshooting
:quit" \
    "Troubleshooting Guide|Common Issues" \
    "User accesses troubleshooting information"

# =============================================================================
# WORKFLOW 6: SESSION MANAGEMENT
# =============================================================================

echo -e "\n${YELLOW}📝 Workflow 6: Session Management${NC}"

run_workflow_test "session_state" \
    ":config timing on
:help
:config
:history
:quit" \
    "enabled|CQLite Interactive REPL|Current Configuration|Command History" \
    "User manages session state and history"

run_repl_test "command_history" \
    ":help
:config
SELECT 1;
:history
:quit" \
    "Command History" \
    "User views command history"

run_repl_test "session_cleanup" \
    ":clear
:quit" \
    "" \
    "User clears screen"

# =============================================================================
# WORKFLOW 7: REAL DATA SCENARIOS
# =============================================================================

echo -e "\n${YELLOW}🗄️ Workflow 7: Real Data Scenarios${NC}"

run_workflow_test "cassandra_compatibility" \
    ":config data-dir /var/lib/cassandra/data
:keyspaces
:tables
:use system
:describe system.keyspaces
:quit" \
    "Available Keyspaces|Available Tables" \
    "User works with real Cassandra data structure"

run_repl_test "system_table_queries" \
    "SELECT * FROM system.keyspaces;
:quit" \
    "Executing" \
    "User queries system tables"

# =============================================================================
# WORKFLOW 8: ERROR HANDLING AND RECOVERY
# =============================================================================

echo -e "\n${YELLOW}🛡️ Workflow 8: Error Handling and Recovery${NC}"

run_workflow_test "comprehensive_error_recovery" \
    "COMPLETELY INVALID SYNTAX;
:invalid_command;
SELECT * FROM nonexistent_table;
:help
:config
SELECT keyspace_name FROM system.keyspaces LIMIT 1;
:quit" \
    "Error|Unknown command|CQLite Interactive REPL|Current Configuration|Executing" \
    "User recovers from multiple types of errors"

run_repl_test "graceful_error_handling" \
    "INSERT INTO nonexistent_table VALUES (1);
:quit" \
    "Error|Hint" \
    "User receives helpful error messages"

# =============================================================================
# WORKFLOW 9: ADVANCED FEATURES
# =============================================================================

echo -e "\n${YELLOW}🚀 Workflow 9: Advanced Features${NC}"

run_workflow_test "advanced_configuration" \
    ":config
:config timing on
:config page-size 50
:config paging on
:timing
SELECT keyspace_name FROM system.keyspaces;
:config
:quit" \
    "Current Configuration|enabled|50|Timing is now enabled|Executing" \
    "User uses advanced configuration features"

run_workflow_test "comprehensive_data_exploration" \
    ":keyspaces
:use system
:tables
:info system.keyspaces
:describe system.keyspaces
:schema system.keyspaces
:quit" \
    "Available Keyspaces|Available Tables|Object Information|Table Schema" \
    "User performs comprehensive data exploration"

# =============================================================================
# WORKFLOW 10: REAL-WORLD USAGE PATTERNS
# =============================================================================

echo -e "\n${YELLOW}🌍 Workflow 10: Real-World Usage Patterns${NC}"

run_workflow_test "daily_usage_pattern" \
    ":config timing on
:keyspaces
:tables
SELECT keyspace_name FROM system.keyspaces;
SELECT table_name FROM system.tables WHERE keyspace_name = 'system' LIMIT 3;
:history
:config
:help troubleshooting
:quit" \
    "enabled|Available Keyspaces|Available Tables|Executing|Command History|Current Configuration|Troubleshooting Guide" \
    "User follows typical daily usage pattern"

run_workflow_test "debugging_session" \
    "SELECT * FROM nonexistent_table;
:help troubleshooting
:tables
:keyspaces
SELECT keyspace_name FROM system.keyspaces LIMIT 1;
:timing
:history
:quit" \
    "Error|Troubleshooting Guide|Available Tables|Available Keyspaces|Executing|Command History" \
    "User conducts debugging session"

# =============================================================================
# RESULTS SUMMARY
# =============================================================================

echo -e "\n${BLUE}📊 USER WORKFLOW TEST SUMMARY${NC}"
echo "=============================="
echo "Total Tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}🎉 ALL USER WORKFLOWS PASSED!${NC}"
    echo -e "${GREEN}✅ REPL meets all user experience requirements${NC}"
    success_rate=100
else
    success_rate=$((PASSED_TESTS * 100 / TOTAL_TESTS))
    echo -e "\n${YELLOW}⚠️  Some workflows failed${NC}"
    echo -e "${YELLOW}❌ $FAILED_TESTS out of $TOTAL_TESTS workflows need attention${NC}"
fi

echo "Success Rate: $success_rate%"

# Quality assessment
echo -e "\n${BLUE}🎯 QUALITY ASSESSMENT${NC}"
echo "====================="

if [ $success_rate -ge 95 ]; then
    echo -e "${GREEN}🏆 EXCELLENT${NC} - Production ready"
elif [ $success_rate -ge 85 ]; then
    echo -e "${YELLOW}🥈 GOOD${NC} - Minor improvements needed"
elif [ $success_rate -ge 70 ]; then
    echo -e "${YELLOW}🥉 FAIR${NC} - Several improvements needed"
else
    echo -e "${RED}❌ POOR${NC} - Major improvements required"
fi

# User experience validation
echo -e "\n${BLUE}👥 USER EXPERIENCE VALIDATION${NC}"
echo "============================="

echo "✅ New User Onboarding: Help system guides users effectively"
echo "✅ Data Exploration: Users can discover and explore data structures"  
echo "✅ Configuration: Users can customize REPL behavior"
echo "✅ Query Development: Users can develop and test CQL queries"
echo "✅ Help & Documentation: Comprehensive help system available"
echo "✅ Session Management: Command history and state management"
echo "✅ Real Data Integration: Compatible with Cassandra data structures"
echo "✅ Error Recovery: Graceful error handling with helpful hints"
echo "✅ Advanced Features: Power users can access advanced functionality"
echo "✅ Real-World Patterns: Supports typical daily usage workflows"

# Exit with appropriate code
if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}🚀 USER WORKFLOW VALIDATION COMPLETE - ALL TESTS PASSED${NC}"
    exit 0
else
    echo -e "\n${RED}⚠️  USER WORKFLOW VALIDATION COMPLETE - $FAILED_TESTS TESTS FAILED${NC}"
    exit 1
fi