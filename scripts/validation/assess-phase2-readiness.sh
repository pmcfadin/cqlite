#!/bin/bash
# Phase 2 Readiness Assessment Script
# Comprehensive evaluation of Phase 2 readiness after Phase 1 completion

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Scoring system
declare -A SCORES
declare -A WEIGHTS
declare -A CRITERIA

# Initialize scoring
TOTAL_SCORE=0
MAX_SCORE=0

# Configuration
READINESS_THRESHOLD=90  # 90% required for Phase 2 approval

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_critical() {
    echo -e "${RED}[CRITICAL]${NC} $1"
}

log_section() {
    echo -e "\n${PURPLE}=== $1 ===${NC}"
}

log_subsection() {
    echo -e "${CYAN}--- $1 ---${NC}"
}

# Scoring functions
add_score() {
    local category="$1"
    local score="$2"
    local weight="$3"
    local description="$4"
    
    SCORES["$category"]="$score"
    WEIGHTS["$category"]="$weight"
    CRITERIA["$category"]="$description"
    
    local weighted_score=$((score * weight))
    TOTAL_SCORE=$((TOTAL_SCORE + weighted_score))
    MAX_SCORE=$((MAX_SCORE + (100 * weight)))
    
    local status_icon="✅"
    if [ "$score" -lt 60 ]; then
        status_icon="❌"
    elif [ "$score" -lt 80 ]; then
        status_icon="⚠️"
    fi
    
    echo -e "  ${status_icon} ${description}: ${score}/100 (weight: ${weight}x)"
}

# Run Phase 1 validation first
validate_phase1_completion() {
    log_section "Phase 2 Readiness: Phase 1 Validation Check"
    
    local script_path="./scripts/validation/validate-phase1-complete.sh"
    
    if [ ! -f "$script_path" ]; then
        log_critical "Phase 1 validation script not found: $script_path"
        add_score "phase1_validation" 0 5 "Phase 1 completion validation"
        return 1
    fi
    
    log_info "Running Phase 1 completion validation..."
    
    if bash "$script_path" >/dev/null 2>&1; then
        log_success "✅ Phase 1 validation PASSED"
        add_score "phase1_validation" 100 5 "Phase 1 completion validation"
        return 0
    else
        log_error "❌ Phase 1 validation FAILED"
        log_critical "Phase 2 readiness assessment CANNOT proceed"
        add_score "phase1_validation" 0 5 "Phase 1 completion validation"
        return 1
    fi
}

# Assess build system reliability
assess_build_reliability() {
    log_section "Phase 2 Readiness: Build System Reliability"
    
    local build_score=0
    local consistency_score=0
    
    log_subsection "Multi-target compilation test"
    
    # Test different build targets
    local targets=("--lib" "--bins" "--examples" "--tests")
    local successful_targets=0
    
    for target in "${targets[@]}"; do
        if timeout 300 cargo check $target --workspace >/dev/null 2>&1; then
            ((successful_targets++))
            log_info "✅ Build target $target: OK"
        else
            log_warning "⚠️  Build target $target: FAILED"
        fi
    done
    
    build_score=$((successful_targets * 100 / ${#targets[@]}))
    
    log_subsection "Cross-platform compatibility check"
    
    # Check for platform-specific code issues
    local platform_issues=0
    
    # Look for potential platform issues in code
    if grep -r "std::process::Command" --include="*.rs" src/ >/dev/null 2>&1; then
        log_info "Platform-specific code detected (std::process::Command)"
        ((platform_issues++))
    fi
    
    if grep -r "std::path::Path" --include="*.rs" src/ >/dev/null 2>&1; then
        log_info "Path handling code detected"
    fi
    
    # Test feature flags
    log_subsection "Feature flag compatibility"
    
    local feature_tests=("--no-default-features" "--all-features")
    local successful_features=0
    
    for feature in "${feature_tests[@]}"; do
        if timeout 180 cargo check $feature --workspace >/dev/null 2>&1; then
            ((successful_features++))
            log_info "✅ Feature configuration $feature: OK"
        else
            log_warning "⚠️  Feature configuration $feature: FAILED"
        fi
    done
    
    consistency_score=$((successful_features * 100 / ${#feature_tests[@]}))
    
    local overall_build_score=$(((build_score + consistency_score) / 2))
    add_score "build_reliability" $overall_build_score 3 "Build system reliability and consistency"
}

# Assess test infrastructure quality
assess_test_infrastructure() {
    log_section "Phase 2 Readiness: Test Infrastructure Quality"
    
    local test_score=0
    local coverage_score=0
    local reliability_score=0
    
    log_subsection "Test execution reliability"
    
    # Run tests multiple times to check consistency
    local test_runs=3
    local successful_runs=0
    
    for i in $(seq 1 $test_runs); do
        log_info "Test run $i/$test_runs..."
        
        if timeout 300 cargo test --workspace --no-fail-fast >/dev/null 2>&1; then
            ((successful_runs++))
            log_info "✅ Test run $i: PASSED"
        else
            log_warning "⚠️  Test run $i: FAILED"
        fi
    done
    
    reliability_score=$((successful_runs * 100 / test_runs))
    
    log_subsection "Test coverage analysis"
    
    # Check for coverage tools and run if available
    if command -v cargo-tarpaulin >/dev/null 2>&1; then
        log_info "Running coverage analysis..."
        
        local coverage_output=$(timeout 300 cargo tarpaulin --workspace --line --ignore-tests --out Json 2>/dev/null || echo '{"coverage": 0}')
        local coverage_percent=$(echo "$coverage_output" | grep -o '"coverage":[0-9.]*' | cut -d':' -f2 | head -1 || echo "0")
        
        # Convert to integer
        coverage_percent=$(echo "$coverage_percent" | cut -d'.' -f1)
        
        if [ "${coverage_percent:-0}" -gt 15 ]; then
            coverage_score=100
            log_success "✅ Code coverage: ${coverage_percent}% (target: >15%)"
        elif [ "${coverage_percent:-0}" -gt 10 ]; then
            coverage_score=80
            log_warning "⚠️  Code coverage: ${coverage_percent}% (below target)"
        else
            coverage_score=50
            log_warning "⚠️  Code coverage: ${coverage_percent}% (significantly below target)"
        fi
    else
        log_warning "⚠️  Coverage tool not available"
        coverage_score=60
    fi
    
    log_subsection "Test organization and quality"
    
    # Check test structure
    local test_files=$(find . -name "*.rs" -path "*/tests/*" -o -name "*test*.rs" | wc -l)
    local test_quality_score=50
    
    if [ "$test_files" -gt 20 ]; then
        test_quality_score=100
        log_success "✅ Comprehensive test suite ($test_files test files)"
    elif [ "$test_files" -gt 10 ]; then
        test_quality_score=80
        log_info "Good test coverage ($test_files test files)"
    elif [ "$test_files" -gt 5 ]; then
        test_quality_score=60
        log_warning "⚠️  Limited test coverage ($test_files test files)"
    else
        test_quality_score=40
        log_warning "⚠️  Minimal test coverage ($test_files test files)"
    fi
    
    test_score=$(((reliability_score + coverage_score + test_quality_score) / 3))
    add_score "test_infrastructure" $test_score 4 "Test infrastructure quality and reliability"
}

# Assess documentation completeness
assess_documentation() {
    log_section "Phase 2 Readiness: Documentation Completeness"
    
    local doc_score=0
    local found_docs=0
    local total_docs=8
    
    log_subsection "Essential documentation check"
    
    # Check for essential documentation files
    local required_docs=(
        "README.md:Project overview and setup"
        "docs/installation.md:Installation instructions"  
        "docs/user-guides/quick-start.md:Quick start guide"
        "CLAUDE.md:Development instructions"
        "docs/technical/architecture.md:Architecture documentation"
        "docs/development/DEVELOPMENT.md:Development guide"
        "docs/user-guides/troubleshooting.md:Troubleshooting guide"
        "Cargo.toml:Project configuration"
    )
    
    for doc_entry in "${required_docs[@]}"; do
        local doc_file="${doc_entry%%:*}"
        local doc_desc="${doc_entry##*:}"
        
        if [ -f "$doc_file" ]; then
            ((found_docs++))
            log_success "✅ $doc_desc: $doc_file"
        else
            log_warning "⚠️  Missing: $doc_desc ($doc_file)"
        fi
    done
    
    log_subsection "API documentation"
    
    # Check for API documentation
    if timeout 120 cargo doc --no-deps --workspace >/dev/null 2>&1; then
        log_success "✅ API documentation generation successful"
        ((found_docs++))
        total_docs=$((total_docs + 1))
    else
        log_warning "⚠️  API documentation generation failed"
    fi
    
    log_subsection "Code examples and demos"
    
    # Check for examples
    local examples_count=$(find examples/ -name "*.rs" 2>/dev/null | wc -l || echo "0")
    
    if [ "$examples_count" -gt 5 ]; then
        log_success "✅ Rich example collection ($examples_count examples)"
        ((found_docs++))
    elif [ "$examples_count" -gt 0 ]; then
        log_info "Examples available ($examples_count examples)"
    else
        log_warning "⚠️  No code examples found"
    fi
    
    total_docs=$((total_docs + 1))
    doc_score=$((found_docs * 100 / total_docs))
    
    add_score "documentation" $doc_score 3 "Documentation completeness and accuracy"
}

# Assess technical debt status
assess_technical_debt() {
    log_section "Phase 2 Readiness: Technical Debt Assessment"
    
    local debt_score=100
    local code_quality_issues=0
    
    log_subsection "Code quality analysis"
    
    # Run detailed clippy analysis
    local clippy_output=$(cargo clippy --workspace --all-targets -- -W clippy::all 2>&1 || true)
    local clippy_warnings=$(echo "$clippy_output" | grep -c "warning:" || echo "0")
    
    if [ "$clippy_warnings" -eq 0 ]; then
        log_success "✅ No clippy warnings detected"
    elif [ "$clippy_warnings" -lt 10 ]; then
        log_warning "⚠️  $clippy_warnings clippy warnings (acceptable)"
        debt_score=$((debt_score - 10))
        code_quality_issues=$((code_quality_issues + 1))
    else
        log_warning "⚠️  $clippy_warnings clippy warnings (should be addressed)"
        debt_score=$((debt_score - 20))
        code_quality_issues=$((code_quality_issues + 2))
    fi
    
    log_subsection "Dependency analysis"
    
    # Check dependency tree for issues
    local dep_output=$(cargo tree --duplicates 2>/dev/null || true)
    local duplicate_deps=$(echo "$dep_output" | grep -c "^[[:alnum:]]" || echo "0")
    
    if [ "$duplicate_deps" -eq 0 ]; then
        log_success "✅ No duplicate dependencies detected"
    elif [ "$duplicate_deps" -lt 5 ]; then
        log_info "Minor duplicate dependencies ($duplicate_deps)"
        debt_score=$((debt_score - 5))
    else
        log_warning "⚠️  Multiple duplicate dependencies ($duplicate_deps)"
        debt_score=$((debt_score - 15))
        code_quality_issues=$((code_quality_issues + 1))
    fi
    
    log_subsection "TODO and FIXME analysis"
    
    # Count TODOs and FIXMEs in code
    local todo_count=$(grep -r "TODO\|FIXME\|XXX\|HACK" --include="*.rs" src/ 2>/dev/null | wc -l || echo "0")
    
    if [ "$todo_count" -eq 0 ]; then
        log_success "✅ No pending TODOs/FIXMEs"
    elif [ "$todo_count" -lt 10 ]; then
        log_info "Minor technical debt markers ($todo_count items)"
        debt_score=$((debt_score - 5))
    else
        log_warning "⚠️  Significant technical debt markers ($todo_count items)"
        debt_score=$((debt_score - 20))
        code_quality_issues=$((code_quality_issues + 1))
    fi
    
    # Ensure minimum score
    if [ "$debt_score" -lt 0 ]; then
        debt_score=0
    fi
    
    add_score "technical_debt" $debt_score 3 "Technical debt reduction and code quality"
}

# Assess performance baselines
assess_performance_readiness() {
    log_section "Phase 2 Readiness: Performance Baseline Assessment"
    
    local perf_score=50  # Start with baseline score
    
    log_subsection "Benchmark infrastructure"
    
    # Check for benchmark setup
    if [ -d "benches/" ]; then
        local bench_files=$(find benches/ -name "*.rs" | wc -l)
        
        if [ "$bench_files" -gt 0 ]; then
            log_success "✅ Benchmark infrastructure present ($bench_files benchmark files)"
            perf_score=$((perf_score + 20))
            
            # Try to run benchmarks
            if timeout 300 cargo bench --workspace >/dev/null 2>&1; then
                log_success "✅ Benchmarks execute successfully"
                perf_score=$((perf_score + 20))
            else
                log_warning "⚠️  Benchmark execution issues"
                perf_score=$((perf_score + 10))
            fi
        else
            log_warning "⚠️  Empty benchmark directory"
        fi
    else
        log_warning "⚠️  No benchmark infrastructure found"
    fi
    
    log_subsection "Build performance"
    
    # Measure build time
    local build_start=$(date +%s)
    
    if timeout 600 cargo build --release >/dev/null 2>&1; then
        local build_end=$(date +%s)
        local build_time=$((build_end - build_start))
        
        if [ "$build_time" -lt 120 ]; then
            log_success "✅ Fast build time: ${build_time}s"
            perf_score=$((perf_score + 10))
        elif [ "$build_time" -lt 300 ]; then
            log_info "Acceptable build time: ${build_time}s"
        else
            log_warning "⚠️  Slow build time: ${build_time}s"
            perf_score=$((perf_score - 10))
        fi
    else
        log_error "❌ Build timed out or failed"
        perf_score=$((perf_score - 20))
    fi
    
    # Ensure score bounds
    if [ "$perf_score" -gt 100 ]; then
        perf_score=100
    elif [ "$perf_score" -lt 0 ]; then
        perf_score=0
    fi
    
    add_score "performance_readiness" $perf_score 2 "Performance baseline and monitoring readiness"
}

# Assess scope alignment
assess_scope_alignment() {
    log_section "Phase 2 Readiness: Scope Alignment Assessment"
    
    local scope_score=70  # Start with reasonable baseline
    
    log_subsection "PRD alignment check"
    
    # Check for scope documentation
    if [ -f "docs/development/PRD.md" ]; then
        log_success "✅ PRD documentation available"
        scope_score=$((scope_score + 10))
    else
        log_warning "⚠️  PRD documentation not found"
    fi
    
    if [ -f "SCOPE_ALIGNMENT_ANALYSIS.md" ]; then
        log_success "✅ Scope alignment analysis available"
        scope_score=$((scope_score + 10))
    else
        log_warning "⚠️  Scope alignment analysis not found"
    fi
    
    if [ -f "TECHNICAL_DEBT_REDUCTION_PLAN.md" ]; then
        log_success "✅ Technical debt reduction plan available"
        scope_score=$((scope_score + 10))
    else
        log_warning "⚠️  Technical debt reduction plan not found"
    fi
    
    log_subsection "Feature set validation"
    
    # Check that we have core SSTable reading functionality
    local core_files=(
        "cqlite-core/src/lib.rs"
        "cqlite-core/src/storage/"
        "cqlite-cli/src/main.rs"
    )
    
    local found_core=0
    
    for core_file in "${core_files[@]}"; do
        if [ -e "$core_file" ]; then
            ((found_core++))
        fi
    done
    
    local core_percentage=$((found_core * 100 / ${#core_files[@]}))
    
    if [ "$core_percentage" -eq 100 ]; then
        log_success "✅ Core architecture components present"
        scope_score=$((scope_score + 10))
    elif [ "$core_percentage" -gt 66 ]; then
        log_warning "⚠️  Most core components present ($core_percentage%)"
    else
        log_warning "⚠️  Missing core components ($core_percentage%)"
        scope_score=$((scope_score - 10))
    fi
    
    # Ensure score bounds
    if [ "$scope_score" -gt 100 ]; then
        scope_score=100
    elif [ "$scope_score" -lt 0 ]; then
        scope_score=0
    fi
    
    add_score "scope_alignment" $scope_score 3 "Scope alignment with PRD and project goals"
}

# Generate comprehensive readiness report
generate_readiness_report() {
    log_section "Phase 2 Readiness Assessment Report"
    
    local final_score=0
    
    if [ "$MAX_SCORE" -gt 0 ]; then
        final_score=$((TOTAL_SCORE * 100 / MAX_SCORE))
    fi
    
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    echo "# Phase 2 Readiness Assessment Report"
    echo "Generated: $timestamp"
    echo ""
    
    echo "## Executive Summary"
    echo ""
    echo "**Overall Readiness Score: ${final_score}/100**"
    echo ""
    
    if [ "$final_score" -ge "$READINESS_THRESHOLD" ]; then
        echo "### ✅ PHASE 2 READY - APPROVED FOR PROGRESSION"
        echo ""
        echo "All critical readiness criteria have been met."
        echo "Phase 2 development is authorized to begin."
        echo ""
        local approval_level="APPROVED"
    elif [ "$final_score" -ge 75 ]; then
        echo "### ⚠️ PHASE 2 CONDITIONAL - MINOR ISSUES TO ADDRESS"
        echo ""
        echo "Most readiness criteria met with minor issues."
        echo "Phase 2 can proceed once minor issues are resolved."
        echo ""
        local approval_level="CONDITIONAL"
    else
        echo "### ❌ PHASE 2 NOT READY - SIGNIFICANT ISSUES TO RESOLVE"
        echo ""
        echo "Critical readiness criteria not met."
        echo "Phase 2 progression is BLOCKED until issues are resolved."
        echo ""
        local approval_level="BLOCKED"
    fi
    
    echo "## Detailed Assessment"
    echo ""
    echo "| Category | Score | Weight | Weighted Score | Status |"
    echo "|----------|-------|--------|----------------|---------|"
    
    for category in "${!SCORES[@]}"; do
        local score="${SCORES[$category]}"
        local weight="${WEIGHTS[$category]}"
        local weighted=$((score * weight))
        local description="${CRITERIA[$category]}"
        
        local status="✅ PASS"
        if [ "$score" -lt 60 ]; then
            status="❌ FAIL"
        elif [ "$score" -lt 80 ]; then
            status="⚠️ WARN"
        fi
        
        echo "| $description | $score/100 | ${weight}x | $weighted | $status |"
    done
    
    echo ""
    echo "**Total Score: $TOTAL_SCORE / $MAX_SCORE = ${final_score}%**"
    echo ""
    
    echo "## Recommendations"
    echo ""
    
    if [ "$final_score" -ge "$READINESS_THRESHOLD" ]; then
        echo "### ✅ Ready for Phase 2"
        echo "- All critical criteria met"
        echo "- Proceed with Phase 2 development"
        echo "- Continue monitoring quality metrics"
        echo "- Maintain current development standards"
    elif [ "$final_score" -ge 75 ]; then
        echo "### ⚠️ Address Minor Issues Before Phase 2"
        echo ""
        
        # Provide specific recommendations based on low scores
        for category in "${!SCORES[@]}"; do
            local score="${SCORES[$category]}"
            if [ "$score" -lt 80 ]; then
                local description="${CRITERIA[$category]}"
                echo "- **$description**: Address issues to improve from $score/100"
            fi
        done
    else
        echo "### ❌ Critical Issues Must Be Resolved"
        echo ""
        
        # Identify blocking issues
        for category in "${!SCORES[@]}"; do
            local score="${SCORES[$category]}"
            if [ "$score" -lt 60 ]; then
                local description="${CRITERIA[$category]}"
                echo "- **CRITICAL: $description**: Score $score/100 is below minimum threshold"
            fi
        done
        
        echo ""
        echo "**Phase 2 is BLOCKED until all critical issues are resolved.**"
    fi
    
    echo ""
    echo "## Next Steps"
    echo ""
    
    if [ "$approval_level" = "APPROVED" ]; then
        echo "1. ✅ Begin Phase 2 development activities"
        echo "2. 📊 Continue quality monitoring with existing gates"
        echo "3. 🔄 Run regular readiness assessments during Phase 2"
        echo "4. 📝 Update documentation as features are implemented"
        
        return 0
    elif [ "$approval_level" = "CONDITIONAL" ]; then
        echo "1. 🔧 Address identified minor issues"
        echo "2. 🔄 Re-run readiness assessment"
        echo "3. ✅ Proceed to Phase 2 once score ≥ $READINESS_THRESHOLD%"
        echo "4. 📊 Implement additional monitoring for weak areas"
        
        return 1
    else
        echo "1. ❌ **DO NOT BEGIN PHASE 2 DEVELOPMENT**"
        echo "2. 🔧 Resolve all critical issues (scores < 60%)"
        echo "3. 🔄 Re-run Phase 1 validation if needed"
        echo "4. 🔄 Re-run readiness assessment after fixes"
        echo "5. ✅ Only proceed when readiness score ≥ $READINESS_THRESHOLD%"
        
        return 2
    fi
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════╗"
    echo "║     PHASE 2 READINESS ASSESSOR       ║"
    echo "║                                      ║"
    echo "║  Comprehensive Phase 2 readiness     ║"
    echo "║  evaluation and scoring              ║"
    echo "╚══════════════════════════════════════╝"
    echo -e "${NC}"
    
    log_info "Starting Phase 2 readiness assessment at $(date)"
    log_info "Working directory: $(pwd)"
    log_info "Readiness threshold: ${READINESS_THRESHOLD}%"
    
    # Run all assessment categories
    validate_phase1_completion || true
    assess_build_reliability || true
    assess_test_infrastructure || true
    assess_documentation || true
    assess_technical_debt || true
    assess_performance_readiness || true
    assess_scope_alignment || true
    
    # Generate final report and get exit code
    echo ""
    generate_readiness_report
    local exit_code=$?
    
    echo ""
    if [ $exit_code -eq 0 ]; then
        log_success "🎉 Phase 2 readiness assessment: APPROVED!"
        log_info "Phase 2 development is authorized to proceed."
    elif [ $exit_code -eq 1 ]; then
        log_warning "⚠️  Phase 2 readiness assessment: CONDITIONAL APPROVAL"
        log_info "Address minor issues before proceeding to Phase 2."
    else
        log_error "💥 Phase 2 readiness assessment: BLOCKED!"
        log_error "Critical issues must be resolved before Phase 2."
    fi
    
    exit $exit_code
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi