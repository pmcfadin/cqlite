#!/bin/bash

# CQLite Test Data Validation Script
# Validates generated test data quality and completeness
# Issue #18: Docker-based test data generation

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATED_DIR="$SCRIPT_DIR/../generated"
OUTPUT_DIR="$GENERATED_DIR/validation-reports"

# Cassandra versions to validate
VERSIONS=("3.7" "3.11" "4.0" "4.1")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Initialize validation environment
init_validation() {
    log_info "Initializing validation environment..."
    
    mkdir -p "$OUTPUT_DIR"
    
    # Install Python dependencies for validation
    pip install cassandra-driver pyyaml tabulate >/dev/null 2>&1
    
    log_success "Validation environment initialized"
}

# Validate directory structure
validate_directory_structure() {
    local version=$1
    local base_dir="$GENERATED_DIR/v$version"
    
    log_info "Validating directory structure for Cassandra $version..."
    
    local issues=()
    
    # Check main directories exist
    local required_dirs=("sstables" "metadata" "schemas" "compression-variants")
    for dir in "${required_dirs[@]}"; do
        if [ ! -d "$base_dir/$dir" ]; then
            issues+=("Missing directory: $dir")
        fi
    done
    
    # Check keyspace directories
    local required_keyspaces=("test_basic" "test_collections" "test_timeseries" "test_wide_rows")
    for keyspace in "${required_keyspaces[@]}"; do
        if [ ! -d "$base_dir/sstables/$keyspace" ]; then
            issues+=("Missing keyspace directory: $keyspace")
        fi
    done
    
    # Check compression variant directories
    local compression_types=("snappy" "lz4" "deflate" "uncompressed")
    for comp in "${compression_types[@]}"; do
        if [ ! -d "$base_dir/compression-variants/$comp" ]; then
            issues+=("Missing compression directory: $comp")
        fi
    done
    
    # Generate report
    local report_file="$OUTPUT_DIR/structure_validation_v$version.txt"
    {
        echo "Directory Structure Validation Report"
        echo "Cassandra Version: $version"
        echo "Generated: $(date)"
        echo "Base Directory: $base_dir"
        echo ""
        
        if [ ${#issues[@]} -eq 0 ]; then
            echo "✅ PASS: All required directories found"
        else
            echo "❌ FAIL: Missing directories found"
            for issue in "${issues[@]}"; do
                echo "  - $issue"
            done
        fi
        
        echo ""
        echo "Directory Tree:"
        if [ -d "$base_dir" ]; then
            tree "$base_dir" 2>/dev/null || find "$base_dir" -type d | sort
        else
            echo "Base directory does not exist"
        fi
    } > "$report_file"
    
    if [ ${#issues[@]} -eq 0 ]; then
        log_success "Directory structure validation passed for version $version"
        return 0
    else
        log_error "Directory structure validation failed for version $version (${#issues[@]} issues)"
        return 1
    fi
}

# Validate SSTable files
validate_sstable_files() {
    local version=$1
    local base_dir="$GENERATED_DIR/v$version"
    
    log_info "Validating SSTable files for Cassandra $version..."
    
    # Create Python validation script
    cat > "/tmp/sstable_validator_$version.py" << 'EOF'
import os
import sys
import json
from pathlib import Path

def validate_sstable_files(base_dir):
    """Validate SSTable files for completeness and correctness"""
    base_path = Path(base_dir)
    sstables_dir = base_path / "sstables"
    
    if not sstables_dir.exists():
        return {"error": "SSTables directory does not exist"}
    
    validation_results = {
        "total_files": 0,
        "data_files": 0,
        "index_files": 0,
        "filter_files": 0,
        "statistics_files": 0,
        "summary_files": 0,
        "toc_files": 0,
        "other_files": 0,
        "total_size_bytes": 0,
        "keyspaces": {},
        "issues": [],
        "file_pairs": {}
    }
    
    # Scan all files
    for file_path in sstables_dir.rglob("*"):
        if file_path.is_file():
            validation_results["total_files"] += 1
            file_size = file_path.stat().st_size
            validation_results["total_size_bytes"] += file_size
            
            filename = file_path.name
            keyspace = file_path.parent.parent.name if file_path.parent.parent.name.startswith('test_') else 'unknown'
            table = file_path.parent.name
            
            # Initialize keyspace tracking
            if keyspace not in validation_results["keyspaces"]:
                validation_results["keyspaces"][keyspace] = {
                    "tables": {},
                    "file_count": 0,
                    "total_size": 0
                }
            
            if table not in validation_results["keyspaces"][keyspace]["tables"]:
                validation_results["keyspaces"][keyspace]["tables"][table] = {
                    "files": 0,
                    "size": 0,
                    "data_files": 0
                }
            
            validation_results["keyspaces"][keyspace]["file_count"] += 1
            validation_results["keyspaces"][keyspace]["total_size"] += file_size
            validation_results["keyspaces"][keyspace]["tables"][table]["files"] += 1
            validation_results["keyspaces"][keyspace]["tables"][table]["size"] += file_size
            
            # Categorize file types
            if filename.endswith('-Data.db'):
                validation_results["data_files"] += 1
                validation_results["keyspaces"][keyspace]["tables"][table]["data_files"] += 1
                
                # Extract generation for file pairing validation
                parts = filename.split('-')
                if len(parts) >= 2:
                    generation = parts[1]
                    pair_key = f"{keyspace}/{table}/{generation}"
                    if pair_key not in validation_results["file_pairs"]:
                        validation_results["file_pairs"][pair_key] = {
                            "data": False, "index": False, "filter": False, 
                            "statistics": False, "summary": False, "toc": False
                        }
                    validation_results["file_pairs"][pair_key]["data"] = True
                    
            elif filename.endswith('-Index.db'):
                validation_results["index_files"] += 1
            elif filename.endswith('-Filter.db'):
                validation_results["filter_files"] += 1
            elif filename.endswith('-Statistics.db'):
                validation_results["statistics_files"] += 1
            elif filename.endswith('-Summary.db'):
                validation_results["summary_files"] += 1
            elif filename.endswith('-TOC.txt'):
                validation_results["toc_files"] += 1
            else:
                validation_results["other_files"] += 1
            
            # Validate file size
            if file_size == 0:
                validation_results["issues"].append(f"Empty file: {file_path}")
            elif file_size < 100:  # Very small files might be suspicious
                validation_results["issues"].append(f"Unusually small file ({file_size} bytes): {file_path}")
    
    # Validate expected keyspaces exist
    expected_keyspaces = ["test_basic", "test_collections", "test_timeseries", "test_wide_rows"]
    for keyspace in expected_keyspaces:
        if keyspace not in validation_results["keyspaces"]:
            validation_results["issues"].append(f"Missing expected keyspace: {keyspace}")
        elif validation_results["keyspaces"][keyspace]["file_count"] == 0:
            validation_results["issues"].append(f"No files found for keyspace: {keyspace}")
    
    # Validate file pairs (each data file should have associated files)
    for pair_key, files in validation_results["file_pairs"].items():
        if files["data"] and not files["index"]:
            validation_results["issues"].append(f"Missing index file for: {pair_key}")
    
    return validation_results

def format_bytes(bytes_value):
    """Format bytes in human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python sstable_validator.py <base_dir> <output_file>")
        sys.exit(1)
    
    base_dir = sys.argv[1]
    output_file = sys.argv[2]
    
    results = validate_sstable_files(base_dir)
    
    # Write detailed JSON report
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Print summary
    if "error" in results:
        print(f"❌ VALIDATION FAILED: {results['error']}")
        sys.exit(1)
    
    print(f"📊 FILES ANALYZED: {results['total_files']}")
    print(f"💾 TOTAL SIZE: {format_bytes(results['total_size_bytes'])}")
    print(f"📁 DATA FILES: {results['data_files']}")
    print(f"🔍 INDEX FILES: {results['index_files']}")
    print(f"🏷️  KEYSPACES: {len(results['keyspaces'])}")
    print(f"⚠️  ISSUES: {len(results['issues'])}")
    
    if results['issues']:
        print("\nISSUES FOUND:")
        for issue in results['issues'][:10]:  # Show first 10 issues
            print(f"  - {issue}")
        if len(results['issues']) > 10:
            print(f"  ... and {len(results['issues']) - 10} more issues")
    
    sys.exit(0 if len(results['issues']) == 0 else 1)
EOF

    # Run validation
    local report_file="$OUTPUT_DIR/sstable_validation_v$version.json"
    if python3 "/tmp/sstable_validator_$version.py" "$base_dir" "$report_file"; then
        log_success "SSTable validation passed for version $version"
        local exit_code=0
    else
        log_error "SSTable validation failed for version $version"
        local exit_code=1
    fi
    
    # Clean up temporary file
    rm -f "/tmp/sstable_validator_$version.py"
    
    return $exit_code
}

# Validate metadata files
validate_metadata() {
    local version=$1
    local base_dir="$GENERATED_DIR/v$version"
    
    log_info "Validating metadata for Cassandra $version..."
    
    local issues=()
    local metadata_dir="$base_dir/metadata"
    
    # Check metadata directory exists
    if [ ! -d "$metadata_dir" ]; then
        issues+=("Missing metadata directory")
        log_error "Metadata validation failed for version $version: missing directory"
        return 1
    fi
    
    # Check for required metadata files
    local required_files=("summary.json")
    for file in "${required_files[@]}"; do
        if [ ! -f "$metadata_dir/$file" ]; then
            issues+=("Missing metadata file: $file")
        fi
    done
    
    # Check keyspace metadata files
    local expected_keyspaces=("test_basic" "test_collections" "test_timeseries" "test_wide_rows")
    for keyspace in "${expected_keyspaces[@]}"; do
        local metadata_file="$metadata_dir/${keyspace}_metadata.json"
        if [ ! -f "$metadata_file" ]; then
            issues+=("Missing keyspace metadata: ${keyspace}_metadata.json")
        else
            # Validate JSON format
            if ! python3 -m json.tool "$metadata_file" >/dev/null 2>&1; then
                issues+=("Invalid JSON format: ${keyspace}_metadata.json")
            fi
        fi
    done
    
    # Generate report
    local report_file="$OUTPUT_DIR/metadata_validation_v$version.txt"
    {
        echo "Metadata Validation Report"
        echo "Cassandra Version: $version"
        echo "Generated: $(date)"
        echo "Metadata Directory: $metadata_dir"
        echo ""
        
        if [ ${#issues[@]} -eq 0 ]; then
            echo "✅ PASS: All metadata files found and valid"
        else
            echo "❌ FAIL: Metadata issues found"
            for issue in "${issues[@]}"; do
                echo "  - $issue"
            done
        fi
        
        echo ""
        echo "Available Metadata Files:"
        if [ -d "$metadata_dir" ]; then
            ls -la "$metadata_dir"
        fi
    } > "$report_file"
    
    if [ ${#issues[@]} -eq 0 ]; then
        log_success "Metadata validation passed for version $version"
        return 0
    else
        log_error "Metadata validation failed for version $version (${#issues[@]} issues)"
        return 1
    fi
}

# Generate comprehensive validation report
generate_comprehensive_report() {
    log_info "Generating comprehensive validation report..."
    
    local report_file="$OUTPUT_DIR/comprehensive_validation_report.md"
    
    {
        echo "# CQLite Test Data Validation Report"
        echo ""
        echo "**Generated:** $(date)"
        echo "**Validation Tool:** cqlite-test-data-validator"  
        echo "**Issue:** #18 - Docker-based test data generation"
        echo ""
        
        echo "## Executive Summary"
        echo ""
        
        local total_versions=0
        local passed_versions=0
        local total_issues=0
        
        for version in "${VERSIONS[@]}"; do
            local base_dir="$GENERATED_DIR/v$version"
            if [ -d "$base_dir" ]; then
                total_versions=$((total_versions + 1))
                
                # Count issues from individual reports
                local structure_report="$OUTPUT_DIR/structure_validation_v$version.txt"
                local sstable_report="$OUTPUT_DIR/sstable_validation_v$version.json"
                local metadata_report="$OUTPUT_DIR/metadata_validation_v$version.txt"
                
                local version_issues=0
                
                if [ -f "$structure_report" ] && grep -q "✅ PASS" "$structure_report"; then
                    : # Structure validation passed
                else
                    version_issues=$((version_issues + 1))
                fi
                
                if [ -f "$sstable_report" ]; then
                    local sstable_issues=$(python3 -c "import json; print(len(json.load(open('$sstable_report'))['issues']))" 2>/dev/null || echo "1")
                    version_issues=$((version_issues + sstable_issues))
                fi
                
                if [ -f "$metadata_report" ] && grep -q "✅ PASS" "$metadata_report"; then
                    : # Metadata validation passed
                else
                    version_issues=$((version_issues + 1))
                fi
                
                total_issues=$((total_issues + version_issues))
                
                if [ $version_issues -eq 0 ]; then
                    passed_versions=$((passed_versions + 1))
                fi
            fi
        done
        
        echo "- **Total Versions Tested:** $total_versions"
        echo "- **Versions Passed:** $passed_versions"
        echo "- **Versions Failed:** $((total_versions - passed_versions))"
        echo "- **Total Issues Found:** $total_issues"
        echo ""
        
        if [ $total_issues -eq 0 ]; then
            echo "🎉 **OVERALL STATUS: PASS** - All validations successful!"
        else
            echo "⚠️ **OVERALL STATUS: ISSUES FOUND** - See detailed reports below"
        fi
        echo ""
        
        echo "## Version-Specific Results"
        echo ""
        
        for version in "${VERSIONS[@]}"; do
            local base_dir="$GENERATED_DIR/v$version"
            
            echo "### Cassandra $version"
            echo ""
            
            if [ ! -d "$base_dir" ]; then
                echo "❌ **Status:** NOT FOUND - Version directory missing"
                echo ""
                continue
            fi
            
            local total_size=$(du -sh "$base_dir" 2>/dev/null | cut -f1 || echo "unknown")
            local file_count=$(find "$base_dir" -type f 2>/dev/null | wc -l || echo "unknown")
            
            echo "- **Status:** $([ -d "$base_dir" ] && echo "Generated" || echo "Missing")"
            echo "- **Total Size:** $total_size"
            echo "- **File Count:** $file_count"
            echo ""
            
            # Include results from individual validation reports
            local structure_report="$OUTPUT_DIR/structure_validation_v$version.txt"
            if [ -f "$structure_report" ]; then
                echo "**Directory Structure:**"
                if grep -q "✅ PASS" "$structure_report"; then
                    echo "✅ PASS"
                else
                    echo "❌ FAIL"
                fi
                echo ""
            fi
            
            local sstable_report="$OUTPUT_DIR/sstable_validation_v$version.json" 
            if [ -f "$sstable_report" ]; then
                echo "**SSTable Files:**"
                local data_files=$(python3 -c "import json; print(json.load(open('$sstable_report'))['data_files'])" 2>/dev/null || echo "unknown")
                local issues_count=$(python3 -c "import json; print(len(json.load(open('$sstable_report'))['issues']))" 2>/dev/null || echo "unknown")
                echo "- Data Files: $data_files"
                echo "- Issues: $issues_count"
                if [ "$issues_count" = "0" ]; then
                    echo "✅ PASS"
                else
                    echo "❌ FAIL"
                fi
                echo ""
            fi
            
            local metadata_report="$OUTPUT_DIR/metadata_validation_v$version.txt"
            if [ -f "$metadata_report" ]; then
                echo "**Metadata:**"
                if grep -q "✅ PASS" "$metadata_report"; then
                    echo "✅ PASS"
                else
                    echo "❌ FAIL"
                fi
                echo ""
            fi
        done
        
        echo "## Detailed Validation Reports"
        echo ""
        echo "Individual validation reports are available in:"
        echo "- \`$OUTPUT_DIR/\`"
        echo ""
        echo "### File Structure"
        echo ""
        echo "\`\`\`"
        tree "$OUTPUT_DIR" 2>/dev/null || find "$OUTPUT_DIR" -type f | sort
        echo "\`\`\`"
        echo ""
        
        echo "## Recommendations"
        echo ""
        
        if [ $total_issues -eq 0 ]; then
            echo "- ✅ All validations passed successfully"
            echo "- ✅ Test data is ready for CQLite integration testing"
            echo "- ✅ Proceed with CI/CD pipeline integration"
        else
            echo "- ⚠️ Address validation issues before using test data"
            echo "- ⚠️ Review individual validation reports for specific problems"
            echo "- ⚠️ Consider regenerating test data for failed versions"
        fi
        
        echo ""
        echo "## Next Steps"
        echo ""
        echo "1. **If validation passed:** Integrate with CQLite testing framework"
        echo "2. **If issues found:** Review detailed reports and regenerate as needed"
        echo "3. **Performance testing:** Run CQLite benchmarks with generated data"
        echo "4. **CI/CD integration:** Add validation to automated testing pipeline"
        echo ""
        
        echo "---"
        echo "*Report generated by CQLite test data validation system*"
        
    } > "$report_file"
    
    log_success "Comprehensive validation report generated: $report_file"
}

# Main execution
main() {
    log_info "Starting CQLite test data validation..."
    
    if [ ! -d "$GENERATED_DIR" ]; then
        log_error "Generated data directory not found: $GENERATED_DIR"
        exit 1
    fi
    
    # Initialize validation
    init_validation
    
    local validation_passed=true
    
    # Validate each Cassandra version
    for version in "${VERSIONS[@]}"; do
        local base_dir="$GENERATED_DIR/v$version"
        
        if [ ! -d "$base_dir" ]; then
            log_warning "Skipping validation for Cassandra $version - directory not found"
            continue
        fi
        
        log_info "Validating Cassandra $version..."
        
        local version_passed=true
        
        # Run individual validations
        if ! validate_directory_structure "$version"; then
            version_passed=false
        fi
        
        if ! validate_sstable_files "$version"; then
            version_passed=false
        fi
        
        if ! validate_metadata "$version"; then
            version_passed=false
        fi
        
        if $version_passed; then
            log_success "All validations passed for Cassandra $version"
        else
            log_error "Validation failed for Cassandra $version"
            validation_passed=false
        fi
    done
    
    # Generate comprehensive report
    generate_comprehensive_report
    
    # Final summary
    if $validation_passed; then
        log_success "All test data validation completed successfully!"
        echo ""
        echo "📊 Validation Summary:"
        echo "  ✅ All Cassandra versions validated"
        echo "  ✅ Directory structures correct"
        echo "  ✅ SSTable files validated"
        echo "  ✅ Metadata files verified"
        echo ""
        echo "📋 Reports available in: $OUTPUT_DIR"
        exit 0
    else
        log_error "Test data validation failed - see reports for details"
        echo ""
        echo "📊 Validation Summary:"
        echo "  ❌ Some validations failed"
        echo "  📋 Check individual reports in: $OUTPUT_DIR"
        echo "  🔧 Consider regenerating failed test data"
        exit 1
    fi
}

# Execute main function
main "$@"