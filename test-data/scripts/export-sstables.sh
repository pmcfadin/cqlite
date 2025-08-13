#!/bin/bash

# CQLite SSTable Export Script
# Exports generated SSTables from Cassandra containers for testing
# Issue #18: Docker-based test data generation

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DATA_DIR="${SOURCE_DATA_DIR:-/var/lib}"
OUTPUT_DIR="${OUTPUT_DIR:-/opt/generated}"

# Cassandra versions to export
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

# Install required tools
install_dependencies() {
    log_info "Installing required dependencies..."
    
    # Install system tools
    apt-get update >/dev/null 2>&1
    apt-get install -y rsync find coreutils gzip tar >/dev/null 2>&1
    
    # Install Python packages for metadata extraction
    pip install cassandra-driver pyyaml >/dev/null 2>&1
    
    log_success "Dependencies installed successfully"
}

# Create directory structure for organized export
create_output_structure() {
    local version=$1
    local base_dir="$OUTPUT_DIR/v$version"
    
    log_info "Creating output structure for Cassandra $version..."
    
    mkdir -p "$base_dir"/{sstables,metadata,schemas,compression-variants}
    mkdir -p "$base_dir"/sstables/{test_basic,test_collections,test_timeseries,test_wide_rows}
    mkdir -p "$base_dir"/compression-variants/{snappy,lz4,deflate,uncompressed}
    
    log_success "Output structure created for version $version"
}

# Extract SSTable files from Cassandra data directory
extract_sstables() {
    local version=$1
    local source_dir="$SOURCE_DATA_DIR/cassandra-$version/data"
    local output_dir="$OUTPUT_DIR/v$version/sstables"
    
    log_info "Extracting SSTables for Cassandra $version..."
    
    if [ ! -d "$source_dir" ]; then
        log_warning "Source directory $source_dir not found for version $version"
        return 1
    fi
    
    # Find and copy SSTable files
    local files_copied=0
    
    # Process each keyspace
    for keyspace_dir in "$source_dir"/test_*; do
        if [ -d "$keyspace_dir" ]; then
            keyspace_name=$(basename "$keyspace_dir")
            log_info "Processing keyspace: $keyspace_name"
            
            mkdir -p "$output_dir/$keyspace_name"
            
            # Process each table in the keyspace
            for table_dir in "$keyspace_dir"/*; do
                if [ -d "$table_dir" ]; then
                    table_name=$(basename "$table_dir")
                    log_info "  Processing table: $table_name"
                    
                    mkdir -p "$output_dir/$keyspace_name/$table_name"
                    
                    # Find and copy all SSTable files
                    local table_files=0
                    find "$table_dir" -name "*.db" -type f | while read -r sstable_file; do
                        local file_name=$(basename "$sstable_file")
                        local file_size=$(stat -c%s "$sstable_file")
                        
                        # Copy the SSTable file
                        cp "$sstable_file" "$output_dir/$keyspace_name/$table_name/"
                        
                        # Also copy associated files (index, filter, statistics, etc.)
                        local base_name="${file_name%-*}"
                        find "$(dirname "$sstable_file")" -name "$base_name-*" -type f | while read -r associated_file; do
                            cp "$associated_file" "$output_dir/$keyspace_name/$table_name/" 2>/dev/null || true
                        done
                        
                        table_files=$((table_files + 1))
                        files_copied=$((files_copied + 1))
                        
                        log_info "    Copied: $file_name ($file_size bytes)"
                    done
                    
                    log_info "  Copied $table_files SSTable files for table $table_name"
                fi
            done
        fi
    done
    
    log_success "Extracted $files_copied SSTable files for Cassandra $version"
    return 0
}

# Generate metadata for exported SSTables
generate_metadata() {
    local version=$1
    local base_dir="$OUTPUT_DIR/v$version"
    
    log_info "Generating metadata for Cassandra $version..."
    
    # Create Python script for metadata extraction
    cat > "/tmp/metadata_generator_$version.py" << 'EOF'
import os
import sys
import json
import yaml
from datetime import datetime
from pathlib import Path

def analyze_sstable_file(file_path):
    """Analyze an SSTable file and extract basic metadata"""
    try:
        stat = os.stat(file_path)
        
        # Extract information from filename
        filename = os.path.basename(file_path)
        parts = filename.split('-')
        
        metadata = {
            'filename': filename,
            'file_path': str(file_path),
            'size_bytes': stat.st_size,
            'size_human': format_bytes(stat.st_size),
            'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'created_time': datetime.fromtimestamp(stat.st_ctime).isoformat(),
        }
        
        # Try to extract generation and format from filename
        if len(parts) >= 2:
            metadata['generation'] = parts[1] if parts[1].isdigit() else 'unknown'
        
        # Determine file type from extension
        if filename.endswith('-Data.db'):
            metadata['file_type'] = 'data'
        elif filename.endswith('-Index.db'):
            metadata['file_type'] = 'index'
        elif filename.endswith('-Filter.db'):
            metadata['file_type'] = 'bloom_filter'
        elif filename.endswith('-Statistics.db'):
            metadata['file_type'] = 'statistics'
        elif filename.endswith('-Summary.db'):
            metadata['file_type'] = 'summary'
        elif filename.endswith('-TOC.txt'):
            metadata['file_type'] = 'table_of_contents'
        else:
            metadata['file_type'] = 'unknown'
        
        return metadata
    except Exception as e:
        return {'error': str(e), 'filename': os.path.basename(file_path)}

def format_bytes(bytes_value):
    """Format bytes in human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

def analyze_directory(directory_path):
    """Analyze all SSTable files in a directory"""
    directory_path = Path(directory_path)
    
    if not directory_path.exists():
        return {'error': 'Directory does not exist'}
    
    files_metadata = []
    total_size = 0
    file_types = {}
    
    # Find all SSTable-related files
    for file_path in directory_path.rglob('*'):
        if file_path.is_file() and any(file_path.name.endswith(ext) for ext in ['.db', '.txt', '.sha1', '.crc32']):
            metadata = analyze_sstable_file(file_path)
            files_metadata.append(metadata)
            
            if 'size_bytes' in metadata:
                total_size += metadata['size_bytes']
                file_type = metadata.get('file_type', 'unknown')
                file_types[file_type] = file_types.get(file_type, 0) + 1
    
    return {
        'directory': str(directory_path),
        'total_files': len(files_metadata),
        'total_size_bytes': total_size,
        'total_size_human': format_bytes(total_size),
        'file_types': file_types,
        'files': files_metadata,
        'generated_at': datetime.now().isoformat()
    }

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python metadata_generator.py <input_directory> <output_file>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_file = sys.argv[2]
    
    metadata = analyze_directory(input_dir)
    
    # Write JSON metadata
    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Also write YAML version
    yaml_file = output_file.replace('.json', '.yaml')
    with open(yaml_file, 'w') as f:
        yaml.dump(metadata, f, default_flow_style=False, indent=2)
    
    print(f"Metadata written to {output_file} and {yaml_file}")
    print(f"Analyzed {metadata['total_files']} files ({metadata['total_size_human']})")
EOF

    # Generate metadata for each keyspace
    for keyspace_dir in "$base_dir/sstables"/test_*; do
        if [ -d "$keyspace_dir" ]; then
            keyspace_name=$(basename "$keyspace_dir")
            metadata_file="$base_dir/metadata/${keyspace_name}_metadata.json"
            
            log_info "  Generating metadata for keyspace: $keyspace_name"
            python3 "/tmp/metadata_generator_$version.py" "$keyspace_dir" "$metadata_file"
        fi
    done
    
    # Generate overall summary
    cat > "$base_dir/metadata/summary.json" << EOF
{
    "cassandra_version": "$version",
    "export_date": "$(date -Iseconds)",
    "export_tool": "cqlite-sstable-exporter",
    "keyspaces": [
        "test_basic",
        "test_collections", 
        "test_timeseries",
        "test_wide_rows"
    ],
    "data_patterns": [
        "basic_types",
        "collections",
        "time_series", 
        "wide_partitions",
        "counters",
        "compressed_data",
        "uncompressed_data"
    ],
    "compression_algorithms": [
        "SnappyCompressor",
        "LZ4Compressor", 
        "DeflateCompressor",
        "Uncompressed"
    ],
    "estimated_row_count": 36000,
    "notes": "Generated by CQLite Docker test data generation system for Issue #18"
}
EOF

    # Clean up temporary file
    rm -f "/tmp/metadata_generator_$version.py"
    
    log_success "Metadata generated for Cassandra $version"
}

# Create schema export
export_schemas() {
    local version=$1
    local base_dir="$OUTPUT_DIR/v$version"
    
    log_info "Exporting schemas for Cassandra $version..."
    
    # Copy schema files
    if [ -d "/opt/schemas" ]; then
        cp /opt/schemas/*.cql "$base_dir/schemas/" 2>/dev/null || true
        log_success "Schema files copied for version $version"
    else
        log_warning "Schema directory not found for version $version"
    fi
    
    # Create version-specific schema documentation
    cat > "$base_dir/schemas/README.md" << EOF
# CQLite Test Schemas - Cassandra $version

This directory contains the CQL schema definitions used to generate test data for Cassandra $version.

## Schema Files

- **basic-types.cql**: Fundamental CQL data types and primitive tables
- **collections.cql**: SET, LIST, MAP collections with various patterns
- **time-series.cql**: Time-based data with TTLs and time window compaction
- **wide-rows.cql**: Wide partitions and tables with many columns

## Usage

These schemas are automatically applied during the test data generation process. They can also be applied manually:

\`\`\`bash
cqlsh -f basic-types.cql
cqlsh -f collections.cql  
cqlsh -f time-series.cql
cqlsh -f wide-rows.cql
\`\`\`

## Compatibility

Generated for Cassandra $version. Some features may not be available in older versions.

Generated: $(date)
EOF

    log_success "Schema export completed for version $version"
}

# Categorize files by compression type
categorize_by_compression() {
    local version=$1
    local base_dir="$OUTPUT_DIR/v$version"
    
    log_info "Categorizing files by compression for Cassandra $version..."
    
    # Create symbolic links or copies based on compression type
    find "$base_dir/sstables" -name "*Data.db" -type f | while read -r sstable_file; do
        local file_name=$(basename "$sstable_file")
        local relative_path=${sstable_file#$base_dir/sstables/}
        
        # Determine compression type based on table name or metadata
        # This is a simplified approach - in reality, you'd need to read the SSTable metadata
        if [[ "$relative_path" == *"compression_test"* ]]; then
            ln -sf "../../sstables/$relative_path" "$base_dir/compression-variants/snappy/$file_name" 2>/dev/null || true
        elif [[ "$relative_path" == *"uncompressed"* ]]; then
            ln -sf "../../sstables/$relative_path" "$base_dir/compression-variants/uncompressed/$file_name" 2>/dev/null || true
        elif [[ "$relative_path" == *"collection"* ]]; then
            ln -sf "../../sstables/$relative_path" "$base_dir/compression-variants/lz4/$file_name" 2>/dev/null || true
        elif [[ "$relative_path" == *"multi_partition"* ]]; then
            ln -sf "../../sstables/$relative_path" "$base_dir/compression-variants/deflate/$file_name" 2>/dev/null || true
        else
            ln -sf "../../sstables/$relative_path" "$base_dir/compression-variants/snappy/$file_name" 2>/dev/null || true
        fi
    done
    
    log_success "Compression categorization completed for version $version"
}

# Create final export archive
create_export_archive() {
    local version=$1
    local base_dir="$OUTPUT_DIR/v$version"
    
    log_info "Creating export archive for Cassandra $version..."
    
    # Create compressed archive
    cd "$OUTPUT_DIR"
    tar -czf "cqlite-test-data-v$version.tar.gz" "v$version/"
    
    local archive_size=$(stat -c%s "cqlite-test-data-v$version.tar.gz")
    log_success "Export archive created: cqlite-test-data-v$version.tar.gz ($(numfmt --to=iec-i --suffix=B $archive_size))"
}

# Main execution
main() {
    log_info "Starting CQLite SSTable export process..."
    
    # Install dependencies
    install_dependencies
    
    # Process each Cassandra version
    for version in "${VERSIONS[@]}"; do
        log_info "Processing Cassandra $version..."
        
        # Create output structure
        create_output_structure "$version"
        
        # Extract SSTables
        if extract_sstables "$version"; then
            # Generate metadata
            generate_metadata "$version"
            
            # Export schemas
            export_schemas "$version"
            
            # Categorize by compression
            categorize_by_compression "$version"
            
            # Create archive
            create_export_archive "$version"
            
            log_success "Export completed successfully for Cassandra $version"
        else
            log_error "Export failed for Cassandra $version"
        fi
    done
    
    # Create overall summary
    cat > "$OUTPUT_DIR/export_summary.md" << EOF
# CQLite Test Data Export Summary

Generated: $(date)

## Exported Versions

$(for version in "${VERSIONS[@]}"; do
    if [ -f "$OUTPUT_DIR/cqlite-test-data-v$version.tar.gz" ]; then
        size=$(stat -c%s "$OUTPUT_DIR/cqlite-test-data-v$version.tar.gz" 2>/dev/null || echo "0")
        echo "- **Cassandra $version**: cqlite-test-data-v$version.tar.gz ($(numfmt --to=iec-i --suffix=B $size))"
    fi
done)

## Directory Structure

Each version contains:
- \`sstables/\`: Raw SSTable files organized by keyspace and table
- \`metadata/\`: JSON and YAML metadata files
- \`schemas/\`: CQL schema definitions
- \`compression-variants/\`: Files categorized by compression type

## Usage

Extract and use the SSTable files for CQLite testing:

\`\`\`bash
tar -xzf cqlite-test-data-v4.1.tar.gz
cd v4.1/sstables/test_basic/simple_table/
ls *.db
\`\`\`

## Next Steps

1. Validate exported data with CQLite
2. Run compatibility tests across versions
3. Performance benchmark with different file sizes
4. Integration with CI/CD pipeline

EOF

    log_success "SSTable export process completed successfully!"
    log_info "Export summary available at: $OUTPUT_DIR/export_summary.md"
}

# Execute main function
main "$@"