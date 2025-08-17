#!/usr/bin/env python3
"""
Script to systematically remove unused imports from Rust files
"""

import re
import subprocess
import sys
from pathlib import Path

def run_cargo_check():
    """Run cargo check and return unused import warnings"""
    result = subprocess.run(['cargo', 'check'], capture_output=True, text=True, cwd='/Users/patrick/local_projects/cqlite')
    return result.stderr

def parse_unused_imports(cargo_output):
    """Parse cargo check output to extract unused import information"""
    warnings = []
    lines = cargo_output.split('\n')
    
    for i, line in enumerate(lines):
        if 'unused import' in line:
            # Get the file path from the next line
            if i + 1 < len(lines) and '-->' in lines[i + 1]:
                file_line = lines[i + 1].strip()
                if '-->' in file_line:
                    file_path = file_line.split('-->')[1].strip().split(':')[0]
                    line_num = file_line.split(':')[1] if ':' in file_line else None
                    
                    # Extract the unused import name
                    import_match = re.search(r'`([^`]+)`', line)
                    if import_match:
                        import_name = import_match.group(1)
                        warnings.append({
                            'file': file_path,
                            'line': line_num,
                            'import': import_name,
                            'full_warning': line
                        })
    
    return warnings

def main():
    print("🧹 Starting systematic unused import cleanup...")
    
    cargo_output = run_cargo_check()
    warnings = parse_unused_imports(cargo_output)
    
    print(f"Found {len(warnings)} unused import warnings")
    
    # Group by file
    files_to_fix = {}
    for warning in warnings:
        file_path = warning['file']
        if file_path not in files_to_fix:
            files_to_fix[file_path] = []
        files_to_fix[file_path].append(warning)
    
    print(f"Files to fix: {len(files_to_fix)}")
    
    for file_path, file_warnings in files_to_fix.items():
        print(f"\n📁 {file_path}:")
        for warning in file_warnings:
            print(f"  - {warning['import']}")

if __name__ == "__main__":
    main()