#!/usr/bin/env python3
"""
Script to fix unused imports in Rust files.
This script parses cargo test output and removes unused imports.
"""

import re
import subprocess
import sys
from pathlib import Path

def get_unused_imports():
    """Run cargo test and extract unused import warnings."""
    try:
        result = subprocess.run(
            ['cargo', 'test', '--workspace', '--', '--nocapture'],
            capture_output=True,
            text=True,
            cwd='.'
        )
        
        # Find all unused import warnings
        unused_imports = []
        lines = result.stderr.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i]
            if 'unused import' in line:
                # Extract file path and line number
                if '-->' in line:
                    i += 1
                    continue
                    
                # Look for the next line with file path
                for j in range(i + 1, min(i + 5, len(lines))):
                    if '-->' in lines[j]:
                        parts = lines[j].split('-->')
                        if len(parts) > 1:
                            file_info = parts[1].strip()
                            file_parts = file_info.split(':')
                            if len(file_parts) >= 2:
                                file_path = file_parts[0].strip()
                                line_num = int(file_parts[1].strip())
                                
                                # Extract the import text
                                import_text = None
                                for k in range(j + 1, min(j + 10, len(lines))):
                                    if lines[k].strip().startswith('|') and 'use ' in lines[k]:
                                        import_line = lines[k].split('|', 1)[1].strip()
                                        unused_imports.append({
                                            'file': file_path,
                                            'line': line_num,
                                            'import': import_line
                                        })
                                        break
                        break
            i += 1
            
        return unused_imports
        
    except subprocess.CalledProcessError as e:
        print(f"Error running cargo test: {e}")
        return []

def remove_unused_import_line(file_path, line_num):
    """Remove a specific line from a file."""
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        if 1 <= line_num <= len(lines):
            # Check if the line contains 'use'
            if 'use ' in lines[line_num - 1]:
                print(f"Removing line {line_num} from {file_path}: {lines[line_num - 1].strip()}")
                
                # Remove the line
                del lines[line_num - 1]
                
                # Write back to file
                with open(file_path, 'w') as f:
                    f.writelines(lines)
                    
                return True
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def fix_specific_unused_imports():
    """Fix specific unused imports that we identified."""
    fixes = [
        # Format: (file_path, line_number, import_text)
        ("cqlite-cli/tests/unit_tests.rs", 106, "use clap::Parser;"),
        ("cqlite-cli/tests/unit_tests.rs", 413, "use super::*;"),
        ("cqlite-cli/tests/comprehensive_test_framework.rs", 17, "use std::process::Command;"),
        ("cqlite-cli/tests/comprehensive_test_framework.rs", 18, "use std::sync::Arc;"),
        ("cqlite-cli/tests/comprehensive_test_framework.rs", 12, "use assert_cmd::prelude::*;"),
    ]
    
    for file_path, line_num, import_text in fixes:
        try:
            full_path = Path(file_path)
            if full_path.exists():
                with open(full_path, 'r') as f:
                    lines = f.readlines()
                
                # Find and remove the matching line
                for i, line in enumerate(lines):
                    if import_text.strip() in line.strip():
                        print(f"Removing from {file_path}: {line.strip()}")
                        del lines[i]
                        
                        with open(full_path, 'w') as f:
                            f.writelines(lines)
                        break
        except Exception as e:
            print(f"Error fixing {file_path}: {e}")

def main():
    print("Fixing unused imports in Rust project...")
    
    # Run a simplified approach first
    fix_specific_unused_imports()
    
    print("Done! Run 'cargo test' again to verify fixes.")

if __name__ == "__main__":
    main()