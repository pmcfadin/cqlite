#!/bin/bash
# Script to validate and report remaining skip patterns in test files

echo "=== Issue #198: Test Skip Pattern Analysis ==="
echo ""

echo "Files with skip patterns remaining:"
echo ""

# Search for skip patterns in all test files
echo "## cqlite-core/tests/"
grep -r "println.*Skipping test" /Users/patrick/local_projects/cqlite/cqlite-core/tests/*.rs 2>/dev/null | wc -l | xargs echo "  Patterns found:"

echo ""
echo "## cqlite-cli/tests/"
grep -r "eprintln.*Skipping test" /Users/patrick/local_projects/cqlite/cqlite-cli/tests/*.rs 2>/dev/null | wc -l | xargs echo "  Patterns found:"

echo ""
echo "## tests/src/"
grep -r "println.*Skipping" /Users/patrick/local_projects/cqlite/tests/src/*.rs 2>/dev/null | wc -l | xargs echo "  Patterns found:"

echo ""
echo "=== Detailed Breakdown ==="
echo ""
echo "Top files still needing fixes:"
grep -r "println\|eprintln.*Skip" /Users/patrick/local_projects/cqlite/cqlite-core/tests/*.rs /Users/patrick/local_projects/cqlite/cqlite-cli/tests/*.rs 2>/dev/null | cut -d: -f1 | sort | uniq -c | sort -rn | head -10

