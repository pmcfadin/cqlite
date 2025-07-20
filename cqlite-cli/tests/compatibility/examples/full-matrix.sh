#!/bin/bash
# Full compatibility matrix example

echo "🔄 Running full compatibility matrix across all Cassandra versions..."

# Run comprehensive matrix test
./scripts/compatibility_checker.sh --matrix --test-suite comprehensive --parallel

echo "📊 Matrix testing completed! View dashboard:"
echo "  open ./results/compatibility-dashboard.md"