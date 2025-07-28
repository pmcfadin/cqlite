#!/bin/bash

# Script to fix test compilation errors by correcting imports
echo "🔧 Fixing test compilation errors for Issue #9..."

# Fix CqliteError -> Error imports
echo "📝 Fixing CqliteError references..."
find tests/src -name "*.rs" -exec sed -i '' 's/use cqlite_core::error::CqliteError/use cqlite_core::error::Error/g' {} \;
find tests/src -name "*.rs" -exec sed -i '' 's/CqliteError/Error/g' {} \;

# Fix Value imports
echo "📝 Fixing Value imports..."
find tests/src -name "*.rs" -exec sed -i '' 's/use cqlite_core::types::Value/use cqlite_core::Value/g' {} \;
find tests/src -name "*.rs" -exec sed -i '' 's/use cqlite_core::Value/use cqlite_core::{Value, types::*}/g' {} \;

# Fix CqlTypeId imports
echo "📝 Fixing CqlTypeId imports..."
find tests/src -name "*.rs" -exec sed -i '' 's/use cqlite_core::types::CqlTypeId/use cqlite_core::parser::types::CqlTypeId/g' {} \;

# Fix other common missing imports
echo "📝 Fixing other missing imports..."

# Add missing use statements to files that need them
for file in $(find tests/src -name "*.rs" -exec grep -l "Value\|CqlTypeId\|RowKey\|DataType\|TableId\|StorageEngine\|SchemaManager\|Platform" {} \;); do
    echo "Fixing imports in $file"
    
    # Create a temporary file with proper imports at the top
    temp_file=$(mktemp)
    
    # Check what's already imported to avoid duplicates
    has_value=$(grep -c "use cqlite_core::Value" "$file" || echo 0)
    has_types=$(grep -c "use cqlite_core::types::" "$file" || echo 0)
    has_parser_types=$(grep -c "use cqlite_core::parser::types::" "$file" || echo 0)
    has_error=$(grep -c "use cqlite_core::error::" "$file" || echo 0)
    
    # Start with existing content
    cp "$file" "$temp_file"
    
    # Add missing imports after existing use statements
    if [[ $has_error -eq 0 ]]; then
        sed -i '' '1i\
use cqlite_core::error::{Error, Result};
' "$temp_file"
    fi
    
    if [[ $has_value -eq 0 ]]; then
        sed -i '' '1i\
use cqlite_core::Value;
' "$temp_file"
    fi
    
    if [[ $has_types -eq 0 ]]; then
        sed -i '' '1i\
use cqlite_core::types::{DataType, RowKey, TableId, ColumnId};
' "$temp_file"
    fi
    
    if [[ $has_parser_types -eq 0 ]]; then
        sed -i '' '1i\
use cqlite_core::parser::types::CqlTypeId;
' "$temp_file"
    fi
    
    # Also add commonly needed imports
    sed -i '' '1i\
use cqlite_core::{storage::StorageEngine, schema::SchemaManager, platform::Platform};
' "$temp_file"
    
    # Move the temp file back
    mv "$temp_file" "$file"
done

echo "✅ Import fixes applied. Testing compilation..."

# Test if fixes worked
if cargo test --no-run --workspace 2>&1 | grep -q "error\[E0433\]"; then
    echo "❌ Some compilation errors remain. Manual fixes may be needed."
    echo "📊 Remaining errors:"
    cargo test --no-run --workspace 2>&1 | grep "error\[E0433\]" | sort | uniq -c
    exit 1
else
    echo "✅ All compilation errors fixed!"
    exit 0
fi