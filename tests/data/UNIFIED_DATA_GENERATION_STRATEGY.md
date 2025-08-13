# 📊 Unified Test Data Generation Strategy

## Overview

This document outlines the strategy for generating consistent, comprehensive test data from the master schema.

## Data Generation Architecture

### Single Source Approach
- **Master Schema**: `/tests/schemas/master_test_schema.cql`
- **Generated Data**: All data derives from master schema
- **Consistency**: Same schema = same data structure everywhere

### Data Categories

#### 1. Basic Type Data
```python
# Generate data for comprehensive_types table
data = {
    'text_field': ['Sample text', 'Unicode: 你好世界', ''],
    'int_field': [0, 2147483647, -2147483648],
    'bigint_field': [0, 9223372036854775807, -9223372036854775808],
    'boolean_field': [True, False, None],
    # ... all 21 CQL types
}
```

#### 2. Collection Data
```python
# Generate complex nested collections
nested_data = {
    'ultra_nested': {
        'category1': [{'tag1', 'tag2'}, {'tag3'}],
        'category2': [{'tag4', 'tag5'}]
    }
}
```

#### 3. UDT Data
```python
# Generate deeply nested UDT data
enterprise_portfolio = {
    'portfolio_id': uuid4(),
    'business_relationships': [
        {
            'primary_organization': {
                'key_personnel': [
                    {
                        'contact_info': {
                            'email': 'test@example.com'
                        }
                    }
                ]
            }
        }
    ]
}
```

#### 4. Performance Data
- Large partitions (1000+ rows per partition)
- Wide rows (150+ columns)
- Large collections (10000+ elements)
- Binary data (1MB+ blobs)

#### 5. Edge Case Data
- NULL values in every position
- Empty collections
- Unicode and special characters
- Extreme numeric values
- Large text fields (1MB+)

## Implementation Plan

### Phase 1: Core Data Generators
Create Python scripts for each data category:

```
tests/data/generators/
├── basic_types_generator.py
├── collection_generator.py
├── udt_generator.py
├── performance_generator.py
└── edge_case_generator.py
```

### Phase 2: Docker Integration
Update Docker compose to use new generators:

```yaml
services:
  data-generator:
    build: ./tests/data/generators
    volumes:
      - ./tests/schemas/master_test_schema.cql:/schema.cql
    environment:
      - SCHEMA_FILE=/schema.cql
```

### Phase 3: CI/CD Integration  
Add data generation to CI pipeline:

```yaml
- name: Generate Test Data
  run: |
    cd tests/data/generators
    python master_data_generator.py
```

## Data Validation

### Schema Compliance
- All generated data must validate against master schema
- Type checking for all 21 CQL types
- Collection structure validation
- UDT nesting validation

### Coverage Metrics
- 100% of master schema tables have data
- All edge cases covered
- Performance scenarios populated
- Compression testing data included

## Benefits

1. **Consistency**: All test data derives from single schema
2. **Completeness**: Covers all CQL features comprehensively  
3. **Maintainability**: One schema to rule them all
4. **Scalability**: Easy to add new scenarios
5. **Validation**: Built-in schema compliance checking