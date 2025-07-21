# M3 Cassandra SSTable Data Acquisition Strategy

## 🎯 CRITICAL MISSION: Get Real Cassandra 5+ SSTable Files with Complex Types

This document outlines our strategy for acquiring real Cassandra SSTable files containing complex types to PROVE CQLite compatibility.

## 📊 Current Status
- **Analysis Complete**: Existing codebase has partial complex type support
- **Gap Identified**: Need REAL Cassandra 5+ format validation
- **Priority**: Acquire test data files containing all 4 complex type categories

## 🎯 Complex Type Categories to Validate

### 1. Collections (with Cassandra 5+ tuple representation)
- **List<T>**: `CREATE TABLE test_lists (id UUID PRIMARY KEY, my_list list<int>)`
- **Set<T>**: `CREATE TABLE test_sets (id UUID PRIMARY KEY, my_set set<text>)`
- **Map<K,V>**: `CREATE TABLE test_maps (id UUID PRIMARY KEY, my_map map<text, int>)`

### 2. User Defined Types (UDT)
```cql
CREATE TYPE address (
    street text,
    city text,
    zip_code int
);
CREATE TABLE test_udts (
    id UUID PRIMARY KEY,
    home_address address,
    work_address address
);
```

### 3. Tuples
- **Simple**: `CREATE TABLE test_tuples (id UUID PRIMARY KEY, coordinates tuple<float, float>)`
- **Complex**: `CREATE TABLE test_complex_tuples (id UUID PRIMARY KEY, data tuple<int, text, list<int>>)`

### 4. Frozen Types
- **Frozen Collections**: `CREATE TABLE test_frozen (id UUID PRIMARY KEY, tags frozen<set<text>>)`
- **Frozen UDT**: `CREATE TABLE test_frozen_udt (id UUID PRIMARY KEY, addr frozen<address>)`

## 🛠️ Acquisition Strategy

### Option 1: Generate Test Data (PREFERRED)
Create a Cassandra 5+ cluster and generate test SSTable files:

```bash
# Step 1: Install Cassandra 5.0
# Step 2: Create test schemas
# Step 3: Insert sample data
# Step 4: Flush to SSTable files
# Step 5: Extract SSTable files for testing
```

### Option 2: Public Test Data Sources
- Apache Cassandra test suite
- Cassandra documentation examples
- Community-provided test datasets

### Option 3: Synthetic Generation
- Use Cassandra's binary format specification
- Generate valid SSTable files programmatically
- Ensure compliance with Cassandra 5+ format

## 📁 Test Data Requirements

### File Types Needed:
1. **Data.db** - Main SSTable data file
2. **Index.db** - Row index file  
3. **Filter.db** - Bloom filter file
4. **CompressionInfo.db** - Compression metadata
5. **Statistics.db** - Table statistics
6. **Summary.db** - Index summary

### Data Scenarios:
1. **Simple Collections**: Basic list/set/map with primitive types
2. **Nested Collections**: list<map<text, int>>, set<tuple<int, text>>
3. **Complex UDTs**: Nested UDTs, UDTs with collections
4. **Edge Cases**: Empty collections, null values, large datasets
5. **Mixed Types**: Tables with multiple complex type columns

## 🎯 Success Criteria

### File Validation:
- [ ] Files parse without corruption errors
- [ ] Complex type metadata reads correctly
- [ ] Binary data decodes to expected values
- [ ] Performance meets benchmarks

### Data Coverage:
- [ ] All 4 complex type categories represented
- [ ] Edge cases covered (empty, null, nested)
- [ ] Real-world data patterns included
- [ ] Multiple SSTable formats tested

## 🔄 Implementation Plan

### Phase 1: Setup Cassandra 5+ Environment
```bash
# Install Cassandra 5.0+
docker run -d --name cassandra cassandra:5.0
# Or use local installation
```

### Phase 2: Create Test Schemas
```cql
-- Execute CQL scripts to create all test tables
-- Insert representative data for each complex type
-- Force flush to generate SSTable files
```

### Phase 3: Extract and Organize
```bash
# Copy SSTable files from Cassandra data directory
# Organize by complex type category
# Document schema definitions
```

### Phase 4: Validate CQLite Compatibility
```rust
// Test each SSTable file with CQLite parser
// Verify complex type parsing works correctly
// Generate validation report
```

## 📊 Expected Deliverables

1. **Real SSTable Files**: Organized collection of Cassandra 5+ files
2. **Schema Definitions**: JSON schemas for each test table
3. **Test Data Manifest**: Documentation of what each file contains
4. **Validation Scripts**: Automated testing of CQLite compatibility
5. **Performance Baselines**: Expected performance for each operation

## 🚨 Risk Mitigation

### Cassandra Version Compatibility
- Test with both Cassandra 4.x and 5.x formats
- Document any format changes between versions
- Maintain backward compatibility where possible

### Data Quality
- Generate sufficient data volume for performance testing
- Include edge cases and error conditions
- Validate data integrity before testing

### Legal Considerations
- Use only public domain or generated test data
- Document data sources and licenses
- Avoid using production or sensitive data

## 📞 Coordination Notes

This acquisition strategy coordinates with:
- **Complex_Type_Architect**: Schema design validation
- **Collections_Specialist**: Tuple format verification  
- **UDT_Expert**: UDT schema parsing requirements
- **Performance_Guardian**: Benchmark data requirements

---

**CRITICAL**: Without real Cassandra SSTable files, we cannot PROVE CQLite compatibility. This is the highest priority task for M3 validation.