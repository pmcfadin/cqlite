# 🔄 CQLite Schema Migration Guide

## Overview

This guide walks through migrating from the scattered schema files to the new master schema system.

## Quick Migration Commands

### Step 1: Create Archive Directory
```bash
cd /Users/patrick/local_projects/cqlite
mkdir -p tests/schemas/legacy
```

### Step 2: Archive Legacy Schemas
```bash
# Archive test-env schemas
cp test-env/cassandra5/scripts/create-keyspaces.cql tests/schemas/legacy/
cp test-env/cassandra5/scripts/generate-test-data.cql tests/schemas/legacy/
cp test-env/cassandra5/*.json tests/schemas/legacy/
cp test-env/counter_schema.json tests/schemas/legacy/

# Archive test-data schemas  
cp test-data/schemas/*.cql tests/schemas/legacy/

# Archive example schemas
cp examples/schemas/*.cql tests/schemas/legacy/
cp extracted_schema.cql tests/schemas/legacy/
```

### Step 3: Update Test References
Replace references to old schema files with:
```sql
-- Source the master schema
SOURCE '/tests/schemas/master_test_schema.cql';
```

### Step 4: Update Docker Configurations
Update docker-compose files to mount master schema:
```yaml
volumes:
  - ./tests/schemas/master_test_schema.cql:/schema/master_test_schema.cql
```

## Validation

Run existing tests to ensure compatibility:
```bash
cargo test --all
```

## Rollback Procedure

If issues arise, restore from legacy:
```bash
cp tests/schemas/legacy/* test-env/cassandra5/scripts/
```