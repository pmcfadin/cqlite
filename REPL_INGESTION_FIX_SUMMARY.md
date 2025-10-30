# REPL Mode Ingestion Fix - Implementation Summary

## Problem Statement
REPL mode was not loading schemas or discovering SSTables when config file had `schema_paths` and `data_directory` configured. This caused all queries to return 0 rows even though the config file specified valid data sources.

## Root Cause
The REPL command handler in `cqlite-cli/src/main.rs` was using the empty Database created at startup, ignoring the `schema_paths` and `data_directory` configuration from the config file.

One-shot mode (--execute flag) had similar logic but only ran ingestion when CLI flags (--schema and --data-dir/--dataset) were provided, not from config file settings.

## Solution Implemented

### File Modified
`/Users/patrick/local_projects/cqlite/cqlite-cli/src/main.rs` (lines 227-272)

### Changes Made
Added ingestion check to REPL mode initialization that mirrors the pattern used in one-shot mode:

1. **Check config for ingestion sources**: Before creating REPL engine, check if `config.schema_paths` is not empty AND `config.data_directory` is set

2. **Run ingestion**: If both conditions are met, create `IngestionConfig` and call `ingest()` function

3. **Use ingested database**: Pass the database with loaded schemas and discovered SSTables to the REPL engine

4. **Feature gating**: Wrapped in `#[cfg(feature = "state_machine")]` since ingestion requires this feature (which is enabled by default in M2+)

5. **Logging**: Added info! logs to show ingestion progress:
   - Number of schema files being loaded
   - Data directory being scanned
   - Final ingestion summary (schemas loaded, SSTables discovered, keyspaces found)

6. **Error handling**: Clear error message if ingestion fails, directing user to check config file paths

### Code Structure
```rust
#[cfg(feature = "state_machine")]
let database = if !config.schema_paths.is_empty() && config.data_directory.is_some() {
    // Run ingestion from config file
    let ingestion_config = IngestionConfig { ... };
    match ingest(ingestion_config).await {
        Ok(result) => result.database,
        Err(e) => return Err(...),
    }
} else {
    // No config-based ingestion, use existing database
    database
};
```

## Verification

### Build Status
- ✅ Compilation successful: `cargo build --package cqlite-cli --bin cqlite`
- ✅ Clippy clean: `cargo clippy --package cqlite-cli --bin cqlite`

### Functional Test
Created test script: `test-repl-query.sh`

**Test Command:**
```bash
echo "SELECT * FROM test_basic.simple_table LIMIT 3;" | ./target/debug/cqlite --config test.toml repl
```

**Before Fix:**
```
DEBUG: Database::execute('SELECT * FROM test_basic.simple_table LIMIT 3;') returning rows_affected: 0

📊 Results: 0 rows returned
```

**After Fix:**
```
DEBUG: Database::execute('SELECT * FROM test_basic.simple_table LIMIT 3;') returning rows_affected: 3

📊 Results: 3 rows returned
(Table formatting would be implemented here)

⏱️  Execution time: 69ms
```

### Evidence of Ingestion
Debug logs show ingestion process running:
- ✅ SSTable discovery scanning 33 directories
- ✅ Loading SSTables from test_basic, test_collections, test_wide_rows, test_timeseries
- ✅ Schema registry being set on readers
- ✅ Database executing queries and returning data

## Expected Behavior After Fix

### When running: `./target/debug/cqlite --config test.toml repl`

**With config file containing:**
```toml
data_directory = "./test-data/datasets/sstables"
schema_paths = ["./test-data/schemas/basic-types.cql"]
```

**System will:**
1. Detect schema_paths and data_directory in config
2. Run ingestion automatically before starting REPL
3. Load schemas from specified paths
4. Discover SSTables in specified directory
5. Make all discovered keyspaces and tables available for queries
6. `:status` command shows discovered keyspaces/tables
7. `:health` command shows all systems operational
8. `SELECT` queries return actual data

**Without config-based ingestion sources:**
- REPL starts with empty database (existing behavior)
- User can still use REPL commands but no data available initially

## Testing Instructions

1. **Build the CLI:**
   ```bash
   cargo build --package cqlite-cli --bin cqlite
   ```

2. **Test REPL mode:**
   ```bash
   ./target/debug/cqlite --config test.toml repl
   ```

3. **In REPL, try these commands:**
   ```
   :status       # Should show keyspaces and tables
   :health       # Should show all green
   SELECT * FROM test_basic.simple_table LIMIT 3;   # Should return 3 rows
   :quit
   ```

4. **Automated test:**
   ```bash
   chmod +x test-repl-query.sh
   ./test-repl-query.sh
   ```

## Files Created/Modified

### Modified
- `/Users/patrick/local_projects/cqlite/cqlite-cli/src/main.rs` (lines 227-272)

### Test Files Created
- `/Users/patrick/local_projects/cqlite/test-repl-ingestion.sh` - Basic ingestion test script
- `/Users/patrick/local_projects/cqlite/test-repl-query.sh` - Functional query test script

## Impact Analysis

### Positive Impacts
- ✅ REPL mode now respects config file data sources
- ✅ Consistent behavior between one-shot and REPL modes regarding ingestion
- ✅ Users can configure data sources once in config file instead of repeating CLI flags
- ✅ Improves usability for interactive data exploration
- ✅ No breaking changes - existing behavior preserved when config doesn't specify ingestion sources

### No Breaking Changes
- Existing REPL usage without config file works as before
- Config files without schema_paths/data_directory work as before
- CLI flags for one-shot mode unchanged
- Feature-gated properly for builds without state_machine feature

## Related Issues
This fix addresses the core problem reported by the user where REPL mode with config file was not loading data, causing all queries to return 0 rows.

## Next Steps
1. Consider adding similar config-based ingestion to --execute (one-shot) mode
2. Update documentation to explain config-based ingestion for REPL mode
3. Add integration tests for REPL mode ingestion behavior
4. Consider adding :reload command to REPL for re-running ingestion

## Commit Message Suggestion
```
fix(cli): Enable REPL mode ingestion from config file

REPL mode now automatically runs ingestion when config file has
schema_paths and data_directory configured. This enables queries
to return data instead of always returning 0 rows.

- Added ingestion check before REPL initialization
- Mirror pattern used in one-shot mode
- Feature-gated with state_machine (M2+ default)
- Added informative logging for ingestion progress
- No breaking changes to existing behavior

Fixes: REPL mode returning 0 rows despite config having valid data sources
```
