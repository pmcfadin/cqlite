# M1 Test Commands - How to Run Tests

## Quick Test Commands

### Run all tests (default, no features)
```bash
cargo test
```

### Run only library tests (faster)
```bash
cargo test --lib
```

### Run tests with output on failure
```bash
cargo test -- --nocapture
```

### Run specific test
```bash
cargo test test_name_here
```

## Verify Clean Compilation

### Clean build (no features)
```bash
cargo clean
cargo build
```

### Check for warnings
```bash
cargo build 2>&1 | grep warning || echo "No warnings"
```

## Feature-Gated Tests

### Run with benchmarks feature
```bash
cargo test --features benchmarks
```

### Run with specific features
```bash
cargo test --features antlr
cargo test --features state_machine
cargo test --features events
cargo test --features tombstones
```

## Verify Quarantined Tests

### Check that quarantined tests are ignored
```bash
# Should show 0 tests running
cargo test test_parse_cql_schema_enhanced 2>&1 | grep "running"
cargo test test_event_recording 2>&1 | grep "running"
```

### List all ignored tests
```bash
cargo test -- --ignored --list | head -20
```

## CI-Ready Commands

### Core M1 lane (should pass)
```bash
cargo test --no-fail-fast
```

### With all output
```bash
cargo test --no-fail-fast -- --nocapture
```

## Troubleshooting

### If you see the dead code warning
The warning about `should_include_value_after_merge`, `extract_ttl_from_value`, and `extract_write_time_from_value` has been fixed by adding `#[cfg(feature = "tombstones")]` guards to these methods in `cqlite-core/src/storage/sstable/reader.rs`.

### To verify the fix
```bash
cargo build 2>&1 | grep -E "should_include_value_after_merge|extract_ttl_from_value|extract_write_time_from_value" || echo "No warnings"
```

## Summary

For M1 validation, the primary command is simply:
```bash
cargo test
```

This runs all M1-scope tests without any feature flags, with quarantined tests properly excluded. The test suite should compile cleanly and run without the dead code warnings.