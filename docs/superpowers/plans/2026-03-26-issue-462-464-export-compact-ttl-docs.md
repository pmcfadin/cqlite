# Issue #462 & #464: Export --compact Flag & TTL Documentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `--compact` flag to export-sstable CLI and document per-cell TTL via JSON mutations.

**Architecture:** Issue #464 replaces the no-op `--skip-compact` flag with an opt-in `--compact` flag that runs `maintenance_step()` before export. Issue #462 adds doc-comments to the mutation types explaining JSON-based per-cell TTL.

**Tech Stack:** Rust, clap CLI, cargo test

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `cqlite-cli/src/cli_types.rs` | Modify | Replace `--skip-compact` with `--compact` flag |
| `cqlite-cli/src/main.rs` | Modify | Update match arm for new flag name |
| `cqlite-cli/src/commands/write.rs` | Modify | Run `maintenance_step()` when compact=true |
| `cqlite-core/src/storage/write_engine/export.rs` | Modify | Remove error for `compact_before_export=true`, implement via maintenance |
| `cqlite-core/src/storage/write_engine/mutation.rs` | Modify | Add doc-comments about per-cell TTL via JSON |
| `cqlite-core/tests/write_integration.rs` | Modify | Add test for compact-before-export path |

---

### Task 1: Replace `--skip-compact` with `--compact` in CLI types (Issue #464)

**Files:**
- Modify: `cqlite-cli/src/cli_types.rs:399-416`

- [ ] **Step 1: Update ExportSstableArgs**

Replace the `skip_compact` field with a `compact` field:

```rust
/// Arguments for the export-sstable subcommand (Issue #392)
#[derive(Args, Debug, Clone)]
pub struct ExportSstableArgs {
    /// Output directory for exported SSTables
    pub output: PathBuf,
    /// Keyspace name for the exported SSTable
    #[arg(long, default_value = "export")]
    pub keyspace: String,
    /// Table name for the exported SSTable
    #[arg(long, default_value = "data")]
    pub table: String,
    /// Run compaction before export to merge multiple SSTables (Issue #464)
    #[arg(long)]
    pub compact: bool,
    /// Skip validation after export
    #[arg(long)]
    pub skip_validate: bool,
}
```

- [ ] **Step 2: Update the long_about text**

In the `Commands` enum, update the `ExportSstable` variant's `long_about` to mention `--compact`:

```rust
#[command(
    long_about = "Export data from the write engine as Cassandra-compatible SSTables. \
                  Use --compact to run compaction before export. \
                  Example: cqlite export-sstable /tmp/export --compact --writable --write-dir /path/to/data"
)]
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build --package cqlite-cli --features write-support`
Expected: Compile error in main.rs (field name changed) — that's expected, we fix it in Task 2.

---

### Task 2: Update CLI wiring for `--compact` flag (Issue #464)

**Files:**
- Modify: `cqlite-cli/src/main.rs:929-960`
- Modify: `cqlite-cli/src/commands/write.rs:279-311`

- [ ] **Step 1: Update match arm in main.rs**

Change the destructuring from `skip_compact` to `compact`:

```rust
Some(Commands::ExportSstable(ExportSstableArgs {
    output,
    keyspace,
    table,
    compact,
    skip_validate,
})) => {
    #[cfg(feature = "write-support")]
    {
        let engine = write_engine
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Export requires --writable mode"))?;
        let result = commands::write::handle_export(
            engine,
            &output,
            &keyspace,
            &table,
            compact,
            skip_validate,
        )
        .await?;
        result.display();
        Ok(())
    }
    #[cfg(not(feature = "write-support"))]
    {
        let _ = (output, keyspace, table, compact, skip_validate);
        Err(anyhow::anyhow!(
            "Write support is not enabled. Build with --features write-support to enable write operations."
        ))
    }
}
```

- [ ] **Step 2: Update handle_export signature and logic**

In `cqlite-cli/src/commands/write.rs`, change `skip_compact: bool` to `compact: bool` and run `maintenance_step()` when compact is true:

```rust
#[cfg(feature = "write-support")]
pub async fn handle_export(
    write_engine: &mut WriteEngine,
    output_dir: &Path,
    keyspace: &str,
    table: &str,
    compact: bool,
    skip_validate: bool,
) -> Result<ExportResult> {
    let start = Instant::now();

    // Run compaction before export if requested (Issue #464)
    if compact {
        use std::time::Duration;
        log::info!("Running compaction before export (--compact)");
        let budget = Duration::from_secs(300); // 5 minute budget
        let report = write_engine
            .maintenance_step(budget)
            .with_context(|| "Pre-export compaction failed")?;
        log::info!(
            "Compaction complete: {} merges, {} rows merged in {:?}",
            report.completed_merges.len(),
            report.rows_merged,
            report.time_spent
        );
    }

    let generation = write_engine.generation();
    let mut options = ExportOptions::new(keyspace, table, generation);
    if skip_validate {
        options = options.skip_validation();
    }

    let report = write_engine
        .export_sstable(output_dir, options)
        .await
        .with_context(|| "SSTable export failed")?;

    Ok(ExportResult {
        output_path: report.output_path,
        row_count: report.row_count,
        data_file_size: report.data_file_size,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
    })
}
```

- [ ] **Step 3: Remove compact_before_export error path from export.rs**

In `cqlite-core/src/storage/write_engine/export.rs`, remove the `compact_before_export` error path since compaction is now handled at the CLI level. Also remove the `skip_compaction()` method and `compact_before_export` field from `ExportOptions` since it's no longer needed there:

Actually, keep the field but remove the error — other callers (like tests) might use it. Just make the `compact_before_export = true` path a no-op with a log warning, since the CLI now handles compaction externally:

```rust
// Step 2: Full compaction (if enabled)
let source_sstable = if options.compact_before_export {
    log::warn!(
        "compact_before_export on ExportOptions is deprecated. \
         Use WriteEngine::maintenance_step() before export instead."
    );
    self.find_most_recent_sstable().await?
} else {
    log::info!("Skipping compaction, using most recent SSTable");
    self.find_most_recent_sstable().await?
};
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build --package cqlite-cli --features write-support`
Expected: PASS

- [ ] **Step 5: Run existing tests**

Run: `cargo test --package cqlite-cli --features write-support`
Expected: PASS (existing tests should not break)

- [ ] **Step 6: Commit**

```bash
git add cqlite-cli/src/cli_types.rs cqlite-cli/src/main.rs cqlite-cli/src/commands/write.rs cqlite-core/src/storage/write_engine/export.rs
git commit -m "feat: add --compact flag to export-sstable CLI (closes #464)"
```

---

### Task 3: Add per-cell TTL documentation to mutation types (Issue #462)

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mutation.rs:154-180`

- [ ] **Step 1: Add doc-comments to CellOperation enum**

Add documentation explaining per-cell TTL availability via JSON mutations:

```rust
/// Operations that can be applied to individual cells within a row.
///
/// # Per-Cell TTL
///
/// Per-cell TTL is supported via the `WriteWithTtl` variant when using
/// the JSON mutation format directly. CQL syntax (`USING TTL`) applies
/// TTL uniformly to all cells in a statement. To set different TTLs
/// per column, submit separate mutations or use JSON mutations with
/// `WriteWithTtl`:
///
/// ```json
/// {"WriteWithTtl": {"column": "session_token", "value": {"Text": "abc"}, "ttl_seconds": 3600}}
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CellOperation {
```

- [ ] **Step 2: Add doc-comment to WriteWithTtl variant**

```rust
    /// Write a value to a column with TTL (expiring cell).
    ///
    /// The cell will expire after `ttl_seconds` seconds. This is the only
    /// way to set per-column TTL — CQL `USING TTL` applies to all cells
    /// in a statement. Use JSON mutations to set different TTLs per column.
    WriteWithTtl {
```

- [ ] **Step 3: Add doc-comment to Mutation::ttl_seconds field**

```rust
    /// Time-to-live in seconds applied to all cells in this mutation (None = no expiration).
    ///
    /// This is set by `USING TTL` in CQL statements and applies uniformly to all
    /// `Write` operations. For per-column TTL, use `CellOperation::WriteWithTtl`
    /// in the operations list instead.
    pub ttl_seconds: Option<u32>,
```

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo build --package cqlite-core --features write-support && cargo test --package cqlite-core --features write-support`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mutation.rs
git commit -m "docs: document per-cell TTL via JSON mutations (closes #462)"
```

---

### Task 4: Add integration test for --compact export path (Issue #464)

**Files:**
- Modify: `cqlite-core/tests/write_integration.rs`

- [ ] **Step 1: Add test for compact-before-export deprecation warning**

Add a test that verifies `compact_before_export = true` no longer errors but logs a deprecation:

```rust
#[tokio::test]
async fn test_export_compact_before_export_deprecated() {
    let dir = tempfile::tempdir().unwrap();
    let output_dir = tempfile::tempdir().unwrap();
    let mut engine = create_test_engine(dir.path()).await;

    // Write some data
    let mutation = create_simple_mutation("key1", "value1");
    engine.apply_mutation(mutation).await.unwrap();
    engine.flush().await.unwrap();

    // compact_before_export = true should NOT error (deprecated, becomes no-op)
    let mut options = ExportOptions::new("test_ks", "test_tbl", 1);
    options.compact_before_export = true;

    let result = engine.export_sstable(output_dir.path(), options).await;
    assert!(result.is_ok(), "compact_before_export=true should not error (deprecated)");
}
```

- [ ] **Step 2: Run the new test**

Run: `cargo test --package cqlite-core --features write-support test_export_compact_before_export_deprecated`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_integration.rs
git commit -m "test: verify compact_before_export deprecation path (Issue #464)"
```

---

### Task 5: Final validation

- [ ] **Step 1: Run clippy**

Run: `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: PASS with no warnings

- [ ] **Step 2: Run fmt**

Run: `cargo fmt --check`
Expected: No formatting issues

- [ ] **Step 3: Run full test suite**

Run: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --workspace`
Expected: All tests pass

- [ ] **Step 4: Squash commits if needed and push**

```bash
git push origin main
```
