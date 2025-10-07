## QueryResult Interface Contract (CLI Writers)

Defines how CLI output writers consume `cqlite-core::query::QueryResult` to produce table/JSON/CSV outputs with stable, cqlsh-compatible formatting in M2.

---

### Structures (from core)

```rust
// cqlite_core::query::result
pub struct QueryResult {
  pub rows: Vec<QueryRow>,
  pub rows_affected: u64,
  pub execution_time_ms: u64,
  pub metadata: QueryMetadata,
}

pub struct QueryRow {
  pub values: std::collections::HashMap<String, Value>,
  pub key: RowKey,
  pub metadata: RowMetadata,
}

pub struct QueryMetadata {
  pub columns: Vec<ColumnInfo>,
  pub total_rows: Option<u64>,
  pub plan_info: Option<PlanInfo>,
  pub performance: PerformanceMetrics,
  pub warnings: Vec<String>,
}

pub struct ColumnInfo {
  pub name: String,
  pub data_type: cqlite_core::types::DataType,
  pub nullable: bool,
  pub position: usize,
  pub table_name: Option<String>,
}
```

---

### Guarantees for Writers

1. Column order: use `metadata.columns` sequence as the single source of truth. Do not rely on `HashMap` iteration order.
2. Column names: display `ColumnInfo.name` verbatim; case as provided by schema/planner.
3. Row access: for each column name in order, look up `row.values[name]`; if missing, treat as null.
4. Nulls: render as `null` in JSON, empty cell in table/CSV (match cqlsh conventions).
5. Totals: prefer `metadata.total_rows` when rendering row count suffix; otherwise fall back to `rows.len()`.
6. Timing: use `execution_time_ms` for “X ms” display when requested.

---

### Value → String Mapping (M2)

- UUID/TimeUUID: lowercase hyphenated.
- Timestamps: `YYYY-MM-DD HH:MM:SS[.fff][+0000]`, default UTC.
- Collections: list `[a, b]`, set `{a, b}`, map `{k: v}`.
- Blob: `0x`-prefixed lowercase hex.
- Boolean: `true`/`false`.
- Numbers: standard Rust formatting; avoid scientific unless necessary.

Writers should use a thin adapter layer to format `Value` according to these rules without changing core types.

---

### Writer Responsibilities

Table (cqlsh-compatible):
- Use `metadata.columns` for headers and order
- Right-align numeric where applicable; stable separators and header rules via `CqlshTableFormatter`
- Print `(N rows)` footer matching cqlsh style

JSON:
- Emit an array of objects
- Keys in column order (materialize in order rather than iterating HashMap)
- Render nulls as `null`

CSV:
- First row: headers from `metadata.columns`
- Values stringified per mapping rules; empty for null

---

### Large Results & Pagination (M2)

- Writers operate on the in-memory `rows` provided (bounded by `LIMIT`/page size).
- REPL/CLI should apply `--limit` and `--page-size` earlier in the pipeline to avoid unbounded memory.

---

### Backwards/Future Compatibility

- Additional metadata fields may be added; writers should ignore unknown metadata.
- If column aliases are introduced in future SELECTs, `metadata.columns` will carry final display names.


