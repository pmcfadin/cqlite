# Design — intern per-cell column names

## Problem shape

Per-cell decode clones the schema column-name `String` into the cells map:

```rust
// row_data.rs:514 (simple) and :458 (complex) — fires for every non-key cell of every row
cells.insert(column.name.clone(), value);
```

The name then travels, moved (no new alloc), through a map-key carrier and into the public row:

```
row_data.rs:458/514   cells: HashMap<String, Value>            ← THE per-cell String alloc
  → block_emit_windowed.rs:414   Value::Map(vec![(Value::Text(name), value), …])  (sorted by name)
  → row_build.rs:110-112         QueryRow.values.insert(name, value)   (String moved in)
```

`QueryRow.values: HashMap<String, Value>` (`query/result.rs:68`) is the public sink. Readers reach
cells only by name (`.get(&str)`, `.keys()`): query engine (`executor.rs`, `select_executor/*`), CLI
writers (`json/csv/table.rs`), and both bindings (`bindings/{python,node}/src/*`). None index
positionally.

## Options considered

### (A) Intern name as `Arc<str>`, carried end-to-end — RECOMMENDED
Intern each column name once (schema-owned `Arc<str>`); the cells map becomes
`HashMap<Arc<str>, Value>` and `QueryRow.values` becomes `HashMap<Arc<str>, Value>`. Per-cell "clone"
is an `Arc::clone` refcount bump — no heap `String` alloc.

- **Removes the alloc**: yes — but ONLY if the `Value::Text(String)` key round-trip is removed so the
  emit→build pipeline carries the `Arc<str>` straight into `QueryRow.values`. If left in place,
  `block_emit_windowed.rs:414` would `arc.to_string()` and reintroduce the alloc. This change fixes the
  pipeline in the same PR.
- **Public blast radius (small):** `.get(&str)` / `.keys()` readers are **source-compatible**
  (`Arc<str>: Borrow<str>`). Signature touch-ups on `QueryRow::set/with_values/from_map` (accept
  `impl Into<Arc<str>>` or `Arc<str>`), and construction sites in `row_build.rs`,
  `select_executor/{mod,aggregation,predicate}.rs`. Node `streaming.rs:162` moves the whole map out and
  `value.rs:442` iterates it — both accept `Arc<str>` keys with iteration unchanged.
- **serde:** enable serde's `rc` feature so `HashMap<Arc<str>, Value>` (de)serializes; JSON/YAML/JSONL
  shape (object of name→value) is **unchanged** (an `Arc<str>` serializes as its string).
- **Ordering:** emit-time alphabetical sort (`block_emit_windowed.rs`) is preserved verbatim (sort key
  is the name string, reachable through `Arc<str>`).

### (B) Per-schema name registry (indices into `Vec<Arc<str>>`) — REJECTED
Key cells by a small integer index into a shared name table.

- Removes the alloc, but **reshapes** the cells map: every `.get(&str)` site (query engine, all 3 CLI
  writers, both bindings) must translate name→index, and the serde shape of `QueryRow` changes unless a
  custom (de)serialize reconstructs the name→value object — risking JSONL/CLI-parity drift. Far larger
  public churn for the same alloc win. Rejected.

### (C) Borrowed `&str` (zero-alloc, lifetimes into schema/buffer) — REJECTED (infeasible)
`QueryResult`/`QueryRow` are owned, `Clone`, `Serialize`/`Deserialize`, and cross the async scan
boundary (`streaming.rs` moves `row.values` out of the iterator; bindings own copies past the core
lifetime). A borrow would infect every downstream type with a lifetime and break the owned
async-result model. Infeasible.

## Decision

**Option (A): schema-owned `Arc<str>` interned names, carried end-to-end into `QueryRow.values`,
deleting the `Value::Text(String)` key round-trip; enable serde `rc`.** It is the only option that
removes the allocation without reshaping every consumer or breaking output/parity — `.get(&str)`
readers stay untouched, the name-keyed map shape (and thus JSONL/CLI parity) is preserved, and ordering
is byte-identical.

## Correctness invariant (what the audit/tests must prove)

Observed CQL values, output bytes (JSON/CSV/table), JSONL parity goldens, and row/column ordering are
**byte-identical before and after**. The change is representation-internal; any observable difference is
a defect.

## Evidence plan

`scripts/profile.sh heap` (dhat, `cqlite-core/examples/heap_profile.rs`, full scans of
`test_basic.simple_table` + a type-heavy table — the exact path hitting `row_data.rs:514`) is the
authoritative before/after. Acceptance: the per-cell name `String` allocation disappears from the dhat
total-blocks / top ranks, with the <128MB budget still met. Capture both dhat summaries in the PR.

## Risks

- **serde `rc` omission** silently breaks `QueryResult` JSON/YAML (de)serialization — enable it and keep
  a serialize round-trip test.
- **Residual re-alloc** if the `Value::Text(String)` carrier is not fully removed — the heap harness is
  the guard (the alloc must be *gone*, not moved).
- Sibling clustering-key insert (`row_data.rs:136`) is a distinct, once-per-clustering-column site;
  intern it consistently for the same reason (lower volume, same pattern).
