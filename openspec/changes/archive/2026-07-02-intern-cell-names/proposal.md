## Why

Follow-up from #1046 (PR #1332). dhat heap profiling of the read path ranks the per-cell
column-name `String` clone as allocation **rank ~3**: the row decoder clones the schema's
column-name `String` into the returned cells map **once per cell, per row**
(`cqlite-core/.../row_decoder/row_data.rs:458` and `:514`). A full scan of a wide table
therefore allocates `#rows × #columns` short-lived `String`s whose contents are identical to the
long-lived schema column names.

A real, durable reduction cannot be a tight buffer-reuse — it requires **sharing** the name across
cells instead of cloning it. That means changing how the public row representation carries column
names, so this is **design-driven** (touches a public surface), not an oracle-driven bug fix.

## What constrains the design (audit facts)

- There is **no `Value::Row` enum variant**. The issue's "row representation" is the public
  `QueryRow.values: HashMap<String, Value>` (`cqlite-core/src/query/result.rs:65-85`); the cells map
  key is the column-name `String`.
- The single per-cell allocation is `column.name.clone()` at `row_data.rs:458/514`. Downstream the
  name is **moved** (no new alloc) through a `Value::Text(name)` map-key carrier
  (`block_emit_windowed.rs:412-429`) into `QueryRow.values` (`select_executor/row_build.rs:110-112`).
- **Load-bearing trap:** because the name is *moved* into `Value::Text(String)` today, naively
  interning only the `cells` map key (`String` → `Arc<str>`) would force a `.to_string()` re-alloc at
  `block_emit_windowed.rs:414` — **cancelling the win**. The interned name must be carried end-to-end
  into `QueryRow.values`, deleting the `Value::Text(String)` key round-trip.
- All readers access cells by name via `.get(&str)` / `.keys()` (query engine, all three CLI writers,
  Python + Node bindings) — never positionally. `Arc<str>: Borrow<str>` keeps those readers
  source-compatible.
- `QueryRow` is `#[derive(Serialize, Deserialize)]`; a `HashMap<Arc<str>, Value>` requires serde's
  **`rc`** feature (not currently enabled).
- Row ordering is imposed at emit time (alphabetical by name, `block_emit_windowed.rs:393-429`), not
  carried by the map type — so the interning change must preserve that emit-time ordering exactly.

## What changes

- **Milestone:** M7 (perf validation) / read-path allocation reduction. **Design-driven.**
- Adds a `query-row-representation` capability documenting the invariant: **column names in a decoded
  row are shared (interned), not cloned per cell — with observed CQL values, output, and ordering
  byte-identical.**
- Interns column names once (schema-owned `Arc<str>`) and carries the shared handle from the decoder
  through the emit→build pipeline into `QueryRow.values`, removing the per-cell `String` allocation
  **and** the `Value::Text(String)` re-alloc trap.

## Non-goals

- No change to observed CQL values, JSON/CSV/table output bytes, JSONL parity goldens, or row/column
  ordering. This is a representation-internal optimization; outputs are invariant.
- No new public query API, no positional/index-based cells access (option B registry is rejected — see
  design.md).
- No change to the SSTable binary format, parsing correctness, or the no-heuristics mandate.
- Not a general zero-copy/borrowed-row rework (option C is infeasible against the owned async-result
  model — see design.md).

## Doctrine impact

None to CLAUDE.md or the agents-developing site — this is an internal representation change with no
new workflow or contributor-facing rule. The dhat before/after evidence is captured in the PR, not in
doctrine.
