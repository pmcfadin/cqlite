# Tasks — intern per-cell column names

## 1. Intern names at the source (schema)
- [ ] 1.1 Intern each column name once as `Arc<str>` on the schema (`schema/mod.rs`), or provide a
      cheap `Arc<str>` handle from the long-lived `TableSchema` column definitions. Surface exercised:
      schema column-name access consumed by the decoder.

## 2. Decoder carries the shared handle (remove the per-cell String alloc)
- [ ] 2.1 Change the cells map in `row_data.rs` to key by `Arc<str>`; replace `column.name.clone()` at
      `:458` and `:514` (and the clustering-key site `:136`) with an `Arc::clone` of the interned name.
      Surface: the scan/read decode path.
- [ ] 2.2 Remove the `Value::Text(String)` map-key round-trip so the `Arc<str>` handle is carried
      through `block_emit_windowed.rs:412-429` and `select_executor/row_build.rs:110-112` into
      `QueryRow.values` with NO `.to_string()` re-allocation. Preserve the emit-time alphabetical
      ordering exactly. Surface: emit→build pipeline.

## 3. Public row type
- [ ] 3.1 Change `QueryRow.values` to `HashMap<Arc<str>, Value>` (`query/result.rs`). Update
      `set/with_values/from_map` signatures (`impl Into<Arc<str>>` / `Arc<str>`) and `column_names()`.
      Confirm `.get(&str)`/`.keys()` readers compile unchanged via `Arc<str>: Borrow<str>`. Surface:
      public `QueryRow` API.
- [ ] 3.2 Enable serde's `rc` feature (workspace `Cargo.toml`) so `HashMap<Arc<str>, Value>`
      (de)serializes; add/keep a `QueryResult` serialize→deserialize round-trip test. Surface: serde of
      the public result type.

## 4. Consumers compile + behave unchanged
- [ ] 4.1 Update construction/read sites: query engine (`executor.rs`, `select_executor/{mod,
      aggregation,predicate,row_build}.rs`), CLI writers (`output/{json,csv,table}.rs`). Surface:
      query engine + CLI output.
- [ ] 4.2 Update bindings: Python (`bindings/python/src/result.rs`) and Node
      (`bindings/node/src/{database,streaming,value}.rs`) — accept `Arc<str>` keys; iteration/move
      sites unchanged in behavior. Surface: Python + Node public row APIs.

## 5. Prove it (wiring evidence + parity + heap)
- [ ] 5.1 Parity: run the Python parity suite (all 33 tables) + sstabledump/JSONL goldens — values
      unchanged. Surface: end-to-end read path.
- [ ] 5.2 Output determinism: CLI JSON/CSV/table output-determinism regression tests pass byte-identical.
- [ ] 5.3 Bindings: Python + Node test suites pass.
- [ ] 5.4 Heap evidence: run `scripts/profile.sh heap` before/after; confirm the per-cell column-name
      `String` allocation is GONE from the dhat top ranks (not merely moved) and the <128MB budget holds.
      Paste both dhat summaries in the PR.

## 6. Quality gates
- [ ] 6.1 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim. Run with
      `CQLITE_DATASETS_ROOT` pointed at the main repo's `test-data/datasets` (worktree lacks binaries).
- [ ] 6.2 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features` clean; no
      `unwrap()`/`expect()` in library code.
- [ ] 6.3 Intent audit **C** (spec-auditor anchored to `openspec/changes/intern-cell-names/specs/**`)
      reports PASS — every requirement satisfied with a public-surface test as evidence.
- [ ] 6.4 roborev (`--agent codex --base origin/main`) clean.
