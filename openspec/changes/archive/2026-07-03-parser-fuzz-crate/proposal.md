## Why

The parser decodes **untrusted bytes** — SSTable component files can come from any source — yet the
repo has **no fuzz target anywhere**. The parser's safety discipline (zero `unsafe`, length-guarded
slices, `Err`-not-panic) has never been proven against adversarial input. Any panic that reaches a
binding aborts the host process today (see #1440); recent P0 fixes hardened specific spots
(depth-guard #1690, VInt framing #1624, BTI cap #1629) but nothing continuously exercises the decode
surface against arbitrary bytes.

This is epic #1601's parser safety net (finding H1, audit block 2). It is **design-driven**: there is
no external byte-oracle to match; the deliverable is a standing fuzz harness + the *invariant* it
proves (never panic/hang/OOM on arbitrary input), and the harness layout/wiring has latitude.

## What changes

- **Milestone:** safety net / robustness hardening (epic #1601). **Design-driven.**
- Add a **cargo-fuzz / libFuzzer** crate at `fuzz/`, **excluded from the main workspace** so
  `scripts/agent-gate.sh` and every normal `cargo` build are unaffected.
- Add **five** fuzz targets, each enforcing the same contract — arbitrary input → `Ok` or `Err`,
  **never panic, never hang, never OOM**:
  - `fuzz_vint` — `parse_vint` / `parse_vuint` / `parse_vint_length` (`cqlite-core/src/parser/vint.rs`).
  - `fuzz_value_decode` — the schema-typed value decoder over a fixed type list (every scalar +
    `list<int>`, `set<text>`, `map<text,int>`, a tuple, nested `frozen<list<list<int>>>`).
  - `fuzz_block_emit` — the decompressed-block partition-loop entry (`parse_block_emit`,
    `.../v5_compressed_legacy/`) against one fixed simple schema (`test_basic.simple_table`).
  - `fuzz_bti` — BTI node decode + DFS traversal (`.../bti/parser/node_decode.rs`, `traversal.rs`).
  - `fuzz_schema_parse` — arbitrary strings to `parse_create_table` / `cql_type` / `cql_type_to_type_id`
    (`cqlite-core/src/schema/cql_parser.rs`). **Queued from #1690** (schema type-parser depth-guard,
    merged PR #1739): generalizes the deep-nesting regression coverage. Filed as a follow-up note on
    this issue by the #1690 worker + the delivery lead's block-6 filing — reconciled here.
- Add a **feature-gated fuzz-support surface** on `cqlite-core` (a `fuzz` feature exposing the exact
  internal entry points the targets need) so the external fuzz crate reaches `pub(crate)` parsers
  **without widening the default public API** (see design.md).
- **Seed corpora** from small real component files under `test-data/datasets/sstables/`.
- **CI:** a PR **smoke lane** (each target run for a short bounded time) + a **nightly long-run** job,
  in their own workflow (fuzzing needs nightly Rust) so the stable gate is untouched.

## Non-goals

- Do **not** add the fuzz crate to the default workspace build or the agent gate.
- Do **not** silently "fix" crashes found by fuzzing inside this issue — each crash is filed as its own
  bug issue with the reproducer attached (unless it is a one-line guard). The targets are the standing
  net; finding a crash later is a *success*, tracked separately.
- No change to parser behavior/semantics: this issue only *observes* the parser under adversarial input.

## Doctrine impact

Adds the fuzz harness to the contributor-facing safety-net story. Update CLAUDE.md (a short
"Fuzzing" note pointing at `fuzz/` + how to run a target) and mirror on the `agents-developing/` site
as part of this change.
