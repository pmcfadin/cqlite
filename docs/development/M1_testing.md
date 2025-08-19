## M1 Testing Remediation Plan (Core Reading Only)

Audience: engineers and automation agents. Follow steps exactly. Do not delete tests; quarantine or gate them.

Outcome: a consistently green “M1 Core” test lane that verifies Cassandra 5 SSTable reading/parsing and header conformance, with out‑of‑scope tests preserved but not run by default.

### Scope Definitions

- In‑scope (M1 Core Reading per PRD):
  - SSTable header format and version decode (exact 32 bytes; roundtrip serialization).
  - Parsing of CQL types including collections and UDTs.
  - Buffer consumption guarantees (no trailing bytes after parse), EOF/null handling.
  - One minimal real‑fixture smoke test (Cassandra 5) proving we can read.

- Out‑of‑scope for M1 (quarantine):
  - ANTLR/CQL schema parser, query/state machine orchestration.
  - Event bus/validation metrics, tombstone/GC semantics.
  - Benchmarks/performance and large real‑data validators.

### Phased Plan (execute in order)

#### Phase 0 — Prepare and Inventory (30–60 min)

1) Create a working branch.

```bash
git checkout -b m1-test-remediation
```

2) Capture current failing tests (for traceability).

```bash
cargo test --no-run 2>/dev/null | cat
cargo test -- --list | cat
```

3) Snapshot test names to a file for reference.

```bash
cargo test -- --list | sed 's/ (.*$//' > /tmp/current_tests.txt
git add -N . && git status -s | cat
```

Acceptance criteria: branch created; list of tests saved locally; no code changes yet.

#### Phase 1 — Quarantine Out‑of‑Scope Tests (gate, don’t delete) (1–2 h)

Introduce feature flags for out‑of‑scope areas and mark remaining aspirational tests as ignored. None enabled by default.

1) Define features in `cqlite-core/Cargo.toml` (do not enable by default):

```toml
[features]
antlr = []            # CQL/ANTLR parser work (M2+)
state_machine = []    # query/state orchestration (M2+)
events = []           # validation/event recording (M2+)
tombstones = []       # tombstone/GC logic (M3+)
benchmarks = []       # opt-in perf runs
experimental = []     # umbrella for WIP tests
```

2) Gate entire modules where possible:

- For schema/ANTLR tests: add at the top of the test module/file:

```rust
#![cfg(feature = "antlr")]
```

- For state‑machine orchestration tests:

```rust
#![cfg(feature = "state_machine")]
```

- For event/validation tests:

```rust
#![cfg(feature = "events")]
```

- For tombstone/GC tests:

```rust
#![cfg(feature = "tombstones")]
```

If you cannot gate the whole module, mark individual tests:

```rust
#[ignore = "M2+ feature; gated for M1"]
#[test]
fn test_name() { /* existing body */ }
```

3) Quarantine known categories by name (do not rename tests). Locate and apply gating/ignore to:

- Schema/ANTLR:
  - `test_parse_cql_schema_enhanced`
  - `test_parse_cql_schema_simple`
  - `test_parse_cql_schemas_batch`
  - `test_validate_cql_schema_syntax`

- State machine:
  - `test_complex_collection_state_machine`
  - `test_schema_driven_udt_parsing`
  - `test_mixed_type_row_parsing`
  - `test_schema_mismatch_handling`

- Validation/events:
  - `test_event_history_limit`
  - `test_event_recording`
  - `test_get_events_by_type`
  - `test_validation_statistics`
  - `test_major_discrepancy_detection`
  - `test_zero_tolerance_evidence_generation`

- Tombstones/storage:
  - `test_collection_tombstone_handling`
  - `test_garbage_collection_identification`
  - `test_fast_tombstone_check_performance`
  - `test_sstable_id_generation`

- Misc “intentional” failure:
  - `test_memory_safety_suite` (mark `#[ignore = "intentional failure; enable when ready"]`).

4) Move or gate benchmarks so they do not run in `cargo test`:

- If a file under `src/` contains benchmark/test code (e.g., `cqlite-core/src/parser/m3_performance_benchmarks.rs`), add at its top:

```rust
#![cfg(feature = "benchmarks")]
```

- Prefer using `benches/`:

```bash
mkdir -p cqlite-core/benches
# Create a new bench file that calls into library functions
```

- Add `criterion` (optional) in `cqlite-core/Cargo.toml` under `[dev-dependencies]` and configure benches later when M6 is active.

5) Large real‑data validators (under `tests/src/*real*` or similar): gate with `#![cfg(feature = "experimental")]`.

6) Verify the suite:

```bash
cargo test | cat
```

Acceptance criteria: `cargo test` finishes without running quarantined tests; remaining failures only concern M1‑scope items (parser/header correctness).

#### Phase 2 — Stabilize In‑Scope Tests (brittleness fixes, no feature work) (0.5–1 day)

1) Add a shared test helpers module for M1:

- Create `tests/src/support/assert.rs` (or an existing test support module) with:

```rust
pub fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() <= eps.max(1e-12)
}

pub fn assert_fully_consumed(parsed_remaining: &[u8]) {
    assert!(parsed_remaining.is_empty(), "trailing bytes remain after parse: {}", parsed_remaining.len());
}
```

2) Replace exact float assertions in M1 tests:

- Find: `assert_eq!(<float_a>, <float_b>)`
- Replace with: `assert!(approx_eq(<float_a>, <float_b>, 1e-9))`

3) Enforce full buffer consumption in collection/UDT/tuple tests:

- After each parse call, assert no remaining bytes using `assert_fully_consumed`.

4) Make ID‑related tests deterministic or gated:

- If a test asserts unique/random IDs but the generator is time‑based, either:
  - Inject a deterministic seed/clock via a test‑only constructor and use it in the test, or
  - Mark those tests `#[ignore = "non-deterministic; revisit post‑M2"]` if not core to M1.

Acceptance criteria: All remaining M1 tests use tolerant float comparisons and assert full consumption; no flakiness.

#### Phase 3 — Minimal Real Fixtures + Header Snapshots (0.5 day)

1) Add a tiny Cassandra 5 fixture set (keep repo‑small):

- Path: `tests/fixtures/cassandra5/minimal/`
- Contents: the smallest valid SSTable files needed to read header and one row.
- Document provenance in a `README.md` within the fixture folder.

2) Add header snapshot tests using `insta`:

- Dev dependency in `cqlite-core/Cargo.toml`:

```toml
[dev-dependencies]
insta = { version = "1", features = ["redactions"] }
```

- Test pattern:

```rust
let header_bytes = read_header_bytes_from_fixture(/* path */)?;
insta::assert_snapshot!(hex::encode(header_bytes));
```

3) Add one smoke test that opens the minimal fixture and parses a single row end‑to‑end (no query language). Keep assertions minimal and deterministic.

Acceptance criteria: Header snapshot passes; smoke test validates we can read one real row; fixtures are small and committed.

#### Phase 4 — CI Lanes and Commands (0.5 day)

1) Ensure default CI runs only M1 core lane:

- Command: `cargo test` in workspace root, no features.
- Do not run `--ignored` by default.

2) Add optional extended lanes (allowed to fail or manual):

- Experimental: `cargo test --features experimental`
- ANTLR: `cargo test -p cqlite-core --features antlr`
- State machine: `cargo test -p cqlite-core --features state_machine`
- Events: `cargo test -p cqlite-core --features events`
- Tombstones: `cargo test -p cqlite-core --features tombstones`
- Benchmarks (manual only): `cargo bench` (later, when wired)

3) If using GitHub Actions, create separate jobs for core vs extended, with only core marked `continue-on-error: false`.

Acceptance criteria: CI is green on core lane; extended lanes are gated/optional.

#### Phase 5 — Reintroduction Plan (tag tests, map to PRD milestones)

Keep tests organized for future milestones; do not lose them.

- Tag quarantined modules with a comment header documenting the enabling feature and target milestone, e.g.:

```rust
//! Target milestone: M2 (CLI/query)
//! Enable with: --features antlr,state_machine
```

- Maintain a simple mapping table in `docs/development/TEST_MATRIX.md`:
  - M1 Core: parser, header, UDT/collections, fixture smoke.
  - M2 CLI: ANTLR/schema parsing, basic SELECT, snapshots.
  - M3 Data semantics: events, tombstones/GC, validation metrics.
  - M6 Performance: benches and perf acceptance.

Acceptance criteria: Each quarantined test/module clearly indicates when/how it returns.

### Concrete Edit Checklist (agent‑friendly)

Apply the following in order. Use search/replace precisely; do not rename tests.

1) Add features to `cqlite-core/Cargo.toml` under `[features]` exactly as shown above.

2) For files containing these tests, add the corresponding `#![cfg(feature = "…")]` line at the file top:

- Files with schema/ANTLR tests → `antlr`
- Files with state machine orchestration → `state_machine`
- Files with event/validation tests → `events`
- Files with tombstone/GC tests → `tombstones`
- Files with perf/benchmark code → `benchmarks`

3) If a file mixes in‑scope and out‑of‑scope tests and cannot be gated as a whole, add `#[ignore = "M2+ feature; gated for M1"]` to only the out‑of‑scope tests from the lists in Phase 1 step 3.

4) Create `tests/src/support/assert.rs` with `approx_eq` and `assert_fully_consumed` helpers (Phase 2 step 1). Import and use them in M1 tests. Prefer `assert!(approx_eq(...))` over `assert_eq!` for floats.

5) Where a parse returns remaining bytes or a slice cursor, assert zero remaining bytes using `assert_fully_consumed` after a successful parse.

6) For ID/time/rand‑dependent tests within M1, inject determinism; otherwise mark them ignored with a reason.

7) Add minimal Cassandra 5 fixtures under `tests/fixtures/cassandra5/minimal/` and write one header snapshot test using `insta` and one row‑level smoke test.

8) Update CI to run only `cargo test` (no features, no `--ignored`) for the required gate.

### Verification Commands

Run locally after each phase:

```bash
# Core lane
cargo test | cat

# Extended (manual, optional)
cargo test --features experimental | cat
cargo test -p cqlite-core --features antlr | cat
cargo test -p cqlite-core --features state_machine | cat
cargo test -p cqlite-core --features events | cat
cargo test -p cqlite-core --features tombstones | cat
```

### Acceptance for Completion

- `cargo test` (no features) passes locally and in CI.
- Only M1‑scope tests execute by default.
- Quarantined tests are preserved, discoverable, and clearly labeled with re‑enable conditions.
- Minimal real‑fixture smoke test and header snapshot exist and pass.

### Risks and Mitigations

- Risk: Accidentally gating in‑scope tests. Mitigation: cross‑check against PRD M1 list; keep a short mapping in `TEST_MATRIX.md`.
- Risk: Fixtures balloon repo size. Mitigation: keep minimal fixture; compress if needed.
- Risk: Flakiness persists. Mitigation: remove time/rand from assertions; use deterministic helpers.

### Rollback Plan

- Branch isolates changes. If core lane regresses, revert gating commits while keeping helper improvements.

