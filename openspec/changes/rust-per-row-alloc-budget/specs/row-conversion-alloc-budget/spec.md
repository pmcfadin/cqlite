# row-conversion-alloc-budget

## ADDED Requirements

### Requirement: A per-row allocation-count ratchet observes transient native allocations on the row-conversion hot path

CQLite SHALL provide a `cqlite-core` test that measures the number of heap allocations performed by the
public per-row conversion `build_row_from_scan_cached` (re-exported at `select_executor/mod.rs`) using the
in-crate counting global allocator (`crate::test_alloc_probe::measure`), and SHALL assert that the
allocations-per-row do not exceed a **measured, documented baseline**. The test SHALL drive the real public
conversion surface (not a private helper), so a regression on the row hot path is observed end-to-end. The
test SHALL be gated to the counting-allocator build
(`#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]`) so it never conflicts with the
mutually-exclusive dhat global allocator.

The measured baseline SHALL be recorded in-test (exact allocation counts for a narrow and a wide fixture)
and SHALL be tolerance-free where the counting allocator is deterministic for the fixed input; any explicit
slack SHALL be documented with its reason.

#### Scenario: The per-row conversion stays within the measured allocation budget
- **GIVEN** a synthesized wide result (a `PartitionKeyCache` plus a set of projected cells) representative
  of a scan row
- **WHEN** `build_row_from_scan_cached` is invoked inside `test_alloc_probe::measure`
- **THEN** the reported allocation count is less than or equal to a measured budget of the form
  `one-time setup + rows * (per-row + per-cell * columns)`, where the one-time term is held SEPARATE from
  the per-row term rather than amortized into it — a per-row quotient would fold the first row's
  `PartitionKeyCache` miss into the steady-state rate and make the budget valid at only one row count
- **AND** the same assertion holds for both a narrow (few-column) and a wide (many-column) fixture

#### Scenario: The counting-allocator test lane is available in the default test build
- **GIVEN** the default `cqlite-core` feature set (which includes `state_machine`) and no `dhat-heap`
  feature
- **WHEN** `cargo test -p cqlite-core` runs
- **THEN** the per-row allocation-budget test is compiled and executed (it is not silently skipped)

### Requirement: The ratchet turns the row-conversion allocation properties this crate owns into regression gates

The per-row allocation-budget test SHALL gate BOTH allocation properties this conversion owns, each with its own
differential control asserted by a strict `<` against a reference implementation of the pre-fix behaviour (an
absolute constant alone is insufficient — a constant can be re-measured upward by a later reader, a differential
cannot): (a) per-cell `Arc<str>` column-name interning (#1334), and (b) the single sized value map (#1584).
Restoring each fix returns the test to green. The negative-control deltas SHALL be documented in-test.

**Scope correction (measured, supersedes the original #1447/#1445/#1446 framing).** #1883 was filed on the
premise that this ratchet would gate #1447 (clone→move) and #1445/#1446 (key interning). Those three fixes are
**binding-layer**: #1447 is `bindings/node/src/database.rs` (`ExecuteNativeTask::compute`), #1446 is Node
JsString interning, #1445 is Python `Row` ordering. No `cqlite-core` test can gate code in the binding crates.
Measured directly: reverting the clone→move *inside `build_row_from_scan_cached`* is **exactly
allocation-neutral** (41 vs 41 narrow, 273 vs 273 wide), because `Value::Text` is `Bytes`-backed (clone is a
refcount bump) and `Value::into_owned`'s TIER-1 compaction (#1644) copies a small payload either way. A clone
control here would therefore be VACUOUS. The test SHALL NOT assert a control it cannot make fail, and SHALL
document this measurement in-test. Ratcheting #1447/#1445/#1446 requires an allocation probe inside the
binding crates, tracked as follow-up issue #2894.

#### Scenario: Dropping per-cell key interning trips the budget
- **GIVEN** the per-row allocation-budget test at its measured baseline
- **WHEN** the key handling is changed to allocate a fresh key string per projected cell instead of reusing
  the interned `Arc<str>`
- **THEN** the measured allocations grow with the projected-column count and exceed the baseline, failing the
  test (measured: narrow 41 → 89, wide 273 → 785, exactly +2 allocations per cell)
- **AND** restoring the interned handle returns the test to PASS

#### Scenario: Dropping the row map's capacity hint trips the budget
- **GIVEN** the per-row allocation-budget test at its measured baseline
- **WHEN** the row-value map is built unsized (`HashMap::new()`) instead of capacity-hinted (#1584)
- **THEN** rehash growth adds one allocation per row and the test FAILS (measured: narrow 41 → 49)
- **AND** restoring the capacity hint returns the test to PASS

#### Scenario: The clone→move control is recorded as measured-neutral rather than asserted
- **GIVEN** the per-row allocation-budget test at its measured baseline
- **WHEN** the row-conversion value insert is changed from a move (`into_owned`) back to a per-cell clone
- **THEN** the measured allocation count is UNCHANGED (the fix is not observable in this crate)
- **AND** the test documents that measurement and its cause instead of asserting a vacuous control

### Requirement: L5 (FxHashMap row map) is DEFERRED, not delivered in this change

L5 — swapping the per-row `row_values` map to `rustc_hash::FxHashMap` — was implemented during this change and
then **deliberately reverted before merge** on the owner's decision. It SHALL NOT ship here. Two facts, both
discovered by implementing it, drove the reversal:

1. **It is a PUBLIC breaking API change.** `row_values` moves directly into `QueryRow.values`, so the hasher
   cannot change without changing that public field's type (the working implementation added
   `pub type RowValues` and rippled through `cqlite-core`, `cqlite-flight` and `cqlite-cli`).
2. **It contradicts a written invariant.** `cqlite-core/Cargo.toml` reserves `rustc-hash` for
   "integer/digest-keyed hot-path maps only — NOT for maps exposed to untrusted string keys (#1590, E8)".
   `QueryRow.values` is string-keyed, and on the default read path those column names come from the file's
   `Statistics.db` serialization header — attacker-controlled for a hostile SSTable, where FxHash's easy
   collisions give O(N²) per-row inserts.

Combined with the fact that **no row-conversion benchmark was run** (so the projected ~1.04× stayed a
projection), the change SHALL NOT claim an L5 win. Revisiting L5 — behind a measured benchmark, a HashDoS
answer, and an API plan — is tracked as issue #2901.

#### Scenario: The row map keeps its default hasher in this change
- **GIVEN** the per-row conversion `build_row_from_scan_cached`
- **WHEN** this change is merged
- **THEN** `QueryRow.values` is still the default-hasher `HashMap<Arc<str>, Value>` (no public type change)
- **AND** no throughput multiplier is claimed for L5 anywhere in the change's docs

### Requirement: The L4 RowKey-hoist question is adjudicated by the ratchet's measurement, not by a speculative hoist

The change SHALL record, in `docs/architecture/throughput-program-2026-07.md` §7 M4, the per-row allocation
count measured by the new ratchet and the resulting **L4 verdict**: either a concrete follow-up issue naming
the exact hoistable per-row `Arc` allocation site (if the measurement reveals one) OR an explicit
**1.0× / no-op** credit for L4 (if the measurement shows the residual `RowKey(Arc<[u8]>)` is already
per-row-minimal, the partition-constant decode already being hoisted by `PartitionKeyCache` #1817). The
change SHALL NOT claim an L4 field win the profile cannot support.

#### Scenario: The throughput-program doc records the measured L4 verdict
- **GIVEN** the per-row allocation-budget ratchet has measured the conversion's allocations-per-row
- **WHEN** `docs/architecture/throughput-program-2026-07.md` §7 M4 is updated
- **THEN** it states the measured allocations-per-row and either links a concrete L4 follow-up issue or
  records L4 as a measured 1.0×/no-op — with no unsupported field-win claim
