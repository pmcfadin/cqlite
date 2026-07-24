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
- **THEN** the reported allocation count divided by the number of rows converted is less than or equal to
  the documented measured baseline
- **AND** the same assertion holds for both a narrow (few-column) and a wide (many-column) fixture

#### Scenario: The counting-allocator test lane is available in the default test build
- **GIVEN** the default `cqlite-core` feature set (which includes `state_machine`) and no `dhat-heap`
  feature
- **WHEN** `cargo test -p cqlite-core` runs
- **THEN** the per-row allocation-budget test is compiled and executed (it is not silently skipped)

### Requirement: The ratchet turns the #1447 clone→move and #1445/#1446 interning fixes into regression gates

The per-row allocation-budget test SHALL be shaped so that re-introducing a per-row value clone (reverting
#1447 `into_iter`→`iter().clone()`) OR emitting fresh per-cell key strings (reverting the #1445/#1446 key
interning) pushes the measured allocation count above the baseline, failing the test; restoring the fixes
returns it to green. The negative-control deltas (how many allocations each revert adds) SHALL be documented
in-test.

#### Scenario: Reverting the clone→move fix trips the budget
- **GIVEN** the per-row allocation-budget test at its measured baseline
- **WHEN** the row-conversion value insert is changed from a move (`into_owned`/`into_iter`) back to a
  per-row clone
- **THEN** the measured allocations-per-row exceed the baseline and the test FAILS
- **AND** reverting that change back to the move returns the test to PASS

#### Scenario: Dropping per-cell key interning trips the budget on the wide fixture
- **GIVEN** the per-row allocation-budget test at its measured baseline
- **WHEN** the key handling is changed to allocate a fresh key string per projected cell instead of reusing
  the interned `Arc<str>`
- **THEN** the measured allocations grow with the projected-column count and exceed the wide-fixture
  baseline, failing the test

### Requirement: The row-conversion map uses a non-cryptographic hasher without adding per-row allocations (L5)

The per-row `row_values` map in `build_row_from_scan_cached` SHALL use `rustc_hash::FxHashMap`
(the workspace's already-vendored `rustc-hash` dependency) instead of the default SipHash `HashMap`, so
SipHash is removed from the row hot path. This change SHALL preserve the capacity hint and SHALL NOT change
the conversion's observable output (same keys, same values, same row shape). The per-row allocation-budget
ratchet SHALL confirm the hasher swap adds no per-row allocation (alloc-neutral).

#### Scenario: FxHashMap swap is output-equivalent and alloc-neutral
- **GIVEN** a scan result converted by `build_row_from_scan_cached`
- **WHEN** the `row_values` map is backed by `FxHashMap` (capacity-hinted) rather than std `HashMap`
- **THEN** the produced row has identical keys and values to the std-`HashMap` behaviour
- **AND** the per-row allocation-budget test still passes at (or below) its baseline — the hasher swap adds
  no per-row heap allocation

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
