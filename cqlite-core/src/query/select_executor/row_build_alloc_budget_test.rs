//! Issue #1883 (M4): the per-row allocation-count ratchet for the row hot path.
//!
//! Lives in its OWN in-crate test module for two reasons:
//!
//! 1. `crate::test_alloc_probe` is `pub(crate)` and its `CountingAllocator` is
//!    installed as `#[global_allocator]` only for `cqlite-core`'s OWN test binary
//!    (`#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]`,
//!    `lib.rs`). An integration test under `cqlite-core/tests/` would link the
//!    system allocator and could not reach the probe, so this MUST be an in-crate
//!    `#[cfg(test)]` module.
//! 2. Keeping it beside `row_build.rs` rather than inside it holds that file under
//!    the campsite-rule source threshold (#1116).

use super::{build_row_from_scan_cached, PartitionKeyCache};
use crate::query::result::QueryRow;
use crate::query::select_executor::test_support::single_pk_schema;
use crate::types::{RowKey, ScanRow, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Rows converted per measurement. All rows share one partition key — the
/// production-common shape (a scan yields a partition's rows consecutively),
/// which lets `PartitionKeyCache` (#1817) hoist the decode.
const RATCHET_ROWS: usize = 8;
/// Narrow fixture: pins the fixed per-row cost.
const NARROW_COLS: usize = 3;
/// Wide fixture: pins the PER-CELL scaling, where a lost intern shows up as
/// allocations growing with the projected-column count.
const WIDE_COLS: usize = 32;

/// MEASURED total allocations for the narrow fixture (3 cols x 8 rows), as of
/// this commit: **41** (5 per row). See the module test's doc for the
/// breakdown. Asserted as `<=` so a future improvement ratchets DOWN without
/// failing; a regression trips it.
const NARROW_ALLOC_BUDGET: u64 = 41;
/// MEASURED total allocations for the wide fixture (32 cols x 8 rows): **273**
/// (34 per row) — ~1 allocation per cell plus ~2 fixed per row.
const WIDE_ALLOC_BUDGET: u64 = 273;

/// Build `RATCHET_ROWS` scan entries of `cols` text columns, all in ONE
/// partition, with every column name pre-interned exactly as the decoder hands
/// them over (#1334). Built OUTSIDE the measured region so only the conversion
/// itself is counted.
fn ratchet_inputs(cols: usize) -> (Vec<(RowKey, ScanRow)>, crate::schema::TableSchema) {
    let names: Vec<Arc<str>> = (0..cols)
        .map(|i| Arc::from(format!("c{i}").as_str()))
        .collect();
    let key = RowKey::new(b"partition-0".to_vec());
    let inputs = (0..RATCHET_ROWS)
        .map(|r| {
            let cells: Vec<(Arc<str>, Value)> = names
                .iter()
                .enumerate()
                .map(|(c, n)| (Arc::clone(n), Value::text(format!("r{r}-c{c}-payload"))))
                .collect();
            (key.clone(), ScanRow::Row(cells))
        })
        .collect();
    (inputs, single_pk_schema("id", "text"))
}

/// Convert through the REAL public surface, with a shared `PartitionKeyCache`.
fn convert_current(
    inputs: Vec<(RowKey, ScanRow)>,
    schema: &crate::schema::TableSchema,
) -> Vec<QueryRow> {
    let mut cache = PartitionKeyCache::default();
    let mut out = Vec::with_capacity(RATCHET_ROWS);
    for (key, row) in inputs {
        out.push(
            build_row_from_scan_cached(key, row, &[], Some(schema), &mut cache)
                .expect("a live row must convert"),
        );
    }
    out
}

/// Issue #1883 (M4): the per-row allocation-count ratchet for the row hot path.
///
/// # What this gates, and what it deliberately does not
///
/// #1449 pinned per-row conversion budgets from the BINDING layer (V8
/// heap-delta / Python `tracemalloc`). Those probes cannot observe a TRANSIENT
/// Rust-heap allocation that is freed before the row crosses the FFI boundary.
/// This test closes that gap by counting allocations IN-PROCESS, on the real
/// public conversion surface.
///
/// It gates the two allocation properties that `build_row_from_scan_cached`
/// actually owns:
///
/// 1. **Per-cell column-name interning (#1334).** The decoder hands over
///    interned `Arc<str>` handles and the conversion MOVES them in. Emitting a
///    fresh key string per cell instead costs +2 allocations per cell —
///    measured 41 -> 89 (narrow) and 273 -> 785 (wide, exactly +2*32*8 = +512).
///    This is the `reference_pre_intern` differential below.
/// 2. **The single sized value map (#1584).** `HashMap::with_capacity` gives one
///    allocation per row with no rehash growth. The absolute budget below pins it,
///    so a lost capacity hint (which would add rehash-growth reallocations as the
///    map fills) trips the wide fixture.
///
/// **Not gated here: the #1447 clone->move fix.** #1447 lives in
/// `bindings/node/src/database.rs` (`ExecuteNativeTask::compute`), NOT in this
/// crate, so no `cqlite-core` test can gate it. Measured directly: reverting
/// the move to a per-cell `col_value.clone().into_owned()` here is EXACTLY
/// allocation-neutral (41 vs 41 narrow, 273 vs 273 wide), because
/// `Value::Text` is `Bytes`-backed (clone is a refcount bump) and
/// `Value::into_owned`'s TIER-1 compaction (#1644) copies a small payload
/// either way. A "clone" control here would therefore be VACUOUS, so it is
/// deliberately omitted rather than asserted with a weakened `<=`. The same
/// applies to #1445/#1446, which are Python/Node binding fixes. Ratcheting
/// those three needs an allocation probe inside the binding crates — tracked
/// as issue #2894.
///
/// # Shape
///
/// Mirrors `lookup.rs`'s `cartesian_product_builds_each_combo_in_one_allocation`:
/// a `reference()` reproduces the pre-fix behaviour, and the current impl must
/// (a) produce identical output and (b) allocate STRICTLY fewer times. A
/// differential cannot pass vacuously — if the fixture stopped exercising the
/// path, both sides would collapse together and the strict inequality would
/// fail. The absolute budgets are exact measured counts (the counting
/// allocator is deterministic for a fixed input, so no tolerance is needed).
///
/// `not(dhat-heap)`: issue #1668's dhat allocator (`lib.rs`'s `DHAT_TEST_ALLOC`)
/// mutually excludes `test_alloc_probe`'s own `CountingAllocator` (only one
/// `#[global_allocator]` per binary), so under a feature combination with BOTH
/// `state_machine` and `dhat-heap` enabled (e.g. `--all-features`)
/// `test_alloc_probe` is configured out entirely — skip this specific
/// allocation-count probe rather than fail to resolve it.
#[test]
fn build_row_from_scan_cached_holds_the_per_row_alloc_budget() {
    use crate::test_alloc_probe::measure;

    /// PRE-#1334: allocate a fresh key string per projected cell instead of
    /// moving the decoder's interned `Arc<str>` handle.
    fn reference_pre_intern(
        inputs: Vec<(RowKey, ScanRow)>,
        schema: &crate::schema::TableSchema,
    ) -> Vec<QueryRow> {
        let mut cache = PartitionKeyCache::default();
        let mut out = Vec::with_capacity(RATCHET_ROWS);
        for (key, row) in inputs {
            let cells = row.into_cells().expect("live row");
            let mut row_values: HashMap<Arc<str>, Value> =
                HashMap::with_capacity(cells.len() + schema.partition_keys.len());
            for (name, col_value) in cells {
                // The reverted form: a fresh per-cell `Arc<str>` key.
                let fresh: Arc<str> = Arc::from(name.as_ref().to_string().as_str());
                row_values.insert(fresh, col_value.into_owned());
            }
            for (name, value) in cache.columns_for(&key.0, schema) {
                row_values.insert(Arc::clone(name), value.clone());
            }
            out.push(QueryRow {
                values: row_values,
                key,
                metadata: Default::default(),
                cell_metadata: None,
            });
        }
        out
    }

    for (label, cols, budget) in [
        ("narrow", NARROW_COLS, NARROW_ALLOC_BUDGET),
        ("wide", WIDE_COLS, WIDE_ALLOC_BUDGET),
    ] {
        let (inputs, schema) = ratchet_inputs(cols);
        let (inputs_ref, _) = ratchet_inputs(cols);

        let (cur_allocs, cur_rows) = measure(|| convert_current(inputs, &schema));
        let (intern_allocs, intern_rows) = measure(|| reference_pre_intern(inputs_ref, &schema));

        // --- non-vacuity: the fixture really converted real rows ---
        assert_eq!(
            cur_rows.len(),
            RATCHET_ROWS,
            "{label}: every input row must convert — a zero/short result would \
             make the allocation budget meaningless"
        );
        for row in &cur_rows {
            assert_eq!(
                row.values.len(),
                cols + 1,
                "{label}: each row must carry all {cols} cells plus the \
                 reconstructed partition-key column"
            );
        }

        // --- output equivalence: interning changed cost, never bytes ---
        for (i, (cur, old)) in cur_rows.iter().zip(intern_rows.iter()).enumerate() {
            assert_eq!(
                cur.values, old.values,
                "{label} row {i}: output must be identical to the pre-intern build"
            );
        }

        // --- the ratchet: strictly fewer allocations than the revert ---
        assert!(
            cur_allocs < intern_allocs,
            "{label}: dropping per-cell key interning (#1334) must trip the \
             budget — current {cur_allocs} allocations is not strictly fewer \
             than the fresh-key reference's {intern_allocs}"
        );

        // --- the absolute budget (exact measured counts) ---
        assert!(
            cur_allocs <= budget,
            "{label}: {cur_allocs} allocations over {RATCHET_ROWS} rows exceeds \
             the measured budget of {budget} \
             ({} per row vs {} budgeted)",
            cur_allocs as usize / RATCHET_ROWS,
            budget as usize / RATCHET_ROWS
        );
    }
}
