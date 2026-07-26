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

// MEASURED allocation budget, expressed as a FORMULA over the fixture rather than
// as hard-coded totals, so editing `RATCHET_ROWS` / `*_COLS` cannot silently
// decouple the budget from what it pins:
//
//   total = FIXED_SETUP + RATCHET_ROWS * (PER_ROW_MAP + PER_CELL * cols)
//
// Reproduces the measured totals exactly: narrow 9 + 8*(1 + 1*3) = **41**,
// wide 9 + 8*(1 + 1*32) = **273**.
//
// The three terms are SEPARATELY measured, not fitted — solved from two row counts
// (narrow at 8 rows = 41, at 4 rows = 25 => 4 allocations per row, constant 9) and
// confirmed against the wide fixture independently:
//
//   FIXED_SETUP  = 9  ONCE per measured call, NOT per row: the
//                     `Vec::with_capacity(RATCHET_ROWS)` collector, plus the FIRST
//                     row's `PartitionKeyCache` MISS, which pays the whole
//                     `decode_partition_key_columns` (decoded-column `Vec`, the
//                     name `String`, its `Arc<str>` intern, the interned `Vec`) and
//                     the PK value's first `Bytes` promotion inside the measured
//                     region. Rows 2..N hit the cache and pay none of it.
//   PER_ROW_MAP  = 1  the single sized row map (#1584).
//   PER_CELL     = 1  `Value::into_owned`'s TIER-1 compaction (#1644) — a small
//                     payload copied into a tight allocation.
//
// Keeping FIXED_SETUP OUT of the per-row term is load-bearing (issue #2904
// roborev): folding it in as a per-row constant made the budget correct only at
// RATCHET_ROWS=8 — raising the row count silently LOOSENED the ratchet, and
// lowering it failed spuriously while the message blamed toolchain drift.
//
// Asserted as `<=`, so an improvement ratchets DOWN without failing.
//
// RETUNING (read before raising any of these): the totals also pin std/
// `hashbrown`/`bytes` internals (table sizing, `Bytes::copy_from_slice` on the
// TIER-1 path), so a toolchain or `bytes` bump can shift them with no change here.
// Tell the cases apart by WHICH term moves: a dependency-driven shift moves
// FIXED_SETUP and/or PER_ROW_MAP and leaves PER_CELL at 1 (equivalently,
// `wide - narrow` stays `29 * RATCHET_ROWS`), whereas a real per-cell regression
// changes PER_CELL. Re-derive any term by temporarily zeroing it and reading the
// actual count out of the assertion message; re-derive the SPLIT by measuring at
// two different `RATCHET_ROWS` values. Never raise a number without first
// confirming the two differential controls below still pass — they, not these
// constants, are what make the interning and capacity-hint properties un-rot-able.
const PER_CELL_ALLOCS: u64 = 1;
const PER_ROW_MAP_ALLOCS: u64 = 1;
const FIXED_SETUP_ALLOCS: u64 = 9;

// The `reference_unsized_map` control below is only non-vacuous while the narrow
// fixture's insert count (`NARROW_COLS` cells + 1 reconstructed PK column) exceeds
// hashbrown's first growth threshold (3). At or below it an UNSIZED map also takes
// exactly one table allocation, the two counts converge, and the strict `<` fails
// spuriously — looking like a regression when production is correct.
const _: () = assert!(
    NARROW_COLS + 1 > 3,
    "narrow fixture must exceed hashbrown's first growth threshold or the \
     unsized-map control (#1584) becomes vacuous"
);

/// The measured allocation budget for a `cols`-wide fixture over [`RATCHET_ROWS`]
/// rows. Derived from the fixture so the two track together, with the one-time
/// setup cost kept OUT of the per-row term.
fn alloc_budget(cols: usize) -> u64 {
    FIXED_SETUP_ALLOCS + RATCHET_ROWS as u64 * (PER_ROW_MAP_ALLOCS + PER_CELL_ALLOCS * cols as u64)
}

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
///    allocation per row with no rehash growth. Dropping the capacity hint costs
///    +1 allocation per row in rehash growth — measured 41 -> 49 (narrow). This is
///    the `reference_unsized_map` differential below. It exists so this property
///    is gated by a CONTROL and not merely by the absolute budget: a constant can
///    be "re-measured" upward by a future reader, a strict `<` against a reference
///    cannot.
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

    fn reference_unsized_map(
        inputs: Vec<(RowKey, ScanRow)>,
        schema: &crate::schema::TableSchema,
    ) -> Vec<QueryRow> {
        let mut cache = PartitionKeyCache::default();
        let mut out = Vec::with_capacity(RATCHET_ROWS);
        for (key, row) in inputs {
            let cells = row.into_cells().expect("live row");
            // The reverted form: no capacity hint.
            let mut row_values: HashMap<Arc<str>, Value> = HashMap::new();
            for (name, col_value) in cells {
                row_values.insert(name, col_value.into_owned());
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

    for (label, cols) in [("narrow", NARROW_COLS), ("wide", WIDE_COLS)] {
        let budget = alloc_budget(cols);

        // Warm-up (roborev): the current impl is measured FIRST, so any one-time
        // lazy initialization reachable from the conversion path would be charged
        // to it and to neither reference — biasing every strict-`<` against the
        // thing under test, and doing so nondeterministically depending on what
        // else ran first in this test binary. Burn one discarded conversion.
        let (warm, warm_schema) = ratchet_inputs(cols);
        drop(convert_current(warm, &warm_schema));

        let (inputs, schema) = ratchet_inputs(cols);
        let (inputs_ref, _) = ratchet_inputs(cols);
        let (inputs_unsized, _) = ratchet_inputs(cols);

        let (cur_allocs, cur_rows) = measure(|| convert_current(inputs, &schema));
        let (intern_allocs, intern_rows) = measure(|| reference_pre_intern(inputs_ref, &schema));
        let (unsized_allocs, unsized_rows) =
            measure(|| reference_unsized_map(inputs_unsized, &schema));

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

        // --- output equivalence: the fixes changed cost, never bytes ---
        // Both references must yield the SAME NUMBER of rows before zipping, or a
        // short reference would silently degrade the comparison to a prefix.
        assert_eq!(
            intern_rows.len(),
            cur_rows.len(),
            "{label}: the pre-intern reference must convert the same row count"
        );
        assert_eq!(
            unsized_rows.len(),
            cur_rows.len(),
            "{label}: the unsized-map reference must convert the same row count"
        );
        for (i, (cur, old)) in cur_rows.iter().zip(intern_rows.iter()).enumerate() {
            assert_eq!(
                cur.values, old.values,
                "{label} row {i}: output must be identical to the pre-intern build"
            );
        }
        for (i, (cur, old)) in cur_rows.iter().zip(unsized_rows.iter()).enumerate() {
            assert_eq!(
                cur.values, old.values,
                "{label} row {i}: output must be identical to the unsized-map build"
            );
        }

        // --- ratchet 1 (#1334): strictly fewer allocations than losing interning ---
        assert!(
            cur_allocs < intern_allocs,
            "{label}: dropping per-cell key interning (#1334) must trip the \
             budget — current {cur_allocs} allocations is not strictly fewer \
             than the fresh-key reference's {intern_allocs}"
        );

        // --- ratchet 2 (#1584): strictly fewer than losing the capacity hint ---
        // This is what keeps the sized-map property gated even if the absolute
        // budgets below are ever retuned for a dependency-driven shift.
        assert!(
            cur_allocs < unsized_allocs,
            "{label}: dropping the row map's capacity hint (#1584) must trip the \
             budget — current {cur_allocs} allocations is not strictly fewer \
             than the unsized-map reference's {unsized_allocs}"
        );

        // --- the absolute budget (exact measured totals; see the constants' doc) ---
        assert!(
            cur_allocs <= budget,
            "{label}: {cur_allocs} allocations over {RATCHET_ROWS} rows exceeds the \
             measured budget of {budget} — see the RETUNING note on the budget \
             constants before changing this number"
        );
    }
}
