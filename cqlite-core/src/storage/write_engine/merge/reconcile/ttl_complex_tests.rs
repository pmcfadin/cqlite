//! Issue #1537: TTL expiry during compaction MUST apply to complex/collection/UDT
//! ELEMENTS, not just simple cells.
//!
//! Follow-up from #1382 (roborev F2). #1382's [`ReconcileState::expire_ttl_cells`]
//! skipped every `cell.is_complex_element`, so an expiring collection/UDT element
//! whose authoritative on-disk `localDeletionTime` was already past the pinned
//! evaluation instant survived a compaction as a LIVE expiring element instead of
//! being turned into an element tombstone (and then purged past grace).
//!
//! ## Parity oracle
//!
//! Apache Cassandra `AbstractCell.purge(DeletionPurger, long nowInSec)` treats an
//! EXPIRED expiring cell UNIFORMLY, simple cell or complex-column element:
//!
//! ```java
//! if (!isLive(nowInSec)) {
//!     if (purger.shouldPurge(timestamp(), localDeletionTime())) return null;   // purged
//!     if (isExpiring()) {
//!         // converts an expired expiring cell to a tombstone PRESERVING path()
//!         Cell<?> newCell = BufferCell.tombstone(column, timestamp(), localDeletionTime(), path());
//!         return purger.shouldPurge(timestamp(), localDeletionTime()) ? null : newCell;
//!     }
//! }
//! return this;
//! ```
//!
//! The crux for #1537 is `path()`: the converted tombstone KEEPS the element's
//! cell path. So an expired collection element becomes an element-level tombstone
//! at the SAME path, with `markedForDeleteAt` (the cell's own write timestamp) and
//! `localDeletionTime` (the expiry instant) unchanged, and no live value. Once
//! `localDeletionTime < gcBefore` (and the overlap gate allows) it is purged.
//!
//! ## What these tests exercise
//!
//! They drive the REAL production reconcile pipeline
//! ([`KWayMerger::reconcile_cluster_with_overlap_counted`], the same entry the
//! compactor's `merge_partition_rows` calls) with a PINNED `now_secs`/`gc_before`
//! and a `MergeEntry` carrying an expiring complex-element [`CellData`] (the shape
//! the reader surfaces for a non-frozen collection element on the compaction read
//! path — epic #899). No wall clock is sampled: every threshold is pinned relative
//! to the element's own on-disk `localDeletionTime`.
//!
//! The end-to-end write→flush→compact→read path is NOT exercised here: CQLite's
//! WriteEngine→KWayMerger round-trip of a freshly WRITTEN non-frozen collection is
//! independently broken (a whole-column map/set flush does not read back as
//! per-element complex cells through the merge reader) — a WRITE-PATH gap outside
//! #1537's reconcile scope. The reconcile-level parity here is the faithful,
//! deterministic proof of the fix.

#![cfg(feature = "write-support")]

use std::collections::HashMap;

use super::super::{CellData, KWayMerger, MergeEntry, PurgeCounts, RowData};
use crate::storage::write_engine::mutation::DecoratedKey;
use crate::types::{TombstoneType, Value};

/// A decorated key from a single token byte.
fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

/// An expiring ELEMENT of a non-frozen complex column, as the compaction reader
/// surfaces it (epic #899): `is_complex_element == true`, an authoritative
/// `cell_path`, live value, per-element `ttl` + `local_deletion_time`, and
/// `is_deleted == false` (a live expiring element is NOT a tombstone).
fn expiring_complex_element(
    column: &str,
    cell_path: &[u8],
    value: Value,
    has_empty_value: bool,
    ts: i64,
    ttl: u32,
    ldt: i32,
) -> CellData {
    CellData {
        column: column.to_string(),
        value,
        timestamp: ts,
        ttl: Some(ttl),
        cell_path: Some(cell_path.to_vec()),
        local_deletion_time: Some(ldt),
        is_complex_element: true,
        is_deleted: false,
        has_empty_value,
    }
}

/// A plain live simple survivor cell so the row is never a phantom key-only drop.
fn survivor(column: &str, v: i32, ts: i64) -> CellData {
    CellData::new(column.to_string(), Value::Integer(v), ts)
}

/// Drive the production reconcile with pinned `now_secs`/`gc_before` (full
/// compaction: `max_purgeable_timestamp == i64::MAX`), returning the surviving
/// cells (or empty if the row is dropped whole).
fn reconcile(
    cells: Vec<CellData>,
    row_ts: i64,
    gc_before: Option<i64>,
    now: Option<i64>,
) -> Vec<CellData> {
    let row = MergeEntry::new(0, dk(1), None, row_ts, RowData::Live { cells });
    let mut purges = PurgeCounts::default();
    let merged = KWayMerger::reconcile_cluster_with_overlap_counted(
        None,
        vec![row],
        &HashMap::new(),
        gc_before,
        i64::MAX,
        now,
        &mut purges,
    );
    match merged {
        Some(entry) => match entry.row_data {
            RowData::Live { cells } => cells,
            RowData::Tombstone { .. } => Vec::new(),
        },
        None => Vec::new(),
    }
}

fn complex_element<'a>(cells: &'a [CellData], column: &str) -> Option<&'a CellData> {
    cells
        .iter()
        .find(|c| c.column == column && c.cell_path.is_some())
}

fn has_survivor(cells: &[CellData], column: &str) -> bool {
    cells
        .iter()
        .any(|c| c.column == column && matches!(c.value, Value::Integer(_)))
}

// ===========================================================================
// Criterion 1 — MAP element expired within grace → element tombstone at path
// ===========================================================================

#[test]
fn map_element_expired_within_grace_becomes_element_tombstone() {
    let ldt: i32 = 1_600_000_000;
    let cells = vec![
        expiring_complex_element(
            "props",
            b"k1",
            Value::Text("secret".to_string()),
            false,
            100,
            60,
            ldt,
        ),
        survivor("score", 7, 100),
    ];
    // Expired: now AFTER ldt. Within grace: gcBefore == ldt so `ldt < gcBefore`
    // is false → the element tombstone is RETAINED.
    let out = reconcile(cells, 100, Some(i64::from(ldt)), Some(i64::from(ldt) + 10));

    let elem = complex_element(&out, "props").unwrap_or_else(|| {
        panic!("expired-within-grace element must survive as a tombstone, got {out:?}")
    });
    // Cassandra `BufferCell.tombstone(..., path())`: element tombstone at the SAME
    // path, no live value, TTL cleared, ldt/markedForDeleteAt preserved.
    assert!(
        elem.is_deleted,
        "must carry the IS_DELETED element-tombstone flag"
    );
    assert_eq!(
        elem.cell_path.as_deref(),
        Some(b"k1".as_slice()),
        "cell path (Cassandra `path()`) must be preserved"
    );
    assert!(elem.ttl.is_none(), "a tombstone carries no TTL");
    assert_eq!(elem.timestamp, 100, "markedForDeleteAt == element write ts");
    assert_eq!(
        elem.local_deletion_time,
        Some(ldt),
        "localDeletionTime == expiry instant, preserved"
    );
    // The live value must NOT survive: it is wrapped as a CellTombstone.
    assert_ne!(
        elem.value,
        Value::Text("secret".to_string()),
        "the expired element's live value must not survive"
    );
    assert!(
        matches!(&elem.value, Value::Tombstone(info)
            if info.tombstone_type == TombstoneType::CellTombstone),
        "expired element surfaces a CellTombstone value, got {:?}",
        elem.value
    );
    assert!(
        has_survivor(&out, "score"),
        "the independent survivor cell stays live"
    );
}

// ===========================================================================
// Criterion 2 — MAP element expired past grace → purged entirely
// ===========================================================================

#[test]
fn map_element_expired_past_grace_is_purged() {
    let ldt: i32 = 1_600_000_000;
    let cells = vec![
        expiring_complex_element(
            "props",
            b"k1",
            Value::Text("secret".to_string()),
            false,
            100,
            60,
            ldt,
        ),
        survivor("score", 7, 100),
    ];
    // Expired AND past grace: gcBefore STRICTLY > ldt → purged (full compaction →
    // overlap gate +inf allows it).
    let out = reconcile(
        cells,
        100,
        Some(i64::from(ldt) + 1),
        Some(i64::from(ldt) + 1000),
    );

    assert!(
        complex_element(&out, "props").is_none(),
        "expired-past-grace element must be purged entirely, got {out:?}"
    );
    assert!(
        has_survivor(&out, "score"),
        "purging only the expired element must leave the survivor cell present"
    );
}

// ===========================================================================
// Criterion 3 — live (un-expired) MAP element survives unchanged
// ===========================================================================

#[test]
fn live_map_element_survives_unchanged() {
    let ldt: i32 = 2_000_000_000; // far future
    let cells = vec![
        expiring_complex_element(
            "props",
            b"k1",
            Value::Text("alive".to_string()),
            false,
            100,
            10_000_000,
            ldt,
        ),
        survivor("score", 7, 100),
    ];
    // now BEFORE the expiry instant → not expired.
    let out = reconcile(
        cells,
        100,
        Some(i64::from(ldt) - 100),
        Some(i64::from(ldt) - 1),
    );

    let elem = complex_element(&out, "props")
        .unwrap_or_else(|| panic!("un-expired element must survive, got {out:?}"));
    assert!(!elem.is_deleted, "an un-expired element is not a tombstone");
    assert_eq!(
        elem.value,
        Value::Text("alive".to_string()),
        "live element keeps its value"
    );
    assert!(elem.ttl.is_some(), "live expiring element keeps its TTL");
    assert_eq!(elem.local_deletion_time, Some(ldt), "LDT unchanged");
}

// ===========================================================================
// Criterion 4 — SET member (empty value) expired within grace → element tombstone
// ===========================================================================

#[test]
fn set_member_expired_within_grace_becomes_element_tombstone() {
    let ldt: i32 = 1_600_000_000;
    let cells = vec![
        // A SET member: identity lives in the cell path, empty value.
        expiring_complex_element("tags", b"member1", Value::Null, true, 100, 60, ldt),
        survivor("score", 7, 100),
    ];
    let out = reconcile(cells, 100, Some(i64::from(ldt)), Some(i64::from(ldt) + 10));

    let elem = complex_element(&out, "tags")
        .unwrap_or_else(|| panic!("expired SET member must survive as a tombstone, got {out:?}"));
    assert!(elem.is_deleted, "SET member tombstone carries IS_DELETED");
    assert_eq!(
        elem.cell_path.as_deref(),
        Some(b"member1".as_slice()),
        "SET member cell path preserved"
    );
    assert!(elem.ttl.is_none(), "a tombstone carries no TTL");
    assert!(has_survivor(&out, "score"), "survivor stays live");
}

// ===========================================================================
// Criterion 5 — expiry DISABLED (now_secs == None) is a strict no-op for a
// complex element too (regression guard: the #1382 no-op invariant extends to
// #1537's complex path).
// ===========================================================================

#[test]
fn expiry_disabled_leaves_complex_element_live() {
    let ldt: i32 = 1_600_000_000;
    let cells = vec![expiring_complex_element(
        "props",
        b"k1",
        Value::Text("secret".to_string()),
        false,
        100,
        60,
        ldt,
    )];
    // now_secs = None → expiry is a strict no-op even though the element is
    // long-expired relative to any real clock.
    let out = reconcile(cells, 100, None, None);
    let elem = complex_element(&out, "props")
        .unwrap_or_else(|| panic!("no-op expiry must leave the element live, got {out:?}"));
    assert!(
        !elem.is_deleted,
        "no-op expiry must not tombstone the element"
    );
    assert_eq!(
        elem.value,
        Value::Text("secret".to_string()),
        "value untouched"
    );
    assert_eq!(elem.ttl, Some(60), "ttl untouched");
}
