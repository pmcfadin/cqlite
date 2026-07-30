//! Issue #1849: deterministic pins for the post-merge read-visibility filter,
//! independent of on-disk fixtures. The end-to-end multi-generation proof lives
//! in `tests/issue_1849_multigen_tombstone_ttl_shadow.rs`.
use super::*;

fn shadow(now_secs: i64) -> ReadShadow {
    ReadShadow {
        now_secs,
        key_columns: HashSet::new(),
    }
}

fn shadow_with_keys(now_secs: i64, keys: &[&str]) -> ReadShadow {
    ReadShadow {
        now_secs,
        key_columns: keys.iter().map(|k| k.to_string()).collect(),
    }
}

fn live_cell(column: &str, ts: i64) -> CellData {
    CellData::new(column.to_string(), Value::Integer(1), ts)
}

fn expiring_cell(column: &str, ts: i64, ldt: i32) -> CellData {
    let mut c = CellData::new(column.to_string(), Value::Integer(1), ts);
    c.ttl = Some(60);
    c.local_deletion_time = Some(ldt);
    c
}

/// A TTL-expired data cell (past `localDeletionTime`) is dropped while a
/// live-forever sibling survives; the row itself stays (not partition-shadowed).
#[test]
fn filter_live_drops_ttl_expired_cell_keeps_live() {
    let now = 2_000_000i64;
    let cells = vec![
        live_cell("name", 100),
        expiring_cell("token", 100, 1_000), // expired: 1000 <= now
    ];
    let kept = shadow(now).filter_live(None, cells).expect("row visible");
    let names: Vec<&str> = kept.iter().map(|c| c.column.as_str()).collect();
    assert_eq!(names, vec!["name"], "expired `token` must be dropped");
}

/// A cell tombstone is never live data (dropped like the single-gen path).
#[test]
fn filter_live_drops_cell_tombstone() {
    let mut tomb = CellData::new("gone".to_string(), Value::Integer(0), 100);
    tomb.value = Value::Tombstone(Box::new(crate::types::TombstoneInfo {
        deletion_time: 100,
        tombstone_type: crate::types::TombstoneType::CellTombstone,
        local_deletion_time: 0,
        ttl: None,
        range_start: None,
        range_end: None,
    }));
    let kept = shadow(0)
        .filter_live(None, vec![live_cell("keep", 100), tomb])
        .expect("row visible");
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].column, "keep");
}

/// A row whose every data cell is shadowed by the partition tombstone (all data
/// older than `markedForDeleteAt`) is hidden entirely (`None`); a row with a cell
/// strictly newer than the deletion survives.
#[test]
fn filter_live_partition_cover_hides_fully_shadowed_row() {
    let cover = Some(2_000i64);
    // All data older/equal to the cover → whole row hidden.
    let hidden = shadow(0).filter_live(cover, vec![live_cell("a", 1_000), live_cell("b", 2_000)]);
    assert!(hidden.is_none(), "fully-shadowed row must be hidden");
    // A cell strictly newer than the cover → row survives (and keeps newer cell).
    let kept = shadow(0)
        .filter_live(cover, vec![live_cell("a", 1_000), live_cell("b", 3_000)])
        .expect("row visible");
    let names: Vec<&str> = kept.iter().map(|c| c.column.as_str()).collect();
    assert_eq!(names, vec!["b"], "older `a` shadowed, newer `b` survives");
}

/// A row kept alive by a DATA cell newer than the partition tombstone (a
/// post-delete resurrecting UPDATE) must retain its clustering-key pseudo-cell
/// even when that pseudo-cell's own write timestamp is `<= cover`. The shadow/
/// expiry drop is STRUCTURALLY skipped for key columns (matching the single-gen
/// path + the merger), so the surviving row is never emitted missing its
/// clustering-key value (issue #1849; roborev multi-gen finding).
#[test]
fn filter_live_keeps_clustering_key_under_partition_cover() {
    let cover = Some(2_000i64);
    // `ck` is a clustering pseudo-cell with ts <= cover (would be dropped if the
    // shadow test were applied to it); `data` is a newer resurrecting cell; `old`
    // is a stale data cell that MUST be shadowed away.
    let cells = vec![
        live_cell("ck", 1_000),
        live_cell("old", 1_000),
        live_cell("data", 3_000),
    ];
    let kept = shadow_with_keys(0, &["ck"])
        .filter_live(cover, cells)
        .expect("row survives via newer `data` cell");
    let names: Vec<&str> = kept.iter().map(|c| c.column.as_str()).collect();
    assert!(
        names.contains(&"ck"),
        "clustering-key pseudo-cell must be retained, got {names:?}"
    );
    assert!(
        names.contains(&"data"),
        "newer resurrecting data cell must survive, got {names:?}"
    );
    assert!(
        !names.contains(&"old"),
        "stale data cell shadowed by the partition tombstone must be dropped, got {names:?}"
    );
}

/// A clustering-key pseudo-cell does NOT by itself keep a row alive: a row whose
/// only non-key data is fully shadowed is still hidden even though the key cell is
/// retained-eligible (the key cell is excluded from the `max_data_ts` fold).
#[test]
fn filter_live_key_cell_does_not_resurrect_shadowed_row() {
    let cover = Some(2_000i64);
    let cells = vec![live_cell("ck", 1_000), live_cell("old", 1_000)];
    let hidden = shadow_with_keys(0, &["ck"]).filter_live(cover, cells);
    assert!(
        hidden.is_none(),
        "a key pseudo-cell alone must not keep a fully-shadowed row visible"
    );
}

/// AC4 post-2038: an expiring cell whose `localDeletionTime` is a post-2038
/// instant stored as a NEGATIVE `i32` bit pattern is reinterpreted UNSIGNED, so
/// it reads as a large FUTURE expiry (not wrongly wrapped negative / long-expired).
#[test]
fn cell_expiry_secs_reinterprets_post_2038_unsigned() {
    // 2039-ish epoch seconds > i32::MAX, stored as the wrapping `as i32`.
    let future: i64 = 2_200_000_000;
    let stored = future as u32 as i32; // negative bit pattern
    let c = expiring_cell("token", 100, stored);
    assert_eq!(
        cell_expiry_secs(&c),
        Some(future),
        "post-2038 LDT must reinterpret unsigned to a future expiry"
    );
    // A non-expiring (no-TTL) cell has no expiry.
    assert_eq!(cell_expiry_secs(&live_cell("x", 100)), None);
}
