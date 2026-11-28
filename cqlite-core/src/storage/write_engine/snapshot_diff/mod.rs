//! `cqlite diff` — a read-only two-snapshot comparison of one table's reconciled state, with
//! resurrection-risk classification (issue #4201; OpenSpec change `snapshot-diff`).
//!
//! # Mechanism (design.md §D1)
//!
//! Each side — a flat table directory of generations — is reconciled **independently** by the
//! unmodified #4193 mechanism `cqlite explain` drives (`merge::build_traced_merger_with_registry`
//! in full-compaction configuration at one pinned `now`, with a recording sink). This module adds
//! only the comparison layer: the two sides' winner cells are compared per
//! `(clustering, column)`, and every tombstone held by exactly ONE side is cross-checked against
//! the other side's winners. A tombstone both sides hold is omitted: the report shows
//! disagreement, not an inventory.
//!
//! A one-sided tombstone `T` whose scope still holds live data on the other side with
//! `writetime <= T.deletion_time` is a **resurrection risk**: once `T` passes the strict purge
//! boundary `local_deletion_time < now - gc_grace_seconds` its holder may purge it on its next
//! compaction, and the other side's shadowed data returns the next time the two are merged. The
//! rules and their `cassandra-5.0.8` citations live in [`resurrection`].
//!
//! # Read-only
//!
//! Nothing here opens a file for writing, creates a file, or modifies either input. There is no
//! repair path and none may be added: the report is input to `sstableloader` / repair, never a
//! write.
//!
//! # Memory
//!
//! Partitions are folded one at a time: both sides' recording sinks (bounded by
//! `max_result_bytes`) are dropped before the next partition. The key list for the un-scoped scan
//! is `O(partitions)` raw keys; the report itself is bounded by `max_result_bytes` and exceeding
//! it is [`crate::Error::ResultTooLarge`], never a truncated report.
//!
//! # Declared gaps
//!
//! * A row's liveness marker is not a cell in the trail (see [`resurrection`]).
//! * Two sides holding different content at one identical write timestamp are a `conflict` cell
//!   (owner Q21=b), resolved by `Cells.resolveRegular`; see `cells.rs`.

mod cells;
mod fold;
pub mod resurrection;
mod side;
mod types;

use std::collections::BTreeMap;

pub use types::{
    CellDiffEntry, CellSide, CellStatus, DiffError, DiffOptions, DiffReport, DiffSide, DiffSummary,
    PartitionDiff, ResurrectionRisk, ShadowedCell, Side, SideInfo, TombstoneDiffEntry,
};

use crate::schema::{TableSchema, UdtRegistry};
use crate::storage::partition_key_codec::decode_partition_key_columns;
use crate::storage::write_engine::merge::{compute_gc_before, effective_compaction_schema};
use crate::util::value_fmt::ValueFormatter;
use crate::Error;

/// The side's header: its directory and generation names, oldest first (the input is newest first).
fn side_info(side: &DiffSide) -> SideInfo {
    SideInfo {
        dir: side.dir.display().to_string(),
        generations: if !side.listed_names.is_empty() {
            side.listed_names.clone()
        } else {
            side.generations
                .iter()
                .rev()
                .map(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name.strip_suffix("-Data.db").unwrap_or(name).to_string())
                        .unwrap_or_else(|| path.display().to_string())
                })
                .collect()
        },
    }
}

fn render_partition_key(raw: &[u8], schema: &TableSchema) -> crate::Result<String> {
    Ok(decode_partition_key_columns(raw, schema)?
        .iter()
        .map(|(_, value)| ValueFormatter::format_value(value))
        .collect::<Vec<_>>()
        .join(", "))
}

fn check_sides(side_a: &DiffSide, side_b: &DiffSide) -> crate::Result<()> {
    for (label, side) in [(Side::A, side_a), (Side::B, side_b)] {
        if side.generations.is_empty() {
            return Err(Error::InvalidInput(format!(
                "side={label}: no live generations in {}",
                side.dir.display()
            )));
        }
    }
    Ok(())
}

/// Compare every partition held by either side (spec R1-R5, R8). Partitions are visited in token
/// order; a key present on only one side is compared against an empty trail on the other.
///
/// # Errors
///
/// [`DiffError::SideRead`] when a generation of either side cannot be read (no report is produced
/// from a shorter trail), or [`DiffError::Core`] for a schema
/// problem or when the report would exceed `options.max_result_bytes`.
pub async fn diff_table(
    side_a: &DiffSide,
    side_b: &DiffSide,
    table: &str,
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    options: &DiffOptions,
) -> Result<DiffReport, DiffError> {
    check_sides(side_a, side_b)?;
    side::preflight_generations(Side::A, side_a).await?;
    side::preflight_generations(Side::B, side_b).await?;
    let schema_a = effective_compaction_schema(schema, &side_a.generations);
    let schema_b = effective_compaction_schema(schema, &side_b.generations);
    let mut keys: BTreeMap<(i64, Vec<u8>), ()> = BTreeMap::new();
    side::enumerate_keys(Side::A, side_a, &schema_a, &mut keys).await?;
    side::enumerate_keys(Side::B, side_b, &schema_b, &mut keys).await?;
    let keys: Vec<Vec<u8>> = keys.into_keys().map(|(_, key)| key).collect();
    run(side_a, side_b, table, schema, udt_registry, options, &keys).await
}

/// Compare only the requested partitions through the point-read path (spec R11): no enumeration,
/// every requested key yields an entry (an empty one when neither side holds it).
///
/// `keys` are raw partition-key bytes (`encode_partition_key_columns`).
///
/// # Errors
///
/// As [`diff_table`].
pub async fn diff_partitions(
    side_a: &DiffSide,
    side_b: &DiffSide,
    table: &str,
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    options: &DiffOptions,
    keys: &[Vec<u8>],
) -> Result<DiffReport, DiffError> {
    check_sides(side_a, side_b)?;
    side::preflight_generations(Side::A, side_a).await?;
    side::preflight_generations(Side::B, side_b).await?;
    let mut ordered: BTreeMap<(i64, Vec<u8>), ()> = BTreeMap::new();
    for key in keys {
        let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(key);
        ordered.insert((token, key.clone()), ());
    }
    let ordered: Vec<Vec<u8>> = ordered.into_keys().map(|(_, key)| key).collect();
    run(
        side_a,
        side_b,
        table,
        schema,
        udt_registry,
        options,
        &ordered,
    )
    .await
}

/// Compare one partition (spec R11).
///
/// # Errors
///
/// As [`diff_table`].
pub async fn diff_partition(
    side_a: &DiffSide,
    side_b: &DiffSide,
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    key: &[u8],
    options: &DiffOptions,
) -> Result<PartitionDiff, DiffError> {
    let mut report = diff_partitions(
        side_a,
        side_b,
        "",
        schema,
        udt_registry,
        options,
        &[key.to_vec()],
    )
    .await?;
    report
        .partitions
        .pop()
        .ok_or_else(|| DiffError::Core(Error::Internal("diff produced no partition entry".into())))
}

/// The most any report may use, whatever `max_result_bytes` asks for: with each trail capped at a
/// quarter of the remaining budget, the worst-case peak (report + two trails + fold copies) stays
/// well inside the project's 128 MiB target.
pub const MAX_EFFECTIVE_BUDGET: u64 = 32 * 1024 * 1024;

/// What one side's trail may record for the next partition (see `run`).
fn trail_budget(budget: u64, estimated: u64) -> u64 {
    (budget.saturating_sub(estimated) / 4).max(TRAIL_FLOOR)
}

/// A trail's serialized events are far more verbose than the report entries they fold into, so
/// even a tiny report budget leaves a trail this much room (one partition's decisions).
const TRAIL_FLOOR: u64 = 1024 * 1024;

/// The table's declared `gc_grace_seconds` (Cassandra's default 864000 when absent); `None` when
/// unparseable or negative. Mirrors `compute_gc_before`.
fn declared_gc_grace(schema: &TableSchema) -> Option<i64> {
    match schema.comments.get("gc_grace_seconds") {
        None => Some(864_000),
        Some(text) => text.trim().parse::<i64>().ok().filter(|grace| *grace >= 0),
    }
}

/// Serialized JSON length without materializing the bytes (a counting writer, no copy).
fn encoded_len_of<T: serde::Serialize>(value: &T) -> u64 {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)
        .map(|()| counter.0)
        .unwrap_or(u64::MAX)
}

async fn run(
    side_a: &DiffSide,
    side_b: &DiffSide,
    table: &str,
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    options: &DiffOptions,
    keys: &[Vec<u8>],
) -> Result<DiffReport, DiffError> {
    // One source for the boundary: `compute_gc_before` is what the merger purges with, and the
    // report's `gc_grace_seconds` is derived from it so the two cannot drift. `None` means the
    // table's declared gc_grace_seconds is unparseable or negative.
    // `now` is a GC-clock second: Cassandra bounds a local deletion time to `[0, MAX_DELETION_TIME]`
    // (`db/DeletionTime.java:57`, unsigned 32-bit), and `now - gc_grace_seconds` must not overflow.
    // Checked here so no `--now` can panic a debug build or wrap a release one.
    let now = options.now_secs;
    let gc_grace = declared_gc_grace(schema).ok_or_else(|| {
        Error::Schema(format!(
            "table '{table}' declares an invalid gc_grace_seconds; refusing to classify purge phases"
        ))
    })?;
    if !(0..=i64::from(u32::MAX)).contains(&now) || now.checked_sub(gc_grace).is_none() {
        return Err(DiffError::InvalidNow {
            value: now,
            reason: format!(
                "outside the GC-clock range 0..={} for gc_grace_seconds={gc_grace}",
                u32::MAX
            ),
        });
    }
    let gc_before = compute_gc_before(schema, now).ok_or_else(|| {
        Error::Schema(format!(
            "table '{table}' declares an invalid gc_grace_seconds; refusing to classify purge phases"
        ))
    })?;
    let budget = options.max_result_bytes.min(MAX_EFFECTIVE_BUDGET);
    let schema_a = effective_compaction_schema(schema, &side_a.generations);
    let schema_b = effective_compaction_schema(schema, &side_b.generations);

    let mut report = DiffReport {
        now: fold::fmt_secs(options.now_secs),
        table: table.to_string(),
        gc_grace_seconds: options.now_secs - gc_before,
        side_a: side_info(side_a),
        side_b: side_info(side_b),
        partitions: Vec::new(),
        summary: DiffSummary::default(),
    };
    // The budget covers the WHOLE serialized report (metadata, generation lists, summary, JSON
    // punctuation), so the API never returns more than `max_result_bytes`. Start from the empty
    // report's size; each partition adds its own size plus one separator.
    let encoded_len = |report: &DiffReport| encoded_len_of(report);
    let too_large = |estimated: u64, rows: usize| {
        DiffError::Core(Error::ResultTooLarge {
            budget_bytes: usize::try_from(budget).unwrap_or(usize::MAX),
            estimated_bytes: usize::try_from(estimated).unwrap_or(usize::MAX),
            rows,
        })
    };
    let mut estimated: u64 = encoded_len(&report);
    for key in keys {
        // Peak memory: the report so far, both trails and the fold's working copies are alive at
        // once. Each trail may use at most a quarter of what the report has left, so the two
        // trails together take half and the fold structures the rest.
        let trail_budget = trail_budget(budget, estimated);
        let trail_a =
            side::trace_partition(side_a, key, &schema_a, udt_registry, options, trail_budget);
        let trail_a = match trail_a {
            Ok(trail) => trail,
            Err(error) => return Err(side::locate_failure(Side::A, side_a, error).await),
        };
        let trail_b =
            side::trace_partition(side_b, key, &schema_b, udt_registry, options, trail_budget);
        let trail_b = match trail_b {
            Ok(trail) => trail,
            Err(error) => return Err(side::locate_failure(Side::B, side_b, error).await),
        };
        let partition = fold::fold_partition(
            render_partition_key(key, schema)?,
            &trail_a,
            &trail_b,
            gc_before,
            schema,
        )?;
        // Both trails are dropped here, before the next partition (R8).
        // JSON array arithmetic: `[]` already counted; the first element adds only its own bytes,
        // each later one adds a `,` too, so the running estimate equals the final length.
        let separator = u64::from(!report.partitions.is_empty());
        estimated = estimated
            .saturating_add(encoded_len_of(&partition))
            .saturating_add(separator);
        if estimated > budget {
            return Err(too_large(estimated, report.partitions.len() + 1));
        }
        report.summary.add(&partition);
        report.partitions.push(partition);
    }
    // The exact serialized size (the summary's digits grew while folding).
    let exact = encoded_len(&report);
    if exact > budget {
        return Err(too_large(exact, report.partitions.len()));
    }
    Ok(report)
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn trails_never_take_more_than_half_of_what_is_left() {
        for (budget, used) in [(100, 0), (100, 60), (100, 100), (100, 500), (u64::MAX, 0)] {
            let each = trail_budget(budget, used);
            let left = budget.saturating_sub(used);
            assert!(each <= (left / 4).max(TRAIL_FLOOR));
            assert!(each.saturating_mul(2) <= left / 2 || each == TRAIL_FLOOR);
        }
        assert_eq!(trail_budget(100, 100), TRAIL_FLOOR);
        assert_eq!(
            trail_budget(MAX_EFFECTIVE_BUDGET, 0),
            MAX_EFFECTIVE_BUDGET / 4
        );
    }

    #[test]
    fn the_effective_budget_is_capped_below_the_heap_target() {
        assert!(MAX_EFFECTIVE_BUDGET * 4 <= 128 * 1024 * 1024);
    }

    #[test]
    fn gc_grace_parsing_mirrors_the_merge() {
        let mut schema = TableSchema::new_for_testing("ks", "t");
        assert_eq!(declared_gc_grace(&schema), Some(864_000));
        schema
            .comments
            .insert("gc_grace_seconds".into(), "0".into());
        assert_eq!(declared_gc_grace(&schema), Some(0));
        schema
            .comments
            .insert("gc_grace_seconds".into(), "-1".into());
        assert_eq!(declared_gc_grace(&schema), None);
    }
}
