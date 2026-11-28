//! The one-shot compaction entry points and the dropped-column survivor pre-pass (split out of
//! `merge/mod.rs`, #4658).

use super::*;

/// Report returned by [`compact_sstables`].
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct CompactReport {
    /// Metadata for the SSTable written by the compaction.
    pub output: crate::storage::sstable::writer::SSTableInfo,
    /// Statistics about the merge that produced it.
    pub stats: MergeStats,
}

#[cfg(feature = "write-support")]
/// Determine which dropped columns still have surviving cells after the merge.
///
/// Runs a merge pre-pass with the decode `schema` (so the dropped-column filter
/// applies identically to the write pass) and collects the names of
/// `dropped_columns` that appear in at least one surviving `Live` cell — i.e.
/// columns re-added with writes after their drop time. `compact_sstables` keeps
/// exactly these dropped columns in the output writer schema; the rest are
/// stripped from the output header. Stops early once every dropped column has
/// been observed surviving.
///
/// A dropped column also counts as surviving when its only surviving state is a
/// retained `ComplexDeletion` marker. A complex (collection / UDT) tombstone for
/// a dropped COMPLEX column lives in `row.complex_deletions`, NOT in `cells`, so
/// counting only live `cells` would strip such a column from the output schema —
/// and the writer only emits complex-element columns present in the schema,
/// silently dropping a complex tombstone the merge decided to RETAIN. Because the
/// merge pre-pass runs reconcile with the SAME `gc_before`/`purge_safe` as the
/// write pass, a marker purged by gc never reaches the yielded `rows`, so a
/// retained marker IS counted while a purged one is NOT (#921 finding 2 / #847).
///
/// `purge_safe` (#921 finding 1) and `max_purgeable_timestamp` (#935) MUST match
/// the values the write pass uses (`compact_sstables`' / `start_merge`'s). The
/// pre-pass builds its merger with the SAME gc cutoff (`gc_before_secs`/`now_secs`),
/// the SAME `purge_safe` flag, AND the SAME overlap bound so its purge decisions
/// are byte-identical to the write pass. Without this, a purgeable tombstone in a
/// dropped column would count as a survivor here (no purging) yet be purged in the
/// real merge (purging on) — leaving an empty dropped column in the output header
/// that this pre-pass exists to strip.
pub(crate) fn compute_surviving_dropped_columns(
    input_paths: Vec<PathBuf>,
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
    max_purgeable_timestamp: Option<i64>,
) -> Result<std::collections::HashSet<String>> {
    let mut surviving: std::collections::HashSet<String> = std::collections::HashSet::new();
    let total = schema.dropped_columns.len();
    let mut merger = KWayMerger::new_with_gc(input_paths, schema, gc_before_secs, now_secs)?
        .with_purge_safe(purge_safe)
        .with_max_purgeable_timestamp(max_purgeable_timestamp);
    loop {
        match merger.step()? {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in &rows {
                    if let RowData::Live { cells } = &row.row_data {
                        for cell in cells {
                            if schema.dropped_columns.contains_key(&cell.column) {
                                surviving.insert(cell.column.clone());
                            }
                        }
                    }
                    // A surviving (within-grace / purge-unsafe) complex-deletion
                    // marker for a dropped COMPLEX column is a survivor too: the
                    // marker lives here, not in `cells`, and the writer must keep
                    // the column to emit it.
                    for cd in &row.complex_deletions {
                        if schema.dropped_columns.contains_key(&cd.column) {
                            surviving.insert(cd.column.clone());
                        }
                    }
                }
                if surviving.len() == total {
                    break; // every dropped column already shown to survive
                }
            }
        }
    }
    Ok(surviving)
}

/// One-shot compaction entry point (behind the `cqlite compact` CLI command).
///
/// `purge_safe` (#921 finding 1) gates gc_grace tombstone purging on overlap
/// safety: pass `true` ONLY when `input_paths` spans EVERY SSTable for the table
/// (so no non-included overlapping SSTable can hold data shadowed by a purged
/// tombstone). When `false`, tombstones are retained even if older than
/// `gcBefore`, so a partial compaction cannot resurrect deleted data.
pub async fn compact_sstables(
    input_paths: Vec<PathBuf>,
    output_dir: &std::path::Path,
    schema: &TableSchema,
    generation: u64,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
) -> Result<CompactReport> {
    compact_sstables_with_registry(
        input_paths,
        output_dir,
        schema,
        generation,
        gc_before_secs,
        now_secs,
        purge_safe,
        None,
    )
    .await
}

/// Like [`compact_sstables`], but accepts an optional [`UdtRegistry`] so a UDT
/// column ADDED after the inputs were written (absent from every input header)
/// is normalized to its `UserType(...)` marshal in the output instead of being
/// emitted as a bare `BytesType` column (roborev #1027). Pass `None` to keep the
/// header-only behavior of [`compact_sstables`].
#[allow(clippy::too_many_arguments)]
pub async fn compact_sstables_with_registry(
    input_paths: Vec<PathBuf>,
    output_dir: &std::path::Path,
    schema: &TableSchema,
    generation: u64,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
    udt_registry: Option<&crate::schema::UdtRegistry>,
) -> Result<CompactReport> {
    if input_paths.is_empty() {
        return Err(Error::InvalidInput(
            "compaction requires at least one input SSTable".to_string(),
        ));
    }

    // Fully-expired SSTable drop (issue #1388, OQ-1 → (A)): the CLI one-shot has no
    // knowledge of SSTables outside its explicit input list, so the drop is only
    // overlap-safe when the operator asserts `--major` (`purge_safe == true`),
    // which means the input set spans EVERY overlapping SSTable for the table ⇒
    // empty outside set ⇒ +inf overlap bound ⇒ every fully-expired input is
    // provably safe to drop. Without `--major` no drop occurs (conservative,
    // matching the tombstone-purge conservatism). The drop-set is subtracted from
    // the merger's input list BEFORE building the merger, so dropped SSTables are
    // never read/decoded (the perf win); their components are deleted only after
    // the output publishes. `split_merge_and_dropped` handles the all-dropped guard.
    // Each input's Statistics.db is read once through the generation module (G11, GD9.1).
    let all_inputs = InputStats::resolve(&input_paths);
    all_inputs.require_trusted()?; // #4612: damaged Statistics.db
    let drop_set: Vec<PathBuf> = if purge_safe {
        fully_expired::fully_expired_sstables_in(
            &all_inputs,
            &InputStats::default(),
            gc_before_secs,
        )
    } else {
        Vec::new()
    };
    let (merge_inputs, dropped_whole) = split_merge_and_dropped(&input_paths, drop_set);
    // Merge only the drop-filtered `merge_inputs`; dropped ones are reclaimed after publish.
    let input_paths = merge_inputs;
    let inputs = all_inputs.subset(&input_paths);

    // #850: read static-row presence from the input SSTable headers. If a static
    // column is absent from the current schema but an input SSTable still declares
    // it (e.g. it was dropped from the catalog entirely, not retained via
    // `dropped_columns`), the effective schema re-adds it so the merger decodes the
    // static cells and the writer emits the static prelude. Byte-identical to
    // `schema` when no such column exists (the common case). This composes with the
    // #847/#904 dropped-column flow below: the effective schema keeps `schema`'s
    // `dropped_columns` map, so the retained-dropped pre-pass and write-schema
    // derivation operate on it unchanged.
    let mut effective_schema = effective_compaction_schema_in(schema, &inputs);
    // #929: copy each UDT column's exact UserType(...) marshal from the input
    // headers onto the EFFECTIVE (decode) schema so the compaction reader treats
    // it as complex (is_complex_column) and round-trips the per-field UDT cells
    // (roborev #1009/#1013/#1015/#1017/#1019). This derives ENTIRELY from the
    // input headers, so it runs unconditionally — a registry is needed only at
    // flush time. No-op when no input advertises a UserType column; errors on a
    // mixed encoding rather than corrupting. `write_schema` inherits it via
    // `for_compaction_output` (clones columns). When `udt_registry` is supplied,
    // a UDT column absent from every input (schema evolution) is registry-
    // normalized so the output header is not a bare/BytesType column.
    apply_udt_marshals_from_inputs_in(&mut effective_schema, &inputs, udt_registry)?;

    // Decode with `effective_schema` (retains dropped columns so their input cells
    // parse and can be purged), but WRITE with a post-drop schema: fully-purged
    // dropped columns are stripped from the output serialization header (a natural
    // post-drop reader is not misaligned) while dropped columns with surviving
    // re-added cells are retained (those cells keep a matching header column).
    // Which dropped columns survive is data-dependent and the writer fixes its
    // header before the first row, so determine the surviving set with a merge
    // pre-pass (only when any column is dropped). The pre-pass uses the SAME merge
    // logic as the write pass — including the same `purge_safe` flag (#921 finding
    // 1) and gc cutoff — so the two make IDENTICAL purge decisions and a tombstone
    // purged in the write pass is also purged in the pre-pass (never counted as a
    // surviving cell). See #847 review.
    let retained_dropped = if effective_schema.dropped_columns.is_empty() {
        std::collections::HashSet::new()
    } else {
        compute_surviving_dropped_columns(
            input_paths.clone(),
            &effective_schema,
            gc_before_secs,
            now_secs,
            purge_safe,
            // The one-shot `compact_sstables` entry point only knows its explicit
            // input list, not the rest of the table, so it has no overlap bound to
            // supply (#935 overlap-aware purging is driven by the WriteEngine
            // background path). Purging here is governed solely by `purge_safe`.
            None,
        )?
    };
    // `write_schema` inherits the #929 UDT normalization from the already-
    // normalized `effective_schema` (for_compaction_output clones columns).
    let write_schema = effective_schema.for_compaction_output(&retained_dropped);

    // Issue #1234: thread the registry onto the merge readers so a top-level
    // `frozen<UDT>` value decodes structurally instead of erroring out and
    // dropping the partition. Cloned because the merger owns its readers.
    let merger = KWayMerger::new_with_gc_and_registry(
        input_paths.clone(),
        &effective_schema,
        gc_before_secs,
        now_secs,
        udt_registry.cloned(),
    )?
    .with_purge_safe(purge_safe);

    // Repair-state preservation + mixed-state rejection (issue #1021): reject a
    // mixed repaired/unrepaired/pending-repair input set (Cassandra never mixes
    // them in one compaction) and otherwise carry the shared state forward.
    let repair_state = repair_state::classify_inputs_in(&inputs)?;

    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        output_dir.to_path_buf(),
        generation,
        &write_schema,
    )?;
    writer.set_repair_state(
        repair_state.repaired_at,
        repair_state.pending_repair,
        repair_state.is_transient,
    );
    // Compaction output (issue #1222): emit the uncompressed-BIG CRC.db with
    // Cassandra's compaction-only trailing empty-final-chunk CRC32 = 0.
    writer.mark_compaction_output();

    // Two-pass compaction (issue #729): seed the output's encoding baselines from
    // the inputs' Statistics.db before writing any partition.
    let (baseline_min_ts, mut baseline_min_ldt, baseline_min_ttl) =
        compute_baseline_min_in(&inputs);
    // Issue #1537: when expiry is active, an expired expiring cell converts to a
    // creation-time (`ldt - ttl`) tombstone whose LDT is BELOW the input-derived
    // baseline; lower the LDT baseline to a provably-safe floor so the DataWriter's
    // unsigned LDT-delta never underflows. No-op when no input carries expiring cells.
    if now_secs.is_some() {
        if let Some(floor) = compute_expiry_ttl_ldt_floor_in(&inputs) {
            baseline_min_ldt = baseline_min_ldt.min(floor);
        }
    }
    writer.pre_seed_encoding_baselines(baseline_min_ts, baseline_min_ldt, baseline_min_ttl);

    let mut stats = merger.merge(&mut writer)?;
    let output = writer.finish().await?;

    // Issue #4666: NOTHING is removed here. `writer.finish()` only wrote the output into the
    // caller's `output_dir` (a stage for the CLI); it is not published yet. Removing the
    // dropped-whole inputs now would lose their data if the publish then fails, so the caller
    // reclaims them with [`CompactReport::reclaim_dropped_inputs`] AFTER its durable publish
    // (Cassandra obsoletes originals only after the `LifecycleTransaction` commits).
    // Record the drop decision in the report/stats (issue #1388, R4), distinct from
    // the merged inputs so it is assertable from the plan, not just output absence.
    stats.dropped_whole = dropped_whole;

    Ok(CompactReport { output, stats })
}
