//! SSTable compaction execution: K-way merge start + atomic finalize.
//!
//! Extracted verbatim from `write_engine/maintenance.rs` (issue #1120, epic
//! #1116) as a behavior-preserving split. Owns the merge state-machine entry
//! points driven by `maintenance_step`: `start_merge` (opens the K-way merger +
//! tmp output writer) and the blocking/async finalizers that atomically publish
//! the merged SSTable and reclaim the inputs. `WriteEngine`'s fields are
//! reachable here because this is a sibling module in the same crate.

use super::maintenance::{ActiveMerge, MaintenanceReport};
use super::{merge, KWayMerger, WriteEngine};
use crate::error::{Error, Result};
use std::path::PathBuf;
use std::time::Instant;

// Test-only crash-injection seam (issue #1393).
//
// When set on the thread that drives `finalize_merge_async`, the finalizer
// aborts *after* the tmp component files have been written but *before* the
// atomic publication rename — and deliberately leaves the `.compaction-tmp-*`
// directory on disk, exactly as a real process crash mid-compaction would. The
// startup orphan sweep is then exercised end-to-end against genuine partial
// output. It is a `thread_local` (not a global) so parallel tests cannot see
// each other's injection: the sweep e2e test runs `maintenance_step`
// synchronously with no ambient runtime, so `block_on_async` polls the
// finalizer on the very thread that set the flag.
#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_COMPACTION_BEFORE_RENAME: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

// Test-only publication-barrier fsync fault seams (issue #1959).
//
// These let a test force the directory durability `fsync` to fail at each of
// the two publication points inside `finalize_merge_async`, exercising the
// rollback branches that a real EIO/ENOSPC on the directory handle would hit:
//
//   * `FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC` — fault the fsync performed AFTER
//     the non-TOC component renames but BEFORE the TOC rename (step 2b). A
//     failure here must roll back the already-renamed components and abort with
//     `Error::Storage`, so no partially-published set is left behind and the
//     inputs stay intact.
//   * `FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC` — fault the fsync performed AFTER
//     the TOC rename but BEFORE the inputs are deleted (step 2c). A failure here
//     must roll back the whole published set (INCLUDING the just-renamed TOC) so
//     no visible-but-not-durable TOC survives, and the inputs — still the only
//     durable copy — are never deleted.
//
// Like `FAIL_COMPACTION_BEFORE_RENAME` these are `thread_local` so parallel
// tests cannot see each other's injection; the driving test polls the finalizer
// synchronously on the same thread that set the flag.
#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    pub(crate) static FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

impl WriteEngine {
    /// Start a new merge operation (M5.2 helper, Issue #474)
    ///
    /// ## Atomicity design
    ///
    /// The SSTableWriter is pointed at a temporary directory (`tmp_dir`) that lives
    /// alongside the final SSTable directory. After `writer.finish()` succeeds, each
    /// output component is atomically renamed into the final directory. Input files are
    /// deleted only after all renames complete. This guarantees:
    ///
    /// - A crash before renames: tmp files are incomplete; inputs intact.
    /// - A crash mid-rename: at worst a partial output component exists in the final
    ///   directory, but the old inputs are still there and the TOC.txt (publication
    ///   barrier) has not been renamed yet, so the partial output is never discovered
    ///   by readers scanning for `TOC.txt`.
    /// - A crash after all renames but before input deletion: a harmless duplicate
    ///   exists until next compaction.
    ///
    /// `purge_safe` (#921 finding 1): `true` only when `input_paths` spans every
    /// SSTable for the table (a major/full compaction), allowing gc_grace
    /// tombstone purging without risking resurrection of data shadowed in a
    /// non-included overlapping SSTable. `false` keeps every tombstone.
    ///
    /// `max_purgeable_timestamp` (#935): for a PARTIAL compaction (`purge_safe ==
    /// false`), the min write timestamp across the non-included overlapping
    /// SSTables — see [`merge::compute_max_purgeable_timestamp`]. When `Some`, a
    /// tombstone older than every outside SSTable is purged even in a partial
    /// compaction; `None` keeps the conservative #921 behavior (no purging).
    #[tracing::instrument(name = "compaction.start_merge", skip(self, input_paths, max_purgeable_timestamp, outside_paths), fields(inputs = input_paths.len()))]
    pub(crate) fn start_merge(
        &mut self,
        input_paths: Vec<PathBuf>,
        purge_safe: bool,
        max_purgeable_timestamp: Option<i64>,
        outside_paths: Vec<PathBuf>,
    ) -> Result<()> {
        tracing::info!(
            "Starting compaction merge of {} SSTables",
            input_paths.len()
        );

        // gc_grace / gcBefore cutoff (issue #845): the fully-expired drop-set
        // (issue #1388) needs the same cutoff the merger's purge stage uses, so
        // compute it up front. `now_secs` is reused below for the merger.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let gc_before_secs = merge::compute_gc_before(&self.config.schema, now_secs);

        // Fully-expired SSTable drop (issue #1388): compute the subset of the
        // selected inputs that are provably fully expired (authoritative
        // `Statistics.db` `max_deletion_time < gcBefore`, no cell scan) AND
        // overlap-safe against the outside set (`max_timestamp` below the outside
        // min write timestamp — the same #935 coarse global bound). For a full
        // compaction `outside_paths` is empty ⇒ +inf bound ⇒ every fully-expired
        // input is droppable. These SSTables are EXCLUDED from the merger's input
        // list below (never read/decoded — the perf win) and reclaimed after the
        // merged output publishes.
        let drop_set = merge::fully_expired_sstables(&input_paths, &outside_paths, gc_before_secs);
        // Subtract the drop-set from the merger inputs (with the all-dropped guard);
        // shared with the CLI one-shot path.
        let (input_paths, dropped_whole) = merge::split_merge_and_dropped(&input_paths, drop_set);

        // Measure total bytes read (sum of Data.db file sizes as an approximation)
        let bytes_read: u64 = input_paths
            .iter()
            .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();

        let output_generation = self.generation;

        // Final SSTable directory: data_dir/keyspace/table/
        let sstable_dir = self
            .config
            .data_dir
            .join(&self.config.schema.keyspace)
            .join(&self.config.schema.table);

        // Temporary root: data_dir/.compaction-tmp-{gen}/
        //
        // Placing the tmp root inside `data_dir` (not inside `sstable_dir`) keeps
        // the path simple and guarantees the rename is within the same filesystem.
        // The SSTableWriter appends `keyspace/table/` internally, so component files
        // land at: data_dir/.compaction-tmp-{gen}/keyspace/table/nb-{gen}-big-*.db
        let tmp_dir = self
            .config
            .data_dir
            .join(format!(".compaction-tmp-{}", output_generation));

        // Create the tmp root (SSTableWriter::finish will create the subdirs)
        std::fs::create_dir_all(&tmp_dir).map_err(|e| {
            Error::Storage(format!(
                "Failed to create compaction tmp directory {:?}: {}",
                tmp_dir, e
            ))
        })?;

        // #850: derive the effective compaction schema by reading static-row
        // presence from the input SSTable headers. If a static column was dropped
        // from the current schema but an input still carries it, re-add it so the
        // merger decodes the static cells and the writer emits the static prelude
        // (header-driven static presence). Byte-identical to `config.schema` when
        // no such column exists.
        let mut effective_schema =
            merge::effective_compaction_schema(&self.config.schema, &input_paths);
        // #929: normalize bare-name UDT columns to their UserType(...) marshal on
        // the EFFECTIVE (decode) schema by copying each column's exact UserType
        // marshal from the input headers. The compaction reader decides complex-
        // vs-simple from this schema's `data_type` (is_complex_column), so a bare
        // name would make it misdecode/drop pre-existing per-field UDT cells
        // (roborev #1009/#1013/#1015/#1017/#1019/#1021). Derived ENTIRELY from the
        // input headers, so it runs unconditionally (a registry is needed only at
        // flush time); errors on a mixed encoding rather than corrupting.
        // `write_schema` derives from this via `for_compaction_output`. The
        // configured registry handles a UDT column added after the inputs were
        // written (absent from every input header) so the output is not emitted
        // as a bare/BytesType column (roborev #1023).
        merge::apply_udt_marshals_from_inputs(
            &mut effective_schema,
            &input_paths,
            self.config.udt_registry.as_ref(),
        )?;

        // Create K-way merger.
        //
        // gc_grace / gcBefore purging (issue #845): `gcBefore = now -
        // gc_grace_seconds` and `now_secs` were computed above (also used by the
        // #1388 fully-expired drop-set). `compute_gc_before` returns `None` when the
        // table declares no valid `gc_grace_seconds`, disabling purging (and, above,
        // dropping) — preserving the pre-#845 behavior. `now_secs` is the wall-clock
        // used for both the purge boundary and TTL evaluation.
        //
        // Overlap-safety gate (#921 finding 1): only a major/full compaction
        // (one that spans every candidate SSTable for the table) may purge
        // tombstones. A partial compaction sets `purge_safe = false`, which
        // makes the gc_grace purge stage a strict no-op and cannot resurrect
        // data shadowed in a non-included overlapping SSTable.

        // #921 finding 2: mirror `compact_sstables`' dropped-column survivor
        // pre-pass. Decode with `effective_schema` (retains dropped columns so
        // their cells parse and can be purged), but WRITE with a post-drop
        // schema: a background compaction that purges the LAST cell/tombstone of
        // a dropped column must NOT emit that column in the output
        // SerializationHeader (stale header columns misalign a post-drop reader).
        // The pre-pass uses the SAME gc cutoff AND `purge_safe` as the write
        // merger below so the two make IDENTICAL purge decisions — a tombstone
        // purged in the write pass is also purged here and never counted as a
        // surviving cell. Reuses the exact helper `compact_sstables` uses.
        let retained_dropped = if effective_schema.dropped_columns.is_empty() {
            std::collections::HashSet::new()
        } else {
            merge::compute_surviving_dropped_columns(
                input_paths.clone(),
                &effective_schema,
                gc_before_secs,
                Some(now_secs),
                purge_safe,
                max_purgeable_timestamp,
            )?
        };
        // `write_schema` inherits the #929 UDT normalization from the already-
        // normalized `effective_schema` (for_compaction_output clones columns).
        let write_schema = effective_schema.for_compaction_output(&retained_dropped);

        // Issue #1234: thread the configured UDT registry onto the merge readers so
        // a top-level `frozen<UDT>` value decodes structurally during background
        // compaction instead of erroring out and dropping the partition.
        let merger = KWayMerger::new_with_gc_and_registry(
            input_paths.clone(),
            &effective_schema,
            gc_before_secs,
            Some(now_secs),
            self.config.udt_registry.clone(),
        )?
        .with_purge_safe(purge_safe)
        // #935: overlap-aware purging for a partial compaction. `purge_safe`
        // overrides this in the merger (full compaction → +inf bound), so passing
        // the partial bound unconditionally is safe.
        .with_max_purgeable_timestamp(max_purgeable_timestamp);

        // Repair-state preservation + mixed-state rejection (issue #1021).
        //
        // Cassandra partitions compaction candidates by repair state and never
        // mixes repaired / unrepaired / pending-repair SSTables in one
        // compaction. CQLite cannot reproduce the repair-boundary tombstone
        // constraints, so it MUST NOT silently merge across that boundary: read
        // each input's persisted repair state from its Statistics.db, reject a
        // mixed set with a typed error, and PRESERVE the single shared state into
        // the merged output below. Authoritative metadata only (no heuristics).
        let repair_state = merge::classify_inputs(&input_paths)?;

        // Point the SSTableWriter at the tmp root; it will write to
        // tmp_dir/keyspace/table/nb-{gen}-big-*.db. Use the post-drop
        // `write_schema` (NOT `effective_schema`) so fully-purged dropped
        // columns are stripped from the output header (#921 finding 2).
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            tmp_dir.clone(),
            output_generation,
            &write_schema,
        )?;
        // Compaction output (issue #1222): emit the uncompressed-BIG CRC.db with
        // Cassandra's compaction-only trailing empty-final-chunk CRC32 = 0. The
        // flush path leaves this unset, keeping its CRC.db byte-identical.
        writer.mark_compaction_output();

        // Carry the inputs' shared repair state into the merged output's
        // Statistics.db (issue #1021). For a normal unrepaired corpus this is the
        // unrepaired default (a no-op vs the previous hardcoded zeros).
        writer.set_repair_state(
            repair_state.repaired_at,
            repair_state.pending_repair,
            repair_state.is_transient,
        );

        // Two-pass compaction (issue #729): compute output FINAL encoding baselines
        // by reading the minimum values from each input SSTable's Statistics.db.
        // The output baseline must be ≤ all per-partition values from all inputs.
        let (baseline_min_ts, mut baseline_min_ldt, baseline_min_ttl) =
            merge::compute_baseline_min(&input_paths);
        // Issue #1537: unlike `compact_sstables_with_registry` — whose
        // `now_secs: Option<i64>` parameter can be `None` (expiry disabled), so it
        // gates the floor application on `now_secs.is_some()` — this WriteEngine
        // path computes `now_secs` locally above as a plain `i64` (unwrap_or(0))
        // and ALWAYS builds the merger with the literal `Some(now_secs)` (see the
        // `KWayMerger::new_with_gc_and_registry` construction above). Expiry is
        // therefore unconditionally active here, so no `is_some()` gate is needed
        // (and `now_secs` is not an `Option`, so a literal mirror would not even
        // compile). An expired expiring cell converts to a creation-time
        // (`ldt - ttl`) tombstone whose LDT sits BELOW the input-derived baseline,
        // so lower the LDT baseline to a provably-safe floor so the DataWriter's
        // unsigned LDT-delta never underflows. No-op when no input carries
        // expiring cells.
        if let Some(floor) = merge::compute_expiry_ttl_ldt_floor(&input_paths) {
            baseline_min_ldt = baseline_min_ldt.min(floor);
        }
        writer.pre_seed_encoding_baselines(baseline_min_ts, baseline_min_ldt, baseline_min_ttl);

        // Increment generation for next operation
        self.generation += 1;

        self.active_merge = Some(ActiveMerge {
            merger,
            writer,
            input_paths,
            tmp_dir,
            sstable_dir,
            rows_merged: 0,
            bytes_read,
            started_at: Instant::now(),
            effective_schema,
            dropped_whole,
        });

        Ok(())
    }

    /// Finalize the active merge - blocking version (M5.2 helper).
    ///
    /// Bridges to the async finalizer via [`merge::block_on_async`], which is
    /// safe to call from within an active Tokio runtime (e.g. the CLI's
    /// `#[tokio::main]` worker threads). A nested `Handle::block_on` here would
    /// otherwise panic with "Cannot start a runtime from within a runtime"
    /// (Issue #587).
    pub(crate) fn finalize_merge_blocking(&mut self, report: &mut MaintenanceReport) -> Result<()> {
        merge::block_on_async(self.finalize_merge_async(report))
    }

    /// Finalize the active merge - async version (M5.2 helper, Issue #474)
    ///
    /// ## Atomicity protocol
    ///
    /// 1. `writer.finish()` flushes all component files to the tmp directory.
    /// 2. Each component file is renamed from `tmp_dir/` to `sstable_dir/`.
    ///    The TOC.txt rename is performed **last** (it is the publication barrier:
    ///    readers discover SSTables by scanning for `TOC.txt`).
    /// 3. Only after all renames succeed are the input SSTable files deleted.
    /// 4. The now-empty tmp directory is removed.
    ///
    /// If any step 2 rename fails, the partially-renamed output components are
    /// cleaned up and an error is returned. The input SSTables remain intact.
    ///
    /// Single-boundary error recording (issue #1037): this is an *unrecorded*
    /// inner helper. Any error returned here escapes through
    /// `maintenance_step_inner` to the public `maintenance_step`, which wraps the
    /// whole step in `record_result("compaction", ..)` and counts it exactly
    /// once. Recording here too would double-count finalize failures.
    #[tracing::instrument(name = "compaction.finalize", skip(self, report))]
    async fn finalize_merge_async(&mut self, report: &mut MaintenanceReport) -> Result<()> {
        let merge = match self.active_merge.take() {
            Some(m) => m,
            None => return Ok(()),
        };

        let input_count = merge.input_paths.len() as u64;
        let merge_rows = merge.rows_merged;
        let elapsed = merge.started_at.elapsed();
        tracing::info!(
            "Finalizing compaction merge: {} rows, {:?} elapsed",
            merge.rows_merged,
            elapsed
        );

        // Step 1: Finish writing all components to the tmp directory.
        // If this fails the tmp directory may contain partial files, but inputs are safe.
        let tmp_info = match merge.writer.finish().await {
            Ok(info) => info,
            Err(e) => {
                // Clean up tmp directory on failure (best effort)
                let _ = std::fs::remove_dir_all(&merge.tmp_dir);
                return Err(Error::Storage(format!(
                    "Compaction merge write failed (inputs intact): {}",
                    e
                )));
            }
        };

        tracing::info!(
            "Compaction tmp output: {} bytes, {} partitions",
            tmp_info.data_size,
            tmp_info.partition_count
        );

        // Test-only crash injection (issue #1393): simulate a process crash after
        // the tmp component files are on disk but before the publication rename.
        // We intentionally do NOT clean up `merge.tmp_dir` — a real crash leaves
        // it for the startup orphan sweep to reclaim. Inputs are still intact
        // (no rename or delete has run yet).
        #[cfg(test)]
        if FAIL_COMPACTION_BEFORE_RENAME.with(|f| f.get()) {
            return Err(Error::Storage(
                "injected compaction crash before publication rename (test seam, issue #1393)"
                    .to_string(),
            ));
        }

        // Step 2: Atomically rename each component from the tmp sub-directory to
        // the final SSTable directory. Because both directories are under the same
        // `data_dir`, the rename is within the same filesystem (POSIX atomic).
        // We rename TOC.txt last because it is the publication barrier.
        let sstable_dir = &merge.sstable_dir;
        // The configured data root — the top of the leaf→data-root ancestor
        // chain fsynced at publication (issue #1959, symmetry with the flush
        // barrier). Cloned so it does not alias the `&mut self` borrows below.
        let data_root = self.config.data_dir.clone();

        // Ensure the final SSTable directory exists (it normally does, but handle
        // the edge case where it was created by start_merge only in the tmp path).
        // This defensive `create_dir_all` can materialize `sstable_dir` (and even
        // an ancestor) whose PARENT dirent is not otherwise fsynced by this path;
        // the leaf→data-root chain fsync at publication (steps 2b/2c below) makes
        // every ancestor dirent durable, so the newly created directory cannot be
        // lost on a crash after the inputs are deleted (issue #1959).
        std::fs::create_dir_all(sstable_dir).map_err(|e| {
            Error::Storage(format!(
                "Failed to create SSTable directory {:?}: {}",
                sstable_dir, e
            ))
        })?;

        // Helper: map a tmp component path to its final destination.
        // tmp_info paths look like: data_dir/.compaction-tmp-N/keyspace/table/nb-N-big-X.db
        // Final destination:        data_dir/keyspace/table/nb-N-big-X.db
        let make_rename = |src: &PathBuf| -> Result<(PathBuf, PathBuf)> {
            let filename = src
                .file_name()
                .ok_or_else(|| Error::Storage("Component path has no filename".to_string()))?;
            let dst = sstable_dir.join(filename);
            Ok((src.clone(), dst))
        };

        // Build list of (src, dst) renames. TOC.txt goes last (publication barrier).
        let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();

        // Non-TOC components first. Index.db/Summary.db are BIG-only (BTI writers
        // report them as None — issue #908); only rename them when present.
        // Filter.db is optional too: a table with a disabled bloom filter
        // (bloom_filter_fp_chance = 1.0, AlwaysPresentFilter) emits NO Filter.db,
        // so there is nothing to rename. Including a non-existent path here would
        // fail the atomic publish (Issue #852).
        let mut non_toc: Vec<&PathBuf> = vec![&tmp_info.data_path];
        if let Some(ref p) = tmp_info.index_path {
            non_toc.push(p);
        }
        if let Some(ref p) = tmp_info.filter_path {
            non_toc.push(p);
        }
        if let Some(ref p) = tmp_info.summary_path {
            non_toc.push(p);
        }
        non_toc.push(&tmp_info.stats_path);
        non_toc.push(&tmp_info.digest_path);
        // CRC.db is emitted for uncompressed BIG output (issue #1197); rename it
        // when present so the published SSTable keeps its per-chunk CRC file.
        if let Some(ref p) = tmp_info.crc_path {
            non_toc.push(p);
        }
        for src in non_toc {
            renames.push(make_rename(src)?);
        }
        if let Some(ref p) = tmp_info.partitions_path {
            renames.push(make_rename(p)?);
        }
        if let Some(ref ci_path) = tmp_info.compression_info_path {
            renames.push(make_rename(ci_path)?);
        }
        // TOC.txt is the publication barrier: rename it LAST and — per the
        // crash-safety invariant — only AFTER the non-TOC component dirents are
        // durable (issue #1959). Keep it OUT of the `renames` list so the
        // intervening directory fsync can be sequenced between the components and
        // the TOC.
        let toc_rename = make_rename(&tmp_info.toc_path)?;

        // Perform the renames. On failure, remove any already-renamed files so
        // we don't leave a half-published SSTable, then return the error.
        // Time the rename/publication-barrier phase (issue #1037).
        let finalize_start = Instant::now();
        let mut renamed: Vec<PathBuf> = Vec::with_capacity(renames.len() + 1);

        // Best-effort rollback: undo the renames done so far and drop the tmp dir.
        let rollback = |renamed: &[PathBuf], tmp_dir: &std::path::Path| {
            for dst in renamed {
                let _ = std::fs::remove_file(dst);
            }
            let _ = std::fs::remove_dir_all(tmp_dir);
        };

        // Fsync the full leaf→data-root ancestor chain of the published SSTable
        // directory (issue #1959). This mirrors the flush durability barrier
        // (`durability::finalize_flush_durability`), which fsyncs the same chain
        // via `dirs_to_sync`: not just the leaf `sstable_dir` (its component
        // dirents) but every ancestor up to the data root, so the leaf's own
        // entry in its parent — potentially just created by the defensive
        // `create_dir_all` above — is durable too. Each `sync_directory` is a
        // no-op on non-Unix (the Unix-only invariant, matching the flush path).
        let fsync_publish_chain = || -> Result<()> {
            for dir in super::durability::dirs_to_sync(sstable_dir, &data_root) {
                super::durability::sync_directory(&dir)?;
            }
            Ok(())
        };

        // Step 2a: rename every NON-TOC component into the final directory.
        for (src, dst) in &renames {
            if let Err(e) = std::fs::rename(src, dst) {
                let err = Error::Storage(format!(
                    "Atomic rename of {:?} to {:?} failed (rolling back, inputs intact): {}",
                    src, dst, e
                ));
                rollback(&renamed, &merge.tmp_dir);
                return Err(err);
            }
            tracing::debug!(
                "Renamed {:?} → {:?}",
                src.file_name().unwrap_or_default(),
                dst.file_name().unwrap_or_default()
            );
            renamed.push(dst.clone());
        }

        // Step 2b: directory durability barrier BEFORE publishing the TOC
        // (issue #1959). Component *contents* were fsynced in `writer.finish()`,
        // but the renames only altered dirents; fsync the destination directory
        // (and its ancestors) so every non-TOC component's dirent is durable
        // before the TOC — the barrier readers scan for — is renamed in.
        // Sequencing the fsync here means the TOC dirent can only become durable
        // AFTER the components it names, so a crash can never expose a visible
        // TOC that references a not-yet-durable component. NOTE: this invariant
        // holds on Unix only — `sync_directory` is a no-op on non-Unix, where the
        // per-file fsyncs are the durability guarantee (issue #1392 / #1959).
        #[cfg(test)]
        if FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC.with(|f| f.get()) {
            rollback(&renamed, &merge.tmp_dir);
            return Err(Error::Storage(
                "injected directory fsync fault before TOC rename (test seam, issue #1959)"
                    .to_string(),
            ));
        }
        if let Err(e) = fsync_publish_chain() {
            rollback(&renamed, &merge.tmp_dir);
            return Err(e);
        }

        // Step 2c: publish by renaming TOC.txt last, then fsync the LEAF
        // directory AGAIN so the TOC dirent itself is durable BEFORE the input
        // SSTables (the only other durable copy of this data) are deleted below.
        // Without this second fsync, a crash after the inputs are removed but
        // before the TOC dirent reached the device would lose the SSTable
        // entirely (issue #1959). Same Unix-only scope as step 2b: a no-op on
        // non-Unix, where the per-file fsyncs are the durability guarantee.
        //
        // ASYMMETRY vs step 2b (issue #1959): 2b fsyncs the FULL leaf→data-root
        // chain because the non-TOC renames + the defensive `create_dir_all` can
        // touch ancestor dirents that must be durable before publication; between
        // 2b and 2c the ONLY dirent that changed is the TOC entry in the LEAF
        // `sstable_dir` (the TOC rename). The ancestor dirents were already made
        // durable by 2b, so re-fsyncing the whole chain here is redundant I/O —
        // fsyncing only the leaf is sufficient to make the TOC dirent durable.
        {
            let (src, dst) = &toc_rename;
            if let Err(e) = std::fs::rename(src, dst) {
                let err = Error::Storage(format!(
                    "Atomic rename of {:?} to {:?} failed (rolling back, inputs intact): {}",
                    src, dst, e
                ));
                rollback(&renamed, &merge.tmp_dir);
                return Err(err);
            }
            renamed.push(dst.clone());
        }
        #[cfg(test)]
        if FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC.with(|f| f.get()) {
            // Roll back the WHOLE published set, including the just-renamed TOC,
            // so no visible-but-not-durable TOC survives; the inputs are never
            // deleted (they remain the sole durable copy).
            rollback(&renamed, &merge.tmp_dir);
            return Err(Error::Storage(
                "injected directory fsync fault after TOC rename (test seam, issue #1959)"
                    .to_string(),
            ));
        }
        // Leaf-only (see the step 2c asymmetry note above): only the TOC dirent in
        // `sstable_dir` changed since the full-chain fsync at 2b.
        if let Err(e) = super::durability::sync_directory(sstable_dir) {
            rollback(&renamed, &merge.tmp_dir);
            return Err(e);
        }

        // Finalize/rename latency in seconds (issue #1037): the atomic
        // publication barrier just completed successfully.
        crate::observability::record_histogram(
            crate::observability::catalog::COMPACTION_FINALIZE_DURATION,
            finalize_start.elapsed().as_secs_f64(),
            &[],
        );

        // Step 3: All renames succeeded. The new SSTable is now visible.
        // Delete input SSTable files. If deletion fails we log a warning but do
        // NOT return an error — the merge output is correct.
        //
        // Issue #591 (write-while-mapped / Windows policy): the inputs were read
        // through buffered I/O (never memory-mapped) and the merge has fully
        // drained every source through its streaming producer by this point
        // (issue #827), so the merger holds no mapping over them — deleting them
        // cannot fault with SIGBUS. `delete_sstable_files` removes each
        // input's TOC.txt first (unpublishing it) and is best-effort on the data
        // components, so a component still pinned by a concurrent mapped reader on
        // Windows becomes an invisible orphan reclaimed on the next startup rather
        // than a hard failure or a source of duplicate rows.
        for input_path in &merge.input_paths {
            if let Err(e) = self.delete_sstable_files(input_path) {
                tracing::warn!(
                    "Failed to delete compaction input {:?}: {} \
                     (merge output is valid; inputs will be re-evaluated next cycle)",
                    input_path,
                    e
                );
            }
        }

        // Reclaim the DROPPED-WHOLE SSTables (issue #1388): proven fully expired +
        // overlap-safe, they were EXCLUDED from the merger (never read) so their
        // reclamation happens here — AFTER the merged output published — via the
        // same best-effort component-delete path as the merged inputs. A failure is
        // an invisible orphan (TOC.txt removed first) reclaimed on next startup.
        //
        // This surface already deleted EVERY merge input above, so pass
        // `already_deleted = merge.input_paths`: in the degenerate all-expired case
        // the SSTable the all-dropped guard retained as a merge input is ALSO in
        // `dropped_whole`, and the loop above already reclaimed it — the dedup skips
        // it here to avoid a double-delete + spurious orphan warning (roborev #1388).
        crate::storage::write_engine::merge::reclaim_dropped_whole(
            &merge.dropped_whole,
            &merge.input_paths,
            |dropped| {
                if let Err(e) = self.delete_sstable_files(dropped) {
                    tracing::warn!(
                        "Failed to delete dropped-whole compaction input {:?}: {} \
                         (merge output is valid; leftover is an invisible orphan)",
                        dropped,
                        e
                    );
                }
            },
        );

        // Step 4: Remove the now-empty tmp directory (best effort).
        if let Err(e) = std::fs::remove_dir_all(&merge.tmp_dir) {
            tracing::debug!(
                "Failed to remove compaction tmp directory {:?}: {}",
                merge.tmp_dir,
                e
            );
        }

        // The final Data.db path is in sstable_dir (renamed from tmp)
        let final_data_path = sstable_dir.join(
            tmp_info
                .data_path
                .file_name()
                .ok_or_else(|| Error::Storage("Data.db path has no filename".to_string()))?,
        );

        // Compute total bytes written across ALL output SSTable components (not just Data.db).
        // We stat the final paths (post-rename) so we measure what was actually persisted.
        let stat_final = |p: &PathBuf| -> u64 {
            let filename = p.file_name().unwrap_or_default();
            std::fs::metadata(sstable_dir.join(filename))
                .map(|m| m.len())
                .unwrap_or(0)
        };
        // Index.db/Summary.db are BIG-only (BTI reports None — issue #908) and
        // Filter.db is optional (disabled bloom filter omits it, Issue #852), so
        // only stat the components that were actually written.
        let mut byte_paths: Vec<&PathBuf> = vec![&tmp_info.data_path];
        if let Some(ref p) = tmp_info.index_path {
            byte_paths.push(p);
        }
        if let Some(ref p) = tmp_info.filter_path {
            byte_paths.push(p);
        }
        if let Some(ref p) = tmp_info.summary_path {
            byte_paths.push(p);
        }
        byte_paths.push(&tmp_info.stats_path);
        byte_paths.push(&tmp_info.digest_path);
        if let Some(ref p) = tmp_info.crc_path {
            byte_paths.push(p);
        }
        if let Some(ref p) = tmp_info.partitions_path {
            byte_paths.push(p);
        }
        let total_bytes_written: u64 = byte_paths.iter().map(|p| stat_final(p)).sum::<u64>()
            + tmp_info
                .compression_info_path
                .as_ref()
                .map(stat_final)
                .unwrap_or(0);

        // Update per-step report
        report.completed_merges.push(final_data_path);
        report.bytes_written += total_bytes_written;
        // Record the dropped-whole set (issue #1388, R4), distinct from the merged
        // inputs, so the drop decision is assertable from the plan/stats.
        report
            .dropped_whole
            .extend(merge.dropped_whole.iter().cloned());

        // Update cumulative lifetime stats
        self.cumulative_stats.compactions_completed += 1;
        self.cumulative_stats.sstables_merged_in += merge.input_paths.len() as u64;
        self.cumulative_stats.sstables_produced += 1;
        self.cumulative_stats.bytes_read += merge.bytes_read;
        self.cumulative_stats.bytes_written += total_bytes_written;
        self.cumulative_stats.rows_merged += merge.rows_merged;
        self.cumulative_stats.total_time += elapsed;

        // Compaction completion metrics (issue #1037): full-compaction duration
        // (pairs with COMPACTION_ROWS_MERGED — emitted incrementally per
        // maintenance step — for rows/sec throughput), bytes written across all
        // output components, and SSTables in/out. Rows are NOT re-counted here to
        // avoid double-counting the per-step COMPACTION_ROWS_MERGED increments.
        let _ = merge_rows;
        {
            use crate::observability::{self as obs, catalog};
            obs::record_histogram(catalog::COMPACTION_DURATION, elapsed.as_secs_f64(), &[]);
            obs::add_counter(catalog::COMPACTION_BYTES_WRITTEN, total_bytes_written, &[]);
            obs::add_counter(catalog::COMPACTION_SSTABLES_IN, input_count, &[]);
            obs::add_counter(catalog::COMPACTION_SSTABLES_OUT, 1, &[]);
        }

        tracing::info!(
            "Compaction complete: merged {} inputs → 1 output ({} bytes total across all components, {} rows, {:?})",
            merge.input_paths.len(),
            total_bytes_written,
            merge.rows_merged,
            elapsed
        );

        Ok(())
    }
}
