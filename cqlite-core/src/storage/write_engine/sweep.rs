//! Startup orphan sweeps for the write engine (issue #1393).
//!
//! When the process crashes mid-compaction, `finalize_merge_async` can leave two
//! kinds of orphan on disk:
//!
//!   (a) a `.compaction-tmp-{gen}/` directory under `data_dir` holding partial
//!       output component files that were flushed but never renamed, and
//!   (b) a partial set of renamed `nb-{gen}-big-*.db` components in
//!       `data_dir/{keyspace}/{table}/` that lack a matching `TOC.txt` (the
//!       publication barrier), left when a crash lands between the first
//!       component rename and the `TOC.txt` rename.
//!
//! Both are reclaimed by the startup sweeps below, which run in
//! [`WriteEngine::open`](super::WriteEngine) before the WAL is replayed. The
//! sweeps are the single most dangerous delete-code in the write engine: a bug
//! here can destroy live data. They are therefore **best-effort and observable**
//! — each returns a [`SweepOutcome`] recording exactly what was removed and any
//! non-fatal failure, so a failure never aborts startup yet is still surfaced
//! (and asserted in tests) rather than swallowed into a log line.

use super::{WriteEngine, WriteEngineConfig};
use std::path::{Path, PathBuf};

/// Outcome of a single startup orphan sweep.
///
/// The sweep is best-effort and never aborts engine startup. This struct makes
/// its actions observable so callers (and tests) can assert what happened
/// without scraping log output:
///
/// * `removed` — orphan paths that were successfully reclaimed. For the partial
///   SSTable sweep this is the `Data.db` path whose component set was deleted.
/// * `failures` — one human-readable message per orphan that could NOT be
///   removed (for example a permissions error). A non-empty `failures` is the
///   "surfaced but non-fatal" condition described in issue #1393 AC #3.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SweepOutcome {
    /// Orphan paths successfully removed by this sweep.
    pub(crate) removed: Vec<PathBuf>,
    /// Non-fatal failures encountered (one message per un-removable orphan).
    pub(crate) failures: Vec<String>,
}

impl SweepOutcome {
    /// True when at least one orphan could not be removed. Startup continues
    /// regardless; callers log the condition.
    pub(crate) fn had_failures(&self) -> bool {
        !self.failures.is_empty()
    }
}

impl WriteEngine {
    /// Run both startup orphan sweeps and log any non-fatal failures.
    ///
    /// Called once from [`WriteEngine::open`](super::WriteEngine) before WAL
    /// replay. Never returns an error: an un-removable orphan is logged and left
    /// for a later sweep, it does not abort startup (issue #1393 AC #3).
    pub(crate) fn sweep_startup_orphans(config: &WriteEngineConfig) {
        let data_dir = &config.data_dir;
        let tmp = Self::sweep_orphaned_compaction_tmp(data_dir);
        let partial = Self::sweep_orphaned_partial_sstables(
            data_dir,
            &config.schema.keyspace,
            &config.schema.table,
        );
        for outcome in [&tmp, &partial] {
            if outcome.had_failures() {
                for failure in &outcome.failures {
                    tracing::warn!(
                        "startup orphan sweep left an un-removable orphan (non-fatal, \
                         will retry next startup): {}",
                        failure
                    );
                }
            }
        }
    }

    /// Startup sweep (a): remove any `.compaction-tmp-*` directories left under
    /// `data_dir` by a previous crash mid-rename. Best-effort — individual
    /// failures are recorded in the returned [`SweepOutcome`] and logged, but do
    /// not abort engine startup.
    pub(crate) fn sweep_orphaned_compaction_tmp(data_dir: &Path) -> SweepOutcome {
        let mut outcome = SweepOutcome::default();
        let read_dir = match std::fs::read_dir(data_dir) {
            Ok(rd) => rd,
            Err(e) => {
                tracing::debug!(
                    "sweep_orphaned_compaction_tmp: cannot read {:?}: {}",
                    data_dir,
                    e
                );
                return outcome;
            }
        };

        for entry in read_dir.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(".compaction-tmp-") && path.is_dir() {
                tracing::warn!("removing orphaned compaction tmp directory: {:?}", path);
                match std::fs::remove_dir_all(&path) {
                    Ok(()) => outcome.removed.push(path),
                    Err(e) => {
                        tracing::warn!(
                            "failed to remove orphaned compaction tmp directory {:?}: {}",
                            path,
                            e
                        );
                        outcome.failures.push(format!("{:?}: {}", path, e));
                    }
                }
            }
        }
        outcome
    }

    /// Startup sweep (b): remove any `nb-{gen}-big-Data.db` (and its siblings)
    /// under `data_dir/keyspace/table/` that lack a matching `TOC.txt`.
    ///
    /// Such files are left when a crash occurs after some component renames but
    /// before `TOC.txt` is renamed (the publication barrier). A published
    /// generation — Data.db *with* a sibling TOC.txt — is never touched. The
    /// returned [`SweepOutcome`] records each reclaimed Data.db path and any
    /// non-fatal removal failure.
    pub(crate) fn sweep_orphaned_partial_sstables(
        data_dir: &Path,
        keyspace: &str,
        table: &str,
    ) -> SweepOutcome {
        let mut outcome = SweepOutcome::default();
        let sstable_dir = data_dir.join(keyspace).join(table);

        let read_dir = match std::fs::read_dir(&sstable_dir) {
            Ok(rd) => rd,
            Err(_) => {
                // Directory doesn't exist yet — nothing to sweep.
                return outcome;
            }
        };

        for entry in read_dir.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Look for Data.db files produced by the writer (nb-{gen}-big-Data.db)
            if !name_str.starts_with("nb-")
                || !name_str.ends_with("-big-Data.db")
                || !path.is_file()
            {
                continue;
            }

            // Extract the base prefix (e.g. "nb-5-big") to find the TOC sibling
            let base = match name_str.strip_suffix("-Data.db") {
                Some(b) => b.to_owned(),
                None => continue,
            };

            // Extract the generation number for the log message
            let gen_str = base
                .strip_prefix("nb-")
                .and_then(|s| s.strip_suffix("-big"))
                .unwrap_or(&base);

            let toc_path = sstable_dir.join(format!("{}-TOC.txt", base));
            if !toc_path.exists() {
                tracing::warn!(
                    "removing orphaned partial SSTable components for generation {}: missing TOC.txt",
                    gen_str
                );
                match Self::delete_sstable_files_static(&path) {
                    Ok(()) => outcome.removed.push(path.clone()),
                    Err(e) => {
                        tracing::warn!(
                            "failed to remove orphaned partial SSTable for generation {}: {}",
                            gen_str,
                            e
                        );
                        outcome
                            .failures
                            .push(format!("generation {}: {}", gen_str, e));
                    }
                }
            }
        }
        outcome
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use crate::schema::TableSchema;
    use crate::storage::sstable::reader::compaction_row::CompactionRowData;
    use crate::storage::sstable::reader::SSTableReader;
    use crate::storage::write_engine::test_support::{create_test_schema, flush_n_sstables_sync};
    use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Build a WriteEngineConfig pointing at a given temp dir's data/wal pair.
    fn config_for(temp_dir: &TempDir) -> WriteEngineConfig {
        WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            create_test_schema(),
        )
    }

    /// The `keyspace/table` directory holding published SSTable components for
    /// the shared test schema.
    fn sstable_dir(temp_dir: &TempDir) -> std::path::PathBuf {
        temp_dir
            .path()
            .join("data")
            .join("test_ks")
            .join("test_table")
    }

    /// Enumerate the immediate names inside a directory as a sorted set. Used to
    /// assert byte-for-byte set equality before/after a sweep.
    fn dir_names(dir: &Path) -> BTreeSet<String> {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Read the number of live rows from a single on-disk SSTable Data.db file.
    fn live_row_count(data_path: &Path, schema: &TableSchema) -> usize {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let config = crate::Config::default();
            let platform =
                std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
            let reader = SSTableReader::open(data_path, &config, platform)
                .await
                .unwrap();
            let rows = reader
                .iterate_all_partitions_for_compaction(Some(schema))
                .await
                .unwrap();
            rows.iter()
                .filter(|r| matches!(r.row_data, CompactionRowData::Live { .. }))
                .count()
        })
    }

    /// Probe whether this process can be blocked from deleting a file by making
    /// its parent directory read-only. Returns `true` on a normal (non-root)
    /// account where the sweep-failure path is exercisable; `false` when running
    /// as root (permission checks bypassed) so the permissions test can skip
    /// rather than falsely pass. Restores permissions before returning.
    #[cfg(unix)]
    fn readonly_dir_blocks_delete() -> bool {
        use std::os::unix::fs::PermissionsExt;
        let probe = TempDir::new().unwrap();
        let victim = probe.path().join("victim");
        std::fs::write(&victim, b"x").unwrap();
        std::fs::set_permissions(probe.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let blocked = std::fs::remove_file(&victim).is_err();
        // Restore write permission so TempDir cleanup succeeds.
        std::fs::set_permissions(probe.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        blocked
    }

    // ── AC #1: true orphan removed, live generation untouched & readable ────────

    #[test]
    fn true_orphan_removed_live_generation_untouched_and_readable() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Produce one COMPLETE, published live generation (5 rows).
        let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
        let live_paths = flush_n_sstables_sync(&mut engine, 1);
        let live_data = live_paths[0].clone();
        drop(engine);

        let dir = sstable_dir(&temp_dir);
        let live_bytes_before = std::fs::read(&live_data).unwrap();
        let live_rows_before = live_row_count(&live_data, &schema);
        assert_eq!(live_rows_before, 5, "sanity: live generation has 5 rows");

        // Simulate a crash mid-compaction: a `.compaction-tmp-99/` dir with a
        // partial component AND a partial-rename orphan (nb-99-big-Data.db with
        // no TOC.txt) alongside the live generation.
        let orphan_tmp = temp_dir.path().join("data").join(".compaction-tmp-99");
        std::fs::create_dir_all(orphan_tmp.join("test_ks").join("test_table")).unwrap();
        std::fs::write(
            orphan_tmp
                .join("test_ks")
                .join("test_table")
                .join("nb-99-big-Data.db"),
            b"partial",
        )
        .unwrap();
        for comp in &["nb-99-big-Data.db", "nb-99-big-Index.db"] {
            std::fs::write(dir.join(comp), b"orphan").unwrap();
        }

        // Restart the engine — startup sweeps fire.
        let _engine = WriteEngine::new(config_for(&temp_dir)).unwrap();

        // Both orphan kinds are gone.
        assert!(
            !orphan_tmp.exists(),
            "orphan .compaction-tmp-99 must be swept"
        );
        assert!(
            !dir.join("nb-99-big-Data.db").exists(),
            "partial-rename orphan Data.db must be swept"
        );
        assert!(!dir.join("nb-99-big-Index.db").exists());

        // Live generation is byte-for-byte intact and still readable.
        assert_eq!(
            std::fs::read(&live_data).unwrap(),
            live_bytes_before,
            "live Data.db must be byte-for-byte untouched"
        );
        assert_eq!(
            live_row_count(&live_data, &schema),
            live_rows_before,
            "live generation must still return its rows after the sweep"
        );
    }

    // ── AC #2: never deletes live data (complete gen resembling candidates) ─────

    #[test]
    fn sweep_never_deletes_a_complete_generation() {
        // A COMPLETE published generation whose Data.db has a sibling TOC.txt but
        // omits optional components (no Filter.db / no Summary.db) and uses a
        // high edge generation number — it must survive both sweeps untouched.
        let temp_dir = TempDir::new().unwrap();
        let dir = sstable_dir(&temp_dir);
        std::fs::create_dir_all(&dir).unwrap();

        let complete = [
            "nb-2147483647-big-Data.db",
            "nb-2147483647-big-Index.db",
            "nb-2147483647-big-Statistics.db",
            "nb-2147483647-big-Digest.crc32",
            "nb-2147483647-big-TOC.txt",
        ];
        for name in &complete {
            std::fs::write(dir.join(name), b"complete").unwrap();
        }
        // A non-SSTable file with an SSTable-ish name must also be ignored.
        std::fs::write(dir.join("nb-not-a-generation.txt"), b"noise").unwrap();

        let before = dir_names(&dir);
        let _engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
        let after = dir_names(&dir);

        assert_eq!(
            before, after,
            "a complete generation (TOC.txt present) must be left exactly as-is"
        );
    }

    #[test]
    fn compaction_tmp_sweep_ignores_non_matching_entries() {
        // Files/dirs that merely resemble the tmp orphan name must be left alone;
        // only directories whose name starts with `.compaction-tmp-` are swept.
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // A FILE (not a dir) named like a tmp orphan — must NOT be removed.
        std::fs::write(data_dir.join(".compaction-tmp-7"), b"i am a file").unwrap();
        // A directory with an unrelated name — must NOT be removed.
        std::fs::create_dir_all(data_dir.join("compaction-tmp-real")).unwrap();

        let before = dir_names(&data_dir);
        let outcome = WriteEngine::sweep_orphaned_compaction_tmp(&data_dir);
        let after = dir_names(&data_dir);

        assert!(
            outcome.removed.is_empty(),
            "nothing matched — nothing removed"
        );
        assert!(!outcome.had_failures());
        assert_eq!(before, after, "non-matching entries must be untouched");
    }

    // ── AC #3: sweep failure is non-fatal + surfaced ────────────────────────────

    #[cfg(unix)]
    #[test]
    fn undeletable_orphan_is_non_fatal_and_surfaced() {
        use std::os::unix::fs::PermissionsExt;

        if !readonly_dir_blocks_delete() {
            // Running as root: the permission gate does not apply. Skip rather
            // than falsely pass.
            eprintln!("skipping: cannot exercise permission-denied path (root?)");
            return;
        }

        let temp_dir = TempDir::new().unwrap();
        let dir = sstable_dir(&temp_dir);
        std::fs::create_dir_all(&dir).unwrap();

        // A partial-rename orphan (no TOC.txt) that we then make un-removable by
        // dropping write permission on its parent directory.
        std::fs::write(dir.join("nb-42-big-Data.db"), b"orphan").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        // The sweep must NOT panic/abort and MUST surface the failure.
        let outcome = WriteEngine::sweep_orphaned_partial_sstables(
            temp_dir.path().join("data").as_path(),
            "test_ks",
            "test_table",
        );

        // Restore permissions so the file survives for the assertion and so
        // TempDir cleanup can succeed.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            outcome.had_failures(),
            "an un-removable orphan must be surfaced as a non-fatal failure"
        );
        assert!(
            outcome.failures[0].contains("generation 42"),
            "the surfaced failure must identify the orphan generation, got: {:?}",
            outcome.failures
        );
        // Non-fatal: the file is still present (removal was blocked, not aborted).
        assert!(dir.join("nb-42-big-Data.db").exists());

        // And full engine startup over the same un-removable orphan must still
        // succeed (best-effort sweep never aborts open()).
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let opened = WriteEngine::new(config_for(&temp_dir));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            opened.is_ok(),
            "engine startup must not abort on an un-removable orphan"
        );
    }

    // ── AC #4: idempotence — running the sweep twice is a no-op the 2nd time ────

    #[test]
    fn sweeps_are_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let dir = sstable_dir(&temp_dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Seed both orphan kinds.
        std::fs::create_dir_all(data_dir.join(".compaction-tmp-5")).unwrap();
        std::fs::write(data_dir.join(".compaction-tmp-5").join("p.db"), b"x").unwrap();
        std::fs::write(dir.join("nb-5-big-Data.db"), b"orphan").unwrap();
        std::fs::write(dir.join("nb-5-big-Index.db"), b"orphan").unwrap();

        // First sweep removes the orphans.
        let tmp1 = WriteEngine::sweep_orphaned_compaction_tmp(&data_dir);
        let part1 =
            WriteEngine::sweep_orphaned_partial_sstables(&data_dir, "test_ks", "test_table");
        assert_eq!(
            tmp1.removed.len(),
            1,
            "first tmp sweep removes the orphan dir"
        );
        assert_eq!(
            part1.removed.len(),
            1,
            "first partial sweep removes the orphan"
        );
        assert!(!tmp1.had_failures() && !part1.had_failures());

        let state_after_first = dir_names(&data_dir);

        // Second sweep is a strict no-op: nothing removed, nothing failed, and the
        // on-disk state is unchanged.
        let tmp2 = WriteEngine::sweep_orphaned_compaction_tmp(&data_dir);
        let part2 =
            WriteEngine::sweep_orphaned_partial_sstables(&data_dir, "test_ks", "test_table");
        assert!(tmp2.removed.is_empty(), "second tmp sweep removes nothing");
        assert!(
            part2.removed.is_empty(),
            "second partial sweep removes nothing"
        );
        assert!(!tmp2.had_failures() && !part2.had_failures());
        assert_eq!(
            state_after_first,
            dir_names(&data_dir),
            "the second sweep must not change the directory"
        );
    }

    // ── AC #5: crash-mid-compaction e2e ─────────────────────────────────────────

    #[test]
    fn crash_mid_compaction_then_restart_sweeps_and_recompacts() {
        use crate::storage::write_engine::compaction::FAIL_COMPACTION_BEFORE_RENAME;

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let data_dir = temp_dir.path().join("data");

        // Produce 4 complete, published pre-compaction generations (5 rows each).
        let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
        let pre_paths = flush_n_sstables_sync(&mut engine, 4);
        assert_eq!(pre_paths.len(), 4);

        // Inject a crash after tmp files are written but before the publication
        // rename, then drive a compaction. finalize fails; the tmp dir and the
        // untouched inputs are left on disk exactly as a real crash would.
        FAIL_COMPACTION_BEFORE_RENAME.with(|f| f.set(true));
        let policy = crate::storage::write_engine::STCSPolicy::new(4, 32, 0.5, 1.5, 0).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();
        let crashed = engine.maintenance_step(Duration::from_secs(60));
        FAIL_COMPACTION_BEFORE_RENAME.with(|f| f.set(false));
        assert!(
            crashed.is_err(),
            "the injected pre-rename failure must surface as a compaction error"
        );

        // A tmp dir with real partial output survived the "crash".
        let tmp_dirs: Vec<_> = std::fs::read_dir(&data_dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".compaction-tmp-")
            })
            .collect();
        assert_eq!(
            tmp_dirs.len(),
            1,
            "the crash must leave exactly one .compaction-tmp-* dir"
        );

        // Pre-compaction inputs are intact (renames never happened).
        for p in &pre_paths {
            assert!(p.exists(), "input {:?} must survive a failed compaction", p);
        }

        // "Restart": drop and reopen the engine — startup sweeps fire.
        drop(engine);
        let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();

        let leftover: Vec<_> = std::fs::read_dir(&data_dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".compaction-tmp-")
            })
            .collect();
        assert!(
            leftover.is_empty(),
            "startup sweep must reclaim the orphaned tmp dir, found {:?}",
            leftover.iter().map(|e| e.path()).collect::<Vec<_>>()
        );

        // Pre-compaction generations still serve reads (20 rows total across 4).
        let total_pre: usize = pre_paths.iter().map(|p| live_row_count(p, &schema)).sum();
        assert_eq!(
            total_pre, 20,
            "all 20 pre-compaction rows must still read back"
        );

        // A re-run compaction now succeeds and its output holds every row.
        let policy = crate::storage::write_engine::STCSPolicy::new(4, 32, 0.5, 1.5, 0).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();
        let report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
        assert_eq!(
            report.completed_merges.len(),
            1,
            "the re-run compaction must complete one merge"
        );
        let merged = &report.completed_merges[0];
        assert_eq!(
            live_row_count(merged, &schema),
            20,
            "the recompacted output must contain all 20 rows"
        );
    }

    // ── issue #1959 (rust-reviewer Low 3): publication-barrier fsync faults ──────
    //
    // The directory durability barrier runs at two publication points inside
    // `finalize_merge_async`: step 2b (after the non-TOC renames, before the TOC
    // rename) and step 2c (after the TOC rename, before the inputs are deleted).
    // A real EIO/ENOSPC on either fsync must roll back cleanly. These tests arm
    // the `#[cfg(test)]` fault seam at each point and assert the invariant holds:
    //   (a) the input SSTables stay intact and readable,
    //   (b) no orphan output component and no visible-but-not-durable output TOC
    //       survive (and the `.compaction-tmp-*` dir is dropped), and
    //   (c) finalize returns `Error::Storage`.

    /// Which publication-barrier directory fsync to fault.
    #[derive(Clone, Copy)]
    enum FsyncFaultPoint {
        /// Step 2b: after the non-TOC renames, before the TOC rename.
        BeforeToc,
        /// Step 2c: after the TOC rename, before the inputs are deleted.
        AfterToc,
    }

    fn drive_compaction_with_fsync_fault(point: FsyncFaultPoint) {
        use crate::error::Error;
        use crate::storage::write_engine::compaction::{
            FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC, FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC,
        };

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let dir = sstable_dir(&temp_dir);
        let data_dir = temp_dir.path().join("data");

        // Four complete, published input generations (5 distinct rows each).
        let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
        let pre_paths = flush_n_sstables_sync(&mut engine, 4);
        assert_eq!(pre_paths.len(), 4);

        let count_suffix = |suffix: &str| -> usize {
            dir_names(&dir)
                .iter()
                .filter(|n| n.ends_with(suffix))
                .count()
        };
        assert_eq!(count_suffix("-big-Data.db"), 4, "sanity: 4 input Data.db");
        assert_eq!(count_suffix("-big-TOC.txt"), 4, "sanity: 4 input TOCs");

        // Arm the fsync fault at the requested publication point.
        match point {
            FsyncFaultPoint::BeforeToc => {
                FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC.with(|f| f.set(true))
            }
            FsyncFaultPoint::AfterToc => FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC.with(|f| f.set(true)),
        }
        let policy = crate::storage::write_engine::STCSPolicy::new(4, 32, 0.5, 1.5, 0).unwrap();
        engine.set_merge_policy(Box::new(policy)).unwrap();
        let result = engine.maintenance_step(Duration::from_secs(60));
        // Disarm BOTH flags unconditionally so a failed assertion below cannot
        // leak the injection into another test on this thread.
        FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC.with(|f| f.set(false));
        FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC.with(|f| f.set(false));

        // (c) finalize returns Error::Storage carrying the injected marker.
        let err = result.expect_err("the injected publication fsync fault must fail finalize");
        assert!(
            matches!(&err, Error::Storage(msg) if msg.contains("injected directory fsync fault")),
            "expected Error::Storage from the injected fsync fault, got: {err:?}"
        );

        // (a) The inputs are intact and still readable (never renamed away/deleted).
        for p in &pre_paths {
            assert!(
                p.exists(),
                "input {:?} must survive a rolled-back finalize",
                p
            );
        }
        let total: usize = pre_paths.iter().map(|p| live_row_count(p, &schema)).sum();
        assert_eq!(
            total, 20,
            "all 20 input rows must still read back after the rollback"
        );

        // (b) The rollback removed every renamed file — including the TOC on the
        // 2c path — so only the four input generations remain (no orphan output
        // component, no visible output TOC), and the tmp dir is gone.
        assert_eq!(
            count_suffix("-big-Data.db"),
            4,
            "rollback must leave exactly the 4 input Data.db (no orphan output component)"
        );
        assert_eq!(
            count_suffix("-big-TOC.txt"),
            4,
            "rollback must leave exactly the 4 input TOCs — no visible output TOC may survive"
        );
        let leftover_tmp: Vec<_> = std::fs::read_dir(&data_dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".compaction-tmp-")
            })
            .collect();
        assert!(
            leftover_tmp.is_empty(),
            "rollback must drop the .compaction-tmp-* dir, found {:?}",
            leftover_tmp.iter().map(|e| e.path()).collect::<Vec<_>>()
        );

        drop(engine);
    }

    #[test]
    fn publish_fsync_fault_before_toc_rolls_back_intact() {
        drive_compaction_with_fsync_fault(FsyncFaultPoint::BeforeToc);
    }

    #[test]
    fn publish_fsync_fault_after_toc_rolls_back_intact() {
        drive_compaction_with_fsync_fault(FsyncFaultPoint::AfterToc);
    }
}
