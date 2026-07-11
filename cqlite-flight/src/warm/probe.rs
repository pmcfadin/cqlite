//! Per-request generation-set staleness probe (issue #2310, WS2 #2341).
//!
//! Decision 2 (design.md) + spec Requirements 2 & 3. On EVERY request the warm
//! handle probes the current SSTable generation set for the resolved table and
//! compares it to the cached set:
//!
//! * **Authoritative backbone (2a):** a directory listing of `*-Data.db` files,
//!   each resolved to its inode-stable [`GenerationId`]. A listing IS ground
//!   truth for "what generations exist right now" — ZERO staleness window, and
//!   NO heuristic (it reads the truth, it does not infer it from mtime/timing;
//!   no-heuristics mandate #28). A flush/compaction is visible on the very next
//!   request.
//! * **Snapshot manifest fast path (2b):** in snapshot mode the snapshot carries
//!   a `manifest.json`; when it is byte-identical to the cached one, skip the
//!   `read_dir` and take the warm hit. It is an OPTIMIZATION ONLY — equivalent to
//!   the authoritative probe, never a weaker guarantee. Absent/unparsable
//!   manifest → fall back to the authoritative listing (never a stale hit).
//!
//! Fail-closed: any probe error — the dir unreadable, a per-entry iteration or
//! `stat` failure, or a `Data.db` whose resolved path escapes the table dir
//! (issue #1430 containment) — is surfaced so the caller treats it as "changed /
//! re-resolve failed" rather than serving a stale warm hit or a silently smaller
//! generation set that could mask a live generation.

use std::path::{Path, PathBuf};

use crate::cancel::CancelFlag;

use super::identity::{GenerationId, GenerationSet};
use super::WarmError;

/// One enumerated generation: its inode-stable identity and the `Data.db` path a
/// rebuild opens it from.
#[derive(Debug, Clone)]
pub struct GenerationEntry {
    /// Inode-stable identity (the cache-key member).
    pub id: GenerationId,
    /// The `Data.db` path to open this generation from (in the resolved dir).
    pub path: PathBuf,
}

/// The result of a staleness probe.
#[derive(Debug)]
pub enum ProbeOutcome {
    /// The snapshot `manifest.json` was byte-identical to the cached one: the
    /// cached generation set is authoritative, and NO `read_dir` was performed
    /// (spec Requirement 3 fast path).
    UnchangedByManifest,
    /// An authoritative directory listing was performed. Carries the current
    /// generation entries and (in snapshot mode) the manifest bytes to cache for
    /// the next fast-path comparison.
    Enumerated {
        /// The current on-disk generations (id + path), newest resolution order
        /// unspecified — the registry sorts.
        entries: Vec<GenerationEntry>,
        /// Snapshot `manifest.json` bytes, when present, to cache for the next
        /// request's fast-path comparison. `None` in live mode or when absent.
        manifest: Option<Vec<u8>>,
        /// Whether a `read_dir` was actually performed (always `true` here; the
        /// field lets tests/benches assert the fast path elided it).
        read_dir_performed: bool,
    },
}

impl ProbeOutcome {
    /// The generation set of an `Enumerated` outcome (empty for a manifest hit,
    /// which by definition kept the cached set).
    pub fn set(entries: &[GenerationEntry]) -> GenerationSet {
        GenerationSet::from_ids(entries.iter().map(|e| e.id).collect())
    }
}

/// The snapshot manifest file Cassandra writes into a `snapshots/<name>/` dir.
const MANIFEST_FILE: &str = "manifest.json";

/// Read the snapshot `manifest.json` bytes from `dir`, when present. Used by the
/// registry's manifest-hit-race fallback to re-cache the manifest after an
/// authoritative re-enumeration.
pub(super) fn read_manifest(dir: &Path) -> Option<Vec<u8>> {
    std::fs::read(dir.join(MANIFEST_FILE)).ok()
}

/// Probe the current generation set for `dir`.
///
/// `snapshot_mode` enables the `manifest.json` fast path (2b). `cached_manifest`
/// is the manifest bytes cached from the previous probe of the SAME warm entry
/// (used only to decide the fast path). `cancel` is polled first so a
/// pre-cancelled request does ZERO probe work (spec Requirement 7).
pub fn probe_generation_set(
    dir: &Path,
    snapshot_mode: bool,
    cached_manifest: Option<&[u8]>,
    cancel: &CancelFlag,
) -> Result<ProbeOutcome, WarmError> {
    // Cancellation (issue #2264/#1473): a pre-cancelled request does zero work.
    if cancel.is_cancelled() {
        return Err(WarmError::Cancelled);
    }

    // Fast path (2b): snapshot mode + a cached manifest that matches the on-disk
    // one → the generation set is unchanged, skip the read_dir entirely. Any read
    // failure or mismatch falls through to the authoritative listing below; the
    // fast path is an optimization, never the correctness backbone.
    if snapshot_mode {
        if let Some(cached) = cached_manifest {
            if let Ok(current) = std::fs::read(dir.join(MANIFEST_FILE)) {
                if current == cached {
                    return Ok(ProbeOutcome::UnchangedByManifest);
                }
            }
        }
    }

    // Cancellation boundary before the (potentially I/O-heavy) listing.
    if cancel.is_cancelled() {
        return Err(WarmError::Cancelled);
    }

    let entries = enumerate_generations(dir)?;
    // In snapshot mode, cache the manifest bytes (if any) for the next fast path.
    let manifest = if snapshot_mode {
        std::fs::read(dir.join(MANIFEST_FILE)).ok()
    } else {
        None
    };
    Ok(ProbeOutcome::Enumerated {
        entries,
        manifest,
        read_dir_performed: true,
    })
}

/// Enumerate the inode-stable generations (`*-Data.db` files) directly under
/// `dir`, fail-closed on the completeness posture of the cold path.
///
/// * A `read_dir` failure — or a per-entry iteration failure — surfaces as
///   [`WarmError::Probe`] (never a silently smaller set, #2310).
/// * Per-file containment (issue #1430) mirrors the cold-path
///   `DirSource::data_paths` filter: a SYMLINK inside an otherwise-valid dir can
///   resolve to a `Data.db` OUTSIDE it. Here it is fail-closed as
///   [`WarmError::ProbeEntry`] (treated as changed) rather than silently
///   excluded, so a poisoned entry can never produce a stale warm hit.
/// * A `*-Data.db` we can list but cannot `stat` is likewise fail-closed as
///   [`WarmError::ProbeEntry`] — a FAILED stat is treated as changed, distinct
///   from a genuinely-not-an-SSTable filename (a benign, non-error skip).
pub(super) fn enumerate_generations(dir: &Path) -> Result<Vec<GenerationEntry>, WarmError> {
    let read = std::fs::read_dir(dir).map_err(|source| WarmError::Probe {
        path: dir.to_path_buf(),
        source,
    })?;
    let mut entries = Vec::new();
    for entry in read {
        // A per-entry iteration failure is fail-closed (#2310): surface it as a
        // probe error → treated as changed, never a silently smaller set.
        let entry = entry.map_err(|source| WarmError::Probe {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let is_data = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with("-Data.db"));
        if !is_data {
            // A genuinely-not-an-SSTable filename is a non-error skip (it is not a
            // member of the generation set), distinct from a FAILED stat below.
            continue;
        }
        // Per-file containment (issue #1430), mirroring the cold path but
        // fail-closed: an escaping entry aborts the probe (treated as changed).
        if let Err(reason) = crate::pathsafe::assert_within("sstable", dir, &path) {
            return Err(WarmError::ProbeEntry {
                path,
                reason: reason.to_string(),
            });
        }
        match GenerationId::resolve(&path) {
            Some(id) => entries.push(GenerationEntry { id, path }),
            None => {
                // A `*-Data.db` we can list but cannot `stat`: fail closed
                // (treated as changed) rather than a silently smaller set (#2310).
                let reason = std::fs::metadata(&path)
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Data.db entry could not be resolved".to_string());
                return Err(WarmError::ProbeEntry { path, reason });
            }
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_data(dir: &Path, name: &str) {
        std::fs::write(dir.join(name), b"x").unwrap();
    }

    #[test]
    fn enumerates_data_files_only() {
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        write_data(dir.path(), "nb-2-big-Data.db");
        write_data(dir.path(), "nb-1-big-Index.db"); // ignored
        let entries = enumerate_generations(dir.path()).unwrap();
        assert_eq!(entries.len(), 2, "only Data.db files count as generations");
    }

    #[test]
    #[cfg(unix)]
    fn symlink_escaping_data_db_is_probe_error_not_enumerated() {
        // Finding 1 (#2310): a symlink inside the table dir whose name is a
        // `*-Data.db` but whose target escapes the dir must FAIL the probe
        // (fail-closed containment, issue #1430), never be enumerated as a
        // generation. Red on pre-fix code: the escapee is silently enumerated.
        use std::os::unix::fs::symlink;
        let table = tempfile::TempDir::new().unwrap();
        let outside = tempfile::TempDir::new().unwrap();
        let escapee = outside.path().join("nb-9-big-Data.db");
        std::fs::write(&escapee, b"x").unwrap();
        symlink(&escapee, table.path().join("nb-9-big-Data.db")).unwrap();

        let err = enumerate_generations(table.path())
            .expect_err("an escaping symlink Data.db must fail the probe, not enumerate");
        assert!(matches!(err, WarmError::ProbeEntry { .. }), "got {err:?}");
    }

    #[test]
    #[cfg(unix)]
    fn unstatable_data_db_entry_is_probe_error_not_silent_skip() {
        // Finding 3 (#2310): a `*-Data.db` we can list but cannot `stat` (a
        // dangling symlink) must fail closed (treated as changed), NOT be
        // silently dropped into a smaller set. Red on pre-fix code: the entry is
        // silently skipped and the probe returns Ok with a smaller set.
        use std::os::unix::fs::symlink;
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        symlink(
            dir.path().join("missing-target"),
            dir.path().join("nb-2-big-Data.db"),
        )
        .unwrap();

        let err = enumerate_generations(dir.path())
            .expect_err("an unstatable Data.db must fail the probe, not be skipped");
        assert!(matches!(err, WarmError::ProbeEntry { .. }), "got {err:?}");
    }

    #[test]
    fn non_sstable_filename_is_a_non_error_skip() {
        // The fail-closed posture must NOT turn a benign non-SSTable filename
        // into an error: only Data.db entries participate in the set.
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        write_data(dir.path(), "nb-1-big-Index.db"); // not a Data.db
        write_data(dir.path(), "manifest.json"); // not a Data.db
        let entries = enumerate_generations(dir.path()).unwrap();
        assert_eq!(
            entries.len(),
            1,
            "only the one Data.db counts; others skipped"
        );
    }

    #[test]
    fn missing_dir_is_probe_error_not_stale_hit() {
        let err = enumerate_generations(Path::new("/nonexistent/warm/dir")).unwrap_err();
        assert!(matches!(err, WarmError::Probe { .. }));
    }

    #[test]
    fn pre_cancelled_probe_does_zero_work() {
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        let cancel = CancelFlag::new();
        cancel.cancel();
        let err = probe_generation_set(dir.path(), false, None, &cancel).unwrap_err();
        assert!(matches!(err, WarmError::Cancelled));
    }

    #[test]
    fn matching_manifest_takes_fast_path_without_readdir() {
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        let manifest = br#"{"files":["nb-1-big-Data.db"]}"#.to_vec();
        std::fs::write(dir.path().join(MANIFEST_FILE), &manifest).unwrap();
        let outcome =
            probe_generation_set(dir.path(), true, Some(&manifest), &CancelFlag::new()).unwrap();
        assert!(
            matches!(outcome, ProbeOutcome::UnchangedByManifest),
            "byte-identical manifest must take the fast path"
        );
    }

    #[test]
    fn absent_manifest_falls_back_to_authoritative_listing() {
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        // Snapshot mode, cached manifest present, but NO manifest on disk → the
        // fast path cannot fire; fall back to the authoritative listing.
        let outcome =
            probe_generation_set(dir.path(), true, Some(b"cached"), &CancelFlag::new()).unwrap();
        match outcome {
            ProbeOutcome::Enumerated {
                read_dir_performed, ..
            } => assert!(read_dir_performed, "must fall back to the readdir"),
            ProbeOutcome::UnchangedByManifest => panic!("no on-disk manifest → no fast path"),
        }
    }

    #[test]
    fn changed_manifest_re_enumerates() {
        let dir = tempfile::TempDir::new().unwrap();
        write_data(dir.path(), "nb-1-big-Data.db");
        std::fs::write(dir.path().join(MANIFEST_FILE), b"new").unwrap();
        let outcome =
            probe_generation_set(dir.path(), true, Some(b"old"), &CancelFlag::new()).unwrap();
        assert!(
            matches!(outcome, ProbeOutcome::Enumerated { .. }),
            "a differing manifest must re-enumerate, never a stale hit"
        );
    }
}
