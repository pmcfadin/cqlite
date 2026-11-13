//! Refused-SSTable ledger (issue #4159).
//!
//! The initial load paths open each `Data.db` best-effort: one file that cannot
//! be opened (metadata/header refusal, bad `Statistics.db`, unsupported version,
//! I/O error) must not make every OTHER table in the directory unreadable. But
//! the old code logged the error and dropped it, so a table whose every
//! generation was refused had ZERO readers and `scan` answered `Ok(vec![])` —
//! indistinguishable from an empty table (the #3721 swallow class at SSTable
//! granularity).
//!
//! The manager now RECORDS each refusal here, keyed by the table the file
//! belongs to, and every read entry point consults the ledger before it
//! answers: `Ok(empty)` means "no data", `Err` means "unreadable". Partial
//! refusals (some generations opened, one did not) also error, because a
//! partial answer presented as complete is the same silent data loss.
//!
//! The recorded cause keeps its error KIND (a `Schema` refusal stays `Schema`,
//! #4139) rather than being flattened to text.

use crate::Error;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One refused `Data.db`: where, and the cause (kind-preserving).
#[derive(Debug)]
struct Refusal {
    path: PathBuf,
    cause: Error,
}

/// Table key -> refused generations.
#[derive(Debug, Default)]
pub(crate) struct RefusalLedger {
    by_table: Mutex<HashMap<String, Vec<Refusal>>>,
}

/// Rebuild `e` as an owned error of the SAME kind (`Error` is not `Clone`
/// because it can carry an `io::Error`); kinds that carry no structure fold into
/// `Storage`, which still names the full cause text.
fn clone_kind(e: &Error) -> Error {
    match e {
        Error::Schema(m) => Error::Schema(m.clone()),
        Error::Corruption(m) => Error::Corruption(m.clone()),
        Error::InvalidFormat(m) => Error::InvalidFormat(m.clone()),
        Error::UnsupportedFormat(m) => Error::UnsupportedFormat(m.clone()),
        Error::UnsupportedVersion { version, floor } => Error::UnsupportedVersion {
            version: version.clone(),
            floor: floor.clone(),
        },
        other => Error::Storage(other.to_string()),
    }
}

/// Unqualified table name of a (possibly `keyspace.table`) key.
fn unqualified(name: &str) -> &str {
    name.rfind('.').map_or(name, |dot| &name[dot + 1..])
}

/// Ledger key for a refusal whose table cannot be derived from its path
/// (non-standard or non-UTF-8 directory names). It matches EVERY read: when the
/// affected table is unknowable, no table can be proven unaffected (#4159).
pub(crate) const UNRESOLVED_KEY: &str = "\u{0}unresolved";

/// [`table_dir_table_key`](super::refresh::table_dir_table_key) or [`UNRESOLVED_KEY`].
pub(crate) fn table_dir_key_or_unresolved(path: &Path) -> String {
    super::refresh::table_dir_table_key(path).unwrap_or_else(|| UNRESOLVED_KEY.to_string())
}

/// Ledger key for a refused file whose reader never existed (no header table
/// name): derived ONLY through `base_path_table_key`, the helper (with its base-directory
/// exclusion) the manager keys base-path readers with;
/// anything else is [`UNRESOLVED_KEY`]. An arbitrary parent-directory name is not
/// authoritative for a nonstandard layout, so it is deliberately not a fallback.
pub(crate) fn refusal_key(path: &Path, base_dir_name: &str) -> String {
    super::refresh::base_path_table_key(path, base_dir_name, "")
        .unwrap_or_else(|| UNRESOLVED_KEY.to_string())
}

/// `e` rebuilt as the SAME kind with `what` named in its message, so a failure that returns
/// to a caller says which file or directory it concerns without changing how it matches.
fn name_failure(e: Error, what: &str) -> Error {
    let msg = format!("{what}: {e}");
    match e {
        Error::Schema(_) => Error::Schema(msg),
        Error::Corruption(_) => Error::Corruption(msg),
        Error::InvalidFormat(_) => Error::InvalidFormat(msg),
        Error::UnsupportedFormat(_) => Error::UnsupportedFormat(msg),
        Error::UnsupportedVersion { version, floor } => Error::UnsupportedVersion {
            version: format!("{version} ({what})"),
            floor,
        },
        Error::Io(io) => Error::Io(std::io::Error::new(io.kind(), msg)),
        other => Error::Storage(format!("{what}: {other}")),
    }
}

/// Is this directory entry a GENERATION? A `*-Data.db` name alone is not enough: only a REGULAR
/// file is one (`DirEntry::file_type` does not follow symlinks), so a directory or symlink that
/// merely carries the name is ignored -- not a generation and not a refusal (roborev #3234 r12).
/// If the type cannot be observed the entry stays a candidate, so the failure surfaces as a
/// refusal on open instead of the entry silently vanishing.
pub(crate) async fn is_generation_entry(filename: &str, entry: &tokio::fs::DirEntry) -> bool {
    filename.ends_with("-Data.db")
        && !super::is_apple_double_sidecar(filename)
        && entry.file_type().await.map(|t| t.is_file()).unwrap_or(true)
}

impl RefusalLedger {
    /// Record that `path` (belonging to table `key`) was refused with `cause`.
    pub(crate) fn record(&self, key: String, path: &Path, cause: &Error) {
        tracing::warn!("SSTable {:?} refused ({}): {}", path, key, cause);
        if let Ok(mut map) = self.by_table.lock() {
            let list = map.entry(key).or_default();
            list.retain(|r| r.path != path);
            list.push(Refusal {
                path: path.to_path_buf(),
                cause: clone_kind(cause),
            });
        }
    }

    /// Forget any refusal recorded for `path` (it was re-opened successfully).
    pub(crate) fn clear_path(&self, path: &Path) {
        if let Ok(mut map) = self.by_table.lock() {
            for list in map.values_mut() {
                list.retain(|r| r.path != path);
            }
            map.retain(|_, l| !l.is_empty());
        }
    }

    /// Forget the refusals of every path in `paths` (they opened, and the refresh
    /// that opened them has been committed).
    pub(crate) fn clear_paths(&self, paths: &[PathBuf]) {
        for p in paths {
            self.clear_path(p);
        }
    }

    /// Reconcile after a SUCCESSFUL refresh: keep a refusal only for a `Data.db`
    /// still in the discovered set. Directory-level refusals are all dropped:
    /// discovery fails the refresh when an existing directory cannot be listed, so
    /// reaching the commit means every configured directory was enumerated (or is
    /// gone), and a still-unreadable one is re-recorded by the next load/refresh.
    pub(crate) fn retain_discovered(&self, discovered: &[PathBuf]) {
        if let Ok(mut map) = self.by_table.lock() {
            for list in map.values_mut() {
                list.retain(|r| {
                    r.path.to_string_lossy().ends_with("-Data.db") && discovered.contains(&r.path)
                });
            }
            map.retain(|_, l| !l.is_empty());
        }
    }

    /// `Err` naming the cause when any generation of `table_name` was refused.
    ///
    /// Matching mirrors the reader resolution: exact key first; otherwise the
    /// unqualified names must agree and at most one side may carry a keyspace
    /// (two different keyspaces never bleed into one another).
    pub(crate) fn ensure_readable(&self, table_name: &str) -> crate::Result<()> {
        let Ok(map) = self.by_table.lock() else {
            return Err(Error::Internal(
                "refused-SSTable ledger lock poisoned; cannot prove the table readable".into(),
            ));
        };
        // Deterministic report: exact-key refusals first, then unqualified matches,
        // then UNRESOLVED; within a class, sorted by path. Never HashMap order.
        let mut ranked: Vec<(u8, &Refusal)> = Vec::new();
        for (key, list) in map.iter() {
            let both_qualified = key.contains('.') && table_name.contains('.');
            let class = if key == table_name {
                Some(0)
            } else if key == UNRESOLVED_KEY {
                Some(2)
            } else if !both_qualified && unqualified(key) == unqualified(table_name) {
                Some(1)
            } else {
                None
            };
            if let Some(c) = class {
                ranked.extend(list.iter().map(|r| (c, r)));
            }
        }
        ranked.sort_by(|a, b| (a.0, &a.1.path).cmp(&(b.0, &b.1.path)));
        let hits: Vec<&Refusal> = ranked.into_iter().map(|(_, r)| r).collect();
        let Some(first) = hits.first() else {
            return Ok(());
        };
        // Name EVERY refused file (capped) so no cause is hidden behind another.
        let mut names: Vec<String> = hits
            .iter()
            .take(4)
            .map(|r| r.path.display().to_string())
            .collect();
        if hits.len() > 4 {
            names.push(format!("and {} more", hits.len() - 4));
        }
        let msg = format!(
            "table '{}' is unreadable: SSTable(s) refused: {}; first cause: {}",
            table_name,
            names.join(", "),
            first.cause
        );
        Err(match &first.cause {
            Error::UnsupportedVersion { version, floor } => Error::UnsupportedVersion {
                version: format!("{version} ({msg})"),
                floor: floor.clone(),
            },
            Error::Schema(_) => Error::Schema(msg),
            Error::Corruption(_) => Error::Corruption(msg),
            Error::InvalidFormat(_) => Error::InvalidFormat(msg),
            Error::UnsupportedFormat(_) => Error::UnsupportedFormat(msg),
            _ => Error::Storage(msg),
        })
    }
}

impl super::SSTableManager {
    /// Reader snapshot for a read that returns rows: `Err` naming the cause when
    /// any SSTable of the table was refused, else
    /// [`resolve_reader_snapshot`](Self::resolve_reader_snapshot). Probes and shape
    /// hints keep the unchecked form (they are fail-safe: the real read errs).
    pub(crate) async fn checked_snapshot(
        &self,
        table_id: &crate::types::TableId,
    ) -> crate::Result<(Vec<std::sync::Arc<super::reader::SSTableReader>>, bool)> {
        self.refusals.ensure_readable(table_id.name())?;
        #[cfg(test)]
        race_hook::pause_if_probe(table_id.name()).await;
        let snapshot = self.resolve_reader_snapshot(table_id).await;
        // The consistency point: a refusal recorded (by a concurrent refresh) while
        // the snapshot was being taken is visible HERE, so a result is never
        // returned after the refusal it should have reported.
        self.refusals.ensure_readable(table_id.name())?;
        Ok(snapshot)
    }

    /// [`checked_snapshot`](Self::checked_snapshot) for the streaming scans.
    #[cfg(not(feature = "tombstones"))]
    pub(super) async fn checked_table_readers(
        &self,
        table_id: &crate::types::TableId,
    ) -> crate::Result<Vec<std::sync::Arc<super::reader::SSTableReader>>> {
        self.refusals.ensure_readable(table_id.name())?;
        #[cfg(test)]
        race_hook::pause_if_probe(table_id.name()).await;
        let readers = self.resolve_table_readers(table_id).await;
        self.refusals.ensure_readable(table_id.name())?; // consistency point, as above
        Ok(readers)
    }

    /// Record that an EXISTING table directory could not be listed, and hand the
    /// error back for the caller to return (refresh) or drop (initial load).
    pub(super) fn refuse_dir(&self, dir: &Path, e: crate::Error) -> crate::Error {
        let probe = dir.join("unreadable-Data.db");
        self.refusals
            .record(table_dir_key_or_unresolved(&probe), dir, &e);
        e
    }

    /// List the `Data.db` files of one existing table directory for a REFRESH. EVERY
    /// failure -- opening the listing or advancing it -- is returned (naming the
    /// directory), so a partial enumeration can never be mistaken for a complete one.
    /// A failed refresh changes NOTHING (storage-freshness spec): neither the reader
    /// set nor the refusal ledger; its `Err` is the loud signal.
    pub(super) async fn list_data_files(&self, dir: &Path) -> crate::Result<Vec<PathBuf>> {
        let mut entries = match self.platform.fs().read_dir(dir).await {
            Ok(entries) => entries,
            Err(e) => return Err(name_failure(e, &format!("directory {}", dir.display()))),
        };
        let mut out = Vec::new();
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    return Err(name_failure(
                        e.into(),
                        &format!("directory {}", dir.display()),
                    ))
                }
            };
            let path = entry.path();
            let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if is_generation_entry(fname, &entry).await {
                out.push(path);
            }
        }
        Ok(out)
    }

    /// Hint-only form: a refused table yields no readers.
    #[cfg(not(feature = "tombstones"))]
    pub(super) async fn readers_if_readable(
        &self,
        table_id: &crate::types::TableId,
    ) -> Vec<std::sync::Arc<super::reader::SSTableReader>> {
        self.checked_table_readers(table_id)
            .await
            .unwrap_or_default()
    }

    /// Open a refresh candidate. A failure is returned NAMING the candidate (same error
    /// kind) and records nothing: a failed refresh changes neither the reader set nor
    /// the refusal ledger (storage-freshness spec, "Corrupt new generation rejects the
    /// whole refresh"). A success does not clear anything here either: stale refusals
    /// are cleared together with the reader-set swap, by
    /// [`commit_refusals`](Self::commit_refusals).
    pub(super) async fn open_candidate(
        &self,
        path: &Path,
    ) -> crate::Result<std::sync::Arc<super::reader::SSTableReader>> {
        self.open_reader_with_schema(path)
            .await
            .map_err(|e| name_failure(e, &format!("refresh candidate {}", path.display())))
    }

    /// Commit a SUCCESSFUL refresh's ledger changes with the reader-set swap:
    /// the candidates that opened lose their refusals, and refusals of files no
    /// longer on disk are reconciled away.
    pub(super) fn commit_refusals(&self, opened: &[PathBuf], discovered: &[PathBuf]) {
        self.refusals.clear_paths(opened);
        self.refusals.retain_discovered(discovered);
    }
}

/// Test-only deterministic pause between the first ledger check and the reader
/// snapshot (no sleeps): active only for the reserved probe table name.
#[cfg(test)]
pub(crate) mod race_hook {
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    pub(crate) const PROBE: &str = "__race_probe__";
    type Gate = (Arc<Notify>, Arc<Notify>); // (reached, release)
    static GATE: Mutex<Option<Gate>> = Mutex::new(None);

    pub(crate) fn arm() -> Gate {
        let gate = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        if let Ok(mut g) = GATE.lock() {
            *g = Some(gate.clone());
        }
        gate
    }

    pub(crate) async fn pause_if_probe(name: &str) {
        if name != PROBE {
            return;
        }
        let gate = GATE.lock().ok().and_then(|g| g.clone());
        if let Some((reached, release)) = gate {
            reached.notify_one();
            release.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refusal recorded between the ledger check and the snapshot (a concurrent
    /// refresh) must turn the read into an `Err`, never an empty/partial `Ok`.
    #[tokio::test]
    async fn refusal_recorded_during_snapshot_fails_the_read() {
        let tmp = tempfile::tempdir().expect("tmp");
        let config = crate::Config::default();
        let platform = std::sync::Arc::new(crate::Platform::new(&config).await.expect("platform"));
        let manager = std::sync::Arc::new(
            super::super::SSTableManager::new(
                tmp.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .expect("manager"),
        );
        let (reached, release) = race_hook::arm();
        let reader = {
            let m = manager.clone();
            tokio::spawn(async move {
                m.checked_snapshot(&crate::types::TableId::from(race_hook::PROBE))
                    .await
                    .map(|_| ())
            })
        };
        reached.notified().await; // the read has passed its first ledger check
        manager.refusals.record(
            race_hook::PROBE.to_string(),
            Path::new("/d/ks/probe-1/nb-1-big-Data.db"),
            &Error::Corruption("refused by a concurrent refresh".into()),
        );
        release.notify_one();
        let result = reader.await.expect("join");
        assert!(
            result.is_err(),
            "the read must observe the refusal recorded mid-read: {result:?}"
        );
    }

    #[test]
    fn coexisting_refusal_classes_report_deterministically() {
        // Exact key (Schema) outranks unqualified (Corruption) outranks UNRESOLVED
        // (InvalidFormat), whatever the insertion order.
        let rec = |l: &RefusalLedger, k: &str, p: &str, e: Error| {
            l.record(k.to_string(), Path::new(p), &e)
        };
        for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0], [1, 0, 2]] {
            let ledger = RefusalLedger::default();
            for i in order {
                match i {
                    0 => rec(
                        &ledger,
                        "ks.t",
                        "/d/ks/t-1/nb-1-big-Data.db",
                        Error::Schema("a".into()),
                    ),
                    1 => rec(
                        &ledger,
                        "t",
                        "/d/other/t-2/nb-2-big-Data.db",
                        Error::Corruption("b".into()),
                    ),
                    _ => rec(
                        &ledger,
                        UNRESOLVED_KEY,
                        "/d/\u{fffd}/nb-3-big-Data.db",
                        Error::InvalidFormat("c".into()),
                    ),
                }
            }
            match ledger.ensure_readable("ks.t") {
                Err(Error::Schema(m)) => assert!(
                    m.find("t-1").unwrap_or(usize::MAX) < m.find("t-2").unwrap_or(0),
                    "{m}"
                ),
                other => panic!("expected the exact-key Schema refusal first, got {other:?}"),
            }
        }
    }

    #[test]
    fn unsupported_version_keeps_its_kind_and_names_the_file() {
        let ledger = RefusalLedger::default();
        ledger.record(
            "ks.t".to_string(),
            Path::new("/d/ks/t-1/nb-1-big-Data.db"),
            &Error::UnsupportedVersion {
                version: "ma".into(),
                floor: "na".into(),
            },
        );
        match ledger.ensure_readable("ks.t") {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert!(
                    version.starts_with("ma") && version.contains("nb-1-big-Data.db"),
                    "{version}"
                );
                assert_eq!(floor, "na");
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }
}
