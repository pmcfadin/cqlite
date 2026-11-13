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

/// Ledger key for a refused file whose reader never existed, so its header
/// table name is unavailable: path-derived, never "no key" (a refusal must not
/// be droppable for lack of a name).
/// Ledger key for a refusal whose table cannot be derived from its path
/// (non-standard or non-UTF-8 directory names). It matches EVERY read: when the
/// affected table is unknowable, no table can be proven unaffected (#4159).
pub(crate) const UNRESOLVED_KEY: &str = "\u{0}unresolved";

/// [`table_dir_table_key`](super::refresh::table_dir_table_key) or [`UNRESOLVED_KEY`].
pub(crate) fn table_dir_key_or_unresolved(path: &Path) -> String {
    super::refresh::table_dir_table_key(path).unwrap_or_else(|| UNRESOLVED_KEY.to_string())
}

pub(crate) fn refusal_key(path: &Path, base_dir_name: &str) -> String {
    super::refresh::base_path_table_key(path, base_dir_name, "")
        .or_else(|| super::refresh::table_dir_table_key(path))
        .or_else(|| {
            path.parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| UNRESOLVED_KEY.to_string())
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
        let mut hits: Vec<&Refusal> = Vec::new();
        for (key, list) in map.iter() {
            let both_qualified = key.contains('.') && table_name.contains('.');
            let matches = key == table_name
                || key == UNRESOLVED_KEY
                || (!both_qualified && unqualified(key) == unqualified(table_name));
            if matches {
                hits.extend(list.iter());
            }
        }
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
        Ok(self.resolve_reader_snapshot(table_id).await)
    }

    /// [`checked_snapshot`](Self::checked_snapshot) for the streaming scans.
    #[cfg(not(feature = "tombstones"))]
    pub(super) async fn checked_table_readers(
        &self,
        table_id: &crate::types::TableId,
    ) -> crate::Result<Vec<std::sync::Arc<super::reader::SSTableReader>>> {
        self.refusals.ensure_readable(table_id.name())?;
        Ok(self.resolve_table_readers(table_id).await)
    }

    /// Record that an EXISTING table directory could not be listed, and hand the
    /// error back for the caller to return (refresh) or drop (initial load).
    pub(super) fn refuse_dir(&self, dir: &Path, e: crate::Error) -> crate::Error {
        let probe = dir.join("unreadable-Data.db");
        self.refusals
            .record(table_dir_key_or_unresolved(&probe), dir, &e);
        e
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

    /// Open a refresh candidate. A failure is RECORDED before it is returned, so
    /// an aborted refresh can never leave the failing file unnamed. A success does
    /// NOT clear anything here: stale refusals are cleared together with the reader
    /// set swap, by [`commit_refusals`](Self::commit_refusals).
    pub(super) async fn open_candidate_recording(
        &self,
        path: &Path,
    ) -> crate::Result<std::sync::Arc<super::reader::SSTableReader>> {
        match self.open_reader_with_schema(path).await {
            Ok(r) => Ok(r),
            Err(e) => {
                let key = match &self.discovery_source {
                    super::refresh::DiscoverySource::BasePath => {
                        let base = self.base_path.file_name().and_then(|n| n.to_str());
                        refusal_key(path, base.unwrap_or(""))
                    }
                    _ => table_dir_key_or_unresolved(path),
                };
                self.refusals.record(key, path, &e);
                Err(e)
            }
        }
    }

    /// Commit a SUCCESSFUL refresh's ledger changes with the reader-set swap:
    /// the candidates that opened lose their refusals, and refusals of files no
    /// longer on disk are reconciled away.
    pub(super) fn commit_refusals(&self, opened: &[PathBuf], discovered: &[PathBuf]) {
        self.refusals.clear_paths(opened);
        self.refusals.retain_discovered(discovered);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
