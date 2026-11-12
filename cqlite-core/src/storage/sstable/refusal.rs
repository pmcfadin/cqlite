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
#[derive(Debug, Clone)]
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
pub(crate) fn refusal_key(path: &Path, base_dir_name: &str) -> String {
    super::refresh::base_path_table_key(path, base_dir_name, "")
        .or_else(|| super::refresh::table_dir_table_key(path))
        .or_else(|| {
            path.parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .map(str::to_string)
        })
        .unwrap_or_default()
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
                || (!both_qualified && unqualified(key) == unqualified(table_name));
            if matches {
                hits.extend(list.iter());
            }
        }
        let Some(first) = hits.first() else {
            return Ok(());
        };
        let more = if hits.len() > 1 {
            format!(" (and {} more refused SSTable(s))", hits.len() - 1)
        } else {
            String::new()
        };
        let msg = format!(
            "table '{}' is unreadable: SSTable {} was refused{}: {}",
            table_name,
            first.path.display(),
            more,
            first.cause
        );
        Err(match &first.cause {
            Error::Schema(_) => Error::Schema(msg),
            Error::Corruption(_) => Error::Corruption(msg),
            Error::InvalidFormat(_) => Error::InvalidFormat(msg),
            Error::UnsupportedFormat(_) => Error::UnsupportedFormat(msg),
            _ => Error::Storage(msg),
        })
    }
}
