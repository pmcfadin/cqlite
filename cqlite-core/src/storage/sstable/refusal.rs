//! The per-table SSTable REFUSAL ledger, and the guard every read consults
//! (issue #4159).
//!
//! # The defect this module exists to remove
//!
//! Both [`SSTableManager`] constructors load the discovered `*-Data.db`
//! generations **best-effort**: a generation whose
//! [`SSTableReader::open`](crate::storage::sstable::reader::SSTableReader::open)
//! fails was logged at `warn!` and then simply not inserted into the reader map.
//! `SSTableManager::new` therefore returned `Ok`, the table's reader list came out
//! EMPTY, and the read surfaces' `reader_list.is_empty()` guard answered
//! `Ok(Vec::new())`. A scan over an SSTable that could not be read returned a
//! successful EMPTY result, indistinguishable at the public API from a table that
//! genuinely holds no rows — the issue #3721 swallow class at SSTable granularity,
//! and the failure mode nothing downstream can detect.
//!
//! # The shape of the fix, and why it is not "propagate from the constructor"
//!
//! Making the constructor propagate would let ONE corrupt file render every OTHER
//! table under the same base path unreadable, which is not what a per-file
//! best-effort load is for. So the refusal is **recorded** — path plus the
//! original [`Error`], never a rendered string — under the same table key the
//! successful open would have used, and every read of that table then FAILS CLOSED
//! with [`Error::UnreadableSSTable`] naming the cause.
//!
//! **A PARTIAL answer is still a refusal.** When some of a table's generations
//! opened and one did not, the read still returns `Err`: a partial result
//! presented as complete is the same silent-data-loss defect, one degree weaker.
//!
//! # Unattributed refusals
//!
//! A refused open yields no header, so the table key is derived from the PATH
//! alone. When even that fails, the refusal is recorded under
//! [`UNATTRIBUTED_TABLE_KEY`] and is consulted by EVERY table's read: "this file
//! could not be read and we cannot even say which table it belonged to" means no
//! table's answer is knowably complete. Fail-closed is the only honest direction
//! there — the alternative silently drops the refusal on the floor, which is the
//! defect.
//!
//! [`SSTableManager`]: crate::storage::sstable::SSTableManager

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{Error, Result};

/// Ledger key for a refusal whose owning table could NOT be derived from its path.
///
/// A NUL byte prefix makes it unrepresentable as a `keyspace.table` key, so it can
/// never collide with a real table (and a query for it can never be spelled).
pub(crate) const UNATTRIBUTED_TABLE_KEY: &str = "\u{0}unattributed";

/// One SSTable generation whose open REFUSED, with the refusal itself.
///
/// The [`Error`] is retained behind an [`Arc`] rather than rendered to a message:
/// a caller matching [`Error::UnreadableSSTable`] can walk its `source` for the
/// authoritative cause, and re-synthesising the text would discard exactly the
/// information the operator needs.
#[derive(Debug, Clone)]
pub(crate) struct RefusedSSTable {
    path: PathBuf,
    cause: Arc<Error>,
}

impl RefusedSSTable {
    pub(crate) fn new(path: PathBuf, cause: Error) -> Self {
        Self {
            path,
            cause: Arc::new(cause),
        }
    }

    /// The refused `Data.db` path.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// A shared handle on the ORIGINAL refusal.
    ///
    /// Reference-counted, never re-rendered: `Error` is not `Clone`, and a ledger
    /// that had to hand out owned errors would end up re-wrapping each report's
    /// MESSAGE and discarding the cause — the exact mistake #3814 left behind at
    /// `big_promoted.rs:108`. [`Error::UnreadableSSTable`] therefore stores an
    /// `Arc<Error>` as its `#[source]`.
    pub(crate) fn cause(&self) -> Arc<Error> {
        Arc::clone(&self.cause)
    }
}

/// Per-table-key refusals. The key is computed by the SAME helper the successful
/// open uses (`refresh::base_path_table_key` / `refresh::table_dir_table_key`) so
/// the ledger and the reader map can never disagree about a table's identity.
pub(crate) type RefusalLedger = HashMap<String, Vec<RefusedSSTable>>;

/// Every refusal that bears on a read of `table_name`: the exact key, the
/// unqualified fallback (mirroring
/// [`SSTableManager::resolve_reader_list`](crate::storage::sstable::SSTableManager::resolve_reader_list)),
/// and every unattributed refusal.
///
/// Returned as a flat borrow list so the caller can report a count AND the first
/// refusal without a second lookup.
fn bearing_on<'a>(ledger: &'a RefusalLedger, table_name: &str) -> Vec<&'a RefusedSSTable> {
    let mut out: Vec<&RefusedSSTable> = Vec::new();
    if let Some(list) = ledger.get(table_name) {
        out.extend(list.iter());
    }
    let unqualified = table_name
        .rfind('.')
        .map_or(table_name, |dot| &table_name[dot + 1..]);
    if unqualified != table_name {
        if let Some(list) = ledger.get(unqualified) {
            out.extend(list.iter());
        }
    }
    if let Some(list) = ledger.get(UNATTRIBUTED_TABLE_KEY) {
        out.extend(list.iter());
    }
    out
}

/// `Ok(())` when no refusal bears on a read of `table_name`; otherwise
/// [`Error::UnreadableSSTable`] naming the count and the first refusal's path and
/// cause.
///
/// Pure over the ledger so it is unit-testable without a filesystem, a platform
/// or an async runtime.
pub(crate) fn check(ledger: &RefusalLedger, table_name: &str) -> Result<()> {
    if ledger.is_empty() {
        return Ok(());
    }
    let bearing = bearing_on(ledger, table_name);
    let Some(first) = bearing.first() else {
        return Ok(());
    };
    Err(Error::unreadable_sstable(
        table_name,
        first.path().to_path_buf(),
        bearing.len(),
        first.cause(),
    ))
}

/// Record `cause` for `path` under `key`.
pub(crate) fn record(ledger: &mut RefusalLedger, key: String, path: PathBuf, cause: Error) {
    ledger
        .entry(key)
        .or_default()
        .push(RefusedSSTable::new(path, cause));
}

/// Drop every recorded refusal whose path is no longer among the DISCOVERED
/// generations, and drop keys left empty.
///
/// Called from `refresh_tables` under the same write guard that applies the
/// reader diff: once a refused file is gone from disk it must stop poisoning its
/// table, or a table stays permanently unreadable after the operator removed the
/// bad generation. `still_present` is asked in CANONICAL form by the caller, which
/// owns the canonicalization cache.
pub(crate) fn retain_present(ledger: &mut RefusalLedger, still_present: impl Fn(&Path) -> bool) {
    for list in ledger.values_mut() {
        list.retain(|r| still_present(r.path()));
    }
    ledger.retain(|_key, list| !list.is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger_with(key: &str, path: &str) -> RefusalLedger {
        let mut l = RefusalLedger::new();
        record(
            &mut l,
            key.to_string(),
            PathBuf::from(path),
            Error::corruption("staged refusal"),
        );
        l
    }

    #[test]
    fn empty_ledger_permits_every_table() {
        let l = RefusalLedger::new();
        assert!(check(&l, "ks.t").is_ok());
        assert!(check(&l, "t").is_ok());
    }

    #[test]
    fn exact_key_refuses_and_names_the_cause() {
        let l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        let e = check(&l, "ks.t").expect_err("a recorded refusal must fail the read");
        match &e {
            Error::UnreadableSSTable {
                table,
                path,
                refused,
                source,
            } => {
                assert_eq!(table, "ks.t");
                assert_eq!(path, &PathBuf::from("/d/ks/t-1/nb-1-big-Data.db"));
                assert_eq!(*refused, 1);
                assert!(
                    source.to_string().contains("staged refusal"),
                    "the ORIGINAL cause must be carried, got {source}"
                );
            }
            other => panic!("expected UnreadableSSTable, got {other:?}"),
        }
    }

    #[test]
    fn unqualified_fallback_mirrors_reader_resolution() {
        let l = ledger_with("t", "/d/t-1/nb-1-big-Data.db");
        assert!(
            check(&l, "ks.t").is_err(),
            "a fully-qualified query resolves readers via the bare name, so it must \
             also see that key's refusals"
        );
    }

    #[test]
    fn an_unrelated_table_is_unaffected() {
        let l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        assert!(
            check(&l, "ks.other").is_ok(),
            "one corrupt file must not render an unrelated table unreadable"
        );
    }

    #[test]
    fn unattributed_refusal_bears_on_every_table() {
        let l = ledger_with(UNATTRIBUTED_TABLE_KEY, "/d/stray-Data.db");
        assert!(check(&l, "ks.t").is_err());
        assert!(check(&l, "other").is_err());
    }

    #[test]
    fn count_spans_every_bearing_refusal() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        record(
            &mut l,
            "ks.t".to_string(),
            PathBuf::from("/d/ks/t-1/nb-2-big-Data.db"),
            Error::corruption("second"),
        );
        record(
            &mut l,
            UNATTRIBUTED_TABLE_KEY.to_string(),
            PathBuf::from("/d/stray-Data.db"),
            Error::corruption("third"),
        );
        match check(&l, "ks.t") {
            Err(Error::UnreadableSSTable { refused, .. }) => assert_eq!(refused, 3),
            other => panic!("expected 3 bearing refusals, got {other:?}"),
        }
    }

    #[test]
    fn retain_present_clears_a_removed_generation() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        retain_present(&mut l, |_p| false);
        assert!(l.is_empty(), "a refused file that is gone must stop refusing");
        assert!(check(&l, "ks.t").is_ok());
    }

    #[test]
    fn retain_present_keeps_a_still_present_generation() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        retain_present(&mut l, |_p| true);
        assert!(check(&l, "ks.t").is_err());
    }
}
