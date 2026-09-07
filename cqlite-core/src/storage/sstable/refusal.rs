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
//! **That key is the widest effect in this module, so reach for it only for a
//! refused SSTable.** Its rows belong to SOME table, which is what makes
//! "every table" the honest scope. A fact with a different subject must not borrow
//! it: an unreadable DIRECTORY is recorded in
//! [`incomplete_walk`](SSTableManager::incomplete_walk) instead, because a stock
//! root-owned `lost+found` recorded here would refuse every read on the most common
//! real deployment layout. Both recording sites carry this note
//! (`manager_open.rs`), so it is visible where the next one gets added.
//!
//! [`SSTableManager`]: crate::storage::sstable::SSTableManager

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{reader, SSTableManager};
use crate::types::TableId;
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

/// Drop every recorded refusal that no longer bears on its table, and drop keys
/// left empty.
///
/// Called from `refresh_tables` under the same write guard that applies the reader
/// diff. A refusal must be invalidated by BOTH of the ways a bad generation stops
/// being bad, or the table stays permanently unreadable:
///
/// * the file is **gone** — the operator deleted the bad generation;
/// * the file was **repaired in place** — the same path now opens. This is the
///   common case and the one a "still on disk?" test gets WRONG: a manager opened
///   mid-`rsync` records a refusal for a half-written `Statistics.db`, the copy
///   then completes, and the next refresh re-opens that exact path successfully.
///   Keying invalidation on disappearance alone left the refusal in place forever
///   with every generation healthy and open.
///
/// `still_refusing` is asked in CANONICAL form by the caller, which owns the
/// canonicalization cache and knows which paths this refresh re-opened.
pub(crate) fn retain_still_refusing(
    ledger: &mut RefusalLedger,
    still_refusing: impl Fn(&Path) -> bool,
) {
    for list in ledger.values_mut() {
        list.retain(|r| still_refusing(r.path()));
    }
    ledger.retain(|_key, list| !list.is_empty());
}

impl SSTableManager {
    /// [`resolve_reader_snapshot`](SSTableManager::resolve_reader_snapshot), but
    /// FAILING CLOSED first when any SSTable of `table_id` was REFUSED at open
    /// (issue #4159).
    ///
    /// # Why the check lives in the resolver and not at each guard
    ///
    /// Every read surface resolves its reader list through exactly one of two
    /// helpers, and each then had its own `reader_list.is_empty()` early return to
    /// `Ok(empty)`. Putting the check at each of those guards would (a) miss the
    /// PARTIAL case — some generations opened, one refused, so the list is NOT empty
    /// and the guard never fires — and (b) leave a new read surface free to inherit
    /// the swallow by writing its own guard. Checking HERE, before the snapshot is
    /// handed out, makes both impossible: a surface cannot obtain a reader list
    /// without the refusal question having been answered.
    ///
    /// The order matters: the refusal check runs BEFORE any I/O and before the
    /// emptiness test, so "the table is unreadable" is never reported as "the table
    /// is empty".
    pub(crate) async fn resolve_readers_checked(
        &self,
        table_id: &TableId,
    ) -> Result<(Vec<Arc<reader::SSTableReader>>, bool)> {
        self.ensure_readable(table_id).await?;
        let snapshot = self.resolve_reader_snapshot(table_id).await;
        if snapshot.0.is_empty() {
            self.ensure_absence_is_knowable(table_id).await?;
        }
        Ok(snapshot)
    }

    /// [`resolve_table_readers`](SSTableManager::resolve_table_readers) with the
    /// same check applied first — the streaming surfaces' counterpart to
    /// [`resolve_readers_checked`](SSTableManager::resolve_readers_checked).
    #[cfg(not(feature = "tombstones"))]
    pub(super) async fn resolve_table_readers_checked(
        &self,
        table_id: &TableId,
    ) -> Result<Vec<Arc<reader::SSTableReader>>> {
        self.ensure_readable(table_id).await?;
        let readers = self.resolve_table_readers(table_id).await;
        if readers.is_empty() {
            self.ensure_absence_is_knowable(table_id).await?;
        }
        Ok(readers)
    }

    /// `Ok(())` iff no recorded refusal bears on a read of `table_id`.
    ///
    /// Held in one place so the ledger's resolution rule (exact key, unqualified
    /// fallback, plus every unattributed refusal) cannot drift from
    /// [`resolve_reader_list`](SSTableManager::resolve_reader_list)'s.
    pub(crate) async fn ensure_readable(&self, table_id: &TableId) -> Result<()> {
        let refused = self.refused.read().await;
        check(&refused, table_id.name())
    }

    /// Record a directory an EXTERNAL discovery could not read (issue #4159).
    ///
    /// `SSTableManager::new_from_discovered_paths` is handed a table-directory list
    /// that someone else enumerated. If THAT enumeration was incomplete — an
    /// unreadable keyspace directory, say — this manager cannot find out on its own:
    /// it only ever reads the directories it was given, so a table that was never
    /// handed over is indistinguishable from a table that does not exist. Telling it
    /// closes that gap, and the note SURVIVES `refresh_tables` (which re-walks only
    /// the given directories and so can never re-observe, or clear, a gap above
    /// them).
    pub async fn note_incomplete_discovery(&self, directory: std::path::PathBuf, cause: Error) {
        let mut incomplete = self.incomplete_walk.write().await;
        incomplete.note_external(super::discovery_walk::UnreadableDir::new(directory, cause));
    }

    /// `Ok(())` iff the manager may honestly report that `table_id` has NO data.
    ///
    /// Asked ONLY when the resolved reader list came out empty, which is the one
    /// question an incomplete discovery walk can change the answer to. A table that
    /// WAS discovered and opened reads normally no matter how much of the rest of
    /// the tree was unreadable — that is what keeps a stock root-owned `lost+found`
    /// (mode 0700, present on essentially every ext4 data volume) harmless instead
    /// of turning it into a whole-database outage.
    ///
    /// See [`discovery_walk`](super::discovery_walk) for the four-outcome table this
    /// implements, and why the fact is NOT recorded as an unattributed refusal.
    pub(crate) async fn ensure_absence_is_knowable(&self, table_id: &TableId) -> Result<()> {
        let incomplete = self.incomplete_walk.read().await;
        let Some(first) = incomplete.iter().next() else {
            return Ok(());
        };
        Err(Error::incomplete_discovery(
            table_id.name(),
            first.path().to_path_buf(),
            incomplete.len(),
            first.cause(),
        ))
    }
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
    fn retain_clears_a_removed_generation() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        retain_still_refusing(&mut l, |_p| false);
        assert!(
            l.is_empty(),
            "a refused file that is gone must stop refusing"
        );
        assert!(check(&l, "ks.t").is_ok());
    }

    #[test]
    fn retain_keeps_a_generation_that_is_still_refusing() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        retain_still_refusing(&mut l, |_p| true);
        assert!(check(&l, "ks.t").is_err());
    }
}
