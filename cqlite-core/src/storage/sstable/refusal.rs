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

/// Every refusal that bears on a read of `table_name`, given that reader
/// resolution has already decided HOW this query reaches its readers.
///
/// # The invariant this function has to satisfy, in both directions
///
/// > The refusal lookup must consult exactly the key(s) the RECORDING side could
/// > have used for a table that resolution has decided this query reads.
/// > Narrowing the lookup is only sound to the extent the recording side is
/// > correspondingly precise.
///
/// Getting either direction wrong is a distinct defect, and the two pull opposite
/// ways:
///
/// * consult too FEW keys and a real refusal is missed — the read returns a
///   silently incomplete result, which is issue #4159 itself;
/// * consult too MANY and an unrelated table's refusal blocks a healthy one — a
///   false refusal.
///
/// This originally consulted the unqualified key UNCONDITIONALLY. That is safe
/// against the first failure (a strict superset of resolution's either/or cannot
/// miss a key) and unsafe against the second: reader resolution falls back to the
/// bare name only when NO exact fully-qualified entry exists, so for a query on a
/// healthy exact `ks.t` the union also picked up refusals belonging to a completely
/// unrelated FLAT-layout table `t`.
///
/// # Why gating the unqualified key on resolution is sound here
///
/// Because a refused generation of a QUALIFIED-layout table can never be keyed
/// under the bare name. Both recording keys come from the same path-derived
/// helpers the successful open uses
/// ([`base_path_table_key`](super::refresh::base_path_table_key) /
/// [`table_dir_table_key`](super::refresh::table_dir_table_key)), and
/// `extract_keyspace_and_table_name` is `extract_table_name` AND
/// `extract_keyspace` — both PURE functions of the path. Every generation of one
/// table shares a directory, hence a path shape, hence a key. So the qualified key
/// is derived for the refused generation exactly when it is derived for its healthy
/// siblings; the bare name is reached only when `extract_keyspace` finds no keyspace
/// level at all, i.e. a genuinely flat layout, where the bare name IS the table's
/// identity and resolution would have used the fallback to reach it.
///
/// The ONE place the refused and healthy keys really can diverge is priority 3: a
/// successful open can fall back to the header's `table_name`, and a refused open
/// has no header to read, so it lands on [`UNATTRIBUTED_TABLE_KEY`] instead. That
/// is precisely why the unattributed key stays consulted UNCONDITIONALLY below —
/// narrowing must never drop it, or this fix reintroduces the swallow it exists to
/// remove.
///
/// # How narrow the bare-name recording key actually is
///
/// Measured from the helpers, because it bounds what an end-to-end test can even
/// stage. `base_path_table_key` reaches its priority-2 (bare) branch only when
/// priority 1 fails, and priority 1 has two failure modes: `extract_keyspace` is
/// `None`, or `table_name == base_dir_name`. The second cannot help, because
/// priority 2 compares the SAME `extract_table_name` value against the SAME
/// `base_dir_name` and fails identically. So the bare key requires
/// `extract_keyspace(path) == None`, and that is
/// `resolve_table_dir(path).parent().file_name()` — `None` only when the table
/// directory is a child of the filesystem ROOT (`/t/nb-1-big-Data.db`) or of a
/// depth-1 RELATIVE path. `table_dir_table_key` has the same requirement and no
/// `base_dir_name` guard at all.
///
/// Two consequences, both load-bearing:
///
/// * the false refusal this gating removes is real but needs an SSTable directly
///   under `/`, so it is NOT stageable inside a `TempDir` — it is pinned here, at
///   the function whose contract it is, rather than end to end;
/// * a refusal can never be keyed by the header-derived bare name either, since the
///   recording side passes `""` for `header_table_name` and priority 3 skips an
///   empty string. Every constructible refusal key is therefore the QUALIFIED name
///   or [`UNATTRIBUTED_TABLE_KEY`] — which is exactly why gating the bare key is
///   sound and gating the unattributed key would not be.
///
/// `resolution_used_exact_key` is
/// [`fully_qualified_match`](crate::storage::sstable::SSTableManager::fully_qualified_match),
/// taken from the SAME `table_readers` guard that produced the reader list, so the
/// lookup can never be gated on a stale view of the map.
///
/// Returned as a flat borrow list so the caller can report a count AND the first
/// refusal without a second lookup.
fn bearing_on<'a>(
    ledger: &'a RefusalLedger,
    table_name: &str,
    resolution_used_exact_key: bool,
) -> Vec<&'a RefusedSSTable> {
    let mut out: Vec<&RefusedSSTable> = Vec::new();
    if let Some(list) = ledger.get(table_name) {
        out.extend(list.iter());
    }
    // Only when resolution ACTUALLY fell back to the bare name. When it matched the
    // fully-qualified key exactly, a bare-name refusal belongs to a different table.
    if !resolution_used_exact_key {
        let unqualified = table_name
            .rfind('.')
            .map_or(table_name, |dot| &table_name[dot + 1..]);
        if unqualified != table_name {
            if let Some(list) = ledger.get(unqualified) {
                out.extend(list.iter());
            }
        }
    }
    // UNCONDITIONAL, by design and in both directions — see the note above about
    // priority 3. "We could not read this file and cannot even say which table it
    // belonged to" means no table's answer is knowably complete.
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
pub(crate) fn check(
    ledger: &RefusalLedger,
    table_name: &str,
    resolution_used_exact_key: bool,
) -> Result<()> {
    if ledger.is_empty() {
        return Ok(());
    }
    let bearing = bearing_on(ledger, table_name, resolution_used_exact_key);
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

/// `Ok(())` when the manager may honestly report that `table_name` has NO data;
/// otherwise [`Error::IncompleteDiscovery`] naming the first unreadable directory.
///
/// Asked ONLY when the resolved reader list came out empty, which is the one
/// question an incomplete discovery walk can change the answer to. A table that WAS
/// discovered and opened reads normally no matter how much of the rest of the tree
/// was unreadable — that is what keeps a stock root-owned `lost+found` (mode 0700,
/// present on essentially every ext4 data volume) harmless instead of turning it
/// into a whole-database outage.
///
/// Pure over the walk record, for the same reason [`check`] is pure over the
/// ledger: the caller reads BOTH under one guard acquisition and evaluates
/// afterwards (see [`SSTableManager::resolve_checked_snapshot`]).
///
/// See [`discovery_walk`](super::discovery_walk) for the four-outcome table this
/// implements, and why the fact is NOT recorded as an unattributed refusal.
pub(crate) fn absence_check(
    incomplete: &super::discovery_walk::IncompleteDiscovery,
    table_name: &str,
) -> Result<()> {
    let Some(first) = incomplete.iter().next() else {
        return Ok(());
    };
    Err(Error::incomplete_discovery(
        table_name,
        first.path().to_path_buf(),
        incomplete.len(),
        first.cause(),
    ))
}

/// The COMPLETE read-admission verdict for one consistent snapshot.
///
/// Every input is a value already read under a single `table_readers` guard
/// acquisition, so this function cannot observe a torn state — which is the whole
/// point (issue #4159, roborev round 5). Keeping it pure also makes the ordering
/// rule below unit-testable without a runtime, a filesystem or a lock.
///
/// The order is load-bearing and is asserted by the tests: a REFUSAL outranks an
/// empty reader list, so "this table is unreadable" is never reported as "this
/// table is empty"; and the absence question is asked ONLY when the reader list is
/// empty, so a discovered, healthy table still reads while part of the tree is
/// unreadable.
pub(crate) fn verdict_for(
    ledger: &RefusalLedger,
    incomplete: &super::discovery_walk::IncompleteDiscovery,
    table_name: &str,
    resolution_used_exact_key: bool,
    readers_are_empty: bool,
) -> Result<()> {
    check(ledger, table_name, resolution_used_exact_key)?;
    if readers_are_empty {
        absence_check(incomplete, table_name)?;
    }
    Ok(())
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
    /// # ONE consistent snapshot — resolution and validation cannot be torn
    ///
    /// Issue #4159, roborev round 5. The reader list and the refusal/discovery
    /// verdict used to be THREE separate lock acquisitions, and the window between
    /// them was not benign. A read could resolve an EMPTY reader list, a concurrent
    /// `refresh_tables` could then add a repaired reader and clear its refusal, and
    /// the checks — running after — saw a clean ledger. The read returned
    /// `Ok(empty)`: at the snapshot instant the answer was `Err` (a refused
    /// generation), after the refresh it was one row, and `Ok(empty)` was correct at
    /// NEITHER. That is a linearizability violation, not stale-but-valid data, and
    /// its symptom is exactly the silent empty this issue exists to remove — which
    /// is why the ordinary "a snapshot may be stale" argument (issue #1591, where
    /// `Ok(empty)` before a flush IS the true answer at the scan's instant) does not
    /// cover it.
    ///
    /// So all three are read under a SINGLE `table_readers` read-guard acquisition
    /// and the verdict is computed from those values by the pure
    /// [`verdict_for`]. A refresh cannot interleave, because applying its diff needs
    /// the `table_readers` WRITE guard.
    ///
    /// ## No guard is held across I/O (issue #1591 preserved)
    ///
    /// Everything inside the guarded block is a MEMORY read — cloning `Arc`s and
    /// two `HashMap`/`Vec` lookups; no SSTable, index or statistics byte is touched.
    /// All three guards are released at the end of that block, BEFORE this function
    /// returns, so every caller does its reader I/O with nothing held. That is the
    /// property `#1591`'s `scan_gate` exists to police, and it is unchanged.
    ///
    /// ## Lock order: `table_readers` -> `refused` -> `incomplete_walk`
    ///
    /// A strict SUFFIX of the writers' order (`readers` -> `table_readers` ->
    /// `refused` -> `incomplete_walk`, in `refresh_tables` and both loaders), so a
    /// scan can never deadlock against a refresh. Never invert these.
    async fn resolve_checked_snapshot(
        &self,
        table_id: &TableId,
    ) -> Result<(Vec<Arc<reader::SSTableReader>>, bool)> {
        let table_name = table_id.name();

        let (readers, resolution_used_exact_key, verdict) = {
            // (1) `table_readers` — the reader set AND the exact-vs-fallback flag,
            //     so the flag can never describe a different map than the readers.
            let table_readers = self.table_readers.read().await;
            let resolution_used_exact_key = Self::fully_qualified_match(&table_readers, table_name);
            let readers = Self::resolve_reader_list(&table_readers, table_name)
                .cloned()
                .unwrap_or_default();

            // (2) + (3) `refused`, then `incomplete_walk`, both STILL under (1).
            let refused = self.refused.read().await;
            let incomplete = self.incomplete_walk.read().await;
            let verdict = verdict_for(
                &refused,
                &incomplete,
                table_name,
                resolution_used_exact_key,
                readers.is_empty(),
            );

            (readers, resolution_used_exact_key, verdict)
        }; // ALL guards dropped here — nothing is held across the caller's I/O.

        verdict?;
        Ok((readers, resolution_used_exact_key))
    }

    /// [`resolve_reader_snapshot`](SSTableManager::resolve_reader_snapshot), but
    /// FAILING CLOSED when any SSTable of `table_id` was REFUSED at open, or when
    /// the table was not discovered and discovery was incomplete (issue #4159).
    pub(crate) async fn resolve_readers_checked(
        &self,
        table_id: &TableId,
    ) -> Result<(Vec<Arc<reader::SSTableReader>>, bool)> {
        self.resolve_checked_snapshot(table_id).await
    }

    /// [`resolve_table_readers`](SSTableManager::resolve_table_readers) with the
    /// same check applied — the streaming surfaces' counterpart to
    /// [`resolve_readers_checked`](SSTableManager::resolve_readers_checked).
    ///
    /// Shares [`resolve_checked_snapshot`](SSTableManager::resolve_checked_snapshot)
    /// verbatim rather than repeating the acquisition, so the lock ORDER and the
    /// one-snapshot property exist in exactly one place and a future edit cannot
    /// make the two surfaces disagree. `resolve_table_readers`' own lookup is
    /// semantically identical (exact key, else the bare name), so no reader set
    /// changes.
    #[cfg(not(feature = "tombstones"))]
    pub(super) async fn resolve_table_readers_checked(
        &self,
        table_id: &TableId,
    ) -> Result<Vec<Arc<reader::SSTableReader>>> {
        Ok(self.resolve_checked_snapshot(table_id).await?.0)
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
        assert!(check(&l, "ks.t", true).is_ok());
        assert!(check(&l, "t", true).is_ok());
    }

    #[test]
    fn exact_key_refuses_and_names_the_cause() {
        let l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        let e = check(&l, "ks.t", true).expect_err("a recorded refusal must fail the read");
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
            check(&l, "ks.t", false).is_err(),
            "when resolution FELL BACK to the bare name, a fully-qualified query is \
             reading that very table, so it must see that key's refusals"
        );
    }

    #[test]
    fn an_unrelated_table_is_unaffected() {
        let l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        assert!(
            check(&l, "ks.other", true).is_ok(),
            "one corrupt file must not render an unrelated table unreadable"
        );
    }

    #[test]
    fn unattributed_refusal_bears_on_every_table() {
        let l = ledger_with(UNATTRIBUTED_TABLE_KEY, "/d/stray-Data.db");
        // Unconditional in BOTH resolution modes: this is the deliberate
        // poison-everything key, and narrowing must never drop it.
        assert!(check(&l, "ks.t", true).is_err());
        assert!(check(&l, "ks.t", false).is_err());
        assert!(check(&l, "other", true).is_err());
    }

    /// THE FINDING (roborev round 4): a flat-layout table's refusal must not block
    /// an unrelated, healthy, exactly-matched qualified table.
    ///
    /// Reader resolution falls back to the bare name only when no exact
    /// fully-qualified entry exists. When `ks.t` HAS exact readers, a refusal keyed
    /// `t` belongs to a different table entirely, and blocking on it is a false
    /// refusal — the union that used to be unconditional here did exactly that.
    #[test]
    fn a_flat_layout_refusal_does_not_block_an_exactly_matched_qualified_table() {
        let l = ledger_with("t", "/d/t-1/nb-1-big-Data.db");
        assert!(
            check(&l, "ks.t", true).is_ok(),
            "resolution matched the fully-qualified key EXACTLY, so it never reads \
             the flat table `t`; its refusal must not block this read"
        );
    }

    /// THE INVERSE, and the regression guard for the next person who narrows this.
    ///
    /// Narrowing the lookup is only sound where the RECORDING side is correspondingly
    /// precise, and there is exactly one place it is not: a successful open can key a
    /// table by its header `table_name` (priority 3), while a REFUSED open has no
    /// header to read and lands on [`UNATTRIBUTED_TABLE_KEY`]. So a table can hold
    /// healthy exact readers AND a refusal recorded over-broad, and an exact-only
    /// check would miss it and answer a silently incomplete result — issue #4159
    /// reintroduced by its own fix.
    ///
    /// The bare-name key cannot diverge this way (see [`bearing_on`]: both recording
    /// keys are pure functions of the path, and generations of one table share a
    /// directory), which is why the unqualified key MAY be gated and the unattributed
    /// key MAY NOT.
    #[test]
    fn an_over_broad_unattributed_refusal_is_seen_even_on_an_exact_match() {
        let l = ledger_with(UNATTRIBUTED_TABLE_KEY, "/d/ks/t-1/nb-2-big-Data.db");
        let e = check(&l, "ks.t", true).expect_err(
            "a refused open whose table could not be derived from its path is keyed \
             UNATTRIBUTED — narrowing to the exact key must not drop it, or a table \
             with healthy exact readers answers a silently incomplete result",
        );
        assert!(matches!(e, Error::UnreadableSSTable { .. }));
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
        match check(&l, "ks.t", false) {
            Err(Error::UnreadableSSTable { refused, .. }) => assert_eq!(refused, 3),
            other => panic!("expected 3 bearing refusals, got {other:?}"),
        }
    }

    /// The verdict is a pure function of ONE snapshot, and this pins the exact
    /// outcome the torn read produced wrongly (roborev round 5).
    ///
    /// The race was: resolve an EMPTY reader list, a concurrent refresh clears the
    /// refusal, the later check sees a clean ledger, and the read answers
    /// `Ok(empty)` — correct at neither instant. With both values taken from one
    /// snapshot, "empty reader list AND a live refusal" can only ever be `Err`.
    #[test]
    fn an_empty_reader_list_with_a_live_refusal_is_err_not_empty_success() {
        let l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        let none = crate::storage::sstable::discovery_walk::IncompleteDiscovery::default();
        let e = verdict_for(&l, &none, "ks.t", true, /* readers_are_empty */ true)
            .expect_err("an unreadable table must never be reported as an empty one");
        assert!(matches!(e, Error::UnreadableSSTable { .. }));
    }

    /// A REFUSAL outranks emptiness, and emptiness is only consulted when the reader
    /// list really is empty. Both halves of the ordering, from one snapshot.
    #[test]
    fn a_refusal_outranks_emptiness_and_a_non_empty_table_ignores_absence() {
        let l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        let none = crate::storage::sstable::discovery_walk::IncompleteDiscovery::default();

        // Non-empty reader list + a refusal on one generation: still Err (the
        // PARTIAL case — a partial answer presented as complete is the same defect).
        assert!(matches!(
            verdict_for(&l, &none, "ks.t", true, false),
            Err(Error::UnreadableSSTable { .. })
        ));

        // Clean ledger, complete walk: both list states are Ok.
        let clean = RefusalLedger::new();
        assert!(verdict_for(&clean, &none, "ks.t", true, true).is_ok());
        assert!(verdict_for(&clean, &none, "ks.t", true, false).is_ok());
    }

    /// An incomplete walk makes ABSENCE unknowable, but never makes a DISCOVERED
    /// table unreadable — the property that keeps a stock `lost+found` from being a
    /// whole-database outage. Also from one snapshot.
    #[test]
    fn an_incomplete_walk_only_bears_on_an_empty_reader_list() {
        let clean = RefusalLedger::new();
        let mut incomplete =
            crate::storage::sstable::discovery_walk::IncompleteDiscovery::default();
        incomplete.extend_from_walk(
            [crate::storage::sstable::discovery_walk::UnreadableDir::new(
                PathBuf::from("/d/lost+found"),
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "denied",
                )),
            )],
        );

        // Discovered and healthy: reads normally despite the gap.
        assert!(
            verdict_for(&clean, &incomplete, "ks.t", true, false).is_ok(),
            "a table that WAS discovered and opened must still read"
        );

        // Not discovered, and the walk could not see everything: absence is not a
        // fact we know.
        let e = verdict_for(&clean, &incomplete, "ks.t", true, true)
            .expect_err("absence cannot be confirmed while part of the tree is unreadable");
        match e {
            Error::IncompleteDiscovery { directory, .. } => {
                assert_eq!(directory, PathBuf::from("/d/lost+found"));
            }
            other => panic!("expected IncompleteDiscovery, got {other:?}"),
        }
    }

    /// The ORDERING property itself, deterministically and with no wall clock:
    /// resolution and validation really do share ONE `table_readers` guard
    /// acquisition (roborev round 5).
    ///
    /// # How this discriminates, without a timing race
    ///
    /// From outside, the atomic and the split shapes are hard to tell apart: both
    /// end up touching the same locks. The one observable difference is what is HELD
    /// while the snapshot waits on `refused`. So the test parks it there on purpose:
    /// it takes the `refused` WRITE guard first, starts the snapshot, and drives the
    /// spawned task to its first pending await with `yield_now` — cooperative
    /// scheduling, not a sleep. At that point the snapshot is blocked on
    /// `refused.read()`.
    ///
    /// `table_readers.try_write()` is then the probe, and it is non-blocking, so
    /// there is no wall-clock threshold anywhere in this test:
    ///
    /// * ONE snapshot  -> `table_readers` is still held  -> `try_write` FAILS  (asserted)
    /// * TWO snapshots -> it was released before `refused` -> `try_write` SUCCEEDS
    ///
    /// A refresh applies its diff under `table_readers.write()`, so "try_write
    /// fails" is exactly "a refresh cannot interleave between resolution and
    /// validation" — the property the finding is about.
    #[tokio::test]
    async fn resolution_and_validation_share_one_table_readers_guard() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let config = crate::Config::default();
        let platform = Arc::new(crate::Platform::new(&config).await.expect("platform"));
        let manager = Arc::new(
            SSTableManager::new(
                tmp.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .expect("manager over an empty directory"),
        );

        // Hold `refused` for writing so the snapshot MUST park on it.
        let refused_guard = manager.refused.write().await;

        let parked = {
            let m = Arc::clone(&manager);
            tokio::spawn(async move {
                let _ = m.resolve_checked_snapshot(&TableId::new("ks.t")).await;
            })
        };

        // Drive the spawned task to its first pending await. Cooperative yields,
        // never a sleep — nothing here depends on elapsed time.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !parked.is_finished(),
            "control: the snapshot must be BLOCKED on `refused` for this probe to \
             mean anything"
        );

        assert!(
            manager.table_readers.try_write().is_err(),
            "the snapshot must STILL hold the `table_readers` read guard while it \
             waits for `refused`. If it has released it, resolution and validation \
             are two separate snapshots and a concurrent refresh can interleave \
             between them — the read then answers Ok(empty) for a state that was \
             true at neither instant (#4159, roborev round 5)"
        );

        drop(refused_guard);
        parked
            .await
            .expect("the parked snapshot completes once `refused` is free");

        // And with nothing held, the guards really were all released: a refresh's
        // write acquisition succeeds.
        assert!(
            manager.table_readers.try_write().is_ok(),
            "every guard must be dropped before the snapshot returns, or #1591's \
             no-guard-across-I/O property is broken"
        );
    }

    #[test]
    fn retain_clears_a_removed_generation() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        retain_still_refusing(&mut l, |_p| false);
        assert!(
            l.is_empty(),
            "a refused file that is gone must stop refusing"
        );
        assert!(check(&l, "ks.t", true).is_ok());
    }

    #[test]
    fn retain_keeps_a_generation_that_is_still_refusing() {
        let mut l = ledger_with("ks.t", "/d/ks/t-1/nb-1-big-Data.db");
        retain_still_refusing(&mut l, |_p| true);
        assert!(check(&l, "ks.t", true).is_err());
    }
}
