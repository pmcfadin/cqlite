//! The raw SSTable view's storage-side access seam (issue #4222).
//!
//! ONE responsibility: hand the raw view's row producers the per-generation
//! [`SSTableReader`](super::sstable::reader::SSTableReader) set for a table, WITHOUT
//! going through any of [`StorageEngine`](super::StorageEngine)'s reconciling
//! read paths.
//!
//! Split out of `storage/mod.rs` under the campsite rule (epic #1116): that
//! file sat 2 lines under the 800-line threshold on `origin/main` and this
//! accessor is what pushed it over. Since the accessor is #4222's own code and
//! is a self-contained responsibility, extracting it returns `storage/mod.rs`
//! to its pre-PR size instead of spending a `CQLITE_ALLOW_FILE_GROWTH=1`
//! opt-out on a file this change is solely responsible for crossing.
//!
//! ## Why the gate is on the `impl`, not on the module
//!
//! The accessor is reachable only under `state_machine` (its sole consumer,
//! `query::select_executor::raw_view`, lives under `query::select_executor`,
//! which is itself `#[cfg(feature = "state_machine")]` — `query/mod.rs:39`).
//! Ungated it is genuinely dead code in any feature set omitting
//! `state_machine`: the gate's `feature-iso-delta-scan` component builds
//! exactly such a set with `-D warnings` and FAILed on it.
//!
//! The `#[cfg]` therefore sits on the `impl` block below rather than on the
//! `mod` declaration in `storage/mod.rs`. Two reasons, in order:
//!
//! 1. `storage/mod.rs` had **2 lines of headroom** under the 800-line campsite
//!    threshold on `origin/main` (798). A gated declaration costs three lines
//!    (`#[cfg]` + `mod` + separator) and re-crosses it; a bare one costs one
//!    and does not. The ratchet exists to stop NEW threshold crossings, and
//!    this is the difference between causing one and not.
//! 2. It does not weaken #1712, whose rule is that a top-level `pub mod NAME;`
//!    in `lib.rs` must not be silently switched off by an inner `#![cfg(...)]`
//!    in `NAME`'s own file. This is a PRIVATE module, not a `lib.rs` public
//!    declaration, and the gate here is an ITEM-level `#[cfg]` on the `impl`,
//!    not a module-level `#![cfg]` — so nothing about the crate's public
//!    surface is misrepresented. Under a non-`state_machine` build this module
//!    compiles to nothing, which is exactly what it should do.

// Types are named by FULLY-QUALIFIED path below rather than imported: a `use`
// here would be an unused import — a `-D warnings` ERROR — in every build that
// cfg's the `impl` out, which is precisely the `feature-iso-delta-scan` lane
// this module's gate exists to satisfy. Gating the imports too would work but
// costs three more `#[cfg]` lines to say the same thing.
#[cfg(feature = "state_machine")]
impl super::StorageEngine {
    /// Snapshot the resolved [`SSTableReader`](super::sstable::reader::SSTableReader) set
    /// for `table_id`, plus the authoritative `fully_qualified_match` signal
    /// (issue #4222, raw SSTable view).
    ///
    /// A thin passthrough to `SSTableManager::resolve_reader_snapshot` — the
    /// raw view's point-key and full-scan row producers need the per-generation
    /// readers DIRECTLY (never through
    /// [`scan`](super::StorageEngine::scan)/[`scan_partition`](super::StorageEngine::scan_partition),
    /// which RECONCILE across generations) so they can emit one row per
    /// physical row per generation. `pub(crate)`: this bypasses every
    /// reconciliation guarantee `StorageEngine`'s other methods provide, so it
    /// is deliberately not part of the public API — only the query engine's
    /// raw-view producer (`query::select_executor::raw_view`) calls it.
    ///
    /// The bool mirrors `SSTableManager::resolve_reader_snapshot`'s own
    /// `fully_qualified_match`: `false` means a fully-qualified `table_id` (one
    /// carrying a keyspace) resolved ONLY via the bare-table-name fallback —
    /// the same signal the point-read path (`manager_point_read.rs`) threads
    /// into `get_with_resolution_unmetered` to keep strict keyspace matching
    /// on a fallback resolution (#1321), so a qualified raw-view name never
    /// silently reads another keyspace's same-named table's rows.
    ///
    /// Reachable only under `state_machine` (see the module doc and the gated
    /// declaration in `storage/mod.rs`): its sole consumer,
    /// `query::select_executor::raw_view`, lives under
    /// `query::select_executor`, which is itself
    /// `#[cfg(feature = "state_machine")]` (`query/mod.rs:39`). Ungated it is
    /// genuinely dead code in any feature set omitting `state_machine` — the
    /// gate's `feature-iso-delta-scan` component builds exactly such a set with
    /// `-D warnings` and FAILed on it. Gated to match the consumer rather than
    /// silenced with `allow(dead_code)`, because this is a real "unreachable in
    /// this configuration", not a false positive.
    pub(crate) async fn raw_view_reader_snapshot(
        &self,
        table_id: &crate::TableId,
    ) -> (
        Vec<std::sync::Arc<super::sstable::reader::SSTableReader>>,
        bool,
    ) {
        self.sstables.resolve_reader_snapshot(table_id).await
    }
}
