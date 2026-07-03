//! `Database.refresh()` + `RefreshReport` for Node.js bindings (issue #1749).
//!
//! Exposes an explicit directory refresh through the Node public surface. A
//! `Database` is a snapshot at `open()`: a Cassandra flush/compaction (or a
//! CQLite `--flush`) may add or remove SSTable generations under a warm handle,
//! and those changes become queryable only after `await db.refresh()`.

use napi_derive::napi;

use crate::database::Database;
use crate::error::to_napi_error;

/// Report returned by `Database.refresh()`.
///
/// Describes what an explicit directory refresh applied to the database's held
/// SSTable reader set: newly present generations become queryable, removed
/// generations stop being queried, and unchanged generations keep their warm
/// parsed state (they are not re-parsed).
///
/// Field names are camelCase on the JavaScript side (`tablesScanned`,
/// `readersAdded`, `readersRemoved`).
///
/// ## Example
///
/// ```javascript
/// const report = await db.refresh();
/// console.log(
///   `scanned ${report.tablesScanned} tables, ` +
///   `+${report.readersAdded}/-${report.readersRemoved} readers`
/// );
/// ```
#[napi(object)]
pub struct RefreshReport {
    /// Number of table directories re-discovered during the refresh.
    pub tables_scanned: u32,

    /// Number of SSTable generations newly opened and made queryable.
    pub readers_added: u32,

    /// Number of SSTable generations dropped from the reader set.
    pub readers_removed: u32,
}

impl RefreshReport {
    /// Build the Node report from the core [`cqlite_core::RefreshReport`].
    fn from_core(report: cqlite_core::RefreshReport) -> Self {
        // Saturating conversion (no-silent-data-loss posture): the counts are
        // usize on the core side; clamp to u32::MAX rather than truncating.
        Self {
            tables_scanned: u32::try_from(report.tables_scanned).unwrap_or(u32::MAX),
            readers_added: u32::try_from(report.readers_added).unwrap_or(u32::MAX),
            readers_removed: u32::try_from(report.readers_removed).unwrap_or(u32::MAX),
        }
    }
}

#[napi]
impl Database {
    /// Re-discover the data directory and apply changes to the held reader set.
    ///
    /// Re-runs the same TOC/filename-based discovery that `open()` used (no
    /// content sniffing, no heuristics) and applies the diff:
    /// - newly present generations become queryable,
    /// - removed generations stop being queried,
    /// - unchanged generations keep their warm parsed Index/Statistics/bloom
    ///   state (they are not re-parsed).
    ///
    /// In-flight queries are never affected: a scan already running completes
    /// against the pre-refresh set; a query issued after this Promise resolves
    /// sees the post-refresh set. The refresh is atomic and fail-closed — if any
    /// newly discovered generation fails to open (e.g. a corrupt
    /// `Statistics.db`), the returned Promise rejects and the previously held
    /// reader set is left fully unchanged.
    ///
    /// @returns Promise resolving to a RefreshReport with the applied counts
    /// @throws {CqliteError} If the database is closed or a new generation fails to open
    ///
    /// @example
    /// ```javascript
    /// const report = await db.refresh();
    /// console.log(`+${report.readersAdded}/-${report.readersRemoved} readers`);
    /// ```
    #[napi]
    pub async fn refresh(&self) -> napi::Result<RefreshReport> {
        self.ensure_open()?;
        let report = self.inner.refresh().await.map_err(to_napi_error)?;
        Ok(RefreshReport::from_core(report))
    }
}
