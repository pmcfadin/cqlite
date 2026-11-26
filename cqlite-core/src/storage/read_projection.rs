//! The columns a SELECT will actually read, carried to the cross-generation merged read
//! (issue #4640, owner ruling Q51).
//!
//! The merged read reassembles each non-frozen collection from its per-element cells
//! (`write_engine::merge::assemble_read_cells_with_udts`). Reassembly can fail closed
//! (a composite/UDT element the registry cannot resolve), so it must run ONLY for the
//! columns the query reads; otherwise a table that merely HAS such a collection would
//! break a query that selects other columns.
//!
//! The storage entry points (`scan`, `scan_stream`, `scan_partition*`, ...) are public
//! API used by many callers and carry no projection. Rather than widen every one, the
//! SELECT executor scopes the needed set around its storage calls with
//! [`with_needed`], and the merged-read drivers read it ONCE, synchronously, at entry
//! ([`current`]) before they hop to a blocking task. A call made outside any scope, or
//! from a task spawned inside one that did not re-enter it, reads `None`, which means
//! "every column" - the safe, fail-closed default, never a narrower answer.

use std::collections::HashSet;
#[cfg(feature = "state_machine")]
use std::future::Future;
use std::sync::Arc;

tokio::task_local! {
    static NEEDED: Option<Arc<HashSet<String>>>;
}

/// Run `fut` with `needed` as the active projection (`None` = every column).
#[cfg(feature = "state_machine")]
pub(crate) async fn with_needed<F: Future>(
    needed: Option<Arc<HashSet<String>>>,
    fut: F,
) -> F::Output {
    NEEDED.scope(needed, fut).await
}

/// The active projection, or `None` (every column) outside a [`with_needed`] scope.
#[cfg_attr(not(feature = "write-support"), allow(dead_code))]
pub(crate) fn current() -> Option<Arc<HashSet<String>>> {
    NEEDED.try_with(Clone::clone).ok().flatten()
}
