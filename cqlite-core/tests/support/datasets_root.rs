//! TABLE-granular datasets-root resolution for the dataset-backed read-path lanes
//! (issue #3220).
//!
//! # The defect this module exists to remove
//!
//! FOUR lanes carried a private, byte-identical copy of the same broken selection —
//! the three in `cqlite-core/tests` reconciled here (`point_vs_full_differential.rs`
//! + its `one_vs_n_generation` submodule, `query_semantics_oracle_parity.rs`,
//! `read_path_forcing_e2e.rs`) and a fourth, `cqlite-flight/tests/
//! query_semantics_flight_parity.rs`, which is **NOT** reconciled (see below):
//!
//! ```ignore
//! candidates.into_iter().flatten().find(|root| root.join(keyspace).is_dir())
//! ```
//!
//! That selects a corpus root by **keyspace**, then COMMITS to it with no fallback,
//! even though what every caller actually needs is a specific **table**. A root that
//! holds the keyspace but not the table therefore wins the selection and the table is
//! then declared absent — while a *different* candidate root (typically the checkout's
//! own committed corpus) has the fixture right there.
//!
//! That is not hypothetical. On a fleet box `fetch-datasets.sh --verify-only` names
//! `CQLITE_DATASETS_ROOT=/data/datasets`, whose `sstables/test_da/` holds
//! `wide_table` but NOT the git-committed `multiclustering_table` (#3032). The
//! keyspace-granular resolver picked `/data/datasets/sstables`, missed the table, and
//! the #3032 multi-component clustering differential case SKIPPED SILENTLY while the
//! suite reported green (#3220) — precisely the "dataset-dependent assertion that can
//! pass without ever running" class the fail-closed doctrine exists to eliminate.
//!
//! # The rule
//!
//! [`sstables_root_for_table`] walks EVERY candidate root in preference order and
//! returns the first one that actually carries `<keyspace>/<table>-*/…-Data.db`. A
//! committed fixture is therefore always found in the checkout, whatever a machine's
//! `CQLITE_DATASETS_ROOT` happens to contain. Modelled on the already-correct
//! `issue_3032_multiclustering_clustering_slice_select.rs::datasets_root`.
//!
//! Presence is judged by an actual `*-Data.db` component, never by directory
//! existence: the repo commits JSONL sidecars for fixtures whose binaries are
//! gitignored, so `<table>-<uuid>/` can exist with no readable SSTable in it.
//!
//! # The FOURTH lane, and why it is deliberately not reconciled here
//!
//! `cqlite-flight/tests/query_semantics_flight_parity.rs` still carries the
//! keyspace-granular `sstables_root(keyspace)` copy. It is LOWER RISK — its gate
//! component pins `CQLITE_REQUIRE_FIXTURES=1`, so a miss FAILs there instead of
//! skipping silently — and reconciling it is NOT a mechanical swap:
//!
//!   * it is a DIFFERENT CRATE, so it cannot reach this module without real
//!     plumbing (a shared test-support crate wired as a `dev-dependency`, or a
//!     cross-crate `#[path]` include whose own nested `#[path]` to
//!     `test-data/support/fixture_roots.rs` then resolves from a foreign manifest);
//!   * its lookup is GENERATION-granular, not table-granular: `fixture_dir` requires
//!     a specific `<sstable_prefix>-Data.db` (`nb-1-big-…`) inside a
//!     `<prefix><uuid>/` directory, where [`table_has_data`] accepts ANY `*-Data.db`,
//!     so the shared helper would need a new generation-aware entry point; and
//!   * its `schema_path` climbs `..` from `CQLITE_DATASETS_ROOT` — the #3148 trap
//!     this module refuses — so swapping it in is a behaviour change to that lane,
//!     not a refactor.
//!
//! Tracked as follow-up work rather than smuggled into #3220. Update this note (and
//! the count above) if that lane moves onto the shared helper.
//!
//! # Roots, and which one owns what
//!
//! Dataset (fetched, relocatable) vs schemas (committed source) resolution is owned by
//! `test-data/support/fixture_roots.rs` (#3131/#3148); this module builds the
//! table-granular search on top of it rather than re-deriving either root. In
//! particular [`schema_path`] resolves the COMMITTED schema fixtures
//! checkout-relative — it never climbs `..` from the datasets root, the symlink trap
//! #3148 removed.
//!
//! Included with `#[path = "support/datasets_root.rs"] mod datasets_root;` from a
//! `cqlite-core/tests/*.rs` target root.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

#[path = "../../../test-data/support/fixture_roots.rs"]
pub mod fixture_roots;

/// The repository root (the parent of `cqlite-core/`).
///
/// Anchored on the workspace-root `Cargo.toml` marker (see
/// [`fixture_roots::workspace_root`]), falling back to the manifest's parent — the
/// shape the private copies used — when no `[workspace]` manifest is found.
pub fn repo_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    fixture_roots::workspace_root()
        .unwrap_or_else(|| manifest.parent().unwrap_or(manifest).to_path_buf())
}

/// TEST-HARNESS ONLY: substitute for the CHECKOUT candidate root.
///
/// # Why this seam has to exist
///
/// The fail-closed contract has two directions, and both must be DEMONSTRABLE
/// (#3220 AC2): a fixture that is *present but empty* must fail, and a fixture that
/// is *absent from every candidate root* must fail. The first is easy to stage —
/// point `CQLITE_DATASETS_ROOT` at a temp root holding a truncated `Data.db`. The
/// second is not: the checkout candidate is derived from `CARGO_MANIFEST_DIR`, a
/// COMPILE-TIME constant, so no runtime environment can make a committed fixture
/// invisible. Staging it for real would mean deleting a git-tracked fixture from the
/// working tree — which also trips the gate's mid-run `tree-integrity` check — or
/// recompiling the whole crate inside a throwaway worktree, far too slow for a
/// committed self-test. So the candidate list takes this substitution instead.
///
/// # Why it cannot weaken anything
///
/// It can only ever REMOVE fixtures from view, never add or fake one: every
/// resolution still requires a real `*-Data.db` under the substituted root. So the
/// only reachable effect of a stray value is that lanes FAIL (a `must_run` case that
/// did not run), never that one passes vacuously — the safe direction. It lives in
/// test-support code compiled solely into test binaries, never into the library, and
/// its own behavior is asserted by `scripts/tests/test_point_vs_full_failclosed.sh`.
pub const CHECKOUT_SSTABLES_ROOT_OVERRIDE_ENV: &str = "CQLITE_TEST_CHECKOUT_SSTABLES_ROOT";

/// Every `sstables/` root to search, in preference order: the `CQLITE_DATASETS_ROOT`
/// corpus (when set and present) first, then the checkout's committed corpus.
///
/// Deduplicated, so a `CQLITE_DATASETS_ROOT` that already points at the checkout does
/// not report the same path twice in a diagnostic.
///
/// Note this is an ORDER, not a PREFERENCE: [`first_root_with_table`] picks by
/// EVIDENCE (which root actually holds the table), so the order only breaks ties
/// between roots that can both serve the request. Neither root is a superset of the
/// other — the fetched corpus carries keyspaces the checkout does not, and the
/// checkout carries committed parity fixtures the fetched corpus does not — so any
/// fixed preference between them is wrong for one set of tables (#3104).
pub fn sstables_root_candidates() -> Vec<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(env_root) = fixture_roots::datasets_root_if_present() {
        candidates.push(env_root.join("sstables"));
    }
    let checkout = match std::env::var_os(CHECKOUT_SSTABLES_ROOT_OVERRIDE_ENV) {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => fixture_roots::checkout_test_data_dir()
            .join("datasets")
            .join("sstables"),
    };
    if !candidates.contains(&checkout) {
        candidates.push(checkout);
    }
    candidates
}

/// True when `<root>/<keyspace>` holds at least one `<table>-*` directory carrying a
/// `*-Data.db` — i.e. the binaries are really there, not just the JSONL sidecars.
pub fn table_has_data(root: &Path, keyspace: &str, table: &str) -> bool {
    let ks_dir = root.join(keyspace);
    let Ok(entries) = std::fs::read_dir(&ks_dir) else {
        return false;
    };
    let prefix = format!("{table}-");
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
        })
        .any(|dir| {
            std::fs::read_dir(&dir)
                .map(|rd| {
                    rd.filter_map(|e| e.ok()).any(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.ends_with("-Data.db"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        })
}

/// The first candidate `sstables/` root that actually carries
/// `<keyspace>/<table>-*/…-Data.db`, or `None` when no candidate does.
///
/// TABLE-granular BY CONTRACT: selecting on the keyspace alone is the #3220 defect —
/// it commits to a root that may not hold the table and reports a fixture that exists
/// in the checkout as absent.
pub fn sstables_root_for_table(keyspace: &str, table: &str) -> Option<PathBuf> {
    first_root_with_table(&sstables_root_candidates(), keyspace, table).map(Path::to_path_buf)
}

/// PURE form of [`sstables_root_for_table`], parameterized on the candidate list.
///
/// Factored so the selection rule is testable against SYNTHETIC roots
/// (`issue_3220_datasets_root_resolution.rs`): the real candidate list is half
/// environment and half a COMPILE-TIME checkout path, so a test reading it can only
/// ever observe this machine's layout — and the defect (#3220) was a rule that looked
/// fine on a machine where every root happened to hold every table.
pub fn first_root_with_table<'a>(
    roots: &'a [PathBuf],
    keyspace: &str,
    table: &str,
) -> Option<&'a Path> {
    roots
        .iter()
        .find(|root| table_has_data(root, keyspace, table))
        .map(PathBuf::as_path)
}

/// A diagnostic naming every candidate root, for callers whose fixture lookup is not
/// `<keyspace>/<table>-*` shaped (e.g. the oracle's `fixture_dir_prefix` form).
pub fn describe_roots() -> String {
    let roots = sstables_root_candidates()
        .iter()
        .map(|r| r.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("searched [{roots}] (fetch: bash test-data/scripts/fetch-datasets.sh)")
}

/// A diagnostic naming EVERY root searched for `<keyspace>.<table>` — so a SKIP or a
/// fail-closed message says where the lane looked, not merely that something was
/// "absent" (issue #3220: the previous message named neither the table nor the roots,
/// which is what made the same absence read as a confusing hard FAIL in one lane and a
/// silent SKIP in another).
pub fn describe_search(keyspace: &str, table: &str) -> String {
    let roots = sstables_root_candidates()
        .iter()
        .map(|r| r.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "no *-Data.db for {keyspace}.{table} under any candidate sstables root [{roots}] \
         (fetch: bash test-data/scripts/fetch-datasets.sh)"
    )
}

/// Absolute path to a COMMITTED schema fixture, or `None` when it is unreadable.
///
/// Delegates to `fixture_roots` (checkout-relative, honoring an ABSOLUTE
/// `CQLITE_SCHEMAS_ROOT` override), so the schemas root is never derived from
/// `CQLITE_DATASETS_ROOT` by climbing `..` (#3148). Returns `Option` rather than
/// panicking so a caller keeps its own SKIP-vs-fail-closed decision.
pub fn schema_path(schema_file: &str) -> Option<PathBuf> {
    let (root, source) = fixture_roots::schemas_root_resolved();
    fixture_roots::resolve_schema_path(&root, source, schema_file).ok()
}
