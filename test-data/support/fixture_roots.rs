//! Single-source resolution of the `test-data/` roots used by tests and benches
//! (issues #3131 / #3148).
//!
//! # Why this file exists, and why it lives here
//!
//! Two distinct roots were historically conflated:
//!
//! | Root | Nature | Owner |
//! |------|--------|-------|
//! | `test-data/datasets` | **fetched, relocatable** binary corpus (`fetch-datasets.sh`) | `CQLITE_DATASETS_ROOT` |
//! | `test-data/schemas`  | **committed source** (23 files incl. `legacy/`, `udts/`) | the checkout |
//!
//! Four independently-written call sites used to derive the *schemas* root from the
//! *datasets* root by climbing `..` (`datasets_root().join("../schemas")`). That is
//! wrong in two ways:
//!
//! 1. It makes committed source's location depend on an env var whose entire purpose
//!    is to point at *relocatable fetched data*. A machine whose corpus lives at
//!    `/data/datasets` then needs a `/data/schemas` that no `git checkout` ever
//!    creates — the #3131 failure ("no single `CQLITE_DATASETS_ROOT` works").
//! 2. `join("..")` is **not** a lexical parent at the syscall level: the kernel
//!    resolves `datasets/..` against the *symlink target's* parent. So
//!    `ln -s <checkout>/test-data/datasets /data/datasets` makes
//!    `/data/datasets/../schemas` silently resolve to `<checkout>/test-data/schemas`,
//!    while a real `/data/datasets` directory resolves to `/data/schemas`. Two
//!    visually identical layouts, opposite outcomes, no error explaining why
//!    (#3148, "the symlink trap").
//!
//! **Owner decision (#3148 AC (h), proposed fix 4): the schemas root is resolved
//! CHECKOUT-RELATIVE, never from `CQLITE_DATASETS_ROOT`.** A checkout always has
//! these files, so the failure mode is structurally impossible and the symlink trap
//! disappears rather than being papered over — this module contains no `..` climbing
//! from the datasets root, so there is nothing left to mis-resolve.
//!
//! This file is deliberately hosted under `test-data/` rather than inside either
//! crate: it encodes the layout of `test-data/` itself, it is owned by neither
//! `cqlite-core` nor `cqlite-cli`, and the include path is symmetric (`../../` from
//! any `<crate>/tests/` or `<crate>/benches/` directory). It is pulled in with
//! `#[path = "…/test-data/support/fixture_roots.rs"] mod fixture_roots;`, so it
//! compiles into each consuming test/bench target with no new crate or dependency.
//!
//! # The `datasets_root()` contract (#3148 AC (e))
//!
//! There is ONE resolution rule with TWO documented shapes. The distinction is a
//! real, deliberate behavioral choice — not an accident:
//!
//! * [`datasets_root()`] — **infallible**, with a checkout-relative fallback when
//!   `CQLITE_DATASETS_ROOT` is unset. For benches and for tests that *must* have the
//!   corpus: they proceed and fail later with an actionable per-fixture message. This
//!   is the shape that lets `cargo bench` work from a plain checkout with no env setup.
//! * [`datasets_root_if_present()`] — **fallible**, `Some` only when
//!   `CQLITE_DATASETS_ROOT` is set AND names a directory; **no checkout fallback**.
//!   For SKIP-gated tests: with no env var they must skip, not silently run against
//!   the checkout's ~19 committed byte-parity reference files (which do not contain
//!   the canonical `test_basic` corpus) and report a 0-row pass.
//!
//! Both shapes derive from [`checkout_test_data_dir()`] and the same env var, so the
//! two cannot drift apart the way the three hand-written copies did.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Environment override for the **fetched** dataset corpus.
pub const DATASETS_ROOT_ENV: &str = "CQLITE_DATASETS_ROOT";

/// Environment override for the **committed** schema fixtures. Exists only for
/// out-of-tree runs (a packaged corpus + schemas shipped together); the default is
/// checkout-relative and needs no environment at all.
pub const SCHEMAS_ROOT_ENV: &str = "CQLITE_SCHEMAS_ROOT";

/// How [`schemas_root_resolved`] arrived at its path — carried into panic messages so
/// an operator can tell an override apart from the checkout default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemasRootSource {
    /// `CQLITE_SCHEMAS_ROOT` was set, non-empty, and named a readable directory.
    EnvOverride,
    /// Checkout-relative: the `test-data/schemas` of the enclosing checkout.
    Checkout,
}

impl SchemasRootSource {
    fn describe(self) -> String {
        match self {
            Self::EnvOverride => format!("{SCHEMAS_ROOT_ENV} override"),
            Self::Checkout => format!(
                "checkout-relative (CARGO_MANIFEST_DIR={})",
                env!("CARGO_MANIFEST_DIR")
            ),
        }
    }
}

/// A non-empty env var value, or `None`. An empty value is treated as unset: an
/// exported-but-empty variable is a scripting accident, never an intentional root.
fn env_dir(key: &str) -> Option<PathBuf> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(PathBuf::from(v)),
        _ => None,
    }
}

/// The `test-data` directory of the enclosing checkout.
///
/// Walks up from `CARGO_MANIFEST_DIR` (expanded at compile time to the *consuming
/// crate's* directory) and returns the first ancestor holding a `test-data/schemas`
/// directory. Walking ancestors — rather than hardcoding `../test-data` — keeps this
/// correct for a crate nested deeper than one level (e.g. `bindings/python`), and it
/// never constructs a `..` component, so no path handed to the kernel can be
/// re-rooted by a symlink.
///
/// When no ancestor qualifies (a checkout with the fixtures deleted), returns the
/// canonical one-level-up guess so callers can name a concrete absolute path in their
/// error message instead of reporting "not found" with no path at all.
pub fn checkout_test_data_dir() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    for ancestor in manifest.ancestors() {
        let candidate = ancestor.join("test-data");
        if candidate.join("schemas").is_dir() {
            return candidate;
        }
    }
    manifest.join("../test-data")
}

/// The **fetched** dataset corpus root — infallible shape. See the module docs for
/// the contract; prefer [`datasets_root_if_present`] in a SKIP-gated test.
pub fn datasets_root() -> PathBuf {
    env_dir(DATASETS_ROOT_ENV).unwrap_or_else(|| checkout_test_data_dir().join("datasets"))
}

/// The **fetched** dataset corpus root — fallible shape, for SKIP-gated tests.
///
/// `Some` only when `CQLITE_DATASETS_ROOT` is set and names a directory. Deliberately
/// has **no** checkout fallback: a test that skips when the corpus is unavailable must
/// not instead run against a checkout that carries only committed byte-parity
/// references and report a vacuous 0-row pass.
pub fn datasets_root_if_present() -> Option<PathBuf> {
    let p = env_dir(DATASETS_ROOT_ENV)?;
    p.is_dir().then_some(p)
}

/// The `sstables/` subtree of the dataset corpus.
pub fn sstables_root() -> PathBuf {
    datasets_root().join("sstables")
}

/// The **committed** schema-fixture root plus the provenance of that decision.
///
/// `CQLITE_SCHEMAS_ROOT` wins when set, non-empty, and readable — an unreadable or
/// empty override falls through to the checkout rather than pinning the run to a path
/// that cannot work, so a stale export in a shell profile degrades to the correct
/// answer instead of breaking every fixture load.
pub fn schemas_root_resolved() -> (PathBuf, SchemasRootSource) {
    if let Some(p) = env_dir(SCHEMAS_ROOT_ENV) {
        if p.is_dir() {
            return (p, SchemasRootSource::EnvOverride);
        }
    }
    (
        checkout_test_data_dir().join("schemas"),
        SchemasRootSource::Checkout,
    )
}

/// The **committed** schema-fixture root (`test-data/schemas`).
///
/// NEVER derived from [`datasets_root`] — see the module docs (#3148 AC (h)).
pub fn schemas_root() -> PathBuf {
    schemas_root_resolved().0
}

/// Absolute path to a committed `.cql`/`.json` schema fixture, verified readable.
///
/// Panics with an actionable message naming the resolved **absolute** path, how the
/// root was chosen, and the remedy — never a bare `Path does not exist:` from deep
/// inside ingestion, which is the diagnosis-free failure #3148 was filed for. Test
/// and bench code may panic; this file is never compiled into the library.
pub fn schema_path(schema_file: &str) -> PathBuf {
    let (root, source) = schemas_root_resolved();
    let path = root.join(schema_file);
    if path.is_file() {
        return path;
    }
    panic!(
        "committed schema fixture '{schema_file}' is not readable at {}\n\
         \x20 schemas root : {} ({})\n\
         \x20 note         : test-data/schemas is COMMITTED SOURCE — it is NOT part of the fetched\n\
         \x20                dataset corpus and is NOT derived from {DATASETS_ROOT_ENV} (#3148).\n\
         \x20 remedy       : unset {SCHEMAS_ROOT_ENV} to use the checkout default, or restore the\n\
         \x20                committed fixtures:  git restore --source=HEAD -- test-data/schemas",
        path.display(),
        root.display(),
        source.describe(),
    )
}

/// Assert every named schema fixture is readable, returning the first failure as an
/// actionable message (the `Err` shape of [`schema_path`], for a caller that wants to
/// report rather than panic).
pub fn check_schema_files(files: &[&str]) -> Result<(), String> {
    let (root, source) = schemas_root_resolved();
    for f in files {
        let path = root.join(f);
        if !path.is_file() {
            return Err(format!(
                "schema fixture '{f}' unreadable at {} (root {} via {})",
                path.display(),
                root.display(),
                source.describe()
            ));
        }
    }
    Ok(())
}
