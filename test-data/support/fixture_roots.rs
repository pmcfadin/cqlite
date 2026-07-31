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
//! disappears rather than being papered over — nothing here climbs `..` from the
//! datasets root, so there is nothing left to mis-resolve.
//!
//! # Two rules that keep the resolution honest
//!
//! * **The checkout is identified by a checkout MARKER, not by the fixtures.** Keying
//!   the ancestor walk on `test-data/schemas` looked convenient and was wrong: a sparse
//!   checkout, or a worktree created *inside* another checkout, would skip past its own
//!   root and silently resolve BOTH roots to the OUTER checkout's `test-data` —
//!   wrong-but-existing fixtures, reported as `Checkout`, no warning. That is exactly
//!   the failure class this module exists to eliminate, so the walk keys on the
//!   workspace-root `Cargo.toml` (the nearest ancestor manifest declaring
//!   `[workspace]`) and a missing `test-data/schemas` under it fails LOUDLY at
//!   [`schema_path`] instead of being papered over by a neighbour's copy.
//! * **A `CQLITE_SCHEMAS_ROOT` override MUST be absolute.** A *relative* override is
//!   rejected fail-closed rather than resolved, because it cannot mean the same thing on
//!   both sides of the contract: `scripts/agent-gate.sh` evaluates it with CWD =
//!   repository root, while cargo runs each test binary with CWD = the *package*
//!   directory. A relative value therefore let the gate stamp
//!   `schemas: 6/6 … under packaged/schemas` while the tests silently read the
//!   checkout's schemas — the block certifying root A for a run that used root B, i.e.
//!   the "positively misleading `STATUS: OK`" defect of #3148 reintroduced by its own
//!   fix. Rejecting relative values makes the two sides agree by construction.
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
    /// `CQLITE_SCHEMAS_ROOT` was set, ABSOLUTE, and named a readable directory.
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
fn env_dir(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

/// True when `p` is a regular file that can actually be OPENED for reading.
///
/// `Path::is_file()` alone answers the type question, not the permission question: a
/// mode-000 fixture is `is_file() == true` and then fails inside ingestion, bypassing the
/// actionable message this module exists to produce — and disagreeing with the gate's
/// `[ -f ] && [ -r ]` check, which is the drift the whole change is meant to prevent
/// (roborev job 8, finding 2). Both sides now answer "readable regular file".
pub fn readable_file(p: &Path) -> bool {
    p.is_file() && std::fs::File::open(p).is_ok()
}

/// The workspace root: the NEAREST ancestor of `CARGO_MANIFEST_DIR` whose `Cargo.toml`
/// declares `[workspace]`.
///
/// A checkout MARKER, deliberately — not the fixtures themselves. Keying the walk on
/// `test-data/schemas` (the first cut of this module) meant a sparse checkout or a
/// worktree nested inside another checkout resolved to the OUTER checkout's `test-data`:
/// wrong-but-existing fixtures, no warning. `[workspace]` is present in the repository
/// root manifest and absent from every member manifest, so the NEAREST match is always
/// the enclosing checkout's own root — nesting depth and fixture presence are both
/// irrelevant to it.
fn workspace_root() -> Option<PathBuf> {
    for ancestor in Path::new(env!("CARGO_MANIFEST_DIR")).ancestors() {
        let manifest = ancestor.join("Cargo.toml");
        if let Ok(text) = std::fs::read_to_string(&manifest) {
            if text
                .lines()
                .any(|l| l.trim_start().starts_with("[workspace]"))
            {
                return Some(ancestor.to_path_buf());
            }
        }
    }
    None
}

/// The `test-data` directory of the enclosing checkout.
///
/// Anchored on [`workspace_root`]. The result is returned **whether or not it exists**,
/// so a checkout missing its fixtures fails LOUDLY at [`schema_path`] — naming its OWN
/// absolute path — rather than being silently satisfied by a neighbouring checkout's copy.
///
/// Falls back to the manifest's parent only when no `[workspace]` manifest is found at
/// all (not reachable from a cargo-built target in this repository). Both branches use
/// `Path::parent`/`join` on the absolute `CARGO_MANIFEST_DIR`, so no `..` component is
/// ever constructed and nothing handed to the kernel can be re-rooted by a symlink.
pub fn checkout_test_data_dir() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root =
        workspace_root().unwrap_or_else(|| manifest.parent().unwrap_or(manifest).to_path_buf());
    root.join("test-data")
}

/// The **fetched** dataset corpus root — infallible shape. See the module docs for
/// the contract; prefer [`datasets_root_if_present`] in a SKIP-gated test.
pub fn datasets_root() -> PathBuf {
    env_dir(DATASETS_ROOT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| checkout_test_data_dir().join("datasets"))
}

/// The **fetched** dataset corpus root — fallible shape, for SKIP-gated tests.
///
/// `Some` only when `CQLITE_DATASETS_ROOT` is set and names a directory. Deliberately
/// has **no** checkout fallback: a test that skips when the corpus is unavailable must
/// not instead run against a checkout that carries only committed byte-parity
/// references and report a vacuous 0-row pass.
pub fn datasets_root_if_present() -> Option<PathBuf> {
    let p = PathBuf::from(env_dir(DATASETS_ROOT_ENV)?);
    p.is_dir().then_some(p)
}

/// The `sstables/` subtree of the dataset corpus.
pub fn sstables_root() -> PathBuf {
    datasets_root().join("sstables")
}

/// PURE resolution of the schemas root from a raw override value.
///
/// Separated from the environment read so the contract is testable without mutating
/// process-global state (an env-mutating test races every other test in the binary).
/// Mirrors `_gate_schemas_root` in `scripts/agent-gate.sh` rule for rule:
///
/// | raw override | result |
/// |---|---|
/// | absent / blank | `Ok(checkout)` — an exported-but-empty var is a scripting accident |
/// | **relative** | **`Err`** — cannot mean the same thing under two CWDs (see module docs) |
/// | absolute + readable dir | `Ok(override)` |
/// | absolute, not a dir | `Ok(checkout)` — a stale export degrades instead of breaking every load |
pub fn resolve_schemas_root(
    raw_override: Option<&str>,
) -> Result<(PathBuf, SchemasRootSource), String> {
    let checkout = || {
        (
            checkout_test_data_dir().join("schemas"),
            SchemasRootSource::Checkout,
        )
    };
    let Some(raw) = raw_override.filter(|v| !v.trim().is_empty()) else {
        return Ok(checkout());
    };
    let p = PathBuf::from(raw);
    if !p.is_absolute() {
        return Err(format!(
            "{SCHEMAS_ROOT_ENV} must be an ABSOLUTE path, got '{raw}'.\n\
             \x20 why    : a relative value cannot mean the same thing on both sides of the\n\
             \x20          contract — scripts/agent-gate.sh evaluates it with CWD = repository\n\
             \x20          root, while cargo runs each test binary with CWD = the package dir.\n\
             \x20          The gate would certify one schemas root while the tests read another.\n\
             \x20 remedy : export an absolute path, or unset {SCHEMAS_ROOT_ENV} to use the\n\
             \x20          checkout's test-data/schemas."
        ));
    }
    if p.is_dir() {
        return Ok((p, SchemasRootSource::EnvOverride));
    }
    Ok(checkout())
}

/// The **committed** schema-fixture root plus the provenance of that decision.
///
/// Reads `CQLITE_SCHEMAS_ROOT` and applies [`resolve_schemas_root`]. Panics on a
/// REJECTED override (a relative path) — fail-closed is the point: silently resolving it
/// is what would let the gate certify a root the run never used.
pub fn schemas_root_resolved() -> (PathBuf, SchemasRootSource) {
    match resolve_schemas_root(env_dir(SCHEMAS_ROOT_ENV).as_deref()) {
        Ok(v) => v,
        Err(e) => panic!("{e}"),
    }
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
    if readable_file(&path) {
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
