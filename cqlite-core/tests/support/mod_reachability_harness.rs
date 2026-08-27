//! Shared test harness for the `mod`-reachability guard (issue #1714).
//!
//! Two helpers, used by every `issue_1714_mod_reachability*` integration test:
//!
//! * [`ScratchCrate`] — a throwaway crate-shaped directory tree, so the detector's teeth
//!   are proven against a tree the test controls end to end rather than against the live
//!   `cqlite-core/src` (whose orphan set changes as #1715 / #3364 / #3365 land);
//! * [`stripped`] — the sanitizer at its own boundary.
//!
//! It lives in `support/` (not inlined in one test file) because each integration test is
//! its OWN crate: without a shared file, a second test file must copy the harness, and a
//! copy is where the two drift apart. Every includer declares both modules:
//!
//! ```ignore
//! #[path = "support/mod_reachability.rs"]
//! mod mod_reachability;
//! #[path = "support/mod_reachability_harness.rs"]
//! mod harness;
//! ```

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::mod_reachability::{analyze, strip_comments_and_strings, ModuleGraphSpec, Report};

/// Sanitize `src` and return the blanked text, panicking on a refusal.
///
/// Tests that WANT the refusal call [`strip_comments_and_strings`] directly and assert on
/// the `Err` — a helper that swallowed it would turn a fail-closed refusal into a pass.
pub fn stripped(src: &str) -> String {
    strip_comments_and_strings(src).unwrap_or_else(|e| panic!("sanitize failed: {e}"))
}

const SRC_DIR: &str = "src";

static SCRATCH_SEQ: AtomicUsize = AtomicUsize::new(0);

/// A throwaway crate-shaped directory tree under the OS temp dir.
pub struct ScratchCrate {
    dir: PathBuf,
}

impl ScratchCrate {
    pub fn new(label: &str) -> Self {
        let seq = SCRATCH_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "cqlite-1714-mod-reach-{}-{seq}-{label}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("cannot create {}: {e}", dir.display()));
        Self { dir }
    }

    /// The crate directory itself — for the few cases that must build something the
    /// `write` helper cannot express (a symlink, a directory outside `src`).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn write(&self, rel: &str, contents: &str) {
        let path = self.dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("cannot create {}: {e}", parent.display()));
        }
        fs::write(&path, contents)
            .unwrap_or_else(|e| panic!("cannot write {}: {e}", path.display()));
    }

    pub fn spec(&self) -> ModuleGraphSpec {
        ModuleGraphSpec {
            crate_dir: self.dir.clone(),
            root_file_rel: "src/lib.rs".to_string(),
            src_dir_rel: SRC_DIR.to_string(),
        }
    }

    pub fn analyze(&self) -> Report {
        analyze(&self.spec()).unwrap_or_else(|cause| {
            panic!(
                "walk of scratch crate {} failed: {cause}",
                self.dir.display()
            )
        })
    }

    /// Assert the walk fails closed and return the cause.
    pub fn expect_failure(&self) -> String {
        match analyze(&self.spec()) {
            Ok(report) => panic!(
                "expected a FAIL-CLOSED refusal, got a report (orphans={:?}) — a skip-and-continue \
                 here IS the vacuous pass",
                report.orphans
            ),
            Err(cause) => cause,
        }
    }
}

impl Drop for ScratchCrate {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
