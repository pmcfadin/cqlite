//! Generator: operator-facing Flight metrics reference (issue #2426).
//!
//! Renders the operator metrics reference FROM the observability catalog
//! (`cqlite-core/src/observability/{catalog,operator_docs}.rs`) into two
//! committed artifacts:
//!   - `docs/reports/flight-metrics-reference.md` — the canonical report, and
//!   - `website/src/content/docs/agents-using/flight-metrics-reference.md` — the
//!     published docs-site page (same render + Starlight front matter).
//!
//! Mirrors the #1338 parity-report derived-artifact pattern: deterministic output
//! + a `--check` mode so a gate can fail on a stale committed file.
//!
//! Usage (from the repo root):
//! ```bash
//! # Regenerate both committed artifacts:
//! cargo run -p cqlite-core --example gen_operator_metrics_doc
//! # Verify the committed artifacts match a fresh render (non-zero on drift):
//! cargo run -p cqlite-core --example gen_operator_metrics_doc -- --check
//! ```
//! The catalog (and thus this generator) is always compiled — no feature flag
//! is required.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use cqlite_core::observability::operator_docs::{
    render_markdown, render_website_markdown, COMMITTED_DOC_REL, WEBSITE_DOC_REL,
};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is cqlite-core/; committed artifacts are repo-root-relative.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent (repo root)")
        .to_path_buf()
}

/// Write or check one artifact. Returns Ok(()) on success, Err(message) otherwise.
fn handle(path: &Path, rendered: &str, check: bool) -> Result<String, String> {
    if check {
        match std::fs::read_to_string(path) {
            Ok(current) if current == rendered => Ok(format!("FRESH ({})", path.display())),
            Ok(_) => Err(format!(
                "STALE — {} does not match a fresh render.\n\
                 Regenerate: cargo run -p cqlite-core --example gen_operator_metrics_doc",
                path.display()
            )),
            Err(e) => Err(format!("cannot read committed doc {}: {e}", path.display())),
        }
    } else {
        std::fs::write(path, rendered)
            .map(|()| format!("wrote {}", path.display()))
            .map_err(|e| format!("cannot write {}: {e}", path.display()))
    }
}

fn main() -> ExitCode {
    let check = std::env::args().any(|a| a == "--check");

    let report = match render_markdown() {
        Ok(md) => md,
        Err(e) => {
            eprintln!("operator-metrics-doc: generation FAILED (fail-closed): {e}");
            return ExitCode::FAILURE;
        }
    };
    let website = match render_website_markdown() {
        Ok(md) => md,
        Err(e) => {
            eprintln!("operator-metrics-doc: website generation FAILED (fail-closed): {e}");
            return ExitCode::FAILURE;
        }
    };

    let root = repo_root();
    let mut failed = false;
    for (rel, rendered) in [(COMMITTED_DOC_REL, &report), (WEBSITE_DOC_REL, &website)] {
        match handle(&root.join(rel), rendered, check) {
            Ok(msg) => println!("operator-metrics-doc: {msg}"),
            Err(msg) => {
                eprintln!("operator-metrics-doc: {msg}");
                failed = true;
            }
        }
    }

    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
