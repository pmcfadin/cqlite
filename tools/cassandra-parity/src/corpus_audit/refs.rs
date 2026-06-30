//! Missing/stale reference classification for the corpus audit (design D3).
//!
//! Only manifest references that point INTO the regenerated corpus tree
//! (`test-data/datasets/...`) are audited here; arbitrary repo files (test
//! sources, scripts) are covered by the manifest linter's path-existence check.
//!
//! A reference is:
//!   * **OK** when its exact path is present in the regenerated corpus.
//!   * **Stale** when the corpus still produces the same table+component, but
//!     only under a *different* generation directory — i.e. the manifest pins an
//!     obsolete table-UUID dir (the common real-world drift: every regeneration
//!     mints fresh table UUIDs).
//!   * **Missing** when no regenerated component matches the table+component at all.

use std::collections::BTreeSet;

use crate::model::{Manifest, Scenario};

use super::{AuditFinding, CorpusInventory, FindingKind};

/// Only references under this prefix are part of the regenerated corpus.
const CORPUS_PREFIX: &str = "test-data/datasets/";

/// Classify every corpus-tree reference of every non-`planned` scenario.
pub fn check_references(manifest: &Manifest, inventory: &CorpusInventory) -> Vec<AuditFinding> {
    // (table_key, basename) of every regenerated file, for stale-vs-missing.
    let mut produced: BTreeSet<(String, String)> = BTreeSet::new();
    for f in &inventory.files {
        let (parent, base) = split_path(f);
        produced.insert((table_key(parent), base.to_string()));
    }

    let mut out = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for s in &manifest.scenarios {
        if s.status == "planned" {
            continue;
        }
        for r in corpus_refs(s) {
            if !seen.insert(r.clone()) {
                continue;
            }
            if inventory.files.contains(&r) {
                continue;
            }
            let (parent, base) = split_path(&r);
            let key = (table_key(parent), base.to_string());
            if produced.contains(&key) {
                out.push(AuditFinding::new(
                    FindingKind::StaleReference,
                    r.clone(),
                    format!(
                        "scenario {} pins a generation dir the fresh corpus replaced \
                         (same table+component exists under a new directory)",
                        s.id
                    ),
                ));
            } else {
                out.push(AuditFinding::new(
                    FindingKind::MissingReference,
                    r.clone(),
                    format!(
                        "scenario {} references a corpus component the regeneration did not produce",
                        s.id
                    ),
                ));
            }
        }
    }
    out
}

/// Collect a scenario's references that live under the regenerated corpus tree.
fn corpus_refs(s: &Scenario) -> Vec<String> {
    let mut v = Vec::new();
    for r in &s.fixtures.references {
        if is_corpus(r) {
            v.push(r.clone());
        }
    }
    for r in &s.fixtures.datasets {
        if is_corpus(r) {
            v.push(r.clone());
        }
    }
    for r in &s.evidence.reference_paths {
        if is_corpus(r) {
            v.push(r.clone());
        }
    }
    v
}

fn is_corpus(path: &str) -> bool {
    path.starts_with(CORPUS_PREFIX)
}

/// Split a `/`-separated path into `(parent, basename)`.
fn split_path(p: &str) -> (&str, &str) {
    match p.rfind('/') {
        Some(i) => (&p[..i], &p[i + 1..]),
        None => ("", p),
    }
}

/// A UUID-independent key for the table a component lives in: the parent dir with
/// the trailing `-<uuid>` stripped from its last segment. So
/// `.../test_basic/simple_table-<uuidA>` and `.../test_basic/simple_table-<uuidB>`
/// share the key `.../test_basic/simple_table`.
fn table_key(parent: &str) -> String {
    let (grand, last) = split_path(parent);
    let stripped = strip_uuid_suffix(last);
    if grand.is_empty() {
        stripped.to_string()
    } else {
        format!("{grand}/{stripped}")
    }
}

/// Strip a trailing `-<hex>` UUID suffix (>= 16 hex chars) from a directory
/// segment; otherwise return it unchanged.
fn strip_uuid_suffix(seg: &str) -> &str {
    if let Some(i) = seg.rfind('-') {
        let suffix = &seg[i + 1..];
        if suffix.len() >= 16 && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            return &seg[..i];
        }
    }
    seg
}
