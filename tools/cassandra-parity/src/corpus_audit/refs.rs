//! Missing reference classification for the corpus audit (design D3).
//!
//! Only manifest references that point INTO the regenerated corpus tree
//! (`test-data/datasets/...`) are audited here; arbitrary repo files (test
//! sources, scripts) are covered by the manifest linter's path-existence check.
//!
//! The lane audits a FRESHLY regenerated corpus: `regenerate-datasets.sh`
//! `rm -rf`s the corpus and re-creates each table via `CREATE TABLE`, so
//! Cassandra mints a NEW random table UUID for each table every run. The
//! committed manifest references pin OLD UUID directories, so a healthy
//! reference almost never matches by exact path. Classification is therefore by
//! the SAME UUID-independent identity [`component_identity`] the sibling
//! component-change audit uses — never by raw path:
//!   * **OK** when its exact path is present in the regenerated corpus, OR when
//!     a regenerated file shares its `(table_key, basename)` identity (the
//!     manifest merely pins an obsolete table-UUID dir — pure per-run churn).
//!   * **Missing** only on GENUINE disappearance: no regenerated file shares the
//!     reference's table+component identity at all.

use std::collections::BTreeSet;

use crate::model::{Manifest, Scenario};

use super::{AuditFinding, CorpusInventory, FindingKind};

/// Only references under this prefix are part of the regenerated corpus.
pub const CORPUS_PREFIX: &str = "test-data/datasets/";

/// Classify every corpus-tree reference of every non-`planned` scenario.
///
/// A reference is a [`FindingKind::MissingReference`] ONLY when no regenerated
/// file shares its UUID-independent [`component_identity`]. A reference whose
/// identity IS present — even under a churned `<table>-<uuid>` directory the
/// manifest does not pin — produces ZERO findings, matching the churn-tolerant
/// contract of [`super::check_component_changes`] (issue #1026).
pub fn check_references(manifest: &Manifest, inventory: &CorpusInventory) -> Vec<AuditFinding> {
    // UUID-independent identity of every regenerated file, so a reference pinned
    // to an obsolete table-UUID dir still resolves to its churned twin.
    let produced: BTreeSet<String> = inventory
        .files
        .iter()
        .map(|f| component_identity(f))
        .collect();

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
            // Exact path present, or the same table+component exists under a
            // churned UUID dir -> the corpus still produces it; not a finding.
            if inventory.files.contains(&r) || produced.contains(&component_identity(&r)) {
                continue;
            }
            // Genuine disappearance: no regenerated file shares this reference's
            // table+component identity.
            out.push(AuditFinding::new(
                FindingKind::MissingReference,
                r.clone(),
                format!(
                    "scenario {} references a corpus component the regeneration did not produce \
                     (no regenerated file shares its table+component identity)",
                    s.id
                ),
            ));
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

/// True when any `/`-separated segment of `path` names a Cassandra system
/// keyspace — exactly `system` or any `system_*` (covers `system`,
/// `system_schema`, `system_auth`, `system_distributed`, `system_traces`,
/// `system_views`, `system_virtual_schema`). The COVERAGE/PRESENCE audit
/// excludes these from the expected inventory because a system keyspace's
/// on-disk contents are inherently run-dependent and must not red the lane
/// (issue #2009).
pub fn is_system_keyspace_path(path: &str) -> bool {
    path.split('/')
        .any(|seg| seg == "system" || seg.starts_with("system_"))
}

/// A UUID-independent identity for a single component file: its [`table_key`]
/// (parent directory with the trailing `-<uuid>` stripped) joined with its
/// basename. So the same golden under `simple_table-<uuidA>/nb-1-big-Data.db.jsonl`
/// and `simple_table-<uuidB>/nb-1-big-Data.db.jsonl` share one identity. The
/// component-change audit (issue #1026) keys both the committed-expected and the
/// regenerated-actual inventories by this identity, so the per-run table-UUID
/// churn is not mistaken for a presence/checksum change.
pub fn component_identity(path: &str) -> String {
    let (parent, base) = split_path(path);
    let key = table_key(parent);
    if key.is_empty() {
        base.to_string()
    } else {
        format!("{key}/{base}")
    }
}

/// Split a `/`-separated path into `(parent, basename)`. The parent is `""` for a
/// top-level path. Shared with [`super`]'s component-change audit so the
/// parent-of-a-path rule lives in exactly one place (issue #1026).
pub(crate) fn split_path(p: &str) -> (&str, &str) {
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
