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
            // A DIRECTORY / keyspace reference (e.g. `.../sstables/test_tomb` or the
            // corpus root `.../sstables`) names no single component file, so it can
            // never match by component identity. Under the coverage/presence
            // contract it is satisfied when the regeneration produced ANY file
            // under it (issue #2009).
            if reference_is_satisfied_directory(&r, &inventory.files) {
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

/// True when `reference` names a directory/keyspace under the corpus that the
/// regeneration populated — i.e. some produced file lives strictly under it. Used
/// so a scenario that references a keyspace dir (or the corpus root) rather than a
/// single component file is satisfied by coverage of that directory (issue #2009).
fn reference_is_satisfied_directory(reference: &str, files: &BTreeSet<String>) -> bool {
    let prefix = format!("{}/", reference.trim_end_matches('/'));
    files.iter().any(|f| f.starts_with(&prefix))
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

/// A UUID- AND generation-independent identity for a single component file: its
/// [`table_key`] (parent directory with the trailing `-<uuid>` stripped) joined
/// with its [`normalize_basename_generation`]-normalized basename. So the same
/// golden under `simple_table-<uuidA>/nb-1-big-Data.db.jsonl` and
/// `simple_table-<uuidB>/nb-2-big-Data.db.jsonl` share one identity. The
/// component-change + missing-reference audits (issues #1026, #2009) key both the
/// committed-expected and the regenerated-actual inventories by this identity, so
/// neither the per-run table-UUID churn NOR the per-run SSTable generation number
/// (a fresh regen flushes/compacts to a different generation, e.g. `nb-1-big` ->
/// `nb-2-big`) is mistaken for a presence change.
pub fn component_identity(path: &str) -> String {
    let (parent, base) = split_path(path);
    let key = table_key(parent);
    let base = normalize_basename_generation(base);
    if key.is_empty() {
        base
    } else {
        format!("{key}/{base}")
    }
}

/// Normalize the per-run SSTable **generation** number out of a component
/// basename, so a golden is identified by (table, version, format, component)
/// regardless of which generation Cassandra assigned this run. A Cassandra 5.0
/// SSTable descriptor filename is `<version>-<generation>-<format>-<component>`
/// (e.g. `nb-1-big-Data.db`, `oa-2-big-Statistics.db.txt`, `da-2-bti-Data.db.jsonl`);
/// only the `<generation>` digits vary across a fresh regeneration. This replaces
/// those digits with a fixed `<gen>` token. A basename that does NOT match the
/// descriptor shape (version = 2 ascii-alnum, generation = digits, format =
/// `big`|`bti`) is returned UNCHANGED, so non-SSTable files (schemas, manifests)
/// are never rewritten (issue #2009).
pub fn normalize_basename_generation(base: &str) -> String {
    let parts: Vec<&str> = base.splitn(4, '-').collect();
    if parts.len() == 4
        && parts[0].len() == 2
        && parts[0].chars().all(|c| c.is_ascii_alphanumeric())
        && !parts[1].is_empty()
        && parts[1].chars().all(|c| c.is_ascii_digit())
        && (parts[2] == "big" || parts[2] == "bti")
    {
        format!("{}-<gen>-{}-{}", parts[0], parts[2], parts[3])
    } else {
        base.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_basename_generation_strips_only_the_generation() {
        for (input, want) in [
            ("nb-1-big-Data.db", "nb-<gen>-big-Data.db"),
            ("nb-2-big-Data.db.jsonl", "nb-<gen>-big-Data.db.jsonl"),
            (
                "oa-2-big-Statistics.db.txt",
                "oa-<gen>-big-Statistics.db.txt",
            ),
            ("da-61-bti-TOC.txt", "da-<gen>-bti-TOC.txt"),
            ("nb-45-big-Digest.crc32", "nb-<gen>-big-Digest.crc32"),
        ] {
            assert_eq!(normalize_basename_generation(input), want, "input {input}");
        }
        // Non-descriptor basenames are returned unchanged.
        for unchanged in ["schema.cql", "metadata.yml", "manifest-v1.yml", "TOC.txt"] {
            assert_eq!(normalize_basename_generation(unchanged), unchanged);
        }
    }

    #[test]
    fn component_identity_is_uuid_and_generation_independent() {
        let committed =
            "test-data/datasets/sstables/test_basic/simple_table-aaaa000000000000000000000000ffff/nb-1-big-Data.db.jsonl";
        let regenerated =
            "test-data/datasets/sstables/test_basic/simple_table-bbbb000000000000000000001111ffff/nb-2-big-Data.db.jsonl";
        assert_eq!(
            component_identity(committed),
            component_identity(regenerated)
        );
        // A different component under the same table has a DIFFERENT identity.
        let other =
            "test-data/datasets/sstables/test_basic/simple_table-bbbb000000000000000000001111ffff/nb-2-big-Index.db";
        assert_ne!(component_identity(committed), component_identity(other));
    }

    #[test]
    fn directory_reference_satisfied_only_when_a_file_lives_under_it() {
        let mut files = BTreeSet::new();
        files.insert("test-data/datasets/sstables/test_tomb/tt-abcd/nb-1-big-Data.db".to_string());
        // A keyspace-dir reference is satisfied by a file strictly under it.
        assert!(reference_is_satisfied_directory(
            "test-data/datasets/sstables/test_tomb",
            &files
        ));
        // The corpus root is satisfied too.
        assert!(reference_is_satisfied_directory(
            "test-data/datasets/sstables",
            &files
        ));
        // A sibling keyspace with no produced file is NOT satisfied.
        assert!(!reference_is_satisfied_directory(
            "test-data/datasets/sstables/test_absent",
            &files
        ));
        // A prefix that is not a path-boundary parent must NOT match.
        assert!(!reference_is_satisfied_directory(
            "test-data/datasets/sstables/test_to",
            &files
        ));
    }
}
