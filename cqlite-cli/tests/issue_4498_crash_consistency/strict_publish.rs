//! The `Scope::CassandraStrict` assertions (#4529), split out to keep the harness under the
//! test file-size threshold.

use super::*;

/// Whether a path (relative to `out/`) lies inside a `.cqlite-publish-*.tmp` staging directory.
pub(super) fn is_stage_path(p: &Path) -> bool {
    p.components().any(|c| {
        c.as_os_str()
            .to_str()
            .is_some_and(|n| n.starts_with(".cqlite-publish-") && n.ends_with(".tmp"))
    })
}

/// `#4529` contract for one crashed run (see the `Scope::CassandraStrict` call site).
pub(super) fn strict_publish_contract(
    tag: &str,
    a: &Area<'_>,
    reference: &BTreeMap<PathBuf, Vec<u8>>,
    failures: &mut Vec<String>,
    verb: Verb,
    f: &Fixture,
) {
    let is_stage = |p: &Path| is_stage_path(p);
    let after = output_set(a);
    let finals: BTreeMap<_, _> = after.iter().filter(|(p, _)| !is_stage(p)).collect();
    let want: BTreeMap<_, _> = reference.iter().collect();
    let complete = finals == want;
    if !complete
        && finals.keys().any(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
    {
        failures.push(format!(
            "{tag}: a final-named Data.db exists in a partial set ({} final file(s)) -> Cassandra loads it",
            finals.len()
        ));
    }
    let unnamed: Vec<_> = finals
        .keys()
        .filter(|p| !reference.contains_key(**p))
        .collect();
    if !unnamed.is_empty() {
        failures.push(format!("{tag}: unidentifiable leftover(s) outside a `.cqlite-publish-*.tmp` stage: {unnamed:?}"));
    }
    if !complete && !out_is_genuinely_empty(a) {
        let again = run_verb(verb, f, a, Some((shim(), None)));
        let text = again.text();
        if again.code() == Some(0)
            || !(text.contains("not empty") || text.contains(".cqlite-publish-"))
        {
            failures.push(format!(
                "{tag}: re-run into the leftovers must refuse naming the cause; got code={:?}: {}",
                again.code(),
                text.lines().last().unwrap_or("")
            ));
        }
    }
}

/// Whether an uninterrupted run is expected to contain a `rename` boundary (the atomic-publication
/// step): `true` since #4529 publishes by rename.
const EXPECT_RENAME: bool = true;

/// The enumerated boundaries must contain every syscall CATEGORY an SSTable write verb performs,
/// per output file, so a shim that stopped seeing (say) `copy_file_range` or vectored writes
/// cannot shrink the matrix unnoticed behind a "enough boundaries" count:
/// every output component file needs a `create` and, when non-empty, at least one `write`/`pwrite`/`copy`, the
/// manifest needs the same, and the run needs >= 1 `mkdir`, >= 1 `fsync` (and a `rename` when
/// [`EXPECT_RENAME`]).
pub(super) fn assert_expected_categories(
    verb: Verb,
    bounds: &[(String, String)],
    reference: &BTreeMap<PathBuf, Vec<u8>>,
) {
    // Components are created/written in the `.cqlite-publish-*.tmp` stage (#4529), so match the
    // staged path; the manifest is written directly.
    let has = |kinds: &[&str], path: &str| {
        let base = path.rsplit('/').next().unwrap_or(path);
        bounds.iter().any(|(k, p)| {
            kinds.contains(&k.as_str())
                && (p == path
                    || (path.starts_with("out/")
                        && p.contains("/.cqlite-publish-")
                        && (p.ends_with(&format!(".tmp/{base}"))
                            || path
                                .strip_prefix("out/")
                                .is_some_and(|rel| p.ends_with(&format!(".tmp/{rel}"))))))
        })
    };
    let mut files: Vec<(String, bool)> = reference
        .iter()
        .map(|(rel, bytes)| (format!("out/{}", rel.display()), !bytes.is_empty()))
        .collect();
    files.push(("manifest.json".to_string(), true));
    for (f, non_empty) in &files {
        assert!(
            has(&["create"], f),
            "{verb:?}: no `create` boundary enumerated for {f} — shim blind to file creation?"
        );
        // A legitimately EMPTY output (e.g. an empty BTI Rows.db) has a create and no write.
        assert!(
            !non_empty || has(&["write", "pwrite", "copy"], f),
            "{verb:?}: no write-class boundary (write/pwrite/copy) enumerated for {f} — shim blind \
             to the call this writer uses?"
        );
    }
    let any = |kind: &str| bounds.iter().any(|(k, _)| k == kind);
    assert!(any("mkdir"), "{verb:?}: no `mkdir` boundary enumerated");
    assert!(any("fsync"), "{verb:?}: no `fsync` boundary enumerated");
    if EXPECT_RENAME {
        assert!(any("rename"), "{verb:?}: no `rename` boundary enumerated");
    }
}
