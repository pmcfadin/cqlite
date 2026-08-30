//! Locating and staging the fixture ONE case compares (issue #1491).
//!
//! Split out of `golden_value_compare.rs` under the campsite rule (CLAUDE.md,
//! epic #1135), which had reached the ~1500-line test-file target. A different
//! responsibility from the comparison itself: this half answers "which SSTable,
//! which golden, staged where", entirely from the filesystem, and knows nothing
//! about values.
//!
//! Re-exported by the parent module, so every existing `compare::golden_path` /
//! `compare::stage_single_table` call site is unchanged.

use super::super::fs_probe;
use std::path::{Path, PathBuf};

/// The `<table>-<uuid>` directory holding this table's SSTable under an ALREADY
/// CHOSEN `sstables/` root, or an error naming that root.
///
/// Choosing the root is the caller's job — see `super::fixture_root`, where a
/// git-committed case is pinned to the checkout copy and only a fetched-corpus case
/// walks the candidate roots by evidence (#1491 finding J1, #3220).
pub fn fixture_dir_in(root: &Path, keyspace: &str, table: &str) -> Result<PathBuf, String> {
    let mut dirs = fixture_dirs_in(root, keyspace, table)?;
    if dirs.is_empty() {
        return Err(format!(
            "no {table}-* directory with a *-Data.db under {}",
            root.join(keyspace).display()
        ));
    }
    Ok(dirs.remove(0))
}

/// EVERY `<table>-<uuid>` directory holding a `*-Data.db` under `root/keyspace`,
/// in sorted order.
///
/// Returned as the whole set, not just the first, so a caller that compares one of
/// them can COUNT the narrowing and declare it instead of picking silently (issue
/// #1491 review finding L3).
pub fn fixture_dirs_in(root: &Path, keyspace: &str, table: &str) -> Result<Vec<PathBuf>, String> {
    let ks_dir = root.join(keyspace);
    let prefix = format!("{table}-");
    // THREE-VALUED at every step (`super::super::fs_probe`), because each of the
    // four questions asked here used to collapse "I could not tell" onto "it is not
    // there": `read_dir`'s per-entry `Result` was dropped by
    // `.filter_map(Result::ok)`, `p.is_dir()` answers `false` for an entry it could
    // not describe, `to_str()` answers `None` for a name that is not UTF-8, and
    // `has_data_db` ended in `.unwrap_or(false)`. Under an ALREADY CHOSEN root the
    // consequence is one step milder than in `fixture_root` — both answers end the
    // search — but it is the same defect: the caller is told the root holds no such
    // table when nothing established that (issue #1491 review finding V1).
    let entries = fs_probe::dir_entries(&ks_dir)?
        .ok_or_else(|| format!("{}: no such directory", ks_dir.display()))?;
    let mut matches: Vec<PathBuf> = Vec::new();
    for entry in entries {
        if !fs_probe::name_starts_with(&entry.file_name(), &prefix) {
            continue;
        }
        let path = entry.path();
        if fs_probe::is_dir(&path)? && has_data_db(&path)? {
            matches.push(path);
        }
    }
    matches.sort();
    Ok(matches)
}

/// Does this directory hold a `*-Data.db`? `Ok(false)` only for a directory that
/// ANSWERED — one that has vanished, or one holding no such file.
fn has_data_db(dir: &Path) -> Result<bool, String> {
    let Some(entries) = fs_probe::dir_entries(dir)? else {
        return Ok(false);
    };
    Ok(entries
        .iter()
        .any(|e| fs_probe::name_ends_with(&e.file_name(), "-Data.db")))
}

/// The golden that describes the fixture's `*-Data.db`, PAIRED BY NAME:
/// `<gen>-Data.db` is described by `<gen>-Data.db.jsonl` and by no other file.
///
/// Two selections used to be silent here, and both could compare a CLI reading of
/// one SSTable against a dump of another (issue #1491 review finding L3):
///
///   * the lexicographically FIRST golden in the directory was taken, so a
///     directory holding `nb-1-…jsonl` next to a `nb-2-big-Data.db` compared the
///     wrong generation's dump — 26 committed fixture directories carry more than
///     one golden, so the shape is real even though no covered case has it today;
///   * a directory holding SEVERAL `*-Data.db` was accepted, and
///     [`stage_single_table`] copies the whole directory, so the CLI reads all of
///     them while one golden describes one. That is not narrowed coverage but an
///     unsound comparison, so it FAILS naming the files rather than being counted.
pub fn golden_path(fixture: &Path) -> Result<PathBuf, String> {
    let mut data_dbs: Vec<PathBuf> = Vec::new();
    let mut goldens: Vec<PathBuf> = Vec::new();
    let entries = fs_probe::dir_entries(fixture)?
        .ok_or_else(|| format!("{}: no such directory", fixture.display()))?;
    for entry in entries {
        // Classified on the NAME'S BYTES, and the `.jsonl` test comes first because
        // `<gen>-Data.db.jsonl` ends with neither suffix of the other: a name that
        // is not valid UTF-8 used to fall through `to_str()` into the `_ => {}` arm
        // and be dropped, which for a SECOND `*-Data.db` in the directory meant the
        // multi-SSTable refusal below did not fire (issue #1491 finding V1's sweep).
        let name = entry.file_name();
        if fs_probe::name_ends_with(&name, "-Data.db.jsonl") {
            goldens.push(entry.path());
        } else if fs_probe::name_ends_with(&name, "-Data.db") {
            data_dbs.push(entry.path());
        }
    }
    data_dbs.sort();
    goldens.sort();
    let names = |paths: &[PathBuf]| {
        paths
            .iter()
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let [data_db] = data_dbs.as_slice() else {
        return Err(format!(
            "{} holds {} *-Data.db files ({}) — the whole directory is staged as one \
             table, so the CLI would read all of them while a golden describes one; \
             this lane compares exactly one SSTable per case",
            fixture.display(),
            data_dbs.len(),
            if data_dbs.is_empty() {
                "none".to_string()
            } else {
                names(&data_dbs)
            }
        ));
    };
    let expected = PathBuf::from(format!("{}.jsonl", data_db.display()));
    // Three-valued: `is_file()` answers `false` for a golden it could not describe,
    // so an UNREADABLE golden was reported as one that is not there — the same
    // verdict (both fail) reached through a false statement.
    if !fs_probe::is_file(&expected)? {
        return Err(format!(
            "no golden {} beside the SSTable it must describe{}",
            expected.display(),
            if goldens.is_empty() {
                String::new()
            } else {
                format!(
                    " (the directory holds {}, which describe other generations)",
                    names(&goldens)
                )
            }
        ));
    }
    Ok(expected)
}

/// Stage a `--data-dir` holding EXACTLY this one table, by copying the fixture's
/// component files into `<dest>/<keyspace>/<fixture-dir-name>/`.
///
/// One table per data dir keeps each case independent (a sibling table's
/// unparseable component cannot perturb it) and keeps the whole lane fast: CLI
/// ingestion walks one directory instead of the whole corpus, so ~50 CLI
/// invocations stay in the low seconds. Copied rather than symlinked so the lane
/// does not depend on `std::os::unix`.
pub fn stage_single_table(dest: &Path, keyspace: &str, fixture: &Path) -> Result<(), String> {
    let name = fixture
        .file_name()
        .ok_or_else(|| format!("{} has no final component", fixture.display()))?;
    let target = dest.join(keyspace).join(name);
    std::fs::create_dir_all(&target)
        .map_err(|e| format!("cannot create {}: {e}", target.display()))?;
    let entries = fs_probe::dir_entries(fixture)?
        .ok_or_else(|| format!("{}: no such directory", fixture.display()))?;
    let mut copied = 0usize;
    for entry in entries {
        let path = entry.path();
        // Three-valued, and here the permissive collapse was the most consequential
        // of the sweep (issue #1491 finding V1): a component file `is_file()` could
        // not describe was silently NOT COPIED, so the CLI read an INCOMPLETE
        // SSTable — a missing `Index.db` or `CompressionInfo.db` — and whatever it
        // then rendered was compared against the full golden.
        if !fs_probe::is_file(&path)? {
            continue;
        }
        let file_name = path.file_name().ok_or_else(|| {
            format!(
                "{} has no final component, so it cannot be staged",
                path.display()
            )
        })?;
        std::fs::copy(&path, target.join(file_name))
            .map_err(|e| format!("cannot copy {}: {e}", path.display()))?;
        copied += 1;
    }
    if copied == 0 {
        return Err(format!(
            "no component files copied from {}",
            fixture.display()
        ));
    }
    Ok(())
}

// ===========================================================================
// L3: the golden is PAIRED with the SSTable it describes
// ===========================================================================
//
// Moved here with the code it exercises (campsite rule): these cases are about
// which files are picked off disk, which is this module's whole subject.

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
        std::fs::write(path, bytes).expect("write");
    }

    /// The discriminating case for the "lexicographically first golden" pick: a
    /// directory holding an EARLIER golden for a generation that is not the one
    /// present. Taking the first sorted golden compared the CLI's reading of
    /// `nb-2-big-Data.db` against `nb-1`'s dump — a wrong oracle, silently. 26
    /// committed fixture directories carry more than one golden, so the shape exists
    /// in this repository (issue #1491 review finding L3).
    #[test]
    fn the_golden_is_the_one_named_after_the_sstable_present() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let fixture = tmp.path().join("t-abc");
        touch(&fixture.join("nb-2-big-Data.db"), b"x");
        touch(&fixture.join("nb-1-big-Data.db.jsonl"), b"{}");
        let why = golden_path(&fixture).expect_err("nb-1's dump does not describe nb-2");
        assert!(
            why.contains("nb-2-big-Data.db.jsonl") && why.contains("nb-1-big-Data.db.jsonl"),
            "the failure must name both the golden it needs and the one it found: {why}"
        );

        // With the paired golden present it is chosen, and the earlier unpaired one is
        // ignored rather than preferred.
        touch(&fixture.join("nb-2-big-Data.db.jsonl"), b"{}");
        assert_eq!(
            golden_path(&fixture).expect("the paired golden"),
            fixture.join("nb-2-big-Data.db.jsonl")
        );
    }

    /// Several SSTables in ONE directory is not a narrowing but an UNSOUND
    /// comparison: `stage_single_table` copies the whole directory, so the CLI reads
    /// every generation while one golden describes one. It fails, naming them.
    #[test]
    fn a_directory_holding_several_sstables_is_refused() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let fixture = tmp.path().join("t-abc");
        touch(&fixture.join("nb-1-big-Data.db"), b"x");
        touch(&fixture.join("nb-1-big-Data.db.jsonl"), b"{}");
        touch(&fixture.join("nb-2-big-Data.db"), b"y");
        touch(&fixture.join("nb-2-big-Data.db.jsonl"), b"{}");
        let why = golden_path(&fixture).expect_err("two staged SSTables, one golden");
        assert!(
            why.contains("nb-1-big-Data.db")
                && why.contains("nb-2-big-Data.db")
                && why.contains("exactly one SSTable per case"),
            "{why}"
        );

        let empty = tmp.path().join("t-def");
        std::fs::create_dir_all(&empty).expect("mkdir");
        let why = golden_path(&empty).expect_err("no SSTable at all");
        assert!(why.contains("holds 0 *-Data.db files (none)"), "{why}");
    }

    /// The sweep's staging half: a name or an entry this module could not read used
    /// to VANISH from its scan instead of failing it (issue #1491 review finding V1).
    ///
    /// Each of the three scans dropped a different thing, and the consequence
    /// differed with it:
    ///
    ///   * `fixture_dirs_in` — `p.is_dir()` answers `false` for an entry it cannot
    ///     describe, so the root was reported as holding no such table;
    ///   * `golden_path` — classification went through `OsStr::to_str`, which answers
    ///     `None` for a name that is not valid UTF-8, so a SECOND `*-Data.db` so
    ///     named was not counted and the multi-SSTable refusal did not fire. That
    ///     refusal exists because the whole directory is staged: the CLI would read
    ///     both generations while one golden describes one;
    ///   * `stage_single_table` — `path.is_file()` answers `false` the same way, so
    ///     the component was silently NOT COPIED and the CLI read an incomplete
    ///     SSTable, whose rendering was then compared against the full golden.
    ///
    /// `#[cfg(unix)]`: a self-referential symlink is the one way to make `metadata`
    /// fail on an entry that IS in the listing without a chmod (which passes
    /// vacuously as root), and a non-UTF-8 name cannot be built portably in safe
    /// code.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_name_or_entry_fails_every_scan_instead_of_vanishing() {
        use std::os::unix::ffi::OsStrExt;
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path();
        let fixture = root.join("ks/t-abc");
        touch(&fixture.join("nb-1-big-Data.db"), b"x");
        touch(&fixture.join("nb-1-big-Data.db.jsonl"), b"{}");

        // Sound before anything is planted, so each failure below is attributable to
        // what it plants and not to the scaffolding.
        assert_eq!(
            fixture_dirs_in(root, "ks", "t").expect("readable"),
            vec![fixture.clone()]
        );
        assert_eq!(
            golden_path(&fixture).expect("paired"),
            fixture.join("nb-1-big-Data.db.jsonl")
        );
        stage_single_table(&tmp.path().join("staged"), "ks", &fixture).expect("stages");

        // A SECOND SSTable whose name is not valid UTF-8. `golden_path` must count
        // it, because the CLI reading the staged directory certainly will.
        let odd = std::ffi::OsStr::from_bytes(b"nb-2-\xff-big-Data.db");
        assert!(odd.to_str().is_none(), "the staged name is not valid UTF-8");
        std::fs::write(fixture.join(odd), b"y").expect("write");
        let why = golden_path(&fixture).expect_err("two SSTables, one golden");
        assert!(
            why.contains("holds 2 *-Data.db files") && why.contains("exactly one SSTable per case"),
            "a name it cannot read is still a second SSTable: {why}"
        );
        std::fs::remove_file(fixture.join(odd)).expect("unlink");

        // An entry the filesystem cannot DESCRIBE, in the fixture directory and in
        // the keyspace directory beside it.
        std::os::unix::fs::symlink("nb-2-big-Data.db", fixture.join("nb-2-big-Data.db"))
            .expect("symlink");
        std::os::unix::fs::symlink("t-loop", root.join("ks/t-loop")).expect("symlink");

        let why = fixture_dirs_in(root, "ks", "t").expect_err("an entry it cannot describe");
        assert!(
            why.contains("t-loop") && why.contains("cannot be described"),
            "{why}"
        );
        let why = stage_single_table(&tmp.path().join("staged2"), "ks", &fixture)
            .expect_err("a component it cannot describe must not be silently skipped");
        assert!(
            why.contains("nb-2-big-Data.db") && why.contains("cannot be described"),
            "{why}"
        );
    }

    /// Every candidate directory is returned, sorted, so a caller comparing one of
    /// them can COUNT what it left out instead of picking silently. A directory
    /// without a `*-Data.db` is not a candidate.
    #[test]
    fn every_sstable_directory_for_a_table_is_enumerated() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path();
        touch(&root.join("ks/t-bbb/nb-1-big-Data.db"), b"x");
        touch(&root.join("ks/t-aaa/nb-1-big-Data.db"), b"x");
        std::fs::create_dir_all(root.join("ks/t-ccc")).expect("mkdir");
        touch(&root.join("ks/other-aaa/nb-1-big-Data.db"), b"x");
        let dirs = fixture_dirs_in(root, "ks", "t").expect("readable");
        assert_eq!(
            dirs,
            vec![root.join("ks/t-aaa"), root.join("ks/t-bbb")],
            "sorted, and only directories holding a *-Data.db"
        );
        assert_eq!(
            fixture_dir_in(root, "ks", "t").expect("resolves"),
            root.join("ks/t-aaa"),
            "the first of them is the one compared"
        );
    }
}
