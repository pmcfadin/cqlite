//! `--manifest` path SAFETY for `cqlite salvage` (issue #4196) — refusing a
//! manifest path that would destroy part of the input, before any work is done.
//!
//! Split out of [`super::report`] (round 22, campsite rule / epic #1116) when
//! adding this guard took that file past the ~800-line source threshold — and
//! it is a genuine responsibility seam either way: `report` decides the exit
//! code and RENDERS the manifest, this module decides whether the manifest may
//! be written where the operator asked at all. A PURE MOVE of the guard plus
//! its unit tests; no behavior changed by the split.

use std::path::{Path, PathBuf};

use crate::cli_types::SalvageArgs;

/// Refuse a `--manifest` path that would DESTROY part of the input, BEFORE any
/// work is done (roborev, issue #4196, round-22 Low finding).
///
/// `--out` is guarded fail-closed (`execute_salvage_command` exits 1 on a
/// non-empty directory), but `--manifest` accepted ANY path and
/// [`super::report::write_manifest_file`] then `create_dir_all(parent)` +
/// `File::create`s it — which TRUNCATES unconditionally. `--manifest
/// ./damaged-table-dir/nb-1-big-Statistics.db` therefore destroyed a component
/// of the very input the tool exists to preserve, violating spec R5.1 (the
/// input is not modified) — the contract this branch just pinned with a test.
/// Salvage runs on data its operator has already lost once; the second, better
/// attempt must still be possible.
///
/// Two independent refusals, either one sufficient — the second is not
/// redundant, since a path can reach an SSTable component without resolving
/// inside THIS input's directory (a component of a DIFFERENT generation, or a
/// symlink):
///
/// 1. the manifest's directory resolves INSIDE the input's own directory (the
///    input dir itself, or anything below it) — salvage writes only under
///    `--out`, never into its input, whatever the file is called;
/// 2. the manifest path ALREADY EXISTS and is named like an SSTable component
///    (`*.db`, `*-TOC.txt`, `*-Digest.crc32`).
///
/// Returns the collision, NAMED, for the caller to print before exiting 1.
pub(super) fn validate_manifest_path(args: &SalvageArgs) -> Result<(), String> {
    let Some(manifest) = &args.manifest else {
        return Ok(());
    };
    manifest_path_collision(&args.input, manifest).map_or(Ok(()), Err)
}

/// [`validate_manifest_path`]'s decision, over plain paths so it is unit-testable
/// without a whole `SalvageArgs`. `Some(reason)` = refuse.
fn manifest_path_collision(input: &Path, manifest: &Path) -> Option<String> {
    // (2) first: it needs no canonicalization of the input at all, so it still
    // fires for an input path that does not resolve.
    if manifest.exists()
        && manifest
            .file_name()
            .and_then(|n| n.to_str())
            .map(looks_like_sstable_component)
            .unwrap_or(false)
    {
        return Some(format!(
            "--manifest {} names an EXISTING SSTable component — writing the manifest would \
             TRUNCATE it. Salvage must not modify a byte of any SSTable it can reach (spec \
             R5.1); point --manifest at a path of its own, e.g. <--out>/salvage.json",
            manifest.display()
        ));
    }

    // (1) containment. The input DIRECTORY is `input` itself (a table dir) or,
    // for a single `Data.db` FILE, its parent; the manifest's is its parent,
    // since the manifest file itself need not exist yet. `canonicalize` on
    // both, so `./x/../x`, a trailing slash and a symlinked corpus root all
    // compare correctly.
    //
    // A path that is NEITHER an existing directory nor an existing file has no
    // input directory to protect, and this arm must not INVENT one: taking
    // `parent()` unconditionally made a typo'd input (`salvage ./no-such-dir
    // --manifest ./m.json`) resolve its "input dir" to the enclosing
    // directory, which then contains the manifest — a spurious refusal, caught
    // by `a_nonexistent_input_does_not_break_the_guard`. `discover_salvage_inputs`
    // reports the missing input on its own; there is nothing here to destroy.
    let input_dir = if input.is_dir() {
        input.to_path_buf()
    } else if input.is_file() {
        input.parent()?.to_path_buf()
    } else {
        return None;
    };
    let manifest_parent = match manifest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        // `--manifest salvage.json` — the current working directory.
        _ => PathBuf::from("."),
    };
    let (Ok(input_dir), Ok(manifest_dir)) =
        (input_dir.canonicalize(), manifest_parent.canonicalize())
    else {
        return None;
    };
    if manifest_dir.starts_with(&input_dir) {
        return Some(format!(
            "--manifest {} resolves inside the INPUT directory {} — salvage writes only under \
             --out and must not modify a byte of its input (spec R5.1), and File::create would \
             truncate whatever is at that path. Point --manifest outside the input, e.g. \
             <--out>/salvage.json",
            manifest.display(),
            input_dir.display()
        ));
    }
    None
}

/// `true` for the file-name shapes Cassandra uses for SSTable components:
/// every `*.db` component, the `-TOC.txt` publication barrier, and
/// `-Digest.crc32`.
fn looks_like_sstable_component(name: &str) -> bool {
    name.ends_with(".db") || name.ends_with("-TOC.txt") || name.ends_with("-Digest.crc32")
}

/// Roborev, issue #4196, round-22 Low finding — [`manifest_path_collision`]'s
/// two refusals and, just as importantly, what it must NOT refuse.
#[cfg(test)]
mod tests {
    use super::manifest_path_collision;
    use std::path::Path;
    use tempfile::TempDir;

    /// A staged input generation directory holding one real-looking component
    /// set, plus a sibling `out` directory — the documented layout.
    fn staged(temp: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
        let input = temp.path().join("mytable-abcdef01");
        std::fs::create_dir_all(&input).expect("create input dir");
        for component in [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
            "nb-1-big-Digest.crc32",
        ] {
            std::fs::write(input.join(component), b"component bytes").expect("write component");
        }
        let out = temp.path().join("out");
        (input, out)
    }

    /// The exact destructive invocation the finding names: `--manifest` aimed
    /// at a component of the input.
    #[test]
    fn manifest_at_an_input_component_is_refused_and_names_it() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let manifest = input.join("nb-1-big-Statistics.db");
        let collision = manifest_path_collision(&input, &manifest)
            .expect("a --manifest aimed at an input component must be refused");
        assert!(
            collision.contains("nb-1-big-Statistics.db"),
            "the refusal must NAME the collision; got: {collision}"
        );
    }

    /// Every component NAME shape, existing, is refused — including via a
    /// single-`Data.db` input, where the input DIRECTORY is the file's parent.
    #[test]
    fn every_component_name_shape_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let data_db = input.join("nb-1-big-Data.db");
        for component in [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
            "nb-1-big-Digest.crc32",
        ] {
            let manifest = input.join(component);
            assert!(
                manifest_path_collision(&data_db, &manifest).is_some(),
                "{component} must be refused for a single-Data.db input too (the input directory \
                 is the file's parent)"
            );
        }
    }

    /// Any path inside the input directory is refused, component-named or not:
    /// salvage writes only under `--out`, never into its input.
    #[test]
    fn a_plain_json_path_inside_the_input_dir_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let collision = manifest_path_collision(&input, &input.join("salvage.json"))
            .expect("a manifest inside the input dir must be refused");
        assert!(collision.contains("INPUT directory"), "got: {collision}");
        // ...and below it, too.
        let nested = input.join("snapshots").join("salvage.json");
        std::fs::create_dir_all(input.join("snapshots")).expect("create nested dir");
        assert!(manifest_path_collision(&input, &nested).is_some());
    }

    /// `./x/../x` and a trailing slash must not defeat the containment check —
    /// both sides are canonicalized.
    #[test]
    fn a_traversal_path_into_the_input_dir_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let sneaky = input
            .join("snapshots")
            .join("..")
            .join("nb-1-big-Statistics.db");
        std::fs::create_dir_all(input.join("snapshots")).expect("create nested dir");
        assert!(
            manifest_path_collision(&input, &sneaky).is_some(),
            "a `..` traversal back into the input dir must still be refused"
        );
    }

    /// The DOCUMENTED pattern must keep working: `--manifest <--out>/salvage.json`,
    /// where `--out` does not exist yet. A guard that refused this would break
    /// `--help`'s own example.
    #[test]
    fn the_documented_out_dir_manifest_is_allowed() {
        let temp = TempDir::new().expect("tempdir");
        let (input, out) = staged(&temp);
        assert!(
            !out.exists(),
            "the documented pattern has --out not yet created"
        );
        assert_eq!(
            manifest_path_collision(&input, &out.join("salvage.json")),
            None
        );
        // And a plain sibling path outside the input.
        assert_eq!(
            manifest_path_collision(&input, &temp.path().join("m.json")),
            None
        );
    }

    /// A component-NAMED path that does NOT exist yet, outside the input, is
    /// allowed: nothing would be truncated. (Odd, but not this guard's business
    /// — refusing it would be a name-shape taboo rather than a real collision.)
    #[test]
    fn a_nonexistent_component_named_path_outside_the_input_is_allowed() {
        let temp = TempDir::new().expect("tempdir");
        let (input, out) = staged(&temp);
        assert_eq!(
            manifest_path_collision(&input, &out.join("nb-9-big-Data.db")),
            None
        );
    }

    /// An EXISTING component-named file OUTSIDE the input directory is still
    /// refused — the second rule is not redundant with containment: a component
    /// of a DIFFERENT generation is just as much someone's data.
    #[test]
    fn an_existing_component_outside_the_input_is_still_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let other = temp.path().join("othertable-01234567");
        std::fs::create_dir_all(&other).expect("create other dir");
        let victim = other.join("nb-1-big-Index.db");
        std::fs::write(&victim, b"someone else's data").expect("write victim");
        assert!(manifest_path_collision(&input, &victim).is_some());
    }

    /// An input that does not exist at all cannot be destroyed, so the
    /// containment arm simply does not fire (`discover_salvage_inputs` reports
    /// the missing input on its own) — the guard must not panic or refuse
    /// spuriously.
    #[test]
    fn a_nonexistent_input_does_not_break_the_guard() {
        let temp = TempDir::new().expect("tempdir");
        let missing = temp.path().join("no-such-table-dir");
        assert_eq!(
            manifest_path_collision(&missing, &temp.path().join("m.json")),
            None
        );
        assert_eq!(
            manifest_path_collision(Path::new(""), Path::new("m.json")),
            None
        );
    }
}
