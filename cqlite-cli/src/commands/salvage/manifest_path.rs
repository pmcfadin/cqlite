//! `--manifest` path SAFETY for `cqlite salvage` (issue #4196) — refusing a
//! manifest path that would destroy bytes the operator did not name for
//! overwriting, before any work is done.
//!
//! Split out of [`super::report`] (round 22, campsite rule / epic #1116) when
//! adding this guard took that file past the ~800-line source threshold — and
//! it is a genuine responsibility seam either way: `report` decides the exit
//! code and RENDERS the manifest, this module decides whether the manifest may
//! be written where the operator asked at all.
//!
//! The CONTAINMENT decision itself moved out again in round 23, into the shared
//! [`crate::commands::write_guard`]: `--manifest` and `--out` were two independently
//! maintained guards, which is exactly how the round-23 F1/F5 findings happened
//! (a manifest aimed at the run's own recovered `Data.db` truncated it after a
//! clean-looking salvage, and `--out` was never checked for WHERE it pointed at
//! all). What is left here is the ONE rule specific to the manifest: its own
//! `*.db`/`-TOC.txt`/`-Digest.crc32` name-shape taboo.

use std::path::Path;

use crate::cli_types::SalvageArgs;

use crate::commands::write_guard::{WriteGuard, MANIFEST_REMEDY};

/// Refuse a `--manifest` path that would DESTROY bytes the operator did not
/// name for overwriting, BEFORE any work is done (roborev, issue #4196,
/// round-22 Low finding; round-23 findings F1/F4, both REPRODUCED against the
/// compiled binary and real Cassandra-written SSTable bytes by an independent
/// Cassandra-format expert review).
///
/// [`super::report::write_manifest_file`] does `create_dir_all(parent)` +
/// `File::create` — which TRUNCATES unconditionally — so `--manifest
/// ./damaged-table-dir/nb-1-big-Statistics.db` destroyed a component of the very
/// input the tool exists to preserve, violating spec R5.1 (the input is not
/// modified). Salvage runs on data its operator has already lost once; the
/// second, better attempt must still be possible.
///
/// Two independent refusals, either one sufficient — the second is not
/// redundant, since a path can reach an SSTable component without resolving
/// inside any PROTECTED directory (a component of a DIFFERENT generation, or a
/// different table entirely):
///
/// 1. the manifest RESOLVES into a protected root — the input's own directory,
///    or the run's own planned output generation directory ([`WriteGuard`]);
/// 2. the RESOLVED manifest path ALREADY EXISTS and is named like an SSTable
///    component (`*.db`, `*-TOC.txt`, `*-Digest.crc32`).
///
/// Both now read the RESOLVED path, never the literal argument. Round 22
/// canonicalized only the manifest's PARENT for (1) and pattern-matched only its
/// `file_name()` for (2), so a SYMLINK named `salvage.json` whose own parent sat
/// outside the input walked past both halves and `File::create` followed it into
/// a real Cassandra-written `nb-1-big-Statistics.db` INSIDE the input — 5265
/// bytes to 531 bytes of manifest JSON, exit 0, clean summary (round-23 F4a).
///
/// Returns the collision, NAMED, for the caller to print before exiting 1.
pub(super) fn validate_manifest_path(args: &SalvageArgs, guard: &WriteGuard) -> Result<(), String> {
    let Some(manifest) = &args.manifest else {
        return Ok(());
    };
    manifest_path_collision(manifest, guard).map_or(Ok(()), Err)
}

/// [`validate_manifest_path`]'s decision, over a plain path so it is
/// unit-testable without a whole `SalvageArgs`. `Some(reason)` = refuse.
fn manifest_path_collision(manifest: &Path, guard: &WriteGuard) -> Option<String> {
    // (1) containment, which also RESOLVES the path once (following symlinks)
    // and fails CLOSED on any resolution ambiguity — round 23 F4b: the previous
    // `let (Ok(..), Ok(..)) = ... else { return None }` made every unresolvable
    // path an ALLOW, in a guard that documented itself as fail-closed.
    let resolved = match guard.resolve_disjoint("--manifest", manifest, MANIFEST_REMEDY) {
        Ok(resolved) => resolved,
        Err(collision) => return Some(collision),
    };

    // (2) the name-shape taboo, applied to the RESOLVED path: an EXISTING
    // SSTable component anywhere — including outside every protected root — is
    // someone's data, and `File::create` would truncate it.
    if resolved.exists()
        && resolved
            .file_name()
            .and_then(|n| n.to_str())
            .map(looks_like_sstable_component)
            .unwrap_or(false)
    {
        return Some(format!(
            "--manifest {} resolves to {}, an EXISTING SSTable component — writing the manifest \
             would TRUNCATE it. Salvage must not modify a byte of any SSTable it can reach (spec \
             R5.1); {MANIFEST_REMEDY}",
            manifest.display(),
            resolved.display()
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

/// Roborev, issue #4196, round-22 Low finding and round-23 findings F1/F4 (the
/// latter confirmed by an independent Cassandra-format expert review with a
/// working reproduction) — [`manifest_path_collision`]'s two refusals and, just
/// as importantly, what it must NOT refuse.
#[cfg(test)]
mod tests {
    use super::manifest_path_collision;
    use crate::commands::write_guard::{WriteGuard, OUTPUT_LABEL};
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

    /// The two-argument shape every round-22 case was written against: protect
    /// the INPUT only, with no planned output root in play. The output root is
    /// supplied explicitly by the round-23 F1 case below, which is the only one
    /// whose subject it is.
    fn collision_for_input(input: &Path, manifest: &Path) -> Option<String> {
        let guard = WriteGuard::new(input, None).expect("the guard must build for this input");
        manifest_path_collision(manifest, &guard)
    }

    /// The exact destructive invocation the finding names: `--manifest` aimed
    /// at a component of the input.
    #[test]
    fn manifest_at_an_input_component_is_refused_and_names_it() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let manifest = input.join("nb-1-big-Statistics.db");
        let collision = collision_for_input(&input, &manifest)
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
                collision_for_input(&data_db, &manifest).is_some(),
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
        let collision = collision_for_input(&input, &input.join("salvage.json"))
            .expect("a manifest inside the input dir must be refused");
        assert!(collision.contains("INPUT directory"), "got: {collision}");
        // ...and below it, too.
        let nested = input.join("snapshots").join("salvage.json");
        std::fs::create_dir_all(input.join("snapshots")).expect("create nested dir");
        assert!(collision_for_input(&input, &nested).is_some());
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
            collision_for_input(&input, &sneaky).is_some(),
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
        assert_eq!(collision_for_input(&input, &out.join("salvage.json")), None);
        // And a plain sibling path outside the input.
        assert_eq!(
            collision_for_input(&input, &temp.path().join("m.json")),
            None
        );
        // Still allowed with the run's real planned output root in play: the
        // manifest lives BESIDE the recovered generation, never inside it.
        let output_dir = out.join("ks").join("mytable");
        let guard = WriteGuard::new(&input, Some(&output_dir)).expect("guard builds");
        assert_eq!(
            manifest_path_collision(&out.join("salvage.json"), &guard),
            None
        );
    }

    /// Round-23 F1 (HIGH, REPRODUCED) — a manifest resolving under the run's OWN
    /// planned output directory is REFUSED, and the refusal names it.
    ///
    /// This case REPLACES round 22's
    /// `a_nonexistent_component_named_path_outside_the_input_is_allowed`, whose
    /// stated rationale ("nothing would be truncated") was true at VALIDATION
    /// time and false at WRITE time: the run itself creates the file, so
    /// `--manifest <out>/<keyspace>/<table>/nb-1-big-Data.db` recovered 100
    /// partitions, wrote the real `Data.db`, then overwrote it with 607 bytes of
    /// manifest JSON — console `partitions: total=100 recovered=100 lost=0`,
    /// exit 0. A guard that only ever asks about paths that exist ALREADY cannot
    /// see the run's own output; the protected set has to include it.
    #[test]
    fn a_manifest_under_the_planned_output_root_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, out) = staged(&temp);
        let output_dir = out.join("ks").join("mytable");
        let guard = WriteGuard::new(&input, Some(&output_dir)).expect("guard builds");
        for candidate in [
            output_dir.join("nb-1-big-Data.db"),
            output_dir.join("salvage.json"),
            output_dir.clone(),
        ] {
            let collision = manifest_path_collision(&candidate, &guard).unwrap_or_else(|| {
                panic!(
                    "a --manifest resolving under the planned output root must be refused: \
                     {candidate:?}"
                )
            });
            assert!(
                collision.contains(&candidate.display().to_string())
                    && collision.contains(OUTPUT_LABEL),
                "the refusal must NAME the path and the output root it collided with; got: \
                 {collision}"
            );
        }
    }

    /// Round-23 F4a (MEDIUM, REPRODUCED DESTRUCTIVELY) — a SYMLINK named
    /// `salvage.json` pointing at a component INSIDE the input is refused.
    ///
    /// Neither round-22 rule could see it: rule 2 inspected only
    /// `manifest.file_name()`, so a link *named* `salvage.json` was not
    /// component-shaped, and rule 1 canonicalized only `manifest.parent()` —
    /// never the manifest entry itself — so a link whose own parent sits outside
    /// the input passed both. `File::create` then followed it.
    #[cfg(unix)]
    #[test]
    fn a_symlink_named_salvage_json_into_the_input_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let victim = input.join("nb-1-big-Statistics.db");
        let link = temp.path().join("salvage.json");
        std::os::unix::fs::symlink(&victim, &link).expect("create symlink");
        assert!(
            link.file_name().and_then(|n| n.to_str()) == Some("salvage.json"),
            "the case needs a link whose own NAME is innocuous"
        );
        let collision = collision_for_input(&input, &link)
            .expect("a symlink into the input must be refused (spec R5.1)");
        assert!(
            collision.contains("nb-1-big-Statistics.db"),
            "the refusal must name the RESOLVED victim, not just the link the operator typed; \
             got: {collision}"
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
        assert!(collision_for_input(&input, &victim).is_some());
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
            collision_for_input(&missing, &temp.path().join("m.json")),
            None
        );
        assert_eq!(
            collision_for_input(Path::new(""), Path::new("m.json")),
            None
        );
    }
}
