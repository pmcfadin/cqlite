//! DESTRUCTIVE-WRITE CONTAINMENT for `cqlite salvage` (issue #4196, round-23
//! findings F1/F4/F5 — an independent Cassandra-format expert review REPRODUCED
//! F1 and F4 against the compiled binary and real Cassandra-written SSTable
//! bytes, so none of this is theoretical).
//!
//! # The one invariant, in one place
//!
//! Every guard this command grew up to round 22 validated a narrow LOCAL PROXY
//! for safety rather than the property anyone cares about:
//!
//! * `--out` was checked for EMPTINESS (`mod.rs`) and never for WHERE it points,
//!   so `--out <input>/recovered` wrote a whole recovered generation inside the
//!   input tree the tool exists to preserve — and a re-run then discovered that
//!   output as one more generation to salvage (F5);
//! * `--manifest` was checked against the INPUT only, so the manifest could be
//!   aimed at the run's OWN output: the recovered `Data.db` was written, then
//!   TRUNCATED by 607 bytes of manifest JSON, and the run still printed
//!   `recovered=100 lost=0` and exited 0 (F1);
//! * that same check canonicalized the manifest's PARENT and pattern-matched the
//!   manifest's FILE NAME, so a SYMLINK named `salvage.json` walked straight past
//!   both halves and `File::create` followed it into a real Cassandra-written
//!   `nb-1-big-Statistics.db` INSIDE the input (5265 bytes -> 531 bytes of JSON,
//!   exit 0, clean summary) — a direct spec R5.1 violation (F4a);
//! * and it FAILED OPEN: any `canonicalize` failure returned "no collision", in
//!   a guard whose own module doc claimed to fail closed (F4b).
//!
//! Four symptoms, ONE defect: nothing asked *will any write this run performs
//! land on a byte the operator did not name for overwriting, once every path is
//! FULLY RESOLVED and once the run's OWN planned output is counted among the
//! things worth protecting.* This module answers exactly that question, once, for
//! every destructive path salvage takes — because two independently-maintained
//! guards drifting apart is literally how F1 and F5 happened.
//!
//! # Shape
//!
//! [`WriteGuard`] holds the PROTECTED SET (each entry LABELED, so a refusal can
//! say what it collided with) and [`WriteGuard::assert_disjoint`] decides one
//! candidate against all of it. Resolution is [`resolve_write_target`]:
//! canonicalize when the path exists — which FOLLOWS symlinks, closing F4a —
//! and otherwise canonicalize the nearest EXISTING ancestor and re-apply the
//! remaining components LEXICALLY (`..` popping, `.` dropped), so a path that
//! does not exist yet still resolves to where it will actually land.
//!
//! Every resolution failure is a REFUSAL that names the reason (F4b). There is
//! exactly ONE "not protected" answer that is not a decision about the
//! candidate: an `input` that is neither an existing directory nor an existing
//! file has no input directory to protect at all — see [`WriteGuard::new`].

use std::path::{Component, Path, PathBuf};

/// Label for the input protected root. Also asserted on by
/// `manifest_path::tests::a_plain_json_path_inside_the_input_dir_is_refused` (the
/// round-22 case) — the operator has to be told WHICH boundary was crossed, and
/// "the input" and "your own output" are very different mistakes.
pub(super) const INPUT_LABEL: &str = "the INPUT directory";

/// Label for the run's own planned output generation directory (F1).
pub(super) const OUTPUT_LABEL: &str = "the run's OWN planned OUTPUT directory";

/// The remedy clause for a refused `--manifest`, kept identical to the pattern
/// `--help` and `dev-cookbook.md` document.
pub(super) const MANIFEST_REMEDY: &str =
    "point --manifest at a path of its own, outside both the input and the recovered generation, \
     e.g. <--out>/salvage.json";

/// The remedy clause for a refused `--out` (F5).
pub(super) const OUT_REMEDY: &str =
    "point --out at a directory OUTSIDE the input tree (a sibling of the table directory, or \
     anywhere on another volume) — salvage must be re-runnable on an input it has not altered";

/// The set of paths a salvage run must not write into, each LABELED for the
/// refusal message.
pub(super) struct WriteGuard {
    protected: Vec<(&'static str, PathBuf)>,
}

impl WriteGuard {
    /// Build the protected set for a run.
    ///
    /// * `input` — the literal `input` ARGUMENT, whose own directory is protected
    ///   (the argument itself when it is a table directory; its parent when it is
    ///   a single `Data.db` FILE). The boundary is deliberately the literal
    ///   argument and NOT "the keyspace/table tree": this CLI cannot enumerate
    ///   that tree, and pretending otherwise would invent a notion of "all this
    ///   table's generations" the tool does not have.
    /// * `output_dir` — where the run will actually nest its recovered
    ///   generation, i.e. `<--out>/<keyspace>/<table>/` (`recover.rs` documents
    ///   that `SSTableWriter` nests it there). `None` before the schema — and so
    ///   the keyspace/table — is known; that call (the `--out` check itself)
    ///   protects the input only, which is all it needs.
    ///
    /// Deliberately NOT protected: `--out` ITSELF. The documented invocation is
    /// `--manifest <--out>/salvage.json`, pinned by
    /// `manifest_path::tests::the_documented_out_dir_manifest_is_allowed` and by
    /// `salvage_cli_tests::manifest_inside_the_input_is_refused_and_the_input_is_untouched`'s
    /// second half — the manifest is SUPPOSED to live beside the recovered
    /// generation, just never INSIDE it.
    ///
    /// # Errors
    ///
    /// Fail-closed: a protected path that EXISTS but cannot be resolved (a
    /// permission-denied ancestor, a dangling link) is an unresolvable
    /// ambiguity, and a guard that cannot locate the thing it is protecting must
    /// refuse rather than wave the run through.
    ///
    /// The ONE exception, preserved from round 22 deliberately: an `input` that
    /// is neither an existing directory nor an existing file contributes NO
    /// protected entry. That is "there is nothing here to destroy", not "I could
    /// not decide" — `discover_salvage_inputs` reports the missing input on its
    /// own, and taking `parent()` unconditionally made a typo'd input
    /// (`salvage ./no-such-dir --manifest ./m.json`) resolve its "input dir" to
    /// the enclosing directory, which then contains the manifest: a spurious
    /// refusal, pinned by
    /// `manifest_path::tests::a_nonexistent_input_does_not_break_the_guard`.
    pub(super) fn new(input: &Path, output_dir: Option<&Path>) -> Result<Self, String> {
        let mut protected = Vec::with_capacity(2);

        // The input DIRECTORY, when there is one to protect at all. Resolve the
        // input ITSELF and take the parent for a FILE input, never the reverse:
        // `Path::parent()` of a bare relative `nb-1-big-Data.db` is `""` (which
        // round 22 then failed to canonicalize, and so failed OPEN), and the
        // directory to protect for a SYMLINKED input file is the one holding the
        // bytes that will actually be read. A resolved path is already
        // canonical, so its parent needs no second resolution.
        let input_dir = if input.is_dir() || input.is_file() {
            let resolved = resolve_write_target(input).map_err(|why| {
                format!(
                    "the input {} could not be resolved, so salvage cannot prove any write is \
                     safe: {why}. Refusing rather than guessing (spec R5.1: the input is not \
                     modified)",
                    input.display()
                )
            })?;
            if input.is_dir() {
                Some(resolved)
            } else {
                // `None` only for a `Data.db` sitting at the filesystem root,
                // which has no enclosing generation directory to protect.
                resolved.parent().map(Path::to_path_buf)
            }
        } else {
            None
        };
        if let Some(dir) = input_dir {
            protected.push((INPUT_LABEL, dir));
        }

        // The run's own planned output generation directory. It normally does
        // NOT exist yet, which is exactly why `resolve_write_target` resolves a
        // not-yet-existing path instead of demanding one.
        if let Some(dir) = output_dir {
            let resolved = resolve_write_target(dir).map_err(|why| {
                format!(
                    "the planned output directory {} could not be resolved, so salvage cannot \
                     prove any write is safe: {why}. Refusing rather than guessing",
                    dir.display()
                )
            })?;
            protected.push((OUTPUT_LABEL, resolved));
        }

        Ok(Self { protected })
    }

    /// [`Self::resolve_disjoint`] for a caller with no further rules of its own.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_disjoint`].
    pub(super) fn assert_disjoint(
        &self,
        subject: &str,
        candidate: &Path,
        remedy: &str,
    ) -> Result<(), String> {
        self.resolve_disjoint(subject, candidate, remedy)
            .map(|_| ())
    }

    /// Refuse `candidate` when the path it will ACTUALLY be written to lands
    /// inside (or on) any protected root; on success return that RESOLVED path,
    /// so a caller with further rules of its own ([`super::manifest_path`]'s
    /// component-name taboo) applies them to the path the write will really
    /// open rather than to the string the operator typed — and so resolution
    /// happens exactly ONCE, with exactly one wording for its failure.
    ///
    /// `subject` is the flag being validated (`--manifest`, `--out`) and
    /// `remedy` the closing advice; both go into the message verbatim, so a
    /// refusal always names the offending path, what it collided with, spec R5.1
    /// and the way out.
    ///
    /// # Errors
    ///
    /// The collision, NAMED, for the caller to print before exiting 1 — or the
    /// RESOLUTION FAILURE, also named (F4b): an unresolvable candidate is a
    /// refusal, never an allow.
    pub(super) fn resolve_disjoint(
        &self,
        subject: &str,
        candidate: &Path,
        remedy: &str,
    ) -> Result<PathBuf, String> {
        let resolved = resolve_write_target(candidate).map_err(|why| {
            format!(
                "{subject} {} cannot be proven safe to write: {why}. Salvage refuses rather than \
                 guess — a write it cannot locate could land anywhere, including on the input it \
                 exists to preserve (spec R5.1); {remedy}",
                candidate.display()
            )
        })?;
        for (label, protected) in &self.protected {
            // Component-wise, so `/a/bc` does NOT start with `/a/b`, and an
            // exact match (`--manifest <the input dir itself>`) still counts.
            if resolved.starts_with(protected) {
                return Err(format!(
                    "{subject} {} resolves to {}, inside {label} {} — a write there would destroy \
                     bytes the operator never named for overwriting: salvage writes only the \
                     recovered generation and must not modify a byte of any SSTable it can reach \
                     (spec R5.1), and File::create TRUNCATES unconditionally. {remedy}",
                    candidate.display(),
                    resolved.display(),
                    protected.display()
                ));
            }
        }
        Ok(resolved)
    }
}

/// Resolve the path a write to `candidate` will ACTUALLY land on.
///
/// * The candidate EXISTS -> `canonicalize`, which FOLLOWS symlinks. That is the
///   whole point of round-23 F4a: the round-22 guard canonicalized the manifest's
///   PARENT and never the manifest entry itself, so a symlink whose own parent
///   sat outside the input passed both of its rules and `File::create` followed
///   it into the input.
/// * The candidate does NOT exist yet -> canonicalize the nearest EXISTING
///   ancestor (which therefore contains no unresolved symlinks) and re-apply the
///   remaining components LEXICALLY: `.` dropped, `..` popped, `Normal` pushed.
///   Naive re-joining would leave `..` in the result and a containment check
///   would then read `<out>/x/../../<input>/salvage.json` as living under `<out>`
///   while the write in fact lands in the input.
///
/// # Errors
///
/// Every state in which the landing site is not KNOWABLE, each naming its cause:
/// a relative candidate with an unreadable current directory; an existing entry
/// that will not canonicalize (permission-denied ancestor); a DANGLING SYMLINK
/// at the candidate or at any ancestor (`canonicalize` reports plain `NotFound`
/// for one, yet `File::create` follows it and creates its target — so the
/// not-yet-exists branch must never absorb this case); no existing ancestor at
/// all; and a `..` chain that walks off the filesystem root.
pub(super) fn resolve_write_target(candidate: &Path) -> Result<PathBuf, String> {
    let absolute = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        let cwd = std::env::current_dir().map_err(|e| {
            format!(
                "{} is a relative path and the current directory could not be read ({e})",
                candidate.display()
            )
        })?;
        cwd.join(candidate)
    };

    match absolute.canonicalize() {
        Ok(resolved) => return Ok(resolved),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!(
                "{} exists but could not be resolved: {e}",
                candidate.display()
            ))
        }
    }

    // `NotFound` is ambiguous: it is ALSO what a dangling symlink reports, and
    // `File::create` on one CREATES ITS TARGET wherever that points. Refuse.
    if let Some(why) = dangling_symlink_reason(&absolute, candidate) {
        return Err(why);
    }

    let mut base = None;
    for ancestor in absolute.ancestors().skip(1) {
        match ancestor.canonicalize() {
            Ok(resolved) => {
                base = Some((ancestor.to_path_buf(), resolved));
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(why) = dangling_symlink_reason(ancestor, candidate) {
                    return Err(why);
                }
            }
            Err(e) => {
                return Err(format!(
                    "{}: its ancestor {} could not be resolved: {e}",
                    candidate.display(),
                    ancestor.display()
                ))
            }
        }
    }
    let Some((ancestor, mut resolved)) = base else {
        return Err(format!(
            "{} has no existing ancestor directory to resolve it against",
            candidate.display()
        ));
    };
    let tail = absolute.strip_prefix(&ancestor).map_err(|e| {
        format!(
            "{}: {} is not a prefix of it ({e})",
            candidate.display(),
            ancestor.display()
        )
    })?;
    for component in tail.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !resolved.pop() {
                    return Err(format!(
                        "{} walks above the filesystem root",
                        candidate.display()
                    ));
                }
            }
            Component::Normal(name) => resolved.push(name),
            Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "{}: unexpected root component below {}",
                    candidate.display(),
                    ancestor.display()
                ))
            }
        }
    }
    Ok(resolved)
}

/// `Some(reason)` when `path` is an ENTRY that exists as a symlink even though
/// resolving it reported `NotFound` — i.e. a dangling link. `candidate` is the
/// operator's original path, so the message names what they typed.
fn dangling_symlink_reason(path: &Path, candidate: &Path) -> Option<String> {
    let meta = path.symlink_metadata().ok()?;
    if !meta.file_type().is_symlink() {
        return None;
    }
    let target = std::fs::read_link(path)
        .map(|t| t.display().to_string())
        .unwrap_or_else(|e| format!("<unreadable link: {e}>"));
    Some(format!(
        "{} reaches the SYMLINK {} -> {}, whose target does not resolve — a write would FOLLOW it \
         and create that target, wherever it points",
        candidate.display(),
        path.display(),
        target
    ))
}

/// Round-23 F1/F4/F5 (confirmed by an independent Cassandra-format expert review
/// with a working reproduction) — the shared containment decision: what it
/// refuses, what it must NOT refuse, and the resolution states that are
/// refusals rather than allows.
#[cfg(test)]
mod tests {
    use super::{
        resolve_write_target, WriteGuard, INPUT_LABEL, MANIFEST_REMEDY, OUTPUT_LABEL, OUT_REMEDY,
    };
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    /// A staged input generation directory holding one real-looking component
    /// set, plus the sibling `out` directory of the documented layout.
    fn staged(temp: &TempDir) -> (PathBuf, PathBuf) {
        let input = temp.path().join("mytable-abcdef01");
        std::fs::create_dir_all(&input).expect("create input dir");
        for component in ["nb-1-big-Data.db", "nb-1-big-Statistics.db"] {
            std::fs::write(input.join(component), b"component bytes").expect("write component");
        }
        (input, temp.path().join("out"))
    }

    /// F5 — `--out` inside the input tree is REFUSED. The round-22 `--out` guard
    /// was an EMPTINESS probe and nothing else, so `--out <input>/recovered`
    /// wrote a full recovered generation inside the input the tool exists to
    /// preserve; a re-run then discovered that output as another generation to
    /// salvage.
    #[test]
    fn out_inside_the_input_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let guard = WriteGuard::new(&input, None).expect("guard builds for a real input dir");
        let refusal = guard
            .assert_disjoint("--out", &input.join("recovered"), OUT_REMEDY)
            .expect_err("--out inside the input dir must be refused");
        assert!(
            refusal.contains("recovered") && refusal.contains(INPUT_LABEL),
            "the refusal must name the path AND what it collided with; got: {refusal}"
        );
        // The input directory ITSELF, and a deeper nesting, likewise.
        assert!(guard.assert_disjoint("--out", &input, OUT_REMEDY).is_err());
        assert!(guard
            .assert_disjoint("--out", &input.join("a").join("b"), OUT_REMEDY)
            .is_err());
    }

    /// ...and the documented layout — `--out` a SIBLING of the input — is still
    /// allowed. A guard that refused this would refuse every invocation in
    /// `--help`.
    #[test]
    fn out_beside_the_input_is_allowed() {
        let temp = TempDir::new().expect("tempdir");
        let (input, out) = staged(&temp);
        let guard = WriteGuard::new(&input, None).expect("guard builds");
        assert_eq!(guard.assert_disjoint("--out", &out, OUT_REMEDY), Ok(()));
    }

    /// F1 — the run's OWN planned output directory is protected, so a manifest
    /// aimed at the recovered generation is refused BEFORE the run rather than
    /// truncating the recovered `Data.db` after it.
    #[test]
    fn a_candidate_inside_the_planned_output_dir_is_refused() {
        let temp = TempDir::new().expect("tempdir");
        let (input, out) = staged(&temp);
        let output_dir = out.join("ks").join("mytable");
        let guard = WriteGuard::new(&input, Some(&output_dir)).expect("guard builds");
        let refusal = guard
            .assert_disjoint(
                "--manifest",
                &output_dir.join("nb-1-big-Data.db"),
                MANIFEST_REMEDY,
            )
            .expect_err("a manifest inside the planned output dir must be refused");
        assert!(
            refusal.contains("nb-1-big-Data.db") && refusal.contains(OUTPUT_LABEL),
            "the refusal must name the path AND the output root it collided with; got: {refusal}"
        );
        // The documented `<--out>/salvage.json` sits BESIDE the generation, not
        // inside it, and stays allowed.
        assert_eq!(
            guard.assert_disjoint("--manifest", &out.join("salvage.json"), MANIFEST_REMEDY),
            Ok(())
        );
    }

    /// A `..` chain through a not-yet-existing directory must not defeat
    /// containment: the tail is re-applied LEXICALLY, so the candidate resolves
    /// to where the write actually lands.
    #[test]
    fn a_traversal_through_a_nonexistent_dir_still_resolves_into_the_input() {
        let temp = TempDir::new().expect("tempdir");
        let (input, out) = staged(&temp);
        std::fs::create_dir_all(&out).expect("create out");
        let guard = WriteGuard::new(&input, None).expect("guard builds");
        // <out>/nope/../../mytable-abcdef01/salvage.json -> inside the input.
        let sneaky = out
            .join("nope")
            .join("..")
            .join("..")
            .join("mytable-abcdef01")
            .join("salvage.json");
        assert!(
            guard
                .assert_disjoint("--manifest", &sneaky, MANIFEST_REMEDY)
                .is_err(),
            "a `..` traversal through a nonexistent directory back into the input must be refused"
        );
    }

    /// F4b — a candidate whose resolution FAILS for a reason other than "does
    /// not exist yet" is REFUSED with the reason NAMED. Round 22's
    /// `let (Ok(..), Ok(..)) = ... else { return None }` made every such state
    /// an ALLOW, in a guard whose module doc claimed to fail closed.
    ///
    /// A dangling symlink is the portable instance: `canonicalize` reports plain
    /// `NotFound` for it — indistinguishable from a path that simply is not
    /// there yet — while `File::create` FOLLOWS it and creates its target.
    #[cfg(unix)]
    #[test]
    fn an_unresolvable_candidate_is_refused_with_a_named_reason() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let guard = WriteGuard::new(&input, None).expect("guard builds");

        let dangling = temp.path().join("salvage.json");
        std::os::unix::fs::symlink(temp.path().join("no-such-target"), &dangling)
            .expect("create dangling symlink");
        assert!(
            !dangling.exists() && dangling.symlink_metadata().is_ok(),
            "the case needs a DANGLING link: absent to `exists()`, present as an entry"
        );
        let refusal = guard
            .assert_disjoint("--manifest", &dangling, MANIFEST_REMEDY)
            .expect_err("an unresolvable candidate must be refused, not allowed");
        assert!(
            refusal.contains("SYMLINK") && refusal.contains("no-such-target"),
            "the refusal must NAME the reason and the link target; got: {refusal}"
        );

        // The same, one level up: a dangling link as an ANCESTOR component.
        let via_ancestor = dangling.join("m.json");
        assert!(
            guard
                .assert_disjoint("--manifest", &via_ancestor, MANIFEST_REMEDY)
                .is_err(),
            "a dangling link as an ancestor component must be refused too"
        );
    }

    /// An existing candidate resolves THROUGH its symlink — the property F4a
    /// depends on, asserted directly on the resolver so it cannot be lost by a
    /// later refactor of the containment loop.
    #[cfg(unix)]
    #[test]
    fn resolution_follows_a_symlink_to_its_target() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let victim = input.join("nb-1-big-Statistics.db");
        let link = temp.path().join("salvage.json");
        std::os::unix::fs::symlink(&victim, &link).expect("create symlink");
        assert_eq!(
            resolve_write_target(&link).expect("an existing link resolves"),
            victim.canonicalize().expect("canonicalize victim")
        );
    }

    /// A path that does not exist YET resolves to where it WILL land, and a
    /// relative path resolves against the current directory — the two states
    /// that must stay ALLOWS, since the documented `<--out>/salvage.json` is the
    /// first and `--manifest salvage.json` the second.
    #[test]
    fn a_not_yet_existing_path_resolves_to_where_it_will_land() {
        let temp = TempDir::new().expect("tempdir");
        let canonical = temp.path().canonicalize().expect("canonicalize temp");
        assert_eq!(
            resolve_write_target(&temp.path().join("out").join("salvage.json"))
                .expect("a not-yet-existing path resolves"),
            canonical.join("out").join("salvage.json")
        );
        let relative = resolve_write_target(Path::new("salvage.json"))
            .expect("a relative path resolves against the cwd");
        assert!(
            relative.is_absolute() && relative.ends_with("salvage.json"),
            "got: {}",
            relative.display()
        );
    }

    /// An input that exists NOWHERE contributes no protected entry — "nothing to
    /// destroy", not "cannot decide". Pinned separately from
    /// `manifest_path`'s own case because THIS is where the decision now lives.
    #[test]
    fn a_nonexistent_input_protects_nothing() {
        let temp = TempDir::new().expect("tempdir");
        let guard = WriteGuard::new(&temp.path().join("no-such-table-dir"), None)
            .expect("a nonexistent input must not break the guard");
        assert_eq!(
            guard.assert_disjoint("--manifest", &temp.path().join("m.json"), MANIFEST_REMEDY),
            Ok(())
        );
        let empty = WriteGuard::new(Path::new(""), None).expect("an empty input path is not fatal");
        assert_eq!(
            empty.assert_disjoint("--manifest", Path::new("m.json"), MANIFEST_REMEDY),
            Ok(())
        );
    }

    /// A SYMLINKED input file protects the directory holding the bytes that will
    /// actually be READ, not the directory the link happens to sit in — the input
    /// is resolved before its parent is taken.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_input_file_protects_the_real_generation_directory() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let link = temp.path().join("nb-1-big-Data.db");
        std::os::unix::fs::symlink(input.join("nb-1-big-Data.db"), &link).expect("create symlink");
        let guard = WriteGuard::new(&link, None).expect("guard builds for a symlinked file input");
        assert!(
            guard
                .assert_disjoint(
                    "--manifest",
                    &input.join("nb-1-big-Statistics.db"),
                    MANIFEST_REMEDY
                )
                .is_err(),
            "the REAL generation directory must be protected, not the link's own parent"
        );
        // And the link's own parent (the temp root) is NOT protected by
        // accident — that would refuse the documented sibling `--out`.
        assert_eq!(
            guard.assert_disjoint("--out", &temp.path().join("out"), OUT_REMEDY),
            Ok(())
        );
    }

    /// A single `Data.db` FILE input protects its PARENT directory — the whole
    /// generation, not just the one file named.
    #[test]
    fn a_data_db_file_input_protects_its_directory() {
        let temp = TempDir::new().expect("tempdir");
        let (input, _out) = staged(&temp);
        let data_db = input.join("nb-1-big-Data.db");
        let guard = WriteGuard::new(&data_db, None).expect("guard builds for a file input");
        assert!(guard
            .assert_disjoint(
                "--manifest",
                &input.join("nb-1-big-Statistics.db"),
                MANIFEST_REMEDY
            )
            .is_err());
    }
}
