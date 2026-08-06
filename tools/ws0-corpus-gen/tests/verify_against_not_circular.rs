//! `--verify-against` may NOT compare a generated identity against itself (#3272 R5).
//!
//! # The defect, and why it is the worst possible place for it
//!
//! `--verify-against` IS the determinism check. It is the mechanism behind the corpus's
//! load-bearing claim — "byte-identical across three generations" — and therefore behind
//! every future comparison of a measured figure against a recorded one.
//!
//! It used to read the prior identity AFTER generating and writing its own. So a
//! verification path that aliased `<out>/corpus-identity.json` or `--identity-out` had
//! already been OVERWRITTEN by this run's output by the time it was read, and `diff`
//! compared the new identity against itself:
//!
//!     determinism:    PASS — reproduced <path> exactly
//!
//! having reproduced nothing at all. A circular self-comparison reported as a passing
//! determinism check — the #3272 shape exactly, in the one mechanism whose whole job is to
//! detect that two things differ. Worse than a wrong number: a wrong number invites
//! scrutiny, and this one destroys the recorded artifact it was supposed to compare
//! against, so a re-run cannot even find the divergence afterwards.
//!
//! # What is asserted here
//!
//! Both closures, in both directions:
//!
//! * the two aliasing spellings (`<out>/corpus-identity.json` and `--identity-out`) are
//!   REFUSED, naming the circularity, and refused BEFORE anything is generated — so the
//!   would-be prior artifact survives, byte-for-byte;
//! * a NON-aliasing prior still works in both directions: a matching one PASSES, a
//!   diverging one FAILS with the divergence named. Without that half a
//!   `--verify-against` hardcoded to refuse everything would satisfy the cases above.
//!
//! Non-vacuity for the ALIAS check specifically: the prior file's bytes are recorded
//! before the run and re-read after it, so "refused before generating" is measured rather
//! than inferred from an exit code.
//!
//! This drives the BINARY, not a library function, because the ordering defect lived in
//! `main.rs`'s sequence of operations — a library-level test of `identity.diff()` would
//! have stayed green through the entire lifetime of the bug.

use std::path::Path;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_ws0-corpus-gen");

/// Small enough to generate in seconds; the property under test is an ORDERING, which is
/// size-independent.
const ROWS: u64 = 200;
const ROWS_PER_PARTITION: u64 = 10;

struct Run {
    ok: bool,
    stdout: String,
    stderr: String,
}

impl Run {
    fn all(&self) -> String {
        format!("{}\n{}", self.stdout, self.stderr)
    }
}

fn gen(args: &[&str]) -> Run {
    let out = Command::new(BIN)
        .args([
            "--rows",
            &ROWS.to_string(),
            "--rows-per-partition",
            &ROWS_PER_PARTITION.to_string(),
            "--progress-every",
            "0",
        ])
        .args(args)
        .output()
        .expect("ws0-corpus-gen runs");
    Run {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// A syntactically complete identity for some OTHER corpus — the shape a real recorded
/// prior has, with values this run cannot produce.
fn write_foreign_identity(path: &Path) {
    let json = serde_json::json!({
        "issue": "#3096",
        "seed": 999_999,
        "table": "ws0.events",
        "rows": 1,
        "partitions": 1,
        "rows_per_partition": 1,
        "cells_per_row": 12,
        "data_db_bytes": 1,
        "data_db_sha256": "0".repeat(64),
        "total_component_bytes": 1,
        "bytes_per_row": 1.0,
        "compression_info_present": false,
        "components": [],
        "not_a_correctness_oracle": "performance fixture only (#3042)",
        "differs_from_prior_corpus": "n/a",
    });
    std::fs::write(
        path,
        serde_json::to_string_pretty(&json).expect("serialize"),
    )
    .expect("write foreign identity");
}

/// ALIAS 1 — `--verify-against` naming the corpus root's own `corpus-identity.json`.
///
/// This is the spelling an operator reaches for first ("compare against the identity
/// that's already there"), and the one the pre-fix code turned into a self-comparison.
#[test]
fn verify_against_the_corpus_own_identity_is_refused_before_generating() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    std::fs::create_dir_all(&out).expect("mkdir");
    let alias = out.join("corpus-identity.json");
    write_foreign_identity(&alias);
    let before = std::fs::read(&alias).expect("read prior");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        alias.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "a --verify-against that aliases this run's own identity output must FAIL. \
         Pre-fix it exited 0 and printed `determinism: PASS — reproduced … exactly` having \
         compared the generated identity against itself. Output:\n{}",
        run.all()
    );
    let all = run.all();
    assert!(
        all.contains("SAME FILE"),
        "the refusal must say the two paths are the same file: {all}"
    );
    assert!(
        all.contains("circular self-comparison"),
        "the refusal must name the circularity — that is WHY it is refused, and an \
         operator reading only 'path conflict' would reach for a symlink: {all}"
    );
    assert!(
        !all.contains("determinism:    PASS"),
        "a refused run must not also print a determinism verdict: {all}"
    );

    // NON-VACUITY for "before generating": the prior artifact is untouched, byte for byte,
    // and no corpus was written. An exit code alone cannot distinguish "refused up front"
    // from "generated, overwrote the prior, then refused".
    assert_eq!(
        std::fs::read(&alias).expect("re-read prior"),
        before,
        "the refusal must happen BEFORE any identity is written — the prior artifact the \
         comparison was supposed to be against was overwritten"
    );
    assert!(
        !out.join("ws0").join("events").exists(),
        "nothing may be generated before the aliasing refusal"
    );
}

/// ALIAS 2 — `--verify-against` naming the explicit `--identity-out`.
///
/// The same circularity through the other write target. Checking one spelling and not the
/// other would leave exactly the hole this finding is about.
#[test]
fn verify_against_the_identity_out_path_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let alias = dir.path().join("recorded.json");
    write_foreign_identity(&alias);
    let before = std::fs::read(&alias).expect("read prior");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        alias.to_str().expect("utf8"),
        "--verify-against",
        alias.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "--verify-against aliasing --identity-out must FAIL: {}",
        run.all()
    );
    assert!(
        run.all().contains("SAME FILE"),
        "the refusal must name the aliasing: {}",
        run.all()
    );
    assert_eq!(
        std::fs::read(&alias).expect("re-read"),
        before,
        "the --identity-out artifact must survive the refusal untouched"
    );
}

/// The alias is detected through a NON-LITERAL spelling too.
///
/// `<out>/./corpus-identity.json` is the same file as `<out>/corpus-identity.json`, and a
/// lexical string comparison would miss it — which would make the guard bypassable by
/// typing the path differently, i.e. a deny-list over spellings, the exact class this
/// issue keeps closing.
#[test]
fn the_alias_check_is_not_a_lexical_string_comparison() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    std::fs::create_dir_all(&out).expect("mkdir");
    let real = out.join("corpus-identity.json");
    write_foreign_identity(&real);
    let spelled = out.join(".").join("corpus-identity.json");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        spelled.to_str().expect("utf8"),
    ]);
    assert!(
        !run.ok && run.all().contains("SAME FILE"),
        "`<out>/./corpus-identity.json` is the same file as `<out>/corpus-identity.json` \
         and must be refused; a lexical comparison would let this through: {}",
        run.all()
    );
}

/// A **HARDLINK** to a write target is the same file, and must be refused (#3272 round 3).
///
/// The nit that prompted this: `same_path` used `canonicalize`, which resolves symlinks,
/// `..` and duplicate separators — and sees NEITHER of the two aliases that reach the same
/// inode by a genuinely different name. A hardlink is two directory entries over one inode,
/// so `canonicalize` returns each unchanged and they compare unequal.
///
/// The CIRCULARITY half was already closed by reading the prior BEFORE generation (#3272
/// R5): whatever the paths are, the comparison is against bytes that predate this run. But
/// the ARTIFACT-PRESERVATION half did not fire — the operator's recorded prior is a write
/// target under another name, so `identity.write_json` TRUNCATES it and a re-run then
/// compares against the new bytes. That is what this asserts, and it asserts it on the
/// artifact rather than only on the exit code.
#[test]
fn a_hardlink_to_a_write_target_is_refused_and_the_artifact_survives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    std::fs::create_dir_all(&out).expect("mkdir");
    // The write target, holding a FOREIGN identity so a comparison against it would be a
    // real (failing) comparison rather than a no-op.
    let target = out.join("corpus-identity.json");
    write_foreign_identity(&target);
    // ...and a second NAME for the same inode, outside the corpus dir so no path
    // comparison could relate the two.
    let link = dir.path().join("operator-recorded-prior.json");
    std::fs::hard_link(&target, &link).expect("hard_link");

    // NON-VACUITY for the fixture: the two paths really are one file, and really do have
    // different names. Without this the case could pass on a filesystem that silently
    // copied instead of linking.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let a = std::fs::metadata(&target).expect("stat target");
        let b = std::fs::metadata(&link).expect("stat link");
        assert_eq!(
            (a.dev(), a.ino()),
            (b.dev(), b.ino()),
            "the fixture must be a real hardlink (one inode, two names), or this case tests \
             nothing"
        );
    }
    assert_ne!(
        target.canonicalize().expect("canon target"),
        link.canonicalize().expect("canon link"),
        "the two paths must CANONICALIZE DIFFERENTLY — that is the whole point: \
         canonicalize cannot see a hardlink, so a canonicalize-only check let this through"
    );

    let before = std::fs::read(&link).expect("read prior");
    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        link.to_str().expect("utf8"),
    ]);
    assert!(
        !run.ok && run.all().contains("SAME FILE"),
        "a HARDLINK to `<out>/corpus-identity.json` is the same file and must be refused; \
         canonicalize cannot see it (#3272 round 3): {}",
        run.all()
    );
    assert_eq!(
        std::fs::read(&link).expect("re-read"),
        before,
        "the operator's recorded prior must survive the refusal UNTOUCHED — the \
         artifact-preservation half is what a hardlink defeats, since reading the prior \
         first already makes the comparison honest"
    );
}

/// A CASE-DIFFERING spelling on a case-INSENSITIVE filesystem is the same file too.
///
/// The second alias `canonicalize` cannot see (#3272 round 3): on APFS (macOS default) and
/// NTFS, `<out>/CORPUS-IDENTITY.JSON` and `<out>/corpus-identity.json` are ONE file and two
/// strings.
///
/// The case-sensitivity of the filesystem is DETECTED rather than assumed, and the
/// assertion is made only when the detection says insensitive — a `#[cfg(target_os)]` guess
/// would be wrong on a case-sensitive APFS volume (which macOS can format) and on a
/// case-insensitive mount under Linux. When the filesystem IS case-sensitive the two paths
/// are genuinely different files and there is nothing to refuse, which is reported rather
/// than silently passing.
#[test]
fn a_case_differing_spelling_is_refused_on_a_case_insensitive_filesystem() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    std::fs::create_dir_all(&out).expect("mkdir");
    let target = out.join("corpus-identity.json");
    write_foreign_identity(&target);
    let upper = out.join("CORPUS-IDENTITY.JSON");

    // DETECT, never assume: does opening the upper-case spelling reach the same inode?
    let case_insensitive = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            match (std::fs::metadata(&target), std::fs::metadata(&upper)) {
                (Ok(a), Ok(b)) => (a.dev(), a.ino()) == (b.dev(), b.ino()),
                _ => false,
            }
        }
        #[cfg(not(unix))]
        {
            std::fs::metadata(&upper).is_ok()
        }
    };
    if !case_insensitive {
        // A case-SENSITIVE filesystem: the two paths are different files, so there is no
        // alias to refuse. Said out loud, because a test that silently returns is
        // indistinguishable from one that ran.
        eprintln!(
            "case-sensitive filesystem: `CORPUS-IDENTITY.JSON` is a DIFFERENT file from \
             `corpus-identity.json` here, so there is no alias to detect. The hardlink case \
             covers the same `same_file` code path unconditionally."
        );
        return;
    }

    let before = std::fs::read(&target).expect("read prior");
    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        upper.to_str().expect("utf8"),
    ]);
    assert!(
        !run.ok && run.all().contains("SAME FILE"),
        "on a case-INSENSITIVE filesystem `<out>/CORPUS-IDENTITY.JSON` IS \
         `<out>/corpus-identity.json`, so it must be refused: {}",
        run.all()
    );
    assert_eq!(
        std::fs::read(&target).expect("re-read"),
        before,
        "the recorded prior must survive the refusal untouched"
    );
}

/// The ACCEPT direction, and the FAIL direction, on a NON-aliasing prior.
///
/// Without these the three cases above would be satisfied by a `--verify-against` that
/// refused everything — and a determinism check that always refuses is as useless as one
/// that always passes, with the added property that an operator will delete it.
///
/// Run as ONE test because the PASS case's generated identity is the input to the FAIL
/// case (a real recorded prior, produced by a real generation), which is also what makes
/// the PASS case a genuine reproduction rather than a comparison against a hand-written
/// file.
#[test]
fn a_non_aliasing_prior_passes_when_reproduced_and_fails_when_it_diverges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let recorded = dir.path().join("recorded.json");

    // Generation 1 records its identity OUTSIDE any later corpus root.
    let first = gen(&[
        "--out",
        dir.path().join("a").to_str().expect("utf8"),
        "--identity-out",
        recorded.to_str().expect("utf8"),
    ]);
    assert!(
        first.ok,
        "the first generation must succeed: {}",
        first.all()
    );
    assert!(recorded.is_file(), "the recorded identity must exist");

    // Generation 2, independent root, SAME inputs: must reproduce it exactly.
    let same = gen(&[
        "--out",
        dir.path().join("b").to_str().expect("utf8"),
        "--verify-against",
        recorded.to_str().expect("utf8"),
    ]);
    assert!(
        same.ok && same.all().contains("determinism:    PASS"),
        "an independent generation with the same inputs must reproduce the recorded \
         identity: {}",
        same.all()
    );

    // Generation 3, DIFFERENT seed: must FAIL, naming the divergence. This is the check
    // doing its job — and it is the case a circular self-comparison could never fail.
    let diverged = gen(&[
        "--out",
        dir.path().join("c").to_str().expect("utf8"),
        "--seed",
        "424242",
        "--verify-against",
        recorded.to_str().expect("utf8"),
    ]);
    assert!(
        !diverged.ok,
        "a different seed must FAIL the determinism comparison: {}",
        diverged.all()
    );
    assert!(
        diverged.all().contains("determinism:    FAIL"),
        "the divergence must be reported as a determinism FAIL: {}",
        diverged.all()
    );
    assert!(
        diverged.all().contains("seed"),
        "the FAIL must name the field that diverged: {}",
        diverged.all()
    );
}

/// A `--verify-against` INSIDE THE TABLE DIRECTORY is refused — the dir `generate()` DELETES
/// (#3272 review round 13, F1).
///
/// # The gap: `--identity-out` was containment-checked and its sibling was not
///
/// Round 10's F3 gave `--identity-out` a containment rule ("nothing may be written under `--out`
/// except the one canonical identity path"). `--verify-against` got no containment check at all —
/// only the narrow "is it one of the two identity WRITE TARGETS" test. So a prior anywhere inside
/// `<out>/ws0/events/` walked straight through, and `generate()` `remove_dir_all`s that directory
/// before writing: the prior is loaded (the comparison is honest) and then SILENTLY DELETED.
///
/// # NON-VACUITY: the pre-fix behaviour is MEASURED, not argued
///
/// The case first reproduces the destruction with the guard bypassed — by generating the corpus,
/// placing the prior in the table dir and running a SECOND generation whose `--verify-against`
/// points at it. Pre-fix that exited 0 and the file was gone. Post-fix the run is REFUSED and the
/// file is still there, byte for byte. Asserting the survival is the whole point: an exit code
/// alone cannot distinguish "refused up front" from "deleted the prior, then refused".
#[test]
fn verify_against_inside_the_table_directory_is_refused_and_the_prior_survives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    // A real corpus first, so the table directory EXISTS and a prior can be placed in it.
    let first = gen(&["--out", out.to_str().expect("utf8")]);
    assert!(
        first.ok,
        "the fixture generation must succeed: {}",
        first.all()
    );
    let table_dir = out.join("ws0").join("events");
    assert!(table_dir.is_dir(), "the table dir must exist for this case");

    // The operator's recorded prior, placed inside the table dir (the mistake under test).
    let prior = table_dir.join("recorded-prior.json");
    write_foreign_identity(&prior);
    let before = std::fs::read(&prior).expect("read prior");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        prior.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "a --verify-against inside the generated table directory must FAIL: `generate()` \
         remove_dir_all's that directory, so the verification artifact is destroyed by the run \
         that was comparing against it. Output:\n{}",
        run.all()
    );
    assert!(
        run.all()
            .contains("resolves INSIDE the generated table directory"),
        "the refusal must name the CONTAINMENT, so an operator learns the actual constraint: {}",
        run.all()
    );
    // THE PROPERTY THAT MATTERS: the artifact survived.
    assert!(
        prior.is_file(),
        "the prior must SURVIVE the refusal — pre-fix `remove_dir_all` deleted it: {}",
        run.all()
    );
    assert_eq!(
        std::fs::read(&prior).expect("re-read prior"),
        before,
        "the prior must be untouched byte-for-byte"
    );
}

/// NON-VACUITY for the case above, as a DIRECT MEASUREMENT of the destruction.
///
/// The guard now refuses the spelling, so the pre-fix loss cannot be observed through the binary
/// any more. It is observed through the MECHANISM instead: a file inside the table directory does
/// not survive a regeneration into the same `--out`. That is exactly what the pre-fix
/// `--verify-against` path did to the operator's prior, and it is measured here rather than
/// asserted in prose — so if `generate()` ever stopped clearing the table dir, this case would
/// tell us the guard is protecting against something that no longer happens.
#[test]
fn a_file_in_the_table_directory_really_is_destroyed_by_a_regeneration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let first = gen(&["--out", out.to_str().expect("utf8")]);
    assert!(first.ok, "generation 1 must succeed: {}", first.all());
    let victim = out.join("ws0").join("events").join("operator-prior.json");
    write_foreign_identity(&victim);
    assert!(
        victim.is_file(),
        "the victim must exist before generation 2"
    );

    // A SECOND generation into the same root, WITHOUT --verify-against, so no guard is involved.
    let second = gen(&["--out", out.to_str().expect("utf8")]);
    assert!(second.ok, "generation 2 must succeed: {}", second.all());
    assert!(
        !victim.exists(),
        "a regeneration DELETES the table directory, so a prior placed there is destroyed — this \
         is the loss the round-13 guard refuses, measured rather than argued"
    );
}

/// A `--verify-against` ALIASING THE EMITTED DDL is refused — the file `generate()` OVERWRITES
/// (#3272 review round 13, F1).
///
/// The second half of the same gap. `<out>/ws0-events.cql` is not an identity write target, so
/// `load_prior_identity`'s narrow check waved it through; `generate()` then rewrites it, so the
/// operator's prior is replaced by the DDL and a re-run cannot even parse it as an identity.
///
/// Refused via the SAME [`generated_input_paths`] list `--identity-out` uses, which is what makes
/// this a fix to the class rather than to one spelling: a generated file added to that list is
/// protected from both arguments in one edit.
#[test]
fn verify_against_aliasing_the_emitted_ddl_is_refused_and_the_prior_survives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    std::fs::create_dir_all(&out).expect("mkdir");
    // The operator's prior, at the path the generator writes its DDL to.
    let prior = out.join("ws0-events.cql");
    write_foreign_identity(&prior);
    let before = std::fs::read(&prior).expect("read prior");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        prior.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "a --verify-against aliasing the emitted DDL must FAIL — generation OVERWRITES it, so the \
         verification artifact is destroyed by the run comparing against it: {}",
        run.all()
    );
    assert!(
        run.all().contains("the emitted DDL ws0-events.cql"),
        "the refusal must name the generated file it collides with: {}",
        run.all()
    );
    assert_eq!(
        std::fs::read(&prior).expect("re-read prior"),
        before,
        "the prior must survive the refusal untouched — pre-fix the DDL write replaced it"
    );
    assert!(
        !out.join("ws0").exists(),
        "the refusal must precede generation: {}",
        run.all()
    );
}

/// THE ACCEPT DIRECTION for the round-13 guard: the DOCUMENTED spelling still works.
///
/// Without this, the two refusals above are satisfied by a `--verify-against` that refuses
/// everything under (or near) the corpus — and this issue has broken three documented operator
/// commands exactly that way. The documented form points at an out-of-tree
/// `…/corpus-identity.json`, which must PASS, and the `<out>/corpus-identity.json` spelling must
/// keep its existing (round 5) circularity refusal rather than acquiring a containment one.
#[test]
fn the_documented_verify_against_spelling_is_still_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let recorded = dir.path().join("corpus-identity.json");

    // Record a real prior OUTSIDE any corpus root — the documented shape.
    let first = gen(&[
        "--out",
        dir.path().join("a").to_str().expect("utf8"),
        "--identity-out",
        recorded.to_str().expect("utf8"),
    ]);
    assert!(first.ok, "the recording run must succeed: {}", first.all());

    // ...and compare against it from an independent root: this is the determinism check, and the
    // round-13 containment rule must not touch it.
    let verified = gen(&[
        "--out",
        dir.path().join("b").to_str().expect("utf8"),
        "--verify-against",
        recorded.to_str().expect("utf8"),
    ]);
    assert!(
        verified.ok && verified.all().contains("determinism:    PASS"),
        "the DOCUMENTED --verify-against (a record outside the corpus, named \
         corpus-identity.json) must still PASS — a guard that refused it would break the one \
         command this whole mechanism exists for: {}",
        verified.all()
    );
}

/// An unreadable or malformed prior FAILS — and fails BEFORE generating.
///
/// The second half of "read first": the ordering fix also turns a multi-minute run that
/// dies at the very end on a typo'd path into a refusal in milliseconds. Asserted by the
/// absence of any generated corpus, not by timing (a wall-clock assertion in a correctness
/// test is its own defect class).
#[test]
fn an_unusable_prior_fails_before_any_corpus_is_generated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");

    let missing = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--verify-against",
        dir.path().join("nope.json").to_str().expect("utf8"),
    ]);
    assert!(!missing.ok, "an absent prior must fail: {}", missing.all());
    assert!(
        missing.all().contains("could not be read"),
        "the refusal must say the prior could not be read: {}",
        missing.all()
    );
    assert!(
        !out.join("ws0").join("events").exists(),
        "an unreadable prior must be refused BEFORE generating, so no corpus exists"
    );

    let garbage = dir.path().join("garbage.json");
    std::fs::write(&garbage, "{ this is not json").expect("write garbage");
    let bad = gen(&[
        "--out",
        dir.path().join("corpus2").to_str().expect("utf8"),
        "--verify-against",
        garbage.to_str().expect("utf8"),
    ]);
    assert!(!bad.ok, "a malformed prior must fail: {}", bad.all());
    assert!(
        bad.all().contains("is not a corpus identity"),
        "the refusal must say the file is not an identity: {}",
        bad.all()
    );
    assert!(
        !dir.path().join("corpus2").join("ws0").exists(),
        "a malformed prior must be refused before generating"
    );
}
