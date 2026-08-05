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
    std::fs::write(path, serde_json::to_string_pretty(&json).expect("serialize"))
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
    assert!(first.ok, "the first generation must succeed: {}", first.all());
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
