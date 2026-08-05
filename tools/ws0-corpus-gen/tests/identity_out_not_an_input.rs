//! `--identity-out` MUST NOT OVERWRITE A GENERATED CORPUS INPUT (issue #3272 review R3).
//!
//! # The finding
//!
//! `--identity-out` was written verbatim. Nothing stopped it naming a path inside the generated
//! table directory (`<out>/ws0/events/nb-1-big-Index.db`) or the emitted DDL
//! (`<out>/ws0-events.cql`), so `identity.write_json` REPLACED a generated input **after its
//! size and digest had been recorded in that very identity** — and generation still exited 0.
//!
//! The artifact then describes a corpus that is not on disk: the identity says `Index.db` is N
//! bytes with digest D, while `Index.db` now holds the identity JSON. A reporting path handed
//! the identity alone cites recorded digests for bytes that were overwritten by the record of
//! them, which is this issue's subject exactly — an instrument reporting success without having
//! measured.
//!
//! # A SEPARATE FILE from `verify_against_not_circular.rs`, deliberately
//!
//! That file's subject is `--verify-against` CIRCULARITY (comparing a generated identity against
//! itself). This file's subject is `--identity-out` DESTRUCTION (an output overwriting an input).
//! They share the `same_path`/`same_file` mechanism, which is the point of not duplicating it —
//! but they are different defects with different remedies, and that file is already at ~490
//! lines against the campsite-rule target.
//!
//! # What makes each case non-vacuous
//!
//! Every refusal case records the target file's BYTES BEFORE the run and re-reads them after, so
//! "refused before generating" is MEASURED rather than inferred from an exit code — a check that
//! refused after writing would still exit non-zero while having already destroyed the input.
//! And the ACCEPT case asserts the corpus inputs SURVIVE a legitimate `--identity-out`, so the
//! guard cannot be a function that refuses every value of the flag.
//!
//! This drives the BINARY, because the defect lives in `main.rs`'s sequence of operations.

use std::path::Path;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_ws0-corpus-gen");

/// Small enough to generate in seconds. The property under test is a PATH RELATION, which is
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

/// Generate a real corpus at `out`, so the cases below have genuine generated inputs to alias.
fn generate_corpus(out: &Path) -> Run {
    gen(&["--out", out.to_str().expect("utf8")])
}

/// CASE 1 — `--identity-out` INSIDE the generated table directory.
///
/// The strongest form of the defect: the identity replaces an SSTable component whose digest the
/// same identity records. Note the table directory does NOT exist when the check runs, which is
/// why containment is decided through the deepest existing ancestor rather than by `canonicalize`
/// alone.
#[test]
fn identity_out_inside_the_table_directory_is_refused_before_generating() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let target = out.join("ws0").join("events").join("nb-1-big-Index.db");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        target.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "an --identity-out inside the generated table dir must FAIL. Pre-fix it exited 0 having \
         replaced a component after recording its digest. Output:\n{}",
        run.all()
    );
    assert!(
        run.all()
            .contains("resolves INSIDE the generated table directory"),
        "the refusal must name the containment, so an operator knows WHY: {}",
        run.all()
    );
    // NON-VACUITY: refused BEFORE generating, so nothing was written at all.
    assert!(
        !out.join("ws0").join("events").exists(),
        "the refusal must precede generation — no table dir may exist: {}",
        run.all()
    );
}

/// The same case for a path inside the table dir that is NOT a known component name.
///
/// Component names are unknown before the writer runs, so the guard cannot be a name list; it
/// asks about CONTAINMENT. This case is what distinguishes the two: a name-based check would
/// pass this and then write into the table directory anyway.
#[test]
fn identity_out_inside_the_table_directory_is_refused_whatever_it_is_called() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let target = out
        .join("ws0")
        .join("events")
        .join("some-name-no-component-has.json");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        target.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "containment, not a component-name list, is the question — nothing may be written into \
         the table directory whatever it is called: {}",
        run.all()
    );
}

/// CASE 2 — `--identity-out` ALIASING the emitted DDL, which R2 makes a verified measurement
/// input.
///
/// `ws0-events.cql` is what the Flight ticket is generated from and what the bare scan ingests,
/// so replacing it with the identity JSON changes what BOTH arms read.
#[test]
fn identity_out_aliasing_the_emitted_ddl_is_refused_and_the_ddl_survives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    // Generate a real corpus first, so the DDL EXISTS and its bytes can be compared.
    let first = generate_corpus(&out);
    assert!(
        first.ok,
        "the fixture generation must succeed: {}",
        first.all()
    );
    let ddl = out.join("ws0-events.cql");
    let before = std::fs::read(&ddl).expect("read the generated DDL");
    assert!(!before.is_empty(), "the DDL fixture must be non-empty");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        ddl.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "an --identity-out aliasing the emitted DDL must FAIL: it is a measurement input both \
         arms read, and overwriting it after its digest was recorded leaves the two arms reading \
         something other than the corpus the identity describes. Output:\n{}",
        run.all()
    );
    assert!(
        run.all().contains("the emitted DDL ws0-events.cql"),
        "the refusal must name the input it protects: {}",
        run.all()
    );
    // NON-VACUITY, and the property that actually matters: the input SURVIVED byte-for-byte.
    // A guard that refused only AFTER `write_json` would still exit non-zero here.
    let after = std::fs::read(&ddl).expect("re-read the DDL");
    assert_eq!(
        before, after,
        "the generated DDL must be UNTOUCHED by the refused run — that is the whole point of \
         checking before generation, not merely exiting non-zero"
    );
}

/// The alias that only FILE IDENTITY can see: a HARDLINK.
///
/// `canonicalize` resolves symlinks and returns each hardlink name unchanged, so a lexical or
/// canonicalized comparison reports these as different files. `same_file`'s `dev`+`ino` pair is
/// the only test that sees it — which is why R3 reuses round 5's mechanism rather than writing a
/// second one that would re-acquire this hole.
#[test]
fn identity_out_hardlinked_to_a_generated_input_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let first = generate_corpus(&out);
    assert!(
        first.ok,
        "the fixture generation must succeed: {}",
        first.all()
    );
    let ddl = out.join("ws0-events.cql");
    let before = std::fs::read(&ddl).expect("read the generated DDL");

    let link = dir.path().join("looks-unrelated.json");
    std::fs::hard_link(&ddl, &link).expect("hardlink the DDL");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        link.to_str().expect("utf8"),
    ]);

    assert!(
        !run.ok,
        "a HARDLINK to a generated input is the SAME FILE and must be refused; canonicalize \
         cannot see it, so this case is what proves the check uses file identity. Output:\n{}",
        run.all()
    );
    let after = std::fs::read(&ddl).expect("re-read the DDL");
    assert_eq!(
        before, after,
        "the hardlinked input must survive — writing through the link would destroy it"
    );
}

/// THE ACCEPT DIRECTION, without which every case above is satisfied by a flag that never works.
///
/// A legitimate `--identity-out` outside the corpus must be WRITTEN, and every generated input
/// must survive.
#[test]
fn a_legitimate_identity_out_is_written_and_every_input_survives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let record = dir.path().join("in-tree-record.json");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        record.to_str().expect("utf8"),
    ]);

    assert!(
        run.ok,
        "an --identity-out OUTSIDE the corpus is the documented use and must succeed: {}",
        run.all()
    );
    let written = std::fs::read_to_string(&record).expect("the identity record must be written");
    assert!(
        written.contains("data_db_sha256"),
        "the record must be a corpus identity: {written}"
    );
    // Every generated input is present and non-empty — the guard did not cost the accept path.
    let ddl = std::fs::read(out.join("ws0-events.cql")).expect("the DDL must exist");
    assert!(!ddl.is_empty(), "the emitted DDL must be non-empty");
    let table = out.join("ws0").join("events");
    let components: Vec<_> = std::fs::read_dir(&table)
        .expect("the table dir must exist")
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        components.len() >= 5,
        "the corpus components must all survive a legitimate --identity-out (found {})",
        components.len()
    );
    // ...and the identity beside the data is written too, unaffected by --identity-out.
    assert!(
        out.join("corpus-identity.json").exists(),
        "the copy beside the data is written unconditionally"
    );
}

/// `--identity-out` naming the corpus root's OWN `corpus-identity.json` stays a NO-OP, not a
/// refusal.
///
/// That path is written unconditionally and the second write is skipped when they are equal, so
/// naming it is redundant rather than destructive. Asserted so the guard's scope is deliberate:
/// widening it to "any path under `--out`" would break this documented spelling.
#[test]
fn identity_out_naming_the_corpus_own_identity_is_still_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    let same = out.join("corpus-identity.json");

    let run = gen(&[
        "--out",
        out.to_str().expect("utf8"),
        "--identity-out",
        same.to_str().expect("utf8"),
    ]);

    assert!(
        run.ok,
        "--identity-out naming the corpus's own identity is the documented no-op, not a \
         destroyed input: {}",
        run.all()
    );
    assert!(
        same.exists(),
        "the identity beside the data must exist: {}",
        run.all()
    );
}
