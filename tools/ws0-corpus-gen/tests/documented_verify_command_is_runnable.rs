//! THE DOCUMENTED DIGEST-ORACLE COMMAND MUST BE RUNNABLE (issue #3272 round 10, L1).
//!
//! # The finding
//!
//! Round 9's F2 made the Arrow-digest oracle's pinned expectations MANDATORY: a
//! `CQLITE_WS0_CORPUS_DIR` supplied without `CQLITE_WS0_EXPECT_ARROW_{ROWS,BATCHES,DIGEST}` is
//! REFUSED. Correct — the pre-fix oracle only checked that its two taps agreed WITH EACH OTHER and
//! then PRINTED the digest, so both arms drifting together exited 0 and `ARROW_BUFFER_DIGEST` /
//! `ARROW_BUFFER_BATCHES` were compared against an observation by nothing, ever.
//!
//! But the command documented in `docs/reports/ws0-3096-artifacts/measurement-method.md` §5 still
//! set ONLY `CQLITE_WS0_CORPUS_DIR`, so from that commit onward the documented command FAILED
//! IMMEDIATELY. Third instance of that shape on this issue (round 9's F1 `operator_verify_corpus`,
//! the Arrow-digest procedure deferred to #3326, and this one), and the cost is not cosmetic: a
//! command that always fails teaches an operator to stop running it, which loses the check.
//!
//! # What is asserted here, and why it is a HELPER rather than a corrected string
//!
//! Retyping the three expectations into the markdown would fix this instance and leave the shape:
//! the next re-pin of `ARROW_BUFFER_DIGEST` staleifies it again, and a stale expectation is WORSE
//! than an absent one, because the oracle would then compare against a value nobody chose. So the
//! command is EMITTED from the pins by the `ws0-verify-commands` bin (which delegates to
//! `measurement_corpus::operator_verify_digest`), and this file asserts the three properties that
//! make that a fix rather than a relocation:
//!
//!   1. the helper RUNS and emits every variable the Flight oracle REQUIRES, with the values the
//!      pins hold (a helper that omitted one would reproduce the finding);
//!   2. the DOCUMENTATION points at the helper and no longer carries the bare form that cannot
//!      succeed — asserted against the real committed markdown, so an edit that reintroduces the
//!      hand-written command reds;
//!   3. the variable set is read out of `cqlite-flight`'s oracle SOURCE, not restated here — so
//!      the oracle adding a fourth required expectation FAILS this test instead of silently making
//!      the documented command unrunnable a fourth time. That is the drift oracle the previous two
//!      instances lacked.
//!
//! # Hermeticity
//!
//! Runs one in-tree binary (`CARGO_BIN_EXE_…`, the pattern this crate's other integration tests
//! use) and reads two committed source files. No corpus, no network, no `cargo`, no `sudo`, and
//! nothing is written. This file does not name the perf driver, so it is outside the WS0
//! hermeticity lint's content census by construction rather than by exemption.

use std::path::{Path, PathBuf};
use std::process::Command;

use ws0_corpus_gen::measurement_corpus as mc;

const HELPER: &str = env!("CARGO_BIN_EXE_ws0-verify-commands");

/// The committed operator documentation whose §5 command this test is about.
const METHOD_DOC: &str = "docs/reports/ws0-3096-artifacts/measurement-method.md";

/// The Flight oracle whose REQUIRED expectations the documented command must supply.
const FLIGHT_ORACLE: &str = "cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| panic!("the crate manifest dir has no grandparent — expected <repo>/tools/ws0-corpus-gen"))
}

fn read_tracked(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read the committed {rel} ({}): {e}", path.display()))
}

/// Run the helper in `mode` over `root` and return its stdout, asserting it SUCCEEDED.
fn helper(mode: &str, root: &str) -> String {
    let out = Command::new(HELPER)
        .args([mode, root])
        .output()
        .unwrap_or_else(|e| panic!("{HELPER} {mode} {root} did not run: {e}"));
    assert!(
        out.status.success(),
        "the helper must SUCCEED — an operator asking for a command must not be handed an error. \
         mode={mode} status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// THE EXPECTATIONS THE FLIGHT ORACLE REQUIRES, read out of ITS SOURCE (#3272 L1).
///
/// This is the property that stops a fourth instance: the required set is DISCOVERED from the
/// oracle rather than restated here, so an oracle that starts requiring a fourth expectation makes
/// this test fail rather than making the documented command silently unrunnable.
///
/// A `const … : &str = "CQLITE_WS0_EXPECT_…"` in that file is the declaration being read. Finding
/// NONE is a FAILURE, not an empty pass — an empty required set would make every assertion below
/// vacuously true, which is this issue's own defect class.
fn required_expectation_vars() -> Vec<String> {
    let src = read_tracked(FLIGHT_ORACLE);
    let mut found: Vec<String> = Vec::new();
    for line in src.lines() {
        let t = line.trim();
        // Only the DECLARATIONS, so a mention inside a panic message cannot inflate the set.
        if !t.starts_with("const ") {
            continue;
        }
        let Some(open) = t.find('"') else { continue };
        let rest = &t[open + 1..];
        let Some(close) = rest.find('"') else { continue };
        let value = &rest[..close];
        if value.starts_with("CQLITE_WS0_EXPECT_") && !found.iter().any(|v| v == value) {
            found.push(value.to_string());
        }
    }
    assert!(
        !found.is_empty(),
        "no `CQLITE_WS0_EXPECT_*` constant was found in {FLIGHT_ORACLE}. Either the oracle stopped \
         requiring pinned expectations (in which case F2's fix has been undone) or this scan no \
         longer matches how they are declared. An EMPTY required set would make every assertion in \
         this file vacuously true, so it is a failure rather than a pass (#3272)."
    );
    found
}

/// THE HELPER SUPPLIES EVERY EXPECTATION THE ORACLE REQUIRES, at the pinned values.
#[test]
fn the_helper_emits_every_expectation_the_flight_oracle_requires() {
    let cmd = helper("--digest", "/data/ws0-3096");
    let required = required_expectation_vars();
    // Three, today. Asserted as a floor with the discovered set printed, so a fourth appearing is
    // reported as such rather than silently satisfying a `>= 1`.
    assert!(
        required.len() >= 3,
        "the oracle should require at least the rows/batches/digest expectations; discovered \
         {required:?}"
    );
    for var in &required {
        assert!(
            cmd.contains(&format!("{var}=")),
            "the emitted command must SET {var} (the oracle REFUSES a corpus dir without it), \
             discovered required set {required:?}:\n{cmd}"
        );
    }
    // The VALUES, from the pins — a command setting a variable to something else would satisfy a
    // name-only check while pinning nothing.
    assert!(
        cmd.contains(&format!("{}={}", mc::EXPECT_ROWS_ENV, mc::ROWS)),
        "the pinned {} rows:\n{cmd}",
        mc::ROWS
    );
    assert!(
        cmd.contains(&format!(
            "{}={}",
            mc::EXPECT_BATCHES_ENV,
            mc::ARROW_BUFFER_BATCHES
        )),
        "the pinned {} batches:\n{cmd}",
        mc::ARROW_BUFFER_BATCHES
    );
    assert!(
        cmd.contains(&format!(
            "{}=0x{:016x}",
            mc::EXPECT_DIGEST_ENV,
            mc::ARROW_BUFFER_DIGEST
        )),
        "the pinned digest 0x{:016x}:\n{cmd}",
        mc::ARROW_BUFFER_DIGEST
    );
    // ...and the command it prints must be the test invocation, not just an env prefix.
    assert!(
        cmd.contains("cargo test -p cqlite-flight --test issue_3096_arrow_buffer_digest"),
        "the emitted command must name the oracle to run:\n{cmd}"
    );
}

/// THE HELPER NAMES THE ROOT IT WAS GIVEN, SHELL-QUOTED (#3272 L1 + L2 at one call site).
#[test]
fn the_helper_names_the_given_root_and_quotes_it() {
    let plain = helper("--digest", "/scratch/elsewhere");
    assert!(plain.contains("/scratch/elsewhere"), "{plain}");
    assert!(
        !plain.contains("/data/ws0-3096"),
        "the helper must not carry a hardcoded example root when given another (#3272 round 4): \
         {plain}"
    );
    // A whitespace/metacharacter-bearing root must arrive QUOTED, or the pasted command breaks.
    let hazard = "/scratch/ws0 corpus$(id)";
    let hazardous = helper("--digest", hazard);
    assert!(
        hazardous.contains(&mc::shell_quote(hazard)),
        "the helper must emit the SHELL-QUOTED root:\n{hazardous}"
    );
}

/// `--all` emits BOTH procedures, in the order they are run.
#[test]
fn the_all_mode_emits_both_procedures_in_order() {
    let cmd = helper("--all", "/scratch/both");
    let gen_at = cmd
        .find("--verify-against")
        .unwrap_or_else(|| panic!("--all must emit the corpus regeneration/verify step:\n{cmd}"));
    let digest_at = cmd
        .find(mc::EXPECT_DIGEST_ENV)
        .unwrap_or_else(|| panic!("--all must emit the digest step:\n{cmd}"));
    assert!(
        gen_at < digest_at,
        "the corpus must be generated/verified BEFORE the digest is re-folded over it:\n{cmd}"
    );
}

/// A MISUSE IS A USAGE ERROR, never a default mode — a helper that guesses can hand an operator a
/// command for the wrong corpus.
#[test]
fn a_missing_or_unknown_mode_is_a_usage_error() {
    for args in [
        vec![],
        vec!["--digest"],
        vec!["--digest", ""],
        vec!["--nonsense", "/scratch/x"],
        vec!["--digest", "/scratch/x", "extra"],
    ] {
        let out = Command::new(HELPER)
            .args(&args)
            .output()
            .unwrap_or_else(|e| panic!("{HELPER} {args:?} did not run: {e}"));
        assert_eq!(
            out.status.code(),
            Some(2),
            "{args:?} must exit 2 (usage), got {:?}\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            String::from_utf8_lossy(&out.stdout).trim().is_empty(),
            "a usage error must print NO command on stdout — a partial command is worse than \
             none, because it looks pasteable: {args:?}"
        );
    }
}

/// THE DOCUMENTED COMMAND POINTS AT THE HELPER, AND THE UNRUNNABLE FORM IS GONE (#3272 L1).
///
/// # Non-vacuity, which is the substance of this test
///
/// The pre-fix documentation set `CQLITE_WS0_CORPUS_DIR=…` and then invoked the oracle with NO
/// expectations — a form the oracle REFUSES. So the assertion is not merely "the doc mentions the
/// helper": it is that no line in that block still sets the corpus dir as the ONLY expectation-free
/// prefix to an oracle invocation. Stated over the real committed markdown, so reintroducing the
/// hand-written command reds this test.
#[test]
fn the_documented_command_block_is_runnable() {
    let doc = read_tracked(METHOD_DOC);
    assert!(
        doc.contains("ws0-verify-commands"),
        "{METHOD_DOC} must ask the pins for the digest command rather than restating the \
         expectations — a restated expectation goes stale on the next re-pin, and a STALE \
         expectation is worse than an absent one because the oracle then compares against a value \
         nobody chose (#3272 L1)"
    );
    // THE UNRUNNABLE FORM: a `CQLITE_WS0_CORPUS_DIR=` assignment whose command is the oracle with
    // no expectation set anywhere in its continuation. Reconstructed by joining backslash
    // continuations, because that is how the pre-fix block was written.
    let required = required_expectation_vars();
    let mut logical = String::new();
    let mut offences: Vec<String> = Vec::new();
    for raw in doc.lines() {
        let line = raw.trim();
        // Comment lines legitimately NAME the variables while setting nothing.
        if line.starts_with('#') {
            continue;
        }
        if let Some(head) = line.strip_suffix('\\') {
            logical.push_str(head.trim_end());
            logical.push(' ');
            continue;
        }
        logical.push_str(line);
        let joined = std::mem::take(&mut logical);
        if joined.contains("CQLITE_WS0_CORPUS_DIR=")
            && joined.contains("--test issue_3096_arrow_buffer_digest")
            && !required.iter().any(|v| joined.contains(v.as_str()))
        {
            offences.push(joined);
        }
    }
    assert!(
        offences.is_empty(),
        "{METHOD_DOC} still documents the form the oracle REFUSES — it sets \
         CQLITE_WS0_CORPUS_DIR and invokes issue_3096_arrow_buffer_digest without any of the \
         REQUIRED expectations {required:?}, which fails immediately (#3272 L1). Point the \
         operator at `ws0-verify-commands --digest <root>` instead.\nOffending command(s):\n{}",
        offences.join("\n")
    );
    // NON-VACUITY for the detector itself: the PRE-FIX text must be something it CATCHES. Run the
    // same rule over the exact command the documentation used to carry.
    let prefix_form = "CQLITE_WS0_CORPUS_DIR=/data/ws0-3096 \\\n  cargo test -p cqlite-flight \
                       --test issue_3096_arrow_buffer_digest -- --nocapture";
    let flat = prefix_form.replace("\\\n", " ");
    assert!(
        flat.contains("CQLITE_WS0_CORPUS_DIR=")
            && flat.contains("--test issue_3096_arrow_buffer_digest")
            && !required.iter().any(|v| flat.contains(v.as_str())),
        "the detector above would not have caught the PRE-FIX command, so its silence over the \
         current document proves nothing: {flat}"
    );
}
