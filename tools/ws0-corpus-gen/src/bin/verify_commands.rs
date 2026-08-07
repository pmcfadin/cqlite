//! `ws0-verify-commands` — EMIT the operator verification commands FROM the in-tree pins
//! (issue #3272 round 10, L1).
//!
//! # The finding, and why it is the THIRD of its shape on this issue
//!
//! Round 9's F2 fix made the Arrow-digest oracle's pinned expectations MANDATORY: a
//! `CQLITE_WS0_CORPUS_DIR` supplied without `CQLITE_WS0_EXPECT_ARROW_{ROWS,BATCHES,DIGEST}` is now
//! REFUSED, because the pre-fix oracle only checked that its two taps agreed WITH EACH OTHER and
//! then printed the digest — so both arms drifting together exited 0 and the pin was compared
//! against nothing. Correct. But the documented command in
//! `docs/reports/ws0-3096-artifacts/measurement-method.md` §5 still set ONLY the corpus dir, so
//! from that commit onward THE DOCUMENTED COMMAND FAILED IMMEDIATELY.
//!
//! That is the third time on this issue that a fix has made a documented operator command
//! unusable:
//!
//!   1. round 9's F1 — `operator_verify_corpus` became permanently unable to succeed, because a
//!      newly-REQUIRED `schema_sha256` met an artifact that predates the field;
//!   2. the Arrow-digest procedure deferred to #3326;
//!   3. this one.
//!
//! A command that always fails teaches an operator to stop running it, which loses the whole
//! check — so the failure mode is not cosmetic.
//!
//! # THE FIX IS A HELPER, NOT A THIRD HAND-MAINTAINED STRING
//!
//! Every instance above has the same cause: a command an operator runs was written down in a
//! place that a change to the code it invokes cannot reach. Editing the markdown to add the three
//! expectations would fix THIS instance and leave the shape intact — the next re-pin of
//! [`ARROW_BUFFER_DIGEST`] silently staleifies it again, and a stale expectation is worse than an
//! absent one because the oracle would then compare against a value nobody chose.
//!
//! So the command is EMITTED from the constants, by
//! [`ws0_corpus_gen::measurement_corpus::operator_verify_digest`] — the same function the Flight
//! oracle's own refusal message already names, and which
//! `measurement_corpus::tests::the_digest_procedure_supplies_the_pinned_expectations` asserts
//! carries the formatted pins. A re-pin therefore changes what this prints, in the same commit,
//! with no documentation edit and no possibility of drift.
//!
//! # Why a BIN and not a flag on `ws0-corpus-gen`
//!
//! `ws0-corpus-gen`'s `--out` is a REQUIRED argument (it is a generator; a run without an output
//! root is meaningless). A print-only mode there would have to make `--out` optional, which loosens
//! a required argument on the binary that WRITES 2.8 GB in order to add a mode that writes nothing.
//! A separate zero-side-effect bin is the smaller change and cannot affect the generator's argument
//! surface at all.
//!
//! # It writes NOTHING and reads NOTHING
//!
//! No filesystem access, no corpus, no network: it formats two strings from compiled-in constants.
//! The root it is given need not exist — the commands it prints are for a corpus the operator is
//! about to generate.

use std::process::ExitCode;

use ws0_corpus_gen::measurement_corpus as mc;

const USAGE: &str = "\
ws0-verify-commands — print the #3096 corpus/digest verification commands FROM the in-tree pins

USAGE:
    ws0-verify-commands --digest <CORPUS_ROOT>
    ws0-verify-commands --corpus <CORPUS_ROOT>
    ws0-verify-commands --all    <CORPUS_ROOT>

    --digest   the Arrow-buffer digest oracle, WITH the three pinned expectations it REQUIRES
               (CQLITE_WS0_EXPECT_ARROW_ROWS/BATCHES/DIGEST). This is the command
               docs/reports/ws0-3096-artifacts/measurement-method.md §5 refers to; it is emitted
               rather than written down so a re-pin of the digest cannot leave it stale.
    --corpus   the ~2.8 GB regeneration + --verify-against identity check.
    --all      both, in the order an operator runs them (generate/verify, then re-fold).

The root is SHELL-QUOTED in the emitted commands, so a path containing whitespace or a shell
metacharacter is safe to paste (#3272 L2).

Writes nothing, reads nothing: the output is formatted from compiled-in constants, so the root
need not exist yet.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // An UNRECOGNISED invocation is a usage error, never a default mode: a helper that guesses
    // what an operator meant can hand them a command for the wrong corpus.
    let (mode, root) = match args.as_slice() {
        [mode, root] if !root.is_empty() => (mode.as_str(), root.as_str()),
        _ => {
            eprintln!("{USAGE}");
            eprintln!(
                "\nERROR: expected exactly two arguments (a mode and a NON-EMPTY corpus root), \
                 got {}: {args:?}",
                args.len()
            );
            return ExitCode::from(2);
        }
    };
    match mode {
        "--digest" => println!("{}", mc::operator_verify_digest(root)),
        "--corpus" => println!("{}", mc::operator_verify_corpus(root)),
        "--all" => {
            println!("# 1. regenerate + verify the corpus identity (minutes, ~2.8 GB):");
            println!("{}", mc::operator_verify_corpus(root));
            println!("\n# 2. re-fold the Arrow-buffer digest over it and COMPARE to the pins:");
            println!("{}", mc::operator_verify_digest(root));
        }
        other => {
            eprintln!("{USAGE}");
            eprintln!("\nERROR: unknown mode {other:?} (expected --digest, --corpus or --all)");
            return ExitCode::from(2);
        }
    }
    ExitCode::SUCCESS
}
