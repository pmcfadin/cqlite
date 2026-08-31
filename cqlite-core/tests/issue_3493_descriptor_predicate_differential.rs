//! Differential: `check-dataset-manifest.sh`'s `_reader_accepts_descriptor` vs the REAL reader.
//!
//! UNIX-ONLY (roborev #3493 round 58). The differential invokes `bash`/`sed` to exercise the
//! shell predicate ITSELF, which is the whole point -- it must run the COMMITTED script rather
//! than a Rust re-implementation, since a re-implementation is the drift this test exists to
//! detect. Established precedent in this crate: 5+ cqlite-core tests carry a `cfg(unix)` guard,
//! and no CI lane runs cqlite-core on Windows.
//!
//! The shell function is a PORT of two Rust decisions -- `SsTableDescriptor::parse_filename`
//! and the version gates -- and a port's correctness is only knowable by differential testing
//! against the original, never against a model of it (CLAUDE.md, #3283). Reading the Rust and
//! writing a matching regex is exactly the "tested against a model of Go" mistake that issue
//! records; this test asks the original.
//!
//! It has already paid for itself twice. Rounds 36-38 argued about whether a BIG version
//! allowlist existed (it does, and two windowed reads missed it), and round 56 found the ID
//! charset was `[0-9a-f-]+` while the parser imposes NO charset at all -- so `nb-foo-big-Data.db`
//! was judged unreadable, making the manifest STRICTER than the reader and suppressing the Node
//! suite for a corpus the reader opens.
//!
//! WHAT THIS DOES **NOT** ESTABLISH, measured and recorded so the scope is not overread.
//! Production DISCOVERY is not this predicate: `manager_open.rs` takes any
//! `filename.ends_with("-Data.db")`, and `SSTableComponent::from_filename` maps any such name
//! to the Data component -- neither consults `parse_filename`. A per-file open error is then
//! logged and SKIPPED (best-effort load). Measured with garbage bytes beside a healthy `nb-1`:
//!
//!     nb-1-big-Foo-Data.db -> the query THROWS      (shares the nb-1-big- prefix)
//!     junk-Data.db         -> 100 rows, tolerated   (discovered, open fails, skipped)
//!     nb-9-big-Data.db     -> 100 rows, tolerated   (valid descriptor, other generation)
//!     xx-1-big-Data.db     -> 100 rows, tolerated
//!     nb-foo-big-Data.db   -> 100 rows, tolerated
//!
//! So "the reader would open this" and "an unusable file here breaks the run" are DIFFERENT
//! questions, and only the prefix-collision shape is fatal. That shape is checked in
//! `check-dataset-manifest.sh` directly, not here. This test's subject is narrower and worth
//! stating plainly: that the shell predicate's notion of a GENERATION matches the descriptor
//! parser plus the version gates. It is not a model of discovery and must not be read as one.
//!
//! Both directions matter and both are asserted:
//!   * shell ACCEPT / reader REJECT -- a false-PRESENT: an unreadable fixture passes the manifest
//!     and the Node suite then fails on it, with the #2078 opt-out unable to suppress it.
//!   * shell REJECT / reader ACCEPT -- a false-MISSING: a readable corpus is reported incomplete
//!     and the suite is suppressed, silently losing the coverage this whole issue exists to add.

#![cfg(unix)]

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::Command;

use cqlite_core::storage::sstable::version_gate::{
    BigVersionGates, BtiVersionGates, SsTableDescriptor, SsTableFormat,
};

/// Every name shape either side has ever disagreed about, plus the ordinary ones.
///
/// Deliberately includes shapes no writer emits (`nb--big`, `nb-x-big-y-big`): "no writer emits
/// that" is precisely the reasoning that produced the round-56 defect, so the port is held to the
/// parser's ACTUAL behaviour rather than to the subset someone expects to see on disk.
const NAMES: &[&[u8]] = &[
    // ordinary, and the two ID forms real Cassandra 5.0 writes
    b"nb-1-big-Data.db",
    b"nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db",
    b"nb-6aa08200-a251-11f0-a3fe-f1a551383fb9-big-Data.db",
    b"oa-1-big-Data.db",
    b"da-2-bti-Data.db",
    // arbitrary ID: parses, and the version gate lets it through (round 56)
    b"nb-foo-big-Data.db",
    b"nb--big-Data.db",
    b"nb-x-big-y-big-Data.db",
    // version/format pairings the gates reject
    b"nb-1-bti-Data.db", // BTI accepts only `da`
    b"nc-1-big-Data.db", // above the #1249 floor, outside the #1297 allowlist
    b"ma-1-big-Data.db", // pre-`na`, out of scope
    b"zz-9-big-Data.db",
    // HYPHENATED COMPONENT (round 57): parses, passes the version gate, and its component is
    // "Foo-Data.db" -- a component that merely ENDS IN `-Data.db`, not the Data component. The
    // manifest globs `*-Data.db`, so these names reach the predicate and must be rejected.
    b"nb-1-big-Foo-Data.db",
    b"oa-1-big-Foo-Data.db",
    b"da-2-bti-Foo-Data.db",
    // ... and the same shape for a real non-Data component, which must also not count as a
    // generation even though the file is a legitimate part of one.
    b"nb-1-big-CompressionInfo.db",
    b"nb-1-big-Statistics.db",
    // not descriptors at all
    b"junk-Data.db",
    b"Data.db",
    // INVALID UTF-8 (round 6). Both discovery sites and the parser go through
    // `file_name().and_then(|n| n.to_str())`, which returns None here, so the reader skips
    // the file — while the shell's `.*` matches arbitrary BYTES. Expressible only as bytes,
    // which is why the vector set is `&[u8]` rather than `&str`.
    b"nb-\xff-big-Data.db",
];

/// What the READER does for the question the MANIFEST is asking: would it open this file as
/// a **Data.db generation**?
///
/// Three conditions, and the third was missing (roborev #3493 round 57 raised the case; the
/// measurement below reversed its conclusion). Parse + version gate alone is a LOOSER oracle
/// than the manifest's question, because `parse_filename` splits
/// `<version>-<id>-<format>-<component>` by scanning right-to-left for the format segment and
/// treats EVERYTHING after it as the component. Measured against the real parser:
///
///     nb-1-big-Data.db      -> component "Data.db"
///     nb-1-big-Foo-Data.db  -> component "Foo-Data.db"     <-- parses, but is NOT a Data.db
///
/// So `nb-1-big-Foo-Data.db` is a *validly named component that is not the Data component*.
/// The shell predicate rejecting it is CORRECT, and the round-57 proposal to accept it would
/// have made the manifest treat a non-Data component as a generation. What was genuinely
/// wrong was this oracle: it would have called that name "reader-accepted" and reported a
/// drift that does not exist.
fn reader_accepts(name: &[u8]) -> bool {
    // Through a PATH, as production does: `parse` calls `file_name().and_then(to_str)`, so an
    // invalid-UTF-8 basename is rejected there rather than by any charset rule.
    let p = PathBuf::from(OsStr::from_bytes(name));
    match SsTableDescriptor::parse(&p) {
        Err(_) => false,
        Ok(d) => {
            let gated = match d.format {
                SsTableFormat::Big => BigVersionGates::from_version(&d.version).is_ok(),
                _ => BtiVersionGates::from_version(&d.version).is_ok(),
            };
            gated && d.component == "Data.db"
        }
    }
}

fn manifest_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data/scripts/check-dataset-manifest.sh")
}

/// What the SHELL PORT does. Sources the two functions out of the committed script rather than
/// re-implementing them here -- a copy would drift, which is the defect class this test exists for.
fn shell_accepts(name: &[u8]) -> bool {
    let script = manifest_script();
    // A plain literal, not `format!`: nothing is interpolated, and clippy's `useless_format`
    // is right to say so.
    let program = "eval \"$(sed -n '/^_re_match() {/,/^}/p;/^_reader_accepts_descriptor() {/,/^}/p' \"$1\")\"\n\
                   _reader_accepts_descriptor \"$2\"";
    let out = Command::new("bash")
        .arg("-c")
        .arg(program)
        .arg("bash")
        .arg(&script)
        .arg(OsStr::from_bytes(name))
        .output()
        .expect("failed to run bash");
    // TRI-STATE, not `status.success()` (roborev, post-rebase round 6). Collapsing every
    // nonzero status onto "reject" hides an OPERATIONAL failure — exit 2 (the script's own
    // malfunction code) or 127 (bash could not run) — whenever the reader happens to reject
    // the same name, which is most of the negative vectors. That is the very
    // status-collapse family this differential exists to police, in the differential itself.
    match out.status.code() {
        Some(0) => true,
        Some(1) => false,
        other => panic!(
            "shell predicate exited {other:?} for {:?} — that is a MALFUNCTION (2 = the \
             script's own tooling-failure code, 127 = bash/sed missing), not a verdict. \
             stderr: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(&out.stderr)
        ),
    }
}

#[test]
fn shell_predicate_matches_the_reader() {
    let script = manifest_script();
    assert!(
        script.is_file(),
        "committed manifest script not found at {} -- the differential cannot run, and a \
         silently skipped differential is the vacuous pass this test exists to prevent",
        script.display()
    );

    let mut disagreements = Vec::new();
    for name in NAMES {
        let (r, s) = (reader_accepts(name), shell_accepts(name));
        if r != s {
            disagreements.push(format!(
                "{}: reader={} shell={} ({})",
                String::from_utf8_lossy(name),
                if r { "ACCEPT" } else { "REJECT" },
                if s { "ACCEPT" } else { "REJECT" },
                if s {
                    "false-PRESENT: an unreadable fixture would pass the manifest"
                } else {
                    "false-MISSING: a readable corpus would be reported incomplete"
                }
            ));
        }
    }
    assert!(
        disagreements.is_empty(),
        "check-dataset-manifest.sh's _reader_accepts_descriptor has drifted from the reader:\n  {}",
        disagreements.join("\n  ")
    );

    // A differential over an empty or all-agreeing-by-vacuity set proves nothing: require that
    // the corpus of names actually exercises BOTH verdicts.
    let accepted = NAMES.iter().filter(|n| reader_accepts(n)).count();
    assert!(
        accepted > 0 && accepted < NAMES.len(),
        "the differential must cover both verdicts; reader accepted {accepted}/{}",
        NAMES.len()
    );
}
