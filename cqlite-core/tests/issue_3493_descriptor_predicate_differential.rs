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
//! Both directions matter and both are asserted:
//!   * shell ACCEPT / reader REJECT -- a false-PRESENT: an unreadable fixture passes the manifest
//!     and the Node suite then fails on it, with the #2078 opt-out unable to suppress it.
//!   * shell REJECT / reader ACCEPT -- a false-MISSING: a readable corpus is reported incomplete
//!     and the suite is suppressed, silently losing the coverage this whole issue exists to add.

#![cfg(unix)]

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
const NAMES: &[&str] = &[
    // ordinary, and the two ID forms real Cassandra 5.0 writes
    "nb-1-big-Data.db",
    "nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db",
    "nb-6aa08200-a251-11f0-a3fe-f1a551383fb9-big-Data.db",
    "oa-1-big-Data.db",
    "da-2-bti-Data.db",
    // arbitrary ID: parses, and the version gate lets it through (round 56)
    "nb-foo-big-Data.db",
    "nb--big-Data.db",
    "nb-x-big-y-big-Data.db",
    // version/format pairings the gates reject
    "nb-1-bti-Data.db", // BTI accepts only `da`
    "nc-1-big-Data.db", // above the #1249 floor, outside the #1297 allowlist
    "ma-1-big-Data.db", // pre-`na`, out of scope
    "zz-9-big-Data.db",
    // HYPHENATED COMPONENT (round 57): parses, passes the version gate, and its component is
    // "Foo-Data.db" -- a component that merely ENDS IN `-Data.db`, not the Data component. The
    // manifest globs `*-Data.db`, so these names reach the predicate and must be rejected.
    "nb-1-big-Foo-Data.db",
    "oa-1-big-Foo-Data.db",
    "da-2-bti-Foo-Data.db",
    // ... and the same shape for a real non-Data component, which must also not count as a
    // generation even though the file is a legitimate part of one.
    "nb-1-big-CompressionInfo.db",
    "nb-1-big-Statistics.db",
    // not descriptors at all
    "junk-Data.db",
    "Data.db",
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
fn reader_accepts(name: &str) -> bool {
    match SsTableDescriptor::parse_filename(name) {
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
fn shell_accepts(name: &str) -> bool {
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
        .arg(name)
        .output()
        .expect("failed to run bash");
    out.status.success()
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
                "{name}: reader={} shell={} ({})",
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
