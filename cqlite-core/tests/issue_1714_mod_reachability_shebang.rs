//! Issue #1714: the SHEBANG half of the `mod`-reachability sanitizer, measured against
//! rustc's own rule.
//!
//! # Why this is its own test file
//!
//! rustc DISCARDS a `#!` line at byte offset 0 (`rustc_lexer::strip_shebang`), so those
//! bytes are not Rust at all — while `#![…]` at offset 0 is an inner attribute and IS
//! Rust. Both directions are load-bearing and both are dangerous to get wrong:
//!
//! * reading a shebang as code invents declarations (`#!/usr/bin/env -S tool mod orphan;`)
//!   and makes an unreachable file look reachable — the silent FALSE PASS this guard
//!   exists to prevent;
//! * reading an inner attribute as a shebang blanks a line of real code, swallowing any
//!   `pub mod wired;` that shares it.
//!
//! rustc's rule is narrow and non-obvious (see `shebang_end`), so these cases are pinned
//! against **measured rustc behavior** rather than against a reading of the docs, and kept
//! together where the next reader can see the whole table at once.

#[path = "support/mod_reachability_harness.rs"]
mod harness;
#[path = "support/mod_reachability.rs"]
mod mod_reachability;

use harness::{stripped, ScratchCrate};
use mod_reachability::strip_comments_and_strings;

/// rustc DISCARDS a `#!` line at byte offset 0 of a source file, so whatever that line
/// says is not Rust at all. Parsing it as ordinary code is the false-PASS path this
/// asserts against: a `#!/usr/bin/env -S tool mod orphan;` first line would otherwise
/// yield a bogus `mod orphan;` declaration and make an unreachable file look reachable —
/// the exact silent green this guard exists to prevent (#1714).
#[test]
fn shebang_line_at_offset_zero_does_not_declare_a_module() {
    let tree = ScratchCrate::new("shebang-not-a-decl");
    tree.write(
        "src/lib.rs",
        "#!/usr/bin/env -S tool mod orphan;\npub mod wired;\n",
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "a `mod orphan;` inside a SHEBANG line (which rustc discards) must NOT make \
         `orphan.rs` reachable; enumerated={:?} reachable={:?}",
        report.enumerated,
        report.reachable
    );
    assert!(
        report.reachable.contains("src/wired.rs"),
        "a shebang-bearing root must still resolve the real declarations below it"
    );
}

/// The other direction of the same fix, and the one that would be catastrophic to get
/// backwards: `#![...]` at byte offset 0 is a Rust INNER ATTRIBUTE (this crate's own
/// `lib.rs` opens with one), not a shebang. Blanking it would eat the rest of that line,
/// so the real `pub mod wired;` sharing it must survive.
#[test]
fn inner_attribute_at_offset_zero_is_not_mistaken_for_a_shebang() {
    let tree = ScratchCrate::new("inner-attr-not-shebang");
    tree.write("src/lib.rs", "#![allow(dead_code)] pub mod wired;\n");
    tree.write("src/wired.rs", "pub fn f() {}\n");

    let report = tree.analyze();
    assert!(
        report.orphans.is_empty(),
        "`#![allow(dead_code)]` at offset 0 is an inner attribute, not a shebang — \
         blanking its line swallowed the real `pub mod wired;`: orphans={:?}",
        report.orphans
    );
    let out = stripped("#![allow(dead_code)] pub mod wired;\n");
    assert_eq!(
        out, "#![allow(dead_code)] pub mod wired;\n",
        "an inner attribute at offset 0 must reach the parser byte-for-byte"
    );
}

/// The shebang rule applies at BYTE OFFSET 0 and nowhere else: an implementation that
/// stripped any line merely *starting* with `#!` would eat this file's real declaration,
/// which sits on the ordinary `#![...]`-after-a-comment-header line every Rust crate has.
#[test]
fn hash_bang_after_offset_zero_is_not_a_shebang() {
    let tree = ScratchCrate::new("hashbang-not-at-zero");
    tree.write(
        "src/lib.rs",
        "//! Crate docs, line 1.\n//! Line 2.\n//! Line 3.\n\n#![allow(dead_code)] pub mod wired;\n",
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");

    let report = tree.analyze();
    assert!(
        report.orphans.is_empty(),
        "a `#!` line that is NOT at byte offset 0 is not a shebang; its declarations must \
         survive: orphans={:?}",
        report.orphans
    );
}
/// A shebang line is blanked like every other non-Rust span: same byte length, newline
/// preserved, so byte offsets and reported line numbers stay aligned with the original
/// source. The both-directions half is asserted too — `#![...]` at offset 0 is an inner
/// attribute and must reach the parser untouched — plus the fail-closed refusal for the
/// one shape whose shebang-vs-attribute reading this walker does not model.
#[test]
fn stripper_blanks_a_shebang_but_never_an_inner_attribute() {
    let src = "#!/usr/bin/env cargo\nmod a;\n";
    let out = stripped(src);
    assert_eq!(
        out, "                    \nmod a;\n",
        "the shebang line must be blanked in place (offsets and newline preserved): {out:?}"
    );
    assert_eq!(out.len(), src.len(), "byte offsets must be preserved");
    assert_eq!(
        out.matches('\n').count(),
        src.matches('\n').count(),
        "line breaks must be preserved so reported line numbers stay correct"
    );

    for attr in [
        "#![allow(dead_code)]\nmod a;\n",
        "#![doc = \"not a shebang\"]\nmod a;\n",
    ] {
        // The literal's own value is blanked (it is data), so compare the code skeleton.
        let out = stripped(attr);
        assert!(
            out.starts_with("#![") && out.contains("mod a;"),
            "`#![...]` at offset 0 is an inner attribute, not a shebang: {attr:?} -> {out:?}"
        );
    }

    // `#!` + whitespace/comment + `[` is an inner attribute to rustc (it skips both
    // before deciding) and a shebang to a naive next-byte test. This walker models
    // neither reading, so it refuses rather than guessing (FAIL-CLOSED).
    for src in [
        "#! [allow(dead_code)]\nmod a;\n",
        "#!\n[allow(dead_code)]\nmod a;\n",
        "#!/* c */[allow(dead_code)]\nmod a;\n",
        "#!// c\n[allow(dead_code)]\nmod a;\n",
    ] {
        assert!(
            strip_comments_and_strings(src).is_err(),
            "the unmodeled shebang-vs-inner-attribute shape must fail closed: {src:?}"
        );
    }
}
