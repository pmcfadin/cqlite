//! Issue #1714 (AK3): every `cqlite-core/src/**/*.rs` file must be reachable from a
//! `mod` declaration chain rooted at `lib.rs`.
//!
//! # Why this guard exists
//!
//! An unreachable file is **never compiled**. `cqlite-core/src/memory_safety_tests.rs`
//! sat orphaned for months: 1000+ lines of "memory safety tests" that rustc never saw,
//! so every edit to it was a silent no-op and every one of its assertions was dead
//! text. Deleting it (PR #2044) fixed that instance; only a standing guard stops the
//! next one. This is that guard — the issue's own words: "the real deliverable".
//!
//! # Anti-vacuity: the guard is OBSERVED to fire, not merely present
//!
//! A reachability walker has one catastrophic failure mode — enumerating nothing, or
//! resolving nothing, and reporting green. So this lane:
//!
//! * asserts a **census floor** (a collapsed enumeration FAILs, never passes);
//! * asserts a **named positive case per resolution rule**, so each rule is seen working
//!   against the live tree;
//! * **demonstrates the detector on a synthetic tree** (a throwaway crate with one
//!   deliberate orphan), so the detector's teeth are proven independently of the live
//!   tree's state — it keeps working after #1715 and #3364 empty the exception list;
//! * proves `mod` mentions inside comments/strings are **data, not declarations**
//!   (CLAUDE.md #3312), the false-PASS path a raw-text scan would take;
//! * asserts the exception list **per entry**, fail-closed in BOTH directions (issue
//!   #3220: a suite-wide aggregate cannot see one case skipping behind its siblings).
//!
//! The walker itself lives in `support/mod_reachability.rs` and is crate-agnostic so
//! #1502 (the `cqlite-cli` mod-wiring guard) can drive it for `src/main.rs`.

#[path = "support/mod_reachability_harness.rs"]
mod harness;
#[path = "support/mod_reachability.rs"]
mod mod_reachability;

use std::fs;
use std::path::{Path, PathBuf};

use harness::{stripped, ScratchCrate};
use mod_reachability::{analyze, strip_comments_and_strings, ExpectedOrphan, ModuleGraphSpec};

const SRC_DIR: &str = "src";

/// Files known to be unreachable, each with the issue that owns removing it.
///
/// This list is **fail-closed both ways** (see [`exception_list_is_fail_closed`]): an
/// entry whose file is gone FAILs, and an entry whose file became reachable FAILs. So
/// #1715 and #3364 cannot land their deletions without deleting their entry here, and
/// the list cannot silently accumulate stale excuses.
const EXPECTED_ORPHANS: &[ExpectedOrphan] = &[
    ExpectedOrphan {
        path: "schema/cql_generator.rs",
        issue: "#1715",
        reason: "AK4 owns deleting this 856-LOC orphan; #1714 must not delete it here",
    },
    ExpectedOrphan {
        path: "storage/sstable/header_fix_functions.rs",
        issue: "#3364",
        reason: "#3364 owns deleting this 102-LOC orphan; #1714 must not delete it here",
    },
    // Found BY THIS GUARD on its first run over the live tree, and not by the hand
    // census that preceded it: `parser/mod.rs:115` carries a COMMENTED-OUT
    // `// pub mod collection_udt_tests;`, which a raw `mod <stem>` grep counts as a
    // wiring. Stripping comments before parsing is what exposes it — the guard's first
    // real catch, and the reason the control-vs-data requirement is not academic.
    ExpectedOrphan {
        path: "parser/collection_udt_tests.rs",
        issue: "#3365",
        reason: "#3365 owns deleting/wiring this 374-LOC orphan; #1714 must not decide it here",
    },
];

/// A collapsed census is the vacuous-pass failure mode, so the floor is asserted rather
/// than assumed. There were 481 `.rs` files under `cqlite-core/src` when this landed;
/// the floor is deliberately slack enough to survive ordinary refactoring.
const CENSUS_FLOOR: usize = 300;

fn core_spec() -> ModuleGraphSpec {
    ModuleGraphSpec {
        crate_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        root_file_rel: "src/lib.rs".to_string(),
        src_dir_rel: SRC_DIR.to_string(),
    }
}

// ---------------------------------------------------------------------------
// The live lane
// ---------------------------------------------------------------------------

#[test]
fn cqlite_core_src_has_no_unknown_orphan_modules() {
    let spec = core_spec();
    let report = match analyze(&spec) {
        Ok(report) => report,
        Err(cause) => panic!(
            "mod-reachability walk of `cqlite-core/src` could not complete: {cause}\n\
             This is a FAIL-CLOSED refusal, not a pass: the walker met a construct it does \
             not model, so it cannot certify that every file is compiled. Either teach the \
             walker that construct (tests/support/mod_reachability.rs) or remove it."
        ),
    };

    assert!(
        report.enumerated.len() >= CENSUS_FLOOR,
        "census collapsed: enumerated only {} `.rs` file(s) under `{}` (floor {CENSUS_FLOOR}).\n\
         A walker that enumerates (almost) nothing reports green while checking nothing — the \
         vacuous pass this guard exists to prevent. Fix the enumeration, do not lower the floor.",
        report.enumerated.len(),
        SRC_DIR
    );
    assert!(
        report.mod_decls_resolved >= CENSUS_FLOOR,
        "only {} `mod` declaration(s) resolved from `src/lib.rs` — the module graph did not \
         actually get walked, so every file would look orphaned (or, with an over-broad \
         exception list, every file would look fine). Vacuous either way.",
        report.mod_decls_resolved
    );

    let unexpected = report.unexpected_orphans(EXPECTED_ORPHANS, SRC_DIR);
    assert!(
        unexpected.is_empty(),
        "unreachable file(s) under `cqlite-core/src` — no `mod` chain from `lib.rs` reaches \
         them, so rustc NEVER COMPILES them and every edit to them is a silent no-op \
         (tests inside them never run):\n  {}\n\n\
         Two legitimate remedies:\n  \
         1. wire it in — add the `mod <name>;` declaration that makes it part of the crate;\n  \
         2. delete it — an uncompiled file is not code, it is text.\n\
         If it is deliberate dead code pending a tracked deletion, add an entry to \
         EXPECTED_ORPHANS in this file citing THAT issue (#1714 AK3).",
        unexpected.join("\n  ")
    );
}

#[test]
fn exception_list_is_fail_closed() {
    let spec = core_spec();
    let report = analyze(&spec).unwrap_or_else(|cause| panic!("walk failed: {cause}"));

    // Per ENTRY, never a suite-wide aggregate (#3220): an aggregate cannot see one
    // entry going stale behind its siblings.
    for expected in EXPECTED_ORPHANS {
        let key = format!("{SRC_DIR}/{}", expected.path);
        let abs = spec.crate_dir.join(&key);
        assert!(
            abs.is_file(),
            "stale exception: `{key}` (owner {}) no longer exists.\n\
             Remedy: the file was deleted — remove this exception entry from EXPECTED_ORPHANS.",
            expected.issue
        );
        assert!(
            report.orphans.contains(&key),
            "stale exception: `{key}` (owner {}) is now REACHABLE from `lib.rs`.\n\
             Remedy: this file is wired in — remove this exception entry from EXPECTED_ORPHANS.\n\
             Reason recorded when the entry was added: {}",
            expected.issue,
            expected.reason
        );
    }
}

/// One named live-tree case per resolution rule, so every rule is OBSERVED working.
/// If a rule silently stopped resolving, the live lane above would report a flood of
/// false orphans — but only after someone had already been told "everything is fine"
/// by a walker that quietly resolved nothing. These cases pin the rules directly.
#[test]
fn every_resolution_rule_is_observed_working() {
    let spec = core_spec();
    let report = analyze(&spec).unwrap_or_else(|cause| panic!("walk failed: {cause}"));

    let cases: &[(&str, &str)] = &[
        // `mod name;` in lib.rs -> `name.rs`
        (
            "src/config.rs",
            "plain `pub mod config;` in lib.rs -> config.rs",
        ),
        // `mod name;` in lib.rs -> `name/mod.rs`
        (
            "src/storage/mod.rs",
            "`pub mod storage;` in lib.rs -> storage/mod.rs",
        ),
        // `mod name;` in `foo.rs` -> `foo/name.rs` (a non-mod.rs file owns `foo/`)
        (
            "src/types/comparator.rs",
            "`pub mod comparator;` in types.rs -> types/comparator.rs",
        ),
        // cfg-gated modules are REACHABLE (issue #1714 AC-3) — no feature evaluation.
        (
            "src/types/comparator_test.rs",
            "`#[cfg(test)] mod comparator_test;` in types.rs is reachable despite the cfg",
        ),
        // `#[path = "…"]` is relative to the directory of the DECLARING FILE.
        (
            "src/observability/otel_tests.rs",
            "`#[path = \"otel_tests.rs\"]` in observability/otel.rs",
        ),
        (
            "src/export/arrow_size_render.rs",
            "`#[path = \"arrow_size_render.rs\"]` in export/arrow_size.rs",
        ),
        (
            "src/query/select_executor/row_build_alloc_budget_test.rs",
            "`#[path = \"row_build_alloc_budget_test.rs\"]` in query/select_executor/row_build.rs",
        ),
        (
            "src/storage/sstable/reader/scan_stream_windowed_guard.rs",
            "`#[path = \"scan_stream_windowed_guard.rs\"]` in reader/scan_stream_windowed.rs",
        ),
    ];

    for (key, rule) in cases {
        assert!(
            report.enumerated.contains(*key),
            "resolution-rule case `{key}` ({rule}) is not in the census — the case moved or \
             was renamed. Repoint it at a live file; do not delete the case, the rule needs a \
             witness."
        );
        assert!(
            report.reachable.contains(*key),
            "resolution rule NOT working: `{key}` should be reachable via {rule}, but the \
             walker did not reach it. A rule that stopped resolving turns real files into \
             false orphans (and, combined with a wide exception list, hides real ones)."
        );
    }
}

// ---------------------------------------------------------------------------
// Detector demonstration on synthetic trees (the mechanized TDD requirement)
// ---------------------------------------------------------------------------

#[test]
fn detector_reports_exactly_the_unreachable_file() {
    let tree = ScratchCrate::new("detects-orphan");
    tree.write("src/lib.rs", "pub mod wired;\n");
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "the detector must report EXACTLY the unreachable file; enumerated={:?} reachable={:?}",
        report.enumerated,
        report.reachable
    );
    assert_eq!(
        report.enumerated.len(),
        3,
        "census must see all three files"
    );
}

/// A `mod` mention inside a comment or a literal is DATA, not a declaration
/// (CLAUDE.md #3312). Scanning raw text would make this orphan look reachable — a
/// false PASS, the worst outcome for a hygiene guard. Live instance of the hazard:
/// `src/storage/write_engine/merge/mod.rs` mentions `cql_generator` in a doc comment.
#[test]
fn mod_mentions_in_comments_and_literals_are_data_not_declarations() {
    let tree = ScratchCrate::new("data-not-control");
    tree.write(
        "src/lib.rs",
        r####"
//! Doc comment mentioning `mod orphan;` — data.
// Line comment: mod orphan;
/* block comment: mod orphan;
   /* nested: mod orphan; */
   still a comment: mod orphan; */
/** outer doc block: mod orphan; */
pub mod wired;

pub const A: &str = "mod orphan;";
pub const B: &str = r"mod orphan;";
pub const C: &str = r#"raw with quote " and mod orphan;"#;
pub const D: &str = r##"deeper "# raw and mod orphan;"##;
pub const E: &str = "escaped quote \" then mod orphan;";
pub const F: &[u8] = b"byte string mod orphan;";
pub const G: char = '\'';
pub fn lifetimes<'a>(s: &'a str) -> &'a str { s }
"####,
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "a `mod orphan;` inside a comment or a literal must NOT make `orphan.rs` reachable"
    );
}

/// Every resolution rule, exercised against a tree the test controls end to end.
#[test]
fn synthetic_tree_exercises_every_resolution_rule() {
    let tree = ScratchCrate::new("all-rules");
    tree.write(
        "src/lib.rs",
        r#"
pub mod flat;                       // -> src/flat.rs
pub mod nested;                     // -> src/nested/mod.rs
pub(crate) mod vis_crate;           // visibility prefixes are accepted
pub(in crate::nested) mod vis_in;
#[cfg(test)]
mod cfg_gated;                      // cfg-gated is REACHABLE (AC-3)
#[cfg(feature = "nope")]
mod cfg_feature_gated;
#[cfg_attr(test, allow(dead_code))]
mod cfg_attr_gated;
#[path = "redirected_by_path.rs"]
mod aliased;                        // #[path] is relative to lib.rs's directory
pub mod inline_host;
"#,
    );
    tree.write("src/flat.rs", "pub mod child;\n"); // -> src/flat/child.rs
    tree.write("src/flat/child.rs", "pub fn f() {}\n");
    // A mod-rs file (`mod.rs`): a `#[path]` mod inside an inline block resolves under
    // `<dir-of-mod.rs>/<inline names>/`, with NO file-stem component.
    tree.write(
        "src/nested/mod.rs",
        r#"
pub mod leaf;
pub mod inner2 {
    #[path = "deep_alias.rs"]
    mod aliased;
}
"#,
    );
    tree.write("src/nested/inner2/deep_alias.rs", "pub fn f() {}\n");
    tree.write("src/nested/leaf.rs", "pub fn f() {}\n");
    tree.write("src/vis_crate.rs", "pub fn f() {}\n");
    tree.write("src/vis_in.rs", "pub fn f() {}\n");
    tree.write("src/cfg_gated.rs", "pub fn f() {}\n");
    tree.write("src/cfg_feature_gated.rs", "pub fn f() {}\n");
    tree.write("src/cfg_attr_gated.rs", "pub fn f() {}\n");
    tree.write("src/redirected_by_path.rs", "pub fn f() {}\n");
    // A non-mod-rs file (`inline_host.rs`): an inline block contributes no file of its
    // own, and both a plain `mod` and a `#[path]` mod inside it resolve under
    // `<dir>/<stem>/<inline names>/` — the stem component is what distinguishes this
    // from the mod-rs case above (Rust Reference, "Modules / The path attribute").
    tree.write(
        "src/inline_host.rs",
        r#"
pub mod inner {
    pub mod deep;
    #[path = "path_in_inline.rs"]
    mod aliased_inner;
}
pub fn f() {}
"#,
    );
    tree.write("src/inline_host/inner/deep.rs", "pub fn f() {}\n");
    tree.write("src/inline_host/inner/path_in_inline.rs", "pub fn f() {}\n");

    let report = tree.analyze();
    assert!(
        report.orphans.is_empty(),
        "every file in this tree is wired in, yet the walker reported orphans: {:?}",
        report.orphans
    );
    assert_eq!(
        report.enumerated.len(),
        report.reachable.len(),
        "census and reachable set must coincide for a fully-wired tree"
    );
}

// ---------------------------------------------------------------------------
// Fail-closed: unmodeled constructs are errors, never skips
// ---------------------------------------------------------------------------

#[test]
fn include_macro_fails_closed() {
    let tree = ScratchCrate::new("include-macro");
    tree.write(
        "src/lib.rs",
        "pub mod wired;\ninclude!(\"generated.rs\");\n",
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/generated.rs", "pub fn f() {}\n");
    let cause = tree.expect_failure();
    assert!(
        cause.contains("include!"),
        "an `include!` must fail closed naming itself; got: {cause}"
    );
}

#[test]
fn unresolvable_mod_declaration_fails_closed() {
    let tree = ScratchCrate::new("unresolvable-mod");
    tree.write("src/lib.rs", "pub mod ghost;\n");
    let cause = tree.expect_failure();
    assert!(
        cause.contains("src/ghost.rs") && cause.contains("src/ghost/mod.rs"),
        "an unresolvable `mod` must name both candidates; got: {cause}"
    );
}

#[test]
fn path_attr_on_inline_mod_fails_closed() {
    let tree = ScratchCrate::new("path-on-inline");
    tree.write(
        "src/lib.rs",
        "#[path = \"elsewhere\"]\npub mod inline_block { pub fn f() {} }\n",
    );
    let cause = tree.expect_failure();
    assert!(
        cause.contains("INLINE"),
        "`#[path]` on an inline mod block must fail closed; got: {cause}"
    );
}

#[test]
fn cfg_attr_path_redirect_fails_closed() {
    let tree = ScratchCrate::new("cfg-attr-path");
    tree.write(
        "src/lib.rs",
        "#[cfg_attr(test, path = \"other.rs\")]\nmod thing;\n",
    );
    tree.write("src/thing.rs", "pub fn f() {}\n");
    tree.write("src/other.rs", "pub fn f() {}\n");
    let cause = tree.expect_failure();
    assert!(
        cause.contains("cfg_attr"),
        "a conditional `#[cfg_attr(…, path = …)]` redirect must fail closed; got: {cause}"
    );
}

#[test]
fn missing_path_target_fails_closed() {
    let tree = ScratchCrate::new("missing-path-target");
    tree.write("src/lib.rs", "#[path = \"nope.rs\"]\nmod aliased;\n");
    let cause = tree.expect_failure();
    assert!(
        cause.contains("nope.rs") && cause.contains("does not exist"),
        "a `#[path]` pointing at a missing file must fail closed; got: {cause}"
    );
}

/// Rust permits an ABSOLUTE `#[path]` value, and rustc resolves it as an absolute path —
/// against the filesystem root, not against the declaring file's directory. Prepending the
/// declaring module's directory to it and normalizing the result as if it were relative
/// re-points the declaration at an entirely different, IN-CRATE file: `#[path =
/// "/abs/target.rs"]` in `src/lib.rs` becomes `src/abs/target.rs`, so a same-named in-crate
/// orphan is marked reachable while rustc compiles something outside the crate. That is a
/// FALSE PASS — an orphan reported as wired — which is why the walker refuses instead.
///
/// Both spellings are covered: POSIX (`/…`) and the Windows forms (`\…`, `C:\…`, `C:/…`),
/// since `normalize_key` unifies `\` to `/` and would otherwise fold a drive-letter or UNC
/// value into an in-crate key on any platform.
#[test]
fn absolute_path_attribute_value_fails_closed() {
    // (source spelling of the literal, the value it decodes to). A Windows path must
    // double its backslashes in Rust source — `"\abs"` is an invalid escape rustc rejects
    // too, and the sanitizer refuses it one layer earlier.
    for (spelling, value) in [
        ("/abs/target.rs", "/abs/target.rs"),
        ("\\\\abs\\\\target.rs", "\\abs\\target.rs"),
        ("C:\\\\abs\\\\target.rs", "C:\\abs\\target.rs"),
        ("C:/abs/target.rs", "C:/abs/target.rs"),
    ] {
        let tree = ScratchCrate::new("absolute-path-attr");
        tree.write(
            "src/lib.rs",
            &format!("#[path = \"{spelling}\"]\nmod aliased;\npub mod wired;\n"),
        );
        tree.write("src/wired.rs", "pub fn f() {}\n");
        // The file a "prepend the module directory and normalize" reading would land on.
        // It is a real orphan: rustc, reading the value as absolute, never compiles it.
        tree.write("src/abs/target.rs", "pub fn never_compiled() {}\n");
        tree.write("src/C/abs/target.rs", "pub fn never_compiled() {}\n");

        let cause = tree.expect_failure();
        assert!(
            cause.contains(value) && cause.contains("src/lib.rs:2") && cause.contains("absolute"),
            "the refusal must name the file, the line and the absolute value verbatim; \
             got: {cause}"
        );
    }
}

/// A `#[path]` value is read out of the sanitizer's literal side table, so the string
/// scanner must reproduce the value BYTE-EXACTLY. Decoding unescaped bytes one at a time
/// (`byte as char`) turns the two UTF-8 bytes of `ó` into two Latin-1 characters, and the
/// mojibake resolves to nothing. That fails *closed* (an unresolvable path errors), so it
/// is a false FAIL rather than a false PASS — but CLAUDE.md records a six-defect
/// path-normalisation family born of exactly this byte-vs-char confusion.
#[test]
fn non_ascii_path_attribute_values_survive_the_sanitizer() {
    // (a) the character written literally in the source.
    let tree = ScratchCrate::new("non-ascii-path-literal");
    tree.write("src/lib.rs", "#[path = \"módulo_ñ.rs\"]\nmod aliased;\n");
    tree.write("src/módulo_ñ.rs", "pub fn f() {}\n");
    let report = tree.analyze();
    assert!(
        report.reachable.contains("src/módulo_ñ.rs"),
        "a non-ASCII `#[path]` value must resolve to its file; reachable={:?}",
        report.reachable
    );
    assert!(
        report.orphans.is_empty(),
        "no file in this tree is unreachable, yet orphans={:?}",
        report.orphans
    );

    // (b) the same character written as a `\u{…}` escape — escape handling must keep
    //     working alongside the raw-byte accumulation.
    let tree = ScratchCrate::new("non-ascii-path-escape");
    tree.write(
        "src/lib.rs",
        "#[path = \"m\\u{f3}dulo_\\u{f1}.rs\"]\nmod aliased;\n",
    );
    tree.write("src/módulo_ñ.rs", "pub fn f() {}\n");
    let report = tree.analyze();
    assert!(
        report.reachable.contains("src/módulo_ñ.rs"),
        "a `\\u{{…}}`-escaped `#[path]` value must resolve to the same file; reachable={:?}",
        report.reachable
    );
}

/// A symlink under `src/` is a hole in the census. `DirEntry::file_type().is_dir()` is
/// FALSE for a symlink that points at a directory, so the whole subtree behind it would
/// be walked past in silence and every orphan inside it would pass undetected — a silent
/// census omission is a vacuous pass. Following it instead would need canonical-path
/// boundary and cycle checks to buy nothing (there are no symlinks under
/// `cqlite-core/src`), so the walker refuses.
#[test]
#[cfg(unix)]
fn symlink_under_src_fails_closed() {
    use std::os::unix::fs::symlink;

    // (a) a symlinked DIRECTORY: `is_dir()` is false, so the subtree is skipped.
    let tree = ScratchCrate::new("symlink-dir");
    tree.write("src/lib.rs", "pub mod wired;\n");
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("outside/hidden_orphan.rs", "pub fn never_compiled() {}\n");
    symlink(
        tree.dir().join("outside"),
        tree.dir().join("src/linked_dir"),
    )
    .unwrap_or_else(|e| panic!("cannot create directory symlink: {e}"));
    let cause = tree.expect_failure();
    assert!(
        cause.contains("symlink") && cause.contains("linked_dir"),
        "a symlinked DIRECTORY under `src/` must fail closed naming the path; got: {cause}"
    );

    // (b) a symlinked `.rs` FILE, which could name a file outside the crate entirely.
    let tree = ScratchCrate::new("symlink-file");
    tree.write("src/lib.rs", "pub mod wired;\n");
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("outside/target.rs", "pub fn f() {}\n");
    symlink(
        tree.dir().join("outside/target.rs"),
        tree.dir().join("src/linked.rs"),
    )
    .unwrap_or_else(|e| panic!("cannot create file symlink: {e}"));
    let cause = tree.expect_failure();
    assert!(
        cause.contains("symlink") && cause.contains("linked.rs"),
        "a symlinked `.rs` FILE under `src/` must fail closed naming the path; got: {cause}"
    );
}

/// A `mod` token inside a macro's token tree is neither control nor data the walker can
/// read: rustc decides what the macro expands to, and this walker does not expand macros.
/// Counting it as a real declaration makes an unreachable file look reachable — the exact
/// false-PASS class this guard exists to prevent (same shape as the commented-out
/// `mod` that hid `parser/collection_udt_tests.rs`). So it fails CLOSED, naming the macro.
#[test]
fn mod_declaration_inside_a_macro_fails_closed() {
    for (label, lib_rs, needle) in [
        (
            "macro-rules-body",
            "pub mod wired;\nmacro_rules! make_it { () => { mod orphan; }; }\n",
            "make_it",
        ),
        (
            "macro-invocation-paren",
            "pub mod wired;\npub const S: &str = stringify!(mod orphan;);\n",
            "stringify!",
        ),
        (
            "macro-invocation-brace",
            "pub mod wired;\ncfg_if::cfg_if! { mod orphan; }\n",
            "cfg_if!",
        ),
        (
            "macro-invocation-bracket",
            "pub mod wired;\npub const S: &str = stringify![mod orphan;];\n",
            "stringify!",
        ),
        (
            "macro-rules-metavariable",
            "pub mod wired;\nmacro_rules! decl { ($n:ident) => { mod $n; }; }\n",
            "decl",
        ),
    ] {
        let tree = ScratchCrate::new(label);
        tree.write("src/lib.rs", lib_rs);
        tree.write("src/wired.rs", "pub fn f() {}\n");
        tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");
        let cause = tree.expect_failure();
        assert!(
            cause.contains("macro") && cause.contains(needle),
            "case `{label}`: a `mod` inside a macro token tree must fail closed naming the \
             macro (`{needle}`); got: {cause}"
        );
    }
}

/// The other half, and it is the half that keeps the guard alive: an over-broad macro
/// rule that reds on EVERY macro is the rule someone deletes. Ordinary macros — a
/// `macro_rules!` definition with no `mod` in it, `format!`, `vec![]`, and a `!=` that
/// merely looks like an invocation — must leave the walk untouched, and the orphan
/// underneath must still be found.
#[test]
fn an_ordinary_macro_does_not_trip_the_mod_in_macro_guard() {
    let tree = ScratchCrate::new("macro-without-mod");
    tree.write(
        "src/lib.rs",
        r####"
macro_rules! shout {
    ($x:expr) => {{
        let modified = format!("{}!", $x);
        modified
    }};
}
macro_rules! braces_and_brackets {
    () => {
        vec![1usize, 2, 3]
    };
}
pub mod wired;
pub fn f() -> String { shout!("hi") }
pub fn g() -> Vec<usize> { braces_and_brackets!() }
pub fn h(a: usize, b: usize) -> bool { a != b }
pub fn i() { assert!(matches!(Some(1u8), Some(_))); }
"####,
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "ordinary macros must neither fail the walk nor hide the orphan; enumerated={:?} \
         reachable={:?}",
        report.enumerated,
        report.reachable
    );
    assert!(
        report.reachable.contains("src/wired.rs"),
        "a macro-bearing root must still resolve its real `mod` declarations"
    );
}

#[test]
fn empty_source_tree_fails_closed_rather_than_passing() {
    let tree = ScratchCrate::new("no-src-dir");
    // Deliberately no `src/` at all.
    let cause = tree.expect_failure();
    assert!(
        cause.contains("source directory"),
        "a missing source directory must fail closed (a zero census is the vacuous pass); \
         got: {cause}"
    );
}

// ---------------------------------------------------------------------------
// The literal/identifier prefix family (issue #1714, roborev rounds 1-2)
// ---------------------------------------------------------------------------

/// Every Rust literal form, one case each, with `mod orphan;` **inside** the literal.
///
/// This is the family test, not a set of special cases. Five review findings on this
/// walker were all the same shape: prefix recognition lived in three parsers, each
/// knowing a different subset of Rust's prefixes, so an unrecognized prefix fell through
/// to ordinary scanning and the `mod orphan;` inside it was counted as a real
/// declaration — a silent FALSE PASS. `cr#"…"#` (raw C-string) was the last one found;
/// its exact adversarial case is pinned below, because the embedded `"` is the point: a
/// scanner that terminates the literal at the first quote lands squarely in `mod orphan;`.
#[test]
fn every_literal_prefix_form_is_data_not_a_declaration() {
    for (label, literal) in [
        ("string", r####""mod orphan;""####),
        (
            "string-escaped-quote",
            r####""escaped \" then mod orphan;""####,
        ),
        ("raw", r####"r"mod orphan;""####),
        ("raw-hash", r####"r#"quote " and mod orphan;"#"####),
        ("raw-hash2", r####"r##"deeper "# and mod orphan;"##"####),
        ("byte-string", r####"b"mod orphan;""####),
        ("byte-raw", r####"br"mod orphan;""####),
        ("byte-raw-hash", r####"br#"quote " and mod orphan;"#"####),
        ("cstring", r####"c"mod orphan;""####),
        ("cstring-raw", r####"cr"mod orphan;""####),
        // The roborev round-2 finding, verbatim: embedded quotes inside a hashed raw
        // C-string. A naive scanner terminates at the first `"` and reads the rest as code.
        (
            "cstring-raw-hash",
            r####"cr#"left " mod orphan; " right"#"####,
        ),
        (
            "cstring-raw-hash2",
            r####"cr##"deeper "# and mod orphan;"##"####,
        ),
        // A char literal cannot hold `mod orphan;`, so the byte-char cases pin the other
        // way a mis-lex hurts: a quote inside `b'…'`/`'…'` that is mistaken for the start
        // of a string literal shifts every later quote pairing, which drops the following
        // string's `mod orphan;` into code position.
        ("byte-char-quote", r####"(b'"', "mod orphan;")"####),
        ("char-quote", r####"('"', "mod orphan;")"####),
        ("char-escaped-quote", r####"('\'', "mod orphan;")"####),
    ] {
        let tree = ScratchCrate::new(&format!("literal-{label}"));
        tree.write(
            "src/lib.rs",
            &format!("pub mod wired;\npub fn f() {{ let _ = {literal}; }}\n"),
        );
        tree.write("src/wired.rs", "pub fn f() {}\n");
        tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

        let report = tree.analyze();
        assert_eq!(
            report.orphans.iter().cloned().collect::<Vec<_>>(),
            vec!["src/orphan.rs".to_string()],
            "case `{label}` ({literal}): a `mod orphan;` inside a literal is DATA — it must \
             not make `orphan.rs` look reachable; reachable={:?}",
            report.reachable
        );
        assert!(
            report.reachable.contains("src/wired.rs"),
            "case `{label}` ({literal}): the literal must not swallow the real `mod wired;` \
             that follows it; reachable={:?}",
            report.reachable
        );
    }
}

/// **The test that keeps the family closed.** An identifier-ish token glued to `"`, `'`
/// or `#` that the lexer's `PREFIXES` table does not know must be an `Err` naming the
/// file, line and token — never a fall-through to ordinary scanning.
///
/// Rust reserves every such sequence (edition-2021 reserved prefixes) precisely so new
/// literal forms can be added; `c"…"` and `cr#"…"#` themselves arrived that way in Rust
/// 1.77. Without this branch, the *next* prefix silently disables the guard, which is how
/// this walker got two findings of the same shape in consecutive review rounds. With it,
/// the walk stops and a human adds one table row.
#[test]
fn unrecognized_literal_prefix_fails_closed() {
    for (label, snippet, token) in [
        // A hypothetical future string prefix (`f"…"`, `k#"…"#`) — the real threat model.
        (
            "future-string-prefix",
            "pub fn f() { let _ = f\"mod orphan;\"; }\npub mod wired;\n",
            "f\"",
        ),
        (
            "future-hash-prefix",
            "pub fn f() { let _ = k#\"mod orphan;\"#; }\npub mod wired;\n",
            "k#",
        ),
        // `b#`/`c#` are NOT raw-string prefixes (`br#`/`cr#` are) — a one-character slip
        // that must refuse rather than scan on.
        (
            "byte-hash-is-not-a-raw-prefix",
            "pub fn f() { let _ = b#\"mod orphan;\"#; }\npub mod wired;\n",
            "b#",
        ),
        (
            "cstring-hash-is-not-a-raw-prefix",
            "pub fn f() { let _ = c#\"mod orphan;\"#; }\npub mod wired;\n",
            "c#",
        ),
        // `r##foo` is neither a raw string (no `\"`) nor a raw identifier (two hashes).
        (
            "double-hash-raw-identifier",
            "pub fn f() { let _ = r##orphan; }\npub mod wired;\n",
            "r#",
        ),
        // A future char-like prefix.
        (
            "future-char-prefix",
            "pub fn f() { let _ = q'x'; }\npub mod wired;\n",
            "q'",
        ),
    ] {
        let tree = ScratchCrate::new(&format!("prefix-{label}"));
        tree.write("src/lib.rs", snippet);
        tree.write("src/wired.rs", "pub fn f() {}\n");
        tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

        let cause = tree.expect_failure();
        assert!(
            cause.contains("unrecognized literal/identifier prefix"),
            "case `{label}`: an unrecognized prefix must fail CLOSED as a prefix refusal \
             (a fall-through to ordinary scanning is the silent false PASS); got: {cause}"
        );
        assert!(
            cause.contains(token),
            "case `{label}`: the refusal must NAME the offending token (`{token}`) so a \
             human can add the table row; got: {cause}"
        );
        assert!(
            cause.contains("src/lib.rs") && cause.contains("line 1"),
            "case `{label}`: the refusal must name the file and line; got: {cause}"
        );
        assert!(
            cause.contains("#1714"),
            "case `{label}`: the refusal must point at the issue that owns the table; \
             got: {cause}"
        );
    }
}

/// **The identifier-family half of the same boundary.** Rust identifiers are
/// `XID_Start XID_Continue*`, so `macro_rules! café { () => { mod orphan; } }` is a real
/// macro definition — and this walker's lexer is deliberately ASCII-only. Scanning past
/// the `é` lexes the macro name as `caf`, leaves the rest to the ordinary scan, fails to
/// recognize the macro context, and counts the `mod orphan;` in the macro body as a real
/// declaration: the walker's critical FALSE PASS, arrived at with no unrecognized prefix
/// anywhere in sight.
///
/// The fix is the same one the prefix family got — refuse, never skip — because the
/// alternative is shipping Unicode XID tables, i.e. a second implementation of rustc's
/// lexer, which is what produced this walker's review history. Both positions are pinned:
/// a non-ASCII character where an identifier could START, and one where the identifier
/// just scanned could CONTINUE.
#[test]
fn non_ascii_identifiers_fail_closed_rather_than_hiding_a_macro_context() {
    for (label, snippet, shown, position) in [
        // THE case: without the refusal this walk reports zero orphans.
        (
            "unicode-macro-definition",
            "macro_rules! café { () => { mod orphan; } }\npub mod wired;\n",
            "é",
            "CONTINUE",
        ),
        (
            "unicode-macro-invocation",
            "pub fn f() { café!(mod orphan;); }\npub mod wired;\n",
            "é",
            "CONTINUE",
        ),
        // An ordinary Unicode identifier: legal Rust this walker refuses to read rather
        // than half-read. A loud refusal is the doctrine; a silent half-parse is not.
        (
            "unicode-identifier-continue",
            "pub fn f() { let café = 1; let _ = café; }\npub mod wired;\n",
            "é",
            "CONTINUE",
        ),
        // XID_Start itself non-ASCII, so the very first byte of the identifier is the
        // refusal point — the START position, reached from the sanitizer's scan loop.
        (
            "unicode-identifier-start",
            "pub fn f() { let _ = Übersicht; }\npub mod wired;\n",
            "Ü",
            "START",
        ),
        (
            "non-latin-identifier-start",
            "pub fn 模块() {}\npub mod wired;\n",
            "模",
            "START",
        ),
    ] {
        let tree = ScratchCrate::new(&format!("non-ascii-{label}"));
        tree.write("src/lib.rs", snippet);
        tree.write("src/wired.rs", "pub fn f() {}\n");
        tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

        let cause = tree.expect_failure();
        assert!(
            cause.contains("NON-ASCII character"),
            "case `{label}`: a non-ASCII identifier byte must fail CLOSED as a non-ASCII \
             refusal — scanning past it is the silent false PASS; got: {cause}"
        );
        assert!(
            cause.contains(shown),
            "case `{label}`: the refusal must NAME the offending character (`{shown}`); \
             got: {cause}"
        );
        assert!(
            cause.contains(position),
            "case `{label}`: the refusal must say which identifier position it refused \
             ({position}); got: {cause}"
        );
        assert!(
            cause.contains("ASCII-only"),
            "case `{label}`: the refusal must state that the ASCII-only identifier rules \
              are deliberate, so the reader does not \"fix\" them by widening a byte range; \
             got: {cause}"
        );
        assert!(
            cause.contains("src/lib.rs") && cause.contains("line 1"),
            "case `{label}`: the refusal must name the file and line; got: {cause}"
        );
        assert!(
            cause.contains("#1714"),
            "case `{label}`: the refusal must point at the issue that owns the lexer; \
             got: {cause}"
        );
    }
}

/// The refusal must not fire on a non-ASCII character that is DATA. Comments and string
/// literals are where non-ASCII actually lives in this repository (456 of the ~540 files
/// measured below contain some), and the sanitizer blanks them before the identifier scan
/// can reach a byte inside one — so every one of those files must walk cleanly. A refusal
/// branch that reds on ordinary prose is the branch someone deletes.
#[test]
fn non_ascii_in_comments_and_literals_is_data_not_an_identifier() {
    let tree = ScratchCrate::new("non-ascii-data");
    tree.write(
        "src/lib.rs",
        "// café — mod orphan;\n\
         /// Übersicht: mod orphan;\n\
         /* 模块 mod orphan; */\n\
         pub fn f() -> &'static str { \"café mod orphan;\" }\n\
         pub fn g() -> &'static str { r#\"Übersicht mod orphan;\"# }\n\
         pub fn h() -> char { '模' }\n\
         pub mod wired;\n",
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "non-ASCII inside comments and literals is DATA: it must neither red the walk nor \
         hide the orphan; reachable={:?}",
        report.reachable
    );
}

/// A raw identifier is legal in a `mod` declaration, and the `r#` is **not** part of the
/// name: `mod r#type;` declares a module whose file is `type.rs`. Failing closed here
/// would be a real false FAIL the moment someone writes `mod r#try;`, so the resolution
/// is pinned in both flavors (`name.rs` and `name/mod.rs`).
#[test]
fn raw_identifier_module_declarations_resolve() {
    let tree = ScratchCrate::new("raw-ident-mod");
    tree.write(
        "src/lib.rs",
        "pub mod r#type;\npub mod r#try;\npub mod wired;\n",
    );
    tree.write("src/type.rs", "pub fn f() {}\n");
    tree.write("src/try/mod.rs", "pub fn f() {}\n");
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    for expected in ["src/type.rs", "src/try/mod.rs", "src/wired.rs"] {
        assert!(
            report.reachable.contains(expected),
            "`mod r#…;` must resolve with the `r#` stripped: `{expected}` missing from {:?}",
            report.reachable
        );
    }
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "raw-identifier modules must not disturb orphan detection"
    );
}

/// A macro may be *named* with a raw identifier, in both definition and invocation
/// position. If the walker does not consume `r#name` atomically it never recognizes the
/// macro at all, parses its token tree as ordinary Rust, and counts the `mod orphan;`
/// inside it as a real declaration — the roborev round-2 finding.
#[test]
fn raw_identifier_named_macros_reach_the_mod_in_macro_guard() {
    for (label, lib_rs, needle) in [
        (
            "raw-macro-rules-definition",
            "pub mod wired;\nmacro_rules! r#make { () => { mod orphan; }; }\n",
            "r#make",
        ),
        (
            "raw-macro-invocation-paren",
            "pub mod wired;\npub const S: &str = r#make!(mod orphan;);\n",
            "r#make!",
        ),
        (
            "raw-macro-invocation-brace",
            "pub mod wired;\nouter::r#bar! { mod orphan; }\n",
            "r#bar!",
        ),
        (
            "raw-macro-invocation-bracket",
            "pub mod wired;\npub const S: &str = r#make![mod orphan;];\n",
            "r#make!",
        ),
        (
            "raw-macro-rules-metavariable",
            "pub mod wired;\nmacro_rules! r#decl { ($n:ident) => { mod $n; }; }\n",
            "r#decl",
        ),
    ] {
        let tree = ScratchCrate::new(&format!("raw-macro-{label}"));
        tree.write("src/lib.rs", lib_rs);
        tree.write("src/wired.rs", "pub fn f() {}\n");
        tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

        let cause = tree.expect_failure();
        assert!(
            cause.contains("macro") && cause.contains(needle),
            "case `{label}`: a `mod` inside a raw-identifier-named macro's token tree must \
             fail closed naming the macro (`{needle}`); got: {cause}"
        );
    }
}

/// The other direction for raw identifiers: `r#mod` is an IDENTIFIER, never the keyword,
/// and an ordinary raw-identifier-named macro with no `mod` in it must leave the walk
/// alone. An over-broad guard is the guard someone deletes.
#[test]
fn raw_identifiers_that_are_not_declarations_do_not_trip_the_guard() {
    let tree = ScratchCrate::new("raw-ident-benign");
    tree.write(
        "src/lib.rs",
        r####"
macro_rules! r#fn {
    ($x:expr) => {{ let r#mod = $x; r#mod }};
}
pub mod wired;
pub struct S { pub r#type: u8, pub r#mod: u8 }
pub fn f() -> usize { r#fn!(1usize) }
pub fn g<'a>(r#match: &'a str) -> &'a str { r#match }
"####,
    );
    tree.write("src/wired.rs", "pub fn f() {}\n");
    tree.write("src/orphan.rs", "pub fn never_compiled() {}\n");

    let report = tree.analyze();
    assert_eq!(
        report.orphans.iter().cloned().collect::<Vec<_>>(),
        vec!["src/orphan.rs".to_string()],
        "`r#mod` is an identifier, not a declaration, and a benign raw-named macro must \
         neither fail the walk nor hide the orphan; reachable={:?}",
        report.reachable
    );
    assert!(
        report.reachable.contains("src/wired.rs"),
        "a raw-identifier-bearing root must still resolve its real `mod` declarations"
    );
}

// ---------------------------------------------------------------------------
// Real-corpus anti-vacuity measurement
// ---------------------------------------------------------------------------

/// Every real `.rs` file in the two crates this walker serves — `cqlite-core/src` (its
/// subject) and `cqlite-cli/src` (#1502's subject, which will drive the same walker).
///
/// Nothing here is allowed to swallow a filesystem error. `read_dir`'s iterator yields a
/// `Result` PER ENTRY, and the idiomatic-looking `entries.flatten()` DISCARDS the `Err`
/// arm: a directory whose entries fail to stat would silently shrink the census while the
/// count floor below still passed — a vacuous pass in the very measurement that exists to
/// prove the refusals are not over-broad. Same for `Path::is_dir()`, which answers `false`
/// on an IO error, so the entry's `file_type()` is asked instead and its error propagated.
fn real_rust_corpus() -> Vec<PathBuf> {
    fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
        let entries =
            fs::read_dir(dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|e| panic!("cannot read an entry of {}: {e}", dir.display()));
            let path = entry.path();
            let file_type = entry
                .file_type()
                .unwrap_or_else(|e| panic!("cannot stat {}: {e}", path.display()));
            if file_type.is_dir() {
                collect(&path, out);
            } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
                out.push(path);
            }
        }
    }

    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent workspace directory")
        .to_path_buf();
    let mut files = Vec::new();
    for crate_src in ["cqlite-core/src", "cqlite-cli/src"] {
        let dir = workspace.join(crate_src);
        assert!(
            dir.is_dir(),
            "`{}` must exist — a probe with no subject measures nothing (FAIL-CLOSED)",
            dir.display()
        );
        collect(&dir, &mut files);
    }
    // A collapsed census is the vacuous pass; there were 481 + 60 files when this landed.
    assert!(
        files.len() >= 300,
        "only {} files probed — the census collapsed, so this test proved nothing",
        files.len()
    );
    files
}

/// Read a corpus file, refusing to guess at an unreadable one.
fn read_corpus_file(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// The anti-vacuity complement to [`unrecognized_literal_prefix_fails_closed`]: a
/// refusal branch that reds on ORDINARY Rust is the branch someone deletes, and deleting
/// it re-opens the whole false-PASS family. So the lexer is measured against every real
/// `.rs` file in the two crates it serves and must refuse none of them.
///
/// Measured when this landed: 1400 `.rs` files across the whole workspace produced
/// exactly one refusal, and that one is correct (a rust-script file whose shebang makes
/// `#!` an invalid Rust escape, outside any crate's `src`).
#[test]
fn the_prefix_refusal_never_reds_ordinary_rust() {
    let files = real_rust_corpus();
    let mut refusals = Vec::new();
    for file in &files {
        if let Err(cause) = mod_reachability::sanitize(&read_corpus_file(file)) {
            refusals.push(format!("{}: {cause}", file.display()));
        }
    }
    assert!(
        refusals.is_empty(),
        "the lexer refused {} of {} real Rust file(s); an over-broad refusal is the branch \
         someone deletes:\n{}",
        refusals.len(),
        files.len(),
        refusals.join("\n")
    );
}

/// The same measurement for the NON-ASCII identifier refusal, which needs its own case
/// because non-ASCII is *common* in this repository's real code — em dashes and accented
/// words in comments, doc comments and string literals — while being *rare* in the one
/// position the refusal covers. If the refusal fired on data, this guard would red on
/// hundreds of ordinary files and be deleted within a week.
///
/// Three properties, so a pass cannot be vacuous:
/// 1. a large majority of the corpus genuinely CONTAINS non-ASCII bytes (asserted, not
///    assumed — otherwise the probe would be measuring ASCII files and proving nothing);
/// 2. no such file is refused;
/// 3. each one's SANITIZED text is pure ASCII and the same byte length as the input, i.e.
///    every non-ASCII byte went through the blanking path rather than past the scan. That
///    is the invariant `parse_mod_decls`, `find_mod_token` and `ident_token` rely on when
///    they apply ASCII-only identifier rules to sanitized text.
#[test]
fn the_non_ascii_identifier_refusal_never_reds_ordinary_rust() {
    let files = real_rust_corpus();
    let mut with_non_ascii = 0usize;
    let mut refusals = Vec::new();
    let mut leaked = Vec::new();

    for file in &files {
        let text = read_corpus_file(file);
        if text.is_ascii() {
            continue;
        }
        with_non_ascii += 1;
        match mod_reachability::sanitize(&text) {
            Err(cause) => refusals.push(format!("{}: {cause}", file.display())),
            Ok(sanitized) => {
                if !sanitized.text.is_ascii() || sanitized.text.len() != text.len() {
                    leaked.push(file.display().to_string());
                }
            }
        }
    }

    // Property 1: the hazard is actually present in the corpus.
    assert!(
        with_non_ascii >= 100,
        "only {with_non_ascii} of {} corpus files contain non-ASCII bytes — the probe is \
         measuring ASCII files and proves nothing about the non-ASCII refusal",
        files.len()
    );
    // Property 2: it never fires on them.
    assert!(
        refusals.is_empty(),
        "the non-ASCII refusal fired on {} of {with_non_ascii} real Rust file(s) that carry \
         non-ASCII text; a refusal that reds ordinary prose is the branch someone deletes, \
         so this is a DESIGN problem, not a file to fix:\n{}",
        refusals.len(),
        refusals.join("\n")
    );
    // Property 3: every one of those bytes was blanked, offsets preserved.
    assert!(
        leaked.is_empty(),
        "{} file(s) sanitized to text that is not pure ASCII (or changed byte length), so a \
         non-ASCII byte reached code position unrefused — the downstream parsers' \
         ASCII-only identifier rules would split an identifier around it:\n{}",
        leaked.len(),
        leaked.join("\n")
    );

    // The measurement itself, so a reader of the test output sees what was covered rather
    // than trusting the assertions' silence.
    eprintln!(
        "non-ASCII refusal census: {} real .rs files scanned, {with_non_ascii} carried \
         non-ASCII bytes, 0 refusals",
        files.len()
    );
}

// ---------------------------------------------------------------------------
// Stripper unit tests (control vs data, at the sanitizer boundary)
// ---------------------------------------------------------------------------

#[test]
fn stripper_blanks_every_comment_form() {
    for src in [
        "// mod x;\nlet a = 1;\n",
        "/// mod x;\nlet a = 1;\n",
        "//! mod x;\nlet a = 1;\n",
        "/* mod x; */let a = 1;\n",
        "/* outer /* nested mod x; */ still outer mod x; */let a = 1;\n",
        "/** doc block mod x; */let a = 1;\n",
    ] {
        let out = stripped(src);
        assert!(
            !out.contains("mod x"),
            "comment content survived the stripper: input {src:?} -> {out:?}"
        );
        assert!(
            out.contains("let a = 1;"),
            "the stripper ate real code: input {src:?} -> {out:?}"
        );
        assert_eq!(
            out.len(),
            src.len(),
            "the stripper must preserve byte offsets so diagnostics stay accurate"
        );
        assert_eq!(
            out.matches('\n').count(),
            src.matches('\n').count(),
            "the stripper must preserve line breaks so reported line numbers are correct"
        );
    }
}

#[test]
fn stripper_blanks_every_literal_form() {
    for src in [
        "let s = \"mod x;\";\n",
        "let s = \"escaped \\\" then mod x;\";\n",
        "let s = r\"mod x;\";\n",
        "let s = r#\"quote \" and mod x;\"#;\n",
        "let s = r##\"deeper \"# and mod x;\"##;\n",
        "let s = b\"mod x;\";\n",
        "let s = br\"mod x;\";\n",
        "let s = br#\"mod x;\"#;\n",
        "let s = c\"mod x;\";\n",
        "let s = cr\"mod x;\";\n",
        "let s = cr#\"quote \" and mod x;\"#;\n",
        "let s = cr##\"deeper \"# and mod x;\"##;\n",
        "let s = (b'\"', \"mod x;\");\n",
    ] {
        let out = stripped(src);
        assert!(
            !out.contains("mod x"),
            "literal content survived the stripper: input {src:?} -> {out:?}"
        );
        assert_eq!(out.len(), src.len(), "byte offsets must be preserved");
    }
}

#[test]
fn stripper_keeps_lifetimes_and_char_literals_apart() {
    let src = "fn f<'a>(c: char) -> &'a str { let q = '\\''; let z = 'x'; \"s\" }\n";
    let out = stripped(src);
    assert!(
        out.contains("fn f<'a>") && out.contains("&'a str"),
        "lifetimes must survive (mistaking one for a char literal would swallow real code): {out:?}"
    );
    assert_eq!(out.len(), src.len(), "byte offsets must be preserved");
    assert!(
        !out.contains('"'),
        "string literals must be blanked: {out:?}"
    );
}

#[test]
fn stripper_fails_closed_on_unterminated_input() {
    for (src, what) in [
        ("/* never closed\n", "block comment"),
        ("let s = \"never closed\n", "string literal"),
        ("let s = r#\"never closed\n", "raw string literal"),
    ] {
        assert!(
            strip_comments_and_strings(src).is_err(),
            "unterminated {what} must fail closed rather than yield a guessed parse"
        );
    }
}
