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

#[path = "support/mod_reachability.rs"]
mod mod_reachability;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

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
            report.enumerated.contains(&key.to_string()),
            "resolution-rule case `{key}` ({rule}) is not in the census — the case moved or \
             was renamed. Repoint it at a live file; do not delete the case, the rule needs a \
             witness."
        );
        assert!(
            report.reachable.contains(&key.to_string()),
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
// Stripper unit tests (control vs data, at the sanitizer boundary)
// ---------------------------------------------------------------------------

fn stripped(src: &str) -> String {
    strip_comments_and_strings(src).unwrap_or_else(|e| panic!("sanitize failed: {e}"))
}

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
        "let s = br#\"mod x;\"#;\n",
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

// ---------------------------------------------------------------------------
// Scratch crate helper
// ---------------------------------------------------------------------------

static SCRATCH_SEQ: AtomicUsize = AtomicUsize::new(0);

/// A throwaway crate-shaped directory tree under the OS temp dir.
struct ScratchCrate {
    dir: PathBuf,
}

impl ScratchCrate {
    fn new(label: &str) -> Self {
        let seq = SCRATCH_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "cqlite-1714-mod-reach-{}-{seq}-{label}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("cannot create {}: {e}", dir.display()));
        Self { dir }
    }

    fn write(&self, rel: &str, contents: &str) {
        let path = self.dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("cannot create {}: {e}", parent.display()));
        }
        fs::write(&path, contents)
            .unwrap_or_else(|e| panic!("cannot write {}: {e}", path.display()));
    }

    fn spec(&self) -> ModuleGraphSpec {
        ModuleGraphSpec {
            crate_dir: self.dir.clone(),
            root_file_rel: "src/lib.rs".to_string(),
            src_dir_rel: SRC_DIR.to_string(),
        }
    }

    fn analyze(&self) -> mod_reachability::Report {
        analyze(&self.spec()).unwrap_or_else(|cause| {
            panic!(
                "walk of scratch crate {} failed: {cause}",
                self.dir.display()
            )
        })
    }

    /// Assert the walk fails closed and return the cause.
    fn expect_failure(&self) -> String {
        match analyze(&self.spec()) {
            Ok(report) => panic!(
                "expected a FAIL-CLOSED refusal, got a report (orphans={:?}) — a skip-and-continue \
                 here IS the vacuous pass",
                report.orphans
            ),
            Err(cause) => cause,
        }
    }
}

impl Drop for ScratchCrate {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
