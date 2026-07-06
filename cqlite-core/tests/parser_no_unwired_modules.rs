//! Unwired-symbol guard (issue #1637, parser-audit finding J3).
//!
//! Asserts that every module declared under `cqlite-core/src/parser/` that is
//! neither `#[cfg(test)]`-gated nor `#[cfg(feature = "benchmarks")]`-gated has at
//! least one non-test, non-benchmark caller. A module is "wired" if EITHER:
//!
//! - **(a)** some non-test, non-bench `.rs` source file in the workspace — other
//!   than the module's own file/directory and other than `parser/mod.rs` —
//!   references it as a path (`<module>::`); OR
//! - **(b)** `parser/mod.rs` contains a **non-`cfg`-gated** `pub use <module>::`
//!   facade re-export (covers `binary`, reached only through the facade).
//!
//! Benchmark-gated re-exports do NOT count as wiring — that is exactly what makes
//! this guard red on `optimized_complex_types`/`zero_copy_parser` on pre-delete
//! main (their only re-export was `#[cfg(feature = "benchmarks")]`-gated or
//! absent). The guard makes the dead-generation class unre-introducible: a parser
//! module that loses all callers fails this test.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Absolute path to the workspace root (parent of the `cqlite-core` crate dir).
fn workspace_root() -> PathBuf {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .parent()
        .expect("cqlite-core has a parent workspace dir")
        .to_path_buf()
}

/// True for files the guard must not treat as "callers": tests and benchmarks.
fn is_test_or_bench_file(path: &Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return true,
    };
    name.ends_with("_test.rs")
        || name.ends_with("_tests.rs")
        || name.ends_with("benchmarks.rs")
        || name == "mod.rs" && path.parent().and_then(|p| p.file_name()) == Some("tests".as_ref())
}

/// Recursively collect `.rs` files under `dir`, skipping `target/` and any file
/// under a `tests/` directory (integration tests are not "callers").
fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let dname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // Skip build output, dependency trees, VCS, and integration-test dirs
            // (none are production callers).
            if dname == "target"
                || dname == "tests"
                || dname == "node_modules"
                || dname.starts_with('.')
            {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
            && !is_test_or_bench_file(&path)
        {
            out.push(path);
        }
    }
}

/// Parse `parser/mod.rs` and return the set of non-test, non-benchmark module
/// declarations plus the set of modules with a non-`cfg`-gated `pub use M::`
/// facade re-export.
fn parse_mod_rs(mod_rs: &Path) -> (BTreeSet<String>, BTreeSet<String>) {
    let src = fs::read_to_string(mod_rs).expect("read parser/mod.rs");
    let mut modules = BTreeSet::new();
    let mut facade_reexports = BTreeSet::new();

    let lines: Vec<&str> = src.lines().collect();
    for (i, raw) in lines.iter().enumerate() {
        let line = raw.trim();
        // The attribute (if any) immediately preceding this declaration.
        let prev_attr = i
            .checked_sub(1)
            .map(|j| lines[j].trim())
            .filter(|p| p.starts_with("#["));
        let gated_test = prev_attr == Some("#[cfg(test)]");
        let gated_bench = prev_attr
            .map(|p| p.contains("feature = \"benchmarks\""))
            .unwrap_or(false);

        // Module declarations: `mod M;`, `pub mod M;`, `pub(crate) mod M;`.
        if let Some(name) = module_decl_name(line) {
            if !gated_test && !gated_bench {
                modules.insert(name);
            }
            continue;
        }

        // Non-cfg-gated facade re-export: `pub use M::...;`.
        if !gated_test && !gated_bench {
            if let Some(name) = reexport_module_name(line) {
                facade_reexports.insert(name);
            }
        }
    }
    (modules, facade_reexports)
}

/// If `line` declares a module, return its name.
fn module_decl_name(line: &str) -> Option<String> {
    let rest = line
        .strip_prefix("pub(crate) mod ")
        .or_else(|| line.strip_prefix("pub mod "))
        .or_else(|| line.strip_prefix("mod "))?;
    let name = rest.trim_end_matches(';').trim();
    if name.is_empty() || name.contains(' ') {
        return None;
    }
    Some(name.to_string())
}

/// If `line` is `pub use M::...;`, return `M`.
fn reexport_module_name(line: &str) -> Option<String> {
    let rest = line.strip_prefix("pub use ")?;
    let module = rest.split("::").next()?.trim();
    if module.is_empty() || module.contains(' ') || module.contains('{') {
        return None;
    }
    Some(module.to_string())
}

/// Whether `path` is the source of module `M` (file `parser/M.rs` or dir `parser/M/`).
fn is_own_source(path: &Path, parser_dir: &Path, module: &str) -> bool {
    let file = parser_dir.join(format!("{module}.rs"));
    if path == file {
        return true;
    }
    let dir = parser_dir.join(module);
    path.starts_with(&dir)
}

#[test]
fn every_parser_module_has_a_caller() {
    let root = workspace_root();
    let parser_dir = root.join("cqlite-core/src/parser");
    let mod_rs = parser_dir.join("mod.rs");
    assert!(mod_rs.is_file(), "parser/mod.rs must exist at {mod_rs:?}");

    let (modules, facade_reexports) = parse_mod_rs(&mod_rs);
    assert!(
        !modules.is_empty(),
        "expected to discover parser modules in mod.rs"
    );

    // Source dirs that may contain callers (never `tests/` — excluded in the walk).
    let source_dirs = [
        "cqlite-core/src",
        "cqlite-cli/src",
        "cqlite-flight/src",
        "bindings",
    ];
    let mut files = Vec::new();
    for d in source_dirs {
        collect_rs_files(&root.join(d), &mut files);
    }

    // Pre-read every candidate file once.
    let sources: Vec<(PathBuf, String)> = files
        .into_iter()
        .filter_map(|p| fs::read_to_string(&p).ok().map(|s| (p, s)))
        .collect();

    let mut orphaned = Vec::new();
    for module in &modules {
        // Mode (b): non-gated facade re-export.
        if facade_reexports.contains(module) {
            continue;
        }
        // Mode (a): a `<module>::` path reference in a non-test/non-bench file
        // other than the module's own source and parser/mod.rs.
        let needle = format!("{module}::");
        let wired = sources.iter().any(|(path, text)| {
            if path == &mod_rs || is_own_source(path, &parser_dir, module) {
                return false;
            }
            path_reference(text, &needle)
        });
        if !wired {
            orphaned.push(module.clone());
        }
    }

    assert!(
        orphaned.is_empty(),
        "unwired parser module(s) with no non-test/non-bench caller: {orphaned:?}. \
         Every non-test, non-benchmark module under cqlite-core/src/parser/ must have at least one \
         caller (a `<module>::` path use, or a non-gated facade re-export in parser/mod.rs). \
         Delete the dead module or wire it in (issue #1637, finding J3)."
    );
}

/// True if `text` uses `needle` (`<module>::`) as a path segment, i.e. the char
/// immediately before it is not an identifier char (so `foo_module::` does not
/// match `module::`).
fn path_reference(text: &str, needle: &str) -> bool {
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(rel) = text[from..].find(needle) {
        let idx = from + rel;
        let ok_prefix = idx == 0 || {
            let prev = bytes[idx - 1];
            !(prev.is_ascii_alphanumeric() || prev == b'_')
        };
        if ok_prefix {
            return true;
        }
        from = idx + 1;
    }
    false
}
