//! Standing repo-hygiene guard (issue #1714): every `.rs` file under
//! `cqlite-core/src/` must be REACHABLE from `src/lib.rs` through the
//! `mod`-declaration graph. An unreachable file is dead source that compiles
//! nowhere, is linted nowhere and is tested nowhere, while reading as shipped
//! code — the defect this guard exists for.
//!
//! Scope, per the owner's 2026-08-30 re-scope ruling on #1714 (which killed the
//! previous lexer-equivalence design and closed its PR #3368): this is a
//! deliberately SMALL textual walker. It does NOT model, and must not grow to
//! model, shebang lines, non-ASCII or raw (`r#`) identifiers, a table of Rust
//! literal prefixes, byte-char/escaped-quote spans (a `'{'` is guarded for
//! explicitly, not lexed), `#[path]` ON an inline `mod` block (that form sets the
//! directory its children resolve in), `mod` declarations that do not begin
//! their own (trimmed) source line, escape sequences inside a `#[path]` value,
//! or a `mod` produced by macro expansion. `#[cfg_attr(…, path = …)]` is
//! DETECTED and refused loudly, never resolved. It DOES model out-of-line
//! modules nested in inline `mod` blocks, `#[path]` on those, and attributes
//! broken across lines. There is deliberately **no exception list**: an orphan
//! is a failure to fix, never an entry to add.
//!
//! The vacuity floor (>= 400 enumerated files) is checked BEFORE the orphan
//! assertion, so on a tree far smaller than today's it reds on the floor rather
//! than reporting orphans — a census that lost its subject is not a pass.
//!
//! Residual risk is therefore a textual FALSE PASS — a `mod` declaration this
//! walker reads that rustc does not, or a file rustc reaches by a route not
//! modeled here. The guard is a cheap standing net, not a proof.
//!
//! The durable replacement is **#3366**: a set difference against rustc's own
//! `--emit=dep-info`, which has no lexer of ours to diverge from. It is deferred
//! to P2/Backlog by owner ruling; until it lands, this file is the coverage.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

// ---------------------------------------------------------------------------
// Comment / string-literal stripping
// ---------------------------------------------------------------------------

/// Result of stripping one source file: text with comments removed and every
/// string literal replaced by the placeholder `"@@S<n>@@"`, plus the table of
/// literal contents so a `#[path = "..."]` value can still be recovered.
struct Stripped {
    text: String,
    literals: Vec<String>,
}

impl Stripped {
    /// Resolve a `"@@S<n>@@"` placeholder body back to the literal's contents.
    fn literal(&self, placeholder_body: &str) -> Option<&str> {
        let n = placeholder_body
            .strip_prefix("@@S")?
            .strip_suffix("@@")?
            .parse::<usize>()
            .ok()?;
        self.literals.get(n).map(String::as_str)
    }
}

/// Strip `//` line comments, nested `/* */` block comments, and string
/// literals (normal and raw). Newlines outside literals are preserved so the
/// caller can still scan line-anchored declarations.
fn strip(src: &str) -> Stripped {
    let b = src.as_bytes();
    // Bytes, not chars: cutting only at ASCII boundaries means a byte-wise copy
    // is safe, and nothing here can slice a multi-byte char in half.
    let mut text: Vec<u8> = Vec::with_capacity(src.len());
    let mut literals: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            let mut depth = 1usize;
            i += 2;
            while i < b.len() && depth > 0 {
                if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    if b[i] == b'\n' {
                        text.push(b'\n');
                    }
                    i += 1;
                }
            }
        } else if b[i] == b'"' {
            // One backward look decides raw-vs-normal without a prefix table:
            // a contiguous run of `#` immediately before the quote, preceded by
            // `r`, is a raw string of that hash count (covers `r"`, `r#"`,
            // `br#"`, `cr##"` alike).
            let mut hashes = 0usize;
            let mut j = i;
            while j > 0 && b[j - 1] == b'#' {
                j -= 1;
                hashes += 1;
            }
            let raw = j > 0 && b[j - 1] == b'r';
            let start = i + 1;
            let mut k = start;
            let end = loop {
                if k >= b.len() {
                    break b.len();
                }
                match b[k] {
                    b'\\' if !raw => k += 2,
                    b'"' if !raw => break k,
                    b'"' if b[k + 1..].iter().take_while(|c| **c == b'#').count() >= hashes => {
                        break k
                    }
                    _ => k += 1,
                }
            };
            i = if end < b.len() {
                end + 1 + if raw { hashes } else { 0 }
            } else {
                end
            };
            let body = String::from_utf8_lossy(&b[start.min(b.len())..end]).into_owned();
            text.extend_from_slice(format!("\"@@S{}@@\"", literals.len()).as_bytes());
            literals.push(body);
        } else {
            text.push(b[i]);
            i += 1;
        }
    }
    Stripped {
        text: String::from_utf8_lossy(&text).into_owned(),
        literals,
    }
}

// ---------------------------------------------------------------------------
// Declaration parsing
// ---------------------------------------------------------------------------

/// An out-of-line (`mod foo;`) declaration found in one file.
struct ModDecl {
    name: String,
    path_attr: Option<String>,
    /// Names of the enclosing inline `mod` blocks, outermost first. rustc
    /// resolves such a child under `<moddir>/<name1>/…/<namen>/`.
    scope: Vec<String>,
    /// A `#[cfg_attr(…, path = "…")]` preceded this declaration. That form is
    /// NOT modeled; the walk refuses loudly rather than resolve it wrongly.
    cfg_attr_path: bool,
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Consume leading `#[...]` / `#![...]` attributes from a trimmed line,
/// recording any `#[path = "..."]` value in `pending` and flagging any
/// `#[cfg_attr(…, path = …)]` in `cfg_attr_path`. Returns the remainder.
fn eat_attrs<'a>(
    mut t: &'a str,
    s: &Stripped,
    pending: &mut Option<String>,
    cfg_attr_path: &mut bool,
) -> &'a str {
    while t.starts_with('#') {
        let after_hash = t[1..].trim_start();
        let after_hash = after_hash.strip_prefix('!').unwrap_or(after_hash);
        if !after_hash.starts_with('[') {
            break;
        }
        let open = t.find('[').expect("bracket located above");
        let mut depth = 0usize;
        let mut close = None;
        for (idx, c) in t[open..].char_indices() {
            match c {
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(open + idx);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close) = close else { break };
        let inner = t[open + 1..close].trim();
        if let Some(v) = parse_path_attr(inner, s) {
            *pending = Some(v);
        } else if inner.starts_with("cfg_attr") && mentions_path_assignment(inner) {
            *cfg_attr_path = true;
        }
        t = t[close + 1..].trim_start();
    }
    t
}

/// `path = "..."` (attribute interior) -> the literal's contents.
fn parse_path_attr(inner: &str, s: &Stripped) -> Option<String> {
    let rest = inner.strip_prefix("path")?.trim_start();
    let rest = rest.strip_prefix('=')?.trim();
    let body = rest.strip_prefix('"')?.strip_suffix('"')?;
    s.literal(body).map(str::to_owned)
}

/// Does this attribute interior contain a `path = ...` assignment? Used only to
/// DETECT the unmodeled `#[cfg_attr(…, path = …)]` form, never to resolve it.
fn mentions_path_assignment(inner: &str) -> bool {
    let mut from = 0usize;
    while let Some(idx) = inner[from..].find("path") {
        let at = from + idx;
        let before_ok = at == 0 || !is_ident_char(inner[..at].chars().next_back().unwrap_or(' '));
        if before_ok && inner[at + 4..].trim_start().starts_with('=') {
            return true;
        }
        from = at + 4;
    }
    false
}

/// Newlines inside `[...]` become spaces, so an attribute broken across lines
/// (e.g. a multi-line `#[path = "..."]`) is still ONE logical line for the
/// line-anchored scan below.
fn join_bracketed_lines(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut depth = 0usize;
    for c in text.chars() {
        match c {
            '[' => {
                depth += 1;
                out.push(c);
            }
            ']' => {
                depth = depth.saturating_sub(1);
                out.push(c);
            }
            '\n' if depth > 0 => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}

/// Track `{}` nesting across one logical line, maintaining the stack of
/// enclosing inline-module names. `inline_name`, when set, is the module opened
/// by the FIRST brace on this line.
fn scan_braces(
    line: &str,
    depth: &mut usize,
    stack: &mut Vec<(usize, String)>,
    inline_name: &mut Option<String>,
) {
    let b = line.as_bytes();
    for i in 0..b.len() {
        if b[i] != b'{' && b[i] != b'}' {
            continue;
        }
        // A char literal `'{'` is not a block delimiter, and char literals are
        // deliberately not stripped (out of scope). 13 such literals live in
        // cqlite-core/src today, so without this guard the depth would skew
        // permanently and mis-scope every later declaration in the file.
        if i > 0 && b[i - 1] == b'\'' && b.get(i + 1) == Some(&b'\'') {
            continue;
        }
        if b[i] == b'{' {
            *depth += 1;
            if let Some(n) = inline_name.take() {
                stack.push((*depth, n));
            }
        } else {
            *depth = depth.saturating_sub(1);
            while stack.last().map(|(d, _)| *d > *depth).unwrap_or(false) {
                stack.pop();
            }
        }
    }
}

/// `[pub[(..)]] mod NAME (;|{)` at the start of a trimmed line.
/// Returns `(name, is_inline)`.
fn parse_mod_decl(t: &str) -> Option<(String, bool)> {
    let mut rest = t;
    if let Some(r) = rest.strip_prefix("pub") {
        if r.starts_with(|c: char| is_ident_char(c)) {
            return None;
        }
        rest = r.trim_start();
        if rest.starts_with('(') {
            let close = rest.find(')')?;
            rest = rest[close + 1..].trim_start();
        }
    }
    let r = rest.strip_prefix("mod")?;
    if r.starts_with(|c: char| is_ident_char(c)) {
        return None;
    }
    rest = r.trim_start();
    let name: String = rest.chars().take_while(|c| is_ident_char(*c)).collect();
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    rest = rest[name.len()..].trim_start();
    match rest.chars().next()? {
        ';' => Some((name, false)),
        '{' => Some((name, true)),
        _ => None,
    }
}

/// All out-of-line module declarations in one source file.
fn mod_decls(src: &str) -> Vec<ModDecl> {
    let s = strip(src);
    let joined = join_bracketed_lines(&s.text);
    let mut out = Vec::new();
    let mut pending: Option<String> = None;
    let mut pending_cfg_attr_path = false;
    let mut depth = 0usize;
    let mut stack: Vec<(usize, String)> = Vec::new();
    for line in joined.lines() {
        let t = line.trim();
        let mut inline_name: Option<String> = None;
        if !t.is_empty() {
            let rest = eat_attrs(t, &s, &mut pending, &mut pending_cfg_attr_path);
            if !rest.is_empty() {
                match parse_mod_decl(rest) {
                    // An inline `mod x { … }` declares no file, but it DOES
                    // scope every out-of-line child inside it.
                    Some((name, true)) => {
                        pending = None;
                        pending_cfg_attr_path = false;
                        inline_name = Some(name);
                    }
                    Some((name, false)) => out.push(ModDecl {
                        name,
                        path_attr: pending.take(),
                        scope: stack.iter().map(|(_, n)| n.clone()).collect(),
                        cfg_attr_path: std::mem::take(&mut pending_cfg_attr_path),
                    }),
                    None => {
                        pending = None;
                        pending_cfg_attr_path = false;
                    }
                }
            }
        }
        scan_braces(line, &mut depth, &mut stack, &mut inline_name);
    }
    out
}

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

fn normalize(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[derive(Default)]
struct Walk {
    reached: BTreeSet<PathBuf>,
    /// Declarations that resolved to no existing file: the walker's resolution
    /// is wrong, or the source does not compile. Never silently skipped.
    unresolved: Vec<String>,
}

impl Walk {
    fn visit(&mut self, file: &Path) {
        let file = normalize(file);
        if !self.reached.insert(file.clone()) {
            return;
        }
        let src = match fs::read_to_string(&file) {
            Ok(s) => s,
            Err(e) => {
                self.unresolved
                    .push(format!("{} could not be read: {e}", file.display()));
                return;
            }
        };
        let base = file.parent().unwrap_or(Path::new(".")).to_path_buf();
        let stem = file
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        // `lib.rs`/`main.rs`/`mod.rs` own their own directory; any other file
        // owns the `<stem>/` subdirectory beneath it.
        let moddir = if matches!(stem.as_str(), "lib" | "main" | "mod") {
            base.clone()
        } else {
            base.join(&stem)
        };
        for decl in mod_decls(&src) {
            assert!(
                !decl.cfg_attr_path,
                "{}: `mod {};` is preceded by a #[cfg_attr(…, path = …)] attribute — an \
                 unmodeled attribute form. This guard refuses it rather than resolve the \
                 module to the wrong file (issue #1714); wire the module with a plain \
                 #[path] instead, or extend this walker deliberately",
                file.display(),
                decl.name,
            );
            // An out-of-line module inside inline `mod` blocks resolves under
            // `<moddir>/<block1>/…/<blockn>/`, and a `#[path]` on it is relative
            // to that same directory rather than to the declaring file's dir.
            let scoped = decl
                .scope
                .iter()
                .fold(moddir.clone(), |d, name| d.join(name));
            let path_base = if decl.scope.is_empty() {
                base.clone()
            } else {
                scoped.clone()
            };
            if let Some(rel) = &decl.path_attr {
                // `#[path]` on an out-of-line module resolves relative to the
                // directory containing the DECLARING FILE, never to `<stem>/`.
                // Getting this backwards reports 57 false orphans in this repo.
                assert!(
                    !Path::new(rel).is_absolute(),
                    "{}: #[path = {rel:?}] on `mod {}` is an ABSOLUTE path; this guard \
                     refuses to model absolute #[path] values (an absolute value silently \
                     folding in-crate marked an unrelated in-crate file reachable)",
                    file.display(),
                    decl.name,
                );
                let target = normalize(&path_base.join(rel));
                if target.is_file() {
                    self.visit(&target);
                } else {
                    self.unresolved.push(format!(
                        "{}: `mod {};` with #[path = {rel:?}] resolves to {}, which does not exist",
                        file.display(),
                        decl.name,
                        target.display()
                    ));
                }
                continue;
            }
            let flat = scoped.join(format!("{}.rs", decl.name));
            let dir = scoped.join(&decl.name).join("mod.rs");
            if flat.is_file() {
                self.visit(&flat);
            } else if dir.is_file() {
                self.visit(&dir);
            } else {
                self.unresolved.push(format!(
                    "{}: `mod {};` resolves to neither {} nor {}",
                    file.display(),
                    decl.name,
                    flat.display(),
                    dir.display()
                ));
            }
        }
    }
}

fn walk_from(root_file: &Path) -> Walk {
    let mut w = Walk::default();
    w.visit(root_file);
    w
}

fn enumerate_rs(dir: &Path) -> BTreeSet<PathBuf> {
    let mut out = BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = fs::read_dir(&d).unwrap_or_else(|e| panic!("read_dir {}: {e}", d.display()));
        for entry in entries {
            let entry = entry.unwrap_or_else(|e| panic!("dir entry under {}: {e}", d.display()));
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().map(|e| e == "rs").unwrap_or(false) {
                out.insert(normalize(&p));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// A. The live assertion
// ---------------------------------------------------------------------------

#[test]
fn every_cqlite_core_src_file_is_reachable() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let all = enumerate_rs(&src);

    // The measurement must have a subject: a walk that enumerated nothing (or
    // almost nothing) must never read as a pass. 526 files as of 2026-09-01.
    assert!(
        all.len() >= 400,
        "enumerated only {} .rs files under {} — the census failed; this guard \
         cannot pass vacuously (expect ~526)",
        all.len(),
        src.display()
    );

    let w = walk_from(&src.join("lib.rs"));
    assert!(
        w.unresolved.is_empty(),
        "mod declarations that resolve to no file ({}):\n  {}",
        w.unresolved.len(),
        w.unresolved.join("\n  ")
    );

    let orphans: Vec<String> = all
        .difference(&w.reached)
        .map(|p| p.display().to_string())
        .collect();
    assert!(
        orphans.is_empty(),
        "{} file(s) under cqlite-core/src are UNREACHABLE from lib.rs through the \
         mod graph — they compile nowhere, are linted nowhere and are tested nowhere \
         (issue #1714). Fix by deleting the file, or by wiring it with a `mod` \
         declaration in its parent. There is deliberately no exception list.\n  {}",
        orphans.len(),
        orphans.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// B. Synthetic-tree unit tests
// ---------------------------------------------------------------------------

/// Materialize `files` (paths relative to a fresh `src/`), walk from
/// `src/lib.rs`, and return `(orphans, unresolved)` — orphans as `/`-joined
/// paths relative to `src/`, sorted.
fn probe(files: &[(&str, &str)]) -> (Vec<String>, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src");
    for (rel, body) in files {
        let p = src.join(rel);
        fs::create_dir_all(p.parent().expect("file has a parent")).expect("create_dir_all");
        fs::write(&p, body).expect("write fixture");
    }
    let w = walk_from(&src.join("lib.rs"));
    let all = enumerate_rs(&src);
    let orphans = all
        .difference(&w.reached)
        .map(|p| {
            p.strip_prefix(&src)
                .expect("enumerated under src")
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    (orphans, w.unresolved.clone())
}

/// `probe` asserting no unresolved declarations, returning the orphan list.
fn orphans(files: &[(&str, &str)]) -> Vec<String> {
    let (orphans, unresolved) = probe(files);
    assert!(
        unresolved.is_empty(),
        "unexpected unresolved: {unresolved:?}"
    );
    orphans
}

#[test]
fn plain_mod_resolves_to_sibling_file() {
    assert!(orphans(&[("lib.rs", "mod a;\n"), ("a.rs", "")]).is_empty());
}

#[test]
fn directory_module_and_its_nested_child_are_reached() {
    assert!(orphans(&[
        ("lib.rs", "mod a;\n"),
        ("a/mod.rs", "mod b;\n"),
        ("a/b.rs", ""),
    ])
    .is_empty());
}

#[test]
fn non_mod_rs_parent_owns_its_stem_directory() {
    // `a.rs` is not `mod.rs`, so `mod b;` inside it means `a/b.rs`.
    assert!(orphans(&[("lib.rs", "mod a;\n"), ("a.rs", "mod b;\n"), ("a/b.rs", "")]).is_empty());
}

#[test]
fn visibility_modifiers_do_not_hide_a_declaration() {
    assert!(orphans(&[
        (
            "lib.rs",
            "pub mod a;\npub(crate) mod b;\npub(in crate::a) mod c;\n"
        ),
        ("a.rs", ""),
        ("b.rs", ""),
        ("c.rs", ""),
    ])
    .is_empty());
}

#[test]
fn path_attr_in_lib_resolves_beside_lib() {
    assert!(orphans(&[
        ("lib.rs", "#[path = \"elsewhere.rs\"]\nmod x;\n"),
        ("elsewhere.rs", ""),
    ])
    .is_empty());
}

#[test]
fn path_attr_resolves_beside_the_declaring_file_not_under_its_stem_dir() {
    // THE regression that matters (issue #1714): `#[path]` on an out-of-line
    // module is relative to the directory holding the DECLARING FILE. Reading
    // it as `<stem>/` instead reports 57 false orphans in cqlite-core.
    // `a/p.rs` is a decoy: a walker using the wrong base reaches it and
    // reports `p.rs` orphaned, inverting this assertion.
    assert_eq!(
        orphans(&[
            ("lib.rs", "mod a;\n"),
            ("a.rs", "#[path = \"p.rs\"]\nmod x;\n"),
            ("p.rs", ""),
            ("a/p.rs", ""),
        ]),
        vec!["a/p.rs".to_string()]
    );
}

#[test]
fn path_attr_associates_across_other_attributes_in_either_order() {
    for lib in [
        "#[cfg(test)]\n#[path = \"t.rs\"]\nmod tests;\n",
        "#[path = \"t.rs\"]\n#[cfg(test)]\nmod tests;\n",
        "#[cfg(test)]\n/// doc\n#[path = \"t.rs\"]\nmod tests;\n",
        "#[cfg(test)] #[path = \"t.rs\"] mod tests;\n",
    ] {
        assert!(
            orphans(&[("lib.rs", lib), ("t.rs", "")]).is_empty(),
            "failed for {lib:?}"
        );
    }
}

#[test]
fn cfg_gated_mod_counts_as_reachable() {
    // A gated module IS declared; this guard filters on no attribute at all.
    assert!(orphans(&[
        (
            "lib.rs",
            "#[cfg(test)]\nmod a;\n#[cfg(feature = \"nope\")]\nmod b;\n"
        ),
        ("a.rs", ""),
        ("b.rs", ""),
    ])
    .is_empty());
}

#[test]
fn inline_mod_declares_no_file() {
    // `mod x { }` declares no file, so `x.rs` beside it stays an orphan.
    assert_eq!(
        orphans(&[("lib.rs", "mod x {\n    pub fn f() {}\n}\n"), ("x.rs", "")]),
        vec!["x.rs".to_string()]
    );
}

// --- RED arms: each differs from its green twin in exactly ONE property -----

#[test]
fn an_unreferenced_file_is_reported_as_an_orphan() {
    // Green twin: lib.rs declares `mod orphan;`.
    assert!(orphans(&[("lib.rs", "mod orphan;\n"), ("orphan.rs", "")]).is_empty());
    // RED arm: the declaration is absent. Only property changed.
    assert_eq!(
        orphans(&[("lib.rs", "\n"), ("orphan.rs", "")]),
        vec!["orphan.rs".to_string()]
    );
}

#[test]
fn a_mod_inside_a_line_comment_does_not_reach_its_file() {
    assert!(orphans(&[("lib.rs", "mod orphan;\n"), ("orphan.rs", "")]).is_empty());
    assert_eq!(
        orphans(&[("lib.rs", "// mod orphan;\n"), ("orphan.rs", "")]),
        vec!["orphan.rs".to_string()]
    );
}

#[test]
fn a_mod_inside_a_block_comment_does_not_reach_its_file() {
    assert!(orphans(&[("lib.rs", "mod orphan;\n"), ("orphan.rs", "")]).is_empty());
    assert_eq!(
        orphans(&[("lib.rs", "/*\nmod orphan;\n*/\n"), ("orphan.rs", "")]),
        vec!["orphan.rs".to_string()]
    );
    // Nested block comments close at the right depth.
    assert_eq!(
        orphans(&[("lib.rs", "/* /* mod orphan; */ */\n"), ("orphan.rs", "")]),
        vec!["orphan.rs".to_string()]
    );
}

#[test]
fn a_mod_inside_a_string_literal_does_not_reach_its_file() {
    assert!(orphans(&[("lib.rs", "mod orphan;\n"), ("orphan.rs", "")]).is_empty());
    assert_eq!(
        orphans(&[
            ("lib.rs", "pub const S: &str = \"mod orphan;\";\n"),
            ("orphan.rs", ""),
        ]),
        vec!["orphan.rs".to_string()]
    );
    // Raw string, decided by one backward look at the `#` run before the quote.
    assert_eq!(
        orphans(&[
            ("lib.rs", "pub const S: &str = r#\"mod orphan;\"#;\n"),
            ("orphan.rs", ""),
        ]),
        vec!["orphan.rs".to_string()]
    );
}

#[test]
fn a_mod_declaration_resolving_to_no_file_is_reported_loudly() {
    let (_, unresolved) = probe(&[("lib.rs", "mod missing;\n")]);
    assert_eq!(unresolved.len(), 1, "{unresolved:?}");
    assert!(
        unresolved[0].contains("`mod missing;`") && unresolved[0].contains("missing.rs"),
        "{unresolved:?}"
    );
}

#[test]
#[should_panic(expected = "ABSOLUTE")]
fn an_absolute_path_attr_is_refused_loudly() {
    let _ = probe(&[("lib.rs", "#[path = \"/etc/passwd\"]\nmod x;\n")]);
}
