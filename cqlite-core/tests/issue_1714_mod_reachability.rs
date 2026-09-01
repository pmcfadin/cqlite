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
//! literal prefixes, byte-char/escaped-quote spans, `#[path]` inside an inline
//! `mod` block, `mod` declarations that do not begin their own (trimmed) source
//! line, escape sequences inside a `#[path]` value, or a `mod` produced by macro
//! expansion. There is deliberately **no exception list**: an orphan is a
//! failure to fix, never an entry to add.
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
    let mut text = String::with_capacity(src.len());
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
                        text.push('\n');
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
            let end;
            if raw {
                let mut k = start;
                loop {
                    if k >= b.len() {
                        end = b.len();
                        break;
                    }
                    if b[k] == b'"' && b[k + 1..].iter().take(hashes).filter(|c| **c == b'#').count() == hashes {
                        end = k;
                        break;
                    }
                    k += 1;
                }
                i = if end < b.len() { end + 1 + hashes } else { end };
            } else {
                let mut k = start;
                loop {
                    if k >= b.len() {
                        end = b.len();
                        break;
                    }
                    if b[k] == b'\\' {
                        k += 2;
                        continue;
                    }
                    if b[k] == b'"' {
                        end = k;
                        break;
                    }
                    k += 1;
                }
                i = if end < b.len() { end + 1 } else { end };
            }
            let body = String::from_utf8_lossy(&b[start.min(b.len())..end]).into_owned();
            text.push_str(&format!("\"@@S{}@@\"", literals.len()));
            literals.push(body);
        } else {
            // Copy one char (not byte) so multi-byte UTF-8 stays intact.
            let ch = src[i..].chars().next().expect("index is a char boundary");
            text.push(ch);
            i += ch.len_utf8();
        }
    }
    Stripped { text, literals }
}

// ---------------------------------------------------------------------------
// Declaration parsing
// ---------------------------------------------------------------------------

/// An out-of-line (`mod foo;`) declaration found in one file.
struct ModDecl {
    name: String,
    path_attr: Option<String>,
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Consume leading `#[...]` / `#![...]` attributes from a trimmed line,
/// recording any `#[path = "..."]` value in `pending`. Returns the remainder.
fn eat_attrs<'a>(mut t: &'a str, s: &Stripped, pending: &mut Option<String>) -> &'a str {
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
        if let Some(v) = parse_path_attr(t[open + 1..close].trim(), s) {
            *pending = Some(v);
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
    let mut out = Vec::new();
    let mut pending: Option<String> = None;
    for line in s.text.lines() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let rest = eat_attrs(t, &s, &mut pending);
        if rest.is_empty() {
            continue;
        }
        match parse_mod_decl(rest) {
            Some((name, inline)) => {
                let path_attr = pending.take();
                if !inline {
                    out.push(ModDecl { name, path_attr });
                }
            }
            None => pending = None,
        }
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
                let target = normalize(&base.join(rel));
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
            let flat = moddir.join(format!("{}.rs", decl.name));
            let dir = moddir.join(&decl.name).join("mod.rs");
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
