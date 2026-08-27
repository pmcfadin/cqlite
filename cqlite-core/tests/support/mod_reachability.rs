//! Crate-agnostic `mod`-reachability walker (issue #1714, AK3).
//!
//! # What this exists to catch
//!
//! A `.rs` file under a crate's `src/` that no `mod` declaration chain reaches from
//! the crate root is **never compiled**. Editing it is a silent no-op: it type-checks
//! nothing, its tests never run, and `cargo test` stays green while the code rots.
//! `cqlite-core/src/memory_safety_tests.rs` was exactly that for months before it was
//! deleted (PR #2044) — and the deletion alone does not stop the next one. This walker
//! is the standing guard: it enumerates every `src/**/*.rs`, computes the set rustc
//! actually reaches, and fails on the difference.
//!
//! # Reuse by other crates (issue #1502, the CLI mod-wiring guard)
//!
//! Nothing here is `cqlite-core`-specific: [`ModuleGraphSpec`] parameterizes the crate
//! directory, the root file (`src/lib.rs` for a library, `src/main.rs` for a binary)
//! and the source directory, and the expected-orphan list lives in the *caller's* test.
//! **Recommendation for #1502**: pull this module into `cqlite-cli/tests/` with
//! `#[path = "../../cqlite-core/tests/support/mod_reachability.rs"] mod mod_reachability;`
//! and pass `root_file_rel = "src/main.rs"`. That keeps one implementation and one set
//! of unit tests. Promotion to a shared `dev-dependency` crate is the cleaner end state
//! but costs a new workspace member, so it is only worth doing once a third caller
//! appears. The CLI side is **not** implemented here — it is #1502's scope.
//!
//! # Control vs data: sanitize before parsing (CLAUDE.md #3312)
//!
//! A `mod foo;` inside a comment, a doc comment, or a string literal is **data**, not
//! a declaration. Scanning raw text would let a doc example such as the `cql_generator`
//! mention in `storage/write_engine/merge/mod.rs` make an orphan look reachable — a
//! **false PASS**, the worst failure mode a hygiene guard has. So [`sanitize`] blanks
//! every comment (line, block, **nested** block, doc) and every complete literal
//! (string with escapes, raw string at any hash level, byte string, char literal)
//! before [`parse_mod_decls`] ever sees the text, and the attribute values the parser
//! genuinely needs (`#[path = "…"]`) are recovered from a **side table** keyed by byte
//! offset rather than by re-reading the text. Control tokens and caller data therefore
//! never share a channel.
//!
//! # Fail-closed
//!
//! Every construct this walker does not model is an `Err`, never a skip: `include!`,
//! `#[cfg_attr(…, path = …)]`, a `#[path]` on an inline `mod` block, a `mod` token inside
//! a macro's token tree (see [`scan_macro_context`] — rustc, not this walker, decides
//! what a macro expands to), a symlink under the source directory (see
//! [`enumerate_rs_files`] — a silently-skipped subtree is a census with a hole in it), an
//! unterminated comment or literal, an unreadable file, an unknown escape, and a
//! `mod name;` that resolves to neither candidate file. A skip is the vacuous pass this
//! guard exists to prevent.

#![allow(dead_code)]

use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

/// What to analyze. Crate-agnostic on purpose — see the module docs (#1502).
#[derive(Debug, Clone)]
pub struct ModuleGraphSpec {
    /// Absolute path to the crate directory (the one holding `Cargo.toml`).
    pub crate_dir: PathBuf,
    /// Crate-relative root file, e.g. `src/lib.rs` (library) or `src/main.rs` (binary).
    pub root_file_rel: String,
    /// Crate-relative source directory whose `*.rs` files must all be reachable.
    pub src_dir_rel: String,
}

/// A file that is *known* to be unreachable, with the issue that owns removing it.
#[derive(Debug, Clone, Copy)]
pub struct ExpectedOrphan {
    /// Path relative to [`ModuleGraphSpec::src_dir_rel`], forward slashes.
    pub path: &'static str,
    /// The issue that owns deleting (or wiring in) the file.
    pub issue: &'static str,
    /// Why the exception exists.
    pub reason: &'static str,
}

/// Outcome of a reachability analysis. All keys are crate-relative, `/`-separated.
#[derive(Debug, Clone)]
pub struct Report {
    /// Every `*.rs` file found under the source directory.
    pub enumerated: BTreeSet<String>,
    /// Every file rustc reaches from the root, including the root itself.
    pub reachable: BTreeSet<String>,
    /// `enumerated - reachable`, with no exceptions applied (the caller applies those).
    pub orphans: BTreeSet<String>,
    /// How many module files were parsed while walking (root included).
    pub modules_parsed: usize,
    /// How many `mod` declarations were resolved to a file.
    pub mod_decls_resolved: usize,
}

impl Report {
    /// `orphans` minus the expected set — the ones a human must act on.
    pub fn unexpected_orphans(
        &self,
        expected: &[ExpectedOrphan],
        src_dir_rel: &str,
    ) -> Vec<String> {
        let allowed: BTreeSet<String> = expected
            .iter()
            .map(|e| join_key(src_dir_rel, e.path))
            .collect();
        self.orphans
            .iter()
            .filter(|o| !allowed.contains(*o))
            .cloned()
            .collect()
    }
}

/// Walk the module graph and diff it against the on-disk census.
///
/// Returns `Err` with an actionable cause for every unmodeled construct (see module docs).
pub fn analyze(spec: &ModuleGraphSpec) -> Result<Report, String> {
    let src_root = spec.crate_dir.join(&spec.src_dir_rel);
    if !src_root.is_dir() {
        return Err(format!(
            "source directory `{}` does not exist or is not a directory — \
             a walker with no census cannot certify anything (FAIL-CLOSED)",
            src_root.display()
        ));
    }

    let mut enumerated = BTreeSet::new();
    enumerate_rs_files(&spec.crate_dir, &src_root, &mut enumerated)?;

    let root_key = normalize_key(&spec.root_file_rel)?;
    if !spec.crate_dir.join(&root_key).is_file() {
        return Err(format!(
            "root file `{root_key}` does not exist under `{}` (FAIL-CLOSED)",
            spec.crate_dir.display()
        ));
    }

    let mut reachable: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(root_key);
    let mut modules_parsed = 0usize;
    let mut mod_decls_resolved = 0usize;

    while let Some(key) = queue.pop_front() {
        if !reachable.insert(key.clone()) {
            continue;
        }
        let abs = spec.crate_dir.join(&key);
        let text = fs::read_to_string(&abs).map_err(|e| {
            format!(
                "unreadable module file `{key}`: {e} — an unreadable file cannot be \
                 analyzed, so its children are unknown (FAIL-CLOSED)"
            )
        })?;
        modules_parsed += 1;
        let sanitized = sanitize(&text).map_err(|e| format!("{key}: {e}"))?;
        let decls = parse_mod_decls(&sanitized, &key)?;
        for decl in decls {
            let child = resolve_mod_file(&spec.crate_dir, &key, &decl)?;
            mod_decls_resolved += 1;
            queue.push_back(child);
        }
    }

    let orphans = enumerated.difference(&reachable).cloned().collect();
    Ok(Report {
        enumerated,
        reachable,
        orphans,
        modules_parsed,
        mod_decls_resolved,
    })
}

fn enumerate_rs_files(
    crate_dir: &Path,
    dir: &Path,
    out: &mut BTreeSet<String>,
) -> Result<(), String> {
    let entries = fs::read_dir(dir).map_err(|e| {
        format!(
            "cannot read directory `{}`: {e} (FAIL-CLOSED)",
            dir.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "cannot read entry in `{}`: {e} (FAIL-CLOSED)",
                dir.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| format!("cannot stat `{}`: {e} (FAIL-CLOSED)", path.display()))?;
        if file_type.is_dir() {
            enumerate_rs_files(crate_dir, &path, out)?;
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            let rel = path.strip_prefix(crate_dir).map_err(|_| {
                format!(
                    "`{}` is not under crate dir `{}` (FAIL-CLOSED)",
                    path.display(),
                    crate_dir.display()
                )
            })?;
            out.insert(path_to_key(rel));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Sanitizer: comments and literals become blanks; literal values go to a side table.
// ---------------------------------------------------------------------------

/// A complete literal that was blanked out of the sanitized text.
#[derive(Debug, Clone)]
pub struct Literal {
    /// Byte offset of the literal's first byte in the sanitized text.
    pub start: usize,
    /// Byte offset one past the literal's last byte.
    pub end: usize,
    /// The literal's unescaped value (empty for char literals).
    pub value: String,
}

/// Sanitized source: parseable text plus the literal values the parser may need.
#[derive(Debug, Clone)]
pub struct Sanitized {
    /// Same byte length as the input; comments/literals blanked, newlines preserved.
    pub text: String,
    /// Every string/char literal that was blanked, in source order.
    pub literals: Vec<Literal>,
}

impl Sanitized {
    fn literals_in(&self, start: usize, end: usize) -> Vec<&Literal> {
        self.literals
            .iter()
            .filter(|l| l.start >= start && l.end <= end)
            .collect()
    }
}

/// Convenience wrapper used by the stripper unit tests.
pub fn strip_comments_and_strings(src: &str) -> Result<String, String> {
    Ok(sanitize(src)?.text)
}

/// Blank every comment and literal, preserving byte offsets and line breaks.
pub fn sanitize(src: &str) -> Result<Sanitized, String> {
    let b = src.as_bytes();
    let n = b.len();
    let mut out = b.to_vec();
    let mut literals: Vec<Literal> = Vec::new();
    let mut i = 0usize;

    while i < n {
        let c = b[i];
        if c == b'/' && i + 1 < n && b[i + 1] == b'/' {
            let mut j = i + 2;
            while j < n && b[j] != b'\n' {
                j += 1;
            }
            blank(&mut out, i, j);
            i = j;
        } else if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
            let mut depth = 1usize;
            let mut j = i + 2;
            while j < n && depth > 0 {
                if b[j] == b'/' && j + 1 < n && b[j + 1] == b'*' {
                    depth += 1;
                    j += 2;
                } else if b[j] == b'*' && j + 1 < n && b[j + 1] == b'/' {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            if depth > 0 {
                return Err("unterminated block comment (FAIL-CLOSED)".to_string());
            }
            blank(&mut out, i, j);
            i = j;
        } else if c == b'"' {
            let (end, value) = scan_string(b, i)?;
            blank(&mut out, i, end);
            literals.push(Literal {
                start: i,
                end,
                value,
            });
            i = end;
        } else if c == b'\'' {
            match scan_char_or_lifetime(b, i)? {
                Some(end) => {
                    blank(&mut out, i, end);
                    literals.push(Literal {
                        start: i,
                        end,
                        value: String::new(),
                    });
                    i = end;
                }
                // A lifetime (`'a`): ordinary code, leave it alone.
                None => i += 1,
            }
        } else if is_ident_start(c) {
            let ident_end = ident_end(b, i);
            let ident = &src[i..ident_end];
            let next = b.get(ident_end).copied();
            // `r"…"` / `r#"…"#` / `br#"…"#` are raw STRINGS; `r#type` is a raw
            // IDENTIFIER (Rust allows keywords as identifiers that way, and
            // `cqlite-core` uses `r#type`). They differ only in what follows the
            // hashes: a `"` for the string, an identifier byte for the identifier.
            let raw_prefix = matches!(ident, "r" | "br") && is_raw_string_prefix(b, ident_end);
            let byte_prefix = ident == "b" && next == Some(b'"');
            if raw_prefix {
                let (end, value) = scan_raw_string(b, i, ident_end)?;
                blank(&mut out, i, end);
                literals.push(Literal {
                    start: i,
                    end,
                    value,
                });
                i = end;
            } else if byte_prefix {
                let (end, value) = scan_string(b, ident_end)?;
                blank(&mut out, i, end);
                literals.push(Literal {
                    start: i,
                    end,
                    value,
                });
                i = end;
            } else {
                i = ident_end;
            }
        } else {
            i += 1;
        }
    }

    let text = String::from_utf8(out).map_err(|e| {
        format!("sanitized text is not valid UTF-8 ({e}) — refusing to parse (FAIL-CLOSED)")
    })?;
    Ok(Sanitized { text, literals })
}

fn blank(out: &mut [u8], start: usize, end: usize) {
    for byte in &mut out[start..end] {
        if *byte != b'\n' {
            *byte = b' ';
        }
    }
}

/// Scan a `"…"` literal starting at the opening quote. Returns `(end, unescaped value)`.
fn scan_string(b: &[u8], start: usize) -> Result<(usize, String), String> {
    let n = b.len();
    let mut j = start + 1;
    let mut value = String::new();
    while j < n {
        match b[j] {
            b'\\' => {
                if j + 1 >= n {
                    return Err("unterminated escape in string literal (FAIL-CLOSED)".to_string());
                }
                let esc = b[j + 1];
                let decoded = match esc {
                    b'\\' => '\\',
                    b'"' => '"',
                    b'\'' => '\'',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'0' => '\0',
                    b'\n' => {
                        // Line continuation: skip the newline and following whitespace.
                        j += 2;
                        while j < n && (b[j] as char).is_ascii_whitespace() {
                            j += 1;
                        }
                        continue;
                    }
                    b'x' => {
                        // `\xNN` — exactly two hex digits.
                        let hex = b.get(j + 2..j + 4).ok_or_else(|| {
                            "truncated `\\x` escape in string literal (FAIL-CLOSED)".to_string()
                        })?;
                        let text = std::str::from_utf8(hex).map_err(|_| {
                            "non-UTF-8 `\\x` escape in string literal (FAIL-CLOSED)".to_string()
                        })?;
                        let byte = u8::from_str_radix(text, 16).map_err(|_| {
                            format!("malformed `\\x{text}` escape in string literal (FAIL-CLOSED)")
                        })?;
                        value.push(byte as char);
                        j += 4;
                        continue;
                    }
                    b'u' => {
                        // `\u{HEX…}`
                        if b.get(j + 2) != Some(&b'{') {
                            return Err("malformed `\\u` escape (expected `{`) in string literal \
                                 (FAIL-CLOSED)"
                                .to_string());
                        }
                        let mut k = j + 3;
                        let mut digits = String::new();
                        while k < n && b[k] != b'}' {
                            if b[k] != b'_' {
                                digits.push(b[k] as char);
                            }
                            k += 1;
                        }
                        if k >= n {
                            return Err(
                                "unterminated `\\u{…}` escape in string literal (FAIL-CLOSED)"
                                    .to_string(),
                            );
                        }
                        let code = u32::from_str_radix(&digits, 16).map_err(|_| {
                            format!("malformed `\\u{{{digits}}}` escape (FAIL-CLOSED)")
                        })?;
                        let ch = char::from_u32(code).ok_or_else(|| {
                            format!("`\\u{{{digits}}}` is not a Unicode scalar (FAIL-CLOSED)")
                        })?;
                        value.push(ch);
                        j = k + 1;
                        continue;
                    }
                    other => {
                        return Err(format!(
                            "unmodeled string escape `\\{}` — this walker does not decode it, \
                             and guessing could mis-read a `#[path]` value (FAIL-CLOSED)",
                            other as char
                        ))
                    }
                };
                value.push(decoded);
                j += 2;
            }
            b'"' => return Ok((j + 1, value)),
            other => {
                value.push(other as char);
                j += 1;
            }
        }
    }
    Err("unterminated string literal (FAIL-CLOSED)".to_string())
}

/// Scan `r#"…"#` (any hash count). `prefix_end` is the byte after the `r`/`br` ident.
fn scan_raw_string(b: &[u8], start: usize, prefix_end: usize) -> Result<(usize, String), String> {
    let n = b.len();
    let mut hashes = 0usize;
    let mut j = prefix_end;
    while j < n && b[j] == b'#' {
        hashes += 1;
        j += 1;
    }
    if j >= n || b[j] != b'"' {
        return Err(format!(
            "malformed raw string literal at byte {start} (FAIL-CLOSED)"
        ));
    }
    let content_start = j + 1;
    let mut k = content_start;
    while k < n {
        if b[k] == b'"' {
            let mut closing = 0usize;
            while closing < hashes && k + 1 + closing < n && b[k + 1 + closing] == b'#' {
                closing += 1;
            }
            if closing == hashes {
                let value = String::from_utf8_lossy(&b[content_start..k]).into_owned();
                return Ok((k + 1 + hashes, value));
            }
        }
        k += 1;
    }
    Err("unterminated raw string literal (FAIL-CLOSED)".to_string())
}

/// `Ok(Some(end))` for a char literal, `Ok(None)` for a lifetime.
fn scan_char_or_lifetime(b: &[u8], start: usize) -> Result<Option<usize>, String> {
    let n = b.len();
    if start + 1 >= n {
        return Ok(None);
    }
    if b[start + 1] == b'\\' {
        let mut j = start + 2;
        // Escapes may be multi-byte (`\u{1F600}`); scan to the closing quote.
        while j < n && b[j] != b'\'' {
            j += 1;
        }
        if j >= n {
            return Err("unterminated char literal (FAIL-CLOSED)".to_string());
        }
        return Ok(Some(j + 1));
    }
    // One UTF-8 scalar followed by `'` is a char literal; anything else is a lifetime.
    let mut char_len = 1usize;
    while start + 1 + char_len < n && (b[start + 1 + char_len] & 0xC0) == 0x80 {
        char_len += 1;
    }
    let close = start + 1 + char_len;
    if close < n && b[close] == b'\'' {
        Ok(Some(close + 1))
    } else {
        Ok(None)
    }
}

/// `true` when the bytes at `after_prefix` (just past an `r`/`br` ident) open a raw
/// STRING literal: zero or more `#` followed by `"`. `r#type` therefore returns `false`
/// (that is a raw identifier, not a literal).
fn is_raw_string_prefix(b: &[u8], after_prefix: usize) -> bool {
    let mut j = after_prefix;
    while j < b.len() && b[j] == b'#' {
        j += 1;
    }
    b.get(j) == Some(&b'"')
}

fn is_ident_start(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphabetic()
}

fn is_ident_byte(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

fn ident_end(b: &[u8], start: usize) -> usize {
    let mut j = start;
    while j < b.len() && is_ident_byte(b[j]) {
        j += 1;
    }
    j
}

// ---------------------------------------------------------------------------
// Parser: `mod` declarations out of sanitized text.
// ---------------------------------------------------------------------------

/// One `mod` declaration that names a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModDecl {
    pub name: String,
    /// `Some(value)` when declared with `#[path = "value"]`.
    pub path_attr: Option<String>,
    /// Names of the enclosing *inline* `mod` blocks, outermost first.
    pub inline_prefix: Vec<String>,
    /// 1-based line of the `mod` keyword, for diagnostics.
    pub line: usize,
}

/// Extract every external (`mod name;`) declaration, recursing through inline blocks.
///
/// cfg-gated declarations are **kept**: a `#[cfg(test)]`/`#[cfg(feature = …)]` module is
/// reachable (issue #1714 AC-3). No feature evaluation is attempted, by design.
pub fn parse_mod_decls(s: &Sanitized, file_label: &str) -> Result<Vec<ModDecl>, String> {
    let text = &s.text;
    let b = text.as_bytes();
    let n = b.len();
    let mut i = 0usize;
    let mut depth = 0usize;
    // (inline mod name, brace depth of its body)
    let mut inline_stack: Vec<(String, usize)> = Vec::new();
    let mut pending_path: Option<String> = None;
    let mut out: Vec<ModDecl> = Vec::new();

    while i < n {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'#' {
            let mut j = i + 1;
            if j < n && b[j] == b'!' {
                j += 1;
            }
            if j < n && b[j] == b'[' {
                let close = matching_bracket(b, j, b'[', b']').ok_or_else(|| {
                    format!("{file_label}: unbalanced attribute brackets (FAIL-CLOSED)")
                })?;
                let inner_start = j + 1;
                let inner = &text[inner_start..close];
                if let Some(path) = attr_path_value(s, inner, inner_start, close, file_label)? {
                    pending_path = Some(path);
                }
                i = close + 1;
                continue;
            }
            i += 1;
            continue;
        }
        if c == b'{' {
            depth += 1;
            i += 1;
            continue;
        }
        if c == b'}' {
            depth = depth.saturating_sub(1);
            while inline_stack.last().map(|t| t.1 > depth).unwrap_or(false) {
                inline_stack.pop();
            }
            i += 1;
            continue;
        }
        if is_ident_start(c) {
            let end = ident_end(b, i);
            let word = &text[i..end];
            let word_start = i;
            i = end;
            match word {
                "pub" => {
                    // `pub(crate)`, `pub(super)`, `pub(in path)` — skip the qualifier,
                    // keep any pending `#[path]` for the item that follows.
                    let mut k = skip_ws(b, i);
                    if k < n && b[k] == b'(' {
                        let close = matching_bracket(b, k, b'(', b')').ok_or_else(|| {
                            format!("{file_label}: unbalanced visibility parens (FAIL-CLOSED)")
                        })?;
                        k = close + 1;
                    }
                    i = k;
                }
                "mod" => {
                    let k = skip_ws(b, i);
                    if k >= n || !is_ident_start(b[k]) {
                        return Err(format!(
                            "{file_label}:{}: `mod` not followed by an identifier (FAIL-CLOSED)",
                            line_of(text, word_start)
                        ));
                    }
                    let name_end = ident_end(b, k);
                    let name = text[k..name_end].to_string();
                    let after = skip_ws(b, name_end);
                    if after < n && b[after] == b';' {
                        out.push(ModDecl {
                            name,
                            path_attr: pending_path.take(),
                            inline_prefix: inline_stack.iter().map(|t| t.0.clone()).collect(),
                            line: line_of(text, word_start),
                        });
                        i = after + 1;
                    } else if after < n && b[after] == b'{' {
                        if pending_path.is_some() {
                            return Err(format!(
                                "{file_label}:{}: `#[path]` on an INLINE `mod {name} {{ … }}` \
                                 changes the directory its children resolve against; this walker \
                                 does not model it (FAIL-CLOSED — see #1714)",
                                line_of(text, word_start)
                            ));
                        }
                        depth += 1;
                        inline_stack.push((name, depth));
                        i = after + 1;
                    } else {
                        return Err(format!(
                            "{file_label}:{}: `mod {name}` followed by neither `;` nor `{{` \
                             (FAIL-CLOSED)",
                            line_of(text, word_start)
                        ));
                    }
                    pending_path = None;
                }
                "r" if b.get(i) == Some(&b'#')
                    && b.get(i + 1).map(|c| is_ident_start(*c)).unwrap_or(false) =>
                {
                    // Raw identifier (`r#type`, `r#mod`): an identifier, never a keyword.
                    i = ident_end(b, i + 1);
                    pending_path = None;
                }
                "include" => {
                    let k = skip_ws(b, i);
                    if k < n && b[k] == b'!' {
                        return Err(format!(
                            "{file_label}:{}: `include!` brings a file into the module graph in a \
                             way this walker does not model — a file included this way would be \
                             misreported as an orphan (FAIL-CLOSED — see #1714)",
                            line_of(text, word_start)
                        ));
                    }
                    pending_path = None;
                }
                _ => {
                    // A macro's token tree is neither control nor data this walker can
                    // read — rustc decides what it expands to. `Some(end)` means the
                    // tree held no `mod` token and is skipped wholesale; a `mod` inside
                    // one is an `Err` (see `scan_macro_context`).
                    if let Some(after_macro) =
                        scan_macro_context(b, text, word, i, word_start, file_label)?
                    {
                        i = after_macro;
                    }
                    pending_path = None;
                }
            }
            continue;
        }
        pending_path = None;
        i += 1;
    }
    Ok(out)
}

/// If the identifier just consumed opens a **macro context**, return the byte offset one
/// past its token tree; `None` when it does not open one.
///
/// Two shapes are recognized: a definition `macro_rules! name { … }` and an invocation
/// `name!( … )` / `name![ … ]` / `name!{ … }` (a path-qualified `foo::bar!( … )` arrives
/// here as its last segment, `bar`).
///
/// # Why a `mod` in here is an `Err` and not a declaration
///
/// `mod orphan;` inside a macro is **not** a declaration the walker can trust: rustc
/// decides whether that token tree is ever expanded, how many times, and with what name
/// (`mod $n;`). Counting it makes an unreachable file look reachable — the same false
/// PASS a commented-out `mod` produced for `parser/collection_udt_tests.rs`, and the
/// worst failure mode a hygiene guard has. Expanding macros is out of scope, so this
/// fails CLOSED instead (module docs, "Fail-closed").
///
/// The refusal is scoped to token trees that actually contain a `mod` token: an
/// over-broad rule that reds on every `format!` is the rule someone deletes, which is
/// why `an_ordinary_macro_does_not_trip_the_mod_in_macro_guard` pins the other
/// direction.
fn scan_macro_context(
    b: &[u8],
    text: &str,
    word: &str,
    ident_end_at: usize,
    word_start: usize,
    file_label: &str,
) -> Result<Option<usize>, String> {
    let n = b.len();
    let mut k = skip_ws(b, ident_end_at);
    if k >= n || b[k] != b'!' {
        return Ok(None);
    }
    k = skip_ws(b, k + 1);

    // `macro_rules! name { … }` names the macro BETWEEN the `!` and the token tree;
    // an invocation puts the name before the `!`.
    let mut macro_name = format!("{word}!");
    if word == "macro_rules" && k < n && is_ident_start(b[k]) {
        let name_end = ident_end(b, k);
        macro_name = format!("macro_rules! {}", &text[k..name_end]);
        k = skip_ws(b, name_end);
    }

    // No delimiter after the `!` means this was never a macro invocation — `a != b`
    // reaches here and must be left to the ordinary scan.
    let (open, close_byte) = match b.get(k) {
        Some(b'(') => (b'(', b')'),
        Some(b'[') => (b'[', b']'),
        Some(b'{') => (b'{', b'}'),
        _ => return Ok(None),
    };
    let close = matching_bracket(b, k, open, close_byte).ok_or_else(|| {
        format!(
            "{file_label}:{}: unbalanced `{}` token tree for `{macro_name}` — the walker \
             cannot tell where the macro ends, so it cannot know what follows it \
             (FAIL-CLOSED)",
            line_of(text, word_start),
            open as char
        )
    })?;

    if let Some(mod_at) = find_mod_token(b, text, k + 1, close) {
        return Err(format!(
            "{file_label}:{}: a `mod` declaration appears inside the token tree of \
             `{macro_name}`. This walker does not expand macros, so it cannot tell whether \
             that declaration names a real module file, how many times it is expanded, or \
             what its name expands to — counting it would make an unreachable file look \
             reachable (FAIL-CLOSED — see #1714).\n\
             Remedy: declare the module outside the macro, or teach the walker this macro's \
             expansion (tests/support/mod_reachability.rs).",
            line_of(text, mod_at)
        ));
    }
    Ok(Some(close + 1))
}

/// Byte offset of a `mod <ident>` / `mod $meta` token sequence within `[start, end)`, if any.
///
/// Token-aware on both sides: `module` and `r#mod` are identifiers, not the keyword.
fn find_mod_token(b: &[u8], text: &str, start: usize, end: usize) -> Option<usize> {
    let mut i = start;
    while i < end {
        if is_ident_start(b[i]) {
            let word_end = ident_end(b, i).min(end);
            // `r#mod` is a raw IDENTIFIER; the `#` before it is the tell.
            let raw_ident = i > 0 && b[i - 1] == b'#';
            if !raw_ident && &text[i..word_end] == "mod" {
                let after = skip_ws(b, word_end);
                // `mod name;` or a `macro_rules!` metavariable `mod $name;`.
                if after < end && (is_ident_start(b[after]) || b[after] == b'$') {
                    return Some(i);
                }
            }
            i = word_end.max(i + 1);
        } else {
            i += 1;
        }
    }
    None
}

/// `Some(value)` when the attribute body is a `path = "…"`; `Err` for unmodeled shapes.
fn attr_path_value(
    s: &Sanitized,
    inner: &str,
    inner_start: usize,
    inner_end: usize,
    file_label: &str,
) -> Result<Option<String>, String> {
    let trimmed = inner.trim_start();
    if trimmed.starts_with("cfg_attr") && contains_word(inner, "path") {
        return Err(format!(
            "{file_label}: `#[cfg_attr(…, path = …)]` conditionally redirects a module file; \
             this walker does not model it (FAIL-CLOSED — see #1714)"
        ));
    }
    if !trimmed.starts_with("path") {
        return Ok(None);
    }
    let after = trimmed["path".len()..].trim_start();
    if !after.starts_with('=') {
        return Ok(None);
    }
    let lits = s.literals_in(inner_start, inner_end);
    if lits.len() != 1 {
        return Err(format!(
            "{file_label}: `#[path …]` with {} literal(s) — expected exactly one string value \
             (FAIL-CLOSED)",
            lits.len()
        ));
    }
    Ok(Some(lits[0].value.clone()))
}

fn contains_word(haystack: &str, word: &str) -> bool {
    let b = haystack.as_bytes();
    let w = word.as_bytes();
    let mut i = 0usize;
    while i + w.len() <= b.len() {
        if &b[i..i + w.len()] == w {
            let before_ok = i == 0 || !is_ident_byte(b[i - 1]);
            let after_ok = i + w.len() == b.len() || !is_ident_byte(b[i + w.len()]);
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn matching_bracket(b: &[u8], open_at: usize, open: u8, close: u8) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open_at;
    while i < b.len() {
        if b[i] == open {
            depth += 1;
        } else if b[i] == close {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn line_of(text: &str, offset: usize) -> usize {
    text.as_bytes()[..offset.min(text.len())]
        .iter()
        .filter(|c| **c == b'\n')
        .count()
        + 1
}

// ---------------------------------------------------------------------------
// File resolution (rustc semantics).
// ---------------------------------------------------------------------------

/// Resolve one `mod` declaration to the crate-relative file it names.
fn resolve_mod_file(crate_dir: &Path, parent_key: &str, decl: &ModDecl) -> Result<String, String> {
    let parent_dir = dirname(parent_key);
    let stem = file_stem(parent_key);
    // `lib.rs`/`main.rs`/`mod.rs` own their own directory; `foo.rs` owns `foo/`.
    let module_dir = if matches!(stem.as_str(), "lib" | "main" | "mod") {
        parent_dir.clone()
    } else {
        join_key(&parent_dir, &stem)
    };

    if let Some(raw) = &decl.path_attr {
        // The Rust Reference, "Modules / The path attribute":
        //   * a `#[path]` mod at FILE TOP LEVEL resolves relative to the DIRECTORY of the
        //     declaring file (so `#[path = "otel_tests.rs"]` in `observability/otel.rs`
        //     names `observability/otel_tests.rs`, NOT `observability/otel/…`);
        //   * a `#[path]` mod INSIDE an inline `mod` block resolves relative to the
        //     module directory (i.e. `<dir>` for a mod-rs file — `lib.rs`/`main.rs`/
        //     `mod.rs` — and `<dir>/<stem>` for any other file) extended by the names of
        //     the enclosing inline `mod` blocks.
        let mut base = if decl.inline_prefix.is_empty() {
            parent_dir
        } else {
            module_dir
        };
        for part in &decl.inline_prefix {
            base = join_key(&base, part);
        }
        let key = normalize_key(&join_key(&base, raw))?;
        if crate_dir.join(&key).is_file() {
            return Ok(key);
        }
        return Err(format!(
            "{parent_key}:{}: `#[path = \"{raw}\"] mod {}` resolves to `{key}`, which does not \
             exist (FAIL-CLOSED)",
            decl.line, decl.name
        ));
    }

    let mut base = module_dir;
    for part in &decl.inline_prefix {
        base = join_key(&base, part);
    }
    let flat = normalize_key(&format!("{base}/{}.rs", decl.name))?;
    let nested = normalize_key(&format!("{base}/{}/mod.rs", decl.name))?;
    let flat_exists = crate_dir.join(&flat).is_file();
    let nested_exists = crate_dir.join(&nested).is_file();
    match (flat_exists, nested_exists) {
        (true, false) => Ok(flat),
        (false, true) => Ok(nested),
        (true, true) => Err(format!(
            "{parent_key}:{}: `mod {}` is ambiguous — both `{flat}` and `{nested}` exist \
             (rustc rejects this too) (FAIL-CLOSED)",
            decl.line, decl.name
        )),
        (false, false) => Err(format!(
            "{parent_key}:{}: `mod {}` resolves to neither `{flat}` nor `{nested}`, and carries \
             no `#[path]` attribute — the module graph cannot be walked past this point \
             (FAIL-CLOSED)",
            decl.line, decl.name
        )),
    }
}

fn path_to_key(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn join_key(a: &str, b: &str) -> String {
    if a.is_empty() {
        b.to_string()
    } else if b.is_empty() {
        a.to_string()
    } else {
        format!("{}/{}", a.trim_end_matches('/'), b)
    }
}

fn dirname(key: &str) -> String {
    match key.rfind('/') {
        Some(idx) => key[..idx].to_string(),
        None => String::new(),
    }
}

fn file_stem(key: &str) -> String {
    let base = key.rsplit('/').next().unwrap_or(key);
    match base.rfind('.') {
        Some(idx) => base[..idx].to_string(),
        None => base.to_string(),
    }
}

/// Lexically resolve `.` and `..` in a `/`-separated key. Escaping the crate root is an error.
fn normalize_key(key: &str) -> Result<String, String> {
    let mut parts: Vec<String> = Vec::new();
    let unified = key.replace('\\', "/");
    for part in unified.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(format!(
                        "path `{key}` escapes the crate directory — this walker only models \
                         in-crate module files (FAIL-CLOSED)"
                    ));
                }
            }
            other => parts.push(other.to_string()),
        }
    }
    Ok(parts.join("/"))
}
