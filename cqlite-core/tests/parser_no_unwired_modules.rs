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

/// If `line` is a `pub use …M::...;` facade re-export, return `M`.
///
/// Normalizes the leading path prefixes so that `pub use M::…`,
/// `pub use self::M::…`, `pub use crate::parser::M::…`, and
/// `pub use crate::…::parser::M::…` all resolve to module `M` (issue #1637,
/// guard-robustness): a re-export written with an explicit `self::`/`crate::`
/// prefix must not register the facade under `self`/`crate` and thereby falsely
/// orphan a genuinely wired module.
fn reexport_module_name(line: &str) -> Option<String> {
    let rest = line.strip_prefix("pub use ")?.trim();
    // Strip a leading `self::`.
    let rest = rest.strip_prefix("self::").unwrap_or(rest);
    // If the path routes through `…parser::`, the module is the segment right
    // after the last `parser::` (covers `crate::parser::M` and
    // `crate::<...>::parser::M`).
    let rest = match rest.rfind("parser::") {
        Some(pos) => &rest[pos + "parser::".len()..],
        None => rest,
    };
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

    // Pre-read every candidate file once, stripping comments, string/char/raw
    // literals, AND inline `#[cfg(test)]` / `#[cfg(feature = "benchmarks")]`
    // module bodies so a `<module>::` mention that survives only in a
    // comment/string or an inline unit-test/bench module does not falsely count
    // as a production caller (issue #1637, guard-robustness; roborev FIX 1/FIX 2).
    let sources: Vec<(PathBuf, String)> = files
        .into_iter()
        .filter_map(|p| {
            fs::read_to_string(&p)
                .ok()
                .map(|s| (p, strip_inline_cfg_test_modules(&s)))
        })
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

/// Strip Rust line comments (`// … EOL`), block comments (`/* … */`),
/// double-quoted string literals, **char literals** (`'x'`, `'\n'`, `'\''`,
/// `'"'`), and **raw strings** (`r"…"`, `r#"…"#`, `br"…"`, …) from `text`,
/// replacing every removed byte with a space (newlines preserved) so token
/// boundaries stay intact. This ensures a `<module>::` mention that survives
/// ONLY inside a comment, a string, or a raw string does NOT falsely count as
/// wiring (issue #1637, guard-robustness; roborev FIX 2).
///
/// The scanner is length-preserving (each input byte maps to exactly one output
/// byte), so byte offsets in the returned text align with `text` — a property
/// [`strip_inline_cfg_test_modules`] relies on.
///
/// Char literals are consumed as a unit so a char that *holds* a `"` (`'"'`)
/// does not spuriously flip the scanner into string state and blank a following
/// real code span. A lifetime / label (`'a`, `'static`, `'outer:`) has no
/// closing `'` before a non-identifier byte and is therefore left as code
/// (neither a char nor a lifetime can contain a `::` path segment, so leaving a
/// lifetime as code can never create a false wiring match).
fn strip_comments_and_strings(text: &str) -> String {
    enum State {
        Code,
        LineComment,
        BlockComment,
        Str,
        /// Inside a raw string; the payload is the `#` hash count of the opener.
        RawStr(usize),
    }
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut state = State::Code;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let next = bytes.get(i + 1).copied();
        match state {
            State::Code => {
                if b == b'/' && next == Some(b'/') {
                    state = State::LineComment;
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                } else if b == b'/' && next == Some(b'*') {
                    state = State::BlockComment;
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                } else if (b == b'r' || b == b'b') && raw_string_open(bytes, i).is_some() {
                    // Raw string opener `r"`, `r#…"`, `br"`, `br#…"`. Blank the
                    // opener and enter RawStr with the opener's hash count.
                    let (skip, hashes) = raw_string_open(bytes, i).expect("checked Some");
                    out.extend(std::iter::repeat_n(b' ', skip));
                    i += skip;
                    state = State::RawStr(hashes);
                } else if b == b'\'' {
                    if let Some(clen) = char_literal_len(&bytes[i..]) {
                        // A char literal (blank it whole; it holds no newline).
                        out.extend(std::iter::repeat_n(b' ', clen));
                        i += clen;
                    } else {
                        // A lifetime / label: leave the quote as code.
                        out.push(b);
                        i += 1;
                    }
                } else if b == b'"' {
                    state = State::Str;
                    out.push(b' ');
                    i += 1;
                } else {
                    // Non-ASCII bytes are copied verbatim; our delimiters are all
                    // ASCII, so a multibyte char is never split (output stays
                    // valid UTF-8).
                    out.push(b);
                    i += 1;
                }
            }
            State::LineComment => {
                if b == b'\n' {
                    state = State::Code;
                    out.push(b'\n');
                } else {
                    out.push(b' ');
                }
                i += 1;
            }
            State::BlockComment => {
                if b == b'*' && next == Some(b'/') {
                    state = State::Code;
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                } else {
                    out.push(if b == b'\n' { b'\n' } else { b' ' });
                    i += 1;
                }
            }
            State::Str => {
                if b == b'\\' {
                    // Blank the escape and the escaped byte together.
                    out.push(b' ');
                    if next.is_some() {
                        out.push(b' ');
                        i += 2;
                    } else {
                        i += 1;
                    }
                } else if b == b'"' {
                    state = State::Code;
                    out.push(b' ');
                    i += 1;
                } else {
                    out.push(if b == b'\n' { b'\n' } else { b' ' });
                    i += 1;
                }
            }
            State::RawStr(hashes) => {
                // Raw strings have no escapes; the terminator is `"` followed by
                // exactly `hashes` `#`. Everything up to and including it is blanked.
                if b == b'"' && raw_string_close(bytes, i, hashes) {
                    out.extend(std::iter::repeat_n(b' ', 1 + hashes));
                    i += 1 + hashes;
                    state = State::Code;
                } else {
                    out.push(if b == b'\n' { b'\n' } else { b' ' });
                    i += 1;
                }
            }
        }
    }
    String::from_utf8(out).expect("stripped output is valid UTF-8")
}

/// UTF-8 byte length of the code point whose lead byte is `b` (defaults to 1 for
/// a continuation/invalid byte, which is harmless for the char-literal check).
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// If `s` (which starts at a `'`) is a complete Rust **char literal**, return its
/// byte length; otherwise `None` (i.e. it is a lifetime, label, or malformed).
///
/// Handles single chars (`'x'`, `'é'`), the `"`-holding char `'"'`, single-char
/// escapes (`'\n'`, `'\''`, `'\\'`), `\x` hex escapes (`'\xFF'`), and `\u{…}`
/// escapes (`'\u{1F600}'`). A lifetime (`'a`, `'static`) has no closing `'`
/// before a non-identifier byte and returns `None`.
fn char_literal_len(s: &[u8]) -> Option<usize> {
    if s.len() < 2 || s[0] != b'\'' {
        return None;
    }
    if s[1] == b'\\' {
        // Escaped char literal.
        if s.len() < 4 {
            return None;
        }
        match s[2] {
            b'x' => (s.len() >= 6 && s[5] == b'\'').then_some(6),
            b'u' => {
                // '\u{HHHH}': find '}' then the closing '\''.
                let mut j = 3;
                while j < s.len() && s[j] != b'}' {
                    j += 1;
                }
                (j < s.len() && s[j] == b'}' && j + 1 < s.len() && s[j + 1] == b'\'')
                    .then_some(j + 2)
            }
            // Single-char escape: '\n', '\'', '\\', '\0', '\t', …
            _ => (s[3] == b'\'').then_some(4),
        }
    } else {
        // Unescaped single char (possibly multibyte). A lifetime has an
        // identifier byte here and no closing quote → falls through to None.
        let clen = utf8_char_len(s[1]);
        let end = 1 + clen;
        (s.len() > end && s[end] == b'\'').then_some(end + 1)
    }
}

/// If a raw-string opener starts at `bytes[i]` (`r"`, `r#…"`, `br"`, `br#…"`),
/// return `(opener_byte_len, hash_count)`; otherwise `None`.
///
/// The opener must sit at a token boundary (the preceding byte is not an
/// identifier byte) so an identifier ending in `r`/`b` (`for`, `str`, `sub`)
/// does not false-trigger.
fn raw_string_open(bytes: &[u8], i: usize) -> Option<(usize, usize)> {
    let prev_ident = i > 0 && is_ident_byte(bytes[i - 1]);
    if prev_ident {
        return None;
    }
    let mut p = i;
    // Optional byte-string prefix `b` (`br"…"`).
    if bytes.get(p) == Some(&b'b') {
        p += 1;
    }
    if bytes.get(p) != Some(&b'r') {
        return None;
    }
    p += 1;
    let mut hashes = 0;
    while bytes.get(p) == Some(&b'#') {
        hashes += 1;
        p += 1;
    }
    if bytes.get(p) == Some(&b'"') {
        Some((p - i + 1, hashes))
    } else {
        None
    }
}

/// True if `bytes[i]` (`"`) begins a raw-string terminator with exactly `hashes`
/// trailing `#` bytes.
fn raw_string_close(bytes: &[u8], i: usize, hashes: usize) -> bool {
    (1..=hashes).all(|k| bytes.get(i + k) == Some(&b'#'))
}

/// True for identifier bytes (`[A-Za-z0-9_]`).
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Strip comments/strings/char/raw literals (via [`strip_comments_and_strings`])
/// AND blank the bodies of inline `#[cfg(test)]` / `#[cfg(feature =
/// "benchmarks")]` modules, so a `<module>::` reference confined to an inline
/// unit-test or benchmark module does not falsely count as production wiring
/// (issue #1637, guard-robustness; roborev FIX 1).
///
/// The guard already excludes whole test/bench *files* by name, but inline
/// `#[cfg(test)] mod tests { … }` blocks live inside ordinary source files
/// (`binary.rs`, `vint.rs`, `statistics.rs`, …); without this, a parser module
/// referenced only from another file's inline unit-test module would count as
/// "wired" — a lenient false-pass that undercuts the dead-code guard.
///
/// Attribute detection reads the raw text (so the `"benchmarks"` string literal
/// survives), while brace/bracket matching runs on the comment/string-stripped
/// text (so braces/brackets inside strings, chars, or comments never miscount).
/// The two align because [`strip_comments_and_strings`] is length-preserving.
fn strip_inline_cfg_test_modules(text: &str) -> String {
    let stripped = strip_comments_and_strings(text);
    let raw = text.as_bytes();
    let sbytes = stripped.into_bytes();
    debug_assert_eq!(raw.len(), sbytes.len(), "strip must be length-preserving");
    let mut out = sbytes.clone();

    let mut i = 0;
    while i + 3 <= sbytes.len() {
        let is_mod_kw = &sbytes[i..i + 3] == b"mod"
            && (i == 0 || !is_ident_byte(sbytes[i - 1]))
            && (i + 3 == sbytes.len() || !is_ident_byte(sbytes[i + 3]));
        if is_mod_kw {
            // Parse `mod <ident> {` on the stripped text.
            let mut j = i + 3;
            while j < sbytes.len() && sbytes[j].is_ascii_whitespace() {
                j += 1;
            }
            let name_start = j;
            while j < sbytes.len() && is_ident_byte(sbytes[j]) {
                j += 1;
            }
            let name_end = j;
            while j < sbytes.len() && sbytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if name_end > name_start
                && j < sbytes.len()
                && sbytes[j] == b'{'
                && preceding_attrs_are_test_or_bench(raw, &sbytes, i)
            {
                if let Some(close) = matching_brace(&sbytes, j) {
                    for byte in &mut out[(j + 1)..close] {
                        if *byte != b'\n' {
                            *byte = b' ';
                        }
                    }
                    i = close + 1;
                    continue;
                }
            }
        }
        i += 1;
    }
    String::from_utf8(out).expect("stripped output is valid UTF-8")
}

/// Given the index of an opening `{` in `bytes` (comment/string-stripped, so
/// braces appear only in real code), return the index of its matching `}`.
fn matching_brace(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Scanning backward from the start of a `mod` keyword at `mod_start`, decide
/// whether the item's attributes mark it as a `#[cfg(test)]` or
/// `#[cfg(feature = "benchmarks")]` module. Skips an optional `pub` / `pub(…)`
/// visibility, then walks the contiguous run of preceding `#[…]` attributes.
///
/// Bracket matching uses `stripped` (so `]` inside a string/char never
/// miscounts); attribute content is read from `raw` (so the `"benchmarks"`
/// string, which `strip` blanks, is still visible).
fn preceding_attrs_are_test_or_bench(raw: &[u8], stripped: &[u8], mod_start: usize) -> bool {
    let mut k = mod_start;

    // Skip whitespace, then an optional visibility modifier (`pub` / `pub(…)`).
    while k > 0 && stripped[k - 1].is_ascii_whitespace() {
        k -= 1;
    }
    if k > 0 && stripped[k - 1] == b')' {
        // `pub(crate)` / `pub(super)` / `pub(in …)` — skip the paren group.
        let mut depth = 0i32;
        let mut q = k - 1;
        loop {
            match stripped[q] {
                b')' => depth += 1,
                b'(' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            if q == 0 {
                break;
            }
            q -= 1;
        }
        k = q;
    }
    while k > 0 && stripped[k - 1].is_ascii_whitespace() {
        k -= 1;
    }
    if k >= 3 && &stripped[k - 3..k] == b"pub" && (k == 3 || !is_ident_byte(stripped[k - 4])) {
        k -= 3;
    }

    // Walk the contiguous run of preceding `#[…]` attributes.
    loop {
        while k > 0 && stripped[k - 1].is_ascii_whitespace() {
            k -= 1;
        }
        if k == 0 || stripped[k - 1] != b']' {
            return false;
        }
        let close = k - 1; // index of ']'
        let mut depth = 0i32;
        let mut q = close;
        let open;
        loop {
            match stripped[q] {
                b']' => depth += 1,
                b'[' => {
                    depth -= 1;
                    if depth == 0 {
                        open = q;
                        break;
                    }
                }
                _ => {}
            }
            if q == 0 {
                return false;
            }
            q -= 1;
        }
        // Require a `#` (outer attribute) before the `[`.
        if open == 0 || raw[open - 1] != b'#' {
            return false;
        }
        // Attribute content (between `[` and `]`), read from raw, whitespace-stripped.
        let norm: String = raw[open + 1..close]
            .iter()
            .filter(|c| !c.is_ascii_whitespace())
            .map(|&c| c as char)
            .collect();
        if norm.contains("cfg(test)") || norm.contains("feature=\"benchmarks\"") {
            return true;
        }
        // Not the target attribute — keep scanning further-back stacked attributes
        // (k now points at the `#`, which the next iteration excludes).
        k = open - 1;
    }
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

// ---------------------------------------------------------------------------
// Self-checks for the guard's own robustness (issue #1637, guard-robustness).
// These keep the guard from silently degrading into a tautology.
// ---------------------------------------------------------------------------

#[test]
fn comment_or_string_only_mention_is_not_wiring() {
    let src = "\
//! doc: see foo::bar for details
// line comment foo::baz
/* block foo::qux
   still foo::quux */
let s = \"foo::in_a_string\";
let e = \"esc \\\" foo::after_escape\";
";
    let stripped = strip_comments_and_strings(src);
    assert!(
        !path_reference(&stripped, "foo::"),
        "a `foo::` mention appearing ONLY in comments/strings must NOT count as wiring; \
         stripped text was: {stripped:?}"
    );
}

#[test]
fn real_code_path_reference_still_counts_as_wiring() {
    let src = "let x = foo::bar(); // foo::comment\n";
    let stripped = strip_comments_and_strings(src);
    assert!(
        path_reference(&stripped, "foo::"),
        "a genuine `foo::` code reference must still count as wiring"
    );
}

#[test]
fn reexport_prefixes_normalize_to_module_name() {
    // Bare form (current tree) still recognized.
    assert_eq!(
        reexport_module_name("pub use binary::Foo;").as_deref(),
        Some("binary")
    );
    assert_eq!(
        reexport_module_name("pub use self::binary::Foo;").as_deref(),
        Some("binary")
    );
    assert_eq!(
        reexport_module_name("pub use crate::parser::binary::Foo;").as_deref(),
        Some("binary")
    );
    assert_eq!(
        reexport_module_name("pub use crate::storage::parser::binary::Foo;").as_deref(),
        Some("binary")
    );
    // Non-`pub use` lines are not re-exports.
    assert_eq!(reexport_module_name("mod binary;"), None);
}

#[test]
fn char_literal_holding_a_quote_does_not_flip_into_string() {
    // FIX 2: a char literal that holds a `"` (`'"'`) must be consumed as a unit,
    // not treated as a string opener that would blank the following real code
    // reference (which would spuriously orphan `foo`).
    let src = "let q = '\"'; let _ = foo::bar();\n";
    let stripped = strip_comments_and_strings(src);
    assert!(
        path_reference(&stripped, "foo::"),
        "a `foo::` code reference after a `'\\\"'` char literal must still count as wiring; \
         stripped text was: {stripped:?}"
    );
    // Other char-literal forms must not perturb a following code reference.
    for prefix in [
        "let c = '\\'';",
        "let n = '\\n';",
        "let e = '\\u{1F600}';",
        "let x = 'z';",
    ] {
        let s = format!("{prefix} let _ = bar::baz();\n");
        let stripped = strip_comments_and_strings(&s);
        assert!(
            path_reference(&stripped, "bar::"),
            "code after char literal {prefix:?} must count as wiring; stripped: {stripped:?}"
        );
    }
    // A lifetime (`'a`) is left as code and never blanks a following reference.
    let src = "fn f<'a>(x: &'a str) { let _ = qux::run(); }\n";
    let stripped = strip_comments_and_strings(src);
    assert!(
        path_reference(&stripped, "qux::"),
        "a lifetime must not blank a following code reference; stripped: {stripped:?}"
    );
}

#[test]
fn raw_string_only_mention_is_not_wiring() {
    // FIX 2: a `foo::` inside a raw string (including a hashed raw string that
    // itself contains `"`) must NOT count as wiring.
    let src = "let s = r#\"foo::in_raw and \"quoted\" text\"#; let x = 1;\n";
    let stripped = strip_comments_and_strings(src);
    assert!(
        !path_reference(&stripped, "foo::"),
        "a `foo::` mention appearing ONLY inside a raw string must NOT count as wiring; \
         stripped text was: {stripped:?}"
    );
    // Bare raw string and byte raw string too.
    for s in [
        "let s = r\"foo::x\"; let _ = 1;",
        "let s = br#\"foo::y\"#; let _ = 1;",
    ] {
        let stripped = strip_comments_and_strings(s);
        assert!(
            !path_reference(&stripped, "foo::"),
            "raw string form {s:?} must be stripped; got {stripped:?}"
        );
    }
    // A real reference AFTER a raw string still counts (raw string closed correctly).
    let src = "let s = r#\"junk\"#; let _ = wired::call();\n";
    let stripped = strip_comments_and_strings(src);
    assert!(
        path_reference(&stripped, "wired::"),
        "code after a closed raw string must still count as wiring; stripped: {stripped:?}"
    );
}

#[test]
fn reference_only_inside_inline_cfg_test_module_is_not_wiring() {
    // FIX 1: a `<module>::` reference confined to an inline `#[cfg(test)]` module
    // must NOT count as production wiring, while a production reference outside
    // the test module survives the strip.
    let src = "\
pub fn real() {
    let _ = prod::call();
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn t() {
        let _ = testonly::dead();
    }
}
";
    let stripped = strip_inline_cfg_test_modules(src);
    assert!(
        path_reference(&stripped, "prod::"),
        "a production `prod::` reference outside the inline test module must survive; \
         stripped: {stripped:?}"
    );
    assert!(
        !path_reference(&stripped, "testonly::"),
        "a `testonly::` reference confined to an inline #[cfg(test)] module must NOT count \
         as wiring; stripped: {stripped:?}"
    );
}

#[test]
fn reference_only_inside_inline_benchmarks_module_is_not_wiring() {
    // FIX 1: same rule for `#[cfg(feature = "benchmarks")]`-gated inline modules
    // (with a stacked non-target attribute in front to exercise the attribute walk).
    let src = "\
#[cfg(feature = \"benchmarks\")]
#[allow(clippy::all)]
mod benches {
    fn b() {
        let _ = benchonly::run();
    }
}
";
    let stripped = strip_inline_cfg_test_modules(src);
    assert!(
        !path_reference(&stripped, "benchonly::"),
        "a `benchonly::` reference confined to an inline benchmarks module must NOT count \
         as wiring; stripped: {stripped:?}"
    );
}

#[test]
fn non_test_module_body_is_preserved() {
    // Guard against over-stripping: an ordinary (non-cfg-gated) inline module's
    // body must be preserved so its `<module>::` references still count.
    let src = "\
mod util {
    pub fn helper() {
        let _ = wired::call();
    }
}
";
    let stripped = strip_inline_cfg_test_modules(src);
    assert!(
        path_reference(&stripped, "wired::"),
        "a reference inside an ordinary (non-test) inline module must still count as wiring; \
         stripped: {stripped:?}"
    );
}
