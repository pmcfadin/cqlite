//! Table-driven Rust lexical recognition for the `mod`-reachability walker (issue #1714).
//!
//! # Why this is one table and not scattered prefix checks
//!
//! The walker's whole value is that it cannot be fooled into calling an unreachable file
//! reachable. Every false PASS it has ever had came from the same shape: a `mod orphan;`
//! that lives inside something the lexer did not recognize — a comment, a string, a macro
//! token tree — and therefore got scanned as ordinary code. Two review rounds produced
//! five findings of that shape, and the last two (`cr#"…"#` raw C-strings, and raw
//! identifiers such as `macro_rules! r#make`) were the *same* bug twice: the prefix
//! recognition lived in three places (`sanitize`, `parse_mod_decls`, `find_mod_token`),
//! each knowing a different subset of Rust's prefixes, and **an unrecognized prefix fell
//! through to ordinary scanning**.
//!
//! So prefix knowledge lives here, once, in [`PREFIXES`], and every caller goes through
//! [`lex_token`] / [`ident_token`]. Adding a Rust literal form is a one-row edit.
//!
//! # Fail closed on an unrecognized prefix — the load-bearing half
//!
//! Rust **reserves** every `ident"…"`, `ident'…'` and `ident#…` sequence (edition 2021
//! reserved prefixes, RFC 3101), precisely so future literal forms can be added. This
//! lexer takes the same position: an identifier-ish token immediately followed by `"`,
//! `'` or `#` that is not in [`PREFIXES`] is an **`Err` naming the file, line and token**
//! — never a fall-through to ordinary scanning. That converts the entire family from
//! "silent false PASS" into "loud refusal", so a Rust literal prefix newer than this
//! table cannot quietly disable the guard: the walk stops and a human adds the row.
//!
//! # The complete set as of Rust 2024
//!
//! | form | example |
//! |------|---------|
//! | string | `"s"` |
//! | raw string (any hash count) | `r"s"`, `r#"s"#`, `r##"s"##` |
//! | byte string | `b"s"` |
//! | raw byte string | `br"s"`, `br#"s"#` |
//! | C string | `c"s"` |
//! | raw C string | `cr"s"`, `cr#"s"#` |
//! | char | `'c'`, `'\''`, `'\u{1F600}'` |
//! | byte char | `b'c'` |
//! | raw identifier (legal anywhere an identifier is) | `r#type`, `r#make` |
//!
//! A bare `'` is a char literal **or** a lifetime/label (`'a`, `'static`, and the raw
//! lifetime `'r#fn`); telling them apart is [`scan_char_or_lifetime`]'s job.
//!
//! # Fail closed on a NON-ASCII identifier — the same boundary, one family over
//!
//! Rust identifiers are `XID_Start XID_Continue*`, so `café`, `Übersicht` and `模块` are
//! all legal identifiers and `macro_rules! café { () => { mod orphan; } }` is a real macro
//! definition. This lexer's identifier rules are **deliberately ASCII-only**
//! ([`is_ident_start`] / [`is_ident_byte`]), because widening them means shipping (or
//! depending on) Unicode XID tables — a second implementation of rustc's lexer, which is
//! the mistake that produced this walker's whole review history.
//!
//! So the ASCII-only rules keep a **boundary**, not a blind spot: a non-ASCII byte in a
//! position where an identifier could start or continue is an `Err`
//! ([`refuse_non_ascii`]), exactly as an unrecognized literal prefix is. Without it the
//! lexer would return `caf` for `café`, leave `é` to the ordinary scan, fail to recognize
//! the macro context, and count the `mod orphan;` in the macro body as a real declaration
//! — the silent FALSE PASS this walker exists to prevent.

#![allow(dead_code)]

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/// The byte that follows a prefix identifier and decides what it introduces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Follower {
    /// `"` — a quoted literal starts immediately.
    Quote,
    /// `'` — a char-like literal (or, unprefixed, possibly a lifetime).
    Apostrophe,
    /// `#` — raw string hashes, or a raw identifier.
    Hash,
}

/// How to scan the token a recognized (prefix, follower) pair introduces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    /// `"…"` with escapes: string, byte string, C string.
    Quoted,
    /// `r"…"` / `r#"…"#` at any hash count, byte or C flavored.
    RawQuoted,
    /// `'…'` — unprefixed, so it may instead be a lifetime/label.
    CharOrLifetime,
    /// `b'…'` — always a literal; a lifetime cannot carry the `b` prefix.
    ByteChar,
    /// `r#` — either a raw string (`r#"…"#`) or a raw identifier (`r#type`).
    RawQuotedOrRawIdent,
}

struct PrefixRow {
    /// The identifier immediately before the follower; `""` for the unprefixed forms.
    ident: &'static str,
    follower: Follower,
    form: Form,
}

/// Every literal/identifier prefix Rust 2024 defines. **This is the single place prefix
/// knowledge lives** (see the module docs); anything absent from it fails closed.
const PREFIXES: &[PrefixRow] = &[
    // Unprefixed forms.
    PrefixRow {
        ident: "",
        follower: Follower::Quote,
        form: Form::Quoted,
    },
    PrefixRow {
        ident: "",
        follower: Follower::Apostrophe,
        form: Form::CharOrLifetime,
    },
    // Byte flavored.
    PrefixRow {
        ident: "b",
        follower: Follower::Quote,
        form: Form::Quoted,
    },
    PrefixRow {
        ident: "b",
        follower: Follower::Apostrophe,
        form: Form::ByteChar,
    },
    PrefixRow {
        ident: "br",
        follower: Follower::Quote,
        form: Form::RawQuoted,
    },
    PrefixRow {
        ident: "br",
        follower: Follower::Hash,
        form: Form::RawQuoted,
    },
    // C-string flavored (Rust 1.77+): `c"…"`, `cr"…"`, `cr#"…"#`.
    PrefixRow {
        ident: "c",
        follower: Follower::Quote,
        form: Form::Quoted,
    },
    PrefixRow {
        ident: "cr",
        follower: Follower::Quote,
        form: Form::RawQuoted,
    },
    PrefixRow {
        ident: "cr",
        follower: Follower::Hash,
        form: Form::RawQuoted,
    },
    // Plain raw: `r"…"`, and `r#` which is a raw string OR a raw identifier.
    PrefixRow {
        ident: "r",
        follower: Follower::Quote,
        form: Form::RawQuoted,
    },
    PrefixRow {
        ident: "r",
        follower: Follower::Hash,
        form: Form::RawQuotedOrRawIdent,
    },
];

fn follower_of(byte: u8) -> Option<Follower> {
    match byte {
        b'"' => Some(Follower::Quote),
        b'\'' => Some(Follower::Apostrophe),
        b'#' => Some(Follower::Hash),
        _ => None,
    }
}

fn lookup(ident: &str, follower: Follower) -> Option<Form> {
    PREFIXES
        .iter()
        .find(|row| row.ident == ident && row.follower == follower)
        .map(|row| row.form)
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// An identifier, with any `r#` consumed **atomically**.
///
/// `name_start..name_end` is the identifier *name* — the `r#` is not part of it, which is
/// why `mod r#type;` declares a module whose file is `type.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentSpan {
    pub start: usize,
    /// One past the whole token (past `type` in `r#type`).
    pub end: usize,
    pub name_start: usize,
    pub name_end: usize,
    /// `true` for `r#ident`: an identifier, therefore **never** a keyword.
    pub raw: bool,
}

/// What [`lex_token`] found.
#[derive(Debug, Clone)]
pub enum LexToken {
    /// A complete literal spanning `start..end`; `value` is unescaped (empty for chars).
    Literal { end: usize, value: String },
    /// An identifier (possibly raw).
    Ident(IdentSpan),
    /// A lifetime/label sigil (`'a`, `'static`, `'r#fn`) — ordinary code, one byte wide.
    Lifetime { end: usize },
}

/// The identifier token starting at `start`, consuming a leading `r#` atomically.
///
/// Infallible by design: this is the form used on **already-sanitized** text, where every
/// literal has been blanked, so `r#` can only be a raw identifier. A stray `r#` followed
/// by a non-identifier byte (impossible post-[`sanitize`]-style blanking, which fails
/// closed on it) degrades to the plain identifier `r`, leaving the `#` to the caller —
/// never to a silent skip.
///
/// [`sanitize`]: crate
pub fn ident_token(b: &[u8], start: usize) -> IdentSpan {
    let id_end = ident_end(b, start);
    if b.get(id_end) == Some(&b'#')
        && b.get(id_end + 1)
            .copied()
            .map(is_ident_start)
            .unwrap_or(false)
        && &b[start..id_end] == b"r"
    {
        let name_start = id_end + 1;
        let name_end = ident_end(b, name_start);
        return IdentSpan {
            start,
            end: name_end,
            name_start,
            name_end,
            raw: true,
        };
    }
    IdentSpan {
        start,
        end: id_end,
        name_start: start,
        name_end: id_end,
        raw: false,
    }
}

/// Lex the token at `start`, which must be an identifier-start byte, `"`, or `'`.
///
/// Returns `Err` for an unrecognized prefix (see the module docs — this is the
/// fail-closed branch that keeps the prefix family closed) and for a malformed or
/// unterminated literal.
pub fn lex_token(b: &[u8], src: &str, start: usize) -> Result<LexToken, String> {
    // Identifier-family analogue of the unrecognized-prefix refusal below: a non-ASCII
    // byte here could START a Unicode identifier this lexer does not model (#1714).
    refuse_non_ascii(b, src, start, IdentPos::Start)?;
    // The unprefixed rows are looked up in exactly the same table as the prefixed ones,
    // so `"` and `'` cannot drift away from what `PREFIXES` says about them.
    if let Some(follower) = follower_of(b[start]) {
        if follower != Follower::Hash {
            let form =
                lookup("", follower).ok_or_else(|| unrecognized(src, start, "", b[start]))?;
            return scan_form(b, src, start, start, form, "");
        }
    }
    if !is_ident_start(b[start]) {
        return Err(format!(
            "line {}: `{}` is not the start of a token this lexer models (FAIL-CLOSED — \
             see #1714)",
            line_of(src, start),
            b[start] as char
        ));
    }
    let id_end = ident_end(b, start);
    // ...and the CONTINUE position. `ident_end` stops at the first non-ident ASCII byte,
    // so without this the identifier `café` would lex as `caf` and its macro context
    // would go unrecognized (#1714).
    refuse_non_ascii(b, src, id_end, IdentPos::Continue)?;
    let ident = &src[start..id_end];
    let Some(next) = b.get(id_end).copied() else {
        return Ok(LexToken::Ident(ident_token(b, start)));
    };
    let Some(follower) = follower_of(next) else {
        return Ok(LexToken::Ident(ident_token(b, start)));
    };
    // An identifier-ish token glued to `"`, `'` or `#` is a PREFIX. If the table does not
    // know it, refuse: falling through to ordinary scanning would parse the literal's
    // contents as code, and a `mod orphan;` inside it would be counted as a real
    // declaration — a silent FALSE PASS, the one failure mode this walker exists to
    // prevent. Rust itself reserves these sequences for exactly this reason (#1714).
    let form = lookup(ident, follower).ok_or_else(|| unrecognized(src, start, ident, next))?;
    scan_form(b, src, start, id_end, form, ident)
}

fn scan_form(
    b: &[u8],
    src: &str,
    start: usize,
    prefix_end: usize,
    form: Form,
    ident: &str,
) -> Result<LexToken, String> {
    match form {
        Form::Quoted => {
            let (end, value) = scan_string(b, prefix_end)?;
            Ok(LexToken::Literal { end, value })
        }
        Form::RawQuoted => {
            let (end, value) = scan_raw_string(b, start, prefix_end)?;
            Ok(LexToken::Literal { end, value })
        }
        Form::CharOrLifetime => match scan_char_or_lifetime(b, prefix_end)? {
            Some(end) => Ok(LexToken::Literal {
                end,
                value: String::new(),
            }),
            None => Ok(LexToken::Lifetime { end: start + 1 }),
        },
        Form::ByteChar => match scan_char_or_lifetime(b, prefix_end)? {
            Some(end) => Ok(LexToken::Literal {
                end,
                value: String::new(),
            }),
            // `b'` cannot introduce a lifetime, so an unclosed one is malformed input,
            // not code to scan past.
            None => Err(format!(
                "line {}: malformed byte-char literal `b'` (FAIL-CLOSED — see #1714)",
                line_of(src, start)
            )),
        },
        Form::RawQuotedOrRawIdent => {
            if is_raw_string_prefix(b, prefix_end) {
                let (end, value) = scan_raw_string(b, start, prefix_end)?;
                return Ok(LexToken::Literal { end, value });
            }
            let span = ident_token(b, start);
            if span.raw {
                return Ok(LexToken::Ident(span));
            }
            // `r##foo`, `r#1`: neither a raw string nor a raw identifier.
            Err(unrecognized(src, start, ident, b'#'))
        }
    }
}

/// Which identifier position a non-ASCII byte was met in. Both are refusals; the
/// distinction is diagnostic only — it tells the reader whether the character would have
/// begun an identifier or extended the one just scanned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentPos {
    /// A byte where an identifier could START (`café`'s `c` position).
    Start,
    /// A byte where the identifier just scanned could CONTINUE (`café`'s `é`).
    Continue,
}

impl IdentPos {
    fn describe(self) -> &'static str {
        match self {
            IdentPos::Start => "could START a Rust identifier",
            IdentPos::Continue => "could CONTINUE the identifier just scanned",
        }
    }
}

/// Refuse a non-ASCII byte met where an identifier could start or continue.
///
/// **This is the identifier-family analogue of [`unrecognized`]** (the literal-prefix
/// refusal): the same fail-closed boundary, one lexical family over. See the module docs
/// for why the identifier rules are ASCII-only and why widening them is the wrong fix.
///
/// `Ok(())` for `at` past the end of `b` and for every ASCII byte — non-ASCII text in
/// comments and literals never reaches here, because the sanitizer blanks and skips those
/// spans before the scan can reach a byte inside one.
pub fn refuse_non_ascii(b: &[u8], src: &str, at: usize, pos: IdentPos) -> Result<(), String> {
    let Some(&byte) = b.get(at) else {
        return Ok(());
    };
    if byte.is_ascii() {
        return Ok(());
    }
    Err(non_ascii_refusal(src, at, byte, pos))
}

/// `byte` is passed in rather than re-read from `src`, so this cannot index out of bounds
/// even if a caller ever hands it a `src` that is not `b`'s text.
fn non_ascii_refusal(src: &str, at: usize, byte: u8, pos: IdentPos) -> String {
    // The byte is shown as a CHARACTER when the offset is a UTF-8 boundary (it always is
    // in practice: everything scanned before it was ASCII), and as a raw byte otherwise,
    // so the diagnostic can never itself mangle the input it is reporting.
    let shown = match src.get(at..).and_then(|rest| rest.chars().next()) {
        Some(ch) => format!("`{ch}` (U+{:04X})", ch as u32),
        None => format!("byte 0x{byte:02X}"),
    };
    format!(
        "line {}: {shown} is a NON-ASCII character in a position that {}. This lexer's \
         identifier rules are deliberately ASCII-only, while Rust permits Unicode \
         identifiers (`XID_Start XID_Continue*`), so scanning past it could misclassify a \
         macro context — `macro_rules! café {{ () => {{ mod orphan; }} }}` would be read as \
         ordinary code and the `mod orphan;` in its body counted as a real declaration, \
         the silent FALSE PASS this walker exists to prevent (FAIL-CLOSED — see #1714).\n\
         Remedy: rename the identifier to ASCII, or teach this lexer real Unicode XID \
         identifier rules (tests/support/mod_reachability_lexer.rs) — which means Unicode \
         tables, not a wider byte range.",
        line_of(src, at),
        pos.describe()
    )
}

fn unrecognized(src: &str, start: usize, ident: &str, follower: u8) -> String {
    format!(
        "line {}: `{ident}{}` is an unrecognized literal/identifier prefix. Rust reserves \
         EVERY `ident\"…\"`, `ident'…'` and `ident#…` sequence, so this is either a literal \
         form newer than this lexer's table or not Rust at all. Scanning past it as \
         ordinary code would parse its contents as code — and a `mod orphan;` inside it \
         would be counted as a real declaration, the silent FALSE PASS this walker exists \
         to prevent (FAIL-CLOSED — see #1714).\n\
         Remedy: add the form to `PREFIXES` in tests/support/mod_reachability_lexer.rs.",
        line_of(src, start),
        follower as char
    )
}

// ---------------------------------------------------------------------------
// Scanners
// ---------------------------------------------------------------------------

/// Scan a `"…"` literal starting at the opening quote. Returns `(end, unescaped value)`.
///
/// The value is accumulated as **raw bytes** and decoded as UTF-8 once at the end. The
/// obvious `value.push(byte as char)` decodes UTF-8 one byte at a time, so the two bytes
/// of `ó` become two Latin-1 characters and `#[path = "módulo.rs"]` resolves to mojibake
/// that names no file — the byte-vs-char confusion behind CLAUDE.md's six-defect
/// path-normalisation family.
pub fn scan_string(b: &[u8], start: usize) -> Result<(usize, String), String> {
    let n = b.len();
    let mut j = start + 1;
    let mut value: Vec<u8> = Vec::new();
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
                        // `\xNN` names a CODE POINT here, not a raw byte: Rust rejects
                        // `\x80`+ in a string literal, and this scanner is shared with
                        // byte strings (`b"\xff"`), where accumulating the raw byte would
                        // make a legitimate literal invalid UTF-8 and fail the walk closed.
                        push_char(&mut value, byte as char);
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
                        push_char(&mut value, ch);
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
                push_char(&mut value, decoded);
                j += 2;
            }
            b'"' => {
                let decoded = String::from_utf8(value).map_err(|e| {
                    format!(
                        "string literal is not valid UTF-8 ({e}) — refusing to guess its \
                         value (FAIL-CLOSED)"
                    )
                })?;
                return Ok((j + 1, decoded));
            }
            other => {
                // Raw byte: a multi-byte character arrives here one byte per iteration and
                // must be reassembled, not transcoded per byte.
                value.push(other);
                j += 1;
            }
        }
    }
    Err("unterminated string literal (FAIL-CLOSED)".to_string())
}

/// Append `ch`'s UTF-8 encoding to a raw-byte literal accumulator.
fn push_char(out: &mut Vec<u8>, ch: char) {
    let mut buf = [0u8; 4];
    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
}

/// Scan `r#"…"#` at any hash count (also `br`/`cr` flavored). `prefix_end` is the byte
/// after the prefix identifier; `start` is the prefix's first byte, so the returned span
/// covers the prefix too.
///
/// The closing delimiter is `"` followed by **exactly** the opening hash count, which is
/// what makes an embedded `"` — the adversarial `cr#"left " mod orphan; " right"#` — part
/// of the value rather than a premature terminator.
pub fn scan_raw_string(
    b: &[u8],
    start: usize,
    prefix_end: usize,
) -> Result<(usize, String), String> {
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

/// `Ok(Some(end))` for a char literal, `Ok(None)` for a lifetime/label.
pub fn scan_char_or_lifetime(b: &[u8], start: usize) -> Result<Option<usize>, String> {
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

/// `true` when the bytes at `after_prefix` (just past an `r`/`br`/`cr` ident) open a raw
/// STRING literal: zero or more `#` followed by `"`. `r#type` therefore returns `false`
/// (that is a raw identifier, not a literal).
pub fn is_raw_string_prefix(b: &[u8], after_prefix: usize) -> bool {
    let mut j = after_prefix;
    while j < b.len() && b[j] == b'#' {
        j += 1;
    }
    b.get(j) == Some(&b'"')
}

// ---------------------------------------------------------------------------
// Shared byte helpers
// ---------------------------------------------------------------------------

/// ASCII-only by design — the boundary that makes that sound is [`refuse_non_ascii`],
/// which refuses (never skips) a non-ASCII byte in this position. See the module docs.
pub fn is_ident_start(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphabetic()
}

/// ASCII-only by design; see [`is_ident_start`] and [`refuse_non_ascii`].
pub fn is_ident_byte(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

/// One past the last identifier byte at `start`.
///
/// Stops at the first byte [`is_ident_byte`] rejects, which includes every non-ASCII byte
/// — so a caller that may see raw source must pair this with [`refuse_non_ascii`] at the
/// returned offset rather than treat the stop as a token boundary.
pub fn ident_end(b: &[u8], start: usize) -> usize {
    let mut j = start;
    while j < b.len() && is_ident_byte(b[j]) {
        j += 1;
    }
    j
}

pub fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// 1-based line of `offset` within `text`.
pub fn line_of(text: &str, offset: usize) -> usize {
    text.as_bytes()[..offset.min(text.len())]
        .iter()
        .filter(|c| **c == b'\n')
        .count()
        + 1
}
