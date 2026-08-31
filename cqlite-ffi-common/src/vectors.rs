//! Committed cross-binding test vectors (issue #1452).
//!
//! Unit tests inside this crate prove the shared functions are *correct*. They
//! do NOT prove a binding actually **calls** them — a binding could keep a
//! private copy and stay green, which is precisely the pre-#1452 state. These
//! tables close that gap: each binding exposes one internal, underscore-prefixed
//! test-support surface that renders **every** entry through its own
//! **production** conversion path, and each suite asserts the whole table.
//! Because both suites read the same committed data, a divergence between the
//! bindings — or a re-introduced local implementation in either — fails both.
//!
//! Only what a binding actually calls is `pub`: the tables, [`Input::bytes`],
//! [`vector_outcome`] and [`VectorOutcome`]. The comparison rules
//! (`check_outcome`), the `digest` reduction and the [`Expect`] accessors are
//! crate-internal — they have no binding caller, and this crate exports no
//! routine that lacks one. In particular the SHA-256 comparison is deliberately
//! NOT exported: each binding hashes with its own standard library, which is
//! what makes the hash a cross-implementation check rather than one more shared
//! function.
//!
//! The tables are ordinary `pub const` data with no feature gate: they are inert
//! and tiny, and gating them would make them unreachable from the bindings' own
//! test builds, which is the entire point.
//!
//! # How an expectation is written, and how a suite checks it
//!
//! Every entry's expectation is an [`Expect`] — either a rendered value or a
//! refusal message. A binding's test-support surface renders each entry through
//! its production path and reports the result as a [`VectorOutcome`] built by
//! [`vector_outcome`], so both bindings emit the same four fields and each suite
//! needs exactly two rules:
//!
//! * `kind == "value"` ⇒ `outcome == "ok"`, and the **full** [`VectorOutcome::rendered`]
//!   text satisfies the entry's exact check (below).
//! * `kind == "error"` ⇒ `outcome == "err"` and `expected` appears **verbatim
//!   inside** `actual`. Containment, not equality, only because each binding
//!   wraps the canonical message in its own typed-error envelope (Python's
//!   `CqliteError`, Node's prefixed `Error`) — the shared text itself is exact.
//!
//! # Every rendering is checked EXACTLY, and never by digit count
//!
//! A few DECIMAL magnitudes are multi-kilobyte by design (the positional
//! threshold and the refusal ceiling live up there), and their exact renderings
//! run to thousands of digits. Committing those as literals would be
//! unreadable, so `digest` collapses any digit run longer than
//! [`DIGEST_RUN_THRESHOLD`] to `{<length>}`.
//!
//! A digest is therefore a **diagnostic**, never the oracle: it pins the digit
//! COUNT and the surrounding form, so two bindings rendering *different digits
//! of the same length* would both satisfy it. The exact check is carried by the
//! expectation's own variant, and every value entry has one:
//!
//! * [`Expect::Value`] — the rendering is short enough to commit verbatim, so
//!   the check is `rendered == expected`, character for character.
//! * [`Expect::Digested`] — the rendering is multi-kilobyte, so the committed
//!   oracle is the **SHA-256 hex of the FULL rendering** and the check is
//!   `sha256(rendered) == expected_sha256`. The digest rides along for the
//!   failure message only.
//!
//! **Encoding, stated once for all three implementations:** the hash is SHA-256
//! over the **UTF-8 bytes** of the rendered string, lower-case hex. Every
//! rendering here is ASCII, so no implementation has an encoding choice to make,
//! and each side computes the hash with its own standard library — this crate's
//! test with `sha2`, Python with `hashlib.sha256(...).hexdigest()`, Node with
//! `crypto.createHash('sha256').update(..., 'utf8').digest('hex')`. Three
//! independent hash implementations over one committed hex string; nothing
//! shared but the data.

pub mod tables;

pub use tables::{DECIMAL_VECTORS, INET_VECTORS, JSON_NUMBER_VECTORS, VARINT_VECTORS};

/// Digit runs longer than this collapse to `{<length>}` in a `digest`.
///
/// 64 is above the widest rendering any VARINT or small DECIMAL entry produces
/// (a 17-byte varint reaches 41 digits), so every ordinary expectation is
/// committed literally and only the deliberately-huge magnitudes are summarised.
pub const DIGEST_RUN_THRESHOLD: usize = 64;

/// How an entry's input bytes are obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Input {
    /// Committed literal bytes.
    Literal(&'static [u8]),
    /// `len` copies of `byte`, for the multi-kilobyte boundary magnitudes whose
    /// literal form would be unreadable in source.
    Repeated {
        /// The repeated byte.
        byte: u8,
        /// How many copies.
        len: usize,
    },
}

impl Input {
    /// Materialise the entry's input bytes.
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Input::Literal(bytes) => bytes.to_vec(),
            Input::Repeated { byte, len } => vec![*byte; *len],
        }
    }
}

/// The single expected outcome of rendering an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// A rendering short enough to commit verbatim: the exact expected text,
    /// compared character for character. `digest` leaves such a rendering
    /// unchanged, which the crate's own vector test asserts.
    Value(&'static str),
    /// A multi-kilobyte rendering, committed as its `digest` for readability
    /// PLUS the SHA-256 hex of the FULL rendering, which is the exact check.
    ///
    /// A separate variant rather than an optional field, so it is structurally
    /// impossible to commit a collapsed digest with no exact oracle beside it.
    Digested {
        /// The rendering's `digest` — a human-readable diagnostic, never the
        /// oracle (it pins only the digit count and the surrounding form).
        digest: &'static str,
        /// Lower-case SHA-256 hex of the UTF-8 bytes of the full rendering.
        sha256: &'static str,
    },
    /// A refusal, carrying the exact error message both bindings must surface.
    Error(&'static str),
}

impl Expect {
    /// `"value"` for a rendering, `"error"` for a refusal — the field that tells
    /// a suite which of the two comparison rules to apply.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Expect::Value(_) | Expect::Digested { .. } => "value",
            Expect::Error(_) => "error",
        }
    }

    /// The expected text: the rendering (or its `digest`), or the refusal
    /// message. For a [`Expect::Digested`] entry this is the DIAGNOSTIC form —
    /// [`Expect::sha256`] carries that entry's exact oracle.
    pub(crate) const fn text(&self) -> &'static str {
        match self {
            Expect::Value(value) => value,
            Expect::Digested { digest, .. } => digest,
            Expect::Error(message) => message,
        }
    }

    /// The committed SHA-256 hex of the full rendering, for an entry whose
    /// expectation is too long to commit literally; `None` when [`text`] is
    /// itself the exact expectation.
    ///
    /// [`text`]: Expect::text
    pub(crate) const fn sha256(&self) -> Option<&'static str> {
        match self {
            Expect::Digested { sha256, .. } => Some(sha256),
            Expect::Value(_) | Expect::Error(_) => None,
        }
    }
}

/// One entry's expectation paired with what a binding's production path actually
/// produced — the record both bindings' test-support surfaces return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorOutcome {
    /// The entry's stable identifier.
    pub name: &'static str,
    /// `"value"` or `"error"`: which comparison rule applies.
    pub kind: &'static str,
    /// The committed expectation.
    pub expected: String,
    /// Lower-case SHA-256 hex of the UTF-8 bytes of the expected rendering, for
    /// an entry whose expectation is committed as a digest; `None` when
    /// `expected` is itself exact (every refusal, and every short rendering).
    pub expected_sha256: Option<&'static str>,
    /// `"ok"` if the production path rendered, `"err"` if it refused.
    pub outcome: &'static str,
    /// The rendering's `digest`, or the binding's full error message — the
    /// HUMAN-READABLE field, reported in failure messages. Never the oracle for
    /// a long rendering: a digest compares digit counts, not digits.
    pub actual: String,
    /// The FULL, un-digested rendering the production path produced, or `None`
    /// when it refused. This is what a suite hashes (or compares literally), so
    /// the exact digits are always what gets checked.
    pub rendered: Option<String>,
}

/// Build the [`VectorOutcome`] for one entry from what the production path
/// produced (`Ok(rendered)` / `Err(full error message)`).
///
/// Deliberately does NOT decide pass/fail: it reports, and the binding's suite
/// asserts. That keeps the oracle in committed data and the judgement in the
/// test suite.
pub fn vector_outcome(
    name: &'static str,
    expect: Expect,
    produced: Result<&str, &str>,
) -> VectorOutcome {
    let (outcome, actual, rendered) = match produced {
        Ok(text) => ("ok", digest(text), Some(text.to_string())),
        Err(message) => ("err", message.to_string(), None),
    };
    VectorOutcome {
        name,
        kind: expect.kind(),
        expected: expect.text().to_string(),
        expected_sha256: expect.sha256(),
        outcome,
        actual,
        rendered,
    }
}

/// Apply the two comparison rules to a reported outcome, returning `Err` with a
/// human-readable reason when it does not satisfy its expectation.
///
/// Used by this crate's own vector test. The binding suites deliberately apply
/// the same two rules in Python/JavaScript instead, so what a suite asserts is
/// visible in the suite — which is why this is test-only and unexported.
#[cfg(test)]
fn check_outcome(reported: &VectorOutcome) -> Result<(), String> {
    match reported.kind {
        "value" => {
            if reported.outcome != "ok" {
                return Err(format!(
                    "`{}`: expected a rendering but the path refused with: {}",
                    reported.name, reported.actual
                ));
            }
            let Some(rendered) = reported.rendered.as_deref() else {
                return Err(format!(
                    "`{}`: the path reported `ok` but carried no rendering, so \
                     nothing exact could be compared",
                    reported.name
                ));
            };
            // The digest first, because it is the readable half of a failure.
            if reported.actual != reported.expected {
                return Err(format!(
                    "`{}`: expected `{}`, got `{}`",
                    reported.name, reported.expected, reported.actual
                ));
            }
            // Then the EXACT check, on the full rendering. A digest match alone
            // would only mean the digit COUNT agreed.
            match reported.expected_sha256 {
                Some(expected_hex) => {
                    let actual_hex = sha256_hex(rendered);
                    if actual_hex != expected_hex {
                        return Err(format!(
                            "`{}`: the rendering digests to the expected `{}` but its \
                             digits differ — SHA-256 of the full rendering is `{}`, \
                             expected `{}`",
                            reported.name, reported.expected, actual_hex, expected_hex
                        ));
                    }
                }
                None => {
                    if rendered != reported.expected {
                        return Err(format!(
                            "`{}`: expected the exact rendering `{}`, got `{}`",
                            reported.name, reported.expected, rendered
                        ));
                    }
                }
            }
            Ok(())
        }
        "error" => {
            if reported.outcome != "err" {
                return Err(format!(
                    "`{}`: expected a refusal but the path rendered `{}`",
                    reported.name, reported.actual
                ));
            }
            if !reported.actual.contains(&reported.expected) {
                return Err(format!(
                    "`{}`: expected the message to contain `{}`, got `{}`",
                    reported.name, reported.expected, reported.actual
                ));
            }
            Ok(())
        }
        other => Err(format!(
            "`{}`: unknown expectation kind `{other}`",
            reported.name
        )),
    }
}

/// Lower-case SHA-256 hex of a string's UTF-8 bytes.
///
/// Test-only and unexported on purpose: the whole value of the hash is that each
/// side computes it with its own standard library (`sha2` here, `hashlib` in
/// Python, `crypto` in Node), so a shared helper would turn a
/// cross-implementation check back into one more shared function.
#[cfg(test)]
fn sha256_hex(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Collapse every maximal ASCII-digit run longer than [`DIGEST_RUN_THRESHOLD`]
/// to `{<length>}`, leaving everything else — signs, points, exponent markers
/// and short digit runs — untouched.
///
/// This keeps a multi-thousand-digit expectation readable while still pinning
/// the exact digit COUNT (full precision preservation) and the exact surrounding
/// form (positional vs exponent, sign, exponent value).
pub(crate) fn digest(rendered: &str) -> String {
    let mut out = String::with_capacity(rendered.len());
    let mut run = 0usize;
    // Push the digit run that just ended, collapsed if it is long enough.
    fn flush(out: &mut String, source: &str, end: usize, run: usize) {
        if run == 0 {
            return;
        }
        if run > DIGEST_RUN_THRESHOLD {
            out.push_str(&format!("{{{run}}}"));
        } else {
            out.push_str(&source[end - run..end]);
        }
    }
    for (index, ch) in rendered.char_indices() {
        if ch.is_ascii_digit() {
            run += 1;
        } else {
            flush(&mut out, rendered, index, run);
            run = 0;
            out.push(ch);
        }
    }
    flush(&mut out, rendered, rendered.len(), run);
    out
}

/// One DECIMAL cross-binding entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalVector {
    /// Stable identifier, reported by both suites on failure.
    pub name: &'static str,
    /// The cell's scale.
    pub scale: i32,
    /// The cell's unscaled two's-complement big-endian magnitude.
    pub unscaled: Input,
    /// The single expected outcome.
    pub expect: Expect,
}

/// One VARINT cross-binding entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarintVector {
    /// Stable identifier, reported by both suites on failure.
    pub name: &'static str,
    /// The cell's big-endian two's-complement payload.
    pub bytes: Input,
    /// The single expected outcome — the integer's canonical decimal string.
    pub expect: Expect,
}

/// The host-language SHAPE a JSON number must arrive as.
///
/// Deliberately coarse — `Integer` / `Float` — because the two bindings' exact
/// types differ by design (issue #3505 AC5): Python renders both integer classes
/// as `int`, while Node uses `number` inside `i32` range and `BigInt` beyond it.
/// Each suite maps this to its own rule, and the shared table only commits the
/// property both must satisfy: an integer literal must NOT arrive as a float.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonHostKind {
    /// A JSON INTEGER literal. Must arrive as an exact integer, never a float:
    /// this is the property #3505 was violating.
    Integer,
    /// A JSON FLOAT literal. Arrives as the host's double.
    Float,
}

impl JsonHostKind {
    /// The stable wire name both bindings' reports carry (`"integer"`/`"float"`).
    pub fn name(&self) -> &'static str {
        match self {
            JsonHostKind::Integer => "integer",
            JsonHostKind::Float => "float",
        }
    }
}

/// One JSON-number cross-binding entry (issue #3505).
///
/// Unlike the other three tables the input is TEXT, not bytes: the whole point
/// of #3505 is that the JSON **lexical form** decides the class (an integer
/// literal stays exact; a float literal is an `f64` by construction), so a
/// vector that started from bytes could not express the distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonNumberVector {
    /// Stable identifier, reported by both suites on failure.
    pub name: &'static str,
    /// The JSON number literal, verbatim, as it would appear in a `Value::Json`
    /// payload.
    pub json_text: &'static str,
    /// The host shape the value must arrive as.
    pub host_kind: JsonHostKind,
    /// The single expected outcome — the value's canonical decimal string as
    /// BOTH hosts render it (see the table's docs for why the table is
    /// restricted to literals the two hosts stringify identically).
    pub expect: Expect,
}

/// One INET cross-binding entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InetVector {
    /// Stable identifier, reported by both suites on failure.
    pub name: &'static str,
    /// The cell's packed address bytes.
    pub bytes: Input,
    /// The single expected outcome — the address text, or the malformed-length
    /// error message.
    pub expect: Expect,
}

// Tests live in a sibling file to keep this module small (#1116).
#[cfg(test)]
#[path = "vectors_tests.rs"]
mod tests;
