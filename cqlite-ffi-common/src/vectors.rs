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
//! routine that lacks one.
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
//! * `kind == "value"` ⇒ `outcome == "ok"` and `actual == expected`, exactly.
//! * `kind == "error"` ⇒ `outcome == "err"` and `expected` appears **verbatim
//!   inside** `actual`. Containment, not equality, only because each binding
//!   wraps the canonical message in its own typed-error envelope (Python's
//!   `CqliteError`, Node's prefixed `Error`) — the shared text itself is exact.
//!
//! A few DECIMAL magnitudes are multi-kilobyte by design (the positional
//! threshold and the refusal ceiling live up there), and their exact renderings
//! run to thousands of digits. Committing those as literals would be
//! unreadable, so `digest` collapses any digit run longer than
//! [`DIGEST_RUN_THRESHOLD`] to `{<length>}`. Short renderings — every VARINT and
//! INET entry, and every small DECIMAL one — digest to themselves and so are
//! committed verbatim.

pub mod tables;

pub use tables::{DECIMAL_VECTORS, INET_VECTORS, VARINT_VECTORS};

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
    /// The rendering, as a `digest` (identical to the rendering itself unless
    /// it contains a digit run longer than [`DIGEST_RUN_THRESHOLD`]).
    Value(&'static str),
    /// A refusal, carrying the exact error message both bindings must surface.
    Error(&'static str),
}

impl Expect {
    /// `"value"` for a rendering, `"error"` for a refusal — the field that tells
    /// a suite which of the two comparison rules to apply.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Expect::Value(_) => "value",
            Expect::Error(_) => "error",
        }
    }

    /// The expected text: the rendering's `digest`, or the refusal message.
    pub(crate) const fn text(&self) -> &'static str {
        match self {
            Expect::Value(value) => value,
            Expect::Error(message) => message,
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
    /// `"ok"` if the production path rendered, `"err"` if it refused.
    pub outcome: &'static str,
    /// The rendering's `digest`, or the binding's full error message.
    pub actual: String,
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
    let (outcome, actual) = match produced {
        Ok(rendered) => ("ok", digest(rendered)),
        Err(message) => ("err", message.to_string()),
    };
    VectorOutcome {
        name,
        kind: expect.kind(),
        expected: expect.text().to_string(),
        outcome,
        actual,
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
            if reported.actual != reported.expected {
                return Err(format!(
                    "`{}`: expected `{}`, got `{}`",
                    reported.name, reported.expected, reported.actual
                ));
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
