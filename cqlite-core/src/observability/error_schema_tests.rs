//! `error_schema` invariant + code↔doc completeness tests.
//!
//! Split out of `error_schema.rs` so that file stays pure source inside the
//! campsite-rule target (#1116); logically the `tests` submodule of that module.
//!
//! The completeness tests here close issue #1705 (AI5, epic #1686 "observability
//! honesty"): the module-doc taxonomy table in `error_schema.rs` is the operator-
//! and binding-facing contract for what `classify()` does, and NOTHING made the
//! table track the code. It had drifted — six `Error` variants routed by
//! `classify()` were absent from the table (`UnsupportedVersion`,
//! `UnsupportedCommitLogVersion`, `CorruptCommitLogFrame`, `ResultTooLarge`,
//! `ForcedReadPathUnavailable`, `InvalidReadPath`).
//!
//! Both sides are derived PROGRAMMATICALLY, so there is no second hand-maintained
//! list to drift. This taxonomy is TELEMETRY-ONLY: the language bindings read
//! `cqlite_ffi_common::error_contract` (issue #1451), which mirrors the distinct
//! `Error::category()` enum, not `classify()`.
//!
//! # Where the code side's authority comes from (roborev B1 on this issue)
//!
//! An earlier version derived the code side from a source scrape of `classify()`
//! alone. That is the classic vacuous-guard shape CLAUDE.md names: a variant
//! absorbed by a catch-all arm is absent from BOTH parsed sides, so the advertised
//! set-equality passes while the taxonomy was never updated. The code side is now
//! THREE mutually-checking oracles, the first of which is the compiler:
//!
//! 1. **`classify()` has no catch-all arm** (`classify_has_no_catch_all_arm`), and
//!    it matches on `&Error`. Rust's exhaustiveness check therefore REFUSES TO
//!    COMPILE until a newly-added `Error` variant is named in an arm explicitly —
//!    the compiler, not a scrape, is what makes the enumeration complete.
//! 2. **The `Error` enum declaration** is parsed structurally
//!    ([`declared_error_variants`]) and asserted equal to the arm set, so a parser
//!    that stops seeing arms fails loudly instead of shrinking the guard.
//! 3. **Each compiled variant has a constructed sample** whose variant name is read
//!    back from its derived `Debug` output, and whose category is MEASURED by
//!    calling `classify()` on the value — an affirmative runtime measurement.

use super::*;
use crate::error::Error;
use std::collections::BTreeMap;

/// The one copy of `error_schema.rs`'s source both parsers read.
fn error_schema_src() -> &'static str {
    include_str!("error_schema.rs")
}

/// Every ``backticked`` token in `s`, in order.
fn backticked(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(open) = s[i..].find('`') {
        let open = i + open;
        let Some(close) = s[open + 1..].find('`') else {
            break;
        };
        let close = open + 1 + close;
        let token = s[open + 1..close].trim();
        if !token.is_empty() {
            out.push(token);
        }
        i = close + 1;
    }
    out
}

/// Split a `Maps from` cell into its NON-parenthetical text and its parenthetical
/// commentary, or `Err` on an unbalanced parenthesis.
///
/// Parentheses are the table's one sanctioned place for commentary (`Cancelled`
/// uses it), and a comma inside one — "…a cooperative abort, never `Io`" — must not
/// be read as an item separator. Splitting first is therefore what lets the item
/// rule below be strict.
fn split_commentary(cell: &str) -> Result<(String, String), String> {
    let mut body = String::new();
    let mut commentary = String::new();
    let mut depth = 0usize;
    for c in cell.chars() {
        match c {
            '(' => {
                depth += 1;
                commentary.push(c);
            }
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| format!("unbalanced `)` in `Maps from` cell: {cell}"))?;
                commentary.push(c);
            }
            _ if depth > 0 => commentary.push(c),
            _ => body.push(c),
        }
    }
    if depth != 0 {
        return Err(format!("unclosed `(` in `Maps from` cell: {cell}"));
    }
    Ok((body, commentary))
}

/// Behavioural claims the table may NOT make, in prose or in a parenthetical.
///
/// `classify()` has no catch-all arm (`classify_has_no_catch_all_arm`), so every
/// one of these is FALSE by construction: a new `Error` variant does not land in
/// `Other`, it fails to compile until categorised. The `Other` row carried
/// "and any future variant (catch-all)" for exactly as long as nothing checked
/// (issue #1705, F5) — the completeness comparison could not see it because the
/// phrase names no backticked variant and was silently discarded as prose.
const FORBIDDEN_TABLE_CLAIMS: [&str; 5] = [
    "catch-all",
    "catchall",
    "future variant",
    "other variant",
    "all remaining",
];

/// The `Error` variants a `Maps from` cell documents — parsed FAIL-CLOSED.
///
/// Rules, in order:
///
/// 1. Parenthetical commentary is separated out ([`split_commentary`]).
/// 2. Every remaining comma-separated item must be EXACTLY one backticked
///    identifier. Unbacketed prose is an ERROR, not something to skip: skipping is
///    what let the `Other` row promise a catch-all that does not exist.
/// 3. Neither the items nor the commentary may assert one of the
///    [`FORBIDDEN_TABLE_CLAIMS`].
///
/// Returns `Err` (rather than panicking) so
/// [`the_maps_from_parser_rejects_prose_that_smuggles_a_behavioural_claim`] can
/// exercise THIS parser — the one the table guard calls — on synthetic cells.
fn parse_maps_from(cell: &str) -> Result<Vec<String>, String> {
    let (body, commentary) = split_commentary(cell)?;
    let lowered = format!("{body} {commentary}").to_ascii_lowercase();
    for claim in FORBIDDEN_TABLE_CLAIMS {
        if lowered.contains(claim) {
            return Err(format!(
                "`Maps from` cell claims {claim:?}, which classify() does not do — it \
                 has no catch-all arm, so an uncategorised variant is a compile error, \
                 not an `Other`: {cell}"
            ));
        }
    }
    let mut out = Vec::new();
    for item in body.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue; // a wrapped row's trailing comma
        }
        let tokens = backticked(item);
        let ident = match tokens.first() {
            Some(first) if tokens.len() == 1 && item == format!("`{first}`") => *first,
            _ => {
                return Err(format!(
                    "`Maps from` item {item:?} is not a backticked `Error` variant name \
                     (put commentary in parentheses or in the prose below the table): {cell}"
                ))
            }
        };
        if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || !ident.starts_with(|c: char| c.is_ascii_uppercase())
        {
            return Err(format!(
                "`Maps from` item {ident:?} is not an `Error` variant identifier: {cell}"
            ));
        }
        out.push(ident.to_string());
    }
    Ok(out)
}

/// Parse the `# Taxonomy` markdown table out of `error_schema.rs`'s module doc.
///
/// Returns `(category variant, as_str label, mapped-from `Error` variants)` rows.
/// A continuation row (blank first cell — rustfmt-friendly wrapping of a long
/// variant list, as `Other` uses) is folded into the row above it, so a wrapped
/// cell can never silently drop variants from the documented set.
fn documented_taxonomy() -> Vec<(String, String, Vec<String>)> {
    let mut rows: Vec<(String, String, Vec<String>)> = Vec::new();
    let mut past_separator = false;
    for line in error_schema_src().lines() {
        let Some(doc) = line.strip_prefix("//!") else {
            continue;
        };
        let doc = doc.trim();
        if !doc.starts_with('|') {
            // The table is contiguous; once we have rows, the first non-row doc
            // line ends it (later sections must not be scanned for cells).
            if past_separator && !rows.is_empty() {
                break;
            }
            continue;
        }
        if doc.contains("|---") {
            past_separator = true;
            continue;
        }
        if !past_separator {
            // Header row.
            continue;
        }
        let cells: Vec<&str> = doc.trim_matches('|').split('|').collect();
        assert_eq!(
            cells.len(),
            3,
            "taxonomy table row must have exactly 3 columns: {doc}"
        );
        let variant = backticked(cells[0]);
        let label = backticked(cells[1]);
        let mapped: Vec<String> = parse_maps_from(cells[2])
            .unwrap_or_else(|why| panic!("taxonomy table row is not parseable: {why}"));
        if variant.is_empty() {
            // Continuation of the previous row.
            let last = rows
                .last_mut()
                .expect("a continuation row must follow a row");
            last.2.extend(mapped);
            continue;
        }
        assert_eq!(
            variant.len(),
            1,
            "column 1 must name exactly one category variant: {doc}"
        );
        assert_eq!(
            label.len(),
            1,
            "column 2 must name exactly one as_str() label: {doc}"
        );
        rows.push((variant[0].to_string(), label[0].to_string(), mapped));
    }
    assert!(
        rows.len() > 1,
        "the taxonomy table must have been found and parsed"
    );
    rows
}

/// The `crate::error` source the `Error`-enum declaration parser reads.
fn error_src() -> &'static str {
    include_str!("../error.rs")
}

/// Whether this build compiles the `#[cfg(target_arch = "wasm32")]` `Error`
/// variants. On every other target such a variant does not exist, so it can be
/// neither constructed nor classified at runtime — see [`documented_map`].
const WASM_VARIANTS_COMPILED: bool = cfg!(target_arch = "wasm32");

/// Every variant the `Error` enum DECLARES → is it `wasm32`-gated.
///
/// **Why a source parse at all, and what it cannot see.** Rust has no reflection
/// over enum variants, so the declared set has to be read from the one construct
/// that holds it: the `pub enum Error { … }` block in `error.rs`. This parser is
/// deliberately NOT the sole oracle — it exists to catch a scrape regression in
/// [`classify_arms`] (which the compiler keeps complete). It recognises a variant
/// as a line at the enum's own 4-space indent opening with an ASCII-uppercase
/// identifier followed by `(`, `{` or `,`; it therefore cannot see a
/// macro-generated variant or one written at another indentation. Either would
/// disagree with the compiler-audited arm set and FAIL
/// `declared_error_variants_equal_classify_arms` — the failure mode is a red test,
/// never a silent pass.
fn declared_error_variants() -> BTreeMap<String, bool> {
    const HEAD: &str = "pub enum Error {";
    let src = error_src();
    let start = src
        .find(HEAD)
        .expect("error.rs must declare `pub enum Error`");
    let body = &src[start + HEAD.len()..];
    let mut out = BTreeMap::new();
    let mut wasm_gated = false;
    for line in body.lines() {
        if line == "}" {
            break; // end of the enum declaration
        }
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        if indent != 4 {
            continue; // struct-variant fields / wrapped attribute continuations
        }
        if trimmed.starts_with("#[cfg(target_arch = \"wasm32\")]") {
            wasm_gated = true;
            continue;
        }
        if !trimmed.starts_with(|c: char| c.is_ascii_uppercase()) {
            continue; // doc comment, attribute, or a wrapped `)]`
        }
        let ident: String = trimmed
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let rest = trimmed[ident.len()..].trim_start();
        if !(rest.starts_with('(') || rest.starts_with('{') || rest.starts_with(',')) {
            continue;
        }
        out.insert(ident, wasm_gated);
        wasm_gated = false;
    }
    assert!(
        out.len() > 30,
        "the Error enum declaration must have been parsed; got {out:?}"
    );
    out
}

/// Parse a `classify()`-shaped match body into `Error` variant →
/// (`ObsErrorCategory` variant, is-`wasm32`-gated).
///
/// Takes the body as an argument so [`the_classify_arm_parser_rejects_a_catch_all`]
/// can feed it synthetic bodies — the parser under test is THIS one, not a copy of
/// it (CLAUDE.md: "a port is a second implementation").
///
/// **Fail-closed on any arm alternative that is not an explicit `Error::` pattern.**
/// That single rule is what makes the parsed set COMPLETE rather than merely
/// non-empty: `classify()` matches on `&Error`, so if every alternative must name a
/// variant, Rust's exhaustiveness check guarantees every variant is named. A
/// wildcard (`_ =>`), a named binding (`other =>`), or `Error::X | _ =>` all return
/// `Err` here instead of quietly absorbing future variants.
fn parse_classify_arms(body: &str) -> Result<BTreeMap<String, (String, bool)>, String> {
    const ARM: &str = "=> ObsErrorCategory::";
    const CFG_WASM: &str = "#[cfg(target_arch = \"wasm32\")]";
    let mut out: BTreeMap<String, (String, bool)> = BTreeMap::new();
    // Start after the `match … {` opener: the function signature ahead of it is not
    // an arm pattern, and feeding it to the per-alternative rule below would reject
    // every well-formed body.
    let mut prev_end = body
        .find("match ")
        .and_then(|i| body[i..].find('{').map(|j| i + j + 1))
        .ok_or_else(|| "classify()'s body must contain a `match … {` opener".to_string())?;
    for (idx, _) in body.match_indices(ARM) {
        if idx < prev_end {
            continue;
        }
        let patterns = &body[prev_end..idx];
        let after = &body[idx + ARM.len()..];
        let category: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if category.is_empty() {
            return Err("an arm must name an ObsErrorCategory variant".to_string());
        }
        prev_end = idx + ARM.len() + category.len();

        // Drop `//` comments (arms carry explanatory prose) and the previous arm's
        // trailing comma, then judge each `|` alternative on its own.
        let cleaned: String = patterns
            .lines()
            .map(|l| l.split("//").next().unwrap_or("").trim())
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let cleaned = cleaned.trim().trim_start_matches(',').trim();
        for alt in cleaned.split('|') {
            let alt = alt.trim();
            if alt.is_empty() {
                continue;
            }
            let (gated, alt) = match alt.strip_prefix(CFG_WASM) {
                Some(rest) => (true, rest.trim()),
                None => (false, alt),
            };
            let Some(rest) = alt.strip_prefix("Error::") else {
                return Err(format!(
                    "classify() arm alternative {alt:?} is not an explicit `Error::` \
                     pattern — a wildcard or named binding would absorb future \
                     variants silently, which is exactly the drift this guard exists \
                     to catch (issue #1705)"
                ));
            };
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.is_empty() {
                return Err(format!("could not read a variant name from {alt:?}"));
            }
            if let Some(prior) = out.insert(ident.clone(), (category.clone(), gated)) {
                if prior.0 != category {
                    return Err(format!(
                        "Error::{ident} appears in two classify() arms ({} and {category})",
                        prior.0
                    ));
                }
            }
        }
    }
    if out.len() <= 20 {
        return Err(format!("classify() must have been parsed; got {out:?}"));
    }
    Ok(out)
}

/// The real `classify()` body, extracted from `error_schema.rs`.
fn classify_body() -> &'static str {
    let src = error_schema_src();
    let start = src
        .find("fn classify(")
        .expect("classify() must exist in error_schema.rs");
    let body = &src[start..];
    let end = body
        .find("\n}\n")
        .expect("classify() must be terminated by a column-0 closing brace");
    &body[..end]
}

/// `classify()`'s arms — COMPLETE because the compiler says so (see the module doc).
fn classify_arms() -> BTreeMap<String, (String, bool)> {
    match parse_classify_arms(classify_body()) {
        Ok(map) => map,
        Err(why) => panic!("classify() is not exhaustively enumerable: {why}"),
    }
}

/// One constructed `Error` value per variant THIS TARGET compiles.
///
/// No variant name is written beside a value: [`debug_variant_name`] reads it back
/// out of the derived `Debug` output, so a sample cannot mislabel itself, and the
/// category is obtained by CALLING `classify()` on the value
/// ([`measured_categories`]) rather than by reading source text.
fn error_samples() -> Vec<Error> {
    let mut samples = vec![
        Error::Io(std::io::Error::other("x")),
        Error::Serialization {
            message: "m".into(),
            source: None,
        },
        Error::Corruption("c".into()),
        // Issue #3721: a per-column decode failure, wrapping its underlying cause.
        Error::column_decode("col", "int", 0, Error::Corruption("c".into())),
        Error::Schema("s".into()),
        Error::CqlParse("q".into()),
        Error::InvalidFormat("f".into()),
        Error::UnsupportedFormat("f".into()),
        Error::UnsupportedVersion {
            version: "ma".into(),
            floor: "na".into(),
        },
        Error::UnsupportedCommitLogVersion {
            version: 5,
            floor: 6,
            ceiling: 7,
        },
        Error::CorruptCommitLogFrame("f".into()),
        Error::Timeout("t".into()),
        Error::InvalidPath("p".into()),
        Error::InvalidState("s".into()),
        Error::QueryExecution("q".into()),
        Error::QueryTimeout {
            operation: "query.execute".into(),
            elapsed: std::time::Duration::from_millis(1500),
            limit: std::time::Duration::from_millis(1000),
        },
        Error::ResultTooLarge {
            budget_bytes: 1,
            estimated_bytes: 2,
            rows: 3,
        },
        Error::InvalidReadPath {
            value: "nope".into(),
        },
        Error::ForcedReadPathUnavailable {
            forced: "point",
            reason: "r".into(),
        },
        Error::TypeConversion("t".into()),
        Error::Configuration("c".into()),
        Error::Storage("s".into()),
        Error::Memory("m".into()),
        Error::Concurrency("c".into()),
        Error::WriteDirLocked { path: "/d".into() },
        Error::NotFound("n".into()),
        Error::Table("t".into()),
        Error::AlreadyExists("a".into()),
        Error::InvalidOperation("o".into()),
        Error::ConstraintViolation("v".into()),
        Error::Transaction("t".into()),
        Error::Index("i".into()),
        Error::Compaction("c".into()),
        Error::Internal("i".into()),
        Error::Parse("p".into()),
        Error::InvalidInput("i".into()),
        Error::UnsupportedQuery("q".into()),
        Error::Cancelled,
    ];
    #[cfg(target_arch = "wasm32")]
    samples.push(Error::Wasm("w".into()));
    samples.sort_by_key(debug_variant_name);
    samples
}

/// The variant name of `err`, read from its derived `Debug` output.
///
/// `#[derive(Debug)]` on an enum prints the variant identifier first (`Cancelled`,
/// `Io(..)`, `Serialization { .. }`), so the leading identifier IS the variant name
/// — obtained from the VALUE, never from a hand-written literal beside it.
fn debug_variant_name(err: &Error) -> String {
    format!("{err:?}")
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

/// `Error` variant → category, MEASURED by calling `classify()` on a real value of
/// each variant this target compiles.
fn measured_categories() -> BTreeMap<String, String> {
    error_samples()
        .iter()
        .map(|e| (debug_variant_name(e), format!("{:?}", classify(e))))
        .collect()
}

/// The variant → category map the doc table is compared against.
///
/// Runtime-measured for every variant this target compiles. A `wasm32`-gated
/// variant cannot be constructed here (it does not exist in this build), so its
/// category is taken from the compiler-audited `classify()` arm text and the fact is
/// stated rather than hidden: on a `wasm32` build it becomes runtime-measured like
/// everything else. The doc table lists it unconditionally because the table is a
/// human-facing description of the enum, not of one target.
fn actual_map() -> BTreeMap<String, String> {
    let mut out = measured_categories();
    if !WASM_VARIANTS_COMPILED {
        for (variant, (category, gated)) in classify_arms() {
            if gated {
                out.insert(variant, category);
            }
        }
    }
    out
}

/// The variant → category map the module-doc taxonomy table declares.
fn documented_map() -> BTreeMap<String, String> {
    let mut documented = BTreeMap::new();
    for (category, _, mapped) in documented_taxonomy() {
        for variant in mapped {
            let prior = documented.insert(variant.clone(), category.clone());
            assert!(
                prior.is_none(),
                "Error::{variant} is documented under two categories \
                 ({prior:?} and {category})"
            );
        }
    }
    documented
}

#[test]
fn as_str_is_lowercase_and_unique() {
    let mut seen = std::collections::HashSet::new();
    for c in ObsErrorCategory::ALL {
        let s = c.as_str();
        assert_eq!(s, s.to_ascii_lowercase());
        assert!(seen.insert(s), "duplicate category label {s}");
    }
    assert_eq!(seen.len(), ObsErrorCategory::ALL.len());
}

#[test]
fn display_matches_as_str() {
    for c in ObsErrorCategory::ALL {
        assert_eq!(c.to_string(), c.as_str());
    }
}

#[test]
fn documented_categories_equal_the_error_category_enum() {
    // Issue #1705: category-level code↔doc set equality. The table's column 1
    // must be exactly `ObsErrorCategory::ALL`, in the same order, with column 2
    // matching `as_str()` — so a new telemetry category cannot ship undocumented
    // and a retired one cannot linger in the operator-facing table.
    let documented: Vec<(String, String)> = documented_taxonomy()
        .into_iter()
        .map(|(v, l, _)| (v, l))
        .collect();
    let actual: Vec<(String, String)> = ObsErrorCategory::ALL
        .iter()
        .map(|c| (format!("{c:?}"), c.as_str().to_string()))
        .collect();
    assert_eq!(
        documented, actual,
        "the error_schema taxonomy table's categories must equal ObsErrorCategory::ALL \
         (variant + as_str label, same order)"
    );
}

#[test]
fn classify_has_no_catch_all_arm() {
    // Issue #1705 (roborev B1) — the LOAD-BEARING pin. `classify()` matches on
    // `&Error` with every arm alternative an explicit `Error::<Variant>` pattern,
    // so Rust's exhaustiveness check refuses to compile until a newly-added variant
    // is categorised BY HAND. Re-adding `_ => ObsErrorCategory::Other` (or a named
    // binding arm) would restore exactly the hole this issue closes: a new variant
    // silently absorbed into `Other`, absent from the doc table, and invisible to
    // every parsed comparison below. So the absence of a catch-all is asserted
    // directly rather than assumed.
    if let Err(why) = parse_classify_arms(classify_body()) {
        panic!(
            "classify() must stay exhaustively enumerable (no catch-all arm): {why}\n\
             The taxonomy guard's completeness comes from the COMPILER refusing to \
             build until each new Error variant is named in an arm."
        );
    }
}

#[test]
fn the_classify_arm_parser_rejects_a_catch_all() {
    // The pin above is only worth as much as the parser behind it, so prove the
    // rejection on synthetic bodies — asserting THIS parser, the one
    // `classify_has_no_catch_all_arm` calls, not a copy of it.
    let mut ok = String::from("fn classify(err: &Error) -> ObsErrorCategory {\n    match err {\n");
    for n in 0..21 {
        ok.push_str(&format!(
            "        Error::V{n}(_) => ObsErrorCategory::Other,\n"
        ));
    }
    let parsed = parse_classify_arms(&ok).expect("an all-explicit body must parse");
    assert_eq!(parsed.len(), 21);
    assert_eq!(parsed["V7"], ("Other".to_string(), false));

    for bad in [
        // a bare wildcard
        format!("{ok}        _ => ObsErrorCategory::Other,\n"),
        // a wildcard folded into an otherwise explicit arm
        format!("{ok}        Error::VX(_) | _ => ObsErrorCategory::Other,\n"),
        // a NAMED binding, which is a catch-all that contains no `_` at all
        format!("{ok}        other => ObsErrorCategory::Other,\n"),
    ] {
        let err = parse_classify_arms(&bad)
            .expect_err("a catch-all arm must be rejected, not silently parsed");
        assert!(
            err.contains("not an explicit `Error::` pattern"),
            "unexpected rejection reason: {err}"
        );
    }
}

#[test]
fn declared_error_variants_equal_classify_arms() {
    // Issue #1705 (roborev B1): the two independent code-side derivations must
    // agree. `classify_arms()` is kept complete by the compiler; this asserts the
    // structural `Error`-enum parse sees the same set, so a parser regression on
    // either side is a RED test rather than a quietly shrinking guard.
    let declared: Vec<String> = declared_error_variants().into_keys().collect();
    let arms: Vec<String> = classify_arms().into_keys().collect();
    assert_eq!(
        declared, arms,
        "the Error enum declaration and classify()'s match arms must name the same \
         variants (classify() is exhaustive, so any difference is a parse bug in \
         one of the two derivations)"
    );
}

#[test]
fn every_compiled_error_variant_has_a_constructed_sample() {
    // Issue #1705 (roborev B1): the categories compared against the doc table are
    // MEASURED by calling `classify()` on a real value of each variant, so every
    // variant this target compiles must have a sample. Adding an `Error` variant
    // therefore fails HERE (add a sample) as well as at the compiler (categorise it
    // in `classify()`) — no path leaves the taxonomy table unchecked.
    let expected: Vec<String> = declared_error_variants()
        .into_iter()
        .filter(|(_, wasm_gated)| WASM_VARIANTS_COMPILED || !wasm_gated)
        .map(|(v, _)| v)
        .collect();
    let sampled: Vec<String> = error_samples().iter().map(debug_variant_name).collect();
    assert_eq!(
        sampled, expected,
        "error_samples() must hold exactly one constructed value per Error variant \
         this target compiles (names are read back from each value's Debug output)"
    );
}

#[test]
fn the_maps_from_parser_rejects_prose_that_smuggles_a_behavioural_claim() {
    // Issue #1705 (F5) — the RED test for this file's OWN bug class. The `Other`
    // row read "`Internal`, `Wasm`, and any future variant (catch-all)" while the
    // prose beneath it asserted the table was EXACT and `classify()` had no
    // catch-all arm. All three could not be true, and the completeness guard was
    // blind to it: the phrase names no backticked variant, so the old extractor
    // discarded it as commentary. A guard that cannot see this defect will not see
    // the next one, so unbacketed items and catch-all claims are now errors.
    let stale = "`Internal`, `Wasm`, and any future variant (catch-all)";
    let why = parse_maps_from(stale).expect_err("the stale `Other` cell must be rejected");
    assert!(
        why.contains("catch-all"),
        "the rejection must name the false claim: {why}"
    );

    // Prose with no claim vocabulary is still rejected: an item that is not a
    // backticked variant name cannot be compared against classify() at all.
    let prose = parse_maps_from("`Internal`, plus whatever else turns up")
        .expect_err("an unbacketed item must be rejected");
    assert!(
        prose.contains("is not a backticked `Error` variant name"),
        "unexpected rejection reason: {prose}"
    );
    // Backticked-but-not-a-variant prose (a lowercase token, or a variant with
    // trailing commentary outside parentheses) is rejected too.
    assert!(parse_maps_from("`Internal`, `see below`").is_err());
    assert!(parse_maps_from("`Internal` and any others").is_err());

    // What the table legitimately says still parses: parenthetical commentary,
    // including a comma and a contrast mention inside it, and a cfg note.
    assert_eq!(
        parse_maps_from("`Cancelled` (issue #2264 — a cooperative abort, never `Io`)")
            .expect("parenthetical commentary is allowed"),
        vec!["Cancelled".to_string()],
    );
    assert_eq!(
        parse_maps_from("`Internal`, `Wasm` (`wasm32` builds only)")
            .expect("a cfg note is allowed"),
        vec!["Internal".to_string(), "Wasm".to_string()],
    );
    // An unbalanced parenthesis fails closed rather than swallowing the rest.
    assert!(parse_maps_from("`Internal` (oops").is_err());
    assert!(parse_maps_from("`Internal`)").is_err());

    // And the REAL table must be free of both defects — i.e. this parser is what
    // the shipped module doc is held to, not just synthetic strings.
    let rows = documented_taxonomy();
    let other = rows
        .iter()
        .find(|(category, _, _)| category == "Other")
        .expect("the taxonomy table must have an `Other` row");
    assert!(
        other.2.contains(&"Internal".to_string()),
        "the `Other` row must still document its variants: {other:?}"
    );
}

#[test]
fn every_error_variant_classify_routes_is_documented_in_the_taxonomy_table() {
    // Issue #1705 (AI5) — the RED test. `classify()`'s module-doc table is what
    // operators read for telemetry (the bindings read `cqlite_ffi_common::error_contract`
    // instead — see the module doc). Assert
    // variant→category set equality in BOTH directions:
    //
    //   * a variant `classify()` routes but the table omits  → undocumented
    //     behaviour (drifted on `main`: `UnsupportedVersion` and five others).
    //   * a variant the table names but `classify()` does not → a phantom row
    //     describing behaviour that does not exist.
    //
    // Mapping equality, not just membership: a variant listed under the WRONG
    // category row is also caught. The code side is the RUNTIME-measured map
    // (`classify()` called on a constructed value per variant), not a scrape of
    // the function's text — see the module doc's oracle note (roborev B1).
    let classified = actual_map();
    let documented = documented_map();

    let undocumented: Vec<String> = classified
        .iter()
        .filter(|(v, _)| !documented.contains_key(v.as_str()))
        .map(|(v, c)| format!("Error::{v} -> {c}"))
        .collect();
    assert!(
        undocumented.is_empty(),
        "classify() routes Error variants that the error_schema taxonomy table \
         does NOT document (add them to the table's `Maps from` column): {undocumented:?}"
    );

    let phantom: Vec<String> = documented
        .iter()
        .filter(|(v, _)| !classified.contains_key(v.as_str()))
        .map(|(v, c)| format!("Error::{v} (documented under {c})"))
        .collect();
    assert!(
        phantom.is_empty(),
        "the error_schema taxonomy table documents Error variants that classify() \
         does not route (remove the phantom rows): {phantom:?}"
    );

    assert_eq!(
        classified, documented,
        "every Error variant must be documented under the SAME category classify() \
         actually assigns it"
    );
}

/// The INDEPENDENT, hand-written expectation set: one constructed `Error` value per
/// variant paired with the category it MUST classify as.
///
/// These are written by hand ON PURPOSE (issue #1038, tightened by #1705 roborev
/// F10). Every other guard in this file compares two derivations of the same source
/// — `classify()`'s arms against the module-doc table — and consistency cannot
/// detect a variant filed under the WRONG category, because both sides agree. This
/// list is the only oracle for the CORRECT category, so it is the one place where a
/// human judgement is recorded rather than a comparison.
///
/// A LIST rather than a sequence of asserts so
/// [`the_independent_category_test_covers_every_variant_the_taxonomy_documents`] can
/// MEASURE its coverage (each value's variant name is read back from `Debug`)
/// instead of scraping the test's text. That is what stops this oracle silently
/// lagging behind the enum again — the F10 defect was exactly that: the six variants
/// this issue documents were absent here while every consistency guard stayed green.
fn independent_expectations() -> Vec<(Error, ObsErrorCategory)> {
    use ObsErrorCategory::*;
    let mut out = vec![
        (Error::from(std::io::Error::other("x")), Io),
        (Error::invalid_path("p"), Io),
        (Error::Timeout("t".into()), Io),
        (Error::serialization("s"), Serialization),
        (Error::type_conversion("t"), Serialization),
        (Error::corruption("c"), Corruption),
        // A corrupt CommitLog frame is on-disk corruption, alongside `Corruption` —
        // deliberately NOT `Parsing`, so a checksum/framing failure lands on the
        // corruption dashboard an operator watches for bit-rot (#1705, F10).
        (Error::CorruptCommitLogFrame("f".into()), Corruption),
        // Issue #3721: a column whose value could not be decoded IS undecodable
        // data at the cell level, so it joins `Corruption` on the dashboard an
        // operator watches for bad bytes — deliberately NOT `Schema` (the declared
        // type may be perfectly valid and the BYTES wrong) and never `Other`.
        (
            Error::column_decode("col", "int", 0, Error::corruption("c")),
            Corruption,
        ),
        (Error::schema("s"), Schema),
        (Error::Table("t".into()), Schema),
        (Error::parse("p"), Parsing),
        (Error::cql_parse("p"), Parsing),
        (Error::invalid_format("f"), Parsing),
        (Error::unsupported_format("f"), Parsing),
        // A version/format floor rejection is a FORMAT-parsing failure, not a
        // configuration or storage one — same bucket as `UnsupportedFormat`.
        (
            Error::UnsupportedVersion {
                version: "ma".into(),
                floor: "na".into(),
            },
            Parsing,
        ),
        (
            Error::UnsupportedCommitLogVersion {
                version: 5,
                floor: 6,
                ceiling: 7,
            },
            Parsing,
        ),
        (Error::storage("s"), Storage),
        (Error::memory("m"), Storage),
        (Error::index("i"), Storage),
        (Error::compaction("c"), Storage),
        (Error::write_dir_locked("/d"), Storage),
        (Error::concurrency("c"), Concurrency),
        (Error::transaction("t"), Concurrency),
        (Error::constraint_violation("c"), Constraints),
        (Error::already_exists("a"), Constraints),
        (Error::query_execution("q"), Query),
        (Error::unsupported_query("q"), Query),
        (Error::invalid_input("i"), Query),
        // The three query-time outcomes: a byte-budget refusal, and the #1918
        // read-path forcing knob failing closed, are QUERY outcomes — not `Storage`,
        // not `Configuration` (#1705, F10).
        (
            Error::ResultTooLarge {
                budget_bytes: 1,
                estimated_bytes: 2,
                rows: 3,
            },
            Query,
        ),
        (
            Error::ForcedReadPathUnavailable {
                forced: "point",
                reason: "r".into(),
            },
            Query,
        ),
        (
            Error::InvalidReadPath {
                value: "nope".into(),
            },
            Query,
        ),
        // Issue #1695: an elapsed `query.max_execution_time`. Judged from the
        // variant's MEANING, not from what classify() does: the budget is set by an
        // OPERATOR, so its elapse says nothing about the data (never `Corruption`)
        // and nothing about the transport (never `Io`). Nor is it `Query` with the
        // genuine query faults — a malformed or unsupported query is the caller's
        // bug and needs the query fixed, whereas a timeout is a capacity signal
        // needing the budget raised or the scan narrowed, and a rising rate of it is
        // the one thing an operator dashboard must be able to see. That is its own
        // bucket, and `Other` would bury it.
        (
            Error::QueryTimeout {
                operation: "query.execute".into(),
                elapsed: std::time::Duration::from_millis(1500),
                limit: std::time::Duration::from_millis(1000),
            },
            Timeout,
        ),
        (Error::configuration("c"), Other),
        (Error::invalid_state("s"), Other),
        (Error::invalid_operation("o"), Other),
        (Error::not_found("n"), Other),
        (Error::internal("i"), Other),
        // Issue #2264: a cooperative cancellation must be its OWN bucket, not `Io`
        // (misleading — it is not a transport failure) and not lumped into the
        // generic `Other` bucket (would hide cancellation rate).
        (Error::Cancelled, Cancelled),
    ];
    out.extend(wasm_expectations());
    out
}

/// The hand-written expectation for the `#[cfg(target_arch = "wasm32")]` variants:
/// present on a `wasm32` build, empty elsewhere.
///
/// Issue #1705 (roborev F14): the coverage guard used to exclude `wasm32`-gated
/// variants UNCONDITIONALLY, so on a `wasm32` build — where `Error::Wasm` really is
/// constructible — the one oracle that does not derive from `classify()` would have
/// had nothing to say about it, and `classify()` plus the taxonomy table could agree
/// on a wrong category undetected. That is a vacuity hole in the guard whose whole
/// purpose is to be un-vacuous, so the exclusion is now conditional on
/// [`WASM_VARIANTS_COMPILED`].
///
/// `Other`, judged from the variant's MEANING: the telemetry taxonomy has no
/// platform bucket (`crate::error::ErrorCategory::Platform` is the developer-facing
/// enum, not this one), and a WASM host/JS-boundary failure is none of the specific
/// ones — not the transport (`Io`), not the data, not the query, not a budget. With
/// no dedicated bucket to route it to, `Other` is the honest answer.
///
/// UNEXECUTED today: CQLite does not build for `wasm32` (WASM bindings are M6), so
/// on every target we compile this list is empty and only its non-`wasm32` half is
/// exercised. It is written now so that the day a `wasm32` build exists, `Wasm` has
/// the same independent second opinion as every other variant instead of a hole.
fn wasm_expectations() -> Vec<(Error, ObsErrorCategory)> {
    #[cfg(target_arch = "wasm32")]
    {
        vec![(Error::Wasm("w".into()), ObsErrorCategory::Other)]
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        Vec::new()
    }
}

#[test]
fn classify_every_error_variant() {
    for (err, expected) in independent_expectations() {
        let variant = debug_variant_name(&err);
        assert_eq!(
            err.obs_category(),
            expected,
            "Error::{variant} must classify as {expected:?}"
        );
    }
}

#[test]
fn the_independent_category_test_covers_every_variant_the_taxonomy_documents() {
    // Issue #1705 (roborev F10): [`independent_expectations`] is the ONLY oracle for
    // the CORRECT category — every other guard here compares two derivations of the
    // same source, which cannot detect a variant filed under the wrong one. So its
    // coverage must not lag behind the enum, which is precisely how the six variants
    // this issue documents came to be unpinned.
    //
    // MEASURED, not scraped: each expectation's variant name is read back from its
    // value's derived `Debug`, and compared against the compiler-audited
    // `classify()` arm set. Adding an `Error` variant therefore reds THIS test until
    // a hand-written expectation for it exists.
    let covered: std::collections::BTreeSet<String> = independent_expectations()
        .iter()
        .map(|(err, _)| debug_variant_name(err))
        .collect();
    assert!(
        !covered.is_empty(),
        "the coverage guard must have a subject — an empty expectation list passes \
         vacuously"
    );

    let missing: Vec<String> = classify_arms()
        .into_iter()
        // A `wasm32`-gated variant is excluded ONLY where it does not compile and so
        // cannot be constructed (`actual_map` states that gap too). Where it DOES
        // compile it is demanded like any other variant — an unconditional exclusion
        // left the independent oracle vacuous for it (#1705, F14).
        .filter(|(variant, (_, wasm_gated))| {
            (WASM_VARIANTS_COMPILED || !wasm_gated) && !covered.contains(variant)
        })
        .map(|(variant, (category, _))| format!("Error::{variant} -> {category}"))
        .collect();
    assert!(
        missing.is_empty(),
        "classify() routes variants that the INDEPENDENT hand-written expectation \
         list never pins, so nothing asserts their CORRECT category (add them to \
         `independent_expectations`): {missing:?}"
    );

    // And the affirmative direction: every expectation names a variant classify()
    // actually routes, so a stale entry for a deleted variant cannot sit here
    // unnoticed.
    let routed: std::collections::BTreeSet<String> = classify_arms().into_keys().collect();
    let stale: Vec<&String> = covered.iter().filter(|v| !routed.contains(*v)).collect();
    assert!(
        stale.is_empty(),
        "the independent expectation list names variants classify() does not route \
         (remove the stale entries): {stale:?}"
    );
}
