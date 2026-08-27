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
//! Both sides are derived PROGRAMMATICALLY from the one source file — the table by
//! parsing the module doc comment, the mapping by parsing `classify()`'s match arms
//! — so there is no second hand-maintained list to drift. Cross-block rule (epic
//! #1686 capstone §3): `classify()` is the authority the bindings' error table
//! derives from, so the core table must be exact.

use super::*;
use crate::error::Error;

/// The one copy of `error_schema.rs`'s source both parsers read.
fn error_schema_src() -> &'static str {
    include_str!("error_schema.rs")
}

/// Every ``backticked`` token in `s`, in order.
fn backticked(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = s.as_bytes();
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
        debug_assert!(i <= bytes.len());
    }
    out
}

/// The `Error` variants a `Maps from` cell documents.
///
/// The cell is a comma-separated list; an item DOCUMENTS a variant only when the
/// item OPENS with a backticked identifier, and any prose after it inside the same
/// item is commentary. That rule (rather than "every backticked token in the cell")
/// is what keeps the `Cancelled` row's contrast mention — ``never `Io``` — from
/// being read as a second `Io` mapping, which would make the doc side of the
/// comparison self-contradictory. It also drops the `Other` row's trailing
/// "and any future variant (catch-all)" prose, which names no variant.
fn documented_variants(cell: &str) -> Vec<String> {
    let mut out = Vec::new();
    for item in cell.split(',') {
        let item = item.trim();
        if !item.starts_with('`') {
            continue;
        }
        if let Some(first) = backticked(item).first() {
            out.push((*first).to_string());
        }
    }
    out
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
        let mapped: Vec<String> = documented_variants(cells[2]);
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

/// Parse `classify()`'s body into `Error` variant → `ErrorCategory` variant.
///
/// Reads the ACTUAL match arms, so a variant added to `classify()` without a
/// doc-table row is visible here even though Rust has no reflection over enum
/// variants. Arms are delimited by `=> ErrorCategory::<Cat>`; every `Error::<V>`
/// pattern preceding a delimiter belongs to that arm.
fn classified_variants() -> std::collections::BTreeMap<String, String> {
    let src = error_schema_src();
    let start = src
        .find("fn classify(")
        .expect("classify() must exist in error_schema.rs");
    let body = &src[start..];
    let end = body
        .find("\n}\n")
        .expect("classify() must be terminated by a column-0 closing brace");
    let body = &body[..end];

    const ARM: &str = "=> ErrorCategory::";
    let mut out = std::collections::BTreeMap::new();
    let mut prev_end = 0usize;
    for (idx, _) in body.match_indices(ARM) {
        let patterns = &body[prev_end..idx];
        let after = &body[idx + ARM.len()..];
        let category: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        assert!(
            !category.is_empty(),
            "an arm must name an ErrorCategory variant"
        );
        for (v, _) in patterns.match_indices("Error::") {
            let ident: String = patterns[v + "Error::".len()..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.is_empty() {
                continue;
            }
            let prior = out.insert(ident.clone(), category.clone());
            assert!(
                prior.is_none() || prior.as_deref() == Some(category.as_str()),
                "Error::{ident} appears in two classify() arms ({prior:?} and {category})"
            );
        }
        prev_end = idx + ARM.len() + category.len();
    }
    assert!(
        out.len() > 20,
        "classify() must have been parsed; got {out:?}"
    );
    out
}

#[test]
fn as_str_is_lowercase_and_unique() {
    let mut seen = std::collections::HashSet::new();
    for c in ErrorCategory::ALL {
        let s = c.as_str();
        assert_eq!(s, s.to_ascii_lowercase());
        assert!(seen.insert(s), "duplicate category label {s}");
    }
    assert_eq!(seen.len(), ErrorCategory::ALL.len());
}

#[test]
fn display_matches_as_str() {
    for c in ErrorCategory::ALL {
        assert_eq!(c.to_string(), c.as_str());
    }
}

#[test]
fn documented_categories_equal_the_error_category_enum() {
    // Issue #1705: category-level code↔doc set equality. The table's column 1
    // must be exactly `ErrorCategory::ALL`, in the same order, with column 2
    // matching `as_str()` — so a new telemetry category cannot ship undocumented
    // and a retired one cannot linger in the operator-facing table.
    let documented: Vec<(String, String)> = documented_taxonomy()
        .into_iter()
        .map(|(v, l, _)| (v, l))
        .collect();
    let actual: Vec<(String, String)> = ErrorCategory::ALL
        .iter()
        .map(|c| (format!("{c:?}"), c.as_str().to_string()))
        .collect();
    assert_eq!(
        documented, actual,
        "the error_schema taxonomy table's categories must equal ErrorCategory::ALL \
         (variant + as_str label, same order)"
    );
}

#[test]
fn every_error_variant_classify_routes_is_documented_in_the_taxonomy_table() {
    // Issue #1705 (AI5) — the RED test. `classify()` is the authority the
    // bindings' error table derives from (epic #1686 capstone §3), and its
    // module-doc table is what operators and binding authors read. Assert
    // variant→category set equality in BOTH directions:
    //
    //   * a variant `classify()` routes but the table omits  → undocumented
    //     behaviour (drifted on `main`: `UnsupportedVersion` and five others).
    //   * a variant the table names but `classify()` does not → a phantom row
    //     describing behaviour that does not exist.
    //
    // Mapping equality, not just membership: a variant listed under the WRONG
    // category row is also caught.
    let classified = classified_variants();

    let mut documented = std::collections::BTreeMap::new();
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

// Exhaustively cover every Error constructor / variant -> expected category.
#[test]
fn classify_every_error_variant() {
    use ErrorCategory::*;

    let io = std::io::Error::other("x");
    assert_eq!(Error::from(io).obs_category(), Io);
    assert_eq!(Error::invalid_path("p").obs_category(), Io);
    assert_eq!(Error::Timeout("t".into()).obs_category(), Io);

    assert_eq!(Error::serialization("s").obs_category(), Serialization);
    assert_eq!(Error::type_conversion("t").obs_category(), Serialization);

    assert_eq!(Error::corruption("c").obs_category(), Corruption);

    assert_eq!(Error::schema("s").obs_category(), Schema);
    assert_eq!(Error::Table("t".into()).obs_category(), Schema);

    assert_eq!(Error::parse("p").obs_category(), Parsing);
    assert_eq!(Error::cql_parse("p").obs_category(), Parsing);
    assert_eq!(Error::invalid_format("f").obs_category(), Parsing);
    assert_eq!(Error::unsupported_format("f").obs_category(), Parsing);

    assert_eq!(Error::storage("s").obs_category(), Storage);
    assert_eq!(Error::memory("m").obs_category(), Storage);
    assert_eq!(Error::index("i").obs_category(), Storage);
    assert_eq!(Error::compaction("c").obs_category(), Storage);
    assert_eq!(Error::write_dir_locked("/d").obs_category(), Storage);

    assert_eq!(Error::concurrency("c").obs_category(), Concurrency);
    assert_eq!(Error::transaction("t").obs_category(), Concurrency);

    assert_eq!(Error::constraint_violation("c").obs_category(), Constraints);
    assert_eq!(Error::already_exists("a").obs_category(), Constraints);

    assert_eq!(Error::query_execution("q").obs_category(), Query);
    assert_eq!(Error::unsupported_query("q").obs_category(), Query);
    assert_eq!(Error::invalid_input("i").obs_category(), Query);

    assert_eq!(Error::configuration("c").obs_category(), Other);
    assert_eq!(Error::invalid_state("s").obs_category(), Other);
    assert_eq!(Error::invalid_operation("o").obs_category(), Other);
    assert_eq!(Error::not_found("n").obs_category(), Other);
    assert_eq!(Error::internal("i").obs_category(), Other);

    // Issue #2264: a cooperative cancellation must be its OWN bucket, not
    // `Io` (misleading — it is not a transport failure) and not lumped into
    // the generic `Other` catch-all (would hide cancellation rate).
    assert_eq!(Error::Cancelled.obs_category(), Cancelled);
}
