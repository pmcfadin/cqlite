//! The per-type SPELLING differential for the CSV container seam (issue #3644,
//! item (a)) — split out of `golden_csv_container_tests.rs` under the campsite
//! rule (CLAUDE.md, epic #1135).
//!
//! ONE responsibility: `stringified_csv_text`'s doc comment carries a
//! hand-derived, UNMEASURED census of how the GOLDEN spells each scalar type
//! against how `ValueFormatter` — the CSV egress's own formatter — spells it
//! (`blob` DIFFERS materially and is translated; `timestamp` differs immaterially;
//! "every other type — IDENTICAL text"). That paragraph is a second implementation
//! of the formatter's per-type behaviour written in English, and CLAUDE.md's rule
//! is that a port's correctness is knowable only by differential testing against
//! the original. This module is that differential.
//!
//! A child of the container test module, so `ty_of`, `tag` and `VARIANTS` — the
//! same DDL parser and the same `CqlType` census the sibling cases use — are
//! reached through `use super::*` and are stated once.

use super::*;

/// One declared type's two golden spellings, and what the seam must do with each.
///
/// `natural` is the RAW JSON token `sstabledump` wrote with
/// `writeRawValue(type.toJSONString(v))`; `stringified` is the TEXT it wrote with
/// `writeString(type.getString(v))`. Both are the GOLDEN side and neither is taken
/// from CQLite: each case's `source` names where its spelling comes from — a
/// committed `*-Data.db.jsonl` (real `sstabledump` output) where the corpus has
/// one, else the pin `cassandra-5.0.8` as recorded, with file citations, in
/// [`Kinding`]'s doc comment. The CSV side is never written down here at all: it
/// is MEASURED, by calling `ValueFormatter::format_value` on `value`.
struct SpellingCase {
    decl: &'static str,
    value: cqlite_core::types::Value,
    natural: &'static str,
    natural_expect: Expect,
    stringified: &'static str,
    stringified_expect: Expect,
    source: &'static str,
}

/// What the differential requires of ONE (case, kinding) pair.
enum Expect {
    /// The census's claim: the golden's text IS the CSV text, so the seam passing
    /// it through verbatim (or, for `blob`, translating it) is correct.
    Same,
    /// A DECLARED narrowing — the ONLY exception set this differential admits.
    /// Carries the CSV spelling `ValueFormatter` produces, so the divergence is
    /// PINNED on both sides rather than tolerated: a change in either spelling
    /// fails, and `why` names the narrowing and where the value comparison closes
    /// it.
    ///
    /// A divergence that is not one of these must FAIL. Adding an entry to make a
    /// newly-measured divergence pass would convert a defect into documentation of
    /// itself.
    Narrowed {
        csv: &'static str,
        why: &'static str,
    },
}

/// THE DIFFERENTIAL. For each declared type, at BOTH kindings:
///
/// ```text
/// scalar_csv_text(<the golden node at this position>, Some(&ty), kinding)
///     == ValueFormatter::format_value(&<the value>)
/// ```
///
/// # Why the node is built per KINDING and not once
///
/// The golden node for one value DIFFERS by position: at [`Kinding::Stringified`]
/// `sstabledump` wrote `writeString(type.getString(v))`, so the node is always a
/// JSON STRING; at [`Kinding::Natural`] it wrote
/// `writeRawValue(type.toJSONString(v))`, so it carries its own JSON kind — a
/// number for the numeric family, a boolean for `boolean`, a string for the rest.
/// Feeding a string node to a natural position would manufacture a divergence that
/// no golden has.
///
/// # The exception set, in full — two entries, both already declared
///
///   * **`timestamp`, both kindings.** `2025-10-06 01:12:05.394Z` (natural) /
///     `2025-10-06T01:12:05.394Z` (stringified) against `ValueFormatter`'s
///     `2025-10-06 01:12:05.394+0000`. This lane's declared timestamp narrowing;
///     the value comparison closes it in `super::super::canon_timestamp`, and
///     neither spelling can carry a `, `, a bracket, an undoubled `: ` or an empty
///     rendering, so the structural question this seam answers is unaffected.
///   * **`double` 0.0, both kindings.** The golden's `0.0` (Java
///     `Double.toString`) against `ValueFormatter`'s `0` (Rust's `{}`). The
///     declared trailing-zero narrowing; the value comparison closes it in
///     `super::super::normalize_decimal`, and it likewise cannot carry a
///     separator, a bracket or an empty rendering.
///
/// # What this case deliberately does NOT assert, and why
///
///   * **`duration`.** A MEASURED, DECLARED MATERIAL DIVERGENCE — reported by both
///     sides, excused by neither: Cassandra's `Duration.toString()`
///     decomposes into `y/mo/w/d/h/m/s/ms/us/ns` — the committed
///     `test_basic.simple_table` golden carries `"12h58m22s"` and `"1h20m44s"` —
///     while `ValueFormatter::format_duration` prints months/days/NANOS only, i.e.
///     `46702000000000ns` for that same value. Same value, materially different
///     text. The repository already records the same divergence for the sibling
///     #1490 lane (`tests/support/parquet_parity/spelling.rs`, module doc). The
///     census that this differential measures NOW DECLARES it, as a MATERIAL
///     divergence (`stringified_csv_text`'s doc in
///     `tests/support/golden_csv_container.rs`), so
///     the two no longer disagree — and no case is added here even so, in either
///     direction: an [`Expect::Narrowed`] entry would document a DEFECT as itself
///     (that variant is for a declared narrowing the value comparison closes, and
///     nothing closes this one), and an [`Expect::Same`] case would simply fail.
///     It is withheld pending the `format_duration` follow-up, not because the
///     divergence is in doubt. No committed fixture in THIS lane's case list
///     carries a `duration` column, so nothing is suppressed by leaving it out.
///   * **a NON-FINITE `double`.** `DoubleType.toJSONString` writes the literal
///     `null` at a natural position (see
///     `gap::Divergence::NonFiniteFloatRendersAsJsonNull`), so the golden has no
///     spelling of the value there and the pair is not a spelling question.
///     `getString`'s `NaN`/`Infinity`/`-Infinity` and `ValueFormatter`'s are
///     already pinned by that gap's own coverage.
///   * **an IPv6 `inet`.** No committed or fetched golden carries one, and
///     `getString`'s IPv6 spelling cannot be established from an authority
///     reachable offline, so asserting either way would be a guess. IPv4 is
///     measured, from a real golden.
#[test]
fn every_declared_type_spells_a_scalar_the_way_the_csv_egress_does() {
    use cqlite_core::types::Value as CoreValue;
    use cqlite_core::util::value_fmt::ValueFormatter;
    use std::collections::BTreeSet;

    let cases: Vec<SpellingCase> = vec![
        // --- the integer family: `String.valueOf` / `BigInteger.toString(10)`
        // against `to_string()`.
        SpellingCase {
            decl: "int",
            value: CoreValue::Integer(1),
            natural: "1",
            natural_expect: Expect::Same,
            stringified: "1",
            stringified_expect: Expect::Same,
            source: "test_signed_coll.signed_special_collections golden, \
                     partition key [\"1\"]",
        },
        SpellingCase {
            decl: "bigint",
            value: CoreValue::BigInt(-9),
            natural: "-9",
            natural_expect: Expect::Same,
            stringified: "-9",
            stringified_expect: Expect::Same,
            source: "pin: LongType, the default String.valueOf spelling",
        },
        SpellingCase {
            decl: "smallint",
            value: CoreValue::SmallInt(11684),
            natural: "11684",
            natural_expect: Expect::Same,
            stringified: "11684",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, medium_number 11684",
        },
        SpellingCase {
            decl: "tinyint",
            value: CoreValue::TinyInt(122),
            natural: "122",
            natural_expect: Expect::Same,
            stringified: "122",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, small_number 122",
        },
        SpellingCase {
            decl: "varint",
            value: CoreValue::varint(vec![0x30, 0x39]),
            natural: "12345",
            natural_expect: Expect::Same,
            stringified: "12345",
            stringified_expect: Expect::Same,
            source: "pin: IntegerType, BigInteger.toString(10)",
        },
        // --- decimal: `BigDecimal.toString()` on both sides.
        SpellingCase {
            decl: "decimal",
            value: CoreValue::Decimal {
                scale: 2,
                unscaled: vec![0x30, 0x36, 0x0f],
            },
            natural: "31595.67",
            natural_expect: Expect::Same,
            stringified: "31595.67",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, account_balance 31595.67",
        },
        SpellingCase {
            decl: "decimal",
            value: CoreValue::Decimal {
                scale: 1,
                unscaled: vec![0xf1],
            },
            natural: "-1.5",
            natural_expect: Expect::Same,
            stringified: "-1.5",
            stringified_expect: Expect::Same,
            source: "test_signed_coll.signed_special_collections golden, \
                     `sd` cell path \"-1.5\"",
        },
        // --- the float family.
        SpellingCase {
            decl: "float",
            value: CoreValue::Float32(1.84),
            natural: "1.84",
            natural_expect: Expect::Same,
            stringified: "1.84",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, height 1.84",
        },
        SpellingCase {
            decl: "double",
            value: CoreValue::Float(2.5),
            natural: "2.5",
            natural_expect: Expect::Same,
            stringified: "2.5",
            stringified_expect: Expect::Same,
            source: "test_signed_coll.signed_special_collections golden, \
                     `sf` cell path \"2.5\"",
        },
        SpellingCase {
            decl: "double",
            value: CoreValue::Float(0.0),
            natural: "0.0",
            natural_expect: Expect::Narrowed {
                csv: "0e0",
                why: "DECLARED float-spelling narrowing (exponent form): Java \
                      Double.toString spells zero `0.0`, while ValueFormatter's \
                      `|f| < 1e-6` branch takes `{:e}` and spells it `0e0`. Closed \
                      for VALUE equality by normalize_decimal, which reads an \
                      exponent; carries no separator, bracket or empty spelling, so \
                      the structural question is unaffected",
            },
            stringified: "0.0",
            stringified_expect: Expect::Narrowed {
                csv: "0e0",
                why: "the same declared narrowing at the stringified position",
            },
            source: "test_signed_coll.signed_special_collections golden, \
                     `sf` cell path \"0.0\"",
        },
        // --- boolean: `Boolean.toString()` on both sides, JSON-kind-split only.
        SpellingCase {
            decl: "boolean",
            value: CoreValue::Boolean(true),
            natural: "true",
            natural_expect: Expect::Same,
            stringified: "true",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, active true",
        },
        // --- text / varchar / ascii: the raw string on both sides.
        SpellingCase {
            decl: "text",
            value: CoreValue::text("Mr. James Hoffman"),
            natural: "\"Mr. James Hoffman\"",
            natural_expect: Expect::Same,
            stringified: "Mr. James Hoffman",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, name",
        },
        SpellingCase {
            decl: "varchar",
            value: CoreValue::text(""),
            natural: "\"\"",
            natural_expect: Expect::Same,
            stringified: "",
            stringified_expect: Expect::Same,
            source: "pin: UTF8Type — the one type whose rendering CAN be empty, \
                     which is why the EMPTY-CONTAINER bound exists",
        },
        SpellingCase {
            decl: "ascii",
            value: CoreValue::text("ascii"),
            natural: "\"ascii\"",
            natural_expect: Expect::Same,
            stringified: "ascii",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, ascii_field \"ascii\"",
        },
        // --- blob: the ONE type the census calls materially different, and the
        // one the seam TRANSLATES. Both cases require the translation to land on
        // `ValueFormatter`'s `0x…`, so this proves the seam WORKS rather than
        // merely restating that the two sides differ.
        SpellingCase {
            decl: "blob",
            value: CoreValue::blob(vec![0x61]),
            natural: "\"0x61\"",
            natural_expect: Expect::Same,
            stringified: "61",
            stringified_expect: Expect::Same,
            source: "pin: BytesType.toJSONString = \"0x\" + hex; \
                     BytesSerializer.toString = ByteBufferUtil.bytesToHex",
        },
        SpellingCase {
            decl: "blob",
            value: CoreValue::blob(Vec::new()),
            natural: "\"0x\"",
            natural_expect: Expect::Same,
            // `""` — the case that synthesized an EMPTY body and made a sole
            // empty-blob member an unrecoverable-node refusal.
            stringified: "",
            stringified_expect: Expect::Same,
            source: "pin: the empty blob's bytesToHex is the empty string",
        },
        // --- timestamp: the declared narrowing, at both kindings.
        SpellingCase {
            decl: "timestamp",
            value: CoreValue::Timestamp(1_759_713_125_394),
            natural: "\"2025-10-06 01:12:05.394Z\"",
            natural_expect: Expect::Narrowed {
                csv: "2025-10-06 01:12:05.394+0000",
                why: "DECLARED timestamp narrowing: FORMATTER_TO_JSON's `X` zone \
                      against ValueFormatter's `+0000`. Closed for VALUE equality \
                      by canon_timestamp; the pattern's colons are digit-flanked \
                      and neither spelling is ever empty",
            },
            stringified: "2025-10-06T01:12:05.394Z",
            stringified_expect: Expect::Narrowed {
                csv: "2025-10-06 01:12:05.394+0000",
                why: "the same declared narrowing, with FORMATTER_UTC's `T` \
                      separator as well",
            },
            source: "test_basic.simple_table golden, created \
                     \"2025-10-06 01:12:05.394Z\"",
        },
        // --- the opaque scalars: one function on both sides.
        SpellingCase {
            decl: "date",
            value: CoreValue::Date(20257),
            natural: "\"2025-06-18\"",
            natural_expect: Expect::Same,
            stringified: "2025-06-18",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, birth_date \"2025-06-18\"",
        },
        SpellingCase {
            decl: "time",
            value: CoreValue::Time(4_325_394_017_000),
            natural: "\"01:12:05.394017000\"",
            natural_expect: Expect::Same,
            stringified: "01:12:05.394017000",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, work_time \
                     \"01:12:05.394017000\"",
        },
        SpellingCase {
            decl: "uuid",
            value: CoreValue::Uuid([
                0x15, 0x29, 0x1a, 0x77, 0xd7, 0x39, 0x4e, 0x73, 0x83, 0x97, 0xb7, 0x87, 0x44, 0x2f,
                0x3a, 0x1f,
            ]),
            natural: "\"15291a77-d739-4e73-8397-b787442f3a1f\"",
            natural_expect: Expect::Same,
            stringified: "15291a77-d739-4e73-8397-b787442f3a1f",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, partition key",
        },
        SpellingCase {
            decl: "timeuuid",
            value: CoreValue::Uuid([
                0x78, 0xf6, 0x41, 0x00, 0xa2, 0x51, 0x11, 0xf0, 0xa1, 0x8d, 0xd6, 0x72, 0x6a, 0x63,
                0x7a, 0x4c,
            ]),
            natural: "\"78f64100-a251-11f0-a18d-d6726a637a4c\"",
            natural_expect: Expect::Same,
            stringified: "78f64100-a251-11f0-a18d-d6726a637a4c",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, session_id",
        },
        SpellingCase {
            decl: "inet",
            value: CoreValue::inet(vec![154, 47, 65, 214]),
            natural: "\"154.47.65.214\"",
            natural_expect: Expect::Same,
            stringified: "154.47.65.214",
            stringified_expect: Expect::Same,
            source: "test_basic.simple_table golden, ip_address \"154.47.65.214\"",
        },
    ];

    let mut seen: BTreeSet<&'static str> = BTreeSet::new();
    let mut compared = 0usize;
    for case in &cases {
        let ty = ty_of(case.decl);
        seen.insert(tag(&ty));
        // The CSV side, measured — never written down in the case.
        let csv = ValueFormatter::format_value(&case.value);
        let natural_node: Value = serde_json::from_str(case.natural).unwrap_or_else(|why| {
            panic!("{}: the natural golden token is not JSON: {why}", case.decl)
        });
        for (kinding, node, expect, writer) in [
            (
                Kinding::Natural,
                natural_node,
                &case.natural_expect,
                "writeRawValue(toJSONString(v))",
            ),
            (
                Kinding::Stringified,
                Value::String(case.stringified.to_string()),
                &case.stringified_expect,
                "writeString(getString(v))",
            ),
        ] {
            let got = scalar_csv_text(&node, Some(&ty), kinding);
            match expect {
                Expect::Same => assert_eq!(
                    got, csv,
                    "{} at {kinding:?}: the census claims the golden's \
                     {writer} text IS the CSV text, and the seam produced {got:?} \
                     where ValueFormatter renders {csv:?} (golden spelling from {})",
                    case.decl, case.source
                ),
                Expect::Narrowed { csv: pinned, why } => {
                    assert_eq!(
                        &csv, pinned,
                        "{} at {kinding:?}: the CSV spelling moved, so this \
                         declared narrowing no longer describes the divergence \
                         it excuses ({why})",
                        case.decl
                    );
                    assert_ne!(
                        got, csv,
                        "{} at {kinding:?}: the two sides now AGREE, so the \
                         declared narrowing is stale and must be removed rather \
                         than left excusing nothing ({why})",
                        case.decl
                    );
                }
            }
            compared += 1;
        }
    }

    // A CONTAINER declared type IS reachable here with a SCALAR golden node — that
    // is `gap::Divergence::NestedFrozenValueLeftUndecodedByGolden`, where
    // `sstabledump` leaves a frozen value inside a multi-cell collection undecoded
    // and the golden carries flat hex. No differential is possible (the golden has
    // no per-value spelling there), and the required behaviour is that the seam
    // invents none: the text passes through VERBATIM, and in particular the
    // bare-hex spelling does NOT collect the `0x` a declared `blob` would get.
    let container_decls = [
        "list<frozen<list<int>>>",
        "set<frozen<set<int>>>",
        "map<int, int>",
        "tuple<int, text>",
        "frozen<address>",
    ];
    for decl in container_decls {
        let ty = ty_of(decl);
        seen.insert(tag(&ty));
        let undecoded = Value::String("000000020000".to_string());
        for kinding in [Kinding::Natural, Kinding::Stringified] {
            assert_eq!(
                scalar_csv_text(&undecoded, Some(&ty), kinding),
                "000000020000",
                "{decl} at {kinding:?}: an undecoded golden must pass through \
                 verbatim — translating it would invent a spelling no side emits"
            );
            compared += 1;
        }
    }

    // ANTI-VACUITY. A loop whose body never ran, or a case list someone shortened,
    // must not read as green.
    assert!(
        cases.len() >= 22,
        "the differential lost cases: {} left",
        cases.len()
    );
    assert_eq!(
        compared,
        2 * (cases.len() + container_decls.len()),
        "every case and every container decl must be measured at BOTH kindings"
    );
    for variant in VARIANTS {
        assert!(
            seen.contains(variant),
            "no case establishes the {variant} variant's spelling against the CSV \
             egress's own formatter"
        );
    }
}
