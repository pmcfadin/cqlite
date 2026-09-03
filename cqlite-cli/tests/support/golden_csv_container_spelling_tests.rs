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

// `schema` sits two modules up (this is a child of the container test module,
// which is itself a child of the container module), and the census below names it
// three times; one alias keeps the path out of the assertions.
use super::super::super::schema::native_scalar_decls;

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
    ///
    /// Carries BOTH spellings, so the divergence is PINNED on both sides rather
    /// than tolerated: `csv` is what `ValueFormatter` produces and `golden` is what
    /// the SEAM produces from the golden node at this kinding, and a change in
    /// EITHER fails. An earlier revision carried `csv` alone and then asserted only
    /// that the two sides still differ, which pinned the golden side not at all —
    /// arbitrary golden-side text passed while the doc claimed both were pinned.
    ///
    /// `why` names the narrowing and where the value comparison closes it.
    ///
    /// A divergence that is not one of these must FAIL. Adding an entry to make a
    /// newly-measured divergence pass would convert a defect into documentation of
    /// itself.
    Narrowed {
        csv: &'static str,
        golden: &'static str,
        why: &'static str,
    },
    /// The position CANNOT EXIST, so there is nothing to compare and nothing is
    /// compared: the pair is COUNTED as declared-unreachable and the seam is not
    /// called.
    ///
    /// This is NOT a second exception set for DIVERGENCES, and must never be used
    /// as one. [`Expect::Narrowed`] is for two spellings of a value a golden DOES
    /// carry; this is for a (type, kinding) pair no golden can carry at all,
    /// because Cassandra's own type rules forbid the type from ever sitting there.
    /// `why` must cite those rules from the pin — an assertion here would have to
    /// invent a golden spelling, which is exactly what the `inet` IPv6 omission
    /// refuses to do.
    Unreachable { why: &'static str },
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
/// # What is CENSUSED, and how completely
///
/// Two censuses, because a `CqlType` VARIANT census cannot see a missing CONCRETE
/// type — every numeric type maps to `CqlType::Numeric`, so any ONE numeric case
/// satisfied it and `counter` had no spelling case at all (roborev job 21 F2):
///
///   * **CONCRETE declared types**, over
///     `super::super::super::schema::native_scalar_decls()` — the SAME lists
///     `parse_bare_type` matches against, so this is the parser's own native
///     scalar set and not a copy of it. Every name must have a case, or be in the
///     assertion's `WITHHELD` table with a reason (`duration` is the only entry,
///     and a withholding that goes stale FAILS). Bounded claim, stated: complete
///     with respect to those lists, i.e. to the types this lane's DDL parser
///     RECOGNISES — a type it rejects outright is a parse error, not a silent gap.
///   * **`CqlType` variants**, kept because it covers what the first cannot: the
///     CONTAINER variants, which have no fixed declared spelling, and — via
///     `tag`'s total match — a NEW variant, which is a compile error whose author
///     lands here.
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
///     `Double.toString`) against `ValueFormatter`'s `0e0` — its `|f| < 1e-6`
///     branch takes `{:e}`. The declared exponent-form narrowing; the value
///     comparison closes it in `super::super::normalize_decimal`, which reads an
///     exponent, and it likewise cannot carry a separator, a bracket or an empty
///     rendering.
///
/// # The one DECLARED UNREACHABLE position
///
///   * **`counter` at [`Kinding::Stringified`].** Not an exception to a spelling
///     claim but a position that cannot exist, so nothing is compared and no
///     golden spelling is written down (see [`Expect::Unreachable`]).
///     `CounterColumnType.getString` is `accessor.toHex(value)`
///     (`cassandra-5.0.8:.../marshal/CounterColumnType.java:74-77`) — BARE HEX,
///     which this seam translates only for `blob`, so it WOULD be a material
///     divergence if a golden could carry it. None can: a counter is refused as a
///     PRIMARY KEY column (`CreateTableStatement.java:231-232`) and inside a
///     collection, keys included (`CQL3Type.java:825-836`), which is every
///     stringified position `JsonTransformer` writes. The count of such positions
///     is asserted, so a second one cannot join silently. Its NATURAL position IS
///     measured, from the committed `test_types.ct_single_sstable` golden.
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
        // --- counter: a `CqlType::Numeric` whose two kindings are NOT the same
        // question. See the doc's unreachable-position entry.
        SpellingCase {
            decl: "counter",
            value: CoreValue::Counter(422_212_677_445_164),
            natural: "422212677445164",
            natural_expect: Expect::Same,
            // `writeString(getString(v))` is BARE HEX for a counter
            // (`CounterColumnType.java:74-77` returns `accessor.toHex(value)`),
            // which this seam does not translate — but no golden can carry it,
            // because every stringified position is barred to a counter by
            // Cassandra itself. So the divergence is DECLARED UNREACHABLE rather
            // than pinned: pinning it would assert a golden spelling that cannot
            // occur.
            stringified: "",
            stringified_expect: Expect::Unreachable {
                why: "a counter can occupy NO stringified position: a PRIMARY KEY \
                      column is refused by CreateTableStatement.java:231-232 \
                      (\"counter type is not supported for PRIMARY KEY column\") \
                      and a multicell set element or map key by \
                      CQL3Type.java:825-836 (\"Counters are not allowed inside \
                      collections\"), which is every stringified position \
                      JsonTransformer has. Its getString would diverge materially \
                      if one existed (CounterColumnType.java:74-77 is \
                      accessor.toHex, bare hex like a blob's)",
            },
            source: "test_types.ct_single_sstable golden (COMMITTED), `c` cell \
                     value 422212677445164 — an unquoted JSON number, per \
                     CounterColumnType.toJSONString:91-94 \
                     (CounterSerializer.deserialize(buffer).toString()) written \
                     with writeRawValue. NOTE: that number is the counter CONTEXT \
                     read as a long, which is what sstabledump writes; it is the \
                     GOLDEN's spelling of the cell and this case is about spelling \
                     alone",
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
                // `writeRawValue(toJSONString(0.0))` is the bare JSON number
                // `0.0`, which the seam spells back as `0.0`.
                golden: "0.0",
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
                // `writeString(getString(0.0))` is the text `0.0`, passed through.
                golden: "0.0",
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
                // FORMATTER_TO_JSON's `yyyy-MM-dd HH:mm:ss.SSSX`, as the committed
                // golden carries it; the seam unquotes the JSON string and stops.
                golden: "2025-10-06 01:12:05.394Z",
                why: "DECLARED timestamp narrowing: FORMATTER_TO_JSON's `X` zone \
                      against ValueFormatter's `+0000`. Closed for VALUE equality \
                      by canon_timestamp; the pattern's colons are digit-flanked \
                      and neither spelling is ever empty",
            },
            stringified: "2025-10-06T01:12:05.394Z",
            stringified_expect: Expect::Narrowed {
                csv: "2025-10-06 01:12:05.394+0000",
                // FORMATTER_UTC's `yyyy-MM-dd'T'HH:mm:ss.SSSX`, so the `T`
                // separator as well; `stringified_csv_text` translates only `blob`.
                golden: "2025-10-06T01:12:05.394Z",
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
    let mut decls_seen: BTreeSet<&'static str> = BTreeSet::new();
    let mut compared = 0usize;
    let mut unreachable = 0usize;
    for case in &cases {
        decls_seen.insert(case.decl);
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
            match expect {
                Expect::Same => {
                    let got = scalar_csv_text(&node, Some(&ty), kinding);
                    assert_eq!(
                        got, csv,
                        "{} at {kinding:?}: the census claims the golden's \
                         {writer} text IS the CSV text, and the seam produced \
                         {got:?} where ValueFormatter renders {csv:?} (golden \
                         spelling from {})",
                        case.decl, case.source
                    );
                    compared += 1;
                }
                Expect::Narrowed {
                    csv: pinned,
                    golden: pinned_golden,
                    why,
                } => {
                    let got = scalar_csv_text(&node, Some(&ty), kinding);
                    // BOTH sides pinned, and the three failures are worded apart
                    // because they are three different events: the CSV spelling
                    // moved, the GOLDEN-side spelling moved, or the divergence
                    // closed and the narrowing is now stale.
                    assert_eq!(
                        &csv, pinned,
                        "{} at {kinding:?}: the CSV spelling moved, so this \
                         declared narrowing no longer describes the divergence \
                         it excuses ({why})",
                        case.decl
                    );
                    assert_eq!(
                        &got, pinned_golden,
                        "{} at {kinding:?}: the GOLDEN-side spelling moved — the \
                         seam produced {got:?} from the {writer} golden where \
                         this narrowing pins {pinned_golden:?}, so the narrowing \
                         no longer describes the divergence it excuses ({why}) \
                         (golden spelling from {})",
                        case.decl, case.source
                    );
                    assert_ne!(
                        got, csv,
                        "{} at {kinding:?}: the two sides now AGREE, so the \
                         declared narrowing is stale and must be removed rather \
                         than left excusing nothing ({why})",
                        case.decl
                    );
                    compared += 1;
                }
                Expect::Unreachable { why } => {
                    // Nothing is compared, and nothing may be WRITTEN DOWN
                    // either: a spelling in the case that no assertion reads
                    // would be an unchecked claim about a golden that cannot
                    // exist.
                    assert!(
                        matches!(kinding, Kinding::Stringified) && case.stringified.is_empty(),
                        "{} at {kinding:?}: an unreachable position must carry NO \
                         golden spelling ({why})",
                        case.decl
                    );
                    unreachable += 1;
                }
            }
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
        cases.len() >= 23,
        "the differential lost cases: {} left",
        cases.len()
    );
    assert_eq!(
        compared + unreachable,
        2 * (cases.len() + container_decls.len()),
        "every case and every container decl must be accounted for at BOTH \
         kindings — measured, or counted as declared-unreachable"
    );
    // The declared-unreachable positions are COUNTED, so a second one cannot join
    // silently: `counter` at a stringified position is the only one, and adding
    // another means arguing here that Cassandra's type rules forbid it too.
    assert_eq!(
        unreachable, 1,
        "exactly ONE (case, kinding) pair is declared unreachable (counter at a \
         stringified position); {unreachable} were counted"
    );

    // --- the two censuses ---------------------------------------------------
    //
    // A `CqlType` VARIANT census cannot see a missing CONCRETE type: every numeric
    // type maps to `CqlType::Numeric`, so any ONE numeric case satisfied it and
    // `counter` had no spelling case at all (roborev job 21 F2). So both are
    // asserted, and they answer different questions.

    // (1) CONCRETE declared types. The subject set is
    // `native_scalar_decls()` — the same lists
    // `parse_bare_type` matches against, so a native scalar this lane's DDL parser
    // newly recognises joins this census automatically and FAILS until its
    // spelling is established.
    //
    // WITHHELD names are the ONLY escape, they are NAMED with a reason, and each
    // must still be a native scalar AND still be uncovered — so a stale
    // withholding fails once its case lands. The reasons are the doc's, restated
    // where the assertion is.
    const WITHHELD: &[(&str, &str)] = &[(
        "duration",
        "MEASURED, DECLARED MATERIAL DIVERGENCE (see this test's doc): Duration.toString \
         decomposes into y/mo/w/d/h/m/s/ms/us/ns while ValueFormatter::format_duration \
         prints months/days/NANOS. Withheld pending the format_duration follow-up — an \
         Expect::Narrowed entry would document a defect as itself, and an Expect::Same \
         case would simply fail",
    )];
    for (name, why) in WITHHELD {
        assert!(
            native_scalar_decls().contains(name),
            "withheld `{name}` is not a native scalar type this lane's parser \
             recognises, so the withholding names nothing ({why})"
        );
        assert!(
            !decls_seen.contains(name),
            "`{name}` now HAS a spelling case, so its withholding is stale and \
             must be removed rather than left excusing a case that exists ({why})"
        );
    }
    for decl in native_scalar_decls() {
        if let Some((_, why)) = WITHHELD.iter().find(|(name, _)| *name == decl) {
            // Named, with a reason, in the doc AND here.
            let _ = why;
            continue;
        }
        assert!(
            decls_seen.contains(decl),
            "no case establishes the CONCRETE declared type `{decl}`'s spelling \
             against the CSV egress's own formatter — add one (from a committed \
             golden where the corpus has one, else the pin), or, if it diverges \
             materially, declare it in this test's doc and in WITHHELD rather \
             than leaving it unmeasured"
        );
    }
    // The other direction: a case for a decl the parser does not list would mean
    // the lists are no longer the parser's whole native scalar set.
    for decl in &decls_seen {
        assert!(
            native_scalar_decls().contains(decl),
            "case decl `{decl}` is not in the parser's own native scalar lists, so \
             those lists are no longer the subject set this census claims"
        );
    }

    // (2) `CqlType` VARIANTS, kept because it still adds what (1) cannot: it
    // covers the CONTAINER variants (`list`/`set`/`map`/`tuple`/`udt`), which have
    // no fixed declared spelling, and `tag`'s total match makes a NEW variant a
    // compile error whose author lands here.
    for variant in VARIANTS {
        assert!(
            seen.contains(variant),
            "no case establishes the {variant} variant's spelling against the CSV \
             egress's own formatter"
        );
    }
}
