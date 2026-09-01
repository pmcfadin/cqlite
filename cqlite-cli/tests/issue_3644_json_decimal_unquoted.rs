//! The JSON egress emits a `decimal` as an UNQUOTED number, digit for digit
//! (issue #3644 item 3).
//!
//! # The oracle
//!
//! Cassandra, read at the pinned tag — never CQLite's own output:
//!
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/DecimalType.java:314-317`
//!   — `toJSONString` returns
//!   `Objects.toString(getSerializer().deserialize(buffer), "\"\"")`, i.e. an
//!   UNQUOTED `BigDecimal.toString()`. It deliberately OVERRIDES
//!   `AbstractType.java:186-189`, which is the QUOTING form.
//! * `cassandra-5.0.8:tools/.../JsonTransformer.java:494` — a cell VALUE is
//!   written with `writeRawValue(cellType.toJSONString(...))`, so that text
//!   reaches the document unquoted.
//!
//! The expected DIGITS come from the committed
//! `test_signed_coll.signed_special_collections` `*-Data.db.jsonl`, which
//! `sstabledump` wrote: its `sd` (`set<decimal>`) members live in the cell PATH,
//! spelled by `writeString(type.getString(v))` (`JsonTransformer.java:452`), so
//! the golden carries each value's exact text. The QUOTES around them there are
//! that `getString` path's artifact and not the egress oracle — which is why this
//! test takes the golden's DIGITS and requires the egress to spell them WITHOUT
//! quotes.
//!
//! # Why this test exists beside the `issue_1491` parity lane
//!
//! That lane compares parsed values, and its JSON parse is `serde_json::Value`'s
//! (`i64`/`u64`/`f64`, no `arbitrary_precision`), so a `decimal` past a double's
//! precision — this fixture's 33 significant digits — is gone before the
//! comparison; the lane declares that as
//! `Divergence::ExactDecimalNotCarriedByThisLanesJsonParse`. This test reads the
//! emitted TEXT instead (`serde_json::value::RawValue` keeps each member's
//! lexeme), so the digits are checked exactly. Nothing here round-trips through
//! an `f64`, which would defeat the point.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::value::RawValue;

/// The one committed generation of the fixture this test is about.
///
/// Committed fixtures are resolved CHECKOUT-relative and are fail-closed: this
/// table is git-tracked, so an absent one is a broken checkout, never a skip
/// (CLAUDE.md, issue #3220 — a corpus-conditional skip behind a green suite is
/// the failure mode this rule exists for).
fn fixture_dir(checkout: &Path) -> PathBuf {
    let keyspace_dir = checkout
        .join("test-data")
        .join("datasets")
        .join("sstables")
        .join("test_signed_coll");
    let mut matches: Vec<PathBuf> = std::fs::read_dir(&keyspace_dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", keyspace_dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("signed_special_collections-"))
        })
        .collect();
    matches.sort();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly ONE committed generation of \
         test_signed_coll.signed_special_collections under {} (a second one would make \
         `--data-dir` ambiguous), found {matches:?}",
        keyspace_dir.display()
    );
    matches.remove(0)
}

/// Every `sd` cell-path token in the golden, in the order `sstabledump` wrote
/// them.
fn golden_sd_tokens(fixture: &Path) -> Vec<String> {
    let mut goldens: Vec<PathBuf> = std::fs::read_dir(fixture)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", fixture.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db.jsonl"))
        })
        .collect();
    goldens.sort();
    assert_eq!(
        goldens.len(),
        1,
        "expected exactly ONE *-Data.db.jsonl in {}, found {goldens:?}",
        fixture.display()
    );
    let text = std::fs::read_to_string(&goldens[0])
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", goldens[0].display()));

    let mut tokens = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let doc: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("golden line is not JSON: {e}"));
        let rows = doc
            .get("rows")
            .and_then(|r| r.as_array())
            .unwrap_or_else(|| panic!("golden line has no `rows` array"));
        for row in rows {
            let cells = row
                .get("cells")
                .and_then(|c| c.as_array())
                .unwrap_or_else(|| panic!("golden row has no `cells` array"));
            for cell in cells {
                if cell.get("name").and_then(|n| n.as_str()) != Some("sd") {
                    continue;
                }
                // A multicell SET member IS its cell path; a cell carrying no
                // path is the column's complex deletion, which holds no value.
                let Some(path) = cell.get("path").and_then(|p| p.as_array()) else {
                    continue;
                };
                assert_eq!(
                    path.len(),
                    1,
                    "a set<decimal> member has exactly one path component: {path:?}"
                );
                tokens.push(
                    path[0]
                        .as_str()
                        .unwrap_or_else(|| panic!("cell path component is not a string: {path:?}"))
                        .to_string(),
                );
            }
        }
    }
    // Fail closed: an oracle that found nothing proves nothing.
    assert!(
        tokens.len() >= 4,
        "the committed golden must carry the fixture's four `sd` members, found {tokens:?}"
    );
    tokens
}

/// The raw LEXEME of every member of every row's `sd` array, exactly as the
/// egress spelled it.
///
/// `RawValue` is what keeps the digits: parsing these members into a
/// `serde_json::Value` would put each through an `f64` and destroy the very thing
/// under test.
fn cli_sd_lexemes(json: &str) -> Vec<String> {
    let rows: Vec<BTreeMap<String, Box<RawValue>>> = serde_json::from_str(json)
        .unwrap_or_else(|e| panic!("the CLI's JSON egress is not valid JSON: {e}\n{json}"));
    assert!(!rows.is_empty(), "the CLI emitted no rows:\n{json}");
    let mut lexemes = Vec::new();
    for row in &rows {
        let cell = row
            .get("sd")
            .unwrap_or_else(|| panic!("the CLI emitted no `sd` column:\n{json}"));
        let members: Vec<Box<RawValue>> = serde_json::from_str(cell.get())
            .unwrap_or_else(|e| panic!("`sd` is not a JSON array: {e} ({})", cell.get()));
        lexemes.extend(members.into_iter().map(|m| m.get().to_string()));
    }
    lexemes
}

#[test]
fn json_egress_spells_a_decimal_as_an_unquoted_number_digit_for_digit() {
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-cli has a parent directory")
        .to_path_buf();
    let fixture = fixture_dir(&checkout);
    let schema = checkout
        .join("test-data")
        .join("schemas")
        .join("signed-collection-parity.cql");
    assert!(
        schema.is_file(),
        "committed schema {} is missing (see #3148)",
        schema.display()
    );

    let out_dir = tempfile::tempdir().expect("temp dir");
    let out = out_dir.path().join("egress.json");
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(
            fixture
                .parent()
                .expect("keyspace dir")
                .parent()
                .expect("sstables dir"),
        )
        .arg("export")
        .arg(&out)
        .arg("--format")
        .arg("json")
        .arg("--table")
        .arg("test_signed_coll.signed_special_collections")
        .output()
        .unwrap_or_else(|e| panic!("cannot run the CLI: {e}"));
    assert!(
        output.status.success(),
        "export --format json failed ({:?})\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let json = std::fs::read_to_string(&out)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", out.display()));

    let mut expected = golden_sd_tokens(&fixture);
    let mut emitted = cli_sd_lexemes(&json);
    // Compared as SETS of lexemes: the dump writes a multicell set in comparator
    // order and the reader reads it in storage order, and member ORDER is the
    // `issue_1491` lane's subject, not this one's. The SPELLING is this one's.
    expected.sort();
    emitted.sort();
    assert_eq!(
        emitted, expected,
        "every `sd` member must be spelled exactly as the golden spells it, and \
         UNQUOTED — `DecimalType.toJSONString:314-317` returns a bare \
         BigDecimal.toString(). A quoted member would arrive here wrapped in \
         `\"`.\nemitted document:\n{json}"
    );
    // Belt: the assertion above would also pass if the golden's tokens were
    // themselves quoted, which they are not — state it directly.
    for lexeme in &emitted {
        assert!(
            !lexeme.starts_with('"'),
            "a decimal member must be a JSON NUMBER, not a string: {lexeme}"
        );
    }
}

// ============================================================================
// Per-value cases, through BOTH public JSON writer paths
// ============================================================================
//
// These moved here from `cqlite-cli/src/output/json_cell/tests.rs`, which is a
// `--lib` unit module: `cqlite-cli`'s 255 lib/bin unit tests execute in NO gate
// component and NO CI job (`scripts/tests/workspace-test-disposition.txt` records
// the crate as `PARTIAL / contradicts-doctrine` — the gate's `cli-tests` passes no
// `--lib`, and `.github/workflows/ci.yml` runs only `--test unit_tests`). The gate
// derives its `--test` set from the `cqlite-cli/tests/*.rs` glob, so a case here
// enrols automatically.
//
// Each case runs through BOTH writers — `JSONWriter::write` (batch) and
// `StreamingJSONWriter` (streaming) — because they are two independent
// serializations of the same cell and only the streaming one was exercised
// before. Assertions are on the emitted LEXEME, never on a re-parsed
// `serde_json::Value`: parsing a JSON number yields an `f64` and destroys the
// digits under test.
#[cfg(feature = "state_machine")]
mod writer_paths {
    use std::collections::HashMap;

    use cqlite_cli::config::OutputConfig;
    use cqlite_cli::output::{JSONWriter, StreamingJSONWriter, StreamingWriter};
    use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
    use cqlite_core::types::{DataType, UdtField, UdtValue};
    use cqlite_core::{RowKey, Value};
    use serde_json::value::RawValue;

    /// A one-row, one-column (`v`) result carrying `value`.
    fn one_cell(value: Value) -> QueryResult {
        let metadata = QueryMetadata {
            columns: vec![ColumnInfo {
                name: "v".to_string(),
                data_type: DataType::Text,
                nullable: true,
                position: 0,
                table_name: None,
                cql_type: None,
            }],
            ..Default::default()
        };
        let mut values = HashMap::new();
        values.insert("v".into(), value);
        QueryResult {
            rows: vec![QueryRow {
                values,
                key: RowKey::new(vec![0]),
                metadata: Default::default(),
                cell_metadata: None,
            }],
            rows_affected: 0,
            execution_time_ms: 0,
            metadata,
        }
    }

    /// The `v` cell's raw LEXEME as each writer spelled it: `(batch, streaming)`.
    ///
    /// Both are read back with `RawValue`, which keeps the text verbatim. A
    /// disagreement between the two is itself a defect, so every case asserts on
    /// both.
    fn cell_lexemes(value: Value) -> (String, String) {
        let result = one_cell(value);

        let batch_doc = JSONWriter::write(&result, &OutputConfig::default())
            .unwrap_or_else(|e| panic!("batch JSONWriter::write failed: {e}"));

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamingJSONWriter::new(&mut buf);
            writer
                .write_header(&result.metadata)
                .unwrap_or_else(|e| panic!("streaming write_header failed: {e}"));
            writer
                .write_chunk(&result.rows)
                .unwrap_or_else(|e| panic!("streaming write_chunk failed: {e}"));
            writer
                .finalize()
                .unwrap_or_else(|e| panic!("streaming finalize failed: {e}"));
        }
        let streaming_doc =
            String::from_utf8(buf).unwrap_or_else(|e| panic!("streaming output is not UTF-8: {e}"));

        (lexeme(&batch_doc), lexeme(&streaming_doc))
    }

    /// The `v` member's lexeme in a one-row document, whichever writer wrote it,
    /// with insignificant whitespace removed.
    ///
    /// BOTH writers pretty-print by default, so a CONTAINER cell's lexeme arrives
    /// indented; the indentation is not this test's subject and normalizing it is
    /// what lets one expectation cover both writers. The normalizer is
    /// [`compact`], which removes whitespace only OUTSIDE strings — a JSON number
    /// cannot contain whitespace, so not one digit under test can move. It is
    /// deliberately NOT a re-serialization through `serde_json::Value`, which
    /// would put every number through an `f64` and destroy exactly what is being
    /// asserted.
    fn lexeme(doc: &str) -> String {
        let rows: Vec<HashMap<String, Box<RawValue>>> = serde_json::from_str(doc)
            .unwrap_or_else(|e| panic!("the JSON egress is not valid JSON: {e}\n{doc}"));
        assert_eq!(rows.len(), 1, "expected exactly one row:\n{doc}");
        compact(
            rows[0]
                .get("v")
                .unwrap_or_else(|| panic!("no `v` column:\n{doc}"))
                .get(),
        )
    }

    /// `text` with whitespace outside JSON strings removed, every other byte
    /// verbatim.
    fn compact(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut in_string = false;
        let mut escaped = false;
        for c in text.chars() {
            if in_string {
                out.push(c);
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_string = false;
                }
            } else if c == '"' {
                in_string = true;
                out.push(c);
            } else if !c.is_ascii_whitespace() {
                out.push(c);
            }
        }
        out
    }

    /// Assert both writers spelled the cell exactly `expected`.
    fn assert_both(value: Value, expected: &str) {
        let (batch, streaming) = cell_lexemes(value);
        assert_eq!(batch, expected, "batch JSONWriter::write");
        assert_eq!(streaming, expected, "StreamingJSONWriter");
    }

    fn big_unscaled(digits: &[u8]) -> Vec<u8> {
        num_bigint::BigInt::parse_bytes(digits, 10)
            .expect("literal parses")
            .to_signed_bytes_be()
    }

    /// A 33-significant-digit `decimal` — the committed
    /// `test_signed_coll.signed_special_collections` fixture's own `sd` member —
    /// survives digit for digit, unquoted, through both writers.
    #[test]
    fn decimal_renders_as_an_unquoted_number_with_every_digit() {
        // -999999999999999999999999999999.999 = unscaled
        // -999999999999999999999999999999999 at scale 3.
        assert_both(
            Value::Decimal {
                scale: 3,
                unscaled: big_unscaled(b"-999999999999999999999999999999999"),
            },
            "-999999999999999999999999999999.999",
        );
    }

    /// A `varint` beyond `u64::MAX` — `IntegerType.toJSONString:488-491`.
    #[test]
    fn varint_renders_as_an_unquoted_number_beyond_u64() {
        assert_both(
            Value::varint(big_unscaled(b"170141183460469231731687303715884105727")),
            "170141183460469231731687303715884105727",
        );
    }

    /// The small cases the pre-#3644 `test_varint_formatting` /
    /// `test_decimal_formatting` pinned as QUOTED strings. Same digits, no quotes.
    #[test]
    fn small_varint_and_decimal_are_unquoted_too() {
        assert_both(Value::varint(vec![0x01, 0x00]), "256");
        assert_both(
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x30, 0x39],
            },
            "123.45",
        );
        // A negative one, and one whose scale exceeds its digit count.
        assert_both(
            Value::Decimal {
                scale: 5,
                unscaled: vec![0xFF, 0xFF, 0xCF, 0xC7],
            },
            "-0.12345",
        );
    }

    /// FAIL-SAFE. `ValueFormatter::format_value` is total and renders an
    /// over-bound `decimal` as the marker `<corrupt-decimal:…>`
    /// (`cqlite-core/src/util/value_fmt.rs`, the 32 KiB ceiling from issue #1754).
    /// That is not a JSON number, so it MUST fall back to a quoted string —
    /// emitting it raw would produce an unparseable document.
    #[test]
    fn a_non_numeric_rendering_falls_back_to_a_json_string() {
        let (batch, streaming) = cell_lexemes(Value::Decimal {
            scale: 2,
            unscaled: vec![0x01; 32 * 1024 + 1],
        });
        for (which, text) in [("batch", &batch), ("streaming", &streaming)] {
            assert!(
                text.starts_with("\"<corrupt-decimal:") && text.ends_with('"'),
                "{which}: the corrupt marker must be a quoted string, got: {text}"
            );
        }
        assert_eq!(batch, streaming, "the two writers must agree");
    }

    /// FAIL-SAFE, the second REACHABLE class, and it is ordinary well-formed data:
    /// a ZERO magnitude at a NEGATIVE scale. `format_decimal` makes `decimal_str`
    /// `"0"` and its `scale <= 0` branch appends `"0".repeat(1)`, so the text is
    /// `00` — which JSON forbids (a leading zero followed by a digit). It
    /// therefore falls back to a quoted string rather than emitting an
    /// unparseable document.
    ///
    /// Java's `BigDecimal.toString()` spells this value `0E+1`, so `00` is a
    /// FORMATTER divergence reported separately (`format_decimal`, not this
    /// egress). This case pins what the egress does with the text it is GIVEN: it
    /// does not invent a spelling, and it does not emit invalid JSON.
    #[test]
    fn a_zero_magnitude_at_a_negative_scale_falls_back_to_a_json_string() {
        // `BigInteger.ZERO` at scale -1 — Cassandra's `0E+1`.
        assert_both(
            Value::Decimal {
                scale: -1,
                unscaled: vec![0x00],
            },
            "\"00\"",
        );
        // Two zeros, to show the class is "`0` followed by N zeros" and not one
        // special value.
        assert_both(
            Value::Decimal {
                scale: -2,
                unscaled: vec![0x00],
            },
            "\"000\"",
        );
        // A NON-zero unscaled at a negative scale is a valid JSON number and
        // stays unquoted — the negative scale alone is not the trigger.
        assert_both(
            Value::Decimal {
                scale: -1,
                unscaled: vec![0x05],
            },
            "50",
        );
    }

    /// The exponent form `format_decimal` uses for an over-bound but VALID
    /// magnitude (issue #1754: `<digits>e<-scale>`) is legal JSON, so it stays a
    /// raw number. Measured, not assumed — the fail-safe must not quietly quote a
    /// legitimate value.
    #[test]
    fn the_bounded_exponent_form_stays_an_unquoted_number() {
        // 1025 unscaled bytes is over the positional bound (1024) and well under
        // the 32 KiB corruption ceiling.
        let unscaled = vec![0x01u8; 1025];
        let (batch, streaming) = cell_lexemes(Value::Decimal { scale: 4, unscaled });
        assert_eq!(batch, streaming, "the two writers must agree");
        assert!(
            !batch.contains('"'),
            "a valid over-bound decimal must stay a number: {}",
            &batch[..batch.len().min(40)]
        );
        assert!(
            batch.ends_with("e-4"),
            "expected the exponent form: …{}",
            &batch[batch.len() - 8..]
        );
    }

    /// NESTING. The divergence is a property of the TYPE, not of the position —
    /// and the fixture that exposed it (`set<decimal>`) is nested. A `decimal`
    /// inside a set, a list, a tuple, a map value, a frozen wrapper and a UDT
    /// field is unquoted in every one.
    #[test]
    fn a_nested_decimal_is_unquoted_at_every_position() {
        let d = || Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39],
        };

        assert_both(Value::Set(vec![d()]), "[123.45]");
        assert_both(Value::List(vec![d()]), "[123.45]");
        assert_both(Value::Tuple(vec![Value::Integer(1), d()]), "[1,123.45]");
        assert_both(
            Value::Map(vec![(Value::text("k".to_string()), d())]),
            r#"[{"key":"k","value":123.45}]"#,
        );
        assert_both(Value::Frozen(Box::new(d())), "123.45");

        let udt = Value::Udt(Box::new(UdtValue {
            type_name: "money".to_string(),
            keyspace: "ks".to_string(),
            fields: vec![
                UdtField {
                    name: "amount".to_string(),
                    value: Some(d()),
                },
                UdtField {
                    name: "missing".to_string(),
                    value: None,
                },
            ],
        }));
        // Declared fields and nothing else (issue #3629), an absent field as
        // `null`, and the decimal unquoted.
        assert_both(udt, r#"{"amount":123.45,"missing":null}"#);
    }

    /// Item 2 of issue #3644, kept as a pinned CORRECT behaviour rather than a
    /// gap: a non-finite `double`/`float` renders as JSON `null`, matching
    /// `cassandra-5.0.8:.../marshal/DoubleType.java:114-123` and
    /// `FloatType.java:115-124`, whose `toJSONString` returns the literal `null`
    /// ("JSON does not support NaN, Infinity and -Infinity values. Most of the
    /// parser convert them into null."). This is NOT a defect awaiting a fix.
    #[test]
    fn a_non_finite_float_renders_as_json_null_per_doubletype() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_both(Value::Float(v), "null");
        }
        for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert_both(Value::Float32(v), "null");
        }
        // A FINITE float is a number, so the null above is the format's limit and
        // not a blanket rule.
        assert_both(Value::Float(2.5), "2.5");
    }

    /// A whole document carrying a raw decimal must still PARSE — a raw fragment
    /// that is not valid JSON would be worse than a quoted number. Asserted on
    /// the writers' own bytes, including the streaming PRETTY path, whose
    /// per-line re-indentation is only correct because a raw fragment is
    /// single-line.
    #[test]
    fn a_document_carrying_a_raw_decimal_is_valid_json() {
        let result = one_cell(Value::Decimal {
            scale: 3,
            unscaled: big_unscaled(b"123456789012345678901234567890123"),
        });
        let batch = JSONWriter::write(&result, &OutputConfig::default()).expect("batch writes");
        assert!(
            batch.contains("123456789012345678901234567890.123"),
            "batch document lost the digits:\n{batch}"
        );

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamingJSONWriter::new(&mut buf);
            writer.write_header(&result.metadata).expect("header");
            writer.write_chunk(&result.rows).expect("chunk");
            writer.finalize().expect("finalize");
        }
        let pretty = String::from_utf8(buf).expect("UTF-8");
        assert!(
            pretty.contains("123456789012345678901234567890.123"),
            "streaming pretty document lost the digits:\n{pretty}"
        );
        for doc in [&batch, &pretty] {
            let rows: Vec<HashMap<String, Box<RawValue>>> =
                serde_json::from_str(doc).unwrap_or_else(|e| panic!("document parses: {e}\n{doc}"));
            assert_eq!(
                rows[0]["v"].get(),
                "123456789012345678901234567890.123",
                "the lexeme must survive the round trip:\n{doc}"
            );
        }
    }
}
