//! Issue #3722 (oracle-driven) — every UDT FIELD whose declared type is not
//! `blob` must decode to its own `Value` variant, on every type SPELLING and
//! through every container ROUTE that reaches the shared field decoder.
//!
//! # The defect
//! Two shared UDT-field decoders with DIVERGENT arm sets both ended in
//! `_ => Value::Blob`, so `smallint`/`tinyint`/`decimal`/`varint`/`time`/
//! `timeuuid`/`duration`/collections/tuples arrived as opaque bytes, and WHICH
//! types were dropped depended on which of the two routes the value took. Both
//! are gone; `row_decoder/udt_field.rs` is now the only one and is total over
//! `CqlType`.
//!
//! # Oracle — Cassandra-written bytes, never CQLite's own output
//! The fixture is written by a real `cassandra:5.0.2` (cqlsh INSERT +
//! `nodetool flush`), committed under `test-data/fixtures/issue_3722/`, and every
//! expectation below is the decode of its `sstabledump` JSONL golden — which
//! renders each field DECODED (`"s": -300`, `"d": 123.45`, `"fm": {"k1": 10,
//! "k2": -20}`, …). A CQLite-written/CQLite-read round trip could not detect this
//! class at all: both sides would make the identical mistake and stay green
//! (CLAUDE.md; issue #3722 AC6).
//!
//! Two numbers worth stating because they show the fixture really is the
//! issue's subject: `s = -300` is on-disk `fe d4` and `t = -1` is `ff` — exactly
//! the `Blob(fe d4)` / `Blob(ff)` the issue body measured.
//!
//! # Fixtures are COMMITTED => fail closed (#3220)
//! No `CQLITE_DATASETS_ROOT`, no skip path, and no suite-wide `assert!(ran > 0)`:
//! absence is a broken checkout, and every case asserts for itself.
#![cfg(feature = "cli-helpers")]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::{UdtValue, Value};
use cqlite_core::Database;

const KEYSPACE: &str = "test_udt_wide_fields";
const TABLE: &str = "udt_wide_fields";

// ── Fixture resolution (checkout-relative, glob the table UUID) ──────────────

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core always has a workspace parent directory")
        .to_path_buf()
}

/// The fixture root, asserting the committed binaries are intact. The table
/// directory is GLOBBED: a regeneration mints a fresh table UUID, so a
/// hardcoded path would rot.
fn fixture_root() -> PathBuf {
    let root = repo_root().join("test-data/fixtures/issue_3722");
    let dirs: Vec<PathBuf> = std::fs::read_dir(root.join(KEYSPACE))
        .unwrap_or_else(|e| panic!("committed fixture keyspace dir unreadable ({root:?}): {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .collect();
    assert_eq!(
        dirs.len(),
        1,
        "expected exactly one {TABLE}-* dir under {root:?}, got {dirs:?}"
    );
    let has_data = std::fs::read_dir(&dirs[0])
        .unwrap_or_else(|e| panic!("fixture table dir unreadable: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .any(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        });
    assert!(
        has_data,
        "no *-Data.db under {:?} — the binaries are force-added; see \
         test-data/fixtures/issue_3722/README.md",
        dirs[0]
    );
    root
}

/// `Spelling::CqlShort` loads the committed `.cql`, so field types reach the
/// decoder as CQL short forms (`smallint`, `frozen<list<int>>`, …).
///
/// `Spelling::Marshal` loads NO schema, so the reader falls back to the
/// `Statistics.db` SerializationHeader and the very same fields arrive as
/// authoritative Cassandra marshal forms — `ShortType`, `ByteType`,
/// `DecimalType`, `IntegerType`, `TimeType`, `TimeUUIDType`, `DurationType`,
/// `SimpleDateType`, `InetAddressType`, `ListType(...)`, `MapType(...)`,
/// `TupleType(...)`, a nested `UserType(...)`, `BytesType`, `Int32Type`.
/// Empty `schema_paths` is an established form (cf.
/// `read_path_forcing_schemaless_1918.rs`).
///
/// Issue #3722 AC2 requires the fix to hold on BOTH; the third spelling (a bare
/// registry name) is only reachable in-crate, so it is a unit test in
/// `row_decoder/udt_field.rs` rather than here.
#[derive(Clone, Copy, Debug)]
enum Spelling {
    CqlShort,
    Marshal,
}

async fn open_db(spelling: Spelling) -> Database {
    let schema_paths = match spelling {
        Spelling::CqlShort => {
            vec![repo_root().join("test-data/schemas/issue-3722-udt-wide-fields.cql")]
        }
        Spelling::Marshal => vec![],
    };
    let config = IngestionConfig {
        schema_paths,
        data_dir: fixture_root(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: None,
    };
    ingest(config)
        .await
        .unwrap_or_else(|e| panic!("committed fixture must ingest ({spelling:?}): {e}"))
        .database
}

async fn rows(spelling: Spelling) -> Vec<cqlite_core::query::result::QueryRow> {
    let db = open_db(spelling).await;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over committed fixture failed ({spelling:?}): {e}"));
    assert!(
        !result.rows.is_empty(),
        "committed fixture decoded ZERO rows ({spelling:?}) — 0-rows-when-present is a failure"
    );
    result.rows
}

fn column<'a>(rows: &'a [cqlite_core::query::result::QueryRow], id: i32, name: &str) -> &'a Value {
    let row = rows
        .iter()
        .find(|r| r.values.get("id").and_then(Value::as_i32) == Some(id))
        .unwrap_or_else(|| panic!("fixture row id={id} missing"));
    row.values
        .get(name)
        .unwrap_or_else(|| panic!("row id={id} has no column {name}"))
}

/// Peel any number of `Frozen` wrappers and return the `UdtValue` inside.
fn as_udt<'a>(v: &'a Value, ctx: &str) -> &'a UdtValue {
    let mut cur = v;
    loop {
        match cur {
            Value::Frozen(inner) => cur = inner,
            Value::Udt(u) => return u,
            other => panic!(
                "{ctx}: expected a decoded Value::Udt (optionally Frozen-wrapped), got {}",
                variant(other)
            ),
        }
    }
}

fn field<'a>(udt: &'a UdtValue, name: &str) -> &'a Value {
    udt.fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| {
            panic!(
                "UDT {} has no field {name}; fields = {:?}",
                udt.type_name,
                udt.fields.iter().map(|f| &f.name).collect::<Vec<_>>()
            )
        })
        .value
        .as_ref()
        .unwrap_or_else(|| {
            panic!(
                "UDT {} field {name} is NULL; the fixture writes a value there",
                udt.type_name
            )
        })
}

/// Variant name only — used in failure messages so a wrong-variant decode says
/// WHICH variant it got rather than dumping bytes.
fn variant(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Boolean(_) => "Boolean",
        Value::Integer(_) => "Integer",
        Value::BigInt(_) => "BigInt",
        Value::Counter(_) => "Counter",
        Value::Float(_) => "Float",
        Value::Float32(_) => "Float32",
        Value::Text(_) => "Text",
        Value::Blob(_) => "Blob",
        Value::Timestamp(_) => "Timestamp",
        Value::Date(_) => "Date",
        Value::Time(_) => "Time",
        Value::Uuid(_) => "Uuid",
        Value::Varint(_) => "Varint",
        Value::Decimal { .. } => "Decimal",
        Value::Duration { .. } => "Duration",
        Value::Json(_) => "Json",
        Value::TinyInt(_) => "TinyInt",
        Value::SmallInt(_) => "SmallInt",
        Value::List(_) => "List",
        Value::Set(_) => "Set",
        Value::Map(_) => "Map",
        Value::Tuple(_) => "Tuple",
        Value::Udt(_) => "Udt",
        Value::Frozen(_) => "Frozen",
        Value::Tombstone(_) => "Tombstone",
        Value::Inet(_) => "Inet",
    }
}

/// Raw payload bytes for the variants whose value IS a byte string.
///
/// `Value::as_bytes()` deliberately covers only `Blob` and `Text`, so it returns
/// `None` for `Varint` and `Inet` — using it for those two silently compares
/// `None` against the expectation and would fail on CORRECT output.
fn raw_bytes(v: &Value) -> Option<&[u8]> {
    match peel(v) {
        Value::Varint(b) | Value::Inet(b) | Value::Blob(b) | Value::Text(b) => Some(b.as_ref()),
        _ => None,
    }
}

fn peel(v: &Value) -> &Value {
    match v {
        Value::Frozen(inner) => peel(inner),
        other => other,
    }
}

/// Every `wide` UDT reachable in `column` across all rows, in row order,
/// whatever container the column is.
///
/// Content-addressed rather than keyed on `id` ON PURPOSE: a SCHEMA-LESS ingest
/// (the marshal spelling) does not project the partition-key column at all —
/// measured, the row keys are `{fmw, fsw, mw, sw, w}` with no `id` — while the
/// UDT itself still decodes from the `Statistics.db` marshal types. Keying rows
/// by `id` therefore works only in the CQL-short spelling, and using it for both
/// would fail on CORRECT marshal-spelling output.
/// The first non-null value of `name` across all rows — used only for the
/// STRUCTURAL (outer-variant) assertion, which needs no particular row.
fn any_column<'a>(
    rows: &'a [cqlite_core::query::result::QueryRow],
    name: &str,
    ctx: &str,
) -> &'a Value {
    rows.iter()
        .filter_map(|r| r.values.get(name))
        .find(|v| !matches!(peel(v), Value::Null))
        .unwrap_or_else(|| panic!("{ctx}: column {name} is absent or NULL in every row"))
}

fn udts_in_column<'a>(
    rows: &'a [cqlite_core::query::result::QueryRow],
    name: &str,
) -> Vec<&'a UdtValue> {
    let mut out: Vec<&UdtValue> = Vec::new();
    let push = |v: &'a Value, out: &mut Vec<&'a UdtValue>| {
        if let Value::Udt(u) = peel(v) {
            out.push(u.as_ref());
        }
    };
    for r in rows {
        let Some(v) = r.values.get(name) else {
            continue;
        };
        match peel(v) {
            Value::Udt(u) => out.push(u.as_ref()),
            Value::Map(pairs) => {
                for (k, _) in pairs {
                    push(k, &mut out);
                }
            }
            Value::Set(items) | Value::List(items) => {
                for e in items {
                    push(e, &mut out);
                }
            }
            _ => {}
        }
    }
    out
}

/// The first FULLY POPULATED `wide` UDT in `column`.
///
/// "Fully populated" and not merely "first": fixture row 2 deliberately carries
/// NULL fields, so the first UDT found is not necessarily the one whose every
/// field is assertable.
///
/// An empty result is itself the failure this file exists to catch — it means the
/// column did not decode to a UDT at all.
fn full_udt<'a>(
    rows: &'a [cqlite_core::query::result::QueryRow],
    name: &str,
    ctx: &str,
) -> &'a UdtValue {
    let found = udts_in_column(rows, name);
    found
        .into_iter()
        .find(|u| u.fields.iter().all(|f| f.value.is_some()))
        .unwrap_or_else(|| panic!("{ctx}: no fully-populated `wide` UDT decoded in column {name}"))
}

// ════════════════════════════════════════════════════════════════════════════
// The assertion. Every expected value is the decode of the sstabledump golden.
// ════════════════════════════════════════════════════════════════════════════

/// Assert the `wide` UDT decoded EVERY field to its declared type.
///
/// Values, not just variants: a variant-only assertion would pass on a
/// wrong-WIDTH read (that is exactly how `smallint` looked like a working
/// `Blob`). `s = -300` cannot be produced by reading the wrong number of bytes.
fn assert_wide_fully_decoded(udt: &UdtValue, ctx: &str) {
    assert_eq!(udt.type_name, "wide", "{ctx}: wrong UDT type name");

    // ── scalars the two old decoders dropped ────────────────────────────────
    assert_eq!(
        field(udt, "s"),
        &Value::SmallInt(-300),
        "{ctx}: smallint (on-disk fe d4)"
    );
    assert_eq!(
        field(udt, "t"),
        &Value::TinyInt(-1),
        "{ctx}: tinyint (on-disk ff)"
    );
    assert_eq!(
        field(udt, "d"),
        &Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39]
        },
        "{ctx}: decimal 123.45 = scale 2 / unscaled 12345"
    );
    // varint 90071992547409910000 EXCEEDS u64::MAX (1.8e19), so an i64/u64
    // shortcut cannot represent it — the value is chosen to prove that.
    assert_eq!(
        raw_bytes(field(udt, "vi")),
        Some([0x04, 0xE1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD8, 0xF0].as_slice()),
        "{ctx}: varint bytes (big-endian two's complement of 90071992547409910000)"
    );
    assert!(
        matches!(peel(field(udt, "vi")), Value::Varint(_)),
        "{ctx}: varint must be Value::Varint, got {}",
        variant(peel(field(udt, "vi")))
    );
    assert_eq!(
        field(udt, "tm"),
        &Value::Time(48_654_234_000_000),
        "{ctx}: time 13:30:54.234000000 in nanos since midnight"
    );
    assert_eq!(
        field(udt, "tu"),
        &Value::Uuid([
            0x8a, 0xc6, 0xd5, 0x80, 0x6d, 0x4d, 0x11, 0xee, 0xb9, 0x62, 0x02, 0x42, 0xac, 0x12,
            0x00, 0x02,
        ]),
        "{ctx}: timeuuid — the arm ONLY the deleted free function used to handle"
    );
    assert_eq!(
        field(udt, "du"),
        &Value::Duration {
            months: 2,
            days: 3,
            nanos: 14_700_000_000_000
        },
        "{ctx}: duration 2mo3d4h5m — all three components non-zero"
    );
    // #3722's SECOND bug: this arm applied no 2^31 offset and was wrong by
    // 2^31 days. Authority: cassandra-5.0.8 SimpleDateSerializer,
    // dayToTimeInMillis(days) = Duration.ofDays(days + Integer.MIN_VALUE).
    assert_eq!(
        field(udt, "dt"),
        &Value::Date(20_526),
        "{ctx}: date 2026-03-14 = 20526 days since epoch; an UNOFFSET read gives 20526 - 2^31"
    );
    assert_eq!(
        raw_bytes(field(udt, "ip")),
        Some([192, 168, 1, 42].as_slice()),
        "{ctx}: inet 192.168.1.42"
    );

    // ── collections / tuple / nested UDT ───────────────────────────────────
    assert_eq!(
        peel(field(udt, "fl")),
        &Value::List(vec![
            Value::Integer(1),
            Value::Integer(-2),
            Value::Integer(3)
        ]),
        "{ctx}: frozen<list<int>>"
    );
    match peel(field(udt, "fs")) {
        Value::Set(items) => {
            let got: Vec<&str> = items.iter().filter_map(Value::as_str).collect();
            assert_eq!(got, vec!["a", "b"], "{ctx}: frozen<set<text>> elements");
        }
        other => panic!(
            "{ctx}: frozen<set<text>> must be Value::Set, got {}",
            variant(other)
        ),
    }
    match peel(field(udt, "fm")) {
        Value::Map(pairs) => {
            let got: Vec<(&str, i32)> = pairs
                .iter()
                .filter_map(|(k, v)| Some((k.as_str()?, v.as_i32()?)))
                .collect();
            assert_eq!(
                got,
                vec![("k1", 10), ("k2", -20)],
                "{ctx}: frozen<map<text,int>> pairs"
            );
        }
        other => panic!(
            "{ctx}: frozen<map<text,int>> must be Value::Map, got {}",
            variant(other)
        ),
    }
    match peel(field(udt, "tp")) {
        Value::Tuple(items) => {
            assert_eq!(items.len(), 2, "{ctx}: tuple arity");
            assert_eq!(items[0].as_i32(), Some(7), "{ctx}: tuple.0");
            assert_eq!(items[1].as_str(), Some("seven"), "{ctx}: tuple.1");
        }
        other => panic!(
            "{ctx}: frozen<tuple<int,text>> must be Value::Tuple, got {}",
            variant(other)
        ),
    }
    // A UDT INSIDE A COLLECTION inside a UDT field — roborev round 1's BLOCKER 1.
    // The element type used to be rendered to a STRING, and `CqlType::Udt` renders
    // to a BARE NAME, so the inline field defs were dropped and the element
    // resolved by registry name (⇒ `Value::Blob` schema-less). No other field here
    // reaches that path: `nu` is a UDT DIRECTLY (the field arm, always structural)
    // and fl/fs/fm/tp carry only scalar elements.
    //
    // Golden (sstabledump): `[{"a": 11, "b": "e1"}, {"a": -22, "b": "e2"}]`.
    match peel(field(udt, "fu")) {
        Value::List(items) => {
            assert_eq!(items.len(), 2, "{ctx}: frozen<list<frozen<inner_u>>> arity");
            for (i, (want_a, want_b)) in [(11, "e1"), (-22, "e2")].iter().enumerate() {
                let e = as_udt(&items[i], &format!("{ctx}: fu[{i}]"));
                assert_eq!(e.type_name, "inner_u", "{ctx}: fu[{i}] type name");
                assert_eq!(
                    field(e, "a"),
                    &Value::Integer(*want_a),
                    "{ctx}: fu[{i}].a — a BARE-NAME element render would have made this a Blob"
                );
                assert_eq!(field(e, "b").as_str(), Some(*want_b), "{ctx}: fu[{i}].b");
            }
        }
        other => panic!(
            "{ctx}: frozen<list<frozen<inner_u>>> must be Value::List of Udt, got {}",
            variant(other)
        ),
    }

    let nested = as_udt(field(udt, "nu"), &format!("{ctx}: nested frozen<inner_u>"));
    assert_eq!(nested.type_name, "inner_u", "{ctx}: nested UDT type name");
    assert_eq!(
        field(nested, "a"),
        &Value::Integer(5),
        "{ctx}: nested UDT field a"
    );
    assert_eq!(
        field(nested, "b").as_str(),
        Some("nested"),
        "{ctx}: nested UDT field b"
    );
    // roborev round 2, BLOCKER A: the nested-UDT field arm built its value with
    // an EMPTY keyspace, so a UDT reached through a UDT field had a DIFFERENT
    // public identity (`_keyspace` in the Python/Node bindings; part of `Udt`
    // equality and hashing, #3504) from the same UDT reached any other way.
    assert_eq!(
        nested.keyspace, KEYSPACE,
        "{ctx}: nested UDT keyspace — `\"\"` is a different public identity (#3504)"
    );

    // ── CONTROLS ───────────────────────────────────────────────────────────
    // `bl` is declared `blob`, so it MUST STILL be a Blob. A fix that simply
    // stopped emitting Blob would break here.
    assert_eq!(
        field(udt, "bl"),
        &Value::blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        "{ctx}: CONTROL — a field DECLARED blob must remain Value::Blob"
    );
    // `i` decoded correctly before the fix; a regression in the working path
    // shows up here.
    assert_eq!(
        field(udt, "i"),
        &Value::Integer(7),
        "{ctx}: CONTROL — int already decoded pre-fix and must not regress"
    );

    // ── the blanket property AC1 states ────────────────────────────────────
    for f in &udt.fields {
        if f.name == "bl" {
            continue;
        }
        if let Some(v) = &f.value {
            assert!(
                !matches!(peel(v), Value::Blob(_)),
                "{ctx}: field `{}` is declared {} and decoded to Value::Blob — \
                 no field whose declared type is not `blob` may be an opaque blob (#3722 AC1)",
                f.name,
                "non-blob"
            );
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// AC1 + AC2 + AC3 — three container routes x two reachable spellings
// ════════════════════════════════════════════════════════════════════════════

/// AC3 route 1: a top-level `frozen<udt>` column.
#[tokio::test]
async fn top_level_frozen_udt_column_decodes_every_field_both_spellings() {
    for spelling in [Spelling::CqlShort, Spelling::Marshal] {
        let rows = rows(spelling).await;
        let ctx = format!("column w (top-level frozen<udt>), {spelling:?}");
        assert!(
            matches!(peel(any_column(&rows, "w", &ctx)), Value::Udt(_)),
            "{ctx}: the column must decode to Value::Udt"
        );
        assert_wide_fully_decoded(full_udt(&rows, "w", &ctx), &ctx);
    }
}

/// AC3 route 2: the UDT in a FROZEN MAP KEY. A frozen map is one value cell, so
/// the key really is decoded as a UDT — unlike the multicell cell-path route at
/// the bottom of this file, which #3612 owns.
///
/// CQL-SHORT SPELLING ONLY, and the reason is measured rather than assumed: with
/// NO schema loaded, a FROZEN-OUTER collection column decodes to an opaque
/// `Value::Blob` as a whole — the outer `frozen<map<…>>` type is not resolved
/// from the `Statistics.db` marshal header, so no UDT is reached to decode. That
/// is a PRE-EXISTING gap unrelated to #3722 (the same behaviour
/// `test-data/schemas/nested-udt-keys.cql` records for a schema that fails to
/// load) and it is not this issue's subject. The marshal spelling is still
/// covered for this fix, by the `w` and `sw` routes, where the column type IS
/// resolved and the UDT fields do decode from marshal types.
#[tokio::test]
async fn frozen_map_udt_key_decodes_every_field_cql_short_spelling() {
    for spelling in [Spelling::CqlShort] {
        let rows = rows(spelling).await;
        let ctx = format!("column fmw (frozen map KEY), {spelling:?}");
        assert!(
            matches!(peel(any_column(&rows, "fmw", &ctx)), Value::Map(_)),
            "{ctx}: the column must decode to Value::Map"
        );
        assert_wide_fully_decoded(full_udt(&rows, "fmw", &ctx), &ctx);
    }
}

/// AC3 route 3: the UDT as a FROZEN SET ELEMENT.
///
/// CQL-short spelling only, for the same measured reason as `fmw` above: a
/// frozen-outer collection column is an opaque `Value::Blob` when no schema is
/// loaded. Pre-existing, not #3722's subject.
#[tokio::test]
async fn frozen_set_udt_element_decodes_every_field_cql_short_spelling() {
    for spelling in [Spelling::CqlShort] {
        let rows = rows(spelling).await;
        let ctx = format!("column fsw (frozen set ELEMENT), {spelling:?}");
        assert!(
            matches!(peel(any_column(&rows, "fsw", &ctx)), Value::Set(_)),
            "{ctx}: the column must decode to Value::Set"
        );
        assert_wide_fully_decoded(full_udt(&rows, "fsw", &ctx), &ctx);
    }
}

/// A NULL UDT field must be null, not an opaque blob and not a zero value.
/// Fixture row 2 nulls `t`, `vi`, `du`, `fs`, `fm`, `nu` while keeping the rest.
#[tokio::test]
async fn null_udt_fields_stay_null_and_populated_siblings_still_decode() {
    let rows = rows(Spelling::CqlShort).await;
    let udt = as_udt(column(&rows, 2, "w"), "w/row2");
    for name in ["t", "vi", "du", "fs", "fm", "nu", "fu"] {
        let f = udt
            .fields
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("row 2: UDT has no field {name}"));
        assert!(
            f.value.is_none() || matches!(f.value.as_ref().map(peel), Some(Value::Null)),
            "row 2: field {name} is written NULL and must decode as null, got {:?}",
            f.value.as_ref().map(variant)
        );
    }
    // The populated siblings must still decode — a null field must not derail
    // the fields after it (the absent-field encoding is positional).
    assert_eq!(
        field(udt, "s"),
        &Value::SmallInt(-300),
        "row 2: populated smallint"
    );
    assert_eq!(
        field(udt, "dt"),
        &Value::Date(20_526),
        "row 2: populated date"
    );
    assert_eq!(field(udt, "i"), &Value::Integer(7), "row 2: populated int");
}

/// AC3 route 4: a MULTICELL set's UDT ELEMENT (`set<frozen<wide>>`, non-frozen).
///
/// MEASURED, and it corrects an assumption worth recording: a multicell set's
/// elements are carried in the cell PATH, so the obvious inference is that they
/// share the multicell-map-key fate below and are blocked by #3612. They are
/// NOT — a multicell set element is decoded through a different path than a
/// multicell map key, and it resolves to a full `Value::Udt`. So this route works
/// and is asserted, not characterized.
/// Covered on BOTH spellings: unlike the frozen-outer columns, a NON-frozen set
/// column's type IS resolved without a schema, so the marshal spelling reaches
/// the UDT here (measured).
#[tokio::test]
async fn multicell_set_udt_element_decodes_every_field_both_spellings() {
    for spelling in [Spelling::CqlShort, Spelling::Marshal] {
        let rows = rows(spelling).await;
        let ctx = format!("column sw (MULTICELL set ELEMENT), {spelling:?}");
        assert!(
            matches!(peel(any_column(&rows, "sw", &ctx)), Value::Set(_)),
            "{ctx}: the column must decode to Value::Set"
        );
        assert_wide_fully_decoded(full_udt(&rows, "sw", &ctx), &ctx);
    }
}

/// AC3 route 5 — a MULTICELL MAP's UDT KEY (`mw`), the last of AC3's four named
/// routes, and the one that needed #3612 as well as this change.
///
/// # This test was a CHARACTERIZATION PIN until #3612 landed, and it fired
///
/// It used to assert the OPPOSITE: that `mw`'s key stayed an opaque
/// `Value::Blob`, because `parse_cell_path_key` matched a closed scalar-only
/// allowlist and fell back to `Value::Blob` for a frozen UDT — so on this route
/// the UDT never became a `Value::Udt` and no field decoder ever ran. That was
/// issue #3612's subject, deliberately out of scope here, and the pin carried the
/// instruction "when #3612 lands this will go RED; assert the decoded fields
/// instead". #3612 merged (`8c503f7cf`, decoding composite cell-path keys
/// structurally) while this branch was in review, the pin went red on the rebase
/// exactly as written, and this is the promised replacement.
///
/// # Why it takes BOTH changes
///
/// #3612 makes the cell-path key a `Value::Udt` at all; #3722 makes that UDT's
/// FIELDS decode instead of arriving as opaque bytes. Neither alone gets this
/// column right, which is why the assertion lives here rather than in #3612 — and
/// why this fixture's 16-field UDT is the subject: #3612's own coverage cannot
/// distinguish "the key is a Udt" from "the key's fields are correct".
#[tokio::test]
async fn multicell_map_udt_key_decodes_every_field() {
    let rows = rows(Spelling::CqlShort).await;
    let ctx = "column mw (MULTICELL map cell-path KEY)";
    match peel(any_column(&rows, "mw", ctx)) {
        Value::Map(pairs) => {
            assert!(!pairs.is_empty(), "{ctx}: decoded ZERO pairs");
            assert!(
                !pairs.iter().any(|(k, _)| matches!(peel(k), Value::Blob(_))),
                "{ctx}: a cell-path UDT key is still an opaque Blob — #3612 has landed, \
                 so this is a regression, not the recorded gap"
            );
            assert_wide_fully_decoded(full_udt(&rows, "mw", ctx), ctx);
        }
        other => panic!("{ctx}: expected Value::Map, got {}", variant(other)),
    }
}

/// CASE FLOOR (issue #3544's idiom, and this file has already needed it).
///
/// While flipping the `mw` pin, a span-replacing edit silently DELETED
/// `multicell_set_udt_element_decodes_every_field_both_spellings` and the suite
/// reported a cheerful `5 passed` — a green tally over a shrunken suite. Assert
/// the roster so a future edit that drops a case FAILS instead of passing quietly.
#[test]
fn every_case_in_this_file_is_still_present() {
    const EXPECTED: &[&str] = &[
        "top_level_frozen_udt_column_decodes_every_field_both_spellings",
        "frozen_map_udt_key_decodes_every_field_cql_short_spelling",
        "frozen_set_udt_element_decodes_every_field_cql_short_spelling",
        "null_udt_fields_stay_null_and_populated_siblings_still_decode",
        "multicell_set_udt_element_decodes_every_field_both_spellings",
        "multicell_map_udt_key_decodes_every_field",
    ];
    let src = include_str!("issue_3722_udt_field_type_fidelity.rs");
    for name in EXPECTED {
        assert!(
            src.contains(&format!("async fn {name}(")),
            "case `{name}` is GONE from this file — a green tally over a shrunken \
             suite proves nothing. Restore it, or remove it from EXPECTED with a \
             stated reason."
        );
    }
    // The needle is SPLIT so this guard cannot match its own source line — it
    // counted itself and reported 7-of-6 on the first attempt.
    // The needle is SPLIT so this guard cannot match its own source line, and it
    // is matched against WHOLE TRIMMED LINES rather than as a substring: a
    // substring count also picked up the mention inside this test's own assert
    // message, and reported 7-of-6 twice before this was anchored.
    let needle = concat!("#[tokio", "::test]");
    let declared = src.lines().filter(|l| l.trim() == needle).count();
    assert_eq!(
        declared,
        EXPECTED.len(),
        "this file declares {declared} async test attributes but EXPECTED lists {} — \
         add the new case to EXPECTED so the floor rises with the suite",
        EXPECTED.len()
    );
}
