//! Issue #3805 — the EMPTY-BUFFER sentinel [`Value::Empty`]: present, distinct
//! from `Null` and from the type's zero value, ordered first, zero-length on the
//! wire.
//!
//! # What is under test, and what the oracle is
//!
//! A non-frozen `map<K, V>` is multicell: each entry's KEY travels in its cell's
//! CellPath, framed as `[unsigned VInt length][bare serialized key]`
//! (`db/marshal/CollectionType.java:361-382` → `utils/ByteBufferUtil.java:356-360`,
//! `:382-389`, at the pinned tag `cassandra-5.0.8`). The length is UNSIGNED and
//! `CellPath.create` asserts non-null (`db/rows/CellPath.java:44-48`), so a
//! **zero-length cell path can only mean the key's serialized form is the EMPTY
//! buffer** — never "absent", never "null". Cassandra accepts it
//! (`cql3/Maps.java:342-345`, `:426-429` — every map-key guard is a REFERENCE-null
//! or sentinel-IDENTITY check, never a length test), writes it
//! (`cql3/UpdateParameters.java:164-175`, `db/rows/Cell.java:300-301`), orders it
//! (`db/marshal/Int32Type.java:61-71`) and reads it back
//! (`db/rows/Cell.java:324-326`).
//!
//! **Every expectation below is derived from Cassandra source at
//! `cassandra-5.0.8` or from measured `sstabledump` output — never from CQLite's
//! prior behaviour** (CLAUDE.md, *"a CQLite `file:line` is NEVER format
//! authority"*). The full derivation, with both authorities, is committed at
//! `docs/round-artifacts/issue-3805-cassandra-oracle.md`.
//!
//! # Scope
//!
//! This file covers the SENTINEL AND ITS SEMANTICS — the representation, its
//! distinctness, its ordering, and the zero-length round trip. The DECODER
//! WIRING (making the row decoder emit this sentinel for a zero-length cell
//! path) is a separate slice and is NOT exercised here.

use cqlite_core::schema::CqlType;
use cqlite_core::types::{EmptyValueType, Value};

/// Every family the sentinel admits, paired with the type's ZERO value — the
/// second wrong answer this sentinel exists to rule out (the first being
/// `Null`).
///
/// The membership of this table is source-derived, not curated: a family is
/// admitted iff its `validate()` accepts an empty buffer AND its
/// `deserialize()` maps an empty buffer to `null` (empty is MEANINGLESS for
/// it). See [`EmptyValueType`] for the per-family citations.
///
/// `Float`/`Double` are named in CQL widths: CQL `float` is 4 bytes (CQLite's
/// `Value::Float32`) and CQL `double` is 8 bytes (CQLite's `Value::Float`).
fn admitted_families() -> Vec<(EmptyValueType, &'static str, Value)> {
    vec![
        (EmptyValueType::Int, "int", Value::Integer(0)),
        (EmptyValueType::BigInt, "bigint", Value::BigInt(0)),
        (EmptyValueType::Counter, "counter", Value::Counter(0)),
        (EmptyValueType::Float, "float", Value::Float32(0.0)),
        (EmptyValueType::Double, "double", Value::Float(0.0)),
        (EmptyValueType::Timestamp, "timestamp", Value::Timestamp(0)),
        (EmptyValueType::Uuid, "uuid", Value::Uuid([0u8; 16])),
        (EmptyValueType::TimeUuid, "timeuuid", Value::Uuid([0u8; 16])),
        (EmptyValueType::Boolean, "boolean", Value::Boolean(false)),
        (EmptyValueType::Inet, "inet", Value::inet(vec![0, 0, 0, 0])),
        (
            EmptyValueType::Decimal,
            "decimal",
            Value::Decimal {
                scale: 0,
                unscaled: vec![0],
            },
        ),
        (EmptyValueType::Varint, "varint", Value::varint(vec![0])),
    ]
}

/// A spread of NON-EMPTY values of each family, INCLUDING the type's minimum,
/// because `Int32Type.compareCustom` puts the empty buffer strictly before
/// every non-empty value *including `Integer.MIN_VALUE`*
/// (`db/marshal/Int32Type.java:61-71`).
fn non_empty_values(ty: EmptyValueType) -> Vec<Value> {
    match ty {
        EmptyValueType::Int => vec![
            Value::Integer(i32::MIN),
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(i32::MAX),
        ],
        EmptyValueType::BigInt => vec![
            Value::BigInt(i64::MIN),
            Value::BigInt(0),
            Value::BigInt(i64::MAX),
        ],
        EmptyValueType::Counter => vec![
            Value::Counter(i64::MIN),
            Value::Counter(0),
            Value::Counter(i64::MAX),
        ],
        EmptyValueType::Float => vec![
            Value::Float32(f32::NEG_INFINITY),
            Value::Float32(-0.0),
            Value::Float32(0.0),
            Value::Float32(f32::MAX),
        ],
        EmptyValueType::Double => vec![
            Value::Float(f64::NEG_INFINITY),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(f64::MAX),
        ],
        EmptyValueType::Timestamp => vec![
            Value::Timestamp(i64::MIN),
            Value::Timestamp(0),
            Value::Timestamp(i64::MAX),
        ],
        EmptyValueType::Uuid | EmptyValueType::TimeUuid => {
            vec![Value::Uuid([0u8; 16]), Value::Uuid([0xff; 16])]
        }
        EmptyValueType::Boolean => vec![Value::Boolean(false), Value::Boolean(true)],
        EmptyValueType::Inet => vec![
            Value::inet(vec![0, 0, 0, 0]),
            Value::inet(vec![10, 0, 0, 1]),
            Value::inet(vec![0xff; 16]),
        ],
        EmptyValueType::Decimal => vec![
            Value::Decimal {
                scale: 0,
                unscaled: vec![0],
            },
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x7f, 0xff],
            },
        ],
        EmptyValueType::Varint => vec![
            Value::varint(vec![0]),
            Value::varint(vec![0x80]),
            Value::varint(vec![0x7f, 0xff]),
        ],
    }
}

/// A CASE FLOOR on the table (CLAUDE.md #3544): an emptied or silently shrunk
/// table would otherwise let every loop below pass having asserted nothing.
/// The named families are the ones the owner ruling enumerates, plus the two
/// the source admits on their own `validate()` spelling.
#[test]
fn the_admitted_family_table_covers_every_family_the_source_admits() {
    let families = admitted_families();
    assert_eq!(
        families.len(),
        12,
        "family table shrank or grew — re-derive membership from validate()/deserialize() \
         at cassandra-5.0.8 before changing this floor"
    );
    for required in [
        "int",
        "bigint",
        "float",
        "double",
        "timestamp",
        "uuid",
        "timeuuid",
        "boolean",
        "inet",
        "decimal",
        "counter",
        "varint",
    ] {
        assert!(
            families.iter().any(|(ty, _, _)| ty.cql_name() == required),
            "family `{required}` is missing from the admitted table"
        );
    }
    // The tag's own name must agree with the table's label, or every other
    // assertion here is about the wrong family.
    for (ty, name, _) in &families {
        assert_eq!(&ty.cql_name(), name, "tag/label disagreement for {ty:?}");
    }
}

// ---------------------------------------------------------------------------
// (1) DISTINCTNESS — not `Null`, not the type's zero value
// ---------------------------------------------------------------------------

/// `Null` is ruled out three independent ways at `cassandra-5.0.8`: a null map
/// key is ILLEGAL CQL (`cql3/Maps.java:342-343` *"null is not supported inside
/// collections"*, `:426-427` / `:510-511` *"Invalid null map key"*); the
/// comparator gives the empty buffer a UNIQUE first position rather than
/// treating it as anything else (`db/marshal/Int32Type.java:61-71`); and the
/// driver on the path a user observes hands back a PRESENT `EmptyValue`
/// sentinel explicitly distinct from `None` (measured on Cassandra 5.0.2 —
/// oracle §4b.3).
#[test]
fn a_sentinel_is_never_equal_to_null_and_is_not_null() {
    for (ty, name, _) in admitted_families() {
        let empty = Value::Empty(ty);
        assert_ne!(empty, Value::Null, "Empty({name}) collapsed onto Null");
        assert!(
            !empty.is_null(),
            "Empty({name}) reported is_null(); the empty⇒null contract of \
             TypeSerializer.java:71-74 is a VALUE-path property and does not \
             transfer to a key"
        );
    }
}

/// The type's zero value is ruled out because `0` has a distinct 4-byte
/// encoding (`serializers/Int32Serializer.java:35-38`) and a distinct sort
/// position (`db/marshal/Int32Type.java:61-71`), so collapsing empty onto it
/// would COLLIDE with a genuine `0` key in the same map — the fixture writes
/// both, precisely so that collapse is detectable.
#[test]
fn a_sentinel_is_never_equal_to_its_types_zero_value() {
    for (ty, name, zero) in admitted_families() {
        let empty = Value::Empty(ty);
        assert_ne!(
            empty, zero,
            "Empty({name}) collapsed onto the zero value {zero:?}"
        );
        assert_ne!(zero, empty, "asymmetric equality for {name}");
    }
}

/// Two sentinels of DIFFERENT declared types are different values — the type is
/// carried, not erased.
#[test]
fn sentinels_of_different_declared_types_are_distinct() {
    let families = admitted_families();
    for (i, (a, name_a, _)) in families.iter().enumerate() {
        for (b, name_b, _) in families.iter().skip(i + 1) {
            assert_ne!(
                Value::Empty(*a),
                Value::Empty(*b),
                "Empty({name_a}) == Empty({name_b})"
            );
        }
    }
}

/// The sentinel reports its DECLARED type, which is what makes its ordering
/// decidable without a schema lookup.
#[test]
fn a_sentinel_carries_its_declared_type() {
    for (ty, name, _) in admitted_families() {
        assert_eq!(
            Value::Empty(ty).data_type(),
            ty.cql_type(),
            "declared type lost for {name}"
        );
        assert_eq!(
            EmptyValueType::for_cql_type(&ty.cql_type()),
            Some(ty),
            "CqlType round trip failed for {name}"
        );
    }
}

// ---------------------------------------------------------------------------
// (2) ORDERING — strictly before every non-empty value of the type
// ---------------------------------------------------------------------------

/// `db/marshal/Int32Type.java:61-71`:
///
/// ```java
/// if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
///     return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));
/// ```
///
/// With only the LEFT empty this is `Boolean.compare(false, true) == -1`.
/// Measured on real Cassandra-5.0.2 bytes for four independent key types: the
/// empty key is FIRST in every column of the dump (oracle §4b.4).
#[test]
fn a_sentinel_sorts_strictly_before_every_non_empty_value_of_its_type() {
    use std::cmp::Ordering;
    for (ty, name, _) in admitted_families() {
        let empty = Value::Empty(ty);
        let others = non_empty_values(ty);
        assert!(
            !others.is_empty(),
            "no non-empty values listed for {name} — the case would pass vacuously"
        );
        for other in others {
            assert_eq!(
                empty.partial_cmp(&other),
                Some(Ordering::Less),
                "Empty({name}) did not sort before {other:?}"
            );
            assert_eq!(
                other.partial_cmp(&empty),
                Some(Ordering::Greater),
                "{other:?} did not sort after Empty({name})"
            );
        }
    }
}

/// Two empty buffers of one type are EQUAL, which is the other branch of
/// `compareCustom`: both empty ⇒ `Boolean.compare(true, true) == 0`. It also
/// means a map can carry AT MOST ONE empty key — one empty buffer, one sort
/// position (oracle §5, non-decision 5).
#[test]
fn two_sentinels_of_one_type_compare_equal() {
    use std::cmp::Ordering;
    for (ty, name, _) in admitted_families() {
        assert_eq!(
            Value::Empty(ty).partial_cmp(&Value::Empty(ty)),
            Some(Ordering::Equal),
            "Empty({name}) is not equal to itself under the comparator"
        );
        assert_eq!(Value::Empty(ty), Value::Empty(ty), "PartialEq disagreement");
    }
}

/// Sorting a column of keys must place the sentinel first — the assertion the
/// measured dump licenses directly (`"path" : [ "" ]` precedes `"42"`, `"99"`,
/// `"10.0.0.1"` and `"k"`, oracle §4b.4).
#[test]
fn sorting_a_key_column_puts_the_sentinel_first() {
    for (ty, name, _) in admitted_families() {
        let mut keys = non_empty_values(ty);
        keys.push(Value::Empty(ty));
        keys.sort_by(|a, b| a.partial_cmp(b).expect("total over one key type"));
        assert_eq!(
            keys.first(),
            Some(&Value::Empty(ty)),
            "Empty({name}) did not sort first in {keys:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// (3) BYTE-EXACT ROUND TRIP — a ZERO-LENGTH buffer
// ---------------------------------------------------------------------------

/// The write path's cell-value serializer must append NOTHING: the sentinel's
/// serialized form is the empty buffer, and the length lives in the enclosing
/// framing (an unsigned VInt for a cell path,
/// `db/marshal/CollectionType.java:361-382`; the `HAS_EMPTY_VALUE_MASK` flag bit
/// for a cell value, `db/rows/Cell.java:264`) — never in these bytes.
#[test]
fn the_type_aware_serializer_emits_exactly_zero_bytes() {
    let serializer = cqlite_core::storage::serialization::types::TypeSerializer::new();
    for (ty, name, _) in admitted_families() {
        let bytes = serializer
            .serialize_value(&Value::Empty(ty), name)
            .unwrap_or_else(|e| panic!("serializing Empty({name}) failed: {e}"));
        assert!(
            bytes.is_empty(),
            "Empty({name}) serialized to {} bytes, expected 0",
            bytes.len()
        );
    }
}

/// A sentinel whose declared type DISAGREES with the column's type is refused,
/// not silently written — a caller bug is never papered over by inferring from
/// bytes (no-heuristics, issue #28).
#[test]
fn the_type_aware_serializer_refuses_a_tag_column_mismatch() {
    let serializer = cqlite_core::storage::serialization::types::TypeSerializer::new();
    assert!(
        serializer
            .serialize_value(&Value::Empty(EmptyValueType::Int), "bigint")
            .is_err(),
        "an Empty(int) written into a bigint column must be refused"
    );
}

/// REGRESSION, roborev job 448 finding A. The legacy tagged `parser::types`
/// serializer used to emit the declared type's id and no payload for the
/// sentinel, under a comment claiming a byte-exact round trip. The bytes were
/// byte-exact and the VALUE was not: nothing in that format reads back as
/// `Value::Empty(_)` (pinned by the next test), so it REFUSES instead of
/// writing bytes its own reader cannot decode. Every admitted family, because
/// a refusal that holds for `int` and not `varint` is not a refusal.
#[test]
fn the_tagged_serializer_refuses_the_sentinel_for_every_admitted_family() {
    for (ty, name, _) in admitted_families() {
        let err = cqlite_core::parser::types::serialize_cql_value(&Value::Empty(ty))
            .expect_err(&format!(
                "Empty({name}) was serialized by the legacy tagged format, which has \
                 no representation that reads back as the sentinel"
            ));
        let msg = err.to_string();
        for needle in ["4072", "28", name] {
            assert!(
                msg.contains(needle),
                "the refusal of Empty({name}) must name {needle}; got: {msg}"
            );
        }
    }
}

/// THE MEASUREMENT BEHIND THAT REFUSAL, pinned so the rationale cannot rot into
/// the false claim it replaced. For every family the tag admits, the tagged
/// form the old arm would have written is `[type byte]` with no payload — and
/// BOTH of this module's readers reject a zero-length payload, so that byte
/// string decodes to nothing at all, let alone to the sentinel.
///
/// If a future change makes any of these decode, this test reds and the
/// serializer's refusal has to be re-argued rather than silently outlived.
#[test]
fn no_tagged_form_of_the_sentinel_reads_back_as_the_sentinel() {
    use cqlite_core::parser::types::{parse_cql_value, parse_cql_value_raw, CqlTypeId};

    // The wire type id this format uses for each family, declared here rather
    // than read out of the (private) mapping the serializer used, so the test
    // carries its own oracle.
    let wire_ids: Vec<(EmptyValueType, &str, CqlTypeId)> = vec![
        (EmptyValueType::Int, "int", CqlTypeId::Int),
        (EmptyValueType::BigInt, "bigint", CqlTypeId::BigInt),
        (EmptyValueType::Counter, "counter", CqlTypeId::Counter),
        (EmptyValueType::Float, "float", CqlTypeId::Float),
        (EmptyValueType::Double, "double", CqlTypeId::Double),
        (EmptyValueType::Timestamp, "timestamp", CqlTypeId::Timestamp),
        (EmptyValueType::Uuid, "uuid", CqlTypeId::Uuid),
        (EmptyValueType::TimeUuid, "timeuuid", CqlTypeId::Timeuuid),
        (EmptyValueType::Boolean, "boolean", CqlTypeId::Boolean),
        (EmptyValueType::Inet, "inet", CqlTypeId::Inet),
        (EmptyValueType::Decimal, "decimal", CqlTypeId::Decimal),
        (EmptyValueType::Varint, "varint", CqlTypeId::Varint),
    ];
    // The table must cover exactly the admitted set — neither a family that
    // silently escapes the check nor a stale row (#3544 floor + ceiling).
    let admitted: Vec<EmptyValueType> = admitted_families().into_iter().map(|f| f.0).collect();
    assert_eq!(
        wire_ids.len(),
        admitted.len(),
        "the wire-id table has {} rows for {} admitted families",
        wire_ids.len(),
        admitted.len()
    );
    for (ty, name, _) in &wire_ids {
        assert!(
            admitted.contains(ty),
            "{name} is in the wire-id table but is not an admitted family"
        );
    }

    for (_, name, id) in &wire_ids {
        for (reader, outcome) in [
            ("parse_cql_value", parse_cql_value(&[], *id)),
            ("parse_cql_value_raw", parse_cql_value_raw(&[], *id)),
        ] {
            match outcome {
                Err(_) => {}
                Ok((_, v)) => panic!(
                    "{reader} decoded a payload-free {name} into {v:?}; the tagged \
                     serializer's refusal is argued on this being undecodable"
                ),
            }
        }
    }
}

/// The refusal must also hold for a sentinel NESTED in a collection, which is
/// the surface #3805 exists for (a map KEY). That path runs through the private
/// bare-element serializer — the second of the two sites the finding named — so
/// it needs its own coverage: a fix confined to the top-level arm would leave a
/// zero-length element on the wire.
#[test]
fn the_tagged_serializer_refuses_a_sentinel_nested_in_a_collection() {
    let sentinel = Value::Empty(EmptyValueType::Int);
    let cases: Vec<(&str, Value)> = vec![
        (
            "map key",
            Value::Map(vec![(sentinel.clone(), Value::Integer(1))]),
        ),
        (
            "map value",
            Value::Map(vec![(Value::Integer(1), sentinel.clone())]),
        ),
        ("list element", Value::List(vec![sentinel.clone()])),
        ("set element", Value::Set(vec![sentinel.clone()])),
        ("tuple field", Value::Tuple(vec![sentinel.clone()])),
        ("frozen inner", Value::Frozen(Box::new(sentinel.clone()))),
    ];
    for (what, value) in cases {
        assert!(
            cqlite_core::parser::types::serialize_cql_value(&value).is_err(),
            "a sentinel as a {what} must be refused, not written as a zero-length element"
        );
    }
}

// ---------------------------------------------------------------------------
// (4) RENDERING — `""`, because that is what BOTH Cassandra renderers emit
// ---------------------------------------------------------------------------

/// `sstabledump` renders an empty fixed-width cell path as the EMPTY JSON
/// STRING (`tools/JsonTransformer.java:444-458` →
/// `db/marshal/AbstractType.java:146-156` →
/// `serializers/Int32Serializer.java:46-49`, whose `toString(null)` is `""`),
/// and it round-trips (`db/marshal/Int32Type.java:85-89`: `fromString("")`
/// returns EMPTY). `SELECT JSON` agrees (`{"": v}`,
/// `db/marshal/MapType.java:362-388`). Rendering `null` or `0` here would be a
/// parity failure.
#[test]
fn the_rendering_is_the_empty_string_for_every_family() {
    for (ty, name, _) in admitted_families() {
        assert_eq!(
            cqlite_core::util::value_fmt::ValueFormatter::format_value(&Value::Empty(ty)),
            "",
            "Empty({name}) did not render as the empty string"
        );
    }
}

/// REGRESSION, roborev job 438 F1. A JSON object key must be a STRING, so a map
/// key is RENDERED rather than converted, and `QueryRow::to_json` did it with
/// `format!("{}", k)` — i.e. through `Display for Value`, which renders the
/// sentinel as `EMPTY(int)`. The value-path case above could not catch it,
/// because the value path and the KEY path are different code.
///
/// A MAP KEY is exactly the surface #3805 exists for, and Cassandra renders an
/// empty key as `""`: `sstabledump` prints `"path" : [ "" ]`
/// (`tools/JsonTransformer.java:444-458` →
/// `db/marshal/AbstractType.java:146-156`) and `SELECT JSON` yields `{"": v}`
/// (`db/marshal/MapType.java:362-388`), both at `cassandra-5.0.8`.
#[test]
fn a_map_key_sentinel_renders_as_the_empty_json_key_not_a_diagnostic_string() {
    use cqlite_core::query::result::QueryRow;
    use cqlite_core::types::RowKey;

    for (ty, name, _) in admitted_families() {
        let mut row = QueryRow::new(RowKey::new(b"pk".to_vec()));
        row.set(
            "m".to_string(),
            Value::Map(vec![
                (Value::Empty(ty), Value::text("empty")),
                (Value::Integer(42), Value::text("forty-two")),
            ]),
        );

        let json = row.to_json();
        let map = json
            .get("m")
            .and_then(|m| m.as_object())
            .unwrap_or_else(|| panic!("column `m` is not a JSON object for {name}"));

        assert!(
            map.contains_key(""),
            "Empty({name}) map key rendered as {:?}, expected the empty string key",
            map.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            map.get(""),
            Some(&serde_json::json!("empty")),
            "the empty key's VALUE was lost or attached to the wrong key for {name}"
        );
        // The empty key must not collide with, or displace, its non-empty
        // sibling — the fixture writes both for exactly this reason.
        assert_eq!(map.len(), 2, "entry lost or merged for {name}");
        assert!(
            !map.keys().any(|k| k.contains("EMPTY")),
            "a diagnostic Display rendering leaked into a data surface for {name}"
        );
    }
}

// ---------------------------------------------------------------------------
// (5) THE LEGAL/CORRUPTION LINE — keyed on `validate()`, never on decodability
// ---------------------------------------------------------------------------

/// NEGATIVE case. `tinyint`, `smallint`, `date` and `time` are spelled with a
/// BARE `!= N` validate and no `isEmpty` escape clause
/// (`serializers/ByteSerializer.java:40-44`,
/// `serializers/ShortSerializer.java:40-44`,
/// `serializers/SimpleDateSerializer.java:118-122`,
/// `serializers/TimeSerializer.java:71-75`), so for them a zero-length cell
/// path is CORRUPTION on Cassandra's own terms —
/// `schema/ColumnMetadata.java:457-467` (`validateCellPath`) would itself reject
/// it, and CQL refuses to construct one (measured: `blobAsTinyint(0x)` and its
/// three siblings are the ONLY four of 17 probed types that CQL rejects, oracle
/// §4b.1).
///
/// Note the asymmetry that makes this worth pinning: all four families'
/// `deserialize` STILL returns `null` on empty
/// (`serializers/ByteSerializer.java:30-33`,
/// `serializers/ShortSerializer.java:30-33`,
/// `serializers/SimpleDateSerializer.java:50-53`,
/// `serializers/TimeSerializer.java:32-35`), so a reader keyed on
/// *decodability* rather than on *`validate`* would silently accept bytes
/// Cassandra rejects.
#[test]
fn the_four_bare_length_check_families_are_not_admitted() {
    for ty in [
        CqlType::TinyInt,
        CqlType::SmallInt,
        CqlType::Date,
        CqlType::Time,
    ] {
        assert_eq!(
            EmptyValueType::for_cql_type(&ty),
            None,
            "{ty:?} must NOT be admitted: an empty buffer there is corruption on \
             Cassandra's own terms (bare != N validate)"
        );
    }
}

/// NEGATIVE case, for the OTHER reason. `text`/`ascii`/`varchar`/`blob`
/// OVERRIDE `isNull` precisely to say an empty buffer is a REAL value —
/// `serializers/BytesSerializer.java:57-62` (*"is not \"null\" for bytes types,
/// it is byte[0]"*) and `serializers/AbstractTextSerializer.java:72-77`. CQLite
/// already represents those natively (`Text(Bytes::new())` /
/// `Blob(Bytes::new())`), so a sentinel there would be a SECOND spelling of one
/// value.
#[test]
fn the_byte_backed_families_are_not_admitted_because_empty_is_meaningful_there() {
    for ty in [
        CqlType::Text,
        CqlType::Ascii,
        CqlType::Varchar,
        CqlType::Blob,
    ] {
        assert_eq!(
            EmptyValueType::for_cql_type(&ty),
            None,
            "{ty:?} must NOT be admitted: an empty buffer is a meaningful value there"
        );
    }
    // …and the native representation is genuinely distinct from every sentinel.
    assert_ne!(Value::text(""), Value::Empty(EmptyValueType::Int));
    assert_ne!(Value::blob(Vec::new()), Value::Empty(EmptyValueType::Int));
}

/// MECHANIZES THE "ONLY ONE LOSSY PAIR" CLAIM behind the F2 fix (roborev job
/// 438), so it cannot decay into a stale comment.
///
/// `Value::data_type()` is the bridge any declared-type check crosses, and it
/// is LOSSY wherever two CQL types share one `Value` variant. F2 was that
/// shape for uuid/timeuuid. This walks EVERY admitted family and asserts the
/// round trip `tag -> non-empty value -> data_type() -> tag` is exact, with
/// the uuid/timeuuid pair as the ONE declared exception. A new lossy pair —
/// or a new family whose variant is shared — fails here by name instead of
/// silently making some sentinel incomparable in `try_compare_values`.
#[test]
fn data_type_round_trips_for_every_admitted_family_except_the_uuid_pair() {
    let mut exceptions: Vec<&'static str> = Vec::new();
    for (ty, name, zero) in admitted_families() {
        // `zero` is a NON-EMPTY value of the family, which is what a
        // declared-type check is handed at runtime.
        let observed = EmptyValueType::for_cql_type(&zero.data_type());
        if observed == Some(ty) {
            continue;
        }
        exceptions.push(name);
        // Any exception must be inside the 16-byte uuid/timeuuid pair —
        // anything else is a new instance of the F2 class.
        assert!(
            matches!(
                (ty, observed),
                (
                    EmptyValueType::Uuid | EmptyValueType::TimeUuid,
                    Some(EmptyValueType::Uuid) | Some(EmptyValueType::TimeUuid)
                )
            ),
            "family `{name}` does not round-trip through data_type() (observed \
             {observed:?}) and is NOT the declared uuid/timeuuid pair — this is a \
             NEW lossy pair and try_compare_values will refuse its sentinel"
        );
    }
    assert_eq!(
        exceptions,
        vec!["timeuuid"],
        "the set of lossy families changed; the F2 helper in value_ops.rs \
         documents exactly one (timeuuid) and must be revisited"
    );
}

// ---------------------------------------------------------------------------
// (6) THE SIZE PIN — asserted numerically so a future widening is legible
// ---------------------------------------------------------------------------

/// `cqlite-core/src/types.rs` carries
/// `const _: () = assert!(std::mem::size_of::<Value>() <= 40)` (the #1565
/// ratchet), so a widening would already fail the build. This asserts the
/// NUMBER as well, so the measurement is visible in a test log rather than
/// implicit in a compile error, and so a change from 40 is a named failure.
///
/// Measured before this issue: 40. Measured after: 40 — the sentinel's payload
/// is a fieldless 1-byte tag, which fits in the existing padding of the widest
/// (32-byte `Bytes`) variant. An inline `CqlType` payload would have taken
/// `Value` to 56 (`size_of::<CqlType>()` is 48, measured), which is why the tag
/// exists — pinned below as the inequality that actually decides it, so a future
/// `CqlType` that SHRANK past the ceiling would surface as a named failure
/// inviting a re-evaluation rather than leaving a stale comment.
#[test]
fn the_value_layout_pin_is_unmoved_at_40_bytes() {
    assert_eq!(
        std::mem::size_of::<Value>(),
        40,
        "size_of::<Value>() moved; measure and box the next-widest variant \
         rather than bumping the #1565 pin"
    );
    assert_eq!(
        std::mem::size_of::<EmptyValueType>(),
        1,
        "EmptyValueType is no longer a fieldless tag; re-measure the Value pin"
    );
    assert!(
        std::mem::size_of::<CqlType>() > 32,
        "size_of::<CqlType>() now fits inside Value's 40-byte ceiling ({} bytes); \
         re-evaluate whether the sentinel should carry a CqlType directly \
         instead of the EmptyValueType tag",
        std::mem::size_of::<CqlType>()
    );
}
