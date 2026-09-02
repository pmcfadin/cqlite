//! The per-SCALAR decode rules the `CqlType` decoder needs: a scalar's canonical CQL
//! short form, the widths its serialized value may take, and whether an EMPTY value
//! is a value or a NULL (issue #3631).
//!
//! Split out of `typed_value.rs` under the campsite rule (epic #1116) along a real
//! responsibility boundary: `typed_value.rs` walks the STRUCTURE of a value
//! (collections, tuples, UDTs, the exhaustion assert) while these three tables state
//! what Cassandra's per-type `validate` / `deserialize` implementations accept for a
//! single scalar. Each is read at the pinned tag, per type — never inferred from
//! CQLite's own prior output (#3041).

use super::super::*;

impl V5CompressedLegacyParser {
    /// The EXACT serialized width of a fixed-width scalar, or `None` when the type is
    /// variable-width (`text`, `blob`, `varint`, `decimal`, `duration`, `inet`).
    ///
    /// Authority: `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/`, whose
    /// fixed-width `AbstractType.validate` implementations accept the exact width OR
    /// an EMPTY buffer and reject everything else (e.g. `Int32Type.validate`:
    /// `if (bytes.remaining() != 4 && bytes.remaining() != 0) throw`). The
    /// type-string decoder this module delegates to bounds-checks with `<`, so it
    /// happily reads a 4-byte int out of a 9-byte frame and drops the rest — the same
    /// silent-discard class the exhaustion assert exists to refuse, one level down.
    pub(super) fn fixed_scalar_width(short: &str) -> Option<usize> {
        Some(match short {
            "boolean" | "tinyint" => 1,
            "smallint" => 2,
            "int" | "float" | "date" => 4,
            "bigint" | "counter" | "double" | "time" | "timestamp" => 8,
            "uuid" | "timeuuid" => 16,
            _ => return None,
        })
    }

    /// Whether an EMPTY serialized value is a VALUE for `short`, rather than NULL.
    ///
    /// # Measured at the pinned tag, serializer by serializer (issue #3631)
    /// A zero-length UDT field carries `ByteBufferUtil.EMPTY_BYTE_BUFFER`, so the
    /// question is what Cassandra's `deserialize` returns for it. Read at
    /// `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/`:
    ///
    /// * `Int32Serializer`, `LongSerializer`, `ShortSerializer`, `ByteSerializer`,
    ///   `BooleanSerializer`, `FloatSerializer`, `DoubleSerializer`, `UUIDSerializer`,
    ///   `TimeUUIDSerializer`, `TimestampSerializer`, `SimpleDateSerializer`,
    ///   `TimeSerializer`, `DecimalSerializer`, `IntegerSerializer` (varint),
    ///   `InetAddressSerializer` and `DurationSerializer` all read
    ///   `accessor.isEmpty(value) ? null : …` — i.e. **empty means NULL**. Note that
    ///   this is WIDER than `AbstractType.isEmptyValueMeaningless()`, which
    ///   `ShortType`/`ByteType`/`SimpleDateType`/`TimeType`/`DurationType` do not
    ///   override even though their serializers return null; the serializer is the
    ///   authority for what the bytes MEAN, so it is what this predicate follows.
    /// * `UTF8Serializer`/`AsciiSerializer` (text/ascii/varchar) and `BytesSerializer`
    ///   (blob) have no such guard: an empty buffer is the empty string / empty blob,
    ///   a real value.
    ///
    /// Structured types (list/set/map/tuple/udt) are not scalars and never reach here;
    /// their empty semantics are the empty collection / all-null components, spelled by
    /// the arms in [`Self::parse_typed_value_reporting`].
    ///
    /// This replaced `create_empty_value_for_type`, whose `_ =>` arm was an EMPTY BLOB
    /// for every type it did not enumerate — so an empty `int`, an empty `tuple` and an
    /// empty nested UDT all surfaced as `Blob([])`, which is #3631 criterion 5's silent
    /// degradation in the zero-length case.
    pub(super) fn empty_is_a_value(short: &str) -> bool {
        matches!(short, "text" | "ascii" | "varchar" | "blob")
    }

    /// A short, allocation-free label for `ty`, for the consumption assert's message.
    pub(super) fn typed_value_label(ty: &CqlType) -> &'static str {
        match ty {
            CqlType::Frozen(_) => "frozen",
            CqlType::List(_) => "list",
            CqlType::Set(_) => "set",
            CqlType::Map(_, _) => "map",
            CqlType::Tuple(_) => "tuple",
            CqlType::Udt(_, _) | CqlType::Custom(_) => "udt",
            scalar => Self::cql_scalar_short_form(scalar).unwrap_or("unknown"),
        }
    }

    /// The canonical CQL short form of a SCALAR `CqlType`, or `None` when `ty` is not
    /// a scalar this decoder can name.
    ///
    /// The returned strings are the arm labels of `parse_value_from_raw_bytes`, which
    /// implements each scalar's on-disk byte layout. Deliberately exhaustive over
    /// `CqlType` with no wildcard, so a NEW variant is a compile error here rather
    /// than a silent blob at run time.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn cql_scalar_short_form(
        ty: &CqlType,
    ) -> Option<&'static str> {
        Some(match ty {
            CqlType::Boolean => "boolean",
            CqlType::TinyInt => "tinyint",
            CqlType::SmallInt => "smallint",
            CqlType::Int => "int",
            CqlType::BigInt => "bigint",
            CqlType::Counter => "counter",
            CqlType::Float => "float",
            CqlType::Double => "double",
            CqlType::Decimal => "decimal",
            CqlType::Text => "text",
            CqlType::Ascii => "ascii",
            CqlType::Varchar => "varchar",
            CqlType::Blob => "blob",
            CqlType::Timestamp => "timestamp",
            CqlType::Date => "date",
            CqlType::Time => "time",
            CqlType::Uuid => "uuid",
            CqlType::TimeUuid => "timeuuid",
            CqlType::Inet => "inet",
            CqlType::Duration => "duration",
            CqlType::Varint => "varint",
            // Structured types are handled by `parse_typed_value`'s own arms and must
            // never reach the scalar delegation.
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _)
            | CqlType::Frozen(_)
            | CqlType::Custom(_) => return None,
        })
    }
}
