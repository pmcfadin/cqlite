//! PRIMITIVE Cassandra marshal-type → canonical CQL short-form normalization
//! for the bounded raw-value decoder (campsite split of `raw_value.rs`,
//! epic #1116 / issue #3723).
//!
//! Only the marshal→short-form mapping lives here. The dispatch that consumes
//! it is in `raw_value.rs`; the fixed-width scalar arms it dispatches to are in
//! `fixed_width.rs`.

use super::*;

impl V5CompressedLegacyParser {
    /// Map a PRIMITIVE Cassandra marshal type (e.g.
    /// `org.apache.cassandra.db.marshal.Int32Type`) to the canonical CQL short
    /// form (`"int"`) understood by [`parse_value_from_raw_bytes`]'s match
    /// (issue #1081). Returns `None` for any non-primitive marshal form
    /// (UserType / collection / tuple / reversed / frozen / custom), so the
    /// caller leaves those to the dedicated arms. The suffix set is a *superset*
    /// of the authoritative marshal→`CqlType` mapping in
    /// [`parse_cassandra_type_with_depth`] (no heuristics — issue #28): in
    /// addition to the scalars that mapping enumerates, this also normalizes a
    /// few marshal forms that `parse_cassandra_type_with_depth` routes to
    /// `Custom` (`VarcharType`, `CounterColumnType`, `LexicalUUIDType`,
    /// `ShortType`, `ByteType`). Those extra mappings are required so we can
    /// decode the corresponding scalar UDT field values — e.g. `ShortType`/
    /// `ByteType` are needed to read `smallint`/`tinyint` UDT fields, which
    /// otherwise fall through to the blob default.
    pub(super) fn primitive_marshal_to_cql_short(marshal_type: &str) -> Option<&'static str> {
        // Composite marshal forms carry a `(` after the type name; primitives do
        // not. Reject anything parameterised so we never misread a collection /
        // UDT as a scalar.
        if marshal_type.contains('(') {
            return None;
        }
        let s = marshal_type;
        let short = if s.ends_with("UTF8Type") || s.ends_with("VarcharType") {
            "text"
        } else if s.ends_with("AsciiType") {
            "ascii"
        } else if s.ends_with("Int32Type") {
            "int"
        } else if s.ends_with("LongType") || s.ends_with("CounterColumnType") {
            "bigint"
        } else if s.ends_with("FloatType") {
            "float"
        } else if s.ends_with("DoubleType") {
            "double"
        } else if s.ends_with("BooleanType") {
            "boolean"
        } else if s.ends_with("TimeUUIDType") {
            "timeuuid"
        } else if s.ends_with("UUIDType") || s.ends_with("LexicalUUIDType") {
            "uuid"
        } else if s.ends_with("SimpleDateType") {
            // CQL `date` (`SimpleDateType`) is a 4-byte unsigned days-since-epoch
            // value. This is distinct from the legacy `DateType` handled below.
            "date"
        } else if s.ends_with("DateType") {
            // Legacy Cassandra `DateType` is an 8-byte millis-since-epoch value —
            // the same wire format as `TimestampType`. Mapping it to `date` would
            // wrongly decode only the first 4 bytes, so route it to `timestamp`.
            // NOTE: this `ends_with` arm must follow the `SimpleDateType` arm above
            // because `SimpleDateType` also ends with `DateType`.
            "timestamp"
        } else if s.ends_with("TimestampType") {
            "timestamp"
        } else if s.ends_with("TimeType") {
            "time"
        } else if s.ends_with("DecimalType") {
            "decimal"
        } else if s.ends_with("IntegerType") {
            "varint"
        } else if s.ends_with("DurationType") {
            "duration"
        } else if s.ends_with("ShortType") {
            "smallint"
        } else if s.ends_with("ByteType") {
            "tinyint"
        } else if s.ends_with("InetAddressType") {
            "inet"
        } else if s.ends_with("BytesType") {
            "blob"
        } else {
            return None;
        };
        Some(short)
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    /// Issue #1081: `primitive_marshal_to_cql_short` must normalize every
    /// PRIMITIVE Cassandra marshal type (fully-qualified) to its canonical CQL
    /// short form, and must reject any parameterised/composite marshal form
    /// (anything containing `(` — UDT / collection / reversed) so the
    /// no-heuristics `(`-rejection guard never misreads a composite as a scalar.
    #[test]
    fn primitive_marshal_to_cql_short_maps_scalars_and_rejects_composites() {
        const P: &str = "org.apache.cassandra.db.marshal.";

        // (marshal type name, expected canonical CQL short form)
        let cases: &[(&str, &str)] = &[
            ("UTF8Type", "text"),
            ("AsciiType", "ascii"),
            ("Int32Type", "int"),
            ("LongType", "bigint"),
            ("FloatType", "float"),
            ("DoubleType", "double"),
            ("BooleanType", "boolean"),
            ("UUIDType", "uuid"),
            ("TimeUUIDType", "timeuuid"),
            ("TimestampType", "timestamp"),
            ("SimpleDateType", "date"),
            // Legacy `DateType` is an 8-byte millis-since-epoch value (same wire
            // format as `TimestampType`), NOT the 4-byte CQL `date`
            // (`SimpleDateType`). It must normalize to `timestamp`.
            ("DateType", "timestamp"),
            ("TimeType", "time"),
            ("DecimalType", "decimal"),
            ("IntegerType", "varint"),
            ("DurationType", "duration"),
            ("ShortType", "smallint"),
            ("ByteType", "tinyint"),
            ("InetAddressType", "inet"),
            ("BytesType", "blob"),
        ];

        for (marshal, expected) in cases {
            let full = format!("{}{}", P, marshal);
            assert_eq!(
                V5CompressedLegacyParser::primitive_marshal_to_cql_short(&full),
                Some(*expected),
                "primitive marshal {} should map to {}",
                full,
                expected
            );
        }

        // Parameterised / composite marshal forms must be rejected (return None)
        // by the `(`-guard, leaving them to the dedicated composite arms.
        let composites = [
            "org.apache.cassandra.db.marshal.UserType(...)",
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)",
        ];
        for composite in composites {
            assert_eq!(
                V5CompressedLegacyParser::primitive_marshal_to_cql_short(composite),
                None,
                "composite marshal {} must be rejected by the `(`-guard",
                composite
            );
        }
    }
}
