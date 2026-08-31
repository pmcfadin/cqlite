//! Decoding a MULTICELL collection cell's CELL PATH as a map KEY (issue #3612).
//!
//! A non-frozen `map<K, V>` is multicell: each entry is its own cell and the KEY
//! is carried in that cell's CellPath. Cassandra frames a CellPath as
//! `[VInt length][bare serialized key]`
//! (`CollectionType.CollectionPathSerializer`); the caller in
//! [`super::complex_column`] has already stripped the VInt, so the slice reaching
//! this module IS the serialized key and carries NO length prefix of its own.
//!
//! Historically this site carried its OWN type ladder — an allowlist of six
//! scalar families with a `Value::Blob` default — so a COMPOSITE key
//! (`frozen<udt>`, `tuple<…>`, a frozen collection) and roughly ten further
//! scalar families surfaced as raw bytes, while the FROZEN spelling of the very
//! same map decoded structurally. The fix is to delegate to
//! [`super::V5CompressedLegacyParser::parse_value_from_raw_bytes`] — the
//! structural decoder the SET branch already used for a set member's cell path,
//! whose convention ("the entire slice IS the value") is exactly this one, and
//! whose framing for a tuple/UDT (`[i32 BE len][bytes]` per component, `-1` =
//! null, per Cassandra's `TupleType.buildValue`) is byte-identical to a
//! composite CellPath's.
//!
//! Two properties this module owns, because delegation alone would lose them:
//!
//! 1. **The ORIGINAL-CASE type string is forwarded.**
//!    `primitive_marshal_to_cql_short` matches marshal suffixes CASE-SENSITIVELY
//!    (`s.ends_with("Int32Type")`), so lowercasing before delegation would fail
//!    every marshal-form normalization and land straight back in an opaque blob —
//!    which is precisely the no-schema `Statistics.db` path, where the key type
//!    arrives in marshal form.
//!
//! 2. **Fixed-width keys are validated for EXACT width.**
//!    `parse_value_from_raw_bytes` rejects only UNDER-width input (`< N`) because
//!    its other callers hand it element bytes already bounded by an outer length
//!    prefix, so trailing bytes cannot occur. A CellPath has no such outer bound
//!    here: the whole slice is the key, so an over-long slice is corruption and
//!    must not be decoded from its prefix. Cassandra agrees, and is the authority
//!    (5.0.8, `org.apache.cassandra.serializers.*.validate`): `Int32Serializer`
//!    `size != 4`, `LongSerializer`/`TimestampSerializer`/`DoubleSerializer`/
//!    `TimeSerializer` `!= 8`, `FloatSerializer`/`SimpleDateSerializer` `!= 4`,
//!    `UUIDSerializer` `!= 16`, `ShortSerializer` `!= 2`, `ByteSerializer`
//!    `!= 1`, `BooleanSerializer` `size > 1` — all raising `MarshalException`.
//!    (Cassandra additionally admits a ZERO-length buffer for most of these; a
//!    zero-length path cannot reach here because the caller only decodes a
//!    NON-EMPTY `path_bytes`.)

use super::*;

impl V5CompressedLegacyParser {
    /// Parse a MULTICELL map's cell-path key.
    ///
    /// `data` is the bare serialized key (the CellPath's VInt length prefix is
    /// already stripped by the caller). `type_str` is the map's declared KEY type
    /// in whatever spelling the authoritative source provided — a CQL short form
    /// from the schema (`frozen<collide>`, `int`) or a Cassandra marshal form from
    /// `Statistics.db` (`org.apache.cassandra.db.marshal.UserType(…)`) — and is
    /// forwarded WITH ITS CASE INTACT (see the module header).
    pub(super) fn parse_cell_path_key(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
    ) -> Result<Value> {
        if let Some(expected) = Self::cell_path_key_exact_width(type_str) {
            if data.len() != expected {
                return Err(Error::corruption(format!(
                    "Map key '{}' of type '{}' requires exactly {} bytes, got {}",
                    column_name,
                    type_str,
                    expected,
                    data.len()
                )));
            }
        }
        // depth 0: a cell-path key is a top-level value, exactly as the SET
        // branch treats a set member's cell path.
        let decoded = self.parse_value_from_raw_bytes(data, type_str, column_name, 0)?;
        Ok(Self::unwrap_frozen_cell_path_key(decoded))
    }

    /// Drop the `Value::Frozen` wrapper the structural decoder adds for a
    /// `frozen<…>`-spelled type, so a MULTICELL map key presents EXACTLY as the
    /// FROZEN spelling of the same map presents it.
    ///
    /// MEASURED, not assumed (issue #3612): reading the committed Cassandra
    /// fixture `test_udt_collision.udt_collide`, the frozen control
    /// `fcm frozen<map<frozen<collide>, int>>` yields a BARE `Value::Udt` key,
    /// because [`super::cell_value_complex`] prefers the on-disk marshal element
    /// type (`UserType(…)`, issue #1340) over the schema's `frozen<collide>` and
    /// so never enters the `frozen<` arm. The multicell sibling
    /// `cm map<frozen<collide>, int>` resolves its key type from
    /// `column.data_type`, which DOES carry the `frozen<…>` (or `FrozenType(…)`)
    /// spelling, so delegation would wrap where the control does not — leaving
    /// the two spellings of one map with different internal shapes.
    ///
    /// The wrapper carries no information at this position: CQL requires a
    /// composite map key to be frozen (`map<collide, int>` is rejected;
    /// `map<frozen<collide>, int>` is the only legal spelling), so `Frozen` here
    /// is constant-true. Every render surface already unwraps it transparently
    /// (`ValueFormatter::format_value`, the CLI JSON writer, Arrow's
    /// `unwrap_frozen_value`, and both bindings), so this normalizes the internal
    /// shape rather than changing any rendered output.
    fn unwrap_frozen_cell_path_key(mut value: Value) -> Value {
        while let Value::Frozen(inner) = value {
            value = *inner;
        }
        value
    }

    /// The EXACT byte width a fixed-width cell-path key must have, or `None` for
    /// a variable-width / composite type (where the whole slice is consumed and
    /// no width invariant applies).
    ///
    /// Accepts both spellings of the type. A marshal form is normalized through
    /// the CASE-SENSITIVE [`Self::primitive_marshal_to_cql_short`], which returns
    /// `None` for anything parameterised (`UserType(…)`, `TupleType(…)`,
    /// `FrozenType(…)`, collections), so a composite key is never width-checked.
    fn cell_path_key_exact_width(type_str: &str) -> Option<usize> {
        let short: &str = if type_str.contains("org.apache.cassandra.db.marshal.") {
            Self::primitive_marshal_to_cql_short(type_str)?
        } else if type_str.contains('<') || type_str.contains('(') {
            // A CQL-short composite (`frozen<…>`, `tuple<…>`, `map<…>`): variable width.
            return None;
        } else {
            // A CQL short form. `parse_value_from_raw_bytes` matches on the
            // LOWERCASED spelling, so normalize the same way here or a `"Int"`
            // from a hand-written schema would skip the check it then decodes under.
            return Self::cql_short_exact_width(&type_str.to_ascii_lowercase());
        };
        Self::cql_short_exact_width(short)
    }

    /// The exact width of a canonical lowercase CQL short form, if fixed.
    ///
    /// Kept as a single table so the marshal and short-form routes cannot drift
    /// into two different opinions about a family's width.
    fn cql_short_exact_width(short: &str) -> Option<usize> {
        match short {
            "boolean" | "tinyint" | "byte" => Some(1),
            "smallint" | "short" => Some(2),
            "int" | "float" | "date" => Some(4),
            "bigint" | "counter" | "double" | "timestamp" | "time" => Some(8),
            "uuid" | "timeuuid" => Some(16),
            // Variable-width by definition: text/ascii/varchar, blob/bytes,
            // varint, decimal, inet, duration — plus every composite.
            _ => None,
        }
    }
}
