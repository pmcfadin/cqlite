//! The EMPTY-BUFFER sentinel's declared type (issue #3805).
//!
//! Split out of `types.rs` under the campsite rule (epic #1116); the
//! [`crate::types::Value::Empty`] variant itself, and its ordering/rendering
//! arms, stay beside the rest of `Value`.

use crate::schema::CqlType;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Declared CQL type of a [`Value::Empty`] sentinel — the type families for which
/// Cassandra treats the EMPTY BUFFER as a legal, meaningless-valued encoding
/// (issue #3805).
///
/// # Why a dedicated, CLOSED tag rather than [`CqlType`]
///
/// Two independent reasons, both load-bearing:
///
/// 1. **Closure is a correctness property.** Cassandra draws a hard line between
///    a family whose `validate()` ACCEPTS an empty buffer and one whose
///    `validate()` THROWS on it. At `cassandra-5.0.8`, `int` is spelled
///    `if (accessor.size(value) != 4 && !accessor.isEmpty(value)) throw` —
///    `serializers/Int32Serializer.java:40-44`, whose diagnostic literally reads
///    *"Expected 4 or 0 byte int"* — while `tinyint`, `smallint`, `date` and
///    `time` are spelled as a BARE `!= N` with no escape clause
///    (`serializers/ByteSerializer.java:40-44`,
///    `serializers/ShortSerializer.java:40-44`,
///    `serializers/SimpleDateSerializer.java:118-122`,
///    `serializers/TimeSerializer.java:71-75`). For those four an empty cell path
///    is CORRUPTION on Cassandra's own terms —
///    `schema/ColumnMetadata.java:457-467` (`validateCellPath`) would itself
///    reject it. A closed tag makes `Empty(tinyint)` UNCONSTRUCTIBLE; a
///    `Box<CqlType>` payload would happily admit it.
/// 2. **The [`Value`] size pin.** `size_of::<CqlType>()` is **48, measured**
///    (its widest variant is `Udt(String, Vec<(String, CqlType)>)` = 24 + 24,
///    with a niche-packed discriminant), so an inline `CqlType` would take
///    `Value` to **56** and break the 40-byte ceiling in `types.rs`. A
///    `Box<CqlType>` would fit but costs a heap allocation per sentinel and buys
///    nothing over a 1-byte tag. This tag is fieldless, so
///    `size_of::<EmptyValueType>()` is 1 and `Value` stays at 40 (both measured;
///    pinned in `cqlite-core/tests/issue_3805_empty_value_sentinel.rs`, which
///    also pins the load-bearing inequality `size_of::<CqlType>() > 32` rather
///    than trusting this prose).
///
/// # Membership rule (source-derived, not curated)
///
/// A family is admitted iff **both** hold at `cassandra-5.0.8`:
///
/// * its `validate()` accepts the empty buffer (so the bytes are legal data
///   Cassandra would have read), **and**
/// * its `deserialize()` maps the empty buffer to `null`, i.e. empty is
///   MEANINGLESS for it — `TypeSerializer.java:71-74` (`isNull` == `buffer ==
///   null || accessor.isEmpty(buffer)`) is the declared base contract, and
///   `AbstractType.java:455-461` (`isEmptyValueMeaningless`) names the property.
///
/// The second clause is what excludes `text`/`ascii`/`varchar`/`blob`: those
/// OVERRIDE `isNull` precisely to say an empty buffer is a real value —
/// `serializers/BytesSerializer.java:57-62` (*"is not \"null\" for bytes types,
/// it is byte[0]"*) and `serializers/AbstractTextSerializer.java:72-77`. CQLite
/// already represents those natively as `Text(Bytes::new())` /
/// `Blob(Bytes::new())`, and a second representation would create two spellings
/// of one value.
///
/// `counter` is admitted from the source only (`CounterSerializer extends
/// LongSerializer` and adds nothing — `serializers/CounterSerializer.java:20-23`);
/// it is not reachable as a map key, since CQL forbids a `counter` collection
/// element.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EmptyValueType {
    /// CQL `int` — `serializers/Int32Serializer.java:30-33`, `:40-44`.
    Int,
    /// CQL `bigint` — `serializers/LongSerializer.java:30-33`, `:40-44`.
    BigInt,
    /// CQL `counter` — `serializers/CounterSerializer.java:20-23` (inherits
    /// `LongSerializer` wholesale). Source-only: not map-key reachable.
    Counter,
    /// CQL `float` (4 bytes on the wire; CQLite's [`Value::Float32`]) —
    /// `serializers/FloatSerializer.java:30-36`, `:43-47`.
    Float,
    /// CQL `double` (8 bytes on the wire; CQLite's [`Value::Float`]) —
    /// `serializers/DoubleSerializer.java:30-35`, `:42-46`.
    Double,
    /// CQL `timestamp` — `serializers/TimestampSerializer.java:137-140`, `:184-188`.
    Timestamp,
    /// CQL `uuid` — `serializers/UUIDSerializer.java:31-34`, `:42-47`.
    Uuid,
    /// CQL `timeuuid` — `utils/TimeUUID.java:339-342`, `:306-316` (there is NO
    /// `serializers/TimeUUIDSerializer.java` at this tag).
    TimeUuid,
    /// CQL `boolean` — `serializers/BooleanSerializer.java:32-38`; its `validate`
    /// (`:46-50`) rejects only `size > 1`, so `0` passes without needing an
    /// `isEmpty` escape clause at all.
    Boolean,
    /// CQL `inet` — `serializers/InetAddressSerializer.java:32-45` and `:52-55`,
    /// which use the EARLY-RETURN spelling (`if (isEmpty) return;`) rather than
    /// the `&& !isEmpty` conjunct.
    Inet,
    /// CQL `decimal`. **Admitted, and this corrects a claim committed elsewhere
    /// in this repository.** `serializers/DecimalSerializer.java:58-63` throws
    /// only `if (!accessor.isEmpty(value) && accessor.size(value) < 4)` and its
    /// message reads *"Expected 0 or at least 4 bytes"*; `:31-34` null-guards
    /// empty BEFORE the `getInt(value, 0)` that would underflow. So empty is
    /// explicitly legal, in the same escape-clause family as `int` — NOT
    /// "corrupt because a decimal needs 4 bytes". Cassandra 5.0.2 wrote an empty
    /// `decimal` map key and `sstabledump` rendered it `"path" : [ "" ]`
    /// (`docs/round-artifacts/issue-3805-cassandra-oracle.md` §4b.4, §4c(a)).
    Decimal,
    /// CQL `varint` — `serializers/IntegerSerializer.java:31-34` returns `null`
    /// on empty and its `validate` body is the comment `// no invalid integers.`,
    /// so everything passes.
    Varint,
}

impl EmptyValueType {
    /// The CQL type this sentinel declares.
    ///
    /// Total by construction: every admitted family has a [`CqlType`]
    /// counterpart, so there is no fallback arm to mis-attribute.
    #[must_use]
    pub fn cql_type(self) -> CqlType {
        match self {
            EmptyValueType::Int => CqlType::Int,
            EmptyValueType::BigInt => CqlType::BigInt,
            EmptyValueType::Counter => CqlType::Counter,
            EmptyValueType::Float => CqlType::Float,
            EmptyValueType::Double => CqlType::Double,
            EmptyValueType::Timestamp => CqlType::Timestamp,
            EmptyValueType::Uuid => CqlType::Uuid,
            EmptyValueType::TimeUuid => CqlType::TimeUuid,
            EmptyValueType::Boolean => CqlType::Boolean,
            EmptyValueType::Inet => CqlType::Inet,
            EmptyValueType::Decimal => CqlType::Decimal,
            EmptyValueType::Varint => CqlType::Varint,
        }
    }

    /// The lowercase CQL type name, as it appears in a schema.
    #[must_use]
    pub fn cql_name(self) -> &'static str {
        match self {
            EmptyValueType::Int => "int",
            EmptyValueType::BigInt => "bigint",
            EmptyValueType::Counter => "counter",
            EmptyValueType::Float => "float",
            EmptyValueType::Double => "double",
            EmptyValueType::Timestamp => "timestamp",
            EmptyValueType::Uuid => "uuid",
            EmptyValueType::TimeUuid => "timeuuid",
            EmptyValueType::Boolean => "boolean",
            EmptyValueType::Inet => "inet",
            EmptyValueType::Decimal => "decimal",
            EmptyValueType::Varint => "varint",
        }
    }

    /// The declared type of an empty buffer, from a [`CqlType`] — `None` when
    /// that type does NOT admit an empty buffer.
    ///
    /// This is the ONE place the legal/corruption line is drawn, and it is drawn
    /// on `validate()` (never on decodability): all four refused families'
    /// `deserialize` ALSO returns `null` on empty
    /// (`serializers/ByteSerializer.java:30-33`,
    /// `serializers/ShortSerializer.java:30-33`,
    /// `serializers/SimpleDateSerializer.java:50-53`,
    /// `serializers/TimeSerializer.java:32-35`), so a reader keyed on
    /// decodability would silently accept bytes Cassandra's own
    /// `validateCellPath` throws on.
    ///
    /// `text`/`ascii`/`varchar`/`blob` return `None` for the OTHER reason — an
    /// empty buffer is a legal, MEANINGFUL value there, represented natively as
    /// `Text(Bytes::new())` / `Blob(Bytes::new())`, never as a sentinel.
    #[must_use]
    pub fn for_cql_type(ty: &CqlType) -> Option<EmptyValueType> {
        match ty {
            CqlType::Int => Some(EmptyValueType::Int),
            CqlType::BigInt => Some(EmptyValueType::BigInt),
            CqlType::Counter => Some(EmptyValueType::Counter),
            CqlType::Float => Some(EmptyValueType::Float),
            CqlType::Double => Some(EmptyValueType::Double),
            CqlType::Timestamp => Some(EmptyValueType::Timestamp),
            CqlType::Uuid => Some(EmptyValueType::Uuid),
            CqlType::TimeUuid => Some(EmptyValueType::TimeUuid),
            CqlType::Boolean => Some(EmptyValueType::Boolean),
            CqlType::Inet => Some(EmptyValueType::Inet),
            CqlType::Decimal => Some(EmptyValueType::Decimal),
            CqlType::Varint => Some(EmptyValueType::Varint),
            // CORRUPTION on Cassandra's own terms — bare `!= N` validate.
            CqlType::TinyInt | CqlType::SmallInt | CqlType::Date | CqlType::Time => None,
            // Empty is a MEANINGFUL value for these; no sentinel.
            CqlType::Text | CqlType::Ascii | CqlType::Varchar | CqlType::Blob => None,
            // Not a scalar family this sentinel speaks for.
            CqlType::Duration
            | CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _)
            | CqlType::Frozen(_)
            | CqlType::Custom(_) => None,
        }
    }

    /// THE ONE ADMISSION CHECK every WRITE path shares: may a sentinel tagged
    /// `self` be serialized (as zero bytes) into a position whose DECLARED type
    /// is `declared`?
    ///
    /// Two refusals, both of them a caller bug rather than something to paper
    /// over by inferring from the bytes (no-heuristics, issue #28):
    ///
    ///  * the declared type does not admit an empty buffer at all — for
    ///    `tinyint`/`smallint`/`date`/`time` an empty buffer is CORRUPTION on
    ///    Cassandra's own terms (bare `!= N` validate,
    ///    `serializers/ByteSerializer.java:40-44` and siblings;
    ///    `schema/ColumnMetadata.java:457-467` would reject it), and for
    ///    `text`/`blob` an empty buffer is a MEANINGFUL native value that must
    ///    never be spelled as a sentinel;
    ///  * the sentinel's own tag DISAGREES with the declared type, which would
    ///    write bytes that read back as a different type.
    ///
    /// `declared_spelling` is the caller's own rendering of the declared type,
    /// used only in the diagnostic — [`CqlType`] has no `Display`, and a
    /// `{:?}` of it is not what the operator wrote in their schema.
    ///
    /// # Why this lives here and not at a call site
    /// It lives beside the tag TABLE it is derived from, which is derived from
    /// Cassandra's `validate()`; a copy at a call site would be a second opinion
    /// able to drift from that table — the "one fact written twice" shape this
    /// repository removes elsewhere.
    ///
    /// # Exactly ONE write path may legally emit the zero-byte form
    /// The multicell MAP CELL PATH in the SSTable writer
    /// (`storage::sstable::writer::data_writer::encoding::serialize_map_cell_path_key_into`),
    /// and nothing else — pinned by the write-surface census in
    /// `write_surface_census_tests`, which requires exactly one admitting
    /// disposition across every value-serializing function in the crate. An
    /// earlier revision of this comment named a SECOND path, the type-aware
    /// [`crate::storage::serialization::types::TypeSerializer`]; that was the
    /// defect roborev job 452 found. A declared type is necessary and NOT
    /// sufficient: the sentinel also needs a framing context in which a
    /// zero-length buffer means "empty", which a cell value (and a
    /// length-prefixed collection/tuple/UDT component) does not supply. So this
    /// check answers only the TYPE half; the caller owns the framing half.
    pub fn check_admits(self, declared: &CqlType, declared_spelling: &str) -> Result<()> {
        match EmptyValueType::for_cql_type(declared) {
            Some(admitted) if admitted == self => Ok(()),
            Some(admitted) => Err(Error::InvalidInput(format!(
                "empty-buffer sentinel declares type `{}` but the declared type \
                 `{declared_spelling}` admits only `{}` (issue #3805)",
                self.cql_name(),
                admitted.cql_name()
            ))),
            None => Err(Error::InvalidInput(format!(
                "type `{declared_spelling}` does not admit an empty buffer, so an \
                 empty-buffer sentinel cannot be serialized for it (issue #3805)"
            ))),
        }
    }
}
