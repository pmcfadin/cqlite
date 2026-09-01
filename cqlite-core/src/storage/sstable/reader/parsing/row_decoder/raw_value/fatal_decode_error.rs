//! Which element/field decode errors are FATAL to a row read, and which stay
//! TOLERATED (issue #3723).
//!
//! ## Why a discrimination exists at all
//!
//! Two call sites in this decoder have historically been TOLERANT of a failed
//! decode: the complex-column loop in `row_data.rs` (`break`, keeping the cells
//! decoded so far) and the multicell-`set` member decode in `complex_column.rs`
//! (`None`, omitting the member). That tolerance predates issue #3723 and
//! covers a large, unaudited surface of partially-readable rows; converting
//! every decode error into a hard read failure would change behaviour for every
//! one of those paths, far beyond this issue.
//!
//! Issue #3723 added ONE error class that must NOT be tolerated: a
//! [`Error::FixedWidthLengthMismatch`] reporting a WRONG WIDTH — a fixed-width
//! element whose declared on-disk length is a non-zero length the type's
//! serializer does not admit (a 5-byte `int`, a 3-byte `smallint`, a 17-byte
//! `uuid`). A mismatch reporting a ZERO length is refused too, but is NOT
//! fatal — see "the zero-length half" below.
//!
//! ## Why THIS class is fatal where the others are tolerated
//!
//! Authority is the pinned `cassandra-5.0.8` source, not CQLite's prior
//! behaviour. Cassandra refuses such input OUTRIGHT — it does not drop the
//! offending element and keep the rest of the collection:
//!
//! * `serializers/SetSerializer.java` `validate(...)` calls
//!   `elements.validate(value, accessor)` for EVERY element and lets the
//!   resulting `MarshalException` escape, then throws
//!   `"Unexpected extraneous bytes after set value"` if any byte is left over.
//!   The failure mode is a thrown exception for the whole value.
//! * `serializers/Int32Serializer.java` `validate(...)` is the element-level
//!   refusal itself: `if (accessor.size(value) != 4 && !accessor.isEmpty(value))
//!   throw new MarshalException("Expected 4 or 0 byte int (%d)")`. The sibling
//!   fixed-width serializers (`LongSerializer`, `UUIDSerializer`, ...) have the
//!   identical shape.
//!
//! So a wrong declared width is corrupt input that Cassandra REJECTS. Surfacing
//! it as a silently partial row (a short cell list, or a set missing a member)
//! would make the refusal unobservable at the read path — the strict-rejection
//! ruling on issue #3723 requires the opposite.
//!
//! ## The zero-length half: REFUSED, but NOT fatal
//!
//! CQLite's guard is strictly narrower than Cassandra's on one axis and this is
//! deliberate, already-shipped behaviour, documented with its reasons in
//! `raw_value/fixed_width.rs`: the zero-length case the six "or 0" serializers
//! admit (deserializing to Java `null`) is REFUSED here, because there is no
//! `Value` in this decoder meaning "the element deserialized to null" inside a
//! `Value::Set`/`Value::List`.
//!
//! That REFUSAL is unchanged. Its DISPOSITION is deliberately not the wrong
//! width's, for two independent reasons:
//!
//! 1. **It is not a class this branch introduced.** Before issue #3723 every
//!    fixed-width arm already refused an empty slice (`data.len() < N` /
//!    `data.is_empty()`), as `Error::Corruption` — which BOTH tolerant call
//!    sites absorbed, so the member was omitted / the loop broke and the read
//!    continued. Making the same bytes a hard read failure would change the
//!    behaviour of a PRE-EXISTING path, which is exactly what this issue's
//!    zero-regression argument (below) promises not to do. #3723's subject is a
//!    wrong width being SILENTLY TRUNCATED, not an empty element.
//! 2. **Cassandra does not reject it for the "or 0" family**, so the authority
//!    argument above does not reach it: `Int32Serializer.validate` accepts an
//!    empty buffer. (The four strict serializers — `ShortSerializer`,
//!    `ByteSerializer`, `SimpleDateSerializer`, `TimeSerializer` — DO throw on
//!    an empty buffer, but reason 1 governs them too: the disposition follows
//!    "did this branch introduce the class", not the serializer family, so the
//!    predicate stays one decision rather than a per-type table that would be a
//!    third width table free to drift from the other two.)
//!
//! The zero case is discriminated by the variant's own `actual` field: `actual
//! == 0` IS the zero-length case and nothing else can produce it (`actual` is
//! the slice length, `expected` is never 0, and an equal length never errors).
//! No redundant flag is carried, because a flag could disagree with `actual`.
//!
//! ## Scope
//!
//! This predicate makes ONE decision on purpose. It is not a general
//! "is this corruption" test: `Error::Corruption`, `Error::InvalidInput`,
//! truncation and every other decode failure keep their pre-#3723 tolerant
//! handling at both call sites — as does a zero-length mismatch. So this change
//! cannot alter the behaviour of any path that did not already produce a
//! WRONG-WIDTH `FixedWidthLengthMismatch`, an outcome no path could produce
//! before issue #3723 (the pre-#3723 guards were `< N`, so an over-long element
//! decoded from its prefix and a short one errored as `Corruption`).

use crate::Error;

/// `true` when `err` must abort the enclosing row/collection read rather than
/// being tolerated as a partial result (issue #3723).
///
/// See the module header for the Cassandra authority, for why the fatal set is
/// one variant, and for why the zero-length half of that variant is excluded.
#[inline]
pub(in crate::storage::sstable::reader::parsing::row_decoder) fn is_fatal_decode_error(
    err: &Error,
) -> bool {
    match err {
        // A WRONG WIDTH: fatal. Cassandra's serializer THROWS on it, and no
        // path in this decoder could produce this outcome before issue #3723,
        // so nothing that read before stops reading.
        Error::FixedWidthLengthMismatch { actual, .. } if *actual != 0 => true,
        // A ZERO LENGTH (`actual == 0`): still REFUSED — the caller gets this
        // same named error — but TOLERATED, keeping the disposition these bytes
        // had before issue #3723, when the arms returned `Error::Corruption`
        // and both call sites absorbed it.
        Error::FixedWidthLengthMismatch { .. } => false,
        // Every other class keeps its pre-#3723 tolerant handling.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_fatal_decode_error;
    use crate::Error;

    #[test]
    fn fixed_width_length_mismatch_is_fatal() {
        assert!(is_fatal_decode_error(&Error::FixedWidthLengthMismatch {
            cql_type: "int".to_string(),
            context: "my_set".to_string(),
            expected: 4,
            actual: 5,
        }));
    }

    /// A ZERO declared length is REFUSED (it still produces the named variant)
    /// but is NOT fatal: it keeps the tolerated disposition it had before issue
    /// #3723, when every fixed-width arm returned `Error::corruption` for an
    /// empty slice (verified against `origin/main`).
    #[test]
    fn zero_declared_length_is_not_fatal() {
        for (cql_type, expected) in [
            ("int", 4usize),
            ("bigint", 8),
            ("uuid", 16),
            ("boolean", 1),
            ("float", 4),
            // Also the four STRICT serializers, which Cassandra itself refuses
            // an empty buffer for: the disposition is decided by "did this
            // branch introduce the class", not by which serializer family the
            // type belongs to.
            ("smallint", 2),
            ("tinyint", 1),
            ("date", 4),
            ("time", 8),
        ] {
            assert!(
                !is_fatal_decode_error(&Error::FixedWidthLengthMismatch {
                    cql_type: cql_type.to_string(),
                    context: "my_set".to_string(),
                    expected,
                    actual: 0,
                }),
                "a zero-length `{cql_type}` element must keep its pre-#3723 tolerated \
                 disposition — refused, but not a fatal read failure"
            );
        }
    }

    /// The boundary: one byte is a WRONG width, zero bytes is not. Without this
    /// a predicate that tolerated every mismatch would still pass the test above.
    #[test]
    fn one_byte_is_fatal_where_zero_bytes_is_not() {
        let mismatch = |actual| Error::FixedWidthLengthMismatch {
            cql_type: "smallint".to_string(),
            context: "my_set".to_string(),
            expected: 2,
            actual,
        };
        assert!(
            is_fatal_decode_error(&mismatch(1)),
            "1 byte is a wrong width"
        );
        assert!(!is_fatal_decode_error(&mismatch(0)), "0 bytes is not");
    }

    /// The tolerated classes stay tolerated: this is the anti-blanket-propagation
    /// pin. If someone widens the predicate to "any corruption", this fails.
    #[test]
    fn pre_3723_error_classes_stay_tolerated() {
        let tolerated = [
            Error::corruption("truncated cell"),
            Error::InvalidInput("bad length".to_string()),
            Error::schema("unknown column"),
            Error::internal("dispatch slip"),
            Error::serialization("short read"),
        ];
        for err in tolerated {
            assert!(
                !is_fatal_decode_error(&err),
                "pre-#3723 decode error class must keep its tolerant handling: {err:?}"
            );
        }
    }
}
