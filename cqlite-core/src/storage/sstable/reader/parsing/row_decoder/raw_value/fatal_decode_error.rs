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
//! Issue #3723 added ONE error class that must NOT be tolerated:
//! [`Error::FixedWidthLengthMismatch`] — a fixed-width element whose declared
//! on-disk length is not a length the type's serializer admits.
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
//! CQLite's guard is strictly narrower than Cassandra's on one axis and this is
//! deliberate, already-shipped behaviour, documented with its four reasons in
//! `raw_value/fixed_width.rs`: the zero-length case Cassandra's `validate`
//! admits (deserializing to Java `null`) is REFUSED here, because there is no
//! `Value` in this decoder meaning "the element deserialized to null" inside a
//! `Value::Set`/`Value::List`.
//!
//! ## Scope
//!
//! This predicate names ONE variant on purpose. It is not a general
//! "is this corruption" test: `Error::Corruption`, `Error::InvalidInput`,
//! truncation and every other decode failure keep their pre-#3723 tolerant
//! handling at both call sites, so this change cannot alter the behaviour of
//! any path that did not already produce a `FixedWidthLengthMismatch` — a
//! variant that did not exist before issue #3723.

use crate::Error;

/// `true` when `err` must abort the enclosing row/collection read rather than
/// being tolerated as a partial result (issue #3723).
///
/// See the module header for the Cassandra authority and for why the set is
/// deliberately a single variant.
#[inline]
pub(in crate::storage::sstable::reader::parsing::row_decoder) fn is_fatal_decode_error(
    err: &Error,
) -> bool {
    matches!(err, Error::FixedWidthLengthMismatch { .. })
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
