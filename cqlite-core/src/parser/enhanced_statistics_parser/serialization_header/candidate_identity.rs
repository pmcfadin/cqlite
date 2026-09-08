//! Is a marker-search CANDIDATE actually a SerializationHeader? (#4104, roborev
//! job 120)
//!
//! # Why the question exists at all
//!
//! A semantic refusal (`FrozenType(<scalar>)` — a type no Cassandra writer can
//! have recorded) is authoritative only where the header's LOCATION is known. On
//! the TOC-anchored path it is known, so the refusal is propagated immediately.
//! The MARKER SEARCH is the opposite situation: it walks arbitrary bytes looking
//! for a byte run that decodes like a header, and the decoders accept any UTF-8
//! string as the key type. So an EARLIER FALSE candidate — a cell value, a
//! min/max clustering value, any run that happens to sit a few bytes before an
//! `org.apache.cassandra.db.marshal` occurrence — can decode a plausible
//! `FrozenType(<scalar>)` column and refuse.
//!
//! Honouring that refusal would abort an otherwise VALID SSTable before the real
//! header was ever reached (an over-refusal). Ignoring it would restore the
//! fail-open the search had before: a correct refusal replaced by a later
//! heuristic match or by the dispatcher's own empty-schema success. The answer is
//! neither: CONFIRM the candidate first, and honour the refusal only then.
//!
//! # What confirms a candidate — Cassandra's own writer
//!
//! `SerializationHeader.Component.serializer` writes `keyType`, every
//! `clusteringTypes` entry and every column type through `writeType`, which
//! serialises `AbstractType::toString()` — the FULLY-QUALIFIED marshal class name
//! (`cassandra-5.0.8:src/java/org/apache/cassandra/db/SerializationHeader.java`;
//! the field order and the `[VInt len][string]` framing are tabulated in
//! `docs/sstables-definitive-guide/chapters/08-statistics-db.md`,
//! "SerializationHeader Component"). Two consequences are used here, and both are
//! properties of the WRITER, never of CQLite's prior behaviour:
//!
//! 1. A header decodes to the END of its own declared clustering/static/regular
//!    lists. A run of arbitrary bytes that briefly looked header-shaped does not.
//! 2. Its key type — and each clustering type — is a marshal class SPELLING, not
//!    merely a string that contains one somewhere. A candidate the search entered
//!    a few bytes early decodes a key type with junk in front of the class name,
//!    which is exactly that difference.
//!
//! Nothing here decides a SCHEMA: the survey decode's values are consumed and
//! dropped, and the only output is the boolean. That is what keeps the deferred
//! refusal of [`super::sequential::SemanticGate::Survey`] from becoming a
//! heuristic type fallback (#28).

use super::super::schema_refusal::HeaderSchemaError;
use super::super::SerializationHeaderResult;

/// The package every SerializationHeader type string is qualified with.
const MARSHAL_PACKAGE: &str = "org.apache.cassandra.db.marshal.";

/// Whether `survey` — the SAME candidate bytes decoded under
/// [`super::sequential::SemanticGate::Survey`], so its refusal did not cut the
/// decode short — shows a complete SerializationHeader identity.
///
/// `true` means a semantic refusal from those bytes is authoritative and must
/// fail closed. `false` means the candidate is a guess that did not pan out, so
/// the search continues past it.
///
/// The survey result is taken BY VALUE and dropped: a caller cannot accidentally
/// promote an ungated decode into a schema.
pub(super) fn candidate_confirmed_as_header(
    survey: Result<(&[u8], SerializationHeaderResult), HeaderSchemaError<'_>>,
) -> bool {
    let Ok((_, (key_types, clustering_types, _columns))) = survey else {
        tracing::debug!(
            "Marker-search candidate refused a type but does not decode as a complete \
             header; treating it as a failed candidate and continuing the search"
        );
        return false;
    };

    // `keyType` is ONE field of the header, and the decoders return exactly the
    // one they read; anything else is not the shape Cassandra wrote.
    let [key_type] = key_types.as_slice() else {
        return false;
    };

    if !is_marshal_type_spelling(key_type) {
        tracing::debug!(
            "Marker-search candidate refused a type but its key type is not a marshal \
             class spelling ({:?}); continuing the search",
            key_type
        );
        return false;
    }

    clustering_types.iter().all(|clustering_type| {
        let spelled = is_marshal_type_spelling(clustering_type);
        if !spelled {
            tracing::debug!(
                "Marker-search candidate refused a type but its clustering type is not a \
                 marshal class spelling ({:?}); continuing the search",
                clustering_type
            );
        }
        spelled
    })
}

/// Whether `type_string` is a fully-qualified Cassandra marshal class spelling —
/// what `AbstractType::toString()` produces and therefore the only thing
/// `SerializationHeader.writeType` can have written.
///
/// Deliberately a SPELLING test, not a "contains" test: a candidate the marker
/// search entered before the true type-length VInt decodes a key type of the form
/// `<junk>org.apache.cassandra.db.marshal.…`, which contains the package and is
/// still not a class name.
fn is_marshal_type_spelling(type_string: &str) -> bool {
    // The accepted header forms sometimes hand the top-level type over with a
    // wrapping paren or bracket (`(org.apache…UUIDType`,
    // `[org.apache…ReversedType(…)`) — the same normalisation
    // `marshal_type::is_reversed_comparator` performs.
    let spelling = type_string.trim().trim_start_matches(['(', '[']).trim_start();
    let Some(class_name) = spelling.strip_prefix(MARSHAL_PACKAGE) else {
        return false;
    };
    // A Java class name starts with a letter and carries no control characters.
    class_name.starts_with(|c: char| c.is_ascii_alphabetic())
        && !spelling.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The survey result a confirmed candidate produces: one properly spelled
    /// key type, spelled clustering types, and a decode that reached the end.
    fn survey_ok<'a>(
        key_types: &[&str],
        clustering_types: &[&str],
    ) -> Result<(&'a [u8], SerializationHeaderResult), HeaderSchemaError<'a>> {
        Ok((
            &[],
            (
                key_types.iter().map(|t| t.to_string()).collect(),
                clustering_types.iter().map(|t| t.to_string()).collect(),
                Vec::new(),
            ),
        ))
    }

    const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";

    #[test]
    fn a_complete_header_identity_confirms_the_candidate() {
        assert!(candidate_confirmed_as_header(survey_ok(
            &[UTF8],
            &["org.apache.cassandra.db.marshal.TimestampType"],
        )));
    }

    /// The over-refusal case: the search entered a few bytes EARLY, so the key
    /// type carries junk in front of the class name. Cassandra's `writeType`
    /// cannot have written that, so the candidate's refusal is not authoritative.
    #[test]
    fn a_key_type_entered_early_does_not_confirm_the_candidate() {
        assert!(!candidate_confirmed_as_header(survey_ok(
            &["\u{0}\u{1}org.apache.cassandra.db.marshal."],
            &[],
        )));
    }

    #[test]
    fn an_unspelled_clustering_type_does_not_confirm_the_candidate() {
        assert!(!candidate_confirmed_as_header(survey_ok(
            &[UTF8],
            &["a value that merely mentions org.apache.cassandra.db.marshal.Int32Type"],
        )));
    }

    /// A candidate that does not decode to the end of its own declared lists is
    /// bytes that briefly looked like a header.
    #[test]
    fn an_incomplete_decode_does_not_confirm_the_candidate() {
        assert!(!candidate_confirmed_as_header(Err(
            HeaderSchemaError::structural_at(&[])
        )));
    }

    /// `keyType` is one field of the header (guide Ch.8); a survey reporting
    /// anything else is not the shape Cassandra wrote.
    #[test]
    fn a_candidate_without_exactly_one_key_type_is_unconfirmed() {
        assert!(!candidate_confirmed_as_header(survey_ok(&[], &[])));
        assert!(!candidate_confirmed_as_header(survey_ok(&[UTF8, UTF8], &[])));
    }

    #[test]
    fn marshal_spellings_accepted_include_the_parenthesised_header_forms() {
        assert!(is_marshal_type_spelling(UTF8));
        assert!(is_marshal_type_spelling(
            "(org.apache.cassandra.db.marshal.UUIDType"
        ));
        assert!(is_marshal_type_spelling(
            "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)"
        ));
        assert!(is_marshal_type_spelling(
            "[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"
        ));
    }

    #[test]
    fn a_string_that_merely_contains_the_package_is_not_a_spelling() {
        // Package with no class name after it.
        assert!(!is_marshal_type_spelling(
            "org.apache.cassandra.db.marshal."
        ));
        // A different package entirely (the partitioner string, which precedes
        // the header in the no-TOC preamble).
        assert!(!is_marshal_type_spelling(
            "org.apache.cassandra.dht.Murmur3Partitioner"
        ));
        // A cell value quoting a type name.
        assert!(!is_marshal_type_spelling(
            "frozen<int> is org.apache.cassandra.db.marshal.FrozenType(x)"
        ));
    }
}
