//! The typed failure channel of the post-EncodingStats SerializationHeader
//! schema decoder.
//!
//! `parse_serialization_header_schema` can fail for two reasons that a caller
//! must NOT treat alike, and a `nom::Err` cannot tell them apart:
//!
//! * The bytes at the TOC-anchored offset do not hold a readable header at all —
//!   truncated, mispositioned, an implausible declared length, a non-UTF-8 type
//!   name. Retrying with the marker-search decoder is legitimate: it is a second
//!   attempt to find WHERE the header is.
//! * The bytes decoded fine and DECLARE a type Cassandra cannot have written (a
//!   `frozen<scalar>`; `CQL3Type.Raw::freeze()` throws for every
//!   non-collection/tuple/UDT/vector — cassandra-5.0.8
//!   `src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`). Nothing about
//!   WHERE we are looking is in doubt, so a second decoder cannot improve the
//!   answer — it can only replace a correct refusal with a marker-search guess,
//!   which is both fail-open and a no-heuristics violation (#28).
//!
//! Issue #4104 (roborev job 116): the second case used to reach the caller as a
//! bare `nom::Err` and was retried through the heuristic fallback, so a header
//! the frozen-scalar gate had correctly refused was accepted anyway — while the
//! sibling KEY-type gate in the same function failed closed. This enum is what
//! makes the two gates agree.
//!
//! # Its reach, after the #4158 review round
//!
//! It is no longer only the anchored decoder's channel. Job 119 found the same
//! fail-open in the MARKER-SEARCH decoders — a refusal there became an ordinary
//! failed candidate, so the search continued to its empty-schema success — so
//! `serialization_header::{mod, sequential}` and `parse_regular_columns` and the
//! ASCII fallback all carry this type now, and `Refused` terminates the search
//! wherever it arises. Blocker A extended it in the other direction: the carried
//! `Error` is propagated out through `parse_nb_format_statistics_data_with_toc`
//! and the `_detailed` entry points to `StatisticsReader::open`, so the refusal
//! the user sees is the message the gate wrote.

use crate::error::Error;

/// Why [`super::serialization_header::parse_serialization_header_schema`] did not
/// return a schema.
///
/// Callers MUST match on the variant: only [`Self::Structural`] may be retried
/// with the marker-search fallback.
#[derive(Debug)]
pub(super) enum HeaderSchemaError<'a> {
    /// The bytes at this offset are not a readable header (truncated,
    /// mispositioned, implausible length, non-UTF-8). Position is in doubt, so
    /// the marker-search fallback may legitimately be tried.
    Structural(nom::Err<nom::error::Error<&'a [u8]>>),
    /// The header decoded, and what it declares is not writable by Cassandra.
    /// FAIL-CLOSED: never retried with a heuristic. The carried [`Error`] is
    /// PROPAGATED to the user (#4158 review, blocker A) — it is the only text that
    /// names the refused column, the refused type and the Cassandra citation, and
    /// it used to be logged and then dropped, so the refusal reached the caller as
    /// a bare `ErrorKind::Verify` that read like data corruption. The DECISION is
    /// still taken on the VARIANT, never on the message text.
    Refused(Error),
}

impl<'a> From<nom::Err<nom::error::Error<&'a [u8]>>> for HeaderSchemaError<'a> {
    /// Every nom failure the decoder propagates with `?` is structural by
    /// construction: it comes from a length/`take`/VInt primitive, none of which
    /// can express a semantic judgement about a declared type.
    fn from(err: nom::Err<nom::error::Error<&'a [u8]>>) -> Self {
        Self::Structural(err)
    }
}

impl<'a> HeaderSchemaError<'a> {
    /// A structural refusal raised by the decoder's own sanity checks at
    /// `input` (implausible declared count/length, non-UTF-8 name or type).
    pub(super) fn structural_at(input: &'a [u8]) -> Self {
        Self::Structural(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )))
    }

    /// A semantic refusal of `detail`, which already carries the Cassandra
    /// citation from the frozen-scalar gate.
    pub(super) fn refused(detail: impl Into<String>) -> Self {
        Self::Refused(Error::schema(detail.into()))
    }
}
