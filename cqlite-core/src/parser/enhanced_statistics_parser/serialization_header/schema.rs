//! Post-EncodingStats SerializationHeader schema parser.
//!
//! [`parse_serialization_header_schema`] is the authoritative sequential decoder
//! invoked once the TOC offset has positioned us immediately after the three
//! EncodingStats VInts. It walks keyType, clusteringTypes, staticColumns, and
//! regularColumns in their on-disk order (`SerializationHeader.java`,
//! cassandra-5.0.8).
//!
//! # It returns a NAMED refusal, not `ErrorKind::Verify` (issue #4159)
//!
//! Every refusal here used to be `nom::Err::Error(ErrorKind::Verify)` with the
//! reason written only to a `tracing::debug!`. `nom::error::Error` carries an input
//! slice and an `ErrorKind` and nothing else, so fifteen structurally different
//! refusals — a zero-length declared type, a non-UTF-8 type name, an absurd column
//! count — all reached `StatisticsReader::open` as one indistinguishable
//! `code: Verify` plus a hex dump. #4159's AC1 requires the error a caller receives
//! to NAME the cause, so this module returns [`crate::Error::Corruption`] carrying
//! the reason AND the byte offset within the header body at which it was decided.
//!
//! # The two column loops are ONE loop
//!
//! `staticColumns` and `regularColumns` have identical on-disk framing
//! (`[VInt count]` then `[VInt name_len][name][VInt type_len][type]` per column),
//! and this file used to carry two byte-identical 60-line copies of it — eight of
//! the fifteen refusal sites were duplicates of four. [`parse_column_list`] is that
//! framing, once, parameterized only on `is_static` and on the noun used in
//! diagnostics.

use super::super::super::header::ColumnInfo;
use super::super::super::vint::parse_vuint;
use super::super::marshal_type::convert_marshal_type_to_cql_checked;
use super::super::SerializationHeaderResult;
use crate::{Error, Result};
use nom::bytes::complete::take;

/// Longest plausible marshal-type class name. Cassandra's longest built-in type
/// strings are well under 200 bytes; deeply-nested frozen collection types can be
/// long, so the bound is generous and exists only to refuse an absurd declared
/// length before allocating on it.
const MAX_TYPE_LEN: u64 = 5000;
/// Longest plausible CQL identifier. Cassandra's own limit is 48 characters
/// (`SchemaConstants`), so 200 refuses only nonsense.
const MAX_NAME_LEN: u64 = 200;
/// Refuse an absurd clustering-key count before allocating on it. Cassandra tables
/// have single-digit clustering key counts in practice.
const MAX_CLUSTERING_COUNT: u64 = 1000;
/// Refuse an absurd column count before allocating on it.
const MAX_COLUMN_COUNT: u64 = 10000;

/// Byte offset of `rest` within `whole` — for diagnostics only; no decision is made
/// from it.
fn at(whole: &[u8], rest: &[u8]) -> usize {
    whole.len().saturating_sub(rest.len())
}

/// Decode one unsigned VInt, naming the field on failure.
fn vuint<'a>(whole: &[u8], input: &'a [u8], field: &str) -> Result<(&'a [u8], u64)> {
    parse_vuint(input).map_err(|_| {
        Error::corruption(format!(
            "SerializationHeader: {field} is not a decodable unsigned VInt at byte {} \
             of the header body ({} byte(s) remain)",
            at(whole, input),
            input.len()
        ))
    })
}

/// Take `len` bytes, naming the field on truncation.
fn bytes<'a>(whole: &[u8], input: &'a [u8], len: u64, field: &str) -> Result<(&'a [u8], &'a [u8])> {
    let want = usize::try_from(len).map_err(|_| {
        Error::corruption(format!(
            "SerializationHeader: {field} declares {len} bytes, which does not fit this \
             platform's address space (at byte {} of the header body)",
            at(whole, input)
        ))
    })?;
    take::<usize, &'a [u8], nom::error::Error<&'a [u8]>>(want)(input).map_err(|_| {
        Error::corruption(format!(
            "SerializationHeader: {field} declares {len} bytes but only {} remain at byte \
             {} of the header body",
            input.len(),
            at(whole, input)
        ))
    })
}

/// Refuse a declared length outside `1..=max`.
fn checked_len(whole: &[u8], input: &[u8], len: u64, max: u64, field: &str) -> Result<()> {
    if len == 0 || len > max {
        return Err(Error::corruption(format!(
            "SerializationHeader: {field} declares a length of {len}, which is zero or \
             beyond the {max}-byte bound, at byte {} of the header body",
            at(whole, input)
        )));
    }
    Ok(())
}

/// Refuse a declared count above `max`.
fn checked_count(whole: &[u8], input: &[u8], count: u64, max: u64, field: &str) -> Result<()> {
    if count > max {
        return Err(Error::corruption(format!(
            "SerializationHeader: {field} declares {count} entries, beyond the bound of \
             {max}, at byte {} of the header body",
            at(whole, input)
        )));
    }
    Ok(())
}

/// Decode a UTF-8 string, naming the field on invalid UTF-8.
fn utf8(whole: &[u8], input: &[u8], raw: &[u8], field: &str) -> Result<String> {
    std::str::from_utf8(raw).map(str::to_string).map_err(|e| {
        Error::corruption(format!(
            "SerializationHeader: {field} is not valid UTF-8 ({e}) over its declared {} \
             byte(s), at byte {} of the header body",
            raw.len(),
            at(whole, input)
        ))
    })
}

/// Parse the schema portion of a SerializationHeader (after EncodingStats have been
/// consumed).
///
/// Format:
/// 1. keyType (VInt length + UTF-8 type string)
/// 2. clusteringTypes (VInt count + [VInt type_len + type]*)
/// 3. staticColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
/// 4. regularColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
pub(in crate::parser::enhanced_statistics_parser) fn parse_serialization_header_schema(
    body: &[u8],
) -> Result<SerializationHeaderResult> {
    // Step 1: keyType (partition key type).
    let (input, pk_type_len) = vuint(body, body, "keyType length")?;
    checked_len(body, input, pk_type_len, MAX_TYPE_LEN, "keyType")?;
    let (input, pk_type_bytes) = bytes(body, input, pk_type_len, "keyType")?;
    // ═══ GATE 2 OF #4104, RE-HOMED ONTO THE ANCHORED DECODER (#4159) ═══
    //
    // A `FrozenType(<scalar>)` header is not something a Cassandra writer can
    // emit: the header records `column.type`, and `CQL3Type.Raw::freeze()` throws
    // for every non-collection/tuple/UDT/vector (cassandra-5.0.8
    // `src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`). Refusing it
    // fail-closed is the no-heuristics answer (#28).
    //
    // #4104 applied this gate at THREE sites. Two of them lived in the
    // marker-search decoders that #4159 deleted; this decoder is the only route
    // to a SerializationHeader that survives, so every one of those gates has to
    // be here or the gate is gone. The refusal keeps the `Error::Schema` KIND all
    // the way to `StatisticsReader::open`, which is what stops a deliberate
    // refusal being presented to the user as a corrupt file.
    let pk_marshal_type = utf8(body, input, pk_type_bytes, "the partition key type")?;
    let partition_key_type = convert_marshal_type_to_cql_checked(&pk_marshal_type).map_err(|e| {
        Error::schema(format!(
            "SerializationHeader partition key type is not writable by Cassandra: {e}"
        ))
    })?;
    tracing::debug!(
        "HEADER: Partition key type: {} ({} bytes)",
        partition_key_type,
        pk_type_len
    );

    // Step 2: clusteringTypes. Only the RAW comparator class name is kept here
    // (issue #759): `build_clustering_key_columns` is the single place that converts
    // to a CQL type AND derives clustering order from a `ReversedType(...)` wrapper,
    // so converting eagerly would discard the DESC signal. That conversion is
    // idempotent for already-CQL strings, keeping the other parse paths correct.
    let (mut input, clustering_count) = vuint(body, input, "clusteringTypes count")?;
    checked_count(
        body,
        input,
        clustering_count,
        MAX_CLUSTERING_COUNT,
        "clusteringTypes",
    )?;
    tracing::debug!("HEADER: {} clustering key types", clustering_count);

    let mut clustering_key_types =
        Vec::with_capacity(usize::try_from(clustering_count).unwrap_or(0));
    for i in 0..clustering_count {
        let field = format!("clustering key type {i}");
        let (rest, ck_type_len) = vuint(body, input, &format!("{field} length"))?;
        checked_len(body, rest, ck_type_len, MAX_TYPE_LEN, &field)?;
        let (rest, ck_type_bytes) = bytes(body, rest, ck_type_len, &field)?;
        let ck_type = utf8(body, rest, ck_type_bytes, &field)?;
        tracing::debug!("HEADER: Clustering key {i}: {ck_type} ({ck_type_len} bytes)");
        clustering_key_types.push(ck_type);
        input = rest;
    }

    // Steps 3 + 4: staticColumns then regularColumns, same framing.
    let (input, mut all_columns) = parse_column_list(body, input, true, "static")?;
    let (input, regular_columns) = parse_column_list(body, input, false, "regular")?;
    all_columns.extend(regular_columns);

    tracing::debug!(
        "HEADER parsing complete: partition_key='{}', {} clustering keys, {} total columns \
         ({} header-body byte(s) consumed)",
        partition_key_type,
        clustering_key_types.len(),
        all_columns.len(),
        at(body, input)
    );

    Ok((
        vec![partition_key_type],
        clustering_key_types,
        all_columns,
    ))
}

/// `[VInt count]` then `count` × `[VInt name_len][name][VInt type_len][type]` — the
/// framing shared by `staticColumns` and `regularColumns`.
fn parse_column_list<'a>(
    body: &[u8],
    input: &'a [u8],
    is_static: bool,
    noun: &str,
) -> Result<(&'a [u8], Vec<ColumnInfo>)> {
    let (mut input, count) = vuint(body, input, &format!("{noun}Columns count"))?;
    checked_count(
        body,
        input,
        count,
        MAX_COLUMN_COUNT,
        &format!("{noun}Columns"),
    )?;
    tracing::debug!("HEADER: {count} {noun} columns");

    let mut columns = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
    for i in 0..count {
        let name_field = format!("{noun} column {i} name");
        let (rest, name_len) = vuint(body, input, &format!("{name_field} length"))?;
        checked_len(body, rest, name_len, MAX_NAME_LEN, &name_field)?;
        let (rest, name_bytes) = bytes(body, rest, name_len, &name_field)?;
        let column_name = utf8(body, rest, name_bytes, &name_field)?;

        let type_field = format!("{noun} column '{column_name}' type");
        let (rest, type_len) = vuint(body, rest, &format!("{type_field} length"))?;
        checked_len(body, rest, type_len, MAX_TYPE_LEN, &type_field)?;
        let (rest, type_bytes) = bytes(body, rest, type_len, &type_field)?;
        // Gate 2 of #4104 — see the partition-key site for the authority. This
        // loop is BOTH `staticColumns` and `regularColumns` (they share their
        // framing), so this single site carries the two per-column gates #4104
        // spelled out separately, and `noun` names which one refused.
        let marshal_type = utf8(body, rest, type_bytes, &type_field)?;
        let cql_type = convert_marshal_type_to_cql_checked(&marshal_type).map_err(|e| {
            Error::schema(format!(
                "SerializationHeader {noun} column {i} ('{column_name}') type is not \
                 writable by Cassandra: {e}"
            ))
        })?;

        tracing::debug!("HEADER: {noun} column '{column_name}': {cql_type} ({type_len} bytes)");
        columns.push(ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static,
            is_clustering: false,
            clustering_reversed: false,
        });
        input = rest;
    }
    Ok((input, columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `[len]org.apache.cassandra.db.marshal.<name>`
    fn marshal(name: &str) -> Vec<u8> {
        let s = format!("org.apache.cassandra.db.marshal.{name}");
        let mut out = vec![s.len() as u8];
        out.extend_from_slice(s.as_bytes());
        out
    }

    /// A minimal well-formed header body: Int32Type partition key, no clustering
    /// keys, no static columns, one regular `name` UTF8Type column.
    fn well_formed() -> Vec<u8> {
        let mut b = marshal("Int32Type");
        b.push(0x00); // clustering count = 0
        b.push(0x00); // static count = 0
        b.push(0x01); // regular count = 1
        b.push(0x04); // name length
        b.extend_from_slice(b"name");
        b.extend_from_slice(&marshal("UTF8Type"));
        b
    }

    #[test]
    fn a_well_formed_body_decodes() {
        let (pk, ck, cols) =
            parse_serialization_header_schema(&well_formed()).expect("well-formed body");
        assert_eq!(pk, vec!["int".to_string()]);
        assert!(ck.is_empty());
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "name");
        assert!(!cols[0].is_static);
    }

    /// The static/regular distinction survives the shared loop — the property the
    /// two former copies of it each carried separately.
    #[test]
    fn static_and_regular_columns_keep_their_kind() {
        let mut b = marshal("Int32Type");
        b.push(0x00); // no clustering keys
        b.push(0x01); // one static column
        b.push(0x02);
        b.extend_from_slice(b"st");
        b.extend_from_slice(&marshal("UTF8Type"));
        b.push(0x01); // one regular column
        b.push(0x03);
        b.extend_from_slice(b"reg");
        b.extend_from_slice(&marshal("Int32Type"));
        let (_, _, cols) = parse_serialization_header_schema(&b).expect("well-formed body");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "st");
        assert!(cols[0].is_static, "the static column must stay static");
        assert_eq!(cols[1].name, "reg");
        assert!(!cols[1].is_static, "the regular column must stay regular");
    }

    /// #4159: each refusal NAMES its cause. Asserted through the returned error, not
    /// through a log line — a log level is not a remedy (AC6).
    #[test]
    fn a_non_utf8_partition_key_type_names_itself() {
        let mut b = well_formed();
        b[1] = 0xFF; // first byte of the marshal class name
        let e = parse_serialization_header_schema(&b).expect_err("invalid UTF-8 must refuse");
        assert!(
            matches!(e, Error::Corruption(_)),
            "the refusal must keep the corruption kind, got {e:?}"
        );
        let msg = e.to_string();
        assert!(
            msg.contains("partition key type") && msg.contains("not valid UTF-8"),
            "the refusal must name WHICH field and WHY: {msg}"
        );
    }

    #[test]
    fn a_zero_length_key_type_names_itself() {
        let e = parse_serialization_header_schema(&[0x00])
            .expect_err("a zero-length keyType must refuse");
        let msg = e.to_string();
        assert!(
            msg.contains("keyType") && msg.contains("zero or beyond"),
            "the refusal must name the field and the bound: {msg}"
        );
    }

    #[test]
    fn an_absurd_clustering_count_names_itself() {
        let mut b = marshal("Int32Type");
        // Unsigned VInt for 2000: two-byte form (0x80 | high bits).
        b.extend_from_slice(&[0x87, 0xD0]);
        let e = parse_serialization_header_schema(&b)
            .expect_err("an absurd clustering count must refuse");
        let msg = e.to_string();
        assert!(
            msg.contains("clusteringTypes") && msg.contains("beyond the bound"),
            "the refusal must name the field and the bound: {msg}"
        );
    }

    #[test]
    fn a_truncated_type_string_names_the_shortfall() {
        let mut b = vec![0x28]; // declares 40 bytes
        b.extend_from_slice(b"org.apache"); // supplies 10
        let e =
            parse_serialization_header_schema(&b).expect_err("a truncated type must refuse");
        let msg = e.to_string();
        assert!(
            msg.contains("keyType") && msg.contains("only 10 remain"),
            "the refusal must name the field, the declared length and what remains: {msg}"
        );
    }

    #[test]
    fn a_truncated_column_name_names_the_column_list() {
        let mut b = marshal("Int32Type");
        b.push(0x00); // no clustering keys
        b.push(0x00); // no static columns
        b.push(0x01); // one regular column
        b.push(0x40); // declares a 64-byte name
        b.extend_from_slice(b"ab"); // supplies 2
        let e = parse_serialization_header_schema(&b)
            .expect_err("a truncated column name must refuse");
        let msg = e.to_string();
        assert!(
            msg.contains("regular column 0 name"),
            "the refusal must name which column list and which index: {msg}"
        );
    }

    /// The byte offset is reported so an operator can hexdump the right place. It is
    /// diagnostics only — no decision is made from it.
    #[test]
    fn a_refusal_reports_the_offset_within_the_header_body() {
        let mut b = marshal("Int32Type");
        b.push(0x00);
        b.push(0x00);
        b.push(0x01);
        b.push(0x00); // a zero-length column name, at a non-zero offset
        let e = parse_serialization_header_schema(&b).expect_err("must refuse");
        let msg = e.to_string();
        assert!(
            msg.contains("of the header body"),
            "the refusal must locate itself: {msg}"
        );
        assert!(
            !msg.contains("at byte 0 of"),
            "the offset must be the REAL one, not a constant: {msg}"
        );
    }
}
