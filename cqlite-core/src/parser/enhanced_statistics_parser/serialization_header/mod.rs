//! SerializationHeader schema decoding from the Statistics.db
//! `SERIALIZATION_HEADER` component (partition/clustering/static/regular columns).
//!
//! # There is ONE decoder here, and that is the point (issues #4159, #28)
//!
//! [`schema::parse_serialization_header_schema`] is the AUTHORITATIVE sequential
//! decoder: the caller positions it from the TOC's `HEADER` offset, immediately
//! after the three EncodingStats VInts, and it walks keyType, clusteringTypes,
//! staticColumns and regularColumns in their on-disk order
//! (`SerializationHeader.java`). It refuses — it does not guess — on a bad declared
//! length, a non-UTF-8 type or name, or an absurd count.
//!
//! # What used to live here, and why it is gone
//!
//! A MARKER-SEARCH dispatcher plus three fallbacks:
//!
//! * `parse_serialization_header` — scanned the first 8 KiB for the literal
//!   `org.apache.cassandra.db.marshal`, then worked BACKWARDS through a 15-byte
//!   look-back trying every offset that looked VInt-shaped;
//! * `sequential::parse_serialization_header_at_offset` /
//!   `parse_serialization_header_sequential` — the per-candidate-offset decoders
//!   that scan drove, each answering `Err(_) => continue` for every candidate;
//! * `fallback_parse_serialization_header_ascii` — an ASCII sweep for type names,
//!   with `parse_success = false; break;` per malformed column;
//! * `parse_regular_columns` — the same shape for the column list, silently
//!   omitting a column whose name or type did not decode.
//!
//! Every one of those inferred structure FROM BYTE PATTERNS, which is the
//! no-heuristics mandate's central prohibition (#28), and none was behind the
//! `legacy-heuristics` feature. Their effect on issue #4159 was worse than
//! untidiness: they made a LEGITIMATE serialization-header refusal unobservable at
//! every layer above, substituting a plausible-looking column list decoded against
//! guessed offsets — so a scan returned rows built from the wrong columns instead of
//! reporting that the file could not be read.
//!
//! MEASURED before removal: a `Statistics.db` truncated to half its length, and one
//! whose partition-key marshal type was made invalid UTF-8, BOTH still parsed
//! successfully through this path. MEASURED after removal:
//! `smoke-test-all-tables.sh` = 51/51 enforced tables PASS, so the marker search was
//! not load-bearing on real Cassandra-written data.
//!
//! The three deleted tests (`test_partition_key_extraction_via_backtracking`,
//! `test_partition_key_extraction_with_longer_type`,
//! `test_backtracking_with_no_partition_key`,
//! `test_backtracking_rejects_invalid_types`, and the three
//! `test_serialization_header_with_*` cases) pinned the marker search's own
//! behaviour on hand-built byte strings; they went with the code they described.

mod schema;

pub(in crate::parser::enhanced_statistics_parser) use schema::parse_serialization_header_schema;
