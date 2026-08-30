//! The timestamp SPELLING an sstabledump golden may carry — issue #1490 (AD1)
//! round 17, the FALSE-PASS half of golden validation, epic #1469.
//!
//! # The defect this target exists for
//!
//! `golden_schema.rs` validates the WHOLE structure of every golden the harness
//! consumes, and for a timestamp-bearing field it used to validate by ASKING the
//! shared canonical parser: `if parse_timestamp_micros(text).is_none() { refuse }`.
//!
//! **Delegating validation to a lenient parser is not validation.**
//! `parse_timestamp_micros` NORMALIZES rather than refuses, so a `Some` from it
//! establishes only that SOME instant could be produced — not that the golden is
//! one `sstabledump` could have written. `2025-01-01T24:00:00Z` yields exactly
//! the microseconds of `2025-01-02T00:00:00Z`; `2025-02-30` rolls forward into
//! March; a 7th fractional digit is truncated away. Each of those is a MALFORMED
//! oracle that compares EQUAL to a correct export — the one direction this whole
//! parity harness exists to measure, reintroduced one level inside the pass built
//! to prevent it.
//!
//! # The authority, and why it is not CQLite
//!
//! Cassandra 5.0.8 `JsonTransformer.dateString`, read at the pinned tag, writes
//! EVERY timestamp in EVERY committed golden:
//!
//! ```text
//! long secs   = from.toSeconds(time);
//! long offset = Math.floorMod(from.toNanos(time), 1000_000_000L);
//! return Instant.ofEpochSecond(secs, offset).toString();
//! ```
//!
//! `Instant.toString()` is `DateTimeFormatter.ISO_INSTANT` over `java.time`, so
//! what that formatter can write IS what a well-formed golden can say. Every
//! expectation below is derived from it — never from what CQLite's own parser
//! happens to tolerate, which would be circular (#3041).
//!
//! # How each case is kept from passing vacuously
//!
//! A refusal of a string nothing would have accepted is not a fix, so every case
//! MEASURES the equivalence first: the shared parser really does read the
//! malformed spelling as the same instant as a well-formed one, so the malformed
//! golden really would have compared equal. Only then is the refusal asserted —
//! at every one of the four positions `Rule::Timestamp` guards, and paired with
//! the positive control that the well-formed spelling of the SAME instant is
//! still accepted.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

/// A timestamp spelling the shared parser NORMALIZES is REFUSED, not normalized
/// — the FALSE-PASS direction of timestamp validation (#1490 round 17).
///
/// The previous pass asked `parse_timestamp_micros` whether it returned
/// something. It does — for spellings `Instant.toString()` can NEVER write.
/// **Delegating validation to a lenient parser is not validation**: a `Some`
/// establishes only that SOME instant could be produced, and an instant produced
/// from a malformed spelling compares EQUAL to a correct export.
///
/// Each case MEASURES the equivalence first — the lenient parser really does
/// read the malformed spelling as the SAME µs as a well-formed one, so the
/// malformed golden really would have passed — and only then asserts the
/// refusal. Without the first half the second proves nothing: refusing a string
/// nothing would have accepted is not a fix.
///
/// Authority for every expectation below is `JsonTransformer.dateString`
/// (cassandra-5.0.8), which returns `Instant.ofEpochSecond(...).toString()`,
/// i.e. `DateTimeFormatter.ISO_INSTANT` over `java.time` — never CQLite's own
/// parser, which is the circular reading this test exists to remove.
#[test]
fn a_spelling_the_shared_parser_normalizes_is_refused_not_normalized() {
    use parquet_parity::canonical_jsonl::parse_timestamp_micros;
    use parquet_parity::golden_rows::validate_golden_text;

    // Every position `Rule::Timestamp` guards, so each case is asserted at ALL
    // of them rather than at whichever one it happened to be written with.
    fn lines_with(ts: &str) -> Vec<String> {
        vec![
            format!(
                r#"{{"partition":{{"key":["1"]}},"rows":[{{"type":"row","liveness_info":{{"tstamp":"{ts}"}},"cells":[{{"name":"v","value":"x"}}]}}]}}"#
            ),
            format!(
                r#"{{"partition":{{"key":["1"]}},"rows":[{{"type":"row","cells":[{{"name":"v","value":"x","tstamp":"{ts}"}}]}}]}}"#
            ),
            format!(
                r#"{{"partition":{{"key":["1"]}},"rows":[{{"type":"row","cells":[{{"name":"m","deletion_info":{{"marked_deleted":"{ts}","local_delete_time":"2025-01-02T00:00:00Z"}}}}]}}]}}"#
            ),
            format!(
                r#"{{"partition":{{"key":["1"]}},"rows":[{{"type":"row","cells":[{{"name":"m","deletion_info":{{"local_delete_time":"{ts}"}}}}]}}]}}"#
            ),
        ]
    }

    // (malformed spelling, the well-formed spelling the parser reads it AS, the
    // clause the refusal must name).
    let normalized_away: &[(&str, &str, &str)] = &[
        // The finding's own case: hour 24. `ISO_INSTANT` writes 00..=23, and
        // `24:00:00` is read as the FOLLOWING midnight.
        ("2025-01-01T24:00:00Z", "2025-01-02T00:00:00Z", "hour 24"),
        ("2025-01-01T00:60:00Z", "2025-01-01T01:00:00Z", "minute 60"),
        // A leap second is NOT accepted: `dateString` builds an `Instant`, and
        // `java.time` models a day as exactly 86 400 seconds, so `ISO_INSTANT`
        // cannot write `:60` at all. A `:60` spelling is a malformed golden, and
        // the lenient parser rolls it into the following minute.
        ("2025-01-01T00:00:60Z", "2025-01-01T00:01:00Z", "second 60"),
        // Calendar dates that do not exist, each rolled FORWARD into one that
        // does. 2025 and 2023 are not leap years; April has 30 days.
        (
            "2025-02-30T00:00:00Z",
            "2025-03-02T00:00:00Z",
            "day 30 does not exist",
        ),
        (
            "2023-02-29T00:00:00Z",
            "2023-03-01T00:00:00Z",
            "day 29 does not exist",
        ),
        (
            "2025-04-31T00:00:00Z",
            "2025-05-01T00:00:00Z",
            "day 31 does not exist",
        ),
        // Precision the µs comparison cannot represent: the 7th digit is
        // TRUNCATED, so the over-precise golden reads as the truncated one.
        (
            "2025-01-01T00:00:00.1234567Z",
            "2025-01-01T00:00:00.123456Z",
            "7 digits",
        ),
    ];

    for (malformed, equivalent, needle) in normalized_away {
        // The DEFECT, measured: to the shared parser these are the same instant,
        // so accepting the first was accepting the second.
        let lenient = parse_timestamp_micros(malformed);
        assert!(
            lenient.is_some() && lenient == parse_timestamp_micros(equivalent),
            "this case's premise is that the shared parser NORMALIZES {malformed:?} into the \
             same instant as {equivalent:?} ({lenient:?} vs {:?}); if it no longer does, the \
             refusal below is guarding something else and must be re-derived",
            parse_timestamp_micros(equivalent)
        );

        for line in lines_with(malformed) {
            let err = validate_golden_text(&format!("{line}\n"))
                .err()
                .unwrap_or_else(|| {
                    panic!(
                        "a spelling `Instant.toString()` cannot write must be REFUSED, never \
                         normalized into a comparable instant: {line}"
                    )
                });
            assert!(
                err.contains(needle),
                "the refusal must name WHAT it found ({needle:?}): {err}"
            );
            assert!(
                err.contains("ISO_INSTANT"),
                "…and anchor on Cassandra's own writer rather than on what CQLite's parser \
                 tolerates: {err}"
            );
        }
        // The refusal is of the SPELLING, not of the instant: the well-formed
        // spelling of the very same instant is still accepted at every position.
        for line in lines_with(equivalent) {
            validate_golden_text(&format!("{line}\n")).unwrap_or_else(|e| {
                panic!("the well-formed spelling of the same instant must be accepted: {e}")
            });
        }
    }

    // Spellings `ISO_INSTANT` never writes because they are not fixed-width and
    // zero-padded. The shared parser's `str::parse` accepts them; this pass does
    // not, so an unpadded or signed component cannot reach a comparison either.
    for (malformed, needle) in [
        ("2025-1-01T00:00:00Z", "ASCII digits"),
        ("2025-01-01T0:00:00Z", "ASCII digits"),
        ("+2025-01-01T00:00:00Z", "ASCII digits"),
        ("2025-01-01T00:00:00.Z", "no digits"),
    ] {
        for line in lines_with(malformed) {
            let err = validate_golden_text(&format!("{line}\n"))
                .expect_err("a spelling `ISO_INSTANT` cannot write must be refused");
            assert!(
                err.contains(needle),
                "the refusal must name what it found ({needle:?}): {err}"
            );
        }
    }

    // CONTROLS. Every timestamp form the committed corpus goldens actually carry
    // — measured across every `*-Data.db.jsonl` in the fetched corpus: a bare
    // second, a 3-digit fraction and a 6-digit fraction, all `T`-separated and
    // zero-padded — plus the SPACE separator and the 2-digit fraction the shared
    // parser's own tests carry, plus the boundaries (a leap day, the last µs of a
    // year, the epoch itself). If this fix ever starts refusing one of these the
    // corpus cell count moves, and that is a defect.
    let accepted = [
        "2025-01-01T00:00:00Z",
        "2025-10-06T01:12:07.265Z",
        "2025-10-06T01:12:07.265432Z",
        "2025-10-06 01:12:07.265Z",
        "2025-01-01T00:00:00.06Z",
        "2024-02-29T00:00:00Z",
        "2025-12-31T23:59:59.999999Z",
        "1970-01-01T00:00:00Z",
    ];
    for ts in accepted {
        for line in lines_with(ts) {
            validate_golden_text(&format!("{line}\n")).unwrap_or_else(|e| {
                panic!("{ts:?} is a real sstabledump timestamp and must be accepted: {e}")
            });
        }
        // The DIRECTIONAL agreement property, in the only safe direction:
        // nothing this pass accepts is a string the canonical parser — which
        // builds the comparison — cannot reproduce. The equality of the two
        // instants is enforced per FIELD inside the validator itself
        // (`timestamp_disagreement`), not merely sampled here.
        assert!(
            parse_timestamp_micros(ts).is_some(),
            "{ts:?} is accepted by the validator, so the canonical parser must read an instant \
             from it — a golden accepted here and unreadable there is the fallback-to-`None` \
             hazard this whole rule exists to remove"
        );
    }
}
