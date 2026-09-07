//! What a WELL-FORMED SPELLING of each non-text scalar type is — one predicate per
//! type, each quoting the `cassandra-5.0.8` source it is read from (issue #3726).
//!
//! # Why this module exists
//!
//! `container::canon_matches_declared_kinds` reads [`super::canon_typed`]'s OWN
//! OUTPUT back against the declared type, so that the undecoded-golden gap
//! (`compare::gap::Divergence::NestedFrozenValueLeftUndecodedByGolden`) cannot
//! suppress a MALFORMED decode at a position where the golden is undecoded and
//! there is no other side left to catch it. Its leaf question is `Canon` VARIANT
//! only, and for four declared types the variant is the same one — `Canon::Text` —
//! so a single arm accepted every string for `blob`, `timestamp` and all six
//! opaque scalars: a non-hex blob, a non-UUID uuid and arbitrary text at an `inet`
//! position were all excused as decodes of those types (roborev job 105, Medium).
//!
//! This is the THIRD narrowing of that one shape in that function, after the CSV
//! `Boolean` arm (roborev job 72) and `Canon::Null` (job 60), and it follows their
//! rule exactly: read the canonicalizer's own output back with an
//! AUTHORITY-BACKED SPELLING CHECK, per type, and nothing more.
//!
//! # NOT a strict type validator (issue #3846)
//!
//! #3846 rules that a general "second opinion about what a well-formed value of a
//! declared type is" — the shape #3500 abandoned — wants its own design rather
//! than a clause on a gap matcher. Nothing here is that. Each predicate is a
//! BOUNDED spelling test over ONE type, quoting the Cassandra writer that produces
//! the spelling, and NO predicate is invented for a type whose spelling this module
//! has not read an authority for: [`opaque_spelling_matches`] answers `None` there,
//! and its caller must then REFUSE to match rather than accept arbitrary text. A
//! positive verdict requires an affirmative measurement (CLAUDE.md).
//!
//! # The authorities, and what each one says
//!
//! A container member reachable from the gap is FROZEN and therefore
//! [`super::Kinding::Natural`], i.e. written by `toJSONString` rather than
//! `getString` (see the `container` module doc), so `toJSONString` is the authority
//! for every spelling below.
//!
//!   * `blob` — `BytesType.toJSONString`:
//!     `return "\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"';` — `0x` and a byte
//!     string's hex, hence an EVEN number of hex digits. [`is_blob_hex`].
//!   * `timestamp` — `TimestampType.toJSONString` is
//!     `'"' + toString(TimestampSerializer.instance.deserialize(buffer)) + '"'`,
//!     and `TimestampSerializer.toStringUTC` formats with the pattern
//!     `yyyy-MM-dd'T'HH:mm:ss.SSSX` at `ZoneId.of("UTC")`. The CLI spells the same
//!     instant `2025-10-06 01:12:05.394+0000` (measured on
//!     `test_basic.simple_table`), which is why the check is a FIXED POINT of
//!     [`super::canon_timestamp`] rather than a match on Cassandra's raw text:
//!     `canon_typed` has already mapped both legitimate spellings onto one
//!     canonical form, and an unrecognised spelling is passed through VERBATIM, so
//!     "the canonical form re-canonicalizes to itself" is exactly "the
//!     canonicalizer recognised a timestamp here". [`is_canonical_timestamp`].
//!   * `uuid`, `timeuuid` — neither type overrides `toJSONString`, so
//!     `AbstractType.toJSONString` applies:
//!     `'"' + Objects.toString(getSerializer().deserialize(buffer), "") + '"'`,
//!     and `UUIDSerializer.toString` returns `value.toString()`, i.e. Java
//!     `UUID.toString`'s 8-4-4-4-12 hex form.
//!   * `date` — `SimpleDateType.toJSONString` delegates to
//!     `SimpleDateSerializer.toString`, which is
//!     `Instant.ofEpochMilli(dayToTimeInMillis(value)).atZone(UTC).format(formatter)`
//!     with `formatter = DateTimeFormatter.ISO_LOCAL_DATE`, i.e. `yyyy-MM-dd` —
//!     and, outside the four-digit year range CQL's `date` can reach, ISO's signed
//!     expanded-year form, which is why the year field is not pinned to exactly
//!     four digits.
//!   * `time` — `TimeType.toJSONString` delegates to `TimeSerializer.toString`,
//!     which builds `HH:MM:SS.` followed by `leftPadZeros(milli, 3)`,
//!     `leftPadZeros(micro, 3)` and `leftPadZeros(nano, 3)` — a FIXED-WIDTH
//!     `HH:MM:SS.nnnnnnnnn`, nine fraction digits always.
//!   * `duration` — `DurationSerializer.toString` returns `duration.toString()`,
//!     i.e. `cql3.Duration.toString`: an optional `-`, then `<digits><unit>` groups
//!     emitted in the FIXED order `y mo d h m s ms us ns`, each group present only
//!     when its component is non-zero (`append` returns without appending when
//!     `dividend == 0 || dividend < divisor`), so the zero duration is the EMPTY
//!     string.
//!   * `inet` — `InetAddressType.toJSONString` is
//!     `'"' + toString(getSerializer().deserialize(buffer)) + '"'` and
//!     `InetAddressSerializer.toString` returns `value.getHostAddress()`, over an
//!     `InetAddress.getByAddress(byte[])` of the cell's 4 or 16 bytes — so the text
//!     is an IPv4 or IPv6 address literal and nothing else. The check is a PARSE
//!     rather than a spelling match, deliberately: the CLI renders IPv6 COMPRESSED
//!     where the golden renders it expanded (the measured divergence
//!     `compare::gap::Divergence::InetIpv6RendersCompressed` declares), and both are
//!     well-formed addresses.
//!
//! # Measured against real egress before being narrowed
//!
//! Every predicate here was checked against what the CLI actually emits, so none of
//! them reds correct input: `test_basic.simple_table` renders
//! `id` = `15291a77-d739-4e73-8397-b787442f3a1f`,
//! `session_id` = `78f64100-a251-11f0-a18d-d6726a637a4c`,
//! `birth_date` = `2025-06-18`, `work_time` = `01:12:05.394017000`,
//! `duration_val` = `46702000000000ns`, `ip_address` = `154.47.65.214`, and
//! `description` = `0x94df07b2…`; `test_comparator_order.collection_order`'s
//! `pair_set` — the ONE live gap position that reaches an opaque type — renders
//! `["2001:db8::1", "00:00:10.000000000"]`.
//!
//! Note `duration_val`: the CLI spells that value as a raw nanosecond count where
//! `Duration.toString` would spell it `12h58m22s`. That is a real spelling
//! divergence and is NOT this module's business — `46702000000000ns` is grammatical
//! under the authority above, so the GRAMMAR is what is checked here and the exact
//! rendering is left to the ordinary comparison, which is the side that can see
//! both spellings.

/// CQL's blob literal: `0x` and an EVEN number of hex digits (a byte string), and
/// nothing else. `0x` alone is a legal empty blob and is accepted; the point of the
/// check is that arbitrary text at that position is NOT a blob.
///
/// Authority: `cassandra-5.0.8 BytesType.toJSONString` returns
/// `"\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"'`.
///
/// DIGIT CASE IS NOT PINNED, and that latitude is declared rather than accidental:
/// `Hex.bytesToHex` emits lowercase and the CLI does too (measured above), so an
/// uppercase spelling would be a divergence — but this predicate is SHARED with
/// `compare::gap::Divergence::MulticellMapKeyUndecodedByGoldenRendersAsBlobHex`,
/// where it has always been case-insensitive, and tightening it here would change
/// that gap's behaviour under an issue about a different one. What it does close is
/// the finding's subject: text that is not a hex byte string at all.
pub fn is_blob_hex(text: &str) -> bool {
    let Some(digits) = text.strip_prefix("0x") else {
        return false;
    };
    digits.len() % 2 == 0 && digits.chars().all(|c| c.is_ascii_hexdigit())
}

/// Is `text` a spelling [`super::canon_timestamp`] RECOGNISED as a timestamp?
///
/// Asked as a FIXED POINT, which is what makes this a read-back of the
/// canonicalizer's own output rather than a second opinion about timestamps:
/// `canon_typed`'s `Timestamp` arm stores `canon_timestamp(s)` when that returns
/// `Some` and the input VERBATIM when it returns `None`, and the canonical form
/// `YYYY-MM-DDTHH:MM:SS[.fff]Z` re-canonicalizes to itself. So `text` is its own
/// canonical form exactly when the canonicalizer read a timestamp there.
///
/// Consequently a spelling `canon_timestamp` deliberately declines — a NON-ZERO UTC
/// offset, which it leaves opaque rather than silently shifting — is NOT accepted
/// here either. That is the fail-closed direction: the gap stops suppressing at
/// that position and the two spellings are reported as an ordinary diff, which is
/// exactly what `canon_timestamp`'s own doc comment says should happen.
pub fn is_canonical_timestamp(text: &str) -> bool {
    super::canon_timestamp(text).as_deref() == Some(text)
}

/// Does `text` have a spelling a value of the OPAQUE type `name` can have?
///
/// `None` means NO AUTHORITY HAS BEEN READ for `name`, and the caller must then
/// refuse to match: accepting arbitrary text for an unrecognised type is the
/// permissive branch this whole narrowing exists to remove. The recognised set is
/// exactly `schema::NATIVE_OPAQUE`, and
/// `golden_value_scalar_spelling_tests::every_native_opaque_type_has_a_spelling_rule`
/// derives the census FROM that list, so a native opaque type added to the reader
/// without a rule here FAILS rather than joining the permissive branch.
pub fn opaque_spelling_matches(name: &str, text: &str) -> Option<bool> {
    match name {
        "uuid" | "timeuuid" => Some(is_uuid(text)),
        "date" => Some(is_iso_local_date(text)),
        "time" => Some(is_time_of_day(text)),
        "duration" => Some(is_duration(text)),
        "inet" => Some(text.parse::<std::net::IpAddr>().is_ok()),
        _ => None,
    }
}

/// Java `UUID.toString`: 8-4-4-4-12 hex digits, hyphen-separated, 36 characters.
fn is_uuid(text: &str) -> bool {
    let b = text.as_bytes();
    if b.len() != 36 {
        return false;
    }
    b.iter().enumerate().all(|(i, c)| match i {
        8 | 13 | 18 | 23 => *c == b'-',
        _ => c.is_ascii_hexdigit(),
    })
}

/// `DateTimeFormatter.ISO_LOCAL_DATE`: `yyyy-MM-dd`, with ISO's signed expanded
/// year outside the four-digit range (`+10000-01-01`, `-0001-01-01`).
///
/// The MONTH and DAY fields are exactly two digits — ISO fixes them — while the
/// year is "at least four digits", because CQL's `date` reaches years ISO spells
/// with more. Nothing here range-checks a field: this is a SPELLING test, and a
/// calendar validator would be the strict type validator #3846 rules out.
fn is_iso_local_date(text: &str) -> bool {
    let rest = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    let mut parts = rest.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let digits = |s: &str, min: usize, max: usize| {
        (min..=max).contains(&s.len()) && s.bytes().all(|c| c.is_ascii_digit())
    };
    digits(year, 4, 10) && digits(month, 2, 2) && digits(day, 2, 2)
}

/// `TimeSerializer.toString`: `HH:MM:SS.nnnnnnnnn` — fixed width, nine fraction
/// digits always (three `leftPadZeros(_, 3)` calls, milli then micro then nano).
fn is_time_of_day(text: &str) -> bool {
    let b = text.as_bytes();
    if b.len() != 18 {
        return false;
    }
    b.iter().enumerate().all(|(i, c)| match i {
        2 | 5 => *c == b':',
        8 => *c == b'.',
        _ => c.is_ascii_digit(),
    })
}

/// The units `cql3.Duration.toString` emits, IN THE ORDER it emits them.
///
/// Longest-first WITHIN each start letter is what makes the scan below
/// unambiguous: `mo` and `ms` must be tried before `m`, and `ns`/`us` are the only
/// spellings starting with those letters.
const DURATION_UNITS: &[&str] = &["y", "mo", "d", "h", "m", "s", "ms", "us", "ns"];

/// `cql3.Duration.toString`: an optional `-`, then `<digits><unit>` groups whose
/// units appear in `DURATION_UNITS` order, each at most once.
///
/// The EMPTY string is accepted after the optional sign only when there was no
/// sign: `toString` appends nothing for a duration whose months, days and
/// nanoseconds are all zero (`append` returns early when `dividend == 0`), and
/// `isNegative()` is false for that value, so a lone `-` is not a spelling it can
/// produce.
fn is_duration(text: &str) -> bool {
    let rest = match text.strip_prefix('-') {
        // A sign with no groups after it is not producible.
        Some("") => return false,
        Some(after) => after,
        None => text,
    };
    let mut cursor = rest;
    let mut next_unit = 0usize;
    while !cursor.is_empty() {
        let digits = cursor.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return false;
        }
        cursor = &cursor[digits..];
        // The first unit AT OR AFTER `next_unit` whose spelling starts here, taking
        // the LONGEST match so `mo`/`ms` are never read as `m` plus stray text.
        let mut matched = None;
        for (index, unit) in DURATION_UNITS.iter().enumerate().skip(next_unit) {
            if cursor.starts_with(unit)
                && matched.is_none_or(|(_, prev): (usize, &str)| unit.len() > prev.len())
            {
                matched = Some((index, *unit));
            }
        }
        let Some((index, unit)) = matched else {
            return false;
        };
        cursor = &cursor[unit.len()..];
        next_unit = index + 1;
    }
    true
}

#[cfg(test)]
#[path = "golden_value_scalar_spelling_tests.rs"]
mod tests;
