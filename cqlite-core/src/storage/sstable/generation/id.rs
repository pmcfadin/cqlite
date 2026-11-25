//! Generation ids: parsing, limits and Cassandra's comparator (requirement G5).
//!
//! Authority (`cassandra-5.0.8`, `src/java/org/apache/cassandra/io/sstable/`):
//! - `UUIDBasedSSTableId.java:41,114,130`: a UUID id is exactly 28 characters matching
//!   `([0-9a-z]{4})_([0-9a-z]{4})_([0-9a-z]{5})([0-9a-z]{13})` case-insensitively. The groups are
//!   base-36: days, seconds-in-day, 100ns ticks, and the unsigned 64-bit low word of a TimeUUID.
//! - `SequenceBasedSSTableId.java:90,112,124`: a sequence id is 1 to 10 ASCII digits that
//!   `Integer.parseInt` accepts, i.e. `0..=2147483647`.
//! - `SSTableIdFactory.java:38-45`: UUID is tried first. `:81-96` (`COMPARATOR`): sequence ids
//!   compare as `int`, UUID ids as TimeUUID, every sequence id sorts before every UUID id.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Length of the text form of a UUID id (`UUIDBasedSSTableId.STRING_LEN`).
const UUID_ID_LEN: usize = 28;
/// Most digits a sequence id may have (`SequenceBasedSSTableId.java:112`).
const SEQUENCE_ID_MAX_DIGITS: usize = 10;
/// 100ns ticks per second, and seconds per day (`UUIDBasedSSTableId.fromString`).
const TICKS_PER_SECOND: i64 = 10_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// The largest sequence id Cassandra can represent: Java `Integer.MAX_VALUE`.
pub const MAX_SEQUENCE_ID: u32 = i32::MAX as u32;

/// A UUID-form generation id: a base-36 rendering of a TimeUUID.
#[derive(Debug, Clone)]
pub struct UuidGenerationId {
    /// The 100ns-tick timestamp Cassandra rebuilds from the text (`uuidTimestamp`).
    timestamp: i64,
    /// The low 64 bits, compared as a signed `long` exactly as `TimeUUID.compareTo` does.
    lsb: i64,
    /// The token as it appears in the file name.
    text: String,
}

impl UuidGenerationId {
    /// The id exactly as written in the file name, e.g. `3gb2_0rbd_2lhua2pw7a2hlyvbsp`.
    pub fn text(&self) -> &str {
        &self.text
    }
}

impl PartialEq for UuidGenerationId {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.lsb == other.lsb
    }
}
impl Eq for UuidGenerationId {}

impl Hash for UuidGenerationId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.lsb.hash(state);
    }
}

impl Ord for UuidGenerationId {
    /// `TimeUUID.compareTo` (`utils/TimeUUID.java:297-302`): timestamp, then `lsb`, both signed.
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.lsb.cmp(&other.lsb))
    }
}
impl PartialOrd for UuidGenerationId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The `<id>` token of an SSTable descriptor, in either of Cassandra's two forms.
///
/// The derived-by-hand [`Ord`] is `SSTableIdFactory.COMPARATOR`: sequence ids by integer value,
/// UUID ids by TimeUUID, and every sequence id before every UUID id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GenerationId {
    /// Digits within the Java `int` range (`0..=`[`MAX_SEQUENCE_ID`]`).
    Sequence(u32),
    /// A 28-character base-36 TimeUUID id.
    Uuid(UuidGenerationId),
}

impl GenerationId {
    /// Parse a descriptor `<id>` token. `None` means the token is neither id form, which makes the
    /// file "not an SSTable component of this table" in Cassandra (`Descriptor.java:392-400`) and
    /// therefore ignored, never an error.
    pub fn parse(token: &str) -> Option<Self> {
        // UUID first (`SSTableIdFactory.java:38-45`). A 28-character token can never be a
        // sequence id (at most 10 digits), so a UUID-shaped token that fails to convert is
        // simply invalid, as `fromString` throwing is in Cassandra.
        if token.len() == UUID_ID_LEN {
            return parse_uuid(token).map(Self::Uuid);
        }
        parse_sequence(token).map(Self::Sequence)
    }

    /// The sequence value, or `None` for a UUID id.
    pub fn sequence(&self) -> Option<u32> {
        match self {
            Self::Sequence(n) => Some(*n),
            Self::Uuid(_) => None,
        }
    }
}

impl fmt::Display for GenerationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequence(n) => write!(f, "{n}"),
            Self::Uuid(u) => f.write_str(u.text()),
        }
    }
}

impl Ord for GenerationId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Sequence(a), Self::Sequence(b)) => a.cmp(b),
            (Self::Uuid(a), Self::Uuid(b)) => a.cmp(b),
            (Self::Sequence(_), Self::Uuid(_)) => Ordering::Less,
            (Self::Uuid(_), Self::Sequence(_)) => Ordering::Greater,
        }
    }
}
impl PartialOrd for GenerationId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn parse_sequence(token: &str) -> Option<u32> {
    if token.is_empty()
        || token.len() > SEQUENCE_ID_MAX_DIGITS
        || !token.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    // At most 10 digits always fit an i64; `Integer.parseInt` then rejects above Integer.MAX_VALUE.
    let value: i64 = token.parse().ok()?;
    u32::try_from(value).ok().filter(|v| *v <= MAX_SEQUENCE_ID)
}

fn parse_uuid(token: &str) -> Option<UuidGenerationId> {
    let bytes = token.as_bytes();
    // Layout: 4 '_' 4 '_' 5 13 (`UUIDBasedSSTableId.Builder.PATTERN`).
    if bytes.get(4) != Some(&b'_') || bytes.get(9) != Some(&b'_') {
        return None;
    }
    let groups = [&token[0..4], &token[5..9], &token[10..15], &token[15..28]];
    if !groups
        .iter()
        .all(|g| g.bytes().all(|b| b.is_ascii_alphanumeric()))
    {
        return None;
    }
    let day = i64::from_str_radix(groups[0], 36).ok()?;
    let second = i64::from_str_radix(groups[1], 36).ok()?;
    let nano = i64::from_str_radix(groups[2], 36).ok()?;
    // `Long.parseUnsignedLong(.., 36)`: overflow past 64 bits throws, so the token is invalid.
    let random = u64::from_str_radix(groups[3], 36).ok()?;
    let timestamp = day
        .checked_mul(SECONDS_PER_DAY)?
        .checked_add(second)?
        .checked_mul(TICKS_PER_SECOND)?
        .checked_add(nano)?;
    Some(UuidGenerationId {
        timestamp,
        // Reinterpret as the signed `long` Java stores and compares.
        lsb: random as i64,
        text: token.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> GenerationId {
        GenerationId::parse(s).unwrap_or_else(|| panic!("{s} must parse"))
    }

    #[test]
    fn sequence_ids_parse_within_the_java_int_range() {
        assert_eq!(id("1"), GenerationId::Sequence(1));
        assert_eq!(id("0"), GenerationId::Sequence(0));
        assert_eq!(id("0000000007"), GenerationId::Sequence(7));
        assert_eq!(id("2147483647"), GenerationId::Sequence(2_147_483_647));
    }

    /// G5.4: above `Integer.MAX_VALUE` (10 digits) and 11 digits are not ids.
    #[test]
    fn sequence_ids_outside_the_limits_do_not_parse() {
        assert!(GenerationId::parse("2147483648").is_none());
        assert!(GenerationId::parse("12345678901").is_none());
        assert!(GenerationId::parse("9999999999").is_none());
        assert!(GenerationId::parse("").is_none());
        assert!(GenerationId::parse("-1").is_none());
        assert!(GenerationId::parse("+1").is_none());
        assert!(GenerationId::parse("1a").is_none());
    }

    /// G5.1: the id Cassandra 5.0.8 wrote with `uuid_sstable_identifiers_enabled: true`.
    #[test]
    fn uuid_id_parses_and_keeps_its_text() {
        let parsed = id("3gb2_0rbd_2lhua2pw7a2hlyvbsp");
        assert_eq!(parsed.to_string(), "3gb2_0rbd_2lhua2pw7a2hlyvbsp");
        assert!(parsed.sequence().is_none());
    }

    #[test]
    fn uuid_matches_case_insensitively_but_enforces_shape() {
        assert!(GenerationId::parse("3GB2_0RBD_2LHUA2PW7A2HLYVBSP").is_some());
        // wrong separator position, wrong length, non-alphanumeric
        assert!(GenerationId::parse("3gb2-0rbd_2lhua2pw7a2hlyvbsp").is_none());
        assert!(GenerationId::parse("3gb2_0rbd_2lhua2pw7a2hlyvbs").is_none());
        assert!(GenerationId::parse("3gb2_0rbd_2lhua2pw7a2hlyvbsp0").is_none());
        assert!(GenerationId::parse("3gb2_0rbd_2lhua2pw7a2hlyvb.p").is_none());
        // 13 base-36 digits above 2^64 overflow `parseUnsignedLong`: not an id
        assert!(GenerationId::parse("0000_0000_00000zzzzzzzzzzzzz").is_none());
    }

    /// G5.2: numeric, not textual.
    #[test]
    fn sequence_ids_order_numerically() {
        let mut ids = vec![id("100"), id("9"), id("10")];
        ids.sort();
        assert_eq!(ids, vec![id("9"), id("10"), id("100")]);
    }

    /// G5.3: every sequence id before every UUID id, whatever the values.
    #[test]
    fn every_sequence_id_sorts_before_every_uuid_id() {
        let uuid = id("0000_0000_000000000000000000");
        let top = id("2147483647");
        assert!(top < uuid);
        assert!(uuid > top);
    }

    /// `TimeUUID.compareTo`: timestamp first (days, then seconds, then ticks), then signed lsb.
    #[test]
    fn uuid_ids_order_by_timeuuid() {
        let early = id("0001_0000_000000000000000001");
        let later_day = id("0002_0000_000000000000000001");
        let later_second = id("0001_0001_000000000000000001");
        let later_tick = id("0001_0000_000010000000000001");
        assert!(early < later_second);
        assert!(later_second < later_day);
        assert!(early < later_tick);
    }

    /// Same timestamp: the low word decides, compared as a SIGNED Java `long`.
    #[test]
    fn signed_lsb_comparison_matches_java_long() {
        // 2 * 36^12 = 9.47e18 > i64::MAX, so Java reads it as a negative long and it sorts
        // BEFORE a small positive low word although it is larger as an unsigned number.
        let negative = id("0001_0000_000002000000000000");
        let small = id("0001_0000_00000000000000000a");
        assert!(negative < small);
        // Both below 2^63: plain numeric order.
        let a = id("0001_0000_00000000000000000a");
        let b = id("0001_0000_00000000000000000b");
        assert!(a < b);
    }

    #[test]
    fn equality_is_by_value_not_text_case() {
        assert_eq!(
            id("3gb2_0rbd_2lhua2pw7a2hlyvbsp"),
            id("3GB2_0RBD_2LHUA2PW7A2HLYVBSP")
        );
    }
}
