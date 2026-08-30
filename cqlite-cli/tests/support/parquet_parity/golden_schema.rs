//! The TOTAL validation of an sstabledump golden's STRUCTURE (#1490 round 17).
//!
//! # Why this is ONE table-driven pass and not a list of checks
//!
//! The eligibility/shape checks this module replaces were written one finding at
//! a time: each round of review found another field that was consumed WITHOUT
//! having been validated, a check was added for that field, and the next round
//! found two more. Rounds 14, 15 and 16 each produced instances of the SAME
//! defect — a field validated for its SHAPE but not its CONTENT (`partition.key`
//! confirmed to be an array without confirming its components are JSON strings;
//! a timestamp confirmed to be a string without confirming it PARSES). Patching
//! the named fields would have produced three more next round, because the
//! generator was the enumeration itself.
//!
//! So validation is TOTAL and DECLARATIVE: [`TOP_FIELDS`] and the tables it
//! reaches describe the WHOLE sstabledump structure this harness consumes — for
//! every field, the JSON type it must have, the GRAMMAR its content must satisfy,
//! whether its absence is tolerable, and who validates it if this pass does not.
//! The walker is driven by that description, and it REFUSES a field the tables do
//! not describe ([`unrecognized_field`]). A field therefore cannot be consumed
//! before it has been validated: to reach a consumer it must appear in a table,
//! and appearing in a table is what gets it checked.
//!
//! # The authority for the tables
//!
//! Cassandra 5.0.8's `org.apache.cassandra.tools.JsonTransformer` — the writer of
//! every committed golden — read at the pinned tag, never from CQLite's own code
//! (#3041). The properties the tables encode, and the line that decides each:
//!
//!   * `partition.key` components are written with `json.writeString(...)` over
//!     `AbstractType.getString` (`serializePartitionKey`), so EVERY component is
//!     a JSON string. Likewise a cell `path` component (`serializeCell`, both the
//!     collection and the non-frozen-UDT branch).
//!   * `clustering` components are written with
//!     `json.writeRawValue(type.toJSONString(...))` (`serializeClustering`), so
//!     they are TYPED JSON — an `int` clustering is a JSON number, a frozen
//!     collection is a JSON array, and an unset trailing component is the string
//!     `"*"`. They are deliberately NOT constrained here: constraining them to
//!     strings would REFUSE a correct golden.
//!   * every timestamp-bearing field (`liveness_info.tstamp`, a cell `tstamp`,
//!     `marked_deleted`, `local_delete_time`) is written by `dateString(...)`,
//!     which is why each is [`Rule::Timestamp`] rather than [`Rule::Text`].
//!     `dateString` returns `Instant.ofEpochSecond(secs, nanos).toString()` —
//!     i.e. `DateTimeFormatter.ISO_INSTANT` over `java.time` — and THAT, not
//!     whatever the shared parser tolerates, is the grammar
//!     [`strict_timestamp_micros`] enforces: a zero-padded 4-digit-year
//!     proleptic-Gregorian date, hour <= 23, minute <= 59, second <= 59
//!     (`java.time` models NO leap seconds, 86 400 s/day, so `:60` is
//!     unwritable), a fraction of at most microsecond precision, and the `Z`
//!     designator. `dateString`'s other branch — sstabledump's `--raw-time` —
//!     writes a BARE INTEGER with no `Z`, which this harness does not consume
//!     and therefore refuses.
//!   * `liveness_info` always carries `tstamp`; a cell tombstone's
//!     `deletion_info` always carries `local_delete_time` (and only a complex
//!     deletion also carries `marked_deleted`).
//!
//! # What this pass deliberately does NOT judge, and who does
//!
//! Two positions hold TYPED DATA whose meaning needs the DECLARED CQL type,
//! which this pass does not have:
//!
//!   * a cell `value` — canonicalized at its declared position by `declared.rs`,
//!     after `golden_text.rs` has refused a duplicate JSON key at any depth;
//!   * a `clustering` component — canonicalized as a key component by
//!     `golden_rows.rs` through the same declared-type door.
//!
//! [`Rule::AnyJson`] records that decision IN THE TABLE, so "nobody checks this"
//! and "someone else checks this" are not the same entry.
//!
//! # Requiredness
//!
//! A field is [`required`] when its ABSENCE would be silently CONSUMED as a
//! default by the lenient shared parser — a container read as empty, a name read
//! as absent, a timestamp read as `None`. A field the harness merely records
//! (`position`, `table kind`) is [`optional`] but still TYPE-checked: the
//! harness's own minimal synthetic goldens omit those, and demanding them would
//! refuse a golden for a field nothing reads.

use serde_json::{Map, Value};

use super::canonical_jsonl::parse_timestamp_micros;

// ---------------------------------------------------------------------------
// The description
// ---------------------------------------------------------------------------

/// What ONE field of the sstabledump structure IS.
#[derive(Debug, Clone, Copy)]
pub enum Rule {
    /// Arbitrary JSON, judged ELSEWHERE by the declared CQL type (`declared.rs`,
    /// `golden_rows.rs`). Recorded rather than omitted so the table distinguishes
    /// "validated by its owner" from "validated by nobody".
    AnyJson,
    /// A JSON string whose content is unconstrained.
    Text,
    /// A JSON string that MUST be a timestamp `sstabledump` could have WRITTEN
    /// — judged by [`strict_timestamp_micros`] BEFORE the shared parser is asked
    /// anything, and then cross-checked against
    /// `canonical_jsonl::parse_timestamp_micros`, which must yield the SAME
    /// instant.
    ///
    /// Asking the shared parser ALONE was the defect (#1490 round 17):
    /// **delegating validation to a lenient parser is not validation.** That
    /// parser NORMALIZES rather than refuses, so a `Some` from it establishes
    /// only that SOME instant could be produced — `2025-01-01T24:00:00Z` yields
    /// the same µs as `2025-01-02T00:00:00Z`, a 7th fractional digit is
    /// truncated away, and `2025-02-30` is a real date to it — and an instant
    /// produced from a malformed spelling compares EQUAL to a correct export,
    /// which is a FALSE PASS. So the SPELLING is judged here and the parser is
    /// then required to AGREE: the two can still never hold different notions of
    /// what instant a golden denotes, and the strictness runs in the only safe
    /// direction (this pass may REFUSE a spelling the parser would have
    /// normalized; it may never ACCEPT one the parser cannot reproduce).
    Timestamp,
    /// A JSON integer.
    Integer,
    /// An sstabledump `rows[]` entry type: only `"row"` is eligible, and every
    /// other spelling is classified by name rather than skipped.
    EntryType,
    /// A JSON object holding exactly these fields.
    Object(&'static [Field]),
    /// A JSON array, every element of which satisfies `elem`.
    Array { elem: &'static Rule },
    /// A field whose mere PRESENCE disqualifies the fixture for physical-dump
    /// parity (#1742). Stronger than any shape check: there is no spelling of it
    /// this oracle would accept, so nothing is validated and nothing is read.
    /// `{name}` in the text is replaced by the owning object's `name` field.
    IneligibleIfPresent(&'static str),
}

/// One field of one object in the structure.
#[derive(Debug, Clone, Copy)]
pub struct Field {
    key: &'static str,
    rule: Rule,
    required: bool,
    /// How the thing INSIDE this field is NAMED in a refusal: `{owner}` is the
    /// containing object's name, `{i}` the array index. Unused for scalar rules.
    label: &'static str,
}

const fn required(key: &'static str, rule: Rule) -> Field {
    Field {
        key,
        rule,
        required: true,
        label: "{owner}",
    }
}

const fn optional(key: &'static str, rule: Rule) -> Field {
    Field {
        key,
        rule,
        required: false,
        label: "{owner}",
    }
}

const fn labeled(f: Field, label: &'static str) -> Field {
    Field {
        key: f.key,
        rule: f.rule,
        required: f.required,
        label,
    }
}

static TEXT: Rule = Rule::Text;
static ANY_JSON: Rule = Rule::AnyJson;
static ROWS_ENTRY: Rule = Rule::Object(ROW_FIELDS);
static CELL: Rule = Rule::Object(CELL_FIELDS);

/// The top of one sstabledump LINE — one partition.
///
/// Field ORDER is the refusal PRECEDENCE: the walker reports the first field
/// that does not hold, so the eligibility-bearing fields come before the ones a
/// reader merely records.
pub const TOP_FIELDS: &[Field] = &[
    labeled(
        required("partition", Rule::Object(PARTITION_FIELDS)),
        "`partition`",
    ),
    labeled(
        required("rows", Rule::Array { elem: &ROWS_ENTRY }),
        "row {i}",
    ),
    optional("table kind", Rule::Text),
];

const PARTITION_FIELDS: &[Field] = &[
    // EVERY component is a JSON string (`serializePartitionKey`). Not a shape
    // detail: a bare numeric component canonicalizes straight to an `Int` and
    // compares EQUAL to a correct export — a FALSE PASS.
    required("key", Rule::Array { elem: &TEXT }),
    optional(
        "deletion_info",
        Rule::IneligibleIfPresent("a partition-level deletion"),
    ),
    optional("position", Rule::Integer),
];

const ROW_FIELDS: &[Field] = &[
    required("type", Rule::EntryType),
    optional(
        "deletion_info",
        Rule::IneligibleIfPresent("a row-level deletion"),
    ),
    labeled(
        optional("liveness_info", Rule::Object(LIVENESS_FIELDS)),
        "{owner} `liveness_info`",
    ),
    // TYPED JSON, not strings — see the module docs.
    optional("clustering", Rule::Array { elem: &ANY_JSON }),
    labeled(
        required("cells", Rule::Array { elem: &CELL }),
        "{owner} cells[{i}]",
    ),
    optional("position", Rule::Integer),
];

const LIVENESS_FIELDS: &[Field] = &[
    // All three are written TOGETHER by `serializeRow` for an expiring row, so
    // each one's presence is a row TTL — and a TTL can expire between fixture
    // generation and test time, which is what disqualifies it.
    optional("ttl", Rule::IneligibleIfPresent("a row TTL")),
    optional("expires_at", Rule::IneligibleIfPresent("a row TTL")),
    optional("expired", Rule::IneligibleIfPresent("a row TTL")),
    // The row writetime a collection-shell shadowing decision FALLS BACK TO.
    required("tstamp", Rule::Timestamp),
];

const CELL_FIELDS: &[Field] = &[
    // First, so a refusal below can name the column it found (`{name}`).
    required("name", Rule::Text),
    optional("ttl", Rule::IneligibleIfPresent("a TTL on column '{name}'")),
    optional(
        "expires_at",
        Rule::IneligibleIfPresent("a TTL on column '{name}'"),
    ),
    optional(
        "expired",
        Rule::IneligibleIfPresent("a TTL on column '{name}'"),
    ),
    // NOT disqualifying on its own: a collection-shell deletion is the ordinary
    // marker an INSERT of a whole non-frozen collection writes, and
    // `golden_rows::project_column` decides from its timestamp which elements are
    // shadowed. So its CONTENT is what must hold.
    labeled(
        optional("deletion_info", Rule::Object(CELL_DELETION_FIELDS)),
        "{owner} `deletion_info`",
    ),
    optional("tstamp", Rule::Timestamp),
    // Every component is a JSON string (`serializeCell`), for the same reason
    // `partition.key`'s are.
    optional("path", Rule::Array { elem: &TEXT }),
    optional("value", Rule::AnyJson),
];

const CELL_DELETION_FIELDS: &[Field] = &[
    // Written only by a COMPLEX (collection-shell) deletion; a per-cell tombstone
    // carries `local_delete_time` alone.
    optional("marked_deleted", Rule::Timestamp),
    required("local_delete_time", Rule::Timestamp),
];

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

/// Validate a whole golden document — every line, every field — against the
/// structure above.
///
/// Run on the very text handed to `canonical_jsonl::parse_document_str_with_keys`
/// (see `golden_rows::load_golden`), so what is validated IS what is parsed.
pub fn validate_golden_text(content: &str) -> Result<(), String> {
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        validate_line(line.trim()).map_err(|e| format!("line {}: {e}", idx + 1))?;
    }
    Ok(())
}

/// One sstabledump line — one PARTITION.
pub fn validate_line(line: &str) -> Result<(), String> {
    let value: Value = serde_json::from_str(line)
        .map_err(|e| format!("an sstabledump line must be one JSON object: {e}"))?;
    let top = value
        .as_object()
        .ok_or_else(|| "an sstabledump line must be one JSON object".to_string())?;
    check_object("the sstabledump line", TOP_FIELDS, top)
}

fn check_object(
    owner: &str,
    fields: &'static [Field],
    object: &Map<String, Value>,
) -> Result<(), String> {
    // The object's own `name`, when it has one: a refusal template may name it
    // (`{name}`), so a per-cell refusal says WHICH column it found.
    let subject = object.get("name").and_then(Value::as_str);
    for field in fields {
        match object.get(field.key) {
            Some(value) => check_value(owner, field, subject, value)?,
            None if field.required => return Err(missing(owner, field)),
            None => {}
        }
    }
    // TOTALITY: a field the tables do not describe is REFUSED, never skipped.
    for key in object.keys() {
        if !fields.iter().any(|f| f.key == key.as_str()) {
            return Err(unrecognized_field(owner, key));
        }
    }
    Ok(())
}

fn check_value(
    owner: &str,
    field: &'static Field,
    subject: Option<&str>,
    value: &Value,
) -> Result<(), String> {
    match &field.rule {
        // Judged by its declared type elsewhere — see the module docs.
        Rule::AnyJson => Ok(()),
        Rule::Text => value
            .as_str()
            .map(|_| ())
            .ok_or_else(|| present_but_invalid(owner, field.key, "a JSON string", value)),
        Rule::Timestamp => {
            let text = value
                .as_str()
                .ok_or_else(|| present_but_invalid(owner, field.key, "a JSON string", value))?;
            // The SPELLING first, against `ISO_INSTANT` as `JsonTransformer`
            // writes it — never by asking the lenient parser whether it managed
            // to produce something.
            let micros = strict_timestamp_micros(text)
                .map_err(|why| malformed_timestamp(owner, field.key, text, &why))?;
            // Then the AGREEMENT, re-established at every field of every golden
            // rather than asserted once in a test: the instant this pass accepts
            // is the instant the canonical parser will read.
            match parse_timestamp_micros(text) {
                Some(parsed) if parsed == micros => Ok(()),
                Some(parsed) => Err(timestamp_disagreement(
                    owner, field.key, text, micros, parsed,
                )),
                None => Err(malformed_timestamp(
                    owner,
                    field.key,
                    text,
                    "it satisfies this pass's ISO_INSTANT grammar yet the canonical parser yields                      `None` for it, so the two disagree about whether it is a timestamp at all",
                )),
            }
        }
        Rule::Integer => {
            if value.is_i64() || value.is_u64() {
                Ok(())
            } else {
                Err(present_but_invalid(
                    owner,
                    field.key,
                    "a JSON integer",
                    value,
                ))
            }
        }
        Rule::EntryType => check_entry_type(owner, field.key, value),
        Rule::Object(fields) => {
            let object = value
                .as_object()
                .ok_or_else(|| present_but_invalid(owner, field.key, "a JSON object", value))?;
            check_object(&label(field.label, owner, None), fields, object)
        }
        Rule::Array { elem } => {
            let items = value
                .as_array()
                .ok_or_else(|| present_but_invalid(owner, field.key, "a JSON array", value))?;
            for (i, item) in items.iter().enumerate() {
                check_element(owner, field, elem, i, item)?;
            }
            Ok(())
        }
        Rule::IneligibleIfPresent(what) => Err(ineligible_at(owner, &subject_named(what, subject))),
    }
}

fn check_element(
    owner: &str,
    field: &'static Field,
    elem: &'static Rule,
    index: usize,
    item: &Value,
) -> Result<(), String> {
    match elem {
        Rule::AnyJson => Ok(()),
        Rule::Text => item
            .as_str()
            .map(|_| ())
            .ok_or_else(|| non_string_component(owner, field.key, index, item)),
        Rule::Object(fields) => {
            let where_ = label(field.label, owner, Some(index));
            let object = item
                .as_object()
                .ok_or_else(|| format!("{where_}: `{}` must hold JSON objects", field.key))?;
            check_object(&where_, fields, object)
        }
        // No table uses another element rule. Refused rather than skipped, so an
        // unimplemented element rule can never become an UNVALIDATED position.
        other => Err(format!(
            "{owner}: internal — `{}` declares the element rule {other:?}, which this walker \
             does not implement; a position it cannot validate is a REFUSAL, never a skip",
            field.key
        )),
    }
}

fn check_entry_type(owner: &str, key: &str, value: &Value) -> Result<(), String> {
    match value
        .as_str()
        .ok_or_else(|| present_but_invalid(owner, key, "a JSON string", value))?
    {
        "row" => Ok(()),
        // The dump's other entry types, each also refused downstream — named HERE
        // too so a malformed or missing `type` cannot become an empty string that
        // reads as "not one of the above". Refusing at `type` is also what keeps
        // a range-tombstone entry's `start`/`end` from reaching the
        // unrecognized-field check: such an entry is never descended into.
        rtype @ ("static_block" | "range_tombstone_bound" | "range_tombstone_boundary") => {
            Err(ineligible_at(owner, &format!("a '{rtype}' entry")))
        }
        other => Err(format!(
            "{owner}: unrecognized sstabledump entry type {other:?} — the harness refuses an \
             entry it cannot classify rather than treat it as a row"
        )),
    }
}

/// Substitute `{owner}` and `{i}` in a field's child label.
fn label(template: &str, owner: &str, index: Option<usize>) -> String {
    let text = template.replace("{owner}", owner);
    match index {
        Some(i) => text.replace("{i}", &i.to_string()),
        None => text,
    }
}

/// Substitute `{name}` in a refusal template with the owning object's `name`.
fn subject_named(template: &str, subject: Option<&str>) -> String {
    match subject {
        Some(name) => template.replace("{name}", name),
        // No `name` to substitute: say so rather than print the placeholder.
        None => template.replace("{name}", "<unnamed>"),
    }
}

// ---------------------------------------------------------------------------
// The timestamp SPELLING
// ---------------------------------------------------------------------------
//
// Authority: Cassandra 5.0.8 `JsonTransformer.dateString`, read at the pinned
// tag —
//
//     long secs   = from.toSeconds(time);
//     long offset = Math.floorMod(from.toNanos(time), 1000_000_000L);
//     return Instant.ofEpochSecond(secs, offset).toString();
//
// `Instant.toString()` is `DateTimeFormatter.ISO_INSTANT`, so EVERY timestamp in
// EVERY committed golden was written by that formatter. What it can and cannot
// write is therefore what a well-formed golden can and cannot say — decided
// here, from Cassandra's writer, never from what CQLite's own parser happens to
// tolerate (#3041: CQLite is not format authority for CQLite).

/// The instant `text` denotes, or WHY it is not a timestamp `sstabledump` could
/// have written.
///
/// Every component is range- and calendar-checked BEFORE any arithmetic, and
/// every step of the arithmetic is checked, so no out-of-range spelling can be
/// normalized — or overflowed — into a plausible instant.
fn strict_timestamp_micros(text: &str) -> Result<i64, String> {
    let body = text.strip_suffix('Z').ok_or_else(|| {
        "it does not end in the `Z` UTC designator `ISO_INSTANT` always writes (a BARE INTEGER \
         with no `Z` is what sstabledump's `--raw-time` emits, and this harness does not consume \
         such a dump)"
            .to_string()
    })?;
    // `ISO_INSTANT` writes `T`; the real sstabledump output in the committed
    // corpus writes a space. Accept exactly those two, tried in the SAME order
    // the canonical parser tries them, so the two can never split one string
    // differently.
    let (date_part, time_part) = body
        .split_once('T')
        .or_else(|| body.split_once(' '))
        .ok_or_else(|| {
            "it has no date/time separator — neither the `T` of `ISO_INSTANT` nor the space real \
             sstabledump writes"
                .to_string()
        })?;

    let (year, month, day) = strict_date(date_part)?;
    let (hour, minute, second, frac_micros) = strict_time(time_part)?;

    let days = days_since_epoch_checked(year, month, day).ok_or_else(|| {
        format!("the date {date_part:?} is outside the range of an epoch instant")
    })?;
    let micros = days
        .checked_mul(86_400)
        .and_then(|s| s.checked_add(hour * 3_600 + minute * 60 + second))
        .and_then(|s| s.checked_mul(1_000_000))
        .and_then(|us| us.checked_add(frac_micros))
        .ok_or_else(|| {
            "the instant it denotes overflows epoch microseconds; the harness refuses it rather \
             than let a wrapped value read as a plausible instant"
                .to_string()
        })?;
    Ok(micros)
}

/// `YYYY-MM-DD`, zero-padded, and a date that EXISTS.
fn strict_date(date_part: &str) -> Result<(i64, i64, i64), String> {
    let parts: Vec<&str> = date_part.split('-').collect();
    let [y, m, d] = parts[..] else {
        return Err(format!(
            "its date {date_part:?} is not the three `-`-separated fields `YYYY-MM-DD` \
             `ISO_INSTANT` writes (it has {})",
            parts.len()
        ));
    };
    // A 4-digit year is what `ISO_INSTANT` writes for years 1000..=9999; outside
    // that it writes a SIGNED, EXPANDED year (`+10000-01-01T00:00:00Z`), which no
    // committed golden carries and which this harness refuses rather than
    // reinterpret. Fixed widths also refuse `2025-1-1`, which the shared parser's
    // `str::parse` would have accepted.
    let year = strict_field(y, 4, "year", date_part)?;
    let month = strict_field(m, 2, "month", date_part)?;
    let day = strict_field(d, 2, "day", date_part)?;
    if !(1..=12).contains(&month) {
        return Err(format!(
            "its month {month} is out of range (`ISO_INSTANT` writes 01..=12)"
        ));
    }
    let last = days_in_month(year, month);
    if !(1..=last).contains(&day) {
        return Err(format!(
            "its day {day} does not exist: {year}-{month:02} has {last} days. `java.time`'s \
             `IsoChronology` is the proleptic Gregorian calendar, so a non-leap February has 28 \
             days and no month has 31 unconditionally — a date that does not exist cannot have \
             been written by `Instant.toString()`, and the shared parser would have rolled it \
             forward into a real instant that compares EQUAL to a correct export"
        ));
    }
    Ok((year, month, day))
}

/// `HH:MM:SS[.f{1,6}]`, zero-padded, every component in range.
fn strict_time(time_part: &str) -> Result<(i64, i64, i64, i64), String> {
    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time_part, None),
    };
    let parts: Vec<&str> = hms.split(':').collect();
    let [h, m, sec] = parts[..] else {
        return Err(format!(
            "its time {time_part:?} is not the three `:`-separated fields `HH:MM:SS` \
             `ISO_INSTANT` writes (it has {})",
            parts.len()
        ));
    };
    let hour = strict_field(h, 2, "hour", time_part)?;
    let minute = strict_field(m, 2, "minute", time_part)?;
    let second = strict_field(sec, 2, "second", time_part)?;
    if hour > 23 {
        return Err(format!(
            "its hour {hour} is out of range. `Instant.toString()` never writes an hour above 23, \
             and the shared parser does not refuse one — it NORMALIZES it, so \
             `2025-01-01T24:00:00Z` yields exactly the µs of `2025-01-02T00:00:00Z` and a \
             malformed golden compares EQUAL to a correct export"
        ));
    }
    if minute > 59 {
        return Err(format!(
            "its minute {minute} is out of range (`ISO_INSTANT` writes 00..=59)"
        ));
    }
    // NO leap second. `dateString` builds an `Instant`, and `java.time` models a
    // day as exactly 86 400 seconds (`Instant`'s epoch-second scale has no leap
    // seconds at all), so `ISO_INSTANT` cannot write `:60`. A `:60` spelling is
    // therefore a malformed golden, not a leap second — and the shared parser
    // would have normalized it into the following minute.
    if second > 59 {
        return Err(format!(
            "its second {second} is out of range. `java.time` models no leap seconds — an \
             `Instant`'s day is exactly 86 400 seconds — so `Instant.toString()` cannot write \
             `:60`, and the shared parser would have rolled it into the following minute"
        ));
    }
    let frac_micros = match frac {
        None => 0,
        Some(f) => strict_fraction(f)?,
    };
    Ok((hour, minute, second, frac_micros))
}

/// A sub-second fraction, in microseconds.
///
/// At most SIX digits: this oracle compares at microsecond resolution (Cassandra
/// writetimes are `TimeUnit.MICROSECONDS`), and the shared parser silently
/// TRUNCATES a 7th digit — so `…00.1234567Z` would have compared equal to
/// `…00.123456Z`. A golden carrying more precision than can be represented is
/// malformed, not roundable.
fn strict_fraction(frac: &str) -> Result<i64, String> {
    if frac.is_empty() {
        return Err(
            "it ends the seconds with a `.` and no digits, which `ISO_INSTANT` never writes"
                .to_string(),
        );
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "its sub-second fraction {frac:?} is not all ASCII digits"
        ));
    }
    if frac.len() > 6 {
        return Err(format!(
            "its sub-second fraction {frac:?} carries {} digits, more than the SIX this oracle \
             compares at (Cassandra writetimes are `TimeUnit.MICROSECONDS`). The shared parser \
             TRUNCATES the extra digits, so the golden would have compared EQUAL to the \
             microsecond-truncated export — a golden carrying unrepresentable precision is \
             malformed, not roundable",
            frac.len()
        ));
    }
    // Right-pad to exactly 6 digits: lossless, because the fraction is at most 6
    // digits long. (`.06` is 60 000 µs, the same instant as `.060000`.)
    let padded = format!("{frac:0<6}");
    padded
        .parse::<i64>()
        .map_err(|e| format!("its sub-second fraction {frac:?} is not a number: {e}"))
}

/// One fixed-width, all-ASCII-digit component.
fn strict_field(text: &str, width: usize, what: &str, whole: &str) -> Result<i64, String> {
    if text.len() != width || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "its {what} {text:?} (in {whole:?}) is not {width} ASCII digits; `ISO_INSTANT` writes \
             every component zero-padded to a fixed width, whereas the shared parser's \
             `str::parse` would also have accepted an unpadded, signed or space-prefixed \
             spelling"
        ));
    }
    text.parse::<i64>()
        .map_err(|e| format!("its {what} {text:?} is not a number: {e}"))
}

/// Days in a proleptic-Gregorian month (`java.time`'s `IsoChronology`).
/// `month` is already validated to 1..=12; the fallthrough keeps the function
/// total rather than panicking.
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days from 1970-01-01 for an ALREADY-VALIDATED proleptic-Gregorian date, every
/// step checked (Howard Hinnant's `days_from_civil`).
fn days_since_epoch_checked(year: i64, month: i64, day: i64) -> Option<i64> {
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = (if y >= 0 { y } else { y.checked_sub(399)? }) / 400;
    let yoe = y.checked_sub(era.checked_mul(400)?)?;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe
        .checked_mul(365)?
        .checked_add(yoe / 4)?
        .checked_sub(yoe / 100)?
        .checked_add(doy)?;
    era.checked_mul(146_097)?
        .checked_add(doe)?
        .checked_sub(719_468)
}

// ---------------------------------------------------------------------------
// The refusals
// ---------------------------------------------------------------------------

fn missing(owner: &str, field: &Field) -> String {
    let key = field.key;
    // An absence is only worth a clause where the LENIENT parser would have
    // CONSUMED it as a default.
    match field.rule {
        Rule::Array { .. } => format!(
            "{owner} has no `{key}`; the harness refuses a golden with no `{key}` rather than \
             read it as an empty one"
        ),
        Rule::Timestamp => format!(
            "{owner} has no `{key}`; the harness refuses a golden with no `{key}` rather than \
             read it as an absent timestamp, which reports nothing and silently moves a \
             collection-shadowing decision onto another timestamp, or onto none"
        ),
        _ => format!("{owner} has no `{key}`"),
    }
}

/// A field that is there but is not what it must be. Named as its OWN state,
/// because the whole point is that the shared parser would have reported it as an
/// ABSENCE.
fn present_but_invalid(owner: &str, key: &str, want: &str, got: &Value) -> String {
    format!(
        "{owner}: `{key}` is PRESENT but is not {want} (it is {}); the harness refuses it \
         rather than read a malformed field as an ABSENT one, which is how a physical-dump \
         eligibility check (#1742) gets silently satisfied",
        clipped(&got.to_string())
    )
}

/// A component of a STRINGIFIED array (`partition.key`, a cell `path`) that is
/// not a JSON string.
///
/// Shape is not content: an array whose components are bare JSON numbers or
/// booleans passes every "is it an array" check, and each component then
/// canonicalizes DIRECTLY to an `Int`/`Bool` that compares EQUAL to a correct
/// export — a FALSE PASS in the one direction this oracle exists to measure.
fn non_string_component(owner: &str, key: &str, index: usize, got: &Value) -> String {
    format!(
        "{owner}: `{key}`[{index}] is not a JSON string (it is {}); sstabledump writes EVERY \
         `{key}` component with `json.writeString` over Cassandra's `AbstractType.getString` \
         (`JsonTransformer.serializePartitionKey`/`serializeCell`, cassandra-5.0.8), so a bare \
         numeric or boolean component is a MALFORMED oracle rather than a value. Canonicalized \
         it would become a typed value directly and compare EQUAL to a correct export, so the \
         harness REFUSES the golden instead",
        clipped(&got.to_string())
    )
}

/// A timestamp-bearing field that IS a string but is not a timestamp
/// `sstabledump` could have WRITTEN.
///
/// Both failure directions are named, because they are different hazards and the
/// harness refuses for both:
///
///   * the shared parser yields `None` — a `None` here REPORTS NOTHING and
///     silently drops a collection-shadowing decision onto the row liveness
///     timestamp (or onto no timestamp at all);
///   * the shared parser yields `Some` for a spelling `ISO_INSTANT` cannot
///     write, because it NORMALIZES (hour 24 → the next midnight, a 7th
///     fractional digit truncated, 2025-02-30 rolled into March) — and an
///     instant produced from a malformed spelling compares EQUAL to a correct
///     export, which is a FALSE PASS. This is the one this refusal exists for:
///     asking `parse_timestamp_micros` whether it returned `Ok` establishes only
///     that SOME instant could be produced, which is not validation.
fn malformed_timestamp(owner: &str, key: &str, got: &str, why: &str) -> String {
    format!(
        "{owner}: `{key}` is a string but is not an sstabledump timestamp (it is {}): {why}. \
         Cassandra 5.0.8 writes every one of these fields with `JsonTransformer.dateString`, \
         which returns `Instant.ofEpochSecond(...).toString()` — `DateTimeFormatter.ISO_INSTANT` \
         — so this spelling is not one the oracle's own writer can produce. The harness refuses \
         it HERE rather than hand it to `canonical_jsonl::parse_timestamp_micros`, which \
         normalizes what it can and so cannot tell a malformed golden from a well-formed one",
        clipped(&format!("{got:?}"))
    )
}

/// The spelling satisfies `ISO_INSTANT` yet this pass and the canonical parser
/// read DIFFERENT instants out of it. Unreachable for any spelling either side
/// accepts today — kept because the whole point of computing the instant here is
/// that the two can never silently diverge: a divergence is a REFUSAL, never the
/// parser's answer winning by default.
fn timestamp_disagreement(owner: &str, key: &str, got: &str, strict: i64, parsed: i64) -> String {
    format!(
        "{owner}: `{key}` is a well-formed `ISO_INSTANT` timestamp ({}) but this pass reads it as \
         {strict}µs while `canonical_jsonl::parse_timestamp_micros` — the grammar the canonical \
         parser uses to build the comparison — reads {parsed}µs. The harness refuses a golden the \
         validator and the parser do not agree about rather than compare against whichever \
         instant the parser chose",
        clipped(&format!("{got:?}"))
    )
}

/// A field the structure tables do not describe.
///
/// The refusal that makes the pass TOTAL: an undescribed field is one nobody
/// validated, and "the harness has not been taught this dump" and "this dump is
/// corrupt" are the same hazard from here.
fn unrecognized_field(owner: &str, key: &str) -> String {
    format!(
        "{owner}: unrecognized field `{key}` — this harness validates the WHOLE sstabledump \
         structure it consumes (`JsonTransformer`, cassandra-5.0.8) and REFUSES a field it does \
         not describe rather than skip past it: an undescribed field is a piece of the golden \
         nobody validated, which is exactly how a malformed oracle reaches a comparison"
    )
}

pub(super) fn ineligible_at(where_: &str, what: &str) -> String {
    format!(
        "{where_} carries {what}: this fixture is NOT eligible for physical-dump \
         parity (a Parquet export is the RECONCILED result set, a JSONL dump is not — \
         issue #1742). Use the query-semantics oracle for it, or drop the case."
    )
}

/// Truncated on a CHAR boundary — a golden can carry any UTF-8, and slicing a
/// byte offset would panic on a multi-byte one.
fn clipped(text: &str) -> String {
    match text.char_indices().nth(60) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text.to_string(),
    }
}
