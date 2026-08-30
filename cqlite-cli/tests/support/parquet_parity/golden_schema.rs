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
    /// A JSON string that MUST parse under the CANONICAL parser's OWN timestamp
    /// grammar — literally `canonical_jsonl::parse_timestamp_micros`, the very
    /// function the parser calls, so the validator and the parser can never hold
    /// two different notions of a valid timestamp.
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
            if parse_timestamp_micros(text).is_none() {
                return Err(unparseable_timestamp(owner, field.key, text));
            }
            Ok(())
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

/// A timestamp-bearing field that IS a string but does not PARSE.
fn unparseable_timestamp(owner: &str, key: &str, got: &str) -> String {
    format!(
        "{owner}: `{key}` is a string but is not an sstabledump timestamp (it is {}); \
         `canonical_jsonl::parse_timestamp_micros` — the SAME grammar the canonical parser \
         uses, so the two can never disagree about what a timestamp is — yields `None` for it, \
         and a `None` here reports nothing: it silently drops a collection-shadowing decision \
         onto the row liveness timestamp (or onto no timestamp at all), which can classify a \
         live element as shadowed or a shadowed one as live. The harness refuses the malformed \
         oracle instead",
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
