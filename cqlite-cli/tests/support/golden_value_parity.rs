//! Value-level oracle for the AD2 JSON/CSV egress parity lane (issue #1491,
//! epic #1469).
//!
//! # The gap this closes
//!
//! Before #1491, nothing compared the CLI's JSON/CSV *values* to the
//! `sstabledump` goldens. `one_shot_e2e_tests.rs::validate_json_structure`
//! asserted only "non-empty array of objects, `len <= reference.len()`", and
//! `export_integration_tests.rs::test_export_json_deterministic` /
//! `test_export_csv_deterministic` asserted shape and row counts. A regression in
//! `ValueFormatter` / `value_to_json` (blob hex, decimal text, timestamp
//! rendering, `null` for an absent cell) therefore passed silently.
//!
//! # The oracle, and why it is the right one
//!
//! The committed `*-Data.db.jsonl` files are Apache Cassandra `sstabledump`
//! output — the *physical-dump* oracle. That is deliberately the correct oracle
//! for an **egress formatting** property: the question here is "is this value
//! rendered the way Cassandra renders it", not "is the post-reconciliation result
//! set right" (that is the query-semantics oracle's job — CLAUDE.md, "Two parity
//! oracles"). Every expectation in this module is therefore derived from the
//! golden bytes or from `sstabledump` semantics; **nothing** is derived from
//! CQLite's own current output.
//!
//! Because the physical dump enumerates on-disk cells rather than a reconciled
//! result set, a table whose golden carries a partition/row deletion, a range
//! tombstone, a static block or a TTL is **not** comparable this way — the CLI
//! legitimately returns a different row set. Those tables are excluded BY NAME,
//! with a reason, in the case table of the test target; this module refuses to
//! parse such a golden ([`golden_rows`] returns `Err`) so an exclusion can never
//! be applied silently or accidentally widened.
//!
//! # Normalization: only where two spellings denote the same value
//!
//! * **Timestamps.** `sstabledump` writes a timestamp cell as
//!   `YYYY-MM-DD HH:MM:SS.mmmZ`; the CLI writes `YYYY-MM-DD HH:MM:SS.mmm+0000`
//!   (and CSV the same). Both are the same instant, so both are canonicalized to
//!   `YYYY-MM-DDTHH:MM:SS.mmmZ`. Only a ZERO UTC offset is accepted — a non-zero
//!   offset is left as opaque text so it FAILS loudly rather than being
//!   silently shifted.
//! * **Numeric text vs JSON number — for a numeric CQL type AND ONLY at the
//!   positions `sstabledump` itself stringifies.** In the JSON lane the two
//!   sides must agree on JSON KIND, so an ordinary `int` cell rendered `"1"`
//!   instead of `1` is a DIVERGENCE. The exception is the positions where
//!   Cassandra's own dumper writes a string: a partition-key component
//!   (`"key": ["1"]`) and a non-frozen collection's cell path
//!   (`"path": ["-5"]`, i.e. a multicell set's elements and a multicell map's
//!   keys). There the GOLDEN's string is read as a number — and only the
//!   golden's: the CLI is held to its declared type's JSON kind everywhere, so a
//!   CLI `"id":"1"` for an `int` partition key is a divergence (finding M1). A
//!   map KEY works the same way: the dump renders a map as a JSON object and an
//!   object key can only be a string, so the GOLDEN key is relaxed while the
//!   CLI's `{"key","value"}` key keeps its declared type's kind (finding N1).
//!   See [`Kinding`], which derives the rule from
//!   `cassandra-5.0.8 JsonTransformer` and the committed DDL. The comparison
//!   itself is the pure-string [`normalize_decimal`] (no `10^scale`
//!   materialization and no `f64` round-trip OF ITS OWN, so a 30-digit `decimal`
//!   arriving as TEXT — which is how the dump writes a cell path — keeps every
//!   digit).
//!
//!   In the CSV lane every cell arrives as text — the format carries no JSON
//!   kinds at all — so a numeric cell is compared by value everywhere.
//!
//!   A `text`/`varchar`/`ascii` value is compared as an EXACT STRING, so the UDT
//!   zip `"22201"` never equals the number `22201` and `"00000"` never equals
//!   `"0"`. The type comes from the committed `CREATE TABLE` (see [`schema`]),
//!   not from the golden's JSON kind — the golden renders a key/path of ANY type
//!   as a string, so its kind cannot answer the question — and it is threaded
//!   through nesting, so a map value or UDT field that is CQL `text` is exact
//!   even when its content looks numeric.
//! * **Map spelling.** `sstabledump` renders a map as a JSON object
//!   (`{"x": 10}`); the CLI renders it as an array of `{"key": …, "value": …}`
//!   pairs. The SPELLING is normalized; the ORDER is not — entries are compared
//!   in EMITTED order, because Cassandra stores a map's entries in key-comparator
//!   order and both sides read the same SSTable (finding N2). Sorting both sides
//!   first, which this lane used to do, made a reordering compare equal.
//!
//!   A map KEY the DDL declares as a CONTAINER is paired the same way, because
//!   `cassandra-5.0.8 MapType.toJSONString` spells the golden's object key as the
//!   key value's own `toJSONString` document: it is parsed and compared as an
//!   ordinary value of the declared key type (issue #3726, [`container`]). The one
//!   position that is not a `toJSONString` document is a MULTICELL map's key, which
//!   is a cell PATH — `writeString(getString(...))` — and is a declared gap.
//! * **UDT fields.** `sstabledump` renders a UDT as a plain field→value object,
//!   and since #3629 so does the JSON egress: the `_type` discriminator it used to
//!   add is GONE, so both sides carry the declared fields and nothing else and
//!   nothing is dropped from either. Authority is
//!   `cassandra-5.0.8:.../UserType.java:261` (`toJSONString`), which emits declared
//!   fields only, with no type key and no keyspace key. KNOWN COVERAGE REDUCTION:
//!   the deleted discriminator check compared the emitted type name against the
//!   committed `CREATE TYPE`, so this lane can no longer detect a UDT resolved
//!   against the WRONG type when two types declare the same field names, order and
//!   types (`collide`/`collide_twin` in `test-data/fixtures/issue_3504/`). That is
//!   unavoidable — the egress no longer carries type identity for a comparator to
//!   check — and the old code was already blind here, refusing outright to compare
//!   any UDT declaring a `_type` field. CSV never rendered a discriminator.
//! * **Two spellings this DELIBERATELY does not distinguish, stated so neither
//!   reads as an oversight.** (1) A timestamp's SPELLING: the separator, a zero
//!   offset's form and the fraction width are normalized away on BOTH sides, so
//!   only the instant is asserted — the golden's form is Cassandra's
//!   `TimestampSerializer` JSON format, and requiring the CLI to copy it would
//!   assert a product decision nothing establishes. (2) A `decimal`'s SCALE:
//!   [`normalize_decimal`] trims trailing fractional zeros, so `1.50` and `1.5`
//!   compare equal although CQL `decimal` carries scale. Measured on the compared
//!   corpus at the time of writing: the only `decimal` values anywhere in it are
//!   `test_signed_coll.signed_special_collections`'s `sd`
//!   (`-999999999999999999999999999999.999`, `-1.5`, `0`,
//!   `123456789012345678901234567890.123`), none of which carries a trailing
//!   fractional zero, so nothing in the corpus is currently hidden by it.
//! * **One narrowing of the PARSE, likewise latent rather than hidden.** Both
//!   sides are read by [`strict_json`], which uses `serde_json`'s own number
//!   handling (i64 / u64 / f64, no arbitrary precision). So an UNQUOTED JSON
//!   number literal too long for that — a >2^53 `varint`, a high-precision
//!   scalar `decimal` — is rounded IDENTICALLY on both sides and would compare
//!   equal in the JSON lane (the CSV lane, whose cells are text, would fail
//!   loudly instead). Measured: zero such literals in any of the 28 compared
//!   tables' goldens. Closing it means arbitrary-precision parsing on both sides,
//!   which is a workspace-wide `serde_json` feature change and not this lane's.
//! * **CSV containers.** CSV carries no types, so a collection/UDT arrives as
//!   one flat text field (`{a, b}`, `[1, 2]`, `{k: v}`) and is decoded back into
//!   the shape the GOLDEN and the DECLARED TYPE jointly state before comparison
//!   — see [`csv_container`], which states the grammar, why the decoder is
//!   deliberately strict (each collection kind must use its own bracket, taken
//!   from the DDL), and the two ambiguities CSV genuinely cannot express. A cell
//!   whose GOLDEN content cannot survive an unquoted rendering is REFUSED, never
//!   guessed, and the refusal is counted and named in the run census. What
//!   survives it is the bracket frame and the body's EMPTINESS — so `null` or an
//!   unrelated non-container spelling is still a divergence (finding N3) — while
//!   WHICH members a refused body holds is NOT compared; `csv_container`'s module
//!   doc states that residual exactly (finding Q1).
//!
//! Everything else is compared byte-exactly, including blob `0x…` hex, decimal
//! text, booleans, UUID text and `null`.
//!
//! # What is NOT normalized, on purpose
//!
//! Everything not listed above. In particular the decoder above never *repairs*
//! a container: a changed separator, a changed bracket, a dropped/re-ordered
//! member and a wrongly rendered scalar all fail, because the expected values
//! come from the golden and the grammar is matched exactly.

#![allow(dead_code)]

// The #3220 TABLE-granular datasets-root rule, reused rather than re-derived: a
// root is chosen by EVIDENCE (does this table's `*-Data.db` exist under it), never
// by a fixed env-first/checkout-first preference. The nested `#[path]` inside that
// file resolves against its own directory, so the cross-crate include is sound.
#[path = "../../../cqlite-core/tests/support/datasets_root.rs"]
pub mod datasets_root;

/// The comparator + CLI-egress readers + fixture staging (split out to keep both
/// files well inside the campsite-rule size target).
#[path = "golden_value_compare.rs"]
pub mod compare;

/// Decoding a CSV container cell back into the golden's shape.
#[path = "golden_csv_container.rs"]
pub mod csv_container;

/// The committed `CREATE TABLE` DDL: the authority for which columns a row must
/// carry and what CQL type each value is (issue #1491 review findings).
#[path = "golden_schema.rs"]
pub mod schema;

/// The dump SHAPES that make a golden non-comparable, and the scan that finds
/// them (split out of the test target under the campsite rule).
#[path = "golden_dump_shapes.rs"]
pub mod dump_shapes;

/// A JSON parse that refuses a duplicate object key, used for BOTH the CLI's own
/// JSON egress and each golden JSONL line (issue #1491 review finding K2).
#[path = "golden_strict_json.rs"]
pub mod strict_json;

/// THREE-VALUED filesystem answers (`verified-absent` / `present` / `unreadable`),
/// through which every filesystem question in this lane is asked — a two-valued
/// `Path::is_dir`/`is_file`/`exists` collapses "cannot tell" onto "not there"
/// (issue #1491 review finding V1 and its two predecessors).
#[path = "golden_fs_probe.rs"]
pub mod fs_probe;

/// The git-committed fixture SET and each committed fixture's ORACLE, both answered
/// from `git ls-files`: a committed case is certified by the COMMITTED golden paired
/// with the committed `*-Data.db`, never by an untracked file that happens to sit
/// beside it (issue #1491 review finding BB1).
#[path = "golden_committed_set.rs"]
pub mod committed_set;

/// WHICH root supplies a case's fixture: a git-committed case is pinned to the
/// checkout copy, a fetched-corpus case walks the candidate roots by evidence
/// (issue #1491 review finding J1).
#[path = "golden_fixture_root.rs"]
pub mod fixture_root;

/// CONTAINERS in the canonical value model: the recursive arms of [`canon_typed`]
/// and the ONE rule for what a golden map key denotes (issue #3726).
#[path = "golden_value_canon_container.rs"]
pub mod container;

use schema::CqlType;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// A row projected onto `column name → value`, with collections already
/// reconstructed into a container value. Both the golden side and the CLI side
/// are reduced to this shape before comparison.
pub type Row = BTreeMap<String, Value>;

/// The storage shape of a NON-frozen collection column, taken from the committed
/// `CREATE TABLE` in `test-data/schemas/*.cql`.
///
/// Required because `sstabledump` flattens a multi-cell collection into one cell
/// per element and the three kinds are only distinguishable by *where* the element
/// lives: a `set` puts it in the cell `path`, a `list` in the cell `value` (its
/// path being an internal timeuuid), a `map` puts the key in the path and the
/// value in the value. Inferring the kind from "is the value empty" would be
/// exactly the byte-pattern guessing the no-heuristics mandate (#28) forbids, so
/// the kind is DECLARED and an undeclared multi-cell column is a hard error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Multicell {
    Set,
    List,
    Map,
}

/// Which egress format is being compared. Affects scalar canonicalization only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Egress {
    /// `export --format json`: values keep their JSON kind (number/bool/null).
    Json,
    /// `export --format csv`: every cell is text, and an empty TOP-LEVEL field
    /// reads as `null` (see [`Depth`] — inside a container it does not).
    Csv,
}

/// Where a value sits inside its column's value tree.
///
/// Needed because CSV's empty-field ambiguity is a property of the FIELD, not of
/// the value. At the top level the writer has exactly one spelling — an empty
/// field — for both an absent value and an empty `text`, so the two are genuinely
/// indistinguishable. One level in that is no longer true: `ValueFormatter` spells
/// a null member `null`, so `{last_name: }` and `{last_name: null}` are DIFFERENT
/// renderings, and collapsing empty onto null there would accept a member the
/// format can perfectly well tell apart (issue #1491 review finding F1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Depth {
    /// The whole CSV field / the whole JSON column value.
    TopLevel,
    /// A collection member, a map key or value, a UDT field, a tuple slot.
    Inside,
}

/// How the GOLDEN spells this position's JSON kind — i.e. whether a numeric JSON
/// *string* found HERE, on the golden side, denotes a number.
///
/// A statement about ONE side. `compare::compare_value_at` applies it to the
/// golden and holds the CLI to [`Kinding::Natural`] at every position, because the
/// stringification below is `sstabledump`'s documented behaviour and not a licence
/// for the CLI to spell a number as a string (issue #1491 review finding M1).
///
/// Derived from Cassandra's own dumper, `cassandra-5.0.8`
/// `org.apache.cassandra.tools.JsonTransformer`, which uses exactly two writers:
///
///   * `json.writeString(type.getString(v))` — the value becomes a JSON STRING
///     whatever its CQL type. Used by `serializePartitionKey` for EVERY partition
///     key component, and by `serializeCell` for a non-frozen collection's cell
///     `path` (a multicell set's element, a multicell map's key).
///   * `json.writeRawValue(type.toJSONString(v, …))` — the value keeps its
///     natural JSON kind, so a numeric type yields a JSON NUMBER. Used by
///     `serializeClustering` for every clustering value and by `serializeCell`
///     for every cell VALUE (hence a list's elements, a frozen collection's
///     members and a UDT's fields).
///
/// So cross-kind numeric normalization is CORRECT at the first set of positions
/// and WRONG everywhere else: applying it everywhere let an ordinary `int` cell
/// rendered as `"1"` pass as `1` (issue #1491 review finding R1).
///
/// # Which TYPES the two writers spell differently
///
/// Numbers are not the only ones, so the whole native type set was walked against
/// `cassandra-5.0.8` (finding T1). `getString` is `serializer.toString(deserialize(v))`
/// (`AbstractType`), and the default `toJSONString` is
/// `'"' + Objects.toString(deserialize(v)) + '"'`:
///
///   * **DIVERGE IN KIND** — `int`/`bigint`/`smallint`/`tinyint`/`varint`/`float`/
///     `double`/`decimal` (`"1"` vs `1`) and **`boolean`** (`"true"` vs `true`,
///     `BooleanType.toJSONString` = `deserialize(buffer).toString()` written raw).
///     Both are relaxed in [`canon_typed`], golden-side only;
///   * **DIVERGE IN SPELLING, same kind** — **`blob`** (`BytesSerializer.toString`
///     is the bare hex, `BytesType.toJSONString` is `"0x" + hex`) and `timestamp`
///     (`FORMATTER_UTC`'s `yyyy-MM-dd'T'HH:mm:ss.SSSX` vs `FORMATTER_TO_JSON`'s
///     `yyyy-MM-dd HH:mm:ss.SSSX` — the two spellings [`canon_timestamp`] already
///     accepts, which is why this position needed no separate relaxation);
///   * **IDENTICAL** — `text`/`varchar`/`ascii` (`getString` is the raw string and
///     `writeString` escapes it; `UTF8Type`/`AsciiType.toJSONString` quote it with
///     `JsonUtils.quoteAsJsonString`), `uuid`/`timeuuid`/`duration` (the default
///     `toJSONString`, whose `Object.toString()` is what those serializers return),
///     and `date`/`time` (both override `toJSONString` to call `serializer.toString`,
///     the very function `getString` uses). `counter` cannot occupy a stringified
///     position at all.
///
/// Not covered, and named rather than implied: a FROZEN collection/tuple/UDT as a
/// partition-key component. `getString` spells the whole frozen value as one string,
/// which is nothing like the CLI's container, so the two sides mismatch by SHAPE and
/// `compare::compare_value_at` fails loudly — a false divergence, but one that needs
/// a spelling oracle of its own rather than a kinding relaxation, and no committed
/// fixture has one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kinding {
    /// The golden keeps its natural JSON kind here, so both sides must agree on
    /// kind: for a numeric column `1` and `"1"` are DIFFERENT renderings.
    Natural,
    /// `sstabledump` stringified the golden here, so a numeric golden string and
    /// a numeric CLI number denote the same value. Bounded to partition keys,
    /// multicell cell paths and map keys.
    ///
    /// Applied to the GOLDEN side only, so it relaxes what the golden may be
    /// SPELLED as and never what the CLI may emit: at a stringified position the
    /// CLI must still render a numeric column as a JSON number.
    ///
    /// That holds at a map KEY too. The golden spells a map as a JSON object,
    /// whose key can only be a string, so the golden key is read with this
    /// kinding — but the CLI spells a map as an array of `{"key","value"}`
    /// objects, whose `key` keeps its declared type's JSON kind, so the CLI key is
    /// held to [`Kinding::Natural`] like every other CLI value (issue #1491 review
    /// finding N1). See `compare::compare_map`.
    Stringified,
}

/// WHICH SIDE of the comparison a value came from.
///
/// A structural fact about the CALLER, carried explicitly for the same reason
/// [`container::MapKeySpelling`] is (issue #3726): the two sides spell a MAP
/// differently BY CONSTRUCTION — the dump writes a JSON object, the egress a
/// `{key,value}` array — and inferring which is which from the shape in front of you
/// is exactly what lets a regression that emitted the OTHER side's spelling
/// canonicalize equal.
///
/// It matters only inside [`container`]: at a whole map COLUMN the comparator's own
/// `(Value::Object, Value::Array)` match already pins each side, but a map nested
/// inside a container map KEY is walked by [`canon_typed`] ALONE, so nothing else is
/// left to catch it there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The `sstabledump` JSONL golden.
    Golden,
    /// The CLI's own egress, as read back (CSV containers arrive decoded).
    Cli,
}

/// A canonical scalar: the unit of value equality.
///
/// `Ord` is derived so a collection of canonical values can be SORTED — the
/// row-order check compares the two sides' key multisets that way (see
/// `compare::row_order_divergence`). It is a total order for that purpose only and
/// carries no semantic meaning: it orders by variant first, so it is not the CQL
/// comparator and must never be used to decide what order an egress should emit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Canon {
    Null,
    Bool(bool),
    /// A number, canonicalized as exact decimal text (see [`normalize_decimal`]).
    Num(String),
    /// Opaque text: blob hex, UUID, `text`, and a canonicalized timestamp.
    Text(String),
    /// A list, a set or a tuple: every one of the three is a JSON ARRAY in both the
    /// dump's spelling and the egress's, so one variant serves all three and the
    /// DECLARED type — which [`canon_typed`] has already applied to each member —
    /// is what distinguishes them (issue #3726).
    Seq(Vec<Canon>),
    /// A map, IN EMITTED ORDER. Order-sensitive on purpose: Cassandra stores a
    /// map's entries in key-comparator order and both the dump and a reader of the
    /// same SSTable see that order, so a canonical form that sorted them would make
    /// a reordering compare equal — issue #1491 finding N2, which is why
    /// `compare::compare_map` compares positionally.
    Entries(Vec<(Canon, Canon)>),
    /// A UDT, in DECLARED field order (`cassandra-5.0.8 UserType.toJSONString`
    /// iterates the declared type list). The names are carried for
    /// [`Canon::describe`]; equality is decided by the whole sequence, and
    /// `container::canon_udt` has already refused any value whose field set or order
    /// is not the DDL's.
    Fields(Vec<(String, Canon)>),
}

impl Canon {
    /// The CSV projection: CSV carries no JSON kinds, so a boolean is compared as
    /// its text spelling and numbers stay numeric (`1` == `"1"`).
    ///
    /// At [`Depth::TopLevel`] an EMPTY string collapses onto `null`, because the
    /// format cannot distinguish them: the CLI writes an absent value as an empty
    /// field and an empty `text` value as the same empty field. Cassandra's own
    /// CSV egress (`cqlsh COPY TO`) has exactly this ambiguity, so it is a
    /// property of the format, not of CQLite — and the JSON lane keeps the
    /// distinction strict (`null` vs `""`), so it is still asserted somewhere.
    ///
    /// At [`Depth::Inside`] the collapse is NOT applied: a container member has a
    /// distinct `null` spelling, so an empty member and a null member are
    /// different values and must compare as such (review finding F1).
    /// A CONTAINER is returned unchanged, and that is not an omission: both rules
    /// above are about a SCALAR's spelling, and every member of a container has
    /// already been canonicalized — hence projected — at its own [`Depth::Inside`]
    /// by `container::canon_member`. Collapsing anything at the container level
    /// would apply a TopLevel rule to a member that is not at the top level.
    fn for_csv(self, depth: Depth) -> Canon {
        match self {
            Canon::Bool(b) => Canon::Text(b.to_string()),
            Canon::Text(t) if t.is_empty() && depth == Depth::TopLevel => Canon::Null,
            other => other,
        }
    }

    /// The DIAGNOSTIC rendering — and, for a map entry, the PATH a declared gap is
    /// matched against by exact string (`compare::compare_map` builds the path from
    /// this). The container arms are therefore INJECTIVE: two distinct values may
    /// never describe alike, or one gap would silently cover both. See
    /// `container::escape` for how, and for the trivial collision an unescaped
    /// rendering has (issue #1491 finding DD1, one level down).
    pub fn describe(&self) -> String {
        match self {
            Canon::Null => "null".to_string(),
            Canon::Bool(b) => format!("bool:{b}"),
            Canon::Num(n) => format!("num:{n}"),
            Canon::Text(t) => format!("text:{t}"),
            Canon::Seq(items) => container::describe_seq(items),
            Canon::Entries(entries) => container::describe_entries(entries),
            Canon::Fields(fields) => container::describe_fields(fields),
        }
    }
}

// ===========================================================================
// Scalar canonicalization
// ===========================================================================

/// Canonicalize a JSON scalar WITHOUT a declared type: ordering keys and failure
/// messages only.
///
/// Deliberately NOT the comparison path — [`canon_typed`] is. Untyped, a numeric
/// spelling has to be read numerically so that the golden's string `"1"` and the
/// CLI's number `1` produce the same ORDERING key and the two sides pair up; using
/// the same rule for equality is what let a `text` `"22201"` equal the number
/// `22201`, which is the false-pass this split closes. A permissive ordering key
/// can only mis-pair rows (and any mis-pairing then surfaces as a value diff),
/// while a permissive equality rule silently passes a regression.
pub fn canon_scalar(v: &Value, egress: Egress) -> Result<Canon, String> {
    let canon = match v {
        Value::Null => Canon::Null,
        Value::Bool(b) => Canon::Bool(*b),
        Value::Number(n) => match normalize_decimal(&n.to_string()) {
            Some(text) => Canon::Num(text),
            // Unreachable for any JSON number serde can produce; reported rather
            // than silently coerced so an unexpected spelling cannot pass.
            None => return Err(format!("uncanonicalizable JSON number {n}")),
        },
        Value::String(s) => canon_text(s),
        Value::Array(_) | Value::Object(_) => {
            return Err("container value in a scalar position".to_string())
        }
    };
    Ok(match egress {
        Egress::Json => canon,
        // The permissive TopLevel projection: this is the ORDERING/diagnostic
        // path, where collapsing empty onto null can only affect a sort position
        // or a message, never a verdict.
        Egress::Csv => canon.for_csv(Depth::TopLevel),
    })
}

/// Canonicalize a scalar whose declared CQL type is KNOWN — the comparison path.
///
/// This, not [`canon_scalar`], decides value equality. Two things bound the
/// numeric normalization, and both are needed:
///
///   * the declared TYPE — it is applied only where the DDL says the value is a
///     number, so a `text` column holding `"22201"` or `"00000"` is compared as
///     the exact string it is;
///   * the [`Kinding`] the CALLER states for this value — in the JSON lane a
///     numeric string is read as a number only where `sstabledump` stringifies,
///     so an ordinary numeric cell must match by JSON kind as well as by value.
///     `compare::compare_value_at` passes the position's kinding for the GOLDEN
///     and [`Kinding::Natural`] for the CLI, so the relaxation never licenses a
///     CLI spelling (finding M1).
///
/// A JSON number arriving in a text-typed column is canonicalized as a number
/// precisely so that it compares UNEQUAL to the golden's string and the failure
/// message names both kinds.
pub fn canon_typed(
    v: &Value,
    egress: Egress,
    ty: &CqlType,
    depth: Depth,
    kinding: Kinding,
    side: Side,
) -> Result<Canon, String> {
    // A CONTAINER type is canonicalized RECURSIVELY, by [`container`] (issue #3726).
    // `depth` deliberately does not travel with it: [`Canon::for_csv`] is a rule
    // about a SCALAR's spelling, and each member is canonicalized — hence projected
    // — at its own [`Depth::Inside`] by `container::canon_member`, so applying this
    // position's depth to the container as a whole would apply a TopLevel rule to
    // values that are not at the top level. The scalar arms below keep the
    // "container value where the schema declares the scalar type" refusal, which is
    // now exactly what it says: a container arriving where the DDL declares a
    // scalar.
    if container::is_container_type(ty) {
        return container::canon_container(v, egress, ty, kinding, side);
    }
    // A SCALAR has one spelling per side by construction, so `side` says nothing
    // here: the asymmetry a scalar needs is already carried by `kinding` (only the
    // GOLDEN is ever given [`Kinding::Stringified`]).
    let _ = side;
    // May a numeric TEXT be read as a NUMBER here?
    let cross_kind = match egress {
        // CSV carries no JSON kinds at all — the reader hands every cell over as
        // text — so a numeric cell is compared by value throughout the lane.
        Egress::Csv => true,
        // JSON: only where the golden itself is a string by construction.
        Egress::Json => kinding == Kinding::Stringified,
    };
    // Is this the side, and the position, `sstabledump` wrote with
    // `writeString(type.getString(v))`? NARROWER than `cross_kind`, which also
    // absorbs CSV's kind-blindness. Only the GOLDEN is ever given
    // [`Kinding::Stringified`] (`compare::compare_value_at` and
    // `compare::compare_map` hold the CLI to [`Kinding::Natural`] everywhere), so
    // the two relaxations keyed on it below cannot license a CLI spelling — in
    // either egress. Using `cross_kind` for them would have handed the blob
    // relaxation to the CLI's own CSV cells, where it would mask a missing `0x`.
    let golden_stringified = kinding == Kinding::Stringified;
    let canon = match v {
        Value::Null => Canon::Null,
        Value::Bool(b) => Canon::Bool(*b),
        Value::Number(n) => match normalize_decimal(&n.to_string()) {
            Some(text) => Canon::Num(text),
            // Unreachable for any JSON number serde can produce; reported rather
            // than silently coerced so an unexpected spelling cannot pass.
            None => return Err(format!("uncanonicalizable JSON number {n}")),
        },
        Value::String(s) => match ty {
            // The one place a numeric TEXT may be read as a number: a numeric
            // declared type AT a position where the two sides may legitimately
            // spell the kind differently. Elsewhere the string stays a string, so
            // it compares UNEQUAL to the golden's number and the message names
            // both kinds.
            CqlType::Numeric(_) if cross_kind => match normalize_decimal(s) {
                Some(text) => Canon::Num(text),
                // e.g. the golden's `Infinity`/`NaN` for a double: left opaque so
                // it fails loudly rather than being coerced.
                None => Canon::Text(s.clone()),
            },
            // A BOOLEAN has the numeric case's shape: `BooleanSerializer.toString`
            // (`cassandra-5.0.8`) returns `value.toString()`, i.e. the TEXT
            // `true`/`false`, so `writeString(getString(v))` spells a stringified
            // boolean `"true"`; `BooleanType.toJSONString` returns the same
            // `Boolean.toString()` written with `writeRawValue`, so at every other
            // position the golden carries the raw JSON boolean `true`. Without this,
            // a boolean partition key / multicell-set element / map key made a
            // CORRECT CLI diverge — the false-divergence class this lane treats as a
            // defect in its own right (issue #1491 review finding T1).
            //
            // Asymmetric like the numeric relaxation, and by the same mechanism:
            // `golden_stringified` is only ever true for the golden side, so a CLI
            // that spelled a boolean column `"true"` is still held to its declared
            // type's JSON kind and fails.
            CqlType::Boolean if golden_stringified => match s.as_str() {
                "true" => Canon::Bool(true),
                "false" => Canon::Bool(false),
                // Not a spelling `BooleanSerializer.toString` can produce, so it
                // stays opaque and fails loudly rather than being coerced.
                _ => Canon::Text(s.clone()),
            },
            // A BLOB is the same family one step over: the divergence is in the
            // SPELLING rather than the kind. `BytesSerializer.toString` returns the
            // bare lowercase hex (`Hex.bytesToHex`, whose `byteToChar` table is
            // `Integer.toHexString`), so a stringified blob golden reads `"deadbeef"`
            // — and the empty blob reads `""` — while `BytesType.toJSONString`
            // returns `"0x" + <the same hex>`, which is what every other position
            // and the CLI carry. So the golden's bare hex is read as the `0x` form it
            // denotes.
            //
            // Guarded on the shape rather than applied blindly: an already-prefixed
            // or non-hex string is NOT what `getString` emits, so it stays exact and
            // a regression that dropped the prefix on the CLI side still fails.
            CqlType::Blob if golden_stringified => match stringified_blob_spelling(s) {
                Some(csv) => Canon::Text(csv),
                // Not a spelling `BytesSerializer.toString` can produce, so it
                // stays exact and fails loudly rather than being coerced.
                None => Canon::Text(s.clone()),
            },
            CqlType::Timestamp => match canon_timestamp(s) {
                Some(text) => Canon::Text(text),
                None => Canon::Text(s.clone()),
            },
            // text / varchar / ascii / uuid / date / time / duration / inet: EXACT
            // — for each of those, `cassandra-5.0.8` spells `getString` and
            // `toJSONString` the same way, so no relaxation applies (the census is in
            // the [`Kinding`] doc comment).
            _ => Canon::Text(s.clone()),
        },
        Value::Array(_) | Value::Object(_) => {
            return Err(format!(
                "container value where the schema declares the scalar type `{}`",
                ty.describe()
            ))
        }
    };
    Ok(match egress {
        Egress::Json => canon,
        Egress::Csv => canon.for_csv(depth),
    })
}

/// The `0x…` spelling a STRINGIFIED blob golden denotes, or `None` when `s` is not
/// a spelling `BytesSerializer.toString` can produce.
///
/// The one place this repository states the blob half of `sstabledump`'s
/// two-writer split, so the comparison ([`canon_typed`]) and the CSV lane's
/// structural refusal question (`csv_container::golden_rendering`) cannot drift
/// apart on it. Read from the PIN, `cassandra-5.0.8`:
/// `BytesSerializer.toString` is `ByteBufferUtil.bytesToHex`, i.e. the bare
/// lowercase hex (`""` for the empty blob), while every non-stringified position
/// carries `BytesType.toJSONString`'s `"0x" + <the same hex>`.
pub fn stringified_blob_spelling(s: &str) -> Option<String> {
    is_bare_lowercase_hex(s).then(|| format!("0x{s}"))
}

/// Is `s` exactly what `Hex.bytesToHex` emits — an even-length run of lowercase
/// hex digits, the empty string included (the empty blob's `getString`)?
///
/// Deliberately not "looks hexish": an odd length, an uppercase digit or a `0x`
/// prefix is not a spelling `BytesSerializer.toString` can produce, so it must not
/// be normalized into one.
fn is_bare_lowercase_hex(s: &str) -> bool {
    s.len() % 2 == 0
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Canonicalize a textual scalar: a timestamp spelling first, then a boolean, a
/// blob and a numeric spelling, else opaque text.
///
/// The UNTYPED projection, so every rule here is deliberately permissive — it
/// serves only the PAIRING key and diagnostics (see [`canon_scalar`]). Each rule
/// exists because `sstabledump`'s two writers spell the SAME value differently at a
/// stringified position, so a key that cannot see through that difference pairs the
/// golden's `"1"` against some other row than the CLI's `1` and reports a divergence
/// in every column of both (issue #1491 review finding T1, the pairing-key half).
/// The three relaxations therefore mirror the golden-side ones in [`canon_typed`]:
///
///   * a numeric spelling → a number (`"1"` pairs with `1`);
///   * `true`/`false` → a boolean (a stringified boolean partition key `"true"`
///     pairs with the CLI's `true`);
///   * a `0x`-prefixed bare-hex spelling → the bare hex `BytesSerializer.toString`
///     emits (`"0xdeadbeef"` pairs with `"deadbeef"`, and `"0x"` with `""`).
///     Normalized toward the BARE form because untyped there is no way to tell a
///     blob from a `text` value that happens to be hex, so the reverse direction
///     would rewrite ordinary text.
///
/// None of this touches value EQUALITY, which is [`canon_typed`]'s and is driven by
/// the declared type: a `text` column holding `"true"` or `"0xdeadbeef"` still
/// compares as the exact string it is. Nor does it touch the emitted ROW ORDER,
/// which used to read this projection and no longer does: two distinct legal `text`
/// keys `"1"` and `"1.0"` canonicalize alike here, so a swap of those two rows was
/// invisible — `compare::row_order_divergence` is typed for exactly that reason
/// (finding V2).
pub fn canon_text(s: &str) -> Canon {
    if let Some(ts) = canon_timestamp(s) {
        return Canon::Text(ts);
    }
    match s {
        "true" => return Canon::Bool(true),
        "false" => return Canon::Bool(false),
        _ => {}
    }
    if let Some(hex) = s.strip_prefix("0x") {
        if is_bare_lowercase_hex(hex) {
            return Canon::Text(hex.to_string());
        }
    }
    match normalize_decimal(s) {
        Some(text) => Canon::Num(text),
        None => Canon::Text(s.to_string()),
    }
}

/// Canonicalize `YYYY-MM-DD[ T]HH:MM:SS[.frac](Z|+0000|+00:00)` to
/// `YYYY-MM-DDTHH:MM:SS.fffZ`, or `None` when `s` is not that shape.
///
/// A NON-ZERO offset deliberately returns `None`: shifting it here would silently
/// reinterpret the value, while leaving it opaque makes the comparison fail and
/// name the two spellings.
pub fn canon_timestamp(s: &str) -> Option<String> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    if !(digits(0..4) && b[4] == b'-' && digits(5..7) && b[7] == b'-' && digits(8..10)) {
        return None;
    }
    if !(b[10] == b' ' || b[10] == b'T') {
        return None;
    }
    if !(digits(11..13) && b[13] == b':' && digits(14..16) && b[16] == b':' && digits(17..19)) {
        return None;
    }
    let mut rest = &s[19..];
    let mut frac = String::new();
    if let Some(stripped) = rest.strip_prefix('.') {
        let n = stripped.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            return None;
        }
        frac = stripped[..n].to_string();
        rest = &stripped[n..];
    }
    // Zero UTC offsets only (see the doc comment).
    if !matches!(rest, "Z" | "+0000" | "+00:00" | "-0000" | "-00:00") {
        return None;
    }
    // Trailing zeros in the fraction are not significant; `.000` and no fraction
    // denote the same instant.
    let frac = frac.trim_end_matches('0');
    let date_time = &s[..19];
    let date_time = date_time.replacen(' ', "T", 1);
    if frac.is_empty() {
        Some(format!("{date_time}Z"))
    } else {
        Some(format!("{date_time}.{frac}Z"))
    }
}

/// Largest number of zeros this will pad when re-scaling an exponent. Bounds the
/// allocation so a hostile-looking `1e999999999` in a golden cannot blow up the
/// test process; such an input is reported as non-numeric (opaque text) instead.
const MAX_DECIMAL_PAD: i64 = 4096;

/// Exact decimal canonicalization of a numeric TEXT, or `None` when `s` is not a
/// plain decimal literal (`0x…` hex, `Infinity`, `NaN`, a UUID, … all return
/// `None` and are then compared as opaque text).
///
/// Pure string arithmetic: no `10^scale` materialization and no `f64` round-trip,
/// so a 30-digit `decimal` from a golden keeps every digit. Negative zero is
/// preserved (`-0.0` → `-0`), because Cassandra distinguishes `-0.0` from `0.0`
/// and the goldens contain both.
pub fn normalize_decimal(s: &str) -> Option<String> {
    let mut rest = s;
    let negative = match rest.as_bytes().first() {
        Some(b'-') => {
            rest = &rest[1..];
            true
        }
        Some(b'+') => {
            rest = &rest[1..];
            false
        }
        _ => false,
    };
    let (mantissa, exp_text) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|c| c.is_ascii_digit())
        || !frac_part.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let exp: i64 = match exp_text {
        None => 0,
        Some(t) => {
            if t.is_empty() {
                return None;
            }
            let (sign, mag) = match t.as_bytes()[0] {
                b'-' => (-1i64, &t[1..]),
                b'+' => (1i64, &t[1..]),
                _ => (1i64, t),
            };
            if mag.is_empty() || !mag.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            // A magnitude too large to matter is refused, not saturated: a
            // saturated exponent would silently change the value.
            let mag: i64 = mag.parse().ok()?;
            sign.checked_mul(mag)?
        }
    };

    let digits: String = format!("{int_part}{frac_part}");
    let point = i64::try_from(int_part.len()).ok()?.checked_add(exp)?;
    let len = i64::try_from(digits.len()).ok()?;

    let text = if point <= 0 {
        let pad = point.checked_neg()?;
        if pad > MAX_DECIMAL_PAD {
            return None;
        }
        format!("0.{}{}", "0".repeat(pad as usize), digits)
    } else if point >= len {
        let pad = point.checked_sub(len)?;
        if pad > MAX_DECIMAL_PAD {
            return None;
        }
        format!("{}{}", digits, "0".repeat(pad as usize))
    } else {
        let cut = point as usize;
        format!("{}.{}", &digits[..cut], &digits[cut..])
    };

    // Trim to a single canonical spelling per value.
    let text = if text.contains('.') {
        let trimmed = text.trim_end_matches('0');
        trimmed.trim_end_matches('.').to_string()
    } else {
        text
    };
    let (whole, fraction) = match text.split_once('.') {
        Some((w, f)) => (w, Some(f)),
        None => (text.as_str(), None),
    };
    let whole_trimmed = whole.trim_start_matches('0');
    let whole_out = if whole_trimmed.is_empty() {
        "0"
    } else {
        whole_trimmed
    };
    let body = match fraction {
        Some(f) => format!("{whole_out}.{f}"),
        None => whole_out.to_string(),
    };
    Some(if negative { format!("-{body}") } else { body })
}

// ===========================================================================
// Golden (sstabledump JSONL) → rows
// ===========================================================================

/// A golden document's ARRAY field, read strictly.
///
/// Absent means the empty array — `sstabledump` legitimately omits `rows` for a
/// partition with none, and `cells`/`clustering` for a row with none. PRESENT BUT
/// NOT AN ARRAY is an ERROR, never the empty array: `and_then(Value::as_array)
/// .unwrap_or(&[])` read "I could not tell what this is" as "there is nothing
/// here", so a `rows`/`cells` field of any other JSON shape silently contributed
/// ZERO rows or ZERO cells — dropping part of the oracle while every surviving
/// sibling kept the comparison non-empty and green.
pub fn array_field<'v>(
    owner: &'v Value,
    name: &str,
    at: &dyn Fn() -> String,
) -> Result<&'v [Value], String> {
    match owner.get(name) {
        None => Ok(&[]),
        Some(Value::Array(items)) => Ok(items.as_slice()),
        Some(other) => Err(format!(
            "{}: `{name}` is {}, not an array — a shape this reader cannot enumerate must \
             be reported, never read as an empty one",
            at(),
            shape_of(other)
        )),
    }
}

/// The JSON shape name of a value, for the diagnostic above.
fn shape_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Parse a `*-Data.db.jsonl` golden into comparable rows.
///
/// `Err` means the golden is NOT comparable to a reconciled CLI result set (a
/// partition/row deletion, a range tombstone, a static block, a TTL, an
/// undeclared multi-cell column, a key arity that contradicts the declared
/// schema). It is a hard error rather than a skip so that a table's presence in
/// the parity set is always a decision someone made explicitly.
pub fn golden_rows(
    jsonl: &str,
    pk: &[&str],
    ck: &[&str],
    multicell: &[(&str, Multicell)],
) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    for (lineno, line) in jsonl.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let at = || format!("golden line {}", lineno + 1);
        // Strict: a duplicate object key in the GOLDEN would silently discard part
        // of the oracle — the same shape as two multicell map cells for one key,
        // which this reader already refuses rather than collapses (finding K2).
        let doc: Value = strict_json::parse(line, &at())?;
        let partition = doc
            .get("partition")
            .ok_or_else(|| format!("{}: no `partition`", at()))?;
        if partition.get("deletion_info").is_some() {
            return Err(format!(
                "{}: partition deletion marker — the physical dump keeps a \
                 partition the CLI's reconciled result set drops",
                at()
            ));
        }
        let keys = partition
            .get("key")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("{}: no `partition.key` array", at()))?;
        if keys.len() != pk.len() {
            return Err(format!(
                "{}: golden partition key arity {} but {} partition column(s) declared ({pk:?})",
                at(),
                keys.len(),
                pk.len()
            ));
        }
        for row in array_field(&doc, "rows", &at)? {
            rows.push(golden_row(row, keys, pk, ck, multicell, &at)?);
        }
    }
    Ok(rows)
}

/// One `sstabledump` cell of a NON-frozen collection, with the one fact the
/// reconstruction has to know before it can use the cell: is it a tombstone?
///
/// Kept as a struct rather than filtering on the fly because the answer is needed
/// THREE times — to drop the element from the reconciled container, to refuse a
/// golden that would need arbitration between a tombstone and a live cell at the
/// same path, and to decide which cells a complex-column deletion marker has to be
/// checked against (a tombstone contributes nothing either way, so the marker
/// cannot change the expectation for it).
struct MultiCell<'a> {
    cell: &'a Value,
    /// `deletion_info` and no `value`, i.e. `Cell.isTombstone()` in
    /// `cassandra-5.0.8 JsonTransformer.serializeCell`.
    deleted: bool,
}

fn golden_row(
    row: &Value,
    keys: &[Value],
    pk: &[&str],
    ck: &[&str],
    multicell: &[(&str, Multicell)],
    at: &dyn Fn() -> String,
) -> Result<Row, String> {
    let kind = row.get("type").and_then(Value::as_str).unwrap_or("<none>");
    if kind != "row" {
        return Err(format!(
            "{}: unsupported dump element `{kind}` — a range tombstone or static \
             block is a read-time-reconciliation shape, not an egress-formatting one",
            at()
        ));
    }
    if row.get("deletion_info").is_some() {
        return Err(format!(
            "{}: row deletion marker — the physical dump keeps a row the CLI drops",
            at()
        ));
    }
    let liveness = row.get("liveness_info");
    if let Some(li) = liveness {
        for key in ["ttl", "expires_at", "expired"] {
            if li.get(key).is_some() {
                return Err(format!(
                    "{}: row liveness carries `{key}` — TTL expiry is reconciliation, \
                     not formatting",
                    at()
                ));
            }
        }
    }
    let row_tstamp = liveness
        .and_then(|li| li.get("tstamp"))
        .and_then(Value::as_str);
    // Parsed ONCE, and a present-but-unparseable stamp is an ERROR: folding it into
    // `None` would report "no row liveness" for a row that has one, and the
    // shadowing check below would then refuse for the wrong reason.
    let row_us = match row_tstamp {
        Some(text) => Some(
            parse_iso_micros(text)
                .ok_or_else(|| format!("{}: unparseable row liveness tstamp `{text}`", at()))?,
        ),
        None => None,
    };

    let clustering = array_field(row, "clustering", at)?;
    if clustering.len() != ck.len() {
        return Err(format!(
            "{}: golden clustering arity {} but {} clustering column(s) declared ({ck:?})",
            at(),
            clustering.len(),
            ck.len()
        ));
    }

    let mut out: Row = BTreeMap::new();
    for (name, value) in pk.iter().zip(keys.iter()) {
        out.insert((*name).to_string(), value.clone());
    }
    for (name, value) in ck.iter().zip(clustering.iter()) {
        out.insert((*name).to_string(), value.clone());
    }

    let kind_of = |name: &str| -> Option<Multicell> {
        multicell.iter().find(|(n, _)| *n == name).map(|(_, k)| *k)
    };
    let mut multi: BTreeMap<String, Vec<MultiCell<'_>>> = BTreeMap::new();
    // Complex-column deletion markers: `(column, marked_deleted text, micros)`.
    let mut complex_deletions: Vec<(String, String, i64)> = Vec::new();
    for cell in array_field(row, "cells", at)? {
        let name = cell
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{}: cell with no `name`", at()))?;
        for key in ["ttl", "expires_at", "expired"] {
            if cell.get(key).is_some() {
                return Err(format!("{}: cell `{name}` carries `{key}` (TTL)", at()));
            }
        }
        if cell.get("path").is_some() {
            if kind_of(name).is_none() {
                return Err(format!(
                    "{}: cell `{name}` is multi-cell (has a `path`) but no collection \
                     kind is declared for it",
                    at()
                ));
            }
            // The deletion is examined HERE, before the cell is taken as a live
            // member. `serializeCell` (cassandra-5.0.8 `JsonTransformer`) writes
            // `deletion_info` INSTEAD of `value` whenever `cell.isTombstone()`,
            // for a multicell cell exactly as for a scalar one — the committed
            // corpus has the shape verbatim:
            // `{"name":"tags","path":["remove_me"],"deletion_info":{"local_delete_time":…}}`
            // next to a live `{"name":"tags","path":["keep_me"],"value":""}`.
            // Collecting first and looking at the deletion later reconstructed a
            // DELETED set element as PRESENT, and reported a deleted list/map
            // element as "no value" — both wrong expectations (review finding L2).
            let deleted = match (cell.get("deletion_info"), cell.get("value")) {
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(_), Some(_)) => {
                    return Err(format!(
                        "{}: multicell cell `{name}` carries both value and deletion",
                        at()
                    ))
                }
                // `serializeCell` writes exactly one of the two for every cell —
                // a multicell SET's live cell carries `"value": ""`, it is not
                // value-less — so a cell with neither is not a dump this reader
                // understands.
                (None, None) => {
                    return Err(format!(
                        "{}: multicell cell `{name}` carries neither a value nor a deletion",
                        at()
                    ))
                }
            };
            multi
                .entry(name.to_string())
                .or_default()
                .push(MultiCell { cell, deleted });
            continue;
        }
        if let Some(del) = cell.get("deletion_info") {
            if cell.get("value").is_some() {
                return Err(format!(
                    "{}: cell `{name}` carries both value and deletion",
                    at()
                ));
            }
            // A complex-column tombstone: Cassandra writes one ahead of a
            // full-collection INSERT (`UnfilteredSerializer` writes the complex
            // deletion before the collection's cells). It shadows every cell of
            // ITS OWN column that is not strictly newer than it, so it is
            // ignorable only when every LIVE cell of that column is — checked
            // after the loop, once they are all known (see the shadowing check
            // below).
            if kind_of(name).is_none() {
                // `serializeDeletion` (the complex-column path) writes
                // `marked_deleted` alongside `local_delete_time`; `serializeCell`'s
                // tombstone branch writes `local_delete_time` ALONE
                // (cassandra-5.0.8 `JsonTransformer`). So a `marked_deleted` here
                // is the dump saying this column is COMPLEX while the case declares
                // it simple — reconciling it as a scalar cell tombstone would set
                // the whole column to null on a guess.
                if del.get("marked_deleted").is_some() {
                    return Err(format!(
                        "{}: `{name}` carries a complex deletion (`marked_deleted`), so \
                         the dump says the column is a non-frozen collection, but the \
                         case declares no collection kind for it",
                        at()
                    ));
                }
                // A CELL tombstone on a scalar column: the column reconciles to
                // NULL — exactly the "tombstone → null" egress property this lane
                // exists to pin. `sstabledump` keeps the marker; a `SELECT` sees a
                // null. There can be no competing value cell for the same name in
                // the same row (that collision is an error), so no timestamp
                // arbitration is needed.
                if out.insert(name.to_string(), Value::Null).is_some() {
                    return Err(format!(
                        "{}: cell tombstone for `{name}` collides with another cell or \
                         a declared key column",
                        at()
                    ));
                }
                continue;
            }
            let marked = del
                .get("marked_deleted")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{}: `{name}` deletion with no marked_deleted", at()))?;
            let marked_us = parse_iso_micros(marked)
                .ok_or_else(|| format!("{}: unparseable marked_deleted `{marked}`", at()))?;
            // Collected, not decided: whether this marker shadows anything is a
            // question about the column's CELLS, and they are only all known once
            // the cell loop has finished. Deciding it here against the row liveness
            // alone was review finding M2.
            complex_deletions.push((name.to_string(), marked.to_string(), marked_us));
            continue;
        }
        let value = cell
            .get("value")
            .ok_or_else(|| format!("{}: cell `{name}` has no `value`", at()))?;
        if out.insert(name.to_string(), value.clone()).is_some() {
            return Err(format!(
                "{}: cell `{name}` collides with a declared key column",
                at()
            ));
        }
    }

    // A complex-column tombstone is IGNORABLE only when it shadows nothing this
    // reader is about to reconstruct — asserted per CELL, never assumed.
    //
    // Cassandra's shadowing rule is one line and exact: `DeletionTime.deletes(cell)`
    // is `cell.timestamp() <= markedForDeleteAt()` (cassandra-5.0.8
    // `DeletionTime.java`). It is used here ONLY to REFUSE. Applying it to DROP the
    // shadowed cell would be a second implementation of Cassandra's read-time
    // reconciliation living in test code, which the physical-dump oracle cannot
    // express and cannot check (CLAUDE.md, "Two parity oracles"); refusing states
    // the same fact without inventing an expectation.
    //
    // Comparing the marker with the ROW LIVENESS alone was wrong in both directions
    // (review finding M2): a cell carrying its own `tstamp` older than the marker
    // was shadowed even when the row liveness was newer, and a row with a marker but
    // no liveness at all was refused even when every cell of the column states its
    // own timestamp.
    //
    // Only LIVE cells are checked. A cell that is itself a tombstone contributes
    // nothing to the reconstructed collection whether or not the marker also covers
    // it, so the marker cannot change the expectation for it.
    for (name, marked, marked_us) in &complex_deletions {
        for cell in multi.get(name).map(Vec::as_slice).unwrap_or_default() {
            if cell.deleted {
                continue;
            }
            // The cell's timestamp: its own `tstamp` when the dump wrote one, else
            // the row's liveness timestamp. `serializeCell` writes `tstamp` when
            // `liveInfo.isEmpty() || cell.timestamp() != liveInfo.timestamp()`, so a
            // cell WITHOUT one has exactly the row's liveness timestamp — and a row
            // with no liveness stamp cannot produce such a cell at all.
            let cell_us = match cell.cell.get("tstamp") {
                Some(Value::String(text)) => parse_iso_micros(text).ok_or_else(|| {
                    format!("{}: unparseable cell tstamp `{text}` on `{name}`", at())
                })?,
                Some(other) => {
                    return Err(format!(
                        "{}: cell `tstamp` on `{name}` is {other}, not a string — \
                         sstabledump writes every timestamp as an ISO-8601 string",
                        at()
                    ))
                }
                None => row_us.ok_or_else(|| {
                    format!(
                        "{}: a cell of `{name}` carries no `tstamp` and the row carries no \
                         liveness tstamp — serializeCell omits a cell's `tstamp` only when \
                         the row liveness is non-empty and equal to it, so this golden is \
                         not one this reader can time against the complex deletion",
                        at()
                    )
                })?,
            };
            if cell_us <= *marked_us {
                return Err(format!(
                    "{}: the complex deletion on `{name}` at {marked} shadows a live cell \
                     of that column (cell timestamp {cell_us}µs <= marker), so the CLI's \
                     reconciled result set drops a value this dump still carries — \
                     deciding it is timestamp arbitration, which this reader does not do",
                    at()
                ));
            }
        }
    }

    for (name, all_cells) in multi {
        let kind = kind_of(&name).ok_or_else(|| format!("{}: `{name}` kind vanished", at()))?;
        // A tombstoned element sharing its path with another cell of the same
        // column would need TIMESTAMP ARBITRATION, which is reconciliation and not
        // something a single SSTable's dump ever asks for: within one row of one
        // SSTable a complex column's cells are keyed by `CellPath`, so a path
        // appears at most once. Refused rather than resolved, so the day such a
        // golden appears the lane says so instead of picking one silently.
        for (i, cell) in all_cells.iter().enumerate() {
            if !cell.deleted {
                continue;
            }
            let path = cell.cell.get("path");
            if let Some(twin) = all_cells
                .iter()
                .enumerate()
                .find(|(j, other)| *j != i && other.cell.get("path") == path)
            {
                return Err(format!(
                    "{}: `{name}` carries a tombstoned cell and another cell for the same \
                     path {} — deciding between them is timestamp arbitration, which this \
                     reader does not do",
                    at(),
                    twin.1
                        .cell
                        .get("path")
                        .map_or("<none>".to_string(), |p| p.to_string())
                ));
            }
        }
        // A DELETED element is not part of the reconciled collection: the
        // remaining live cells are.
        let cells: Vec<&Value> = all_cells
            .iter()
            .filter(|c| !c.deleted)
            .map(|c| c.cell)
            .collect();
        if cells.is_empty() {
            // Every cell of this column is a tombstone, so the column has no live
            // cell at all and reconciles to NULL — the same state, and therefore
            // the same expectation, as the zero-cell shape the golden already
            // spells by omitting the column (`test-data/schemas/cql-type-parity.cql`
            // records that an emptied multicell collection reads back as null).
            if out.insert(name.clone(), Value::Null).is_some() {
                return Err(format!(
                    "{}: fully deleted collection `{name}` collides with a declared key column",
                    at()
                ));
            }
            continue;
        }
        let value = match kind {
            // `sstabledump` puts a set element in the cell path.
            Multicell::Set => Value::Array(
                cells
                    .iter()
                    .map(|c| path_head(c, at))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            // A list's path is an internal timeuuid; the element is the value.
            Multicell::List => Value::Array(
                cells
                    .iter()
                    .map(|c| {
                        c.get("value")
                            .cloned()
                            .ok_or_else(|| format!("{}: list cell `{name}` has no value", at()))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            // A map's path is the key, the value is the value.
            Multicell::Map => {
                let mut obj = Map::new();
                for c in &cells {
                    // `sstabledump` writes a cell PATH with
                    // `writeString(ct.nameComparator().getString(...))` (see
                    // `Kinding`), so a multicell map's key is ALWAYS a JSON string
                    // in the golden. A non-string here means the golden is not the
                    // document this reader understands; projecting it with
                    // `Value::to_string()` instead invented a key — `true`, `1`,
                    // `null` — that a genuine `text` key of that spelling would
                    // then compare EQUAL to.
                    let key = match path_head(c, at)? {
                        Value::String(s) => s,
                        other => {
                            return Err(format!(
                                "{}: map cell `{name}` has the non-string path head {other} \
                                 — sstabledump writes every cell path as a JSON string, so \
                                 this golden is not one this reader can key a map by",
                                at()
                            ))
                        }
                    };
                    let value = c
                        .get("value")
                        .cloned()
                        .ok_or_else(|| format!("{}: map cell `{name}` has no value", at()))?;
                    // Two cells for the same key cannot both be compared: inserting
                    // the later over the earlier silently DISCARDS a golden cell,
                    // shrinking the oracle. Whatever produced such a golden, one the
                    // reader must drop part of is not a usable oracle, so it is
                    // refused rather than collapsed (issue #1491 finding J2's class,
                    // golden side).
                    if obj.insert(key.clone(), value).is_some() {
                        return Err(format!(
                            "{}: map cell `{name}` carries two cells for the key `{key}` — a \
                             golden the reader would have to discard part of is not a usable \
                             oracle",
                            at()
                        ));
                    }
                }
                Value::Object(obj)
            }
        };
        if out.insert(name.clone(), value).is_some() {
            return Err(format!(
                "{}: collection `{name}` collides with a declared key column",
                at()
            ));
        }
    }
    Ok(out)
}

fn path_head(cell: &Value, at: &dyn Fn() -> String) -> Result<Value, String> {
    let path = cell
        .get("path")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{}: cell path is not an array", at()))?;
    if path.len() != 1 {
        return Err(format!(
            "{}: cell path arity {} — nested collection paths are out of scope",
            at(),
            path.len()
        ));
    }
    Ok(path[0].clone())
}

/// Microseconds since the Unix epoch for an `sstabledump` ISO-8601 UTC stamp
/// (`YYYY-MM-DDTHH:MM:SS[.frac]Z`), or `None` when unparseable.
///
/// Needed because the complex-deletion guard is an ORDERING question and the
/// goldens mix fraction widths (`.001Z` next to `.378920Z`), which makes a plain
/// string comparison wrong.
pub fn parse_iso_micros(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once(['T', ' '])?;
    let mut d = date.split('-');
    let year: i64 = d.next()?.parse().ok()?;
    let month: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    if d.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (time, ""),
    };
    let mut t = hms.split(':');
    let hour: i64 = t.next()?.parse().ok()?;
    let minute: i64 = t.next()?.parse().ok()?;
    let second: i64 = t.next()?.parse().ok()?;
    if t.next().is_some() {
        return None;
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
        return None;
    }
    if frac.len() > 9 || !frac.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let micros_frac: i64 = if frac.is_empty() {
        0
    } else {
        let padded = format!("{frac:0<6}");
        padded[..6].parse().ok()?
    };
    // days_from_civil (Howard Hinnant's algorithm), exact for the proleptic
    // Gregorian calendar.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days
        .checked_mul(86_400)?
        .checked_add(hour * 3600 + minute * 60 + second)?;
    secs.checked_mul(1_000_000)?.checked_add(micros_frac)
}

/// Unit coverage for the golden READER (split out under the campsite rule).
#[cfg(test)]
#[path = "golden_reader_tests.rs"]
mod golden_reader_tests;

#[cfg(test)]
#[path = "golden_value_canon_tests.rs"]
mod tests;
