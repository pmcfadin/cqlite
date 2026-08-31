//! The HASHABLE PROJECTION: `cqlite_core::Value` → a Python object usable as a
//! `dict` key or a `frozenset` element.
//!
//! This is the second of the two jobs the value layer does, split out from
//! [`crate::value`] (which owns the ORDINARY host conversion, `Value` → the
//! Python object a caller sees in a result row). The seam is a responsibility
//! boundary, not a line-count dodge: the ordinary conversion answers "what does
//! this value LOOK like in Python", and this module answers "what can stand in
//! for it where Python demands hashability" — and the two answers deliberately
//! DIFFER for a `list` (unconditionally) and for a UDT-bearing `set` (issue
//! #804's CLI-parity `list`).
//!
//! # ROUTING: what reaches this module, and via which ARM — THE ONE STATEMENT
//!
//! This section is the SINGLE authoritative statement of which columns of the
//! #3500 fixture reach [`value_to_hashable_key`] and on which arm they land.
//! `test-data/schemas/nested-udt-keys.cql`,
//! `test-data/scripts/generate-nested-udt-keys.sh` and
//! `bindings/python/tests/test_nested_udt_hashable.py` POINT here and assert no
//! routing of their own. That is not tidiness: the fact was restated in every
//! file that mentioned it and DRIFTED in all but one — wrong in four places in
//! the test file (#3500 review round 10) and then in both fixture headers
//! (round 11, roborev job 267), each time as the same over-broad "only the
//! frozen maps reach it at all".
//! It lives here because this is the file whose code decides the routing,
//! so the person who invalidates the claim is the person editing it. Same
//! treatment, for the same reason, as the `Value::Json` reachability claim
//! stated once at its own arm.
//!
//! The DIRECT callers, both in [`crate::value`]:
//!
//! * `map_to_py` projects EVERY key through [`value_to_hashable_key`]. There is
//!   no gate — a Python `dict` key must be hashable whatever it holds.
//! * `set_to_py` projects elements through it only on its `frozenset` branch,
//!   i.e. when [`contains_udt`] is false for every element. A UDT-bearing set
//!   takes the `list` branch (#804) and converts each element with
//!   `value_to_py` instead.
//!
//! The REACH is WIDER than those two callers, because `value_to_py` on a nested
//! value re-enters `map_to_py`. Per column of
//! `test_nested_udt_keys.nested_udt_keys`, which is this module's whole corpus:
//!
//! * `f_map_tuple_udt` — REACHES, via its frozen-map KEYS, which
//!   `parse_frozen_map_value` decodes structurally. Arm: `Tuple`, recursing into
//!   `Udt` (including its `None => py.None()` field branch).
//! * `f_map_set_udt` — REACHES, same route. Arm: `Set`, recursing into `Udt`
//!   (including that same `None` branch).
//! * `f_map_tuple_list_udt` — REACHES, same route. Arm: `Tuple`, recursing
//!   through the SAME `List | Tuple` arm for the key's nested `frozen<list>` and
//!   then into `Udt` (including that same `None` branch). It is the ONLY column
//!   whose key the ORDINARY projection cannot produce at all — `value_to_py`
//!   answers with a `tuple` holding an unhashable Python `list` — which is what
//!   makes the arm's RECURSION observable rather than merely compiled; the
//!   argument is stated once, in that column's test class.
//! * `s_map_udt_key` — REACHES, via the INNER frozen map's keys:
//!   `set_to_py`'s `list` branch → `value_to_py` → `map_to_py`. Arm: `Udt`
//!   (through `Frozen`), the inner key type being `frozen<key_part>`.
//! * `s_map_udt_val` — REACHES, same route; its inner map keys are `int`. Arm:
//!   SCALAR (`Integer`).
//! * `m_tuple_udt` — REACHES, via `map_to_py` over a MULTICELL map's decoded
//!   keys. Arm: `Tuple`, then `Udt` through `Frozen` — the SAME arms and the
//!   same output as its frozen sibling `f_map_tuple_udt`. Until #3612 its keys
//!   arrived as opaque `Value::Blob` from a then scalar-only
//!   `parse_cell_path_key`, so this entry read `Arm: SCALAR (Blob)`; #3612 made
//!   that site delegate to the structural decoder, and
//!   `TestTupleBorneUdtAsMapKey` flipped from a documented gap to the positive
//!   case in the same change.
//! * `s_tuple_udt`, `s_set_udt`, `s_list_udt`, `f_set_tuple_udt` — do NOT reach
//!   it. `set_to_py`'s `list` branch converts each element with `value_to_py`,
//!   and nothing below those elements is a map.
//!
//! Those first three are also the ONLY values in this repository that reach
//! `build_udt`'s `None => py.None()` field branch with
//! `convert = value_to_hashable_key`, i.e. through this module's `Udt` arm — two
//! through `Tuple`, one through `Set`. The BRANCH itself is not this arm's
//! property: `build_udt` is shared with `value::udt_to_py` since #3504, so the
//! set columns' null fields execute the same branch with
//! `convert = value_to_py`. What is exclusive is the ARM's route into it, which
//! is the whole reason a coverage claim here has to name the arm.
//!
//! Two conclusions, both of which have been got wrong in the other direction:
//!
//! * "the frozen maps are the only columns that reach this function AT ALL" is
//!   FALSE — SIX of the ten columns do.
//! * "the frozen maps are the only columns that reach the NEW `Tuple`/`Set`
//!   arms" is TRUE, and is why those THREE columns exist: `s_map_udt_key`'s
//!   inner map key is a UDT (`Udt` arm), `s_map_udt_val`'s is an `int` (scalar
//!   arm) and `m_tuple_udt`'s is a `Blob` (scalar arm), so no other route lands
//!   on `Tuple` or `Set`.
//!
//! Hence the rule the drift keeps proving: scope a coverage claim to the ARM it
//! reaches, NEVER to the function.
//!
//! # Totality, and why it is the compiler's job
//!
//! [`value_to_hashable_key`], [`json_to_hashable_key`] and [`contains_udt`] are
//! each exhaustive with NO `_ =>` arm, pinned by
//! `#[deny(clippy::wildcard_enum_match_arm)]` (issue #3500). The two `Value`
//! matches are a MATCHED PAIR and live in this module together on purpose:
//! `contains_udt` decides WHICH path `set_to_py` takes and
//! `value_to_hashable_key` executes it, so a wildcard in either desynchronises
//! them. Keeping them side by side is what makes that argument checkable.

use pyo3::prelude::*;
use pyo3::types::{PyFrozenSet, PyTuple};

use crate::value::{build_udt, json_to_py, value_to_py};
use cqlite_core::Value;

/// Convert a Value for use in a Python HASHABLE position — a `dict` key or a
/// `frozenset` element.
///
/// Python `dict` keys and `frozenset` elements must be hashable, but the
/// ordinary projection of several CQL types is not: a `List` becomes a `list`
/// UNCONDITIONALLY (`list_to_py`), a `Set` becomes a `list` when it contains a
/// UDT anywhere inside (`set_to_py`), and a UDT's FIELD VALUES are projected by
/// [`value_to_py`] (`udt_to_py`), so a UDT with a collection field is
/// unhashable even though the [`crate::value::Udt`] class itself implements `__hash__`
/// (issue #3504).
/// This function is the TOTAL hashable projection over `cqlite_core::Value` —
/// total in the sense of SHAPE and variant coverage (see *Totality is enforced
/// by the COMPILER* below), NOT in the sense of being infallible:
///
/// - `List`, `Tuple` → `tuple` (elements recursively projected)
/// - `Set` → `frozenset` (elements recursively projected)
/// - `Map` → `tuple` of `(key, value)` tuples (both sides recursively projected)
/// - `Udt` → a [`crate::value::Udt`] instance carrying the type identity OUT OF BAND, with
///   every field value recursively projected (issue #3504)
/// - `Frozen` → unwrap and recurse
/// - `Json` → `tuple` (array) / `frozenset` of pairs (object), recursively; its
///   SCALAR arm delegates to [`json_to_py`] and is since #3505 the ONE arm that
///   can return `Err` — see [`json_to_hashable_key`]
///   (reachability: the one statement of it is at the `Value::Json` arm)
/// - every other variant → its ordinary [`value_to_py`] projection, which is
///   already hashable
///
/// # Totality is enforced by the COMPILER (issue #3500)
///
/// There is deliberately **no `_ =>` arm**: every one of `Value`'s variants is
/// named. A wildcard is precisely what made this function non-total — a
/// composite reached through a `Tuple` or through a nested `Set` fell through to
/// [`value_to_py`], whose projections are not hashability-projected, and the
/// whole `SELECT` raised `TypeError: unhashable type: 'list'` (or `'dict'`) on
/// legal CQL. The two routes fail for different reasons, and issue #3504
/// narrowed one of them without closing it:
///
/// - the `Set` route fails UNCONDITIONALLY when a UDT is anywhere inside, because
///   `value::set_to_py` answers with a `list` there on purpose (#804);
/// - the `Tuple` route reaches `value::tuple_to_py`, which projects its elements with
///   [`value_to_py`]. Since #3504 a UDT element is a hashable [`crate::value::Udt`], so a
///   `tuple<frozen<udt>>` of SCALAR-field UDTs happens to survive; a `list`/`map`
///   element, or a UDT carrying a collection FIELD, still does not.
///
/// Naming every variant turns a future `Value` variant into a COMPILE ERROR here
/// instead of a runtime `TypeError` on somebody's data, which is strictly
/// stronger than detecting it at run time — and it removes the case analysis
/// above from the set of things a reader has to redo.
///
/// That property is PINNED by `#[deny(clippy::wildcard_enum_match_arm)]` — on
/// this function, on [`json_to_hashable_key`] and on [`contains_udt`] — rather
/// than by a text guard: clippy runs `-D warnings` in the gate, so a
/// reintroduced `_ =>` is a hard error. A grep-style guard was rejected because
/// it matches the PROSE above (which necessarily contains `_ =>`). All three
/// attributes were RED-verified by planting a wildcard and watching clippy
/// error; presence of a lint attribute is not enforcement.
///
/// Recursion goes through THIS function, never through [`value_to_py`] or
/// `value::set_to_py`: `set_to_py`'s UDT branch returns an unhashable `list` **on
/// purpose** (issue #804) because that is the right answer for a top-level
/// column, and the wrong one inside a hashable position.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn value_to_hashable_key(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        // Both project to a Python `tuple`: a list needs one for hashability, a
        // CQL tuple maps to one anyway. The element projection is what matters —
        // it must recurse HERE so a nested UDT's FIELD VALUES are projected
        // hashably too, which `value_to_py`'s route through `udt_to_py` does not
        // do.
        Value::List(items) | Value::Tuple(items) => {
            let converted: Vec<PyObject> = items
                .iter()
                .map(|v| value_to_hashable_key(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyTuple::new(py, converted)?.into_any().unbind())
        }
        Value::Set(items) => {
            // Recursion stays HERE and never re-enters `set_to_py`, whose UDT
            // branch deliberately returns an unhashable `list` (#804): the right
            // answer for a top-level column, the wrong one inside a hashable
            // position.
            //
            // What this arm buys AS THE CODE NOW STANDS is (a) the MAP-KEY path
            // — a `map<frozen<set<…>>, v>` key, projected by `map_to_py` — and
            // (b) compiler-enforced totality. It is NOT what fixes
            // `set<frozen<set<frozen<udt>>>>`: post-fix `contains_udt` sees the
            // nested UDT, so `set_to_py` routes that column to its `list`
            // branch and this arm is never reached for it. (PRE-fix that column
            // did reach a frozenset holding an unhashable element — that was the
            // #3500 failure, and it is history, not current behaviour.) Which
            // FIXTURE COLUMN takes which route is not restated here: it is in
            // this module's ROUTING section, and this comment must stay
            // consistent with it.
            let converted: Vec<PyObject> = items
                .iter()
                .map(|v| value_to_hashable_key(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyFrozenSet::new(py, &converted)?.into_any().unbind())
        }
        Value::Map(pairs) => {
            // Maps as keys are rare but possible - convert to tuple of tuples
            let converted: Vec<PyObject> = pairs
                .iter()
                .map(|(k, v)| {
                    let key = value_to_hashable_key(py, k)?;
                    let val = value_to_hashable_key(py, v)?;
                    Ok(PyTuple::new(py, [key, val])?.into_any().unbind())
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyTuple::new(py, converted)?.into_any().unbind())
        }
        Value::Frozen(inner) => {
            // Unwrap Frozen and recurse so that FROZEN<UDT> and FROZEN<collection>
            // are handled by the appropriate arm rather than falling through to
            // value_to_py, whose conversions are not projected for hashability.
            //
            // `Frozen` is present at some nesting levels and ABSENT at others —
            // a multicell column decodes as
            // `Set([Frozen(Tuple([Frozen(Udt), Integer]))])` while the same
            // nesting under a frozen outer collection decodes as
            // `Frozen(Set([Tuple([Udt, Integer])]))`, with no inner wrappers —
            // so every arm here must work with and without it (#3500).
            value_to_hashable_key(py, inner)
        }
        Value::Udt(udt) => {
            // UDT in a hashable position (issue #3504): project to a `Udt`
            // instance, so the type name and keyspace ride OUTSIDE the field
            // namespace.
            //
            // The projection this replaced pushed a pair for `_type`, then one
            // for `_keyspace`, then one per field, into a single `frozenset` — so
            // a field named `_type` produced TWO `_type` pairs with different
            // values and nothing deduped them. The pair set now holds exactly one
            // entry per declared field and no metadata entry at all, while
            // `Udt.__eq__`/`__hash__` keep the identity in the comparison, so two
            // UDTs of different declared types with identical fields still hash
            // differently and compare unequal.
            //
            // `convert` is THIS function, so every field value is recursively
            // projected and a `Udt` reaching a `dict` key / set member position is
            // hashable — which `udt_to_py`'s `value_to_py` conversion does not
            // give you for a UDT with a collection field. Sharing `build_udt`
            // between the two keeps the field namespace identical by
            // construction.
            Ok(build_udt(py, udt, value_to_hashable_key)?.into_any())
        }
        // DEFENSIVE — and not for the reason either earlier version of this
        // comment gave (both were wrong, in opposite directions), so every clause
        // here was re-derived at source and the two mechanical ones were run.
        //
        // The DECODERS exist: `reader/parsing/custom_scalar.rs:55` (the `"json"`
        // arm of `decode_custom_scalar`) and
        // `reader/parsing/comparator_value_parsing.rs:234` both return
        // `Ok(Value::Json(..))`. What does not exist is an ingestion path that
        // can deliver a `json`-typed column to either:
        //
        // 1. `CqlType::parse("json")` yields `CqlType::Custom("json")`
        //    (`schema/cql_type_parser.rs:208`, the fallback arm; the `udt:`
        //    branch at :175-184 is skipped, needing a NOT-all-lowercase name).
        //    Verified by running it.
        // 2. That bare `Custom` is then REJECTED by schema validation:
        //    `check_type_udt_references` (`schema/mod.rs:655-660`) strips no
        //    `udt:` prefix from a bare name, `is_udt_identifier("json")` is true
        //    (`:304-309`, alphanumeric), and `ensure_udt_exists` (`:680-702`)
        //    errors unless a UDT literally named `json` exists. This binding
        //    reaches that check on its ONLY schema path (`database/open.rs:137`
        //    -> `ingestion::ingest`, `validate_udt_dependencies: true` +
        //    `graceful_degradation: false` at `ingestion.rs:204-207`, fail-fast
        //    at `:231-242`). Verified by running it: `Err(Schema("Column 'doc'
        //    references undefined UDT 'json' in keyspace 'ks'"))`.
        //    And if a UDT named `json` DOES exist, the column is not a JSON
        //    column either — registry-backed resolution matches that UDT and
        //    yields `ComparatorType::Udt` (`types/comparator.rs:250-277`).
        // 3. A HEADER-derived schema cannot produce it: nothing constructs
        //    `ComparatorType::Json`. A census finds it only in `match` arms, plus
        //    `schema/parser.rs:531` mapping it OUTWARD to `Custom("json")`; the
        //    sole inbound `Custom` mapping is `types/comparator.rs:136`
        //    (`CqlType::Custom(n) -> ComparatorType::Custom(n)`). The variant is
        //    constructed only in a unit test
        //    (`value_parsing_schema_type_tests.rs:330`).
        //
        // So: not "there is no decoder" (there is), and not "a `.cql` `json`
        // column reads real cells into this arm" (that schema is refused at
        // open). The arm is unreachable from any supported ingestion path and is
        // kept because this `match` is exhaustive by design (see above), so a
        // future inbound path must revisit it. Adjacent oddity, not a bug:
        // cqlite-core carries decode support for a `json` custom scalar that no
        // schema can reach.
        //
        // THIS BLOCK IS THE ONE SITE THAT STATES `Value::Json` REACHABILITY, and
        // the blocker is STRUCTURAL (schema validation refuses the type), NOT
        // fixture absence — writing a fixture cannot reach this arm. It was
        // restated in five places and drifted in four, so every other mention
        // (the doc list above, [`json_to_hashable_key`], `M4_spec.md` rows b-5 /
        // "JSON object", `test_cli_parity.py`'s
        // `test_json_object_cell_normalizes_as_a_cql_map`) POINTS here and
        // asserts nothing. If this changes, it changes here, once.
        //
        // IF that path became reachable, one limitation would apply: the
        // projection below inherits `json_to_py`'s number handling, so JSON `1`
        // becomes `int`, `1.0` `float` and `true` `bool`, and since Python holds
        // `1 == 1.0 == True` with EQUAL hashes, `{"a": 1}` and `{"a": 1.0}` would
        // collapse onto one element in a set-element or map-key position. That is
        // a LATENT instance of the collapse class tracked by **#3615**, not a
        // live data-loss bug today, precisely because nothing delivers a
        // `Value::Json` here.
        //
        // #3615's other members are independent of JSON and ARE live: `-0.0` vs
        // `+0.0` in a `set<double>` (Cassandra orders by
        // `Double.compare`/`Float.compare`, where `-0.0 < +0.0`, so both zeros in
        // one set is ordinary legal data, while Python has
        // `hash(-0.0) == hash(0.0)`); and `Null` vs `Tombstone` both projecting
        // to `None` (listed for completeness — probably desired). Both PRE-DATE
        // #3500, which neither introduced nor widened either; fixing them needs a
        // type-preserving projection, a behaviour change out of scope here.
        //
        // A THIRD member — a UDT field literally named `_type`/`_keyspace`
        // shadowing the metadata pair this arm used to inject — is GONE, fixed by
        // issue #3504: the identity now rides on the [`crate::value::Udt`] instance and the
        // field namespace holds declared fields only, so there is no metadata
        // pair left to shadow.
        Value::Json(json) => json_to_hashable_key(py, json),
        // Every remaining variant's ordinary projection is ALREADY hashable, so
        // it delegates to `value_to_py`. Named exhaustively — never `_ =>` — so
        // a new `Value` variant fails to COMPILE here (see the note above).
        //
        // Each is hashable for a checked reason, not by assumption:
        // `Text`/`Blob`/`Varint`/the integer and float variants project to
        // immutable Python built-ins; `Timestamp`/`Date` to `datetime`/`date`;
        // `Uuid` to `uuid.UUID`; `Decimal` to `decimal.Decimal`; `Inet` to
        // `ipaddress.IPv4Address`/`IPv6Address`; `Duration` to the
        // `#[pyclass(frozen, eq, hash)]` [`crate::value::Duration`] class; and `Null` /
        // `Tombstone` to `None`.
        Value::Null
        | Value::Boolean(_)
        | Value::TinyInt(_)
        | Value::SmallInt(_)
        | Value::Integer(_)
        | Value::BigInt(_)
        | Value::Counter(_)
        | Value::Float32(_)
        | Value::Float(_)
        | Value::Text(_)
        | Value::Blob(_)
        | Value::Timestamp(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Uuid(_)
        | Value::Varint(_)
        | Value::Decimal { .. }
        | Value::Duration { .. }
        | Value::Inet(_)
        | Value::Tombstone(_) => value_to_py(py, value),
    }
}

/// Hashable projection of a JSON value (companion to [`json_to_py`]).
///
/// Whether anything can deliver a `Value::Json` here is recorded in exactly ONE
/// place — the `Value::Json` arm of [`value_to_hashable_key`] — and this doc
/// asserts nothing about it. Arrays become `tuple`s and objects
/// become `frozenset`s of `(key, value)` pairs so that a JSON value in a
/// hashable position can never be the unhashable `list`/`dict` that
/// [`json_to_py`] would build.
///
/// Scalars delegate to [`json_to_py`], so its NUMBER projection is inherited
/// WHOLE — since **#3505** that means both its precision and its one error arm.
/// Two consequences, pulling in opposite directions:
///
/// * A known collapse: JSON `1`/`1.0`/`true` become Python
///   `int`/`float`/`bool`, which compare equal with equal hashes, so `{"a": 1}`
///   and `{"a": 1.0}` are the same frozenset. That is one instance of the
///   projection-collapse class tracked by **#3615** (whose other members —
///   `-0.0`/`+0.0`, `_type` field shadowing, `Null`/`Tombstone` — have nothing
///   to do with JSON); recorded in full at the `Value::Json` arm. #3505 leaves
///   it UNCHANGED — `1` classifies `I64` → `int` and `1.0` classifies `F64` →
///   `float` exactly as before — and adds no new member, because a `Beyond`
///   number becomes an exact `int` or an `Err`, never a `str`, so it cannot
///   collide with a genuine `Value::Text`.
/// * An inherited REFUSAL, which is why the totality claimed above is about
///   SHAPE and not about infallibility: a JSON number no host type can
///   represent exactly yields `Err` rather than a wrong-but-hashable value.
///   That is the direction #3500's AC3 asked for ("prefer an explicit error
///   over silently producing an unhashable value"), and it is UNREACHABLE in
///   this build: without `arbitrary_precision`, `serde_json` rounds such a
///   literal to an `f64` in the PARSER, before any binding code runs — measured
///   and test-asserted in `cqlite-ffi-common/src/json_number.rs`.
#[deny(clippy::wildcard_enum_match_arm)]
fn json_to_hashable_key(py: Python<'_>, json: &serde_json::Value) -> PyResult<PyObject> {
    match json {
        serde_json::Value::Array(arr) => {
            let items: Vec<PyObject> = arr
                .iter()
                .map(|v| json_to_hashable_key(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyTuple::new(py, items)?.into_any().unbind())
        }
        serde_json::Value::Object(obj) => {
            let mut pairs: Vec<PyObject> = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                let key = k.as_str().into_pyobject(py)?.into_any().unbind();
                let val = json_to_hashable_key(py, v)?;
                pairs.push(PyTuple::new(py, [key, val])?.into_any().unbind());
            }
            // `serde_json::Map` iterates in a deterministic order (insertion or
            // sorted, per its feature set), but a `frozenset` hashes
            // order-independently either way — matching the `Udt` arm.
            Ok(PyFrozenSet::new(py, &pairs)?.into_any().unbind())
        }
        // Scalars already project to immutable Python built-ins.
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => json_to_py(py, json),
    }
}
/// Return `true` if `value` is or CONTAINS a UDT value, at any nesting depth.
///
/// Used by `value::set_to_py` to decide whether a `SET` is returned as a `frozenset`
/// (no UDT anywhere inside) or as a `list` (a UDT is in there, so #804's
/// CLI-parity shape applies). Since issue #3504 that is a SHAPE decision and not
/// a hashability one — see the note at the exhaustive arm below.
///
/// # Why this has to be a full traversal (issue #3500)
///
/// It used to look only through `Frozen`, so a UDT reached through a `Tuple`, a
/// nested `Set`, a `Map` or a `List` was invisible and `set_to_py` took the
/// `frozenset` branch for it. On legal CQL such as
/// `set<frozen<tuple<frozen<udt>, int>>>` that branch then projected the element
/// through the NON-total [`value_to_hashable_key`] of the time, which fell
/// through to [`value_to_py`], and Python raised
/// `TypeError: unhashable type: 'list'` (or `'dict'`). #804's rule is about a
/// UDT being ANYWHERE in the set, so the answer must be about the whole subtree,
/// not the outermost wrapper.
///
/// A `Map` is searched on BOTH sides — a UDT can sit in a map key as legally as
/// in a map value. `Frozen` is followed because it is present at some nesting
/// levels and absent at others (a multicell column yields
/// `Set([Frozen(Tuple([Frozen(Udt), …]))])` while a frozen outer collection
/// yields `Frozen(Set([Tuple([Udt, …])]))`).
///
/// Every remaining variant is a scalar and cannot contain a `Value::Udt`, so it
/// is `false` — including `Json`, whose payload is a `serde_json::Value` tree
/// with no CQL values in it. Those variants are named EXHAUSTIVELY: like
/// [`value_to_hashable_key`], this function has no `_ =>` arm, because the two
/// are a matched pair and a wildcard in either one defeats the other's
/// exhaustiveness (see the comment at that arm).
///
/// Reaching a `Udt` answers `true` immediately: a UDT nested inside another
/// UDT's field cannot change that answer, so there is no recursion into UDT
/// fields here — it would be code with no reachable effect.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn contains_udt(value: &Value) -> bool {
    match value {
        Value::Udt(_) => true,
        Value::Frozen(inner) => contains_udt(inner),
        Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
            items.iter().any(contains_udt)
        }
        Value::Map(pairs) => pairs
            .iter()
            .any(|(k, v)| contains_udt(k) || contains_udt(v)),
        // Scalars cannot contain anything, so they are `false`. Named
        // exhaustively — there is deliberately no `_ =>` arm, for the same
        // reason as in `value_to_hashable_key` and one more besides: these two
        // functions are a MATCHED PAIR. This one decides WHICH path
        // `set_to_py` takes; that one executes it. A wildcard here would
        // desynchronise them — adding a composite `Value` variant would be a
        // compile error there (correct) while this function silently answered
        // `false`, putting `set_to_py` back on the frozenset path.
        //
        // The consequence of that is a #804 SHAPE regression, NOT a
        // `TypeError`: `value_to_hashable_key` is now TOTAL, so a new composite
        // variant given a recursive arm there still projects HASHABLY, and the
        // frozenset would build. The column would simply come back as a
        // `frozenset` of projected elements where #804 requires the `list` of
        // `dict`s the CLI renders. So what `contains_udt` buys post-fix is
        // SHAPE POLICY, not hashability safety — and a new composite variant
        // must still fail to compile in BOTH halves, because the compiler is
        // the only thing keeping the two in step.
        //
        // `false` here also has to be an ANSWER, not a default: `_ => false`
        // said "no UDT" because it recognised no composite, which is not the
        // same as having looked.
        Value::Null
        | Value::Boolean(_)
        | Value::TinyInt(_)
        | Value::SmallInt(_)
        | Value::Integer(_)
        | Value::BigInt(_)
        | Value::Counter(_)
        | Value::Float32(_)
        | Value::Float(_)
        | Value::Text(_)
        | Value::Blob(_)
        | Value::Timestamp(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Uuid(_)
        | Value::Varint(_)
        | Value::Decimal { .. }
        | Value::Duration { .. }
        | Value::Json(_)
        | Value::Inet(_)
        | Value::Tombstone(_) => false,
    }
}
