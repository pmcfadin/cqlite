//! Value conversion from cqlite_core to Python types.
//!
//! This module handles conversion of all CQL data types to their Python equivalents.
//! The mapping follows M4 spec section 5.2 for type fidelity.

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFrozenSet, PyList, PyTuple};

use crate::error::to_py_err;
use cqlite_core::Value;

/// Convert a CQL Value to a Python object.
///
/// Handles all CQL types with proper Python type mapping:
/// - Primitives: Null→None, Boolean→bool, Integer→int, Float→float, Text→str
/// - Binary: Blob→bytes, Uuid→str (formatted), Inet→str (IP format)
/// - Temporal: Timestamp→datetime, Date→date, Time→int (nanoseconds since
///   midnight, lossless), Duration→[`Duration`] (exact months/days/nanos)
/// - Collections: List→list, Set→frozenset, Map→dict, Tuple→tuple
/// - Complex: Udt→dict, Varint→int, Decimal→decimal.Decimal
/// - Special: Tombstone→None, Frozen→unwrap
pub fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Boolean(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::TinyInt(i) => Ok((*i as i64).into_pyobject(py)?.into_any().unbind()),
        Value::SmallInt(i) => Ok((*i as i64).into_pyobject(py)?.into_any().unbind()),
        Value::Integer(i) => Ok((*i as i64).into_pyobject(py)?.into_any().unbind()),
        Value::BigInt(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        Value::Counter(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        Value::Float32(f) => Ok((*f as f64).into_pyobject(py)?.into_any().unbind()),
        Value::Float(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        // Text → Python `str` (NOT `bytes`). The variant is now `Bytes`-backed
        // (issue #1644); its bytes are UTF-8-validated at construction, so lossy
        // decode is exact. Converting through an owned `String` guarantees a
        // Python `str` (a `&Bytes`/`&[u8]` would surface as `bytes`) and copies at
        // the FFI boundary so the Python object owns its memory.
        Value::Text(s) => Ok(String::from_utf8_lossy(s)
            .into_owned()
            .into_pyobject(py)?
            .into_any()
            .unbind()),
        Value::Blob(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        Value::Timestamp(ts) => timestamp_to_datetime(py, *ts),
        Value::Date(d) => date_to_pydate(py, *d),
        Value::Time(t) => Ok(t.into_pyobject(py)?.into_any().unbind()),
        Value::Uuid(u) => uuid_to_py(py, u),
        Value::Varint(v) => varint_to_pyint(py, v),
        Value::Decimal { scale, unscaled } => decimal_to_pydecimal(py, *scale, unscaled),
        Value::Duration {
            months,
            days,
            nanos,
        } => Ok(Py::new(
            py,
            Duration {
                months: *months,
                days: *days,
                nanos: *nanos,
            },
        )?
        .into_any()),
        Value::Json(j) => json_to_py(py, j),
        Value::List(l) => list_to_py(py, l),
        Value::Set(s) => set_to_py(py, s),
        Value::Map(m) => map_to_py(py, m),
        Value::Tuple(t) => tuple_to_py(py, t),
        Value::Udt(u) => udt_to_py(py, u),
        Value::Frozen(v) => value_to_py(py, v),
        Value::Inet(b) => inet_to_py(py, b),
        Value::Tombstone(_) => Ok(py.None()), // Treat deleted data as None
    }
}

/// Convert a Value for use in a Python HASHABLE position — a `dict` key or a
/// `frozenset` element.
///
/// Python `dict` keys and `frozenset` elements must be hashable, but the
/// ordinary projection of several CQL types is not: a `Udt` becomes a `dict`, a
/// `List` becomes a `list` UNCONDITIONALLY (`list_to_py`), and a `Set` becomes a
/// `list` when it contains a UDT anywhere inside (`set_to_py`).
/// This function is the TOTAL hashable projection over `cqlite_core::Value`:
///
/// - `List`, `Tuple` → `tuple` (elements recursively projected)
/// - `Set` → `frozenset` (elements recursively projected)
/// - `Map` → `tuple` of `(key, value)` tuples (both sides recursively projected)
/// - `Udt` → `frozenset` of `(field_name, value)` tuples
/// - `Frozen` → unwrap and recurse
/// - `Json` → `tuple` (array) / `frozenset` of pairs (object), recursively
///   (reachability: the one statement of it is at the `Value::Json` arm)
/// - every other variant → its ordinary [`value_to_py`] projection, which is
///   already hashable
///
/// # Totality is enforced by the COMPILER (issue #3500)
///
/// There is deliberately **no `_ =>` arm**: every one of `Value`'s variants is
/// named. A wildcard is precisely what made this function non-total — a UDT
/// reached through a `Tuple` or through a nested `Set` fell through to
/// [`value_to_py`], which returns an unhashable `dict`/`list`, and the whole
/// `SELECT` raised `TypeError: unhashable type: 'dict'` (or `'list'`) on legal
/// CQL. Naming every variant turns a future `Value` variant into a COMPILE
/// ERROR here instead of a runtime `TypeError` on somebody's data, which is
/// strictly stronger than detecting it at run time.
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
/// [`set_to_py`]: `set_to_py`'s UDT branch returns an unhashable `list` **on
/// purpose** (issue #804) because that is the right answer for a top-level
/// column, and the wrong one inside a hashable position.
#[deny(clippy::wildcard_enum_match_arm)]
pub fn value_to_hashable_key(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        // Both project to a Python `tuple`: a list needs one for hashability, a
        // CQL tuple maps to one anyway. The element projection is what matters —
        // it must recurse HERE so a nested UDT becomes a frozenset rather than a
        // dict.
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
            // #3500 failure, and it is history, not current behaviour.)
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
            // value_to_py (which would return an unhashable dict for UDTs).
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
            // UDT in a hashable position: a frozenset of (name, value) tuples.
            // Fields: _type, _keyspace, and all named fields (matching udt_to_py).
            //
            // The pairs are pushed in schema order and NOT sorted: a
            // `frozenset`'s hash and equality are order-independent BY
            // CONSTRUCTION, so ordering the pairs first cannot change the
            // resulting object. An earlier version sorted by field name "so the
            // frozenset hash is order-independent" — dead work resting on a
            // false premise, removed rather than kept as a cosmetic step.
            let mut pairs: Vec<PyObject> = Vec::with_capacity(udt.fields.len() + 2);

            // _type and _keyspace metadata fields
            let type_key = "_type".into_pyobject(py)?.into_any().unbind();
            let type_val = udt
                .type_name
                .as_str()
                .into_pyobject(py)?
                .into_any()
                .unbind();
            pairs.push(PyTuple::new(py, [type_key, type_val])?.into_any().unbind());

            let ks_key = "_keyspace".into_pyobject(py)?.into_any().unbind();
            let ks_val = udt.keyspace.as_str().into_pyobject(py)?.into_any().unbind();
            pairs.push(PyTuple::new(py, [ks_key, ks_val])?.into_any().unbind());

            // Named fields, in schema order (immaterial — see above).
            for field in &udt.fields {
                let val = match &field.value {
                    Some(v) => value_to_hashable_key(py, v)?,
                    None => py.None(),
                };
                let k = field.name.as_str().into_pyobject(py)?.into_any().unbind();
                pairs.push(PyTuple::new(py, [k, val])?.into_any().unbind());
            }

            Ok(PyFrozenSet::new(py, &pairs)?.into_any().unbind())
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
        // `hash(-0.0) == hash(0.0)`); a UDT field literally named
        // `_type`/`_keyspace` shadowing the metadata pair injected by the `Udt`
        // arm; and `Null` vs `Tombstone` both projecting to `None` (listed for
        // completeness — probably desired). All PRE-DATE #3500, which neither
        // introduced nor widened any; fixing them needs a type-preserving
        // projection, a behaviour change out of scope here.
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
        // `#[pyclass(frozen, eq, hash)]` [`Duration`] class; and `Null` /
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
/// Scalars delegate to [`json_to_py`], so its NUMBER projection is inherited and
/// with it a known collapse: JSON `1`/`1.0`/`true` become Python
/// `int`/`float`/`bool`, which compare equal with equal hashes, so `{"a": 1}`
/// and `{"a": 1.0}` are the same frozenset. That is one instance of the
/// projection-collapse class tracked by **#3615** (whose other members —
/// `-0.0`/`+0.0`, `_type` field shadowing, `Null`/`Tombstone` — have nothing to
/// do with JSON); recorded in full at the `Value::Json` arm.
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

/// Convert milliseconds since epoch to datetime.datetime (UTC).
fn timestamp_to_datetime(py: Python<'_>, millis: i64) -> PyResult<PyObject> {
    let datetime = py.import("datetime")?;
    let dt_class = datetime.getattr("datetime")?;
    let timezone = datetime.getattr("timezone")?;
    let utc = timezone.getattr("utc")?;
    let timedelta = datetime.getattr("timedelta")?;

    // Build the datetime integer-exactly: epoch(0, tz=utc) + timedelta(milliseconds=millis).
    //
    // Routing `millis` through an f64 (as the old `fromtimestamp` path did) loses
    // microsecond exactness for far-future/far-past timestamps because such values
    // exceed the 53-bit mantissa. Passing `millis` to `timedelta(milliseconds=...)`
    // as a Python int keeps the conversion exact, and `timedelta` normalizes the
    // value to days/seconds/microseconds with correct handling of negatives — which
    // preserves the pre-epoch/negative correctness of Issue #341 without the
    // Euclidean-division dance.
    let epoch = dt_class.call_method1("fromtimestamp", (0i64, utc))?;

    let kwargs = PyDict::new(py);
    kwargs.set_item("milliseconds", millis)?;
    let td = timedelta.call((), Some(&kwargs))?;

    let dt = epoch.call_method1("__add__", (td,))?;
    Ok(dt.into_pyobject(py)?.into_any().unbind())
}

/// Convert days since epoch to datetime.date.
fn date_to_pydate(py: Python<'_>, days: i32) -> PyResult<PyObject> {
    let datetime = py.import("datetime")?;
    let date_class = datetime.getattr("date")?;

    // CQL date is days since 1970-01-01 with center at 2^31
    // The value stored is unsigned centered at 2^31
    // Actual days = stored_value - 2^31
    // For our purposes, we treat it as signed days since epoch
    let epoch = date_class.call_method1("fromordinal", (719163i32,))?; // 1970-01-01
    let timedelta = datetime.getattr("timedelta")?;
    // Use keyword argument for days parameter
    let kwargs = PyDict::new(py);
    kwargs.set_item("days", days)?;
    let delta = timedelta.call((), Some(&kwargs))?;
    let result = epoch.call_method1("__add__", (delta,))?;
    Ok(result.into_pyobject(py)?.into_any().unbind())
}

/// Exact CQL `duration` value.
///
/// A CQL `duration` has three independent components — `months`, `days`, and
/// `nanos` — that cannot be collapsed into a single scalar without loss (a
/// month is not a fixed number of days, and a day is not a fixed number of
/// nanoseconds). This type preserves all three exactly, mirroring the Node
/// binding's `{ months, days, nanos }` object.
///
/// Before v0.13 the Python binding returned a `datetime.timedelta`, which
/// approximated months as 30 days and truncated nanoseconds to microseconds
/// (the M4 §5.2 lossy mapping). That approximation has been removed.
#[pyclass(module = "cqlite", frozen, eq, hash)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Duration {
    /// Whole months (may be negative).
    #[pyo3(get)]
    pub months: i32,
    /// Whole days (may be negative).
    #[pyo3(get)]
    pub days: i32,
    /// Sub-day component in nanoseconds (may be negative).
    #[pyo3(get)]
    pub nanos: i64,
}

#[pymethods]
impl Duration {
    /// Construct a `Duration` from its exact components.
    #[new]
    fn new(months: i32, days: i32, nanos: i64) -> Self {
        Duration {
            months,
            days,
            nanos,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Duration(months={}, days={}, nanos={})",
            self.months, self.days, self.nanos
        )
    }
}

/// Register value-conversion types (the exact [`Duration`] class) on the module.
pub fn register_value(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Duration>()?;
    Ok(())
}

/// Convert UUID bytes to Python uuid.UUID object.
fn uuid_to_py(py: Python<'_>, uuid: &[u8; 16]) -> PyResult<PyObject> {
    let uuid_mod = py.import("uuid")?;
    let uuid_class = uuid_mod.getattr("UUID")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("bytes", PyBytes::new(py, uuid))?;
    let result = uuid_class.call((), Some(&kwargs))?;
    Ok(result.into_any().unbind())
}

/// Format UUID bytes as standard string representation (for tests/debug).
#[cfg(test)]
fn uuid_to_string(uuid: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid[0], uuid[1], uuid[2], uuid[3],
        uuid[4], uuid[5],
        uuid[6], uuid[7],
        uuid[8], uuid[9],
        uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15]
    )
}

/// Convert variable-length integer bytes to a Python `int`.
///
/// A thin adapter: the CQL `varint` semantic (big-endian two's complement, empty
/// payload meaning zero, sign extension at any width) is decided ONCE in
/// [`cqlite_ffi_common::varint::varint_to_bigint`], and pyo3's `num-bigint`
/// conversion hands that value straight to Python. No sign handling, no byte
/// round trip and no length dispatch remain here (issue #1452).
///
/// `pub(crate)` so the internal `_varint_from_bytes` test-support helper
/// (lib.rs) can drive this exact production path.
pub(crate) fn varint_to_pyint(py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject> {
    Ok(cqlite_ffi_common::varint::varint_to_bigint(bytes)
        .into_pyobject(py)?
        .into_any()
        .unbind())
}

/// Render a CQL DECIMAL to its exact text through the ONE shared implementation.
///
/// The single Python-specific step is mapping the shared
/// [`cqlite_ffi_common::decimal::DecimalError`] onto
/// [`cqlite_core::Error::corruption`] and thence through this binding's existing
/// production [`to_py_err`] path, so a refused cell's exception CLASS still
/// comes from the one FFI error contract and its MESSAGE has one spelling in the
/// repository (issue #1452).
///
/// `pub(crate)` so the vector test-support surface can report the exact rendered
/// text: `decimal.Decimal.__str__` re-normalises exponent form (`Decimal("123e2")`
/// prints as `1.23E+4`), so the text — not the object's `str()` — is what the
/// cross-binding vectors compare.
pub(crate) fn decimal_render_text(scale: i32, unscaled: &[u8]) -> PyResult<String> {
    cqlite_ffi_common::decimal::decimal_to_string(scale, unscaled)
        .map_err(|err| to_py_err(cqlite_core::Error::corruption(err.to_string())))
}

/// Convert decimal to Python `decimal.Decimal`.
///
/// A thin adapter over [`decimal_render_text`]: the digit split, the sign, the
/// scale arithmetic and the refusal policy all live in the shared crate. The
/// previous body's `int.from_bytes` + Python `str()` round trip — and the
/// `sys.get_int_max_str_digits()` probe that made a well-formed value raise here
/// while the Node binding rendered it — are gone (issue #1452; see
/// `CHANGELOG.md`).
///
/// `pub(crate)` so the internal `_decimal_from_parts` test helper (lib.rs) can
/// drive this exact production path, exercising the fail-closed
/// corrupt-DECIMAL guard without a multi-kilobyte on-disk fixture.
pub(crate) fn decimal_to_pydecimal(
    py: Python<'_>,
    scale: i32,
    unscaled: &[u8],
) -> PyResult<PyObject> {
    let text = decimal_render_text(scale, unscaled)?;
    let decimal_mod = py.import("decimal")?;
    let decimal_class = decimal_mod.getattr("Decimal")?;
    Ok(decimal_class.call1((text,))?.into_any().unbind())
}

/// Convert serde_json::Value to Python object.
fn json_to_py(py: Python<'_>, json: &serde_json::Value) -> PyResult<PyObject> {
    match json {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                // Fallback to string representation
                Ok(n.to_string().into_pyobject(py)?.into_any().unbind())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let items: Vec<PyObject> = arr
                .iter()
                .map(|v| json_to_py(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, items)?.into_any().unbind())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Convert CQL list to Python list.
fn list_to_py(py: Python<'_>, items: &[Value]) -> PyResult<PyObject> {
    let converted: Vec<PyObject> = items
        .iter()
        .map(|v| value_to_py(py, v))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyList::new(py, converted)?.into_any().unbind())
}

/// Convert CQL set to Python frozenset (or list when elements contain UDTs).
///
/// CQL SET semantics require unique, ordered elements. For scalar types, we
/// return a Python `frozenset` (immutable, hashable, set-semantic). However,
/// `dict` objects are unhashable in Python, so `SET<FROZEN<UDT>>` columns
/// cannot use `frozenset`. In that case we fall back to a `list`, which
/// matches the CLI JSON output and is consistent with how the CLI renders
/// SET-of-UDT (see epic #795 and issue #804).
fn set_to_py(py: Python<'_>, items: &[Value]) -> PyResult<PyObject> {
    // Check whether any element is (or wraps) a UDT.
    // UDT values become Python dicts, which are unhashable and cannot be
    // placed in a frozenset.
    let has_udt = items.iter().any(contains_udt);

    if has_udt {
        // Return a list so that each UDT element stays as a plain Python dict.
        // This aligns with the CLI JSON representation of SET<FROZEN<UDT>>.
        let converted: Vec<PyObject> = items
            .iter()
            .map(|v| value_to_py(py, v))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new(py, converted)?.into_any().unbind())
    } else {
        let converted: Vec<PyObject> = items
            .iter()
            .map(|v| value_to_hashable_key(py, v))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyFrozenSet::new(py, &converted)?.into_any().unbind())
    }
}

/// Return `true` if `value` is or CONTAINS a UDT value, at any nesting depth.
///
/// Used by [`set_to_py`] to decide whether a `SET` is returned as a `frozenset`
/// (no UDT anywhere inside, so every element projects to something hashable) or
/// as a `list` (a UDT is in there, and its `dict` projection is unhashable).
///
/// # Why this has to be a full traversal (issue #3500)
///
/// It used to look only through `Frozen`, so a UDT reached through a `Tuple`, a
/// nested `Set`, a `Map` or a `List` was invisible: `set_to_py` took the
/// `frozenset` branch and Python raised `TypeError: unhashable type: 'dict'` (or
/// `'list'`) on legal CQL such as `set<frozen<tuple<frozen<udt>, int>>>`. The
/// answer must therefore be about the whole subtree, not the outermost wrapper.
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
fn contains_udt(value: &Value) -> bool {
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

/// Convert CQL map to Python dict.
fn map_to_py(py: Python<'_>, pairs: &[(Value, Value)]) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (k, v) in pairs {
        let key = value_to_hashable_key(py, k)?;
        let val = value_to_py(py, v)?;
        dict.set_item(key, val)?;
    }
    Ok(dict.into_any().unbind())
}

/// Convert CQL tuple to Python tuple.
fn tuple_to_py(py: Python<'_>, items: &[Value]) -> PyResult<PyObject> {
    let converted: Vec<PyObject> = items
        .iter()
        .map(|v| value_to_py(py, v))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, converted)?.into_any().unbind())
}

/// Convert UDT to Python dict with field names as keys.
fn udt_to_py(py: Python<'_>, udt: &cqlite_core::UdtValue) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    // Add type metadata
    dict.set_item("_type", &udt.type_name)?;
    dict.set_item("_keyspace", &udt.keyspace)?;
    // Add fields
    for field in &udt.fields {
        let value = match &field.value {
            Some(v) => value_to_py(py, v)?,
            None => py.None(),
        };
        dict.set_item(&field.name, value)?;
    }
    Ok(dict.into_any().unbind())
}

/// Convert inet bytes to Python `ipaddress.IPv4Address` or `IPv6Address`.
///
/// A thin adapter: the 4/16 length dispatch and the malformed-length message are
/// decided ONCE in [`cqlite_ffi_common::inet`], so this module holds no literal
/// copy of that message (issue #1453 had aligned the two bindings by
/// hand-copying the string into both files; issue #1452 removed the copy). Per
/// the no-heuristics mandate (issue #28) there is no passthrough or hex-fallback
/// branch: the only outcomes are IPv4, IPv6 and a typed error.
///
/// The error class stays `ParseError` (a malformed-scalar decode), unchanged by
/// the extraction.
pub(crate) fn inet_to_py(py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject> {
    use cqlite_ffi_common::inet::InetKind;

    let kind = cqlite_ffi_common::inet::inet_kind(bytes)
        .map_err(|err| crate::error::ParseError::new_err(err.to_string()))?;
    let ipaddress = py.import("ipaddress")?;
    // `ipaddress` builds both families from PACKED BYTES, so this binding never
    // formats an address itself — the shared part is the dispatch, not the text.
    let class = match kind {
        InetKind::V4 => ipaddress.getattr("IPv4Address")?,
        InetKind::V6 => ipaddress.getattr("IPv6Address")?,
    };
    let addr = class.call1((PyBytes::new(py, bytes),))?;
    Ok(addr.into_any().unbind())
}

/// Helper to create a KeyError for missing columns.
pub fn key_error(key: &str) -> PyErr {
    PyKeyError::new_err(key.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_formatting() {
        let uuid: [u8; 16] = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
            0xde, 0xf0,
        ];
        let formatted = uuid_to_string(&uuid);
        assert_eq!(formatted, "12345678-9abc-def0-1234-56789abcdef0");
    }

    // The two inet formatting tests that lived here exercised a `#[cfg(test)]`-only
    // THIRD inet formatter whose hex fallback contradicted production behaviour.
    // Both the formatter and the tests moved to
    // `cqlite-ffi-common/src/inet.rs`, where they exercise the production path
    // (issue #1452). Cross-binding coverage is in `tests/test_shared_vectors.py`.
}
