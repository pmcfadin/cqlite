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
/// ordinary projection of several CQL types is not: a `Udt` becomes a `dict`,
/// and a `List`/`Set` that contains UDTs becomes a `list` (see `set_to_py`).
/// This function is the TOTAL hashable projection over `cqlite_core::Value`:
///
/// - `List`, `Tuple` → `tuple` (elements recursively projected)
/// - `Set` → `frozenset` (elements recursively projected)
/// - `Map` → `tuple` of `(key, value)` tuples (both sides recursively projected)
/// - `Udt` → `frozenset` of `(field_name, value)` tuples, sorted by field name
/// - `Frozen` → unwrap and recurse
/// - `Json` → `tuple` (array) / `frozenset` of pairs (object), recursively
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
/// Recursion goes through THIS function, never through [`value_to_py`] or
/// [`set_to_py`]: `set_to_py`'s UDT branch returns an unhashable `list` **on
/// purpose** (issue #804) because that is the right answer for a top-level
/// column, and the wrong one inside a hashable position.
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
            // NOT routed through `set_to_py`, whose UDT fallback returns a
            // `list` — unhashable, and therefore the #3500 failure for
            // `set<frozen<set<frozen<udt>>>>`.
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
            // UDT as a map/set key: represent as a frozenset of (name, value) tuples
            // sorted by field name for deterministic ordering.
            // Fields: _type, _keyspace, and all named fields (matching udt_to_py).
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

            // Named fields (in schema order; sort by name for a stable hash)
            let mut field_tuples: Vec<(&str, PyObject)> = udt
                .fields
                .iter()
                .map(|f| {
                    let v = match &f.value {
                        Some(v) => value_to_hashable_key(py, v),
                        None => Ok(py.None()),
                    }?;
                    Ok((f.name.as_str(), v))
                })
                .collect::<PyResult<Vec<_>>>()?;

            // Sort by field name so the frozenset hash is order-independent
            field_tuples.sort_by_key(|(name, _)| *name);

            for (name, val) in field_tuples {
                let k = name.into_pyobject(py)?.into_any().unbind();
                pairs.push(PyTuple::new(py, [k, val])?.into_any().unbind());
            }

            Ok(PyFrozenSet::new(py, &pairs)?.into_any().unbind())
        }
        // DEFENSIVE. No real Cassandra SSTable produces `Value::Json`:
        // `ComparatorType::Json` has no INBOUND parser — `schema/parser.rs` maps
        // only OUTWARD, onto `CqlType::Custom("json")` — so no marshal class
        // decodes to this variant. It is still projected hashably rather than
        // delegated to `json_to_py`, which returns an unhashable `list`/`dict`
        // for arrays/objects: that is the exact defect class #3500 removed, and
        // leaving one variant-shaped hole would reintroduce it.
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
/// DEFENSIVE ONLY — see the `Value::Json` arm of [`value_to_hashable_key`] for
/// why this variant cannot arrive from a Cassandra SSTable. Arrays become
/// `tuple`s and objects become `frozenset`s of `(key, value)` pairs so that a
/// JSON value in a hashable position can never be the unhashable `list`/`dict`
/// that [`json_to_py`] would build.
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
/// Every remaining variant is a scalar and cannot contain anything, so it is
/// `false`. Reaching a `Udt` answers `true` immediately: a UDT nested inside
/// another UDT's field cannot change that answer, so there is no recursion into
/// UDT fields here — it would be code with no reachable effect.
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
        _ => false,
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
