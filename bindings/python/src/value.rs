//! Value conversion from cqlite_core to Python types.
//!
//! This module handles conversion of all CQL data types to their Python equivalents.
//! The mapping follows M4 spec section 5.2 for type fidelity.
//!
//! It owns the ORDINARY host conversion only. The HASHABLE PROJECTION — what a
//! value becomes in a `dict`-key or `frozenset`-element position, which
//! deliberately DIFFERS for a `list` and for a UDT-bearing `set` — lives in
//! [`crate::value_hashable`] (split out by issue #3500). `set_to_py` and
//! `map_to_py` call into it; it calls back for the scalar arms.

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFrozenSet, PyList, PyTuple};

use crate::error::to_py_err;
use crate::value_hashable::{contains_udt, value_to_hashable_key};
use cqlite_core::Value;

/// Convert a CQL Value to a Python object.
///
/// Handles all CQL types with proper Python type mapping:
/// - Primitives: Null→None, Boolean→bool, Integer→int, Float→float, Text→str
/// - Binary: Blob→bytes, Uuid→str (formatted), Inet→str (IP format)
/// - Temporal: Timestamp→datetime, Date→date, Time→int (nanoseconds since
///   midnight, lossless), Duration→[`Duration`] (exact months/days/nanos)
/// - Collections: List→list, Set→frozenset, Map→dict, Tuple→tuple
/// - Complex: Udt→[`Udt`] (type identity out of band, issue #3504), Varint→int,
///   Decimal→decimal.Decimal
/// - Special: Tombstone→None, Frozen→unwrap
pub fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        // EMPTY-BUFFER SENTINEL (issue #3805) → the empty Python `str`.
        //
        // `""` is Cassandra's own rendering of an empty fixed-width buffer:
        // `sstabledump` prints `"path" : [ "" ]`
        // (`tools/JsonTransformer.java:444-458` →
        // `db/marshal/AbstractType.java:146-156` →
        // `serializers/Int32Serializer.java:46-49`, whose `toString(null)` is
        // `""`) and `SELECT JSON` yields `{"": v}`
        // (`db/marshal/MapType.java:362-388`), both at `cassandra-5.0.8`.
        //
        // NOT `None`: the entry is PRESENT and its key is DISTINCT from null (a
        // null map key is illegal CQL — `cql3/Maps.java:342-343`). The
        // distinctness lives in the core `Value` type; every SURFACE renders
        // `""` so the three bindings agree (cross-binding parity, issue #1455).
        // DECLARED RESIDUAL: the Python DRIVER hands back its own `EmptyValue`
        // sentinel object here; CQLite does not mirror that (it would need a new
        // Python type and would break 3-way surface agreement), so the type is
        // recoverable from the schema, not from this object.
        Value::Empty(_) => Ok("".into_pyobject(py)?.into_any().unbind()),
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

/// A CQL user-defined-type value, with its **type identity carried out of band**
/// (issue #3504).
///
/// Before this type, a UDT was rendered as a plain `dict` holding `_type` and
/// `_keyspace` alongside the UDT's own fields — one flat namespace shared by
/// control markers and user-controlled field names, with the markers written
/// first. A UDT field named `_type` or `_keyspace` (legal CQL via a quoted
/// identifier) therefore **overwrote** the marker and the type name became
/// unrecoverable. Type identity now lives on the instance and the fields have a
/// namespace of their own, so there is no slot to compete for.
///
/// Mapping access is retained (`udt["street"]`, `"city" in udt`, `len(udt)`,
/// `iter(udt)`, `keys`/`values`/`items`) and delegates to [`method@Self::fields`], so
/// ordinary field access is unchanged. `udt["_type"]` now reaches the FIELD of
/// that name, raising `KeyError` when no such field is declared — that is the
/// removed shared channel, observable.
///
/// Equality and hashing are over `(keyspace, type_name, fields)`, so two UDTs of
/// different declared types with identical fields stay distinct. Hashing a UDT
/// whose field values are themselves unhashable (a `dict` from a nested map)
/// raises `TypeError`, exactly as a tuple containing a list does; in the
/// hashable-key projection ([`value_to_hashable_key`]) every field value has
/// already been projected to a hashable form, so hashing succeeds there.
#[pyclass(module = "cqlite", frozen)]
pub struct Udt {
    /// The declared UDT type name (e.g. `address`), never read from the fields.
    #[pyo3(get)]
    pub type_name: String,
    /// The keyspace the UDT type is declared in, never read from the fields.
    #[pyo3(get)]
    pub keyspace: String,
    /// The UDT's declared fields, name → value. This mapping holds ONLY fields:
    /// no injected metadata entry can appear here, and no field can displace the
    /// type identity above.
    ///
    /// NOT exposed with a derived `#[pyo3(get)]` — see the [`method@Self::fields`]
    /// getter for why that would break the hash invariant.
    pub fields: Py<PyDict>,
}

#[pymethods]
impl Udt {
    /// Construct a `Udt` from its type identity and its field mapping.
    ///
    /// `fields` is COPIED, so a later mutation of the caller's dict cannot change
    /// this value's equality or hash.
    #[new]
    fn new(type_name: String, keyspace: String, fields: &Bound<'_, PyDict>) -> PyResult<Self> {
        Ok(Udt {
            type_name,
            keyspace,
            fields: fields.copy()?.unbind(),
        })
    }

    /// The declared fields, name → value, as a READ-ONLY mapping.
    ///
    /// A derived `#[pyo3(get)]` would hand out a new reference to the very
    /// `dict` that [`Self::__hash__`] and [`Self::__eq__`] read, so
    /// `udt.fields["z"] = 1` would move a `Udt` already used as a `dict` key out
    /// of its hash bucket and make it unretrievable — the class is declared
    /// `frozen` and documented as usable as a `dict` key, so that hole
    /// contradicts its own contract. `__new__` copies the caller's `dict`, which
    /// protects the value from the CONSTRUCTOR's argument but not from this
    /// accessor.
    ///
    /// `types.MappingProxyType` is chosen over returning a fresh `dict` copy
    /// because a copy would ACCEPT the write and silently discard it — a
    /// permissive no-op that reads as success — whereas the proxy REFUSES it
    /// with `TypeError`, making the immutability the class advertises
    /// observable. It is also O(1) rather than O(fields) per access, and every
    /// read shape callers use (`udt.fields[k]`, `dict(udt.fields)`,
    /// `.items()`/`.keys()`/`.values()`, `in`, `len`, iteration, `==` against a
    /// plain `dict`) works identically on a `mappingproxy`. Callers that need a
    /// mutable mapping take `dict(udt.fields)` explicitly.
    #[getter]
    fn fields(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(py
            .import("types")?
            .getattr("MappingProxyType")?
            .call1((self.fields.bind(py),))?
            .unbind())
    }

    /// The value of the field named `key`, raising `KeyError` when the UDT
    /// declares no such field.
    ///
    /// Delegates to `fields`, so `udt["_type"]` reaches a FIELD named `_type` —
    /// never the type name.
    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        Ok(self.fields.bind(py).as_any().get_item(key)?.unbind())
    }

    /// Whether the UDT declares a field named `key`.
    fn __contains__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.fields.bind(py).as_any().contains(key)
    }

    /// Iterate the declared field names, in schema order.
    fn __iter__(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self
            .fields
            .bind(py)
            .as_any()
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// The number of declared fields — no injected entries are counted.
    fn __len__(&self, py: Python<'_>) -> usize {
        self.fields.bind(py).len()
    }

    /// The declared field names, in schema order.
    fn keys(&self, py: Python<'_>) -> Py<PyList> {
        self.fields.bind(py).keys().unbind()
    }

    /// The declared field values, in schema order.
    fn values(&self, py: Python<'_>) -> Py<PyList> {
        self.fields.bind(py).values().unbind()
    }

    /// The declared `(name, value)` pairs, in schema order.
    fn items(&self, py: Python<'_>) -> Py<PyList> {
        self.fields.bind(py).items().unbind()
    }

    /// Equality over `(keyspace, type_name, fields)`.
    ///
    /// The type identity participates, so two UDTs with identical fields but
    /// different declared types are UNEQUAL — the property the previous
    /// `frozenset` projection got from its injected metadata pairs, retained here
    /// without putting metadata in the field namespace.
    /// A non-`Udt` operand yields `NotImplemented`, NOT `False`, so Python falls
    /// back to the other operand's reflected `__eq__` — a future cooperating type
    /// (a UDT-shaped record from another library, a test double) can then
    /// participate in the comparison. Returning `False` here would decide the
    /// comparison unilaterally and silently.
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let Ok(other) = other.downcast::<Udt>() else {
            return Ok(py.NotImplemented());
        };
        let other = other.get();
        let equal = self.type_name == other.type_name
            && self.keyspace == other.keyspace
            && self.fields.bind(py).as_any().eq(other.fields.bind(py))?;
        Ok(equal.into_pyobject(py)?.to_owned().into_any().unbind())
    }

    /// Hash over `(keyspace, type_name, fields)`, consistent with `__eq__`.
    ///
    /// The field mapping is hashed as a `frozenset` of its items, so the hash is
    /// independent of field order while still distinguishing different field
    /// values. A field value that is itself unhashable propagates `TypeError`
    /// from here rather than being silently dropped.
    ///
    /// Cost note: the `frozenset` is rebuilt on EVERY call, so hashing is
    /// O(fields) with an allocation, not O(1) — nothing is cached because a
    /// cached hash would have to be invalidated, and the fields `dict` is
    /// internal but reachable by C-API callers. A hot UDT-keyed-map path should
    /// start here (memoise into a `OnceCell<isize>` on the frozen instance).
    fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        let fields = PyFrozenSet::new(py, self.fields.bind(py).items().iter())?;
        let identity = PyTuple::new(
            py,
            [
                self.keyspace
                    .as_str()
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
                self.type_name
                    .as_str()
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
                fields.into_any().unbind(),
            ],
        )?;
        identity.hash()
    }

    /// A Python-`repr`-shaped rendering, e.g.
    /// `Udt(type_name='address', keyspace='ks', fields={'street': '1 Main St'})`.
    ///
    /// The two strings are rendered by Python's own `repr` rather than Rust's
    /// `{:?}`, so quoting and escaping match every other repr a caller sees.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Udt(type_name={}, keyspace={}, fields={})",
            self.type_name.as_str().into_pyobject(py)?.repr()?,
            self.keyspace.as_str().into_pyobject(py)?.repr()?,
            self.fields.bind(py).as_any().repr()?
        ))
    }
}

/// Register value-conversion types (the exact [`Duration`] class and the
/// out-of-band-identity [`Udt`] class) on the module.
pub fn register_value(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Duration>()?;
    m.add_class::<Udt>()?;
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

/// Convert a JSON number to the Python object that represents it EXACTLY.
///
/// A thin adapter over the ONE shared classifier
/// [`cqlite_ffi_common::json_number::classify_json_number`]: the arm order and
/// the `arbitrary_precision` decision live there, tested where the tests
/// actually run (issue #3505; `cqlite-py`'s Rust half executes nowhere, so a
/// unit test written here would too).
///
/// Python's `int` is arbitrary precision, so BOTH integer classes are exact and
/// neither needs a float:
///
/// * `I64` / `U64` → Python `int`. The `U64` arm is the #3505 fix: the previous
///   body tried `as_i64()` and then `as_f64()`, and for a JSON integer above
///   `i64::MAX` the `as_f64()` call SUCCEEDED LOSSILY — `18446744073709551615`
///   reached Python as `1.8446744073709552e19`.
/// * `F64` → Python `float`. Only a JSON float LITERAL lands here now, where the
///   `f64` is the exact parsed value.
/// * `Beyond` → an exact `int` via `BigInt` if the text is an integer literal,
///   else a REFUSAL. Never a lossy float, and never the old `n.to_string()`
///   fallback, which shifted the host type from a number to a `str` (the
///   `str` row of the `M4_spec.md` §5.3 host-shape lattice named exactly that
///   source; it is gone).
fn json_number_to_py(py: Python<'_>, n: &serde_json::Number) -> PyResult<PyObject> {
    use cqlite_ffi_common::json_number::JsonNumberClass;
    match cqlite_ffi_common::json_number::classify_json_number(n) {
        JsonNumberClass::I64(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        JsonNumberClass::U64(u) => Ok(u.into_pyobject(py)?.into_any().unbind()),
        JsonNumberClass::F64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        JsonNumberClass::Beyond(text) => {
            match cqlite_ffi_common::json_number::beyond_text_to_bigint(&text) {
                // Python `int` holds it exactly, the same route the VARINT
                // adapter uses (`varint_to_pyint`).
                Some(big) => Ok(big.into_pyobject(py)?.into_any().unbind()),
                // Fail closed. A number nothing can represent exactly is a data
                // fault, reported through this binding's one production error
                // path so it carries the single FFI error contract's identity.
                None => Err(to_py_err(cqlite_core::Error::unsupported_format(
                    cqlite_ffi_common::json_number::beyond_range_message(&text),
                ))),
            }
        }
    }
}

/// Convert serde_json::Value to Python object.
pub(crate) fn json_to_py(py: Python<'_>, json: &serde_json::Value) -> PyResult<PyObject> {
    match json {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => json_number_to_py(py, n),
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
/// return a Python `frozenset` (immutable, hashable, set-semantic).
/// `SET<FROZEN<UDT>>` falls back to a `list`, which matches the CLI JSON output
/// and is consistent with how the CLI renders SET-of-UDT (see epic #795 and issue
/// #804).
///
/// That fallback is RETAINED, and its reason has narrowed. It used to be forced:
/// a UDT was an unhashable `dict`, so no `frozenset` was possible. Since issue
/// #3504 a UDT is a [`Udt`], which IS hashable when its field values are, so the
/// fallback is now a deliberate CLI-parity choice rather than a hard
/// impossibility — changing it would change the observable shape of every
/// `SET<FROZEN<UDT>>` column, which is out of #3504's scope.
fn set_to_py(py: Python<'_>, items: &[Value]) -> PyResult<PyObject> {
    // Check whether any element is (or wraps) a UDT.
    let has_udt = items.iter().any(contains_udt);

    if has_udt {
        // Return a list of `Udt` values, aligning with the CLI JSON
        // representation of SET<FROZEN<UDT>> (issue #804).
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

/// Convert a UDT to a [`Udt`], whose type identity is carried OUT OF BAND.
///
/// Issue #3504: this used to return a plain `dict` seeded with `_type` and
/// `_keyspace` and then filled with the UDT's fields, so a field named `_type` or
/// `_keyspace` — legal CQL via a quoted identifier — overwrote the marker and the
/// type name became unrecoverable. The identity now lives on the instance and
/// `fields` holds nothing but declared fields, so no field name can displace it
/// and no marker can shadow a field.
fn udt_to_py(py: Python<'_>, udt: &cqlite_core::UdtValue) -> PyResult<PyObject> {
    Ok(build_udt(py, udt, value_to_py)?.into_any())
}

/// Build a [`Udt`] from `udt`, converting each field value with `convert`.
///
/// The two callers differ only in that conversion: [`udt_to_py`] uses
/// [`value_to_py`], while [`value_to_hashable_key`]'s `Udt` arm uses itself, so
/// the field values of a projected UDT are hashable. Sharing the construction
/// keeps the two shapes identical by construction — the previous code built the
/// dict and the frozenset independently, which is how the projection came to emit
/// a DUPLICATE `_type` pair while `udt_to_py` merely overwrote one.
pub(crate) fn build_udt(
    py: Python<'_>,
    udt: &cqlite_core::UdtValue,
    convert: impl Fn(Python<'_>, &Value) -> PyResult<PyObject>,
) -> PyResult<Py<Udt>> {
    let fields = PyDict::new(py);
    for field in &udt.fields {
        let value = match &field.value {
            Some(v) => convert(py, v)?,
            None => py.None(),
        };
        fields.set_item(&field.name, value)?;
    }
    Py::new(
        py,
        Udt {
            type_name: udt.type_name.clone(),
            keyspace: udt.keyspace.clone(),
            fields: fields.unbind(),
        },
    )
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
