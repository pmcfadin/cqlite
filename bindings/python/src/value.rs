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
        Value::Text(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
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

/// Convert a Value for use as a Python dict key (must be hashable).
///
/// Python dicts require hashable keys. This converts:
/// - List → tuple (recursively)
/// - Map → tuple of (key, value) tuples
/// - Set → frozenset (elements recursively made hashable)
/// - Frozen → unwrap and recurse
/// - UDT → frozenset of (field_name, hashable_value) tuples (sorted by name)
/// - Other types → as-is (already hashable)
///
/// Note: `SET<FROZEN<UDT>>` is handled at the `set_to_py` level by
/// returning a `list` instead of a `frozenset` (see `set_to_py`). This
/// function is still called for UDTs that appear as MAP keys, which are
/// unusual but possible in CQL.
pub fn value_to_hashable_key(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::List(items) => {
            // Convert list to tuple for hashability
            let converted: Vec<PyObject> = items
                .iter()
                .map(|v| value_to_hashable_key(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyTuple::new(py, converted)?.into_any().unbind())
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
        // Other types are already hashable or handled by value_to_py
        _ => value_to_py(py, value),
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

/// Convert variable-length integer bytes to Python int.
fn varint_to_pyint(py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject> {
    if bytes.is_empty() {
        return Ok(0i64.into_pyobject(py)?.into_any().unbind());
    }

    // Varint is big-endian two's complement
    // Use kwargs for signed parameter as required by Python 3.11+
    let int_class = py.get_type::<pyo3::types::PyInt>();
    let kwargs = PyDict::new(py);
    kwargs.set_item("signed", true)?;
    let py_bytes = PyBytes::new(py, bytes);
    let result = int_class.call_method("from_bytes", (py_bytes, "big"), Some(&kwargs))?;
    Ok(result.into_any().unbind())
}

/// Convert decimal to Python decimal.Decimal.
///
/// `pub(crate)` so the internal `_decimal_from_parts` test helper (lib.rs) can
/// drive this exact conversion path directly, exercising the fail-closed
/// corrupt-DECIMAL guard (issue #1741) without a multi-kilobyte on-disk fixture.
pub(crate) fn decimal_to_pydecimal(
    py: Python<'_>,
    scale: i32,
    unscaled: &[u8],
) -> PyResult<PyObject> {
    let decimal_mod = py.import("decimal")?;
    let decimal_class = decimal_mod.getattr("Decimal")?;

    if unscaled.is_empty() {
        return Ok(decimal_class
            .call1(("0",))?
            .into_pyobject(py)?
            .into_any()
            .unbind());
    }

    // Convert unscaled bytes to integer using kwargs for signed parameter
    let int_class = py.get_type::<pyo3::types::PyInt>();
    let kwargs = PyDict::new(py);
    kwargs.set_item("signed", true)?;
    let py_bytes = PyBytes::new(py, unscaled);
    let unscaled_int = int_class.call_method("from_bytes", (py_bytes, "big"), Some(&kwargs))?;

    // Apply scale: result = unscaled * 10^(-scale)
    if scale == 0 {
        Ok(decimal_class
            .call1((unscaled_int,))?
            .into_pyobject(py)?
            .into_any()
            .unbind())
    } else {
        // Fail-closed guard (issue #1741, abort-safety regression). Real Cassandra
        // DECIMAL values are tiny; a multi-thousand-digit unscaled value or an
        // absurd scale only arises from a CORRUPT SSTable. The rendering below
        // would otherwise (a) call Python `str()` on the unscaled int — which
        // raises a bare `ValueError` once the digit count exceeds
        // `sys.get_int_max_str_digits()` (py3.11+, default 4300) — or (b) use
        // `scale` as a `format!` width, which panics with "Formatting argument
        // out of range". Neither is a catchable driver error (the former escapes
        // as an uncaught `ValueError`, aborting the caller), so we refuse to
        // stringify an unbounded integer and instead surface a typed corruption
        // error. This preserves the abort-safety guarantee (issue #1437/#1440):
        // a corrupt SSTable raises `CqliteError`, it never crashes the driver.
        //
        // The read-side tombstone/TTL shadowing added for #1741 changed which
        // corrupt row surfaces first on the truncated/bitflipped fixtures,
        // exposing this pre-existing binding fragility (the unscaled-`str()`
        // path) that the previous emit order happened to avoid.
        const DECIMAL_HARD_DIGIT_CAP: usize = 1_000_000;
        // Python's own configured int->str safety threshold (py3.11+, 0 ==
        // unlimited). Absent on older interpreters, where there is no such limit;
        // fall back to the hard cap so the `format!`-width panic is still avoided.
        let py_int_limit: usize = py
            .import("sys")
            .and_then(|sys| sys.getattr("get_int_max_str_digits"))
            .and_then(|f| f.call0())
            .and_then(|v| v.extract::<i64>())
            .map(|n| if n <= 0 { 0 } else { n as usize })
            .unwrap_or(0);
        let cap = match py_int_limit {
            0 => DECIMAL_HARD_DIGIT_CAP,
            n => n.min(DECIMAL_HARD_DIGIT_CAP),
        };
        // TIGHT upper bound on the decimal digit count of |unscaled|. For an
        // N-byte SIGNED two's-complement integer one bit is the sign, so the
        // MAGNITUDE is at most 2^(8N-1) — hence at most ceil((8N-1) * log10(2))
        // decimal digits. That product is never an integer (log10(2) is
        // irrational × an integer), so `ceil` equals `floor + 1`, which is the
        // EXACT digit-count upper bound: no extra rounding margin is needed. The
        // previous `ceil(N * log10(256)) + 1` added a spurious +1 and over-rejected
        // a minimal value whose magnitude sits exactly at the cap (e.g. a 1785-byte
        // integer fits in 4300 digits but the old formula computed 4301). Rejecting
        // only when this tight bound STILL exceeds `cap` lets every representable
        // value render while a truly unbounded/corrupt byte length is still refused
        // (fail-closed) — the abort-safety guarantee is preserved. Compute the bit
        // count in f64 so `8*len - 1` cannot underflow `usize` (len == 0 yields a
        // negative product → 0 digits after the clamp); the float->int `as`
        // saturates in Rust, so the guard never wraps or under-counts an oversized
        // value.
        let magnitude_bits = 8.0 * (unscaled.len() as f64) - 1.0;
        let max_digits = (magnitude_bits * std::f64::consts::LOG10_2).ceil().max(0.0) as usize;
        if max_digits > cap || (scale.unsigned_abs() as usize) > cap {
            return Err(to_py_err(cqlite_core::Error::corruption(format!(
                "DECIMAL cell not representable (scale={scale}, unscaled_len={} bytes, \
                 cap={cap} digits): corrupt SSTable — refusing to stringify an unbounded \
                 integer (issue #1741)",
                unscaled.len()
            ))));
        }

        // Create string representation for exact decimal
        // Convert Python int to string by calling str()
        let builtins = py.import("builtins")?;
        let str_func = builtins.getattr("str")?;
        let unscaled_str_obj = str_func.call1((&unscaled_int,))?;
        let unscaled_str: String = unscaled_str_obj.extract()?;
        let decimal_str = if scale > 0 {
            // Positive scale means divide by 10^scale
            let len = unscaled_str.len();
            let scale_usize = scale as usize;
            if let Some(digits) = unscaled_str.strip_prefix('-') {
                if digits.len() <= scale_usize {
                    format!("-0.{:0>width$}", digits, width = scale_usize)
                } else {
                    let split_point = digits.len() - scale_usize;
                    format!("-{}.{}", &digits[..split_point], &digits[split_point..])
                }
            } else if len <= scale_usize {
                format!("0.{:0>width$}", unscaled_str, width = scale_usize)
            } else {
                let split_point = len - scale_usize;
                format!(
                    "{}.{}",
                    &unscaled_str[..split_point],
                    &unscaled_str[split_point..]
                )
            }
        } else {
            // Negative scale means multiply by 10^(-scale)
            format!("{}e{}", unscaled_str, -scale)
        };
        Ok(decimal_class
            .call1((decimal_str,))?
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }
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

/// Return `true` if `value` is or wraps a UDT value.
///
/// Used by `set_to_py` to decide whether a `SET` should be returned as a
/// `frozenset` (scalars, always hashable) or a `list` (UDT elements, whose
/// dict representation is unhashable).
fn contains_udt(value: &Value) -> bool {
    match value {
        Value::Udt(_) => true,
        Value::Frozen(inner) => contains_udt(inner),
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

/// Convert inet bytes to Python ipaddress.IPv4Address or IPv6Address.
fn inet_to_py(py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject> {
    let ipaddress = py.import("ipaddress")?;
    match bytes.len() {
        4 => {
            // IPv4: Use ipaddress.IPv4Address(packed_bytes)
            let ipv4_class = ipaddress.getattr("IPv4Address")?;
            let py_bytes = PyBytes::new(py, bytes);
            let addr = ipv4_class.call1((py_bytes,))?;
            Ok(addr.into_any().unbind())
        }
        16 => {
            // IPv6: Use ipaddress.IPv6Address(packed_bytes)
            let ipv6_class = ipaddress.getattr("IPv6Address")?;
            let py_bytes = PyBytes::new(py, bytes);
            let addr = ipv6_class.call1((py_bytes,))?;
            Ok(addr.into_any().unbind())
        }
        _ => {
            // Fallback: Return raw bytes for invalid/unknown length
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        }
    }
}

/// Convert inet bytes to IP address string (for tests/debug).
#[cfg(test)]
fn inet_to_string(bytes: &[u8]) -> String {
    match bytes.len() {
        4 => {
            // IPv4
            format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3])
        }
        16 => {
            // IPv6
            let segments: Vec<String> = (0..8)
                .map(|i| {
                    let high = bytes[i * 2] as u16;
                    let low = bytes[i * 2 + 1] as u16;
                    format!("{:x}", (high << 8) | low)
                })
                .collect();
            segments.join(":")
        }
        _ => {
            // Fallback: hex representation
            format!("0x{}", hex::encode(bytes))
        }
    }
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

    #[test]
    fn test_inet_ipv4_formatting() {
        let ipv4 = vec![192, 168, 1, 1];
        let formatted = inet_to_string(&ipv4);
        assert_eq!(formatted, "192.168.1.1");
    }

    #[test]
    fn test_inet_ipv6_formatting() {
        let ipv6 = vec![
            0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70,
            0x73, 0x34,
        ];
        let formatted = inet_to_string(&ipv6);
        assert_eq!(formatted, "2001:db8:85a3:0:0:8a2e:370:7334");
    }
}
