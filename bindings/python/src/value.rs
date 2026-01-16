//! Value conversion from cqlite_core to Python types.
//!
//! This module handles conversion of all CQL data types to their Python equivalents.
//! The mapping follows M4 spec section 5.2 for type fidelity.

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFrozenSet, PyList, PyTuple};

use cqlite_core::Value;

/// Convert a CQL Value to a Python object.
///
/// Handles all CQL types with proper Python type mapping:
/// - Primitives: Null→None, Boolean→bool, Integer→int, Float→float, Text→str
/// - Binary: Blob→bytes, Uuid→str (formatted), Inet→str (IP format)
/// - Temporal: Timestamp→datetime, Date→date, Time→time
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
        Value::Time(t) => time_to_pytime(py, *t),
        Value::Uuid(u) => Ok(uuid_to_string(u).into_pyobject(py)?.into_any().unbind()),
        Value::Varint(v) => varint_to_pyint(py, v),
        Value::Decimal { scale, unscaled } => decimal_to_pydecimal(py, *scale, unscaled),
        Value::Duration {
            months,
            days,
            nanos,
        } => duration_to_dict(py, *months, *days, *nanos),
        Value::Json(j) => json_to_py(py, j),
        Value::List(l) => list_to_py(py, l),
        Value::Set(s) => set_to_py(py, s),
        Value::Map(m) => map_to_py(py, m),
        Value::Tuple(t) => tuple_to_py(py, t),
        Value::Udt(u) => udt_to_py(py, u),
        Value::Frozen(v) => value_to_py(py, v),
        Value::Inet(b) => Ok(inet_to_string(b).into_pyobject(py)?.into_any().unbind()),
        Value::Tombstone(_) => Ok(py.None()), // Treat deleted data as None
    }
}

/// Convert a Value for use as a Python dict key (must be hashable).
///
/// Python dicts require hashable keys. This converts:
/// - List → tuple (recursively)
/// - Set → frozenset (already hashable from set_to_py)
/// - Other types → as-is
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

    // Convert milliseconds to seconds and microseconds
    // Use Euclidean division/remainder to correctly handle negative timestamps
    let seconds = millis.div_euclid(1000);
    let micros = millis.rem_euclid(1000) * 1000;

    // Use fromtimestamp with UTC timezone
    let dt = dt_class.call_method1(
        "fromtimestamp",
        (seconds as f64 + micros as f64 / 1_000_000.0, utc),
    )?;
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

/// Convert nanoseconds since midnight to datetime.time.
fn time_to_pytime(py: Python<'_>, nanos: i64) -> PyResult<PyObject> {
    let datetime = py.import("datetime")?;
    let time_class = datetime.getattr("time")?;

    // Convert nanoseconds to hours, minutes, seconds, microseconds
    let total_micros = nanos / 1000;
    let total_seconds = total_micros / 1_000_000;
    let micros = (total_micros % 1_000_000) as i32;
    let total_minutes = total_seconds / 60;
    let seconds = (total_seconds % 60) as i32;
    let hours = (total_minutes / 60) as i32;
    let minutes = (total_minutes % 60) as i32;

    let time = time_class.call1((hours, minutes, seconds, micros))?;
    Ok(time.into_pyobject(py)?.into_any().unbind())
}

/// Format UUID bytes as standard string representation.
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
fn decimal_to_pydecimal(py: Python<'_>, scale: i32, unscaled: &[u8]) -> PyResult<PyObject> {
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

/// Convert duration to dict with months, days, nanos.
fn duration_to_dict(py: Python<'_>, months: i32, days: i32, nanos: i64) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("months", months)?;
    dict.set_item("days", days)?;
    dict.set_item("nanos", nanos)?;
    Ok(dict.into_any().unbind())
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

/// Convert CQL set to Python frozenset.
fn set_to_py(py: Python<'_>, items: &[Value]) -> PyResult<PyObject> {
    let converted: Vec<PyObject> = items
        .iter()
        .map(|v| value_to_hashable_key(py, v))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyFrozenSet::new(py, &converted)?.into_any().unbind())
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

/// Convert inet bytes to IP address string.
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
