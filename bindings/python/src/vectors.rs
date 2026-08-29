//! Test-support: render the committed cross-binding vectors through THIS
//! binding's production conversion paths (issue #1452).
//!
//! The tables live in `cqlite_ffi_common::vectors` and are read by the Node
//! binding's twin surface too, so a divergence between the bindings — or a
//! re-introduced local implementation in either — fails BOTH suites. Nothing
//! here decides pass/fail: it reports `expected` beside `actual` and
//! `tests/test_shared_vectors.py` asserts.
//!
//! Not part of the stable public API; the module's single `#[pyfunction]` is
//! registered as `_ffi_common_render_vectors`.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use cqlite_ffi_common::vectors::{
    vector_outcome, VectorOutcome, DECIMAL_VECTORS, INET_VECTORS, VARINT_VECTORS,
};
use cqlite_core::Value;

/// Turn one reported outcome into the dict shape both binding suites consume.
///
/// `cql_type`, `scale` and `bytes` are carried so a suite can re-drive the same
/// input through another production surface (e.g. `_decimal_from_parts`) without
/// a second test-support function.
fn outcome_dict<'py>(
    py: Python<'py>,
    cql_type: &str,
    scale: i32,
    input: &[u8],
    reported: &VectorOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("cql_type", cql_type)?;
    dict.set_item("name", reported.name)?;
    dict.set_item("kind", reported.kind)?;
    dict.set_item("expected", reported.expected.as_str())?;
    dict.set_item("outcome", reported.outcome)?;
    dict.set_item("actual", reported.actual.as_str())?;
    dict.set_item("scale", scale)?;
    dict.set_item("bytes", PyBytes::new(py, input))?;
    Ok(dict)
}

/// The message text of a raised Python exception, for the `actual` field of a
/// refusal.
fn error_text(py: Python<'_>, err: &PyErr) -> String {
    err.value(py).to_string()
}

/// Render every committed vector through this binding's production path.
///
/// * **DECIMAL** goes through [`crate::value::decimal_render_text`], the function
///   `decimal_to_pydecimal` itself calls. The rendered TEXT is what the vectors
///   compare, because `decimal.Decimal.__str__` re-normalises exponent form
///   (`Decimal("123e2")` prints `1.23E+4`) and would make a character-for-character
///   cross-binding comparison a comparison of Python's formatter. The full
///   object path is asserted separately in `test_shared_vectors.py` via
///   `_decimal_from_parts`, using `Decimal` value equality.
/// * **VARINT** and **INET** go through the full [`crate::value::value_to_py`]
///   dispatch; `str(int)` and `str(IPv4Address/IPv6Address)` are already the
///   canonical forms, so no re-normalisation is in the way.
#[pyfunction]
pub fn _ffi_common_render_vectors(py: Python<'_>) -> PyResult<Py<PyList>> {
    let mut rows: Vec<Bound<'_, PyDict>> = Vec::new();

    for vector in DECIMAL_VECTORS {
        let unscaled = vector.unscaled.bytes();
        let produced = crate::value::decimal_render_text(vector.scale, &unscaled);
        let message = produced.as_ref().err().map(|err| error_text(py, err));
        let reported = vector_outcome(
            vector.name,
            vector.expect,
            match (&produced, &message) {
                (Ok(text), _) => Ok(text.as_str()),
                (Err(_), Some(text)) => Err(text.as_str()),
                // A `PyErr` always renders a message; this arm cannot be reached.
                (Err(_), None) => Err(""),
            },
        );
        rows.push(outcome_dict(
            py,
            "decimal",
            vector.scale,
            &unscaled,
            &reported,
        )?);
    }

    for vector in VARINT_VECTORS {
        let bytes = vector.bytes.bytes();
        let produced = crate::value::value_to_py(py, &Value::Varint(bytes.clone().into()))
            .and_then(|obj| obj.bind(py).str().map(|s| s.to_string()));
        let message = produced.as_ref().err().map(|err| error_text(py, err));
        let reported = vector_outcome(
            vector.name,
            vector.expect,
            match (&produced, &message) {
                (Ok(text), _) => Ok(text.as_str()),
                (Err(_), Some(text)) => Err(text.as_str()),
                (Err(_), None) => Err(""),
            },
        );
        rows.push(outcome_dict(py, "varint", 0, &bytes, &reported)?);
    }

    for vector in INET_VECTORS {
        let bytes = vector.bytes.bytes();
        let produced = crate::value::value_to_py(py, &Value::Inet(bytes.clone().into()))
            .and_then(|obj| obj.bind(py).str().map(|s| s.to_string()));
        let message = produced.as_ref().err().map(|err| error_text(py, err));
        let reported = vector_outcome(
            vector.name,
            vector.expect,
            match (&produced, &message) {
                (Ok(text), _) => Ok(text.as_str()),
                (Err(_), Some(text)) => Err(text.as_str()),
                (Err(_), None) => Err(""),
            },
        );
        rows.push(outcome_dict(py, "inet", 0, &bytes, &reported)?);
    }

    Ok(PyList::new(py, rows)?.unbind())
}
