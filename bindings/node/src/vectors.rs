//! Test-support: render the committed cross-binding vectors through THIS
//! binding's production conversion paths (issue #1452).
//!
//! The tables live in `cqlite_ffi_common::vectors` and the Python binding's twin
//! surface reads the same ones, so a divergence between the bindings — or a
//! re-introduced local implementation in either — fails BOTH suites. Nothing
//! here decides pass/fail: it reports `expected` beside `actual` and
//! `__test__/shared-vectors.test.js` asserts.
//!
//! Not part of the stable public API; `lib/index.js` re-exports the single
//! function as `_ffiCommonRenderVectors`.

use cqlite_core::types::Value;
use cqlite_ffi_common::vectors::{
    vector_outcome, VectorOutcome, DECIMAL_VECTORS, INET_VECTORS, VARINT_VECTORS,
};
use napi::{Env, JsUnknown, Result};

use crate::value::{value_to_napi, ConvCtx};

/// One committed vector, rendered through this binding's production path.
///
/// `cqlType`, `scale` and `bytes` are carried so a suite can re-drive the same
/// input through another surface without a second test-support function.
#[napi_derive::napi(object)]
pub struct VectorReport {
    /// `"decimal"`, `"varint"` or `"inet"`.
    #[napi(js_name = "cqlType")]
    pub cql_type: String,
    /// The entry's stable identifier.
    pub name: String,
    /// `"value"` or `"error"`: which comparison rule the suite applies.
    pub kind: String,
    /// The committed expectation.
    pub expected: String,
    /// `"ok"` if the production path rendered, `"err"` if it refused.
    pub outcome: String,
    /// The rendering's digest, or the production error's message.
    pub actual: String,
    /// The DECIMAL scale (`0` for the other types).
    pub scale: i32,
    /// The entry's input bytes.
    pub bytes: Vec<u8>,
}

impl VectorReport {
    fn new(cql_type: &str, scale: i32, input: &[u8], reported: &VectorOutcome) -> Self {
        VectorReport {
            cql_type: cql_type.to_string(),
            name: reported.name.to_string(),
            kind: reported.kind.to_string(),
            expected: reported.expected.clone(),
            outcome: reported.outcome.to_string(),
            actual: reported.actual.clone(),
            scale,
            bytes: input.to_vec(),
        }
    }
}

/// Render one CQL value through the production [`value_to_napi`] dispatch and
/// reduce the JS result to its string form.
///
/// `String(x)` is the canonical form for every type this is used with: a JS
/// `BigInt` stringifies to its decimal digits and the DECIMAL/INET arms already
/// produce strings, so nothing is re-formatted on the way out.
fn render_through_dispatch(env: &Env, value: &Value) -> Result<String> {
    let ctx = ConvCtx::new(env);
    let rendered: JsUnknown = value_to_napi(&ctx, value)?;
    rendered.coerce_to_string()?.into_utf8()?.into_owned()
}

/// Render every committed vector through this binding's production path.
///
/// All three types go through the full [`value_to_napi`] dispatch — the same
/// call `row_to_object` makes for a real result row — so a binding that re-grew
/// a private implementation would be caught here.
#[napi_derive::napi]
pub fn ffi_common_render_vectors(env: Env) -> Result<Vec<VectorReport>> {
    let mut reports = Vec::new();

    for vector in DECIMAL_VECTORS {
        let unscaled = vector.unscaled.bytes();
        let produced = render_through_dispatch(
            &env,
            &Value::Decimal {
                scale: vector.scale,
                unscaled: unscaled.clone(),
            },
        );
        reports.push(VectorReport::new(
            "decimal",
            vector.scale,
            &unscaled,
            &reported(vector.name, vector.expect, &produced),
        ));
    }

    for vector in VARINT_VECTORS {
        let bytes = vector.bytes.bytes();
        let produced = render_through_dispatch(&env, &Value::Varint(bytes.clone().into()));
        reports.push(VectorReport::new(
            "varint",
            0,
            &bytes,
            &reported(vector.name, vector.expect, &produced),
        ));
    }

    for vector in INET_VECTORS {
        let bytes = vector.bytes.bytes();
        let produced = render_through_dispatch(&env, &Value::Inet(bytes.clone().into()));
        reports.push(VectorReport::new(
            "inet",
            0,
            &bytes,
            &reported(vector.name, vector.expect, &produced),
        ));
    }

    Ok(reports)
}

/// Adapt a `napi::Result<String>` into the shared crate's outcome record.
fn reported(
    name: &'static str,
    expect: cqlite_ffi_common::vectors::Expect,
    produced: &Result<String>,
) -> VectorOutcome {
    match produced {
        Ok(text) => vector_outcome(name, expect, Ok(text.as_str())),
        Err(err) => vector_outcome(name, expect, Err(err.reason.as_str())),
    }
}
