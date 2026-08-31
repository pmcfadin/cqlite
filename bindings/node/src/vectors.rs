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
    vector_outcome, VectorOutcome, DECIMAL_VECTORS, INET_VECTORS, JSON_NUMBER_VECTORS,
    VARINT_VECTORS,
};
use napi::{Env, JsUnknown, Result};
use napi_derive::napi;

use crate::value::{value_to_napi, ConvCtx};

/// One committed vector, rendered through this binding's production path.
///
/// `cqlType`, `scale` and `bytes` are carried so a suite can re-drive the same
/// input through another surface without a second test-support function.
#[napi(object)]
pub struct VectorReport {
    /// `"decimal"`, `"varint"` or `"inet"`.
    #[napi(js_name = "cqlType")]
    pub cql_type: String,
    /// The entry's stable identifier.
    pub name: String,
    /// `"value"` or `"error"`: which comparison rule the suite applies.
    pub kind: String,
    /// The committed expectation (a digest, for a multi-kilobyte rendering).
    pub expected: String,
    /// Lower-case SHA-256 hex of the UTF-8 bytes of the expected rendering, for
    /// an entry committed as a digest; `null` when `expected` is itself exact.
    #[napi(js_name = "expectedSha256")]
    pub expected_sha256: Option<String>,
    /// `"ok"` if the production path rendered, `"err"` if it refused.
    pub outcome: String,
    /// The rendering's digest, or the production error's message — the readable
    /// field for failure messages, never the oracle for a long rendering.
    pub actual: String,
    /// The FULL, un-digested rendering this binding produced (`null` on a
    /// refusal). The suite hashes THIS, so the exact digits get checked.
    pub rendered: Option<String>,
    /// The DECIMAL scale (`0` for the other types).
    pub scale: i32,
    /// The entry's input bytes. For a `json_number` entry these are the UTF-8
    /// bytes of the JSON literal, so a suite can re-drive it through
    /// `_jsonNumberFromText`.
    pub bytes: Vec<u8>,
    /// `"integer"`/`"float"` for a `json_number` entry (issue #3505): the host
    /// SHAPE the value must arrive as. `null` for the byte-input types, which
    /// have no host-shape choice to make.
    #[napi(js_name = "hostKind")]
    pub host_kind: Option<String>,
}

impl VectorReport {
    fn new(cql_type: &str, scale: i32, input: &[u8], reported: &VectorOutcome) -> Self {
        VectorReport {
            cql_type: cql_type.to_string(),
            name: reported.name.to_string(),
            kind: reported.kind.to_string(),
            expected: reported.expected.clone(),
            expected_sha256: reported.expected_sha256.map(|hex| hex.to_string()),
            outcome: reported.outcome.to_string(),
            actual: reported.actual.clone(),
            rendered: reported.rendered.clone(),
            scale,
            bytes: input.to_vec(),
            host_kind: None,
        }
    }

    /// The same record with the JSON host-shape field set (issue #3505).
    fn with_host_kind(mut self, host_kind: &str) -> Self {
        self.host_kind = Some(host_kind.to_string());
        self
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
#[napi]
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

    // JSON numbers (issue #3505). The input is TEXT, so its UTF-8 bytes ride in
    // `bytes` and the suite re-drives the literal through `_jsonNumberFromText`
    // for the host-TYPE assertion — `String(x)` cannot tell a `BigInt` from a
    // `number` when both hold the same value.
    //
    // The rendering goes through the FULL `value_to_napi` dispatch, so a binding
    // that stopped calling the shared classifier is caught here.
    for vector in JSON_NUMBER_VECTORS {
        let bytes = vector.json_text.as_bytes();
        let produced = match serde_json::from_str::<serde_json::Number>(vector.json_text) {
            Ok(number) => render_through_dispatch(
                &env,
                &Value::Json(Box::new(serde_json::Value::Number(number))),
            ),
            // A committed literal that does not parse is a table defect; report
            // it as a refusal rather than skipping the entry, so it cannot pass
            // silently.
            Err(err) => Err(napi::Error::from_reason(format!(
                "committed literal `{}` did not parse: {err}",
                vector.json_text
            ))),
        };
        reports.push(
            VectorReport::new(
                "json_number",
                0,
                bytes,
                &reported(vector.name, vector.expect, &produced),
            )
            .with_host_kind(vector.host_kind.name()),
        );
    }

    Ok(reports)
}

/// Test-support: convert a JSON number LITERAL to the JS value the production
/// path delivers, through the exact production conversion
/// ([`value_to_napi`] on a `Value::Json`).
///
/// The full chain is the one a real result row takes:
/// `value_to_napi` → `json_to_napi` → `json_number_to_napi` →
/// `cqlite_ffi_common::json_number::classify_json_number`. Nothing is
/// re-implemented here, which is the point: without this surface the production
/// adapter had NO test caller at all, so #3505's observable claim — a JSON
/// integer above `i64::MAX` reaches JS as a `BigInt`, never a rounded `number` —
/// was asserted by nothing (issue #3505 review round 2).
///
/// `text` is a JSON number literal (`"18446744073709551615"`, `"1.5"`), parsed
/// with `serde_json` exactly as the reader would, so the LEXICAL form decides
/// the class. Input that is not a JSON number throws (fail-closed: a typo'd
/// literal must never look like a passing conversion).
///
/// Not part of the stable public API; `lib/index.js` re-exports it as
/// `_jsonNumberFromText`.
#[napi]
pub fn json_number_from_text(env: Env, text: String) -> Result<JsUnknown> {
    let number: serde_json::Number = serde_json::from_str(&text).map_err(|err| {
        napi::Error::from_reason(format!("`{text}` is not a JSON number literal: {err}"))
    })?;
    let ctx = ConvCtx::new(&env);
    value_to_napi(
        &ctx,
        &Value::Json(Box::new(serde_json::Value::Number(number))),
    )
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
