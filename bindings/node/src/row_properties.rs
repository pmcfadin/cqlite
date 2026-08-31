//! Own-property definition for result rows — the M2 mechanism (issue #3630).
//!
//! # What this exists to do
//!
//! [`row_to_object`](crate::value::row_to_object) must write **user-controlled**
//! column names onto a row object. An ordinary property assignment is a
//! JavaScript `[[Set]]`, which consults the prototype chain, so a column named
//! `__proto__` reaches `Object.prototype`'s inherited accessor instead of
//! becoming a property — silently losing the column, or replacing the object's
//! prototype when the value is null. `napi_define_properties` performs
//! `[[DefineOwnProperty]]`, which never consults the prototype, so the channel is
//! REMOVED rather than filtered. Full rationale, including why a row keeps
//! `Object.prototype` while #3504's UDT field bag does not, is on
//! `row_to_object`.
//!
//! # Why this is raw `napi_sys` and not napi-rs's safe `Property`
//!
//! **A measurement, not a preference.** napi-rs's `Property` stores its name as a
//! `CString` and `Property::raw()` sets the descriptor's `name` field to null,
//! using `utf8name` instead — so the pre-interned `JsString` handles that #1446
//! builds ONCE PER RESULT cannot be reused, and every row re-allocates a
//! `CString` per column and makes V8 re-intern every name. That is exactly the
//! `O(rows × columns)` cost #1446 exists to remove.
//!
//! Measured with the harness and the decision rule pinned in
//! `openspec/changes/node-binding-drops-column/design.md` D1b (1 warmup + 7
//! timed pairs, alternating arms, dev profile, baseline relative half-range
//! 0.69% so the run is valid by that rule):
//!
//! | arm | median rows/s |
//! |---|---|
//! | pre-fix `[[Set]]` baseline | 12624.2 |
//! | safe `Property` (M1) | 11143.4 — **11.73% regression** |
//!
//! The rule's threshold is 5%, so M1 was refused and this mechanism was
//! required. The `unsafe` here was authorized by the delivery lead CONDITIONAL on
//! that measurement selecting it.
//!
//! A release-profile A/B pointed the same way (18.78%) but **is not cited as
//! evidence**: its baseline relative half-range was 14.02% under host
//! contention, failing D1b's own validity precondition, and "points the same
//! way" is the reasoning that precondition exists to refuse.
//!
//! # Safety
//!
//! One `unsafe` block, calling `napi_define_properties`. Its preconditions and
//! why each holds are documented at the call site. Every `napi_value` in the
//! descriptor array is a handle created in — and borrowed for the lifetime of —
//! the current `Env` scope, and the array is passed by pointer to a call that
//! does not retain it.

use napi::{sys, Env, JsObject, JsString, JsUnknown, NapiRaw, Result};

/// The attribute set of an ordinary ASSIGNED property, so a DEFINED column is
/// observationally identical to an assigned one for every read a caller performs.
///
/// Spelled out deliberately. `sys::PropertyAttributes::default` is **0** —
/// non-writable, NON-ENUMERABLE, non-configurable — and napi-rs additionally has
/// a *different* `PropertyAttributes::Default` bitflag constant with that same
/// zero value, distinct from its `Default::default()` trait impl which is the
/// three-way OR. Getting that wrong once already cost a build+test round here:
/// every column arrived with `{writable: false, enumerable: false, configurable:
/// false}`, so the VALUE was present but `Object.keys(row)` was EMPTY — one
/// silent-wrong-output bug substituted for the one being fixed.
const DATA_PROPERTY: sys::napi_property_attributes = sys::PropertyAttributes::writable
    | sys::PropertyAttributes::enumerable
    | sys::PropertyAttributes::configurable;

/// Accumulates `(name, value)` handle pairs for one row, then defines them all in
/// a single `napi_define_properties` call.
///
/// Descriptors are applied in PUSH ORDER, and V8 preserves insertion order for
/// own string-keyed properties, which is what keeps #1446's contract that
/// `Object.keys(row)` equals `columns.map(c => c.name)` — declared columns in
/// authoritative SELECT order, then name-sorted extras.
pub struct RowProperties {
    descriptors: Vec<sys::napi_property_descriptor>,
}

impl RowProperties {
    /// A builder sized for `capacity` columns — one allocation per row, replacing
    /// M1's one `CString` allocation per column per row.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            descriptors: Vec::with_capacity(capacity),
        }
    }

    /// Queue one column, taking its name as an ALREADY-INTERNED [`JsString`].
    ///
    /// This is the whole point of the mechanism: the descriptor's `name` field
    /// takes a `napi_value`, so the once-per-result interned handle is reused
    /// directly and no per-row string work happens at all. `utf8name` is left
    /// null — Node-API requires exactly one of the two, and supplying `name`
    /// means the length-delimited handle is used.
    ///
    /// A side effect worth recording because it RETIRES a requirement M1 needed:
    /// M1 built each name as a `CString`, which fails on an interior NUL byte, so
    /// it needed an explicit refusal path to avoid silently dropping such a
    /// column. Here the name is an already-created length-delimited `JsString`
    /// (`intern_column_keys` uses `Env::create_string`, i.e.
    /// `napi_create_string_utf8` WITH a length), so there is no unrepresentable
    /// name and therefore no refusal to implement. That is an absence of a
    /// failure mode, not a tested behaviour — no test exercises an interior-NUL
    /// column name, because no fixture can produce one.
    pub fn push(&mut self, name: &JsString, value: &JsUnknown) {
        self.descriptors.push(sys::napi_property_descriptor {
            // SAFETY: `NapiRaw::raw` IS an unsafe fn (an earlier version of this
            // comment claimed otherwise, which was simply false — it sits inside
            // an `unsafe` block). Its obligation is that the extracted
            // `napi_value` must not be used after its handle scope ends. It is
            // not: the raw handles are stored only in `self.descriptors` and
            // consumed by `define_on`, which the caller invokes in the same
            // enclosing scope.
            //
            // Storing the RAW handle rather than the wrapper is sound because a
            // `napi_value` is owned by the napi HANDLE SCOPE, not by the Rust
            // wrapper: dropping a `JsString`/`JsUnknown` does not release or
            // invalidate the underlying value, so the descriptor array does not
            // borrow anything it could outlive.
            utf8name: std::ptr::null(),
            name: unsafe { name.raw() },
            method: None,
            getter: None,
            setter: None,
            value: unsafe { value.raw() },
            attributes: DATA_PROPERTY,
            data: std::ptr::null_mut(),
        });
    }

    /// Define every queued column on `obj` as an own enumerable data property.
    ///
    /// A no-op for an empty builder, so a row with no emitted column is left as
    /// the plain object it already is.
    pub fn define_on(self, env: &Env, obj: &mut JsObject) -> Result<()> {
        if self.descriptors.is_empty() {
            return Ok(());
        }
        // SAFETY: `napi_define_properties`' preconditions, each discharged:
        //  * `env` is the live `napi_env` of the current call — it comes from the
        //    `Env` this function was handed, not stored or forged.
        //  * `obj` is a `JsObject` handle from the same scope, so it is a live
        //    object value.
        //  * the pointer/length pair describes a contiguous array owned by
        //    `self.descriptors`, which is alive for the whole call.
        //  * every `napi_value` inside was produced in this scope by
        //    `push`, and Node-API does not retain the array or the descriptors
        //    past the call, so no handle escapes its scope.
        //  * exactly one of `utf8name`/`name` is set per descriptor (`name`).
        // The returned status is converted by `check_status!`, so a failure is a
        // napi `Error` rather than a silently ignored code.
        napi::check_status!(unsafe {
            sys::napi_define_properties(
                env.raw(),
                obj.raw(),
                self.descriptors.len(),
                self.descriptors.as_ptr(),
            )
        })
    }
}
