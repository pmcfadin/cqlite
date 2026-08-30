//! The OpenTelemetry option-name list both bindings accept (issue #1452).

/// The recognised `otel_config` / `otel` option names, in **snake_case**.
///
/// snake_case is the spelling of Python's dict keys and of both bindings' Rust
/// field names. Node's JS-visible property names are the camelCase
/// `#[napi(js_name)]` forms of the same fields (`serviceName`, `samplingRatio`,
/// `timeoutMs`), so a JS caller writes camelCase while this list stays the one
/// canonical set.
///
/// # What is shared, and what deliberately is not
///
/// Only the **name list** is shared. The validation *mechanism* differs by FFI
/// shape and is left alone on purpose: Python receives an untyped `dict` and
/// raises `ValueError` for an unrecognised key, while napi deserializes a typed
/// object and silently drops unknown JS properties. See the crate docs.
///
/// Protocol parsing (`OtelProtocol::parse`) and the sampling-ratio clamp are
/// already shared — both bindings delegate them to
/// `cqlite_core::observability`, and this change does not touch that.
///
/// Each binding has an **enforcing consumer**: Python's allowlist reads this
/// list directly, and the Node binding has a test asserting that its
/// `OtelOptions` field names and this list are the same set in both directions,
/// so a field added to one and not the other fails a test instead of shipping an
/// asymmetry.
pub const KNOWN_OTEL_KEYS: &[&str] = &[
    "enabled",
    "endpoint",
    "protocol",
    "service_name",
    "service_version",
    "sampling_ratio",
    "timeout_ms",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_snake_case_unique_and_non_empty() {
        assert!(!KNOWN_OTEL_KEYS.is_empty());
        let unique: std::collections::BTreeSet<&&str> = KNOWN_OTEL_KEYS.iter().collect();
        assert_eq!(unique.len(), KNOWN_OTEL_KEYS.len(), "keys must be unique");
        for key in KNOWN_OTEL_KEYS {
            assert!(
                !key.is_empty()
                    && key
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()),
                "`{key}` is not snake_case"
            );
        }
    }
}
