//! The committed vectors are checked against the shared implementations HERE, so
//! a wrong expectation is caught in this crate rather than only in a binding
//! suite (issue #1452).

use super::*;
use crate::decimal::{decimal_to_string, DecimalError, DECIMAL_MAX_UNSCALED_BYTES};
use crate::inet::inet_bytes_to_string;
use crate::varint::varint_to_bigint;

#[test]
fn every_decimal_vector_matches_the_shared_implementation() {
    assert!(!DECIMAL_VECTORS.is_empty());
    for vector in DECIMAL_VECTORS {
        let unscaled = vector.unscaled.bytes();
        let rendered = decimal_to_string(vector.scale, &unscaled);
        let message = rendered.as_ref().err().map(|e| e.to_string());
        let produced = match (&rendered, &message) {
            (Ok(text), _) => Ok(text.as_str()),
            (Err(_), Some(text)) => Err(text.as_str()),
            (Err(_), None) => unreachable!("an Err always has a Display message"),
        };
        let reported = vector_outcome(vector.name, vector.expect, produced);
        if let Err(why) = check_outcome(&reported) {
            panic!("{why}");
        }
    }
}

#[test]
fn every_varint_vector_matches_the_shared_implementation() {
    assert!(!VARINT_VECTORS.is_empty());
    for vector in VARINT_VECTORS {
        let rendered = varint_to_bigint(&vector.bytes.bytes()).to_string();
        let reported = vector_outcome(vector.name, vector.expect, Ok(rendered.as_str()));
        if let Err(why) = check_outcome(&reported) {
            panic!("{why}");
        }
    }
}

#[test]
fn every_inet_vector_matches_the_shared_implementation() {
    assert!(!INET_VECTORS.is_empty());
    for vector in INET_VECTORS {
        let rendered = inet_bytes_to_string(&vector.bytes.bytes());
        let message = rendered.as_ref().err().map(|e| e.to_string());
        let produced = match (&rendered, &message) {
            (Ok(text), _) => Ok(text.as_str()),
            (Err(_), Some(text)) => Err(text.as_str()),
            (Err(_), None) => unreachable!("an Err always has a Display message"),
        };
        let reported = vector_outcome(vector.name, vector.expect, produced);
        if let Err(why) = check_outcome(&reported) {
            panic!("{why}");
        }
    }
}

/// Names are the identifier both suites report on failure, so a duplicate would
/// make a failure ambiguous.
#[test]
fn vector_names_are_unique_across_all_tables() {
    let mut names: Vec<&str> = DECIMAL_VECTORS.iter().map(|v| v.name).collect();
    names.extend(VARINT_VECTORS.iter().map(|v| v.name));
    names.extend(INET_VECTORS.iter().map(|v| v.name));
    let unique: std::collections::BTreeSet<&&str> = names.iter().collect();
    assert_eq!(unique.len(), names.len(), "vector names must be unique");
}

/// Each table must carry at least one expected-error entry, so the refusal path
/// is cross-binding-covered and not only the happy path.
#[test]
fn decimal_and_inet_tables_cover_the_refusal_path() {
    assert!(DECIMAL_VECTORS
        .iter()
        .any(|v| matches!(v.expect, Expect::Error(_))));
    assert!(INET_VECTORS
        .iter()
        .any(|v| matches!(v.expect, Expect::Error(_))));
}

/// The committed refusal message and `DecimalError`'s `Display` are the same
/// text — the vector table does not get to hold a second spelling.
#[test]
fn committed_refusal_message_equals_the_error_display() {
    let expected = DECIMAL_VECTORS
        .iter()
        .find(|v| v.name == "decimal/past-refusal-ceiling")
        .map(|v| v.expect.text())
        .expect("the past-ceiling vector must exist");
    assert_eq!(
        expected,
        DecimalError::UnscaledTooLarge {
            scale: 3,
            unscaled_len: DECIMAL_MAX_UNSCALED_BYTES + 1,
            max_unscaled_bytes: DECIMAL_MAX_UNSCALED_BYTES,
        }
        .to_string()
    );
}

#[test]
fn digest_leaves_short_renderings_alone() {
    for rendering in ["0", "-1.23", "123e2", "1e-2147483647", "::1", "192.168.1.1"] {
        assert_eq!(digest(rendering), rendering);
    }
    // A run of exactly the threshold stays literal; one past it collapses.
    let at = "1".repeat(DIGEST_RUN_THRESHOLD);
    assert_eq!(digest(&at), at);
    let past = "1".repeat(DIGEST_RUN_THRESHOLD + 1);
    assert_eq!(digest(&past), format!("{{{}}}", DIGEST_RUN_THRESHOLD + 1));
}

#[test]
fn digest_collapses_each_long_run_separately_and_keeps_the_form() {
    let long = "7".repeat(2464);
    assert_eq!(digest(&format!("{long}.83")), "{2464}.83");
    assert_eq!(digest(&format!("-{long}e-3")), "-{2464}e-3");
    assert_eq!(digest(&format!("{long}.{long}")), "{2464}.{2464}");
}

/// The two comparison rules a binding suite applies, held as assertions so a
/// change to them is visible here too.
#[test]
fn check_outcome_enforces_exactness_for_values_and_containment_for_errors() {
    let value = |actual: &str| VectorOutcome {
        name: "t",
        kind: "value",
        expected: "1.23".to_string(),
        outcome: "ok",
        actual: actual.to_string(),
    };
    assert!(check_outcome(&value("1.23")).is_ok());
    assert!(check_outcome(&value("1.230")).is_err());

    let refusal = |outcome: &'static str, actual: &str| VectorOutcome {
        name: "t",
        kind: "error",
        expected: "canonical text".to_string(),
        outcome,
        actual: actual.to_string(),
    };
    // The binding's envelope may wrap the canonical text; it may not replace it.
    assert!(check_outcome(&refusal("err", "ParseError: canonical text")).is_ok());
    assert!(check_outcome(&refusal("err", "something else")).is_err());
    // A refusal expected but a rendering produced (and vice versa) both fail.
    assert!(check_outcome(&refusal("ok", "canonical text")).is_err());
    assert!(check_outcome(&VectorOutcome {
        name: "t",
        kind: "value",
        expected: "1.23".to_string(),
        outcome: "err",
        actual: "1.23".to_string(),
    })
    .is_err());
    // An unrecognised kind is a failure, never a silent pass.
    assert!(check_outcome(&VectorOutcome {
        name: "t",
        kind: "who-knows",
        expected: String::new(),
        outcome: "ok",
        actual: String::new(),
    })
    .is_err());
}
