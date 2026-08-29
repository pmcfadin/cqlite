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
        expected_sha256: None,
        outcome: "ok",
        actual: actual.to_string(),
        rendered: Some(actual.to_string()),
    };
    assert!(check_outcome(&value("1.23")).is_ok());
    assert!(check_outcome(&value("1.230")).is_err());

    let refusal = |outcome: &'static str, actual: &str| VectorOutcome {
        name: "t",
        kind: "error",
        expected: "canonical text".to_string(),
        expected_sha256: None,
        outcome,
        actual: actual.to_string(),
        rendered: None,
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
        expected_sha256: None,
        outcome: "err",
        actual: "1.23".to_string(),
        rendered: None,
    })
    .is_err());
    // An unrecognised kind is a failure, never a silent pass.
    assert!(check_outcome(&VectorOutcome {
        name: "t",
        kind: "who-knows",
        expected: String::new(),
        expected_sha256: None,
        outcome: "ok",
        actual: String::new(),
        rendered: Some(String::new()),
    })
    .is_err());
    // `ok` with no rendering carries nothing exact to compare, so it fails
    // rather than passing on the digest alone.
    assert!(check_outcome(&VectorOutcome {
        name: "t",
        kind: "value",
        expected: "1.23".to_string(),
        expected_sha256: None,
        outcome: "ok",
        actual: "1.23".to_string(),
        rendered: None,
    })
    .is_err());
}

/// Every committed `Expect::Digested` hash is the SHA-256 of the FULL rendering
/// the shared implementation produces — the check that binds the DIGITS, not
/// just their count.
///
/// The committed hex came from CPython (see `tables.rs`); this asserts the Rust
/// rendering hashes to the same value, so the two independent derivations agree.
#[test]
fn every_digested_expectation_pins_the_full_rendering_by_sha256() {
    let mut checked = 0usize;
    for vector in DECIMAL_VECTORS {
        let Expect::Digested {
            digest: committed,
            sha256,
        } = vector.expect
        else {
            continue;
        };
        // A malformed hex string would make the comparison meaningless.
        assert_eq!(
            sha256.len(),
            64,
            "`{}`: sha256 must be 64 hex chars",
            vector.name
        );
        assert!(
            sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "`{}`: sha256 must be lower-case hex",
            vector.name
        );
        let rendered = decimal_to_string(vector.scale, &vector.unscaled.bytes())
            .unwrap_or_else(|e| panic!("`{}`: expected a rendering, got {e}", vector.name));
        assert_eq!(digest(&rendered), committed, "`{}`: digest", vector.name);
        assert_eq!(sha256_hex(&rendered), sha256, "`{}`: sha256", vector.name);
        // A digested entry only exists because the literal form is unreadable.
        assert!(
            rendered.len() > DIGEST_RUN_THRESHOLD,
            "`{}`: a short rendering must be committed literally",
            vector.name
        );
        checked += 1;
    }
    assert!(
        checked >= 3,
        "the long-rendering boundary entries must be digested; checked {checked}"
    );
}

/// A literal `Expect::Value` must never hold a COLLAPSED digest: that is exactly
/// the state this pairing exists to prevent — an expectation that compares digit
/// counts with no exact oracle beside it.
#[test]
fn no_literal_expectation_is_a_collapsed_digest() {
    let mut texts: Vec<(&str, &str)> = DECIMAL_VECTORS
        .iter()
        .filter_map(|v| match v.expect {
            Expect::Value(text) => Some((v.name, text)),
            _ => None,
        })
        .collect();
    texts.extend(VARINT_VECTORS.iter().filter_map(|v| match v.expect {
        Expect::Value(text) => Some((v.name, text)),
        _ => None,
    }));
    texts.extend(INET_VECTORS.iter().filter_map(|v| match v.expect {
        Expect::Value(text) => Some((v.name, text)),
        _ => None,
    }));
    assert!(!texts.is_empty());
    for (name, text) in texts {
        assert_eq!(
            digest(text),
            text,
            "`{name}`: a collapsed digest must use `Expect::Digested` so a SHA-256 \
             of the full rendering pins its digits"
        );
    }
}

/// THE defect this pairing closes, held as a permanent assertion: a rendering
/// with the SAME digit count and the SAME surrounding form but DIFFERENT digits
/// satisfies the digest comparison and must still FAIL.
#[test]
fn a_same_length_different_digit_rendering_fails_the_exact_check() {
    let vector = DECIMAL_VECTORS
        .iter()
        .find(|v| v.name == "decimal/large-well-formed-2000-bytes-scale-3")
        .expect("the 2000-byte convergence vector must exist");
    let real = decimal_to_string(vector.scale, &vector.unscaled.bytes())
        .expect("the 2000-byte magnitude renders");

    // Perturb ONE digit, keeping the length, the digit count and the exponent
    // identical — the case a digit-count comparison cannot see.
    let mut perturbed: Vec<char> = real.chars().collect();
    let index = perturbed
        .iter()
        .position(|c| c.is_ascii_digit())
        .expect("the rendering has digits");
    perturbed[index] = if perturbed[index] == '9' { '8' } else { '9' };
    let perturbed: String = perturbed.into_iter().collect();
    assert_ne!(perturbed, real);
    assert_eq!(perturbed.len(), real.len());

    let reported = vector_outcome(vector.name, vector.expect, Ok(perturbed.as_str()));
    // The digest half PASSES: this is what the pre-fix comparison compared.
    assert_eq!(reported.actual, reported.expected);
    // The exact half FAILS, and says why.
    let why = check_outcome(&reported).expect_err("a wrong-digit rendering must fail");
    assert!(why.contains("SHA-256"), "{why}");
}
