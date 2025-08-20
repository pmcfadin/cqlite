/// Floating point equality assertion with configurable epsilon
pub fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() <= eps.max(1e-12)
}

/// Assert that all bytes in a buffer have been consumed during parsing
pub fn assert_fully_consumed(parsed_remaining: &[u8]) {
    assert!(
        parsed_remaining.is_empty(),
        "trailing bytes remain after parse: {}",
        parsed_remaining.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_eq() {
        assert!(approx_eq(1.0, 1.0, 1e-6));
        assert!(approx_eq(1.0, 1.000001, 1e-5));
        assert!(!approx_eq(1.0, 1.1, 1e-6));
    }

    #[test]
    fn test_assert_fully_consumed_empty() {
        assert_fully_consumed(&[]);
    }

    #[test]
    #[should_panic(expected = "trailing bytes remain after parse: 3")]
    fn test_assert_fully_consumed_with_remaining() {
        assert_fully_consumed(&[1, 2, 3]);
    }
}