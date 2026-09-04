//! Direct unit coverage for the EXACT decimal arithmetic behind the float
//! tie-break declared gap (issue #3777).
//!
//! Why direct, and not only through the gap predicate: the gap exercises this
//! arithmetic on essentially ONE value, so a parse, scale-alignment or
//! cross-multiplication defect would be invisible there and would silently make
//! the gap over- or under-apply — which is the exact finding class this gap has
//! now cost three review rounds. A child module of the code it tests, so the
//! private significand/scale are readable and the assertions can be stated over
//! the parse RESULT rather than over a re-derived value.
//!
//! Every table below carries a case FLOOR: a span-replacing edit that deletes
//! cases otherwise leaves a green run over a shrunken set (#3544).

use super::*;

/// The parse RESULT, as `(significand, scale)` — the value being
/// `significand * 10^-scale`. Stated as strings so a table entry reads as the
/// arithmetic it claims.
fn parsed(text: &str) -> Option<(String, u32)> {
    ExactDecimal::parse(text).map(|d| (d.digits.to_string(), d.scale))
}

/// Spellings this module reads EXACTLY, with the significand/scale each denotes.
///
/// The negative-scale entries are the normalisation: an exponent that moves the
/// point RIGHT is absorbed into the significand, so `scale` is always a plain
/// power of ten in the denominator and two decimals are always comparable by
/// lifting the smaller scale.
const ACCEPTED: &[(&str, &str, u32)] = &[
    ("0", "0", 0),
    ("42", "42", 0),
    ("-7", "-7", 0),
    ("007", "7", 0),
    ("1.50", "150", 2),
    ("0.000", "0", 3),
    // `-0.0` is the roborev counterexample's golden spelling: an exact zero, whose
    // sign no INTEGER significand can carry — which is precisely why `-0.0` and
    // `-0` compare EQUAL below and so cannot be a tie.
    ("-0.0", "0", 1),
    ("36.601562", "36601562", 6),
    (".5", "5", 1),
    ("1.", "1", 0),
    ("1e5", "100000", 0),
    ("1E5", "100000", 0),
    ("1e+5", "100000", 0),
    // The smallest positive f32 subnormal's magnitude: the `10^-45` this module
    // exists to handle without a machine integer.
    ("1e-45", "1", 45),
    ("1.5e-3", "15", 4),
    ("-2.5e2", "-250", 0),
];

#[test]
fn parse_reads_an_accepted_spelling_exactly() {
    assert!(
        ACCEPTED.len() >= 16,
        "case floor: at least sixteen accepted spellings must be exercised"
    );
    for (text, digits, scale) in ACCEPTED {
        assert_eq!(
            parsed(text),
            Some(((*digits).to_string(), *scale)),
            "{text} must be read as {digits}e-{scale}"
        );
    }

    // The digit BOUND is inclusive at the limit and refused one past it, so the
    // bound is a stated rule rather than an accident of some other check.
    let at_limit = "1".repeat(MAX_MANTISSA_DIGITS as usize);
    assert_eq!(
        parsed(&at_limit),
        Some((at_limit.clone(), 0)),
        "a mantissa of exactly MAX_MANTISSA_DIGITS digits is decidable"
    );
    assert_eq!(
        parsed(&"1".repeat(MAX_MANTISSA_DIGITS as usize + 1)),
        None,
        "one digit past the bound is refused"
    );
}

/// Spellings OUTSIDE the grammar. Each must be REFUSED — `None`, "I cannot decide
/// this" — never guessed at, because a guess here is a suppression.
const REFUSED: &[&str] = &[
    "",
    "-",
    ".",
    "+1",  // no leading `+`: neither formatter emits one
    "--1", // a doubled sign
    "1-2",
    "abc",
    "NaN",
    "Infinity",
    "1.2.3",
    "1 ", // trailing space
    " 1",
    "1_000", // Rust's own literal separator is not decimal text
    "0x10",
    "1e", // an exponent with no digits
    "1e+",
    "1e1e1",
    "1e401", // past MAX_ABS_EXPONENT
    "1e-401",
    // The i64 BOUNDARY, from both directions. `1e-9223372036854775808` is a real
    // `i64` exponent whose `abs()` does not exist, so it used to PANIC in a debug
    // build where the module promises a refusal (roborev job 96); its positive
    // twin and one step past the type are refused by the `i64` parse itself, and
    // all three must read the same from outside.
    "1e-9223372036854775808",
    "1e9223372036854775807",
    "1e-9223372036854775809",
];

#[test]
fn parse_refuses_a_spelling_outside_the_grammar() {
    assert!(
        REFUSED.len() >= 22,
        "case floor: at least twenty-two refusals must be exercised"
    );
    for text in REFUSED {
        assert_eq!(parsed(text), None, "{text} must be refused, not guessed at");
    }

    // The exponent BOUND, at the limit and one past it, for the same reason as the
    // digit bound above.
    assert!(
        parsed(&format!("1e{MAX_ABS_EXPONENT}")).is_some(),
        "an exponent at the bound is decidable"
    );
    assert_eq!(
        parsed(&format!("1e{}", MAX_ABS_EXPONENT + 1)),
        None,
        "one past the exponent bound is refused"
    );
}

/// GENUINE exact ties: `(value, serde_json's spelling, Rust `Display`'s spelling)`,
/// where the f32 is exactly the mean of the two decimals.
///
/// Derived, not invented, and at four magnitudes so the proof is not pinned to one
/// exponent: each value is `odd / 2^k` whose exact expansion ends in a `5` one digit
/// past the shortest round-tripping length, which is what makes both 8-digit
/// neighbours round-trip and the two formatters break the tie in opposite
/// directions. Written as fractions of exactly-representable operands (a decimal
/// literal trips `clippy::excessive_precision`, and its suggested truncation is one
/// of the two spellings UNDER TEST). The self-checks in the test body are what make
/// these formatter ties rather than merely arithmetic ones.
const TIES: &[(f32, &str, &str)] = &[
    (4685.0 / 128.0, "36.601562", "36.601563"), // the MEASURED sensor_data cell
    (-4685.0 / 128.0, "-36.601562", "-36.601563"), // the sign path
    (1.0 / 4096.0, "0.00024414062", "0.00024414063"), // 2^-12
    (2049.0 / 128.0, "16.007812", "16.007813"),
];

#[test]
fn an_exact_tie_is_proven_at_every_magnitude_and_sign() {
    assert!(
        TIES.len() >= 4,
        "case floor: at least four exact ties, spanning magnitudes and both signs"
    );
    for (value, tie_to_even, away_from_zero) in TIES {
        // Self-checked case data: each spelling is its own formatter's output for
        // THIS f32, and both denote it, so what the arithmetic below adds is the
        // TIE and nothing else.
        assert_eq!(
            &serde_json::to_string(value).expect("serialize f32"),
            tie_to_even,
            "the golden side must be serde_json's spelling"
        );
        assert_eq!(
            &value.to_string(),
            away_from_zero,
            "the CLI side must be Display's spelling"
        );
        assert_eq!(
            tie_to_even.parse::<f32>().expect("f32").to_bits(),
            value.to_bits(),
            "{tie_to_even} must denote the value under test"
        );

        assert!(
            is_exact_tie(tie_to_even, away_from_zero, *value),
            "{value} is exactly the mean of {tie_to_even} and {away_from_zero}"
        );
        // The relation is symmetric: which side the walk reads first is not part of
        // the claim.
        assert!(
            is_exact_tie(away_from_zero, tie_to_even, *value),
            "the midpoint proof must not depend on argument order"
        );
        // And it is about THIS f32: one ulp away is not the midpoint of the pair.
        let next = f32::from_bits(value.to_bits() + 1);
        assert!(
            !is_exact_tie(tie_to_even, away_from_zero, next),
            "the pair is the tie of {value}, not of its neighbour"
        );
    }
}

/// Pairs that are NOT an exact tie, with WHY. Every one of these used to be
/// suppressed by the formatter-pair-only predicate (`-0.0` is roborev's own
/// counterexample) or would be suppressed by an f32-equality one.
const NOT_TIES: &[(f32, &str, &str, &str)] = &[
    (
        -0.0,
        "-0.0",
        "-0",
        "one exact value, two spellings: nothing is approximated",
    ),
    (1.0, "1.0", "1", "an integral value spelled two ways"),
    (1.5, "1.50", "1.5", "the same value at two different SCALES"),
    (
        4685.0 / 128.0,
        "36.601562",
        "36.601564",
        "straddles the value but is not equidistant from it",
    ),
    (
        4685.0 / 128.0,
        "36.601561",
        "36.601562",
        "both decimals lie BELOW the value",
    ),
    (
        4685.0 / 128.0,
        "36.601563",
        "36.601565",
        "both decimals lie ABOVE the value",
    ),
];

#[test]
fn a_pair_that_is_not_an_exact_tie_is_not_a_midpoint() {
    assert!(
        NOT_TIES.len() >= 6,
        "case floor: at least six non-tie pairs must be exercised"
    );
    for (value, a, b, why) in NOT_TIES {
        assert!(
            !is_exact_tie(a, b, *value),
            "{a} vs {b} about {value} is not a tie ({why})"
        );
    }
}

/// Two spellings at DIFFERENT scales must be ALIGNED before anything is compared —
/// the likeliest defect in this code, and one no other test would see, because both
/// sides of the measured cell happen to carry six fraction digits.
///
/// Every pair below denotes the SAME two decimals as the measured tie, spelled with
/// a different scale on one side (an exponent, a padded fraction, an integer
/// significand), so each must still prove the tie.
#[test]
fn two_scales_are_aligned_before_the_comparison() {
    let value: f32 = 4685.0 / 128.0;
    let respellings: &[(&str, &str)] = &[
        ("3.6601562e1", "36.601563"),
        ("36601562e-6", "36.601563"),
        ("36.601562", "36.6015630"),
        ("36.6015620", "3.6601563e1"),
        ("0.36601562e2", "366015630e-7"),
    ];
    assert!(
        respellings.len() >= 5,
        "case floor: at least five scale respellings must be exercised"
    );
    for (a, b) in respellings {
        assert!(
            is_exact_tie(a, b, value),
            "{a} vs {b} denotes the measured tie however it is spelled"
        );
    }

    // The alignment is not a licence to equate different values: a scale
    // respelling of a NON-tie stays a non-tie.
    assert!(
        !is_exact_tie("3.6601562e1", "36.6015640", value),
        "aligning scales must not make an unequal pair equidistant"
    );
}

/// The FAIL-CLOSED direction, which is the property that matters: an input this
/// module cannot decide is "not this gap", never a suppression.
///
/// Asserted through [`is_exact_tie`] — the one entry point the gap predicate itself
/// calls — so this is the caller's own behaviour and not a re-implementation of it.
/// The two halves are separate facts: `parse` REFUSES the text (`None`), and the
/// decision built on it is `false`.
#[test]
fn an_undecidable_input_is_never_a_tie() {
    let value: f32 = 4685.0 / 128.0;
    let genuine = "36.601563";
    for text in REFUSED {
        assert_eq!(parsed(text), None, "precondition: {text} is undecidable");
        assert!(
            !is_exact_tie(text, genuine, value),
            "an undecidable golden side ({text}) is not this gap"
        );
        assert!(
            !is_exact_tie(genuine, text, value),
            "an undecidable CLI side ({text}) is not this gap"
        );
    }

    // `is_exact_midpoint_of` is deliberately THREE-valued, and the caller collapses
    // anything other than `Some(true)` to "not this gap" — so a hypothetical
    // undecidable comparison can never become a suppression. Stated over the two
    // reachable answers, since the bounds in `parse` are what make `None` from the
    // arithmetic unreachable at all.
    let tie_to_even = ExactDecimal::parse("36.601562").expect("decidable");
    let away = ExactDecimal::parse(genuine).expect("decidable");
    assert_eq!(tie_to_even.is_exact_midpoint_of(&away, value), Some(true));
    assert_eq!(
        tie_to_even.is_exact_midpoint_of(&away, 1.0),
        Some(false),
        "a decided NO is Some(false), distinct from an undecidable None"
    );

    // And the arithmetic's own `None` is REACHABLE, not decorative: a scale past
    // `MAX_SCALE` is refused rather than sizing a `10^k` the module never bounded.
    // Nothing `parse` accepts can reach it — hence the hand-built operand, which
    // only this child module can construct — but the guard is what makes the
    // boundedness local to the arithmetic.
    let past_the_bound = ExactDecimal {
        digits: BigInt::from(1u8),
        scale: MAX_SCALE + 1,
    };
    assert_eq!(
        past_the_bound.is_exact_midpoint_of(&away, value),
        None,
        "a scale past MAX_SCALE is undecidable, not an unbounded allocation"
    );
    assert_eq!(
        away.is_exact_midpoint_of(&past_the_bound, value),
        None,
        "the bound holds whichever operand carries the scale"
    );
}

/// The WORST case `parse` can build is inside the stated bound and still decides —
/// so the bound is a real ceiling on the arithmetic, not a limit that refuses legal
/// input. A 64-digit mantissa at the most negative accepted exponent is the largest
/// scale reachable from text; it must parse, and comparing it must terminate with an
/// answer rather than a panic or a `10^huge`.
#[test]
fn the_largest_scale_reachable_from_text_still_decides() {
    let fraction = "1".repeat(MAX_MANTISSA_DIGITS as usize - 1);
    let text = format!("0.{fraction}e-{MAX_ABS_EXPONENT}");
    let long = ExactDecimal::parse(&text).expect("the worst case is still decidable");
    assert_eq!(
        long.scale,
        MAX_MANTISSA_DIGITS - 1 + MAX_ABS_EXPONENT,
        "the worst reachable scale"
    );
    assert!(long.scale <= MAX_SCALE, "and it is inside the stated bound");

    let other = ExactDecimal::parse("36.601563").expect("decidable");
    assert_eq!(
        long.is_exact_midpoint_of(&other, 4685.0 / 128.0),
        Some(false),
        "a decided answer at the worst reachable scale"
    );
}

/// The dyadic decomposition every comparison rests on: `v == m * 2^e`, exactly,
/// including the subnormal branch (biased exponent 0, no implicit leading bit),
/// which no tie case reaches and which a `10^-45` spelling makes reachable.
#[test]
fn an_f32_decomposes_into_an_exact_dyadic_rational() {
    let cases: &[(f32, &str, i32)] = &[
        (1.0, "8388608", -23), // 2^23 * 2^-23
        (-1.0, "-8388608", -23),
        (0.0, "0", -149),
        (4685.0 / 128.0, "9594880", -18),
        (f32::from_bits(1), "1", -149), // the smallest positive subnormal
        (f32::MIN_POSITIVE, "8388608", -149), // the smallest NORMAL
    ];
    assert!(
        cases.len() >= 6,
        "case floor: at least six decompositions, including both subnormal edges"
    );
    for (value, m, e) in cases {
        let (digits, exponent) = f32_as_dyadic(*value);
        assert_eq!(
            (digits.to_string(), exponent),
            ((*m).to_string(), *e),
            "{value} must decompose exactly"
        );
    }
}

/// And the decomposition really DENOTES the value: `m * 2^e` re-expressed as an
/// exact decimal (`m * 5^-e * 10^e`, for the negative `e` every finite f32 here
/// has) must equal the value's own exact decimal literal — compared as integers
/// after lifting to a common scale, so the check itself carries no rounding.
///
/// This is what makes the table above a MEASUREMENT rather than a restatement of
/// the code: a sign or bias error would agree with `f32_as_dyadic` and disagree
/// with the literal.
#[test]
fn the_dyadic_decomposition_denotes_the_value_it_came_from() {
    // Exactly-representable values with SHORT exact expansions, so the literal can
    // be written in full: an f32 whose expansion needs more digits than
    // `MAX_MANTISSA_DIGITS` (every subnormal) is out of the literal's reach, which
    // is a property of the TEXT, not of the decomposition.
    let cases: &[(f32, &str)] = &[
        (1.0, "1"),
        (-1.0, "-1"),
        (0.5, "0.5"),
        (-0.0, "0"),
        (4685.0 / 128.0, "36.6015625"),
        (2049.0 / 128.0, "16.0078125"),
        (1.0 / 4096.0, "0.000244140625"),
    ];
    assert!(
        cases.len() >= 7,
        "case floor: at least seven values reconstructed from their decomposition"
    );
    for (value, literal) in cases {
        let (m, e) = f32_as_dyadic(*value);
        assert!(
            e < 0,
            "every finite f32 in these cases has a negative exponent"
        );
        let scale = u32::try_from(-e).expect("in range");
        let from_bits = ExactDecimal {
            digits: m * BigInt::from(5u8).pow(scale),
            scale,
        };
        let from_text = ExactDecimal::parse(literal).expect("an exact literal");
        let k = from_bits.scale.max(from_text.scale);
        let lift = |d: &ExactDecimal| &d.digits * BigInt::from(10u8).pow(k - d.scale);
        assert_eq!(
            lift(&from_bits),
            lift(&from_text),
            "{value}'s decomposition must denote {literal} exactly"
        );
    }
}
