//! Type-spelling normalization for the Parquet↔JSONL parity harness (#1490).
//!
//! Issue #1490 asks for the same kind of normalization the Python binding parity
//! harness already applies (`bindings/python/tests/test_cli_parity.py`):
//! erase differences that are purely how a VALUE IS SPELLED, and nothing else.
//!
//! # `duration` is the one CQL type whose two sides spell the same value differently
//!
//! Cassandra's `sstabledump` prints a duration with `Duration.toString()`, which
//! decomposes into `y / mo / d / h / m / s / ms / us / ns` — `"50m33s"`. CQLite's
//! Parquet export writes the `ValueFormatter` spelling, which prints only
//! months/days/nanos — `"3033000000000ns"`. Both denote 3 033 000 000 000 ns, so
//! comparing the STRINGS would report a difference where there is none, while
//! comparing the parsed (months, days, nanos) triple compares the value.
//!
//! Both spellings are parsed by ONE parser over the union grammar, and the
//! canonical form is the triple. That is a normalization, not a tolerance: two
//! genuinely different durations still differ, and a malformed spelling is an
//! ERROR rather than a string that quietly compares unequal-but-unexplained.
//!
//! (The CQL `duration` type cannot appear in a primary key, so this never
//! affects the harness's sort keys.)

#![allow(dead_code)]

use super::canonical_jsonl::CanonicalValue;
use super::cql_type::CqlTypeSpec;

/// Months per year and the nanosecond scales `Duration.toString()` uses.
const MONTHS_PER_YEAR: i128 = 12;
const DAYS_PER_WEEK: i128 = 7;
const NANOS_PER_MICRO: i128 = 1_000;
const NANOS_PER_MILLI: i128 = 1_000_000;
const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_MINUTE: i128 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: i128 = 60 * NANOS_PER_MINUTE;

/// Rewrite a value into its spelling-independent canonical form, guided by the
/// DECLARED CQL type. Recurses into collections so a `list<duration>` is handled
/// like a scalar `duration`.
pub fn normalize_spelling(
    v: CanonicalValue,
    spec: &CqlTypeSpec,
    ctx: &str,
) -> Result<CanonicalValue, String> {
    match (v, spec) {
        (CanonicalValue::Text(s), CqlTypeSpec::Scalar(name)) if name == "duration" => {
            let (months, days, nanos) = parse_duration(&s, ctx)?;
            Ok(CanonicalValue::Tuple(vec![
                ("months".to_string(), CanonicalValue::Int(months)),
                ("days".to_string(), CanonicalValue::Int(days)),
                ("nanos".to_string(), CanonicalValue::Int(nanos)),
            ]))
        }
        (CanonicalValue::List(xs), CqlTypeSpec::Seq { elem, .. }) => Ok(CanonicalValue::List(
            xs.into_iter()
                .map(|x| normalize_spelling(x, elem, ctx))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        (CanonicalValue::Set(xs), CqlTypeSpec::Seq { elem, .. }) => Ok(CanonicalValue::Set(
            xs.into_iter()
                .map(|x| normalize_spelling(x, elem, ctx))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        (CanonicalValue::Map(kvs), CqlTypeSpec::Map { key, value }) => Ok(CanonicalValue::Map(
            kvs.into_iter()
                .map(|(k, v)| {
                    Ok((
                        normalize_spelling(k, key, ctx)?,
                        normalize_spelling(v, value, ctx)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        (other, _) => Ok(other),
    }
}

/// Parse either duration spelling into `(months, days, nanos)`.
///
/// Grammar (the union of both writers, plus the CQL literal's `w`):
/// `[-] (<int><unit>)+` with `unit ∈ {y, mo, w, d, h, m, s, ms, us, µs, ns}`.
/// A component may carry its own `-`.
///
/// # The sign rule, and why it needs one
///
/// The two writers spell a NEGATIVE duration differently: Cassandra prints one
/// leading `-` and then absolute components (`-1mo2d`), while CQLite's
/// `ValueFormatter` prints each component with its own sign (`-1mo-2d`). So a
/// leading `-` followed by any POSITIVE component can only be Cassandra's global
/// form, and the global sign is applied to the rest; when every component
/// carries its own sign, the components are authoritative. (Every duration in
/// the corpus is positive, so this is defensive, not load-bearing.)
pub fn parse_duration(s: &str, ctx: &str) -> Result<(i128, i128, i128), String> {
    let tokens = tokenize(s, ctx)?;
    if tokens.is_empty() {
        return Err(format!("{ctx}: duration '{s}' has no components"));
    }
    let global_negative = tokens[0].0 < 0 && tokens.iter().skip(1).any(|(n, _)| *n > 0);

    let mut months: i128 = 0;
    let mut days: i128 = 0;
    let mut nanos: i128 = 0;
    // Checked throughout: the component counts come from a FILE, so an absurd
    // literal must produce a named error rather than an arithmetic panic (or, in
    // a release build, a wrapped value that compares equal to something).
    for (i, (raw, unit)) in tokens.iter().enumerate() {
        let n = if global_negative && i > 0 {
            raw.checked_neg()
                .ok_or_else(|| format!("{ctx}: duration '{s}' component {raw} cannot be negated"))?
        } else {
            *raw
        };
        let (target, scale): (&mut i128, i128) = match unit.as_str() {
            "y" => (&mut months, MONTHS_PER_YEAR),
            "mo" => (&mut months, 1),
            "w" => (&mut days, DAYS_PER_WEEK),
            "d" => (&mut days, 1),
            "h" => (&mut nanos, NANOS_PER_HOUR),
            "m" => (&mut nanos, NANOS_PER_MINUTE),
            "s" => (&mut nanos, NANOS_PER_SECOND),
            "ms" => (&mut nanos, NANOS_PER_MILLI),
            "us" | "µs" => (&mut nanos, NANOS_PER_MICRO),
            "ns" => (&mut nanos, 1),
            other => {
                return Err(format!(
                    "{ctx}: duration '{s}' carries unit '{other}', which is not in the \
                     y/mo/w/d/h/m/s/ms/us/ns grammar either writer emits"
                ))
            }
        };
        let scaled = n.checked_mul(scale).ok_or_else(|| {
            format!("{ctx}: duration '{s}' component {n}{unit} overflows when scaled")
        })?;
        *target = target.checked_add(scaled).ok_or_else(|| {
            format!("{ctx}: duration '{s}' overflows while accumulating {n}{unit}")
        })?;
    }
    Ok((months, days, nanos))
}

/// Split a duration into `(signed count, unit)` pairs, erroring on anything the
/// grammar does not allow (a trailing count with no unit, a unit with no count,
/// stray characters).
fn tokenize(s: &str, ctx: &str) -> Result<Vec<(i128, String)>, String> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    while chars.peek().is_some() {
        let mut num = String::new();
        if chars.peek() == Some(&'-') {
            num.push('-');
            chars.next();
        }
        while let Some(c) = chars.peek() {
            if c.is_ascii_digit() {
                num.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        let mut unit = String::new();
        while let Some(c) = chars.peek() {
            if c.is_ascii_alphabetic() || *c == 'µ' {
                unit.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if num.is_empty() || num == "-" || unit.is_empty() {
            return Err(format!(
                "{ctx}: duration '{s}' is not a sequence of <int><unit> components"
            ));
        }
        let count: i128 = num.parse().map_err(|e| {
            format!("{ctx}: duration '{s}' component '{num}' is not an integer: {e}")
        })?;
        out.push((count, unit.to_ascii_lowercase()));
    }
    Ok(out)
}
