//! The decoded-partition-cache decision procedure, in executable form
//! (issue #2827).
//!
//! The normative statement of this procedure — with its arithmetic, its
//! provenance and its owner-settable threshold — is the committed note at
//! `docs/research/decoded-partition-cache-decision.md`. This module is the same
//! procedure as code, so a closed [`WindowSummary`] can be priced without a human
//! analysis round and so the refusal conditions are testable rather than
//! aspirational.
//!
//! **What it does not do.** It does not deliver a field skew number and it does not
//! deliver the go/no-go for the 64–128 MiB decoded-partition cache. Issue #2827's
//! original AC2 is **not satisfied** by this change — not waived, not deferred:
//! satisfiable on a real keyed workload run with the probe enabled, whose absence
//! (`docs/research/phase2-verify-caching.md:214-216`) is the reason it is unmet here.
//!
//! "Satisfiable" is SCOPED, not universal: this module REFUSES a window it cannot
//! price, and three of those refusals are reachable on a healthy system — a
//! non-census window, a non-zero `unavailable` fraction, and (under #2412's lazy
//! Summary-guided open) a BIG generation whose `Index.db` is not resident. Every one
//! fails SAFE — a refusal is never a false "go" — but the FIRST real window may well
//! be refused, and obtaining a priceable one can take a shorter window, a resident
//! index, or both. Applied to a window this
//! repository generated, the procedure REFUSES by construction
//! ([`WindowSource::Synthetic`]) — a synthetic input is a legitimate oracle for a
//! claim about the instrument and an illegitimate one for a claim about the world.
//!
//! **`H_max` is NOT a ceiling — it is an estimate under a stated ranking
//! heuristic.** It assumes a clairvoyant (Belady) cache, which alone would make it
//! an upper bound; but buckets are ordered by `accesses / bytes` rather than by what
//! a cache actually serves, `(accesses − distinct) / bytes`, so a bucket of large HOT
//! partitions can be outranked by dense small SINGLETONS that serve nothing once
//! admitted. The error from that defect was measured at ≈0.10 maximum observed, and
//! because other mechanisms push independently (the fractional final-bucket take,
//! and the instrument's own coverage limitations) **the total error can bias in
//! EITHER direction**.
//!
//! Tracked as **issue #3340**. **#3340 MUST land before any go/no-go verdict is
//! derived from a real production window**; until then a value near the threshold
//! decides nothing, and neither a high nor a low reading is sound on its own.

use super::{RepeatBucket, WindowSummary};

/// Where the measured window came from. A window this project generated can never
/// be the go/no-go, however green it looks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowSource {
    /// A real workload observed in the field.
    Field,
    /// A load this repository generated (a test, a rig, a loadgen run).
    Synthetic,
}

/// Minimum accesses before a window is treated as a workload at all.
pub const MIN_ACCESSES: u64 = 10_000;

/// The recommended go threshold on `H_max(128 MiB)`.
///
/// **An OWNER-SETTABLE parameter, recorded as such**, not a derived constant. Its
/// arithmetic: a decoded-partition cache targets decode/merge work, and the Arm-1
/// CPU decomposition (#2818) measured k-way merge at 3.2% of on-CPU against LZ4
/// decompress + CRC at ~23%. A cache whose ESTIMATED hit ratio is below 50% on a ≤~3% work
/// share cannot move the end-to-end number by more than ~1.5%, which is under the
/// round harness's noise floor. Naming a default with its arithmetic is what stops
/// the first person to run the procedure from re-litigating it.
pub const RECOMMENDED_GO_THRESHOLD: f64 = 0.50;

/// The decode multiplier `m`: decoded bytes per on-disk byte.
///
/// **This is the one remaining ASSUMPTION in the procedure, not a measurement.**
/// Its provenance is the Phase-0 wire estimate at
/// `docs/research/phase2-verify-caching.md:221-222`, which is explicitly labelled
/// an estimate there. Measuring it is follow-up F3.
pub const ASSUMED_DECODE_MULTIPLIER: f64 = 3.5;

/// Why the procedure declined to price a window. Each is a NO ANSWER, never a
/// default verdict.
#[derive(Clone, Debug, PartialEq)]
pub enum Refusal {
    /// The byte total is incomplete by an unknown amount: some partitions had no
    /// authoritative extent at all — neither an index-recorded size nor a
    /// measurable successor gap. A window whose bytes were MEASURED is complete and
    /// is NOT refused here; incompleteness, not provenance, is what this condition
    /// tests.
    UnpriceableFraction {
        /// Fraction of distinct partitions with `size_source = unavailable`.
        fraction: f64,
        /// How many of them there were.
        partitions: u64,
    },
    /// The recorder hit its sampling floor; the surviving sample is worthless.
    SamplingFloor {
        /// The sampling scale in force at close.
        sample_denominator: u64,
    },
    /// The window is a SAMPLE, not a census, so its per-bucket byte totals are in
    /// the sample domain and cannot be filled against a real cache budget.
    NonCensusSample {
        /// The sampling scale in force at close (`2^k`).
        sample_denominator: u64,
    },
    /// The recorder could not seat some accesses in its table at all, so the
    /// histogram is missing input.
    DroppedAccesses {
        /// How many accesses were lost.
        dropped: u64,
        /// How many the window was asked to record.
        recorded: u64,
    },
    /// The window is not a workload.
    TooFewAccesses {
        /// Accesses attributable to the admitted sample.
        accesses: u64,
        /// The stated minimum.
        minimum: u64,
    },
    /// The window came from load this project generated. Recordable as an
    /// instrument self-check; never citable as the go/no-go.
    SyntheticWorkload,
    /// Every priced partition reported zero bytes, so no budget can be filled.
    NoPricedBytes,
    /// An input was outside the domain the arithmetic is defined on.
    ///
    /// Checked before anything else because the failure is a FALSE GO, not a wrong
    /// number: `decode_multiplier == 0.0` makes the on-disk budget `+inf`, every
    /// bucket then "fits", and the procedure reports a maximal hit ratio clearing any
    /// threshold. A `NaN` multiplier yields a `NaN` ratio that clears nothing but
    /// reports a verdict anyway; a non-positive or `NaN` threshold makes
    /// `clears_threshold` meaningless.
    InvalidInput {
        /// Which input, and what was wrong with it.
        detail: &'static str,
        /// The offending value, for the operator reading the refusal.
        value: f64,
    },
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::UnpriceableFraction {
                fraction,
                partitions,
            } => write!(
                f,
                "REFUSED: {partitions} distinct partitions ({:.1}% of the window) have \
                 size_source=unavailable, so the on-disk byte total is incomplete by an \
                 unknown amount; a hit-ratio computed from a partial byte total would \
                 overstate what fits in the budget",
                fraction * 100.0
            ),
            Refusal::SamplingFloor { sample_denominator } => write!(
                f,
                "REFUSED: the recorder reached its sampling floor (denominator \
                 {sample_denominator}); the surviving sample is statistically worthless"
            ),
            Refusal::NonCensusSample { sample_denominator } => write!(
                f,
                "REFUSED: this window is a 1-in-{sample_denominator} SAMPLE, not a \
                 census. Its per-bucket bytes are sample-domain totals, so filling a \
                 real cache budget against them would price the whole budget against \
                 1/{sample_denominator} of the working set and OVERSTATE what fits. \
                 Remedy: shorten the measurement window so the distinct set fits the \
                 counting table — set CQLITE_PARTITION_ACCESS_WINDOW_SECS (or \
                 CQLITE_PARTITION_ACCESS_WINDOW_ACCESSES) and re-measure"
            ),
            Refusal::DroppedAccesses { dropped, recorded } => write!(
                f,
                "REFUSED: {dropped} of {recorded} accesses could not be seated in the \
                 counting table, so the histogram is missing input. Only NEW keys are \
                 ever dropped, which suppresses the singleton bucket and overstates \
                 concentration — the direction that flatters the cache"
            ),
            Refusal::TooFewAccesses { accesses, minimum } => write!(
                f,
                "REFUSED: {accesses} accesses is below the stated minimum of {minimum}; \
                 this window is not a workload"
            ),
            Refusal::SyntheticWorkload => write!(
                f,
                "REFUSED: the window came from synthetic or self-generated load. Its output \
                 may be recorded as an instrument self-check and may NEVER be cited as the \
                 decoded-partition-cache go/no-go — the answer would be a function of a \
                 distribution we chose"
            ),
            Refusal::NoPricedBytes => write!(
                f,
                "REFUSED: no partition in the window carried authoritative on-disk bytes, \
                 so no budget can be filled"
            ),
            Refusal::InvalidInput { detail, value } => write!(
                f,
                "REFUSED: {detail} (got {value}). This is rejected rather than computed \
                 because the arithmetic would still produce a verdict — a zero decode \
                 multiplier makes the on-disk budget infinite, every bucket fits, and \
                 the result is a maximal hit ratio that clears any threshold: a FALSE GO"
            ),
        }
    }
}

/// A priced window.
///
/// **The name is historical**: it comes from the Belady/clairvoyant framing the
/// procedure was originally written around. It is retained because renaming a public
/// type is not worth a churn here — but [`Self::h_max`] is an ESTIMATE under a stated
/// ranking heuristic, not a ceiling. See that field's documentation and issue #3340.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Ceiling {
    /// The decoded-cache budget priced, in bytes.
    pub budget_bytes: u64,
    /// On-disk bytes that fit in that budget under the assumed decode multiplier.
    pub on_disk_budget_bytes: f64,
    /// Estimated hit ratio under the procedure's stated ranking heuristic.
    ///
    /// **Not a ceiling.** The clairvoyance assumption alone would make it one, but
    /// the bucket ranking uses `accesses / bytes` where a cache serves
    /// `(accesses − distinct) / bytes`, so large hot partitions can be outranked by
    /// dense small singletons and the budget spent on them. Measured error from that
    /// defect ≈0.10 max observed; with the fractional final-bucket take and the
    /// instrument's coverage limitations pushing independently, the total error can
    /// go EITHER way. Issue #3340 must land before this decides anything real.
    pub h_max: f64,
    /// `h_max >= threshold`. Given the above this is an INDICATION, not a verdict:
    /// a `false` is not automatically a sound no-go and a `true` is not a licence to
    /// build — at most a licence to simulate LRU against the captured window.
    pub clears_threshold: bool,
    /// The threshold applied (an owner-settable parameter).
    pub threshold: f64,
}

/// The procedure's answer.
#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    /// The window could not be priced. Names the reason.
    Refused(Refusal),
    /// The window was priced.
    Priced(Ceiling),
}

impl Verdict {
    /// Convenience: was this a refusal?
    pub fn is_refusal(&self) -> bool {
        matches!(self, Verdict::Refused(_))
    }
}

/// Apply the procedure to one closed window at one decoded-cache budget.
///
/// Refusal conditions are checked FIRST and in order; each yields no answer rather
/// than a default verdict. Every one of them is a property this procedure cannot
/// price around: an incomplete byte total, lost input, a sample rather than a
/// census, too little traffic to be a workload, or load we generated ourselves.
pub fn evaluate(
    summary: &WindowSummary,
    source: WindowSource,
    budget_bytes: u64,
    decode_multiplier: f64,
) -> Verdict {
    evaluate_with_threshold(
        summary,
        source,
        budget_bytes,
        decode_multiplier,
        RECOMMENDED_GO_THRESHOLD,
    )
}

/// [`evaluate`] with an explicit go threshold — the threshold is the owner's
/// parameter, so it is an argument rather than a constant baked into the answer.
///
/// `decode_multiplier` must be finite and `> 0`; `threshold` must be finite and in
/// `[0.0, 1.0]`. Anything else is [`Refusal::InvalidInput`] — see that variant for why
/// these are refused rather than computed.
pub fn evaluate_with_threshold(
    summary: &WindowSummary,
    source: WindowSource,
    budget_bytes: u64,
    decode_multiplier: f64,
    threshold: f64,
) -> Verdict {
    // 0. INPUT DOMAIN, before anything else. No in-repo caller can supply a bad value
    //    today (they all pass `ASSUMED_DECODE_MULTIPLIER` / `RECOMMENDED_GO_THRESHOLD`),
    //    but both are `f64` on a public function, and the failure mode is a false GO
    //    rather than a visible error — the exact bias class this instrument exists to
    //    avoid. The day a CLI flag or config plumbs the multiplier through, this is the
    //    guard that stops a typo from reading as a verdict.
    if !decode_multiplier.is_finite() || decode_multiplier <= 0.0 {
        return Verdict::Refused(Refusal::InvalidInput {
            detail: "the decode multiplier must be a finite number greater than zero",
            value: decode_multiplier,
        });
    }
    if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
        return Verdict::Refused(Refusal::InvalidInput {
            detail: "the go threshold must be a finite hit ratio in [0.0, 1.0]",
            value: threshold,
        });
    }

    // 1. An incomplete byte total cannot be priced.
    let unavailable = summary.unavailable_partitions();
    if unavailable > 0 {
        return Verdict::Refused(Refusal::UnpriceableFraction {
            fraction: summary.unavailable_fraction(),
            partitions: unavailable,
        });
    }
    // 2. Input the recorder could not seat at all. Checked early because the loss
    //    is BIASED — see the refusal text — so it invalidates the shape, not just
    //    the totals.
    if summary.dropped_accesses > 0 {
        return Verdict::Refused(Refusal::DroppedAccesses {
            dropped: summary.dropped_accesses,
            recorded: summary.recorded_accesses,
        });
    }
    // 3. A window at the sampling floor is statistically worthless.
    if summary.at_sampling_floor {
        return Verdict::Refused(Refusal::SamplingFloor {
            sample_denominator: summary.sample_denominator,
        });
    }
    // 4. A SAMPLE cannot be filled against a real budget.
    //
    //    The budget arithmetic below compares `C / m` — real bytes — against
    //    `B_b`, which for a downsampled window covers only the admitted 1-in-2^k
    //    share of the distinct partitions. Pricing the full budget against a
    //    fraction of the working set makes everything appear to fit and yields a
    //    FALSE "go". The committed note is explicit that absolute `n_b` and `B_b`
    //    must be scaled by `2^k` before they mean anything.
    //
    //    Refusing rather than scaling, deliberately: scaling by `2^k` gives an
    //    unbiased POINT estimate of the population totals but says nothing about
    //    its variance, and the procedure's output is a go/no-go, not an interval —
    //    so a scaled verdict would look exactly as authoritative as a census one
    //    while resting on an extrapolation this instrument cannot bound. The spec's
    //    only priced scenario is a census, the bucket FRACTIONS a sample does
    //    support are still readable off the emitted series, and a census is cheap
    //    to obtain (shorten the window until the distinct set fits the table).
    if !summary.is_census() {
        return Verdict::Refused(Refusal::NonCensusSample {
            sample_denominator: summary.sample_denominator,
        });
    }
    // 5. A window below the stated minimum is not a workload.
    let total_accesses = summary.total_accesses();
    if total_accesses < MIN_ACCESSES {
        return Verdict::Refused(Refusal::TooFewAccesses {
            accesses: total_accesses,
            minimum: MIN_ACCESSES,
        });
    }
    // 6. Self-generated load is not evidence about the world.
    if source == WindowSource::Synthetic {
        return Verdict::Refused(Refusal::SyntheticWorkload);
    }
    if summary.total_bytes() == 0 {
        return Verdict::Refused(Refusal::NoPricedBytes);
    }

    // Order buckets by ACCESS DENSITY (accesses per on-disk byte), descending: the
    // best bytes to spend the budget on are the ones serving the most accesses.
    let mut ordered: Vec<(RepeatBucket, f64, u64, u64, u64)> = RepeatBucket::ALL
        .iter()
        .filter_map(|b| {
            let s = summary.bucket(*b);
            // PRICED distinct partitions, whichever authoritative provenance they
            // carry: an extent measured as a successor gap is as real as one an
            // index handed over, and skipping it here would silently price the
            // window at zero.
            let priced = s.distinct_priced();
            if s.bytes == 0 || priced == 0 {
                return None;
            }
            let density = s.accesses as f64 / s.bytes as f64;
            Some((*b, density, s.accesses, priced, s.bytes))
        })
        .collect();
    ordered.sort_by(|a, b| {
        // Descending density. `partial_cmp` cannot be `None` here (both operands
        // are finite positives), but fall back to Equal rather than unwrapping.
        b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
    });

    let on_disk_budget = budget_bytes as f64 / decode_multiplier;
    let mut remaining = on_disk_budget;
    let mut served = 0f64;
    for (_, _, accesses, distinct, bytes) in ordered {
        if remaining <= 0.0 {
            break;
        }
        // Every selected partition's FIRST access in the window is compulsory, so a
        // bucket of `n` partitions serving `a` accesses can hit at most `a - n`.
        let hittable = accesses.saturating_sub(distinct) as f64;
        let bytes_f = bytes as f64;
        if bytes_f <= remaining {
            served += hittable;
            remaining -= bytes_f;
        } else {
            // Take the last bucket fractionally, by byte share.
            let f = remaining / bytes_f;
            served += f * hittable;
            remaining = 0.0;
        }
    }

    let h_max = served / total_accesses as f64;
    Verdict::Priced(Ceiling {
        budget_bytes,
        on_disk_budget_bytes: on_disk_budget,
        h_max,
        clears_threshold: h_max >= threshold,
        threshold,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::partition_access::{
        AccessWeight, PartitionAccessRecorder, TableScope, WindowConfig,
    };
    use std::time::Duration;

    fn recorder() -> PartitionAccessRecorder {
        PartitionAccessRecorder::new(WindowConfig {
            duration: Duration::from_secs(86_400),
            max_accesses: u64::MAX,
            ..WindowConfig::default()
        })
    }

    /// Build a census window with `n` partitions each accessed `times` times and
    /// each `bytes` bytes on disk.
    fn window(spec: &[(u64, u32, u64)]) -> WindowSummary {
        let r = recorder();
        let mut key = 0u64;
        for (n, times, bytes) in spec {
            for _ in 0..*n {
                key += 1;
                for _ in 0..*times {
                    r.record(
                        TableScope::new("ks", "t"),
                        &key.to_le_bytes(),
                        AccessWeight::SuccessorGap(*bytes),
                    );
                }
            }
        }
        r.close_window().expect("window recorded accesses")
    }

    #[test]
    fn a_window_with_unpriceable_partitions_is_refused_by_name() {
        let r = recorder();
        for i in 0..12_000u64 {
            r.record(
                TableScope::new("ks", "t"),
                &i.to_le_bytes(),
                AccessWeight::SuccessorGap(1_024),
            );
        }
        r.record(
            TableScope::new("ks", "t"),
            b"bti-resolved",
            AccessWeight::Unavailable,
        );
        let s = r.close_window().expect("accesses recorded");
        let v = evaluate(
            &s,
            WindowSource::Field,
            128 * 1024 * 1024,
            ASSUMED_DECODE_MULTIPLIER,
        );
        match v {
            Verdict::Refused(Refusal::UnpriceableFraction {
                partitions,
                fraction,
            }) => {
                assert_eq!(partitions, 1);
                assert!(fraction > 0.0);
            }
            other => panic!("expected an unpriceable-fraction refusal, got {other:?}"),
        }
    }

    #[test]
    fn an_out_of_domain_input_is_refused_rather_than_priced() {
        // M2: every one of these produces a VERDICT if the arithmetic is allowed to
        // run, and the first is a false GO — the failure class this whole change
        // exists to eliminate.
        let s = window(&[(600, 20, 1_024), (10_000, 1, 1_024)]);
        assert!(
            s.total_accesses() >= MIN_ACCESSES,
            "so only the input is at issue"
        );

        // A zero multiplier makes the on-disk budget +inf: every bucket fits and the
        // ratio is maximal, clearing any threshold.
        for bad_multiplier in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            match evaluate(&s, WindowSource::Field, 128 * 1024 * 1024, bad_multiplier) {
                Verdict::Refused(Refusal::InvalidInput { value, .. }) => {
                    assert!(
                        value.is_nan() == bad_multiplier.is_nan()
                            && (value.is_nan() || value == bad_multiplier),
                        "the refusal must name the offending value"
                    );
                }
                other => panic!("multiplier {bad_multiplier} must be refused: {other:?}"),
            }
        }

        // A threshold outside [0,1] makes `clears_threshold` meaningless: <= 0 always
        // clears, > 1 never can, NaN never compares.
        for bad_threshold in [-0.1, 1.5, f64::NAN, f64::INFINITY] {
            let v = evaluate_with_threshold(
                &s,
                WindowSource::Field,
                128 * 1024 * 1024,
                ASSUMED_DECODE_MULTIPLIER,
                bad_threshold,
            );
            assert!(
                matches!(v, Verdict::Refused(Refusal::InvalidInput { .. })),
                "threshold {bad_threshold} must be refused: {v:?}"
            );
        }

        // The BOUNDARIES are inside the domain and must still price.
        for ok_threshold in [0.0, 1.0, RECOMMENDED_GO_THRESHOLD] {
            let v = evaluate_with_threshold(
                &s,
                WindowSource::Field,
                128 * 1024 * 1024,
                ASSUMED_DECODE_MULTIPLIER,
                ok_threshold,
            );
            assert!(
                matches!(v, Verdict::Priced(_)),
                "threshold {ok_threshold} is in domain and must price: {v:?}"
            );
        }
        // A tiny-but-positive multiplier is in domain — unhelpful, not invalid.
        assert!(matches!(
            evaluate(
                &s,
                WindowSource::Field,
                128 * 1024 * 1024,
                f64::MIN_POSITIVE
            ),
            Verdict::Priced(_)
        ));
    }

    #[test]
    fn a_synthetic_window_is_refused_however_good_it_looks() {
        // 1,000 partitions at 20 accesses each: a very hot set, and still not an
        // answer about the world.
        let s = window(&[(1_000, 20, 1_024)]);
        let v = evaluate(
            &s,
            WindowSource::Synthetic,
            128 * 1024 * 1024,
            ASSUMED_DECODE_MULTIPLIER,
        );
        assert_eq!(v, Verdict::Refused(Refusal::SyntheticWorkload));
    }

    #[test]
    fn a_window_below_the_minimum_access_count_is_refused() {
        let s = window(&[(10, 5, 1_024)]);
        let v = evaluate(
            &s,
            WindowSource::Field,
            128 * 1024 * 1024,
            ASSUMED_DECODE_MULTIPLIER,
        );
        assert_eq!(
            v,
            Verdict::Refused(Refusal::TooFewAccesses {
                accesses: 50,
                minimum: MIN_ACCESSES
            })
        );
    }

    #[test]
    fn a_complete_census_window_is_priced_and_the_estimate_is_hand_checkable() {
        // 500 hot partitions × 20 accesses at 1 KiB, plus 10,000 cold partitions
        // accessed once at 1 KiB.
        //   A = 500*20 + 10_000 = 20_000
        //   hot bucket (17+):  a=10_000, n=500,   B=512_000 B  → density 0.0195
        //   cold bucket (1):   a=10_000, n=10_000,B=10_240_000 → density 0.00098
        // On-disk budget at 128 MiB / 3.5 = 38.34 MiB, which swallows both buckets
        // (10.75 MiB total), so served = (10_000-500) + (10_000-10_000) = 9_500
        // and H_max = 9_500 / 20_000 = 0.475.
        let s = window(&[(500, 20, 1_024), (10_000, 1, 1_024)]);
        assert!(s.is_census());
        assert_eq!(s.total_accesses(), 20_000);
        let v = evaluate(
            &s,
            WindowSource::Field,
            128 * 1024 * 1024,
            ASSUMED_DECODE_MULTIPLIER,
        );
        match v {
            Verdict::Priced(c) => {
                assert!(
                    (c.h_max - 0.475).abs() < 1e-9,
                    "expected the hand-computed 0.475, got {}",
                    c.h_max
                );
                assert!(!c.clears_threshold, "0.475 is below the 0.50 threshold");
            }
            other => panic!("expected a priced verdict, got {other:?}"),
        }
    }

    #[test]
    fn a_tight_budget_takes_the_densest_bucket_first() {
        // Same window, but a budget that only fits part of the data: the hot bucket
        // (highest accesses-per-byte) is taken first, so the estimate stays high.
        let s = window(&[(500, 20, 1_024), (10_000, 1, 1_024)]);
        // 512_000 on-disk bytes × 3.5 = 1_792_000 decoded bytes fits the hot bucket
        // exactly and nothing else.
        let v = evaluate(
            &s,
            WindowSource::Field,
            1_792_000,
            ASSUMED_DECODE_MULTIPLIER,
        );
        match v {
            Verdict::Priced(c) => {
                assert!(
                    (c.h_max - 0.475).abs() < 1e-6,
                    "the cold bucket contributes no hits, so the estimate is unchanged: {}",
                    c.h_max
                );
            }
            other => panic!("expected a priced verdict, got {other:?}"),
        }
    }
}
