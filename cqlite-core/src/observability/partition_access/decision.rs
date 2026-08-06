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
//! satisfiable on the first real keyed workload run with the probe enabled, and
//! blocked only by the absence of such a workload
//! (`docs/research/phase2-verify-caching.md:214-216`). Applied to a window this
//! repository generated, the procedure REFUSES by construction
//! ([`WindowSource::Synthetic`]) — a synthetic input is a legitimate oracle for a
//! claim about the instrument and an illegitimate one for a claim about the world.
//!
//! **The bound is a CEILING.** `H_max` assumes a clairvoyant (Belady) cache, so a
//! LOW value is a sound no-go while a HIGH value is necessary but not sufficient
//! for a "go" — it is a licence to simulate LRU against the captured window, not a
//! licence to build.

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
/// decompress + CRC at ~23%. A cache whose CEILING is below 50% on a ≤~3% work
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
    /// The byte total is incomplete by an unknown amount: some partitions were
    /// resolved by a path that records no size.
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
        }
    }
}

/// A priced window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Ceiling {
    /// The decoded-cache budget priced, in bytes.
    pub budget_bytes: u64,
    /// On-disk bytes that fit in that budget under the assumed decode multiplier.
    pub on_disk_budget_bytes: f64,
    /// The clairvoyant (Belady) hit-ratio CEILING. A real LRU cache does strictly
    /// worse.
    pub h_max: f64,
    /// `h_max >= threshold`. A `false` here is a sound no-go; a `true` is a licence
    /// to simulate LRU against the captured window, not a licence to build.
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
/// than a default verdict.
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
pub fn evaluate_with_threshold(
    summary: &WindowSummary,
    source: WindowSource,
    budget_bytes: u64,
    decode_multiplier: f64,
    threshold: f64,
) -> Verdict {
    // 1. An incomplete byte total cannot be priced.
    let unavailable = summary.unavailable_partitions();
    if unavailable > 0 {
        return Verdict::Refused(Refusal::UnpriceableFraction {
            fraction: summary.unavailable_fraction(),
            partitions: unavailable,
        });
    }
    // 2. A window at the sampling floor is statistically worthless.
    if summary.at_sampling_floor {
        return Verdict::Refused(Refusal::SamplingFloor {
            sample_denominator: summary.sample_denominator,
        });
    }
    // 3. A window below the stated minimum is not a workload.
    let total_accesses = summary.total_accesses();
    if total_accesses < MIN_ACCESSES {
        return Verdict::Refused(Refusal::TooFewAccesses {
            accesses: total_accesses,
            minimum: MIN_ACCESSES,
        });
    }
    // 4. Self-generated load is not evidence about the world.
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
            if s.bytes == 0 || s.distinct_index == 0 {
                return None;
            }
            let density = s.accesses as f64 / s.bytes as f64;
            Some((*b, density, s.accesses, s.distinct_index, s.bytes))
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
        AccessWeight, PartitionAccessRecorder, WindowConfig,
    };
    use std::time::Duration;

    fn recorder() -> PartitionAccessRecorder {
        PartitionAccessRecorder::new(WindowConfig {
            duration: Duration::from_secs(86_400),
            max_accesses: u64::MAX,
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
                    r.record(&key.to_le_bytes(), AccessWeight::Index(*bytes));
                }
            }
        }
        r.close_window().expect("window recorded accesses")
    }

    #[test]
    fn a_window_with_unpriceable_partitions_is_refused_by_name() {
        let r = recorder();
        for i in 0..12_000u64 {
            r.record(&i.to_le_bytes(), AccessWeight::Index(1_024));
        }
        r.record(b"bti-resolved", AccessWeight::Unavailable);
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
    fn a_complete_census_window_is_priced_and_the_ceiling_is_hand_checkable() {
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
        // (highest accesses-per-byte) is taken first, so the ceiling stays high.
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
                    "the cold bucket contributes no hits, so the ceiling is unchanged: {}",
                    c.h_max
                );
            }
            other => panic!("expected a priced verdict, got {other:?}"),
        }
    }
}
