//! The half-open token bound pushed into the Summary-guided walk (issue #2413),
//! split out of `summary_scan/mod.rs` so the membership rule and its pins sit in
//! one small file (campsite rule, epic #1116).

/// Half-open `(start_excl, end_incl]` token bound pushed into the per-SSTable walk
/// (issue #2413 Option A). Mirrors the flight `TokenFilter` half-open semantics
/// exactly (including the `start == end` FULL-ring convention, #2228); the flight
/// crate constructs one of these from its `TokenFilter` and a grid test pins that
/// the two agree, so the membership rule lives in ONE place.
///
/// Wrapping is DERIVED from the two endpoints ([`Self::is_wraparound`]), never
/// carried as a third field: a caller-supplied flag can disagree with the bounds
/// it describes, and Cassandra has no such flag to disagree with (issue #3634).
#[derive(Debug, Clone, Copy)]
pub struct ScanTokenBound {
    /// Exclusive lower bound.
    pub start_excl: i64,
    /// Inclusive upper bound.
    pub end_incl: i64,
}

impl ScanTokenBound {
    /// Whether this is a ring-wraparound segment, derived exactly as
    /// `Range.isWrapAround(left, right)` does at `cassandra-5.0.8`
    /// (`src/java/org/apache/cassandra/dht/Range.java`):
    /// `left.compareTo(right) >= 0`.
    ///
    /// The `>=` is load-bearing, and is why the FULL-ring convention (#2228)
    /// needs no special case: equal endpoints wrap, and the wraparound arm of
    /// [`Self::contains`] is then `t > s || t <= s`, which every token satisfies.
    pub fn is_wraparound(&self) -> bool {
        self.start_excl >= self.end_incl
    }

    /// Whether `token` is inside this half-open `(start, end]` range.
    ///
    /// Transcribed from `Range.contains(left, right, point)` at `cassandra-5.0.8`.
    pub fn contains(&self, token: i64) -> bool {
        if self.is_wraparound() {
            token > self.start_excl || token <= self.end_incl
        } else {
            token > self.start_excl && token <= self.end_incl
        }
    }

    /// Whether every remaining (token-ascending) partition is guaranteed to be
    /// ABOVE this range, so a forward walk can stop. Only sound for a
    /// non-wraparound range (a wraparound range has in-range tokens at both ends
    /// of the ring, so a forward walk cannot early-stop) — which subsumes the
    /// full ring, since equal endpoints wrap.
    pub(super) fn can_stop_past(&self, token: i64) -> bool {
        !self.is_wraparound() && token > self.end_incl
    }
}

#[cfg(test)]
mod tests {
    use super::ScanTokenBound;

    #[test]
    fn contains_non_wraparound_half_open() {
        let b = ScanTokenBound {
            start_excl: 10,
            end_incl: 20,
        };
        assert!(!b.contains(10), "start is exclusive");
        assert!(b.contains(11));
        assert!(b.contains(20), "end is inclusive");
        assert!(!b.contains(21));
    }

    #[test]
    fn contains_equal_endpoints_is_full_ring() {
        let b = ScanTokenBound {
            start_excl: 5,
            end_incl: 5,
        };
        assert!(
            b.is_wraparound(),
            "equal endpoints wrap (Range.isWrapAround `>=`)"
        );
        for t in [i64::MIN, -1, 0, 5, 6, i64::MAX] {
            assert!(b.contains(t), "equal endpoints cover every token (#2228)");
        }
    }

    #[test]
    fn contains_wraparound() {
        let b = ScanTokenBound {
            start_excl: 100,
            end_incl: -100,
        };
        assert!(b.is_wraparound(), "start > end wraps");
        assert!(b.contains(101), "above start is in range");
        assert!(b.contains(-100), "at/below end is in range");
        assert!(!b.contains(0), "the interior gap is excluded");
    }

    #[test]
    fn can_stop_past_only_non_wraparound_above_end() {
        let fwd = ScanTokenBound {
            start_excl: 10,
            end_incl: 20,
        };
        assert!(!fwd.can_stop_past(20), "at end still in range");
        assert!(fwd.can_stop_past(21), "past end can stop");
        let wrap = ScanTokenBound {
            start_excl: 100,
            end_incl: -100,
        };
        assert!(
            !wrap.can_stop_past(i64::MAX),
            "a wraparound range never early-stops a forward walk"
        );
    }

    /// #3634: the `start != end` guard the flag-carrying form needed is SUBSUMED
    /// by the derived predicate, so the full ring must still never early-stop a
    /// forward walk — an early stop there would silently truncate the scan.
    #[test]
    fn a_full_ring_never_early_stops() {
        for s in [i64::MIN, -1, 0, 1, i64::MAX] {
            let full = ScanTokenBound {
                start_excl: s,
                end_incl: s,
            };
            for t in [
                i64::MIN,
                s.saturating_sub(1),
                s,
                s.saturating_add(1),
                i64::MAX,
            ] {
                assert!(
                    !full.can_stop_past(t),
                    "a full ring ({s}, {s}] admits tokens above {t}, so the walk \
                     must not stop"
                );
            }
        }
    }

    /// The derived form must agree with the flag-carrying form wherever the flag
    /// was CONSISTENT with its endpoints — i.e. every shape a live caller built.
    #[test]
    fn can_stop_past_matches_the_consistent_flag_form() {
        for (start_excl, end_incl) in [(10, 20), (100, -100), (5, 5), (i64::MIN, i64::MAX)] {
            let b = ScanTokenBound {
                start_excl,
                end_incl,
            };
            for t in [i64::MIN, -101, -100, 0, 20, 21, 100, i64::MAX] {
                let consistent_flag = start_excl > end_incl;
                let previous = !consistent_flag && start_excl != end_incl && t > end_incl;
                assert_eq!(
                    b.can_stop_past(t),
                    previous,
                    "({start_excl}, {end_incl}] at {t}"
                );
            }
        }
    }
}
