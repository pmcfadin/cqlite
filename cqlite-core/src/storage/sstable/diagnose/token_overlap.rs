//! Generation-overlap-per-token-range (issue #4204, `design.md` D4).
//!
//! No existing CQLite primitive answers "how many generations' token spans cover
//! each of `K` equal-width token buckets" (confirmed at task 0 — the existing
//! `write_engine` "overlap" hits are the #921/#935 PURGE-SAFETY bound, a
//! different question). Built fresh, metadata-only, from each generation's own
//! `[first_token, last_token]` span (`SSTableReader::endpoint_tokens`, populated
//! from `Summary.db` at `open()` time — no Data.db read).
//!
//! `K` is fixed at [`TOKEN_OVERLAP_BUCKETS`] (16) for this slice, matching D4's
//! "not operator-configurable" — a fixed `K` keeps the JSON report shape stable.

/// Fixed bucket count for the table-wide token-overlap histogram (D4). NOT
/// operator-configurable in this slice.
pub(crate) const TOKEN_OVERLAP_BUCKETS: usize = 16;

/// Compute, for each of `buckets` equal-width token buckets covering the UNION
/// of every span in `spans`, how many spans intersect that bucket (D4 steps 1-3).
///
/// `spans` are `[first_token, last_token]` (inclusive, `first <= last`) pairs.
/// Returns a vector of length `buckets`; an EMPTY `spans` returns all-zero
/// buckets (nothing to cover — never a divide-by-zero or a fabricated count).
///
/// Bucket `i` covers the half-open range `[lo + i*width, lo + (i+1)*width)`,
/// except the LAST bucket, which is closed at the global maximum (`hi`) so the
/// single point `hi` itself always lands in a bucket. When every span is a
/// single point (`lo == hi`, or `buckets == 0`), everything degenerates to
/// bucket 0.
pub(crate) fn compute_token_overlap(spans: &[(i64, i64)], buckets: usize) -> Vec<u64> {
    if buckets == 0 {
        return Vec::new();
    }
    let mut counts = vec![0u64; buckets];
    if spans.is_empty() {
        return counts;
    }

    // Step 1: union of every span (i128 to stay clear of i64 overflow when
    // computing width later).
    let lo = spans.iter().map(|&(f, _)| f as i128).min().unwrap();
    let hi = spans.iter().map(|&(_, l)| l as i128).max().unwrap();

    if hi <= lo {
        // Degenerate: a single point (or an inverted span, which we treat
        // conservatively as a point at `lo`) — every span intersects bucket 0.
        counts[0] = spans.len() as u64;
        return counts;
    }

    // Step 2: partition [lo, hi] into `buckets` equal-width buckets.
    let total_width = hi - lo;
    let bucket_width = total_width / buckets as i128;
    // When `buckets` does not evenly divide `total_width`, `bucket_width` can be
    // 0 for a narrow union with a large `buckets` count; guard so every bucket
    // still gets a well-defined (possibly empty) range rather than dividing by
    // zero below.
    let bucket_width = bucket_width.max(1);

    let bucket_start = |i: usize| -> i128 { lo + bucket_width * i as i128 };
    let bucket_end = |i: usize| -> i128 {
        if i + 1 == buckets {
            hi
        } else {
            bucket_start(i + 1) - 1
        }
    };

    // Step 3: for each bucket, count intersecting spans.
    for &(first, last) in spans {
        let (first, last) = if first as i128 <= last as i128 {
            (first as i128, last as i128)
        } else {
            // Defensive: an inverted span (should not occur for an authoritative
            // reader-derived endpoint pair) is treated as its own single point.
            (first as i128, first as i128)
        };
        for (i, count) in counts.iter_mut().enumerate() {
            let (bs, be) = (bucket_start(i), bucket_end(i));
            if first <= be && last >= bs {
                *count += 1;
            }
        }
    }

    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_spans_all_zero() {
        assert_eq!(compute_token_overlap(&[], 4), vec![0, 0, 0, 0]);
    }

    #[test]
    fn single_point_lands_in_bucket_zero() {
        assert_eq!(compute_token_overlap(&[(5, 5)], 4), vec![1, 0, 0, 0]);
    }

    #[test]
    fn one_span_covering_everything_hits_every_bucket() {
        assert_eq!(compute_token_overlap(&[(0, 100)], 4), vec![1, 1, 1, 1]);
    }

    #[test]
    fn disjoint_spans_hit_disjoint_buckets() {
        // [0,100] split into 4 buckets: [0,24],[25,49],[50,74],[75,100].
        let spans = vec![(0i64, 24i64), (75i64, 100i64)];
        assert_eq!(compute_token_overlap(&spans, 4), vec![1, 0, 0, 1]);
    }

    #[test]
    fn overlapping_spans_accumulate_counts() {
        // Both spans cover the whole [0,100] range -> every bucket sees 2.
        let spans = vec![(0i64, 100i64), (10i64, 90i64)];
        assert_eq!(compute_token_overlap(&spans, 4), vec![2, 2, 2, 2]);
    }

    #[test]
    fn full_i64_range_does_not_overflow() {
        let spans = vec![(i64::MIN, i64::MAX)];
        let counts = compute_token_overlap(&spans, TOKEN_OVERLAP_BUCKETS);
        assert_eq!(counts.len(), TOKEN_OVERLAP_BUCKETS);
        assert!(counts.iter().all(|&c| c == 1));
    }
}
