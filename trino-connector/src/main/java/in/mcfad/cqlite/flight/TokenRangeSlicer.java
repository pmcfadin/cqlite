package in.mcfad.cqlite.flight;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Deterministically sub-splits a Cassandra read-replica token range {@code (start, end]} into
 * K equal-token-span slices (issue #2680) so per-owner assigned weight converges to ~1/N of the
 * total regardless of how unequal the per-range weights are (weight-balanced split→pod
 * assignment). Slicing happens at ONE seam in {@link CqliteFlightSplitManager} before any
 * consumer, so scan splits, aggregate fan-out, the snapshot host chooser (#2227), and plan-time
 * pruning (#2679) all operate on slices.
 *
 * <p><b>Overflow-safe ring arithmetic.</b> Murmur3 tokens are signed 64-bit on a ring that wraps
 * at {@link Long#MAX_VALUE}→{@link Long#MIN_VALUE}; a range's token span can reach {@code 2^64 - 1}
 * (which overflows a signed {@code long}), so all span/boundary math is done in {@link BigInteger}
 * on an unsigned ring position axis {@code pos(t) = t - Long.MIN_VALUE} (order-preserving:
 * {@code MIN→0}, {@code MAX→2^64-1}). This avoids the signed-overflow blocker class entirely.
 *
 * <p><b>Conventions preserved.</b> Each slice keeps the half-open {@code (start, end]} convention
 * and its own wraparound flag ({@code sliceStart >= sliceEnd}, matching
 * {@link Murmur3Token#tokenInRange} and {@code CqliteFlightSplitManager.validateRingCoverage}).
 * The K slices cover the parent exactly — no gaps, no overlaps — and the last slice ends at the
 * parent's {@code end} exactly (no drift). A slice is NEVER an empty {@code (x, x]} range (which
 * the wraparound convention would misread as the full ring, #2228): a degenerate span narrower
 * than K tokens emits fewer, non-empty slices. With K=1 the single emitted slice is byte-identical
 * to the parent range.
 */
public final class TokenRangeSlicer {
    /** 2^64 — the ring circumference on the unsigned position axis. */
    private static final BigInteger RING = BigInteger.ONE.shiftLeft(64);
    private static final BigInteger MIN = BigInteger.valueOf(Long.MIN_VALUE);

    private TokenRangeSlicer() {}

    /**
     * One slice of a parent range: half-open {@code (start, end]} with {@code wraparound}
     * set when {@code start >= end} (the ring-crossing slice), per the existing convention.
     */
    public record Slice(long start, long end, boolean wraparound) {}

    /**
     * Expand {@code (start, end]} into {@code subSplitsPerRange} equal-token-span slices.
     * Deterministic (a pure function of {@code start}, {@code end}, {@code subSplitsPerRange}).
     *
     * @param start                exclusive start token (parent range)
     * @param end                  inclusive end token (parent range); {@code start == end} is the
     *                             full ring (#2228), spanning all {@code 2^64} tokens
     * @param subSplitsPerRange    K, the number of slices to target (>= 1); fewer are emitted for
     *                             a span narrower than K
     */
    public static List<Slice> slice(long start, long end, int subSplitsPerRange) {
        if (subSplitsPerRange < 1) {
            throw new IllegalArgumentException(
                    "subSplitsPerRange must be >= 1, got " + subSplitsPerRange);
        }
        BigInteger startPos = pos(start);
        // Unsigned wrapping span (end - start) mod 2^64; a full-ring range (start == end) spans the
        // whole ring (2^64), never 0 — an empty (x,x] slice would misread as the full ring (#2228).
        BigInteger span = span(start, end);
        // Degenerate span (< K tokens): emit exactly `span` unit-wide slices, never an empty one.
        int k = span.compareTo(BigInteger.valueOf(subSplitsPerRange)) < 0
                ? span.intValueExact()
                : subSplitsPerRange;
        BigInteger kBig = BigInteger.valueOf(k);
        List<Slice> slices = new ArrayList<>(k);
        BigInteger prevBoundary = startPos;
        for (int i = 1; i <= k; i++) {
            // boundary_i = startPos + floor(span * i / k) (mod 2^64); boundary_k == pos(end) exactly.
            BigInteger boundary = startPos
                    .add(span.multiply(BigInteger.valueOf(i)).divide(kBig))
                    .mod(RING);
            long sliceStart = token(prevBoundary);
            long sliceEnd = token(boundary);
            slices.add(new Slice(sliceStart, sliceEnd, sliceStart >= sliceEnd));
            prevBoundary = boundary;
        }
        return slices;
    }

    /**
     * The unsigned wrapping token span of a range {@code (start, end]} as a {@link BigInteger}
     * (issue #2680): {@code (end - start) mod 2^64}, and the full ring ({@code 2^64}) when
     * {@code start == end}. Overflow-safe (a range span can reach {@code 2^64 - 1}). Used to size
     * each split's {@link CqliteFlightSplit#getSplitWeight()} proportional to its token span.
     */
    public static BigInteger span(long start, long end) {
        BigInteger s = pos(end).subtract(pos(start)).mod(RING);
        return s.signum() == 0 ? RING : s;
    }

    /** Map a signed token to its unsigned ring position in {@code [0, 2^64)} (order-preserving). */
    private static BigInteger pos(long token) {
        return BigInteger.valueOf(token).subtract(MIN);
    }

    /** Inverse of {@link #pos}: a ring position in {@code [0, 2^64)} back to its signed token. */
    private static long token(BigInteger position) {
        return position.add(MIN).longValueExact();
    }
}
