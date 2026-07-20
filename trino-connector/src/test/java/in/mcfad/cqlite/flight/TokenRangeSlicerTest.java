package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.TokenRangeSlicer.Slice;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit coverage for {@link TokenRangeSlicer} (issue #2680): exact coverage (no gap/overlap), the
 * wraparound seam, boundary-token ownership under {@link Murmur3Token#tokenInRange}, the K=1
 * identity, and degenerate (span &lt; K) ranges. All assertions use overflow-safe {@link BigInteger}
 * span math so the whole ring ({@code 2^64 - 1} span) is exercised without signed overflow.
 */
class TokenRangeSlicerTest {

    private static final BigInteger RING = BigInteger.ONE.shiftLeft(64);
    private static final BigInteger MIN = BigInteger.valueOf(Long.MIN_VALUE);

    /** Unsigned ring position of a signed token, matching the slicer's internal axis. */
    private static BigInteger pos(long token) {
        return BigInteger.valueOf(token).subtract(MIN);
    }

    /** Unsigned wrapping span of (start, end], with start == end == full ring (2^64). */
    private static BigInteger span(long start, long end) {
        BigInteger s = pos(end).subtract(pos(start)).mod(RING);
        return s.signum() == 0 ? RING : s;
    }

    /**
     * Assert the slices tile (start, end] exactly: contiguous (each slice's end == the next's
     * start), the first starts at the parent start, the last ends at the parent end, and the
     * summed spans equal the parent span (no gap, no overlap).
     */
    private static void assertExactCoverage(long start, long end, List<Slice> slices) {
        assertFalse(slices.isEmpty(), "at least one slice");
        assertEquals(start, slices.get(0).start(), "first slice starts at parent start");
        assertEquals(end, slices.get(slices.size() - 1).end(), "last slice ends at parent end");
        BigInteger total = BigInteger.ZERO;
        for (int i = 0; i < slices.size(); i++) {
            Slice sl = slices.get(i);
            total = total.add(span(sl.start(), sl.end()));
            if (i > 0) {
                assertEquals(slices.get(i - 1).end(), sl.start(),
                        "slice " + i + " starts exactly where slice " + (i - 1) + " ended (no gap/overlap)");
            }
            // No empty (x, x] slice — that would misread as the full ring (#2228).
            assertFalse(sl.start() == sl.end() && slices.size() > 1,
                    "no empty (x,x] slice when more than one slice is emitted");
        }
        assertEquals(span(start, end), total, "summed slice spans equal the parent span");
    }

    @Test
    void k4CoversNormalRangeExactlyWithNearEqualSpans() {
        long start = -1_000_000L;
        long end = 3_000_000L; // span 4_000_000, divisible by 4
        List<Slice> slices = TokenRangeSlicer.slice(start, end, 4);
        assertEquals(4, slices.size());
        assertExactCoverage(start, end, slices);
        // Equal-span: 4M / 4 = 1M each.
        for (Slice sl : slices) {
            assertEquals(BigInteger.valueOf(1_000_000L), span(sl.start(), sl.end()));
            assertFalse(sl.wraparound(), "no interior slice of a non-wrapping range wraps");
        }
    }

    @Test
    void spansDifferByAtMostOneTokenWhenNotDivisible() {
        long start = 0L;
        long end = 10L; // span 10, K=4 → 2,3,2,3 (differ by at most 1)
        List<Slice> slices = TokenRangeSlicer.slice(start, end, 4);
        assertEquals(4, slices.size());
        assertExactCoverage(start, end, slices);
        BigInteger min = null;
        BigInteger max = null;
        for (Slice sl : slices) {
            BigInteger sp = span(sl.start(), sl.end());
            min = (min == null || sp.compareTo(min) < 0) ? sp : min;
            max = (max == null || sp.compareTo(max) > 0) ? sp : max;
        }
        assertEquals(BigInteger.ONE, max.subtract(min), "slice spans differ by at most 1 token");
    }

    @Test
    void wholeRingMinToMaxSlicesWithoutOverflow() {
        // Span is 2^64 - 1, which overflows a signed long — exercises the BigInteger path.
        List<Slice> slices = TokenRangeSlicer.slice(Long.MIN_VALUE, Long.MAX_VALUE, 4);
        assertEquals(4, slices.size());
        assertExactCoverage(Long.MIN_VALUE, Long.MAX_VALUE, slices);
        for (Slice sl : slices) {
            assertFalse(sl.wraparound(), "an unwrapped (MIN,MAX] range yields only unwrapped slices");
        }
    }

    @Test
    void wraparoundRangeHasExactlyOneWrappingSlice() {
        // start >= end → the parent wraps the MAX→MIN seam.
        long start = Long.MAX_VALUE - 1000L;
        long end = Long.MIN_VALUE + 1000L;
        List<Slice> slices = TokenRangeSlicer.slice(start, end, 4);
        assertEquals(4, slices.size());
        assertExactCoverage(start, end, slices);
        long wrapping = slices.stream().filter(Slice::wraparound).count();
        assertEquals(1L, wrapping, "exactly one slice carries the wraparound");
    }

    @Test
    void everyBoundaryTokenBelongsToExactlyOneSliceUnderTokenInRange() {
        long start = Long.MAX_VALUE - 1000L;
        long end = Long.MIN_VALUE + 1000L;
        List<Slice> slices = TokenRangeSlicer.slice(start, end, 4);
        // Each slice's inclusive end token must be owned by that slice and no other.
        for (Slice owner : slices) {
            long boundary = owner.end();
            int owners = 0;
            for (Slice s : slices) {
                if (Murmur3Token.tokenInRange(boundary, s.start(), s.end(), s.wraparound())) {
                    owners++;
                }
            }
            assertEquals(1, owners, "boundary token " + boundary + " is owned by exactly one slice");
        }
    }

    @Test
    void k1IsIdentityForNormalRange() {
        List<Slice> slices = TokenRangeSlicer.slice(-100L, 100L, 1);
        assertEquals(1, slices.size());
        assertEquals(-100L, slices.get(0).start());
        assertEquals(100L, slices.get(0).end());
        assertFalse(slices.get(0).wraparound());
    }

    @Test
    void k1IsIdentityForWraparoundAndFullRing() {
        List<Slice> wrap = TokenRangeSlicer.slice(100L, -100L, 1);
        assertEquals(1, wrap.size());
        assertEquals(100L, wrap.get(0).start());
        assertEquals(-100L, wrap.get(0).end());
        assertTrue(wrap.get(0).wraparound(), "K=1 preserves the parent's wraparound flag");

        // Full ring (start == end, #2228) at K=1 stays the single full-ring slice.
        List<Slice> full = TokenRangeSlicer.slice(42L, 42L, 1);
        assertEquals(1, full.size());
        assertEquals(42L, full.get(0).start());
        assertEquals(42L, full.get(0).end());
        assertTrue(full.get(0).wraparound(), "a (T,T] full ring stays full ring at K=1");
    }

    @Test
    void fullRingSlicesIntoKNonEmptySlicesWithOneWrap() {
        List<Slice> slices = TokenRangeSlicer.slice(0L, 0L, 4);
        assertEquals(4, slices.size());
        assertExactCoverage(0L, 0L, slices);
        assertEquals(1L, slices.stream().filter(Slice::wraparound).count(),
                "a sliced full ring has exactly one seam-crossing slice");
        // Each quarter spans 2^62.
        for (Slice sl : slices) {
            assertEquals(RING.divide(BigInteger.valueOf(4)), span(sl.start(), sl.end()));
        }
    }

    @Test
    void degenerateSpanEmitsFewerNonEmptySlices() {
        // Span 3 < K=4 → exactly 3 unit-wide slices, never an empty (x,x].
        List<Slice> slices = TokenRangeSlicer.slice(0L, 3L, 4);
        assertEquals(3, slices.size());
        assertExactCoverage(0L, 3L, slices);
        for (Slice sl : slices) {
            assertEquals(BigInteger.ONE, span(sl.start(), sl.end()), "unit-wide slice");
        }
    }

    @Test
    void singleTokenSpanEmitsExactlyOneSlice() {
        List<Slice> slices = TokenRangeSlicer.slice(7L, 8L, 4);
        assertEquals(1, slices.size());
        assertEquals(7L, slices.get(0).start());
        assertEquals(8L, slices.get(0).end());
    }

    @Test
    void rejectsNonPositiveK() {
        assertThrows(IllegalArgumentException.class, () -> TokenRangeSlicer.slice(0L, 10L, 0));
    }
}
