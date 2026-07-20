package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import io.trino.spi.SplitWeight;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Weight-balanced split→pod assignment (issue #2680): per-owner assigned token span converges to
 * ~1/N of the total under an RF==N fixture with deliberately unequal per-range spans, and each
 * split reports a {@link SplitWeight} proportional to its slice's token span (mean-span slice =
 * standard, clamped to Trino's valid range).
 */
class CqliteFlightWeightBalanceTest {

    private static final CqliteFlightTableHandle TABLE =
            new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY)");

    // RF == N == 3: one identical owner set shared by every range (the field's 3-node/RF=3 shape).
    private static final List<String> OWNERS =
            List.of("10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000");

    private static ReplicaInfo range(long start, long span) {
        return new ReplicaInfo(Long.toString(start), Long.toString(start + span), Map.of("dc1", OWNERS));
    }

    private static TokenRangeReplicasResponse ringOf(List<ReplicaInfo> ranges) {
        return new TokenRangeReplicasResponse(List.of(), ranges);
    }

    private static List<CqliteFlightSplit> build(TokenRangeReplicasResponse resp, int k) {
        return CqliteFlightSplitManager.buildSplits(
                TABLE, resp, "dc1", 8815, Optional.empty(), Set.of(), k);
    }

    @Test
    void perOwnerSpanBalancesWithin1_25xOfMeanUnderUnequalWeights() {
        // Six ranges, spans varying 8× (800 vs 100). Start tokens set each range's rotation head
        // (floorMod(start, 3)) so the heavy ranges' remainder slice spreads across all owners.
        List<ReplicaInfo> ranges = List.of(
                range(0, 800), range(1, 800), range(2, 800),   // heads 0,1,2 (heavy)
                range(3, 100), range(4, 100), range(5, 100));  // heads 0,1,2 (light)
        var splits = build(ringOf(ranges), 4);
        assertEquals(6 * 4, splits.size(), "K=4 slices per range");

        // Per-owner sum of assigned token span.
        Map<String, BigInteger> perOwner = new java.util.HashMap<>();
        for (CqliteFlightSplit s : splits) {
            perOwner.merge(s.host(), TokenRangeSlicer.span(s.tokenStart(), s.tokenEnd()), BigInteger::add);
        }
        assertEquals(3, perOwner.size(), "every owner is primary for some slice");

        BigInteger total = perOwner.values().stream().reduce(BigInteger.ZERO, BigInteger::add);
        // mean = total / N; assert max owner span <= 1.25 * mean, i.e. 4 * maxOwner <= 5 * mean,
        // i.e. 4 * N * maxOwner <= 5 * total (all-integer, no rounding).
        BigInteger maxOwner = perOwner.values().stream().max(BigInteger::compareTo).orElseThrow();
        BigInteger lhs = maxOwner.multiply(BigInteger.valueOf(4L * perOwner.size()));
        BigInteger rhs = total.multiply(BigInteger.valueOf(5));
        assertTrue(lhs.compareTo(rhs) <= 0,
                "per-owner span " + maxOwner + " exceeds 1.25x mean (total " + total + ")");

        // Count cap at slice granularity: no owner primary for more than ceil(totalSlices/N).
        Map<String, Long> counts = splits.stream()
                .collect(Collectors.groupingBy(CqliteFlightSplit::host, Collectors.counting()));
        long cap = ((long) splits.size() + perOwner.size() - 1) / perOwner.size();
        counts.forEach((h, c) -> assertTrue(c <= cap,
                h + " primary for " + c + " slices, exceeding ceil(totalSlices/N)=" + cap));
    }

    @Test
    void meanSpanSliceReportsStandardWeight() {
        // All ranges equal span → every slice span equals the mean → weight is exactly standard.
        List<ReplicaInfo> ranges = List.of(range(0, 400), range(1000, 400), range(2000, 400));
        var splits = build(ringOf(ranges), 4);
        for (CqliteFlightSplit s : splits) {
            assertEquals(SplitWeight.standard(), s.getSplitWeight(),
                    "a mean-span slice reports the standard weight");
        }
    }

    @Test
    void weightTracksTokenSpanRoughlyProportionally() {
        // Two ranges, one span 3× the other, K=1 → two slices whose weights differ ~3×.
        List<ReplicaInfo> ranges = List.of(range(0, 300), range(10_000, 100));
        var splits = build(ringOf(ranges), 1);
        assertEquals(2, splits.size());
        CqliteFlightSplit big = splits.stream()
                .max((a, b) -> TokenRangeSlicer.span(a.tokenStart(), a.tokenEnd())
                        .compareTo(TokenRangeSlicer.span(b.tokenStart(), b.tokenEnd()))).orElseThrow();
        CqliteFlightSplit small = splits.stream()
                .min((a, b) -> TokenRangeSlicer.span(a.tokenStart(), a.tokenEnd())
                        .compareTo(TokenRangeSlicer.span(b.tokenStart(), b.tokenEnd()))).orElseThrow();
        double ratio = (double) big.getSplitWeight().getRawValue() / small.getSplitWeight().getRawValue();
        assertTrue(ratio >= 2.5 && ratio <= 3.5, "weights track span ~3x, got ratio " + ratio);
    }

    @Test
    void extremeSpansStayWithinTrinoValidWeightRange() {
        // A near-zero-span slice and a whole-ring-span slice: both weights must be valid (no
        // exception, no zero raw value) after clamping.
        List<ReplicaInfo> ranges = List.of(
                range(0, 1),                                        // span 1 (tiny)
                new ReplicaInfo(Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE),
                        Map.of("dc1", OWNERS)));                    // span 2^64 - 1 (huge)
        var splits = build(ringOf(ranges), 1);
        for (CqliteFlightSplit s : splits) {
            SplitWeight w = s.getSplitWeight(); // must not throw
            assertTrue(w.getRawValue() > 0, "no split reports a zero weight");
        }
    }

    @Test
    void aggregateSplitWeightIsTheClampedSumOfItsSlices() {
        List<ReplicaInfo> ranges = List.of(range(0, 400), range(1000, 400), range(2000, 400));
        List<CqliteFlightSplit> slices = build(ringOf(ranges), 4);
        double expectedSum = slices.stream().mapToDouble(CqliteFlightSplit::weightProportion).sum();
        CqliteFlightAggregateSplit agg = new CqliteFlightAggregateSplit(
                "ks", "t", "ddl", new ArrayList<>(slices), Optional.empty(), "{}", "{}");
        assertEquals(SplitWeight.fromProportion(CqliteFlightSplit.clampProportion(expectedSum)),
                agg.getSplitWeight(), "aggregate weight is the clamped sum of its slice proportions");
        assertTrue(agg.getSplitWeight().getRawValue()
                        >= slices.get(0).getSplitWeight().getRawValue(),
                "the fan-out weight is at least a single slice's weight");
    }
}
