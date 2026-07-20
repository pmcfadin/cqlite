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

    /**
     * The balance fixture: SIX ranges that CONTIGUOUSLY TILE the whole signed-64-bit ring from
     * {@link Long#MIN_VALUE} to {@link Long#MAX_VALUE} (each range's start is the previous range's
     * end, half-open {@code (start, end]}), with deliberately unequal spans — three "heavy" ranges
     * of ~8 units and three "light" ranges of ~1 unit, an 8× variation. Boundaries are chosen so
     * each range's rotation head ({@code floorMod(startToken, 3)}) spreads over the three owners.
     *
     * <p>An overlapping fixture would be worthless evidence: {@link
     * CqliteFlightSplitManager#validateRingCoverage} rejects overlaps, so such a topology can never
     * reach {@code buildSplits} in production ({@code ringTilesTheRingExactlyOnce} asserts this
     * fixture passes that guard).
     */
    private static final long[] TILING_BOUNDARIES = {
        Long.MIN_VALUE,
        -8540159293384051678L,
        -7856946549913327548L,
        -2391244602147534484L,
        3074457345618258583L,
        8540159293384051650L,
        Long.MAX_VALUE,
    };

    /** The contiguous unequal-span tiling above, as read-replica ranges sharing one owner set. */
    private static List<ReplicaInfo> contiguousUnequalRing() {
        List<ReplicaInfo> ranges = new ArrayList<>();
        for (int i = 0; i + 1 < TILING_BOUNDARIES.length; i++) {
            ranges.add(new ReplicaInfo(
                    Long.toString(TILING_BOUNDARIES[i]),
                    Long.toString(TILING_BOUNDARIES[i + 1]),
                    Map.of("dc1", OWNERS)));
        }
        return ranges;
    }

    /**
     * The balance fixture is a topology that can actually reach {@code buildSplits}: it tiles the
     * ring exactly once, so the fail-closed coverage guard accepts it (no overlap, no gap). Also
     * pins the deliberate span inequality the balance property is claimed under (≥8×).
     */
    @Test
    void balanceFixtureTilesTheRingExactlyOnceWithUnequalSpans() {
        List<ReplicaInfo> ranges = contiguousUnequalRing();
        CqliteFlightSplitManager.validateRingCoverage(ranges); // must not throw
        BigInteger max = BigInteger.ZERO;
        BigInteger min = null;
        for (ReplicaInfo r : ranges) {
            BigInteger span = TokenRangeSlicer.span(r.startToken(), r.endToken());
            max = max.max(span);
            min = min == null ? span : min.min(span);
        }
        assertTrue(max.compareTo(min.multiply(BigInteger.valueOf(8))) >= 0,
                "fixture spans vary by at least 8x (max " + max + ", min " + min + ")");
    }

    @Test
    void perOwnerSpanBalancesWithin1_25xOfMeanUnderUnequalWeights() {
        // A CONTIGUOUS ring tiling (MIN..MAX) with 8x-unequal spans — the only shape that reaches
        // buildSplits in production. Boundaries set each range's rotation head (floorMod(start, 3))
        // so heavy and light ranges' remainder slices spread across all three owners.
        List<ReplicaInfo> ranges = contiguousUnequalRing();
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
    void aggregateSplitWeightIsTheClampedSumOfItsRanges() {
        // The aggregate path builds at K=1 (see CqliteFlightSplitManager#getSplits), so its
        // members are PARENT ranges. Well below the aggregate cap → strictly proportional.
        List<ReplicaInfo> ranges = List.of(range(0, 400), range(1000, 400), range(2000, 400));
        List<CqliteFlightSplit> members = build(ringOf(ranges), 1);
        double expectedSum = members.stream().mapToDouble(CqliteFlightSplit::weightProportion).sum();
        assertTrue(expectedSum < CqliteFlightAggregateSplit.MAX_AGGREGATE_WEIGHT_PROPORTION,
                "this fixture is in the un-saturated regime");
        CqliteFlightAggregateSplit agg = new CqliteFlightAggregateSplit(
                "ks", "t", "ddl", new ArrayList<>(members), Optional.empty(), "{}", "{}");
        assertEquals(SplitWeight.fromProportion(CqliteFlightSplit.clampProportion(expectedSum)),
                agg.getSplitWeight(), "aggregate weight is the clamped sum of its range proportions");
        assertTrue(agg.getSplitWeight().getRawValue()
                        >= members.get(0).getSplitWeight().getRawValue(),
                "the fan-out weight is at least a single range's weight");
    }

    /**
     * The aggregate weight's clamp boundary and SATURATED regime (issue #2680 roborev). Weights are
     * normalized so a mean-span member is 1.0 → the raw sum equals the member count. A fan-out just
     * under the cap stays strictly proportional; a larger one saturates at
     * {@link CqliteFlightAggregateSplit#MAX_AGGREGATE_WEIGHT_PROPORTION} rather than growing toward
     * the 1000 single-split cap (past a node's admission budget more weight changes no scheduling
     * decision — see that constant's rationale).
     */
    @Test
    void aggregateWeightSaturatesAtTheAggregateCap() {
        int cap = (int) CqliteFlightAggregateSplit.MAX_AGGREGATE_WEIGHT_PROPORTION;
        SplitWeight justUnder = aggregateWeightOverEqualRanges(cap - 10);
        assertEquals(SplitWeight.fromProportion(cap - 10.0), justUnder,
                "below the cap the fan-out weight is exactly the member count (mean-span members)");

        SplitWeight saturated = aggregateWeightOverEqualRanges(cap * 3);
        assertEquals(SplitWeight.fromProportion(CqliteFlightAggregateSplit.MAX_AGGREGATE_WEIGHT_PROPORTION),
                saturated, "a 3x-cap fan-out saturates at the aggregate cap, not at 1000");
        assertTrue(saturated.getRawValue()
                        < SplitWeight.fromProportion(CqliteFlightSplit.MAX_WEIGHT_PROPORTION).getRawValue(),
                "the saturated aggregate weight stays below the single-split maximum");
    }

    /** The aggregate weight over {@code count} equal-span (hence mean-span, proportion 1.0) ranges. */
    private static SplitWeight aggregateWeightOverEqualRanges(int count) {
        List<ReplicaInfo> ranges = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ranges.add(range(i * 1000L, 400));
        }
        List<CqliteFlightSplit> members = build(ringOf(ranges), 1);
        assertEquals(count, members.size(), "one member per range at K=1");
        return new CqliteFlightAggregateSplit(
                "ks", "t", "ddl", members, Optional.empty(), "{}", "{}").getSplitWeight();
    }
}
