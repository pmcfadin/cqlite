package in.mcfad.cqlite.flight;

import com.sun.net.httpserver.HttpServer;
import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PUBLIC-surface wiring evidence for weight-balanced sub-splitting (issue #2680): the configured
 * {@code cqlite.sub-splits-per-range} actually multiplies the split count emitted by
 * {@link CqliteFlightSplitManager#getSplits} on the ORDINARY (unpruned) full-scan path — the path
 * every scan takes — and the DEFAULT config yields the documented 4× fan-out. Without this, a
 * regression dropping {@code config.subSplitsPerRange()} at the getSplits call site would leave the
 * static {@code buildSplits} unit tests green (issue #2680 roborev).
 *
 * <p>Also pins the aggregate exemption: an aggregated handle's finalize split is built at K=1, so
 * its {@code ranges()} are PARENT ranges. Its PageSource fans out SEQUENTIALLY (one blocking DoGet
 * per member on one driver), so slicing it K-ways would multiply serialized round trips K× with
 * nothing to balance across nodes.
 */
class CqliteFlightSubSplitWiringTest {

    private static final String DDL = "CREATE TABLE ks.t (id int PRIMARY KEY, v text)";

    /** A three-range full-ring tiling with WIDE spans, so every range slices into exactly K slices. */
    private static final long[] RING_BOUNDARIES = {
        Long.MIN_VALUE, -3074457345618258602L, 3074457345618258602L, Long.MAX_VALUE,
    };

    private static final int RANGE_COUNT = RING_BOUNDARIES.length - 1;

    private static String wideRingJson() {
        StringBuilder sb = new StringBuilder("{\"writeReplicas\":[],\"readReplicas\":[");
        for (int i = 0; i + 1 < RING_BOUNDARIES.length; i++) {
            sb.append(i == 0 ? "" : ",")
                    .append("{\"start\":\"").append(RING_BOUNDARIES[i])
                    .append("\",\"end\":\"").append(RING_BOUNDARIES[i + 1])
                    .append("\",\"replicasByDatacenter\":{\"dc1\":[\"10.0.0.1:7000\","
                            + "\"10.0.0.2:7000\",\"10.0.0.3:7000\"]}}");
        }
        return sb.append("]}").toString();
    }

    /** LIVE-mode SnapshotManager (no snapshot fan-out) with a no-op per-host Sidecar. */
    private static SnapshotManager liveSnapshots() {
        HostSnapshotApis noop = host -> new SnapshotApi() {
            @Override
            public void createSnapshot(String ks, String t, String name, Optional<String> ttl) {}

            @Override
            public void clearSnapshot(String ks, String t, String name) {}
        };
        return new SnapshotManager(noop, ReadMode.LIVE, Optional.empty());
    }

    private static CqliteFlightConfig config(URI sidecarUri, boolean pruningEnabled, int subSplitsPerRange) {
        return new CqliteFlightConfig(
                sidecarUri, 8815, "dc1", GroupByPushdownPolicy.AUTOMATIC, 0.5, 3000,
                ReadMode.LIVE, Optional.empty(),
                CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS,
                CqliteFlightConfig.DEFAULT_SNAPSHOT_RETIRE_GRACE_MILLIS,
                pruningEnabled, subSplitsPerRange);
    }

    private static HttpServer startFakeSidecar(String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        server.start();
        return server;
    }

    private static List<ConnectorSplit> drain(ConnectorSplitSource source)
            throws ExecutionException, InterruptedException {
        List<ConnectorSplit> all = new java.util.ArrayList<>();
        while (!source.isFinished()) {
            all.addAll(source.getNextBatch(1000).get().getSplits());
        }
        source.close();
        return all;
    }

    private static final CqliteFlightColumnHandle ID_INT =
            new CqliteFlightColumnHandle("id", INTEGER, PushdownCapability.FULL);

    /** Plan an unconstrained scan of {@code handle} through the PUBLIC getSplits surface. */
    private static List<ConnectorSplit> getSplits(
            CqliteFlightConfigFactory configFactory, CqliteFlightTableHandle handle) throws Exception {
        return getSplits(configFactory, handle, new Constraint(TupleDomain.all()));
    }

    /** Plan {@code handle} under {@code constraint} through the PUBLIC getSplits surface. */
    private static List<ConnectorSplit> getSplits(
            CqliteFlightConfigFactory configFactory, CqliteFlightTableHandle handle, Constraint constraint)
            throws Exception {
        HttpServer server = startFakeSidecar(wideRingJson());
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            var mgr = new CqliteFlightSplitManager(
                    configFactory.create(uri), new SidecarClient(uri), liveSnapshots());
            return drain(mgr.getSplits(null, null, handle, null, constraint));
        } finally {
            server.stop(0);
        }
    }

    /** Builds the connector config once the fake Sidecar's ephemeral port is known. */
    private interface CqliteFlightConfigFactory {
        CqliteFlightConfig create(URI sidecarUri);
    }

    private static CqliteFlightTableHandle scanHandle() {
        return new CqliteFlightTableHandle("ks", "t", DDL);
    }

    /**
     * A CONFIGURED K multiplies the emitted split count on the ordinary unpruned scan path.
     * Pruning is explicitly DISABLED so the count is the pure sub-splitting effect.
     */
    @Test
    void configuredSubSplitsPerRangeMultipliesSplitCountThroughPublicSurface() throws Exception {
        for (int k : new int[] {1, 2, 3, 8}) {
            List<ConnectorSplit> splits = getSplits(uri -> config(uri, false, k), scanHandle());
            assertEquals(RANGE_COUNT * k, splits.size(),
                    "K=" + k + " must emit rangeCount x K splits through getSplits");
        }
    }

    /**
     * A LIMIT-pushed handle plans at K=1 (issue #2680 defense in depth against the #2782 hang):
     * the emitted scan split count equals the range count, NOT range count × 4, even at the
     * default configured K. The pushed LIMIT shape is kept structurally out of the multi-stream
     * path independent of the drain fix.
     */
    @Test
    void limitPushedHandlePlansAtK1ThroughPublicSurface() throws Exception {
        CqliteFlightTableHandle limited = new CqliteFlightTableHandle(
                "ks", "t", DDL, Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.of(10));
        List<ConnectorSplit> splits = getSplits(uri -> config(uri, false, 4), limited);
        assertEquals(RANGE_COUNT, splits.size(),
                "a LIMIT-pushed handle plans at K=1 (range count), not range count × 4");
    }

    /**
     * A fully-bound partition-key point read plans at K=1 (issue #2680): the covering range is NOT
     * sub-split, so exactly one covering split is emitted and it spans the FULL parent range (a
     * K=4 slice would be a quarter-width sub-range). This proves K=1 specifically, not merely that
     * pruning collapses to one split (a sliced-then-pruned plan would also yield one split).
     */
    @Test
    void fullyBoundPointReadPlansAtK1ThroughPublicSurface() throws Exception {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A}); // int 42
        // Which parent range of the wide 3-range ring covers the token?
        int covering = -1;
        for (int i = 0; i + 1 < RING_BOUNDARIES.length; i++) {
            if (Murmur3Token.tokenInRange(token, RING_BOUNDARIES[i], RING_BOUNDARIES[i + 1],
                    RING_BOUNDARIES[i] >= RING_BOUNDARIES[i + 1])) {
                covering = i;
                break;
            }
        }
        assertTrue(covering >= 0, "the wide ring covers the point-read token");

        var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                (ColumnHandle) ID_INT, Domain.singleValue(INTEGER, 42L))), Constant.TRUE, Map.of());
        List<ConnectorSplit> splits =
                getSplits(uri -> config(uri, true, 4), scanHandle(), constraint);

        assertEquals(1, splits.size(), "a fully-bound point read emits exactly one covering split");
        CqliteFlightSplit s = (CqliteFlightSplit) splits.get(0);
        assertEquals(RING_BOUNDARIES[covering], s.tokenStart(),
                "the covering split spans the FULL parent range start (K=1, not a K=4 slice)");
        assertEquals(RING_BOUNDARIES[covering + 1], s.tokenEnd(),
                "the covering split spans the FULL parent range end (K=1, not a K=4 slice)");
    }

    /** The DEFAULT config (no explicit K) emits the documented 4x fan-out through getSplits. */
    @Test
    void defaultConfigEmitsFourSplitsPerRangeThroughPublicSurface() throws Exception {
        List<ConnectorSplit> splits = getSplits(
                uri -> config(uri, true, CqliteFlightConfig.DEFAULT_SUB_SPLITS_PER_RANGE),
                scanHandle());
        assertEquals(RANGE_COUNT * 4, splits.size(),
                "the default cqlite.sub-splits-per-range=4 emits 4 splits per range");
        assertEquals(4, CqliteFlightConfig.DEFAULT_SUB_SPLITS_PER_RANGE, "documented default");
    }

    /**
     * The aggregate exemption (issue #2680 roborev): at K=4 the ONE finalize split still carries
     * PARENT ranges, not 4x slices — its PageSource fans out sequentially on a single driver, so
     * slicing would only multiply serialized DoGets (~4x round trips) with nothing to balance.
     */
    @Test
    void aggregatedHandleFinalizeSplitKeepsParentRangesAtK4() throws Exception {
        CqliteFlightTableHandle aggregated = new CqliteFlightTableHandle(
                "ks", "t", DDL, Optional.empty(),
                Optional.of("{\"aggregates\":[],\"group_by\":[]}"), Optional.of("{}"));

        List<ConnectorSplit> splits = getSplits(uri -> config(uri, true, 4), aggregated);
        assertEquals(1, splits.size(), "an aggregated handle plans exactly one finalize split");
        CqliteFlightAggregateSplit finalize = (CqliteFlightAggregateSplit) splits.get(0);
        assertEquals(RANGE_COUNT, finalize.ranges().size(),
                "the finalize split fans out over PARENT ranges at K=4, not rangeCount x 4 slices");

        // And the scan path over the SAME topology/config does sub-split — so this is an
        // aggregate-specific exemption, not sub-splitting being off altogether.
        assertEquals(RANGE_COUNT * 4, getSplits(uri -> config(uri, true, 4), scanHandle()).size(),
                "the non-aggregated scan path still sub-splits at K=4");
    }

    /** The finalize split's weight stays in Trino's valid range and is not the SPI default. */
    @Test
    void finalizeSplitReportsAPositiveNonDefaultWeight() throws Exception {
        List<ConnectorSplit> splits = getSplits(uri -> config(uri, true, 4),
                new CqliteFlightTableHandle("ks", "t", DDL, Optional.empty(),
                        Optional.of("{\"aggregates\":[],\"group_by\":[]}"), Optional.of("{}")));
        CqliteFlightAggregateSplit finalize = (CqliteFlightAggregateSplit) splits.get(0);
        assertTrue(finalize.getSplitWeight().getRawValue() > 0, "no zero weight");
        assertTrue(finalize.getSplitWeight().getRawValue()
                        <= io.trino.spi.SplitWeight.fromProportion(
                                CqliteFlightAggregateSplit.MAX_AGGREGATE_WEIGHT_PROPORTION).getRawValue(),
                "the finalize weight never exceeds the aggregate cap");
    }
}
