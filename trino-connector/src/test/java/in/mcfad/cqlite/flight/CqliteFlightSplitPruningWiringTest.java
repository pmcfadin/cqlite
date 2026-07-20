package in.mcfad.cqlite.flight;

import com.sun.net.httpserver.HttpServer;
import in.mcfad.cqlite.flight.sidecar.HostSnapshotApis;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SnapshotApi;
import io.airlift.slice.Slices;
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
import java.util.concurrent.ExecutionException;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end wiring evidence for plan-time split pruning (issue #2679): a fully-bound-PK
 * point read flows through the PUBLIC {@link CqliteFlightSplitManager#getSplits} surface —
 * a real {@link SidecarClient} against an in-process fake Sidecar returning a multi-range
 * topology — and the resulting {@link ConnectorSplitSource} yields exactly ONE split
 * (→ one DoGet to one pod). The assertion reads the public split count, not a helper return.
 *
 * <p>Also the connector-level DIFFERENTIAL check (spec req 5): the SAME query planned with
 * pruning ENABLED vs FORCED OFF yields split sets that cover the bound key identically
 * (every range containing the key's token is retained; none dropped), and the pruned set is
 * strictly fewer splits. Because split coverage is what determines which rows a scan can
 * return, identical covering coverage is the plan-time proxy for identical result rows.
 */
class CqliteFlightSplitPruningWiringTest {

    private static final String DDL = "CREATE TABLE ks.t (id int PRIMARY KEY, v text)";
    private static final CqliteFlightColumnHandle ID_INT =
            new CqliteFlightColumnHandle("id", INTEGER, PushdownCapability.FULL);

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

    private static CqliteFlightConfig config(URI sidecarUri, boolean pruningEnabled) {
        return new CqliteFlightConfig(
                sidecarUri, 8815, "dc1", GroupByPushdownPolicy.AUTOMATIC, 0.5, 3000,
                ReadMode.LIVE, Optional.empty(),
                CqliteFlightConfig.DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS,
                CqliteFlightConfig.DEFAULT_SNAPSHOT_RETIRE_GRACE_MILLIS,
                pruningEnabled);
    }

    /**
     * Fake Sidecar serving a full-ring tiling of many single-token-wide ranges around the
     * token of int 42, so a point read on id=42 must prune to exactly the covering range.
     */
    private static String tokenRangeReplicasJson(long token) {
        long[] boundaries = {token - 2, token - 1, token, token + 1, token + 2};
        StringBuilder sb = new StringBuilder("{\"writeReplicas\":[],\"readReplicas\":[");
        long prev = Long.MIN_VALUE;
        boolean first = true;
        for (long b : boundaries) {
            sb.append(rangeJson(prev, b, first));
            first = false;
            prev = b;
        }
        sb.append(rangeJson(prev, Long.MAX_VALUE, false));
        sb.append("]}");
        return sb.toString();
    }

    private static String rangeJson(long start, long end, boolean first) {
        return (first ? "" : ",")
                + "{\"start\":\"" + start + "\",\"end\":\"" + end
                + "\",\"replicasByDatacenter\":{\"dc1\":[\"10.0.0.1:7000\"]}}";
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

    @Test
    void pointReadThroughPublicGetSplitsYieldsExactlyOneSplit() throws Exception {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A}); // int 42
        HttpServer server = startFakeSidecar(tokenRangeReplicasJson(token));
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            var splitManager = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            CqliteFlightTableHandle handle = new CqliteFlightTableHandle("ks", "t", DDL);
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    ID_INT, Domain.singleValue(INTEGER, 42L))), Constant.TRUE, Map.of());

            ConnectorSplitSource source =
                    splitManager.getSplits(null, null, handle, null, constraint);
            List<ConnectorSplit> splits = drain(source);

            assertEquals(1, splits.size(),
                    "a fully-bound-PK point read yields exactly one split through the public surface");
            CqliteFlightSplit s = (CqliteFlightSplit) splits.get(0);
            assertTrue(Murmur3Token.tokenInRange(token, s.tokenStart(), s.tokenEnd(), s.wraparound()),
                    "the single emitted split covers the key's token");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void prunedVsForcedUnprunedCoverTheKeyIdenticallyAndPrunedIsFewer() throws Exception {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        HttpServer server = startFakeSidecar(tokenRangeReplicasJson(token));
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            CqliteFlightTableHandle handle = new CqliteFlightTableHandle("ks", "t", DDL);
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    ID_INT, Domain.singleValue(INTEGER, 42L))), Constant.TRUE, Map.of());

            var prunedMgr = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            var fullMgr = new CqliteFlightSplitManager(config(uri, false), sidecar, liveSnapshots());

            List<ConnectorSplit> pruned = drain(prunedMgr.getSplits(null, null, handle, null, constraint));
            List<ConnectorSplit> full = drain(fullMgr.getSplits(null, null, handle, null, constraint));

            assertTrue(pruned.size() < full.size(), "pruning emits strictly fewer splits");
            // Differential: the covering set for the key is identical (every full-fan-out split
            // that contains the token is present in the pruned set, and vice versa).
            List<CqliteFlightSplit> fullCovering = full.stream()
                    .map(CqliteFlightSplit.class::cast)
                    .filter(s -> Murmur3Token.tokenInRange(
                            token, s.tokenStart(), s.tokenEnd(), s.wraparound()))
                    .toList();
            List<CqliteFlightSplit> prunedCovering = pruned.stream()
                    .map(CqliteFlightSplit.class::cast)
                    .filter(s -> Murmur3Token.tokenInRange(
                            token, s.tokenStart(), s.tokenEnd(), s.wraparound()))
                    .toList();
            assertEquals(fullCovering.size(), prunedCovering.size(),
                    "pruned coverage of the bound key equals the unpruned coverage");
            assertEquals(prunedCovering.size(), pruned.size(),
                    "every pruned split covers the key — none is spurious");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void inListThroughPublicGetSplitsYieldsUnionCoverage() throws Exception {
        // Two keys → union of covering ranges. Ring built around both tokens.
        long t1 = Murmur3Token.token(new byte[] {0, 0, 0, 0x01});
        long t2 = Murmur3Token.token(new byte[] {0, 0, 0, 0x02});
        long lo = Math.min(t1, t2);
        long hi = Math.max(t1, t2);
        String json = "{\"writeReplicas\":[],\"readReplicas\":["
                + rangeJson(Long.MIN_VALUE, lo - 1, true)
                + rangeJson(lo - 1, lo, false)
                + rangeJson(lo, hi - 1, false)
                + rangeJson(hi - 1, hi, false)
                + rangeJson(hi, Long.MAX_VALUE, false)
                + "]}";
        HttpServer server = startFakeSidecar(json);
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            var mgr = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            CqliteFlightTableHandle handle = new CqliteFlightTableHandle("ks", "t", DDL);
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    ID_INT, Domain.multipleValues(INTEGER, List.of(1L, 2L)))), Constant.TRUE, Map.of());

            List<ConnectorSplit> splits = drain(mgr.getSplits(null, null, handle, null, constraint));
            assertEquals(2, splits.size(), "IN over two keys in two ranges → the deduped two-range union");
            assertTrue(splits.stream().map(CqliteFlightSplit.class::cast).anyMatch(
                    s -> Murmur3Token.tokenInRange(t1, s.tokenStart(), s.tokenEnd(), s.wraparound())));
            assertTrue(splits.stream().map(CqliteFlightSplit.class::cast).anyMatch(
                    s -> Murmur3Token.tokenInRange(t2, s.tokenStart(), s.tokenEnd(), s.wraparound())));
        } finally {
            server.stop(0);
        }
    }

    /**
     * uuid PK (VARCHAR + EQUALITY capability) prunes through the PUBLIC getSplits surface, and the
     * ONE emitted split covers the token of the PARSED 16-byte uuid — proving the uuid string→16-byte
     * parse + token path (PartitionKeyBytes.uuidBytes) is exercised end-to-end, not just some split.
     */
    @Test
    void uuidPkPointReadPrunesToParsedByteTokenThroughPublicSurface() throws Exception {
        CqliteFlightColumnHandle idUuid =
                new CqliteFlightColumnHandle("id", VARCHAR, PushdownCapability.EQUALITY);
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        // The pinned Murmur3TokenTest vector for this uuid's 16 big-endian bytes.
        byte[] parsed = {0x55, 0x0e, (byte) 0x84, 0x00, (byte) 0xe2, (byte) 0x9b, 0x41, (byte) 0xd4,
                (byte) 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00};
        long token = Murmur3Token.token(parsed);
        assertEquals(4277286421682315655L, token, "pinned uuid vector token");
        HttpServer server = startFakeSidecar(tokenRangeReplicasJson(token));
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            var mgr = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            CqliteFlightTableHandle handle =
                    new CqliteFlightTableHandle("ks", "u", "CREATE TABLE ks.u (id uuid PRIMARY KEY, v text)");
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    idUuid, Domain.singleValue(VARCHAR, Slices.utf8Slice(uuid)))), Constant.TRUE, Map.of());

            List<ConnectorSplit> splits = drain(mgr.getSplits(null, null, handle, null, constraint));
            assertEquals(1, splits.size(), "uuid PK point read prunes to one covering split");
            CqliteFlightSplit s = (CqliteFlightSplit) splits.get(0);
            assertTrue(Murmur3Token.tokenInRange(token, s.tokenStart(), s.tokenEnd(), s.wraparound()),
                    "the single emitted split covers the token of the parsed 16-byte uuid");
        } finally {
            server.stop(0);
        }
    }

    /** A non-canonical uuid string cannot parse to 16 bytes → fail-safe full fan-out, never misprune. */
    @Test
    void nonCanonicalUuidStringKeepsFullFanOutThroughPublicSurface() throws Exception {
        CqliteFlightColumnHandle idUuid =
                new CqliteFlightColumnHandle("id", VARCHAR, PushdownCapability.EQUALITY);
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        HttpServer server = startFakeSidecar(tokenRangeReplicasJson(token));
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            var mgr = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            CqliteFlightTableHandle handle =
                    new CqliteFlightTableHandle("ks", "u", "CREATE TABLE ks.u (id uuid PRIMARY KEY, v text)");
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    idUuid, Domain.singleValue(VARCHAR, Slices.utf8Slice("not-a-uuid")))), Constant.TRUE, Map.of());

            List<ConnectorSplit> splits = drain(mgr.getSplits(null, null, handle, null, constraint));
            assertTrue(splits.size() > 1,
                    "a non-canonical uuid string cannot be serialized → full fan-out (never misprune)");
        } finally {
            server.stop(0);
        }
    }

    /**
     * Composite-PK fail-safe through the PUBLIC getSplits surface (issue #2679 roborev): a
     * composite-PK DDL whose quoted {@code ')'} identifier makes the main extractor UNDER-count
     * the partition key must emit the FULL fan-out even with BOTH real components bound. The
     * independent arity cross-check disagrees with the under-count, so the connector refuses to
     * prune — never a mis-pruned (row-dropping) plan. Asserted on the public split count.
     */
    @Test
    void compositePkExtractorUnderCountKeepsFullFanOutThroughPublicSurface() throws Exception {
        CqliteFlightColumnHandle pk1 =
                new CqliteFlightColumnHandle("a)b", VARCHAR, PushdownCapability.FULL);
        CqliteFlightColumnHandle pk2 = new CqliteFlightColumnHandle("b", INTEGER, PushdownCapability.FULL);
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        HttpServer server = startFakeSidecar(tokenRangeReplicasJson(token));
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            var mgr = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            CqliteFlightTableHandle handle = new CqliteFlightTableHandle(
                    "ks", "t", "CREATE TABLE ks.t (\"a)b\" text, b int, v text, PRIMARY KEY ((\"a)b\", b)))");
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    pk1, Domain.singleValue(VARCHAR, Slices.utf8Slice("hello")),
                    pk2, Domain.singleValue(INTEGER, 42L))), Constant.TRUE, Map.of());

            List<ConnectorSplit> splits = drain(mgr.getSplits(null, null, handle, null, constraint));
            assertTrue(splits.size() > 1,
                    "an extractor under-count on a composite PK → full fan-out (never drop rows)");
        } finally {
            server.stop(0);
        }
    }

    /** VARCHAR text PK (utf-8 bytes) also prunes through the public surface. */
    @Test
    void textPkPointReadPrunesThroughPublicSurface() throws Exception {
        CqliteFlightColumnHandle nameText =
                new CqliteFlightColumnHandle("id", VARCHAR, PushdownCapability.FULL);
        long token = Murmur3Token.token("hello".getBytes(StandardCharsets.UTF_8));
        HttpServer server = startFakeSidecar(tokenRangeReplicasJson(token));
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            SidecarClient sidecar = new SidecarClient(uri);
            var mgr = new CqliteFlightSplitManager(config(uri, true), sidecar, liveSnapshots());
            CqliteFlightTableHandle handle =
                    new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id text PRIMARY KEY, v text)");
            var constraint = new Constraint(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                    nameText, Domain.singleValue(VARCHAR, Slices.utf8Slice("hello")))), Constant.TRUE, Map.of());

            List<ConnectorSplit> splits = drain(mgr.getSplits(null, null, handle, null, constraint));
            assertEquals(1, splits.size(), "text PK point read prunes to one covering split");
        } finally {
            server.stop(0);
        }
    }
}
