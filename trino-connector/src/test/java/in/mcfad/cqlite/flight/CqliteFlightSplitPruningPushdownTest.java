package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Split pruning through the REAL pushdown path (issue #2806). #2679's plan-time split
 * pruning was inert in the field: {@code applyFilter} pushes the fully-bound point-read
 * PK predicate into the handle's {@code filterJson} and returns the summary UNENFORCED
 * (the honesty contract, #2164), so the {@code Constraint} Trino hands {@code getSplits}
 * binds NO columns ({@code TupleDomain.ALL}). Reading the bound key only from that
 * split-time constraint therefore always fell through to the full fan-out.
 *
 * <p>These tests reproduce that exact path — {@link CqliteFlightMetadata#applyFilter}
 * (the true pushdown) to build the handle, then {@link CqliteFlightSplitManager
 * #pruneToBoundPartitionKey} with an {@code ALL} split-time constraint (what getSplits
 * actually receives) — so a unit test that fed a populated {@code Constraint.getSummary()}
 * (as {@link CqliteFlightSplitPruningTest} does) can no longer pass while the field fails.
 */
class CqliteFlightSplitPruningPushdownTest {

    private final CqliteFlightMetadata metadata = new CqliteFlightMetadata(null, null, null);

    private static final String INT_PK_DDL = "CREATE TABLE ks.t (id int PRIMARY KEY, v text)";
    private static final CqliteFlightColumnHandle ID_INT =
            new CqliteFlightColumnHandle("id", INTEGER, PushdownCapability.FULL);

    private static final CqliteFlightColumnHandle V_TEXT =
            new CqliteFlightColumnHandle("v", VARCHAR, PushdownCapability.FULL);

    private static final String COMPOSITE_PK_DDL =
            "CREATE TABLE ks.t (a text, b int, v text, PRIMARY KEY ((a, b)))";
    private static final CqliteFlightColumnHandle A_TEXT =
            new CqliteFlightColumnHandle("a", VARCHAR, PushdownCapability.FULL);

    // Mirror Trino's handle codec (airlift ObjectMapperProvider): serialize by FIELDS, Optional-
    // aware via the jdk8 module — the handle round-trips coordinator->worker like a split does.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static ReplicaInfo range(long start, long end) {
        return new ReplicaInfo(Long.toString(start), Long.toString(end),
                Map.of("dc1", List.of("10.0.0.1:7000")));
    }

    /** A full unwrapped (MIN, .., MAX] tiling with a range per interior boundary. */
    private static TokenRangeReplicasResponse ringCovering(long... interiorBoundaries) {
        long[] sorted = interiorBoundaries.clone();
        java.util.Arrays.sort(sorted);
        List<ReplicaInfo> ranges = new ArrayList<>();
        long prev = Long.MIN_VALUE;
        for (long b : sorted) {
            ranges.add(range(prev, b));
            prev = b;
        }
        ranges.add(range(prev, Long.MAX_VALUE));
        return new TokenRangeReplicasResponse(List.of(), ranges);
    }

    /** The split-time constraint getSplits actually receives after full PK pushdown: binds nothing. */
    private static Constraint enforcedAll() {
        return new Constraint(TupleDomain.all(), Constant.TRUE, Map.of());
    }

    /**
     * Drive the REAL pushdown: apply a summary-delivered predicate through
     * {@link CqliteFlightMetadata#applyFilter}, returning the rebuilt handle (predicate
     * now in filterJson; bound-key tokens precomputed onto the handle).
     */
    private CqliteFlightTableHandle pushdown(String ddl, TupleDomain<ColumnHandle> summary) {
        ConnectorTableHandle base = new CqliteFlightTableHandle("ks", "t", ddl);
        CqliteFlightTableHandle handle = applyFilterOn(base, summary);
        // Sanity: the predicate really did land in filterJson (the field pushdown).
        assertTrue(handle.filterJson().isPresent(), "PK predicate must be pushed into filterJson");
        return handle;
    }

    /** Apply one more predicate to an existing handle through the real applyFilter (iterative pushdown). */
    private CqliteFlightTableHandle applyFilterOn(ConnectorTableHandle base, TupleDomain<ColumnHandle> summary) {
        var applied = metadata.applyFilter(null, base, new Constraint(summary, Constant.TRUE, Map.of()))
                .orElseThrow(() -> new AssertionError("applyFilter should push the predicate"));
        assertEquals(summary, applied.getRemainingFilter(),
                "summary is returned unenforced (#2164) — so the split-time constraint binds nothing");
        return (CqliteFlightTableHandle) applied.getHandle();
    }

    @Test
    void fullyBoundPkPushedIntoFilterJsonStillPrunesToOneSplit() {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A}); // int 42
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);

        CqliteFlightTableHandle handle = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.singleValue(INTEGER, 42L))));

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(handle, resp, "dc1", 8815);
        assertTrue(full.size() > 1, "fixture must fan out to many ranges");

        // The enforced split-time constraint binds NO columns — the pre-#2806 read source.
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                handle, full, enforcedAll(), true, Partitioner.MURMUR3);

        assertEquals(1, pruned.size(),
                "the bound PK lives in filterJson/on the handle, not the split-time constraint — "
                        + "pruning must still reach exactly one covering split");
        CqliteFlightSplit s = pruned.get(0);
        assertTrue(Murmur3Token.tokenInRange(token, s.tokenStart(), s.tokenEnd(), s.wraparound()));
    }

    @Test
    void inListPushedIntoFilterJsonPrunesToTwoRangeUnion() {
        long t1 = Murmur3Token.token(new byte[] {0, 0, 0, 0x01}); // int 1
        long t2 = Murmur3Token.token(new byte[] {0, 0, 0, 0x02}); // int 2
        long lo = Math.min(t1, t2);
        long hi = Math.max(t1, t2);
        TokenRangeReplicasResponse resp = ringCovering(lo - 1, lo, hi - 1, hi);

        CqliteFlightTableHandle handle = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.multipleValues(INTEGER, List.of(1L, 2L)))));

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(handle, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                handle, full, enforcedAll(), true, Partitioner.MURMUR3);

        assertEquals(2, pruned.size(), "IN over two keys in two ranges → the two-range union");
        assertTrue(pruned.stream().anyMatch(
                s -> Murmur3Token.tokenInRange(t1, s.tokenStart(), s.tokenEnd(), s.wraparound())));
        assertTrue(pruned.stream().anyMatch(
                s -> Murmur3Token.tokenInRange(t2, s.tokenStart(), s.tokenEnd(), s.wraparound())));
    }

    @Test
    void notFullyBoundCompositePkKeepsFullFanOut() {
        // Only the first component of a composite PK is bound → the PK is NOT fully bound,
        // so no tokens are attached and the fail-safe full fan-out is preserved even though
        // 'a = hello' was pushed into filterJson.
        TokenRangeReplicasResponse resp = ringCovering(-100L, 0L, 100L);
        CqliteFlightTableHandle handle = pushdown(COMPOSITE_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        A_TEXT, Domain.singleValue(VARCHAR, Slices.utf8Slice("hello")))));

        assertTrue(handle.boundKeyTokens().isEmpty(), "a partial composite PK attaches no tokens");
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(handle, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                handle, full, enforcedAll(), true, Partitioner.MURMUR3);

        assertEquals(full.size(), pruned.size(), "partial PK binding → full fan-out (never drop rows)");
    }

    @Test
    void toggleOffKeepsFullFanOutEvenWithBoundKeyTokens() {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        CqliteFlightTableHandle handle = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.singleValue(INTEGER, 42L))));

        assertFalse(handle.boundKeyTokens().isEmpty(), "a fully-bound PK attaches tokens");
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(handle, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                handle, full, enforcedAll(), false, Partitioner.MURMUR3);

        assertEquals(full.size(), pruned.size(), "the kill switch forces the unpruned baseline");
    }

    @Test
    void unsupportedPartitionerAtSplitTimeIgnoresHandleTokens() {
        // Defensive fail-safe (roborev): even a handle carrying Murmur3 tokens must NOT be
        // consumed when the split-time partitioner resolves to non-Murmur3 — the token fast
        // path is gated on the partitioner, mirroring the constraint path's guard.
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        CqliteFlightTableHandle handle = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.singleValue(INTEGER, 42L))));
        assertFalse(handle.boundKeyTokens().isEmpty(), "a fully-bound PK attaches tokens");

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(handle, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                handle, full, enforcedAll(), true, Partitioner.UNSUPPORTED);

        assertEquals(full.size(), pruned.size(),
                "a non-Murmur3 split-time partitioner must not consume the handle's Murmur3 tokens");
    }

    @Test
    void tokensSurviveASecondNonPkBindingApplyFilterAndStillPrune() {
        // Iterative pushdown (#2164 accumulation): call 1 binds the PK, call 2 binds only a
        // non-PK column. The PK tokens attached by call 1 must be INHERITED by call 2's handle
        // (deleting the inherit branch drops them → this test fails), and pruning still fires.
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A}); // int 42
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);

        CqliteFlightTableHandle afterPk = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.singleValue(INTEGER, 42L))));
        assertFalse(afterPk.boundKeyTokens().isEmpty(), "call 1 (PK-binding) attaches tokens");

        // Call 2 binds only 'v' (a non-PK column) — no PK tokens derivable from this summary.
        CqliteFlightTableHandle afterBoth = applyFilterOn(afterPk,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        V_TEXT, Domain.singleValue(VARCHAR, Slices.utf8Slice("x")))));
        assertEquals(afterPk.boundKeyTokens(), afterBoth.boundKeyTokens(),
                "the PK tokens from call 1 must survive a later non-PK-binding applyFilter call");

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(afterBoth, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                afterBoth, full, enforcedAll(), true, Partitioner.MURMUR3);
        assertEquals(1, pruned.size(), "inherited PK tokens still prune to the covering split");
    }

    @Test
    void tokensSurviveApplyLimitRebuildAndStillPrune() {
        // The canonical field query WHERE key = ? LIMIT n: applyFilter then applyLimit both
        // rebuild the handle (#2129 rebuild-drops-state class). The pruning tokens must survive.
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);

        CqliteFlightTableHandle afterFilter = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.singleValue(INTEGER, 42L))));

        var limited = metadata.applyLimit(null, afterFilter, 5L)
                .orElseThrow(() -> new AssertionError("applyLimit should push the cap"));
        CqliteFlightTableHandle afterLimit = (CqliteFlightTableHandle) limited.getHandle();
        assertEquals(5L, afterLimit.limit().orElseThrow(), "the limit is pushed");
        assertEquals(afterFilter.boundKeyTokens(), afterLimit.boundKeyTokens(),
                "applyLimit's rebuild must preserve the precomputed bound-key tokens");

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(afterLimit, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                afterLimit, full, enforcedAll(), true, Partitioner.MURMUR3);
        assertEquals(1, pruned.size(), "tokens preserved across applyLimit still prune to one split");
    }

    @Test
    void handleWithBoundKeyTokensSurvivesJsonRoundTrip() throws Exception {
        // The handle serializes coordinator->worker like a split; a serde break on the new
        // boundKeyTokens component would fail every query. Round-trip a fully-populated handle.
        CqliteFlightTableHandle original = pushdown(INT_PK_DDL,
                TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                        ID_INT, Domain.singleValue(INTEGER, 42L))));
        assertFalse(original.boundKeyTokens().isEmpty(), "precondition: tokens are attached");

        byte[] json = MAPPER.writeValueAsBytes(original);
        CqliteFlightTableHandle restored = MAPPER.readValue(json, CqliteFlightTableHandle.class);

        assertEquals(original, restored, "the whole 8-component handle round-trips");
        assertEquals(original.boundKeyTokens(), restored.boundKeyTokens(), "bound-key tokens survive serde");
    }
}
