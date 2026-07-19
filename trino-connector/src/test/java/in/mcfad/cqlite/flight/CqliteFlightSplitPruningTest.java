package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plan-time split pruning (issue #2679): a fully-bound partition key prunes the emitted
 * splits to the covering token range(s); anything less keeps the full fan-out. Exercises
 * {@link CqliteFlightSplitManager#pruneToBoundPartitionKey} — the seam {@code getSplits}
 * calls after {@code buildSplits} — over a multi-range fixture whose ranges tile the ring.
 */
class CqliteFlightSplitPruningTest {

    // int PK 'id'; the server surfaces int as FULL-capability.
    private static final CqliteFlightColumnHandle ID_INT =
            new CqliteFlightColumnHandle("id", INTEGER, PushdownCapability.FULL);
    private static final CqliteFlightColumnHandle NAME_TEXT =
            new CqliteFlightColumnHandle("name", VARCHAR, PushdownCapability.FULL);

    private static final CqliteFlightTableHandle INT_PK_TABLE =
            new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id int PRIMARY KEY, v text)");

    // uuid / timeuuid PKs surface as VARCHAR with EQUALITY capability; the server parses the
    // hyphenated string to the 16-byte big-endian UUID (PartitionKeyBytes.uuidBytes) — a
    // distinct serialization from a genuine text (FULL, UTF-8) PK.
    private static final CqliteFlightColumnHandle ID_UUID =
            new CqliteFlightColumnHandle("id", VARCHAR, PushdownCapability.EQUALITY);
    private static final CqliteFlightTableHandle UUID_PK_TABLE =
            new CqliteFlightTableHandle("ks", "u", "CREATE TABLE ks.u (id uuid PRIMARY KEY, v text)");
    private static final CqliteFlightTableHandle TIMEUUID_PK_TABLE =
            new CqliteFlightTableHandle("ks", "u", "CREATE TABLE ks.u (id timeuuid PRIMARY KEY, v text)");

    private static byte[] bytes(int... vals) {
        byte[] out = new byte[vals.length];
        for (int i = 0; i < vals.length; i++) {
            out[i] = (byte) vals[i];
        }
        return out;
    }

    private static ReplicaInfo range(long start, long end) {
        return new ReplicaInfo(Long.toString(start), Long.toString(end),
                Map.of("dc1", List.of("10.0.0.1:7000")));
    }

    /**
     * A full-ring tiling split into many single-token-width ranges so distinct keys land in
     * distinct ranges. Boundaries are the sorted token set plus MIN/MAX endpoints; the ranges
     * are {@code (b[i], b[i+1]]} and the closing range wraps MAX→MIN via a final (MAX==MIN)
     * arrangement. We instead use an explicit unwrapped (MIN, .., MAX] chain (validateRingCoverage
     * accepts it) so no wraparound complicates the covering assertion.
     */
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

    private static Constraint summary(TupleDomain<ColumnHandle> summary) {
        return new Constraint(summary, Constant.TRUE, Map.of());
    }

    private static List<CqliteFlightSplit> prune(
            CqliteFlightTableHandle table, TokenRangeReplicasResponse resp, Constraint constraint) {
        List<CqliteFlightSplit> full =
                CqliteFlightSplitManager.buildSplits(table, resp, "dc1", 8815);
        return CqliteFlightSplitManager.pruneToBoundPartitionKey(
                table, full, constraint, true, Partitioner.MURMUR3);
    }

    // ── Requirement 1: single fully-bound PK → exactly the covering split ────────

    @Test
    void singleFullyBoundPkEmitsExactlyTheCoveringSplit() {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A}); // int 42
        // Build a ring whose boundaries split the token off into its own range.
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_INT, Domain.singleValue(INTEGER, 42L))));

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(INT_PK_TABLE, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = prune(INT_PK_TABLE, resp, constraint);

        assertTrue(full.size() > 1, "fixture must fan out to many ranges on main");
        assertEquals(1, pruned.size(), "a single fully-bound PK prunes to exactly one covering split");
        // The one emitted split's (start, end] contains the key's token.
        CqliteFlightSplit s = pruned.get(0);
        assertTrue(Murmur3Token.tokenInRange(token, s.tokenStart(), s.tokenEnd(), s.wraparound()));
    }

    @Test
    void partialOrAbsentOrRangePkKeepsFullFanOut() {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(INT_PK_TABLE, resp, "dc1", 8815);

        // Absent PK: only a non-PK predicate bound.
        var nonPk = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                NAME_TEXT, Domain.singleValue(VARCHAR, Slices.utf8Slice("x")))));
        assertEquals(full.size(), prune(INT_PK_TABLE, resp, nonPk).size(),
                "a non-PK predicate does not prune");

        // Range on the PK column: not equality/IN → no pruning.
        var rangePk = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_INT, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false))));
        assertEquals(full.size(), prune(INT_PK_TABLE, resp, rangePk).size(),
                "a range on the PK does not prune");

        // Absent constraint (TupleDomain.all()).
        assertEquals(full.size(),
                prune(INT_PK_TABLE, resp, summary(TupleDomain.all())).size(),
                "no bound columns → full fan-out");
    }

    // ── Requirement 2: IN over full PKs → deduped union ─────────────────────────

    @Test
    void inListPrunesToDedupedUnionOfCoveringRanges() {
        long t1 = Murmur3Token.token(new byte[] {0, 0, 0, 0x01}); // int 1
        long t2 = Murmur3Token.token(new byte[] {0, 0, 0, 0x02}); // int 2
        // Ring where t1 and t2 fall into distinct ranges.
        long lo = Math.min(t1, t2);
        long hi = Math.max(t1, t2);
        TokenRangeReplicasResponse resp = ringCovering(lo - 1, lo, hi - 1, hi);
        var in = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_INT, Domain.multipleValues(INTEGER, List.of(1L, 2L)))));

        List<CqliteFlightSplit> pruned = prune(INT_PK_TABLE, resp, in);
        assertEquals(2, pruned.size(), "two keys in two ranges → the two-range union");
        assertTrue(pruned.stream().anyMatch(
                s -> Murmur3Token.tokenInRange(t1, s.tokenStart(), s.tokenEnd(), s.wraparound())));
        assertTrue(pruned.stream().anyMatch(
                s -> Murmur3Token.tokenInRange(t2, s.tokenStart(), s.tokenEnd(), s.wraparound())));
    }

    @Test
    void twoInKeysSharingARangeCollapseToOneSplit() {
        long t1 = Murmur3Token.token(new byte[] {0, 0, 0, 0x01});
        long t2 = Murmur3Token.token(new byte[] {0, 0, 0, 0x02});
        long lo = Math.min(t1, t2);
        long hi = Math.max(t1, t2);
        // One wide range [lo-1, hi] that contains BOTH tokens.
        TokenRangeReplicasResponse resp = ringCovering(lo - 1, hi);
        var in = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_INT, Domain.multipleValues(INTEGER, List.of(1L, 2L)))));

        List<CqliteFlightSplit> pruned = prune(INT_PK_TABLE, resp, in);
        assertEquals(1, pruned.size(), "two keys in the same range collapse to one split");
        assertTrue(Murmur3Token.tokenInRange(
                t1, pruned.get(0).tokenStart(), pruned.get(0).tokenEnd(), pruned.get(0).wraparound()));
        assertTrue(Murmur3Token.tokenInRange(
                t2, pruned.get(0).tokenStart(), pruned.get(0).tokenEnd(), pruned.get(0).wraparound()));
    }

    // ── Requirement 3: fail-safe (partitioner / serialization / toggle) ─────────

    @Test
    void unknownPartitionerDisablesPruning() {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(INT_PK_TABLE, resp, "dc1", 8815);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_INT, Domain.singleValue(INTEGER, 42L))));

        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                INT_PK_TABLE, full, constraint, true, Partitioner.UNSUPPORTED);
        assertEquals(full.size(), pruned.size(), "non-Murmur3 partitioner → no pruning (full fan-out)");
    }

    @Test
    void toggleOffDisablesPruning() {
        long token = Murmur3Token.token(new byte[] {0, 0, 0, 0x2A});
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(INT_PK_TABLE, resp, "dc1", 8815);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_INT, Domain.singleValue(INTEGER, 42L))));

        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                INT_PK_TABLE, full, constraint, false, Partitioner.MURMUR3);
        assertEquals(full.size(), pruned.size(), "toggle off forces the unpruned baseline");
    }

    @Test
    void unserializablePkValueDisablesPruning() {
        // A double PK column has no exact partition-key byte serialization here → no prune.
        var doublePk = new CqliteFlightColumnHandle("id", io.trino.spi.type.DoubleType.DOUBLE,
                PushdownCapability.FULL);
        CqliteFlightTableHandle table =
                new CqliteFlightTableHandle("ks", "t", "CREATE TABLE ks.t (id double PRIMARY KEY)");
        TokenRangeReplicasResponse resp = ringCovering(-100L, 0L, 100L);
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(table, resp, "dc1", 8815);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                doublePk, Domain.singleValue(io.trino.spi.type.DoubleType.DOUBLE, 1.5d))));

        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                table, full, constraint, true, Partitioner.MURMUR3);
        assertEquals(full.size(), pruned.size(), "un-serializable PK value → no pruning, never misprune");
    }

    // ── uuid / timeuuid PK: VARCHAR + EQUALITY → 16-byte parse → covering split ─

    // Canonical uuid whose parsed 16-byte big-endian representation is the pinned
    // Murmur3TokenTest vector {0x55,0x0e,0x84,0x00,0xe2,0x9b,0x41,0xd4,0xa7,0x16,0x44,0x66,0x55,0x44,0,0}.
    private static final String CANONICAL_UUID = "550e8400-e29b-41d4-a716-446655440000";
    private static final byte[] CANONICAL_UUID_BYTES =
            bytes(0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00);

    @Test
    void uuidPkFullyBoundPrunesToCoveringSplitAtParsedByteToken() {
        // The token the prune must target is the token of the PARSED 16 bytes, NOT the UTF-8
        // of the hyphenated string — proving the uuid string→16-byte parse path is exercised.
        long token = Murmur3Token.token(CANONICAL_UUID_BYTES);
        assertEquals(4277286421682315655L, token, "pinned uuid vector token (Murmur3TokenTest)");
        // UTF-8-of-the-string token differs → confirms we are not accidentally hashing the string.
        assertTrue(token != Murmur3Token.token(CANONICAL_UUID.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "the parsed-byte token must differ from the UTF-8-of-string token");

        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_UUID, Domain.singleValue(VARCHAR, Slices.utf8Slice(CANONICAL_UUID)))));

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(UUID_PK_TABLE, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = prune(UUID_PK_TABLE, resp, constraint);

        assertTrue(full.size() > 1, "fixture must fan out to many ranges on main");
        assertEquals(1, pruned.size(), "a fully-bound uuid PK prunes to exactly one covering split");
        CqliteFlightSplit s = pruned.get(0);
        assertTrue(Murmur3Token.tokenInRange(token, s.tokenStart(), s.tokenEnd(), s.wraparound()),
                "the emitted split covers the token of the parsed 16-byte uuid");
    }

    @Test
    void timeuuidPkFullyBoundPrunesToCoveringSplitAtParsedByteToken() {
        // timeuuid uses the same VARCHAR+EQUALITY → 16-byte-parse path as uuid.
        long token = Murmur3Token.token(CANONICAL_UUID_BYTES);
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_UUID, Domain.singleValue(VARCHAR, Slices.utf8Slice(CANONICAL_UUID)))));

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(TIMEUUID_PK_TABLE, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = prune(TIMEUUID_PK_TABLE, resp, constraint);

        assertTrue(full.size() > 1, "fixture must fan out to many ranges on main");
        assertEquals(1, pruned.size(), "a fully-bound timeuuid PK prunes to exactly one covering split");
        assertTrue(Murmur3Token.tokenInRange(
                token, pruned.get(0).tokenStart(), pruned.get(0).tokenEnd(), pruned.get(0).wraparound()));
    }

    @Test
    void nonCanonicalUuidStringKeepsFullFanOut() {
        // A garbage/non-canonical uuid string cannot be parsed to 16 bytes → no serialization →
        // fail-safe full fan-out, never a misprune.
        long token = Murmur3Token.token(CANONICAL_UUID_BYTES);
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(UUID_PK_TABLE, resp, "dc1", 8815);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                ID_UUID, Domain.singleValue(VARCHAR, Slices.utf8Slice("not-a-uuid")))));

        assertEquals(full.size(), prune(UUID_PK_TABLE, resp, constraint).size(),
                "a non-canonical uuid string cannot be serialized → full fan-out (never misprune)");
    }

    // ── Composite PK: both components bound → 1 covering split ──────────────────

    @Test
    void compositePkFullyBoundPrunesToCoveringSplit() {
        CqliteFlightColumnHandle pk1 = new CqliteFlightColumnHandle("a", VARCHAR, PushdownCapability.FULL);
        CqliteFlightColumnHandle pk2 = new CqliteFlightColumnHandle("b", INTEGER, PushdownCapability.FULL);
        CqliteFlightTableHandle table = new CqliteFlightTableHandle(
                "ks", "t", "CREATE TABLE ks.t (a text, b int, v text, PRIMARY KEY ((a, b)))");
        // ('hello', 42) → known composite token from the Rust oracle.
        long token = 7666157718303755816L;
        TokenRangeReplicasResponse resp = ringCovering(token - 1, token, token + 1000);
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                pk1, Domain.singleValue(VARCHAR, Slices.utf8Slice("hello")),
                pk2, Domain.singleValue(INTEGER, 42L))));

        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(table, resp, "dc1", 8815);
        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                table, full, constraint, true, Partitioner.MURMUR3);
        assertEquals(1, pruned.size(), "both composite PK components bound → one covering split");
        assertTrue(Murmur3Token.tokenInRange(
                token, pruned.get(0).tokenStart(), pruned.get(0).tokenEnd(), pruned.get(0).wraparound()));
    }

    @Test
    void compositePkPartiallyBoundKeepsFullFanOut() {
        CqliteFlightColumnHandle pk1 = new CqliteFlightColumnHandle("a", VARCHAR, PushdownCapability.FULL);
        CqliteFlightTableHandle table = new CqliteFlightTableHandle(
                "ks", "t", "CREATE TABLE ks.t (a text, b int, v text, PRIMARY KEY ((a, b)))");
        TokenRangeReplicasResponse resp = ringCovering(-100L, 0L, 100L);
        List<CqliteFlightSplit> full = CqliteFlightSplitManager.buildSplits(table, resp, "dc1", 8815);
        // Only 'a' bound; 'b' free → partial PK → no pruning.
        var constraint = summary(TupleDomain.<ColumnHandle>withColumnDomains(Map.of(
                pk1, Domain.singleValue(VARCHAR, Slices.utf8Slice("hello")))));

        List<CqliteFlightSplit> pruned = CqliteFlightSplitManager.pruneToBoundPartitionKey(
                table, full, constraint, true, Partitioner.MURMUR3);
        assertEquals(full.size(), pruned.size(), "partial composite PK → full fan-out");
    }
}
