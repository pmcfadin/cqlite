package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;
import org.junit.jupiter.api.Test;

import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CqliteFlightMetadata#uniformReplicaCount} — the divisor that
 * turns the physical, replica-summed {@code table_stats} row total into a logical
 * (de-replicated) cardinality (issue #1336).
 *
 * <p>Fixtures are REAL {@code tokenRangeReplicas} JSON decoded through
 * {@link SidecarClient#parseTokenRangeReplicas} (real {@code SidecarModels} parsing),
 * not hand-built records — the divisor must be honest end to end from the wire shape.
 *
 * <p>The count derives ONLY from the authoritative per-range replica lists under the
 * same {@code localDatacenter} scoping as {@code replicaHosts}; it never parses
 * keyspace {@code replication = {...}} strategy strings (no-heuristics mandate #28).
 */
class UniformReplicaCountTest {

    private static TokenRangeReplicasResponse parse(String json) {
        return SidecarClient.parseTokenRangeReplicas(json);
    }

    @Test
    void uniformRf3AcrossRangesCountsThree() {
        // Every range has 3 distinct scoped replicas → divisor 3.
        String json = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"0","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.2:7000","10.0.0.3:7000"]}},
                  {"start":"0","end":"100","replicasByDatacenter":{
                    "dc1":["10.0.0.2:7000","10.0.0.3:7000","10.0.0.4:7000"]}}
                ]}""";

        assertEquals(OptionalInt.of(3),
                CqliteFlightMetadata.uniformReplicaCount(parse(json), "dc1"),
                "every range has 3 distinct scoped replicas → divisor 3");
    }

    @Test
    void multiDcScopesToLocalDatacenterReplicaCount() {
        // dc1 RF=3, dc2 RF=2. With localDatacenter=dc1 the divisor is the dc1-scoped
        // per-range count (3) — matching the host set the stats sum is collected from.
        String json = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"100","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.2:7000","10.0.0.3:7000"],
                    "dc2":["10.1.0.1:7000","10.1.0.2:7000"]}}
                ]}""";

        assertEquals(OptionalInt.of(3),
                CqliteFlightMetadata.uniformReplicaCount(parse(json), "dc1"),
                "localDatacenter=dc1 → dc1-scoped per-range replica count 3");
    }

    @Test
    void multiDcUnsetLocalDatacenterCountsAllDcs() {
        // Same fixture, localDatacenter unset → all-DC scoping → 3 + 2 = 5 distinct hosts.
        String json = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"100","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.2:7000","10.0.0.3:7000"],
                    "dc2":["10.1.0.1:7000","10.1.0.2:7000"]}}
                ]}""";

        assertEquals(OptionalInt.of(5),
                CqliteFlightMetadata.uniformReplicaCount(parse(json), null),
                "no local DC → all-DC distinct replica count 3+2=5");
    }

    @Test
    void nonUniformPerRangeCountsFailClosed() {
        // Range 1 has 3 replicas, range 2 has 2 (topology mid-transition) → empty.
        String json = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"0","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.2:7000","10.0.0.3:7000"]}},
                  {"start":"0","end":"100","replicasByDatacenter":{
                    "dc1":["10.0.0.2:7000","10.0.0.3:7000"]}}
                ]}""";

        assertTrue(CqliteFlightMetadata.uniformReplicaCount(parse(json), "dc1").isEmpty(),
                "differing per-range replica counts must fail closed to empty");
    }

    @Test
    void zeroReplicaRangeFailsClosed() {
        // A range whose local-DC replica list is empty and has no other DC → zero
        // scoped replicas → empty (never divide by zero).
        String json = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"0","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.2:7000"]}},
                  {"start":"0","end":"100","replicasByDatacenter":{}}
                ]}""";

        assertTrue(CqliteFlightMetadata.uniformReplicaCount(parse(json), "dc1").isEmpty(),
                "a range with zero scoped replicas must fail closed to empty");
    }

    @Test
    void duplicateReplicaEntriesWithinARangeAreDeduped() {
        // The same host (port-stripped) appears twice in a range's list → counted once.
        // Two distinct hosts → divisor 2, not 3.
        String json = """
                {"writeReplicas":[],"readReplicas":[
                  {"start":"-100","end":"100","replicasByDatacenter":{
                    "dc1":["10.0.0.1:7000","10.0.0.1:7000","10.0.0.2:7000"]}}
                ]}""";

        assertEquals(OptionalInt.of(2),
                CqliteFlightMetadata.uniformReplicaCount(parse(json), "dc1"),
                "duplicate replica entries within a range are deduped");
    }

    @Test
    void emptyOrNullResponsesFailClosed() {
        assertTrue(CqliteFlightMetadata.uniformReplicaCount(null, "dc1").isEmpty());
        assertTrue(CqliteFlightMetadata.uniformReplicaCount(
                        parse("{\"writeReplicas\":[],\"readReplicas\":[]}"), "dc1").isEmpty(),
                "no ranges → cannot ground a divisor → empty");
    }
}
