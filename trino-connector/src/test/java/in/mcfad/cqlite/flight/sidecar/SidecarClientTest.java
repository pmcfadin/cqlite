package in.mcfad.cqlite.flight.sidecar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SidecarClientTest {

    @Test
    void parsesRingResponse() {
        // The ring endpoint returns a bare array of entries.
        String json = """
                [
                  {"datacenter":"dc1","address":"172.42.0.2","port":9042,"rack":"r1",
                   "status":"UP","state":"NORMAL","token":"-9223372036854775808",
                   "fqdn":"cassandra","hostId":"abc","load":"1 GiB","owns":"100%"}
                ]
                """;
        var ring = SidecarClient.parseRing(json);
        assertEquals(1, ring.entries().size());
        var e = ring.entries().get(0);
        assertEquals("dc1", e.datacenter());
        assertEquals("172.42.0.2", e.address());
        assertEquals(9042, e.port());
        assertEquals("UP", e.status());
    }

    @Test
    void parsesTokenRangeReplicasAndTokens() {
        String json = """
                {
                  "writeReplicas": [],
                  "readReplicas": [
                    {"start":"-9223372036854775808","end":"0",
                     "replicasByDatacenter":{"dc1":["172.42.0.2","172.42.0.3"]}},
                    {"start":"0","end":"9223372036854775807",
                     "replicasByDatacenter":{"dc1":["172.42.0.3"]}}
                  ]
                }
                """;
        var resp = SidecarClient.parseTokenRangeReplicas(json);
        assertEquals(2, resp.readReplicas().size());
        var first = resp.readReplicas().get(0);
        assertEquals(Long.MIN_VALUE, first.startToken());
        assertEquals(0L, first.endToken());
        assertEquals(java.util.List.of("172.42.0.2", "172.42.0.3"),
                first.replicasByDatacenter().get("dc1"));
        assertEquals(Long.MAX_VALUE, resp.readReplicas().get(1).endToken());
    }

    @Test
    void parsesSchemaDdl() {
        String json = """
                {"keyspace":"ks","schema":"CREATE TABLE ks.t (id int PRIMARY KEY, v text);"}
                """;
        var schema = SidecarClient.parseSchema(json);
        assertEquals("ks", schema.keyspace());
        assertTrue(schema.schema().contains("CREATE TABLE"));
    }

    @Test
    void unknownFieldsAreIgnored() {
        // Forward-compat: a newer Sidecar adds fields we don't model.
        String json = """
                [{"address":"10.0.0.1","brandNewField":42}]
                """;
        var ring = SidecarClient.parseRing(json);
        assertEquals("10.0.0.1", ring.entries().get(0).address());
    }

    @Test
    void malformedJsonThrows() {
        assertThrows(SidecarClient.SidecarException.class,
                () -> SidecarClient.parseRing("not json"));
    }
}
