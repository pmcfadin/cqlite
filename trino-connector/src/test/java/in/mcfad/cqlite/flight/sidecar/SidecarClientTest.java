package in.mcfad.cqlite.flight.sidecar;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SidecarClientTest {

    // ── Snapshot path builders (exact Sidecar v1 wire path, no HTTP) ────────────

    @Test
    void buildsSnapshotRoutePath() {
        // apache/cassandra-sidecar ApiEndpointsV1.SNAPSHOTS_ROUTE.
        assertEquals("/api/v1/keyspaces/ks/tables/t/snapshots/cqlite-q1",
                SidecarClient.snapshotPath("ks", "t", "cqlite-q1"));
    }

    @Test
    void createPathAppendsTtlQueryParam() {
        assertEquals("/api/v1/keyspaces/ks/tables/t/snapshots/cqlite-q1?ttl=6h",
                SidecarClient.snapshotCreatePath("ks", "t", "cqlite-q1", Optional.of("6h")));
    }

    @Test
    void createPathOmitsTtlWhenAbsentOrBlank() {
        String bare = "/api/v1/keyspaces/ks/tables/t/snapshots/cqlite-q1";
        assertEquals(bare, SidecarClient.snapshotCreatePath("ks", "t", "cqlite-q1", Optional.empty()));
        assertEquals(bare, SidecarClient.snapshotCreatePath("ks", "t", "cqlite-q1", Optional.of("  ")));
    }

    @Test
    void rejectsUnsafeSnapshotName() {
        assertThrows(SidecarClient.SidecarException.class,
                () -> SidecarClient.snapshotPath("ks", "t", "../../etc/passwd"));
        assertThrows(SidecarClient.SidecarException.class,
                () -> SidecarClient.snapshotPath("ks", "t", "a?b"));
    }

    @Test
    void rejectsDottedSnapshotName() {
        // The connector's allowlist must not be looser than the server's
        // pathsafe::validate_snapshot (cqlite-flight), which rejects '.' too.
        assertThrows(SidecarClient.SidecarException.class,
                () -> SidecarClient.snapshotPath("ks", "t", "cqlite-q1.bak"));
        assertThrows(SidecarClient.SidecarException.class,
                () -> SidecarClient.snapshotPath("ks", "t", "a.b"));
    }

    // ── Snapshot HTTP round-trip against an in-process fake Sidecar ─────────────

    @Test
    void createAndClearSnapshotIssueCorrectMethodAndPath() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            seen.add(ex.getRequestMethod() + " " + ex.getRequestURI());
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        server.start();
        try {
            SidecarClient client = new SidecarClient(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            client.createSnapshot("ks", "t", "cqlite-q1", Optional.of("6h"));
            client.clearSnapshot("ks", "t", "cqlite-q1");
        } finally {
            server.stop(0);
        }

        assertEquals(List.of(
                "PUT /api/v1/keyspaces/ks/tables/t/snapshots/cqlite-q1?ttl=6h",
                "DELETE /api/v1/keyspaces/ks/tables/t/snapshots/cqlite-q1"), seen);
    }

    @Test
    void createSnapshotFailsClosedOnNon2xx() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            ex.sendResponseHeaders(500, -1);
            ex.close();
        });
        server.start();
        try {
            SidecarClient client = new SidecarClient(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            SidecarClient.SidecarException ex = assertThrows(SidecarClient.SidecarException.class,
                    () -> client.createSnapshot("ks", "t", "cqlite-q1", Optional.empty()));
            assertEquals(500, ex.statusCode());
        } finally {
            server.stop(0);
        }
    }

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
