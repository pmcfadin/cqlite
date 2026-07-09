package in.mcfad.cqlite.flight.sidecar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Thin HTTP client for the Cassandra Sidecar API used for cluster discovery:
 * ring/topology, per-keyspace token-range replicas, and schema DDL.
 *
 * <p>JSON parsing is exposed via static {@code parse*} methods so it can be
 * unit-tested without a live Sidecar.
 */
public final class SidecarClient implements SnapshotApi {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Snapshot names the connector will send to the Sidecar. Restricted to a safe
     * identifier charset so the value can never break out of the URL path or smuggle a
     * query string. The connector only ever generates {@code cqlite-<queryId>}, which is
     * a subset of this — the check is defence-in-depth (no-heuristics / fail-closed).
     * No {@code .}: the server's {@code pathsafe::validate_snapshot} (cqlite-flight)
     * rejects dots too, so this allowlist must not be looser than the server's.
     */
    private static final Pattern SAFE_SNAPSHOT_NAME = Pattern.compile("[A-Za-z0-9_-]+");

    private final HttpClient http;
    private final URI base;

    /** @param base Sidecar base URI, e.g. {@code http://cassandra:9043}. */
    public SidecarClient(URI base) {
        this.base = base;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** The base URI this client resolves requests against (package-visible for tests). */
    URI base() {
        return base;
    }

    /** Cluster ring across all keyspaces. */
    public SidecarModels.RingResponse ring() {
        return parseRing(get("/api/v1/cassandra/ring"));
    }

    /** Token-range to replica mapping for a keyspace. */
    public SidecarModels.TokenRangeReplicasResponse tokenRangeReplicas(String keyspace) {
        return parseTokenRangeReplicas(
                get("/api/v1/keyspaces/" + keyspace + "/token-range-replicas"));
    }

    /** CQL DDL for a keyspace. */
    public SidecarModels.SchemaResponse schema(String keyspace) {
        return parseSchema(get("/api/v1/keyspaces/" + keyspace + "/schema"));
    }

    // ── Snapshot lifecycle (issue #2105) ───────────────────────────────────────

    /**
     * Create (PUT) a snapshot of one table's current SSTable set. Fails closed on any
     * non-2xx / transport error. The Sidecar snapshots route is
     * {@code PUT /api/v1/keyspaces/{keyspace}/tables/{table}/snapshots/{snapshot}}
     * (apache/cassandra-sidecar {@code CreateSnapshotRequest} → {@code SNAPSHOTS_ROUTE}).
     */
    @Override
    public void createSnapshot(String keyspace, String table, String snapshotName, Optional<String> ttl) {
        send("PUT", snapshotCreatePath(keyspace, table, snapshotName, ttl));
    }

    /**
     * Clear (DELETE) a snapshot: {@code DELETE} on the same
     * {@code /api/v1/keyspaces/{keyspace}/tables/{table}/snapshots/{snapshot}} route
     * (apache/cassandra-sidecar {@code ClearSnapshotRequest}).
     */
    @Override
    public void clearSnapshot(String keyspace, String table, String snapshotName) {
        send("DELETE", snapshotPath(keyspace, table, snapshotName));
    }

    /**
     * Build the snapshots-route path (no query string). Path segments are URL-encoded and
     * the snapshot name validated against {@link #SAFE_SNAPSHOT_NAME}. Static + package
     * visible so the exact wire path is unit-testable without HTTP.
     */
    static String snapshotPath(String keyspace, String table, String snapshotName) {
        if (!SAFE_SNAPSHOT_NAME.matcher(snapshotName).matches()) {
            throw new SidecarException("Unsafe snapshot name: '" + snapshotName + "'");
        }
        return "/api/v1/keyspaces/" + seg(keyspace)
                + "/tables/" + seg(table)
                + "/snapshots/" + seg(snapshotName);
    }

    /** {@link #snapshotPath} plus the optional Cassandra 4.1+ {@code ?ttl=} query param. */
    static String snapshotCreatePath(String keyspace, String table, String snapshotName, Optional<String> ttl) {
        String path = snapshotPath(keyspace, table, snapshotName);
        return ttl.filter(t -> !t.isBlank())
                .map(t -> path + "?ttl=" + URLEncoder.encode(t, StandardCharsets.UTF_8))
                .orElse(path);
    }

    private static String seg(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String get(String path) {
        return send("GET", path);
    }

    /** Issue an HTTP request and fail closed on any non-2xx status or transport error. */
    private String send(String method, String path) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(base.resolve(path))
                .timeout(Duration.ofSeconds(30))
                .header("Accept", "application/json")
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new SidecarException(
                        "Sidecar " + method + " " + path + " failed: HTTP " + response.statusCode(),
                        response.statusCode());
            }
            return response.body();
        } catch (IOException e) {
            throw new UncheckedIOException("Sidecar " + method + " " + path + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SidecarException("Sidecar " + method + " " + path + " interrupted");
        }
    }

    // ── Parsing (static, testable without HTTP) ────────────────────────────────

    public static SidecarModels.RingResponse parseRing(String json) {
        // The ring endpoint returns a bare JSON array of entries.
        try {
            List<SidecarModels.RingEntry> entries =
                    MAPPER.readValue(json, new TypeReference<List<SidecarModels.RingEntry>>() {});
            return new SidecarModels.RingResponse(entries);
        } catch (IOException e) {
            throw new SidecarException("Failed to parse Sidecar ring response: " + e.getMessage());
        }
    }

    public static SidecarModels.TokenRangeReplicasResponse parseTokenRangeReplicas(String json) {
        return read(json, SidecarModels.TokenRangeReplicasResponse.class);
    }

    public static SidecarModels.SchemaResponse parseSchema(String json) {
        return read(json, SidecarModels.SchemaResponse.class);
    }

    private static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new SidecarException("Failed to parse Sidecar response: " + e.getMessage());
        }
    }

    /** Unchecked error for Sidecar communication/parsing failures. */
    public static final class SidecarException extends RuntimeException {
        /** HTTP status code, or -1 for non-HTTP failures (parse, etc.). */
        private final int statusCode;

        public SidecarException(String message) {
            this(message, -1);
        }

        public SidecarException(String message, int statusCode) {
            super(message);
            this.statusCode = statusCode;
        }

        /** Preserve the underlying cause (e.g. when re-wrapping a per-host create failure). */
        public SidecarException(String message, int statusCode, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }

        public int statusCode() {
            return statusCode;
        }
    }
}
