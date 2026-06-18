package com.rustyrazorblade.cqlite.flight.sidecar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
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
public final class SidecarClient {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final HttpClient http;
    private final URI base;

    /** @param base Sidecar base URI, e.g. {@code http://cassandra:9043}. */
    public SidecarClient(URI base) {
        this.base = base;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
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

    private String get(String path) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(base.resolve(path))
                .timeout(Duration.ofSeconds(30))
                .header("Accept", "application/json")
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new SidecarException(
                        "Sidecar GET " + path + " failed: HTTP " + response.statusCode(),
                        response.statusCode());
            }
            return response.body();
        } catch (IOException e) {
            throw new UncheckedIOException("Sidecar GET " + path + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SidecarException("Sidecar GET " + path + " interrupted");
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

        public int statusCode() {
            return statusCode;
        }
    }
}
