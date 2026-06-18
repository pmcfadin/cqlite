package com.rustyrazorblade.cqlite.flight.sidecar;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * Minimal models for the Cassandra Sidecar HTTP API responses this connector
 * needs: cluster ring/topology, per-keyspace token-range replicas, and schema.
 * All records ignore unknown fields so Sidecar version drift does not break us.
 */
public final class SidecarModels {
    private SidecarModels() {}

    /** One node in the ring (subset of Sidecar's RingEntry). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RingEntry(
            String datacenter,
            String address,
            Integer port,
            String rack,
            String status,
            String state,
            String token,
            String fqdn,
            String hostId) {}

    /** Response of GET /api/v1/cassandra/ring (and keyspace-scoped variant). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RingResponse(List<RingEntry> entries) {}

    /**
     * One token range and the replicas that own it, grouped by datacenter.
     * {@code start} is exclusive and {@code end} inclusive (Cassandra convention);
     * both are decimal strings of a signed 64-bit Murmur3 token.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ReplicaInfo(
            String start,
            String end,
            Map<String, List<String>> replicasByDatacenter) {

        public long startToken() {
            return Long.parseLong(start);
        }

        public long endToken() {
            return Long.parseLong(end);
        }
    }

    /** Response of GET /api/v1/keyspaces/{ks}/token-range-replicas. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TokenRangeReplicasResponse(
            List<ReplicaInfo> writeReplicas,
            List<ReplicaInfo> readReplicas) {}

    /** Response of GET /api/v1/keyspaces/{ks}/schema — {@code schema} is CQL DDL. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SchemaResponse(String keyspace, String schema) {}
}
