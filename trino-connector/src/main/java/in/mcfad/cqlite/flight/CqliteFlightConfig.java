package in.mcfad.cqlite.flight;

import java.net.URI;
import java.util.Map;

/**
 * Connector configuration from the catalog properties file.
 *
 * <pre>
 * connector.name=cqlite_flight
 * cqlite.sidecar-uri=http://cassandra:9043
 * cqlite.flight-port=8815
 * cqlite.local-datacenter=dc1
 * # Aggregation-pushdown gate (issue #893); GROUP BY only — globals always push.
 * cqlite.aggregation-pushdown-group-by=automatic   # automatic | always | never
 * cqlite.aggregation-pushdown-max-group-ratio=0.5   # decline above this groups/rows ratio
 * # Deadline for the optional planning-time table_stats DoAction (issue #944). A slow or
 * # half-open Flight endpoint must degrade to "no estimate", never stall query planning.
 * cqlite.table-stats-timeout-ms=3000
 * </pre>
 */
public record CqliteFlightConfig(
        URI sidecarUri,
        int flightPort,
        String localDatacenter,
        GroupByPushdownPolicy groupByPushdown,
        double maxGroupRatio,
        long tableStatsTimeoutMillis) {

    public static final int DEFAULT_FLIGHT_PORT = 8815;

    /**
     * Default deadline (milliseconds) for the optional planning-time {@code table_stats}
     * DoAction (issue #944). The call runs during query PLANNING, where a slow or
     * half-open Flight endpoint would otherwise stall the planner; a few seconds is enough
     * for a healthy stats RPC and bounds the worst case. On timeout the fetch degrades to
     * "no estimate" (push), exactly like any other fetch failure — it never fails the query.
     */
    public static final long DEFAULT_TABLE_STATS_TIMEOUT_MILLIS = 3000;

    /**
     * Default groups/rows crossover for {@link GroupByPushdownPolicy#AUTOMATIC}. The
     * benefit eval (#841) shows GROUP BY pushdown wins materially while distinct groups
     * stay well under the row count and degrades to break-even-to-loss as they converge;
     * decline once the estimated group count exceeds half the rows.
     */
    public static final double DEFAULT_MAX_GROUP_RATIO = 0.5;

    public CqliteFlightConfig {
        if (maxGroupRatio <= 0.0 || maxGroupRatio > 1.0) {
            throw new IllegalArgumentException(
                    "cqlite.aggregation-pushdown-max-group-ratio must be in (0.0, 1.0], got " + maxGroupRatio);
        }
        if (tableStatsTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "cqlite.table-stats-timeout-ms must be > 0, got " + tableStatsTimeoutMillis);
        }
    }

    public static CqliteFlightConfig fromMap(Map<String, String> config) {
        String sidecar = require(config, "cqlite.sidecar-uri");
        int port = config.containsKey("cqlite.flight-port")
                ? Integer.parseInt(config.get("cqlite.flight-port"))
                : DEFAULT_FLIGHT_PORT;
        String dc = config.get("cqlite.local-datacenter");
        GroupByPushdownPolicy policy =
                GroupByPushdownPolicy.fromConfig(config.get("cqlite.aggregation-pushdown-group-by"));
        double ratio = config.containsKey("cqlite.aggregation-pushdown-max-group-ratio")
                ? Double.parseDouble(config.get("cqlite.aggregation-pushdown-max-group-ratio"))
                : DEFAULT_MAX_GROUP_RATIO;
        long statsTimeoutMs = config.containsKey("cqlite.table-stats-timeout-ms")
                ? Long.parseLong(config.get("cqlite.table-stats-timeout-ms"))
                : DEFAULT_TABLE_STATS_TIMEOUT_MILLIS;
        return new CqliteFlightConfig(URI.create(sidecar), port, dc, policy, ratio, statsTimeoutMs);
    }

    private static String require(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required config property: " + key);
        }
        return value;
    }
}
