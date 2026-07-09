package in.mcfad.cqlite.flight;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * Connector configuration from the catalog properties file.
 *
 * <pre>
 * connector.name=cqlite_flight
 * cqlite.sidecar-uri=http://cassandra:9043
 * cqlite.flight-port=8815
 * cqlite.local-datacenter=dc1
 * # Read mode (issue #2105): snapshot (default; stable file set via a Sidecar snapshot)
 * # or live (current data dir, races compaction).
 * cqlite.read-mode=snapshot                         # snapshot | live
 * # Backstop TTL for per-query snapshots so a coordinator crash can't leak them
 * # (Cassandra auto-drops after this). Blank disables the TTL. Cassandra 4.1+ syntax.
 * cqlite.snapshot-ttl=6h
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
        long tableStatsTimeoutMillis,
        ReadMode readMode,
        Optional<String> snapshotTtl) {

    public static final int DEFAULT_FLIGHT_PORT = 8815;

    /**
     * Default backstop TTL for per-query snapshots (issue #2105). Explicit best-effort
     * cleanup runs at query end, but a coordinator crash between snapshot-create and
     * cleanup would otherwise leak the snapshot forever; a Sidecar-side TTL lets
     * Cassandra auto-drop it. Uses Cassandra 4.1+ TTL syntax ({@code d}/{@code h}/
     * {@code m}/{@code s} units). Set {@code cqlite.snapshot-ttl} blank to disable.
     */
    public static final String DEFAULT_SNAPSHOT_TTL = "6h";

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
        requireRootPerNodeBase(sidecarUri);
    }

    /**
     * Per-host snapshot addressing (issue #2227) derives each replica host's Sidecar URI from
     * this base's scheme + port with the host swapped, keeping nothing else. Any path, query,
     * or fragment on the base would therefore be silently dropped from the per-host snapshot
     * create/clear PUTs, sending them to the wrong endpoint. Reject a non-root base up front so
     * a proxied/non-root Sidecar URI fails at config time, not as a silent snapshot-mode failure.
     * The hostNetwork DaemonSet contract (README) requires a root-path per-node Sidecar base.
     */
    private static void requireRootPerNodeBase(URI base) {
        String path = base.getPath();
        boolean nonRootPath = path != null && !path.isEmpty() && !path.equals("/");
        if (nonRootPath || base.getQuery() != null || base.getFragment() != null) {
            throw new IllegalArgumentException(
                    "cqlite.sidecar-uri must be a root-path per-node Sidecar base URI (e.g. "
                            + "http://cassandra:9043) so per-host snapshot addressing can reach each "
                            + "replica's Sidecar (hostNetwork DaemonSet contract, see README); a base with a "
                            + "path/query/fragment (proxied or non-root) is unsupported, got: " + base);
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
        ReadMode readMode = ReadMode.fromConfig(config.get("cqlite.read-mode"));
        Optional<String> snapshotTtl = parseSnapshotTtl(config.get("cqlite.snapshot-ttl"));
        return new CqliteFlightConfig(
                URI.create(sidecar), port, dc, policy, ratio, statsTimeoutMs, readMode, snapshotTtl);
    }

    /**
     * A snapshot TTL of {@code null} (key absent) → the default backstop; a present but
     * blank value → no TTL (empty); otherwise the given duration string, passed through
     * verbatim to the Sidecar create-snapshot {@code ?ttl=} query parameter.
     */
    private static Optional<String> parseSnapshotTtl(String value) {
        if (value == null) {
            return Optional.of(DEFAULT_SNAPSHOT_TTL);
        }
        return value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }

    private static String require(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required config property: " + key);
        }
        return value;
    }
}
