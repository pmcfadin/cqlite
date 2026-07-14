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
        Optional<String> snapshotTtl,
        long snapshotReuseWindowMillis) {

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

    /**
     * Default snapshot-reuse freshness window (issue #2356/#2306). Within this window a single
     * Sidecar snapshot is REUSED per {@code (keyspace, table)} across queries — one create
     * fan-out (→ one memtable flush per host, #2305/#2306) and a stable resolved path that keeps
     * the flight warm readers warm (#2356). The window is the staleness a Trino/analytics read
     * tolerates: a read may reflect table state up to {@code min(window, time-since-last-
     * generation-change)} old. {@code 0} disables reuse (every query takes a fresh snapshot).
     */
    public static final long DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS = 3_000L;

    /** {@link #DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS} in nanoseconds (the {@link SnapshotManager} unit). */
    public static final long DEFAULT_SNAPSHOT_REUSE_WINDOW_NANOS =
            DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS * 1_000_000L;

    public CqliteFlightConfig {
        if (maxGroupRatio <= 0.0 || maxGroupRatio > 1.0) {
            throw new IllegalArgumentException(
                    "cqlite.aggregation-pushdown-max-group-ratio must be in (0.0, 1.0], got " + maxGroupRatio);
        }
        if (tableStatsTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "cqlite.table-stats-timeout-ms must be > 0, got " + tableStatsTimeoutMillis);
        }
        if (snapshotReuseWindowMillis < 0) {
            throw new IllegalArgumentException(
                    "cqlite.snapshot-reuse-window-ms must be >= 0, got " + snapshotReuseWindowMillis);
        }
        requireRootPerNodeBase(sidecarUri);
    }

    /** The snapshot-reuse freshness window in nanoseconds (the {@link SnapshotManager} unit). */
    public long snapshotReuseWindowNanos() {
        return snapshotReuseWindowMillis * 1_000_000L;
    }

    /**
     * Validate the whole per-host-addressing contract on {@code cqlite.sidecar-uri} up front
     * (issue #2227). Per-host snapshot addressing derives each replica host's Sidecar URI from
     * this base's <b>scheme + port only</b> ({@code HostSnapshotApis.PerHostSidecarClients}),
     * swapping the host and keeping nothing else. Every assumption that derivation (and the db0
     * discovery {@code SidecarClient}) makes about the base must hold at config load, so a
     * misconfiguration fails at catalog load rather than surfacing later at first snapshot use:
     * <ul>
     *   <li>an <b>explicit port</b> ({@code getPort() >= 0}) — the per-host URI is built from it;
     *   <li>a <b>scheme</b> that is present and {@code http}/{@code https} — the derived URI and
     *       the HTTP client both require it;
     *   <li>a non-empty <b>host</b> — the db0 discovery client resolves requests against it;
     *   <li><b>no userinfo</b> — per-host derivation drops it, so credentials would reach db0
     *       discovery but never the per-host snapshot PUTs (silent partial auth);
     *   <li><b>root path, no query, no fragment</b> — all dropped from the derived per-host URI.
     * </ul>
     * {@code HostSnapshotApis.fromBaseUri}/{@code forHost} keep their own port check as
     * defence-in-depth; this is the first failure point. The hostNetwork DaemonSet contract
     * (README) requires a root-path per-node Sidecar base.
     */
    private static void requireRootPerNodeBase(URI base) {
        String scheme = base.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw rejectBase(base, "must have an http or https scheme");
        }
        String host = base.getHost();
        if (host == null || host.isEmpty()) {
            throw rejectBase(base, "must include a host");
        }
        if (base.getPort() < 0) {
            throw rejectBase(base, "must include an explicit port (e.g. http://cassandra:9043) so "
                    + "per-host snapshot addressing can reach each replica's Sidecar");
        }
        if (base.getUserInfo() != null) {
            throw rejectBase(base, "must not carry userinfo/credentials — per-host snapshot "
                    + "addressing keeps only scheme + port, so any credentials would be silently "
                    + "dropped from the per-host snapshot PUTs");
        }
        String path = base.getPath();
        boolean nonRootPath = path != null && !path.isEmpty() && !path.equals("/");
        if (nonRootPath || base.getQuery() != null || base.getFragment() != null) {
            throw rejectBase(base, "must be a root-path per-node Sidecar base URI (e.g. "
                    + "http://cassandra:9043); a base with a path/query/fragment (proxied or "
                    + "non-root) is unsupported");
        }
    }

    /**
     * Build a rejection for an invalid {@code cqlite.sidecar-uri}, naming the property, the
     * constraint, and the offending URI with any userinfo/credentials redacted so a secret in
     * the catalog properties never leaks into logs.
     */
    private static IllegalArgumentException rejectBase(URI base, String constraint) {
        return new IllegalArgumentException(
                "cqlite.sidecar-uri " + constraint + " (hostNetwork DaemonSet contract, see README); got: "
                        + redactUserInfo(base));
    }

    /**
     * Render {@code base} for an error message, replacing any userinfo with {@code ***}.
     * {@code base.toString()} renders the <b>raw</b> (percent-encoded) form, so the match must
     * use {@link URI#getRawUserInfo()} rather than the decoded {@link URI#getUserInfo()} —
     * otherwise encoded credentials (e.g. {@code us%40er:p%23ss}) would fail to match and leak
     * into the exception message verbatim.
     */
    private static String redactUserInfo(URI base) {
        String rawUserInfo = base.getRawUserInfo();
        if (rawUserInfo == null) {
            return base.toString();
        }
        return base.toString().replace(rawUserInfo + "@", "***@");
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
        long reuseWindowMs = config.containsKey("cqlite.snapshot-reuse-window-ms")
                ? Long.parseLong(config.get("cqlite.snapshot-reuse-window-ms"))
                : DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS;
        return new CqliteFlightConfig(
                URI.create(sidecar), port, dc, policy, ratio, statsTimeoutMs, readMode, snapshotTtl,
                reuseWindowMs);
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
