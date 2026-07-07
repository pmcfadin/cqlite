package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Identifies a table to scan. Carries the CQL DDL (fetched from Sidecar) so the
 * split manager and page source can pass it to the cqlite-flight server in the
 * Flight ticket without re-querying.
 *
 * <p>{@code filterJson} holds the serialized {@code PredicateExpr} tree pushed
 * down by {@link CqliteFlightMetadata#applyFilter}, or empty when no predicate
 * could be pushed. It is threaded through to the Flight ticket so the server
 * pre-filters rows; any untranslatable predicate stays a Trino residual filter,
 * so results are correct regardless.
 *
 * <p>{@code aggregationJson} holds the serialized {@code aggregation} object
 * pushed down by {@link CqliteFlightMetadata#applyAggregation}, or empty when no
 * aggregation was pushed. When present, the handle is an <em>aggregated</em>
 * handle: {@link CqliteFlightSplitManager#getSplits} returns ONE finalize split
 * that fans out to all token ranges, pulls each range's PARTIAL aggregate, merges
 * them, and emits the fully merged result (Trino does not re-aggregate across
 * splits — see issue #841).
 *
 * <p>{@code limit} holds the row cap pushed down by
 * {@link CqliteFlightMetadata#applyLimit} (issue #2129), or empty. When present,
 * every split's Flight ticket carries it and the server stops its k-way merge
 * after {@code limit} post-filter rows. The cap is applied PER SPLIT, so the
 * connector returns {@code limitGuaranteed = false} and Trino keeps a global
 * {@code Limit} above the scan. It is never set on an aggregated handle (the
 * aggregate already collapses the row set).
 */
public record CqliteFlightTableHandle(
        String keyspace,
        String table,
        String ddl,
        Optional<String> filterJson,
        Optional<String> aggregationJson,
        Optional<String> finalizePlanJson,
        OptionalLong limit)
        implements ConnectorTableHandle {

    /** Convenience constructor for a handle with no pushed-down filter or aggregation. */
    public CqliteFlightTableHandle(String keyspace, String table, String ddl) {
        this(keyspace, table, ddl, Optional.empty(), Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    /** Convenience constructor for a handle carrying a filter but no aggregation. */
    public CqliteFlightTableHandle(String keyspace, String table, String ddl, Optional<String> filterJson) {
        this(keyspace, table, ddl, filterJson, Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    /**
     * Six-arg constructor (no limit) retained so existing call sites that predate
     * LIMIT pushdown compile unchanged; defaults {@code limit} to empty.
     */
    public CqliteFlightTableHandle(
            String keyspace,
            String table,
            String ddl,
            Optional<String> filterJson,
            Optional<String> aggregationJson,
            Optional<String> finalizePlanJson) {
        this(keyspace, table, ddl, filterJson, aggregationJson, finalizePlanJson, OptionalLong.empty());
    }

    /** True when this handle carries a pushed-down aggregation (the finalize-split path). */
    public boolean isAggregated() {
        return aggregationJson.isPresent();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * True when this aggregated handle carries a non-empty GROUP BY (a grouped
     * aggregate), false for a GLOBAL aggregate (no GROUP BY). Reads the {@code
     * group_by} array of the carried {@code aggregationJson} — the authoritative,
     * already-serialized signal {@link CqliteFlightMetadata#applyAggregation} put
     * there — rather than introducing new handle state. Returns {@code false} when
     * the handle is not aggregated or the JSON is absent/unparseable (treated as a
     * global aggregate, i.e. one output row).
     */
    public boolean hasGroupBy() {
        if (aggregationJson.isEmpty()) {
            return false;
        }
        try {
            JsonNode root = MAPPER.readTree(aggregationJson.get());
            JsonNode groupBy = root.get("group_by");
            return groupBy != null && groupBy.isArray() && !groupBy.isEmpty();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            return false;
        }
    }
}
