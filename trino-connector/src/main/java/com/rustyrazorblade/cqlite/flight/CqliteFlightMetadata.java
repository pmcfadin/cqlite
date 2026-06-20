package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.type.BooleanType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.rustyrazorblade.cqlite.flight.sidecar.SidecarClient;
import com.rustyrazorblade.cqlite.flight.sidecar.SidecarModels.RingEntry;

/**
 * Connector metadata backed by Sidecar (DDL discovery) and the cqlite-flight
 * server (Arrow schema → Trino column types via {@link ArrowTypeMapper}).
 */
public class CqliteFlightMetadata implements ConnectorMetadata {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final CqliteFlightClient flight;

    public CqliteFlightMetadata(CqliteFlightConfig config, SidecarClient sidecar, CqliteFlightClient flight) {
        this.config = config;
        this.sidecar = sidecar;
        this.flight = flight;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // Direct queries resolve via getTableHandle; SHOW SCHEMAS enumeration is
        // not wired (Sidecar exposes no keyspace-list endpoint we model yet).
        return List.of();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        try {
            sidecar.schema(schemaName);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        String keyspace = tableName.getSchemaName();
        String table = tableName.getTableName();
        String keyspaceSchema;
        try {
            keyspaceSchema = sidecar.schema(keyspace).schema();
        } catch (SidecarClient.SidecarException e) {
            if (e.statusCode() == 404) {
                return null; // keyspace genuinely not found
            }
            throw e; // a real Sidecar failure must not masquerade as "no table"
        }
        return CreateTableExtractor.extract(keyspaceSchema, keyspace, table)
                .map(ddl -> (ConnectorTableHandle) new CqliteFlightTableHandle(keyspace, table, ddl))
                .orElse(null);
    }

    /**
     * Translate Trino's predicate ({@code constraint.getExpression()}) into a
     * recursive {@code PredicateExpr} tree pushed down in the Flight ticket.
     * Untranslatable parts are returned as a residual expression so Trino keeps
     * post-filtering them — results are always correct; pushdown is a pure
     * optimization.
     *
     * <p>The {@code TupleDomain} summary is returned unchanged (we do not consume
     * it), and partial-AND pushdown leaves the untranslatable conjuncts in the
     * residual expression.
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        CqliteFlightTableHandle table = (CqliteFlightTableHandle) handle;
        ConnectorExpression expression = constraint.getExpression();

        PredicateTreeTranslator.Result result =
                PredicateTreeTranslator.translate(expression, constraint.getAssignments());
        if (result.pushed().isEmpty()) {
            return Optional.empty(); // nothing translatable to push
        }

        // Trino calls applyFilter iteratively, passing only the predicate at the
        // current FilterNode each time — the previously pushed predicate lives on
        // the handle and is NOT re-passed. So ACCUMULATE: combine this call's tree
        // with whatever the handle already carries. Replacing would silently drop
        // an earlier condition whose residual we already reported as satisfied,
        // returning too many rows.
        JsonNode newlyPushed = result.pushed().get();
        Optional<String> existing = table.filterJson();
        String newlyPushedJson = serialize(newlyPushed);

        // Termination guard: if the handle already carries exactly this predicate,
        // re-combining would duplicate it and loop. Return empty (nothing new).
        if (existing.map(newlyPushedJson::equals).orElse(false)) {
            return Optional.empty();
        }

        JsonNode combined = existing.isPresent()
                ? PredicateTreeTranslator.and(parseFilter(existing.get()), newlyPushed)
                : newlyPushed;
        String filterJson = serialize(combined);

        // No net change after combining → don't re-apply.
        if (existing.map(filterJson::equals).orElse(false)) {
            return Optional.empty();
        }

        CqliteFlightTableHandle newHandle = new CqliteFlightTableHandle(
                table.keyspace(), table.table(), table.ddl(), Optional.of(filterJson));

        // The residual expression Trino must still evaluate (the untranslatable
        // conjuncts, ANDed). Empty residual => TRUE (fully pushed).
        ConnectorExpression remainingExpression = residualExpression(result.residual());

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                constraint.getSummary(), // domain returned unchanged; we don't consume it
                remainingExpression,
                false));
    }

    private static String serialize(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize pushed-down filter", e);
        }
    }

    private static JsonNode parseFilter(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Invalid pushed-down filter JSON on table handle", e);
        }
    }

    /** Re-assemble residual conjuncts into a single expression (AND), or TRUE if none. */
    private static ConnectorExpression residualExpression(List<ConnectorExpression> residual) {
        if (residual.isEmpty()) {
            return Constant.TRUE;
        }
        if (residual.size() == 1) {
            return residual.get(0);
        }
        return new Call(BooleanType.BOOLEAN, StandardFunctions.AND_FUNCTION_NAME, List.copyOf(residual));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        Schema schema = arrowSchema((CqliteFlightTableHandle) table);
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (Field field : schema.getFields()) {
            handles.put(field.getName(), new CqliteFlightColumnHandle(
                    field.getName(), ArrowTypeMapper.toTrino(field), ArrowTypeMapper.capabilityOf(field)));
        }
        return handles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle) {
        CqliteFlightColumnHandle column = (CqliteFlightColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        Schema schema = arrowSchema(handle);
        List<ColumnMetadata> columns = new ArrayList<>();
        for (Field field : schema.getFields()) {
            columns.add(new ColumnMetadata(field.getName(), ArrowTypeMapper.toTrino(field)));
        }
        return new ConnectorTableMetadata(new SchemaTableName(handle.keyspace(), handle.table()), columns);
    }

    /** Resolve the table's Arrow schema by asking any flight node's GetSchema. */
    private Schema arrowSchema(CqliteFlightTableHandle handle) {
        RingEntry node = sidecar.ring().entries().stream()
                .filter(e -> e.address() != null)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No Cassandra nodes in the ring"));
        byte[] ticket = FlightTicketJson.build(
                handle.keyspace(), handle.table(), handle.ddl(),
                Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), List.of(), null);
        return flight.getSchema(node.address(), config.flightPort(), ticket);
    }
}
