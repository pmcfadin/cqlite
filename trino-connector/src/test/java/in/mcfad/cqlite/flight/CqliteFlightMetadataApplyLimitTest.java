package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CqliteFlightMetadata#applyLimit} directly (issue #2129). No
 * live Sidecar/Flight dependency — applyLimit only rewrites the table handle.
 */
class CqliteFlightMetadataApplyLimitTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CqliteFlightMetadata metadata = new CqliteFlightMetadata(null, null, null);

    private static String aggregationJson() {
        return "{\"group_by\":[],"
                + "\"aggregates\":[{\"func\":\"Count\",\"column\":null,\"output\":\"agg0\"}]}";
    }

    @Test
    void pushesLimitOntoHandleWithLimitNotGuaranteed() {
        ConnectorTableHandle handle = new CqliteFlightTableHandle("ks", "t", "ddl");

        Optional<LimitApplicationResult<ConnectorTableHandle>> applied =
                metadata.applyLimit(null, handle, 5);
        assertTrue(applied.isPresent(), "a plain handle must accept a LIMIT push");

        LimitApplicationResult<ConnectorTableHandle> result = applied.get();
        // Per-split cap → the union across splits can exceed the limit, so Trino
        // must keep its Limit above the scan.
        assertFalse(result.isLimitGuaranteed(), "each split caps independently");
        assertFalse(result.isPrecalculateStatistics());

        CqliteFlightTableHandle newHandle = (CqliteFlightTableHandle) result.getHandle();
        assertEquals(OptionalLong.of(5), newHandle.limit());
    }

    @Test
    void ticketForLimitedHandleCarriesLimitField() throws Exception {
        // The cap on the handle must reach the Flight ticket JSON that the split's
        // page source sends to the server (the wire contract with the Rust side).
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle)
                metadata.applyLimit(null, new CqliteFlightTableHandle("ks", "t", "ddl"), 5)
                        .orElseThrow().getHandle();

        byte[] ticket = FlightTicketJson.build(
                "ks", "t", "ddl",
                Optional.empty(), Optional.of(-100L), Optional.of(100L), false,
                Optional.empty(), java.util.List.of(), null, null, handle.limit());
        var root = MAPPER.readTree(ticket);
        assertTrue(root.has("limit"), "ticket must carry the limit field");
        assertEquals(5, root.get("limit").asLong());
    }

    @Test
    void reappliesIdempotentlyWhenLimitAlreadyPresent() {
        // First push records the cap; a second push with an EQUAL (or larger) cap
        // has nothing to improve → empty, so the optimizer stops iterating.
        ConnectorTableHandle plain = new CqliteFlightTableHandle("ks", "t", "ddl");
        ConnectorTableHandle limited = metadata.applyLimit(null, plain, 5).orElseThrow().getHandle();

        assertTrue(metadata.applyLimit(null, limited, 5).isEmpty(),
                "re-applying the same limit must not loop");
        assertTrue(metadata.applyLimit(null, limited, 10).isEmpty(),
                "a looser limit adds nothing");

        // A strictly tighter limit still improves the plan → pushed.
        Optional<LimitApplicationResult<ConnectorTableHandle>> tighter =
                metadata.applyLimit(null, limited, 2);
        assertTrue(tighter.isPresent(), "a tighter limit is pushed");
        assertEquals(OptionalLong.of(2),
                ((CqliteFlightTableHandle) tighter.get().getHandle()).limit());
    }

    @Test
    void declinesOnAggregatedHandle() {
        // An aggregated handle already collapses the row set; the server ignores
        // limit+aggregation, so a row LIMIT must not be pushed onto it.
        CqliteFlightTableHandle aggregated = new CqliteFlightTableHandle(
                "ks", "t", "ddl",
                Optional.empty(), Optional.of(aggregationJson()),
                Optional.of("{\"group_by\":[],\"outputs\":[]}"));
        assertTrue(aggregated.isAggregated());
        assertTrue(metadata.applyLimit(null, aggregated, 5).isEmpty(),
                "no LIMIT push onto an aggregated handle");
    }

    @Test
    void preservesLimitAcrossApplyFilter() throws Exception {
        // applyLimit then applyFilter must NOT drop the cap when it rebuilds the
        // handle (order-independence of the two pushdowns).
        CqliteFlightTableHandle limited = (CqliteFlightTableHandle)
                metadata.applyLimit(null, new CqliteFlightTableHandle("ks", "t", "ddl"), 5)
                        .orElseThrow().getHandle();

        var col = new CqliteFlightColumnHandle("age",
                io.trino.spi.type.BigintType.BIGINT, PushdownCapability.FULL);
        var predicate = new io.trino.spi.expression.Call(
                io.trino.spi.type.BooleanType.BOOLEAN,
                io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                java.util.List.of(
                        new io.trino.spi.expression.Variable("age", io.trino.spi.type.BigintType.BIGINT),
                        new io.trino.spi.expression.Constant(10L, io.trino.spi.type.BigintType.BIGINT)));
        var constraint = new io.trino.spi.connector.Constraint(
                io.trino.spi.predicate.TupleDomain.all(), predicate,
                java.util.Map.of("age", col));

        var filtered = (CqliteFlightTableHandle)
                metadata.applyFilter(null, limited, constraint).orElseThrow().getHandle();
        assertTrue(filtered.filterJson().isPresent(), "filter pushed");
        assertEquals(OptionalLong.of(5), filtered.limit(), "limit survives applyFilter");
    }
}
