package in.mcfad.cqlite.flight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

/**
 * A projected column: its name, resolved Trino type, and the server-declared
 * pushdown capability for predicates on it.
 *
 * <p>{@code capability} gates which operators may be pushed: several CQL types
 * surface as the same Arrow shape as genuine {@code text} (so Arrow type alone
 * cannot distinguish them), and uuid/timeuuid surface as {@code VARCHAR} but
 * support exact match only. See {@link ArrowTypeMapper#capabilityOf} and
 * {@link PushdownCapability}.
 */
public record CqliteFlightColumnHandle(String name, Type type, PushdownCapability capability)
        implements ColumnHandle {

    /** Column with no pushdown capability declared (the safe default). */
    public CqliteFlightColumnHandle(String name, Type type) {
        this(name, type, PushdownCapability.NONE);
    }
}
