package in.mcfad.cqlite.flight;

/**
 * How the cqlite-flight server can push predicates on a given column, declared
 * per Arrow field via the {@code cqlite:pushdown} metadata key.
 *
 * <p>Several CQL types (inet, duration, varint, decimal, …) surface as Arrow
 * UTF-8/other shapes that are indistinguishable from genuine {@code text} by
 * Arrow type alone, and uuid/timeuuid surface as VARCHAR but only support exact
 * match. The server therefore tells the connector each column's capability so
 * pushdown can be gated correctly. {@link #NONE} is the safe default for any
 * absent or unrecognized value.
 */
public enum PushdownCapability {
    /** Nothing can be pushed; every leaf on this column stays a Trino residual. */
    NONE,
    /** Only {@code Equal}, {@code IN}, and {@code IS NULL} are safe to push. */
    EQUALITY,
    /** Every operator (Equal, IN, ordering Gt/Gte/Lt/Lte, Prefix/LIKE, IS NULL) is safe. */
    FULL
}
