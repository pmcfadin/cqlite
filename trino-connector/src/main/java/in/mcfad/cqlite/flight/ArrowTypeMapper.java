package in.mcfad.cqlite.flight;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

/**
 * Maps Arrow schema fields (as produced by the cqlite-flight server's
 * {@code GetSchema}) to Trino column types. Reusing the server's Arrow schema
 * keeps CQL parsing in one place (the Rust core) rather than re-implementing it
 * in Java.
 *
 * <p>The set of types accepted here is kept in lockstep with what
 * {@link ArrowToTrino} can materialize — see {@code ArrowTypeMapperTest} which
 * round-trips every accepted type. v1 supports scalar columns; complex CQL types
 * (collections, UDTs, tuples, decimal) are rejected at planning time with a clear
 * message rather than failing mid-scan.
 */
public final class ArrowTypeMapper {
    /** Arrow extension name the server attaches to uuid/timeuuid columns. */
    private static final String EXTENSION_KEY = "ARROW:extension:name";
    private static final String UUID_EXTENSION = "arrow.uuid";

    /** Metadata key the server uses to declare a column's pushdown capability. */
    private static final String PUSHDOWN_KEY = "cqlite:pushdown";

    private ArrowTypeMapper() {}

    /**
     * Resolve a column's {@link PushdownCapability} from the server-declared
     * {@code cqlite:pushdown} field metadata ({@code "full"}/{@code "equality"}/
     * {@code "none"}). Defaults to {@link PushdownCapability#NONE} when the key is
     * absent or carries an unrecognized value — the safe default, since NONE only
     * ever leaves predicates as a (correct) Trino residual.
     */
    public static PushdownCapability capabilityOf(Field field) {
        String value = field.getMetadata().get(PUSHDOWN_KEY);
        if (value == null) {
            return PushdownCapability.NONE;
        }
        return switch (value) {
            case "full" -> PushdownCapability.FULL;
            case "equality" -> PushdownCapability.EQUALITY;
            default -> PushdownCapability.NONE;
        };
    }

    /**
     * Map one Arrow field to a Trino {@link Type}, throwing when the column's
     * type is not supported. Callers that want to degrade per-column (hide the
     * column rather than fail the whole table) should use {@link #toTrinoOrEmpty}.
     */
    public static Type toTrino(Field field) {
        return toTrinoOrEmpty(field).orElseThrow(() -> new UnsupportedOperationException(
                "Unsupported Arrow type for column '" + field.getName() + "': " + field.getType()
                        + " (the connector currently supports scalar columns)"));
    }

    /**
     * Resolve a column's Trino {@link Type}, or {@link Optional#empty()} when the
     * Arrow type is not one the connector can materialize (decimal, varint,
     * list/set/map, tuple, UDT). {@link CqliteFlightMetadata} uses this to omit
     * unsupported columns from the Trino schema (hide + warn) rather than making
     * the entire table unqueryable.
     */
    public static Optional<Type> toTrinoOrEmpty(Field field) {
        // UUID is carried as FixedSizeBinary(16) tagged with the Arrow UUID
        // extension; surface it as VARCHAR (canonical hyphenated form).
        if (UUID_EXTENSION.equals(field.getMetadata().get(EXTENSION_KEY))) {
            return Optional.of(VarcharType.VARCHAR);
        }

        ArrowType type = field.getType();
        Type mapped = switch (type) {
            case ArrowType.Bool ignored -> BooleanType.BOOLEAN;
            case ArrowType.Int i -> switch (i.getBitWidth()) {
                case 8 -> TinyintType.TINYINT;
                case 16 -> SmallintType.SMALLINT;
                case 32 -> IntegerType.INTEGER;
                default -> BigintType.BIGINT;
            };
            case ArrowType.FloatingPoint fp -> switch (fp.getPrecision()) {
                case SINGLE -> RealType.REAL;
                default -> DoubleType.DOUBLE;
            };
            case ArrowType.Utf8 ignored -> VarcharType.VARCHAR;
            case ArrowType.LargeUtf8 ignored -> VarcharType.VARCHAR;
            case ArrowType.Binary ignored -> VarbinaryType.VARBINARY;
            case ArrowType.FixedSizeBinary ignored -> VarbinaryType.VARBINARY;
            case ArrowType.Date date -> dateType(date);
            // CQL time → Arrow Time (Rust emits Time64(Nanosecond)); Trino has a
            // native TIME whose precision mirrors the Arrow unit.
            case ArrowType.Time t -> timeType(t);
            case ArrowType.Timestamp ts -> timestampType(ts);
            default -> null;
        };
        return Optional.ofNullable(mapped);
    }

    /**
     * Map an Arrow {@code Date} unit to Trino {@link DateType} (epoch-day). Only
     * {@link org.apache.arrow.vector.types.DateUnit#DAY DAY}-unit dates are
     * representable: Trino DATE stores an epoch-day count, which is exactly what a
     * DAY-unit Arrow date (and Cassandra's {@code date} type) already carries.
     *
     * <p>{@code DateMilli} (millisecond-since-epoch) is <b>rejected</b> (returns
     * {@code null} → hidden by {@link #toTrinoOrEmpty} / typed error from
     * {@link #toTrino}). Deriving a calendar day from a millis instant is timezone-
     * ambiguous, so we fail closed rather than silently pick a day — and never let a
     * {@code DateMilliVector} reach {@link ArrowToTrino} where the DAY-typed cast
     * would throw a raw {@code ClassCastException}. The Rust server pins {@code Date32}
     * (DAY); this hardens the Java side against drift.
     */
    private static Type dateType(ArrowType.Date date) {
        return switch (date.getUnit()) {
            case DAY -> DateType.DATE;
            case MILLISECOND -> null;
        };
    }

    /**
     * Map an Arrow {@code Timestamp} unit to Trino {@link TimestampWithTimeZoneType}.
     * The connector materializes timestamps at millisecond precision
     * ({@code TIMESTAMP_TZ_MILLIS}), so:
     * <ul>
     *   <li>{@code MILLISECOND} — identity (the value is already millis; the pinned
     *       server unit).</li>
     *   <li>{@code SECOND} — representable: {@link ArrowToTrino} up-scales ×1000
     *       exactly (overflow-guarded), no precision loss.</li>
     *   <li>{@code MICROSECOND}/{@code NANOSECOND} — <b>rejected</b> (returns
     *       {@code null}). Down-scaling to millis would silently drop sub-millisecond
     *       precision, so we fail closed with a clear typed error rather than
     *       misinterpret the value (a 1000x/1_000_000x error before this fix).</li>
     * </ul>
     */
    private static Type timestampType(ArrowType.Timestamp ts) {
        return switch (ts.getUnit()) {
            case SECOND, MILLISECOND -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
            case MICROSECOND, NANOSECOND -> null;
        };
    }

    /** Map an Arrow {@code Time} unit to the Trino {@link TimeType} of equal precision. */
    private static TimeType timeType(ArrowType.Time time) {
        return switch (time.getUnit()) {
            case SECOND -> TimeType.TIME_SECONDS;
            case MILLISECOND -> TimeType.TIME_MILLIS;
            case MICROSECOND -> TimeType.TIME_MICROS;
            case NANOSECOND -> TimeType.TIME_NANOS;
        };
    }
}
