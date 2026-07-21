package in.mcfad.cqlite.flight;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Maps Arrow schema fields (as produced by the cqlite-flight server's
 * {@code GetSchema}) to Trino column types. Reusing the server's Arrow schema
 * keeps CQL parsing in one place (the Rust core) rather than re-implementing it
 * in Java.
 *
 * <p>The set of types accepted here is kept in lockstep with what
 * {@link ArrowToTrino} can materialize — see {@code ArrowTypeMapperTest} which
 * round-trips every accepted type. Scalar columns map to their Trino scalar type;
 * Arrow {@code List}/{@code Struct}/{@code Map} (CQL collections, tuples, UDTs) map
 * recursively to Trino {@code array}/{@code row}/{@code map}, reusing the scalar
 * leaf mapping (issue #2815). A column is rejected at planning time (with a clear
 * message rather than a mid-scan failure) only when a genuine LEAF type is
 * unsupported (decimal, varint, sub-millisecond timestamp, …).
 */
public final class ArrowTypeMapper {
    /** Arrow extension name the server attaches to uuid/timeuuid columns. */
    private static final String EXTENSION_KEY = "ARROW:extension:name";
    private static final String UUID_EXTENSION = "arrow.uuid";

    /** Metadata key the server uses to declare a column's pushdown capability. */
    private static final String PUSHDOWN_KEY = "cqlite:pushdown";

    /**
     * Shared {@link TypeOperators} for constructing Trino {@link MapType}s (its
     * constructor needs one to derive the key type's hash/equal operators).
     * Stateless and thread-safe; a single instance is reused for every mapped map
     * column.
     */
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private ArrowTypeMapper() {}

    /**
     * Resolve a column's {@link PushdownCapability} from the server-declared
     * {@code cqlite:pushdown} field metadata ({@code "full"}/{@code "equality"}/
     * {@code "none"}). Defaults to {@link PushdownCapability#NONE} when the key is
     * absent or carries an unrecognized value — the safe default, since NONE only
     * ever leaves predicates as a (correct) Trino residual.
     */
    public static PushdownCapability capabilityOf(Field field) {
        // Complex columns (list/set → List, tuple/udt → Struct, map → Map) never
        // support predicate/aggregate pushdown into their elements — force NONE
        // regardless of any server-declared value, so a residual predicate is left
        // (correctly) to Trino rather than pushed against a collection the server
        // cannot compare element-wise.
        if (isComplex(field.getType())) {
            return PushdownCapability.NONE;
        }
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

    /** True for the Arrow complex types the connector maps to array/row/map. */
    private static boolean isComplex(ArrowType type) {
        return type instanceof ArrowType.List
                || type instanceof ArrowType.Struct
                || type instanceof ArrowType.Map;
    }

    /**
     * Map one Arrow field to a Trino {@link Type}, throwing when the column's
     * type is not supported. Callers that want to degrade per-column (hide the
     * column rather than fail the whole table) should use {@link #toTrinoOrEmpty}.
     */
    public static Type toTrino(Field field) {
        return toTrinoOrEmpty(field).orElseThrow(() -> new UnsupportedOperationException(
                "Unsupported Arrow type for column '" + field.getName() + "': " + field.getType()
                        + " (an unsupported leaf type — e.g. decimal/varint — reached directly or"
                        + " nested inside a collection/row/map)"));
    }

    /**
     * Resolve a column's Trino {@link Type}, or {@link Optional#empty()} when the
     * Arrow type is not one the connector can materialize. Collections/rows/maps are
     * mapped recursively; empty is returned only when a genuine LEAF type is
     * unsupported (decimal, varint, sub-millisecond timestamp), whether that leaf is
     * reached directly or nested inside a List/Struct/Map. {@link CqliteFlightMetadata}
     * uses this to omit unsupported columns from the Trino schema (hide + warn) rather
     * than making the entire table unqueryable.
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
            // Complex CQL types the server emits as Arrow List/Struct/Map. Each
            // recurses on its child Field(s), reusing this same leaf mapping, and is
            // unsupported only when a genuine LEAF recurses to empty (issue #2815).
            //   list/set → List(element)                → array(E)
            //   tuple/udt → Struct(f1,f2,…)             → row(f1 T1, f2 T2, …)
            //   map      → Map(entries: Struct(key,value)) → map(K, V)
            case ArrowType.List ignored -> listType(field).orElse(null);
            case ArrowType.Struct ignored -> structType(field).orElse(null);
            case ArrowType.Map ignored -> mapType(field).orElse(null);
            default -> null;
        };
        return Optional.ofNullable(mapped);
    }

    /**
     * Map an Arrow {@code List} field to a Trino {@code array(E)}, recursing on its
     * single element child through {@link #toTrinoOrEmpty}. Empty when the element
     * leaf is unmappable (the whole column is then treated as unsupported).
     */
    private static Optional<Type> listType(Field field) {
        List<Field> children = field.getChildren();
        if (children.size() != 1) {
            // A well-formed Arrow List has exactly one element child; anything else
            // is a shape the connector does not model — fail closed.
            return Optional.empty();
        }
        return toTrinoOrEmpty(children.get(0)).map(ArrayType::new);
    }

    /**
     * Map an Arrow {@code Struct} field (CQL tuple/UDT) to a Trino
     * {@code row(name type, …)}, preserving child field NAME and ORDER, recursing on
     * each child. Empty when any child leaf is unmappable, or when the struct has no
     * children (a zero-field row is not representable).
     */
    private static Optional<Type> structType(Field field) {
        List<Field> children = field.getChildren();
        if (children.isEmpty()) {
            return Optional.empty();
        }
        List<RowType.Field> rowFields = new ArrayList<>(children.size());
        for (Field child : children) {
            Optional<Type> childType = toTrinoOrEmpty(child);
            if (childType.isEmpty()) {
                return Optional.empty();
            }
            rowFields.add(RowType.field(child.getName(), childType.get()));
        }
        return Optional.of(RowType.from(rowFields));
    }

    /**
     * Map an Arrow {@code Map} field to a Trino {@code map(K, V)}. Arrow encodes a
     * Map as {@code Map(entries: Struct(key, value))} (see the server's
     * {@code cql_type_to_arrow_field}); this reads the entry struct's {@code key} /
     * {@code value} children and recurses on each. Empty when the shape is unexpected
     * or either the key or value leaf is unmappable.
     */
    private static Optional<Type> mapType(Field field) {
        List<Field> children = field.getChildren();
        if (children.size() != 1) {
            return Optional.empty();
        }
        // The single child is the entries Struct(key, value).
        List<Field> entryChildren = children.get(0).getChildren();
        if (entryChildren.size() != 2) {
            return Optional.empty();
        }
        Optional<Type> keyType = toTrinoOrEmpty(entryChildren.get(0));
        Optional<Type> valueType = toTrinoOrEmpty(entryChildren.get(1));
        if (keyType.isEmpty() || valueType.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MapType(keyType.get(), valueType.get(), TYPE_OPERATORS));
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
