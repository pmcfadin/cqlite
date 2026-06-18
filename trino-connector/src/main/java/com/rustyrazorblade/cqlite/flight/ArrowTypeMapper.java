package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

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

    private ArrowTypeMapper() {}

    /** Map one Arrow field to a Trino {@link Type}. */
    public static Type toTrino(Field field) {
        // UUID is carried as FixedSizeBinary(16) tagged with the Arrow UUID
        // extension; surface it as VARCHAR (canonical hyphenated form).
        if (UUID_EXTENSION.equals(field.getMetadata().get(EXTENSION_KEY))) {
            return VarcharType.VARCHAR;
        }

        ArrowType type = field.getType();
        return switch (type) {
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
            case ArrowType.Date ignored -> DateType.DATE;
            case ArrowType.Timestamp ignored -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
            default -> throw new UnsupportedOperationException(
                    "Unsupported Arrow type for column '" + field.getName() + "': " + type
                            + " (the connector currently supports scalar columns)");
        };
    }
}
