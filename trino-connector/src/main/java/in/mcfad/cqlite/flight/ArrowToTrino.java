package in.mcfad.cqlite.flight;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.HexFormat;
import java.util.List;

/**
 * Converts an Arrow {@link VectorSchemaRoot} (one Flight batch) into a Trino
 * {@link Page}, projecting and ordering blocks to match the requested columns.
 */
public final class ArrowToTrino {
    private ArrowToTrino() {}

    public static Page toPage(VectorSchemaRoot root, List<CqliteFlightColumnHandle> columns) {
        int rowCount = root.getRowCount();
        Block[] blocks = new Block[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            CqliteFlightColumnHandle col = columns.get(c);
            FieldVector vector = root.getVector(col.name());
            blocks[c] = toBlock(col.type(), vector, rowCount);
        }
        return new Page(rowCount, blocks);
    }

    private static Block toBlock(Type type, FieldVector vector, int rowCount) {
        BlockBuilder builder = type.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector == null || vector.isNull(i)) {
                builder.appendNull();
                continue;
            }
            writeValue(type, vector, i, builder);
        }
        return builder.build();
    }

    private static void writeValue(Type type, FieldVector vector, int i, BlockBuilder builder) {
        switch (type) {
            case BooleanType t -> t.writeBoolean(builder, ((BitVector) vector).get(i) != 0);
            case TinyintType t -> t.writeLong(builder, ((TinyIntVector) vector).get(i));
            case SmallintType t -> t.writeLong(builder, ((SmallIntVector) vector).get(i));
            case IntegerType t -> t.writeLong(builder, ((IntVector) vector).get(i));
            case DateType t -> t.writeLong(builder, ((DateDayVector) vector).get(i));
            case BigintType t -> t.writeLong(builder, ((BigIntVector) vector).get(i));
            case RealType t -> t.writeLong(builder, Float.floatToRawIntBits(((Float4Vector) vector).get(i)));
            case DoubleType t -> t.writeDouble(builder, ((Float8Vector) vector).get(i));
            case TimestampWithTimeZoneType t -> t.writeLong(builder,
                    DateTimeEncoding.packDateTimeWithZone(
                            ((TimeStampVector) vector).get(i), TimeZoneKey.UTC_KEY));
            case TimeType t -> t.writeLong(builder, timePicos(vector, i));
            case VarcharType t -> t.writeSlice(builder, varcharSlice(vector, i));
            case VarbinaryType t -> t.writeSlice(builder, binarySlice(vector, i));
            default -> throw new UnsupportedOperationException(
                    "Unsupported Trino type for Arrow conversion: " + type);
        }
    }

    /**
     * Read one Arrow value as a plain Java object for connector-side aggregation
     * merging (issue #841). The caller must have already checked {@code !isNull(i)}.
     *
     * <p>Returns: {@code Boolean}; {@code Long} for any integer width (so Count/Sum
     * partials and integer group keys all share one numeric path); {@code Float}/
     * {@code Double} for floating point; {@code String} for VARCHAR-shaped vectors
     * (text/inet, and uuid rendered canonically); {@code byte[]} for binary;
     * {@code Long} (epoch-day) for DATE and (epoch-milli) for TIMESTAMP. These types
     * round-trip back through {@link #writeJavaValue}.
     */
    public static Object readJavaValue(FieldVector vector, int i) {
        return switch (vector) {
            case BitVector v -> v.get(i) != 0;
            case TinyIntVector v -> (long) v.get(i);
            case SmallIntVector v -> (long) v.get(i);
            case IntVector v -> (long) v.get(i);
            case BigIntVector v -> v.get(i);
            case DateDayVector v -> (long) v.get(i);
            case TimeStampVector v -> v.get(i);
            // TIME → canonical nanoseconds-of-day (matches the unit writeJavaValue
            // reads back below). All Arrow time widths normalize to nanos here so
            // min/max longs compare correctly and GROUP BY keys stay equal.
            case TimeSecVector v -> timeNanos(v, i);
            case TimeMilliVector v -> timeNanos(v, i);
            case TimeMicroVector v -> timeNanos(v, i);
            case TimeNanoVector v -> timeNanos(v, i);
            case Float4Vector v -> v.get(i);
            case Float8Vector v -> v.get(i);
            case VarCharVector v -> new String(v.get(i), java.nio.charset.StandardCharsets.UTF_8);
            case LargeVarCharVector v -> new String(v.get(i), java.nio.charset.StandardCharsets.UTF_8);
            case FixedSizeBinaryVector v -> formatUuid(v.getObject(i));
            case VarBinaryVector v -> v.get(i);
            default -> throw new UnsupportedOperationException(
                    "Cannot read Arrow vector " + vector.getClass().getSimpleName() + " for aggregation");
        };
    }

    /**
     * Write a merged Java value (from {@link #readJavaValue} or computed avg) into a
     * Trino {@link BlockBuilder} of the given result {@code type}. Numeric widening
     * is tolerated (e.g. a {@code Long} sum into a DOUBLE column for avg).
     */
    public static void writeJavaValue(Type type, Object value, BlockBuilder builder) {
        switch (type) {
            case BooleanType t -> t.writeBoolean(builder, (Boolean) value);
            case TinyintType t -> t.writeLong(builder, ((Number) value).longValue());
            case SmallintType t -> t.writeLong(builder, ((Number) value).longValue());
            case IntegerType t -> t.writeLong(builder, ((Number) value).longValue());
            case DateType t -> t.writeLong(builder, ((Number) value).longValue());
            case BigintType t -> t.writeLong(builder, ((Number) value).longValue());
            case RealType t -> t.writeLong(builder,
                    Float.floatToRawIntBits(((Number) value).floatValue()));
            case DoubleType t -> t.writeDouble(builder, ((Number) value).doubleValue());
            case TimestampWithTimeZoneType t -> t.writeLong(builder,
                    DateTimeEncoding.packDateTimeWithZone(((Number) value).longValue(), TimeZoneKey.UTC_KEY));
            // The merged value is canonical nanoseconds-of-day (see readJavaValue);
            // Trino TIME wants picoseconds-of-day — the same nanos→picos conversion
            // the scan path uses (timePicos → nanosToPicos).
            case TimeType t -> t.writeLong(builder, nanosToPicos(((Number) value).longValue()));
            case VarcharType t -> t.writeSlice(builder, value instanceof byte[] b
                    ? Slices.wrappedBuffer(b) : Slices.utf8Slice((String) value));
            case VarbinaryType t -> t.writeSlice(builder, value instanceof byte[] b
                    ? Slices.wrappedBuffer(b) : Slices.utf8Slice(value.toString()));
            default -> throw new UnsupportedOperationException(
                    "Unsupported Trino type for merged-aggregate write: " + type);
        }
    }

    /**
     * Canonical connector-side representation of an Arrow time-of-day value:
     * nanoseconds since midnight. Rust emits CQL {@code time} as
     * {@code Time64(Nanosecond)}; the other widths are normalized here so the
     * scan path, aggregation merging, and GROUP BY keys all share one unit
     * regardless of whatever precision {@link ArrowTypeMapper} mapped the column to.
     */
    private static long timeNanos(FieldVector vector, int i) {
        return switch (vector) {
            case TimeNanoVector v -> v.get(i);
            case TimeMicroVector v -> v.get(i) * 1_000L;
            case TimeMilliVector v -> v.get(i) * 1_000_000L;
            case TimeSecVector v -> v.get(i) * 1_000_000_000L;
            default -> throw new UnsupportedOperationException(
                    "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to TIME");
        };
    }

    /** Trino encodes TIME as picoseconds-of-day; nanos→picos is a single ×1000. */
    private static long nanosToPicos(long nanos) {
        return nanos * 1_000L;
    }

    /** Convert an Arrow time-of-day value to Trino's picoseconds-of-day encoding. */
    private static long timePicos(FieldVector vector, int i) {
        return nanosToPicos(timeNanos(vector, i));
    }

    /** VARBINARY may be backed by variable or fixed-size binary vectors. */
    private static io.airlift.slice.Slice binarySlice(FieldVector vector, int i) {
        if (vector instanceof VarBinaryVector v) {
            return Slices.wrappedBuffer(v.get(i));
        }
        if (vector instanceof FixedSizeBinaryVector v) {
            return Slices.wrappedBuffer(v.getObject(i));
        }
        throw new UnsupportedOperationException(
                "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to VARBINARY");
    }

    /** VARCHAR may be backed by Utf8 (text/inet) or FixedSizeBinary(16) (uuid). */
    private static io.airlift.slice.Slice varcharSlice(FieldVector vector, int i) {
        if (vector instanceof VarCharVector v) {
            return Slices.wrappedBuffer(v.get(i));
        }
        if (vector instanceof LargeVarCharVector v) {
            return Slices.wrappedBuffer(v.get(i));
        }
        if (vector instanceof FixedSizeBinaryVector u) {
            return Slices.utf8Slice(formatUuid(u.getObject(i)));
        }
        if (vector instanceof VarBinaryVector v) {
            return Slices.wrappedBuffer(v.get(i));
        }
        throw new UnsupportedOperationException(
                "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to VARCHAR");
    }

    /** Format 16 bytes as a canonical hyphenated UUID. */
    static String formatUuid(byte[] bytes) {
        if (bytes.length != 16) {
            return HexFormat.of().formatHex(bytes);
        }
        String hex = HexFormat.of().formatHex(bytes);
        return hex.substring(0, 8) + "-" + hex.substring(8, 12) + "-" + hex.substring(12, 16)
                + "-" + hex.substring(16, 20) + "-" + hex.substring(20, 32);
    }
}
