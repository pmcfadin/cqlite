package com.rustyrazorblade.cqlite.flight;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
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
import org.apache.arrow.vector.SmallIntVector;
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
            case VarcharType t -> t.writeSlice(builder, varcharSlice(vector, i));
            case VarbinaryType t -> t.writeSlice(builder, Slices.wrappedBuffer(((VarBinaryVector) vector).get(i)));
            default -> throw new UnsupportedOperationException(
                    "Unsupported Trino type for Arrow conversion: " + type);
        }
    }

    /** VARCHAR may be backed by Utf8 (text/inet) or FixedSizeBinary(16) (uuid). */
    private static io.airlift.slice.Slice varcharSlice(FieldVector vector, int i) {
        if (vector instanceof VarCharVector v) {
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
