package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowToTrinoTest {

    @Test
    void convertsScalarBatchToPageWithNulls() {
        try (BufferAllocator allocator = new RootAllocator()) {
            IntVector id = new IntVector("id", allocator);
            VarCharVector name = new VarCharVector("name", allocator);
            BigIntVector big = new BigIntVector("big", allocator);
            DoubleVectorHolder d = new DoubleVectorHolder(allocator);
            BitVector flag = new BitVector("flag", allocator);

            int rows = 3;
            id.allocateNew(rows);
            name.allocateNew();
            big.allocateNew(rows);
            d.vec.allocateNew(rows);
            flag.allocateNew(rows);

            id.set(0, 10);
            id.set(1, 20);
            id.setNull(2);
            name.set(0, "alice".getBytes(StandardCharsets.UTF_8));
            name.setNull(1);
            name.set(2, "carol".getBytes(StandardCharsets.UTF_8));
            big.set(0, 100L);
            big.set(1, 200L);
            big.set(2, 300L);
            d.vec.set(0, 1.5);
            d.vec.set(1, 2.5);
            d.vec.set(2, 3.5);
            flag.set(0, 1);
            flag.set(1, 0);
            flag.set(2, 1);

            var root = new VectorSchemaRoot(List.of(id, name, big, d.vec, flag));
            root.setRowCount(rows);

            var columns = List.of(
                    new CqliteFlightColumnHandle("id", IntegerType.INTEGER),
                    new CqliteFlightColumnHandle("name", VarcharType.VARCHAR),
                    new CqliteFlightColumnHandle("big", BigintType.BIGINT),
                    new CqliteFlightColumnHandle("d", DoubleType.DOUBLE),
                    new CqliteFlightColumnHandle("flag", BooleanType.BOOLEAN));

            Page page = ArrowToTrino.toPage(root, columns);
            assertEquals(3, page.getPositionCount());
            assertEquals(5, page.getChannelCount());

            Block idBlock = page.getBlock(0);
            assertEquals(10, IntegerType.INTEGER.getInt(idBlock, 0));
            assertEquals(20, IntegerType.INTEGER.getInt(idBlock, 1));
            assertTrue(idBlock.isNull(2));

            Block nameBlock = page.getBlock(1);
            assertEquals("alice", VarcharType.VARCHAR.getSlice(nameBlock, 0).toStringUtf8());
            assertTrue(nameBlock.isNull(1));
            assertEquals("carol", VarcharType.VARCHAR.getSlice(nameBlock, 2).toStringUtf8());

            assertEquals(300L, BigintType.BIGINT.getLong(page.getBlock(2), 2));
            assertEquals(2.5, DoubleType.DOUBLE.getDouble(page.getBlock(3), 1));
            assertTrue(BooleanType.BOOLEAN.getBoolean(page.getBlock(4), 0));
            assertFalse(BooleanType.BOOLEAN.getBoolean(page.getBlock(4), 1));

            root.close();
        }
    }

    @Test
    void formatsUuidBytes() {
        byte[] bytes = new byte[16];
        bytes[15] = 1;
        assertEquals("00000000-0000-0000-0000-000000000001", ArrowToTrino.formatUuid(bytes));
    }

    // Small holder so the double vector has a stable field name.
    private static final class DoubleVectorHolder {
        final Float8Vector vec;
        DoubleVectorHolder(BufferAllocator allocator) {
            this.vec = new Float8Vector("d", allocator);
        }
    }
}
