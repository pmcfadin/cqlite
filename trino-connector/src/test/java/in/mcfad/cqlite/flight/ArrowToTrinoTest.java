package in.mcfad.cqlite.flight;

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
    void convertsTimestampDateRealBinaryAndUuid() {
        try (BufferAllocator allocator = new RootAllocator()) {
            var ts = new org.apache.arrow.vector.TimeStampMilliTZVector("ts", allocator, "UTC");
            var date = new org.apache.arrow.vector.DateDayVector("d", allocator);
            var real = new org.apache.arrow.vector.Float4Vector("r", allocator);
            var bin = new org.apache.arrow.vector.VarBinaryVector("b", allocator);
            var uuid = new org.apache.arrow.vector.FixedSizeBinaryVector("u", allocator, 16);
            ts.allocateNew(1);
            date.allocateNew(1);
            real.allocateNew(1);
            bin.allocateNew();
            uuid.allocateNew(1);

            long millis = 1_700_000_000_000L;
            ts.set(0, millis);
            date.set(0, 19_000); // days since epoch
            real.set(0, 1.5f);
            bin.set(0, new byte[] {1, 2, 3});
            byte[] uuidBytes = new byte[16];
            uuidBytes[15] = 1;
            uuid.set(0, uuidBytes);

            var root = new VectorSchemaRoot(List.of(ts, date, real, bin, uuid));
            root.setRowCount(1);

            var columns = List.of(
                    new CqliteFlightColumnHandle("ts", io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS),
                    new CqliteFlightColumnHandle("d", io.trino.spi.type.DateType.DATE),
                    new CqliteFlightColumnHandle("r", io.trino.spi.type.RealType.REAL),
                    new CqliteFlightColumnHandle("b", io.trino.spi.type.VarbinaryType.VARBINARY),
                    new CqliteFlightColumnHandle("u", VarcharType.VARCHAR));

            Page page = ArrowToTrino.toPage(root, columns);
            assertEquals(1, page.getPositionCount());

            long packed = io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.getLong(page.getBlock(0), 0);
            assertEquals(millis, io.trino.spi.type.DateTimeEncoding.unpackMillisUtc(packed));
            assertEquals(19_000, io.trino.spi.type.DateType.DATE.getInt(page.getBlock(1), 0));
            assertEquals(1.5f, Float.intBitsToFloat(io.trino.spi.type.RealType.REAL.getInt(page.getBlock(2), 0)));
            assertEquals(3, io.trino.spi.type.VarbinaryType.VARBINARY.getSlice(page.getBlock(3), 0).length());
            assertEquals("00000000-0000-0000-0000-000000000001",
                    VarcharType.VARCHAR.getSlice(page.getBlock(4), 0).toStringUtf8());

            root.close();
        }
    }

    @Test
    void convertsTime64NanosToTrinoPicosOfDay() {
        try (BufferAllocator allocator = new RootAllocator()) {
            var time = new org.apache.arrow.vector.TimeNanoVector("t", allocator);
            time.allocateNew(2);
            // 13:14:15.123456789 → nanos-of-day.
            long nanosOfDay = ((13L * 3600 + 14 * 60 + 15) * 1_000_000_000L) + 123_456_789L;
            time.set(0, nanosOfDay);
            time.setNull(1);

            var root = new VectorSchemaRoot(List.of(time));
            root.setRowCount(2);

            var columns = List.of(
                    new CqliteFlightColumnHandle("t", io.trino.spi.type.TimeType.TIME_NANOS));

            Page page = ArrowToTrino.toPage(root, columns);
            assertEquals(2, page.getPositionCount());

            long picos = io.trino.spi.type.TimeType.TIME_NANOS.getLong(page.getBlock(0), 0);
            // Trino TIME is picoseconds of day: nanos * 1000, exact.
            assertEquals(nanosOfDay * 1_000L, picos);
            assertTrue(page.getBlock(0).isNull(1));

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
