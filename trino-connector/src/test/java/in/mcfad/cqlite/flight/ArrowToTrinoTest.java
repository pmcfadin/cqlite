package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void readJavaValueNormalizesEveryTimeWidthToNanosOfDay() {
        // The aggregation finalize path reads a TIME group/aggregate column via
        // ArrowToTrino.readJavaValue (issue #2229). Every Arrow time width must
        // normalize to the SAME canonical nanoseconds-of-day so GROUP BY keys and
        // min/max longs compare correctly regardless of the mapped precision.
        long secondsOfDay = 13L * 3600 + 14 * 60 + 15; // 13:14:15, exact on all widths
        long nanos = secondsOfDay * 1_000_000_000L;
        try (BufferAllocator allocator = new RootAllocator()) {
            var sec = new org.apache.arrow.vector.TimeSecVector("s", allocator);
            var milli = new org.apache.arrow.vector.TimeMilliVector("ms", allocator);
            var micro = new org.apache.arrow.vector.TimeMicroVector("us", allocator);
            var nano = new org.apache.arrow.vector.TimeNanoVector("ns", allocator);
            sec.allocateNew(1);
            milli.allocateNew(1);
            micro.allocateNew(1);
            nano.allocateNew(1);
            sec.set(0, (int) secondsOfDay);
            milli.set(0, (int) (secondsOfDay * 1_000L));
            micro.set(0, secondsOfDay * 1_000_000L);
            nano.set(0, nanos);

            assertEquals(nanos, ArrowToTrino.readJavaValue(sec, 0));
            assertEquals(nanos, ArrowToTrino.readJavaValue(milli, 0));
            assertEquals(nanos, ArrowToTrino.readJavaValue(micro, 0));
            assertEquals(nanos, ArrowToTrino.readJavaValue(nano, 0));

            sec.close();
            milli.close();
            micro.close();
            nano.close();
        }
    }

    @Test
    void writeJavaValueEncodesTimeNanosAsTrinoPicos() {
        // buildPage in the finalize path writes a merged TIME value (canonical nanos
        // from readJavaValue) into a TimeType block via ArrowToTrino.writeJavaValue.
        // Before #2229 this threw; it must encode picoseconds-of-day (nanos * 1000).
        long nanos = ((13L * 3600 + 14 * 60 + 15) * 1_000_000_000L) + 123_456_789L;
        var type = io.trino.spi.type.TimeType.TIME_NANOS;
        io.trino.spi.block.BlockBuilder builder = type.createBlockBuilder(null, 1);
        ArrowToTrino.writeJavaValue(type, nanos, builder);
        io.trino.spi.block.Block block = builder.build();
        assertEquals(nanos * 1_000L, type.getLong(block, 0), "TIME encoded as picos-of-day");
    }

    @Test
    void timeReadWriteRoundTripsThroughFinalizePathUnits() {
        // Reproduce exactly what accumulate() + buildPage() do to a TIME group
        // column: read the Arrow value, then write it back into a Trino TimeType
        // block. The round-trip must reproduce Trino's picoseconds-of-day encoding.
        long nanos = ((1L * 3600 + 2 * 60 + 3) * 1_000_000_000L) + 456L;
        try (BufferAllocator allocator = new RootAllocator()) {
            var nano = new org.apache.arrow.vector.TimeNanoVector("t", allocator);
            nano.allocateNew(1);
            nano.set(0, nanos);

            Object read = ArrowToTrino.readJavaValue(nano, 0);
            var type = io.trino.spi.type.TimeType.TIME_NANOS;
            io.trino.spi.block.BlockBuilder builder = type.createBlockBuilder(null, 1);
            ArrowToTrino.writeJavaValue(type, read, builder);
            assertEquals(nanos * 1_000L, type.getLong(builder.build(), 0));

            nano.close();
        }
    }

    @Test
    void scanUpScalesSecondUnitTimestampToMillis() {
        // A SECOND-unit timestamp column is mapped to TIMESTAMP_TZ_MILLIS; the scan
        // path must up-scale the raw seconds ×1000 (lossless), not read them as millis
        // — the pre-#2236 code packed the raw long, a 1000x error.
        try (BufferAllocator allocator = new RootAllocator()) {
            var ts = new org.apache.arrow.vector.TimeStampSecTZVector("ts", allocator, "UTC");
            ts.allocateNew(1);
            long epochSeconds = 1_700_000_000L;
            ts.set(0, epochSeconds);

            var root = new VectorSchemaRoot(List.of(ts));
            root.setRowCount(1);
            var columns = List.of(new CqliteFlightColumnHandle(
                    "ts", io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS));

            Page page = ArrowToTrino.toPage(root, columns);
            long packed = io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
                    .getLong(page.getBlock(0), 0);
            assertEquals(epochSeconds * 1_000L,
                    io.trino.spi.type.DateTimeEncoding.unpackMillisUtc(packed));

            root.close();
        }
    }

    @Test
    void scanRejectsMicrosecondUnitTimestampAsTyped() {
        // If the server ever drifts and emits a MICROSECOND timestamp vector into a
        // TIMESTAMP_TZ_MILLIS column, the scan path must fail closed (typed error
        // naming the unit) rather than silently read micros as millis (1000x wrong).
        try (BufferAllocator allocator = new RootAllocator()) {
            var ts = new org.apache.arrow.vector.TimeStampMicroTZVector("ts", allocator, "UTC");
            ts.allocateNew(1);
            ts.set(0, 1_700_000_000_000_000L);

            var root = new VectorSchemaRoot(List.of(ts));
            root.setRowCount(1);
            var columns = List.of(new CqliteFlightColumnHandle(
                    "ts", io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS));

            UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                    () -> ArrowToTrino.toPage(root, columns));
            assertTrue(ex.getMessage().contains("MICROSECOND"), ex.getMessage());

            root.close();
        }
    }

    @Test
    void projectedColumnAbsentFromBatchRaisesNamingTheColumn() {
        // Issue #2238: a column requested in the projection but missing from the
        // delivered Arrow batch (server/connector schema drift) must surface a clear
        // error naming the column — NOT be masked as a silent all-null column.
        try (BufferAllocator allocator = new RootAllocator()) {
            IntVector id = new IntVector("id", allocator);
            id.allocateNew(2);
            id.set(0, 10);
            id.set(1, 20);

            var root = new VectorSchemaRoot(List.of(id));
            root.setRowCount(2);

            // "missing_col" is projected but never delivered in the batch.
            var columns = List.of(
                    new CqliteFlightColumnHandle("id", IntegerType.INTEGER),
                    new CqliteFlightColumnHandle("missing_col", VarcharType.VARCHAR));

            TrinoException ex = assertThrows(TrinoException.class,
                    () -> ArrowToTrino.toPage(root, columns));
            assertTrue(ex.getMessage().contains("missing_col"),
                    "error must name the missing column, was: " + ex.getMessage());

            root.close();
        }
    }

    @Test
    void projectedColumnMissingFirstRaisesNamingTheColumn() {
        // Issue #2270: the missing column is projected FIRST, before the present vectors
        // — the guard must fire on the by-name lookup regardless of projection position,
        // not only when the absent column happens to be last. (Middle position is covered
        // by projectedColumnMissingInMiddleRaisesNamingTheColumn.)
        try (BufferAllocator allocator = new RootAllocator()) {
            IntVector id = new IntVector("id", allocator);
            BigIntVector big = new BigIntVector("big", allocator);
            id.allocateNew(1);
            big.allocateNew(1);
            id.set(0, 10);
            big.set(0, 100L);

            var root = new VectorSchemaRoot(List.of(id, big));
            root.setRowCount(1);

            // "missing_col" is projected FIRST, before the delivered vectors.
            var columns = List.of(
                    new CqliteFlightColumnHandle("missing_col", VarcharType.VARCHAR),
                    new CqliteFlightColumnHandle("id", IntegerType.INTEGER),
                    new CqliteFlightColumnHandle("big", BigintType.BIGINT));

            TrinoException ex = assertThrows(TrinoException.class,
                    () -> ArrowToTrino.toPage(root, columns));
            assertTrue(ex.getMessage().contains("missing_col"),
                    "error must name the missing column, was: " + ex.getMessage());

            root.close();
        }
    }

    @Test
    void projectedColumnMissingInMiddleRaisesNamingTheColumn() {
        // Issue #2270: the missing column is projected in the MIDDLE, between two present
        // vectors — the guard must still fire on the by-name lookup, and toPage must not
        // silently emit an all-null block for it.
        try (BufferAllocator allocator = new RootAllocator()) {
            IntVector id = new IntVector("id", allocator);
            BigIntVector big = new BigIntVector("big", allocator);
            id.allocateNew(1);
            big.allocateNew(1);
            id.set(0, 10);
            big.set(0, 100L);

            var root = new VectorSchemaRoot(List.of(id, big));
            root.setRowCount(1);

            // "missing_col" is projected in the MIDDLE, between the delivered vectors.
            var columns = List.of(
                    new CqliteFlightColumnHandle("id", IntegerType.INTEGER),
                    new CqliteFlightColumnHandle("missing_col", VarcharType.VARCHAR),
                    new CqliteFlightColumnHandle("big", BigintType.BIGINT));

            TrinoException ex = assertThrows(TrinoException.class,
                    () -> ArrowToTrino.toPage(root, columns));
            assertTrue(ex.getMessage().contains("missing_col"),
                    "error must name the missing column, was: " + ex.getMessage());

            root.close();
        }
    }

    @Test
    void emptyBatchWithNonEmptyProjectionRaisesNamingTheFirstColumn() {
        // Issue #2270: a VectorSchemaRoot with ZERO vectors against a non-empty
        // projection must fail loudly (the first projected column is absent), never
        // silently produce all-null blocks.
        try (BufferAllocator allocator = new RootAllocator()) {
            var root = new VectorSchemaRoot(List.<FieldVector>of());
            root.setRowCount(0);

            var columns = List.of(
                    new CqliteFlightColumnHandle("id", IntegerType.INTEGER),
                    new CqliteFlightColumnHandle("name", VarcharType.VARCHAR));

            TrinoException ex = assertThrows(TrinoException.class,
                    () -> ArrowToTrino.toPage(root, columns));
            assertTrue(ex.getMessage().contains("id"),
                    "error must name the missing column, was: " + ex.getMessage());

            root.close();
        }
    }

    @Test
    void readJavaValueRejectsNanosecondUnitTimestampAsTyped() {
        // The aggregation finalize path reads a TIMESTAMP group/aggregate column via
        // readJavaValue; a NANOSECOND vector must be rejected with a typed error, not
        // read raw (1_000_000x wrong).
        try (BufferAllocator allocator = new RootAllocator()) {
            var ts = new org.apache.arrow.vector.TimeStampNanoVector("ts", allocator);
            ts.allocateNew(1);
            ts.set(0, 1_700_000_000_000_000_000L);

            UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                    () -> ArrowToTrino.readJavaValue(ts, 0));
            assertTrue(ex.getMessage().contains("NANOSECOND"), ex.getMessage());

            ts.close();
        }
    }

    @Test
    void readJavaValueUpScalesSecondUnitTimestamp() {
        try (BufferAllocator allocator = new RootAllocator()) {
            var ts = new org.apache.arrow.vector.TimeStampSecVector("ts", allocator);
            ts.allocateNew(1);
            ts.set(0, 1_700_000_000L);

            assertEquals(1_700_000_000_000L, ArrowToTrino.readJavaValue(ts, 0));

            ts.close();
        }
    }

    @Test
    void scanRejectsDateMilliVectorAsTypedNotClassCast() {
        // A DateMilli vector (millis-since-epoch) reaching a DATE column previously
        // threw a raw ClassCastException on the DateDayVector cast. It must now be a
        // clear typed UnsupportedOperationException instead.
        try (BufferAllocator allocator = new RootAllocator()) {
            var date = new org.apache.arrow.vector.DateMilliVector("d", allocator);
            date.allocateNew(1);
            date.set(0, 1_700_000_000_000L);

            var root = new VectorSchemaRoot(List.of(date));
            root.setRowCount(1);
            var columns = List.of(new CqliteFlightColumnHandle(
                    "d", io.trino.spi.type.DateType.DATE));

            UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                    () -> ArrowToTrino.toPage(root, columns));
            assertTrue(ex.getMessage().contains("DATE"), ex.getMessage());

            root.close();
        }
    }

    @Test
    void scanReadsDayUnitDateAsEpochDay() {
        try (BufferAllocator allocator = new RootAllocator()) {
            var date = new org.apache.arrow.vector.DateDayVector("d", allocator);
            date.allocateNew(1);
            date.set(0, 19_000);

            var root = new VectorSchemaRoot(List.of(date));
            root.setRowCount(1);
            var columns = List.of(new CqliteFlightColumnHandle(
                    "d", io.trino.spi.type.DateType.DATE));

            Page page = ArrowToTrino.toPage(root, columns);
            assertEquals(19_000, io.trino.spi.type.DateType.DATE.getInt(page.getBlock(0), 0));

            root.close();
        }
    }

    @Test
    void nullValueWithinPresentVectorStaysNullNotError() {
        // Guardrail (issue #2238): a null CELL within a delivered vector is normal and
        // must still yield an appended null — only an ENTIRELY absent vector errors.
        try (BufferAllocator allocator = new RootAllocator()) {
            IntVector id = new IntVector("id", allocator);
            id.allocateNew(2);
            id.set(0, 10);
            id.setNull(1);

            var root = new VectorSchemaRoot(List.of(id));
            root.setRowCount(2);

            var columns = List.of(new CqliteFlightColumnHandle("id", IntegerType.INTEGER));
            Page page = ArrowToTrino.toPage(root, columns);
            assertEquals(10, IntegerType.INTEGER.getInt(page.getBlock(0), 0));
            assertTrue(page.getBlock(0).isNull(1), "null cell must stay null, not error");

            root.close();
        }
    }

    @Test
    void formatsUuidBytes() {
        byte[] bytes = new byte[16];
        bytes[15] = 1;
        assertEquals("00000000-0000-0000-0000-000000000001", ArrowToTrino.formatUuid(bytes));
    }

    // ---- Complex-type materialization (issue #2815) ------------------------

    @Test
    void listOfTextMaterializesToArrayBlockNullAndEmptyDistinct() {
        // list<text> / list<frozen<udt>> arrive as a ListVector of Utf8. Row 0 has two
        // elements (the multi-UDT case), row 1 is a null list (never set → null block),
        // row 2 is an empty list (→ empty, non-null array).
        try (BufferAllocator allocator = new RootAllocator();
                ListVector addrs = ListVector.empty("addrs", allocator)) {
            UnionListWriter w = addrs.getWriter();
            // Row 0: ["12 Oak St", "9 Elm Ave"] (server-decoded UDT strings).
            w.setPosition(0);
            w.startList();
            w.writeVarChar("12 Oak St");
            w.writeVarChar("9 Elm Ave");
            w.endList();
            // Row 1: null list — do NOT start a list, leave the validity bit clear.
            addrs.setNull(1);
            // Row 2: empty list.
            w.setPosition(2);
            w.startList();
            w.endList();
            addrs.setValueCount(3);

            var root = new VectorSchemaRoot(List.of(addrs));
            root.setRowCount(3);
            var type = new ArrayType(VarcharType.VARCHAR);
            var columns = List.of(new CqliteFlightColumnHandle("addrs", type));

            Page page = ArrowToTrino.toPage(root, columns);
            Block block = page.getBlock(0);

            // Row 0: two VARCHAR elements, in order, equal to the decoded UDT strings.
            assertFalse(block.isNull(0));
            Block elems0 = (Block) type.getObject(block, 0);
            assertEquals(2, elems0.getPositionCount());
            assertEquals("12 Oak St", VarcharType.VARCHAR.getSlice(elems0, 0).toStringUtf8());
            assertEquals("9 Elm Ave", VarcharType.VARCHAR.getSlice(elems0, 1).toStringUtf8());

            // Row 1: never-set → null block entry.
            assertTrue(block.isNull(1), "null list cell must stay null");

            // Row 2: empty list → empty, NON-null array.
            assertFalse(block.isNull(2), "empty list must be non-null");
            Block elems2 = (Block) type.getObject(block, 2);
            assertEquals(0, elems2.getPositionCount(), "empty list → zero elements");

            root.close();
        }
    }

    @Test
    void structMaterializesToRowBlockPreservingFieldOrder() {
        // A tuple/UDT arrives as a StructVector; the ROW block preserves field name+order.
        try (BufferAllocator allocator = new RootAllocator();
                StructVector addr = StructVector.empty("addr", allocator)) {
            NullableStructWriter w = addr.getWriter();
            w.setPosition(0);
            w.start();
            w.varChar("street").writeVarChar("12 Oak St");
            w.integer("zip").writeInt(94040);
            w.end();
            addr.setValueCount(1);

            var root = new VectorSchemaRoot(List.of(addr));
            root.setRowCount(1);
            var type = RowType.from(List.of(
                    RowType.field("street", VarcharType.VARCHAR),
                    RowType.field("zip", IntegerType.INTEGER)));
            var columns = List.of(new CqliteFlightColumnHandle("addr", type));

            Page page = ArrowToTrino.toPage(root, columns);
            Block block = page.getBlock(0);
            SqlRow row = (SqlRow) type.getObject(block, 0);
            int idx = row.getRawIndex();
            assertEquals("12 Oak St",
                    VarcharType.VARCHAR.getSlice(row.getRawFieldBlock(0), idx).toStringUtf8());
            assertEquals(94040, IntegerType.INTEGER.getInt(row.getRawFieldBlock(1), idx));

            root.close();
        }
    }

    @Test
    void mapMaterializesToMapBlockFromEntryStruct() {
        // A map<text,int> arrives as MapVector = List<Struct(key,value)>. The
        // materializer must read the entry struct, not assume parallel vectors.
        try (BufferAllocator allocator = new RootAllocator();
                MapVector m = MapVector.empty("m", allocator, false)) {
            UnionMapWriter w = m.getWriter();
            w.setPosition(0);
            w.startMap();
            w.startEntry();
            w.key().varChar().writeVarChar("a");
            w.value().integer().writeInt(1);
            w.endEntry();
            w.startEntry();
            w.key().varChar().writeVarChar("b");
            w.value().integer().writeInt(2);
            w.endEntry();
            w.endMap();
            m.setValueCount(1);

            var root = new VectorSchemaRoot(List.of(m));
            root.setRowCount(1);
            var type = new MapType(VarcharType.VARCHAR, IntegerType.INTEGER, new TypeOperators());
            var columns = List.of(new CqliteFlightColumnHandle("m", type));

            Page page = ArrowToTrino.toPage(root, columns);
            Block block = page.getBlock(0);
            SqlMap sqlMap = (SqlMap) type.getObject(block, 0);
            assertEquals(2, sqlMap.getSize());
            Block keys = sqlMap.getRawKeyBlock();
            Block values = sqlMap.getRawValueBlock();
            int off = sqlMap.getRawOffset();
            assertEquals("a", VarcharType.VARCHAR.getSlice(keys, off).toStringUtf8());
            assertEquals(1, IntegerType.INTEGER.getInt(values, off));
            assertEquals("b", VarcharType.VARCHAR.getSlice(keys, off + 1).toStringUtf8());
            assertEquals(2, IntegerType.INTEGER.getInt(values, off + 1));

            root.close();
        }
    }

    @Test
    void listFrozenUdtProjectsAndMaterializesThroughMapperAndConverter() {
        // Wiring evidence for the list<frozen<udt>> headline (issue #2815): resolve the
        // column's Trino type from its Arrow FIELD via the REAL ArrowTypeMapper (as the
        // connector does at planning time), then materialize the served List(Utf8) batch
        // via ArrowToTrino.toPage — the same call chain used at scan time. The server
        // emits UDT elements as decoded Utf8 strings, so the column is array(varchar) and
        // each element equals the server-decoded UDT string.
        try (BufferAllocator allocator = new RootAllocator();
                ListVector addrs = ListVector.empty("addrs", allocator)) {
            UnionListWriter w = addrs.getWriter();
            w.setPosition(0);
            w.startList();
            w.writeVarChar("{street: 12 Oak St, zip: 94040}");
            w.writeVarChar("{street: 9 Elm Ave, zip: 94041}");
            w.endList();
            addrs.setValueCount(1);

            // Planning path: resolve the Trino type from the Arrow field.
            io.trino.spi.type.Type resolved = ArrowTypeMapper.toTrino(addrs.getField());
            ArrayType arrayType = (ArrayType) resolved;
            assertEquals(VarcharType.VARCHAR, arrayType.getElementType());

            var root = new VectorSchemaRoot(List.of(addrs));
            root.setRowCount(1);
            var columns = List.of(new CqliteFlightColumnHandle(
                    "addrs", resolved, ArrowTypeMapper.capabilityOf(addrs.getField())));

            // Scan path: materialize.
            Page page = ArrowToTrino.toPage(root, columns);
            Block elems = (Block) arrayType.getObject(page.getBlock(0), 0);
            assertEquals(2, elems.getPositionCount());
            assertEquals("{street: 12 Oak St, zip: 94040}",
                    VarcharType.VARCHAR.getSlice(elems, 0).toStringUtf8());
            assertEquals("{street: 9 Elm Ave, zip: 94041}",
                    VarcharType.VARCHAR.getSlice(elems, 1).toStringUtf8());

            root.close();
        }
    }

    @Test
    void nullElementWithinListMaterializesAsNullEntry() {
        // A null ELEMENT inside a present, non-null list → a null block entry for that
        // element (distinct from a null LIST cell).
        try (BufferAllocator allocator = new RootAllocator();
                ListVector xs = ListVector.empty("xs", allocator)) {
            UnionListWriter w = xs.getWriter();
            w.setPosition(0);
            w.startList();
            w.writeVarChar("present");
            w.varChar().writeNull();
            w.endList();
            xs.setValueCount(1);

            var root = new VectorSchemaRoot(List.of(xs));
            root.setRowCount(1);
            var type = new ArrayType(VarcharType.VARCHAR);
            var columns = List.of(new CqliteFlightColumnHandle("xs", type));

            Page page = ArrowToTrino.toPage(root, columns);
            Block elems = (Block) type.getObject(page.getBlock(0), 0);
            assertEquals(2, elems.getPositionCount());
            assertEquals("present", VarcharType.VARCHAR.getSlice(elems, 0).toStringUtf8());
            assertTrue(elems.isNull(1), "null element must materialize as a null entry");

            root.close();
        }
    }

    // Small holder so the double vector has a stable field name.
    private static final class DoubleVectorHolder {
        final Float8Vector vec;
        DoubleVectorHolder(BufferAllocator allocator) {
            this.vec = new Float8Vector("d", allocator);
        }
    }
}
