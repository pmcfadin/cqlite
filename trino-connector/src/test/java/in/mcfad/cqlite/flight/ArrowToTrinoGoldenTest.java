package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateTimeEncoding;
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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden round-trip: decode an Arrow IPC stream PRODUCED BY THE RUST FLIGHT
 * SERVER (not a hand-built {@code VectorSchemaRoot}) and feed the resulting
 * {@link VectorSchemaRoot} through the real {@link ArrowTypeMapper#toTrino} +
 * {@link ArrowToTrino#toPage}, asserting the decoded Trino {@link Page}'s values
 * for every scalar type the server emits (issue #2234).
 *
 * <p>Unlike {@link ArrowToTrinoTest} (which builds vectors by hand and so cannot
 * see server-side schema/type drift), this test is a drift guard: the golden's
 * field order, Arrow types, timestamp unit, {@code Date32}, the {@code arrow.uuid}
 * extension, and the per-column {@code cqlite:pushdown} capability metadata all
 * come from the server. If the server's emitted schema drifts from what
 * {@link ArrowTypeMapper} and {@link ArrowToTrino} assume, this test fails.
 *
 * <p>Note that {@link ArrowTypeMapper#toTrino} collapses every Arrow
 * {@code Timestamp} unit to {@code TIMESTAMP_TZ_MILLIS}, so the resolved Trino
 * type is unit-agnostic; the {@code c_timestamp} timestamp-unit pin is therefore
 * asserted directly on the Arrow field ({@code TimeUnit.MILLISECOND}) at the
 * schema level, not merely via the decoded millisecond value.
 *
 * <p>Regenerate the golden deliberately with
 * {@code trino-connector/scripts/regen-arrow-golden.sh}.
 */
class ArrowToTrinoGoldenTest {

    /** Canonical form of the pinned FIXTURE_UUID in emit_arrow_golden.rs. */
    private static final String FIXTURE_UUID = "12345678-9abc-4def-8123-456789abcdef";

    @Test
    void decodesServerEmittedGoldenThroughArrowToTrino() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                InputStream in = getClass().getResourceAsStream("/golden/all_scalars.arrows")) {
            assertNotNull(in, "golden resource /golden/all_scalars.arrows must be on the test classpath");

            try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
                assertTrue(reader.loadNextBatch(), "golden stream must carry at least one record batch");
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Schema schema = root.getSchema();

                // Resolve each column's Trino type AND pushdown capability via the
                // SAME ArrowTypeMapper the connector uses at planning time — so a
                // drifted Arrow type surfaces here as an UnsupportedOperationException,
                // and drifted `cqlite:pushdown` field metadata surfaces as a capability
                // mismatch, not silently at scan time.
                List<CqliteFlightColumnHandle> columns = new ArrayList<>();
                for (Field field : schema.getFields()) {
                    Type trinoType = ArrowTypeMapper.toTrino(field);
                    PushdownCapability capability = ArrowTypeMapper.capabilityOf(field);
                    columns.add(new CqliteFlightColumnHandle(field.getName(), trinoType, capability));
                }

                // Assert the server emitted the expected field order + resolved types.
                assertEquals(
                        List.of("id", "c_bool", "c_tinyint", "c_smallint", "c_bigint", "c_float",
                                "c_double", "c_text", "c_blob", "c_timestamp", "c_date", "c_uuid"),
                        columns.stream().map(CqliteFlightColumnHandle::name).toList(),
                        "server field order drifted");
                assertEquals(IntegerType.INTEGER, typeOf(columns, "id"));
                assertEquals(BooleanType.BOOLEAN, typeOf(columns, "c_bool"));
                assertEquals(TinyintType.TINYINT, typeOf(columns, "c_tinyint"));
                assertEquals(SmallintType.SMALLINT, typeOf(columns, "c_smallint"));
                assertEquals(BigintType.BIGINT, typeOf(columns, "c_bigint"));
                assertEquals(RealType.REAL, typeOf(columns, "c_float"));
                assertEquals(DoubleType.DOUBLE, typeOf(columns, "c_double"));
                assertEquals(VarcharType.VARCHAR, typeOf(columns, "c_text"));
                assertEquals(VarbinaryType.VARBINARY, typeOf(columns, "c_blob"));
                assertEquals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS, typeOf(columns, "c_timestamp"));
                assertEquals(DateType.DATE, typeOf(columns, "c_date"));
                // uuid is FixedSizeBinary(16)+arrow.uuid extension → VARCHAR.
                assertEquals(VarcharType.VARCHAR, typeOf(columns, "c_uuid"));

                // Schema-level timestamp-unit pin: ArrowTypeMapper.toTrino collapses
                // every Timestamp unit to TIMESTAMP_TZ_MILLIS, so the resolved Trino
                // type above cannot catch a unit drift. Assert the server emitted a
                // MILLISECOND-unit Timestamp directly on the Arrow field.
                ArrowType.Timestamp tsType =
                        (ArrowType.Timestamp) fieldOf(schema, "c_timestamp").getType();
                assertEquals(TimeUnit.MILLISECOND, tsType.getUnit(),
                        "server c_timestamp Arrow unit drifted from MILLISECOND");

                // Schema-metadata drift guard: assert the server-declared
                // `cqlite:pushdown` capability the connector will gate pushdown on.
                // The three representative capabilities the fixture's columns span:
                //   c_text (text)   -> FULL, c_uuid (uuid) -> EQUALITY, c_blob -> NONE.
                assertEquals(PushdownCapability.FULL, capabilityOf(columns, "c_text"),
                        "c_text pushdown capability drifted");
                assertEquals(PushdownCapability.EQUALITY, capabilityOf(columns, "c_uuid"),
                        "c_uuid pushdown capability drifted");
                assertEquals(PushdownCapability.NONE, capabilityOf(columns, "c_blob"),
                        "c_blob pushdown capability drifted");

                // Run the real conversion.
                Page page = ArrowToTrino.toPage(root, columns);
                assertEquals(2, page.getPositionCount(), "fixture has 2 rows");
                assertEquals(12, page.getChannelCount());

                // Row order is the server's token order (murmur3 of the pk), NOT
                // id order — locate each fixture row by its `id` value.
                int full = rowOf(page, columns, 1); // fully-populated row (id = 1)
                int sparse = rowOf(page, columns, 2); // sparse row (id = 2)

                // --- The fully-populated row (id = 1): one value per type. -------
                assertTrue(BooleanType.BOOLEAN.getBoolean(block(page, columns, "c_bool"), full));
                assertEquals(-7L, TinyintType.TINYINT.getLong(block(page, columns, "c_tinyint"), full));
                assertEquals(1234L, SmallintType.SMALLINT.getLong(block(page, columns, "c_smallint"), full));
                assertEquals(9_876_543_210L, BigintType.BIGINT.getLong(block(page, columns, "c_bigint"), full));
                assertEquals(2.5f, Float.intBitsToFloat(RealType.REAL.getInt(block(page, columns, "c_float"), full)));
                assertEquals(6.25, DoubleType.DOUBLE.getDouble(block(page, columns, "c_double"), full));
                assertEquals("héllo", VarcharType.VARCHAR.getSlice(block(page, columns, "c_text"), full).toStringUtf8());
                // Assert the EXACT blob bytes (the fixture writes {0xde,0xad,0xbe,0xef}),
                // not just the length — reordered/corrupted same-size bytes must fail.
                assertArrayEquals(new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef},
                        VarbinaryType.VARBINARY.getSlice(block(page, columns, "c_blob"), full).getBytes(),
                        "c_blob bytes drifted");

                long packed = TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.getLong(
                        block(page, columns, "c_timestamp"), full);
                assertEquals(1_700_000_000_000L, DateTimeEncoding.unpackMillisUtc(packed));

                assertEquals(19_000, DateType.DATE.getInt(block(page, columns, "c_date"), full));
                assertEquals(FIXTURE_UUID,
                        VarcharType.VARCHAR.getSlice(block(page, columns, "c_uuid"), full).toStringUtf8());

                // --- The sparse row (id = 2): only id + c_text set, rest null. ---
                assertEquals("only-text",
                        VarcharType.VARCHAR.getSlice(block(page, columns, "c_text"), sparse).toStringUtf8());
                for (String nullable : List.of("c_bool", "c_tinyint", "c_smallint", "c_bigint",
                        "c_float", "c_double", "c_blob", "c_timestamp", "c_date", "c_uuid")) {
                    assertTrue(block(page, columns, nullable).isNull(sparse),
                            "expected null in " + nullable + " for the sparse row");
                }
            }
        }
    }

    /** Find the row position whose `id` column equals {@code id}. */
    private static int rowOf(Page page, List<CqliteFlightColumnHandle> columns, int id) {
        Block idBlock = block(page, columns, "id");
        for (int i = 0; i < page.getPositionCount(); i++) {
            if (!idBlock.isNull(i) && IntegerType.INTEGER.getInt(idBlock, i) == id) {
                return i;
            }
        }
        throw new IllegalArgumentException("no row with id=" + id);
    }

    private static Type typeOf(List<CqliteFlightColumnHandle> columns, String name) {
        return columns.stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow().type();
    }

    private static PushdownCapability capabilityOf(List<CqliteFlightColumnHandle> columns, String name) {
        return columns.stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow().capability();
    }

    private static Field fieldOf(Schema schema, String name) {
        return schema.getFields().stream().filter(f -> f.getName().equals(name)).findFirst().orElseThrow();
    }

    private static Block block(Page page, List<CqliteFlightColumnHandle> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return page.getBlock(i);
            }
        }
        throw new IllegalArgumentException("no such column: " + name);
    }
}
