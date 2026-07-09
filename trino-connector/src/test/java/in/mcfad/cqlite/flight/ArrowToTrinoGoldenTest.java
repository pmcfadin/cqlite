package in.mcfad.cqlite.flight;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateTimeEncoding;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /**
     * The pinned CQL {@code time} value from emit_arrow_golden.rs
     * ({@code FIXTURE_TIME_NANOS}): {@code 13:14:15.123456789} as
     * nanoseconds-of-day. Trino encodes TIME as picoseconds-of-day, so the
     * decoded {@code TIME_NANOS} long is this value ×1000.
     */
    private static final long FIXTURE_TIME_NANOS = 47_655_123_456_789L;
    private static final long FIXTURE_TIME_PICOS = FIXTURE_TIME_NANOS * 1_000L;

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
                                "c_double", "c_text", "c_blob", "c_timestamp", "c_date", "c_time", "c_uuid"),
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
                // CQL time → Arrow Time64(Nanosecond) → Trino TIME(9) = TIME_NANOS.
                assertEquals(TimeType.TIME_NANOS, typeOf(columns, "c_time"));
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

                // Schema-level time-unit pin: the server emits CQL time as
                // Time64(Nanosecond). (TimeType above already distinguishes units,
                // but pin the Arrow field directly to mirror the timestamp guard.)
                ArrowType.Time timeType =
                        (ArrowType.Time) fieldOf(schema, "c_time").getType();
                assertEquals(TimeUnit.NANOSECOND, timeType.getUnit(),
                        "server c_time Arrow unit drifted from NANOSECOND");

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
                assertEquals(13, page.getChannelCount());

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
                // TIME is decoded to picoseconds-of-day; assert the EXACT pinned value.
                assertEquals(FIXTURE_TIME_PICOS,
                        TimeType.TIME_NANOS.getLong(block(page, columns, "c_time"), full),
                        "c_time value drifted from the pinned nanosecond-of-day");
                assertEquals(FIXTURE_UUID,
                        VarcharType.VARCHAR.getSlice(block(page, columns, "c_uuid"), full).toStringUtf8());

                // --- The sparse row (id = 2): only id + c_text set, rest null. ---
                assertEquals("only-text",
                        VarcharType.VARCHAR.getSlice(block(page, columns, "c_text"), sparse).toStringUtf8());
                for (String nullable : List.of("c_bool", "c_tinyint", "c_smallint", "c_bigint",
                        "c_float", "c_double", "c_blob", "c_timestamp", "c_date", "c_time", "c_uuid")) {
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

    /**
     * Predicate-push / constant-encoder desync guard (issue #2239, Option A —
     * decouple).
     *
     * <p>The server-declared {@code cqlite:pushdown} capability is OVERLOADED: it
     * gates BOTH predicate pushdown (`PredicateTreeTranslator`) AND value-aggregate
     * pushdown (`CqliteFlightMetadata.supportsValueAggregate`, {@code min}/{@code
     * max}/etc. on {@code FULL}). So it is NOT the connector's predicate-encoder
     * frontier: a type can be {@code FULL} (server-comparable, aggregate-pushable)
     * yet have no {@code constantValue} encoder (e.g. {@code timestamp}). Demoting
     * such a type would silently kill its working aggregate pushdown, so the
     * capability stays {@code FULL} and the predicate simply fails closed.
     *
     * <p>This test therefore guards the real invariant: for every column the server
     * emits, the REAL {@link PredicateTreeTranslator#translate} path must push an
     * {@code Equal} leaf <em>iff</em> the column is capability-pushable AND its
     * constant is {@code constantValue}-encodable. It reads the server's
     * {@code cqlite:pushdown} metadata from the golden and drives the actual
     * translation, so a push path that ever emits a leaf for a type
     * {@code constantValue} cannot encode (a real predicate-push/encoder desync)
     * fails loudly — while {@code timestamp} (FULL, no encoder → not pushed but
     * still aggregate-pushable) is correctly consistent.
     */
    @Test
    void predicatePushPathIsGatedByConstantEncoder() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                InputStream in = getClass().getResourceAsStream("/golden/all_scalars.arrows")) {
            assertNotNull(in, "golden resource /golden/all_scalars.arrows must be on the test classpath");
            try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
                assertTrue(reader.loadNextBatch(), "golden stream must carry at least one record batch");
                Schema schema = reader.getVectorSchemaRoot().getSchema();

                for (Field field : schema.getFields()) {
                    String name = field.getName();
                    PushdownCapability capability = ArrowTypeMapper.capabilityOf(field);
                    Type trinoType = ArrowTypeMapper.toTrino(field);
                    Constant sample = new Constant(sampleValue(name, trinoType), trinoType);

                    boolean encodable = PredicateTreeTranslator.encodeConstantForDriftGuard(sample).isPresent();
                    boolean capabilityPushable = capability != PushdownCapability.NONE;

                    // Drive the REAL predicate translation with `name = sample`.
                    CqliteFlightColumnHandle handle =
                            new CqliteFlightColumnHandle(name, trinoType, capability);
                    ConnectorExpression equal = new Call(
                            BooleanType.BOOLEAN,
                            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                            List.of(new Variable(name, trinoType), sample));
                    boolean pushed = PredicateTreeTranslator
                            .translate(equal, Map.of(name, handle))
                            .pushed()
                            .isPresent();

                    assertTrue(
                            pushAgreesWithEncoder(capabilityPushable, encodable, pushed),
                            "predicate-push / encoder desync for column '" + name + "' ("
                                    + trinoType + ", capability " + capability + "): pushed=" + pushed
                                    + " but capability-pushable=" + capabilityPushable
                                    + " AND constantValue-encodable=" + encodable
                                    + ". A predicate leaf must be pushed IFF the column is pushable and"
                                    + " its constant is encodable (issue #2239). If a push path now emits"
                                    + " a leaf for a type constantValue cannot encode, gate it on"
                                    + " constantValue; a FULL-but-unencodable type (e.g. timestamp, kept"
                                    + " FULL for aggregate pushdown) must fail closed.");
                }
            }
        }
    }

    /**
     * The desync guard has teeth (issue #2239): prove {@link #pushAgreesWithEncoder}
     * FLAGS the real desync class — a predicate push path that emits a leaf for a
     * capability-pushable type whose constant {@code constantValue} cannot encode
     * (push-without-encoder) — while accepting the intentional Option-A shape where
     * a FULL-but-unencodable type (timestamp) correctly fails closed (not pushed).
     */
    @Test
    void desyncGuardFlagsPushWithoutEncoder() {
        // Induced real desync: a pushable column, no encoder, yet a leaf was pushed.
        assertFalse(pushAgreesWithEncoder(true, false, true),
                "a push path emitting a leaf for a type constantValue cannot encode is a desync");
        // Intentional Option-A shape: FULL-but-unencodable (e.g. timestamp) → not pushed.
        assertTrue(pushAgreesWithEncoder(true, false, false),
                "a capability-pushable but unencodable type that is NOT pushed is consistent");
        // The ordinary pushable+encodable+pushed case is consistent.
        assertTrue(pushAgreesWithEncoder(true, true, true));
        // A NONE column that pushed nothing is consistent; if it somehow pushed, flag it.
        assertTrue(pushAgreesWithEncoder(false, false, false));
        assertFalse(pushAgreesWithEncoder(false, true, true),
                "a NONE column must never push a leaf");
    }

    /**
     * The predicate-push contract: a leaf is pushed IFF the column is
     * capability-pushable AND its constant is {@code constantValue}-encodable.
     */
    private static boolean pushAgreesWithEncoder(
            boolean capabilityPushable, boolean encodable, boolean pushed) {
        return pushed == (capabilityPushable && encodable);
    }

    /**
     * A representative internal Trino operand for {@code type}, in the exact Java
     * representation Trino hands the connector (Long-backed integrals/REAL/DATE/
     * TIME/TIMESTAMP_TZ, Slice for VARCHAR/VARBINARY, …). Covers every scalar type
     * the golden emits so the desync guard can drive the real translation for all
     * of them; throws for an unmapped type so a newly-emitted column fails loudly
     * instead of silently skipping.
     */
    private static Object sampleValue(String column, Type type) {
        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            return Slices.utf8Slice(column + "-sample");
        }
        if (type instanceof BigintType || type instanceof IntegerType
                || type instanceof SmallintType || type instanceof TinyintType
                || type instanceof RealType || type instanceof DateType
                || type instanceof TimeType || type instanceof TimestampWithTimeZoneType) {
            // Long-backed operands: integrals/REAL(int bits)/DATE/TIME/TIMESTAMP_TZ.
            return 1L;
        }
        if (type instanceof DoubleType) {
            return 1.0d;
        }
        if (type instanceof BooleanType) {
            return Boolean.TRUE;
        }
        throw new IllegalStateException(
                "no drift-guard sample operand for column '" + column + "' of type " + type
                        + "; add a representative value (issue #2239)");
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
