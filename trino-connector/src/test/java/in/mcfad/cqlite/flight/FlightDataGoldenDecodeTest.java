package in.mcfad.cqlite.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.arrow.flight.FlightMessageDecoder;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * THE Phase-1 oracle for issue #2193: decode a real Rust-emitted {@code FlightData}
 * message sequence with arrow-java's <em>Flight-level</em> machinery — the SAME
 * {@link org.apache.arrow.flight.ArrowMessage} gRPC marshaller a real
 * {@code FlightClient} runs on the receive path — and assert it decodes cleanly.
 *
 * <p>This closes the exact blind spot that let the field failure survive a green
 * suite: every prior test either used an arrow-RS client (misses cross-stack
 * arrow-rs → arrow-java issues) or read the IPC stream framing via
 * {@code ArrowStreamReader} ({@code ArrowToTrinoGoldenTest}) — a DIFFERENT wire
 * shape from Flight's {@code FlightData} messages, so it cannot see a
 * Flight-framing/interop failure. The golden ({@code /golden/keyvalue.flightdata})
 * is the protobuf-encoded FlightData sequence the server's {@code do_get} emits
 * for the exact field failure shape: a 3-row {@code cassandra_easy_stress.keyvalue}
 * ({@code key text, value text}, 1 pk, 0 ck) carrying the {@code cqlite:pushdown}
 * field metadata, produced by the SAME {@code FlightDataEncoderBuilder} path as
 * production (see {@code cqlite-flight/examples/emit_arrow_golden.rs}).
 *
 * <p>Regenerate the golden with {@code trino-connector/scripts/regen-arrow-golden.sh}.
 */
class FlightDataGoldenDecodeTest {

    /** The 3 pinned value-column strings emit_arrow_golden.rs writes. */
    private static final List<String> EXPECTED_VALUES = List.of("1", "2", "3");

    @Test
    void decodesServerEmittedFlightDataAtFlightLevel() throws Exception {
        List<byte[]> messages = readGoldenMessages();
        assertEquals(2, messages.size(),
                "field-shape golden must be exactly [schema message, record-batch message]");

        try (BufferAllocator allocator = new RootAllocator()) {
            FlightMessageDecoder decoder = new FlightMessageDecoder(allocator);

            // --- Message 0: must Flight-decode as a Schema message. --------------
            Schema schema;
            try (FlightMessageDecoder.Decoded m0 = decoder.parse(messages.get(0))) {
                assertTrue(m0.isSchema(),
                        "message 0 must be a Flight Schema message, was " + m0.messageType());
                schema = m0.asSchema();
            }
            assertEquals(
                    List.of("key", "value"),
                    schema.getFields().stream().map(Field::getName).toList(),
                    "server field order drifted from the field keyvalue shape");
            // The sole non-vanilla element on the wire — the per-field
            // `cqlite:pushdown` metadata — must survive the Flight decode. Both
            // columns are CQL `text`, whose pushdown capability is "full".
            for (Field field : schema.getFields()) {
                assertEquals("full", field.getMetadata().get("cqlite:pushdown"),
                        "cqlite:pushdown metadata missing/drifted on field " + field.getName());
            }

            // --- Message 1: must Flight-decode as a RecordBatch of 3 rows. -------
            List<String> values = new ArrayList<>();
            try (FlightMessageDecoder.Decoded m1 = decoder.parse(messages.get(1))) {
                assertTrue(m1.isRecordBatch(),
                        "message 1 must be a Flight RecordBatch message, was " + m1.messageType());
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                        ArrowRecordBatch batch = m1.asRecordBatch()) {
                    assertEquals(3, batch.getLength(), "field fixture has 3 rows");
                    new VectorLoader(root).load(batch);
                    VarCharVector value = (VarCharVector) root.getVector("value");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        values.add(new String(value.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
            // Row order is the server's token order, so compare the value SET.
            assertEquals(
                    new TreeSet<>(EXPECTED_VALUES),
                    new TreeSet<>(values),
                    "decoded value column drifted from the pinned {1,2,3}");
        }
    }

    /**
     * Split the committed golden into per-message protobuf byte arrays using the
     * generated {@code Flight.FlightData} protobuf class ({@code parseDelimitedFrom}
     * reverses the length-delimited framing the emitter wrote). Each element is the
     * raw serialized bytes of one FlightData message — exactly what gRPC hands the
     * Flight marshaller on the client receive path.
     */
    private static List<byte[]> readGoldenMessages() throws Exception {
        List<byte[]> out = new ArrayList<>();
        try (InputStream in =
                FlightDataGoldenDecodeTest.class.getResourceAsStream("/golden/keyvalue.flightdata")) {
            assertNotNull(in, "golden resource /golden/keyvalue.flightdata must be on the test classpath");
            Flight.FlightData data;
            while ((data = Flight.FlightData.parseDelimitedFrom(in)) != null) {
                out.add(data.toByteArray());
            }
        }
        return out;
    }
}
