package org.apache.arrow.flight;

import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Test-only bridge into arrow-java's <em>package-private</em> {@link ArrowMessage}
 * Flight decode path (issue #2193).
 *
 * <p>The field failure ({@code FlightRuntimeException: Failed to read message.})
 * originates in arrow-flight's gRPC marshaller — the exact code a real
 * {@code FlightClient} runs to turn an inbound {@code FlightData} protobuf message
 * into an {@link ArrowMessage} (a bare IPC {@code Message} flatbuffer in the
 * {@code data_header} + a separate {@code data_body}). That marshaller
 * ({@link ArrowMessage#createMarshaller(BufferAllocator)}) is public, but the
 * {@link ArrowMessage} type it decodes to is package-private, so a bridge in this
 * package is the ONLY way to exercise the Flight-level decode offline against
 * committed bytes.
 *
 * <p>This is deliberately NOT {@code ArrowStreamReader}: that reads the IPC
 * stream framing, a DIFFERENT wire shape from Flight's {@code FlightData}
 * messages, and so cannot catch a Flight-framing/interop failure. This bridge
 * decodes with the real Flight marshaller, exposing only public arrow types
 * (Schema / ArrowRecordBatch) to the test.
 */
public final class FlightMessageDecoder {

    private final MethodDescriptor.Marshaller<ArrowMessage> marshaller;

    public FlightMessageDecoder(BufferAllocator allocator) {
        this.marshaller = ArrowMessage.createMarshaller(allocator);
    }

    /**
     * Decode one {@code FlightData} protobuf message (the raw serialized bytes of
     * a single message, exactly as gRPC hands the marshaller on the client receive
     * path) via the real Flight marshaller. Throws whatever the marshaller throws
     * — i.e. reproduces the field decode failure if the bytes are unreadable.
     */
    public Decoded parse(byte[] flightDataProtobuf) {
        ArrowMessage message = marshaller.parse(new ByteArrayInputStream(flightDataProtobuf));
        return new Decoded(message);
    }

    /** A decoded Flight message, exposing only public arrow types to the test. */
    public static final class Decoded implements AutoCloseable {
        private final ArrowMessage message;

        Decoded(ArrowMessage message) {
            this.message = message;
        }

        public boolean isSchema() {
            return message.getMessageType() == ArrowMessage.HeaderType.SCHEMA;
        }

        public boolean isRecordBatch() {
            return message.getMessageType() == ArrowMessage.HeaderType.RECORD_BATCH;
        }

        /** The Flight message type name (e.g. {@code SCHEMA}, {@code RECORD_BATCH}). */
        public String messageType() {
            return message.getMessageType().name();
        }

        /** The decoded Arrow schema (only valid when {@link #isSchema()}). */
        public Schema asSchema() {
            return message.asSchema();
        }

        /** The decoded record batch (only valid when {@link #isRecordBatch()}). */
        public ArrowRecordBatch asRecordBatch() throws IOException {
            return message.asRecordBatch();
        }

        @Override
        public void close() throws Exception {
            message.close();
        }
    }
}
