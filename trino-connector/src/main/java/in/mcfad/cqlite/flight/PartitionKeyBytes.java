package in.mcfad.cqlite.flight;

import io.airlift.slice.Slice;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Serializes typed partition-key values to the canonical Cassandra partition-key
 * byte layout (issue #2679), the exact layout the Rust authority
 * {@code PartitionKey::to_bytes} ({@code cqlite-core/src/storage/write_engine/mutation.rs})
 * feeds to {@code cassandra_murmur3_token}:
 *
 * <ul>
 *   <li><b>single-component</b> key → the raw serialized value bytes (no length prefix);</li>
 *   <li><b>multi-component</b> key → for each component
 *       {@code [len:u16 BE][value bytes][0x00]} concatenated.</li>
 * </ul>
 *
 * <p>Per-value serialization mirrors {@code serialize_value_bytes}
 * ({@code cqlite-core/src/storage/partition_key_codec.rs}): integers are big-endian
 * two's-complement, text is UTF-8, blob is raw, boolean is one byte, and a UUID/timeuuid
 * (which the server surfaces as {@code VARCHAR} with {@link PushdownCapability#EQUALITY})
 * is its 16 big-endian bytes parsed from the canonical hyphenated string.
 *
 * <p><b>Fail-safe by construction (no-heuristics):</b> a value whose Trino type has no
 * exact, unambiguous CQL byte serialization here returns {@link Optional#empty()} so the
 * caller disables pruning for that query (full fan-out is always correct) rather than
 * pruning on an approximate token. This is not a correctness gap — it is the mandated
 * conservative default; unsupported types simply do not prune (and are logged upstream).
 */
public final class PartitionKeyBytes {
    /** A component byte length must fit the u16 length prefix of the composite layout. */
    private static final int MAX_COMPONENT_LEN = 0xFFFF;

    private PartitionKeyBytes() {}

    /**
     * Serialize ONE partition-key column's value to its raw CQL value bytes, or empty when
     * the {@code (type, capability)} pair has no exact serialization here (→ disable pruning).
     *
     * @param type       the column's Trino type (from its {@link CqliteFlightColumnHandle})
     * @param capability the server-declared pushdown capability, used only to disambiguate
     *                   {@code VARCHAR} (genuine text = {@code FULL} → UTF-8; uuid/timeuuid =
     *                   {@code EQUALITY} → 16 bytes)
     * @param value      the Trino native domain value (e.g. {@link Slice}, {@link Long},
     *                   {@link Boolean})
     */
    public static Optional<byte[]> serializeValue(Type type, PushdownCapability capability, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (type instanceof BooleanType) {
            return value instanceof Boolean b
                    ? Optional.of(new byte[] {(byte) (b ? 1 : 0)})
                    : Optional.empty();
        }
        if (type instanceof TinyintType) {
            return asLong(value).map(n -> new byte[] {(byte) (long) n});
        }
        if (type instanceof SmallintType) {
            return asLong(value).map(n -> shortBe((short) (long) n));
        }
        if (type instanceof IntegerType) {
            return asLong(value).map(n -> intBe((int) (long) n));
        }
        if (type instanceof BigintType) {
            return asLong(value).map(PartitionKeyBytes::longBe);
        }
        if (type instanceof VarbinaryType) {
            // blob → raw bytes.
            return value instanceof Slice slice ? Optional.of(slice.getBytes()) : Optional.empty();
        }
        if (type instanceof VarcharType) {
            String text = asString(value).orElse(null);
            if (text == null) {
                return Optional.empty();
            }
            if (capability == PushdownCapability.EQUALITY) {
                // uuid / timeuuid: the CQL value bytes are the 16-byte big-endian UUID, NOT the
                // UTF-8 of the hyphenated string. Parse it; a non-UUID string disables pruning.
                return uuidBytes(text);
            }
            if (capability == PushdownCapability.FULL) {
                // genuine text / ascii / varchar → UTF-8 bytes.
                return Optional.of(text.getBytes(StandardCharsets.UTF_8));
            }
            return Optional.empty();
        }
        // Any other type (real, double, date, time, timestamp, decimal, varint, …) has no
        // exact partition-key serialization wired here → fail-safe: no pruning.
        return Optional.empty();
    }

    /**
     * Assemble the full partition-key bytes from per-component value bytes: a single component
     * is its raw bytes; multiple components use the {@code [len:u16 BE][bytes][0x00]} layout.
     * Returns empty if any component exceeds the u16 length the composite layout can encode.
     */
    public static Optional<byte[]> fullKey(List<byte[]> components) {
        if (components.isEmpty()) {
            return Optional.empty();
        }
        if (components.size() == 1) {
            return Optional.of(components.get(0));
        }
        for (byte[] c : components) {
            if (c.length > MAX_COMPONENT_LEN) {
                return Optional.empty();
            }
        }
        return Optional.of(composite(components.toArray(new byte[0][])));
    }

    /**
     * The multi-component composite layout ({@code [len:u16 BE][bytes][0x00]} per component,
     * including the last) — the exact encoding {@code PartitionKey::to_bytes} emits for a
     * composite partition key. Package-visible so token tests can build composite keys directly.
     */
    static byte[] composite(byte[][] components) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] c : components) {
            int len = c.length;
            out.write((len >>> 8) & 0xFF);
            out.write(len & 0xFF);
            out.write(c, 0, c.length);
            out.write(0x00);
        }
        return out.toByteArray();
    }

    private static Optional<Long> asLong(Object value) {
        if (value instanceof Long l) {
            return Optional.of(l);
        }
        if (value instanceof Number n) {
            return Optional.of(n.longValue());
        }
        return Optional.empty();
    }

    private static Optional<String> asString(Object value) {
        if (value instanceof Slice slice) {
            return Optional.of(slice.toStringUtf8());
        }
        if (value instanceof String s) {
            return Optional.of(s);
        }
        return Optional.empty();
    }

    private static Optional<byte[]> uuidBytes(String text) {
        try {
            UUID uuid = UUID.fromString(text.trim());
            byte[] out = new byte[16];
            longToBe(uuid.getMostSignificantBits(), out, 0);
            longToBe(uuid.getLeastSignificantBits(), out, 8);
            return Optional.of(out);
        } catch (IllegalArgumentException e) {
            return Optional.empty(); // not a canonical UUID → disable pruning, never misprune.
        }
    }

    private static byte[] shortBe(short v) {
        return new byte[] {(byte) (v >>> 8), (byte) v};
    }

    private static byte[] intBe(int v) {
        return new byte[] {(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    private static byte[] longBe(long v) {
        byte[] out = new byte[8];
        longToBe(v, out, 0);
        return out;
    }

    private static void longToBe(long v, byte[] out, int offset) {
        for (int i = 0; i < 8; i++) {
            out[offset + i] = (byte) (v >>> (56 - 8 * i));
        }
    }
}
