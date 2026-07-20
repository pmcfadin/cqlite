package in.mcfad.cqlite.flight;

/**
 * Cassandra-compatible Murmur3 partition-token computation (issue #2679), a
 * byte-exact Java port of the Rust authority
 * {@code cqlite-core/src/util/cassandra_murmur3.rs}
 * ({@code cassandra_murmur3_x64_128} + {@code cassandra_murmur3_token} +
 * {@code normalize}).
 *
 * <p>This preserves Cassandra's {@code MurmurHash.hash3_x64_128} sign-extension
 * behavior in tail-byte processing (Java's {@code byte} is signed, so
 * {@code (long) b} sign-extends exactly as Cassandra does), which diverges from
 * standard Murmur3 for inputs containing bytes {@code >= 0x80}. The token is
 * {@code h1} after {@code normalize} ({@link Long#MIN_VALUE} maps to
 * {@link Long#MAX_VALUE}, the minimum token value being excluded from the ring).
 *
 * <p>Used only for plan-time split pruning: computing the covering token range(s)
 * for a fully-bound partition key so the split manager emits splits for those
 * range(s) instead of the full ~48-way fan-out. Correctness is pinned to the Rust
 * implementation by shared test vectors (see {@code Murmur3TokenTest}).
 */
public final class Murmur3Token {
    private static final long C1 = 0x87c37b911142_53d5L;
    private static final long C2 = 0x4cf5ad4327_45937fL;
    private static final long H1_ADD = 0x52dce729L;
    private static final long H2_ADD = 0x38495ab5L;
    private static final long FMIX_C1 = 0xff51afd7ed558ccdL;
    private static final long FMIX_C2 = 0xc4ceb9fe1a85ec53L;

    private Murmur3Token() {}

    /**
     * Compute the normalized Cassandra Murmur3 partition token for the given
     * partition-key bytes (the canonical {@code PartitionKey::to_bytes} layout).
     */
    public static long token(byte[] data) {
        return normalize(hash3X64128H1(data));
    }

    /**
     * Cassandra's {@code Murmur3Partitioner} token normalization: {@link Long#MIN_VALUE}
     * maps to {@link Long#MAX_VALUE} (the minimum token value is excluded from the ring).
     */
    public static long normalize(long h1) {
        return h1 == Long.MIN_VALUE ? Long.MAX_VALUE : h1;
    }

    /**
     * Half-open {@code (start, end]} token membership with optional ring wraparound,
     * mirroring the server's {@code token_in_half_open_range}
     * ({@code cqlite-flight/src/ticket.rs}) and the ring convention encoded in
     * {@code CqliteFlightSplitManager.validateRingCoverage}. Equal endpoints
     * ({@code start == end}) denote the FULL ring (issue #2228), accepting every token.
     * A wraparound range ({@code start > end}) keeps {@code token > start || token <= end};
     * a normal range keeps {@code start < token <= end}.
     */
    public static boolean tokenInRange(long token, long start, long end, boolean wraparound) {
        if (start == end) {
            return true;
        }
        return wraparound ? (token > start || token <= end) : (token > start && token <= end);
    }

    /** The {@code h1} 64-bit word of Cassandra's {@code MurmurHash.hash3_x64_128} (seed 0). */
    private static long hash3X64128H1(byte[] data) {
        long h1 = 0;
        long h2 = 0;
        int len = data.length;
        int nblocks = len / 16;

        // Body: 16-byte little-endian blocks.
        for (int i = 0; i < nblocks; i++) {
            int base = i * 16;
            long k1 = getLongLE(data, base);
            long k2 = getLongLE(data, base + 8);

            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + H1_ADD;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + H2_ADD;
        }

        // Tail: remaining bytes, sign-extended (Java byte is signed → matches Cassandra).
        long k1 = 0;
        long k2 = 0;
        int tailStart = nblocks * 16;
        int tailLen = len - tailStart;
        for (int pos = 0; pos < tailLen; pos++) {
            long b = data[tailStart + pos]; // implicit sign extension
            int shift = (pos % 8) * 8;
            if (pos < 8) {
                k1 ^= b << shift;
            } else {
                k2 ^= b << shift;
            }
        }
        if (tailLen >= 9) {
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;
        }
        if (tailLen > 0) {
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;
        }

        // Finalization.
        h1 ^= len;
        h2 ^= len;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        // h2 += h1;  // not needed — the token is h1 only.
        return h1;
    }

    /** Read 8 bytes at {@code offset} as a little-endian {@code long}. */
    private static long getLongLE(byte[] data, int offset) {
        return (data[offset] & 0xffL)
                | (data[offset + 1] & 0xffL) << 8
                | (data[offset + 2] & 0xffL) << 16
                | (data[offset + 3] & 0xffL) << 24
                | (data[offset + 4] & 0xffL) << 32
                | (data[offset + 5] & 0xffL) << 40
                | (data[offset + 6] & 0xffL) << 48
                | (data[offset + 7] & 0xffL) << 56;
    }

    private static long fmix64(long value) {
        value ^= value >>> 33;
        value *= FMIX_C1;
        value ^= value >>> 33;
        value *= FMIX_C2;
        value ^= value >>> 33;
        return value;
    }
}
