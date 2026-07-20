package in.mcfad.cqlite.flight;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the Java {@link Murmur3Token} output byte-for-byte to the Rust authority
 * {@code cassandra_murmur3_token} ({@code cqlite-core/src/util/cassandra_murmur3.rs}).
 *
 * <p>Every {@code (keyBytes, expectedToken)} vector below is copied verbatim from
 * the Rust unit tests in that file (each independently verified against a running
 * Cassandra 5.0 {@code SELECT token(id), id}). Because both sides consume the same
 * canonical partition-key byte layout, a Java token that disagrees means the port
 * drifted — a correctness hazard for split pruning — and this test fails loudly.
 * (Spec: "Java Murmur3 token matches the Rust Cassandra-parity authority".)
 */
class Murmur3TokenTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] bytes(int... vals) {
        byte[] out = new byte[vals.length];
        for (int i = 0; i < vals.length; i++) {
            out[i] = (byte) vals[i];
        }
        return out;
    }

    @Test
    void textKeyTokensMatchRust() {
        assertEquals(-8839064797231613815L, Murmur3Token.token(utf8("a")));
        assertEquals(-7815133031266706642L, Murmur3Token.token(utf8("ab")));
        assertEquals(-5434086359492102041L, Murmur3Token.token(utf8("abc")));
        assertEquals(-5153323217664422577L, Murmur3Token.token(utf8("abcd")));
        assertEquals(2321271983248423864L, Murmur3Token.token(utf8("abcde")));
        assertEquals(-1982280103179862187L, Murmur3Token.token(utf8("abcdef")));
        assertEquals(-6427428730009885543L, Murmur3Token.token(utf8("abcdefg")));
        assertEquals(-3708139591217214462L, Murmur3Token.token(utf8("abcdefgh")));
        assertEquals(-3758069500696749310L, Murmur3Token.token(utf8("hello")));
        assertEquals(356242581507269238L, Murmur3Token.token(utf8("cassandra")));
        assertEquals(5322121941860471994L, Murmur3Token.token(utf8("murmur3")));
        assertEquals(-3182120811138177122L, Murmur3Token.token(utf8("test_key_12345")));
        // 15 bytes (all k1 tail positions), 16 bytes (one full block, no tail), long.
        assertEquals(-6472281833689111727L, Murmur3Token.token(utf8("0123456789abcde")));
        assertEquals(5467490433528156583L, Murmur3Token.token(utf8("0123456789abcdef")));
        assertEquals(-7889617755374116647L,
                Murmur3Token.token(utf8("this is a longer test key for murmur3 hashing")));
    }

    @Test
    void intKeyTokensMatchRust() {
        // 4-byte big-endian int values.
        assertEquals(-3485513579396041028L, Murmur3Token.token(bytes(0x00, 0x00, 0x00, 0x00)));
        assertEquals(-4069959284402364209L, Murmur3Token.token(bytes(0x00, 0x00, 0x00, 0x01)));
        assertEquals(7297452126230313552L, Murmur3Token.token(bytes(0xFF, 0xFF, 0xFF, 0xFF)));
        assertEquals(-7160136740246525330L, Murmur3Token.token(bytes(0x00, 0x00, 0x00, 0x2A)));
        assertEquals(4443639997907684431L, Murmur3Token.token(bytes(0x00, 0x00, 0x00, 0x7F)));
        assertEquals(-9081975895656599623L, Murmur3Token.token(bytes(0x00, 0x00, 0x00, 0x80)));
        assertEquals(-8423851636648339959L, Murmur3Token.token(bytes(0x00, 0x00, 0x00, 0xFF)));
        assertEquals(180151580513994396L, Murmur3Token.token(bytes(0x00, 0x00, 0x01, 0x00)));
        assertEquals(7935772098093053663L, Murmur3Token.token(bytes(0x00, 0x00, 0x03, 0xE8)));
        assertEquals(5905661066942090405L, Murmur3Token.token(bytes(0xFF, 0xFF, 0xFC, 0x18)));
        assertEquals(-765994672030311617L, Murmur3Token.token(bytes(0x7F, 0xFF, 0xFF, 0xFF)));
        assertEquals(-420533958509279465L, Murmur3Token.token(bytes(0x80, 0x00, 0x00, 0x00)));
    }

    @Test
    void bigintKeyTokensMatchRust() {
        // 8-byte big-endian bigint values, incl. i64::MIN and i64::MAX.
        assertEquals(2945182322382062539L, Murmur3Token.token(bytes(0, 0, 0, 0, 0, 0, 0, 0)));
        assertEquals(6292367497774912474L, Murmur3Token.token(bytes(0, 0, 0, 0, 0, 0, 0, 1)));
        assertEquals(7071048584287372947L,
                Murmur3Token.token(bytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)));
        assertEquals(-1722304415079482439L,
                Murmur3Token.token(bytes(0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)));
        assertEquals(9204767954415360687L,
                Murmur3Token.token(bytes(0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)));
    }

    @Test
    void uuidKeyTokensMatchRust() {
        assertEquals(5457549051747178710L,
                Murmur3Token.token(bytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
        assertEquals(-2824192546314762522L,
                Murmur3Token.token(bytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)));
        assertEquals(4277286421682315655L,
                Murmur3Token.token(bytes(0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00)));
        assertEquals(-8497799532739775204L,
                Murmur3Token.token(bytes(0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34,
                        0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc)));
    }

    @Test
    void blobTokensExerciseSignExtensionBug() {
        // High-bit bytes: the sign-extension tail behavior must match Cassandra exactly.
        assertEquals(5048724184180415669L, Murmur3Token.token(bytes(0x00)));
        assertEquals(-5284281814142962636L, Murmur3Token.token(bytes(0x80)));
        assertEquals(-4442228696663692417L, Murmur3Token.token(bytes(0xFF)));
        assertEquals(-2002833339314343643L, Murmur3Token.token(bytes(0xFF, 0xFE)));
        assertEquals(-7623170703309721106L,
                Murmur3Token.token(bytes(0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89)));
        assertEquals(597835946752277653L,
                Murmur3Token.token(bytes(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F)));
        assertEquals(-5563837382979743776L,
                Murmur3Token.token(bytes(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10)));
    }

    @Test
    void compositeKeyTokensMatchRust() {
        // ('hello', 42) → 7666157718303755816; composite = [len:u16 BE][bytes][0x00] per comp.
        byte[] helloAnd42 = PartitionKeyBytes.composite(
                new byte[][] {utf8("hello"), bytes(0x00, 0x00, 0x00, 0x2A)});
        assertEquals(7666157718303755816L, Murmur3Token.token(helloAnd42));

        // ('world', 99) → -4641306270390207264.
        byte[] worldAnd99 = PartitionKeyBytes.composite(
                new byte[][] {utf8("world"), bytes(0x00, 0x00, 0x00, 0x63)});
        assertEquals(-4641306270390207264L, Murmur3Token.token(worldAnd99));
    }

    @Test
    void normalizeMapsMinToMax() {
        assertEquals(Long.MAX_VALUE, Murmur3Token.normalize(Long.MIN_VALUE));
        assertEquals(Long.MAX_VALUE, Murmur3Token.normalize(Long.MAX_VALUE));
        assertEquals(0L, Murmur3Token.normalize(0L));
        assertEquals(1L, Murmur3Token.normalize(1L));
        assertEquals(-1L, Murmur3Token.normalize(-1L));
    }

    @Test
    void emptyInputHashesToZero() {
        // Rust test_hash_returns_h1_h2_order: empty input → h1 == 0.
        assertEquals(0L, Murmur3Token.token(new byte[0]));
    }

    @Test
    void tokenInRangeFollowsHalfOpenAndWraparoundConvention() {
        // Normal (start, end]: start exclusive, end inclusive.
        assertTrue(Murmur3Token.tokenInRange(50, 0, 100, false));
        assertTrue(Murmur3Token.tokenInRange(100, 0, 100, false));
        assertTrue(!Murmur3Token.tokenInRange(0, 0, 100, false));
        assertTrue(!Murmur3Token.tokenInRange(101, 0, 100, false));
        // Wraparound (start > end): token > start OR token <= end.
        assertTrue(Murmur3Token.tokenInRange(200, 100, -100, true));
        assertTrue(Murmur3Token.tokenInRange(-100, 100, -100, true));
        assertTrue(!Murmur3Token.tokenInRange(0, 100, -100, true));
        // Full ring (start == end): every token in range (issue #2228).
        assertTrue(Murmur3Token.tokenInRange(Long.MIN_VALUE, 42, 42, false));
        assertTrue(Murmur3Token.tokenInRange(0, 42, 42, true));
    }
}
