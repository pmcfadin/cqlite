//! Cassandra-compatible Murmur3 hash implementation
//!
//! This is an exact port of Cassandra's `MurmurHash.hash3_x64_128` from
//! `org.apache.cassandra.utils.MurmurHash`. It preserves Cassandra's
//! sign-extension bug in tail byte processing, which causes it to diverge
//! from the standard Murmur3 algorithm for inputs containing bytes >= 0x80.
//!
//! Used for:
//! - Murmur3Partitioner token computation
//! - Bloom filter (Filter.db) hash computation

const C1: u64 = 0x87c3_7b91_1142_53d5;
const C2: u64 = 0x4cf5_ad43_2745_937f;

// Body mix constants (per-block additive constants for h1 and h2).
const H1_ADD: u64 = 0x52dc_e729;
const H2_ADD: u64 = 0x3849_5ab5;

// Finalization mix multipliers used by `fmix64`.
const FMIX_C1: u64 = 0xff51_afd7_ed55_8ccd;
const FMIX_C2: u64 = 0xc4ce_b9fe_1a85_ec53;

/// Compute Cassandra's `MurmurHash.hash3_x64_128` with seed 0.
///
/// Returns `(h1, h2)` matching `result[0]` and `result[1]` from the Java implementation.
///
/// This differs from standard Murmur3 in that tail bytes are sign-extended
/// (Java's `byte` is signed), not zero-extended.
pub fn cassandra_murmur3_x64_128(data: &[u8]) -> (i64, i64) {
    let mut h1: u64 = 0;
    let mut h2: u64 = 0;

    // Body: process 16-byte blocks.
    // Cassandra's getBlock reads bytes as unsigned (& 0xff) in little-endian order,
    // which is equivalent to u64::from_le_bytes.
    let mut chunks = data.chunks_exact(16);
    for block in &mut chunks {
        let mut k1 = u64::from_le_bytes(block[0..8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(block[8..16].try_into().unwrap());

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(H1_ADD);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(H2_ADD);
    }

    // Tail: process remaining bytes.
    // Cassandra's bug: `(long) key.get(offset+N)` sign-extends because Java's byte is signed.
    // We replicate this with: byte as i8 as i64 as u64.
    let tail = chunks.remainder();
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;

    // Sign-extend a byte the way Java does: byte → signed byte → long.
    let signed = |byte: u8| -> u64 { byte as i8 as i64 as u64 };

    // Shift schedule matches the Java fall-through switch:
    // positions 0..8 feed k1 (shifts 0, 8, 16, 24, 32, 40, 48, 56);
    // positions 8..15 feed k2 (shifts 0, 8, 16, 24, 32, 40, 48).
    for (pos, &byte) in tail.iter().enumerate() {
        let shift = ((pos % 8) * 8) as u32;
        if pos < 8 {
            k1 ^= signed(byte) << shift;
        } else {
            k2 ^= signed(byte) << shift;
        }
    }

    if tail.len() >= 9 {
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }
    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    // Finalization
    let len = data.len() as u64;
    h1 ^= len;
    h2 ^= len;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1 as i64, h2 as i64)
}

/// Compute a Murmur3 partition token matching Cassandra's Murmur3Partitioner.
///
/// Takes h1 from the 128-bit hash and applies `normalize`:
/// maps `i64::MIN` to `i64::MAX` (Cassandra excludes the minimum token value).
pub fn cassandra_murmur3_token(data: &[u8]) -> i64 {
    let (h1, _) = cassandra_murmur3_x64_128(data);
    normalize(h1)
}

/// Compute `DecoratedKey.filterHashLowerBits()` for a raw partition key, matching
/// Cassandra 5.0's `org.apache.cassandra.db.DecoratedKey`.
///
/// Cassandra computes the full 128-bit Murmur3 hash of the partition-key bytes and
/// uses the two 64-bit words distinctly:
/// - `hash[0]` (`h1`, after `normalize`) becomes the `Murmur3Partitioner` token.
/// - `hash[1]` (`h2`, **un-normalized**) is returned by `filterHashLowerBits()` and
///   is the value whose **low 8 bits** the BTI `Partitions.db` partition index writes
///   as each leaf's "hash byte" (fast mismatch pre-filter).
///
/// Reference: `DecoratedKey.filterHashLowerBits()` returns `hash[1]`
/// (`org.apache.cassandra.db.DecoratedKey`, Cassandra 5.0.8); the BTI partition index
/// stores `(byte) decoratedKey.filterHashLowerBits()`
/// (`org.apache.cassandra.io.sstable.format.bti.PartitionIndex`).
///
/// Verified against the real `da-2-bti-Partitions.db` fixture
/// (`test-data/datasets/sstables/test_da/simple_table-*`): the three UUID partitions
/// store hash bytes `0x24`, `0x22`, `0xf4`, which are exactly the low bytes of `h2`
/// for those keys.
///
/// Note this is `h2`, NOT the token's `h1`; the placeholder it replaces incorrectly
/// derived the byte from the byte-comparable `h1` token.
pub fn cassandra_partition_filter_hash_lower_bits(data: &[u8]) -> i64 {
    let (_, h2) = cassandra_murmur3_x64_128(data);
    h2
}

/// Cassandra's normalize: `Long.MIN_VALUE` maps to `Long.MAX_VALUE`.
fn normalize(v: i64) -> i64 {
    if v == i64::MIN {
        i64::MAX
    } else {
        v
    }
}

fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(FMIX_C1);
    value ^= value >> 33;
    value = value.wrapping_mul(FMIX_C2);
    value ^= value >> 33;
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // Test vectors verified against a running Cassandra 5.0 instance.
    // Each (key_bytes, expected_token) pair was produced by:
    //   SELECT token(id), id FROM ... ;
    // where key_bytes is the CQL-serialized form of the partition key.
    // ---------------------------------------------------------------

    /// Shared assertion loop for token test vectors.
    fn assert_tokens(label: &str, vectors: &[(&[u8], i64)]) {
        for (key_bytes, expected_token) in vectors {
            let token = cassandra_murmur3_token(key_bytes);
            assert_eq!(
                token, *expected_token,
                "Token mismatch for {} key {:02X?}: got {}, expected {}",
                label, key_bytes, token, expected_token
            );
        }
    }

    /// Text key test vectors (key bytes = UTF-8 bytes of the string)
    #[test]
    fn test_text_key_tokens() {
        let vectors: &[(&[u8], i64)] = &[
            (b"a", -8839064797231613815),
            (b"ab", -7815133031266706642),
            (b"abc", -5434086359492102041),
            (b"abcd", -5153323217664422577),
            (b"abcde", 2321271983248423864),
            (b"abcdef", -1982280103179862187),
            (b"abcdefg", -6427428730009885543),
            (b"abcdefgh", -3708139591217214462),
            (b"hello", -3758069500696749310),
            (b"cassandra", 356242581507269238),
            (b"murmur3", 5322121941860471994),
            (b"test_key_12345", -3182120811138177122),
            // 15 bytes (exercises all k1 tail positions)
            (b"0123456789abcde", -6472281833689111727),
            // 16 bytes (exactly one full block, no tail)
            (b"0123456789abcdef", 5467490433528156583),
            // Long string (multiple blocks + tail)
            (
                b"this is a longer test key for murmur3 hashing",
                -7889617755374116647,
            ),
        ];
        assert_tokens("text", vectors);
    }

    /// Int key test vectors (key bytes = 4-byte big-endian)
    #[test]
    fn test_int_key_tokens() {
        let vectors: &[(&[u8], i64)] = &[
            (&[0x00, 0x00, 0x00, 0x00], -3485513579396041028), // 0
            (&[0x00, 0x00, 0x00, 0x01], -4069959284402364209), // 1
            (&[0xFF, 0xFF, 0xFF, 0xFF], 7297452126230313552),  // -1
            (&[0x00, 0x00, 0x00, 0x2A], -7160136740246525330), // 42
            (&[0x00, 0x00, 0x00, 0x7F], 4443639997907684431),  // 127
            (&[0x00, 0x00, 0x00, 0x80], -9081975895656599623), // 128
            (&[0x00, 0x00, 0x00, 0xFF], -8423851636648339959), // 255
            (&[0x00, 0x00, 0x01, 0x00], 180151580513994396),   // 256
            (&[0x00, 0x00, 0x03, 0xE8], 7935772098093053663),  // 1000
            (&[0xFF, 0xFF, 0xFC, 0x18], 5905661066942090405),  // -1000
            (&[0x7F, 0xFF, 0xFF, 0xFF], -765994672030311617),  // i32::MAX
            (&[0x80, 0x00, 0x00, 0x00], -420533958509279465),  // i32::MIN
        ];
        assert_tokens("int", vectors);
    }

    /// UUID key test vectors (key bytes = 16-byte UUID in big-endian)
    #[test]
    fn test_uuid_key_tokens() {
        let vectors: &[(&[u8], i64)] = &[
            // 00000000-0000-0000-0000-000000000000
            (
                &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                5457549051747178710,
            ),
            // ffffffff-ffff-ffff-ffff-ffffffffffff
            (
                &[
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF,
                ],
                -2824192546314762522,
            ),
            // 550e8400-e29b-41d4-a716-446655440000
            (
                &[
                    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55,
                    0x44, 0x00, 0x00,
                ],
                4277286421682315655,
            ),
            // 12345678-1234-1234-1234-123456789abc
            (
                &[
                    0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x56,
                    0x78, 0x9a, 0xbc,
                ],
                -8497799532739775204,
            ),
        ];
        assert_tokens("UUID", vectors);
    }

    /// Blob key test vectors (key bytes = raw bytes, exercises sign-extension bug)
    #[test]
    fn test_blob_key_tokens() {
        let vectors: &[(&[u8], i64)] = &[
            (&[0x00], 5048724184180415669),
            (&[0x80], -5284281814142962636), // High bit set — sign-extension matters
            (&[0xFF], -4442228696663692417), // All bits set — sign-extension matters
            (&[0xFF, 0xFE], -2002833339314343643), // Two high bytes
            // 10-byte blob with all high bytes (0x80-0x89)
            (
                &[0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89],
                -7623170703309721106,
            ),
            // 15-byte blob (exercises all tail positions)
            (
                &[
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                    0x0E, 0x0F,
                ],
                597835946752277653,
            ),
            // 16-byte blob (exactly one block, no tail)
            (
                &[
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                    0x0E, 0x0F, 0x10,
                ],
                -5563837382979743776,
            ),
        ];
        assert_tokens("blob", vectors);
    }

    /// Bigint key test vectors (key bytes = 8-byte big-endian)
    #[test]
    fn test_bigint_key_tokens() {
        let vectors: &[(&[u8], i64)] = &[
            (&[0, 0, 0, 0, 0, 0, 0, 0], 2945182322382062539), // 0
            (&[0, 0, 0, 0, 0, 0, 0, 1], 6292367497774912474), // 1
            (
                &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                7071048584287372947,
            ), // -1
            (
                &[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                -1722304415079482439,
            ), // i64::MAX
            (
                &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
                9204767954415360687,
            ), // i64::MIN
        ];
        assert_tokens("bigint", vectors);
    }

    /// Composite partition key test vectors
    /// Cassandra encodes composite keys as: [len:u16 BE][bytes][0x00] for each component
    #[test]
    fn test_composite_key_tokens() {
        // ('hello', 42) → token 7666157718303755816
        // Composite encoding: [0x00, 0x05] + b"hello" + [0x00] + [0x00, 0x04] + int_bytes(42) + [0x00]
        let mut key = Vec::new();
        key.extend_from_slice(&[0x00, 0x05]); // len of "hello"
        key.extend_from_slice(b"hello");
        key.push(0x00); // end-of-component
        key.extend_from_slice(&[0x00, 0x04]); // len of int (4 bytes)
        key.extend_from_slice(&42i32.to_be_bytes());
        key.push(0x00); // end-of-component
        assert_eq!(
            cassandra_murmur3_token(&key),
            7666157718303755816,
            "Composite key ('hello', 42) token mismatch"
        );

        // ('world', 99) → token -4641306270390207264
        let mut key = Vec::new();
        key.extend_from_slice(&[0x00, 0x05]); // len of "world"
        key.extend_from_slice(b"world");
        key.push(0x00);
        key.extend_from_slice(&[0x00, 0x04]);
        key.extend_from_slice(&99i32.to_be_bytes());
        key.push(0x00);
        assert_eq!(
            cassandra_murmur3_token(&key),
            -4641306270390207264,
            "Composite key ('world', 99) token mismatch"
        );
    }

    /// Verify normalize maps MIN_VALUE to MAX_VALUE
    #[test]
    fn test_normalize() {
        assert_eq!(normalize(i64::MIN), i64::MAX);
        assert_eq!(normalize(i64::MAX), i64::MAX);
        assert_eq!(normalize(0), 0);
        assert_eq!(normalize(1), 1);
        assert_eq!(normalize(-1), -1);
    }

    /// Verify the raw hash function returns (h1, h2) — not swapped
    #[test]
    fn test_hash_returns_h1_h2_order() {
        // For the empty input with seed 0, both h1 and h2 should be 0
        let (h1, h2) = cassandra_murmur3_x64_128(b"");
        assert_eq!(h1, 0);
        assert_eq!(h2, 0);

        // For "a", verify h1 matches the Cassandra token (before normalize)
        let (h1, _h2) = cassandra_murmur3_x64_128(b"a");
        assert_eq!(h1, -8839064797231613815_i64);
    }

    /// Validate the byte-comparable token encoding for the three UUID keys
    /// used in the test_da/simple_table BTI fixture.  These values were verified
    /// against the real Partitions.db trie structure (issue #755).
    ///
    /// The byte-comparable form of a Murmur3 token is:
    ///   (token as u64) XOR 0x8000_0000_0000_0000  (flips sign bit → unsigned sort order)
    ///
    /// The first byte of each bc token matches the Sparse8 transition byte in the
    /// real Partitions.db trie (see da-2-bti-Partitions.db hex analysis in #755).
    #[test]
    fn uuid_murmur3_token_bc_matches_bti_trie_transitions() {
        // These expected values were verified by parsing the real
        // da-2-bti-Partitions.db fixture and reading the Sparse8 transition bytes.
        // Token order determines Data.db write order: 22 < 11 < 33.
        struct Case {
            uuid_byte: u8,
            expected_token: i64,
            expected_bc_first_byte: u8,
        }
        let cases = [
            Case {
                uuid_byte: 0x22,
                expected_token: 1213057064512856170,
                expected_bc_first_byte: 0x90,
            },
            Case {
                uuid_byte: 0x11,
                expected_token: 4360155383588533346,
                expected_bc_first_byte: 0xBC,
            },
            Case {
                uuid_byte: 0x33,
                expected_token: 8780122315263850168u64 as i64,
                expected_bc_first_byte: 0xF9,
            },
        ];
        for c in &cases {
            let uuid = [c.uuid_byte; 16];
            let token = cassandra_murmur3_token(&uuid);
            assert_eq!(
                token, c.expected_token,
                "token mismatch for UUID {:02X}*16",
                c.uuid_byte
            );
            let bc = (token as u64) ^ 0x8000_0000_0000_0000u64;
            assert_eq!(
                (bc >> 56) as u8,
                c.expected_bc_first_byte,
                "bc first byte mismatch for UUID {:02X}*16",
                c.uuid_byte
            );
        }
    }
}
