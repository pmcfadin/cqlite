## Filter.db (Bloom)

Bloom filters stored in `Filter.db` provide fast negative lookups before any disk seeks. This chapter covers parameters, expected false-positive rate (FPR), and how Bloom interacts with summary/promoted index.

### In this chapter you will learn
- Bloom filter parameters and expected false positive rate (FPR)
- How Bloom interacts with summary/promoted index
- Where Bloom sits in the read flow
- Practical impacts and small numeric examples

## Bloom parameters and sizing

Expected FPR depends on bits-per-key and number of hash functions:

- Optimal bits per key: m = −(n · ln p) / (ln 2)²
- Optimal hash functions: k = (m / n) · ln 2

Text-only (ASCII) versions for copy/paste into code:
- m = - (n * ln(p)) / (ln(2))^2
- k = (m / n) * ln(2)

Small numeric example (intuition): for n=1,000 and p=1%, the optimal bits-per-key is ~9.6 and k≈7.

## Hash Algorithm

Cassandra bloom filters use **Murmur3 128-bit hash** with seed 0 for partition key hashing. The 128-bit output is split into two 64-bit values for double hashing:

```
hash128 = murmur3_x64_128(key, seed=0)
hash1 = (hash128 >> 64) & 0xFFFFFFFFFFFFFFFF  // high 64 bits
hash2 = hash128 & 0xFFFFFFFFFFFFFFFF         // low 64 bits
```

For each of the k hash functions, bit positions are derived using the **double hashing formula**:

```
hash_i = hash1 + (i * hash2)  for i = 0, 1, 2, ..., k-1
bit_position = (hash_i mod bit_count)
```

This scheme avoids computing k independent hashes by deriving all positions from two base hashes. The wrapping arithmetic ensures consistent behavior across hash implementations.

**Special cases:**
- Empty key produces `hash1 = 0, hash2 = 0` (Murmur3 seed 0 behavior)
- Hash values are deterministic for a given key
- Different keys produce different hash pairs with high probability

Hashing and bit array notes:
- Double-hashing scheme derives k positions from two base hashes to avoid k separate hashes.
- Bit array is addressed modulo bit_count; serialized as big-endian u64 words with bits packed LSB-first within each byte.

## Read Flow Interaction

During a point lookup, Bloom is checked before any index/summary seeks. A negative result avoids IO. A positive result may be false; the reader continues to `Summary.db` + `Index.db` for confirmation.

`Filter.db` is loaded lazily if present; when absent, reads proceed with higher IO cost via index/summary. For a loader implementation walkthrough, see Appendix C.

### Key Takeaways
- Bloom provides fast negative lookups; positives still consult index/summary.
- FPR is configurable; higher bits-per-key lowers FPR but increases memory.
- The target FPR stored in `Statistics.db` (build-time) may differ from the observed runtime FPR depending on key distribution and filter saturation; adjust `bloom_filter_fp_chance` and rebuild to realign if needed.
- Missing or unreadable Bloom falls back to index/summary without correctness loss.

### References
- Cassandra 5.0.0:
  - `BloomFilter`: [org.apache.cassandra.utils.bloom.BloomFilter](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/bloom/BloomFilter.java)
  - `BloomCalculations`: [org.apache.cassandra.utils.bloom.BloomCalculations](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/bloom/BloomCalculations.java)
  
For implementation details, see Appendix C.

## Filter.db (Bloom) — File Format Layout

The Filter.db file contains a complete bloom filter serialized in Cassandra-compatible format.

### Binary Structure

```
[Hash Count: 4 bytes, big-endian u32]
[Bit Count:  8 bytes, big-endian u64]
[Bit Array:  variable length, big-endian u64 words]
```

**Field details:**
- `hash_count` (u32 BE): Number of hash functions (k) used for insertion/lookup
- `bit_count` (u64 BE): Total number of bits in the filter (m)
- `bit_array`: Sequence of u64 words in big-endian format, with bits packed within each word

The bit array length is calculated as:
```
word_count = ceil(bit_count / 64)
bit_array_bytes = word_count * 8
total_file_size = 12 + bit_array_bytes
```

### Hex Example

Tiny hex excerpt (real file, start):
```
00000000: 0000 0007 0000 0000 0000 0258 a4c0 e2a8 ...
```
Interpretation:
- `0000 0007` → hash_count = 7
- `0000 0000 0000 0258` → bit_count = 600 bits
- next bytes → bit array (75 bytes = ceil(600/64) * 8 = 10 words * 8 bytes)

**Endianness and bit packing:**
- Fixed-width fields are big-endian
- Each u64 word in the bit array is stored in big-endian byte order
- Within each u64 word, bit 0 is the least significant bit

## Write-Time Sizing Guidance

When creating bloom filters at SSTable write time, choose parameters based on expected partition key count and acceptable false positive rate.

### Sizing Formulas

Given `n` expected keys and target false positive rate `p`:

**Optimal bit count (m):**
```
m = ceil(-(n * ln(p)) / (ln(2))^2)
```

**Optimal hash functions (k):**
```
k = ceil((m / n) * ln(2))
k = max(k, 1)  // ensure at least one hash function
```

### Concrete Examples

**Small table (n = 1,000, p = 1%):**
```
m = ceil(-(1000 * ln(0.01)) / (ln(2))^2)
  = ceil(9585.7) = 9,586 bits ≈ 1,198 bytes
k = ceil((9586 / 1000) * ln(2)) = ceil(6.6) = 7 hash functions
```

**Medium table (n = 100,000, p = 1%):**
```
m = ceil(-(100000 * ln(0.01)) / (ln(2))^2)
  = 958,506 bits ≈ 119.8 KB
k = 7 hash functions
```

**Low FPR table (n = 100,000, p = 0.1%):**
```
m = ceil(-(100000 * ln(0.001)) / (ln(2))^2)
  = 1,437,759 bits ≈ 179.7 KB
k = ceil((1437759 / 100000) * ln(2)) = 10 hash functions
```

### Memory vs. FPR Trade-offs

| Target FPR | Bits per Key | Hash Functions | Memory for 1M keys |
|-----------|--------------|----------------|-------------------|
| 10%       | ~4.8         | 3              | ~586 KB          |
| 1%        | ~9.6         | 7              | ~1.15 MB         |
| 0.1%      | ~14.4        | 10             | ~1.73 MB         |
| 0.01%     | ~19.2        | 13             | ~2.30 MB         |

**Runtime FPR estimation:**

After inserting `inserted_count` keys, the actual false positive rate can be estimated as:
```
prob_bit_zero = (1 - 1/m)^(k * inserted_count)
actual_fpr = (1 - prob_bit_zero)^k
```

If `inserted_count` significantly exceeds `n`, the filter becomes saturated and `actual_fpr` rises above the target `p`. In production, rebuild the SSTable with a larger `expected_elements` value.

See `org.apache.cassandra.utils.bloom.BloomFilter` and `BloomCalculations` for writer/reader details.


