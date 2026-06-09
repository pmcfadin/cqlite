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

Cassandra bloom filters use **Murmur3 128-bit hash** for partition key hashing. The 128-bit output
is split into two 64-bit values used for double hashing. The seed is caller-provided; seed=0 is a
common default for the standard `FilterKey` implementation but is not a format constant.

```
[hash[0], hash[1]] = murmur3_x64_128(key, seed)   // result[0]=h1, result[1]=h2
```

For each of the k hash functions, bit positions are derived using the **double hashing formula**:

```
index_i = abs((hash[1] + i * hash[0]) % bitset_capacity)   for i = 0, 1, ..., k-1
```

Here `hash[0]` is MurmurHash result[0] (h1) and `hash[1]` is result[1] (h2). Note that
`hash[1]` (h2) is the base and `hash[0]` (h1) is the increment --- matching
[`BloomFilter.java:84`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomFilter.java):
`setIndexes(hash[1], hash[0], ...)`.

`bitset_capacity` is the allocated bit capacity of the `OffHeapBitSet`:
`capacity = ceil((n * bucketsPerElement + BITSET_EXCESS) / 64) * 64` bits,
where `BITSET_EXCESS = 20`
([`FilterFactory.java:35`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/FilterFactory.java)).
Modulo is over the full allocated capacity, which includes the 20-bit excess.

This scheme avoids computing k independent hashes by deriving all positions from two base hashes.
The wrapping arithmetic ensures consistent behavior across hash implementations.

**Special cases:**
- Hash values are deterministic for a given key and seed
- Different keys produce different hash pairs with high probability

Hashing and bit array notes:
- Double-hashing scheme derives k positions from two base hashes to avoid k separate hashes.
- Bit array is stored as raw bytes (no endianness transformation); bit addressing uses
  `index >> 3` for byte offset and `index & 0x7` for bit mask (LSB-first within each byte).
- Old format (`serializeOldBfFormat`) explicitly writes little-endian 8-byte longs; the new
  format is a raw memory dump with no byte-swapping.

## Read Flow Interaction

During a point lookup, Bloom is checked before any index/summary seeks. A negative result avoids IO. A positive result may be false; the reader continues to `Summary.db` + `Index.db` for confirmation.

`Filter.db` is loaded lazily if present; when absent, reads proceed with higher IO cost via index/summary. For a loader implementation walkthrough, see Appendix C.

### Key Takeaways
- Bloom provides fast negative lookups; positives still consult index/summary.
- FPR is configurable; higher bits-per-key lowers FPR but increases memory.
- The target FPR stored in `Statistics.db` (build-time) may differ from the observed runtime FPR depending on key distribution and filter saturation; adjust `bloom_filter_fp_chance` and rebuild to realign if needed.
- Missing or unreadable Bloom falls back to index/summary without correctness loss.

### References
- Cassandra 5.0.8:
  - `BloomFilter`:
    [`org.apache.cassandra.utils.BloomFilter`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomFilter.java)
  - `BloomCalculations`:
    [`org.apache.cassandra.utils.BloomCalculations`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomCalculations.java)
  - `BloomFilterSerializer`:
    [`org.apache.cassandra.utils.BloomFilterSerializer`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomFilterSerializer.java)
  - `OffHeapBitSet`:
    [`org.apache.cassandra.utils.obs.OffHeapBitSet`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/obs/OffHeapBitSet.java)
  - `FilterFactory`:
    [`org.apache.cassandra.utils.FilterFactory`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/FilterFactory.java)
  - `FilterComponent`:
    [`org.apache.cassandra.io.sstable.format.FilterComponent`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/FilterComponent.java)
  
For implementation details, see Appendix C.

## Filter.db (Bloom) — File Format Layout

The Filter.db file contains a complete bloom filter serialized in Cassandra-compatible format.

### Binary Structure

```
[Hash Count:  4 bytes, signed int]
[Word Count:  4 bytes, signed int]
[Bit Array:   word_count * 8 bytes, raw bytes]
```

**Field details:**
- `hash_count` (4-byte signed int): Number of hash functions (k) used for insertion/lookup
  ([`BloomFilterSerializer.java:53`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomFilterSerializer.java)).
- `word_count` (4-byte signed int): Number of 64-bit words in the bit array
  ([`OffHeapBitSet.java:117`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/obs/OffHeapBitSet.java):
  `out.writeInt((int)(bytes.size() / 8))`).
- `bit_array`: `word_count * 8` bytes of raw bit data, copied directly from off-heap memory
  with no endianness transformation
  ([`OffHeapBitSet.java:118`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/obs/OffHeapBitSet.java):
  `out.write(bytes, 0, bytes.size())`).

The bit array length is calculated as:
```
bit_array_bytes = word_count * 8
total_file_size = 8 + bit_array_bytes
```

Deserialization reads the word count as a 4-byte int and multiplies by 8 to get byte count
([`OffHeapBitSet.java:146`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/obs/OffHeapBitSet.java):
`long byteCount = in.readInt() * 8L`).

> **Note on format versions:** The description above covers the current (new) format.
> An older format (`serializeOldBfFormat`) exists for backward compatibility; it writes each
> 64-bit word as an explicit little-endian long (8 bytes per word). The version-selection
> logic is in `FilterComponent.java` via `descriptor.version.hasOldBfFormat()`.

### Hex Example

Tiny hex excerpt (illustrative, new format):
```
00000000: 0000 0007 0000 000A <80 bytes of raw bit data> ...
```
Interpretation:
- `0000 0007` -> hash_count = 7
- `0000 000A` -> word_count = 10
- next 80 bytes -> bit array (10 words x 8 bytes each)

**Bit addressing:**
- Each byte in the bit array is addressed as `byte_index = bit_index >> 3`
- Within each byte, the bit mask is `1 << (bit_index & 0x7)` (LSB-first)
- There is no endianness transformation in the new format; bytes are stored as-is from
  off-heap memory

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

> **Note:** Cassandra's implementation uses a pre-computed lookup table
> (`BloomCalculations.optKPerBuckets[]`) rather than the continuous formula, so actual k values
> for small bucket counts may differ slightly from the formula above.

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

See [`BloomFilter`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomFilter.java)
and [`BloomCalculations`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/BloomCalculations.java)
for writer/reader details.


