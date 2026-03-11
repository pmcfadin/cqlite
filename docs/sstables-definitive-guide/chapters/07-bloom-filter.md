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

## Filter.db (Bloom) — on-disk layout (overview)

Minimal on-disk fields (Cassandra 5.0):
- bitset_length_bytes (u32, big-endian): number of bytes in the serialized bitset payload
- hash_count_k (u32, big-endian)
- bitset payload (byte array)

Endianness and bit packing:
- Fixed-width fields are big-endian.
- Bits in the payload are packed least-significant-bit first within each byte (bit 0 = LSB).

Tiny hex excerpt (real file, start):
```
00000000: 0000 0005 0000 0002 a4c0 e2a8 02a2 a1b3 ...
```
Interpretation (schematic):
- `0000 0005` → bitset_length_bytes = 5
- `0000 0002` → k (hash count)
- next bytes → bitset payload (5 bytes; bits packed LSB-first per byte)

Concrete sizing example (n = 1,000, p = 1%):
```
m = ceil(-(n * ln p) / (ln 2)^2) = ceil(-(1000 * ln 0.01) / (ln 2)^2)
  = ceil(9585.7) = 9,586 bits ≈ 1,198 bytes
k = round((m / n) * ln 2) = round((9586 / 1000) * 0.6931) ≈ 7
```

See `org.apache.cassandra.utils.bloom.BloomFilter` and `BloomCalculations` for writer/reader details.


