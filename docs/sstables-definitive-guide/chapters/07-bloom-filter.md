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
- Bit array is addressed modulo bit_count; ensure consistent endianness when serializing bitsets.

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


