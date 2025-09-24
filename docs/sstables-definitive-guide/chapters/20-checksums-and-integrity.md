## Checksums and Integrity

SSTables carry integrity metadata at two levels: per-chunk checksums for compressed `Data.db` blocks and the `Digest.crc32` file for component-level verification. Readers validate chunks before decompression and can cross-check digests when scanning directories or transferring files.

### In this chapter you will learn
- How checksums are stored and validated per chunk
- What `Digest.crc32` covers and how it differs from chunk checksums
- How readers/writers interact with integrity metadata
- How to demonstrate a minimal verification example

## Per-Chunk Checksums
When compression is enabled, `CompressionInfo.db` may include a CRC for each compressed chunk. Readers should compute CRC over the compressed bytes and compare with metadata prior to decompression. This catches corruption early and avoids propagating errors downstream.

Readers should validate chunk CRCs where present before decompression; modern formats expect strict CRC adherence. For validation walkthroughs, see Appendix C.

## Digest Files
`Digest.crc32` provides a fast verification for the main components of an SSTable generation. It is complementary to per-chunk CRCs: the digest validates whole-file contents, while per-chunk CRCs validate compressed block integrity during reads.

Minimal example (conceptual): During directory validation, ensure that for each generation listed in `TOC.txt`, all required components exist and optionally check `Digest.crc32` against recomputed CRCs when available.

## Recovery Strategies (Beyond Detection)

Scope note: focus on SSTable-level recovery patterns; node-level operations are out of scope.

- Isolate and quarantine:
  - Move suspected-corrupt components out of the live path; keep originals for forensics
  - Prevent partial reads by ensuring `TOC.txt` no longer references quarantined files

- Targeted file replacement:
  - Replace only failed components from known-good copies (snapshot/backup)
  - Validate digests and, if compressed, sample chunk CRCs before activation

- Range-based rehydration:
  - Trigger repair/streaming for affected token ranges to reconstruct data from replicas
  - Prefer re-streaming over attempting to salvage partially corrupt `Data.db`

- Post-recovery hygiene:
  - Run verification tools; schedule compaction to remove overlap and rebuild summaries if required
  - Monitor error counters; re-scan directories after compaction

### Key Takeaways
- Per-chunk CRCs protect compressed `Data.db` blocks before decompression.
- `Digest.crc32` validates whole-file content at the component level.
- Readers should validate chunks on-the-fly; tools may verify digests offline.
- Fail-fast CRC mismatches indicate corruption; do not attempt heuristic recovery in modern formats.

### References
- Cassandra 5.0.0:
  - `DataIntegrityMetadata`: `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java`
  - `PureJavaCrc32`: `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/PureJavaCrc32.java`
  
For implementation details, see Appendix C.


