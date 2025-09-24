## CompressionInfo.db and Chunking

Explore compression algorithms, chunk sizes, offset maps, and checksums in `CompressionInfo.db`, and how chunking impacts random vs sequential IO.

### In this chapter you will learn
- What `CompressionInfo.db` contains and how it’s used
- How chunk size choices influence performance trade-offs
- How checksums are validated per chunk
- How tooling exposes chunk maps

## Compression Metadata

`CompressionInfo.db` contains algorithm name, chunk length, total uncompressed length, chunk offsets, and optionally per-chunk CRCs and a metadata CRC.

For a concise parser walkthrough, see Appendix C.

## Chunk Size Trade-offs

- Smaller chunks improve random-read locality but add metadata overhead and decompression CPU.
- Larger chunks reduce overhead and improve scans, but increase random-read amplification.

## Checksums

Modern formats can record per-chunk CRCs and a metadata CRC; readers enforce them for Cassandra 5.0 formats. Digest files (`Digest.crc32`) cover component integrity at a coarse level; per-chunk CRCs catch localized corruption.

Readers enforce size and CRC expectations for modern formats. For decompressor details, see Appendix C.

### Key Takeaways
- `CompressionInfo.db` maps chunks and validates integrity for modern formats.
- Chunk length is central to random vs scan performance; choose based on workload.
- Readers must pair `CompressionInfo.db` with `Data.db` to read the right byte ranges.

### References
- Cassandra 5.0.0:
  - `CompressionMetadata`: [org.apache.cassandra.io.compress.CompressionMetadata](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java)
  
For implementation details, see Appendix C.


