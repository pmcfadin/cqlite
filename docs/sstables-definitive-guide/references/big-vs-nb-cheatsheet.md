## BIG vs NB Cheat Sheet (Cassandra 5.0 oriented)

### Quick ID (at a glance)
- **Filename tokens**: `mc-*`/`mm-*` (BIG family), `nb-*` (NB = New Big)
- **`Data.db` global header**: **BIG** = format-dependent; **NB** = none
- **CRC placement**: **BIG** = header CRC may exist (format-dependent); **NB** = per-chunk trailing CRC32 (no header CRC) on compressed data
- **Compression metadata**: both families use `CompressionInfo.db` for chunk map and parameters
- **TOC**: `TOC.txt` is the authoritative component list; validate presence/size before deeper checks

### Component differences and checksums

| Component | Family | Header CRC | Trailing CRC | Digest covered? | Byte order / notes |
|---|---|---|---|---|---|
| `Data.db` | NB | No | Yes (per compressed chunk, u32 BE) | Yes (whole file) | No global header; navigate via `CompressionInfo.db` |
| `Data.db` | BIG | Format-dependent | No (except compression chunks) | Yes | If header CRC present, validate before scanning entries |
| `Index.db` | BIG | Format-dependent | No | Yes | Some variants length-prefix entries; others don’t. Gate by Descriptor/reader |
| `Summary.db` | BIG | Format-dependent | No | Yes | Sampled index metadata |
| `Filter.db` (Bloom) | BIG/NB | No | No | Yes | Bitset + k; validate vs `Statistics.db` |
| `Statistics.db` | BIG/NB | No | No | Yes | Readable via `sstablemetadata` |
| `CompressionInfo.db` | BIG/NB | No | NB has trailing CRCs in `Data.db` | Yes | Chunk map (offset,length) + params; offsets monotonic |
| `Digest.crc32` | BIG/NB | — | — | — | Per-generation file used to verify per-component digests/CRCs |
| `TOC.txt` | BIG/NB | — | — | — | Enumerates components; treat as publication barrier |

Notes:
- NB `Data.db` CRC32 is computed over compressed bytes only and stored immediately after each chunk (big-endian u32).
- `Digest.crc32` coverage: recompute a CRC for each component listed in `TOC.txt` over the entire file contents; compare against recorded values.

### Validation workflow (safe defaults)
1. **Enumerate** `TOC.txt` → verify listed components exist; flag extras.
2. **Digest first** → recompute per-component CRCs in a deterministic order; verify `Digest.crc32`.
3. **NB `Data.db`** → step chunk-by-chunk using `CompressionInfo.db`: read [chunk][CRC32], validate (CRC excludes the CRC word).
4. **BIG headers** → if format indicates header CRCs, validate header before reading entries.
5. **`Index.db`** → choose struct via Descriptor. If len-prefixed, parse `[u16 entry_len][u16 0x0010][digest][vint offset][opt promoted]`; otherwise, parse the non-prefixed variant.
6. **Bloom & Summary** → sanity-check sizes/params vs `Statistics.db` (hash count, bitset length, sample count).

### Common pitfalls (production bugs)
- Treating NB `Data.db` as if it had a header → misaligned reads and bogus “magic numbers”
- Assuming one `Index.db` struct → some BIG artifacts len-prefix entries; others don’t
- Using the wrong VInt flavor for offsets → implement once, test against known bytes
- Forgetting NB CRC excludes the CRC word itself → compute over compressed chunk only

### Handy CLI probes
- `sstabledump <Descriptor>-Data.db | head -n 40` — spot-check partition boundaries
- `sstablemetadata <Descriptor>-Data.db` — verify chunk length, Bloom FP chance, estimated keys
- `hexdump -C <Descriptor>-CompressionInfo.db | head -n 16` — confirm chunk map header


