# Design: chunk-source-scan-decode (issue #2165)

## Context

Current state of the two sites named by the issue:

| Site named in #2165 | Current reality |
|---|---|
| `parse_partition_at_offset` (`parsing/mod.rs:685`) | **Gone.** Removed by the #112 file splits + #2302 full-index enumeration. `parsing/mod.rs` has zero `.decompress(` calls. Allowlist entry is dead. |
| `parse_block_entries` (`parsing/block_entries.rs:111`) | **Live, narrowly reached.** Decompresses an already-read, CRC-validated compressed block (`read_next_block` → `read_nb_format_chunk_data`) inline via `Compression::new(*compression_reader.algorithm())`, with a silent parse-raw-bytes fallback on failure (lines 120–125). Callers: `data_access/sequential.rs:273` (`get_all_entries`) and `:768` (`sequential_scan`), non-stitching branch only. Public surfaces: `SSTableReader::scan` (three index-fallback sites) and `iterate_all_partitions` → `sequential_scan`, plus `full_index_stream.rs:423` bail-to-sequential. |

Reachability note: `requires_chunk_stitching()` routes `V5CompressedLegacy` + nb through the stitch
path (already on `ChunkSource::decompress_only`), and BTI-compressed scans early-return via
`bti_scan_with_metadata`; uncompressed formats have `compression_reader == None`. So the `:111`
decompress fires on a narrow residue of real-world inputs; the deliberate driver is the synthetic
test `test_decompression_fallback_on_failure` (`block_entries.rs:1347`). The change is therefore
primarily an **architecture-invariant completion** (close the G2 plane, delete the allowlist) plus
one **fail-closed semantic fix**, with the full parity suite guarding no-regression.

## Decision 1 — migration shape: `decompress_only`, not `chunk(index)`

**Chosen: (a) mechanical swap to `ChunkSource::decompress_only(self.compression.as_ref(), block_data)`**
(static, uncached, no counter) — exactly the pattern the stitch path uses at `data_access/mod.rs:474`
for the same input shape: a buffer already read + CRC-validated by the unified C2 primitive
(`block_io.rs::read_compressed_chunk_at`).

**Beat: (b) restructuring the non-stitching loop onto `ChunkSource::new(...)` + `chunk(index)`.**
That would require threading a `ReadAt` source, `CompressionInfo`, cache namespace + `cache_id`
through `ScanCursor`/`read_next_block`, duplicate the CRC step already performed upstream, and put
full-scan blocks into the B1 `DecompressedChunkCache` — churning the cache point reads depend on,
for zero correctness gain. The archived G2 design's "adaptation layer" anticipated this heavier
shape before `parse_partition_at_offset` was removed; it is no longer warranted.

## Decision 2 — decompress failure fails closed (behavioral change, corrupt files only)

**Chosen:** a failed block decompress returns a corruption error (via `decompress_only`'s `Err`),
never a silent raw-bytes parse attempt.

**Beat:** preserving the legacy fallback (wrap `decompress_only` in `or_else(raw)`). Rejected as a
no-heuristics violation (#28): "maybe these compressed bytes were actually uncompressed" is
byte-pattern guessing; on a genuinely corrupt file it can fabricate garbage rows instead of an
error. `ChunkSource` (the plane of record) already fails closed on every other path; keeping a
divergent semantic in the one migrated site would defeat the point of consolidation.

`test_decompression_fallback_on_failure` (`block_entries.rs:1347`) — today the only deliberate
driver of the fallback — flips to assert the error. Healthy-file behavior is unchanged
(`compression: None` ⇒ raw passthrough is identical on both models).

## Architecture-test delta

`chunk_decode_single_plane.rs`: delete the two exclusion lines (46–47) + their comment (43–45);
update the `warm_windowed_scan_skips_decompress` doc-comment (267–271) which currently documents the
legacy path as "the scoped #2165 follow-up"; update the `chunk_source.rs:4-9` module doc naming the
deferred sites. The scan then asserts `parsing/` is fully consolidated — the issue's acceptance
criterion verbatim.

## Test plan (wiring evidence)

- **Architecture:** `chunk_decode_single_plane.rs` green with the allowlist entries removed —
  `.decompress(` resolves in exactly one module (`chunk_source`).
- **Fail-closed:** flipped `test_decompression_fallback_on_failure` asserts a corruption error
  through the `parse_block_entries` surface.
- **No-regression (public surfaces, real fixtures):** `v5_compressed_legacy_parity_test.rs`,
  `v5_compressed_legacy_row_count_parity.rs`, `issue_1085_tombstones_full_scan_parity.rs`
  (full-scan reconciliation), `index_size_zero_integration_test.rs` (drives the size=0 →
  `sequential_scan` fallback that reaches `parse_block_entries`), `point_vs_full_differential.rs`
  + `query_semantics_oracle_parity.rs` (read-path equivalence), 33-table smoke.
- **Alloc/perf guards untouched but relevant:** `test_issue_1046_scan_alloc_scaling.rs`,
  `issue_1333_scan_scratch_reuse.rs` (per-block buffer handling unchanged: same
  input buffer, same output `Vec<u8>`).

## Risks

- **Low.** One call-site swap with an in-repo precedent; the narrow reachability means the parity
  blast radius is small, and the full suite covers the scan/iterate surfaces that could regress.
- The fail-closed flip could, in theory, surface errors on previously-"working" corrupt files —
  that is the intended behavior change and is confined to files whose blocks fail decompression.
