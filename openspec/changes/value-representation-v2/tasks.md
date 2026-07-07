# Tasks — Value Representation v2

Sequenced as one train on branch `issue-1583-value-v2-boxing` (staged commits). Each stage names the
surface it exercises and carries a red-then-green test. Anchors are `main`-relative and will drift; the
implementer re-greps before editing.

## Stage 0 — measurement + guards (write tests FIRST, must fail on main)
- [ ] 0.1 Add a `size_of::<Value>()` measurement test and tighten the pin
  `cqlite-core/src/types.rs:96` from `<= 88` to `<= 40` (fails today at 88). (value-representation)
- [ ] 0.2 Enable `#![deny(clippy::large_enum_variant)]` (or crate-attr equivalent) for `cqlite-core`;
  confirm it fires on `main`'s `Value`. (value-representation)
- [ ] 0.3 Add the dhat lanes that must fail on `main`: steady-state scan **allocs/chunk ≤ 1**
  (scan-window-substrate) and text-heavy scan **bytes-copied-into-values ≈ 0** (sstable-value-decode);
  plus a UTF-8-path allocation-count test and a chunk-**retention** test.

## Stage 1 — D1 layout (E1 / #1583): box the fat cold variants
- [ ] 1.1 Re-measure, then box: `Tombstone(Box<TombstoneInfo>)`, `Udt(Box<UdtValue>)`,
  `Json(Box<serde_json::Value>)` in `cqlite-core/src/types.rs:30-85` (TombstoneInfo `types.rs:304-328`
  is the widest → drives the 88B pin; UdtValue `types.rs:250-257`). Box the next-widest if the pin
  still exceeds 40. (value-representation)
- [ ] 1.2 Fix the ~436 match/construction sites (`rg -n "Value::(Tombstone|Udt|Json)"`), keeping edits
  mechanical (box patterns / `ref`). Heaviest core files: `storage/write_engine/merge/mod.rs`,
  `storage/serialization/types.rs`, `parser/types/udt.rs`,
  `storage/sstable/reader/parsing/row_decoder/udt.rs`,
  `.../custom_scalar.rs`, `export/arrow_convert.rs`, `parser/types/tombstones.rs`. (value-representation)
- [ ] 1.3 Update binding conversion arms (payload now boxed → deref): Python
  `bindings/python/src/value.rs:55,60,63,109,507`; Node `bindings/node/src/value.rs:236,251,257` +
  `bindings/node/src/database.rs` (Udt×1, Json×2, Tombstone×1). Run BOTH binding suites.
  (value-representation)
- [ ] 1.4 CLI arms: `cqlite-cli/src/output/json.rs`, `cqlite-cli/src/data_parser.rs`.
  (value-representation)
- [ ] 1.5 Verify `Display`/serde/comparator byte-identical (ordering + serde scenarios). `--lite` green.
  (value-representation)

## Stage 2 — D2 substrate (#1940): window-as-`Bytes`
- [ ] 2.1 Make `WindowCursor` (`reader/window_cursor.rs:28`) able to hand out a refcounted `Bytes`
  subslice; align the fill site `window_cursor.rs:91` (`extend_from_slice`, called from
  `scan_stream_windowed.rs:715,734,736`) with the B1 `Arc<[u8]>` the cache already returns
  (`scan_stream_windowed.rs:719`, `data_access/bti.rs:752`, `data_access/mod.rs:586`).
  (scan-window-substrate)
- [ ] 2.2 Decompress in the IO half; carry `Bytes`/`Arc<[u8]>` on the chunk channel
  (`scan_stream_windowed.rs:402` `mpsc::channel::<Vec<u8>>`). mmap backend: zero-copy `Bytes` view;
  buffered backend: one reused per-cursor scratch. Keep the B1 `Arc` contract (hit = refcount bump).
  (scan-window-substrate)
- [ ] 2.3 Preserve invariants: CRC-before-decompress order unchanged; window-size/bounded-memory
  semantics unchanged; `uncompressed_len` from `CompressionInfo`. (scan-window-substrate)
- [ ] 2.4 Prove: allocs/chunk ≤ 1 dhat lane green; read-op counter still 1/chunk; 33-table byte-parity
  across LZ4/Snappy/Deflate/Zstd green; CRC-corruption scenario. (scan-window-substrate)

## Stage 3 — D3 decode (K5 / #1644): zero-copy extraction
- [ ] 3.1 **Interim S-win (may land at Stage 1, independent of D2):** replace
  `String::from_utf8(bytes.to_vec())` with `str::from_utf8(bytes)?.to_owned()` at the decode sites —
  `row_decoder/raw_type_value.rs:58,118`; `raw_value.rs:139`; `cell_value.rs:369`;
  `row_framing.rs:1398`; `udt.rs:298,550,833`; `complex_column.rs:1338`;
  `comparator_value_parsing.rs:168,229`. Guard with the UTF-8 alloc-count test. (sstable-value-decode)
- [ ] 3.2 Borrow scalar byte payloads from the Stage-2 substrate as `Bytes::slice_ref`: Text/Blob/
  Varint/Decimal.unscaled at the `.to_vec()` copy sites — `raw_value.rs:147,324,334,337,480`;
  `cell_value.rs:301,441,923`; `raw_type_value.rs:419,445,499,538,999,1018,1039,1063,1126,1157`;
  `udt.rs:641,642,672,914,923,989,1000`; `complex_column.rs:940,1422`;
  `comparator_value_parsing.rs:172,216,224`; `custom_scalar.rs:48,64`; `value_parsing.rs:414`. Route via
  the per-column dispatch already resolved (`sstable-value-decode` existing spec — no per-cell type
  inference). (sstable-value-decode)
- [ ] 3.3 Implement + document the **retention copy-out policy** (threshold + long-lived→copy) at the
  extraction site; retention test proves a tiny long-lived value releases its chunk. (sstable-value-decode)
- [ ] 3.4 Prove: H2 text-heavy dhat bytes-copied ≈ 0; 33-table parity (4 codecs) + Python + Node suites
  green. (sstable-value-decode)

## Stage 4 — gate + audit + review (definition of done)
- [ ] 4.1 Full `scripts/agent-gate.sh` ONCE → PASS (paste SUMMARY verbatim). `RUSTFLAGS="-D warnings"`
  clean; no `unwrap()`/`expect()` in library code.
- [ ] 4.2 spec-auditor **C** anchored to `openspec/changes/value-representation-v2/specs/**` → PASS
  (every requirement satisfied with a public-surface test).
- [ ] 4.3 roborev clean (fix findings; test/docs-only rounds re-certify with `--delta`).
- [ ] 4.4 Predicate-eval + sort throughput micro-bench posted (E1 acceptance).
- [ ] 4.5 Close #1583, #1644, #1940 on merge; archive the change.
