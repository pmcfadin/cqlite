# Design — Value Representation v2

This is the E1-owned joint design doc for the "Value v2" train (#1583 layout · #1644 decode ·
#1940 substrate). It is deliberately one document because the three seams are coupled: the decode side
(#1644) can only borrow from the chunk once the window hands out `Bytes` (#1940), and the borrowed
payload must fit inside the enum-layout budget (#1583). Design-review together; ship as one train
(order below).

## Context / measured anchors
- `size_of::<Value>() == 88` bytes today (`cqlite-core/src/types.rs`; the pin is
  `const _: () = assert!(std::mem::size_of::<Value>() <= 88);`). Fat inline variants: `Tombstone(88)`,
  `Udt(72)`, `Json(72)`.
- **436** match/construction sites reference `Value::{Tombstone,Udt,Json}` across the workspace
  (`rg -c`), concentrated in core decode/write/serialization and the CLI/binding conversion layers.
  This is the boxing blast radius — mechanical, but wide, so the layout change is sequenced FIRST and
  landed on its own before the decode rework rides on top.
- The scan window is a front-cursor `WindowCursor { buf: Vec<u8>, start: usize }`
  (`reader/window_cursor.rs:28`, shipped by E7/#1589); the IO→parse channel still ships an owned
  `Vec<u8>` per chunk (`scan_stream_windowed.rs:402` `mpsc::channel::<Vec<u8>>`); decompressed bytes
  reach the window via `WindowCursor::refill` → `self.buf.extend_from_slice(chunk)`
  (`window_cursor.rs:91`). The B1 chunk cache (`DecompressedChunkCache`, `reader/types.rs:379`) already
  returns `Arc<[u8]>` on `.get()`/`.insert()`. `bytes` is already a workspace dependency and is unused
  in the reader/decode hot path today (only export/test code uses `bytes::Bytes`).
- E3 (#1585, merged PR #1955) shipped the single-payload+CRC-read-per-chunk win and a read-op counter;
  it explicitly deferred the ≤1-alloc/chunk `Bytes` substrate to #1940.

## The three coupled decisions

### D1 — `Value` enum layout (E1 / #1583)
**Chosen: box the three fat rare variants** — `Tombstone(Box<TombstoneInfo>)`, `Udt(Box<UdtValue>)`,
`Json(Box<serde_json::Value>)` — re-measuring first and boxing whatever is the new widest inline
payload until `size_of::<Value>() <= 40`. Tighten the A4 pin `<= 88` → `<= 40`. Enable
`clippy::large_enum_variant` (deny) at the crate root so nothing re-inlines a fat variant silently.

Why this over the alternatives:
- **Box only the rare variants (chosen).** The three are cold (tombstone markers, UDTs, embedded JSON);
  the hot scalar/text/blob path never touches the box. One pointer-chase on a cold path in exchange for
  a 2.75× shrink of every hot `Value` slot/clone/`Vec`. Match ergonomics stay mechanical (box patterns
  / `ref`).
- **Rejected — box everything / newtype-per-variant.** Uniform indirection taxes the hot path (Text,
  Integer, etc.) for no layout win, and churns far more than 436 sites.
- **Rejected — split `Value` into hot/cold enums.** A public-API break with a large ripple through
  bindings, serde, and every consumer; disproportionate to a constant-factor fix.

Guardrail (JOINT-OWNER): E1 changes ONLY the enum layout and its match sites — NOT how values are
decoded (that is D3). No semantic change: `Display`/serde/comparison byte-identical.

### D2 — window-as-`Bytes` substrate (#1940 / E3 follow-up)
**Chosen: carry `bytes::Bytes` through the windowed-scan hot seam**, decompressing in the IO half:
- **mmap backend:** decompress from / expose a **zero-copy `Bytes` view** of the mapped compressed
  region where the codec allows; otherwise decompress into a `Bytes`-owned buffer once.
- **buffered backend:** reuse ONE per-cursor compressed scratch (clear+reuse, no realloc), decompress
  once into an `Arc<[u8]>`/`Bytes` shipped on the channel.
- `WindowCursor` becomes able to hand out `Bytes` subslices (a refcounted view) so the borrow path
  replaces the `extend_from_slice`-into-`Vec<u8>` copy at `window_cursor.rs:91` — the B1 `Arc<[u8]>`
  the cache already returns (`scan_stream_windowed.rs:719`, `bti.rs:752`, `data_access/mod.rs:586`) is
  the natural refcounted carrier to align the window fill with.

Preserved invariants (inviolable): **CRC is verified before decompression output is trusted, in the
same order as today**; the windowed scan's bounded-memory discipline and window-size semantics are
unchanged; `uncompressed_len` comes from `CompressionInfo` (authoritative — no guessing); the B1 chunk
cache `Arc` contract is not broken (the window-fill site aligns with the cached `Arc`).

Why: this is the enabling substrate D3 borrows from, and independently reaches ≤1 alloc/chunk on the
steady-state scan (A5 already cut reads to 1). Rejected: leaving the window as `Vec<u8>` and copying in
D3 anyway — that would make K5 "zero-copy" a copy, defeating the point.

### D3 — zero-copy value extraction (K5 / #1644)
**Chosen: `Bytes`-backed payloads for the borrowable scalar-bytes variants** (`Text`, `Blob`, `Varint`,
`Decimal.unscaled`) — a `Bytes::slice_ref`/subslice of the decompressed chunk. UTF-8 for `Text` is
validated in place with `str::from_utf8` on the borrowed slice (validate without copy), storing the
validated `Bytes`. This replaces `String::from_utf8(bytes.to_vec())` / `.to_vec()` extraction on the
decode paths.

**Retention policy (named design concern — the 1-byte-value/64KB-chunk hazard).** A `Bytes` subslice
keeps its parent chunk alive by refcount. To prevent a tiny, long-lived value from pinning a whole
chunk, extraction applies a **copy-out threshold**: values at or below a small byte threshold (and any
value that outlives the scan window, e.g. materialized/collected results) are copied into their own
small allocation instead of borrowing. The threshold and the "long-lived → copy" rule are documented at
the extraction site and covered by a test. Short-lived, large values on the streaming path borrow;
tiny/retained values copy. This keeps steady-state RSS bounded while still eliminating the dominant
per-cell copy on text-heavy scans.

**Interim S-win (independent, can land first):** on any decode path where a copy must remain for now,
replace `String::from_utf8(bytes.to_vec())` with `str::from_utf8(bytes)?.to_owned()` — validates on the
borrowed slice, dropping the throwaway `Vec` on the error path and one intermediate. This is guarded by
an allocation-count test on the UTF-8 path and does not depend on D2.

Guardrail: no decoded value changes for any CQL type; the dispatch tag / type is never inferred from
value bytes (no-heuristics). Byte-parity across all four compression algorithms is the net.

## How `Value` layout interacts with a `Bytes` payload (the coupling)
`Bytes` is 32 bytes (ptr+len+cap+vtable ≈ 3 words + atomic). Replacing `Text(String)` (24B) /
`Blob(Vec<u8>)` (24B) with `Bytes` (32B) does NOT by itself exceed the ≤40 target, but it makes the
`Bytes`-carrying variants the layout floor — which is exactly why D1 (boxing the fat cold variants) is
required for D3 to stay inside budget. The `size_of` pin (≤40) is the compile-time guard that keeps
D1 and D3 mutually consistent; if D3's `Bytes` payloads push the max, D1 boxes the next-widest variant.
This is the single reason the three cannot be designed independently.

## Sequencing (one train, staged PRs on one branch)
1. **D1 layout** first (box fat variants + tighten pin ≤40 + enable `large_enum_variant`) — mechanical,
   436 sites, lands and goes green on its own.
2. **D2 substrate** (`Bytes` window seam) next — the 33-table byte-parity harness across all four codecs
   is the load-bearing net; ≤1-alloc/chunk dhat lane proves it.
3. **D3 decode** last, borrowing from D2's window, staying inside D1's budget — H2 text-heavy dhat shows
   bytes-copied-into-values ≈ 0; retention test proves the tiny-value/big-chunk case copies out.
   (D3's interim S-win may land at step 1 independently.)

## Wiring-evidence / test strategy (red-then-green, must fail on `main`)
- `size_of::<Value>() <= 40` compile-time pin (fails today at 88).
- `clippy::large_enum_variant` deny (would fire today).
- Steady-state scan **allocs/chunk ≤ 1** dhat lane (fails today ≥2–3).
- Text-heavy scan **bytes-copied-into-values ≈ 0** dhat lane (fails today: ~1× payload/value).
- UTF-8-path allocation-count test guarding the interim S-win.
- Chunk-**retention** test: a tiny long-lived value does not retain its 64KB chunk (copy-out threshold).
- **33-table byte-parity** across LZ4/Snappy/Deflate/Zstd + Python + Node binding suites — unchanged.
- Predicate-eval + sort throughput micro-bench (values move through both) posted.

## Risks
- **Blast radius (436 sites).** Mitigated by sequencing D1 alone first and keeping edits mechanical
  (box patterns), plus the binding conversion layers (PyO3/napi match on the three variants) updated and
  both binding suites run.
- **Retention leak** if D3 borrows without the copy-out threshold — explicitly designed against and
  tested (the named `Do NOT` in #1644).
- **CRC-order or bounded-memory regression** in D2 — guarded by the byte-parity harness and the
  unchanged window-size semantics; CRC-before-decompress is a hard invariant restated in the tasks.
