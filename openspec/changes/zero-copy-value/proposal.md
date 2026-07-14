# Zero-copy value extraction — Bytes-backed `Value` (K5, issue #1644)

## Milestone
0.15 (perf train). **Design-driven / Seam-1** — this is the third and final stage of the "Value v2"
train and the design-heaviest one. Owner approves this change before any implementation.

## Where this sits in the Value-v2 train
The train has three stages; the first two have SHIPPED:

- **Stage 1 — E1 (#1583), SHIPPED.** Boxed the fat cold variants (`Tombstone`/`Udt`/`Json`);
  `size_of::<Value>() <= 40` compile-time pin (`cqlite-core/src/types.rs`).
- **Stage 2 — #1940 (E3 follow-up), SHIPPED** (PR #2320, `c42dbdd6`). The IO→parse channel now ships
  one decompressed refcounted `bytes::Bytes` per chunk (`scan_stream_windowed.rs`), reaching
  ≤1 alloc/chunk. **But the parse half still COPIES**: `WindowCursor::refill` does
  `self.buf.extend_from_slice(chunk)` into an owned `Vec<u8>`, and every text/blob value is then copied
  out of that window with `String::from_utf8(bytes.to_vec())` / `.to_vec()`.
- **Stage 3 — K5 (#1644), THIS CHANGE.** Consume the Stage-2 `Bytes` substrate: `Value`'s
  byte-carrying variants become `Bytes`-backed zero-copy views over the decoded chunk, and the
  `WindowCursor` gains the borrow API that #1940 explicitly deferred here (owner decision 2026-07-10,
  the borrow API's only consumer lives in K5).

The ancestor design `openspec/changes/value-representation-v2/` covered all three stages jointly; its
E1 and #1940 requirements are already implemented. This change carries the K5 remainder to completion.
(See open forks: whether to archive the ancestor now that its other two stages shipped.)

## Why (measured problem)
Source: `docs/reports/parser-performance-audit-2026-07-01.md` §K5;
`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic E.

Every text/blob value is copied out of the decompressed chunk per cell. On a text-heavy scan that is
**one full-payload heap allocation + memcpy per cell** — `String::from_utf8(data.to_vec())`,
`Value::Blob(data.to_vec())`, etc. at ~30 decode sites (`row_decoder/raw_value.rs`,
`row_decoder/cell_value.rs`, `parsing/raw_type_value.rs`, `parsing/comparator_value_parsing.rs`,
`parsing/byte_comparable.rs`, `parsing/custom_scalar.rs`, `.../udt.rs`, `.../complex_column.rs`).
Because Stage 2 already hands the parse half a refcounted `Bytes`, the decoder can instead **borrow a
subslice** (a refcount bump, no allocation) — so a value that a predicate rejects is never copied at
all, and a value that survives to a copying sink is copied exactly once (at the sink) instead of twice
(decode + sink). This is the per-row allocation the #2075 dhat lane is built to measure.

## What changes
1. **Representation.** `Value::Text(String)` → `Value::Text(Bytes)` and
   `Value::Blob(Vec<u8>)`/`Varint(Vec<u8>)`/`Inet(Vec<u8>)` → `Bytes`, where the `Bytes` may be a
   zero-copy `slice_ref` view of the decoded chunk. Public accessors (`as_str`/`as_bytes`/`len`/
   `is_empty`) keep working unchanged; serde and `Display` stay byte-identical. (`Decimal.unscaled`
   stays an owned `Vec<u8>` — Bytes-ifying it would push the `Decimal` variant past the ≤40 pin;
   see design D3 + open forks.)
2. **Borrow API.** `WindowCursor` gains a borrow path that hands the decoder a refcounted `Bytes`
   subslice view of the current chunk instead of copying into the owned window buffer — the API #1940
   deferred to K5.
3. **Retention policy (the named hazard).** A borrowed subslice-`Bytes` keeps its whole parent chunk
   alive by refcount, so a 1-byte value can pin a 64 KB chunk. A documented **force-copy boundary**
   compacts (copies to a tight allocation) at every retention boundary; the streaming decode path
   borrows.
4. **FFI stays a copy.** Bindings copy at their boundary (Python `str`/Node `Buffer` own their memory
   — they already do); Arrow builders copy into columnar buffers. Zero-copy is core-internal +
   Arrow-append-internal only.

## Non-goals
- No change to how any value is **decoded semantically** — byte-parity and query-semantics are
  inviolable (pure representation change).
- No comparator / `Display` / serde / ordering change (float/NaN, signed-zero stay parity-pinned).
- No new/removed public `Value` variant; `QueryRow.values` map contract unchanged.
- No zero-copy across the FFI boundary — bindings and Arrow arrays own their materialized memory.
- Not the E1 layout (shipped), the #1940 substrate (shipped), positional-row emit (E2/K3), or the
  chunk cache (B1).
- No pre-`na` format support introduced or revisited.

## Definition of done
`scripts/agent-gate.sh` PASS + spec-auditor **C** PASS (every requirement has a public-surface test) +
roborev clean; `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code; 33-table
byte-parity (LZ4/Snappy/Deflate/Zstd) + query-semantics oracle + Python + Node binding suites green;
the #2075 dhat allocs/row lane shows the measured win. Then archive.
