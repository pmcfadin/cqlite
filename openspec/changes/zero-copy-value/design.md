# Design — Zero-copy value extraction (K5, issue #1644)

This is the design-heaviest stage of the Value-v2 train: it changes the public `Value` representation,
adds a borrow API to the scan window, and must not move a single decoded byte or pin a byte longer than
it should. Everything below is anchored to the CURRENT (post-E1, post-#1940) tree.

## Measured anchors (current tree)
- `Value` (post-E1, `cqlite-core/src/types.rs:30`) — byte-carrying variants are `Text(String)`,
  `Blob(Vec<u8>)`, `Varint(Vec<u8>)`, `Inet(Vec<u8>)`, `Decimal { scale: i32, unscaled: Vec<u8> }`.
  Size pin `const _: () = assert!(size_of::<Value>() <= 40)` (`types.rs:98`); the fat cold variants are
  already boxed.
- Accessors that MUST keep working: `as_str` (`types.rs:825`), `as_bytes` (`types.rs:833`),
  `as_inet_bytes` (`842`), `len` (`851`), `is_empty` (`860`).
- Stage-2 substrate (#1940): `scan_stream_windowed.rs:459` ships `mpsc::channel::<bytes::Bytes>`; the IO
  half decompresses + CRC-verifies and sends one `Bytes` per chunk; the parse half calls
  `WindowCursor::refill(&chunk)` (`window_cursor.rs:77`) which **copies** via `buf.extend_from_slice`.
  `WindowCursor` is `{ buf: Vec<u8>, start: usize }` — a front-cursor over a stitched contiguous buffer.
- Decode copy sites (~30): `String::from_utf8(x.to_vec())` / `Value::Blob(x.to_vec())` etc. in
  `row_decoder/raw_value.rs`, `row_decoder/cell_value.rs`, `parsing/raw_type_value.rs`,
  `parsing/comparator_value_parsing.rs`, `parsing/byte_comparable.rs:245`, `parsing/custom_scalar.rs`,
  `.../udt.rs`, `.../complex_column.rs`.
- Consumers (constrain the design): Python `bindings/python/src/value.rs:34-35` copies Text into a
  Python `str`, Blob into `PyBytes::new` — a COPY. Node `bindings/node/src/value.rs:188-191` copies via
  `create_string` / `create_buffer_copy` — a COPY. Arrow `export/arrow_convert.rs:654,674,1545,1581`
  borrows `s.as_str()`/`b.as_slice()` and the Arrow builder copies into a columnar buffer. Write path
  takes `&Value`. Comparator/ordering read via `as_bytes`.

## The central tension — why this is not a mechanical rename
`Bytes::slice_ref(&self, subset)` (and `Bytes::slice`) yield a view that keeps the WHOLE parent buffer
alive and requires `subset` to be a contiguous sub-slice of one live `Bytes`. Two facts collide with the
current window:

1. **The window STITCHES chunks.** A partition/cell can straddle a decompression-chunk boundary, so
   `WindowCursor` copies each chunk into one contiguous `Vec<u8>` (and `copy_within`-compacts it on
   refill). `window.as_slice()` is therefore backed by the window's own moving `Vec`, **not** by any
   chunk's `Bytes` — you cannot `slice_ref` from it.
2. **A subslice pins its whole parent.** A 1-byte value borrowed from a 64 KB chunk keeps 64 KB alive
   for as long as the value lives — the memory-amplification hazard #1644 names explicitly.

So K5 needs (a) a window that can hand out a real chunk-`Bytes` subslice on the common path, and (b) a
force-copy boundary that bounds retained memory.

## D1 — window borrow API (the #1940-deferred consumer)
**Chosen: a `Bytes`-native window that borrows on the fast path and stitch-copies only on straddle.**
`WindowCursor`'s backing becomes an enum:
- `Backing::Borrowed(Bytes)` — the logical window IS exactly one chunk's `Bytes` (cursor over it). This
  is the steady-state case: the previous chunk was fully consumed before the next arrived, so `refill`
  **replaces** the backing with the incoming `Bytes` (a move + refcount, **no copy**).
- `Backing::Stitched(Vec<u8>)` — a partition straddled a chunk boundary, leaving a residual unconsumed
  tail; `refill` must concatenate residual + new chunk into an owned buffer (the existing copy path).

The window exposes `borrow(range) -> ValueBytes`:
- `Backing::Borrowed(b)` → `ValueBytes::Borrowed(b.slice(range))` — zero-copy refcounted view.
- `Backing::Stitched(v)` → `ValueBytes::Owned(Bytes::copy_from_slice(&v[range]))` — one copy
  (correctness: a straddling value has no single parent `Bytes`).

`as_slice()` keeps returning `&[u8]` for the parser's scanning/boundary logic (unchanged); only the
**value-materialization** call sites switch to `borrow(range)`. The cursor/`consume`/`refill`
byte-movement contract (issue #1589) and the window's bounded-memory / window-size semantics are
unchanged — a `Borrowed` backing holds exactly one chunk, same bound as one stitched chunk.

*Rejected — thread chunk provenance (`&Bytes` + offset) through every decode signature.* That touches
hundreds of `&[u8]` decode signatures, and a straddling value STILL cannot be one `slice_ref`, so the
copy fallback is unavoidable regardless — the enum-backed window localizes the borrow/copy decision to
one place and keeps decode signatures taking a window handle, not raw provenance.

## D2 — the force-copy boundary (the retention decision the issue demands)
**Chosen: borrow on the streaming path; force-copy (compact) at every retention boundary, governed by
one documented slack rule.**

- **Streaming/decode path** — value decoded, evaluated by a predicate, and dropped OR handed to a
  copying sink within the CURRENT window's lifetime: **BORROW** (`ValueBytes::Borrowed`). No threshold,
  no copy. A predicate-rejected value is thus never copied at all. (Straddling values are `Owned` by
  D1 — correctness, not policy.)
- **Retention boundary** — any value that escapes the window: collected/materialized result sets,
  LIMIT / sort / dedup buffers, the chunk/row caches, any `Value` stored in a longer-lived structure,
  and the FFI/binding boundary: **FORCE-COPY (compact)**. `Value::into_owned()` walks a value and, for
  each `Bytes`-backed payload that is a `Borrowed` view whose parent is materially larger than the
  payload, reallocates it into a tight standalone `Bytes` (`Bytes::copy_from_slice`), releasing the
  chunk. "Materially larger" = `backing.len() > payload.len() + RETENTION_SLACK` with
  `RETENTION_SLACK` a documented constant (proposed 4 KiB; tunable, measured). A payload whose backing
  is already close to its own size stays borrowed (no wasteful copy); a tiny value pinning a big chunk
  is copied. This bounds retained RSS to `O(retained value bytes + N × RETENTION_SLACK)`.

Bindings and Arrow **already** copy at their boundary (verified: PyBytes/PyString, napi
`create_buffer_copy`, Arrow columnar builders), so the FFI/export retention boundary is satisfied
for free — the new `into_owned` compaction covers only core-internal retention (result buffers,
caches). Where the scan itself buffers rows before emit, `into_owned` is applied at the buffering point.

*Rejected — a copy-at-decode threshold (copy tiny values eagerly during decode).* It re-introduces an
allocation for the most common small strings on the hot path and erodes the allocs/row win; retention
is the actual hazard, so pay the copy only when a value is actually retained.

## D3 — which variants become `Bytes` (the ≤40 layout arbiter)
`bytes::Bytes` is 32 bytes (4 words) vs `String`/`Vec<u8>` at 24. The `size_of::<Value>() <= 40` pin is
the arbiter:
- **`Text`, `Blob`, `Varint`, `Inet` → `Bytes`.** Each is a single 32-byte `Bytes` payload → variant =
  32 + 8-byte tag = **40** ✓. These carry the text-heavy allocation load.
- **`Decimal.unscaled` STAYS `Vec<u8>`.** `Decimal { scale: i32, unscaled: Bytes }` = 32 (Bytes) + 4
  (i32) padded → **48 > 40**, which would blow the pin and force boxing `Decimal`. Decimals are small
  and rare; the borrow win is negligible. Keep it owned. (Open fork: box `Decimal` to also Bytes-back
  it — recommended NO.)

The compile-time pin stays the guard: if any borrow payload pushes the max, the build fails and the
implementer measures rather than bumping the pin.

## D4 — public API compatibility & breaking changes
Accessors are preserved: `as_str` returns `&str` (Text's `Bytes` is UTF-8-validated **in place at
decode** via `str::from_utf8`, so the invariant holds and the accessor is a cheap view — no re-copy);
`as_bytes`/`as_inet_bytes` return `&[u8]` (`Bytes: Deref<[u8]>`); `len`/`is_empty` unchanged.

**Enumerated breaking changes (public surface):**
1. `Value::Text(String)` → `Value::Text(Bytes)`; `Blob`/`Varint`/`Inet` inner type `Vec<u8>` → `Bytes`.
   Any external code that **matches** `Value::Text(s)` and used `s: String`, or **constructs**
   `Value::Text(some_string)`, must adapt. Mitigation: add ergonomic constructors
   (`Value::text(impl Into<...>)`, `From<String>`/`From<&str>`/`From<Vec<u8>>`/`From<Bytes>`) and keep
   the accessors, so idiomatic call sites (`as_str`, `.into()`) are source-compatible.
2. **serde wire format MUST stay byte-identical.** `Bytes` does not serialize like `String`/`Vec<u8>`
   by default; the variants get `#[serde(with = ...)]`/custom impls so the JSONL goldens and every
   serde round-trip are unchanged (a parity-pinned requirement, not just a nicety).
3. No variant added or removed; `Display`, ordering, hashing unchanged.

## Sequencing (staged, each stage parity-green)
0. **Measurement first (red-on-`main`):** wire/extend the #2075 dhat allocs-per-row lane; add the
   window-borrow work-probe and the retention test. All must fail (or be unmeasurable) on `main`.
1. **Representation + accessors + serde** — Bytes-back Text/Blob/Varint/Inet, keep accessors, custom
   serde, keep the ≤40 pin. Mechanical match-site fixups + both binding conversion arms + CLI arms.
   33-table parity + query-semantics oracle + binding suites green (still copying at decode).
2. **Window borrow API (D1)** — enum-backed `WindowCursor`, `borrow(range)`; decode value-materialization
   sites in `row_decoder/*` + `raw_type_value.rs` switch to `borrow`. Parity + allocs/row lane green
   (allocs/row drops for the primary decode path).
3. **Comparators + byte-comparable** — `comparator_value_parsing.rs`, `byte_comparable.rs`,
   `custom_scalar.rs` borrow. Parity + comparator/ordering scenarios green.
4. **Export** — verify Arrow append borrows-through (no double copy); confirm Flight/Arrow arrays own
   materialized memory (retention boundary). Parity + export tests green.
5. **Retention** — `Value::into_owned` compaction at the core-internal retention/buffer points; the
   retention test proves a tiny long-lived value releases its 64 KB chunk.

## Wiring-evidence / test strategy (red-then-green)
- #2075 dhat **allocs/row** lane on a text-heavy corpus: measurably fewer allocs/row after Stage 2
  (decode-side String/Vec alloc per text/blob cell eliminated on the borrow path). Baseline recorded.
- **bytes-copied-into-values ≈ 0** on the streaming borrow path (work-probe on the window borrow API).
- **Chunk-retention** test: a tiny value retained past the window does not hold its 64 KB chunk.
- `size_of::<Value>() <= 40` pin holds after Bytes-ification.
- **serde round-trip byte-identical** + `Display` unchanged + comparator ordering (NaN-last,
  `-0.0 < +0.0`) unchanged.
- **33-table byte-parity** (LZ4/Snappy/Deflate/Zstd) + **query-semantics oracle** + Python + Node
  binding suites — unchanged (pure representation change).

## Risks
- **Retention leak** if a retained borrow skips compaction — designed against (D2) and tested.
- **serde drift** silently changing the JSONL goldens — pinned by the byte-identical serde requirement.
- **Straddle correctness** — a value crossing a chunk boundary must fall to the owned copy; covered by
  a multi-chunk straddle fixture in the byte-parity harness.
- **Layout regression** — the ≤40 pin fails closed; `Decimal` deliberately left owned.
