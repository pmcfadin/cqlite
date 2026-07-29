# Value Representation v2 — joint decode/emit train (E1 + K5 + E3-follow-up)

## Milestone
0.14 (read-path performance). Design-driven — this change is the **shared design** the three
Ready/In-Progress issues explicitly require before any implementation:

- **#1583 (E1)** — box the fat, rare `Value` variants; owns the `Value` enum layout. Target
  `size_of::<Value>() <= 40` (measured 88 today).
- **#1644 (K5)** — zero-copy value extraction: text/blob/varint/decimal decode into refcounted
  slices of the decompressed chunk instead of a per-cell copy. This is the **decode side** of the
  same enum layout.
- **#1940 (E3 follow-up)** — the **window-as-`Bytes` substrate**: carry chunk bytes through the scan
  as refcounted `Bytes`/`Arc<[u8]>` so a decoded value can borrow the chunk (the enabling substrate
  K5 depends on; reaches ≤1 alloc/chunk on the steady-state scan).

Both #1583 and #1644 state in their own bodies that "the three must be designed together and shipped
as one train; do not start implementation before the joint design exists." E3 (#1585) already shipped
the single-read-per-chunk win (PR #1955) but **deferred** the `Bytes` substrate to #1940. This change
is that joint design.

## Why (measured problem)
Source of truth: `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic E (E1/E3) and
`docs/reports/parser-performance-audit-2026-07-01.md` §K5.

1. **`size_of::<Value>() == 88` bytes (measured, `types.rs`).** Three rare variants are inline and fat
   (`Tombstone`, `Udt`, `Json`). Every `Vec<Value>`, `Option<Value>`, row slot, and clone on every path
   pays 2.75× the necessary size. Boxing the three rare variants brings `Value` to ≤40B (audit: 32B).
2. **Every text/blob/varint/decimal value is copied out of the decompressed chunk per cell**
   (`String::from_utf8(bytes.to_vec())` etc. on the decode paths). A text-heavy scan pays one full
   payload copy per value — plus a throwaway pre-validation `Vec` on the `from_utf8` path.
3. **The scan window is a plain `Vec<u8>`** the parse half copies decompressed bytes into
   (`extend_from_slice`), and the IO→parse channel ships an **owned `Vec<u8>` per chunk**. There is no
   refcounted/borrowed path a decoded value could hold, so zero-copy decode (item 2) is impossible
   until the window hands out `Bytes`. Steady-state scan floors at ≥2 allocs/chunk today.

Individually small; collectively 2–5× on scan throughput and steady-state allocation load.

## What changes (one train, three seams)
- **Layout (E1):** box `Tombstone`/`Udt`/`Json` (re-measure first; box whatever the new max is until
  `Value <= 40`). Tighten the A4 compile-time pin `<= 88` → `<= 40`. Enable
  `clippy::large_enum_variant` (deny) so the regression class is closed permanently.
- **Substrate (#1940):** the windowed-scan hot seam carries `bytes::Bytes` (mmap backend: a zero-copy
  view of the map; buffered backend: a reused per-cursor scratch decompressed once into an
  `Arc<[u8]>`/`Bytes`). CRC-before-decompress order is unchanged; the B1 chunk-cache `Arc` contract is
  preserved. Reaches ≤1 alloc/chunk on the steady-state scan.
- **Decode (K5):** text/blob/varint/decimal `Value` payloads become `Bytes`-backed subslices of the
  chunk (`Bytes::slice_ref`), UTF-8 validated in place via `str::from_utf8` (validate without copy).
  A documented **retention policy** governs the 1-byte-value/64KB-chunk hazard (a copy-out threshold so
  a tiny long-lived value never pins a whole chunk).

## Non-goals
- **No change to how values are *decoded* semantically** (byte-parity is inviolable). Only the enum
  layout, its match sites, the extraction mechanism (copy → borrow), and the window buffer type change.
- **No comparator / `Display` / serde / ordering behavior change.** Float/NaN Cassandra ordering,
  signed-zero, and every JSONL golden stay byte-identical (comparator behavior is parity-pinned).
- **No new public `Value` variant** and no change to the public `QueryRow.values` map contract.
- **Not** the positional-row rework (E2/#1584/#1817 — separate), the shared chunk cache B1/B2
  (separate), or E4/E5/E6/E7/E8.
- **No pre-`na` format support** introduced or revisited (version floor unchanged).

## Doctrine impact
- No-heuristics unaffected — nothing here infers type from bytes; UTF-8 validation is not type
  inference. `uncompressed_len` continues to come from `CompressionInfo` (authoritative).
- Wiring-evidence: every claim is proven by a work-counter / allocation-budget / `size_of` test that
  fails on `main` and passes after (red-then-green), plus the 33-table byte-parity harness across
  LZ4/Snappy/Deflate/Zstd as the load-bearing correctness net.
- CLAUDE.md / website `agents-developing/`: no doctrine text change; this is an internal representation
  change with no user-facing surface.

## Definition of done
`scripts/agent-gate.sh` PASS (SUMMARY pasted) + spec-auditor **C** PASS (every requirement satisfied
with a public-surface test) + roborev clean; `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()`
in library code; 33-table parity + Python + Node binding suites green. Then archive.
