# Runtime DecodePolicy — Safe default / opt-in FastUnsafe lz4 (F6.4)

## Milestone
0.15 (cqlite-trino latency/throughput/operations — lane: scan latency). **Design-driven**: routed
through OpenSpec per the owner decision on issue #2211 (decomposed out of #1596, Epic F #1518). This
is F6.4 of the F6 read-path decode-throughput family.

## Oracle vs design
Design-driven. There is no external oracle for *how fast* to decode — lz4 decode output is byte-fixed
(the parity goldens already pin it), but the **policy surface** (a runtime Safe/FastUnsafe knob, its
safety contract, where it plumbs) has real latitude. The output-parity half stays oracle-pinned: a
FastUnsafe decode of a valid chunk MUST be byte-identical to a Safe decode (the 33-table JSONL
goldens are the truth).

## Problem (claimed, must be MEASURED before any code)
`lz4_flex`'s `safe-decode` feature (default) adds per-copy bounds checks upstream reports at
~20–30% of *decompress* time. On the hot compressed-SSTable read path
(`chunk_decompressor.rs::decompress_lz4_chunk`) a CRC32 over the compressed bytes is verified
**before** decode (`read_compressed_chunk_at_verifies_crc_before_returning` pins the ordering), so the
on-disk bytes are already integrity-checked for a *trusted* file — making an unchecked decode
*plausibly* safe there. But: (a) the 20–30% is a decompress-only micro-number, not an end-to-end scan
number; field evidence (round 7–10) shows scan cost is dominated by index parsing (#2385), snapshot
resolve/lifecycle, and single-node routing (#2397) — **not** lz4 bounds checks; and (b) an unchecked
decode is memory-unsafe on adversarial input. So this must be measured end-to-end AND fuzzed, and
FastUnsafe must never be default-on.

## Why a RUNTIME policy, not a cargo feature (owner + flow-lead decision, restated)
Cargo features are additive/unifying. An opt-OUT of `lz4_flex/safe-decode` is inexpressible without
making some *other* build (notably the mandated minimal-features build) **silently unsafe** under
feature unification: any crate in the graph turning `safe-decode` off flips it off for everyone. A
runtime `DecodePolicy` keeps `safe-decode` **always compiled** and makes the unsafe choice explicit,
local, and per-open — the same binary, an operator's deliberate call, never a build-wide flag.

## Key technical fork this change must resolve (NEW finding)
`lz4_flex` 0.11.6 picks safe vs unchecked decode at **compile time** — two separate modules
(`block/decompress_safe.rs` vs `block/decompress.rs`) selected by `#[cfg(feature = "safe-decode")]`.
It exposes **no runtime-selectable unchecked function**. You cannot instantiate `lz4_flex` twice with
different features in one dependency graph. Therefore a runtime Safe/FastUnsafe choice **requires a
second, unconditionally-compiled decoder** for the fast path (checked `lz4_flex` stays the Safe path).
The design records the three backend options (C `lz4-sys`, vendored in-tree unchecked decoder,
`lz4rip`) and a recommendation; picking one is Seam-1 owner input, not the implementer's call.

## What changes
- A `DecodePolicy` enum in `cqlite-core` (`Safe` default, `FastUnsafe`), threaded to the **one**
  CRC-preceded lz4 chunk-decode site. Default `Safe` — zero behaviour change unless opted in.
- `FastUnsafe` reachable **only** via an explicit, `unsafe`-marked, documented constructor whose
  safety contract states the trusted-input precondition. Never `Default`; never inferred from bytes
  or config strings (no-heuristics, #28).
- A second, unconditionally-compiled unchecked lz4 backend (option chosen at Seam 1) for the fast
  path; `lz4_flex` with `safe-decode` stays the Safe path and the always-on default.
- Flight server: a `--decode-policy` / `CQLITE_DECODE_POLICY` knob (mirrors `--max-concurrent-scans`
  from #2420) whose default is **Safe**; selecting `fast-unsafe` requires the operator to also affirm
  a trusted-files flag, and the choice is logged at startup.
- A committed before/after benchmark on the **real corpus** and a differential fuzz/parity of
  FastUnsafe-vs-Safe on valid chunks, as blocking preconditions.

## Non-goals
- Not a cargo feature and not any change to the minimal/default feature set (they stay Safe-only).
- No FastUnsafe for Snappy/Deflate/Zstd, nor for the non-CRC small-block `Compression::decompress`
  path — lz4 chunk path only, initially.
- No change to any decoded value: FastUnsafe output MUST be byte-identical to Safe on valid input
  (parity goldens unchanged).
- Not a claim that CQLite decodes *untrusted* files fast — FastUnsafe is trusted-files-only; the
  flight default stays Safe.
- No pre-`na` format support introduced or revisited (version floor unchanged).

## Impact
- **No-heuristics (#28)**: policy is explicit runtime config only, never inferred.
- **Memory budget (<128MB)**: unchanged (decode buffers are already chunk-bounded).
- **Bindings/CLI**: no change unless a binding chooses to expose the policy later (out of scope here).
- **If the measurement shows no material end-to-end win, the FastUnsafe path is NOT built and #2211
  closes** — the measure-first gate can legitimately end this work.
