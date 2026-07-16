# Design — Runtime DecodePolicy (F6.4, issue #2211)

## Context
Two lz4 decode call sites exist today:
1. **`storage/sstable/chunk_decompressor.rs::decompress_lz4_chunk`** — the hot production
   compressed-SSTable read path. Reads the compressed record, verifies the 4-byte inline CRC32 over
   the compressed bytes (`stored_crc == crc32fast::hash(&compressed_data)`) **before** decode, then
   calls `lz4_flex::decompress(lz4_data, decompressed_length)` with the exact expected length. This is
   the ONLY site FastUnsafe targets.
2. **`storage/sstable/compression.rs::Compression::decompress` (Lz4 arm)** — a small-block path using
   `decompress_size_prepended`; NOT CRC-preceded at that call. Stays Safe (out of FastUnsafe scope).

The CRC-before-decompress ordering at site 1 is the load-bearing precondition and is already pinned by
`read_compressed_chunk_at_verifies_crc_before_returning`. That pin MUST survive unchanged.

## Decision 1 — a runtime `DecodePolicy`, not a cargo feature
`enum DecodePolicy { Safe, FastUnsafe }`, `Default = Safe`. Threaded into the chunk decompressor
(constructor arg or setter on `ChunkDecompressor`) and read at the lz4 decode branch. Rationale:
`lz4_flex/safe-decode` is a graph-global feature; turning it off to get speed makes the minimal build
(and every other consumer) silently unsafe under feature unification. A runtime enum keeps `safe-decode`
compiled always and localises the unsafe choice to a single deliberate call. Cite issue #2211 body.

## Decision 2 — FastUnsafe needs a SECOND, unconditionally-compiled decoder
`lz4_flex` 0.11.6 selects safe vs unchecked at compile time (`block/decompress_safe.rs` vs
`block/decompress.rs`, `#[cfg(feature = "safe-decode")]`); no runtime unchecked entry point exists, and
one dependency cannot be compiled twice with different features. So the fast path requires a distinct
decoder compiled in **unconditionally** (so the choice is purely runtime, satisfying Decision 1).
Options — **owner picks at Seam 1**:

- **(A) `lz4` / `lz4-sys` (C liblz4)** — `LZ4_decompress_fast` (unchecked) vs `LZ4_decompress_safe`.
  A genuine runtime pair, battle-tested. Cost: a C build dependency (cross-compile/WASM friction, its
  own CVE surface, a `build.rs`/cc toolchain requirement the pure-Rust build avoids today).
- **(B) vendored minimal unchecked lz4 block decoder (in-tree `unsafe`)** — full control, no new
  external dep, pure Rust. Cost: WE own audited `unsafe` and its correctness/UB burden forever; a real
  liability under the "no `unsafe` in library code" leaning of the codebase.
- **(C) `lz4rip`** (from the issue comment) — encapsulated `unsafe`, fewer blocks than `lz4_flex`.
  Caveat: its author states it **validates offsets in both default and `paranoid` builds**, so it may
  not actually expose a *truly unchecked* path — meaning it might deliver little of the 20–30% and
  would still be a second dependency. Verify the achievable delta before adopting.

**Recommendation**: do not adopt any of them until Decision 5's measurement justifies it. If justified,
prefer **(A)** for a real, maintained checked/unchecked pair, accepting the C-dep cost, and keep it
behind the runtime policy so pure-Rust default builds are unaffected in behaviour. Reject (B) unless the
owner explicitly wants to own the `unsafe`.

## Decision 3 — the safety boundary, stated honestly (trusted files only)
CRC32 precedes decode, so for a **non-adversarial** file a passing CRC means the compressed bytes are
intact and an unchecked decode of an intact stream will not read OOB. But the CRC covers the
*compressed* bytes and is written by the same producer: an attacker who can author both the Data.db
payload and its CRC can craft a CRC-valid compressed stream whose lz4 match offsets point out of
bounds. Under FastUnsafe those bounds checks are gone, so **the fast path can read out of bounds / is
undefined behaviour on adversarial input**. Therefore:
- The `unsafe` constructor's `# Safety` doc states: sound **only** for files the operator trusts as
  produced by Cassandra/CQLite and not tampered with; on corrupt/adversarial input, behaviour is
  undefined (possible OOB read / crash / wrong bytes).
- The **flight server default stays Safe**. `--decode-policy fast-unsafe` additionally requires an
  explicit `--assume-trusted-sstables` affirmation (or equivalent), and the active policy is logged at
  startup — mirroring the visible-state pattern of #2128 / #2420.
- We do NOT claim UB-freedom on arbitrary input for FastUnsafe; the honest claim is "Safe is UB-free on
  any input; FastUnsafe is UB-free only on trusted, intact input."

## Decision 4 — scope: CRC-preceded lz4 chunk path only
FastUnsafe applies solely at chunk-decompressor site 1. Snappy/Deflate/Zstd and the small-block
`Compression::decompress` path ignore the policy and stay Safe. Keeps the audited unsafe surface as
small as possible and rides on the one place CRC-before-decode is guaranteed.

## Decision 5 — measure-first is a HARD, blocking gate
No decoder is added and no unsafe path is written until a committed benchmark on the **real corpus**
(a present compressed fixture, not a synthetic micro-bench) shows a **material end-to-end scan
throughput win** attributable to removing lz4 bounds checks — measured on Linux, the deployment target.
The bench must report BOTH the decompress-only delta AND the end-to-end scan delta, because field data
says decode is not the bottleneck. Acceptance threshold is recorded in the bench (proposed: FastUnsafe
must lift end-to-end scan throughput of a decompress-bound workload by a materially useful margin — the
owner sets the number at Seam 1). **If the measurement shows no material win, FastUnsafe is not built
and #2211 closes as not-worth-it.** This is the most likely honest outcome given the field evidence.

## Decision 6 — fuzz strategy: differential-on-valid, not arbitrary-bytes-into-FastUnsafe
Feeding arbitrary bytes to FastUnsafe would (correctly, by contract) trigger OOB reads that ASAN/the
fuzzer reports as crashes — those are *expected*, not bugs, so pointing an arbitrary-bytes fuzz target
at FastUnsafe would only manufacture false crashes. Instead:
- The existing arbitrary-bytes fuzz targets (`fuzz_*`) KEEP using the **Safe** path (unchanged
  guarantee: no panic/hang/OOM on arbitrary input).
- Add a **differential** target/test that generates a *valid* lz4 chunk (compress arbitrary input,
  prepend the length, append the CRC) and asserts **FastUnsafe output == Safe output** byte-for-byte.
  This is the property that actually matters (equivalence on valid input) and is fuzzer-friendly.
- The `# Safety` doc and the fuzz README record explicitly that arbitrary-bytes-into-FastUnsafe is
  out of contract by design.

This satisfies the issue's "include a fuzz target for the unsafe path" while being honest that the only
sound thing to fuzz is the equivalence, not the robustness, of FastUnsafe.

## Alternatives considered
- **Cargo feature `unsafe-lz4-decode`** — rejected (Decision 1): breaks the minimal-build safety
  invariant under feature unification. This is the whole reason #2211 is design-driven.
- **FastUnsafe by default when CRC passed** — rejected: CRC is producer-authored, not a trust anchor;
  default-unsafe violates the "no non-default build silently unsafe" acceptance criterion.
- **Do nothing / close the issue** — a legitimate outcome of Decision 5; recorded as an open fork in
  the proposal.
