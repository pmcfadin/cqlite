# Tasks — Runtime DecodePolicy (F6.4, issue #2211)

Branch `issue-2211-decode-policy`. Anchors are `main`-relative and drift — re-grep before editing.
**Stage 0 is a HARD gate: if it shows no material end-to-end win, STOP, do not build FastUnsafe, and
close #2211.** Nothing in stages 2+ is written until the owner accepts the Stage-0 result and picks a
backend (Decision 2).

## Stage 0 — measure-first (blocking; no unsafe code yet)
- [ ] 0.1 Add a Criterion/bench (or a gated integration bench) that decodes a present real compressed
  LZ4 fixture and reports decompress-only throughput for checked `lz4_flex`. (runtime-decode-policy)
- [ ] 0.2 Add an end-to-end scan bench over the same fixture reporting scan throughput, so the
  decompress delta can be put in end-to-end context. (runtime-decode-policy)
- [ ] 0.3 Prototype the chosen fast backend (Decision 2 option A/B/C) *locally only* to obtain a
  FastUnsafe-vs-Safe number on Linux; commit the benchmark result artifact (both deltas). Do NOT merge
  the prototype. (runtime-decode-policy)
- [ ] 0.4 **Decision point**: if the end-to-end win does not meet the owner-set threshold, close #2211
  as not-worth-it and archive this change with a "measured, not justified" note. Otherwise proceed.

## Stage 1 — the policy type (Safe default, no unsafe yet)
- [ ] 1.1 Add `enum DecodePolicy { Safe, FastUnsafe }` with `#[derive(Default)]`/`Safe` default in
  `cqlite-core` (e.g. `storage/sstable/`); no `From<&str>` route to `FastUnsafe`. Red test: default is
  `Safe`; no ordinary constructor yields `FastUnsafe`. (runtime-decode-policy)
- [ ] 1.2 Thread the policy into `ChunkDecompressor` (constructor arg / setter) defaulting to `Safe`;
  read it only at the `decompress_lz4_chunk` branch. All existing call sites pass `Safe` — zero
  behaviour change. Preserve the CRC-before-decompress pin. (runtime-decode-policy)
- [ ] 1.3 Test: a `Safe` decompressor is byte-identical to today on the parity fixtures (all four
  compression algorithms unaffected). (runtime-decode-policy)

## Stage 2 — the FastUnsafe backend + unsafe constructor (only if Stage 0 justified it)
- [ ] 2.1 Add the chosen unchecked lz4 backend as an **unconditional** dependency (Decision 2). Confirm
  minimal-features and default builds still compile the checked path and do not select the fast path.
  (runtime-decode-policy)
- [ ] 2.2 Add the `unsafe` (or unsafe-named) `FastUnsafe` constructor with a `# Safety` doc stating the
  trusted-files-only precondition and UB-on-corrupt-input consequence. (runtime-decode-policy)
- [ ] 2.3 Wire the `FastUnsafe` branch at the CRC-preceded lz4 site only; Snappy/Deflate/Zstd and the
  small-block `Compression::decompress` path stay checked. Test: policy ignored for non-lz4 and
  small-block paths. (runtime-decode-policy)

## Stage 3 — differential fuzz/parity + robustness
- [ ] 3.1 Add a differential test/fuzz target: compress arbitrary input → valid chunk (length + CRC) →
  assert `FastUnsafe` output == `Safe` output byte-for-byte. (runtime-decode-policy)
- [ ] 3.2 Confirm the arbitrary-bytes `fuzz_*` targets still exercise the Safe path only; document in
  the fuzz README that arbitrary-bytes-into-FastUnsafe is out of contract. (runtime-decode-policy)
- [ ] 3.3 Test: a CRC mismatch under `FastUnsafe` returns the typed error and never enters the
  unchecked decoder. (runtime-decode-policy)

## Stage 4 — flight plumbing (visible, Safe-default, trusted affirmation)
- [ ] 4.1 Add `--decode-policy` (`safe`|`fast-unsafe`) + `CQLITE_DECODE_POLICY` to `cqlite-flight`
  (mirror `--max-concurrent-scans`, #2420), default `safe`. (runtime-decode-policy)
- [ ] 4.2 Require an explicit `--assume-trusted-sstables` (or equivalent) affirmation alongside
  `fast-unsafe`; without it, fail/stay Safe with a message naming the flag. Log the active policy +
  trusted warning at startup. Tests: default is Safe; fast-unsafe without affirmation refuses.
  (runtime-decode-policy)
- [ ] 4.3 Document the trusted-only boundary in flight `--help` and rustdoc. (runtime-decode-policy)

## Stage 5 — close-out
- [ ] 5.1 Update `docs/` (dev-cookbook / feature-flags note) that FastUnsafe is a runtime, trusted-only
  choice, never a build feature. (runtime-decode-policy)
- [ ] 5.2 Run `scripts/agent-gate.sh` (full) — PASS; paste the AGENT-GATE SUMMARY into the PR.
- [ ] 5.3 roborev clean; spec-auditor (C) PASS (every requirement satisfied with a public-surface
  test); then `openspec archive runtime-decode-policy`.
