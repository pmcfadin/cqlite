# Tasks: cold-open-ledger (A5)

## 1. Read-work counters (TDD: tests first)
- [ ] 1.1 Add `cqlite-core/src/storage/sstable/read_work_counters.rs`: `TRIE_WALKS`,
      `DECOMPRESS_CALLS`, `SEEK_CALLS`, `FILE_OPENS` atomics; unconditional
      `record_*()` fns with `#[cfg(any(test, feature = "work-counters"))]` bodies;
      `#[cfg(any(test, feature = "work-counters"))]` getters + `reset()`; fd
      high-water helper (`/dev/fd` macOS, `/proc/self/fd` Linux, `None` else).
      Module doc names each counter's consumer epic-child. `pub mod` it.
- [ ] 1.2 Add Cargo feature `work-counters` (off by default) to `cqlite-core/Cargo.toml`.
- [ ] 1.3 Unit test (serialized on the shared test mutex): local-instance
      round-trip + `reset()` → 0 (deterministic, per #1071).
- [ ] 1.4 Wire increment call sites (unconditional): decompress entry
      (`compression.rs`), BTI trie descent (`data_access/bti.rs`), block-read seek
      (`reader/block_io.rs`), `BlockSource` open (`reader/source.rs`).
- [ ] 1.5 Wiring-evidence integration test (`work-counters` feature): reset →
      cold open + single-chunk point read via the public API → assert
      `DECOMPRESS_CALLS`/`FILE_OPENS`/`TRIE_WALKS` moved as expected.

## 2. Unified history ledger
- [ ] 2.1 Add `cqlite-core/benches/bench_ledger/mod.rs`: `LedgerRecord
      {ts,commit,bench,metric,value,unit}`; `append_metrics(bench, &[(name,value,unit)])`;
      path via `CQLITE_BENCH_LEDGER` env else `<MANIFEST>/../target/profiling/history.jsonl`;
      reuse A2 `current_commit`; best-effort append (log, never abort).
- [ ] 2.2 Pure unit test for record serialization / path resolution.
- [ ] 2.3 Migrate `benches/tail_latency/mod.rs` onto `bench_ledger` (drop its
      `LedgerRecord`/`append_ledger`/`default_ledger_path`); emit per-metric lines.
- [ ] 2.4 `scripts/profile_report.py`: write criterion medians + peak heap in the
      unified `{ts,commit,bench,metric,value,unit}` schema to
      `target/profiling/history.jsonl`; add a longitudinal per-metric reader
      (latest value + delta vs previous distinct commit) rendered in report.md.
- [ ] 2.5 Round-trip test for the report reader (write ledger → read back → assert).
- [ ] 2.6 `.gitignore`: add `target/profiling/history.jsonl` intent (already under
      `/target`); remove the `cqlite-core/benches/tail-latency-history.jsonl` line.

## 3. Cold-open + memory benches
- [ ] 3.1 Add `cqlite-core/benches/open.rs` (`harness = false`): `open/cold_big`,
      `open/cold_bti` (skip-on-absent, panic-on-broken); appends medians via
      `bench_ledger`.
- [ ] 3.2 Add `mem/open_n_readers` (RSS/heap after N opens) → appends a memory
      metric via `bench_ledger`.
- [ ] 3.3 Register `[[bench]] open` in `Cargo.toml`; enable `work-counters` in the
      bench's required-features where it reads counters.

## 4. Docs
- [ ] 4.1 `docs/profiling.md`: document the unified ledger schema + path + CI-artifact
      choice; update the history.jsonl table row.
- [ ] 4.2 `cqlite-core/benches/README.md`: replace the "History ledger" section with
      the unified schema; document `open`/`mem` benches.

## 5. Gate + review
- [ ] 5.1 `scripts/agent-gate.sh` PASS (paste SUMMARY verbatim); `RUSTFLAGS="-D warnings"`
      clean; no `unwrap()`/`expect()` in library code.
- [ ] 5.2 spec-auditor (C) PASS anchored to `openspec/changes/cold-open-ledger/specs/**`.
- [ ] 5.3 roborev `--base origin/main --agent codex` clean.
