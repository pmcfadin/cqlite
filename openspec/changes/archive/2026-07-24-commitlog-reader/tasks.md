## 1. Test fixtures + oracle infrastructure (D2)

- [x] 1.1 Add a Docker-based CommitLog fixture generation script under `test-data/scripts/` (reuses
      the existing Cassandra 5.0 Docker image): run a known, versioned set of CQL inserts against a
      real node, capture the CommitLog segment before discard.
- [x] 1.2 Record the known insert set (table/partition/cell values) as the ground-truth oracle
      alongside the captured segment fixture (gitignored binary + committed ground-truth manifest,
      mirroring the SSTable `*.jsonl` goldens pattern).
- [x] 1.3 Generate at least one deliberately truncated/torn fixture (simulated crash mid-write) and one
      corrupt-CRC fixture, for the truncation-tolerance and malformed-input requirements.
- [x] 1.4 Document the new fixture class + regeneration steps (extends the existing test-data docs, not
      a standalone doc).

## 2. Descriptor parsing (spec: CommitLog descriptor parsing)

- [x] 2.1 Implement `storage/commitlog/descriptor.rs`: `CommitLogDescriptor` parsing (segment id,
      version, compression params) against the real fixture from 1.1, verified field-for-field.
- [x] 2.2 Implement `CommitLogVersionGates` (mirrors `BigVersionGates`/`BtiVersionGates`): accept only
      the Cassandra 5.0-era commitlog version, reject others via typed `Error`.
- [x] 2.3 Test: valid header parses correctly (segment id/version match fixture ground truth).
- [x] 2.4 Test: unsupported version is rejected without attempting mutation-stream parsing.

## 3. Frame + mutation-stream decoding (spec: mutation stream decoding, streaming decode)

- [x] 3.1 Implement `storage/commitlog/frame.rs`: per-record framing (length, length-CRC, payload,
      payload-CRC) and sync-marker-delimited section walking.
- [x] 3.2 Implement `storage/commitlog/mutation.rs`: decoded mutation representation (table, partition
      key, cell values) distinct from `write_engine::mutation`.
- [x] 3.3 Implement `storage/commitlog/reader.rs`: `CommitLogReader` as a streaming
      iterator/generator over decoded mutations — no whole-segment `Vec<Mutation>` materialization.
- [x] 3.4 Test (parity oracle): decode the 1.1/1.2 fixture and assert the decoded mutation set matches
      the recorded ground-truth insert set exactly (table/partition/cell values).
- [ ] 3.5 Test (streaming/memory): assert peak memory does not scale with whole-segment
      materialization (dhat-heap-gated test, following the `test_issue_827_merge_streaming_memory.rs`
      pattern referenced in design.md).

## 4. Truncation + malformed-input safety (spec: torn-tail tolerance, malformed input never panics)

- [x] 4.1 Test: the 1.3 truncated fixture returns all cleanly-decoded mutations before the tear and
      reports truncation, without panicking.
- [x] 4.2 Test: the 1.3 corrupt-CRC fixture returns a typed `Error`, without panicking.
- [x] 4.3 Add `fuzz/fuzz_targets/fuzz_commitlog_frame.rs` exercising the frame parser on arbitrary
      bytes; run locally to confirm no panic/hang/OOM before relying on CI's smoke pass.
- [x] 4.4 Add the new `Error` variants (`UnsupportedCommitLogVersion`, corrupt-frame variant, etc.) to
      `cqlite-core/src/error.rs`, matching the existing `thiserror`-based style — no `unwrap()`/
      `expect()` anywhere in the new module.

## 5. Compression fail-closed (spec: compressed segments fail closed)

- [x] 5.1 Detect a compressor class in the parsed descriptor and return a typed "unsupported" `Error`
      before attempting mutation-stream decode.
- [x] 5.2 Test: a compressed-segment fixture (or a descriptor-only synthetic fixture if generating a
      full compressed segment via Docker is impractical) is rejected cleanly, not misdecoded.

## 6. Public surface — wiring evidence (spec: public surface with end-to-end wiring evidence)

- [x] 6.1 Add the library entry point (`CommitLogReader::open`) to `cqlite-core`'s public API surface.
- [x] 6.2 Add the `read-commitlog` CLI subcommand to `cqlite-cli/src/cli_types.rs`, alongside
      `read-sstable`/`write-stats`, following the existing output-writer conventions.
- [x] 6.3 Integration test: invoke the CLI subcommand end-to-end against a real fixture segment and
      assert the reported output matches the decoded mutation set — the named wiring-evidence test.

## 7. Review-first, then the one gate (implement loop, per CLAUDE.md)

- [x] 7.1 `scripts/agent-gate.sh --lite` after each meaningful unit of work (fmt + scoped clippy +
      blast-radius tests), summary-file redirect only.
- [ ] 7.2 `rust-reviewer` + roborev (`--branch --base origin/main`) on the lite-green diff, BEFORE any
      full gate — fix any blocker findings, batch nits into a follow-up issue.
- [ ] 7.3 Open the PR against `pmcfadin/cqlite` (target: `main`) referencing #2389, noting #2388 as the
      explicit non-goal follow-on.
- [ ] 7.4 Hand off to `flow-closer`: the ONE full `agent-gate.sh` of record → `spec-auditor` (C) intent
      audit anchored to `openspec/changes/commitlog-reader/specs/**` → final roborev pass →
      merge-on-green → `flow-finalize` (archive the OpenSpec change, remove the worktree/branch, close
      #2389 with a traceable comment).

## 8. Documentation (keep doctrine current in the same change)

- [x] 8.1 Add a chapter/section to `docs/sstables-definitive-guide/` or a sibling doc describing the
      CommitLog segment format as understood/implemented (mirrors how SSTable components are
      documented) — scoped to what was actually verified against real fixtures, not the full
      class-level research doc's speculative surface.
- [x] 8.2 Note the new capability + module boundary in `CLAUDE.md`'s workspace-structure /
      source-map pointers if it changes where a future agent should look.
