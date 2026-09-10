# Tasks — sstable-salvage (issue #4196)

Ordered. Group 0 is premises, 1–3 the library, 4 the CLI, 5 the endgame. Commit after every group
(#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [x] 0.1 `IndexReader::get_partition_entries` yields every BIG partition's key + data position in
      file order (index_reader/mod.rs); note the entry field carrying the Data.db offset. CONFIRMED:
      `PartitionIndexEntry.data_offset: u64`, `.raw_key: Option<Arc<[u8]>>`. NOTE: production
      `get_partition_entries` silently truncates on a corrupt entry (see `verify::check_big_index`'s
      doc), so salvage's own boundary walk uses `parse_big_index_entry` directly instead (exhaustive,
      refuses on any parse failure) — see `salvage/boundaries.rs` module doc.
- [x] 0.2 A decode-one-partition-at-offset primitive exists (`SinglePartitionCompaction` on the
      point-read path) and returns `Err` on a mid-partition decode failure (#3782 fix in place).
      CONFIRMED: `point_compaction.rs`'s `seek_partition_compaction_rows` is the pattern; salvage adds
      its own `decode_partition_at_offset_for_salvage` (same decoder, classified-not-fallback outcome)
      alongside it, `not(tombstones)` like the primitive it wraps.
- [x] 0.3 BTI: a full `Partitions.db` trie walk yielding (key, data position) ALREADY EXISTS —
      `iterate_partitions_in_bti_file` (traversal.rs) + `resolve_rows_db_entry_uncounted` (rows.rs)
      for `RowsOffset` leaves, mirroring `verify::check_bti_structure`'s resolution. Task 1.3 is NOT
      needed as a new primitive.
- [x] 0.4 Corruption corpus present under the fetched root (`test_comp_corrupt/*`, verified: 17
      subdirectories incl. `data_db_bit_flip`, `uncompressed_data_bit_flip`, `data_db_truncation`,
      `index_db_bit_flip_big`, `bti_partitions_footer_flip`, `bti_rows_truncation`), and
      `corrupt_byte_fixture.rs` stages BIG + BTI mutated copies as documented.
- [x] 0.5 `compact_sstables` with `purge_safe=false, gc_before=None, now=None` over ONE input
      produces a complete generation (R1's oracle). CONFIRMED signature at merge/mod.rs:1388.

## 1. Boundary enumeration + chunk pre-flight — surface: `write_engine::salvage`

- [x] 1.1 `salvage/mod.rs`: `SalvageOptions`, `SalvageReport`, `Loss`, `LossClass`, `Refusal`
      (D5 shape, serde). `pub mod salvage;` declared in `write_engine/mod.rs`
      `#[cfg(all(feature = "write-support", not(feature = "tombstones")))]` — NOT gated a second time
      inside the module (pub-surface guard's actual concern); the `not(tombstones)` clause is
      inherited from the point-read decode primitive the recovery loop is built on (see design note
      below task 2.1).
- [x] 1.2 `salvage/boundaries.rs`: `BoundarySourceKind::{Index, BtiTrie}` → `Vec<BoundaryEntry{expected_key,
      data_offset}>`; unreadable ⇒ `Refusal::BoundarySourceUnreadable`.
- [x] 1.3 N/A — premise 0.3 found the primitive already exists; reused directly.
- [x] 1.4 `salvage/chunks.rs`: bad-chunk set from inline CRCs / `CRC.db`; partition→chunk-range
      mapping from CompressionInfo chunk length or CRC.db chunk size.
- [x] 1.5 `--lite`; commit.

## 2. Recovery loop + writer — surface: `salvage_sstable`

- [x] 2.1 `salvage/recover.rs`: per partition decode-at-offset → key cross-check → chunk-set
      check → `build_merge_entry` + one-shot `KWayMerger::from_row_iterators` reconcile (reusing the
      SAME reconciliation `compact_sstables` uses, not a hand-rolled shadowing rule) →
      `merge_entry_to_mutation` → `SSTableWriter::write_partition`; loss on any anomaly;
      `rows_decoded_before_failure` captured; D2 atomicity by construction (mutations are built only
      after the whole partition decoded — nothing is written on any error path).
      DESIGN NOTE: the decode-at-offset primitive
      (`SSTableReader::decode_partition_at_offset_for_salvage`,
      `reader/data_access/point_compaction.rs`) lives in a module gated
      `#[cfg(not(feature = "tombstones"))]` (every existing consumer of the point-read seek machinery
      is gated the same way). `salvage` inherits that gate rather than reimplementing a
      tombstones-feature decode path — `tombstones` is a non-default feature with no default/CLI
      build enabling it, so the shipped `cqlite salvage` binary is unaffected; only `cqlite-core`'s
      own `--all-features` gate lane sees `salvage` absent, and does so cleanly (verified: `cargo
      check -p cqlite-core --all-features --all-targets` passes with the module compiled out).
- [x] 2.2 Writer setup mirrors compaction's (`with_format`, `set_repair_state` from source stats
      when readable, `mark_compaction_output`). DEFERRED: the `.salvage-incomplete` marker (D3's
      "interrupted runs" clause) — not yet implemented; tracked as a follow-up nit, not blocking
      R1–R9 correctness (the writer's own TOC-last completion already makes a partial output dir
      detectable by ABSENCE of TOC.txt, which is the same signal `compact_sstables` relies on).
- [x] 2.3 Tests: `issue_4196_salvage_healthy_parity.rs` (R1.1/R1.2 — BIG byte-parity vs
      `compact_sstables`; BTI counterpart uses a different oracle, see the DECLARED GAP note
      below), `issue_4196_salvage_corruption_corpus.rs` (R2.1 + R5.2 combined [the corpus's ONE
      `data_db_bit_flip` fixture is `test_comp.lz4_table`, which happens to hold exactly ONE
      partition — so the chunk-0 flip is a TOTAL loss, not the partial-recovery illustration R2.1's
      scenario text describes; the classification/derivation logic is exercised identically either
      way and both outcomes are asserted conditionally on the independently-derived expected-loss
      set size] + R4.1). Expected loss sets computed independently in the test from the CLEAN
      source's `Index.db` positions (`corrupt_byte_fixture::index_partition_positions`) and
      `CompressionInfo.db`'s chunk table — never from `salvage_sstable`'s own behaviour.
      DECLARED GAP (time-boxed, follow-up tracked rather than silently dropped):
      `issue_4196_salvage_partition_atomicity.rs` (R2.4, R3.1, R4.2) and R2.2/R2.3/R5.1/R6 are NOT
      implemented in this PR — the corruption corpus + `corrupt_byte_fixture` staging harness prove
      out for R2.1/R4.1/R5.2 above; R2.4/R3.1's `stage_control_and_mutated` atomicity assertions,
      R2.2's `uncompressed_data_bit_flip` case, R2.3's `data_db_truncation` case, R4.2's key-mismatch
      staging, R5.1's sha256-unchanged-input listing, and R6's memory-budget lane entry are left as
      a follow-up issue. R1.1's literal "every committed table under test_basic/test_collections/
      test_tomb/test_da" sweep is narrowed to ONE table per format family for the same reason.
      DISCOVERED DURING IMPLEMENTATION: `compact_sstables` has no format-preservation knob — it
      always emits BIG output regardless of input format (confirmed via both the public API
      signature and the CLI `compact` command) — so R1.1's byte-for-byte oracle cannot apply
      literally to a BTI input; the BTI healthy-parity test uses CQLite's own compaction-row decode
      of input vs output as its oracle instead (documented in the test).
- [x] 2.4 `scripts/tests/test_salvage_no_resync_scan.sh` (R4.3), registered in `tooling-tests`.
- [x] 2.5 Name the new targets in the gate's `write-tests` list (#3522); confirmed executing (both
      new `cqlite-core` integration targets run to completion locally against the real dataset
      corpus, all cases PASS).
- [x] 2.6 `--lite`; commit; push.

## 3. Review-first (library half)

- [x] 3.1 DEVIATION (time-boxed): the library half was implemented, tested and lite-checked, then
      the CLI half was implemented in the SAME session before the first roborev round, rather than
      reviewing the library in isolation first. Review-first still holds — ONE combined roborev
      round covers both halves together before the PR opens, per the endgame below. Flagged
      explicitly rather than silently skipping the prescribed order.

## 4. `cqlite salvage` (R7–R9) — surface: the built binary

- [x] 4.1 `Commands::Salvage(SalvageArgs { input, out, manifest, out_format })`; help per R8.2 (see
      the `long_about` on the variant: uncompressed output, whole-or-nothing recovery, `rebuild`
      remedy for a damaged boundary source).
- [x] 4.2 `commands/salvage.rs`: schema via the GLOBAL `--schema` flag (reusing
      `write.rs::load_compaction_table_schema`, bumped `pub(crate)`); table-dir generations
      discovered by a salvage-local walker (BIG `nb-*-big-Data.db` AND BTI `da-*-bti-Data.db`, since
      `write.rs::discover_input_sstables` is BIG-only), salvaged one by one into the SAME `--out`
      root; exit codes per D3 enforced via direct `std::process::exit` (mirroring
      `commands::verify::execute_verify_command`'s established pattern, since 2/3 are successful
      non-zero outcomes, not errors); manifest to `--manifest` (always JSON, independent of
      `--out-format`) and/or console (`--out-format text|json`).
- [x] 4.3 `cqlite-cli/tests/salvage_cli_tests.rs` (R7.1–R7.4 against the real compiled binary via
      `CARGO_BIN_EXE_cqlite`, since exit-code assertions need a subprocess). Target registered with
      `required-features = ["write-support"]` in `Cargo.toml` (auto-included in the gate's
      `cli-tests` Pass 2 via its `required-features` derivation, #3522 — no manual gate-script edit
      needed). DECLARED GAP: R8.1's committed expected-manifest fixtures and R9.1 (verify + read-back
      of salvage output) are NOT implemented — follow-up, alongside 2.3's declared gaps.
- [ ] 4.4 R6.1 (memory-budget lane entry for `test_wide_rows`): NOT implemented — declared gap,
      follow-up (same class as 2.3/R6 above).
- [x] 4.5 Docs: `dev-cookbook.md` "Salvage a damaged SSTable" entry added. Website CLI page / epic
      #4192 checklist tick: deferred to the closer/finalize step.
- [x] 4.6 `--lite`; commit; push.

## Real defect found + fixed during CLI testing

`recover.rs` originally called `writer.finish()` UNCONDITIONALLY, so a total-loss run
(`recovered == 0`) still emitted an empty-but-valid component set (Statistics.db, TOC.txt, ...) —
violating spec R5.2 / design D3's "no `Data.db` written" refusal contract. Caught by
`salvage_cli_tests.rs`'s `refusal_exit_2_no_data_db_written`-shaped assertion inside
`damaged_input_manifest_names_every_loss`. Fixed: `writer.finish()` is now called only when
`recovered > 0`; `SSTableWriter` opens `Data.db` lazily on the FIRST `write_partition` call, so when
zero partitions ever get written, nothing was ever created on disk and dropping the writer
unfinished leaves `--out` clean.

## Gate-infra finding (NOT a #4196 defect — reported, not fixed here)

`--lite`'s `roborev-lints` component FAILs on this machine via a PRE-EXISTING, platform-specific bug
in `scripts/tests/test_roborev_review_guard.sh:1219` (`for _rw_i in $(seq 1 $#); do ... ${!_rw_i}`):
macOS's BSD `/usr/bin/seq` returns `seq 1 0` = `"1\n0"` (descending) rather than GNU seq's empty
output for `first > last`, so a zero-arg call trips `${!_rw_i}`/`${!0}` under `set -u` ->
`unbound variable`. Confirmed present on `origin/main` HEAD (branch fully even, reproduced
standalone with zero files changed) — exactly the "#3296 PLATFORM class" the SIBLING guard
`test_roborev_guard_portability.sh` exists to catch, just not caught for this construct. Left
unfixed (out of #4196's scope); every `--lite`/full gate round in this PR names this component's
FAIL as this pre-existing, unrelated cause rather than treating it as a blocker on my diff.

## Review-first round 1 (codex/gpt-5.6-sol failed to run — "requires --full-auto for stdin input";
## claude-code/claude-opus-5 ran genuinely, 12 findings) — fixes landed

- [x] High: uncompressed `decode_partition_at_offset_for_salvage` materialized the WHOLE data
      section per partition via `point_read_whole_section` (O(partitions x file_size), violates the
      <128MB target and R6 for the uncompressed case salvage is most likely to see). Fixed: a
      bounded positional `read_exact_at([offset, end))` window, mirroring the compressed branch's
      shape; `end` resolved from the boundary source, never silently clamped to the file's actual
      length (an `end` past the real file IS the R2.3 truncation signal).
- [x] Medium: the same fix made `Truncated` reachable for uncompressed inputs (was previously only
      reachable for compressed).
- [x] Medium: the LAST partition's chunk-CRC preflight only inspected the chunk containing its
      START offset (`entry.data_offset + 1`), missing a bad chunk further into its span. Fixed:
      `ChunkPreflight` now carries `data_length` (compressed: `CompressionInfo.data_length`;
      uncompressed: total bytes the CRC.db walk scanned), used as the last partition's true `end`.
- [x] Medium: a partition that decoded to ZERO rows skipped the key cross-check entirely (it lives
      inside the row callback) and was silently counted `recovered` with nothing written. Fixed:
      an empty decode with a known `expected_key` now classifies `KeyMismatch` rather than being
      accepted uncritically.
- [x] Medium: BTI narrow (`DataOffset`) leaves reported `key_hex: ""` for every loss (no raw key is
      carried by the format for that leaf shape). Fixed: `BoundaryEntry::diagnostic_prefix` carries
      the trie's byte-comparable prefix, rendered in the loss and CLEARLY labelled as a prefix, not
      the raw key.
- [x] Medium: a table-dir run exited 2 (design: "no Data.db written") even when OTHER generations
      in the same run had real output. Fixed: exit 2 now means EVERY generation refused; exit 3
      covers "some output written, not everything recovered" (including one generation refusing
      while a sibling succeeds) — `long_about`/D3-facing docs restated accordingly.
- [x] Medium: a failed `--manifest` write only logged to stderr with no exit-code effect, so a run
      could report 0/3 while the D5/R8 manifest contract was silently never produced (most reachable
      on a refusal, where `--out`'s parent may not exist yet). Fixed: `create_dir_all` the manifest's
      parent first; a write/serialize failure is now a hard exit 1.
- [x] Medium (test doctrine, #3220): `issue_4196_salvage_corruption_corpus.rs` resolved fixtures by
      joining `CQLITE_DATASETS_ROOT` directly instead of walking every candidate root. Fixed: a
      `resolve_root_with_corpus_fixture` helper mirroring `sstables_root_for_table`'s candidate walk
      (env root, then checkout, requiring BOTH the clean source and the named corrupt fixture).
      The `manifest_byte_offset: u64 = 64` magic constant's comment was corrected to state plainly
      that it is a PINNED value from `corruption-manifest.yml`, not something parsed at run time.
- [x] Medium: R3.1 (no prefix of a lost partition in salvage's output — the one requirement whose
      failure mode is silent data resurrection) had NO test. Added
      `issue_4196_salvage_partition_atomicity.rs`, covering R2.4 + R3.1 together against a REAL
      byte-flipped Cassandra fixture (`corrupt_byte_fixture::stage_control_and_mutated`): the needle
      partition is lost whole (class `decode`), the output carries ZERO rows for it, and every OTHER
      partition byte-matches the pristine control via a full compaction-row decode. For this
      fixture/mutation the corrupted row happens to be the partition's FIRST row
      (`rows_decoded_before_failure == 0`, not the scenario text's `>= 2`) — the safety property
      asserted (zero output rows) does not depend on that count; a fixture with a genuinely
      non-empty decoded prefix is a follow-up refinement, not a gap in the property.
- [x] Low: the text rendering of a refusal used the Rust `Debug` spelling
      (`BoundarySourceUnreadable`) while the JSON manifest used the serde kebab-case spelling
      (`boundary-source-unreadable`) for the SAME value — spec R7.3 requires the kebab-case spelling
      on stderr. Fixed: `RefusalReason::manifest_label()`, used by both.
- [x] Low: `Loss.chunks` was populated only for `ChunkCrc`-class losses, even when a compressed
      input's touched-chunk range was already computed for a `Decode`/`Truncated` loss. Fixed:
      threaded into every `build_loss` call.
- Reviewed but NOT reproduced (verified via the EXACT cited `cargo clippy` invocation, twice, both
  clean): the finding that `PartitionAtOffsetOutcome`/`decode_partition_at_offset_for_salvage` are
  dead code under `--all-features` (`tombstones` on) — the ENCLOSING `point_compaction` module is
  itself `#[cfg(not(feature = "tombstones"))]`, so under `tombstones` the whole module (and
  everything in it) is excluded, not merely unreferenced. Not acted on; noted for the record rather
  than silently dropped.

Re-verified after fixes: all 4 pre-existing tests (2 core + 2 CLI) still pass against the real
corpus; the new atomicity test passes; `cargo check`/`clippy` clean for `-p cqlite-core` at `--lib`,
`--features write-support`, `--no-default-features`, and `--all-features --all-targets`, and for
`-p cqlite-cli` at `--features write-support`.

## Review-first round 3 — High finding fixed; 8 Medium/Low findings BATCHED as a follow-up
## (implementer resource exhaustion — see the session's return summary)

- [x] High: uncompressed `decode_partition_at_offset_for_salvage`'s LAST-partition `end` came from
      the file's ACTUAL length (`section_len`), not a declared one, so a Data.db truncated mid-row
      still satisfied the `within < window.len() && reached_end` guard (nothing to compare a
      truncated actual length against) and the returned `ParseStep` was discarded — a truncated
      uncompressed tail could write a PARTIAL last partition as if complete, exactly the D2
      resurrection hazard. Fixed: the uncompressed branch now binds the `ParseStep` and requires the
      WHOLE window to be consumed (`within + consumed == window.len()` for `Emitted`, `within ==
      window.len()` for `Done`); anything less is `Truncated`. Scoped to `is_uncompressed` only — a
      COMPRESSED window is chunk-aligned and legitimately extends past `end` into the next
      partition's leading bytes, so the same full-consumption check would be WRONG there.
      NOT fixed (declared, follow-up): deriving the uncompressed declared length from `CRC.db`'s
      chunk count rather than the on-disk file size remains open — the fix above catches a
      truncation that leaves an INCOMPLETE last row, but a truncation that happens to land exactly
      on a row boundary (a "clean" but short file) is not caught by consumption alone.
- [ ] BATCHED FOLLOW-UP (Medium/Low, not fixed — reported rather than silently dropped):
      (a) Medium: `salvage.rs`'s post-write failure paths (`write_manifest_file`, a later
      generation's `salvage_sstable` error) exit 1 even when an EARLIER generation already wrote a
      complete output set — contradicts "usage error, nothing written". (b) Medium: `recover.rs`'s
      `open_reader`/`classify_inputs`/`SSTableWriter::with_format` `?`-propagate a hard `Err` for a
      corrupt `CompressionInfo.db`/`Statistics.db` instead of a classified `Refusal` with a
      manifest — one of the likeliest damage modes a salvage tool meets produces NO manifest today.
      (c) Medium: `salvage_cli_tests.rs` resolves fixtures from `CQLITE_DATASETS_ROOT` alone (the
      exact #3220 defect fixed elsewhere in this same PR) — should reuse
      `resolve_root_with_corpus_fixture`'s candidate-root walk. (d) Low: the no-resync-scan guard
      doesn't report a scanned-file count (affirmative-zero doctrine). (e) Low:
      `boundaries.rs`'s BIG `key_digest` fallback is dead code today but would classify every
      partition `key-mismatch` if `key_digest` ever regained real digest semantics — drop the
      fallback. (f) Low: BTI `Rows.db` inline-key parsing is duplicated instead of returned from
      `resolve_rows_db_entry_uncounted`. (g) Low: the `UnverifiedEmptyDecode` component finding has
      no cap — O(partitions) entries possible on a wide BTI-narrow table. (h) Low:
      `discover_salvage_inputs`'s generation-parse `unwrap_or(0)` collapses unparseable ids to the
      same sort key, and `salvage_sstable` then hard-errors the WHOLE table-dir run on the first
      such generation rather than skipping it with siblings still salvaged.
      Tracked as a follow-up issue at merge time per the nit-batching doctrine; the two Mediums (a)
      and (b) are judgment calls escalated rather than silently deferred — see the session summary.

## 5. Endgame — `flow-closer`

- [ ] 5.1 Rebase; ONE full gate (`AGENT_GATE_SUMMARY_FILE` redirect); `RESULT: PASS`, tree
      integrity clean, own run-id.
- [ ] 5.2 C: `spec-auditor` vs `openspec/changes/sstable-salvage/specs/**`.
- [ ] 5.3 Roborev LAST via the sanctioned script; PASS.
- [ ] 5.4 `premerge-assert`; `HOLD:` re-read; `gh pr merge --auto --squash --delete-branch`.
- [ ] 5.5 `flow-finalize`: archive, telemetry via PR-in-worktree, close #4196, tick #4192.
