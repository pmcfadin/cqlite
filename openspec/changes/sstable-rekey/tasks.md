# Tasks — sstable-rekey (issue #4203)

Ordered. Group 0 is premises, 1 confirms/promotes the shared write guard, 2 the library, 3 the CLI,
4 the Cassandra-side readback extension, 5 the endgame. Commit after every group (#3042). `--lite`
after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 Re-verify against `cassandra-5.0.8` source (never CQLite's own code) that the table id
      appears ONLY in `Descriptor.java`'s directory-naming pattern and nowhere in
      `StatsMetadata`'s (de)serializer, for the CURRENT checkout state of both files (proposal.md
      cites the exact classes/methods; confirm they have not changed shape since this spec was
      drafted).
- [ ] 0.2 Confirm no OTHER SSTable component CQLite reads/writes (Summary.db, Filter.db,
      CompressionInfo.db, Digest.crc32, CRC.db, TOC.txt) carries a table/cf id field either — the
      proposal's citations cover Descriptor.java (path) and StatsMetadata (the one component with
      per-table descriptive fields); do a citation pass over the others' serializers to close the
      gap explicitly rather than by omission.
- [ ] 0.3 Confirm `commands/write_guard.rs` (the module #4199/`sstable-extract-split` promotes out
      of `commands/salvage/write_guard.rs`) exists at that crate-visible path on `origin/main`. If
      #4199 has not yet merged, either wait for it or perform the identical relocation here as a
      first commit (design.md D8) — do not hand-roll a second guard.
- [ ] 0.4 Confirm `query/parser.rs::parse_uuid_literal`/`is_uuid_literal` visibility; bump to
      `pub(crate)` if rekey's `table_id.rs` needs to call them directly rather than duplicate the
      hex/dash logic.
- [ ] 0.5 Confirm `BigVersionGates::from_version`/`BtiVersionGates::from_version`'s exact
      `Error::UnsupportedVersion` shape (fields, floor value) so rekey's version-floor test asserts
      the SAME shape, not a paraphrase.

## 1. Confirm/promote the shared write guard (design.md D8)

- [ ] 1.1 If task 0.3 found the guard already promoted: import it, done. If not: perform #4199's
      documented relocation (`commands/salvage/write_guard.rs` → `commands/write_guard.rs`,
      `pub(super)` → `pub(crate)`) as this change's own first commit, coordinating with whichever of
      #4199/#4203 lands second to avoid a duplicate relocation (flag to the lead if both are
      in-flight simultaneously).

## 2. Library — `rekey_table` (design.md D1–D4)

- [ ] 2.1 New module `cqlite-core/src/storage/write_engine/rekey/mod.rs` + `table_id.rs` (D2:
      dashed/bare-hex parsing and normalization, target-directory derivation, same-id refusal with
      the `--allow-same-id` escape hatch — confirm with the lead whether to keep that flag or always
      refuse, per proposal.md's open question).
- [ ] 2.2 `copy.rs` (D1): per-generation, per-component streaming byte copy into a temp directory
      under `--out`.
- [ ] 2.3 Version-floor and `--version` delegation logic (D4): read each generation's version marker
      from its component filenames; refuse on a real change (naming #4202) or on mixed source
      versions when `--version` is given; inherit `BigVersionGates`/`BtiVersionGates` unchanged for
      the pre-`na` floor.
- [ ] 2.4 Self-audit + atomic publish (D3): `verify --mode full` the temp directory; rename to the
      final name on PASS; remove and refuse on FAIL.
- [ ] 2.5 Unit/integration tests for R1–R4 per `specs/rekey-core/spec.md`; name every new target in
      the gate's `core-tests`/`write-tests` component list (#3522).

## 3. CLI (design.md D5–D8; specs/cli-rekey/spec.md)

- [ ] 3.1 `cli_types.rs`: `Commands::Rekey(RekeyArgs)`, `RekeyOutFormatArg` (or reuse an existing
      shared out-format enum if one exists after #4199 — check before duplicating), `long_about`
      text per R-CLI-2.2.
- [ ] 3.2 `commands/rekey.rs`: arg validation, `--table-id` normalization (task 2.1's helper),
      `WriteGuard` construction from the (confirmed-promoted) shared module, dispatch to
      `rekey_table`, manifest render (JSON D5 shape + text rendering derived from it, R-CLI-2.1),
      exit-code mapping (D6 / R-CLI-1).
- [ ] 3.3 `commands/mod.rs` dispatch wiring, following `dispatch_salvage`'s pattern (owns its whole
      exit-code space; an informative message on a missing feature gate rather than a raw panic).
- [ ] 3.4 CLI tests per `specs/cli-rekey/spec.md` R-CLI-1 through R-CLI-3, named in the gate's
      `cli-tests` component list (#3522): `rekey_cli_tests.rs`, `rekey_write_guard_tests.rs`.

## 4. Cassandra-side readback extension (AC2)

- [ ] 4.1 Extend `test-data/scripts/e2e-cassandra-readback.sh` with a rekeyed-import case: rekey a
      committed fixture to a fresh id, `DROP`+`CREATE TABLE` (or otherwise pin) the target id in a
      live Cassandra 5.0 container, place the rekeyed directory, confirm Cassandra loads it and
      `SELECT *` matches the pre-rekey golden.
- [ ] 4.2 Wire into the `ci:bindings-full`-style optional tier; record as CI-optional evidence in
      the parity manifest (`test-data/cassandra-parity-manifest.yml`), matching how other
      CI-optional Cassandra-readback evidence is already declared there.

## 5. Endgame

- [ ] 5.1 `openspec validate sstable-rekey --strict` clean; commit.
- [ ] 5.2 `--lite` after every fix round (summary-file redirect); review-first: `rust-reviewer` +
      `roborev-review.sh --agent claude-code --model claude-opus-5` on the lite-green diff BEFORE
      any full gate.
- [ ] 5.3 Open PR, noting the #4199 write-guard dependency explicitly in the PR description.
- [ ] 5.4 Hand to `flow-closer`: rebase → ONE full gate (`AGENT_GATE_SUMMARY_FILE=... bash
      scripts/agent-gate.sh`, Linux box) → `spec-auditor` (C) intent audit against
      `specs/rekey-core/spec.md` + `specs/cli-rekey/spec.md` → roborev LAST → `premerge-assert` →
      arm `gh pr merge --auto --squash --delete-branch` → finalize.
