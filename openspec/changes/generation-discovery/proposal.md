# generation-discovery — issue #4626 (architecture-review candidate 1; closes #4622 and #4623 via its PRs)

**Milestone:** 0.20. This merges after the v0.19.0 tag, as the first 0.20 item (owner Q32).
**Priority:** P1. **Routing:** design-driven (OpenSpec + Seam 1). The owner settled the design in
grilling Q32 to Q42 on 2026-11-24/25. This change records those decisions as specs; it does not
reopen them.

## Why

Two defects with one cause.

- **12 copies of discovery.** Every verb that needs "this table's generations" lists the directory
  itself. Each copy has its own idea of which files count. The copies disagree about `oa`/`da`
  versions, UUID ids, `snapshots/` and `backups/`, and Cassandra's transaction logs. Examples:
  `compact` recurses into `snapshots/` and `backups/` and reads only `nb-*-big`
  (`cqlite-cli/src/commands/sstable_inputs.rs`); `WriteEngine`'s `scan_generations` recurses the same
  way and misses `oa`, `da` and UUID ids (`write_engine/mod.rs:1548`); `parse_sstable_filename`
  silently drops UUID ids (`sstable/directory/scan.rs:128-155`). `explain`, `diff`, `find`,
  `diagnose`, `rebuild`, `salvage`, `verify` and `sweep` each carry their own.
- **16+ Statistics-trust sites.** After #4612 the strict integrity check exists
  (`statistics_reader/integrity.rs`), but each site still decides for itself whether to call it, whether
  to check the version floor first, and what to do with a failure. The result is that the same damaged
  `Statistics.db` is a refusal in one verb and a degraded read in another.

Issue #4622 (compact reads `snapshots/` and `backups/`) and #4623 (write engine id allocation misses
formats and UUID ids, and compact can overwrite `nb-1-big`) are the visible symptoms. Fixing each in
place would add a 13th and 14th copy. This change adds one module that owns the question and deletes
the copies, one migration PR at a time.

Cassandra answers the question in one place: `Directories.SSTableLister` over
`LogAwareFileLister`. The module follows it at the pinned `cassandra-5.0.8` tag.

## What changes

New capability `generation-discovery` (module `cqlite-core/src/storage/sstable/generation/`):
Generation, Generation id, Live set, Scope, Condition and Trusted metadata, defined in `design.md` and
the repo-root `GLOSSARY.md`.

User-visible behaviour changes, each landing in the PR named in `tasks.md`:

1. **`compact` no longer reads `snapshots/` or `backups/`** (closes #4622). Its input is the Live set of
   the directory it is given, one directory, no recursion.
2. **`compact` refuses a format or id form it cannot handle** with a named error, instead of silently
   skipping the file as the `nb-*-big` filter does today.
3. **`rebuild` reports a below-floor generation as `UnsupportedVersion`**, a refusal, not as a
   generation whose Statistics fields are `lost`.
4. **UUID-form generation ids are accepted and ordered** (`3gb2_0rbd_2lhua2pw7a2hlyvbsp`), in every verb
   that lists generations, with sequence ids ordered before UUID ids.
5. **TocMissing generations become visible** with the condition `TocMissing` (a `Data.db` present, its
   `TOC.txt` absent). Today most verbs drop them silently and `compact` skips them on purpose.
6. **A TxnInconsistent directory is refused.** A directory whose transaction log is neither complete nor
   consistent with the files on disk fails the whole listing with a named error. Today nothing reads
   transaction logs, so files a committed compaction already obsoleted, or an aborted one left
   behind, count as live.
7. **Writers refuse sequence ids outside the Java `int` range.** The write engine allocates the next
   id as the maximum over the Live set and `backups/`, plus one, across BIG and BTI and every version
   letter, and refuses rather than wrap or emit an id above 2147483647 (closes #4623).

**Behaviour change, no existing spec.** `compact` has no capability spec today (no `compact` or
`write-engine` entry under `openspec/specs/`). Its change (items 1, 2 and 7) is specified only in the
new `generation-discovery` spec. `sweep` and `info` also carry a discovery copy and have no spec
beyond `cli-sweep`; they migrate under the same module and are listed in `tasks.md`.

**Delta specs** (all ADDED requirements, in `specs/`): `rebuild-components`, `cli-rebuild`,
`cli-explain`, `find-key-probe`, `cli-diagnose`, `core-diagnose`, `cli-diff`, `cli-salvage`,
`verify-audit`. `salvage-scan` and `sstable-version-floor` are unchanged.

## Non-goals

- **CQLite writers emitting transaction logs.** The module reads them. Writing them is deferred to
  the 0.20 write verbs (epic #4192), where a verb that obsoletes input generations needs it.
- **Per-verb output vocabularies.** Each verb keeps its own words and JSON shapes for a condition
  (owner Q38). `diagnose` keeps `unreadable_generations`; `explain` keeps exit `2`; `verify` keeps its
  verdict rows.
- **Schema refusal.** Deciding whether a schema fits an SSTable is not this module's job.
- **Candidate 2, the compaction-equivalent merge.** A separate change.
- **A new on-disk format, a new CLI flag, or a new public Rust API beyond the module's own.**

## Doctrine impact

- **No heuristics (#28).** Transaction logs and filename descriptors are authoritative metadata
  (Cassandra's own, `LogAwareFileLister` and `Descriptor`). A file whose descriptor does not parse is
  ignored exactly as Cassandra's `tryComponentFromFilename` ignores it (`Directories.java:1128-1130`);
  it is never guessed at from its bytes. No condition is inferred from content patterns.
- **Version floor.** The floor (`na`/`nb`, `oa` BIG; `da` BTI) is checked first, from the descriptor
  alone, in every verb. Pre-`na` stays out of scope and is not extended.
- **Format authority (#3041).** Every rule cites the pinned `cassandra-5.0.8` file and line.
- **Public surface.** The module is `pub` in `cqlite-core`, so the `pub-surface` ratchet applies, and
  the verbs' behaviour changes above are why this is an OpenSpec change (Specs only for public
  surfaces). Each migration PR deletes the copy it replaces; the final task greps to prove none
  survives.
- **Crate root tells the truth (#1712).** The module is declared with its cfg on the `mod`
  declaration, never an inner `#![cfg]`.
