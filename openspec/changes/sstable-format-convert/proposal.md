# sstable-format-convert — issue #4202 (epic #4192, MOVE)

**Milestone:** unmilestoned (owner convention; board `status:ready`/`resume-dont-ask`).
**Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — a new writing verb whose
Statistics-field-preservation policy and per-field "representable in the target format" answers have
real latitude; the bytes it emits are oracle-bound to CQLite's own already-parity-proven writer and
compaction path, and BTI structural facts are oracle-bound to Cassandra-written `da` bytes (#3002).

**Depends on:** nothing (issue body: "Depends on. Nothing.") — no blocking dependency, unlike #4200.
Shares a cross-cutting concern with #4199 (`sstable-extract-split`, branch
`issue-4199-extract-split`), see "Shared write guard" below; that is a reuse-or-build-once
coordination point, not a blocker.

## Why

Epic #4192, MOVE slice: an operator with a BIG-format table who wants BTI's lazy Summary-guided
index (or vice versa — a BTI table headed for a BIG-only consumer) today has no offline path; the
only route is exporting through CQL and reloading, which loses SSTable-level fidelity (repair state,
exact Statistics) and requires a live cluster. CQLite already has both writers
(`SSTableWriter::with_format`, canonical `da` write shipped in 0.12, `nb` BIG write since M5) and a
byte-parity-proven single-input no-purge merge path (established as `salvage`'s R1 oracle, #4196) —
`convert` is the composition of "decode one generation completely" and "write it with a DIFFERENT
target format's writer" that neither `compact_sstables` (hardcoded BIG output — see design.md D1)
nor `salvage` (preserves the SOURCE format) currently offers.

## What changes

**Library (`cqlite-core`).** A `format_convert` module under `storage/write_engine/` exposing
`convert_sstable_generation(input, output_dir, schema, target_format, options) -> Result<ConvertReport>`.
For ONE input generation (never merges across generations — 1:1, matching the issue's explicit
scope), it drives the SAME single-input, no-purge reconciliation `salvage`'s R1 oracle already
established (`purge_safe(false)`, `gc_before_secs: None`, `now_secs: None` — every cell, tombstone,
static row, complex deletion and range tombstone preserved verbatim, nothing purged, nothing merged
across inputs), and writes the result through `SSTableWriter::with_format(output_dir, generation,
schema, capacity, target_format)` — the one piece neither existing writing verb exercises: a target
format that DIFFERS from the input's own family.

**CLI (`cqlite-cli`).** `cqlite convert <table-dir> --out <dir> --format big|bti [--compression
none]`, schema resolved through the same `--schema` global `query`/`salvage` use. Every generation
under `<table-dir>` is converted separately into `--out`, one output generation per input
generation, generation numbers preserved.

**Shared write guard.** `convert` writes to `--out`, an operator-named destructive path with the
identical F1/F4/F5 hazard class `salvage` closed the hard way over nine roborev rounds (#4196) —
a planned output resolved before it exists, a symlink pointing into the input, `--out` nested inside
the input tree self-poisoning a re-run. `sstable-extract-split` (#4199, branch
`issue-4199-extract-split`, itself still spec-review, not yet implemented) proposes promoting
`commands/salvage/write_guard.rs` (`WriteGuard`, `resolve_write_target`) to a shared
`cqlite-cli/src/commands/write_guard.rs`. `convert` SHALL use that shared module, not write a
fourth copy — see design.md D4 for the ordering: whichever of #4199 / #4202 implements first does
the one-time relocation (mirroring #4199's own task 0.4), the other rebases onto it.

## What this change must establish

1. **A BIG→BTI→BIG round trip is lossless at the reconciliation level**: dump-equal to the source's
   JSONL golden at every step, and the final BIG generation byte-equal to a direct no-purge
   compaction of the ORIGINAL source (not byte-equal to the original file itself — the two are not
   generally byte-identical even for a no-op, per the re-encoding finding in #4196 round 4, see
   design.md D3).
2. **BTI output passes BTI's own structural parity lanes** — the `bti-multiclustering` gate
   component, and the SAME class of assertion `issue_3002_bti_rows_root_base.rs` makes (against
   Cassandra-written `da` bytes, never a CQLite round trip — #3042).
3. **Every Statistics field is accounted for**: preserved-and-tested-equal, or named as
   not-representable in the target format with the `cassandra-5.0.8` source citation (concretely:
   `hasPartitionLevelDeletions` is `da`-only — see design.md D2).
4. **A compressed target refuses** (`Error::UnsupportedFormat`, #1406), and `--help` states the
   boundary.
5. **Wiring evidence from the binary**, both directions (BIG→BTI, BTI→BIG), never modifying input.

## Non-goals

- Pre-`na` input (version floor; `BigVersionGates`/`BtiVersionGates` already reject it).
- Compression change (#1406) — `--compression` accepts only `none`; anything else refuses.
- Merging generations — `convert` is 1:1 per generation, same posture as `salvage`.
- Repairing or recovering a damaged input (that is `salvage`/`rebuild`/`scrub`'s territory);
  `convert` assumes a healthy, verifiable input and is free to fail hard (not classify-and-skip) on
  one that is not, unlike `salvage`'s partition-level fault tolerance.
- Re-doing the general BIG/BTI write parity work `issue_908_bti_canonical_write.rs` and its siblings
  already cover — `convert` reuses those writers verbatim; it is not a second BTI-writer
  implementation.

## Doctrine impact

- **No-heuristics (#28):** the conversion is a straight decode-through-the-existing-reconciliation
  path into the existing target-format writer; no format inference, no byte-pattern guessing about
  what the target format "should" contain.
- **Uncompressed-write claim boundary (#1406):** engaged directly — `convert` is a NEW writing verb,
  so `--compression none` is the only accepted value and `--help` states the boundary explicitly,
  matching `salvage`'s and `compact`'s existing precedent.
- **Cassandra 5.0 only:** `na`/`nb` BIG ↔ `da` BTI, both directions; pre-`na` input is rejected by
  the existing version gates before `convert` ever runs.
- **Oracles:** the reconciliation-preservation claim (AC1) is checked against CQLite's own
  already-parity-proven no-purge-compaction/salvage path — a legitimate internal-consistency oracle
  for "conversion doesn't drop or alter data" — but the BTI FRAMING claim (AC2: does the output
  actually parse as a correct `da` file structurally) is checked against Cassandra-written `da`
  bytes / `cassandra-5.0.8` source, never against CQLite's own prior BTI output, per #3041/#3042 and
  the #908/#3002 lesson named explicitly in design.md D3.

## Open product decisions for the owner (do not decide here)

None block this change's shape the way #4200/purge's did — `convert` is fully scoped by the issue's
own acceptance criteria and has no dependency-driven scheduling question. One sizing/ordering note
for awareness, not a decision needed to approve this spec: if #4199 merges its write-guard promotion
before this issue implements, `convert`'s task 0 becomes "adopt the shared module" instead of
"perform the promotion" — either order works, named in tasks.md Group 0 so the implementer checks
which is true at start time rather than assuming.
