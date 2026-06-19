# Compaction Parity Harness Design

**Date**: 2026-06-18
**Status**: Validated (brainstorm with project owner)
**Tracks**: #842 (byte-for-byte write+compaction parity) and its sub-issues #844–#853
**Inspiration**: the Cassandra `cursor-compaction-completion` branch's
`DifferentialCompactionTester` and `differential/` scenario tree
(rustyrazorblade/cassandra)

## Problem

CQLite already has a compaction engine — k-way merge with LWW cell reconciliation,
row/cell/range tombstones, TTL handling (`cqlite-core/src/storage/write_engine/merge.rs`),
STCS selection (`merge_policy.rs`), two-pass Statistics baselines, and atomic
publication. What it lacks is **proof of correctness against Apache Cassandra**.

The goal (issue #842): prove that CQLite's write and compaction paths produce output
**identical to Apache Cassandra** for the same inputs — not just logically equivalent,
but byte-for-byte across every output component (`Data.db`, `Statistics.db`, `Index.db`,
`Summary.db`, `Filter.db`, `CompressionInfo.db`, `Digest.crc32`).

The recently-filed sub-issues #844–#853 are concrete divergences mined from the
Cassandra branch's commit history by the `compaction-parity-auditor` agent. They are
the catalog of edge cases this harness must exercise. Today there is no test that
feeds CQLite the *exact* SSTables Cassandra compacted and diffs the output.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Success bar | Two-tier: logical (blocking) + byte-for-byte (#842 north star, non-blocking until clean) | Logical parity is achievable now and catches the #844–#853 semantic bugs; byte parity is the real goal but red until the writer is perfect. Both run; only the logical tier gates PRs initially. |
| Reference driver | Java harness (in-JVM `CompactionTask`) | Needs an **explicit `gcBefore`** for deterministic purge decisions — `nodetool` in a container can't pin it. Mirrors the reference's `DifferentialCompactionTester`. |
| Harness home | Self-contained Gradle module in the cqlite repo (`compaction-parity/`), built against a pinned Cassandra 5.0.2 **source checkout** fetched in CI | Test logic lives next to the code under test; the source checkout exposes test-tree classes (`CQLTester`, `CompactionTask`, `JsonTransformer`) not in the published `cassandra-all` jar. |
| Cassandra version | `cassandra-5.0.2` source tag, pinned to the same version as the `cassandra:5.0.2` Docker image | Single source of truth (issue #669); the offline source build and the live Docker corpus stay on the same format. |
| No allowlist | Byte tier has no exception list | The reference branch's final commit was "remove the byte-comparison allowlist; nothing is allowed to diverge." Every divergence is a tracked bug. |

## Architecture

### The differential loop (per scenario)

1. **Build inputs deterministically** (JVM, via `CQLTester`): write rows with explicit
   `USING TIMESTAMP`, flushing at controlled boundaries to produce N input SSTables
   (A, B, …). No value or timestamp may come from the wall clock.
2. **Reference compaction** (Cassandra): run `CompactionTask` with `keepOriginals=true`
   and an **explicit `gcBefore`** → reference output. `keepOriginals=true` is what keeps
   the harness sound: the input bytes survive the compaction, so CQLite compacts the
   *identical* files and any divergence is attributable to CQLite alone.
3. **Candidate compaction** (CQLite): `CqliteCompactionRunner` shells out to the cqlite
   binary on the same input files with the same `gcBefore` → candidate output.
4. **Capture** both outputs as `{component files (copied), normalized sstabledump JSON,
   stats summary}`. CQLite's output JSON is produced by running Cassandra's own
   `sstabledump` over it, so the logical comparison is apples-to-apples.
5. **Assert equivalence at two tiers** (below).

### Two tiers

- **Logical (blocking gate):** the normalized `sstabledump` JSON of every output SSTable
  matches exactly, plus key `Statistics.db` fields (min/max timestamp, min/max clustering,
  tombstone/row/column counts). Reuses the existing JSONL / `sstabledump-validator`
  pipeline. Goes green achievable-now; is the gate that catches #844–#853.
- **Byte-for-byte (#842 north star, non-blocking initially):** `cmp` each output
  component. On mismatch, report per-component, per-offset diffs. Expected red until the
  writer is byte-perfect; flips to blocking once clean. No allowlist.

### Determinism rules

- **Timestamps:** every write uses a fixed `USING TIMESTAMP`; never wall-clock.
- **Purge boundary (#845):** read the actual `localDeletionTime` from the flushed input's
  stats, then run both engines twice — `gcBefore == ldt` (tombstone retained) and
  `gcBefore == ldt + 1` (purged). Exercises the exact boundary without controlling any
  clock.
- **TTL/expiring (#848):** `JsonTransformer`'s `expired` flag is wall-clock-derived, so
  normalize it out of the JSON and compare `expires_at` instead. Keep TTL expiry
  boundaries far from run time.
- **Scale mode:** for the >2 GiB-partition / millions-of-rows scenarios, stream the JSON
  dump into a SHA-256 digest (the reference's `scaleCapture`) so capture memory stays flat.
- **No silent fallback:** if a scenario isn't actually exercised (e.g. cqlite reads zero
  input rows, or no merge occurs), fail loudly rather than pass vacuously.

### What CQLite must add (a real gap)

The existing `export-sstable --compact` operates on a managed write-dir and reads the
clock — unusable for deterministic parity. We add a dedicated entry point:

```
cqlite compact <input-sstable-dir> --output <dir> \
    --schema <ks.tbl.cql> --gc-before <unix-secs> [--now-sec <unix-secs>]
```

- Reads exactly the input SSTables in `<input-sstable-dir>`.
- Threads explicit `--gc-before` / `--now-sec` through the existing merge engine
  (`merge.rs`, two-pass stats) so purge and TTL decisions are deterministic and match
  Cassandra's run.
- Writes the compacted output to `<output-dir>`.

This is the linchpin: without an explicit `gcBefore`, byte parity on any
tombstone-bearing scenario is impossible.

### Module layout

```
compaction-parity/
  build.gradle.kts                     # depends on the Cassandra checkout's built artifacts
  scripts/bootstrap-cassandra.sh       # clone apache/cassandra @ cassandra-5.0.2; `ant jar build-test`
  src/test/java/org/cqlite/parity/differential/
    DifferentialParityTester.java      # base: build inputs, run both engines, capture, compare 2 tiers
    CqliteCompactionRunner.java        # shells out to the cqlite `compact` binary, captures output
    BasicDifferentialTest.java         # scenario #1: rows + range tombstone
    ...                                # one class per scenario family (see catalog)
```

`bootstrap-cassandra.sh` clones `apache/cassandra` at the `cassandra-5.0.2` tag into a
cache dir and runs `ant jar build-test` to produce `build/classes/main`,
`build/test/classes`, and lib jars. The Gradle module puts those on the compile +
runtime classpath; scenarios `extend CQLTester`. The checkout is cached in CI keyed on
the version pin.

## Scenario catalog → issues

One scenario class per family, mirroring the reference's `differential/` tree:

| Scenario | Issue(s) |
|---|---|
| rows + range tombstone (shadow covered cells) | #846 |
| row / cell / partition / **complex** deletion merge (per cell-path) | #844, #853 |
| gc_grace purge boundary (gcBefore == ldt vs ldt+1) | #845 |
| TTL/expiring vs tombstone tie-break | #848 |
| dropped-column cell filtering | #847 |
| static-row presence read from input headers; empty-static stat over-count | #850, #851 |
| clustering reversal / null & empty component ordering | #849 |
| disabled bloom filter (`bloom_filter_fp_chance = 1.0`) | #852 |
| counters (timestamp tie-break by raw value bytes) | catalog |
| wide / pathological schema (64+ columns), large partition (>2 GiB) | catalog |
| cross-generation (recompact CQLite's own output) | catches write-side corruption that desyncs the *next* merge |
| randomized differential soak | broad coverage |

## CI shape

A new workflow:

1. Build the cqlite `compact` binary (release).
2. `bootstrap-cassandra.sh` (cached) → Cassandra 5.0.2 artifacts.
3. Run the `compaction-parity` scenario matrix.
4. On failure, surface per-scenario, per-component diffs (and the captured component
   dirs as artifacts for offline byte-level decoding).

The **logical tier blocks** the PR; the **byte tier reports** (non-blocking) until it is
clean across the matrix, then flips to blocking.

## Build order

1. **(Rust)** Add the `cqlite compact` command with explicit `--gc-before` / `--now-sec`
   and an output dir, reading exact input SSTables.
2. **(Java)** Stand up `compaction-parity/` + `bootstrap-cassandra.sh` +
   `DifferentialParityTester` + one scenario (rows + range tombstone), **logical tier
   only**, green end-to-end.
3. Add the **byte tier** (non-blocking) with per-component / per-offset diff reporting.
4. Grow the scenario matrix issue-by-issue (#844–#853); fix divergences as they surface.
5. Add cross-generation + randomized soak; **flip the byte tier to blocking** when clean.
6. Wire the CI workflow over the full matrix.

## Risks / open questions

- **Cassandra source build cost in CI:** `ant jar build-test` is slow; mitigated by
  caching the built artifacts keyed on the version pin. If too slow, fall back to a
  prebuilt artifact published once per pin.
- **`CQLTester` API stability:** tied to the 5.0.2 source tag; revisit on any version bump.
- **Output SSTable identity / multi-output compactions:** a single compaction can emit
  multiple SSTables; the harness must match outputs by token range (the reference sorts by
  first key), not by filename or instance identity.
- **CQLite multi-output behavior:** if CQLite always emits a single output where Cassandra
  may split, those scenarios need either a size threshold matched to Cassandra or to be
  scoped to single-output cases first.
```
