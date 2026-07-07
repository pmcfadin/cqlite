# Design — demote-write-spans

## Approach

Two mechanical edits + one bench addition:

1. **Spans → DEBUG.** Add `level = "debug"` to the write-side/compaction
   `#[tracing::instrument(...)]` attributes. There are **22** instrument sites in
   `cqlite-core/src/storage`, **none** currently leveled (all default INFO). This
   is an attribute edit only — names/keys/skips untouched.
2. **SELECT `info!` → `debug!`.** ~8 `info!` sites in `cqlite-core/src/query`
   demoted to `debug!`, leaving ≤1 info line per SELECT.
3. **Subscriber-on bench variant** added to the existing overhead gate, advisory-first.

## The one judgment call for the owner: uniform vs selective demotion

The 22 spans fall into two classes:
- **Hot / high-frequency** (the audit's real target): `write.mutation`,
  `memtable.insert`, `wal.append`, `wal.sync`, `merger.step`,
  `writer.write_partition`, `compression.write_chunk`. These fire per-mutation /
  per-partition / per-chunk and MUST leave INFO.
- **Coarse / once-per-user-call**: `flush.public`, `compaction.start_merge`,
  `compaction.finalize`, `write.cql_execute`. These fire once per explicit user
  operation and are arguably reasonable INFO "operation markers."

**Recommendation: demote ALL 22 to DEBUG** (uniform), matching the read side's
blanket `debug_span!` discipline — one simple doctrine ("core spans are DEBUG;
the CLI opts into INFO only for its own top-level lines"), nothing per-call at
INFO to reason about. The acceptance test only requires the hot spans leave INFO,
so uniform demotion satisfies it strictly.
**Alternative:** keep the ~4 coarse once-per-call spans at INFO as operation
markers. Slightly more useful default output, but a fuzzier rule. *(Owner may
pick at approval; default is uniform.)*

## Sequencing vs #1706 (hard)

- The **span** demotion (22 instrument attrs) is independent of #1706 — no shared
  lines — and could land anytime.
- The **`info!` demotion** touches the exact `cqlite-core/src/query` lines #1706
  is rewriting `log::info!` → `tracing::info!`. To avoid a guaranteed conflict,
  **this change's implementation is held until #1706 merges**, then rebased so the
  demotion applies to the already-migrated `tracing::info!` lines.

## Verification (TDD, must fail on `main`)

1. **Counting-subscriber span test**: subscriber at INFO, write N mutations,
   assert INFO-level span count is O(1) not ≥3N. Red on `main`.
2. **SELECT info-count test**: subscriber at INFO, one SELECT, assert ≤1 info
   line. Red on `main`.
3. **Subscriber-on bench**: exists and records a number (advisory).
4. Names/keys preserved — asserted by the span tests reading `name`/fields.

## Wiring evidence

The tests consume events/spans through a real `tracing` subscriber at INFO — the
exact posture a CLI user / embedder experiences — proving the *default output* is
quiet, not merely that a helper is configured.
