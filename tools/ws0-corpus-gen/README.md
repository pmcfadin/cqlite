# `ws0-corpus-gen` — the committed `ws0.events` performance fixture (issue #3096)

Two binaries that make the issue #3096 Arrow-encode measurement reproducible from
a clean checkout:

| binary | what it is |
|---|---|
| `ws0-corpus-gen` | deterministic corpus generator, driving the **production** `cqlite_core::storage::sstable::writer::SSTableWriter` |
| `ws0-scan-bench` | **arm A** of the measurement: the bare scan (`cqlite_core::Database::execute_streaming`) |

Arm B (Flight `do_get`) is the existing `cqlite-flight` server plus
`tools/flight-loadgen`. Both arms are driven together, in one session, on one
verified physical-core sibling pair, by `scripts/perf/ws0-baseline.sh`.

---

## THIS CORPUS IS A PERFORMANCE FIXTURE ONLY — NEVER A CORRECTNESS ORACLE

The corpus is **CQLite-written and CQLite-read**. Per issue **#3042**, that round
trip is **invariant to a uniform framing/serialization error**: both sides make
the identical mistake, the round trip closes, and every test over it stays green
while real Cassandra-written data would read wrong — and, symmetrically, while
CQLite-written data would be unreadable by Cassandra. Two defects that cancel are
undetectable by a symmetric fixture **by construction**. That is not a gap to be
fixed here; it is a property of the construction, and the reason this README says
it three times.

Consequences, which are binding:

* **No on-disk framing or encoding correctness claim may rest on this corpus** —
  not row/cell framing, not VInt encoding, not the index/summary/statistics
  layout, not compression, not the `nb` BIG format at all.
* Correctness stays anchored to the **Cassandra-written** fixtures
  (`test-data/datasets/`, the `nb`/`da` goldens, the `sstabledump` JSONL
  references) and to the oracles in
  `openspec/changes/arrow-encode-doget/design.md` §"Correctness pinning stack".
* The one thing this corpus IS good for: holding the **bytes constant across two
  measurement arms in one session**, at a size that makes a per-row cycle cost
  measurable.

The caveat also travels *inside* the generated `corpus-identity.json`, so a
reader of that artifact alone cannot miss it.

---

## Quick start

```bash
# 1. Generate (writes ~2.8 GB; corpus binaries are NEVER committed)
cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- \
  --out /data/ws0-3096 \
  --identity-out docs/reports/ws0-3096-artifacts/corpus-identity.json

# 2. Prove determinism: regenerate elsewhere and compare against the recorded identity
cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- \
  --out /data/ws0-3096-b --progress-every 0 \
  --verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json

# 3. Measure both arms in one session
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096
```

A smoke run is cheap — `--rows 200000` builds in a couple of seconds.

## What is generated

Pinned schema (`src/schema.rs`, asserted byte-identical to
`docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql` by a unit test):

```sql
CREATE TABLE ws0.events (
  part_id text, seq int, event_time timestamp,
  blob_a blob, blob_b blob, device_id uuid,
  metric_a int, metric_b bigint, metric_c double,
  payload text, region text, status text,
  PRIMARY KEY (part_id, seq, event_time)
) WITH CLUSTERING ORDER BY (seq ASC, event_time ASC);
```

The two committed schema artifacts were reconciled: the `DESCRIBE` form
(`docs/reports/ws0-3026-artifacts/ws0-corpus/schema-as-created.cql`) and the bare
DDL agree on all twelve columns, their types and the primary key. They differ
only in table OPTIONS — most materially the `DESCRIBE` form's
`compression = {LZ4Compressor, chunk_length_in_kb: 16}`. This generator follows
the bare-DDL form, which is also the only form CQLite can honor: the production
write surface emits **uncompressed** SSTables and never a `CompressionInfo.db`
(issue #1406), and the generator asserts that component's absence rather than
assuming it.

Shape: **4,000,000 rows = 40,000 partitions x 100 rows**, partitions written in
**Murmur3 token order** (a hard `SSTableWriter` precondition, verified before the
first write and re-validated by the writer itself), ~692 B/row.

## Determinism

Every field of every row is a pure function of `(seed, partition_index,
row_index)` through a hand-rolled SplitMix64 — deliberately not the `rand` crate,
so a dependency bump cannot silently change the corpus. All timestamps are fixed
constants, never wall-clock. Two runs from the same seed produce a byte-identical
`Data.db` (and, measured, byte-identical Index/Summary/Filter/Statistics/CRC/
Digest/TOC as well).

`--verify-against` compares the regenerated identity field by field and exits
non-zero on any divergence, naming the component that moved.

### Three verdicts, not two (issue #3272 F1)

A recorded identity may PREDATE a field the current generator emits — the committed
`docs/reports/ws0-3096-artifacts/corpus-identity.json` was recorded 2026-08-03 and
carries no `schema_sha256`, which #3272 R2 added afterwards. Such an identity is
READ (a required field would make the determinism check permanently unrunnable
against the only artifact it is ever pointed at), but its absent field is **never**
folded into "matched":

| verdict | meaning | exit |
|---|---|---|
| `PASS` | every recorded field was compared, and every one agreed | 0 |
| `PARTIAL` | everything comparable agreed, but ≥1 field is **UNVERIFIED** because the recorded identity does not carry it — each one named | **non-zero** |
| `FAIL` | ≥1 field disagreed (unverified fields are listed too) | non-zero |

`PARTIAL` exits non-zero on purpose: a scripted caller reads the exit code, and a
zero exit *is* a pass claim however the text is worded. A check that did not run
must not print like one that passed.

Generation always records every field, so a `None` in an identity can only mean
"recorded before that field existed", never "this run declined to look".

## The digest this corpus is NOT

Issue #3096 quotes
`0185909de6da0de839e75defe8b7113f502001017db3b5312d7ed6fd3312f0b1`. That is the
**#3058/#3100** corpus: Cassandra-written and LZ4-compressed. CQLite's
uncompressed-only write surface cannot reproduce those bytes, so this generator
asserts **nothing** against that value. The corpus is pinned by its own recorded
`sha256`, row count, partition count and byte shape.

For the same reason the WS0 **absolutes** (240,100 / 312,155 rows/s) are not
reproducible here and must never be restated as reproduced — issue #3096's
acceptance is a same-session RATIO, not those numbers.

## Anti-vacuity

The generator exits non-zero on: a zero or non-divisible row count; a written row
or partition count that does not match the plan; an empty `Data.db`; a
`CompressionInfo.db`; a Murmur3 token collision. `ws0-scan-bench` exits non-zero
if any pass observes zero rows.
