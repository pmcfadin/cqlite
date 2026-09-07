# CQLite Developer Cookbook

Command reference and usage examples moved out of `CLAUDE.md` (issue #2101) to keep the per-session
agent context lean. `CLAUDE.md` holds the rules; this file holds the recipes.

For the agent gate (`scripts/agent-gate.sh`) see `CLAUDE.md` (contract) and
`docs/development/gate-ops.md` (deep mechanics). For source layout see the
[source map](https://pmcfadin.github.io/cqlite/agents-developing/source-map/).

## roborev: scoping a review is a ROOT-checkout operation (#3229, #3234)

**To change what a roborev review covers you must edit the ROOT checkout's `.roborev.toml` and
restart the daemon.** The daemon binds the repo via `repos.root_path` and reads the root checkout's
config, so an edit inside a worktree is a **silent no-op** — and it fails in the direction that looks
like "`exclude_patterns` doesn't work" (measured on #3234: three rounds of added exclusions moved the
reported prompt size only by the size of the comment added alongside them). `roborev config get` also
answers differently depending on cwd, so it can confirm the worktree's file while the daemon uses
another. Under 1:1:1:1 every issue lives in a worktree, so this is the default situation, not an edge
case. Invocation itself stays `scripts/flow/roborev-review.sh` (see `CLAUDE.md`).

## Profiling loop

See `docs/profiling.md`.

```bash
./scripts/profile.sh baseline        # save criterion baseline
./scripts/profile.sh flame           # CPU flamegraphs (pprof, works in containers)
./scripts/profile.sh heap            # dhat heap profile vs <128MB budget
./scripts/profile.sh bench && ./scripts/profile.sh compare   # re-measure vs baseline
./scripts/profile.sh report          # ranked bottleneck report + history.jsonl ledger
```

FD/RSS resource-leak soak (long-running open/scan/drop loop): see
`docs/development/soak-resource-leak.md`.

Measuring against a **multi-GB** corpus (cold/warm scans, large-I/O work): generate it with
`test-data/scripts/gen-perf-corpus-3068.sh` (BIG/`nb`) or
`test-data/scripts/gen-perf-corpus-bti.sh` (BTI/`da`, below) and run every measurement through
`test-data/scripts/perf-run-contained.sh` — an *uncontained* cold read of an 8 GiB mmap'd `Data.db`
hard-hung a swapless host for 75 minutes with no OOM kill. See
`docs/development/perf-corpus-and-containment.md`.

## BTI (`da`) perf corpus — `gen-perf-corpus-bti.sh` (issue #3234)

Every committed `da-*-bti-*` fixture is a *correctness* fixture (largest: `test_da/wide_table`, a
28 KB `Data.db`), so BTI read-path work needs a generated corpus. Two independent reasons:

- a warm scan of the committed fixtures finishes in microseconds — ~6 orders of magnitude short of a
  ≥10 s profiling window;
- `MADV_RANDOM` is applied only at `file_size >= 8 MiB`, so below that the point-read and scan
  mappings are **the same mapping** and a read-plane A/B is structurally zero, not merely noisy.

```bash
# End-to-end pipeline validation (~2.5 min: 60 s boot + 20 s restart + ~10 s load + golden).
# Defaults the keyspace to perf_bti_smoke so it can never clobber a production corpus.
bash test-data/scripts/gen-perf-corpus-bti.sh --smoke --out /data/corpus-3234-bti

# Production corpus: ~2.0 GiB over 27 SSTables (default --rows 13200000 --chunk-rows 500000).
bash test-data/scripts/gen-perf-corpus-bti.sh --out /data/corpus-3234-bti
bash test-data/scripts/gen-perf-corpus-bti.sh --rows 33000000     # ~5 GiB

# The COMMITTABLE small Cassandra-written BTI golden (a correctness oracle, NOT a profile
# target): ~2 min, one 97.8 KB SSTable + a 188.4 KiB `sstabledump -l` golden. Does NOT touch
# the perf corpus. See "The committed small BTI golden" below.
bash test-data/scripts/gen-perf-corpus-bti.sh --small-golden --out /data/corpus-3234-small-golden

bash test-data/scripts/gen-perf-corpus-bti.sh --validate-only     # flags only; no container, no writes
bash test-data/scripts/gen-perf-corpus-bti.sh --verify-only        # re-assert an existing corpus, offline
bash test-data/scripts/gen-perf-corpus-bti.sh --help              # every flag + its env var
```

**Nothing outside `--out` is written unless you ask.** The manifest always lands at
`$OUT/manifest-bti-3234.json`; replacing the **committed**
`test-data/perf-corpus-bti-manifest.json` requires the explicit `--publish-manifest`, which is
**production-mode only**. This is fail-closed because it was a live footgun: `MANIFEST_OUT` used
to *default* to the committed path, so the advertised `--smoke` invocation overwrote a committed
provenance artifact with `perf_bti_smoke` metadata — after which the default full-corpus scan
rejects that manifest as describing another table (`bti_perf_scan` exit `8`). A `--smoke` /
`--small-golden` run naming the committed manifest is refused at `--validate-only` time.

**`--out` is canonicalized before anything is created or deleted.** The script does
`rm -rf "$OUT/cassandra-data"` as root, so a lexical `!= "/"` check is not enough: `/tmp/..` and a
symlink pointing at `/` both pass it and then resolve to `/`. `--out` is resolved first
(`realpath -m`), a canonical `/` or any system root is refused, and every destructive target is
derived from — and re-checked against — that validated canonical root.

The final line printed is the `export CQLITE_DATASETS_ROOT=<abs>` to use.

**Economics** (measured by the **production** commissioning run on a fleet worker box, 27 chunks →
1.995 GiB): **162.3 B/row on disk** at `--payload-bytes 160` with LZ4 `chunk_length_in_kb=16`, and
**~68k rows/s** end-to-end including CSV generation (13.2 M rows loaded in 194 s). Phase breakdown of
that 7.3-minute run: ~3.2 min of container boot + BTI restart + yaml verification, ~3.2 min of load,
~40 s of asserts + the one `sstabledump` golden. So ~5 GiB ≈ 33 M rows ≈ **~12 min**.
`--chunk-rows 500000` gives ~77.4 MiB per `Data.db` (measured largest 81,151,240 B), an order of
magnitude over the 8 MiB floor; the last chunk is the `--rows` remainder and is smaller (32.4 MiB
here) but still over the floor.

**`pk` is a CQL `int`, so the chunk count has a hard ceiling.** Chunk *N*'s partition keys start at
`N * PK_STRIDE`, and the largest key an `int` column can hold is 2,147,483,647. The generator's
`plan_fits_int32` refuses an over-ceiling `(chunks, chunk-rows)` plan at `--validate-only` time —
*before* any container starts — because the failure mode otherwise costs a partial multi-GB load: at
the original 1e9 stride, chunk 3 of 27 began at 3,000,000,000 and `cqlsh COPY` rejected **every** row
of it (`'i' format requires -2147483648 <= number <= 2147483647`), four minutes and three SSTables
in, while the 2-chunk `--smoke` run never reached chunk 3. The stride is now 1e6, admitting 2147
chunks.

**The two mandatory `cassandra.yaml` settings.** A stock Cassandra 5.0 node emits **`nb` (BIG)**,
because it ships `storage_compatibility_mode: CASSANDRA_4`. Both of these are required, and both
must be in place *before* the table is created (the script applies them, restarts, and then
`grep`-verifies each one):

```yaml
storage_compatibility_mode: NONE     # live in the shipped yaml (~line 2249)
sstable:                             # COMMENTED OUT in the shipped yaml (~line 1142)
  selected_format: bti
```

A miss on **either** silently produces `nb` with no error at all — which is why the yaml greps and
the emitted descriptors are hard failures, not warnings. The fail-closed asserts (all of them
re-runnable offline via `--verify-only`, and pinned by `scripts/tests/test_gen_perf_corpus_bti.sh`
with a negative control each) are: `da-*-bti-*` descriptors only and **no `nb-*`**; ≥1 `Data.db`
> 8 MiB; every `Rows.db` non-empty; each TOC lists `Partitions.db`/`Rows.db` and **not** the
BIG-only `Index.db`/`Summary.db`; rows loaded == `Statistics.db` `totalRows` == `sstabledump` rows
for each dumped generation; and the manifest writer's plan-vs-`Statistics.db` cross-check on **both**
the row count and the partition count (an unreadable `Partition Size` histogram is an error, never a
fabricated 0).

The `sed` that flips those two settings depends on the shipped file's exact comment markers and
two-space indentation, so it lives in one snippet-emitting function used by two callers: the
container path, and `--yaml-flip-check FILE`, a self-test hook that runs the **same text** against
`scripts/tests/fixtures/cassandra-5.0.2-cassandra.yaml.excerpt` (a committed verbatim excerpt of the
image's yaml). Two more hermetic hooks exist for the same reason: `--prune-dry-run`
(+ `PRUNE_KEEP=<basename>`) enumerates the multi-GB dirs a run would `rm -rf` and deletes nothing,
and `DOCKER=scripts/tests/fixtures/stub-docker-cassandra-bti.py` stands in for the container so the
whole pipeline — including both row-count cross-checks and the manifest writer's happy path — runs in
a test with no Cassandra. `--smoke` overrides only the DEFAULTS: an explicit `--rows`/`--chunk-rows`
(or `ROWS`/`CHUNK_ROWS`) survives it.

**Manifest identity** — `test-data/perf-corpus-bti-manifest.json` (committed; mirrors
`perf-corpus-3068-manifest.json`). The corpus itself is multi-GB and **not** committed
(`.gitignore`: `*.db`), so what is reproducible matters — and the two halves are different:

- **The seed reproduces the ROW SET.** The row driver (`gen-perf-corpus-bti-rows.py`) seeds chunk *N*
  with `"<seed>:<N>"`, so every value, the partition count, the rows-per-partition distribution and
  the chunk→SSTable split are a pure function of `(seed, chunk-index)` — not merely the row *count*.
  That is the deliberate divergence from the `#3068` BIG sibling, whose `cassandra-stress` profile
  cannot be reproduced from anything a manifest can record.
  **Nothing in that guarantee depends on the standard library** (roborev #3234 M2): the PRNG
  (MT19937 with CPython's `str` seeding), the weighted width draw and the bucket sampling are all
  **vendored in the driver**, because `random.choices()`/`random.sample()` are *documented
  implementation details* — `sample` even switches algorithm on a `setsize` heuristic — and `python3`
  is unpinned, so a different interpreter could have changed every partition width while the manifests
  kept advertising the old seed identity. What enforces it is `python3
  test-data/scripts/gen-perf-corpus-bti-rows.py --self-check`: 4 configurations whose CSV sha256
  digests are committed in the driver (one of them the committed small golden's exact row set, two on
  the bit-assembly edges), run as a case of the generator self-test with mutation controls proving the
  digests are live. If it ever fails, **find what changed — do not re-pin the digests.**
- **The seed does NOT reproduce the `Data.db` bytes.** Cassandra stamps a wall-clock write timestamp
  on every row, serialized as an unsigned VInt *delta* from the `Statistics.db` `min_timestamp`
  baseline (Ch.5 §"temporal deltas"), so a later run shifts some deltas across a VInt width boundary
  and even the file length changes — **measured**: two same-seed smoke runs produced 19,474,015 B and
  19,474,397 B. The per-SSTable **sha256** is therefore an *instance identity* (prove two measurements
  ran on the same bytes; catch silent corruption or an accidental replacement), **not** a regeneration
  check. A sha mismatch after regenerating is expected, not a defect.

**Timing a sustained warm scan over it** — `cqlite-core/examples/bti_perf_scan/` (committed, so the
measurement is reproducible). It drives `Database::execute_streaming` to exhaustion, not the Flight
`do_get` plane: per issue #3233 BTI is denied the Flight bypass arm, and a criterion bench would spend
minutes of warm-up + samples to report a distribution where a profile needs one sustained window.

```bash
cargo build --release -p cqlite-core --example bti_perf_scan --features cli-helpers
bash test-data/scripts/perf-run-contained.sh --mem 12G --swap 0 -- \
  ./target/release/examples/bti_perf_scan \
    --corpus /data/corpus-3234-bti-full --keyspace perf_bti --table wide_multiclustering \
    --warm-passes 1 --min-seconds 10
```

**WHICH PLANE the number describes — this is not "the BTI read path" in general.**
`execute_streaming` is a library entry point, not one fixed storage route, and the route is a function
of the corpus:

- **27 generations + a resolved schema** (the production corpus) satisfies
  `readers.len() > 1 && schema.is_some()` (`storage/sstable/mod.rs:2141-2148`, `write-support`) and
  routes into `generation_merge::stream_generations_for_read`. Its `KWayMerger` drives one sequential
  producer per generation, and each producer **re-opens its SSTable with `use_mmap = false` /
  `DiskAccessMode::Buffered`** (`storage/write_engine/merge/producer_iter.rs:364-388`), walking
  `Data.db` via `stream_all_partitions_for_compaction`. So the 125.6 s below measures the
  **compaction-style BTI `Data.db` stitch + decode plus the k-way merge, over buffered I/O** — **not**
  the mmap plane and **not** `run_scan_stream`'s BTI trie branch.
- **One generation (or an unresolved schema)** falls through to the per-reader `scan_stream`, where a
  BTI reader takes the trie branch. That is a **separate measurement** with a different memory
  profile, and it is the one to run for the mmap/trie plane.

The harness therefore **prints the route beside the number** — `generations:`, `schema_resolved:`,
`access_path:` (the per-query probe reset at `select_executor/mod.rs:525`) and a `storage_route:` line
naming the branch it took — so a throughput figure can never again be quoted without its plane.

**The WORKLOAD is pinned too, and the row-count assert cannot do it (roborev #3234 M1).** The
generation count selects the route above, so an extra `<table>-<uuid>` directory left in the
discoverable tree by `--no-prune` changes the measured plane — and it changes NOTHING the row-count
assert can see, because the retained generation holds the same rows and reconciliation yields the same
count. So `bti_perf_scan` scopes ingestion to the manifest's exact `tables[].sstable_dir` (validated
corpus-relative, right keyspace/table), refuses an ambiguous root that nothing documents (exit 3,
`OPEN_FAILED`), reports `ingest_scope:` in the result block, and names any SSTable it left outside the
scope. `gen-perf-corpus-bti.sh --verify-only` refuses the same shape and prints `corpus_dirs=`.

**Pinning the DIRECTORY is not pinning the WORKLOAD (roborev job 27 B2).** A second generation dropped
into the documented directory carrying the *same logical rows* leaves reconciliation yielding the same
count — measured on the pre-fix harness: `rows_scanned: 468` with the row-count assert green,
`generations: 2`, and `storage_route: generation_merge::…`, exit 0 — i.e. a `RESULT: PASS` attributed to
a route the manifest does not describe. So the record's `sstable_count` **and** `sstable_generations` are
now compared against the observed `*-Data.db` descriptors *before* the scan (count and exact identifier
set), and both fields are **required** of any `tables[]` record: a record that scopes a measurement
without documenting the workload inside that scope cannot gate it. A hand-written minimal manifest
simply carries no `tables` array and gets the sole-directory resolution instead.

**Two hardening rules that keep the authority and the corpus honest.** The manifest candidate list is
probed with `symlink_metadata`, and only `NotFound` falls through to the next candidate (job 27 B1):
`Path::exists()` reports a *dangling symlink* or an untraversable parent as absent, so a
present-but-unreadable `<corpus>/manifest-bti-3234.json` used to degrade silently to the committed
manifest — which describes a **different** corpus. And both scope branches now require **real directory
components beneath the canonical corpus root** (job 27 B3): the documented branch accepted
`dir.is_dir()`, which *follows* symlinks, so a correctly shaped `sstable_dir` pointed the measurement
outside the corpus entirely (measured pre-fix: `RESULT: PASS` over rows read from outside the corpus).
A consequence worth knowing when re-running an old corpus: a `manifest-bti-3234.json` written by an
earlier generator revision (partial `row_count_cross_check`, no `sstable_generations`) is **refused**,
fail-closed — pass `--manifest test-data/perf-corpus-bti-manifest.json`, whose per-SSTable `sha256`
values identify the bytes it describes.

**A failed regeneration leaves NO provenance, rather than the previous run's (roborev #3234 M2).**
`publish()` replaces the SSTable directory several steps before the manifest is written, so a failure in
between used to leave a syntactically perfect `manifest-bti-3234.json` — the harness's *first* and most
specific candidate — describing bytes that had just been deleted. The generator now vacates that
position first: the old manifest is moved aside as `manifest-bti-3234.json.superseded-<ts>` and a marker
carrying `generation_in_progress` (and no keyspace/table/row count) takes its place, so any consumer
**refuses** (`MANIFEST_UNREADABLE`, exit 8) instead of reading stale numbers; the finished manifest is
renamed over it atomically on success.

**Containment is not optional, and the streaming channel does not bound RSS on every route.** On the
multi-generation merge route the consumer drops rows as they arrive, so the window is bounded; but a
**single-generation** (or schema-less) invocation on a multi-GB BTI corpus takes the trie branch,
which **pre-materializes the whole reconciled table** before streaming (issue #1577 — the exact
condition `scan_stream_materializes` reports `true` on, `storage/sstable/mod.rs:2045-2054`). That is a
multi-GB allocation and precisely the #3068 livelock shape. Always run under
`test-data/scripts/perf-run-contained.sh`.

It is fail-closed on every way a dataset-dependent measurement can lie. The **row-count assert is ON
by default**: with no flag the harness reads the authoritative count from
`<corpus>/manifest-bti-3234.json`, else the committed
`test-data/perf-corpus-bti-manifest.json` (`rows_per_partition.rows`, recorded *observed, not
requested*, and cross-checked against `row_count_cross_check`), and an absent / unparseable /
other-table manifest exits `8` rather than degrading to "assert off". That is the guard that catches a
**silently truncated** scan: `execute_streaming` surfaces producer *errors* as a terminal `Err`, but a
producer *panic* drops its `JoinHandle` and closes the channel (the #3124 class), which the consumer
sees as a clean end-of-stream — a short row count is the only signal there is. Exit codes: `2` usage
(incl. a non-finite or non-positive `--min-seconds`, which `f64::parse` accepts), `3` corpus
missing/open failed, `4` zero rows, `5` row-count mismatch (any pass, warming ones included), `6`
window under the floor — printing the row count that *would* reach it — `7` a scan that started then
failed mid-stream, `8` no authoritative row count. Both asserts have a loud opt-**out**
(`--no-expect-rows`, `--no-min-seconds`) that stamps `*** UNGUARDED: … ***` on the `RESULT:` line, and
`--warm-passes 0` labels its output `COLD` rather than passing a cold scan off as the AC3 number.
Every one of those codes is observed firing by `scripts/tests/test_bti_perf_scan.sh` (38 hermetic
cases against the committed 10 KiB `test_da` BTI fixture — no perf corpus, seconds to run), which the
gate's `tooling-tests` component runs; its generator sibling
`scripts/tests/test_gen_perf_corpus_bti.sh` runs 118. Both declare a case-count floor, so a suite that
stops running cases cannot report success — and both report the same count on either branch of their
one conditional (`CQLITE_BTI_PERF_SCAN_BIN` reuses a prebuilt harness binary instead of building one,
and records that as its case). The generator suite additionally makes an **unrun** case impossible to
report as a passing one (roborev #3234 M1, where two `check_reject` calls sat above the helper's
definition and bash's "command not found" left `fails` at 0): every helper is defined above its first
use, an unresolved name becomes a counted failure via a sentinel file (`command_not_found_handle`'s own
`exit` cannot red the run — bash invokes it in a separate execution environment), and a static
call-before-definition audit runs as its own case *with a negative control proving it detects the
shape*. `set -euo pipefail` is deliberately NOT used: ~100 cases observe an EXPECTED non-zero exit via
`out=$(...); rc=$?`, which `-e` would abort.

**MEASURED on the 1.995 GiB / 13.2 M-row production corpus** (fleet worker box, warm page cache, one
discarded warming pass, the multi-generation merge route above): **125.6 s** wall clock, 13,200,000
rows, **105,073 rows/s** — 12.5× the ≥10 s window issue #3234 AC3 asks for, so the window survives
even a 10× read-path speed-up. Open cost is negligible (27 SSTables discovered in 0.033 s). The two
passes agreed to within 1.2 % (127.1 s vs 125.6 s), which is what confirms the measured pass was
steady-state warm rather than fault-bound. The mmap/trie plane is **not** covered by this number — it
is excluded from the route, not merely un-isolated within it (see the scope statement below).

**Re-confirmed after the harness was hardened** (same box, same corpus, row-count assert ON and read
from `/data/corpus-3234-bti-full/manifest-bti-3234.json`): **127.163 s**, 13,200,000 rows verified,
**103,804 rows/s**, warm pass 126.231 s (0.7 % apart), `generations: 27`, `schema_resolved: true`,
`access_path: fallback_full_scan (partition_key_not_fully_constrained)` — the honest CQL-level label
for an unrestricted `SELECT *` — and `storage_route: generation_merge::stream_generations_for_read`.
The access path is the *query*-level signal; `storage_route` is the plane, and both are printed on
every run.

**SCOPE OF THAT FIGURE — a stated LIMITATION, and THIS page plus the harness's own runtime output
are where it lives.** The measured
`generation_merge` route **EXCLUDES the BTI mmap/trie plane, which is therefore ENTIRELY UNMEASURED**
— not merely un-isolated. Every producer on that route **re-opens its SSTable with `use_mmap = false`
/ `DiskAccessMode::Buffered`** (`storage/write_engine/merge/producer_iter.rs:364-388`) and walks
`Data.db` sequentially (`stream_all_partitions_for_compaction`), so **no `MADV_RANDOM` mapping is
created and no `Partitions.db`/`Rows.db` trie descent happens inside the measured window** (SSTable
open — 0.033 s for 27 SSTables — is outside it). The figure is `Data.db` decode + k-way merge
throughput over buffered reads. Quoting it as a BTI index-plane baseline **would make every A/B
against it wrong by an unknown factor**; that plane needs its own measurement, on the
single-generation `scan_stream` route where a BTI reader takes the trie branch (#3029 WS3 / #3030
WS4). The concrete values:

| field | value |
|---|---|
| `access_path` | `fallback_full_scan (partition_key_not_fully_constrained)` |
| `storage_route` | `generation_merge::stream_generations_for_read` |
| `generations` | 27 |
| wall clock / rows / throughput | 127.163 s / 13,200,000 rows / 103,804 rows/s |

`bti_perf_scan` prints `access_path` and `storage_route` beside the number on **every** run, so the
scope statement travels with the measurement itself rather than being discoverable only from the issue
thread.

**The manifest deliberately records NO throughput figure**, and that is the fix for a real defect
rather than a gap (#3234 review round 10, M1). It used to carry the number as a module constant in
`read_path_measurement_scope`, with an `applies_to_this_corpus` flag computed from **rows +
generations only** — so a corpus with a different seed, payload size or width mix INHERITED an
unrelated result, and even when the flag said `false` the number was still sitting there to be quoted.
A manifest field is now **OBSERVED or ABSENT**: a harness measurement is not derivable from any byte
in the corpus, so it is not a manifest field at all (nor is the fixed `full_generation_golden` block —
the on-demand golden's real size and row count are already recorded, observed, in the per-SSTable
`sstabledump_golden` + `statistics` records). The suite enforces this: the committed production
manifest — **the very corpus this figure was measured on** — is asserted to contain no throughput
number, and the writer is asserted to hold no such constant.

Every number in the manifest is read back from the written bytes (`sstablemetadata` on
`Statistics.db`, the `CompressionInfo.db` header, each `TOC.txt`) and **nothing is inherited from a
previous manifest**. A **nonzero `sstablemetadata` exit is a hard failure before any of its output is
parsed** — the tool can print a complete-looking `totalRows:` / `Partition Size:` block and still
fail (a partial read, an OOM kill in the memory-capped container), and parsing that would publish
half-measured counts as measured. The row plan is additionally checked
**against the run's own configuration** before anything is written — chunk count, a contiguous chunk
index set, per-chunk row counts and each record's `"<seed>:<N>"` seed material — because matching
aggregate totals cannot detect a *stale* plan, and a stale plan would publish a declared seed and
generation plan that do not describe the corpus. And the corpus **SHAPE** is verified in both layers
(`assert_corpus` and the writer's `main`): the SSTable count must EQUAL the plan's chunk count, at
generations `1..CHUNKS`. That is not cosmetic — an unexpected flush split or a compaction preserves
every row and partition (so the aggregate cross-checks stay green) while changing the **generation
count, which selects the scan route and is what the AC3 figure is attributed to**. Because
`--verify-only` derives the expected chunk count from `--rows`/`--chunk-rows`, pass the same values
the corpus was generated with (the production defaults describe the production corpus).

**A manifest field is OBSERVED or ABSENT — there is no third state, and no field is inferred from a
partial match.** Four review rounds on this writer produced one defect in many costumes: a claim
asserted beyond what was checked. So the claims were **deleted**, not defended with another guard —
the fixed AC3 figure and `full_generation_golden` (above), and the
`corpus_committed`/`committed_copy`/`corpus_note` narrative, which declared "committed exact bytes"
from a **`Data.db`-only** hash comparison while counting and summing files it never read and never
consulting git. What is left is one optional field, **`data_db_sha256_also_match_at`**, whose *name is
the whole claim*: a checkout path at the same corpus-relative position where every recorded `Data.db`
sha256 was re-hashed and matched — no other component compared, git tracking not checked — and simply
**absent** when there is no such path (never `false`, which would invite reading it as "not
committed", a thing the check cannot determine). A `mode` field still marks `smoke` / `production` /
`small_golden`, and `min_data_db_floor_bytes` records the floor the run actually **enforced** (0 under
`--small-golden`) beside the fixed `read_plane_threshold_bytes`.

**A committed manifest cannot fall behind the writer** (#3234 review round 10, L4 — the committed
production artifact had drifted three contracts behind: no `sstable_generations`, no
`one_sstable_per_planned_chunk`, no `read_plane_threshold_bytes`). `test_gen_perf_corpus_bti.sh`
compares the committed manifest's key set against a manifest it has **just written with the current
writer**, and against the small golden's, in both directions — so staleness is a test failure, not a
review finding. Both manifests are regenerated **metadata-only** from the existing corpora (their row
plans and grep-verified `cassandra.yaml` lines are kept beside them under `work/`); the recorded
`Data.db` sha256s come out identical, which is the evidence that nothing but metadata moved.

**This is a parity oracle, not just a throughput fixture.** Every byte is **Cassandra-written**, so
the `sstabledump -l` JSONL goldens emitted beside the corpus can back parity work. Per issue #3042 a
CQLite-written round-trip fixture cannot: both halves make the identical framing mistake, so the
round-trip closes while real Cassandra-written data reads wrong. Goldens run ~2× the `Data.db` size,
so only a bounded subset is dumped (`--dump-generations`, default 1) and they live beside the
(gitignored) corpus — the `git add -f` convention applies to the small `test_da` correctness
goldens, never to these. **Measured**: the one golden for a 500k-row generation is 160,752,721 B
(153.3 MiB), **1.98×** its `Data.db`, with 711 partition lines and exactly 500,000 row objects —
matching that generation's `Statistics.db` `totalRows` and `partition_count` — i.e. it is **verified
correct**, and it stays **generated-on-demand**: at 153 MiB it is not committable. A *committable* BTI
golden therefore means a dedicated small table, not a slice of this corpus — which is what
`--small-golden` produces:

### The committed small BTI golden (a Cassandra-written oracle, `--small-golden`)

`test-data/datasets/sstables/test_da/wide_multiclustering_small-47f6a3008f6911f1bc0f8df8badcc262/` is
**committed** (`git add -f`, the `test_da` convention) with all 8 components + `schema.cql`: 600 rows
over 5 partitions, `PRIMARY KEY (pk, bucket, seq)`, LZ4 `chunk_length_in_kb=16`, `Data.db` 97,780 B
(sha256 `d59cd894…`), `Rows.db` 197 B (non-empty — the 400-row partition is ~74 KiB uncompressed,
~18× the image's `column_index_size` default of `4KiB`), and its `sstabledump -l` golden at
**192,935 B (188.4 KiB)**. Recorded identity:
`test-data/perf-corpus-bti-small-golden-manifest.json` (`mode: small_golden`); DDL + provenance:
`test-data/schemas/wide-multiclustering-small-bti.cql`. That manifest names this checkout path in
`data_db_sha256_also_match_at` (every recorded `Data.db` sha256 re-hashed from it and matched), and it
carries **none** of the production AC3 metadata — which no manifest does any more. It is regenerated **metadata-only** — the
committed SSTable bytes and every recorded sha256 stay unchanged, which
`scripts/tests/test_gen_perf_corpus_bti.sh` pins by re-hashing the committed `Data.db` against the
manifest.

- **It IS a correctness oracle** (issue #3042): Cassandra 5.0.2 wrote every byte, so the JSONL golden
  can back BTI row/cell decode, `Rows.db` trie descent and compound-clustering-slice parity work.
- **Its SIZE follows the repo's convention, not a row count.** The closest committed analogue is
  #3032's `test_da/multiclustering_table` — the *same* `PRIMARY KEY (pk, bucket, seq)` shape — at 468
  rows / 3 partitions with a 121,020 B golden. A golden's worth as a Cassandra-written oracle does not
  scale with row count, but its committed size does (~320 B/row), so this fixture is the first cut's
  6,000-row shape divided by exactly 10: the same width **weights**, hence the same partition-count and
  bucket-spread structure, one order of magnitude smaller (2,898,284 B → 297,374 B for the directory).
  The generator's `--small-golden` defaults (`--rows 600`, `--widths 400:20,80:30,20:50`) are pinned by
  `scripts/tests/test_gen_perf_corpus_bti.sh`, because the width mix is what fixes the committed
  fixture's size.
- **It is NOT a profile target.** At 97.8 KB its `Data.db` is far below the 8 MiB
  `MADV_RANDOM` threshold, so the point-read and scan mappings are the same mapping and a read-plane
  A/B on it is structurally zero. Read-path measurement belongs on the perf corpus above.
- Regenerating it is a **fresh short container run** and never touches the perf corpus. Verified on
  regeneration: `da` descriptor from the TOC, `Statistics.db` `totalRows` == `sstabledump` rows == 600,
  and a CQLite CLI read-back over the committed directory returning `(600 rows)`.

### The CQLite-written BTI fixture — a PERFORMANCE DRIFT CONTROL ONLY

`cqlite-core/tests/issue_3234_cqlite_written_bti_drift_control.rs` writes a small BTI (`da`) fixture
through the **production** write surface (`SSTableWriter::with_format(.., SSTableFormat::Bti)`, default
features, **uncompressed** — compressed production writes are fail-closed per #1406, partitions in
Murmur3 token order): 4 partitions / 248 rows, one of them wide enough (200 × ~2 KiB) that `Rows.db` is
non-empty. Identity: `test-data/cqlite-written-bti-drift-control-identity.json`; DDL:
`test-data/schemas/cqlite-written-bti-drift-control.cql` (asserted against the test's in-code DDL).

```bash
cargo test -p cqlite-core --test issue_3234_cqlite_written_bti_drift_control
CQLITE_RECORD_DRIFT_IDENTITY=1 cargo test -p cqlite-core --test issue_3234_cqlite_written_bti_drift_control
```

- **NEVER a correctness oracle.** CQLite-written + CQLite-read is invariant to a *uniform* framing
  error (#3042) — for BTI that is #3002, which hid behind exactly such a symmetric test. The oracle is
  always Cassandra-written bytes (`test_da/**`) or pinned Cassandra 5.0.8 source.
- **What it is for:** read-path *performance* drift. Its bytes are reproducible from a recorded seed, so
  two measurements weeks apart provably ran on identical input — which the Cassandra-written corpora
  cannot offer (wall-clock write timestamps change even a same-seed `Data.db` length). **Verified, not
  asserted:** the test writes three times and compares every component byte for byte — all 7 components
  identical across 3 runs, and stable across processes. A recorded-identity mismatch is the drift signal;
  re-record deliberately and review the diff.

## CLI

```bash
# Run CLI
cargo run --package cqlite-cli -- <command>

# One-shot query mode (Issue #223)
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```

### Output format precedence

- `--out` takes precedence over `--format` when both specified
- `--query` is an alias for `--execute` (`-e`)
- Environment variable: `CQLITE_OUT` sets default output format
- `export` shows a determinate progress bar + ETA when `--limit N` is set (the only
  authoritative total), a spinner otherwise, and emits no progress/summary when
  `--quiet` or when stdout is piped/redirected (Issue #284).

### CLI modes (Issue #242)

The CLI supports three modes with enhanced status display:

**TUI Mode** (`cqlite tui`): Full terminal UI with status bar showing:
```
Health: OK | Mem: 24.5 MB | Data: 1.2 GB | Status: Ready | Mode: EDIT
```

**REPL Mode** (`cqlite repl`): Interactive shell with status line:
```
[OK] Mem: 24.5 MB | Data: 1.2 GB
cqlite>
```

**One-shot Mode**: Direct query execution with `--execute` or `--query` flags.

Status metrics refresh every 5 seconds. Status line disabled for piped output.

## Python bindings

```bash
# Build and test
cd bindings/python && maturin develop --profile dev  # Development build (debug; overrides the release-unwind firewall pin for a fast dev loop)
cd bindings/python && maturin build --profile release-unwind  # Release wheel (panic-unwind firewall, issue #1440 — NOT --release, which is panic=abort)
```

**CI/release profile parity (issue #2653):** both `python-ci.yml` (smoke, build-only-wheels, test
jobs) and `python-release.yml` (build-wheels job) build the wheel with `--profile release-unwind`,
so CI exercises the exact panic = "unwind" firewall build that PyPI ships and a panic-strategy
regression reds a PR instead of surfacing only at release. When changing the build profile in one
workflow, change it in the other to keep the matrix in parity.

```bash
# Run Python tests - fast tests only (default, Issue #331)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -v

# Run all Python tests including slow (CLI parity, performance)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets RUN_SLOW_TESTS=1 pytest bindings/python/tests -v

# Run only slow tests (CLI parity and performance)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -m slow -v

# Exclude slow tests explicitly
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -m "not slow" -v
```

```bash
# Python usage example
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    for row in db.execute('SELECT * FROM test_basic.simple_table LIMIT 5'):
        print(row.to_dict())
"

# Python Parquet export (Epic #682)
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    rows = db.export_parquet('SELECT * FROM test_basic.simple_table', '/tmp/out.parquet',
                             row_group_size=10000, compression='snappy')
    print(f'Exported {rows} rows')
"
```

### Python bindings structure

```
bindings/python/
├── src/                    # PyO3 binding implementation
│   ├── lib.rs             # Module initialization
│   ├── database.rs        # Database class (open/close/execute)
│   ├── result.rs          # QueryResult, Row, StreamingIterator
│   ├── value.rs           # CQL to Python type conversions
│   ├── error.rs           # Exception mapping
│   ├── config.rs          # StreamingConfig, presets
│   ├── runtime.rs         # Tokio runtime management
│   ├── prepared.rs        # PreparedStatement bindings
│   └── stats.rs           # DatabaseStats bindings
├── python/cqlite/
│   ├── __init__.py        # Python package wrapper
│   └── __init__.pyi       # Type stubs for IDE support
├── tests/                 # pytest suite (counts churn — `ls tests/test_*.py`)
│   └── conftest.py        # Shared fixtures and path constants (Issue #330)
├── pyproject.toml         # Maturin build configuration
└── Cargo.toml             # Rust dependencies
```

### Python E2E test architecture (Issue #323)

Primary E2E tests (`bindings/python/tests/`):
- `test_parity.py`: Validates all 33 tables against JSONL golden files
  - `TestRowCountParity`, `TestValueParity`, `TestE2ESummary`
- `test_cli_parity.py`: Python vs CLI output equivalence

Known issues (tracked as XFail): none currently (issue #493, set element tombstones,
was the last and is closed).

## Node.js bindings

```bash
# Build and test (Issue #290, #296, #306)
cd bindings/node && npm install && npm run build  # Build native module
cd bindings/node && npm test                       # Run all tests (Jest)
cd bindings/node && npm run test:watch             # Watch mode for development
cd bindings/node && npm run test:coverage          # Run with coverage report
```

```bash
# Node.js usage example (Issue #296 - Phase 2 complete)
node -e "
const { Database } = require('@cqlite/node');
(async () => {
  const db = await Database.open('test-data/datasets/sstables', {
    schema: 'test-data/schemas/basic-types.cql'
  });
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 5');
  console.log('Rows:', result.rowCount);
  for (const row of result.rows) {
    console.log(row.name);
  }
  await db.close();
})();
"

# Node.js Parquet export (Epic #682)
node -e "
const { Database } = require('@cqlite/node');
(async () => {
  const db = await Database.open('test-data/datasets/sstables', {
    schema: 'test-data/schemas/basic-types.cql'
  });
  const rows = await db.exportParquet(
    'SELECT * FROM test_basic.simple_table', '/tmp/out.parquet',
    { rowGroupSize: 10000, compression: 'snappy' });
  console.log('Exported', rows, 'rows');
  await db.close();
})();
"
```

### Node.js bindings structure

```
bindings/node/
├── src/
│   ├── lib.rs             # napi-rs entry point, module exports
│   ├── database.rs        # Database class, QueryResult, ColumnInfo
│   ├── streaming.rs       # StreamingResult for async iteration (Issue #305)
│   ├── value.rs           # CQL to JavaScript type conversions
│   └── error.rs           # Error mapping (cqlite_core::Error → napi::Error)
├── lib/
│   ├── index.js           # Enhanced entry point with error wrapper
│   ├── index.d.ts         # Complete TypeScript definitions (Issue #312)
│   └── error-wrapper.js   # JavaScript error enhancement layer
├── __test__/              # Jest suite (counts churn — `ls __test__/*.test.js`)
├── jest.config.js         # Jest configuration
├── Cargo.toml             # napi-rs dependencies
├── package.json           # npm package config (@cqlite/node)
└── index.d.ts             # Generated TypeScript definitions
```

**Status**: Phase 3 (Streaming) complete (Issue #305). Key APIs:
- `Database.open(dataDir, options?)` — open with optional schema
- `Database.execute(query)` — **deprecated** (removed next major; emits a `DeprecationWarning`); lossy legacy JSON (blob→base64 string, timestamp→ISO string, varint/decimal→bespoke strings) and slower. Use `executeNative()`
- `Database.executeNative(query)` — native JS types (BigInt, Date, Buffer, Set, Map)
- `Database.executeStreaming(query, config?)` — async iteration for large result sets
- `Database.getStats()` / `Database.close()`

For full Node.js API reference, TypeScript definitions, error codes, and streaming
details, see `bindings/node/lib/index.d.ts` and the issue backlog (#290, #296–#314).

## Python/Node thread safety and output parity

**Python thread safety** (Issue #311, #805, #815): `Arc<Database>` + `AtomicBool`; GIL
released during async ops; concurrent queries on the same database are safe without a
warm-up. Full scans no longer share mutable file state: #815 removed the old
`SSTableReader.scan_mutex` and gave every scan its own `ScanCursor` (independent
file handle + chunk index), so N concurrent full scans run in parallel rather than
serialized.

**Python/CLI parity** (Issue #319): Python uses native types (v0.13 mapping:
`timestamp`→`datetime`, `uuid`→`UUID`, `blob`→`bytes`, `time`→`int` ns since
midnight, `duration`→`cqlite.Duration` — see the
[v0.13 Migration Guide](v0.13-migration-guide.md)); CLI uses JSON
strings. Normalization required for comparison — see
`bindings/python/tests/test_cli_parity.py`.

## Write support (CLI)

```bash
# Build with write support (Issue #392)
cargo build --package cqlite-cli --features write-support

# Write a mutation (requires --writable and --write-dir)
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":[{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}],"clustering_key":[],"operations":[{"Write":{"column":"name","value":{"Text":"Test"}}}],"timestamp_micros":1704067200000000}'

# Flush memtable to SSTable
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --flush

# Issue #1253: a single combined invocation persists durably. `--execute` DML
# now runs BEFORE the flush within the same invocation, so the inserted row
# lands in Data.db (not just the WAL):
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --execute "INSERT INTO test_basic.simple_table (id, name) VALUES (33333333-3333-3333-3333-333333333333, 'Carol')" \
  --flush

# Write subcommands
cargo run --package cqlite-cli --features write-support -- \
  maintenance --budget-ms 100 \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

cargo run --package cqlite-cli --features write-support -- \
  write-stats \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

cargo run --package cqlite-cli --features write-support -- \
  export-sstable /tmp/export --keyspace my_ks --table my_tbl \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql
```

## Delta-export (CDC Parquet, Issue #705 / Epic #696 DS9)

Requires `--features delta-export`. Schema must be a bare `CREATE TABLE` statement
(no `CREATE KEYSPACE` / `USE` preamble).

```bash
cargo build --package cqlite-cli --features delta-export

# Export one SSTable generation as a delta-envelope Parquet file
cargo run --package cqlite-cli --features delta-export -- \
  delta-export test-data/datasets/sstables/test_basic/simple_table-<uuid> \
  --schema test-data/schemas/simple_table.cql \
  --out parquet \
  -o /tmp/delta.parquet

# With custom envelope prefix (to resolve __op/__ts column collisions)
cargo run --package cqlite-cli --features delta-export -- \
  delta-export test-data/datasets/sstables/test_basic/simple_table-<uuid> \
  --schema test-data/schemas/simple_table.cql \
  --out parquet \
  -o /tmp/delta.parquet \
  --envelope-prefix _cqlite_

# Run delta-export integration tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-cli --features delta-export --test delta_export_tests
```

## Feature-flag builds

```bash
# Minimal build (pure library, no query engine)
cargo build --package cqlite-core --no-default-features --features all-compression

# Build with CLI helpers for integration testing
cargo build --package cqlite-core --features cli-helpers

# Build/test core with the embeddable Parquet writer (Epic #682)
cargo build --package cqlite-core --features parquet
cargo test --package cqlite-core --features parquet

# Flight server with the LINKED non-glibc allocator (issue #3997) — Linux only,
# NON-DEFAULT. `default = []`, so every ordinary build (and the GHCR image) links
# glibc malloc; this feature is the only way to get jemalloc, and it applies to
# cqlite-flight's BIN target only — the lib target, every binding and cqlite-core
# keep the system allocator.
cargo build --release -p cqlite-flight --features jemalloc

# Which allocator a built binary ACTUALLY installed — reported by the binary, from
# the same cfg as the installation, so it cannot disagree with what was linked:
./target/release/cqlite-flight --version | grep '^allocator: '   # -> jemalloc | system
# The startup `info` line carries the same value, rendered by tracing as a QUOTED
# string field: `allocator="jemalloc"` / `allocator="system"`. `fmt::layer()` leaves
# ANSI escapes in even when stdout is a file, so strip them before matching —
# a raw `grep 'allocator="system"'` finds NOTHING on a perfectly good binary (#3400,
# in a new place). Both surfaces derive from ONE const, so they cannot disagree, and
# `scripts/tests/test_flight_allocator_link.sh` asserts that AGREEMENT rather than
# each surface against a literal.

# The gate-enforcing guard for the above (a `tooling-tests` component). Builds BOTH
# arms and asserts they DISCRIMINATE — jemalloc symbols present + `allocator: jemalloc`
# under the feature, `0 JEMALLOC SYMBOLS RECOGNISED` + `allocator: system` without it.
# SKIPs naming the cause off-Linux or on a host missing cargo/cc/make/nm; a failed
# build is a FAIL, never a SKIP.
bash scripts/tests/test_flight_allocator_link.sh

# The structural confinement guard: exactly one non-test production `#[global_allocator]`
# in the workspace (cqlite-flight/src/main.rs, feature-gated), `tikv-jemallocator` named
# by no manifest under cqlite-core/, cqlite-cli/ or bindings/, and every cqlite-flight
# dependent linking the LIBRARY target. Needs no cargo and never SKIPs.
bash scripts/tests/test_flight_allocator_confinement.sh
```

**Do not quote a speedup for `--features jemalloc` yet.** #3551's **+29.21% rows/s** was
measured by `LD_PRELOAD`ing jemalloc into one binary, which is a different artifact from a
linked `#[global_allocator]` (initialization order, static-vs-dynamic symbol resolution, what
runs before `main`). The linked A-vs-E measurement — plus `VmHWM`/`VmRSS` against the <128 MB
target — is issue #3997's remaining work on the #3551 rig, and a pre-registered kill criterion
(`openspec/changes/flight-jemalloc/proposal.md`) decides whether `default` flips, the feature
stays opt-in, or it is removed and the null recorded. A null there is a shippable result.

## Runtime tuning knobs (env)

Parsed once per process; unset = shipped default (behavior unchanged).

| Env var | Default | Meaning |
|---------|---------|---------|
| `CQLITE_READ_PATH` | `auto` | Force the read path (`auto`/`point`/`compact`), issue #1918. |
| `CQLITE_FLIGHT_MERGE_PATH` | unset (`auto`) | Force the Flight `do_get` ROW route's arm (issue #3058): `merge` never takes the single-source fast path (the field kill switch — restores the pre-#3058 k-way merge for every request with no redeploy); `bypass` requests the fast path; unset/`auto`/anything unrecognized = automatic. `bypass` NEVER overrides a correctness precondition — a request with ≥2 post-prune sources, a non-empty `dropped_columns`, a STATIC column, an aggregation, or a reader the single-generation walk cannot serve still merges. Read ONCE per request. Which arm actually ran is observable via `cqlite_core::storage::read_path_probe` (merger-construction / reconcile-entry / cell-metadata-map counters) — that is how `cqlite-flight/tests/issue_3058_forced_path_differential.rs` proves the two arms return byte-identical rows over the same bytes at a pinned `now`. |
| `CQLITE_EGRESS_ROW_BUDGET` | `2048` | Adaptive merge egress budget (issue #2765): per-channel `sync_channel` capacity = `clamp(budget / concurrent_merges, min_cap, 256)`. Raise to allow more prefetch buffering per merge under concurrency; lower to cap aggregate memory. Missing/unparseable/zero → default. **Residual K-linear dimension**: the budget divides by merge COUNT only, not per-merge fanout K, so a single wide merge still buffers up to `4 × K × 256` entries invariant to concurrency — intended ("solo merge unchanged for any K"). The `4 ×` is the per-source in-flight multiplier the batched egress introduced in #2820 (channel-resident + consumer-held + producer-parked), so this is 4× the `K × 256` (~60MB at K=100) this table quoted before #2820, i.e. ~240MB at K=100; for fat rows the ≈4 MiB/source BYTE budget binds first. The high-K envelope is covered by the #2895 loadgen sweep. |
| `CQLITE_EGRESS_MIN_CAP` | `8` | Forward-progress floor for the above (clamped to `[1, 256]`; budget forced `≥ min_cap`). The floor engages only at very high concurrency (`budget / min_cap` ≈ 256 concurrent merges at defaults). **Inert-throttle cases** (per-channel cap constant, never shrinks with concurrency): setting this `≥ 256` (floor meets the 256 ceiling), OR a budget `< 2 × min_cap` (degenerate range, cap pinned at `min_cap`). The DEFAULTS (2048/8) do NOT disable the throttle — it engages above 8 concurrent merges. A one-time `tracing::warn!` fires on exactly these two inert cases. Fresh loadgen validation tracked in #2895. |

## Fuzzing (issue #1614)

Policy (nightly-only, out of the stable gate, workspace-excluded) is in `CLAUDE.md`. Run details:

- Five targets prove the parser never panics/hangs/OOMs on arbitrary bytes
  (returns `Ok` or `Err`): `fuzz_vint`, `fuzz_value_decode`, `fuzz_block_emit`,
  `fuzz_bti`, `fuzz_schema_parse`. They reach `cqlite-core` internals via the
  feature-gated `#[doc(hidden)] cqlite_core::fuzz_support` module (build with
  `--features fuzz`), which keeps the default public API unchanged.
- Run one target (needs `rustup toolchain install nightly` + `cargo install cargo-fuzz`):
  ```bash
  cd fuzz && cargo +nightly fuzz run fuzz_vint -- -max_total_time=45 -rss_limit_mb=2048 -timeout=25
  ```
  Or all targets: `fuzz/smoke.sh`. `fuzz_block_emit` fully exercises the
  block-emit path only when `CQLITE_DATASETS_ROOT` points at the test datasets
  (a real `test_basic/simple_table` fixture); otherwise it no-ops.
- CI: `.github/workflows/fuzz.yml` runs a bounded per-target PR smoke lane and a
  nightly long-run (both nightly + cargo-fuzz, isolated from the stable gate). A
  crash fails the job and uploads the reproducer artifact; crashes are filed as
  their own bug issues (not silently patched here).

## Publish dispatches (armed by default? NO — issue #2639)

The publishing workflows are guarded so a bare `workflow_dispatch` cannot publish
to Maven Central or mint/move a release tag from an arbitrary ref. Both guards are
enforced by `scripts/ci/validate-workflows.rb` (they fail the workflow-lint if
removed) and documented in `docs/ci/ci-tier-policy.md` (Release tier).

```bash
# Trino connector: bare dispatch is a DRY RUN (publishToMavenLocal only, no
# Central upload, no secrets) — dry_run DEFAULTS TO TRUE.
gh workflow run trino-publish.yml -f version=0.15.0

# A real Maven Central release requires dry_run=false explicitly:
gh workflow run trino-publish.yml -f version=0.15.0 -f dry_run=false

# flight-image: a manual `version` dispatch REFUSES unless refs/tags/v$version
# already resolves to the commit the run builds (github.sha). Push the release
# tag first, then dispatch with that tag's ref selected:
git push origin v0.15.0
gh workflow run flight-image.yml --ref v0.15.0 -f version=0.15.0

# For a one-off, NON-release image (no vX.Y.Z / latest tags), use image_tag:
gh workflow run flight-image.yml -f image_tag=dev-preview
```

A `v*` tag push (`git push origin v0.15.0`) publishes for real automatically on
both lanes — the guards only constrain manual dispatches.
