# merge-runtime-budget

## ADDED Requirements

### Requirement: The k-way merge bounds its producer-thread cost to O(M) with no per-producer multi-core runtime

The k-way merge SHALL bound the OS threads it creates for a merge over `M` input SSTables to `O(M)`.
The merge in `cqlite-core/src/storage/write_engine/merge/mod.rs` is shared by the Flight `do_get`
streaming egress and the write-engine compaction/maintenance paths. No producer SHALL construct a
multi-threaded Tokio runtime; a producer that needs an async executor to drive its sequential scan
SHALL use an executor that adds **zero** additional worker threads (a `current_thread` runtime). The
per-merge thread cost SHALL therefore be `M` producer threads plus a small constant, NOT
`M + M·num_cpus`. This bound SHALL hold identically for both the Flight read path and the
compaction/maintenance callers, with no change required at the call sites.

#### Scenario: Producer scan drives on a zero-worker executor

- **GIVEN** the merge opens a producer for one input SSTable
- **WHEN** the producer thread drives its sequential streaming compaction scan
- **THEN** it does so without constructing a multi-threaded `tokio::runtime::Runtime` (no
  per-producer pool of `num_cpus` worker threads)
- **AND** the executor it uses adds zero worker threads beyond the producer thread itself.

#### Scenario: One merge over M SSTables costs O(M) threads

- **GIVEN** a real merge over `M` present input SSTables on a host where `num_cpus >= 2`
- **WHEN** the merge runs to completion
- **THEN** the peak count of OS threads the merge adds over the pre-merge baseline is within an
  `O(M)` bound (`M + small_constant`), not `M·num_cpus`
- **AND** the merge produces byte-identical output to the pre-change code.

### Requirement: A pinned regression test asserts the thread bound and fails on the pre-change code

A pinned regression test SHALL run a **real multi-SSTable merge** (real SSTable inputs, never an
empty dataset) and observe the process's peak OS thread count directly (e.g. the Linux
`/proc/self/task` entry count — a direct observation, not a heuristic inference). It SHALL assert the
observed peak stays within the `O(M)` bound. The test SHALL be constructed so that it **FAILS against
the pre-change implementation** (where each producer builds a multi-threaded runtime) and passes after
the fix, on a host where `num_cpus >= 2`. Where the amplification is not observable (`num_cpus < 2`),
the test SHALL guard deterministically rather than flake.

#### Scenario: The regression test fails on today's code

- **GIVEN** the pre-change merge (each producer constructs `Runtime::new()`) on a `num_cpus >= 2` host
- **WHEN** the regression test observes peak OS threads across a real M-input merge
- **THEN** the observed peak exceeds the `O(M)` bound (it includes ~`M·num_cpus` runtime workers)
- **AND** the test FAILS — proving it pins the real defect, not a vacuous invariant.

#### Scenario: The regression test passes after the fix

- **GIVEN** the fixed merge (zero-worker per-producer executor) on the same host
- **WHEN** the same observation runs over the same M inputs
- **THEN** the observed peak is within the `O(M)` bound and the test PASSES.

#### Scenario: The test never flakes on a single-core host

- **GIVEN** a host where `num_cpus < 2` (the amplification term collapses)
- **WHEN** the regression test runs
- **THEN** it guards on the core count deterministically (skips or trivially holds) rather than
  asserting a bound it cannot distinguish.

### Requirement: Byte-parity and cancellation discipline are preserved

The change SHALL NOT alter merge output or reconciliation semantics. Merged SSTable output SHALL be
byte-identical to the pre-change code across the compaction-byte-parity harness and the sstabledump
JSONL goldens. The cooperative cancellation discipline (issue #2264) SHALL be preserved: the
`ScanCancel` token wired onto every per-run reader, the distinct `Cancelled` channel signal, and the
Flight `do_get` abort-on-drop path SHALL behave identically to the pre-change code.

#### Scenario: Compaction byte-parity holds after the change

- **WHEN** the compaction-byte-parity harness and the sstabledump JSONL golden comparison run over
  the present real SSTable corpus after the change
- **THEN** every merged output byte matches the Apache Cassandra / sstabledump golden, unchanged from
  before.

#### Scenario: A cancelled Flight do_get abandons the merge promptly

- **GIVEN** a Flight `do_get` merge over an index-less (Summary.db absent) input driving a
  fully-materialising scan
- **WHEN** the `do_get` stream is dropped (client disconnect) mid-merge
- **THEN** the producer's scan observes the `ScanCancel` token at its next cooperative poll point and
  abandons the walk
- **AND** the receiving end distinguishes the `Cancelled` signal from a genuine I/O/corruption error,
  exactly as before the change.

### Requirement: A merge producer-thread gauge lands in the observability surface

A gauge SHALL be added to the observability catalog (`cqlite-core/src/observability/catalog.rs`)
reflecting the count of live merge producer threads, so the previously-invisible thread cost of a
merge is observable in production. The metric name SHALL be coordinated with epic #2313 WS2 to avoid a
naming collision. The gauge SHALL rise as producers are spawned for a merge and return to its
baseline once the merge's producers are joined/dropped.

#### Scenario: The gauge reflects live producer threads during a merge

- **GIVEN** the observability surface is active
- **WHEN** a merge over `M` inputs spawns its producers and then completes
- **THEN** the gauge rises to reflect the live producer threads during the merge (bounded by `O(M)`)
- **AND** returns to its pre-merge baseline after the merge's producers are joined/dropped.

#### Scenario: The gauge is documented in the observability catalog

- **WHEN** the observability catalog is inspected after the change
- **THEN** the new merge producer-thread gauge is present with a name agreed with epic #2313 WS2, a
  unit, and a description, alongside the existing catalog gauges.
