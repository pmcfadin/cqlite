# Resume audit for #4114, taken during the lane-4104 throttle hold (CPU-free reading only)
Audited HEAD = 02fa61154. NOTHING here was verified by running anything — no cargo, no gate,
per the lead's throttle. Every "looks correct" below is a READ, not a measurement.

## What is genuinely done (read and spot-checked)

AC1  DONE + reported in thread (comments 5563411669, 5563479217). Silent mis-decode confirmed,
     exit 0 with a wrong value on vector_exact. Verified by the lead directly.
AC2  DONE. Cassandra-5.0.8-written fixture, 4 tables, sstabledump JSONL goldens, committed.
AC5  DONE and it looks RIGHT. repair_clustering.rs now recurses on the ELEMENT's own layout,
     cites VectorType.java:94-96 + AbstractType.java:62,490-493, removes "VectorType" from the
     variable list with an inline note, and returns None (honest UNKNOWN -> caller reports
     `Unparsed`) for a malformed vector rather than guessing a width. Correct by construction:
     Fixed(4n) for a fixed element, Variable for vector<text,n>.
AC4  IMPLEMENTED. typed_value.rs:570 calls require_float_element(element, dimension)? BEFORE
     decode_float_vector_exact, so a non-float element is refused BY NAME rather than falling
     back. marshal_type.rs's new vector arm also refuses malformed types by emitting a spelling
     no decoder claims, instead of the old to_lowercase() degradation. Reasoning is documented
     in-comment. NOT yet proven by a test run.

## THE BLOCKING GAP — AC3 and AC6 are NOT satisfied

**There is NO integration test. `git diff --name-only 786166cd8..HEAD -- cqlite-core/tests/` is
EMPTY, and nothing anywhere opens the committed fixture.**

The decoder's only tests are INLINE UNIT TESTS in schema/vector_type/vector_value.rs asserting
against HAND-WRITTEN byte literals (e.g. `[0x3f, 0xc0, 0x00, 0x00]`). Those bytes were typed by
the implementer, so the test proves the decoder agrees with THE AUTHOR'S UNDERSTANDING of the
format — not with Cassandra. That is #3042's round-trip-invariance trap wearing a different hat:
if the author's mental model of the framing were wrong, the literal and the decoder would be
wrong together and the test would still pass. The committed Cassandra-written bytes exist
precisely so that cannot happen, and they are currently unused by any test.

Consequently unmet:
  AC3 — needs n=1, n=384, a NULL vector, and a vector column in BOTH a pk-only and a clustered
        table, read from the fixture.
  AC6 — needs PER-CASE `must_run` assertions (committed fixtures fail closed unconditionally),
        never a suite-wide assert!(ran > 0).
  The vector_exact regression — the whole point — is not pinned by any test that reads it.

### Fixture-location question, still OPEN and it is a real trap
The fixture is at test-data/fixtures/issue_4114/, which may NOT be a candidate root that
cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table walks. A test that silently
SKIPS because the root did not resolve IS the #3220 defect and is worse than no test.
READ THIS PRECEDENT FIRST: cqlite-core/tests/issue_3790_collection_order_cassandra_golden.rs:189
deliberately does NOT use sstables_root_for_table and explains why at :189-230 (SHADOWING —
$CQLITE_DATASETS_ROOT can mask a committed fixture). That comment is directly on point and may
be the sanctioned pattern for a committed-fixture-only table. Decide deliberately and record why.

## Other residuals
- 02fa61154 is a WIP commit: an in-flight edit to the #3631 pinned test
  (regression_3631_marshal_field_types_tests.rs, +87 lines). That flip STILL NEEDS its
  source-cited justification, or a revert. Do not leave it as-is.
- commitlog/schema.rs:234 appears UNTOUCHED. That is probably CORRECT — a vector genuinely is
  not a "simple scalar", so the assertion stays true. Confirm rather than assume.
- NO `--lite` PASS exists yet. Nothing is reviewed (no rust-reviewer, no roborev).
- WIRING EVIDENCE unverified: `read-sstable` on vector_exact must now return the real vector
  instead of 0x0000003f80000040000000. Re-run once CPU is free; it is the end-to-end proof.
- The diff touches 25 files including export/arrow_*, types/comparator.rs and
  storage/serialization/types.rs — wider than the decoder itself. Have the reviewer check those
  are necessary consequences of the new CqlType variant and not scope creep.
- Two new source files (vector_type.rs 337 lines, vector_type/vector_value.rs 348 lines) are
  comfortably under the ~800 file-size target. Good.

## Resume order
1. Confirm lane-4104's gate is done: pgrep -af agent-gate, check /proc/<pid>/cwd for lane-4104.
2. CronDelete the drive-issue-4114 job.
3. Dispatch an implementer for the AC3/AC6 integration test + resolve the fixture-root question.
4. Settle the 02fa61154 WIP flip (justify or revert).
5. `--lite` to PASS, then review-first: rust-reviewer + roborev on the lite-green diff.
6. PR, then flow-closer for the ONE gate of record.
