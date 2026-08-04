# The Arrow-buffer digest oracle — re-pin record and correction note (issue #3096)

This file is the out-of-code half of the digest oracle's pin record. The same
re-pin table is mirrored in the oracle itself,
`cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs` (the `THE PINNED
DIGESTS` block). A pinned oracle whose value changes without a written reason is
indistinguishable from one that was quietly adjusted to pass, so both copies
carry old value, new value, reason, and commit.

Oracle: `cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs`
Fixture: `cqlite-flight/tests/support/ws0_fixture.rs` (written in-test to a
`tempfile::tempdir()`; 500 rows = 5 partitions x 100 rows, `BATCH_SIZE` = 128,
4 batches of 128/128/128/116, 12 columns)

## 1. Re-pin table

| Digest | Old | New | Reason | Commit |
|---|---|---|---|---|
| producer | (did not exist) | `0xd0014e42e893f87f` | **New tap** (roborev finding 1). The pre-existing digest hashed batches only AFTER Flight IPC serialisation and client-side decoding, so the round trip could normalise a buffer representation and the fold might not observe what the server's Arrow builders produced. The new tap folds the producer's `RecordBatch`es BEFORE `streaming.rs::encode_do_get`. | `fcd96ca` |
| wire | `0xd0014e42e893f87f` | `0xe6eccf8a9ffbca11` | **The fixture gained deterministic nulls** (roborev finding 2). All twelve cells of every row were non-null, so no validity bitmap ever had content. The fixture now carries `NullPlan::Pinned` — 150 absent cells over 500 rows — so the hashed data genuinely differs. | `3173e9c` |
| producer | `0xd0014e42e893f87f` | `0xe6eccf8a9ffbca11` | Same cause: the same fixture, now null-bearing, folded at the producer tap. | `3173e9c` |

Both digests are currently the SAME value because, for this shape, the Flight
IPC round trip preserves the buffer layout byte-for-byte — measured, both before
and after the nulls were added. That is a coincidence of the shape (no sliced or
offset buffers), not a property of the round trip, and it is precisely what the
wire tap alone could never tell anyone: the wire digest happened to reflect the
builders' output, but nothing asserted that it would. The oracle reports the
relationship per run (the `taps …` log line) and deliberately does not assert it
in either direction.

## 2. What the null plan places, and where

`NullPlan::Pinned` (`cqlite-flight/tests/support/ws0_fixture.rs`) drops cells as
a pure function of the row's index within its partition (`r`). Partitions enter
the stream 100 rows apart and 100 is congruent to 4 (mod 8), which is what makes
a stride-8 rule inside a partition land on two different bit alignments.

| Column | Arrow type | Rule | Nulls | Observed bit offsets (mod 8) |
|---|---|---|---|---|
| `metric_a` | `Int32` | `r % 8 == 0` | 65 | **`{0, 4}`** — byte-ALIGNED and NON-boundary, in one column |
| `region` | `Utf8` (offsets buffer) | `r % 8 == 3` | 65 | `{3, 7}` |
| `payload` | `Utf8`, 414 chars | `r % 40 == 17` | 15 | `{1, 5}` |
| `device_id` | `FixedSizeBinary(16)` | partition tail (`r == 99`) | 5 | `{3, 7}` — for the final partition this is the last VALID bit of the final batch's last bitmap byte, against the padding |

Totals: 150 null cells, 5,850 non-null cells, 11.70 cells/row (was 12.00). All
four columns hold nulls in all four batches, so nothing is confined to a
trailing position. Every number above is MEASURED by `ValidityCoverage` in the
oracle and asserted per column — a total that still adds up while nulls moved
between columns cannot pass.

## 3. Non-vacuity proof (perturbation, run and reverted, not committed)

The digest is sensitive to validity-bit POSITION, not merely to null count. Two
temporary perturbations of the fixture, each run and then reverted:

| Perturbation | Nulls | Per-column census | Non-null cells | Digest |
|---|---|---|---|---|
| none (committed state) | 150 | device_id 5 / metric_a 65 / payload 15 / region 65 | 5,850 | `0xe6eccf8a9ffbca11` |
| `device_id` null moved from `r == 99` to `r == 98` in ALL 5 partitions | 150 | unchanged | 5,850 | `0x9a5efb7f7206f40c` |
| `device_id` null moved from `r == 99` to `r == 98` in partition 3 ONLY — exactly **one** validity bit, moved one position | 150 | unchanged | 5,850 | `0xa110410383426469` |

The third row is the decisive one: a single validity bit moved by a single row
left the total null count, the per-column null census, the non-null census and
the cells-per-row figure all IDENTICAL — every count-based assertion in the
oracle passed — and only the digest caught it, at both taps. The oracle also
carries a permanent, committed self-check
(`assert_fold_detects_a_shifted_validity_bit`) proving the fold moves when a null
shifts within a bitmap byte, across a byte boundary, or appears at all; without
it, every coverage assertion could pass over a fold that ignored the bitmap.

## 4. No measured figure is re-derived by this change

The `313.0 ns/row` IPC-framing figure (`baseline-2026-08-03.md`, line 225) and
the AC1 `-16.3%` shortfall (`abc-interleaved-2026-08-03.md`, line 462) do NOT
share an input with the CI digest fixture. Verified:

* **Different corpus.** Those figures were measured over the on-disk 4M-row
  corpus at `/data/ws0-3096` (40,000 partitions, 12 cells/row), whose
  `nb-1-big-Data.db` sha256 is pinned in
  `docs/reports/ws0-3096-artifacts/corpus-identity.json` as
  `4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269`. It is
  still exactly that sha256 on disk, mtime `2026-08-03 04:37` — this change
  writes nothing under `/data`. The CI digest fixture is written in-test into a
  `tempfile::tempdir()` by `cqlite-flight/tests/support/ws0_fixture.rs` and
  never touches that path; it is reachable only through the opt-in
  `CQLITE_WS0_CORPUS_DIR` arm, which is unset in CI and skips.
* **The generator and driver are not in this branch.** Those figures came from
  `scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096` over a corpus built by
  `tools/ws0-corpus-gen` (`measurement-method.md`, sections 1 and 2). Neither
  `scripts/perf/` nor `tools/ws0-corpus-gen/` exists in this branch — both are
  re-anchored to issue #3272 — so no code here can regenerate or alter that
  corpus.
* **Different batch shape.** The measured runs use
  `CQLITE_WS0_BATCH_SIZE=8192` (`measurement-method.md`, line 277); the oracle
  uses `BATCH_SIZE = 128`.
* **The one shared module is unchanged for its other consumer.** The only other
  consumer of `ws0_fixture.rs` is
  `cqlite-flight/tests/issue_3096_framing_subphase.rs`, which calls
  `CorpusSpec::small(...)` with no `with_null_plan` and therefore gets
  `NullPlan::None` — the original synthesis, byte-identical fixture bytes. The
  null plan is opt-in for exactly that reason. That test passes unmodified.

## 5. Correction note — the "digest oracle UNCHANGED" citations

Across this delivery the lead cited "digest oracle UNCHANGED" roughly fifteen
times. From the oracle's introduction until commit `3173e9c`, the pinned
Arrow-buffer digest observed exactly two things: the VALUE bytes of every column
of every `RecordBatch` a Flight client decoded off `do_get` after IPC
serialisation and client-side reconstruction, and the invariance of those bytes
between the forced `bypass` arm and the forced `merge` arm at a pinned `now`.
That arm invariance does hold, it is R3's primary claim, and every "UNCHANGED"
citation is a true statement about it. What the oracle did NOT observe over that
period is, first, the Arrow builders' output before `encode_do_get` — it hashed
only post-round-trip bytes, so a builder defect that the IPC round trip
normalised away would not have moved it — and second, any validity-bitmap
content whatsoever: all twelve columns of all 500 fixture rows were non-null, so
every bitmap was absent or all-set and a misplaced validity bit had nothing to
misplace. The bitmaps were folded, not exercised. C's `R3: satisfied` verdict,
which certified the oracle as folding "value buffers and validity bitmaps", was
therefore correct on the first half and unmet on the second, which is why the
owner voided it. The oracle observes both from commit `3173e9c` forward: the
producer-side tap that folds the builders' output before `encode_do_get` arrived
in `fcd96ca`, and validity-bitmap content arrived in `3173e9c` as 150
deterministic nulls placed at both byte-aligned and non-byte-aligned bit offsets
within a single column, with the placement itself measured and asserted rather
than narrated, and with a single shifted validity bit demonstrated to move both
digests while every count-based assertion stayed green.
