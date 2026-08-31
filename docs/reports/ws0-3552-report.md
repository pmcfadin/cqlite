# WS0 #3552 — fold `estimate_arrow_row_bytes` into the `do_get` build pass

**Status: shipped WITHOUT an AC3 measurement.** Correctness is certified; the throughput A/B was
not taken on the delivery box and is **not** reported. This document records why, and everything a
later measurer needs so the run is cheap rather than re-derived.

Parent: epic #2817. Predecessor: #3248 (which priced the lever). Rig: #3096.

## 1. What changed

The standalone estimate pre-pass over every row is gone from both `do_get` row routes. The build
pass's first stage — the row→column transpose — moved to **push** time (`ArrowRowAccumulator`), and
the estimator charges from the cells it just resolved. Each cell is resolved once.

The **aggregate** route (`split_rows_into_batches`) deliberately keeps the standalone estimator: it
post-hoc-groups already-materialized rows, so the seam does not reach it.

## 2. AC3 was not measured — the two blockers, both real, neither fixable from this lane

### 2a. Quiescence could not be established

`scripts/perf/ws0_quiescence.py judge` requires a **zero competing census across the whole
measurement window**. Throughout the delivery window two peer lanes held 88–96 GB of `target/` and
were building. No window existed.

Two rig defects surfaced while trying. **Both fail CLOSED** (false refusals, never false
certifications), so no published number is affected:

1. **The committed sampler and the committed judge do not compose.** `ws0_quiescence.py sample`
   emits `{competing, competing_count, load}` with **no `ts` field**; `judge --timeseries` requires
   one and refuses with `QUIESCENCE_TIMESERIES_MALFORMED: record has no usable ts field`. The
   frozen example in `ws0-3248-artifacts/quiescence/box-load-frozen.jsonl` has a *third*, flat
   schema (`ts/load1/load5/load15/runnable/rustc/cargo/...`) that the current sampler cannot
   produce. Supplying `ts` by hand advances the judge to its coverage check (≤10 s cadence, no
   unobserved stretch >30 s), which is sound.
2. **`COMPETING_CMDLINE = ("agent-gate.sh",)` matches any process that merely MENTIONS the
   string.** Diagnostic `grep`/`pgrep` commands run *by the operator taking the measurement* are
   counted as competing load — observed inflating the census to 15. The file's own comment two
   lines above documents this exact family for `cargo`, says it *"caused a FALSE REFUSAL of a quiet
   box"*, and removed `cargo` for that reason (it is caught by `comm`, "which cannot be spoofed by
   a shell that merely MENTIONS the command"). `agent-gate.sh` was left with the identical flaw and
   no `comm` backstop. Its own stated remedy applies: exclude by identity (self PID + ancestor
   walk), which the file already does elsewhere.

### 2b. A measurement run is a building run

Under the standing disk order, any building invocation derives a bar from its own measured growth.
The delivery box sat at or below the floor for much of the window.

### Why this is a SHIP and not a failure

AC3's own wording: *"A result below +2% is still a SHIP if correctness holds — record honestly, no
narrative padding (#3248 AC6 rule)."* Numbers taken at 85% disk with two lanes compiling would be
padding. #3248's own rule is the precedent: *"a well-measured 'array build is irreducible at this
shape' is a SATISFYING OUTCOME."* An unmeasured lever, honestly recorded, is likewise preferable to
a fabricated delta.

## 3. READ THIS BEFORE MEASURING: `stream_encode` is not comparable across this commit

The `stream_encode` sub-phase **changed scope in both directions**:

* it **lost** the flush-time transpose (moved to push time, then folded back in via
  `egress_flush::StageEncodeAccum`), and
* it **gained** the per-row width charge, which before this change sat in **no sub-phase at all**.

So it can read **lower or higher** on identical work, and there is no honest split of a fused region
into two buckets — that is what fusing means. **The comparable quantities are `rows/s` and
`merge + encode` together. A `stream_encode`-only comparison across this commit is meaningless in
both directions.** Stated in code at `StreamSubPhase::Encode`, `PHASE_STREAM_ENCODE`, `streaming.rs`
and `flush_credited`.

`StageEncodeAccum` costs **zero atomics per row** — per-row elapsed folds into a `u64` in the drive
loop's own frame and reaches the shared counter in ONE `add_nanos` on `Drop`, so a full scan makes
one atomic write regardless of row count.

**It is not clock-free, and an earlier revision of this report wrongly said it was.** When a flight
sink is installed it takes one `Instant::now()` pair per timed call — **two per row**, since both
halves of the push-time transpose (`stage`, then `commit`) are timed (issue #3552 roborev round 7;
before that round it was one pair per row, so the claim was already wrong when first written). With
**no** sink — compaction, CLI, point reads outside `do_get` — there is no clock at all, which is the
case the "zero clocks" wording came from.

Budget the two clock reads per row when interpreting an instrumented A/B. The instrument is cheap
relative to the work it measures, but it is NOT free, and this report exists to stop AC3 being
measured on a false premise.

## 4. Recipe for whoever takes AC3 (the expensive parts are already done)

**Corpus — generated and verified canonical, so do not regenerate blindly:**

```
/data/ws0-3096   4,000,000 rows / 40,000 partitions
                 Data.db 2,774,760,422 B
                 sha256  4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269
                 == the pin in tools/ws0-corpus-gen/src/measurement_corpus.rs
```
Generated by `cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out <dir>` (102 s).
A non-canonical corpus makes the rig stamp the report `NOT A WS0 BASELINE`.

**Same-session A/B.** The driver measures one tree at a time, so stage two binary sets and
alternate **one rep at a time with the arm order rotated per round**, ≥3 rounds:

```
cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen
cp target/release/{ws0-scan-bench,cqlite-flight,flight-loadgen} <bins>/base|after/
md5sum <bins>/*/cqlite-flight          # RECORD these — their absence is a rig gap (#3096 abc record)

scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 --reps 1 --temp warm --arm bypass \
  --bin-dir <bins>/base  --out target/perf-ws0-3096/ab/base-$n
# ...same with --bin-dir <bins>/after --out .../after-$n
```
`--bin-dir` implies `--no-build`; `--out` is refused if non-empty.

**Report `rows/s` AND `cycles/row`, never CPU-share** (a share shift with unmoved rows/s is a FAIL —
the #2877 shape). Split Layer 1 (invariant: IPC, cycles/row, ratio, cycles/row delta) from Layer 2
(absolutes: rows/s), print every spread beside every median, and print the row denominator — the
format `ws0-3248-artifacts/ac0/DELTA-TABLE.md` uses.

**Two inherited rules, both learned the hard way:**
* **No absolute is reusable cross-session** — an untouched bare scan drifted ~10% within one
  delivery. Same-session interleaved A/B with a drift control, or no comparison.
* **This delivery ran on `ip-172-31-6-169`, not #3096/#3248's `ip-172-31-7-163`.** Do not compare
  absolutes across hosts; only same-session ratio and delta.

**Environment (non-negotiable, and it cost this delivery an hour):** state `RUSTFLAGS` and
`CARGO_ENCODED_RUSTFLAGS` **as measured**, and prefer `env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS`.
`systemd-run --scope` **inherits the caller's environment**, and an exported `RUSTFLAGS` also
suppresses cargo's `target.rustflags` managed block, so mold is silently not applied. **The tell is
`mold=overridden` in the gate SUMMARY's `accelerators:` line** (see #3740). A reproduction only
corroborates if its **environment** differs — not just its tree, box, or operator.

## 5. AC4 — the oracle actually used

Certified by the **CI-fixture** buffer digest, which carries real validity coverage: 500 rows /
4 batches / 12 columns, 5,850 non-null cells + **150 nulls**, null bit offsets mod 8 spanning
`{0,1,3,4,5,7}`, nulls present in **all four** batches, producer digest == wire digest.

The **corpus-scale** digest is **unusable on `main`** and was not used: the WS0 corpus produces zero
null cells at the Arrow layer, so the oracle's anti-vacuity guard correctly refuses
(*"the validity bitmaps carry no content and this oracle proves nothing about them"*, exit 101).
Pre-existing, filed as **#3732**; the documented `ws0-verify-commands --digest` path fails on `main`.
**Byte-identity at 4M-row scale is therefore NOT claimed.**

## 6. Related

#3732 (corpus digest oracle unusable), #3742 (zero-column projection row count; premise unverified),
#3740 (never export `RUSTFLAGS`), #1116 (file-size split epic), #3248 / #3096 (rig + pricing).
