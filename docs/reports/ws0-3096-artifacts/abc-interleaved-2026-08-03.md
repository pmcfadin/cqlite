# Issue #3096 — the interleaved A/B/C measurement record (2026-08-03)

The measurement this change's throughput claim rests on, and the **only**
comparison this session licenses. Machine-readable twin, with every per-run
number: `abc-interleaved-runs.json` (same directory). Method, traps and pinning:
`measurement-method.md`. Phase-0 pre-change baseline: `baseline-2026-08-03.md`
— **read its drift annotation before reusing any absolute from it.**

**Headline: cumulative +2.0%. AC1 is UNMET, 15.0% short, and is re-anchored to
issue #3248.** Spec R5 (owner-approved) makes a correctly-measured,
correctly-reported negative result a satisfying outcome of this change; it does
not make AC1 satisfied, and nothing here is framed as if it did.

> ## SUPERSEDED IN PART — read §10 before quoting any lever-4 number here
>
> **Every figure in §§1–7 was measured with the flight-data re-slicing target at
> 4 MiB.** The #3096 review found that value sat exactly ON
> `GRPC_DEFAULT_MAX_MESSAGE_BYTES` behind a `<=` guard that admitted it, and the
> target moved to **4,063,232 B (3.875 MiB)** — ceiling less a 64 KiB framing
> reserve less a 64 KiB inexactness margin. That changes framing geometry, so
> lever 4's **+4,817 rows/s / +2.3% / −441 cycles/row is NOT the delivered
> figure**.
>
> **Re-measured at the shipped target (§10, 8 rounds, 3 arms, 24 runs): lever 4
> measures at ZERO — median −72 rows/s (−0.03%), 4 of 8 rounds positive.** The
> +2.3% did not reproduce. It is kept here as a measurement AT the superseded
> target, labelled as such, and it is never restated as delivered.

> **THE ONE RULE THIS RECORD EXISTS TO ENFORCE.** No absolute on this box is
> reusable across sessions — the *untouched* bare scan drifted **370,134 →
> 333,206 rows/s (~10%)** within this single delivery. Compare arms only inside
> ONE interleaved session, against a byte-identical drift control, or do not
> compare at all.

---

## 1. Design

| | |
|---|---|
| arms | **3** release binaries, one per arm |
| `BASE` | `f4f8ce9` — pre-lever tip of this branch |
| `L4` | `aeadaa2` — `BASE` + lever 4 (state the Flight encoder's re-slicing target) |
| `L46` | `af2d888` — `BASE` + lever 4 + lever 6 (build the egress Arrow schema once per merge) |
| reps | **1 rep at a time**, **10 rounds per arm**, 30 measured runs |
| rotation | arm order **rotated per round**, period 3: `BASE/L4/L46` → `L4/L46/BASE` → `L46/BASE/L4` → … so no arm holds a fixed position |
| continuity | one continuous session, 06:53–07:49 UTC (`ab3` = rounds 1–5, `ab3b` = rounds 6–10); **nothing else on the box** |
| drift control | the bare-scan arm (`execute_streaming`) in **every** run — see the precision note below |
| measured surface | Flight `do_get`, **bypass** arm, over a real loopback gRPC transport |
| corpus | `/data/ws0-3096` — 4,000,000 rows / 40,000 partitions, `Data.db` sha256 `4a903f6f…ae269`, 693.69 B/row, uncompressed |
| pinning | server `taskset -c 2,10` (**verified** siblings of one physical core), client `4,12,5,13,6,14,7,15` |
| counters | `perf stat -C 2,10` — **CPU-WIDE**, sibling-aggregate, never `-p` |

**Three binaries, verified distinct at run time — but their md5 digests are not
recoverable.** Each arm was built separately in `--release` and swapped in per
run, and the digests were checked distinct before the session started. The
digests themselves were never written into the run artifacts and the staged
copies no longer exist, so they are **not restated here rather than
reconstructed**. That is a rig gap, not a footnote: `results.json` should record
the measured binary's digest. Filed as inherited work with the rest of the rig
(#3248 §"Inherited rig").

**The drift control, stated precisely — it is code-identical, not literally
byte-identical.** Neither lever touches the scan path: lever 4 is
`cqlite-flight` only, and lever 6's `cqlite-core` half adds a schema-reuse entry
point in `export/arrow_convert.rs` that the bare scan never calls. So the code
the control **executes** is the same in all three arms. Strictly, though, the
`ws0-scan-bench` binary links `cqlite-core`, so the `L46` arm's copy is not
*literally* byte-identical to `BASE`/`L4`'s. The empirical check that no arm
effect leaks into the control is the per-arm control medians, which agree to
within **0.25%**: `BASE` 332,741 / `L4` 333,557 / `L46` 332,847 rows/s.

**The corpus is a PERFORMANCE FIXTURE ONLY** (CQLite-written + CQLite-read,
therefore invariant to a uniform framing error — #3042). It is never a
correctness oracle.

---

## 2. The discarded attempt — recorded first, because a hidden discard is a hole

**A first interleaved attempt (`ab-lever4b`, 06:32–06:38 UTC) was thrown out in
full. No number from it appears anywhere in this change.**

It was a two-arm interleave (`A` = `BASE`, `B` = `L4`), 1 rep per arm per round,
alternating. A concurrent `cargo` release build landed on the pinned server
cores mid-attempt. The counters carry the signature:

| run | completed (UTC) | arm | `do_get` rows/s | `do_get` cycles/row | `do_get` IPC | drift control rows/s |
|---|---|---|--:|--:|--:|--:|
| `A-1` | 06:32:40 | `BASE` | 215,915 | 24,088 | **1.516** | 335,679 |
| `B-1` | 06:34:30 | `L4` | 206,427 | **26,068** | **1.442** | 332,270 |
| `A-2` | 06:36:16 | `BASE` | 213,386 | **25,914** | 1.458 | 335,540 |
| `B-2` | 06:37:56 | `L4` | — | — | — | **ABORTED** (no `results.json`, no `summary.txt`) |

IPC fell **1.52 → 1.44** and cycles/row rose ~8% while rows/s fell only ~4% —
cycles inflating faster than throughput falls is exactly what foreign work
inside a CPU-wide (`-C 2,10`) window looks like. The attempt was aborted
mid-round-2 and re-run from scratch as `ab3`/`ab3b` after the build finished.

**Attribution, stated at its real strength:** `target/release/deps` holds
`cqlite_core` / `cqlite_flight` / `flight_loadgen` release artifacts stamped
06:38:23–06:40:30 UTC, and `rustc` writes its outputs at the *end* of
compilation — so a release build was resident on the box across the tail of this
attempt. That plus the counter signature is the evidence. It is the operator's
contemporaneous call, **not** an independently instrumented per-CPU trace of the
offending threads.

**The lesson, which is the reason this section exists.** The drift control did
**not** move during the contamination (335,679 / 332,270 / 335,540 rows/s — all
normal). Its ~12 s window escaped interference that hit the ~57 s Flight window.
So a byte-identical drift control is **necessary but not sufficient**: what
flagged this run was the **IPC + cycles/row signature on the measured arm**.
Watch both, and never treat a quiet control as proof the session was clean.

---

## 3. Every per-run number (30 runs)

Raw `perf` CSVs, loadgen JSONL, `results.json` and `summary.txt` per run live in
`target/perf-ws0-3096/{ab3,ab3b}/<ARM>-<n>/` — build output, not committed. Each
row below is one run; each `raw_dir` is named in the JSON twin.

| round | arm order in round | arm | `do_get` rows/s | `do_get` cycles/row | `do_get` IPC | window s | `do_get` cycles | drift control rows/s | control cycles/row | control IPC |
|--:|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | BASE/L4/L46 | `BASE` | 211,967 | 24,414 | 1.497 | 56.612 | 292,964,277,885 | 335,346 | 18,577 | 1.475 |
|  |  | `L4` | 220,380 | 23,717 | 1.513 | 54.451 | 284,601,891,145 | 334,839 | 18,590 | 1.476 |
|  |  | `L46` | 220,948 | 23,536 | 1.519 | 54.311 | 282,435,916,646 | 331,162 | 18,817 | 1.472 |
| 2 | L4/L46/BASE | `L4` | 217,032 | 23,880 | 1.498 | 55.291 | 286,558,740,210 | 334,534 | 18,638 | 1.469 |
|  |  | `L46` | 215,824 | 23,863 | 1.504 | 55.601 | 286,352,426,437 | 334,760 | 18,576 | 1.473 |
|  |  | `BASE` | 214,659 | 24,085 | 1.485 | 55.903 | 289,025,535,414 | 333,589 | 18,563 | 1.480 |
| 3 | L46/BASE/L4 | `L46` | 218,459 | 23,758 | 1.515 | 54.930 | 285,097,853,859 | 328,579 | 18,893 | 1.470 |
|  |  | `BASE` | 216,579 | 24,098 | 1.503 | 55.407 | 289,170,926,286 | 332,752 | 18,688 | 1.475 |
|  |  | `L4` | 227,627 | 23,122 | 1.536 | 52.718 | 277,459,309,334 | 334,026 | 18,668 | 1.469 |
| 4 | BASE/L4/L46 | `BASE` | 213,512 | 24,239 | 1.503 | 56.203 | 290,868,103,577 | 332,729 | 18,737 | 1.473 |
|  |  | `L4` | 215,318 | 24,168 | 1.482 | 55.732 | 290,010,492,199 | 332,679 | 18,716 | 1.472 |
|  |  | `L46` | 222,831 | 23,408 | 1.523 | 53.852 | 280,896,387,303 | 332,478 | 18,712 | 1.477 |
| 5 | L4/L46/BASE | `L4` | 232,694 | 22,883 | 1.529 | 51.570 | 274,597,539,563 | 333,636 | 18,673 | 1.474 |
|  |  | `L46` | 213,853 | 24,117 | 1.501 | 56.113 | 289,400,723,611 | 335,557 | 18,573 | 1.471 |
|  |  | `BASE` | 211,693 | 24,504 | 1.478 | 56.686 | 294,053,655,255 | 333,052 | 18,704 | 1.473 |
| 6 | BASE/L4/L46 | `BASE` | 213,218 | 24,337 | 1.493 | 56.280 | 292,041,566,723 | 332,045 | 18,785 | 1.471 |
|  |  | `L4` | 219,544 | 23,814 | 1.501 | 54.659 | 285,773,265,115 | 335,454 | 18,566 | 1.475 |
|  |  | `L46` | 210,647 | 24,440 | 1.470 | 56.967 | 293,283,329,949 | 333,203 | 18,705 | 1.474 |
| 7 | L4/L46/BASE | `L4` | 211,001 | 24,566 | 1.464 | 56.872 | 294,789,855,909 | 331,532 | 18,765 | 1.474 |
|  |  | `L46` | 214,822 | 24,009 | 1.492 | 55.860 | 288,105,983,082 | 332,490 | 18,753 | 1.474 |
|  |  | `BASE` | 207,889 | 24,818 | 1.472 | 57.723 | 297,815,171,205 | 330,400 | 18,843 | 1.469 |
| 8 | L46/BASE/L4 | `L46` | 217,387 | 23,818 | 1.497 | 55.201 | 285,811,028,528 | 334,381 | 18,588 | 1.473 |
|  |  | `BASE` | 218,338 | 23,900 | 1.517 | 54.961 | 286,796,775,774 | 328,771 | 18,958 | 1.472 |
|  |  | `L4` | 210,940 | 24,552 | 1.470 | 56.888 | 294,622,798,206 | 330,416 | 18,827 | 1.475 |
| 9 | BASE/L4/L46 | `BASE` | 213,430 | 24,360 | 1.487 | 56.224 | 292,320,167,430 | 336,387 | 18,507 | 1.469 |
|  |  | `L4` | 216,472 | 24,025 | 1.500 | 55.434 | 288,301,239,839 | 333,478 | 18,668 | 1.478 |
|  |  | `L46` | 220,806 | 23,553 | 1.522 | 54.346 | 282,630,035,266 | 329,846 | 18,903 | 1.469 |
| 10 | L4/L46/BASE | `L4` | 223,955 | 23,477 | 1.519 | 53.582 | 281,726,325,721 | 332,889 | 18,729 | 1.477 |
|  |  | `L46` | 218,196 | 23,803 | 1.504 | 54.996 | 285,630,528,713 | 336,040 | 18,544 | 1.474 |
|  |  | `BASE` | 219,865 | 23,765 | 1.529 | 54.579 | 285,179,533,190 | 331,744 | 18,824 | 1.472 |

Notes on the columns:

* `do_get` rows/s is over **3 full scans** per run (12,000,000 rows); the drift
  control is **1** scan (4,000,000 rows). Row denominators are printed with
  every figure in the JSON twin.
* the drift control's cycles are **setup-subtracted** (a separately measured
  `--setup-only` `perf` window); the Flight arm's setup happens before its
  window opens, so it is outside by construction.
* `cycles/row` is summed over **both SMT siblings** of the pinned physical core.
  All arms are counted identically, so arm-to-arm deltas are unaffected — the
  **absolute** is comparable only to figures gathered the same way (§7).

---

## 4. Per-arm medians and spreads — AT THE SUPERSEDED 4 MiB TARGET

Flight `do_get` (bypass), n = 10 per arm:

| arm | rows/s (median) | spread | cycles/row (median) | spread | IPC (median) |
|---|--:|--:|--:|--:|--:|
| `BASE` | **213,471** | 5.6% | **24,288** | 4.3% | 1.495 |
| `+L4` | **218,288** | 10.0% | **23,847** | 7.1% | 1.501 |
| `+L4+L6` | **217,791** | 5.6% | **23,810** | 4.3% | 1.504 |

Bare-scan **drift control** — code-identical in all three arms (§1), n = **30**:

| | median | min | max | spread |
|---|--:|--:|--:|--:|
| rows/s | **332,970** | 328,579 | 336,387 | **2.3%** |
| cycles/row | 18,704 | 18,507 | 18,958 | 2.4% |

The control's 2.3% over the whole session is what makes the arm comparison
readable at all: the effect being measured (+2.0%) is *inside* the per-arm
spread (5.6–10.0%), so the medians alone cannot carry the claim — §5 does.

*(`+L4+L6`'s median is 217,791.4 rows/s exactly; earlier notes and issue #3248
round it to 217,792.)*

---

## 5. Paired within-round attribution — each lever on its own (spec R5), AT THE SUPERSEDED 4 MiB TARGET

A round's three runs sit within ~5 minutes of each other, so differencing
**within** a round cancels session drift that the medians cannot. Deltas are
`do_get` rows/s:

| lever | r1 | r2 | r3 | r4 | r5 | r6 | r7 | r8 | r9 | r10 | positive | median |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| lever 4 (`L4` − `BASE`) | +8,413 | +2,373 | +11,048 | +1,806 | +21,001 | +6,326 | +3,112 | -7,398 | +3,042 | +4,091 | 9/10 | **+3,601.6** |
| lever 6 (`L46` − `L4`) | +568 | -1,208 | -9,168 | +7,513 | -18,841 | -8,897 | +3,821 | +6,446 | +4,333 | -5,760 | 5/10 | **-320.1** |
| cumulative (`L46` − `BASE`) | +8,980 | +1,165 | +1,879 | +9,319 | +2,160 | -2,571 | +6,934 | -951 | +7,376 | -1,669 | 7/10 | **+2,019.6** |

Read plainly:

* **Lever 4 (encoder re-slicing target) was a real, small win AT THE 4 MiB
  TARGET.** 9 of 10 rounds positive, median **+3,602 rows/s (+1.7%)**. One round
  negative. Its mechanism is independently verified on message counts, not on
  throughput alone: `cqlite-flight/src/streaming_framing_tests.rs` drives the real
  `encode_do_get` and pins the framing. **At the SHIPPED 3.875 MiB target this
  gain does NOT reproduce — see §10, where it measures at zero.** The framing
  mechanism survives; the throughput number does not.
* **Lever 6 (build the egress Arrow schema once per merge) does not move
  throughput.** 5 of 10 rounds positive, median **−320 rows/s** — indistinguishable
  from noise at this spread, in either direction. It is retained because it is
  strictly less work per batch and its correctness is proven by the unchanged
  digest, **not** because it was measured to help. Recording a lever at zero is
  the point of per-lever attribution (spec R5: "a cumulative number can never
  hide a lever that cost throughput").

---

## 6. Conclusion — AC1 is UNMET (at the superseded target; §10 restates it at the shipped one)

| | |
|---|---|
| cumulative | 213,471 → **217,791 rows/s** = **+4,320 (+2.0%)** |
| ratio (control ÷ `do_get`) | **1.560x → 1.529x** |
| AC1 target on this session's control | **256,131 rows/s** (= 332,970 ÷ 1.3) |
| shortfall | **−38,340 rows/s (−15.0%)** |
| **AC1** | **UNMET** — re-anchored to **#3248**; lever 1 routes to **#3231** |

**We do not launder a negative result through an optimistic title.** The change
delivers a committed rig, a closed attribution blind spot (IPC framing, §7), a
+1.7% lever, a measured-at-zero lever, and an honest 15.0% gap. Spec R5 makes
that a satisfying outcome of *this change*; it does not make the ratio met.

Also still owed, and never restated as reproduced: the WS0 absolutes (240,100 /
312,155 rows/s — corpus- and machine-bound), the stock-Cassandra head-to-head
leg (no Cassandra on this box), and the compressed-corpus shape (#1406).

---

## 7. What is positively measured, and what is only a complement

**The 82% figure is a COMPLEMENT, NOT AN ATTRIBUTION.** Read this before
funding anything.

| region | per row | basis |
|---|--:|---|
| IPC framing (`stream_encode_framing`) | **313.0 ns/row** | **positively measured** — a sub-phase this change added around the encoder stream |
| "array build" (`stream_encode`) | 1,432.9 ns/row | **`1,746 − 313`. A subtraction.** Labeled "array build" from the call graph, with **no per-function data inside it** |

The only positively measured region on the encode path is IPC framing. Levers 2
(drop the intermediate `Vec<Option<T>>`), 3 (fold the estimate pass) and 1
(column-major build) are **three unpriced candidates sitting in one
undifferentiated bucket**. Nothing measured says which — if any — dominates it.
No prose in this tree may say a lever "dominates the 82%".

### Open reconciliation — for the successor, NOT a finding of this change

Two independently measured quantities do not reconcile, and this change does not
claim to reconcile them:

* Flight-vs-bare-scan gap (Phase-0 warm): **+4,697 cycles/row (+25.0%)**
* encode region: **1,746 ns/row** (313.0 + 1,432.9)

**Clock basis, stated explicitly.** `cycles/row` here is **sibling-aggregate**:
`perf stat -C 2,10` is CPU-WIDE over *both* SMT siblings of one physical core —
not per-process, not per-thread, and not per-core-single-thread. The two arms'
own observed sibling-aggregate cycle rates in the Phase-0 session are
`370,134 × 18,814 = 6.96e9` and `249,041 × 23,511 = 5.86e9` cycles/s (measured
per-thread clock on this box: ~3.6 GHz). Converting the gap:

| reading | ns/row for +4,697 cycles/row | share of the 1,746 ns/row region |
|---|--:|--:|
| Flight arm's observed sibling-aggregate rate (5.86e9) | ~802 | 46% |
| scan arm's observed sibling-aggregate rate (6.96e9) | ~674 | 39% |
| both siblings fully busy at 3.6 GHz (7.2e9) | ~652 | 37% |
| most generous single-thread reading (3.6e9) | ~1,305 | 75% |

**Under every reading the gap is smaller than the encode region — and under the
sibling-aggregate readings, less than half of it.** Direction is robust to the
clock assumption. The implication is not cosmetic: **the encode region is not
wholly additive over the bare-scan arm.** Part of it is work the bare scan does
too, which means a lever on the shared path could be a large absolute win and
nearly worthless for AC1's ratio.

A second, separate mismatch feeds the same reconciliation: the region's ns/row
are **wall times on concurrent pipeline threads** (they overlap and do not sum
to the `stream` phase), while `cycles/row` is a CPU-wide count over two
siblings. Those are different currencies. **This is #3248 AC4 — an open task,
not a resolved result.** If it cannot be reconciled, that is itself the finding.

---

## 8. Cross-links

* **#3248 — successor. AC1 is re-anchored there** (attribute inside the
  unattributed region and build a bare-scan-vs-`do_get` differential *before*
  any array-build lever is funded). The reconciliation in §7 is its AC4.
* **#3231** — lever 1 (column-major build) as an unpriced candidate.
* **#3232 (publishable absolutes vs stock Cassandra)** required corpus sha
  `22d9ae22…ce922c` **or a geometry-matched regeneration**. The committed
  `tools/ws0-corpus-gen` **is** that regeneration path — deterministic from seed
  `30960001`, driven through the production `SSTableWriter`, pinned by its own
  recorded sha256/row-count/byte-shape. **This change therefore retires that
  blocker**; what remains on #3232 is the provision hold and the absence of
  Cassandra on the box, not a corpus-identity gap.
* **#3234 (BTI `da` perf corpus)** should **mirror this determinism contract**
  rather than invent a second one: byte-identical output across 3 runs from a
  recorded seed, generated on the **production writer**, pinned by its own
  recorded digest + shape, non-vacuous (zero rows exits non-zero).
  `gen-perf-corpus-bti.sh` should use `tools/ws0-corpus-gen` +
  `scripts/perf/ws0-baseline.sh` as its **template**. See `scripts/perf/README.md`.
* **#3224** — the tooling gap (bare-metal counters) that leaves §7's region
  unattributed; this is the second consecutive perf run to pay for its absence.

## 9. Provenance

| artifact | where |
|---|---|
| per-run raw (`perf` CSV, loadgen JSONL, `results.json`, `summary.txt`) | `target/perf-ws0-3096/{ab3,ab3b}/<ARM>-<n>/` — build output, not committed |
| discarded attempt's raw | `target/perf-ws0-3096/ab-lever4b/` — build output, not committed |
| **every per-run number, committed** | `abc-interleaved-runs.json` (this directory) |
| driver transcripts | `/tmp/ab3.log`, `/tmp/ab3b.log` — machine-local, superseded by the JSON twin |

Re-derive from a clean checkout:

```bash
cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out /data/ws0-3096
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096   # per arm, interleaved, rotated
```

---

## 10. THE REVIEW RE-MEASUREMENT — lever 4 at the SHIPPED 3.875 MiB target (2026-08-03, 16:19–16:59 UTC)

**Why this session exists.** The #3096 review found
`FLIGHT_DATA_SIZE_TARGET_BYTES` set to exactly `GRPC_DEFAULT_MAX_MESSAGE_BYTES`
(4 MiB) behind a `<=` compile-time guard that therefore ADMITTED the one value the
module declared unsafe. The target is now **derived**: `4,194,304 − 65,536
(framing reserve) − 65,536 (the encoder's documented inexactness) = 4,063,232 B
(3.875 MiB)`. Changing the target changes framing geometry, so **§§4–6's lever-4
numbers stopped being certified** and had to be re-measured rather than
re-narrated.

### 10.1 Design — three arms, and why the third one is the honest isolation

| | |
|---|---|
| `BASE` | `f4f8ce9` release binary — the **same** BASE §1 used, for continuity |
| `NOTGT` | `HEAD` with the `with_max_flight_data_size` call **REMOVED** (so it inherits arrow-flight 53.4.1's 2 MiB default) |
| `L4P` | `HEAD` **as shipped**: target 4,063,232 B |
| reps | 1 rep at a time, **8 rounds per arm**, **24 measured runs** |
| rotation | arm order rotated per round, period 3: `BASE/NOTGT/L4P` → `NOTGT/L4P/BASE` → `L4P/BASE/NOTGT` → … |
| drift control | the bare scan in **every** run — and here the `ws0-scan-bench` and `flight-loadgen` binaries are **literally identical** across arms (only `cqlite-flight` is swapped), which is stronger than §1's code-identical control |
| binary md5 | `BASE` `bd5a7b4c180d6dc25f5c81a0449b3c04`, `NOTGT` `72d5b96a1ac45ceebe5168501aa93bdd`, `L4P` `21823b6e81e9d561da3495a5e177a0a4` — **recorded this time**, closing the §1 rig gap |
| driver | the COMMITTED rig: `scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 --reps 1 --temp warm --arm bypass --no-build`, once per (arm, round) |
| corpus / pinning / counters | unchanged from §1: `/data/ws0-3096`, server `taskset -c 2,10` (verified siblings), `perf stat -C 2,10` (CPU-WIDE, never `-p`) |
| prewarm | `ok` on all 24 runs (now recorded per rep in `results.json` — see the nit fix in this same change) |

`NOTGT` is the arm that actually answers the question. `L4P − BASE` also carries
the split and lever 6; only `L4P − NOTGT` isolates the encoder target with
everything else byte-identical.

### 10.2 Every per-run number (24 runs)

| round | arm order | arm | `do_get` rows/s | `do_get` cycles/row | `do_get` IPC | control rows/s | control cycles/row |
|--:|---|---|--:|--:|--:|--:|--:|
| 1 | BASE/NOTGT/L4P | `BASE` | 232,687 | 24,192 | 1.482 | 359,004 | 18,735 |
|  |  | `NOTGT` | 214,268 | 25,726 | 1.407 | 355,660 | 18,892 |
|  |  | `L4P` | 226,364 | 24,468 | 1.471 | 353,920 | 18,986 |
| 2 | NOTGT/L4P/BASE | `BASE` | 226,913 | 24,686 | 1.470 | 359,225 | 18,809 |
|  |  | `NOTGT` | 234,987 | 24,050 | 1.488 | 357,025 | 18,897 |
|  |  | `L4P` | 226,016 | 24,571 | 1.469 | 359,843 | 18,808 |
| 3 | L4P/BASE/NOTGT | `BASE` | 227,208 | 24,726 | 1.468 | 360,383 | 18,759 |
|  |  | `NOTGT` | 237,173 | 23,921 | 1.495 | 359,304 | 18,803 |
|  |  | `L4P` | 236,237 | 23,835 | 1.504 | 358,019 | 18,918 |
| 4 | BASE/NOTGT/L4P | `BASE` | 236,336 | 24,066 | 1.478 | 357,561 | 18,926 |
|  |  | `NOTGT` | 235,362 | 24,044 | 1.494 | 355,104 | 19,000 |
|  |  | `L4P` | 228,402 | 24,323 | 1.483 | 360,405 | 18,766 |
| 5 | NOTGT/L4P/BASE | `BASE` | 238,692 | 23,931 | 1.487 | 356,295 | 18,956 |
|  |  | `NOTGT` | 233,935 | 24,164 | 1.484 | 360,100 | 18,793 |
|  |  | `L4P` | 238,442 | 23,765 | 1.493 | 357,435 | 18,897 |
| 6 | L4P/BASE/NOTGT | `BASE` | 229,222 | 24,567 | 1.479 | 358,084 | 18,891 |
|  |  | `NOTGT` | 231,448 | 24,308 | 1.481 | 357,068 | 18,959 |
|  |  | `L4P` | 232,240 | 24,121 | 1.491 | 357,145 | 18,913 |
| 7 | BASE/NOTGT/L4P | `BASE` | 233,726 | 24,247 | 1.473 | 358,560 | 18,865 |
|  |  | `NOTGT` | 228,475 | 23,466 | 1.509 | 359,738 | 18,769 |
|  |  | `L4P` | 226,017 | 24,108 | 1.486 | 331,886 | 18,766 |
| 8 | NOTGT/L4P/BASE | `BASE` | 231,802 | 24,299 | 1.473 | 359,482 | 18,718 |
|  |  | `NOTGT` | 226,646 | 24,312 | 1.476 | 347,355 | 18,796 |
|  |  | `L4P` | 238,638 | 23,531 | 1.493 | 356,738 | 18,761 |

### 10.3 Per-arm medians and spreads (n = 8 per arm)

| arm | rows/s (median) | spread | cycles/row (median) | spread | IPC |
|---|--:|--:|--:|--:|--:|
| `BASE` | **232,245** | 5.1% | **24,273** | 3.3% | 1.476 |
| `NOTGT` | **232,691** | 9.8% | **24,107** | 9.4% | 1.486 |
| `L4P` (shipped) | **230,321** | 5.5% | **24,115** | 4.3% | 1.489 |

Bare-scan **drift control**, n = **24**: median **357,790 rows/s**
(331,886..360,405, spread 8.0%), cycles/row 18,837 (1.5%). The 8.0% is driven by
ONE run (`L4P-7`, 331,886); over the other 23 the control spread is **3.6%**
(347,355..360,405, median 358,019). The outlier is **recorded, not trimmed** — it
stays in every median above. Per-arm control medians agree to within **0.5%**
(`BASE` 358,782 / `NOTGT` 357,047 / `L4P` 357,290), so no arm effect leaks into
the control.

### 10.4 Paired within-round attribution

| comparison | r1 | r2 | r3 | r4 | r5 | r6 | r7 | r8 | positive | median rows/s | median cycles/row |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| **lever 4' isolated** (`L4P` − `NOTGT`) | +12,095 | -8,970 | -937 | -6,960 | +4,508 | +792 | -2,458 | +11,992 | 4/8 | **−72.2 (−0.03%)** | −136.9 |
| cumulative (`L4P` − `BASE`) | -6,323 | -897 | +9,028 | -7,933 | -249 | +3,018 | -7,709 | +6,835 | 3/8 | **−573.1 (−0.25%)** | −152.1 |
| no-target vs BASE (`NOTGT` − `BASE`) | -18,419 | +8,073 | +9,965 | -974 | -4,757 | +2,225 | -5,251 | -5,156 | 3/8 | **−2,865.2 (−1.23%)** | −140.2 |

### 10.5 Conclusion, stated plainly

* **Lever 4's throughput gain does NOT survive the wire-safety fix. At the shipped
  3.875 MiB target it measures at ZERO** — median **−72 rows/s (−0.03%)**, 4 of 8
  rounds positive, against per-arm spreads of 5.5–9.8%. The recorded **+4,817
  rows/s / +2.3% / −441 cycles/row was measured AT the superseded 4 MiB target**
  and is superseded, not re-labelled.
* **The cumulative figure is also indistinguishable from zero** in this session:
  `L4P − BASE` median **−573 rows/s (−0.25%)**, 3 of 8 rounds positive. §6's
  +2.0% belongs to the superseded target.
* **Lever 4' does cut cycles/row** by a median **136.9 (~0.6%)** with rows/s
  unmoved. **Spec R1 forbids reporting that as a win** — a profile improvement
  with unmoved throughput is explicitly not evidence of a gain (the #2877 shape),
  and it is not claimed as one here.
* **What lever 4 is retained for is WIRE SAFETY, and secondarily framing** — both
  positively verified in-repo, not by throughput:
  `cqlite-flight/src/streaming_framing_tests.rs` drives the real `encode_do_get`
  and asserts the message counts plus, at a capacity/payload ratio of ~1.0, that
  **every** emitted `data_body` stays under the reserved ceiling.
* **AC1 remains UNMET, by a wider margin in this session:** control median
  357,790 ÷ `do_get` 230,321 = ratio **1.553x**; AC1's target on this session's
  control is **275,223 rows/s**; shortfall **−44,902 rows/s (−16.3%)**.
  Re-anchored to **#3248**.
* **The cross-session rule holds again.** This session's untouched control sits
  ~7% ABOVE the morning session's (357,790 vs 332,970 rows/s) on the same box over
  the same bytes, nine hours apart. Second observation, same lesson: compare only
  within one interleaved session.

**So both landed levers are now measured at zero at the shipped target.** That is
the honest result, and spec R5 makes a correctly-measured, correctly-reported
negative result a satisfying outcome of this change. It is not padded, not
re-narrated, and no number here is laundered.

Machine-readable twin: `abc-interleaved-runs.json` →
`review_remeasurement_2026_08_03` (design, all 24 runs with binary md5s, per-arm
medians, the paired deltas, and this conclusion). Per-run raw (`perf` CSVs,
loadgen JSONL, `results.json`, `summary.txt`): `/data/ws0-arms/session/<ARM>-<n>/`
— build output, not committed.
