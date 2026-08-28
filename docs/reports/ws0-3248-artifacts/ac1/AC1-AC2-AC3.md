# AC1 / AC2 / AC3 — per-function attribution, the differential, and the priced levers

**This is the first per-function data ever taken inside this region.** #3217 left ~87% of its IPC
decay unattributed; #3096 left 82% of per-row encode in one undifferentiated complement labelled
"array build" from a call graph, with **zero per-function data inside it**. That complement is what
this document opens.

Instrument: `perfsym` binaries (symbols, no debuginfo, no frame pointers — codegen-faithful),
CPU-wide `perf record -e cycles -F 499 -C 2,10` bracketing exactly each counted window, 3 warm reps
per arm, 0 lost samples. Demangling verified by positive control before any of this was believed
(`raw/demangler-probe.md`).

---

## AC5 compliance, stated first because it shapes everything below

A flat profile yields a **CPU-share**, and AC5 forbids reporting one ("a share shift with unmoved
rows/s is a FAIL — the #2877 shape"). Every share below is therefore converted to **cycles/row** by
multiplying by the arm's own measured cycles/row **from the same profiled run** the shares come from
(bare scan 19,266; Flight 26,854). Shares are shown only in parentheses, as provenance for the
conversion.

---

## AC2 — the differential: SHARED vs FLIGHT-MARGINAL. TWO NUMBERS, NEVER SUMMED.

| bucket | bare scan (arm B) | Flight `do_get` (arm A) |
|---|--:|--:|
| **SHARED** | **7,348 cyc/row** (38.14%) | **8,926 cyc/row** (33.24%) |
| **FLIGHT-MARGINAL** | **0 cyc/row** (0.00%) | **3,842 cyc/row** (14.31%) |
| libc (allocator/memory) | 5,578 (28.95%) | 8,315 (30.96%) |
| kernel | 4,497 (23.34%) | 3,873 (14.42%) |
| unclassified | 1,260 (6.54%) | 693 (2.58%) |

Per-rep spread is tight — SHARED on arm B `38.56 / 38.06 / 37.80`, FLIGHT-MARGINAL on arm A
`14.64 / 13.99 / 14.29`.

**The classification is validated, not assumed: arm B measures 0.00% Flight-marginal in all three
reps.** Every symbol classified Flight-only is absent from the arm that should not execute it. Had
the split been wrong, that cell would not be zero.

### What this says about the gap, which is the question the issue exists to answer

The gap (AC0, unprofiled) is **+6,707 cycles/row**. Decomposed:

| component | cyc/row | share of the gap |
|---|--:|--:|
| **Flight-marginal code** (Arrow encode, estimator, framing) | **3,842** | **~57%** |
| the SAME shared code costing MORE on the Flight arm | +1,578 | ~24% |
| the allocator costing MORE on the Flight arm | +2,737 | ~41% |
| kernel costing LESS on the Flight arm | −624 | −9% |
| unclassified delta | −567 | −8% |

(Components are computed from the profiled run's internal gap of +7,588 cyc/row; they do not sum
exactly to it because the profiles account for ~96% of samples. They are reported as a
decomposition, not as an identity.)

**So the issue's central thesis is confirmed AND sharpened.** It predicted that the per-row
`HashMap` construction is "paid by BOTH arms. Removing it speeds up both; the ratio barely moves."
Measured: the shared bucket is **7,348 cyc/row on the bare scan** — a large absolute cost, and
**deleting it would move the ratio almost not at all**, because the Flight arm pays it too. Only the
**3,842 cyc/row of Flight-marginal code**, plus the arm-specific EXCESS on shared code and the
allocator, can close the ratio.

**And a result the issue did not anticipate: only ~57% of the gap is Flight-only CODE.** A further
~24% is the *same* shared code running **21.5% more expensively** on the Flight arm (8,926 vs 7,348),
and ~41% is allocator work. Candidate causes, none established here: cache pressure from Arrow
buffers, and SMT contention — #3096's pinning puts the `spawn_blocking` merge/encode thread and the
async gRPC framing thread on **one physical core's two hyperthreads** (`taskset -c 2,10`). That is a
hypothesis this profile cannot settle; it is recorded as the next question, not an answer.

---

## AC1 — per-function attribution inside the region

### The SHARED row-build path, measured on the arm that pays it alone (bare scan)

| cyc/row | share | symbol |
|--:|--:|---|
| 1,276 | 6.62% | `V5CompressedLegacyParser::parse_row_data_with_offset_impl` |
| 715 | 3.71% | `V5CompressedLegacyParser::parse_cell_value_schema_order` |
| 559 | 2.90% | `core::hash::sip::Hasher::write` |
| 536 | 2.78% | `HashMap<Arc<str>, Value>::insert` |
| 518 | 2.69% | `V5CompressedLegacyParser::decode_scalar_cell_value` |
| 472 | 2.45% | `core::str::converts::from_utf8` |
| 435 | 2.26% | `drop_glue::<QueryRow>` |
| 397 | 2.06% | `Value::into_owned` |
| 314 | 1.63% | `RandomState::hash_one::<&Arc<str>>` |
| 287 | 1.49% | `build_row_from_scan_cached` |

**The per-row `HashMap<Arc<str>, Value>` costs ~1,409 cyc/row in hashing and insertion alone**
(sip-hash 559 + `hash_one` 314 + `insert` 536) — **7.3% of the bare scan**, before `into_owned` (397)
or `QueryRow` drop glue (435). This is the quantity #3096 could only describe as a complement.

### The FLIGHT-MARGINAL region, per function

| cyc/row | share | symbol |
|--:|--:|---|
| **1,881** | **7.00%** | **`export::arrow_convert::rows_to_record_batch`** |
| 592 | 2.20% | `export::arrow_size::estimate_arrow_row_bytes` |
| 386 | 1.44% | `GenericShunt<Map<slice::Iter<Option<…>>>>` (the `collect::<Result<Vec<Option<T>>>>`) |
| 163 | 0.61% | `arrow_size::Estimator::charge_slot` |
| 136 | 0.51% | `MergeProducer::drive_row_source` |
| 98 / 72 / 39 / 38 / 35 | 0.36/0.27/0.15/0.14/0.13% | further `GenericShunt` instantiations |
| 93 | 0.35% | `arrow_size::Estimator::charge_child` |
| 51 | 0.19% | `arrow_convert::convert_column_to_array` |
| 38 | 0.14% | `bypass::ScanRowSource::next_step` |
| 38 | 0.14% | `arrow_array::GenericByteBuilder<GenericStringType>` |
| 20 / 19 | 0.07% each | `PrimitiveArray<Int32Type>::from`, `FixedSizeBinaryBuilder::append_value` |
| 18 | 0.07% | `h2::proto::connection::Connection` (the gRPC framing thread) |
| 15 | 0.06% | `arrow_builders_scalar::build_string_array` |
| 12 | 0.04% | `arrow_size::shape::value_branches` |
| **3,743** | **13.94%** | **listed total** (bucket total 3,842; the remainder is symbols below 0.04%) |

**`rows_to_record_batch` absorbs its inlined callees**, so its 1,881 cyc/row is the fused encode
body, not a wrapper. That is why `convert_to_arrays`, `rows_to_record_batch_trusted_schema` and
`transpose_columns` are absent — see the prediction outcomes below.

### The granularity floor, which is set by CODEGEN and not by effort

Several encode-region functions are single-call-site wrappers that fat LTO folds away. **AC1's
resolution is therefore bounded by the compiler**, and the bound is real: `rows_to_record_batch`'s
1,881 cyc/row cannot be subdivided further *on a codegen-faithful binary*. Going below it requires
`#[inline(never)]`, which perturbs codegen and is reported separately when taken.

---

## AC3 — the three levers, priced individually

Each lever is priced as the Flight-marginal cycles/row it would remove, with the **ratio** delta and
the **absolute** delta stated separately — because for a shared-path lever those diverge, which is
the whole reason AC3 asks for both.

| lever | what it removes | measured cyc/row | predicted ratio delta | predicted absolute delta |
|---|---|--:|---|---|
| **3 — fold the estimate pass** | `estimate_arrow_row_bytes` (592) + `charge_slot` (163) + `charge_child` (93) + `value_branches` (12) | **860** | 1.685x → **~1.63x** | Flight +3.3% rows/s; bare scan **0** |
| **2 — drop the intermediate `Vec<Option<T>>`** | the six `GenericShunt<Map<slice::Iter<Option<…>>>>` instantiations (668), plus an unquantified share of the allocator delta | **668+** | 1.685x → **~1.64x** | Flight +2.6% rows/s (+ allocator); bare scan **0** |
| **1 — column-major** (routed to #3231, priced here only) | would restructure `rows_to_record_batch` (1,881) + the transpose folded into it | **≤1,881** | 1.685x → **~1.57x** at best | Flight +7.5% rows/s at best; bare scan **0** |

**All three are Flight-marginal, so for all three the ratio delta and the absolute delta point the
same way** — the divergence AC3 warns about does *not* bite for these levers. It bites for a
**shared-path** lever, and this differential now prices that case too: removing the shared
row-build `HashMap` hashing (**1,409 cyc/row**) would speed up **both** arms, gaining roughly
**+7.3% on the bare scan and +5.2% on Flight** — a **larger absolute win than any of the three
levers** while moving the ratio *the wrong way* (1.685x → ~1.71x). **That is the inversion the issue
was written to catch, and it is now measured rather than argued.**

Even the most optimistic combination of levers 2, 3 and 1 (860 + 668 + 1,881 = 3,409 cyc/row, i.e.
**deleting 89% of all Flight-marginal code**) reaches a ratio of ~1.53x — **still short of 1.3x.**
Per AC6 this document does **not** re-assert that target; the figure is given only to show that the
priced levers cannot reach it, which is a finding about the levers rather than a goal restated.

---

## Prediction outcomes, reported as outcomes

**1. A registered prediction was FALSIFIED, and it is the largest symbol in the region.**
`rows_to_record_batch` was predicted absent (inlined). It **survived, at 7.00% — the single largest
Flight-marginal cost.** The rest of the expected-inlined list held exactly: `convert_to_arrays`,
`rows_to_record_batch_trusted_schema`, `flush_buffer`, `materialize_pending` and `transpose_columns`
are all absent, and `convert_column_to_array` survived only vestigially (0.19%).

**2. This lane's own leading hypothesis is PARTIALLY FALSIFIED.** Before measuring, this lane
registered `estimate_arrow_row_bytes`' 12-probes-per-row as "the leading Flight-marginal candidate",
on the reasoning that AC0's lower Flight IPC pointed at hashing. It is real and measurable — **592
cyc/row, with its estimator family totalling 860** — but it is **3.2x smaller than
`rows_to_record_batch`**. The hypothesis was directionally sound and wrong about rank; recorded here
rather than re-ranked silently after the fact.

**3. THE ARM IDENTITY IS RESOLVED — a question the rig declares unobservable.** The rig warns that
`bypass` only *requests* the fast path, the server never reports which it took, and "in the limit the
two arm rows could be the same code measured twice." The profile settles it by symbol presence:
`bypass::ScanRowSource::next_step` is **present**; `entry_to_row` and `assemble_read_cells`
(merge-path only) are **absent from all three reps**. **The server took the bypass arm**, so that
caveat is discharged for this run — and the technique generalises: a per-function profile can answer
arm identity whenever the production code will not report it.

---

## Controls, and an honest limitation that bounds all three

Three runs, giving a three-way decomposition:

| run | bare scan rows/s | Flight rows/s |
|---|--:|--:|
| release + no profiler (**AC0**) | 347,987 | 206,480 |
| perfsym + no profiler (**codegen control**) | 349,999 | 196,824 |
| perfsym + profiler (**AC1**) | 335,109 | 193,596 |

| effect | bare scan | Flight |
|---|--:|--:|
| codegen (perfsym vs release) | +0.58% | −4.68% |
| profiler attachment | −4.25% | −1.64% |
| combined | −3.70% | −6.24% |

**THE LIMITATION, STATED PLAINLY: all three runs are in DIFFERENT SESSIONS, and the documented
cross-session drift (~10%) EXCEEDS every effect in that table (0.58%–4.68%). So these controls
establish that the perturbations are SMALL — bounded by a few percent — but they cannot resolve them
individually.** Doing so would need release and perfsym interleaved within one session, which the rig
cannot do: `--bin-dir` takes one directory per run.

**Why AC1's conclusions survive that limitation.** The profile's shares are used as a *decomposition
of an arm*, never as an absolute, and the decomposition is not close to any boundary: a
few-percent perturbation cannot turn a **14.31% vs 0.00%** split into a different answer, and the
per-rep spreads (±0.33pp on the marginal bucket) are an order of magnitude below the effect being
reported. The IPC and cycles/row *shape* is also nearly unchanged across all three runs (bare-scan
IPC 1.4408 / 1.4528 / 1.4521), which is the property that licenses reading the profiled run's
composition as the unprofiled arm's composition.

Where a conclusion WOULD be sensitive to this, it is not drawn. In particular the −4.68% Flight
codegen effect is **not** reported as a perfsym property: at 2x the arm's own spread and inside the
drift band, it is **unresolved**, and it is listed here as an open item rather than a measurement.
