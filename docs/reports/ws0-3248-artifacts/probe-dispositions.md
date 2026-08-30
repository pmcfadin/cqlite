# Probe dispositions — D3, D4, and the #3096 zeros closure note

Every added deliverable is accounted for here: run, or not run with the reason. A probe silently
dropped is indistinguishable from a probe that found nothing.

---

## D3 — stub the sizing pre-pass and re-profile: **NOT RUN, and the reason is a confound**

**Registered** (as amended and approved): stub `estimate_arrow_row_bytes` to a constant in a
throwaway branch and re-profile, to validate lever 3 by removal.

**Why it is not run.** The estimate is not a passive observation — **it feeds the byte cap**
(`producer_stream.rs:351` → `BatchByteCap::cut_before` / `accumulate`). Stubbing it to a constant
therefore changes **where batches are cut**, and batch size is a first-order driver of the encode
cost: the bytes-touched differential measured 23,745 bytes/row of L2 traffic, so a 128-row batch
moves ~3.0 MB and an 8,192-row batch ~195 MB. A naive stub would measure *(estimator removed) +
(batching changed)* with no way to separate the two, and the change in batching is plausibly the
LARGER term.

Preserving the batching would require stubbing to the true **mean** Arrow row width — and the field
that looks like it supplies that, the Flight record's `bytes_total`, is
[the mislabelled one](raw/loadgen-bytes-metric-mislabelled.md): it reports allocated array memory,
not payload width. So the constant D3 needs is not available from the artifacts, and deriving it
would itself be a small project.

**What discharges D3's purpose anyway.** D3 exists to price lever 3, and lever 3 **is** priced — by
attribution, at **860 cyc/row** (`estimate_arrow_row_bytes` 592 + `Estimator::charge_slot` 163 +
`charge_child` 93 + `shape::value_branches` 12), from per-function self-time on three reps with per-rep
spread under 0.4 pp. Removal-validation would have been *additional* confidence, not the only route.

**What is lost by not running it, stated rather than glossed.** Attribution prices the estimator's
*self-time*; it does not capture second-order effects of removing it — freed cache footprint, or a
batching change that removal would itself cause. So the 860 cyc/row is a **lower bound** on lever 3's
value. Recommended as a follow-up with the batching held constant explicitly (stub to a measured mean
width AND assert the per-batch row count is unchanged between arms), which is a cleaner experiment
than this issue can fit.

---

## D4 — allocations/row via a counting allocator: **NOT RUN, with a partial substitute**

**Registered:** allocations/row ≈ the count of var-len columns (prediction **P2**), measurable via
dhat or a counting allocator.

**Why it is not run.** An exact count needs a `#[global_allocator]` wrapper in the server binary — a
change to the binary under measurement, and one whose overhead (a counter increment on every
allocation, on a path that allocates several times per row) is not obviously small relative to the
quantity being measured. That is a throwaway-build experiment on the same footing as the
`#[inline(never)]` variant, and it needs its own perturbation control.

**The partial substitute, from data already in hand.** The allocator's *cost* is measured, even though
its *count* is not:

| | bare scan | Flight `do_get` |
|---|--:|--:|
| libc (allocator/memory) share | 28.95% | 30.96% |
| in cycles/row | **5,578** | **8,315** |

So the allocator costs **+2,737 cyc/row more on the Flight arm** — ~41% of the +6,707 gap, and the
second-largest component after Flight-marginal code itself.

**What this settles about P2, and what it does not.** P2's prediction was ~6 allocations/row from
var-len column materialization. The AC2 differential already establishes the more important half:
**the per-row owned materialization (`Value::into_owned`, 397 cyc/row) is on the SHARED path**, so
whatever its allocation count, **it cancels in the differential and cannot close the ratio**. The
Flight-only allocation is the per-batch Arrow buffer and `Vec<Option<T>>` work, which is amortized
over the batch rather than per-row. So P2 as worded predicts a quantity on the wrong side of the
split — established without needing the count.

An exact allocations/row figure remains worth having for the absolute-throughput question (which is
#3231's and #3288's concern), and is proposed as a follow-up.

---

## Closure note — the mechanism behind #3096's two ZEROS

#3096 reported two Arrow-encode levers "measuring at ZERO". This work can now say **why**, from
per-function data rather than from inference, which is what the synthesis asked to have recorded.

**Lever 4 — IPC framing / `with_max_flight_data_size`: zero because there is nothing to remove.**
The framing path is `arrow_flight::encode::split_batch_for_grpc_response` driven by
`GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES = 2 MiB`. At that size the operation is one size summation plus a
single zero-copy slice. This profile corroborates it directly: the entire gRPC/h2 framing presence in
the Flight arm's profile is **`h2::proto::connection::Connection` at 18 cyc/row (0.07%)** — the
framing thread barely appears at all. A lever cannot recover 313 ns/row from a region measuring
0.07% of the arm. (#3096's own 313.0 ns/row framing figure came from an **in-process** sub-phase
timer with no gRPC transport, which is a different quantity — see `ac4-reconciliation.md` §4.)

**Lever 6 — per-batch schema construction: zero because it is amortized to nothing.**
`build_arrow_schema` (`arrow_convert.rs:122`) is called once per batch, and it does not appear as a
distinct symbol anywhere in three reps of the Flight profile — it is below the 0.02% reporting floor,
i.e. under ~5 cyc/row. At batch sizes in the hundreds, a per-batch cost is divided by hundreds before
it reaches a per-row figure.

**The generalisable point, which is this issue's subject.** Both zeros were *correct measurements of
regions that were already negligible*. Neither was an instrument failure. What #3096 lacked was the
ability to know that **before** funding the levers — the region was one undifferentiated 1,432.9
ns/row complement, so "array build" was equally consistent with "framing dominates" and with
"framing is 0.07%". **Per-function attribution is what distinguishes a lever that fails from a lever
that was never worth funding**, and that distinction is the whole return on this issue.
