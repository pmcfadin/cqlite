# FINDING — the WS0 Flight arm's `bytes_total` is allocated ARRAY MEMORY, not payload or wire bytes

Found while sourcing a mean row width for probe D3, and it is a near-miss: this figure was one step
away from being used as the **bytes-touched-per-row differential** the owner added as a deliverable.

## The observation that started it

Every WS0 Flight record carries `bytes_total` and `bytes_per_s`. On this corpus:

```
rows_total 12,000,000   bytes_total 151,941,683,808   bytes_per_s 2,451,278,640
```

That is **12,661.8 bytes/row**, against a corpus of **693.69 bytes/row on disk** — an **18.25x
expansion**, and `2.45 GB/s` presented as a byte rate. Per-row framing overhead cannot produce 18x,
so either the Flight path was inflating payload enormously (a major finding) or the metric measures
something else.

## The cause, read from source rather than inferred

`tools/flight-loadgen/src/client.rs:89`:

```rust
bytes = bytes.saturating_add(batch.get_array_memory_size() as u64);
```

Arrow's own definition of that method (`arrow-array-53.4.1/src/array/mod.rs:336-339`):

> Returns the total number of bytes of memory occupied **physically** by this array. This value will
> **always be greater** than returned by `get_buffer_memory_size()` and **includes the overhead of
> the data structures that contain the pointers to the various buffers.**

So `bytes_total` is the **client-side, decoded, in-memory Arrow representation's allocated size,
including Rust data-structure overhead** — not bytes on the wire, and not even Arrow buffer payload
(that would be the *smaller* `get_buffer_memory_size`).

The 18.25x follows without any performance anomaly: this corpus flushes small batches, so per-array
`ArrayData`/`Buffer`/`Arc` overhead is amortized over few rows, and 12 columns multiply it.

## What is and is not affected

**NOT affected — no published figure is contaminated:**

* `rows_per_s` and `cycles_per_row` do not derive from it. AC0's divergence, AC1's attribution and
  AC2's differential are all untouched.
* The rig's **content-volume self-consistency check** stays valid: it compares the untimed preflight
  against the timed requests using **the same metric on both sides**, so a systematic mislabelling
  cancels. It was never claiming to be an absolute.

**Affected — the metric's NAME and the report's DESCRIPTION of it:**

The generated report calls this quantity "the **ARROW PAYLOAD VOLUME** of each flight rep" and "the
timed response's Arrow payload volume". Both are wrong, on two counts: it is memory **occupancy**,
not payload, and it includes data-structure overhead that no payload contains. A reader taking
`bytes_per_s = 2.45 GB/s` as loopback throughput, or `12,661` as bytes/row, is wrong by roughly 18x
on this corpus — and by a *different, batch-size-dependent* factor on any other.

## Why this is the issue's own theme, one level out

This is the **adjacency hazard** in the rig's reporting: a number that is a faithful measurement of
one quantity, labelled as another, with nothing in the artifact objecting. It is not a fabricated
value, not an unmeasured counter, and not a scaled estimate — every existing guard passes it,
because every existing guard asks whether the number was *observed*, and this one was. What no guard
asks is whether it measures **what its name says**.

**And it nearly propagated.** The owner's added deliverable is a bytes-touched-per-row differential,
explicitly to serve #3288's LLC-footprint target. `bytes_total` is the obvious source for it, sits
right beside `rows_total` in every record, and is already named "bytes". Using it would have produced
a differential that was internally consistent, plausible, reproducible — and about allocated client
memory rather than bytes touched. The bytes-touched differential is therefore taken from **hardware
counters** (`l2_lines_in.all x 64 B`) and not from this field.

## Remediation, proposed not actioned

Renaming a field in a committed record schema is a rig-wide change and this issue should not smuggle
one in under a profiling deliverable. Proposed as a follow-up:

1. **Rename** `bytes_total`/`bytes_per_s` to `array_memory_total`/`array_memory_per_s`, or keep the
   name and change the accumulator to `get_buffer_memory_size()` — a decision about what the rig
   wants to measure, which belongs to whoever owns the loadgen.
2. **Correct the report's prose**, which currently asserts "Arrow payload volume".
3. If a genuine **wire** figure is wanted, it must be measured at the transport, not derived from a
   decoded batch — the decoded form has no fixed relationship to the encoded one.
