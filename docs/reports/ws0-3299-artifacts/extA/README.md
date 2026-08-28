# Extension A — the first bracketing attempt; SUPERSEDED by extB for the verdict

**Partially valid, and superseded.** Grid `6:24,32,48,64`, incumbent interleaved.
Kept as the audit trail for how the verdict was reached, not as its source.

Three things it established, all of which survive:

1. **Single reps were not enough.** extA round 1 put N=32 at **+0.68%** above
   N=24; extB round 1 put it at **−1.95%**. The **sign flipped**, which is why
   the verdict waited for 3 reps — AC1's ≥3-rep requirement earning its keep.
2. **N≥48 is not a measurable configuration** under the 24 GiB containment cap:
   48 independent scan processes OOM-kill the scope. A property of this
   harness's N-independent-process design, **not** of CQLite or of production
   `do_get`.
3. **An in-band drift read**: its N=24 came in at 2,647,966, **−3.1%** against
   the main grid ~1.5 h earlier.

**`s6-n24-round1` is CONTAMINATED — do not use it.** Its 60 s window overlapped
an agent-gate run (`--lite` compiles), which is the failure mode this campaign
documents: a spoiled rep still exits 0 and still reports `100.00% pct_running`,
and the only tell is the clock — it read 3.337 against a clean 3.358 on the
neighbouring point. Prefer rounds 2–3 for anything read from this tree.

The final verdict is extension B's; see `../extB/README.md`.
