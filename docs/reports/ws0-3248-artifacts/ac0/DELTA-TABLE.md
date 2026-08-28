# AC0 — reproduction on the hardened rig: the invariant layer DIVERGES, arm-specifically

Run `EXIT=0`, 8m47s, 2026-08-28 16:15:27Z-16:24:14Z. Defaults: 3 reps, warm+cold, arm `bypass`,
events `cycles,instructions`, `target/release`, server `taskset -c 2,10`, client
`4,12,5,13,6,14,7,15` — the same configuration #3096 recorded.

Quiescence: **certified on 48 attributable sampler samples**, zero competing processes across the
whole window, competing census zero at both boundaries. See `quiescence-verdict.json`.

Host identity recorded (`host-identity.json`) — instance `i-04ac0a860eef7f241`, `boot_id
bb94898a-...`, microcode `0x2b000661`. The #3096 record has no host identity at all, so whether
this is cross-session or cross-instance is **undecidable from that record**; that is a gap in the
inherited method, not a property of this run.

## The two layers, reported separately as ruled

### Layer 1 — the INVARIANT layer, where a reproduction claim can stand

| quantity | #3096 | AC0 | delta | #3096's own spread |
|---|--:|--:|--:|--:|
| **warm bare-scan IPC** | 1.4524 | 1.4408 | **-0.80%** | 0.31% |
| **warm Flight IPC** | 1.5228 | 1.3601 | **-10.68%** | 0.74% |
| warm bare-scan cycles/row | 18,813.9 | 19,600.4 | +4.18% | 1.54% |
| warm Flight cycles/row | 23,511.3 | 26,307.4 | **+11.89%** | 4.84% |
| warm ratio bare/flight | 1.4862x | 1.6853x | **+13.40%** | same-session |
| warm cycles/row delta | +4,697 | +6,707 | **+42.78%** | same-session |
| **cold bare-scan IPC** | 1.4623 | 1.4510 | **-0.77%** | 0.58% |
| **cold Flight IPC** | 1.4810 | 1.3374 | **-9.70%** | 1.72% |
| cold bare-scan cycles/row | 17,939.2 | 18,080.7 | +0.79% | 0.96% |
| cold Flight cycles/row | 24,411.6 | 27,230.7 | **+11.55%** | 3.38% |
| cold ratio bare/flight | 0.9839x | 1.0043x | **+2.07%** | same-session |
| cold cycles/row delta | +6,472 | +9,150 | **+41.37%** | same-session |

**The finding: the bare scan reproduces; the Flight arm does not, and the gap between them has
grown.**

* **The bare scan reproduces well.** Cold rows/s **+1.4%**, cold cycles/row **+0.8%**, and both
  temperatures' IPC within **0.8%** — inside its own recorded spread on every axis.
* **The Flight arm is systematically more expensive per row**: cycles/row **+11.9% warm / +11.5%
  cold**, against recorded spreads of 4.84% / 3.38%.
* **Flight IPC fell ~10% while the bare scan's did not.** This is the sharpest signal in the table.
  IPC was the *tightest* recorded quantity — spreads of 0.31% to 1.72% — so a 9.7-10.7% move is
  **6x to 34x its own spread**.
* **Both arms were measured in the SAME session**, so no common-mode drift can move Flight IPC by
  ~10% while leaving bare-scan IPC at -0.8%. **This divergence is arm-specific and cannot be drift.**
* Consequently the **ratio** moved +13.4% (warm) and the **cycles/row delta grew +42.8%**, from
  +4,697 to **+6,707 cycles/row**.

### Layer 2 — the ABSOLUTE layer, outside cross-session resolving power

| quantity | #3096 | AC0 | delta | own spread |
|---|--:|--:|--:|--:|
| warm bare scan rows/s | 370,134 | 347,987 | -6.0% | 2.08% |
| warm Flight do_get rows/s | 249,041 | 206,480 | -17.1% | 7.94% |
| cold bare scan rows/s | 194,638 | 197,410 | +1.4% | 1.99% |
| cold Flight do_get rows/s | 197,832 | 196,573 | -0.6% | 0.51% |

**Neither reproduced nor diverged, as ruled.** The warm Flight rows/s carries a 7.94% within-session
spread against ~10% documented cross-session drift on an untouched path, so it has no resolving power
across sessions; the cold figures happen to land within 1.5% but that is not evidence either, at this
resolution.

**This is not adjusting a rig toward recorded numbers. It is declining to over-read an instrument
whose own documentation states its limit.** The one absolute worth flagging is warm Flight rows/s at
**-17.1%**, which exceeds the ~10% drift band — but it is exactly the figure with the widest spread,
and the invariant layer already carries the same signal with far better resolution.

## What the divergence is consistent with, stated as a prediction and not a conclusion

More cycles per row **and lower IPC** means more *stalled* work per row, not merely more work: the
Flight arm is retiring fewer instructions per cycle than it did. That is the signature of added
pointer-chasing or hashing, not of added arithmetic.

This lane's **pre-registered** leading Flight-marginal hypothesis fits it exactly:
`estimate_arrow_row_bytes` (`cqlite-core/src/export/arrow_size.rs:251`) performs
`row.values.get(col.name.as_str())` once per column per row — **12 sip-hash probes per row** on this
12-column corpus — which is structurally the cost `transpose_columns` (#1495) was written to delete,
reintroduced by the byte cap. A per-row hash probe would raise cycles/row, depress IPC, and leave the
bare scan untouched. **All three are observed.**

That is a *consistency*, not a demonstration. The AC1 per-function profile is what can confirm or kill
it, and it is registered here before that profile is taken.

## Consequence for AC4, which must be stated

**AC4's subject quantity has itself moved.** The reconciliation in `ac4-reconciliation.md` analyses the
*recorded* `+4,697 cycles/row` against the *recorded* `1,746 ns/row`, and that analysis stands as an
analysis of the record. But this session measures **+6,707 cycles/row**, and the `1,746` was measured
on #3096's binary, not this one. The current wall-clock gap is
`1/206,480 - 1/347,987 = 1,969 ns/row` (versus 1,313.7 ns/row then).

So the region must be **re-measured on this binary** before it can be reconciled against this gap;
mixing today's gap with #3096's region would be precisely the cross-session comparison this document
declines to make elsewhere.

