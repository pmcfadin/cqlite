## Equivalence control — #3299 worker vs the rig's `ws0-scan-bench`

Same physical core, same session, same bytes.

| arm | rows/s | note |
|---|--:|---|
| `ws0-scan-bench --passes 3` (median pass) | 361,779 | the #3096/#3272 rig's bare-scan arm |
| — its individual passes | 366,638, 361,779, 358,983 | own spread **2.1%** |
| `ws0-3299-scan-worker` S=1, aligned window | 356,763 | this harness |

**Delta: -1.39%.** Decomposition:

- attribution shortfall (a known LOW bias of this harness, see harness README): **+0.0639%** of it;
- the bench's own three passes span **2.1%** within one run, and the worker's figure sits at the bottom of that range — consistent with the worker measuring continuous steady state while a 3-pass median is weighted toward the earliest, fastest pass;
- residual after the shortfall: **-1.32%**, against the bench's own measured pass-to-pass spread of **2.1%**.

**VERDICT: EQUIVALENT.** |residual| **1.32%** is within that spread, so this run is not evidence of a different code path.

This is a DECIDED comparison, not a remark: `derive.py --equivalence` exits non-zero with `GUARD-FAIL EQUIV_DIVERGENCE` when the residual exceeds the bench's own spread. A divergence LARGE against it, in either direction, would mean the two are not the same code path and the S=1 point would not be comparable to the existing rig's.

