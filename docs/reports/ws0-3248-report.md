# WS0 #3248 — attributing the 82% unattributed encode region

**This issue bought a PROFILE, not a patch.** It set out to open a 1,432.9 ns/row complement that
#3096 could only label "array build" from a call graph, and to separate — for the first time —
which of the Flight arm's cost is **shared** with the bare scan and which is **Flight-marginal**.

Both are done. The headline results, then where each acceptance criterion landed.

Full artifacts: `ws0-3248-artifacts/`. Method and its epistemic rules:
[`measurement-method.md`](ws0-3248-artifacts/measurement-method.md). Whether the committed
`results.json` artifacts re-derive, measured per artifact — including the one thing that does **not**
re-derive and the manifest edit I deliberately refused:
[`raw/artifact-reproduction.md`](ws0-3248-artifacts/raw/artifact-reproduction.md).

---

## The five results that matter

**1. Only ~57% of the Flight/bare-scan gap is Flight-only CODE.**
The gap is +6,707 cycles/row. Flight-marginal code accounts for **3,842**. A further ~24% is the
**same shared code running 21.5% more expensively** on the Flight arm (8,926 vs 7,348 cyc/row), and
~41% is allocator work. The issue anticipated the shared/marginal split; it did not anticipate that a
quarter of the gap would be identical code costing more.

**And "identical" has now been checked at the machine-code level, which sharpens the effect but
shrinks its scope.** The two arms are *different binaries* (`ws0-scan-bench` vs `cqlite-flight`) and
the shared bucket is assigned by *symbol presence*, so "identical code" was a claim about codegen
resting on evidence about names — different inlining per binary was a live competing explanation.
Measured by disassembling every shared symbol in both binaries and comparing operands with only the
relocatable parts normalized ([`ac1/codegen-identity.py`](ws0-3248-artifacts/ac1/codegen-identity.py)):
**136 of 363 (37%) are operand-identical**, and partitioning the excess by that fact gives:

| shared sub-bucket | scan | Flight | excess |
|---|---|---|---|
| SHARED total | 7,327 | 8,879 | **+21.2%** |
| **operand-identical** (136 syms, lower bound) | 1,133 | 2,008 | **+77.1%** |
| different machine code (227 syms, upper bound) | 5,897 | 6,865 | +16.4% |

**The excess is concentrated in provably identical code — +77.1% where the instructions, registers
and immediates all match.** But that bucket is only ~6–7% of self-time, so identical code accounts
for **+875 cyc/row of the +6,707 gap (~13%)**, not the ~24% the shared bucket as a whole represents.

**Two corrections to earlier versions of this paragraph, both found by review, because the oracle was
wrong twice.** A *byte* comparison is far too strict (only 15/363 match, since operands relocate
between binaries) and a *mnemonic* comparison is too loose (it reported 291/363, of which 155 are
not — 49 differ in a register or a real immediate). The figures above come from the third oracle.
Correspondingly, **the earlier claim that "different codegen is excluded as the explanation" is
withdrawn**: 227 of 363 shared symbols *do* differ in machine code and they carry the majority of
shared self-time, so their +16.4% excess may be partly codegen. What survives is narrower and still
the interesting result: for the subset where the code is provably the same, the Flight arm pays
**+77.1%**, which is a memory-system signature and is what the bytes-touched differential
independently measures. 136 is a lower bound, so a tighter oracle could only enlarge that subset.

**2. The shared-path lever inversion is now a measured number, not an argument.**
The issue's central warning was that a shared-path lever "could be the largest absolute throughput win
in the 0.17 program AND nearly worthless for AC1". Measured: removing the shared per-row `HashMap`
hashing (**1,409 cyc/row**) would gain **+7.3% on the bare scan and +5.2% on Flight** — **a larger
absolute win than any of the three funded-candidate levers** — while moving the ratio **the wrong
way**, 1.685x → ~1.71x.

**3. The Flight arm touches 5.19x the bytes for 1.2x the accesses.**
L2 traffic per row: bare scan **4,578 B**, Flight **23,745 B**. Retired loads/row differ by only
1.22x. That is a **locality** result, and it supplies the mechanism for both the arm-specific IPC
divergence in AC0 and the "same code costs more" finding above.

**4. AC0 does not reproduce, arm-specifically — and it cannot be drift.**
The bare scan reproduces (cold rows/s +1.4%, cycles/row +0.8%, IPC within 0.8%). The Flight arm does
not: **cycles/row +11.9% warm / +11.5% cold, IPC −10.7% / −9.7%** against recorded spreads of
0.74%/1.72%. Both arms were measured in the **same session**, so no common-mode drift can move one
arm's IPC by ~10% and leave the other's at −0.8%.

**5. The levers cannot reach the old target, and the profile says why.**
Priced individually: lever 3 **860 cyc/row**, lever 2 **≤668**, lever 1 **≤1,881**. Deleting **89% of
all Flight-marginal code** reaches ~1.53x. Per AC6 this report does **not** re-assert the 1.3x target;
the figure is stated only to show the priced levers cannot reach it, which is a finding about the
levers.

---

## Acceptance criteria

| AC | Verdict | Where |
|---|---|---|
| **AC0** — reproduce #3096's figures on the hardened rig; divergence is a finding | **MET — and it diverges.** Reported in two layers: invariant (claim-bearing) and absolute (outside cross-session resolving power) | [`ac0/DELTA-TABLE.md`](ws0-3248-artifacts/ac0/DELTA-TABLE.md) |
| **AC1** — per-function attribution, demangler verified | **MET.** First per-function data ever taken inside this region; demangler verified by positive control before anything was believed | [`ac1/AC1-AC2-AC3.md`](ws0-3248-artifacts/ac1/AC1-AC2-AC3.md) |
| **AC2** — differential separating shared from Flight-marginal, two numbers, never summed | **MET.** 7,348 vs 8,926 shared; **0 vs 3,842** marginal. Classification validated: arm B measures 0.00% marginal in all 3 reps | same |
| **AC3** — levers 2, 3, 1 priced with ratio AND absolute deltas | **MET**, and the divergence AC3 warns about is demonstrated on the shared-path case | same |
| **AC4** — reconcile 1,746 ns/row vs +4,697 cycles/row, or report the irreconcilability | **MET via the escape clause.** `1,746` is not a valid single-currency total: it sums two wall times measured on **concurrently-running threads**, which the source artifact itself says "do not sum" | [`ac4-reconciliation.md`](ws0-3248-artifacts/ac4-reconciliation.md) |
| **AC5** — rows/s AND cycles/row, never CPU-share | **MET.** Every profile share is converted to cycles/row; shares appear only as conversion provenance | throughout |
| **AC6** — do not re-assert 1.3x; a well-measured negative is satisfying | **HONOURED.** No target is re-asserted | — |
| added — bytes-touched/row differential at S=1 | **MET at the L2 boundary**; the LLC boundary is unavailable on this host | [`bytes-touched/`](ws0-3248-artifacts/bytes-touched/BYTES-TOUCHED.md) |
| added — probe **D5** | **MET, with both readings reported** because the verdict flips and the two components cannot be separated on a codegen-faithful binary | [`ac1/D5-dispatch-region.md`](ws0-3248-artifacts/ac1/D5-dispatch-region.md) |
| added — probes **D3**, **D4** | **NOT RUN, with reasons.** D3 has a batching confound; D4 needs a counting allocator. Both purposes partly discharged by other means | [`probe-dispositions.md`](ws0-3248-artifacts/probe-dispositions.md) |
| added — closure note on #3096's two zeros | **MET.** Both zeros were correct measurements of already-negligible regions | same |

### Pre-registered predictions

| | Outcome |
|---|---|
| **P1** — dispatch + downcasts + owned materialization = 0.8–1.0 µs/row | **Partly unfalsifiable as worded**: `field_builder::<T>` downcasts **do not exist** in this tree — structurally absent, not measured-at-zero |
| **P2** — allocations/row ≈ var-len column count | **Predicts a quantity on the wrong side of the split**: the per-row owned materialization is **shared**, so it cancels in the differential |
| **P3** — builder realloc count per batch > 2 | **Structurally inapplicable**: the scalar path uses no Arrow builder and no `finish()` |
| inherited — `rows_to_record_batch` will be inlined away | **FALSIFIED** — it survived as the **largest** Flight-marginal symbol (1,881 cyc/row) |
| this lane's own — `estimate_arrow_row_bytes` is the leading marginal cost | **PARTLY FALSIFIED** — real (592, family 860) but **3.2x smaller** than `rows_to_record_batch` |

---

## What this work found in the instruments, which was not the plan

The issue's premise was that a trustworthy profile needs trustworthy instruments. That turned out to
be the larger half of the delivery.

* **A `SyntaxError` on `main` made the whole rig unrunnable** — a backslash inside an f-string
  expression at two fatal steps. Filed as **#3451**. It had no coverage that *executed* those steps,
  because the self-tests stop above them by design.
* **No multiplexing guard existed.** The perf CSV parser never read perf's enabled-percentage column,
  so a scaled estimate would flow through as an ordinary integer. Added fail-closed.
* **A silent-zero counter class.** Five events program successfully, report `100.00%` enabled, and
  return a hard `0` — passing every guard including the one this issue added.
* **The Flight record's `bytes_total` is allocated array memory, not payload.** 18.25x the on-disk
  rate. Nothing published depends on it, but it is described in the report as "Arrow payload volume".
* **Four capabilities this virtualized host lacks**, now enumerated: LLC counters, LBR branch stacks,
  dwarf unwinding at this binary size, and the L3 events above.

New guards, all with their refusal paths observed firing: occupancy-enforced clock derivation (16
cases), box-quiescence gating (19), per-subcommand perf allowlists with a second sanctioned wrapper
(123).

**And a hazard named because it recurred five times in one session across two agents:** *asserting a
state from something adjacent to it rather than measuring the state.* Instances and the rule that
falls out are in [`measurement-method.md` §6](ws0-3248-artifacts/measurement-method.md). Three of the
five were this lane's own.

---

## What should happen next, and what should not

**Do not fund lever 1, 2 or 3 on absolute-throughput grounds.** Together they are ≤3,409 cyc/row of a
26,854 cyc/row arm. The largest absolute win available in this measurement is the **shared** row-build
path — and it moves the ratio adversely, so it belongs to a programme measured in box-level rows/s,
not to the ratio.

**The unexplored ~24%** — shared code costing more on the Flight arm — is the most interesting
unpriced quantity here, and it splits into two unequal parts. Restricted to the **136** shared symbols
that are **operand-identical**, the penalty is **+77.1%** (1,133 → 2,008 cyc/row): same instructions,
same registers, same immediates, nearly double the cycles. That is a memory-system signature, and the
bytes-touched differential points the same way. The remaining 227 symbols do differ in machine code
and carry more of the weight at a smaller +16.4%, where codegen and locality cannot be separated by
this measurement. Neither part is any of the three named levers.

**#3288 is partly unblocked and partly hardware-blocked.** It now has a per-row footprint (23,745 B
at the L2 boundary), but its "fit ~1/6 of 54 MiB LLC" target is an LLC-boundary constraint this host
cannot measure — that is **#3224**'s subject, and this is the **fourth** issue to pay for its absence.

**A well-measured "irreducible at this shape" was licensed by AC6 and is NOT what was found.** The
region is not irreducible; it is **mostly not where the levers were aimed**.
