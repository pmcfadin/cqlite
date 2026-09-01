# WS0 #3445 — bounding the INLINED VInt-decode share of scan on-CPU

**Verdict: KILL** under the pre-registered definition. VInt decode is **1.70% of bare-scan
on-CPU** (annotate route, mean of 6 warm pinned reps, 1.66-1.80%), against a **pre-registered
3% fund/kill threshold**. No VInt-targeted lever may be filed citing this issue.

**Read the denominator before quoting the number.** "Share of scan on-CPU" is measured against
**all** cycles in the window, libc and kernel included, which is what the issue's wording says
and is the basis the verdict is taken on. **42.8% of those cycles are outside the measured
binary** and are structurally unreachable by the attribution, so on an **in-binary basis the
same numerator is 2.98%** — 0.02 pp under the same cliff. Both figures are reported throughout;
the pre-registered basis is primary, and the choice between them is the single largest
judgement call in this report (§2.4). It is **not mine to make**: the owner is being asked to
rule on which denominator the pre-registered 3% governs.

Epic #2817. Sibling of #3248 (encode side, same method). Measurement only: **no production
code changed by this PR.**

| | value |
|---|---|
| **AC1 — Route 1 (PRIMARY), `perf annotate` / DWARF inline chain** | **1.7027%** of total on-CPU (1.6621-1.7960, sd 0.052, n=6) · **2.9779%** in-binary (2.9262-3.1259) |
| AC1 — Route 1, wide boundary (decoder + nom adapters) | 1.7378% total-basis |
| **AC1 — Route 2 (CROSS-CHECK), `#[inline(never)]` probe** | **2.2624%** total-basis (2.1541-2.4583, sd 0.120) · 3.9763% in-binary |
| **AC1 — quantified disagreement** | **+0.56 pp / 1.329x** on the total basis; **+1.00 pp / 1.335x** in-binary — the RATIO is stable across bases, so the disagreement is a property of the probe, not of the denominator |
| **AC2 — stall attribution** | **NOT CONFIRMED — at the scan average.** Like-for-like (both on the in-binary basis) stalls/cycles = **1.061x**. Neither concentrated nor anti-concentrated |
| **AC3 — verdict vs pre-registered 3%** | **KILL** on the pre-registered total basis (1.70%, margin 1.30 pp). **MARGINAL** on an in-binary basis (2.98% mean, reps 2.93-3.13 straddle the line; probe route 3.98%) |
| AC4 — mission doc | **untouched**; it contains zero VInt claims (`grep -c -i vint` = 0), so no published number moves |

Corpus: the pinned #3096 `ws0.events` — 4,000,000 rows / 40,000 partitions / 12 cells per
row / 693.69 B per row, Data.db `sha256 4a903f6f…ae269`, regenerated from seed on this host
and re-hashed 8/8 on disk by an independent tool (`ws0-3445-artifacts/ac0/`).

---

## 1. What the issue asked, and what was actually in the way

#3027 measured a named VInt symbol at **0.74%** of on-CPU and said so as a **floor**, not a
share: `decode_unsigned` is `#[inline]`, so its cycles are booked to callers. #3027's decode
bucket named `parse_row_data_with_offset_impl` at **9.64%**, and the issue's framing was that
the structural cost of VInts — the serial dependency where field N+1's offset depends on
decoding field N — "would show up as ILP stalls inside that 9.6%".

Two things had to be established before any number could be believed.

**The inlining is real.** In the measured binary, `nm | grep -c decode_unsigned` = **0**. The
decoder has no symbol at all; it is inlined at every hot call site. So the blind spot #3027
reported was genuine, not an artifact of its tooling.

**Function-level attribution cannot see through it, and neither can source lines alone.** The
decoder's own `leading_ones()` call inlines `<u8>::leading_ones` from core, whose instructions
carry an innermost source line of `core/src/num/uint_macros.rs:201`. A source-line filter on
`parser/vint.rs` would silently drop those instructions as "core, not vint". The mechanism
used here reads the **full DWARF inline chain** at each sampled address and asks whether
`decode_unsigned`/`decode_signed` appears anywhere in it — the compiler's own record of which
source functions an instruction came from, which gets that case right by construction.

## 2. AC1 — the bounded number, both routes, and their disagreement

### Route 1 (PRIMARY): `perf annotate` cycle-level attribution

Six warm, pinned 40 s reps on the bare scan (`Database::execute_streaming`, the #3299 worker
reused unchanged), sampling `cycles` at a fixed period, classified by inline chain.

| rep | narrow (decoder proper) | wide (+ nom adapters) |
|---|--:|--:|
| `ac1-perfprof-1` | 1.6621% | 1.6941% |
| `ac1-perfprof-2` | 1.6695% | 1.7002% |
| `ac1-perfprof-3` | 1.7960% | 1.8352% |
| `ab-annotate-1` | 1.6686% | 1.7064% |
| `ab-annotate-2` | 1.6875% | 1.7282% |
| `ab-annotate-3` | 1.7326% | 1.7627% |
| **mean** | **1.7027%** | **1.7378%** |

Two boundaries are reported because "the vint share" is genuinely ambiguous, and the
ambiguity is worth more than a single number: **narrow** is the decoder itself; **wide** adds
the `parse_vuint`/`parse_vint` nom adapters a call site pays to reach it. They differ by
0.035 pp, so the ambiguity does not matter to the verdict.

Write-side `storage::serialization::vint` measured **0.0000%** in every rep. That is a
check, not an assumption — a non-trivial write-side share would have meant the harness was
measuring the wrong thing.

**Region identification was corroborated two independent ways, as AC1 requires.** The
disassembly fingerprint of the known #1638 J4 codegen (`leading_ones` → `not`/`bsr`/`xor
$0x7`; `u64::from_be_bytes` → `bswap`) locates 138 anchors in the binary; **133 fall inside a
DWARF-derived vint range**. The 5 that do not are not a disagreement — they are *other*
decoders carrying the same idiom (`bti::parser::rows::read_unsigned_vint_from_slice`,
`parser::repair_metadata::try_skip_improved_min_max`), which DWARF correctly declines to call
`parser::vint::decode_unsigned`.

### Route 2 (CROSS-CHECK ONLY): the `#[inline(never)]` probe build

`#[inline(never)]` on `decode_unsigned`/`decode_signed` makes the symbol visible (`nm`: 0 → 2).
Six reps: **2.1541 / 2.1575 / 2.2349 / 2.2166 / 2.4583 / 2.3530%**, mean **2.2624%**.

**The stated caveat, which is why this is a cross-check and not the answer:** the attribute
adds call/ret and argument marshalling and blocks optimisation across the boundary, so the
probe measures a decoder that is *not the one that ships*. Its number is expected to be high,
and is. **The annotate route is primary.**

### The disagreement, quantified

| comparison | annotate | probe | delta | ratio |
|---|--:|--:|--:|--:|
| pooled (n=6 each) | 1.7027% | 2.2624% | +0.5597 pp | **1.329×** |
| matched-co-tenancy A/B pairs (n=3 each) | 1.6962% | 2.3426% | +0.6464 pp | **1.381×** |

The two arms were originally taken under very different box load (annotate at loadavg 1.0–5.3,
probe at 14.8–18.3 with up to 30 peer processes), which cannot separate the attribute's effect
from the box's. Three **interleaved** pairs were added, each taking both arms back to back. The
confound turned out to be pushing the *other* way — matched pairs widen the gap to 1.381× —
so the disagreement is a property of the inline attribute, not of the machine.

**Both routes agree on the only thing that matters here: the share is under 3%.** The probe's
structurally inflated ceiling (2.46% at its worst rep) still does not reach the threshold.

### 2.4 The denominator — the largest judgement call in this report

Stated explicitly because it moves the headline by 1.75x and was disclosed nowhere in the first
version of this report.

`vint_share.py` can only attribute a sample it can ask DWARF about, which means a sample **in
the measured binary**. Every other sample — libc `malloc`/`free`/`memcpy`, the kernel — is
counted in the denominator and is unreachable by the numerator:

| | mean |
|---|--:|
| cycles outside the measured binary (cycles reps) | **42.83%** |
| cycles outside the measured binary (stall reps) | **56.47%** |
| in-binary cycles with NO DWARF inline chain | **0.0000%** |

So there are two defensible denominators, and they answer different questions:

| basis | annotate route | what it means |
|---|--:|---|
| **total on-CPU** (all DSOs) — *the pre-registered basis* | **1.7027%** | of everything the scan does on-CPU |
| **in-binary** | **2.9779%** | of the cycles the attribution can actually see |

The issue's wording is "share of scan on-CPU", so the total basis is primary and the verdict is
taken on it. Neither is wrong; quoting one without naming it is.

**Both bases UNDERCOUNT VInt, and the direction is knowable even where the size is not.** §6
records that the multi-byte path issues a dynamic-length `call memcpy@GLIBC`, and 44.4% of
decodes take it. Those callee cycles execute **in libc**, so they are off-binary: attributed to
neither basis' numerator, and to the total basis' denominator. Bounding it from the profile:

* Cycles at unsymbolised libc addresses total **12.69%** of scan on-CPU. These split by region
  into a memcpy/memmove family (`__nss_database_lookup + 0x19xxx`, glibc's signature for the
  ifunc-resolved SIMD implementations) at **8.89%**, and a malloc-region family
  (`__default_morecore + 0x8xx`) at **3.78%**.
* **8.89% is therefore a hard CEILING** on VInt's hidden memcpy cost — and a very loose one,
  because a scan copies row and cell bytes on many paths and VInt's copies are 1-8 bytes each.
* The exact split is **UNMEASURED**. Getting it needs call-graph attribution of libc frames to
  their callers, which this lane did not run (the box could not be quiesced, §7), and #3248's
  precedent treats call-graph runs as structural evidence only. It is stated as unmeasured
  rather than estimated.

The undercount cannot change the verdict on the pre-registered basis: adding the *entire*
8.89% ceiling to 1.70% gives 10.6% — but that figure is meaningless, since almost all of that
memcpy is other callers' work. What it does mean is that **1.70% is a floor, not a ceiling**,
in exactly the way #3027's 0.74% was, one level down. This is the residual this issue reduced
rather than eliminated.

Blocker note: the no-DWARF-chain undercount, which could in principle have been large, is
**measured at 0.0000% of in-binary cycles** — every sampled in-binary address resolved to an
inline chain. The ~12% of unsymbolised addresses visible in the profile are all *libc*, i.e.
already off-binary and already counted above. `vint_share.py` now refuses above a stated bound
rather than folding such addresses silently into "not VInt".

### The pipeline was validated against an independent instrument

On the probe binary, `decode_unsigned` is out-of-line, so `perf` can attribute it with no
inline-chain reasoning at all. The two methods agree to within **0.01 pp** on all three reps
(1: 2.1541 vs 2.15; 2: 2.1575 vs 2.16; 3: 2.2349 vs 2.23). That is the strongest evidence here
that the DWARF pipeline is measuring what it claims.

## 3. #3027's hypothesis has two halves; one is falsified and one is not confirmed

#3027 supposed (a) that the hidden VInt cycles were inside `parse_row_data_with_offset_impl`'s
9.64%, and (b) that the serial dependency "would show up as ILP stalls inside that 9.6%".
**(a) is falsified by direct measurement. (b) is NOT confirmed and NOT falsified** — VInt
stalls at the scan average (§4), and the instrument here cannot resolve a few-percent effect.
Only (a) is settled below, and it is a caller-attribution fact that does not depend on any
denominator choice.

| host function | share of vint cycles | of scan on-CPU | #3027's figure for that symbol |
|---|--:|--:|--:|
| `parse_row_metadata` | 51.95% | 0.86% | 0.87% |
| `read_vint_length_prefixed_bytes` | 45.63% | 0.76% | 0.74% |
| `parse_clustering_prefix` | 2.42% | 0.04% | — |
| **`parse_row_data_with_offset_impl`** | **~0%** | **~0%** | **9.64%** |

The inlined VInt cycles are **not** inside the 9.6% function. They are inside the two *small*
functions #3027 had already named at 0.87% and 0.74%. The inlining blind spot was real, and it
was worth about **2.2× the 0.74% floor** — not the order of magnitude the blind spot left room
for.

Both `%of scan on-CPU` columns above are on the pre-registered **total** basis, so they are
directly comparable with #3027's own figures, which were taken the same way.

**No published number moves.** #3027's measurements (0.74%, 9.64%) remain correct; what is
falsified is half (a) of an inference in its narrative about where the hidden cycles must be.
Per AC4 this report does not edit that document, and the mission doc contains no VInt claim at
all.

## 4. AC2 — the stall attribution statement

**Answer: NOT CONFIRMED. The VInt decode chain stalls at the scan average — 1.061x — so the
serial dependency is neither visible as concentrated stall cycles nor measurably absent.**

### This section previously said the opposite, and the correction is the important part

The first version of this report claimed the dependency was "measurably ANTI-concentrated" at
**0.806x**, i.e. that VInt was *less* stall-bound than the average scan cycle. **That was
wrong, and it was wrong for a reason worth recording:** it divided two shares whose
**denominators differ**.

| | vint share | out-of-binary cycles in those reps |
|---|--:|--:|
| cycles reps | 1.7027% of total | **42.83%** |
| stall reps | 1.3732% of total | **56.47%** |

Off-binary work (malloc, libc, kernel) stalls *more* than the measured binary does, so the
stall reps carry a larger unreachable denominator. Dividing `1.3732 / 1.7027` therefore
measured the difference between two denominators and reported it as a property of VInt. On a
like-for-like basis:

| basis | cycle share | stall share | ratio |
|---|--:|--:|--:|
| in-binary (correct — same denominator both sides) | 2.9779% | 3.1601% | **1.061x** |
| total (WRONG — different denominators) | 1.7027% | 1.3732% | 0.806x |

**1.061x is parity.** VInt stalls in proportion to the cycles it consumes.

This is the same failure shape as the two PIE-rebase errors in §7 — a complete, confident,
wrong table — with one difference that matters: the rebase self-check *caught* those, and
nothing caught this, because a self-check guards the mechanism it wraps and not the arithmetic
a human later builds on its output. The `denominator_note` field now shipped in every result
JSON, and the separate `*_in_binary` figures, exist so the mistake is not available to make.

### What is measurable, and what is not

| quantity | value |
|---|--:|
| vint share of cycles, in-binary | 2.9779% |
| vint share of `cycle_activity.stalls_total`, in-binary | 3.1601% (2.7865-3.6172, n=3) |
| **ratio** | **1.061x** |
| whole-scan IPC | 2.41 |
| whole-scan `stalls_total` / cycles | 28.98% |
| whole-scan `stalls_l1d_miss` / cycles | 7.30% |
| whole-scan `int_misc.recovery_cycles` / cycles | 2.14% |

All three counting reps report **100.00% `pct_running`** on all six events, so none of this is
a multiplexing artifact.

Instruction granularity is consistent with parity: within the region the stall distribution
tracks the cycle distribution (`bswap` 70.5% of vint stalls against 72.7% of vint cycles,
ratio 0.97). The larger per-opcode ratios (`not` 5.1x, `test` 7.3x, `bsr` 5.5x) sit on opcodes
carrying 0.26%, 0.12% and 0.06% of vint cycles and are noise, recorded rather than quoted.

**The bound on this answer.** This host exposes no `topdown.slots` and no PEBS, so AC2 cannot
be answered in Topdown's frontend/backend vocabulary and sample IPs are not precise.
`cycle_activity.stalls_total` counts cycles in which no uops are delivered, so a dependency
chain whose latency is *hidden* by surrounding independent work never registers as a stall.
The defensible claim is therefore the one above — **at parity, dependency not confirmed** — and
**not** that no dependency latency exists. Separating hidden latency from absent latency needs
precise events or Topdown slots, and this host has neither: that part is **unmeasurable here**,
and is reported as unmeasurable rather than as zero. Given the ratio is 1.06x and the rep
spread on the stall side is wide (2.79-3.62%, sd 0.42), a real effect of a few percent would
sit inside this instrument's noise, which is the honest resolution limit.

## 5. AC3 — the pre-registered verdict

> If VInt share on the annotate route is < 3% of scan on-CPU, the verdict is **KILL** and no
> VInt lever may be filed citing this issue.

Annotate route, on the basis the threshold was registered against ("share of scan on-CPU", so
all cycles in the window): **1.7027%**. **1.30 pp below the threshold. The verdict is KILL.**

**It is robust to every choice inside the method EXCEPT the denominator**, and an earlier
version of this report wrongly claimed it depended on no judgement call at all. Sensitivity,
worst case in each row:

| variation | worst figure | verdict |
|---|--:|---|
| widest single annotate rep | 1.7960% | KILL |
| wide boundary (decoder + nom adapters), worst rep | 1.8352% | KILL |
| skid band, worst *narrow* edge | 1.8369% | KILL |
| skid band, worst *wide* edge (most permissive within the basis) | 1.8880% | KILL |
| cross-check probe, known to inflate | 2.4583% | KILL |
| no-DWARF-chain cycles (measured undercount, §2.4) | 0.0000% | no effect |
| **in-binary denominator, mean** | **2.9779%** | **KILL by 0.02 pp** |
| **in-binary denominator, worst rep** | **3.1259%** | **ABOVE the line** |
| **in-binary denominator, probe route** | **3.9763%** | **ABOVE the line** |

So: **on the pre-registered basis the verdict is KILL with a 1.30 pp margin and nothing in the
method threatens it. On an in-binary basis it is MARGINAL** — the mean sits 0.02 pp under the
cliff, individual reps cross it, and the probe route is well over. Which denominator the
pre-registered 3% governs is a question about the pre-registration, **not a measurement
question, and not mine to settle**; it is with the owner. Both numbers are stated so the ruling
can be applied without re-measuring.

One thing the in-binary reading does *not* do is make a lever attractive: even at 2.98% the
entire cost of VInt decoding would have to be removed, and §6 shows 55.6% of decodes never
touch the expensive path.

## 6. What a future lever would have faced, recorded so the KILL is informative

Not a recommendation — the verdict above forbids one. Recorded because a KILL that explains
*where the cost would have been* is worth more than a bare number.

* One `bswap` (`u64::from_be_bytes`) carries **72.7%** of all vint decode cycles.
* That instruction is only on the multi-byte path, and the measured width distribution is
  **55.6% single-byte / 44.4% multi-byte** (5 bytes and wider: effectively absent). So the
  concentration is where the work is, not an artifact of sample skid.
* The multi-byte path issues a **dynamic-length `call memcpy@GLIBC`** for
  `be[8-extra..].copy_from_slice(...)`, and the `bswap` consumes a store-to-load forward from
  that staging buffer. #1638 J4's claim of "no per-byte index loop" holds; the loop became a
  libc call with a runtime length.
* Ceiling: >= 11.7 VInt decodes per row, total cost 1.70% of scan on-CPU. A change making
  multi-byte decoding *free* could not recover more than that, and 55.6% of decodes never
  touch the path at all.

## 7. Method, capabilities, and what was refused

Full detail in `ws0-3445-artifacts/`. The four things that most affect how much weight these
numbers carry:

**Capability preconditions were OBSERVED, not assumed** (`raw/counter-capability-census.md`).
`perf_event_paranoid = -1`, `kptr_restrict = 0` re-confirmed at lane start. Rust v0 demangling
verified firing on *this* binary — perf 6.17.13 still reports `libbfd: OFF`, whose natural
inference is wrong, and #3217 lost a 50.57 s bucket to exactly that silent failure.

**No PEBS on this host** (`cycles:pp` is `<not supported>`). Attribution is therefore
non-precise, which matters for a region ~8–20 instructions wide. Handled by measurement: every
share is recomputed with samples re-attributed K instructions earlier for K in 0..3, and the
spread is reported as a band (about ±0.05 pp per rep). It is a sensitivity band, not an error
bar.

**The rebase self-check is load-bearing and caught two real errors before publication.** A
`PERF_RECORD_MMAP2` record carries a mapping's *file offset*, not its segment vaddr (this
binary's executable LOAD has `p_offset` 0x102be0 vs `p_vaddr` 0x103be0), and selecting a LOAD
segment by offset containment alone picks the read-only segment, which also spans that offset.
Both errors resolved to real symbols and real line numbers — a complete, confident, **wrong**
table, the silent-instrument shape. The check (nm and perf must agree which symbol each
rebased address lies in) refused both, and passes at 0 mismatches on every published rep.

**Codegen fidelity was measured, not asserted.** The line-attribution build (`debug = 1`) grows
`.text` by 11,584 bytes (**+0.262%**), so it is *not* codegen-identical and cannot be assumed
harmless. Measured against the codegen-faithful build over every symbol >= 1%: max |delta|
**0.17 pp**, with `parse_row_data_with_offset_impl` identical at 9.31% in both.

**Quiescence: every published rep was taken on a CONTENDED box, and that is disclosed rather
than argued around.** This box is shared; a peer lane's `--lite` gate had it at loadavg
15.8/22.8 during this work, and the gate semaphore does not protect a perf run (it serialises
gate against gate; a perf run holds no slot). Applied strictly to the >= 2-3 loadavg bar,
**none of the 18 published reps clears it**. Per-rep before/after loads are in
`raw/validity-and-refusals.md`. Whether that can move either verdict was then **measured**
(`raw/load-sensitivity.txt`), using the fact that the published set spans a 5x load range:

* **AC3 is not load-sensitive.** Slope of cycle share vs peak load is **-0.0009 pp per unit**
  (slightly negative — the share is a ratio on a pinned core, and contention slows numerator
  and denominator together). Total spread over the 5x range is **0.1338 pp**; extrapolated to
  idle the share is **1.7087%** against a measured mean of 1.7027%. Flipping KILL -> FUND needs
  **1.2973 pp — 10x the entire observed load-induced variation.**
* **AC2 survives the contamination that would break it.** Contention inflates stalls and so
  pushes vint's stall share *up* toward its cycle share — the exact way a contended rep could
  manufacture "the dependency IS visible" — and the data shows that direction (**+0.0042 pp per
  unit load**). It still does not reach the conclusion's boundary: the most contended stall rep
  gives ratio **0.899x**, the mean 0.806x, and the idle extrapolation **0.785x**. Quiescence
  would make this finding *stronger*, not weaker. Had it come out the other way, the number
  would have had to be withheld.

**The quiescence check is PROSPECTIVE and the published reps were not validated by it** — this
is declared as its own heading in `raw/validity-and-refusals.md`. No load data was captured
*during* any published rep; the per-rep `loadavg` figures are endpoint pairs, which is the read
the harness header itself calls insufficient. On that endpoint data every published rep would be
refused by the new bound. What they *were* validated by: `pct_running == 100.00%`, warm, pinned
with kernel read-back, 0 lost samples, >= 3 reps, and interleaved A/B for the route comparison.

Confirmatory quiet reps were attempted and **not obtained**: the box went to loadavg 67-80 with
43 concurrent `agent-gate` processes and a peer's `/tmp` cleanup removed the waiting job. None
is reported, and **no published number was re-rolled** — `harness/record-scan.sh` now samples
load across each window, refuses on the maximum, and never retries.

**Refusals** (`raw/validity-and-refusals.md`) — recorded as observations, never dropped: two
10 s development smoke reps (one analysed with the wrong rebase, which is what caused the
self-check to be written); one width-probe run that emitted **no histogram at all**, recorded
as a failed observation rather than as "no multi-byte VInts found", because an absent
measurement is not a zero. The width probe's *timing* and the `inline(never)` build are both
excluded by design from the headline.

**Production source is unchanged.** Both probes were applied in a detached throwaway
`git worktree` and ship as patches (`ac1/inline-never-probe.patch`,
`ac1/vint-width-probe.patch`) so the cross-checks are reproducible without a measurement-only
flag in `cqlite-core` — which, per #1699/#3522, would be a feature nothing executes.

## 8. Reproducing

```bash
# 1. corpus (regenerates the pinned #3096 corpus bit-identically from seed)
cargo build --release -p ws0-corpus-gen
./target/release/ws0-corpus-gen --out /data/ws0-3096 --seed 30960001 \
  --rows 4000000 --rows-per-partition 100 \
  --verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json

# 2. the measured binary: release codegen + symbols + line tables
cd docs/reports/ws0-3299-artifacts/harness/scan-worker
CARGO_TARGET_DIR=/tmp/b CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none \
  cargo build --release --locked

# 3. one warm pinned rep, then the share
bash docs/reports/ws0-3445-artifacts/harness/record-scan.sh \
  --out /tmp/rep1 --binary /tmp/b/release/ws0-3299-scan-worker --secs 40 --settle 6 --cpu 2
python3 docs/reports/ws0-3445-artifacts/harness/vint_share.py \
  --perf-data /tmp/rep1/perf.data --binary /tmp/b/release/ws0-3299-scan-worker
```
