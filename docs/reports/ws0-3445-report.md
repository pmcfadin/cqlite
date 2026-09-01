# WS0 #3445 — bounding the INLINED VInt-decode share of scan on-CPU

**Verdict: KILL.** VInt decode is **1.70% of bare-scan on-CPU** (annotate route, mean of 6
warm pinned reps, range 1.66–1.80%), against a **pre-registered 3% fund/kill threshold**.
No VInt-targeted lever may be filed citing this issue.

Epic #2817. Sibling of #3248 (encode side, same method). Measurement only: **no production
code changed by this PR.**

| | value |
|---|---|
| **AC1 — Route 1 (PRIMARY), `perf annotate` / DWARF inline chain** | **1.7027%** mean (1.6621–1.7960, stdev 0.052, n=6) |
| AC1 — Route 1, wide boundary (decoder + nom adapters) | 1.7378% mean (1.6941–1.8352) |
| **AC1 — Route 2 (CROSS-CHECK), `#[inline(never)]` probe** | **2.2624%** mean (2.1541–2.4583, stdev 0.120, n=6) |
| **AC1 — quantified disagreement** | **+0.56 pp pooled / +0.65 pp matched-co-tenancy; probe reads 1.33–1.38× HIGH** |
| **AC2 — stall attribution** | **Visible and measurably ANTI-concentrated**: 1.3732% of stalls vs 1.7027% of cycles = **0.806× the scan average stall density** |
| **AC3 — verdict vs pre-registered 3%** | **KILL**, by 1.30 pp |
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

### The pipeline was validated against an independent instrument

On the probe binary, `decode_unsigned` is out-of-line, so `perf` can attribute it with no
inline-chain reasoning at all. The two methods agree to within **0.01 pp** on all three reps
(1: 2.1541 vs 2.15; 2: 2.1575 vs 2.16; 3: 2.2349 vs 2.23). That is the strongest evidence here
that the DWARF pipeline is measuring what it claims.

## 3. #3027's hypothesis about WHERE the cycles were is falsified

This is the substantive correction, and it is the opposite shape from what #3027 expected.

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

**No published number moves.** #3027's measurements (0.74%, 9.64%) remain correct; what is
falsified is an inference in its narrative about where the hidden cycles must be. Per AC4 this
report does not edit that document, and the mission doc contains no VInt claim at all.

## 4. AC2 — the stall attribution statement

**Answer: the serial-dependency cost is NOT visible as concentrated stall cycles. It is
measurably anti-concentrated, and this is a measured negative rather than an unmeasurable.**

This host exposes no `topdown.slots` and `perf stat -M TopdownL1` is unavailable, so AC2 cannot
be answered in Topdown's frontend/backend vocabulary. It is answered instead by **sampling on
the stall event itself** (`cycle_activity.stalls_total`, which IS available and IS samplable)
and attributing stalls through the *same* inline chain as cycles — which makes vint's share of
stalls directly comparable to its share of cycles.

| quantity | mean | 
|---|--:|
| vint share of **cycles** | 1.7027% |
| vint share of **stalls** | 1.3732% (1.3652 / 1.2229 / 1.5315) |
| **relative stall density** | **0.806×** the scan average |

Whole-scan context, all three counting reps at **100.00% `pct_running`** on six events: IPC
**2.41**, `stalls_total` 28.98% of cycles, `stalls_l1d_miss` 7.30%, `int_misc.recovery_cycles`
2.14%. This is not a stall-dominated workload, and the VInt chain is *less* stall-bound than
its average cycle.

Instruction granularity agrees. Within the region the stall distribution tracks the cycle
distribution — `bswap` carries 70.5% of vint stalls against 72.7% of vint cycles (ratio 0.97)
— so no instruction in the decode is disproportionately stall-bound. The larger ratios in the
per-opcode table (`not` 5.1×, `test` 7.3×, `bsr` 5.5×) sit on opcodes carrying 0.26%, 0.12%
and 0.06% of vint cycles; they are noise and are recorded as such rather than quoted.

**The bound on this answer, stated because it is real.** `cycle_activity.stalls_total` counts
cycles in which no uops are delivered. A dependency chain in a short region surrounded by
independent work can have its latency *hidden* by that work and never register as a stall. So
the defensible claim is the one made above — at both whole-scan and instruction granularity the
dependency is not visible as excess stall cycles — and **not** the stronger claim that no
latency exists. Separating hidden latency from absent latency needs precise events or Topdown
slots, and **this host has neither**; that part is **unmeasurable here**, and is reported as
unmeasurable rather than as zero.

## 5. AC3 — the pre-registered verdict

> If VInt share on the annotate route is < 3% of scan on-CPU, the verdict is **KILL** and no
> VInt lever may be filed citing this issue.

Annotate route: **1.7027%**. **1.30 pp below the threshold. The verdict is KILL.**

It does not depend on any judgement call in the method:

* widest single annotate rep: 1.7960% — KILL
* wide boundary (decoder + nom adapters), worst rep: 1.8352% — KILL
* skid band, worst edge across all reps: ~1.89% — KILL
* even the cross-check probe, which is *known* to inflate: 2.2624% mean, 2.4583% worst — KILL

For a VInt lever to be worth funding it would have to remove more than the entire measured
cost of VInt decoding, twice over.

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
