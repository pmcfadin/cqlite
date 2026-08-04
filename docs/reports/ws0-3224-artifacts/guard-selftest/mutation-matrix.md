# #3224 — mutation evidence that every guard is load-bearing

`selftest-guards.sh` passing **71/71** shows the guards behave correctly on the inputs
it supplies. It does **not** by itself show that the guards are what produce that
result — a test can pass for reasons unrelated to the code it means to pin. So each
fix was **reverted in place** and the selftest re-run. The requirement the owner set
was *"show the guard rejecting the bad input it now catches"*; a mutation run shows
the complement, which is what makes it evidence rather than assertion: **with the
guard removed, the bad input is accepted.**

Three review rounds produced **18 findings** (6, then 7, then 5) and this file records a
mutation run for each fix. Run on `ip-172-31-3-252` (i4i.metal), 2026-08-04, against the
artefacts committed in PR #3286. Every mutation was reverted immediately afterwards and
the PASS re-confirmed; `guard-selftest/selftest-output.txt` is the final unmutated run.

**Two of the mutations found defects in the TESTS rather than the code** (round 2's #7 and
round 3's #1) — both source-text assertions passing against a mutant whose branch was
unreachable. Those are written up below rather than quietly repaired, because they are the
same defect class as the findings themselves and the second one arrived one round *after*
the first was documented.

## Round 1 — the original six

| # | mutation applied | cases that flipped to FAIL | result |
|---|---|--:|---|
| ① | re-invert `evaluate()` so the 2× movement gate runs before the `LLC-load-misses` miss-rate branch | 1 | **caught** — healthy host verdict became `UNRELIABLE_NO_MOVEMENT`, reproducing the original false-FAIL exactly |
| ② a | raise `INSTR_PER_ACCESS_MAX` to ∞ (drop the absolute ceiling) | 1 | **caught** — the uniformly-inflated sweep passed, because the cross-row check derives its reference from the same contaminated data |
| ② b | raise `UNIFORMITY_TOL_PCT` to ∞ (drop the cross-row check) | 1 | **caught** — the single +3.2% row (the real `LLC_8M` value) passed, being under the ceiling |
| ③ | remove the multiplexing verdict, leaving the printed warning | 3 | **caught** — multiplexed hostile arm, multiplexed friendly arm, and multiplexed `LLC-load-misses` all read `OK` again |
| ④ a | make `ws0_guard_all_rc_zero` skip arms matching `loadgen*` | 4 | **caught** — both load-generator arms accepted, the unrecordable-rc case accepted, and the printed roster no longer named them |
| ④ b | replace the enumerated `meta.json` rc gate with a hardcoded four-arm roster | 1 | **caught** — the extracted-source test rejected the roster that omits the load-generator arms |
| ⑤ | delete `rep-complete.py`'s rc and counter-file checks (i.e. restore the old three-condition predicate) | 4 | **caught** — the failed-loadgen rep, the empty CSV, the absent CSV and the multiplexed row were all certified "complete and safe to skip" again |
| ⑥ | `sys.exit(0)` before the terminal verdict block | 2 | **caught** — INDETERMINATE and UNAVAILABLE byte accounting both reported success again |

## Round 2 — seven further findings, same method

The round-1 fixes were themselves reviewed, and the round found seven more fail-opens of
the same class — **three of them in code the round-1 fixes had just introduced**, which is
the pattern CLAUDE.md records for #3229. Same mutation treatment:

| # | mutation applied | cases that flipped | result |
|---|---|--:|---|
| 1 | remove `derive.py`'s `assert_rc_all_zero` calls | 2 | **caught** — a rep with `rc.loadgen_interior=1` and a rep with a failed group-C arm both derived cleanly |
| 2 | remove `derive.py`'s group-C all-or-nothing refusal | 1 | **caught** — group C for 1 of 3 reps derived the headline from a single undispersed rep |
| 3 | neuter `ac5-analyse.py`'s IMC roster assertion | 2 | **caught** — both the truncated capture and the near-zero-rows capture resolved successfully |
| 4 | neuter `ac5-analyse.py`'s duplicate-row check | 1 | **caught** — the duplicate row was silently overwritten again |
| 5 | revert the `MUX_UNREADABLE` marker in `verdict-logic.sh` | 2 | **caught** — an unreadable enabled% on either arm read `OK` |
| 6 | make `rep-complete.py`'s roster/event-set differences empty | 3 | **caught** — partial `rc` block, partial `perf_files`, and a CSV truncated to 2 of 7 events all certified complete |
| 7 | change the `--quick` branch condition to `elif false` | 2 | **caught — but only after the test was rewritten; see below** |
| 8 | disable `penalty-probe.sh`'s row-parser `problems` exit | 1 | **caught** — a `cycles` counter at 43% enabled was published as a latency |

### The mutation run found a defect in the TEST, and this is the part worth keeping

**Mutation 7 initially passed 59/59 with the fix reverted.** The `--quick` case had been
written as a source-text match: it asserted that the string `QUICK_MECHANICS` appears in
the final-verdict block and appears before `RESULT=PASS`. Against a mutant whose branch
condition was changed to `elif false`, every one of those assertions still held — the text
was all present, and the branch was simply unreachable.

**That is precisely the defect class these fourteen findings are made of**: a presence test
standing in for a behaviour test, passing because nothing bad was found rather than because
something good was measured. It was written *while fixing seven instances of that exact
shape*, which is worth recording plainly rather than quietly repairing — the shape is not
something one stops being susceptible to by understanding it.

It is now a behaviour test: the committed decision block is **extracted and executed** with
injected state, asserting the `RESULT`/`RC` pair it actually produces, across four
combinations. Re-run against the same mutant, it fails 2 cases.

**The generalisable rule, since it applied to two cases here:** where a guard cannot be
invoked directly (it lives inside a heredoc, or deep in a script needing perf and root),
extract the committed source **text** and **run it**. Do not assert on the text. The
`capture-endpoint.sh` metadata gate and this `--quick` verdict are both tested that way, and
in both cases testing a transcription instead would have proved only that the transcription
works — the porting lesson CLAUDE.md records from #3229.

### One honest limitation of mutation 8

Only 1 of the 5 row-parser cases flips under mutation 8. The other four (`<not counted>`
cycles, zero cycles, absent `LLC-load-misses`) still fail with the `problems` exit disabled,
because the mutant then hits a `TypeError` or a later explicit refusal downstream. They do
pin the property that matters — the parser refuses the input rather than publishing a
number — but they do not specifically pin the `problems` list. Recorded rather than
presented as five independent demonstrations.

## The two results worth reading twice

**②a and ②b are the interesting pair, and they are why there are two checks rather
than one.** Each mutation is caught by *exactly one* case, and neither case is
caught by the other check. That measures the complementarity the guard's docstring
claims rather than asserting it:

* the **absolute ceiling** is the only thing that refuses a sweep in which *every*
  row is inflated — the cross-row check computes its reference from the sweep
  itself, so a uniform error is invisible to it. This is CLAUDE.md's *"a positive
  verdict requires an affirmative measurement"*: a check whose baseline comes from
  the data it is checking can pass vacuously.
* the **cross-row uniformity check** is the only thing that refuses a row
  contaminated by +3.2%, which is not a hypothetical — it is the committed
  `LLC_8M` row's actual value, comfortably under any defensible ceiling.

Drop either and a real, measured contamination shape ships unnoticed.

**④a's fourth flipped case was not one of the three the finding named.** Removing
the loadgen arms from the roster also broke the *unrecordable-rc* case, because the
mutation's `continue` skipped the `checked` accumulation as well — so the guard
silently lost its "no arms at all" subject test. Worth recording because it is the
shape of the original defect one level down: a roster that quietly covers a subset
is indistinguishable from one that covers everything, and the only defence is
printing what was actually checked.

## Round 3 — five further findings, and the structural fix that should end the pattern

| # | mutation applied | cases that flipped | result |
|---|---|--:|---|
| 1 | capture straight into `$out` again, no swap | 2 | **caught** — the invalid rep was overwritten in place by a broken capture |
| 2 | make the schema's rc-roster difference empty | 2 | **caught** — a partial `rc` block passed in `rep-complete.py` AND `derive.py`, from one edit |
| 3 | track uncore completeness as a flat event set again | 3 | **caught** — all 24 events on S0 with no S1 rows certified complete |
| 4 | change the group-C cross-endpoint check to a constant false | 1 | **caught** — group C at S=1 only derived silently via the modelled fallback |
| 5 | remove the AC4 basis / `AC4_UNAVAILABLE` marker | 2 | **caught** — `ac4_accounting` emitted with neither an attribution nor a residual |

### Why round 3 existed at all, which matters more than the five fixes

Findings came in at **6, then 7, then 5** — not converging. Looking at *which* defects
arrived rather than how many, the same fact kept being wrong in a different file:

| the fact | round 1 | round 2 | round 3 |
|---|---|---|---|
| the rc arm roster | `capture-endpoint.sh` omitted two arms | `rep-complete.py` checked "nonempty", not the roster | `derive.py` enumerated the dict, not the roster |
| the uncore roster | asserted in `derive.py` | missing in `ac5-analyse.py` | `rep-complete.py` tracked event names globally, not `(socket, event)` pairs |

**Four consumers, two facts, fixed six times, wrong in a new way each time.** Fixing each
report where it pointed could not converge, because the defect was not in any of those
files — it was that **the schema had no single home**, so every consumer re-derived it and
each re-derivation was an independent chance to get it wrong.

`harness/ws0schema.py` is that home. Mutation 2 is the evidence it works: **one** edit to
the schema flips cases in **two** different consumers, which is exactly the coupling that
was missing. A roster correction now lands everywhere at once, and two consumers cannot
disagree about a question they both ask the same code.

### And the round-2 test lesson repeated itself immediately

The `run-all.sh` atomicity case was first written as a source-text assertion again —
grep for `staging`, check the swap comes after its gate — one round after the mutation
matrix recorded that exact mistake. It was rewritten to **execute** the real loop against
a stub `capture-endpoint.sh` and assert the resulting directory state.

Doing so found a second defect in the case: the pre-existing rep it seeded was a **valid**
one, so `run-all.sh` correctly SKIPPED it and the capture never ran — three cases
"passing" while exercising nothing. The scenario the finding describes is *recapturing an
invalid rep*, so the fixture now seeds an invalid one. **Knowing about a failure shape does
not confer immunity to it**; only running the thing does.

## Round 4 — four findings; the trend and what it says

| # | mutation applied | cases that flipped | result |
|---|---|--:|---|
| 1 | take the busy fraction back out of `occupancy()`'s `ok` | 1 | **caught** — a half-idle arm passed every validity gate again |
| 2 | replace `derive.py`'s schema call with an empty problem list | 2 | **caught** — both the partial IMC set and the missing+duplicate pair derived cleanly |
| 3 | change the `MIN_REPS` refusal back to a warning | 1 | **caught** (via the opt-out marker, see note) |
| 4 | accept `--iters <= 0` in `cache-hostile.c` again | 3 | **caught** — 0, -1 and an unparseable `ten` all ran and exited 0 |

**Note on mutation 3.** The case that flips is the *opt-out* one, not the refusal one:
with the primary check disabled the group-C `MIN_REPS` refusal still fires on the same
tree, so "1 of 3 reps → REFUSED" still passes for a different reason. The mutation is
caught, but by the marker rather than the refusal. Recorded because the distinction is
the whole point of running these.

**Finding 2 is the single-homing paying off a second time.** The count-based check in
`derive.py` was the last independent restatement of the uncore roster; routing it through
`ws0schema.validate_counter_file()` is what made *identity* checking automatic. The
finding it closes could not have been caught by any count: **one missing event plus one
duplicated event on the same socket yields exactly the expected row count**, and the
selftest asserts that property of its own fixture so the case cannot pass for the wrong
reason.

### The trend, and why it is worth stating rather than just stopping

Findings per round: **6 → 7 → 5 → 4**. The count is falling, but slowly, and the honest
reading is not "nearly clean" — it is that this harness had a *systemic* fail-open habit
and each round exposes a thinner layer of it. Three things are true at once:

1. **The class never changed.** All 22 findings are one defect: a measurement step whose
   failure was recorded, printed, or structurally representable but not *acted on*. Not
   one was a logic error in an actual computation.
2. **Later rounds reach further from the headline.** Round 1 hit the penalty probe that
   feeds a published figure; round 4 hit `--iters 0` and a busy fraction that no committed
   artefact violates. That is what convergence looks like here — the defects are still
   real, and they are no longer near the numbers.
3. **Three of the 22 were introduced by earlier fixes.** That rate did not fall by being
   careful; it fell when the schema got a single home, which removed the *opportunity*.
   The generalisation worth carrying: where the same fact is restated in N consumers,
   expect N chances to get it wrong, and fix the restatement rather than the instances.

## Round 5 — four findings, and a THIRD test defect found the same way

| # | mutation applied | cases that flipped | result |
|---|---|--:|---|
| 1 | divide the rounded milli-rates again in `compute_missrate` | 2 | **caught** — a 0.05% friendly rate against a LOWER hostile rate read `inf` → `OK` |
| 2 | remove the occupancy roster completeness check | 2 | **caught** — a block omitting `uncore` certified in `rep-complete.py` AND `derive.py`, from one edit |
| 3 | remove the read-time busy-fraction re-check | 1 | **caught** — a recorded 0.42 with a stale `ok=true` certified itself |
| 4 | revert `do_stalls`'s XOR refusal | 2 | **caught — only after the cases were rewritten; see below** |
| 5 | take the busy floor back out of `capture-stalls.sh`'s `ok` | 1 | **caught** — the arm feeding the headline attribution was ungated again |
| 6 | hardcode the floor in `common.sh` (0.75) | 1 | **caught** — two homes for one threshold |

**Finding #1 is the only one of all 22 that is a genuine arithmetic defect** rather than an
unacted-upon failure signal, and it is worth its own note. `compute_missrate` divided two
integer *milli*-rates, so a friendly miss rate below 0.1% truncated to zero and the rise
became `inf` — which `evaluate` reads as an unconditional OK. A counter whose hostile rate
was **equal or lower** therefore passed P4: the exact flat-counter case the gate exists to
catch, waved through by a rounding step three lines earlier. Two rounding steps in the same
function, one of which decided a verdict.

**Mutations 2 and 6 are the single-homing paying off twice more.** One edit to the schema
flips cases in two consumers; one edit to `common.sh` is caught by a case that compares it
against the schema. Six of the 22 findings were "the same fact restated in another file",
and that shape is now structurally hard to reintroduce rather than merely discouraged.

### The XOR cases passed 97/97 against their own mutant — the third instance

Reverting `do_stalls`'s XOR refusal changed nothing, because both fixtures still exited
non-zero: one through a downstream `event absent from CSV` error, the other through the
asymmetric-endpoint guard. **Defence in depth, which is good — and which made an
exit-code assertion worthless as evidence for this particular guard.**

It also mistook what the finding asked for. A half-present group C refused as "event
absent from CSV" tells the operator nothing actionable; refused as a **failed capture**
tells them to re-run `capture-stalls.sh`. Absent-versus-failed have *opposite* remedies,
so **the diagnosis is the deliverable**, not the exit code. The cases now assert on the
diagnosis text and fail 2/2 against the mutant.

**Three test defects across five rounds, all the same shape** (round 2 #7, round 3 #1, round
5 #4): an assertion that passes for a reason other than the guard under test. Each was found
by mutation and by nothing else. The rule that came out of the first two — *extract the
source and run it, never assert on it* — was necessary but not sufficient; this one adds:
**assert on the thing the finding actually asks for.** A guard's exit code and a guard's
diagnosis are different deliverables, and where the remedy depends on which failure
occurred, the diagnosis is the one that matters.

## Round 6 — four findings

| # | mutation applied | cases that flipped | result |
|---|---|--:|---|
| 1 | `validate_occupancy` trusts the recorded `ok` again | 7 | **caught** — `ok=true` beside zero rows, errors, unavailable requests, zero successes, zero duration and a partial scan all certified |
| 2 | drop the finite/non-negative/0–100 checks on counter rows | 5 | **caught** — `nan`/`inf`/negative values and `nan`/`10000` percentages all passed |
| 3 | drop the analyser-side stream-record validation | 3 of 5 | **caught** (the other 2 fail downstream — defence in depth again) |
| 4 | remove the pre-start occupied-port refusal | 1 | **caught** |
| 5 | remove the readiness pid re-check | 1 | **caught** |

**Finding #2 is the permissive-branch trap in its purest form,** and worth quoting because
CLAUDE.md names the shape and this is the cleanest instance of it in the repo:
`float('nan') < 99.0` is **False**. The multiplexing floor was written as *"is it below
the threshold"*, and NaN is not below anything, so a NaN enabled percentage passed the
check designed to catch exactly untrustworthy percentages. The fix keys the branch on the
affirmative property — finite **and** within 0–100 — rather than on the absence of the bad
one.

**Finding #1 is "re-check rather than trust" applied to the function that was added to
re-check.** `validate_occupancy` (round 5) consulted `v.get('ok')` and independently
re-derived only the busy fraction — so an artefact carrying `ok=true` beside zero rows was
accepted by the very validator whose purpose was not to take the capture's word for it. The
recorded flag is now compared *against* the re-derived answer, and a disagreement is
reported as a stale verdict in its own right.

### And a fourth test defect, caught before commit rather than by mutation

The first version of the re-derivation demanded `target_concurrency > 0`. That field is
**not** in the occupancy block — it lives in `steps` — so every one of this PR's six
committed reps immediately read `target_concurrency=None (must be > 0)`. **A false FAIL on
correct input: round 1 finding #1's failure mode wearing a completeness fix's clothes.**

Caught by running the selftest against the committed artefacts before committing, which is
why the "every committed rep still certifies" case exists at all — it is the counterweight
to every tightening, and the fifth round in a row where it earned its place. The rule this
adds: **a re-derivation may only demand fields the capture actually records**, and the
recorded set belongs in a comment next to the check so the next tightening does not guess.
Concurrency is not lost, incidentally — a computable `busy_fraction_estimate` already
establishes that concurrency and duration were both non-zero at write time.
