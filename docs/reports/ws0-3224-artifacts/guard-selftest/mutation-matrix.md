# #3224 — mutation evidence that each of the six guards is load-bearing

`selftest-guards.sh` passing 37/37 shows the guards behave correctly on the inputs
it supplies. It does **not** by itself show that the guards are what produce that
result — a test can pass for reasons unrelated to the code it means to pin. So each
fix was **reverted in place** and the selftest re-run. The requirement the owner set
was *"show the guard rejecting the bad input it now catches"*; a mutation run shows
the complement, which is what makes it evidence rather than assertion: **with the
guard removed, the bad input is accepted.**

Run on `ip-172-31-3-252` (i4i.metal), 2026-08-04, against the artefacts committed in
PR #3286. Every mutation was reverted immediately afterwards and the 37/37 PASS
re-confirmed; `guard-selftest/selftest-output.txt` is the final unmutated run.

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
