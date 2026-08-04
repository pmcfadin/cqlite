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
