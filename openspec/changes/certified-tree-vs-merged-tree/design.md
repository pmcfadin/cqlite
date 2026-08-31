# Design: base-staleness advisory (slice 1 of issue #3650)

Decisions that bind the implementation. Every one is a choice with a rejected alternative, because the
issue's own warning is that a mechanism here can be *satisfied and wrong*.

## D1 — Blast radius = path intersection ∪ a hard-coded gate-global list

**Decided by measurement, against the case that produced the issue** (derivation:
`docs/round-artifacts/issue-3650-blast-radius-measurements.md`).

- *Rejected: path intersection alone.* On PR #3362 the culprit commit `5e08db201` and the PR's diff share
  **no path**, so its `M = 0` branch calls a certification fresh exactly when it is not. Unsound in the
  malign direction.
- *Rejected: any churn behind the base.* 107 of 107 commits — this is the shape the owner's ruling exists
  to refuse, and it forces re-gate loops on a repo where `main` moves 12× in 4 hours.
- **Adopted: intersection ∪ gate-global.** **28 of 107 (26%)** as shipped, leaving 74% of churn
  non-staling. Catches the motivating case, and names it (`matched 5e08db201 gate-global
  .config/nextest.toml`), so the detection is attributable and not a coincidence on a count. *An
  earlier draft of this decision said 22 of 107 (21%); that was a hand measurement using root-only
  `Cargo.*` and excluding the diff's own paths, and it was stale within one implementation round —
  the delta is attributed row-by-row in the measurements artifact. The script reports its own count
  and is the authority for it.*

The gate-global list is **content that can change ANY gate's verdict regardless of the diff**:

```
.config/nextest.toml   rust-toolchain.toml   Cargo.toml   Cargo.lock
scripts/agent-gate.sh  scripts/ci/**   cqlite-core/tests/support/**
test-data/**           .github/workflows/**
```

It is **one list in one place, hard-coded in the script, with no env override**. Rationale is #3312's
second rule: an override is settable by the party it constrains, and *"which paths stale my certification"*
is precisely what a lane wanting to skip a re-gate would widen. One visible location keeps it inside a diff
a reviewer already reads.

## D2 — The verdict vocabulary is chosen so the advisory CANNOT be read as a certification

AC5 says the advisory must not itself become a false certification. That is a property of its **words**,
not of its intent, because this repo's failure mode is someone grepping a token.

- **No `PASS`, no `OK`, no `RESULT:`** anywhere in its output — those three tokens are the verdict
  vocabulary of `AGENT-GATE *SUMMARY`, `ROBOREV REVIEW SUMMARY` and `PREMERGE:`. Its prefix is
  `BASE-STALENESS:`, distinct from all of them.
- The no-finding verdict is **`NO-STALENESS-RECOGNISED`**, never `FRESH` and never `CLEAN`. It names a
  *scan result*, not a *state of the world* — the distinction the whole issue turns on.
- `M = 0` prints **`0 RECOGNISED`**, never a bare `0`. Precedent: `cfg-gated-subtree gaps: N RECOGNISED`
  (CLAUDE.md), for the identical reason — a bare zero in a log reads as a verified all-clear from a scan
  documented as incomplete.
- Every run prints its own **`NON-EXHAUSTIVE`** lines. Not a footnote in docs; in the output, every time,
  because the output is what gets pasted.

## D3 — Exit codes give slice 2 a machine-readable signal, and UNMEASURED is fail-closed BY CONTRACT

| exit | meaning |
|---|---|
| 0 | `NO-STALENESS-RECOGNISED` — scan completed, affirmatively measured, nothing recognised |
| 4 | `STALE-RECOGNISED` — at least one commit behind the base touches the blast radius |
| 5 | `UNMEASURED` — the scan could not be performed (missing ref, no merge-base, git failure) |
| 3 | usage error |

**The contract, written now so slice 2 cannot walk into the fail-open hole:** a consumer MUST treat
**`5`/`UNMEASURED` as STALE**, never as fresh. This is CLAUDE.md's standing rule — *never derive a pass
from the absence of a bad signal; where the sole oracle could not be consulted the verdict is
non-passing*. It is stated in the script header, in the spec, and asserted by a test, because the shape
that keeps recurring in this repo is a multi-state signal whose unmeasured state inherits the permissive
branch.

Distinct codes `4`/`5` are used precisely because `premerge-assert.sh` already owns `0`/`2`/`3`; reusing
one would make the advisory indistinguishable from its caller's verdicts.

## D4 — The base is the MERGE-BASE, never the base ref's tip

`N` = `git rev-list --count $(git merge-base origin/main HEAD)..origin/main`.

#3392 is the recorded cost of getting this wrong: an assert that expected the base ref's *tip* FAILed
deterministically on every correct review of a branch whose `main` had advanced, and was misdiagnosed as a
race **twice**. Same trap, same shape, so the same resolution — and the script **prints the merge-base it
used**, so the two can never be confused in a pasted block.

## D5 — The advisory does not fetch, and says which `origin/main` it measured

A verifier with a side effect is a worse verifier. The script reads `origin/main` as it finds it and
**prints that sha and its commit date**, so a reader can see whether the measurement itself is stale. A
missing `origin/main` is `UNMEASURED` (exit 5), never a silent zero. Doctrine tells the caller to fetch
first — `premerge-assert`'s existing guidance already says `git fetch && git rebase origin/main` before
every gate.

## D6 — Slice 1 changes NO verdict, and the #3465 disclaimer is RETAINED

`premerge-assert.sh` gains `PREMERGE: ADVISORY` lines and nothing else: it cannot fail on the advisory, and
an advisory that errors or is absent is reported, not fatal. The three existing `PREMERGE: SCOPE` lines
naming #3650 **stay** — slice 1 does not close the gap they disclose, and removing them would be the
overclaim the lead named. They are extended by one line pointing at the advisory.

`scripts/tests/test_premerge_assert.sh` Case 39 pins the literal `PREMERGE: SCOPE`, `#3650` and
`does NOT prove` on both success paths; the new lines are additive so that case keeps holding, and it is
extended rather than replaced.

## D7 — Home: a sourced helper next to `premerge-assert.sh`, following the roborev model

`scripts/flow/base-staleness.sh` is a **standalone executable** (so it is usable by hand for triage, which
is the standing fleet rule this mechanizes) that `premerge-assert.sh` invokes. Resolved from the caller's
own directory with **no env override** — #3312's enforcer rule, and it costs nothing here.

*Rejected: inlining into `premerge-assert.sh`.* It is 691 lines already, and the triage use ("is the fix
for this red already on main and merely absent from my base?") wants a command a human can run on its own.

## D8 — Mutation check, per #3465's precedent

`scripts/tests/test_base_staleness.sh`, wired into the existing **`tooling-tests`** component (which
already runs `test_premerge_assert.sh`). Beyond ordinary cases it carries:

1. **The motivating case as a pinned regression fixture** — a synthetic repo reproducing PR #3362's shape
   (a diff sharing no path with a commit behind that touches `.config/nextest.toml`), asserting
   `STALE-RECOGNISED`. **It reds if the gate-global set is removed**, which is the mutation that matters.
2. **A planted-mutant case** following `scripts/tests/test_ws0_perf_invocation_lint.sh:812-830`: copy the
   script, plant the narrow-definition defect, assert the suite catches it — so the guard is shown to fire
   rather than assumed to.
3. **A vocabulary case**: the output contains none of `PASS`, `OK`, `RESULT:` in any run, and `M = 0`
   never appears as a bare `0`. This is the AC5 property, tested directly rather than reasoned about.
4. **An `UNMEASURED`-is-not-`0` case**: a git failure yields exit 5, never exit 0.

## D9 — Cost

Git plumbing: one `rev-list`, then one `show --name-only` per commit behind. Measured on the 107-commit
case at **≈1.5 s** warm. It is **not** added to `--lite` and adds nothing to the full gate's critical path
beyond `tooling-tests`' own suite. A pathological N is bounded by reporting and continuing, never by
silently truncating the scan (a truncated scan would be an `UNMEASURED`, per D3).

## D10 — This change CAN certify itself, unlike its neighbours

`premerge-assert.sh` and `base-staleness.sh` are read from the **checkout**, not from a PR's base ref, so
the demonstration can run on this PR. Stated because the neighbouring class cannot — `required` evaluates
the aggregator and registry from the PR's **base** ref (#2910), and roborev reads `exclude_patterns` from
the **root** checkout at daemon start (#3229) — and three lanes were caught by that class on the night
this issue was filed. Not a claim that every part is self-demonstrating: the *enforcement* it feeds is
slice 2's, and slice 2's own demonstration is planned there.
