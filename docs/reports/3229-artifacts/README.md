# #3229 artifacts — the AC2 live probe, and the AC7 backfill ruling

| File | What it is |
|---|---|
| `live-probe-procedure.md` | The AC2 demonstration, as **prose** and as a **post-merge** step — with the reason it cannot be pre-merge, the expected summary values, and how to read the token line. |
| `../../../website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` | A deny-listed extension (`.json`) under a **nested** `docs/reports/*-artifacts/` directory — the end-to-end confirmation that a slash-containing pattern is ROOT-ANCHORED. It **discriminates**: `docs/reports/*-artifacts/**/*.json` matches this path under the incorrect `**/`-prefixed reading and NOT under the correct root-anchored one, so its survival is real evidence. It survives the current configuration but **not** a `**/`-prefixed misreading of it — which is the property that makes it a probe rather than a decoration. |

## Why there is no executable probe here any more

This directory originally carried an **executable** under `docs/` on purpose: that is the exact shape
`exclude_patterns = ['docs/**', '*.md']` made invisible to roborev, so the PR would have been a
#3222-shaped demonstration of its own fix.

It cannot be. roborev's daemon resolves `exclude_patterns` from the **repo root path**, not from the
worktree, and it **snapshots that config at start** — so the narrowed set does not apply to this PR's
own review at all. An executable under root `docs/` is therefore dropped from the review of its own
change for as long as the change is unmerged, and the wrapper FAILs on its absence from the prompt —
**correctly**. Measured at the time, under a since-removed pre-enqueue key that named the mechanism
directly (the key is gone; see below):

```
census-exclusion: FAIL (1/7 code census paths excluded:
  docs/reports/3229-artifacts/probe-census-exclusion.sh by 'docs/**' [root-config])
```

Today the same condition surfaces as `prompt-content: FAIL (1/7 code census paths absent from the
prompt)` — the same fail-closed refusal, with a cause that names the symptom rather than the mechanism,
because **nothing predicts roborev's exclusion set pre-enqueue any more (deferred to #3283)**.

A pre-merge self-demonstration was a **deadlock**, not a test — the specimen that proves the fix is
the specimen the unfixed configuration eats. The executable was removed; the procedure lives in
`live-probe-procedure.md` and is scheduled for after the merge. The requirement is rescheduled, not
dropped, and it carries a named trigger (board `In Review`, not `Done`, until the evidence is posted).

**The primary AC2 evidence is a real PR, not this probe.** The first post-merge PR that carries an
executable under `docs/` proves the fix on a diff nobody shaped for it, which is strictly better
evidence; the documented procedure is the fallback. See `live-probe-procedure.md`.

## The AC7 backfill ruling — ACCEPT AS-IS (owner, 2026-08-03)

The already-merged, never-reviewed harness code shipped under `docs/reports/*-artifacts/` by **#3026**,
**#3100** and **#3217** is **accepted as-is. No retroactive review pass.** Recorded here as well as in
`openspec/changes/narrow-roborev-docs-exclusion/design.md` (D7), because an unrecorded "we decided it was
fine" is indistinguishable from nobody having looked.

The reasoning — the part that matters, not just the verdict:

1. **The exposure is bounded by what the code is.** These are measurement harnesses: Part A/B drivers, an
   off-CPU classifier, a demangler, counter parsers, summarisation tools. None of it ships in the library,
   none of it is imported by `cqlite-core` / `cqlite-cli` / the bindings, and none of it runs in CI or the
   agent gate. A defect can corrupt a *report's numbers*; it cannot corrupt a release, a user's data, or a
   gate verdict.
2. **The largest tranche already had a full adversarial pass.** #3222's **34** executables were reviewed
   file by file when the wrapper refused to certify them, and that review is recorded in the PR. It found
   **no blockers** — and it did find real defects (a fourth silent-failure instance where every driver log
   fabricated `rc=0`, because `$(…)` resets `$?`, plus two provenance defects), all fixed pre-merge. The
   biggest slice of the exposure is reviewed by a *more* expensive mechanism than roborev.
3. **The class cannot recur silently — but the guarantee is weaker than originally planned, and that is
   stated rather than glossed.** What holds: no blanket directory glob in `exclude_patterns`, the census
   classifying `docs/` executables as CODE, hermetic `(cx*)` cases failing the `--lite` loop on a
   classification regression, and `prompt-content:` failing closed on any path the reviewer did not
   receive. What does **not** hold: there is **no automated guard against a future `.roborev.toml`
   re-broadening**. The pre-enqueue check that would have caught it at edit time was removed (deferred to
   #3283) because its own false-PASS rate was rising; the regression it would catch is a hand edit to a
   version-controlled file on `main`, and doctrine names the hazard in prose. A re-broadening would
   therefore surface later, as a `prompt-content:` FAIL on an unrelated PR, rather than immediately.
4. **Retroactively reviewing code whose outputs are already banked buys audit theatre, not safety.** Those
   reports are merged and already acted on; a finding now would produce a comment on a historical
   artifact, not un-bank a number. Spending review rounds there instead of on live code is the worse
   allocation.

**What would reopen it:** any of that harness code being promoted into a shipped path — a gate component,
a CI step, an imported module. At that point it inherits the review obligation of the surface it joins.
That is a rule about *promotion*, not about history.
