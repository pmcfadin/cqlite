# #3229 artifacts — the AC2 live probe, and the AC7 backfill ruling

This directory is itself part of the change. It carries an **executable** under `docs/`, which is the
exact shape that `exclude_patterns = ['docs/**', '*.md']` used to make invisible to roborev — so this
pull request is a #3222-shaped demonstration of its own fix.

| File | What it is |
|---|---|
| `probe-census-exclusion.sh` | The AC2 live probe **and** the specimen. Runs the sanctioned wrapper and prints the summary-block lines to record. Not a gate component. |
| `../../../website/src/content/docs/_3229-root-anchoring-probe.json` | A deny-listed extension (`.json`) under a **nested** `docs` directory — the end-to-end confirmation that a slash-containing pattern is ROOT-ANCHORED. |

## The live probe (AC2) — procedure and expected values

```bash
bash docs/reports/3229-artifacts/probe-census-exclusion.sh --repo "$(git rev-parse --show-toplevel)"
```

It needs the network and a live reviewer, so it is **documented and recorded, never gate-run** (the
hermetic half is the `(cx*)` case family in `scripts/tests/test_roborev_review_guard.sh`, which runs in
`--lite` and the full gate). Record these lines from the emitted block:

| Line | Expected |
|---|---|
| `census:` | the branch's `git diff --numstat --no-renames origin/main...HEAD` counts |
| `code-free:` | `PASS` — a `docs/` path prefix never makes a program documentation |
| `census-exclusion:` | `PASS (<n>/<n> code census paths survive the effective exclusion set; corroboration: OK)` |
| `prompt-content:` | `PASS (<n>/<n> code census paths present)` |
| `reviewed-sha:` | the RANGE `<base40>..<head40>`, head endpoint = branch HEAD |
| `tokens:` | the **genuine-review band**: 398k–649k input / 314k–554k cached / 5.0k–6.3k output, minutes of wall time |

**Read the tokens before the verdict.** A signature near the **vacuous baseline** — ~18.7k input, 0
cached, 53–56 output, ~8s; PR #3222 itself measured **15,443 in / 89 out** beside
`prompt-content: FAIL (136/136 code census paths absent)` — means the defect persists, whatever `RESULT:`
says. A `RESULT` of `FINDINGS`/`FAIL` because the reviewer found real issues is **not** a probe failure:
the probe is about scope, not verdict.

**The second, independent assertion.** `website/src/content/docs/_3229-root-anchoring-probe.json` MUST be
PRESENT in the prompt actually sent:

```bash
bash docs/reports/3229-artifacts/probe-census-exclusion.sh --check-nested   # prints the assertion
roborev show <job> --prompt | grep -F 'website/src/content/docs/_3229-root-anchoring-probe.json'
```

Present ⇒ the disassembly-recovered `git.FormatExcludeArgs` is confirmed live. **Absent ⇒ the port is
FALSIFIED and the change is BLOCKED** — both the pattern list in `.roborev.toml` and the ported
construction in `scripts/flow/roborev-review-oracles.sh` rest on that root-anchoring result. It is not an
acceptable outcome to merely record.

Everything here is pinned to **`roborev v0.61.2`**. Re-run the probe, and re-verify the port, after any
roborev version bump: an upstream change to `FormatExcludeArgs` would silently invalidate the port while
every summary block still read `PASS`.

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
3. **The class cannot recur silently, which is most of what a backfill would buy.** That assurance now
   comes from mechanism: no blanket directory glob in `exclude_patterns`, a pre-enqueue
   `census-exclusion:` check that FAILs closed naming the swallowed paths and the pattern responsible, and
   hermetic `(cx*)` cases that fail the `--lite` loop on a regression.
4. **Retroactively reviewing code whose outputs are already banked buys audit theatre, not safety.** Those
   reports are merged and already acted on; a finding now would produce a comment on a historical
   artifact, not un-bank a number. Spending review rounds there instead of on live code is the worse
   allocation.

**What would reopen it:** any of that harness code being promoted into a shipped path — a gate component,
a CI step, an imported module. At that point it inherits the review obligation of the surface it joins.
That is a rule about *promotion*, not about history.
