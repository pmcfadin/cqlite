# roborev severity: blocker vs nit (issue #2088)

roborev's own output is unstratified: every finding — a real correctness bug and a
cosmetic naming nit alike — has historically forced the same expensive re-verify
round (334 rework rounds across 174 issues, 64.9%, the #1 weighted failure driver in
the delivery-telemetry retro). This rubric lets the reviewer of record triage each
finding into exactly one severity before deciding whether it blocks merge.

## Rubric

**BLOCKER** — must be fixed pre-merge, triggers a re-verify round:
- Correctness bugs (wrong output, wrong control flow, off-by-one, wrong precedence)
- Data-parity gaps (byte-parity, sstabledump/JSONL-golden mismatches, no-heuristics
  violations — schema/metadata bypassed in favor of guessed types)
- Safety issues — `unwrap()`/`expect()`/panic paths reachable in library code, unsound
  `unsafe`, unchecked arithmetic that can overflow/panic
- Wiring-evidence gaps — a feature or fix that isn't actually reachable/exercised by a
  public-surface test (looks done, isn't proven done)
- Security issues (injection, path traversal, secret handling)
- Anything that would fail one of the issue's stated acceptance criteria

**NIT** — batched, never blocks merge on its own:
- Style and formatting preferences already inside what `cargo fmt`/clippy accept
- Naming choices with no behavioral effect
- Comment/doc polish, wording, typos
- Test-robustness *suggestions* that name no concrete failing scenario (e.g. "consider
  adding a test for X" with no reproduction of X actually failing)

## Rule of thumb

**When in doubt, blocker.** A conservative bar here costs one extra look at merge time;
an under-classified blocker slipping to "nit" costs a production bug plus the same
re-verify round the classification was supposed to avoid. The pre-merge full gate and
the required CI backstop are the safety net for a mis-graded nit — they are not a
safety net for a mis-graded blocker that was waved through as cosmetic.

## Loop rule

- **Blockers**: fixed pre-merge, in the same round as everything else roborev found.
  Each one re-triggers the normal fix → `--lite` gate → re-review loop.
- **Nits**: never trigger a re-verify round. All nits from one PR's roborev pass are
  batched into **one** linked follow-up issue (labeled, referencing the PR) opened at
  merge time, not fixed inline and not re-reviewed.
- **Scripts and harnesses — two-round cap (owner ruling 2026-09-01, #3893)**: for diffs whose
  code is `scripts/**`, `.claude/**`, `.github/**` or `docs/reports/*-artifacts/**`, the
  fix → `--lite` → re-review loop runs at most **two** rounds. Round-3 findings are disposed
  (one follow-up issue per PR + a `roborev-defer` marker requested on the merits), not fixed —
  except a **hang** or a **false verdict**, which are always fixed regardless of round count.
  Rationale: bash has no compiler, so fix rounds seed the next round's findings (22/25/32
  findings over 7–12 rounds on three harness PRs in one day). Product code (`cqlite-*/src`,
  bindings) keeps the uncapped blocker rule above.

## Telemetry

`scripts/delivery-telemetry.py record` accepts optional `--roborev-blockers` /
`--roborev-nits`, which must be supplied together and must sum to
`--roborev-findings`. Absence means severity was not classified for that issue —
never zero-filled (authoritative-only, per the no-heuristics/telemetry mandate). The
`retro` subcommand prefers the blocker count for its weighted ranking when severity
data is present, and reports the nit count separately so it never inflates the
recurring-failure score.
