# Design — logging-facade-tracing

## Decision: Option 1 (migrate to `tracing`), not Option 2 (document the bridge)

The issue offered two options. **We choose Option 1: migrate all core event sites
to `tracing`.**

| | Option 1 — migrate to `tracing` | Option 2 — document the bridge |
|---|---|---|
| Fixes the silent-drop for embedders | **Yes, structurally** — one facade, nothing to bridge | Only if the embedder reads the docs and wires `LogTracer` correctly |
| Enforceable | **Yes** — grep-guard test fails if the mix returns | No — a doc note rots; regressions are invisible |
| Removes the trap | **Yes** — `log` dep can be dropped | No — the two-facade trap remains, just documented |
| Cost | Mechanical sweep of ~600 sites (fastmod/ast-grep) | Low, but leaves the hazard live |
| Blast radius | Large diff, but message-preserving and tool-assisted | Small |

Option 2 leaves the exact failure mode (a tracing-only embedder silently losing
the #586 corruption warning) **live** and merely documented — unacceptable for a
P0 "promised-visible" diagnostic. Option 1 removes the hazard at the root and is
enforceable, so it wins despite the larger diff.

## Mechanical sweep approach

- The migration is a **message-preserving macro-path rewrite**:
  `log::warn!(…)` → `tracing::warn!(…)` (and `info`/`debug`/`error`/`trace`).
  Arguments, format strings, and structured fields are copied verbatim.
- Use `ast-grep`/`fastmod` with a **word-boundary** match so identifiers that
  merely contain the substring `log::` (e.g. `catalog::`, `dialog::`) are NOT
  rewritten. The exact remaining-count is established by the implementer with the
  bounded regex; the ~600 figure is a scoping estimate, not a contract.
- `use log::…;` imports become `use tracing::…;` (or are dropped in favour of
  fully-qualified `tracing::warn!`), and any `log = …` line leaves
  `cqlite-core/Cargo.toml` once the build confirms no residual reference.

## Sequencing vs AI3 (#1703, OPEN)

The issue notes: migrate **after AG5 and AI3 land** (they churn the same lines).
- **AG5 (#1694): CLOSED/landed** — its data-safe message wording is already in
  place, so this change preserves that content and does not touch it.
- **AI3 (#1703): still OPEN** — "demote write-side spans + per-query info
  chatter." AI3 touches span levels / some info sites in the same files.

**Recommendation:** because AI3 is unclaimed and this migration is purely
mechanical + message-preserving, proceed now and treat any AI3 overlap as a
routine rebase (whichever lands second re-runs the sweep on the merged lines).
The grep-guard test makes a missed site impossible to merge, so a rebase cannot
silently reintroduce a `log::` event. **Owner call point:** if you would rather
hard-serialize behind AI3, say so at approval and I will park this change on its
branch until #1703 merges.

## Verification (the acceptance oracle)

1. **Bridge-less capture test (TDD, must fail on `main`)**: install a
   `tracing`-only subscriber with NO `LogTracer`; drive the #586 corruption-warning
   path; assert the event is captured. Red on `main`, green after migration.
2. **Grep-guard test**: zero `log::{warn,info,debug,error,trace}!` in
   `cqlite-core/src`. Red before the sweep, green after.
3. **Level-preservation**: spot-checked in the capture test (level asserted).
4. **`log` dep drop**: `cqlite-core` builds with `log` removed from its
   `Cargo.toml`; full `scripts/agent-gate.sh` PASS.

## Wiring evidence

The public surface exercised is **the event stream a real embedder observes**:
the capture test consumes events through a stock `tracing` subscriber (no bridge)
— exactly what an embedder sees — proving the corruption warning is delivered end
to end, not merely that a helper emits it.
