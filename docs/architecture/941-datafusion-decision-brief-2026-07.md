# Decision brief: promoting #941 (DataFusion execution)

**Date**: 2026-07-16 · **Decision owner**: @pmcfadin · **Status**: DECIDED — Option C (spike first, promote on data)

## The decision

Whether (and when) to activate epic #941 — embed DataFusion as the columnar query-execution
engine (children #1905–#1914, parked Backlog-by-design since the 2026-07-04 Design-A packet,
`docs/architecture` @ d734c44b).

## Why it's forced eventually — the ladder math

Round 12 (issue #2367, 2026-07-16) measured full-scan at **~10.6k rows/s/pod**
(1.94M rows, 61.1s, 3 pods) — the baseline rung of the ratified A4 ladder
(10k → **Stage 1: 100k** → **Stage 2: 600k** → Stage 3: millions;
`docs/architecture/performance-goals-2026-07.md`). B3's headline (66s → **10s** → 3s) maps to
the same rungs. Stage 1 is row-engine territory. **Stage 2 is not**: 600k rows/s/pod through a
row-at-a-time engine means ~1.6µs/row end-to-end including decode, merge, and Arrow encode —
vectorized-execution territory, which is what the Design-A packet concluded. Nothing since
contradicts it.

## What's genuinely unknown

How far Stage-1 levers get us. Three are in flight as of this writing: #1644 zero-copy
(allocation elimination in row assembly), #2600 egress backpressure (R12's dominant saturation
signal — 3,505 rows queued at merge-egress while the blocking pool sat at 8/~512 and admission
at 12/64), and the shipped Epic-K hot-loop work. Plausible Stage-1 outcome: 66s → 15–25s.
That changes the *urgency* of #941, not its inevitability.

## Options considered

| | A: promote now (0.16) | B: defer until Stage-1 measured | C: spike now, promote on data |
|---|---|---|---|
| What | Activate #1905–#1914 as an 0.16 program | Hold Backlog until #1644 + #2600 land and a field round measures the new ceiling | One de-risking spike in 0.16 (thin DataFusion `TableProvider` over the existing flight scan, feature-gated, bench-only); promotion decision after the next field round |
| Wins | Longest lead time on the hardest work | Zero risk of building on a moving baseline | Proves the integration seam + measures the vectorization delta on OUR data; decision lands on two numbers, not estimates |
| Costs | Heavy dependency lands while the row path churns under it; competes with 0.16's export/CLI theme | If Stage-1 stalls (~20s), a release cycle of lead time is burned | ~1 issue of fleet time; spike code carries throwaway risk |
| Risk | Integration rework as #1644/#2600 reshape the scan path | B3 slips a full release | Low |

## Decision — Option C (owner, 2026-07-16)

- Groom one spike issue for 0.16: **DataFusion `TableProvider` PoC over the flight scan path**,
  benchmarked against the row engine on the R12 corpus, feature-gated, zero production wiring.
- The promotion call happens at the next field round with two hard numbers: the post-Stage-1
  row-engine ceiling and the measured DataFusion delta on identical data.
- Trigger rule: if Stage-1 lands short of **~30k rows/s/pod**, promote the full epic for 0.16;
  if it clears that comfortably, #941 targets 0.17 with the spike already banked.
- The Design-A packet remains the blueprint; this decides *when*, on data.

## References

- #941 (epic) · #1905–#1914 (children) · #2403 (0.15 theme epic) · #2367 (field rounds)
- `docs/architecture/performance-goals-2026-07.md` (ladder) · Design-A packet @ d734c44b
- R12 evidence: `docs/round-artifacts/r12/report.html`
