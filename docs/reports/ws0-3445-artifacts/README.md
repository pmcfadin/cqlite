# `ws0-3445-artifacts/` — raw evidence for `../ws0-3445-report.md`

Measurement only (issue #3445, epic #2817). **No production code is changed by this PR**;
both probe builds were applied in a detached throwaway `git worktree` and ship here as
patches.

## Read in this order

| path | what it establishes |
|---|---|
| `ac0/corpus-rehash.md` | the corpus is the pinned #3096 one — regenerated from seed on this host, then re-hashed 8/8 on disk by an INDEPENDENT tool, plus the corpus shape every per-row figure divides by |
| `ac0/host-identity.json` | this is not #3248's host; the cross-instance move is stated rather than left undecidable |
| `raw/counter-capability-census.md` | what this box can and CANNOT measure. **Read before trusting any number here**: no PEBS and no Topdown slots, and both bound the claims |
| `ac1/codegen-fingerprint.md` | the disassembly identification of the inlined decoder — `decode_unsigned` has NO symbol (nm: 0), and the four fingerprint elements are annotated against `vint.rs:40-77` |
| `ac1/vint-share-*.json`, `ac1/ab-*.json` | per-rep shares with the PIE rebase self-check verdict and the skid band |
| `ac1/SUMMARY-STATS.txt` | the pooled statistics, the two-route disagreement, and the AC3 verdict line |
| `ac1/vint-regions-perfprof-1.json` | fingerprint-vs-DWARF agreement (133/138) plus the per-opcode and per-caller composition |
| `ac1/vint-width-distribution.md` | the measured 55.6% single-byte / 44.4% multi-byte split that explains the `bswap` concentration |
| `ac1/codegen-fidelity-profiles.json` | `debug=1` vs codegen-faithful function profiles — max delta 0.17 pp |
| `ac2/counters-rep*.csv` | the counting reps, `pct_running` **100.00** in field 5 on all six events |
| `ac2/stall-share-rep*.json`, `ac2/stall-regions-rep1.json` | vint's share of `cycle_activity.stalls_total`, attributed through the same inline chain as cycles |
| `raw/validity-and-refusals.md` | every rep's validity checks, the REFUSED observations, and the co-tenancy discussion |

## Harness (`harness/`) — reviewed code, not scratch

| file | role |
|---|---|
| `record-scan.sh` | places a `perf` window strictly inside the #3299 worker's WARM, pinned, post-barrier steady state. Adds only the window, the pinning and the validity bookkeeping — the scan itself is the #3299 worker, reused unchanged |
| `vint_share.py` | the headline attribution: classify each sample by its full DWARF inline chain; two boundaries; PIE rebase self-check that REFUSES rather than print a wrong table; skid sensitivity band |
| `vint_regions.py` | corroborates DWARF against the disassembly fingerprint, and reports per-opcode / per-caller composition |

The scan worker itself is **not** copied here. It lives at
`../ws0-3299-artifacts/harness/scan-worker/` and is built with two environment overrides
(`CARGO_PROFILE_RELEASE_DEBUG`, `CARGO_PROFILE_RELEASE_STRIP`) rather than being forked, so
this issue measures exactly the code path #3299 and #3248 measured.

## The two probe patches

Both are applied ONLY in a detached `git worktree` and are committed here as artifacts so the
cross-checks are reproducible without shipping a measurement-only feature flag in
`cqlite-core` — which, per #1699/#3522, would be a feature nothing executes.

* `ac1/inline-never-probe.patch` — `#[inline]` → `#[inline(never)]` on the two decoders, for
  AC1's Route 2 symbol-visible cross-check. **Changes codegen; cross-check only.**
* `ac1/vint-width-probe.patch` — relaxed-atomic width counters, for the width distribution.
  **Perturbs the hot loop; its TIMING is discarded and only its distribution is used.**

## Why the number here is not just "0.74% was wrong"

`ac1/` shows the inlining blind spot was real but worth ~2.2x the floor, and — the part worth
knowing — that #3027's guess about *where* the hidden cycles were is falsified: they are not
inside `parse_row_data_with_offset_impl`'s 9.6% but inside the two small functions #3027 had
already named at 0.87% and 0.74%.
