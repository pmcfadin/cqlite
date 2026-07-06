# Synthesis — THROUGHPUT: where wall-clock goes per issue

**Theme:** where does per-issue wall-clock actually go, which gates/CI lanes are redundant or oversized, how the pipelining doctrine holds up, and what makes a single machine ship faster without weakening the bar. All numbers below are recomputed directly from `docs/reports/delivery-telemetry.jsonl` (n=174, 2026-06-28→07-06), not the digest.

## Findings

**F1 — Wall-clock per issue is ~99% backlog, ~1% active work.** Median created→PR = **29.4h**; median PR→merge = **16.2 min** (recomputed from `phase_s`). The merge tail is already fast. Design-routed issues wait **91.0h** median in backlog vs oracle **22.4h** — the OpenSpec Seam-1 spec-approval wait is ~4× the oracle activation wait. **The dominant throughput sink is not any gate or CI lane; it is the human/scheduling latency before a worker starts.** No machinery optimization moves the median issue faster while it sits 29h waiting for activation.

**F2 — Inside active work, the cost is the fix-loop, not the gate wall-time.** Full gate is now ~258s warm (post #1841 nextest+2-lane, retros-and-audits.md). But **47% of all gate runs fail** (152 of 325 total; only 87/174 issues — 50% — pass first try). The re-runs are driven by review: issues **with** roborev findings average **2.71** rework rounds vs **0.17** without (recomputed). 608 total roborev findings across 174 issues. Each failed-then-fixed round pays another full gate. The gate is fast; we run it too many times per issue.

**F3 — Loop ordering wastes the expensive gate.** Current implement loop is `lite → (conditional) review-first → FULL gate → C → roborev → fix → re-gate` (agents-and-skills.md, CLAUDE.md). Roborev runs **after** the full gate, so any roborev finding needing a src change forces a **second full gate**. Review-first is "conditional/optional" and skipped for mechanical diffs. Given F2's 2.71-round correlation, review is discovering fixable problems **after** we already paid for the gate.

**F4 — Node bindings are the #2 gate component and the only LTO build in the gate.** Warm breakdown (retros-and-audits.md): core-tests 67%, **node-bindings 174s (17%)**, python 72s (7%). node-bindings compiles cqlite-core under `release-unwind` (LTO, codegen-units=1) — the single release/LTO build in the whole gate (redundant-compilation matrix, retros §"Redundant compilation matrix"). This ~150s of LTO is paid on **every** full gate purely to exercise the panic-unwind firewall, which already has its own self-test (`test_binding_unwind_profile.sh`, 4 configs) and nightly coverage.

**F5 — Binding parity runs the 33-table corpus 4×.** core, CLI, Python, and Node each full-scan all 33 tables (retros §4, testing-suite.md). The digest estimates <60s if compressed to conversion-boundary representatives with a nightly full sweep as backstop. Owner decision pending since 2026-07-03.

**F6 — Rebase churn silently multiplies gate runs.** 63.2% of issues rebase (186 total). At 174 issues in 9 days, main advances fast; each rebase before merge can re-trigger the full gate. This is a hidden multiplier on F2's gate-run count that no tier (lite/delta) currently absorbs — a rebase changes the diff base, so `--delta` re-cert can't cover it.

**F7 — sccache corruption forces the accelerator off under load, and #1776 is a self-amplifying flake.** sccache intermittently served corrupted objects under extreme load (2026-07-06 MEMORY note); workaround `CQLITE_DISABLE_SCCACHE=1` forfeits the 25.6% fresh-build win. Flaky components #1776/#1774/#1803/#1819 logged 69 false-red events; #1776's wall-clock throughput assertion fails **because** of load that re-runs create — a contention feedback loop. #1825's cap stops SIGKILL but does not fix the underlying flake.

**F8 — Pipelining doctrine works where measured.** `--lite` (1–5 min inner loop) + `--delta` re-cert (test/docs-only rounds skip the full gate) + merge-on-green (no CI busy-wait) are real wins: PR→merge at 16.2 min median proves the tail is not the problem. The friction is that all three are **human-discipline, not code-enforced** (doctrine-and-process.md): a subagent that runs the full gate blocks the worker to a 600s watchdog kill; `--delta` fails closed if a diff touches node `__test__/` or `scripts/tests/*.sh`, forcing a full gate for a shell-test tweak.

## Recommendations (ranked)

**R1 — Move roborev + rust-reviewer review-first BEFORE the first full gate; make it default, not conditional.**
Reorder the implement loop to `lite → rust-reviewer/roborev on lite-green diff → fix → FULL gate → C → merge`. Rationale (F2/F3): 47% gate-fail rate and 2.71-round rework correlate with review findings landing after the gate. Reviewing the lite-green diff first means the expensive gate runs on already-reviewed code, converting multi-run issues toward single-run.
*Payoff:* if this converts even 40% of the 87 multi-run issues to single-run, ~35–50 fewer full gate runs (~2.5–3.5 gate-hours over a 174-issue sample), plus fewer rebase-exposure windows. *Cost:* doctrine + skill edit in `flow-implement`; +5–10 min review earlier (but that time is already spent later, at higher cost). Low risk.

**R2 — Drop node-bindings' LTO build from the full gate; run it dev-profile in the gate, release-unwind only in nightly + the existing self-test.**
Rationale (F4): ~150s of codegen-units=1 LTO on every gate for firewall coverage that `test_binding_unwind_profile.sh` + nightly `gate.yml` already provide. Build node bindings under `dev` in `scripts/agent-gate.sh`; keep the unwind assertion in the self-test and nightly.
*Payoff:* ~150s (~10%) off **every** full gate; at ~325 gate runs that is ~13.5 wall-clock hours saved over the sample. *Cost:* one gate-script change + confirm the unwind firewall stays covered nightly. Medium risk — must not lose the panic=abort→unwind regression signal (F4 mitigations make this safe).

**R3 — Compress binding-parity to conversion-boundary representatives in the gate; full 33-table 4× sweep nightly.**
Rationale (F5): the 4× full-corpus scan is redundant per-gate cost; the type-conversion boundary is what bindings actually add over core. Owner decision has been pending since 2026-07-03 — escalate it.
*Payoff:* digest estimates the 4 suites drop from ~250s+ to <60s. *Cost:* pick representative tables per CQL-type-family; wire a nightly full sweep backstop. Medium risk — a representative subset could miss a table-specific parity break; nightly sweep bounds the exposure to 24h.

**R4 — Root-cause sccache corruption and de-flake #1776 as one contention-hardening task.**
Rationale (F7): both are load-triggered. Fix sccache's under-load corruption (likely concurrent-write race in the shared cache) so `CQLITE_DISABLE_SCCACHE=1` stops being the standing workaround, and replace #1776's wall-clock throughput assertion with a deterministic or load-independent check (assert work performed, not elapsed time).
*Payoff:* restores the 25.6% fresh-build win on loaded machines + removes a chunk of the 69 false-red events (each a wasted gate). *Cost:* real debugging (root cause TBD); the #1776 rewrite is small. Medium risk, high value on a busy single machine.

**R5 — Extend `--delta` components to execute node `__test__/` and `scripts/tests/*.sh` so post-gate polish rounds stop forcing full gates.**
Rationale (F8): `--delta` fail-closes on those two file classes purely because the delta lane can't run them, not because they're risky — so a shell-test or node-test-only roborev round pays a full gate. Add those executors to the delta lane.
*Payoff:* eliminates a class of unnecessary full gates on address rounds (the #1853/#1921 pattern that burned 2–3 gates each). *Cost:* wire two test executors into `--delta`; keep fail-closed for src/Cargo/config. Low risk.

**R6 — Attack the real ceiling: pre-activate a Ready buffer so the worker never idles on backlog.**
Rationale (F1): 29.4h median backlog (91h for design) dwarfs everything else. For single-machine throughput the machinery is already near-optimal; the win is keeping a groomed+approved Ready queue deep enough that the worker always has the next issue. This is owner-gated (Seam 1 spec approval is sacred) — surface it as a scheduling decision, not a code change: batch spec-approval sessions so design issues don't each wait 91h.
*Payoff:* potentially the largest wall-clock reduction available, but bounded by owner availability. *Cost:* owner process change; no code. This is a **NEEDS-YOU**, not an autonomous fix.

## Risks / tradeoffs

- **R1 front-loads review** — if a diff would have passed the gate clean anyway, review-first adds latency to that issue. Mitigated by the 50% first-pass rate: half of issues are the clean case, but the other half's rework cost dominates the average. Keep the mechanical-diff skip.
- **R2/R3 shrink per-gate coverage** and lean on nightly for the moved checks. The full gate stops being fully self-contained; a binding-parity or unwind regression could land and only be caught within 24h by `gate.yml`/nightly. The delta re-cert doctrine already accepts exactly this nightly-backstop tradeoff (#1892), so it is consistent — but it does weaken the "one full gate proves everything" guarantee. Requires the nightly lanes to be genuinely watched, not just green-by-default.
- **R4** is open-ended debugging; the sccache race may be upstream (Mozilla sccache) and not locally fixable — fallback stays `CQLITE_DISABLE_SCCACHE=1` + one-worker-per-machine.
- **R6** cannot be automated without eroding Seam 1; pushing activation cadence too hard risks approving under-specified design work, which F2 shows costs more in rework downstream. The backlog wait is partly a **quality investment**, not pure waste.
- **General:** all gate-tiering boundaries remain human-discipline, not code-enforced (F8). Every recommendation that adds a tier/branch to the loop adds another rule an implementer can violate; the 600s subagent watchdog is the only hard backstop.
