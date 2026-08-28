# `phase2-run/` — the `do_get`-on-Corpus-B raw data (AC4's numerator)

**These are the artifacts behind every `do_get` figure in `../../ws0-3299-report.md`.**
They were produced by invoking `flight-loadgen` **directly**, not through a committed
runner — the two phase-2 scripts that were written for this were deleted after
review found them broken and unused (they could not start the server, mismatched
their perf window against their row count, and printed verdicts they never
validated). The exact commands are in the report's reproduction section.

**The `do_get` arm carries NO perf counters.** Its rows/s is `flight-loadgen`'s own
per-step accounting — the same arm-B convention #3100/#3217 used. That is a real
methodological asymmetry against the bare-scan arm's aligned window (control-FIFO
bracketed, rows differenced from emitted progress records), and it is disclosed in
the report rather than hidden by the deletion. Fidelity to the existing arm-B
convention was chosen over consistency with this issue's own arm-A convention.

| file | what it is |
|---|---|
| `ticket-template.json` | THE REQUEST every rep re-read. sha256 `f4efb7b7724986f655c37d99ceb668b99b08fd73d5de9cead4a1b672a778a858`. The schema travels in the ticket (`service.rs` `parse_schema`), so the server needs no `--schema`. |
| `smoke.jsonl` | servability positive control: ONE full-shape `do_get` returning **exactly 4,000,000 rows** = the corpus row count. Run before any measurement, because a 0-row `do_get` would otherwise read as a very fast one. |
| `falsification-client-2phys.jsonl` / `falsification-client-1phys.jsonl` | the client-bound falsification. Identical server (6 physical cores) and ramp; only the client's cores differ (2 physical vs 1 physical). **1,027,268 vs 1,027,467 rows/s = +0.02%**, so the S=6 measurement is NOT client-bound. This REFUTED `../phase2-recon.md`'s own recommendation against measuring S=6 — the recon proposed the test so the answer would be measured, and the measurement went against it. |
| `doget-s6-r{1,2,3}.jsonl` | `do_get` S=6, server pinned to 6 physical cores, client to `6,14,7,15`. best-N=16 ⇒ **1,198,673 rows/s**. N=24 is −0.11% (within spread) ⇒ plateau, take the lower N. |
| `doget-s1-r{1,2,3}.jsonl` | `do_get` S=1 at the rig's **calibrated 1:4 split** (`--server-cpus 2,10`, `--client-cpus 4,12,5,13,6,14,7,15`). best-N=2 ⇒ **243,536 rows/s**, bracketed (219,401 @ N=1 < 243,536 @ N=2 > 223,835 @ N=4). |
| `server-s{1,6}-provenance.txt` | the server's own startup line. **`max_concurrent_scans` is DERIVED from the affinity mask** (#3225): 24 pinned to 6 physical cores, 4 pinned to 1. Every N ladder here stays under its ceiling — but it silently caps any `do_get` N sweep, so a future reader must check it.  |

**`--shape full` is mandatory** and is on every command. `flight-loadgen` defaults to
`--shape mixed` (`ptr=0.6,lim=0.3,full=0.1`), which would silently have measured a
different workload than the bare-scan arm.

**Excluded from the report:** `do_get` S=6 at N=4 (24.61% spread, 5-7 of 8 requests
completing) — ramp warm-up, not a measurement, and not best-N.
