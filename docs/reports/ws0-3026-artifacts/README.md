# WS0 (#3026) raw measurement artifacts

Evidence behind `docs/reports/ws0-cassandra-baseline-2026-07-27.md`. Committed to close **B1** of the
owner review on PR #3081: the report originally cited `/home/ubuntu/ws0/...` paths on **ephemeral EC2
storage**, so every figure would have become an unverifiable assertion when the instance terminated —
while AC#1 ("re-runnable by a future agent") was marked satisfied. That was wrong; this directory fixes it.

Measured on **c7i.4xlarge** (16 vCPU / 8 physical cores, Xeon Platinum 8488C, 30 GiB, **EBS-only**),
kernel 6.17.0-1019-aws, against Cassandra **5.0.8** on OpenJDK 17.0.19 and CQLite **0.16.1**.

## Layout

| Path | Contents |
|---|---|
| `ws0-results/head-to-head-method.md` | **The method document.** Full re-run procedure, plus the F1–F7 finding notes that several report figures cite. |
| `ws0-results/h2h/summary-*.json`, `scan-*.json`, `*.jsonl` | Per-run measurement summaries — the source of every rows/s, cycles/row, IPC and `l2_lines_in.all` figure in §4. Each `rep-*` triple is one median-of-3 arm. |
| `ws0-results/h2h/*.log`, `*.err` | Per-run stdout/stderr. |
| `ws0-results/fio-*.json`, `00-layout.json`, `fio-summary.txt`, `bwlog-*`, `latlog-*` | §8 `fio`, all four arms, raw JSON + per-second bandwidth/latency logs. |
| `ws0-results/membw-calibration.txt` | §7 proxy calibration (`l2_lines_in.all × 64`) at 512 MiB / 32 MiB / 1 MiB, **re-measured** after the fact-check found the original had no retained output. |
| `ws0-results/stream-*.txt`, `readbw-*.txt`, `stream_bench.c`, `read_bw.c` | §7 memory-bandwidth measurements + sources. |
| `ws0-results/h2h-perf-summaries/` | **Truncated** perf/jstack reports (see exclusions). **These do NOT back the §5 percentages** — corrected below. |
| `ws0-results/session-timeline.txt` | Kernel-log timeline of the measurement session: the OOM kill (17:05:28Z), the swapfile creation (17:59:08Z), the mid-session **reboot** (the §8 `fio` arms ran on the previous boot), and the `thread_siblings_list` SMT topology. Backs the before/after-swapfile attribution and the "physical core *N* = logical *N* and *N*+8" claim in the report. |
| `ws0-corpus/` | Corpus generation, in full: `rerun.sh`, `gen-corpus.sh`, `ws0-profile.yaml`, `schema-as-created.cql`, `cassandra.yaml.diff`, `measure-sstable.py`, `fullscan.py`, `trace-scan.bt`, `verify-claims.sh`, `corpus-geometry.txt`, `claims-evidence.txt`, `full-load.log`. |
| `ws0-cqlite/` | Scan-harness sources, `membw_cal.c`, `run-scan-measure.sh`, `build.log`. |
| `ws0-h2h/` | Head-to-head drivers. |

## Deliberately EXCLUDED, and why

Three files were ~300 MB of the 318 MB original tree — perf/jstack reports dominated by call-graph text:

| Excluded | Size | Substitute |
|---|--:|---|
| `h2h/prof-rows.txt` | 161 MB | `h2h-perf-summaries/prof-rows.top400.txt` |
| `h2h/prof-count.txt` | 121 MB | `h2h-perf-summaries/prof-count.top400.txt` |
| `h2h/jstack-rows.txt` | 17.7 MB | `h2h-perf-summaries/jstack-rows.top300.txt` |
| `*.data` (perf binary) | 14.8 MB | none — regenerate via the method doc |
| `*.stcap` | 1.7 MB | none |
| `ws0-cqlite/harness-target/**` | 1.3 GB | build artifacts; rebuild from source |
| `ws0-h2h/datasets/**` | 754 MB | the SSTable corpus itself; regenerate with `ws0-corpus/rerun.sh` |

**The truncation is worse than a formality, and worse than first described here.** The original claim above
— that the `top400` files "carry the ranked frames the report cites" — is **wrong**, and was corrected in a
later review round. `prof-count.top400.txt` and `prof-rows.top400.txt` are the **first 400 lines** of the raw
`SJK ssa --print` output, i.e. a single sample timestamp of mostly parked threads; `jstack-rows.top300.txt`
is the head of a plain `jstack`. Verified properties: **zero `%` characters in any of the three files**, so
no percentage the report cites appears in them; `prof-count.top400.txt` contains **one** `ReadStage`
mention (thread 89, state `WAITING`) and **zero** occurrences of `Cell$Serializer`,
`createDataResponse`, `UnfilteredPartitionIterators`, `BTree.apply` or `ResultSet$Codec`.

Consequence, recorded rather than papered over: **every percentage in report §5, and finding F2 in the
method doc, are unretained** — they survive only as prose. The report now labels them as such. The
*structural* claims those sections make are independently checkable in `cqlite-core` /
`cassandra-5.0.8` source, and #3058's priority rests on the artifact-backed rows/s and cycles/row gaps in
report §4, not on any of these shares. To recover the percentages, re-run a full profile per the method doc.

The corpus is **not** committed (783,799,203 B `Data.db`), but it is reproducible: `ws0-corpus/rerun.sh`
regenerates it and `corpus-geometry.txt` records the identity to verify against —
`sha256 22d9ae224b439b2176c287a59eee6a7d1f08b4f1fafc4d2198b3da50cdce922c`, 3,999,890 rows,
692.70 B/row uncompressed, LZ4 3.535×, `chunk_length` 16384, single `nb` SSTable.

## Figures the report labels "no raw run artefact retained"

These were recorded only as prose in `head-to-head-method.md` (F3–F7) and have **no per-run JSON** here.
They are load-bearing for #3058's priority and #3031's blocking status, so the gap is called out rather
than papered over:

- 292,849 rows/s / 13.66 s at `--inflight 1`, and 2,086,231 rows/s full-box (§9.1, the client-bound finding)
- the two `r--s` mmap observation (§9.3)
- 360K rows/s unmetered and ~540K context switches (§9.5, the `perf stat -p` artefact)
- 18.74 s vs 11.16 s unpinned-vs-pinned, 1.98M vs 310K voluntary context switches (§9.6)
- the four-table 81.2k–629.4k rows/s spread (§9.7)
- **every percentage in §5** and method-doc finding **F2** (the `count(*)` profile) — see the truncation
  note above; the substitute files do not contain them

Anyone re-running should regenerate these first — they are the weakest-evidenced claims in the report.

One further provenance note found in the same review round: the report's **Flight 1-hardware-thread** row is
**n=1**, not a median. It comes from `h2h/flight-fl1t.jsonl` + `h2h/perf-fl1t.txt` (5 scans in one 359.75 s
step). The two attempted 1-hw-thread replicates, `rep-fl-2-r1` and `fl-warm-1t`, **failed to connect**
(`flight-loadgen: ... transport error`); their `.jsonl` files are committed but empty, and their `perf-*.txt`
files hold ~5 s of near-idle counters. They are retained deliberately, as evidence of the failure rather
than as replicates.

## Reading order

1. `ws0-results/head-to-head-method.md` — the procedure and its traps.
2. `ws0-corpus/rerun.sh` — regenerate the corpus; verify geometry against `corpus-geometry.txt`.
3. `ws0-results/h2h/summary-rep-*.json` — the median-of-3 arms behind §4.
4. `ws0-results/membw-calibration.txt` — validate the traffic proxy before trusting §7.
