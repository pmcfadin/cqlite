# WS0 #3217 measurement harness — full-box C(N) + `do_get` handoff attribution

Measurement tooling only. Nothing here touches production source under `cqlite-*/src/**`.

Issue #3217: does the Flight `do_get` mpsc handoff cost real throughput as concurrency and
core count grow, or is the C(N) shape explained by something else? Part A measures the
curve; Parts B/C attribute where the time goes.

---

## Scripts

| Script | Purpose |
|---|---|
| `common.sh` | Shared plumbing: sysctl re-assert, verified topology, the server-core-set table, server launch/readiness/stop, `/proc` counter helpers. Sourced, never run. |
| `sweep.sh` | **Part A.** The C(N) driver: sweeps N at a fixed server core count S, ≥3 reps per point, one JSONL record per (S, N, rep) plus a summary table. |
| `profile-oncpu.sh` | **AC3.** `perf record -F 999 --call-graph=fp -C <server-cpus>` over a steady-state window → folded text + SVG, and gates the unsymbolized-frame fraction. |
| `profile-offcpu.sh` | **AC4 + AC5.** Off-CPU (blocked-time) stacks per N → folded + SVG + the ranked classified attribution table, plus the per-N scheduler-cost table (context switches, migrations, run-queue latency). |
| `classify-offcpu.py` | Buckets blocked stacks into the seven AC4 categories. The match table is one visible, ordered block at the top of the file. |
| `corpus-basis.py` | Measures the corpus byte basis (AC6): on-disk compressed exactly, logical uncompressed from `CompressionInfo.db`. |
| `emit-point.py` | Folds one point's perf CSV + loadgen JSONL + `/proc` deltas into one record. Owns the byte-basis labelling and the client-saturation stamp. |
| `summarize-sweep.py` | Curve statistics: min/median/max dispersion, speedup, marginal efficiency, the three byte bases, the AC5 table. Excludes saturated points. |
| `unsym-check.py` | The AC3 gate: computes the unsymbolized-frame fraction and exits non-zero above threshold. |
| `parse-runqlat.py` | `runqlat-bpfcc` log2 histogram → JSON with bucket-bounded percentiles. |
| `offcpu-fallback.bt` | bpftrace off-CPU collector, used only if `offcputime-bpfcc` is unusable. Same microsecond unit. |
| `selftest.sh` | 36 mechanics checks that need neither the corpus nor a live server. Run this first. |

---

## Verified CPU topology

Read from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`, not assumed. Every
script re-derives it at start and writes `cpu-topology.json` beside its results.

```
Intel(R) Xeon(R) Platinum 8488C — 16 logical CPUs, 8 physical cores, 1 NUMA node (node0)
SMT sibling pairs: (0,8) (1,9) (2,10) (3,11) (4,12) (5,13) (6,14) (7,15)   i.e. (c, c+8)
kernel 6.17.0-1019-aws
```

Server core sets (table-driven in `common.sh`, both SMT siblings of each physical core):

| S | server CPUs | note |
|---|---|---|
| 1 | `2,10` | **exactly reproduces #3100's pinned control** |
| 2 | `0,2,8,10` | |
| 4 | `0-3,8-11` | |
| 6 | `0-5,8-13` | full box less the client cores |

Client is fixed at `6,7,14,15` (2 physical cores) for every S, so the client side is a
constant across the curve. `sweep.sh` **refuses to run** if the two sets overlap — a shared
CPU would make `perf stat -C <server-cpus>` count client work as engine work.

---

## Traps (all of these have already bitten)

**1. The sysctls silently revert.** `kernel.perf_event_paranoid` and `kernel.kptr_restrict`
go back to `4` / `1` on their own schedule. Every script calls `ws0_assert_sysctl` at start,
which re-asserts via `sudo -n sysctl -w` and **dies** if the values did not take. A stale
value shows up as unsymbolized kernel frames — i.e. as an AC3 failure with a misleading cause.

**2. `--call-graph=dwarf` HANGS.** Against the ~143 MB `cqlite-flight` binary, dwarf
unwinding does not finish within 120s. The server is built with
`-C force-frame-pointers=yes` plus `CARGO_PROFILE_RELEASE_STRIP=none
CARGO_PROFILE_RELEASE_DEBUG=true`, so **frame-pointer unwinding** (`-g` / `--call-graph=fp`)
is both correct and fast. Never use dwarf here.

**3. `pkill -f 'cqlite-flight --data-dir'` kills the launching shell.** The pattern matches
the harness's own command line. Nothing in this harness calls `pkill`; `ws0_stop_server`
takes an explicit PID (TERM, 15s grace, then KILL). `selftest.sh` asserts both that the
target dies and that the launching shell survives, and greps the scripts for any `pkill`
call.

**4. The BPF collectors need root.** A permissive `perf_event_paranoid` is not enough — it
governs perf events, not BPF map creation. Unprivileged, bcc fails with
`could not open bpf map: warn_events, error: Operation not permitted` and bpftrace refuses
outright. `offcputime-bpfcc`, `runqlat-bpfcc` and `bpftrace` are all invoked via `sudo -n`
(override with `WS0_SUDO`). `perf` itself does not need sudo once the sysctls are set.

**5. A single long `sleep` records no off-CPU time.** `offcputime` charges a blocked interval
only when the thread is switched back *in*, so a process that never wakes inside the window
contributes nothing. This bit the dry run; it also means an off-CPU window must be long
enough to contain wake-ups of the intervals you care about.

**6. The Part B parsers need `rust_demangler`, and its absence is SILENT-ish.** bcc and bpftrace
emit RAW Rust v0 symbols; `demangle_helper.py` falls back to returning a frame UNCHANGED when the
`rust_demangler` module is missing, so `classify-offcpu-v2.py` still runs, still produces a
plausible-looking table, and **mis-buckets almost everything** (measured while re-deriving these
artefacts: `mpsc_send_park` collapsed 50.57 s -> 2.89 s). Run the Part B parsers with an
interpreter that has it (`/data/ws0/venv-demangle/bin/python` on this box) and sanity-check that
no `_RN`-prefixed symbol survives into the output table.

**7. A corpus still being written misparses.** `corpus-basis.py`'s
`ceil(dataLength/chunkLength) == chunkCount` cross-check legitimately fails mid-flush. It is
a visible warning, not a rejection — but re-measure the basis on the **final** corpus.

---

## The client-saturation validity gate (load-bearing)

If the client pinned set exceeds **70%** busy, the point measured the *loadgen*, not the
engine, and must never be quoted as a server throughput number. So the point is stamped:

```json
"validity": "INVALID_CLIENT_SATURATED",
"client_saturated": true,
"client_saturation_note": "CLIENT SATURATED: ... It MUST NOT be reported as a server throughput measurement."
```

`summarize-sweep.py` **excludes** saturated points from the curve, reports how many were
dropped per N, lists them under a `!!! EXCLUDED - CLIENT SATURATED !!!` banner, and still
records them in `summary.json` under `excluded_points`. Threshold: `WS0_CLIENT_SAT_THRESHOLD`.
Override the exclusion with `--include-saturated` only to inspect, never to report.

If points saturate, the fix is more client cores (and re-running the whole curve with the new
client set), not a higher threshold.

---

## Byte basis (AC6) — never a bare MB/s

Three bases for the same rows, differing by ~3.5x on this corpus. Every throughput field is
emitted separately and paired with a `*_basis` string:

| Field | Meaning |
|---|---|
| `rows_per_s_aggregate` / `rows_per_s_per_stream` | the primary figure |
| `bytes_per_s_logical_uncompressed` | rows/s × logical bytes/row, from `CompressionInfo.db` `dataLength` |
| `bytes_per_s_ondisk_compressed` | rows/s × on-disk bytes/row, from the exact sum of `*-Data.db` sizes |
| `bytes_per_s_arrow_wire_capacity` | `flight-loadgen` `bytes_per_s` — Arrow buffer **capacity**, *not* gRPC-on-the-wire bytes |

Per-row constants are derived from `rows_total / requests_ok` observed in the same step. If a
basis cannot be established the value is `null` and the basis string says why — never a
fabricated number. `WS0_LOGICAL_BYTES_PER_ROW` overrides the logical basis.

Parse validation: on the live ws0 corpus the parser gives logical/on-disk = **3.54x**, against
#3026's independently-derived `692.70 / 195.96` = **3.53x**.

`/proc/<pid>/io` `rchar`, `read_bytes` and `syscr` are three different layers and are reported
raw, side by side. **Never divide one by another.**

---

## Invocations

Common environment (nothing is hardcoded — the corpus comes from a peer agent, the binaries
from this worktree):

```bash
export WT=/home/ubuntu/workspace/repo/.claude/worktrees/issue-3217-fullbox-cn-attribution
export WS0_STAGE=/data/ws0/ws0-corpus/sstables            # staged SSTable dir
export WS0_FLIGHT_BIN=$WT/target/release/cqlite-flight
export WS0_LOADGEN_BIN=$WT/target/release/flight-loadgen
export WS0_TICKET_TPL=$WT/docs/reports/ws0-3100-artifacts/ws0-h2h/ws0-events-template.json
cd $WT/docs/reports/ws0-3217-artifacts/harness
```

Binaries are built (in this worktree only) with:

```bash
CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true \
  RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release -p cqlite-flight -p flight-loadgen > /data/ws0/logs/build.log 2>&1
```

### 0. Self-test first

```bash
./selftest.sh              # 36 checks; --no-bpf to skip the perf/BPF captures
```

### 1. Part A — the C(N) curve

```bash
# S=1 control (reproduces #3100), then the full-box sweep
./sweep.sh cn-s1 s1 6,7,14,15 1,2,4,8,16 120 3 bypass
./sweep.sh cn-s2 s2 6,7,14,15 1,2,4,8,16 120 3 bypass
./sweep.sh cn-s4 s4 6,7,14,15 1,2,4,8,16 120 3 bypass
./sweep.sh cn-s6 s6 6,7,14,15 1,2,4,8,16 120 3 bypass

# merge-arm reference point at N=1
./sweep.sh cn-s6-merge-n1 s6 6,7,14,15 1 120 3 merge
```

Results land in `/data/ws0/results/<label>/`: `points.jsonl`, `summary.json`, `summary.txt`,
`corpus-basis.json`, `cpu-topology.json`, `run-config.json`. Logs (never read into an agent
context) go to `/data/ws0/logs/<label>/`.

`WS0_DRY_RUN=1` validates args, topology, the overlap guard and the corpus basis without
launching a server.

### 2. AC3 — on-CPU flame graphs

```bash
for s in s1 s6; do for n in 1 8 16; do
  ./profile-oncpu.sh oncpu-$s-N$n $s $n 30
done; done
```

Per run in `/data/ws0/profiles/<label>/`: `oncpu.svg`, `oncpu.folded`, `perf.data`,
`perf.script`, `unsym-check.json`. **Both** the SVG and the folded text are retained — the
folded text is what allows re-plotting and differencing later (AC8).

The script **fails loudly** if unsymbolized frames exceed 10%. Three readings are reported
(`frame_weighted`, `sample_fraction_with_any_unknown_frame`, `leaf`) because AC3's "frames …
of samples" is ambiguous; the gated one defaults to `frame_weighted_unsym_fraction` and is
switchable via `unsym-check.py --gate-metric`.

### 3. AC4 + AC5 — off-CPU attribution and scheduler cost

```bash
./profile-offcpu.sh offcpu-s1 s1 1,8,16 30
./profile-offcpu.sh offcpu-s6 s6 1,8,16 30
```

Per N: `offcpu-N<k>.folded`, `offcpu-N<k>.svg`, `offcpu-N<k>.attribution.{json,txt}`,
`runqlat-N<k>.{txt,json}`; plus `scheduler-cost.{jsonl,txt}` across all N.

This is the instrument that can indict or acquit the handoff. An on-CPU profile cannot: a
thread parked on a full bounded channel burns no cycles and is invisible to `perf record`.

---

## The AC4 classifier

Collector: `offcputime-bpfcc -f -p <pid> <dur>` (folded, **microseconds**). Chosen over the
bpftrace fallback because it needed no custom probe, resolved both user and kernel frames on
this kernel, and emits FlameGraph-ready folded output directly. `offcpu-fallback.bt` is used
only if the bcc probe fails; whichever ran is recorded in `run-config.json` and in the table
label, so it is never a guess.

Every stack lands in exactly one bucket. Match is a case-insensitive substring against the
whole stack, **first rule in this order wins**:

| # | Bucket | Matches on |
|---|---|---|
| 1 | `egress_credit_acquire` | `egress_credit`, `egress_permit`, `egress_budget`, `egress_reservation` |
| 2 | `mpsc_send_park` | `ChannelSink`, `stream_subphase`/`GrpcWrite`, `mpsc::bounded::Sender`, `Sender::reserve`, `blocking_send`, `chan::Tx`, `run_merge_catching_panics` |
| 3 | `mpsc_recv_park` | `mpsc::bounded::Receiver`, `ReceiverStream`, `poll_recv`, `recv_many`, `chan::Rx` |
| 4 | `tonic_grpc_socket_write` | `tcp_sendmsg`, `sock_sendmsg`, `sk_stream_wait_memory`, `framed_write`, `poll_flush`/`poll_write`, `tonic::codec::encode`, `h2::proto::streams::send`, `hyper::proto::h2` |
| 5 | `disk_io` | `io_schedule`, `submit_bio`, `blk_mq`, `nvme`, `folio_wait_bit`, `filemap_`, `pread64`, `vfs_read`, `ext4`/`xfs_` |
| 6 | `tokio_scheduler` | `tokio::runtime::scheduler`, `multi_thread::worker`, `blocking::pool`, `Parker`/`park_timeout`, `epoll_wait`, `io::driver`, `condvar` |
| 7 | `other` | everything else → listed in full in the `unclassified` residue |

**Order is the whole design**, because real stacks carry frames from several layers:

- `egress_credit_acquire` **before** `mpsc_send_park`: `ChannelSink::reserve` calls the
  egress-credit reserve, so a park there carries *both* sets of frames. Credit acquisition is
  the more specific cause and must win.
- `mpsc_recv_park` **before** `tonic_grpc_socket_write`: the receiver is polled *by* tonic's
  stream machinery, so a recv park carries tonic/h2 frames too. "gRPC layer idle waiting for a
  batch" is the meaningful attribution, not "socket".
- `disk_io` **before** `tokio_scheduler`: a page-cache miss is a concrete cause; a scheduler
  park is generic.

Deliberately **not** match keys: `futex`, `schedule`, `__schedule`, `finish_task_switch`,
`do_syscall_64`. Those are the *mechanism* of every off-CPU stack, not the reason — matching
on them would swallow the whole profile into one bucket.

Two AC4 rules enforced mechanically, both covered by `selftest.sh`:

- **Every bucket is always emitted**, with `"present": false` and an explicit
  `"blocked_time_us": 0` when absent. A bucket missing from the table is an acceptance
  failure; a measured zero is fine.
- **Nothing is silently swallowed.** Everything in `other` is listed, ranked, in the
  `unclassified` residue so the table can be extended rather than quietly under-counting.

Relevant source for symbol matching: `cqlite-flight/src/streaming.rs` (the
`tokio::sync::mpsc` channel at ~line 386, `DO_GET_CHANNEL_CAPACITY = 4`,
`IN_FLIGHT_ALLOWANCE = 3`, `ChannelSink::emit`'s `Handle::block_on` backpressure park),
`producer_stream.rs`, `metered_stream.rs`, `egress_credit.rs`, `admission.rs`.

---

## Validation status

**Smoke-validated** (`selftest.sh`, 36/36, no corpus needed): sysctl re-assert; topology
derivation; the S-table and CPU-list helpers; the server/client overlap guard; argument
validation and `WS0_DRY_RUN`; `corpus-basis.py` against real Cassandra 5.0 SSTables (3.54x
ratio cross-check); the classifier against a synthetic folded file covering all seven buckets
*and both ordering traps*; explicit-zero emission; residue listing; the unsym gate's PASS and
FAIL paths; curve statistics, dispersion, marginal efficiency, saturation exclusion and
admission flagging; the runqlat parser; record assembly and the validity gate at 0.50 vs 0.90;
server launch, readiness poll and explicit-PID stop (with the launching shell proven to
survive); a real `perf record` → collapse → flamegraph → AC3-gate cycle; and a real
`offcputime` → fold → flamegraph → classify cycle.

**Not validated until the corpus lands** — everything that needs real data or a real server:
end-to-end `sweep.sh` against `cqlite-flight`; whether the loadgen's `--ramp N` steady state
is reached within `WS0_STEADY_PRE_SECS` (20s default); the *actual* unsymbolized fraction of a
real `cqlite-flight` profile (only a `sleep`-workload capture has been gated so far, so the
frame-pointer build has not yet been proven to unwind the server's own Rust stacks in
practice); whether the classifier's match table covers the symbols that really appear (the
residue list is there precisely to show what it missed on the first real run); whether
`--stack-storage-size 32768` is enough for a 16-thread server; and whether the fixed 2-physical-core
client can drive S=6 at N=16 without tripping the saturation gate — **if it cannot, the
full-box points are unmeasurable with this client allocation and the split must be
rebalanced before the curve means anything.**

---

## Environment reference

| Variable | Default | Meaning |
|---|---|---|
| `WS0_STAGE` | *(required)* | staged SSTable dir (`--data-dir`) |
| `WS0_FLIGHT_BIN` / `WS0_LOADGEN_BIN` | *(required)* | binary paths |
| `WS0_TICKET_TPL` | *(required)* | `--ticket-template` JSON |
| `WS0_ROOT` | `/data/ws0` | scratch root (`logs/`, `results/`, `profiles/`, `artifacts/`) |
| `FLAMEGRAPH_DIR` | `/data/ws0/tools/FlameGraph` | FlameGraph scripts |
| `WS0_CLIENT_SAT_THRESHOLD` | `0.70` | client-saturation validity gate |
| `WS0_UNSYM_THRESHOLD` | `0.10` | AC3 unsymbolized-frame gate |
| `WS0_LOGICAL_BYTES_PER_ROW` | *(unset)* | override the logical byte basis |
| `WS0_WARM_SECS` | `45` | warm pre-pass; `0` disables |
| `WS0_STEADY_PRE_SECS` | `20` | settle time before a profile window opens |
| `WS0_SETTLE_SECS` | `5` | idle gap between sweep points |
| `WS0_OFFCPU_TOOL` | `auto` | `auto` \| `bpfcc` \| `bpftrace` |
| `WS0_SUDO` | `sudo -n` | privilege prefix for the BPF tools |
| `WS0_DRY_RUN` | `0` | validate mechanics without a server |
| `WS0_SEED` | `42` | loadgen RNG seed (matches #3100) |

Server flags are fixed to #3100's recorded invocation (`--batch-size 8192`,
`--max-batch-bytes 4194304`, `--max-inflight-egress-bytes 12582912`,
`--max-concurrent-scans 16`, `--admission-wait-timeout-ms 30000`) and are overridable
individually via `WS0_BATCH_SIZE` etc.
