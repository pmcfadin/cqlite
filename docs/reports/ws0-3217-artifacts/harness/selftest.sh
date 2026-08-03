#!/usr/bin/env bash
# Mechanics self-test for the #3217 harness. Validates everything that does NOT
# need the corpus or a live cqlite-flight server, so a real run fails on data,
# never on plumbing.
#
#   ./selftest.sh            # everything (BPF checks included; needs sudo -n)
#   ./selftest.sh --no-bpf   # skip the perf/BPF captures
#
# What it covers:
#   1  sysctl re-assert + verified CPU topology artefact
#   2  CPU-list table/expansion helpers and the server/client overlap guard
#   3  corpus-basis.py against a real SSTable dir (skipped if none is present)
#   4  classify-offcpu.py against a SYNTHETIC folded file that exercises ALL
#      seven AC4 buckets, including the two ordering traps (a ChannelSink stack
#      that must classify as egress_credit_acquire, and a tonic-carrying stack
#      that must classify as mpsc_recv_park)
#   5  unsym-check.py PASS and FAIL paths
#  11  partB-run analysis tools: classify-offcpu-v2, parse-sched-switch, parse-llc-counters
#   6  summarize-sweep.py incl. the client-saturation exclusion and the
#      marginal-efficiency arithmetic
#   7  parse-runqlat.py against a synthetic histogram
#   8  sweep.sh argument validation + dry run
#   9  perf record -> collapse -> flamegraph -> AC3 gate (real capture)
#  10  offcputime/bpftrace -> fold -> flamegraph -> classify (real capture)

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
NO_BPF=0
[ "${1:-}" = "--no-bpf" ] && NO_BPF=1

PASS=0; FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL+1)); }
step() { printf '\n== %s ==\n' "$1"; }

# ---------------------------------------------------------------- 1 + 2
step "1/2  sysctl, topology, cpu-list helpers"
# shellcheck source=common.sh
source "$HERE/common.sh"
ws0_assert_sysctl >/dev/null 2>&1 && ok "sysctl re-assert (perf_event_paranoid=-1, kptr_restrict=0)" \
  || bad "sysctl re-assert"
ws0_verify_topology "$TMP/topo.json" >/dev/null 2>&1
python3 - "$TMP/topo.json" <<'PY' && ok "topology artefact verified from /sys" || bad "topology artefact"
import json,sys
d=json.load(open(sys.argv[1]))
assert d["logical_cpus"]==16 and d["physical_cores"]==8, d
assert d["sibling_pair_rule_observed"]=="(c, c+8)", d
assert [2,10] in d["smt_sibling_pairs"], d
PY
[ "$(ws0_server_cpus_for_s 1)" = "2,10" ]      && ok "S=1 -> 2,10 (reproduces #3100's pinned control)" || bad "S=1 table"
[ "$(ws0_server_cpus_for_s 6)" = "0-5,8-13" ]  && ok "S=6 -> 0-5,8-13 (full box less the client cores)" || bad "S=6 table"
[ "$(ws0_cpulist_count "0-5,8-13")" = "12" ]   && ok "cpu-list count" || bad "cpu-list count"
[ "$(ws0_cpulist_expand "0-2,8")" = "0,1,2,8" ] && ok "cpu-list expand" || bad "cpu-list expand"
(ws0_server_cpus_for_s 3 >/dev/null 2>&1) && bad "unknown S should die" || ok "unknown S rejected"

# ---------------------------------------------------------------- 3
step "3  corpus-basis.py"
STAGE_PROBE=""
for cand in "${WS0_STAGE:-}" /data/ws0/ws0-corpus/sstables /data/ws0/cassandra-data/data/ws0; do
  [ -n "$cand" ] && [ -d "$cand" ] && find "$cand" -name '*-Data.db' -print -quit 2>/dev/null | grep -q . \
    && { STAGE_PROBE="$cand"; break; }
done
if [ -n "$STAGE_PROBE" ]; then
  python3 "$HERE/corpus-basis.py" "$STAGE_PROBE" -o "$TMP/basis.json" >/dev/null 2>&1
  python3 - "$TMP/basis.json" <<'PY' && ok "corpus basis parsed (on-disk exact; logical from CompressionInfo.db)" || bad "corpus basis"
import json,sys
d=json.load(open(sys.argv[1]))
assert d["ondisk_compressed_bytes"]>0, d
assert "arrow_wire_bytes_basis" in d
if d.get("logical_uncompressed_bytes"):
    r=d["logical_uncompressed_bytes"]/d["ondisk_compressed_bytes"]
    assert 1.0<=r<=20.0, "implausible compression ratio %r"%r
    print("      logical/on-disk ratio = %.2fx"%r)
PY
else
  echo "  SKIP  no *-Data.db found; corpus not staged yet"
fi

# ---------------------------------------------------------------- 4
step "4  classify-offcpu.py — all seven AC4 buckets + the two ordering traps"
cat >"$TMP/synth.folded" <<'FOLDED'
cqlite-flight;std::sys::backtrace;cqlite_flight::streaming::ChannelSink::emit;tokio::runtime::park::CachedParkThread::block_on;tokio::sync::mpsc::bounded::Sender<T>::reserve;entry_SYSCALL_64;do_syscall_64;futex_wait;schedule;finish_task_switch 1500000
cqlite-flight;cqlite_flight::streaming::ChannelSink::reserve;cqlite_flight::egress_credit::EgressCredit::acquire;tokio::sync::batch_semaphore::Semaphore::acquire;futex_wait;schedule 800000
cqlite-flight;tonic::server::grpc::Grpc::streaming;h2::proto::streams::send::Send::poll;tokio_stream::wrappers::ReceiverStream::poll_next;tokio::sync::mpsc::bounded::Receiver<T>::poll_recv;schedule 600000
cqlite-flight;tonic::transport::server::Server::serve;h2::codec::framed_write::FramedWrite::flush;__sys_sendto;sock_sendmsg;tcp_sendmsg;sk_stream_wait_memory;schedule 400000
cqlite-flight;cqlite_core::storage::sstable::reader::read_at;__x64_sys_pread64;vfs_read;filemap_read;folio_wait_bit;io_schedule;schedule 250000
cqlite-flight;tokio::runtime::scheduler::multi_thread::worker::run;tokio::runtime::park::Parker::park;epoll_wait;schedule 300000
cqlite-flight;some::unrecognised::path;[unknown];schedule;finish_task_switch 100000
FOLDED
python3 "$HERE/classify-offcpu.py" "$TMP/synth.folded" --label selftest \
  --out-json "$TMP/attr.json" --out-table "$TMP/attr.txt" >/dev/null
python3 - "$TMP/attr.json" <<'PY' && ok "all 7 buckets present with the expected attribution" || bad "bucket attribution"
import json,sys
d=json.load(open(sys.argv[1]))
got={b["bucket"]:b["blocked_time_us"] for b in d["buckets"]}
want={"mpsc_send_park":1500000,"egress_credit_acquire":800000,"mpsc_recv_park":600000,
      "tonic_grpc_socket_write":400000,"disk_io":250000,"tokio_scheduler":300000,"other":100000}
assert set(got)==set(want), (sorted(got),sorted(want))
for k,v in want.items():
    assert got[k]==v, "bucket %s: got %d want %d"%(k,got[k],v)
assert d["total_blocked_time_us"]==sum(want.values())
PY
python3 - "$TMP/attr.json" <<'PY' && ok "ordering trap: ChannelSink::reserve -> egress_credit_acquire, not mpsc_send_park" || bad "egress-before-send ordering"
import json,sys
d=json.load(open(sys.argv[1]))
r=[x for x in d["ranked_stacks"] if "EgressCredit" in x["stack"]][0]
assert r["bucket"]=="egress_credit_acquire", r["bucket"]
PY
python3 - "$TMP/attr.json" <<'PY' && ok "ordering trap: tonic-carrying recv stack -> mpsc_recv_park, not socket write" || bad "recv-before-tonic ordering"
import json,sys
d=json.load(open(sys.argv[1]))
r=[x for x in d["ranked_stacks"] if "ReceiverStream" in x["stack"]][0]
assert r["bucket"]=="mpsc_recv_park", r["bucket"]
PY
python3 - "$TMP/attr.json" <<'PY' && ok "residue listed, nothing silently swallowed" || bad "residue"
import json,sys
d=json.load(open(sys.argv[1]))
assert d["unclassified"]["unique_stacks"]==1 and d["unclassified"]["stacks"], d["unclassified"]
PY
# an empty bucket must still be emitted as an explicit zero (AC4)
head -1 "$TMP/synth.folded" >"$TMP/one.folded"
python3 "$HERE/classify-offcpu.py" "$TMP/one.folded" --out-json "$TMP/one.json" >/dev/null
python3 - "$TMP/one.json" <<'PY' && ok "absent buckets emitted as EXPLICIT ZERO (AC4)" || bad "explicit zero"
import json,sys
d=json.load(open(sys.argv[1]))
z=[b for b in d["buckets"] if not b["present"]]
assert len(z)==6, len(z)
assert all(b["blocked_time_us"]==0 and b["absent_note"] for b in z)
PY

# ---------------------------------------------------------------- 5
step "5  unsym-check.py gate"
printf 'a;b;c 100\n' >"$TMP/clean.folded"
python3 "$HERE/unsym-check.py" "$TMP/clean.folded" --out "$TMP/u1.json" >/dev/null 2>&1 \
  && ok "clean folded -> PASS (exit 0)" || bad "clean folded should pass"
printf 'a;[unknown];0x7f9a1b2c3d4e 100\n' >"$TMP/dirty.folded"
python3 "$HERE/unsym-check.py" "$TMP/dirty.folded" --out "$TMP/u2.json" >/dev/null 2>&1 \
  && bad "dirty folded should FAIL the AC3 gate" || ok "dirty folded -> FAIL (exit 1), AC3 gate bites"
python3 - "$TMP/u2.json" <<'PY' && ok "hex addresses and [unknown] both counted unsymbolized" || bad "unsym detection"
import json,sys
d=json.load(open(sys.argv[1]))
assert abs(d["frame_weighted_unsym_fraction"]-2/3)<1e-9, d["frame_weighted_unsym_fraction"]
assert d["verdict"]=="FAIL"
PY

# ---------------------------------------------------------------- 6
step "6  summarize-sweep.py"
python3 - "$TMP/points.jsonl" <<'PY'
import json,sys
recs=[]
def pt(n,rep,rows,cli,sat=False,unavail=0):
    return {"schema":"ws0-3217.sweep-point/v1","label":"selftest","ts_unix_ms":0,
     "server_physical_cores_S":6,"server_cpus":"0-5,8-13","client_cpus":"6,7,14,15",
     "merge_path":"bypass","target_concurrency_N":n,"rep":rep,
     "rows_per_s_aggregate":rows,"rows_per_s_per_stream":rows/n,
     "client_saturated":sat,"client_cpu_utilization_of_pinned_set":cli,
     "admission_clean":unavail==0,"requests_unavailable":unavail,
     "bytes_per_s_logical_uncompressed":rows*700.0,
     "bytes_per_s_ondisk_compressed":rows*198.0,
     "bytes_per_s_arrow_wire_capacity":rows*900.0,
     "server_cpu_utilization_of_pinned_set":0.5,"IPC":1.4,
     "context_switches_per_second_cpu_wide":1000.0*n,
     "server_voluntary_ctxt_switches_per_s":10.0*n,
     "server_nonvoluntary_ctxt_switches_per_s":1.0*n,
     "latency_ms":{"p50":10.0,"p95":11.0,"p99":12.0,"max":13.0,"samples":5}}
for rep,rows in ((1,1000.0),(2,1100.0),(3,1050.0)): recs.append(pt(1,rep,rows,0.30))
for rep,rows in ((1,3000.0),(2,3200.0),(3,3100.0)): recs.append(pt(8,rep,rows,0.40))
# one saturated point that MUST be excluded, and one admission-shed point
recs.append(pt(16,1,9999.0,0.95,sat=True))
recs.append(pt(16,2,3500.0,0.50,unavail=7))
recs.append(pt(16,3,3400.0,0.50))
open(sys.argv[1],"w").write("".join(json.dumps(r)+"\n" for r in recs))
PY
python3 "$HERE/summarize-sweep.py" "$TMP/points.jsonl" --out-json "$TMP/sum.json" --out-table "$TMP/sum.txt" >/dev/null
python3 - "$TMP/sum.json" <<'PY' && ok "curve stats, dispersion, marginal efficiency" || bad "curve stats"
import json,sys
d=json.load(open(sys.argv[1]))
c={r["N"]:r for r in d["curve"]}
assert c[1]["rows_per_s_aggregate_median"]==1050.0, c[1]
assert c[1]["rows_per_s_aggregate_min"]==1000.0 and c[1]["rows_per_s_aggregate_max"]==1100.0
assert c[8]["reps"]==3 and c[8]["rows_per_s_aggregate_median"]==3100.0
# marginal efficiency = rows(N) / (N * rows(1)) = 3100 / (8*1050)
assert abs(c[8]["marginal_efficiency_vs_linear"]-3100.0/(8*1050.0))<1e-9
assert abs(c[8]["speedup_vs_N1"]-3100.0/1050.0)<1e-9
PY
python3 - "$TMP/sum.json" <<'PY' && ok "client-saturated point EXCLUDED from the curve and listed" || bad "saturation exclusion"
import json,sys
d=json.load(open(sys.argv[1]))
c={r["N"]:r for r in d["curve"]}
assert c[16]["reps"]==2, "saturated rep must not be in the curve"
assert c[16]["excluded_client_saturated_reps"]==1
assert d["points_excluded_client_saturated"]==1
assert d["excluded_points"][0]["rows_per_s_aggregate"]==9999.0
PY
python3 - "$TMP/sum.json" <<'PY' && ok "admission shed flagged (AC1 asserts requests_unavailable == 0)" || bad "admission flag"
import json,sys
d=json.load(open(sys.argv[1]))
assert d["admission_clean_all_points"] is False
assert d["admission_violations"][0]["requests_unavailable"]==7
PY
grep -q "CLIENT SATURATED" "$TMP/sum.txt" && ok "saturation is impossible to miss in the text table" || bad "saturation banner"
grep -q "logical-uncomp B/s" "$TMP/sum.txt" && grep -q "on-disk-comp B/s" "$TMP/sum.txt" \
  && grep -q "arrow-wire-cap B/s" "$TMP/sum.txt" \
  && ok "AC6: three named byte bases in the table, no bare MB/s" || bad "byte-basis table"

# ---------------------------------------------------------------- 7
step "7  parse-runqlat.py"
cat >"$TMP/runqlat.txt" <<'RQ'
     usecs               : count     distribution
         0 -> 1          : 10       |****    |
         2 -> 3          : 80       |********|
         4 -> 7          : 10       |****    |
RQ
python3 "$HERE/parse-runqlat.py" "$TMP/runqlat.txt" --out "$TMP/rq.json" >/dev/null
python3 - "$TMP/rq.json" <<'PY' && ok "log2 histogram parsed; percentiles reported as bucket intervals" || bad "runqlat parse"
import json,sys
d=json.load(open(sys.argv[1]))
assert d["total_wakeup_events"]==100, d
assert d["p50_bucket_usecs"]==[2,3], d
assert d["p99_bucket_usecs"]==[4,7], d
PY

# ---------------------------------------------------------------- 8
step "8  sweep.sh argument validation + dry run"
(WS0_DRY_RUN=1 "$HERE/sweep.sh" bad s1 6,7,14,15 1,2 10 3 nonsense >/dev/null 2>&1) \
  && bad "invalid merge-path should be rejected" || ok "invalid merge-path rejected"
(WS0_DRY_RUN=1 "$HERE/sweep.sh" bad s1 6,7,14,15 1,x 10 3 >/dev/null 2>&1) \
  && bad "malformed ramp should be rejected" || ok "malformed ramp rejected"
(WS0_DRY_RUN=1 "$HERE/sweep.sh" bad "2,10" "2,10" 1 10 3 >/dev/null 2>&1) \
  && bad "overlapping server/client sets should be rejected" || ok "server/client CPU overlap rejected"
(WS0_STAGE=/nonexistent WS0_DRY_RUN=1 "$HERE/sweep.sh" bad s1 6,7,14,15 1 10 3 >/dev/null 2>&1) \
  && bad "missing stage should be rejected" || ok "missing WS0_STAGE rejected (never hardcoded)"
if [ -n "$STAGE_PROBE" ] && [ -x "${WS0_FLIGHT_BIN:-/nonexistent}" ] && [ -x "${WS0_LOADGEN_BIN:-/nonexistent}" ] \
   && [ -f "${WS0_TICKET_TPL:-/nonexistent}" ]; then
  (WS0_DRY_RUN=1 WS0_STAGE="$STAGE_PROBE" "$HERE/sweep.sh" selftest-dry s6 6,7,14,15 1,2,4 10 3 bypass >/dev/null 2>&1) \
    && ok "sweep.sh dry run (topology + overlap + basis, no server)" || bad "sweep.sh dry run"
else
  echo "  SKIP  sweep.sh dry run: set WS0_STAGE/WS0_FLIGHT_BIN/WS0_LOADGEN_BIN/WS0_TICKET_TPL"
fi

# ---------------------------------------------------------------- 8b
step "8b  emit-point.py record assembly"
cat >"$TMP/perf.csv" <<'CSV'
# started on Sat Aug  2 00:00:00 2026
1000000000,,cycles,2000000000,100.00,,
2000000000,,instructions,2000000000,100.00,2.00,insn per cycle
50000,,context-switches,2000000000,100.00,,
CSV
cat >"$TMP/step.jsonl" <<'JL'
{"schema":"flight-loadgen.step/v1","round":"t","step":1,"target_concurrency":8,"shape":"full","seed":42,"duration_s":10.0,"requests_ok":4,"requests_unavailable":0,"requests_error":0,"error_codes":{},"qps":0.4,"rows_per_s":40000.0,"bytes_per_s":36000000.0,"rows_total":400000,"bytes_total":360000000,"latency_ms":{"p50":9000.0,"p95":9500.0,"p99":9900.0,"max":10000.0,"samples":4}}
JL
mk_ctx() { python3 - "$1" "$2" <<PY
import json,sys
json.dump({"label":"t","ts_unix_ms":0,"harness_commit":"deadbeef","server_physical_cores_S":6,
 "server_cpus":"0-5,8-13","server_cpu_count":12,"client_cpus":"6,7,14,15","client_cpu_count":4,
 "merge_path":"bypass","N":8,"rep":1,"reps_total":3,"step_seconds":10,"server_flags":"--x",
 "wall_secs":10.0,"server_cpu_secs_delta":60.0,"client_cpuset_busy_secs_delta":$2,
 "client_saturation_threshold":0.70,
 "server_io_delta":{"rchar":123,"read_bytes":456,"syscr":7},
 "server_ctxt_delta":{"voluntary_ctxt_switches":100,"nonvoluntary_ctxt_switches":20},
 "corpus_basis":{"ondisk_compressed_bytes":19600000,"logical_uncompressed_bytes":69270000},
 "logical_bytes_per_row_override":None}, open(sys.argv[1],"w"))
PY
}
mk_ctx "$TMP/ctx-ok.json" 20.0        # 20/10/4 = 0.50 utilisation -> valid
mk_ctx "$TMP/ctx-sat.json" 36.0       # 36/10/4 = 0.90 utilisation -> SATURATED
: >"$TMP/pts.jsonl"
python3 "$HERE/emit-point.py" --perf-csv "$TMP/perf.csv" --step-jsonl "$TMP/step.jsonl" \
  --context-json "$TMP/ctx-ok.json" --out "$TMP/pts.jsonl" >/dev/null
python3 "$HERE/emit-point.py" --perf-csv "$TMP/perf.csv" --step-jsonl "$TMP/step.jsonl" \
  --context-json "$TMP/ctx-sat.json" --out "$TMP/pts.jsonl" >/dev/null
python3 - "$TMP/pts.jsonl" <<'PY' && ok "throughput, IPC, per-row counters, three byte bases" || bad "record assembly"
import json,sys
a,b=[json.loads(l) for l in open(sys.argv[1])]
assert a["rows_per_s_aggregate"]==40000.0 and a["rows_per_s_per_stream"]==5000.0
assert abs(a["IPC"]-2.0)<1e-9 and abs(a["cycles_per_row"]-2500.0)<1e-9
assert a["rows_per_scan_observed"]==100000.0
# 19_600_000 on-disk / 100_000 rows = 196 B/row ; x 40000 rows/s
assert abs(a["bytes_per_s_ondisk_compressed"]-40000.0*196.0)<1e-6, a["bytes_per_s_ondisk_compressed"]
assert abs(a["bytes_per_s_logical_uncompressed"]-40000.0*692.7)<1e-6
assert a["bytes_per_s_arrow_wire_capacity"]==36000000.0
assert "CAPACITY" in a["bytes_per_s_arrow_wire_capacity_basis"]
assert not any(k.endswith("MB_per_s") for k in a), "AC6: no bare MB/s field allowed"
assert a["server_io_delta"]=={"rchar":123,"read_bytes":456,"syscr":7}
assert a["server_voluntary_ctxt_switches"]==100 and a["server_nonvoluntary_ctxt_switches"]==20
assert a["context_switches_per_second_cpu_wide"]==5000.0
assert a["admission_clean"] is True
PY
python3 - "$TMP/pts.jsonl" <<'PY' && ok "validity gate: 0.50 -> OK, 0.90 -> INVALID_CLIENT_SATURATED" || bad "validity gate"
import json,sys
a,b=[json.loads(l) for l in open(sys.argv[1])]
assert abs(a["client_cpu_utilization_of_pinned_set"]-0.5)<1e-9
assert a["client_saturated"] is False and a["validity"]=="OK"
assert abs(b["client_cpu_utilization_of_pinned_set"]-0.9)<1e-9
assert b["client_saturated"] is True and b["validity"]=="INVALID_CLIENT_SATURATED"
assert "MUST NOT be reported as a server" in b["client_saturation_note"]
PY

# ---------------------------------------------------------------- 8c
step "8c  server launch / readiness poll / explicit-PID stop (the pkill trap)"
STUB="$TMP/stub-flight"
cat >"$STUB" <<'STUBEOF'
#!/usr/bin/env bash
# Stands in for `cqlite-flight --data-dir ...`: accepts and ignores the real
# flag set, then listens so the readiness poll can succeed.
port=8815
while [ $# -gt 0 ]; do case "$1" in --listen) port="${2##*:}"; shift 2;; *) shift;; esac; done
exec python3 -c "
import socket,sys,time
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('127.0.0.1',int(sys.argv[1]))); s.listen(16)
while True:
    try: c,_=s.accept(); c.close()
    except Exception: time.sleep(0.1)
" "$port"
STUBEOF
chmod +x "$STUB"
export WS0_STAGE="${STAGE_PROBE:-$TMP}" WS0_FLIGHT_BIN="$STUB" \
       WS0_LOADGEN_BIN="$STUB" WS0_TICKET_TPL="$TMP/topo.json" \
       WS0_LISTEN_PORT=18815
MY_PID=$$
if ws0_start_server "2,10" bypass "$TMP/stub.log" >/dev/null 2>&1; then
  ok "server launched under taskset and readiness poll succeeded (pid $WS0_SERVER_PID)"
  STUB_PID="$WS0_SERVER_PID"
  ws0_stop_server >/dev/null 2>&1
  kill -0 "$STUB_PID" 2>/dev/null && bad "ws0_stop_server left the server alive" \
    || ok "ws0_stop_server killed the target PID"
  kill -0 "$MY_PID" 2>/dev/null && ok "TRAP CHECK: the launching shell SURVIVED (no pkill -f)" \
    || bad "the launching shell was killed"
  # Real usage only: comment lines documenting the trap are expected and fine.
  if grep -vh '^[[:space:]]*#' "$HERE"/common.sh "$HERE"/sweep.sh \
        "$HERE"/profile-oncpu.sh "$HERE"/profile-offcpu.sh | grep -q 'pkill'; then
    bad "a script still CALLS pkill (it matches and kills the launching shell)"
  else
    ok "no pkill call anywhere in the harness (explicit PIDs only)"
  fi
else
  bad "server launch / readiness poll"
fi
unset WS0_LISTEN_PORT

# ---------------------------------------------------------------- 9 + 10
if [ "$NO_BPF" = "1" ]; then
  echo; echo "== 9/10  perf + BPF captures: SKIPPED (--no-bpf) =="
else
  step "9  perf record -> stackcollapse -> flamegraph -> AC3 gate (real 2s capture)"
  (WS0_DRY_RUN=1 "$HERE/profile-oncpu.sh" selftest-oncpu s1 1 2 >/dev/null 2>&1) \
    && ok "on-CPU pipeline end to end (fp unwinding, never dwarf)" || bad "on-CPU pipeline"
  [ -s /data/ws0/profiles/selftest-oncpu/oncpu.svg ] && [ -s /data/ws0/profiles/selftest-oncpu/oncpu.folded ] \
    && ok "BOTH the SVG and the folded text retained (AC8)" || bad "artefact retention"

  step "10  offcputime -> fold -> flamegraph -> classify (real 5s capture)"
  (WS0_DRY_RUN=1 "$HERE/profile-offcpu.sh" selftest-offcpu s1 1 4 >/dev/null 2>&1) \
    && ok "off-CPU pipeline end to end" || bad "off-CPU pipeline"
  [ -s /data/ws0/profiles/selftest-offcpu/offcpu-dryrun.folded ] \
    && ok "off-CPU folded capture non-empty (collector reachable under sudo)" || bad "off-CPU capture empty"
fi

# ---------------------------------------------------------------- 11
# P7: sections 1-10 cover the harness/ tools only. The Part B ANALYSIS tools
# (partB-run/) shipped untested, including the two that produce the acquittal:
# classify-offcpu-v2.py and parse-sched-switch.py. These are smoke tests against
# synthetic inputs with KNOWN answers - no corpus, no server, no BPF.
step "11  partB-run analysis tools (classify-offcpu-v2, parse-sched-switch, parse-llc-counters)"
PB="$HERE/../partB-run"
TD="$(mktemp -d)"; trap 'rm -rf "$TD"' EXIT

# --- classify-offcpu-v2: channel identity + explicit zero + named residue -------
cat > "$TD/v2.folded" <<'EOF'
tokio-rt-worker;thread_start;tokio::future::block_on::block_on::<<tokio::sync::mpsc::bounded::Sender<bytes::bytes::Bytes>>::send> 1000
tokio-rt-worker;thread_start;pread64;tokio::future::block_on::block_on::<<tokio::sync::mpsc::bounded::Sender<bytes::bytes::Bytes>>::send> 2000
tokio-rt-worker;thread_start;<std::sync::mpmc::Sender<cqlite_core::storage::sstable::reader::data_access::summary_scan::query_rows::QueryRowMsg>>::send 3000
tokio-rt-worker;thread_start;__lll_lock_wait_private;_int_malloc 4000
tokio-rt-worker;thread_start;some::totally::unmatched::frame 5000
EOF
if python3 "$PB/classify-offcpu-v2.py" "$TD/v2.folded" --already-demangled \
     --label selftest --out-json "$TD/v2.json" >/dev/null 2>&1; then
  python3 - "$TD/v2.json" <<'PY2' && ok "classify-offcpu-v2: buckets, channel identity, explicit zero, named residue" \
    || bad "classify-offcpu-v2 smoke"
import json,sys
d=json.load(open(sys.argv[1]))
b={x["bucket"]:x["blocked_time_us"] for x in d["buckets"]}
o={x["cause"]:x["blocked_time_us"] for x in d["other_breakdown"]}
c={x["channel"]:x["blocked_time_us"] for x in d["channel_identity"]["channels"]}
assert b["mpsc_send_park"]==6000, b            # incl. the pread64 stack: LEAF-FIRST wins
assert b["disk_io"]==0, b                      # the pread64 stack must NOT land here
assert b["egress_credit_acquire"]==0, b        # explicit zero, present in the table
assert o["glibc_malloc_arena_lock"]==4000, o
assert o["unclassified_residual"]==5000, o     # residue LABELLED, not dropped
assert c["core_raw_chunk"]==3000 and c["core_query_rows"]==3000, c
assert c["do_get_batch"]==0, c                 # the accused: explicit zero
PY2
else
  bad "classify-offcpu-v2 failed to run"
fi

# --- parse-sched-switch: --from-folded re-derivation + EXPLICIT ZERO sites ------
cat > "$TD/sched.folded" <<'EOF'
tokio-rt-worker;thread_start;send;Sender<bytes::bytes::Bytes> 300
tokio-rt-worker;thread_start;alloc;__lll_lock_wait 100
EOF
if python3 "$PB/parse-sched-switch.py" --from-folded "$TD/sched.folded" --involuntary 7 \
     --window-secs 10 --rows-per-s 81920 --label selftest \
     --out-json "$TD/sched.json" >/dev/null 2>&1; then
  python3 - "$TD/sched.json" <<'PY2' && ok "parse-sched-switch: --from-folded exact, unobserved sites emit EXPLICIT ZERO" \
    || bad "parse-sched-switch smoke"
import json,sys
d=json.load(open(sys.argv[1]))
s={x["site"]:x for x in d["sites"]}
assert d["voluntary"]==400 and d["involuntary"]==7, d
assert s["core_raw_chunk_chan"]["parks"]==300
assert s["glibc_malloc_arena_lock"]["parks"]==100
# The point of P4: the accused site is PRESENT in the artefact, as a measured zero.
assert "do_get_mpsc_handoff" in s, sorted(s)
assert s["do_get_mpsc_handoff"]["parks"]==0
assert s["do_get_mpsc_handoff"]["present"] is False
assert s["egress_credit"]["parks"]==0
# 400 parks / (81920/8192 = 10 batches/s * 10 s) = 4 per batch
assert abs(s["core_raw_chunk_chan"]["parks_per_flight_batch"]-3.0)<1e-9, s
PY2
else
  bad "parse-sched-switch failed to run"
fi

# --- parse-llc-counters: <not supported> must be null, never 0 -----------------
mkdir -p "$TD/ctr"
cat > "$TD/ctr/llc-x-N1.perf-stat.csv" <<'EOF'
# started on selftest
1000000,,cycles,20000000000,100.00,,
2000000,,instructions,20000000000,100.00,,
<not supported>,,LLC-load-misses,0,100.00,,
40000,,L1-dcache-loads,20000000000,100.00,,
400,,L1-dcache-load-misses,20000000000,100.00,,
200,,dTLB-load-misses,20000000000,100.00,,
20000000000,,task-clock,20000000000,100.00,,
EOF
echo '{"rows_per_s": 50.0}' > "$TD/ctr/llc-x-N1.step.jsonl"
cat > "$TD/ctr/llc-capture-config.json" <<'EOF'
{"captures":{"x-N1":{"window_secs":20,"server_hw_threads":1}}}
EOF
if python3 "$PB/parse-llc-counters.py" "$TD/ctr" --out-json "$TD/llc.json" >/dev/null 2>&1; then
  python3 - "$TD/llc.json" <<'PY2' && ok "parse-llc-counters: per-row from committed CSV; unsupported counter is NULL not 0" \
    || bad "parse-llc-counters smoke"
import json,sys
d=json.load(open(sys.argv[1]))["llc-x-N1"]
assert d["instr_per_row"]==2000.0, d          # 2e6 / (50 rows/s * 20 s = 1000 rows)
assert d["cycles_per_row"]==1000.0, d
assert abs(d["ipc"]-2.0)<1e-12, d
assert d["llc_miss_per_row"] is None, d       # NEVER 0.0 for an unprogrammable counter
assert "LLC-load-misses" in d["unsupported_counters"], d
assert abs(d["window_secs_derived_from_task_clock"]-20.0)<1e-9, d
PY2
else
  bad "parse-llc-counters failed to run"
fi

printf '\n==== #3217 HARNESS SELFTEST: %d passed, %d failed ====\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
