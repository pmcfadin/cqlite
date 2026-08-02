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

printf '\n==== #3217 HARNESS SELFTEST: %d passed, %d failed ====\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
