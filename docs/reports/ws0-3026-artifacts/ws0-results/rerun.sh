#!/usr/bin/env bash
# WS0 (#3026) disk + memory bandwidth baseline -- re-runnable method.
# Box of record: c7i.4xlarge, 16 vCPU / 8 physical cores, Xeon Platinum 8488C (Sapphire Rapids),
# 30 GiB RAM, kernel 6.17.0-1019-aws. ROOT EBS VOLUME (nvme0n1, 150 GiB, mounted /).
# THERE IS NO LOCAL NVMe INSTANCE STORE ON THIS BOX. All disk numbers are EBS.
# Owner constraint: stay off /data (nvme1n1). All work under /home/ubuntu/ws0/.
set -euo pipefail
R=/home/ubuntu/ws0/ws0-results
T=/home/ubuntu/ws0/fio-test/testfile
mkdir -p "$R" /home/ubuntu/ws0/fio-test
cd "$R"

# ---------- Task 1: fio, READ-ONLY, O_DIRECT ----------
# 48 GiB file: > the 30 GiB of RAM, so even without O_DIRECT the page cache cannot
# hold it; we ALSO pass --direct=1. Belt and braces. This is a plain file write to a
# mounted filesystem -- we never issue a write/trim workload against the raw device.
[ -f "$T" ] || fio --name=layout --filename="$T" --rw=write --bs=1M --size=48G \
  --direct=1 --iodepth=32 --ioengine=libaio --end_fsync=1 \
  --output-format=json > 00-layout.json

COMMON="--filename=$T --direct=1 --ioengine=libaio --time_based --ramp_time=5 \
--group_reporting --output-format=json --log_avg_msec=1000 --percentile_list=50:90:99:99.9"
run(){ fio --name="$1" $COMMON --rw=$2 --bs=$3 --iodepth=$4 --runtime=$5 \
        --write_bw_log="bwlog-$1" --write_lat_log="latlog-$1" --output="fio-$1.json" >/dev/null; }

run randread-4k-qd1   randread 4k 1  300   # CQLite's real pattern (~4.4 KB reads at QD1). 300s => burst-decay check.
run randread-1M-qd32  randread 1M 32 120   # the arm prior work used as its denominator
run seqread-4k-qd1    read     4k 1  90
run seqread-1M-qd32   read     1M 32 120
python3 parse.py

# ---------- Task 2: memory bandwidth ----------
# 8 physical cores == logical CPUs 0-7 (SMT siblings are 8-15); verify with `lscpu -p`.
gcc -O3 -march=native -fopenmp -o ws0-stream  stream_bench.c
gcc -O3 -march=native -fopenmp -o ws0-readbw  read_bw.c
for N in 1 2 4 6 8; do
  CPUS=$(seq -s, 0 $((N-1)))
  echo "== ${N}T on physical cores ${CPUS} =="
  OMP_NUM_THREADS=$N OMP_PROC_BIND=true OMP_PLACES=cores taskset -c "$CPUS" ./ws0-stream 512 12
  OMP_NUM_THREADS=$N OMP_PROC_BIND=true OMP_PLACES=cores taskset -c "$CPUS" ./ws0-readbw 2048 10 | tail -1
done
echo "== 16 logical (SMT included, still 8 physical cores) =="
OMP_NUM_THREADS=16 OMP_PROC_BIND=true taskset -c 0-15 ./ws0-stream 512 12
OMP_NUM_THREADS=16 OMP_PROC_BIND=true taskset -c 0-15 ./ws0-readbw 2048 10 | tail -1
