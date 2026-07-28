#!/usr/bin/env bash
echo $$ > /home/ubuntu/ws0/ws0-h2h/pid
exec taskset -c 2 /home/ubuntu/ws0/ws0-cqlite/harness-target/release/ws0-scan-harness \
  --datasets-root /home/ubuntu/ws0/ws0-h2h/datasets --stage-dir /home/ubuntu/ws0/ws0-h2h/datasets/sstables \
  --keyspace ws0 --table events --schema /home/ubuntu/ws0/ws0-h2h/schemas/ws0-events.cql \
  --mode scan --passes 1 --no-fold
