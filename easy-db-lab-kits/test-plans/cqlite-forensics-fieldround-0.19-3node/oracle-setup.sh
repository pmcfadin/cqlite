#!/bin/bash
# per-node offline oracle environment: yaml pointing at a symlink farm over the read-only /snap copy
for n in 0 1 2; do
  R=/work/oracle/db$n; mkdir -p $R/data/fr $R/commitlog $R/saved_caches $R/hints $R/cdc_raw $R/conf
  cp -a /etc/cassandra/. $R/conf/ 2>/dev/null
  ln -sfn /work/oracle/data/system_schema $R/data/system_schema
  for d in /snap/db$n/fr/*; do ln -sfn $d $R/data/fr/$(basename $d); done
  sed -i "s|^# *data_file_directories:.*|data_file_directories: [$R/data]|; s|^# *commitlog_directory:.*|commitlog_directory: $R/commitlog|; s|^# *saved_caches_directory:.*|saved_caches_directory: $R/saved_caches|; s|^# *hints_directory:.*|hints_directory: $R/hints|; s|^# *cdc_raw_directory:.*|cdc_raw_directory: $R/cdc_raw|" $R/conf/cassandra.yaml
  grep -n "^data_file_directories\|^commitlog_directory\|^storage_compat" $R/conf/cassandra.yaml
done
