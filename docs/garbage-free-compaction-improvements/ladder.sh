#!/bin/zsh
# Full differential ladder, sequential. Journal-mandated quirks handled:
# - accord build race: rm -rf modules/accord/accord-core/build/libs between ant runs
# Logs: build/tmp/ladder/<ShortName>.log ; summary at summary.txt
cd /Users/jhaddad/dev/cassandra || exit 1
LOGDIR=build/tmp/ladder
mkdir -p "$LOGDIR"
SUMMARY="$LOGDIR/summary.txt"
: > "$SUMMARY"

SUITES=(
  org.apache.cassandra.db.DeletionTimeTest
  org.apache.cassandra.db.compaction.CursorCellPathOrderingTest
  org.apache.cassandra.db.compaction.CursorCounterContextMergeTest
  org.apache.cassandra.io.sstable.CursorColumnsSubsetEncodingTest
  org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiCounterDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.CursorSupportMatrixTest
  org.apache.cassandra.db.compaction.differential.BasicDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.EdgeCaseDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.MultiOutputDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.PartialSetDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.PurgeBoundaryDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.HarryDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.MaterializedViewDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiMaterializedViewDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.AccordTableDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.FarFutureDeletionDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.DroppedColumnDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiDroppedColumnDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.ComplexColumnCursorReadTest
  org.apache.cassandra.db.compaction.differential.BtiCursorReadTest
  org.apache.cassandra.db.compaction.differential.CursorCompactionAllocationGateTest
  org.apache.cassandra.db.compaction.differential.BtiCursorCompactionAllocationGateTest
  org.apache.cassandra.db.compaction.differential.RandomDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiRandomDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.PathologicalWideTableDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiPathologicalWideTableDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BigVolumeDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiBigVolumeDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.LargePartitionDifferentialCompactionTest
  org.apache.cassandra.db.compaction.differential.BtiLargePartitionDifferentialCompactionTest
)

overall=0
for s in "${SUITES[@]}"; do
  short="${s##*.}"
  rm -rf modules/accord/accord-core/build/libs
  start=$(date +%s)
  ant testsome -Dtest.name="$s" > "$LOGDIR/$short.log" 2>&1
  rc=$?
  end=$(date +%s)
  # ant exit code can be green while junit recorded failures; check the log too
  if [ $rc -eq 0 ] && ! grep -a -E -q "(Failures: [1-9]|Errors: [1-9]|BUILD FAILED)" "$LOGDIR/$short.log"; then
    echo "PASS  $short  ($((end-start))s)" >> "$SUMMARY"
  else
    echo "FAIL  $short  ($((end-start))s)  rc=$rc" >> "$SUMMARY"
    overall=1
  fi
done

echo "---" >> "$SUMMARY"
if [ $overall -eq 0 ]; then echo "LADDER GREEN: ${#SUITES[@]}/${#SUITES[@]} suites" >> "$SUMMARY"; else echo "LADDER HAS FAILURES" >> "$SUMMARY"; fi
exit $overall
