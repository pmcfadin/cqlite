# Raw reconciliation decisions

Issue #4193 uses these two uncompressed BIG generations to retain both a losing
cell and the later mutation that hides it. Cassandra wrote each generation in a
separate flush with autocompaction disabled. No compaction ran. Existing parity
goldens were not regenerated.

Generate with `bash test-data/scripts/generate-explain-trace.sh`. The schema is
`test-data/schemas/explain-trace.cql`. Each table directory retains the exact
mutations, Cassandra version and image digest, `sstabledump` JSONL,
`sstablemetadata` output, and SHA-256 inventory. The generator is Cassandra 5.0.2;
format and reconciliation authority is the `cassandra-5.0.8` tag, commit
`bc1db9ce043cc13a1b2fe9d3f4e2c7ad7e552021`.

For the committed fixture, generation A is `nb-1-big` and B is `nb-2-big`.
Ordinary cells use writetime 1000 in A and 2000 in B. B's tombstones have local
deletion time **1789963135**; `gc_grace_seconds` is **864000**. Evaluate ordinary
cases at `now=1789963136`. Expected decisions below come from the mutations and
Cassandra rules, not from CQLite output.

| Partition | Cell/version | Expected decision |
|---|---|---|
| 1 | A `v='older'` / B `v='newer'` | A shadowed by timestamp; B wins |
| 2 | A `v='row-shadowed'` | Shadowed by B's row deletion at 2000 |
| 3 | A ck=1 `v='range-shadowed'` | Shadowed by B's inclusive range [1,2] at 2000 |
| 3 | A ck=3 `v='outside-range'` | Winner outside that range |
| 4 | A `v='equal-timestamp'` | Shadowed by B's cell tombstone at equal timestamp 1000 |
| 5 | A `v='partition-shadowed'` | Shadowed by B's partition deletion at 2000 |
| 6 | A `m['old']=1` | Shadowed by B's complex deletion at 1999 |
| 6 | B `m['new']=2` | Winner at 2000 |
| 7 | A `v='expires'` | Expired at `now=1789966735`, after stored expiry 1789966734 |
| 8 | B cell tombstone | Retained at `now=1790827135`; purgeable at 1790827136 |

Cassandra source anchors: `db/rows/Cells.java::reconcile` and
`resolveRegular` for timestamp and equal-timestamp tombstone precedence;
`db/rows/Rows.java::merge` and `db/rows/BTreeRow.java` for row/complex deletion;
`db/DeletionTime.java::deletes` for deletion coverage;
`db/rows/AbstractCell.java::isLive,purge` for expiry; and
`db/partitions/PurgeFunction.java` for strict `localDeletionTime < gcBefore`.

The existing `test_tomb/dropped_regular_col` fixture supplies the dropped-column
case. This fixture does not replace the broader traced/untraced and SELECT
agreement corpus required by the approved specification.
