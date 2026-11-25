# issue #4626 generation-discovery fixtures (tasks 1.9, 1.10)

Generated 2026-11-25 on the Incus VM `wintermute:cassandra` (project `wm-sandboxes`), docker container
`cassandra` from image `cassandra:5.0.8` (sha256:259578bb...16c5b), `nodetool version` = `ReleaseVersion: 5.0.8`.
Output: `test-data/fixtures/issue_4626/` (layout and scenario mapping in its README). Nothing under the
datasets bundle (`datasets-v4`) or `~/.cache-dev/cqlite-datasets` was touched, and no VM was launched,
deleted or snapshot-restored.

Record format cited from the pinned source:
`git -C $CQLITE_CASSANDRA_REPO show cassandra-5.0.8:src/java/org/apache/cassandra/db/lifecycle/LogRecord.java`
(`REGEX = ^(add|remove|commit|abort):\[([^,]*),?([^,]*),?([^,]*)\]\[(\d*)\]$`: type, path prefix, updateTime,
numFiles, then a checksum per record; `COMMIT:[,0,0][checksum]` is the final record).

## Config changes and restoration

| Setting | Change | Restored |
|---|---|---|
| `uuid_sstable_identifiers_enabled` in `/etc/cassandra/cassandra.yaml` (container) | `false` -> `true` for the UUID flushes, then back | yes, by `docker cp` of the saved original; `sha256sum` of the file is `0c226908bb4bbc95bb360694a92f235389e45fc15124c1715b2c2b7a4535da1c` before and after (original kept in the VM at `/root/gen4626/cassandra.yaml.orig`) |
| `incremental_backups` | not edited. Switched at runtime with `nodetool enablebackup` / `disablebackup` (`statusbackup` read `not running` before and after) | yes |

**Side effect on the node, and its repair.** With the flag on, Cassandra also named the SSTables of
`system*` tables with UUID ids (flushes at the restart). After the yaml was restored the node REFUSED to start
(`CassandraDaemon.java:887`: "UUID sstable identifiers are disabled but some sstables have been created with
UUID identifiers"). Flipping the flag back is therefore not reversible by config alone. Repair, done with the
node stopped: every UUID-named SSTable file in every table directory (168 files, map in the VM at
`/root/gen4626/rename-map.txt`) was RENAMED to the next free sequence id of its directory (rename only, no
bytes changed; ids carry no content in any component), then `docker start cassandra`. The node came up on the
restored yaml and `select * from gd4626.uuid_mixed` returned all 8 rows. Keyspace `gd4626` (3 sequence-id
tables + `uuid_mixed`, `uuid_only`) was left on the node; `envcheck` was untouched.

## Commands

All via `incus exec wintermute:cassandra --project wm-sandboxes -- docker exec cassandra ...`.

1. Keyspace and tables (default LZ4 compression, auto-compaction off per table so flushes stay separate):
   `CREATE KEYSPACE gd4626 WITH replication={'class':'SimpleStrategy','replication_factor':1};` and
   `CREATE TABLE gd4626.<t> (id int PRIMARY KEY, v text) WITH compaction={'class':'SizeTieredCompactionStrategy','enabled':'false'};`
   for `uuid_mixed`, `txn_compaction`, `snap_backup`. Two rows per generation, `nodetool flush gd4626 <t>` after each.
2. Sequence generations (flag `false`): 2 flushes each of `uuid_mixed`, `snap_backup`; 3 of `txn_compaction`.
3. **Transaction log (G1).** `/root/gen4626/watch.py` polled the `txn_compaction` directory and, the instant a
   `*_txn_*.log` appeared, hard-linked every file in it (inputs and log) into `/root/gen4626/stage_txn`; then
   `nodetool compact gd4626 txn_compaction` ran. Cassandra later unlinked its names (system.log:
   `Deleting sstable ... nb-3-big`, `nb-2-big`, `nb-1-big`) but the hard links kept the inodes, and the log inode
   had received its `COMMIT` append in between. The staged log is therefore Cassandra's complete committed log, byte for
   byte, with the three obsolete inputs still beside it:

   ```
   ADD:[.../nb-4-big-,0,8][2436736489]
   REMOVE:[.../nb-3-big-,1795588286893,8][1736859284]
   REMOVE:[.../nb-2-big-,1795588283873,8][3206408674]
   REMOVE:[.../nb-1-big-,1795588282962,8][1949738987]
   COMMIT:[,0,0][2613697770]
   ```

   The output `nb-4-big-*` was copied from the live directory after the compaction finished (`txn_committed`).
4. **UUID ids (G5).** Edited the yaml flag with `sed -i`, `docker restart cassandra`, then 2 more flushes of
   `uuid_mixed` (the dir now holds `nb-1`, `nb-2`, and two UUID ids) and a new table `uuid_only` with 2 flushes.
   Yaml restored and the repair above applied afterwards. The files copied off the VM were taken BEFORE the repair
   rename, so the fixture keeps the UUID names Cassandra wrote.
5. **Snapshots and backups (G6, G8).** On `snap_backup` (gens 1,2 live): `nodetool snapshot -t pre-upgrade -kt gd4626.snap_backup`;
   `nodetool enablebackup`; flush gens 3 and 4 (each is hard-linked into `backups/`); `nodetool disablebackup`;
   flush gen 5 (no backup); `nodetool snapshot -t later -kt gd4626.snap_backup`.
6. Copy-off: `tar` of the table directories streamed out of `incus exec` and extracted locally; goldens from
   `/opt/cassandra/tools/bin/sstabledump -l <Data.db>`; log excerpt from `/var/log/cassandra/system.log`.

## Derived states (the only non-Cassandra bytes operations)

| Fixture | Derivation | Why it cannot be natural |
|---|---|---|
| `txn_incomplete` | copy of `txn_committed`; the last line of the log (`COMMIT:...`, with its newline) removed. The earlier records keep their own checksums, so they stay valid. | The window between `prepareToCommit` (REMOVE records written) and `COMMIT` lasts milliseconds on a tiny table and cannot be sampled reliably. Log-file truncation only; no SSTable byte touched. |
| `txn_inconsistent` | `txn_incomplete` with the whole files `nb-1-big-*`, `nb-2-big-*`, `nb-3-big-*` deleted | Cassandra never deletes inputs before the `COMMIT` is durable, so incomplete log + missing REMOVE files only arises after a crash or an operator action. Whole-file deletion only. |
| `snap_backup_live_low` | `snap_backup` with live `nb-2..5-big-*` deleted | Cassandra's id generator only counts up, so live max < backups max never arises without deleting live files. Whole-file deletion only. |

No SSTable byte was hand-written anywhere.

## Not produced

* A real (not derived) incomplete log with REMOVE records. Only the ADD-only mid-compaction log is natural,
  and it was not captured. See the derivation above.
* BTI (`da`/`oa`) UUID fixtures: only BIG `nb` was generated (BTI needs a table with the BTI format; not requested).
* The parity manifest `test-data/cassandra-parity-manifest.yml` was NOT edited: it indexes parity scenarios
  for the datasets corpus, and sibling `test-data/fixtures/issue_*` fixtures (e.g. issue_3504) are not listed
  in it.
