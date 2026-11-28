# issue_4626 -- generation-discovery oracle fixtures (tasks 1.9 and 1.10)

Cassandra **5.0.8**-written SSTable directories for `openspec/changes/generation-discovery`. Provenance,
exact commands and the (small) set of derivations are in `test-data/REGEN-NOTES-4626.md`.

Why here and not in `test-data/datasets/sstables/`: the checkout corpus is part of the datasets bundle
and the corpus audit enumerates keyspaces from disk, so a new keyspace there would become an enforced
smoke-test keyspace. This directory is checkout-relative and no env var can hide it (precedent:
`../issue_3504/README.md`). Binaries are gitignored (`*.db`, `*.log`) and committed with `git add -f`.

Layout: `<state>/gd4626/<table>-<uuid>/` is one table directory, listed as `Live`. Every state is its own
root, so state is a directory choice, never a per-test mutation.

| Root | Contents | Written by | Scenarios |
|---|---|---|---|
| `uuid_ids/gd4626/uuid_mixed-*` | `nb-1-big`, `nb-2-big` (sequence) then `nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big`, `nb-3h5t_0i6s_1tsu22lxu6305iz8wl-big` (UUID), the flag flipped between the flushes | Cassandra | G5.1, G5.3, G6.4 (sequence ids still count, UUID ids do not), G5.2 order |
| `uuid_ids/gd4626/uuid_only-*` | `nb-3h5t_0i6t_1jigq2lxu6305iz8wl-big`, `nb-3h5t_0i6u_0xfpc2lxu6305iz8wl-big`, no sequence id | Cassandra | G5.1, G6.4 (next id is 1) |
| `txn_committed/gd4626/txn_compaction-*` | `nb-1..3-big` (compaction inputs, still on disk), `nb-4-big` (output), complete `nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log` (ADD nb-4, REMOVE nb-3/2/1, COMMIT) | Cassandra (see notes on how the inputs were kept) | G1.1 (Live = {4}) |
| `txn_incomplete/gd4626/txn_compaction-*` | same files, log with its final `COMMIT` record removed | derived from the committed log, one line | G1.2 (all REMOVE files present, so ADD nb-4 hidden, Live = {1,2,3}) |
| `txn_inconsistent/gd4626/txn_compaction-*` | the incomplete log plus only `nb-4-big-*` (nb-1..3 files deleted) | derived: incomplete log + whole-file deletion | G2.1 (`TxnInconsistent`) |
| `snap_backup/gd4626/snap_backup-*` | live `nb-1..5-big`; `backups/nb-3-big`, `backups/nb-4-big` (written by `incremental_backups`); `snapshots/pre-upgrade` (nb-1,2) and `snapshots/later` (nb-1..5), each with `manifest.json` and `schema.cql` | Cassandra | G8.1, G8.2 (`Snapshot("pre-upgrade")`), G6.2/G6.3 natural layout; G8.4 is any other root here (none has `backups/`) |
| `snap_backup_live_low/gd4626/snap_backup-*` | the same, with live `nb-2..5` deleted: live max 1, backups max 4, snapshots max 5 | derived: whole-file deletion | G6.2 (next id 5, not 2), G6.3 (snapshot max 5 does not count) |

`goldens/` holds `sstabledump -l` output (5.0.8) for the UUID and compaction-output Data.db files and an
excerpt of the Cassandra `system.log` for the keyspace. Sidecars are kept out of the table directories on
purpose: an extra file in a listed directory would change what G13 expects.

## Caveats a test author needs

* **mtimes are not preserved by git.** The `REMOVE` records carry the inputs' last-modified times
  (`LogRecord.java`, `updateTime`); Cassandra verifies them (`LogFile.verifyRecord`). A checkout has new
  mtimes, so a module that verified `updateTime` against disk would call every record corrupt (G2.2).
  The record `numFiles` (8, equal to the component count) is stable.
* The log's record paths are the container's absolute paths (`/var/lib/cassandra/data/gd4626/...`), not the
  fixture's path. Match records to files by file name inside the listed directory.
* The tables use Cassandra's default LZ4 compression (`CompressionInfo.db` present, no `CRC.db`).
