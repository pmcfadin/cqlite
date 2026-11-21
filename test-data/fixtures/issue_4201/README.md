# issue_4201 — Cassandra 5.0.8-written two-generation fixture for `cqlite diff`

Backs **#4201** (`cqlite diff`: replica divergence and resurrection risk).

- Keyspace `test_diff`, table `risk_cases` (`id int, ck int, v text, PRIMARY KEY (id, ck)`,
  uncompressed, `gc_grace_seconds = 864000`); two raw generations, flushed separately with
  autocompaction off. Schema `test-data/schemas/issue-4201-diff-risk.cql`.
- **Provenance:** `cassandra:5.0.8` (image `sha256:259578bbeac315a7326c2a99e60eab86f3415e9e895207bf2a032af4de8c16b5`)
  in the owner-authorized `wintermute:cassandra` VM by
  `test-data/scripts/generate-issue-4201-diff-risk.sh`; every mutation pins `USING TIMESTAMP`
  (`generation-1.cql`, `generation-2.cql`), `sstabledump` goldens (`*-Data.db.jsonl`) and
  `sstablemetadata` (`*-Statistics.db.txt`) sit next to the bytes. The tombstones' local deletion
  time is `1795281617` (2026-11-21T17:20:17Z, `nb-2-big-Statistics.db.txt`); with
  `gc_grace_seconds = 864000` the strict-`<` purge boundary is `1796145617`.
- Checkout-relative on purpose (an sstables root, like `issue_4530`); `*.db` are force-added.
- Consumers: `cqlite-core/tests/issue_4201_diff_resurrection.rs`, `issue_4201_diff_symmetry.rs`,
  `cqlite-cli/tests/diff_cli_tests.rs`.

Staging: **side A = generation 2 only** (`nb-2-big`), **side B = generation 1 only** (`nb-1-big`).
Expected outcomes follow from the mutations and Cassandra's shadow rule
(`DeletionTime.deletes`: a cell is deleted iff `timestamp <= markedForDeleteAt`,
cassandra-5.0.8 `db/DeletionTime.java:173-175`), never from CQLite output:

| id | side A (gen 2) | side B (gen 1) | expected |
|---|---|---|---|
| 1 | row tombstone `ck=1` @2000 | `v` @3000 | tombstone `only-a`, risk **no** (3000 > 2000); cell `only-b` |
| 2 | partition tombstone @2000 | (key never written) | tombstone `only-a`, risk **no** (nothing to shadow); no cells |
| 3 | row tombstone `ck=1` @2000 | `v` @2000 | tombstone `only-a`, risk **yes** (tie: 2000 <= 2000); cell `only-b` |
| 4 | `v='a-older'` @2000 | `v='b-newer'` @3000 | cell `newer-in-b` |
| 5 | cell tombstone `v` @2000 | `v` @3000 | tombstone `only-a`, risk **no**; cell `only-b` |
| 6 | `v='a-newer'` @4000 | `v='b-older'` @3000 | cell `newer-in-a` |
| 7 | | `v='only-in-b'` | cell `only-b` |
| 8 | `v='only-in-a'` | | cell `only-a` |
