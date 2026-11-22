# issue_4201 — Cassandra 5.0.8-written two-generation fixture for `cqlite diff`

Backs **#4201** (`cqlite diff`: replica divergence and resurrection risk).

- Keyspace `test_diff`, table `risk_cases` (`id, ck, v text, m map<text,int>, s set<text>, fl frozen<list<text>>`,
  uncompressed, `gc_grace_seconds = 864000`); two raw generations, flushed separately with
  autocompaction off. Schema `test-data/schemas/issue-4201-diff-risk.cql`.
- **Provenance:** `cassandra:5.0.8` (image `sha256:259578bbeac315a7326c2a99e60eab86f3415e9e895207bf2a032af4de8c16b5`)
  in the owner-authorized `wintermute:cassandra` VM by
  `test-data/scripts/generate-issue-4201-diff-risk.sh`; every mutation pins `USING TIMESTAMP`
  (`generation-1.cql`, `generation-2.cql`), `sstabledump` goldens (`*-Data.db.jsonl`) and
  `sstablemetadata` (`*-Statistics.db.txt`) sit next to the bytes. The tombstones' local deletion
  time is `1795307390` (2026-11-22T00:29:50Z, `nb-2-big-Statistics.db.txt`); with
  `gc_grace_seconds = 864000` the strict-`<` purge boundary is `1796171390`.
- Checkout-relative on purpose (an sstables root, like `issue_4530`); `*.db` are force-added.
- Consumers: `cqlite-core/tests/issue_4201_diff_resurrection.rs`, `issue_4201_diff_symmetry.rs`,
  `cqlite-cli/tests/diff_cli_tests.rs`.

Staging: **side A = generations 2 + 3** (`nb-2-big`, `nb-3-big`; generation 3 holds only id 19's re-insert), **side B = generation 1 only** (`nb-1-big`).
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
| 9 | `v='zebra'` @5000 | `v='apple'` @5000 | cell `conflict`, `resolves_to: a` (equal ts; larger bytes `zebra`, `Cells.java:128`) |
| 10 | `v='apple'` @5000 | `v='zebra'` @5000 | cell `conflict`, `resolves_to: b` |
| 11 | cell tombstone `v` @5000 | `v='live-tie'` @5000 | cell `conflict`, `resolves_to: a` (tombstone beats live, `Cells.java:93-98`); tombstone `only-a`, risk **yes** (tie) |
| 12 | `v='expiring-wins'` @5000 TTL 1e8 | `v='live-loses'` @5000 | cell `conflict`, `resolves_to: a` (expiring beats live) |
| 13 | `m['k2']=7` @5000 | `m['k1']=7` @5000 | per element: `k2` `only-a`, `k1` `only-b` (same value and ts, different paths) |
| 14 | `s + {'y'}` @5000 | `s + {'x'}` @5000 | per element: `y` `only-a`, `x` `only-b`; no conflict |
| 15 | frozen `['a','b']` @5000 | frozen `['a, b']` @5000 | cell `conflict` (not equal), `resolves_to: a` (element count 2 > 1 leads the serialized bytes) |
| 16 | element tombstone `m['k1']` @6000, `m['k3']=3` @5000 | `m['k1']=1`, `m['k3']=3` @5000 | `k1`: tombstone `only-a` (risk **yes**, shadows only `k1`), cell `only-b`; `k3` `equal` |
| 17 | row tombstone @6000 | liveness-only row @5000 | tombstone `only-a`, risk **no** (declared limit #4601: a row marker is not a cell) |
| 18 | element tombstones `m['k1']`, `m['k2']` (ONE statement) @6000 | `m['k1']`, `m['k2']`, `m['k3']` @5000 | two distinct `only-a` cell tombstones (same ts and ldt, different `cell_path`), each shadowing only its own element; `k3` `only-b` |
| 19 | cell tombstone `v` @6000 (gen 2), then re-insert `v='reinserted'` @7000 (gen 3) | `v='old-value'` @5000 | cell `newer-in-a`; the tombstone is `only-a` with risk **no**: A's own newer live cell wins the merge (`Cells.resolveRegular`), so the old value cannot resurrect |
