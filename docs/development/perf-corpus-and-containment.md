# Perf corpus + memory containment (issue #3068)

Tooling for generating and safely measuring against a **field-shaped, multi-GB, LZ4-compressed,
single-SSTable** Cassandra 5.0 corpus. Built for issue #3068 (read-plane scan window / large I/O)
and reusable by any read-path performance work — #3029, #3030, #3031.

| File | Purpose |
|------|---------|
| `test-data/scripts/gen-perf-corpus-3068.sh` | Generate the corpus (cassandra:5.0.2 container + `cassandra-stress`) |
| `test-data/scripts/write-perf-corpus-manifest.py` | Emit the manifest, every value read back off disk |
| `test-data/scripts/read-compression-info.py` | Parse `CompressionInfo.db` (compressor / chunk length / chunk count) |
| `test-data/scripts/perf-run-contained.sh` | Run a measurement inside a memory-capped cgroup scope |
| `test-data/perf-corpus-3068-manifest.json` | Committed manifest of the generated corpus |

## Why a bespoke corpus

The committed `test-data/datasets/` fixtures are tiny, and the Phase-0 perf anchor was
**uncompressed** — so it never executed the compressed read path at all. Read-path measurement
needs a corpus that:

1. is **LZ4-compressed at the Cassandra table default `chunk_length_in_kb = 16`** (a compressed
   scan window is a no-op without real chunks);
2. has a `Data.db` far larger than any CPU cache and comparable to RAM, so "cold" is genuinely
   cold and "warm" is a real page-cache state;
3. is **one SSTable**, so a k-way merge cannot pollute the read-plane number.

Two row shapes are produced, both `nb` (BIG) with 10 rows per partition:

| Table | Row shape | Rows | `Data.db` |
|-------|-----------|------|-----------|
| `perf_3068.medium_700b` | ~700 B/row, 11 columns | 11,994,840 | ~8.0 GiB |
| `perf_3068.wide_4kb` | >= 4 KB/row (large `text` body + blob) | 1,199,890 | ~4.7 GiB |

`wide_4kb`'s payload is close to incompressible, so LZ4 leaves it marginally **larger** than
its logical size (Cassandra reports a compression ratio of ~1.0006) — that is expected, not a bug.

## The corpus is NOT committed; the manifest is

The corpus is multi-GB and lives outside the repo (default `/home/ubuntu/corpus-3068`, laid out so
`CQLITE_DATASETS_ROOT=$CORPUS_ROOT` works directly). What is committed is
`test-data/perf-corpus-3068-manifest.json`, and it records **only values read back from the written
bytes** — never values assumed from the DDL or from the generator's intent:

- component filenames + sizes from `stat`; `Data.db` sha256 by hashing the file;
- compressor, chunk length, and chunk count from the **`CompressionInfo.db` header**, not from the
  table's `compression` option (a later `ALTER` or a Cassandra-side clamp would make the DDL a lie);
- row count from **Cassandra's own `sstablemetadata`** reading `Statistics.db` (`totalRows`), run in
  a throwaway memory-capped container. This is **fail-closed**: if `totalRows` cannot be read the
  generator errors out rather than recording an unobserved number.

Regenerate + re-verify:

```bash
bash test-data/scripts/gen-perf-corpus-3068.sh           # hours; needs ~30 GiB free
python3 test-data/scripts/write-perf-corpus-manifest.py \
  --corpus-root /home/ubuntu/corpus-3068 --keyspace perf_3068 --image cassandra:5.0.2 \
  --table "medium_700b:$CORPUS/sstables/perf_3068/medium_700b-<uuid>" \
  --table "wide_4kb:$CORPUS/sstables/perf_3068/wide_4kb-<uuid>" \
  --out test-data/perf-corpus-3068-manifest.json
```

The recorded `data_db_sha256` is the reproducibility check: a regenerated corpus with a different
hash is a *different* corpus, and any perf number measured against it is not comparable.

## Why `perf-run-contained.sh` exists — read this before your first cold scan

**2026-07-28: an uncontained cold scan of the 8.0 GiB `Data.db` hard-hung an entire host for 75
minutes.** The failure mode is worth internalizing because it is not the one people expect:

- The reader mmap'd the whole `Data.db`. On a **swapless** box, `Committed_AS` reached **105% of
  `CommitLimit`**.
- **No OOM kill ever fired.** The kernel livelocked in direct reclaim instead — load average 62.7,
  every task in `D` state, including `sshd`. There was nothing left to schedule that could observe
  the problem, so nothing killed the offending process.
- Recovery took 75 minutes of the kernel grinding, not a reboot-and-continue.

The lesson: **on a swapless host, "the OOM killer will save me" is false.** Reclaim livelock has no
killer and no timeout. The only reliable protection is an *a priori* cap, so the offending process
hits a hard limit and dies while the host stays schedulable.

`perf-run-contained.sh` provides that cap. It runs the command in a transient systemd scope with
`MemoryMax` / `MemorySwapMax` / `OOMPolicy=kill`, and additionally **refuses to start** when the
system is already `>= 95%` committed — the exact precondition that wedged the box.

```bash
# Use this for ANY measurement that reads a multi-GB corpus.
bash test-data/scripts/perf-run-contained.sh --mem 8G --swap 2G -- \
  ./target/release/cqlite query --data-dir /home/ubuntu/corpus-3068/sstables ...
```

`--mem`/`--swap` take systemd memory syntax (byte count with an optional `K`/`M`/`G`/`T`[`i`]
suffix, a percentage, `max`, or `infinity`) and are **validated before `sudo`**. That validation is
safety-critical, not cosmetic: a bare `8` is a legal systemd value meaning *8 bytes*, so a typo
must not quietly turn every run into an instant OOM that looks like a real result. It is pinned by
`scripts/tests/test_perf_run_contained.sh` (gate component `tooling-tests`); `--check-args`
validates and prints the resolved caps without executing anything.

Practical caps on a 16 GiB box: `--mem 8G --swap 2G` leaves room for the host. If the measurement
gets OOM-killed under that cap, that is a **finding about the read path's memory behavior** (see the
`<128MB` memory target and the gate's `oom-audit`) — not a reason to raise the cap.
