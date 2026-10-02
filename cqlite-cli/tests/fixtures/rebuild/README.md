# `cqlite rebuild` expected-manifest fixtures (issue #4197, spec R9.1)

Committed expected `rebuild` manifests (design.md §D5). `rebuild_cli_tests.rs`
deep-equal-compares the manifest the real binary produces against the file named
for each scenario, after NORMALISING exactly the fields design.md calls volatile:

| field | normalisation | why |
|---|---|---|
| `input` | absolute path → `<input>/<basename>` | the input lives in a per-run `TempDir` (or the fetched dataset root) |
| `output` | absolute path → `<out>` / `<out>/<subdir>` | same; the RELATIVE part (the per-generation subdirectory) is kept, because R7.1 is about exactly that |
| `now` | removed after asserting it parses as an RFC3339 timestamp | wall-clock |
| `cqlite_version` | removed after asserting it is a non-empty `<major>.<minor>.<patch>` string | release-dependent |

Nothing else is normalised. In particular the refusal `remedy` string is pinned
VERBATIM, hex CRC values and all: spec R7.2 makes "stderr and the manifest both
name `data-corrupt` and the `salvage` (#4196) remedy" a contract, and a pinned
string is what makes a silent reword visible.

## Files

| file | scenario | input |
|---|---|---|
| `r7_1_table_dir_two_generations.json` | R7.1 — a 2-generation table dir, `--components digest,toc`: ARRAY-shaped, one `refused: null` entry per generation | two renamed copies of the committed `test_comp/lz4_table` fixture |
| `r3_classification_filter_summary_crc.json` | R3.1/R3.2/R9 — `--components filter,summary,crc` against a compressed input: the full per-field `classification` map plus a real `skipped_not_applicable` entry | the committed `test_comp/lz4_table` fixture + `lz4_table_with_fp_chance.cql` |
| `r7_2_refused_data_corrupt.json` | R7.2 — the Cassandra-verified `test_comp_corrupt/data_db_bit_flip` fixture refuses with `data-corrupt` at the exact chunk offset | the FETCHED corruption corpus (the test skips when absent) |
| `lz4_table_with_fp_chance.cql` | the `--schema` input that makes `bloom_filter_fp_chance` RECOVERABLE | — |

## Why `lz4_table_with_fp_chance.cql` exists, and where its value comes from

No schema under `test-data/schemas/` states `bloom_filter_fp_chance` (premise
task 0.2), so no committed corpus schema can exercise R3.1's `recovered`
classification. This file is `compression-parity.cql`'s `lz4_table` with the
option added at the value Cassandra's OWN `sstablemetadata` dump for that
generation records (`nb-1-big-Statistics.db.txt`: `Bloom Filter FP chance:
0.01`) — a Cassandra-written oracle, not a value invented to make a test pass.

The manifest fixture pins the CLASSIFICATION (`recovered` — the value came from
the schema rather than from a hardcoded default), which is what spec R9
contracts. It deliberately makes NO Filter.db byte-parity claim: see the long
note in `cqlite-core/tests/issue_4197_rebuild_summary_classification.rs` for why
`Filter.db` byte parity does not hold for these fixtures at all (`estimatedKeys`
is a compaction-inherited estimate, not a recount).
