# Corpus provenance — regenerated on this host, geometry matched exactly

Generated 2026-08-04T01:14–01:19Z on `i4i.metal` / `ip-172-31-3-252` from the committed recipe,
verified, then Cassandra was **stopped** before any measurement (RUNBOOK pre-flight: nothing JVM may
compete for CPU).

## Provenance chain

| step | value |
|---|---|
| recipe | `docs/reports/ws0-3026-artifacts/ws0-corpus/gen-corpus.sh`, unmodified |
| invocation | `gen-corpus.sh 200000 375 6 96 3 96 16 2 10 8 50000` (exactly `rerun.sh`'s line) |
| Cassandra | `apache-cassandra-5.0.8-bin.tar.gz` from `archive.apache.org`, sha256 `1579d7d3f2d812741a28cd2c2cbe29e83541bb4d25fb21ec2c00c1e4fb3b9a8f` |
| JDK | OpenJDK 17.0.19 (`openjdk-17-jdk-headless`, Ubuntu 24.04) |
| daemon heap | `MAX_HEAP_SIZE=8G` — #3026's one deliberate non-path deviation, kept; `nodetool info` confirmed `8192.00 MB` |
| config delta | `cassandra.yaml.diff` applied with `patch(1)`; `diff <(diff stock patched) cassandra.yaml.diff` is **empty** → the config delta is byte-identical to #3026's |
| stress result | 4 batches × 50,000 partitions, **0 errors** |
| SSTable | one `nb-16-big`, 8 components, produced by `nodetree flush` + `compact` |
| source dir | `/home/ubuntu/ws0/cassandra-data/data/ws0/events-52ff1a008fa211f1ac2485829b296e3f` (symlinked onto the `/data` NVMe, not the EBS root) |
| staged to | `/data/ws0/ws0-corpus/sstables` (`WS0_STAGE`) |

## Geometry — measured here vs the targets

| metric | RUNBOOK target | #3026 committed | **measured here** | Δ vs #3026 | oracle |
|---|--:|--:|--:|--:|---|
| rows | 3,999,890 | 3,999,890 | **3,999,890** | **exact** | `sstablemetadata totalRows` |
| rows (independent) | — | 3,999,890 | **3,999,890** | **exact** | `fullscan.py 512` (512 token ranges) |
| `totalColumnsSet` | 35,999,010 | 35,999,010 | **35,999,010** | **exact** | `sstablemetadata` |
| logical (uncompressed) B/row | 693.29 | 692.70 | **692.58** | −0.017% | `dataLength` ÷ rows |
| on-disk (compressed) B/row | 196.09 | 195.96 | **195.94** | −0.010% | `Data.db` bytes ÷ rows |
| `dataLength` uncompressed | — | 2,770,741,510 | **2,770,255,150** | −0.018% | `CompressionInfo.db` |
| `Data.db` on disk | — | 783,799,203 | **783,752,072** | −0.006% | `stat` |
| compression ratio | — | 3.5350× | **3.5346×** | −0.011% | derived |
| SSTable count / format | 1 / `nb-16-big` | 1 / `nb-16-big` | **1 / `nb-16-big`** | match | `ls` + `sstablemetadata` |
| droppable tombstones | 0.0 | 0.0 | **0.0** | match | `sstablemetadata` |

**New `sha256(Data.db)` = `b1656ae8c0e45feb30f3da641b8a23c4969d1be43e5f341ef0af6bb3a9b41042`**
(`cassandra-stress` is not byte-deterministic, so the accepted bar — the one #3100 and #3217 both
used — is matched geometry plus a documented new sha256. Both halves are discharged here.)

**Both row counts landed exactly**, and every derived byte figure is within 0.02% of #3026's. The
recipe reproduces on a different microarchitecture.

## `now`-pinning: N/A, recorded rather than omitted (AC6)

`sstablemetadata` on this SSTable:

```
SSTable min local deletion time: no tombstones (9223372036854775807)
SSTable max local deletion time: no tombstones (9223372036854775807)
TTL min: 0
TTL max: 0
Estimated droppable tombstones: 0.0
```

No TTL and no tombstones, so **no read-time reconciliation depends on `now`** and there is nothing
to pin. Recorded as N/A per AC6 rather than silently skipped. Keep it so — a future corpus that
introduces a TTL would make this a live requirement.

## A documentation inconsistency worth noting (not a defect in this run)

The RUNBOOK's geometry table gives **693.29** logical B/row and **196.09** on-disk B/row, but
#3026's own committed `corpus-geometry.txt` records **692.70** and **195.96** for the very corpus the
RUNBOOK is describing. The two source documents disagree with each other by ~0.09%, before this run
measured anything. This run's values (692.58 / 195.94) match the **committed artefact**, which is the
better authority than the prose table. Flagged so a later reader does not mistake the 0.1% for
drift introduced here.

## Deviations from the recipe

1. `/home/ubuntu/ws0` is a **symlink to `/data/ws0/cass-home`** so the recipe's hardcoded paths
   resolve onto the instance-store NVMe rather than the EBS root (RUNBOOK pre-flight requires the
   scratch root on `/data`). No path string in the recipe changed.
2. `MAX_HEAP_SIZE=8G` on a 1,007 GiB box. This is #3026's own deviation, kept for fidelity; without
   it `cassandra-env.sh` auto-sizing would pick a vastly larger heap than the corpus that is being
   reproduced was built with.

Nothing performance-relevant in `cassandra.yaml` was changed — proven by the empty diff above, not
asserted.
