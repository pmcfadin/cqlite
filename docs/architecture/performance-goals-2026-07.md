## OFFICIAL PERFORMANCE GOALS — owner-ratified 2026-07-15 (anchored to R11b field baselines, #2367)

Derivation: `docs/architecture/{perf-class-marks,htap-positioning-*,parquet-backend-comparison}-2026-07*.md` (branch research-parquet-comparison). Measurement rig: **#2418 flight-loadgen** (server-direct) + round-N field runs (through-Trino).

### Lane A — server-direct Flight (engine-owned)
| ID | Goal | Baseline (R11b) | Target | Stretch |
|---|---|---|---|---|
| A1 | Warm keyed read | server ~2ms (wall unmeasured sans Trino) | **p50 ≤10ms, p99 ≤100ms** | p99 ≤50ms |
| A2 | Keyed throughput | — | **≥1,000 qps/pod warm** | 5k/pod |
| A3 | Cold first-touch/table | O(summary) | **≤250ms** | ≤100ms |
| A4 | Scan feed rate | ~10k rows/s/pod | **Stage 1 ≥100k/pod** → **Stage 2 ≥600k/pod** (beats Spark-Cassandra ancestor, OLTP-isolated) | Stage 3 millions/s (columnar) |
| A5 | Overload stability | 0 restarts @80-thread | regression floor | — |

### Lane B — through-Trino (floor-shared)
| ID | Goal | Baseline | Target |
|---|---|---|---|
| B1 | Warm interactive | p50 227ms / p99 366ms | **≤300/≤500ms = regression floor** |
| B2 | Concurrency | 34 qps @8thr | **≥100 qps @32thr, 3 pods** |
| B3 | Full-scan 1.94M rows | 66s | **Stage 1 ≤10s → Stage 2 ≤3s** |
| B4 | Freshness / memory | ≤3s · 391Mi peak · 3-4Mi idle | floors: **≤3s · ≤512Mi · ≤16Mi** |

### Sequencing consequence (owner-acknowledged)
Stage 1 = this epic + #2313's remit (0.15). **Stage 2 requires promoting #941 (DataFusion projection/pushdown); Stage 3 requires #2037 (ArrowMemtable)** — the ladder forces that decision when Stage 1 saturates. First actions: #2418 (loadgen = the rig) and #2419 (saturation gauges) are Ready with sealed specs; every round-N report stamps the A/B tables.
