# flight-table-observability

## ADDED Requirements

### Requirement: tables_discovered reflects the visible on-disk table set and is bidirectional
The service SHALL export a gauge `cqlite.flight.tables_discovered` equal to the number of
`<keyspace>/<table>` SSTable directories currently visible under `--data-dir`, re-sampled on the existing
~2s saturation tick. The value SHALL rise when a table directory appears and fall when one is removed. A
wrong or empty `--data-dir` SHALL read 0.

#### Scenario: falls when a table directory is removed
- **GIVEN** a `--data-dir` containing K genuine `<keyspace>/<table>` SSTable directories
- **WHEN** the sampler tick runs and then one table directory is removed and the next tick runs
- **THEN** `cqlite.flight.tables_discovered` reads K, then reads K-1 after the removal.

#### Scenario: rises when a table directory appears
- **GIVEN** a `--data-dir` sampled at count K
- **WHEN** a new genuine `<keyspace>/<table>` SSTable directory appears and the next tick runs
- **THEN** `cqlite.flight.tables_discovered` reads K+1.

#### Scenario: empty or wrong mount reads zero
- **GIVEN** a `--data-dir` that is empty (or points at a directory with no table dirs)
- **WHEN** the sampler tick runs
- **THEN** `cqlite.flight.tables_discovered` reads 0.

### Requirement: tables_discovered counts only genuine table dirs, structurally
`cqlite.flight.tables_discovered` SHALL count only `<keyspace>/<table>` directories that directly contain
SSTable data, SHALL be correct on UUID-suffixed table directories, and SHALL exclude `snapshots/`,
`backups/`, and non-table entries. Classification SHALL be structural (directory layout) only, never
inferred from file contents.

#### Scenario: UUID-suffixed table dir counts; snapshots/backups excluded
- **GIVEN** a `--data-dir` with a `<keyspace>/<table>-<uuid>/` dir containing a `*-Data.db`, plus
  `snapshots/` and `backups/` subdirectories and an unrelated non-table entry
- **WHEN** the sampler tick runs
- **THEN** the UUID-suffixed table dir is counted exactly once
- **AND** the `snapshots/` and `backups/` subdirs and the non-table entry are NOT counted.

### Requirement: the discovery walk performs no SSTable opens or parses
The `tables_discovered` discovery walk SHALL use directory reads only and SHALL NOT open, stat-for-generation,
or parse any SSTable, preserving the cold-start invariant.

#### Scenario: sampling produces zero index_parses delta
- **GIVEN** a populated `--data-dir` and a baseline `cqlite.sstable.index_parses_total` reading
- **WHEN** N saturation sampler ticks run with no query activity
- **THEN** `cqlite.sstable.index_parses_total` shows zero delta from the sampling.

### Requirement: warm_tables reflects the live warm registry size and is bidirectional
The service SHALL export a gauge `cqlite.flight.warm_tables` equal to the current number of tables with a
live warm reader set in `WarmTableRegistry`, updated at the registry mutation sites (independent of sampler
cadence). It SHALL rise on the first serve of a previously-unseen table and fall on eviction/retirement.

#### Scenario: increments after a do_get on a previously-unseen table (public surface)
- **GIVEN** a running Flight service with an empty warm registry
- **WHEN** a real `do_get` is served through the public Flight surface for a previously-unseen table
- **THEN** `cqlite.flight.warm_tables` increments to reflect the newly warm table.

#### Scenario: decrements after eviction/retirement
- **GIVEN** `cqlite.flight.warm_tables` reads W with at least one warm table
- **WHEN** a warm table's last reader is evicted/retired (generation turnover or budget eviction)
- **THEN** `cqlite.flight.warm_tables` falls below W.

### Requirement: startup log line reports the discovered table/keyspace count
After the first sample, the service SHALL emit exactly one `info` log line reporting the number of tables,
the number of keyspaces, and the `--data-dir` path, so an inert/empty mount is visible in logs without a
metrics stack.

#### Scenario: first-sample info line present
- **GIVEN** the service starts and the sampler takes its first sample
- **WHEN** the first sample completes
- **THEN** exactly one `info` line is emitted naming N tables, M keyspaces, and the data-dir path.

### Requirement: both gauges are exported total-only through the standard OTLP pipeline
`cqlite.flight.tables_discovered` and `cqlite.flight.warm_tables` SHALL be registered as OTel gauges with
dedicated instrument arms (no ad-hoc fallback) and emitted with no attributes (total-only), so they carry no
high-cardinality labels.

#### Scenario: gauges are registered total-only with dedicated instruments
- **GIVEN** the metric catalog and OTel instrument registration
- **WHEN** the catalog/instrument invariants are checked
- **THEN** both metrics are namespaced, unique, registered in the metric set, have dedicated gauge
  instrument arms, and are emitted with an empty attribute set.
