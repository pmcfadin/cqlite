# flight-warm-snapshot-closure Specification

## Purpose
TBD - created by archiving change snapshot-lifecycle-closure. Update Purpose after archive.
## Requirements
### Requirement: A repeated query over a stable snapshot path takes a warm hit with zero re-parse

The flight server SHALL serve, from the warm reader set and WITHOUT re-opening or re-parsing any
component, consecutive `do_get` requests for the same `(keyspace, table)` that resolve to the SAME
snapshot directory (a reused/longer-lived snapshot, §B/§C) whose `Data.db` inodes are unchanged. The
full-`Index.db`-parse counter (`cqlite.sstable.index_parses_total`) and
the warm reader-open work-probe (`reader_opens`) SHALL increment by 0 for such a warm hit. This SHALL
hold end-to-end through the flight `do_get` path (wiring evidence), not only at the registry helper
level.

#### Scenario: Second identical snapshot-mode query re-parses nothing

- **GIVEN** a flight service warmed for a `(keyspace, table)` from a snapshot directory, with the
  work counters (`index_parses_total`, `reader_opens`) reset after the first `do_get`
- **WHEN** a second `do_get` for the same table resolves to the SAME snapshot directory (same
  `Data.db` inodes)
- **THEN** the second `do_get` returns the same rows byte-for-byte as the first
- **AND** `index_parses_total` increments by 0 and `reader_opens` increments by 0 for the second
  request (a pure warm hit, no rebind needed because the path is unchanged).

#### Scenario: Warm-hit work is independent of partition count (scale-free)

- **GIVEN** two warmed generations of the same shape differing only in partition count (small vs large)
- **WHEN** each is served a repeat `do_get` over its stable snapshot path with counters reset
- **THEN** both repeats increment `index_parses_total` and `reader_opens` by 0 (the warm-hit work is
  bounded by the warm-set diff, not by partition count).

### Requirement: Rebind-by-inode closes a fresh matching snapshot dir without re-parse; a changed-inode set fails closed

The warm registry SHALL rebind each cached reader to the current directory's `Data.db` — preserving all
parsed state and incrementing `index_parses_total` by 0 — when a `do_get` resolves to a FRESH snapshot
directory (window rollover or a per-query dir) whose cached readers' paths are now dead, when and only
when the current `Data.db`'s authoritative `(device, inode, generation)` + file size match the cached
`GenerationId`. A generation whose inodes do NOT match (a recycled inode, a genuinely new generation, or a
`stat` failure) SHALL fail closed to a full rebuild — never a stale serve, never an ENOENT. The #2352
regression invariant (dead path with CHANGED inodes must rebuild) SHALL stay green.

#### Scenario: Fresh same-inode snapshot dir rebinds parsed state, zero parse

- **GIVEN** a warm reader set whose cached snapshot dir was cleared, and a fresh snapshot dir for the
  same table hardlinking the SAME `Data.db` inodes, counters reset
- **WHEN** a `do_get` resolves to the fresh dir and takes a warm hit
- **THEN** each matching reader is rebound to the fresh path (`rebind_hits_total` increments per rebound
  generation) and `index_parses_total` increments by 0
- **AND** the returned rows match the physical-dump golden byte-for-byte.

#### Scenario: Changed-inode generation fails closed to a rebuild

- **GIVEN** a warm reader set whose cached path is dead and a fresh dir whose `Data.db` has a DIFFERENT
  `(device, inode)` than the cached `GenerationId`
- **WHEN** a `do_get` resolves to the fresh dir
- **THEN** the mismatching generation is fully rebuilt (`reader_opens` increments for it), never rebound
- **AND** the request never serves the stale cached reader and never returns an ENOENT error.

### Requirement: A snapshot cleared mid-lifecycle never serves stale data or an ENOENT

The request SHALL fail closed to a fresh rebuild of the affected generation when a resolved snapshot
directory is deleted (a window rollover, an operator `clearsnapshot`, or a retired reused snapshot) such
that a warm-hit candidate's backing path is dead and no identity-matching live entry exists in the
current resolved directory. It SHALL NOT return partial/stale rows from the dead path and SHALL NOT surface a
raw ENOENT mid-merge. Row-level parity SHALL hold on both the physical-dump and query-semantics oracles
after the rebuild.

#### Scenario: Dead cached path with no live match rebuilds cleanly

- **GIVEN** a warm reader set whose cached snapshot dir is deleted and a current resolved dir that does
  NOT contain a matching-inode `Data.db` for that generation
- **WHEN** a `do_get` is served
- **THEN** the request rebuilds the generation from the authoritative current directory (or returns an
  authoritative not-found if the generation is genuinely gone), never an ENOENT and never a stale serve
- **AND** the returned result set matches the query-semantics oracle at a pinned `now`.

### Requirement: Closure counters are exposed for field measurement

The flight service SHALL expose scale-free work-probe counters that distinguish a pure warm hit (path
unchanged, zero rebind) from a warm-hit-with-rebind from a full rebuild, so a field round can verify the
snapshot-lifecycle closure round-over-round. At minimum: the per-request directory `resolves` counter
(unchanged, authoritative per request), `index_parses_total`, `reader_opens`, and a new
`rebind_hits_total` (rebound generations). These counters SHALL be observable through the existing
flight stats/metrics surface. Exposure surface (explicit, so a field probe and the intent audit agree):
`reader_opens` and `rebind_hits_total` SHALL be exposed via the programmatic `WarmMetricsSnapshot` stats
surface (the same surface the warm-closure tests and field probes read), which is authoritative for the
round-over-round verification; unlike the warm cache hit/miss/evict/refresh counters they are NOT also
mirrored to the OTel counter export (a snapshot-only exposure this change does not expand). `resolves`
and `index_parses_total` retain their existing surfaces (#2341/#2412).

#### Scenario: Counters distinguish pure warm hit, rebind, and rebuild over a query sequence

- **GIVEN** a sequence of `do_get` requests: (1) cold, (2) repeat on a stable path, (3) repeat on a
  fresh same-inode dir, (4) repeat on a changed-inode dir, with the metrics surface observed
- **WHEN** the four requests complete
- **THEN** request 1 increments `reader_opens` (cold build), request 2 increments none of
  `reader_opens`/`index_parses_total`/`rebind_hits_total` (pure warm hit), request 3 increments
  `rebind_hits_total` but not `index_parses_total` (rebind), and request 4 increments `reader_opens`
  (rebuild)
- **AND** the `resolves` counter increments by exactly 1 per request (the resolve stays authoritative
  per request, #2341/#1430, unchanged).

