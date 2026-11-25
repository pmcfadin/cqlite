# rebuild-components — delta (generation-discovery, issue #4626)

`rebuild_components` takes its generation and its Statistics trust from the `generation-discovery`
module. Existing requirements R1 to R4 are unchanged. All requirements are ADDED.

## ADDED Requirements

### Requirement: GD1 — A below-floor generation is refused as UnsupportedVersion, never reported as lost

`rebuild_components` SHALL evaluate the version floor from the filename descriptor before opening any
component (`generation-discovery` G9). A generation below the floor (`ma`-`me`, or a non-`da` BTI
version) SHALL be refused with `Error::UnsupportedVersion` naming the version and the floor. Its
Statistics fields SHALL NOT be classified `lost` (R4): a refusal is not a classification. Cassandra
itself rejects such a descriptor (`io/sstable/Descriptor.java:388-407`).

#### Scenario: GD1.1 `ma-1-big` is refused
- **Given** a directory holding `ma-1-big-Data.db` and no `Statistics.db`
- **When** `rebuild_components(..., &[Statistics])` runs
- **Then** it returns `Err(UnsupportedVersion)` naming `ma` and `na`, writes nothing, and the manifest
  has no `classification.statistics` entry for it.

### Requirement: GD2 — Rebuild accepts a TocMissing generation and discovers it through the module

`rebuild` SHALL list its input through `generation-discovery` with Scope Live, accepting TocMissing,
because regenerating a missing `TOC.txt` is a headline rebuild case (R1.3). A UUID-id generation
(`nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-Data.db`) SHALL be rebuilt like any other, and its output name
SHALL carry the same id text. An Orphan SHALL be reported in the manifest, not rebuilt, and an
`TxnInconsistent` directory SHALL be refused naming the log.

#### Scenario: GD2.1 TOC deleted, generation still rebuilt
- **Given** a `test_basic` copy with `nb-1-big-TOC.txt` deleted
- **When** rebuild runs for `toc`
- **Then** the generation is rebuilt and the manifest lists `TocMissing` as the input condition.

#### Scenario: GD2.2 UUID-id generation
- **Given** a Cassandra 5.0.8 generation written with UUID ids
- **When** rebuild runs for `digest` and `crc`
- **Then** both regenerated files are byte-identical to the originals.

#### Scenario: GD2.3 inconsistent directory
- **Given** a directory with an incomplete `nb_txn_compaction_<uuid>.log` and its old files missing
- **When** rebuild runs
- **Then** it exits with the `TxnInconsistent` refusal and writes nothing.

### Requirement: GD3 — A present but damaged Statistics.db is read through the module's trusted metadata

Where R2 and R4 read the original `Statistics.db`, they SHALL obtain it through the module's trusted
metadata accessor (G11). A `Corrupt` outcome keeps R2's existing fail-closed behaviour (#4612, owner
Q23) unchanged.

#### Scenario: GD3.1 corrupt Statistics.db still fails closed
- **Given** a `nb-1-big-Statistics.db` with a flipped byte
- **When** rebuild runs for `index`
- **Then** it fails closed naming the path and both remedies, exactly as R2 requires today.
