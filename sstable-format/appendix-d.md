---
title: "Appendix D — Tools & Workflows"
description: "In this appendix you will..."
sidebar:
  label: "Appendix D — Tools & Workflows"
  order: 104
---

In this appendix you will learn:
- Minimal `sstabledump`, `sstablemetadata`, and scrub/verify invocations
- How to capture trimmed outputs for the guide
- Safety notes and cross-links to lifecycle chapters

## Cassandra Tools (CLI)

Examples use tiny, trimmed outputs for clarity. Cassandra 5.0 NB-format files use the `nb-1-big-` prefix.

```bash
sstabledump /var/lib/cassandra/data/ks/tbl-.../nb-1-big-Data.db | head -n 10
# Example trimmed output (illustrative):
# {"partition": {"key": "..."}, "row": {"cells": [ ... ]}}
```

```bash
sstablemetadata /var/lib/cassandra/data/ks/tbl-.../nb-1-big-Data.db
# Example trimmed output:
# SSTable Metadata: minTimestamp=..., maxTimestamp=..., partitionCount=...
```

Other tools:
- `sstablescrub` — validate and attempt repair of corrupted SSTables (run on copies)
- `sstablelevelreset` — reset LCS levels
- `sstableverify` — verify data checksums and components

Pin source classes (Cassandra 5.0.8):
- `org.apache.cassandra.tools.SSTableExport` — implements the `sstabledump` CLI
- `org.apache.cassandra.tools.SSTableMetadataViewer` — implements the `sstablemetadata` CLI

## Operational Notes (Cassandra)

- Prefer running tools against snapshots or copies.
- Use `TOC.txt` to verify component completeness before inspection.

## Safety Notes
- Run tools against copies; avoid modifying live data paths.
- Cross-check TOC and component presence before analysis.

## References
- `SSTableExport` (sstabledump): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/tools/SSTableExport.java`
- `SSTableMetadataViewer` (sstablemetadata): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/tools/SSTableMetadataViewer.java`
