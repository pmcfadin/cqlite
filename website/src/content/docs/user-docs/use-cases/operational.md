---
title: "Operational Scenarios"
description: Using CQLite for migration validation, generating test fixtures from production data, and inspecting Cassandra backups and snapshots without a live cluster.
sidebar:
  label: Operational
  order: 4
---

# Operational Scenarios

CQLite's no-cluster-dependency model makes it useful for several operational
tasks that normally require a running Cassandra instance. This page covers three
patterns: migration validation, test fixture generation from production data, and
backup/snapshot inspection.

## Migration validation

When migrating between Cassandra clusters, versions, or schemas, you need to verify
that the data in the new cluster matches the source. CQLite lets you read the source
SSTables directly from a backup or snapshot — no source cluster required.

### Row count and schema smoke test

```bash
# Count rows in the source SSTable
cqlite --schema source-schema.cql \
       --data-dir /mnt/cassandra-backup/data/my_ks/my_table-<uuid>/ \
       --query "SELECT COUNT(*) FROM my_ks.my_table" \
       --out json

# Sample rows from the source for spot-checking
cqlite --schema source-schema.cql \
       --data-dir /mnt/cassandra-backup/data/my_ks/my_table-<uuid>/ \
       --query "SELECT * FROM my_ks.my_table LIMIT 1000" \
       --out csv > source-sample.csv
```

Then compare against the target cluster's output to validate row counts and
spot-check values.

### Python validation script

```python
import cqlite
import json

SOURCE_SSTABLES = "/mnt/cassandra-backup/data"
SOURCE_SCHEMA   = "/etc/cassandra-schemas/my_keyspace.cql"

# Read source SSTables
with cqlite.open(SOURCE_SSTABLES, schema=SOURCE_SCHEMA) as db:
    result = db.execute("SELECT id, checksum_field FROM my_ks.my_table")
    source_rows = {row.get("id"): row.get("checksum_field") for row in result.rows}

print(f"Source: {len(source_rows)} rows")

# Compare against target (using your Cassandra driver of choice)
# target_rows = query_target_cluster(...)
# mismatches = {k for k in source_rows if source_rows[k] != target_rows.get(k)}
# print(f"Mismatches: {len(mismatches)}")
```

### What CQLite validates and what it does not

CQLite reads each SSTable's own view of the data. It does **not**:

- Merge multiple SSTables the way a live Cassandra node does (compaction merges cells
  via last-write-wins). For a fully merged view, point `--data-dir` at the full table
  directory, not a single SSTable generation.
- Validate against compaction state or repair history.
- Resolve TTL expirations at query time relative to a wall clock (TTL behaviour
  may differ between source and target if substantial time has passed).

For high-confidence migration validation, use both CQLite for a quick structural
check and a secondary validation against a restored cluster.

## Test fixtures from production data

Running integration tests against a live production cluster is risky and slow.
CQLite lets you export a sanitized slice of production SSTables as test fixtures:

### Extract a fixture slice

```bash
# Export a deterministic slice to JSON for use as test fixtures
cqlite --schema prod-schema.cql \
       --data-dir /mnt/prod-backup/data/app_ks/users-<uuid>/ \
       --query "SELECT id, name, email, role FROM app_ks.users LIMIT 200" \
       --out json > tests/fixtures/users-sample.json
```

### Sanitize PII before committing fixtures

```python
import cqlite
import json
import hashlib

SOURCE_SSTABLES = "/mnt/prod-backup/data"
SCHEMA          = "/etc/cassandra-schemas/app.cql"
OUTPUT          = "tests/fixtures/users-anonymised.json"

def anonymise(row: dict) -> dict:
    """Replace PII fields with deterministic hashes."""
    out = dict(row)
    if "email" in out and out["email"]:
        out["email"] = hashlib.sha256(str(out["email"]).encode()).hexdigest()[:12] + "@test.example"
    if "name" in out and out["name"]:
        out["name"] = "User-" + hashlib.sha256(str(out["id"]).encode()).hexdigest()[:8]
    return out

with cqlite.open(SOURCE_SSTABLES, schema=SCHEMA) as db:
    result = db.execute(
        "SELECT id, name, email, role, created FROM app_ks.users LIMIT 500"
    )
    rows = [anonymise(row.to_dict()) for row in result.rows]

with open(OUTPUT, "w") as f:
    json.dump(rows, f, indent=2, default=str)

print(f"Written {len(rows)} anonymised rows to {OUTPUT}")
```

### Use fixtures in CI

Once exported, fixtures are plain JSON files that work with any test framework —
no Cassandra connection needed in CI:

```python
import json
import pytest

@pytest.fixture
def sample_users():
    with open("tests/fixtures/users-anonymised.json") as f:
        return json.load(f)

def test_user_roles(sample_users):
    roles = {row["role"] for row in sample_users}
    assert "admin" in roles or "user" in roles
```

## Backup and snapshot inspection

Cassandra's `nodetool snapshot` and cloud backup tools (Medusa, Priam) produce
directories of SSTable files. CQLite can read them directly without restoring to
a cluster first.

### Quick health check on a backup

```bash
# Verify a backup is readable and contains expected data
cqlite --schema schema.cql \
       --data-dir /mnt/backup/2025-01-15/my_ks/events-<uuid>/ \
       --query "SELECT COUNT(*) FROM my_ks.events" \
       --out json
```

### List all tables in a backup directory

```bash
# Inspect what's in a backup (shell, not CQL)
find /mnt/backup/data -name "TOC.txt" \
  | sed 's|.*/\([^/]*/[^/]*\)/.*|\1|' \
  | sort -u
```

### Spot-check specific rows

```bash
# Check if a specific partition exists in a backup
cqlite --schema schema.cql \
       --data-dir /mnt/backup/data/my_ks/orders-<uuid>/ \
       --query "SELECT order_id, status, total FROM my_ks.orders WHERE order_id = 12345" \
       --out json
```

### Python: audit backup completeness

```python
import cqlite
import os
from pathlib import Path

BACKUP_ROOT = Path("/mnt/cassandra-backup/data")
SCHEMA      = "/etc/cassandra-schemas/my_keyspace.cql"

# Find all SSTable directories for a keyspace
ks_path = BACKUP_ROOT / "my_ks"
table_dirs = [d for d in ks_path.iterdir() if d.is_dir()]

print(f"Found {len(table_dirs)} table directories")

with cqlite.open(str(BACKUP_ROOT), schema=SCHEMA) as db:
    result = db.execute("SELECT COUNT(*) FROM my_ks.orders")
    rows = result.rows
    if rows:
        print("Order count:", rows[0].to_dict())
```

### Disaster recovery validation

CQLite is useful in DR drills: given a set of SSTables extracted from a backup,
verify that critical data is present and readable before committing to a full
cluster restore:

```bash
#!/usr/bin/env bash
# dr-validate.sh — quick sanity check on a restored backup

SCHEMA="/etc/cassandra-schemas/critical.cql"
DATA_DIR="/mnt/dr-restore/data"

for table in critical_ks.accounts critical_ks.transactions; do
    count=$(cqlite --schema "$SCHEMA" \
                   --data-dir "$DATA_DIR" \
                   --query "SELECT COUNT(*) FROM $table" \
                   --out json 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0].get('count(*)', 0))" 2>/dev/null || echo "ERROR")
    echo "$table: $count rows"
done
```

## Schema evolution inspection

When you need to understand what columns existed at a particular point in time
(from a backup), or when a live schema change has made old SSTables ambiguous:

```bash
# Inspect the Statistics.db.txt file (no CQLite needed — plain text)
cat /mnt/backup/data/my_ks/my_table-<uuid>/nb-1-big-Statistics.db.txt

# Read using the old schema to see what columns were populated
cqlite --schema old-schema.cql \
       --data-dir /mnt/backup/data/my_ks/my_table-<uuid>/ \
       --query "SELECT * FROM my_ks.my_table LIMIT 5" \
       --out json
```

:::caution
CQLite requires a schema file to decode SSTables (no-heuristics mandate). If the
schema has evolved since the backup was taken, use the schema that was in effect
when the backup was created, not the current schema. Decoding with a mismatched
schema can produce incorrect values or errors.
:::

## What you cannot do (yet)

- **Full compaction merge across all SSTables in a keyspace.** CQLite reads the
  SSTables you point it at. If those span multiple non-merged generations, duplicate
  partition keys appear as separate rows. For a fully merged view, point at the full
  table directory or run `nodetool compact` before exporting.
- **Schema-free inspection.** CQLite always requires a schema file. If you have lost
  the schema, use `DESCRIBE TABLE` from `cqlsh` on a running cluster, or extract it
  from `system_schema.tables` in an older backup.
- **Real-time CDC / streaming from commitlog.** Delta-scan and CDC-style projections
  are not yet available; CQLite reads per-SSTable snapshots, not a change stream.

## Further reading

- [GitHub repository](https://github.com/pmcfadin/cqlite)
- [API reference](/cqlite/api/latest/)
<!-- TODO(W4): link to CLI reference when merged -->
<!-- TODO(W5): link to Python and Node bindings pages when merged -->
