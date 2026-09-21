#!/usr/bin/env bash
# Raw, overlapping Cassandra generations for explain decisions (#4193).
# Never compact: the older cells must remain on disk to exercise loser verdicts.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT_DIR="$DATA_ROOT/datasets"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="${2:?--out requires a directory}"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
mkdir -p "$OUT_DIR/sstables"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
STAGE="$(mktemp -d "$OUT_DIR/.explain-export.XXXXXX")"
CONTAINER_NAME="${CONTAINER_NAME:-cqlite-explain-$$}"
IMAGE="cassandra:5.0.2"
OWN_CONTAINER=0
cleanup() {
  if [[ "$OWN_CONTAINER" == 1 ]]; then docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true; fi
  rm -rf "$STAGE"
}
trap cleanup EXIT
if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "Container already exists: $CONTAINER_NAME" >&2
  exit 1
fi
docker run -d --name "$CONTAINER_NAME" -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256M "$IMAGE" >/dev/null
OWN_CONTAINER=1
READY=0
for ((attempt=0; attempt<60; attempt++)); do
  if docker exec "$CONTAINER_NAME" cqlsh -e 'SELECT release_version FROM system.local' >/dev/null 2>&1; then READY=1; break; fi
  sleep 5
done
[[ "$READY" == 1 ]] || { echo 'Cassandra did not become ready' >&2; exit 1; }
docker cp "$DATA_ROOT/schemas/explain-trace.cql" "$CONTAINER_NAME:/tmp/schema.cql" >/dev/null
docker exec "$CONTAINER_NAME" cqlsh -f /tmp/schema.cql
docker exec "$CONTAINER_NAME" nodetool disableautocompaction test_explain
cat > "$STAGE/generation-a.cql" <<'CQL'
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (1,1,'older') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (2,1,'row-shadowed') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (3,1,'range-shadowed') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (3,3,'outside-range') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (4,1,'equal-timestamp') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (5,1,'partition-shadowed') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,m) VALUES (6,1,{'old':1}) USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (7,1,'expires') USING TIMESTAMP 1000 AND TTL 3600;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (8,1,'purge-boundary') USING TIMESTAMP 1000;
CQL
cat > "$STAGE/generation-b.cql" <<'CQL'
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (1,1,'newer') USING TIMESTAMP 2000;
DELETE FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=2 AND ck=1;
DELETE FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=3 AND ck>=1 AND ck<=2;
DELETE v FROM test_explain.trace_decisions USING TIMESTAMP 1000 WHERE id=4 AND ck=1;
DELETE FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=5;
UPDATE test_explain.trace_decisions USING TIMESTAMP 2000 SET m={'new':2} WHERE id=6 AND ck=1;
DELETE v FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=8 AND ck=1;
CQL
for generation in a b; do
  docker cp "$STAGE/generation-$generation.cql" "$CONTAINER_NAME:/tmp/mutations.cql" >/dev/null
  docker exec "$CONTAINER_NAME" cqlsh -f /tmp/mutations.cql
  docker exec "$CONTAINER_NAME" nodetool flush test_explain
done
docker exec "$CONTAINER_NAME" nodetool version > "$STAGE/cassandra-version.txt"
docker image inspect "$IMAGE" --format '{{json .RepoDigests}}' > "$STAGE/image-digests.json"
docker exec "$CONTAINER_NAME" tar -C /var/lib/cassandra/data -cf - test_explain | tar -xf - -C "$STAGE"
python3 - "$STAGE" "$CONTAINER_NAME" <<'PY'
import hashlib, json, pathlib, subprocess, sys, zlib
root=pathlib.Path(sys.argv[1]); container=sys.argv[2]
tables=list((root/'test_explain').glob('trace_decisions-*'))
assert len(tables)==1, tables
table=tables[0]
data=sorted(table.glob('*-Data.db'))
assert len(data)==2, f'Expected two raw generations, got {data}'
for source in data:
    prefix=source.name.removesuffix('Data.db')
    components=(table/(prefix+'TOC.txt')).read_text().splitlines()
    assert all((table/(prefix+name)).is_file() for name in components), source
    assert zlib.crc32(source.read_bytes()) == int((table/(prefix+'Digest.crc32')).read_text()), source
    remote='/var/lib/cassandra/data/'+str(source.relative_to(root))
    dump=subprocess.check_output(['docker','exec',container,'/opt/cassandra/tools/bin/sstabledump',remote],text=True)
    partitions=json.loads(dump)
    assert partitions, source
    source.with_name(source.name+'.jsonl').write_text(''.join(json.dumps(p,separators=(',',':'))+'\n' for p in partitions))
    metadata=subprocess.check_output(['docker','exec',container,'/opt/cassandra/tools/bin/sstablemetadata',remote],text=True)
    source.with_name(source.name.replace('Data.db','Statistics.db.txt')).write_text(''.join(line.rstrip()+'\n' for line in metadata.splitlines()))
for name in ['generation-a.cql','generation-b.cql','cassandra-version.txt','image-digests.json']:
    (root/name).rename(table/name)
files=sorted(p for p in table.iterdir() if p.is_file())
(table/'sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2)+'\n')
PY
# Publish only our table after both raw generations and sidecars validate.
mkdir -p "$OUT_DIR/sstables/test_explain"
for previous in "$OUT_DIR/sstables/test_explain"/trace_decisions-*; do
  [[ ! -e "$previous" ]] || rm -rf "$previous"
done
mv "$STAGE/test_explain"/trace_decisions-* "$OUT_DIR/sstables/test_explain/"
echo "Generated two raw Cassandra generations under $OUT_DIR/sstables/test_explain"
echo 'Force-add the generated .db files when committing this fixture.'
