#!/usr/bin/env python3
"""CQL for the resurrection scenarios (plan Phase 6). usage: scenario.py write|delete TABLE  > prints CQL; journal to stderr (JSONL).
Timestamps explicit: inserts TS_INS, deletes TS_DEL = TS_INS + 10 s."""
import sys, json
cmd, t = sys.argv[1], sys.argv[2]
TS_INS = 1795600000_000000; TS_DEL = TS_INS + 10_000_000
if cmd == 'write':
    print('CONSISTENCY LOCAL_QUORUM;')
    for i in range(1000):
        for ck in range(3):
            ttl = ' AND TTL 2592000' if i % 7 == 0 else ''
            print(f"INSERT INTO fr.{t} (id,ck,v) VALUES ({i},{ck},'v{i}_{ck}') USING TIMESTAMP {TS_INS}{ttl};")
else:
    print('CONSISTENCY TWO;')
    for i in range(0, 100):
        print(f"DELETE FROM fr.{t} USING TIMESTAMP {TS_DEL} WHERE id={i};"); print(json.dumps(dict(table=t, op='partition-delete', id=i, ts=TS_DEL)), file=sys.stderr)
    for i in range(100, 300):
        print(f"DELETE FROM fr.{t} USING TIMESTAMP {TS_DEL} WHERE id={i} AND ck=1;"); print(json.dumps(dict(table=t, op='row-delete', id=i, ck=1, ts=TS_DEL)), file=sys.stderr)
    for i in range(300, 400):
        print(f"DELETE FROM fr.{t} USING TIMESTAMP {TS_DEL} WHERE id={i} AND ck>0;"); print(json.dumps(dict(table=t, op='range-delete', id=i, ck_gt=0, ts=TS_DEL)), file=sys.stderr)
