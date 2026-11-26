#!/usr/bin/env python3
"""C3 live oracle: for each sample key read the partition at CONSISTENCY ALL with WRITETIME/TTL per scalar column; print JSONL.
usage: live.py TABLE   (keys from /samples/TABLE.keys: category<TAB>literal). Non-frozen collections cannot take WRITETIME: reported as element counts."""
import sys, json, os, uuid, datetime
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
T = sys.argv[1]
# table -> (partition key cols, clustering cols, scalar regular/static cols with writetime, non-frozen collection cols)
SPEC = dict(
 t_basic=(['id'], ['bucket'], 'a t i b s ti f d dec vi bo ts dt tm u tu ip bl du'.split(), []),
 t_coll=(['id'], [], ['fl', 'fm'], ['l', 's', 'm', 'nested']),
 t_udt=(['id'], ['ck'], ['p', 'tp'], ['ps']),
 t_wide=(['pk'], ['ck'], ['v', 'n'], []),
 t_static=(['pk', 'c1'], ['c2', 'c3'], ['s1', 'v'], ['s2']),
 t_tomb=(['id'], ['c1', 'c2'], ['v'], ['l', 'm']),
 t_resurrect=(['id'], ['ck'], ['v'], []),
 t_resurrect_purged=(['id'], ['ck'], ['v'], []))
pk, ck, scal, coll = SPEC[T]
prof = ExecutionProfile(load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'), consistency_level=ConsistencyLevel.ALL, request_timeout=120)
c = Cluster([os.environ.get('CASS_HOST', 'cassandra.fr.svc.cluster.local')], execution_profiles={EXEC_PROFILE_DEFAULT: prof}); s = c.connect()
def sel():
    cols = pk + ck + [x for col in scal for x in (col, f'WRITETIME({col})', f'TTL({col})')] + coll
    return ', '.join(cols)
def j(v):
    if isinstance(v, (datetime.datetime, datetime.date)): return str(v)
    if isinstance(v, (set, frozenset)): return sorted(map(str, v))
    if isinstance(v, (bytes, bytearray)): return list(v)
    return v if isinstance(v, (int, float, str, bool, type(None))) else str(v)
for line in open(f'/samples/{T}.keys'):
    cat, lit = line.rstrip('\n').split('\t')
    where = ' AND '.join(f'{k}=%s' % l for k, l in zip(pk, [x.strip() for x in lit.split(', ')]))
    q = f'SELECT {sel()} FROM fr.{T} WHERE {where}' + (' LIMIT 200' if T == 't_wide' else '')
    try:
        rows = list(s.execute(q))
    except Exception as e:
        print(json.dumps(dict(cat=cat, key=lit, error=str(e)[:300]))); continue
    out = []
    for r in rows:
        d = r._asdict(); cell = {}
        for col in scal:
            if d[col] is not None or d.get(f'writetime_{col}') is not None:
                cell[col] = dict(v=j(d[col]), wt=d[f'writetime_{col}'], ttl=d[f'ttl_{col}'])
        for col in coll:
            v = d[col]
            if v is not None: cell[col] = dict(n=len(v))
        out.append(dict(ck=[j(d[k]) for k in ck], cells=cell))
    print(json.dumps(dict(cat=cat, key=lit, rows=out)))
