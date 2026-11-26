#!/usr/bin/env python3
"""diagnose (cheap tier, NOW run) vs sstablemetadata, per generation. usage: compare_diag.py OUTROOT META_TSV"""
import json, glob, sys, re, collections
R, M = sys.argv[1:3]; NAME = sys.argv[3] if len(sys.argv) > 3 else 'diagnose'; DROP = NAME != 'diagnose'
meta = {}
for l in open(M):
    p = l.rstrip('\n').split('\t')
    if len(p) < 11: continue
    d = dict(x.split('=', 1) for x in p[3:])
    meta[(p[0], p[1], int(re.match(r'..-(\d+)-', p[2]).group(1)))] = (p[2], d)
res = collections.Counter(); bad = []
for f in sorted(glob.glob(f'{R}/*/*/{NAME}.out')):
    node, table = f.split('/')[-3:-1]
    txt = open(f).read(); j = json.loads(txt[txt.index('{'):])
    gens = j['generations']
    ng_meta = len([k for k in meta if k[0] == node and k[1] == table])
    if len(gens) != ng_meta: bad.append((node, table, f'generation count diagnose={len(gens)} sstablemetadata={ng_meta}'))
    for g in gens:
        m = meta.get((node, table, g['generation']))
        if not m: bad.append((node, table, f'gen {g["generation"]} not in metadata')); continue
        name, d = m; why = []
        if 'unreadable' in json.dumps(g).lower(): why.append('unreadable field')
        if g['min_timestamp'] != int(d['min']): why.append(f'min_ts {g["min_timestamp"]}!={d["min"]}')
        if g['max_timestamp']['value'] != int(d['max']): why.append(f'max_ts {g["max_timestamp"]["value"]}!={d["max"]}')
        def ldt(x): return int(re.sub(r'.*\((\d+)\)', r'\1', x)) if '(' in x else int(x) if x.isdigit() else None
        if g['min_local_deletion_time'] != ldt(d['minldt']): why.append(f'min_ldt {g["min_local_deletion_time"]}!={d["minldt"]}')
        if g['max_local_deletion_time'] != ldt(d['maxldt']): why.append(f'max_ldt {g["max_local_deletion_time"]}!={d["maxldt"]}')
        ep = g['estimated_partition_count']['value']
        if ep != int(d['parts']): why.append(f'partitions {ep}!={d["parts"]}')
        dr = g['estimated_droppable_tombstone_ratio']['value']
        if DROP and (dr is None or abs(dr - float(d['drop'])) > 1e-6): why.append(f'droppable {dr}!={d["drop"]}')
        if g['repaired_at'] != int(d['rep']): why.append(f'repaired_at {g["repaired_at"]}!={d["rep"]}')
        cr = g['compression_ratio']['value']
        if cr is None or abs(cr - float(d['cr'])) > 1e-9: why.append(f'compression_ratio {cr}!={d["cr"]}')
        res['gens'] += 1
        if why: res['mismatch'] += 1; bad.append((node, table, name, '; '.join(why)))
    res['tables'] += 1
print(dict(res)); [print(b) for b in bad[:25]]
