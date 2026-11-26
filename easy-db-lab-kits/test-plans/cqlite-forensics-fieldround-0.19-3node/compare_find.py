#!/usr/bin/env python3
"""find (CQLite, db0) vs sstabledump -k (Cassandra 5.0.8) per sample key and generation.
usage: compare_find.py TABLE OUTM_DB0_TABLE_DIR DUMP_DIR KEYS_FILE(samples) [NODE_LABEL]
Compares: the set of generations holding the key (hit vs sstabledump finds the partition), max write timestamp of the hit, partition-deletion flag.
Absent keys must be bloom-negative/index-miss, never hit."""
import sys, json, os, glob, collections, re
T, OUT, DUMP, KEYS = sys.argv[1:5]
def tstamps(o, acc):
    if isinstance(o, dict):
        for k, v in o.items():
            if k in ('tstamp', 'marked_deleted') and isinstance(v, (str, int)) and str(v).lstrip('-').isdigit(): acc.append(int(v))
            else: tstamps(v, acc)
    elif isinstance(o, list):
        for x in o: tstamps(x, acc)
dump = {}   # gen basename -> {key: (maxts, partdel)}
for f in glob.glob(f'{DUMP}/*.json'):
    g = os.path.basename(f)[:-5]
    txt = open(f).read().strip()
    try: arr = json.loads(txt) if txt else []
    except Exception as e: print('DUMP-UNPARSEABLE', f, e); continue
    d = {}
    for p in arr:
        key = ':'.join(str(x) for x in p['partition']['key'])
        acc = []; tstamps(p, acc)
        d[key] = (max(acc) if acc else None, 'deletion_info' in p['partition'])
    dump[g] = d
res = collections.Counter(); issues = []
keys = [l.rstrip('\n').split('\t') for l in open(KEYS)]
for i, (cat, lit) in enumerate(keys, 1):
    k = ':'.join(x.strip().strip("'") for x in lit.split(', '))
    f = f'{OUT}/find-{i:03d}.out'
    if not os.path.exists(f): res['no-find-output'] += 1; continue
    txt = open(f).read()
    try: fj = json.loads(txt[txt.index('{'):])
    except Exception as e: res['unparseable'] += 1; issues.append((i, cat, k, 'find output unparseable')); continue
    fgen = {os.path.basename(g['basename'])[:-8]: g for g in fj['generations']}
    for g, dd in dump.items():
        gi = fgen.get(g)
        if gi is None: issues.append((i, cat, k, g, 'generation missing from find output')); res['missing-gen'] += 1; continue
        d_hit = k in dd; f_probe = gi['probe']
        if f_probe == 'unreadable': res['find-unreadable'] += 1; issues.append((i, cat, k, g, 'find UNREADABLE: ' + gi.get('cause', '')[:100])); continue
        f_hit = f_probe in ('hit', 'scanned') and 'data_offset' in gi or f_probe == 'hit'
        if f_hit != d_hit: res['hit-mismatch'] += 1; issues.append((i, cat, k, g, f'find={f_probe} sstabledump_found={d_hit}')); continue
        res['gen-agree'] += 1
        if d_hit:
            mt, pdel = dump[g][k]
            if gi.get('max_writetime') != mt: res['maxts-mismatch'] += 1; issues.append((i, cat, k, g, f'max_writetime find={gi.get("max_writetime")} sstabledump={mt}'))
            else: res['maxts-agree'] += 1
            if bool(gi.get('partition_deletion')) != pdel: res['pdel-mismatch'] += 1; issues.append((i, cat, k, g, f'partition_deletion find={gi.get("partition_deletion")} sstabledump={pdel}'))
            else: res['pdel-agree'] += 1
        elif cat == 'absent' and f_probe == 'hit': res['absent-hit'] += 1
print(dict(res))
for x in issues[:15]: print(x)
