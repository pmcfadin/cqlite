#!/usr/bin/env python3
"""C6 scenario checks. Expectations come from the generator's deletion journal (scen/journal-*.jsonl) and sstabledump of both sides
(results/oracle/full-*.json: A0 gen2 holds tombstones for ids 0-99 partition, 100-299 row ck=1, 300-399 range ck>0; A2/B2 hold all 3000 rows;
B0 holds 2300 rows and no tombstone), NOT the diff output."""
import json, collections, sys
R = 'results/outF'
def load(n):
    t = open(f'{R}/diff-{n}.out').read(); return json.loads(t[t.index('\n{') + 1:] if not t.startswith('{') else t)
def kind(i): return 'partition' if i < 100 else 'row' if i < 300 else 'range' if i < 400 else None
SH = {'partition': {'0', '1', '2'}, 'row': {'1'}, 'range': {'1', '2'}}
def check(name, tomb_side, phase, mirrored=False, purged=False):
    j = load(name); s = j['summary']; bad = []
    cell_side = 'only-b' if tomb_side == 'a' else 'only-a'
    tomb_pk = {}
    cells = collections.defaultdict(set)
    for p in j['partitions']:
        i = int(p['partition_key'])
        for t in p['tombstones']: tomb_pk[i] = t
        for c in p['cells']:
            if c['status'] != 'equal': cells[i].add((c['clustering'], c['status']))
    if purged:
        if tomb_pk: bad.append(f'tombstones reported although none exist: {len(tomb_pk)}')
        if s['resurrection_risk_before_gc_grace'] or s['resurrection_risk_past_gc_grace']: bad.append('resurrection-risk invented')
    else:
        if set(tomb_pk) != set(range(400)): bad.append(f'tombstone partitions {len(tomb_pk)} != 400 (missing {sorted(set(range(400))-set(tomb_pk))[:5]} extra {sorted(set(tomb_pk)-set(range(400)))[:5]})')
        for i, t in tomb_pk.items():
            if t['kind'] != kind(i) or t['side'] != tomb_side: bad.append(f'{i}: kind/side {t["kind"]}/{t["side"]}')
            r = t['resurrection_risk']
            if r['status'] != 'yes' or r['phase'] != phase: bad.append(f'{i}: risk {r["status"]}/{r["phase"]}')
            if {x['clustering'] for x in r['shadowed_cells']} != SH[kind(i)]: bad.append(f'{i}: shadowed {r["shadowed_cells"]}')
    exp = {i: {(c, cell_side) for c in SH[kind(i)]} for i in range(400)}
    got = {i: v for i, v in cells.items()}
    if got != exp: bad.append(f'cell divergence differs from journal: got {len(got)} partitions, expected 400; sample diff {[(i, got.get(i), exp.get(i)) for i in sorted(set(got)|set(exp)) if got.get(i)!=exp.get(i)][:2]}')
    print(f'{name}: summary={ {k:v for k,v in s.items() if v} } -> ' + ('PASS' if not bad else 'FAIL ' + '; '.join(bad[:3])))
check('resA-db0-db2-in', 'a', 'before-gc-grace')
check('resA-db0-db2-past', 'a', 'past-gc-grace')
check('resA-db2-db0-in', 'b', 'before-gc-grace')
check('resA-db2-db0-past', 'b', 'past-gc-grace')
j = load('resA-db0-db1-in'); print('resA-db0-db1-in:', {k: v for k, v in j['summary'].items() if v}, 'PASS' if j['summary']['only_a'] == j['summary']['only_b'] == j['summary']['tombstones_only_a'] == j['summary']['tombstones_only_b'] == 0 else 'FAIL')
for k in ('p5', 'p150', 'p350', 'p700'):
    j = load('resA-db0-db2-' + k); print(k, 'partitions_compared', j['summary']['partitions_compared'], {kk: v for kk, v in j['summary'].items() if v and kk != 'partitions_compared'})
j = load('resA-self-db2'); print('resA-self-db2:', {k: v for k, v in j['summary'].items() if v})
check('resB-db0-db2-in', 'a', 'before-gc-grace', purged=True)
check('resB-db0-db2-past', 'a', 'past-gc-grace', purged=True)
check('resB-db2-db0-in', 'b', 'before-gc-grace', purged=True)
j = load('resB-db0-db1-in'); print('resB-db0-db1-in:', {k: v for k, v in j['summary'].items() if v})
