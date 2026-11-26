#!/usr/bin/env python3
"""Sample keys per table (category<TAB>CQL-literal). Categories come from the generator rules in gen.py (the
generator-side record, NOT a CQLite oracle). 100 existing + 20 never-written keys per table."""
import sys, uuid, random
R = random.Random(20261123)
def pick(n, pred, lo, hi, k):
    out = [i for i in range(lo, hi) if pred(i)][:k]  # deterministic prefix
    return out
def spread(pred, lo, hi, k):
    xs = [i for i in R.sample(range(lo, hi), min(hi - lo, k * 60)) if pred(i)]
    return xs[:k]
def w(name, rows):
    with open(f'samples/{name}.keys', 'w') as f:
        for cat, lit in rows: f.write(f'{cat}\t{lit}\n')
NB = 1_600_000
rows = [('overwritten', str(i)) for i in spread(lambda i: i % 50 == 0, 0, NB // 2, 20)] \
     + [('cell-deleted', str(i)) for i in spread(lambda i: i % 97 == 0, 0, NB // 2, 15)] \
     + [('row-deleted', str(i)) for i in spread(lambda i: i % 89 == 0, 0, NB // 2, 15)] \
     + [('plainA', str(i)) for i in spread(lambda i: i % 50 and i % 97 and i % 89, 0, NB // 2, 25)] \
     + [('plainB', str(i)) for i in spread(lambda i: True, NB // 2, NB, 25)] \
     + [('absent', str(10**7 + i)) for i in range(20)]
w('t_basic', rows)
NC = 1_000_000
u = lambda i: str(uuid.UUID(int=i))
w('t_coll', [('coll-overwrite', u(i)) for i in spread(lambda i: i % 20 == 0, 0, NC // 2, 25)]
    + [('elem-delete', u(i)) for i in spread(lambda i: i % 30 == 0, 0, NC // 2, 20)]
    + [('plainA', u(i)) for i in spread(lambda i: i % 20 and i % 30, 0, NC // 2, 25)]
    + [('plainB', u(i)) for i in spread(lambda i: True, NC // 2, NC, 30)] + [('absent', u(5_000_000 + i)) for i in range(20)])
NU = 200_000
w('t_udt', [('udt-overwrite', str(i)) for i in spread(lambda i: i % 40 == 0, 0, NU // 2, 30)]
    + [('plainA', str(i)) for i in spread(lambda i: i % 40, 0, NU // 2, 35)] + [('plainB', str(i)) for i in spread(lambda i: True, NU // 2, NU, 35)]
    + [('absent', str(10**6 + i)) for i in range(20)])
w('t_wide', [('giant', '0')] + [('small', str(i)) for i in range(1, 40)] + [('absent', str(1000 + i)) for i in range(20)])
NS = 150_000
sk = lambda i: f"'pk{i}', {i % 7}"
w('t_static', [('static-overwrite', sk(i)) for i in spread(lambda i: i % 30 == 0, 0, NS // 2, 25)]
    + [('static', sk(i)) for i in spread(lambda i: i % 3 == 0 and i % 30, 0, NS // 2, 25)]
    + [('no-static', sk(i)) for i in spread(lambda i: i % 3, 0, NS // 2, 20)]
    + [('plainB', sk(i)) for i in spread(lambda i: True, NS // 2, NS, 30)] + [('absent', sk(10**7 + i)) for i in range(20)])
NT = 50_000
cats = {0: 'partition-delete', 1: 'range-delete-c1', 2: 'range-delete-c1c2', 3: 'row-delete', 4: 'cell-delete', 5: 'collection-overwrite',
        6: 'short-ttl', 7: 'ttl-30d', 8: 'older-insert-shadowed', 9: 'plain'}
rows = []
for k in range(10): rows += [(cats[k], str(i)) for i in spread(lambda i, k=k: i % 10 == k, 0, NT // 2, 8)]
rows += [('plainB', str(i)) for i in spread(lambda i: True, NT // 2, NT, 20)] + [('absent', str(10**6 + i)) for i in range(20)]
w('t_tomb', rows)
for t in ('t_resurrect', 't_resurrect_purged'):
    w(t, [('sample', str(i)) for i in range(0, 1000, 10)] + [('absent', str(10**6 + i)) for i in range(20)])
