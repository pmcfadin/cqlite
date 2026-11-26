#!/usr/bin/env python3
"""C3: explain (CQLite) vs live CQL at CL ALL, per sample key. usage: compare_c3.py TABLE EXPLAIN_DIR LIVE_JSONL
Compares the set of winner cells (clustering, column) with live non-null cells: presence both ways, writetime, TTL-presence, and the
value where both sides are plain text/integer. Non-frozen collections: number of winner cells vs live element count.
Prints one line per key: MATCH or MISMATCH(reason) and a summary."""
import sys, json, os, collections
T, D, L = sys.argv[1:4]
STATIC_COLS = {'t_static': {'s1', 's2'}}
COLL = {'t_coll': {'l', 's', 'm', 'nested'}, 't_udt': {'ps'}, 't_static': {'s2'}, 't_tomb': {'l', 'm'}}
def norm(v):
    if isinstance(v, dict) and len(v) == 1: v = next(iter(v.values()))
    return v
def ckey(cl):
    if cl is None: return ()
    return tuple(str(norm(x[1])) for x in cl['columns'])
res = collections.Counter(); lines = []
live = [json.loads(l) for l in open(L) if l.strip()]
for i, rec in enumerate(live, 1):
    f = os.path.join(D, f'explain-{i:03d}.out')
    if not os.path.exists(f): lines.append(f'{i:03d} {rec["cat"]} {rec["key"]}: NO-EXPLAIN-OUTPUT'); res['no-output'] += 1; continue
    txt = open(f).read()
    try: ex = json.loads(txt[txt.index('\n{') + 1:] if not txt.startswith('{') else txt)
    except Exception as e: lines.append(f'{i:03d} {rec["cat"]} {rec["key"]}: EXPLAIN-UNPARSEABLE {e}'); res['unparseable'] += 1; continue
    if 'error' in rec: lines.append(f'{i:03d} {rec["cat"]} {rec["key"]}: LIVE-ERROR {rec["error"]}'); res['live-error'] += 1; continue
    win = collections.defaultdict(list)
    for c in ex.get('cells', []):
        if c.get('verdict') == 'winner': win[(ckey(c['clustering']), c['column'])].append(c)
    coll_cols = COLL.get(T, set())
    liv = {}; livc = {}
    statics = {}
    for r in rec['rows']:
        for col, d in r['cells'].items():
            if col in coll_cols and 'n' in d: livc[(tuple(map(str, r['ck'])), col)] = d['n']
            elif col in STATIC_COLS.get(T, ()): statics[col] = d
            else: liv[(tuple(map(str, r['ck'])), col)] = d
    why = []
    for (ck, col), cs in win.items():
        if col in coll_cols:
            n = livc.get((ck, col), livc.get(((), col)))
            if n is None and not any(k[1] == col for k in livc): why.append(f'explain-only coll {ck}{col}')
            continue
        if col in STATIC_COLS.get(T, ()):
            d = statics.get(col)
            if d is None: why.append(f'explain-only static {col}')
            elif cs[0]['writetime'] != d['wt']: why.append(f'static {col} wt {cs[0]["writetime"]}!={d["wt"]}')
            continue
        d = liv.get((ck, col))
        if d is None: why.append(f'explain-only {ck} {col}'); continue
        c = cs[0]
        if c['writetime'] != d['wt']: why.append(f'{ck} {col} wt {c["writetime"]}!={d["wt"]}')
        if (c.get('ttl') is None) != (d['ttl'] is None): why.append(f'{ck} {col} ttl {c.get("ttl")}!={d["ttl"]}')
        ev, lv = norm(c.get('value')), d['v']
        if col not in ('dt','tm','ts','tu','u','ip','du','dec','vi','f','d') and isinstance(ev, (str, int)) and isinstance(lv, (str, int)) and not isinstance(ev, bool) and str(ev) != str(lv): why.append(f'{ck} {col} value {ev!r}!={lv!r}')
    for k in liv:
        if k not in win: why.append(f'live-only {k}')
    for k in statics:
        if not any(kk[1] == k for kk in win): why.append(f'live-only static {k}')
    for (ck, col), n in livc.items():
        m = len(win.get((ck, col), win.get(((), col), [])))
        if m != n and not (n == 0): why.append(f'coll {ck}{col} winners {m} != live {n}')
    if why: res['mismatch'] += 1; lines.append(f'{i:03d} {rec["cat"]} {rec["key"]}: MISMATCH ' + '; '.join(why[:4]))
    else: res['match'] += 1; lines.append(f'{i:03d} {rec["cat"]} {rec["key"]}: MATCH ({len(win)} winner cells, {len(live[i-1]["rows"])} live rows)')
print('\n'.join(lines)); print('SUMMARY', T, dict(res))
