#!/usr/bin/env python3
"""NEW C-SELECT compare: cqlite `query SELECT *` JSON (per key) vs live cqlsh SELECT JSON at CL ALL.
usage: compare_sel.py TABLE LIVE_OUT SEL_DIR KEYS_FILE. Per key: MATCH or MISMATCH(columns). Collections compared element by element, in order
(lists exact order; sets and maps: exact order and also as a set, reported separately). Missing == null."""
import sys, json, re, collections
T, LIVE, D, KEYS = sys.argv[1:5]
keys = [l.rstrip('\n').split('\t') for l in open(KEYS)]
live = [json.loads(l)['rows'] for l in open(LIVE)]
assert len(live) == len(keys), (len(live), len(keys))
def norm(v):
    # cqlite renders maps as [{"key":..,"value":..}]; convert to ordered list of pairs, live dict -> same
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, list): return [norm(x) for x in v]
    return v
def cql_map(v):
    if isinstance(v, list) and v and all(isinstance(x, dict) and set(x) == {'key', 'value'} for x in v):
        return [(x['key'], cql_map(x['value'])) for x in v]
    if isinstance(v, list): return [cql_map(x) for x in v]
    if isinstance(v, dict): return {k: cql_map(x) for k, x in v.items()}
    return v
def live_map(v):
    if isinstance(v, dict) and 'name' not in v and not any(k in v for k in ('street', 'zip')): return [(k, live_map(x)) for k, x in v.items()]
    if isinstance(v, list): return [live_map(x) for x in v]
    if isinstance(v, dict): return {k: live_map(x) for k, x in v.items()}
    return v
res = collections.Counter(); out = []
for i, ((cat, key), lrows) in enumerate(zip(keys, live), 1):
    f = f'{D}/{i:03d}.json'
    try: cr = json.load(open(f))
    except Exception as e: out.append(f'{i:03d} {cat} {key}: CQLITE-UNPARSEABLE/ERROR {e}'); res['error'] += 1; continue
    why = []
    srt = lambda rs: sorted(rs, key=lambda r: r.get('ck', 0))
    cr, lrows = srt(cr), srt(lrows)
    
    ca = {r.get('ck'): r for r in cr}; lb = {r.get('ck'): r for r in lrows}
    for k in sorted(set(ca) - set(lb), key=str): why.append(f'ck={k} only in cqlite')
    for k in sorted(set(lb) - set(ca), key=str): why.append(f'ck={k} only in live')
    for k in sorted(set(ca) & set(lb), key=str):
        a, b = ca[k], lb[k]
        for col in sorted(set(a) | set(b)):
            av, bv = cql_map(a.get(col)), live_map(b.get(col))
            # an empty collection and null are the same in CQL
            if av in (None, [], {}) and bv in (None, [], {}): continue
            if av != bv:
                # order-insensitive check for diagnosing
                try: same_set = sorted(map(json.dumps, av)) == sorted(map(json.dumps, bv))
                except Exception: same_set = False
                n_a = len(av) if hasattr(av, '__len__') else '-'; n_b = len(bv) if hasattr(bv, '__len__') else '-'
                why.append(f'ck={a.get("ck", b.get("ck"))} {col}: cqlite n={n_a} live n={n_b}' + (' (same elements, different ORDER)' if same_set else ''))
    if why: res['mismatch'] += 1; out.append(f'{i:03d} {cat} {key}: MISMATCH ' + '; '.join(why[:4]))
    else: res['match'] += 1; out.append(f'{i:03d} {cat} {key}: MATCH ({len(cr)} rows)')
print('\n'.join(out)); print('SUMMARY', T, dict(res))
