#!/usr/bin/env python3
"""Deterministic load generator for the CQLite 0.19 field round (#4503).
usage: gen.py ddl | load TABLE PHASE [--shard k/n] | scenario ... | samples
Write timestamps are explicit (USING TIMESTAMP) so shadowing outcomes are known by construction."""
import sys, os, json, random, uuid, datetime, base64, time, argparse
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.util import Duration, uuid_from_time

SEED = 20261123
TS_INS = 1795600000_000000          # insert timestamp (micros)
TS_DEL = TS_INS + 10_000_000        # deletes/overwrites: +10 s, strictly newer
TS_OLD = TS_INS - 10_000_000        # older than the inserts (shadowed-by-tombstone cases)
SIZES = dict(basic=int(os.environ.get('N_BASIC', 1_600_000)),   # partitions, 5 rows each
             coll=int(os.environ.get('N_COLL', 1_000_000)), udt=int(os.environ.get('N_UDT', 200_000)),   # udt: partitions x 5
             wide_big=int(os.environ.get('N_WIDE_BIG', 2_500_000)), wide_small=int(os.environ.get('N_WIDE_SMALL', 40_000)),
             static=int(os.environ.get('N_STATIC', 150_000)),    # partitions x 10 rows
             tomb=int(os.environ.get('N_TOMB', 50_000)))         # partitions x 20 rows

def rnd(*k): return random.Random(hash_key(k))
def hash_key(k):
    h = SEED
    for x in k: h = (h * 1000003 + (x if isinstance(x, int) else sum(map(ord, str(x))))) & 0xFFFFFFFFFFFF
    return h
def words(r, n): return base64.b64encode(bytes(r.getrandbits(8) for _ in range(n))).decode()

def connect():
    host = os.environ.get('CASS_HOST', 'cassandra.fr.svc.cluster.local')
    prof = ExecutionProfile(load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'),
                            consistency_level=ConsistencyLevel.LOCAL_QUORUM, request_timeout=60)
    c = Cluster([host], execution_profiles={EXEC_PROFILE_DEFAULT: prof}, protocol_version=5)
    return c, c.connect()

def run(session, stmt, rows, conc=64, chunk=20000):
    it = iter(rows); n = 0; t0 = time.time()
    while True:
        part = [x for _, x in zip(range(chunk), it)]
        if not part: break
        for ok, res in execute_concurrent_with_args(session, stmt, part, concurrency=conc, raise_on_first_error=False):
            if not ok: raise res
        n += len(part)
        print(f'{n} ops {n/(time.time()-t0):.0f}/s', flush=True)
    return n

journal = None
def J(**kw):
    kw['t'] = time.time()
    if journal: journal.write(json.dumps(kw) + '\n'); journal.flush()

def shard_range(lo, hi, shard):
    k, n = shard
    return range(lo + k, hi, n)

# ---- per table row builders -------------------------------------------------
def basic_row(i, b):
    r = rnd('basic', i, b)
    return (i, b, 'a%d' % i, words(r, 12), r.randint(-2**31, 2**31-1), r.getrandbits(62), r.randint(-3000, 3000), r.randint(-100, 100),
            r.random() * 100, r.random() * 1e6, __import__('decimal').Decimal(r.randint(0, 10**9)) / 100, r.getrandbits(80),
            r.random() < .5, datetime.datetime(2020, 1, 1) + datetime.timedelta(seconds=r.randint(0, 10**8)),
            datetime.date(2000, 1, 1) + datetime.timedelta(days=r.randint(0, 9000)), r.randint(0, 86399 * 10**9),
            uuid.UUID(int=r.getrandbits(128), version=4), uuid_from_time(1.7e9 + r.randint(0, 10**7), 0x010203040506, r.getrandbits(14)),
            '10.%d.%d.%d' % (r.randint(0, 255), r.randint(0, 255), r.randint(1, 254)), bytes(r.getrandbits(8) for _ in range(64)),
            Duration(r.randint(0, 11), r.randint(0, 30), r.randint(0, 10**9)))

def load_basic(s, phase, shard):
    n = SIZES['basic']; half = n // 2
    ins = s.prepare('INSERT INTO fr.t_basic (id,bucket,a,t,i,b,s,ti,f,d,dec,vi,bo,ts,dt,tm,u,tu,ip,bl,du) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    rng = shard_range(0, half, shard) if phase == 'A' else shard_range(half, n, shard)
    run(s, ins, (basic_row(i, b) for i in rng for b in range(5)))
    if phase == 'B':   # shadowing overwrites of phase-A rows (newer ts), cell delete, row delete
        upd = s.prepare('UPDATE fr.t_basic USING TIMESTAMP %d SET t=?, i=? WHERE id=? AND bucket=?' % TS_DEL)
        run(s, upd, ((words(rnd('ov', i), 6), 7, i, 1) for i in shard_range(0, half, shard) if i % 50 == 0))
        dc = s.prepare('DELETE bl FROM t_basic USING TIMESTAMP %d WHERE id=? AND bucket=?' % TS_DEL, ) if False else s.prepare('DELETE bl FROM fr.t_basic USING TIMESTAMP %d WHERE id=? AND bucket=?' % TS_DEL)
        run(s, dc, ((i, 0) for i in shard_range(0, half, shard) if i % 97 == 0))
        dr = s.prepare('DELETE FROM fr.t_basic USING TIMESTAMP %d WHERE id=? AND bucket=?' % TS_DEL)
        run(s, dr, ((i, 2) for i in shard_range(0, half, shard) if i % 89 == 0))
        for i in (x for x in shard_range(0, half, shard) if x % 89 == 0 and x < 2000): J(table='t_basic', op='row-delete', id=i, bucket=2, ts=TS_DEL)

def load_coll(s, phase, shard):
    n = SIZES['coll']; half = n // 2
    stmt = s.prepare('INSERT INTO fr.t_coll (id,l,s,m,fl,fm,nested) VALUES (?,?,?,?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    def row(i):
        r = rnd('coll', i); k = r.randint(5, 20)
        return (uuid.UUID(int=i), [words(r, 4) for _ in range(k)], set(r.sample(range(1000), k)), {words(r, 3): r.randint(0, 99) for _ in range(k)},
                [r.randint(0, 99) for _ in range(k)], {words(r, 2): set(r.sample(range(50), 3)) for _ in range(3)},
                {words(r, 2): [words(r, 2) for _ in range(3)] for _ in range(k // 2 + 1)})
    run(s, stmt, (row(i) for i in (shard_range(0, half, shard) if phase == 'A' else shard_range(half, n, shard))))
    if phase == 'B':   # collection overwrite (full-collection tombstone), element update/delete, on phase-A rows
        ow = s.prepare('UPDATE fr.t_coll USING TIMESTAMP %d SET l=?, s=s+? WHERE id=?' % TS_DEL)
        run(s, ow, (([words(rnd('ol', i), 3)], {5000 + i % 7}, uuid.UUID(int=i)) for i in shard_range(0, half, shard) if i % 20 == 0))
        ed = s.prepare('DELETE m[?], s[?] FROM fr.t_coll USING TIMESTAMP %d WHERE id=?' % TS_DEL)
        run(s, ed, ((words(rnd('coll', i), 3), 0, uuid.UUID(int=i)) for i in shard_range(0, half, shard) if i % 30 == 0))

def udt_person(r):
    return {'name': words(r, 6), 'born': datetime.date(1950, 1, 1) + datetime.timedelta(days=r.randint(0, 20000)),
            'home': {'street': words(r, 8), 'city': words(r, 5), 'zip': r.randint(10000, 99999)}, 'tags': set(words(r, 2) for _ in range(3))}

def load_udt(s, phase, shard):
    n = SIZES['udt']; half = n // 2
    stmt = s.prepare('INSERT INTO fr.t_udt (id,ck,p,ps,tp) VALUES (?,?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    def row(i, c):
        r = rnd('udt', i, c)
        return (i, c, udt_person(r), [udt_person(r) for _ in range(r.randint(1, 4))], (r.randint(0, 9), words(r, 3), uuid.UUID(int=r.getrandbits(128), version=4)))
    # (the connect() session registers the UDT classes below)
    run(s, stmt, (row(i, c) for i in (shard_range(0, half, shard) if phase == 'A' else shard_range(half, n, shard)) for c in range(5)))
    if phase == 'B':
        up = s.prepare('UPDATE fr.t_udt USING TIMESTAMP %d SET p=? WHERE id=? AND ck=?' % TS_DEL)
        run(s, up, ((udt_person(rnd('ovu', i)), i, 1) for i in shard_range(0, half, shard) if i % 40 == 0))

def load_wide(s, phase, shard):
    stmt = s.prepare('INSERT INTO fr.t_wide (pk,ck,v,n) VALUES (?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    base = datetime.datetime(2026, 11, 1)
    big, small = SIZES['wide_big'], SIZES['wide_small']
    # pk 0 is the giant partition; pk 1..39 are small. half of each in each phase, ck ascending in time.
    def rows(pk, lo, hi):
        for j in range(lo, hi):
            r = rnd('wide', pk, j)
            yield (pk, base + datetime.timedelta(seconds=j), words(r, 60), r.randint(0, 10**6))
    from itertools import chain
    for pk, tot in [(0, big)] + [(p, small) for p in range(1, 40)]:
        lo, hi = (0, tot // 2) if phase == 'A' else (tot // 2, tot)
        lo2 = lo + (shard[0] * (hi - lo)) // shard[1]; hi2 = lo + ((shard[0] + 1) * (hi - lo)) // shard[1]
        run(s, stmt, rows(pk, lo2, hi2), conc=100, chunk=50000)

def load_static(s, phase, shard):
    n = SIZES['static']; half = n // 2
    ins = s.prepare('INSERT INTO fr.t_static (pk,c1,c2,c3,v) VALUES (?,?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    st = s.prepare('INSERT INTO fr.t_static (pk,c1,s1,s2) VALUES (?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    rng = shard_range(0, half, shard) if phase == 'A' else shard_range(half, n, shard)
    run(s, st, (('pk%d' % i, i % 7, words(rnd('s1', i), 8), {words(rnd('s2', i), 2): i % 100}) for i in rng if i % 3 == 0))   # statics on 1/3
    run(s, ins, (('pk%d' % i, i % 7, c2, 'c%d' % (c2 % 3), bytes(rnd('sv', i, c2).getrandbits(8) for _ in range(40))) for i in rng for c2 in range(10)))
    if phase == 'B':   # overwrite statics of phase-A partitions with newer ts
        up = s.prepare('UPDATE fr.t_static USING TIMESTAMP %d SET s1=? WHERE pk=? AND c1=?' % TS_DEL)
        run(s, up, ((words(rnd('s1n', i), 5), 'pk%d' % i, i % 7) for i in shard_range(0, half, shard) if i % 3 == 0 and i % 30 == 0))

def load_tomb(s, phase, shard):
    n = SIZES['tomb']
    ins = s.prepare('INSERT INTO fr.t_tomb (id,c1,c2,v,l,m) VALUES (?,?,?,?,?,?) USING TIMESTAMP %d' % TS_INS)
    insttl = s.prepare('INSERT INTO fr.t_tomb (id,c1,c2,v) VALUES (?,?,?,?) USING TIMESTAMP %d AND TTL ?' % TS_INS)
    def rows(i):
        for c1 in range(5):
            for c2 in range(4):
                r = rnd('tomb', i, c1, c2)
                if i % 10 == 6: continue
                yield (i, c1, c2, words(r, 8), [r.randint(0, 9) for _ in range(3)], {k: words(r, 2) for k in range(3)})
    half = n // 2
    rng = shard_range(0, half, shard) if phase == 'A' else shard_range(half, n, shard)
    run(s, ins, (x for i in rng for x in rows(i)))
    # TTL rows: ids %10==6 get short TTL (30 s, expired at snapshot) on even c1 and 30-day TTL on odd, ids %10==7 per-write TTL 30 d
    run(s, insttl, ((i, c1, c2, 'ttl%d' % i, 30 if (i % 10 == 6 and c1 % 2 == 0) else 2592000) for i in rng if i % 10 in (6, 7) for c1 in range(5) for c2 in range(4)))
    if phase == 'B':   # deletions of every kind against PHASE-A data (older generations): ids in [0, half)
        stmts = dict(
            part=s.prepare('DELETE FROM fr.t_tomb USING TIMESTAMP %d WHERE id=?' % TS_DEL),
            rng1=s.prepare('DELETE FROM fr.t_tomb USING TIMESTAMP %d WHERE id=? AND c1=?' % TS_DEL),
            rng2=s.prepare('DELETE FROM fr.t_tomb USING TIMESTAMP %d WHERE id=? AND c1=? AND c2>?' % TS_DEL),
            row=s.prepare('DELETE FROM fr.t_tomb USING TIMESTAMP %d WHERE id=? AND c1=? AND c2=?' % TS_DEL),
            cell=s.prepare('DELETE v FROM fr.t_tomb USING TIMESTAMP %d WHERE id=? AND c1=? AND c2=?' % TS_DEL),
            coll=s.prepare('UPDATE fr.t_tomb USING TIMESTAMP %d SET l=?, m[1]=null WHERE id=? AND c1=? AND c2=?' % TS_DEL),
            shadow=s.prepare('INSERT INTO fr.t_tomb (id,c1,c2,v) VALUES (?,?,?,?) USING TIMESTAMP %d' % TS_OLD))   # older than data: reconciles away
        for i in shard_range(0, half, shard):
            k = i % 10
            if k == 0: stmts_exec(s, stmts['part'], (i,)); J(table='t_tomb', op='partition-delete', id=i, ts=TS_DEL) if i < 500 else None
            elif k == 1: stmts_exec(s, stmts['rng1'], (i, 2)); J(table='t_tomb', op='range-delete-c1', id=i, c1=2, ts=TS_DEL) if i < 500 else None
            elif k == 2: stmts_exec(s, stmts['rng2'], (i, 3, 1)); J(table='t_tomb', op='range-delete-c1c2', id=i, c1=3, c2gt=1, ts=TS_DEL) if i < 500 else None
            elif k == 3: stmts_exec(s, stmts['row'], (i, 1, 1)); J(table='t_tomb', op='row-delete', id=i, c1=1, c2=1, ts=TS_DEL) if i < 500 else None
            elif k == 4: stmts_exec(s, stmts['cell'], (i, 0, 0)); J(table='t_tomb', op='cell-delete', id=i, c1=0, c2=0, col='v', ts=TS_DEL) if i < 500 else None
            elif k == 5: stmts_exec(s, stmts['coll'], ([9, 9], i, 4, 3)); J(table='t_tomb', op='collection-overwrite', id=i, c1=4, c2=3, ts=TS_DEL) if i < 500 else None
            elif k == 8: stmts_exec(s, stmts['shadow'], (i, 0, 0, 'older')); J(table='t_tomb', op='older-insert-shadowed', id=i, ts=TS_OLD) if i < 500 else None

def stmts_exec(s, st, params): s.execute(st, params)

LOADERS = dict(t_basic=load_basic, t_coll=load_coll, t_udt=load_udt, t_wide=load_wide, t_static=load_static, t_tomb=load_tomb)

def register_udts(c):
    class Addr: 
        def __init__(self, street, city, zip): self.street, self.city, self.zip = street, city, zip
    class Person:
        def __init__(self, name, born, home, tags): self.name, self.born, self.home, self.tags = name, born, home, tags
    c.register_user_type('fr', 'addr', Addr); c.register_user_type('fr', 'person', Person)
    return Addr, Person

def main():
    global journal
    ap = argparse.ArgumentParser(); ap.add_argument('cmd'); ap.add_argument('table', nargs='?'); ap.add_argument('phase', nargs='?')
    ap.add_argument('--shard', default='0/1'); ap.add_argument('--journal', default='/tmp/ops.jsonl')
    a = ap.parse_args(); journal = open(a.journal, 'a')
    shard = tuple(int(x) for x in a.shard.split('/'))
    c, s = connect()
    if a.cmd == 'ddl':
        for stmt in open('/gen/ddl.cql').read().split(';\n'):
            if stmt.strip(): s.execute(stmt); time.sleep(2)
        print('ddl ok'); return
    Addr, Person = register_udts(c)
    global udt_person
    _up = udt_person
    def udt_person(r, _up=_up):
        d = _up(r); return Person(d['name'], d['born'], Addr(**d['home']), d['tags'])
    LOADERS[a.table](s, a.phase, shard)
    print('done', a.table, a.phase, a.shard)
main()
