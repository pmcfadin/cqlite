#!/usr/bin/env python3
"""Aggregate perf flat self-time into the AC1/AC2 shared-vs-Flight-marginal split."""
import re, subprocess, sys, json, collections

# Classification is by SYMBOL, from the verified code map. SHARED = executed by both arms;
# MARGINAL = arm A only. Anything unmatched is UNCLASSIFIED and reported as such -- never
# silently folded into either side, because that is where a wrong split would hide.
SHARED = [
    r'row_build::build_row_from_scan', r'types::Value>::into_owned', r'ScanRow',
    r'PartitionKeyCache', r'storage::sstable::reader', r'value_borrow',
    r'QueryRow', r'hashbrown::map::HashMap<alloc::sync::Arc<str>',
    r'hash::sip::Hasher', r'BuildHasher>::hash_one', r'from_utf8',
    r'mpsc::bounded::Sender', r'to_lowercase',
]
MARGINAL = [
    r'export::arrow_size', r'export::arrow_convert', r'export::arrow_columnar',
    r'export::arrow_builders', r'arrow_size_shape', r'arrow_size_render',
    r'estimate_arrow_row_bytes', r'batch_bytes', r'egress_flush', r'egress_credit',
    r'producer_stream', r'cqlite_flight::producer', r'row_source',
    r'arrow_flight::', r'arrow_ipc', r'arrow_array', r'arrow_buffer', r'arrow_data',
    r'arrow_schema', r'cqlite_flight::streaming', r'write_engine::merge',
    r'RecordBatch', r'tonic::', r'h2::', r'hyper::', r'prost',
]
def classify(sym, dso):
    for p in MARGINAL:
        if re.search(p, sym): return "flight-marginal"
    for p in SHARED:
        if re.search(p, sym): return "shared"
    if dso == 'libc.so.6': return "libc(alloc/mem)"
    if 'kallsyms' in dso: return "kernel"
    if dso.startswith('ld-linux'): return "libc(alloc/mem)"
    return "unclassified"

def load(path):
    out = subprocess.run(["perf","report","-i",path,"--stdio","--no-children",
                          "--percent-limit","0.01","-q"],
                         capture_output=True, text=True).stdout
    rows=[]
    for line in out.splitlines():
        m = re.match(r'\s*([\d.]+)%\s+(\S+)\s+(\S+)\s+\[[.k]\]\s+(.*)', line)
        if m:
            pct, comm, dso, sym = float(m.group(1)), m.group(2), m.group(3), m.group(4).strip()
            rows.append((pct, comm, dso, sym))
    return rows

def summarize(label, path):
    rows = load(path)
    total = sum(r[0] for r in rows)
    buckets = collections.Counter()
    for pct, comm, dso, sym in rows:
        buckets[classify(sym, dso)] += pct
    print(f"\n## {label}")
    print(f"symbols>=0.01%: {len(rows)}  accounted: {total:.1f}%")
    for k in ("shared","flight-marginal","libc(alloc/mem)","kernel","unclassified"):
        print(f"  {k:18s} {buckets[k]:6.2f}%")
    print(f"  top symbols:")
    for pct, comm, dso, sym in rows[:12]:
        short = sym.replace("cqlite_core::","cc::").replace("cqlite_flight::","cf::")
        print(f"    {pct:5.2f}%  [{classify(sym,dso)[:8]:8s}] {short[:96]}")
    return buckets, rows

if __name__ == "__main__":
    for label, path in zip(sys.argv[1::2], sys.argv[2::2]):
        summarize(label, path)
