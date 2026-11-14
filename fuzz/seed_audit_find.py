#!/usr/bin/env python3
"""Seed `fuzz/corpus/fuzz_audit` and `fuzz/corpus/fuzz_find` from the corruption corpus
(issue #4502, criterion 2).

Input layout (see `fuzz_targets/support/mod.rs`): byte 0 = control (bit7 high/mode, bit6 flag,
bits0-3 component index), bytes 1.. = the replacement component. Seeds, per BASE generation:

  * every component of every `corruption/test_comp_corrupt/<fixture>/` (the
    `generate-corruption-corpus.sh` outputs) that DIFFERS from the base's clean component — the
    exact corrupted bytes the verifier tests pin;
  * the clean components themselves (the fuzzer mutates from valid structure), and each
    component truncated to 0, 1 and 3 bytes and to half its length (the unreadable shapes the
    property asserts on);
  * for `fuzz_find` CommitLog mode: the committed `commitlog_find` segment (clean), its
    truncations, and the `commitlog/` corruption corpus segments (corrupt-crc, truncated).

Seeds are small (largest component ~28 KB). Re-run with CQLITE_DATASETS_ROOT set; the output is
committed. Deterministic: names are `seed_<control byte>_<content hash>`.
"""
import glob
import hashlib
import os
import sys

ROOT = os.environ.get("CQLITE_DATASETS_ROOT")
if not ROOT:
    sys.exit("set CQLITE_DATASETS_ROOT to the datasets root (see test-data/scripts/fetch-datasets.sh)")
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPONENTS = ["Data.db", "Index.db", "Summary.db", "Filter.db", "CompressionInfo.db",
              "Statistics.db", "Digest.crc32", "TOC.txt", "Partitions.db", "Rows.db"]
MAX = 64 * 1024


def comp_of(name):
    for i, c in enumerate(COMPONENTS):
        if name.endswith(c) and not name.endswith(".txt.jsonl") and not name.endswith(".db.txt"):
            return i
    return None


def table_dir(ks, table):
    d = sorted(glob.glob(f"{ROOT}/sstables/{ks}/{table}-????????????????????????????????"))
    return d[0] if d else None


def components(d):
    out = {}
    for f in sorted(os.listdir(d)):
        i = comp_of(f)
        if i is not None:
            out[i] = open(os.path.join(d, f), "rb").read()
    return out


def emit(corpus, ctl, payload):
    if len(payload) > MAX:
        return 0
    data = bytes([ctl]) + payload
    # `seed_` prefix: fuzz/.gitignore keeps `corpus/*/seed_*` and ignores fuzzer-grown inputs.
    path = os.path.join(corpus, f"seed_{ctl:02x}_{hashlib.sha1(data).hexdigest()[:12]}")
    if not os.path.exists(path):
        open(path, "wb").write(data)
        return 1
    return 0


def variants(payload):
    yield payload
    for n in (0, 1, 3):
        yield payload[:n]
    yield payload[: len(payload) // 2]


def seed_dir(corpus, base_comps, high, flag, extra_dirs):
    n = 0
    for idx, clean in base_comps.items():
        ctl = (0x80 if high else 0) | (0x40 if flag else 0) | idx
        for v in variants(clean):
            n += emit(corpus, ctl, v)
    for d in extra_dirs:
        for idx, b in components(d).items():
            if base_comps.get(idx) != b:  # the corrupted component
                ctl = (0x80 if high else 0) | (0x40 if flag else 0) | idx
                n += emit(corpus, ctl, b)
    return n


corrupt = sorted(glob.glob(f"{ROOT}/corruption/test_comp_corrupt/*/"))
big_corrupt = [d for d in corrupt if glob.glob(d + "nb-*-big-Data.db")]
bti_corrupt = [d for d in corrupt if glob.glob(d + "da-*-bti-Data.db")]
total = {}
for tool in ("fuzz_audit", "fuzz_find"):
    corpus = os.path.join(REPO, "fuzz", "corpus", tool)
    os.makedirs(corpus, exist_ok=True)
    n = 0
    big = table_dir("test_comp", "lz4_table")
    bti = table_dir("test_da", "wide_table")
    for base, hi_bit, bc in ((big, False, big_corrupt), (bti, True, bti_corrupt)):
        if base is None:
            sys.exit(f"missing base generation for {tool}: {base}")
        for deep in (False, True):
            # audit: bit7 = BTI base, bit6 = deep.  find: bit6 = BTI base (bit7 = commitlog mode).
            if tool == "fuzz_audit":
                n += seed_dir(corpus, components(base), hi_bit, deep, bc)
            elif deep is False:
                n += seed_dir(corpus, components(base), False, hi_bit, bc)
    if tool == "fuzz_find":
        segs = sorted(glob.glob(f"{REPO}/test-data/datasets/commitlog_find/commitlog/*.log")
                      + glob.glob(f"{ROOT}/commitlog/*.log"))
        for s in segs:
            raw = open(s, "rb").read()
            for v in variants(raw):
                n += emit(corpus, 0x80, v)
    total[tool] = n
print(total)
