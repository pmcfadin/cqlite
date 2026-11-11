#!/usr/bin/env python3
"""splice-datasets-v4.py — assemble the datasets-v4 tree (issue #4492, part of #4482).

The v3.6 bundle is itself a splice (REGEN-NOTES-1935.md): table directory names (the
random table UUID suffix) and component prefixes (`nb-1-big`, ...) are pinned by manifests
and tests, so a regenerated table is renamed onto the OLD directory name and generation
prefixes instead of changing every reference.

    splice-datasets-v4.py --base <v3.6 datasets root> --gen <generator out dir> [--gen ...] \
                          --dest <new datasets root> [--report <file>]

`--base`  an extracted v3.6 `test-data/datasets` tree (read only).
`--gen`   one generator `--out` directory (contains `sstables/<ks>/<table>-<uuid>/`);
          repeatable. Every table found there REPLACES the same-named base table.
`--dest`  the new tree (created; must not exist). Everything not replaced is copied
          from the base byte-for-byte.

`--preserve KS.TABLE` (repeatable) keeps that table's directory byte-for-byte from the base
(bytes AND goldens), whichever --gen/--goldens would have replaced it: used for the aged TTL
seams and for tables whose committed content is pinned by absolute wall-clock/offset values.

`--goldens <dir>` overlays re-dumped goldens (see redump3.sh) for tables the generators did
not rebuild: `<dir>/sstables/<ks>/<table-dir>/*-Data.db.jsonl.raw` (raw sstabledump -l
output) and `*-Statistics.db.txt`. Each JSONL is written in the SAME style as the base golden
(compact `{"a":1}` vs sstabledump's native `{"a": 1}`), because tests match literals such as
`"type": "row"`.

Per replaced table the old directory name is kept, the new generation numbers are mapped
onto the old ones in sorted order, and any base file with no regenerated counterpart that
is not a standard SSTable component (e.g. `*.counter-select.txt`) is carried over.
"""
import argparse
import json
import os
import re
import shutil
import sys

COMP = re.compile(r"^(?P<fmt>[a-z]{2})-(?P<gen>\d+)-(?P<kind>big|bti)-(?P<rest>.+)$")
TABLE_DIR = re.compile(r"^(?P<name>.+)-(?P<uuid>[0-9a-f]{32})$")


def split_dir(d):
    m = TABLE_DIR.match(d)
    return (m.group("name"), m.group("uuid")) if m else (None, None)


def gens(files):
    out = set()
    for f in files:
        m = COMP.match(f)
        if m:
            out.add((m.group("fmt"), int(m.group("gen")), m.group("kind")))
    return sorted(out, key=lambda t: t[1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--gen", action="append", required=True)
    ap.add_argument("--dest", required=True)
    ap.add_argument("--preserve", action="append", default=[])
    ap.add_argument("--goldens")
    ap.add_argument("--report")
    a = ap.parse_args()
    if os.path.exists(a.dest):
        sys.exit("dest exists: " + a.dest)
    shutil.copytree(a.base, a.dest, symlinks=True)
    log = []
    problems = 0
    for g in a.gen:
        sst = os.path.join(g, "sstables")
        if not os.path.isdir(sst):  # e.g. generate-issue-3790 writes <out>/<ks>/ directly
            sst = g
        for ks in sorted(os.listdir(sst)):
            kd = os.path.join(sst, ks)
            if not os.path.isdir(kd):
                continue
            for d in sorted(os.listdir(kd)):
                name, _ = split_dir(d)
                if not name:
                    continue
                if f"{ks}.{name}" in a.preserve:
                    log.append(f"PRESERVED {ks}.{name} (verbatim from base)")
                    continue
                newd = os.path.join(kd, d)
                newfiles = sorted(os.listdir(newd))
                if not any(f.endswith("-Data.db") for f in newfiles):
                    continue
                bks = os.path.join(a.dest, "sstables", ks)
                olds = [x for x in sorted(os.listdir(bks))
                        if split_dir(x)[0] == name
                        and any(f.endswith("-Data.db") for f in os.listdir(os.path.join(bks, x)))] \
                    if os.path.isdir(bks) else []
                if len(olds) != 1:
                    log.append(f"PROBLEM {ks}.{name}: {len(olds)} base dirs with Data.db (need exactly 1)")
                    problems += 1
                    continue
                oldd = os.path.join(bks, olds[0])
                og, ng = gens(os.listdir(oldd)), gens(newfiles)
                if len(og) != len(ng):
                    log.append(f"PROBLEM {ks}.{name}: base has {len(og)} generations, regenerated {len(ng)}")
                    problems += 1
                    continue
                gmap = {n[1]: o for o, n in zip(og, ng)}
                keep = [f for f in os.listdir(oldd) if not COMP.match(f)]
                for f in os.listdir(oldd):
                    if COMP.match(f):
                        os.remove(os.path.join(oldd, f))
                for f in newfiles:
                    m = COMP.match(f)
                    if not m:
                        if f not in keep:
                            keep.append(f)
                            shutil.copy2(os.path.join(newd, f), os.path.join(oldd, f))
                        continue
                    ofmt, ogen, okind = gmap[int(m.group("gen"))]
                    dst = f"{ofmt}-{ogen}-{okind}-{m.group('rest')}"
                    shutil.copy2(os.path.join(newd, f), os.path.join(oldd, dst))
                maps = ",".join(f"{n[0]}-{n[1]}->{o[0]}-{o[1]}" for o, n in zip(og, ng))
                log.append(f"REPLACED {ks}.{name} -> {olds[0]} gens[{maps}] kept-extra={sorted(set(keep))}")
    if a.goldens:
        gs = os.path.join(a.goldens, "sstables")
        for ks in sorted(os.listdir(gs)):
            for d in sorted(os.listdir(os.path.join(gs, ks))):
                name, _ = split_dir(d)
                if f"{ks}.{name}" in a.preserve:
                    log.append(f"PRESERVED {ks}.{name} (verbatim from base, goldens not re-dumped)")
                    continue
                src = os.path.join(gs, ks, d)
                dst = os.path.join(a.dest, "sstables", ks, d)
                for f in sorted(os.listdir(src)):
                    if f.endswith(".jsonl.raw"):
                        target = os.path.join(dst, f[: -len(".raw")])
                        raw = open(os.path.join(src, f)).read()
                        lines = [l for l in raw.splitlines() if l.strip()]
                        old = [l for l in open(target).read().splitlines() if l.strip()] if os.path.exists(target) else []
                        # pick the JSON dialect that reproduces EVERY base line exactly
                        styles = [((",", ":"), False), ((",", ":"), True), ((",", ": "), False), ((",", ": "), True)]
                        style = next((st for st in styles
                                      if all(l == json.dumps(json.loads(l), separators=st[0], ensure_ascii=st[1]) for l in old)),
                                     styles[0])
                        out = [json.dumps(json.loads(l), separators=style[0], ensure_ascii=style[1]) for l in lines]
                        compact = style[0] == (",", ":")
                        open(target, "w").write("\n".join(out) + ("\n" if out else ""))
                        log.append(f"GOLDEN {ks}/{d}/{f[:-4]} style={'compact' if compact else 'spaced'}{'' if style[1] else '+utf8'} lines={len(out)}")
                    elif f.endswith("Statistics.db.txt"):
                        shutil.copy2(os.path.join(src, f), os.path.join(dst, f))
    text = "\n".join(log) + "\n"
    if a.report:
        open(a.report, "w").write(text)
    sys.stdout.write(text)
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
