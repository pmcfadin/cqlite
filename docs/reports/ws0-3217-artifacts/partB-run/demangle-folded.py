#!/usr/bin/env python3
"""Demangle Rust symbols in a bcc-produced folded stack file. Issue #3217 Part B.

WHY THIS EXISTS. `perf script` demangles Rust automatically, so the AC3 on-CPU
folded files arrive readable (`cqlite_flight::shutdown::shutdown_signal`). bcc's
`offcputime` does NOT: its user frames come out RAW v0-mangled
(`_RNvNvMs0_NtNtNtCs2AWtUsOyxgP_3std3sys6thread4unix...`). The AC4 classifier
matches on readable substrings (`tokio::runtime::scheduler`, `ChannelSink`), so
against raw bcc output it matched almost nothing and dumped ~83% of blocked time
into `other`. That was a TOOLING artefact, not a measurement result.

Handles both Rust manglings:
  * v0   `_R...`   via the `rust_demangler` package
  * legacy `_ZN...E` via a length-prefixed path walk (no c++filt dependency)
Anything that demangles to nothing, or is not a Rust symbol (kernel frames,
`[unknown]`, C symbols), is passed through UNCHANGED — this must never invent a
frame it could not decode.

Usage: demangle-folded.py <in.folded> <out.folded> [--report <json>]
"""
from __future__ import annotations

import argparse
import json
import re
import sys

try:
    import rust_demangler
except ImportError:  # pragma: no cover - environment guard
    rust_demangler = None

_LEGACY = re.compile(r"^_ZN(.+)E$")
_HASHSUF = re.compile(r"^h[0-9a-f]{16}$")


def _legacy(sym: str) -> str | None:
    """`_ZN5tokio7runtime17h0123456789abcdefE` -> `tokio::runtime`."""
    m = _LEGACY.match(sym)
    if not m:
        return None
    body, out, i = m.group(1), [], 0
    while i < len(body):
        j = i
        while j < len(body) and body[j].isdigit():
            j += 1
        if j == i:
            return None
        n = int(body[i:j])
        seg = body[j:j + n]
        if len(seg) != n:
            return None
        if not _HASHSUF.match(seg):
            out.append(seg)
        i = j + n
    return "::".join(out) if out else None


def demangle(sym: str) -> tuple[str, bool]:
    """-> (symbol, changed). Never raises; an undecodable symbol passes through."""
    s = sym.strip()
    if s.startswith("_R") and rust_demangler is not None:
        try:
            d = rust_demangler.demangle(s)
            if d:
                return d, True
        except Exception:
            return sym, False
    if s.startswith("_ZN"):
        d = _legacy(s)
        if d:
            return d, True
    return sym, False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--report")
    a = ap.parse_args()
    if rust_demangler is None:
        print("ERROR: rust_demangler unavailable; refusing to emit a "
              "silently-undemangled file", file=sys.stderr)
        return 2

    cache: dict[str, tuple[str, bool]] = {}
    frames = changed = lines = 0
    with open(a.src) as fh, open(a.dst, "w") as out:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            try:
                stack, val = line.rsplit(" ", 1)
                float(val)
            except ValueError:
                out.write(line + "\n")
                continue
            lines += 1
            newf = []
            for f in stack.split(";"):
                if f not in cache:
                    cache[f] = demangle(f)
                d, ch = cache[f]
                frames += 1
                changed += 1 if ch else 0
                newf.append(d)
            out.write(";".join(newf) + " " + val + "\n")

    rep = {
        "schema": "ws0-3217.demangle-folded/v1",
        "src": a.src, "dst": a.dst,
        "folded_lines": lines,
        "frame_instances": frames,
        "frame_instances_demangled": changed,
        "unique_frames": len(cache),
        "unique_frames_demangled": sum(1 for v in cache.values() if v[1]),
        "note": ("bcc/offcputime emits RAW Rust symbols; perf script demangles on its own. "
                 "Undecodable and non-Rust frames pass through unchanged."),
    }
    if a.report:
        open(a.report, "w").write(json.dumps(rep, indent=1) + "\n")
    print(json.dumps(rep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
