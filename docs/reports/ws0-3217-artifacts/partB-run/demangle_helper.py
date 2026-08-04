"""Shared Rust symbol demangling for the #3217 Part B parsers.

bcc AND bpftrace both emit RAW Rust symbols; only `perf script` demangles. A
frame that cannot be decoded is returned UNCHANGED — never invented.
"""
from __future__ import annotations

import re

try:
    import rust_demangler
except ImportError:  # pragma: no cover
    rust_demangler = None

_OFFSET = re.compile(r"\+0x[0-9a-fA-F]+$|\+\d+$")
_LEGACY = re.compile(r"^_ZN(.+)E$")
_HASHSUF = re.compile(r"^h[0-9a-f]{16}$")
_cache: dict[str, str] = {}


def _legacy(sym: str) -> str | None:
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


def demangle_frame(frame: str) -> str:
    if frame in _cache:
        return _cache[frame]
    s = _OFFSET.sub("", frame.strip())
    out = s
    if s.startswith("_R") and rust_demangler is not None:
        try:
            d = rust_demangler.demangle(s)
            if d:
                out = d
        except Exception:
            pass
    elif s.startswith("_ZN"):
        out = _legacy(s) or s
    _cache[frame] = out
    return out
