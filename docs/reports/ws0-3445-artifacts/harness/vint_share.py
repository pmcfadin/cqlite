#!/usr/bin/env python3
"""vint_share.py — attribute sampled scan cycles to the INLINED VInt decoder (issue #3445, AC1).

WHAT PROBLEM THIS SOLVES
------------------------
`decode_unsigned` is `#[inline]` and carries no symbol in the measured binary, so
function-level `perf report` cannot see it: its cycles are booked to whatever caller it
was inlined into. #3027 hit exactly that wall and could only report the 0.74% of a NAMED
wrapper symbol, explicitly as a floor.

This script reads through the inlining. For every sample it asks DWARF for the FULL INLINE
CHAIN at the sampled address and classifies the sample by whether `decode_unsigned` /
`decode_signed` appears anywhere in that chain. That is an exact test, not a heuristic: the
chain is the compiler's own record of which source functions that instruction came from.
It handles the case a source-line test gets wrong — an instruction whose innermost line is
`core/src/num/uint_macros.rs:201` (`<u8>::leading_ones`, inlined INTO the decoder) is VInt
work, and only the chain says so.

TWO BOUNDARIES ARE REPORTED, because "the vint share" is ambiguous and the ambiguity is
worth more than a single number:

  narrow  the decoder proper -- a chain frame in DECODER_FRAMES
          (`decode_unsigned`, `decode_signed`, `zigzag_decode`).
  wide    the whole read-side VInt module surface -- any chain frame in
          `cqlite_core::parser::vint`, which adds the `parse_vuint` / `parse_vint` nom
          adapters (slice re-framing and error mapping) that a call site pays to use the
          decoder at all.

Anything reachable only through the WRITE-side `storage::serialization::vint` is counted
separately and is expected to be ~0 on a scan; that expectation is a CHECK, not an
assumption -- a non-trivial write-side share would mean this harness is measuring the wrong
thing and is reported rather than dropped.

WHY THE SELF-CHECK IS NOT OPTIONAL
----------------------------------
The binary is a PIE, so a sampled runtime IP must be rebased to a file address before DWARF
can be asked about it. Getting that base wrong does not fail loudly -- it yields a complete,
confident, entirely wrong attribution table, which is the silent-instrument shape #3217 lost
a 50.57s bucket to. So the base is derived from the PERF_RECORD_MMAP2 record and then
VERIFIED: for every distinct address, the symbol `nm` reports at the computed file address
must equal the symbol `perf` independently reported for that sample. A mismatch rate above
`--max-symbol-mismatch` REFUSES to print a share.

SKID
----
This host exposes no PEBS (`cycles:pp` is `<not supported>` -- see
`../raw/counter-capability-census.md`), so sample IPs are NOT precise: the recorded IP can
sit a few instructions past the instruction that actually consumed the cycle. A VInt region
is short, so that matters, and it is quantified rather than waved at: the share is
recomputed with every sample re-attributed K instructions EARLIER, for K in 0..--skid-max,
using the binary's own instruction sequence. The spread across K is reported as the skid
band. It is a sensitivity band, not an error bar -- it states how much the answer could move
under a bounded mis-attribution, which is the honest form of "we cannot resolve below this".
"""
from __future__ import annotations

import argparse
import bisect
import collections
import json
import re
import subprocess
import sys

# The decoder proper. `zigzag_decode` is included because `decode_signed` is defined as
# `decode_unsigned` + ZigZag unmap, so the unmap is decode work by construction.
DECODER_FRAMES = (
    "cqlite_core::parser::vint::decode_unsigned",
    "cqlite_core::parser::vint::decode_signed",
    "cqlite_core::parser::vint::zigzag_decode",
)
READ_VINT_MODULE = "cqlite_core::parser::vint::"
WRITE_VINT_MODULE = "cqlite_core::storage::serialization::vint::"


def run(cmd: list[str], **kw) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw).stdout


def mmap_record(perf_data: str, binary: str) -> tuple[int, int]:
    """(mapped runtime vaddr, mapped file offset) for `binary`'s executable mapping.

    Read from the trace rather than assumed, so the result is correct under ASLR.
    """
    out = run(["perf", "script", "-i", perf_data, "--show-mmap-events"])
    pat = re.compile(r"PERF_RECORD_MMAP2[^:]*: \[0x([0-9a-f]+)\((0x[0-9a-f]+)\) @ (0x[0-9a-f]+)")
    for line in out.splitlines():
        if "PERF_RECORD_MMAP2" not in line or binary not in line:
            continue
        m = pat.search(line)
        if m:
            return int(m.group(1), 16), int(m.group(3), 16)
    raise SystemExit(f"vint_share.py: no PERF_RECORD_MMAP2 for {binary} in {perf_data}")


def pie_bias(binary: str, map_vaddr: int, map_fileoff: int) -> int:
    """Load bias, so that `file_addr = runtime_ip - bias` with `file_addr` in nm's space.

    A MMAP2 record carries the mapping's FILE OFFSET, which is NOT the segment's virtual
    address: this binary's executable LOAD segment has p_offset 0x102be0 against p_vaddr
    0x103be0, a 0x1000 difference, and mapping offsets are page-aligned down on top of
    that. Subtracting the file offset directly (the obvious reading of the record) puts
    every address 0x1000 out -- which resolves to real symbols and real line numbers, just
    the WRONG ones. That is the whole reason the caller's symbol self-check exists, and it
    is what caught this during development rather than after publication.

    Within one LOAD segment, file offset and virtual address advance together, so
        seg_vaddr_at(map_fileoff) = p_vaddr + (map_fileoff - p_offset)
    and the bias is the difference between where the kernel put it and that.
    """
    out = run(["readelf", "-lW", binary])
    for line in out.splitlines():
        f = line.split()
        if len(f) < 8 or f[0] != "LOAD":
            continue
        # The mapping under measurement is the EXECUTABLE one (`r-xp`), and selecting by
        # file-offset containment alone picks the WRONG segment: this binary's read-only
        # LOAD spans [0, 0x102be0) and so also contains the page-aligned offset 0x102000
        # that the executable segment was mapped at. That mis-selection is precisely the
        # off-by-0x1000 the self-check rejected during development.
        if "E" not in f[6:]:
            continue
        p_offset, p_vaddr = int(f[1], 16), int(f[2], 16)
        filesz = int(f[4], 16)
        # Page-align the segment's file range down the way the kernel maps it.
        lo = p_offset & ~0xFFF
        if lo <= map_fileoff < p_offset + filesz:
            return map_vaddr - (p_vaddr + (map_fileoff - p_offset))
    raise SystemExit(
        f"vint_share.py: no LOAD segment of {binary} covers mapped file offset "
        f"0x{map_fileoff:x} -- refusing rather than guessing a bias"
    )


def nm_symbols(binary: str) -> tuple[list[int], list[tuple[int, str]]]:
    """Sorted (address, name) symbol table with sizes, for the base self-check."""
    syms: list[tuple[int, str]] = []
    for line in run(["nm", "-C", "--defined-only", binary]).splitlines():
        parts = line.split(" ", 2)
        if len(parts) == 3 and parts[1].lower() in ("t", "w"):
            try:
                syms.append((int(parts[0], 16), parts[2].strip()))
            except ValueError:
                continue
    syms.sort()
    return [a for a, _ in syms], syms


def instruction_addresses(binary: str) -> list[int]:
    """Every instruction address in `.text`, ascending — the skid walk's step unit."""
    out = run(["objdump", "-d", "--no-show-raw-insn", binary])
    addrs = []
    for line in out.splitlines():
        m = re.match(r"^\s+([0-9a-f]+):\t", line)
        if m:
            addrs.append(int(m.group(1), 16))
    addrs.sort()
    return addrs


def read_samples(perf_data: str, binary: str) -> tuple[collections.Counter, dict[int, str], int, int]:
    """(cycles by file address in `binary`, perf's symbol per address, cycles total, cycles off-binary).

    The denominator is EVERY sample in the window, including libc and kernel: the question
    is a share of scan on-CPU, so work outside the binary must stay in the denominator.
    """
    map_vaddr, map_fileoff = mmap_record(perf_data, binary)
    base = pie_bias(binary, map_vaddr, map_fileoff)
    out = run(["perf", "script", "-i", perf_data, "-F", "ip,dso,sym,symoff,period"])
    by_addr: collections.Counter = collections.Counter()
    persym: dict[int, str] = {}
    total = other = 0
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        # `-F ip,dso,sym,symoff,period` renders as: PERIOD IP SYM+0xOFF (DSO)
        try:
            period = int(parts[0])
        except (ValueError, IndexError):
            continue
        total += period
        if binary not in line:
            other += period
            continue
        try:
            ip = int(parts[1], 16)
        except (ValueError, IndexError):
            other += period
            continue
        fa = ip - base
        by_addr[fa] += period
        if fa not in persym:
            m = re.match(r"^(.*)\+0x[0-9a-f]+$", " ".join(parts[2:]).rsplit(" (", 1)[0])
            persym[fa] = m.group(1) if m else ""
    return by_addr, persym, total, other


def verify_base(persym: dict[int, str], sym_addrs: list[int], syms: list[tuple[int, str]]) -> tuple[int, int]:
    """Do nm and perf agree about which symbol each computed file address lies in?"""
    ok = bad = 0
    for fa, pname in persym.items():
        if not pname:
            continue
        i = bisect.bisect_right(sym_addrs, fa) - 1
        nname = syms[i][1] if i >= 0 else ""
        # perf and nm render templates/closures slightly differently; compare on a
        # normalised form rather than demanding byte equality of two demanglers.
        norm = lambda s: re.sub(r"[<>\s]", "", s)
        if nname and (norm(nname) == norm(pname) or norm(pname) in norm(nname) or norm(nname) in norm(pname)):
            ok += 1
        else:
            bad += 1
    return ok, bad


def inline_chains(binary: str, addrs: list[int]) -> dict[int, list[str]]:
    """file address -> full inline chain (innermost first), from DWARF via addr2line -i."""
    if not addrs:
        return {}
    stdin = "\n".join(f"0x{a:x}" for a in addrs) + "\n"
    out = subprocess.run(
        ["addr2line", "-e", binary, "-i", "-f", "-C", "-a"],
        input=stdin, capture_output=True, text=True, check=True,
    ).stdout
    chains: dict[int, list[str]] = {}
    cur: int | None = None
    expect_func = True
    for line in out.splitlines():
        if line.startswith("0x"):
            cur = int(line.strip(), 16)
            chains[cur] = []
            expect_func = True
            continue
        if cur is None:
            continue
        # addr2line -f -i alternates FUNCTION then FILE:LINE for each inline level.
        if expect_func:
            chains[cur].append(line.strip())
        expect_func = not expect_func
    return chains


def classify(chain: list[str]) -> str:
    if any(f.startswith(d) for f in chain for d in DECODER_FRAMES):
        return "narrow"
    if any(READ_VINT_MODULE in f for f in chain):
        return "wide_only"
    if any(WRITE_VINT_MODULE in f for f in chain):
        return "write_vint"
    return "other"


def shares(by_addr: collections.Counter, chains: dict[int, list[str]], total: int) -> dict:
    buckets: collections.Counter = collections.Counter()
    for fa, cyc in by_addr.items():
        buckets[classify(chains.get(fa, []))] += cyc
    narrow = buckets["narrow"]
    wide = narrow + buckets["wide_only"]
    return {
        "narrow_cycles": narrow,
        "wide_cycles": wide,
        "write_vint_cycles": buckets["write_vint"],
        "narrow_pct": 100.0 * narrow / total if total else 0.0,
        "wide_pct": 100.0 * wide / total if total else 0.0,
        "write_vint_pct": 100.0 * buckets["write_vint"] / total if total else 0.0,
    }


def skid_shift(by_addr: collections.Counter, insns: list[int], k: int) -> collections.Counter:
    """Re-attribute every sample K instructions EARLIER in program order."""
    if k == 0:
        return by_addr
    shifted: collections.Counter = collections.Counter()
    for fa, cyc in by_addr.items():
        i = bisect.bisect_left(insns, fa)
        j = max(0, i - k)
        shifted[insns[j] if insns else fa] += cyc
    return shifted


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--perf-data", required=True)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--json-out")
    ap.add_argument("--skid-max", type=int, default=3)
    ap.add_argument("--max-symbol-mismatch", type=float, default=0.5,
                    help="percent of addresses where nm and perf may disagree before this "
                         "REFUSES to report a share (default 0.5)")
    args = ap.parse_args()

    by_addr, persym, total, other = read_samples(args.perf_data, args.binary)
    if total == 0:
        print("vint_share.py: REFUSED — zero cycles sampled; a 0-sample window is a failure, "
              "never a measurement", file=sys.stderr)
        return 1

    sym_addrs, syms = nm_symbols(args.binary)
    ok, bad = verify_base(persym, sym_addrs, syms)
    checked = ok + bad
    mismatch_pct = 100.0 * bad / checked if checked else 100.0
    if mismatch_pct > args.max_symbol_mismatch:
        print(f"vint_share.py: REFUSED — PIE rebase self-check failed: nm and perf disagree on "
              f"{bad}/{checked} addresses ({mismatch_pct:.2f}% > {args.max_symbol_mismatch}%). "
              f"A wrong base yields a confident WRONG table, so no share is printed.",
              file=sys.stderr)
        return 1

    insns = instruction_addresses(args.binary)
    result = {
        "perf_data": args.perf_data,
        "binary": args.binary,
        "cycles_total": total,
        "cycles_outside_binary": other,
        "pct_outside_binary": 100.0 * other / total,
        "rebase_selfcheck": {"addresses_checked": checked, "mismatches": bad,
                             "mismatch_pct": mismatch_pct, "verdict": "PASS"},
        "skid_band": {},
    }
    for k in range(0, args.skid_max + 1):
        shifted = skid_shift(by_addr, insns, k)
        chains = inline_chains(args.binary, sorted(shifted))
        result["skid_band"][f"k={k}"] = shares(shifted, chains, total)
    base_row = result["skid_band"]["k=0"]
    result.update({k: base_row[k] for k in base_row})
    ks = list(result["skid_band"].values())
    result["narrow_pct_min"] = min(r["narrow_pct"] for r in ks)
    result["narrow_pct_max"] = max(r["narrow_pct"] for r in ks)
    result["wide_pct_min"] = min(r["wide_pct"] for r in ks)
    result["wide_pct_max"] = max(r["wide_pct"] for r in ks)

    print(json.dumps(result, indent=2))
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(result, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
