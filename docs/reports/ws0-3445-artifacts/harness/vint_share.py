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
# Page size used to align a segment's file range the way the kernel maps it. 4 KiB is the
# x86-64 base page and matches every LOAD alignment observed here; a host with a different
# base page would mis-align this, which is why the caller's symbol self-check is what
# actually certifies the rebase rather than this constant.
PAGE_SIZE = 4096

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
        lo = p_offset & ~(PAGE_SIZE - 1)
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


def read_samples(perf_data: str, binary: str) -> tuple[collections.Counter, dict[int, str], int, int, int]:
    """(cycles by file address in `binary`, perf's symbol per address, total, off-binary, unparsed).

    THE DENOMINATOR IS THE LARGEST JUDGEMENT CALL IN THIS SCRIPT, so it is returned in
    parts rather than as one number the caller has to trust. `total` is EVERY sample in the
    window, including libc and the kernel, because the question asked is a share of scan
    ON-CPU. But 43-58% of that is off-binary and is UNREACHABLE by the numerator (only the
    measured binary has DWARF for an inline chain), so a share against `total` and a share
    against the in-binary subset are different quantities and BOTH are reported. Comparing
    two shares taken against DIFFERENT denominators is how this script's first user
    (me) published an AC2 ratio with the wrong sign.

    `unparsed` exists because a line landing in neither `total` nor `other` would vanish
    silently, which is an unmeasured quantity masquerading as zero.
    """
    map_vaddr, map_fileoff = mmap_record(perf_data, binary)
    base = pie_bias(binary, map_vaddr, map_fileoff)
    out = run(["perf", "script", "-i", perf_data, "-F", "ip,dso,sym,symoff,period"])
    by_addr: collections.Counter = collections.Counter()
    persym: dict[int, str] = {}
    total = other = unparsed = 0
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        # `-F ip,dso,sym,symoff,period` renders as: PERIOD IP SYM+0xOFF (DSO)
        try:
            period = int(parts[0])
        except (ValueError, IndexError):
            unparsed += 1
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
            m = re.match(r"^(.*)\+0x([0-9a-f]+)$", " ".join(parts[2:]).rsplit(" (", 1)[0])
            persym[fa] = (m.group(1), int(m.group(2), 16)) if m else ("", None)
    return by_addr, persym, total, other, unparsed


def verify_base(persym: dict[int, tuple[str, int | None]], sym_addrs: list[int],
                syms: list[tuple[int, str]]) -> tuple[int, int]:
    """Is the PIE rebase right? Answered by ADDRESS, not by comparing two demanglers.

    perf reports each sample as `symbol + symoff`, so `file_addr - symoff` is where perf
    thinks that symbol STARTS, in file-address space. If the rebase is correct that value
    must land exactly on a symbol start in nm's table. Exact arithmetic on both sides, and
    no dependence on how anything is spelled.

    Two name-based versions were tried and both were wrong instruments:
      * bidirectional substring -- accepts `foo` against `foo_inner`, i.e. a genuinely
        wrong symbol, which is what this check exists to reject;
      * normalised equality -- REJECTED 32 of 3430 addresses on this binary where perf
        renders LLVM's local-copy suffix (`…::clone.3959`) and nm renders `…::clone`. A
        pure rendering difference, so that version red on correct input.
    Comparing addresses removes the whole class: there is nothing left to spell.
    """
    starts = set(sym_addrs)
    ok = bad = 0
    for fa, (pname, symoff) in persym.items():
        if not pname or symoff is None:
            continue
        if (fa - symoff) in starts:
            ok += 1
        else:
            bad += 1
    return ok, bad


def inline_chains(binary: str, addrs: list[int]) -> dict[int, list[tuple[str, str]]]:
    """file address -> inline chain as (function, source-location) pairs, innermost first.

    BOTH HALVES ARE PRESERVED, and that is the point. `addr2line -i -f` emits a FUNCTION line
    then a FILE:LINE line per inline level. Two rounds of review landed here, each on a
    different way of throwing half of it away:

      round 1: only the FUNCTION line was appended, so an address that resolved to nothing
               came back as `["??"]` -- TRUTHY -- and was booked "not VInt" instead of
               "unknown". `no_chain_cycles` was then 0 BY CONSTRUCTION.
      round 2: the fix tested function AND location, but only to DROP a frame when both were
               unknown. The mirror case survived: function `??` with a REAL location was still
               appended as `"??"`, again truthy, again bypassing the guard -- while the
               location it discarded may have been the one thing identifying VInt code.

    The class-level fix is to stop discarding either half. Callers classify on the pair, so a
    frame is useful if EITHER component is, and a chain is unresolved only when nothing in it
    is usable.
    """
    if not addrs:
        return {}
    stdin = "\n".join(f"0x{a:x}" for a in addrs) + "\n"
    out = subprocess.run(
        ["addr2line", "-e", binary, "-i", "-f", "-C", "-a"],
        input=stdin, capture_output=True, text=True, check=True,
    ).stdout
    chains: dict[int, list[tuple[str, str]]] = {}
    cur: int | None = None
    pending_func: str | None = None
    for line in out.splitlines():
        if line.startswith("0x"):
            cur = int(line.strip(), 16)
            chains[cur] = []
            pending_func = None
            continue
        if cur is None:
            continue
        if pending_func is None:
            pending_func = line.strip()
            continue
        func, loc = pending_func, line.strip()
        if _func_usable(func) or _loc_usable(loc):
            chains[cur].append((func, loc))
        pending_func = None
    return chains


def _func_usable(func: str) -> bool:
    """Does this frame name a function? `??` is addr2line's placeholder, not a name."""
    return func not in ("", "??")


def _loc_usable(loc: str) -> bool:
    """Does this frame name a source location? `??:0` / `??:?` are placeholders."""
    return loc not in ("", "??", "??:0", "??:?") and not loc.startswith("??:")


# Source file of the canonical read-side decoder, and the LINE RANGES of the decoder functions
# within it. Used ONLY to rescue a frame whose function name addr2line could not render but
# whose LOCATION it could. Never used in place of a function name that exists.
#
# The ranges matter: an earlier version rescued ANY unresolved frame anywhere in vint.rs, which
# swept in `parse_vint`, `parse_vint_length_signed`, `encode_cassandra_vint` and everything else
# in the file and called it narrow DECODE work. The rescue must be no broader than the claim it
# supports. Ranges are from `cqlite-core/src/parser/vint.rs` at the measured commit:
#   decode_unsigned  40-73     decode_signed  79-82     zigzag_decode  255-257
# These are LITERAL and will go stale if vint.rs is reflowed. That is acceptable and bounded:
# the rescue only ever fires for a frame whose function name addr2line could not render, which
# is 0 of 3430 sampled in-binary addresses on the published data, so a stale range cannot move a
# published figure -- it can only fail to rescue a frame that does not currently occur.
VINT_SOURCE = "cqlite-core/src/parser/vint.rs"
DECODER_LINE_RANGES = ((40, 73), (79, 82), (255, 257))


def _loc_in_decoder(loc: str) -> bool:
    """Is this source location inside one of the decoder functions in vint.rs?

    Requires BOTH the file and a decoder line range. A location in vint.rs outside those ranges
    is some other function in the same file and is NOT decode work.
    """
    if VINT_SOURCE not in loc:
        return False
    tail = loc.rsplit(":", 1)
    if len(tail) != 2 or not tail[1].isdigit():
        return False          # unparseable line number: do not rescue on a guess
    line = int(tail[1])
    return any(lo <= line <= hi for lo, hi in DECODER_LINE_RANGES)


def classify(chain: list[tuple[str, str]]) -> str:
    """Bucket one inline chain. Returns "unresolved" when nothing in it is usable.

    "unresolved" is a FIRST-CLASS answer, never folded into "other": an address DWARF could not
    describe must not become a positive statement that it is not VInt, because that can only push
    the share DOWN. Callers route it to `no_chain_cycles`, which is thresholded and refused above
    a bound in BOTH scripts that consume these chains, not just one of them.
    """
    funcs = [f for f, _ in chain if _func_usable(f)]
    locs = [l for _, l in chain if _loc_usable(l)]
    if not funcs and not locs:
        return "unresolved"
    if any(f.startswith(d) for f in funcs for d in DECODER_FRAMES):
        return "narrow"
    # THE RESCUE IS PER FRAME, and both halves of that matter (roborev r4).
    #
    # Too narrow before: the test was "no usable function ANYWHERE in the chain", so a perfectly
    # ordinary resolved OUTER caller (`parse_cell_value_schema_order`, say) made `funcs`
    # non-empty and suppressed the rescue for an inner frame that genuinely was the decoder --
    # the common case, not a corner one.
    #
    # Too broad before: it accepted any unresolved function anywhere in vint.rs, so `parse_vint`,
    # the length helpers and the encode path all counted as narrow DECODE work.
    #
    # Both are fixed by asking the question of each FRAME on its own: is THIS frame's function
    # unusable while THIS frame's location is inside a decoder line range?
    if any(not _func_usable(f) and _loc_in_decoder(l) for f, l in chain):
        return "narrow"
    if any(READ_VINT_MODULE in f for f in funcs):
        return "wide_only"
    # An unresolved frame located in vint.rs but OUTSIDE the decoder ranges is still read-side
    # VInt module code (`parse_vint`, the length helpers) -- so it belongs to the WIDE boundary,
    # which is defined as the whole module surface. Booking it "other" would undercount the wide
    # figure; booking it "narrow" would overcount the decoder. Same for a location in vint.rs
    # whose line number cannot be parsed: the file is known, the function is not, so the widest
    # claim the evidence supports is module membership.
    if any(not _func_usable(f) and VINT_SOURCE in l for f, l in chain):
        return "wide_only"
    if any(WRITE_VINT_MODULE in f for f in funcs):
        return "write_vint"
    return "other"


def shares(by_addr: collections.Counter, chains: dict[int, list[tuple[str, str]]],
           total: int, in_binary: int) -> dict:
    """Bucket cycles, reporting shares against BOTH denominators.

    An address for which DWARF yielded NO chain is counted in its own bucket, never folded
    into "other". Folding it in makes an address `addr2line` answered `??` for into a
    positive statement that it is not VInt -- a pass derived from the absence of a signal,
    and it can only push the share DOWN.
    """
    buckets: collections.Counter = collections.Counter()
    no_chain = 0
    for fa, cyc in by_addr.items():
        ch = chains.get(fa)
        # Absent chain and unusable chain are the SAME state and take the same branch, so a
        # future change to one cannot silently diverge from the other.
        if not ch:
            no_chain += cyc
            continue
        kind = classify(ch)
        if kind == "unresolved":
            no_chain += cyc
            continue
        buckets[kind] += cyc
    narrow = buckets["narrow"]
    wide = narrow + buckets["wide_only"]
    pct = lambda v, d: (100.0 * v / d) if d else 0.0
    return {
        "narrow_cycles": narrow,
        "wide_cycles": wide,
        "write_vint_cycles": buckets["write_vint"],
        "no_chain_cycles": no_chain,
        "narrow_pct": pct(narrow, total),
        "wide_pct": pct(wide, total),
        "write_vint_pct": pct(buckets["write_vint"], total),
        "no_chain_pct_of_total": pct(no_chain, total),
        "no_chain_pct_of_in_binary": pct(no_chain, in_binary),
        # In-binary basis: the same numerator over only the cycles the numerator could
        # REACH. Reported because a share against `total` and a share against this are
        # different quantities, and two shares on different bases must never be divided.
        "narrow_pct_in_binary": pct(narrow, in_binary),
        "wide_pct_in_binary": pct(wide, in_binary),
    }


def skid_shift(by_addr: collections.Counter, insns: list[int], k: int) -> collections.Counter:
    """Re-attribute every sample K instructions EARLIER in program order.

    The walk is over the binary's flat instruction sequence and so can step across a
    function boundary for a sample landing within K instructions of a function's start.
    That is accepted deliberately: this produces a SENSITIVITY BAND, not a corrected
    attribution, and a boundary-crossing step can only move cycles between buckets in a
    way the band is meant to expose. Making it boundary-aware would narrow the band, i.e.
    make the reported uncertainty smaller than the instrument's -- the wrong direction.
    """
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
    ap.add_argument("--max-no-chain", type=float, default=20.0,
                    help="percent of IN-BINARY cycles at addresses for which DWARF yields no "
                         "inline chain, above which this REFUSES to report a share. Those "
                         "cycles can only push the share DOWN, so an unbounded quantity here "
                         "is an unbounded UNDERCOUNT (default 20.0)")
    args = ap.parse_args()

    by_addr, persym, total, other, unparsed = read_samples(args.perf_data, args.binary)
    in_binary = total - other
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
        "cycles_in_binary": in_binary,
        "pct_outside_binary": 100.0 * other / total,
        "lines_unparsed": unparsed,
        "denominator_note": (
            "narrow_pct/wide_pct are against cycles_total (ALL DSOs incl. libc and kernel) "
            "as the issue's 'share of scan on-CPU' wording requires. *_in_binary are against "
            "cycles_in_binary, the only cycles the DWARF numerator can reach. The two are "
            "DIFFERENT quantities: never divide one by the other, and never compare a share "
            "on one basis with a share on the other."
        ),
        "rebase_selfcheck": {"addresses_checked": checked, "mismatches": bad,
                             "mismatch_pct": mismatch_pct, "verdict": "PASS"},
        "skid_band": {},
    }
    for k in range(0, args.skid_max + 1):
        shifted = skid_shift(by_addr, insns, k)
        chains = inline_chains(args.binary, sorted(shifted))
        result["skid_band"][f"k={k}"] = shares(shifted, chains, total, in_binary)

    nc = result["skid_band"]["k=0"]["no_chain_pct_of_in_binary"]
    if nc > args.max_no_chain:
        print(
            f"vint_share.py: REFUSED — {nc:.2f}% of in-binary cycles are at addresses with no "
            f"DWARF inline chain (> {args.max_no_chain}%). Those cycles can only push the "
            f"share DOWN, so the result would be an unbounded undercount.",
            file=sys.stderr,
        )
        return 1
    base_row = result["skid_band"]["k=0"]
    result.update({k: base_row[k] for k in base_row})
    ks = list(result["skid_band"].values())
    result["narrow_pct_min"] = min(r["narrow_pct"] for r in ks)
    result["narrow_pct_max"] = max(r["narrow_pct"] for r in ks)
    result["wide_pct_min"] = min(r["wide_pct"] for r in ks)
    result["wide_pct_max"] = max(r["wide_pct"] for r in ks)
    result["narrow_pct_in_binary_min"] = min(r["narrow_pct_in_binary"] for r in ks)
    result["narrow_pct_in_binary_max"] = max(r["narrow_pct_in_binary"] for r in ks)

    print(json.dumps(result, indent=2))
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(result, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
