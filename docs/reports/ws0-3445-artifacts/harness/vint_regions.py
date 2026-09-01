#!/usr/bin/env python3
"""vint_regions.py — corroborate the DWARF attribution against the DISASSEMBLY, and say
WHERE inside the decode the cycles sit (issue #3445, AC1 + AC2 input).

`vint_share.py` answers "how much". This answers the two questions that make that number
believable and actionable:

1. **Do two INDEPENDENT identifications of the vint regions agree?** AC1 asks for regions
   identified "via disassembly against the known codegen of `decode_unsigned`". DWARF inline
   records are one identification; the instruction fingerprint from #1638 J4's codegen
   (`leading_ones` -> `not`/`bsr`/`xor $0x7`, `u64::from_be_bytes` -> `bswap`) is a second,
   derived from a completely different source. If a `bsr`+`xor $0x7` pair sits OUTSIDE every
   DWARF-derived vint range, one of the two is wrong and the share cannot be trusted. That
   is checked here and reported as a count, both directions.

2. **Which instructions, and which CALLERS, carry the vint cycles?** A share alone cannot
   distinguish "spread thinly over many cheap decodes" from "piled on one stalling
   instruction", and that distinction is exactly AC2's subject. The per-opcode and
   per-caller tables are the input to it.

Regions are DELIMITED by DWARF (exact, compiler-authored) and CORROBORATED by the
fingerprint (independent). Neither is asked to do the other's job: a fingerprint cannot
delimit a region without a judgement-laden boundary rule, and DWARF cannot tell you that its
own answer matches the machine code.
"""
from __future__ import annotations

import argparse
import bisect
import collections
import json
import re
import os
import subprocess
import sys

DECODER_FRAMES = (
    "cqlite_core::parser::vint::decode_unsigned",
    "cqlite_core::parser::vint::decode_signed",
    "cqlite_core::parser::vint::zigzag_decode",
)


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def disasm(binary: str) -> tuple[list[int], dict[int, str], dict[int, str]]:
    """(ascending addresses, addr -> mnemonic, addr -> full text) over `.text`."""
    addrs: list[int] = []
    mnem: dict[int, str] = {}
    text: dict[int, str] = {}
    for line in run(["objdump", "-d", "--no-show-raw-insn", binary]).splitlines():
        m = re.match(r"^\s+([0-9a-f]+):\t(.*)$", line)
        if not m:
            continue
        a = int(m.group(1), 16)
        body = m.group(2).strip()
        addrs.append(a)
        mnem[a] = body.split()[0] if body else ""
        text[a] = body
    addrs.sort()
    return addrs, mnem, text


def nm_table(binary: str) -> tuple[list[int], list[str]]:
    syms = []
    for line in run(["nm", "-C", "--defined-only", binary]).splitlines():
        parts = line.split(" ", 2)
        if len(parts) == 3 and parts[1].lower() in ("t", "w"):
            try:
                syms.append((int(parts[0], 16), parts[2].strip()))
            except ValueError:
                continue
    syms.sort()
    return [a for a, _ in syms], [n for _, n in syms]


def sym_of(addr: int, sym_addrs: list[int], sym_names: list[str]) -> str:
    i = bisect.bisect_right(sym_addrs, addr) - 1
    return sym_names[i] if i >= 0 else "?"


def chains_for(binary: str, addrs: list[int]) -> dict[int, list[str]]:
    if not addrs:
        return {}
    out = subprocess.run(
        ["addr2line", "-e", binary, "-i", "-f", "-C", "-a"],
        input="\n".join(f"0x{a:x}" for a in addrs) + "\n",
        capture_output=True, text=True, check=True,
    ).stdout
    chains: dict[int, list[str]] = {}
    cur = None
    expect_func = True
    for line in out.splitlines():
        if line.startswith("0x"):
            cur = int(line.strip(), 16)
            chains[cur] = []
            expect_func = True
            continue
        if cur is None:
            continue
        if expect_func:
            chains[cur].append(line.strip())
        expect_func = not expect_func
    return chains


def is_vint(chain: list[str]) -> bool:
    return any(f.startswith(d) for f in chain for d in DECODER_FRAMES)


def fingerprint_anchors(addrs: list[int], mnem: dict[int, str], text: dict[int, str]) -> list[int]:
    """Addresses of the F2 anchor: a `bsr` followed within 2 instructions by `xor $0x7`.

    That pairing is `<u8>::leading_ones` — a bit-index read plus the 7-complement that turns
    it into a leading-ONES count. `bsr` alone is not specific enough (it appears in generic
    integer-log paths) and `xor $0x7` alone is meaningless, so the anchor is the PAIR.
    """
    out = []
    for i, a in enumerate(addrs):
        if mnem.get(a) != "bsr":
            continue
        for j in (i + 1, i + 2):
            if j < len(addrs) and mnem.get(addrs[j]) == "xor" and "$0x7," in text.get(addrs[j], ""):
                out.append(a)
                break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--perf-data", help="perf.data to weight opcodes/callers by cycles")
    ap.add_argument("--json-out")
    args = ap.parse_args()

    addrs, mnem, text = disasm(args.binary)
    sym_addrs, sym_names = nm_table(args.binary)

    anchors = fingerprint_anchors(addrs, mnem, text)
    bswaps = [a for a in addrs if mnem.get(a) == "bswap"]

    # DWARF side: classify every anchor and every bswap, plus a window around each anchor.
    probe = sorted(set(anchors) | set(bswaps))
    chains = chains_for(args.binary, probe)
    anchors_in_vint = [a for a in anchors if is_vint(chains.get(a, []))]
    anchors_outside = [a for a in anchors if not is_vint(chains.get(a, []))]
    bswaps_in_vint = [a for a in bswaps if is_vint(chains.get(a, []))]

    result = {
        "binary": args.binary,
        "fingerprint_vs_dwarf": {
            "f2_anchors_total": len(anchors),
            "f2_anchors_inside_dwarf_vint": len(anchors_in_vint),
            "f2_anchors_outside_dwarf_vint": len(anchors_outside),
            "bswap_total": len(bswaps),
            "bswap_inside_dwarf_vint": len(bswaps_in_vint),
            "anchors_outside_symbols": sorted(
                {sym_of(a, sym_addrs, sym_names) for a in anchors_outside}
            )[:40],
        },
    }

    if args.perf_data:
        # Cycle-weighted composition: for every SAMPLED address that DWARF calls vint, book
        # its cycles to (opcode, hosting symbol, innermost non-vint caller frame).
        # os.path.dirname, not rsplit: invoked bare ("python3 vint_regions.py") __file__
        # has no separator, and rsplit would insert the FILENAME onto sys.path.
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import vint_share as vsh  # the ONE sample reader, reused rather than re-implemented

        by_addr, persym, total, other, _unparsed = vsh.read_samples(args.perf_data, args.binary)
        in_binary = total - other
        # The PIE rebase self-check is NOT optional here either. This script publishes a
        # cycle-weighted table, and a wrong rebase yields a complete, confident, WRONG one
        # exactly as it would in vint_share.py -- so it must REFUSE on the same terms rather
        # than inherit the neighbouring script's diligence by proximity.
        sym_addrs2, syms2 = vsh.nm_symbols(args.binary)
        ok, bad = vsh.verify_base(persym, sym_addrs2, syms2)
        checked = ok + bad
        mismatch_pct = 100.0 * bad / checked if checked else 100.0
        if mismatch_pct > 0.5:
            print(
                f"vint_regions.py: REFUSED — PIE rebase self-check failed: nm and perf "
                f"disagree on {bad}/{checked} addresses ({mismatch_pct:.2f}%). No "
                f"cycle-weighted table is printed.",
                file=sys.stderr,
            )
            return 1
        result["rebase_selfcheck"] = {
            "addresses_checked": checked, "mismatches": bad,
            "mismatch_pct": mismatch_pct, "verdict": "PASS",
        }
        schains = vsh.inline_chains(args.binary, sorted(by_addr))
        by_op: collections.Counter = collections.Counter()
        by_caller: collections.Counter = collections.Counter()
        vint_cycles = 0
        no_chain_cycles = 0
        for a, cyc in by_addr.items():
            ch = schains.get(a)
            if not ch:
                # No DWARF chain: counted, never folded into "not VInt". See vint_share.py.
                no_chain_cycles += cyc
                continue
            if not is_vint(ch):
                continue
            vint_cycles += cyc
            by_op[mnem.get(a, "?")] += cyc
            # The CALLER is the innermost cqlite_core frame outside the vint module.
            # An earlier rule excluded `core::` and `<u8>::` and so mistook `<u64>::swap_bytes`
            # and `<[u8]>::copy_from_slice` -- which are core helpers inlined INTO the decoder,
            # i.e. callees -- for callers, attributing 72% of vint cycles to `<u64>::swap_bytes`.
            # A frame is only a caller if it is cqlite_core code that is not vint itself.
            caller = next(
                (f for f in ch
                 if "cqlite_core::" in f and "cqlite_core::parser::vint::" not in f),
                "?",
            )
            by_caller[caller] += cyc
        result["cycle_weighted"] = {
            "cycles_total_all_dsos": total,
            "cycles_in_binary": in_binary,
            "vint_cycles": vint_cycles,
            "no_chain_cycles": no_chain_cycles,
            "no_chain_pct_of_in_binary": 100.0 * no_chain_cycles / in_binary if in_binary else 0.0,
            "vint_pct_of_total": 100.0 * vint_cycles / total if total else 0.0,
            "vint_pct_of_in_binary": 100.0 * vint_cycles / in_binary if in_binary else 0.0,
            "by_opcode": [
                {"opcode": o, "cycles": c, "pct_of_vint": 100.0 * c / vint_cycles}
                for o, c in by_op.most_common()
            ],
            "by_caller": [
                {"caller": k, "cycles": c, "pct_of_vint": 100.0 * c / vint_cycles,
                 "pct_of_total": 100.0 * c / total}
                for k, c in by_caller.most_common(15)
            ],
        }

    print(json.dumps(result, indent=2))
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(result, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
