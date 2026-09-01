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


# SWEEP (b)/(dedup), roborev r6: `disasm` and `nm_table` used to live here as SECOND
# implementations of parsers `vint_share` already had -- the same duplication that produced two
# different notions of an "unresolved" frame and, before that, a whole second addr2line parser.
# The mnemonic/text maps are the only thing this script needs beyond `vint_share`'s, so it takes
# the ADDRESS parsing from there and adds only the per-address decode text.


def disasm(binary: str) -> tuple[list[int], dict[int, str], dict[int, str], object]:
    """(addresses, addr->mnemonic, addr->full text, parse accounting)."""
    vsh = _vint_share()
    acct = vsh.ParseAccounting("objdump decode lines")
    mnem: dict[int, str] = {}
    text: dict[int, str] = {}
    for line in vsh.run(["objdump", "-d", "--no-show-raw-insn", binary]).splitlines():
        m = re.match(r"^\s+([0-9a-f]+):\t(.*)$", line)
        if not m:
            if re.match(r"^\s+[0-9a-f]+:", line):
                acct.skip(line)
            continue
        a = int(m.group(1), 16)
        body = m.group(2).strip()
        mnem[a] = body.split()[0] if body else ""
        text[a] = body
        acct.keep()
    addrs = sorted(mnem)
    return addrs, mnem, text, acct


def nm_table(binary: str) -> tuple[list[int], list[str], object]:
    """Text symbols via `vint_share.nm_symbols` -- one implementation, not two."""
    sym_addrs, syms, acct = _vint_share().nm_symbols(binary)
    return sym_addrs, [n for _, n in syms], acct


def sym_of(addr: int, sym_addrs: list[int], sym_names: list[str]) -> str:
    i = bisect.bisect_right(sym_addrs, addr) - 1
    return sym_names[i] if i >= 0 else "?"


# NOTE: this module deliberately has NO chain builder of its own. It had one -- a second
# addr2line parser returning bare function strings -- and that duplication is exactly how the
# two scripts came to hold different notions of an "unresolved" frame, which roborev found
# twice. `vint_share.inline_chains` is the single implementation; this module imports it.


def caller_of(chain) -> str:
    """Innermost `cqlite_core` frame outside the vint module, or "?" if there is none.

    A FUNCTION so it can be tested, which is how this earns its keep: two defects have lived in
    this one expression.

    1. It excluded `core::` and `<u8>::` but not `<u64>::` or `<[u8]>::`, so `<u64>::swap_bytes`
       and `<[u8]>::copy_from_slice` -- core helpers inlined INTO the decoder, i.e. CALLEES --
       were reported as callers holding 72% of vint cycles.
    2. After chains became (function, location) PAIRS it still searched the TUPLE, and
       `"str" in ("a", "b")` is a MEMBERSHIP test, not a substring test, so it never matched:
       EVERY caller was reported as "?" and the committed artifacts shipped that way. A
       representation change needs a sweep of every consumer, and this is the consumer it missed.
    """
    return next(
        (func for func, _loc in chain
         if "cqlite_core::" in func and "cqlite_core::parser::vint::" not in func),
        "?",
    )


def is_vint(chain) -> bool:
    """Delegates to `vint_share.classify` so the two scripts cannot drift apart.

    This re-implemented the test over function strings, which is exactly how the two ended up
    with different notions of an "unresolved" frame. One definition, one place.
    """
    return _vint_share().classify(chain) == "narrow"


def _vint_share():
    """Import the sibling module, resolving its directory the same way `main` does."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import vint_share
    return vint_share


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
    ap.add_argument("--max-unparseable", type=lambda v: _vint_share()._pct(v), default=1.0,
                    help="percent of inputs any parsing loop may fail to parse before REFUSING "
                         "(SWEEP (b)); same bound and same helper as vint_share.py")
    ap.add_argument("--max-no-chain", type=lambda v: _vint_share()._pct(v), default=20.0,
                    help="percent of IN-BINARY cycles at addresses DWARF cannot describe, above "
                         "which this REFUSES to publish the cycle-weighted tables. Same bound "
                         "vint_share.py enforces: a guard added to one consumer of a dataset and "
                         "not the other is a guard with a hole in it")
    args = ap.parse_args()

    addrs, mnem, text, dis_acct = disasm(args.binary)
    sym_addrs, sym_names, nm_acct = nm_table(args.binary)
    for acct in (dis_acct, nm_acct):
        msg = acct.require(args.max_unparseable)
        if msg:
            print(f"vint_regions.py: REFUSED — {msg}", file=sys.stderr)
            return 1

    anchors = fingerprint_anchors(addrs, mnem, text)
    bswaps = [a for a in addrs if mnem.get(a) == "bswap"]

    # DWARF side: classify every anchor and every bswap, plus a window around each anchor.
    probe = sorted(set(anchors) | set(bswaps))
    chains = _vint_share().inline_chains(args.binary, probe)
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

        # roborev r6 finding 3: this DISCARDED the sample accounting (`_unparsed`), so this
        # tool could publish opcode/caller tables with unknown sample weight omitted. It is now
        # adjudicated on the same bound as vint_share.py, and the unattributable in-binary
        # weight is refused rather than silently dropped.
        by_addr, persym, total, other, sample_acct, unattributable = vsh.read_samples(
            args.perf_data, args.binary)
        in_binary = total - other
        sa_msg = sample_acct.require(args.max_unparseable)
        if sa_msg:
            print(f"vint_regions.py: REFUSED — {sa_msg}", file=sys.stderr)
            return 1
        pct_unatt = 100.0 * unattributable / total if total else 0.0
        if pct_unatt > args.max_unparseable:
            print(
                f"vint_regions.py: REFUSED — {pct_unatt:.4f}% of sampled cycles are in-binary "
                f"rows whose IP would not parse (> {args.max_unparseable}%).",
                file=sys.stderr,
            )
            return 1
        # The PIE rebase self-check is NOT optional here either. This script publishes a
        # cycle-weighted table, and a wrong rebase yields a complete, confident, WRONG one
        # exactly as it would in vint_share.py -- so it must REFUSE on the same terms rather
        # than inherit the neighbouring script's diligence by proximity.
        sym_addrs2, syms2, _nm2 = vsh.nm_symbols(args.binary)
        ok, bad, vb_acct = vsh.verify_base(persym, sym_addrs2, syms2)
        vb_msg = vb_acct.require(args.max_unparseable)
        if vb_msg:
            print(f"vint_regions.py: REFUSED — {vb_msg}", file=sys.stderr)
            return 1
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
            "coverage": vb_acct.as_dict(),
        }
        schains = vsh.inline_chains(args.binary, sorted(by_addr))
        by_op: collections.Counter = collections.Counter()
        by_caller: collections.Counter = collections.Counter()
        vint_cycles = 0
        no_chain_cycles = 0
        for a, cyc in by_addr.items():
            ch = schains.get(a)
            if not ch or vsh.classify(ch) == "unresolved":
                # Absent chain and UNUSABLE chain are the same state and take the same branch,
                # never folded into "not VInt". See vint_share.py.
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
            caller = caller_of(ch)
            by_caller[caller] += cyc
        nc_pct = 100.0 * no_chain_cycles / in_binary if in_binary else 100.0
        if nc_pct > args.max_no_chain:
            print(
                f"vint_regions.py: REFUSED — {nc_pct:.2f}% of in-binary cycles are at addresses "
                f"DWARF cannot describe (> {args.max_no_chain}%). The cycle-weighted tables would "
                f"be undercounted by an unbounded amount.",
                file=sys.stderr,
            )
            return 1

        result["cycle_weighted"] = {
            "cycles_total_all_dsos": total,
            "cycles_in_binary": in_binary,
            "vint_cycles": vint_cycles,
            "no_chain_cycles": no_chain_cycles,
            "no_chain_pct_of_in_binary": 100.0 * no_chain_cycles / in_binary if in_binary else 0.0,
            "sample_rows": sample_acct.as_dict(),
            "unattributable_cycles": unattributable,
            "vint_pct_of_total": 100.0 * vint_cycles / total if total else 0.0,
            "vint_pct_of_in_binary": 100.0 * vint_cycles / in_binary if in_binary else 0.0,
            # This table is built from RAW, UNSHIFTED sample IPs, and this host has no PEBS
            # (../raw/counter-capability-census.md), so an IP can sit a few instructions past
            # the one that consumed the cycle -- at instruction granularity, the same order as
            # the quantity reported. INDICATIVE ONLY: no per-instruction concentration claim
            # may rest on it. The skid band in vint_share.py covers the REGION-level number,
            # which is the granularity this instrument supports.
            "by_opcode_caveat": (
                "UNSHIFTED, non-precise sample IPs (no PEBS on this host). Indicative of where "
                "cycles land within the region; NOT evidence of per-instruction concentration."
            ),
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
