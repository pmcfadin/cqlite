#!/usr/bin/env python3
"""Is the SHARED bucket really the same CODE? Partition it by verified machine-code identity.

WHY THIS EXISTS. The report claimed ~24% of the Flight/scan gap was "identical code costing 21.5%
more". The two arms are DIFFERENT BINARIES -- arm A is `ws0-scan-bench`, arm B is `cqlite-flight` --
and the shared bucket is assigned by SYMBOL PRESENCE (see aggregate-profiles.py). A shared symbol is
therefore the same SOURCE FUNCTION; nothing in that establishes the same MACHINE CODE, because each
binary inlines and specialises independently. So "identical code" was a claim about codegen supported
only by evidence about names, and different codegen was a live competing explanation for the excess
that the wording ruled out rhetorically.

WHAT IT MEASURES. For every shared-pattern symbol present in both binaries, compare the disassembled
MNEMONIC SEQUENCE over the symbol's extent.

WHY MNEMONICS AND NOT BYTES. Bytes are the wrong oracle here and using them would have overstated the
answer badly in the other direction: only 15 of 363 shared symbols are byte-identical, because
call targets and PC-relative operands are RELOCATED differently in two different binaries even when
the instruction sequence is the same. Measured: of 295 shared symbols with identical size, 291 have
an IDENTICAL mnemonic sequence -- same instructions, different operands. Reporting "4% identical"
from a byte comparison would have been the same error as the claim it replaced, pointing the other
way. Size alone is likewise insufficient (4 same-size symbols do differ in mnemonics), so both
checks are applied.

Requires the perfsym binaries and the committed flat profiles. Re-derives every figure in the
"identical code" paragraph of the report.
"""
import glob
import json
import pathlib
import re
import subprocess
import sys

SHARED = [r'row_build::build_row_from_scan', r'types::Value>::into_owned', r'ScanRow',
          r'PartitionKeyCache', r'storage::sstable::reader', r'value_borrow',
          r'QueryRow', r'hashbrown::map::HashMap<alloc::sync::Arc<str>',
          r'hash::sip::Hasher', r'BuildHasher>::hash_one', r'from_utf8',
          r'mpsc::bounded::Sender', r'to_lowercase']
MARGINAL = [r'export::arrow_', r'arrow_size_', r'estimate_arrow_row_bytes', r'batch_bytes',
            r'egress_', r'producer_stream', r'cqlite_flight::', r'row_source', r'arrow_flight::',
            r'arrow_ipc', r'arrow_array', r'arrow_buffer', r'arrow_data', r'arrow_schema',
            r'write_engine::merge', r'RecordBatch', r'tonic::', r'h2::', r'hyper::', r'prost']


def classify(sym: str) -> str:
    for p in MARGINAL:
        if re.search(p, sym):
            return 'm'
    for p in SHARED:
        if re.search(p, sym):
            return 's'
    return 'o'


def text_symbols(binpath: str) -> dict:
    """name -> (addr, size) for sized text symbols. `nm -S`, demangled."""
    out = subprocess.run(['nm', '-S', '--defined-only', '--demangle', binpath],
                         capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"FATAL: nm failed on {binpath}: {out.stderr.strip()}")
    d = {}
    for line in out.stdout.splitlines():
        p = line.split(None, 3)
        if len(p) < 4 or p[2] not in ('t', 'T'):
            continue
        try:
            addr, size = int(p[0], 16), int(p[1], 16)
        except ValueError:
            continue
        d.setdefault(p[3].strip(), (addr, size))
    # A binary with no symbols is the #3217 failure mode: `[profile.release] strip = true` makes
    # per-function attribution impossible, and the guard must FAIL rather than report 0 shared.
    if not d:
        sys.exit(f"FATAL: {binpath} yielded ZERO sized text symbols -- stripped? "
                 "Per-function attribution is impossible on a stripped binary.")
    return d


def mnemonics_by_addr(binpath: str) -> dict:
    out = subprocess.run(['objdump', '-d', '--no-show-raw-insn', binpath],
                         capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"FATAL: objdump failed on {binpath}: {out.stderr.strip()}")
    per = {}
    for line in out.stdout.splitlines():
        m = re.match(r'\s+([0-9a-f]+):\s+(\S+)', line)
        if m:
            per[int(m.group(1), 16)] = m.group(2)
    return per


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__)
        print("usage: codegen-identity.py <scan-binary> <flight-binary> <flat-profile-dir>")
        return 2
    scan_bin, flight_bin, flatdir = sys.argv[1], sys.argv[2], sys.argv[3]
    A, B = text_symbols(scan_bin), text_symbols(flight_bin)
    pa, pb = mnemonics_by_addr(scan_bin), mnemonics_by_addr(flight_bin)

    def seq(per, addr, size):
        return [per[a] for a in range(addr, addr + size) if a in per]

    both = [k for k in A if k in B and classify(k) == 's']
    if not both:
        sys.exit("FATAL: no shared-pattern symbol is present in BOTH binaries. Either the "
                 "classification patterns or the binaries are wrong; a 0-symbol split is not a "
                 "result.")
    ident, diff = set(), set()
    for k in both:
        if A[k][1] == B[k][1] > 0 and seq(pa, *A[k]) == seq(pb, *B[k]):
            ident.add(k)
        else:
            diff.add(k)
    print(f"SHARED symbols present in BOTH binaries: {len(both)}")
    print(f"  VERIFIED-IDENTICAL instruction sequence: {len(ident)} "
          f"({len(ident) / len(both) * 100:.0f}%)")
    print(f"  DIFFERENT machine code:                  {len(diff)} "
          f"({len(diff) / len(both) * 100:.0f}%)")

    share = {}
    for arm, pat in (("scan", "flat-scan-warm-*.txt"), ("flight", "flat-flight-warm-*.txt")):
        files = sorted(glob.glob(str(pathlib.Path(flatdir) / pat)))
        if not files:
            sys.exit(f"FATAL: no flat profiles matched {pat} under {flatdir}. A missing profile "
                     "set would silently report 0% for this arm.")
        acc = {'ident': 0.0, 'diff': 0.0, 'shared': 0.0, 'one_binary_only': 0.0}
        for f in files:
            for line in open(f):
                m = re.match(r'\s*([\d.]+)%\s+\S+\s+(\S+)\s+\[\.\]\s+(.*?)\s*$', line)
                if not m:
                    continue
                pct, sym = float(m.group(1)), m.group(3)
                if classify(sym) != 's':
                    continue
                acc['shared'] += pct
                if sym in ident:
                    acc['ident'] += pct
                elif sym in diff:
                    acc['diff'] += pct
                else:
                    acc['one_binary_only'] += pct
        share[arm] = {k: v / len(files) for k, v in acc.items()}
        s = share[arm]
        print(f"\n{arm}: shared self-time {s['shared']:.2f}% "
              f"(identical {s['ident']:.2f}%, different {s['diff']:.2f}%, "
              f"present in one binary only {s['one_binary_only']:.2f}%)")

    res = pathlib.Path(flatdir) / "results-profiled.json"
    cr = {m['arm']: m['cycles_per_row']['median']
          for m in json.loads(res.read_text())['measurements']}
    scan_cr, fl_cr = cr['bare_scan'], cr['flight_do_get_bypass']
    print(f"\nprofiled cyc/row: scan={scan_cr:.0f} flight={fl_cr:.0f}")
    print(f"\n{'bucket':<40}{'scan':>9}{'flight':>9}{'excess':>10}")
    for label, key in (('SHARED total', 'shared'),
                       ('  VERIFIED-IDENTICAL instructions', 'ident'),
                       ('  DIFFERENT machine code', 'diff')):
        a = share['scan'][key] / 100 * scan_cr
        b = share['flight'][key] / 100 * fl_cr
        print(f"{label:<40}{a:9.0f}{b:9.0f}{(b / a - 1) * 100:9.1f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
