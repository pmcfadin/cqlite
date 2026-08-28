#!/usr/bin/env python3
"""Is the SHARED bucket really the same CODE? Partition it by machine-code identity.

WHY THIS EXISTS. The report claimed ~24% of the Flight/scan gap was "identical code costing 21.5%
more". The two arms are DIFFERENT BINARIES -- arm A is `ws0-scan-bench`, arm B is `cqlite-flight` --
and the shared bucket is assigned by SYMBOL PRESENCE (see aggregate-profiles.py). A shared symbol is
therefore the same SOURCE FUNCTION; nothing in that establishes the same MACHINE CODE, because each
binary inlines and specialises independently.

THREE ORACLES WERE TRIED. The first two were WRONG IN OPPOSITE DIRECTIONS, and both are recorded
because each looked obviously right while it was in use.

  (1) BYTE EQUALITY -- far too STRICT. Only 15 of 363 shared symbols are byte-identical, because
      call targets and PC-relative displacements are RELOCATED differently in two different
      binaries even when the instruction stream is the same. Reporting "4% identical" from this
      would have been a large error.

  (2) MNEMONIC SEQUENCE -- too LOOSE, and this one shipped for one round before review caught it
      (roborev job 71 finding 1). Discarding operands keeps only the opcode names, so two functions
      with the same shape but DIFFERENT REGISTERS, DIFFERENT IMMEDIATES or a DIFFERENT CALLEE
      compare equal. It reported 291 of 363 identical; 155 of those 291 are NOT. Measured
      divergences among them: 49 symbols differ in a register or a real immediate -- something a
      mnemonic compare can never see -- and most of the rest reference a different target symbol.

  (3) NORMALIZED OPERANDS -- what this script does. Compare mnemonic AND operands, normalizing ONLY
      the parts that must relocate:
        * an intra-function branch target becomes `L+<offset from the function start>`, which is
          invariant; an external target becomes the callee's NAME, which is the invariant thing
          about a call;
        * a `0x...(%rip)` displacement becomes `RIP`, and the target symbol named in objdump's
          trailing comment is kept -- the displacement relocates, the referent does not.
      Registers and `$`-prefixed immediates are KEPT and compared, because those are exactly what
      oracle (2) was blind to.

  Result: 136 of 363 (37%) are operand-identical.

THE RESIDUAL, because oracle (3) is not provably final either. 136 is a LOWER BOUND on "identical"
and 227 an UPPER BOUND on "different": the largest divergence class is a differing TARGET SYMBOL, and
some of those callees may differ only by monomorphisation or crate-disambiguator hash, i.e. be the
same code under a different name. Tightening that further needs recursive comparison of callees,
which is not attempted here. The bound is stated rather than resolved, and every claim in the report
is phrased against the LOWER bound so that a tighter oracle could only move it favourably.

ALSO CHECKED, AND MEASURED TO BE A NO-OP: `%fs:0x...` TLS offsets survive normalization, and TLS
layout differs per binary, so in principle identical source could compare DIFFERENT. Normalizing them
changes the identical count NOT AT ALL, and ZERO symbols diverge only by a TLS offset. Recorded so a
later reader does not read it as overlooked -- a checked-and-zero hazard is not an unconsidered one.
Separately verified that no bare branch/call targets exist (every one carries a `<sym>` annotation),
so the branch normalization has no gap.

THE CONCLUSION THIS SCRIPT NO LONGER SUPPORTS. Four oracles gave 4%, 80%, 37% and 33% "identical",
with the excess ratio climbing (+54.5%, +77.1%, +90.7%) as the base shrank to 2.56% of self-time. That
regress is the signature of a FITTED quantity, so the report WITHDREW the decomposition as a headline
claim and keeps only the bucket-total +21.2%, which assumes nothing about machine-code identity. What
this script is still for: showing that symbol presence does not imply shared machine code, and
quantifying the 23% of the bucket that is UNRESOLVABLE by name.

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
    """name -> LIST of (addr, size) for sized text symbols. `nm -S`, demangled.

    A LIST, NOT A SINGLE ENTRY (roborev job 72). This used `setdefault`, which kept an ARBITRARY
    FIRST definition when several text symbols share one demangled name -- and then one comparison
    of that arbitrary pick was applied to every profile sample bearing the name. Measured: 57
    shared-pattern names are duplicated in ws0-scan-bench and 30 in cqlite-flight, and 16 / 7 of
    those have definitions of DIFFERENT SIZES, so the pick decided the answer.

    Worse, and not fixable by picking better: the flat profiles are keyed by demangled name too, so
    a sample on a duplicated name cannot be attributed to an instantiation AT ALL. Such names are
    therefore reported as UNRESOLVABLE rather than assigned to either bucket.
    """
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
        d.setdefault(p[3].strip(), []).append((addr, size))
    # A binary with no symbols is the #3217 failure mode: `[profile.release] strip = true` makes
    # per-function attribution impossible, and the guard must FAIL rather than report 0 shared.
    if not d:
        sys.exit(f"FATAL: {binpath} yielded ZERO sized text symbols -- stripped? "
                 "Per-function attribution is impossible on a stripped binary.")
    return d


_BRANCH = re.compile(r'^(\S+)\s+([0-9a-f]+)\s+<([^>]*)>\s*$')
# THE SIGN IS PART OF THE DISPLACEMENT (roborev job 73 finding 3). This matched only
# positive displacements, so objdump's `-0x123(%rip)` normalized to `-RIP` and an identical
# reference whose displacement changed SIGN between binaries compared DIFFERENT -- a false
# difference, i.e. an UNDERSTATED identical set. The `-RIP` form was visible in my own probe
# output and I read past it.
_RIP = re.compile(r'-?0x[0-9a-f]+\(%rip\)')
_COMMENT = re.compile(r'\s*#\s*[0-9a-f]+\s*<([^>]*)>\s*$')


def instructions_by_addr(binpath: str) -> dict:
    """addr -> raw instruction text (mnemonic AND operands, whitespace-normalized)."""
    out = subprocess.run(['objdump', '-d', '--no-show-raw-insn', binpath],
                         capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"FATAL: objdump failed on {binpath}: {out.stderr.strip()}")
    per = {}
    for line in out.stdout.splitlines():
        m = re.match(r'\s+([0-9a-f]+):\t(.*)$', line)
        if m:
            per[int(m.group(1), 16)] = m.group(2).strip()
    if not per:
        sys.exit(f"FATAL: objdump produced no instructions for {binpath}. An empty disassembly "
                 "would make every symbol compare equal, i.e. a vacuous 100% identical.")
    return per


def normalize(body: str, start: int, size: int) -> str:
    """Normalize ONLY relocatable operands. Registers and $-immediates are KEPT and compared.

    This is the whole substance of oracle (3): the previous version compared mnemonics alone and
    was blind to a differing register, immediate or callee, which is what review caught.
    """
    m = _BRANCH.match(body)
    if m:
        mnemonic, target, sym = m.group(1), int(m.group(2), 16), m.group(3)
        if start <= target < start + size:
            # Intra-function branch: the offset from the function's OWN start is invariant, while
            # the absolute address is not.
            return f"{mnemonic} L+{target - start}"
        # External: the callee's NAME is the invariant thing about the call.
        return f"{mnemonic} <{sym}>"
    target_sym = None
    c = _COMMENT.search(body)
    if c:
        target_sym = c.group(1)
        body = body[:c.start()]
    # The displacement relocates; the referent named in objdump's comment does not.
    body = _RIP.sub('RIP', body).strip()
    return f"{body} ->{target_sym}" if target_sym else body


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__)
        print("usage: codegen-identity.py <scan-binary> <flight-binary> <flat-profile-dir>")
        return 2
    scan_bin, flight_bin, flatdir = sys.argv[1], sys.argv[2], sys.argv[3]
    A, B = text_symbols(scan_bin), text_symbols(flight_bin)
    pa, pb = instructions_by_addr(scan_bin), instructions_by_addr(flight_bin)

    def seq(per, addr, size):
        return [normalize(per[a], addr, size) for a in range(addr, addr + size) if a in per]

    both = [k for k in A if k in B and classify(k) == 's']
    if not both:
        sys.exit("FATAL: no shared-pattern symbol is present in BOTH binaries. Either the "
                 "classification patterns or the binaries are wrong; a 0-symbol split is not a "
                 "result.")
    ident, diff, unresolvable = set(), set(), set()
    for k in both:
        # A name with more than one definition in EITHER binary cannot be attributed from a
        # name-keyed profile, so it is neither "identical" nor "different" -- it is UNKNOWN, and
        # folding an unknown into either bucket is the vacuous-pass shape this rig exists to refuse.
        if len(A[k]) > 1 or len(B[k]) > 1:
            unresolvable.add(k)
            continue
        (a_addr, a_size), (b_addr, b_size) = A[k][0], B[k][0]
        if a_size == b_size > 0 and seq(pa, a_addr, a_size) == seq(pb, b_addr, b_size):
            ident.add(k)
        else:
            diff.add(k)
    print(f"SHARED symbol NAMES present in BOTH binaries: {len(both)}")
    print(f"  UNRESOLVABLE (name has >1 definition):    {len(unresolvable)} "
          f"({len(unresolvable) / len(both) * 100:.0f}%)")
    print(f"  operand-identical:                        {len(ident)} "
          f"({len(ident) / len(both) * 100:.0f}%)")
    print(f"  different machine code:                   {len(diff)} "
          f"({len(diff) / len(both) * 100:.0f}%)")

    share = {}
    for arm, pat in (("scan", "flat-scan-warm-*.txt"), ("flight", "flat-flight-warm-*.txt")):
        files = sorted(glob.glob(str(pathlib.Path(flatdir) / pat)))
        if not files:
            sys.exit(f"FATAL: no flat profiles matched {pat} under {flatdir}. A missing profile "
                     "set would silently report 0% for this arm.")
        acc = {'ident': 0.0, 'diff': 0.0, 'shared': 0.0, 'one_binary_only': 0.0,
               'unresolvable': 0.0}
        # THE DENOMINATOR IS FILES THAT ACTUALLY PARSED, NOT FILES THAT EXIST (roborev job 71
        # finding 2). `len(files)` counted an empty, truncated or format-incompatible profile in
        # the average, which silently DIVIDES every attributed percentage down and yields plausible
        # but low cycle estimates -- a wrong number with no error. A profile that parses to zero
        # rows is a broken input, so it FAILS rather than being averaged in or quietly skipped:
        # skipping would still let a 3-rep claim rest on 2 reps.
        parsed_files = 0
        for f in files:
            rows_in_file = 0
            for line in open(f):
                m = re.match(r'\s*([\d.]+)%\s+\S+\s+(\S+)\s+\[\.\]\s+(.*?)\s*$', line)
                if not m:
                    continue
                pct, sym = float(m.group(1)), m.group(3)
                rows_in_file += 1
                if classify(sym) != 's':
                    continue
                acc['shared'] += pct
                if sym in unresolvable:
                    acc['unresolvable'] += pct
                elif sym in ident:
                    acc['ident'] += pct
                elif sym in diff:
                    acc['diff'] += pct
                else:
                    acc['one_binary_only'] += pct
            if rows_in_file == 0:
                sys.exit(f"FATAL: {f} parsed to ZERO profile rows. An unparseable profile counted "
                         "in the averaging denominator would scale every attributed percentage "
                         "down silently; it is refused instead.")
            parsed_files += 1
        if parsed_files != len(files):
            sys.exit(f"FATAL: {len(files) - parsed_files} of {len(files)} {arm} profiles did not "
                     "parse; the average would rest on fewer reps than it claims.")
        share[arm] = {k: v / parsed_files for k, v in acc.items()}
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
                       ('  operand-identical', 'ident'),
                       ('  different machine code', 'diff'),
                       ('  UNRESOLVABLE (duplicated name)', 'unresolvable')):
        a = share['scan'][key] / 100 * scan_cr
        b = share['flight'][key] / 100 * fl_cr
        # A bucket with no scan-side self-time has no ratio; printing one would be a
        # ZeroDivisionError at best and an invented number at worst.
        excess = f"{(b / a - 1) * 100:8.1f}%" if a > 0 else "     n/a"
        print(f"{label:<40}{a:9.0f}{b:9.0f}{excess:>9}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
