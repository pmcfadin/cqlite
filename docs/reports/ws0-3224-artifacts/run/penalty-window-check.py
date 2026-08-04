#!/usr/bin/env python3
"""#3224 — is the penalty probe's perf window actually gated to the chase?

    python3 penalty-window-check.py <penalty-dir> <accesses>

Exit 0 = every row's counted interval contains the chase and nothing else.
Exit 1 = contamination detected; the cycles/access in that directory are NOT
         access latencies and no penalty may be derived from them.

THE DEFECT THIS EXISTS TO CATCH (roborev finding #2, PR #3286)
--------------------------------------------------------------
`penalty-probe.sh` used to invoke perf as a plain wrapper:

    perf stat -x, -e cycles:u,... -- cache-hostile chase --working-kib N ...

with neither `-D` nor a control FIFO, while `cache-hostile` defaults to
`delay_s = 10.0` and calls `wait_for_window()`. perf therefore counted from
process start, so the identity-fill + Sattolo permutation build — which walks the
WORKING SET, not a fixed amount of work — landed INSIDE the measured interval.
cycles/access was then working-set-dependent, which is precisely the quantity the
probe exists to measure independently of working set, and every penalty derived
from it (DRAM latency minus LLC-hit latency) inherited the error.

It was measured, not theorised. The committed contaminated CSVs give, after
subtracting the L1d row's chase-only instruction count (120,174,195 = 6.01
instr/access, where nodes=512 makes init negligible):

    row          nodes       extra instructions   instr/node   instr/access
    L1d_32K          512                      0           --          6.009
    LLC_8M       131,072              3,789,715        28.91          6.198
    LLC_32M      524,288             15,199,301        28.99          6.769
    DRAM_256M  4,194,304            121,688,792        29.01         12.093
    DRAM_1G   16,777,216            486,790,479        29.01         30.348
    DRAM_2G   33,554,432            973,592,180        29.02         54.688

29.0 instructions per node, constant across five orders of magnitude. That is
the init loop, inside the window.

WHY TWO CHECKS, NEITHER SUFFICIENT ALONE
----------------------------------------
This is CLAUDE.md's "a positive verdict requires an affirmative measurement"
applied at the point where it is easy to get wrong:

  (A) ABSOLUTE CEILING on instructions/access. Externally anchored: the chase
      body is `idx = buf[idx*8]; sum += idx;` plus loop control, a fixed ~6
      instructions per access that CANNOT depend on the working set. A ceiling
      catches gross contamination and, crucially, catches a sweep in which EVERY
      row is inflated.

  (B) CROSS-ROW UNIFORMITY, each row within TOL of the sweep minimum. Catches
      working-set-dependent contamination too small to breach (A) — LLC_8M above
      is +3.2%, comfortably under any defensible ceiling, and would pass (A)
      while being contaminated.

(B) alone would be a vacuous pass under uniform inflation, because its reference
is derived from the same possibly-contaminated data. (A) alone is blind to the
small rows. The defect is bounded only by both, so both must pass and each
reports its own verdict line.
"""
import os
import re
import sys

# (A) The chase body is a fixed instruction sequence; 6.01 measured with cc -O2.
# The ceiling carries headroom for a different compiler's loop shape while staying
# far below any contaminated row of consequence.
INSTR_PER_ACCESS_MAX = 8.0
# (B) Init excluded, the count is identical in every row; the slack covers only
# the FIFO handshake's couple of syscalls over 20M accesses.
UNIFORMITY_TOL_PCT = 2.0


def read_instructions(path):
    """The `instructions` count from a perf -x, CSV.

    perf strips the `:u` modifier from some event names and keeps it on others
    (measured on perf 6.17.13: `instructions:u` retains it, `LLC-loads:u` does
    not), so match on the BASE name — the same rule positive-control.sh's
    ev_field applies, for the same reason.
    """
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            f = line.split(',')
            if len(f) < 5:
                continue
            if f[2].split(':')[0] != 'instructions':
                continue
            try:
                return float(f[0])
            except ValueError:
                return None
    return None


def main(argv):
    if len(argv) != 3:
        sys.exit(__doc__.strip().splitlines()[2].strip())
    pdir, accesses = argv[1], float(argv[2])
    if accesses <= 0:
        sys.exit('FATAL: accesses must be positive (got %r)' % argv[2])

    csvs = sorted(
        p for p in (os.path.join(pdir, n) for n in os.listdir(pdir))
        if re.match(r'^perf-.*\.csv$', os.path.basename(p)))
    if not csvs:
        # No subject, no verdict. A "0/0 PASS" here would be the vacuous pass
        # this whole file exists to make impossible.
        sys.exit('FATAL: no perf-*.csv rows found under %s — a window check with '
                 'no subject cannot pass.' % pdir)

    rows = []
    for p in csvs:
        ins = read_instructions(p)
        label = os.path.basename(p)[len('perf-'):-len('.csv')]
        if ins is None:
            sys.exit("FATAL: %s carries no readable `instructions` count. The "
                     "window cannot be verified, so the row cannot be published."
                     % p)
        rows.append((label, ins, ins / accesses))

    print('-- penalty-probe window-gate integrity (%d rows, %d accesses/row) --'
          % (len(rows), accesses))
    for label, ins, ipa in rows:
        print('   %-12s instructions %14.0f   instr/access %8.3f' % (label, ins, ipa))

    failures = []

    over = [(l, i) for l, _, i in rows if i > INSTR_PER_ACCESS_MAX]
    if over:
        failures.append(
            'absolute-ceiling: %d row(s) above %.1f instr/access — %s. The perf '
            'window is counting work that is not the chase (buffer init and/or '
            'address-space teardown). Remedy: gate the window with '
            '`perf stat -D -1 --control fifo:<ctl>,<ack>` and pass --ctl-fifo/'
            '--ack-fifo to cache-hostile.'
            % (len(over), INSTR_PER_ACCESS_MAX,
               ', '.join('%s=%.3f' % x for x in over)))
        print('   absolute-ceiling: FAIL (%s)'
              % ', '.join('%s=%.3f' % x for x in over))
    else:
        print('   absolute-ceiling: PASS (all %d rows <= %.1f instr/access)'
              % (len(rows), INSTR_PER_ACCESS_MAX))

    base = min(i for _, _, i in rows)
    drift = [(l, i, (i - base) / base * 100.0) for l, _, i in rows]
    bad = [d for d in drift if d[2] > UNIFORMITY_TOL_PCT]
    if bad:
        failures.append(
            'cross-row-uniformity: %d row(s) more than %.1f%% above the sweep '
            'minimum %.3f instr/access — %s. The instruction count per access is '
            'a property of the chase loop and CANNOT depend on the working set, '
            'so a working-set-dependent count means init is inside the window. '
            'Same remedy as above.'
            % (len(bad), UNIFORMITY_TOL_PCT, base,
               ', '.join('%s=%.3f(+%.1f%%)' % d for d in bad)))
        print('   cross-row-uniformity: FAIL (%s)'
              % ', '.join('%s=%.3f(+%.1f%%)' % d for d in bad))
    else:
        print('   cross-row-uniformity: PASS (all %d rows within %.1f%% of %.3f)'
              % (len(rows), UNIFORMITY_TOL_PCT, base))

    if failures:
        sys.exit('WINDOW CHECK FAILED (fail-closed):\n  - '
                 + '\n  - '.join(failures))
    print('   VERDICT: PASS — the counted interval is the chase, so cycles/access '
          'is an access latency and a penalty may be derived from it.')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
