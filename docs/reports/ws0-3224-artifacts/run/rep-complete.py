#!/usr/bin/env python3
"""#3224 — is this rep genuinely complete and valid, i.e. safe to SKIP on resume?

    python3 rep-complete.py <repdir>

Exit 0 = complete and valid; run-all.sh may skip it.
Exit 1 = incomplete, invalid, or unverifiable; the rep must be REDONE.

THE DEFECT THIS EXISTS TO CATCH (roborev finding #5, PR #3286)
--------------------------------------------------------------
run-all.sh's resume predicate was three conditions inlined in a `python3 -c`:
occupancy arms `ok`, `warm_verified_zero_disk_reads`, and
`client_saturation_gate_pass`. It read NEITHER the recorded return codes NOR the
counter files. Two consequences, and the second is the expensive one:

  * A capture whose load generator failed (`rc.loadgen_uncore != 0`) still writes
    a structurally perfect meta.json. On resume it was reported "already complete
    and valid" and skipped — permanently, because a skipped rep is never
    revisited — so the failed capture stayed in the results tree.

  * A rep whose `perf-uncore.csv` was absent or empty passed too, because nothing
    looked at it. derive.py then read no IMC rows and derived 0 GB/s (fixed
    separately in derive.py). The two defects COMPOUND: resume preserves the
    broken rep, and the derivation publishes a number for it.

WHY A SEPARATE FILE. The predicate is the thing that decides whether a measurement
is allowed to stand, and inlined in a `-c` string it could not be tested with a
crafted input. selftest-guards.sh now drives it with (a) the committed good rep,
(b) that rep with one nonzero rc, (c) that rep with an emptied counter file, and
asserts the verdicts — which is what makes the guard demonstrably load-bearing
rather than merely present.

FAIL CLOSED ON EVERY UNKNOWN. Anything unreadable, unparseable, absent or
unrecognised is INCOMPLETE, never "probably fine": the only cost of redoing a
good rep is time on a metered box, while the cost of keeping a bad one is a
published number with no provenance.
"""
import json
import os
import sys

# Same floor as derive.py's MUX_MIN and positive-control.sh's MUX_MIN_PCT: below
# this a count is a multiplexed estimate, not a count.
MUX_MIN = 99.0


def csv_rows_valid(path):
    """-> (ok, detail). A counter file is valid only if it AFFIRMATIVELY carries
    readable, supported, non-multiplexed rows.

    Note the shape deliberately avoided here: this does not return ok because it
    failed to find something bad. An empty or header-only file yields n_rows == 0
    and is REFUSED, because a file with no rows has measured nothing — the vacuous
    pass CLAUDE.md names. Both perf layouts are handled: `--per-socket` inserts
    two leading fields, so the enabled column moves from 5 to 7, and reading the
    wrong one silently interprets run_time as a percentage.
    """
    if not os.path.exists(path):
        return False, 'absent'
    if os.path.getsize(path) == 0:
        return False, 'empty (0 bytes)'
    n_rows = 0
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            f = line.split(',')
            per_socket = bool(f) and f[0].startswith('S') and len(f) >= 7
            if per_socket:
                val, enabled = f[2], f[6]
            elif len(f) >= 5:
                val, enabled = f[0], f[4]
            else:
                continue
            if val in ('<not supported>', '<not counted>'):
                return False, 'carries %s' % val
            try:
                float(val)
                e = float(enabled)
            except ValueError:
                continue
            if e < MUX_MIN:
                return False, 'multiplexed at %.2f%% enabled (floor %.0f%%)' % (e, MUX_MIN)
            n_rows += 1
    if n_rows == 0:
        return False, 'no readable counter rows'
    return True, '%d rows' % n_rows


def check(repdir):
    """-> list of reasons this rep is NOT safe to skip (empty list = safe)."""
    meta_path = os.path.join(repdir, 'meta.json')
    if not os.path.exists(meta_path):
        return ['meta.json absent']
    try:
        with open(meta_path) as fh:
            d = json.load(fh)
    except (OSError, ValueError) as exc:
        return ['meta.json unreadable/unparseable: %s' % exc]

    bad = []

    occ = d.get('occupancy')
    if not occ:
        bad.append('occupancy block absent or empty')
    else:
        for arm, v in sorted(occ.items()):
            if not v:
                bad.append('occupancy[%s]: no step record' % arm)
            elif not v.get('ok'):
                bad.append('occupancy[%s]: ok=%s rows=%s err=%s'
                           % (arm, v.get('ok'), v.get('rows_total'),
                              v.get('requests_error')))

    # The half the old predicate was missing. Enumerated from the dict so a new
    # arm cannot default to unchecked.
    rc = d.get('rc')
    if not isinstance(rc, dict) or not rc:
        bad.append('rc block absent or empty — a rep with no recorded return '
                   'codes cannot be certified complete')
    else:
        nz = {k: v for k, v in rc.items() if v != 0}
        if nz:
            bad.append('rc nonzero: %s (all of %s must be 0)' % (nz, sorted(rc)))

    # The other half: the counter files this rep's own meta.json says it wrote.
    # Driven by meta.json's roster rather than a hardcoded list, so a rep that
    # names a file it did not write fails on that file.
    pf = d.get('perf_files')
    if not isinstance(pf, dict) or not pf:
        bad.append('perf_files block absent or empty — the counter files cannot '
                   'be verified, so the rep is unverifiable')
    else:
        for key, name in sorted(pf.items()):
            ok, detail = csv_rows_valid(os.path.join(repdir, name))
            if not ok:
                bad.append('counter file %s (%s): %s' % (name, key, detail))

    if d.get('warm_verified_zero_disk_reads') is not True:
        bad.append('warmth: warm_verified_zero_disk_reads=%r (want True), '
                   'read_bytes delta=%r'
                   % (d.get('warm_verified_zero_disk_reads'),
                      d.get('warm_read_bytes_delta')))
    if d.get('client_saturation_gate_pass') is not True:
        bad.append('client saturation: gate_pass=%r util=%r'
                   % (d.get('client_saturation_gate_pass'),
                      d.get('client_utilisation')))
    return bad


def main(argv):
    if len(argv) != 2:
        sys.exit('usage: rep-complete.py <repdir>')
    bad = check(argv[1])
    if bad:
        print('INCOMPLETE %s:\n  - %s' % (argv[1], '\n  - '.join(bad)))
        return 1
    print('COMPLETE %s (occupancy, rc, counter files, warmth, client saturation '
          'all verified)' % argv[1])
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
