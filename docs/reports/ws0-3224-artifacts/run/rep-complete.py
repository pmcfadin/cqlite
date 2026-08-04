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

# THE SCHEMA IS SINGLE-HOMED in harness/ws0schema.py, not restated here.
#
# It was restated here, once, and that is how round 3 arrived: the rosters this file
# pinned were correct, and derive.py's equivalent check — written in the same hour —
# enumerated the dict instead, so the SAME finding came back against the sibling
# file. Four consumers, one fact; every independent restatement is another chance to
# get it wrong in a new way. See that module's header for the full sequence.
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)), '..', 'harness'))
import ws0schema


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

    # Occupancy: roster + values, asked of the schema (round 5 finding #4). This used
    # to iterate only the keys PRESENT, so a block omitting `uncore` was certified.
    bad += ws0schema.validate_occupancy(d.get('occupancy'),
                                        ws0schema.OCCUPANCY_ARMS_PRIMARY,
                                        'meta.json',
                                        corpus_rows=d.get('corpus_rows'))

    # The half the old predicate was missing, now asked of the schema.
    bad += ws0schema.validate_rc_block(d.get('rc'),
                                       ws0schema.RC_ARMS_PRIMARY, 'meta.json')

    # The other half: the counter files. meta.json's own `perf_files` is
    # CROSS-CHECKED against the schema roster rather than trusted AS the roster — a
    # roster read out of the artefact under test can only confirm that the artefact
    # agrees with itself.
    pf = d.get('perf_files')
    if not isinstance(pf, dict) or not pf:
        bad.append('perf_files block absent or empty — the counter files cannot '
                   'be verified, so the rep is unverifiable')
    else:
        missing_files = set(ws0schema.PERF_FILES_PRIMARY) - set(pf)
        if missing_files:
            bad.append('perf_files roster incomplete: missing %s'
                       % sorted(missing_files))
        for key, name in sorted(pf.items()):
            bad += ws0schema.validate_counter_file(os.path.join(repdir, name), key)

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
