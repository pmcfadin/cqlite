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

# THE SCHEMA-DEFINED ROSTERS, asserted PRESENT (roborev round 2 finding #6).
#
# The first version of this predicate required only that `rc` and `perf_files` be
# NONEMPTY dicts and that each CSV carry at least ONE readable row. Both are
# existence tests standing in for completeness tests, and the gap is the same one
# they were written to close: a capture that wrote `rc` with two of six arms, or a
# CSV truncated after its first event, satisfied every condition and was certified
# "complete and safe to skip" — permanently, because a skipped rep is never
# revisited. "At least one row" is the vacuous-pass shape with a threshold of one.
#
# So the rosters are pinned to what capture-endpoint.sh's schema actually writes,
# and a rep is complete only if it carries ALL of them. Pinned rather than derived
# from the file under test, because a roster read out of the artefact can only ever
# confirm the artefact agrees with itself.
EXPECTED_RC_ARMS = {
    'alignedA', 'alignedB', 'core_interior', 'loadgen_interior',
    'uncore', 'loadgen_uncore',
}
EXPECTED_RC_ARMS_STALLS = {'alignedC'}
EXPECTED_PERF_FILES = {'alignedA', 'alignedB', 'interiorA', 'uncore'}

# The event set each counter file must carry in full, from derive.py's CORE_A /
# CORE_B and the uncore roster. A file holding a strict subset is a truncated
# capture, and derive.py would fail on it later — the point of catching it here is
# that resume would otherwise have SKIPPED the rep, so derive.py never sees it.
CORE_A_EVENTS = {'cycles', 'instructions', 'task-clock', 'LLC-loads',
                 'LLC-load-misses', 'cache-references', 'cache-misses'}
CORE_B_EVENTS = {'cycles', 'instructions', 'task-clock', 'L1-dcache-loads',
                 'L1-dcache-load-misses', 'dTLB-load-misses', 'branch-misses'}
IMC_COUNT = 12
UNCORE_EVENTS = set('uncore_imc_%d/cas_count_%s/' % (i, k)
                    for i in range(IMC_COUNT) for k in ('read', 'write'))
EXPECTED_EVENTS = {
    'alignedA': ('core', CORE_A_EVENTS),
    'alignedB': ('core', CORE_B_EVENTS),
    'interiorA': ('core', CORE_A_EVENTS),
    'uncore': ('uncore', UNCORE_EVENTS),
}


def csv_rows_valid(path, kind, expected_events):
    """-> (ok, detail). A counter file is valid only if it AFFIRMATIVELY carries the
    COMPLETE expected event set, every row readable, supported and unmultiplexed.

    Note the two shapes deliberately avoided here. It does not return ok because it
    failed to find something bad; and it does not accept "at least one row", which
    is the vacuous pass with the threshold set to one — a CSV truncated after its
    first event used to certify a rep as complete. The expected set is passed IN, so
    the file is checked against the schema rather than against itself.

    Both perf layouts are handled: `--per-socket` inserts two leading fields, so the
    enabled column moves from 5 to 7, and reading the wrong one silently interprets
    run_time as a percentage.
    """
    if not os.path.exists(path):
        return False, 'absent'
    if os.path.getsize(path) == 0:
        return False, 'empty (0 bytes)'
    seen = set()
    n_rows = 0
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            f = line.split(',')
            per_socket = bool(f) and f[0].startswith('S') and len(f) >= 7
            if per_socket:
                val, name, enabled = f[2], f[4], f[6]
            elif len(f) >= 5:
                val, name, enabled = f[0], f[2], f[4]
            else:
                continue
            if val in ('<not supported>', '<not counted>'):
                return False, '%s carries %s' % (name, val)
            try:
                float(val)
                e = float(enabled)
            except ValueError:
                # An unreadable enabled% is not a healthy one: reading it is the only
                # evidence the count is not a multiplexed estimate. Same three-state
                # trap as positive-control.sh's MUXMIN default.
                return False, ('%s has an unreadable enabled%% (%r) — an '
                               'unverifiable count is not a usable one'
                               % (name, enabled))
            if e < MUX_MIN:
                return False, ('%s multiplexed at %.2f%% enabled (floor %.0f%%)'
                               % (name, e, MUX_MIN))
            # perf strips the `:u` modifier from some event names and keeps it on
            # others, so compare on the base name for core events. Uncore event
            # names carry no modifier and their slashes must survive intact.
            seen.add(name if kind == 'uncore' else name.split(':')[0])
            n_rows += 1
    if n_rows == 0:
        return False, 'no readable counter rows'
    missing = expected_events - seen
    if missing:
        shown = sorted(missing)
        if len(shown) > 6:
            shown = shown[:6] + ['... %d more' % (len(missing) - 6)]
        return False, ('%d of %d expected events present; MISSING %s (a truncated '
                       'capture, not a complete one)'
                       % (len(seen & expected_events), len(expected_events), shown))
    return True, '%d rows, all %d expected events present' % (n_rows,
                                                             len(expected_events))


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

    # The half the old predicate was missing. Checked against the SCHEMA roster, not
    # merely "nonempty" — and enumerated as well, so an arm outside the roster is
    # still required to be zero rather than ignored.
    rc = d.get('rc')
    if not isinstance(rc, dict) or not rc:
        bad.append('rc block absent or empty — a rep with no recorded return '
                   'codes cannot be certified complete')
    else:
        missing_arms = EXPECTED_RC_ARMS - set(rc)
        if missing_arms:
            bad.append('rc roster incomplete: missing %s (a partial rc block '
                       'records some arms and says nothing about the rest)'
                       % sorted(missing_arms))
        nz = {k: v for k, v in rc.items() if v != 0}
        if nz:
            bad.append('rc nonzero: %s (all of %s must be 0)' % (nz, sorted(rc)))

    # The other half: the counter files, checked against the schema roster AND each
    # one's complete expected event set. meta.json's own `perf_files` is cross-checked
    # against the roster rather than trusted as the roster, because a roster read out
    # of the artefact can only confirm the artefact agrees with itself.
    pf = d.get('perf_files')
    if not isinstance(pf, dict) or not pf:
        bad.append('perf_files block absent or empty — the counter files cannot '
                   'be verified, so the rep is unverifiable')
    else:
        missing_files = EXPECTED_PERF_FILES - set(pf)
        if missing_files:
            bad.append('perf_files roster incomplete: missing %s'
                       % sorted(missing_files))
        for key, name in sorted(pf.items()):
            exp = EXPECTED_EVENTS.get(key)
            if exp is None:
                bad.append('perf_files names an unrecognised arm %r (%s); this '
                           'predicate has no expected event set for it and will '
                           'not certify what it cannot check' % (key, name))
                continue
            kind, events = exp
            ok, detail = csv_rows_valid(os.path.join(repdir, name), kind, events)
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
