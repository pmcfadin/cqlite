"""#3224 — THE capture schema, single-homed. Imported, never run.

    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    '..', 'harness'))
    import ws0schema

WHY THIS FILE EXISTS, and it is a process observation rather than a design one.

Three successive roborev rounds on PR #3286 found 6, then 7, then 5 fail-open
defects, and the *same* defect kept reappearing in a *different* consumer:

  round 1  capture-endpoint.sh's rc expression omitted two arms
  round 2  rep-complete.py certified on a NONEMPTY rc dict, not the roster
  round 3  derive.py's assert_rc_all_zero enumerated the dict, not the roster
           -- i.e. exactly the round-2 finding, in the sibling file, because the
           round-2 fix was applied where the finding pointed and nowhere else

Same story for the uncore roster: asserted in derive.py (round 1), then found
missing in ac5-analyse.py (round 2), then found in rep-complete.py tracking event
names GLOBALLY instead of per (socket, event) (round 3). Four files, one fact,
fixed four times, wrong in a new way each time.

Fixing each report as it arrived was never going to converge, because the defect is
not in any of those files -- it is that **the schema had no single home**, so every
consumer re-derived it and each re-derivation was an independent chance to get it
wrong. This module is that home. A consumer that asks it a question cannot disagree
with a consumer that asks the same question, and a roster correction lands
everywhere at once.

CONTRACT: importing has no side effects. Every function is pure and returns a LIST
OF PROBLEM STRINGS -- empty means valid. Nothing here raises or exits, so each
caller keeps its own error convention (derive.py's SystemExit, rep-complete.py's
exit code, ac5-analyse.py's terminal verdict).

FAIL CLOSED, AFFIRMATIVELY. Every check requires the expected thing to be PRESENT
rather than testing for the absence of a bad thing, and no check returns valid for
an empty subject: a validation with no subject has no verdict to give.
"""

import os

# enabled% floor. Below this a count is a scaled estimate, not a count. Mirrors
# positive-control.sh's MUX_MIN_PCT; they are the same threshold in two languages.
MUX_MIN = 99.0

# ---------------------------------------------------------------- rc rosters
# Exactly what run/capture-endpoint.sh writes into meta.json's "rc" block, and what
# run/capture-stalls.sh writes into meta-stalls.json. The two load-generator arms are
# listed FIRST because they are the ones every previous version of every consumer
# omitted: perf wrapping `sleep` exits 0 when the load generator behind it died, so
# these two are the only record that the row counts are valid.
RC_ARMS_PRIMARY = frozenset({
    'loadgen_interior', 'loadgen_uncore',
    'alignedA', 'alignedB', 'core_interior', 'uncore',
})
RC_ARMS_STALLS = frozenset({'alignedC'})

# ---------------------------------------------------------- occupancy rosters
# The arms capture-endpoint.sh records occupancy for, and capture-stalls.sh's single
# arm. Added in round 5 (finding #4) because both consumers iterated only the keys
# PRESENT in meta.json — the same existence-instead-of-completeness shape as the rc
# block two rounds earlier, in a third field. A block omitting `uncore` was certified
# for resume AND passed derive.py's equivalent loop, so the arm whose absence matters
# most to the bandwidth figures was the one nothing required.
OCCUPANCY_ARMS_PRIMARY = frozenset({'interior', 'alignedA', 'alignedB', 'uncore'})
OCCUPANCY_ARMS_STALLS = frozenset({'alignedC'})

# Minimum busy fraction for an occupancy arm. Guards against IDLE PERIODS inside a
# step, which break the interior convention's premise that whole-step throughput
# represents the interior perf window.
#
# LOW BY DESIGN, and the reason is measured rather than cautious: the estimate is
# requests_ok * p50 / concurrency / duration — a product of three measured quantities,
# so it carries their combined error, and one committed arm reads 1.0031 (above 1.0,
# which is only possible as estimator error). A tight floor here would be a false-FAIL
# generator, i.e. round 1 finding #1 again. Committed reps span 0.9453..1.0031.
#
# Single-homed here rather than in common.sh so the shell captures and the Python
# validators cannot disagree; common.sh reads its default from this value's twin and
# the selftest asserts the committed data clears it.
BUSY_FRACTION_FLOOR = 0.90

# ------------------------------------------------------- counter-file rosters
PERF_FILES_PRIMARY = frozenset({'alignedA', 'alignedB', 'interiorA', 'uncore'})

CORE_A_EVENTS = frozenset({
    'cycles', 'instructions', 'task-clock', 'LLC-loads', 'LLC-load-misses',
    'cache-references', 'cache-misses'})
CORE_B_EVENTS = frozenset({
    'cycles', 'instructions', 'task-clock', 'L1-dcache-loads',
    'L1-dcache-load-misses', 'dTLB-load-misses', 'branch-misses'})
CORE_C_EVENTS = frozenset({
    'cycles', 'instructions', 'task-clock',
    'cycle_activity.stalls_l3_miss', 'cycle_activity.stalls_l2_miss',
    'cycle_activity.stalls_total',
    'l1d_pend_miss.pending', 'l1d_pend_miss.pending_cycles'})

# uncore_imc_0..11 on this host; host/sysfs-pmus.txt is authoritative and
# host/imc-socket-map.txt records the per-socket split. Each instance is read for
# cas_count_read and cas_count_write.
IMC_COUNT = 12
IMC_KINDS = ('read', 'write')
SOCKETS = ('S0', 'S1')
IMC_EVENTS = frozenset(
    'uncore_imc_%d/cas_count_%s/' % (i, k)
    for i in range(IMC_COUNT) for k in IMC_KINDS)
# A COMPLETE uncore capture is every event on EVERY socket -- (socket, event) pairs,
# not a flat event set. Round 3 found rep-complete.py tracking the flat set, so a CSV
# carrying all 24 events for S0 and no S1 rows at all certified as complete.
IMC_PAIRS = frozenset((s, e) for s in SOCKETS for e in IMC_EVENTS)

# arm key -> (layout, expected event set). The layout decides how a perf -x, row is
# read AND how its event name is normalised, so it must travel with the event set.
COUNTER_FILES = {
    'alignedA': ('core', CORE_A_EVENTS),
    'alignedB': ('core', CORE_B_EVENTS),
    'interiorA': ('core', CORE_A_EVENTS),
    'uncore': ('uncore', IMC_EVENTS),
    'alignedC': ('core', CORE_C_EVENTS),
}


def _fmt(items, limit=6):
    s = sorted(str(x) for x in items)
    if len(s) > limit:
        return s[:limit] + ['... %d more' % (len(s) - limit)]
    return s


def validate_rc_block(rc, roster, which):
    """The rc block: present, complete against `roster`, and every arm zero.

    All three, in that order. Completeness before values, because a partial block
    whose present arms are all zero is the exact shape that passed round 3: it
    reports on some arms and says NOTHING about the rest, and "nothing" is not
    "fine". Arms outside the roster are still required to be zero rather than
    ignored, so an arm added to the capture later cannot default to unchecked.
    """
    problems = []
    if not isinstance(rc, dict) or not rc:
        return ['%s: rc block absent or empty — a capture with no recorded return '
                'codes cannot be certified' % which]
    missing = set(roster) - set(rc)
    if missing:
        problems.append(
            '%s: rc roster incomplete, missing %s (a partial block reports on some '
            'arms and says nothing about the rest)' % (which, _fmt(missing)))
    nonzero = {k: v for k, v in rc.items() if v != 0}
    if nonzero:
        problems.append(
            '%s: rc nonzero %s (all of %s must be 0). A counter file can parse '
            'cleanly and still be wrong: a dead load generator leaves perf own rc '
            'at 0 while the row counts every per-row figure divides by are invalid.'
            % (which, nonzero, _fmt(rc, 12)))
    return problems


def read_counter_rows(path, layout):
    """-> (rows, problems). rows is a list of (socket, event, value, enabled).

    ONE reader for both perf layouts, because reading the wrong column is a silent
    corruption rather than an error: `--per-socket` inserts two leading fields, so
    the enabled percentage moves from field 5 to field 7, and field 5 there is
    run_time — a nanosecond count that always compares as >= 99.
    """
    problems = []
    rows = []
    if not os.path.exists(path):
        return rows, ['absent']
    if os.path.getsize(path) == 0:
        return rows, ['empty (0 bytes)']
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            f = line.split(',')
            if layout == 'uncore':
                if len(f) < 7 or not f[0].startswith('S'):
                    continue
                sock, val, unit, name, enabled = f[0], f[2], f[3], f[4], f[6]
            else:
                if len(f) < 5:
                    continue
                sock, val, unit, name, enabled = '', f[0], f[1], f[2], f[4]
            if val in ('<not supported>', '<not counted>'):
                problems.append('%s reads %s' % (name, val))
                continue
            try:
                v = float(val)
            except ValueError:
                problems.append('%s has an unreadable value (%r)' % (name, val))
                continue
            try:
                e = float(enabled)
            except ValueError:
                # An unreadable percentage is NOT a healthy one. Reading it is the
                # only evidence the count is not a multiplexed estimate, so this is
                # a non-passing state rather than a missing nicety.
                problems.append('%s has an unreadable enabled%% (%r) — an '
                                'unverifiable count is not a usable one'
                                % (name, enabled))
                continue
            if e < MUX_MIN:
                problems.append('%s only %.2f%% enabled (floor %.0f%%): a '
                                'MULTIPLEXED ESTIMATE, not a count'
                                % (name, e, MUX_MIN))
                continue
            # perf strips the `:u` modifier from some event names and keeps it on
            # others, so core events are compared on the base name. Uncore names
            # carry no modifier and their slashes must survive intact.
            key = name if layout == 'uncore' else name.split(':')[0]
            rows.append((sock, key, v, e, unit))
    return rows, problems


def validate_counter_file(path, arm):
    """-> problems. The file carries its COMPLETE expected event set, once each.

    For a core arm that is the event set. For the uncore arm it is (socket, event)
    PAIRS on both sockets — the distinction round 3 turned on. Duplicates are
    refused in both cases: a repeated instance means a malformed capture, and summing
    it would double-count that channel.
    """
    spec = COUNTER_FILES.get(arm)
    if spec is None:
        return ['%s: unrecognised arm %r — this schema has no expected event set '
                'for it and will not certify what it cannot check' % (path, arm)]
    layout, events = spec
    rows, problems = read_counter_rows(path, layout)
    problems = ['%s: %s' % (os.path.basename(path), p) for p in problems]
    if not rows:
        problems.append('%s: no readable counter rows — a file with no rows has '
                        'measured nothing' % os.path.basename(path))
        return problems

    if layout == 'uncore':
        seen = [(r[0], r[1]) for r in rows]
        expected = IMC_PAIRS
        label = '(socket, event) pairs'
    else:
        seen = [r[1] for r in rows]
        expected = set(events)
        label = 'events'

    dupes = sorted({x for x in seen if seen.count(x) > 1})
    if dupes:
        problems.append('%s: DUPLICATE %s %s — a repeated instance means a '
                        'malformed capture'
                        % (os.path.basename(path), label, _fmt(dupes)))
    missing = expected - set(seen)
    if missing:
        problems.append(
            '%s: %d of %d expected %s present, MISSING %s (a truncated capture, '
            'not a complete one)'
            % (os.path.basename(path), len(set(seen) & expected), len(expected),
               label, _fmt(missing)))
    return problems


def validate_occupancy(occ, roster, which):
    """The occupancy block: present, complete against `roster`, every arm ok.

    Completeness BEFORE values, for the same reason as validate_rc_block: both
    consumers used to iterate `occ.items()`, so a block that simply omitted an arm
    reported on what was there and said nothing about what was not — and the arm most
    likely to be missing, `uncore`, is the one the DRAM-bandwidth and NUMA-confinement
    figures rest on.

    The busy fraction is checked HERE as well as in the capture, deliberately. The
    capture computes `ok` at write time; this re-derives the judgement at read time
    from the recorded estimate, so an artefact written by an older capture — one whose
    `ok` predates the floor — cannot be certified by its own stale verdict. That is the
    same "re-check rather than trust" contract derive.py states for every other gate.
    """
    problems = []
    if not isinstance(occ, dict) or not occ:
        return ['%s: occupancy block absent or empty' % which]
    missing = set(roster) - set(occ)
    if missing:
        problems.append(
            '%s: occupancy roster incomplete, missing %s (a partial block reports on '
            'the arms present and says nothing about the rest — and `uncore` is the '
            'arm the DRAM bandwidth and NUMA figures rest on)'
            % (which, _fmt(missing)))
    for arm in sorted(occ):
        v = occ[arm]
        if not v:
            problems.append('%s: occupancy[%s] has no step record' % (which, arm))
            continue
        if not v.get('ok'):
            problems.append(
                '%s: occupancy[%s] ok=%s rows=%s err=%s unavailable=%s'
                % (which, arm, v.get('ok'), v.get('rows_total'),
                   v.get('requests_error'), v.get('requests_unavailable')))
        bf = v.get('busy_fraction_estimate')
        if bf is None:
            problems.append(
                '%s: occupancy[%s] busy fraction not computable — an unverifiable '
                'occupancy is not an established one' % (which, arm))
        elif bf < BUSY_FRACTION_FLOOR:
            problems.append(
                '%s: occupancy[%s] busy fraction %.4f below the floor %.2f — the '
                'workers were idle for part of the step, so whole-step throughput '
                'does not represent the interior perf window'
                % (which, arm, bf, BUSY_FRACTION_FLOOR))
    return problems
