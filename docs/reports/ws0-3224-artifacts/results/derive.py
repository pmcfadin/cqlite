#!/usr/bin/env python3
"""#3224 derivation — every headline re-derived from COMMITTED inputs only.

Usage:
    python3 derive.py <results-root> [--out derived.json] [--md derived.md]

Reads, per endpoint per rep:  meta.json, perf-coreA-aligned.csv,
perf-coreB-aligned.csv, perf-coreA-interior.csv, perf-uncore.csv
and writes the full accounting. Nothing is hardcoded that the artefacts carry:
the window, the row counts, the CPU sets and the corpus size are all READ.

DESIGN RULES THIS SCRIPT ENFORCES (each one a #3217 lesson):
  * FAIL CLOSED on multiplexing: any event with enabled% < 99 is refused, not
    silently published as a scaled estimate.
  * FAIL CLOSED on an event-name miss: perf strips the modifier from some event
    names, so match either form (the bug that false-FAILed the positive control).
  * The window is DATA, never a literal.
  * >=3 reps -> min/median/max, because a delta between two undispersed points
    cannot be defended (#3217 ran reps=1).
  * IPC is a pure ratio and therefore invariant to the window; cycles/row is NOT,
    which is exactly why both denominator conventions are computed and compared.
  * The residual is printed as a NUMBER and as a PERCENTAGE. AC4 fails if omitted.
"""
import argparse, glob, json, os, statistics, sys

# THE SCHEMA IS SINGLE-HOMED in harness/ws0schema.py. This file's own restatement of
# the rc roster is exactly what round 3 caught: assert_rc_all_zero enumerated the
# dict it was handed, so a partial block such as {"alignedA": 0} passed while saying
# nothing about the other five arms. See that module's header.
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'harness'))
import ws0schema

CORPUS_DEFAULT = 3999890
MUX_MIN = 99.0            # enabled% floor; below this the count is an estimate
# The IMC roster comes from the schema, not from a local copy: it is the same fact
# ac5-analyse.py and rep-complete.py need, and it was independently restated in each
# of them before round 3. IMC_EXPECTED_INSTANCES is the PER-SOCKET count (every
# instance read for cas_count_read and cas_count_write), which is what the summing
# loop below counts.
IMC_COUNT = ws0schema.IMC_COUNT
IMC_EXPECTED_INSTANCES = len(ws0schema.IMC_EVENTS)
# The dispersion floor named in this file's DESIGN RULES: min/median/max needs >= 3
# points, because a delta between undispersed points cannot be defended (#3217 ran
# reps=1). Single-homed so the primary-arm WARNING and the group-C refusal below
# cannot drift apart.
MIN_REPS = 3


# --------------------------------------------------------------- perf CSV I/O
def parse_perf(path, per_socket=False):
    """-> {event: {'value': float, 'enabled': float, 'unit': str, 'socket': str}}

    perf's -x, layout is  value,unit,event,run_time,enabled_pct,...
    With --per-socket TWO leading fields are inserted (S<n>, cpu-count), so the
    enabled column the RUNBOOK calls 'field 5' is FIELD 7 there. Getting this
    wrong silently reads run_time as a percentage.
    """
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        f = line.split(',')
        try:
            if per_socket:
                if len(f) < 7:
                    continue
                sock, val, unit, ev, enabled = f[0], f[2], f[3], f[4], f[6]
            else:
                if len(f) < 5:
                    continue
                sock, val, unit, ev, enabled = '', f[0], f[1], f[2], f[4]
        except IndexError:
            continue
        if val in ('<not supported>', '<not counted>'):
            out.setdefault(ev, []).append(
                {'value': None, 'enabled': None, 'unit': unit, 'socket': sock,
                 'raw': val})
            continue
        try:
            v = float(val); e = float(enabled)
        except ValueError:
            continue
        out.setdefault(ev, []).append(
            {'value': v, 'enabled': e, 'unit': unit, 'socket': sock})
    return out


def ev(parsed, name, socket=None):
    """Fetch one event, tolerating perf's modifier stripping (LLC-loads:u is
    echoed back as LLC-loads while cache-references:u keeps its suffix)."""
    for key in (name, name + ':u'):
        if key in parsed:
            rows = parsed[key]
            if socket is not None:
                rows = [r for r in rows if r['socket'] == socket]
            if rows:
                return rows
    base = name.split(':')[0]
    for key, rows in parsed.items():
        if key.split(':')[0] == base:
            if socket is not None:
                rows = [r for r in rows if r['socket'] == socket]
            if rows:
                return rows
    return []


def scalar(parsed, name, path):
    rows = ev(parsed, name)
    if not rows:
        raise SystemExit("FATAL: event %r absent from %s" % (name, path))
    r = rows[0]
    if r['value'] is None:
        raise SystemExit("FATAL: event %r reads %s in %s — a counter that does "
                         "not count cannot be published"
                         % (name, r.get('raw'), path))
    if r['enabled'] is not None and r['enabled'] < MUX_MIN:
        raise SystemExit(
            "FATAL: event %r in %s was only %.2f%% enabled (floor %.0f%%). The "
            "count is a MULTIPLEXED ESTIMATE. Split the event group; do not "
            "publish scaled values (RUNBOOK step 6)."
            % (name, path, r['enabled'], MUX_MIN))
    return r['value']


# ------------------------------------------------------------------ one rep
CORE_A = ['cycles', 'instructions', 'task-clock', 'LLC-loads',
          'LLC-load-misses', 'cache-references', 'cache-misses']
CORE_B = ['cycles', 'instructions', 'task-clock', 'L1-dcache-loads',
          'L1-dcache-load-misses', 'dTLB-load-misses', 'branch-misses']
# Group C — the MEASURED memory-stall arm (run/capture-stalls.sh). Optional: a
# results tree without it still derives everything else, and the AC4 accounting
# then falls back to the MODELLED charge alone and says so.
CORE_C = ['cycles', 'instructions', 'task-clock',
          'cycle_activity.stalls_l3_miss', 'cycle_activity.stalls_l2_miss',
          'cycle_activity.stalls_total',
          'l1d_pend_miss.pending', 'l1d_pend_miss.pending_cycles']


def assert_rc_all_zero(meta, repdir, which, roster):
    """Refuse a rep whose own meta file records a failed or incompletely-recorded arm.

    THE GATES ARE RE-CHECKED HERE rather than trusted, which is this script's stated
    contract — but the `rc` block was the one recorded fact it never read (round 2
    finding #1), and the first version of this function then checked only the arms
    that happened to be present (round 3 finding #2). Both are now the schema's
    business, so this function cannot disagree with rep-complete.py's answer to the
    same question.
    """
    problems = ws0schema.validate_rc_block(meta.get('rc'), roster,
                                           '%s %s' % (which, repdir))
    if problems:
        raise SystemExit('FATAL: ' + '\n  - '.join([''] + problems).lstrip())


def do_stalls(repdir):
    """Group C for one rep, or None if this rep has no stalls arm.

    Same fail-closed contract as do_rep: the gates are RE-CHECKED here rather
    than trusted from the meta file, and a multiplexed or absent counter is
    refused rather than published.
    """
    meta_p = os.path.join(repdir, 'meta-stalls.json')
    csv_p = os.path.join(repdir, 'perf-coreC-aligned.csv')
    if not (os.path.exists(meta_p) and os.path.exists(csv_p)):
        return None
    meta = json.load(open(meta_p))
    assert_rc_all_zero(meta, repdir, 'stalls rep', ws0schema.RC_ARMS_STALLS)
    rows = meta['occupancy']['alignedC']['rows_total']
    problems = []
    if not meta['occupancy']['alignedC'].get('ok'):
        problems.append('occupancy[alignedC] not ok')
    if not meta.get('warm_verified_zero_disk_reads'):
        problems.append('warmth: read_bytes delta=%s' % meta.get('warm_read_bytes_delta'))
    if not meta.get('client_saturation_gate_pass'):
        problems.append('client saturation util=%s' % meta.get('client_utilisation'))
    if problems:
        raise SystemExit("FATAL: stalls rep %s is invalid: %s" % (repdir, problems))

    p = parse_perf(csv_p)
    c = {name: scalar(p, name, csv_p) for name in CORE_C}
    per_row = {k: v / rows for k, v in c.items()}
    pend, pend_cyc = c['l1d_pend_miss.pending'], c['l1d_pend_miss.pending_cycles']
    return {
        'repdir': repdir, 'label': meta['label'], 'rep': meta['rep'],
        'rows_in_window': rows,
        'counters': c, 'per_row': per_row,
        'ipc': c['instructions'] / c['cycles'],
        # Fractions of TOTAL cycles: what share of the core's time is spent
        # stalled with an L3 miss outstanding. This is the attribution term.
        'stalls_l3_miss_frac_cycles': c['cycle_activity.stalls_l3_miss'] / c['cycles'],
        'stalls_l2_miss_frac_cycles': c['cycle_activity.stalls_l2_miss'] / c['cycles'],
        'stalls_total_frac_cycles': c['cycle_activity.stalls_total'] / c['cycles'],
        # MEASURED memory-level parallelism: mean outstanding L1D misses over the
        # cycles in which at least one was outstanding. This is the divisor that
        # turns an unloaded serial-chase latency into a per-miss cost, so the
        # MODELLED cross-check no longer needs a guessed MLP.
        'mlp': (pend / pend_cyc) if pend_cyc else None,
        'client_utilisation': meta['client_utilisation'],
        'warm_read_bytes_delta': meta['warm_read_bytes_delta'],
    }


def parse_penalty_summary(path):
    """Parse the committed run/penalty-probe.sh summary table.

    Columns: level ws_MiB buf_MiB cyc_per_acc ns_per_acc LLCld_acc LLCmiss_acc
             dTLBmiss_acc
    Returns {level: {...}}. Absent file -> {} (the MODELLED cross-check is then
    reported as unavailable rather than invented).
    """
    if not path or not os.path.exists(path):
        return {}
    out = {}
    for line in open(path):
        f = line.split()
        if len(f) != 8 or f[0] in ('level', '=='):
            continue
        try:
            out[f[0]] = {
                'ws_MiB': float(f[1]), 'buf_MiB': float(f[2]),
                'cycles_per_access': float(f[3]), 'ns_per_access': float(f[4]),
                'llc_loads_per_access': float(f[5]),
                'llc_misses_per_access': float(f[6]),
                'dtlb_misses_per_access': float(f[7]),
            }
        except ValueError:
            continue
    return out


def do_rep(repdir):
    meta = json.load(open(os.path.join(repdir, 'meta.json')))
    rows_corpus = meta.get('corpus_rows', CORPUS_DEFAULT)
    occ = meta['occupancy']

    # ---- validity gates, re-checked here rather than trusted from meta ----
    assert_rc_all_zero(meta, repdir, 'rep', ws0schema.RC_ARMS_PRIMARY)
    problems = []
    for arm, o in occ.items():
        if not o or not o.get('ok'):
            problems.append('occupancy[%s] not ok' % arm)
    if not meta.get('warm_verified_zero_disk_reads'):
        problems.append('warmth: read_bytes delta=%s'
                        % meta.get('warm_read_bytes_delta'))
    if not meta.get('client_saturation_gate_pass'):
        problems.append('client saturation util=%s'
                        % meta.get('client_utilisation'))
    if problems:
        raise SystemExit("FATAL: rep %s is invalid: %s" % (repdir, problems))

    pA = parse_perf(os.path.join(repdir, 'perf-coreA-aligned.csv'))
    pB = parse_perf(os.path.join(repdir, 'perf-coreB-aligned.csv'))
    pI = parse_perf(os.path.join(repdir, 'perf-coreA-interior.csv'))
    fA = os.path.join(repdir, 'perf-coreA-aligned.csv')
    fB = os.path.join(repdir, 'perf-coreB-aligned.csv')
    fI = os.path.join(repdir, 'perf-coreA-interior.csv')

    # ================= ALIGNED convention =================================
    # The perf window IS the loadgen step, so rows_in_window is the step's own
    # row count -- numerator and denominator share ONE interval by construction.
    rowsA = occ['alignedA']['rows_total']
    rowsB = occ['alignedB']['rows_total']
    aligned = {}
    for name in CORE_A:
        aligned[name] = scalar(pA, name, fA)
    for name in CORE_B:
        if name in ('cycles', 'instructions', 'task-clock'):
            continue
        aligned[name] = scalar(pB, name, fB)

    perrow = {k: v / rowsA for k, v in aligned.items() if k in CORE_A}
    perrow.update({k: aligned[k] / rowsB for k in CORE_B
                   if k not in ('cycles', 'instructions', 'task-clock')})

    ipc_A = aligned['instructions'] / aligned['cycles']
    # Group B saw a DIFFERENT loadgen run; its IPC must still agree, which is the
    # cross-check that both groups observed the same workload (positive-control
    # P2 applied across event groups).
    ipc_B = scalar(pB, 'instructions', fB) / scalar(pB, 'cycles', fB)

    # ================= INTERIOR convention (#3217's) ======================
    # counters over an interior window; rate from the whole step. Reproduces
    # #3217 exactly so the two conventions can be compared.
    win = meta['window_secs']
    rate_step = occ['interior']['rows_per_s_step']
    rows_interior = rate_step * win
    interior = {name: scalar(pI, name, fI) for name in CORE_A}
    interior_perrow = {k: v / rows_interior for k, v in interior.items()}
    ipc_I = interior['instructions'] / interior['cycles']

    # ================= uncore / DRAM bandwidth ============================
    #
    # FAIL CLOSED ON AN ABSENT OR INCOMPLETE IMC SET (roborev finding #5, PR
    # #3286). parse_perf returns {} for a file that does not exist, so a missing
    # or emptied perf-uncore.csv used to flow straight through the summing loop
    # below: s_r and s_w stayed 0, n stayed 0, and the rep published
    # total_GB_per_s = 0.0 and per-socket 0.0 — a NUMBER, indistinguishable in
    # derived.json from a host that genuinely moved no DRAM traffic. Combined with
    # the resume predicate that skipped such a rep as "complete", a failed uncore
    # capture could be preserved and then published as 0 GB/s.
    #
    # The check is an AFFIRMATIVE one: the expected instance count is asserted
    # PRESENT, rather than the absence of an error being read as success. On this
    # host that is 12 IMCs x {read,write} x {S0,S1} = 48 rows, and all six
    # committed reps carry exactly 48. IMC_EXPECTED_INSTANCES is derived from the
    # same range(12) the loop scans, so the two cannot drift apart.
    upath = os.path.join(repdir, 'perf-uncore.csv')
    if not os.path.exists(upath):
        raise SystemExit(
            "FATAL: %s absent. The uncore capture did not produce a counter "
            "file, so this rep has no DRAM traffic measurement. Deriving 0 GB/s "
            "from an absent file would publish a number for a capture that "
            "failed; re-run the rep (run/run-all.sh redoes it now that "
            "run/rep-complete.py refuses to skip it)." % upath)
    pU = parse_perf(upath, per_socket=True)
    if not pU:
        raise SystemExit(
            "FATAL: %s carries no readable perf rows (size %d bytes). Same "
            "refusal as an absent file: an empty counter file is not a "
            "measurement of zero traffic."
            % (upath, os.path.getsize(upath)))
    uwin = meta['window_secs']
    dram = {'per_socket': {}, 'note':
            "perf reports cas_count_* already scaled to MiB (the x64 B/cacheline "
            "conversion is APPLIED BY PERF); multiplying by 64 again overcounts 64x."}
    tot_mib = 0.0
    for sock in ('S0', 'S1'):
        s_r = s_w = 0.0
        n = 0
        for i in range(IMC_COUNT):
            for kind, acc in (('read', 'r'), ('write', 'w')):
                rows = ev(pU, 'uncore_imc_%d/cas_count_%s/' % (i, kind), socket=sock)
                for r in rows:
                    if r['value'] is None:
                        continue
                    if r['enabled'] is not None and r['enabled'] < MUX_MIN:
                        raise SystemExit(
                            "FATAL: uncore imc_%d %s on %s only %.2f%% enabled"
                            % (i, kind, sock, r['enabled']))
                    if kind == 'read':
                        s_r += r['value']
                    else:
                        s_w += r['value']
                    n += 1
        # Per-socket completeness, asserted before the socket's total is believed.
        # A partial IMC set understates that socket's traffic, which would show up
        # as a plausible-but-low GB/s figure and — because the far-socket fraction
        # is a RATIO of the two sockets — could shift the NUMA-confinement claim in
        # either direction without ever looking wrong.
        if n != IMC_EXPECTED_INSTANCES:
            raise SystemExit(
                "FATAL: %s reports %d of %d expected uncore_imc instances on %s "
                "(12 IMCs x {read,write}). A partial IMC set cannot be summed "
                "into a socket total: the missing channels' traffic is not zero, "
                "it is unmeasured. Re-run the rep."
                % (upath, n, IMC_EXPECTED_INSTANCES, sock))
        dram['per_socket'][sock] = {
            'cas_read_MiB': s_r, 'cas_write_MiB': s_w,
            'total_MiB': s_r + s_w,
            'GB_per_s': (s_r + s_w) * 1048576 / 1e9 / uwin,
            'instances_counted': n,
        }
        tot_mib += s_r + s_w
    dram['total_MiB'] = tot_mib
    dram['total_GB_per_s'] = tot_mib * 1048576 / 1e9 / uwin
    dram['window_secs'] = uwin
    # A complete IMC set that genuinely sums to zero is not a plausible reading for
    # a running scan, and it is the last shape in which a 0 GB/s figure could still
    # reach derived.json. Refused rather than published with a caveat.
    if tot_mib <= 0:
        raise SystemExit(
            "FATAL: %s: all %d expected uncore_imc instances present on both "
            "sockets, yet total DRAM traffic sums to %.1f MiB. A running scan "
            "cannot move zero bytes; this is a counter/permission failure, not a "
            "measurement of zero." % (upath, IMC_EXPECTED_INSTANCES * 2, tot_mib))
    dram['far_socket_fraction'] = (
        dram['per_socket']['S1']['total_MiB'] / tot_mib)

    return {
        'repdir': repdir,
        'label': meta['label'], 'rep': meta['rep'],
        'S': meta['S_physical_cores'], 'N': meta['N_streams'],
        'server_cpus': meta['server_cpus'], 'client_cpus': meta['client_cpus'],
        'numa_node': meta['numa_node'],
        'window_secs': win,
        'client_utilisation': meta['client_utilisation'],
        'warm_read_bytes_delta': meta['warm_read_bytes_delta'],
        'aligned': {
            'rows_in_window': rowsA,
            'duration_s': occ['alignedA']['duration_s'],
            'rows_per_s': occ['alignedA']['rows_per_s_step'],
            'busy_fraction': occ['alignedA']['busy_fraction_estimate'],
            'whole_scans': occ['alignedA']['whole_scans'],
            'counters': aligned, 'per_row': perrow,
            'ipc': ipc_A, 'ipc_groupB': ipc_B,
            'ipc_group_agreement': abs(ipc_A - ipc_B) / ipc_A,
        },
        'interior': {
            'rows_in_window': rows_interior,
            'rows_per_s_step': rate_step,
            'duration_s': occ['interior']['duration_s'],
            'counters': interior, 'per_row': interior_perrow,
            'ipc': ipc_I,
        },
        'dram': dram,
        'stalls': do_stalls(repdir),
    }


# ------------------------------------------------------------------ aggregate
def agg(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {'min': min(vals), 'median': statistics.median(vals),
            'max': max(vals), 'n': len(vals),
            'spread_pct': ((max(vals) - min(vals)) / statistics.median(vals) * 100)
                          if statistics.median(vals) else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('root')
    ap.add_argument('--out', default=None)
    ap.add_argument('--md', default=None)
    ap.add_argument('--penalty-summary', default=None,
                    help='run/penalty-probe.sh summary.txt — the on-host measured '
                         'latency table used for the MODELLED cross-check. Absent '
                         'means the cross-check is reported unavailable, never '
                         'invented.')
    a = ap.parse_args()

    reps = {}
    for meta_path in sorted(glob.glob(os.path.join(a.root, '*', 'rep*', 'meta.json'))):
        d = os.path.dirname(meta_path)
        r = do_rep(d)
        reps.setdefault(r['label'], []).append(r)

    if not reps:
        raise SystemExit("FATAL: no reps found under %s" % a.root)

    doc = {'schema': 'ws0-3224.derived/v1', 'reps': reps, 'endpoints': {},
           'penalty_table': parse_penalty_summary(a.penalty_summary),
           'penalty_summary_path': a.penalty_summary}
    for label, rs in reps.items():
        e = {'n_reps': len(rs), 'S': rs[0]['S'], 'N': rs[0]['N'],
             'server_cpus': rs[0]['server_cpus'],
             'numa_node': rs[0]['numa_node']}
        if len(rs) < MIN_REPS:
            e['WARNING'] = ("only %d rep(s) — #3217's undispersed reps=1 gap is "
                            "not closed at this endpoint" % len(rs))
        for conv in ('aligned', 'interior'):
            e[conv] = {
                'cycles_per_row': agg([x[conv]['per_row']['cycles'] for x in rs]),
                'instructions_per_row': agg([x[conv]['per_row']['instructions'] for x in rs]),
                'ipc': agg([x[conv]['ipc'] for x in rs]),
                'rows_per_s': agg([x[conv].get('rows_per_s') or x[conv].get('rows_per_s_step') for x in rs]),
            }
            for name in CORE_A + CORE_B:
                if name in ('cycles', 'instructions', 'task-clock'):
                    continue
                series = [x[conv]['per_row'].get(name) for x in rs]
                if any(s is not None for s in series):
                    e[conv].setdefault('events_per_row', {})[name] = agg(series)
        e['dram_GB_per_s'] = agg([x['dram']['total_GB_per_s'] for x in rs])
        e['dram_far_socket_fraction'] = agg(
            [x['dram'].get('far_socket_fraction') for x in rs])
        e['dram_per_socket_GB_per_s'] = {
            s: agg([x['dram']['per_socket'][s]['GB_per_s'] for x in rs])
            for s in ('S0', 'S1')}
        e['ipc_group_agreement_max'] = max(
            x['aligned']['ipc_group_agreement'] for x in rs)
        e['client_utilisation_max'] = max(x['client_utilisation'] for x in rs)

        # ---- group C: the MEASURED stall arm, if this tree has one ----------
        #
        # GROUP C IS ALL-OR-NOTHING, AND NEVER FEWER THAN MIN_REPS (roborev round 2
        # finding #2). `do_stalls` returns None for a rep with no stalls arm, and this
        # block used to aggregate whatever survived — so a tree in which group C had
        # failed for two of three reps produced the HEADLINE measured attribution and
        # residual from ONE rep, silently, with no warning and no visible dispersion.
        #
        # That defeats this script's own stated design rule: ">=3 reps -> min/median/
        # max, because a delta between two undispersed points cannot be defended
        # (#3217 ran reps=1)". #3217's central method gap was undispersed points, and
        # a partial group C reintroduces it in exactly the term the report leads with,
        # while `n_reps` recorded the shrinkage in a field no conclusion reads.
        #
        # Optional-in-full stays supported (the AC4 accounting falls back to the
        # modelled charge and says so), because a tree that never captured group C is
        # a different situation from one whose capture failed. What is refused is the
        # SUBSET: present for some reps and not others is a failed capture, not a
        # design choice.
        st = [x['stalls'] for x in rs if x['stalls']]
        if st and len(st) != len(rs):
            raise SystemExit(
                "FATAL: %s has group-C (stalls) data for %d of %d reps. Group C is "
                "all-or-nothing: a PARTIAL set would derive the headline measured "
                "attribution from a reduced, silently-undispersed sample, which is "
                "#3217's method gap in the one term this report leads with. Either "
                "re-run the missing stalls captures (run/capture-stalls.sh) or "
                "remove the partial ones so the accounting falls back to the "
                "modelled charge explicitly. Reps with group C: %s"
                % (label, len(st), len(rs),
                   sorted(os.path.basename(x['repdir']) for x in st)))
        if st and len(st) < MIN_REPS:
            raise SystemExit(
                "FATAL: %s has group-C data for only %d rep(s); this script's "
                "dispersion rule needs >= %d. A delta between undispersed points "
                "cannot be defended (#3217 ran reps=1), and the AC4 headline is "
                "computed from this arm." % (label, len(st), MIN_REPS))
        if st:
            e['stalls'] = {
                'n_reps': len(st),
                'cycles_per_row': agg([x['per_row']['cycles'] for x in st]),
                'instructions_per_row': agg([x['per_row']['instructions'] for x in st]),
                'ipc': agg([x['ipc'] for x in st]),
                'stalls_l3_miss_per_row': agg(
                    [x['per_row']['cycle_activity.stalls_l3_miss'] for x in st]),
                'stalls_l2_miss_per_row': agg(
                    [x['per_row']['cycle_activity.stalls_l2_miss'] for x in st]),
                'stalls_total_per_row': agg(
                    [x['per_row']['cycle_activity.stalls_total'] for x in st]),
                'stalls_l3_miss_frac_cycles': agg(
                    [x['stalls_l3_miss_frac_cycles'] for x in st]),
                'stalls_l2_miss_frac_cycles': agg(
                    [x['stalls_l2_miss_frac_cycles'] for x in st]),
                'stalls_total_frac_cycles': agg(
                    [x['stalls_total_frac_cycles'] for x in st]),
                'mlp': agg([x['mlp'] for x in st]),
            }
            # Cross-arm consistency: group C ran its OWN loadgen step, so its
            # cycles/row must agree with the primary arm's or the two arms did
            # not observe the same workload (the P2 symmetry idea again).
            pc = e['aligned']['cycles_per_row']['median']
            sc = e['stalls']['cycles_per_row']['median']
            e['stalls']['cycles_per_row_vs_primary_pct'] = (sc - pc) / pc * 100
        doc['endpoints'][label] = e

    # ------------------------------------------------- the AC4 delta + residual
    lo, hi = 'llc-s1-N2', 'llc-s6-N16'
    if lo in doc['endpoints'] and hi in doc['endpoints']:
        doc['delta'] = {}
        for conv in ('aligned', 'interior'):
            a_lo = doc['endpoints'][lo][conv]
            a_hi = doc['endpoints'][hi][conv]
            cl = a_lo['cycles_per_row']['median']
            ch = a_hi['cycles_per_row']['median']
            il = a_lo['instructions_per_row']['median']
            ih = a_hi['instructions_per_row']['median']
            doc['delta'][conv] = {
                'cycles_per_row_low': cl, 'cycles_per_row_high': ch,
                'delta_cycles_per_row': ch - cl,
                'delta_pct': (ch - cl) / cl * 100,
                'instructions_per_row_low': il, 'instructions_per_row_high': ih,
                'instructions_per_row_delta_pct': (ih - il) / il * 100,
                'ipc_low': a_lo['ipc']['median'], 'ipc_high': a_hi['ipc']['median'],
                'ipc_delta_pct': (a_hi['ipc']['median'] - a_lo['ipc']['median'])
                                 / a_lo['ipc']['median'] * 100,
            }
        # Do the two denominator conventions agree? This is the #3224 method
        # question. Reported as a number, either way.
        ca = doc['delta']['aligned']['delta_cycles_per_row']
        ci = doc['delta']['interior']['delta_cycles_per_row']
        doc['denominator_convention_check'] = {
            'aligned_delta_cycles_per_row': ca,
            'interior_delta_cycles_per_row': ci,
            'abs_difference': ci - ca,
            'relative_difference_pct': (ci - ca) / ca * 100 if ca else None,
            'aligned_cycles_per_row_s1N2': doc['delta']['aligned']['cycles_per_row_low'],
            'interior_cycles_per_row_s1N2': doc['delta']['interior']['cycles_per_row_low'],
            's1N2_relative_difference_pct':
                (doc['delta']['interior']['cycles_per_row_low']
                 - doc['delta']['aligned']['cycles_per_row_low'])
                / doc['delta']['aligned']['cycles_per_row_low'] * 100,
            'note': ("#3217 computed cycles/row with counters from a 20 s interior "
                     "window and rows/s from the whole loadgen step. Both "
                     "conventions are computed here on the same host from the same "
                     "reps; if they agree, #3217's baseline convention is sound."),
        }

        # ============================ AC4: the accounting ====================
        # "residual = delta - attributed; publish residual and residual/delta as
        #  a percentage. AC4 explicitly fails if this number is omitted."
        #
        # Two independent routes to `attributed`, reported side by side:
        #
        #   MEASURED  d(cycle_activity.stalls_l3_miss / row). Hardware-counted
        #             execution-stall cycles with an L3 miss outstanding. No
        #             penalty, no MLP assumption, no model.
        #   MODELLED  d(LLC-load-misses / row) x penalty, the #3217-style charge,
        #             with the penalty MEASURED on this host by the serial chase
        #             (DRAM latency - LLC-hit latency) and reported BOTH at zero
        #             MLP (an upper bound on the charge) and divided by the
        #             MEASURED MLP (the physically motivated value).
        #
        # Direction-of-conservatism note, stated because it is easy to get
        # backwards: a LARGER penalty inflates `attributed` and SHRINKS the
        # residual, which flatters the hypothesis that the decay is explained.
        # AC7 and RUNBOOK step 7 forbid rounding toward the hypothesis, so the
        # HEADLINE attribution is the MEASURED stall term, and the modelled
        # zero-MLP figure is reported as the upper bound it is -- never as the
        # attribution.
        conv = 'aligned'
        d = doc['delta'][conv]
        delta = d['delta_cycles_per_row']
        ac4 = {'convention': conv, 'delta_cycles_per_row': delta,
               'delta_pct': d['delta_pct'],
               'instructions_per_row_delta_pct': d['instructions_per_row_delta_pct'],
               'components': {}, 'notes': []}

        e_lo, e_hi = doc['endpoints'][lo], doc['endpoints'][hi]

        # GROUP C IS ALL-OR-NOTHING ACROSS THE TWO HEADLINE ENDPOINTS, not just
        # within each one (round 3 finding #4). The per-endpoint refusal above stops a
        # PARTIAL set of reps at one endpoint; it says nothing about group C being
        # complete at S=1 and absent at S=6. In that case the `and` below is simply
        # False, the code takes the modelled fallback, and a study that captured the
        # measured stall arm for half its experiment publishes as though it had never
        # captured it at all — the expensive half of the data silently discarded with
        # a note nobody reads.
        #
        # An asymmetric capture is a FAILED capture, and it is distinguishable from a
        # deliberate choice not to measure stalls precisely because one endpoint has
        # them. Never-captured stays supported; half-captured does not.
        if ('stalls' in e_lo) != ('stalls' in e_hi):
            have = lo if 'stalls' in e_lo else hi
            lack = hi if 'stalls' in e_lo else lo
            raise SystemExit(
                "FATAL: group C (stalls) is present at %s and ABSENT at %s. The AC4 "
                "headline is a DELTA between the two endpoints, so it needs the "
                "measured stall arm at BOTH or neither: with one side missing the "
                "accounting silently falls back to the modelled charge and discards "
                "the stall data that was captured. Either run "
                "run/capture-stalls.sh for %s, or remove it from %s so the fallback "
                "is an explicit choice." % (have, lack, lack, have))

        # ---- route 1: MEASURED stall cycles --------------------------------
        # DENOMINATOR DISCIPLINE: the stall counters come from group C, which ran
        # its OWN loadgen steps, so their delta is charged against GROUP C's OWN
        # cycles/row delta -- numerator and denominator from ONE arm. Mixing group
        # C's stall numerator with the primary arm's cycles denominator would
        # shift every share by the (small) inter-arm difference, which is exactly
        # the numerator/denominator-from-different-intervals error the ALIGNED
        # convention exists to avoid. The primary arm's delta is reported beside
        # it, and their agreement is itself a cross-arm consistency check.
        if 'stalls' in e_lo and 'stalls' in e_hi:
            delta_primary = delta
            delta = (e_hi['stalls']['cycles_per_row']['median']
                     - e_lo['stalls']['cycles_per_row']['median'])
            ac4['delta_cycles_per_row_primary_arm'] = delta_primary
            ac4['delta_cycles_per_row_groupC_arm'] = delta
            ac4['delta_cycles_per_row'] = delta
            ac4['delta_basis'] = ('group C own arm (stall numerators and the '
                                  'cycles denominator share one interval)')
            ac4['delta_arm_agreement_pct'] = (
                (delta - delta_primary) / delta_primary * 100)
            ac4['delta_pct'] = (
                delta / e_lo['stalls']['cycles_per_row']['median'] * 100)
            s_lo = e_lo['stalls']['stalls_l3_miss_per_row']['median']
            s_hi = e_hi['stalls']['stalls_l3_miss_per_row']['median']
            t_lo = e_lo['stalls']['stalls_total_per_row']['median']
            t_hi = e_hi['stalls']['stalls_total_per_row']['median']
            l2_lo = e_lo['stalls']['stalls_l2_miss_per_row']['median']
            l2_hi = e_hi['stalls']['stalls_l2_miss_per_row']['median']
            ac4['components']['measured_l3_miss_stalls'] = {
                'source': 'cycle_activity.stalls_l3_miss (hardware-counted)',
                'per_row_low': s_lo, 'per_row_high': s_hi,
                'delta_cycles_per_row': s_hi - s_lo,
                'share_of_delta_pct': (s_hi - s_lo) / delta * 100,
            }
            ac4['components']['measured_all_execution_stalls'] = {
                'source': 'cycle_activity.stalls_total (hardware-counted)',
                'per_row_low': t_lo, 'per_row_high': t_hi,
                'delta_cycles_per_row': t_hi - t_lo,
                'share_of_delta_pct': (t_hi - t_lo) / delta * 100,
                'note': ('a SUPERSET of memory stalls; the gap between this and '
                         'the l3_miss term is stall cycles the memory system did '
                         'NOT cause'),
            }
            ac4['components']['measured_l2_miss_stalls'] = {
                'source': 'cycle_activity.stalls_l2_miss (hardware-counted)',
                'per_row_low': l2_lo, 'per_row_high': l2_hi,
                'delta_cycles_per_row': l2_hi - l2_lo,
                'share_of_delta_pct': (l2_hi - l2_lo) / delta * 100,
                'note': ('superset of the l3_miss term: includes L2 misses that '
                         'HIT in the L3, so the difference is L3-hit stall cost'),
            }
            attributed = s_hi - s_lo
            ac4['attributed_cycles_per_row'] = attributed
            ac4['attributed_basis'] = 'measured cycle_activity.stalls_l3_miss delta'
            ac4['residual_cycles_per_row'] = delta - attributed
            ac4['residual_pct_of_delta'] = (delta - attributed) / delta * 100
            ac4['mlp_low'] = e_lo['stalls']['mlp']['median']
            ac4['mlp_high'] = e_hi['stalls']['mlp']['median']

            # ---- the ADDITIVE decomposition ---------------------------------
            # The three stall counters NEST: stalls_l3_miss is a subset of
            # stalls_l2_miss (an L2 miss either hits L3 or misses it), which is a
            # subset of stalls_total. Differencing the nested pairs turns them
            # into DISJOINT buckets, and the leftover -- cycles that were not
            # stalled at all -- closes the identity:
            #
            #   d(cycles/row) = d(L3-miss stalls)          DRAM-served misses
            #                 + d(L2-miss-but-L3-hit stalls)  LLC-served misses
            #                 + d(other stalls)            non-memory stalls
            #                 + d(non-stalled cycles)      may be NEGATIVE
            #
            # This sums to the measured delta BY CONSTRUCTION, so it is not an
            # estimate with a residual -- it is a partition. The AC4 "residual"
            # is then a CHOICE of where to draw the memory boundary, and both
            # choices are published rather than one being quietly preferred.
            d_l3 = s_hi - s_lo
            d_l3hit = (l2_hi - l2_lo) - (s_hi - s_lo)
            d_other = (t_hi - t_lo) - (l2_hi - l2_lo)
            d_unstalled = delta - (t_hi - t_lo)
            parts = {
                'l3_miss_stalls_DRAM': d_l3,
                'l2_miss_l3_hit_stalls_LLC': d_l3hit,
                'other_stalls_non_memory': d_other,
                'non_stalled_cycles': d_unstalled,
            }
            ac4['additive_decomposition'] = {
                'parts_cycles_per_row': parts,
                'parts_pct_of_delta': {k: v / delta * 100 for k, v in parts.items()},
                'sum_cycles_per_row': sum(parts.values()),
                'closure_error_cycles_per_row': sum(parts.values()) - delta,
                'memory_attributed_cycles_per_row': d_l3 + d_l3hit,
                'memory_attributed_pct': (d_l3 + d_l3hit) / delta * 100,
                'non_memory_cycles_per_row': d_other + d_unstalled,
                'non_memory_pct': (d_other + d_unstalled) / delta * 100,
                'note': ('the three cycle_activity stall counters NEST, so the '
                         'differenced buckets are disjoint and sum to the measured '
                         'delta by construction (closure_error is a float-rounding '
                         'check, and must be ~0). Two defensible residuals follow: '
                         'strict DRAM boundary -> residual is everything but '
                         'l3_miss_stalls; whole-cache boundary -> residual is '
                         'other_stalls + non_stalled.'),
            }
        else:
            ac4['notes'].append(
                'group C (stalls) absent: no MEASURED attribution available, so '
                'the modelled charge is all there is and the residual below is '
                'model-dependent')

        # ---- route 2: MODELLED charge, penalty measured on this host -------
        pen = doc.get('penalty_table') or {}
        llc_lo = e_lo[conv]['events_per_row']['LLC-load-misses']['median']
        llc_hi = e_hi[conv]['events_per_row']['LLC-load-misses']['median']
        d_llc = llc_hi - llc_lo
        tlb_lo = e_lo[conv]['events_per_row']['dTLB-load-misses']['median']
        tlb_hi = e_hi[conv]['events_per_row']['dTLB-load-misses']['median']
        if pen.get('LLC_8M') and pen.get('DRAM_256M'):
            llc_hit = pen['LLC_8M']['cycles_per_access']
            dram = pen['DRAM_256M']['cycles_per_access']
            penalty = dram - llc_hit
            mlp = ac4.get('mlp_high')
            ac4['components']['modelled_llc_miss_charge'] = {
                'source': ('serial dependent chase on THIS host: DRAM_256M '
                           '%.2f cyc/access - LLC_8M %.2f cyc/access = %.2f '
                           'cycles per miss' % (dram, llc_hit, penalty)),
                'penalty_cycles_per_miss': penalty,
                'delta_misses_per_row': d_llc,
                'charge_zero_mlp': d_llc * penalty,
                'charge_zero_mlp_share_of_delta_pct': d_llc * penalty / delta * 100,
                'measured_mlp_at_high_point': mlp,
                'charge_mlp_corrected': (d_llc * penalty / mlp) if mlp else None,
                'charge_mlp_corrected_share_of_delta_pct':
                    (d_llc * penalty / mlp / delta * 100) if mlp else None,
                'caveat': ('the DRAM_256M row carries %.2f dTLB misses/access, so '
                           'its latency BUNDLES a page-table walk and the penalty '
                           'is an OVERestimate; dTLB is also charged separately '
                           'below, so the two terms partly double-count'
                           % pen['DRAM_256M']['dtlb_misses_per_access']),
            }
            ac4['components']['modelled_dtlb_charge'] = {
                'delta_misses_per_row': tlb_hi - tlb_lo,
                'note': ('small in absolute terms (%.2f -> %.2f per row); listed '
                         'for completeness. Not added to the headline, because '
                         'the measured stall term already contains any stall a '
                         'walk caused.' % (tlb_lo, tlb_hi)),
            }
        else:
            ac4['notes'].append(
                'penalty table absent (run/penalty-probe.sh output not supplied '
                'via --penalty-summary): modelled cross-check unavailable, and a '
                'penalty with no source is not an attribution')

        # ---- the efficiency cross-check RUNBOOK step 7.5 asks for ----------
        # Throughput comes from the PRIMARY arm, so the cycles/row inflation it is
        # compared against must come from the primary arm too -- not from group C.
        r_lo = e_lo[conv]['rows_per_s']['median']
        r_hi = e_hi[conv]['rows_per_s']['median']
        s_ratio = e_hi['S'] / e_lo['S']
        ac4['marginal_efficiency'] = {
            'basis': 'primary arm throughput vs primary arm cycles/row inflation',
            'rows_per_s_low': r_lo, 'rows_per_s_high': r_hi,
            'throughput_ratio': r_hi / r_lo,
            'core_ratio': s_ratio,
            'measured_efficiency': (r_hi / r_lo) / s_ratio,
            'predicted_from_cycles_per_row':
                1.0 / (1.0 + d['delta_cycles_per_row'] / d['cycles_per_row_low']),
            'note': ('efficiency predicted purely from the cycles/row inflation '
                     'vs the efficiency actually measured from throughput. These '
                     'are the same quantity by two routes; the gap is a check on '
                     'this arithmetic, not a target.'),
        }
        eff = ac4['marginal_efficiency']
        eff['gap_pp'] = (eff['measured_efficiency']
                         - eff['predicted_from_cycles_per_row']) * 100

        # AC4'S OWN CONTRACT, ENFORCED ON AC4 (round 3 finding #5). This file's
        # DESIGN RULES say: "The residual is printed as a NUMBER and as a PERCENTAGE.
        # AC4 fails if omitted." Route 1 populates those three keys; the modelled
        # fallback populated NONE of them, and an `ac4_accounting` object was emitted
        # successfully regardless — accounting with no attribution and no residual,
        # presented in the same shape as accounting that has both.
        #
        # In the fallback the modelled charge IS available (it is computed just
        # above), so a residual can be stated — but it is model-dependent and must
        # not be mistaken for the measured one, so it goes in under DIFFERENT key
        # names with the basis named. Where even that is impossible, AC4 is marked
        # UNAVAILABLE rather than emitted incomplete: a criterion that could not be
        # evaluated has no verdict, and an object that looks like a verdict is worse
        # than an absent one.
        if 'attributed_cycles_per_row' not in ac4:
            mc = ac4['components'].get('modelled_llc_miss_charge')
            charge = (mc or {}).get('charge_mlp_corrected')
            if charge is not None and delta:
                ac4['modelled_attributed_cycles_per_row'] = charge
                ac4['modelled_residual_cycles_per_row'] = delta - charge
                ac4['modelled_residual_pct_of_delta'] = (
                    (delta - charge) / delta * 100)
                ac4['attribution_basis'] = 'MODELLED (group C absent)'
                ac4['notes'].append(
                    'AC4 residual here is MODELLED, not measured: it is '
                    'd(LLC-load-misses)/row x an on-host penalty / measured MLP, '
                    'and it is reported under modelled_* keys so it can never be '
                    'read as the measured stall attribution. The penalty is an '
                    'UPPER bound, which inflates attribution and shrinks this '
                    'residual — the anti-conservative direction.')
            else:
                # WHY THIS IS THE COMMON CASE, and why it is correct rather than a
                # gap: the MLP-corrected charge needs a MEASURED MLP, and MLP comes
                # from l1d_pend_miss.* — which is a GROUP C counter. So group C being
                # absent removes the measured attribution AND the only defensible
                # modelled one in the same stroke.
                #
                # The zero-MLP charge does survive, and it is deliberately NOT used
                # here. It accounted for 140.51% of the delta on the real data (report
                # 5.4): charging the full unloaded latency per miss inflates
                # attribution and shrinks the residual, which is the direction AC7 and
                # RUNBOOK step 7 forbid. Reaching for it to avoid an UNAVAILABLE
                # verdict would be precisely the rounding-toward-the-hypothesis this
                # file's own route-1 comment rules out — so AC4 stays unevaluated.
                ac4['AC4_UNAVAILABLE'] = (
                    'no defensible attribution is available: group C (measured '
                    'stalls) is absent, and MLP is itself a group C counter '
                    '(l1d_pend_miss.*), so the MLP-corrected modelled charge cannot '
                    'be computed either. The zero-MLP charge is NOT substituted: it '
                    'reached 140.51%% of the delta on this data, and an inflated '
                    'penalty shrinks the residual, which is the direction AC7 '
                    'forbids. AC4 requires the residual as a number and a '
                    'percentage, so it is UNEVALUATED rather than partially or '
                    'flatteringly reported. Remedy: run run/capture-stalls.sh.')
                ac4['attribution_basis'] = 'NONE — AC4 unevaluated'
        else:
            ac4['attribution_basis'] = 'MEASURED cycle_activity.stalls_l3_miss'
        doc['ac4_accounting'] = ac4

    out = a.out or os.path.join(a.root, 'derived.json')
    open(out, 'w').write(json.dumps(doc, indent=1, default=str) + '\n')
    print('wrote', out)

    # -------------------------------------------------------------- summary
    for label in sorted(doc['endpoints']):
        e = doc['endpoints'][label]
        print('\n== %s  (S=%d N=%d, %d reps, cpus %s, node %s)'
              % (label, e['S'], e['N'], e['n_reps'], e['server_cpus'], e['numa_node']))
        for conv in ('aligned', 'interior'):
            c = e[conv]
            print('   %-8s cycles/row %12.1f (min %.1f max %.1f, spread %.2f%%)'
                  % (conv, c['cycles_per_row']['median'], c['cycles_per_row']['min'],
                     c['cycles_per_row']['max'], c['cycles_per_row']['spread_pct']))
            print('   %-8s instr/row  %12.1f   IPC %6.4f   rows/s %.0f'
                  % ('', c['instructions_per_row']['median'], c['ipc']['median'],
                     c['rows_per_s']['median']))
        print('   DRAM %.2f GB/s (far-socket %.4f)  |  IPC group agreement %.5f  |  client util %.4f'
              % (e['dram_GB_per_s']['median'],
                 (e['dram_far_socket_fraction'] or {}).get('median', -1),
                 e['ipc_group_agreement_max'], e['client_utilisation_max']))
    if 'delta' in doc:
        for conv in ('aligned', 'interior'):
            d = doc['delta'][conv]
            print('\n== DELTA (%s): cycles/row %.1f -> %.1f = %+.1f (%+.2f%%) | '
                  'instr/row %+.3f%% | IPC %.4f -> %.4f (%+.2f%%)'
                  % (conv, d['cycles_per_row_low'], d['cycles_per_row_high'],
                     d['delta_cycles_per_row'], d['delta_pct'],
                     d['instructions_per_row_delta_pct'],
                     d['ipc_low'], d['ipc_high'], d['ipc_delta_pct']))
        k = doc['denominator_convention_check']
        print('\n== DENOMINATOR CONVENTION CHECK')
        print('   S=1/N=2 cycles/row: aligned %.1f vs interior %.1f  -> %+.2f%%'
              % (k['aligned_cycles_per_row_s1N2'], k['interior_cycles_per_row_s1N2'],
                 k['s1N2_relative_difference_pct']))
        print('   delta cycles/row:   aligned %.1f vs interior %.1f  -> %+.2f%%'
              % (k['aligned_delta_cycles_per_row'], k['interior_delta_cycles_per_row'],
                 k['relative_difference_pct']))

    for label in sorted(doc['endpoints']):
        st = doc['endpoints'][label].get('stalls')
        if not st:
            continue
        print('\n== %s GROUP C (measured stalls, %d reps)' % (label, st['n_reps']))
        print('   cycles/row %10.1f  (vs primary arm %+.2f%%)   IPC %.4f'
              % (st['cycles_per_row']['median'],
                 st['cycles_per_row_vs_primary_pct'], st['ipc']['median']))
        for key, nm in (('stalls_l3_miss', 'stalls_l3_miss'),
                        ('stalls_l2_miss', 'stalls_l2_miss'),
                        ('stalls_total', 'stalls_total')):
            print('   %-16s/row %9.1f   = %5.2f%% of cycles'
                  % (nm, st[key + '_per_row']['median'],
                     st[key + '_frac_cycles']['median'] * 100))
        print('   MLP (measured)  %.3f' % st['mlp']['median'])

    if 'ac4_accounting' in doc:
        ac4 = doc['ac4_accounting']
        print('\n' + '=' * 74)
        print('== AC4 CYCLES-PER-ROW ACCOUNTING  (convention: %s)' % ac4['convention'])
        print('=' * 74)
        print('   delta cycles/row        %+10.1f  (%+.2f%% of the low point)'
              % (ac4['delta_cycles_per_row'], ac4['delta_pct']))
        if 'delta_cycles_per_row_primary_arm' in ac4:
            print('     basis: %s' % ac4['delta_basis'])
            print('     primary arm delta %+.1f -> the two arms agree to %+.2f%%'
                  % (ac4['delta_cycles_per_row_primary_arm'],
                     ac4['delta_arm_agreement_pct']))
        print('   instructions/row        %+10.2f%%   <- flat means SAME WORK'
              % ac4['instructions_per_row_delta_pct'])
        print('   --- components ---')
        for name, c in ac4['components'].items():
            if 'delta_cycles_per_row' in c:
                print('   %-34s %+10.1f  = %6.2f%% of delta'
                      % (name, c['delta_cycles_per_row'], c['share_of_delta_pct']))
            elif 'charge_zero_mlp' in c:
                print('   %-34s penalty %.1f cyc/miss x %.2f misses/row'
                      % (name, c['penalty_cycles_per_miss'],
                         c['delta_misses_per_row']))
                print('   %-34s   zero-MLP    %+10.1f  = %6.2f%% of delta (UPPER BOUND)'
                      % ('', c['charge_zero_mlp'],
                         c['charge_zero_mlp_share_of_delta_pct']))
                if c.get('charge_mlp_corrected'):
                    print('   %-34s   MLP %.2f    %+10.1f  = %6.2f%% of delta'
                          % ('', c['measured_mlp_at_high_point'],
                             c['charge_mlp_corrected'],
                             c['charge_mlp_corrected_share_of_delta_pct']))
        if 'additive_decomposition' in ac4:
            ad = ac4['additive_decomposition']
            print('   --- ADDITIVE decomposition (disjoint buckets, sums by construction) ---')
            for k, v in ad['parts_cycles_per_row'].items():
                print('   %-34s %+10.1f  = %6.2f%% of delta'
                      % (k, v, ad['parts_pct_of_delta'][k]))
            print('   %-34s %+10.1f  (closure error %+.3f — must be ~0)'
                  % ('SUM', ad['sum_cycles_per_row'],
                     ad['closure_error_cycles_per_row']))
            print('   memory-attributed (L3 miss + L3 hit) %+8.1f = %6.2f%%'
                  % (ad['memory_attributed_cycles_per_row'], ad['memory_attributed_pct']))
            print('   non-memory                           %+8.1f = %6.2f%%'
                  % (ad['non_memory_cycles_per_row'], ad['non_memory_pct']))
        if 'attributed_cycles_per_row' in ac4:
            print('   --- verdict ---')
            print('   ATTRIBUTED   %+10.1f cycles/row   (%s)'
                  % (ac4['attributed_cycles_per_row'], ac4['attributed_basis']))
            print('   RESIDUAL     %+10.1f cycles/row   = %.2f%% of delta UNATTRIBUTED'
                  % (ac4['residual_cycles_per_row'], ac4['residual_pct_of_delta']))
        eff = ac4['marginal_efficiency']
        print('   --- efficiency cross-check ---')
        print('   throughput %.0f -> %.0f rows/s over %gx cores = %.4f measured efficiency'
              % (eff['rows_per_s_low'], eff['rows_per_s_high'], eff['core_ratio'],
                 eff['measured_efficiency']))
        print('   predicted from cycles/row inflation alone       = %.4f  (gap %+.2f pp)'
              % (eff['predicted_from_cycles_per_row'], eff['gap_pp']))
        for n in ac4['notes']:
            print('   NOTE: %s' % n)


if __name__ == '__main__':
    main()
