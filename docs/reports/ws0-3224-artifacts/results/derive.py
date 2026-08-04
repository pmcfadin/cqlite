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

CORPUS_DEFAULT = 3999890
MUX_MIN = 99.0            # enabled% floor; below this the count is an estimate


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


def do_rep(repdir):
    meta = json.load(open(os.path.join(repdir, 'meta.json')))
    rows_corpus = meta.get('corpus_rows', CORPUS_DEFAULT)
    occ = meta['occupancy']

    # ---- validity gates, re-checked here rather than trusted from meta ----
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
    pU = parse_perf(os.path.join(repdir, 'perf-uncore.csv'), per_socket=True)
    uwin = meta['window_secs']
    dram = {'per_socket': {}, 'note':
            "perf reports cas_count_* already scaled to MiB (the x64 B/cacheline "
            "conversion is APPLIED BY PERF); multiplying by 64 again overcounts 64x."}
    tot_mib = 0.0
    for sock in ('S0', 'S1'):
        s_r = s_w = 0.0
        n = 0
        for i in range(12):
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
    if tot_mib > 0:
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
    a = ap.parse_args()

    reps = {}
    for meta_path in sorted(glob.glob(os.path.join(a.root, '*', 'rep*', 'meta.json'))):
        d = os.path.dirname(meta_path)
        r = do_rep(d)
        reps.setdefault(r['label'], []).append(r)

    if not reps:
        raise SystemExit("FATAL: no reps found under %s" % a.root)

    doc = {'schema': 'ws0-3224.derived/v1', 'reps': reps, 'endpoints': {}}
    for label, rs in reps.items():
        e = {'n_reps': len(rs), 'S': rs[0]['S'], 'N': rs[0]['N'],
             'server_cpus': rs[0]['server_cpus'],
             'numa_node': rs[0]['numa_node']}
        if len(rs) < 3:
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


if __name__ == '__main__':
    main()
