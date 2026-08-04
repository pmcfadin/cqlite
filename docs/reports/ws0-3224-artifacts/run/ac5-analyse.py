#!/usr/bin/env python3
"""#3224 AC5 — the triad's IMC cross-check and byte accounting, fail-closed.

    python3 ac5-analyse.py <perf-uncore-triad.csv> <stream.txt>

Exit 0 = the byte accounting RESOLVED the channels-vs-duplicates question and the
         per-instance sum is the right basis for a bandwidth figure.
Exit 1 = unresolved. No bandwidth figure may be published.

THE DEFECT THIS EXISTS TO CATCH (roborev finding #6, PR #3286)
--------------------------------------------------------------
This analysis used to be a heredoc inside ac5-peak.sh, and BOTH of its failure
paths printed a warning and then exited 0:

  * the INDETERMINATE branch (`ratio` matching neither ~1x nor ~8x) printed "Do
    NOT publish a bandwidth figure until this is resolved" and returned success;
  * the `except (KeyError, ValueError)` branch printed "byte accounting
    UNAVAILABLE ... no bandwidth figure may be published" and returned success.

So a script whose sole job is to decide whether a GB/s figure is 1x or 8x too
high could fail to decide it, say so in prose, and exit 0 — while its caller had
already printed a nonzero perf return code without stopping either. A message that
tells the operator not to publish, delivered by a process that reports success, is
advice; the exit code is the only part a pipeline reads.

WHY IT MATTERS HERE SPECIFICALLY. The whole AC5 claim rests on this ratio. perf
exposes uncore_imc_0..11, and per socket EIGHT report a near-identical non-zero
value while FOUR read exactly 0.0. `sum/max = 7.996` is consistent with BOTH
"8 populated channels, sum them" and "8 duplicate reports of one aggregate, do
not" — and every GB/s figure in the report differs by 8x between them. Byte
accounting is the only thing that separates them, so an unresolved verdict is not
a caveat to note, it is the absence of the measurement.

Extracted to its own file so selftest-guards.sh can drive it with a crafted
indeterminate input and assert the nonzero exit.
"""
import sys
if len(sys.argv) != 3:
    sys.exit('usage: ac5-analyse.py <perf-uncore-triad.csv> <stream.txt>')
csv, stxt = sys.argv[1], sys.argv[2]

# THE EXPECTED IMC ROSTER, asserted PRESENT rather than inferred from whatever
# parsed (roborev round 2 finding #3). The byte-accounting ratio has a broad
# acceptance band (0.6-1.6), so a TRUNCATED capture can land inside it and exit
# successfully — and the four near-zero S1 rows can go missing without moving the
# ratio at all, since they contribute almost nothing to the sum. So the ratio cannot
# police its own inputs, and "it parsed and the number looked right" is exactly the
# plausible-output-from-a-broken-instrument failure this harness exists to refuse.
#
# Same constants and same reasoning as results/derive.py's IMC_COUNT /
# IMC_EXPECTED_INSTANCES: uncore_imc_0..11 (host/sysfs-pmus.txt is authoritative),
# each read for cas_count_read and cas_count_write, on both sockets.
# The roster comes from harness/ws0schema.py, not from a local copy. It was a local
# copy, and that is how the same finding kept arriving in a new file each round.
import os
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'harness'))
import ws0schema
IMC_COUNT = ws0schema.IMC_COUNT
EXPECTED = set(ws0schema.IMC_EVENTS)
SOCKETS = ws0schema.SOCKETS

per = {s: {} for s in SOCKETS}
elapsed = None
for line in open(csv):
    line = line.strip()
    if not line or line.startswith('#'):
        continue
    f = line.split(',')
    if len(f) < 7:
        continue
    sock, val, unit, ev, enabled = f[0], f[2], f[3], f[4], f[6]
    try:
        v = float(val)
    except ValueError:
        # A `<not counted>`/`<not supported>` row is a MISSING instance, not an
        # absent one: skipping it here leaves the roster check below to catch it,
        # which is where the diagnosis belongs.
        continue
    try:
        e = float(enabled)
    except ValueError:
        sys.exit("FATAL: %s %s has an unreadable enabled%% (%r). An unverifiable "
                 "count is not a usable one — reading the percentage is the only "
                 "evidence the count is not a multiplexed estimate."
                 % (sock, ev, enabled))
    if e < ws0schema.MUX_MIN:
        sys.exit("FATAL: %s %s only %s%% enabled" % (sock, ev, enabled))
    if sock in per and 'cas_count' in ev:
        # A duplicate row would be SILENTLY OVERWRITTEN by dict assignment, hiding
        # a double-counted or malformed capture.
        if ev in per[sock]:
            sys.exit("FATAL: %s carries a DUPLICATE row for %s on %s. A repeated "
                     "instance means the capture is malformed; summing it would "
                     "double-count that channel." % (csv, ev, sock))
        if unit != 'MiB':
            sys.exit("FATAL: %s %s reports unit %r, expected 'MiB'. perf applies "
                     "the x64 B/cacheline conversion itself, so a different unit "
                     "means every byte figure below is wrong by that factor."
                     % (sock, ev, unit))
        per[sock][ev] = v         # already MiB; do NOT multiply by 64 again
    if elapsed is None:
        try:
            elapsed = float(f[5]) / 1e9
        except (ValueError, IndexError):
            pass

# The roster assertion itself. Both sockets, complete, before any arithmetic.
for sock in SOCKETS:
    missing = EXPECTED - set(per[sock])
    unexpected = set(per[sock]) - EXPECTED
    if missing:
        sys.exit("FATAL: %s reports %d of %d expected uncore_imc instances on %s; "
                 "missing %s. A truncated capture can still land inside the "
                 "0.6-1.6 ratio band — the four near-zero S1 rows barely move the "
                 "sum — so the ratio cannot detect its own missing inputs. Re-run "
                 "the triad."
                 % (csv, len(per[sock]), len(EXPECTED), sock,
                    sorted(missing)[:6] + (['...'] if len(missing) > 6 else [])))
    if unexpected:
        sys.exit("FATAL: %s carries unexpected cas_count instances on %s: %s. The "
                 "roster is pinned to uncore_imc_0..%d; an instance outside it "
                 "means this host's IMC topology differs from the one the byte "
                 "accounting was derived for."
                 % (csv, sock, sorted(unexpected), IMC_COUNT - 1))

st = {}
for line in open(stxt):
    if '=' in line:
        k, _, v = line.strip().partition('=')
        st[k] = v

# THE STREAM RECORD IS VALIDATED HERE, INDEPENDENTLY OF ITS PRODUCER (round 6
# finding #3). cache-hostile now refuses --iters <= 0 at the boundary, and this
# analyser was left trusting that — so a stream.txt from an OLDER binary, or a
# hand-edited or truncated one, carrying iters=0 would still be certified: with
# iters=0 the `expected` total collapses to the init pass alone, the IMC counters
# (which did capture the init traffic) divide by it, and the ratio can land inside
# the 0.6-1.6 band. RESOLVED, from a run that measured no bandwidth iteration.
#
# Producer-side validation is not consumer-side validation. The artefact is the
# interface, and this is the only place that sees BOTH the counters and the record
# they are divided by — so it must establish for itself that the record describes a
# real measurement. Same reasoning as derive.py re-checking every gate rather than
# trusting meta.json's `ok`.
_sproblems = []
if st.get('mode') != 'stream':
    _sproblems.append('mode=%r (expected "stream") — this is not a triad record'
                      % st.get('mode'))
if st.get('init_overrun') not in ('0', None):
    _sproblems.append('init_overrun=%r: initialisation overran the measurement '
                      'window, so the counted interval includes it'
                      % st.get('init_overrun'))
for _k, _positive in (('elements', True), ('iters', True), ('best_iter_s', True),
                      ('init_s', False)):
    _raw = st.get(_k)
    if _raw is None:
        _sproblems.append('%s absent from the stream record' % _k)
        continue
    try:
        _v = float(_raw)
    except ValueError:
        _sproblems.append('%s=%r is not a number' % (_k, _raw))
        continue
    # Affirmatively in range, not merely "not obviously bad": float() accepts
    # 'nan' and 'inf', and `nan > 0` is False, so a NaN would fail a positivity
    # test by accident and pass a "not negative" one. Checked explicitly.
    import math as _math
    if not _math.isfinite(_v):
        _sproblems.append('%s=%r is non-finite' % (_k, _raw))
    elif _positive and _v <= 0:
        _sproblems.append('%s=%r must be > 0 (with zero iterations the timed loop '
                          'never ran, and `expected` would collapse to the init '
                          'pass the counters did capture)' % (_k, _raw))
    elif not _positive and _v < 0:
        _sproblems.append('%s=%r must be >= 0' % (_k, _raw))
if _sproblems:
    sys.exit('==== AC5 BYTE ACCOUNTING: UNRESOLVED (invalid stream record) ====\n'
             'The triad record at %s does not describe a completed measurement, so '
             'no byte accounting can be derived from it:\n  - %s\n'
             'Re-run run/ac5-peak.sh. Note cache-hostile refuses these at the '
             'boundary now, so such a record comes from an older binary or has been '
             'edited.' % (stxt, '\n  - '.join(_sproblems)))

tot_mib = sum(sum(d.values()) for d in per.values())
# The verdict this script exists to produce. It stays None until the byte
# accounting AFFIRMATIVELY resolves the channels-vs-duplicates question, so every
# path that fails to resolve it — an unparseable stream.txt, a missing key, a ratio
# matching neither hypothesis, an empty CSV — leaves it None and exits non-zero.
# Keyed on reaching a positive answer, never on the absence of a bad one.
resolved = None
print()
print("-- IMC cross-check of the triad's own DRAM traffic --")
print("   window (perf run_time)  : %.3f s" % (elapsed or float('nan')))
for sock in ('S0', 'S1'):
    vals = list(per[sock].values())
    nz = [v for v in vals if v > 0]
    print("   %s: %d instances, %d non-zero, sum %.1f MiB (per-instance min %.1f "
          "max %.1f)" % (sock, len(vals), len(nz), sum(vals),
                         min(nz) if nz else 0.0, max(nz) if nz else 0.0))
if tot_mib:
    print("   far-socket fraction     : %.4f  (membind=node0 should keep this near 0)"
          % (sum(per['S1'].values()) / tot_mib))

try:
    n = float(st['elements']); iters = float(st['iters'])
    best = float(st['best_iter_s']); init = float(st['init_s'])
    # Steady-state traffic: `iters` passes at 32 B/element (2 reads + the written
    # line's read-for-ownership + the writeback). The init pass writes all three
    # arrays once, so charge it at 3 arrays x 8 B x n, x2 for RFO + writeback.
    steady = 32.0 * n * iters
    init_bytes = 2.0 * 3.0 * 8.0 * n
    expected = steady + init_bytes
    measured = tot_mib * 1048576.0
    ratio = measured / expected if expected else float('nan')
    print()
    print("   -- byte accounting: is the per-instance sum right? --")
    print("   elements %.0f, iters %.0f" % (n, iters))
    print("   expected steady traffic  : %.1f GB (32 B/elem x elements x iters)"
          % (steady / 1e9))
    print("   expected init traffic    : %.1f GB (3 arrays written once, RFO+WB)"
          % (init_bytes / 1e9))
    print("   expected TOTAL           : %.1f GB" % (expected / 1e9))
    print("   IMC measured TOTAL       : %.1f GB" % (measured / 1e9))
    print("   ratio measured/expected  : %.3f" % ratio)
    if 0.6 <= ratio <= 1.6:
        print("   VERDICT: ~1x  -> the 8 non-zero instances per socket are DISTINCT")
        print("            CHANNELS and summing them is CORRECT.")
        resolved = 'channels'
    elif 6.0 <= ratio <= 10.0:
        # RESOLVED, and resolved AGAINST the derivation as written. The question is
        # answered, so this is not an indeterminate outcome — but every summed GB/s
        # figure is then 8x high, so it must still stop the run rather than let a
        # caller proceed on a printed warning.
        print("   VERDICT: ~8x  -> the instances are DUPLICATE reports of one")
        print("            socket aggregate. EVERY GB/s figure derived by summing")
        print("            them is 8x too high and MUST be divided by the non-zero")
        print("            instance count. Fix the derivation before publishing.")
        resolved = 'duplicates'
    else:
        print("   VERDICT: INDETERMINATE (ratio %.3f matches neither ~1x nor ~8x)."
              % ratio)
        print("            Do NOT publish a bandwidth figure until this is resolved.")
    print()
    print("   rates, for information only (NOT the verdict): the triad's own GB/s")
    print("   comes from its BEST iteration (%.6f s) while the IMC counters cover"
          % best)
    print("   the WHOLE %.3f s window including a %.3f s single-threaded init, so"
          % (elapsed or float('nan'), init))
    print("   the IMC window-average rate is necessarily the lower of the two.")
    if elapsed:
        print("   IMC window-average      : %.2f GB/s" % (measured / 1e9 / elapsed))
    print("   triad best-iteration     : basis24 %s GB/s | basis32 %s GB/s"
          % (st.get('gbps_basis24', '?'), st.get('gbps_basis32', '?')))
    print("   steady-state IMC equivalent at the best iteration rate:")
    print("                            : %.2f GB/s" % (32.0 * n / best / 1e9))
except (KeyError, ValueError) as exc:
    print("   byte accounting UNAVAILABLE (%s) — the channels-vs-duplicates"
          % exc)
    print("   question is then UNRESOLVED and no bandwidth figure may be published.")

print()
print("   NOTE: this is a STREAM-TRIAD-CLASS reference, not the vendor STREAM")
print("   benchmark. Quote which byte basis (24 B/elem architectural, or 32 B/elem")
print("   including read-for-ownership) any published figure uses.")

# ------------------------------------------------------------------ the verdict
# The exit code IS the verdict. Everything above is the working; a caller reads
# this. The two non-'channels' outcomes are distinguished in the message because
# their remedies differ — 'duplicates' means fix the derivation, None means
# re-measure — but both are failures, and neither may be walked past.
print()
if resolved == 'channels':
    print("==== AC5 BYTE ACCOUNTING: RESOLVED (channels) ====")
    print("The per-instance sum is the correct basis; a bandwidth figure derived")
    print("by summing the non-zero instances may be published.")
    sys.exit(0)
if resolved == 'duplicates':
    sys.exit("==== AC5 BYTE ACCOUNTING: RESOLVED AGAINST THE DERIVATION "
             "(duplicates) ====\nThe instances are duplicate reports of one "
             "socket aggregate, so every summed GB/s figure is 8x too high. Fix "
             "the derivation to divide by the non-zero instance count, then "
             "re-run. Publishing nothing from this run.")
sys.exit("==== AC5 BYTE ACCOUNTING: UNRESOLVED ====\nThe channels-vs-duplicates "
         "question was NOT answered, so the 8x ambiguity stands and NO bandwidth "
         "figure may be published from this run. sum/max ~ 7.996 is consistent "
         "with both hypotheses, so it cannot substitute. Re-run the triad with a "
         "readable stream.txt and a complete uncore CSV.")
