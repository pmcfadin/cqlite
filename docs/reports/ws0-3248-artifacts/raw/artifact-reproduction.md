# Are the committed results.json artifacts re-derivable? Measured, per artifact

Written because roborev job 70 finding 2 found the committed generated artifacts **predate the final
report schema** — they carry no `profile` and no `quiescence`, so the profiled and unprofiled AC1
results are *machine-indistinguishable* despite measured profiler overhead between them, and the AC0
verdict lacks fields the final reporter requires. The finding is correct on every particular. What
follows is what I measured about it, including the part that does not resolve the way the finding
implies.

## 1. The current code CANNOT read these sessions, and that is the guards getting stricter

Running the committed reporter against all four retained session directories:

```
$ python3 scripts/perf/ws0_report.py --dir /data/ws0-3248/<session>/rig --corpus /data/ws0-3096 --skip-corpus-digest
FATAL: .../session-corpus-pin.json `config` is INCOMPLETE — no quiescence, profile. Every field of
the configuration is one the report makes a claim about, so a partial manifest cannot establish what
this session measured. Re-run the session with the current driver.
```

All four sessions, identical refusal. The retained pre-measurement manifests stamp:

```
arms, baseline_mode, bin_dir, client_cpus, events, flight_endpoint, reps, scan_passes,
server_cpus, step_duration, temps
```

`profile` and `quiescence` are absent because they **did not exist when these sessions ran** — they
were added by later rounds of this same delivery, in response to earlier findings. So the refusal is
not data corruption; it is a **later, stricter reader correctly declining to guess a configuration
that was never stamped.**

## 2. The fix I did NOT apply, and why

The obvious way to make the artifacts re-derivable is to add `"profile": …, "quiescence": …` to the
four retained manifests. **I did not do that, and it must not be done.**

`session-corpus-pin.json` is stamped *before the first rep* precisely so that a re-report cannot
substitute a different configuration and claim it was verified. Writing new fields into it after the
measurement would be **fabricating a pre-measurement record** — inventing the provenance the reader
exists to check, in the one file whose whole value is that it predates the data. It would make the
artifacts pass, which is the problem: the check would then be satisfied by my typing rather than by
the session. The reader's advice (`Re-run the session with the current driver`) is the honest remedy,
and re-running is a re-measurement, not a re-report.

## 3. What IS demonstrated: every committed figure re-derives IDENTICALLY at its producing revision

Reproducibility is a claim about a *code state*. Naming that state makes it checkable. Each artifact
was re-derived from its retained session by checking the rig out at the commit that produced it:

```bash
git archive <commit> scripts/perf | tar -x -C /tmp/rd
cd /tmp/rd && python3 scripts/perf/ws0_report.py \
    --dir /data/ws0-3248/<session>/rig --corpus /data/ws0-3096 --skip-corpus-digest
```

| committed artifact | retained session | rig commit | re-run | medians vs committed |
|---|---|---|---|---|
| `ac0/results.json` | `ac0-20260828T161527Z` | `aa8268cb2` | rc=0 | **identical** |
| `ac1/results-profiled.json` | `ac1b-20260828T164706Z` | `d0c6e3baf` | rc=0 | **identical** |
| `ac1/results-perfsym-noprofiler.json` | `ctl-perfsym-noprof-165622Z` | `d0c6e3baf` | rc=0 | **identical** |
| `bytes-touched/results.json` | `bytes-touched-170756Z` | `6782d5cd9` | rc=0 | **identical** |

"Identical" = every `rows_per_sec`, `cycles_per_row` and `ipc` **median**, compared per
(arm, temperature) with the arm/temperature sets required to match. No figure in the report moved.

## 4. The profiled/unprofiled distinction, recovered from evidence rather than asserted

The finding's sharpest point is that the two AC1 artifacts cannot be told apart by machine. True of
the artifacts — but the retained sessions carry the discriminator, and it needs no manifest edit:

| session | committed as | `profiles/*.data` on disk | manifest `bin_dir` |
|---|---|---|---|
| `ac1b-20260828T164706Z` | `results-profiled.json` | **6** | `target/perfsym` |
| `ctl-perfsym-noprof-165622Z` | `results-perfsym-noprofiler.json` | **0** | `target/perfsym` |
| `ac0-20260828T161527Z` | `ac0/results.json` | 0 | `target/release` |
| `bytes-touched-170756Z` | `bytes-touched/results.json` | 0 | `target/release` |

The control's value is visible here: **same `bin_dir`, same symbol-bearing binaries, zero profiles** —
which is what makes it a profiler-overhead control rather than a second measurement. A reader can
check the profiled/unprofiled claim by counting files, against evidence written at measurement time.

## 5. Residual, stated rather than closed

The artifacts remain **not re-derivable with HEAD's rig**, and no edit short of re-measuring changes
that. What is established is narrower and, I think, the honest version of the claim:

* every published figure re-derives **bit-identically** from retained raw records, at a **named
  commit**, by a **stated command** — verified, not asserted;
* the reason HEAD refuses them is **recorded and benign** (a stricter reader, not bad data);
* the one distinction the artifacts genuinely could not express — profiled vs not — is recoverable
  from measurement-time evidence.

Re-measuring all four sessions under the current driver would close the residual completely and is
the correct long-run answer. It needs a fresh quiescence window and it would move every published
number, so it is a **coordination decision, not a fix I should make unilaterally** — raised to the
lead rather than actioned here.
