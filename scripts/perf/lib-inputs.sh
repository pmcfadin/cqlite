#!/usr/bin/env bash
# lib-inputs.sh — THE MEASUREMENT INPUTS A SESSION VERIFIES AND DERIVES BEFORE IT PINS
# (issue #3272 review round 13, F2).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library mutates the
# SOURCING shell's options, which is the caller's decision (the rule `lib-cpu.sh`, `lib-args.sh`,
# `lib-host-state.sh`, `lib-perf-lint.sh`, `lib-server.sh`, `lib-outdir.sh`, `lib-measure.sh` and
# `lib-binaries.sh` follow — `lib-cpu.sh` had to be corrected for exactly that). The driver sets all
# three itself.
#
# # Why this is a library, and why THIS seam
#
# Split out under the campsite rule: F2's session-owned ticket path took `ws0-baseline.sh` to 959
# lines, 159 over the ~800 source target and moving the wrong way. Note the gate's `file-size`
# ratchet is `.rs`-ONLY, so a shell file crosses the threshold SILENTLY; this is checked with
# `wc -l`.
#
# The seam is a RESPONSIBILITY, not a line count. Every rig library owns one question about whether a
# measurement means what it says; this one owns the two INPUTS that are neither the corpus nor the
# programs — the SCHEMA the bytes are read with, and the REQUEST that is asked of them:
#
#     lib-cpu.sh          are the pinned CPUs one physical core?
#     lib-host-state.sh   is the host's state put back?
#     lib-args.sh         are the arguments values this rig can measure?
#     lib-perf-lint.sh    is the counting domain CPU-wide?
#     lib-outdir.sh       do the artifacts being read all come from ONE session?
#     lib-measure.sh      how is ONE rep of an arm executed, prewarmed and counted?
#     lib-binaries.sh     WHICH PROGRAMS are being measured, and are they this revision's?
#     lib-inputs.sh       WHICH SCHEMA are the bytes read with, and WHICH REQUEST is asked?
#
# What deliberately STAYS in the driver: the ORDER of operations (arguments before creation,
# verification before measurement, binaries before the pin, THE SCHEMA BEFORE THE TICKET DERIVED
# FROM IT, the ticket before the pin, the pin before the first rep), the round loop, and
# `perf_stat_c`. Two suites assert that order by LINE NUMBER against the driver, so the call sites
# below must remain in it.
#
# # WHY `perf_stat_c` IS NOT IN HERE — the same load-bearing reason it is not in `lib-measure.sh`
#
# `perf_invocation_lint_tree` DISCOVERS which file owns the single perf wrapper and lints EXACTLY ONE
# file in `owner` mode and every other `scripts/perf/*.sh` in `library` mode, where DEFINING
# `perf_stat_c` is itself a FINDING ("the rig has exactly ONE"). Moving the wrapper into a library
# would flip the owner and invert layer 1 of the three-layer perf guard. This library neither defines
# nor calls it: it runs before the first perf window exists.
#
# # WHAT THIS LIBRARY READS FROM THE DRIVER, stated because it is a real coupling
#
# `verify_corpus_schema_input` reads `$CORPUS`; `write_ticket_template_for_session` reads `$OUT_DIR`, `$CORPUS`
# and SETS `$TICKET_TEMPLATE` (the path `lib-measure.sh` passes to every `flight-loadgen
# --ticket-template`). That is the same coupling the code had as driver-local statements, recorded
# rather than hidden — and `$TICKET_TEMPLATE` is assigned HERE, beside the write that creates the
# file, for the reason `record_measured_binaries` reassigns `$BIN` beside the freeze: a driver-side
# assignment could drift from the path the python actually wrote. Under the driver's `set -u` an
# unset global is a fatal error rather than an empty expansion, so a caller that sourced this and
# skipped the setup fails loudly instead of measuring nothing.
#
# # FAILURE PROPAGATION, stated because a split is where it gets lost
#
# Both functions `return 2` on failure and NEITHER runs in a command substitution at its call site,
# so `|| exit 2` in the driver is what terminates the run — the `exit`-inside-a-subshell trap that
# bit `lib-outdir.sh` on this branch cannot apply here. No error path became permissive: every
# refusal that was a `{ echo …; exit 2; }` is now a `{ echo …; return 2; }` under the driver's
# `|| exit`, and the diagnostics are unchanged.

# THIS LIBRARY'S OWN DIRECTORY, resolved from `BASH_SOURCE` at source time — the pattern
# `lib-measure.sh` and `lib-binaries.sh` established. NOT the driver's `$HERE`: that would be an
# undocumented coupling, and under `set -u` a caller that skipped it would die mid-run rather than at
# source time.
WS0_INPUTS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# verify_corpus_schema_input — the DDL is a MEASUREMENT INPUT, so it is VERIFIED (#3272 round 6, R2).
#
# `-r` used to be the whole of it. The DDL was outside every verification this rig performs: absent
# from the Data.db digest check, absent from the component check (it is not in the table directory),
# absent from the session pin.
#
# THE TWO ARMS READ IT ASYMMETRICALLY, which is what makes a modification both silent and harmful:
# the TICKET TEMPLATE is generated from it ONCE, at setup — while the BARE SCAN ingests the file on
# EVERY invocation. So editing it between setup and a later rep makes the two arms measure DIFFERENT
# SCHEMAS (a different column set, clustering order or type) while every recorded identity still
# agrees and the report exits 0. A head-to-head number between two arms reading two schemas compares
# nothing.
#
# Called BEFORE the ticket and before the pin, so a mismatch costs seconds rather than a full
# measurement run. There is no skip flag: the file is a few hundred bytes.
verify_corpus_schema_input() {
  local ddl="$CORPUS/ws0-events.cql"
  [[ -r "$ddl" ]] || { echo "FATAL: $ddl missing — regenerate the corpus" >&2; return 2; }
  if ! python3 - "$WS0_INPUTS_LIB_DIR" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid, load_corpus_identity
from ws0_schema_input import verify_schema_input
corpus = pathlib.Path(sys.argv[2])
try:
    rec = verify_schema_input(corpus, load_corpus_identity(corpus))
except Invalid as exc:
    print(f"FATAL: {exc}", file=sys.stderr)
    raise SystemExit(1)
print(f"schema pin:   {rec['schema_sha256_measured']} ({rec['schema_bytes']} B) — ws0-events.cql"
      " re-hashed from disk and matched the recorded identity BEFORE any measurement")
PY
  then
    echo "FATAL: the corpus's schema could not be verified against its recorded identity." >&2
    echo "       ws0-events.cql is a MEASUREMENT INPUT both arms read (#3272 R2)." >&2
    return 2
  fi
}

# write_ticket_template_for_session — THE FLIGHT TICKET, WRITTEN BEFORE THE PIN BECAUSE IT IS PINNED
# (#3272 round 10 M1; relocated by round 13 F2).
#
# `ticket-template.json` IS THE REQUEST (keyspace/table/DDL/token range/projection/predicates/
# aggregation/limit), and `flight-loadgen --ticket-template` re-reads it on EVERY invocation of every
# rep of every arm. It used to be written 90 lines BELOW the pin and recorded NOWHERE, so it could be
# changed between reps or between ARMS while the corpus stayed untouched — leaving every corpus
# digest, the component set and the schema in agreement and the report exiting 0 having compared two
# arms that answered DIFFERENT QUERIES. Round 10's F-B one layer out (F-B: different corpora; this:
# different requests). Full argument: scripts/perf/ws0_ticket_input.py.
#
# WHAT the request is lives in that module (a fixed full-ring `SELECT *`, as data, beside the check
# that decides whether the measured request was the pinned one); it is generated from the DDL whose
# digest `verify_corpus_schema_input` verified, so request and data are anchored to one schema.
#
# ...AND IT IS WRITTEN INTO `$OUT_DIR`, NOT THE CORPUS (#3272 round 13, F2). M1 put it in the SHARED
# CORPUS directory, so two lanes measuring ONE corpus overwrote each other's request BETWEEN the pin
# and the reps: identical shapes agreed silently (a guarantee held by luck), differing ones refused a
# correct multi-minute run diagnosing a "mid-session mutation" nobody performed. It also forced an
# otherwise immutable, shareable, read-only-mountable corpus to be WRITABLE for no reason. `$OUT_DIR`
# is claimed EXCLUSIVELY by `claim_out_dir` (an atomic mkdir marker), which is the same ownership move
# round 12's F2 made for the measured binaries.
write_ticket_template_for_session() {
  # SET HERE, beside the write, so the path the reps use and the path the session pinned cannot drift.
  TICKET_TEMPLATE="$OUT_DIR/ticket-template.json"
  python3 -c '
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid
from ws0_ticket_input import write_ticket_template
out, ddl = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
try:
    digest = write_ticket_template(out, ddl)
except Invalid as exc:
    print(f"FATAL: {exc}", file=sys.stderr)
    raise SystemExit(1)
print(f"ticket pin:   {digest} — ticket-template.json (the REQUEST every Flight rep re-reads)"
      " written from the verified DDL into THIS SESSION dir BEFORE the corpus pin, which records"
      " this digest — the corpus is never written to, so a concurrent session cannot collide")
' "$WS0_INPUTS_LIB_DIR" "$OUT_DIR" "$CORPUS/ws0-events.cql" \
    || { echo "FATAL: the Flight ticket template could not be written, so this session cannot pin" >&2
         echo "       WHICH REQUEST it measures — and an unpinned request can be changed between" >&2
         echo "       ARMS while the corpus stays untouched and every corpus digest still agrees," >&2
         echo "       so two arms would answer different queries under one report (#3272 M1)." >&2
         return 2; }
}

# verify_corpus_is_canonical_or_declared — IS THIS THE CORPUS THE BASELINE IS DEFINED AS?
# (#3272 round 13, F3)
#
# THE FINDING. The pre-measurement pin (`write_session_corpus_pin`, called immediately below this in
# the driver) snapshots the identity of the corpus it was HANDED and compares it against nothing.
# Everything downstream is then self-consistency ABOUT THAT CORPUS: the pin matches the report-time
# identity, the components match the pin, the schema matches its recorded digest, every rep's rows
# are an exact multiple of the pinned row count. All of it is equally true of a corpus generated with
# smoke-test row counts, a different seed, or any other noncanonical parameter — so such a corpus
# passed the driver AND the reporter as a WS0 BASELINE, with nothing in the printed report to
# distinguish it from the real thing.
#
# The identity being verified was never compared to the identity the baseline is DEFINED as. The
# canonical shape lives in `tools/ws0-corpus-gen/src/measurement_corpus.rs`, and until now NOTHING
# under `scripts/` referred to it — see `ws0_canonical_corpus.py` for the cross-language bridge and
# why it is a source parse rather than a build or a committed copy.
#
# A SMOKE CORPUS STILL RUNS, under `--non-baseline`, LABELLED in the manifest and the report. That is
# not a courtesy: rounds 9 and 10 each shipped a fix that made a documented operator command unable
# to succeed, and a command that always fails teaches an operator to stop running it, which loses the
# whole check. Forbidding the smoke corpus would be the fourth instance.
#
# CALLED BEFORE THE PIN, from the driver, so the ORDER stays legible at the driver's top level and a
# refusal costs seconds rather than a multi-minute measurement.
verify_corpus_is_canonical_or_declared() {
  WS0_BASELINE_MODE_ARG="$BASELINE_MODE" python3 - "$WS0_INPUTS_LIB_DIR" "$CORPUS" "$REPO_ROOT" <<'PY' \
    || { echo "FATAL: this session's corpus was not established to be the canonical measurement" >&2
         echo "       corpus, so it cannot be measured as a WS0 BASELINE. The pre-measurement pin" >&2
         echo "       used to record the identity of whatever corpus it was handed and compare it" >&2
         echo "       against NOTHING, so a smoke-sized corpus reported as a baseline (#3272 F3)." >&2
         echo "       Pass --non-baseline to measure it anyway (the run and the report are then" >&2
         echo "       LABELLED as not a baseline), or regenerate the canonical corpus." >&2
         return 2; }
import os, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_canonical_corpus import require_canonical_or_declared
from ws0_validate import Invalid, load_corpus_identity
corpus, repo_root = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
mode = os.environ["WS0_BASELINE_MODE_ARG"]
try:
    rec = require_canonical_or_declared(
        repo_root, load_corpus_identity(corpus), mode, corpus
    )
except Invalid as exc:
    print(f"FATAL: {exc}", file=sys.stderr)
    raise SystemExit(1)
print(f"baseline mode: {rec['mode']} — {rec['label']}"
      f" ({len(rec['compared_fields'])} canonical field(s) compared against"
      f" {rec['canonical_pin_source']} BEFORE the first rep)")
PY
}
