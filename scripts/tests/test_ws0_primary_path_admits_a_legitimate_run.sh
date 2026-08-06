#!/usr/bin/env bash
# test_ws0_primary_path_admits_a_legitimate_run.sh — THE ACCEPT DIRECTION OF THE PRIMARY PATH
# (issue #3272 review round 11, structural finding + F1).
#
# # WHY THIS FILE EXISTS — a root cause, not a fourth guard
#
# Review rounds 9, 10 and 11 each returned exactly FOUR findings, and most of each round's findings
# lived in the PREVIOUS round's fixes. Three of round 11's four were in code round 10 had just added.
# The root cause is specific:
#
#     EVERY FIX ADDED A TEST THAT OBSERVES ITS GUARD **REJECTING** BAD INPUT,
#     AND NOTHING TESTED THE **ACCEPT** DIRECTION OF THE PRIMARY COMMAND.
#
# So the same defect shipped three times, each time in a newly-added guard:
#
#   round 9,  F1  — broke the documented `--verify-against` command;
#   round 10, L1  — broke the documented digest-oracle command;
#   round 10, M2  — broke the NORMAL MEASUREMENT COMMAND (round 11's F1, below). The mtime-vs-HEAD
#                   staleness check was applied in BOTH build modes on the premise that "under a
#                   build it cannot fire, because `cargo build` has just touched every artifact."
#                   That premise is FALSE for cargo's central design reason: **cargo does not
#                   rewrite an artifact it considers current.** A script- or docs-only commit
#                   followed by a successful `cargo build --release` relinks nothing, so every mtime
#                   stays earlier than HEAD and the driver REFUSED — telling the operator to
#                   "re-run without --no-build" when they had not passed it and the build had
#                   succeeded.
#
# Three instances of one class. The three REJECT-direction suites (`test_ws0_provenance_guards.sh`,
# `test_ws0_report_guards.sh`, `test_ws0_fabrication_guards.sh`) are all green throughout, and by
# construction cannot see this: a guard that refuses EVERYTHING satisfies every one of them.
#
# The property this file asserts is therefore the complement, and it is one sentence:
#
#     THE PROVENANCE AND VALIDATION GATES **ADMIT A LEGITIMATE RUN.**
#
# # WHAT THIS FILE EXERCISES, AND WHAT IT DOES NOT — stated, because an honest partial is fine
#     and a SILENT partial is the thing this rig exists to refuse
#
# It cannot reach a MEASUREMENT hermetically: measuring needs `perf`, `taskset`, `sudo -n` for the
# sysctl relaxation, a 2.8 GB corpus and a multi-minute release build, and running any of that from
# the gate's `tooling-tests` component is the exact defect `lib-ws0-hermetic.sh` was created for. So
# the coverage is stated per gate rather than claimed wholesale.
#
# REACHED AND ASSERTED TO ACCEPT (part 1, through the real driver via `ws0_driver_run`):
#
#   * the whole ARGUMENT-VALIDATION stage of `scripts/perf/ws0-baseline.sh` for the DOCUMENTED
#     invocation (`--corpus <dir>` with every other flag defaulted, exactly as `scripts/perf/
#     README.md` step 2 prints it), plus the documented narrow and repeated-run variants —
#     `--reps N`, `--temp both`, `--arm both`, `--no-build`, `--out DIR`, `--scan-passes N`,
#     `--step-duration`/`--cold-step-duration`. Each must print `ARGUMENTS OK` AND have executed
#     nothing (`ws0_driver_ran_hermetically`);
#   * `perf_invocation_lint_tree`, which runs unconditionally at driver startup ABOVE the argument
#     boundary — so a rig source tree that tripped its own perf lint could not print `ARGUMENTS OK`;
#   * `require_unused_out_dir`, called above the boundary, for the ACCEPT case (a fresh `--out`).
#
# REACHED AND ASSERTED TO ACCEPT (part 2, the shipped provenance functions driven directly, against
# a throwaway git repo and real files under `$TMPDIR`):
#
#   * `refuse_binaries_older_than_head` — F1's subject. The ACCEPT case is EXACTLY the input the
#     unscoped check refused: a binary whose mtime PRECEDES HEAD, under `built`;
#   * `record_binary_provenance` end to end — copy three real executables into the session dir, hash
#     the copies, run `git` in a real repo, write `binary-provenance.json`, and have the SHIPPED
#     READER (`verify_binary_provenance`) accept what the SHIPPED WRITER produced. That
#     writer/reader round-trip is the one thing no reject-direction case can establish, and it is
#     where a writer/reader field disagreement would surface as a refusal blaming the operator;
#   * THE FREEZE ITSELF (#3272 F2), by EXECUTING THE RACE: after the freeze, the SOURCE binary under
#     `target/release` is overwritten — exactly what a concurrent `cargo build` does mid-session —
#     and the session's copy is asserted unchanged, still matching its recorded digest, with the
#     reader still accepting. That is the property stated rather than the mechanism assumed;
#   * `verify_schema_input`, `verify_ticket_template`-shaped ticket verification and
#     `verify_session_corpus_pin` over a real (tiny) corpus written by the shipped writers.
#
# NOT REACHED (and therefore NOT claimed):
#
#   * anything below the argument boundary IN THE DRIVER: corpus resolution, the CPU-sibling and
#     disjointness verification against real sysfs, the port probe, `relax_perf_sysctls`, the
#     release build, cache drops, `perf stat`, the rep loop, `ws0_report.py` on a real session.
#     The sibling/disjointness accept direction is covered by
#     `test_ws0_cpu_pinning_guards.sh` against an injected topology root; the reporter's accept
#     direction is covered by the `OBSERVED`/`NON-VACUITY` control cases in the three reject suites
#     (each carries a "the SAME session WITHOUT the tamper reports cleanly" half).
#   * a real `cargo build`. F1's premise ("cargo does not rewrite a current artifact") is not
#     re-verified here — it is cargo's documented behaviour, and asserting it would require the
#     multi-minute build this file must not run. What IS asserted is that the rig no longer DEPENDS
#     on the opposite being true.
#
# Hermetic: the driver runs only through `ws0_driver_run` (`--validate-args-only` + recording
# `sudo`/`cargo`/`perf`/`taskset` shims, asserted to have recorded nothing), plus a throwaway git
# repo and a few KB of files under `$TMPDIR`. No cargo, perf, sudo, taskset, corpus, network or root.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
PERF_DIR="$REPO_ROOT/scripts/perf"

fails=0
# `checks` counts what actually RAN, so the floor at the end can see a block that silently never
# executed while the gate reads only the exit code.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
# python3 is a HARD REQUIREMENT of this rig (the driver refuses to run without it), so its absence
# is a FAILED CHECK and never a skip: exiting 0 here would record the component as SUCCESS with
# none of the checks below having run — the vacuous green this rig exists to refuse.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig, so its absence"
  echo "       is a failed check and not a skip."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# The ONE sanctioned way a self-test may invoke the driver: `--validate-args-only` prepended and
# `sudo`/`cargo`/`perf`/`taskset` shimmed to RECORD and fail. The structural hermeticity lint FAILs
# on any other invocation, which is why every driver call below goes through `ws0_driver_run`.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
ws0_hermetic_init "$TMP"
# ...and the shims must be OBSERVED to record, or every "it executed nothing" assertion below is
# vacuous: an oracle that cannot answer proves nothing by staying silent. Every shimmed tool is
# probed, not just one — a `sudo` shim that records while the `cargo` shim does not would leave the
# most expensive escape (a release build) unobserved.
#
# Probed with NO ARGUMENTS. Passing an option here (`--probe`) put an unrecognised option token on a
# line whose command word is a variable, which `lib-perf-lint.sh`'s fail-closed layer 1 reads as a
# possible perf invocation carrying an unallowlisted option — a finding that is CORRECT about the
# shape and wrong about the intent, and the fix is to give it nothing to object to rather than to
# mark a check that is working (the same call the driver's prewarm line records).
shims_ok=1
for shim_tool in $WS0_SHIM_TOOLS; do
  ws0_hermetic_reset
  PATH="$WS0_SHIM_BIN:$PATH" "$shim_tool" >/dev/null 2>&1
  ws0_driver_ran_hermetically && shims_ok=0
done
if [ "$shims_ok" -eq 1 ]; then
  pass "shims-record: EVERY recording shim ($WS0_SHIM_TOOLS) was OBSERVED to record — so 'the driver executed nothing' below is an assertion an oracle can actually make"
else
  fail "a recording shim did not record, so every hermeticity assertion here would be vacuous"
fi
ws0_hermetic_reset

# A corpus directory. Its CONTENT does not matter for the argument stage — the driver resolves and
# stats the corpus BELOW the boundary — but it must exist, because an operator's real invocation
# names a real directory and this file's whole subject is the real invocation.
CORPUS="$TMP/corpus"
mkdir -p "$CORPUS/ws0/events"

# ============================================================================
# PART 1 — THE DOCUMENTED INVOCATIONS PRINT `ARGUMENTS OK`, HAVING EXECUTED NOTHING
# ============================================================================
# `scripts/perf/README.md` step 2 is `scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096`. That
# command, and the documented variants, must reach the argument boundary and PASS it. A guard added
# above that boundary which refuses any of them has broken the rig for its only user, and that has
# now happened three times.
#
# `accept_args <label> <args…>` asserts BOTH halves: `ARGUMENTS OK` on stdout, and nothing executed.
accept_args() {
  local label="$1"; shift
  local out rc
  out=$(ws0_driver_run "$DRIVER" "$@"); rc=$?
  if [ "$rc" -ne 0 ] || ! grep -q 'ARGUMENTS OK' <<<"$out"; then
    fail "primary-path ACCEPT ($label): the documented invocation must pass argument validation (rc=$rc, out: $(head -3 <<<"$out"))"
    return
  fi
  if ! ws0_driver_ran_hermetically; then
    fail "primary-path ACCEPT ($label): argument validation must execute NOTHING (shims recorded: $(ws0_hermetic_calls | head -3))"
    return
  fi
  pass "primary-path ACCEPT: $label"
}

# THE headline case: the command the README prints, with every other flag defaulted.
accept_args "the README's own command — --corpus DIR, everything else defaulted" --corpus "$CORPUS"
# The documented variants an operator actually reaches for. Each one is a flag whose validation a
# future guard could break, and none of them is exercised in the accept direction anywhere else.
accept_args "--reps 3 (the median-of-N run the method describes)" --corpus "$CORPUS" --reps 3
accept_args "--temp both (warm AND cold — separate claims, never blended)" --corpus "$CORPUS" --temp both
accept_args "--arm both (the head-to-head the reported ratio IS)" --corpus "$CORPUS" --arm both
accept_args "--no-build (re-measuring without a 5-minute rebuild — the normal operator loop)" \
  --corpus "$CORPUS" --no-build
accept_args "--out DIR (a FRESH dir: require_unused_out_dir's accept direction)" \
  --corpus "$CORPUS" --out "$TMP/fresh-out-dir"
accept_args "--scan-passes 4 with --temp warm (multi-pass amortization, legal when not cold)" \
  --corpus "$CORPUS" --temp warm --scan-passes 4
accept_args "the durations spelled as the help text spells them" \
  --corpus "$CORPUS" --step-duration 45s --cold-step-duration 1s
accept_args "the CPU pins spelled explicitly (the #3096 host's own pair)" \
  --corpus "$CORPUS" --server-cpus 2,10 --client-cpus 4,12
# ...and the FULL documented matrix in one command, because a per-flag accept cannot see an
# INTERACTION guard that refuses a legal combination (the cold/scan-passes pair is such a guard, and
# it must fire only on the ILLEGAL combination).
accept_args "the full documented matrix in ONE command" \
  --corpus "$CORPUS" --reps 3 --temp both --arm both --scan-passes 1 \
  --step-duration 45s --cold-step-duration 1s --port 47311 --out "$TMP/fresh-matrix-out"

# NON-VACUITY for part 1: `accept_args` must be capable of FAILING. Asserted by running the same
# harness against an invocation that MUST be refused — without this, a broken `ws0_driver_run` (or a
# driver that printed `ARGUMENTS OK` unconditionally) would satisfy every case above.
out=$(ws0_driver_run "$DRIVER" --corpus "$CORPUS" --reps 0); rc=$?
if [ "$rc" -ne 0 ] && ! grep -q 'ARGUMENTS OK' <<<"$out"; then
  pass "primary-path NON-VACUITY: the same harness REFUSES --reps 0, so the accepts above are measurements rather than a harness that always passes"
else
  fail "the accept harness must be able to fail: --reps 0 was not refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the ILLEGAL interaction really is refused, so the legal one being accepted above is
# discrimination rather than permissiveness.
out=$(ws0_driver_run "$DRIVER" --corpus "$CORPUS" --temp cold --scan-passes 4); rc=$?
if [ "$rc" -ne 0 ] && ! grep -q 'ARGUMENTS OK' <<<"$out"; then
  pass "primary-path NON-VACUITY: --temp cold with --scan-passes 4 is still REFUSED (the blend guard discriminates; it does not refuse every multi-pass run)"
else
  fail "the cold/scan-passes blend guard must still refuse the illegal combination (rc=$rc)"
fi

# ============================================================================
# PART 2 — ROUND 11's F1: THE STALENESS CHECK MUST ADMIT A FRESHLY-BUILT BINARY
# ============================================================================
# The defect, precisely: `refuse_binaries_older_than_head` compared each binary's mtime against the
# HEAD commit's timestamp in BOTH build modes. Cargo does not rewrite an already-current artifact,
# so after a commit touching no rust a successful `cargo build --release` leaves every mtime earlier
# than HEAD — and the driver refused the normal measurement command.
#
# Driven against the SHIPPED function with a throwaway git repo under $TMPDIR. Hermetic: `git init`
# in a temp dir, no cargo, no perf, no host state.
if python3 - "$PERF_DIR" "$TMP/f1-repo" <<'PY'
import os, pathlib, subprocess, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import refuse_binaries_older_than_head
from ws0_validate import Invalid

repo = pathlib.Path(sys.argv[2]); repo.mkdir(parents=True, exist_ok=True)
env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@e",
       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@e"}
def git(*a):
    subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, env=env)
git("init", "-q")
(repo / "f").write_text("x")
git("add", "f"); git("commit", "-qm", "a script-only commit")
head = int(subprocess.run(["git", "-C", str(repo), "log", "-1", "--format=%ct"],
                          capture_output=True, text=True, check=True).stdout.strip())

# THE ACCEPT CASE, and it is EXACTLY the input the unscoped check refused: a binary a day older
# than HEAD, in `built` mode — what a default `cargo build` legitimately leaves after a commit that
# touched no rust.
note = refuse_binaries_older_than_head(repo, {"b": {"mtime_epoch": head - 86400}}, "built")
# The note must SAY the check did not apply, and WHY. A silent skip prints exactly like a pass, so
# "does not apply" must be readable IN THE RECORD rather than inferred from its absence.
assert "does NOT APPLY" in note, note
assert "cargo does not rewrite" in note, note
assert "reused" in note, note
# ...and `reused` STILL REFUSES the same input, so the scoping narrowed the guard rather than
# deleting it. This is the case `--no-build` makes reachable and the reason the check exists.
try:
    refuse_binaries_older_than_head(repo, {"b": {"mtime_epoch": head - 86400}}, "reused")
except Invalid as exc:
    assert "STALE BINARIES" in str(exc), str(exc)
else:
    raise SystemExit("under `reused`, a binary older than HEAD must STILL be refused")
# An UNCLASSIFIED mode is REFUSED rather than defaulted: `built` would silently skip the check that
# closes --no-build's silence, and `reused` would refuse legitimate fresh builds. Neither default is
# safe, so there is none.
try:
    refuse_binaries_older_than_head(repo, {"b": {"mtime_epoch": head - 10}}, "assumed")
except Invalid as exc:
    assert "not one of" in str(exc), str(exc)
else:
    raise SystemExit("an unclassified build_mode must be refused by the staleness check")
PY
then
  pass "round11 F1 ACCEPT: under \`built\`, a binary OLDER than HEAD is ADMITTED with a note stating the check does not apply and why — this exact input broke the NORMAL measurement command, because cargo does not rewrite an already-current artifact"
else
  fail "round11 F1: the staleness check must be scoped to \`reused\`, must still refuse there, and must refuse an unclassified mode"
fi
# NON-VACUITY: the round-10 predicate really did refuse that input. A replica of the removed
# mode-blind decision, observed rejecting it — without this the accept case could be about an input
# that was never refused.
if python3 - <<'PY'
# The round-10 decision, in substance: no build_mode in it at all.
head, mtime = 1_700_000_000, 1_700_000_000 - 86400
raise SystemExit(0 if mtime < head else 1)
PY
then
  pass "round11 F1 NON-VACUITY: the PRE-FIX mode-blind predicate REFUSED that same freshly-built binary — i.e. the normal measurement command failed on it, which is the defect"
else
  fail "round11 F1: the pre-fix predicate must have refused it, else the accept case proves nothing"
fi
# STRUCTURAL: the mode must REACH the check. A correctly-scoped function nobody passes the mode to
# would fail closed on every run — the same defect with a different spelling.
if grep -q 'refuse_binaries_older_than_head(repo_root, observed, build_mode)' \
     "$PERF_DIR/ws0_binaries.py"; then
  pass "round11 F1 wired: record_binary_provenance passes the BUILD MODE to the staleness check"
else
  fail "round11 F1: the build mode must be passed to refuse_binaries_older_than_head"
fi

# ============================================================================
# PART 3 — THE SHIPPED WRITER'S OUTPUT SATISFIES THE SHIPPED READER
# ============================================================================
# `record_binary_provenance` runs at MEASUREMENT time and `verify_binary_provenance` at REPORT time,
# and no reject-direction case can establish that the first satisfies the second: the reject suites
# feed the reader a HAND-WRITTEN fixture (`ws0_pin_binaries`), whose shape is asserted against
# `PROVENANCE_FIELDS` but which the real writer never produced. A field the writer omits and the
# reader demands would surface at report time as a refusal blaming the session dir for a driver
# defect — the F1 class one layer over.
#
# So: run the REAL writer over three real files in a real git repo, then hand its output to the REAL
# reader. Both halves shipped, neither re-implemented.
if python3 - "$PERF_DIR" "$TMP/rt" <<'PY'
import os, pathlib, subprocess, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import (MEASURED_BINARIES, describe_record, measured_bin_dir,
                          record_binary_provenance, verify_binary_provenance)

base = pathlib.Path(sys.argv[2]); base.mkdir(parents=True, exist_ok=True)
repo, session, bindir = base / "repo", base / "session", base / "bin"
for d in (repo, session, bindir):
    d.mkdir(parents=True, exist_ok=True)
env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@e",
       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@e"}
def git(*a):
    subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, env=env)
git("init", "-q")
(repo / "f").write_text("x")
git("add", "f"); git("commit", "-qm", "c")

# Three real files standing in for the measured programs. Non-empty (the reader refuses a
# zero-length binary — it cannot have been executed), MODE 0755 (the writer copies them and refuses
# a copy that is not executable, which is the property a real measured binary has), and deliberately
# given an OLD mtime, which is F1's whole point: this is what a default `cargo build` leaves behind,
# and it must be ADMITTED.
for i, name in enumerate(MEASURED_BINARIES):
    p = bindir / name
    p.write_bytes(b"\x7fELF" + bytes([i]) * 64)
    os.chmod(p, 0o755)
    os.utime(p, (1, 1))

rec = record_binary_provenance(session, bindir, repo, "built")
# The READER accepts what the WRITER wrote — the round trip no reject case can establish.
back = verify_binary_provenance(session)
assert set(back["binaries"]) == set(MEASURED_BINARIES), sorted(back["binaries"])
assert back["source_revision"] == rec["source_revision"], (back, rec)
assert back["build_mode"] == "built", back
# THE FREEZE (#3272 F2): the writer must have COPIED the executables into the session's own
# directory, recorded THOSE paths, and left them executable — so the reps run bytes a concurrent
# `cargo build` cannot replace. Asserted on the real files, not on the record's own claim.
import hashlib  # noqa: E402  (used only for this assertion)
for name, spec in rec["binaries"].items():
    copied = pathlib.Path(spec["path"])
    assert copied.parent == measured_bin_dir(session), (name, spec["path"])
    assert copied.is_file() and os.access(copied, os.X_OK), (name, spec["path"])
    # The recorded digest must be the digest OF THE COPY — hashing the source and copying separately
    # would leave the two reads racing, so the digest could describe bytes the copy did not receive.
    assert hashlib.sha256(copied.read_bytes()).hexdigest() == spec["sha256"], name
    # ...and the copy must be independent of the source: overwriting the SOURCE after the freeze must
    # not change what the session would run. This is the race itself, executed.
    src = pathlib.Path(spec["source_path"])
    src.write_bytes(b"\x7fELFREBUILT" + b"\xff" * 64)
    assert hashlib.sha256(copied.read_bytes()).hexdigest() == spec["sha256"], (
        f"{name}: rebuilding the source changed the frozen copy — the copies are not independent"
    )
# ...and the reader still accepts after that "rebuild", because it reads the copies.
verify_binary_provenance(session)
# ...and the driver's own one-line summary is derivable from the record (the line an operator reads).
line = describe_record(rec)
assert "binary pin" in line, line
assert rec["source_revision_short"] in line, line
# The record must state which staleness regime applied, so "the check did not apply" can never be
# read as "the check passed".
assert "does NOT APPLY" in rec["provenance"], rec["provenance"]
PY
then
  pass "round11 structural + F2: the SHIPPED WRITER's record (real files, real git repo, OLD mtimes) satisfies the SHIPPED READER, and the executables were FROZEN into the session dir — OBSERVED by overwriting the SOURCE after the freeze and finding the copy unchanged (the mid-session-rebuild race, executed)"
else
  fail "round11: record_binary_provenance's output must satisfy verify_binary_provenance"
fi

# ============================================================================
# PART 4 — THE CORPUS-SIDE INPUT VERIFIERS ADMIT A LEGITIMATE CORPUS
# ============================================================================
# The schema, the ticket and the session pin are each verified before the first rep, and each was
# added by a previous round. Their accept direction is exercised through the SHIPPED writers over a
# real (tiny) corpus, for the same reason as part 3: a writer/verifier disagreement is invisible to
# a fixture-fed reject case.
if python3 - "$PERF_DIR" "$TMP/corpus-accept" <<'PY'
import hashlib, json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_schema_input import verify_schema_input
from ws0_session import verify_session_corpus_pin, write_session_corpus_pin
from ws0_ticket_input import write_ticket_template
from ws0_validate import load_corpus_identity

base = pathlib.Path(sys.argv[2])
corpus, session = base / "corpus", base / "session"
table = corpus / "ws0" / "events"
table.mkdir(parents=True, exist_ok=True)
session.mkdir(parents=True, exist_ok=True)

raw = bytes(range(256)) * 16
components = {}
for name, body in (("nb-1-big-Data.db", raw),
                   ("nb-1-big-Index.db", b"IDX" * 16),
                   ("nb-1-big-Statistics.db", b"STAT" * 8),
                   ("nb-1-big-Summary.db", b"SUM" * 8),
                   ("nb-1-big-Filter.db", b"FLT" * 8)):
    (table / name).write_bytes(body)
    components[name] = {"name": name, "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest()}
ddl = b"CREATE TABLE ws0.events (part_id text, seq int, PRIMARY KEY (part_id, seq));\n"
(corpus / "ws0-events.cql").write_bytes(ddl)
rows = 1000
(corpus / "corpus-identity.json").write_text(json.dumps({
    "rows": rows, "partitions": 10, "seed": 1, "cells_per_row": 12,
    "data_db_bytes": len(raw), "data_db_sha256": hashlib.sha256(raw).hexdigest(),
    "bytes_per_row": len(raw) / rows, "components": components,
    "schema_sha256": hashlib.sha256(ddl).hexdigest(),
}))
# THE SHIPPED ticket writer, then the shipped verifier over what it wrote.
write_ticket_template(corpus, corpus / "ws0-events.cql")

identity = load_corpus_identity(corpus)
schema_rec = verify_schema_input(corpus, identity)
assert len(schema_rec["schema_sha256_measured"]) == 64, schema_rec

config = {"reps": "1", "temps": "warm", "arms": "bypass", "scan_passes": "1",
          "server_cpus": "2,10", "client_cpus": "4,12", "step_duration": "45s/1s"}
write_session_corpus_pin(session, corpus, identity, config)
# `verify_session_corpus_pin` calls `verify_pinned_ticket` UNCONDITIONALLY (not behind a flag — a
# request check that can be switched off is the fail-open shape one level out), so accepting here
# covers the TICKET's accept direction as well as the pin's.
pin = verify_session_corpus_pin(session, corpus, identity)
assert pin, pin
# The verification's OWN report must carry the ticket digest it re-derived from disk — asserted so
# this case cannot pass on a `verify_pinned_ticket` that was never reached.
assert len(pin.get("pinned_ticket_sha256", "")) == 64, pin
assert pin.get("pinned_ticket_bytes", 0) > 0, pin
PY
then
  pass "round11 structural: the schema verifier, the SHIPPED ticket writer+verifier and the session-pin writer+verifier ADMIT a legitimate corpus (each accept direction, through the shipped code, over real bytes)"
else
  fail "round11: the corpus-side input verifiers must admit a legitimate corpus written by the shipped writers"
fi

# ============================================================================
# A MINIMUM CHECK COUNT
# ============================================================================
# `set -uo pipefail` (no `-e`) means a block that silently never executes lowers the count and
# registers NO failure, while the gate reads only the exit code. Deliberately below the current
# count (so adding a case does not red it) and far above zero.
MIN_CHECKS=16
echo
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would otherwise lower the count with no"
  echo "       failure registered, and the gate reads only the exit code (#3272)."
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 primary-path ACCEPT-direction checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks WS0 primary-path ACCEPT-direction check(s) FAILED"
exit 1
