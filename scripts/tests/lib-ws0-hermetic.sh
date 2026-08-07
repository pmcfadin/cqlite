#!/usr/bin/env bash
# lib-ws0-hermetic.sh — the ONE sanctioned way a self-test may invoke the WS0
# measurement driver, and the STRUCTURAL lint that no test invokes it any other way
# (issue #3272 review round 3, B1).
#
# Sourced, not executed. It sets NO shell options: `set -uo pipefail` in a sourced
# library mutates the SOURCING shell's options, which is a caller's decision.
#
# # Why this is a library and a lint rather than a rule in a comment
#
# `scripts/perf/ws0-baseline.sh` has an ARGUMENT-VALIDATION BOUNDARY
# (`--validate-args-only`). Above it everything is a string/integer decision; below it
# the driver stats the corpus, reads the host CPU topology, WRITES HOST SYSCTLS via
# `sudo -n`, runs `cargo build --release`, drops the page cache and takes 45-second
# `perf stat` measurements. A self-test that runs the driver WITHOUT that flag, and
# without PATH shims, is one regression away from doing all of it inside the gate's
# `tooling-tests` component.
#
# That is not hypothetical — it is the review finding this file exists for, and it had
# already escaped TWO fix rounds:
#
#   * round 1 asserted the ACCEPT direction by running the real driver and accepting
#     "it failed at some later checkpoint" as proof the arguments were fine. On Linux
#     that ran `relax_perf_sysctls` + a full release build, six times over.
#   * round 2 introduced `--validate-args-only` and the recording shims and converted
#     the accept cases — but left ONE call site bare (the cold-ceiling accept case),
#     and every REJECT-direction call site bare as well. A manual sweep missed it
#     twice, which is the argument for a mechanism.
#
# MEASURED against that bare call site, on a LINUX-SHAPED host (a fake
# `/sys/devices/system/cpu` with genuine `2,10` siblings, readable sysctl priors,
# recording PATH shims), the shim recording file held:
#
#     sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
#     sudo -n sysctl -w kernel.perf_event_paranoid=2
#     sudo -n sysctl -w kernel.kptr_restrict=1
#
# i.e. the run mutated host hardening and then had to restore it. On the gate's own
# Linux box — where `sudo -n` succeeds — control continues into
# `cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen` and then
# into `measure_scan`/`measure_flight`: 3 reps x 2 arms of 45s Flight steps under real
# `perf stat`. It was invisible on macOS only because the run stops earlier.
#
# So the contract is MECHANICAL, not remembered:
#
#   1. Every driver invocation in every `scripts/tests/test_ws0_*.sh` goes through
#      `ws0_driver_run` (or its `ws0_driver_run_copy` sibling for an injected copy),
#      which prepends `--validate-args-only` AND the recording shims.
#   2. `ws0_hermeticity_lint` FAILS on any other invocation, by LOCATION rather than
#      by spelling — the same posture as `scripts/perf/lib-perf-lint.sh`'s layer 1, and
#      after round 4 it actually IS that posture: an unresolvable command word is treated
#      as an invocation (see the header of `ws0_hermeticity_lint.py`, where the three
#      spellings the awk version missed are recorded with their measurements).
#      A line that genuinely must run past the boundary carries an explicit
#      `ws0-hermetic-allow` marker, so it is a decision someone made in review.
#   3. The shims must be OBSERVED to record (`ws0_assert_shims_record`), because an
#      empty recording file is what "hermetic" is asserted from, and an oracle that
#      cannot answer would make every such assertion vacuous.

# ---------------------------------------------------------------------------
# The recording shims
# ---------------------------------------------------------------------------
# `sudo`, `cargo`, `perf` and `taskset` are shimmed to RECORD any invocation and exit
# non-zero. The recording file must stay EMPTY across a driver run: that is the
# hermeticity contract, asserted rather than assumed.
WS0_SHIM_TOOLS='sudo cargo perf taskset'

# ws0_hermetic_init <tmpdir> — build the shim dir + recording file under <tmpdir>.
# Sets WS0_SHIM_BIN and WS0_HERMETIC_CALLS.
ws0_hermetic_init() {
  local tmp="$1" tool
  WS0_SHIM_BIN="$tmp/ws0-hermetic-bin"
  WS0_HERMETIC_CALLS="$tmp/ws0-hermetic-calls.txt"
  mkdir -p "$WS0_SHIM_BIN"
  : > "$WS0_HERMETIC_CALLS"
  for tool in $WS0_SHIM_TOOLS; do
    cat > "$WS0_SHIM_BIN/$tool" <<SHIM
#!/usr/bin/env bash
printf '%s %s\n' "$tool" "\$*" >> "$WS0_HERMETIC_CALLS"
exit 97
SHIM
    chmod +x "$WS0_SHIM_BIN/$tool"
  done
}

# ws0_hermetic_reset — clear the recording file before a case.
ws0_hermetic_reset() { : > "$WS0_HERMETIC_CALLS"; }

# ws0_hermetic_calls — whatever the shims recorded since the last reset.
ws0_hermetic_calls() { cat "$WS0_HERMETIC_CALLS"; }

# ws0_driver_run <driver> <args…> — THE sanctioned invocation. Runs <driver> with the
# shims on PATH and `--validate-args-only` prepended, so it STOPS at the argument
# boundary having touched nothing outside its own process. Prints stdout+stderr,
# returns the driver's status, and RESETS the recording file first so the caller's
# `ws0_hermetic_calls` describes this run alone.
#
# `--validate-args-only` is prepended rather than appended because the driver's
# argument loop is order-independent and a caller's `--corpus X` must still be parsed;
# prepending also means a caller cannot accidentally place it after a `--` style
# terminator.
#
# Note what this DOES still exercise: the argument loop, `require_positive_int`, the
# duration parser and its ceilings, the scan-passes/cold interaction, AND the perf
# invocation lint (`perf_invocation_lint_tree`), which runs at driver startup above the
# boundary. So every refusal a self-test asserts is reachable through this path.
ws0_driver_run() {
  local driver="$1"; shift
  ws0_hermetic_reset
  PATH="$WS0_SHIM_BIN:$PATH" bash "$driver" --validate-args-only "$@" 2>&1  # ws0-hermetic-allow: THE sanctioned invocation
}

# ws0_driver_ran_hermetically — 0 when the last `ws0_driver_run` executed NOTHING.
ws0_driver_ran_hermetically() { [ ! -s "$WS0_HERMETIC_CALLS" ]; }

# ---------------------------------------------------------------------------
# The structural lint — delegated to scripts/tests/ws0_hermeticity_lint.py
# ---------------------------------------------------------------------------
# The lint ITSELF lives in python (#3272 review round 4). Two blockers forced that move, and
# both were "the guard asks the wrong question", not "the pattern was slightly off":
#
#   B1 — the awk predicate required a literal `bash`/`sh` TOKEN on the SAME PHYSICAL LINE as a
#        driver token, i.e. it asked by SPELLING while its own header claimed LOCATION. Three
#        ordinary shapes walked past it, each MEASURED at zero findings: a line-continuation
#        split (`bash \` / newline / `"$DRIVER" …`), a direct `"$DRIVER" …`/`exec "$DRIVER" …`,
#        and `env -i "$DRIVER" …` — which is how the driver's own usage text at
#        `ws0-baseline.sh:52` documents running it. The fix is the perf lint's posture: reduce
#        a JOINED LOGICAL LINE to its command words and treat an UNRESOLVABLE one as an
#        invocation. Fail closed on "could be", so there is no enumeration left to be wrong.
#   B2 — the subject was `test_ws0_*.sh` ONLY, which excluded the two `lib-ws0-*.sh` helpers
#        round 3 had just added (this file, where `ws0_driver_run` LIVES, was one of them), and
#        the "subject is complete" check compared the subject glob against THE SAME GLOB. The
#        fix separates a DEFINITION (`subject`: every `*.sh`/`*.py` under scripts/tests) from an
#        INDEPENDENT ORACLE (`census`: every tracked file whose content mentions the driver),
#        and asserts containment between them — an assertion that can actually fail.
#
# Why python and not more awk: joining logical lines, stepping over assignment prefixes and
# wrapper options, and walking `git ls-files` are three things awk does badly and a second awk
# rewrite would be the fourth attempt at the same predicate. python3 is already a HARD
# requirement of this rig (`ws0-baseline.sh` refuses to run without it), so it adds no
# dependency. The shell wrappers below keep the call sites unchanged.

# The python implementation. Resolved from THIS file's location, so a sourcing test in any
# directory finds it.
WS0_HERMETIC_LINT_PY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ws0_hermeticity_lint.py"

# _ws0_lint_py <args…> — run the lint and turn a MISSING `#COMPLETE` marker into a finding.
#
# The caller counts OUTPUT, so a python that died mid-scan would print nothing and read exactly
# like a clean tree. The marker is filtered here rather than by the reader: a diagnostic a human
# must remember to ignore is one they will read as a finding.
_ws0_lint_py() {
  local out rc
  out="$(python3 "$WS0_HERMETIC_LINT_PY" "$@" 2>&1)"; rc=$?
  if ! grep -q '^#COMPLETE ' <<<"$out"; then
    echo "0: the hermeticity lint did not COMPLETE (exit $rc) — a partial scan prints exactly"
    echo "0: like a clean one. Output was: $(head -3 <<<"$out" | tr '\n' ' ')"
    return 0
  fi
  grep -v '^#COMPLETE ' <<<"$out" | grep -v '^$' || true
}

# ws0_hermeticity_lint <file> — one `<lineno>: <reason>` per violation, nothing when clean.
#
# A violation is a line whose COMMAND WORD is (or could be) the WS0 driver, outside
# `ws0_driver_run`. The marker `ws0-hermetic-allow` exempts a line that must genuinely run past
# the argument boundary. VACUITY IS REPORTED: an unreadable or empty file is a finding.
ws0_hermeticity_lint() {
  local file="$1"
  if [[ ! -r "$file" ]]; then
    echo "0: $file is not readable, so the hermeticity lint's subject is ABSENT — which prints exactly like a clean file"
    return 0
  fi
  # The python prints `<path>:<lineno>: …`; this wrapper's contract is `<lineno>: …`, so the
  # path prefix is stripped. `${file//./\\.}` is not needed — the path is matched literally.
  _ws0_lint_py lint "$file" | while IFS= read -r line; do
    printf '%s\n' "${line#"$file":}"
  done
}

# ws0_hermeticity_lint_tree <dir> — lint the WHOLE SUBJECT, printing
# `<file>:<lineno>: <reason>` per violation plus any `UNCOVERED`/`STALE-EXEMPTION` finding from
# the completeness oracle.
#
# The subject is every `*.sh`/`*.py` under <dir> — the `lib-ws0-*.sh` helpers included (B2) —
# and the completeness of that subject is asserted against the INDEPENDENT census of tracked
# files mentioning the driver. ZERO files is a finding rather than a clean tree.
ws0_hermeticity_lint_tree() {
  local dir="$1" out subj_out
  local -a files=()
  while IFS= read -r line; do
    [[ "$line" == SUBJECT$'\t'* ]] || continue
    files+=("${line#SUBJECT$'\t'}")
  done < <(python3 "$WS0_HERMETIC_LINT_PY" subject "$dir" 2>/dev/null)
  if [[ "${#files[@]}" -eq 0 ]]; then
    echo "$dir:0: the hermeticity lint's subject is EMPTY, which prints exactly like a clean tree"
    return 0
  fi
  # A file's path in the subject list is repo-relative; resolve against the repo root.
  local root
  root="$(cd "$dir/../.." && pwd)"
  local -a abs=()
  for f in "${files[@]}"; do abs+=("$root/$f"); done
  _ws0_lint_py lint "${abs[@]}"
  # ...and the SUBJECT-COMPLETENESS findings, which are findings of the same lint: a tracked
  # file that mentions the driver, is outside the subject, and carries no recorded exemption.
  subj_out="$(python3 "$WS0_HERMETIC_LINT_PY" subject "$dir" 2>&1)"
  if ! grep -q '^#COMPLETE ' <<<"$subj_out"; then
    echo "$dir:0: the subject/completeness check did not COMPLETE — a partial answer prints like a complete one"
    return 0
  fi
  grep -E '^(UNCOVERED|STALE-EXEMPTION)' <<<"$subj_out" | while IFS= read -r line; do
    printf '%s:0: %s\n' "$dir" "$line"
  done
}

# ws0_hermeticity_lint_subject <dir> — the files the tree lint WOULD examine, one absolute path
# per line. Exists so a test can assert the SUBJECT rather than trust it: "the lint covers every
# file" is a claim about a SET, and a set claim needs the set printed — and, per B2, needs to be
# checked against something that is not the set's own definition (see `ws0_hermeticity_subject_report`).
ws0_hermeticity_lint_subject() {
  local dir="$1" root line
  root="$(cd "$dir/../.." && pwd)"
  while IFS= read -r line; do
    [[ "$line" == SUBJECT$'\t'* ]] || continue
    printf '%s\n' "$root/${line#SUBJECT$'\t'}"
  done < <(python3 "$WS0_HERMETIC_LINT_PY" subject "$dir" 2>/dev/null)
}

# ws0_hermeticity_lint_reserved — the RESERVED-WORD set the lint's grammar relies on, as
# `<CLASS>\t<word>` lines. Exists so the suite can compare it against `bash -c 'compgen -k'`:
# that enumeration is the ONLY one the grammar depends on, and the claim that it is CLOSED needs
# an oracle outside the lint's own constant (#3272 round 6, B1). VACUITY IS REPORTED — a missing
# `#COMPLETE` marker becomes a finding line rather than an empty set that would compare equal to
# an empty lint set.
ws0_hermeticity_lint_reserved() {
  local out rc
  out="$(python3 "$WS0_HERMETIC_LINT_PY" reserved 2>&1)"; rc=$?
  if ! grep -q '^#COMPLETE reserved=' <<<"$out"; then
    echo "INCOMPLETE${TAB:-	}the reserved-word report did not COMPLETE (exit $rc): $(head -2 <<<"$out" | tr '\n' ' ')"
    return 0
  fi
  grep -v '^#COMPLETE ' <<<"$out"
}

# ws0_hermeticity_subject_report <dir> — the RAW subject/census report: `SUBJECT`, `EXEMPT`,
# `UNCOVERED`, `STALE-EXEMPTION` and the `#COMPLETE` marker. For a test that needs to assert
# on the completeness oracle itself.
ws0_hermeticity_subject_report() {
  python3 "$WS0_HERMETIC_LINT_PY" subject "$1" 2>&1
}
