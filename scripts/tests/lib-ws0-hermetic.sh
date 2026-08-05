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
#      by spelling — the same posture as `scripts/perf/lib-perf-lint.sh`'s layer 1.
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
# The structural lint
# ---------------------------------------------------------------------------
# ws0_hermeticity_lint <file> — print one `<lineno>: <reason>` per violation and
# nothing when the file is clean.
#
# A violation is a non-comment line that invokes the WS0 driver without going through
# `ws0_driver_run`. "Invokes the driver" is asked by LOCATION and by TOKEN, never by
# guessing at a spelling: the line runs a shell interpreter (`bash`/`sh`) against a
# token that names the driver — `$DRIVER`, `${DRIVER}`, a literal `ws0-baseline.sh`
# path, or a driver COPY variable (`$copy`, `$treedir/...`). The marker
# `ws0-hermetic-allow` exempts a line that must genuinely run past the boundary.
#
# VACUITY IS REPORTED, not left to look like cleanliness: a file with no lines, or one
# the scan could not read, is a finding.
ws0_hermeticity_lint() {
  local file="$1" out
  if [[ ! -r "$file" ]]; then
    echo "0: $file is not readable, so the hermeticity lint's subject is ABSENT — which prints exactly like a clean file"
    return 0
  fi
  out="$(awk '
    /^[[:space:]]*#/ { next }                       # full-line comment
    {
      # THE MARKER IS READ OFF THE WHOLE LINE, BEFORE the trailing comment is stripped.
      # Reading it after would look for the marker in the text the strip just removed —
      # i.e. the exemption could never fire, and every marked line would be a finding.
      # (Observed while writing this: the first version stripped first and reported the
      # deliberately-marked probe line as a violation.)
      if (index($0, "ws0-hermetic-allow") > 0) next

      # Strip a trailing comment so prose mentioning the driver is not argv. A `#`
      # only starts a comment at a token boundary.
      line = $0
      if (match(line, /[[:space:]]#/)) line = substr(line, 1, RSTART - 1)

      # Does the line run a shell interpreter?
      runs_shell = (line ~ /(^|[^[:alnum:]_.\/-])(bash|sh)[[:space:]]/)
      if (!runs_shell) next

      # …against a token that names the WS0 driver or a copy of it?
      names_driver = (line ~ /\$\{?DRIVER\}?/) \
                  || (line ~ /ws0-baseline\.sh/) \
                  || (line ~ /\$\{?copy\}?/)
      if (!names_driver) next

      print NR ": invokes the WS0 driver outside ws0_driver_run — no --validate-args-only and no recording shims, so on a LINUX host this reaches relax_perf_sysctls (a host sudo sysctl -w), cargo build --release and the measurement loop (#3272 B1). Route it through ws0_driver_run, or mark the line ws0-hermetic-allow if it must genuinely run past the argument boundary."
      seen++
    }
    END { printf "#LINT-COMPLETE %d\n", NR }
  ' "$file")" || true
  if ! grep -q '^#LINT-COMPLETE ' <<<"$out"; then
    echo "0: the hermeticity lint did not COMPLETE over $file (awk died mid-file) — a partial scan prints like a clean one"
    return 0
  fi
  local nr
  nr="$(sed -n 's/^#LINT-COMPLETE //p' <<<"$out" | tail -1)"
  if [[ "${nr:-0}" -eq 0 ]]; then
    echo "0: $file has no lines — the hermeticity lint's subject is EMPTY"
    return 0
  fi
  grep -v '^#LINT-COMPLETE ' <<<"$out" | grep -v '^$' || true
}

# ws0_hermeticity_lint_tree <dir> — lint EVERY `test_ws0_*.sh` in <dir>, printing
# `<file>:<lineno>: <reason>` per violation. The subject is DISCOVERED by glob, so a
# fourth self-test file cannot silently be added outside the contract, and ZERO files
# is a finding rather than a clean tree.
ws0_hermeticity_lint_tree() {
  local dir="$1" f count=0 out
  local -a files=()
  for f in "$dir"/test_ws0_*.sh; do
    [[ -e "$f" ]] || continue
    files+=("$f")
    count=$((count + 1))
  done
  if [[ "$count" -eq 0 ]]; then
    echo "$dir:0: no test_ws0_*.sh found — the hermeticity lint's subject is EMPTY, which prints exactly like a clean tree"
    return 0
  fi
  for f in "${files[@]}"; do
    out="$(ws0_hermeticity_lint "$f")"
    [[ -z "$out" ]] || printf '%s:%s\n' "$f" "$out"
  done
}

# ws0_hermeticity_lint_subject <dir> — the files the tree lint WOULD examine, one per
# line. Exists so a test can assert the SUBJECT is every self-test rather than trust
# that it is: "the lint covers every file" is a claim about a SET, and a set claim
# needs the set printed.
ws0_hermeticity_lint_subject() {
  local dir="$1" f
  for f in "$dir"/test_ws0_*.sh; do
    [[ -e "$f" ]] && printf '%s\n' "$f"
  done
}
