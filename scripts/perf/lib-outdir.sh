#!/usr/bin/env bash
# lib-outdir.sh — THE SESSION OUTPUT DIRECTORY'S WHOLE LIFECYCLE: refuse a used one, create it
# exclusively, and CLAIM it against a concurrent peer (issue #3272 R1 + review round 7 F3).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library mutates
# the SOURCING shell's options, which is the caller's decision (the same rule `lib-cpu.sh`,
# `lib-args.sh`, `lib-host-state.sh` and `lib-perf-lint.sh` follow).
#
# # Why this is a library, and why THIS seam
#
# Split out of `ws0-baseline.sh` under the campsite rule (source target ~800 lines; that file was
# already at 966 and F3's claim mechanism took it to 1035). Note the gate's `file-size` ratchet is
# `.rs`-ONLY, so a shell file crosses it silently — which is why this is checked with `wc -l`
# rather than left to the gate.
#
# The seam is a RESPONSIBILITY and not a line count. Every other rig library owns one question
# about whether a measurement means what it says:
#
#     lib-cpu.sh          are the pinned CPUs one physical core?
#     lib-host-state.sh   is the host's state put back?
#     lib-args.sh         are the arguments values this rig can measure?
#     lib-perf-lint.sh    is the counting domain CPU-wide?
#     lib-outdir.sh       do the artifacts being read all come from ONE session?
#
# That last one is exactly as load-bearing as the others: the reporter reads whatever rep files
# are present in the session directory and CANNOT TELL which run wrote them, so two sessions
# sharing a directory produce a median across mixed sessions that looks like a valid result.
#
# # The two halves, and why they are on opposite sides of the argument boundary
#
# `require_unused_out_dir` is a pure ARGUMENT check (no perf, no topology, no corpus), so the
# driver calls it ABOVE its argument-validation boundary — which is what makes it reachable, and
# therefore OBSERVABLE by the hermetic self-tests, through `--validate-args-only`.
#
# `create_out_dir`/`claim_out_dir` CREATE things, so they stay BELOW that boundary:
# `--validate-args-only` must still touch nothing outside its own process.
#
# # Both halves are needed, and neither is sufficient
#
# The refusal cannot see a concurrent peer: two runs handed the same absent-or-empty directory
# both pass it, because both check before either writes. The claim cannot replace it either: a
# claim marker makes the directory non-empty, so the refusal is what turns a LATER re-run into a
# clear diagnostic instead of a third session silently mixing in.
# require_unused_out_dir <out-dir-or-empty> — an EXPLICIT `--out` MUST NOT BE A USED DIRECTORY
# (#3272 round 6, R1).
#
# Called ABOVE the driver's argument boundary, and CREATION stays below it. That split is
# deliberate: this is a pure ARGUMENT check (it needs no perf, no topology, no corpus), so
# putting it there makes it reachable — and therefore OBSERVABLE by the hermetic self-tests —
# through `--validate-args-only`, while `--validate-args-only` still creates nothing.
#
# Measuring into a used dir mixes artifacts from DIFFERENT SESSIONS into one report: any rep
# file this session does not overwrite (a different temperature or arm, a higher rep index from
# a longer previous run) is read as part of THIS run, and the reporter cannot tell — it reads
# whatever rep files are present. REFUSED rather than auto-suffixed, because an operator who
# named a directory means that directory, and silently measuring into `<name>-2` would be its
# own attribution defect.
#
# An EMPTY argument (no `--out` given) is a no-op: the default-name branch handles that case, and
# refusing here would make the flag mandatory.
require_unused_out_dir() {
  local out_dir="${1:-}"
  [[ -n "$out_dir" && -e "$out_dir" ]] || return 0
  if [[ ! -d "$out_dir" ]]; then
    echo "FATAL: --out $out_dir exists and is not a directory" >&2
    exit 2
  fi
  # `find -mindepth 1 … -print -quit` answers "non-empty" without listing the whole tree — but
  # its EXIT STATUS MUST BE OBSERVED (#3272 review round 9, F4).
  #
  # It used to read `if [[ -n "$(find … 2>/dev/null)" ]]`, which is the same class of defect as
  # `check-root-junk-files.sh`'s process-substitution enumeration (#3272 F4): a `find` that
  # FAILED — an unreadable directory (mode 0300 is the reproducer: a prior session's rep files
  # are in there, and `find` cannot read the entries) — produced NO OUTPUT and `2>/dev/null`
  # threw away the reason. An empty result and a failed look are then INDISTINGUISHABLE, and the
  # empty result takes the PERMISSIVE branch: the used-directory refusal silently passes, and
  # this session measures into a directory still holding another session's rep files. The
  # reporter reads whatever rep files are present and cannot tell.
  #
  # So: written to a FILE (a plain command status, not a subshell's), status captured, STATUS
  # CHECKED BEFORE EMPTINESS, and stderr KEPT so a `find` that explains itself is quoted rather
  # than discarded. Checking emptiness first would put the failure straight into the
  # "directory is empty, proceed" branch, which is the defect.
  local _ru_out _ru_err _ru_rc=0
  _ru_out="$(mktemp)" || {
    echo "FATAL: could not create a temp file to enumerate --out $out_dir." >&2
    echo "       The enumeration is written to a file so its EXIT STATUS is observable;" >&2
    echo "       without that, a failing find produces no lines and reads as an EMPTY" >&2
    echo "       directory, which silently passes this refusal (#3272 F4)." >&2
    exit 2
  }
  _ru_err="$(mktemp)" || {
    rm -f "$_ru_out"
    echo "FATAL: could not create a temp file for find's stderr while checking --out $out_dir." >&2
    exit 2
  }
  find "$out_dir" -mindepth 1 -print -quit >"$_ru_out" 2>"$_ru_err" || _ru_rc=$?
  if [[ "$_ru_rc" -ne 0 ]]; then
    echo "FATAL: could not enumerate --out $out_dir to check whether it is already used." >&2
    echo "       find exited $_ru_rc: $(tr '\n' ' ' < "$_ru_err")" >&2
    echo "       This is a FAILURE and not an empty directory. A failed enumeration used to be" >&2
    echo "       indistinguishable from an empty one (the status was discarded and stderr sent" >&2
    echo "       to /dev/null), so it took the PERMISSIVE branch and this session would have" >&2
    echo "       measured into a directory that may still hold another session's rep files —" >&2
    echo "       which the reporter cannot tell apart from its own (#3272 F4)." >&2
    echo "       Fix the directory's permissions, or name an --out this user can read." >&2
    rm -f "$_ru_out" "$_ru_err"
    exit 2
  fi
  # STATUS first, THEN emptiness (see above). Only a SUCCESSFUL enumeration may be read.
  if [[ -s "$_ru_out" ]]; then
    rm -f "$_ru_out" "$_ru_err"
    echo "FATAL: --out $out_dir already exists and is NOT EMPTY." >&2
    echo "       Measuring into a used dir mixes artifacts from different sessions into one" >&2
    echo "       report: any rep file this session does not overwrite (a different temperature" >&2
    echo "       or arm, a higher rep index from a longer previous run) is read as part of THIS" >&2
    echo "       run, and the reporter cannot tell the difference (#3272 R1)." >&2
    echo "       Name an unused directory, or remove that one." >&2
    exit 2
  fi
  # The ACCEPT path: a SUCCESSFUL enumeration that found nothing. Cleaned up here rather than in
  # a trap, so this library still sets no options and installs no handlers in the sourcing shell.
  rm -f "$_ru_out" "$_ru_err"
}

# --- THE OUTPUT DIR IS CREATED EXCLUSIVELY, NEVER REUSED (#3272 round 6, R1) ---------
# It used to be `mkdir -p "$OUT_DIR"` over a default name with only SECOND-level uniqueness.
# Two ways that mixes artifacts from DIFFERENT SESSIONS into one report, and the reporter
# cannot see either — it reads whatever rep files are present:
#
#   * TWO CONCURRENT RUNS started in the same second share the default dir. Each writes the
#     rep files for its own arms, `mkdir -p` succeeds for both, and the second run's pin
#     overwrites the first's. The report then assembles a median across two sessions.
#   * AN EXPLICIT `--out <dir>` pointed at a previous run's dir keeps that run's rep files.
#     Any rep the new session does not overwrite (a different temperature, a different arm, a
#     higher rep index from a longer previous run) is silently read as part of this one.
#
# `mkdir` WITHOUT `-p` is the atomic primitive: it FAILS if the directory exists, and that
# failure is the exclusion. So the default name gets a uniqueness suffix and retries, while an
# explicit `--out` is REFUSED if it exists and is non-empty — refused rather than
# suffixed, because an operator who named a directory means that directory, and silently
# measuring into `<name>-2` would be its own attribution defect.
#
# --- WHY THE EXPLICIT PATH NEEDED A CLAIM TOO (#3272 review round 7, F3) ---------------
# R1 fixed the DEFAULT path (an atomic `mkdir` on a unique name) and left the explicit path on
# `mkdir -p`, which succeeds whether or not the directory already existed. So the exact defect
# R1 was about survived one branch over: TWO CONCURRENT RUNS given the same ABSENT-OR-EMPTY
# `--out` BOTH pass the used-directory refusal above (it is empty for both, and both check
# before either writes) and BOTH `mkdir -p` succeed. Each then writes its own session pin and
# rep artifacts over the other's, and the reporter assembles a seemingly valid median from
# MIXED SESSIONS — it reads whatever rep files are present and cannot tell.
#
# A refusal on EXISTENCE would not fix it and would break the accepted case: R1 deliberately
# accepts an existing-but-EMPTY `--out` (a directory an operator or a wrapper script made), and
# in exactly that case `mkdir` is not available as the arbiter. So the exclusion is a CLAIM
# rather than the directory create: `claim_out_dir` atomically creates a marker SUBDIRECTORY,
# and `mkdir` on that marker is the same atomic primitive one level down — it fails with EEXIST
# for the loser whichever way `$OUT_DIR` came to exist.
#
# Applied to BOTH branches, so there is ONE mechanism to reason about rather than two. On the
# default branch it is strictly redundant with the unique-name create (kept untouched), but it
# also closes that branch's own narrow window: a second run handed the first's default dir
# EXPLICITLY, in the interval before the first writes its first artifact, would pass the
# non-empty check.
#
# The claim is never released. It is a permanent record of which pid owned the session — and it
# makes the directory non-empty, so a later re-run against the same `--out` is refused above
# with the used-directory diagnostic rather than silently mixing a third session in.
#
# Below the ARGUMENT BOUNDARY, like every other creation: `--validate-args-only` still creates
# nothing (asserted by test_ws0_provenance_guards.sh).
WS0_CLAIM_NAME=".ws0-session-claim"
claim_out_dir() {
  local dir="$1" how="$2" claim="$1/$WS0_CLAIM_NAME"
  # `mkdir` on the MARKER, without `-p`: an existing marker is an ERROR, and that error is the
  # exclusion. Two concurrent runs cannot both succeed here whatever the parent's history is.
  if ! mkdir "$claim" 2>/dev/null; then
    if [[ -e "$claim" ]]; then
      echo "FATAL: $dir is ALREADY CLAIMED by another measurement session." >&2
      echo "       $claim exists, so a concurrent (or previous) run owns this directory." >&2
      if [[ -r "$claim/owner" ]]; then
        echo "       Claimed by: $(cat "$claim/owner" 2>/dev/null)" >&2
      fi
      echo "       Two sessions writing one directory interleave their session pins and rep" >&2
      echo "       artifacts, and the reporter cannot tell — it reads whatever rep files are" >&2
      echo "       present, so it would assemble a median across MIXED SESSIONS and report it" >&2
      echo "       as one (#3272 F3). Name a different --out, or wait for that run to finish." >&2
    else
      echo "FATAL: cannot claim $dir — $claim could not be created." >&2
      echo "       The claim is what makes this directory exclusively this session's; without" >&2
      echo "       it two concurrent runs would both measure into it (#3272 F3). Refused" >&2
      echo "       rather than proceeding unclaimed: an unenforced exclusion is not one." >&2
    fi
    exit 2
  fi
  # Recorded so the refusal above can NAME the owner rather than merely asserting there is one.
  printf 'pid=%s host=%s started=%s out=%s (%s)\n' \
    "$$" "$(hostname 2>/dev/null || echo unknown)" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$dir" "$how" > "$claim/owner"
}
# create_out_dir <out-dir-or-empty> <default-base> — create AND claim the session directory,
# echoing the path that was claimed.
#
# The caller assigns the echoed path (`OUT_DIR="$(create_out_dir "$OUT_DIR" "$base")"`), because
# the DEFAULT branch chooses the name and the caller must learn it.
create_out_dir() {
  local out_dir="${1:-}" base="$2" try=0 ts
  if [[ -n "$out_dir" ]]; then
    # An EXPLICIT dir, already checked ABOVE the argument boundary for the used-directory case.
    # Absent => create it; present-and-EMPTY => use it (a dir made by an operator or a wrapper).
    #
    # `mkdir -p` STAYS on this line deliberately: the exclusion is `claim_out_dir` below, not this
    # create. Refusing an existing directory here would break R1's accepted case (an
    # existing-but-empty dir), and `mkdir` without `-p` cannot both create a nested path and
    # arbitrate — which is precisely why the claim is a level down.
    mkdir -p "$out_dir" || { echo "FATAL: cannot create --out $out_dir" >&2; exit 2; }
    claim_out_dir "$out_dir" "explicit --out"
    printf '%s\n' "$out_dir"
    return 0
  fi
  # THE DEFAULT NAME. A UTC second plus `$$` plus a retry counter: the pid makes two runs
  # started in the same second distinct even on the same host, and `mkdir` without `-p`
  # arbitrates whatever the name does not — so uniqueness rests on an ATOMIC CREATE, never on
  # the name being clever enough.
  mkdir -p "$base" || { echo "FATAL: cannot create $base" >&2; exit 2; }
  while :; do
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    if [[ "$try" -eq 0 ]]; then
      out_dir="$base/$ts-$$"
    else
      out_dir="$base/$ts-$$-$try"
    fi
    # `mkdir` WITHOUT `-p`: an existing dir is an ERROR here, which is the exclusion.
    if mkdir "$out_dir" 2>/dev/null; then
      break
    fi
    try=$((try + 1))
    if [[ "$try" -gt 64 ]]; then
      echo "FATAL: could not create a unique output dir under $base after 64 attempts" >&2
      exit 2
    fi
  done
  # The SAME claim on the default branch too, so there is ONE mechanism rather than two. Here it
  # is strictly redundant with the unique-name create above — but it closes that branch's own
  # narrow window (a second run handed this dir EXPLICITLY before the first writes an artifact),
  # and a single mechanism is what keeps a future edit from re-splitting the two paths (#3272 F3).
  claim_out_dir "$out_dir" "default name"
  printf '%s\n' "$out_dir"
}
