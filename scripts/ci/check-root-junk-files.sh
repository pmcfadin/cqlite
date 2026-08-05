#!/usr/bin/env bash
# check-root-junk-files.sh — refuse an ACCIDENTAL-REDIRECT ARTIFACT at the repo root
# (issue #3272 review round 5, F5).
#
# # The defect this exists for, and why it needs a MECHANISM rather than a deletion
#
# An empty file named `0` was committed at the repo root of `issue-3272-harden-ws0-rig`
# THREE times: removed in `bd1b4c363`, re-added by `a3e42773b`, removed again in
# `06c295289`. Nothing in the committed rig produces it — a grep for a
# redirect-to-a-literal-`0` idiom across `scripts/perf/` and `scripts/tests/` finds
# nothing. It is the residue of an AD-HOC SHELL REDIRECT typed during a fix round
# (`… 2>0`, `… >0`, a mistyped `2>&1`), which leaves an empty file in `$PWD`, and a
# later `git add -A`/`git add .` then captured it.
#
# So deleting the file is not the fix: the fix is that the NEXT one cannot land.
#
# # THE VERDICT-BEARING SUBJECT IS THE TRACKED ROOT ONLY (#3272 review round 8)
#
# This guard originally FAILED on both states. The untracked half is now DEMOTED to a
# NON-FAILING NOTICE, because it was OBSERVED reddening the gate of record on debris that was
# never anyone's diff:
#
#   On the LINUX gate of record at the certified SHA, 30 of 31 components PASSED and this one
#   FAILED. The artifact was a file named `720` at the repo root: UNTRACKED, never committed,
#   and ALREADY GONE by the time the run was inspected — transient debris created and removed
#   by a CONCURRENT STEP OF THE SAME GATE. Re-running the scan on that same box printed
#   `root-junk: PASS`.
#
# A guard whose subject is the LIVE working tree can be tripped by a peer step of the very run
# it is certifying, so it reds the gate of record at random over content no author can act on —
# and a guard people learn to waive is worse than no guard (this issue's own doctrine). The fix
# is to NARROW THE SUBJECT, deliberately NOT to re-check after a settle delay: a retry window
# would make the verdict timing-dependent, which is a worse property than a narrower subject.
#
# So the two states are now handled differently, and this file no longer claims to guard the
# untracked root:
#
#   * TRACKED at the root — FAILS, loudly, naming the file. This is the state the defect
#     actually reached (twice) and the only one that can SHIP. It is committed content, so it
#     is attributable to the diff and no concurrent step of a gate can manufacture it.
#   * UNTRACKED at the root — a NOTICE on stdout, carrying no verdict and unable to change the
#     exit status. Still reported rather than dropped, because the information is actionable by
#     whoever is looking (`rm ./<name>` before a blanket `git add` captures it) and silence is
#     not the third option — but it is not a gate verdict, because the gate cannot attribute it.
#
# # What counts as junk, and why the shape list is NARROW
#
# A guard that reds on a legitimate file is the guard someone deletes, so the predicate is
# the SHAPE OF A REDIRECT TARGET and nothing else:
#
#   * a BARE INTEGER name (`0`, `1`, `2`, `12`) — `cmd 2>0`, `cmd >1`. A file whose entire
#     name is a file descriptor number is never something anyone meant to write.
#   * an `&`-PREFIXED integer (`&1`, `&2`) — `cmd >&1` / `cmd 2> &1`, the mistyped
#     `2>&1`, which in several shells creates a file literally called `&1`.
#   * a name made ONLY of redirect punctuation and digits (`>`, `>>`, `2>`, `|`) — the
#     residue of a quoted or escaped operator.
#
# Deliberately NOT flagged, because each is a real thing a repo may hold: any name with a
# letter, a dot, a dash or an underscore in it (`2026-report.md`, `v2`, `0.14.0.md`), and
# anything below the root (`docs/reports/0` would be a deliberate act, and this file's
# subject is the root because that is where an ad-hoc `cd`-less redirect lands).
#
# # Hermetic, and OBSERVED in both directions
#
# `--self-test` builds a throwaway git repo under `$TMPDIR`, plants each junk shape and
# requires the TRACKED half to FAIL, requires the UNTRACKED half NOT to fail while still being
# NOTICED, plants the legitimate look-alikes and requires a PASS, and asserts the two halves
# SEPARATELY — per #3249 a guard that has not been observed firing is not evidence, and per
# round 8 a half that has been DEMOTED must be observed NOT firing, or the demotion is a claim
# rather than a fact. It also drives a `git` SHIM that FAILS the enumeration, which must FAIL
# the guard rather than green it (#3272 F4), paired with a frozen replica of the pre-fix loop
# observed to have been fail-open.
#
# It needs `git` and `mktemp` (coreutils, present wherever git is) and nothing else: no python3,
# no network, no cargo. `mktemp` was added by the F4 fix — the enumeration is written to a FILE so
# its exit status is observable, because command substitution STRIPS NUL BYTES and `-z` output is
# NUL-separated. Its failure is handled as a FAILURE, so neither dependency introduces a SKIP
# PATH and there is still no way for this guard to record a vacuous success.
#
# Usage:
#   scripts/ci/check-root-junk-files.sh [<repo-root>]   # scan (default: this checkout)
#   scripts/ci/check-root-junk-files.sh --self-test     # drive both directions, then scan
set -uo pipefail

# is_junk_name <name> — 0 when <name> is an accidental-redirect shape.
#
# Stated as three ANCHORED patterns rather than one clever regex so each shape is
# individually readable, and so adding one is a visible decision.
is_junk_name() {
  local n="$1"
  case "$n" in
    # a name made only of digits: `0`, `2`, `12`
    *[!0-9]*) ;;
    "") return 1 ;;
    *) return 0 ;;
  esac
  # `&1`, `&2` — a mistyped `2>&1`
  case "$n" in
    "&"*) case "${n#&}" in "" | *[!0-9]*) ;; *) return 0 ;; esac ;;
  esac
  # a name made ONLY of redirect punctuation (and digits): `>`, `>>`, `2>`, `|`, `<`
  case "$n" in
    *[!0-9\>\<\|\&]*) ;;
    "") ;;
    *) return 0 ;;
  esac
  return 1
}

# root_junk <repo-root> — one `<state>\t<name>` line per junk file at the root.
#
# Two INDEPENDENT sources, because they are two different states of the same defect:
#   * `git ls-files --others --exclude-standard` — UNTRACKED (pre-`git add`).
#   * `git ls-files` — TRACKED (already committed).
# Both are restricted to depth 1 (`*/*` excluded), which is the subject.
#
# Both are still ENUMERATED here; the distinction between them is drawn by the CALLER, once:
# `scan` fails on `tracked` and only NOTICES `untracked` (round 8, above). Keeping the
# enumeration whole and the verdict in one place means there is exactly one line to read to
# know which state is verdict-bearing, rather than a silently half-blind enumerator.
#
# # THE ENUMERATION'S STATUS IS CHECKED, NOT DISCARDED (#3272 review round 7, F4)
#
# Each `git ls-files` used to run inside a PROCESS SUBSTITUTION with its stderr suppressed:
#
#     done < <(git "${args[@]}" 2>/dev/null)
#
# A process substitution's exit status is UNAVAILABLE to the shell — it is not in
# `PIPESTATUS`, `$?` is the `while` loop's, and `set -o pipefail` does not apply. So a `git`
# that FAILED produced no lines, the `while` body never ran, `root_junk` printed nothing, and
# `scan` reported the AFFIRMATIVE "no accidental-redirect artifact at the root" — over a subject
# that was never enumerated. Exactly the fail-open shape this guard exists to catch, inside the
# guard that caught a live recurrence: it reported CLEAN because it had looked at NOTHING.
#
# Fixed by REDIRECTING each enumeration TO A FILE, where the status IS observable, then reading
# that file. `scan` can then distinguish "enumerated, found nothing" from "could not enumerate" —
# the two states that used to print identically.
#
# # Why a FILE and not a variable (found by this file's own self-test)
#
# The obvious smaller fix, `raw="$(git … )" || rc=$?`, is WRONG here and the self-test caught it
# immediately: COMMAND SUBSTITUTION STRIPS NUL BYTES (bash warns "ignored null byte in input"),
# and `-z` output is NUL-separated — so capturing it collapses every path into one unsplittable
# blob and 12 of 22 checks failed. `-z` cannot be dropped in exchange: a newline-separated
# enumeration makes a path's FIRST LINE stand in for the path, which is the membership defect
# recorded in CLAUDE.md's roborev notes (a `grep -Fxq` over newline-delimited paths producing a
# genuine false PASS). A file preserves the bytes exactly AND makes the status checkable.
#
# `mktemp` is the one addition to this guard's dependencies. Recorded rather than glossed: the
# header says "needs git and nothing else, so there is NO SKIP PATH". `mktemp` is coreutils —
# present wherever git is — and its failure is handled as a FAILURE below, so it introduces no
# skip path either.
#
# Returns: 0 with the findings on stdout; 2 (and a diagnostic on stderr) if EITHER enumeration
# failed. Deliberately NOT 1: `scan` returns 1 for "junk found", and a failure to look is a
# different verdict from a finding.
ROOT_JUNK_RC_ERROR=2
root_junk() {
  local root="$1" rel state rc tmpf errf
  tmpf="$(mktemp)" || {
    echo "FAIL: could not create a temp file to enumerate the root of $root." >&2
    echo "      The enumeration is written to a file so its EXIT STATUS is observable;" >&2
    echo "      without that, a failing git produces no lines and reads as a clean root" >&2
    echo "      (#3272 F4). Refused rather than falling back to an unstatused read." >&2
    return "$ROOT_JUNK_RC_ERROR"
  }
  errf="$(mktemp)" || { rm -f "$tmpf"; echo "FAIL: could not create a temp file for git's stderr." >&2; return "$ROOT_JUNK_RC_ERROR"; }
  for state in untracked tracked; do
    local -a args=(-C "$root" ls-files -z)
    [ "$state" = untracked ] && args+=(--others --exclude-standard)
    # REDIRECTED, so the status is a plain command status. stderr is KEPT rather than discarded —
    # a `git` that explains itself should not have that explanation thrown away.
    rc=0
    git "${args[@]}" >"$tmpf" 2>"$errf" || rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "FAIL: could not enumerate the $state files at the root of $root:" >&2
      echo "      git ls-files exited $rc: $(tr '\n' ' ' < "$errf")" >&2
      echo "      This is a FAILURE and not an empty result. The enumeration used to run inside" >&2
      echo "      a process substitution whose status is unavailable to the shell (not in" >&2
      echo "      PIPESTATUS, not \$?, unaffected by pipefail), so a failing git produced no" >&2
      echo "      lines and the scan printed 'no accidental-redirect artifact' over a subject it" >&2
      echo "      had never enumerated (#3272 F4)." >&2
      rm -f "$tmpf" "$errf"
      return "$ROOT_JUNK_RC_ERROR"
    fi
    while IFS= read -r -d '' rel; do
      case "$rel" in */*) continue ;; esac
      is_junk_name "$rel" || continue
      printf '%s\t%s\n' "$state" "$rel"
    done < "$tmpf"
  done
  rm -f "$tmpf" "$errf"
}

# scan <repo-root> — exit 1 naming every TRACKED junk file, else 0.
#
# The untracked findings are reported as a NOTICE and cannot change the exit status (round 8,
# see the header): the verdict-bearing subject is committed content, which is attributable to a
# diff, and not the live working tree, which a concurrent step of the same gate can dirty.
scan() {
  local root="$1" found rc tracked untracked
  if ! git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
    echo "FAIL: $root is not a git checkout, so the root-junk subject cannot be" >&2
    echo "      enumerated — an unenumerated subject prints exactly like a clean one." >&2
    return 1
  fi
  # The status is checked BEFORE the emptiness (#3272 F4). Checking emptiness first would put
  # the failure case straight into the "clean root" branch, which is the defect: an enumeration
  # that FAILED and one that found nothing produce the same empty string, and only the status
  # tells them apart. `root_junk`'s own diagnostic is already on stderr.
  found="$(root_junk "$root")" || rc=$?
  rc="${rc:-0}"
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the root-junk subject could not be ENUMERATED, so this checkout is" >&2
    echo "      UNVERIFIED rather than clean. A guard that cannot look must not report a" >&2
    echo "      pass (#3272 F4)." >&2
    return 1
  fi
  tracked=""
  untracked=""
  if [ -n "$found" ]; then
    # `^tracked` cannot match `untracked` — the anchor keeps the two states apart.
    tracked="$(printf '%s\n' "$found" | grep '^tracked' || true)"
    untracked="$(printf '%s\n' "$found" | grep '^untracked' || true)"
  fi

  # The UNTRACKED half: reported, never a verdict (round 8). Worded so no reader can mistake it
  # for one — no PASS/FAIL token, and it says outright that it does not affect the result.
  if [ -n "$untracked" ]; then
    echo "root-junk notice (not a verdict, does not affect the exit status): untracked" \
         "redirect-shaped file(s) at the root of $root:"
    printf '%s\n' "$untracked" | while IFS=$'\t' read -r state name; do
      echo "         ./$name"
    done
    echo "         These are NOT gate-failing: the working tree is shared with whatever else is"
    echo "         running, and a concurrent step of a gate has been observed creating and"
    echo "         removing exactly this kind of debris mid-run (a transient './720' on the Linux"
    echo "         gate of record, #3272 round 8). Only TRACKED files carry a verdict here."
    echo "         Worth clearing anyway (\`rm ./<name>\`) so a blanket \`git add\` cannot commit one."
  fi

  if [ -z "$tracked" ]; then
    # AFFIRMATIVE, not the absence of a complaint: the line states that the subject was
    # enumerated, so a pasted log shows the check RAN. It names the verdict-bearing half
    # explicitly, so it cannot be read as a claim about the untracked root.
    echo "root-junk: PASS — no TRACKED accidental-redirect artifact at the root of $root" \
         "(the tracked root enumerated; the untracked root is reported as a notice only)"
    return 0
  fi
  echo "FAIL: TRACKED accidental-redirect artifact(s) at the repo root:" >&2
  printf '%s\n' "$tracked" | while IFS=$'\t' read -r state name; do
    echo "      [$state] $name" >&2
  done
  cat >&2 <<'EOF'
      A file whose entire name is a file descriptor number (`0`, `2`), an `&`-prefixed
      one (`&1`), or bare redirect punctuation is the residue of an ad-hoc shell
      redirect — `cmd 2>0`, a mistyped `2>&1` — not something anyone meant to write.
      On this repo an empty `0` reached the tree TWICE, re-added each time by a
      `git add -A` that swept it up (#3272 F5). That is COMMITTED content: it is in the
      diff, it ships, and it is what this guard refuses.
      Fix: `git rm --cached ./<name>` then `rm ./<name>`, and STAGE EXPLICIT PATHS
      rather than `git add -A`/`git add .`.
EOF
  return 1
}

# ---------------------------------------------------------------------------
# --self-test: the guard OBSERVED to fire, and observed to stay silent
# ---------------------------------------------------------------------------
# A `trap … RETURN` here would be a VACUITY BUG, and it was one: bash fires a RETURN trap
# on EVERY function return in the enclosing context, not only this function's, so
# `rm -rf "$tmp"` ran the first time `scan` (or `_ok`) returned. The probe repo was then
# GONE for every case after the first, `scan` failed because there was no repo, and each
# "OBSERVED … FAILS" case passed for the wrong reason — the guard was never observed at all.
# MEASURED: 7 of 22 checks failed only because the diagnostic assertions could still tell the
# difference; without those, the whole suite would have read green having tested nothing.
# So the cleanup is EXPLICIT, at the single exit point.
self_test() {
  local tmp fails=0 checks=0 rc=0
  tmp="$(mktemp -d)" || { echo "FAIL - could not create a temp dir"; return 1; }
  local repo="$tmp/probe"
  mkdir -p "$repo/docs"
  git -C "$repo" init -q 2>/dev/null || git init -q "$repo" 2>/dev/null
  git -C "$repo" config user.email probe@example.com
  git -C "$repo" config user.name probe
  printf 'x\n' > "$repo/README.md"
  git -C "$repo" add README.md
  git -C "$repo" commit -qm init

  _ok() { checks=$((checks + 1)); echo "ok   - $1"; }
  _no() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

  # 0. the CLEAN direction first, so a guard that reds unconditionally cannot pass the
  #    firing cases below.
  if scan "$repo" >/dev/null 2>&1; then
    _ok "self-test: a clean root PASSES (the guard is not unconditional)"
  else
    _no "self-test: a clean root must PASS (got: $(scan "$repo" 2>&1))"
  fi

  # 1. the UNTRACKED half — DEMOTED to a notice (round 8), so it is observed NOT FAILING.
  #
  # A demotion asserted in a comment is a claim; a demotion driven here is a fact. Both
  # directions are required of every shape, because either alone would be satisfiable by a
  # broken guard: the exit status must stay 0 (a transient artifact of a concurrent gate step
  # cannot red the gate of record — the `./720` observation in the header), AND the finding must
  # still be REPORTED (silence is not the alternative to a verdict; the information stays
  # actionable for whoever is looking).
  #
  # The assertions CAPTURE the output instead of piping it, and that is a correctness
  # requirement, not a style choice. Two ways the obvious spelling `scan … | grep -qF …` is
  # broken here, and BOTH were measured on this file:
  #
  #   * `set -o pipefail` (this file's own option) makes a pipeline return the RIGHTMOST
  #     NON-ZERO status, and `scan` exits 1 on a TRACKED finding BY DESIGN — so the pipeline
  #     returned 1 even when grep MATCHED, and the state-naming assertions took their failing
  #     branch unconditionally. They could never pass, i.e. they asserted nothing about the
  #     guard: the FAIL they printed was about the pipeline.
  #   * `[untracked]` in a BASIC REGEX is a CHARACTER CLASS matching ONE character, so a
  #     `grep -q` would not have matched the literal bracketed state anyway. Hence `-F`.
  #
  # This is the same shape as an earlier finding on this rig — an assertion whose verdict comes
  # from something other than the thing under test.
  local shape out
  for shape in 0 2 12 '&1' '>'; do
    : > "$repo/$shape"
    if scan "$repo" >/dev/null 2>&1; then
      _ok "self-test: OBSERVED (round 8) — an UNTRACKED root file named '$shape' does NOT fail the scan (a concurrent gate step can create one; only TRACKED files carry a verdict)"
    else
      _no "self-test: round 8 — an UNTRACKED root file named '$shape' must NOT fail the scan (got: $(scan "$repo" 2>&1))"
    fi
    # ...and it must still be REPORTED, or the demotion became silence.
    out="$(scan "$repo" 2>&1)"
    if grep -qF "./$shape" <<<"$out"; then
      _ok "self-test: the untracked '$shape' is still REPORTED (demoted, not dropped)"
    else
      _no "self-test: the untracked '$shape' must still be reported as a notice (got: $out)"
    fi
    # ...as a NOTICE that no reader can mistake for a verdict: it must say so, and it must not
    # print the word FAIL.
    if grep -qF 'not a verdict' <<<"$out" && ! grep -qF 'FAIL' <<<"$out"; then
      _ok "self-test: the untracked notice for '$shape' declares itself non-verdict-bearing and prints no FAIL token"
    else
      _no "self-test: the untracked notice for '$shape' must be unmistakably non-verdict-bearing (got: $out)"
    fi
    rm -f "$repo/$shape"
  done

  # 2. the TRACKED half — the state `0` actually reached, TWICE. This is the verdict-bearing
  #    subject after round 8's narrowing, so it is the one case that MUST still red the gate.
  : > "$repo/0"
  git -C "$repo" add ./0
  git -C "$repo" commit -qm 'the defect'
  if scan "$repo" >/dev/null 2>&1; then
    _no "self-test: a TRACKED root file named '0' must FAIL the scan"
  else
    _ok "self-test: OBSERVED — a TRACKED root '0' FAILS (the state it reached twice; still the verdict after round 8's narrowing)"
  fi
  out="$(scan "$repo" 2>&1)"
  if grep -qF '[tracked] 0' <<<"$out"; then
    _ok "self-test: the finding names it as TRACKED (distinct from the untracked half)"
  else
    _no "self-test: the finding must name the tracked state (got: $out)"
  fi
  # ...and the demotion of the untracked half must not SWALLOW a tracked finding when both are
  # present. Without this, round 8's narrowing could have been an over-broad filter that greened
  # the very case the guard exists for whenever any untracked debris happened to be lying around.
  : > "$repo/2"
  out="$(scan "$repo" 2>&1)"; rc=$?
  if [ "$rc" -ne 0 ] && grep -qF '[tracked] 0' <<<"$out"; then
    _ok "self-test: OBSERVED (round 8) — a TRACKED '0' still FAILS while UNTRACKED debris is also present (the notice does not swallow the verdict)"
  else
    _no "self-test: round 8 — a tracked finding must survive concurrent untracked debris (rc=$rc, out: $out)"
  fi
  rm -f "$repo/2"
  git -C "$repo" rm -q --cached ./0 >/dev/null 2>&1
  rm -f "$repo/0"
  git -C "$repo" commit -qm 'removed'

  # 3. the SILENT direction, over names a repo legitimately holds. A guard that reds on
  #    these is the guard someone deletes, so each is driven rather than reasoned about.
  #
  # These are planted COMMITTED, not untracked, and that is REQUIRED after round 8: an untracked
  # plant can no longer fail the scan for ANY name, so an untracked spelling of this case would
  # pass identically whether `is_junk_name` flagged the name or not — a check that asserts
  # nothing. Committing them puts each name on the one path that still carries a verdict, which
  # is the only place non-flagging is observable.
  for shape in 2026-report.md v2 0.14.0.md '0x' 'a0' '_0' 'CHANGELOG-2.md'; do
    : > "$repo/$shape"
    git -C "$repo" add -- "./$shape"
    git -C "$repo" commit -qm "legit $shape"
    if scan "$repo" >/dev/null 2>&1; then
      _ok "self-test: a legitimate TRACKED root file named '$shape' is NOT flagged"
    else
      _no "self-test: '$shape' must not be flagged (got: $(scan "$repo" 2>&1))"
    fi
    git -C "$repo" rm -q --cached -- "./$shape" >/dev/null 2>&1
    rm -f "$repo/$shape"
    git -C "$repo" commit -qm "drop $shape"
  done

  # 4. the SUBJECT IS THE ROOT, stated by driving it: a `0` one directory down is a
  #    deliberate act and outside this guard's subject. COMMITTED for the same reason as case 3 —
  #    untracked, the depth restriction would be untestable because nothing untracked can fail.
  : > "$repo/docs/0"
  git -C "$repo" add -- ./docs/0
  git -C "$repo" commit -qm 'nested 0'
  if scan "$repo" >/dev/null 2>&1; then
    _ok "self-test: a TRACKED '0' BELOW the root is not flagged (the subject is the root)"
  else
    _no "self-test: docs/0 must not be flagged (got: $(scan "$repo" 2>&1))"
  fi
  git -C "$repo" rm -q --cached -- ./docs/0 >/dev/null 2>&1
  rm -f "$repo/docs/0"
  git -C "$repo" commit -qm 'drop nested 0'

  # 5. VACUITY: a non-repo path must FAIL rather than report a clean root, because an
  #    unenumerable subject prints exactly like an empty one.
  if scan "$tmp/not-a-repo" >/dev/null 2>&1; then
    _no "self-test: a non-git path must FAIL, not read as a clean root"
  else
    _ok "self-test: OBSERVED — a non-git path FAILS (an unenumerable subject is not clean)"
  fi

  # 6. A FAILING `git ls-files` MUST FAIL THE GUARD (#3272 review round 7, F4).
  #
  # The `rev-parse` probe in case 5 catches a non-repo, and that is a DIFFERENT failure: it runs
  # before the enumeration and is a plain `if !`. F4 is about the enumeration ITSELF failing on a
  # path that IS a repo — which the process-substitution form could not observe at all (a process
  # substitution's status is not in PIPESTATUS, is not `$?`, and is unaffected by pipefail), so a
  # failing `git ls-files` produced no lines and `scan` printed its AFFIRMATIVE clean line.
  #
  # Driven with a `git` SHIM on PATH that succeeds for `rev-parse` (so the repo probe passes and
  # the run reaches the enumeration) and FAILS for `ls-files`. That isolates F4 from case 5.
  local shim_bin="$tmp/shim"
  mkdir -p "$shim_bin"
  cat > "$shim_bin/git" <<'SHIM'
#!/usr/bin/env bash
# Succeed for the repo probe; FAIL for the enumeration. `-C <dir>` may precede the subcommand.
for a in "$@"; do
  case "$a" in
    rev-parse) exit 0 ;;
    ls-files) echo "fatal: simulated ls-files failure (#3272 F4 probe)" >&2; exit 128 ;;
  esac
done
exit 0
SHIM
  chmod +x "$shim_bin/git"
  # The shim must genuinely behave as described, or the case below proves nothing about the guard.
  if PATH="$shim_bin:$PATH" git -C "$repo" rev-parse --git-dir >/dev/null 2>&1 \
     && ! PATH="$shim_bin:$PATH" git -C "$repo" ls-files -z >/dev/null 2>&1; then
    _ok "self-test: the F4 probe shim is correct — rev-parse SUCCEEDS and ls-files FAILS (so the run reaches the enumeration)"
  else
    _no "self-test: the F4 shim must pass rev-parse and fail ls-files, else the case below proves nothing"
  fi
  out="$(PATH="$shim_bin:$PATH" scan "$repo" 2>&1)"; rc=$?
  if [ "$rc" -ne 0 ]; then
    _ok "self-test: OBSERVED (round7 F4) — a FAILING git ls-files makes the scan exit NON-ZERO (pre-fix: the process substitution's status was unobservable, so it printed a clean root)"
  else
    _no "self-test: round7 F4 — a failing git ls-files must FAIL the scan (rc=$rc, out: $out)"
  fi
  # ...and it must NOT print the affirmative clean line, which is the specific fail-open text.
  if ! grep -qF 'root-junk: PASS' <<<"$out"; then
    _ok "self-test: OBSERVED (round7 F4) — the failing enumeration does NOT print the affirmative 'root-junk: PASS' line"
  else
    _no "self-test: round7 F4 — a failed enumeration must never print the clean line (out: $out)"
  fi
  # ...and the diagnostic must NAME the enumeration as the cause, or a reader is sent after the
  # wrong thing — the same property every other refusal in this rig is held to.
  if grep -qF 'could not enumerate' <<<"$out"; then
    _ok "self-test: the F4 diagnostic NAMES the failed enumeration (not a generic error)"
  else
    _no "self-test: round7 F4 — the diagnostic must name the enumeration failure (out: $out)"
  fi
  # THE CONTROL: with the shim OFF, the very same repo scans clean. Without this, the three cases
  # above could be satisfied by a guard that fails on this repo for some unrelated reason.
  if scan "$repo" >/dev/null 2>&1; then
    _ok "self-test: the CONTROL — the same repo scans CLEAN without the shim (so F4's failure is the shim, not the repo)"
  else
    _no "self-test: round7 F4 — the repo must scan clean without the shim (got: $(scan "$repo" 2>&1))"
  fi
  # NON-VACUITY, driven rather than asserted: the PRE-FIX form really was fail-open. A frozen
  # replica of the removed loop, run against the SAME shim — it must exit 0 with an EMPTY result
  # from a `git` that exited 128, which is what made `scan` print its clean line. Without this
  # half, the cases above could be about an input that was never a bypass.
  #
  # A frozen historical replica: never called by the guard, and never to be "kept in sync".
  prefix_root_junk_prefix4() {
    local root="$1" rel state
    for state in untracked tracked; do
      local -a args=(-C "$root" ls-files -z)
      [ "$state" = untracked ] && args+=(--others --exclude-standard)
      while IFS= read -r -d '' rel; do
        printf '%s\t%s\n' "$state" "$rel"
      done < <(git "${args[@]}" 2>/dev/null)
    done
  }
  local pre_out pre_rc
  pre_out="$(PATH="$shim_bin:$PATH" prefix_root_junk_prefix4 "$repo")"; pre_rc=$?
  if [ "$pre_rc" -eq 0 ] && [ -z "$pre_out" ]; then
    _ok "self-test: NON-VACUITY (round7 F4) — the PRE-FIX process-substitution form exits 0 with an EMPTY result from a git that exited 128 (this is the fail-open that printed a clean root)"
  else
    _no "self-test: round7 F4 — the pre-fix form must have been fail-open, else the cases above prove nothing (rc=$pre_rc, out: $pre_out)"
  fi
  rm -f "$shim_bin/git"

  # 6. the CHECK-COUNT FLOOR: a block that silently never ran would leave `checks` short,
  #    and a suite that asserts nothing exits 0 exactly like one that asserted everything.
  # 22 → 27 with round 7's six F4 cases → 33 with round 8's narrowing (#3272). DERIVED from the
  # observed count, not lowered to fit: round 8 added a third assertion to each of the five
  # untracked shapes (+5, they must now be observed NOT failing AND still reporting AND
  # self-declaring as non-verdict) and one no-swallow case (+1), so 28 checks became 34. The
  # floor is one below that, so adding a case does not red the guard while a block that stops
  # running does.
  local min=33
  if [ "$checks" -lt "$min" ]; then
    echo "FAIL - only $checks checks RAN (expected at least $min) — a block never executed"
    fails=$((fails + 1))
  else
    echo "ok   - $checks checks ran (floor $min)"
  fi

  echo "root-junk self-test: $checks checks, $fails failure(s)"
  [ "$fails" -eq 0 ] || rc=1
  rm -rf "$tmp"
  return "$rc"
}

main() {
  local root
  if [ "${1:-}" = "--self-test" ]; then
    self_test || return 1
    # ...and then the REAL subject, so `--self-test` is a superset of the plain scan and a
    # gate hook cannot certify the probe repo while leaving this checkout unexamined.
    root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    scan "$root"
    return
  fi
  root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
  scan "$root"
}

main "$@"
