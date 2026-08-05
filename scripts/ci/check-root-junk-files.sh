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
# So deleting the file is not the fix: the fix is that the NEXT one cannot land. Both
# halves of the sequence are covered, because both are how it actually happened:
#
#   * UNTRACKED at the root — the state the file is in the moment before a `git add -A`
#     sweeps it up. Caught here it never becomes a commit.
#   * TRACKED at the root — the state it reached twice. Caught here it cannot survive a
#     gate run, whichever `git add` spelling put it there.
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
# requires a FAIL, plants the legitimate look-alikes and requires a PASS, and asserts the
# untracked and tracked halves SEPARATELY — per #3249 a guard that has not been observed
# firing is not evidence. It needs `git` and nothing else: no python3, no network, no
# cargo, so there is NO SKIP PATH and therefore no way for it to record a vacuous success.
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
# Two INDEPENDENT sources, because they are two different states of the same defect and a
# check that saw only one would have missed the way it actually landed:
#   * `git ls-files --others --exclude-standard` — UNTRACKED (pre-`git add`).
#   * `git ls-files` — TRACKED (already committed).
# Both are restricted to depth 1 (`*/*` excluded), which is the subject.
root_junk() {
  local root="$1" rel state
  for state in untracked tracked; do
    local -a args=(-C "$root" ls-files -z)
    [ "$state" = untracked ] && args+=(--others --exclude-standard)
    while IFS= read -r -d '' rel; do
      case "$rel" in */*) continue ;; esac
      is_junk_name "$rel" || continue
      printf '%s\t%s\n' "$state" "$rel"
    done < <(git "${args[@]}" 2>/dev/null)
  done
}

# scan <repo-root> — exit 0 when the root is clean, 1 naming every junk file otherwise.
scan() {
  local root="$1" found
  if ! git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
    echo "FAIL: $root is not a git checkout, so the root-junk subject cannot be" >&2
    echo "      enumerated — an unenumerated subject prints exactly like a clean one." >&2
    return 1
  fi
  found="$(root_junk "$root")"
  if [ -z "$found" ]; then
    # AFFIRMATIVE, not the absence of a complaint: the line states that the subject was
    # enumerated, so a pasted log shows the check RAN.
    echo "root-junk: PASS — no accidental-redirect artifact at the root of $root" \
         "(both the untracked and the tracked halves enumerated)"
    return 0
  fi
  echo "FAIL: accidental-redirect artifact(s) at the repo root:" >&2
  printf '%s\n' "$found" | while IFS=$'\t' read -r state name; do
    echo "      [$state] $name" >&2
  done
  cat >&2 <<'EOF'
      A file whose entire name is a file descriptor number (`0`, `2`), an `&`-prefixed
      one (`&1`), or bare redirect punctuation is the residue of an ad-hoc shell
      redirect — `cmd 2>0`, a mistyped `2>&1` — not something anyone meant to write.
      On this repo an empty `0` reached the tree TWICE, re-added each time by a
      `git add -A` that swept it up (#3272 F5).
      Fix: `rm ./<name>` (untracked) or `git rm --cached ./<name>` then `rm ./<name>`
      (tracked), and STAGE EXPLICIT PATHS rather than `git add -A`/`git add .`.
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

  # 1. the UNTRACKED half — the state the file is in before a `git add -A`.
  #
  # The diagnostic assertion CAPTURES the output instead of piping it, and that is a
  # correctness requirement, not a style choice. Two ways the obvious spelling
  # `scan … | grep -qF …` is broken here, and BOTH were measured on this file:
  #
  #   * `set -o pipefail` (this file's own option) makes a pipeline return the RIGHTMOST
  #     NON-ZERO status, and `scan` exits 1 on a finding BY DESIGN — so the pipeline
  #     returned 1 even when grep MATCHED, and all six state-naming assertions took their
  #     failing branch unconditionally. They could never pass, i.e. they asserted nothing
  #     about the guard: the FAIL they printed was about the pipeline.
  #   * `[untracked]` in a BASIC REGEX is a CHARACTER CLASS matching ONE character, so a
  #     `grep -q` would not have matched the literal bracketed state anyway. Hence `-F`.
  #
  # This is the same shape as the finding this whole round is about — an assertion whose
  # verdict comes from something other than the thing under test.
  local shape out
  for shape in 0 2 12 '&1' '>'; do
    : > "$repo/$shape"
    if scan "$repo" >/dev/null 2>&1; then
      _no "self-test: an UNTRACKED root file named '$shape' must FAIL the scan"
    else
      _ok "self-test: OBSERVED — an UNTRACKED root file named '$shape' FAILS"
    fi
    # ...and the diagnostic must name the state, or a reader cannot act on it.
    out="$(scan "$repo" 2>&1)"
    if grep -qF "[untracked] $shape" <<<"$out"; then
      _ok "self-test: the finding for '$shape' names it as UNTRACKED"
    else
      _no "self-test: the finding for '$shape' must name the untracked state (got: $out)"
    fi
    rm -f "$repo/$shape"
  done

  # 2. the TRACKED half — the state `0` actually reached, TWICE.
  : > "$repo/0"
  git -C "$repo" add ./0
  git -C "$repo" commit -qm 'the defect'
  if scan "$repo" >/dev/null 2>&1; then
    _no "self-test: a TRACKED root file named '0' must FAIL the scan"
  else
    _ok "self-test: OBSERVED — a TRACKED root '0' FAILS (the state it reached twice)"
  fi
  out="$(scan "$repo" 2>&1)"
  if grep -qF '[tracked] 0' <<<"$out"; then
    _ok "self-test: the finding names it as TRACKED (distinct from the untracked half)"
  else
    _no "self-test: the finding must name the tracked state (got: $out)"
  fi
  git -C "$repo" rm -q --cached ./0 >/dev/null 2>&1
  rm -f "$repo/0"
  git -C "$repo" commit -qm 'removed'

  # 3. the SILENT direction, over names a repo legitimately holds. A guard that reds on
  #    these is the guard someone deletes, so each is driven rather than reasoned about.
  for shape in 2026-report.md v2 0.14.0.md '0x' 'a0' '_0' 'CHANGELOG-2.md'; do
    : > "$repo/$shape"
    if scan "$repo" >/dev/null 2>&1; then
      _ok "self-test: a legitimate root file named '$shape' is NOT flagged"
    else
      _no "self-test: '$shape' must not be flagged (got: $(scan "$repo" 2>&1))"
    fi
    rm -f "$repo/$shape"
  done

  # 4. the SUBJECT IS THE ROOT, stated by driving it: a `0` one directory down is a
  #    deliberate act and outside this guard's subject.
  : > "$repo/docs/0"
  if scan "$repo" >/dev/null 2>&1; then
    _ok "self-test: a '0' BELOW the root is not flagged (the subject is the root)"
  else
    _no "self-test: docs/0 must not be flagged (got: $(scan "$repo" 2>&1))"
  fi
  rm -f "$repo/docs/0"

  # 5. VACUITY: a non-repo path must FAIL rather than report a clean root, because an
  #    unenumerable subject prints exactly like an empty one.
  if scan "$tmp/not-a-repo" >/dev/null 2>&1; then
    _no "self-test: a non-git path must FAIL, not read as a clean root"
  else
    _ok "self-test: OBSERVED — a non-git path FAILS (an unenumerable subject is not clean)"
  fi

  # 6. the CHECK-COUNT FLOOR: a block that silently never ran would leave `checks` short,
  #    and a suite that asserts nothing exits 0 exactly like one that asserted everything.
  local min=22
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
