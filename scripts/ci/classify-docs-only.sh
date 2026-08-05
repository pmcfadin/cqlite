#!/usr/bin/env bash
# Docs-only classifier for the required PR gate (issue #2645, epic #2636;
# NARROWED by issue #3250).
#
# pr-gate.yml is a REQUIRED status check with no path awareness: a docs- or
# board-only PR still compiles cqlite-core all-features and runs the read-time
# reconciliation oracle (#2644). This classifier lets the gate short-circuit
# such PRs to green in seconds WITHOUT using paths/paths-ignore on the workflow
# trigger — a path filter would prevent the required check from ever reporting,
# permanently blocking a PR that touches only ignored paths.
#
# Contract (pure + hermetic so it is self-testable):
#   - Reads a newline-delimited changed-file list on STDIN (one RAW
#     repo-relative path per line; blank lines ignored).
#   - Exit 0  => DOCS-ONLY: every changed file is in the conservative docs
#                allowlist. The caller MAY skip the Rust/oracle/heavy steps.
#   - Exit 1  => FULL PATH REQUIRED (fail-closed): at least one file is NOT in
#                the docs allowlist, OR the changed set is empty/ambiguous.
#
# FAIL-CLOSED by construction: this is an ALLOWLIST. Only files that are
# unambiguously documentation short-circuit; ANY other class — Rust sources,
# Cargo manifests, test-data manifests/fixtures, .github workflows/actions,
# scripts, config, lockfiles, or an unrecognized extension — forces the full
# run. An empty changed set is ambiguous and also forces the full run.
#
# Sensitive directories (.github/, scripts/, test-data/) force the full path
# EVEN for a *.md file inside them, so a Markdown edit next to a workflow, a
# gate script, or a parity manifest can never smuggle a code-relevant change
# past the gate.
#
# ---------------------------------------------------------------------------
# ISSUE #3250: A `docs/` PATH PREFIX IS NOT A VERDICT.
#
# This script used to answer documentation for ANY path under `docs/`, extension
# blind. This repository ships measurement harnesses under
# `docs/reports/<ws>-artifacts/` BY CONVENTION (owner-ruled: the convention
# stays), so that was not a corner case: three merged PRs reported `required`
# green in 13-16 s against a ~14-minute baseline, having compiled and tested
# nothing — one of them carrying a whole Cargo crate (a manifest plus
# `src/main.rs`) and 34 executables in total.
#
# A path under `docs/` is now documentation ONLY on an AFFIRMATIVE match against
# a named allowlist layer, decided in ONE place (`docs_path_is_documentation`):
#
#   L1  prose / image / legal at any depth  — this classifier's OWN semantics,
#       unchanged by #3250, including its behaviour outside `docs/`.
#   L2  INERT report artifacts at any depth under `docs/` — the inert bucket of
#       the IMPORTED artifact extension set.
#   L3  CODE-BEARING report artifacts (json, html) ONLY inside an
#       artifact-bearing directory, via the IMPORTED component-wise matcher.
#   ==> anything else — unrecognized extension, NO extension, an imported
#       extension this classifier has not bucketed — forces the full path.
#
# There is deliberately no `else return 0` and no "not obviously code => prose":
# a positive verdict requires a positive match, so every unmeasured or
# unanticipated case inherits `full`. A deny-list of executable extensions was
# rejected for the opposite reason: it is fail-OPEN, and the next extension
# somebody commits under `docs/` would be documentation by default.
#
# The extensionless rule is UNCONDITIONAL and does NOT consult git's executable
# bit (the sibling subsystem #3229 does): this script's input carries no mode, a
# mode read would need a repository and two resolvable refs, a `chmod -x` must
# not be able to move a program into the documentation class when the
# consequence is an ungated merge, and the measured cost is zero — every tracked
# extensionless file under `docs/` is a mode-100755 harness binary.
# ---------------------------------------------------------------------------
#
# The script prints a one-word verdict ("docs-only" / "full") to STDOUT and a
# human-readable reason to STDERR; callers key off the EXIT CODE.

set -euo pipefail

# ---------------------------------------------------------------------------
# THE IMPORT (issue #3250, AC5): what counts as committed run output beside a
# report is declared ONCE, in scripts/flow/roborev-review-oracles.sh (#3229).
# It is IMPORTED here, never restated — no copy of the extension list, no copy
# of a directory glob, and no re-implementation of the directory matcher (its
# component-wise walk is load-bearing: a bash `case` glob crosses `/`, git's
# `:(glob)` `*` does not, so an approximation would disagree with the pathspec
# the sibling subsystem actually configures).
#
# THE COUPLING THIS CREATES, stated because it is obvious for one release and
# invisible afterwards: this classifier depends on that declaration's CONTENT.
# It does NOT depend on roborev v0.61.2's `git.FormatExcludeArgs` pathspec
# semantics — it feeds roborev no pathspec and reads no `.roborev.toml`. What it
# inherits is the consequence: the declaration mirrors `.roborev.toml`, whose
# correctness was established against that pinned version, so a roborev upgrade
# may move the declaration — and an edit to it now moves THE CORRECTNESS GATE as
# well as the reviewer's diff. After any roborev upgrade, re-verify that the
# declaration still means what the GATE needs and re-run
# scripts/tests/test_classify_docs_only.sh, whose bucket-partition assertion is
# the mechanical half of that obligation.
#
# Resolved relative to THIS SCRIPT'S OWN LOCATION, never `$PWD`: the classifier
# is invoked from varying working directories, and location-relative resolution
# is what lets the self-test mutate the declaration in a temporary tree and
# prove the verdict follows it.
# ---------------------------------------------------------------------------
CLASSIFY_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ARTIFACT_DECLARATION="$CLASSIFY_SCRIPT_DIR/../flow/roborev-review-oracles.sh"
ARTIFACT_DECLARATION_STATUS="unusable"
ARTIFACT_DECLARATION_DETAIL="import not attempted"

if [ ! -f "$ARTIFACT_DECLARATION" ]; then
  ARTIFACT_DECLARATION_DETAIL="declaring file absent: $ARTIFACT_DECLARATION"
# shellcheck source=../flow/roborev-review-oracles.sh
elif ! source "$ARTIFACT_DECLARATION"; then
  ARTIFACT_DECLARATION_DETAIL="declaring file failed to source: $ARTIFACT_DECLARATION"
else
  _declared_globs=0
  if [ -n "${CODE_FREE_ARTIFACT_DIR_GLOBS+x}" ]; then
    _declared_globs=${#CODE_FREE_ARTIFACT_DIR_GLOBS[@]}
  fi
  if [ -z "${CODE_FREE_ARTIFACT_EXTENSIONS:-}" ]; then
    ARTIFACT_DECLARATION_DETAIL="CODE_FREE_ARTIFACT_EXTENSIONS is empty or undefined"
  elif [ "$_declared_globs" -lt 1 ]; then
    ARTIFACT_DECLARATION_DETAIL="CODE_FREE_ARTIFACT_DIR_GLOBS has no elements"
  elif ! declare -F roborev_path_in_artifact_dir >/dev/null 2>&1; then
    ARTIFACT_DECLARATION_DETAIL="roborev_path_in_artifact_dir is not defined"
  else
    ARTIFACT_DECLARATION_STATUS="ok"
    ARTIFACT_DECLARATION_DETAIL="imported from $ARTIFACT_DECLARATION"
  fi
fi

# ---------------------------------------------------------------------------
# BUCKETING the imported extension set (issue #3250). The two subsystems ask
# different questions of the same material — #3229 asks "must a REVIEWER see
# this?", this script asks "must the CORRECTNESS GATE run?" — so the shared
# declaration is the answer to one sub-question ("what is committed run output
# beside a report") and the bucketing below is this classifier's own.
#
# Doctrine (CLAUDE.md): "exclusion of code-bearing formats MUST be scoped by
# directory, never by extension alone", because "an extension describes a
# FORMAT; a directory records an INTENT". So:
#
#   inert       — documentation ANYWHERE under docs/. No pr-gate-core step reads
#                 a run dump, and no gate component treats one as a contract.
#                 `jsonl` is the load-bearing member: a purely directory-scoped
#                 rule would force the ~14-minute core on every flow-finalize
#                 telemetry PR, one per delivery cycle, for a one-line append.
#   code-bearing— documentation ONLY inside an artifact-bearing directory. The
#                 falsifying cases are the Grafana dashboard the gate's own
#                 kit-dashboard-drift component guards, and the schema governing
#                 the delivery ledger: both are functional, both live under
#                 docs/, neither is a report artifact.
#   image-layer — answered by L1 first (the pre-existing repo-wide image
#                 allowlist, untouched here); listed so the buckets PARTITION
#                 the imported set and the self-test can prove it.
#
# An imported extension in NO bucket classifies `full` at runtime AND FAILs the
# self-test naming the extension and issue #3250 — the mechanical disagreement
# check, aimed at the only place drift remains possible once the list itself is
# imported.
# ---------------------------------------------------------------------------
CLASSIFY_INERT_ARTIFACT_EXTENSIONS="txt jsonl log err csv gz pdf jfr mmd tex diff"
CLASSIFY_CODE_BEARING_ARTIFACT_EXTENSIONS="json html"
CLASSIFY_IMAGE_LAYER_ARTIFACT_EXTENSIONS="png svg"

# _ext_in_list <extension> <space-separated list> -> 0 when present.
_ext_in_list() {
  local needle="$1" candidate
  # shellcheck disable=SC2086  # intentional word-split of the space-separated list
  for candidate in $2; do
    [ "$candidate" = "$needle" ] && return 0
  done
  return 1
}

# docs_path_is_documentation <path-under-docs/> -> 0 when the path is
# documentation. THE ONLY place a path under `docs/` is classified (#3250): the
# defect this replaced was not one wrong `case` arm but a verdict reachable from
# a PATH SHAPE instead of a NAMED CLASS, which left the next `docs/`-shaped arm
# one edit away. The layers below are a disjunction over DISJOINT extension
# sets, so permuting them cannot change an answer.
docs_path_is_documentation() {
  local path="$1" base ext

  # An unusable import fails CLOSED for EVERY path under docs/ — including
  # prose. Degrading to "L1 only and carry on" would let an infra fault produce
  # a MORE PERMISSIVE gate, which is the shape #3229 named as an unmeasurable
  # input reaching a permissive branch.
  [ "$ARTIFACT_DECLARATION_STATUS" = "ok" ] || return 1

  base="${path##*/}"

  # L1 — prose / image / legal, at any depth. This classifier's own semantics.
  #
  # The legal names are an EXACT-NAME plus CLOSED-PROSE-SUFFIX allowlist, never a
  # `LICENSE.*` wildcard (roborev round 1, High): a wildcard suffix accepts ANY
  # extension, so `docs/tools/LICENSE.sh` and `docs/observability/NOTICE.json`
  # would be documentation — the very bypass class this change exists to close,
  # and a direct contradiction of both the fail-closed rule for unrecognized
  # extensions and the directory scoping of code-bearing formats. Measured before
  # tightening: only 3 LICENSE/NOTICE files are tracked repo-wide and all 3 carry
  # the exact extensionless name, so this regresses nothing real.
  case "$base" in
    *.md | *.markdown) return 0 ;;
    *.png | *.jpg | *.jpeg | *.gif | *.svg | *.webp | *.ico) return 0 ;;
    LICENSE | LICENSE.md | LICENSE.markdown | LICENSE.txt | LICENSE.rst) return 0 ;;
    NOTICE | NOTICE.md | NOTICE.markdown | NOTICE.txt | NOTICE.rst) return 0 ;;
  esac

  # No extension at all => full, unconditionally, with no mode lookup.
  case "$base" in
    *.*) ext="${base##*.}" ;;
    *) return 1 ;;
  esac

  # L2 / L3 — a report artifact is one the SINGLE DECLARATION recognises AND
  # this classifier has bucketed. Both conditions are required, so removing an
  # extension upstream tightens this gate and adding one leaves it closed until
  # the extension is bucketed here deliberately.
  _ext_in_list "$ext" "$CODE_FREE_ARTIFACT_EXTENSIONS" || return 1

  # L2 — inert: documentation anywhere under docs/.
  if _ext_in_list "$ext" "$CLASSIFY_INERT_ARTIFACT_EXTENSIONS"; then
    return 0
  fi

  # L3 — code-bearing: documentation only inside an artifact-bearing directory.
  if _ext_in_list "$ext" "$CLASSIFY_CODE_BEARING_ARTIFACT_EXTENSIONS"; then
    if roborev_path_in_artifact_dir "$path"; then
      return 0
    fi
    return 1
  fi

  # Imported but unbucketed (or an image extension, already answered by L1 and
  # therefore unreachable here) => fail closed.
  return 1
}

# is_docs_file <path> -> 0 if the single path is unambiguously a docs file.
# Fail-closed: unrecognized => 1 (not docs).
is_docs_file() {
  local path="$1"

  # Sensitive dirs force the full path regardless of extension.
  case "$path" in
    .github/* | scripts/* | test-data/*) return 1 ;;
  esac

  # THE RAW-PATH BOUNDARY (issue #3250), fixed once, here. `git diff
  # --name-only` C-QUOTES a path containing non-ASCII or control bytes, and a
  # quoted spelling like "docs/\303\251.md" has apparent extension `md"` — so an
  # extension-based verdict read off it would be an accident of spelling.
  # pr-gate.yml therefore passes `core.quotePath=false`, and any spelling that
  # still arrives quoted is not a raw repo-relative path and fails closed. A
  # path containing a control character stays unrepresentable in a
  # newline-delimited stream, and is closed by this rule rather than by
  # assuming it cannot occur.
  case "$path" in
    '"'*) return 1 ;;
  esac

  # THE SINGLE DISPATCH: one address for "classify a path under docs/".
  case "$path" in
    docs/*) docs_path_is_documentation "$path"; return $? ;;
  esac

  # The global legal arm carries the same exact-name + closed-prose-suffix
  # allowlist as L1, for the same reason. The `LICENSE.*` wildcard here was
  # PRE-EXISTING (it long predates issue #3250 and this change did not introduce
  # it), and it let a root-level `LICENSE.sh` short-circuit the gate. It is fixed
  # opportunistically because it is the identical one-line defect in the function
  # this change hardens, and leaving a known gate bypass in place merely because
  # it predates the change would be indefensible. Deliberately still anchored to
  # the WHOLE path, exactly as before: making it match a basename would WIDEN the
  # fast path to nested legal files outside `docs/` (e.g. `bindings/node/LICENSE`,
  # `full` before and after), which is a loosening this change does not make.
  case "$path" in
    *.md | *.markdown) return 0 ;;
    *.png | *.jpg | *.jpeg | *.gif | *.svg | *.webp | *.ico) return 0 ;;
    LICENSE | LICENSE.md | LICENSE.markdown | LICENSE.txt | LICENSE.rst) return 0 ;;
    NOTICE | NOTICE.md | NOTICE.markdown | NOTICE.txt | NOTICE.rst) return 0 ;;
    *) return 1 ;;
  esac
}

main() {
  local saw_file=0
  local path
  local non_docs=""

  if [ "$ARTIFACT_DECLARATION_STATUS" != "ok" ]; then
    echo "classify-docs-only: artifact-declaration-unusable ($ARTIFACT_DECLARATION_DETAIL)" \
      "-> EVERY path under docs/ forces the FULL PATH (fail-closed, #3250)" >&2
  fi

  while IFS= read -r path || [ -n "$path" ]; do
    # Skip blank lines (trailing newline, empty diff output).
    [ -n "$path" ] || continue
    saw_file=1
    if ! is_docs_file "$path"; then
      non_docs="$path"
      break
    fi
  done

  if [ "$saw_file" -eq 0 ]; then
    echo "full"
    echo "classify-docs-only: empty/ambiguous changed set -> FULL PATH (fail-closed)" >&2
    return 1
  fi

  if [ -n "$non_docs" ]; then
    echo "full"
    case "$non_docs" in
      '"'*)
        echo "classify-docs-only: '$non_docs' is not a raw repo-relative path" \
          "(C-quoted spelling) -> FULL PATH (fail-closed, #3250)" >&2
        ;;
      *)
        echo "classify-docs-only: non-docs file '$non_docs' -> FULL PATH (fail-closed)" >&2
        ;;
    esac
    return 1
  fi

  echo "docs-only"
  echo "classify-docs-only: all changed files are docs -> short-circuit to green" >&2
  return 0
}

main "$@"
