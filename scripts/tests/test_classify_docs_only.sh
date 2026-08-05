#!/usr/bin/env bash
# Self-test for the docs-only PR-gate classifier (issue #2645, epic #2636).
#
# Proves the three acceptance properties of scripts/ci/classify-docs-only.sh:
#   (a) a docs-/board-only changed set short-circuits (exit 0 => "docs-only"),
#       so the required gate can return green in seconds;
#   (b) any Rust / Cargo / test-data manifest / .github workflow / scripts /
#       config / unknown-extension file forces the FULL path (exit 1 => "full");
#   (c) FAIL-CLOSED: an empty/ambiguous changed set forces the full path, and a
#       *.md file living under a sensitive dir (.github/, scripts/, test-data/)
#       does NOT short-circuit.
#
# Also asserts the workflow contract that makes the required status ALWAYS
# report: pr-gate.yml uses NO paths/paths-ignore trigger filter, always runs the
# classifier step, and gates every heavy step (including the #2644
# query-semantics oracle) on the classifier output.
#
# Hermetic: pure shell, no cargo/Docker/datasets/network. A failure FAILs the
# tooling-tests gate component.
#
# Run standalone:   bash scripts/tests/test_classify_docs_only.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
CLASSIFY="$REPO_ROOT/scripts/ci/classify-docs-only.sh"
WORKFLOW="$REPO_ROOT/.github/workflows/pr-gate.yml"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -f "$CLASSIFY" ] || { echo "FATAL: classifier missing: $CLASSIFY" >&2; exit 1; }

# assert_docs_only <label> <newline-file-list>
# Expects exit 0 and STDOUT verdict "docs-only".
assert_docs_only() {
  local label="$1" input="$2" out rc
  out=$(printf '%s' "$input" | bash "$CLASSIFY" 2>/dev/null)
  rc=$?
  if [ "$rc" -eq 0 ] && [ "$out" = "docs-only" ]; then
    ok "$label (short-circuit: exit 0, verdict=docs-only)"
  else
    bad "$label (expected exit 0/docs-only, got exit $rc/'$out')"
  fi
}

# assert_full <label> <newline-file-list>
# Expects exit 1 and STDOUT verdict "full".
assert_full() {
  local label="$1" input="$2" out rc
  out=$(printf '%s' "$input" | bash "$CLASSIFY" 2>/dev/null)
  rc=$?
  if [ "$rc" -eq 1 ] && [ "$out" = "full" ]; then
    ok "$label (full path: exit 1, verdict=full)"
  else
    bad "$label (expected exit 1/full, got exit $rc/'$out')"
  fi
}

echo "== (a) docs-/board-only sets short-circuit to green =="
assert_docs_only "single markdown"        $'README.md\n'
assert_docs_only "docs tree"              $'docs/development/dev-cookbook.md\n'
assert_docs_only "image asset"            $'docs/img/diagram.png\n'
assert_docs_only "multiple docs files"    $'docs/a.md\nREADME.md\ndocs/img/x.svg\nCHANGELOG.markdown\n'
assert_docs_only "license"                $'LICENSE\n'
assert_docs_only "trailing blank lines"   $'docs/a.md\n\n\n'

echo "== (b) code-relevant sets force the FULL path =="
assert_full "rust source"                 $'cqlite-core/src/lib.rs\n'
assert_full "rust among docs"             $'docs/a.md\ncqlite-core/src/reader.rs\n'
assert_full "cargo manifest"              $'cqlite-core/Cargo.toml\n'
assert_full "cargo lockfile"              $'Cargo.lock\n'
assert_full ".github workflow"            $'.github/workflows/pr-gate.yml\n'
assert_full ".github action"              $'.github/actions/setup-rust-ci/action.yml\n'
assert_full "scripts dir"                 $'scripts/agent-gate.sh\n'
assert_full "test-data manifest"          $'test-data/cassandra-parity-manifest.yml\n'
assert_full "test-data fixture"           $'test-data/datasets/sstables/x/nb-1-big-Data.db\n'

echo "== (c) fail-closed on ambiguous / smuggled changes =="
assert_full "empty set"                   ''
assert_full "blank lines only"            $'\n\n'
assert_full "unknown extension"           $'tools/helper.py\n'
assert_full "no-extension top-level"      $'Makefile\n'
assert_full "md under .github (smuggle)"  $'.github/README.md\n'
assert_full "md under scripts (smuggle)"  $'scripts/notes.md\n'
assert_full "md under test-data (smuggle)" $'test-data/README.md\n'
assert_full "docs + smuggled workflow md" $'docs/ok.md\n.github/CONTRIBUTING.md\n'

# ===========================================================================
# ISSUE #3250 — a `docs/` path prefix is no longer a verdict.
#
# The classifier used to answer `docs-only` for ANY path under `docs/`, and this
# repository ships measurement harnesses under `docs/reports/*-artifacts/` BY
# CONVENTION, so three merged PRs reported `required` green in 13-16 s having
# compiled and tested nothing. A path under `docs/` is now documentation ONLY on
# an affirmative allowlist match, with the artifact declaration IMPORTED from
# #3229's single declaration (`scripts/flow/roborev-review-oracles.sh`).
#
# Everything below is ADDITIVE: every assertion above is unchanged.
# ===========================================================================
ORACLES="$REPO_ROOT/scripts/flow/roborev-review-oracles.sh"
[ -f "$ORACLES" ] || { echo "FATAL: artifact declaration missing: $ORACLES" >&2; exit 1; }

TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/classify-docs-only-3250.XXXXXX")
cleanup_3250() { rm -rf "$TMPROOT"; }
trap cleanup_3250 EXIT

# --- reading the REAL declaration, never a fixture mirror -------------------
# A symmetric copy of a production constant is not a test: it shares any defect
# in the original, so both sides agree and the suite is green while the gate is
# broken (#3042's blindness, in shell). Every assertion that needs the artifact
# extension set or the directory globs reads them from the declaring FILE.
decl_extensions() {  # <declaring-file>
  bash -c 'set -uo pipefail; source "$1" >/dev/null 2>&1 || exit 1
           printf "%s" "${CODE_FREE_ARTIFACT_EXTENSIONS:-}"' _ "$1"
}
decl_globs() {       # <declaring-file> -> one glob per line
  bash -c 'set -uo pipefail; source "$1" >/dev/null 2>&1 || exit 1
           printf "%s\n" "${CODE_FREE_ARTIFACT_DIR_GLOBS[@]:-}"' _ "$1"
}

# assert_full_reason <label> <file-list> <ERE the STDERR reason must match>
# Fail-closed verdicts must NAME their cause: a `full` with no reason is
# indistinguishable from a `full` for the wrong reason.
assert_full_reason() {
  local label="$1" input="$2" want="$3" out rc errf
  errf="$TMPROOT/err.$$"
  out=$(printf '%s' "$input" | bash "$CLASSIFY" 2>"$errf")
  rc=$?
  if [ "$rc" -eq 1 ] && [ "$out" = "full" ] && grep -Eq "$want" "$errf"; then
    ok "$label (full + reason matches /$want/)"
  else
    bad "$label (expected exit 1/full + reason /$want/, got exit $rc/'$out' reason: $(tr '\n' ' ' <"$errf"))"
  fi
  rm -f "$errf"
}

echo "== (d) #3250: executables and config-as-code under docs/ force the FULL path =="
assert_full "harness .sh under an artifact dir" $'docs/reports/ws0-3217-artifacts/harness/common.sh\n'
assert_full "harness .py under an artifact dir" $'docs/reports/ws0-3026-artifacts/ws0-h2h/cas-scan.py\n'
assert_full "harness .bt under an artifact dir" $'docs/reports/ws0-3026-artifacts/ws0-corpus/trace-scan.bt\n'
assert_full "docs-hosted Cargo.toml alone"      $'docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/Cargo.toml\n'
assert_full "docs-hosted src/main.rs alone"     $'docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/src/main.rs\n'
assert_full "docs-hosted crate (both files)"    $'docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/Cargo.toml\ndocs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/src/main.rs\n'
assert_full "extensionless harness (no mode read)" $'docs/reports/ws0-3026-artifacts/ws0-results/ws0-readbw\n'
assert_full "space-bearing harness .sh"         $'docs/reports/ws0-3217-artifacts/harness/run all.sh\n'
assert_full "unrecognized extension .rb"        $'docs/tools/run.rb\n'
assert_full "unrecognized extension .jq"        $'docs/tools/query.jq\n'
assert_full "unrecognized extension .mjs"       $'docs/tools/build.mjs\n'
assert_full "grafana dashboard json (functional config)" $'docs/observability/grafana/dashboards/cqlite-overview.json\n'
assert_full "telemetry schema json"             $'docs/reports/delivery-telemetry.schema.json\n'
assert_full "dashboard json among prose"        $'docs/a.md\ndocs/observability/grafana/dashboards/cqlite-overview.json\ndocs/b.md\n'
assert_full "schema json among prose"           $'docs/a.md\ndocs/reports/delivery-telemetry.schema.json\n'
# A bash `case` glob crosses `/`; git's `:(glob)` `*` does not. The imported
# component-wise matcher must agree with the pathspec, not approximate it.
assert_full "separator-crossing pseudo-artifact dir" $'docs/reports/a/b-artifacts/x.json\n'
# The real PR #3222 shape: 34 executables among 154 prose paths. Order must not
# decide, so the same set is asserted with the executable last AND first.
PROSE20=""
_i=1
while [ "$_i" -le 20 ]; do
  PROSE20="${PROSE20}docs/reports/ws0-3217-artifacts/note-${_i}.md"$'\n'
  _i=$((_i + 1))
done
assert_full "one .py LAST among 20 prose files"  "${PROSE20}docs/reports/ws0-3217-artifacts/harness/emit-point.py"$'\n'
assert_full "one .py FIRST among 20 prose files" $'docs/reports/ws0-3217-artifacts/harness/emit-point.py\n'"${PROSE20}"
assert_full "prose sibling does not rescue the set" $'docs/reports/ws0-3217-artifacts/README.md\ndocs/reports/ws0-3217-artifacts/harness/parse-runqlat.py\n'
# The raw-path boundary (one place, #3229's six-blocker lesson applied once).
assert_full_reason "C-quoted spelling fails closed" $'"docs/\\303\\251-notes.md"\n' 'not a raw repo-relative path'

echo "== (e) #3250: prose, images, legal text and report artifacts still short-circuit =="
assert_docs_only "prose + image + svg + legal" $'docs/development/dev-cookbook.md\ndocs/img/diagram.png\ndocs/img/x.svg\nREADME.md\nCHANGELOG.markdown\nLICENSE\n'
assert_docs_only "WS0 report set (prose + inert + json in artifact dir)" $'docs/reports/ws0-3217-artifacts/README.md\ndocs/reports/ws0-3217-artifacts/results/run.txt\ndocs/reports/ws0-3217-artifacts/results/points.jsonl\ndocs/reports/ws0-3217-artifacts/results/summary.csv\ndocs/reports/ws0-3217-artifacts/results/driver.log\ndocs/reports/ws0-3217-artifacts/results/driver.err\ndocs/reports/ws0-3217-artifacts/results/curve.png\ndocs/reports/ws0-3217-artifacts/results/profile.json\n'
# The inert bucket exists for exactly this row: a purely directory-scoped rule
# would force the ~14-minute core on every flow-finalize telemetry PR.
assert_docs_only "delivery telemetry ledger (.jsonl outside an artifact dir)" $'docs/reports/delivery-telemetry.jsonl\n'
assert_docs_only "pandoc header (.tex outside an artifact dir)" $'docs/sstables-definitive-guide/pandoc-header.tex\n'
assert_docs_only "annotated dump (.txt outside an artifact dir)" $'docs/sstables-definitive-guide/statistics-db-annotated-dump.txt\n'
assert_docs_only "photo extensions outside the imported set" $'docs/img/photo.jpg\ndocs/img/anim.gif\ndocs/img/favicon.ico\n'
assert_docs_only "json inside an artifact dir"  $'docs/reports/ws0-3217-artifacts/x.json\n'
assert_docs_only "html under a nested jfr-reports glob" $'docs/observability/jfr-reports/run.html\n'
assert_docs_only "json deep under round-artifacts" $'docs/round-artifacts/2026-08/deep/nested/out.json\n'
assert_docs_only "html under round-artifacts"   $'docs/round-artifacts/soak/report.html\n'
assert_docs_only "svg under the diagrams dir"   $'docs/sstables-definitive-guide/diagrams/partition-layout.svg\n'
assert_docs_only "raw non-ASCII prose path"     $'docs/\303\251-notes.md\n'
assert_docs_only "space-bearing prose path"     $'docs/research/CQLite Writes (M5) \342\200\224 notes.md\n'

echo "== (e2) #3250 roborev r1 (High): a legal NAME must not launder an extension =="
# `LICENSE.* | NOTICE.*` accepted ANY extension at any depth, so a legal-looking
# NAME laundered an executable straight through the gate — the same bypass class
# this change exists to close. Both arms are now exact-name + closed prose suffix.
assert_full "LICENSE.sh under docs/"            $'docs/tools/LICENSE.sh\n'
assert_full "NOTICE.json under docs/"           $'docs/observability/NOTICE.json\n'
assert_full "LICENSE.py under docs/"            $'docs/foo/LICENSE.py\n'
# An artifact directory must not rescue a code-bearing legal-named file either.
assert_full "LICENSE.bt inside an artifact dir" $'docs/reports/ws0-3217-artifacts/LICENSE.bt\n'
# Root level: the identical wildcard was PRE-EXISTING (not introduced by #3250)
# and is fixed opportunistically in the same hunk.
assert_full "LICENSE.sh at the repo root"       $'LICENSE.sh\n'
assert_full "NOTICE.py at the repo root"        $'NOTICE.py\n'
# ...and the legitimate legal fast path survives the tightening.
assert_docs_only "exact LICENSE at the root"    $'LICENSE\n'
assert_docs_only "exact NOTICE at the root"     $'NOTICE\n'
assert_docs_only "exact LICENSE under docs/"    $'docs/x/LICENSE\n'
assert_docs_only "exact NOTICE under docs/"     $'docs/x/NOTICE\n'
assert_docs_only "LICENSE.md"                   $'LICENSE.md\n'
assert_docs_only "LICENSE.txt"                  $'LICENSE.txt\n'
assert_docs_only "NOTICE.rst under docs/"       $'docs/x/NOTICE.rst\n'
# The three legal files this repo actually tracks, read from git rather than
# assumed, so the "regresses nothing real" claim is measured and not asserted.
# Two are NESTED, and a nested legal file was `full` BEFORE this change too (the
# global arm is anchored to the whole path); #3250 does not widen that.
while IFS= read -r _legal; do
  case "$_legal" in
    */*) assert_full     "tracked legal file (nested, unchanged): $_legal" "$_legal"$'\n' ;;
    *)   assert_docs_only "tracked legal file (root): $_legal"             "$_legal"$'\n' ;;
  esac
done < <(git -C "$REPO_ROOT" ls-files | grep -E '(^|/)(LICENSE|NOTICE)([.][^/]*)?$')

echo "== (f) #3250: ONE canonical decision point for a docs/ path (structural) =="
# docs_case_arms <file>: one line per `case` ARM whose PATTERN mentions `docs/`,
# as "<first-line-number>:<arm text, multi-line arms joined>". Joining is what
# stops a blanket arm hiding by putting `return 0` on its own line.
docs_case_arms() {
  awk '
    function emit() { printf "%d:%s\n", start, substr(body, 1, 200); in_arm = 0; body = "" }
    # A `case` ARM PATTERN is a `|`-separated list of glob WORDS terminated by
    # `)`. Requiring every token to be space-free is what keeps a body line such
    # as `echo "... docs/ ... (fail-closed, #3250)"` — which also carries a `)`
    # after a `docs/` — from being misread as an arm.
    function is_arm_pattern(line,   pat, n, i, toks) {
      if (index(line, ")") == 0) return 0
      pat = line
      sub(/\).*/, "", pat)
      sub(/^[[:space:]]*\(?[[:space:]]*/, "", pat)
      sub(/[[:space:]]+$/, "", pat)
      if (pat == "") return 0
      n = split(pat, toks, /[[:space:]]*\|[[:space:]]*/)
      for (i = 1; i <= n; i++) {
        if (toks[i] == "" || toks[i] ~ /[[:space:]]/) return 0
      }
      return 1
    }
    /^[[:space:]]*#/ { next }
    { if ($0 ~ /(^|[[:space:]])case[[:space:]].*[[:space:]]in[[:space:]]*$/) case_depth++ }
    { if ($0 ~ /^[[:space:]]*esac([[:space:]]|;|$)/) case_depth-- }
    in_arm { body = body " " $0; if ($0 ~ /;;/) emit(); next }
    {
      if (case_depth <= 0) next
      if (!is_arm_pattern($0)) next
      pat = $0; sub(/\).*/, "", pat)
      if (pat !~ /docs\//) next
      start = NR; body = $0; in_arm = 1
      if ($0 ~ /;;/) emit()
    }
    END { if (in_arm) emit() }
  ' "$1"
}
fn_body_range() {  # <file> <fn-name> -> "<first-line> <last-line>"
  awk -v fn="$2" '
    $0 ~ "^"fn"\\(\\)[[:space:]]*\\{" { s = NR; inside = 1; next }
    inside && /^}/ { print s, NR; inside = 0 }
  ' "$1"
}
# S1: no docs/ arm may reach a verdict itself — it may only dispatch.
structural_s1_reason() {
  local f="$1" line
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    case "$line" in
      *docs_path_is_documentation*) continue ;;
    esac
    printf 'S1: docs/ case arm at line %s decides a verdict instead of dispatching: %s\n' \
      "${line%%:*}" "${line#*:}"
  done < <(docs_case_arms "$f")
}
# S2/S3: exactly ONE docs/ arm, inside is_docs_file, and ONE dispatch target.
structural_s2_reason() {
  local f="$1" arms n range lo hi ln no defs
  arms=$(docs_case_arms "$f")
  n=$(printf '%s\n' "$arms" | grep -c '[^[:space:]]')
  [ "$n" -eq 1 ] || printf 'S2: expected exactly ONE docs/ case arm (the single dispatch), found %s\n' "$n"
  range=$(fn_body_range "$f" is_docs_file)
  if [ -z "$range" ]; then
    printf 'S2: is_docs_file() not found in %s\n' "$f"
  else
    lo=${range%% *}; hi=${range##* }
    while IFS= read -r ln; do
      [ -n "$ln" ] || continue
      no=${ln%%:*}
      if [ "$no" -lt "$lo" ] || [ "$no" -gt "$hi" ]; then
        printf 'S2: a SECOND docs/ decision site outside is_docs_file() at line %s: %s\n' "$no" "${ln#*:}"
      fi
    done < <(printf '%s\n' "$arms")
  fi
  defs=$(grep -cE '^docs_path_is_documentation\(\)[[:space:]]*\{' "$f")
  [ "$defs" -eq 1 ] || printf 'S3: docs_path_is_documentation() defined %s time(s), expected 1\n' "$defs"
}
structural_reason() { structural_s1_reason "$1"; structural_s2_reason "$1"; }

_r=$(structural_reason "$CLASSIFY")
if [ -z "$_r" ]; then
  ok "classifier has exactly one docs/ decision point (single dispatch in is_docs_file)"
else
  bad "classifier structure (#3250): $(printf '%s' "$_r" | tr '\n' '|')"
fi

# MUTATION-TESTING THE STRUCTURAL ASSERTS. An assert that cannot fail on the
# reintroduced defect is not an assert.
MUT_BLANKET="$TMPROOT/mut-blanket-arm.sh"
awk '{ print; if (!ins && $0 ~ /^[[:space:]]*docs\/\*\)/) { print "    docs/*) return 0 ;;"; ins = 1 } }' \
  "$CLASSIFY" >"$MUT_BLANKET"
if cmp -s "$MUT_BLANKET" "$CLASSIFY"; then
  bad "mutation 1 (blanket docs/* arm) did not apply — the assertion below would be vacuous"
else
  ok "mutation 1 (blanket docs/* arm) applied"
  [ -n "$(structural_reason "$MUT_BLANKET")" ] \
    && ok "structural assert FAILs a reintroduced blanket 'docs/*) return 0' arm" \
    || bad "structural assert did NOT catch a reintroduced blanket 'docs/*) return 0' arm"
fi

MUT_MULTILINE="$TMPROOT/mut-multiline-arm.sh"
awk '{ print; if (!ins && $0 ~ /^[[:space:]]*docs\/\*\)/) { print "    docs/legacy/*)"; print "      return 0"; print "      ;;"; ins = 1 } }' \
  "$CLASSIFY" >"$MUT_MULTILINE"
if cmp -s "$MUT_MULTILINE" "$CLASSIFY"; then
  bad "mutation 2 (multi-line docs/ arm) did not apply — the assertion below would be vacuous"
else
  ok "mutation 2 (multi-line docs/ arm) applied"
  [ -n "$(structural_reason "$MUT_MULTILINE")" ] \
    && ok "structural assert FAILs a MULTI-LINE blanket docs/ arm (return 0 on its own line)" \
    || bad "structural assert did NOT catch a multi-line blanket docs/ arm"
fi

MUT_SECOND="$TMPROOT/mut-second-site.sh"
awk '
  !ins && /^main\(\)[[:space:]]*\{/ {
    print "docs_legacy_gate() {"
    print "  case \"$1\" in"
    print "    docs/*) docs_path_is_documentation \"$1\"; return $? ;;"
    print "  esac"
    print "  return 1"
    print "}"
    print ""
    ins = 1
  }
  { print }
' "$CLASSIFY" >"$MUT_SECOND"
if cmp -s "$MUT_SECOND" "$CLASSIFY"; then
  bad "mutation 3 (second decision site) did not apply — the assertions below would be vacuous"
else
  ok "mutation 3 (second decision site) applied"
  [ -n "$(structural_reason "$MUT_SECOND")" ] \
    && ok "structural assert FAILs a SECOND docs/ decision site" \
    || bad "structural assert did NOT catch a second docs/ decision site"
  # The second site DISPATCHES, so S1 cannot see it: this proves the arm-count /
  # location half (S2) is load-bearing rather than redundant with S1.
  [ -z "$(structural_s1_reason "$MUT_SECOND")" ] \
    && ok "second-site mutant is invisible to S1, so S2 is proven load-bearing" \
    || bad "second-site mutant was caught by S1, so S2 remains unproven"
fi

echo "== (g) #3250: the artifact declaration is IMPORTED, never restated (AC5) =="
_exts=$(decl_extensions "$ORACLES")
if [ -z "$_exts" ]; then
  bad "could not read CODE_FREE_ARTIFACT_EXTENSIONS from $ORACLES"
elif grep -Fq "$_exts" "$CLASSIFY"; then
  bad "classifier holds a LITERAL COPY of the imported artifact extension list (#3250 AC5)"
else
  ok "classifier holds no literal copy of the imported artifact extension list"
fi
_glob_copies=""
while IFS= read -r _g; do
  [ -n "$_g" ] || continue
  if grep -Fq "$_g" "$CLASSIFY"; then _glob_copies="$_glob_copies $_g"; fi
done < <(decl_globs "$ORACLES")
if [ -n "$_glob_copies" ]; then
  bad "classifier holds a LITERAL COPY of artifact directory glob(s):$_glob_copies (#3250 AC5)"
else
  ok "classifier holds no literal copy of any artifact directory glob"
fi

# The buckets must PARTITION the imported set: an extension #3229 adds upstream
# must be assigned here deliberately, and an unassigned one fails closed.
partition_reason() {  # <classifier> <declaring-file>
  local f="$1" o="$2" imported inert code img all e b n
  imported=$(decl_extensions "$o")
  inert=$(sed -n 's/^CLASSIFY_INERT_ARTIFACT_EXTENSIONS="\(.*\)"$/\1/p' "$f")
  code=$(sed -n 's/^CLASSIFY_CODE_BEARING_ARTIFACT_EXTENSIONS="\(.*\)"$/\1/p' "$f")
  img=$(sed -n 's/^CLASSIFY_IMAGE_LAYER_ARTIFACT_EXTENSIONS="\(.*\)"$/\1/p' "$f")
  if [ -z "$imported" ]; then
    printf 'partition: could not read CODE_FREE_ARTIFACT_EXTENSIONS from %s\n' "$o"
    return 0
  fi
  if [ -z "$inert$code$img" ]; then
    printf 'partition: could not read the CLASSIFY_*_ARTIFACT_EXTENSIONS buckets from %s\n' "$f"
    return 0
  fi
  all="$inert $code $img"
  for e in $imported; do
    n=0
    for b in $all; do [ "$b" = "$e" ] && n=$((n + 1)); done
    [ "$n" -eq 1 ] || printf 'partition: imported extension "%s" is in %s bucket(s), expected exactly 1 — assign it in scripts/ci/classify-docs-only.sh (issue #3250)\n' "$e" "$n"
  done
  for b in $all; do
    n=0
    for e in $imported; do [ "$b" = "$e" ] && n=$((n + 1)); done
    [ "$n" -eq 1 ] || printf 'partition: bucketed extension "%s" is not in the imported declaration (issue #3250)\n' "$b"
  done
}
_r=$(partition_reason "$CLASSIFY" "$ORACLES")
if [ -z "$_r" ]; then
  ok "the inert/code-bearing/image-layer buckets partition the imported extension set exactly"
else
  bad "bucket partition (#3250): $(printf '%s' "$_r" | tr '\n' '|')"
fi

# --- the import, mutated in a temp tree ------------------------------------
# The classifier resolves its import relative to its OWN location, which is what
# makes this possible: a copy of the classifier beside a MUTATED copy of the real
# declaration must return DIFFERENT verdicts. A classifier carrying its own
# hardcoded lists returns the unchanged verdicts and fails here.
make_tree() {  # <dir>
  mkdir -p "$1/scripts/ci" "$1/scripts/flow"
  cp "$CLASSIFY" "$1/scripts/ci/classify-docs-only.sh"
  cp "$ORACLES" "$1/scripts/flow/roborev-review-oracles.sh"
}
assert_tree() {  # <label> <tree-dir> <file-list> <docs-only|full>
  local label="$1" d="$2" input="$3" want="$4" out rc wantrc=1
  [ "$want" = "docs-only" ] && wantrc=0
  out=$(printf '%s' "$input" | bash "$d/scripts/ci/classify-docs-only.sh" 2>/dev/null)
  rc=$?
  if [ "$rc" -eq "$wantrc" ] && [ "$out" = "$want" ]; then
    ok "$label (=> $want)"
  else
    bad "$label (expected $wantrc/$want, got $rc/'$out')"
  fi
}
assert_tree_mutated() {  # <label> <tree-dir> <relative-file>
  if cmp -s "$2/$3" "$REPO_ROOT/$3"; then
    bad "$1 (mutation did not apply — the assertion below would be vacuous)"
  else
    ok "$1 (mutation applied)"
  fi
}

TXT_IN_ARTIFACT_DIR=$'docs/reports/ws0-3217-artifacts/results/run.txt\n'
HTML_IN_ROUND_ARTIFACTS=$'docs/round-artifacts/soak/report.html\n'
SYNTHETIC_EXT_PATH=$'docs/reports/ws0-3217-artifacts/results/out.zzz\n'

T_BASE="$TMPROOT/t-baseline"; make_tree "$T_BASE"
assert_tree "baseline tree: inert .txt in an artifact dir" "$T_BASE" "$TXT_IN_ARTIFACT_DIR" docs-only
assert_tree "baseline tree: .html under round-artifacts"   "$T_BASE" "$HTML_IN_ROUND_ARTIFACTS" docs-only
assert_tree "baseline tree: synthetic .zzz extension"      "$T_BASE" "$SYNTHETIC_EXT_PATH" full

T_NOTXT="$TMPROOT/t-decl-minus-txt"; make_tree "$T_NOTXT"
sed -i.bak 's/^CODE_FREE_ARTIFACT_EXTENSIONS="txt /CODE_FREE_ARTIFACT_EXTENSIONS="/' \
  "$T_NOTXT/scripts/flow/roborev-review-oracles.sh"
rm -f "$T_NOTXT/scripts/flow/roborev-review-oracles.sh.bak"
assert_tree_mutated "declaration mutation: 'txt' removed from the imported set" \
  "$T_NOTXT" scripts/flow/roborev-review-oracles.sh
assert_tree "declaration without 'txt': .txt verdict MOVES to full" "$T_NOTXT" "$TXT_IN_ARTIFACT_DIR" full

T_NOGLOB="$TMPROOT/t-decl-minus-glob"; make_tree "$T_NOGLOB"
sed -i.bak "/^  'docs\/round-artifacts'$/d" "$T_NOGLOB/scripts/flow/roborev-review-oracles.sh"
rm -f "$T_NOGLOB/scripts/flow/roborev-review-oracles.sh.bak"
assert_tree_mutated "declaration mutation: one directory glob removed" \
  "$T_NOGLOB" scripts/flow/roborev-review-oracles.sh
assert_tree "declaration without that glob: .html verdict MOVES to full" "$T_NOGLOB" "$HTML_IN_ROUND_ARTIFACTS" full

# A synthetic extension added UPSTREAM ONLY is unassigned here, so it fails
# CLOSED at runtime and the partition assert names it.
T_SYNTH="$TMPROOT/t-decl-plus-synthetic"; make_tree "$T_SYNTH"
sed -i.bak 's/^CODE_FREE_ARTIFACT_EXTENSIONS="\(.*\)"$/CODE_FREE_ARTIFACT_EXTENSIONS="\1 zzz"/' \
  "$T_SYNTH/scripts/flow/roborev-review-oracles.sh"
rm -f "$T_SYNTH/scripts/flow/roborev-review-oracles.sh.bak"
assert_tree_mutated "declaration mutation: synthetic extension added upstream" \
  "$T_SYNTH" scripts/flow/roborev-review-oracles.sh
assert_tree "upstream-only synthetic extension fails CLOSED" "$T_SYNTH" "$SYNTHETIC_EXT_PATH" full
_r=$(partition_reason "$T_SYNTH/scripts/ci/classify-docs-only.sh" "$T_SYNTH/scripts/flow/roborev-review-oracles.sh")
if printf '%s' "$_r" | grep -q 'zzz' && printf '%s' "$_r" | grep -q '#3250'; then
  ok "partition assert FAILs on an unassigned upstream extension, naming it and #3250"
else
  bad "partition assert did not name the unassigned extension and #3250 (got: $(printf '%s' "$_r" | tr '\n' '|'))"
fi

# Assigned on BOTH sides, the verdict moves to docs-only; assigned only in the
# classifier, it does NOT — which is what proves the DECLARATION is authoritative.
T_BOTH="$TMPROOT/t-both-sides"; make_tree "$T_BOTH"
sed -i.bak 's/^CODE_FREE_ARTIFACT_EXTENSIONS="\(.*\)"$/CODE_FREE_ARTIFACT_EXTENSIONS="\1 zzz"/' \
  "$T_BOTH/scripts/flow/roborev-review-oracles.sh"
sed -i.bak 's/^CLASSIFY_INERT_ARTIFACT_EXTENSIONS="/CLASSIFY_INERT_ARTIFACT_EXTENSIONS="zzz /' \
  "$T_BOTH/scripts/ci/classify-docs-only.sh"
rm -f "$T_BOTH"/scripts/flow/*.bak "$T_BOTH"/scripts/ci/*.bak
assert_tree_mutated "two-sided mutation: declaration" "$T_BOTH" scripts/flow/roborev-review-oracles.sh
assert_tree_mutated "two-sided mutation: classifier bucket" "$T_BOTH" scripts/ci/classify-docs-only.sh
assert_tree "synthetic extension declared AND bucketed: verdict MOVES to docs-only" "$T_BOTH" "$SYNTHETIC_EXT_PATH" docs-only

T_LOCAL="$TMPROOT/t-classifier-only"; make_tree "$T_LOCAL"
sed -i.bak 's/^CLASSIFY_INERT_ARTIFACT_EXTENSIONS="/CLASSIFY_INERT_ARTIFACT_EXTENSIONS="zzz /' \
  "$T_LOCAL/scripts/ci/classify-docs-only.sh"
rm -f "$T_LOCAL"/scripts/ci/*.bak
assert_tree_mutated "classifier-only mutation: bucket extended" "$T_LOCAL" scripts/ci/classify-docs-only.sh
assert_tree "bucketed but NOT declared: still full (declaration is authoritative)" "$T_LOCAL" "$SYNTHETIC_EXT_PATH" full

echo "== (h) #3250: an unusable import fails CLOSED, never toward prose =="
# An infra fault must not produce a MORE PERMISSIVE gate, so even prose under
# docs/ forces the full path when the declaration cannot be used.
T_ABSENT="$TMPROOT/t-decl-absent"; make_tree "$T_ABSENT"
rm -f "$T_ABSENT/scripts/flow/roborev-review-oracles.sh"
assert_tree "declaration absent: inert .txt forces full" "$T_ABSENT" "$TXT_IN_ARTIFACT_DIR" full
assert_tree "declaration absent: even prose under docs/ forces full" "$T_ABSENT" $'docs/a.md\n' full
_out=$(printf '%s' "$TXT_IN_ARTIFACT_DIR" | bash "$T_ABSENT/scripts/ci/classify-docs-only.sh" 2>&1 >/dev/null)
if printf '%s' "$_out" | grep -Eq 'artifact-declaration-unusable'; then
  ok "declaration absent: reason names the unusable declaration"
else
  bad "declaration absent: no named reason (got: $(printf '%s' "$_out" | tr '\n' '|'))"
fi

T_EMPTY="$TMPROOT/t-decl-empty"; make_tree "$T_EMPTY"
{ printf '\n%s\n' 'CODE_FREE_ARTIFACT_EXTENSIONS=""'; printf '%s\n' 'CODE_FREE_ARTIFACT_DIR_GLOBS=()'; } \
  >>"$T_EMPTY/scripts/flow/roborev-review-oracles.sh"
assert_tree_mutated "declaration mutation: emptied" "$T_EMPTY" scripts/flow/roborev-review-oracles.sh
assert_tree "declaration empty: inert .txt forces full" "$T_EMPTY" "$TXT_IN_ARTIFACT_DIR" full
assert_tree "declaration empty: even prose under docs/ forces full" "$T_EMPTY" $'docs/a.md\n' full
_out=$(printf '%s' "$TXT_IN_ARTIFACT_DIR" | bash "$T_EMPTY/scripts/ci/classify-docs-only.sh" 2>&1 >/dev/null)
if printf '%s' "$_out" | grep -Eq 'artifact-declaration-unusable'; then
  ok "declaration empty: reason names the unusable declaration"
else
  bad "declaration empty: no named reason (got: $(printf '%s' "$_out" | tr '\n' '|'))"
fi

# Sourcing the declaration must not pollute the classifier's contract: STDOUT
# stays exactly the one-word verdict.
_lines=$(printf '%s' $'docs/a.md\n' | bash "$CLASSIFY" 2>/dev/null | wc -l | tr -d ' ')
_lines_full=$(printf '%s' $'docs/tools/run.rb\n' | bash "$CLASSIFY" 2>/dev/null | wc -l | tr -d ' ')
if [ "$_lines" = "1" ] && [ "$_lines_full" = "1" ]; then
  ok "STDOUT is exactly the one-word verdict in both branches (import has no side effects)"
else
  bad "STDOUT is not a single line (docs-only branch: $_lines, full branch: $_lines_full)"
fi

echo "== workflow contract: required status ALWAYS reports =="
if [ -f "$WORKFLOW" ]; then
  # No path filter on the trigger (would stop the required check from firing).
  if grep -Eq '^\s*(paths|paths-ignore)\s*:' "$WORKFLOW"; then
    bad "pr-gate.yml must NOT use paths/paths-ignore (required check must always fire)"
  else
    ok "pr-gate.yml trigger has no paths/paths-ignore filter"
  fi

  # Issue #3250: the changed-file list must reach the classifier as RAW
  # repo-relative paths. `git diff --name-only` C-QUOTES a non-ASCII path by
  # default, so `docs/é-notes.md` would arrive with apparent extension `md"` —
  # and the classifier now decides on the extension. `-z` is deliberately NOT
  # used: it would change the classifier's newline-delimited stdin contract.
  if grep -Eq 'git -c core\.quotePath=false diff --name-only' "$WORKFLOW"; then
    ok "pr-gate.yml computes the changed-file list with core.quotePath=false (#3250)"
  else
    bad "pr-gate.yml must compute the changed-file list with 'git -c core.quotePath=false diff --name-only' (#3250)"
  fi

  # The classifier step itself must have no `if:` gate (always runs).
  if command -v ruby >/dev/null 2>&1; then
    ruby - "$WORKFLOW" <<'RUBY'
      require "yaml"
      wf = YAML.load_file(ARGV[0])
      # The docs-only classifier lives in `pr-gate-core`; `required` (issue #2910)
      # is the sibling-tier aggregator and is asserted separately below.
      steps = wf.dig("jobs", "pr-gate-core", "steps") || []
      classify = steps.find { |s| s.is_a?(Hash) && s["id"] == "classify" }
      abort("no classify step") unless classify
      # Always runs (no if:), and heavy steps are gated on its output.
      abort("classify step must not be gated") if classify.key?("if")
      # Issue #3250: the raw-path boundary, asserted on the classify step itself
      # (the grep above pins the spelling; this pins WHICH step carries it).
      abort("classify step must disable path quoting (core.quotePath=false)") unless classify["run"].to_s.include?("core.quotePath=false")
      gated = steps.select { |s| s.is_a?(Hash) && s["if"].to_s.include?("steps.classify.outputs.docs_only") }
      # Heavy steps = everything the docs-only path must SKIP (all gated steps
      # except the docs-only informational summary, which runs on == 'true').
      heavy = gated.reject { |s| s["if"].to_s.include?("== 'true'") }
      names = heavy.map { |s| s["name"].to_s }
      abort("oracle step not gated on classifier") unless names.any? { |n| n.include?("oracle") }
      abort("build step not gated on classifier") unless names.any? { |n| n.include?("build") }
      abort("no heavy steps gated on classifier") if heavy.empty?
      # Every heavy step must gate with != 'true' (fail-closed: default = full run).
      bad_gate = heavy.find { |s| !s["if"].to_s.include?("!= 'true'") }
      abort("heavy step gate not fail-closed (!= 'true'): #{bad_gate&.fetch("name")}") if bad_gate
      # And a docs-only branch step must exist so the required status still
      # reports (green summary) when the heavy path is skipped.
      abort("no docs-only branch step (required status would not report)") unless gated.any? { |s| s["if"].to_s.include?("== 'true'") }
      # Issue #2910: `required` is the branch-protection context and must ALWAYS
      # report — never skipped when pr-gate-core fails — and must fail closed on
      # a non-success core result.
      required = wf.dig("jobs", "required")
      abort("no `required` job") unless required.is_a?(Hash)
      abort("`required` job must be named 'required'") unless required["name"].to_s == "required"
      abort("`required` must run with if: always() so the context always reports") unless required["if"].to_s.include?("always()")
      abort("`required` must depend on pr-gate-core") unless Array(required["needs"]).map(&:to_s).include?("pr-gate-core")
      body = Array(required["steps"]).map { |s| s.is_a?(Hash) ? [s["run"].to_s, (s["env"] || {}).values.join("\n")].join("\n") : "" }.join("\n")
      abort("`required` must fail when pr-gate-core did not succeed") unless body.include?("needs.pr-gate-core.result")
      abort("`required` must run the sibling-tier aggregator") unless body.include?("aggregate-required-tiers.sh")
      # Issue #2910 P1: the aggregator re-reads the PR's CURRENT labels so a
      # `ci:waive:<tier-id>` applied to a wedged PR takes effect. That read needs
      # both the PR number and `pull-requests: read`; without them the break-glass
      # silently degrades to the event payload snapshot.
      #
      # ISSUE #3033: the same `pull-requests: read` also authorizes the SECOND half
      # of the break-glass — `gh api repos/{slug}/issues/{n}/events`, the `labeled`
      # events behind waiver attribution and head-binding. GitHub's per-permission
      # reference lists that endpoint under `Pull requests` read as well as `Issues`
      # read, so `issues: read` must NOT be added to pr-gate.yml: it would widen a
      # fork-reachable token over every issue in the repo to authorize nothing new.
      # This single assertion is therefore the only home for that invariant — the
      # sibling suite (test_aggregate_required_tiers.sh) deliberately does not
      # duplicate it. A missing grant kills BOTH reads at once.
      abort("`required` must pass the PR number for the live label read") unless body.include?("pull_request.number")
      perms = wf["permissions"]
      abort("pr-gate.yml must grant `pull-requests: read` for the live label read") unless perms.is_a?(Hash) && perms["pull-requests"].to_s == "read"
RUBY
    if [ "$?" -eq 0 ]; then
      ok "classify step always runs; heavy steps (incl. #2644 oracle) gated fail-closed on its output"
      ok "required always reports, needs pr-gate-core, aggregates sibling tiers, and can re-read labels (#2910)"
    else
      bad "pr-gate.yml classify/gating contract not satisfied"
    fi
  else
    printf 'info - ruby absent; skipped structural workflow assertion\n'
  fi
else
  bad "pr-gate.yml not found at $WORKFLOW"
fi

echo
echo "==== classify-docs-only self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ]
