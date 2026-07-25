#!/usr/bin/env bash
# Structural lint for issue #2874: gate self-tests under scripts/tests/ MUST be
# HERMETIC per run — every fixture/sentinel/temporary path a per-run mktemp namespace
# with a terminal `XXXXXX` template (macOS-safe), never a FIXED shared name. Two
# concurrent self-test lanes in one checkout must not be able to collide on a shared
# path (the residual #2874 kill surface: the parity-report self-test's fixed
# `.tmp-*-mutated` fixture, whose EXIT trap `rm`'d a peer lane's live fixture).
#
# This is a static regression guard so the class cannot silently return. It scans
# scripts/tests/*.sh for:
#   A. macOS-UNSAFE mktemp templates — a `X{3,}` run that is NOT trailing (macOS
#      mktemp requires the X's to be the LAST chars of the template).
#   B. FIXED `.tmp-*` fixture names — a `.tmp-<name>.<ext>` literal (the offending
#      convention), as opposed to a per-run `mktemp ....XXXXXX` name (no extension).
#
# Comment lines are ignored (so doc references to a retired fixed name don't trip it).
# Deliberate exceptions carry a trailing `# hermetic-lint-allow` marker on the line.
#
# The detector itself is SELF-VERIFIED (issue #2874 review): the test plants a
# non-terminal-X mktemp template, a fixed `.tmp-*.yml` name, an allow-marked line, and
# a clean template into a throwaway dir and asserts each is (or is not) reported — so a
# regex that silently stops matching FAILS this test instead of letting the "fixed-name
# fixture regression is caught structurally" guarantee go inert.
#
# Run standalone:   bash scripts/tests/test_gate_selftest_hermetic.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
SELF=$(basename "$0")

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

shopt -s nullglob

# scan_dir <dir>: emit one `<rule>\t<file>:<lineno>:<content>` line per REAL violation
# under <dir>/*.sh (the linter itself, full-line comments, and `# hermetic-lint-allow`
# lines excluded). Two file-level grep passes — no per-line subprocess fan-out.
#   Rule A: a mktemp template whose X-run is not TRAILING — 3+ X's immediately followed
#           by ANY character that continues the template rather than terminating it.
#           Inverted class (review finding 7): flag `X{3,}` followed by anything that is
#           NOT one of {another X, whitespace, a quote/backtick, a shell token terminator
#           `)`/`;`/`|`/`&`/`}`}. This catches suffixes an allowlist misses — `$$`,
#           `${suffix}`, `~`, `%`, `@`, `+`, `,` — while a genuinely trailing X-run
#           (`"...XXXXXX"`, `$(mktemp ...XXXXXX)`, `...XXXXXX;`, end-of-line) is NOT
#           flagged. macOS mktemp requires the X's to be the last chars of the template.
#   Rule B: a FIXED `.tmp-<name>.<ext>` fixture literal (the retired shared-name
#           convention). A per-run mktemp name ends in `XXXXXX` with no extension.
scan_dir() {
  local dir="$1" f list=() line rest file base content trimmed rule
  list=("$dir"/*.sh)
  [ "${#list[@]}" -gt 0 ] || return 0        # nullglob: no files -> nothing to scan
  # Inverted trailing-char class; built via double quotes so the embedded ' and ` are
  # literal ERE members. Excludes X, whitespace, quotes/backtick, and ) ; | & }.
  local rule_a_re="X{3,}[^X[:space:]\"'\`);|&}]"
  local raw
  raw=$(
    {
      grep -HnE 'mktemp' "${list[@]}" 2>/dev/null | grep -E "$rule_a_re" | sed 's/^/A	/'
      grep -HnE '\.tmp-[A-Za-z0-9_-]+\.(yml|yaml|json|txt|md|db|cql)' "${list[@]}" 2>/dev/null | sed 's/^/B	/'
    }
  )
  [ -n "$raw" ] || return 0
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    rule=${line%%	*}                          # leading TAB-delimited rule tag
    rest=${line#*	}                           # file:lineno:content
    file=${rest%%:*}
    base=$(basename "$file")
    [ "$base" = "$SELF" ] && continue          # never flag the linter itself
    content=${rest#*:}; content=${content#*:}  # strip "file:lineno:"
    case "$content" in *'# hermetic-lint-allow'*) continue ;; esac
    trimmed=${content#"${content%%[![:space:]]*}"}
    case "$trimmed" in '#'*) continue ;; esac  # full-line comment
    printf '%s	%s\n' "$rule" "$rest"
  done <<<"$raw"
}

# --- 1. Real-tree scan: scripts/tests/*.sh must be clean. ----------------------
real=$(scan_dir "$SCRIPT_DIR")
if [ -z "$real" ]; then
  ok "no macOS-unsafe mktemp templates or fixed '.tmp-*' fixture names in scripts/tests/*.sh"
else
  while IFS= read -r v; do
    [ -n "$v" ] || continue
    rule=${v%%	*}; loc=${v#*	}
    case "$rule" in
      A) bad "macOS-unsafe mktemp template (X's not trailing): $loc" ;;
      B) bad "fixed '.tmp-*' fixture name (use a per-run mktemp XXXXXX): $loc" ;;
      *) bad "unknown-rule violation: $v" ;;
    esac
    printf '        %s\n' "${loc#*:}"
  done <<<"$real"
fi

# --- 2. Detector self-verification: plant violations and assert they're caught. -
probe=$(mktemp -d "${TMPDIR:-/tmp}/hermetic-lint-probe.XXXXXX")
trap 'rm -rf "$probe"' EXIT INT TERM

printf '%s\n' '#!/usr/bin/env bash' 'x=$(mktemp "/tmp/foo.XXXXXX.yml")' > "$probe/bad_mktemp.sh"
printf '%s\n' '#!/usr/bin/env bash' 'MUT="$REPO_ROOT/test-data/.tmp-parity-manifest-mutated.yml"' > "$probe/bad_fixed.sh"
printf '%s\n' '#!/usr/bin/env bash' 'ok=$(mktemp "/tmp/z.XXXXXX.yml") # hermetic-lint-allow' > "$probe/allow_marked.sh"
printf '%s\n' '#!/usr/bin/env bash' 'good=$(mktemp "/tmp/ok.XXXXXX")' > "$probe/clean.sh"
# $-suffixed (review finding 7): a template whose X-run is followed by `$$`/`${...}` is
# equally macOS-unsafe — the inverted class must catch it (an allowlist missed it).
printf '%s\n' '#!/usr/bin/env bash' 'd=$(mktemp "/tmp/x.XXXXXX$$")' > "$probe/bad_dollar.sh"
# Paren-terminated UNQUOTED trailing-X template must NOT be flagged (the terminator
# exclusion): `$(mktemp /tmp/ok.XXXXXX)` is safe.
printf '%s\n' '#!/usr/bin/env bash' 'p=$(mktemp /tmp/ok2.XXXXXX)' > "$probe/clean_paren.sh"

pv=$(scan_dir "$probe")

if grep -q "^A	$probe/bad_mktemp.sh:" <<<"$pv"; then
  ok "self-verify: detector catches a non-terminal-X mktemp template (Rule A live)"
else
  bad "self-verify: detector MISSED a non-terminal-X mktemp template (Rule A went inert)"
fi
if grep -q "^A	$probe/bad_dollar.sh:" <<<"$pv"; then
  ok "self-verify: detector catches an X-run followed by \$\$ (inverted class covers \$-suffix)"
else
  bad "self-verify: detector MISSED an X-run followed by \$\$ (inverted class regressed)"
fi
if grep -q "^B	$probe/bad_fixed.sh:" <<<"$pv"; then
  ok "self-verify: detector catches a fixed '.tmp-*.yml' fixture name (Rule B live)"
else
  bad "self-verify: detector MISSED a fixed '.tmp-*.yml' fixture name (Rule B went inert)"
fi
if grep -q "$probe/allow_marked.sh:" <<<"$pv"; then
  bad "self-verify: '# hermetic-lint-allow' did NOT suppress a flagged line"
else
  ok "self-verify: '# hermetic-lint-allow' suppresses a flagged line"
fi
if grep -q "$probe/clean.sh:" <<<"$pv"; then
  bad "self-verify: false-positive on a clean terminal-XXXXXX template"
else
  ok "self-verify: no false-positive on a clean terminal-XXXXXX template"
fi
if grep -q "$probe/clean_paren.sh:" <<<"$pv"; then
  bad "self-verify: false-positive on a paren-terminated trailing-X template"
else
  ok "self-verify: no false-positive on a paren-terminated trailing-X template"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
