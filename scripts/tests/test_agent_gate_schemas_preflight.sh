#!/usr/bin/env bash
# Regression test for issues #3148 / #3131: the agent-gate fixture preflight must
# validate the COMMITTED CQL schemas root, not just the fetched SSTable corpus, and
# the schemas root must be resolved CHECKOUT-RELATIVE rather than by climbing `..`
# from $CQLITE_DATASETS_ROOT.
#
# POSITIVE CONTROL is the point of this file (#3148 AC (c)). The #3148 gap survived
# because the preflight was only ever observed passing on a good layout: "STATUS: OK"
# was never proven to be a *decision*. So every case below drives a layout the
# preflight must REJECT and asserts the rejection text, alongside the happy path.
#
# Fast + hermetic by design: the FULL-gate cases exit at the preflight (before any
# cargo component), and every dataset/schemas root is a temp dir — no real corpus, no
# network, no Docker.
#
# Run standalone:   bash scripts/tests/test_agent_gate_schemas_preflight.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

# #2751 defense-in-depth: never let an inherited summary path be clobbered — every
# invocation below pins its own.
unset AGENT_GATE_SUMMARY_FILE
# A CQLITE_SCHEMAS_ROOT exported by the caller would silently redirect the "checkout
# default" cases; scrub it so this file tests the committed contract, not the shell.
unset CQLITE_SCHEMAS_ROOT

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Scratch root, VALIDATED before anything is built under it (roborev job 10, finding 1).
# This script runs `set -uo pipefail` deliberately (no `errexit`: every case must run so a
# single failure does not hide the rest), so an unchecked `mktemp -d` failure would leave
# `$tmp` EMPTY and every derived path would become root-level — `/ds-corpus`,
# `/schemas-empty`, `/hollow/datasets`, … — which a privileged CI job WOULD create, and the
# EXIT trap would then `rm -rf ""`. A verification script that can write to `/` on a bad
# `mktemp` is not a safe guard, so this fails loudly instead, BEFORE the trap is installed
# (arming a cleanup trap on an unvalidated path is the second half of the same hazard).
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-schemas.XXXXXX") || {
  echo "FATAL: mktemp -d failed; refusing to run with an unset scratch root (would resolve to /)" >&2
  exit 1
}
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  echo "FATAL: mktemp -d produced no usable directory ('$tmp'); refusing to run (paths would resolve under /)" >&2
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT

# CHILD PROBE MODE. The scratch-root-guard case below re-invokes THIS script with a failing
# `mktemp` stub on PATH; if the guard is absent the child would otherwise run every case —
# including that one — and recurse without bound (observed: it did, while proving the case
# discriminates). So a child stops HERE, immediately after the scratch root is validated,
# which is the only thing the parent needs from it: with the guard present the child dies
# above with `refusing to run`; without it, the child reaches this line and exits 0, and the
# parent's assertion fails. Bounded to depth 1, fast, and the child creates nothing.
if [ -n "${AGENT_GATE_SCHEMAS_SELFTEST_CHILD:-}" ]; then
  echo "child: scratch root validated ('$tmp')"
  exit 0
fi

# The six canonical .cql the gate's dataset-backed components consume. Kept here as a
# LITERAL list rather than read back from agent-gate.sh: if someone shrinks
# CANONICAL_SCHEMA_FILES, this file must redden, not agree with the shrink.
# DERIVED from the gate, not restated here (#3493). This list was a hand-written copy of
# `CANONICAL_SCHEMA_FILES`, so when that constant grew from 6 entries to 8 (the node suite
# needs `oa-test.cql` and `write-test.cql`, and since #3522 the gate runs that whole suite)
# THIS file silently began building synthetic roots that were missing two schemas — and two
# cases failed for a reason that had nothing to do with what they test. Deriving it means
# the next change to that constant cannot drift them apart.
#
# A failed derivation is a FAIL naming the derivation, never a fallback to a hard-coded
# list: a fallback would restore exactly the copy this removes.
CANONICAL=()
while IFS= read -r _cf; do [ -n "$_cf" ] && CANONICAL+=("$_cf"); done < <(
  sed -n 's/^CANONICAL_SCHEMA_FILES="\(.*\)"$/\1/p' "$GATE" | head -1 | tr ' ' '\n')
if [ "${#CANONICAL[@]}" -eq 0 ]; then
  echo "FAIL - could not derive CANONICAL_SCHEMA_FILES from $GATE; refusing to run against a guessed list" >&2
  exit 1
fi

# DERIVATION IS FOR BUILDING FIXTURES; MEMBERSHIP IS ASSERTED INDEPENDENTLY (roborev #3493).
# Deriving BOTH sides is a tautology: deleting a schema from the production list would delete
# it from the expectation too, and the regression would stay green. So the entries this repo
# depends on are named HERE, on purpose, as a second source.
#
# `oa-test.cql` and `write-test.cql` are required because the gate runs the WHOLE node suite
# (#3522) and its OA parity and write cases resolve those schemas; if they leave
# CANONICAL_SCHEMA_FILES, the #3148 preflight stops guarding them and their absence surfaces
# as a suite failure instead of a named preflight FAIL.
# THE COMPLETE SET, not a sample (roborev, post-rebase round 3). Naming only three left the
# other five derivable-away: removing `time-series.cql`, `wide-table-bti.cql` or
# `collections.cql` from production would have deleted them from the expectation too and
# this test would have stayed green — the same tautology, just smaller.
#
# Every entry here is a schema the corpus or the node suite resolves, so dropping ANY of them
# stops the #3148 preflight guarding a file something reads.
for _req in basic-types.cql da-test.cql oa-test.cql write-test.cql \
            time-series.cql wide-table-bti.cql collections.cql wide-rows.cql; do
  case " ${CANONICAL[*]} " in
    *" $_req "*) : ;;
    *) echo "FAIL - $_req is no longer in the gate's CANONICAL_SCHEMA_FILES; the #3148 preflight" >&2
       echo "       has stopped guarding a schema the node suite resolves. If that is intended," >&2
       echo "       remove it from this list too — deliberately, in the same diff." >&2
       exit 1 ;;
  esac
done

# And the reverse direction: an ADDITION to production must be acknowledged here too, or this
# list silently stops being the complete set it claims to be.
if [ "${#CANONICAL[@]}" -ne 8 ]; then
  echo "FAIL - the gate's CANONICAL_SCHEMA_FILES has ${#CANONICAL[@]} entries, this test expects 8." >&2
  echo "       If a schema was added, add it to the required list above — deliberately." >&2
  exit 1
fi

# A dataset root whose canonical corpus IS present, so the #2078 corpus guard is
# satisfied and the run reaches the #3148 schemas guard.
ds_corpus="$tmp/ds-corpus"
mkdir -p "$ds_corpus/sstables/test_basic/simple_table-0001"
: >"$ds_corpus/sstables/test_basic/simple_table-0001/nb-1-big-Data.db"

# Hostile schemas roots.
schemas_empty="$tmp/schemas-empty"                 # readable dir, zero fixtures
mkdir -p "$schemas_empty"
schemas_partial="$tmp/schemas-partial"             # SOME fixtures — the case a
mkdir -p "$schemas_partial"                        # directory-existence check misses
: >"$schemas_partial/basic-types.cql"
: >"$schemas_partial/collections.cql"

hook_field() {  # hook_field <field> <output>
  printf '%s\n' "$2" | grep "^$1: " | sed "s/^$1: //"
}

# ---------------------------------------------------------------------------
# 1. Hidden --preflight-schemas hook: the PURE decision, both ways.
# ---------------------------------------------------------------------------
good_out=$(bash "$GATE" --preflight-schemas 2>/dev/null)
if [ "$(hook_field STATUS "$good_out")" = OK ] \
   && [ "$(hook_field ROOT "$good_out")" = "$REPO/test-data/schemas" ] \
   && [ "$(hook_field SOURCE "$good_out")" = "checkout-relative" ]; then
  ok "3148-hook-good: checkout resolves the committed schemas root -> STATUS OK"
else
  bad "3148-hook-good: expected STATUS OK + checkout-relative $REPO/test-data/schemas"
  printf '%s\n' "$good_out"
fi

empty_out=$(CQLITE_SCHEMAS_ROOT="$schemas_empty" bash "$GATE" --preflight-schemas 2>/dev/null)
empty_missing=$(hook_field MISSING "$empty_out")
missing_all=1
for f in "${CANONICAL[@]}"; do
  grep -qw -- "$f" <<<"$empty_missing" || missing_all=0
done
if [ "$(hook_field STATUS "$empty_out")" = FAIL ] && [ "$missing_all" -eq 1 ] \
   && [ "$(hook_field SOURCE "$empty_out")" = "CQLITE_SCHEMAS_ROOT override" ]; then
  ok "3148-hook-empty: schemas-less root -> STATUS FAIL naming all ${#CANONICAL[@]} unreadable .cql"
else
  bad "3148-hook-empty: expected STATUS FAIL listing every canonical .cql"
  printf '%s\n' "$empty_out"
fi

# A directory-EXISTENCE check would pass this root: it exists and holds two of the six.
# Only a per-FILE readability check rejects it, naming exactly the four absentees.
partial_out=$(CQLITE_SCHEMAS_ROOT="$schemas_partial" bash "$GATE" --preflight-schemas 2>/dev/null)
partial_missing=$(hook_field MISSING "$partial_out")
if [ "$(hook_field STATUS "$partial_out")" = FAIL ] \
   && grep -qw -- 'da-test.cql' <<<"$partial_missing" \
   && grep -qw -- 'wide-rows.cql' <<<"$partial_missing" \
   && ! grep -qw -- 'basic-types.cql' <<<"$partial_missing" \
   && ! grep -qw -- 'collections.cql' <<<"$partial_missing"; then
  ok "3148-hook-partial: per-FILE readability rejects a present-but-incomplete root"
else
  bad "3148-hook-partial: expected FAIL naming only the absent files (got '$partial_missing')"
fi

# A bare `-r` test accepts a DIRECTORY named `basic-types.cql`; the Rust side asks for a
# readable REGULAR file (`readable_file`). Both sides must ask the same question or the
# gate can certify a layout the tests reject (reviewer nit N7 / roborev finding 2).
schemas_dirtrap="$tmp/schemas-dirtrap"
mkdir -p "$schemas_dirtrap"
for f in "${CANONICAL[@]}"; do mkdir -p "$schemas_dirtrap/$f"; done
dirtrap_out=$(CQLITE_SCHEMAS_ROOT="$schemas_dirtrap" bash "$GATE" --preflight-schemas 2>/dev/null)
dirtrap_missing=$(hook_field MISSING "$dirtrap_out")
dirtrap_all=1
for f in "${CANONICAL[@]}"; do
  grep -qw -- "$f" <<<"$dirtrap_missing" || dirtrap_all=0
done
if [ "$(hook_field STATUS "$dirtrap_out")" = FAIL ] && [ "$dirtrap_all" -eq 1 ]; then
  ok "3148-hook-dirtrap: a DIRECTORY named like a .cql is not a readable regular file"
else
  bad "3148-hook-dirtrap: expected FAIL for directories named like the fixtures (got '$dirtrap_missing')"
fi

# ---------------------------------------------------------------------------
# 1b. A RELATIVE CQLITE_SCHEMAS_ROOT is REJECTED, not resolved (blocker B1).
#
#     The gate evaluates a relative override with CWD = REPO_ROOT; cargo runs each test
#     binary with CWD = the PACKAGE dir. Resolving it let the gate stamp
#     `schemas: 6/6 … under packaged/schemas (override)` while the tests silently fell
#     back to the checkout — the SUMMARY certifying root A for a run that used root B,
#     which IS #3148's defect. So the decision must be FAIL, the reported ROOT must be
#     the checkout (never the relative string dressed up as absolute), and the reason
#     must be named.
# ---------------------------------------------------------------------------
rel_out=$(CQLITE_SCHEMAS_ROOT="packaged/schemas" bash "$GATE" --preflight-schemas 2>/dev/null)
if [ "$(hook_field STATUS "$rel_out")" = FAIL ] \
   && [ "$(hook_field ROOT "$rel_out")" = "$REPO/test-data/schemas" ] \
   && [ "$(hook_field SOURCE "$rel_out")" = "CQLITE_SCHEMAS_ROOT override REJECTED" ] \
   && grep -q 'must be an ABSOLUTE path' <<<"$(hook_field REJECT "$rel_out")"; then
  ok "3148-relative-override: a relative CQLITE_SCHEMAS_ROOT is rejected, not resolved"
else
  bad "3148-relative-override: expected FAIL + REJECTED source + the absolute-path reason"
  printf '%s\n' "$rel_out"
fi

# NOTE: the AC-(b) "never labelled absolute" assert deliberately does NOT live here. It
# used to, matching against `$rel_out` — but `expected absolute path:` is
# apply_schemas_preflight STDERR, and this hook prints only STATUS/ROOT/SOURCE/REJECT/
# MISSING on stdout, so the pattern could never appear and the case passed unconditionally,
# INCLUDING after a full revert of the fix. It now runs against the real FULL-gate emit
# below, where the string can actually occur.

# Every relative shape, not just the bare one; and a blank/whitespace value is NOT an
# override at all (a scripting accident), matching the Rust side's `trim().is_empty()`.
rel_shapes_ok=1
for raw in './schemas' '../schemas' 'a/b/schemas'; do
  st=$(CQLITE_SCHEMAS_ROOT="$raw" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^STATUS:' | sed 's/^STATUS: //')
  [ "$st" = FAIL ] || { rel_shapes_ok=0; echo "   (not rejected: $raw -> $st)"; }
done
for raw in '' '   '; do
  st=$(CQLITE_SCHEMAS_ROOT="$raw" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^STATUS:' | sed 's/^STATUS: //')
  [ "$st" = OK ] || { rel_shapes_ok=0; echo "   (blank treated as an override: '$raw' -> $st)"; }
done
if [ "$rel_shapes_ok" -eq 1 ]; then
  ok "3148-relative-shapes: every relative form rejected; blank/whitespace is not an override"
else
  bad "3148-relative-shapes: relative/blank handling diverges from the Rust resolver"
fi

# A WHITESPACE-ONLY override must be treated as UNSET, matching the Rust resolver's
# `v.trim().is_empty()` (roborev job 9, finding 1). The two mirrors DID disagree here:
# `_gate_schemas_override_reject` trimmed before deciding presence, while
# `_gate_schemas_root`/`_gate_schemas_root_source` tested the RAW `-n` value — so a
# directory literally named "   " would have been reported as the override while Rust read
# the checkout's schemas. Presence is now normalized once, in `_gate_schemas_override`.
#
# PREMISE CORRECTION, recorded so nobody re-derives it: roborev's exploit path ("if a
# relative directory named only with whitespace exists") is NOT reachable from a caller's
# CWD, because agent-gate.sh `cd`s to its OWN repository root before anything else
# (scripts/agent-gate.sh:410). The whitespace-named directory would have to exist in the
# repo root itself. The inconsistency was real and is fixed; this case pins the OBSERVABLE
# rule (whitespace-only ⇒ unset ⇒ checkout-relative), and the discriminating control for
# the trim rule lives on the Rust side, where the resolver is pure and takes the value as
# an argument (`absent_or_blank_override_resolves_to_the_checkout`).
ws_dir_present="$tmp/ws-present"
mkdir -p "$ws_dir_present/   "
# `$'\t'` — a REAL tab. A literal '\t' would be a two-character RELATIVE path, which is
# correctly REJECTED, so writing it that way tests the wrong rule (it did, first try).
for raw in '   ' $'\t' ' ' $'\n'; do
  ws_out=$(CQLITE_SCHEMAS_ROOT="$raw" bash "$GATE" --preflight-schemas 2>/dev/null)
  if [ "$(hook_field STATUS "$ws_out")" = OK ] \
     && [ "$(hook_field SOURCE "$ws_out")" = "checkout-relative" ] \
     && [ "$(hook_field ROOT "$ws_out")" = "$REPO/test-data/schemas" ] \
     && [ -z "$(hook_field REJECT "$ws_out")" ]; then
    :
  else
    rel_shapes_ok=0
    echo "   (whitespace-only value '$raw' was not treated as unset)"
    printf '%s\n' "$ws_out"
  fi
done
if [ "$rel_shapes_ok" -eq 1 ]; then
  ok "3148-whitespace-override: a whitespace-only value is unset (trim rule, single-sourced presence)"
else
  bad "3148-whitespace-override: whitespace-only handling diverges from the Rust resolver"
fi

# A CONTROL-CHARACTER override must be REJECTED (roborev job 10, finding 2). Measured before
# the fix: with `CQLITE_SCHEMAS_ROOT=$'<existing-dir>\n'` the gate reported STATUS OK +
# SOURCE "CQLITE_SCHEMAS_ROOT override" + ROOT "<existing-dir>" — the newline eaten by the
# `$( )` the helper was consumed through — while Rust kept it, failed `is_dir()`, and
# degraded to the checkout. Two roots; the gate certifying the unused one. The directory here
# is REAL and holds all six fixtures, so pre-fix the gate genuinely reported OK: that is what
# makes this case discriminating rather than decorative.
cc_root="$tmp/cc-override"
mkdir -p "$cc_root"
for f in "${CANONICAL[@]}"; do printf -- '-- synthetic\n' >"$cc_root/$f"; done
cc_ok=1
for raw in "$cc_root"$'\n' $'\n'"$cc_root" "$cc_root"$'\r' "$cc_root"$'\tsub'; do
  cc_out=$(CQLITE_SCHEMAS_ROOT="$raw" bash "$GATE" --preflight-schemas 2>/dev/null)
  if [ "$(hook_field STATUS "$cc_out")" = FAIL ] \
     && [ "$(hook_field SOURCE "$cc_out")" = "CQLITE_SCHEMAS_ROOT override REJECTED" ] \
     && grep -q 'must not contain control characters' <<<"$(hook_field REJECT "$cc_out")" \
     && [ "$(hook_field ROOT "$cc_out")" = "$REPO/test-data/schemas" ]; then
    :
  else
    cc_ok=0
    echo "   (control-character override not rejected: $(printf '%q' "$raw"))"
    printf '%s\n' "$cc_out"
  fi
done
if [ "$cc_ok" -eq 1 ]; then
  ok "3148-control-char-override: newline/CR/tab-bearing overrides are rejected, never silently trimmed"
else
  bad "3148-control-char-override: a control-character override was accepted (shell/Rust root divergence)"
fi

# The scratch-root guard (roborev job 10, finding 1) must fail LOUDLY rather than let every
# path resolve under `/`. Driven with a stub `mktemp` that always fails, first on PATH. Safe
# to run: with the guard the script dies before building anything; the assertion additionally
# proves no root-level scratch path was created, which is the harm being prevented.
mk_stub="$tmp/mk-stub"
mkdir -p "$mk_stub"
printf '#!/bin/sh\nexit 1\n' >"$mk_stub/mktemp"
chmod +x "$mk_stub/mktemp"
guard_out=$(PATH="$mk_stub:$PATH" AGENT_GATE_SCHEMAS_SELFTEST_CHILD=1 bash "$0" 2>&1)
guard_rc=$?
if [ "$guard_rc" -ne 0 ] \
   && grep -q 'refusing to run' <<<"$guard_out" \
   && [ ! -e /ds-corpus ] && [ ! -e /schemas-empty ]; then
  ok "3148-scratch-root-guard: a failing mktemp aborts loudly instead of resolving paths under /"
else
  bad "3148-scratch-root-guard: expected a loud abort and no root-level scratch paths (rc=$guard_rc)"
  printf '%s\n' "$guard_out" | head -5
fi

# The FULL gate must FAIL CLOSED on the relative override too — with its own reason, not
# a misleading "missing files" list (the checkout's fixtures are in fact complete).
rel_full="$tmp/3148-rel-full.txt"
rel_full_out="$tmp/3148-rel-full.out"   # stdout+stderr: where `expected absolute path:` lives
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
  CQLITE_SCHEMAS_ROOT="packaged/schemas" AGENT_GATE_SUMMARY_FILE="$rel_full" \
  timeout 180 bash "$GATE" >"$rel_full_out" 2>&1
rel_full_rc=$?
if [ "$rel_full_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$rel_full" 2>/dev/null \
   && grep -q "relative CQLITE_SCHEMAS_ROOT rejected" "$rel_full" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$rel_full" 2>/dev/null \
   && ! grep -q "^schemas: " "$rel_full" 2>/dev/null; then
  ok "3148-relative-full: FULL gate FAILs CLOSED on a relative override and stamps no positive schemas line"
else
  bad "3148-relative-full: expected fail-closed with the relative-override reason (rc=$rel_full_rc)"
  cat "$rel_full" 2>/dev/null
fi

# AC (b), asserted where the string can actually appear: no relative value may ever be
# printed on an "expected absolute path" line, in the SUMMARY or on stderr. Revert the
# reject branch and `_gate_schemas_root` resolves `packaged/schemas`, all six fixtures are
# reported missing, and stderr prints
# `expected absolute path: packaged/schemas/basic-types.cql` — so this case discriminates.
if ! grep -q 'expected absolute path: *packaged/schemas' "$rel_full_out" 2>/dev/null \
   && ! grep -q 'expected absolute path: *packaged/schemas' "$rel_full" 2>/dev/null \
   && grep -q 'must be an ABSOLUTE path' "$rel_full_out" 2>/dev/null; then
  ok "3148-relative-absolute-label: no relative path is ever labelled absolute (AC (b), real emit)"
else
  bad "3148-relative-absolute-label: a relative path reached an 'expected absolute path' line"
  grep -n 'expected absolute path' "$rel_full_out" "$rel_full" 2>/dev/null | head -5
fi

# ---------------------------------------------------------------------------
# 2. The FULL gate FAILS CLOSED, with a marker DISTINGUISHABLE from #2078's.
#    apply_schemas_preflight fires before any cargo component, so this is fast.
#
#    `timeout 180` on the real gate invocations here and below is part of the contract, not
#    defensive padding: these cases assert the run exits AT the preflight, so a run that
#    does not is a FAILURE — and without a bound the regression it catches (the guard
#    letting the layout through) would instead launch a 15-20 minute full gate inside a
#    self-test. Observed while proving these cases discriminate.
# ---------------------------------------------------------------------------
full_fail="$tmp/3148-full-fail.txt"
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
  CQLITE_SCHEMAS_ROOT="$schemas_empty" AGENT_GATE_SUMMARY_FILE="$full_fail" \
  timeout 180 bash "$GATE" >/dev/null 2>&1
full_rc=$?
if [ "$full_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$full_fail" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$full_fail" 2>/dev/null \
   && ! grep -q "^RESULT: PASS" "$full_fail" 2>/dev/null; then
  ok "3148-full-fail: FULL gate FAILs CLOSED on a schemas-less root (marker + RESULT: FAIL, no cargo)"
else
  bad "3148-full-fail: expected non-zero exit + missing-schemas FAIL-CLOSED + RESULT: FAIL (rc=$full_rc)"
  cat "$full_fail" 2>/dev/null
fi

# The two causes must be separable in a pasted block: a schemas failure must NEVER
# stamp #2078's corpus marker (the corpus here is deliberately complete).
if ! grep -q "missing-fixtures:" "$full_fail" 2>/dev/null; then
  ok "3148-marker-distinct: schemas failure carries no missing-fixtures line (#2078 vs #3148 separable)"
else
  bad "3148-marker-distinct: the schemas failure also stamped #2078's missing-fixtures marker"
fi

# AC (b): the failure text names the exact expected ABSOLUTE path and the fix command.
if grep -q "$schemas_empty/basic-types.cql" "$full_fail" 2>/dev/null \
   && grep -q "unset CQLITE_SCHEMAS_ROOT" "$full_fail" 2>/dev/null \
   && grep -q "restore --source=HEAD -- test-data/schemas" "$full_fail" 2>/dev/null; then
  ok "3148-remedy: block names the exact absolute path + both fix commands"
else
  bad "3148-remedy: expected the absolute .cql path and the remedy commands in the block"
  cat "$full_fail" 2>/dev/null
fi

# ---------------------------------------------------------------------------
# 2b. A POSITIVE line must never assert a check that did not run.
#
#     `_schemas_status` returns OK unconditionally under --only/--lite (leniency, AC (g)),
#     so the OK branch used to stamp `schemas: 6/6 canonical .cql readable under <root>`
#     for a check that NEVER RAN — #3148's own misleading `STATUS: OK`, one mode over. The
#     assertion is therefore on the SUMMARY TEXT the real apply_schemas_preflight stamps,
#     driven through the --preflight-schemas-line hook (a real `--only core-tests` run
#     would spend minutes in cargo before printing anything).
# ---------------------------------------------------------------------------
line_field() { printf '%s\n' "$1" | grep '^SCHEMAS_LINE: ' | sed 's/^SCHEMAS_LINE: //'; }

only_line_out=$(CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  bash "$GATE" --preflight-schemas-line core-tests 2>/dev/null)
only_line_rc=$?
only_line=$(line_field "$only_line_out")
if [ "$only_line_rc" -eq 0 ] \
   && ! grep -q 'readable' <<<"$only_line" \
   && grep -q '^schemas: not checked' <<<"$only_line" \
   && grep -q -- '--only core-tests' <<<"$only_line"; then
  ok "3148-only-no-false-positive: an --only run stamps an explicit 'not checked', never 'N/N readable'"
else
  bad "3148-only-no-false-positive: a lenient --only run must not assert readability (rc=$only_line_rc, line: '$only_line')"
fi

# Same for a RELATIVE override under --only. This ALSO pins the second half of the same
# class: the REJECT branch was not governed by _schemas_status, so it FAILed even a
# lenient --only run — the effectful guard diverging from the pure decision it is
# documented to consume.
rel_only_out=$(CQLITE_SCHEMAS_ROOT="packaged/schemas" \
  bash "$GATE" --preflight-schemas-line core-tests 2>/dev/null)
rel_only_rc=$?
rel_only=$(line_field "$rel_only_out")
if [ "$rel_only_rc" -eq 0 ] && grep -q '^schemas: not checked' <<<"$rel_only"; then
  ok "3148-only-reject-lenient: --only stays lenient for a relative override too (no strict-path drift)"
else
  bad "3148-only-reject-lenient: the reject branch is not governed by the lenient mode check (rc=$rel_only_rc, line: '$rel_only')"
fi

# …and the POSITIVE line must still appear when the check DID run, otherwise the two
# asserts above would be satisfied by simply never stamping anything.
full_line=$(line_field "$(bash "$GATE" --preflight-schemas-line 2>/dev/null)")
# COUNT DERIVED TOO: a literal here is the same copy one level down.
_n_canon=${#CANONICAL[@]}
if grep -q "^schemas: $_n_canon/$_n_canon canonical .cql readable under $REPO/test-data/schemas" <<<"$full_line"; then
  ok "3148-full-positive-line: a FULL-mode check that ran stamps the positive N/N readable line"
else
  bad "3148-full-positive-line: expected the positive line for a real check (got '$full_line')"
fi

# ---------------------------------------------------------------------------
# 2c. The FAIL emit must name the ACTUAL rejection reason (spec-auditor, AC (b) partial).
#     The reject branch hard-coded "a RELATIVE schemas root ..." and stamped `... relative
#     CQLITE_SCHEMAS_ROOT rejected` for EVERY rejection — FALSE for a control-character
#     value. It was untested because the round-3 control-char coverage went through the pure
#     `--preflight-schemas` hook, which never reaches the emit. So this case drives the REAL
#     FULL gate and asserts the emitted block, which is the only place the wording lives.
# ---------------------------------------------------------------------------
cc_emit_root="$tmp/cc-emit"
mkdir -p "$cc_emit_root"
for f in "${CANONICAL[@]}"; do printf -- '-- synthetic\n' >"$cc_emit_root/$f"; done
cc_full="$tmp/3148-cc-full.txt"
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
  CQLITE_SCHEMAS_ROOT="$cc_emit_root"$'\n' AGENT_GATE_SUMMARY_FILE="$cc_full" \
  timeout 180 bash "$GATE" >"$tmp/3148-cc-full.out" 2>&1
cc_full_rc=$?
if [ "$cc_full_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$cc_full" 2>/dev/null \
   && grep -q "contains a control character" "$cc_full" 2>/dev/null \
   && ! grep -q "relative CQLITE_SCHEMAS_ROOT rejected" "$cc_full" 2>/dev/null \
   && grep -q "must not contain control characters" "$tmp/3148-cc-full.out" 2>/dev/null \
   && ! grep -q "a RELATIVE schemas root" "$tmp/3148-cc-full.out" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$cc_full" 2>/dev/null; then
  ok "3148-cc-emit-wording: the real FAIL emit names the control-character cause, not a false 'relative' one"
else
  bad "3148-cc-emit-wording: the emitted block/stderr misnames the rejection cause (rc=$cc_full_rc)"
  cat "$cc_full" 2>/dev/null
fi

# 2d. The #2078 opt-out must NOT buy a pass against a schemas-less root (spec-auditor (v)).
#     The schemas guard deliberately has no opt-out — the fetched corpus is legitimately
#     absent sometimes, committed source in a checkout never is. The code ignores
#     AGENT_GATE_ALLOW_MISSING_FIXTURES, but nothing asserted it, so a future "consistency"
#     patch wiring it in would land GREEN.
optout_full="$tmp/3148-optout-full.txt"
CQLITE_GATE_DISABLE_CAP=1 AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  CQLITE_DATASETS_ROOT="$ds_corpus" CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  AGENT_GATE_SUMMARY_FILE="$optout_full" timeout 180 bash "$GATE" >/dev/null 2>&1
optout_full_rc=$?
if [ "$optout_full_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$optout_full" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$optout_full" 2>/dev/null \
   && ! grep -q "^RESULT: PASS" "$optout_full" 2>/dev/null; then
  ok "3148-no-optout: AGENT_GATE_ALLOW_MISSING_FIXTURES=1 does NOT buy a pass on a schemas-less root"
else
  bad "3148-no-optout: the #2078 opt-out must not weaken the schemas guard (rc=$optout_full_rc)"
  cat "$optout_full" 2>/dev/null
fi

# 2e. NO environment value may turn the fail-closed guard into a soft return (requirement 8).
#     The report-only mode began life as `${_SCHEMAS_PREFLIGHT_REPORT_ONLY:-}`, never
#     initialized — so an INHERITED or EXPORTED value converted the FULL gate's `exit 1` into
#     `return 1` at a bare call site with no errexit, the run continued, and the
#     `missing-schemas: FAIL-CLOSED` text could be stamped inside a block reading
#     `RESULT: PASS`. It is now a POSITIONAL ARGUMENT, which no export can supply. This case
#     exports the retired name anyway — and any other plausible spelling — because the point is
#     that the ENVIRONMENT cannot reach the mode at all, not that one variable was renamed.
ro_ok=1
for evil in _SCHEMAS_PREFLIGHT_REPORT_ONLY SCHEMAS_PREFLIGHT_REPORT_ONLY report_only mode; do
  ro_full="$tmp/3148-report-only-$evil.txt"
  env "$evil=1" CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
    CQLITE_SCHEMAS_ROOT="$schemas_empty" AGENT_GATE_SUMMARY_FILE="$ro_full" \
    timeout 180 bash "$GATE" >/dev/null 2>&1
  ro_rc=$?
  if [ "$ro_rc" -ne 0 ] \
     && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$ro_full" 2>/dev/null \
     && grep -q "^RESULT: FAIL" "$ro_full" 2>/dev/null \
     && ! grep -q "^RESULT: PASS" "$ro_full" 2>/dev/null; then
    :
  else
    ro_ok=0
    echo "   (exported $evil=1 weakened the guard: rc=$ro_rc)"
    cat "$ro_full" 2>/dev/null
  fi
done
if [ "$ro_ok" -eq 1 ]; then
  ok "3148-no-env-report-only: no exported variable can turn the fail-closed guard into a soft return"
else
  bad "3148-no-env-report-only: an environment value defeated the schemas fail-closed guard (requirement 8)"
fi

# 2f. A NON-UTF-8 override is REJECTED at the REAL emit (roborev job 11, BLOCKER) — the THIRD
#     instance of the certify-A-use-B class, alongside control-characters (2c) and relative
#     paths (2a). Bash handles the value as BYTES and used to accept it (measured: STATUS: OK +
#     SOURCE: override for a `bad\xff\xfedir` path) while Rust's `var_os(..).to_str()` cannot
#     represent it. The directory is REAL, so pre-fix the gate genuinely validated it — that is
#     what makes this discriminating. A legitimate NON-ASCII UTF-8 root must still be ACCEPTED,
#     asserted below, or the fix would be an over-broad ban on non-ASCII paths.
nu_root="$tmp/$(printf 'bad\xff\xfedir')"
mkdir -p "$nu_root"
for f in "${CANONICAL[@]}"; do printf -- '-- synthetic\n' >"$nu_root/$f"; done
nu_full="$tmp/3148-nu-full.txt"
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_corpus" \
  CQLITE_SCHEMAS_ROOT="$nu_root" AGENT_GATE_SUMMARY_FILE="$nu_full" \
  timeout 180 bash "$GATE" >"$tmp/3148-nu-full.out" 2>&1
nu_rc=$?
if [ "$nu_rc" -ne 0 ] \
   && grep -q "^missing-schemas: FAIL-CLOSED (#3148)" "$nu_full" 2>/dev/null \
   && grep -q "is not valid UTF-8" "$nu_full" 2>/dev/null \
   && grep -q "must be valid UTF-8" "$tmp/3148-nu-full.out" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$nu_full" 2>/dev/null \
   && ! grep -q "^RESULT: PASS" "$nu_full" 2>/dev/null \
   && ! grep -q "^schemas: " "$nu_full" 2>/dev/null; then
  ok "3148-non-utf8-override: a non-UTF-8 override fails closed at the real emit, naming the cause"
else
  bad "3148-non-utf8-override: a non-UTF-8 override did not fail closed (rc=$nu_rc)"
  cat "$nu_full" 2>/dev/null
fi

# …and the guard must not become a ban on non-ASCII paths: a legitimate UTF-8 root with
# multibyte characters is still a VALID override.
utf8_root="$tmp/schémas-ünïcode"
mkdir -p "$utf8_root"
for f in "${CANONICAL[@]}"; do printf -- '-- synthetic\n' >"$utf8_root/$f"; done
#
# TWO WORLDS, deliberately. `_gate_schemas_override_is_utf8` REJECTS a non-ASCII value when
# `iconv` is ABSENT ("could not check" must not mean "accept"). Asserting only the ACCEPT
# outcome made this pin demand a result the implementation does not promise on an iconv-less
# host, so `tooling-tests` — and with it the whole gate — would have redded there on a
# perfectly valid root (roborev job 12, finding 2). That is a false RED, not a false green,
# but a fleet foot-gun now that this self-test runs inside the gate. So assert whichever
# outcome is DOCUMENTED for the host we are on.
utf8_out=$(CQLITE_SCHEMAS_ROOT="$utf8_root" bash "$GATE" --preflight-schemas 2>/dev/null)
if command -v iconv >/dev/null 2>&1; then
  if [ "$(hook_field STATUS "$utf8_out")" = OK ] \
     && [ "$(hook_field SOURCE "$utf8_out")" = "CQLITE_SCHEMAS_ROOT override" ] \
     && [ "$(hook_field ROOT "$utf8_out")" = "$utf8_root" ]; then
    ok "3148-utf8-override-ok: a legitimate multibyte-UTF-8 override is still accepted (iconv present)"
  else
    bad "3148-utf8-override-ok: the UTF-8 guard over-rejected a valid non-ASCII root"
    printf '%s\n' "$utf8_out"
  fi
else
  # NOT a silent skip — on THIS host the documented result is a fail-closed rejection, so that
  # is what gets asserted. A case that quietly stops testing is the pattern this change has
  # already had to dig out of itself three times.
  if [ "$(hook_field STATUS "$utf8_out")" = FAIL ] \
     && printf '%s' "$utf8_out" | grep -q "must be valid UTF-8"; then
    ok "3148-utf8-override-ok: iconv ABSENT here, so a multibyte override fails closed (documented)"
  else
    bad "3148-utf8-override-ok: iconv absent, but a multibyte override was not rejected fail-closed"
    printf '%s\n' "$utf8_out"
  fi
fi

# The absent-`iconv` branch above is UNREACHABLE on a host that has iconv, which would leave
# the fail-closed-when-unverifiable promise permanently untested exactly where it matters.
# Exercise it for real: re-run the hook with a PATH farm symlinking every executable EXCEPT
# iconv, so `command -v iconv` genuinely fails while the gate still finds everything else.
noiconv_bin="$tmp/noiconv-bin"
mkdir -p "$noiconv_bin"
farm_n=0
for d in $(printf '%s\n' "$PATH" | tr ':' '\n'); do
  [ -d "$d" ] || continue
  for f in "$d"/*; do
    [ -x "$f" ] || continue
    b=${f##*/}
    [ "$b" = iconv ] && continue
    [ -e "$noiconv_bin/$b" ] || { ln -s "$f" "$noiconv_bin/$b" 2>/dev/null && farm_n=$((farm_n + 1)); }
  done
done
if [ "$farm_n" -gt 0 ] && [ -e "$noiconv_bin/bash" ] && [ ! -e "$noiconv_bin/iconv" ]; then
  noiconv_out=$(env PATH="$noiconv_bin" CQLITE_SCHEMAS_ROOT="$utf8_root" \
    bash "$GATE" --preflight-schemas 2>/dev/null)
  if [ "$(hook_field STATUS "$noiconv_out")" = FAIL ] \
     && [ "$(hook_field SOURCE "$noiconv_out")" = "CQLITE_SCHEMAS_ROOT override REJECTED" ] \
     && printf '%s' "$noiconv_out" | grep -q "must be valid UTF-8"; then
    ok "3148-utf8-no-iconv: iconv removed from PATH, so a VALID multibyte override fails CLOSED"
  else
    bad "3148-utf8-no-iconv: iconv absent from PATH but the multibyte override was not rejected fail-closed"
    printf '%s\n' "$noiconv_out"
  fi
else
  bad "3148-utf8-no-iconv: could not build an iconv-less PATH farm (symlinked=$farm_n) — case NOT exercised"
fi

# ---------------------------------------------------------------------------
# 3. AC (g): --lite and --only stay LENIENT (unchanged from #2078's contract).
# ---------------------------------------------------------------------------
lite_block="$tmp/3148-lite.txt"
CQLITE_DATASETS_ROOT="$ds_corpus" CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  AGENT_GATE_SUMMARY_FILE="$lite_block" \
  bash "$GATE" --lite --emit-summary-selftest >/dev/null 2>&1
lite_rc=$?
# `! grep '^schemas: '` as well as the marker: a LITE block must carry NO schemas line at
# all — neither a failure marker nor a POSITIVE assertion. run_lite always exits before
# apply_schemas_preflight, so SCHEMAS_LINE is never stamped; this pins that, so a future
# call-site move cannot start asserting readability in a mode that never checked it.
if [ "$lite_rc" -eq 0 ] \
   && grep -q "AGENT-GATE LITE SUMMARY" "$lite_block" 2>/dev/null \
   && ! grep -q "missing-schemas:" "$lite_block" 2>/dev/null \
   && ! grep -q "^schemas: " "$lite_block" 2>/dev/null; then
  ok "3148-lite: --lite unaffected by an unreachable schemas root (no schemas line at all)"
else
  bad "3148-lite: --lite must stay lenient (rc=$lite_rc)"
  cat "$lite_block" 2>/dev/null
fi

# The arg dispatch is a single `case "$1"`, so `--only X --preflight-schemas` is not
# expressible; the hook's optional 2nd arg seeds ONLY, exercising the SAME pure
# decision the real --only run consumes. `core-tests` is deliberately a DATASET
# component: even the selection that most needs schemas must stay lenient under --only.
only_status=$(CQLITE_SCHEMAS_ROOT="$schemas_empty" \
  bash "$GATE" --preflight-schemas core-tests 2>/dev/null | grep '^STATUS:' | sed 's/^STATUS: //')
if [ "$only_status" = OK ]; then
  ok "3148-only: --only stays lenient (STATUS OK even with the schemas root unreachable)"
else
  bad "3148-only: expected the --only selection to stay lenient (got '$only_status')"
fi

# ---------------------------------------------------------------------------
# 4. AC (f): the symlink trap is GONE, not papered over.
#
#    `join("..")` is not a lexical parent at the syscall level: the kernel resolves
#    `datasets/..` against the SYMLINK TARGET's parent. So a corpus reached through a
#    symlinked `datasets` used to mis-resolve `datasets/../schemas` silently. The fix
#    removes all `..` climbing, which is only meaningful if the schemas decision is
#    INDEPENDENT of $CQLITE_DATASETS_ROOT — asserted directly here across three
#    datasets layouts (real dir / symlink-to-elsewhere / nonexistent).
# ---------------------------------------------------------------------------
sym_parent="$tmp/sym-parent"
mkdir -p "$sym_parent"
ln -s "$ds_corpus" "$sym_parent/datasets"
indep=1
for layout in "$ds_corpus" "$sym_parent/datasets" "$tmp/does-not-exist/datasets"; do
  st=$(CQLITE_DATASETS_ROOT="$layout" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^STATUS:' | sed 's/^STATUS: //')
  rt=$(CQLITE_DATASETS_ROOT="$layout" bash "$GATE" --preflight-schemas 2>/dev/null \
    | grep '^ROOT:' | sed 's/^ROOT: //')
  { [ "$st" = OK ] && [ "$rt" = "$REPO/test-data/schemas" ]; } || indep=0
done
if [ "$indep" -eq 1 ]; then
  ok "3148-symlink-independence: schemas root is identical for real/symlinked/absent datasets roots"
else
  bad "3148-symlink-independence: the schemas root still varies with CQLITE_DATASETS_ROOT"
fi

# The structural half of AC (f)/(d): no code may reintroduce the `..` climb. Comment
# text is exempt (the doc comments deliberately quote the retired idiom); a real
# expression is a hard failure. `grep -v` on a leading `//`/`#` comment marker after
# the `path:line:` prefix is what makes that distinction.
climbs=$(grep -rn --include='*.rs' 'join("\.\./schemas")' "$REPO" 2>/dev/null \
  | grep -v ':[0-9]*: *//' || true)
if [ -z "$climbs" ]; then
  ok "3148-no-dotdot-climb: zero open-coded join(\"../schemas\") expressions in Rust code"
else
  bad "3148-no-dotdot-climb: an open-coded ../schemas climb was reintroduced:"
  printf '%s\n' "$climbs"
fi

# ---------------------------------------------------------------------------
# 5. AC (d)/(e): ONE shared resolution file, included by every historical site.
# ---------------------------------------------------------------------------
shared="$REPO/test-data/support/fixture_roots.rs"
if [ -f "$shared" ] \
   && grep -q 'pub fn resolve_schemas_root' "$shared" \
   && grep -q 'pub fn schemas_root_resolved' "$shared" \
   && grep -q 'pub fn datasets_root_if_present' "$shared" \
   && grep -q 'pub fn datasets_root' "$shared"; then
  ok "3148-shared-file: test-data/support/fixture_roots.rs defines the single contract"
else
  bad "3148-shared-file: the shared fixture-roots module is missing or incomplete"
fi

sites_ok=1
for site in \
  cqlite-core/benches/fixtures/mod.rs \
  cqlite-core/tests/dead_cache_delete_tests.rs \
  cqlite-core/tests/observability_correctness.rs \
  cqlite-cli/benches/export_csv.rs
do
  grep -q 'test-data/support/fixture_roots.rs' "$REPO/$site" || { sites_ok=0; echo "   (no include: $site)"; }
done
if [ "$sites_ok" -eq 1 ]; then
  ok "3148-all-sites: all four historical call sites include the shared module"
else
  bad "3148-all-sites: a call site no longer resolves roots through the shared module"
fi

# No second copy of the resolution may reappear WHERE #3148 removed one. Scope: the two
# bench trees plus the three files that carried the divergent `datasets_root()` copies.
# The wider `cqlite-core/tests/**` and `src/**` inline suites keep their own ad-hoc env
# reads — out of scope for #3148 (which names three copies), so asserting over them
# would be a scope claim this change does not make.
dupes=$(grep -rln --include='*.rs' 'env::var("CQLITE_DATASETS_ROOT")' \
  "$REPO/cqlite-core/benches" "$REPO/cqlite-cli/benches" \
  "$REPO/cqlite-core/tests/dead_cache_delete_tests.rs" \
  "$REPO/cqlite-core/tests/observability_correctness.rs" 2>/dev/null || true)
if [ -z "$dupes" ]; then
  ok "3148-no-dupe-root: no bench / migrated test re-reads CQLITE_DATASETS_ROOT directly"
else
  bad "3148-no-dupe-root: a datasets-root resolution copy reappeared:"
  printf '%s\n' "$dupes"
fi

# ---------------------------------------------------------------------------
# 6. #3131 items 1-2: fetch-datasets.sh must never report success while leaving a
#    root an operator cannot use, and must PRINT the export line it guarantees.
#    Driven through --verify-only, which performs no download/extraction/removal —
#    so this stays hermetic and never touches the real corpus or the tree.
# ---------------------------------------------------------------------------
FETCH="$REPO/test-data/scripts/fetch-datasets.sh"

# 6a. Hollow root (exists, empty): must FAIL LOUDLY with a remedy — never exit 0.
hollow="$tmp/hollow/datasets"
mkdir -p "$hollow"
hollow_out=$(CQLITE_DATASETS_ROOT="$hollow" bash "$FETCH" --verify-only 2>&1)
hollow_rc=$?
if [ "$hollow_rc" -ne 0 ] \
   && grep -q "does not hold a usable dataset corpus" <<<"$hollow_out" \
   && grep -q "remedy: re-run this script with the pin cleared" <<<"$hollow_out"; then
  ok "3131-hollow-root: an unusable root exits non-zero with a remedy (never a green no-op)"
else
  bad "3131-hollow-root: expected non-zero + remedy text (rc=$hollow_rc)"
  printf '%s\n' "$hollow_out"
fi

# 6a-bis. --verify-only must CREATE NOTHING (blocker B2). The first cut of case 6a
#         pre-`mkdir`ed its hollow root, which made it BLIND to exactly this bug:
#         `canonicalize_dataset_root` runs `mkdir -p "${parent}"` before the mode
#         dispatch, so probing a root under a nonexistent parent silently created that
#         parent and then reported the root unusable. The root here is therefore
#         deliberately NOT pre-created, and the assertion is on the filesystem, not on
#         the message.
absent_parent="$tmp/verify-nomutate"
absent_root="$absent_parent/v4/datasets"
nomutate_out=$(CQLITE_DATASETS_ROOT="$absent_root" bash "$FETCH" --verify-only 2>&1)
nomutate_rc=$?
if [ "$nomutate_rc" -ne 0 ] && [ ! -e "$absent_parent" ]; then
  ok "3131-verify-no-mutation: --verify-only creates nothing, even a missing parent dir"
else
  bad "3131-verify-no-mutation: expected non-zero AND no filesystem mutation (rc=$nomutate_rc, created: $(ls -d "$absent_parent" 2>&1))"
  printf '%s\n' "$nomutate_out"
fi

# 6a-ter. Unrecognized arguments must be REJECTED, not silently ignored (blocker B3).
#         The default path is DESTRUCTIVE (`rm -rf "${DATASET_ROOT}"` before extraction),
#         so `--quiet --verify-only` or any typo previously skipped the probe and reached
#         the rm -rf against the operator's corpus. Asserted with a real, POPULATED root:
#         if the rejection ever regresses, this case would attempt the destructive path,
#         so the surviving fixture is itself part of the assertion.
argsafe_ok=1
argsafe_root="$tmp/argsafe/datasets"
mkdir -p "$argsafe_root/sstables/test_basic/simple_table-0001"
: >"$argsafe_root/sstables/test_basic/simple_table-0001/nb-1-big-Data.db"
for badarg in "--quiet --verify-only" "-verify-only" "--verifyonly" "verify-only" "--Verify-Only"; do
  # shellcheck disable=SC2086  # intentional word-split: some cases pass TWO arguments
  out=$(CQLITE_DATASETS_ROOT="$argsafe_root" bash "$FETCH" $badarg 2>&1)
  rc=$?
  if [ "$rc" -ne 2 ] || ! grep -q "unrecognized argument" <<<"$out"; then
    argsafe_ok=0; echo "   (not rejected with exit 2: '$badarg' -> rc=$rc)"
  fi
done
if [ ! -f "$argsafe_root/sstables/test_basic/simple_table-0001/nb-1-big-Data.db" ]; then
  argsafe_ok=0; echo "   (DESTRUCTIVE path reached: the fixture Data.db was deleted)"
fi
if [ "$argsafe_ok" -eq 1 ]; then
  ok "3131-arg-safety: every unrecognized argument exits 2 before any destructive work"
else
  bad "3131-arg-safety: an unrecognized argument was not fail-closed"
fi

# …and the recognized flag plus --help still work (a fail-closed parser that rejects its
# own flag would be a silent regression of the probe).
help_out=$(bash "$FETCH" --help 2>&1); help_rc=$?
if [ "$help_rc" -eq 0 ] && grep -q -- '--verify-only' <<<"$help_out"; then
  ok "3131-arg-safety: --help documents --verify-only and exits 0"
else
  bad "3131-arg-safety: --help should exit 0 and document the flag (rc=$help_rc)"
fi

# 6b. A root holding the required content must report success AND print the exact
#     `export CQLITE_DATASETS_ROOT=<absolute path>` line it guarantees — the missing
#     half of #3131 item 2 (the pre-fix warm path named no actionable root at all).
good="$tmp/fetch-good/datasets"
wide="$good/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
mkdir -p "$wide" "$good/sstables/test_basic/simple_table-0001"
printf 'synthetic: true\n' >"$good/metadata.yml"
printf '{}\n' >"$wide/nb-2-big-Data.db.jsonl"
for c in nb-2-big-Data.db nb-2-big-Index.db nb-2-big-Digest.crc32 nb-2-big-CompressionInfo.db; do
  : >"$wide/$c"
done
for c in nb-1-big-Data.db nb-1-big-Index.db nb-1-big-Summary.db nb-1-big-Statistics.db; do
  : >"$good/sstables/test_basic/simple_table-0001/$c"
done
good_out=$(CQLITE_DATASETS_ROOT="$good" bash "$FETCH" --verify-only 2>&1)
good_rc=$?
if [ "$good_rc" -eq 0 ] \
   && grep -q "^  export CQLITE_DATASETS_ROOT=$good$" <<<"$good_out" \
   && grep -q "Dataset root VERIFIED" <<<"$good_out"; then
  ok "3131-export-line: a usable root is confirmed and prints its exact export line"
else
  bad "3131-export-line: expected exit 0 + the verbatim export line for $good (rc=$good_rc)"
  printf '%s\n' "$good_out"
fi

# 6b-bis. The export line must be PASTEABLE, not merely printed (roborev job 8, finding
#         3). A root containing a space or a shell metacharacter would, under plain
#         interpolation, print a command that breaks (or does something else) when
#         pasted — so the promise "the exact export line" would be false exactly when it
#         matters. Asserted by EVALUATING the printed line and comparing the resulting
#         variable to the real path: the strongest available statement of "pasteable".
spacey="$tmp/fetch space & meta/datasets"
spacey_wide="$spacey/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
mkdir -p "$spacey_wide" "$spacey/sstables/test_basic/simple_table-0001"
printf 'synthetic: true\n' >"$spacey/metadata.yml"
printf '{}\n' >"$spacey_wide/nb-2-big-Data.db.jsonl"
for c in nb-2-big-Data.db nb-2-big-Index.db nb-2-big-Digest.crc32 nb-2-big-CompressionInfo.db; do
  : >"$spacey_wide/$c"
done
for c in nb-1-big-Data.db nb-1-big-Index.db nb-1-big-Summary.db nb-1-big-Statistics.db; do
  : >"$spacey/sstables/test_basic/simple_table-0001/$c"
done
spacey_line=$(CQLITE_DATASETS_ROOT="$spacey" bash "$FETCH" --verify-only 2>/dev/null \
  | grep '^  export CQLITE_DATASETS_ROOT=' | sed 's/^  //')
spacey_eval=$(
  unset CQLITE_DATASETS_ROOT
  eval "$spacey_line" 2>/dev/null
  printf '%s' "${CQLITE_DATASETS_ROOT:-}"
)
if [ -n "$spacey_line" ] && [ "$spacey_eval" = "$spacey" ]; then
  ok "3131-export-quoting: the printed export line round-trips a path with spaces/metacharacters"
else
  bad "3131-export-quoting: pasting the line does not reproduce the root (line: '$spacey_line' -> '$spacey_eval')"
fi

# 6b-ter. A SYMLINKED dataset root must verify, not be reported unusable (roborev job 9,
#         finding 2). `find <symlink>` without `-H` stats the link and never descends, so
#         every count came back 0 and `has_required_content` rejected a perfectly good
#         corpus — and `ln -s <real corpus> <somewhere>/datasets` is precisely the layout
#         #3148 documents operators reaching for.
symparent="$tmp/symlinked"
mkdir -p "$symparent"
ln -s "$good" "$symparent/datasets"
sym_out=$(CQLITE_DATASETS_ROOT="$symparent/datasets" bash "$FETCH" --verify-only 2>&1)
sym_rc=$?
if [ "$sym_rc" -eq 0 ] \
   && grep -q "Dataset root VERIFIED" <<<"$sym_out" \
   && ! grep -q "0 \*-Data.db present" <<<"$sym_out"; then
  ok "3131-symlinked-root: a datasets root that is itself a symlink verifies with a real count"
else
  bad "3131-symlinked-root: expected exit 0 and a non-zero Data.db count (rc=$sym_rc)"
  printf '%s\n' "$sym_out"
fi

# 6d. The WARM-CACHE call site and the "NOT the checkout default" NOTE (spec-auditor,
#     requirement 12 partial). Every case above drives `--verify-only`; NOTHING exercised
#     `guarantee_usable_root "warm cache, download skipped"` or the divergence NOTE, so
#     deleting either line reddened nothing — and the warm-cache no-op is EXACTLY #3131 item
#     2's original defect ("a green fetch is not evidence the tree is usable").
#
#     Hermetic: the root carries content AND a `.dataset-pin` matching the TRACKED pin, so
#     `has_required_dataset` succeeds and the download is skipped. A failing `curl` stub is
#     first on PATH so that if the pin ever stops matching, the case dies WITHOUT network and
#     WITHOUT reaching `rm -rf` — a self-test must not be one typo away from a download.
warm="$tmp/fetch-warm/datasets"
warm_wide="$warm/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
mkdir -p "$warm_wide" "$warm/sstables/test_basic/simple_table-0001"
printf 'synthetic: true\n' >"$warm/metadata.yml"
printf '{}\n' >"$warm_wide/nb-2-big-Data.db.jsonl"
for c in nb-2-big-Data.db nb-2-big-Index.db nb-2-big-Digest.crc32 nb-2-big-CompressionInfo.db; do
  : >"$warm_wide/$c"
done
for c in nb-1-big-Data.db nb-1-big-Index.db nb-1-big-Summary.db nb-1-big-Statistics.db; do
  : >"$warm/sstables/test_basic/simple_table-0001/$c"
done
# The pin comes from the tracked source of truth, never hard-coded (issue #2646).
# shellcheck disable=SC1090
. "$REPO/test-data/dataset-pin.env"
{ echo "tag=${DATASET_TAG}"; echo "asset=${DATASET_ASSET}"; echo "sha256=${DATASET_SHA256}"; } >"$warm/.dataset-pin"
curl_stub="$tmp/curl-stub"
mkdir -p "$curl_stub"
printf '#!/bin/sh\necho "SELFTEST GUARD: curl invoked — the warm-cache path was NOT taken" >&2\nexit 1\n' >"$curl_stub/curl"
chmod +x "$curl_stub/curl"
warm_out=$(cd "$REPO" && PATH="$curl_stub:$PATH" CQLITE_DATASETS_ROOT="$warm" bash "$FETCH" 2>&1)
warm_rc=$?
if [ "$warm_rc" -eq 0 ] \
   && grep -q "already present in $warm; skipping download" <<<"$warm_out" \
   && grep -q "Dataset root VERIFIED (warm cache, download skipped)" <<<"$warm_out" \
   && grep -q "^  export CQLITE_DATASETS_ROOT=$warm$" <<<"$warm_out" \
   && ! grep -q "SELFTEST GUARD" <<<"$warm_out"; then
  ok "3131-warm-cache: the warm-skip path VERIFIES the root and prints the export line it guarantees"
else
  bad "3131-warm-cache: expected a verified warm-cache skip naming the root (rc=$warm_rc)"
  printf '%s\n' "$warm_out" | head -8
fi

# …and the divergence NOTE: the populated root here is NOT the checkout default, which is the
# fact that made the documented `CQLITE_DATASETS_ROOT=$PWD/test-data/datasets` silently wrong.
if grep -q "NOTE: this run populated $warm, NOT the checkout default" <<<"$warm_out" \
   && grep -q "NOTE:   $REPO/test-data/datasets" <<<"$warm_out" \
   && grep -q "NOTE: CQL schema fixtures (test-data/schemas) are committed source" <<<"$warm_out"; then
  ok "3131-warm-cache-note: the run names the root it populated AND the checkout default it did not"
else
  bad "3131-warm-cache-note: the divergence NOTE (or the schemas NOTE) is missing"
  printf '%s\n' "$warm_out" | head -12
fi

# 6e. The fetch script's REMEDY line must be pasteable too (roborev job 11, nit 3) — it
#     interpolated PIN_FILE/DATASET_ROOT unquoted, so a path with a space or a metacharacter
#     printed a command that breaks when followed. Same `eval` round-trip proof as the export
#     line: evaluate the printed `CQLITE_DATASETS_ROOT=...` assignment out of the remedy and
#     compare it to the real root.
rem_root="$tmp/rem space & meta/datasets"
mkdir -p "$rem_root"
rem_out=$(CQLITE_DATASETS_ROOT="$rem_root" bash "$FETCH" --verify-only 2>&1)
rem_line=$(printf '%s\n' "$rem_out" | grep 'CQLITE_DATASETS_ROOT=' | grep 'rm -f' | sed 's/^ERROR:   //')
# Extract ONLY the assignment. Evaluating the whole remedy would run its `rm -f`, and appending
# a command to the assignment would make it a per-command PREFIX (temporary), which never sets
# the variable in the shell — that mistake made this case fail on a correctly-quoted line.
rem_assign=$(printf '%s' "$rem_line" | sed 's/.*&& //; s/ bash .*//')
rem_eval=$(
  unset CQLITE_DATASETS_ROOT
  eval "$rem_assign" 2>/dev/null
  printf '%s' "${CQLITE_DATASETS_ROOT:-}"
)
if [ -n "$rem_line" ] && [ "$rem_eval" = "$rem_root" ]; then
  ok "3131-remedy-quoting: the failure remedy round-trips a path with spaces/metacharacters"
else
  bad "3131-remedy-quoting: the remedy line does not reproduce the root (line: '$rem_line' -> '$rem_eval')"
fi

# 6c. #2878 boundary: this change must NOT have touched the rm -rf /
#     restore_ci_tracked_dataset_files behavior. Both must still be present verbatim,
#     so a future reader can see the sibling defect was left to its own delivery.
if grep -q 'rm -rf "${DATASET_ROOT}"' "$FETCH" \
   && grep -q '\[ -n "${CI:-}" \] || return 0' "$FETCH"; then
  ok "3131-2878-boundary: rm -rf + restore_ci_tracked_dataset_files left untouched (#2878)"
else
  bad "3131-2878-boundary: the #2878-owned behavior was modified by this change"
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
