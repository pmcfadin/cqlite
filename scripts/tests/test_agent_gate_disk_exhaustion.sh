#!/usr/bin/env bash
# test_agent_gate_disk_exhaustion.sh -- the #3800 `disk-exhaustion:` SUMMARY marker.
#
# SUBJECT: `_disk_exhaustion_line` and its helpers, AS SHIPPED in scripts/agent-gate.sh.
#
# WHY IT EXISTS. A full gate died because the DISK filled, and the ONE artifact doctrine
# tells an agent to retain said `minimal-build: FAIL (611s)` beside 36/37 PASS and
# `tree-integrity: PASS`. Nothing in the block named the host; the reader debugged a
# minimal-features build that was never broken. #3800 adds a marker line that NAMES a
# recognised disk-exhaustion signature. This file is that line's pin.
#
# THE SUITE MEASURES THE SHIPPED CODE, NEVER A COPY. Every case EXTRACTS the functions out
# of scripts/agent-gate.sh (the repo idiom -- see test_cargo_output_parsers.sh and
# test_agent_gate_feature_matrix_annotation.sh) and runs them, so deleting or unrouting the
# helper REDS this suite instead of greening it. Extraction is FAIL-CLOSED and its own
# assertions come first: a suite that extracted nothing and then reported "no signature
# matched" would be an instance of the defect it exists to prevent.
#
# HERMETIC: temp dirs only. No cargo, no python3, no network, no datasets, no gh, no Docker.
# Registered in the gate's `tooling-tests` component BEFORE its python3 SKIP branch, because
# it needs nothing that branch guards.
#
# Run standalone:   bash scripts/tests/test_agent_gate_disk_exhaustion.sh
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# #2751 defence in depth: never let an inherited summary path be clobbered by the two
# end-to-end cases at the bottom -- each pins its own.
unset AGENT_GATE_SUMMARY_FILE

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if [ ! -r "$GATE" ]; then
  echo "FATAL: required file not readable: $GATE" >&2
  exit 1
fi

# Scratch root VALIDATED before anything is built under it and before the trap is armed:
# this script runs without `errexit` (every case must run so one failure cannot hide the
# rest), so an unchecked `mktemp -d` would leave $tmp EMPTY, derive root-level paths, and
# hand the EXIT trap an `rm -rf ""`.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-disk.XXXXXX") || {
  echo "FATAL: mktemp -d failed; refusing to run with an unset scratch root" >&2; exit 1; }
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  echo "FATAL: mktemp -d produced no usable directory ('$tmp'); refusing to run" >&2; exit 1; fi
trap 'chmod -R u+rwX "$tmp" 2>/dev/null; rm -rf "$tmp"' EXIT

ESC="$(printf '\033')"
NOSPACE='No space left on device'

# ─────────────────────────────────────────────────────────────────────────────────
# EXTRACTION (fail-closed). awk, not python3: this suite must stay runnable on a box
# with no python3, because the gate registers it OUTSIDE `tooling-tests`' python3 gate.
# ─────────────────────────────────────────────────────────────────────────────────
EX="$tmp/extracted.sh"
: > "$EX"

extract_region() {  # <start-regex-anchored-at-col-0> <end-regex-anchored-at-col-0>
  awk -v s="$1" -v e="$2" '
    !inb && $0 ~ s { inb=1; print; next }
    inb { print; if ($0 ~ e) exit }
  ' "$GATE"
}

EXTRACT_OK=1
# The array first (the closed signature set), then every function the line depends on.
# Column-zero anchors are what make this honest: a `^  ` shape would extract nothing and
# the fail-closed checks below would fire rather than a silent empty source file.
{
  # awk uses ERE, where `\(` is not a portable literal paren -- bracket expressions are.
  extract_region '^DISK_EXHAUSTION_SIGNATURES=[(]$' '^[)]$'
  for fn in _disk_safe _disk_abbrev _disk_df_probe _disk_gib _disk_free_leg \
            _disk_free_field _disk_scan_field _disk_exhaustion_line; do
    extract_region "^${fn}[(][)] [{]\$" '^[}]$'
  done
} >> "$EX"

for want in DISK_EXHAUSTION_SIGNATURES _disk_safe _disk_abbrev _disk_df_probe _disk_gib \
            _disk_free_leg _disk_free_field _disk_scan_field _disk_exhaustion_line; do
  if ! grep -q "^${want}" "$EX"; then
    bad "extract: '$want' was NOT extracted from the shipped agent-gate.sh -- every case below would be vacuous"
    EXTRACT_OK=0
  fi
done
if [ "$EXTRACT_OK" -eq 1 ]; then
  ok "extract: the shipped signature set + 8 helpers were extracted from scripts/agent-gate.sh"
fi
if bash -n "$EX" 2>/dev/null; then
  ok "extract: the extracted region is syntactically valid bash (the cases run the real thing)"
else
  bad "extract: the extracted region is not valid bash -- the shipped function shape changed and this extraction no longer captures it"
  EXTRACT_OK=0
fi
if [ "$EXTRACT_OK" -ne 1 ]; then
  echo "FATAL: extraction failed; refusing to report per-case results that would measure nothing" >&2
  printf '\npassed: %d  failed: %d\n' "$PASS" "$FAIL"
  exit 1
fi

# The subject harness. Every call runs in a SUBSHELL with the df-derived globals cleared,
# so the free field is deterministic (`free: UNMEASURED`) except in the one case that
# deliberately seeds them.
run_line() {  # <logdir> <name> <status> ...
  local ldir="$1"; shift
  (
    . "$EX"
    LOG_DIR="$ldir"
    DISK_TARGET_PATH=""; DISK_LOGS_PATH=""
    DISK_FREE_START_TARGET=""; DISK_FREE_START_LOGS=""
    _disk_exhaustion_line "$@"
  )
}

# ─────────────────────────────────────────────────────────────────────────────────
# The closed signature set is what the doctrine claims it is.
# ─────────────────────────────────────────────────────────────────────────────────
sig_count=$(grep -c "^  '" "$EX")
if [ "$sig_count" -eq 3 ]; then
  ok "signatures: the shipped set holds exactly 3 entries (closed set, as documented)"
else
  bad "signatures: expected 3 entries in DISK_EXHAUSTION_SIGNATURES, found $sig_count -- the doctrine text and the code disagree"
fi
# A BARE `ENOSPC` token must stay OUT: it occurs in this repo's own test names/comments.
if grep -q "^  'enospc\||ENOSPC'" "$EX"; then
  bad "signatures: a bare ENOSPC token was added to the closed set -- it fires on logs that merely MENTION the class (#3800 declared exclusion)"
else
  ok "signatures: a bare ENOSPC token is deliberately EXCLUDED from the closed set"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (1)(2)(3) each signature of the closed set is RECOGNISED on a non-PASS component.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c1"; mkdir -p "$d"
{ echo "line one"; echo "line two"; echo "error: failed to write: $NOSPACE (os error 28)"; } > "$d/minimal-build.log"
out=$(run_line "$d" fmt PASS minimal-build FAIL)
if [ "${out#disk-exhaustion: RECOGNISED (#3800)}" != "$out" ] \
   && case "$out" in *"'no-space-left-on-device'"*) true ;; *) false ;; esac \
   && case "$out" in *"component 'minimal-build'"*) true ;; *) false ;; esac \
   && case "$out" in *"minimal-build.log:3"*) true ;; *) false ;; esac; then
  ok "1-no-space: RECOGNISED, naming the signature, the component and the line number (minimal-build.log:3)"
else
  bad "1-no-space: expected RECOGNISED naming signature+component+line; got: $out"
fi
# The RECOGNISED line reports EVIDENCE, never a CONCLUSION (roborev job 299). It must name the
# HOST as what the signature is consistent with, give the remedy, and DECLINE to clear the diff.
if case "$out" in *"CONSISTENT WITH disk exhaustion on this HOST"*"free space and re-run before treating this FAIL as a defect in the diff"*) true ;; *) false ;; esac; then
  ok "1-no-space: the line attributes the signature to the HOST and gives the remedy before any diff conclusion"
else
  bad "1-no-space: the RECOGNISED line does not carry the host-attribution + remedy text; got: $out"
fi
if case "$out" in *"EVIDENCE, NOT PROOF"*"the diff is NOT thereby cleared"*) true ;; *) false ;; esac; then
  ok "1-no-space: the line states plainly that it is evidence, not proof, and does NOT clear the diff"
else
  bad "1-no-space: the RECOGNISED line omits the evidence-not-proof qualifier; got: $out"
fi
# NEGATIVE: the retired wording asserted innocence the scan cannot support, and contradicted this
# marker's own doctrine. A test can PRINT a signature into its own log, and a diff CAN itself drive
# disk usage -- so this exact claim must never come back.
if case "$out" in *"NOT a defect in the diff"*) true ;; *) false ;; esac; then
  bad "1-no-space: the RECOGNISED line asserts 'NOT a defect in the diff' -- an unsupportable conclusion (roborev job 299); report evidence, not innocence"
else
  ok "1-no-space: the line never asserts the diff is NOT at fault (an unsupportable conclusion)"
fi
if case "$out" in *"This is an ATTRIBUTION and does NOT change RESULT."*) true ;; *) false ;; esac; then
  ok "1-no-space: the ATTRIBUTION-not-a-verdict clause is retained"
else
  bad "1-no-space: the RECOGNISED line dropped the 'ATTRIBUTION and does NOT change RESULT' clause; got: $out"
fi

d="$tmp/c2"; mkdir -p "$d"
{ echo "a"; echo 'thread panicked: Os { code: 28, kind: StorageFull, message: "..." } os error 28'; } > "$d/core-tests.log"
out=$(run_line "$d" core-tests FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"'os-error-28'"*"core-tests.log:2"*) true ;; *) false ;; esac; then
  ok "2-os-error-28: RECOGNISED at core-tests.log:2"
else
  bad "2-os-error-28: expected RECOGNISED 'os-error-28'; got: $out"
fi

d="$tmp/c3"; mkdir -p "$d"
echo 'sccache: failed: Disk quota exceeded' > "$d/smoke.log"
out=$(run_line "$d" smoke FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"'disk-quota-exceeded'"*"smoke.log:1"*) true ;; *) false ;; esac; then
  ok "3-disk-quota: RECOGNISED at smoke.log:1"
else
  bad "3-disk-quota: expected RECOGNISED 'disk-quota-exceeded'; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (4) #3400: an ANSI-COLOURED log is still detected. Real ESC bytes, injected via printf --
# never a hand-typed two-character `\x1b`, which would make this case test nothing. The
# cargo status word carries the colour and the reset lands BEFORE the payload, exactly as
# measured on #3400, and the signature is pure payload so it survives.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c4"; mkdir -p "$d"
{
  printf '%s[1m%s[92m   Compiling%s[0m cqlite-core v0.14.0\n' "$ESC" "$ESC" "$ESC"
  printf '%s[1m%s[91merror%s[0m: failed to write output: %s\n' "$ESC" "$ESC" "$ESC" "$NOSPACE"
} > "$d/minimal-build.log"
esc_n=$(LC_ALL=C tr -cd '\033' < "$d/minimal-build.log" | wc -c | tr -d ' ')
if [ "$esc_n" -gt 0 ]; then
  ok "4-ansi fixture provenance: the coloured log carries $esc_n real ESC bytes"
else
  bad "4-ansi fixture provenance: the 'coloured' fixture carries 0 ESC bytes -- the case below would be vacuous"
fi
out=$(run_line "$d" minimal-build FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"'no-space-left-on-device'"*"minimal-build.log:2"*) true ;; *) false ;; esac; then
  ok "4-ansi: the signature is detected in a COLOURED log (payload carries no escapes -- #3400)"
else
  bad "4-ansi: an ANSI-coloured log defeated detection; got: $out"
fi
# The disk-safety property that motivated NOT using _ansi_stripped_log: the scan must not
# materialise a sibling file. A diagnostic that needs free disk is useless under ENOSPC.
if [ -z "$(find "$d" -name '*.ansi-stripped' 2>/dev/null)" ] && [ "$(find "$d" -type f | wc -l | tr -d ' ')" = 1 ]; then
  ok "4-ansi: the scan wrote NO sibling file (it reads the RAW log; a diagnostic needing free disk is useless under ENOSPC)"
else
  bad "4-ansi: the scan materialised a file under the log dir -- it is routed through _ansi_stripped_log and will fail closed exactly when the disk is full"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (5) a clean FAILing log -> `0 RECOGNISED`, asserted as the LITERAL string. A bare `0`
# reads as a verified all-clear from a scan documented as incomplete.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c5"; mkdir -p "$d"
{ echo 'error[E0308]: mismatched types'; echo 'error: could not compile `cqlite-core`'; } > "$d/minimal-build.log"
out=$(run_line "$d" fmt PASS minimal-build FAIL)
if case "$out" in "disk-exhaustion: 0 RECOGNISED (#3800)"*) true ;; *) false ;; esac; then
  ok "5-clean: a clean FAILing log yields the literal '0 RECOGNISED', never a bare '0'"
else
  bad "5-clean: expected a leading 'disk-exhaustion: 0 RECOGNISED (#3800)'; got: $out"
fi
if case "$out" in *"scanned 1 non-PASS component log(s) (minimal-build)"*"every subject log was READ"*) true ;; *) false ;; esac; then
  ok "5-clean: the clean verdict is keyed on the AFFIRMATIVE fact (every subject log was READ) and names its subject"
else
  bad "5-clean: the clean verdict does not state that every subject log was read; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (6) every component PASS -> no subject at all.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c6"; mkdir -p "$d"
out=$(run_line "$d" fmt PASS clippy PASS smoke PASS)
if case "$out" in "disk-exhaustion: 0 RECOGNISED (#3800) -- no non-PASS component to scan (3/3 PASS)"*) true ;; *) false ;; esac; then
  ok "6-all-pass: '0 RECOGNISED -- no non-PASS component to scan (3/3 PASS)'"
else
  bad "6-all-pass: expected the no-subject rendering with an N/N PASS count; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (7) a PASSing component whose log CONTAINS the phrase is NOT a subject. Scanning it
# would report a signature that explains nothing -- the run recovered.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c7"; mkdir -p "$d"
echo "warning: transient: $NOSPACE (retried, succeeded)" > "$d/core-tests.log"
echo 'error[E0425]: cannot find value' > "$d/minimal-build.log"
out=$(run_line "$d" core-tests PASS minimal-build FAIL)
if case "$out" in "disk-exhaustion: 0 RECOGNISED"*) true ;; *) false ;; esac \
   && case "$out" in *core-tests*) false ;; *) true ;; esac; then
  ok "7-no-false-positive: a PASSing component's log carrying the phrase is neither scanned nor named"
else
  bad "7-no-false-positive: a PASSing component's log leaked into the verdict; got: $out"
fi
# ...and the SAME log under a non-PASS status IS reported -- the positive control, without
# which the case above could pass because the scan is simply broken.
out=$(run_line "$d" core-tests FAIL minimal-build FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED"*"component 'core-tests'"*) true ;; *) false ;; esac; then
  ok "7-positive-control: the SAME log under a FAIL status IS reported (case 7 discriminates on status, not on a broken scan)"
else
  bad "7-positive-control: flipping core-tests to FAIL did not produce RECOGNISED; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (8) an absent / unreadable subject log -> UNMEASURED, and NEVER a claim of a clean scan.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c8"; mkdir -p "$d"
out=$(run_line "$d" minimal-build FAIL)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"minimal-build(no log)"*) true ;; *) false ;; esac; then
  ok "8-no-log: a non-PASS component with NO log is UNMEASURED, naming it"
else
  bad "8-no-log: expected UNMEASURED naming the absent subject; got: $out"
fi
if case "$out" in *"never reported as 'no signature'"*) true ;; *) false ;; esac \
   && case "$out" in *"0 RECOGNISED"*|*"every subject log was READ"*) false ;; *) true ;; esac; then
  ok "8-no-log: UNMEASURED is not permissive -- the line makes no clean-scan claim"
else
  bad "8-no-log: the UNMEASURED line claims a clean scan; got: $out"
fi
# grep's rc is THREE-valued: a subject that is a DIRECTORY makes grep exit 2 ("Is a
# directory"). Collapsing >=2 onto "no match" is the two-valued-predicate defect. This
# variant works as root, where a chmod-000 file is still readable.
d="$tmp/c8b"; mkdir -p "$d/write-tests.log"
out=$(run_line "$d" write-tests FAIL)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"write-tests.log(unreadable)"*) true ;; *) false ;; esac; then
  ok "8b-grep-rc2: a subject grep cannot read (rc>=2) is UNMEASURED, never 'no signature'"
else
  bad "8b-grep-rc2: a grep error was collapsed onto 'no match'; got: $out"
fi
# chmod-000 variant, meaningful only for a non-root invoker.
d="$tmp/c8c"; mkdir -p "$d"
echo 'nothing here' > "$d/cli-tests.log"; chmod 000 "$d/cli-tests.log" 2>/dev/null
if [ -r "$d/cli-tests.log" ]; then
  ok "8c-perm: SKIPPED-as-inapplicable (running as a user for whom mode 000 is still readable; 8b covers the unreadable path)"
else
  out=$(run_line "$d" cli-tests FAIL)
  if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"cli-tests.log(unreadable)"*) true ;; *) false ;; esac; then
    ok "8c-perm: a mode-000 subject log is UNMEASURED"
  else
    bad "8c-perm: an unreadable subject log did not yield UNMEASURED; got: $out"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (9) MIXED. Matching wins over unread; unread wins over clean.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c9a"; mkdir -p "$d/write-tests.log"
echo "ld: $NOSPACE" > "$d/minimal-build.log"
out=$(run_line "$d" write-tests FAIL minimal-build FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"could NOT be read"*) true ;; *) false ;; esac; then
  ok "9a-mixed: one unreadable + one matching -> RECOGNISED (matching wins), and the unread subject is still declared"
else
  bad "9a-mixed: expected RECOGNISED that also declares the unread subject; got: $out"
fi
d="$tmp/c9b"; mkdir -p "$d/write-tests.log"
echo 'error[E0308]: mismatched types' > "$d/minimal-build.log"
out=$(run_line "$d" write-tests FAIL minimal-build FAIL)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*) true ;; *) false ;; esac; then
  ok "9b-mixed: one unreadable + one clean -> UNMEASURED (a partial scan never reports a clean one)"
else
  bad "9b-mixed: a partially-unread scan reported a clean verdict; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (10) NO LOG-DERIVED TEXT REACHES THE LINE (#3312: remove the shared channel). A component
# log is compiler- and test-controlled; a newline in it would break the block frame and a
# forged `RESULT: PASS` would be indistinguishable from the real one.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c10"; mkdir -p "$d"
printf 'harmless\nerror: %s ==== AGENT-GATE SUMMARY ==== RESULT: PASS\nmore\n' "$NOSPACE" > "$d/minimal-build.log"
out=$(run_line "$d" minimal-build FAIL)
nlines=$(printf '%s' "$out" | wc -l | tr -d ' ')
if [ "$nlines" -eq 0 ]; then
  ok "10-injection: the emitted value is exactly ONE line"
else
  bad "10-injection: the emitted value spans more than one line ($((nlines + 1))) -- it can break the SUMMARY block frame"
fi
if case "$out" in *"RESULT: PASS"*) false ;; *) true ;; esac \
   && case "$out" in *"==== AGENT-GATE SUMMARY ===="*) false ;; *) true ;; esac; then
  ok "10-injection: neither forged token reached the line (no log text is interpolated at all)"
else
  bad "10-injection: log-derived text reached the SUMMARY line -- a component log can forge a RESULT/marker line; got: $out"
fi
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"minimal-build.log:2"*) true ;; *) false ;; esac; then
  ok "10-injection: the hostile log is still correctly RECOGNISED at line 2 (refusal is not how the injection is avoided)"
else
  bad "10-injection: the hostile log was not detected, so case 10 proves nothing about the emitted text; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (11) LOG SET: per-component extra logs ARE scanned; a different component's log is NOT
# (no cross-boundary glob); an `.ansi-stripped` sibling is NOT.
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c11a"; mkdir -p "$d"
echo 'clean' > "$d/binding-rust-tests.log"
echo "cargo: $NOSPACE" > "$d/binding-rust-tests.cqlite-ffi-common.log"
out=$(run_line "$d" binding-rust-tests FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"binding-rust-tests.cqlite-ffi-common.log:1"*) true ;; *) false ;; esac; then
  ok "11a-sublogs: a per-subject '<name>.<sub>.log' is scanned and named"
else
  bad "11a-sublogs: the extra per-component log was not scanned; got: $out"
fi
d="$tmp/c11b"; mkdir -p "$d"
echo 'clean' > "$d/smoke.log"
echo "cargo: $NOSPACE" > "$d/smoke-extra.log"     # a DIFFERENT component; not a subject here
out=$(run_line "$d" smoke FAIL)
if case "$out" in "disk-exhaustion: 0 RECOGNISED"*) true ;; *) false ;; esac; then
  ok "11b-no-cross-boundary: 'smoke-extra.log' is NOT attributed to component 'smoke' (the glob is '<name>.log' + '<name>.*.log', never '<name>*.log')"
else
  bad "11b-no-cross-boundary: a peer component's log was attributed across the boundary; got: $out"
fi
d="$tmp/c11c"; mkdir -p "$d"
echo 'clean' > "$d/cli-tests.log"
echo "cargo: $NOSPACE" > "$d/cli-tests.log.ansi-stripped"
out=$(run_line "$d" cli-tests FAIL)
if case "$out" in "disk-exhaustion: 0 RECOGNISED"*) true ;; *) false ;; esac; then
  ok "11c-no-ansi-sibling: a '<log>.ansi-stripped' sibling left by another guard is not scanned (it would double-count one log)"
else
  bad "11c-no-ansi-sibling: an .ansi-stripped sibling was scanned; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (12) THE FREE FIELD IS A DELTA, NOT AN INSTANTANEOUS READ. Seeded start captures render
# start->emit; absent ones render UNMEASURED WITHOUT making the whole line UNMEASURED
# (the log scan is the primary oracle and the two facts stay independent).
# ─────────────────────────────────────────────────────────────────────────────────
d="$tmp/c12"; mkdir -p "$d"
echo "ld: $NOSPACE" > "$d/minimal-build.log"
out=$(
  . "$EX"
  LOG_DIR="$d"
  DISK_TARGET_PATH=""; DISK_LOGS_PATH=""
  DISK_FREE_START_TARGET="124143616 /data"; DISK_FREE_START_LOGS="124143616 /data"
  _disk_exhaustion_line minimal-build FAIL
)
if case "$out" in *"free(start->emit): target+logs fs /data 118.3G->?"*) true ;; *) false ;; esac; then
  ok "12-free-delta: a same-filesystem pair is reported ONCE as a start->emit delta"
else
  bad "12-free-delta: expected a single 'target+logs fs /data <start>-><emit>' field; got: $out"
fi
out=$(
  . "$EX"
  LOG_DIR="$d"
  DISK_TARGET_PATH=""; DISK_LOGS_PATH=""
  DISK_FREE_START_TARGET="124143616 /data"; DISK_FREE_START_LOGS="20971520 /tmp"
  _disk_exhaustion_line minimal-build FAIL
)
if case "$out" in *"target fs /data "*"logs fs /tmp "*) true ;; *) false ;; esac; then
  ok "12-free-two-fs: two distinct filesystems are reported as two legs"
else
  bad "12-free-two-fs: expected separate target/logs legs; got: $out"
fi
out=$(run_line "$d" minimal-build FAIL)   # no start capture at all
if case "$out" in "disk-exhaustion: RECOGNISED"*"free: UNMEASURED"*) true ;; *) false ;; esac; then
  ok "12-free-independent: an unmeasurable df renders 'free: UNMEASURED' and does NOT downgrade the RECOGNISED verdict"
else
  bad "12-free-independent: an unmeasurable df changed the log-scan verdict; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (13) THE SCAN DECLARES ITS OWN NON-EXHAUSTIVENESS, in every rendering.
# ─────────────────────────────────────────────────────────────────────────────────
declared=0; total_rend=0
for probe_out in \
  "$(run_line "$tmp/c1" minimal-build FAIL)" \
  "$(run_line "$tmp/c5" minimal-build FAIL)" \
  "$(run_line "$tmp/c6" fmt PASS)" \
  "$(run_line "$tmp/c8" minimal-build FAIL)" ; do
  total_rend=$((total_rend + 1))
  case "$probe_out" in *"NON-EXHAUSTIVE by construction"*"DECLARED false negative"*) declared=$((declared + 1)) ;; esac
done
if [ "$declared" -eq "$total_rend" ]; then
  ok "13-declared-gap: all $total_rend renderings (RECOGNISED / clean / no-subject / UNMEASURED) declare the closed set's non-exhaustiveness"
else
  bad "13-declared-gap: only $declared of $total_rend renderings declare the scan's own non-exhaustiveness"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (14) STRUCTURAL: a CENSUS over EVERY emit site in the shipped script, derived from
# source -- never a hard-coded list and never a count.
#
# WHY IT REPLACED THE OLD CASE (roborev job 299, finding 1). The previous case derived
# its subject set from sites containing `_fm_summary_line` -- i.e. only the sites that
# carry a COMPONENT TABLE, which are exactly the sites already compliant. It was
# CIRCULAR: it could only ever find sites that were already marked, so it could not see
# (and did not see) that the doctrine's "every terminal block carries the line" claim was
# false for the other 19 call sites. A guard whose subject set is the compliant set is a
# guard that cannot fail.
#
# THE TRUE CONTRACT, which this census enforces: every emit site that carries a COMPONENT
# TABLE appends the attribution line; every OTHER site is DECLARED exempt, at the site, with
# a stated reason. Blocks emitted before any component runs have nothing to attribute -- the
# line could only render a misleading `0 RECOGNISED ... (0/0 PASS)` -- and each already names
# its own cause with its own dedicated marker, so adding the line there would be noise that
# dilutes the marker's meaning.
#
# Verdicts, on the `component-set-exempt:` census's own model (this file mirrors
# scripts/tests/test_agent_gate_component_set.sh's derivation deliberately -- one idiom,
# not two):
#   MARKED-DIRECT           the call's own argument list contains _disk_exhaustion_line;
#   MARKED-VIA-<ARRAY>      the call passes "${ARRAY[@]}" and the array's OWN construction
#                           region pushes the line;
#   MARKED-VIA-RENDERER-<f> the array is filled from a function whose BODY emits the line;
#   EXEMPT                  a contiguous comment line above the site (or the site line
#                           itself) carries `disk-exhaustion-exempt: <reason>`, non-empty;
#   GAP                     none of the above => FAIL, naming the site.
# So a NEW emit site lands in GAP with NO edit to this file, which is the property that
# stops this class recurring. MARKED is tested BEFORE EXEMPT: observation beats declaration,
# so a site that really does append the line reads MARKED even if someone also exempted it.
#
# Two derivation details that are load-bearing, both learned by the sibling census:
#   * a renderer/array scan counts a token only on a CODE line. `_tree_boundary_meta_lines`
#     contains the WORDS "disk-exhaustion:" in its DECLARED-OMISSION comment; counting a
#     comment would make a census satisfied by a sentence ABOUT the check.
#   * the exemption is read from a CONTIGUOUS comment block above the site, not from the
#     single line above, because several of these sites already carry a
#     `component-set-exempt:` annotation on that line and the sibling census reads exactly
#     line i-1. Splitting the two would red that suite.
# ─────────────────────────────────────────────────────────────────────────────────
DISK_CENSUS_AWK="$tmp/disk-emit-census.awk"
cat >"$DISK_CENSUS_AWK" <<'DISK_CENSUS_PROG'
{ line[NR] = $0 }
function _real_exemption(t,   r) {
  if (t !~ /disk-exhaustion-exempt:[ \t]*[^ \t]/) return 0
  r = t; sub(/^.*disk-exhaustion-exempt:[ \t]*/, "", r)
  sub(/[ \t]+$/, "", r)
  if (r == "") return 0
  if (r ~ /^<.*>$/) return 0
  if (tolower(r) ~ /^(why|todo|tbd|reason)\.?$/) return 0
  return 1
}
function _marking_fn(fn,   k, inside) {
  inside = 0
  for (k = 1; k <= NR; k++) {
    if (line[k] ~ ("^" fn "\\(\\) \\{")) { inside = 1; continue }
    if (inside && line[k] ~ /^\}/) return 0
    if (inside && line[k] !~ /^[ \t]*#/ && line[k] ~ /_disk_exhaustion_line/) return 1
  }
  return 0
}
END {
  for (i = 1; i <= NR; i++) {
    l = line[i]
    if (l ~ /^[ \t]*#/) continue
    if (l ~ /^(emit_summary|_emit_terminal_summary)\(\)/) continue
    if (l !~ /(^|[^_a-zA-Z])(emit_summary|_emit_terminal_summary)[ \t]/) continue
    args = l; j = i
    while (args ~ /\\[ \t]*$/) { j++; args = args "\n" line[j] }
    verdict = "GAP"
    if (args ~ /_disk_exhaustion_line/) verdict = "MARKED-DIRECT"
    else if (match(args, /\$\{[A-Za-z_]+\[@\]\}/)) {
      nm = substr(args, RSTART + 2, RLENGTH - 6)
      for (k = i; k > 0; k--) {
        if (line[k] ~ ("(declare -a |local -a )?" nm "=\\(")) break
        if (k < i && line[k] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{|^\}/) break
        if (line[k] !~ /^[ \t]*#/ && line[k] ~ ("" nm "\\+=\\(\"\\$\\(_disk_exhaustion_line")) { verdict = "MARKED-VIA-" nm; break }
        if (match(line[k], /< <\(([A-Za-z_][A-Za-z0-9_]*)\)/)) {
          fn = substr(line[k], RSTART + 4, RLENGTH - 5)
          if (_marking_fn(fn)) { verdict = "MARKED-VIA-RENDERER-" fn; break }
        }
      }
    }
    if (verdict == "GAP") {
      # The exemption: the site line itself, or a CONTIGUOUS comment block above it.
      # The reason must be SUBSTANTIVE -- _real_exemption refuses an unsubstituted `<...>`
      # placeholder and the bare why/todo/tbd vocabulary, exactly as scripts/flow/claim.sh:795
      # refuses them for --reason. Without it, the `disk-exhaustion-exempt: <reason>`
      # PLACEHOLDER inside this contract's OWN doctrine comment would silently exempt any emit
      # site a future edit happens to add beneath it: an artifact that DESCRIBES the escape
      # hatch BECOMING it, which is #3312's shape one directory over.
      if (_real_exemption(args)) verdict = "EXEMPT"
      else for (k = i - 1; k > 0; k--) {
        if (line[k] !~ /^[ \t]*#/) break
        if (_real_exemption(line[k])) { verdict = "EXEMPT"; break }
      }
    }
    printf "%s\t%d\t%s\n", verdict, i, substr(l, 1, 70)
  }
}
DISK_CENSUS_PROG

disk_census() { awk -f "$DISK_CENSUS_AWK" "${1:-$GATE}"; }
dc_out=$(disk_census)
dc_sites=$(printf '%s\n' "$dc_out" | grep -c '	')
dc_marked=$(printf '%s\n' "$dc_out" | grep -c '^MARKED')
dc_exempt=$(printf '%s\n' "$dc_out" | grep -c '^EXEMPT	')
dc_gaps=$(printf '%s\n' "$dc_out" | grep -c '^GAP	')

# FAIL-CLOSED on the derivation itself: a scan that finds nothing is a clean census of
# nothing, which is the vacuous pass this suite exists to prevent.
if [ "$dc_sites" -ge 20 ]; then
  ok "14-census: derived $dc_sites emit sites from the shipped source ($dc_marked marked, $dc_exempt declared exempt) -- no hard-coded list, no hard-coded count"
else
  bad "14-census: the emit-site derivation found only $dc_sites call sites in $GATE -- the call shape changed and this census would be vacuous"
fi
if [ "$dc_sites" -gt 0 ] && [ "$dc_gaps" -eq 0 ]; then
  ok "14-census: all $dc_sites emit sites are ACCOUNTED FOR -- each either appends the disk-exhaustion line or carries 'disk-exhaustion-exempt: <reason>'"
else
  bad "14-census: $dc_gaps of $dc_sites emit site(s) neither append the disk-exhaustion line nor carry 'disk-exhaustion-exempt: <reason>':"
  printf '%s\n' "$dc_out" | grep '^GAP	' | while IFS='	' read -r _v _ln _src; do
    echo "   line $_ln: $_src"
  done
fi

# ─────────────────────────────────────────────────────────────────────────────────
# The COMPONENT-TABLE sites specifically. The census above proves ACCOUNTABILITY, which an
# author could satisfy by exempting a table-bearing site; this pins the stronger half --
# a block that carries a component table may never be exempted away.
#
# ITS SUBJECT SET USED TO BE CIRCULAR ONE LEVEL DOWN (#3800, final round). It derived
# table-bearing sites from `_fm_summary_line` -- ONE renderer's NAME -- so the ONE block that
# renders a component table with its own `printf '%-18s %s (%ss)'`, the #2926 tree-integrity
# COMPONENT-BOUNDARY FAIL (`_tree_boundary_meta_lines`), was INVISIBLE to it. That is the same
# defect roborev found in the parent census (a subject set that is the compliant set), and it
# is why that site survived a round exempted on reasoning that does not hold: a
# `tree-integrity: FAIL` is itself reachable from ENOSPC, because the capture manifest is
# written into $LOG_DIR and `TREE_CAPTURE_FAIL_REASON` is a FIXED CONSTANT that cannot name
# disk.
#
# THE DERIVATION IS NOW OVER THE ROW FORMAT, NOT OVER A RENDERER'S NAME, in two steps:
#   1. a function is a COMPONENT-ROW RENDERER if its body emits the canonical verdict-row
#      format `%-18s %s (%s` on a CODE line;
#   2. a table-bearing ROW SITE is any code line OUTSIDE every renderer's own body that
#      either contains that format directly or CALLS a renderer.
# Each row site maps to the first emit call at or after it, and those emit sites are DEDUPED,
# so the count is table-bearing BLOCKS. A third renderer written tomorrow is recognised with
# no edit to this file, which is the property that stops this class recurring a third time.
# The verdict always comes from the CENSUS, never from a second derivation that could disagree.
# ─────────────────────────────────────────────────────────────────────────────────
DISK_TABLE_AWK="$tmp/disk-table-sites.awk"
cat >"$DISK_TABLE_AWK" <<'DISK_TABLE_PROG'
{ line[NR] = $0 }
END {
  fmt = "%-18s %s (%s"           # the canonical per-component verdict row
  fn = ""
  for (i = 1; i <= NR; i++) {
    l = line[i]
    if (l ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/) { fn = l; sub(/\(\).*$/, "", fn); fnstart[fn] = i; continue }
    if (fn != "" && l ~ /^\}/) { fnend[fn] = i; fn = "" ; continue }
    if (fn != "" && l !~ /^[ \t]*#/ && index(l, fmt) > 0) renderer[fn] = 1
  }
  nr = 0; names = ""
  for (f in renderer) { nr++; names = names (names == "" ? "" : ",") f; if (!(f in fnend)) fnend[f] = NR }
  printf "RENDERERS\t%d\t%s\n", nr, names
  for (i = 1; i <= NR; i++) {
    l = line[i]
    if (l ~ /^[ \t]*#/) continue
    inside = 0
    for (f in renderer) if (i >= fnstart[f] && i <= fnend[f]) inside = 1
    if (inside) continue          # a renderer's own definition is not a USE of it
    hit = ""
    if (index(l, fmt) > 0) hit = "inline-format"
    else for (f in renderer) if (l ~ ("(^|[^_a-zA-Z0-9])" f "([^_a-zA-Z0-9]|$)")) { hit = f; break }
    if (hit != "") printf "%d\t%s\n", i, hit
  }
}
DISK_TABLE_PROG

table_renderers() { awk -f "$DISK_TABLE_AWK" "${1:-$GATE}" | awk -F'\t' '$1=="RENDERERS"{print $2"\t"$3}'; }
# table_verdicts <file> -> "<emit-line>\t<census-verdict>\t<row-line>\t<via>" per DISTINCT
# table-bearing emit site.
table_verdicts() {
  local f="${1:-$GATE}" cen rl how el v seen=" "
  cen=$(disk_census "$f")
  while IFS=$'\t' read -r rl how; do
    [ -n "$rl" ] || continue
    el=$(printf '%s\n' "$cen" | awk -F'\t' -v s="$rl" '$2 >= s { print $2; exit }')
    v=$(printf '%s\n'  "$cen" | awk -F'\t' -v s="$rl" '$2 >= s { print $1; exit }')
    [ -n "$el" ] || { el="none"; v="NO-EMIT-SITE"; }
    case "$seen" in *" $el "*) continue ;; esac
    seen="$seen$el "
    printf '%s\t%s\t%s\t%s\n' "$el" "$v" "$rl" "$how"
  done < <(awk -f "$DISK_TABLE_AWK" "$f" | grep -v '^RENDERERS')
}

tr_n=$(table_renderers | cut -f1)
tr_names=$(table_renderers | cut -f2)
if [ "${tr_n:-0}" -ge 2 ]; then
  ok "14-renderers: derived $tr_n component-row RENDERER(s) from the row FORMAT, not from a name ($tr_names)"
else
  bad "14-renderers: derived only ${tr_n:-0} component-row renderer(s) ($tr_names) -- the row-format derivation no longer matches the script, so the table census would be blind again"
fi
tv_out=$(table_verdicts)
tbl_n=$(printf '%s\n' "$tv_out" | grep -c '	')
tbl_marked=$(printf '%s\n' "$tv_out" | grep -c '	MARKED')
tbl_bad=$(printf '%s\n' "$tv_out" | awk -F'\t' '$2 !~ /^MARKED/ { printf "%s(%s via %s),", $1, $2, $4 }')
if [ "$tbl_n" -ge 7 ]; then
  ok "14-tables: derived $tbl_n DISTINCT component-table emit sites from the shipped source, across $tr_n renderer(s) (not a hard-coded count, not one renderer's name)"
else
  bad "14-tables: derived only $tbl_n component-table emit sites -- the derivation no longer matches the script's shape"
  printf '%s\n' "$tv_out"
fi
if [ "$tbl_n" -gt 0 ] && [ "$tbl_marked" -eq "$tbl_n" ]; then
  ok "14-tables: all $tbl_n component-table emit sites are MARKED (a table-bearing block may never be exempted away)"
else
  bad "14-tables: $tbl_marked of $tbl_n component-table sites are MARKED; not-marked: ${tbl_bad:-<none>}"
fi
# Both renderers must actually be REPRESENTED in the derived set. Counting 7 sites proves
# nothing if all 7 came through one renderer -- that is the blind spot restated as a number.
tv_vias=$(printf '%s\n' "$tv_out" | cut -f4 | sort -u | grep -c .)
if [ "$tv_vias" -ge 2 ]; then
  ok "14-tables: the derived sites reach the census through $tv_vias DISTINCT renderers -- the second renderer is represented, not merely counted"
else
  bad "14-tables: every derived table site came through ONE renderer ($(printf '%s\n' "$tv_out" | cut -f4 | sort -u | tr '\n' ' ')) -- the derivation is single-renderer again"
fi

# POSITIVE CONTROLS, both directions. A guard that has not been shown to fail on a planted
# break is not evidence -- and each direction needs its OWN plant, because they fail through
# different arms of the census.
# (a) DELETE ONE EXEMPTION => that site must become a GAP.
ctl_a="$tmp/disk-census-control-a.sh"
awk 'BEGIN { done = 0 }
     { if (!done && $0 ~ /disk-exhaustion-exempt:/) { done = 1; next }
       print }' "$GATE" >"$ctl_a"
if ! cmp -s "$GATE" "$ctl_a"; then
  a_gaps=$(disk_census "$ctl_a" | grep -c '^GAP	')
  if [ "$a_gaps" -ge 1 ]; then
    ok "14-control-a: deleting ONE 'disk-exhaustion-exempt:' comment makes the census report a GAP ($a_gaps) -- the exemption arm is live, not inert"
  else
    bad "14-control-a: a gate with an exemption REMOVED still censused clean -- the exemption arm is inert"
  fi
else
  bad "14-control-a: could not build the control (no exemption comment matched) -- the census cannot be shown to discriminate"
fi
# (d) DOWNGRADE one real exemption reason to an UNSUBSTITUTED PLACEHOLDER => it must stop
#     counting as an exemption. A refusal that has never been SEEN to fire is not evidence
#     (and the placeholder literally occurs in this contract's own doctrine comment, so the
#     refusal is load-bearing, not decorative).
ctl_d="$tmp/disk-census-control-d.sh"
awk 'BEGIN { done = 0 }
     { if (!done && $0 ~ /disk-exhaustion-exempt:[ \t]*[^ \t]/) {
         done = 1
         sub(/disk-exhaustion-exempt:.*$/, "disk-exhaustion-exempt: <reason>")
       }
       print }' "$GATE" >"$ctl_d"
if ! cmp -s "$GATE" "$ctl_d"; then
  d_gaps=$(disk_census "$ctl_d" | grep -c '^GAP	')
  if [ "$d_gaps" -ge 1 ]; then
    ok "14-control-d: an exemption reason downgraded to the '<reason>' PLACEHOLDER stops exempting ($d_gaps GAP) -- the placeholder refusal is live, not inert"
  else
    bad "14-control-d: a placeholder '<reason>' still exempted its site -- the refusal is inert, so this contract's own doctrine comment could exempt a future site beneath it"
  fi
else
  bad "14-control-d: could not build the control (no exemption reason matched) -- the placeholder refusal cannot be shown to discriminate"
fi
# (b) REMOVE THE _disk_exhaustion_line CALL from one component-table site => that site must
#     stop being MARKED, which the census reports as a GAP (it carries no exemption).
ctl_b="$tmp/disk-census-control-b.sh"
awk 'BEGIN { done = 0 }
     { if (!done && $0 ~ /META\+=\("\$\(_disk_exhaustion_line/) { done = 1; next }
       print }' "$GATE" >"$ctl_b"
if ! cmp -s "$GATE" "$ctl_b"; then
  b_gaps=$(disk_census "$ctl_b" | grep -c '^GAP	')
  if [ "$b_gaps" -ge 1 ]; then
    ok "14-control-b: removing the attribution call from ONE component-table site makes the census report a GAP ($b_gaps) -- the marking arm is live, not inert"
  else
    bad "14-control-b: a gate with one attribution call REMOVED still censused clean -- the marking arm is inert"
  fi
else
  bad "14-control-b: could not build the control (no marked component-table site matched) -- the census cannot be shown to discriminate"
fi
# (c) PERMANENT CONTROL FOR THE SECOND RENDERER (#3800, final round). Controls (a) and (b)
#     both plant into the `_fm_summary_line` family; neither can fail if the table-site
#     derivation goes blind to `_tree_boundary_meta_lines` again -- which is exactly what
#     happened, and is why that site shipped exempt for a round. Plant: delete the bare
#     `_disk_exhaustion_line` call from the boundary renderer's body (the only call of that
#     shape -- every other call site is an array append). The census must report a GAP AND
#     the table-site case must stop being all-MARKED. A refactor that reintroduces the blind
#     spot reds this case instead of greening the suite.
ctl_c="$tmp/disk-census-control-c.sh"
ctl_c_hits=$(grep -c '^  _disk_exhaustion_line ' "$GATE" | tr -d ' ')
awk 'BEGIN { done = 0 }
     { if (!done && $0 ~ /^  _disk_exhaustion_line /) { done = 1; next }
       print }' "$GATE" >"$ctl_c"
if [ "$ctl_c_hits" = 1 ] && ! cmp -s "$GATE" "$ctl_c"; then
  c_gaps=$(disk_census "$ctl_c" | grep -c '^GAP	')
  c_tv=$(table_verdicts "$ctl_c")
  c_n=$(printf '%s\n' "$c_tv" | grep -c '	')
  c_marked=$(printf '%s\n' "$c_tv" | grep -c '	MARKED')
  if [ "$c_gaps" -ge 1 ]; then
    ok "14-control-c: removing the attribution from the NON-_fm_summary_line renderer makes the census report a GAP ($c_gaps)"
  else
    bad "14-control-c: a gate with the boundary renderer's attribution REMOVED still censused clean"
  fi
  if [ "$c_n" -ge 7 ] && [ "$c_marked" -lt "$c_n" ]; then
    ok "14-control-c: and the table-site case FAILS on it -- $c_marked of $c_n sites MARKED, so the second renderer's site is genuinely in the subject set"
  else
    bad "14-control-c: the table-site derivation still reported $c_marked/$c_n MARKED with the boundary attribution removed -- it is BLIND to the second renderer again"
    printf '%s\n' "$c_tv"
  fi
else
  bad "14-control-c: could not build the control (expected exactly one bare '_disk_exhaustion_line' call, found ${ctl_c_hits:-0}) -- the second-renderer arm cannot be shown to discriminate"
fi
# Every exemption must carry a reason that DISTINGUISHES its site: 19 copies of one generic
# sentence is not 19 reasons. Measured as: the reason texts are distinct from one another.
ex_reasons=$(grep -o 'disk-exhaustion-exempt:.*' "$GATE" | sed 's/^disk-exhaustion-exempt:[[:space:]]*//')
ex_total=$(printf '%s\n' "$ex_reasons" | grep -c .)
ex_uniq=$(printf '%s\n' "$ex_reasons" | grep . | sort -u | wc -l | tr -d ' ')
if [ "$ex_total" -gt 0 ] && [ "$ex_total" -eq "$ex_uniq" ]; then
  ok "14-reasons: all $ex_total exemption reasons are DISTINCT -- a reason that does not distinguish its site is not a reason"
else
  bad "14-reasons: $ex_total exemption reasons collapse to $ex_uniq distinct texts -- a duplicated generic reason explains nothing"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (15) STRUCTURAL: the line is an ATTRIBUTION, never a verdict. No extracted function may
# assign OVERALL/RESULT, and no emit site may derive a status from the call.
# ─────────────────────────────────────────────────────────────────────────────────
if grep -qE '^[^#]*\b(OVERALL|RESULT)=' "$EX"; then
  bad "15-attribution: an extracted disk-exhaustion function assigns OVERALL/RESULT -- it must never change the verdict"
else
  ok "15-attribution: no extracted function assigns OVERALL or RESULT"
fi
if grep -n '_disk_exhaustion_line' "$GATE" | grep -v '^[0-9]*:#' | grep -qE 'OVERALL|if |&&|\|\|'; then
  bad "15-attribution: a call site of _disk_exhaustion_line feeds a conditional or OVERALL"
else
  ok "15-attribution: every call site is a pure append to a summary meta array"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (16) END TO END through the SHIPPED script's real emit path: the line lands in the block
# and the RESULT is exactly what it would have been.
# ─────────────────────────────────────────────────────────────────────────────────
sf="$tmp/e2e-pass.txt"
AGENT_GATE_SUMMARY_FILE="$sf" bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if grep -q '^disk-exhaustion: 0 RECOGNISED (#3800) -- no non-PASS component to scan (4/4 PASS)' "$sf" \
   && grep -q '^RESULT: PASS$' "$sf"; then
  ok "16-e2e-pass: --emit-summary-selftest carries the line at column zero and still reports RESULT: PASS"
else
  bad "16-e2e-pass: the marker/RESULT pair is wrong in the real emit path; block: $(grep -c . "$sf" 2>/dev/null) lines"
fi
if [ "$(grep -c '^disk-exhaustion:' "$sf")" = 1 ]; then
  ok "16-e2e-pass: exactly ONE disk-exhaustion line per block"
else
  bad "16-e2e-pass: expected exactly one disk-exhaustion line, found $(grep -c '^disk-exhaustion:' "$sf")"
fi
sf="$tmp/e2e-fail.txt"
AGENT_GATE_SUMMARY_FILE="$sf" AGENT_GATE_TEST_LITE_RESULTS="fmt:PASS clippy:FAIL" \
  bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1
if grep -q '^disk-exhaustion: UNMEASURED (#3800)' "$sf" && grep -q '^RESULT: FAIL$' "$sf"; then
  ok "16-e2e-fail: a non-PASS component produces the marker and the FAIL verdict is UNCHANGED by it"
else
  bad "16-e2e-fail: expected a UNMEASURED marker beside RESULT: FAIL in the lite-aggregate selftest"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# CASE FLOOR. A span-replacing edit that silently deletes cases must RED this suite, not
# green it -- `failed: 0` over a shrunken subject set is the vacuous pass these suites are
# for (#3544's own lesson, one directory over). Raise it deliberately when adding cases.
# ─────────────────────────────────────────────────────────────────────────────────
# 30 at introduction; +3 (roborev job 299 finding 2: the RECOGNISED wording split into
# host-attribution, evidence-not-proof, the negative on the retired 'NOT a defect in the diff'
# claim, and the retained ATTRIBUTION clause -- 4 cases replacing 1); +5 (job 299 finding 1: the
# circular 2-case 14-emit-sites replaced by a 7-case census over ALL emit sites -- accountability,
# the table-bearing subset, two positive controls and the distinct-reasons assert); +4 (#3800
# final round: the table-site derivation went from ONE renderer's name to the row FORMAT, adding
# 14-renderers, the two-distinct-renderers assert and control (c)'s two cases -- the permanent
# pin against the blind spot that let the tree-integrity boundary site ship exempt); +1 (control
# (d): the EXEMPT arm now refuses an unsubstituted `<...>` placeholder reason, as
# scripts/flow/claim.sh:795 already does for --reason, and a refusal nobody has seen fire is not
# evidence -- this contract's own doctrine comment contains that placeholder).
CASE_FLOOR=43
printf '\n%s\n' "----------------------------------------"
if [ $((PASS + FAIL)) -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case-floor: %d cases ran but this suite declares a floor of %d -- cases were REMOVED or are dying silently.\n' \
    "$((PASS + FAIL))" "$CASE_FLOOR"
  FAIL=$((FAIL + 1))
fi
printf 'passed: %d  failed: %d  (floor %d)\n' "$PASS" "$FAIL" "$CASE_FLOOR"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
