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
if case "$out" in *"ENVIRONMENTAL"*"NOT a defect in the diff"*) true ;; *) false ;; esac; then
  ok "1-no-space: the line states the FAIL is ENVIRONMENTAL and not a defect in the diff"
else
  bad "1-no-space: the RECOGNISED line does not carry the environmental-attribution remedy text; got: $out"
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
# (14) STRUCTURAL: the emit-site set is DERIVED FROM SOURCE, never hard-coded. Every place
# the shipped script appends a per-component verdict row to a summary meta array is a
# terminal emit site, and each must also append the attribution line.
# ─────────────────────────────────────────────────────────────────────────────────
sites=$(grep -n 'META+=("\$(_fm_summary_line\|^ *meta+=("\$(_fm_summary_line' "$GATE" | cut -d: -f1)
site_n=0; wired=0; unwired=""
for ln in $sites; do
  site_n=$((site_n + 1))
  if sed -n "${ln},$((ln + 15))p" "$GATE" | grep -q '_disk_exhaustion_line'; then
    wired=$((wired + 1))
  else
    unwired="${unwired:+$unwired,}$ln"
  fi
done
if [ "$site_n" -ge 5 ]; then
  ok "14-emit-sites: derived $site_n component-table emit sites from the shipped source (not a hard-coded count)"
else
  bad "14-emit-sites: derived only $site_n emit sites -- the derivation no longer matches the script's shape and this case would be vacuous"
fi
if [ "$site_n" -gt 0 ] && [ "$wired" -eq "$site_n" ]; then
  ok "14-emit-sites: all $site_n emit sites append the disk-exhaustion attribution line"
else
  bad "14-emit-sites: $wired of $site_n emit sites append the line; unwired at line(s): ${unwired:-<none>}"
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
CASE_FLOOR=30
printf '\n%s\n' "----------------------------------------"
if [ $((PASS + FAIL)) -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case-floor: %d cases ran but this suite declares a floor of %d -- cases were REMOVED or are dying silently.\n' \
    "$((PASS + FAIL))" "$CASE_FLOOR"
  FAIL=$((FAIL + 1))
fi
printf 'passed: %d  failed: %d  (floor %d)\n' "$PASS" "$FAIL" "$CASE_FLOOR"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
