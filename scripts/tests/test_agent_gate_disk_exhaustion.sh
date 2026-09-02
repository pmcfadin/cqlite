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
#
# #3800 (roborev job 301) added the SECOND KIND OF SUBJECT -- the gate's own in-memory
# capture-failure text -- so the extracted set now also carries `DISK_MEM_SUBJECTS`, the
# recorder, the one shared signature loop, and the REAL `_tree_identity` plus its helpers.
# The last group is what lets the /dev/full case below drive a GENUINE ENOSPC through the
# shipped capture rather than assert that a line exists.
{
  # awk uses ERE, where `\(` is not a portable literal paren -- bracket expressions are.
  extract_region '^DISK_EXHAUSTION_SIGNATURES=[(]$' '^[)]$'
  # A one-line array declaration has no region to extract; take the SHIPPED line itself so
  # this suite still measures the shipped source and never a copy typed here.
  grep -m1 '^DISK_MEM_SUBJECTS=()$' "$GATE"
  # #3800 (roborev job 304): the THIRD kind of subject -- a `.result` verdict the gate could
  # not READ -- plus the ONE reader every `.result` site now routes through, and
  # `record_result` itself so the in-memory arm of the fix is measured as shipped.
  grep -m1 '^DISK_UNREAD_VERDICTS=()$' "$GATE"
  grep -m1 '^DISK_RECORDED_PAIRS=()$' "$GATE"
  grep -m1 '^DISK_PREFLIGHT_META=()$' "$GATE"
  # `record_result` needs its OWN anchor: its shipped header line carries a trailing
  # `# <name> <status> <seconds>` comment, so the loop's `[{]$` anchor would extract NOTHING
  # and the fail-closed check below would fire. Anchored without the end-of-line assertion.
  extract_region '^record_result[(][)] [{]' '^[}]$'
  # #3800 (roborev job 316): the seconds GRAMMAR and the AGGREGATION wrapper. The shipped
  # `aggregate_lite_components` case 20c drives is extracted SEPARATELY, into $EX_AGG -- see the
  # reason at its extraction below.
  for fn in _disk_safe _disk_abbrev _disk_df_probe _disk_gib _disk_free_leg \
            _disk_free_field _disk_scan_field _disk_note_capture_failure \
            _disk_note_unread_verdict _disk_secs_is_int _disk_verdict_read \
            _disk_verdict_read_aggregate _disk_recorded_pairs _disk_preflight_meta \
            _disk_scan_subject _disk_exhaustion_line \
            _tree_excluded _tree_probe_tools _tree_sort0 _tree_digest_file _tree_hex_id_ok \
            _tree_digest_ok _tree_manifest_ok _tree_mtime _tree_identity \
            _tree_emit_capture_diag _tree_note_capture_failure \
            _tree_boundary_fail; do
    extract_region "^${fn}[(][)] [{]\$" '^[}]$'
  done
} >> "$EX"

for want in DISK_EXHAUSTION_SIGNATURES DISK_MEM_SUBJECTS DISK_UNREAD_VERDICTS _disk_safe _disk_abbrev \
            _disk_df_probe _disk_gib _disk_free_leg _disk_free_field _disk_scan_field \
            _disk_note_capture_failure _disk_note_unread_verdict _disk_secs_is_int \
            _disk_verdict_read _disk_verdict_read_aggregate _disk_recorded_pairs \
            _disk_preflight_meta \
            record_result _disk_scan_subject _disk_exhaustion_line \
            _tree_excluded _tree_probe_tools _tree_sort0 _tree_digest_file _tree_hex_id_ok \
            _tree_digest_ok _tree_manifest_ok _tree_mtime _tree_identity \
            _tree_emit_capture_diag _tree_note_capture_failure _tree_boundary_fail; do
  if ! grep -q "^${want}" "$EX"; then
    bad "extract: '$want' was NOT extracted from the shipped agent-gate.sh -- every case below would be vacuous"
    EXTRACT_OK=0
  fi
done
if [ "$EXTRACT_OK" -eq 1 ]; then
  ok "extract: the shipped signature set, BOTH gate-internal subject channels and 27 helpers (incl. the REAL _tree_identity, the shipped record_result and the shipped _tree_boundary_fail) were extracted from scripts/agent-gate.sh"
fi

# #3800 (roborev job 316, rationale corrected in round 3): `aggregate_lite_components` is
# extracted into its OWN file. The original reason was case 15's subject set -- it scanned the
# WHOLE of `$EX` for an `OVERALL=` assignment, and the aggregator's entire job is to make one.
# That reason is now obsolete: case 15 derives its population from the SHIPPED source instead
# (every `_disk_*` function), which is stronger and cannot be eroded by how this file partitions
# its extractions. The separation is KEPT because it states a real distinction: the aggregator is
# a CONSUMER of the attribution channels, not a member of the attribution family.
#
# And the distinction is what keeps "an attribution, never a verdict" true while the aggregation
# fails closed. What job 316 wired to the verdict is the AGGREGATION's disposition of a `.result`
# file it could not READ -- correct even if the `disk-exhaustion:` line did not exist. The line
# itself still changes nothing, which is why case 15 must keep failing on any ATTRIBUTION
# function that touches OVERALL.
EX_AGG="$tmp/extracted-aggregator.sh"
extract_region '^aggregate_lite_components[(][)] [{]$' '^[}]$' > "$EX_AGG"
if grep -q '^aggregate_lite_components' "$EX_AGG" && grep -q '_disk_verdict_read_aggregate' "$EX_AGG"; then
  ok "extract-agg: the SHIPPED aggregate_lite_components was extracted separately (and does route through the aggregation wrapper), so case 20c drives the real aggregator rather than a model of it"
else
  bad "extract-agg: aggregate_lite_components was NOT extracted from the shipped gate -- case 20c would be vacuous"
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
    # #3800: the in-memory subject channel and the START-capture cross-check are cleared,
    # so the component-log cases below measure the component-log arm alone. Case 17 seeds
    # them deliberately.
    DISK_MEM_SUBJECTS=(); DISK_UNREAD_VERDICTS=(); TREE_CAPTURE_FAILED=0
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
if case "$out" in *"scanned 1 non-PASS component log(s) (minimal-build)"*"0 in-memory subject(s)"*"every subject was READ"*) true ;; *) false ;; esac; then
  ok "5-clean: the clean verdict is keyed on the AFFIRMATIVE fact (every subject was READ) and names BOTH kinds of subject"
else
  bad "5-clean: the clean verdict does not state that every subject (both kinds) was read; got: $out"
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
# A subject that is a DIRECTORY. This USED to reach grep (which exits 2, "Is a directory") and was
# how the three-valued-rc arm was exercised; since #3800 (roborev job 343) the regular-file guard
# REFUSES it earlier, because `grep` on a FIFO or a device -- which `-e` and `-r` also admit --
# blocks forever on the path to the terminal emit. The OUTCOME that matters is unchanged and is
# what this case now asserts: UNMEASURED naming the subject, never a clean scan. The rc>=2 arm it
# used to reach is still live and is covered by 8b2, which reaches grep on a REGULAR file.
d="$tmp/c8b"; mkdir -p "$d/write-tests.log"
out=$(run_line "$d" write-tests FAIL)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"write-tests.log(not-a-regular-file)"*) true ;; *) false ;; esac \
   && case "$out" in *"0 RECOGNISED"*) false ;; *) true ;; esac; then
  ok "8b-nonregular: a DIRECTORY subject is refused as non-regular and reported UNMEASURED naming it -- neither scanned (grep would error) nor silently skipped (which would read as clean)"
else
  bad "8b-nonregular: a non-regular subject was not reported as UNMEASURED; got: $out"
fi
# (8b2) THE THREE-VALUED grep rc, on a REGULAR file so the guard above does not pre-empt it.
# `grep`'s rc is three-valued (0 match / 1 no-match / >=2 ERROR) and collapsing >=2 onto "no match"
# is this repo's two-valued-predicate defect: it would report a clean scan over a subject that was
# never read. A real grep error on a readable regular file cannot be induced portably, so the
# failure is INJECTED via a PATH shim -- labelled as such, because an injected condition evidences
# the ARM and not the likelihood of reaching it.
d="$tmp/c8b2"; mkdir -p "$d/bin"
echo 'nothing to see' > "$d/core-tests.log"
printf '#!/bin/sh
exit 2
' > "$d/bin/grep"; chmod +x "$d/bin/grep"
out=$(
  PATH="$d/bin:$PATH" run_line "$d" core-tests FAIL
)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"core-tests.log(unreadable)"*) true ;; *) false ;; esac \
   && case "$out" in *"0 RECOGNISED"*) false ;; *) true ;; esac; then
  ok "8b2-grep-rc2: an ERRORING grep (rc 2, injected) over a REGULAR readable subject yields UNMEASURED, never 'no signature' -- the three-valued rc is not collapsed onto no-match"
else
  bad "8b2-grep-rc2: a grep ERROR was collapsed onto 'no match'; got: $out"
fi
# CONTROL: the same shim exiting 1 (a genuine no-match) must read CLEAN, or 8b2 would pass for any
# shim at all and would not be distinguishing rc 1 from rc 2.
printf '#!/bin/sh\nexit 1\n' > "$d/bin/grep"
out=$(PATH="$d/bin:$PATH" run_line "$d" core-tests FAIL)
if case "$out" in *"0 RECOGNISED"*) true ;; *) false ;; esac; then
  ok "8b2-control: the same shim returning rc 1 (a real no-match) reads as the affirmative clean scan, so 8b2 distinguishes ERROR from no-match rather than reacting to any non-zero rc"
else
  bad "8b2-control: a no-match grep did not produce the clean reading, so 8b2's rc-2 result is not attributable to the ERROR arm; got: $out"
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

# (13b) THE SUBJECT SET IS DECLARED TOO (#3800, roborev job 304). Declaring the SIGNATURE set
# as closed while leaving the SUBJECT set implicit is what let three consecutive rounds each
# find a different unwatched gate-internal writer. Every rendering must name the three kinds
# of subject AND name the known writers that sit outside them -- a reader of a pasted block
# must be able to check the boundary, not infer it.
subj_dec=0; subj_named=0; total_rend=0
for probe_out in \
  "$(run_line "$tmp/c1" minimal-build FAIL)" \
  "$(run_line "$tmp/c5" minimal-build FAIL)" \
  "$(run_line "$tmp/c6" fmt PASS)" \
  "$(run_line "$tmp/c8" minimal-build FAIL)" ; do
  total_rend=$((total_rend + 1))
  case "$probe_out" in
    *"DECLARED SUBJECT SET"*"(a) logs of non-PASS components"*"(b) the gate OWN in-memory capture-failure subjects"*"(c) components whose .result verdict could NOT be read"*)
      subj_dec=$((subj_dec + 1)) ;;
  esac
  case "$probe_out" in
    *"gate-internal writers OUTSIDE the subject set"*"_fm_* sidecars"*"node-bindings.leak-lane"*"summary-integrity.fail"*"heartbeat file"*)
      subj_named=$((subj_named + 1)) ;;
  esac
done
if [ "$subj_dec" -eq "$total_rend" ]; then
  ok "13b-subject-set: all $total_rend renderings declare the SUBJECT set and name its three kinds (logs / in-memory captures / unread verdicts)"
else
  bad "13b-subject-set: only $subj_dec of $total_rend renderings declare the subject set -- the scan states which SIGNATURES it can miss but not which WRITERS it cannot see"
fi
if [ "$subj_named" -eq "$total_rend" ]; then
  ok "13b-subject-set-writers: all $total_rend renderings NAME the known gate-internal writers outside the subject set, so the boundary is checkable rather than a hedge"
else
  bad "13b-subject-set-writers: only $subj_named of $total_rend renderings name the un-covered writers"
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
    else {
      # EVERY array named in the args is examined, not just the first one, and BOTH spellings are
      # recognised. Two defects were here:
      #   * the derivation took the FIRST `${NAME[@]}` in the args and walked upward for THAT
      #     name only. A site that expands several arrays -- e.g. `${TREE_META_LINES[@]}` before
      #     the one carrying the attribution -- censused as a GAP while appending the line.
      #   * it matched only the plain `${arr[@]}`, while agent-gate.sh MANDATES the
      #     `"${arr[@]+"${arr[@]}"}"` idiom wherever an array may be empty (a plain `${arr[@]}` of
      #     an empty array is "unbound" under `set -u` on bash 3.2).
      # The name runs from `${` to the `[`, which is identical for both spellings.
      rest = args
      while (match(rest, /\$\{[A-Za-z_]+\[@\]/)) {
        nm = substr(rest, RSTART + 2, RLENGTH - 5)
        rest = substr(rest, RSTART + RLENGTH)
        for (k = i; k > 0; k--) {
          if (line[k] ~ ("(declare -a |local -a )?" nm "=\\(")) break
          if (k < i && line[k] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{|^\}/) break
          if (line[k] !~ /^[ \t]*#/ && line[k] ~ ("" nm "\\+=\\(\"\\$\\(_disk_exhaustion_line")) { verdict = "MARKED-VIA-" nm; break }
          if (match(line[k], /< <\(([A-Za-z_][A-Za-z0-9_]*)\)/)) {
            fn = substr(line[k], RSTART + 4, RLENGTH - 5)
            if (_marking_fn(fn)) { verdict = "MARKED-VIA-RENDERER-" fn; break }
          }
          # A BARE CALL to a function that appends the line. Three pre-flight sites share one
          # helper (`_disk_preflight_meta`) rather than copying the decision, so the append lives
          # in a FUNCTION and the upward walk for `NAME+=(...)` cannot see it -- it censused as a
          # GAP while attributing correctly. This is the same allowance `< <(fn)` already gets,
          # for the same reason: what matters is that SOME reachable code appends the line to the
          # array this site expands, not the syntax by which it does.
          if (line[k] !~ /^[ \t]*#/ && match(line[k], /^[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*$/)) {
            fn = line[k]; gsub(/^[ \t]+|[ \t]+$/, "", fn)
            if (_marking_fn(fn)) { verdict = "MARKED-VIA-HELPER-" fn; break }
          }
        }
        if (verdict != "GAP") break
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
# #3800 (roborev job 319 round 3): THE SUBJECT SET IS DERIVED FROM THE SHIPPED SOURCE, not from
# `$EX`. Scanning the whole extracted file conflated two different populations: the ATTRIBUTION
# family, which must never touch the verdict, and the CONSUMERS this suite also extracts so its
# cases can drive real code (`record_result`, `aggregate_lite_components`, `_tree_boundary_fail`),
# which legitimately DO own verdicts -- so the assert red on correct input twice, once when the
# aggregator arrived and once when `record_result` gained its fail-closed `OVERALL=FAIL`. Fixing
# it by EXCLUDING those names would have been the "exclude whatever fails" shape; deriving the
# population from the source is strictly STRONGER instead, because it covers every `_disk_*`
# function in the shipped gate including any this suite never extracted.
_att_offenders=$(awk '
  /^_disk_[A-Za-z0-9_]*\(\) \{/ { inb=1; fn=$1; next }
  inb && /^\}$/ { inb=0; next }
  inb && /^[^#]*(OVERALL|RESULT)=/ { print fn ": " $0 }
' "$GATE")
_att_n=$(awk '/^_disk_[A-Za-z0-9_]*\(\) \{/ { n++ } END { print n+0 }' "$GATE")
if [ "$_att_n" -ge 8 ] && [ -z "$_att_offenders" ]; then
  ok "15-attribution: none of the $_att_n _disk_* functions in the SHIPPED gate assigns OVERALL or RESULT -- the line is an attribution, never a verdict (population derived from source, so a helper this suite never extracts is covered too)"
else
  bad "15-attribution: population=$_att_n (expected >= 8; a collapsed population would green this vacuously) offenders:
$_att_offenders"
fi
# POSITIVE CONTROL: the scan must FAIL on a planted assignment, or its green means only that the
# awk found nothing -- the same vacuity this suite pins everywhere else.
_att_ctl=$(mktemp "$tmp/att-ctl.XXXXXX")
awk '
  { print }
  /^_disk_exhaustion_line\(\) \{/ && !done { print "  OVERALL=FAIL"; done=1 }
' "$GATE" > "$_att_ctl"
_att_ctl_off=$(awk '
  /^_disk_[A-Za-z0-9_]*\(\) \{/ { inb=1; fn=$1; next }
  inb && /^\}$/ { inb=0; next }
  inb && /^[^#]*(OVERALL|RESULT)=/ { print fn }
' "$_att_ctl")
if [ -n "$_att_ctl_off" ]; then
  ok "15-attribution-control: an OVERALL assignment planted inside _disk_exhaustion_line IS detected ($_att_ctl_off), so the clean scan above is a measurement and not an empty match"
else
  bad "15-attribution-control: the planted assignment was NOT detected -- the attribution scan is blind and its green says nothing"
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
# (17) THE IN-MEMORY SUBJECT CHANNEL (#3800, roborev job 301) -- HOST-INDEPENDENT.
#
# THE DEFECT THIS PINS. The attribution was added to the tree-integrity BOUNDARY block
# because a `tree-integrity: FAIL` is reachable from ENOSPC. But the scan's subject set was
# NON-PASS COMPONENT LOGS, and on that path _tree_identity fails independently of any
# component, its write-error text reaches NO component log, and the components are still
# PASS -- so the block would have emitted an affirmative `0 RECOGNISED` on exactly the path
# the line was added for. Adding a marker to a block does not make that block's CAUSE
# observable to the marker.
#
# Every case here therefore runs with EVERY COMPONENT PASSING: under the old subject set the
# scan had nothing to look at, so a RECOGNISED here can only have come from the new channel.
# Case (18) drives the same channel from a REAL ENOSPC; this one pins it on every host,
# /dev/full or not.
# ─────────────────────────────────────────────────────────────────────────────────
run_mem() {  # run_mem <mode> <text> [<component> <status> ...]
  local mode="$1" text="$2"; shift 2
  (
    . "$EX"
    LOG_DIR="$tmp/c17-empty"
    DISK_TARGET_PATH=""; DISK_LOGS_PATH=""
    DISK_FREE_START_TARGET=""; DISK_FREE_START_LOGS=""
    DISK_MEM_SUBJECTS=(); DISK_UNREAD_VERDICTS=(); TREE_CAPTURE_FAILED=0
    case "$mode" in
      record)     _disk_note_capture_failure 'tree-identity manifest write (start capture)' "$text" ;;
      empty)      _disk_note_capture_failure 'tree-identity manifest write (start capture)' "" ;;
      unrecorded) TREE_CAPTURE_FAILED=1 ;;
    esac
    _disk_exhaustion_line "$@"
  )
}
mkdir -p "$tmp/c17-empty"

# (17a) the shipped rc-2 diagnostic wording, recorded through the shipped recorder.
out=$(run_mem record 'bash: printf: write error: No space left on device' fmt PASS clippy PASS)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"'no-space-left-on-device'"*"IN-MEMORY subject 'tree-identity manifest write (start capture)'"*) true ;; *) false ;; esac; then
  ok "17a-mem-recognised: a tree-capture failure text is RECOGNISED and named as an IN-MEMORY subject, with every component PASS (the component-log subject set is EMPTY here -- the exact blind spot)"
else
  bad "17a-mem-recognised: the in-memory capture text produced no RECOGNISED verdict; got: $out"
fi
# It must name the subject in OUR vocabulary and must NOT invent a `<log>:<line>` it has none of.
if case "$out" in *".log:"*) false ;; *) true ;; esac \
   && case "$out" in *"the gate's OWN capture, which reaches NO component log"*) true ;; *) false ;; esac; then
  ok "17a-mem-vocabulary: the in-memory rendering carries no fabricated '<log>:<line>' and says the subject reaches no component log"
else
  bad "17a-mem-vocabulary: the in-memory rendering borrowed the component-log shape; got: $out"
fi

# (17b) NEGATIVE CONTROL -- the arm must DISCRIMINATE, not merely fire. A capture failure
# whose text carries NO signature is a clean reading, and it must SAY the subject was read.
out=$(run_mem record 'fatal: unable to read tree (deadbeef)' fmt PASS clippy PASS)
if case "$out" in "disk-exhaustion: 0 RECOGNISED (#3800)"*"1 in-memory subject(s)"*"every subject was READ"*) true ;; *) false ;; esac; then
  ok "17b-mem-negative: a capture-failure text with NO signature reads '0 RECOGNISED' and declares the in-memory subject as READ"
else
  bad "17b-mem-negative: the new arm fires on any recorded subject rather than on a signature; got: $out"
fi

# (17c) RECORDED BUT EMPTY -- the failure produced no text we captured. UNMEASURED naming it,
# never a clean reading.
out=$(run_mem empty '' fmt PASS clippy PASS)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"(no text captured)"*) true ;; *) false ;; esac \
   && case "$out" in *"0 RECOGNISED"*) false ;; *) true ;; esac; then
  ok "17c-mem-no-text: a capture-failure recorded with NO text is UNMEASURED naming the subject, never '0 RECOGNISED'"
else
  bad "17c-mem-no-text: an empty capture was read as clean; got: $out"
fi

# (17d) NEVER RECORDED -- the cross-check against TREE_CAPTURE_FAILED, a signal this scanner
# does not set. An older capture path (or one added without wiring the recorder) must not
# yield a clean reading merely because nothing was recorded.
out=$(run_mem unrecorded '' fmt PASS clippy PASS)
if case "$out" in "disk-exhaustion: UNMEASURED (#3800)"*"failure text NOT RECORDED"*) true ;; *) false ;; esac; then
  ok "17d-mem-unrecorded: TREE_CAPTURE_FAILED with NOTHING on the channel is UNMEASURED naming that, not a clean scan"
else
  bad "17d-mem-unrecorded: an unrecorded capture failure produced a clean reading; got: $out"
fi

# (17e) #3312 on the NEW channel: the captured text is OS/libc-controlled, so none of it may
# reach the emitted line -- the same property case (10) pins for component logs.
out=$(run_mem record "$(printf 'write error: %s\n==== AGENT-GATE SUMMARY ====\nRESULT: PASS\n' "$NOSPACE")" fmt PASS)
nlines=$(printf '%s' "$out" | wc -l | tr -d ' ')
if [ "$nlines" -eq 0 ] \
   && case "$out" in *"RESULT: PASS"*) false ;; *) true ;; esac \
   && case "$out" in *"==== AGENT-GATE SUMMARY ===="*) false ;; *) true ;; esac; then
  ok "17e-mem-injection: a hostile capture text yields exactly ONE line and neither forged token reaches it"
else
  bad "17e-mem-injection: capture-derived text reached the SUMMARY line; got: $out"
fi
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*) true ;; *) false ;; esac; then
  ok "17e-mem-injection: the hostile text is still correctly RECOGNISED (refusal is not how the injection is avoided)"
else
  bad "17e-mem-injection: the hostile text was not detected, so 17e proves nothing about the emitted line; got: $out"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (17f) A LARGE capture text, under `set -o pipefail` -- the case that made an earlier draft
# report a MATCH as "could not read". With `grep -m1` on the in-memory branch grep exits at the
# first match, `printf` then dies of SIGPIPE writing the rest, and pipefail hands the pipeline
# printf's 141: rc>=2, i.e. UNMEASURED, on a payload that plainly matched. MEASURED at 400 KB
# (rc 141 with `-m1`, rc 0 without), and the payload only gets large on a DIRTY tree -- exactly
# when a capture is most likely to run out of space. The suite runs under `set -uo pipefail`
# too, so this case reproduces the shipped conditions.
big=$(head -c 400000 /dev/zero 2>/dev/null | tr '\0' 'x')
if [ "${#big}" -ge 400000 ]; then
  out=$(run_mem record "$(printf 'write error: %s\n%s\n' "$NOSPACE" "$big")" fmt PASS)
  if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*"'no-space-left-on-device'"*) true ;; *) false ;; esac; then
    ok "17f-mem-large: a 400 KB capture text whose FIRST line matches is RECOGNISED -- the in-memory branch does not turn a match into UNMEASURED via pipefail+SIGPIPE"
  else
    bad "17f-mem-large: a large matching capture text did not read RECOGNISED (pipefail is handing the pipeline printf's SIGPIPE status); got: ${out:0:200}"
  fi
else
  bad "17f-mem-large: could not build the 400 KB payload (${#big} bytes) -- the case would prove nothing"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (18) A REAL ENOSPC, THROUGH THE SHIPPED CAPTURE. "The line exists" is not a test: the
# question is whether a GENUINE out-of-space condition produces text this scan can see.
#
# /dev/full returns a real ENOSPC with the platform's own strerror, needs no root and
# mutates nothing. The manifest paths are SYMLINKS to it, so `> "$out"` and `3> "$out.report"`
# both hit the device and the shipped `_tree_identity` fails on its OWN validation.
#
# /dev/full is LINUX-ONLY and macOS is a first-class gate host, so the case is guarded and
# the skip is DECLARED in the output -- a silent skip is the vacuous pass this suite exists
# to prevent. Case (17) covers the same channel on every host.
# ─────────────────────────────────────────────────────────────────────────────────
df_usable=0
if [ -c /dev/full ] && [ -w /dev/full ] && command -v git >/dev/null 2>&1; then
  ( : > /dev/full ) 2>/dev/null && df_usable=1
fi
if [ "$df_usable" -ne 1 ]; then
  ok "18-enospc: DECLARED SKIP -- this host has no writable /dev/full (or no git), so a REAL ENOSPC cannot be induced hermetically; case 17 pins the same channel host-independently"
  ok "18-enospc: DECLARED SKIP (second half) -- the mutation control that removes the in-memory subject from the scan is skipped with it"
else
  r="$tmp/enospc-repo"; mkdir -p "$r"
  ( cd "$r" && git init -q . && printf 'hello\n' > README.md && git add -A \
      && git -c user.name=t -c user.email=t@e commit -qm init ) >/dev/null 2>&1
  printf 'an untracked file, so the capture has a body record to write\n' > "$r/untracked.txt"
  ln -s /dev/full "$r/manifest"
  ln -s /dev/full "$r/manifest.report"
  # The shipped sequence, verbatim: capture -> record the rc-2 text -> render the line.
  enospc_out=$(
    cd "$r" || exit 1
    . "$EX"
    _tree_probe_tools
    TREE_EXCLUDE_REL=""; TREE_STDOUT_REL=""; TREE_STDERR_REL=""
    TREE_HASH_CAP_BYTES=5242880
    LOG_DIR="$tmp/c17-empty"
    DISK_TARGET_PATH=""; DISK_LOGS_PATH=""
    DISK_FREE_START_TARGET=""; DISK_FREE_START_LOGS=""
    DISK_MEM_SUBJECTS=(); DISK_UNREAD_VERDICTS=(); TREE_CAPTURE_FAILED=0
    id=$(_tree_identity "$r/manifest"); rc=$?
    printf 'RC %s\n' "$rc"
    printf 'DIAGSIG %s\n' "$(printf '%s' "$id" | grep -c 'No space left on device')"
    _tree_note_capture_failure "tree-identity manifest write (start capture)" "$rc" "$id"
    printf 'LINE %s\n' "$(_disk_exhaustion_line fmt PASS clippy PASS smoke PASS)"
    # MUTATION CONTROL, in the same shell and on the same captured text: drop the in-memory
    # subject from the scan and the verdict must collapse. Without this, 18b could pass for
    # any reason at all.
    DISK_MEM_SUBJECTS=()
    printf 'MUTANT %s\n' "$(_disk_exhaustion_line fmt PASS clippy PASS smoke PASS)"
  )
  e_rc=$(printf '%s\n' "$enospc_out" | sed -n 's/^RC //p')
  e_sig=$(printf '%s\n' "$enospc_out" | sed -n 's/^DIAGSIG //p')
  e_line=$(printf '%s\n' "$enospc_out" | sed -n 's/^LINE //p')
  e_mut=$(printf '%s\n' "$enospc_out" | sed -n 's/^MUTANT //p')
  # FIXTURE PROVENANCE FIRST: without a real rc 2 carrying real strerror text, everything
  # below would be measuring nothing.
  if [ "${e_rc:-}" = 2 ] && [ "${e_sig:-0}" -ge 1 ]; then
    ok "18a-enospc-real: the shipped _tree_identity returned rc 2 against /dev/full and its rc-2 channel carried the platform's REAL 'No space left on device' text"
  else
    bad "18a-enospc-real: expected rc 2 with real ENOSPC text on the rc-2 channel; rc='${e_rc:-<none>}' signature-lines='${e_sig:-<none>}'"
  fi
  if case "$e_line" in "disk-exhaustion: RECOGNISED (#3800)"*"'no-space-left-on-device'"*"IN-MEMORY subject 'tree-identity manifest write (start capture)'"*) true ;; *) false ;; esac; then
    ok "18b-enospc-attributed: a REAL ENOSPC in the tree-identity capture is RECOGNISED and named, with all three components PASS (nothing in the component-log subject set could ever have shown it)"
  else
    bad "18b-enospc-attributed: a real ENOSPC capture failure was not attributed; got: $e_line"
  fi
  if case "$e_mut" in "disk-exhaustion: RECOGNISED (#3800)"*) false ;; *) true ;; esac \
     && case "$e_mut" in "disk-exhaustion: 0 RECOGNISED (#3800) -- no non-PASS component to scan"*) true ;; *) false ;; esac; then
    ok "18c-enospc-mutation: removing the in-memory subject from the scan collapses the SAME real-ENOSPC run to the affirmative '0 RECOGNISED -- no non-PASS component to scan' -- which IS the false clean reading this round exists to remove, and proves the new arm is what makes 18b pass"
  else
    bad "18c-enospc-mutation: the verdict did not collapse as expected when the in-memory subject was removed, so 18b is not measuring the new arm; got: $e_mut"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (19) THE THIRD KIND OF SUBJECT: A `.result` VERDICT THE GATE COULD NOT READ
# (#3800, roborev job 304).
#
# THE DEFECT THIS PINS. `record_result` writes `$LOG_DIR/<component>.result`. Under ENOSPC
# that write fails, its error text goes to GATE STDERR -- neither a component log nor an
# in-memory subject -- and the parent's fail-closed guard then synthesises `FAIL 0` for a
# component whose OWN log is CLEAN, because the component may genuinely have SUCCEEDED and
# died on the write. Both existing subject channels are therefore empty and the scan
# rendered an affirmative `0 RECOGNISED`: the identical false-clean shape to round 3's
# tree-capture case, one writer over.
#
# Every case below therefore pairs an unreadable verdict with a DELIBERATELY CLEAN component
# log, and each has a MUTATION CONTROL in the same shell that removes the new channel and
# shows the false clean reading come straight back -- a green with no such contrast could be
# passing for any reason at all.
# ─────────────────────────────────────────────────────────────────────────────────
_disk_env() {   # the shared subshell preamble: shipped code, deterministic free field
  DISK_TARGET_PATH=""; DISK_LOGS_PATH=""
  DISK_FREE_START_TARGET=""; DISK_FREE_START_LOGS=""
  DISK_MEM_SUBJECTS=(); DISK_UNREAD_VERDICTS=(); TREE_CAPTURE_FAILED=0
}

# (19a) ABSENT `.result` + CLEAN component log -> UNMEASURED, never `0 RECOGNISED`. Recorded
# exactly as the shipped fail-closed guard records it (19g asserts the guard really does).
d="$tmp/c19a"; mkdir -p "$d"
{ echo 'running 37 tests'; echo 'test result: ok. 37 passed; 0 failed'; } > "$d/binding-rust-tests.log"
o19a=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _disk_note_unread_verdict binding-rust-tests "verdict file ABSENT"
  printf 'LINE %s\n' "$(_disk_exhaustion_line fmt PASS binding-rust-tests FAIL)"
  DISK_UNREAD_VERDICTS=()
  printf 'MUTANT %s\n' "$(_disk_exhaustion_line fmt PASS binding-rust-tests FAIL)"
)
l19a=$(printf '%s\n' "$o19a" | sed -n 's/^LINE //p')
m19a=$(printf '%s\n' "$o19a" | sed -n 's/^MUTANT //p')
if case "$l19a" in "disk-exhaustion: UNMEASURED (#3800)"*"binding-rust-tests(verdict file ABSENT)"*) true ;; *) false ;; esac \
   && case "$l19a" in *"0 RECOGNISED"*) false ;; *) true ;; esac; then
  ok "19a-absent-verdict: a component whose .result is ABSENT, whose own log is CLEAN, is UNMEASURED naming it -- never '0 RECOGNISED'"
else
  bad "19a-absent-verdict: expected UNMEASURED naming the unread verdict; got: $l19a"
fi
if case "$m19a" in "disk-exhaustion: 0 RECOGNISED (#3800) -- scanned 1 non-PASS component log(s) (binding-rust-tests)"*) true ;; *) false ;; esac; then
  ok "19a-mutation: dropping the unread-verdict channel returns the SAME run to the affirmative '0 RECOGNISED ... every subject was READ' -- which IS the false clean reading this round removes, and proves 19a measures the new arm"
else
  bad "19a-mutation: the verdict did not collapse to the false clean reading when the channel was emptied, so 19a is not measuring the new arm; got: $m19a"
fi

# (19b) MALFORMED `.result` -- the EMPTY file an ENOSPC write actually leaves behind
# (open+truncate succeeds, the write does not). Driven through the SHIPPED `_disk_verdict_read`.
d="$tmp/c19b"; mkdir -p "$d"
: > "$d/core-tests.result"
echo 'test result: ok. 3562 passed; 0 failed' > "$d/core-tests.log"
o19b=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _disk_verdict_read core-tests "$d/core-tests.result"; printf 'RC %s\n' "$?"
  printf 'N %s\n' "${#DISK_UNREAD_VERDICTS[@]}"
  printf 'LINE %s\n' "$(_disk_exhaustion_line fmt PASS core-tests FAIL)"
  DISK_UNREAD_VERDICTS=()
  printf 'MUTANT %s\n' "$(_disk_exhaustion_line fmt PASS core-tests FAIL)"
)
r19b=$(printf '%s\n' "$o19b" | sed -n 's/^RC //p')
n19b=$(printf '%s\n' "$o19b" | sed -n 's/^N //p')
l19b=$(printf '%s\n' "$o19b" | sed -n 's/^LINE //p')
m19b=$(printf '%s\n' "$o19b" | sed -n 's/^MUTANT //p')
if [ "${r19b:-}" = 1 ] && [ "${n19b:-0}" = 1 ] \
   && case "$l19b" in "disk-exhaustion: UNMEASURED (#3800)"*"core-tests(verdict file MALFORMED)"*) true ;; *) false ;; esac \
   && case "$l19b" in *"0 RECOGNISED"*) false ;; *) true ;; esac; then
  ok "19b-malformed-empty: the EMPTY .result an ENOSPC write leaves behind reads rc 1, records ONE unread verdict, and renders UNMEASURED naming it"
else
  bad "19b-malformed-empty: expected rc 1 + 1 recorded subject + UNMEASURED; rc='${r19b:-<none>}' recorded='${n19b:-<none>}' line: $l19b"
fi
if case "$m19b" in "disk-exhaustion: 0 RECOGNISED (#3800) -- scanned 1 non-PASS component log(s) (core-tests)"*) true ;; *) false ;; esac; then
  ok "19b-mutation: emptying the channel restores the affirmative clean reading over the SAME unreadable verdict"
else
  bad "19b-mutation: the malformed-verdict arm is not what makes 19b pass; got: $m19b"
fi

# (19c) A SHORT WRITE, not merely an empty one: a partially-flushed `.result` whose STATUS
# token is truncated. The status is validated against the CLOSED set record_result can write
# (PASS/FAIL/SKIP), so `PAS 12` is MALFORMED rather than a silently-adopted verdict.
d="$tmp/c19c"; mkdir -p "$d"
printf 'PAS 1' > "$d/write-tests.result"
echo 'test result: ok. 88 passed' > "$d/write-tests.log"
o19c=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _disk_verdict_read write-tests "$d/write-tests.result"; printf 'RC %s\n' "$?"
  printf 'LINE %s\n' "$(_disk_exhaustion_line write-tests FAIL)"
)
r19c=$(printf '%s\n' "$o19c" | sed -n 's/^RC //p')
l19c=$(printf '%s\n' "$o19c" | sed -n 's/^LINE //p')
if [ "${r19c:-}" = 1 ] && case "$l19c" in "disk-exhaustion: UNMEASURED (#3800)"*"write-tests(verdict file MALFORMED)"*) true ;; *) false ;; esac; then
  ok "19c-short-write: a truncated STATUS token is MALFORMED (the token is checked against the closed PASS/FAIL/SKIP set), not adopted as a verdict"
else
  bad "19c-short-write: a truncated status token was not caught; rc='${r19c:-<none>}' line: $l19c"
fi

# (19d) NEGATIVE CONTROL -- the arm must DISCRIMINATE. A WELL-FORMED `.result` records
# nothing, reads rc 0, returns its two fields, and leaves the clean reading intact.
d="$tmp/c19d"; mkdir -p "$d"
printf 'PASS 412\n' > "$d/core-tests.result"
echo 'error[E0308]: mismatched types' > "$d/minimal-build.log"
o19d=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _disk_verdict_read core-tests "$d/core-tests.result"; printf 'RC %s\n' "$?"
  printf 'ST %s\n' "$DISK_VERDICT_ST"; printf 'SECS %s\n' "$DISK_VERDICT_SECS"
  printf 'N %s\n' "${#DISK_UNREAD_VERDICTS[@]}"
  printf 'LINE %s\n' "$(_disk_exhaustion_line core-tests PASS minimal-build FAIL)"
)
if [ "$(printf '%s\n' "$o19d" | sed -n 's/^RC //p')" = 0 ] \
   && [ "$(printf '%s\n' "$o19d" | sed -n 's/^ST //p')" = PASS ] \
   && [ "$(printf '%s\n' "$o19d" | sed -n 's/^SECS //p')" = 412 ] \
   && [ "$(printf '%s\n' "$o19d" | sed -n 's/^N //p')" = 0 ] \
   && case "$(printf '%s\n' "$o19d" | sed -n 's/^LINE //p')" in "disk-exhaustion: 0 RECOGNISED (#3800)"*) true ;; *) false ;; esac; then
  ok "19d-negative-control: a WELL-FORMED .result reads rc 0 with both fields, records NOTHING, and leaves the clean reading intact -- the arm discriminates rather than firing on every component"
else
  bad "19d-negative-control: a well-formed .result did not read cleanly; got: $o19d"
fi

# (19i) THE UNREAD VERDICT ON AN OTHERWISE ALL-PASS RUN -- the case that makes the BRANCH
# ORDER load-bearing, and it is reachable rather than theoretical: a `.result` whose STATUS
# token is intact but whose SECONDS field is not (`PASS abc`) yields a component the table
# records as PASS while its verdict was NOT fully read. With the pre-#3800-job-304 order the
# `no non-PASS component to scan` arm got first refusal and rendered the affirmative clean
# reading over an unmeasured subject -- this issue's own defect, on a run with nothing else
# wrong. Found by MUTATION while writing this round: removing the reorder left every other
# case in this file green.
d="$tmp/c19i"; mkdir -p "$d"
printf 'PASS abc\n' > "$d/core-tests.result"
o19i=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _disk_verdict_read core-tests "$d/core-tests.result"; printf 'RC %s\n' "$?"
  printf 'ST %s\n' "$DISK_VERDICT_ST"
  printf 'LINE %s\n' "$(_disk_exhaustion_line fmt PASS core-tests PASS)"
  DISK_UNREAD_VERDICTS=()
  printf 'MUTANT %s\n' "$(_disk_exhaustion_line fmt PASS core-tests PASS)"
)
r19i=$(printf '%s\n' "$o19i" | sed -n 's/^RC //p')
s19i=$(printf '%s\n' "$o19i" | sed -n 's/^ST //p')
l19i=$(printf '%s\n' "$o19i" | sed -n 's/^LINE //p')
m19i=$(printf '%s\n' "$o19i" | sed -n 's/^MUTANT //p')
if [ "${r19i:-}" = 1 ] && [ "${s19i:-}" = PASS ] \
   && case "$l19i" in "disk-exhaustion: UNMEASURED (#3800)"*"core-tests(verdict file MALFORMED)"*) true ;; *) false ;; esac \
   && case "$l19i" in *"no non-PASS component to scan"*) false ;; *) true ;; esac; then
  ok "19i-allpass-unread: an unread verdict on a run where EVERY recorded component PASSed is still UNMEASURED -- the 'no non-PASS component to scan' arm does not get first refusal over an unmeasured subject"
else
  bad "19i-allpass-unread: an all-PASS run with an unread verdict reported a clean reading; rc='${r19i:-<none>}' st='${s19i:-<none>}' line: $l19i"
fi
if case "$m19i" in "disk-exhaustion: 0 RECOGNISED (#3800) -- no non-PASS component to scan (2/2 PASS)"*) true ;; *) false ;; esac; then
  ok "19i-mutation: with the channel emptied the SAME run renders 'no non-PASS component to scan (2/2 PASS)' -- the exact affirmative clean reading the branch order now prevents"
else
  bad "19i-mutation: the all-PASS arm is not what makes 19i pass; got: $m19i"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (19e) THE RECORD_RESULT IN-MEMORY ARM, DRIVEN BY A REAL ENOSPC. `record_result` is
# extracted from the shipped script and its four unrelated chokepoint hooks (`_hb_ensure`,
# the feature-matrix note, the two integrity asserts) are stubbed -- this case is about the
# VERDICT WRITE and nothing else. The `.result` path is a symlink to /dev/full, so the write
# gets the platform's own strerror. Every component PASSes, so under the OLD subject set the
# scan had nothing at all to look at.
#
# /dev/full is LINUX-ONLY (macOS is a first-class gate host), so the branch is guarded and
# the skip is DECLARED with the same case count -- a silent skip is the vacuous pass this
# suite exists to prevent.
# ─────────────────────────────────────────────────────────────────────────────────
if [ "$df_usable" -ne 1 ]; then
  ok "19e-record-result-enospc: DECLARED SKIP -- this host has no writable /dev/full, so a REAL ENOSPC on the verdict write cannot be induced hermetically"
  ok "19e-record-result-enospc: DECLARED SKIP (second half) -- the mutation control that removes the in-memory subject is skipped with it"
else
  d="$tmp/c19e"; mkdir -p "$d"
  ln -s /dev/full "$d/minimal-build.result"
  o19e=$(
    . "$EX"; LOG_DIR="$d"; _disk_env
    _hb_ensure() { :; }
    _fm_note_if_no_cargo_observed() { :; }
    _assert_summary_integrity() { :; }
    _assert_tree_integrity() { :; }
    record_result minimal-build PASS 611
    printf 'N %s\n' "${#DISK_MEM_SUBJECTS[@]}"
    printf 'LINE %s\n' "$(_disk_exhaustion_line fmt PASS minimal-build PASS)"
    DISK_MEM_SUBJECTS=()
    printf 'MUTANT %s\n' "$(_disk_exhaustion_line fmt PASS minimal-build PASS)"
  )
  n19e=$(printf '%s\n' "$o19e" | sed -n 's/^N //p')
  l19e=$(printf '%s\n' "$o19e" | sed -n 's/^LINE //p')
  m19e=$(printf '%s\n' "$o19e" | sed -n 's/^MUTANT //p')
  if [ "${n19e:-0}" = 1 ] \
     && case "$l19e" in "disk-exhaustion: RECOGNISED (#3800)"*"'no-space-left-on-device'"*"IN-MEMORY subject 'component verdict write (minimal-build.result)'"*) true ;; *) false ;; esac; then
    ok "19e-record-result-enospc: a REAL ENOSPC on the shipped record_result verdict write is captured IN MEMORY and RECOGNISED, with EVERY component PASS (nothing in either older subject set could have shown it)"
  else
    bad "19e-record-result-enospc: the verdict-write failure was not captured/attributed; recorded='${n19e:-<none>}' line: $l19e"
  fi
  if case "$m19e" in "disk-exhaustion: 0 RECOGNISED (#3800) -- no non-PASS component to scan"*) true ;; *) false ;; esac; then
    ok "19e-mutation: removing the in-memory subject collapses the SAME real-ENOSPC run to the affirmative '0 RECOGNISED -- no non-PASS component to scan'"
  else
    bad "19e-mutation: the verdict did not collapse when the in-memory subject was removed; got: $m19e"
  fi
fi

# (19f) NEGATIVE CONTROL FOR THE SAME ARM, and a guard on the wrapper itself: a NORMAL
# record_result must record NOTHING and must still write exactly the two-field verdict. The
# fix moved that redirect inside a command substitution, so "the write still happens, byte
# for byte" is a property this case has to assert rather than assume.
d="$tmp/c19f"; mkdir -p "$d"
o19f=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _hb_ensure() { :; }
  _fm_note_if_no_cargo_observed() { :; }
  _assert_summary_integrity() { :; }
  _assert_tree_integrity() { :; }
  record_result minimal-build PASS 611; printf 'RC %s\n' "$?"
  printf 'N %s\n' "${#DISK_MEM_SUBJECTS[@]}"
)
if [ "$(printf '%s\n' "$o19f" | sed -n 's/^RC //p')" = 0 ] \
   && [ "$(printf '%s\n' "$o19f" | sed -n 's/^N //p')" = 0 ] \
   && [ "$(cat "$d/minimal-build.result" 2>/dev/null)" = "PASS 611" ] \
   && [ "$(wc -l < "$d/minimal-build.result" | tr -d ' ')" = 1 ]; then
  ok "19f-record-result-clean: a successful verdict write records NOTHING on the in-memory channel and still emits exactly the two-field 'PASS 611' line (the capture wrapper did not change the artifact)"
else
  bad "19f-record-result-clean: a successful record_result mis-behaved; probe: $o19f file: '$(cat "$d/minimal-build.result" 2>/dev/null)'"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# (19g)(19h) STRUCTURAL, over the SHIPPED script. The cases above prove the scan ARM works;
# these prove it is WIRED -- the same distinction that let the tree-integrity boundary site
# ship with a marker whose evidence could never reach it.
# ─────────────────────────────────────────────────────────────────────────────────
# (19g) the fail-closed missing-result guard is where a MISSING verdict is DETECTED, so it is
# where the subject must be recorded. Both lanes, derived from the shipped source.
g_side=$(awk '/^for _sc in "\$\{SELECTED_SIDE/,/^done$/' "$GATE" | grep -c '_disk_note_unread_verdict')
g_main=$(awk '/^for _mc in "\$\{SELECTED_MAIN/,/^done$/' "$GATE" | grep -c '_disk_note_unread_verdict')
if [ "$g_side" -ge 1 ] && [ "$g_main" -ge 1 ]; then
  ok "19g-guard-wired: BOTH lanes of the shipped fail-closed missing-result guard record the component as an unread verdict, so the synthetic FAIL can never be scanned against a clean log and reported clean"
else
  bad "19g-guard-wired: the missing-result guard does not record an unread verdict (side=$g_side main=$g_main) -- the synthetic FAIL is back to being an unmeasured verdict the scan calls clean"
fi
# (19h) ONE READER. A second, private `read -r <st> <secs> < <a .result>` anywhere in the
# shipped script is a reader that records nothing -- exactly how this family keeps
# regenerating. Derived from source: every such read must be inside _disk_verdict_read.
raw_reads=$(awk '
  /^_disk_verdict_read\(\) \{/ { inb=1 }
  inb && /^\}$/ { inb=0; next }
  !inb && /read -r [A-Za-z_]+ [A-Za-z_]+ < / { print FILENAME ":" FNR ": " $0 }
' "$GATE")
if [ -z "$raw_reads" ]; then
  ok "19h-one-reader: no two-field verdict read outside _disk_verdict_read -- every .result site routes through the ONE reader that records unreadability"
else
  bad "19h-one-reader: a private two-field verdict read survives outside _disk_verdict_read (it records nothing, so its unreadable verdicts render clean):
$raw_reads"
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
# evidence -- this contract's own doctrine comment contains that placeholder); +9 (roborev job
# 301: the SECOND KIND OF SUBJECT. Case 17 pins the in-memory channel host-independently --
# RECOGNISED, our-vocabulary naming, the negative control that makes the arm discriminate, an
# empty capture, the never-recorded cross-check and the two #3312 injection halves: 7 -- and
# case 18 drives a GENUINE ENOSPC through the shipped _tree_identity at /dev/full: fixture
# provenance, the attributed verdict, and the mutation control that removes the subject from
# the scan: 3. Case 18 declares a 2-case SKIP where /dev/full is unavailable, so the floor
# holds on macOS too: 8 + 3 = 11 on a /dev/full host, 8 + 2 = 10 without it, and a floor must
# take the LOWER. The 8th case-17 entry is 17f, the 400 KB payload that pins the in-memory
# branch against pipefail+SIGPIPE turning a MATCH into UNMEASURED -- found by measurement while
# writing this round, not predicted.); +13 (roborev job 304: the THIRD KIND OF SUBJECT -- a
# `.result` verdict the gate could not READ (15 cases). 13b asserts the emitted line declares its SUBJECT
# set and NAMES the writers outside it, in all four renderings: 2. Case 19 pins the arm --
# absent verdict + clean log, its mutation control, the EMPTY .result an ENOSPC write really
# leaves, its mutation control, a truncated STATUS token, and the well-formed negative control
# that makes the arm discriminate: 6. The record_result in-memory arm at /dev/full plus its
# mutation control: 2 (and a 2-case DECLARED SKIP where /dev/full is unavailable, so the count
# is host-independent), with a success control that also proves the capture wrapper still
# writes the two-field artifact byte for byte: 1. Two STRUCTURAL cases prove the arm is WIRED
# rather than merely working -- the guard records, and no private two-field verdict read
# survives outside the ONE reader: 2. Case 19i is the pair MUTATION found -- an unread
# verdict on an otherwise ALL-PASS run, which the pre-fix branch order rendered clean and which
# every other case in this file missed: 2. 2+6+2+1+2+2 = 15.)
# ============================================================================
# (20) roborev job 316 -- THE MARKER SAID UNMEASURED AND THE VERDICT SAID CERTIFIED.
#
# Round 4 built a reader that DETECTS an unreadable `.result` and made it an UNMEASURED subject
# of the `disk-exhaustion:` line. It left the three AGGREGATION loops keying `OVERALL=FAIL` on
# the status token being exactly `FAIL` -- so a present-but-malformed verdict was recorded as
# unmeasured by the marker and left OVERALL untouched, and the gate of record could emit
# `RESULT: PASS` for a run in which a SELECTED component's verdict was never read. Two arms:
# the seconds GRAMMAR (which decided whether the subject was recorded at all) and the
# AGGREGATION (which decides whether the run may certify).
# ============================================================================

# (20a) THE SECONDS GRAMMAR. `case $v in ''|*[!0-9-]*)` reads as "digits, minus admitted" and
# admits `-`, `--`, `1-2`, `-1-`: a character-class complement cannot say WHERE the minus may
# appear. Each of those is a partially-written duration that passed as well-formed, so its
# component was OMITTED from the unread-verdict subject set -- the one place this channel exists
# to populate. Driven through the SHIPPED `_disk_verdict_read`, both directions, because a
# grammar tightened too far reds on correct input (`$((end - start))` can legitimately be
# negative on a backwards clock, and 117 call sites pass exactly that).
d="$tmp/c20a"; mkdir -p "$d"
o20a=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  for bad_secs in '-' '--' '1-2' '-1-' '12x' '-' ''; do
    printf 'PASS %s\n' "$bad_secs" > "$d/x.result"
    _disk_verdict_read x "$d/x.result" && printf 'ACCEPTED [%s]\n' "$bad_secs"
  done
  for ok_secs in 0 12 -12 000; do
    printf 'PASS %s\n' "$ok_secs" > "$d/y.result"
    _disk_verdict_read y "$d/y.result" || printf 'REFUSED [%s]\n' "$ok_secs"
  done
  printf 'DONE\n'
)
if case "$o20a" in *ACCEPTED*) false ;; *REFUSED*) false ;; *DONE*) true ;; *) false ;; esac; then
  ok "20a-seconds-grammar: every misplaced-minus shape (-, --, 1-2, -1-, 12x, empty) is MALFORMED, and a plain, zero-padded and NEGATIVE integer are all still accepted (the backwards-clock case 117 call sites can produce)"
else
  bad "20a-seconds-grammar: the grammar is wrong in one direction or the other: $o20a"
fi

# (20a-mutation) the PRE-FIX spelling, planted into the extracted copy, must ACCEPT the shapes
# 20a refuses -- otherwise 20a would pass against either implementation and measure nothing.
o20am=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _disk_secs_is_int() { case "${1-}" in ''|*[!0-9-]*) return 1 ;; esac; return 0; }
  for bad_secs in '-' '--' '1-2' '-1-'; do
    printf 'PASS %s\n' "$bad_secs" > "$d/x.result"
    _disk_verdict_read x "$d/x.result" && printf 'ACCEPTED [%s]\n' "$bad_secs"
  done
)
if [ "$(printf '%s\n' "$o20am" | grep -c '^ACCEPTED')" = 4 ]; then
  ok "20a-mutation: the pre-fix character-class spelling accepts all four misplaced-minus shapes, so 20a discriminates between the two implementations rather than passing on both"
else
  bad "20a-mutation: the pre-fix spelling did not accept the shapes 20a refuses, so 20a is not measuring the grammar fix: $o20am"
fi

# (20b) THE AGGREGATION WRAPPER's contract: an UNREAD verdict is normalised to a synthetic
# `FAIL 0` (so no caller can render a blank cell over an unread verdict, and no caller can read
# a non-FAIL status off one), the rc is passed through UNCHANGED (so ABSENT stays
# distinguishable from UNREADABLE -- the full gate treats absence as "not selected" and
# `--delta` treats it as a measurement failure), and a WELL-FORMED verdict is passed through
# VERBATIM (a wrapper that normalised everything would fail every run).
d="$tmp/c20b"; mkdir -p "$d"
o20b=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  : > "$d/empty.result"
  printf 'PASS abc\n' > "$d/badsecs.result"
  printf 'PAS 12\n'   > "$d/badtok.result"
  printf 'PASS 12\n'  > "$d/good.result"
  for c in empty badsecs badtok good; do
    rc=0; _disk_verdict_read_aggregate "$c" "$d/$c.result" || rc=$?
    printf '%s rc=%s st=%s secs=%s\n' "$c" "$rc" "$DISK_VERDICT_ST" "$DISK_VERDICT_SECS"
  done
  rc=0; _disk_verdict_read_aggregate gone "$d/gone.result" || rc=$?
  printf 'gone rc=%s st=%s secs=%s\n' "$rc" "$DISK_VERDICT_ST" "$DISK_VERDICT_SECS"
  printf 'RECORDED %s\n' "${#DISK_UNREAD_VERDICTS[@]}"
)
exp20b='empty rc=1 st=FAIL secs=0
badsecs rc=1 st=FAIL secs=0
badtok rc=1 st=FAIL secs=0
good rc=0 st=PASS secs=12
gone rc=2 st=FAIL secs=0
RECORDED 3'
if [ "$o20b" = "$exp20b" ]; then
  ok "20b-wrapper: every UNREAD shape (empty, truncated seconds, truncated token) normalises to a synthetic FAIL 0 at rc 1, a WELL-FORMED verdict passes through verbatim at rc 0, an ABSENT file keeps its distinct rc 2, and only the three present-but-unread ones are RECORDED as subjects"
else
  bad "20b-wrapper: contract violated.
--- got ---
$o20b
--- want ---
$exp20b"
fi

# (20c) THE PROPERTY THE FINDING WAS ABOUT, through the SHIPPED aggregator. `PASS abc` is the
# sharpest shape: the STATUS token is valid, so the pre-fix loops recorded `PASS` in the table
# and left OVERALL alone -- a certified run over a verdict that was never fully read. The
# EMPTY-file shape is included because it is what an ENOSPC write actually leaves behind.
d="$tmp/c20c"; mkdir -p "$d"
o20c=$(
  . "$EX"; . "$EX_AGG"; LOG_DIR="$d"; _disk_env
  # The shipped aggregator asks `_pool_selected` whether an ABSENT .result was skipped by --only or
  # is a measurement failure (job 358). It is not part of the extracted set, and a missing command
  # returns 127 -- which would take the `|| continue` branch and silently make every absence a
  # skip, i.e. the pre-fix behaviour. Stubbed to the real predicate's semantics.
  _pool_selected() { [ -z "${ONLY:-}" ] && return 0; case " ${ONLY//,/ } " in *" $1 "*) return 0 ;; esac; return 1; }
  printf 'PASS 3\n'   > "$d/file-size.result"
  printf 'PASS abc\n' > "$d/fmt.result"
  : > "$d/clippy.result"
  # ONLY names exactly the three components with a .result, so this case measures the WRAPPER's
  # disposition of an UNREAD verdict and not job 358's separate rule that a SELECTED component with
  # NO .result fails the run -- which would otherwise fail the run via roborev-lints and make the
  # mutation control below unable to discriminate.
  ONLY="file-size,fmt,clippy"
  OVERALL=PASS; NAMES=(); STATUSES=(); TIMES=()
  aggregate_lite_components
  printf 'OVERALL %s\n' "$OVERALL"
  for i in "${!NAMES[@]}"; do printf 'ROW %s %s %s\n' "${NAMES[$i]}" "${STATUSES[$i]}" "${TIMES[$i]}"; done
  printf 'RECORDED %s\n' "${#DISK_UNREAD_VERDICTS[@]}"
)
exp20c='OVERALL FAIL
ROW file-size PASS 3s
ROW fmt FAIL 0s
ROW clippy FAIL 0s
RECORDED 2'
if [ "$o20c" = "$exp20c" ]; then
  ok "20c-aggregation: the SHIPPED aggregator turns an unread verdict into OVERALL=FAIL and a FAIL 0 row -- a valid-token/truncated-seconds 'PASS abc' can no longer render PASS beside a certified RESULT, and the well-formed sibling is untouched"
else
  bad "20c-aggregation: the aggregation does not fail closed on an unread verdict.
--- got ---
$o20c
--- want ---
$exp20c"
fi

# (20c-mutation) restore the pre-fix disposition (read raw, do not normalise, never signal) and
# the SAME run must certify -- which IS the false PASS this round removes. Without this control a
# green 20c proves only that the aggregator sets OVERALL somewhere.
o20cm=$(
  . "$EX"; . "$EX_AGG"; LOG_DIR="$d"; _disk_env
  _pool_selected() { [ -z "${ONLY:-}" ] && return 0; case " ${ONLY//,/ } " in *" $1 "*) return 0 ;; esac; return 1; }
  _disk_verdict_read_aggregate() { _disk_verdict_read "$1" "$2" || true; return 0; }
  ONLY="file-size,fmt,clippy"
  OVERALL=PASS; NAMES=(); STATUSES=(); TIMES=()
  aggregate_lite_components
  printf 'OVERALL %s\n' "$OVERALL"
)
if [ "$o20cm" = "OVERALL PASS" ]; then
  ok "20c-mutation: with the pre-fix disposition restored the identical run reports OVERALL=PASS over two unread verdicts -- the false certification this round removes, and proof 20c measures the new arm"
else
  bad "20c-mutation: the pre-fix disposition did not certify, so 20c is not measuring the aggregation fix; got: $o20cm"
fi

# (20d) STRUCTURAL -- the wrapper cannot be BYPASSED, and each of its call sites must DISPOSE of
# the failure. Behavioural cases only cover the sites someone already thought of; this is a
# census in the MARKED/EXEMPT/GAP idiom case 14 uses, and it is what stops a fourth aggregation
# site being added straight back into the hole. Exactly ONE raw `_disk_verdict_read` call site is
# permitted and it must be inside the wrapper; every wrapper call site must either force OVERALL
# or carry a declared renderer exemption.
raw20d=$(grep -c '_disk_verdict_read "' "$GATE" || true)
raw_in_wrapper=$(awk '
  /^_disk_verdict_read_aggregate\(\) \{/ { inw=1 }
  inw && /_disk_verdict_read "/ { n++ }
  inw && /^\}$/ { inw=0 }
  END { print n+0 }' "$GATE")
if [ "$raw20d" = 1 ] && [ "$raw_in_wrapper" = 1 ]; then
  ok "20d-no-bypass: the raw two-field reader has exactly ONE call site in the shipped gate and it is inside the aggregation wrapper -- no aggregation path can consume a verdict without the fail-closed normalisation"
else
  bad "20d-no-bypass: raw _disk_verdict_read call sites=$raw20d (inside the wrapper=$raw_in_wrapper); a raw read outside the wrapper renders an unread verdict as a non-FAIL status while the run certifies"
fi
agg_sites=$(grep -c '_disk_verdict_read_aggregate "' "$GATE" || true)
agg_fails=$(grep -c '_disk_verdict_read_aggregate "[^"]*" "[^"]*" || OVERALL=FAIL' "$GATE" || true)
agg_exempt=$(grep -c '_disk_verdict_read_aggregate "[^"]*" "[^"]*" || true' "$GATE" || true)
if [ "$agg_sites" -ge 3 ] && [ "$agg_fails" -ge 3 ] && [ $((agg_fails + agg_exempt)) -eq "$agg_sites" ]; then
  ok "20d-census: all $agg_sites wrapper call sites dispose of the failure -- $agg_fails force OVERALL=FAIL (the three certifying aggregations: full-gate reconstruction, --lite, --delta) and $agg_exempt are the DECLARED mid-run renderer exemptions inside a block that is already a FAIL emit; a new site with neither disposition is a GAP and reds here"
else
  bad "20d-census: wrapper call sites=$agg_sites forcing-OVERALL=$agg_fails declared-exempt=$agg_exempt -- every site must do one or the other, and at least the three certifying aggregations must force OVERALL"
fi

# +6 (roborev job 316: THE MARKER SAID UNMEASURED AND THE VERDICT SAID CERTIFIED. The seconds
# grammar, both directions, plus the mutation that proves the pre-fix character-class spelling
# accepts the four misplaced-minus shapes: 2. The wrapper contract -- three unread shapes
# normalised, a well-formed verdict verbatim, ABSENT keeping its distinct rc, and only the
# present-but-unread ones recorded: 1. The property itself through the SHIPPED
# aggregate_lite_components, plus the mutation that restores the pre-fix disposition and
# recertifies the same run: 2. The no-bypass + call-site-disposition census: 1 (two ok() calls,
# counted as 1 case each -> the two structural asserts are the 6th and 7th ok, so the floor
# rises by 7, not 6.) 2+1+2+2 = 7, plus the separate extract-agg provenance case = 8.)
# ============================================================================
# (21) roborev job 319 -- TWO FALSE-PASS ROUTES THAT SURVIVED ROUND 5.
# ============================================================================

# (21a) THE TERMINATOR IS PART OF THE VERDICT. `record_result` writes `printf '%s %s\n'`; an
# ENOSPC short write can truncate that ON A FIELD BOUNDARY. `PASS 12` losing only its newline,
# and `PASS 1` being all that reached the disk of `PASS 12`, BOTH parse as well-formed two-field
# verdicts -- the second with a VALID integer that is simply the wrong number -- so every content
# check passes and the sole remaining evidence is the MISSING NEWLINE, which `read` reports as a
# nonzero status and the pre-fix code discarded with `|| true`. Trailing content is refused on the
# same principle: what is on disk must be exactly the one terminated line the writer emits.
d="$tmp/c21a"; mkdir -p "$d"
o21a=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  printf 'PASS 12'          > "$d/a.result"   # newline lost
  printf 'PASS 1'           > "$d/b.result"   # truncated mid-number: VALID integer, wrong value
  printf 'PASS 12\nPASS 9\n'> "$d/c.result"   # trailing content
  printf 'PASS 12\nJUNK'    > "$d/e.result"   # trailing content, itself unterminated
  printf 'PASS 12\n'        > "$d/d.result"   # the WELL-FORMED control
  for c in a b c e d; do
    rc=0; _disk_verdict_read "$c" "$d/$c.result" || rc=$?
    printf '%s rc=%s\n' "$c" "$rc"
  done
  printf 'RECORDED %s\n' "${#DISK_UNREAD_VERDICTS[@]}"
)
exp21a='a rc=1
b rc=1
c rc=1
e rc=1
d rc=0
RECORDED 4'
if [ "$o21a" = "$exp21a" ]; then
  ok "21a-terminator: an unterminated verdict line (incl. one truncated to a VALID but WRONG integer) and any trailing content are MALFORMED and RECORDED, while the well-formed terminated line still reads clean"
else
  bad "21a-terminator: the terminator/trailing-content contract is not enforced.
--- got ---
$o21a
--- want ---
$exp21a"
fi
o21am=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  # The pre-fix read: take the two fields, discard read's status, check content only.
  _disk_verdict_read() {
    local comp="${1:-}" rf="${2:-}"
    DISK_VERDICT_ST=""; DISK_VERDICT_SECS=""
    [ -f "$rf" ] || return 2
    read -r DISK_VERDICT_ST DISK_VERDICT_SECS < "$rf" || true
    case "$DISK_VERDICT_ST" in PASS|FAIL|SKIP) ;; *) return 1 ;; esac
    _disk_secs_is_int "$DISK_VERDICT_SECS" || return 1
    return 0
  }
  for c in a b c; do
    rc=0; _disk_verdict_read "$c" "$d/$c.result" || rc=$?
    printf '%s rc=%s st=%s secs=%s\n' "$c" "$rc" "$DISK_VERDICT_ST" "$DISK_VERDICT_SECS"
  done
)
exp21am='a rc=0 st=PASS secs=12
b rc=0 st=PASS secs=1
c rc=0 st=PASS secs=12'
if [ "$o21am" = "$exp21am" ]; then
  ok "21a-mutation: the pre-fix read ADOPTS all three truncated verdicts as clean PASSes -- including 'PASS 1', a verdict whose duration is simply wrong -- so 21a measures the terminator check and not something else"
else
  bad "21a-mutation: the pre-fix read did not adopt the truncated verdicts, so 21a is not measuring the fix.
--- got ---
$o21am
--- want ---
$exp21am"
fi

# (21b) THE SIDE-LANE MARKER CHANNEL IS A FILE ON THE FILESYSTEM THAT MAY BE FULL -- the SIDE-lane
# half of this issue's own ENOSPC path, and it was fail-OPEN. A SIDE-lane component detecting a
# tree-capture failure escalates by APPENDING to $LOG_DIR/tree-integrity.fail; under ENOSPC that
# append fails, the pre-fix code swallowed it with `|| true`, the in-memory channel is lost at the
# subshell boundary, and the component's own `.result` is a COMPLETE WELL-FORMED PASS written
# before the disk filled -- so nothing anywhere was malformed and the run could CERTIFY.
#
# Driven through the SHIPPED `_tree_boundary_fail` with a REAL ENOSPC: the marker path is a
# symlink to /dev/full, so the append returns the platform's own ENOSPC. The assertion is the
# zero-allocation fallback -- the component's verdict is TRUNCATED, which round 4 already made a
# first-class UNREAD verdict and job 316 already made fatal.
if [ -c /dev/full ] && : 2>/dev/null >/dev/full; then
  d="$tmp/c21b"; mkdir -p "$d"
  ln -s /dev/full "$d/tree-integrity.fail"
  printf 'PASS 611\n' > "$d/legacy-heuristics.result"
  o21b=$(
    . "$EX"; LOG_DIR="$d"; _disk_env
    # A SUBSHELL is what makes this the SIDE-lane branch: the shipped function selects on
    # BASHPID != $$, so this is the real lane discriminator and not a flag the test sets.
    ( _tree_boundary_fail legacy-heuristics "tree-capture-failed; the tree cannot be proven unchanged" capture-failed ) 2>&1 | sed -n 's/.*\(marker write FAILED\).*/STDERR \1/p'
    printf 'SIZE %s\n' "$(wc -c < "$d/legacy-heuristics.result" | tr -d ' ')"
    rc=0; _disk_verdict_read legacy-heuristics "$d/legacy-heuristics.result" || rc=$?
    printf 'RC %s\n' "$rc"
    printf 'LINE %s\n' "$(_disk_exhaustion_line legacy-heuristics FAIL)"
  )
  size21b=$(printf '%s\n' "$o21b" | sed -n 's/^SIZE //p')
  rc21b=$(printf '%s\n' "$o21b" | sed -n 's/^RC //p')
  line21b=$(printf '%s\n' "$o21b" | sed -n 's/^LINE //p')
  if [ "${size21b:-x}" = 0 ] && [ "${rc21b:-}" = 1 ] \
     && case "$o21b" in *"STDERR marker write FAILED"*) true ;; *) false ;; esac \
     && case "$line21b" in "disk-exhaustion: UNMEASURED (#3800)"*"legacy-heuristics(verdict file MALFORMED)"*) true ;; *) false ;; esac; then
    ok "21b-side-enospc: on a REAL ENOSPC marker write the SIDE lane says so and INVALIDATES its own verdict (truncate allocates nothing, so it succeeds where the append failed) -- the parent then reads an unread verdict and the line is UNMEASURED naming the component, instead of certifying a well-formed PASS"
  else
    bad "21b-side-enospc: the SIDE-lane fallback did not fire: size='${size21b:-<none>}' rc='${rc21b:-<none>}' line: $line21b
raw: $o21b"
  fi
  # NEGATIVE CONTROL: with a WRITABLE marker path the verdict must be left ALONE -- the fallback
  # must fire only when the marker channel actually failed, or every SIDE-lane detection would
  # lose its reason.
  d="$tmp/c21bn"; mkdir -p "$d"
  printf 'PASS 611\n' > "$d/legacy-heuristics.result"
  o21bn=$(
    . "$EX"; LOG_DIR="$d"; _disk_env
    ( _tree_boundary_fail legacy-heuristics "mid-run mutation" mutation ) 2>/dev/null
    printf 'SIZE %s\n' "$(wc -c < "$d/legacy-heuristics.result" | tr -d ' ')"
    printf 'MARKER %s\n' "$(wc -l < "$d/tree-integrity.fail" | tr -d ' ')"
  )
  if case "$o21bn" in *"SIZE 9"*"MARKER 1"*) true ;; *) false ;; esac; then
    ok "21b-control: with a WRITABLE marker path the marker is recorded and the component's verdict is left untouched -- the invalidation fires only on a failed marker write, so a normal SIDE-lane detection keeps its reason"
  else
    bad "21b-control: the fallback fired (or the marker was not recorded) on a HEALTHY marker write: $o21bn"
  fi
else
  printf 'SKIP - 21b-side-enospc: /dev/full is unavailable/unwritable on this host, so a GENUINE ENOSPC marker write cannot be induced (Linux-only). DECLARED, not silently omitted; 21a and the job-316 cases carry the rest of the property.\n'
fi

# (22) roborev job 319 round 3 -- A COMPONENT CAN VANISH FROM A CERTIFICATION.
#
# The verdict-write failure was ATTRIBUTED and not DISPOSED of (job 316's correction, one
# function over), and two aggregation paths read an ABSENT `.result` as benign:
#   * the full gate excluded `file-size` from SELECTED_MAIN (it runs inline before the dataset
#     preflight and must not be dispatched twice), so it was the ONE selected component the
#     post-drain presence guard did not cover -- and the reconstruction loop skips an absent
#     `.result` as "not selected". An ENOSPC stopping `file-size.result` therefore OMITTED the
#     component from a gate reporting RESULT: PASS.
#   * the lite aggregator read "absent" as "--only skipped it", which is only one of its two
#     causes; the other is "it ran and its verdict never reached the disk".

# (22a) STRUCTURAL: file-size must be RECORDED as selected while still not being DISPATCHED.
# Both halves matter and they pull in opposite directions, which is why the original code was
# written the way it was -- a test that checked only one would license re-breaking the other.
fs_sel=$(awk '
  /^launch_components\(\) \{/ { inb=1 }
  inb && /_pool_selected "\$c" && SELECTED_MAIN\+=\("\$c"\)/ { sel=1 }
  inb && /dispatch_component "\$c"/ { disp=1 }
  inb && /^\}$/ { inb=0 }
  END { printf "sel=%d\n", sel+0 }' "$GATE")
fs_skips_dispatch=$(awk '
  /^launch_components\(\) \{/ { inb=1 }
  inb && /\[ "\$c" = file-size \]/ { seen=1 }
  inb && seen && /continue/ { c++ }
  inb && /^\}$/ { inb=0 }
  END { print (c+0 > 0) ? "yes" : "no" }' "$GATE")
if [ "$fs_sel" = "sel=1" ] && [ "$fs_skips_dispatch" = yes ]; then
  ok "22a-file-size-selected: file-size is RECORDED in SELECTED_MAIN (so the fail-closed presence guard covers it) while still being skipped for DISPATCH (it already ran inline before the dataset preflight) -- the two halves that pull in opposite directions"
else
  bad "22a-file-size-selected: file-size is not both recorded-as-selected and skipped-for-dispatch (recorded: $fs_sel, skips dispatch: $fs_skips_dispatch); if it is not recorded, an ENOSPC on its .result omits it from a RESULT: PASS"
fi

# (22b) THE LITE PATH, through the SHIPPED aggregator. With no `--only`, an absent `.result` for a
# component this run SELECTED is a measurement failure and must fail; under `--only` the very same
# absence is a legitimate skip. One predicate decides -- `_pool_selected`, the same one
# run_file_size/run_component early-return on -- so the two cases cannot be confused.
d="$tmp/c22b"; mkdir -p "$d"
o22b=$(
  . "$EX"; . "$EX_AGG"; LOG_DIR="$d"; _disk_env
  _pool_selected() { [ -z "$ONLY" ] && return 0; case " ${ONLY//,/ } " in *" $1 "*) return 0 ;; esac; return 1; }
  printf 'PASS 3\n' > "$d/file-size.result"
  printf 'PASS 9\n' > "$d/fmt.result"
  # clippy + roborev-lints ran but their verdicts never reached the disk.
  ONLY=""; OVERALL=PASS; NAMES=(); STATUSES=(); TIMES=(); DISK_UNREAD_VERDICTS=()
  aggregate_lite_components
  printf 'PLAIN %s rows=%s recorded=%s\n' "$OVERALL" "${#NAMES[@]}" "${#DISK_UNREAD_VERDICTS[@]}"
  # Same directory, but this run only ever selected file-size and fmt: the identical absences are
  # now legitimate skips and must NOT fail.
  ONLY="file-size,fmt"; OVERALL=PASS; NAMES=(); STATUSES=(); TIMES=(); DISK_UNREAD_VERDICTS=()
  aggregate_lite_components
  printf 'ONLY %s rows=%s recorded=%s\n' "$OVERALL" "${#NAMES[@]}" "${#DISK_UNREAD_VERDICTS[@]}"
)
exp22b='PLAIN FAIL rows=4 recorded=2
ONLY PASS rows=2 recorded=0'
if [ "$o22b" = "$exp22b" ]; then
  ok "22b-lite-absent: with no --only, a SELECTED component whose verdict never reached the disk FAILS the lite run and is recorded as an unread verdict (4 rows, not 2); with --only naming the other two, the IDENTICAL absences are legitimate skips and the run still passes"
else
  bad "22b-lite-absent: absence is not being discriminated by SELECTION.
--- got ---
$o22b
--- want ---
$exp22b"
fi
o22bm=$(
  . "$EX"; . "$EX_AGG"; LOG_DIR="$d"; _disk_env
  _pool_selected() { return 0; }
  # The pre-fix reading: absent => skip, unconditionally.
  aggregate_lite_components() {
    local -a LN=() LS=() LT=(); local c rf st secs
    for c in file-size fmt clippy roborev-lints; do
      rf="$LOG_DIR/$c.result"; [ -f "$rf" ] || continue
      _disk_verdict_read_aggregate "$c" "$rf" || OVERALL=FAIL
      st="$DISK_VERDICT_ST"; secs="$DISK_VERDICT_SECS"
      LN+=("$c"); LS+=("$st"); LT+=("${secs}s"); [ "$st" = FAIL ] && OVERALL=FAIL
    done
    NAMES=("${LN[@]+"${LN[@]}"}")
  }
  ONLY=""; OVERALL=PASS; NAMES=(); DISK_UNREAD_VERDICTS=()
  aggregate_lite_components
  printf 'MUTANT %s rows=%s\n' "$OVERALL" "${#NAMES[@]}"
)
if [ "$o22bm" = "MUTANT PASS rows=2" ]; then
  ok "22b-mutation: the pre-fix reading certifies the SAME directory with two components silently missing from the table -- the vanishing this case removes"
else
  bad "22b-mutation: the pre-fix reading did not certify, so 22b is not measuring the selection fix; got: $o22bm"
fi

# (22c) `record_result` FAILS THE RUN, not merely records. Driven through the SHIPPED function at
# /dev/full so the write failure is a REAL ENOSPC. Attribution and disposition are asserted
# TOGETHER: the pre-fix code had the first and not the second.
if [ -c /dev/full ] && : 2>/dev/null >/dev/full; then
  d="$tmp/c22c"; mkdir -p "$d"
  o22c=$(
    . "$EX"; LOG_DIR="$d"; _disk_env
    _hb_ensure() { :; }; _fm_note_if_no_cargo_observed() { :; }
    _assert_summary_integrity() { :; }; _tree_check_boundary() { :; }
    ln -s /dev/full "$d/core-tests.result"
    OVERALL=PASS
    record_result core-tests PASS 611 >/dev/null 2>&1
    printf 'OVERALL %s\n' "$OVERALL"
    printf 'RECORDED %s\n' "${#DISK_MEM_SUBJECTS[@]}"
  )
  if case "$o22c" in *"OVERALL FAIL"*) true ;; *) false ;; esac \
     && case "$o22c" in *"RECORDED 1"*) true ;; *) false ;; esac; then
    ok "22c-record-result-fails: a REAL ENOSPC verdict write both RECORDS the in-memory subject and sets OVERALL=FAIL -- attributing a write failure is not disposing of it, and the shell that knows the verdict never reached disk is the one that fails the run"
  else
    bad "22c-record-result-fails: expected OVERALL FAIL and one recorded subject; got: $o22c"
  fi
else
  printf 'SKIP - 22c-record-result-fails: /dev/full unavailable/unwritable, so a GENUINE ENOSPC verdict write cannot be induced (Linux-only). DECLARED, not silently omitted.\n'
fi

# (24) roborev job 348 -- THE CAPTURE IS BOUNDED, AND A FAILED OPEN IS NOT A CLEAN SCAN.
#
# (24a) `-o` means the captured text is the MATCHED PHRASE from our own closed signature set, not
# the whole log line -- which has no length limit. A command substitution holding an unbounded
# string is the wrong thing to build into a diagnostic that runs when a resource has been
# exhausted, on the path to the terminal emit. Measured on a single 2 MB line: the reported line
# number must still be right, and the RECOGNISED verdict unchanged.
d="$tmp/c24a"; mkdir -p "$d"
# Built by DOUBLING, not by appending one character at a time -- the naive loop is O(n^2) in awk
# and turned this case into a multi-minute hang the first time it was written.
{
  printf 'padding line\n'
  awk 'BEGIN{ s="x"; while (length(s) < 1000000) s = s s; printf "%s No space left on device %s\n", s, s }'
} > "$d/core-tests.log"
out=$(run_line "$d" core-tests FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*) true ;; *) false ;; esac \
   && case "$out" in *"core-tests.log:2"*) true ;; *) false ;; esac \
   && [ "${#out}" -lt 4000 ]; then
  ok "24a-bounded: a 2 MB matching line is still RECOGNISED at the right line number (2) and the emitted line stays small (${#out} chars) -- the capture holds the matched PHRASE, not the log line"
else
  bad "24a-bounded: expected RECOGNISED at line 2 with a small emitted line; len=${#out} got: ${out:0:400}"
fi

# (24b) A FAILED OPEN IS rc 2, NOT rc 1. With `< "$file"` a failed open is reported by BASH as
# status 1 -- identical to grep's "no match" -- so a subject that passed `-r` and then could not be
# opened read as a CLEAN scan. The file is now an OPERAND, so the open is grep's problem and grep
# says rc 2. Induced with a path that passes the caller's checks and cannot be opened by grep: a
# file inside a directory stripped of execute permission (search) after the fact.
d="$tmp/c24b"; mkdir -p "$d/sub"
echo 'nothing here' > "$d/sub/inner.log"
chmod 000 "$d/sub" 2>/dev/null || true
if ! cat "$d/sub/inner.log" >/dev/null 2>&1; then
  o24b=$(
    . "$EX"; LOG_DIR="$d"; _disk_env
    rc=0; _disk_scan_subject file "$d/sub/inner.log" || rc=$?
    printf 'RC %s\n' "$rc"
  )
  chmod 755 "$d/sub" 2>/dev/null || true
  if [ "$(printf '%s\n' "$o24b" | sed -n 's/^RC //p')" = 2 ]; then
    ok "24b-open-fail: a subject that cannot be OPENED yields rc 2 (UNMEASURED), not rc 1 -- so a failed open is never reported as 'read successfully, no signature'"
  else
    bad "24b-open-fail: an unopenable subject did not yield rc 2; got: $o24b"
  fi
else
  chmod 755 "$d/sub" 2>/dev/null || true
  printf 'SKIP - 24b-open-fail: this host still reads a file under a mode-000 directory (running as root), so a failed open cannot be induced. DECLARED, not silently omitted.\n'
fi

# (27) roborev job 370 -- ENUMERATE THE CLASS, DO NOT FIX THE INSTANCES.
#
# Job 358 found that three pre-flight blocks claimed "no component has run" when `run_file_size`
# precedes them. Three sites were fixed; a FOURTH -- the `--only` zero-Data.db block, reachable via
# e.g. `--only file-size,core-tests` -- kept the same false exemption and was found a round later.
# Per-site comments cannot prevent that, because the comment is exactly what is wrong. So the class
# is DERIVED: an emit site that can be reached after `run_file_size` MUST be marked, and an
# exemption there is a FAIL no matter how it is worded.
#
# Reachability is computed one level deep, which is enough for this script's shape: a site is
# post-component if it is a TOP-LEVEL line after the `run_file_size` call, or if it lives in a
# function that is CALLED from such a line. That is an under-approximation (a deeper call chain
# would be missed), so it is DECLARED as such -- it enumerates a superset of the four known sites
# and cannot silently shrink to nothing, which the floor below checks.
_pc_prog="$tmp/postcomp.awk"
cat > "$_pc_prog" <<'POSTCOMP_AWK'
{ line[NR] = $0 }
END {
  # 1. the top-level run_file_size call
  for (i = 1; i <= NR; i++) if (line[i] ~ /^run_file_size$/) { fs = i; break }
  if (!fs) { print "ERROR no-top-level-run_file_size"; exit }
  # 2. functions called from a top-level line AFTER it (bare call, no leading whitespace, and the
  #    indented one-per-line calls inside the top-level `if` blocks that follow)
  for (i = fs + 1; i <= NR; i++) {
    if (line[i] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/) { infn = 1 }
    if (infn && line[i] ~ /^\}/) { infn = 0; continue }
    if (infn) continue
    if (line[i] ~ /^[ \t]*#/) continue
    if (match(line[i], /^[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*$/)) {
      nm = line[i]; gsub(/^[ \t]+|[ \t]+$/, "", nm); called[nm] = 1
    }
    toplevel_after[i] = 1
  }
  # 3. every emit site, classified
  for (i = 1; i <= NR; i++) {
    l = line[i]
    if (l ~ /^[ \t]*#/) continue
    if (l ~ /^(emit_summary|_emit_terminal_summary)\(\)/) continue
    if (l !~ /(^|[^_a-zA-Z])(emit_summary|_emit_terminal_summary)[ \t]/) continue
    # which function is this site in?
    owner = ""
    for (k = i; k > 0; k--) {
      if (line[k] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/) { owner = line[k]; sub(/\(\).*/, "", owner); break }
      if (line[k] ~ /^\}/) break
    }
    post = 0
    if (owner == "" && toplevel_after[i]) post = 1
    if (owner != "" && called[owner]) post = 1
    if (!post) continue
    # marked? (directly, via an array append, or via a helper that appends)
    args = l; j = i
    while (args ~ /\\[ \t]*$/) { j++; args = args "\n" line[j] }
    marked = (args ~ /_disk_exhaustion_line/) ? 1 : 0
    if (!marked && args ~ /DISK_PREFLIGHT_META/) marked = 1
    # The backward window is BOUNDED and stops at anything that means "a different block": a
    # previous emit site, or a function/brace boundary. An unbounded (or merely long) window let a
    # NEIGHBOURING site's helper call credit a site that had none, which is how the control below
    # first failed to discriminate.
    if (!marked) for (k = i - 1; k > 0 && k > i - 15; k--) {
      if (line[k] ~ /(^|[^_a-zA-Z])(emit_summary|_emit_terminal_summary)[ \t]/) break
      if (line[k] ~ /^\}/ || line[k] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/) break
      if (line[k] ~ /^[ \t]*#/) continue
      if (line[k] ~ /_disk_preflight_meta/ || line[k] ~ /_disk_exhaustion_line/) { marked = 1; break }
    }
    printf "%s\t%d\t%s\n", (marked ? "POST-MARKED" : "POST-EXEMPT"), i, (owner == "" ? "<top-level>" : owner)
  }
}
POSTCOMP_AWK
_pc_out=$(awk -f "$_pc_prog" "$GATE")
_pc_total=$(printf '%s\n' "$_pc_out" | grep -c '^POST-' || true)
_pc_bad=$(printf '%s\n' "$_pc_out" | grep -c '^POST-EXEMPT' || true)
if case "$_pc_out" in *ERROR*) true ;; *) false ;; esac; then
  bad "27-postcomponent: the derivation could not find the top-level run_file_size call, so this class check is vacuous: $_pc_out"
elif [ "$_pc_total" -ge 4 ] && [ "$_pc_bad" = 0 ]; then
  ok "27-postcomponent: all $_pc_total emit sites reachable AFTER run_file_size append the attribution -- the CLASS is derived from the call graph, so a fifth such block cannot ship with an 'emitted before the component loop' comment the way the --only block did"
else
  bad "27-postcomponent: $_pc_bad of $_pc_total post-component emit site(s) are still exempt (a floor of 4 is expected; a smaller total means the derivation went blind):
$_pc_out"
fi
# POSITIVE CONTROL: strip the marking from one post-component site and require the class check to
# see it. Without this, a derivation that classified nothing as post-component would read clean.
# The mutation removes BOTH the array expansion -- what actually places the line in the emitted
# block -- and the helper call, at EVERY site. Stripping one site was not enough to discriminate:
# with the expansion gone the site still looked marked by the neighbouring call inside the backward
# window, which is what prompted bounding that window above.
_pc_ctl="$tmp/postcomp-ctl.sh"
awk '
  /DISK_PREFLIGHT_META\[@\]/ { next }
  /^[ \t]*_disk_preflight_meta$/ { next }
  { print }
' "$GATE" > "$_pc_ctl"
_pc_ctl_out=$(awk -f "$_pc_prog" "$_pc_ctl")
if [ "$(printf '%s\n' "$_pc_ctl_out" | grep -c '^POST-EXEMPT')" -ge 1 ]; then
  ok "27-control: removing the attribution from EVERY post-component pre-flight site makes the class check report them, so the clean reading above is a measurement rather than an empty classification"
else
  bad "27-control: the class check did not notice a stripped post-component site -- it is blind:
$_pc_ctl_out"
fi

# (26) roborev job 358 -- THE PRE-FLIGHT BLOCKS ARE EMITTED **AFTER** A COMPONENT HAS RUN.
#
# Their exemptions read "no component has run, so there is nothing to attribute". That was FALSE:
# `run_file_size` executes BEFORE `apply_fixture_preflight` and `apply_schemas_preflight`
# (deliberately -- it needs no dataset, and those guards exit when the corpus is absent), so a
# file-size that died of ENOSPC was named by neither the block's contents nor any attribution: this
# issue's opening defect, inside an exemption written for this issue.
#
# (26a) ORDERING, ASSERTED FROM THE SOURCE rather than reasoned about -- which is the actual lesson,
# since this is the FOURTH time in this issue that an exemption or a subject set rested on a wrong
# claim about what could already have run. If someone reorders these calls the reasoning changes and
# this case must be re-decided, so it reds rather than silently passing.
_ord_fs=$(awk '/^run_file_size$/ { print NR; exit }' "$GATE")
_ord_fx=$(awk '/^[[:space:]]+apply_fixture_preflight$/ { print NR; exit }' "$GATE")
_ord_sc=$(awk '/^[[:space:]]+apply_schemas_preflight$/ { print NR; exit }' "$GATE")
if [ -n "$_ord_fs" ] && [ -n "$_ord_fx" ] && [ -n "$_ord_sc" ] \
   && [ "$_ord_fs" -lt "$_ord_fx" ] && [ "$_ord_fs" -lt "$_ord_sc" ]; then
  ok "26a-ordering: run_file_size (line $_ord_fs) really does execute BEFORE apply_fixture_preflight ($_ord_fx) and apply_schemas_preflight ($_ord_sc) -- so those blocks CAN carry a recorded verdict, which is why they attribute instead of claiming nothing has run"
else
  bad "26a-ordering: could not establish the ordering from source (file-size=${_ord_fs:-<none>} fixtures=${_ord_fx:-<none>} schemas=${_ord_sc:-<none>}) -- if the calls were reordered the pre-flight attribution reasoning must be re-decided, not silently kept"
fi
# ...and the three pre-flight sites must be MARKED, not exempt. Derived from the census output so it
# cannot drift from the classifier the rest of case 14 uses.
_pf_exempt=$(disk_census | awk -F'\t' '$1 == "EXEMPT" && $3 ~ /emit_summary FAIL/ { n++ } END { print n+0 }')
_pf_marked=$(disk_census | grep -c '^MARKED-VIA-HELPER-_disk_preflight_meta' || true)
if [ "$_pf_marked" = 4 ]; then
  ok "26a-preflight-marked: all 4 post-run_file_size pre-flight FAIL blocks append the attribution through the shared _disk_preflight_meta helper (census-derived), so an ENOSPC that killed file-size before the corpus guard is named instead of hidden behind missing-fixtures:"
else
  bad "26a-preflight-marked: $_pf_marked of 4 pre-flight blocks append the attribution (still-exempt emit_summary FAIL sites: $_pf_exempt)"
fi

# (26b) RUNTIME: a recorded FAILing verdict whose log carries the signature is attributed, and an
# EMPTY recorded set yields NO line at all -- the one part of the old exemption that survives, since
# a vacuous "0 RECOGNISED ... (0/0 PASS)" would be worse than silence.
d="$tmp/c26b"; mkdir -p "$d"
o26b=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  COMPONENTS=(file-size fmt clippy)
  printf 'FAIL 3\n' > "$d/file-size.result"
  echo 'error: No space left on device' > "$d/file-size.log"
  _disk_preflight_meta
  printf 'PAIRS %s\n' "${#DISK_RECORDED_PAIRS[@]}"
  printf 'LINE %s\n' "${DISK_PREFLIGHT_META[0]:-<none>}"
  # (b) NO `.result` AT ALL -- the shape ENOSPC leaves when the verdict write never lands -- with
  # the in-memory channel carrying record_result's own failure. Pairs are EMPTY here, so gating on
  # pairs alone omitted the attribution from exactly the run that had the answer (job 365).
  rm -f "$d/file-size.result"
  DISK_MEM_SUBJECTS=("component verdict write (file-size.result)|bash: line 1: No space left on device")
  _disk_preflight_meta
  printf 'MEMPAIRS %s\n' "${#DISK_RECORDED_PAIRS[@]}"
  printf 'MEMLINE %s\n' "${DISK_PREFLIGHT_META[0]:-<none>}"
  # (c) NO subject of ANY kind -> still no line, so a vacuous 0/0 can never be rendered.
  DISK_MEM_SUBJECTS=(); DISK_UNREAD_VERDICTS=()
  _disk_preflight_meta
  printf 'NONE %s\n' "${#DISK_PREFLIGHT_META[@]}"
  # (d) a MALFORMED `.result` -- the note is recorded while GATHERING, which the first version lost
  # because it gathered inside a command substitution. The log's signature is REMOVED here so the
  # UNMEASURED arm is what is measured: with it present the line correctly renders RECOGNISED (a
  # match outranks, and the unread subject is still declared as "N further subject(s)"), which
  # would have tested the wrong arm.
  printf 'PASS abc\n' > "$d/file-size.result"
  echo 'nothing of interest' > "$d/file-size.log"
  DISK_MEM_SUBJECTS=(); DISK_UNREAD_VERDICTS=()
  _disk_preflight_meta
  printf 'UNREAD %s\n' "${#DISK_UNREAD_VERDICTS[@]}"
  printf 'UNREADLINE %s\n' "${DISK_PREFLIGHT_META[0]:-<none>}"
)
l26b=$(printf '%s\n' "$o26b" | sed -n 's/^LINE //p')
m26b=$(printf '%s\n' "$o26b" | sed -n 's/^MEMLINE //p')
u26b=$(printf '%s\n' "$o26b" | sed -n 's/^UNREADLINE //p')
if case "$o26b" in *"PAIRS 2"*) true ;; *) false ;; esac \
   && case "$l26b" in "disk-exhaustion: RECOGNISED (#3800)"*"component 'file-size'"*"file-size.log:1"*) true ;; *) false ;; esac \
   && case "$o26b" in *"MEMPAIRS 0"*) true ;; *) false ;; esac \
   && case "$m26b" in "disk-exhaustion: RECOGNISED (#3800)"*) true ;; *) false ;; esac \
   && case "$o26b" in *"NONE 0"*) true ;; *) false ;; esac \
   && case "$o26b" in *"UNREAD 1"*) true ;; *) false ;; esac \
   && case "$u26b" in "disk-exhaustion: UNMEASURED (#3800)"*) true ;; *) false ;; esac; then
  ok "26b-preflight-runtime: all four pre-flight shapes -- a recorded FAILing verdict (RECOGNISED at its log line), NO .result with the in-memory channel carrying record_result's own ENOSPC (RECOGNISED with ZERO pairs, the false negative job 365 found), no subject of any kind (no line at all, so no vacuous 0/0), and a MALFORMED .result whose unread note survives the gather (UNMEASURED)"
else
  bad "26b-preflight-runtime: the pre-flight attribution path is wrong:
$o26b"
fi

# (25) roborev job 353 -- MANY REPEATED SIGNATURES, and the in-memory branch runs NO subprocess.
#
# `-m1` stops after the first matching LINE but `-o` prints every OCCURRENCE on it, and the
# in-memory branch had no `-m1` at all (to dodge a measured pipefail/SIGPIPE wrong verdict) so it
# reported every occurrence in the WHOLE payload and accumulated all of it before taking the first
# record. The branch now reads GREP'S OWN status via PIPESTATUS[1] under a subshell-scoped
# `set +o pipefail`, which makes printf's SIGPIPE irrelevant instead of avoided and so lets `-m1`
# -- and the bound -- come back. Both branches are exercised against a payload PACKED with the
# signature, which is the shape the finding named.
#
# A pure-bash spelling of this branch was tried first and REVERTED ON MEASUREMENT: it has no
# capture and cannot SIGPIPE, and `case "$payload" in *"$phrase"*` backtracks once per occurrence,
# so this exact 1.5 MB packed shape took 157 SECONDS (against 165 ms for the same size with no
# match) on the path to the terminal emit. Hence 25b's watchdog.
d="$tmp/c25"; mkdir -p "$d"
# ~40k occurrences on ONE line, plus a decoy first line so the reported number must be 2.
_packed=$(awk 'BEGIN{ s="No space left on device "; t=s; while (length(t) < 900000) t = t t; printf "%s", t }')
{ printf 'first line, no signature\n'; printf '%s\n' "$_packed"; } > "$d/core-tests.log"
out=$(run_line "$d" core-tests FAIL)
if case "$out" in "disk-exhaustion: RECOGNISED (#3800)"*) true ;; *) false ;; esac \
   && case "$out" in *"core-tests.log:2"*) true ;; *) false ;; esac \
   && [ "${#out}" -lt 4000 ]; then
  ok "25a-packed-file: a log line packed with ~40k copies of a signature is RECOGNISED at line 2 and the emitted line stays small (${#out} chars) -- the many-occurrence shape does not reach the block"
else
  bad "25a-packed-file: expected RECOGNISED at line 2 with a small emitted line; len=${#out} got: ${out:0:300}"
fi
# The IN-MEMORY branch on the same shape. This is the one whose bound was genuinely absent, and it
# is also the branch with the measured SIGPIPE history -- so the assertion is the MATCH (a wrong
# verdict there reads as UNMEASURED) plus the line number, on a payload with a leading decoy line.
# Under the probed watchdog (when one is available): the pure-bash spelling of this branch took
# 157 SECONDS on this exact shape, and without a bound that regression would come back as a
# mysteriously slow mandatory gate component rather than as a red. This is a WATCHDOG, not a
# performance threshold -- 60 s is ~200x the measured 312 ms, so it fires only on a change of
# algorithmic class.
# The CHILD builds the in-memory payload from the file, so a 1.5 MB string never has to travel
# through argv or through the script text (the first version interpolated it into the `bash -c`
# body and produced no output at all).
o25b=$(
  ${DISK_TIMEOUT:+$DISK_TIMEOUT 60} bash -c '
    . "$1"; LOG_DIR="$2"
    _big="decoy first line
$(tail -1 "$3")"
    rc=0; _disk_scan_subject text "$_big" || rc=$?
    printf "RC %s SIG %s LN %s\n" "$rc" "$DISK_SCAN_SIG" "$DISK_SCAN_LN"
  ' _ "$EX" "$d" "$d/core-tests.log"
)
if [ "$o25b" = "RC 0 SIG no-space-left-on-device LN 2" ]; then
  ok "25b-packed-memory: the in-memory branch matches the same packed payload and reports the right line (2) -- with -m1 restored (bounding the capture to one line) and grep's OWN status read via PIPESTATUS[1], so printf dying of SIGPIPE no longer turns a match into 'could not read'"
else
  bad "25b-packed-memory: expected 'RC 0 SIG no-space-left-on-device LN 2'; got: $o25b"
fi
# NEGATIVE CONTROL for the de-piped branch: a large payload with NO signature must read rc 1 (a
# genuine no-match), not rc 2 -- the single glob test decides this case, and it must decide it
# correctly rather than merely quickly.
o25c=$(
  . "$EX"; LOG_DIR="$d"; _disk_env
  _clean=$(awk 'BEGIN{ t="harmless "; while (length(t) < 900000) t = t t; printf "%s", t }')
  rc=0; _disk_scan_subject text "$_clean" || rc=$?
  printf 'RC %s\n' "$rc"
)
if [ "$o25c" = "RC 1" ]; then
  ok "25c-control: a large payload with NO signature reads rc 1 (a real no-match), so the fast no-match path is not swallowing errors or claiming matches"
else
  bad "25c-control: expected RC 1 for a signature-free payload; got: $o25c"
fi
# STRUCTURAL: the in-memory branch must read GREP'S OWN status, never the pipeline's. The pipeline
# is deliberately present -- grep does the matching in C because the pure-bash alternative measured
# 157 s on a packed 1.5 MB line -- so what has to be pinned is the two constructs that make the
# pipeline safe: `PIPESTATUS[1]` and the subshell-scoped `set +o pipefail`. Reading `$?` instead
# would restore the measured SIGPIPE wrong verdict, and no runtime case here can see that: it needs
# a specific payload size AND `-m1`, and 25b would still report line 2.
#
# COMMENT LINES ARE EXCLUDED, because the first version of this assert matched its OWN explanatory
# comment (which quotes `printf … | grep`) and reported a defect that was not there -- a structural
# guard whose subject set includes the prose describing it.
_mem_body=$(awk '
  /^_disk_scan_subject\(\) \{/ { inb=1 }
  inb && /IN-MEMORY BRANCH READS/ { ins=1 }
  ins && /^    fi$/ { ins=0 }
  ins && $0 !~ /^[[:space:]]*#/ { print }
  inb && /^\}$/ { inb=0 }
' "$GATE")
_mem_pipestatus=$(printf '%s\n' "$_mem_body" | grep -c 'PIPESTATUS\[1\]' || true)
_mem_nopf=$(printf '%s\n' "$_mem_body" | grep -c 'set +o pipefail' || true)
_mem_bare=$(printf '%s\n' "$_mem_body" | grep -cE '\)"; rc=\$\?' || true)
if [ "$_mem_pipestatus" -ge 1 ] && [ "$_mem_nopf" -ge 1 ]; then
  ok "25d-grep-status: the in-memory branch takes grep's OWN status via PIPESTATUS[1] under a subshell-scoped 'set +o pipefail' -- so printf dying of SIGPIPE is irrelevant rather than avoided, which is what lets -m1 (and the bound) come back"
else
  bad "25d-grep-status: PIPESTATUS[1]=$_mem_pipestatus 'set +o pipefail'=$_mem_nopf (excluding comments) -- without both, the pipeline's status is printf's and a matched signature reads as 'could not read' on a large payload; outer rc-capture sites=$_mem_bare"
fi

# (23) roborev job 343 -- THE SCAN MUST NOT BE ABLE TO HANG THE GATE.
#
# The subject glob accepted any existing, readable `<component>.*.log`, and BOTH `-e` and `-r` are
# true for a FIFO and for a symlink to a character device. `grep` on either BLOCKS FOREVER -- and
# this scan runs on the path to the TERMINAL EMIT, so the gate would produce no verdict at all,
# which is worse than the misattribution the line exists to prevent. Refused rather than skipped:
# a silently-skipped subject is indistinguishable from one read that matched nothing.
# `timeout` IS PROBED, NOT ASSUMED (roborev job 348): stock macOS ships `mkfifo` and NOT GNU
# `timeout` (coreutils installs it as `gtimeout`), and this suite is registered in the MANDATORY
# `tooling-tests` component -- so an unprobed `timeout` would not skip, it would FAIL the gate on a
# supported host. The watchdog is not optional here either: without it a regression turns this case
# from a red into a suite that never returns, which is why the answer is a DECLARED skip rather
# than running the case unbounded.
DISK_TIMEOUT=""
for _t in timeout gtimeout; do
  if command -v "$_t" >/dev/null 2>&1 && "$_t" 5 true >/dev/null 2>&1; then DISK_TIMEOUT="$_t"; break; fi
done
if command -v mkfifo >/dev/null 2>&1 && [ -n "$DISK_TIMEOUT" ]; then
  d="$tmp/c23"; mkdir -p "$d"
  mkfifo "$d/core-tests.log" 2>/dev/null || true
  if [ -p "$d/core-tests.log" ]; then
    # The whole point is that this RETURNS, so it is bounded: a hang shows up as exit 124, not as
    # a suite that never finishes.
    o23=$(
      "$DISK_TIMEOUT" 20 bash -c '
        . "$1"; LOG_DIR="$2"; _disk_env
        printf "LINE %s\n" "$(_disk_exhaustion_line core-tests FAIL)"
      ' _ "$EX" "$d" 2>&1
      printf 'EXIT %s\n' "$?"
    )
    ex23=$(printf '%s\n' "$o23" | sed -n 's/^EXIT //p')
    l23=$(printf '%s\n' "$o23" | sed -n 's/^LINE //p')
    if [ "${ex23:-x}" = 0 ] \
       && case "$l23" in "disk-exhaustion: UNMEASURED (#3800)"*"core-tests.log(not-a-regular-file)"*) true ;; *) false ;; esac \
       && case "$l23" in *"0 RECOGNISED"*) false ;; *) true ;; esac; then
      ok "23a-fifo: a FIFO in the subject glob is REFUSED as a non-regular file and counted toward UNMEASURED naming itself -- the scan returns instead of blocking in grep on the path to the terminal emit, and it does not report the affirmative clean reading over a subject it never read"
    else
      bad "23a-fifo: expected a bounded return with UNMEASURED naming the non-regular subject; exit='${ex23:-<none>}' (124 = it HUNG) line: $l23"
    fi
    # MUTATION CONTROL: without the regular-file guard the same call must HANG, or 23a proves only
    # that the line mentions the component. The guard is removed from a scratch copy of the shipped
    # gate and re-extracted, so the control measures the real code path.
    _fifo_ctl=$(mktemp "$tmp/fifo-ctl.XXXXXX")
    awk '
      /^        if \[ ! -f "\$log" \]; then$/ { skip=4; next }
      skip > 0 { skip--; next }
      { print }
    ' "$GATE" > "$_fifo_ctl"
    if ! grep -q 'not-a-regular-file' "$_fifo_ctl"; then
      _fifo_ex=$(mktemp "$tmp/fifo-ex.XXXXXX")
      awk -v s='^DISK_EXHAUSTION_SIGNATURES=[(]$' -v e='^[)]$' '
        !inb && $0 ~ s { inb=1; print; next } inb { print; if ($0 ~ e) exit }' "$_fifo_ctl" > "$_fifo_ex"
      grep -m1 '^DISK_MEM_SUBJECTS=()$' "$_fifo_ctl" >> "$_fifo_ex"
      grep -m1 '^DISK_UNREAD_VERDICTS=()$' "$_fifo_ctl" >> "$_fifo_ex"
      for fn in _disk_safe _disk_abbrev _disk_df_probe _disk_gib _disk_free_leg _disk_free_field \
                _disk_scan_field _disk_note_capture_failure _disk_note_unread_verdict \
                _disk_secs_is_int _disk_verdict_read _disk_verdict_read_aggregate \
                _disk_scan_subject _disk_exhaustion_line; do
        awk -v s="^${fn}[(][)] [{]\$" -v e='^[}]$' '
          !inb && $0 ~ s { inb=1; print; next } inb { print; if ($0 ~ e) exit }' "$_fifo_ctl" >> "$_fifo_ex"
      done
      printf '_disk_env() { :; }\n' >> "$_fifo_ex"
      "$DISK_TIMEOUT" 10 bash -c '. "$1"; LOG_DIR="$2"; _disk_exhaustion_line core-tests FAIL >/dev/null 2>&1' _ "$_fifo_ex" "$d" >/dev/null 2>&1
      _fifo_rc=$?
      if [ "$_fifo_rc" = 124 ]; then
        ok "23a-mutation: with the regular-file guard REMOVED the identical call HANGS (killed at 10s) -- so 23a is measuring the guard and not merely the presence of the component's name in the line"
      else
        bad "23a-mutation: without the guard the call returned (rc $_fifo_rc) instead of hanging, so 23a does not demonstrate the hang it claims to prevent"
      fi
    else
      bad "23a-mutation: could not build the control (the regular-file guard survived the removal), so the hang cannot be shown"
    fi
  else
    printf 'SKIP - 23a-fifo: mkfifo did not produce a FIFO on this filesystem, so a blocking subject cannot be induced. DECLARED, not silently omitted.\n'
  fi
else
  printf 'SKIP - 23a-fifo: this host lacks mkfifo and/or a working timeout|gtimeout (stock macOS has the first and not the second), so a blocking subject cannot be induced under a watchdog. DECLARED, not silently omitted.\n'
fi

# (21c) THE LADDER'S LOWER RUNGS -- roborev job 319 round 2. The first version of 21b's fallback
# ended `|| true`, which is the shape it was written to remove: if BOTH the marker append and the
# truncation fail, the original well-formed PASS survives and the parent certifies. The 21b
# fixture cannot reach this because its RESULT FILE sits on a healthy filesystem. Here the verdict
# is made untruncatable (mode 444) inside an unwritable DIRECTORY (so unlink fails too), which is
# a host-independent way to fail rungs 2 and 3 without needing a full filesystem.
#
# Rung 4 SIGTERMs the gate, so this case must run in its OWN process or it would kill the suite.
# The assertion is the process DYING BY SIGTERM (128+15) -- i.e. the run publishes no verdict at
# all, which doctrine never reads as a certification -- plus the diagnostic naming the state.
d="$tmp/c21c"; mkdir -p "$d/logs"
printf 'PASS 611\n' > "$d/logs/legacy-heuristics.result"
ln -s /dev/full "$d/logs/tree-integrity.fail" 2>/dev/null || true
chmod 444 "$d/logs/legacy-heuristics.result"
chmod 555 "$d/logs"
if [ -c /dev/full ] && ! : 2>/dev/null > "$d/logs/legacy-heuristics.result" && ! rm -f "$d/logs/legacy-heuristics.result" 2>/dev/null; then
  o21c=$(
    bash -c '
      . "$1"; LOG_DIR="$2"; _disk_env
      ( _tree_boundary_fail legacy-heuristics "tree-capture-failed; the tree cannot be proven unchanged" capture-failed ) 2>&1 |
        sed -n -e "s/.*\(TRUNCATION also failed\).*/D1 \1/p" -e "s/.*\(GATE is being terminated\).*/D2 \1/p"
      printf "ALIVE\n"
    ' _ "$EX" "$d/logs" 2>&1
    printf 'EXIT %s\n' "$?"
  )
  chmod 755 "$d/logs" 2>/dev/null || true
  ex21c=$(printf '%s\n' "$o21c" | sed -n 's/^EXIT //p')
  if [ "${ex21c:-0}" = 137 ] \
     && case "$o21c" in *"D1 TRUNCATION also failed"*) true ;; *) false ;; esac \
     && case "$o21c" in *"D2 GATE is being terminated"*) true ;; *) false ;; esac \
     && case "$o21c" in *ALIVE*) false ;; *) true ;; esac; then
    ok "21c-ladder: when the marker append, the truncation AND the unlink all fail, the SIDE lane names each escalation and SIGKILLs the gate (exit 137, and the shell never reaches its next statement) -- so the run publishes no verdict rather than certifying the surviving PASS"
  else
    bad "21c-ladder: the lower rungs did not fire (exit='${ex21c:-<none>}'; expected 137 with both diagnostics and no ALIVE):
$o21c"
  fi
  # (21d) THE SAME OUTCOME WITH SIGTERM IGNORED -- roborev job 319 rounds 4 and 5.
  #
  # Round 4's finding was that a TERM-only rung is survivable (an ignored TERM disposition
  # inherited by the gate's shell survives into bash and cannot be un-ignored, and `kill` returns 0
  # on mere delivery). Round 5's was that the TERM-then-sleep-then-KILL sequence written for it
  # REOPENED the pid-reuse hazard: after the first signal the gate may exit and be reaped during
  # the sleep, and on a four-lane box the pid's next owner is most likely a peer lane's gate.
  #
  # Since the rung now sends ONE SIGKILL, this case's runtime half is nearly a duplicate of 21c
  # -- making an outcome uniform is exactly how one case quietly becomes a copy of another that
  # passes without entering the state it was written for. So it keeps the runtime assertion (an
  # ignored TERM changes nothing) and adds the STRUCTURAL half that 21c cannot express: the rung
  # must contain no first signal and no sleep, because re-adding either re-adds the reuse window.
  chmod 444 "$d/logs/legacy-heuristics.result" 2>/dev/null || true
  chmod 555 "$d/logs" 2>/dev/null || true
  o21d=$(
    bash -c '
      trap "" TERM
      . "$1"; LOG_DIR="$2"; _disk_env
      ( _tree_boundary_fail legacy-heuristics "tree-capture-failed" capture-failed ) >/dev/null 2>&1
      printf "SURVIVED\n"
    ' _ "$EX" "$d/logs" 2>&1
    printf 'EXIT %s\n' "$?"
  )
  chmod 755 "$d/logs" 2>/dev/null || true
  ex21d=$(printf '%s\n' "$o21d" | sed -n 's/^EXIT //p')
  if [ "${ex21d:-0}" = 137 ] && case "$o21d" in *SURVIVED*) false ;; *) true ;; esac; then
    ok "21d-unignorable: with SIGTERM IGNORED the outcome is UNCHANGED -- the gate still dies (exit 137) and the shell never reaches its next statement, so a 'kill' that returns 0 can no longer be mistaken for the target having died"
  else
    bad "21d-unignorable: a gate ignoring SIGTERM SURVIVED rung 4 (exit='${ex21d:-<none>}'), so the original well-formed PASS would stand:
$o21d"
  fi
  # STRUCTURAL: exactly ONE signal to $$ in the SIDE branch, and it is KILL. A first signal (TERM)
  # followed by a wait is what reopened the pid-reuse hazard in round 5: once the gate is signalled
  # it may exit, be reaped, and its pid reassigned -- most likely to a peer lane's gate. 21c/21d
  # cannot see this, because a re-added TERM+sleep+KILL would still exit 137 and pass both.
  _rung_body=$(awk '
    /^_tree_boundary_fail\(\) \{/ { inb=1 }
    inb && /BASHPID/ { ins=1 }
    ins && /^  fi$/ { ins=0 }
    ins { print }
    inb && /^\}$/ { inb=0 }
  ' "$GATE")
  _sig_kill=$(printf '%s\n' "$_rung_body" | grep -c 'kill -KILL "\$\$"' || true)
  _sig_other=$(printf '%s\n' "$_rung_body" | grep -cE 'kill -(TERM|INT|HUP|QUIT) "\$\$"' || true)
  _sig_sleep=$(printf '%s\n' "$_rung_body" | grep -cE '^[[:space:]]*sleep ' || true)
  if [ "$_sig_kill" = 1 ] && [ "$_sig_other" = 0 ] && [ "$_sig_sleep" = 0 ]; then
    ok "21d-one-signal: the SIDE branch signals \$\$ exactly once and with KILL (no TERM, no sleep) -- so there is no window in which the gate can be reaped and its pid reassigned to a peer lane's gate before a second signal lands"
  else
    bad "21d-one-signal: kill-KILL=$_sig_kill other-signals=$_sig_other sleeps=$_sig_sleep -- a first signal followed by a wait reopens the pid-reuse hazard round 5 found, and 21c/21d would both still pass with it"
  fi
  # POSITIVE CONTROL for that guard: plant the exact TERM+sleep+KILL sequence round 5 rejected and
  # require the extraction to SEE it. A structural assert whose awk silently matched nothing would
  # otherwise report clean forever -- and here the region is delimited by `BASHPID` and a bare
  # `  fi`, both of which an unrelated edit can move.
  _ctl_gate=$(mktemp "$tmp/one-signal-ctl.XXXXXX")
  awk '
    { print }
    /kill -KILL "\$\$" 2>\/dev\/null \|\| true/ && !d {
      print "        kill -TERM \"$$\" 2>/dev/null || true"; print "        sleep 1"; d=1 }
  ' "$GATE" > "$_ctl_gate"
  _ctl_body=$(awk '
    /^_tree_boundary_fail\(\) \{/ { inb=1 }
    inb && /BASHPID/ { ins=1 }
    ins && /^  fi$/ { ins=0 }
    ins { print }
    inb && /^\}$/ { inb=0 }
  ' "$_ctl_gate")
  _ctl_other=$(printf '%s\n' "$_ctl_body" | grep -cE 'kill -(TERM|INT|HUP|QUIT) "\$\$"' || true)
  _ctl_sleep=$(printf '%s\n' "$_ctl_body" | grep -cE '^[[:space:]]*sleep ' || true)
  if [ "$_ctl_other" -ge 1 ] && [ "$_ctl_sleep" -ge 1 ]; then
    ok "21d-one-signal-control: the planted TERM+sleep+KILL sequence IS seen by the same extraction (other-signals=$_ctl_other sleeps=$_ctl_sleep), so the clean reading above is a measurement and not an empty match"
  else
    bad "21d-one-signal-control: the planted sequence was NOT seen (other-signals=$_ctl_other sleeps=$_ctl_sleep) -- the one-signal guard's region extraction is blind and its green says nothing"
  fi
else
  chmod 755 "$d/logs" 2>/dev/null || true
  printf 'SKIP - 21c-ladder + 21d-unignorable: this host could not be made to refuse BOTH a truncate and an unlink (running as root, or a permissive filesystem), so rungs 3-4 cannot be induced. DECLARED, not silently omitted.\n'
fi

# +4 (roborev job 319: the two false-PASS routes that survived round 5. 21a the terminator and
# trailing-content contract, plus the mutation showing the pre-fix read ADOPTS all three
# truncated verdicts (incl. `PASS 1`, a valid integer that is the wrong number): 2. 21b the
# SIDE-lane marker channel under a REAL /dev/full ENOSPC -- the zero-allocation verdict
# invalidation -- and the negative control proving a HEALTHY marker write leaves the verdict
# alone: 2, DECLARED as a skip where /dev/full is unavailable, so the floor takes the lower
# count and holds on macOS: 2 + 0. Floor rises by 2, not 4. 21c -- the ladder's lower rungs,
# added in round 2 of the same job when the `|| true` on the truncation turned out to be the very
# shape 21b was written to remove -- also DECLARES its skip (it needs a host that refuses both a
# truncate and an unlink), so it does not raise the floor either.); +3 (roborev job 319 round 3:
# a component can VANISH from a certification. 22a the two opposed halves of file-size's
# selection (recorded for the presence guard, still not dispatched); 22b the lite path through the
# SHIPPED aggregator in BOTH directions -- selected-and-absent FAILS, --only-absent still passes
# -- plus the mutation certifying the same directory with two components missing from the table;
# 22c record_result setting OVERALL=FAIL under a REAL /dev/full ENOSPC, which DECLARES its skip
# elsewhere and so does not raise the floor. 1 + 2 = 3.); +1 (the case-15 positive control added
# with them, when re-deriving that guard's population from source made its own vacuity reachable.);
# +2 (roborev job 343: the regular-file guard -- a FIFO or a device in the subject glob would make
# grep BLOCK FOREVER on the path to the terminal emit. Case 23a plus the mutation control that
# demonstrates the hang with the guard removed; both DECLARE a skip without mkfifo, so they do not
# raise the floor. What DOES raise it is 8b2 + its control: refusing non-regular subjects made
# case 8b's directory unable to reach grep, so the three-valued-rc arm needed its own case with an
# INJECTED erroring grep on a regular file, plus a control proving it distinguishes rc 2 from
# rc 1.); +1 (roborev job 348: 24a pins the BOUNDED capture (`-o`) against a 2 MB matching line,
# asserting the line number and the emitted size together. 24b pins that a failed OPEN is rc 2 and
# not rc 1 -- it DECLARES a skip as root, so it does not raise the floor. The `timeout` probe added
# in the same round raises nothing; it converts a would-be macOS FAILURE into a declared skip.);
# +4 (roborev job 353: the many-occurrence shape. 25a the FILE branch on a line packed with ~40k
# copies; 25b the same shape through the now pure-bash IN-MEMORY branch, whose bound was the one
# genuinely absent and which also carries the measured SIGPIPE history; 25c the large no-signature
# control, so the fast no-match path is shown to decide correctly and not merely quickly; 25d the
# structural assert that the branch reads GREP'S OWN status via PIPESTATUS[1] under a
# subshell-scoped 'set +o pipefail', which no runtime case here can see.); +3 (roborev job 358: the
# pre-flight blocks are emitted AFTER run_file_size, so their "no component has run" exemption was
# false. 26a asserts the ORDERING from source -- the fourth time in this issue that an exemption
# rested on a wrong claim about what could already have run -- and that all 3 sites are census-
# derived MARKED; 26b drives the attribution over a recorded verdict and pins that an EMPTY
# recorded set still yields no line.); +2 (roborev job 370: the FOURTH post-run_file_size site --
# the `--only` zero-Data.db block, reachable via `--only file-size,core-tests` -- kept the same
# false exemption after three INSTANCES were fixed. Case 27 derives the CLASS from the call graph
# (top-level lines after the run_file_size call, plus functions called from them) and FAILs on any
# exempt member, with a control that strips one marking and requires the check to see it.);
# +0 (roborev job 319 rounds 4-5 added 21d, whose runtime half shares 21c's DECLARED skip; its
# STRUCTURAL half needs no host capability but is counted at 0 to keep the floor host-independent.)
CASE_FLOOR=94
printf '\n%s\n' "----------------------------------------"
if [ $((PASS + FAIL)) -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case-floor: %d cases ran but this suite declares a floor of %d -- cases were REMOVED or are dying silently.\n' \
    "$((PASS + FAIL))" "$CASE_FLOOR"
  FAIL=$((FAIL + 1))
fi
printf 'passed: %d  failed: %d  (floor %d)\n' "$PASS" "$FAIL" "$CASE_FLOOR"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
