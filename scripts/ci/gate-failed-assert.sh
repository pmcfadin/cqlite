#!/usr/bin/env bash
# gate-failed-assert.sh — extract the FAILING ASSERT IDENTITY from one already-
# ANSI-normalised agent-gate component log (issue #3765).
#
# WHY THIS EXISTS. A failing component's SUMMARY line used to name only the #3453
# invocation annotation:
#     tooling-tests:  FAIL (1149s)  [test ws0-corpus-gen default-features | + …]
# The bracket is test-SHAPED, so a reader identifies `ws0-corpus-gen` as the failing
# test — which is wrong; the real assert (`FAIL - 1465-skip-declares: …`) lived only in
# the component log. CLAUDE.md forbids reading gate.log ("the SUMMARY block is the ONLY
# gate text an agent retains") while simultaneously requiring a flake citation to match
# the ASSERT rather than the component, so the two rules were unsatisfiable for a FAIL.
#
# CONTRACT
#   usage: gate-failed-assert.sh <ansi-normalised-log> [max-names]
#   stdout (line-oriented, in this order; absent when nothing was recognised):
#       tier=<assert|guard|toolchain>
#       count=<N>                 total DISTINCT identities recognised (never capped).
#                                 DISTINCT is judged on the FULL normalised identity, so
#                                 the display cap below can never change the count.
#       name=<identity>           at most <max-names> lines (default 10), in file order,
#                                 each DISPLAY-CAPPED at 60 chars (57 + `...`)
#   exit 0 — the log was read and scanned (whether or not anything matched)
#   exit 2 — usage error / the log could not be read: the CALLER must render this as a
#            named `not extractable`, never as "no failures found". A guard that could
#            not read its subject has measured nothing (CLAUDE.md: a positive verdict
#            requires an affirmative measurement).
#
# THE CALLER OWNS ANSI NORMALISATION. agent-gate.sh routes the log through
# `_ansi_stripped_log` and passes the STRIPPED path, so there is exactly ONE stripper in
# the repo (#3400: 18 workflows set CARGO_TERM_COLOR=always and colour SURVIVES
# redirection to a file, so a parser keyed on `<status> <payload>` matches NOTHING). A
# second copy here would be a second thing to drift. Every read below is a REDIRECTION
# into awk, never a pipe (#3400: a piped `while read` runs in a subshell and its verdict
# is discarded).
#
# ===== THE RECOGNISER TABLE — one place, one stated rule per entry =====
# Three TIERS, most specific first; the FIRST tier that matches at least once wins, so a
# cargo `error: test failed…` epilogue never displaces the test names above it.
#
#   TIER `assert` — a named test case / assertion identity.
#     A1  `FAIL - <identity>`           the repo's bash test-suite convention: the
#                                       `bad()` helper in scripts/tests/*.sh, which the
#                                       tooling-tests component executes (~16 suites).
#                                       MEASURED: this is the #3765 subject line shape.
#                                       The identity is the WHOLE payload, detail
#                                       included — NOT the tag before the first `:`. A
#                                       tag is routinely SHARED (13 `FAIL - F2: …` lines
#                                       in one measured suite), so tag-only dedup
#                                       UNDERCOUNTS and names nothing.
#     A2  `FAIL: <detail>` (col 0)      the other bash-suite spelling (8 measured sites,
#                                       e.g. "FAIL: guard did NOT trip on …"). Anchored
#                                       at column zero so `>>> [x] FAIL: …` (a gate
#                                       progress line) and `oom-audit: FAIL (…)` (tier
#                                       `guard`) are not swallowed here.
#     A3  `test <name> ... FAILED`      libtest's per-test line. Also the source of the
#                                       `failures:` block below it, which is therefore
#                                       NOT a separate rule (it would double-count).
#     A4  `FAIL [ <t>s] <pkg> <test>`   cargo-nextest's per-test line.
#     A5  `shell-selftest: <f> FAIL`    _run_shell_selftest_files' per-file verdict.
#
#   TIER `guard` — a named guard/component verdict, when no test-case name is available.
#     B1  `<label>: <VERDICT> …`        VERDICT ∈ FAIL-CLOSED|FAIL|REFUSED|STALE.
#                                       MEASURED on arrow-parity-guard ("…: FAIL — 0
#                                       tests ran"), oom-audit ("…: FAIL (enforce)"),
#                                       parity-report ("report: STALE — …") and
#                                       cli-tests ("cli-tests Pass 1 (default):
#                                       FAIL-CLOSED — …").
#     B2  `[<component>] FAIL[-CLOSED]` the bracketed spelling. MEASURED on
#                                       flight-tests, legacy-heuristics,
#                                       binding-rust-tests, all-features-check.
#     B3  `[FAIL] <detail>`             the smoke script's convention (its own [FAIL]/
#                                       [PASS] prefixes). MEASURED on smoke, where the
#                                       whole line IS the identity.
#
#   TIER `toolchain` — no assert identity exists; report the defect line itself.
#     C1  `error[E….]: …` / `error: …`  rustc/cargo/clippy. First-N in FILE ORDER, so
#                                       the real diagnostic precedes cargo's own
#                                       `error: could not compile …` epilogue with no
#                                       exclusion list to curate.
#     C2  `npm error <msg>`             node-bindings. MEASURED (npm ci ENOENT).
#     C3  `bash: <msg>`                 a missing/unexecutable child script. MEASURED
#                                       (roborev-lints, 18 logs).
#     C4  `Diff in <path> at line <n>`  rustfmt --check's diff header. DECLARED
#                                       UNMEASURED: all 16 fmt FAILs in the corpus were
#                                       a cargo-fmt USAGE error (a #3544 fixture with no
#                                       Cargo.toml), never a formatting diff, so this
#                                       one entry is derived from rustfmt's documented
#                                       output rather than observed here.
#     C5  `Error: <msg>`                python/maturin's capitalised spelling. MEASURED
#                                       on python-bindings ("Error: [Errno 28] …").
#
# NON-EXHAUSTIVE BY CONSTRUCTION. This set was derived by MEASUREMENT over the 174 FAILed
# component logs present in this box's ~4200 retained agent-gate run directories (29
# distinct components), not predicted from source. A shape it does not know produces
# `count=0` and the caller renders `0 RECOGNISED`, never a bare `0` and never silence.
set -uo pipefail

log=${1:-}
max=${2:-10}

case "$max" in ''|*[!0-9]*) max=10 ;; esac
[ "$max" -ge 1 ] 2>/dev/null || max=10

if [ -z "$log" ] || [ ! -f "$log" ] || [ ! -r "$log" ]; then
  echo "gate-failed-assert: cannot read log '${log:-<none>}'" >&2
  exit 2
fi

awk -v max="$max" '
  # norm() is the IDENTITY: one line, no control characters, whitespace collapsed. It
  # does NOT truncate. disp() is the DISPLAY form, and it is derived from norm().
  #
  # THE TWO ARE SEPARATE BECAUSE DEDUP AND DISPLAY ARE DIFFERENT QUESTIONS. `add()` used
  # to truncate FIRST and key `seen[]` on the truncated text, so two DISTINCT identities
  # sharing a 57-character prefix — routine for a Rust test path, e.g.
  # `…::bti::rows::tests::verify_root_base_prefix_{alpha,beta}` — collapsed to ONE and
  # `count` UNDERCOUNTED. The count is the field this whole extractor exists to make
  # trustworthy (the #3765 flake signature is the assert name AND the accounted count),
  # so a display bound must never be able to change it. Dedup on the FULL identity;
  # truncate only what is printed.
  function norm(s,   t) {
    t = s
    gsub(/[\001-\037\177]/, " ", t)          # control chars (a path CAN hold a newline)
    gsub(/[[:space:]]+/, " ", t)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", t)
    return t
  }
  # 60 is the per-NAME display cap of THIS extractor. It is a SEPARATE bound from the
  # 300-char `_failassert_clean` cap the gate applies to the whole rendered field; do not
  # collapse the two.
  #
  # THE ELISION IS IN THE MIDDLE, not at the tail, and that is load-bearing. A Rust test
  # identity is a MODULE PATH: the distinguishing token is the LAST one, and the first 57
  # characters of two sibling tests are routinely identical. A tail elision therefore
  # printed two DIFFERENT failing tests as the same visible string — re-creating, inside
  # the field, the very misidentification #3765 exists to remove. Head 27 + `...` +
  # tail 30 = 60, so the cap and the `...` marker are unchanged.
  #
  # A DISPLAY COLLISION IS STILL POSSIBLE (two identities differing only in an elided
  # middle) and is harmless BY CONSTRUCTION: `count` is computed from the FULL identity
  # in add(), so the count stays true whatever the display does. The count is the
  # authority; a name is a pointer.
  function disp(t) {
    if (length(t) > 60) t = substr(t, 1, 27) "..." substr(t, length(t) - 29)
    return t
  }
  function add(tier, id,   c) {
    id = norm(id)
    if (id == "") return
    if ((tier SUBSEP id) in seen) return
    seen[tier SUBSEP id] = 1
    c = ++n[tier]
    if (c <= max) hit[tier SUBSEP c] = disp(id)
  }
  # ---- TIER assert ----
  # The A1 identity is the WHOLE payload after `FAIL - `, detail INCLUDED. It used to be
  # `head(s)` — everything before the first `:` — on the assumption that the leading tag
  # IS the test name. MEASURED FALSIFICATION (roborev job 45): scripts/tests/
  # test_ws0_round_metadata.sh emits THIRTEEN `FAIL - F2: <different assertion>` lines, so
  # the tag is a CATEGORY shared by many distinct asserts. Truncating first collapsed them
  # to one `seen[]` key, and the field reported `1 RECOGNISED (assert): F2` — an
  # UNDERCOUNT naming something that identifies neither failure. That is the same
  # truncate-before-dedup defect add() was already fixed for, one level up: the fix there
  # moved the DISPLAY cap after the dedup, and this one was applied OUTSIDE add(), so
  # add() never saw the full payload. Dedup on the FULL identity; the middle elision in disp()
  # keeps the leading tag AND the distinguishing tail visible.
  /^[[:space:]]*FAIL - / {
    s = $0; sub(/^[[:space:]]*FAIL - /, "", s); add("assert", s); next
  }
  /^FAIL: / { s = $0; sub(/^FAIL: /, "", s); add("assert", s); next }
  /^[[:space:]]*test .* \.\.\. FAILED/ {
    s = $0; sub(/^[[:space:]]*test /, "", s); sub(/ \.\.\. FAILED.*$/, "", s)
    add("assert", s); next
  }
  /^[[:space:]]*FAIL \[/ {
    s = $0; sub(/^[[:space:]]*FAIL \[[^]]*\][[:space:]]*/, "", s); add("assert", s); next
  }
  /^shell-selftest: .* FAIL$/ {
    s = $0; sub(/^shell-selftest: /, "", s); sub(/ FAIL$/, "", s); add("assert", s); next
  }

  # ---- TIER guard ----
  # CHECKED FOR THE A1 DEFECT AND DELIBERATELY UNCHANGED (roborev job 45). B1/B2 key on
  # `<label> (<VERDICT>)` and drop the trailing prose, which LOOKS like the same
  # truncate-before-dedup shape and is not: what A1 discarded was a NAME (a shared tag
  # standing in for many distinct asserts), whereas what B1/B2 discard is an EXPLANATION
  # of one verdict by one named guard — two lines with the same label and the same verdict
  # ARE one guard verdict, and folding the prose in would inflate the count with restated
  # detail. A2/A3/A4/A5 and B3 already pass their FULL payload to add(). The residual is
  # DECLARED: a guard that emits the same label+verdict for two genuinely different
  # failures counts as one.
  /^\[FAIL\][[:space:]]/ {
    s = $0; sub(/^\[FAIL\][[:space:]]+/, "", s); add("guard", s); next
  }
  /^\[[^]]+\] FAIL(-CLOSED)?[: ]/ {
    s = $0; v = "FAIL"
    if (s ~ /^\[[^]]+\] FAIL-CLOSED/) v = "FAIL-CLOSED"
    sub(/^\[/, "", s); sub(/\].*$/, "", s)
    add("guard", s " (" v ")"); next
  }
  /^[^[:space:]].*: (FAIL-CLOSED|FAIL|REFUSED|STALE)([^A-Za-z0-9-]|$)/ {
    s = $0
    if (match(s, /: (FAIL-CLOSED|FAIL|REFUSED|STALE)([^A-Za-z0-9-]|$)/)) {
      lbl = substr(s, 1, RSTART - 1)
      v = substr(s, RSTART + 2, RLENGTH - 2)
      gsub(/[^A-Za-z-]/, "", v)
      if (length(lbl) <= 70) add("guard", lbl " (" v ")")
    }
    next
  }

  # ---- TIER toolchain ----
  /^error(\[[A-Za-z0-9]+\])?: / { add("toolchain", $0); next }
  /^npm error / { add("toolchain", $0); next }
  /^Error: / { add("toolchain", $0); next }
  /^bash: / { add("toolchain", $0); next }
  /^Diff in .* at line [0-9]+:/ {
    s = $0; sub(/^Diff in /, "", s); sub(/:$/, "", s); add("toolchain", "rustfmt diff in " s); next
  }

  END {
    split("assert guard toolchain", order, " ")
    for (i = 1; i <= 3; i++) {
      t = order[i]
      if (!(t in n)) continue
      printf "tier=%s\n", t
      printf "count=%d\n", n[t]
      lim = (n[t] < max ? n[t] : max)
      for (c = 1; c <= lim; c++) printf "name=%s\n", hit[t SUBSEP c]
      exit 0
    }
  }
' < "$log"
