#!/usr/bin/env bash
# lib-perf-lint.sh — THE perf-invocation guard for the WS0 measurement rig
# (issue #3096 spec R2 trap 1, hardened by #3272 item 10 and review round 1).
#
# Sourced, not executed. It sets NO shell options: `set -euo pipefail` in a sourced
# library mutates the SOURCING shell's options, which is a caller's decision to make.
#
# Split into its own file under the campsite rule. It is also the honest boundary: the
# lint is a self-contained CHECKER over a shell source file, with one entry point and
# no dependency on the driver's state — and moving it out means the awk no longer has
# to skip its OWN function body by position, since the file it lints (the driver) no
# longer contains it.

# --- trap 1: the rig contains no per-process perf invocation -------------------
# Spec R2: the driver "contains no per-process perf invocation". Per-process counting
# measured >2x observer cost on this workload, so CPU-wide counting is mandatory and
# a future edit that reaches for the per-process option must not be able to RUN.
#
# # Why this stopped being a deny-list grep (issue #3272, item 10, then review round 1)
#
# The first version was `perf stat[^|]*(-p |--pid)` filtered through
# `grep -v 'self-check'`. Driving it over injected invocations found two bypasses:
# an ATTACHED value (the `-p ` alternative required a trailing SPACE) and ANY LINE
# MENTIONING "self-check" (the `grep -v` discarded by CONTENT, so a comment on a real
# invocation suppressed the guard). Item 10 fixed both — and review round 1 then found
# THREE MORE, all in the fixed pattern, all ordinary bash:
#
#   * a SINGLE-QUOTED attached value: the attached-value class was `[0-9"$]`, which
#     omits `'`, so a genuinely per-process invocation written with single quotes was
#     invisible to it. MEASURED against the item-10 pattern: no match.
#   * an invocation through a VARIABLE, `"$SOME_BIN" stat …`: the pattern anchored on
#     the literal word `perf` immediately before `stat`. MEASURED: no match.
#   * a GLOBAL OPTION between the two words, `perf --no-pager stat …`: the same
#     adjacency anchor. MEASURED: no match.
#
# Three bypasses in the fix for two bypasses is the argument itself. **A deny-list
# over source TEXT must anticipate every spelling of the thing it forbids, and it is
# wrong the moment it misses one — silently, in the permissive direction.** There is no
# amount of pattern care that converts that into a guarantee, because the space of
# spellings is open: a new perf option, a new shell idiom, an `eval`.
#
# So the mechanism is INVERTED into an ALLOWLIST, which is closed by construction:
#
#   LAYER 1 (STRUCTURAL, source): `perf_stat_c` is the ONLY place the linted file
#     invokes `perf`. Every non-comment line whose tokens include a bare `perf`/`stat`
#     command word must either be inside that function's body or carry an explicit
#     `perf-lint-allow` marker (a diagnostic string, a presence probe). A NEW
#     invocation anywhere else fails this regardless of how it is spelled — through a
#     variable, with global options, with any quoting — because the check does not ask
#     what the line looks like, it asks WHERE IT IS. And an affirmative half: the
#     wrapper's own invocation must carry `-C`, so the allowlist cannot be satisfied by
#     a wrapper that counts nothing CPU-wide.
#   LAYER 2 (TOKEN, source): no such line — marked or not, inside the wrapper or not —
#     may carry an option token that makes the invocation per-process. Applied per
#     WHITESPACE-SEPARATED TOKEN rather than by substring, so attached and separated
#     values are one case and quoting is irrelevant. This is what fires when the edit
#     is made INSIDE the wrapper, where layer 1 has nothing to say.
#   LAYER 3 (RUNTIME, argv): `perf_stat_c` inspects the argv it is about to pass. By
#     then bash has done word-splitting and QUOTE REMOVAL, so `-p'1234'` and `-p1234`
#     and `-p "$x"` are the same tokens — the spelling problem does not exist at this
#     layer at all. It is the backstop for a caller that reaches the wrapper with
#     arguments no source scan saw (a computed option, an `eval`).
#
# The option spellings are held in VARIABLES so the diagnostics can name them without
# layer 2 matching its own message — the self-match problem removed rather than filtered
# around, which is what the discarded `grep -v` was trying and failing to do.
_PP_SHORT='-p'
_PP_LONG='--pid'

# perf_invocation_lint <file> — print one `<lineno>: <reason>` per violation, and
# nothing when the file is clean. Exit status is not used; the CALLER counts output,
# so an awk that dies mid-file cannot read as "clean" (it would print nothing AND the
# caller's own affirmative check for the wrapper would fail).
perf_invocation_lint() {
  awk -v pp_short="$_PP_SHORT" -v pp_long="$_PP_LONG" '
    # A token as the SHELL would see it after word-splitting and quote removal:
    # leading/trailing quotes, parens and `;` stripped. That is what makes the
    # spelling of an invocation irrelevant — `"perf"`, `perf`, `(perf` and `perf;`
    # reduce to the same token.
    function bare(t) { gsub(/^[("'\''`]+/, "", t); gsub(/[)"'\''`;]+$/, "", t); return t }
    # Does this line INVOKE the tool? A line invokes it when some token IS the
    # command word (`perf`, or any path ending `/perf`, or the subcommand `stat`).
    # This is deliberately NOT a substring test: `perf_stat_c`, `perf_event_paranoid`
    # and `target/perf-ws0-3096` are identifiers and paths, not invocations, and a
    # substring test on them is what makes a text guard noisy enough to be deleted.
    # Matching the SUBCOMMAND too is what closes review round 1s
    # variable-and-global-option bypasses: `"$BIN" stat …` and `perf --no-pager stat …`
    # both carry a bare `stat` token however the tool itself is spelled.
    function invokes(line,   n, i, t) {
      n = split(line, tk, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        t = bare(tk[i])
        if (t == "perf" || t ~ /\/perf$/ || t == "stat") return 1
      }
      return 0
    }
    /^[[:space:]]*#/ { next }                                   # full-line comment
    # The lint FUNCTION ITSELF is skipped STRUCTURALLY, by position, for the case where
    # this library is pointed at ITSELF (a caller may lint the whole scripts/perf tree).
    # A CONTENT-based exclusion is what the discarded `grep -v self-check` did, and its
    # bypass was a code comment; position cannot be spelled around.
    /^perf_invocation_lint\(\)/ { inlint = 1; next }
    inlint && /^\}/  { inlint = 0; next }
    inlint           { next }
    /^perf_stat_c\(\)/ { inwrap = 1; next }
    inwrap && /^\}/    { inwrap = 0; wrapseen = 1; next }
    {
      mentions = invokes($0)
      marked   = index($0, "perf-lint-allow") > 0
      if (inwrap) {
        if (mentions) { wrapinvoke++; if (index($0, "-C")) wrapcpuwide++ }
      } else if (mentions && !marked) {
        print NR ": perf/stat invocation outside the single perf_stat_c wrapper, unmarked"
      }
      # LAYER 2 applies to a marked line TOO — the marker exempts a line from the
      # allowlist (a diagnostic string may name the tool), never from the option check.
      if (!mentions) next
      n = split($0, tok, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        t = bare(tok[i])
        # An ATTACHED value is part of the token; a SEPARATED one is the next token.
        # Either way the token STARTS WITH the option, so one test covers `-p1234`,
        # `-p 1234`, `-p"1234"`, `-p$x`, `--pid=1234` and `--pid 1234` alike.
        if (index(t, pp_short) == 1 || index(t, pp_long) == 1) {
          print NR ": per-process option token `" tok[i] "` on a perf/stat line"
        }
      }
    }
    END {
      if (!wrapseen)     print "0: perf_stat_c() is absent — there is no single wrapper to allow"
      if (!wrapinvoke)   print "0: perf_stat_c() invokes nothing — the allowlist would be vacuous"
      if (!wrapcpuwide)  print "0: perf_stat_c() does not pass -C — the wrapper counts nothing CPU-wide"
    }
  ' "$1"
}
