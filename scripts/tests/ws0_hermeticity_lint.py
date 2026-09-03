#!/usr/bin/env python3
"""The WS0 hermeticity lint: no self-test may invoke the measurement driver bare.

Two subcommands, and they are deliberately separate because they answer two DIFFERENT
questions that round 4 of #3272 found conflated:

    lint <file>…      does any of these files invoke the driver outside `ws0_driver_run`?
    subject <root>     WHICH files is the lint's subject, and is that subject COMPLETE?

`lint` prints `<file>:<lineno>: <reason>` per finding and nothing when clean. `subject`
prints one `<status>\\t<path>` per line. Both exit 0 on success and non-zero only on a usage
error — the CALLER counts output, so a crash cannot read as "clean" (each mode also emits a
`#COMPLETE` marker the caller verifies).

# WHY THIS FILE EXISTS: two round-4 BLOCKERS, both "the guard asks the wrong question"

## B1 — the predicate asked by SPELLING, not by LOCATION

The awk predecessor required a literal `bash`/`sh` TOKEN on the same physical line as a
driver-naming token. Its own header claimed it asked "by LOCATION rather than by spelling".
It did not, and three ordinary spellings walked past it — MEASURED against that version, all
three produced ZERO findings:

    bash \\
      "$DRIVER" --corpus /c      # line 1 has no driver token; line 2 has no shell token
    "$DRIVER" --corpus /c        # a bare exec of an executable script
    exec "$DRIVER" --corpus /c
    env -i "$DRIVER" --corpus /c

The last shape is how the driver's OWN usage text documents running it
(`ws0-baseline.sh:52`), so it is the most likely one to be written. On the gate's Linux box
any of them reaches `relax_perf_sysctls` (a host `sudo sysctl -w`), `cargo build --release`
and the 45-second-per-rep measurement loop.

THE FIX IS THE PERF LINT'S POSTURE, ported: **an unresolvable command word is TREATED AS an
invocation.** `_command_words` reduces a logical line to the command words that could run
something, stepping over assignment prefixes and known wrappers (`exec`, `env`, `command`,
`nohup`, `time`, `timeout`, `sudo`, `taskset`, `nice`, `xargs`, shells), and a line is a
finding when ANY of its command words could be the driver — including a variable expansion
this file cannot resolve. Failing CLOSED on "could be" is the only posture with no
enumeration to be wrong about; the alternative was a fourth round of adding spellings.

Line continuations are handled by JOINING LOGICAL LINES before classifying, which is what
bash does, and the finding is reported at the line where the logical line STARTED.

## B3 (round 6) — the WRAPPER SET WAS A HAND-WRITTEN LIST, so compound-command position was blind

B1's fix above claimed "no enumeration left to be wrong about". That claim was FALSE, and this
is the THIRD recurrence of the same class (B1: predicate by spelling; F6: a hardcoded
five-token line gate; B3: a hand-written keyword list). `WRAPPERS` enumerated
`then`/`else`/`elif`/`do` and OMITTED `if`, `while`, `until` — **the omission of `if` beside the
presence of `elif` is the tell that the list was written by hand.** Traced for
`if bash "$DRIVER" --corpus /c; then`: `if` was not a wrapper, so it was appended as the command
word and set `expect = False`; `bash` and `"$DRIVER"` then both hit `if not expect: continue`;
`command_words` came out `['if', 'then']` and the driver token was NEVER passed to
`_word_could_be_driver`.

MEASURING it found FIVE MORE shapes the report did not name, from a SECOND cause: control
operators were classified AFTER `_bare()`, which strips exactly the operator characters, so
`_bare(";") == _bare("&&") == _bare("|") == ""` and every STANDALONE operator hit
`if not bare: continue` WITHOUT resetting command position. Eight shapes, all measured at ZERO
findings, all one character from code these suites already contain:

    if bash "$DRIVER" …          while …           until …          elif …
    true && bash "$DRIVER" …     false || …        echo x | …
    ( bash "$DRIVER" … )         a) bash "$DRIVER" … ;;   (a case branch)
    for f in $(bash "$DRIVER" --list)      (`in` is opaque; `$(` was erased to whitespace)

THE FIX IS AN INVERSION TO A CLOSED GRAMMAR, not three more keywords. Every token is exactly
one of: a control operator (closed — bash's grammar fixes it), a RESERVED WORD (closed — see
below), a wrapper, an assignment prefix, or **anything else, which is treated as a possible
command word**. Nothing is stepped over for failing to appear on a list; the unrecognised case
is the FAIL-CLOSED case.

**WHICH FINITE SET THIS RELIES ON, and why it is closed.** `RESERVED_WORDS` is the one
enumeration that remains, and it is closed because **bash itself enumerates it**: `compgen -k`.
`test_ws0_hermeticity.sh`'s `reserved-closure` asserts SET EQUALITY against that oracle in both
directions (plus a positive control on the oracle), so a bash release adding a reserved word
FAILS the suite instead of silently opening a hole. That is the difference from the three
enumerations that preceded it: variable NAMES, invocation SPELLINGS and argument SHAPES are open
sets with no oracle, so enumerating them could only ever be wrong. Each reserved word is
classified TRANSPARENT or OPAQUE, and the partition is asserted TOTAL AND DISJOINT **at import**
— an unclassified word raises rather than inheriting the stop-scanning branch.

**RESIDUAL ENUMERATION, STATED RATHER THAN CLAIMED AWAY.** `WRAPPERS` (external commands that
take a command as an argument: `env`, `timeout`, `taskset`, …) has NO oracle — "programs that run
another program" is open-ended. A wrapper not listed there consumes command position, so a driver
token immediately after it is not examined. That hole is not closable without a model of every
command on `PATH`, and it is not closable by widening either: flagging a driver token in ARGUMENT
position was measured to red on this repo's own shipped code. So it is recorded at the constant
and here, and it is what the header no longer claims to have eliminated.

## B2 — the SUBJECT was one glob, and its completeness check compared that glob to itself

The predecessor's subject was `"$dir"/test_ws0_*.sh` only. Round 3 had itself introduced two
SOURCED LIBRARIES under `scripts/tests/` — `lib-ws0-hermetic.sh` (which is where
`ws0_driver_run` lives) and `lib-ws0-fixtures.sh` — and NEITHER was examined, so a bare
invocation added to a shared helper was invisible. Worse, the "subject is complete" test
compared `ws0_hermeticity_lint_subject` against `ls ./test_ws0_*.sh`: **the same glob against
itself**, which can only ever confirm its own definition.

THE FIX IS AN INDEPENDENT ORACLE, not a wider glob. Two things that must not share a
definition, and the whole of B2 is that they did:

* the SUBJECT (`subject()`) is every `*.sh`/`*.py` under the tests root, plus
  `ADDITIONAL_SUBJECT`. It is a DEFINITION. It does not consult the census.
* the CENSUS (`census()`) is every TRACKED file whose CONTENT mentions the driver, from
  `git ls-files`. It is an ORACLE, and nothing about the subject's definition can influence it.

`subject` mode then asserts SUBJECT ⊇ (CENSUS − EXEMPTIONS) and prints an `UNCOVERED` line per
violation. Because the two are defined independently, that assertion can FAIL — which the
predecessor's could not, and which a first draft of this file also could not, because it folded
the census into the subject and so proved the containment by construction.

The exemption list is PATHS with REASONS, in this file, so adding one is a visible decision;
a STALE exemption (a path that no longer mentions the driver, or is no longer tracked) is
reported too, so the list cannot quietly accumulate claims nobody checks.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys

# The driver, by basename. A path-based test would miss a copy under $TMP, which the
# self-tests legitimately build and run.
DRIVER_BASENAME = "ws0-baseline.sh"

# The marker that exempts a single LINE which must genuinely run past the argument boundary
# (the positive controls in test_ws0_hermeticity.sh, and `ws0_driver_run` itself).
LINE_MARKER = "ws0-hermetic-allow"

# BASH'S RESERVED WORDS — a CLOSED set, and the ONLY reason a complete enumeration is
# achievable in this file at all (see the header's B3 section).
#
# It is closed because BASH ITSELF enumerates it: `bash -c 'compgen -k'`. So this is not a
# hand-written list that someone must remember to extend — `reserved-closure` in
# test_ws0_hermeticity.sh asserts SET EQUALITY against `compgen -k`, so a bash release that adds
# a reserved word FAILS the suite rather than silently opening a hole. That is the difference
# between this enumeration and the three that preceded it: variable NAMES, invocation
# SPELLINGS and argument SHAPES are all open sets with no oracle, so enumerating them could
# only ever be wrong; reserved words have an oracle that ships with the shell.
RESERVED_WORDS = frozenset(
    {
        "if", "then", "else", "elif", "fi", "case", "esac", "for", "select", "while",
        "until", "do", "done", "in", "function", "time", "{", "}", "!", "[[", "]]",
        "coproc",
    }
)

# Every reserved word is classified into exactly one of these two, and
# `reserved-classification` asserts the partition is TOTAL over `RESERVED_WORDS` — so a newly
# added reserved word cannot land unclassified and inherit a default.
#
# TRANSPARENT: a COMMAND may begin at the next token, so keep scanning in command position.
# This is where `if`/`while`/`until` were missing (round 6, B1): `elif` was present and `if`
# was not, which is the signature of a list written by hand.
RESERVED_TRANSPARENT = frozenset(
    {
        "if", "then", "else", "elif", "fi", "while", "until", "do", "done", "esac",
        "time", "coproc", "!", "{", "}",
    }
)
# OPAQUE: the next token is NOT a command — it is a NAME (`for`/`select`), a case SUBJECT
# (`case`), a WORD LIST (`in`), a function name (`function`) or a conditional expression
# (`[[`/`]]`). A command inside one of those positions can only arrive through a command
# SUBSTITUTION, and `open_substitutions` puts that back into command position explicitly, so
# nothing is lost by refusing to scan a word list: MEASURED, `for f in $(bash "$DRIVER")` is
# flagged with `in` opaque.
RESERVED_OPAQUE = frozenset({"case", "for", "select", "in", "function", "[[", "]]"})

# THE PARTITION IS TOTAL AND DISJOINT, asserted at IMPORT — not in a test that could be skipped.
# A reserved word added to `RESERVED_WORDS` without a classification would otherwise fall through
# `stripped_ops in RESERVED_TRANSPARENT` to the OPAQUE branch and silently stop scanning: an
# unrecognised value inheriting the permissive-for-a-bypass default, which is the shape this whole
# file exists to stop. So it is a hard error at import instead.
_unclassified = RESERVED_WORDS - (RESERVED_TRANSPARENT | RESERVED_OPAQUE)
_both = RESERVED_TRANSPARENT & RESERVED_OPAQUE
_extra = (RESERVED_TRANSPARENT | RESERVED_OPAQUE) - RESERVED_WORDS
if _unclassified or _both or _extra:
    raise AssertionError(
        "the reserved-word classification is not a total disjoint partition of RESERVED_WORDS:"
        f" unclassified={sorted(_unclassified)} both={sorted(_both)}"
        f" not-reserved={sorted(_extra)}"
    )

# The sanctioned WRAPPERS — external commands that take a COMMAND as an argument. A line whose
# command word is one of these is not itself the finding; the lint steps over it and keeps
# looking, exactly as bash would.
#
# RESIDUAL ENUMERATION, STATED (round 6, B1): unlike `RESERVED_WORDS`, this set has NO oracle —
# "programs that run another program" is open-ended (`strace`, `nsenter`, `firejail`, `unshare`
# …), so a wrapper NOT listed here consumes command position and a driver token after it is not
# examined. That hole is not closable without a model of every command on `PATH`, and it is NOT
# closable by widening: flagging a driver token in ARGUMENT position was measured to red on this
# repo's own shipped code (`grep -n '…' "$REPO/scripts/perf/ws0-baseline.sh"`). So it is recorded
# here as a known limit rather than claimed away. What bounds it in practice: the driver token
# after an unknown wrapper is still examined whenever a control operator or a reserved word
# re-enters command position later on the line, and the SUBJECT/CENSUS oracle still puts the
# file in scope.
WRAPPERS = frozenset(
    {
        "exec", "env", "command", "builtin", "nohup", "time", "timeout", "sudo",
        "taskset", "nice", "ionice", "stdbuf", "setsid", "xargs", "bash", "sh",
        "dash", "zsh", "ksh",
    }
)

# The CONTROL OPERATORS, also closed — this is bash's own operator set, fixed by the grammar
# rather than by anyone's memory. A token made only of these characters ENDS the current command
# and puts the next token in command position.
#
# The previous code checked these AFTER reducing the token with `_bare`, which strips exactly
# these characters — so `_bare(";") == ""`, `_bare("&&") == ""`, `_bare("|") == ""` and every
# standalone operator hit `if not bare: continue` and NEVER reset command position. MEASURED at
# zero findings for `true && bash "$DRIVER" …`, `false || bash "$DRIVER" …` and
# `echo x | bash "$DRIVER" …` before this fix.
_OP_ONLY_RE = re.compile(r"^[;&|()]+$")
_OP_LEADING_RE = re.compile(r"^[;&|()]+")
_OP_TRAILING_RE = re.compile(r"[;&|()]+$")

# Tokens that name the driver, or COULD. The last case is the whole point: a variable this
# file cannot resolve MIGHT hold the driver path, so it counts (fail closed).
_VAR_RE = re.compile(r'^"?\$\{?[A-Za-z_][A-Za-z0-9_]*')
_ASSIGN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\[[^]]*\])?=")

# Files whose driver mention is NOT an invocation site and which are deliberately outside the
# lint's subject. A PATH plus a REASON, so an addition is a visible decision rather than a
# pattern quietly widening. Checked against the census below, and a stale entry (a path that
# no longer mentions the driver) is itself reported.
EXEMPTIONS: dict[str, str] = {
    "scripts/perf/ws0-baseline.sh": "IS the driver",
    "scripts/perf/lib-args.sh": "a driver library; sourced by the driver, never invokes it",
    "scripts/perf/lib-host-state.sh": "a driver library; prose reference only",
    "scripts/perf/lib-perf-lint.sh": "a driver library; prose reference only",
    # Round 7's campsite-rule split (F3 took the driver to 1035 lines). Reported UNCOVERED the
    # moment it became tracked, exactly as round 5's three splits were — the census is
    # CONTENT-based, so this entry is the decision, recorded, and not a pattern widening to
    # accommodate it. Same class as its three siblings above: sourced by the driver, and its
    # only driver mention is prose (its refusals tell an operator which run owns a directory).
    "scripts/perf/lib-outdir.sh":
        "a driver library; sourced by the driver, never invokes it (prose reference only)",
    # Round 9's campsite-rule split (the guard fixes took the driver to 1008 lines). Reported
    # UNCOVERED the moment it became tracked, exactly as every previous split was — and note the
    # PERF tree lint absorbed it SILENTLY (its subject is a `scripts/perf/*.sh` glob, so a new
    # library joins automatically) while this census, which is CONTENT-based, correctly demanded a
    # decision. Two lints, two postures, and the difference is why both are checked on a split.
    # Same class as its five siblings: sourced by the driver, and its only driver mentions are
    # prose (the split's rationale and the note that `perf_stat_c` stays in the driver).
    "scripts/perf/lib-measure.sh":
        "a driver library; sourced by the driver, never invokes it — its ws0-baseline.sh mentions"
        " are prose explaining the split and why the perf wrapper did not move with it",
    # Round 10's M2 campsite-rule split (the provenance record took the driver to 986 lines).
    # Reported UNCOVERED the moment it became tracked, exactly as its seven predecessors were — and
    # note the two postures again: the PERF tree lint's subject is a `scripts/perf/*.sh` glob, so
    # this library joined it AUTOMATICALLY and was linted in `library` mode (correctly finding no
    # perf invocation and no second `perf_stat_c`), while this CONTENT-based census demanded a
    # decision. Two lints, two postures, and checking BOTH on a split is why. Same class as its six
    # library siblings: sourced by the driver, never invokes it, and its `ws0-baseline.sh` mentions
    # are prose (the split's rationale, the coupling list, and why the perf wrapper did not move).
    "scripts/perf/lib-binaries.sh":
        "a driver library; sourced by the driver, never invokes it — its ws0-baseline.sh mentions"
        " are prose explaining the split, the driver globals it reads, and why perf_stat_c stayed",
    # Round 13's F2 campsite-rule split (the session-owned ticket path took the driver to 959 lines).
    # Reported UNCOVERED the moment it became tracked, exactly as its eight predecessors were — the
    # NINTH time this oracle has caught a split/addition, which is what it is for. Note the two
    # postures once more: the PERF tree lint's subject is a `scripts/perf/*.sh` glob, so this library
    # joined it AUTOMATICALLY and was linted in `library` mode (correctly finding no perf invocation
    # and no second `perf_stat_c`), while this CONTENT-based census demanded a decision. Checking BOTH
    # on a split is why. Same class as its seven library siblings: sourced by the driver, never
    # invokes it, and its `ws0-baseline.sh` mentions are prose (the split's rationale, the driver
    # globals it reads and sets, and why the perf wrapper did not move).
    "scripts/perf/lib-inputs.sh":
        "a driver library; sourced by the driver, never invokes it — its ws0-baseline.sh mentions"
        " are prose explaining the split, the driver globals it reads/sets, and why perf_stat_c"
        " stayed",
    # Round 22's campsite-rule split: the per-rep BOUNDARY CHECK, wired into the measurement loop
    # against a driver already at its hard 950-line budget. Reported UNCOVERED the moment it became
    # tracked, exactly as its nine predecessors were — the TENTH time this oracle has caught a
    # split, which is what it is for. Note the two postures once more: the PERF tree lint's subject
    # is a `scripts/perf/*.sh` glob, so this library joined it AUTOMATICALLY and was linted in
    # `library` mode (correctly finding no perf invocation and no second `perf_stat_c`), while this
    # CONTENT-based census demanded a decision. Same class as its eight library siblings: sourced by
    # the driver, never invokes it, and its `ws0-baseline.sh` mentions are prose (the line budget
    # that forced the split, the driver globals it reads, and the finding's statement — that round
    # 21's boundary verifier was built, tested and CALLED BY NOTHING).
    "scripts/perf/lib-corpus-boundary.sh":
        "a driver library; sourced by the driver, never invokes it — its ws0-baseline.sh mentions"
        " are prose explaining the split, the driver globals it reads, and the round-21 finding"
        " that the boundary verifier was unwired",
    # #3551's campsite-rule split (the three flight-arm flags took the driver ~250 lines further
    # past the ~800-line source target). Reported UNCOVERED the moment it became tracked, exactly
    # as its nine library predecessors were — and the TWO POSTURES showed themselves again in the
    # same run: the PERF tree lint's subject is a `scripts/perf/*.sh` glob, so this library joined
    # all three of its layers AUTOMATICALLY (127 -> 129 checks), while this CONTENT-based census
    # correctly demanded a decision. Checking BOTH on a split is exactly why. Same class as its
    # siblings: sourced by the driver, never invokes it, and its `ws0-baseline.sh` mentions are
    # prose (the line budget that forced the split, and what the driver deliberately keeps — the
    # flag defaults, the --help text and the ORDER of operations).
    "scripts/perf/lib-flight-arm.sh":
        "a driver library; sourced by the driver, never invokes it — its ws0-baseline.sh mentions"
        " are prose explaining the split and what the driver keeps (defaults, --help, the order"
        " of operations)",
    "scripts/perf/README.md": "documentation",
    "scripts/perf/ws0_report.py": "the reporter; prose reference only",
    # Round 22's F1 module — the boundary record READ BACK — and the census reported it UNCOVERED
    # the moment it became tracked, the ELEVENTH time this oracle has caught a split/addition. It
    # was found by RUNNING this suite while building the observing cases for that very module's
    # checker (#3272 round 25): F1 landed the module and the exemption record was never updated, so
    # `test_ws0_hermeticity.sh` was red on the branch. Same class as its python siblings: imported
    # by `ws0_report.py`, never invokes anything, and its single `ws0-baseline.sh` mention is prose
    # — the absent-record refusal that tells an operator to re-run the session with the driver,
    # which is the one thing that can write the observations it requires.
    "scripts/perf/ws0_boundary_observations.py":
        "the boundary-record COMPLETENESS reader; prose reference only (its absent-record refusal"
        " tells an operator to re-run the session with the driver, which is what appends the"
        " observations)",
    "scripts/perf/ws0_rounds.py": "the reporter; prose reference only",
    "scripts/perf/ws0_collect.py": "the reporter; prose reference only",
    # `ws0_validate.py` is deliberately ABSENT: #3272 round 5 moved its driver-mentioning
    # refusals into `ws0_session.py`, and the STALE-EXEMPTION check flagged the leftover entry
    # — the oracle working in the other direction, refusing a claim nobody checks any more.
    #
    # These three arrived with round 5's campsite-rule splits. The census is CONTENT-based, so
    # each was reported UNCOVERED the moment the prose moved into it: a driver mention in a new
    # file is a decision someone records here, not a pattern that quietly widens.
    "scripts/perf/ws0_session.py":
        "the session/corpus identity module; prose reference only (its refusals tell an"
        " operator to re-run the session with the driver)",
    "scripts/perf/ws0_flight_arm.py": "the Flight arm collector; prose reference only",
    # Round 10's M1 module, and the census reported it UNCOVERED the moment it was staged — the
    # SIXTH time this oracle has caught an addition, which is what it is for. Same class as its
    # siblings: it is imported by the driver (via `python3 -c`) and by `ws0_session.py`, and its
    # only `ws0-baseline.sh` mentions are prose — the refusals that tell an operator to re-run the
    # session with the driver, which is the one thing that can write the pin it requires.
    "scripts/perf/ws0_ticket_input.py":
        "the Flight TICKET (request) identity module; prose reference only (its refusals tell an"
        " operator to re-run the session with the driver, which is what pins the request)",
    # Round 13's F3 module, and the census reported it UNCOVERED the moment it was staged — the
    # SEVENTH time this oracle has caught an addition, which is what it is for, and it caught this
    # one on the same run as the shipped-lint check. Same class as its siblings: imported by the
    # driver (through `python3 -c` in `lib-inputs.sh`) and by `ws0_session.py`/`ws0_report.py`, and
    # its only `ws0-baseline.sh` mentions are prose — the finding's statement (the DRIVER's pin
    # compared the corpus against nothing) and the refusal that tells an operator which flag runs a
    # noncanonical corpus anyway.
    "scripts/perf/ws0_canonical_corpus.py":
        "the CANONICAL-corpus comparison module; prose reference only (it names the driver in its"
        " statement of the finding and in the refusal that points an operator at --non-baseline)",
    # Round 10's M2 python half, staged alongside `lib-binaries.sh` above and reported UNCOVERED
    # with it. Same class as its four python siblings: imported by the driver (through
    # `python3 -c`) and its only driver mention is prose — the refusal that tells an operator to
    # re-run the session with the driver, which is the one thing that can write the record it needs.
    "scripts/perf/ws0_binaries.py":
        "the measured-BINARIES identity module; prose reference only (its refusal tells an operator"
        " to re-run the session with the driver, which is what records which programs were measured)",
    # Round 9's F6 module, and the census reported it UNCOVERED the moment it was staged — the
    # fourth time this oracle has caught a split/addition, which is what it is for. Same class as
    # its siblings: its only driver mention is prose (its refusal tells an operator to re-run the
    # session with the driver, which is the one thing that can produce the record it requires).
    "scripts/perf/ws0_pinning.py":
        "the CPU-pin verification record; prose reference only (its refusal tells an operator to"
        " re-run the session with the driver, which is what records the verification)",
    # `ws0_loadgen_record.py` is deliberately absent too: it never names the driver, so an
    # exemption for it would be a claim about nothing — which the STALE-EXEMPTION check
    # correctly refused when it was added speculatively.
    "tools/ws0-corpus-gen/README.md": "documentation",
    "tools/ws0-corpus-gen/src/bin/scan_bench.rs": "rust; cannot invoke a shell script bare",
    # Round 10's F-B test file, and the census reported it UNCOVERED the moment it was staged —
    # the FIFTH time this oracle has caught an addition, which is what it is for. Note again the
    # two postures that make checking both worthwhile: `cargo test -p ws0-corpus-gen` absorbed
    # this new integration target SILENTLY (its subject is the crate's `tests/` directory, so a
    # new file joins automatically) while this CONTENT-based census correctly demanded a decision.
    # Same class as its `scan_bench.rs` sibling: rust cannot invoke a shell script bare, and its
    # single driver mention is prose (it records that `ws0-baseline.sh` runs both of arm A's legs
    # per rep, which is WHY both output paths must carry the verified scope).
    "tools/ws0-corpus-gen/tests/scan_bench_ingests_exactly_one_table_dir.rs":
        "rust; cannot invoke a shell script bare — its ws0-baseline.sh mention is prose stating"
        " that the driver runs both the scanning and --setup-only legs per rep",
}
# Directory prefixes whose every file is documentation.
EXEMPT_PREFIXES = ("docs/",)


def logical_lines(text: str) -> list[tuple[int, str]]:
    """`(starting-lineno, joined-text)` per LOGICAL line — continuations joined.

    A trailing backslash continues a line in bash, so a physical-line scan sees `bash \\` and
    `"$DRIVER" …` as two lines with neither carrying both halves of an invocation. That was
    B1's first bypass. Joining first is what bash does; the finding is reported at the line
    where the logical line STARTED, which is where a reader will look.
    """
    out: list[tuple[int, str]] = []
    start: int | None = None
    buf: list[str] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        if start is None:
            start = lineno
        stripped = raw.rstrip()
        if stripped.endswith("\\") and not stripped.endswith("\\\\"):
            buf.append(stripped[:-1])
            continue
        buf.append(stripped)
        out.append((start, " ".join(buf)))
        start = None
        buf = []
    if start is not None and buf:
        out.append((start, " ".join(buf)))
    return out


def strip_trailing_comment(line: str) -> str:
    """Drop a trailing `#` comment so prose mentioning the driver is not read as argv.

    A `#` only starts a comment at a token boundary, which is why this looks for whitespace
    before it rather than for the character alone (`--flag#x` is one token).
    """
    if line.lstrip().startswith("#"):
        return ""
    m = re.search(r"\s#", line)
    if m:
        return line[: m.start()]
    return line


def _bare(token: str) -> str:
    """A token as the shell would see it after quote removal."""
    return token.strip("\"'`();&|")


def open_substitutions(line: str) -> str:
    """Turn `$( … )` and backticks into token boundaries, so their CONTENTS are scanned.

    A command substitution runs a command, and `out=$("$DRIVER" --corpus /c)` is a real bare
    invocation — it is the dominant shape in these suites (`out=$(ws0_driver_run "$DRIVER" …)`
    with the sanctioned wrapper, and `out=$(bash "$DRIVER" …)` without it). Without this, the
    whole `out=$("$DRIVER"` is ONE whitespace token, matches the assignment-prefix regex, and is
    stepped over: MEASURED, `out=$("$copy" --corpus /c)` produced NO finding.

    Replacing the punctuation with spaces (rather than parsing nesting) is deliberate: the inner
    text then goes through the same command-word rules as any other line, and the only cost is
    that a substitution's first token is treated as a command word — which it IS.

    ROUND 6, B1: the delimiters are replaced with STANDALONE OPERATOR TOKENS rather than with
    whitespace, because whitespace DESTROYS the command boundary the operator carries and two
    shapes were MEASURED at zero findings because of it:

    * `for f in $(bash "$DRIVER" --list); do` — `in` is an OPAQUE reserved word (a word LIST
      follows, not a command), so with `$(` erased the substitution's contents inherited that
      non-command position and `bash "$DRIVER"` was never examined. `$(` now becomes `;`, which
      is exactly what it means: a command substitution starts a FRESH command.
    * `a) bash "$DRIVER" --corpus /c ;;` — a `case` branch. With `)` erased, `a` became a plain
      word in command position, consumed it, and `bash "$DRIVER"` after it was skipped. `)` now
      survives as an operator token, so the pattern ends the (non-)command and the branch body
      is scanned.
    """
    return (
        line.replace("$(", " ; ")
        .replace("`", " ; ")
        .replace(")", " ) ")
        .replace("<(", " ; ")
    )


def command_words(line: str) -> list[str]:
    """Every token in this logical line that could be a COMMAND WORD.

    "Could be", not "is": reimplementing bash word-splitting would be a second implementation
    of bash, only as good as differential testing nobody will do. So this asks a cheap,
    well-defined question and errs toward MORE command words, because more command words means
    more findings, and a false finding is visible while a missed one is not.

    A token is a candidate command word when it is the first token of the line, or follows a
    control operator (`;` `&&` `||` `|` `&`) or a sanctioned wrapper, with assignment prefixes
    stepped over. That covers `"$DRIVER" …`, `exec "$DRIVER" …`, `env -i "$DRIVER" …`,
    `PATH=x bash "$DRIVER" …`, `foo && "$DRIVER" …` and `bash \\<newline> "$DRIVER"` (joined
    above) in ONE rule rather than as five enumerated spellings.
    """
    tokens = [t for t in open_substitutions(line).split() if t]
    words: list[str] = []
    expect = True
    in_wrapper = False
    # QUOTE PARITY, accumulated across tokens. A whitespace split does not respect quoting, so
    # a token INSIDE a quoted string looks exactly like a token in command position — and that
    # produced a real FALSE FINDING on this repo's own test code. MEASURED:
    #
    #     pin_line=$(grep -n '^for temp in $TEMPS; do' "$REPO/scripts/perf/ws0-baseline.sh" …)
    #
    # was flagged, because `$TEMPS;` ended in a `;` that was read as a control operator (reset
    # to command position) and the next token `do'` reduced to the wrapper word `do`, so the
    # driver PATH — an argument to `grep` — became a candidate command word. A guard that reds
    # on `grep -n '… do' "$DRIVER"` is the guard an operator deletes.
    #
    # Tracked as parity rather than as a parser: an ODD count of unescaped quotes means the
    # string is still open, so subsequent tokens are string CONTENT until it closes. That is
    # cheap, and it errs toward SKIPPING tokens inside quotes — which cannot hide an
    # invocation, because an invocation's command word is by definition not inside a string.
    sq = dq = 0
    for token in tokens:
        inside = (sq % 2 == 1) or (dq % 2 == 1)
        sq += token.count("'")
        dq += token.count('"')
        # Skipped when the token IS inside an open string, and ALSO when it OPENS one that
        # stays open past itself — because the whole token is then string content.
        #
        # The second half is not redundant, and leaving it out was measured: in
        # `grep -n 'x; do y' "$DRIVER"`, the token `'x;` is not yet inside a string (the quote
        # opens ON it), so the pre-fix version processed it, saw the trailing `;`, and reset to
        # command position — after which `"$DRIVER"`, an ARGUMENT to grep, was read as a command
        # word and flagged. The `;` inside a quoted pattern is not a control operator, so the
        # token that opens the quote must be skipped as content too.
        if inside or (sq % 2 == 1) or (dq % 2 == 1):
            continue
        # ---- token classification, as a CLOSED GRAMMAR (round 6, B1) --------------------
        # Order matters: operators first (they delimit commands), then reserved words (they are
        # the shell's own vocabulary), then everything else — and "everything else" is the
        # FAIL-CLOSED bucket: an unrecognised token in command position is treated as a
        # possible command word, never stepped over. That is the inversion round 6 required:
        # nothing is skipped because it was not on a list.
        stripped_ops = _OP_TRAILING_RE.sub("", _OP_LEADING_RE.sub("", token))
        if _OP_ONLY_RE.match(token):
            # A STANDALONE control operator: `;`, `&&`, `||`, `|`, `&`, `(`, `)`, `;;`. Ends
            # the current command; the next token is in command position.
            expect = True
            in_wrapper = False
            continue
        bare = _bare(token)
        if not bare:
            continue
        if stripped_ops in RESERVED_WORDS or bare in RESERVED_WORDS:
            stripped_ops = stripped_ops if stripped_ops in RESERVED_WORDS else bare
            # A RESERVED WORD, possibly with an operator glued on (`;;` after a case body,
            # `(` before a subshell). TOTAL over `RESERVED_WORDS` by assertion, so there is no
            # unclassified default: transparent => next token is a command; opaque => it is not.
            if stripped_ops in RESERVED_TRANSPARENT:
                expect = True
            else:
                expect = False
            in_wrapper = False
            continue
        if _OP_TRAILING_RE.search(token) or _OP_LEADING_RE.match(token):
            # A control operator ATTACHED to a word (`cmd;`, `cmd&&`, `(cmd`). The word itself
            # is still a word — in command position when we were expecting one, and note a
            # LEADING operator puts it in command position regardless (`(bash "$D"`).
            if expect or _OP_LEADING_RE.match(token):
                if not (in_wrapper and not could_be_driver_token(bare)):
                    words.append(bare)
            expect = True
            in_wrapper = False
            continue
        if not expect:
            continue
        if _ASSIGN_RE.match(token):
            # An assignment prefix runs nothing in command position: step over it. Its value
            # may contain spaces, in which case the following tokens are part of the VALUE and
            # not a command word — but treating them as candidates only ever ADDS findings,
            # which is the safe direction, so no balance heuristic is needed here.
            continue
        if in_wrapper and not could_be_driver_token(bare):
            # We are between a WRAPPER and its command word, and this token cannot be the
            # driver: it is the wrapper's own option or that option's value. KEEP LOOKING.
            #
            # Stated as "cannot be the driver" rather than as "is an option", because the
            # option-shaped version of this test was ITSELF the B1 shape one level in. MEASURED
            # with `if bare.startswith("-") or bare.isdigit()`:
            # `taskset -c 2,10 "$DRIVER" --corpus /c` produced NO finding — `2,10` is neither an
            # option nor a digit string, so it was taken as the command word and `"$DRIVER"`
            # after it was never examined. Enumerating argument SHAPES is the same losing game
            # as enumerating invocation spellings, so the rule skips everything that is not a
            # candidate and can therefore never skip a candidate.
            continue
        words.append(bare)
        expect = bare in WRAPPERS
        in_wrapper = bare in WRAPPERS
    return words


def could_be_driver_token(word: str) -> bool:
    """Could this token be the driver at all? (`names_driver` without the explanation.)"""
    return names_driver(word) is not None


def names_driver(word: str) -> str | None:
    """Why `word` could name the driver, or None.

    THREE cases, and the third is the fail-closed one B1 was missing: a variable expansion
    this file cannot resolve MIGHT hold the driver path. `perf_invocation_lint`'s layer 1 took
    exactly this posture for an unresolvable command word, and it is the only posture that
    does not depend on an enumeration of spellings being complete.
    """
    if word.endswith(DRIVER_BASENAME):
        return f"names {DRIVER_BASENAME} literally"
    if word in {"$DRIVER", "${DRIVER}", '"$DRIVER"', '"${DRIVER}"'}:
        return "expands $DRIVER"
    if _VAR_RE.match(word) and not word.startswith("$("):
        return (
            f"the command word `{word}` is a VARIABLE this lint cannot resolve, so it COULD"
            " be the driver"
        )
    return None


REASON = (
    "invokes (or could invoke) the WS0 driver outside ws0_driver_run — with no"
    " --validate-args-only and no recording shims, so on a LINUX host this reaches"
    " relax_perf_sysctls (a host `sudo sysctl -w`), `cargo build --release` and the"
    " measurement loop (#3272 B1). Route it through ws0_driver_run, or mark the line"
    f" {LINE_MARKER} if it must genuinely run past the argument boundary."
    " NOTE: an UNRESOLVABLE command word counts as an invocation on purpose — this lint asks"
    " by LOCATION and fails CLOSED rather than enumerating spellings."
)


# A file HAS A HANDLE ON THE DRIVER when its text names it: the basename, or a `DRIVER`
# variable. This is the FILE-level gate below, and it is what keeps an unresolvable command word
# from being a finding in every script in the repo.
_HANDLE_RE = re.compile(re.escape(DRIVER_BASENAME) + r"|\$\{?DRIVER\}?|\bDRIVER=")


def has_driver_handle(text: str) -> bool:
    """Does this FILE name the driver anywhere at all?

    The gate is FILE-scoped, not line-scoped, and that is deliberate: within a file that has a
    handle on the driver, ANY unresolvable command word is a candidate (`bash "$copy"`,
    `"$treedir/..."`) — which is what makes the lint fail closed on a spelling nobody
    anticipated. A file that never mentions the driver has no handle to invoke it with, so
    treating its `"$cmd"` lines as candidates would red on ordinary scripts. MEASURED: without
    this gate, `test_gen_perf_corpus_bti.sh:1122` (`python3 "$copy" --self-check`, an unrelated
    suite's own fixture copy) was reported as a driver invocation.

    RESIDUAL, stated rather than left to be found: a file that assembles the driver path from
    pieces (`"$root/scripts/perf/$name"`) has a handle this test cannot see. What covers that is
    the SUBJECT/CENSUS oracle — the census is over file CONTENT, so any file naming the driver is
    in the subject — and nothing covers a fully computed path. That is a known limit, not an
    oversight; reimplementing shell string evaluation would be a second implementation of bash.
    """
    return bool(_HANDLE_RE.search(text))


# The python APIs that can start a process. A python file's driver MENTION is a string constant
# or a comparison in almost every case (this file is full of them); what matters is whether it
# reaches one of these.
_SPAWN_FUNCS = frozenset({"run", "call", "check_call", "check_output", "Popen", "system",
                          "spawnl", "spawnv", "execv", "execl", "execvp", "execlp"})

# What counts as naming the driver INSIDE a python spawn call. Broader than `_HANDLE_RE` on
# purpose: python has no `$`, so `subprocess.run([DRIVER, "--corpus", c])` carries a bare
# IDENTIFIER. MEASURED before this existed: exactly that line produced no finding while its
# `["bash", "/x/ws0-baseline.sh", …]` sibling was caught — the same
# spelling-vs-location split as B1, one language over. So any identifier whose name contains
# "driver" counts, whatever it is bound to (fail closed on the unresolvable).
_PY_SPAWN_HANDLE_RE = re.compile(
    re.escape(DRIVER_BASENAME) + r"|[A-Za-z_]*driver[A-Za-z_0-9]*", re.IGNORECASE
)


def lint_python(text: str) -> list[str]:
    """Findings for a PYTHON file: driver text reaching a PROCESS-SPAWNING call.

    Asked STRUCTURALLY, over the AST, and NOT as "a line mentioning the driver": a python file
    that implements or tests this lint necessarily contains `"$DRIVER"` and `ws0-baseline.sh` as
    DATA — string constants, dict keys, comparisons. MEASURED while writing this: a line-based
    version reported FIVE findings against this very file's own constants (`DRIVER_BASENAME =
    'ws0-baseline.sh'`, the exemption dict, `if word in {'$DRIVER', …}`), which is the
    reds-on-ordinary-code failure that gets a lint deleted.

    So the question is: does a call to `subprocess.run`/`Popen`/`os.system`/`os.exec*` have a
    driver-naming string ANYWHERE in its arguments? That is the invocation, whatever the spelling
    of the surrounding code, and it fails closed on an f-string or a variable it cannot resolve
    only insofar as the literal text is visible — which is the same limit `has_driver_handle`
    records. Docstrings/comments are irrelevant here because the AST never contained them as
    calls.

    Line numbers come from the AST, so they point at the real source line.
    """
    import ast

    try:
        tree = ast.parse(text)
    except SyntaxError:
        # Unparseable python is a FINDING, not a skip: the file was not scanned, and an
        # unscanned file prints exactly like a clean one.
        return ["0: this python file could not be parsed, so it was NOT SCANNED — an unscanned"
                " file prints exactly like a clean one"]
    findings: list[str] = []
    lines = text.splitlines()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (
            func.id if isinstance(func, ast.Name) else ""
        )
        if name not in _SPAWN_FUNCS:
            continue
        rendered = ast.unparse(node)
        if not _PY_SPAWN_HANDLE_RE.search(rendered):
            continue
        lineno = getattr(node, "lineno", 0)
        src = lines[lineno - 1] if 0 < lineno <= len(lines) else ""
        if LINE_MARKER in src or LINE_MARKER in rendered:
            continue
        findings.append(
            f"{lineno}: {REASON} [a `{name}(…)` process spawn whose arguments name the driver:"
            f" {rendered[:90]}]"
        )
    return findings


# An identifier that is ABOUT the driver — the same posture `_PY_SPAWN_HANDLE_RE` already
# takes one language over. It catches `$DRIVER`, `${DRIVER}`, and a helper such as
# `driver_copy_with` whose RETURN VALUE is a driver path.
_DRIVERISH_IDENT_RE = re.compile(r"[A-Za-z_]*driver[A-Za-z_0-9]*", re.IGNORECASE)

# `NAME=<value>` and `for NAME in <list>` — the two ways a shell name acquires a value.
# The assignment's value is extracted by `_assigned_value` rather than by `(.*)$`, because a
# greedy tail is WRONG for an ENV-PREFIX assignment and was measured to be: in
#
#     PATH="$WS0_SHIM_BIN:$PATH" bash "$LINUX_DRIVER" --corpus …
#
# a `(.*)$` value swallows the COMMAND too, so `PATH` referenced `$LINUX_DRIVER` and became
# driver-bearing — after which every ordinary `PATH="$WS0_SHIM_BIN:$PATH" "$tool" --probe`
# line was a finding. One FALSE FINDING, on this repo's own shipped test code, from reading
# a value that extends past where the shell ends it.
_ASSIGN_CAPTURE_RE = re.compile(r"(?:^|[;&|]\s*|\s)([A-Za-z_][A-Za-z0-9_]*)=")
_FOR_CAPTURE_RE = re.compile(r"\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\s+(.*?)(?:;|$)")


def _assigned_value(code: str, start: int) -> str:
    """The VALUE of an assignment beginning at `code[start]` (just past the `=`).

    A shell assignment's value ends at the first UNQUOTED whitespace — that is what makes
    `VAR=x cmd` an env prefix rather than a value containing a command. Quotes and `$( )`
    nesting are tracked so `out=$(bash "$DRIVER" …)` keeps its whole substitution (which
    genuinely IS the value, and genuinely does hold a driver path).
    """
    out: list[str] = []
    depth = 0
    sq = dq = False
    i = start
    while i < len(code):
        ch = code[i]
        if ch == "\\" and i + 1 < len(code):
            out.append(code[i:i + 2])
            i += 2
            continue
        if not dq and ch == "'":
            sq = not sq
        elif not sq and ch == '"':
            dq = not dq
        elif not sq and not dq:
            if ch == "$" and code.startswith("$(", i):
                depth += 1
            elif ch == "`":
                depth += 1 if depth == 0 else -1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch.isspace() and depth == 0:
                break
        out.append(ch)
        i += 1
    return "".join(out)

# The names that hold the driver WITHOUT having been assigned in the file — `$DRIVER` is set
# by every suite, and may arrive from a sourced library rather than a local assignment.
_SEED_BEARING_NAMES = frozenset({"DRIVER"})


def driver_bearing_names(text: str) -> tuple[frozenset[str], frozenset[str]]:
    """`(bearing, assigned)`: which shell names in this file could HOLD a driver path.

    # Why this is DISCOVERED and not a list (#3272 review round 5, F6)

    `lint_text`'s line gate used to be five hardcoded spellings:

        if DRIVER_BASENAME not in code and "$DRIVER" not in code \\
                and "${DRIVER}" not in code and "$copy" not in code \\
                and "${copy}" not in code:
            continue

    which made `names_driver`'s whole fail-closed posture — "an UNRESOLVABLE command word
    counts as an invocation on purpose … there is no enumeration left to be wrong" — DEAD
    CODE for any variable not named `DRIVER` or `copy`. The line never reached
    `command_words`, so it did not matter what `names_driver` would have said about it.
    MEASURED against the pre-fix lint, both at ZERO findings:

        drv="$DRIVER";                  bash "$drv" --corpus /c
        injected="$TMP/injected-ws0-baseline.sh";  bash "$injected" --corpus /c

    A one-line rename defeated the guard, and the file's own header advertised the opposite
    property. This is the same class as B1 (asking by SPELLING while claiming LOCATION), one
    level in — which is why the answer is a MECHANISM rather than adding `$drv` and
    `$injected` to the list.

    # The mechanism

    A name is driver-bearing when the value it is given could name the driver:

    * the value mentions `ws0-baseline.sh` literally (`injected="$TMP/x-ws0-baseline.sh"`);
    * the value contains a driver-ish IDENTIFIER — `$DRIVER`, `${DRIVER}`, or a call to a
      helper like `driver_copy_with` whose result IS a driver path (that is how
      `test_ws0_cpu_pinning_guards.sh` obtains its `copy`, so the previously-hardcoded
      `copy` is now DERIVED rather than named);
    * the value references an already-discovered bearing name (`a="$DRIVER"; b="$a"`).

    Both `NAME=value` and `for NAME in <list>` are read, because a loop over driver paths
    (`for d in "$DRIVER" "$copy"; do bash "$d"; done`) is a real way to invoke it, and
    iterated to a FIXPOINT so an assignment order like `b="$a"` before `a="$DRIVER"` cannot
    hide a name.

    # What this deliberately does NOT widen to

    A name whose value has no driver connection at all stays out, and that is what keeps the
    lint usable: `SHIM="$TMP/nopython/bin"`, `timeout_probe="$TMP/timeout-probe.sh"` and
    `trap_probe="$TMP/trap-probe.sh"` are probes of OTHER things, and the loop variables
    `$fn`/`$tool`/`$_f` iterate over library function names, shim tool names and file lists.
    Treating every unresolvable command word in a handle-bearing file as a candidate was
    measured at 8 findings over the shipped subject, all of them ordinary code — and the
    same experiment with `has_driver_handle` removed entirely was measured at 74. The gate
    is narrowed by EVIDENCE from the file's own assignments, not by a name list.

    # The UNASSIGNED case fails CLOSED — the second returned set is what makes that possible

    A name this file never assigns carries NO evidence either way: it may arrive from a
    sourced library, a caller's environment, or an `eval`. So it COULD hold the driver and
    counts, exactly as `names_driver` treats an unresolvable command word. Only a name the
    file DOES assign is judged by its value.

    That asymmetry is the entire design, and dropping it was measured: with unassigned names
    treated as non-bearing, `out=$(bash "$copy" --corpus /nonexistent)` in a probe that never
    assigns `copy` produced ZERO findings — re-breaking the B1 posture in the fix for F6. Hence
    two sets: `bearing` (assigned, and the value shows a driver connection) and `assigned`
    (every name the file binds at all), so a caller can ask "bearing OR never assigned".
    """
    bearing = set(_SEED_BEARING_NAMES)
    candidates: list[tuple[str, str]] = []
    for _lineno, logical in logical_lines(text):
        code = strip_trailing_comment(logical)
        if not code.strip():
            continue
        for m in _FOR_CAPTURE_RE.finditer(code):
            candidates.append((m.group(1), m.group(2)))
        for m in _ASSIGN_CAPTURE_RE.finditer(code):
            candidates.append((m.group(1), _assigned_value(code, m.end())))
    # FIXPOINT: a value may reference a name assigned LATER in the file, so one pass is not
    # enough. Bounded by the candidate count, since each round can only add names.
    for _ in range(len(candidates) + 1):
        grew = False
        for name, value in candidates:
            if name in bearing:
                continue
            if DRIVER_BASENAME in value or _DRIVERISH_IDENT_RE.search(value) \
                    or any(_references(value, b) for b in bearing):
                bearing.add(name)
                grew = True
        if not grew:
            break
    return frozenset(bearing), frozenset(name for name, _ in candidates)


def _references(text: str, name: str) -> bool:
    """Does `text` expand `$name` / `${name}`?"""
    return bool(re.search(r"\$\{?" + re.escape(name) + r"\}?", text))


def _word_var_name(word: str) -> str | None:
    """The shell NAME a command word expands, or None if it is not a bare expansion."""
    m = re.match(r'^"?\$\{?([A-Za-z_][A-Za-z0-9_]*)', word)
    return m.group(1) if m else None


def _word_could_be_driver(
    word: str, bearing: frozenset[str], assigned: frozenset[str]
) -> str | None:
    """Why this COMMAND WORD could be the driver, or None.

    `names_driver` filtered by the file's own evidence about the name (#3272 F6). Three
    outcomes, and the middle one is what the hardcoded five-spelling gate could not express:

    * a literal `…ws0-baseline.sh` path => yes, whatever the surrounding code.
    * a variable the file ASSIGNS: yes only when the assigned value shows a driver
      connection (`bearing`). This is what keeps the lint usable — `$tool` bound by
      `for tool in $WS0_SHIM_TOOLS`, `$fn` bound to a library function name and
      `$timeout_probe` assigned an unrelated path are all ASSIGNED and NOT bearing, so
      they are not candidates.
    * a variable the file NEVER assigns: yes, fail closed. There is no evidence either way
      — it may come from a sourced library, the environment, or an `eval` — so it COULD
      hold the driver, which is `names_driver`'s original posture preserved exactly.

    THE QUESTION IS ASKED OF THE COMMAND WORD, not of the line. Asking it of every name
    ANYWHERE on the line was measured to produce a FALSE FINDING on this repo's own shipped
    code: `PATH="$WS0_SHIM_BIN:$PATH" "$tool" --probe` references `$WS0_SHIM_BIN`, which
    THIS file never assigns (it comes from the sourced `lib-ws0-hermetic.sh`), so the line
    passed the gate and its loop-bound command word `$tool` was then flagged. The command
    word is the only token that can actually run something, so it is the only one whose
    provenance matters.
    """
    if word.endswith(DRIVER_BASENAME):
        return f"names {DRIVER_BASENAME} literally"
    name = _word_var_name(word)
    if name is None or word.startswith("$("):
        return None
    if name in bearing:
        return (
            f"the command word `{word}` expands ${name}, which this file assigns a value that"
            " could name the driver"
        )
    if name not in assigned:
        return (
            f"the command word `{word}` is a VARIABLE this lint cannot resolve (never assigned"
            " in this file), so it COULD be the driver"
        )
    return None


def lint_text(text: str, is_python: bool = False) -> list[str]:
    """`<lineno>: <reason>` per finding for one file's contents."""
    if not has_driver_handle(text):
        return []
    if is_python:
        return lint_python(text)
    findings = []
    bearing, assigned = driver_bearing_names(text)
    for lineno, logical in logical_lines(text):
        if LINE_MARKER in logical:
            continue
        code = strip_trailing_comment(logical)
        if not code.strip():
            continue
        # `ws0_driver_run`/`ws0_driver_run_copy` IS the sanctioned path.
        if re.search(r"\bws0_driver_run(_copy)?\b", code):
            continue
        # THE QUESTION IS ASKED OF EACH COMMAND WORD, against evidence DISCOVERED FROM THE
        # FILE (#3272 F6). There used to be a textual line gate of five hardcoded spellings
        # (`$DRIVER`/`${DRIVER}`/`$copy`/`${copy}`/the basename) in front of this loop, which
        # made the fail-closed posture in `names_driver` DEAD CODE for every other variable
        # name — a one-line rename walked past the guard, MEASURED at zero findings for both
        # `drv="$DRIVER"; bash "$drv"` and `injected="…ws0-baseline.sh"; bash "$injected"`.
        # `_word_could_be_driver` replaces both the gate and `names_driver` here, so the
        # decision is made once, on the token that can actually run something.
        for word in command_words(code):
            why = _word_could_be_driver(word, bearing, assigned)
            if why is None:
                continue
            findings.append(f"{lineno}: {REASON} [{why}]")
            break
    return findings


def repo_root(start: pathlib.Path) -> pathlib.Path:
    for p in [start, *start.parents]:
        if (p / ".git").exists():
            return p
    return start


def tracked_files(root: pathlib.Path) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        capture_output=True, text=True, check=True,
    )
    return [p for p in out.stdout.split("\0") if p]


def census(root: pathlib.Path) -> list[str]:
    """Every TRACKED file that MENTIONS the driver — the INDEPENDENT oracle for B2.

    Independent of the subject definition on purpose: the predecessor's completeness check
    compared its own glob against the same glob, so it could only confirm its own definition.
    This is derived from `git ls-files` + file content, which no change to a subject pattern
    can influence.
    """
    found = []
    for rel in tracked_files(root):
        p = root / rel
        if not p.is_file():
            continue
        try:
            if DRIVER_BASENAME in p.read_text(errors="ignore"):
                found.append(rel)
        except OSError:
            found.append(rel)  # unreadable => cannot clear it => report it
    return sorted(found)


def is_exempt(rel: str) -> str | None:
    if rel in EXEMPTIONS:
        return EXEMPTIONS[rel]
    for prefix in EXEMPT_PREFIXES:
        if rel.startswith(prefix):
            return f"under {prefix} (documentation)"
    return None


# Files OUTSIDE the tests root that the lint also examines. Empty today; it exists so the
# subject can be extended WITHOUT the extension being derived from the census — see `subject`.
ADDITIONAL_SUBJECT: tuple[str, ...] = ()


def subject(root: pathlib.Path, tests_dir: pathlib.Path) -> list[str]:
    """The files the lint EXAMINES — defined WITHOUT reference to the census (B2).

    Every `*.sh`/`*.py` under the tests root, plus `ADDITIONAL_SUBJECT`. That is all.

    THE INDEPENDENCE IS THE POINT, and getting it wrong once while writing this is why it is
    spelled out: a first version of this function ALSO folded in "every census file that is not
    exempt", which made `subject ⊇ census - exemptions` BY CONSTRUCTION, so `cmd_subject` could
    never report an uncovered file — the very self-confirming shape B2 is about, rebuilt in the
    fix for B2. The subject is now a definition and the census is an independent oracle over it,
    so a tracked file that mentions the driver, is not under the tests root, and is not exempt
    comes out as UNCOVERED (driven by the probe in test_ws0_hermeticity.sh).

    Note what the tests-root glob buys: the sourced `lib-ws0-*.sh` helpers B2 found missing are
    in, and so is a helper that does not currently mention the driver — so ADDING an invocation
    to an existing helper cannot land outside the subject.
    """
    picked: set[str] = set()
    for pattern in ("*.sh", "*.py"):
        for p in sorted(tests_dir.glob(pattern)):
            picked.add(str(p.relative_to(root)))
    picked.update(ADDITIONAL_SUBJECT)
    return sorted(picked)


def cmd_lint(paths: list[str]) -> int:
    if not paths:
        print("0: the lint was given NO FILES — its subject is EMPTY, which prints exactly"
              " like a clean tree")
        print("#COMPLETE files=0")
        return 0
    scanned = 0
    for arg in paths:
        p = pathlib.Path(arg)
        if not p.is_file():
            print(f"{arg}:0: is not a readable file, so the lint's subject is ABSENT for it"
                  " — which prints exactly like a clean file")
            continue
        text = p.read_text(errors="ignore")
        if not text.strip():
            print(f"{arg}:0: has no content — the lint's subject is EMPTY for this file")
            continue
        scanned += 1
        for finding in lint_text(text, is_python=p.suffix == ".py"):
            print(f"{arg}:{finding}")
    print(f"#COMPLETE files={scanned}")
    return 0


def cmd_subject(root_arg: str) -> int:
    tests_dir = pathlib.Path(root_arg).resolve()
    root = repo_root(tests_dir)
    subj = subject(root, tests_dir)
    for rel in subj:
        print(f"SUBJECT\t{rel}")
    # THE COMPLETENESS ASSERTION, against the independent census.
    uncovered = []
    for rel in census(root):
        reason = is_exempt(rel)
        if reason is not None:
            print(f"EXEMPT\t{rel}\t{reason}")
            continue
        if rel not in subj:
            uncovered.append(rel)
    for rel in uncovered:
        print(f"UNCOVERED\t{rel}\tmentions {DRIVER_BASENAME}, is NOT in the subject, and"
              f" carries no recorded exemption")
    # A STALE exemption is reported too: an exemption for a path that no longer mentions the
    # driver is a claim nobody is checking any more.
    tracked = set(tracked_files(root))
    mentioning = set(census(root))
    for rel in sorted(EXEMPTIONS):
        if rel not in tracked:
            print(f"STALE-EXEMPTION\t{rel}\tno longer tracked")
        elif rel not in mentioning:
            print(f"STALE-EXEMPTION\t{rel}\tno longer mentions {DRIVER_BASENAME}")
    print(f"#COMPLETE subject={len(subj)} uncovered={len(uncovered)}")
    return 0


def cmd_reserved() -> int:
    """Print the reserved-word set the grammar relies on, one per line, with its class.

    This exists so the SHELL suite can compare it against `bash -c 'compgen -k'` — the oracle
    that makes `RESERVED_WORDS` a CLOSED enumeration rather than a fourth hand-written list. The
    comparison has to happen in shell because only bash can answer what bash's reserved words
    are; python asserting against its own constant would confirm its own definition, which is
    exactly B2's self-confirming shape.
    """
    for word in sorted(RESERVED_WORDS):
        cls = "TRANSPARENT" if word in RESERVED_TRANSPARENT else "OPAQUE"
        print(f"{cls}\t{word}")
    print(f"#COMPLETE reserved={len(RESERVED_WORDS)}")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "reserved":
        return cmd_reserved()
    if len(argv) < 3:
        print(f"usage: {argv[0]} lint <file>… | {argv[0]} subject <tests-dir>"
              f" | {argv[0]} reserved", file=sys.stderr)
        return 2
    if argv[1] == "lint":
        return cmd_lint(argv[2:])
    if argv[1] == "subject":
        return cmd_subject(argv[2])
    print(f"unknown subcommand {argv[1]!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
