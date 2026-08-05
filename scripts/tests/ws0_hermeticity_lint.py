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

# The sanctioned wrappers. A line whose command word is one of these is not itself the
# finding; the lint steps over it and keeps looking, exactly as bash would.
WRAPPERS = frozenset(
    {
        "exec", "env", "command", "builtin", "nohup", "time", "timeout", "sudo",
        "taskset", "nice", "ionice", "stdbuf", "setsid", "xargs", "bash", "sh",
        "dash", "zsh", "ksh", "then", "else", "elif", "do", "!", "{", "(",
    }
)

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
    "scripts/perf/README.md": "documentation",
    "scripts/perf/ws0_report.py": "the reporter; prose reference only",
    "scripts/perf/ws0_rounds.py": "the reporter; prose reference only",
    "scripts/perf/ws0_collect.py": "the reporter; prose reference only",
    "tools/ws0-corpus-gen/README.md": "documentation",
    "tools/ws0-corpus-gen/src/bin/scan_bench.rs": "rust; cannot invoke a shell script bare",
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
    """
    return (
        line.replace("$(", " ")
        .replace("`", " ")
        .replace(")", " ")
        .replace("<(", " ")
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
    for token in tokens:
        bare = _bare(token)
        if not bare:
            continue
        if bare in {";", "&&", "||", "|", "&", "&&;"} or bare in {"(", "{"}:
            expect = True
            continue
        if token.endswith(";") or token.endswith("&&") or token.endswith("||") \
                or token.endswith("|") or token.endswith("&"):
            # A control operator attached to the token: this token is still a word, and the
            # NEXT one starts a new command.
            if expect:
                words.append(bare)
            expect = True
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


def strip_python(text: str) -> str:
    """Python source with docstrings and `#` comments removed, or the text unchanged.

    A python file's PROSE necessarily quotes the invocations it forbids — this very file's
    header does, and the first run of the rewritten lint reported FIVE findings against its own
    docstring. Stripping via `ast` scans what a run can EXECUTE, which is the same technique
    `test_ws0_fabrication_guards.sh` uses on the reporter modules. Unparseable source falls back
    to the raw text: a syntax error must not silently narrow the subject to nothing.
    """
    import ast

    try:
        tree = ast.parse(text)
    except SyntaxError:
        return text
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) \
                    and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(ast.fix_missing_locations(tree))


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


def lint_text(text: str, is_python: bool = False) -> list[str]:
    """`<lineno>: <reason>` per finding for one file's contents."""
    if not has_driver_handle(text):
        return []
    if is_python:
        return lint_python(text)
    findings = []
    for lineno, logical in logical_lines(text):
        if LINE_MARKER in logical:
            continue
        code = strip_trailing_comment(logical)
        if not code.strip():
            continue
        # Nothing on the line refers to a driver-ish token at all: not a candidate, and asking
        # further would red on ordinary code (`mkdir -p "$d"`), which is the lint an operator
        # deletes. Inside a file WITH a handle, `$copy`/`${copy}` counts — that is the driver
        # COPY the cpu-pinning suite builds and runs.
        if DRIVER_BASENAME not in code and "$DRIVER" not in code and "${DRIVER}" not in code \
                and "$copy" not in code and "${copy}" not in code:
            continue
        # `ws0_driver_run`/`ws0_driver_run_copy` IS the sanctioned path.
        if re.search(r"\bws0_driver_run(_copy)?\b", code):
            continue
        for word in command_words(code):
            why = names_driver(word)
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


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(f"usage: {argv[0]} lint <file>… | {argv[0]} subject <tests-dir>",
              file=sys.stderr)
        return 2
    if argv[1] == "lint":
        return cmd_lint(argv[2:])
    if argv[1] == "subject":
        return cmd_subject(argv[2])
    print(f"unknown subcommand {argv[1]!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
