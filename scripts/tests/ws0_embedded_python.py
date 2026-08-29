#!/usr/bin/env python3
"""Census + extraction of the python `scripts/perf/ws0-baseline.sh` carries INSIDE itself.

Three subcommands, and the split is the same one `ws0_hermeticity_lint.py` makes for the same
reason — a DEFINITION and an ORACLE must not be the same thing:

    census <driver>        classify EVERY `python3` occurrence in the driver; fail closed on a
                           shape this file cannot classify or a block it cannot delimit
    emit <driver> <n>      print embedded block <n>'s SOURCE, exactly as the driver ships it
    compile <driver>       compile EVERY embedded block; print one finding per block that does not

Every mode prints a `#COMPLETE …` marker on success. The CALLER counts findings, so a crash
must not read as "clean" — the marker is what makes that distinguishable, and every caller
checks for it.

# WHY THIS FILE EXISTS (issue #3451)

`ws0-baseline.sh` shipped for months with SEVEN instances of a defect that makes CPython refuse to
parse: a backslash inside an f-string EXPRESSION (the tokenizer reads it as a line continuation).
They were spread across the driver's TWO multi-line embedded blocks — the session-corpus-pin step
and the CPU-pin-verification step — and both steps are fatal-on-failure, so the entire WS0
measurement path was unrunnable end to end.

Nothing caught it because the driver's python is INVISIBLE to every tool that would have:

* it is not a `.py` file, so no linter, formatter or import ever sees it;
* the hermetic self-tests all stop at `--validate-args-only`, which sits ABOVE both steps, and
  #3272 round 2 finding 14 deliberately made the accept-direction cases execute NOTHING (running
  the real driver invoked `sudo sysctl` and three cargo builds inside a gate component);
* `bash -n` parses the file as SHELL, where a python body inside `python3 -c '…'` is one opaque
  single-quoted STRING — syntactically valid whatever it contains.

So the code was PARSED BY NOTHING until an operator ran the rig.

# THE EXTRACTION IS THE POINT, and it is why this is not a copy of the two blocks

A self-test carrying its own copy of the block certifies THE COPY. The copy does not drift when
the driver does — it stays green while the shipped step is broken, which is precisely the state
this issue found. So the subject is read out of the shipped file on every run.

# FAIL-CLOSED, because an extractor that finds nothing prints like a clean driver

Every `python3` occurrence must land in a classification this file recognises. An occurrence it
cannot classify, or an embedded block whose closing delimiter it cannot find, is a FINDING naming
the driver and the line — never a silent omission from the census. The alternative posture (skip
what you do not understand) is the one that would let instance #8 ship in a shape slightly unlike
the first seven.

## The classification, and what makes it closed

A `python3` invocation can only receive code four ways: `-c`, a script path, standard input, or
`-m`. So the census keys on WHICH of those a given occurrence uses rather than on how it is
spelled:

    -c '…'        embedded, single-quoted — the shape both defective steps use. Extracted.
    <<'EOF'       embedded via a heredoc on stdin. Extracted. (None today; covered because a
                  future step written this way must not fall outside the census.)
    <path>.py     a SCRIPT FILE. Not embedded — it is an ordinary python file every other tool
                  already sees — so it is recorded and not extracted.
    (no argument) a MENTION: a `command -v`-style presence probe, or the word inside prose. It
                  carries no code, so there is nothing to compile.

Anything else — `-c` with a shape that cannot be delimited, `-m`, a bare `-`, a variable where the
code should be — is UNKNOWN, which is a finding.
"""

from __future__ import annotations

import pathlib
import re
import sys

# The word, matched only where it is a standalone token. `requires python3.` inside an `echo`
# string is not an invocation and must not be censused as an unknown shape; a trailing
# word/dot character is the discriminator.
_PY_TOKEN = re.compile(r"(?<![\w./-])python3(?![\w.])")

# The shape both defective steps use: the line ENDS with the opening quote and the body follows on
# subsequent lines, terminated by a line whose first character is the closing quote.
_OPEN_MULTILINE = re.compile(r"^-c\s+'$")
# The same thing on one line: `-c 'import time; print(…)'`.
_INLINE = re.compile(r"^-c\s+'(?P<body>(?:[^']*))'")
# A heredoc redirect: `<<'PY'`, `<<PY`, `<<-'PY'`.
_HEREDOC = re.compile(r"<<-?\s*(?P<q>['\"]?)(?P<tag>[A-Za-z_][A-Za-z0-9_]*)(?P=q)")
# A script-file argument, quoted or not, possibly carrying a shell expansion in its directory
# part: `"$HERE/ws0_report.py"`.
_SCRIPT = re.compile(r"^[\"']?[^\s\"']*\.py[\"']?(\s|$)")


class Unclassifiable(Exception):
    """A `python3` occurrence, or a block delimiter, this file will not guess about."""

    def __init__(self, lineno: int, reason: str) -> None:
        super().__init__(reason)
        self.lineno = lineno
        self.reason = reason


def _strip_comment(rest: str) -> str:
    """Drop a trailing `# …` comment from the text FOLLOWING a python3 token.

    Only where the `#` starts a word — `a#b` is not a comment in bash, and the driver's
    `# perf-lint-allow` marker is exactly the shape this must remove.
    """
    m = re.search(r"(?:^|\s)#", rest)
    return rest[: m.start()] if m else rest


def _delimit_multiline(lines: list[str], start: int) -> tuple[str, int]:
    """Body of the `-c '` block opened on `lines[start]`, plus the line its closer sits on.

    The closer is the first following line whose FIRST character is a single quote — which is how
    the driver writes it (`' "$HERE" "$CORPUS" …`). A block with no such line is a finding rather
    than a body silently truncated at end-of-file.
    """
    for i in range(start + 1, len(lines)):
        if lines[i].startswith("'"):
            return "\n".join(lines[start + 1 : i]) + "\n", i
    raise Unclassifiable(
        start + 1,
        "an embedded `python3 -c '` block is never closed by a line beginning with the closing"
        " quote, so its body cannot be delimited. Extracting to end-of-file would compile a"
        " truncated body and report a defect that is the extractor's.",
    )


def _delimit_heredoc(lines: list[str], start: int, tag: str) -> tuple[str, int]:
    """Body of the heredoc opened on `lines[start]`, plus the line its terminator sits on."""
    for i in range(start + 1, len(lines)):
        if lines[i].strip() == tag:
            return "\n".join(lines[start + 1 : i]) + "\n", i
    raise Unclassifiable(
        start + 1,
        f"an embedded python heredoc opened with `{tag}` is never terminated, so its body cannot"
        " be delimited.",
    )


def census(path: pathlib.Path) -> tuple[list[dict], list[dict]]:
    """Classify every `python3` occurrence. Returns (records, findings)."""
    text = path.read_text()
    lines = text.split("\n")
    records: list[dict] = []
    findings: list[dict] = []
    skip_until = -1
    for idx, line in enumerate(lines):
        if idx <= skip_until:
            continue
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue  # a whole-line shell comment carries no code
        m = _PY_TOKEN.search(line)
        if not m:
            continue
        rest = _strip_comment(line[m.end() :]).strip()
        try:
            if not rest or rest.lstrip(";&|)").strip() in ("", "do", "then"):
                # A presence probe (`for tool in perf taskset python3; do`) or a bare mention:
                # no argument can carry code.
                records.append({"kind": "MENTION", "line": idx + 1, "text": line.strip()})
                continue
            hd = _HEREDOC.search(rest)
            if hd:
                body, end = _delimit_heredoc(lines, idx, hd.group("tag"))
                skip_until = end
                records.append(
                    {"kind": "BLOCK", "shape": "heredoc", "line": idx + 1, "end": end + 1,
                     "body": body}
                )
                continue
            if _OPEN_MULTILINE.match(rest):
                body, end = _delimit_multiline(lines, idx)
                skip_until = end
                records.append(
                    {"kind": "BLOCK", "shape": "dash-c-multiline", "line": idx + 1,
                     "end": end + 1, "body": body}
                )
                continue
            inline = _INLINE.match(rest)
            if inline:
                records.append(
                    {"kind": "BLOCK", "shape": "dash-c-inline", "line": idx + 1,
                     "end": idx + 1, "body": inline.group("body") + "\n"}
                )
                continue
            if _SCRIPT.match(rest):
                records.append({"kind": "SCRIPT", "line": idx + 1, "text": rest})
                continue
            raise Unclassifiable(
                idx + 1,
                "this `python3` invocation is in a shape the census does not recognise"
                f" ({rest[:60]!r}). It may be carrying embedded code the compile check would"
                " therefore never see, so it is a finding rather than a skip.",
            )
        except Unclassifiable as exc:
            findings.append({"line": exc.lineno, "reason": exc.reason})
    return records, findings


def _blocks(records: list[dict]) -> list[dict]:
    return [r for r in records if r["kind"] == "BLOCK"]


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__.splitlines()[0], file=sys.stderr)
        print("usage: ws0_embedded_python.py census|compile|emit <driver> [n]", file=sys.stderr)
        return 2
    mode, driver = argv[1], pathlib.Path(argv[2])
    if not driver.is_file():
        print(f"{driver}:0: the driver is not a readable file, so the census has NO SUBJECT —"
              " which prints exactly like a driver with nothing wrong in it.")
        return 0
    records, findings = census(driver)
    blocks = _blocks(records)
    if mode == "census":
        for f in findings:
            print(f"{driver}:{f['line']}: {f['reason']}")
        for i, r in enumerate(blocks, start=1):
            print(f"BLOCK\t{i}\t{r['line']}\t{r['end']}\t{r['shape']}")
        for r in records:
            if r["kind"] != "BLOCK":
                print(f"{r['kind']}\t{r['line']}\t{r.get('text', '')}")
        print(f"#COMPLETE blocks={len(blocks)} findings={len(findings)}"
              f" occurrences={len(records)}")
        return 0
    if mode == "compile":
        # EVERY embedded block, so instance #8 anywhere in the driver is caught rather than only
        # the two steps #3451 repaired. A census finding is reported here too: a block the census
        # could not delimit is a block this check did not compile, and silence about it would be
        # the vacuous pass.
        for f in findings:
            print(f"{driver}:{f['line']}: {f['reason']}")
        for i, r in enumerate(blocks, start=1):
            try:
                compile(r["body"], f"{driver}:block{i}@{r['line']}", "exec")
            except SyntaxError as exc:
                print(f"{driver}:{r['line']}: embedded python block {i} ({r['shape']}) DOES NOT"
                      f" COMPILE — {type(exc).__name__}: {exc.msg} (block-relative line"
                      f" {exc.lineno}). This step is fatal-on-failure, so the driver cannot run"
                      " past it.")
        print(f"#COMPLETE compiled={len(blocks)} findings={len(findings)}")
        return 0
    if mode == "emit":
        if len(argv) < 4:
            print("usage: ws0_embedded_python.py emit <driver> <n>", file=sys.stderr)
            return 2
        n = int(argv[3])
        if not 1 <= n <= len(blocks):
            print(f"{driver}:0: block {n} was requested and the census found {len(blocks)}",
                  file=sys.stderr)
            return 3
        sys.stdout.write(blocks[n - 1]["body"])
        return 0
    print(f"unknown mode {mode!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
