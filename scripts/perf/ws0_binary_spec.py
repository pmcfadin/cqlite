#!/usr/bin/env python3
"""WHICH FILE was executed for each measured program — the frozen copy's own identity (#3272 F3).

# The finding

Round 12's F2 froze the measured executables: they are COPIED into a session-owned
`measured-bin/` directory and hashed AT THE DESTINATION, so a concurrent `cargo build` cannot
replace mid-session the bytes the later reps run. That part was right. **The check on it was
nominal**, and this module is the check.

The reader decided "this path is the session's own frozen copy" by asking whether the path's
PARENT DIRECTORY WAS NAMED `measured-bin`:

    if pathlib.PurePath(recorded).parent.name != MEASURED_BIN_SUBDIR: raise Invalid(...)

A directory name is not an identity. Three records satisfy that check while describing
something else entirely:

  * **ANOTHER SESSION'S COPY.** `/other-session/measured-bin/cqlite-flight` has the right
    parent name and is a real frozen copy — of a different session, of a different revision,
    possibly of a different branch. The reporter is reading THIS session dir, and nothing
    compared the two.
  * **THE WRONG EXECUTABLE.** `measured-bin/flight-loadgen` recorded under the key
    `cqlite-flight`. Right directory, right session, wrong program — so the report names one
    binary's digest as another's, which is the substitution the whole record exists to prevent.
  * **A COPY THAT IS NOT THOSE BYTES.** The record carried a digest and the frozen copy sat on
    disk beside it, and the two were never compared. A copy that was truncated, replaced, or
    written from a different source satisfied every check, because nothing read it.

Each is invisible in the report, which prints "the executables were FROZEN into the session's
own measured-bin/ directory" — a guarantee about a file the reader never identified.

# The generalisation, because this is the fourth instance of one shape

`round` (F1) and `endpoint` (F2) were verified by the weakest mechanism that could express
them, because the mechanism in reach enumerated the fields someone could name at the time. This
is the same gap one level IN: the binary spec is a five-field object, and the reader considered
three of them (`path` nominally, `sha256` and `bytes` for shape alone) while `source_path` and
`mtime_epoch` were never mentioned. A key the writer adds is then a key the reader silently
drops — the `requests_unavailable` class, in a nested object.

So the subject here is not "check the path harder": it is **every field of the spec**, and the
question F1 posed — *what is the COMPLETE thing this could compare, and does the mechanism let me
express it?* — is answered by `BINARY_SPEC_DISPOSITION`, a census of the spec's own fields with a
DISPOSITION and a REASON for each, closed in both directions:

  * a field in the record that the census does not classify is a REFUSAL, so a writer key the
    reader has never considered cannot become a second `mtime_epoch`;
  * a field the census classifies that the record omits is a REFUSAL;
  * a disposition no checker implements, or a checker claiming a disposition no field carries,
    is refused AT IMPORT — round 12's F2's own shape (the thing was done, the check was nominal),
    which is exactly what this module exists to stop recurring.

# Why the recorded path is RELATIVE

`measured-bin/<binary-name>`, never an absolute path. Three reasons, and the first is the finding:

  * A relative path is CHECKABLE against a session dir. An absolute path recorded at measurement
    time names a directory that may not exist on the host reading the report, so the reader can
    only ever inspect its SPELLING — which is how "the parent is named measured-bin" became the
    whole check. Resolved against the session dir being reported, a relative path is either the
    frozen copy or it is not.
  * The path's ONE legitimate value is derivable from what the reader already holds: the session
    dir it was asked to report, and the binary's own key in the record. So this is not a value to
    be compared against a pinned expectation (the `SESSION_BOUND_INPUTS` shape, for a value only
    the driver can know) — it is a value the reader RECONSTRUCTS and requires. Equality against a
    reconstruction admits neither another session's path nor another program's.
  * A results directory is routinely moved, archived and reviewed elsewhere. An absolute path
    breaks on the first `mv`; a relative one keeps working, and re-hashing the copies that
    travelled with it is exactly what makes the report's freeze claim re-checkable by a reviewer.
"""

from __future__ import annotations

import os
import pathlib

from ws0_session import sha256_file
from ws0_validate import Invalid, _SHA256_RE

# The SESSION-OWNED directory the measured executables are COPIED into (#3272 F2). Inside the
# session's output dir, so the copies live beside the results they produced and anyone reviewing a
# session can re-hash the exact bytes that ran.
MEASURED_BIN_SUBDIR = "measured-bin"


def frozen_relpath(name: str) -> str:
    """The ONE path a frozen copy of `name` may be recorded under, relative to the session dir.

    One spelling, used by the WRITER when it records and by the READER when it reconstructs, so the
    two cannot disagree about what a frozen path looks like. A duplicated `f"measured-bin/{name}"`
    on either side is a second thing to keep in step, and the failure mode of getting it wrong is a
    reader that refuses every record its own driver wrote.
    """
    return f"{MEASURED_BIN_SUBDIR}/{name}"


# EVERY FIELD OF A BINARY SPEC, CLASSIFIED (#3272 F3). See the module docstring for why this is a
# census rather than a longer path check. Each entry is `(DISPOSITION, REASON)`, and the reason is
# at the branch because that is the census's whole value.
BINARY_SPEC_DISPOSITION: dict[str, tuple[str, str]] = {
    # ---- SESSION-DERIVED: the reader RECONSTRUCTS the one legitimate value and requires it ----
    "path": (
        "session-derived",
        "WHICH FILE was executed. Its one legitimate value is `measured-bin/<this binary's key>`"
        " relative to the session dir being reported, so the reader reconstructs it rather than"
        " inspecting its spelling. The pre-fix check asked only whether the path's PARENT DIRECTORY"
        " WAS NAMED `measured-bin`, which admitted ANOTHER SESSION's frozen copy (a real copy, of"
        " another revision, on a path with the right parent name) and the WRONG EXECUTABLE (one"
        " program's copy recorded under another's key) — in both cases the report attributed"
        " measurements to a binary digest that is not the program that produced them (#3272 F3)",
    ),
    # ---- FROZEN-COPY-VERIFIED: shape always; compared against the COPY ON DISK when it is there --
    "sha256": (
        "frozen-copy-verified",
        "the digest that identifies the bytes that ran. Shape-checked always, and RE-DERIVED from"
        " the frozen copy whenever the copy is present in the session dir — the copies travel with"
        " the results, so a reviewer can re-hash them, and until this fix nobody did: a record"
        " could carry one digest while the copy beside it held different bytes",
    ),
    "bytes": (
        "frozen-copy-verified",
        "the frozen copy's size. Re-stat'ed alongside the digest, because a size mismatch names the"
        " defect more usefully than a digest mismatch alone (a truncated copy vs a different build)",
    ),
    # ---- SHAPE-VERIFIED: recorded provenance, checkable without the source tree ------------------
    "source_path": (
        "shape-verified",
        "WHERE the copy came from — recorded provenance about the measuring host's build tree,"
        " which is legitimately absent on a host reviewing the results, so it cannot be resolved"
        " here. Its BASENAME is checkable and is checked: a `source_path` naming a different"
        " program than the key it sits under means the freeze copied the wrong file, which is the"
        " same substitution as a wrong `path` one step earlier in the same operation",
    ),
    "mtime_epoch": (
        "shape-verified",
        "the source's write time, carried through the copy. It is the mtime-vs-HEAD staleness"
        " check's INPUT at measurement time, and that check has already run and been recorded by"
        " then, so at report time this is provenance: required, and required to be a positive"
        " epoch second, because a record whose mtime is absent or nonsensical is one whose"
        " staleness verdict was reached over a value nobody can read",
    ),
}

# The EXACT closed set of dispositions. An unrecognised one would fall through every branch of
# `check_binary_spec` and behave exactly like an unchecked field under a census claiming coverage.
SPEC_DISPOSITIONS = ("session-derived", "frozen-copy-verified", "shape-verified")

# There is deliberately NO non-verifying disposition here. F1 DELETED `required-present` from the
# loadgen census rather than leave it with zero members, because a disposition whose whole content
# is a weaker check invites the next measurement-determining field into it — and `path` is precisely
# a field that had been sitting in such a place. Every field of a binary spec is verified.


def _check_session_derived(session_dir: pathlib.Path, name: str, spec: dict) -> dict:
    """`path`: reconstructed, not inspected.

    Refuses an absolute path, a path under any other directory, and a path naming any other
    program — the three faces of the pre-fix check's single directory-name test.
    """
    recorded = spec.get("path")
    expected = frozen_relpath(name)
    if not isinstance(recorded, str) or not recorded:
        raise Invalid(
            f"{name} records no 'path', so the record cannot say WHICH FILE was executed"
        )
    if recorded != expected:
        raise Invalid(
            f"{name}'s recorded path is {recorded!r}, but the only path a frozen copy of {name} may"
            f" be recorded under is {expected!r}, relative to the session dir. This is not a"
            " stylistic requirement: the pre-fix reader accepted any path whose PARENT DIRECTORY"
            f" WAS NAMED {MEASURED_BIN_SUBDIR!r}, which admitted (a) ANOTHER SESSION's frozen copy"
            " — a real copy of another revision, possibly another branch, sitting on a path with"
            " the right parent name — and (b) THE WRONG EXECUTABLE, one program's copy recorded"
            " under another program's key. In both cases the report attributed this session's"
            " measurements to a binary digest that is not the program that produced them, while"
            " printing that the executables were frozen into THIS session's own directory (#3272"
            " F3). An ABSOLUTE path is refused for the same reason: it names a directory that need"
            " not exist on the host reading the report, so it can only ever be checked by spelling."
            " Re-run the session with the driver, which records the relative path."
        )
    return {"path": recorded, "resolved": str(session_dir / expected)}


def _check_frozen_copy(session_dir: pathlib.Path, name: str, spec: dict) -> dict:
    """`sha256` + `bytes`: shape always, and RE-DERIVED from the copy on disk when it is present.

    # Why "when present" is not the permissive branch it resembles

    A results directory is legitimately archived and reviewed without its `measured-bin/` copies
    (they are release binaries — tens of megabytes each), so requiring them would make reviewing a
    shipped results set impossible. But a check that silently does not run prints exactly like one
    that passed, so the outcome is REPORTED AFFIRMATIVELY: the caller counts how many copies were
    re-verified and states `N/M` in the record it returns. A `0/3` is then visible in the report as
    a fact about what was checked, rather than absent as an unexamined assumption.

    Note this is NOT the re-derivation F6 argued against. That argument is about `target/release`,
    which on a reviewing host describes the REVIEWING checkout's build and so would be compared
    against an unrelated artifact. The frozen copies are inside the session dir and travel with the
    results: re-hashing them compares the record against the very bytes it claims to describe,
    which is the check that makes the freeze claim re-checkable rather than merely asserted.
    """
    digest, size = spec.get("sha256"), spec.get("bytes")
    if not isinstance(digest, str) or not _SHA256_RE.match(digest):
        raise Invalid(
            f"{name}'s 'sha256' is {digest!r}, which is not 64 lowercase hex characters — a"
            " truncated digest cannot identify the program that was measured"
        )
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise Invalid(
            f"{name}'s 'bytes' is {size!r}; a zero-length binary cannot have been executed, so this"
            " record does not describe a measurement"
        )
    copy = session_dir / frozen_relpath(name)
    if not copy.is_file():
        return {"frozen_copy_present": False, "frozen_copy_verified": False}
    try:
        actual_size = copy.stat().st_size
        actual_digest = sha256_file(copy)
    except OSError as exc:
        raise Invalid(
            f"{copy} exists but could not be read ({exc}), so the frozen copy this record claims to"
            " describe cannot be compared against it. An unreadable copy is a failure rather than a"
            " skipped check: a skipped check prints exactly like a passing one."
        ) from None
    if actual_size != size:
        raise Invalid(
            f"{name}'s frozen copy at {copy} is {actual_size} bytes, but the record says {size}."
            " The record and the file beside it describe different programs, so the report's"
            " digests do not identify the bytes in this session dir (#3272 F3). A size mismatch"
            " usually means the copy was truncated or replaced after the record was written."
        )
    if actual_digest != digest:
        raise Invalid(
            f"{name}'s frozen copy at {copy} hashes to {actual_digest}, but the record says"
            f" {digest}. The copy is the SAME SIZE, so this is not a truncation: the bytes in this"
            " session dir are a DIFFERENT BUILD from the one the record identifies, and every"
            " figure attributed to that digest was produced by a program the report does not name"
            " (#3272 F3)."
        )
    return {"frozen_copy_present": True, "frozen_copy_verified": True}


def _check_shape(session_dir: pathlib.Path, name: str, spec: dict) -> dict:
    """`source_path` + `mtime_epoch`: recorded provenance, checked as far as it can be here."""
    source = spec.get("source_path")
    if not isinstance(source, str) or not source:
        raise Invalid(
            f"{name} records no 'source_path', so the record does not say where the frozen copy"
            " came from — the one fact that ties the session's own copy back to a build tree"
        )
    if pathlib.PurePath(source).name != name:
        raise Invalid(
            f"{name}'s 'source_path' is {source!r}, which names"
            f" {pathlib.PurePath(source).name!r} rather than {name!r}. The freeze copied a"
            " DIFFERENT PROGRAM into this key's slot, so the digest recorded here identifies bytes"
            " that are not the program the report attributes them to (#3272 F3)."
        )
    mtime = spec.get("mtime_epoch")
    if not isinstance(mtime, int) or isinstance(mtime, bool) or mtime <= 0:
        raise Invalid(
            f"{name}'s 'mtime_epoch' is {mtime!r}, not a positive epoch second. It is the input to"
            " the mtime-vs-HEAD staleness check performed at measurement time, so a record carrying"
            " an unreadable one is a record whose staleness verdict was reached over a value nobody"
            " can read."
        )
    return {"source_path": source, "mtime_epoch": mtime}


# WHICH CHECKER IMPLEMENTS EACH DISPOSITION, as DATA. The closure below reads this rather than a
# hand-written list, so a disposition can never be classified-but-unchecked — round 12's F2's own
# shape (the freeze was performed; the check on it was nominal), which is this module's subject.
_SPEC_CHECKERS = {
    "session-derived": _check_session_derived,
    "frozen-copy-verified": _check_frozen_copy,
    "shape-verified": _check_shape,
}

# ---- CLOSURE, ASSERTED AT IMPORT, IN BOTH DIRECTIONS ----------------------------------------
for _f, (_d, _why) in BINARY_SPEC_DISPOSITION.items():
    if _d not in SPEC_DISPOSITIONS:
        raise Invalid(
            f"the binary-spec field {_f!r} is classified {_d!r}, which is not one of"
            f" {SPEC_DISPOSITIONS}. An unrecognised disposition would fall through every branch of"
            " check_binary_spec and behave exactly like an unchecked field, under a census that"
            " claims coverage."
        )
    if not isinstance(_why, str) or len(_why.strip()) < 20:
        raise Invalid(
            f"the binary-spec field {_f!r} carries no substantive REASON ({_why!r}); the census's"
            " whole value is the reason at the branch, and an empty one classifies without"
            " explaining what a mismatch costs"
        )
del _f, _d, _why
for _d in SPEC_DISPOSITIONS:
    if _d not in _SPEC_CHECKERS:
        raise Invalid(
            f"the disposition {_d!r} is declared but NO CHECKER implements it, so a field carrying"
            " it would be classified as verified while no code compares it — the half-wired guard"
            " this module exists to make unrepresentable (#3272 F3)."
        )
    if not any(spec[0] == _d for spec in BINARY_SPEC_DISPOSITION.values()):
        raise Invalid(
            f"the disposition {_d!r} has NO MEMBER FIELD, so its checker verifies nothing while the"
            " census claims it does — a check with no subject prints exactly like a passing one."
        )
for _d in _SPEC_CHECKERS:
    if _d not in SPEC_DISPOSITIONS:
        raise Invalid(
            f"a checker claims the disposition {_d!r}, which is not one of {SPEC_DISPOSITIONS} — so"
            " no field could ever carry it and the checker is dead code"
        )
del _d


def check_binary_spec(session_dir: pathlib.Path, name: str, spec: dict) -> dict:
    """Verify EVERY field of one measured binary's spec against `BINARY_SPEC_DISPOSITION`.

    Returns the verified facts (including whether the frozen copy was re-derived). Raises `Invalid`
    naming the field and what the mismatch costs.

    The record's key set is compared against the census IN BOTH DIRECTIONS, at report time and not
    only at import: a field the WRITER adds and this reader has never considered is a refusal rather
    than a silent omission, which is what kept `mtime_epoch` and `source_path` unexamined through
    four rounds of hardening on the object they sit in.
    """
    if not isinstance(spec, dict):
        raise Invalid(
            f"{name}'s entry is a {type(spec).__name__}, not an object, so it states nothing about"
            " which file was executed"
        )
    unclassified = sorted(k for k in spec if k not in BINARY_SPEC_DISPOSITION)
    if unclassified:
        raise Invalid(
            f"{name}'s spec carries field(s) this reader does not classify:"
            f" {', '.join(unclassified)}. Every field of a binary spec must have a disposition in"
            " ws0_binary_spec.BINARY_SPEC_DISPOSITION, or a field the writer adds is one the reader"
            " silently drops — which is how `source_path` and `mtime_epoch` went unexamined through"
            " four rounds of hardening on this very object (#3272 F3). Classify it, with a reason at"
            " the branch."
        )
    absent = sorted(k for k in BINARY_SPEC_DISPOSITION if k not in spec)
    if absent:
        raise Invalid(
            f"{name}'s spec is missing {', '.join(absent)}. The reader requires every classified"
            " field, so an incomplete spec cannot leave one of them unstated while the record reads"
            " as complete."
        )
    verified: dict[str, object] = {}
    for disposition, checker in _SPEC_CHECKERS.items():
        # The checker is handed the WHOLE spec and reads its own fields. Dispatching per FIELD would
        # call `_check_frozen_copy` twice with half its subject each time — the digest and the size
        # are one comparison against one file, and splitting them would re-read that file per field.
        del disposition
        verified.update(checker(session_dir, name, spec))
    return verified


def frozen_copy_coverage(per_binary: dict[str, dict], total: int) -> tuple[int, str]:
    """How many frozen copies were RE-DERIVED, stated affirmatively (#3272 F3).

    A positive measurement, never an absence: the returned sentence says `N/M`, so a session dir
    reviewed WITHOUT its copies reports `0/M` as a fact about what was checked rather than leaving
    the check's silence to read as a pass.
    """
    verified = sum(1 for v in per_binary.values() if v.get("frozen_copy_verified"))
    if verified == total:
        detail = (
            f"all {total} frozen copies were RE-DERIVED from this session dir and match the"
            " recorded size and sha256 exactly, so the digests describe the bytes present here"
        )
    elif verified == 0:
        detail = (
            f"0 of {total} frozen copies are present in this session dir, so the recorded digests"
            " were NOT re-derived — they are the driver's own observation, made when it copied the"
            " executables in before the first rep. Nothing is claimed about the bytes beyond that"
            " record; a results dir is routinely archived without its release binaries, and this"
            " count is stated so the absence of the check is visible rather than assumed"
        )
    else:
        detail = (
            f"{verified} of {total} frozen copies are present and were RE-DERIVED (size and sha256"
            " match); the remainder are absent from this session dir and were not re-derived, so"
            " their digests rest on the driver's record alone"
        )
    return verified, detail


def executable_note(session_dir: pathlib.Path, name: str) -> str:
    """Whether the frozen copy is still executable — recorded, not required.

    Separate from `check_binary_spec` deliberately: a results dir that travelled through an archive
    format without a mode bit is a legitimate thing to review, so the mode is NOT a refusal at
    report time. The DRIVER refuses a non-executable copy at freeze time, where it is the property
    that decides whether the reps can run at all.
    """
    copy = session_dir / frozen_relpath(name)
    if not copy.is_file():
        return "absent from this session dir"
    return "executable" if os.access(copy, os.X_OK) else "present but NOT executable"
