#!/usr/bin/env python3
"""WHICH BINARIES WERE MEASURED (issue #3272 review round 10, M2).

The fourth module in this rig's identity seam, and the last input that had none:

    ws0_session.py        the CORPUS's identity   — which DATA was measured?
    ws0_schema_input.py   the SCHEMA's identity   — which SCHEMA was it read WITH?
    ws0_ticket_input.py   the TICKET's identity   — which REQUEST was measured?
    ws0_binaries.py       the BINARIES' identity  — which PROGRAMS were measured?

# The finding

`--no-build` skips the release build and accepts ANY executable already present under
`target/release`. The session manifest recorded neither the source revision nor any binary digest,
so a STALE artifact — built from a different commit, or from a working tree since changed — could
be measured and reported as a result for the current checkout, with nothing in the report able to
say otherwise.

This rig's entire output is a RATIO BETWEEN TWO BINARIES. "Which binaries were measured" is
therefore the primary provenance question, not bookkeeping: a ratio between an old `cqlite-flight`
and a current `ws0-scan-bench` is a number about two moments in the repo's history, and it is
indistinguishable in the report from a number about one.

# What is recorded, and by whom

The DRIVER records, at measurement time, having observed each binary it is about to run:

  * `source_revision` — `git rev-parse HEAD`, and `source_revision_short`;
  * `source_dirty` — whether the working tree had uncommitted changes, and the changed-path count.
    A dirty tree means the revision does NOT fully describe what was built, so it is recorded as a
    first-class fact rather than dropped;
  * `build_mode` — `built` (this session ran `cargo build --release`) or `reused` (`--no-build`);
  * per binary: `sha256`, `bytes`, `mtime_epoch`.

# WHY REPORT TIME DOES NOT RE-DERIVE THE DIGESTS — the F6 argument, applied here

Every other input's digest is re-derived at reporting. These deliberately are NOT, and the reason
is the same one that keeps the CPU-sibling verification out of the reporter (#3272 F6): a results
dir is routinely reviewed on a different host, or on the same host after a rebuild. `target/release`
at report time describes the REVIEWING checkout's build, not the session's — so re-deriving there
would compare a measurement against an unrelated artifact and would make a legitimate re-report
FAIL after any rebuild. That is the "documented path made unrunnable" defect this issue has now hit
three times (#3272 L1); it is not repeated here.

So the reporter REQUIRES the record, requires it to be COMPLETE and well-formed, and prints it. What
that closes is the SUBSTITUTION: a report can no longer be silent about which programs produced its
ratio, and the revision + digests are on the record for anyone comparing two sessions.

# ...and ONE check that IS enforced at measurement time, because it can be

A binary whose mtime PRECEDES the HEAD commit's time cannot have been built from HEAD. That is a
one-directional fact, not a heuristic: it produces no false positive (if HEAD moved and nothing was
rebuilt, the binary IS stale), and it is silent about the converse (a binary newer than HEAD may
still have been built from other sources — which is exactly why the digests are recorded too). It is
checked by the DRIVER, before the first rep, and it FAILS CLOSED. Under a build it cannot fire; under
`--no-build` it is the case the finding describes.

`--no-build` is therefore RETAINED rather than forbidden for reportable runs. It exists because
re-running a measurement without a 5-minute rebuild is the normal operator loop, and removing it
would push an operator toward editing the driver. What made it dangerous was the SILENCE, and the
silence is what is fixed.
"""

from __future__ import annotations

import json
import pathlib
import subprocess

from ws0_session import sha256_file
from ws0_validate import Invalid, _SHA256_RE

# The binaries this rig MEASURES. The bare-scan arm, the Flight server, the load generator — the
# three programs whose behaviour the reported ratio is about. Named here, once, so the driver's
# existence check and this record cannot cover different sets.
MEASURED_BINARIES = ("ws0-scan-bench", "cqlite-flight", "flight-loadgen")

# The build modes, as an EXACT closed set: `built` = this session compiled them, `reused` =
# `--no-build`. An unrecognised value is refused rather than recorded, because a mode nobody
# classified would reach the report as an unchecked claim about how the binaries came to exist.
BUILD_MODES = ("built", "reused")

BINARY_PROVENANCE = "binary-provenance.json"

# Every field the reader requires. Asserted against the writer's output at import (below), the
# pattern `PINNING_RECORD_FIELDS` and `ZERO_REQUIRED_COUNTERS` established: a field the reader
# demands and the writer never produces would surface at report time as a refusal blaming the
# session dir for a driver defect.
PROVENANCE_FIELDS = (
    "source_revision",
    "source_revision_short",
    "source_dirty",
    "source_dirty_paths",
    "build_mode",
    "binaries",
    "provenance",
)


def provenance_path(session_dir: pathlib.Path) -> pathlib.Path:
    return session_dir / BINARY_PROVENANCE


def _git(repo_root: pathlib.Path, *args: str) -> str:
    """`git <args>` in `repo_root`, or `Invalid`.

    A failure is an ERROR, never an empty string or an `unknown` placeholder: the revision that
    produced the measured binaries is either observed or it is not, and a fabricated default is the
    exact shape this issue exists to remove.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise Invalid(
            f"`git {' '.join(args)}` could not be run in {repo_root} ({exc}), so the SOURCE"
            " REVISION that produced the measured binaries cannot be recorded. This rig's output is"
            " a RATIO BETWEEN TWO BINARIES, so which revision built them is provenance rather than"
            " bookkeeping (#3272 M2)."
        ) from None
    if out.returncode != 0:
        raise Invalid(
            f"`git {' '.join(args)}` failed in {repo_root} (exit {out.returncode}):"
            f" {out.stderr.strip() or '<no stderr>'}. The source revision is not recorded as"
            " `unknown` — a value that was not observed is an error, not a default."
        )
    return out.stdout.strip()


def observe_binaries(bin_dir: pathlib.Path) -> dict:
    """Digest, size and mtime of every binary in [`MEASURED_BINARIES`].

    Absent or unreadable is `Invalid`: the driver is about to RUN these, so a binary it cannot
    identify is one whose measurement could not be attributed.
    """
    observed: dict[str, dict] = {}
    for name in MEASURED_BINARIES:
        path = bin_dir / name
        if not path.is_file():
            raise Invalid(
                f"{path} does not exist, so the program this session is about to measure cannot be"
                " identified. Build it (drop --no-build)."
            )
        try:
            stat = path.stat()
            observed[name] = {
                "path": str(path),
                "sha256": sha256_file(path),
                "bytes": stat.st_size,
                "mtime_epoch": int(stat.st_mtime),
            }
        except OSError as exc:
            raise Invalid(
                f"{path} exists but could not be measured ({exc}), so this session cannot record"
                " WHICH PROGRAM it ran."
            ) from None
    return observed


def refuse_binaries_older_than_head(repo_root: pathlib.Path, observed: dict) -> str:
    """A binary whose mtime PRECEDES the HEAD commit cannot have been built from HEAD (#3272 M2).

    One-directional and therefore not a heuristic: if HEAD moved and nothing was rebuilt the binary
    IS stale, so there is no false positive; and it says nothing about the converse — a binary newer
    than HEAD may still have been built from other sources, which is why the digests are recorded
    beside it rather than instead of it.

    Returns the note recorded in the provenance; raises on a stale binary. Under a build it cannot
    fire, because `cargo build` has just touched every artifact; under `--no-build` it is exactly
    the case the finding describes.
    """
    head_epoch_raw = _git(repo_root, "log", "-1", "--format=%ct")
    try:
        head_epoch = int(head_epoch_raw)
    except ValueError:
        raise Invalid(
            f"`git log -1 --format=%ct` returned {head_epoch_raw!r}, which is not an epoch second."
            " The HEAD commit's time is the comparison this staleness check IS, so an unparseable"
            " value is a failure rather than a skipped check (a skipped check prints exactly like a"
            " passing one)."
        ) from None
    stale = sorted(
        (name, spec)
        for name, spec in observed.items()
        if spec["mtime_epoch"] < head_epoch
    )
    if stale:
        listing = "; ".join(
            f"{n} (mtime {s['mtime_epoch']}, {head_epoch - s['mtime_epoch']}s before HEAD)"
            for n, s in stale
        )
        raise Invalid(
            "STALE BINARIES. These were last written BEFORE the HEAD commit, so they cannot have"
            f" been built from it: {listing}. HEAD was committed at epoch {head_epoch}. This rig's"
            " entire output is a RATIO BETWEEN TWO BINARIES, so measuring a stale one produces a"
            " number about two different moments in the repo's history that is indistinguishable in"
            " the report from a number about one (#3272 M2). Re-run without --no-build, or rebuild"
            " (`cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen`)."
        )
    return (
        f"every measured binary was written AFTER the HEAD commit (epoch {head_epoch}), so none can"
        " be an artifact of an earlier revision. One-directional: this cannot see a binary NEWER"
        " than HEAD that was nevertheless built from other sources, which is why the digests above"
        " are recorded"
    )


def record_binary_provenance(
    session_dir: pathlib.Path,
    bin_dir: pathlib.Path,
    repo_root: pathlib.Path,
    build_mode: str,
) -> dict:
    """Record WHICH PROGRAMS this session measured, before the first rep.

    Written by the driver. Read back by `verify_binary_provenance`.
    """
    if build_mode not in BUILD_MODES:
        raise Invalid(
            f"build_mode {build_mode!r} is not one of {BUILD_MODES}. An unclassified mode would"
            " reach the report as an unchecked claim about how the measured binaries came to exist."
        )
    observed = observe_binaries(bin_dir)
    staleness_note = refuse_binaries_older_than_head(repo_root, observed)
    revision = _git(repo_root, "rev-parse", "HEAD")
    if len(revision) != 40:
        raise Invalid(
            f"`git rev-parse HEAD` returned {revision!r}, which is not a 40-character sha — the"
            " revision that built the measured binaries could not be established"
        )
    # `--porcelain` over the whole tree: a dirty tree means the revision does NOT fully describe
    # what was built, which is a first-class fact about the measurement rather than a detail to
    # drop. Recorded, not refused: measuring a work-in-progress tree is a legitimate and common
    # thing to do — what is not legitimate is a report that does not say so.
    porcelain = _git(repo_root, "status", "--porcelain")
    dirty_paths = [ln for ln in porcelain.splitlines() if ln.strip()]
    rec = {
        "source_revision": revision,
        "source_revision_short": revision[:12],
        "source_dirty": bool(dirty_paths),
        "source_dirty_paths": len(dirty_paths),
        "build_mode": build_mode,
        "binaries": observed,
        "provenance": (
            "written BY THE DRIVER, which observed each binary immediately before running it, so"
            " it establishes what THAT driver measured on the measuring host — not an independent"
            " truth about the repository. Report time REQUIRES this record but deliberately does"
            " NOT re-derive the digests: a results dir is routinely reviewed on another host or"
            " after a rebuild, where target/release describes the REVIEWING checkout's build and a"
            " re-derivation would both compare against an unrelated artifact and make a legitimate"
            " re-report fail (the same argument that keeps the CPU-sibling verification out of the"
            f" reporter, #3272 F6). {staleness_note}"
        ),
    }
    absent = [f for f in PROVENANCE_FIELDS if f not in rec]
    if absent:
        raise Invalid(
            f"the binary-provenance record is missing {absent} — the writer and"
            " PROVENANCE_FIELDS disagree."
        )
    provenance_path(session_dir).write_text(json.dumps(rec, indent=1) + "\n")
    return rec


def describe_record(rec: dict) -> str:
    """The driver's one-line `binary pin:` summary.

    Formatted HERE rather than in the driver's inline python: the driver's job is the ORDER of
    operations, and a multi-line f-string inside a `python3 -c '…'` is also where a stray quote
    breaks a shell script (this rig's `perf_invocation_lint` treats an unresolvable command word as
    a possible perf invocation, so continuation lines there are not free).
    """
    tree = (
        f" (DIRTY tree, {rec['source_dirty_paths']} changed path(s))"
        if rec["source_dirty"]
        else " (clean tree)"
    )
    return (
        f"binary pin:   {len(rec['binaries'])} binaries at {rec['source_revision_short']}{tree},"
        f" build mode {rec['build_mode']} — digests recorded in {BINARY_PROVENANCE} BEFORE the"
        " first rep"
    )


def verify_binary_provenance(session_dir: pathlib.Path) -> dict:
    """REQUIRE the driver's record of which programs it measured (#3272 round 10, M2).

    REQUIRED, not optional. An absent record means this session dir does not say which binaries
    produced its ratio — and `--no-build` accepts any executable already under `target/release`, so
    "not recorded" genuinely covers "an artifact of an unknown revision". A check that silently does
    not run prints exactly like one that passed.
    """
    p = provenance_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {BINARY_PROVENANCE} ({p}), so it does not record WHICH"
            " BINARIES it measured. This rig's whole output is a RATIO BETWEEN TWO BINARIES, and"
            " `--no-build` accepts any executable already present under target/release — so an"
            " unrecorded session may have measured artifacts of a different revision and reported"
            " them as results for the current checkout (#3272 M2). Re-run the session with"
            " scripts/perf/ws0-baseline.sh, which records the revision, the dirty state, the build"
            " mode and every measured binary's digest before the first rep."
        )
    try:
        rec = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(rec, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(rec).__name__}")
    for field in PROVENANCE_FIELDS:
        if field not in rec:
            raise Invalid(
                f"{p} carries no {field!r} — the binary provenance is incomplete, so it cannot"
                " establish which programs this session's ratio is about"
            )
    revision = rec["source_revision"]
    if not isinstance(revision, str) or len(revision) != 40:
        raise Invalid(
            f"{p}: 'source_revision' is {revision!r}, which is not a 40-character sha. A truncated"
            " or absent revision cannot identify the source the measured binaries were built from."
        )
    if rec["build_mode"] not in BUILD_MODES:
        raise Invalid(
            f"{p}: 'build_mode' is {rec['build_mode']!r}, not one of {BUILD_MODES} — a mode nobody"
            " classified is a claim about how the binaries came to exist that nobody checked"
        )
    binaries = rec["binaries"]
    if not isinstance(binaries, dict):
        raise Invalid(f"{p}: 'binaries' must be an object, got {type(binaries).__name__}")
    # EVERY measured binary, not "at least one": a record covering two of the three programs would
    # leave the third's identity unstated while the report read as complete.
    for name in MEASURED_BINARIES:
        spec = binaries.get(name)
        if not isinstance(spec, dict):
            raise Invalid(
                f"{p} records no digest for {name}, which this rig MEASURES. A partial record"
                " leaves one program in the ratio unidentified while the report reads as complete."
            )
        digest = spec.get("sha256")
        if not isinstance(digest, str) or not _SHA256_RE.match(digest):
            raise Invalid(
                f"{p}: {name}'s 'sha256' is {digest!r}, which is not 64 lowercase hex characters —"
                " a truncated digest cannot identify the program that was measured"
            )
        if not isinstance(spec.get("bytes"), int) or spec["bytes"] <= 0:
            raise Invalid(
                f"{p}: {name}'s 'bytes' is {spec.get('bytes')!r}; a zero-length binary cannot have"
                " been executed, so this record does not describe a measurement"
            )
    unknown = sorted(k for k in binaries if k not in MEASURED_BINARIES)
    if unknown:
        raise Invalid(
            f"{p} records binaries this rig does not measure: {', '.join(unknown)}. Every recorded"
            " program must be one the report is about, or the record describes a different session."
        )
    return {
        "source_revision": revision,
        "source_revision_short": rec["source_revision_short"],
        "source_dirty": bool(rec["source_dirty"]),
        "source_dirty_paths": rec["source_dirty_paths"],
        "build_mode": rec["build_mode"],
        "binaries": binaries,
        "provenance": rec["provenance"],
        "note": (
            f"the {len(MEASURED_BINARIES)} measured binaries were identified by digest BEFORE the"
            f" first rep, at source revision {rec['source_revision_short']}"
            + (
                f" with a DIRTY working tree ({rec['source_dirty_paths']} changed path(s)), so the"
                " revision does not fully describe what was built"
                if rec["source_dirty"]
                else " with a clean working tree"
            )
            + f"; build mode {rec['build_mode']}"
        ),
    }
