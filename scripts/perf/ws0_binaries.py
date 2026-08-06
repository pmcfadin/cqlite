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

# ...and ONE check that IS enforced at measurement time, SCOPED TO `reused` MODE

A binary whose mtime PRECEDES the HEAD commit's time cannot have been built from HEAD — but that is
only true of a binary NOBODY JUST BUILT, and the check applies to `reused` (`--no-build`) mode ALONE.

## WHY THE UNSCOPED VERSION BROKE THE NORMAL MEASUREMENT COMMAND (#3272 review round 11, F1)

Round 10 asserted "under a build it cannot fire, because `cargo build` has just touched every
artifact." That is FALSE, and it is false for cargo's central design reason: **cargo does not rewrite
an artifact that is already current.** A successful `cargo build --release` whose inputs have not
changed relinks nothing and leaves every mtime exactly where it was.

So the ordinary sequence — commit a change to a script or a doc, then run the rig — produces:

    HEAD commit time  = now
    binary mtime      = whenever the rust last changed, possibly days earlier
    `cargo build`     = exits 0, having rewritten nothing

and the driver REFUSED, telling the operator to "re-run without --no-build" when they had not passed
`--no-build` and a build had just succeeded. **That is the third time on this issue that a guard has
made a documented command unrunnable** (round 9's F1 broke `--verify-against`, round 10's L1 broke
the digest-oracle command), and the class is now the issue's dominant defect: a guard's REJECT
direction gets a test and its ACCEPT direction gets none.

## WHY SCOPING IS THE RIGHT FIX, AND NOT A WEAKENING

Under `built` the premise the check rests on is supplied by something stronger: `cargo build`
RAN AND SUCCEEDED in this process, on this checkout, immediately before. Cargo's own staleness
tracking is the authority on whether an artifact matches its sources, and it is a far better one
than an mtime comparison against a commit timestamp — which cannot see a source change that was
never committed, and fires on a commit that touched no source at all.

Under `reused` there is no such authority: `--no-build` accepts whatever is on disk, which may be
an artifact of an entirely different revision. That is the case the finding was about, and the check
still fails closed there.

The alternative considered and rejected: force an isolated clean build whenever `built` is claimed.
That would make the normal command correct at the price of a multi-minute rebuild every run, which
pushes an operator toward `--no-build` — i.e. it would drive traffic into the one mode that has no
build-side authority at all. Narrower is better here.

Either way the DIGESTS are recorded, so what was actually measured is on the record in both modes;
and the note the record carries states WHICH of the two regimes applied, so a reader can never
mistake "the check did not apply" for "the check passed."

`--no-build` is therefore RETAINED rather than forbidden for reportable runs. It exists because
re-running a measurement without a 5-minute rebuild is the normal operator loop, and removing it
would push an operator toward editing the driver. What made it dangerous was the SILENCE, and the
silence is what is fixed.

# ROUND 12, F1 — IN `reused` MODE THE SOURCE REVISION IS `UNKNOWN`, NOT `HEAD`

The staleness check above is one-directional and says so. What was NOT fixed with it is the
ATTRIBUTION: this writer recorded `source_revision = git rev-parse HEAD` in BOTH modes, so a reused
binary — an artifact `--no-build` accepted off the disk, possibly built on another branch, in another
worktree, or from a tree since changed — was RECORDED AND REPORTED as belonging to the current
checkout's HEAD. A newer mtime establishes that the binary was WRITTEN after that commit; it
establishes nothing whatever about WHICH REVISION produced it.

That is precisely the FABRICATED-VALUE class this issue's AC3 exists to remove — a value recorded
without having been observed — and it is the same shape as a counter defaulting to 0, one field over.
It is arguably worse here, because the fabricated value is CONFIDENT and PLAUSIBLE: nothing in the
report distinguishes it from a revision that was genuinely established.

## The fix: an honest UNKNOWN, in the manifest AND in the report

Under `reused`, `source_revision` is the sentinel [`REVISION_UNKNOWN`] and `source_revision_observed`
is `False`. The report prints `source revision UNKNOWN (reused binaries)` rather than a sha. Under
`built` the revision IS observed — `cargo build --release` ran and succeeded in this process, on this
checkout, immediately before the binaries were frozen — so it is recorded as before, with
`source_revision_observed = True`.

The `git rev-parse HEAD` value is NOT silently discarded in `reused` mode: it is recorded under the
separate, differently-named `checkout_revision_at_measurement` field, which claims only what it can
support — where THIS CHECKOUT stood while the measurement ran. That is genuinely useful context
(it is what the operator will compare against), and keeping it under a name that cannot be mistaken
for build provenance is what stops the reader inferring one from the other.

## Why an UNKNOWN and not a build-provenance sidecar

The stronger fix is to persist authoritative provenance at BUILD time — a sidecar beside each binary
recording the revision that produced it — and verify it when freezing. It is the right long-term
mechanism and it is deliberately NOT built here: a sidecar is a new artifact with its own writer, its
own reader, its own absent/stale/forged cases and its own tests, and half of it would be unverifiable
in this rig anyway (a binary built by a peer agent's `cargo build`, or by an editor save-hook, gets
no sidecar at all — so the reader would still need an UNKNOWN path for exactly the case the finding
is about). An honest UNKNOWN is therefore not a placeholder for the real fix: it is the correct
terminal state for a build this rig did not perform, and the sidecar would only narrow how often it
is reached.

An honest "unknown" is strictly better than a confident wrong value. That is this issue's whole
thesis, and it applies to the issue's own code.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
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

# THE SENTINEL FOR A REVISION THAT WAS NOT OBSERVED (#3272 round 12, F1). Recorded in `reused`
# (`--no-build`) mode, where the binaries were accepted off the disk and NOTHING establishes which
# revision produced them — a newer mtime establishes only that they were written after HEAD.
#
# A SENTINEL rather than an absent field, and rather than a sha: an absent field would be
# indistinguishable from a pre-fix record whose writer never had the concept, and a sha would be the
# fabricated value the finding is about. This string is a VERDICT, and it is spelled so that it can
# never be mistaken for one — `_SHA_RE`-shaped validation refuses it, and any reader that compares
# revisions between two sessions gets a value that cannot accidentally equal another session's.
REVISION_UNKNOWN = "UNKNOWN-reused-binaries-not-built-by-this-session"

# The SESSION-OWNED directory the measured executables are COPIED into (#3272 F2). Inside the
# session's output dir, so the copies live beside the results they produced and anyone reviewing a
# session can re-hash the exact bytes that ran.
MEASURED_BIN_SUBDIR = "measured-bin"

# Every field the reader requires. Asserted against the writer's output at import (below), the
# pattern `PINNING_RECORD_FIELDS` and `ZERO_REQUIRED_COUNTERS` established: a field the reader
# demands and the writer never produces would surface at report time as a refusal blaming the
# session dir for a driver defect.
PROVENANCE_FIELDS = (
    "source_revision",
    "source_revision_short",
    # WHETHER THE REVISION ABOVE WAS OBSERVED, as a first-class boolean (#3272 round 12, F1). A
    # reader must not have to pattern-match the sentinel to know whether it is looking at build
    # provenance or at a verdict about the absence of it — and a check that infers a state from a
    # string's SPELLING is one rename away from silently passing.
    "source_revision_observed",
    # WHERE THIS CHECKOUT STOOD while the measurement ran. Named for what it can support, and
    # deliberately NOT `source_revision`: in `reused` mode these are different facts, and merging
    # them is the finding.
    "checkout_revision_at_measurement",
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


def refuse_binaries_older_than_head(
    repo_root: pathlib.Path, observed: dict, build_mode: str
) -> str:
    """A REUSED binary whose mtime PRECEDES the HEAD commit cannot have been built from HEAD.

    SCOPED TO `reused` (#3272 review round 11, F1). Under `built` this returns immediately with a
    note saying so, because **cargo does not rewrite an already-current artifact**: a successful
    `cargo build --release` after a script- or docs-only commit relinks nothing and leaves every
    mtime earlier than HEAD, so the unscoped check REFUSED the normal measurement command — telling
    the operator to "re-run without --no-build" when they had not passed it and a build had just
    succeeded. See the module docstring for why scoping (rather than forcing a clean build) is the
    right fix, and why `built` mode's premise is supplied by something stronger.

    Under `reused` it is one-directional and therefore not a heuristic: if HEAD moved and nothing
    was rebuilt the binary IS stale, so there is no false positive; and it says nothing about the
    converse — a binary newer than HEAD may still have been built from other sources, which is why
    the digests are recorded beside it rather than instead of it.

    Returns the note recorded in the provenance; raises on a stale REUSED binary. The note names
    WHICH regime applied, so a reader can never mistake "the check did not apply" for "the check
    passed."
    """
    if build_mode not in BUILD_MODES:
        raise Invalid(
            f"build_mode {build_mode!r} is not one of {BUILD_MODES}, so the staleness check cannot"
            " decide whether it applies. An unclassified mode is refused rather than defaulted:"
            " defaulting to `built` would SKIP the check that closes --no-build's silence, and"
            " defaulting to `reused` would refuse legitimate freshly-built binaries (#3272 F1)."
        )
    if build_mode == "built":
        # NOT a skip that hides: the returned note states the regime, and the DIGESTS are recorded
        # in both modes, so what was measured is on the record either way.
        return (
            "the mtime-vs-HEAD staleness check does NOT APPLY in `built` mode: `cargo build"
            " --release` ran and succeeded in this process, and cargo does not rewrite an artifact"
            " it considers current — so an mtime earlier than HEAD is the NORMAL outcome of"
            " building after a commit that touched no rust, and refusing it made the ordinary"
            " measurement command fail (#3272 F1). Cargo's own staleness tracking is the authority"
            " here, and it is a stronger one than a commit-timestamp comparison. The check applies"
            " under `reused` (--no-build), where there is no such authority; the per-binary digests"
            " above are recorded in BOTH modes"
        )
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
        f"in `reused` (--no-build) mode, every measured binary was written AFTER the HEAD commit"
        f" (epoch {head_epoch}), so none can be an artifact of an earlier revision."
        " One-directional: this cannot see a binary NEWER than HEAD that was nevertheless built"
        " from other sources, which is why the digests above are recorded"
    )


def measured_bin_dir(session_dir: pathlib.Path) -> pathlib.Path:
    """The SESSION-OWNED directory holding the executables this session actually runs (#3272 F2)."""
    return session_dir / MEASURED_BIN_SUBDIR


def freeze_measured_binaries(session_dir: pathlib.Path, bin_dir: pathlib.Path) -> dict:
    """COPY the measured executables into the session dir, and digest THE COPIES (#3272 F2).

    # The finding

    The digests were recorded ONCE, before a session that legitimately runs for many minutes
    (`--reps 3 --temp both --arm both` is 12 reps of 45-second Flight steps plus the bare-scan legs),
    while every rep executed the binaries directly from `target/release`. A `cargo build` in another
    terminal — a peer agent's gate, an editor's save-hook, the operator's own next branch — REPLACES
    those files mid-session. The reps after the rebuild then measure DIFFERENT PROGRAMS, and the
    report attributes every one of them to the digests taken before the first rep.

    That is worse than an unrecorded provenance, because the record is confidently WRONG rather than
    absent: the ratio is between two moments in the repo's history and the report names one.

    # Why COPY rather than re-verify per invocation

    Re-hashing before each of ~14 invocations narrows the window to milliseconds but does not close
    it — the exec happens after the hash, and a replace in between is exactly the same defect with a
    smaller probability. It also costs a full re-read of three binaries per rep INSIDE the
    measurement loop, on the machine whose page cache and CPU the rig is measuring.

    Copying removes the race instead of narrowing it: after this returns, the paths the driver
    executes are inside the session's own output directory, they are the bytes that were hashed, and
    nothing outside the session writes there. A rebuild mid-session then changes `target/release`
    and cannot change what this session runs — which is also the honest thing for the report to
    claim, because the copies are still on disk beside the results for anyone who wants to re-hash
    them.

    Copied with `shutil.copy2` (mode preserved — they must stay executable) and hashed AT THE
    DESTINATION, never at the source: hashing the source and copying separately would leave the two
    reads racing each other, so the digest could describe bytes the copy did not receive.

    Returns the observed record for the COPIES. Raises `Invalid` on any failure — a session that
    cannot own its executables must not measure with borrowed ones.
    """
    dest_dir = measured_bin_dir(session_dir)
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise Invalid(
            f"could not create {dest_dir} ({exc}), so this session cannot take ownership of the"
            " executables it measures. Every rep would run from target/release, where a concurrent"
            " rebuild replaces them mid-session and the report would attribute the later reps'"
            " figures to the digests taken before the first one (#3272 F2)."
        ) from None
    observed: dict[str, dict] = {}
    for name in MEASURED_BINARIES:
        src, dst = bin_dir / name, dest_dir / name
        if not src.is_file():
            raise Invalid(
                f"{src} does not exist, so the program this session is about to measure cannot be"
                " copied or identified. Build it (drop --no-build)."
            )
        try:
            # copy2 preserves the mode, so the copy stays executable; the source's mtime comes with
            # it, which is what keeps the staleness check meaningful about the BUILD rather than
            # about when this copy happened.
            shutil.copy2(src, dst)
            stat = dst.stat()
            observed[name] = {
                # The path RECORDED is the one that will be EXECUTED — the session-owned copy, not
                # the target/release source. A record naming a path the session did not run is the
                # substitution this fix exists to close.
                "path": str(dst),
                "source_path": str(src),
                "sha256": sha256_file(dst),
                "bytes": stat.st_size,
                "mtime_epoch": int(stat.st_mtime),
            }
        except OSError as exc:
            raise Invalid(
                f"{src} could not be copied to {dst} ({exc}), so this session cannot execute an"
                " immutable copy of the program it measures (#3272 F2)."
            ) from None
        if not os.access(dst, os.X_OK):
            raise Invalid(
                f"{dst} is not executable after copying, so the driver could not run it. The copy"
                " preserves the source's mode, so this means the source was not executable either."
            )
    return observed


def record_binary_provenance(
    session_dir: pathlib.Path,
    bin_dir: pathlib.Path,
    repo_root: pathlib.Path,
    build_mode: str,
) -> dict:
    """FREEZE the measured programs into the session dir and record them, before the first rep.

    Written by the driver. Read back by `verify_binary_provenance`.
    """
    if build_mode not in BUILD_MODES:
        raise Invalid(
            f"build_mode {build_mode!r} is not one of {BUILD_MODES}. An unclassified mode would"
            " reach the report as an unchecked claim about how the measured binaries came to exist."
        )
    # FROZEN, not merely observed (#3272 F2): the executables are copied into the session dir and
    # the COPIES are hashed, so the paths the driver runs cannot be replaced by a concurrent
    # `cargo build` mid-session. The digests below therefore describe the bytes that actually ran.
    observed = freeze_measured_binaries(session_dir, bin_dir)
    # The MODE is passed, because the check applies to `reused` alone (#3272 F1) — cargo does not
    # rewrite an already-current artifact, so under `built` an mtime earlier than HEAD is the normal
    # outcome of building after a commit that touched no rust, and refusing it broke the ordinary
    # measurement command.
    staleness_note = refuse_binaries_older_than_head(repo_root, observed, build_mode)
    checkout_revision = _git(repo_root, "rev-parse", "HEAD")
    if len(checkout_revision) != 40:
        raise Invalid(
            f"`git rev-parse HEAD` returned {checkout_revision!r}, which is not a 40-character sha —"
            " the revision this checkout stood at during the measurement could not be established"
        )
    # THE SOURCE REVISION IS ONLY OBSERVED UNDER `built` (#3272 round 12, F1).
    #
    # Under `built`, `cargo build --release` ran and succeeded in THIS process, on THIS checkout,
    # immediately before the binaries were frozen — so HEAD genuinely is the revision they were built
    # from, and cargo's own staleness tracking is the authority that makes that true.
    #
    # Under `reused`, `--no-build` accepted whatever was on disk: another branch's artifact, another
    # worktree's, or one from a tree since changed. The mtime check above establishes that the binary
    # was WRITTEN after HEAD and NOTHING about which revision produced it, so recording HEAD here was
    # a value nobody observed — the fabricated-value class AC3 exists to remove, in its most
    # dangerous form, because a plausible sha is indistinguishable in the report from an established
    # one.
    observed_revision = build_mode == "built"
    revision = checkout_revision if observed_revision else REVISION_UNKNOWN
    # `--porcelain` over the whole tree: a dirty tree means the revision does NOT fully describe
    # what was built, which is a first-class fact about the measurement rather than a detail to
    # drop. Recorded, not refused: measuring a work-in-progress tree is a legitimate and common
    # thing to do — what is not legitimate is a report that does not say so.
    porcelain = _git(repo_root, "status", "--porcelain")
    dirty_paths = [ln for ln in porcelain.splitlines() if ln.strip()]
    rec = {
        "source_revision": revision,
        # The short form is a PREFIX of whatever the long form is, sentinel included: deriving it any
        # other way (e.g. falling back to the checkout sha) would put a real-looking 12-hex string in
        # the field a summary line prints, i.e. re-fabricate the value one layer down.
        "source_revision_short": revision[:12],
        "source_revision_observed": observed_revision,
        "checkout_revision_at_measurement": checkout_revision,
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
            f" reporter, #3272 F6). {staleness_note}."
            + (
                " THE SOURCE REVISION WAS OBSERVED: `cargo build --release` ran and succeeded in"
                " this process, on this checkout, immediately before the binaries were frozen, so"
                " HEAD is the revision they were built from"
                if observed_revision
                else " THE SOURCE REVISION IS UNKNOWN AND IS RECORDED AS SUCH (#3272 F1):"
                " `--no-build` accepted these binaries off the disk, and they may be artifacts of"
                " another branch, another worktree, or a tree since changed. The mtime check"
                " establishes only that they were WRITTEN after HEAD — never which revision"
                " produced them — so recording HEAD here would be a value nobody observed."
                " `checkout_revision_at_measurement` records where this checkout stood, which is a"
                " different fact and is named for the one it can support"
            )
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
    # THE REVISION IS NAMED ONLY WHEN IT WAS OBSERVED (#3272 round 12, F1). In `reused` mode the
    # line says UNKNOWN and names the checkout revision under its own, weaker description — so the
    # operator reading the driver's own output cannot take a reused binary for one built at HEAD.
    where = (
        f"at {rec['source_revision_short']}{tree}"
        if rec["source_revision_observed"]
        else f"at an UNKNOWN source revision{tree} — --no-build accepted them off the disk, so"
        " which revision BUILT them is not established (checkout was at"
        f" {rec['checkout_revision_at_measurement'][:12]} during the measurement)"
    )
    return (
        f"binary pin:   {len(rec['binaries'])} binaries {where},"
        f" build mode {rec['build_mode']} — digests recorded in {BINARY_PROVENANCE} BEFORE the"
        f" first rep, and the executables FROZEN into {MEASURED_BIN_SUBDIR}/ (a concurrent rebuild"
        " of target/release cannot change what this session runs, #3272 F2)"
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
    # THE REVISION'S SHAPE IS DECIDED BY `source_revision_observed`, NOT GUESSED (#3272 round 12, F1).
    #
    # `reused` sessions legitimately carry the UNKNOWN sentinel, so a flat "must be 40 hex" would
    # refuse every `--no-build` session — the "a guard made a documented command unrunnable" defect
    # this issue has now hit three times. But the two states must each be checked STRICTLY, keyed on
    # the AFFIRMATIVE boolean rather than on the string's spelling: a reader that accepted "either a
    # sha or the sentinel" would let a record claim `observed=True` beside the sentinel, or
    # `observed=False` beside a real sha, i.e. exactly the conflation the fix removes.
    revision = rec["source_revision"]
    observed = rec["source_revision_observed"]
    if not isinstance(observed, bool):
        raise Invalid(
            f"{p}: 'source_revision_observed' is {observed!r}, not a boolean. It decides whether"
            " 'source_revision' is build provenance or a verdict about the absence of it, so a"
            " non-boolean leaves the record's central claim unclassified (#3272 F1)."
        )
    if observed:
        if not isinstance(revision, str) or len(revision) != 40:
            raise Invalid(
                f"{p}: 'source_revision' is {revision!r}, which is not a 40-character sha, while"
                " 'source_revision_observed' is true. A record claiming the revision WAS observed"
                " must carry it."
            )
    elif revision != REVISION_UNKNOWN:
        raise Invalid(
            f"{p}: 'source_revision_observed' is false but 'source_revision' is {revision!r}, not"
            f" the {REVISION_UNKNOWN!r} sentinel. A revision nobody observed may not be recorded as"
            " a value: `--no-build` accepts binaries off the disk and a newer mtime establishes only"
            " that they were WRITTEN after HEAD, never which revision produced them — so a sha here"
            " is the fabricated value AC3 exists to remove, in its most dangerous form, because it"
            " is indistinguishable in the report from an established one (#3272 F1)."
        )
    # ...and the CHECKOUT revision is required in BOTH modes, always as a real sha: it is a fact the
    # driver can always observe (where this checkout stood while measuring), so an absent or
    # sentinel value there would mean the record dropped something it had.
    checkout_rev = rec["checkout_revision_at_measurement"]
    if not isinstance(checkout_rev, str) or len(checkout_rev) != 40:
        raise Invalid(
            f"{p}: 'checkout_revision_at_measurement' is {checkout_rev!r}, which is not a"
            " 40-character sha. Where the checkout stood during the measurement is observable in"
            " BOTH build modes, so this is never legitimately unknown."
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
        # THE RECORDED PATH MUST BE THE SESSION'S OWN FROZEN COPY (#3272 F2).
        #
        # A record whose paths point into `target/release` describes a session that ran binaries
        # anything else on the box could replace mid-run, so its digests describe the bytes present
        # BEFORE the first rep rather than the bytes each rep executed. That is precisely the
        # attribution this fix closes, and it is invisible in a record that merely carries digests.
        #
        # Checked on the path's PARENT DIRECTORY NAME rather than on the string containing
        # `measured-bin` anywhere: a `target/release` path under a checkout that happens to live in a
        # directory called `measured-bin` would otherwise satisfy it.
        recorded = spec.get("path")
        if not isinstance(recorded, str) or not recorded:
            raise Invalid(
                f"{p}: {name} records no 'path', so the record cannot say WHICH FILE was executed"
            )
        if pathlib.PurePath(recorded).parent.name != MEASURED_BIN_SUBDIR:
            raise Invalid(
                f"{p}: {name}'s recorded path is {recorded!r}, which is not inside this session's"
                f" own {MEASURED_BIN_SUBDIR}/ directory. The measured executables are COPIED into"
                " the session dir and the copies are what the reps run, because the digests were"
                " otherwise taken once before a many-minute session while every rep executed"
                " straight out of target/release — where a concurrent `cargo build` replaces them"
                " mid-session, so the later reps measured DIFFERENT PROGRAMS and the report"
                " attributed all of them to the digests taken before the first rep (#3272 F2)."
                " Re-run the session with scripts/perf/ws0-baseline.sh, which freezes them."
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
        "source_revision_observed": observed,
        "checkout_revision_at_measurement": checkout_rev,
        "source_dirty": bool(rec["source_dirty"]),
        "source_dirty_paths": rec["source_dirty_paths"],
        "build_mode": rec["build_mode"],
        "binaries": binaries,
        "provenance": rec["provenance"],
        "note": (
            f"the {len(MEASURED_BINARIES)} measured binaries were identified by digest BEFORE the"
            f" first rep AND FROZEN into the session's own {MEASURED_BIN_SUBDIR}/ directory (so a"
            " concurrent rebuild of target/release could not change what the reps ran, #3272 F2),"
            + (
                f" at source revision {rec['source_revision_short']}"
                if observed
                else " at an UNKNOWN source revision — `--no-build` accepted these binaries off the"
                " disk, so which revision BUILT them is NOT established and is recorded as unknown"
                " rather than as this checkout's HEAD, which would be a value nobody observed"
                f" (#3272 F1); the checkout stood at {checkout_rev[:12]} during the measurement"
            )
            + (
                f", with a DIRTY working tree ({rec['source_dirty_paths']} changed path(s)), so the"
                " revision does not fully describe what was built"
                if rec["source_dirty"]
                else ", with a clean working tree"
            )
            + f"; build mode {rec['build_mode']}"
        ),
    }
