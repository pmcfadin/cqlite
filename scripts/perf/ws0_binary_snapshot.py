#!/usr/bin/env python3
"""THE SNAPSHOT ITSELF — were all three copies taken from ONE BUILD? (#3272 round 21, F5).

# The finding

Round 12's F2 froze the measured executables: they are COPIED into a session-owned
`measured-bin/` directory and hashed AT THE DESTINATION, so a concurrent rebuild cannot replace
mid-session the bytes the later reps run. Round 14's F3 then verified every field of each recorded
spec against the copy on disk. Both were right about what they checked, and **neither checked the
snapshot**.

The copies are taken SEQUENTIALLY, in a loop, AFTER cargo has released its build lock. A concurrent
build — a peer agent's gate, an editor save-hook, the operator's own next branch — can replace an
artifact **BETWEEN** two copies:

    capture  ws0-scan-bench      <- build A
    copy     ws0-scan-bench      <- build A
                                     ... a concurrent `cargo build` relinks target/release ...
    copy     cqlite-flight       <- build B
    copy     flight-loadgen      <- build B

The session then measures a RATIO between a build-A scan arm and a build-B Flight arm, and the
report says nothing, because **every destination digest validates**. That is the whole point of the
finding: the pre-fix guard hashes what it WROTE, so it proves THE COPY SUCCEEDED. It does not prove
THE BYTES CAME FROM THE BUILD THIS SESSION MEASURED. The artifact is verified against itself.

# Why that is a class and not a detail

It is the shape #3042 recorded for a CQLite-written + CQLite-read round trip: **self-consistency
standing in for independence**. Both sides of the comparison make the identical mistake, so the
comparison closes and the defect is invariant to it. A per-file digest that can never disagree with
itself is #3249's hardcoded `_PERF_STATE="ok"` wearing a hash — the governing bar there is not
"the guard exists" but "the guard has been OBSERVED TO FIRE", and against a mid-snapshot
replacement the destination digest CANNOT fire.

# The fix: the source artifact's identity, captured BEFORE and re-verified AFTER

Two options were weighed. The structural one — build into a session-specific `CARGO_TARGET_DIR`,
which removes the race by construction rather than policing it — is the better fix and is NOT
implemented here, because the build invocation lives in a shell library this module cannot reach;
it stays the recommended direction if that seam is ever opened.

What is implemented is the form that fits entirely on this side of the seam:

  * `capture_source_identity` records each source artifact's sha256, size, inode, device and
    mtime_ns BEFORE the first copy. That is the SNAPSHOT's baseline.
  * each copy's destination digest is required to EQUAL its captured SOURCE digest, which is the
    tie the pre-fix code lacked: it binds the bytes that ran to an identity observed independently
    of the write that produced them.
  * `reverify_source_identity` re-reads every source AFTER the last copy and refuses if any moved.
    A replacement between two copies changes the source of an ALREADY-COPIED artifact, so it is
    caught even though that copy validated at the time.

An ERROR that refuses the session, never a warning, and it NAMES THE ARTIFACT THAT MOVED — a
session whose two arms are different builds is not a degraded measurement, it is a number about two
moments in the repository's history reported as a number about one.

# Fail CLOSED when identity cannot be established

An unreadable or vanished source is `Invalid`, never "assume unchanged". The unknown case is the
case the finding is about: `--no-build` accepts whatever is on disk, and a source that cannot be
identified is one whose copy is attributable to nothing.

# Why the mtime is not the check

`mtime_ns` and the inode are recorded and compared because they NAME the change usefully (a relink
vs an in-place rewrite), but the VERDICT rests on the digest. A build system that preserves an
mtime while changing the bytes — cargo's own behaviour is to leave a current artifact's mtime
exactly where it is — would defeat an mtime-only comparison, and this rig has already been bitten
once by assuming cargo rewrites what it builds (#3272 round 11, F1).
"""

from __future__ import annotations

import os
import pathlib
import shutil

from ws0_binary_spec import MEASURED_BIN_SUBDIR, frozen_relpath
from ws0_session import sha256_file
from ws0_validate import Invalid

# The fields of ONE captured source identity. A closed set, asserted against the capture below at
# import: an identity field the comparison never reads would be recorded and unchecked, which is
# `mtime_epoch`'s pre-F3 history one object over.
SOURCE_IDENTITY_FIELDS = ("path", "sha256", "bytes", "inode", "device", "mtime_ns")


def measured_bin_dir(session_dir: pathlib.Path) -> pathlib.Path:
    """The SESSION-OWNED directory holding the executables this session actually runs (#3272 F2)."""
    return session_dir / MEASURED_BIN_SUBDIR


def _identify(path: pathlib.Path) -> dict:
    """One source artifact's identity, or `Invalid`.

    Fails CLOSED: an absent or unreadable artifact is an error, because an identity that could not
    be established cannot later be compared, and "could not compare" must never be recorded as
    "unchanged" (#3272 F5).
    """
    try:
        stat = path.stat()
        identity = {
            "path": str(path),
            "sha256": sha256_file(path),
            "bytes": stat.st_size,
            "inode": stat.st_ino,
            "device": stat.st_dev,
            "mtime_ns": stat.st_mtime_ns,
        }
    except OSError as exc:
        raise Invalid(
            f"{path} could not be identified ({exc}), so the SOURCE ARTIFACT this session is about"
            " to copy cannot be tied to the bytes it measures. This is a refusal rather than an"
            " assumption of no change: the destination digest alone verifies only that the COPY"
            " SUCCEEDED, so with no source identity there is nothing independent to compare it"
            " against (#3272 F5)."
        ) from None
    # THE DECLARED FIELD SET IS ASSERTED AGAINST WHAT WAS ACTUALLY CAPTURED, in both directions and
    # at every call — a declared-but-uncaptured field would be one the comparison never reads (which
    # is `mtime_epoch`'s pre-F3 history one object over), and a captured-but-undeclared one is a
    # field the comparison silently drops.
    if set(identity) != set(SOURCE_IDENTITY_FIELDS):
        raise Invalid(
            f"the captured source identity for {path} carries {sorted(identity)}, but"
            f" SOURCE_IDENTITY_FIELDS declares {sorted(SOURCE_IDENTITY_FIELDS)}. A declared field"
            " nobody captures is one the snapshot comparison never reads, and a captured field"
            " nobody declared is one it silently drops (#3272 F5)."
        )
    return identity


def capture_source_identity(
    bin_dir: pathlib.Path, names: tuple[str, ...]
) -> dict[str, dict]:
    """Every source artifact's identity, BEFORE the first copy — the snapshot's baseline."""
    captured: dict[str, dict] = {}
    for name in names:
        src = bin_dir / name
        if not src.is_file():
            raise Invalid(
                f"{src} does not exist, so the program this session is about to measure cannot be"
                " copied or identified. Build it (drop --no-build)."
            )
        captured[name] = _identify(src)
    return captured


def require_copy_matches_source(name: str, captured: dict, dest_digest: str) -> None:
    """The copy's digest must equal the digest captured from its SOURCE before the copy ran.

    THIS is the comparison the pre-fix code did not make. It hashed the destination and compared it
    to nothing, so it proved the copy succeeded and left the copy attributable to no build.
    """
    if dest_digest != captured["sha256"]:
        raise Invalid(
            f"{name}: the frozen copy hashes to {dest_digest}, but the SOURCE artifact identified"
            f" immediately before the copy hashed to {captured['sha256']}"
            f" ({captured['path']}). The copy therefore did NOT receive the bytes this session"
            " identified — the source was replaced WHILE it was being read, which a"
            " destination-only digest cannot see because it validates the write against itself"
            " (#3272 F5). Re-run the session with no concurrent build against target/release."
        )


def reverify_source_identity(
    bin_dir: pathlib.Path, captured: dict[str, dict]
) -> str:
    """Re-read every source AFTER the last copy; refuse if ANY of them moved.

    The copies are taken sequentially, so a rebuild between two of them leaves the earlier copies
    validating perfectly while the session holds binaries from two different builds. Comparing each
    source against its pre-copy identity is what makes that interleaving visible: the artifact
    copied FIRST is the one whose source changed, and its own destination digest agrees with itself
    either way.

    Returns the note recorded in the provenance. Raises `Invalid` NAMING every artifact that moved.
    """
    moved: list[str] = []
    for name, before in sorted(captured.items()):
        after = _identify(bin_dir / name)
        if after["sha256"] == before["sha256"]:
            continue
        moved.append(
            f"{name} ({before['path']}): sha256 {before['sha256'][:12]} ->"
            f" {after['sha256'][:12]}, {before['bytes']} -> {after['bytes']} bytes,"
            f" inode {before['inode']} -> {after['inode']},"
            f" mtime_ns {before['mtime_ns']} -> {after['mtime_ns']}"
        )
    if moved:
        raise Invalid(
            "THE SNAPSHOT IS NOT ONE BUILD. These source artifacts were REPLACED while this"
            " session was copying the measured executables, so the copies were taken from more than"
            f" one build: {'; '.join(moved)}. The copies are made SEQUENTIALLY after cargo releases"
            " its build lock, so a concurrent rebuild between two of them leaves each destination"
            " digest validating perfectly — it hashes what it wrote, which proves the COPY"
            " succeeded and says nothing about WHICH BUILD the bytes came from (#3272 F5). This rig"
            " reports a RATIO BETWEEN TWO BINARIES, so a scan arm from one build against a Flight"
            " arm from another is a number about two moments in the repository's history that is"
            " indistinguishable in the report from a number about one. Re-run with no concurrent"
            " build against target/release (a session-specific CARGO_TARGET_DIR removes the race"
            " outright)."
        )
    return (
        f"and the SNAPSHOT was verified to be ONE BUILD (#3272 F5): each of the {len(captured)}"
        " source artifacts was identified by sha256, size, inode and mtime_ns BEFORE the first copy,"
        " every frozen copy's digest was required to EQUAL its captured SOURCE digest, and every"
        " source was RE-READ after the last copy and found unchanged. Without that, a rebuild"
        " landing BETWEEN two of the sequential copies yields a session holding binaries from two"
        " different builds while every destination digest still validates, because a"
        " destination-only digest verifies the write against itself"
    )


def freeze_measured_binaries(
    session_dir: pathlib.Path, bin_dir: pathlib.Path, names: tuple[str, ...]
) -> tuple[dict, str]:
    """COPY the measured executables into the session dir, and digest THE COPIES (#3272 F2/F5).

    # Why COPY rather than re-verify per invocation

    The digests were recorded ONCE, before a session that legitimately runs for many minutes, while
    every rep executed the binaries directly from `target/release`. A rebuild in another terminal
    REPLACES those files mid-session, so the reps after it measure DIFFERENT PROGRAMS and the report
    attributes every one of them to the digests taken before the first rep.

    Re-hashing before each of ~14 invocations narrows that window to milliseconds without closing it
    — the exec happens after the hash — and costs a full re-read of three binaries per rep inside
    the measurement loop, on the machine whose page cache and CPU the rig is measuring. Copying
    removes the race instead of narrowing it: after this returns, the paths the driver executes are
    inside the session's own output directory and nothing outside the session writes there.

    # ...and the SNAPSHOT is verified to be ONE BUILD (#3272 F5)

    The copies are sequential, so freezing alone does not establish that all three came from the
    same build. Each source is identified BEFORE the first copy, each copy's digest is required to
    equal its captured source digest, and every source is re-read after the last copy. See this
    module's docstring for why a destination-only digest cannot see that interleaving.

    Copied with `shutil.copy2` (mode preserved — they must stay executable) and hashed AT THE
    DESTINATION as well as compared against the capture: hashing only the source would leave the
    record describing bytes the copy might not have received, and hashing only the destination is
    the circular check this fix removes.

    Returns `(observed record for the COPIES, the snapshot note)`. Raises `Invalid` on any failure —
    a session that cannot own its executables must not measure with borrowed ones.
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
    # BEFORE the first copy — the baseline the copies are tied to.
    captured = capture_source_identity(bin_dir, names)
    observed: dict[str, dict] = {}
    for name in names:
        src, dst = bin_dir / name, dest_dir / name
        try:
            # copy2 preserves the mode, so the copy stays executable; the source's mtime comes with
            # it, which is what keeps the staleness check meaningful about the BUILD rather than
            # about when this copy happened.
            shutil.copy2(src, dst)
            stat = dst.stat()
            digest = sha256_file(dst)
        except OSError as exc:
            raise Invalid(
                f"{src} could not be copied to {dst} ({exc}), so this session cannot execute an"
                " immutable copy of the program it measures (#3272 F2)."
            ) from None
        # The copy is tied to an identity observed INDEPENDENTLY of the write that produced it.
        require_copy_matches_source(name, captured[name], digest)
        observed[name] = {
            # The path RECORDED is the one that will be EXECUTED — the session-owned copy, not the
            # target/release source. A record naming a path the session did not run is the
            # substitution F2 exists to close.
            #
            # RELATIVE to the session dir (#3272 F3), from the ONE spelling `frozen_relpath` owns,
            # because that is what makes it CHECKABLE: the reader reconstructs it from the session
            # dir it was asked to report and the binary's own key, so neither another session's
            # frozen copy nor another program's can satisfy it. An absolute path could only ever be
            # checked by spelling, and the pre-fix reader checked exactly that — whether the parent
            # directory happened to be NAMED `measured-bin`.
            "path": frozen_relpath(name),
            "source_path": str(src),
            "sha256": digest,
            "bytes": stat.st_size,
            "mtime_epoch": int(stat.st_mtime),
        }
        if not os.access(dst, os.X_OK):
            raise Invalid(
                f"{dst} is not executable after copying, so the driver could not run it. The copy"
                " preserves the source's mode, so this means the source was not executable either."
            )
    # AFTER the last copy — a replacement between two copies is caught here, where it is visible,
    # rather than at the destination digest of the copy it did not touch.
    snapshot_note = reverify_source_identity(bin_dir, captured)
    return observed, snapshot_note
