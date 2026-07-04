#!/usr/bin/env python3
"""Machine-wide full-gate concurrency-cap slot daemon (issue #1825).

A tiny lock-holder process started in the background by scripts/agent-gate.sh. It
acquires ONE of N machine-wide slot lockfiles via a non-blocking ``fcntl.flock``
(macOS ships no ``flock(1)`` CLI, so the semaphore is implemented in Python, which
is already a gate dependency), signals the gate on success by creating a ready
file, then HOLDS the lock while polling the gate process's liveness. When the gate
exits -- normally OR via SIGKILL -- the daemon releases the slot and exits.

Why a separate process (not an inherited fd in the gate shell): the gate runs its
heavy children (cargo, nextest, ...) after acquiring, and any fd the gate shell
held would be INHERITED by those children, so a SIGKILL of the gate would leave
the slot locked by an orphaned cargo until it finished. This daemon opens the lock
fd AFTER it is forked, so the gate's later children never hold the lock; killing
the gate frees the slot within one poll interval regardless of orphaned children.
That is the SIGKILL-safe stale-slot-reaping guarantee.

PID-reuse caveat (accepted, low-probability trade-off): liveness is a ``kill(pid, 0)``
probe on the gate PID. If a SIGKILLed gate's PID is recycled by an UNRELATED process
before the daemon's next poll, ``_gate_alive`` returns True and the slot stays held
until that unrelated PID also exits. This only delays a slot release (never leaks one
permanently, never over-admits gates), the window is a single poll interval, and PID
reuse landing on exactly the gate PID in that window is rare -- so we accept it rather
than complicating the probe (e.g. with start-time / pidfd matching).

Contract:
  * Blocks (queues) when all N slots are busy; never fails from contention.
  * Exits 0 after a clean release; exits non-zero only on a usage/IO error before
    acquisition (the gate then disables the cap for that run, fail-open).
"""
from __future__ import annotations

import argparse
import fcntl
import os
import sys
import time


def _gate_alive(pid: int) -> bool:
    """True while the gate process is still running (signal 0 probe)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but owned by another user: still alive for our purposes.
        return True
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="agent-gate concurrency-cap slot daemon")
    ap.add_argument("--slots-dir", required=True)
    ap.add_argument("--slots", type=int, required=True)
    ap.add_argument("--gate-pid", type=int, required=True)
    ap.add_argument("--ready-file", required=True)
    ap.add_argument("--poll-secs", type=float, default=2.0)
    args = ap.parse_args()

    n = args.slots if args.slots >= 1 else 1
    poll = args.poll_secs if args.poll_secs > 0 else 2.0
    os.makedirs(args.slots_dir, exist_ok=True)

    held_fd = None
    try:
        # Acquire: sweep the N slots non-blocking until one is free, re-sweeping
        # after a short wait. Give up (without acquiring) if the gate dies while we
        # are still queued -- there is nothing left to protect.
        while held_fd is None:
            for i in range(n):
                path = os.path.join(args.slots_dir, "slot.%d" % i)
                fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    os.close(fd)
                    continue
                held_fd = fd
                break
            if held_fd is not None:
                break
            if not _gate_alive(args.gate_pid):
                # Gate vanished while we were queued; exit cleanly, nothing held.
                return 0
            time.sleep(poll)

        # Signal acquisition atomically (write-then-rename) so the gate never reads
        # a half-written ready file.
        tmp = args.ready_file + ".tmp.%d" % os.getpid()
        with open(tmp, "w") as fh:
            fh.write("ok\n")
        os.replace(tmp, args.ready_file)

        # Hold the slot until the gate exits (normal exit OR SIGKILL). The kernel
        # releases the flock automatically when this process exits and closes the fd.
        while _gate_alive(args.gate_pid):
            time.sleep(poll)
        return 0
    finally:
        if held_fd is not None:
            try:
                os.close(held_fd)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
