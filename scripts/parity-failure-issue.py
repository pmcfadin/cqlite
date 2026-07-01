#!/usr/bin/env python3
"""parity-failure-issue.py — turn a failed scheduled/main parity run into a deduped issue.

Signal routing only (issue #1028, epic #974). When a manifest-backed parity workflow
completes with a `failure` conclusion on a scheduled / main-branch / `workflow_dispatch`
run (NOT a pull_request), this tool computes a stable per-scenario fingerprint and
CREATES or UPDATES one deduplicated `parity-failure` tracking issue per fingerprint. It
never fixes a failure, never gates the parity result, and never auto-closes an issue.

Design (see openspec/changes/issue-1028-parity-failure-issue-automation/):
  D2  read failing scenarios from a structured `parity-failures.json`; a documented
      degraded fallback (summary/log parse) is SURFACED, never silent; zero failures
      parsed from a `failure`-concluded run is an ANOMALY, not a no-op.
  D3  fingerprint = sha256("v1|"+scenario_id+"|"+workflow+"|"+test_target+"|"+
      component_path+"|"+failure_class), each field lower-cased + trimmed; first 12 hex
      chars used in the marker/title. Run-noise (timestamps, run-ids, counts) does not
      change it.
  D4  dedup by body marker `<!-- PARITY-FAIL:<fp> -->` + `parity-failure` label; UPDATE
      the match (dated comment + refreshed run link), never open a duplicate. Warn
      loudly if the open-issue lookup hits its cap (no silent truncation).
  D5  non-gating, fail-open: absent issue-write token -> `::notice::` + exit 0; a
      subsequent green run posts a "resolved" comment (IDEMPOTENTLY — at most one per
      failure→green transition, see Fix B) but does NOT auto-close.
  R4  the `resolve` subcommand implements the green-run path: with `--failures-json` it
      resolves only the matching fingerprints; without it (the green-lane wiring) it posts
      a resolution comment on the open `parity-failure` issues belonging to the now-green
      lane — scoped by `--workflow <filename>` (the lane's `.github/workflows/<file>`,
      matched against the body's `**Workflow:**` record), never closing any. The workflow
      always passes the completed lane's filename so one green lane never comments on
      another lane's issues; an empty `--workflow` (unmapped lane) resolves nothing. The
      resolve is IDEMPOTENT (Fix B): it stamps a `<!-- PARITY-RESOLVED:... -->` body marker
      and skips any issue already carrying it, so a lane that stays green does not re-post a
      resolution comment every scheduled run; a new failure clears the marker (`clear_resolved`
      on the file path), re-arming the next green run to comment once (one per transition).

The GitHub-touching paths sit behind explicit seams (`--open-issues-json`,
`--failures-json`) so the unit tests run with no network. This mirrors
`scripts/delivery-telemetry.py`.

  Fix A  the automation self-provisions the `parity-failure` label idempotently
      (`gh label create --force`) BEFORE the list/create path, so filing on a repo that
      lacks the label is not a silent no-op. Fail-open: a label-provisioning failure is a
      `::warning::`, never a crash. Skipped entirely on the offline/dry-run path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- constants

FINGERPRINT_VERSION = "v1"
PARITY_LABEL = "parity-failure"
# Idempotent self-provisioning of the label (Fix A). `gh label create --force` upserts:
# it creates the label if missing and updates its color/description if present, so a
# fresh repo (or a repo where the label was deleted) never silently no-ops the first
# filing. Color/description are stable so repeated `--force` upserts are inert.
PARITY_LABEL_COLOR = "b60205"
PARITY_LABEL_DESCRIPTION = "Recurring Cassandra parity failure tracked by scripts/parity-failure-issue.py (issue #1028)"
PARENT_EPIC = "#974"
# No silent cap: if the open-issue lookup returns this many, dedupe may be incomplete.
OPEN_ISSUES_LIMIT = 500

# Required fields of one entry in parity-failures.json (D2).
FAILURE_FIELDS = ("scenario_id", "workflow", "test_target", "component_path", "failure_class")


# --------------------------------------------------------------------------- fingerprint

def _norm(value) -> str:
    """Lower-case + trim a field for fingerprinting (run-noise-insensitive)."""
    return str(value if value is not None else "").strip().lower()


def compute_fingerprint(failure: dict) -> str:
    """Stable versioned SHA-256 over normalized, ordered fields (D3).

    The same logical failure (differing timestamps / run-ids / counts) yields an
    identical fingerprint; a difference in any of the five ordered fields yields a
    different one. Returns the full hex digest; callers use `short_fingerprint` for the
    12-char marker/title form.
    """
    payload = "|".join([FINGERPRINT_VERSION, *[_norm(failure.get(f)) for f in FAILURE_FIELDS]])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def short_fingerprint(fingerprint: str) -> str:
    """First 12 hex chars used in the marker/title (D3)."""
    return fingerprint[:12]


def marker(fingerprint: str) -> str:
    """Body marker embedded in every tracking issue (D4)."""
    return f"<!-- PARITY-FAIL:{short_fingerprint(fingerprint)} -->"


# A resolved-state marker written onto the issue body when a green run resolves it, so a
# later green run is IDEMPOTENT (Fix B, round 7): the resolution comment is posted at most
# once per failure→green transition. A subsequent failure recurrence CLEARS this marker
# (via `clear_resolved`) so the next green run comments again — one comment per transition.
RESOLVED_MARKER_PREFIX = "<!-- PARITY-RESOLVED:"
RESOLVED_MARKER_SUFFIX = " -->"


def resolved_marker(run_url: str) -> str:
    """Body marker recording that this issue has been resolved on `run_url` (Fix B)."""
    return f"{RESOLVED_MARKER_PREFIX}{run_url}{RESOLVED_MARKER_SUFFIX}"


def is_resolved(body: str) -> bool:
    """True if the issue body already carries a resolved marker (Fix B idempotency)."""
    return RESOLVED_MARKER_PREFIX in (body or "")


def set_resolved(body: str, run_url: str) -> str:
    """Return `body` with a single up-to-date resolved marker (Fix B).

    Any pre-existing resolved marker is replaced so the body never accumulates stale
    resolution markers across (re-armed) transitions.
    """
    cleared = clear_resolved(body)
    sep = "" if not cleared or cleared.endswith("\n") else "\n"
    return f"{cleared}{sep}{resolved_marker(run_url)}\n"


def clear_resolved(body: str) -> str:
    """Strip any resolved marker line(s) from `body` (Fix B: re-arm on recurrence).

    Called on the failure path so a new failure after a resolve flips the issue back to
    the unresolved state — the next green run then posts exactly one fresh resolution
    comment (one comment per failure→green transition).
    """
    if not body or RESOLVED_MARKER_PREFIX not in body:
        return body or ""
    kept = [
        line for line in body.splitlines()
        if not line.strip().startswith(RESOLVED_MARKER_PREFIX)
    ]
    result = "\n".join(kept)
    # Preserve a trailing newline if the original body had one.
    if body.endswith("\n") and not result.endswith("\n"):
        result += "\n"
    return result


# --------------------------------------------------------------------------- failure input

class DegradedParse(Exception):
    """Raised when the structured artifact is absent so the caller surfaces a notice."""


def load_failures(failures_json: str | None) -> list[dict]:
    """Load + validate the structured parity-failures.json (D2).

    Raises `DegradedParse` (no structured artifact) or `SystemExit` (malformed artifact).
    A malformed artifact is a hard error, never silently coerced to empty.
    """
    if not failures_json:
        raise DegradedParse("no --failures-json supplied")
    path = Path(failures_json)
    if not path.exists():
        raise DegradedParse(f"{path} does not exist")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"error: {path} is not valid JSON: {exc}")
    if not isinstance(data, list):
        raise SystemExit(f"error: {path} must be a JSON array of failure objects")
    failures: list[dict] = []
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise SystemExit(f"error: {path}[{i}] is not an object")
        missing = [f for f in FAILURE_FIELDS if f not in entry]
        if missing:
            raise SystemExit(
                f"error: {path}[{i}] is missing required field(s): {', '.join(missing)}")
        failures.append({f: entry[f] for f in FAILURE_FIELDS})
    return failures


# --------------------------------------------------------------------------- gh seam

def _gh(argv: list[str]) -> str:
    """Run a gh command, returning stdout. Failures raise SystemExit (fail loud)."""
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        raise SystemExit("error: `gh` not found on PATH")
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        raise SystemExit(f"error: `{' '.join(argv)}` failed: {detail}")
    return proc.stdout


def ensure_label() -> None:
    """Idempotently self-provision the `parity-failure` label before filing (Fix A).

    `gh issue create --label parity-failure` FAILS if the label does not exist in the
    repo — a silent production no-op on first use (the workflow's `|| true` fail-open
    only logs a warning and files no tracker). We upsert the label first via
    `gh label create --force`, which creates it if missing and updates it if present, so
    the real filing path is always guaranteed a valid label.

    This performs a live `gh` call, so callers MUST only invoke it on the network-bearing
    path (never in `--dry-run`/offline mode). Fail-open: if label
    provisioning itself fails, surface a `::warning::` and continue rather than crashing
    the (non-gating) job — a genuinely missing label then fails loudly at `issue create`,
    which the workflow already handles fail-open.
    """
    try:
        _gh(["gh", "label", "create", PARITY_LABEL, "--force",
             "--color", PARITY_LABEL_COLOR, "--description", PARITY_LABEL_DESCRIPTION])
    except SystemExit as exc:
        print(f"::warning::parity-failure-issue: could not provision the '{PARITY_LABEL}' "
              f"label ({exc}) — continuing (issue create may fail fail-open)", file=sys.stderr)


def load_open_issues(args) -> list[dict]:
    """Open `parity-failure` issues, from the injected seam or live `gh` (D4)."""
    if args.open_issues_json:
        return json.loads(Path(args.open_issues_json).read_text())
    issues = json.loads(_gh(
        ["gh", "issue", "list", "--label", PARITY_LABEL, "--state", "open",
         "--json", "number,body", "--limit", str(OPEN_ISSUES_LIMIT)]))
    if len(issues) >= OPEN_ISSUES_LIMIT:
        print(f"warning: open {PARITY_LABEL} lookup hit the {OPEN_ISSUES_LIMIT}-issue cap — "
              f"dedupe may miss an existing tracker beyond it", file=sys.stderr)
    return issues


def find_existing(open_issues: list[dict], fingerprint: str) -> dict | None:
    """Substring-match the body marker against open issues (D4)."""
    mk = marker(fingerprint)
    for issue in open_issues:
        if mk in (issue.get("body") or ""):
            return issue
    return None


# --------------------------------------------------------------------------- issue text

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def issue_title(failure: dict, fingerprint: str) -> str:
    return (f"parity-failure: {failure['scenario_id']} "
            f"({short_fingerprint(fingerprint)})")


def issue_body(failure: dict, fingerprint: str, run_url: str, tier: str, repro: str) -> str:
    """Full tracking-issue body (D4): epic ref, scenario, tier, links, repro, summary."""
    return (
        f"{marker(fingerprint)}\n\n"
        f"Tracking recurring parity failure for epic {PARENT_EPIC}.\n\n"
        f"## Failing scenario\n"
        f"- **Scenario ID:** `{failure['scenario_id']}`\n"
        f"- **Workflow:** `{failure['workflow']}`\n"
        f"- **Test target:** `{failure['test_target']}`\n"
        f"- **Component / fixture path:** `{failure['component_path']}`\n"
        f"- **Failure class:** `{failure['failure_class']}`\n"
        f"- **CI tier:** `{tier}`\n"
        f"- **Fingerprint:** `{FINGERPRINT_VERSION}:{fingerprint}`\n\n"
        f"{latest_failure_section(run_url)}\n"
        f"## Reproduction\n"
        f"```\n{repro}\n```\n\n"
        f"_Filed by `scripts/parity-failure-issue.py` (issue #1028). "
        f"Not auto-closed — close manually once resolved._\n"
    )


def update_comment(failure: dict, run_url: str) -> str:
    return (
        f"Recurred on run {run_url} at {_now()} "
        f"(`{failure['failure_class']}` on `{failure['test_target']}`)."
    )


def latest_failure_section(run_url: str) -> str:
    """The `## Latest failure` block (run link + observed timestamp).

    Emitted verbatim into a new body and used to refresh an existing body on a
    repeat failure (D4: "refresh the latest-run link on that existing issue").
    """
    return (
        f"## Latest failure\n"
        f"- **Run:** {run_url}\n"
        f"- **Observed:** {_now()}\n"
    )


def refresh_latest_failure(body: str, run_url: str) -> str:
    """Replace the `## Latest failure` section of an existing issue body (D4).

    The marker, epic/scenario/tier and reproduction sections are left intact; only
    the run link + observed timestamp under `## Latest failure` are rewritten. If no
    such section exists (older/hand-edited body) the fresh section is appended so the
    latest run link is never lost.
    """
    new_section = latest_failure_section(run_url)
    head = "## Latest failure\n"
    start = body.find(head)
    if start == -1:
        sep = "" if body.endswith("\n") else "\n"
        return f"{body}{sep}\n{new_section}"
    # The section runs until the next `## ` heading (or end of body).
    rest = body.find("\n## ", start + len(head))
    if rest == -1:
        return body[:start] + new_section
    return body[:start] + new_section + body[rest + 1:]


def resolution_comment(run_url: str) -> str:
    return (
        f"This fingerprint's lane completed green on run {run_url} at {_now()}. "
        f"Not auto-closed (issue #1028 policy) — close manually once confirmed resolved."
    )


# --------------------------------------------------------------------------- commands

def _plan_failure(failure: dict, open_issues: list[dict], args) -> dict:
    """Compute the create-or-update plan for one failing scenario (pure, testable)."""
    fingerprint = compute_fingerprint(failure)
    existing = find_existing(open_issues, fingerprint)
    if existing is not None:
        # D4: refresh the body's "Latest failure" section (run link + timestamp) on the
        # existing issue, THEN post the dated recurrence comment. Marker + all other
        # sections stay intact.
        # Fix B: a recurrence RE-ARMS the resolved state — strip any resolved marker so the
        # next green run posts exactly one fresh resolution comment (one per transition).
        old_body = clear_resolved(existing.get("body") or "")
        return {
            "action": "update",
            "fingerprint": fingerprint,
            "issue_number": existing["number"],
            "new_body": refresh_latest_failure(old_body, args.run_url),
            "comment": update_comment(failure, args.run_url),
        }
    return {
        "action": "create",
        "fingerprint": fingerprint,
        "title": issue_title(failure, fingerprint),
        "body": issue_body(failure, fingerprint, args.run_url, args.tier, args.repro),
    }


def _apply_plan(plan: dict, dry_run: bool) -> None:
    if dry_run:
        print(f"--- DRY RUN ({plan['action']}) ---")
        if plan["action"] == "create":
            print(f"title: {plan['title']}")
            print(plan["body"])
        else:
            print(f"update #{plan['issue_number']}: refreshed latest-failure link")
            print(plan["new_body"])
            print(f"update #{plan['issue_number']}: {plan['comment']}")
        return
    if plan["action"] == "create":
        url = _gh(["gh", "issue", "create", "--title", plan["title"],
                   "--body", plan["body"], "--label", PARITY_LABEL])
        print(f"created: {url.strip()}")
    else:
        # Refresh the body first (latest-run link), then post the dated recurrence comment.
        _gh(["gh", "issue", "edit", str(plan["issue_number"]), "--body", plan["new_body"]])
        _gh(["gh", "issue", "comment", str(plan["issue_number"]), "--body", plan["comment"]])
        print(f"updated: #{plan['issue_number']}")


def cmd_file(args) -> int:
    """Create/update deduped issues for a failed run's scenarios (D2, D4).

    Non-gating + fail-open (D5): a missing token no-ops with success; this function is
    invoked only after the workflow's token guard, but `--dry-run` still lets the logic
    run token-free for tests.
    """
    try:
        failures = load_failures(args.failures_json)
        degraded = False
    except DegradedParse as exc:
        # D2: surface the degraded path in the run summary, never silent.
        print(f"::notice::parity-failure-issue: no structured parity-failures.json "
              f"({exc}) — degraded fallback used", file=sys.stderr)
        failures = load_degraded(args.summary_file, getattr(args, "workflow_file", None))
        degraded = True

    if degraded:
        print("degraded=true (parsed failures from summary/logs, not parity-failures.json)")

    if not failures:
        # D2: zero failures parsed from a failure-concluded run is an anomaly, not a no-op.
        if args.conclusion == "failure":
            print("::error::parity-failure-issue: run concluded 'failure' but zero failing "
                  "scenarios were parsed — surfacing anomaly (no issue filed)", file=sys.stderr)
            return 2
        print("no failing scenarios parsed — nothing to file")
        return 0

    # R1 dedup: collapse same-fingerprint entries WITHIN this run before planning, so a
    # repeated fingerprint (in parity-failures.json or a degraded parse) yields ONE plan —
    # never multiple duplicate creates in a single invocation. We keep the LAST occurrence
    # (most recent latest-run detail) and preserve first-seen order for deterministic output.
    # Cross-run dedup is unchanged: each surviving entry is still planned against the
    # already-open issues snapshot, so a pre-existing open issue still updates (never dupes).
    deduped: dict[str, dict] = {}
    for failure in failures:
        deduped[compute_fingerprint(failure)] = failure

    # Fix A: self-provision the `parity-failure` label BEFORE the list/create path so the
    # real filing path never no-ops on a repo that lacks the label. Guarded to the live
    # path only — a dry run (or an injected-issues offline preview) stays token/network-free:
    # no label call when --dry-run, and none when --open-issues-json is injected.
    if not args.dry_run and not args.open_issues_json:
        ensure_label()

    open_issues = load_open_issues(args)
    for failure in deduped.values():
        plan = _plan_failure(failure, open_issues, args)
        _apply_plan(plan, args.dry_run)
    return 0


def load_degraded(summary_file: str | None, workflow_file: str | None = None) -> list[dict]:
    """Degraded fallback (D2): parse failing scenarios from the lane summary.

    Kept intentionally conservative — it returns whatever scenarios it can positively
    identify from a `parity_summary.md`-style file (lines beginning `FAIL <scenario_id>`),
    or an empty list. An empty list on a `failure`-concluded run is surfaced by the caller
    as an anomaly (never a silent no-op).

    Lane identity is KNOWN in degraded mode: `workflow_file` is the validated lane workflow
    filename (the same value the workflow's `identity` step derives and passes as the
    `--workflow` filter for the green-run resolve). Populating the degraded record's
    `workflow` with the REAL lane filename keeps two things working that `"unknown"` broke:
      * the green-lane resolver (`resolve --workflow <lane-file>`) can match + resolve a
        degraded-filed issue (R4 for the degraded path);
      * the degraded fingerprint is stable/aligned with the lane, so cross-run dedup of
        degraded failures for the same lane is reliable.

    `test_target` / `component_path` intentionally use a STABLE, lane-level degraded
    sentinel (`"<lane-file>:degraded"`) rather than the free-floating `"unknown"`, so the
    degraded fingerprint is stable across repeated degraded runs of the same lane. This is
    consistent with the accepted lane-level v1 fidelity — finer per-scenario identity in
    degraded mode is tracked in #1345; do NOT attempt per-scenario extraction here.
    """
    if not summary_file:
        return []
    path = Path(summary_file)
    if not path.exists():
        return []
    lane = (workflow_file or "").strip()
    if not lane:
        # The lane filename should always be threaded through after the replay-validation
        # fix. If it is somehow absent, fail closed with a surfaced warning rather than
        # silently writing an unresolvable `"unknown"` identity.
        print("::warning::parity-failure-issue: degraded fallback missing the lane workflow "
              "filename — cannot file a resolvable/stable degraded issue (no issue filed)",
              file=sys.stderr)
        return []
    sentinel = f"{lane}:degraded"
    failures: list[dict] = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped.upper().startswith("FAIL "):
            continue
        scenario = stripped[len("FAIL "):].strip().split()[0] if stripped[len("FAIL "):].strip() else ""
        if not scenario:
            continue
        failures.append({
            "scenario_id": scenario,
            # Real lane filename -> matchable by `resolve --workflow <lane-file>` + stable
            # across runs (unblocks R4 on the degraded path; stabilizes cross-run dedup).
            "workflow": lane,
            # Lane-level degraded sentinels (stable across repeated degraded runs of this
            # lane). Per-scenario degraded identity is #1345 — intentionally not extracted.
            "test_target": sentinel,
            "component_path": sentinel,
            "failure_class": "DegradedParse",
        })
    return failures


def _resolve_one(existing: dict, run_url: str, dry_run: bool) -> None:
    """Post ONE resolution comment + stamp the resolved marker; NEVER close (D5 / R4).

    Fix B (idempotency): in addition to the resolution comment, the issue body is edited
    to carry a `<!-- PARITY-RESOLVED:<run_url> -->` marker. A later green run sees that
    marker and SKIPS re-commenting (see `cmd_resolve`), so we comment at most once per
    failure→green transition. A subsequent failure clears the marker (`clear_resolved` on
    the file path), re-arming the next green run to comment once again.
    """
    new_body = set_resolved(existing.get("body") or "", run_url)
    if dry_run:
        print("--- DRY RUN (resolve) ---")
        print(f"comment #{existing['number']}: {resolution_comment(run_url)}")
        print(f"edit #{existing['number']}: stamped resolved marker")
        return
    # Order matters (Fix A): post the (single) resolution COMMENT first, and stamp the
    # resolved marker on the body ONLY after that comment succeeds. If we stamped the
    # marker first and the comment then failed, a later green run would see the marker,
    # SKIP the issue as already-resolved, and the required resolution comment would never
    # be posted. By stamping only after a successful comment, a comment failure leaves the
    # marker unstamped so the next green run retries and still posts exactly one comment on
    # success. Comment + body edit only — never close (a human confirms + closes).
    _gh(["gh", "issue", "comment", str(existing["number"]),
         "--body", resolution_comment(run_url)])
    _gh(["gh", "issue", "edit", str(existing["number"]), "--body", new_body])
    print(f"resolved-comment: #{existing['number']} (not closed)")


def cmd_resolve(args) -> int:
    """Post a resolution comment on tracked open parity-failure issues; never close (D5 / R4).

    Two modes, both fail-open and non-closing:

    * **fingerprint-scoped** — when `--failures-json` supplies explicit failures, resolve
      only the issues whose fingerprint matches (precise; useful for a targeted replay).
    * **lane-scoped** (the green-run wiring, R4) — a subsequent GREEN run has no failures
      to fingerprint, so we resolve the open `parity-failure` issues tied to the now-green
      lane. Lane membership is matched by `--workflow <filename>` as a substring of the
      issue body (the body records `**Workflow:** <file>`). The green-run workflow ALWAYS
      passes the completed lane's filename, so a single green lane never comments on
      another lane's issues. An empty `--workflow` resolves ALL open parity-failure issues
      — reserved for an explicit manual full-sweep, never emitted by the green-run wiring
      (the workflow skips resolve entirely when it cannot map the lane to a filename).
      Resolution is COMMENT-only — a human confirms + closes.
    """
    open_issues = load_open_issues(args)

    # Fingerprint-scoped resolve (explicit failures provided).
    if args.failures_json:
        try:
            failures = load_failures(args.failures_json)
        except DegradedParse:
            print("::notice::parity-failure-issue: resolve had no structured failures — "
                  "nothing to resolve", file=sys.stderr)
            return 0
        # Within-run dedup (resolve-path analog of R1 on `cmd_file`): collapse
        # same-fingerprint entries BEFORE resolving, so two+ input entries sharing a
        # fingerprint that map to one open issue post exactly ONE resolution comment in
        # this invocation (not one per duplicate). Insertion-ordered by fingerprint for
        # deterministic output; cross-run idempotency (marker + re-arm) is unchanged.
        deduped: dict[str, dict] = {}
        for failure in failures:
            deduped[compute_fingerprint(failure)] = failure
        for fingerprint, _failure in deduped.items():
            existing = find_existing(open_issues, fingerprint)
            if existing is not None and not is_resolved(existing.get("body") or ""):
                # Fix B: same idempotency guard — skip an already-resolved issue.
                _resolve_one(existing, args.run_url, args.dry_run)
        return 0

    # Lane-scoped resolve (green-run path): comment on every open parity-failure issue
    # belonging to the now-green lane (or all, if no lane filter given).
    #
    # Fix B (idempotency): SKIP any issue already carrying a resolved marker. Because
    # issues are never auto-closed, a lane that stays green would otherwise post a fresh
    # resolution comment on EVERY scheduled green run (comment spam). The marker (stamped by
    # `_resolve_one`, cleared on a new failure via `clear_resolved`) makes resolution post
    # at most once per failure→green transition.
    workflow = (getattr(args, "workflow", None) or "").strip()
    resolved = 0
    skipped = 0
    for issue in open_issues:
        body = issue.get("body") or ""
        if workflow and workflow not in body:
            continue
        if is_resolved(body):
            # Already resolved with no intervening failure recurrence — no duplicate comment.
            skipped += 1
            continue
        _resolve_one(issue, args.run_url, args.dry_run)
        resolved += 1
    if resolved == 0:
        scope = f"for lane '{workflow}'" if workflow else "for any lane"
        if skipped:
            print(f"no NEW open {PARITY_LABEL} issues to resolve {scope} — "
                  f"{skipped} already marked resolved (idempotent; no duplicate comment)")
        else:
            print(f"no open {PARITY_LABEL} issues to resolve {scope} — nothing to do")
    return 0


# --------------------------------------------------------------------------- argparse

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--failures-json", default=None,
                        help="path to the structured parity-failures.json artifact")
        sp.add_argument("--summary-file", default=None,
                        help="degraded-fallback lane summary (parsed if no structured artifact)")
        sp.add_argument("--open-issues-json", default=None,
                        help="inject open parity-failure issues (test seam; else live gh)")
        sp.add_argument("--run-url", default="",
                        help="URL of the originating parity run")
        sp.add_argument("--dry-run", action="store_true",
                        help="print the plan without touching GitHub")

    fi = sub.add_parser("file", help="create/update deduped issues for a failed run")
    common(fi)
    fi.add_argument("--conclusion", default="failure",
                    help="originating run conclusion (failure|success|...)")
    fi.add_argument("--tier", default="nightly_docker", help="CI tier of the failing lane")
    fi.add_argument("--repro", default="see run logs", help="reproduction command")
    fi.add_argument("--workflow-file", dest="workflow_file", default=None,
                    help="validated lane workflow filename (e.g. tombstone-ttl-parity.yml); "
                         "used ONLY on the degraded fallback path to populate the known lane "
                         "identity so the degraded issue is resolvable by the green-run "
                         "`resolve --workflow` filter and its fingerprint is stable/aligned "
                         "with the lane. Ignored when a structured --failures-json is present.")

    rs = sub.add_parser("resolve", help="post a resolution comment (never closes)")
    common(rs)
    rs.add_argument("--workflow", default="",
                    help="lane-scope filter: only resolve issues whose body records this "
                         "workflow (substring match); empty resolves all open parity-failure "
                         "issues. Ignored when --failures-json is supplied.")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "file":
        return cmd_file(args)
    if args.cmd == "resolve":
        return cmd_resolve(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
