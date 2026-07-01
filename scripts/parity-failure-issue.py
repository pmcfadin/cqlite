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
      subsequent green run posts a "resolved" comment but does NOT auto-close.

The GitHub-touching paths sit behind explicit seams (`--open-issues-json`,
`--failures-json`) so the unit tests run with no network. This mirrors
`scripts/delivery-telemetry.py`.
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
        old_body = existing.get("body") or ""
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
        failures = load_degraded(args.summary_file)
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

    open_issues = load_open_issues(args)
    for failure in failures:
        plan = _plan_failure(failure, open_issues, args)
        _apply_plan(plan, args.dry_run)
    return 0


def load_degraded(summary_file: str | None) -> list[dict]:
    """Degraded fallback (D2): parse failing scenarios from the lane summary.

    Kept intentionally conservative — it returns whatever scenarios it can positively
    identify from a `parity_summary.md`-style file (lines beginning `FAIL <scenario_id>`),
    or an empty list. An empty list on a `failure`-concluded run is surfaced by the caller
    as an anomaly (never a silent no-op).
    """
    if not summary_file:
        return []
    path = Path(summary_file)
    if not path.exists():
        return []
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
            "workflow": "unknown",
            "test_target": "unknown",
            "component_path": "unknown",
            "failure_class": "DegradedParse",
        })
    return failures


def cmd_resolve(args) -> int:
    """Post a resolution comment on a tracked fingerprint's open issue; never close (D5)."""
    failures = []
    try:
        failures = load_failures(args.failures_json)
    except DegradedParse:
        # For resolve we need explicit fingerprints; degraded resolve is a no-op notice.
        print("::notice::parity-failure-issue: resolve had no structured failures — "
              "nothing to resolve", file=sys.stderr)
        return 0
    open_issues = load_open_issues(args)
    for failure in failures:
        fingerprint = compute_fingerprint(failure)
        existing = find_existing(open_issues, fingerprint)
        if existing is None:
            continue
        if args.dry_run:
            print("--- DRY RUN (resolve) ---")
            print(f"comment #{existing['number']}: {resolution_comment(args.run_url)}")
            continue
        _gh(["gh", "issue", "comment", str(existing["number"]),
             "--body", resolution_comment(args.run_url)])
        print(f"resolved-comment: #{existing['number']} (not closed)")
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

    rs = sub.add_parser("resolve", help="post a resolution comment (never closes)")
    common(rs)

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
