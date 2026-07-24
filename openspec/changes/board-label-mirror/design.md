# Design — enforced board→label mirror

## The status ↔ label mapping

Board Status has five options: `Backlog / Ready / In Progress / In Review / Done`. The active-work
states map 1:1 to a `status:*` label; the two terminal-ish states are handled deliberately:

| Board Status | `status:*` label the mirror sets | Rationale |
|--------------|----------------------------------|-----------|
| Ready        | `status:ready`                   | the discovery target |
| In Progress  | `status:in-progress`             | |
| In Review    | `status:in-review`               | |
| Backlog      | **none** (all `status:*` removed)| Backlog = "not dispatchable"; absence of a status:* label is the signal. Avoids inventing a `status:backlog` label nobody queries. |
| Done         | **none** (all `status:*` removed)| Done items are closed; a closed issue carries no dispatch label. |

`status:spec-review` and `status:addressing` are **sub-states of In Progress** the flow-* skills use
transiently and are NOT board Status options. Decision: the mirror owns ONLY the four board-derived
labels (`ready`, `in-progress`, `in-review`, and the none-for-Backlog/Done case). `spec-review` /
`addressing` are removed from the "mirror-owned" set — the mirror maps In Progress → `status:in-progress`
and does not touch `spec-review`/`addressing` (they remain skill-managed transient markers, OR are
retired in favor of board sub-signals — see Open decision 1). This keeps the mirror's invariant crisp:
*for every open issue, exactly the label matching its board Status is present, and no other
board-derived status label is.*

## Decision: which events fire the mirror

Projects v2 item field-changes are **not** a native GitHub Actions `on:` trigger, so we cannot fire
precisely when Status changes. The mirror therefore runs on:

- **The existing 30-min `sweep` job** (cron) — the reconciliation backstop. It already paginates
  every item and reads `fieldValueByName(name:"Status")`, so the mirror is a few lines added to a
  loop that already exists. This GUARANTEES convergence within ≤30 min regardless of what changed.
- **`workflow_dispatch`** — manual/immediate reconcile (used by the rollout + on demand).
- **`issues: [edited, labeled, unlabeled, reopened]`** — cheap low-latency correction so a
  hand-tampered label is reverted quickly rather than waiting up to 30 min. (An `issues` trigger
  fires on the issue, and the job re-reads that issue's board Status and re-asserts the label.)

We deliberately do NOT try to trigger on Status-change itself (no such event) — the cron sweep is
the correctness guarantee; the event triggers only reduce lag. This is why the label is
discovery-only: the ≤30-min worst-case lag is fine for narrowing candidates, unacceptable for
claiming (hence claim ref stays authoritative).

## Decision: single writer + idempotent force-set

The mirror is **idempotent**: for each open issue it computes the desired label from Status, then
`--add-label <desired>` (no-op if present) and `--remove-label <each other status:* label>` (no-op if
absent). Running it twice changes nothing. This makes the sweep safe to re-run and makes the
drift-detector trivially the same computation with an assert instead of a write.

**Single-writer enforcement is by convention + the detector, not a GitHub ACL** (GitHub can't
restrict who sets a label). So: flow-* skills stop writing `status:*` labels (grep-enforced in
review), and the drift-detector catches any out-of-band write on the next run by failing loud.

## Decision: drift-detector placement

The detector runs as the FINAL step of the sweep job, AFTER the mirror pass, and re-reads the
(Status, labels) for every open issue:
- If any open issue's label set violates the invariant → `::error::` + exit non-zero (red run).
- Because the mirror pass just ran, a red detector means either (a) a race during the run, or (b) a
  bug in the mirror — both worth a red. To avoid flapping on (a), the detector tolerates the same
  auto-add grace window the sweep already uses for null-Status issues.

An alternative — detector as a SEPARATE scheduled workflow — was rejected: co-locating it means it
always validates the state the mirror just wrote, with no second token/PAT to manage.

## Decision: token dependency

The mirror write needs the same `project`-scoped `PROJECTS_TOKEN` PAT the sync already requires (the
default `GITHUB_TOKEN` cannot read user Projects v2). The existing "Guard token" step already fails
loud when it's absent (#2655); the mirror + detector sit behind that same guard, so a missing token
fails the whole run loudly rather than silently skipping the mirror.

## Rollout

1. Merge the workflow change.
2. `workflow_dispatch` an immediate run → the mirror pass reconciles all open issues (fixes today's
   19-issue drift), the detector passes.
3. Land the doctrine + flow-* skill edits in the same PR so no skill re-introduces a hand-written
   label after rollout.

## Alternatives considered

- **Bidirectional sync (label ↔ Status):** rejected — two writers is exactly the drift cause; a
  loop hazard; and Status must stay the single truth.
- **`status:backlog` label for Backlog:** rejected — nothing queries it; "no status:* label = not
  dispatchable" is simpler and self-documenting.
- **Trust the label as claim authority (drop the board read):** rejected — eventually-consistent lag
  makes it unsafe for claiming; the claim ref is the only correct arbiter.

## Test strategy

- A shell test harness for the mirror/detector logic (`scripts/tests/`), driven with a STUBBED
  `gh`/GraphQL layer (like `test_worker_supervisor.sh` stubs), asserting: Ready→`status:ready` set +
  others removed; Backlog→all removed; a seeded mismatch makes the detector exit non-zero; a matching
  state makes it exit 0; idempotency (second run = no change).
- Doctrine grep test: no flow-* skill contains `add-label status:` / `remove-label status:`.
- The gate `tooling-tests` component runs the new shell test.
- Workflow YAML validated (`check-workflow-injection.sh` / actionlint if present) — no
  `${{ }}`-into-`run:` injection (roborev-lints mechanized class).
