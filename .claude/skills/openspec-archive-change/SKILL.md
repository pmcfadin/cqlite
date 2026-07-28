---
name: openspec-archive-change
description: Archive a completed change in the experimental workflow. Use when the user wants to finalize and archive a change after implementation is complete.
license: MIT
compatibility: Requires openspec CLI.
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.4.1"
---

Archive a completed change in the experimental workflow.

**Input**: Optionally specify a change name. If omitted, check if it can be inferred from conversation context. If vague or ambiguous you MUST prompt for available changes.

> **`AskUserQuestion` is ATTENDED-SESSIONS-ONLY (#2666).** Every prompt below assumes a human is watching.
> In an **unattended** session (a worker/supervisor run) `AskUserQuestion` is FORBIDDEN — the session hangs
> until the log-tail watchdog pages it. Unattended, **park** instead: post ONE structured question comment
> on the issue (options + recommendation + default), add the `needs-decision` label, write a `blocked`
> marker with `reason: needs-decision`, and **EXIT**, releasing the machine. Resume only on a strictly-newer
> owner reply (a durable `resume-dont-ask` label is a standing seal that stands in for the answer).

**Steps**

1. **If no change name provided, prompt for selection**

   Run `openspec list --json` to get available changes. Use the **AskUserQuestion tool** (attended only —
   unattended, PARK per the note above) to let the user select.

   Show only active changes (not already archived).
   Include the schema used for each change if available.

   **IMPORTANT**: Do NOT guess or auto-select a change. Always let the user choose.

2. **Check artifact completion status**

   Run `openspec status --change "<name>" --json` to check artifact completion.

   Parse the JSON to understand:
   - `schemaName`: The workflow being used
   - `planningHome`, `changeRoot`, `artifactPaths`, and `actionContext`: path and scope context
   - `artifacts`: List of artifacts with their status (`done` or other)

   If status reports `actionContext.mode: "workspace-planning"`, explain that workspace archive is not supported in this slice and STOP. Do not move workspace changes into repo-local archives or edit linked repos.

   **If any artifacts are not `done`:**
   - Display warning listing incomplete artifacts
   - Use **AskUserQuestion tool** to confirm user wants to proceed (attended only — unattended, PARK per the note at the top)
   - Proceed if user confirms

3. **Check task completion status**

   Read the tasks file (typically `tasks.md`) to check for incomplete tasks.

   Count tasks marked with `- [ ]` (incomplete) vs `- [x]` (complete).

   **If incomplete tasks found:**
   - Display warning showing count of incomplete tasks
   - Use **AskUserQuestion tool** to confirm user wants to proceed (attended only — unattended, PARK per the note at the top)
   - Proceed if user confirms

   **If no tasks file exists:** Proceed without task-related warning.

4. **Assess delta spec sync state**

   Use `artifactPaths.specs.existingOutputPaths` from status JSON to check for delta specs. If none exist, proceed without sync prompt.

   **If delta specs exist:**
   - Compare each delta spec with its corresponding main spec at `openspec/specs/<capability>/spec.md`
   - Determine what changes would be applied (adds, modifications, removals, renames)
   - Show a combined summary before prompting

   **Prompt options:**
   - If changes needed: "Sync now (recommended)", "Archive without syncing"
   - If already synced: "Archive now", "Sync anyway", "Cancel"

   If user chooses sync, use Task tool (subagent_type: "general-purpose", prompt: "Use Skill tool to invoke openspec-sync-specs for change '<name>'. Delta spec analysis: <include the analyzed delta spec summary>"). Proceed to archive regardless of choice.

5. **Perform the archive — via the `openspec` CLI, NEVER a hand-rolled `mkdir`/`mv`**

   ```bash
   openspec archive "<name>" --yes
   ```

   The CLI is the only sanctioned archive path: besides moving `changeRoot` under
   `<planningHome.changesDir>/archive/YYYY-MM-DD-<name>`, it **syncs the delta spec into
   `openspec/specs/<capability>/spec.md`** — which is exactly what CQLite's definition of done requires
   (`flow-finalize` step 3 prescribes the same command). A bare `mkdir -p archive` + `mv` moves the
   directory but **silently skips the spec sync**, leaving the live capability spec stale while the change
   looks archived. Use `--skip-specs` only for a doc/infra change with no capability delta.

   If the CLI reports the target archive name already exists, resolve it as the CLI directs (rename the
   existing archive, or re-run on a different date) — do not work around it with `mv`.

6. **Display summary**

   Show archive completion summary including:
   - Change name
   - Schema that was used
   - Archive location
   - Whether specs were synced (if applicable)
   - Note about any warnings (incomplete artifacts/tasks)

**Output On Success**

```
## Archive Complete

**Change:** <change-name>
**Schema:** <schema-name>
**Archived to:** the archive path derived from `planningHome.changesDir`/YYYY-MM-DD-<name>/
**Specs:** ✓ Synced to main specs (or "No delta specs" or "Sync skipped")

All artifacts complete. All tasks complete.
```

**Guardrails**
- **Archive only via `openspec archive <name> --yes`** — never `mkdir -p archive` + `mv`, which skips the
  delta-spec sync into `openspec/specs/<capability>/spec.md` that done-ness depends on.
- **`AskUserQuestion` is attended-only (#2666)** — unattended, PARK (question comment + `needs-decision`
  label + `blocked` marker + EXIT) instead of prompting.
- Always prompt for change selection if not provided
- Use artifact graph (openspec status --json) for completion checking
- Don't block archive on warnings - just inform and confirm
- Preserve .openspec.yaml when moving to archive (it moves with the directory)
- Show clear summary of what happened
- If sync is requested, use openspec-sync-specs approach (agent-driven)
- If delta specs exist, always run the sync assessment and show the combined summary before prompting
