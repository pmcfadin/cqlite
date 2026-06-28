# Export progress reporting & final statistics (#284)

## Why

`cqlite export` (#278) streams rows to CSV/JSON/CQL/Parquet but its progress UX is
incomplete against issue #284 (epic #907, M3 CLI polish). Today it shows only an
**indeterminate spinner** with a live row count and prints a final summary
(rows / size / time / rate). It never shows a **determinate progress bar** (percent,
position/total) or an **ETA**, even when the total row count is actually known — which is
the headline acceptance criterion of #284.

This change agrees the **output contract** for export progress before touching code (the
manager flagged this as a Seam-1 decision), then fills the gap: a determinate bar + ETA
when a total is known, an explicit spinner otherwise, and a pinned CLI test that proves
progress/stats appear on a TTY and are suppressed when piped or `--quiet`.

- **Milestone:** M3 (Output Writers / CLI polish), epic #907.
- **Routing:** **Design-driven** (CLI UX / output contract) → OpenSpec front door. Not
  oracle-driven; no SSTable-format parity surface is touched.

## What changes

1. **Determinate progress bar when a total is known.** When the export's total row count is
   known up front, render a determinate `indicatif` bar showing percent, `pos/len`, and a
   live ETA (in place of today's always-spinner).
2. **Define "known total" authoritatively (no heuristics).** The total is known **only**
   when the user supplies an explicit `--limit N` (the bounded ceiling of rows to export).
   For an un-limited full-table export, a `WHERE`-filtered export, or a free `SELECT`, the
   total is **not** known and the export uses the existing indeterminate spinner — no
   fabricated total, no fake ETA. (See `design.md` for the rejected Statistics.db-estimate
   alternative.)
3. **Spinner for indeterminate progress (unchanged behavior, made explicit in the spec).**
4. **Final statistics (unchanged behavior, made explicit in the spec):** rows, byte size,
   duration, rate.
5. **Suppression contract (made explicit + pinned by a test):** when `--quiet` is set **or**
   stdout is not a TTY (piped/redirected), emit **no** progress and **no** summary — only
   the export file is produced. This matches existing CLI status-line behavior.
6. **Wiring-evidence test:** a CLI-level test that exercises the public `export` command and
   asserts the summary contract (and its suppression), plus a unit test for the
   total-resolution + ETA-eligibility decision.

## Non-goals

- **No row pre-count pass.** We will not scan the data twice to learn a total; that doubles
  I/O for a progress cosmetic and is out of scope.
- **No Statistics.db / metadata row-count estimate** as a progress total in this change
  (rejected in `design.md`; may be revisited as a follow-up behind a clearly-labeled
  "estimated" bar).
- **No new CLI flags.** Reuse `--quiet` and the existing `--limit`; no `--progress`/`--no-progress`
  toggle is added here.
- **No change to export formats, the streaming engine, or the writer trait surface.**
- **No progress UX for bindings (Python/Node)** — CLI only.

## Doctrine impact

- No-heuristics mandate: respected — the only authoritative "total" is the user-supplied
  `--limit`; no inference from data shape. Documented in `design.md`.
- No new public library API; CLI-internal change. CLAUDE.md CLI section gets a one-line note
  that export shows a determinate bar + ETA when `--limit` is set.
