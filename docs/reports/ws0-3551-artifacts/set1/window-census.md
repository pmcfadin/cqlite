| session | arm | round | pos | window (UTC) | in-window samples | competing | verdict | pinned-CPU busy, TOTAL incl. our own (2,3,10,11) |
|---|---|--:|--:|---|--:|--:|---|--:|
| `r1-A` | A | 1 | 1 | 2026-09-03T02:28:31Z → 2026-09-03T02:30:43Z | 13 | 0 | clean (census 0, max gap 11s) | 45.9% |
| `r1-B` | B | 1 | 2 | 2026-09-03T02:30:43Z → 2026-09-03T02:32:45Z | 12 | 0 | clean (census 0, max gap 10s) | 41.8% |
| `r1-C0` | C0 | 1 | 3 | 2026-09-03T02:32:45Z → 2026-09-03T02:34:58Z | 13 | 0 | clean (census 0, max gap 10s) | 42.7% |
| `r1-C` | C | 1 | 4 | 2026-09-03T02:34:58Z → 2026-09-03T02:36:49Z | 12 | 0 | clean (census 0, max gap 10s) | 43.1% |
| `r2-B` | B | 2 | 1 | 2026-09-03T02:36:49Z → 2026-09-03T02:39:00Z | 14 | 0 | clean (census 0, max gap 10s) | 42.3% |
| `r2-C0` | C0 | 2 | 2 | 2026-09-03T02:39:00Z → 2026-09-03T02:41:13Z | 13 | 0 | clean (census 0, max gap 10s) | 42.5% |
| `r2-C` | C | 2 | 3 | 2026-09-03T02:41:13Z → 2026-09-03T02:43:03Z | 11 | 0 | clean (census 0, max gap 10s) | 42.8% |
| `r2-A` | A | 2 | 4 | 2026-09-03T02:43:03Z → 2026-09-03T02:45:09Z | 12 | 0 | clean (census 0, max gap 11s) | 46.8% |
| `r3-C0` | C0 | 3 | 1 | 2026-09-03T02:45:09Z → 2026-09-03T02:47:23Z | 14 | 0 | clean (census 0, max gap 10s) | 41.0% |
| `r3-C` | C | 3 | 2 | 2026-09-03T02:47:23Z → 2026-09-03T02:49:13Z | 11 | 0 | clean (census 0, max gap 10s) | 42.7% |
| `r3-A` | A | 3 | 3 | 2026-09-03T02:49:13Z → 2026-09-03T02:51:04Z | 11 | 0 | clean (census 0, max gap 10s) | 46.2% |
| `r3-B` | B | 3 | 4 | 2026-09-03T02:51:04Z → 2026-09-03T02:53:08Z | 12 | 0 | clean (census 0, max gap 10s) | 42.3% |

**All 12 sessions clean** — every in-window sample recorded `competing_count = 0`.

`competing_count` bounds compilers, linkers and the `agent-gate.sh` script and NOT total foreign load (issue #3551 D3), and it does not replace the drift control, which is what decides readability.

The pinned-CPU column is TOTAL busy and is dominated by THIS MEASUREMENT during a session (measured: 42-46% in-session against a median 8% idle), so it does NOT separate a peer's cycles from ours and is NOT a contamination bound. It is reported because an UNDER-loaded session is a real failure this makes visible.
