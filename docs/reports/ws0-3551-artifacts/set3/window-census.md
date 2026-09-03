| session | arm | round | pos | window (UTC) | in-window samples | competing | verdict | pinned-CPU busy, TOTAL incl. our own (2,3,10,11) |
|---|---|--:|--:|---|--:|--:|---|--:|
| `r1-A` | A | 1 | 1 | 2026-09-03T03:28:14Z → 2026-09-03T03:30:06Z | 11 | 0 | clean (census 0, max gap 10s) | 46.4% |
| `r1-B` | B | 1 | 2 | 2026-09-03T03:30:06Z → 2026-09-03T03:32:17Z | 13 | 0 | clean (census 0, max gap 11s) | 43.3% |
| `r1-C0` | C0 | 1 | 3 | 2026-09-03T03:32:17Z → 2026-09-03T03:34:30Z | 13 | 1 | **CONTAMINATED** (1 of 13) | 42.0% |
| `r1-C` | C | 1 | 4 | 2026-09-03T03:34:30Z → 2026-09-03T03:36:26Z | 12 | 0 | clean (census 0, max gap 10s) | 43.5% |
| `r1-D` | D | 1 | 5 | 2026-09-03T03:36:26Z → 2026-09-03T03:38:12Z | 10 | 0 | clean (census 0, max gap 10s) | 49.0% |
| `r2-B` | B | 2 | 1 | 2026-09-03T03:38:12Z → 2026-09-03T03:40:17Z | 13 | 0 | clean (census 0, max gap 10s) | 42.4% |
| `r2-C0` | C0 | 2 | 2 | 2026-09-03T03:40:17Z → 2026-09-03T03:42:31Z | 13 | 0 | clean (census 0, max gap 10s) | 42.4% |
| `r2-C` | C | 2 | 3 | 2026-09-03T03:42:31Z → 2026-09-03T03:44:23Z | 12 | 0 | clean (census 0, max gap 10s) | 44.2% |
| `r2-D` | D | 2 | 4 | 2026-09-03T03:44:23Z → 2026-09-03T03:46:09Z | 11 | 0 | clean (census 0, max gap 10s) | 48.4% |
| `r2-A` | A | 2 | 5 | 2026-09-03T03:46:09Z → 2026-09-03T03:48:04Z | 12 | 1 | **CONTAMINATED** (1 of 12) | 46.7% |
| `r3-C0` | C0 | 3 | 1 | 2026-09-03T03:48:04Z → 2026-09-03T03:50:12Z | 13 | 2 | **CONTAMINATED** (2 of 13) | 42.3% |
| `r3-C` | C | 3 | 2 | 2026-09-03T03:50:12Z → 2026-09-03T03:52:03Z | 11 | 0 | clean (census 0, max gap 10s) | 44.2% |
| `r3-D` | D | 3 | 3 | 2026-09-03T03:52:03Z → 2026-09-03T03:53:48Z | 11 | 0 | clean (census 0, max gap 10s) | 48.8% |
| `r3-A` | A | 3 | 4 | 2026-09-03T03:53:48Z → 2026-09-03T03:55:53Z | 12 | 0 | clean (census 0, max gap 10s) | 47.1% |
| `r3-B` | B | 3 | 5 | 2026-09-03T03:55:53Z → 2026-09-03T03:58:03Z | 13 | 0 | clean (census 0, max gap 10s) | 43.0% |

**3 of 15 session(s) NOT USABLE** (contaminated, undercovered, unobserved, or census-unusable): `r1-C0`, `r2-A`, `r3-C0`

`competing_count` bounds compilers, linkers and the `agent-gate.sh` script and NOT total foreign load (issue #3551 D3), and it does not replace the drift control, which is what decides readability.

The pinned-CPU column is TOTAL busy and is dominated by THIS MEASUREMENT during a session (measured: 42-46% in-session against a median 8% idle), so it does NOT separate a peer's cycles from ours and is NOT a contamination bound. It is reported because an UNDER-loaded session is a real failure this makes visible.
