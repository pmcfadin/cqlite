| session | arm | round | pos | window (UTC) | in-window samples | competing | verdict | pinned-CPU busy, TOTAL incl. our own (2,3,10,11) |
|---|---|--:|--:|---|--:|--:|---|--:|
| `r1-A` | A | 1 | 1 | 2026-09-03T02:56:28Z → 2026-09-03T02:58:21Z | 12 | 0 | clean (census 0, max gap 10s) | 45.6% |
| `r1-B` | B | 1 | 2 | 2026-09-03T02:58:21Z → 2026-09-03T03:00:33Z | 13 | 0 | clean (census 0, max gap 10s) | 42.3% |
| `r1-C0` | C0 | 1 | 3 | 2026-09-03T03:00:33Z → 2026-09-03T03:02:48Z | 13 | 0 | clean (census 0, max gap 11s) | 42.9% |
| `r1-C` | C | 1 | 4 | 2026-09-03T03:02:48Z → 2026-09-03T03:04:41Z | 12 | 4 | **CONTAMINATED** (4 of 12) | 43.6% |
| `r1-D` | D | 1 | 5 | 2026-09-03T03:04:41Z → 2026-09-03T03:06:27Z | 11 | 11 | **CONTAMINATED** (11 of 11) | 47.9% |
| `r2-B` | B | 2 | 1 | 2026-09-03T03:06:27Z → 2026-09-03T03:08:38Z | 13 | 10 | **CONTAMINATED** (10 of 13) | 43.1% |
| `r2-C0` | C0 | 2 | 2 | 2026-09-03T03:08:38Z → 2026-09-03T03:10:55Z | 14 | 9 | **CONTAMINATED** (9 of 14) | 42.1% |
| `r2-C` | C | 2 | 3 | 2026-09-03T03:10:55Z → 2026-09-03T03:12:49Z | 11 | 0 | clean (census 0, max gap 10s) | 42.8% |
| `r2-D` | D | 2 | 4 | 2026-09-03T03:12:49Z → 2026-09-03T03:14:35Z | 11 | 0 | clean (census 0, max gap 10s) | 49.8% |
| `r2-A` | A | 2 | 5 | 2026-09-03T03:14:35Z → 2026-09-03T03:16:28Z | 11 | 0 | clean (census 0, max gap 10s) | 47.7% |
| `r3-C0` | C0 | 3 | 1 | 2026-09-03T03:16:28Z → 2026-09-03T03:18:46Z | 14 | 9 | **CONTAMINATED** (9 of 14) | 44.8% |
| `r3-C` | C | 3 | 2 | 2026-09-03T03:18:46Z → 2026-09-03T03:20:30Z | 10 | 10 | **CONTAMINATED** (10 of 10) | 44.8% |
| `r3-D` | D | 3 | 3 | 2026-09-03T03:20:30Z → 2026-09-03T03:22:24Z | 12 | 12 | **CONTAMINATED** (12 of 12) | 57.5% |
| `r3-A` | A | 3 | 4 | 2026-09-03T03:22:24Z → 2026-09-03T03:24:15Z | 11 | 6 | **CONTAMINATED** (6 of 11) | 83.1% |
| `r3-B` | B | 3 | 5 | 2026-09-03T03:24:15Z → 2026-09-03T03:26:21Z | 12 | 0 | clean (census 0, max gap 10s) | 41.7% |

**8 of 15 session(s) NOT USABLE** (contaminated, undercovered, unobserved, or census-unusable): `r1-C`, `r1-D`, `r2-B`, `r2-C0`, `r3-C0`, `r3-C`, `r3-D`, `r3-A`

`competing_count` bounds compilers, linkers and the `agent-gate.sh` script and NOT total foreign load (issue #3551 D3), and it does not replace the drift control, which is what decides readability.

The pinned-CPU column is TOTAL busy and is dominated by THIS MEASUREMENT during a session (measured: 42-46% in-session against a median 8% idle), so it does NOT separate a peer's cycles from ours and is NOT a contamination bound. It is reported because an UNDER-loaded session is a real failure this makes visible.
