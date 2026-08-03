# Owner requirements for the #3217 report (from issue comment 2026-08-02T20:27:47Z)

DESIGN CONFIRMED: S-sweep S in {1,2,4,6} x N in {1,2,4,8,16}, client fixed at 2 physical
cores, validity-gated on measured client headroom. Publish client CPU% at EVERY (S,N).
Any point >~70% of the 2-core client budget is NOT a server measurement.
If the client saturates: downgrade client to drain-and-drop BEFORE ever trading a server
core back; if a core must be traded, label the arm honestly (5 cores, not 6).

TWO EXPLICIT ADDITIONAL ASKS — both are deliverable requirements, not nice-to-haves:

1. METHOD SECTION must carry BOTH dry-run traps, so the next off-CPU run inherits the
   guard and not merely the result:
   (a) a permissive kernel.perf_event_paranoid does NOT cover BPF map creation
       (bcc: "could not open bpf map"; bpftrace refuses outright) -> BPF collectors must
       run under sudo. perf itself does not need sudo.
   (b) offcputime charges only on switch-IN, so a single long-sleep probe records ZERO
       off-CPU time.
   Rationale to state explicitly: BOTH failure modes produce an EMPTY off-CPU profile,
   which reads identically to "the mpsc handoff is innocent" — the exact false-negative
   this issue exists to prevent.

2. The marginal-efficiency table posted to #2817 MUST state, IN ONE LINE, WHICH LEVER IT
   POINTS AT: handoff-fix vs #3096 (Arrow encode). "A curve without the call is half the
   result." This ordering call is the reason the cores were spent.

Standing constraints reaffirmed by the owner:
- physical-core basis throughout
- NO fixes; anything the data indicts becomes a follow-up issue
- a well-measured negative (full-box scales fine, no 4x collapse) is a SUCCESS;
  do not round toward the hypothesis

3. POST A COMMENT ON #3100 WHEN DONE (owner ask, 2026-08-02).
   #3100 is the parent measurement (pinned-core C(N) + device acquittal) that this issue
   extends, so it must not learn its own follow-up landed only via a merged PR.
   The #3100 comment must carry, at minimum:
   - AC2 verdict: did the S=1 arm REPRODUCE #3100's published pinned-core shape
     (peak at N=2 1.16x, decline to 0.96x at N=16)? If it diverged, the explanation.
     Note absolute rows/s need not match to the digit (regenerated corpus sha, different
     box instance) — the SHAPE is what AC2 is about.
   - What the full-box extension adds that the pinned-core curve could not show:
     the efficiency-vs-cores curve (S=1,2,4,6), i.e. one-time handoff tax vs compounding.
   - The off-CPU attribution result — the instrument #3100's baseline explicitly DECLINED
     ("On-CPU profiling only — declined here, not overlooked"), which named this exact
     phenomenon as where off-CPU would pay first (1.98M voluntary ctxt switches; ~13.5%
     of Flight wall time off the metered CPUs, unaccounted). Say whether that 13.5% is
     now accounted for, and by what.
   - Status of #3100's STILL-OPEN follow-up question (publishable absolutes vs Cassandra,
     which needs AWS + a Cassandra arm): explicitly NOT answered here and still open.
   - Corpus provenance note: regenerated, geometry-matched, NEW sha (3a4ee5cd...).
   Cross-post discipline: #2817 gets the marginal-efficiency table + the lever call;
   #3100 gets the shape-reproduction verdict + the off-CPU answer to its declined section.
   They are DIFFERENT comments for different audiences, not the same text twice.
