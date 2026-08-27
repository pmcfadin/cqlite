# Phase-2 recon — can we measure `do_get` on Corpus B, and is S=6 comparable?

Read-only recon, no runs. Answers the three questions asked before phase 2 is
committed to. **The short version: (a) mechanics are straightforward, (c) Corpus
B is servable, and (b) — the decisive one — `do_get` at S=6 is NOT comparable to
bare-scan S=6 on this box, and probably not even a valid `do_get` measurement.**

## (a) Mechanics

| piece | how |
|---|---|
| server | `cqlite-flight --data-dir <root> --port <p>`. `Producer::table_base_dir` resolves `<data_dir>/<keyspace>/<table>`, so `--data-dir /data/ws0-3096` serves `ws0.events`. |
| schema | **Carried in the TICKET, not a server-side file** — `service.rs:424 parse_schema(ticket)` parses CQL DDL per request and caches it (`schemas: Mutex<HashMap<String, Arc<TableSchema>>>`). So the server needs no `--schema`; the loadgen's `--ticket-template` supplies Corpus B's DDL. |
| loadgen | `flight-loadgen --endpoint http://127.0.0.1:<p> --ticket-template <file>` |
| **N** | `--concurrency` — an ordered comma-separated list of target concurrencies, **one ramp step each** (default `1,2,4,8,16,32`). So N is expressed as ramp steps, and one invocation sweeps the whole ladder. |
| rows | per-step records carrying `rows_total` and `rows_per_s` (`record.rs`), written to `--out`. |
| pinning | `ws0-baseline.sh` splits `--server-cpus` / `--client-cpus` and **refuses overlap**. |

## (b) THE PINNING JUDGEMENT — S=6 is not comparable, and I recommend against it

**The rig's own defaults are the evidence.** `ws0-baseline.sh` ships
`SERVER_CPUS="2,10"` (**1** physical core) and
`CLIENT_CPUS="4,12,5,13,6,14,7,15"` (**4** physical cores). Whoever calibrated
this rig gave the loadgen **four times** the server's cores at S=1.

On 8 physical cores, `do_get` at S=6 leaves **2** physical cores for the client.
That is a server:client ratio of **6:2**, against the rig's calibrated **1:4** —
a **12× swing** in relative client provisioning. Two independent problems follow,
and either one alone is disqualifying:

1. **It is a different machine configuration from bare-scan S=6.** The bare-scan
   S=6 point ran 6 pinned cores with **2 cores IDLE** as headroom and **no client
   at all**. The `do_get` S=6 point would saturate those 2 cores with the
   loadgen, so the whole package is loaded — lower all-core turbo, and LLC and
   memory bandwidth shared with a client that the bare-scan point did not have.
   Any ratio between the two arms would silently include that difference.
2. **It would probably be CLIENT-BOUND, and the error points the wrong way.** A
   2-core client driving a 6-core server is far below the provisioning the rig's
   author chose for a 1-core server. A client-bound number is not a measurement
   of `do_get` at all — it is a measurement of the loadgen. And the direction is
   the dangerous one: it **understates `do_get`**, which **overstates** the
   bare-scan-vs-`do_get` gap, which flatters exactly the lever this issue exists
   to calibrate. That is the one error class this whole issue is built to prevent.

**Recommendation: measure S=1 only; take a stated hole at S=6.**

- **S=1 is worth doing.** It reproduces the rig's own calibrated 1:4 split
  (`--server-cpus 2,10 --client-cpus 4,12,5,13,6,14,7,15`), which is the
  configuration #3100/#3272 already used, and it delivers the **same-corpus
  bare-scan-vs-`do_get` ratio that R1 promised** — the thing a skeptic actually
  asks for, and which no existing number provides on one corpus.
- Even S=1 carries a smaller version of caveat 1 and should say so: bare-scan
  S=1 ran with 7 physical cores idle, while `do_get` S=1 loads 5 of 8. Not
  identical; far closer than S=6, and the difference is disclosable.
- **If S=6 is wanted anyway, it needs a bigger box** — enough physical cores to
  provision a 6-core server at the rig's own client ratio (≈24 client cores,
  i.e. 30+ physical). That is a follow-up issue, not something to absorb here.

**And the client-bound claim above should be FALSIFIED, not asserted.** It is
cheap: run S=6 with a 2-core client and again with a 1-core client. If the
aggregate moves materially, the measurement is client-bound and the number is
void; if it does not move, my objection 2 is wrong and only objection 1 stands.
Either way the answer is measured. I would run that before writing any S=6
`do_get` figure into the report.

## (c) Can Flight serve Corpus B? Yes, on the evidence read so far

- **No schema obstacle** — the ticket carries the DDL, so
  `/data/ws0-3096/ws0-events.cql` goes into the ticket template; nothing
  server-side needs changing.
- **No `CompressionInfo.db` assumption found.** The warm-budget accounting is
  explicitly compression-agnostic and enumerates components "whichever format,
  whichever compression setting", naming `CompressionInfo.db` only as one
  optional sibling among others — and it explicitly contemplates the uncompressed
  case, noting `CRC.db` "can DOMINATE on an uncompressed BIG table". Corpus B is
  uncompressed BIG with 8 components and no `CompressionInfo.db`.
- **No Corpus-A schema assumption found** in the Flight path; the table is
  resolved positionally as `<data_dir>/<keyspace>/<table>`.

**Residual, stated rather than assumed away:** this is a code read, not a run. The
cheap confirmation is a single `do_get` request against
`--data-dir /data/ws0-3096` returning a non-zero row count — which must happen
before any phase-2 measurement, since a 0-row `do_get` would otherwise look like
a very fast one.
