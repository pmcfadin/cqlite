//! The concurrency-ramp engine (design §(c); spec: ramp + memory-bound
//! requirements).
//!
//! A ramp is an ordered list of target concurrencies, each held for a step
//! bound. Per step a worker pool of size `C` keeps `C` `do_get`s in flight until
//! the bound is reached; each worker loops build-ticket → `do_get` → drain →
//! record. Workers accumulate into their own [`StepAgg`] (no hot-path lock) and
//! the partials are merged at step end into one [`StepRecord`].
//!
//! Determinism: which data a worker requests depends only on
//! `(seed, step, worker, iteration)` via [`ShapeGen`]; wall-clock only bounds a
//! duration step, never the sampled data.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::client::{connect, do_get_drain};
use crate::record::{StepAgg, StepRecord};
use crate::shape::{Shape, ShapeGen};

/// How a step decides it is done.
#[derive(Debug, Clone, Copy)]
pub enum StepBound {
    /// Hold the concurrency for a wall-clock duration (the operator ramp).
    Duration(Duration),
    /// Issue exactly this many requests total across the step's workers, then
    /// stop (the self-test — no wall-clock, so no timing flake).
    Requests(u64),
}

/// A single ramp configuration.
#[derive(Debug, Clone)]
pub struct RampConfig {
    /// Ordered target concurrencies (one step each).
    pub concurrencies: Vec<usize>,
    /// Per-step bound.
    pub bound: StepBound,
    /// Workload shape swept for every step.
    pub shape: Shape,
    /// Round label stamped on each record.
    pub round: String,
    /// Endpoint URL every worker connects a raw `FlightServiceClient` to (also
    /// stamped on each record). For the self-test this is the in-process
    /// server's ephemeral `http://127.0.0.1:<port>`.
    pub endpoint: String,
    /// Per-worker TCP connect timeout.
    pub connect_timeout: Duration,
    /// Seed stamped on each record (for provenance).
    pub seed: u64,
}

/// Run the full ramp, returning one [`StepRecord`] per COMPLETED step (in order)
/// plus an optional terminal error. `gen` supplies deterministic tickets; each
/// worker connects its own raw client to `config.endpoint`.
///
/// A step failing (e.g. a connect failure at a later, higher-concurrency step —
/// an EXPECTED saturation outcome, design §(c)) STOPS the ramp but is returned
/// alongside the records already gathered from prior successful steps, never in
/// place of them: the caller ([`crate::output::finalize`]) writes the completed
/// steps' JSONL before surfacing the error, so one late-step hiccup can never
/// discard many steps' worth of data.
pub async fn run_ramp(config: &RampConfig, gen: &ShapeGen) -> (Vec<StepRecord>, Option<String>) {
    let mut records = Vec::with_capacity(config.concurrencies.len());
    for (step_idx, &concurrency) in config.concurrencies.iter().enumerate() {
        match run_step(config, gen, step_idx, concurrency).await {
            Ok(record) => records.push(record),
            Err(e) => {
                return (
                    records,
                    Some(format!(
                        "ramp stopped at step {step_idx} (target concurrency {concurrency}): {e}"
                    )),
                );
            }
        }
    }
    (records, None)
}

/// Run one ramp step at `concurrency`, merging per-worker partials into a record.
async fn run_step(
    config: &RampConfig,
    gen: &ShapeGen,
    step_idx: usize,
    concurrency: usize,
) -> Result<StepRecord, String> {
    let concurrency = concurrency.max(1);

    // Connect ALL workers up front, BEFORE spawning any task (design 1a). If any
    // connect fails we return `Err` here with ZERO worker tasks in flight, so no
    // already-spawned worker is ever orphaned: dropping a `JoinHandle` DETACHES the
    // task (it does NOT abort it), which — under the old connect-inside-the-spawn-loop
    // code — left earlier workers issuing uncounted, unbounded load past the step's
    // deadline on a mid-loop `?`. Nothing to abort now: on failure `clients` (and its
    // open channels) simply drop.
    let mut clients = Vec::with_capacity(concurrency);
    for worker_idx in 0..concurrency {
        let client = connect(&config.endpoint, config.connect_timeout)
            .await
            .map_err(|e| format!("worker {worker_idx} connect: {e}"))?;
        clients.push(client);
    }

    // Capture the step's timing window AFTER every connect has completed, so the
    // (variable) connect latency never biases the measured `duration_s`/deadline.
    let started = Instant::now();
    let deadline = match config.bound {
        StepBound::Duration(d) => Some(started + d),
        StepBound::Requests(_) => None,
    };
    // Shared request-slot claim for the count-bounded self-test: workers claim a
    // 0-based index; a claim >= the budget means "stop". Unused for Duration.
    let claimed = Arc::new(AtomicU64::new(0));
    let request_budget = match config.bound {
        StepBound::Requests(n) => Some(n),
        StepBound::Duration(_) => None,
    };

    let mut handles = Vec::with_capacity(concurrency);
    for (worker_idx, mut client) in clients.into_iter().enumerate() {
        let gen = gen.clone();
        let shape = config.shape;
        let step_u = step_idx as u64;
        let worker_u = worker_idx as u64;
        let claimed = Arc::clone(&claimed);
        handles.push(tokio::spawn(async move {
            let mut agg = StepAgg::new();
            let mut iter: u64 = 0;
            loop {
                // Bound check BEFORE issuing so we never exceed the budget/deadline.
                match request_budget {
                    Some(budget) => {
                        let slot = claimed.fetch_add(1, Ordering::Relaxed);
                        if slot >= budget {
                            break;
                        }
                    }
                    None => {
                        if let Some(deadline) = deadline {
                            if Instant::now() >= deadline {
                                break;
                            }
                        }
                    }
                }
                let ticket = gen.build(shape, step_u, worker_u, iter);
                let ticket_bytes = match ticket.to_bytes() {
                    Ok(b) => b,
                    Err(e) => {
                        // A template that cannot serialise is a config error, not
                        // a server outcome — surface it as an error sample.
                        agg.record_outcome(&crate::classify::Outcome::Error(format!(
                            "TicketEncode:{e}"
                        )));
                        iter += 1;
                        continue;
                    }
                };
                let req_start = Instant::now();
                let result = do_get_drain(&mut client, ticket_bytes).await;
                match result.outcome {
                    crate::classify::Outcome::Ok => {
                        let latency_us =
                            req_start.elapsed().as_micros().min(u64::MAX as u128) as u64;
                        agg.record_ok(latency_us, result.rows, result.bytes);
                    }
                    other => agg.record_outcome(&other),
                }
                iter += 1;
            }
            agg
        }));
    }

    let mut merged = StepAgg::new();
    for handle in handles {
        let partial = handle
            .await
            .map_err(|e| format!("worker task panicked/join failed: {e}"))?;
        merged.merge(&partial);
    }

    let duration_s = started.elapsed().as_secs_f64();
    let ts_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0);
    Ok(merged.into_record(
        config.round.clone(),
        config.endpoint.clone(),
        ts_unix_ms,
        config.seed,
        step_idx,
        concurrency,
        config.shape.label().to_string(),
        duration_s,
    ))
}

/// Parse `--ramp` (`1,2,4,8`) into ordered positive concurrencies.
pub fn parse_ramp(s: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let c: usize = part
            .parse()
            .map_err(|_| format!("bad --ramp level {part:?} (expected a positive integer)"))?;
        if c == 0 {
            return Err("--ramp levels must be >= 1".to_string());
        }
        out.push(c);
    }
    if out.is_empty() {
        return Err("--ramp must list at least one concurrency".to_string());
    }
    Ok(out)
}

/// Parse a duration like `30s`, `500ms`, `2m` (default unit: seconds if bare).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    // Validate the parsed number before constructing a Duration: negative, NaN,
    // and infinite values all make Duration::from_secs_f64 panic. We keep this
    // early check for a clearer error, but the authoritative guard is
    // Duration::try_from_secs_f64 applied to the FINAL, SCALED value in every
    // branch below: it also rejects post-scale overflow to +inf (e.g. "1e308m")
    // and finite values that overflow Duration's range (e.g. "1e30"), both of
    // which would still panic under from_secs_f64.
    let parse_num = |num: &str| -> Result<f64, String> {
        let n = num
            .parse::<f64>()
            .map_err(|_| format!("bad duration {s:?}"))?;
        if !n.is_finite() || n < 0.0 {
            return Err(format!(
                "bad duration {s:?}: must be a finite, non-negative number"
            ));
        }
        Ok(n)
    };
    let to_duration = |secs: f64| -> Result<Duration, String> {
        Duration::try_from_secs_f64(secs).map_err(|_| format!("bad duration {s:?}: out of range"))
    };
    if let Some(ms) = s.strip_suffix("ms") {
        to_duration(parse_num(ms)? / 1000.0)
    } else if let Some(m) = s.strip_suffix('m') {
        to_duration(parse_num(m)? * 60.0)
    } else if let Some(sec) = s.strip_suffix('s') {
        to_duration(parse_num(sec)?)
    } else {
        to_duration(parse_num(s)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ramp_orders_and_rejects_bad_input() {
        assert_eq!(
            parse_ramp("1,2,4,8,16,32").unwrap(),
            vec![1, 2, 4, 8, 16, 32]
        );
        assert_eq!(
            parse_ramp(" 3 , 1 ").unwrap(),
            vec![3, 1],
            "order preserved"
        );
        assert!(parse_ramp("").is_err());
        assert!(parse_ramp("0").is_err());
        assert!(parse_ramp("x").is_err());
    }

    /// Regression (roborev Medium): a connect failure must NOT `?`-propagate out of
    /// `run_ramp` and nuke everything. It now returns the records gathered so far
    /// (empty here — the very first step fails) PLUS a terminal error, and returns
    /// PROMPTLY because the connect is attempted up front, before any worker task is
    /// spawned (no orphaned/detached workers issuing load past the deadline).
    #[tokio::test]
    async fn run_ramp_connect_failure_returns_terminal_error_not_panic() {
        // A definitely-closed loopback port: bind then immediately drop the listener.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);

        let gen = ShapeGen::new(
            crate::selftest::selftest_template(),
            42,
            100,
            1 << 40,
            crate::shape::MixWeights::default(),
        );
        let config = RampConfig {
            concurrencies: vec![1, 2],
            bound: StepBound::Requests(3),
            shape: Shape::Full,
            round: "connect-fail".into(),
            endpoint: format!("http://{addr}"),
            connect_timeout: Duration::from_millis(150),
            seed: 42,
        };

        let (records, err) = run_ramp(&config, &gen).await;
        assert!(
            records.is_empty(),
            "no step completed against the closed port"
        );
        let err = err.expect("a connect failure must surface a terminal error");
        assert!(
            err.contains("step 0") && err.contains("connect"),
            "error names the failing step + connect cause: {err}"
        );
    }

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("5").unwrap(), Duration::from_secs(5));
        assert!(parse_duration("nope").is_err());
    }

    #[test]
    fn parse_duration_rejects_non_finite_and_negative_without_panicking() {
        // Regression: these previously reached Duration::from_secs_f64, which
        // panics on negative/NaN/infinite input instead of returning Err.
        // "1e30" (bare, finite, non-negative) overflows Duration's range, and
        // "1e308m" is finite pre-scale but overflows to +inf after *60.0 — both
        // still panicked under from_secs_f64 despite the pre-scale check, so the
        // try_from_secs_f64 guard on the final scaled value must catch them.
        for bad in [
            "-5s", "-1", "nan", "inf", "-inf", "infms", "-2m", "1e30", "1e308m",
        ] {
            assert!(
                parse_duration(bad).is_err(),
                "expected Err for {bad:?}, got Ok"
            );
        }
    }
}
