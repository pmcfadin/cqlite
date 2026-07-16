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

/// Run the full ramp, returning one [`StepRecord`] per configured concurrency,
/// in order. `gen` supplies deterministic tickets; each worker connects its own
/// raw client to `config.endpoint`.
pub async fn run_ramp(config: &RampConfig, gen: &ShapeGen) -> Result<Vec<StepRecord>, String> {
    let mut records = Vec::with_capacity(config.concurrencies.len());
    for (step_idx, &concurrency) in config.concurrencies.iter().enumerate() {
        let record = run_step(config, gen, step_idx, concurrency).await?;
        records.push(record);
    }
    Ok(records)
}

/// Run one ramp step at `concurrency`, merging per-worker partials into a record.
async fn run_step(
    config: &RampConfig,
    gen: &ShapeGen,
    step_idx: usize,
    concurrency: usize,
) -> Result<StepRecord, String> {
    let concurrency = concurrency.max(1);
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
    for worker_idx in 0..concurrency {
        let gen = gen.clone();
        let shape = config.shape;
        let step_u = step_idx as u64;
        let worker_u = worker_idx as u64;
        let claimed = Arc::clone(&claimed);
        let mut client = connect(&config.endpoint, config.connect_timeout).await?;
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
    let parse_num = |num: &str| -> Result<f64, String> {
        num.parse::<f64>()
            .map_err(|_| format!("bad duration {s:?}"))
    };
    if let Some(ms) = s.strip_suffix("ms") {
        Ok(Duration::from_secs_f64(parse_num(ms)? / 1000.0))
    } else if let Some(m) = s.strip_suffix('m') {
        Ok(Duration::from_secs_f64(parse_num(m)? * 60.0))
    } else if let Some(sec) = s.strip_suffix('s') {
        Ok(Duration::from_secs_f64(parse_num(sec)?))
    } else {
        Ok(Duration::from_secs_f64(parse_num(s)?))
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

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("5").unwrap(), Duration::from_secs(5));
        assert!(parse_duration("nope").is_err());
    }
}
