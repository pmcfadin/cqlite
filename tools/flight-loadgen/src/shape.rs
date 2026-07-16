//! Ticket synthesis: base template + seeded shape transforms
//! (design §(b); spec: shapes + determinism requirements).
//!
//! The operator supplies ONE base [`FlightTicket`] template (connector-shaped:
//! `keyspace`/`table`/`ddl`/`snapshot`, full ring, no limit). Each workload shape
//! is derived by transforming a *clone* of that template — the client never
//! invents DDL or partition keys the operator did not provide. Every derived
//! ticket keeps the template's `keyspace`/`table`/`ddl`/`snapshot` unchanged.
//!
//! Determinism (design §(c)): the RNG for a request is seeded purely from
//! `(seed, step, worker, iteration)`, so two runs with the same seed/ramp/shape/
//! template produce a byte-identical ticket sequence — wall-clock timing never
//! perturbs which data is requested.

use cqlite_flight::ticket::FlightTicket;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// The four workload shapes (design §(b)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// Full-ring scan: the template as-is (`limit = None`).
    Full,
    /// `LIMIT`-k scan: the template with `limit = Some(k)`.
    LimitK,
    /// Point read: the template narrowed to a seeded token sub-range
    /// `[t, t + width)` (a setup/admission-cost proxy — design §(b) fork).
    Point,
    /// A seeded weighted draw across `Point`/`LimitK`/`Full`.
    Mixed,
}

impl Shape {
    /// Parse the CLI `--shape` value.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "full" => Ok(Shape::Full),
            "limit-k" | "limitk" | "limit" => Ok(Shape::LimitK),
            "point" | "ptr" => Ok(Shape::Point),
            "mixed" | "mix" => Ok(Shape::Mixed),
            other => Err(format!(
                "unknown shape {other:?} (expected point|limit-k|full|mixed)"
            )),
        }
    }

    /// The record's `shape` label.
    pub fn label(self) -> &'static str {
        match self {
            Shape::Full => "full",
            Shape::LimitK => "limit-k",
            Shape::Point => "point",
            Shape::Mixed => "mixed",
        }
    }
}

/// Weights for the `mixed` shape's seeded draw across point/limit-k/full.
#[derive(Debug, Clone, Copy)]
pub struct MixWeights {
    pub point: f64,
    pub limit_k: f64,
    pub full: f64,
}

impl Default for MixWeights {
    fn default() -> Self {
        Self {
            point: 0.6,
            limit_k: 0.3,
            full: 0.1,
        }
    }
}

impl MixWeights {
    /// Parse `ptr=0.6,lim=0.3,full=0.1` (any subset; missing keys default to 0,
    /// but the total must be positive).
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut w = MixWeights {
            point: 0.0,
            limit_k: 0.0,
            full: 0.0,
        };
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (k, v) = part
                .split_once('=')
                .ok_or_else(|| format!("bad --mix term {part:?} (expected key=weight)"))?;
            let weight: f64 = v
                .trim()
                .parse()
                .map_err(|_| format!("bad --mix weight {v:?}"))?;
            if weight < 0.0 {
                return Err(format!("--mix weight for {k:?} must be >= 0"));
            }
            match k.trim() {
                "ptr" | "point" => w.point = weight,
                "lim" | "limit" | "limit-k" => w.limit_k = weight,
                "full" => w.full = weight,
                other => return Err(format!("unknown --mix key {other:?}")),
            }
        }
        if w.point + w.limit_k + w.full <= 0.0 {
            return Err("--mix weights must sum to a positive value".to_string());
        }
        Ok(w)
    }

    /// Resolve a concrete non-mixed shape from a `[0,1)` draw.
    fn choose(&self, draw: f64) -> Shape {
        let total = self.point + self.limit_k + self.full;
        let pick = draw * total;
        if pick < self.point {
            Shape::Point
        } else if pick < self.point + self.limit_k {
            Shape::LimitK
        } else {
            Shape::Full
        }
    }
}

/// Deterministic ticket generator over one base template (design §(b)/§(c)).
#[derive(Debug, Clone)]
pub struct ShapeGen {
    base: FlightTicket,
    seed: u64,
    limit_k: u64,
    point_width: i64,
    mix: MixWeights,
}

impl ShapeGen {
    /// Build a generator from a base template and CLI parameters.
    pub fn new(
        base: FlightTicket,
        seed: u64,
        limit_k: u64,
        point_width: i64,
        mix: MixWeights,
    ) -> Self {
        Self {
            base,
            seed,
            limit_k,
            point_width: point_width.max(1),
            mix,
        }
    }

    /// The per-request RNG, seeded purely from `(seed, step, worker, iteration)`
    /// via a splitmix-style mix so distinct coordinates decorrelate.
    fn rng(&self, step: u64, worker: u64, iter: u64) -> StdRng {
        // Fold the four coordinates into one 64-bit seed. Each term is spread
        // with a large odd constant so nearby coordinates do not collide.
        let mut s = self.seed;
        for coord in [step, worker, iter] {
            s = s
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(coord.wrapping_add(0xD1B5_4A32_D192_ED03));
        }
        StdRng::seed_from_u64(s)
    }

    /// Build the ticket for `(shape, step, worker, iteration)`. For [`Shape::Mixed`]
    /// the concrete shape is drawn from the same seeded RNG that a `point` draw
    /// would use, so the whole selection is reproducible.
    pub fn build(&self, shape: Shape, step: u64, worker: u64, iter: u64) -> FlightTicket {
        let mut rng = self.rng(step, worker, iter);
        let concrete = match shape {
            Shape::Mixed => self.mix.choose(rng.random::<f64>()),
            other => other,
        };
        self.apply(concrete, &mut rng)
    }

    /// Apply a concrete (non-mixed) shape to a clone of the base template.
    fn apply(&self, shape: Shape, rng: &mut StdRng) -> FlightTicket {
        let mut t = self.base.clone();
        match shape {
            Shape::Full => {
                // Template as-is: full ring, no limit.
                t.limit = None;
            }
            Shape::LimitK => {
                t.limit = Some(self.limit_k);
            }
            Shape::Point => {
                // Seeded narrow token sub-range [t, t + width). Draw the start
                // from the full i64 ring; saturate the end on overflow.
                let start: i64 = rng.random::<i64>();
                let end = start.saturating_add(self.point_width);
                t.token_start = Some(start);
                t.token_end = Some(end);
                t.wraparound = false;
                t.limit = None;
            }
            // `apply` is only ever called with a concrete shape.
            Shape::Mixed => unreachable!("Mixed is resolved before apply"),
        }
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> FlightTicket {
        // `FlightTicket` is `#[non_exhaustive]`; build via `default()` + assign.
        let mut t = FlightTicket::default();
        t.keyspace = "ks".into();
        t.table = "tbl".into();
        t.ddl = "CREATE TABLE ks.tbl (id int PRIMARY KEY, v int)".into();
        t.snapshot = Some("cqlite-snap".into());
        t
    }

    fn gen() -> ShapeGen {
        ShapeGen::new(base(), 42, 100, 1 << 40, MixWeights::default())
    }

    #[test]
    fn full_is_template_with_no_limit_over_full_ring() {
        let t = gen().build(Shape::Full, 0, 0, 0);
        assert_eq!(t.limit, None);
        assert_eq!(t.token_start, None);
        assert_eq!(t.token_end, None);
    }

    #[test]
    fn limit_k_sets_the_limit() {
        let t = gen().build(Shape::LimitK, 0, 0, 0);
        assert_eq!(t.limit, Some(100));
        assert_eq!(t.token_start, None, "limit-k does not narrow the ring");
    }

    #[test]
    fn point_narrows_to_a_seeded_subrange_of_width() {
        let t = gen().build(Shape::Point, 0, 0, 0);
        let start = t.token_start.expect("point sets token_start");
        let end = t.token_end.expect("point sets token_end");
        assert!(!t.wraparound);
        assert_eq!(t.limit, None);
        assert_eq!(
            end,
            start.saturating_add(1 << 40),
            "sub-range width == --point-width"
        );
    }

    #[test]
    fn every_shape_preserves_template_identity_fields() {
        for shape in [Shape::Full, Shape::LimitK, Shape::Point, Shape::Mixed] {
            let t = gen().build(shape, 3, 1, 7);
            assert_eq!(t.keyspace, "ks", "{shape:?} keeps keyspace");
            assert_eq!(t.table, "tbl", "{shape:?} keeps table");
            assert!(
                t.ddl.starts_with("CREATE TABLE ks.tbl"),
                "{shape:?} keeps ddl"
            );
            assert_eq!(
                t.snapshot,
                Some("cqlite-snap".to_string()),
                "{shape:?} keeps snapshot"
            );
        }
    }

    #[test]
    fn identical_seed_reproduces_the_ticket_sequence() {
        // spec: determinism scenario — same seed/shape/template ⇒ byte-identical
        // sequence for a fixed (step, worker) across iterations.
        let a = gen();
        let b = gen();
        for iter in 0..64 {
            let ta = a.build(Shape::Mixed, 5, 2, iter);
            let tb = b.build(Shape::Mixed, 5, 2, iter);
            assert_eq!(
                ta.to_bytes().unwrap(),
                tb.to_bytes().unwrap(),
                "iteration {iter} must be byte-identical across runs"
            );
        }
    }

    #[test]
    fn different_seed_changes_point_tokens() {
        let a = ShapeGen::new(base(), 1, 100, 1 << 40, MixWeights::default());
        let b = ShapeGen::new(base(), 2, 100, 1 << 40, MixWeights::default());
        let ta = a.build(Shape::Point, 0, 0, 0);
        let tb = b.build(Shape::Point, 0, 0, 0);
        assert_ne!(
            ta.token_start, tb.token_start,
            "distinct seeds should pick distinct start tokens"
        );
    }

    #[test]
    fn mix_weights_parse_and_choose() {
        let w = MixWeights::parse("ptr=0.5,lim=0.5,full=0.0").unwrap();
        assert_eq!(w.choose(0.0), Shape::Point);
        assert_eq!(w.choose(0.75), Shape::LimitK);
        assert!(MixWeights::parse("").is_err(), "empty ⇒ non-positive total");
        assert!(MixWeights::parse("bogus").is_err());
    }
}
