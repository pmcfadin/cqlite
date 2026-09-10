//! `cqlite salvage` — recover every completely-decodable partition of a
//! damaged SSTable into a fresh uncompressed generation, and account for
//! every partition it could not (issue #4196, epic #4192).
//!
//! See `openspec/changes/sstable-salvage/{proposal,design}.md` for the full
//! contract. Design D1: enumerate partitions from the authoritative boundary
//! source (BIG `Index.db` / BTI `Partitions.db` trie), decode each at its
//! named offset through the existing compaction-row decoder, and write every
//! partition that decodes COMPLETELY through the production writer. A
//! partition whose decode fails at any row is lost WHOLE (D2 atomicity) —
//! never a prefix, because a later row in that partition can carry a
//! tombstone that shadows the earlier ones salvage already decoded.

mod boundaries;
mod chunks;
mod recover;

pub use boundaries::BoundarySourceKind;
pub use recover::salvage_sstable;

use serde::{Deserialize, Serialize};

/// Options for a salvage run. Reserved for future recovery-policy knobs;
/// empty today — design D2 fixes partition atomicity as the one recovery
/// policy and does not make it configurable.
#[derive(Debug, Clone, Copy, Default)]
pub struct SalvageOptions {}

/// One partition salvage could not recover, with enough context for an
/// operator to locate it manually (design D5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Loss {
    /// The partition key's raw bytes, hex-encoded (always present).
    pub key_hex: String,
    /// The partition key rendered through the schema, when it could be
    /// decoded. `None` when no schema-driven rendering was attempted or it
    /// failed (e.g. the key itself is what is in question).
    pub key: Option<String>,
    /// This slot's Data.db start offset per the boundary source.
    pub data_offset: u64,
    /// Compressed-chunk indices this partition's byte range intersects, when
    /// known: a compressed input (`CompressionInfo.db`), or an uncompressed
    /// input with a readable `CRC.db` sidecar. Empty when neither chunk
    /// table is available.
    pub chunks: Vec<u64>,
    pub class: LossClass,
    /// Rows that had already decoded when the failure occurred. Salvage
    /// writes NONE of them (D2); this is purely informational for an
    /// operator doing a manual look.
    pub rows_decoded_before_failure: usize,
    pub message: String,
}

/// Why a partition was lost (design D5's `class` field; spec R2/R3/R4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LossClass {
    /// The partition's byte range intersects a chunk whose CRC (inline for
    /// compressed input, `CRC.db` for uncompressed) did not validate.
    ChunkCrc,
    /// A row failed to decode partway through the partition.
    Decode,
    /// The decoded key at the boundary source's offset did not match the
    /// key the boundary source named for that slot.
    KeyMismatch,
    /// The partition's authoritative byte range extends past Data.db's
    /// actual end (truncation), so its bytes could not be materialized.
    Truncated,
}

impl LossClass {
    /// The serde kebab-case spelling (`"chunk-crc"`, `"decode"`,
    /// `"key-mismatch"`, `"truncated"`) — the SAME divergence
    /// [`RefusalReason::manifest_label`] was added to fix (roborev, issue
    /// #4196, round-5 Low finding 5): `render_text` previously used the Rust
    /// `Debug` spelling (`ChunkCrc`, `KeyMismatch`, ...) while the JSON
    /// manifest emitted this kebab-case one, for the SAME value — an
    /// operator grepping stderr for the class name the manifest documents
    /// (spec D5's `class` field) found nothing.
    pub fn manifest_label(self) -> &'static str {
        match self {
            LossClass::ChunkCrc => "chunk-crc",
            LossClass::Decode => "decode",
            LossClass::KeyMismatch => "key-mismatch",
            LossClass::Truncated => "truncated",
        }
    }
}

/// A component-level finding observed while salvaging (design D5). The
/// `class` string mirrors
/// [`crate::storage::sstable::verify::VerifyErrorClass::code`] naming so the
/// verifier and the salvage manifest use the same vocabulary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentFinding {
    pub class: String,
    pub component: String,
    pub detail: String,
}

/// Partition totals (design D5).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PartitionTotals {
    pub total: usize,
    pub recovered: usize,
    pub lost: usize,
}

/// Why salvage refused to write any output (design D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefusalReason {
    /// `Index.db` (BIG) or `Partitions.db`/`Rows.db` (BTI) could not be read
    /// or walked structurally — boundaries are unknown, so nothing can be
    /// safely enumerated (spec R4).
    BoundarySourceUnreadable,
    /// The boundary source was readable but every named partition was lost
    /// (spec R5.2) — there is nothing to write.
    NothingDecodable,
    /// A component salvage needs to even OPEN the input or classify its
    /// repair state — `CompressionInfo.db` (chunk table), `Statistics.db`
    /// (repair-state fields) — could not be read (roborev, issue #4196,
    /// batched finding b). Distinct from `BoundarySourceUnreadable`: the
    /// boundary source (`Index.db`/`Partitions.db`) was fine here, a
    /// DIFFERENT component was not, so `rebuild --components index` would
    /// not help — the remedy names the damaged component instead.
    ComponentUnreadable,
}

impl RefusalReason {
    /// The serde kebab-case spelling (`"boundary-source-unreadable"`,
    /// `"nothing-decodable"`) — spec R7.3 requires the CLI's stderr to name
    /// the refusal with THIS vocabulary, and design D5 declares the text
    /// rendering "a rendering of" the JSON manifest, so [`SalvageReport::render_text`]
    /// uses this rather than the Rust `Debug` spelling (roborev, issue #4196:
    /// the two previously diverged — `Debug` prints `BoundarySourceUnreadable`,
    /// the manifest emits `boundary-source-unreadable`).
    pub fn manifest_label(self) -> &'static str {
        match self {
            RefusalReason::BoundarySourceUnreadable => "boundary-source-unreadable",
            RefusalReason::NothingDecodable => "nothing-decodable",
            RefusalReason::ComponentUnreadable => "component-unreadable",
        }
    }
}

/// A refusal: salvage wrote no `Data.db` (design D3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refusal {
    pub reason: RefusalReason,
    /// Operator-facing next step. Names `cqlite rebuild` (issue #4197) when
    /// the boundary source is the problem.
    pub remedy: String,
}

/// The salvage manifest (design D5) — the JSON contract; the CLI's text
/// output is a rendering of it (spec R8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalvageReport {
    pub input: String,
    pub output: String,
    /// `"nb"` for BIG input, `"da"` for BTI input.
    pub format: String,
    pub compressed_input: bool,
    /// `"index"` (BIG) or `"bti-trie"` (BTI) — see [`BoundarySourceKind::manifest_label`].
    pub boundary_source: String,
    pub generation: u64,
    pub partitions: PartitionTotals,
    pub losses: Vec<Loss>,
    pub component_findings: Vec<ComponentFinding>,
    /// `true` once the per-partition recovery loop began — DISTINCT from
    /// `partitions.total`, which is populated as soon as the boundary
    /// source is enumerated, before a single partition is examined
    /// (roborev, issue #4196, round-9 Low finding): two `ComponentUnreadable`
    /// refusal sites (writer construction, `classify_inputs`,
    /// `recover.rs`) fire AFTER `partitions.total` is set but BEFORE the
    /// loop ever runs, so keying `render_text`'s affirmative-zero "NOT
    /// MEASURED" rendering off `partitions.total` alone made a genuine
    /// zero-attempt refusal print identically to a clean, fully-measured,
    /// zero-loss run. `false` in every refusal reached before this point;
    /// `true` from here on, including `RefusalReason::NothingDecodable`
    /// (which always falls through to a normal rendering regardless).
    pub attempted: bool,
    pub refused: Option<Refusal>,
    /// RFC3339 timestamp of the run.
    pub now: String,
    pub cqlite_version: String,
}

impl SalvageReport {
    /// Text rendering of the manifest (design D5: "the CLI text form is a
    /// rendering of it"). `losses: 0 RECOGNISED` is the affirmative empty
    /// form (spec R1.2) — an unmeasured run must never read the same as a
    /// clean one.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("salvage: {}\n", self.input));
        out.push_str(&format!(
            "format: {} (boundary source: {})\n",
            self.format, self.boundary_source
        ));
        if let Some(refusal) = &self.refused {
            out.push_str(&format!("REFUSED: {}\n", refusal.reason.manifest_label()));
            out.push_str(&format!("remedy: {}\n", refusal.remedy));
            // Fall through when SOMETHING was actually measured, rather than
            // returning here (roborev, issue #4196, round-6 Medium finding
            // 1) — `RefusalReason::NothingDecodable` is set alongside a
            // fully-populated `losses` vector, and its remedy literally
            // reads "inspect the losses above", so THAT refusal must still
            // render the partition totals, the loss list and the component
            // findings, exactly like the non-refused path.
            //
            // round-7's fix keyed this off `refusal.reason == NothingDecodable`
            // alone, which was ALSO wrong (roborev, issue #4196, round-8
            // Medium finding): TWO of the `ComponentUnreadable` sites in
            // `recover.rs` (writer construction, `classify_inputs`) fire
            // AFTER `report.partitions.total` is set and AFTER the chunk-CRC
            // pre-flight's own finding was pushed into
            // `report.component_findings` — real, already-measured data that
            // round-7's reason-only check silently printed as `NOT MEASURED`
            // over, dropping what the JSON manifest still carries and
            // breaking D5's "text is a rendering of the manifest".
            //
            // round-8's fix (`partitions.total > 0 ||
            // !component_findings.is_empty()`) was ALSO wrong the OTHER
            // direction (roborev, issue #4196, round-9 Low finding): those
            // SAME two `ComponentUnreadable` sites populate
            // `partitions.total` (boundary enumeration succeeds) but refuse
            // BEFORE the per-partition loop ever runs — genuinely ZERO
            // partitions attempted — yet `partitions.total > 0` made
            // `measured` read `true`, printing `partitions: total=N
            // recovered=0 lost=0` / `losses: 0 RECOGNISED`: an unmeasured
            // run reading identically to a clean one, the EXACT
            // affirmative-zero violation this whole block exists to
            // prevent. Key off the explicit `attempted` flag instead —
            // `recover.rs` sets it `true` only once the per-partition loop
            // itself begins, strictly AFTER every earlier refusal site.
            //
            // roborev, issue #4196, round-10 Low finding: this branch used
            // to `return out;` immediately, which ALSO skipped the
            // `component_findings` block below unconditionally — but a
            // refusal reached AFTER the chunk pre-flight (e.g.
            // `classify_inputs` failing on a `Statistics.db` that is
            // corrupt ALONGSIDE a `Data.db` with real chunk-CRC failures)
            // has genuinely MEASURED component findings even though
            // `attempted` is still `false` (no partition was ever decoded).
            // `NOT MEASURED` is correct for `partitions`/`losses`
            // specifically; it must not also silently swallow findings the
            // JSON manifest still carries, or text stops being "a
            // rendering of the manifest" (D5) for exactly the operator
            // reading the text default. Skip ONLY the
            // partitions/losses block; the component-findings block below
            // always runs.
            if !self.attempted {
                out.push_str("partitions: NOT MEASURED (refused before enumeration)\n");
                out.push_str("losses: NOT MEASURED\n");
                return self.render_component_findings(out);
            }
        }
        out.push_str(&format!(
            "partitions: total={} recovered={} lost={}\n",
            self.partitions.total, self.partitions.recovered, self.partitions.lost
        ));
        if self.losses.is_empty() {
            out.push_str("losses: 0 RECOGNISED\n");
        } else {
            out.push_str(&format!("losses: {}\n", self.losses.len()));
            for loss in &self.losses {
                out.push_str(&format!(
                    "  - key={} offset={} class={} rows_decoded_before_failure={}: {}\n",
                    loss.key_hex,
                    loss.data_offset,
                    loss.class.manifest_label(),
                    loss.rows_decoded_before_failure,
                    loss.message
                ));
            }
        }
        self.render_component_findings(out)
    }

    /// Append the `component findings:` block (design D5) when non-empty,
    /// and return the accumulated text. Factored out (roborev, issue
    /// #4196, round-10 Low finding) so the `!attempted` early-out path and
    /// the normal path share the SAME rendering rather than risk the two
    /// drifting apart.
    fn render_component_findings(&self, mut out: String) -> String {
        if !self.component_findings.is_empty() {
            out.push_str("component findings:\n");
            for f in &self.component_findings {
                out.push_str(&format!(
                    "  - [{}] {}: {}\n",
                    f.class, f.component, f.detail
                ));
            }
        }
        out
    }
}
