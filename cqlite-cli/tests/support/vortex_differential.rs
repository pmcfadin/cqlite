//! Shared Vortex-vs-Parquet export harness (issue #4237): drives the compiled CLI for BOTH
//! formats on one table, reads each back with its OWN reader, and compares them. Used by
//! `issue_4237_vortex_parquet_differential.rs` (the 33-table sweep) and
//! `issue_4237_vortex_type_coverage.rs` (the per-type-family cases, R2.2/R2.3).

#![allow(dead_code)]

use arrow::array::Array;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

// ============================================================================
// CLI invocation + readers
// ============================================================================

pub fn run_cli(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("failed to spawn cqlite");
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        output.status.success(),
    )
}

/// Returns the file-level schema (from the Parquet FOOTER, always present regardless of row
/// count — issue #4237 test-harness fix: deriving the column set from `batches.first()` reads
/// `None` on a genuinely 0-row table and misreports an empty column set as a real divergence)
/// plus every row-group batch.
pub fn read_parquet_file(
    path: &Path,
) -> Result<(std::sync::Arc<ArrowSchema>, Vec<RecordBatch>), Box<dyn StdError>> {
    let bytes = std::fs::read(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
    Ok((schema, batches))
}

/// Read a `.vortex` file back to ONE Arrow `RecordBatch` via Vortex's own reader — the format's
/// own official Arrow bridge, exactly as `read_parquet_file` above uses the `parquet` crate's own
/// reader. `vortex::file::Writer`/`Reader` are natively async, so this helper is too; the caller
/// drives it on a throwaway current-thread runtime (test code only — the production writer in
/// `cqlite-core::export::vortex` uses `tokio` because it is called from the CLI's own async
/// context; this test binary has no ambient runtime, so it builds its own).
pub async fn read_vortex_file_async(path: &Path) -> Result<RecordBatch, Box<dyn StdError>> {
    use vortex::array::stream::ArrayStreamExt;
    use vortex::array::VortexSessionExecute;
    use vortex::arrow::ArrowSessionExt;
    use vortex::file::OpenOptionsSessionExt;
    use vortex::session::VortexSession;
    use vortex::VortexSessionDefault;

    let session = VortexSession::default();
    let file = session.open_options().open_path(path.to_path_buf()).await?;
    let array = file.scan()?.into_array_stream()?.read_all().await?;

    let mut ctx = session.create_execution_ctx();
    let arrow_array = session.arrow().execute_arrow(array, None, &mut ctx)?;
    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or("Vortex read-back did not yield a top-level struct array")?;
    Ok(RecordBatch::from(struct_array.clone()))
}

pub fn read_vortex_file(path: &Path) -> Result<RecordBatch, Box<dyn StdError>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(read_vortex_file_async(path))
}

// ============================================================================
// Comparison — full column set both directions (#3890), order-sensitive values
// ============================================================================

pub fn column_name_set(schema: &ArrowSchema) -> BTreeSet<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Every value in `column`, across all `batches`, as its `array_value_to_string` rendering —
/// value-level (not physical-encoding-level) comparison, so a schema-legal re-encoding (e.g.
/// dictionary vs plain, or a different Vortex/Parquet chunk boundary — design.md D2) between the
/// two formats' read-backs is not mistaken for a value divergence.
pub fn column_values_as_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let Ok(idx) = batch.schema().index_of(column) else {
            continue;
        };
        let col = batch.column(idx);
        for row in 0..batch.num_rows() {
            // Null validity is compared explicitly: `array_value_to_string` renders a null and an
            // empty value alike, so a null must never be able to pass for "".
            if col.is_null(row) {
                out.push("\u{0}<NULL>".to_string());
                continue;
            }
            out.push(
                array_value_to_string(col.as_ref(), row)
                    .unwrap_or_else(|e| format!("<display-error: {e}>")),
            );
        }
    }
    out
}

/// The LOGICAL Arrow type of a field: physical encodings either format may legitimately pick
/// (dictionary vs plain; view / large / small offset widths) are collapsed, everything that
/// carries meaning (the primitive kind, temporal unit, decimal precision/scale, nested shape,
/// child nullability) is kept. Compared per field so that e.g. `Int64` vs `Utf8` reds.
pub fn logical_type(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType as D;
    match dt {
        D::Dictionary(_, value) => logical_type(value),
        D::Utf8 | D::LargeUtf8 | D::Utf8View => "Utf8".to_string(),
        D::Binary | D::LargeBinary | D::BinaryView => "Binary".to_string(),
        D::List(f) | D::LargeList(f) | D::ListView(f) | D::LargeListView(f) => {
            format!("List<{}>", logical_field(f))
        }
        D::FixedSizeList(f, n) => format!("FixedSizeList<{}; {n}>", logical_field(f)),
        D::Struct(fields) => format!(
            "Struct<{}>",
            fields
                .iter()
                .map(|f| logical_field(f))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        D::Map(f, sorted) => format!("Map<{}; sorted={sorted}>", logical_field(f)),
        other => format!("{other:?}"),
    }
}

/// Name, logical type, nullability and Arrow extension name of a (possibly nested) field.
pub fn logical_field(f: &arrow::datatypes::Field) -> String {
    let ext = f
        .metadata()
        .get("ARROW:extension:name")
        .map(|e| format!(" ext={e}"))
        .unwrap_or_default();
    format!(
        "{}: {}{}{}",
        f.name(),
        logical_type(f.data_type()),
        if f.is_nullable() { "?" } else { "" },
        ext
    )
}

pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Both exports of one table, read back with their own readers.
pub struct ExportedPair {
    pub qualified: String,
    pub parquet_schema: std::sync::Arc<ArrowSchema>,
    pub parquet_batches: Vec<RecordBatch>,
    pub vortex_schema: arrow::datatypes::SchemaRef,
    pub vortex_batches: Vec<RecordBatch>,
}

/// Export `<keyspace>.<table>` from `sstables` (an `sstables/` root) to Parquet AND Vortex via
/// the compiled CLI and read both back.
pub fn export_both(
    sstables: &Path,
    keyspace: &str,
    table: &str,
    schema: &Path,
    where_clause: Option<&str>,
) -> Result<ExportedPair, Box<dyn StdError>> {
    let qualified = format!("{keyspace}.{table}");
    let tmp = TempDir::new()?;
    let parquet_path = tmp.path().join("out.parquet");
    let vortex_path = tmp.path().join("out.vortex");

    for (format, path) in [("parquet", &parquet_path), ("vortex", &vortex_path)] {
        let mut args = vec![
            "--schema",
            schema.to_str().unwrap(),
            "--data-dir",
            sstables.to_str().unwrap(),
            "export",
            path.to_str().unwrap(),
            "--format",
            format,
            "--table",
            &qualified,
        ];
        if let Some(w) = where_clause {
            args.extend(["--query", w]);
        }
        let (_, stderr, ok) = run_cli(&args);
        if !ok {
            return Err(format!("{qualified}: {format} export failed: {stderr}").into());
        }
    }

    let (parquet_schema, parquet_batches) = read_parquet_file(&parquet_path)
        .map_err(|e| format!("{qualified}: failed to read back parquet: {e}"))?;
    let vortex_batch = read_vortex_file(&vortex_path)
        .map_err(|e| format!("{qualified}: failed to read back vortex: {e}"))?;
    Ok(ExportedPair {
        qualified,
        parquet_schema,
        parquet_batches,
        vortex_schema: vortex_batch.schema(),
        vortex_batches: vec![vortex_batch],
    })
}

/// Run the differential for one table. Returns `Ok(row_count)` or a descriptive `Err` naming the
/// table and the exact divergence (column-set direction, row count, or the first mismatching
/// column + row).
pub fn diff_table(
    sstables: &Path,
    keyspace: &str,
    table: &str,
    schema: &Path,
) -> Result<usize, Box<dyn StdError>> {
    let pair = export_both(sstables, keyspace, table, schema, None)?;
    diff_pair(&pair)
}

pub fn diff_pair(pair: &ExportedPair) -> Result<usize, Box<dyn StdError>> {
    let qualified = pair.qualified.as_str();
    let parquet_schema = pair.parquet_schema.clone();
    let parquet_batches = &pair.parquet_batches[..];
    let vortex_schema = pair.vortex_schema.clone();
    let vortex_batches = &pair.vortex_batches[..];

    let parquet_row_count = total_rows(parquet_batches);
    let vortex_row_count = total_rows(vortex_batches);
    if parquet_row_count != vortex_row_count {
        return Err(format!(
            "{qualified}: row count diverges — parquet {parquet_row_count}, vortex {vortex_row_count}"
        )
        .into());
    }

    // Derived from the FILE-LEVEL schema (present even for a genuinely 0-row table), not
    // `batches.first()` — see `read_parquet_file`'s doc comment.
    let parquet_cols = column_name_set(&parquet_schema);
    let vortex_cols = column_name_set(&vortex_schema);

    let parquet_only: Vec<_> = parquet_cols.difference(&vortex_cols).cloned().collect();
    let vortex_only: Vec<_> = vortex_cols.difference(&parquet_cols).cloned().collect();
    if !parquet_only.is_empty() || !vortex_only.is_empty() {
        return Err(format!(
            "{qualified}: column set diverges — only in parquet: {parquet_only:?}, only in vortex: {vortex_only:?}"
        )
        .into());
    }
    // Same set, so now the ORDER: a Vortex export with reordered columns must not pass.
    let parquet_order: Vec<&str> = parquet_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let vortex_order: Vec<&str> = vortex_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    if parquet_order != vortex_order {
        return Err(format!(
            "{qualified}: column order diverges — parquet: {parquet_order:?}, vortex: {vortex_order:?}"
        )
        .into());
    }

    // Same names in the same order, so now each field's LOGICAL type, nullability and
    // extension metadata (roborev #4237 r6): string-rendered values alone would let
    // `Int64(1)` vs `Utf8("1")` pass.
    for (pf, vf) in parquet_schema
        .fields()
        .iter()
        .zip(vortex_schema.fields().iter())
    {
        let (p, v) = (logical_field(pf), logical_field(vf));
        if p != v {
            return Err(
                format!("{qualified}: field type diverges — parquet {p}, vortex {v}").into(),
            );
        }
    }

    for column in &parquet_cols {
        let p_values = column_values_as_strings(parquet_batches, column);
        let v_values = column_values_as_strings(vortex_batches, column);
        if p_values != v_values {
            let first_mismatch = p_values
                .iter()
                .zip(v_values.iter())
                .position(|(p, v)| p != v);
            return Err(format!(
                "{qualified}.{column}: value diverges at row {:?} (parquet={:?}, vortex={:?})",
                first_mismatch,
                first_mismatch.map(|i| &p_values[i]),
                first_mismatch.map(|i| &v_values[i]),
            )
            .into());
        }
    }

    Ok(parquet_row_count)
}
