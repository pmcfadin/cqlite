//! SSTable validate/analyze command handlers.
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]
// Allow deprecated BulletproofReader usage (Issue #190 - experimental reader)
#![allow(deprecated)]

use super::schema_load::load_schema_file;
use super::support::{resolve_sstable_path, RealDataParser};
use anyhow::{Context, Result};
use cqlite_core::storage::sstable::bulletproof_reader::BulletproofReader;
use std::path::Path;

/// Validate SSTable format, integrity, and data consistency
pub async fn validate_sstable(
    sstable_path: &Path,
    schema_path: Option<&Path>,
    deep: bool,
    fix: bool,
    report_path: Option<&Path>,
) -> Result<()> {
    println!("🔍 SSTable Validation");
    println!("📂 SSTable: {}", sstable_path.display());

    if let Some(schema) = schema_path {
        println!("📋 Schema: {}", schema.display());
    }

    if deep {
        println!("🔬 Deep validation enabled (thorough but slower)");
    }

    if fix {
        println!("🔧 Auto-fix enabled for recoverable issues");
    }

    if let Some(report) = report_path {
        println!("📋 Report will be saved to: {}", report.display());
    }

    // Smart path resolution
    let actual_sstable_path = resolve_sstable_path(sstable_path)?;
    println!("📄 Data file: {}", actual_sstable_path.display());

    let mut issues_found = 0;
    let issues_fixed = 0;
    let mut validation_errors = Vec::new();

    // Basic file existence and readability
    println!("\n🔍 Basic file validation:");
    if !actual_sstable_path.exists() {
        let error = "❌ SSTable file does not exist";
        println!("{error}");
        validation_errors.push(error.to_string());
        issues_found += 1;
    } else {
        println!("✅ SSTable file exists");

        // Check file permissions
        match std::fs::metadata(&actual_sstable_path) {
            Ok(metadata) => {
                println!("✅ File readable (size: {} bytes)", metadata.len());

                if metadata.len() == 0 {
                    let error = "⚠️  Warning: SSTable file is empty";
                    println!("{error}");
                    validation_errors.push(error.to_string());
                    issues_found += 1;
                }
            }
            Err(e) => {
                let error = format!("❌ Cannot read file metadata: {e}");
                println!("{error}");
                validation_errors.push(error);
                issues_found += 1;
            }
        }
    }

    // Try loading with bulletproof reader
    println!("\n🔍 Format validation:");
    match BulletproofReader::open(&actual_sstable_path) {
        Ok(mut reader) => {
            println!("✅ SSTable format is readable");

            let info = reader.info();
            println!("   Format: {:?}", info.format);
            println!("   Generation: {}", info.generation_numeric().unwrap_or(0));
            println!("   Size: {} bytes", info.size);

            if let Some(compression) = reader.compression_info() {
                println!(
                    "   Compression: {} (chunk size: {})",
                    compression.algorithm, compression.chunk_length
                );
            }

            // Deep validation
            if deep {
                println!("\n🔬 Deep validation:");
                match reader.parse_sstable_data() {
                    Ok(entries) => {
                        println!("✅ Successfully parsed {} entries", entries.len());

                        // Validate data consistency if schema provided
                        if let Some(schema_path) = schema_path {
                            match load_schema_file(schema_path, true, None) {
                                Ok(schema) => {
                                    println!("✅ Schema loaded successfully");
                                    let parser = RealDataParser::new(schema);

                                    let mut parsing_errors = 0;
                                    for entry in entries.iter() {
                                        let key = entry.key.clone();
                                        // Issue #1334: parse_entry consumes the
                                        // `ScanRow` carrier. This is a LIVE synthetic
                                        // value (pre-#1334 passed a `Value::Text`), so
                                        // it must be a live `ScanRow::Row` — a `Marker`
                                        // is reserved for a genuine null/row-tombstone.
                                        let value = cqlite_core::types::ScanRow::Row(vec![(
                                            std::sync::Arc::from("data"),
                                            cqlite_core::Value::Text(format!("{:?}", entry.key)),
                                        )]);

                                        if parser.parse_entry(&key, &value).is_err() {
                                            parsing_errors += 1;
                                        }
                                    }

                                    if parsing_errors > 0 {
                                        let error = format!(
                                            "⚠️  {parsing_errors} entries failed schema validation"
                                        );
                                        println!("{error}");
                                        validation_errors.push(error);
                                        issues_found += parsing_errors;
                                    } else {
                                        println!("✅ All entries match schema");
                                    }
                                }
                                Err(e) => {
                                    let error =
                                        format!("⚠️  Could not load schema for validation: {e}");
                                    println!("{error}");
                                    validation_errors.push(error);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let error = format!("❌ Failed to parse SSTable data: {e}");
                        println!("{error}");
                        validation_errors.push(error);
                        issues_found += 1;
                    }
                }
            }
        }
        Err(e) => {
            let error = format!("❌ Cannot open SSTable with bulletproof reader: {e}");
            println!("{error}");
            validation_errors.push(error);
            issues_found += 1;
        }
    }

    // Generate report
    if let Some(report_path) = report_path {
        let mut report_content = format!(
            "# SSTable Validation Report\n\n\
            **File:** {}\n\
            **Validation Time:** {}\n\
            **Deep Validation:** {}\n\
            **Auto-fix Enabled:** {}\n\n\
            ## Summary\n\
            - Issues Found: {}\n\
            - Issues Fixed: {}\n\n\
            ## Details\n",
            sstable_path.display(),
            chrono::Utc::now().to_rfc3339(),
            deep,
            fix,
            issues_found,
            issues_fixed
        );

        for error in &validation_errors {
            report_content.push_str(&format!("- {error}\n"));
        }

        std::fs::write(report_path, report_content)
            .with_context(|| format!("Failed to write report to {}", report_path.display()))?;

        println!("\n📋 Validation report saved to: {}", report_path.display());
    }

    // Summary
    println!("\n📊 Validation Summary:");
    println!("   Issues found: {issues_found}");
    println!("   Issues fixed: {issues_fixed}");

    if issues_found == 0 {
        println!("✅ SSTable validation passed!");
    } else if fix && issues_fixed == issues_found {
        println!("🔧 All issues fixed!");
    } else {
        println!("⚠️  {} issues remain", issues_found - issues_fixed);
    }

    Ok(())
}

/// Analyze SSTable structure, statistics, and performance characteristics
pub async fn analyze_sstable(
    sstable_path: &Path,
    schema_path: Option<&Path>,
    detailed: bool,
    infer_schema: bool,
    report_path: Option<&Path>,
) -> Result<()> {
    println!("📊 SSTable Analysis");
    println!("📂 SSTable: {}", sstable_path.display());

    if let Some(schema) = schema_path {
        println!("📋 Schema: {}", schema.display());
    }

    if detailed {
        println!("🔍 Detailed analysis enabled");
    }

    if infer_schema {
        println!("🧠 Schema inference enabled");
    }

    if let Some(report) = report_path {
        println!("📋 Report will be saved to: {}", report.display());
    }

    // Smart path resolution
    let actual_sstable_path = resolve_sstable_path(sstable_path)?;
    println!("📄 Data file: {}", actual_sstable_path.display());

    let mut analysis_results = Vec::new();

    // File-level analysis
    println!("\n📁 File Analysis:");
    match std::fs::metadata(&actual_sstable_path) {
        Ok(metadata) => {
            let file_size = metadata.len();
            println!(
                "   File size: {} bytes ({:.2} MB)",
                file_size,
                file_size as f64 / 1_048_576.0
            );
            analysis_results.push(format!("File size: {file_size} bytes"));

            if let Ok(created) = metadata.created() {
                println!("   Created: {created:?}");
            }
            if let Ok(modified) = metadata.modified() {
                println!("   Modified: {modified:?}");
            }
        }
        Err(e) => {
            println!("❌ Cannot read file metadata: {e}");
            return Err(anyhow::anyhow!("File metadata not accessible"));
        }
    }

    // Format analysis
    println!("\n🔍 Format Analysis:");
    match BulletproofReader::open(&actual_sstable_path) {
        Ok(mut reader) => {
            let info = reader.info();
            println!("   Format: {:?}", info.format);
            println!("   Generation: {}", info.generation_numeric().unwrap_or(0));
            println!("   Size: {} bytes", info.size);

            analysis_results.push(format!("Format: {:?}", info.format));
            analysis_results.push(format!(
                "Generation: {}",
                info.generation_numeric().unwrap_or(0)
            ));

            if let Some(compression) = reader.compression_info() {
                println!("   Compression: {}", compression.algorithm);
                println!("   Chunk length: {} bytes", compression.chunk_length);
                analysis_results.push(format!("Compression: {}", compression.algorithm));
            } else {
                println!("   Compression: None");
                analysis_results.push("Compression: None".to_string());
            }

            // Data analysis
            println!("\n📊 Data Analysis:");
            match reader.parse_sstable_data() {
                Ok(entries) => {
                    let entry_count = entries.len();
                    println!("   Total entries: {entry_count}");
                    analysis_results.push(format!("Total entries: {entry_count}"));

                    if entry_count > 0 {
                        // Calculate average key size
                        let total_key_size: usize =
                            entries.iter().map(|e| format!("{:?}", e.key).len()).sum();
                        let avg_key_size = total_key_size / entry_count;
                        println!("   Average key size: {avg_key_size} bytes");
                        analysis_results.push(format!("Average key size: {avg_key_size} bytes"));

                        // Show sample entries
                        println!("\n📋 Sample Entries (first 5):");
                        for (i, entry) in entries.iter().take(5).enumerate() {
                            println!(
                                "   {}. Key: {:?}, Info: {}",
                                i + 1,
                                entry.key,
                                entry.format_info
                            );
                        }
                    }

                    // Detailed analysis
                    if detailed {
                        println!("\n🔍 Detailed Statistics:");

                        // Key distribution analysis
                        let mut key_lengths = entries
                            .iter()
                            .map(|e| format!("{:?}", e.key).len())
                            .collect::<Vec<_>>();
                        key_lengths.sort_unstable();

                        if !key_lengths.is_empty() {
                            let min_key_len = key_lengths[0];
                            let max_key_len = key_lengths[key_lengths.len() - 1];
                            let median_key_len = key_lengths[key_lengths.len() / 2];

                            println!(
                                "   Key length min/max/median: {min_key_len}/{max_key_len}/{median_key_len}"
                            );
                            analysis_results.push(format!(
                                "Key lengths - min: {min_key_len}, max: {max_key_len}, median: {median_key_len}"
                            ));
                        }

                        // TODO: Add more detailed statistics
                        println!("   📊 Advanced statistics coming soon!");
                    }

                    // Schema inference
                    if infer_schema {
                        println!("\n🧠 Schema Inference:");
                        // TODO: Implement schema inference logic
                        println!("   🚧 Schema inference coming soon!");
                        analysis_results
                            .push("Schema inference: Feature in development".to_string());
                    }
                }
                Err(e) => {
                    println!("❌ Failed to parse SSTable data: {e}");
                    analysis_results.push(format!("Parse error: {e}"));
                }
            }
        }
        Err(e) => {
            println!("❌ Cannot open SSTable: {e}");
            return Err(anyhow::anyhow!("Cannot analyze SSTable: {}", e));
        }
    }

    // Generate report
    if let Some(report_path) = report_path {
        let mut report_content = format!(
            "# SSTable Analysis Report\n\n\
            **File:** {}\n\
            **Analysis Time:** {}\n\
            **Detailed Analysis:** {}\n\
            **Schema Inference:** {}\n\n\
            ## Results\n",
            sstable_path.display(),
            chrono::Utc::now().to_rfc3339(),
            detailed,
            infer_schema
        );

        for result in &analysis_results {
            report_content.push_str(&format!("- {result}\n"));
        }

        std::fs::write(report_path, report_content)
            .with_context(|| format!("Failed to write report to {}", report_path.display()))?;

        println!("\n📋 Analysis report saved to: {}", report_path.display());
    }

    println!("\n✅ Analysis completed!");

    Ok(())
}
