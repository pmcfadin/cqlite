//! :status meta-command implementation
//!
//! Displays discovery and schema coverage status using DiscoveryService

use anyhow::Result;
use colored::Colorize;
use std::path::Path;

#[cfg(feature = "state_machine")]
use cqlite_core::discovery::{CoverageBadge, DiscoveryService};

/// Execute the :status command
///
/// Displays:
/// - Data directory and discovery timestamp
/// - Keyspaces and tables found
/// - Schema coverage counts and deltas
/// - Version hints
/// - Coverage badge (Green/Yellow/Red/Unknown)
pub async fn execute_status(data_dir: Option<&Path>) -> Result<()> {
    #[cfg(not(feature = "state_machine"))]
    {
        let _ = data_dir; // Suppress unused warning
        println!(
            "{}",
            "⚠️  Status command requires state_machine feature"
                .yellow()
                .bold()
        );
        println!("{}", "Hint: Rebuild with --features state_machine".dimmed());
        return Ok(());
    }

    #[cfg(feature = "state_machine")]
    {
        // Check if data_dir is set
        let Some(data_dir) = data_dir else {
            println!("{}", "⚠️  Data directory not configured".yellow().bold());
            println!(
                "{}",
                "Hint: Set data directory with :config data-dir <PATH>".dimmed()
            );
            return Ok(());
        };

        // Create discovery service (without schema registry for now)
        let discovery_service = DiscoveryService::new(data_dir.to_path_buf(), None);

        // Scan and display results
        println!("{}", "Scanning data directory...".dimmed());
        let summary = discovery_service.scan().await?;

        display_summary(&summary);

        Ok(())
    }
}

#[cfg(feature = "state_machine")]
fn display_summary(summary: &cqlite_core::discovery::DiscoverySummary) {
    use chrono::{DateTime, Local};

    println!();
    println!("{}", "=== Data Discovery Status ===".green().bold());
    println!();

    // Data directory and timestamp
    println!(
        "{} {}",
        "Data Directory:".cyan(),
        summary.data_dir.display()
    );
    let datetime: DateTime<Local> = summary.timestamp.into();
    println!(
        "{} {}",
        "Discovery Time:".cyan(),
        datetime.format("%Y-%m-%d %H:%M:%S")
    );
    println!();

    // Keyspaces and tables
    println!(
        "{} {} keyspace(s), {} table(s)",
        "Discovered:".cyan(),
        summary.keyspaces.len(),
        summary.tables.len()
    );
    println!(
        "{} {} SSTable file(s)",
        "SSTable Files:".cyan(),
        summary.sstables_found
    );
    println!();

    // List keyspaces if any
    if !summary.keyspaces.is_empty() {
        println!("{}", "Keyspaces:".green().bold());
        for keyspace in &summary.keyspaces {
            println!("  - {}", keyspace.yellow());
        }
        println!();
    }

    // List tables (first 10)
    if !summary.tables.is_empty() {
        println!("{} (showing first 10):", "Tables:".green().bold());
        for table in summary.tables.iter().take(10) {
            println!("  - {}", table.cyan());
        }
        if summary.tables.len() > 10 {
            println!("  {} {} more...", "...".dimmed(), summary.tables.len() - 10);
        }
        println!();
    }

    // Version info
    if let Some(ref version) = summary.resolved_version {
        println!("{} {}", "Cassandra Version:".cyan(), version);
    } else {
        println!("{} {}", "Cassandra Version:".cyan(), "unknown".dimmed());
    }
    println!();

    // Schema coverage (if available)
    if let Some(ref coverage) = summary.coverage {
        println!("{}", "Schema Coverage:".green().bold());

        let coverage_pct = coverage.coverage_percentage();
        println!(
            "  {} {}%",
            "Coverage:".cyan(),
            format!("{:.1}", coverage_pct).bold()
        );

        println!(
            "  {} {}",
            "Tables with schema:".cyan(),
            coverage.tables_with_schema
        );

        if !coverage.tables_missing_schema.is_empty() {
            println!(
                "  {} {} (showing first 5):",
                "Tables missing schema:".yellow(),
                coverage.tables_missing_schema.len()
            );
            for table in coverage.tables_missing_schema.iter().take(5) {
                println!("    - {}", table.yellow());
            }
            if coverage.tables_missing_schema.len() > 5 {
                println!(
                    "    {} {} more...",
                    "...".dimmed(),
                    coverage.tables_missing_schema.len() - 5
                );
            }
            println!(
                "  {}",
                "Hint: Load schemas with :schema load <PATH>".dimmed()
            );
        }

        if !coverage.schemas_without_data.is_empty() {
            println!(
                "  {} {} (showing first 5):",
                "Schemas without data:".yellow(),
                coverage.schemas_without_data.len()
            );
            for schema in coverage.schemas_without_data.iter().take(5) {
                println!("    - {}", schema.dimmed());
            }
            if coverage.schemas_without_data.len() > 5 {
                println!(
                    "    {} {} more...",
                    "...".dimmed(),
                    coverage.schemas_without_data.len() - 5
                );
            }
        }
        println!();
    } else {
        println!(
            "{}",
            "Schema coverage: Not available (no schemas loaded)".yellow()
        );
        println!("{}", "Hint: Load schemas with :schema load <PATH>".dimmed());
        println!();
    }

    // Coverage badge
    let (badge_symbol, badge_text) = match summary.badge {
        CoverageBadge::Green => ("✅", "Green (≥95% coverage)".green()),
        CoverageBadge::Yellow => ("⚠️ ", "Yellow (50-95% coverage)".yellow()),
        CoverageBadge::Red => ("❌", "Red (<50% coverage or critical errors)".red()),
        CoverageBadge::Unknown => ("❓", "Unknown (no schema loaded)".dimmed()),
    };

    println!(
        "{} {} {}",
        "Status:".cyan().bold(),
        badge_symbol,
        badge_text
    );
    println!();
}
