//! :keyspaces meta-command implementation
use anyhow::Result;
use colored::Colorize;
use std::path::Path;

#[cfg(feature = "state_machine")]
use cqlite_core::discovery::DiscoveryService;

pub async fn execute_keyspaces(data_dir: Option<&Path>) -> Result<()> {
    #[cfg(not(feature = "state_machine"))]
    {
        let _ = data_dir;
        return Err(anyhow::anyhow!(
            "Keyspaces command requires state_machine feature. Rebuild with --features state_machine"
        ));
    }

    #[cfg(feature = "state_machine")]
    {
        let Some(data_dir) = data_dir else {
            return Err(anyhow::anyhow!(
                "Data directory not configured. Use :config data-dir <PATH>"
            ));
        };

        let discovery_service = DiscoveryService::new(data_dir.to_path_buf(), None);
        let summary = discovery_service.scan().await?;

        println!("{}", "=== Keyspaces ===".green().bold());
        println!();

        if summary.keyspaces.is_empty() {
            println!("{}", "No keyspaces found".yellow());
        } else {
            println!("Found {} keyspace(s):", summary.keyspaces.len());
            for keyspace in &summary.keyspaces {
                println!("  - {}", keyspace.yellow().bold());
            }
        }
        println!();

        Ok(())
    }
}
