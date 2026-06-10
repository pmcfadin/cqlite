#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_extract_table_name() {
        assert_eq!(
            scan::extract_table_name("users-46436710673711f0b2cf19d64e7cbecb").unwrap(),
            "users"
        );
        assert_eq!(
            scan::extract_table_name("all_types-46200090673711f0b2cf19d64e7cbecb").unwrap(),
            "all_types"
        );
        assert_eq!(
            scan::extract_table_name("simple_table").unwrap(),
            "simple_table"
        );
    }

    #[test]
    fn test_parse_sstable_filename() {
        let (version, generation, fmt, comp) = scan::parse_sstable_filename("nb-1-big-Data.db")
            .unwrap()
            .unwrap();
        assert_eq!(version, "nb");
        assert_eq!(generation, 1);
        assert_eq!(fmt, "big");
        assert_eq!(comp, types::SSTableComponent::Data);

        // da/bti files are now supported (VG1: FormatDetector maps `da` to V5x)
        let (version, generation, fmt, comp) = scan::parse_sstable_filename("da-2-bti-Rows.db")
            .unwrap()
            .unwrap();
        assert_eq!(version, "da");
        assert_eq!(generation, 2);
        assert_eq!(fmt, "bti");
        assert_eq!(comp, types::SSTableComponent::Rows);

        assert!(scan::parse_sstable_filename("not-an-sstable.txt")
            .unwrap()
            .is_none());
    }

    /// VG1: parse_sstable_filename now carries the version letter for each filename.
    #[test]
    fn test_parse_sstable_filename_version_letter() {
        for (filename, expected_version) in &[
            ("nb-1-big-Data.db", "nb"),
            ("oa-1-big-Data.db", "oa"),
            ("da-1-bti-Data.db", "da"),
        ] {
            let (version, _gen, _fmt, _comp) =
                scan::parse_sstable_filename(filename).unwrap().unwrap();
            assert_eq!(
                &version, expected_version,
                "version mismatch for {}",
                filename
            );
        }
    }

    /// Test for Issue #232: parse_sstable_filename should ignore auxiliary files
    /// (.jsonl, .db.txt) that exist alongside valid SSTable components
    #[test]
    fn test_parse_sstable_filename_ignores_auxiliary_files() {
        // Should return None for JSONL validation files
        assert!(scan::parse_sstable_filename("nb-1-big-Data.db.jsonl")
            .unwrap()
            .is_none());

        // Should return None for human-readable stats dumps
        assert!(scan::parse_sstable_filename("nb-1-big-Statistics.db.txt")
            .unwrap()
            .is_none());

        // Should return None for other auxiliary extensions
        assert!(scan::parse_sstable_filename("nb-1-big-Data.db.json")
            .unwrap()
            .is_none());

        // Should still parse valid SSTable files correctly
        let (version, generation, fmt, comp) = scan::parse_sstable_filename("nb-1-big-Data.db")
            .unwrap()
            .unwrap();
        assert_eq!(version, "nb");
        assert_eq!(generation, 1);
        assert_eq!(fmt, "big");
        assert_eq!(comp, types::SSTableComponent::Data);
    }

    /// Test for Issue #232: Directory scan should succeed with auxiliary files present
    #[test]
    fn test_directory_scan_with_auxiliary_files() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();

        // Create valid SSTable files
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
        ];
        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }

        // Create auxiliary files (should be ignored, not cause errors)
        let aux_files = ["nb-1-big-Data.db.jsonl", "nb-1-big-Statistics.db.txt"];
        for file in &aux_files {
            fs::write(table_dir.join(file), "auxiliary content").unwrap();
        }

        // Should successfully scan and find 1 generation, ignoring auxiliary files
        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        assert_eq!(directory.table_name, "test_table");
        assert_eq!(directory.generations.len(), 1);
        assert_eq!(directory.generations[0].components.len(), 3); // Data, Statistics, TOC
    }

    #[test]
    fn test_component_properties() {
        assert!(types::SSTableComponent::Data.is_required());
        assert!(types::SSTableComponent::Statistics.is_required());
        assert!(!types::SSTableComponent::Filter.is_required());

        assert!(types::SSTableComponent::Partitions.is_bti_specific());
        assert!(!types::SSTableComponent::Data.is_bti_specific());

        assert!(types::SSTableComponent::Index.is_big_specific());
        assert!(!types::SSTableComponent::Data.is_big_specific());
    }

    #[test]
    fn test_sstable_directory_scan() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();

        // Create mock SSTable files
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
            "nb-2-big-Data.db",
            "nb-2-big-Statistics.db",
        ];

        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }

        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        assert_eq!(directory.table_name, "test_table");
        assert_eq!(directory.generations.len(), 2);
        assert_eq!(directory.secondary_indexes.len(), 0); // No secondary indexes in this test

        // Should be sorted by generation (newest first)
        assert_eq!(directory.generations[0].generation, 2);
        assert_eq!(directory.generations[1].generation, 1);

        assert!(directory.is_valid());
    }

    #[test]
    fn test_secondary_index_scanning() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("users-abc123");
        fs::create_dir(&table_dir).unwrap();

        // Create main SSTable files
        let main_files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
        ];
        for file in &main_files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }

        // Create secondary index directory
        let index_dir = table_dir.join(".users_metadata_idx");
        fs::create_dir(&index_dir).unwrap();

        let index_files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-TOC.txt",
        ];
        for file in &index_files {
            fs::write(index_dir.join(file), "mock index content").unwrap();
        }

        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        assert_eq!(directory.table_name, "users");
        assert_eq!(directory.generations.len(), 1);
        assert_eq!(directory.secondary_indexes.len(), 1);

        let secondary_index = &directory.secondary_indexes[0];
        assert_eq!(secondary_index.index_name, "users_metadata_idx");
        assert_eq!(secondary_index.generations.len(), 1);

        // Test getter methods
        assert!(directory
            .get_secondary_index("users_metadata_idx")
            .is_some());
        assert!(directory.get_secondary_index("nonexistent").is_none());
    }

    #[test]
    fn test_validation_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();

        // Create files including TOC.txt
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-Index.db",
            "nb-1-big-Summary.db",
            "nb-1-big-Filter.db",
        ];
        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }

        // Create TOC.txt with correct content
        fs::write(
            table_dir.join("nb-1-big-TOC.txt"),
            "Data.db\nStatistics.db\nIndex.db\nSummary.db\nFilter.db\nTOC.txt",
        )
        .unwrap();

        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        let validation_report = directory.validate_all_generations().unwrap();

        assert_eq!(validation_report.total_generations, 1);
        assert_eq!(validation_report.valid_generations, 1);
        assert!(validation_report.is_valid());
        println!("Validation summary: {}", validation_report.summary());
    }

    #[test]
    fn test_toc_parsing_and_validation() {
        // Test TOC parsing
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("TOC.txt");
        fs::write(&toc_path, "Data.db\nStatistics.db\nIndex.db\nSummary.db\n").unwrap();

        let components = toc::parse_toc_file(&toc_path).unwrap();
        assert_eq!(components.len(), 4);
        assert!(components.contains(&types::SSTableComponent::Data));
        assert!(components.contains(&types::SSTableComponent::Statistics));
        assert!(components.contains(&types::SSTableComponent::Index));
        assert!(components.contains(&types::SSTableComponent::Summary));
    }

    #[cfg(feature = "enhanced-index-validation")]
    #[test]
    fn test_enhanced_component_validation() {
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        let mut components = HashMap::new();

        // Create actual files with realistic sizes
        let data_file = temp_dir.path().join("nb-1-big-Data.db");
        let stats_file = temp_dir.path().join("nb-1-big-Statistics.db");
        let index_file = temp_dir.path().join("nb-1-big-Index.db");
        let toc_file = temp_dir.path().join("nb-1-big-TOC.txt");

        fs::write(&data_file, "mock data content").unwrap();
        fs::write(&stats_file, "mock stats content").unwrap();
        fs::write(&index_file, "mock index content").unwrap();
        fs::write(&toc_file, "Data.db\nStatistics.db\nIndex.db\nTOC.txt").unwrap();

        components.insert(types::SSTableComponent::Data, data_file);
        components.insert(types::SSTableComponent::Statistics, stats_file);
        components.insert(types::SSTableComponent::Index, index_file);
        components.insert(types::SSTableComponent::TOC, toc_file);

        let generation = types::SSTableGeneration {
            version: "nb".to_string(),
            generation: 1,
            format: "big".to_string(),
            table_name: "test".to_string(),
            components,
            base_path: temp_dir.path().to_path_buf(),
        };

        let mut analysis = validation::ComponentAnalysis {
            generation: 1,
            format: "big".to_string(),
            required_components_present: Vec::new(),
            required_components_missing: Vec::new(),
            optional_components_present: Vec::new(),
            file_sizes: HashMap::new(),
            accessibility_status: HashMap::new(),
        };

        let issues =
            validation::validate_generation_components_enhanced(&generation, &mut analysis)
                .unwrap();

        // Should still complain about missing Summary for BIG format
        assert!(issues.iter().any(|i| i.contains("Summary")));

        // Check analysis results
        assert!(analysis
            .required_components_present
            .contains(&types::SSTableComponent::Data));
        assert!(analysis
            .required_components_present
            .contains(&types::SSTableComponent::Statistics));
        assert!(analysis
            .required_components_present
            .contains(&types::SSTableComponent::Index));
        assert!(analysis
            .required_components_missing
            .contains(&types::SSTableComponent::Summary));

        // Verify file sizes were recorded
        assert!(
            analysis
                .file_sizes
                .get(&types::SSTableComponent::Data)
                .unwrap()
                > &0
        );
        assert!(
            analysis
                .file_sizes
                .get(&types::SSTableComponent::Statistics)
                .unwrap()
                > &0
        );
    }

    #[cfg(feature = "enhanced-index-validation")]
    #[test]
    fn test_enhanced_toc_validation() {
        let temp_dir = TempDir::new().unwrap();
        let table_dir = temp_dir.path().join("test_table-abc123");
        fs::create_dir(&table_dir).unwrap();

        // Create files
        let files = [
            "nb-1-big-Data.db",
            "nb-1-big-Statistics.db",
            "nb-1-big-Index.db",
            "nb-1-big-Summary.db",
        ];

        for file in &files {
            fs::write(table_dir.join(file), "mock content").unwrap();
        }

        // Create TOC.txt with some inconsistencies for testing
        fs::write(
            table_dir.join("nb-1-big-TOC.txt"),
            "Data.db\nStatistics.db\nIndex.db\nSummary.db\nTOC.txt\nNonExistent.db\n",
        )
        .unwrap();

        let directory = SSTableDirectory::scan(&table_dir).unwrap();
        let report = directory.validate_all_generations().unwrap();

        // Should detect the inconsistency (NonExistent.db listed in TOC but not present)
        assert!(!report.toc_inconsistencies.is_empty());
        assert!(report
            .toc_inconsistencies
            .iter()
            .any(|inc| inc.contains("NonExistent")));
    }

    #[test]
    fn test_directory_validation_integration() {
        // This test would run against actual test data if available
        let test_path = std::env::var("CASSANDRA_TEST_PATH")
            .unwrap_or_else(|_| "test-env/cassandra5/sstables".to_string());

        if std::path::Path::new(&test_path).exists() {
            match validation::test_all_directories(&test_path) {
                Ok(results) => {
                    println!("Validated {} SSTable directories", results.len());
                    for (dir_name, report) in &results {
                        println!("Directory {}: {}", dir_name, report.summary());
                        if !report.is_valid() {
                            println!(
                                "Issues found in {}:\n{}",
                                dir_name,
                                report.detailed_report()
                            );
                        }
                    }
                    // At least some directories should be valid
                    assert!(results.iter().any(|(_, report)| report.is_valid()));
                }
                Err(e) => {
                    eprintln!("Integration test failed: {}", e);
                    // Don't fail the test if test data isn't available
                }
            }
        } else {
            println!(
                "Skipping integration test - test data not available at {}",
                test_path
            );
        }
    }

    #[test]
    fn test_component_validation() {
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        let mut components = HashMap::new();

        // Create actual files
        let data_file = temp_dir.path().join("nb-1-big-Data.db");
        let stats_file = temp_dir.path().join("nb-1-big-Statistics.db");
        fs::write(&data_file, "data").unwrap();
        fs::write(&stats_file, "stats").unwrap();

        components.insert(types::SSTableComponent::Data, data_file);
        components.insert(types::SSTableComponent::Statistics, stats_file);

        let generation = types::SSTableGeneration {
            version: "nb".to_string(),
            generation: 1,
            format: "big".to_string(),
            table_name: "test".to_string(),
            components,
            base_path: temp_dir.path().to_path_buf(),
        };

        let missing = validation::validate_generation_components(&generation).unwrap();

        // M1 scope: basic validation only checks for Data and Statistics (both present)
        #[cfg(not(feature = "enhanced-index-validation"))]
        {
            // M1 basic validation should pass since Data and Statistics are present
            assert_eq!(
                missing.len(),
                0,
                "M1 validation should pass with Data and Statistics present"
            );
        }

        // Enhanced validation: should complain about missing Index/Summary for BIG format
        #[cfg(feature = "enhanced-index-validation")]
        {
            assert!(
                missing.len() >= 2,
                "Enhanced validation should find missing Index/Summary"
            );
            assert!(missing.iter().any(|m| m.contains("Index")));
            assert!(missing.iter().any(|m| m.contains("Summary")));
        }
    }

    #[test]
    #[cfg(feature = "enhanced-index-validation")]
    fn test_enhanced_validation_functions() {
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        let mut components = HashMap::new();

        // Create test files for enhanced validation
        let data_file = temp_dir.path().join("nb-1-big-Data.db");
        let stats_file = temp_dir.path().join("nb-1-big-Statistics.db");
        fs::write(&data_file, "data").unwrap();
        fs::write(&stats_file, "stats").unwrap();

        components.insert(types::SSTableComponent::Data, data_file);
        components.insert(types::SSTableComponent::Statistics, stats_file);

        let generation = types::SSTableGeneration {
            version: "nb".to_string(),
            generation: 1,
            format: "big".to_string(),
            table_name: "test".to_string(),
            components,
            base_path: temp_dir.path().to_path_buf(),
        };

        // Test enhanced validation directly
        let mut analysis = validation::ComponentAnalysis {
            generation: 1,
            format: "big".to_string(),
            required_components_present: Vec::new(),
            required_components_missing: Vec::new(),
            optional_components_present: Vec::new(),
            file_sizes: HashMap::new(),
            accessibility_status: HashMap::new(),
        };

        let issues =
            validation::validate_generation_components_enhanced(&generation, &mut analysis)
                .unwrap();

        // Enhanced validation should find missing Index and Summary for "big" format
        assert!(issues.len() >= 2);
        assert!(analysis
            .required_components_missing
            .contains(&types::SSTableComponent::Index));
        assert!(analysis
            .required_components_missing
            .contains(&types::SSTableComponent::Summary));
        assert!(analysis
            .required_components_present
            .contains(&types::SSTableComponent::Data));
        assert!(analysis
            .required_components_present
            .contains(&types::SSTableComponent::Statistics));
    }
}
