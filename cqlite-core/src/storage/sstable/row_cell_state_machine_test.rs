//! Comprehensive test suite for RowCellStateMachine implementation
//! Tests all state transitions, VInt parsing, and error handling

#[cfg(test)]
mod tests {
    use super::super::row_cell_state_machine::*;
    use crate::error::{Error, Result};
    use std::collections::HashMap;

    // Mock data for testing
    fn create_test_oa_header_data() -> Vec<u8> {
        let mut data = Vec::new();
        // Magic number: 0x6F610000 (big-endian)
        data.extend_from_slice(&[0x6F, 0x61, 0x00, 0x00]);
        // Format version: 0x0001
        data.extend_from_slice(&[0x00, 0x01]);
        // Flags: basic flags set
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x15]); // Has compression, static, regular columns
        // Reserved bytes (22 bytes of zeros)
        data.extend_from_slice(&vec![0; 22]);
        data
    }

    fn create_test_partition_data() -> Vec<u8> {
        let mut data = Vec::new();
        // Partition count (VInt: 2 partitions)
        data.push(0x02);
        // Min timestamp (8 bytes)
        data.extend_from_slice(&1640995200000000i64.to_be_bytes());
        // Max timestamp (8 bytes)
        data.extend_from_slice(&1640995260000000i64.to_be_bytes());
        // Partition key length (VInt: 16 bytes for UUID)
        data.push(0x10);
        // Mock UUID partition key
        data.extend_from_slice(&[
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ]);
        // Row count (VInt: 5 rows)
        data.push(0x05);
        data
    }

    #[test]
    fn test_state_machine_creation() {
        let state_machine = RowCellStateMachine::new();
        assert_eq!(state_machine.current_state(), &State::Header);
        assert_eq!(state_machine.processed_bytes(), 0);
    }

    #[test]
    fn test_header_parsing_success() {
        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_test_oa_header_data();

        let result = state_machine.process_data(&header_data);
        assert!(result.is_ok());
        assert_eq!(state_machine.current_state(), &State::PartitionKey);
        assert_eq!(state_machine.processed_bytes(), 32); // Header is 32 bytes
    }

    #[test]
    fn test_invalid_magic_number() {
        let mut state_machine = RowCellStateMachine::new();
        let mut invalid_data = create_test_oa_header_data();
        // Corrupt magic number
        invalid_data[0] = 0xFF;

        let result = state_machine.process_data(&invalid_data);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidFormat(msg) => assert!(msg.contains("Invalid magic number")),
            _ => panic!("Expected InvalidFormat error"),
        }
    }

    #[test]
    fn test_vint_decoding() {
        let mut state_machine = RowCellStateMachine::new();

        // Test various VInt values
        let test_cases = vec![
            (vec![0x00], 0i64),        // 0
            (vec![0x01], 1i64),        // 1
            (vec![0x3F], 63i64),       // 63
            (vec![0xC0, 0x40], 64i64), // 64
            (vec![0xFF], -1i64),       // -1
        ];

        for (bytes, expected) in test_cases {
            let result = state_machine.decode_vint(&bytes);
            assert!(result.is_ok());
            let (value, consumed) = result.unwrap();
            assert_eq!(value, expected);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn test_state_transitions() {
        let mut state_machine = RowCellStateMachine::new();

        // Start in Header state
        assert_eq!(state_machine.current_state(), &State::Header);

        // Process header
        let header_data = create_test_oa_header_data();
        state_machine.process_data(&header_data).unwrap();
        assert_eq!(state_machine.current_state(), &State::PartitionKey);

        // Process partition data
        let partition_data = create_test_partition_data();
        state_machine.process_data(&partition_data).unwrap();
        // Should transition to DeletionInfo or StaticRow based on flags
        assert!(matches!(
            state_machine.current_state(),
            State::DeletionInfo | State::StaticRow
        ));
    }

    #[test]
    fn test_partition_key_parsing() {
        let mut state_machine = RowCellStateMachine::new();

        // Skip to PartitionKey state
        let header_data = create_test_oa_header_data();
        state_machine.process_data(&header_data).unwrap();

        // Test partition key data
        let mut partition_data = Vec::new();
        partition_data.push(0x10); // Key length: 16 bytes
        partition_data.extend_from_slice(&[
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ]);

        let result = state_machine.process_data(&partition_data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_error_recovery() {
        let mut state_machine = RowCellStateMachine::new();

        // Try to process invalid data
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        let result = state_machine.process_data(&invalid_data);
        assert!(result.is_err());

        // State machine should remain in error-recoverable state
        assert_eq!(state_machine.current_state(), &State::Header);

        // Should be able to reset and try again
        state_machine.reset();
        assert_eq!(state_machine.current_state(), &State::Header);
        assert_eq!(state_machine.processed_bytes(), 0);
    }

    #[test]
    fn test_incremental_processing() {
        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_test_oa_header_data();

        // Process header in chunks
        let chunk_size = 8;
        let mut offset = 0;

        while offset < header_data.len() {
            let end = std::cmp::min(offset + chunk_size, header_data.len());
            let chunk = &header_data[offset..end];

            let result = state_machine.process_data(chunk);
            if offset + chunk_size >= header_data.len() {
                // Last chunk should succeed and transition
                assert!(result.is_ok());
                assert_eq!(state_machine.current_state(), &State::PartitionKey);
            } else {
                // Intermediate chunks may need more data
                assert!(result.is_ok() || matches!(result.unwrap_err(), Error::UnexpectedEof));
            }

            offset += chunk_size;
        }
    }

    #[test]
    fn test_flag_interpretation() {
        let mut state_machine = RowCellStateMachine::new();
        let mut header_data = create_test_oa_header_data();

        // Test different flag combinations
        let test_flags = vec![
            (0x01, "has_compression"),
            (0x02, "has_static_columns"),
            (0x04, "has_regular_columns"),
            (0x08, "has_complex_columns"),
            (0x10, "has_partition_deletion"),
            (0x20, "has_ttl_data"),
        ];

        for (flag_value, flag_name) in test_flags {
            // Reset state machine
            state_machine.reset();

            // Set specific flag
            header_data[7] = flag_value; // Flags are at offset 4-7, using byte 7

            let result = state_machine.process_data(&header_data);
            assert!(
                result.is_ok(),
                "Failed to process header with flag: {}",
                flag_name
            );

            // Verify flag interpretation affects state transitions
            assert_eq!(state_machine.current_state(), &State::PartitionKey);
        }
    }

    #[test]
    fn test_memory_usage() {
        let state_machine = RowCellStateMachine::new();

        // Verify minimal memory footprint
        let size = std::mem::size_of_val(&state_machine);
        assert!(
            size < 1024,
            "State machine should be lightweight, got {} bytes",
            size
        );
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let header_data = Arc::new(create_test_oa_header_data());
        let mut handles = vec![];

        // Spawn multiple threads to test thread safety
        for i in 0..4 {
            let data = Arc::clone(&header_data);
            let handle = thread::spawn(move || {
                let mut state_machine = RowCellStateMachine::new();
                let result = state_machine.process_data(&data);
                assert!(result.is_ok(), "Thread {} failed", i);
                assert_eq!(state_machine.current_state(), &State::PartitionKey);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_large_partition_handling() {
        let mut state_machine = RowCellStateMachine::new();

        // Process header first
        let header_data = create_test_oa_header_data();
        state_machine.process_data(&header_data).unwrap();

        // Create large partition key (1MB)
        let mut large_partition_data = Vec::new();
        large_partition_data.extend_from_slice(&(1024 * 1024u32).to_be_bytes()); // 1MB key length as VInt
        large_partition_data.extend_from_slice(&vec![0xAB; 1024 * 1024]); // 1MB of data

        let result = state_machine.process_data(&large_partition_data);
        assert!(result.is_ok(), "Should handle large partition keys");
    }

    #[test]
    fn test_state_machine_reuse() {
        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_test_oa_header_data();

        // Process first SSTable
        state_machine.process_data(&header_data).unwrap();
        assert_eq!(state_machine.current_state(), &State::PartitionKey);

        // Reset and process second SSTable
        state_machine.reset();
        assert_eq!(state_machine.current_state(), &State::Header);
        assert_eq!(state_machine.processed_bytes(), 0);

        // Should work identically
        state_machine.process_data(&header_data).unwrap();
        assert_eq!(state_machine.current_state(), &State::PartitionKey);
    }

    #[test]
    fn test_error_messages() {
        let mut state_machine = RowCellStateMachine::new();

        // Test various error conditions
        let error_tests = vec![
            (vec![], "UnexpectedEof"),
            (vec![0xFF, 0xFF, 0xFF, 0xFF], "Invalid magic number"),
            (vec![0x6F, 0x61, 0x00, 0x02], "Unsupported format version"), // Wrong version
        ];

        for (data, expected_error) in error_tests {
            state_machine.reset();
            let result = state_machine.process_data(&data);
            assert!(result.is_err());

            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains(expected_error)
                    || error_msg
                        .to_lowercase()
                        .contains(&expected_error.to_lowercase()),
                "Expected error containing '{}', got '{}'",
                expected_error,
                error_msg
            );
        }
    }

    #[test]
    fn test_performance_baseline() {
        use std::time::Instant;

        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_test_oa_header_data();

        // Measure processing time for baseline
        let start = Instant::now();
        for _ in 0..1000 {
            state_machine.reset();
            state_machine.process_data(&header_data).unwrap();
        }
        let duration = start.elapsed();

        // Should process 1000 headers in under 100ms
        assert!(
            duration.as_millis() < 100,
            "Performance regression: took {}ms for 1000 headers",
            duration.as_millis()
        );
    }
}
