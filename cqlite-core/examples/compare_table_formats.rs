use std::fs::File;
use std::io::{Read, Write};

fn decompress_snappy_chunk(
    data_db_path: &str,
    chunk_size_without_crc: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut file = File::open(data_db_path)?;
    let mut compressed_data = vec![0u8; chunk_size_without_crc];
    file.read_exact(&mut compressed_data)?;

    let decompressed = snap::raw::Decoder::new().decompress_vec(&compressed_data)?;
    Ok(decompressed)
}

fn decompress_lz4_chunk(
    data_db_path: &str,
    chunk_size_without_crc: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut file = File::open(data_db_path)?;
    let mut compressed_data = vec![0u8; chunk_size_without_crc];
    file.read_exact(&mut compressed_data)?;

    // LZ4 in Cassandra uses 4-byte little-endian size prefix
    if compressed_data.len() < 4 {
        return Err("Chunk too small for LZ4 size prefix".into());
    }

    let uncompressed_size = u32::from_le_bytes([
        compressed_data[0],
        compressed_data[1],
        compressed_data[2],
        compressed_data[3],
    ]) as usize;

    println!("  LZ4 size prefix: {} bytes", uncompressed_size);

    // Decompress the data after the 4-byte prefix
    let lz4_data = &compressed_data[4..];
    let decompressed = lz4_flex::decompress(lz4_data, uncompressed_size)?;
    Ok(decompressed)
}

fn find_partition_key_in_decompressed(data: &[u8]) -> Option<usize> {
    // Partition starts with: 0x00 0x10 [16 bytes of UUID]
    // Look for the 0x00 0x10 pattern
    (0..data.len().saturating_sub(18)).find(|&i| data[i] == 0x00 && data[i + 1] == 0x10)
}

fn print_hex_with_annotations(label: &str, data: &[u8], start: usize, length: usize) {
    println!("\n{}", "=".repeat(80));
    println!("{}", label);
    println!("{}", "=".repeat(80));

    let end = (start + length).min(data.len());
    let mut offset = start;

    while offset < end {
        let line_end = (offset + 16).min(end);
        let chunk = &data[offset..line_end];

        // Print offset
        print!("{:04x}: ", offset);

        // Print hex bytes
        for (i, byte) in chunk.iter().enumerate() {
            print!("{:02x} ", byte);
            if i == 7 {
                print!(" ");
            }
        }

        // Padding
        for _ in chunk.len()..16 {
            print!("   ");
            if chunk.len() <= 8 {
                print!(" ");
            }
        }

        // Print ASCII
        print!(" |");
        for byte in chunk {
            if *byte >= 32 && *byte < 127 {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");

        offset = line_end;
    }
    println!();
}

fn parse_vint(data: &[u8], offset: &mut usize) -> Option<(u64, usize)> {
    if *offset >= data.len() {
        return None;
    }

    let first_byte = data[*offset];
    let num_extra_bytes = first_byte.leading_ones() as usize;

    // Handle special case: all 1s (0xFF) means we need 8 extra bytes
    if num_extra_bytes >= 8 {
        if *offset + 8 >= data.len() {
            return None;
        }
        let mut value = 0u64;
        for i in 0..8 {
            value = (value << 8) | data[*offset + 1 + i] as u64;
        }
        *offset += 1 + 8;
        return Some((value, 1 + 8));
    }

    if num_extra_bytes == 0 {
        let value = first_byte as u64;
        *offset += 1;
        return Some((value, 1));
    }

    if *offset + num_extra_bytes >= data.len() {
        return None;
    }

    let mask = 0xFFu8 >> num_extra_bytes;
    let mut value = (first_byte & mask) as u64;

    for i in 0..num_extra_bytes {
        value = (value << 8) | data[*offset + 1 + i] as u64;
    }

    *offset += 1 + num_extra_bytes;
    Some((value, 1 + num_extra_bytes))
}

fn analyze_row_header(label: &str, data: &[u8], start: usize) {
    println!("\n{}", "─".repeat(80));
    println!("ANALYZING ROW HEADER: {}", label);
    println!("{}", "─".repeat(80));

    let mut pos = start;

    // Show raw hex first
    println!("\nRaw bytes (first 64 bytes from row start):");
    print_hex_with_annotations("", data, start, 64);

    println!("\nParsing row header:");
    println!("  Position: 0x{:04x}", pos);

    // Parse row flags
    if pos >= data.len() {
        println!("  ERROR: Offset beyond data");
        return;
    }

    let row_flags = data[pos];
    println!(
        "  [0x{:04x}] row_flags = 0x{:02x} ({:08b})",
        pos, row_flags, row_flags
    );
    pos += 1;

    let has_extended_flags = (row_flags & 0x80) != 0;
    let has_timestamp = (row_flags & 0x04) != 0;
    let has_ttl = (row_flags & 0x08) != 0;
    let has_deletion = (row_flags & 0x10) != 0;
    let has_all_columns = (row_flags & 0x20) != 0;

    println!("    Flags:");
    println!("      HAS_EXTENDED_FLAGS (0x80): {}", has_extended_flags);
    println!("      HAS_TIMESTAMP      (0x04): {}", has_timestamp);
    println!("      HAS_TTL            (0x08): {}", has_ttl);
    println!("      HAS_DELETION       (0x10): {}", has_deletion);
    println!("      HAS_ALL_COLUMNS    (0x20): {}", has_all_columns);

    // Extended flags
    if has_extended_flags {
        if pos >= data.len() {
            println!("  ERROR: Cannot read extended_flags");
            return;
        }
        let ext_flags = data[pos];
        println!("  [0x{:04x}] extended_flags = 0x{:02x}", pos, ext_flags);
        pos += 1;
    }

    // Next bytes (what should be row_size according to docs)
    println!("\n  Next 16 bytes after flags:");
    for i in 0..16 {
        if pos + i < data.len() {
            print!("  [0x{:04x}] 0x{:02x}", pos + i, data[pos + i]);

            // Try to interpret as VInt
            let mut temp_pos = pos + i;
            if let Some((vint_val, vint_size)) = parse_vint(data, &mut temp_pos) {
                print!("  (as VInt: {} using {} bytes)", vint_val, vint_size);
            }
            println!();
        }
    }

    // Try parsing as documented format (row_size next)
    println!("\n  Attempting standard parse (row_size after flags):");
    let mut parse_pos = pos;
    if let Some((row_size, size_bytes)) = parse_vint(data, &mut parse_pos) {
        println!(
            "    [0x{:04x}] row_size = {} ({} bytes)",
            pos, row_size, size_bytes
        );

        if let Some((prev_size, prev_bytes)) = parse_vint(data, &mut parse_pos) {
            println!(
                "    [0x{:04x}] prev_size = {} ({} bytes)",
                pos + size_bytes,
                prev_size,
                prev_bytes
            );
        }
    } else {
        println!("    Could not parse row_size as VInt");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "█".repeat(80));
    println!("HEX LAYOUT COMPARISON: simple_table (NO clustering) vs sensor_data (HAS clustering)");
    println!("{}", "█".repeat(80));

    // Decompress simple_table (Snappy)
    println!("\n\n=== DECOMPRESSING simple_table (Snappy) ===");
    let simple_data = decompress_snappy_chunk(
        "test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        15867
    )?;
    println!("Decompressed {} bytes", simple_data.len());

    // Find partition key
    let simple_partition_start = find_partition_key_in_decompressed(&simple_data)
        .ok_or("Could not find partition key marker in simple_table")?;
    println!(
        "Found partition key at offset 0x{:04x}",
        simple_partition_start
    );

    // Decompress sensor_data (LZ4)
    // First chunk size: 11678 bytes total, minus 4-byte CRC = 11674 bytes
    println!("\n\n=== DECOMPRESSING sensor_data (LZ4) ===");
    let sensor_data = decompress_lz4_chunk(
        "test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
        11674
    )?;
    println!("Decompressed {} bytes", sensor_data.len());

    // Find partition key
    let sensor_partition_start = find_partition_key_in_decompressed(&sensor_data)
        .ok_or("Could not find partition key marker in sensor_data")?;
    println!(
        "Found partition key at offset 0x{:04x}",
        sensor_partition_start
    );

    // Show partition headers
    print_hex_with_annotations(
        "simple_table: Partition header (0x0000 - 0x0040)",
        &simple_data,
        0,
        64,
    );

    print_hex_with_annotations(
        "sensor_data: Partition header (0x0000 - 0x0040)",
        &sensor_data,
        0,
        64,
    );

    // Find row starts (after partition key + liveness)
    // Partition key format: 0x00 0x10 [16 bytes UUID]
    // After partition key comes row data

    println!("\n\n{}", "█".repeat(80));
    println!("PARTITION KEY + INITIAL ROW DATA");
    println!("{}", "█".repeat(80));

    // simple_table: partition key at offset, then row starts
    let simple_row_start = simple_partition_start + 2 + 16; // After 0x00 0x10 and 16-byte UUID
    print_hex_with_annotations(
        &format!(
            "simple_table: Partition key + first 128 bytes (from 0x{:04x})",
            simple_partition_start
        ),
        &simple_data,
        simple_partition_start,
        128,
    );

    // sensor_data: same pattern
    let sensor_row_start = sensor_partition_start + 2 + 16;
    print_hex_with_annotations(
        &format!(
            "sensor_data: Partition key + first 128 bytes (from 0x{:04x})",
            sensor_partition_start
        ),
        &sensor_data,
        sensor_partition_start,
        128,
    );

    // Detailed row header analysis
    analyze_row_header(
        "simple_table (NO clustering keys)",
        &simple_data,
        simple_row_start,
    );
    analyze_row_header(
        "sensor_data (HAS clustering key: timestamp)",
        &sensor_data,
        sensor_row_start,
    );

    // Save decompressed data for further analysis
    let mut simple_out = File::create("/tmp/simple_table_chunk0.bin")?;
    simple_out.write_all(&simple_data)?;
    println!("\n\nWrote decompressed simple_table to: /tmp/simple_table_chunk0.bin");

    let mut sensor_out = File::create("/tmp/sensor_data_chunk0.bin")?;
    sensor_out.write_all(&sensor_data)?;
    println!("Wrote decompressed sensor_data to: /tmp/sensor_data_chunk0.bin");

    println!("\n{}", "█".repeat(80));
    println!("ANALYSIS COMPLETE");
    println!("{}", "█".repeat(80));

    Ok(())
}
