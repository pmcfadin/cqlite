//! Hex Dump Analyzer for Cassandra 5+ SSTable Format
//!
//! This tool provides detailed hex analysis of SSTable files with format-aware
//! interpretation of structures, magic numbers, and data layouts.

use clap::{Arg, Command};
use colored::*;
use format_validator::{
    format_constants::*,
    utils::{format_hex_dump, read_file_safe},
    SSTableFileType, ValidationError,
};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("hex-analyzer")
        .about("Hex dump analyzer for Cassandra 5+ SSTable files")
        .version("1.0")
        .arg(
            Arg::new("file")
                .help("SSTable file to analyze")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("offset")
                .help("Starting offset (default: 0)")
                .long("offset")
                .short('o')
                .value_name("BYTES")
                .default_value("0"),
        )
        .arg(
            Arg::new("length")
                .help("Number of bytes to analyze (default: 512)")
                .long("length")
                .short('l')
                .value_name("BYTES")
                .default_value("512"),
        )
        .arg(
            Arg::new("format-aware")
                .help("Enable format-aware interpretation")
                .long("format-aware")
                .short('f')
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("all")
                .help("Analyze entire file (ignores offset/length)")
                .long("all")
                .short('a')
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("magic-scan")
                .help("Scan for magic numbers throughout file")
                .long("magic-scan")
                .short('m')
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("vint-scan")
                .help("Scan for VInt structures")
                .long("vint-scan")
                .short('v')
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let file_path = Path::new(matches.get_one::<String>("file").unwrap());
    let offset: usize = matches.get_one::<String>("offset").unwrap().parse()?;
    let length: usize = matches.get_one::<String>("length").unwrap().parse()?;
    let format_aware = matches.get_flag("format-aware");
    let analyze_all = matches.get_flag("all");
    let magic_scan = matches.get_flag("magic-scan");
    let vint_scan = matches.get_flag("vint-scan");

    println!("{}", "Cassandra 5+ SSTable Hex Analyzer".bright_cyan().bold());
    println!("{}", "=".repeat(50).bright_cyan());
    println!();

    // Read file
    let data = read_file_safe(file_path, 100 * 1024 * 1024)?; // 100MB limit
    let file_type = SSTableFileType::from_path(file_path);

    println!("📁 {}: {} ({} bytes)", "File".bright_yellow(), file_path.display(), data.len());
    println!("📋 {}: {:?}", "Detected Type".bright_yellow(), file_type);
    println!();

    // Format-aware analysis
    if format_aware {
        perform_format_analysis(&data, file_type)?;
        println!();
    }

    // Magic number scanning
    if magic_scan {
        scan_magic_numbers(&data)?;
        println!();
    }

    // VInt scanning
    if vint_scan {
        scan_vints(&data)?;
        println!();
    }

    // Hex dump
    let (dump_offset, dump_length) = if analyze_all {
        (0, data.len().min(4096)) // Limit to first 4KB for display
    } else {
        (offset, length)
    };

    println!("{}", "Hex Dump Analysis".bright_green().bold());
    println!("{}", "─".repeat(50).bright_green());
    println!();

    let hex_dump = format_hex_dump(&data, dump_offset, dump_length);
    println!("{}", hex_dump);

    if analyze_all && data.len() > 4096 {
        println!("{}", format!("... (showing first 4KB of {} total bytes)", data.len()).bright_black());
    }

    Ok(())
}

fn perform_format_analysis(data: &[u8], file_type: SSTableFileType) -> Result<(), ValidationError> {
    println!("{}", "Format-Aware Analysis".bright_green().bold());
    println!("{}", "─".repeat(50).bright_green());

    match file_type {
        SSTableFileType::Data => analyze_data_file(data)?,
        SSTableFileType::Index => analyze_index_file(data)?,
        SSTableFileType::Statistics => analyze_statistics_file(data)?,
        SSTableFileType::Partitions => analyze_bti_partitions_file(data)?,
        SSTableFileType::Rows => analyze_bti_rows_file(data)?,
        _ => {
            println!("⚠️  {}: Format analysis not implemented for {:?}", "Warning".bright_yellow(), file_type);
        }
    }

    Ok(())
}

fn analyze_data_file(data: &[u8]) -> Result<(), ValidationError> {
    if data.len() < 6 {
        return Err(ValidationError::FileTruncated { expected: 6, found: data.len() });
    }

    // Check magic number
    let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let version = u16::from_be_bytes([data[4], data[5]]);

    println!("🔮 {}: {:#010x}", "Magic Number".bright_blue(), magic);
    match magic {
        BIG_FORMAT_OA_MAGIC => println!("   ✅ Valid BigFormat 'oa' magic"),
        BTI_FORMAT_DA_MAGIC => println!("   ✅ Valid BTI 'da' magic"),
        _ => println!("   ❌ Unknown magic number"),
    }

    println!("📊 {}: {:#06x} ({})", "Version".bright_blue(), version, version);
    if version == SUPPORTED_VERSION {
        println!("   ✅ Supported version");
    } else {
        println!("   ⚠️ Unsupported version");
    }

    // Analyze flags if present
    if data.len() >= 10 {
        let flags = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
        println!("🏁 {}: {:#010x}", "Flags".bright_blue(), flags);
        analyze_flags(flags);
    }

    Ok(())
}

fn analyze_flags(flags: u32) {
    println!("   Basic Flags (0-7):");
    if flags & 0x01 != 0 { println!("     🗜️  Has compression"); }
    if flags & 0x02 != 0 { println!("     📊 Has static columns"); }
    if flags & 0x04 != 0 { println!("     📋 Has regular columns"); }
    if flags & 0x08 != 0 { println!("     🔗 Has complex columns"); }
    if flags & 0x10 != 0 { println!("     🗑️  Has partition-level deletion"); }
    if flags & 0x20 != 0 { println!("     ⏱️  Has TTL data"); }

    println!("   Feature Flags (8-15):");
    if flags & 0x0100 != 0 { println!("     🔑 Key range support enabled"); }
    if flags & 0x0200 != 0 { println!("     ⏰ Long deletion time format"); }
    if flags & 0x0400 != 0 { println!("     🎯 Token space coverage present"); }
    if flags & 0x0800 != 0 { println!("     📈 Enhanced min/max timestamps"); }

    println!("   Compression Flags (16-23):");
    if flags & 0x010000 != 0 { println!("     🚀 LZ4 compression"); }
    if flags & 0x020000 != 0 { println!("     ⚡ Snappy compression"); }
    if flags & 0x040000 != 0 { println!("     📦 Deflate compression"); }
    if flags & 0x080000 != 0 { println!("     🔧 Custom compression"); }
}

fn analyze_index_file(data: &[u8]) -> Result<(), ValidationError> {
    println!("📑 Index file analysis not yet implemented");
    Ok(())
}

fn analyze_statistics_file(data: &[u8]) -> Result<(), ValidationError> {
    if data.len() < 4 {
        return Err(ValidationError::FileTruncated { expected: 4, found: data.len() });
    }

    let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    println!("🔮 {}: {:#010x}", "Magic Number".bright_blue(), magic);
    
    if magic == STATISTICS_MAGIC {
        println!("   ✅ Valid Statistics.db magic");
    } else {
        println!("   ❌ Invalid Statistics.db magic");
    }

    Ok(())
}

fn analyze_bti_partitions_file(data: &[u8]) -> Result<(), ValidationError> {
    if data.len() < 16 {
        return Err(ValidationError::FileTruncated { expected: 16, found: data.len() });
    }

    let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let version = u16::from_be_bytes([data[4], data[5]]);
    let root_offset = u64::from_be_bytes([
        data[6], data[7], data[8], data[9],
        data[10], data[11], data[12], data[13]
    ]);
    let flags = u32::from_be_bytes([data[14], data[15], data[16], data[17]]);

    println!("🌳 {}: BTI Partitions File", "Format".bright_blue());
    println!("🔮 {}: {:#010x}", "Magic".bright_blue(), magic);
    println!("📊 {}: {:#06x}", "Version".bright_blue(), version);
    println!("🎯 {}: {:#018x}", "Root Offset".bright_blue(), root_offset);
    println!("🏁 {}: {:#010x}", "Flags".bright_blue(), flags);

    Ok(())
}

fn analyze_bti_rows_file(data: &[u8]) -> Result<(), ValidationError> {
    println!("🌲 BTI Rows file analysis not yet implemented");
    Ok(())
}

fn scan_magic_numbers(data: &[u8]) -> Result<(), ValidationError> {
    println!("{}", "Magic Number Scan".bright_green().bold());
    println!("{}", "─".repeat(50).bright_green());

    let known_magics = vec![
        (BIG_FORMAT_OA_MAGIC, "BigFormat 'oa'"),
        (BTI_FORMAT_DA_MAGIC, "BTI 'da'"),
        (STATISTICS_MAGIC, "Statistics"),
        (0x534E4150, "Snappy"),
        (0x184D2204, "LZ4"),
    ];

    let mut found_count = 0;

    for i in 0..data.len().saturating_sub(3) {
        let magic = u32::from_be_bytes([data[i], data[i+1], data[i+2], data[i+3]]);
        
        for (known_magic, name) in &known_magics {
            if magic == *known_magic {
                println!("✨ {}: {} at offset {:#010x}", 
                    "Found".bright_green(), 
                    name.bright_white(), 
                    i
                );
                found_count += 1;
            }
        }
    }

    if found_count == 0 {
        println!("ℹ️  No known magic numbers found");
    } else {
        println!("📊 Total found: {}", found_count);
    }

    Ok(())
}

fn scan_vints(data: &[u8]) -> Result<(), ValidationError> {
    println!("{}", "VInt Structure Scan".bright_green().bold());
    println!("{}", "─".repeat(50).bright_green());

    let mut found_count = 0;
    let mut i = 0;

    while i < data.len() {
        if let Ok((value, length)) = decode_vint(&data[i..]) {
            if length > 0 && length <= MAX_VINT_SIZE {
                println!("🔢 {}: {} (length: {}) at offset {:#010x}", 
                    "VInt".bright_cyan(), 
                    value, 
                    length, 
                    i
                );
                found_count += 1;
                i += length;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    if found_count == 0 {
        println!("ℹ️  No valid VInt structures found");
    } else {
        println!("📊 Total VInts found: {}", found_count);
    }

    Ok(())
}

fn decode_vint(bytes: &[u8]) -> Result<(i64, usize), ValidationError> {
    if bytes.is_empty() {
        return Err(ValidationError::InvalidVInt {
            offset: 0,
            reason: "Empty input".to_string()
        });
    }

    let first_byte = bytes[0];
    let leading_zeros = first_byte.leading_zeros() as usize;
    let length = if leading_zeros >= 8 { 1 } else { leading_zeros + 1 };

    if length > MAX_VINT_SIZE {
        return Err(ValidationError::InvalidVInt {
            offset: 0,
            reason: format!("VInt too long: {}", length)
        });
    }

    if bytes.len() < length {
        return Err(ValidationError::InvalidVInt {
            offset: 0,
            reason: "Truncated VInt".to_string()
        });
    }

    let mut value: i64;

    if length == 1 {
        value = (first_byte & 0x7F) as i64;
        if first_byte & 0x80 != 0 {
            value |= !0x7F;
        }
    } else {
        let mask = (1u8 << (8 - leading_zeros)) - 1;
        value = (first_byte & mask) as i64;

        for &byte in &bytes[1..length] {
            value = (value << 8) | (byte as i64);
        }

        let sign_bit_pos = (length * 8) - leading_zeros - 1;
        if sign_bit_pos < 64 && (value >> sign_bit_pos) & 1 != 0 {
            let sign_extend_mask = !((1i64 << (sign_bit_pos + 1)) - 1);
            value |= sign_extend_mask;
        }
    }

    Ok((value, length))
}