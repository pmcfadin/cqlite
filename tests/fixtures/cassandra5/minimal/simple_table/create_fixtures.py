#!/usr/bin/env python3
"""
Create minimal Cassandra 5 SSTable fixtures for testing.
This script generates the smallest valid SSTable files that CQLite can parse.
"""

import struct
import hashlib
import os

def create_minimal_data_db():
    """Create minimal Data.db with Cassandra 5 format header and one row."""
    data = bytearray()
    
    # Cassandra 5 SSTable format marker
    data.extend(b'nb')  # Format identifier
    data.extend(struct.pack('>H', 1))  # Version 1
    
    # Minimal header (simplified)
    data.extend(struct.pack('>I', 0))  # Partition size
    data.extend(struct.pack('>I', 1))  # Row count
    
    # Single row: key=1, value="test"
    data.extend(struct.pack('>I', 1))  # Key: integer 1
    data.extend(struct.pack('>I', 4))  # Value length
    data.extend(b'test')               # Value: "test"
    
    return bytes(data)

def create_minimal_statistics_db():
    """Create minimal Statistics.db."""
    stats = bytearray()
    stats.extend(struct.pack('>Q', 1))     # Estimated row count
    stats.extend(struct.pack('>Q', 100))   # Estimated column count
    stats.extend(struct.pack('>Q', 0))     # Min timestamp
    stats.extend(struct.pack('>Q', 0))     # Max timestamp
    stats.extend(struct.pack('>I', 0))     # Max local deletion time
    stats.extend(struct.pack('>f', 1.0))   # Compression ratio
    return bytes(stats)

def create_minimal_index_db():
    """Create minimal Index.db."""
    index = bytearray()
    # Single index entry pointing to offset 0
    index.extend(struct.pack('>I', 1))     # Key: 1
    index.extend(struct.pack('>Q', 0))     # Offset in Data.db
    return bytes(index)

def create_minimal_summary_db():
    """Create minimal Summary.db."""
    summary = bytearray()
    summary.extend(struct.pack('>I', 1))   # Number of entries
    summary.extend(struct.pack('>I', 1))   # First key
    summary.extend(struct.pack('>Q', 0))   # Offset
    return bytes(summary)

def create_minimal_filter_db():
    """Create minimal Filter.db (empty bloom filter)."""
    return struct.pack('>I', 0)  # Empty filter

def create_digest_crc32(data_content):
    """Create Digest.crc32 checksum of Data.db."""
    import zlib
    crc = zlib.crc32(data_content) & 0xffffffff
    return struct.pack('>I', crc)

def main():
    """Generate all minimal fixture files."""
    # Create Data.db
    data_content = create_minimal_data_db()
    with open('Data.db', 'wb') as f:
        f.write(data_content)
    
    # Create other components
    with open('Statistics.db', 'wb') as f:
        f.write(create_minimal_statistics_db())
    
    with open('Index.db', 'wb') as f:
        f.write(create_minimal_index_db())
    
    with open('Summary.db', 'wb') as f:
        f.write(create_minimal_summary_db())
    
    with open('Filter.db', 'wb') as f:
        f.write(create_minimal_filter_db())
    
    # Create checksum
    with open('Digest.crc32', 'wb') as f:
        f.write(create_digest_crc32(data_content))
    
    print("Created minimal Cassandra 5 SSTable fixture files:")
    for filename in ['Data.db', 'Statistics.db', 'Index.db', 'Summary.db', 'Filter.db', 'Digest.crc32', 'TOC.txt']:
        if os.path.exists(filename):
            size = os.path.getsize(filename)
            print(f"  {filename}: {size} bytes")

if __name__ == "__main__":
    main()