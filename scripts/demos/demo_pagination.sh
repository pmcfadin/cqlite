#!/bin/bash

# Demo script for CQLite CLI pagination and performance features
# Shows the enhanced --limit, --skip, --page-size, --parallel, and --buffer-size flags

echo "🚀 CQLite CLI Pagination & Performance Demo"
echo "==========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Sample SSTable and schema files (adjust paths as needed)
SSTABLE_PATH="./test-data/users-table/"
SCHEMA_PATH="./test-data/users-schema.json"

echo -e "${BLUE}📋 Available Pagination Commands:${NC}"
echo ""

echo -e "${GREEN}1. Basic pagination with limit and skip:${NC}"
echo "   cqlite read ${SSTABLE_PATH} --schema ${SCHEMA_PATH} --limit 10 --skip 5"
echo "   → Shows 10 rows starting from row 6"
echo ""

echo -e "${GREEN}2. Large table streaming with page size:${NC}"
echo "   cqlite read ${SSTABLE_PATH} --schema ${SCHEMA_PATH} --limit 100 --page-size 25"
echo "   → Streams 100 rows in chunks of 25 for memory efficiency"
echo ""

echo -e "${GREEN}3. High-performance parallel processing:${NC}"
echo "   cqlite read ${SSTABLE_PATH} --schema ${SCHEMA_PATH} --limit 1000 --parallel"
echo "   → Uses parallel processing for faster data reading"
echo ""

echo -e "${GREEN}4. Memory-optimized processing:${NC}"
echo "   cqlite read ${SSTABLE_PATH} --schema ${SCHEMA_PATH} --max-memory-mb 50 --buffer-size 16384"
echo "   → Limits memory usage to 50MB with 16KB I/O buffers"
echo ""

echo -e "${GREEN}5. Complete performance-optimized command:${NC}"
echo "   cqlite read ${SSTABLE_PATH} --schema ${SCHEMA_PATH} \\"
echo "      --limit 500 --skip 100 --page-size 50 --parallel \\"
echo "      --buffer-size 8192 --max-memory-mb 100 --format json"
echo "   → Full featured pagination with all optimizations"
echo ""

echo -e "${BLUE}📊 SELECT Query Pagination:${NC}"
echo ""

echo -e "${GREEN}6. Paginated SELECT queries:${NC}"
echo "   cqlite select ${SSTABLE_PATH} --schema ${SCHEMA_PATH} \\"
echo "      'SELECT * FROM users LIMIT 50' --page-size 25 --parallel"
echo "   → SELECT query with streaming pagination"
echo ""

echo -e "${GREEN}7. Complex query with filtering:${NC}"
echo "   cqlite select ${SSTABLE_PATH} --schema ${SCHEMA_PATH} \\"
echo "      \"SELECT name, email FROM users WHERE status = 'active'\" \\"
echo "      --page-size 100 --buffer-size 16384 --format csv"
echo "   → Filtered query with CSV output and performance tuning"
echo ""

echo -e "${BLUE}📈 Performance Benefits:${NC}"
echo "• 📄 Cursor-based pagination for large datasets"
echo "• ⚡ Parallel processing support"
echo "• 💾 Memory-efficient streaming"
echo "• 🔧 Configurable I/O buffer sizes"
echo "• 📊 Real-time progress indicators"
echo "• 🎯 Memory pool management"
echo "• 📈 Throughput monitoring"
echo ""

echo -e "${YELLOW}Interactive Mode:${NC}"
echo "In interactive mode, pagination provides:"
echo "• Page navigation (next/prev)"
echo "• Dynamic page size adjustment"
echo "• Progress tracking"
echo "• Memory usage monitoring"
echo ""

echo -e "${BLUE}🎯 Example Usage Scenarios:${NC}"
echo ""

echo -e "${GREEN}Large Table Analysis:${NC}"
echo "# Process 10,000 rows in memory-efficient chunks"
echo "cqlite read large-table/ --schema schema.json \\"
echo "   --limit 10000 --page-size 500 --parallel \\"
echo "   --max-memory-mb 200 --format json > analysis.json"
echo ""

echo -e "${GREEN}Data Export with Pagination:${NC}"
echo "# Export specific page ranges"
echo "cqlite read user-data/ --schema users.json \\"
echo "   --skip 1000 --limit 100 --format csv > users_page_11.csv"
echo ""

echo -e "${GREEN}Performance Testing:${NC}"
echo "# Test different buffer sizes for optimal performance"
echo "cqlite read test-data/ --schema test.json \\"
echo "   --limit 5000 --buffer-size 32768 --parallel \\"
echo "   --max-memory-mb 500"
echo ""

echo -e "${BLUE}🔧 Configuration Options:${NC}"
echo "• --limit N          : Maximum rows to display"
echo "• --skip N           : Number of rows to skip (OFFSET)"
echo "• --page-size N      : Rows per processing chunk (default: 50)"
echo "• --buffer-size N    : I/O buffer size in bytes (default: 8192)"
echo "• --parallel         : Enable parallel processing"
echo "• --max-memory-mb N  : Memory limit in MB (default: 100)"
echo ""

echo -e "${YELLOW}📚 For more information:${NC}"
echo "• Run 'cqlite read --help' for complete options"
echo "• Run 'cqlite select --help' for SELECT query options"
echo "• Use --format to choose output format (table, json, csv, yaml)"
echo ""

echo -e "${GREEN}✅ Demo complete! Try the commands above with your SSTable files.${NC}"