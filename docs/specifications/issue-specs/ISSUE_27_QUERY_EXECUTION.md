# Issue #27: 🔍 Create working query execution against real SSTable data

## 🎯 **Priority: HIGH** - Core Value Proposition

**Status**: Query stubs exist but need real implementation  
**Impact**: Core functionality - reading and querying Cassandra data  
**Estimated Effort**: 4-5 days  
**Assigned**: TBD  

---

## 📋 **Problem Statement**

CQLite's core value proposition is providing "SQLite-like local access to Apache Cassandra SSTables." Currently, the query execution engine has stub implementations that return empty results. We need to implement real CQL query parsing and execution against SSTable data.

Current limitations:
- `QueryExecutor` returns empty `QueryResult` structs
- No CQL parsing or query planning
- No integration with SSTable reading functionality
- Missing filtering, sorting, and aggregation capabilities
- No optimization for SSTable-specific query patterns

## ✅ **Acceptance Criteria**

### **Basic Query Support**
- [ ] Execute `SELECT * FROM table` queries against SSTable files
- [ ] Support `WHERE` clauses with partition key filtering
- [ ] Handle `LIMIT` and basic result pagination
- [ ] Support column selection (`SELECT id, name FROM table`)
- [ ] Return results in multiple formats (table, JSON, CSV)

### **CQL Compatibility**
- [ ] Parse standard CQL SELECT syntax
- [ ] Support all basic CQL data types (text, int, uuid, timestamp, etc.)
- [ ] Handle collection types (list, set, map) properly
- [ ] Support clustering key filtering and ordering
- [ ] Proper handling of null values and tombstones

### **SSTable Integration**
- [ ] Direct reading from SSTable files without Cassandra cluster
- [ ] Efficient streaming for large result sets
- [ ] Schema-aware query validation
- [ ] Index utilization where available (bloom filters, partition index)
- [ ] Multi-SSTable query support for complete table views

### **Performance Requirements**
- [ ] Query execution < 5 seconds for typical datasets
- [ ] Memory usage < 128MB for large result sets (streaming)
- [ ] Support for files up to 10GB in size
- [ ] Parallel processing for multi-file queries

## 🔧 **Technical Requirements**

### **Query Engine Architecture**
```rust
pub struct QueryEngine {
    sstable_manager: SSTableManager,
    schema_registry: SchemaRegistry,
    query_optimizer: QueryOptimizer,
    result_formatter: ResultFormatter,
}

impl QueryEngine {
    pub async fn execute(&self, query: &str) -> Result<QueryResult> {
        // 1. Parse CQL query
        let parsed = self.parse_query(query)?;
        
        // 2. Validate against schema
        let validated = self.validate_query(parsed)?;
        
        // 3. Optimize execution plan
        let plan = self.optimize_query(validated)?;
        
        // 4. Execute against SSTables
        let raw_results = self.execute_plan(plan).await?;
        
        // 5. Format and return results
        Ok(self.format_results(raw_results)?)
    }
}
```

### **CQL Parser Implementation**
```rust
pub struct CqlParser {
    // Leverage existing parser infrastructure
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub query_type: QueryType,
    pub table: TableRef,
    pub columns: Vec<ColumnRef>,
    pub where_clause: Option<WhereClause>,
    pub limit: Option<u32>,
    pub order_by: Vec<OrderByClause>,
}

pub enum QueryType {
    Select,
    // Future: Insert, Update, Delete
}
```

### **Query Execution Pipeline**
```rust  
pub struct ExecutionPlan {
    pub sstable_files: Vec<PathBuf>,
    pub filters: Vec<Filter>,
    pub projections: Vec<Projection>,
    pub ordering: Option<Ordering>,
    pub limit: Option<u32>,
}

impl ExecutionPlan {
    pub async fn execute(&self) -> Result<RawQueryResult> {
        let mut result_stream = self.create_result_stream().await?;
        let mut results = Vec::new();
        
        while let Some(row) = result_stream.next().await? {
            if self.matches_filters(&row)? {
                results.push(self.apply_projections(row)?);
                
                if let Some(limit) = self.limit {
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
        }
        
        Ok(RawQueryResult { rows: results })
    }
}
```

### **Integration with Existing Code**
Replace the current stub implementation in `commands/mod.rs`:

```rust
// Current stub:
impl QueryExecutor {
    pub async fn execute(&self, _query: &str) -> Result<QueryResult> {
        Ok(QueryResult {
            rows: Vec::new(),
            execution_time_ms: 0.0,
        })
    }
}

// New implementation:
impl QueryExecutor {
    pub async fn execute(&self, query: &str) -> Result<QueryResult> {
        let start_time = Instant::now();
        
        // Parse and validate query
        let parsed_query = self.parser.parse(query)?;
        self.validator.validate(&parsed_query, &self.schema)?;
        
        // Create and execute plan
        let plan = self.planner.create_plan(parsed_query)?;
        let raw_results = plan.execute().await?;
        
        // Format results
        let formatted_rows = self.formatter.format_rows(raw_results)?;
        
        Ok(QueryResult {
            rows: formatted_rows,
            execution_time_ms: start_time.elapsed().as_millis() as f64,
        })
    }
}
```

## 🎯 **Query Support Roadmap**

### **Phase 1: Basic SELECT Queries**
```sql
-- Supported in Phase 1
SELECT * FROM users;
SELECT id, name, email FROM users;
SELECT * FROM users LIMIT 100;
SELECT * FROM users WHERE id = 'uuid-value';
```

### **Phase 2: WHERE Clause Support**
```sql
-- Supported in Phase 2  
SELECT * FROM users WHERE id IN ('uuid1', 'uuid2');
SELECT * FROM users WHERE created_at >= '2024-01-01';
SELECT * FROM users WHERE status = 'active' AND region = 'us-east';
```

### **Phase 3: Advanced Features**
```sql
-- Supported in Phase 3
SELECT * FROM users WHERE token(id) > token('some-uuid');
SELECT COUNT(*) FROM users;
SELECT * FROM users ORDER BY created_at DESC;
SELECT DISTINCT region FROM users;
```

## 🧪 **Testing Strategy**

### **Unit Tests**
```rust
#[tokio::test]
async fn test_basic_select_query() {
    let test_data = create_test_sstable_with_users().await?;
    let executor = QueryExecutor::new_with_test_data(test_data).await?;
    
    let result = executor.execute("SELECT * FROM users").await?;
    
    assert!(!result.rows.is_empty());
    assert!(result.execution_time_ms > 0.0);
    
    // Verify data integrity
    let first_row = &result.rows[0];
    assert!(first_row.data.contains_key("id"));
    assert!(first_row.data.contains_key("name"));
}

#[tokio::test]
async fn test_where_clause_filtering() {
    let test_data = create_test_sstable_with_users().await?;
    let executor = QueryExecutor::new_with_test_data(test_data).await?;
    
    let result = executor.execute(
        "SELECT * FROM users WHERE name = 'John Doe'"
    ).await?;
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].data.get("name").unwrap(), "John Doe");
}
```

### **Integration Tests**
```rust
#[tokio::test]
async fn test_query_with_real_cassandra_data() {
    let cassandra_sstables = generate_real_test_data().await?;
    let executor = QueryExecutor::new_with_sstables(cassandra_sstables).await?;
    
    // Test queries that we know should work with our test data
    let queries = vec![
        "SELECT * FROM users LIMIT 10",
        "SELECT id, name FROM users WHERE id = ?",
        "SELECT COUNT(*) FROM users",
    ];
    
    for query in queries {
        let result = executor.execute(query).await?;
        assert!(result.rows.len() > 0 || query.contains("COUNT"));
    }
}
```

### **Property-Based Tests**
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_query_parsing_never_panics(query in "SELECT .{0,100} FROM .{0,20}") {
        let parser = CqlParser::new();
        
        // Should never panic, only return Ok or Err
        let _ = parser.parse(&query);
    }
    
    #[test]
    fn test_limit_bounds_checking(limit in 0u32..1000000u32) {
        let query = format!("SELECT * FROM users LIMIT {}", limit);
        let parser = CqlParser::new();
        
        if let Ok(parsed) = parser.parse(&query) {
            assert!(parsed.limit.unwrap() == limit);
        }
    }
}
```

## 📊 **Performance Requirements**

### **Execution Time Targets**
- [ ] Simple SELECT queries: < 100ms
- [ ] Filtered queries: < 1 second  
- [ ] Large result sets (10K+ rows): < 5 seconds
- [ ] Complex queries with joins: < 10 seconds

### **Memory Usage Targets**
- [ ] Query parsing: < 1MB overhead
- [ ] Result streaming: < 64MB resident memory
- [ ] Large file handling: Constant memory usage regardless of file size
- [ ] Concurrent queries: Linear memory scaling

### **Throughput Targets**
- [ ] Process > 10MB/second of SSTable data
- [ ] Handle > 1000 rows/second query processing
- [ ] Support concurrent queries without degradation
- [ ] Scale to multi-GB SSTable files

## 🔄 **Data Flow Architecture**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CQL Query     │───▶│  Query Parser   │───▶│ Query Validator │
│  "SELECT * ..." │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Formatted       │◀───│ Result          │◀───│ Query Executor  │
│ QueryResult     │    │ Formatter       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Display/Export  │◀───│ SSTable Data    │◀───│ SSTable Reader  │
│                 │    │ Streaming       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 **Implementation Plan**

### **Phase 1: Foundation (Days 1-2)**
1. Implement basic CQL parser for SELECT statements
2. Create query validation framework
3. Build basic execution engine with SSTable integration
4. Add simple filtering and projection capabilities

### **Phase 2: Core Features (Days 2-4)**
1. Implement WHERE clause parsing and execution
2. Add support for all basic CQL data types
3. Implement LIMIT and basic result pagination
4. Add multiple output format support

### **Phase 3: Optimization (Days 4-5)**
1. Add query optimization and planning
2. Implement streaming for large result sets
3. Add performance monitoring and benchmarking
4. Optimize memory usage and execution speed

### **Phase 4: Integration (Day 5)**
1. Integrate with REPL and CLI commands
2. Add comprehensive error handling
3. Complete testing and validation
4. Update documentation and examples

## 📖 **Documentation Needs**

- [ ] CQL syntax support documentation
- [ ] Query performance tuning guide
- [ ] Examples for common query patterns
- [ ] Troubleshooting guide for query issues
- [ ] Comparison with Cassandra CQL capabilities

## ⚠️ **Risk Factors**

- **High**: CQL parsing complexity and completeness
- **Medium**: Performance optimization for large datasets
- **Medium**: Integration complexity with existing SSTable reading
- **Low**: Memory management for streaming results

## 💡 **Success Criteria**

### **Functional Success**
- [ ] Execute basic SELECT queries against real SSTable data
- [ ] Return accurate, formatted results
- [ ] Handle common CQL syntax correctly
- [ ] Integrate seamlessly with CLI and REPL

### **Performance Success**
- [ ] Meet or exceed performance targets
- [ ] Scale to production-sized SSTable files
- [ ] Maintain responsive user experience
- [ ] Efficient resource utilization

### **Quality Success**
- [ ] Comprehensive test coverage (>95%)
- [ ] Robust error handling and user experience
- [ ] Compatible with real Cassandra data
- [ ] Documentation and examples complete

---

**Labels**: `high-priority`, `core`, `query-engine`, `cql`, `phase-1`  
**Milestone**: Core Functionality  
**Dependencies**: SSTable reading (#25), Info command (#26)  
**Enables**: REPL functionality (#24), Advanced CLI features