# CQLite REPL Architecture Design - Issue #10

## Executive Summary

This document presents a comprehensive architecture design for CQLite's interactive REPL system, transforming the basic interactive shell stub into a production-ready, feature-rich command-line interface for Cassandra SSTable exploration and CQL query execution.

## 🔍 Research Findings

### Current State Analysis

**Existing Foundation:**
- Basic REPL stub in `cqlite-cli/src/interactive.rs` (2,171 lines)
- Well-structured CLI framework with clap
- Comprehensive command system for SSTable operations
- Real data parsing with `RealDataParser` and `QueryExecutor`
- Enhanced table formatting with `CqlshTableFormatter`
- Bulletproof SSTable reader supporting all Cassandra versions

**Identified Strengths:**
- Robust SSTable parsing infrastructure
- Comprehensive help system framework
- Good error handling patterns
- Real-time data exploration capabilities
- Configurable output formats (Table, JSON, CSV, YAML)

**Identified Gaps:**
- Limited REPL functionality (basic command handling)
- No advanced line editing or history management
- Missing auto-completion and syntax highlighting
- No multi-line query support
- Basic command parsing without CQL syntax awareness

### External REPL Framework Analysis

**Rust REPL Ecosystem (2024):**

1. **Rustyline** - Industry standard for Rust REPLs
   - Readline-like functionality with command history
   - Built-in line editing capabilities
   - Tab completion support
   - Cross-platform compatibility

2. **Crossterm** - Terminal manipulation library
   - Cross-platform terminal control
   - Event handling and key capture
   - Screen manipulation and cursor control
   - Raw mode terminal access

3. **r3bl_terminal_async** - Async interactive CLI framework
   - Non-blocking interactive applications
   - Concurrent task execution
   - Animated spinners and progress indicators
   - Modern async/await patterns

4. **Console** - High-level terminal utilities
   - Styled output and color support
   - Progress bars and user interaction
   - Terminal feature detection

## 🏗️ REPL Architecture Design

### Core Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                    CQLite REPL System                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐    ┌──────────────────────────────┐   │
│  │   REPL Engine   │    │     Command Processor       │   │
│  │                 │    │                              │   │
│  │ • Input Loop    │◄──►│ • CQL Parser                 │   │
│  │ • Line Editor   │    │ • Meta-Command Router        │   │
│  │ • History Mgmt  │    │ • Query Validator            │   │
│  │ • Auto-Complete │    │ • Syntax Highlighter         │   │
│  └─────────────────┘    └──────────────────────────────┘   │
│           │                           │                     │
│           ▼                           ▼                     │
│  ┌─────────────────┐    ┌──────────────────────────────┐   │
│  │ Session Manager │    │     Execution Engine        │   │
│  │                 │    │                              │   │
│  │ • Configuration │    │ • Query Executor            │   │
│  │ • State Mgmt    │    │ • SSTable Reader             │   │
│  │ • Data Sources  │    │ • Result Formatter           │   │
│  │ • User Prefs    │    │ • Error Handler              │   │
│  └─────────────────┘    └──────────────────────────────┘   │
│           │                           │                     │
│           ▼                           ▼                     │
│  ┌─────────────────┐    ┌──────────────────────────────┐   │
│  │  Data Sources   │    │      Output System           │   │
│  │                 │    │                              │   │
│  │ • Database Conn │    │ • Table Formatter            │   │
│  │ • SSTable Files │    │ • Pagination Engine          │   │
│  │ • Schema Cache  │    │ • Export Functions           │   │
│  │ • Index Cache   │    │ • Progress Indicators        │   │
│  └─────────────────┘    └──────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 1. Enhanced REPL Engine

#### Core Features:
- **Advanced Line Editing**: Implement rustyline for professional editing experience
- **Smart History**: Context-aware command history with search and filtering
- **Multi-line Support**: Handle complex CQL queries spanning multiple lines
- **Auto-completion**: Dynamic completion for tables, columns, keywords, and commands
- **Syntax Highlighting**: Real-time CQL syntax highlighting during input

#### Implementation:
```rust
pub struct ReplEngine {
    editor: Editor<ReplHelper>,
    session: Arc<RwLock<ReplSession>>,
    command_processor: CommandProcessor,
    completion_provider: CompletionProvider,
    syntax_highlighter: SyntaxHighlighter,
}

impl ReplEngine {
    pub async fn start(&mut self) -> Result<()> {
        loop {
            let prompt = self.session.read().await.get_prompt();
            
            match self.editor.readline(&prompt) {
                Ok(line) => {
                    self.editor.add_history_entry(line.as_str());
                    if let Err(e) = self.process_line(&line).await {
                        self.handle_error(e);
                    }
                }
                Err(ReadlineError::Interrupted) => continue,
                Err(ReadlineError::Eof) => break,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }
}
```

### 2. Advanced Command Processing System

#### Command Categories:

**Meta-Commands (Enhanced):**
```
Schema Exploration:
  :tables [pattern]          - List tables with optional pattern matching
  :describe <table>          - Show detailed table schema with indexes
  :info <object>             - Comprehensive object information
  :keyspaces                 - List all keyspaces with statistics
  :indexes [table]           - Show indexes for table or all tables

Data Exploration:
  :sample <table> [n]        - Show n sample rows from table
  :count <table>             - Get row count with performance stats
  :scan <table> [where]      - Streaming table scan with filters
  :profile <query>           - Profile query performance
  :explain <query>           - Show query execution plan

Session Management:
  :config                    - Show/modify session configuration
  :history [n]              - Show command history (last n)
  :save <file>               - Save session history
  :load <file>               - Load and execute commands from file
  :export <format> <file>    - Export last result set

Data Sources:
  :use <keyspace>            - Switch keyspace context
  :source <path>             - Add SSTable data source
  :connect <uri>             - Connect to live Cassandra (future)
  :refresh                   - Refresh schema cache

Display Control:
  :format <type>             - Set output format (table|json|csv|yaml)
  :limit <n>                 - Set default row limit
  :timing on|off             - Toggle timing display
  :paging on|off [size]      - Control result paging
  :width <cols>              - Set display width
```

**CQL Commands (Enhanced):**
```
Basic Queries:
  SELECT * FROM table WHERE condition LIMIT n;
  SELECT column1, column2 FROM table;
  DESCRIBE TABLE table_name;
  DESCRIBE KEYSPACE keyspace_name;

Advanced Features:
  SELECT * FROM table ALLOW FILTERING;
  SELECT COUNT(*) FROM table;
  SELECT * FROM table WHERE token(pk) > token('value');
  
System Queries:
  SELECT * FROM system.tables;
  SELECT * FROM system.keyspaces;
  SELECT * FROM system.columns WHERE table_name = 'users';
```

#### Command Processing Pipeline:
```rust
pub struct CommandProcessor {
    cql_parser: CqlParser,
    meta_parser: MetaCommandParser,
    query_planner: QueryPlanner,
    execution_engine: ExecutionEngine,
}

impl CommandProcessor {
    pub async fn process(&self, input: &str) -> Result<CommandResult> {
        let input = input.trim();
        
        // Multi-line query assembly
        if self.is_incomplete_query(input) {
            return Ok(CommandResult::PartialInput);
        }
        
        // Route to appropriate processor
        match input.chars().next() {
            Some(':') => self.process_meta_command(input).await,
            _ => self.process_cql_command(input).await,
        }
    }
    
    async fn process_cql_command(&self, query: &str) -> Result<CommandResult> {
        // 1. Parse CQL syntax
        let parsed = self.cql_parser.parse(query)?;
        
        // 2. Create execution plan
        let plan = self.query_planner.plan(&parsed)?;
        
        // 3. Execute against data sources
        let result = self.execution_engine.execute(plan).await?;
        
        Ok(CommandResult::QueryResult(result))
    }
}
```

### 3. Intelligent Auto-completion System

#### Completion Provider Architecture:
```rust
pub struct CompletionProvider {
    schema_cache: Arc<RwLock<SchemaCache>>,
    keyword_provider: KeywordProvider,
    table_provider: TableProvider,
    column_provider: ColumnProvider,
    function_provider: FunctionProvider,
}

impl CompletionProvider {
    pub fn complete(&self, context: &CompletionContext) -> Vec<CompletionCandidate> {
        let mut candidates = Vec::new();
        
        match context.position {
            CompletionPosition::Command => {
                candidates.extend(self.meta_commands());
                candidates.extend(self.cql_keywords());
            }
            CompletionPosition::TableName => {
                candidates.extend(self.table_provider.get_tables());
            }
            CompletionPosition::ColumnName { table } => {
                candidates.extend(self.column_provider.get_columns(table));
            }
            CompletionPosition::Function => {
                candidates.extend(self.function_provider.get_functions());
            }
            CompletionPosition::Value { column_type } => {
                candidates.extend(self.suggest_values(column_type));
            }
        }
        
        candidates
    }
}
```

#### Completion Features:
- **Context-aware**: Understands current query position
- **Smart filtering**: Filters suggestions based on partial input
- **Type-aware**: Suggests appropriate values for column types
- **Performance optimized**: Caches schema information
- **Fuzzy matching**: Handles typos and partial matches

### 4. Enhanced Session Management

#### Session State:
```rust
pub struct ReplSession {
    // Database connection
    pub database: Option<Arc<Database>>,
    pub current_keyspace: Option<String>,
    pub data_sources: Vec<DataSource>,
    
    // Display configuration
    pub output_format: OutputFormat,
    pub page_size: usize,
    pub timing_enabled: bool,
    pub paging_enabled: bool,
    pub display_width: usize,
    
    // Query state
    pub last_result: Option<QueryResult>,
    pub query_history: VecDeque<String>,
    pub active_transaction: Option<Transaction>,
    
    // Schema cache
    pub schema_cache: SchemaCache,
    pub table_cache: HashMap<String, TableMetadata>,
    
    // Performance tracking
    pub performance_stats: PerformanceStats,
    pub query_metrics: Vec<QueryMetric>,
}
```

#### Configuration System:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplConfig {
    // Display preferences
    pub default_format: OutputFormat,
    pub default_page_size: usize,
    pub max_display_width: usize,
    pub prompt_format: String,
    
    // Behavior settings
    pub auto_completion: bool,
    pub syntax_highlighting: bool,
    pub timing_threshold_ms: u64,
    pub history_size: usize,
    
    // Data source preferences
    pub default_keyspace: Option<String>,
    pub data_directories: Vec<PathBuf>,
    pub connection_timeout_ms: u64,
    
    // Advanced features
    pub enable_streaming: bool,
    pub cache_size_mb: usize,
    pub parallel_query_limit: usize,
}
```

### 5. Advanced Query Execution Engine

#### Multi-source Query Engine:
```rust
pub enum DataSource {
    SSTableDirectory {
        path: PathBuf,
        keyspace: String,
        tables: HashMap<String, TableMetadata>,
    },
    Database {
        connection: Arc<Database>,
        metadata: DatabaseMetadata,
    },
    RemoteCassandra {
        connection: Arc<CassandraConnection>,
        session: Session,
    },
}

pub struct ExecutionEngine {
    data_sources: Vec<DataSource>,
    query_cache: LruCache<String, QueryResult>,
    performance_monitor: PerformanceMonitor,
    streaming_processor: StreamingProcessor,
}

impl ExecutionEngine {
    pub async fn execute(&self, plan: ExecutionPlan) -> Result<QueryResult> {
        match plan.query_type {
            QueryType::Select => self.execute_select(plan).await,
            QueryType::Describe => self.execute_describe(plan).await,
            QueryType::Count => self.execute_count(plan).await,
            QueryType::System => self.execute_system_query(plan).await,
        }
    }
    
    async fn execute_select(&self, plan: ExecutionPlan) -> Result<QueryResult> {
        let start_time = Instant::now();
        
        // Choose optimal data source
        let data_source = self.select_data_source(&plan.target_table)?;
        
        // Execute with streaming if large result expected
        let result = if plan.estimated_rows > self.streaming_threshold {
            self.execute_streaming(data_source, plan).await?
        } else {
            self.execute_direct(data_source, plan).await?
        };
        
        // Update performance metrics
        self.performance_monitor.record_query(
            &plan.original_query,
            start_time.elapsed(),
            result.row_count,
        );
        
        Ok(result)
    }
}
```

### 6. Enhanced Output and Formatting System

#### Advanced Table Formatter:
```rust
pub struct AdvancedTableFormatter {
    style: TableStyle,
    width_calculator: ColumnWidthCalculator,
    data_formatter: DataFormatter,
    pagination: PaginationConfig,
}

impl AdvancedTableFormatter {
    pub fn format_result(&self, result: &QueryResult, config: &DisplayConfig) -> FormattedOutput {
        let columns = self.calculate_column_layout(&result.columns, config.terminal_width);
        let styled_rows = self.format_rows(&result.rows, &columns);
        
        FormattedOutput {
            header: self.format_header(&columns),
            rows: styled_rows,
            footer: self.format_footer(result),
            pagination_info: self.calculate_pagination(result, config),
        }
    }
}
```

#### Real-time Streaming Display:
```rust
pub struct StreamingDisplay {
    formatter: AdvancedTableFormatter,
    progress_bar: ProgressBar,
    update_interval: Duration,
}

impl StreamingDisplay {
    pub async fn display_stream<T>(&self, stream: T) -> Result<()>
    where
        T: Stream<Item = QueryRow>,
    {
        let mut row_count = 0;
        let mut batch = Vec::new();
        
        tokio::pin!(stream);
        
        while let Some(row) = stream.next().await {
            batch.push(row);
            row_count += 1;
            
            // Display batch when full or timeout reached
            if batch.len() >= self.batch_size || self.should_flush() {
                self.display_batch(&batch)?;
                batch.clear();
            }
            
            self.progress_bar.set_position(row_count);
        }
        
        // Display remaining rows
        if !batch.is_empty() {
            self.display_batch(&batch)?;
        }
        
        self.progress_bar.finish_with_message(format!("Displayed {} rows", row_count));
        Ok(())
    }
}
```

### 7. Performance Optimization Features

#### Query Performance Profiler:
```rust
pub struct QueryProfiler {
    metrics_collector: MetricsCollector,
    execution_tracer: ExecutionTracer,
    bottleneck_analyzer: BottleneckAnalyzer,
}

impl QueryProfiler {
    pub async fn profile_query(&self, query: &str) -> Result<ProfileReport> {
        let trace = self.execution_tracer.trace_execution(query).await?;
        let metrics = self.metrics_collector.collect_metrics(&trace);
        let bottlenecks = self.bottleneck_analyzer.analyze(&metrics);
        
        Ok(ProfileReport {
            query: query.to_string(),
            execution_time: trace.total_time,
            stages: trace.stages,
            memory_usage: metrics.peak_memory,
            io_operations: metrics.io_ops,
            cache_efficiency: metrics.cache_hit_ratio,
            bottlenecks,
            optimization_suggestions: self.generate_suggestions(&bottlenecks),
        })
    }
}
```

#### Smart Caching System:
```rust
pub struct SmartCache {
    schema_cache: Arc<RwLock<SchemaCache>>,
    query_cache: LruCache<String, CachedResult>,
    table_stats_cache: LruCache<String, TableStats>,
    
    // Cache policies
    schema_ttl: Duration,
    query_cache_size: usize,
    stats_refresh_interval: Duration,
}

impl SmartCache {
    pub async fn get_or_compute<T, F>(&self, key: &str, compute: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
        T: Clone + Send + Sync + 'static,
    {
        // Check cache first
        if let Some(cached) = self.get_cached(key) {
            return Ok(cached);
        }
        
        // Compute and cache result
        let result = compute.await?;
        self.cache_result(key, result.clone());
        
        Ok(result)
    }
}
```

## 🔧 Implementation Strategy

### Phase 1: Core REPL Infrastructure (Week 1-2)
1. **Replace basic input loop with rustyline**
   - Implement `ReplEngine` with advanced line editing
   - Add command history persistence
   - Basic auto-completion for commands

2. **Enhanced command processing**
   - Implement `CommandProcessor` with proper routing
   - Add multi-line query support
   - Improve meta-command parsing

3. **Session management upgrade**
   - Implement persistent configuration
   - Add session state management
   - Data source configuration

### Phase 2: Advanced Features (Week 3-4)
1. **Smart auto-completion system**
   - Context-aware completion for CQL
   - Dynamic schema-based suggestions
   - Fuzzy matching and error tolerance

2. **Syntax highlighting**
   - Real-time CQL syntax highlighting
   - Error highlighting and suggestions
   - Theme support

3. **Enhanced output formatting**
   - Advanced table layouts with proper column sizing
   - Streaming display for large results
   - Multiple output formats with improved styling

### Phase 3: Performance and Polish (Week 5-6)
1. **Query profiling and optimization**
   - Execution plan analysis
   - Performance bottleneck detection
   - Optimization suggestions

2. **Advanced data exploration**
   - Table sampling and statistics
   - Index analysis and recommendations
   - Query optimization hints

3. **Integration testing and documentation**
   - Comprehensive test suite
   - User guide and examples
   - Performance benchmarking

## 🎯 User Experience Design

### Startup Experience:
```
╔═══════════════════════════════════════════════╗
║            CQLite Interactive Shell           ║
║      High-Performance Cassandra Reader       ║
╚═══════════════════════════════════════════════╝

🗄️  Database: /path/to/cqlite.db
📊 Engine: CQLite Core v0.1.0
🔗 Cassandra Compatibility: 3.11 | 4.0 | 5.0
📂 Data Sources: 3 keyspaces, 27 tables

Quick Start:
  • :help                   - Comprehensive help system
  • :tables                 - List available tables
  • :config data-dir <path> - Add SSTable directory
  • SELECT * FROM users LIMIT 5; - Sample query

Pro Tips:
  • Use Tab for auto-completion
  • Use ↑/↓ for command history  
  • Use :timing to see query performance
  • Use :profile <query> for optimization hints

cqlite> 
```

### Auto-completion in Action:
```
cqlite> SELECT user_id, em[TAB]
         email     enabled   email_verified

cqlite> SELECT user_id, email FROM [TAB]
         users     user_sessions     user_preferences

cqlite> SELECT user_id, email FROM users WHERE [TAB]
         user_id=         email LIKE      created_at >
         status=          last_login <    
```

### Query Results with Performance:
```
cqlite> SELECT * FROM users WHERE status = 'active' LIMIT 3;
🔍 Executing: SELECT * FROM users WHERE status = 'active' LIMIT 3;

┌────────────┬─────────────────────┬─────────┬──────────────────────┐
│ user_id    │ email               │ status  │ created_at           │
├────────────┼─────────────────────┼─────────┼──────────────────────┤
│ user123    │ alice@example.com   │ active  │ 2024-01-15T10:30:00Z │
│ user456    │ bob@example.com     │ active  │ 2024-01-16T14:22:00Z │
│ user789    │ carol@example.com   │ active  │ 2024-01-17T09:15:00Z │
└────────────┴─────────────────────┴─────────┴──────────────────────┘

✅ 3 rows returned in 12.5ms
📊 Performance: Parse: 0.8ms | Plan: 1.2ms | Execute: 10.5ms
🎯 Cache hit ratio: 89.3% | Memory used: 2.4KB
💡 Optimization: Index on 'status' column would improve performance
```

### Error Handling with Suggestions:
```
cqlite> SELECT * FROM user WHERE email = 'test@example.com';
❌ Table 'user' not found

💡 Did you mean?
  • users (exact match)
  • user_sessions (similar name)
  • user_preferences (similar name)

💡 Use :tables to list all available tables
```

## 🧪 Testing Strategy

### Unit Tests:
- Command parsing and routing
- Auto-completion logic
- Query execution engine
- Data formatting and display
- Configuration management

### Integration Tests:
- End-to-end query execution
- Multi-source data access
- Performance under load
- Error handling scenarios
- Session persistence

### User Acceptance Tests:
- Real SSTable file compatibility
- Interactive workflow testing
- Performance benchmarking
- Documentation validation

## 📊 Success Metrics

### Performance Targets:
- **Startup time**: < 500ms for small datasets
- **Query response**: < 50ms for simple queries
- **Memory usage**: < 100MB for typical sessions
- **Tab completion**: < 100ms response time

### User Experience Targets:
- **Learning curve**: New users productive in < 15 minutes
- **Error recovery**: Clear guidance for 90% of common errors
- **Feature discovery**: Core features discoverable through help system
- **Workflow efficiency**: 50% faster than manual file inspection

## 🎯 Future Enhancements

### Advanced Features:
- **Live Cassandra connection** support
- **Visual query plans** with ASCII diagrams
- **Collaborative sessions** with shared workspaces
- **Plugin system** for custom commands
- **Integration** with popular data tools

### Performance Optimizations:
- **Parallel query execution** across multiple SSTables
- **Smart prefetching** based on user patterns
- **Advanced caching** with machine learning
- **Query optimization** recommendations

## 📝 Implementation Checklist

### Core REPL Engine:
- [ ] Replace basic input with rustyline integration
- [ ] Implement command history with persistence
- [ ] Add multi-line query support
- [ ] Create configuration system
- [ ] Build session state management

### Command Processing:
- [ ] Enhance meta-command parser
- [ ] Implement CQL syntax recognition
- [ ] Add query validation
- [ ] Create execution planning
- [ ] Build error handling system

### Auto-completion:
- [ ] Implement completion provider framework
- [ ] Add schema-aware suggestions
- [ ] Create fuzzy matching
- [ ] Build context detection
- [ ] Add performance optimization

### Output System:
- [ ] Enhance table formatting
- [ ] Add streaming display
- [ ] Implement pagination
- [ ] Create export functions
- [ ] Build progress indicators

### Integration:
- [ ] Connect with existing SSTable readers
- [ ] Integrate with query executor
- [ ] Add configuration persistence
- [ ] Create comprehensive testing
- [ ] Write user documentation

---

This architecture design provides a roadmap for transforming CQLite's basic REPL into a sophisticated, production-ready interactive database exploration tool that rivals commercial alternatives while maintaining the performance and simplicity that makes CQLite unique.