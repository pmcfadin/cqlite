//! Performance benchmarks for schema-driven parsing
//!
//! These benchmarks verify that schema-driven parsing does not introduce
//! performance regressions compared to the previous type-guessing approach.

use cqlite_core::{
    schema::{parser::SchemaParser, registry::ParsingContext, Column, KeyColumn, TableSchema},
    types::{ComparatorType, Value},
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;

struct BenchmarkContext {
    simple_parser: SchemaParser,
    collections_parser: SchemaParser,
    complex_parser: SchemaParser,
}

impl BenchmarkContext {
    fn new() -> Self {
        Self {
            simple_parser: create_simple_parser(),
            collections_parser: create_collections_parser(),
            complex_parser: create_complex_parser(),
        }
    }
}

fn create_simple_parser() -> SchemaParser {
    let schema = TableSchema {
        keyspace: "bench_ks".to_string(),
        table: "simple_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![KeyColumn {
            name: "timestamp".to_string(),
            data_type: "bigint".to_string(),
            position: 0,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: "bigint".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "value".to_string(),
                data_type: "double".to_string(),
                nullable: true,
                default: None,
            },
        ],
        comments: HashMap::new(),
    };

    let mut column_comparators = HashMap::new();
    column_comparators.insert("id".to_string(), ComparatorType::Int);
    column_comparators.insert("timestamp".to_string(), ComparatorType::BigInt);
    column_comparators.insert("name".to_string(), ComparatorType::Text);
    column_comparators.insert("value".to_string(), ComparatorType::Float);

    let context = ParsingContext {
        schema,
        partition_comparators: vec![ComparatorType::Int],
        clustering_comparators: vec![ComparatorType::BigInt],
        column_comparators,
    };

    SchemaParser::new(context).unwrap()
}

fn create_collections_parser() -> SchemaParser {
    let schema = TableSchema {
        keyspace: "bench_ks".to_string(),
        table: "collections_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "metrics".to_string(),
                data_type: "map<text,double>".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "samples".to_string(),
                data_type: "list<bigint>".to_string(),
                nullable: true,
                default: None,
            },
        ],
        comments: HashMap::new(),
    };

    let mut column_comparators = HashMap::new();
    column_comparators.insert("id".to_string(), ComparatorType::Uuid);
    column_comparators.insert(
        "tags".to_string(),
        ComparatorType::Set(Box::new(ComparatorType::Text)),
    );
    column_comparators.insert(
        "metrics".to_string(),
        ComparatorType::Map(
            Box::new(ComparatorType::Text),
            Box::new(ComparatorType::Float),
        ),
    );
    column_comparators.insert(
        "samples".to_string(),
        ComparatorType::List(Box::new(ComparatorType::BigInt)),
    );

    let context = ParsingContext {
        schema,
        partition_comparators: vec![ComparatorType::Uuid],
        clustering_comparators: vec![],
        column_comparators,
    };

    SchemaParser::new(context).unwrap()
}

fn create_complex_parser() -> SchemaParser {
    let schema = TableSchema {
        keyspace: "bench_ks".to_string(),
        table: "complex_table".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "tenant".to_string(),
                data_type: "text".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "shard".to_string(),
                data_type: "int".to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![
            KeyColumn {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "sequence".to_string(),
                data_type: "bigint".to_string(),
                position: 1,
            },
        ],
        columns: vec![
            Column {
                name: "tenant".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "shard".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "sequence".to_string(),
                data_type: "bigint".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "nested".to_string(),
                data_type: "map<text,list<int>>".to_string(),
                nullable: true,
                default: None,
            },
        ],
        comments: HashMap::new(),
    };

    let mut column_comparators = HashMap::new();
    column_comparators.insert("tenant".to_string(), ComparatorType::Text);
    column_comparators.insert("shard".to_string(), ComparatorType::Int);
    column_comparators.insert("timestamp".to_string(), ComparatorType::Timestamp);
    column_comparators.insert("sequence".to_string(), ComparatorType::BigInt);
    column_comparators.insert(
        "nested".to_string(),
        ComparatorType::Map(
            Box::new(ComparatorType::Text),
            Box::new(ComparatorType::List(Box::new(ComparatorType::Int))),
        ),
    );

    let context = ParsingContext {
        schema,
        partition_comparators: vec![ComparatorType::Text, ComparatorType::Int],
        clustering_comparators: vec![ComparatorType::Timestamp, ComparatorType::BigInt],
        column_comparators,
    };

    SchemaParser::new(context).unwrap()
}

// Test data generators

fn generate_simple_partition_key() -> Vec<u8> {
    42i32.to_be_bytes().to_vec()
}

fn generate_simple_clustering_key() -> Vec<u8> {
    1640995200000i64.to_be_bytes().to_vec()
}

fn generate_text_column(text: &str) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&(text.len() as i32).to_be_bytes());
    data.extend_from_slice(text.as_bytes());
    data
}

fn generate_uuid_partition_key() -> Vec<u8> {
    uuid::Uuid::new_v4().as_bytes().to_vec()
}

fn generate_set_column() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&3i32.to_be_bytes()); // 3 elements

    for item in &["tag1", "tag2", "tag3"] {
        data.extend_from_slice(&(item.len() as i32).to_be_bytes());
        data.extend_from_slice(item.as_bytes());
    }

    data
}

fn generate_list_column() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&3i32.to_be_bytes()); // 3 elements
    data.extend_from_slice(&100i64.to_be_bytes());
    data.extend_from_slice(&200i64.to_be_bytes());
    data.extend_from_slice(&300i64.to_be_bytes());
    data
}

fn generate_complex_partition_key() -> Vec<u8> {
    let mut data = Vec::new();
    // tenant: "production"
    data.extend_from_slice(&10i32.to_be_bytes());
    data.extend_from_slice(b"production");
    // shard: 42
    data.extend_from_slice(&42i32.to_be_bytes());
    data
}

fn generate_complex_clustering_key() -> Vec<u8> {
    let mut data = Vec::new();
    // timestamp
    data.extend_from_slice(&1640995200000i64.to_be_bytes());
    // sequence
    data.extend_from_slice(&123456789i64.to_be_bytes());
    data
}

// Benchmark functions

fn bench_simple_parsing(c: &mut Criterion) {
    let ctx = BenchmarkContext::new();

    let partition_data = generate_simple_partition_key();
    let clustering_data = generate_simple_clustering_key();
    let text_data = generate_text_column("Hello, World!");

    let mut group = c.benchmark_group("simple_parsing");

    group.bench_function("partition_key", |b| {
        b.iter(|| {
            ctx.simple_parser
                .parse_partition_key(black_box(&partition_data))
        })
    });

    group.bench_function("clustering_key", |b| {
        b.iter(|| {
            ctx.simple_parser
                .parse_clustering_keys(black_box(&clustering_data))
        })
    });

    group.bench_function("text_column", |b| {
        b.iter(|| {
            ctx.simple_parser
                .parse_column_value(black_box("name"), black_box(&text_data))
        })
    });

    group.finish();
}

fn bench_collections_parsing(c: &mut Criterion) {
    let ctx = BenchmarkContext::new();

    let uuid_data = generate_uuid_partition_key();
    let set_data = generate_set_column();
    let list_data = generate_list_column();

    let mut group = c.benchmark_group("collections_parsing");

    group.bench_function("uuid_partition", |b| {
        b.iter(|| {
            ctx.collections_parser
                .parse_partition_key(black_box(&uuid_data))
        })
    });

    group.bench_function("set_column", |b| {
        b.iter(|| {
            ctx.collections_parser
                .parse_column_value(black_box("tags"), black_box(&set_data))
        })
    });

    group.bench_function("list_column", |b| {
        b.iter(|| {
            ctx.collections_parser
                .parse_column_value(black_box("samples"), black_box(&list_data))
        })
    });

    group.finish();
}

fn bench_complex_parsing(c: &mut Criterion) {
    let ctx = BenchmarkContext::new();

    let partition_data = generate_complex_partition_key();
    let clustering_data = generate_complex_clustering_key();

    let mut group = c.benchmark_group("complex_parsing");

    group.bench_function("multi_component_partition", |b| {
        b.iter(|| {
            ctx.complex_parser
                .parse_partition_key(black_box(&partition_data))
        })
    });

    group.bench_function("multi_component_clustering", |b| {
        b.iter(|| {
            ctx.complex_parser
                .parse_clustering_keys(black_box(&clustering_data))
        })
    });

    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let ctx = BenchmarkContext::new();

    let partition_data = generate_simple_partition_key();
    let text_data = generate_text_column("Benchmark text data");

    let mut group = c.benchmark_group("throughput");
    group.throughput(criterion::Throughput::Elements(1000));

    group.bench_function("parse_1000_partition_keys", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let _ = ctx
                    .simple_parser
                    .parse_partition_key(black_box(&partition_data));
            }
        })
    });

    group.bench_function("parse_1000_text_columns", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let _ = ctx
                    .simple_parser
                    .parse_column_value(black_box("name"), black_box(&text_data));
            }
        })
    });

    group.finish();
}

fn bench_schema_lookup(c: &mut Criterion) {
    let ctx = BenchmarkContext::new();

    let mut group = c.benchmark_group("schema_lookup");

    group.bench_function("comparator_lookup", |b| {
        b.iter(|| {
            let _comparator = ctx
                .simple_parser
                .context()
                .get_column_comparator(black_box("name"));
        })
    });

    group.bench_function("context_access", |b| {
        b.iter(|| {
            let _context = ctx.simple_parser.context();
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_parsing,
    bench_collections_parsing,
    bench_complex_parsing,
    bench_throughput,
    bench_schema_lookup
);
criterion_main!(benches);
