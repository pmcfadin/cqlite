//! Integration tests for schema-driven parsing with sstabledump parity

use cqlite_core::{
    platform::Platform,
    schema::{
        ClusteringColumn, ClusteringOrder, SchemaParser, SchemaRegistry, SchemaRegistryConfig,
        TableSchema,
    },
    types::ComparatorType,
    Config,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Test fixture for schema-driven parsing validation
#[allow(dead_code)]
struct SchemaParityTestFixture {
    registry: Arc<SchemaRegistry>,
    platform: Arc<Platform>,
}

impl SchemaParityTestFixture {
    async fn new() -> Self {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(
            SchemaRegistry::new(registry_config, platform.clone(), config)
                .await
                .unwrap(),
        );

        Self { registry, platform }
    }

    async fn create_test_schemas(&self) {
        // Create schema for simple table
        let simple_schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "simple_table".to_string(),
            partition_keys: vec![cqlite_core::schema::KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                cqlite_core::schema::Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "value".to_string(),
                    data_type: "double".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        };

        self.registry
            .register_schema(simple_schema, cqlite_core::schema::SchemaSource::Manual)
            .await
            .unwrap();

        // Create schema for collections table
        let collections_schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "collections_table".to_string(),
            partition_keys: vec![cqlite_core::schema::KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                cqlite_core::schema::Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "tags".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "metadata".to_string(),
                    data_type: "map<text,int>".to_string(),
                    nullable: true,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "values".to_string(),
                    data_type: "list<double>".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        };

        self.registry
            .register_schema(
                collections_schema,
                cqlite_core::schema::SchemaSource::Manual,
            )
            .await
            .unwrap();

        // Create schema for UDT/frozen table
        let udt_schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "udt_table".to_string(),
            partition_keys: vec![
                cqlite_core::schema::KeyColumn {
                    name: "region".to_string(),
                    data_type: "text".to_string(),
                    position: 0,
                },
                cqlite_core::schema::KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![
                ClusteringColumn {
                    name: "date".to_string(),
                    data_type: "date".to_string(),
                    position: 0,
                    order: ClusteringOrder::Asc,
                },
                ClusteringColumn {
                    name: "sequence".to_string(),
                    data_type: "bigint".to_string(),
                    position: 1,
                    order: ClusteringOrder::Asc,
                },
            ],
            columns: vec![
                cqlite_core::schema::Column {
                    name: "region".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "date".to_string(),
                    data_type: "date".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "sequence".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "address".to_string(),
                    data_type: "frozen<address_type>".to_string(),
                    nullable: true,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "coordinates".to_string(),
                    data_type: "tuple<double,double>".to_string(),
                    nullable: true,
                    default: None,
                },
                cqlite_core::schema::Column {
                    name: "nested".to_string(),
                    data_type: "frozen<map<text,frozen<list<int>>>>".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        };

        self.registry
            .register_schema(udt_schema, cqlite_core::schema::SchemaSource::Manual)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_simple_table_schema_parsing() {
    let fixture = SchemaParityTestFixture::new().await;
    fixture.create_test_schemas().await;

    let context = fixture
        .registry
        .get_parsing_context("test_ks", "simple_table")
        .await
        .unwrap();

    let parser = SchemaParser::new(context).unwrap();

    // Test parsing an integer partition key
    let key_data = vec![0, 0, 0, 42];
    let result = parser.parse_partition_key(&key_data);
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 1);
}

#[tokio::test]
async fn test_collections_table_schema_parsing() {
    let fixture = SchemaParityTestFixture::new().await;
    fixture.create_test_schemas().await;

    let context = fixture
        .registry
        .get_parsing_context("test_ks", "collections_table")
        .await
        .unwrap();

    let parser = SchemaParser::new(context).unwrap();

    // Test parsing UUID partition key and timestamp clustering key
    let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let result = parser.parse_partition_key(&uuid_bytes);
    assert!(result.is_ok());

    let timestamp_bytes = vec![0, 0, 1, 126, 45, 67, 89, 0]; // Sample timestamp
    let result = parser.parse_clustering_keys(&timestamp_bytes);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_multi_component_keys() {
    let fixture = SchemaParityTestFixture::new().await;
    fixture.create_test_schemas().await;

    let context = fixture
        .registry
        .get_parsing_context("test_ks", "udt_table")
        .await
        .unwrap();

    let parser = SchemaParser::new(context).unwrap();

    // Test multi-component partition key: region (text) + id (int)
    let mut key_data = vec![0, 0, 0, 2]; // length of "US"
    key_data.extend_from_slice(b"US");
    key_data.extend_from_slice(&[0, 0, 0, 100]); // id = 100

    let result = parser.parse_partition_key(&key_data);
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 2);
}

#[tokio::test]
async fn test_schema_validation() {
    let fixture = SchemaParityTestFixture::new().await;
    fixture.create_test_schemas().await;

    // Validate simple table schema
    let report = fixture
        .registry
        .validate_schema("test_ks", "simple_table")
        .await
        .unwrap();

    assert_eq!(
        report.status,
        cqlite_core::schema::SchemaValidationStatus::Valid
    );

    // Validate collections table schema
    let report = fixture
        .registry
        .validate_schema("test_ks", "collections_table")
        .await
        .unwrap();

    assert_eq!(
        report.status,
        cqlite_core::schema::SchemaValidationStatus::Valid
    );
}

#[tokio::test]
async fn test_comparator_consistency() {
    let fixture = SchemaParityTestFixture::new().await;
    fixture.create_test_schemas().await;

    // Get comparators for simple table
    let comparators = fixture
        .registry
        .get_table_comparators("test_ks", "simple_table")
        .await
        .unwrap();

    assert_eq!(comparators.get("id"), Some(&ComparatorType::Int));
    assert_eq!(comparators.get("name"), Some(&ComparatorType::Text));
    assert_eq!(comparators.get("value"), Some(&ComparatorType::Float));

    // Get comparators for collections table
    let comparators = fixture
        .registry
        .get_table_comparators("test_ks", "collections_table")
        .await
        .unwrap();

    assert_eq!(comparators.get("id"), Some(&ComparatorType::Uuid));
    assert_eq!(
        comparators.get("timestamp"),
        Some(&ComparatorType::Timestamp)
    );
    assert!(matches!(
        comparators.get("tags"),
        Some(ComparatorType::Set(_))
    ));
    assert!(matches!(
        comparators.get("metadata"),
        Some(ComparatorType::Map(_, _))
    ));
    assert!(matches!(
        comparators.get("values"),
        Some(ComparatorType::List(_))
    ));
}

#[tokio::test]
async fn test_error_on_missing_schema() {
    let fixture = SchemaParityTestFixture::new().await;

    let result = fixture
        .registry
        .get_parsing_context("nonexistent_ks", "nonexistent_table")
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Schema not found"));
}

#[tokio::test]
async fn test_byte_comparable_ordering() {
    let fixture = SchemaParityTestFixture::new().await;
    fixture.create_test_schemas().await;

    let context = fixture
        .registry
        .get_parsing_context("test_ks", "simple_table")
        .await
        .unwrap();

    // Verify partition key comparator supports ordering
    assert!(context.partition_comparators[0].supports_ordering());

    // Test ordering comparison
    let comparator = &context.partition_comparators[0];
    let val1 = cqlite_core::types::Value::Integer(10);
    let val2 = cqlite_core::types::Value::Integer(20);

    assert!(comparator.less_than(&val1, &val2).unwrap());
    assert!(comparator.greater_than(&val2, &val1).unwrap());
    assert!(comparator.equals(&val1, &val1).unwrap());
}
