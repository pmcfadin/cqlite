use cqlite_core::{
    parser::types::CqlTypeId,
    types::{TableId, Value},
};

#[tokio::test]
async fn test_imports_compile() -> Result<(), Box<dyn std::error::Error>> {
    // This test just validates that all the imports we need are available
    println!("Testing that imports compile correctly");

    // Test that we can reference the types
    let _type_id = CqlTypeId::Boolean;
    let _table_id = TableId::new("test");
    let _value = Value::Null;

    println!("✅ All imports are available");
    Ok(())
}
