//! Basic tests for ComparatorType system

use super::comparator::ComparatorType;
use crate::schema::CqlType;
use crate::types::Value;
use std::cmp::Ordering;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_int_comparison() {
        let comparator = ComparatorType::Int;

        let val1 = Value::Integer(10);
        let val2 = Value::Integer(20);

        assert_eq!(comparator.compare(&val1, &val2).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&val2, &val1).unwrap(), Ordering::Greater);
        assert_eq!(comparator.compare(&val1, &val1).unwrap(), Ordering::Equal);

        assert!(comparator.equals(&val1, &val1).unwrap());
        assert!(!comparator.equals(&val1, &val2).unwrap());
        assert!(comparator.less_than(&val1, &val2).unwrap());
        assert!(comparator.supports_ordering());
    }

    #[test]
    fn test_text_comparison() {
        let comparator = ComparatorType::Text;

        let apple = Value::Text("apple".to_string());
        let banana = Value::Text("banana".to_string());

        assert_eq!(comparator.compare(&apple, &banana).unwrap(), Ordering::Less);
        assert_eq!(
            comparator.compare(&banana, &apple).unwrap(),
            Ordering::Greater
        );
        assert_eq!(comparator.compare(&apple, &apple).unwrap(), Ordering::Equal);

        assert!(comparator.supports_ordering());
    }

    #[test]
    fn test_list_comparison() {
        let element_comparator = ComparatorType::Int;
        let list_comparator = ComparatorType::List(Box::new(element_comparator));

        let list1 = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
        let list2 = Value::List(vec![Value::Integer(1), Value::Integer(3)]);
        let list3 = Value::List(vec![Value::Integer(1)]);

        assert_eq!(
            list_comparator.compare(&list1, &list2).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            list_comparator.compare(&list3, &list1).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            list_comparator.compare(&list1, &list1).unwrap(),
            Ordering::Equal
        );

        assert!(list_comparator.supports_ordering());
    }

    #[test]
    fn test_set_comparison() {
        let element_comparator = ComparatorType::Int;
        let set_comparator = ComparatorType::Set(Box::new(element_comparator));

        let set1 = Value::Set(vec![Value::Integer(1), Value::Integer(2)]);
        let set2 = Value::Set(vec![Value::Integer(1), Value::Integer(2)]);
        let set3 = Value::Set(vec![Value::Integer(1), Value::Integer(3)]);

        assert_eq!(
            set_comparator.compare(&set1, &set2).unwrap(),
            Ordering::Equal
        );
        assert_ne!(
            set_comparator.compare(&set1, &set3).unwrap(),
            Ordering::Equal
        );

        // Sets don't support ordering
        assert!(!set_comparator.supports_ordering());
    }

    #[test]
    fn test_null_handling() {
        let comparator = ComparatorType::Int;

        let null_val = Value::Null;
        let int_val = Value::Integer(42);

        // Nulls are always less than non-nulls
        assert_eq!(
            comparator.compare(&null_val, &null_val).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            comparator.compare(&null_val, &int_val).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            comparator.compare(&int_val, &null_val).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_from_cql_type() {
        // Test creating comparators from CQL types
        assert_eq!(
            ComparatorType::from_cql_type(&CqlType::Int).unwrap(),
            ComparatorType::Int
        );

        assert_eq!(
            ComparatorType::from_cql_type(&CqlType::Text).unwrap(),
            ComparatorType::Text
        );

        // Test collection type
        let list_type = CqlType::List(Box::new(CqlType::Int));
        let list_comparator = ComparatorType::from_cql_type(&list_type).unwrap();
        assert!(matches!(list_comparator, ComparatorType::List(_)));

        // Test frozen type
        let frozen_type = CqlType::Frozen(Box::new(CqlType::Text));
        let frozen_comparator = ComparatorType::from_cql_type(&frozen_type).unwrap();
        assert!(matches!(frozen_comparator, ComparatorType::Frozen(_)));
    }

    #[test]
    fn test_from_type_string() {
        assert_eq!(
            ComparatorType::from_type_string("int").unwrap(),
            ComparatorType::Int
        );

        assert_eq!(
            ComparatorType::from_type_string("text").unwrap(),
            ComparatorType::Text
        );

        let list_comparator = ComparatorType::from_type_string("list<int>").unwrap();
        assert!(matches!(list_comparator, ComparatorType::List(_)));
    }

    #[test]
    fn test_type_names() {
        assert_eq!(ComparatorType::Boolean.type_name(), "boolean");
        assert_eq!(ComparatorType::Int.type_name(), "int");
        assert_eq!(ComparatorType::Text.type_name(), "text");
        assert_eq!(
            ComparatorType::List(Box::new(ComparatorType::Int)).type_name(),
            "list"
        );
        assert_eq!(
            ComparatorType::Custom("custom_type".to_string()).type_name(),
            "custom_type"
        );
    }

    #[test]
    fn test_type_mismatch_error() {
        let comparator = ComparatorType::Int;

        let int_val = Value::Integer(42);
        let text_val = Value::Text("hello".to_string());

        // Should return error for type mismatch
        assert!(comparator.compare(&int_val, &text_val).is_err());
    }

    #[test]
    fn test_display_formatting() {
        assert_eq!(format!("{}", ComparatorType::Int), "int");
        assert_eq!(
            format!("{}", ComparatorType::List(Box::new(ComparatorType::Text))),
            "list<text>"
        );
        assert_eq!(
            format!(
                "{}",
                ComparatorType::Map(
                    Box::new(ComparatorType::Text),
                    Box::new(ComparatorType::Int)
                )
            ),
            "map<text, int>"
        );
        assert_eq!(
            format!("{}", ComparatorType::Frozen(Box::new(ComparatorType::Text))),
            "frozen<text>"
        );

        let udt_comparator = ComparatorType::Udt {
            type_name: "Person".to_string(),
            keyspace: Some("test".to_string()),
            field_comparators: vec![],
        };
        assert_eq!(format!("{}", udt_comparator), "test.Person");
    }
}
