use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple, PyBytes, PyString};
use pyo3::{Python, PyResult, PyObject};
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc, NaiveDate, NaiveTime};
use uuid::Uuid;

/// CQL type representation and conversion to Python types
/// 
/// This module handles the mapping between Cassandra CQL types and Python types,
/// ensuring seamless data conversion when querying SSTable files.
/// 
/// Type Mappings:
/// - text, varchar, ascii -> str
/// - int, smallint, tinyint -> int
/// - bigint, counter -> int
/// - float -> float
/// - double -> float  
/// - boolean -> bool
/// - blob -> bytes
/// - uuid, timeuuid -> uuid.UUID
/// - timestamp -> datetime.datetime
/// - date -> datetime.date
/// - time -> datetime.time
/// - inet -> ipaddress.IPv4Address | IPv6Address
/// - decimal, varint -> decimal.Decimal
/// - list<T> -> List[T]
/// - set<T> -> Set[T]
/// - map<K,V> -> Dict[K,V]
/// - tuple<T1,T2,...> -> Tuple[T1,T2,...]
/// - UDT -> dataclass or NamedTuple

#[derive(Debug, Clone)]
pub enum CQLValue {
    // Simple types
    Text(String),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Blob(Vec<u8>),
    Uuid(Uuid),
    TimeUuid(Uuid),
    Timestamp(DateTime<Utc>),
    Date(NaiveDate),
    Time(NaiveTime),
    Decimal(String), // Store as string for Python decimal conversion
    VarInt(String),  // Store as string for Python int conversion
    Inet(String),    // Store as string for Python ipaddress conversion
    
    // Collection types
    List(Vec<CQLValue>),
    Set(HashSet<CQLValue>),
    Map(HashMap<CQLValue, CQLValue>),
    Tuple(Vec<CQLValue>),
    
    // User-defined types
    UDT(HashMap<String, CQLValue>),
    
    // Special values
    Null,
    Empty,
}

impl std::hash::Hash for CQLValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            CQLValue::Text(s) => s.hash(state),
            CQLValue::Int(i) => i.hash(state),
            CQLValue::BigInt(i) => i.hash(state),
            CQLValue::Float(f) => f.to_bits().hash(state),
            CQLValue::Double(f) => f.to_bits().hash(state),
            CQLValue::Boolean(b) => b.hash(state),
            CQLValue::Blob(b) => b.hash(state),
            CQLValue::Uuid(u) => u.hash(state),
            CQLValue::TimeUuid(u) => u.hash(state),
            CQLValue::Timestamp(t) => t.timestamp_nanos().hash(state),
            CQLValue::Date(d) => d.hash(state),
            CQLValue::Time(t) => t.hash(state),
            CQLValue::Decimal(s) => s.hash(state),
            CQLValue::VarInt(s) => s.hash(state),
            CQLValue::Inet(s) => s.hash(state),
            _ => 0.hash(state), // Collections and complex types
        }
    }
}

impl PartialEq for CQLValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CQLValue::Text(a), CQLValue::Text(b)) => a == b,
            (CQLValue::Int(a), CQLValue::Int(b)) => a == b,
            (CQLValue::BigInt(a), CQLValue::BigInt(b)) => a == b,
            (CQLValue::Float(a), CQLValue::Float(b)) => a == b,
            (CQLValue::Double(a), CQLValue::Double(b)) => a == b,
            (CQLValue::Boolean(a), CQLValue::Boolean(b)) => a == b,
            (CQLValue::Blob(a), CQLValue::Blob(b)) => a == b,
            (CQLValue::Uuid(a), CQLValue::Uuid(b)) => a == b,
            (CQLValue::TimeUuid(a), CQLValue::TimeUuid(b)) => a == b,
            (CQLValue::Timestamp(a), CQLValue::Timestamp(b)) => a == b,
            (CQLValue::Date(a), CQLValue::Date(b)) => a == b,
            (CQLValue::Time(a), CQLValue::Time(b)) => a == b,
            (CQLValue::Decimal(a), CQLValue::Decimal(b)) => a == b,
            (CQLValue::VarInt(a), CQLValue::VarInt(b)) => a == b,
            (CQLValue::Inet(a), CQLValue::Inet(b)) => a == b,
            (CQLValue::List(a), CQLValue::List(b)) => a == b,
            (CQLValue::Set(a), CQLValue::Set(b)) => a == b,
            (CQLValue::Map(a), CQLValue::Map(b)) => a == b,
            (CQLValue::Tuple(a), CQLValue::Tuple(b)) => a == b,
            (CQLValue::UDT(a), CQLValue::UDT(b)) => a == b,
            (CQLValue::Null, CQLValue::Null) => true,
            (CQLValue::Empty, CQLValue::Empty) => true,
            _ => false,
        }
    }
}

impl Eq for CQLValue {}

impl CQLValue {
    /// Convert CQLValue to Python object
    pub fn to_python(&self, py: Python) -> PyResult<PyObject> {
        match self {
            CQLValue::Text(s) => Ok(PyString::new(py, s).into()),
            CQLValue::Int(i) => Ok(i.to_object(py)),
            CQLValue::BigInt(i) => Ok(i.to_object(py)),
            CQLValue::Float(f) => Ok(f.to_object(py)),
            CQLValue::Double(f) => Ok(f.to_object(py)),
            CQLValue::Boolean(b) => Ok(b.to_object(py)),
            CQLValue::Blob(bytes) => Ok(PyBytes::new(py, bytes).into()),
            
            CQLValue::Uuid(uuid) => {
                let uuid_module = py.import("uuid")?;
                let uuid_class = uuid_module.getattr("UUID")?;
                uuid_class.call1((uuid.to_string(),))
            }
            
            CQLValue::TimeUuid(uuid) => {
                let uuid_module = py.import("uuid")?;
                let uuid_class = uuid_module.getattr("UUID")?;
                uuid_class.call1((uuid.to_string(),))
            }
            
            CQLValue::Timestamp(dt) => {
                let datetime_module = py.import("datetime")?;
                let datetime_class = datetime_module.getattr("datetime")?;
                datetime_class.call_method1("fromtimestamp", (dt.timestamp(),))
            }
            
            CQLValue::Date(date) => {
                let datetime_module = py.import("datetime")?;
                let date_class = datetime_module.getattr("date")?;
                date_class.call1((date.year(), date.month(), date.day()))
            }
            
            CQLValue::Time(time) => {
                let datetime_module = py.import("datetime")?;
                let time_class = datetime_module.getattr("time")?;
                time_class.call1((
                    time.hour(),
                    time.minute(), 
                    time.second(),
                    time.nanosecond() / 1000, // microseconds
                ))
            }
            
            CQLValue::Decimal(s) => {
                let decimal_module = py.import("decimal")?;
                let decimal_class = decimal_module.getattr("Decimal")?;
                decimal_class.call1((s,))
            }
            
            CQLValue::VarInt(s) => {
                // Convert to Python int
                Ok(s.parse::<i64>().unwrap_or(0).to_object(py))
            }
            
            CQLValue::Inet(s) => {
                let ipaddress_module = py.import("ipaddress")?;
                ipaddress_module.call_method1("ip_address", (s,))
            }
            
            CQLValue::List(list) => {
                let py_list = PyList::empty(py);
                for item in list {
                    py_list.append(item.to_python(py)?)?;
                }
                Ok(py_list.into())
            }
            
            CQLValue::Set(set) => {
                let py_set = py.import("builtins")?.getattr("set")?.call0()?;
                for item in set {
                    py_set.call_method1("add", (item.to_python(py)?,))?;
                }
                Ok(py_set)
            }
            
            CQLValue::Map(map) => {
                let py_dict = PyDict::new(py);
                for (key, value) in map {
                    py_dict.set_item(key.to_python(py)?, value.to_python(py)?)?;
                }
                Ok(py_dict.into())
            }
            
            CQLValue::Tuple(tuple) => {
                let py_items: Result<Vec<_>, _> = tuple
                    .iter()
                    .map(|item| item.to_python(py))
                    .collect();
                Ok(PyTuple::new(py, py_items?).into())
            }
            
            CQLValue::UDT(udt) => {
                // Convert UDT to a Python dict or namedtuple
                let py_dict = PyDict::new(py);
                for (field_name, field_value) in udt {
                    py_dict.set_item(field_name, field_value.to_python(py)?)?;
                }
                Ok(py_dict.into())
            }
            
            CQLValue::Null => Ok(py.None()),
            CQLValue::Empty => Ok(py.None()),
        }
    }
    
    /// Get the Python type name for this CQL value
    pub fn python_type_name(&self) -> &'static str {
        match self {
            CQLValue::Text(_) => "str",
            CQLValue::Int(_) => "int",
            CQLValue::BigInt(_) => "int",
            CQLValue::Float(_) => "float",
            CQLValue::Double(_) => "float",
            CQLValue::Boolean(_) => "bool",
            CQLValue::Blob(_) => "bytes",
            CQLValue::Uuid(_) | CQLValue::TimeUuid(_) => "uuid.UUID",
            CQLValue::Timestamp(_) => "datetime.datetime",
            CQLValue::Date(_) => "datetime.date",
            CQLValue::Time(_) => "datetime.time",
            CQLValue::Decimal(_) => "decimal.Decimal",
            CQLValue::VarInt(_) => "int",
            CQLValue::Inet(_) => "ipaddress.IPv4Address | ipaddress.IPv6Address",
            CQLValue::List(_) => "List",
            CQLValue::Set(_) => "Set",
            CQLValue::Map(_, _) => "Dict",
            CQLValue::Tuple(_) => "Tuple",
            CQLValue::UDT(_) => "Dict",
            CQLValue::Null | CQLValue::Empty => "None",
        }
    }
}

/// A row of data from an SSTable query result
#[derive(Debug, Clone)]
pub struct CQLiteRow {
    pub columns: HashMap<String, CQLValue>,
}

impl CQLiteRow {
    /// Create a new empty row
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }
    
    /// Add a column value to the row
    pub fn add_column(&mut self, name: String, value: CQLValue) {
        self.columns.insert(name, value);
    }
    
    /// Get a column value by name
    pub fn get_column(&self, name: &str) -> Option<&CQLValue> {
        self.columns.get(name)
    }
    
    /// Convert row to Python dictionary
    pub fn to_pydict(&self, py: Python) -> PyResult<PyObject> {
        let py_dict = PyDict::new(py);
        
        for (column_name, column_value) in &self.columns {
            py_dict.set_item(column_name, column_value.to_python(py)?)?;
        }
        
        Ok(py_dict.into())
    }
    
    /// Get column names
    pub fn column_names(&self) -> Vec<&String> {
        self.columns.keys().collect()
    }
    
    /// Get column count
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

impl Default for CQLiteRow {
    fn default() -> Self {
        Self::new()
    }
}

/// Type information for schema representation
#[derive(Debug, Clone)]
pub struct CQLTypeInfo {
    pub name: String,
    pub cql_type: String,
    pub python_type: String,
    pub nullable: bool,
    pub is_partition_key: bool,
    pub is_clustering_key: bool,
    pub clustering_order: Option<String>, // ASC or DESC
}

impl CQLTypeInfo {
    /// Convert to Python dictionary
    pub fn to_pydict(&self, py: Python) -> PyResult<PyObject> {
        let py_dict = PyDict::new(py);
        
        py_dict.set_item("name", &self.name)?;
        py_dict.set_item("cql_type", &self.cql_type)?;
        py_dict.set_item("python_type", &self.python_type)?;
        py_dict.set_item("nullable", self.nullable)?;
        py_dict.set_item("is_partition_key", self.is_partition_key)?;
        py_dict.set_item("is_clustering_key", self.is_clustering_key)?;
        
        if let Some(ref order) = self.clustering_order {
            py_dict.set_item("clustering_order", order)?;
        } else {
            py_dict.set_item("clustering_order", py.None())?;
        }
        
        Ok(py_dict.into())
    }
}

/// Schema information for an SSTable
#[derive(Debug, Clone)]
pub struct CQLiteSchema {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<CQLTypeInfo>,
}

impl CQLiteSchema {
    /// Convert to Python dictionary
    pub fn to_pydict(&self, py: Python) -> PyResult<PyObject> {
        let py_dict = PyDict::new(py);
        
        py_dict.set_item("keyspace", &self.keyspace)?;
        py_dict.set_item("table", &self.table)?;
        
        let py_columns = PyList::empty(py);
        for column in &self.columns {
            py_columns.append(column.to_pydict(py)?)?;
        }
        py_dict.set_item("columns", py_columns)?;
        
        // Separate partition and clustering keys
        let partition_keys: Vec<_> = self.columns
            .iter()
            .filter(|c| c.is_partition_key)
            .map(|c| &c.name)
            .collect();
        
        let clustering_keys: Vec<_> = self.columns
            .iter()
            .filter(|c| c.is_clustering_key)
            .map(|c| &c.name)
            .collect();
        
        py_dict.set_item("partition_keys", partition_keys)?;
        py_dict.set_item("clustering_keys", clustering_keys)?;
        
        Ok(py_dict.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn test_cql_value_conversion() {
        Python::with_gil(|py| {
            let text_val = CQLValue::Text("hello".to_string());
            let py_obj = text_val.to_python(py).unwrap();
            assert!(py_obj.is_instance_of::<PyString>(py));
            
            let int_val = CQLValue::Int(42);
            let py_obj = int_val.to_python(py).unwrap();
            assert_eq!(py_obj.extract::<i32>(py).unwrap(), 42);
        });
    }
    
    #[test]
    fn test_cqlite_row() {
        let mut row = CQLiteRow::new();
        row.add_column("id".to_string(), CQLValue::Int(1));
        row.add_column("name".to_string(), CQLValue::Text("Alice".to_string()));
        
        assert_eq!(row.column_count(), 2);
        assert!(row.get_column("id").is_some());
        assert!(row.get_column("name").is_some());
        assert!(row.get_column("age").is_none());
    }
}