use pyo3::prelude::*;
use pyo3::exceptions::PyException;

/// Custom exception types for CQLite Python bindings
/// 
/// These exceptions provide specific error handling for different types of
/// failures that can occur when querying SSTable files.

/// Base exception for all CQLite errors
#[pyclass(extends=PyException)]
pub struct CQLiteError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub error_code: Option<String>,
}

#[pymethods]
impl CQLiteError {
    #[new]
    fn new(message: String, error_code: Option<String>) -> Self {
        CQLiteError { message, error_code }
    }
    
    fn __str__(&self) -> String {
        if let Some(ref code) = self.error_code {
            format!("[{}] {}", code, self.message)
        } else {
            self.message.clone()
        }
    }
    
    fn __repr__(&self) -> String {
        format!("CQLiteError('{}')", self.message)
    }
}

/// Schema-related errors (invalid schema, missing columns, etc.)
#[pyclass(extends=PyException)]
pub struct SchemaError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub schema_path: Option<String>,
}

#[pymethods]
impl SchemaError {
    #[new]
    fn new(message: String, schema_path: Option<String>) -> Self {
        SchemaError { message, schema_path }
    }
    
    fn __str__(&self) -> String {
        if let Some(ref path) = self.schema_path {
            format!("Schema error in '{}': {}", path, self.message)
        } else {
            format!("Schema error: {}", self.message)
        }
    }
}

/// Query execution errors (invalid SQL, unsupported operations, etc.)
#[pyclass(extends=PyException)]
pub struct QueryError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub sql: Option<String>,
    #[pyo3(get)]
    pub position: Option<u32>,
}

#[pymethods]
impl QueryError {
    #[new]
    fn new(message: String, sql: Option<String>, position: Option<u32>) -> Self {
        QueryError { message, sql, position }
    }
    
    fn __str__(&self) -> String {
        match (&self.sql, self.position) {
            (Some(sql), Some(pos)) => {
                format!("Query error at position {}: {}\nSQL: {}", pos, self.message, sql)
            }
            (Some(sql), None) => {
                format!("Query error: {}\nSQL: {}", self.message, sql)
            }
            _ => format!("Query error: {}", self.message),
        }
    }
}

/// SSTable file errors (file not found, corrupted, unsupported format, etc.)
#[pyclass(extends=PyException)]
pub struct SSTableError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub file_path: Option<String>,
    #[pyo3(get)]
    pub error_type: Option<String>,
}

#[pymethods]
impl SSTableError {
    #[new]
    fn new(message: String, file_path: Option<String>, error_type: Option<String>) -> Self {
        SSTableError { message, file_path, error_type }
    }
    
    fn __str__(&self) -> String {
        match (&self.file_path, &self.error_type) {
            (Some(path), Some(err_type)) => {
                format!("SSTable {} error in '{}': {}", err_type, path, self.message)
            }
            (Some(path), None) => {
                format!("SSTable error in '{}': {}", path, self.message)
            }
            _ => format!("SSTable error: {}", self.message),
        }
    }
}

/// Type conversion errors
#[pyclass(extends=PyException)]
pub struct TypeConversionError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub from_type: Option<String>,
    #[pyo3(get)]
    pub to_type: Option<String>,
}

#[pymethods]
impl TypeConversionError {
    #[new]
    fn new(message: String, from_type: Option<String>, to_type: Option<String>) -> Self {
        TypeConversionError { message, from_type, to_type }
    }
    
    fn __str__(&self) -> String {
        match (&self.from_type, &self.to_type) {
            (Some(from), Some(to)) => {
                format!("Type conversion error from {} to {}: {}", from, to, self.message)
            }
            _ => format!("Type conversion error: {}", self.message),
        }
    }
}

/// Convenience functions for creating exceptions from Rust errors
impl CQLiteError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<CQLiteError, _>(message.to_string())
    }
}

impl SchemaError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<SchemaError, _>(message.to_string())
    }
    
    pub fn new_err_with_path(message: impl ToString, schema_path: impl ToString) -> PyErr {
        let error = SchemaError::new(message.to_string(), Some(schema_path.to_string()));
        PyErr::from_instance(Py::new(Python::acquire_gil().python(), error).unwrap().as_ref(Python::acquire_gil().python()))
    }
}

impl QueryError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<QueryError, _>(message.to_string())
    }
    
    pub fn new_err_with_sql(message: impl ToString, sql: impl ToString) -> PyErr {
        let error = QueryError::new(message.to_string(), Some(sql.to_string()), None);
        PyErr::from_instance(Py::new(Python::acquire_gil().python(), error).unwrap().as_ref(Python::acquire_gil().python()))
    }
    
    pub fn new_err_with_position(message: impl ToString, sql: impl ToString, position: u32) -> PyErr {
        let error = QueryError::new(message.to_string(), Some(sql.to_string()), Some(position));
        PyErr::from_instance(Py::new(Python::acquire_gil().python(), error).unwrap().as_ref(Python::acquire_gil().python()))
    }
}

impl SSTableError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<SSTableError, _>(message.to_string())
    }
    
    pub fn new_err_with_path(message: impl ToString, file_path: impl ToString) -> PyErr {
        let error = SSTableError::new(message.to_string(), Some(file_path.to_string()), None);
        PyErr::from_instance(Py::new(Python::acquire_gil().python(), error).unwrap().as_ref(Python::acquire_gil().python()))
    }
    
    pub fn new_err_with_type(
        message: impl ToString, 
        file_path: impl ToString, 
        error_type: impl ToString
    ) -> PyErr {
        let error = SSTableError::new(
            message.to_string(), 
            Some(file_path.to_string()), 
            Some(error_type.to_string())
        );
        PyErr::from_instance(Py::new(Python::acquire_gil().python(), error).unwrap().as_ref(Python::acquire_gil().python()))
    }
}

impl TypeConversionError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<TypeConversionError, _>(message.to_string())
    }
    
    pub fn new_err_with_types(
        message: impl ToString, 
        from_type: impl ToString, 
        to_type: impl ToString
    ) -> PyErr {
        let error = TypeConversionError::new(
            message.to_string(), 
            Some(from_type.to_string()), 
            Some(to_type.to_string())
        );
        PyErr::from_instance(Py::new(Python::acquire_gil().python(), error).unwrap().as_ref(Python::acquire_gil().python()))
    }
}