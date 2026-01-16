//! Query result types for Python bindings.
//!
//! This module provides Python wrappers for query execution results:
//! - `QueryResult` - Container for rows, metadata, and timing
//! - `Row` - Individual row with dict-like access
//! - `ColumnInfo` - Column metadata
//! - `QueryResultIter` - Iterator for Pythonic for-loops

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;

use crate::value::{key_error, value_to_py};

/// Query result from execute().
///
/// Contains rows, metadata, and execution timing.
/// Supports iteration: `for row in result:`
///
/// # Attributes
///
/// * `rows` - List of Row objects
/// * `rows_affected` - Number of rows affected (for DML)
/// * `execution_time_ms` - Query execution time in milliseconds
/// * `columns` - List of ColumnInfo metadata
///
/// # Example
///
/// ```python
/// result = db.execute("SELECT * FROM users LIMIT 10")
/// print(f"Got {len(result)} rows in {result.execution_time_ms}ms")
/// for row in result:
///     print(row["name"])
/// ```
#[pyclass(module = "cqlite")]
pub struct QueryResult {
    /// Converted rows (using Py<Row> for GIL-independence)
    rows: Vec<Py<Row>>,
    /// Number of rows affected (for DML operations)
    rows_affected: u64,
    /// Execution time in milliseconds
    execution_time_ms: u64,
    /// Column metadata
    columns: Vec<ColumnInfo>,
}

#[pymethods]
impl QueryResult {
    /// Get all rows as a list.
    #[getter]
    fn rows(&self, py: Python<'_>) -> Vec<Py<Row>> {
        self.rows.iter().map(|r| r.clone_ref(py)).collect()
    }

    /// Number of rows affected (INSERT/UPDATE/DELETE).
    #[getter]
    fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Query execution time in milliseconds.
    #[getter]
    fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }

    /// Column information for the result set.
    #[getter]
    fn columns(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    /// Number of rows in result.
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    /// Make result iterable: `for row in result:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> QueryResultIter {
        QueryResultIter {
            rows: slf.rows.iter().map(|r| r.clone_ref(py)).collect(),
            index: 0,
        }
    }

    /// Convert entire result to Python dict.
    ///
    /// Returns a dict with keys: rows, rows_affected, execution_time_ms, columns
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);

        // Convert rows to list of dicts
        let rows_list: Vec<PyObject> = self
            .rows
            .iter()
            .map(|row| row.borrow(py).to_dict(py))
            .collect::<PyResult<Vec<_>>>()?;
        dict.set_item("rows", PyList::new(py, rows_list)?)?;

        // Add metadata
        dict.set_item("rows_affected", self.rows_affected)?;
        dict.set_item("execution_time_ms", self.execution_time_ms)?;

        // Convert columns
        let columns_list: Vec<PyObject> = self
            .columns
            .iter()
            .map(|col| col.to_dict(py))
            .collect::<PyResult<Vec<_>>>()?;
        dict.set_item("columns", PyList::new(py, columns_list)?)?;

        Ok(dict.into_any().unbind())
    }

    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "QueryResult(rows={}, rows_affected={}, execution_time_ms={})",
            self.rows.len(),
            self.rows_affected,
            self.execution_time_ms
        )
    }
}

impl QueryResult {
    /// Convert from cqlite_core QueryResult.
    ///
    /// Eagerly converts all rows and values to Python objects.
    /// This is done within the GIL to ensure thread safety.
    pub(crate) fn from_core(
        py: Python<'_>,
        result: cqlite_core::query::result::QueryResult,
    ) -> PyResult<Self> {
        // Convert columns metadata
        let columns: Vec<ColumnInfo> = result
            .metadata
            .columns
            .iter()
            .map(ColumnInfo::from_core)
            .collect();

        // Convert all rows eagerly and wrap in Py<Row>
        let rows: Vec<Py<Row>> = result
            .rows
            .iter()
            .map(|row| {
                let row_obj = Row::from_core(py, row)?;
                Py::new(py, row_obj)
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            rows,
            rows_affected: result.rows_affected,
            execution_time_ms: result.execution_time_ms,
            columns,
        })
    }
}

/// Iterator over QueryResult rows.
///
/// Enables Pythonic iteration: `for row in result:`
#[pyclass(module = "cqlite")]
pub struct QueryResultIter {
    rows: Vec<Py<Row>>,
    index: usize,
}

#[pymethods]
impl QueryResultIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> Option<Py<Row>> {
        if slf.index < slf.rows.len() {
            let row = slf.rows[slf.index].clone_ref(py);
            slf.index += 1;
            Some(row)
        } else {
            None
        }
    }
}

/// A single row from query results with dict-like access.
///
/// Supports both dict-style indexing and method access:
/// - `row["column_name"]` - Get value by column name
/// - `row.get("column", default)` - Get with fallback
/// - `row.keys()` - Get column names
/// - `"column" in row` - Check if column exists
///
/// # Example
///
/// ```python
/// row = result.rows[0]
/// name = row["name"]
/// age = row.get("age", 0)
/// for key in row.keys():
///     print(f"{key}: {row[key]}")
/// ```
#[pyclass(module = "cqlite")]
pub struct Row {
    /// Column values as Python objects, keyed by column name
    values: HashMap<String, PyObject>,
}

#[pymethods]
impl Row {
    /// Dict-style access: `row["column_name"]`
    ///
    /// Raises KeyError if column doesn't exist.
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<PyObject> {
        self.values
            .get(key)
            .map(|v| v.clone_ref(py))
            .ok_or_else(|| key_error(key))
    }

    /// Membership test: `"column" in row`
    fn __contains__(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// Get all column names.
    fn keys(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    /// Get all values.
    fn values(&self, py: Python<'_>) -> Vec<PyObject> {
        self.values.values().map(|v| v.clone_ref(py)).collect()
    }

    /// Get all (key, value) pairs.
    fn items(&self, py: Python<'_>) -> Vec<(String, PyObject)> {
        self.values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect()
    }

    /// Convert to Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (key, value) in &self.values {
            dict.set_item(key, value.clone_ref(py))?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Get value with default fallback.
    ///
    /// Returns the value for key if it exists, otherwise returns default.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyObject {
        self.values
            .get(key)
            .map(|v| v.clone_ref(py))
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None()))
    }

    /// Number of columns.
    fn __len__(&self) -> usize {
        self.values.len()
    }

    /// String representation.
    fn __repr__(&self) -> String {
        let mut keys: Vec<&str> = self.values.keys().map(|s| s.as_str()).collect();
        keys.sort(); // Sort for deterministic output
        if keys.len() <= 5 {
            format!("Row({{{}}})", keys.join(", "))
        } else {
            format!(
                "Row({{{}, ... and {} more}})",
                keys[..5].join(", "),
                keys.len() - 5
            )
        }
    }
}

impl Row {
    /// Convert from cqlite_core QueryRow.
    pub(crate) fn from_core(
        py: Python<'_>,
        row: &cqlite_core::query::result::QueryRow,
    ) -> PyResult<Self> {
        let mut values = HashMap::with_capacity(row.values.len());
        for (key, value) in &row.values {
            let py_value = value_to_py(py, value)?;
            values.insert(key.clone(), py_value);
        }
        Ok(Self { values })
    }
}

/// Column metadata for result set.
///
/// Provides information about each column in the query result.
///
/// # Attributes
///
/// * `name` - Column name
/// * `data_type` - CQL data type as string (e.g., "text", "int")
/// * `nullable` - Whether column can be null
/// * `position` - Column position (0-indexed)
/// * `table_name` - Original table name (for joined queries)
#[pyclass(module = "cqlite")]
#[derive(Clone)]
pub struct ColumnInfo {
    /// Column name
    #[pyo3(get)]
    name: String,
    /// CQL data type as string
    #[pyo3(get)]
    data_type: String,
    /// Whether column can be null
    #[pyo3(get)]
    nullable: bool,
    /// Column position in result set
    #[pyo3(get)]
    position: usize,
    /// Original table name (for joined queries)
    #[pyo3(get)]
    table_name: Option<String>,
}

#[pymethods]
impl ColumnInfo {
    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "ColumnInfo(name='{}', data_type='{}', nullable={}, position={})",
            self.name, self.data_type, self.nullable, self.position
        )
    }

    /// Convert to Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("data_type", &self.data_type)?;
        dict.set_item("nullable", self.nullable)?;
        dict.set_item("position", self.position)?;
        if let Some(ref table_name) = self.table_name {
            dict.set_item("table_name", table_name)?;
        }
        Ok(dict.into_any().unbind())
    }
}

impl ColumnInfo {
    /// Convert from cqlite_core ColumnInfo.
    fn from_core(col: &cqlite_core::query::result::ColumnInfo) -> Self {
        Self {
            name: col.name.clone(),
            data_type: format!("{:?}", col.data_type),
            nullable: col.nullable,
            position: col.position,
            table_name: col.table_name.clone(),
        }
    }
}

/// Register result types with the Python module.
pub fn register_result(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<QueryResult>()?;
    m.add_class::<QueryResultIter>()?;
    m.add_class::<Row>()?;
    m.add_class::<ColumnInfo>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_info_creation() {
        let col = ColumnInfo {
            name: "test_col".to_string(),
            data_type: "Text".to_string(),
            nullable: true,
            position: 0,
            table_name: None,
        };
        assert_eq!(col.name, "test_col");
        assert_eq!(col.data_type, "Text");
        assert!(col.nullable);
        assert_eq!(col.position, 0);
    }
}
