use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::{Python, PyResult, PyObject};
use std::fs::File;
use std::io::{Write, BufWriter};
use crate::reader::SSTableReader;
use crate::types::CQLiteRow;
use crate::errors::QueryError;

/// Export functionality for CQLite query results
/// 
/// This module provides various export formats for SSTable query results,
/// enabling seamless integration with data analysis workflows.

/// CSV exporter
pub struct CsvExporter {
    delimiter: String,
    include_header: bool,
}

impl CsvExporter {
    pub fn new(delimiter: String, include_header: bool) -> Self {
        CsvExporter {
            delimiter,
            include_header,
        }
    }
    
    pub fn export(
        &self,
        reader: &SSTableReader,
        py: Python,
        sql: &str,
        output_path: &str,
    ) -> PyResult<PyObject> {
        // Execute query to get results
        let results_obj = reader.query(py, sql.to_string(), None, None)?;
        let results_list = results_obj.downcast::<PyList>(py)?;
        
        // Create output file
        let file = File::create(output_path)
            .map_err(|e| QueryError::new_err(format!("Failed to create CSV file: {}", e)))?;
        let mut writer = BufWriter::new(file);
        
        let mut rows_written = 0usize;
        let mut columns_written = 0usize;
        
        // Write header if requested and we have data
        if self.include_header && results_list.len() > 0 {
            if let Ok(first_row) = results_list.get_item(0) {
                if let Ok(row_dict) = first_row.downcast::<PyDict>() {
                    let column_names: Vec<String> = row_dict.keys()
                        .iter()
                        .map(|k| k.to_string())
                        .collect();
                    
                    columns_written = column_names.len();
                    let header = column_names.join(&self.delimiter);
                    writeln!(writer, "{}", header)
                        .map_err(|e| QueryError::new_err(format!("Failed to write CSV header: {}", e)))?;
                }
            }
        }
        
        // Write data rows
        for item in results_list.iter() {
            if let Ok(row_dict) = item.downcast::<PyDict>() {
                let values: Vec<String> = row_dict.values()
                    .iter()
                    .map(|v| self.format_csv_value(v))
                    .collect();
                
                let row_line = values.join(&self.delimiter);
                writeln!(writer, "{}", row_line)
                    .map_err(|e| QueryError::new_err(format!("Failed to write CSV row: {}", e)))?;
                
                rows_written += 1;
            }
        }
        
        writer.flush()
            .map_err(|e| QueryError::new_err(format!("Failed to flush CSV file: {}", e)))?;
        
        // Return export statistics
        let stats = PyDict::new(py);
        stats.set_item("format", "csv")?;
        stats.set_item("output_path", output_path)?;
        stats.set_item("rows_written", rows_written)?;
        stats.set_item("columns_written", columns_written)?;
        stats.set_item("file_size_bytes", std::fs::metadata(output_path).map(|m| m.len()).unwrap_or(0))?;
        
        Ok(stats.into())
    }
    
    fn format_csv_value(&self, value: &PyAny) -> String {
        if value.is_none() {
            String::new()
        } else if let Ok(s) = value.extract::<String>() {
            // Escape quotes and wrap in quotes if contains delimiter or quotes
            if s.contains(&self.delimiter) || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s
            }
        } else {
            value.to_string()
        }
    }
}

/// Parquet exporter
pub struct ParquetExporter {
    compression: String,
}

impl ParquetExporter {
    pub fn new(compression: String) -> Self {
        ParquetExporter { compression }
    }
    
    pub fn export(
        &self,
        reader: &SSTableReader,
        py: Python,
        sql: &str,
        output_path: &str,
    ) -> PyResult<PyObject> {
        // For Parquet export, we'll use pandas + pyarrow
        // First get results as DataFrame
        let df = reader.query_df(py, sql.to_string())?;
        
        // Use pandas to_parquet method
        let kwargs = PyDict::new(py);
        kwargs.set_item("compression", &self.compression)?;
        
        df.call_method("to_parquet", (output_path,), Some(kwargs))?;
        
        // Get file statistics
        let file_size = std::fs::metadata(output_path)
            .map(|m| m.len())
            .unwrap_or(0);
        
        // Get row count from DataFrame
        let row_count = df.getattr("shape")?
            .get_item(0)?
            .extract::<usize>()?;
        
        let col_count = df.getattr("shape")?
            .get_item(1)?
            .extract::<usize>()?;
        
        // Return export statistics
        let stats = PyDict::new(py);
        stats.set_item("format", "parquet")?;
        stats.set_item("output_path", output_path)?;
        stats.set_item("rows_written", row_count)?;
        stats.set_item("columns_written", col_count)?;
        stats.set_item("file_size_bytes", file_size)?;
        stats.set_item("compression", &self.compression)?;
        
        Ok(stats.into())
    }
}

/// JSON exporter (supports both JSON Lines and JSON Array formats)
pub struct JsonExporter {
    format: String, // "lines" or "array"
}

impl JsonExporter {
    pub fn new(format: String) -> Self {
        JsonExporter { format }
    }
    
    pub fn export(
        &self,
        reader: &SSTableReader,
        py: Python,
        sql: &str,
        output_path: &str,
    ) -> PyResult<PyObject> {
        // Execute query to get results
        let results_obj = reader.query(py, sql.to_string(), None, None)?;
        let results_list = results_obj.downcast::<PyList>(py)?;
        
        // Create output file
        let file = File::create(output_path)
            .map_err(|e| QueryError::new_err(format!("Failed to create JSON file: {}", e)))?;
        let mut writer = BufWriter::new(file);
        
        let mut rows_written = 0usize;
        
        match self.format.as_str() {
            "lines" => {
                // JSON Lines format (one JSON object per line)
                for item in results_list.iter() {
                    let json_str = self.convert_to_json_string(py, item)?;
                    writeln!(writer, "{}", json_str)
                        .map_err(|e| QueryError::new_err(format!("Failed to write JSON line: {}", e)))?;
                    rows_written += 1;
                }
            }
            "array" => {
                // JSON Array format
                write!(writer, "[")
                    .map_err(|e| QueryError::new_err(format!("Failed to write JSON array start: {}", e)))?;
                
                for (i, item) in results_list.iter().enumerate() {
                    if i > 0 {
                        write!(writer, ",")
                            .map_err(|e| QueryError::new_err(format!("Failed to write JSON comma: {}", e)))?;
                    }
                    
                    let json_str = self.convert_to_json_string(py, item)?;
                    write!(writer, "{}", json_str)
                        .map_err(|e| QueryError::new_err(format!("Failed to write JSON object: {}", e)))?;
                    rows_written += 1;
                }
                
                write!(writer, "]")
                    .map_err(|e| QueryError::new_err(format!("Failed to write JSON array end: {}", e)))?;
            }
            _ => {
                return Err(QueryError::new_err(format!(
                    "Unsupported JSON format: {}. Use 'lines' or 'array'.", self.format
                )));
            }
        }
        
        writer.flush()
            .map_err(|e| QueryError::new_err(format!("Failed to flush JSON file: {}", e)))?;
        
        // Get column count from first row
        let columns_written = if results_list.len() > 0 {
            if let Ok(first_row) = results_list.get_item(0) {
                if let Ok(row_dict) = first_row.downcast::<PyDict>() {
                    row_dict.len()
                } else {
                    0
                }
            } else {
                0
            }
        } else {
            0
        };
        
        // Return export statistics
        let stats = PyDict::new(py);
        stats.set_item("format", format!("json-{}", self.format))?;
        stats.set_item("output_path", output_path)?;
        stats.set_item("rows_written", rows_written)?;
        stats.set_item("columns_written", columns_written)?;
        stats.set_item("file_size_bytes", std::fs::metadata(output_path).map(|m| m.len()).unwrap_or(0))?;
        
        Ok(stats.into())
    }
    
    fn convert_to_json_string(&self, py: Python, obj: &PyAny) -> PyResult<String> {
        // Use Python's json module for proper serialization
        let json_module = py.import("json")?;
        let json_str = json_module.call_method1("dumps", (obj,))?;
        Ok(json_str.extract::<String>()?)
    }
}

/// Excel exporter (using pandas + openpyxl)
pub struct ExcelExporter {
    sheet_name: String,
    include_index: bool,
}

impl ExcelExporter {
    pub fn new(sheet_name: Option<String>, include_index: bool) -> Self {
        ExcelExporter {
            sheet_name: sheet_name.unwrap_or_else(|| "Sheet1".to_string()),
            include_index,
        }
    }
    
    pub fn export(
        &self,
        reader: &SSTableReader,
        py: Python,
        sql: &str,
        output_path: &str,
    ) -> PyResult<PyObject> {
        // Get results as DataFrame
        let df = reader.query_df(py, sql.to_string())?;
        
        // Use pandas to_excel method
        let kwargs = PyDict::new(py);
        kwargs.set_item("sheet_name", &self.sheet_name)?;
        kwargs.set_item("index", self.include_index)?;
        
        df.call_method("to_excel", (output_path,), Some(kwargs))?;
        
        // Get statistics
        let file_size = std::fs::metadata(output_path)
            .map(|m| m.len())
            .unwrap_or(0);
        
        let row_count = df.getattr("shape")?
            .get_item(0)?
            .extract::<usize>()?;
        
        let col_count = df.getattr("shape")?
            .get_item(1)?
            .extract::<usize>()?;
        
        // Return export statistics
        let stats = PyDict::new(py);
        stats.set_item("format", "excel")?;
        stats.set_item("output_path", output_path)?;
        stats.set_item("rows_written", row_count)?;
        stats.set_item("columns_written", col_count)?;
        stats.set_item("file_size_bytes", file_size)?;
        stats.set_item("sheet_name", &self.sheet_name)?;
        
        Ok(stats.into())
    }
}

/// Multi-format batch exporter
pub struct BatchExporter;

impl BatchExporter {
    /// Export query results to multiple formats simultaneously
    pub fn export_multi_format(
        reader: &SSTableReader,
        py: Python,
        sql: &str,
        output_base_path: &str,
        formats: Vec<String>,
    ) -> PyResult<PyObject> {
        let mut export_results = Vec::new();
        
        for format in formats {
            let output_path = format!("{}.{}", output_base_path, format);
            
            let result = match format.as_str() {
                "csv" => {
                    let exporter = CsvExporter::new(",".to_string(), true);
                    exporter.export(reader, py, sql, &output_path)?
                }
                "json" => {
                    let exporter = JsonExporter::new("lines".to_string());
                    exporter.export(reader, py, sql, &output_path)?
                }
                "parquet" => {
                    let exporter = ParquetExporter::new("snappy".to_string());
                    exporter.export(reader, py, sql, &output_path)?
                }
                "excel" => {
                    let exporter = ExcelExporter::new(None, false);
                    exporter.export(reader, py, sql, &output_path)?
                }
                _ => {
                    return Err(QueryError::new_err(format!(
                        "Unsupported export format: {}", format
                    )));
                }
            };
            
            export_results.push(result);
        }
        
        // Return summary of all exports
        let summary = PyDict::new(py);
        summary.set_item("exports", PyList::new(py, export_results))?;
        summary.set_item("total_formats", formats.len())?;
        
        Ok(summary.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_exporter_creation() {
        let exporter = CsvExporter::new(",".to_string(), true);
        assert_eq!(exporter.delimiter, ",");
        assert!(exporter.include_header);
    }
    
    #[test]
    fn test_csv_value_formatting() {
        let exporter = CsvExporter::new(",".to_string(), true);
        
        assert_eq!(exporter.format_csv_value(&"simple"), "simple");
        assert_eq!(exporter.format_csv_value(&"with,comma"), "\"with,comma\"");
        assert_eq!(exporter.format_csv_value(&"with\"quote"), "\"with\"\"quote\"");
    }
    
    #[test]
    fn test_json_exporter_creation() {
        let exporter = JsonExporter::new("lines".to_string());
        assert_eq!(exporter.format, "lines");
        
        let exporter = JsonExporter::new("array".to_string());
        assert_eq!(exporter.format, "array");
    }
}