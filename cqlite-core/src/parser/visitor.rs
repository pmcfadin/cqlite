//! Visitor pattern implementations for CQL AST traversal
//!
//! This module provides default implementations and utilities for the visitor pattern,
//! allowing easy traversal and transformation of CQL AST nodes.

use crate::error::Result;
use crate::schema::{TableSchema, KeyColumn, ClusteringColumn, Column, CqlType};
use super::ast::*;
use super::traits::{CqlVisitor, ValidationContext};
use std::collections::HashMap;

/// Default visitor implementation that traverses the entire AST
/// 
/// This visitor provides default implementations for all visit methods
/// that recursively traverse child nodes. Implementations can override
/// specific methods to handle particular node types.
#[derive(Debug, Default)]
pub struct DefaultVisitor;

impl<T: Default> CqlVisitor<T> for DefaultVisitor {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<T> {
        match statement {
            CqlStatement::Select(select) => self.visit_select(select),
            CqlStatement::Insert(insert) => self.visit_insert(insert),
            CqlStatement::Update(update) => self.visit_update(update),
            CqlStatement::Delete(delete) => self.visit_delete(delete),
            CqlStatement::CreateTable(create) => self.visit_create_table(create),
            CqlStatement::DropTable(drop) => self.visit_drop_table(drop),
            CqlStatement::CreateIndex(create) => self.visit_create_index(create),
            CqlStatement::AlterTable(alter) => self.visit_alter_table(alter),
            CqlStatement::CreateType(_) => Ok(T::default()),
            CqlStatement::DropType(_) => Ok(T::default()),
            CqlStatement::Use(_) => Ok(T::default()),
            CqlStatement::Truncate(_) => Ok(T::default()),
            CqlStatement::Batch(_) => Ok(T::default()),
        }
    }
    
    fn visit_select(&mut self, select: &CqlSelect) -> Result<T> {
        // Visit select items
        for item in &select.select_list {
            match item {
                CqlSelectItem::Expression { expression, .. } => {
                    self.visit_expression(expression)?;
                }
                CqlSelectItem::Function { args, .. } => {
                    for arg in args {
                        self.visit_expression(arg)?;
                    }
                }
                CqlSelectItem::Wildcard => {}
            }
        }
        
        // Visit WHERE clause
        if let Some(where_clause) = &select.where_clause {
            self.visit_expression(where_clause)?;
        }
        
        Ok(T::default())
    }
    
    fn visit_insert(&mut self, insert: &CqlInsert) -> Result<T> {
        // Visit column names
        for column in &insert.columns {
            self.visit_identifier(column)?;
        }
        
        // Visit values
        match &insert.values {
            CqlInsertValues::Values(expressions) => {
                for expr in expressions {
                    self.visit_expression(expr)?;
                }
            }
            CqlInsertValues::Json(_) => {
                // JSON values are literal strings, no sub-expressions to visit
            }
        }
        
        // Visit USING clause
        if let Some(using) = &insert.using {
            if let Some(ttl) = &using.ttl {
                self.visit_expression(ttl)?;
            }
            if let Some(timestamp) = &using.timestamp {
                self.visit_expression(timestamp)?;
            }
        }
        
        Ok(T::default())
    }
    
    fn visit_update(&mut self, update: &CqlUpdate) -> Result<T> {
        // Visit assignments
        for assignment in &update.assignments {
            self.visit_identifier(&assignment.column)?;
            self.visit_expression(&assignment.value)?;
            
            // Visit map update key if present
            if let CqlAssignmentOperator::MapUpdate(key_expr) = &assignment.operator {
                self.visit_expression(key_expr)?;
            }
        }
        
        // Visit WHERE clause
        self.visit_expression(&update.where_clause)?;
        
        // Visit IF condition
        if let Some(if_condition) = &update.if_condition {
            self.visit_expression(if_condition)?;
        }
        
        // Visit USING clause
        if let Some(using) = &update.using {
            if let Some(ttl) = &using.ttl {
                self.visit_expression(ttl)?;
            }
            if let Some(timestamp) = &using.timestamp {
                self.visit_expression(timestamp)?;
            }
        }
        
        Ok(T::default())
    }
    
    fn visit_delete(&mut self, delete: &CqlDelete) -> Result<T> {
        // Visit column names
        for column in &delete.columns {
            self.visit_identifier(column)?;
        }
        
        // Visit WHERE clause
        self.visit_expression(&delete.where_clause)?;
        
        // Visit IF condition
        if let Some(if_condition) = &delete.if_condition {
            self.visit_expression(if_condition)?;
        }
        
        // Visit USING clause
        if let Some(using) = &delete.using {
            if let Some(timestamp) = &using.timestamp {
                self.visit_expression(timestamp)?;
            }
        }
        
        Ok(T::default())
    }
    
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<T> {
        // Visit table name
        self.visit_identifier(&create.table.name)?;
        if let Some(keyspace) = &create.table.keyspace {
            self.visit_identifier(keyspace)?;
        }
        
        // Visit column definitions
        for column in &create.columns {
            self.visit_identifier(&column.name)?;
            self.visit_data_type(&column.data_type)?;
        }
        
        // Visit primary key
        for pk_column in &create.primary_key.partition_key {
            self.visit_identifier(pk_column)?;
        }
        for ck_column in &create.primary_key.clustering_key {
            self.visit_identifier(ck_column)?;
        }
        
        Ok(T::default())
    }
    
    fn visit_drop_table(&mut self, drop: &CqlDropTable) -> Result<T> {
        // Visit table name
        self.visit_identifier(&drop.table.name)?;
        if let Some(keyspace) = &drop.table.keyspace {
            self.visit_identifier(keyspace)?;
        }
        
        Ok(T::default())
    }
    
    fn visit_create_index(&mut self, create: &CqlCreateIndex) -> Result<T> {
        // Visit index name
        if let Some(name) = &create.name {
            self.visit_identifier(name)?;
        }
        
        // Visit table name
        self.visit_identifier(&create.table.name)?;
        if let Some(keyspace) = &create.table.keyspace {
            self.visit_identifier(keyspace)?;
        }
        
        // Visit indexed columns
        for column in &create.columns {
            match column {
                CqlIndexColumn::Column(id) => self.visit_identifier(id)?,
                CqlIndexColumn::Keys(id) => self.visit_identifier(id)?,
                CqlIndexColumn::Values(id) => self.visit_identifier(id)?,
                CqlIndexColumn::Entries(id) => self.visit_identifier(id)?,
                CqlIndexColumn::Full(id) => self.visit_identifier(id)?,
            }
        }
        
        Ok(T::default())
    }
    
    fn visit_alter_table(&mut self, alter: &CqlAlterTable) -> Result<T> {
        // Visit table name
        self.visit_identifier(&alter.table.name)?;
        if let Some(keyspace) = &alter.table.keyspace {
            self.visit_identifier(keyspace)?;
        }
        
        // Visit operation
        match &alter.operation {
            CqlAlterTableOp::AddColumn(column_def) => {
                self.visit_identifier(&column_def.name)?;
                self.visit_data_type(&column_def.data_type)?;
            }
            CqlAlterTableOp::DropColumn(column) => {
                self.visit_identifier(column)?;
            }
            CqlAlterTableOp::AlterColumn { column, new_type } => {
                self.visit_identifier(column)?;
                self.visit_data_type(new_type)?;
            }
            CqlAlterTableOp::RenameColumn { old_name, new_name } => {
                self.visit_identifier(old_name)?;
                self.visit_identifier(new_name)?;
            }
            CqlAlterTableOp::WithOptions(_) => {
                // Table options are literals, no sub-expressions to visit
            }
        }
        
        Ok(T::default())
    }
    
    fn visit_data_type(&mut self, data_type: &CqlDataType) -> Result<T> {
        match data_type {
            CqlDataType::List(inner) |
            CqlDataType::Set(inner) |
            CqlDataType::Frozen(inner) => {
                self.visit_data_type(inner)?;
            }
            CqlDataType::Map(key_type, value_type) => {
                self.visit_data_type(key_type)?;
                self.visit_data_type(value_type)?;
            }
            CqlDataType::Tuple(types) => {
                for typ in types {
                    self.visit_data_type(typ)?;
                }
            }
            CqlDataType::Udt(name) => {
                self.visit_identifier(name)?;
            }
            _ => {
                // Primitive types have no sub-components to visit
            }
        }
        
        Ok(T::default())
    }
    
    fn visit_expression(&mut self, expression: &CqlExpression) -> Result<T> {
        match expression {
            CqlExpression::Literal(literal) => self.visit_literal(literal),
            CqlExpression::Column(column) => self.visit_identifier(column),
            CqlExpression::Parameter(_) | CqlExpression::NamedParameter(_) => Ok(T::default()),
            CqlExpression::Binary { left, right, .. } => {
                self.visit_expression(left)?;
                self.visit_expression(right)?;
                Ok(T::default())
            }
            CqlExpression::Unary { operand, .. } => {
                self.visit_expression(operand)?;
                Ok(T::default())
            }
            CqlExpression::Function { name, args } => {
                self.visit_identifier(name)?;
                for arg in args {
                    self.visit_expression(arg)?;
                }
                Ok(T::default())
            }
            CqlExpression::In { expression, values } => {
                self.visit_expression(expression)?;
                for value in values {
                    self.visit_expression(value)?;
                }
                Ok(T::default())
            }
            CqlExpression::Contains { column, value } => {
                self.visit_identifier(column)?;
                self.visit_expression(value)?;
                Ok(T::default())
            }
            CqlExpression::ContainsKey { column, key } => {
                self.visit_identifier(column)?;
                self.visit_expression(key)?;
                Ok(T::default())
            }
            CqlExpression::CollectionAccess { collection, index } => {
                self.visit_expression(collection)?;
                self.visit_expression(index)?;
                Ok(T::default())
            }
            CqlExpression::FieldAccess { object, field } => {
                self.visit_expression(object)?;
                self.visit_identifier(field)?;
                Ok(T::default())
            }
            CqlExpression::Case { when_clauses, else_clause } => {
                for when_clause in when_clauses {
                    self.visit_expression(&when_clause.condition)?;
                    self.visit_expression(&when_clause.result)?;
                }
                if let Some(else_expr) = else_clause {
                    self.visit_expression(else_expr)?;
                }
                Ok(T::default())
            }
            CqlExpression::Cast { expression, target_type } => {
                self.visit_expression(expression)?;
                self.visit_data_type(target_type)?;
                Ok(T::default())
            }
        }
    }
    
    fn visit_identifier(&mut self, _identifier: &CqlIdentifier) -> Result<T> {
        Ok(T::default())
    }
    
    fn visit_literal(&mut self, literal: &CqlLiteral) -> Result<T> {
        match literal {
            CqlLiteral::Collection(collection) => {
                match collection {
                    CqlCollectionLiteral::List(items) |
                    CqlCollectionLiteral::Set(items) => {
                        for item in items {
                            let _: T = self.visit_literal(item)?;
                        }
                    }
                    CqlCollectionLiteral::Map(pairs) => {
                        for (key, value) in pairs {
                            let _: T = self.visit_literal(key)?;
                            let _: T = self.visit_literal(value)?;
                        }
                    }
                }
            }
            CqlLiteral::Udt(udt) => {
                for (field_name, field_value) in &udt.fields {
                    let _: T = self.visit_identifier(field_name)?;
                    let _: T = self.visit_literal(field_value)?;
                }
            }
            CqlLiteral::Tuple(items) => {
                for item in items {
                    let _: T = self.visit_literal(item)?;
                }
            }
            _ => {
                // Primitive literals have no sub-components to visit
            }
        }
        
        Ok(T::default())
    }
}

/// Visitor that collects all identifiers in an AST node
#[derive(Debug, Default)]
pub struct IdentifierCollector {
    pub identifiers: Vec<CqlIdentifier>,
}

impl CqlVisitor<()> for IdentifierCollector {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<()> {
        DefaultVisitor.visit_statement(statement)
    }
    
    fn visit_select(&mut self, select: &CqlSelect) -> Result<()> {
        DefaultVisitor.visit_select(select)
    }
    
    fn visit_insert(&mut self, insert: &CqlInsert) -> Result<()> {
        DefaultVisitor.visit_insert(insert)
    }
    
    fn visit_update(&mut self, update: &CqlUpdate) -> Result<()> {
        DefaultVisitor.visit_update(update)
    }
    
    fn visit_delete(&mut self, delete: &CqlDelete) -> Result<()> {
        DefaultVisitor.visit_delete(delete)
    }
    
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<()> {
        DefaultVisitor.visit_create_table(create)
    }
    
    fn visit_drop_table(&mut self, drop: &CqlDropTable) -> Result<()> {
        DefaultVisitor.visit_drop_table(drop)
    }
    
    fn visit_create_index(&mut self, create: &CqlCreateIndex) -> Result<()> {
        DefaultVisitor.visit_create_index(create)
    }
    
    fn visit_alter_table(&mut self, alter: &CqlAlterTable) -> Result<()> {
        DefaultVisitor.visit_alter_table(alter)
    }
    
    fn visit_data_type(&mut self, data_type: &CqlDataType) -> Result<()> {
        DefaultVisitor.visit_data_type(data_type)
    }
    
    fn visit_expression(&mut self, expression: &CqlExpression) -> Result<()> {
        DefaultVisitor.visit_expression(expression)
    }
    
    fn visit_identifier(&mut self, identifier: &CqlIdentifier) -> Result<()> {
        self.identifiers.push(identifier.clone());
        Ok(())
    }
    
    fn visit_literal(&mut self, literal: &CqlLiteral) -> Result<()> {
        DefaultVisitor.visit_literal(literal)
    }
}

/// Visitor that validates semantic correctness of CQL statements
#[derive(Debug)]
pub struct SemanticValidator {
    pub context: ValidationContext,
    pub errors: Vec<String>,
}

impl SemanticValidator {
    /// Create a new semantic validator with the given context
    pub fn new(context: ValidationContext) -> Self {
        Self {
            context,
            errors: Vec::new(),
        }
    }
    
    /// Add a validation error
    fn add_error(&mut self, message: String) {
        self.errors.push(message);
    }
    
    /// Check if validation passed (no errors)
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
    
    /// Get all validation errors
    pub fn get_errors(&self) -> &[String] {
        &self.errors
    }
}

impl CqlVisitor<()> for SemanticValidator {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<()> {
        match statement {
            CqlStatement::Select(select) => self.visit_select(select),
            CqlStatement::Insert(insert) => self.visit_insert(insert),
            CqlStatement::Update(update) => self.visit_update(update),
            CqlStatement::Delete(delete) => self.visit_delete(delete),
            CqlStatement::CreateTable(create) => self.visit_create_table(create),
            CqlStatement::DropTable(drop) => self.visit_drop_table(drop),
            CqlStatement::CreateIndex(create) => self.visit_create_index(create),
            CqlStatement::AlterTable(alter) => self.visit_alter_table(alter),
            CqlStatement::CreateType(_) => Ok(()),
            CqlStatement::DropType(_) => Ok(()),
            CqlStatement::Use(_) => Ok(()),
            CqlStatement::Truncate(_) => Ok(()),
            CqlStatement::Batch(_) => Ok(()),
        }
    }
    
    fn visit_select(&mut self, select: &CqlSelect) -> Result<()> {
        // Check if table exists
        let table_name = select.from.full_name();
        if !self.context.schemas.contains_key(&table_name) {
            if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                self.add_error(format!("Table '{}' does not exist", table_name));
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_select(select)
    }
    
    fn visit_insert(&mut self, insert: &CqlInsert) -> Result<()> {
        // Check if table exists
        let table_name = insert.table.full_name();
        if !self.context.schemas.contains_key(&table_name) {
            if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                self.add_error(format!("Table '{}' does not exist", table_name));
            }
        } else {
            // Validate column count matches values count
            match &insert.values {
                CqlInsertValues::Values(values) => {
                    if insert.columns.len() != values.len() {
                        self.add_error(format!(
                            "Column count ({}) does not match value count ({})",
                            insert.columns.len(),
                            values.len()
                        ));
                    }
                }
                CqlInsertValues::Json(_) => {
                    // JSON values are validated at runtime
                }
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_insert(insert)
    }
    
    fn visit_update(&mut self, update: &CqlUpdate) -> Result<()> {
        // Check if table exists
        let table_name = update.table.full_name();
        if !self.context.schemas.contains_key(&table_name) {
            if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                self.add_error(format!("Table '{}' does not exist", table_name));
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_update(update)
    }
    
    fn visit_delete(&mut self, delete: &CqlDelete) -> Result<()> {
        // Check if table exists
        let table_name = delete.table.full_name();
        if !self.context.schemas.contains_key(&table_name) {
            if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                self.add_error(format!("Table '{}' does not exist", table_name));
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_delete(delete)
    }
    
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<()> {
        // Check for duplicate column names
        let mut column_names = std::collections::HashSet::new();
        for column in &create.columns {
            let name = column.name.as_str();
            if !column_names.insert(name) {
                self.add_error(format!("Duplicate column name: '{}'", name));
            }
        }
        
        // Validate that primary key columns exist
        for pk_column in &create.primary_key.partition_key {
            let name = pk_column.as_str();
            if !create.columns.iter().any(|c| c.name.as_str() == name) {
                self.add_error(format!("Partition key column '{}' not found in column definitions", name));
            }
        }
        
        for ck_column in &create.primary_key.clustering_key {
            let name = ck_column.as_str();
            if !create.columns.iter().any(|c| c.name.as_str() == name) {
                self.add_error(format!("Clustering key column '{}' not found in column definitions", name));
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_create_table(create)
    }
    
    fn visit_drop_table(&mut self, drop: &CqlDropTable) -> Result<()> {
        // Check if table exists (for non-IF EXISTS statements)
        if !drop.if_exists {
            let table_name = drop.table.full_name();
            if !self.context.schemas.contains_key(&table_name) {
                if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                    self.add_error(format!("Table '{}' does not exist", table_name));
                }
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_drop_table(drop)
    }
    
    fn visit_create_index(&mut self, create: &CqlCreateIndex) -> Result<()> {
        // Check if table exists
        let table_name = create.table.full_name();
        if !self.context.schemas.contains_key(&table_name) {
            if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                self.add_error(format!("Table '{}' does not exist", table_name));
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_create_index(create)
    }
    
    fn visit_alter_table(&mut self, alter: &CqlAlterTable) -> Result<()> {
        // Check if table exists
        let table_name = alter.table.full_name();
        if !self.context.schemas.contains_key(&table_name) {
            if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                self.add_error(format!("Table '{}' does not exist", table_name));
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_alter_table(alter)
    }
    
    fn visit_data_type(&mut self, data_type: &CqlDataType) -> Result<()> {
        // Validate UDT references
        if let CqlDataType::Udt(udt_name) = data_type {
            let udt_key = udt_name.as_str();
            if !self.context.udts.contains_key(udt_key) {
                if matches!(self.context.strictness, super::traits::ValidationStrictness::Strict) {
                    self.add_error(format!("UDT '{}' does not exist", udt_key));
                }
            }
        }
        
        // Continue with default traversal
        DefaultVisitor.visit_data_type(data_type)
    }
    
    fn visit_expression(&mut self, expression: &CqlExpression) -> Result<()> {
        // Add expression-specific validation here
        // For now, just traverse
        DefaultVisitor.visit_expression(expression)
    }
    
    fn visit_identifier(&mut self, _identifier: &CqlIdentifier) -> Result<()> {
        // Identifier validation can be added here
        Ok(())
    }
    
    fn visit_literal(&mut self, literal: &CqlLiteral) -> Result<()> {
        // Literal validation can be added here
        DefaultVisitor.visit_literal(literal)
    }
}

/// Visitor that transforms AST nodes
pub struct AstTransformer {
    /// Transformations to apply
    pub transformations: Vec<Box<dyn Fn(&CqlStatement) -> Option<CqlStatement>>>,
}

impl std::fmt::Debug for AstTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AstTransformer")
            .field("transformations", &format!("[{} transformations]", self.transformations.len()))
            .finish()
    }
}

impl AstTransformer {
    /// Create a new AST transformer
    pub fn new() -> Self {
        Self {
            transformations: Vec::new(),
        }
    }
    
    /// Add a transformation function
    pub fn add_transformation<F>(&mut self, transform: F)
    where
        F: Fn(&CqlStatement) -> Option<CqlStatement> + 'static,
    {
        self.transformations.push(Box::new(transform));
    }
    
    /// Apply all transformations to a statement
    pub fn transform(&self, statement: &CqlStatement) -> CqlStatement {
        let mut result = statement.clone();
        
        for transformation in &self.transformations {
            if let Some(transformed) = transformation(&result) {
                result = transformed;
            }
        }
        
        result
    }
}

impl Default for AstTransformer {
    fn default() -> Self {
        Self::new()
    }
}

/// Utility functions for working with visitors
pub mod utils {
    use super::*;
    
    /// Collect all table references in a statement
    pub fn collect_table_references(statement: &CqlStatement) -> Vec<String> {
        let mut tables = Vec::new();
        
        match statement {
            CqlStatement::Select(select) => {
                tables.push(select.from.full_name());
            }
            CqlStatement::Insert(insert) => {
                tables.push(insert.table.full_name());
            }
            CqlStatement::Update(update) => {
                tables.push(update.table.full_name());
            }
            CqlStatement::Delete(delete) => {
                tables.push(delete.table.full_name());
            }
            CqlStatement::CreateTable(create) => {
                tables.push(create.table.full_name());
            }
            CqlStatement::DropTable(drop) => {
                tables.push(drop.table.full_name());
            }
            CqlStatement::CreateIndex(create) => {
                tables.push(create.table.full_name());
            }
            CqlStatement::AlterTable(alter) => {
                tables.push(alter.table.full_name());
            }
            CqlStatement::CreateType(_) => {
                // UDTs don't reference tables
            }
            CqlStatement::DropType(_) => {
                // UDTs don't reference tables
            }
            CqlStatement::Use(_) => {
                // USE statements don't reference tables
            }
            CqlStatement::Truncate(truncate) => {
                tables.push(truncate.table.full_name());
            }
            CqlStatement::Batch(_) => {
                // Batch statements contain other statements, would need recursive handling
            }
        }
        
        tables
    }
    
    /// Check if a statement modifies data
    pub fn is_modifying_statement(statement: &CqlStatement) -> bool {
        matches!(
            statement,
            CqlStatement::Insert(_) |
            CqlStatement::Update(_) |
            CqlStatement::Delete(_) |
            CqlStatement::CreateTable(_) |
            CqlStatement::DropTable(_) |
            CqlStatement::CreateIndex(_) |
            CqlStatement::AlterTable(_)
        )
    }
    
    /// Check if a statement is a data query
    pub fn is_query_statement(statement: &CqlStatement) -> bool {
        matches!(statement, CqlStatement::Select(_))
    }
    
    /// Check if a statement is a schema operation
    pub fn is_schema_statement(statement: &CqlStatement) -> bool {
        matches!(
            statement,
            CqlStatement::CreateTable(_) |
            CqlStatement::DropTable(_) |
            CqlStatement::CreateIndex(_) |
            CqlStatement::AlterTable(_)
        )
    }
}

/// Visitor that converts CQL CREATE TABLE AST to TableSchema
/// 
/// This visitor extracts the business logic from the existing nom parser
/// and converts AST structures to TableSchema objects.
#[derive(Debug, Default)]
pub struct SchemaBuilderVisitor;

impl CqlVisitor<TableSchema> for SchemaBuilderVisitor {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<TableSchema> {
        match statement {
            CqlStatement::CreateTable(create) => self.visit_create_table(create),
            _ => Err(crate::error::Error::invalid_input(
                "SchemaBuilderVisitor only supports CREATE TABLE statements".to_string()
            )),
        }
    }
    
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<TableSchema> {
        // Extract table name and keyspace
        let table_name = create.table.name.as_str().to_string();
        let keyspace = create.table.keyspace
            .as_ref()
            .map(|ks| ks.as_str().to_string())
            .unwrap_or_else(|| "default".to_string());
        
        // Convert partition key columns
        let partition_keys: Result<Vec<KeyColumn>> = create.primary_key.partition_key
            .iter()
            .enumerate()
            .map(|(pos, pk_col)| {
                // Find the column definition for this partition key
                let column_def = create.columns.iter()
                    .find(|col| col.name.as_str() == pk_col.as_str())
                    .ok_or_else(|| crate::error::Error::invalid_input(
                        format!("Partition key column '{}' not found in column definitions", pk_col.as_str())
                    ))?;
                
                Ok(KeyColumn {
                    name: pk_col.as_str().to_string(),
                    data_type: self.convert_cql_data_type_to_string(&column_def.data_type),
                    position: pos,
                })
            })
            .collect();
        
        let partition_keys = partition_keys?;
        
        // Convert clustering key columns
        let clustering_keys: Result<Vec<ClusteringColumn>> = create.primary_key.clustering_key
            .iter()
            .enumerate()
            .map(|(pos, ck_col)| {
                // Find the column definition for this clustering key
                let column_def = create.columns.iter()
                    .find(|col| col.name.as_str() == ck_col.as_str())
                    .ok_or_else(|| crate::error::Error::invalid_input(
                        format!("Clustering key column '{}' not found in column definitions", ck_col.as_str())
                    ))?;
                
                Ok(ClusteringColumn {
                    name: ck_col.as_str().to_string(),
                    data_type: self.convert_cql_data_type_to_string(&column_def.data_type),
                    position: pos,
                    order: "ASC".to_string(), // Default ordering
                })
            })
            .collect();
        
        let clustering_keys = clustering_keys?;
        
        // Convert all columns
        let columns: Vec<Column> = create.columns
            .iter()
            .map(|col_def| {
                Column {
                    name: col_def.name.as_str().to_string(),
                    data_type: self.convert_cql_data_type_to_string(&col_def.data_type),
                    nullable: true, // Default to nullable
                    default: None,
                }
            })
            .collect();
        
        // Build the schema
        Ok(TableSchema {
            keyspace,
            table: table_name,
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
        })
    }
    
    // Default implementations for other visit methods
    fn visit_select(&mut self, _select: &CqlSelect) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support SELECT statements".to_string()
        ))
    }
    
    fn visit_insert(&mut self, _insert: &CqlInsert) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support INSERT statements".to_string()
        ))
    }
    
    fn visit_update(&mut self, _update: &CqlUpdate) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support UPDATE statements".to_string()
        ))
    }
    
    fn visit_delete(&mut self, _delete: &CqlDelete) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support DELETE statements".to_string()
        ))
    }
    
    fn visit_drop_table(&mut self, _drop: &CqlDropTable) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support DROP TABLE statements".to_string()
        ))
    }
    
    fn visit_create_index(&mut self, _create: &CqlCreateIndex) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support CREATE INDEX statements".to_string()
        ))
    }
    
    fn visit_alter_table(&mut self, _alter: &CqlAlterTable) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support ALTER TABLE statements".to_string()
        ))
    }
    
    fn visit_data_type(&mut self, _data_type: &CqlDataType) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support standalone data types".to_string()
        ))
    }
    
    fn visit_expression(&mut self, _expression: &CqlExpression) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support expressions".to_string()
        ))
    }
    
    fn visit_identifier(&mut self, _identifier: &CqlIdentifier) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support identifiers".to_string()
        ))
    }
    
    fn visit_literal(&mut self, _literal: &CqlLiteral) -> Result<TableSchema> {
        Err(crate::error::Error::invalid_input(
            "SchemaBuilderVisitor does not support literals".to_string()
        ))
    }
}

impl SchemaBuilderVisitor {
    /// Convert CqlDataType to string representation compatible with existing schema format
    fn convert_cql_data_type_to_string(&self, data_type: &CqlDataType) -> String {
        match data_type {
            // Primitive types
            CqlDataType::Boolean => "boolean".to_string(),
            CqlDataType::TinyInt => "tinyint".to_string(),
            CqlDataType::SmallInt => "smallint".to_string(),
            CqlDataType::Int => "int".to_string(),
            CqlDataType::BigInt => "bigint".to_string(),
            CqlDataType::Varint => "varint".to_string(),
            CqlDataType::Decimal => "decimal".to_string(),
            CqlDataType::Float => "float".to_string(),
            CqlDataType::Double => "double".to_string(),
            CqlDataType::Text => "text".to_string(),
            CqlDataType::Ascii => "ascii".to_string(),
            CqlDataType::Varchar => "varchar".to_string(),
            CqlDataType::Blob => "blob".to_string(),
            CqlDataType::Timestamp => "timestamp".to_string(),
            CqlDataType::Date => "date".to_string(),
            CqlDataType::Time => "time".to_string(),
            CqlDataType::Uuid => "uuid".to_string(),
            CqlDataType::TimeUuid => "timeuuid".to_string(),
            CqlDataType::Inet => "inet".to_string(),
            CqlDataType::Duration => "duration".to_string(),
            CqlDataType::Counter => "counter".to_string(),
            
            // Collection types
            CqlDataType::List(inner) => {
                format!("list<{}>", self.convert_cql_data_type_to_string(inner))
            }
            CqlDataType::Set(inner) => {
                format!("set<{}>", self.convert_cql_data_type_to_string(inner))
            }
            CqlDataType::Map(key, value) => {
                format!("map<{}, {}>", 
                    self.convert_cql_data_type_to_string(key),
                    self.convert_cql_data_type_to_string(value)
                )
            }
            
            // Complex types
            CqlDataType::Tuple(types) => {
                let type_strs: Vec<String> = types.iter()
                    .map(|t| self.convert_cql_data_type_to_string(t))
                    .collect();
                format!("tuple<{}>", type_strs.join(", "))
            }
            CqlDataType::Udt(name) => name.as_str().to_string(),
            CqlDataType::Frozen(inner) => {
                format!("frozen<{}>", self.convert_cql_data_type_to_string(inner))
            }
            
            // Custom type
            CqlDataType::Custom(name) => name.clone(),
        }
    }
}

/// ValidationVisitor for AST validation
/// 
/// This visitor performs semantic validation of AST nodes beyond syntactic correctness.
#[derive(Debug, Default)]
pub struct ValidationVisitor {
    pub errors: Vec<String>,
}

impl ValidationVisitor {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
        }
    }
    
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
    
    pub fn get_errors(&self) -> &[String] {
        &self.errors
    }
    
    fn add_error(&mut self, error: String) {
        self.errors.push(error);
    }
}

impl CqlVisitor<()> for ValidationVisitor {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<()> {
        match statement {
            CqlStatement::CreateTable(create) => self.visit_create_table(create),
            CqlStatement::Select(select) => self.visit_select(select),
            CqlStatement::Insert(insert) => self.visit_insert(insert),
            CqlStatement::Update(update) => self.visit_update(update),
            CqlStatement::Delete(delete) => self.visit_delete(delete),
            CqlStatement::DropTable(drop) => self.visit_drop_table(drop),
            CqlStatement::CreateIndex(create) => self.visit_create_index(create),
            CqlStatement::AlterTable(alter) => self.visit_alter_table(alter),
            _ => Ok(()), // Other statements not validated
        }
    }
    
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<()> {
        // Validate table name
        if create.table.name.as_str().is_empty() {
            self.add_error("Table name cannot be empty".to_string());
        }
        
        // Validate partition keys exist in columns
        for pk_col in &create.primary_key.partition_key {
            if !create.columns.iter().any(|col| col.name.as_str() == pk_col.as_str()) {
                self.add_error(format!(
                    "Partition key column '{}' not found in column definitions", 
                    pk_col.as_str()
                ));
            }
        }
        
        // Validate clustering keys exist in columns
        for ck_col in &create.primary_key.clustering_key {
            if !create.columns.iter().any(|col| col.name.as_str() == ck_col.as_str()) {
                self.add_error(format!(
                    "Clustering key column '{}' not found in column definitions", 
                    ck_col.as_str()
                ));
            }
        }
        
        // Validate no duplicate column names
        let mut column_names = std::collections::HashSet::new();
        for column in &create.columns {
            let name = column.name.as_str();
            if !column_names.insert(name) {
                self.add_error(format!("Duplicate column name: '{}'", name));
            }
        }
        
        // Validate primary key is not empty
        if create.primary_key.partition_key.is_empty() {
            self.add_error("Table must have at least one partition key column".to_string());
        }
        
        Ok(())
    }
    
    // Default implementations that perform basic validation
    fn visit_select(&mut self, _select: &CqlSelect) -> Result<()> {
        // Basic SELECT validation could be added here
        Ok(())
    }
    
    fn visit_insert(&mut self, _insert: &CqlInsert) -> Result<()> {
        // Basic INSERT validation could be added here
        Ok(())
    }
    
    fn visit_update(&mut self, _update: &CqlUpdate) -> Result<()> {
        // Basic UPDATE validation could be added here
        Ok(())
    }
    
    fn visit_delete(&mut self, _delete: &CqlDelete) -> Result<()> {
        // Basic DELETE validation could be added here
        Ok(())
    }
    
    fn visit_drop_table(&mut self, _drop: &CqlDropTable) -> Result<()> {
        // Validate table name
        if _drop.table.name.as_str().is_empty() {
            self.add_error("Table name cannot be empty".to_string());
        }
        Ok(())
    }
    
    fn visit_create_index(&mut self, _create: &CqlCreateIndex) -> Result<()> {
        // Basic CREATE INDEX validation could be added here
        Ok(())
    }
    
    fn visit_alter_table(&mut self, _alter: &CqlAlterTable) -> Result<()> {
        // Basic ALTER TABLE validation could be added here
        Ok(())
    }
    
    fn visit_data_type(&mut self, _data_type: &CqlDataType) -> Result<()> {
        Ok(())
    }
    
    fn visit_expression(&mut self, _expression: &CqlExpression) -> Result<()> {
        Ok(())
    }
    
    fn visit_identifier(&mut self, _identifier: &CqlIdentifier) -> Result<()> {
        Ok(())
    }
    
    fn visit_literal(&mut self, _literal: &CqlLiteral) -> Result<()> {
        Ok(())
    }
}

/// TypeCollectorVisitor for collecting type information from AST
/// 
/// This visitor extracts all data types used in a statement for analysis.
#[derive(Debug, Default)]
pub struct TypeCollectorVisitor {
    pub types: Vec<CqlDataType>,
}

impl TypeCollectorVisitor {
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
        }
    }
    
    pub fn into_types(self) -> Vec<CqlDataType> {
        self.types
    }
    
    fn collect_type(&mut self, data_type: &CqlDataType) {
        self.types.push(data_type.clone());
        
        // Recursively collect nested types
        match data_type {
            CqlDataType::List(inner) | CqlDataType::Set(inner) | CqlDataType::Frozen(inner) => {
                self.collect_type(inner);
            }
            CqlDataType::Map(key, value) => {
                self.collect_type(key);
                self.collect_type(value);
            }
            CqlDataType::Tuple(types) => {
                for t in types {
                    self.collect_type(t);
                }
            }
            _ => {} // Primitive and UDT types have no nested types
        }
    }
}

impl CqlVisitor<()> for TypeCollectorVisitor {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<()> {
        match statement {
            CqlStatement::CreateTable(create) => self.visit_create_table(create),
            _ => Ok(()), // Other statements may not have explicit type definitions
        }
    }
    
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<()> {
        // Collect types from all column definitions
        for column in &create.columns {
            self.collect_type(&column.data_type);
        }
        Ok(())
    }
    
    fn visit_select(&mut self, _select: &CqlSelect) -> Result<()> {
        Ok(())
    }
    
    fn visit_insert(&mut self, _insert: &CqlInsert) -> Result<()> {
        Ok(())
    }
    
    fn visit_update(&mut self, _update: &CqlUpdate) -> Result<()> {
        Ok(())
    }
    
    fn visit_delete(&mut self, _delete: &CqlDelete) -> Result<()> {
        Ok(())
    }
    
    fn visit_drop_table(&mut self, _drop: &CqlDropTable) -> Result<()> {
        Ok(())
    }
    
    fn visit_create_index(&mut self, _create: &CqlCreateIndex) -> Result<()> {
        Ok(())
    }
    
    fn visit_alter_table(&mut self, alter: &CqlAlterTable) -> Result<()> {
        // Collect types from ALTER TABLE operations
        match &alter.operation {
            CqlAlterTableOp::AddColumn(column_def) => {
                self.collect_type(&column_def.data_type);
            }
            CqlAlterTableOp::AlterColumn { new_type, .. } => {
                self.collect_type(new_type);
            }
            _ => {} // Other operations don't involve type definitions
        }
        Ok(())
    }
    
    fn visit_data_type(&mut self, data_type: &CqlDataType) -> Result<()> {
        self.collect_type(data_type);
        Ok(())
    }
    
    fn visit_expression(&mut self, _expression: &CqlExpression) -> Result<()> {
        Ok(())
    }
    
    fn visit_identifier(&mut self, _identifier: &CqlIdentifier) -> Result<()> {
        Ok(())
    }
    
    fn visit_literal(&mut self, _literal: &CqlLiteral) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_identifier_collector() {
        let statement = CqlStatement::Select(CqlSelect {
            distinct: false,
            select_list: vec![
                CqlSelectItem::Expression {
                    expression: CqlExpression::Column(CqlIdentifier::new("id")),
                    alias: None,
                },
                CqlSelectItem::Expression {
                    expression: CqlExpression::Column(CqlIdentifier::new("name")),
                    alias: None,
                },
            ],
            from: CqlTable::new("users"),
            where_clause: Some(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Parameter(1)),
            }),
            order_by: None,
            limit: None,
            allow_filtering: false,
        });
        
        let mut collector = IdentifierCollector::default();
        collector.visit_statement(&statement).unwrap();
        
        // Should collect: id, name, users, id (from WHERE clause)
        assert_eq!(collector.identifiers.len(), 4);
        assert_eq!(collector.identifiers[0].as_str(), "id");
        assert_eq!(collector.identifiers[1].as_str(), "name");
        assert_eq!(collector.identifiers[2].as_str(), "users");
        assert_eq!(collector.identifiers[3].as_str(), "id");
    }
    
    #[test]
    fn test_semantic_validator() {
        let statement = CqlStatement::Insert(CqlInsert {
            table: CqlTable::new("users"),
            columns: vec![
                CqlIdentifier::new("id"),
                CqlIdentifier::new("name"),
            ],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Parameter(1),
                // Missing second value - should cause validation error
            ]),
            if_not_exists: false,
            using: None,
        });
        
        let context = ValidationContext::new();
        let mut validator = SemanticValidator::new(context);
        validator.visit_statement(&statement).unwrap();
        
        // Should have validation errors
        assert!(!validator.is_valid());
        assert!(validator.get_errors().len() > 0);
    }
    
    #[test]
    fn test_utils() {
        let statement = CqlStatement::Select(CqlSelect {
            distinct: false,
            select_list: vec![CqlSelectItem::Wildcard],
            from: CqlTable::with_keyspace("test", "users"),
            where_clause: None,
            order_by: None,
            limit: None,
            allow_filtering: false,
        });
        
        let tables = utils::collect_table_references(&statement);
        assert_eq!(tables, vec!["test.users"]);
        
        assert!(utils::is_query_statement(&statement));
        assert!(!utils::is_modifying_statement(&statement));
        assert!(!utils::is_schema_statement(&statement));
    }
    
    #[test]
    fn test_schema_builder_visitor() {
        // Create a sample CREATE TABLE AST
        let create_table = CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::with_keyspace("test_keyspace", "users"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("id"),
                    data_type: CqlDataType::Uuid,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("name"),
                    data_type: CqlDataType::Text,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("age"),
                    data_type: CqlDataType::Int,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("tags"),
                    data_type: CqlDataType::List(Box::new(CqlDataType::Text)),
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("id")],
                clustering_key: vec![CqlIdentifier::new("name")],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        };
        
        let statement = CqlStatement::CreateTable(create_table);
        let mut visitor = SchemaBuilderVisitor;
        let schema = visitor.visit_statement(&statement).unwrap();
        
        // Verify the schema was correctly built
        assert_eq!(schema.keyspace, "test_keyspace");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.partition_keys[0].name, "id");
        assert_eq!(schema.partition_keys[0].data_type, "uuid");
        assert_eq!(schema.clustering_keys.len(), 1);
        assert_eq!(schema.clustering_keys[0].name, "name");
        assert_eq!(schema.clustering_keys[0].data_type, "text");
        assert_eq!(schema.columns.len(), 4);
        
        // Check that list type was correctly converted
        let tags_column = schema.columns.iter()
            .find(|col| col.name == "tags")
            .expect("tags column should exist");
        assert_eq!(tags_column.data_type, "list<text>");
    }
    
    #[test]
    fn test_validation_visitor() {
        // Create a CREATE TABLE AST with validation errors
        let create_table = CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::new("test_table"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("id"),
                    data_type: CqlDataType::Uuid,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("name"),
                    data_type: CqlDataType::Text,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("name"), // Duplicate column name
                    data_type: CqlDataType::Int,
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("missing_column")], // Column doesn't exist
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        };
        
        let statement = CqlStatement::CreateTable(create_table);
        let mut visitor = ValidationVisitor::new();
        let _ = visitor.visit_statement(&statement);
        
        // Should have validation errors
        assert!(visitor.has_errors());
        let errors = visitor.get_errors();
        assert!(errors.iter().any(|e| e.contains("Duplicate column name")));
        assert!(errors.iter().any(|e| e.contains("not found in column definitions")));
    }
    
    #[test]
    fn test_type_collector_visitor() {
        // Create a CREATE TABLE AST with various types
        let create_table = CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::new("test_table"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("simple"),
                    data_type: CqlDataType::Text,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("list_col"),
                    data_type: CqlDataType::List(Box::new(CqlDataType::Int)),
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("map_col"),
                    data_type: CqlDataType::Map(
                        Box::new(CqlDataType::Text),
                        Box::new(CqlDataType::Uuid)
                    ),
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("frozen_col"),
                    data_type: CqlDataType::Frozen(Box::new(CqlDataType::Set(Box::new(CqlDataType::BigInt)))),
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("simple")],
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        };
        
        let statement = CqlStatement::CreateTable(create_table);
        let mut visitor = TypeCollectorVisitor::new();
        let _ = visitor.visit_statement(&statement);
        
        let types = visitor.into_types();
        
        // Should collect all types including nested ones
        assert!(types.iter().any(|t| matches!(t, CqlDataType::Text)));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::List(_))));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::Int)));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::Map(_, _))));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::Uuid)));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::Frozen(_))));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::Set(_))));
        assert!(types.iter().any(|t| matches!(t, CqlDataType::BigInt)));
    }
}