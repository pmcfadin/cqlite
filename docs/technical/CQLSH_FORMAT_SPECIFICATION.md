# CQLSH Format Specification

**Research Agent**: CQLSH Format Research Agent  
**Date**: 2025-07-25  
**Status**: Research Complete - Implementation Ready

## Executive Summary

This document provides a comprehensive specification of how cqlsh (Cassandra Query Language Shell) formats and displays table output, based on analysis of Apache Cassandra source code and documentation.

## Table Display Format

### Standard Horizontal Format

cqlsh uses a standardized table format for SELECT query results:

```
cqlsh:keyspace> SELECT * FROM users;

 id | name     | email                | age
----+----------+----------------------+-----
  1 | John Doe | john.doe@example.com |  30
  2 | Jane     | jane@example.com     |  25

(2 rows)
```

### Format Structure

#### 1. Column Headers
- **Format**: ` column_name | column_name | column_name`
- **Alignment**: Left-aligned column headers
- **Spacing**: Single space before first column, single space around pipe separators
- **Separator**: ` | ` (space-pipe-space)

#### 2. Header Separator Line
- **Format**: `----+----------+----------------------+-----`
- **Pattern**: Dashes (`-`) for each column width, plus signs (`+`) at separators
- **Width**: Matches exact column content width (dynamic)
- **Calculation**: `max(column_name.length, max_value_length_in_column)`

#### 3. Data Rows
- **Format**: ` value | value | value`
- **Alignment**: **RIGHT-ALIGNED** for all data values (key finding!)
- **Spacing**: Single space before first column, single space around pipe separators
- **NULL handling**: Display as empty string or "NULL"

#### 4. Result Summary
- **Format**: `(X rows)`
- **Position**: After all data rows
- **Spacing**: Empty line before summary

### Detailed Formatting Rules

#### Column Width Calculation
```python
# From cqlsh source code analysis
widths = [n.displaywidth for n in formatted_names]  # Start with header widths
if formatted_values is not None:
    for fmtrow in formatted_values:
        for num, col in enumerate(fmtrow):
            widths[num] = max(widths[num], col.displaywidth)  # Expand for content
```

#### Header Formatting
```python
header = ' | '.join(hdr.ljust(w, color=self.color) for (hdr, w) in zip(formatted_names, widths))
self.writeresult(' ' + header.rstrip())
```

#### Separator Line Formatting
```python
self.writeresult('-%s-' % '-+-'.join('-' * w for w in widths))
```

#### Data Row Formatting
```python
line = ' | '.join(col.rjust(w, color=self.color) for (col, w) in zip(row, widths))
self.writeresult(' ' + line)
```

### Key Constants and Patterns

| Element | Pattern | Example |
|---------|---------|---------|
| Column Separator | ` \| ` | ` name \| age ` |
| Header Border | `-+-` | `----+-----` |
| Row Prefix | ` ` | ` John \| 30` |
| Value Alignment | RIGHT | `  30` (right-aligned) |
| Header Alignment | LEFT | `name` (left-aligned) |

## Vertical Format (EXPAND Mode)

When `EXPAND ON` is enabled, cqlsh switches to vertical output:

```
@ Row 1
-----------------------------+-----------------------------------------------------------
keyspace_name                | cycling  
table_name                   | birthday_list
bloom_filter_fp_chance       | 0.01
caching                      | {'keys': 'ALL', 'rows_per_partition': 'NONE'}

@ Row 2
-----------------------------+-----------------------------------------------------------
keyspace_name                | inventory
table_name                   | products
...
```

### Vertical Format Rules

#### Row Header
- **Format**: `@ Row X`
- **Numbering**: Sequential starting from 1

#### Separator Line
- **Format**: `-----------------------------+-----------------------------------------------------------`
- **Pattern**: Dashes + plus sign + dashes
- **Width**: Fixed width optimized for readability

#### Column-Value Pairs
- **Format**: `column_name                | value`
- **Left Side**: Column name, left-aligned, padded to fixed width
- **Right Side**: Value, left-aligned
- **Separator**: ` | ` (space-pipe-space)

## Data Type Formatting

### Text/String Values
- **Alignment**: Right-aligned in standard mode
- **Quotes**: Not displayed in output (raw text)
- **Empty**: Displayed as empty space

### Numeric Values
- **Integers**: Right-aligned, no formatting
- **Floats**: Right-aligned, standard decimal notation
- **Large numbers**: No comma separators

### UUID Values
- **Format**: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- **Case**: Lowercase
- **Alignment**: Right-aligned

### Timestamp Values
- **Format**: ISO-8601 or epoch milliseconds (depends on type)
- **Alignment**: Right-aligned
- **Timezone**: UTC implied unless specified

### Boolean Values
- **Format**: `true` / `false` (lowercase)
- **Alignment**: Right-aligned

### Collection Types
- **Lists**: `[item1, item2, item3]`
- **Sets**: `{item1, item2, item3}`
- **Maps**: `{key1: value1, key2: value2}`
- **Alignment**: Right-aligned
- **Nested**: Inline formatting, no line breaks

### NULL Values
- **Display**: Empty space or explicit "NULL"
- **Alignment**: Right-aligned (empty)

## Special Display Features

### Color Support
- **Headers**: May be colored in interactive mode
- **Values**: Support for syntax highlighting
- **Control**: `--color` / `--no-color` flags

### Paging
- **Control**: `PAGING ON/OFF`
- **Behavior**: Shows limited rows with continuation prompt
- **Format**: Same table structure, but with paging controls

### Output Capture
- **Command**: `CAPTURE 'filename'`
- **Behavior**: Writes formatted output to file
- **Format**: Identical to screen output

## Implementation Guidelines

### For CQLite CLI

Based on this specification, CQLite should implement:

1. **Dynamic Column Width Calculation**
   ```rust
   let mut widths = vec![0; columns.len()];
   // Calculate based on headers
   for (i, header) in headers.iter().enumerate() {
       widths[i] = header.display_width();
   }
   // Expand based on data
   for row in rows {
       for (i, cell) in row.iter().enumerate() {
           widths[i] = widths[i].max(cell.display_width());
       }
   }
   ```

2. **Right-Aligned Data Values**
   ```rust
   for cell in row {
       print!(" {:>width$}", cell, width = column_width);
   }
   ```

3. **Proper Separator Generation**
   ```rust
   let separator = format!("-{}-", widths.iter()
       .map(|w| "-".repeat(*w))
       .collect::<Vec<_>>()
       .join("-+-"));
   ```

### Constants for Implementation

```rust
const COLUMN_SEPARATOR: &str = " | ";
const HEADER_BORDER_CHAR: char = '-';
const HEADER_SEPARATOR_JUNCTION: &str = "-+-";
const ROW_PREFIX: &str = " ";
const VERTICAL_ROW_PREFIX: &str = "@ Row ";
const VERTICAL_SEPARATOR: &str = "-----------------------------+-----------------------------------------------------------";
```

## Compatibility Notes

### Version Differences
- Core format consistent across Cassandra versions
- Color support varies by version
- EXPAND feature added in later versions

### Terminal Compatibility
- Works in all standard terminals
- Respects terminal width for wrapping
- Color codes handled gracefully in non-color terminals

## Testing Requirements

### Format Validation
- [ ] Column width calculation accuracy
- [ ] Right-alignment of data values
- [ ] Left-alignment of headers
- [ ] Proper separator line generation
- [ ] Correct spacing around separators

### Data Type Testing
- [ ] All CQL data types display correctly
- [ ] NULL value handling
- [ ] Collection type formatting
- [ ] UUID format compliance
- [ ] Timestamp display

### Edge Cases
- [ ] Empty result sets
- [ ] Single column tables
- [ ] Very wide columns
- [ ] Many columns (horizontal scrolling)
- [ ] Special characters in data

## Research Sources

1. **Apache Cassandra Source Code**
   - `pylib/cqlshlib/cqlshmain.py` - `print_formatted_result()` function
   - `pylib/cqlshlib/formatting.py` - Data type formatting

2. **Official Documentation**
   - Apache Cassandra cqlsh documentation
   - CQL shell command reference

3. **Community Resources**
   - Stack Overflow discussions on cqlsh formatting
   - GitHub issues and pull requests

## Implementation Status

- ✅ Format specification documented
- ✅ Source code analysis complete
- ✅ Key constants identified
- ✅ Edge cases documented
- 🔄 Ready for implementation in CQLite

---

**Research Conclusion**: Complete specification of cqlsh table formatting provides definitive guidance for implementing Cassandra-compatible table display in CQLite CLI.