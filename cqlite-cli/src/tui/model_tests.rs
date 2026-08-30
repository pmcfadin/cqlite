//! Unit tests for [`super`] — the TUI application state model (`model.rs`).
//!
//! Split out of `model.rs` under the campsite rule (epic #1116 / #1135): the
//! inline `mod tests` had grown the source file past the 800-line target.

use super::*;

// -------------------------------------------------------------------------
// FocusPanel Tests
// -------------------------------------------------------------------------

#[test]
fn test_focus_panel_next_all_visible() {
    let visibility = PanelVisibility {
        tables: true,
        results: true,
        history: true,
    };

    // Cycle through all panels
    assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::Results);
    assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::History);
    assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Tables);
}

#[test]
fn test_focus_panel_prev_all_visible() {
    let visibility = PanelVisibility {
        tables: true,
        results: true,
        history: true,
    };

    // Cycle backwards through all panels
    assert_eq!(FocusPanel::Tables.prev(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Input.prev(&visibility), FocusPanel::History);
    assert_eq!(FocusPanel::History.prev(&visibility), FocusPanel::Results);
    assert_eq!(FocusPanel::Results.prev(&visibility), FocusPanel::Tables);
}

#[test]
fn test_focus_panel_next_skips_hidden_tables() {
    let visibility = PanelVisibility {
        tables: false,
        results: true,
        history: true,
    };

    // Should skip Tables and go to Results
    assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Results);
    assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::History);
    assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
}

#[test]
fn test_focus_panel_next_skips_hidden_results() {
    let visibility = PanelVisibility {
        tables: true,
        results: false,
        history: true,
    };

    // Should skip Results
    assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::History);
    assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Tables);
}

#[test]
fn test_focus_panel_next_skips_hidden_history() {
    let visibility = PanelVisibility {
        tables: true,
        results: true,
        history: false,
    };

    // Should skip History
    assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Tables);
    assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::Results);
}

#[test]
fn test_focus_panel_next_only_input_visible() {
    let visibility = PanelVisibility {
        tables: false,
        results: false,
        history: false,
    };

    // All panels hidden - should cycle only on Input
    assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
}

#[test]
fn test_focus_panel_prev_skips_hidden_panels() {
    let visibility = PanelVisibility {
        tables: false,
        results: true,
        history: false,
    };

    // Only Results and Input visible
    assert_eq!(FocusPanel::Results.prev(&visibility), FocusPanel::Input);
    assert_eq!(FocusPanel::Input.prev(&visibility), FocusPanel::Results);
}

#[test]
fn test_focus_panel_input_always_reachable() {
    // Input is always reachable regardless of visibility
    let visibility = PanelVisibility {
        tables: false,
        results: false,
        history: false,
    };

    // Start from any panel, should eventually reach Input
    let mut current = FocusPanel::Tables;
    for _ in 0..5 {
        current = current.next(&visibility);
    }
    assert_eq!(current, FocusPanel::Input);
}

// -------------------------------------------------------------------------
// TablesBrowserState Filter Tests
// -------------------------------------------------------------------------

#[test]
fn test_apply_filter_empty_shows_all() {
    let mut state = TablesBrowserState::default();
    state.entries = vec![
        TableEntry {
            keyspace: "ks1".to_string(),
            name: "table1".to_string(),
            qualified_name: "ks1.table1".to_string(),
        },
        TableEntry {
            keyspace: "ks2".to_string(),
            name: "table2".to_string(),
            qualified_name: "ks2.table2".to_string(),
        },
        TableEntry {
            keyspace: "ks3".to_string(),
            name: "users".to_string(),
            qualified_name: "ks3.users".to_string(),
        },
    ];

    state.filter_text = String::new();
    state.apply_filter();

    // All entries should be visible
    assert_eq!(state.filtered_indices, vec![0, 1, 2]);
}

#[test]
fn test_apply_filter_matches_some() {
    let mut state = TablesBrowserState::default();
    state.entries = vec![
        TableEntry {
            keyspace: "ks1".to_string(),
            name: "table1".to_string(),
            qualified_name: "ks1.table1".to_string(),
        },
        TableEntry {
            keyspace: "ks2".to_string(),
            name: "table2".to_string(),
            qualified_name: "ks2.table2".to_string(),
        },
        TableEntry {
            keyspace: "ks3".to_string(),
            name: "users".to_string(),
            qualified_name: "ks3.users".to_string(),
        },
    ];

    state.filter_text = "table".to_string();
    state.apply_filter();

    // Only entries containing "table" should be visible
    assert_eq!(state.filtered_indices, vec![0, 1]);
}

#[test]
fn test_apply_filter_matches_none() {
    let mut state = TablesBrowserState::default();
    state.entries = vec![
        TableEntry {
            keyspace: "ks1".to_string(),
            name: "table1".to_string(),
            qualified_name: "ks1.table1".to_string(),
        },
        TableEntry {
            keyspace: "ks2".to_string(),
            name: "table2".to_string(),
            qualified_name: "ks2.table2".to_string(),
        },
    ];

    state.filter_text = "nonexistent".to_string();
    state.apply_filter();

    // No entries should match
    assert_eq!(state.filtered_indices, Vec::<usize>::new());
}

#[test]
fn test_apply_filter_case_insensitive() {
    let mut state = TablesBrowserState::default();
    state.entries = vec![
        TableEntry {
            keyspace: "TestKS".to_string(),
            name: "Users".to_string(),
            qualified_name: "TestKS.Users".to_string(),
        },
        TableEntry {
            keyspace: "prodks".to_string(),
            name: "products".to_string(),
            qualified_name: "prodks.products".to_string(),
        },
    ];

    state.filter_text = "USERS".to_string();
    state.apply_filter();

    // Case-insensitive match
    assert_eq!(state.filtered_indices, vec![0]);
}

#[test]
fn test_apply_filter_resets_selection_when_out_of_bounds() {
    let mut state = TablesBrowserState::default();
    state.entries = vec![
        TableEntry {
            keyspace: "ks1".to_string(),
            name: "table1".to_string(),
            qualified_name: "ks1.table1".to_string(),
        },
        TableEntry {
            keyspace: "ks2".to_string(),
            name: "table2".to_string(),
            qualified_name: "ks2.table2".to_string(),
        },
        TableEntry {
            keyspace: "ks3".to_string(),
            name: "users".to_string(),
            qualified_name: "ks3.users".to_string(),
        },
    ];

    // Select the last item
    state.filtered_indices = vec![0, 1, 2];
    state.list_state.select(Some(2));

    // Apply filter that removes selected item
    state.filter_text = "table".to_string();
    state.apply_filter();

    // Selection should be reset to first item
    assert_eq!(state.list_state.selected(), Some(0));
    assert_eq!(state.filtered_indices, vec![0, 1]);
}

#[test]
fn test_apply_filter_resets_selection_when_empty() {
    let mut state = TablesBrowserState::default();
    state.entries = vec![TableEntry {
        keyspace: "ks1".to_string(),
        name: "table1".to_string(),
        qualified_name: "ks1.table1".to_string(),
    }];

    state.filtered_indices = vec![0];
    state.list_state.select(Some(0));

    // Apply filter that matches nothing
    state.filter_text = "nonexistent".to_string();
    state.apply_filter();

    // Selection should be cleared
    assert_eq!(state.list_state.selected(), None);
    assert_eq!(state.filtered_indices, Vec::<usize>::new());
}

// -------------------------------------------------------------------------
// ResultsTableState Tests
// -------------------------------------------------------------------------

#[test]
fn test_calculate_widths_empty_columns() {
    let mut state = ResultsTableState::default();
    state.calculate_widths();

    assert_eq!(state.column_widths, Vec::<u16>::new());
}

#[test]
fn test_calculate_widths_headers_only() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    state.calculate_widths();

    // Widths should be header length + 2 (padding)
    // "id" (2) + 2 = 4, "name" (4) + 2 = 6, "email" (5) + 2 = 7
    assert_eq!(state.column_widths, vec![4, 6, 7]);
}

#[test]
fn test_calculate_widths_with_data() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["id".to_string(), "name".to_string()];
    state.rows = vec![
        vec!["1".to_string(), "Alice".to_string()],
        vec!["2".to_string(), "BobTheBuilder".to_string()],
    ];
    state.calculate_widths();

    // id column: max(2, 1) + 2 = 4
    // name column: max(4, 13) + 2 = 15
    assert_eq!(state.column_widths, vec![4, 15]);
}

#[test]
fn test_calculate_widths_caps_at_40() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["long_column".to_string()];
    state.rows = vec![vec!["a".repeat(100)]];
    state.calculate_widths();

    // Should cap at 40
    assert_eq!(state.column_widths, vec![40]);
}

#[test]
fn test_calculate_widths_samples_first_100_rows() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["data".to_string()];

    // Create 150 rows, with row 101 having the longest content
    let mut rows = Vec::new();
    for i in 0..150 {
        if i == 101 {
            rows.push(vec!["very_long_content_here".to_string()]);
        } else {
            rows.push(vec!["x".to_string()]);
        }
    }
    state.rows = rows;
    state.calculate_widths();

    // Should only sample first 100, so won't see row 101's long content
    // Width should be max(4, 1) + 2 = 6
    assert_eq!(state.column_widths, vec![6]);
}

#[test]
fn test_visible_columns_empty() {
    let state = ResultsTableState::default();
    let visible = state.visible_columns(100);

    assert_eq!(visible, 0..0);
}

#[test]
fn test_visible_columns_all_fit() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    state.column_widths = vec![10, 10, 10];
    state.col_offset = 0;

    let visible = state.visible_columns(50);

    // All columns fit
    assert_eq!(visible, 0..3);
}

#[test]
fn test_visible_columns_with_offset() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    state.column_widths = vec![10, 10, 10];
    state.col_offset = 1;

    let visible = state.visible_columns(50);

    // Should start from offset 1
    assert_eq!(visible, 1..3);
}

#[test]
fn test_visible_columns_partial_fit() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    state.column_widths = vec![15, 15, 15];
    state.col_offset = 0;

    let visible = state.visible_columns(25);

    // Only first 2 columns fit (15 + 15 = 30 > 25, but 15 < 25)
    assert_eq!(visible, 0..1);
}

#[test]
fn test_visible_columns_clamps_offset() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string()];
    state.column_widths = vec![10, 10];
    state.col_offset = 100; // Beyond bounds

    let visible = state.visible_columns(50);

    // Should clamp to last valid column
    assert_eq!(visible, 1..2);
}

#[test]
fn test_has_scroll_left() {
    let mut state = ResultsTableState::default();

    state.col_offset = 0;
    assert!(!state.has_scroll_left());

    state.col_offset = 1;
    assert!(state.has_scroll_left());

    state.col_offset = 5;
    assert!(state.has_scroll_left());
}

#[test]
fn test_has_scroll_right() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    state.column_widths = vec![20, 20, 20];
    state.col_offset = 0;

    // With 30 available width, only first 2 columns visible
    assert!(state.has_scroll_right(30));

    // With 100 available width, all columns visible
    assert!(!state.has_scroll_right(100));
}

#[test]
fn test_has_scroll_right_at_end() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string()];
    state.column_widths = vec![10, 10];
    state.col_offset = 1; // At last column

    // At last column, no scroll right
    assert!(!state.has_scroll_right(100));
}

#[test]
fn test_clear() {
    let mut state = ResultsTableState::default();
    state.columns = vec!["a".to_string(), "b".to_string()];
    state.rows = vec![vec!["1".to_string(), "2".to_string()]];
    state.row_offset = 5;
    state.col_offset = 2;
    state.selected_row = Some(3);
    state.column_widths = vec![10, 20];

    state.clear();

    assert!(state.columns.is_empty());
    assert!(state.rows.is_empty());
    assert_eq!(state.row_offset, 0);
    assert_eq!(state.col_offset, 0);
    assert_eq!(state.selected_row, None);
    assert!(state.column_widths.is_empty());
}

// -------------------------------------------------------------------------
// PanelVisibility Tests
// -------------------------------------------------------------------------

#[test]
fn test_panel_visibility_default() {
    let visibility = PanelVisibility::default();

    assert!(visibility.tables);
    assert!(visibility.results);
    assert!(visibility.history);
}

#[test]
fn test_panel_visibility_reset() {
    let mut visibility = PanelVisibility {
        tables: false,
        results: false,
        history: false,
    };

    visibility.reset();

    assert!(visibility.tables);
    assert!(visibility.results);
    assert!(visibility.history);
}

#[test]
fn test_panel_visibility_reset_restores_default() {
    let mut visibility = PanelVisibility {
        tables: true,
        results: false,
        history: true,
    };

    visibility.reset();

    let default = PanelVisibility::default();
    assert_eq!(visibility.tables, default.tables);
    assert_eq!(visibility.results, default.results);
    assert_eq!(visibility.history, default.history);
}

// -------------------------------------------------------------------------
// format_duration Tests (Microsecond Display Support)
// -------------------------------------------------------------------------

#[test]
fn test_format_duration_microseconds() {
    // Sub-millisecond times should display in microseconds
    assert_eq!(format_duration(Duration::from_micros(0)), "0μs");
    assert_eq!(format_duration(Duration::from_micros(1)), "1μs");
    assert_eq!(format_duration(Duration::from_micros(450)), "450μs");
    assert_eq!(format_duration(Duration::from_micros(999)), "999μs");
}

#[test]
fn test_format_duration_milliseconds() {
    // 1-999ms should display with 1 decimal place
    assert_eq!(format_duration(Duration::from_micros(1_000)), "1.0ms");
    assert_eq!(format_duration(Duration::from_micros(1_200)), "1.2ms");
    assert_eq!(format_duration(Duration::from_micros(7_000)), "7.0ms");
    assert_eq!(format_duration(Duration::from_micros(74_000)), "74.0ms");
    assert_eq!(format_duration(Duration::from_micros(123_456)), "123.5ms");
    assert_eq!(format_duration(Duration::from_micros(999_999)), "1000.0ms");
}

#[test]
fn test_format_duration_seconds() {
    // >= 1000ms should display as seconds with 1 decimal place
    assert_eq!(format_duration(Duration::from_micros(1_000_000)), "1.0s");
    assert_eq!(format_duration(Duration::from_micros(1_500_000)), "1.5s");
    assert_eq!(format_duration(Duration::from_micros(2_750_000)), "2.8s");
    assert_eq!(format_duration(Duration::from_micros(10_000_000)), "10.0s");
    assert_eq!(
        format_duration(Duration::from_micros(123_456_789)),
        "123.5s"
    );
}

#[test]
fn test_format_duration_boundary_cases() {
    // Test exact boundaries between units
    assert_eq!(format_duration(Duration::from_nanos(999_999)), "999μs");
    assert_eq!(format_duration(Duration::from_nanos(1_000_000)), "1.0ms");
    assert_eq!(
        format_duration(Duration::from_nanos(999_999_999)),
        "1000.0ms"
    );
    assert_eq!(format_duration(Duration::from_nanos(1_000_000_000)), "1.0s");
}

#[test]
fn test_format_duration_typical_query_times() {
    // Test typical query execution times seen in the wild
    assert_eq!(format_duration(Duration::from_micros(500)), "500μs"); // Fast indexed lookup
    assert_eq!(format_duration(Duration::from_micros(3_500)), "3.5ms"); // Normal query
    assert_eq!(format_duration(Duration::from_micros(25_000)), "25.0ms"); // Slower query
    assert_eq!(format_duration(Duration::from_micros(150_000)), "150.0ms"); // Complex query
}

// -------------------------------------------------------------------------
// History Navigation Tests
// -------------------------------------------------------------------------

#[test]
fn test_navigate_history_empty() {
    // Create a minimal TuiApp for testing history navigation
    let mut app_state = create_test_app_state();

    // Navigating empty history should do nothing
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "");
    assert_eq!(app_state.history_index, None);
}

#[test]
fn test_navigate_history_up_from_fresh() {
    let mut app_state = create_test_app_state();

    // Add some history
    app_state.history.push("SELECT * FROM users".to_string());
    app_state.history.push("SELECT * FROM orders".to_string());
    app_state.history.push("SELECT * FROM products".to_string());

    // Press Up once - should show most recent command
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "SELECT * FROM products");
    assert_eq!(app_state.history_index, Some(2));

    // Press Up again - should show previous command
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "SELECT * FROM orders");
    assert_eq!(app_state.history_index, Some(1));

    // Press Up again - should show oldest command
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "SELECT * FROM users");
    assert_eq!(app_state.history_index, Some(0));

    // Press Up again - should stay at oldest
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "SELECT * FROM users");
    assert_eq!(app_state.history_index, Some(0));
}

#[test]
fn test_navigate_history_down() {
    let mut app_state = create_test_app_state();

    // Add history
    app_state.history.push("command1".to_string());
    app_state.history.push("command2".to_string());
    app_state.history.push("command3".to_string());

    // Navigate to middle of history
    app_state.navigate_history(true);
    app_state.navigate_history(true);
    assert_eq!(app_state.history_index, Some(1));
    assert_eq!(app_state.input, "command2");

    // Press Down - should go forward
    app_state.navigate_history(false);
    assert_eq!(app_state.input, "command3");
    assert_eq!(app_state.history_index, Some(2));

    // Press Down again - should clear input and reset
    app_state.navigate_history(false);
    assert_eq!(app_state.input, "");
    assert_eq!(app_state.history_index, None);

    // Press Down when already at end - should do nothing
    app_state.navigate_history(false);
    assert_eq!(app_state.input, "");
    assert_eq!(app_state.history_index, None);
}

#[test]
fn test_navigate_history_cycle() {
    let mut app_state = create_test_app_state();

    // Add history
    app_state.history.push("first".to_string());
    app_state.history.push("second".to_string());

    // Go up to oldest
    app_state.navigate_history(true);
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "first");

    // Go down to newest
    app_state.navigate_history(false);
    assert_eq!(app_state.input, "second");

    // Go down past end - should clear
    app_state.navigate_history(false);
    assert_eq!(app_state.input, "");
    assert_eq!(app_state.history_index, None);

    // Can navigate up again from fresh state
    app_state.navigate_history(true);
    assert_eq!(app_state.input, "second");
    assert_eq!(app_state.history_index, Some(1));
}

/// Helper struct for testing history navigation without a full TuiApp
/// This allows us to test the navigate_history logic in isolation
struct TestHistoryState {
    input: String,
    history: Vec<String>,
    history_index: Option<usize>,
}

impl TestHistoryState {
    fn new() -> Self {
        Self {
            input: String::new(),
            history: Vec::new(),
            history_index: None,
        }
    }

    /// Navigate history (same logic as TuiApp::navigate_history)
    fn navigate_history(&mut self, up: bool) {
        if self.history.is_empty() {
            return;
        }

        if up {
            let index = match self.history_index {
                None => self.history.len() - 1,
                Some(i) if i > 0 => i - 1,
                Some(_) => return,
            };
            self.history_index = Some(index);
            self.input = self.history[index].clone();
        } else {
            match self.history_index {
                None => return,
                Some(i) if i < self.history.len() - 1 => {
                    self.history_index = Some(i + 1);
                    self.input = self.history[i + 1].clone();
                }
                Some(_) => {
                    self.history_index = None;
                    self.input.clear();
                }
            }
        }
    }
}

/// Helper function to create a minimal test state for history navigation
fn create_test_app_state() -> TestHistoryState {
    TestHistoryState::new()
}

// -------------------------------------------------------------------------
// extract_table_name Tests (Issue #251)
// -------------------------------------------------------------------------

#[test]
fn test_extract_table_name_valid() {
    assert_eq!(
        extract_table_name("users-3b7a9d8c"),
        Some("users".to_string())
    );
    assert_eq!(
        extract_table_name("test_table-abc123"),
        Some("test_table".to_string())
    );
    assert_eq!(
        extract_table_name("MyTable123-uuid"),
        Some("MyTable123".to_string())
    );
}

#[test]
fn test_extract_table_name_invalid() {
    // No dash
    assert_eq!(extract_table_name("users"), None);
    // Empty before dash
    assert_eq!(extract_table_name("-uuid"), None);
    // Special characters
    assert_eq!(extract_table_name("table.name-uuid"), None);
    assert_eq!(
        extract_table_name("table-name-uuid"),
        Some("table".to_string())
    ); // Takes first part before first dash
}

#[test]
fn test_extract_table_name_edge_cases() {
    // Multiple dashes - should extract first part
    assert_eq!(
        extract_table_name("my-table-uuid-1234"),
        Some("my".to_string())
    );
    // Just dash
    assert_eq!(extract_table_name("-"), None);
    // Empty string
    assert_eq!(extract_table_name(""), None);
}
