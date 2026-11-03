//! `Database::execute` write-path contract at the root package's feature set.
//!
//! The root `cqlite` package builds `cqlite-core` WITHOUT `experimental`, and
//! INSERT is gated behind that feature: it must fail closed with
//! `Error::UnsupportedFormat` naming the feature, never silently succeed or
//! drop the row. CREATE TABLE and an empty SELECT stay available. (#4421: this
//! test previously expected INSERT to succeed, which no default build allows.)

use cqlite_core::{Config, Database, Error};
use tempfile::TempDir;

#[tokio::test]
async fn test_insert_fails_closed_without_experimental() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let db = Database::open(temp_dir.path(), Config::default())
        .await
        .expect("Failed to open database");

    let create_result = db
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        .await
        .expect("CREATE TABLE should succeed without experimental");
    assert_eq!(create_result.rows_affected, 0);

    let err = db
        .execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
        .await
        .expect_err("INSERT must fail closed without the experimental feature");
    match &err {
        Error::UnsupportedFormat(msg) => assert!(
            msg.contains("experimental"),
            "refusal should name the experimental feature, got: {msg}"
        ),
        other => panic!("expected Error::UnsupportedFormat, got: {other:?}"),
    }

    // Nothing was written, so a SELECT returns no rows.
    let select_result = db
        .execute("SELECT * FROM users")
        .await
        .expect("SELECT should succeed on an empty table");
    assert!(
        select_result.rows.is_empty(),
        "a refused INSERT must not leave rows behind"
    );

    db.close().await.expect("Failed to close database");
}
