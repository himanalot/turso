use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(
        seed, 4096, 100, // Always schedule operations asynchronously.
        1, 5,
    ));
    let path = format!("sim_stmt_journal_{seed}.db");
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_i64_rows(
    conn: &Arc<Connection>,
    io: &MemorySimIO,
    sql: &str,
    column_count: usize,
) -> Result<Vec<Vec<i64>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                let mut values = Vec::new();
                for column in 0..column_count {
                    values.push(row.get::<i64>(column).expect("integer column should exist"));
                }
                rows.push(values);
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn query_text(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<String> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                return Ok(row.get::<String>(0).expect("text column should exist"));
            }
            StepResult::Done => panic!("query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_update_expression_error_rolls_back_statement_inside_txn() -> Result<()> {
    let (conn, io) = make_conn(1001)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INT)")?;
    conn.execute("INSERT INTO t VALUES(1, 10), (2, 20)")?;

    conn.execute("BEGIN")?;
    let result = conn.execute(
        "UPDATE t
            SET x = CASE
                WHEN id = 1 THEN 11
                ELSE (char(120) LIKE char(120) ESCAPE (char(121) || char(121)))
            END",
    );
    assert!(
        result.is_err(),
        "UPDATE should fail on the row-dependent ESCAPE expression"
    );

    assert_eq!(
        query_i64_rows(&conn, io.as_ref(), "SELECT id, x FROM t ORDER BY id", 2)?,
        vec![vec![1, 10], vec![2, 20]],
        "failed UPDATE must restore rows changed earlier in the statement"
    );
    assert_eq!(
        query_text(&conn, io.as_ref(), "PRAGMA integrity_check")?,
        "ok"
    );

    conn.execute("COMMIT")?;
    assert_eq!(
        query_i64_rows(&conn, io.as_ref(), "SELECT id, x FROM t ORDER BY id", 2)?,
        vec![vec![1, 10], vec![2, 20]],
        "committing the outer transaction must not persist partial UPDATE effects"
    );
    Ok(())
}

#[test]
fn sim_insert_replace_expression_index_abort_preserves_row() -> Result<()> {
    let (conn, io) = make_conn(1002)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b INT)")?;
    conn.execute(
        "CREATE INDEX idx ON t(
            CASE
                WHEN b = 2 THEN 'x' LIKE 'x' ESCAPE 'yy'
                ELSE b
            END
        )",
    )?;
    conn.execute("INSERT INTO t VALUES(1, 1)")?;

    conn.execute("BEGIN")?;
    let result = conn.execute("INSERT OR REPLACE INTO t VALUES(1, 2)");
    assert!(
        result.is_err(),
        "INSERT OR REPLACE should fail during expression-index maintenance"
    );

    assert_eq!(
        query_i64_rows(&conn, io.as_ref(), "SELECT id, b FROM t ORDER BY id", 2)?,
        vec![vec![1, 1]],
        "failed REPLACE must restore the conflicting row it deleted"
    );
    assert_eq!(
        query_text(&conn, io.as_ref(), "PRAGMA integrity_check")?,
        "ok"
    );

    conn.execute("COMMIT")?;
    assert_eq!(
        query_i64_rows(&conn, io.as_ref(), "SELECT id, b FROM t ORDER BY id", 2)?,
        vec![vec![1, 1]],
        "committing the outer transaction must keep the original row"
    );
    Ok(())
}
