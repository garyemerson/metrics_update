extern crate postgres;
extern crate chrono;

use postgres::{Connection, TlsMode};
use chrono::naive::NaiveDateTime;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::env;


#[derive(Debug)]
struct Metric {
    timestamp: NaiveDateTime,
    program: Option<String>,
    window_title: Option<String>,
    idle_time_ms: Option<i64>,
}

// Usage:
// <exe> postgres://Garrett@garspace.com/Garrett /home/pi/activity_test2 test_metrics2 merge_temp
fn main() {
    let mut args = env::args();
    args.nth(0)
        .expect("Must provide arguments.");
    let conn_str = args.nth(0)
        .expect("Must provide connection string as the first argument");
    let conn = Connection::connect(conn_str.as_ref(), TlsMode::None)
        .expect(&format!("Error setting up connection with connection string '{}'", conn_str));

    let file_path = args.nth(0)
        .expect("Must provide the path to the activity file as the second argument.");
    let mut file_buf = open_file_skip_first_line(&file_path)
        .expect(&format!("Error opening file {} and skipping first line", file_path));
    let table = args.nth(0)
        .expect("Must provide the name of the main db table as the third argument.");
    let temp_table = args.nth(0)
        .expect("Must provide the name of the temp db table to create as the fourth argument.");
    copy_to_temp(&temp_table, &mut file_buf, &conn)
        .expect("Error copying data to temp table");
    remove_dups(&conn, &temp_table)
        .expect(&format!("Error removing dups from temp table {}", temp_table));
    merge(&conn, &temp_table, &table)
        .expect(&format!("Error merging temp table {} into table {}", temp_table, table));
}

// Merge the contents of temp_table into table
fn merge(conn: &Connection, temp_table: &str, table: &str) -> Result<(), String> {
    let stmt_str = format!(
"INSERT INTO {dest_table}
select * from {src_table} 
where not exists (
    select * from {dest_table} 
    where timestamp={src_table}.timestamp)",
        dest_table = table,
        src_table = temp_table);

    conn.execute(&stmt_str, &[])
        .map_err(|e| format!(
            "Error merging temp table {} with table {} using '{}': {}",
            temp_table,
            table,
            stmt_str,
            e))?;
    Ok(())
}

fn remove_dups(conn: &Connection, table: &str) -> Result<(), String> {
    let stmt_str = format!(
"delete from {table}
where (timestamp, program, window_title, idle_time_ms)
in (
    select timestamp, program, window_title, idle_time_ms 
    from (
        SELECT *, ROW_NUMBER() OVER (partition BY timestamp ORDER BY timestamp) AS rnum
        FROM {table}
    ) t
    where t.rnum > 1);",
        table = table);

    conn.execute(&stmt_str, &[])
        .map_err(|e| format!("Error removing duplicates with '{}': {}", stmt_str, e))?;
    Ok(())
}

fn open_file_skip_first_line(file_path: &str) -> Result<BufReader<File>, String> {
    let file = File::open(file_path)
        .map_err(|e| format!("Error opening file {}: {}", file_path, e))?;
    let mut file_buf = BufReader::new(file);
    file_buf.read_line(&mut String::new())
        .map_err(|e| format!("Error reading from buf to file {}: {}", file_path, e))?;
    Ok(file_buf)
}

fn copy_to_temp(
    temp_name: &str,
    file_buf: &mut BufReader<File>,
    conn: &Connection) -> Result<(), String>
{
    // create temp table
    let mut stmt_str = format!(
        "CREATE TEMPORARY TABLE {} (timestamp timestamp, program text, window_title text, idle_time_ms bigint)",
        temp_name);
    conn.execute(&stmt_str, &[])
        .map_err(|e| format!("Error creating temp table with '{}': {}", stmt_str, e))?;

    // use sql COPY to dump csv to temp table
    stmt_str = format!(
        "COPY {} (timestamp, program, window_title, idle_time_ms) FROM STDIN WITH (FORMAT csv)",
        temp_name);
    let stmt = conn.prepare(&stmt_str)
        .map_err(|e| format!("Error preparing statement '{}': {}", stmt_str, e))?;
    stmt.copy_in(&[], file_buf)
        .map_err(|e| format!("Error executing '{}': {}", stmt_str, e))?;
    Ok(())
}
