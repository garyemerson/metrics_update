extern crate postgres;
extern crate chrono;

use postgres::{Connection, TlsMode};
use chrono::naive::NaiveDateTime;
use std::fs::File;
use std::io::Read;
use std::io::{BufReader, BufRead};


#[derive(Debug)]
struct Metric {
    timestamp: NaiveDateTime,
    program: Option<String>,
    window_title: Option<String>,
    idle_time_ms: Option<i64>,
}

fn main() {
    let conn = Connection::connect("postgres://Garrett@garspace.com/Garrett", TlsMode::None).unwrap();

    let file_path = "/home/pi/activity_test2";
    let mut file_buf = open_file_skip_first_line(file_path)
        .expect(&format!("Error opening file {} and skipping first line", file_path));
    copy_to_temp("test_metrics2", &mut file_buf, conn)
        .expect("Error copying data to temp table");

    // for row in &conn.query("SELECT * FROM metrics limit 10000", &[]).unwrap() {
    //     let metric = Metric {
    //         timestamp: row.get(0),
    //         program: row.get(1),
    //         window_title: row.get(2),
    //         idle_time_ms: row.get(3),
    //     };
    //     println!("Found metric {:?}", metric);
    // }
}

// fn remove_dups(table_name: &str) -> Result<(), String> {}

fn open_file_skip_first_line(file_path: &str) -> Result<BufReader<File>, String> {
    let file = File::open(file_path)
        .map_err(|e| format!("Error opening file {}: {}", file_path, e))?;
    let mut file_buf = BufReader::new(file);
    file_buf.read_line(&mut String::new())
        .map_err(|e| format!("Error reading from buf to file {}: {}", file_path, e))?;
    Ok(file_buf)
}

fn copy_to_temp(
    table_name: &str,
    file_buf: &mut BufReader<File>,
    conn: Connection) -> Result<(), String>
{
    // create temp table
    // let mut stmt_str = format!(
    //     "CREATE TEMPORARY TABLE {} (timestamp timestamp primary key, program text, window_title text, idle_time_ms bigint)",
    //     table_name);
    // conn.execute(&stmt_str, &[])
    //     .map_err(|e| format!("Error creating temp table with '{}': {}", stmt_str, e))?;

    // use sql COPY to dump csv to temp table
    let stmt_str = format!(
        "COPY {} (timestamp, program, window_title, idle_time_ms) FROM STDIN WITH (FORMAT csv)",
        table_name);
    let stmt = conn.prepare(&stmt_str)
        .map_err(|e| format!("Error preparing statement '{}': {}", stmt_str, e))?;
    let num_rows = stmt.copy_in(&[], file_buf)
        .map_err(|e| format!("Error executing '{}': {}", stmt_str, e))?;
    println!("{} rows processed with COPY", num_rows);
    Ok(())
}
