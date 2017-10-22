extern crate postgres;
extern crate chrono;

use postgres::{Connection, TlsMode};
use chrono::naive::NaiveDateTime;


#[derive(Debug)]
struct Metric {
    timestamp: NaiveDateTime,
    program: Option<String>,
    window_title: Option<String>,
    idle_time_ms: Option<i64>,
}

fn main() {
    let conn = Connection::connect("postgres://Garrett@garspace.com/Garrett", TlsMode::None).unwrap();
    for row in &conn.query("SELECT * FROM metrics limit 10000", &[]).unwrap() {
        let metric = Metric {
            timestamp: row.get(0),
            program: row.get(1),
            window_title: row.get(2),
            idle_time_ms: row.get(3),
        };
        println!("Found metric {:?}", metric);
    }
}
