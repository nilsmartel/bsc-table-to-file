use postgres::fallible_iterator::FallibleIterator;
use postgres::Row;
mod db;
use std::fs::File;
use std::sync::mpsc::*;
use std::thread::spawn;

mod tablerow;
use tablerow::TableRow;

fn print_help() -> ! {
    println!("<outfile> <tablename>");
    std::process::exit(0)
}

fn parse_args() -> (String, String) {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.contains(&String::from("--help")) || args.len() != 2 {
        print_help();
    }

    let table = args.pop().unwrap();
    let outfile = args.pop().unwrap();

    (outfile, table)
}

fn stream_rows(table: &str) -> Receiver<Row> {
    // stream rows of tables, sorted
    let query = format!("SELECT tokenized, tableid, rowid, colid FROM {table} ORDER BY tokenized");

    let (s, r) = channel();

    spawn(move || {
        let mut client = db::client();
        println!("querying...");
        let params: [bool; 0] = [];
        let mut rows = client.query_raw(&query, &params).expect("query database");

        while let Some(row) = rows.next().unwrap() {
            s.send(row).expect("send row to channel");
        }
    });

    r
}

fn main() {
    let (outfile, table) = parse_args();

    println!("writing rows to {outfile}");

    // write it back to some file
    let mut f = File::create(&outfile).expect("create outfile");

    let mut i = 0;
    for row in stream_rows(&table) {
        let row = TableRow::from_row(&row);
        row.write_bin(&mut f).expect("write row");

        if i & 0x3ff == 0 {
            println!("{:05}%", i);
        }

        i += 1;
    }
    println!("done");
}
