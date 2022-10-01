use anyhow::Result;
mod db;
use std::fs::File;
use std::io::prelude::*;

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

fn main() {
    let (outfile, table) = parse_args();

    let mut client = db::client();

    println!("querying...");

    // load the entire table into memory, sorted.
    let query = format!("SELECT tokenized, tableid, rowid, colid FROM {table} ORDER BY tokenized");
    let rows = client.query(&query, &[]).expect("query database");
    let count = rows.len() as f64;

    println!("received {} rows", count);

    // write it back to some file
    let mut f = File::create(&outfile).expect("create outfile");

    println!("start writing");

    for (i, row ) in rows.iter().enumerate() {
        let row = TableRow::from_row(row);
        row.write_bin(&mut f).expect("write row");

        if i & 0x3ff == 0 {
            let percentage = i as f64 / count;
            println!("{:0.2}%", percentage * 100.0);
        }
    }
    println!("nice.");
}
