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

    // TODO query needs to be adjusted after verifying everything works
    let query = format!("SELECT tokenized, tableid, rowid, colid FROM {table} LIMIT 10");

    let rows = client.query(&query, &[]).expect("query database");

    let mut f = File::create(&outfile).expect("create outfile");

    for row in rows {
        let row = TableRow::from_row(row);
        row.write_bin(&mut f).expect("write row");
    }

    drop(f);

    let f = File::open(outfile).unwrap();

    let content: Vec<u8> = f.bytes().map(Result::unwrap).collect(); //.expect("read outfile");
    let mut content: &[u8] = &content;

    while !content.is_empty() {
        let (_row, rest) = TableRow::from_bin(content).expect("read row");
        content = rest;
    }
    
    println!("nice.");
}
