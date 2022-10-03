use postgres::fallible_iterator::FallibleIterator;
mod db;
use std::fs::File;

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
    let params: [bool; 0] = [];
    let mut rows = client.query_raw(&query, &params).expect("query database");

    println!("received rows");

    // write it back to some file
    let mut f = File::create(&outfile).expect("create outfile");

    println!("start writing");

    let mut i = 0;
    while let Some(row) = rows.next().unwrap() {
        let row = TableRow::from_row(&row);
        row.write_bin(&mut f).expect("write row");

        if i & 0x3ff == 0 {
            println!("{:05}%", i);
        }

        i += 1;
    }
    println!("nice.");
}
