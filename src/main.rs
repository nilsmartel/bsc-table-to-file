use anyhow::Result;
use postgres::{types::FromSql, Client, NoTls};
mod db;
use std::io::prelude::*;
use std::fs::File;
use fast_smaz::Smaz;
use varint_compression::*;

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

    let content = std::fs::read_to_string(outfile).expect("read outfile");
    let mut content = content.as_bytes();

    while !content.is_empty() {
        let ( row, rest ) = TableRow::from_bin(content).expect("read row");
        content = rest;

        dbg!(row);
    }
}

#[derive(Debug, Clone)]
struct TableRow {
    tokenized: String,
    tableid: i32,
    rowid: i64,
    colid: i32,
}

impl TableRow {
    fn from_row(row: postgres::Row) -> Self {
        let tokenized = row.get("tokenized");
        let tableid = row.get("tableid");
        let rowid = row.get("rowid");
        let colid = row.get("colid");

        TableRow { tokenized, tableid, rowid, colid }
    }

    fn write_bin(&self, w: &mut impl Write ) -> Result<()> {
        let tokenized = self.tokenized.smaz_compress();
        let len = compress(tokenized.len() as u64);
        let nums = compress_list(&[self.tableid as u64, self.rowid as u64, self. colid as u64]);

        let total_length = compress((len.len() + tokenized.len() + nums.len()) as u64);

        w.write_all(&total_length)?;
        w.write_all(&len)?;
        w.write_all(&tokenized)?;
        w.write_all(&nums)?;
        
        Ok(())
    }

    fn from_bin(data: &[u8]) -> Result<(Self, &[u8])> {
        let (total_length, rest) = decompress(data);
        let total_length = total_length as usize;

        if rest.len() < total_length {

            return Err(anyhow::Error::msg("need more data"));
        }

        let v = TableRow::from_bin_raw(rest);

        Ok((v, &rest[total_length..]))
    }

    fn from_bin_raw(data: &[u8]) -> Self {
        let (n, rest) = decompress(data);
        let n = n as usize;
        let tokenized = &rest[..n];
        let tokenized = tokenized.smaz_decompress().unwrap();
        let tokenized = String::from_utf8(tokenized).unwrap();

        let ([tableid, rowid, colid], _rest) = decompress_n(&rest[n..]);

        let tableid = tableid as i32;
        let rowid = rowid as i64;
        let colid = colid as i32;

        Self {
            tokenized,
            tableid,
            rowid,
            colid,
        }
    }
}