use anyhow::Result;
use std::io::prelude::*;
use fast_smaz::Smaz;
use varint_compression::*;
use postgres::Row;

#[derive(Debug, Clone)]
pub struct TableRow {
    pub tokenized: String,
    pub tableid: u32,
    pub colid: u32,
    pub rowid: u64,
}

fn get_number(row: &Row, idx: &str) -> i64 {
    if let Ok(n) = row.try_get::<_, i32>(idx) {
        return n as i64;
    }

    if let Ok(n) = row.try_get::<_, i64>(idx) {
        return n as i64;
    }

    if let Ok(n) = row.try_get::<_, i8>(idx) {
        return n as i64;
    }

    if let Ok(n) = row.try_get::<_, i16>(idx) {
        return n as i64;
    }

    // We error here on purpose.
    row.get(idx)
}

impl TableRow {
    fn from_row(row: Row) -> Self {
        let tokenized = row.get("tokenized");
        let tableid = get_number(&row, "tableid") as u32;
        let colid = get_number(&row, "colid") as u32;
        let rowid = get_number(&row, "rowid") as u64;

        TableRow {
            tokenized,
            tableid,
            colid,
            rowid,
        }
    }

    fn write_bin(&self, w: &mut impl Write) -> Result<()> {
        let tokenized = self.tokenized.smaz_compress();
        let len = compress(tokenized.len() as u64);
        let nums = compress_list(&[self.tableid as u64, self.colid as u64, self.rowid as u64, ]);

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

        let ([tableid, colid, rowid], _rest) = decompress_n(&rest[n..]);

        let tableid = tableid as u32;
        let colid = colid as u32;
        let rowid = rowid as u64;

        Self {
            tokenized,
            tableid,
            colid,
            rowid,
        }
    }
}