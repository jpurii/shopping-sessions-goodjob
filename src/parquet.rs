//! A minimal Parquet writer: one row group, one PLAIN data page per column,
//! uncompressed. Enough for the two output tables (strings, dates, integers,
//! one nullable string), and it needs no dependencies.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

pub enum Col<'a> {
    Str(Vec<&'a [u8]>),
    OptStr(Vec<Option<&'a [u8]>>),
    Date(Vec<i32>),
    I64(Vec<i64>),
}

impl Col<'_> {
    fn len(&self) -> usize {
        match self {
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
            Col::Date(v) => v.len(),
            Col::I64(v) => v.len(),
        }
    }
    fn physical(&self) -> i32 {
        match self {
            Col::Str(_) | Col::OptStr(_) => 6, // BYTE_ARRAY
            Col::Date(_) => 1,                 // INT32
            Col::I64(_) => 2,                  // INT64
        }
    }
}

/// Thrift compact protocol, just the parts Parquet metadata needs.
struct Tc {
    b: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl Tc {
    fn new() -> Self {
        Tc { b: Vec::new(), last: 0, stack: Vec::new() }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.varint((((id as i32) << 1) ^ ((id as i32) >> 31)) as u32 as u64);
        }
        self.last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.stack.push(self.last);
        self.last = 0;
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last = self.stack.pop().unwrap_or(0);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn elem_begin(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn elem_i32(&mut self, v: i32) {
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn elem_bin(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
}

fn page_data(col: &Col) -> Vec<u8> {
    let mut out = Vec::new();
    match col {
        Col::Str(v) => {
            out.reserve(v.iter().map(|s| s.len() + 4).sum());
            for s in v {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
        Col::OptStr(v) => {
            // Definition levels: RLE runs of 0 (null) / 1 (present), bit width 1,
            // prefixed by their byte length (data page v1).
            let mut levels = Vec::new();
            let mut i = 0;
            while i < v.len() {
                let present = v[i].is_some();
                let mut j = i;
                while j < v.len() && v[j].is_some() == present {
                    j += 1;
                }
                let mut run = ((j - i) as u64) << 1;
                while run >= 0x80 {
                    levels.push((run as u8) | 0x80);
                    run >>= 7;
                }
                levels.push(run as u8);
                levels.push(present as u8);
                i = j;
            }
            out.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            out.extend_from_slice(&levels);
            for s in v.iter().flatten() {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
        Col::Date(v) => {
            out.reserve(v.len() * 4);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        Col::I64(v) => {
            out.reserve(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
    }
    out
}

pub fn write(path: &Path, cols: &[(&str, Col)]) -> io::Result<()> {
    let nrows = cols.first().map(|c| c.1.len()).unwrap_or(0);
    let mut file = io::BufWriter::with_capacity(1 << 20, File::create(path)?);
    file.write_all(b"PAR1")?;
    let mut offset: i64 = 4;
    let mut chunks = Vec::new(); // (page offset, total size)
    if nrows > 0 {
        for (_, col) in cols {
            let data = page_data(col);
            let mut h = Tc::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, data.len() as i32);
            h.i32(3, data.len() as i32);
            h.begin(5);
            h.i32(1, nrows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end();
            h.b.push(0);
            file.write_all(&h.b)?;
            file.write_all(&data)?;
            let size = (h.b.len() + data.len()) as i64;
            chunks.push((offset, size));
            offset += size;
        }
    }

    let mut m = Tc::new();
    m.i32(1, 1);
    m.list(2, 12, cols.len() + 1);
    m.elem_begin();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for (name, col) in cols {
        m.elem_begin();
        m.i32(1, col.physical());
        m.i32(3, if matches!(col, Col::OptStr(_)) { 1 } else { 0 });
        m.bin(4, name.as_bytes());
        match col {
            Col::Str(_) | Col::OptStr(_) => {
                m.i32(6, 0); // UTF8
                m.begin(10);
                m.begin(1); // STRING
                m.end();
                m.end();
            }
            Col::Date(_) => {
                m.i32(6, 6); // DATE
                m.begin(10);
                m.begin(6); // DATE
                m.end();
                m.end();
            }
            Col::I64(_) => {}
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    if nrows > 0 {
        m.list(4, 12, 1);
        m.elem_begin();
        m.list(1, 12, cols.len());
        for ((name, col), &(off, size)) in cols.iter().zip(&chunks) {
            m.elem_begin();
            m.i64(2, off);
            m.begin(3);
            m.i32(1, col.physical());
            m.list(2, 5, 2);
            m.elem_i32(0);
            m.elem_i32(3);
            m.list(3, 8, 1);
            m.elem_bin(name.as_bytes());
            m.i32(4, 0); // UNCOMPRESSED
            m.i64(5, nrows as i64);
            m.i64(6, size);
            m.i64(7, size);
            m.i64(9, off);
            m.end();
            m.end();
        }
        m.i64(2, offset - 4);
        m.i64(3, nrows as i64);
        m.end();
    } else {
        m.list(4, 12, 0);
    }
    m.bin(6, b"goodjob-sessions");
    m.b.push(0);
    file.write_all(&m.b)?;
    file.write_all(&(m.b.len() as u32).to_le_bytes())?;
    file.write_all(b"PAR1")?;
    file.flush()
}
