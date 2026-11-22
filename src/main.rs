//! Shopping Sessions pipeline (rules.md), one native pass with no dependencies.
//!
//! Every ID (event, user, product, campaign) becomes an exact 128-bit key: IDs
//! up to 15 bytes are packed big-endian with their length in the last byte, so
//! key order is byte order; longer IDs get a 120-bit hash (and keep their bytes
//! where we need order or output). Hash tables are flat open-addressing arrays.
//!
//! Phase A  parse every CSV chunk in parallel: events (partitioned by event_id),
//!          valid clicks, link pairs.
//! Phase B  union-find over the links; each linked key maps to its person's key.
//! Phase C  dedup events per event_id partition (earliest ingest_ts wins, then
//!          validity of that copy decides), map events and clicks to persons and
//!          repartition both by person.
//! Phase D  per person partition: sort, sessionize, credit, assists, daily rows.
//! Phase E  write both tables as Parquet.

mod parquet;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const Q: usize = 256; // person partitions

// ------------------------------------------------------------------ hashing

const K0: u64 = 0xa076_1d64_78bd_642f;
const K1: u64 = 0xe703_7ed1_a0b4_28db;
const K2: u64 = 0x8ebc_6af0_9c88_c6e3;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

fn hash_bytes(b: &[u8], seed: u64) -> u64 {
    let mut h = (b.len() as u64) ^ seed;
    let mut it = b.chunks_exact(8);
    for c in &mut it {
        h = mum(h ^ u64::from_le_bytes(c.try_into().unwrap()), K1);
    }
    let r = it.remainder();
    if !r.is_empty() {
        let mut buf = [0u8; 8];
        buf[..r.len()].copy_from_slice(r);
        h = mum(h ^ u64::from_le_bytes(buf), K2);
    }
    mum(h, K1 ^ K2)
}

/// Exact key of a non-empty ID. Short IDs (<= 15 bytes): bytes big-endian, then
/// the length, so comparing keys compares bytes. Long IDs: low byte 0xFF.
#[inline]
fn key(s: &[u8]) -> u128 {
    let n = s.len();
    if n <= 15 {
        let mut b = [0u8; 16];
        b[..n].copy_from_slice(s);
        b[15] = n as u8;
        u128::from_be_bytes(b)
    } else {
        let h1 = hash_bytes(s, K0) as u128;
        let h2 = hash_bytes(s, K2) as u128;
        (h1 << 64) | (h2 << 8) | 0xFF
    }
}

/// `key(s)` for a slice that lies inside a buffer ending at address `end`:
/// when 16 bytes from the slice start are readable, one unaligned load and a
/// mask replace the variable-length copy. Same result as `key`.
#[inline(always)]
fn key_in(s: &[u8], end: usize) -> u128 {
    let n = s.len();
    if n <= 15 && s.as_ptr() as usize + 16 <= end {
        // Safety: the 16 bytes at s.as_ptr() lie before `end`, inside the buffer.
        let v = unsafe { (s.as_ptr() as *const u128).read_unaligned() }.swap_bytes();
        let keep = if n == 0 { 0 } else { !0u128 << (8 * (16 - n)) };
        (v & keep) | n as u128
    } else {
        key(s)
    }
}

#[inline]
fn is_long(k: u128) -> bool {
    (k as u8) > 15
}

/// Bytes of a short key.
#[inline]
fn unpack(k: u128, out: &mut Vec<u8>) {
    let b = k.to_be_bytes();
    out.extend_from_slice(&b[..b[15] as usize]);
}

#[inline(always)]
fn hk(k: u128) -> u64 {
    mum((k as u64) ^ K0, ((k >> 64) as u64) ^ K1)
}

#[inline(always)]
fn part_of(k: u128, n: usize) -> usize {
    ((hk(k) >> 32) as usize * n) >> 32
}

/// Open-addressing map from non-zero keys to u32.
struct Table {
    e: Vec<(u128, u32)>,
    mask: usize,
}

impl Table {
    fn with_cap(n: usize) -> Table {
        let cap = (n.max(8) * 2).next_power_of_two();
        Table { e: vec![(0, 0); cap], mask: cap - 1 }
    }
    #[inline]
    fn slot(&self, k: u128) -> usize {
        let mut i = hk(k) as usize & self.mask;
        loop {
            let x = self.e[i].0;
            if x == k || x == 0 {
                return i;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn get(&self, k: u128) -> Option<u32> {
        if k == 0 {
            return None;
        }
        let e = self.e[self.slot(k)];
        if e.0 == k {
            Some(e.1)
        } else {
            None
        }
    }
}

/// Open-addressing map from linked ID key to person key; unlinked keys map to
/// themselves.
struct PersonMap {
    e: Vec<(u128, u128)>,
    mask: usize,
}

impl PersonMap {
    #[inline(always)]
    fn prefetch(&self, k: u128) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let p = self.e.as_ptr().add(hk(k) as usize & self.mask);
            _mm_prefetch::<_MM_HINT_T0>(p as *const i8);
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = k;
    }
    #[inline]
    fn get(&self, k: u128) -> u128 {
        let mut i = hk(k) as usize & self.mask;
        loop {
            let (x, p) = self.e[i];
            if x == k {
                return p;
            }
            if x == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- file data

struct Buf {
    ptr: *const u8,
    len: usize,
    _owned: Vec<u8>,
}
unsafe impl Send for Buf {}
unsafe impl Sync for Buf {}

impl Buf {
    fn bytes(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
    fn owned(v: Vec<u8>) -> Buf {
        Buf { ptr: v.as_ptr(), len: v.len(), _owned: v }
    }
}

#[cfg(unix)]
fn load(path: &Path) -> Buf {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    }
    let f = std::fs::File::open(path).expect("open");
    let len = f.metadata().expect("stat").len() as usize;
    if len == 0 {
        return Buf::owned(Vec::new());
    }
    // PROT_READ, MAP_PRIVATE. Never unmapped: the process exits when done.
    let p = unsafe { mmap(std::ptr::null_mut(), len, 1, 2, f.as_raw_fd(), 0) };
    if p as isize == -1 {
        return Buf::owned(std::fs::read(path).expect("read"));
    }
    Buf { ptr: p, len, _owned: Vec::new() }
}

#[cfg(not(unix))]
fn load(path: &Path) -> Buf {
    Buf::owned(std::fs::read(path).expect("read"))
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// The file's body (after the header line), split into chunks at line ends.
fn chunks(data: &[u8], target: usize, out: &mut Vec<(u8, *const u8, usize)>, kind: u8) {
    let start = match memchr(b'\n', data) {
        Some(p) => p + 1,
        None => return,
    };
    let mut s = start;
    while s < data.len() {
        let mut e = (s + target).min(data.len());
        if e < data.len() {
            e = match memchr(b'\n', &data[e..]) {
                Some(p) => e + p + 1,
                None => data.len(),
            };
        }
        out.push((kind, data[s..e].as_ptr(), e - s));
        s = e;
    }
}

#[inline]
fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    s.iter().position(|&x| x == c)
}

// --------------------------------------------------------------- CSV dialect

/// Splits a line into exactly N fields, unquoting each. False if malformed.
#[inline]
fn fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut n = 0;
    let mut start = 0;
    loop {
        let end = match memchr(b',', &line[start..]) {
            Some(p) => start + p,
            None => line.len(),
        };
        if n == N {
            return false;
        }
        let f = &line[start..end];
        if memchr(b'"', f).is_some() {
            let l = f.len();
            if l < 2 || f[0] != b'"' || f[l - 1] != b'"' || memchr(b'"', &f[1..l - 1]).is_some() {
                return false;
            }
            out[n] = &f[1..l - 1];
        } else {
            out[n] = f;
        }
        n += 1;
        if end == line.len() {
            break;
        }
        start = end + 1;
    }
    n == N
}

/// Bitmasks of commas, newlines and quotes in the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |c: u8| {
            let s = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, s)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, s)) as u32 as u64;
            lo | (hi << 32)
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let x = *p.add(i);
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

/// Calls `on_line` with the N unquoted fields of every well-formed line of
/// `body` (the CSV dialect: every comma separates, only `\n` ends a line, a
/// quote may only wrap a whole field). Same results as `fields` per line, but
/// finds every separator and quote with 64-byte SIMD masks in one pass: since
/// no value holds a comma or newline, quotes never change where fields split.
#[inline(always)]
#[allow(unused_assignments)]
fn scan<'a, const N: usize>(body: &'a [u8], mut on_line: impl FnMut(&[&'a [u8]; N])) {
    let len = body.len();
    let mut f: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields ended in this line
    let mut bad = false;
    let mut fstart = 0usize;
    let (mut qn, mut qfirst, mut qlast) = (0u32, 0usize, 0usize);
    let mut tail = [0u8; 64];
    let mut base = 0usize;
    macro_rules! quotes {
        ($m:expr) => {{
            let m: u64 = $m;
            if qn == 0 {
                qfirst = base + m.trailing_zeros() as usize;
            }
            qn += m.count_ones();
            qlast = base + 63 - m.leading_zeros() as usize;
        }};
    }
    macro_rules! end_field {
        ($p:expr) => {{
            let p: usize = $p;
            if nf < N {
                if qn == 0 {
                    f[nf] = &body[fstart..p];
                } else if qn == 2 && qfirst == fstart && qlast + 1 == p {
                    f[nf] = &body[fstart + 1..p - 1];
                } else {
                    bad = true;
                }
            }
            nf += 1;
            fstart = p + 1;
            qn = 0;
        }};
    }
    macro_rules! end_line {
        () => {{
            if !bad && nf == N {
                on_line(&f);
            }
            nf = 0;
            bad = false;
        }};
    }
    while base < len {
        let (cm, nm, mut qm) = if base + 64 <= len {
            unsafe { masks64(body.as_ptr().add(base)) }
        } else {
            let n = len - base;
            tail = [0u8; 64];
            tail[..n].copy_from_slice(&body[base..]);
            let (c, nl, q) = unsafe { masks64(tail.as_ptr()) };
            let valid = (1u64 << n) - 1;
            (c & valid, nl & valid, q & valid)
        };
        let mut sep = cm | nm;
        while sep != 0 {
            let off = sep.trailing_zeros() as usize;
            let below = qm & ((1u64 << off) - 1);
            if below != 0 {
                quotes!(below);
                qm &= !below;
            }
            end_field!(base + off);
            if (nm >> off) & 1 == 1 {
                end_line!();
            }
            sep &= sep - 1;
        }
        if qm != 0 {
            quotes!(qm);
        }
        base += 64;
    }
    // A last line without its newline.
    if fstart < len || nf > 0 {
        end_field!(len);
        end_line!();
    }
    let _ = tail;
}

// ---------------------------------------------------------------- timestamps

#[inline]
fn d2(s: &[u8], i: usize) -> Option<u32> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some(a as u32 * 10 + b as u32)
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// "YYYY-MM-DDTHH:MM:SS" (19 bytes) to epoch ms, validated.
/// Checks the separators and all 14 digits with three word loads (SWAR):
/// a = "YYYY-MM-", b = "DDTHH:MM", c = "M:SS" (bytes 15..19).
#[inline]
fn datetime(s: &[u8]) -> Option<i64> {
    let a = u64::from_le_bytes(s[0..8].try_into().unwrap());
    let b = u64::from_le_bytes(s[8..16].try_into().unwrap());
    let c = u32::from_le_bytes(s[15..19].try_into().unwrap());
    if a & 0xFF00_00FF_0000_0000 != 0x2D00_002D_0000_0000 // '-' at 4 and 7
        || b & 0x0000_FF00_00FF_0000 != 0x0000_3A00_0054_0000 // 'T' at 10, ':' at 13
        || c & 0x0000_FF00 != 0x0000_3A00
    // ':' at 16
    {
        return None;
    }
    // A digit byte is 0x30..=0x39: high nibble 3, and still 3 after adding 6.
    // Separator lanes are masked out and, being '-', 'T' or ':', cannot carry.
    const H64: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    const T64: u64 = 0x3030_3030_3030_3030;
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF; // digit lanes of a: 0,1,2,3,5,6
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF; // digit lanes of b: 0,1,3,4,6,7
    const MC: u32 = 0xFFFF_0000; // digit lanes of c: 2,3
    let ok64 = |x: u64, m: u64| {
        x & H64 & m == T64 & m && x.wrapping_add(0x0606_0606_0606_0606) & H64 & m == T64 & m
    };
    let okc = c & 0xF0F0_F0F0 & MC == 0x3030_3030 & MC
        && c.wrapping_add(0x0606_0606) & 0xF0F0_F0F0 & MC == 0x3030_3030 & MC;
    if !(ok64(a, MA) && ok64(b, MB) && okc) {
        return None;
    }
    let da = |i: u32| ((a >> (8 * i)) & 0xF) as u32;
    let db = |i: u32| ((b >> (8 * i)) & 0xF) as u32;
    let dc = |i: u32| ((c >> (8 * i)) & 0xF) as u32;
    let y = da(0) * 1000 + da(1) * 100 + da(2) * 10 + da(3);
    let mo = da(5) * 10 + da(6);
    let d = db(0) * 10 + db(1);
    let h = db(3) * 10 + db(4);
    let mi = db(6) * 10 + db(7);
    let se = dc(2) * 10 + dc(3);
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let days = days_from_civil(y as i64, mo, d);
    Some(((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + se as i64 * 1000)
}

/// F1, F2 or F3 to epoch ms; None for a bad timestamp (or one too large).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return datetime(&s[..19]);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let base = datetime(&s[..19])?;
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh as i64 * 60 + om as i64) * 60_000;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    if n == 0 {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig > 19 {
                return None; // beyond i64: outside every window
            }
            v = v * 10 + d as u64;
        }
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig > 8 {
                return None;
            }
            v = v * 10 + d as u64;
        }
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ------------------------------------------------------------------ records

/// One parsed event copy. `kind` 0 = invalid (only `eid` and `ingest` matter).
#[derive(Clone, Copy)]
struct Ev {
    eid: u128,
    user: u128,
    /// ingest_ts in whole seconds from window_start - 2^31 s (always F1, so whole
    /// seconds; exact within +-68 years of the window, saturating beyond).
    /// Only compared between copies of one event_id. Keeps `Ev` at 48 bytes.
    ingest: u32,
    ts: u32, // ms since window_start
    price: u32,
    cat: u16,
    kind: u8, // 0 invalid, 1 view, 2 cart, 3 purchase
}

#[derive(Clone, Copy)]
struct Ck<'a> {
    user: u128,
    cid: &'a [u8],
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Sv {
    person: u128,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Cp<'a> {
    person: u128,
    cid: &'a [u8],
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Sess {
    start: i64,
    day: i32,
    revenue: i64,
    purch: bool,
}

const _: () = assert!(std::mem::size_of::<Ev>() == 48);

/// Campaign x day totals [sessions, purchasing, revenue, assisted], dense over
/// (campaign, day - d0): every session starts inside the window, so its day is
/// in d0..d0+nd. A (campaign, day) has a row iff it has a session or an assist.
struct CampDays {
    v: Vec<[i64; 4]>,
    nd: usize,
    d0: i32,
}

impl CampDays {
    fn new(ncamp: usize, d0: i32, nd: usize) -> CampDays {
        CampDays { v: vec![[0; 4]; ncamp * nd], nd, d0 }
    }
    #[inline(always)]
    fn at(&mut self, c: u32, day: i32) -> &mut [i64; 4] {
        &mut self.v[c as usize * self.nd + (day - self.d0) as usize]
    }
}

/// Campaign `c` assisted sessions a..b (in start order): add them per day.
fn assists(camp: &mut CampDays, sess: &[Sess], dend: &[usize], c: u32, a: usize, b: usize) {
    let mut i = a;
    while i < b {
        let e = dend[i].min(b);
        camp.at(c, sess[i].day)[3] += (e - i) as i64;
        i = e;
    }
}

struct UserRow {
    person: u128,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

// ------------------------------------------------------------------ parallel

fn par<S: Send>(
    threads: usize,
    tasks: usize,
    init: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let (init, work, next) = (&init, &work, &next);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                sc.spawn(move || {
                    let mut s = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= tasks {
                            break;
                        }
                        work(&mut s, i);
                    }
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

struct SendPtr(*const u8, usize);
unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

fn find_json_str<'a>(json: &'a [u8], key: &str) -> &'a [u8] {
    let pat = format!("\"{key}\"");
    let pos = json
        .windows(pat.len())
        .position(|w| w == pat.as_bytes())
        .unwrap_or_else(|| panic!("manifest: no {key}"));
    let rest = &json[pos + pat.len()..];
    let q1 = memchr(b'"', rest).expect("manifest value") + 1;
    let q2 = q1 + memchr(b'"', &rest[q1..]).expect("manifest value");
    &rest[q1..q2]
}

fn body_lines(b: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = memchr(b'\n', b).map(|p| &b[p + 1..]).unwrap_or(&[]);
    body.split(|&c| c == b'\n')
}

/// Union-find over all link pairs. Returns the map from each linked ID's key
/// to its person's key (the key of the person's byte-wise smallest ID), the
/// number of linked IDs, and the bytes of linked IDs with long keys.
#[allow(clippy::type_complexity)]
fn build_links(
    lstates: &[Vec<(&'static [u8], &'static [u8])>],
) -> (PersonMap, usize, Vec<(u128, &'static [u8])>) {
    let nsides: usize = lstates.iter().map(|v| v.len() * 2).sum();
    let mut ids = Table::with_cap(nsides);
    let mut names: Vec<&'static [u8]> = Vec::with_capacity(nsides);
    let mut keys: Vec<u128> = Vec::with_capacity(nsides);
    let mut parent: Vec<u32> = Vec::with_capacity(nsides);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for v in lstates {
        for &(a, b) in v {
            let mut id = |s: &'static [u8]| -> u32 {
                let k = key(s);
                let i = ids.slot(k);
                if ids.e[i].0 == 0 {
                    let n = names.len() as u32;
                    ids.e[i] = (k, n);
                    names.push(s);
                    keys.push(k);
                    parent.push(n);
                    n
                } else {
                    ids.e[i].1
                }
            };
            let ia = id(a);
            let ib = id(b);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    let n = names.len();
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n as u32 {
        let r = find(&mut parent, i);
        if names[i as usize] < names[best[r as usize] as usize] {
            best[r as usize] = i;
        }
    }
    let mut pkey: Vec<u128> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        pkey[i] = keys[best[r as usize] as usize];
    }
    // Linked key -> person key in one 32-byte slot (same layout as `ids`), so a
    // lookup is one memory access; callers prefetch slots ahead of use.
    let pmap = PersonMap {
        e: ids.e.iter().map(|&(k, i)| (k, if k == 0 { 0 } else { pkey[i as usize] })).collect(),
        mask: ids.mask,
    };
    let long: Vec<(u128, &'static [u8])> =
        (0..n).filter(|&i| is_long(keys[i])).map(|i| (keys[i], names[i])).collect();
    (pmap, n, long)
}


fn main() {
    let t0 = Instant::now();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .max(1);
    let log = |msg: &str| eprintln!("[{:>7.1} ms] {msg}", t0.elapsed().as_secs_f64() * 1000.0);

    // -------------------------------------------------- small inputs
    let manifest = std::fs::read(data.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(find_json_str(&manifest, "window_start")).expect("window_start");
    let we = parse_ts(find_json_str(&manifest, "window_end")).expect("window_end");

    let products_buf = load(&data.join("products.csv"));
    let campaigns_buf = load(&data.join("campaigns.csv"));
    let mut cat_names: Vec<&[u8]> = Vec::new();
    let mut cat_of_name: HashMap<&[u8], u16> = HashMap::new();
    let nprod = body_lines(products_buf.bytes()).count();
    let mut product_cat = Table::with_cap(nprod);
    for line in body_lines(products_buf.bytes()) {
        let mut f = [&b""[..]; 2];
        if !fields(line, &mut f) || f[0].is_empty() {
            continue;
        }
        let next = cat_names.len() as u16;
        let c = *cat_of_name.entry(f[1]).or_insert_with(|| {
            cat_names.push(f[1]);
            next
        });
        let s = product_cat.slot(key(f[0]));
        if product_cat.e[s].0 == 0 {
            product_cat.e[s] = (key(f[0]), c as u32);
        }
    }
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown");
    // rank[c] = position of category c in byte order (for ties).
    let mut order: Vec<u16> = (0..cat_names.len() as u16).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        rank[c as usize] = r as u16;
    }

    let mut camp_ids: Vec<&[u8]> = Vec::new();
    let mut camp_channels: Vec<&[u8]> = Vec::new();
    let ncampl = body_lines(campaigns_buf.bytes()).count();
    let mut camp_of = Table::with_cap(ncampl);
    for line in body_lines(campaigns_buf.bytes()) {
        let mut f = [&b""[..]; 2];
        if !fields(line, &mut f) || f[0].is_empty() {
            continue;
        }
        let k = key(f[0]);
        let s = camp_of.slot(k);
        if camp_of.e[s].0 == 0 {
            camp_of.e[s] = (k, camp_ids.len() as u32);
            camp_ids.push(f[0]);
            camp_channels.push(f[1]);
        }
    }
    let organic = camp_ids.len() as u32;
    camp_ids.push(b"organic");
    camp_channels.push(b"organic");
    let ncamp = camp_ids.len();

    // -------------------------------------------------- big inputs
    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let lk_files = csv_files(&data.join("links"));
    let paths: Vec<(u8, PathBuf)> = ev_files
        .into_iter()
        .map(|p| (0u8, p))
        .chain(ck_files.into_iter().map(|p| (1u8, p)))
        .chain(lk_files.into_iter().map(|p| (2u8, p)))
        .collect();
    let mut slots: Vec<Option<Buf>> = (0..paths.len()).map(|_| None).collect();
    for (i, b) in par(threads, paths.len(), Vec::new, |v: &mut Vec<(usize, Buf)>, i| v.push((i, load(&paths[i].1))))
        .into_iter()
        .flatten()
    {
        slots[i] = Some(b);
    }
    let bufs: Vec<Buf> = slots.into_iter().map(|b| b.unwrap()).collect();
    let mut raw_chunks = Vec::new();
    for (i, (kind, _)) in paths.iter().enumerate() {
        chunks(bufs[i].bytes(), 8 << 20, &mut raw_chunks, *kind);
    }
    // Links first so the (single-threaded) union-find input is ready early.
    raw_chunks.sort_by_key(|c| match c.0 {
        2 => 0,
        k => k + 1,
    });
    let chunk_list: Vec<(u8, SendPtr)> = raw_chunks.into_iter().map(|(k, p, l)| (k, SendPtr(p, l))).collect();
    log(&format!("{} files, {} chunks, {} threads", paths.len(), chunk_list.len(), threads));

    // -------------------------------------------------- phase A: parse, phase B: links
    // Link chunks come first in chunk_list. Parse them, then run the
    // (sequential) union-find on its own thread while every worker parses
    // events and clicks.
    let nlk = chunk_list.iter().filter(|c| c.0 == 2).count();
    let lstates = par(threads, nlk, Vec::new, |v: &mut Vec<(&'static [u8], &'static [u8])>, i| {
        let sp = &chunk_list[i].1;
        // The buffers live until main returns.
        let body: &'static [u8] = unsafe { std::slice::from_raw_parts(sp.0, sp.1) };
        scan::<2>(body, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                v.push((f[0], f[1]));
            }
        });
    });
    // Software write-combining for the event scatter: each partition first
    // collects WCN events in a small per-thread staging array, then moves them
    // to its buffer in one burst (fewer L1/TLB misses with 1-2K partitions).
    const WCN: usize = 16;
    struct AState {
        ev: Vec<Vec<Ev>>,
        wc: Vec<Ev>,
        wn: Vec<u8>,
        ck: Vec<Ck<'static>>,
        long: Vec<(u128, &'static [u8])>,
    }
    let (cw_lo, cw_hi) = (ws - WEEK, we);
    // Reserve each partition buffer up front (about twice a thread's fair share
    // at ~64 bytes per event line) so pushes never reallocate and copy.
    let ev_bytes: usize = paths.iter().zip(&bufs).filter(|(p, _)| p.0 == 0).map(|(_, b)| b.len).sum();
    // event_id partitions: about 20K event lines each (~64 bytes a line), so a
    // partition's rows and dedup table stay cache-sized as the input grows.
    let prows: usize = 20_000;
    let p_ev = (ev_bytes / 64 / prows).next_power_of_two().clamp(256, 8192);
    let ev_reserve = ev_bytes / 64 / threads / p_ev * 2 + 16;
    let ingest_base = ws.div_euclid(1000) - (1i64 << 31);
    let product_cat = &product_cat;
    let camp_of = &camp_of;
    let (astates, (pmap, n, long_links)) = std::thread::scope(|sc| {
        let links = sc.spawn(|| build_links(&lstates));
        let a = par(
            threads,
            chunk_list.len() - nlk,
            || AState {
                ev: (0..p_ev).map(|_| Vec::with_capacity(ev_reserve)).collect(),
                wc: vec![Ev { eid: 0, user: 0, ingest: 0, ts: 0, price: 0, cat: 0, kind: 0 }; p_ev * WCN],
                wn: vec![0; p_ev],
                ck: Vec::new(),
                long: Vec::new(),
            },
            |st, i| {
                let (kind, ref sp) = chunk_list[i + nlk];
                // The buffers live until main returns.
                let body: &'static [u8] = unsafe { std::slice::from_raw_parts(sp.0, sp.1) };
                let end = body.as_ptr() as usize + body.len();
                if kind == 0 {
                    scan::<7>(body, |f| {
                        if f[0].is_empty() {
                            return;
                        }
                        let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                        let eid = key_in(f[0], end);
                        let ingest_rel = (ingest.div_euclid(1000) - ingest_base).clamp(0, u32::MAX as i64) as u32;
                        let mut ev = Ev { eid, user: 0, ingest: ingest_rel, ts: 0, price: 0, cat: 0, kind: 0 };
                        if !f[1].is_empty() && !f[5].is_empty() {
                            if let Some(ts) = parse_ts(f[2]) {
                                let k = match f[4] {
                                    b"view" => 1u8,
                                    b"cart" => 2,
                                    b"purchase" => 3,
                                    _ => 0,
                                };
                                if ts >= ws && ts < we && k != 0 && ingest.saturating_sub(ts) <= DAY {
                                    let price = if k == 3 { parse_price(f[6]) } else { Some(0) };
                                    if let Some(price) = price {
                                        let user = key_in(f[1], end);
                                        if is_long(user) {
                                            st.long.push((user, f[1]));
                                        }
                                        ev.user = user;
                                        ev.ts = (ts - ws) as u32;
                                        ev.kind = k;
                                        ev.price = price;
                                        ev.cat = product_cat.get(key_in(f[5], end)).map(|c| c as u16).unwrap_or(unknown);
                                    }
                                }
                            }
                        }
                        let j = part_of(eid, p_ev);
                        let n = st.wn[j] as usize;
                        st.wc[j * WCN + n] = ev;
                        if n + 1 == WCN {
                            st.ev[j].extend_from_slice(&st.wc[j * WCN..j * WCN + WCN]);
                            st.wn[j] = 0;
                        } else {
                            st.wn[j] = (n + 1) as u8;
                        }
                    });
                    // Flush the staged rows so every event of this chunk is in st.ev.
                    for j in 0..p_ev {
                        let n = st.wn[j] as usize;
                        if n > 0 {
                            st.ev[j].extend_from_slice(&st.wc[j * WCN..j * WCN + n]);
                            st.wn[j] = 0;
                        }
                    }
                } else {
                    scan::<4>(body, |f| {
                        if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
                            return;
                        }
                        let Some(ts) = parse_ts(f[3]) else { return };
                        if ts < cw_lo || ts >= cw_hi {
                            return;
                        }
                        let Some(camp) = camp_of.get(key_in(f[2], end)) else { return };
                        st.ck.push(Ck { user: key_in(f[1], end), cid: f[0], ts, camp });
                    });
                }
            },
        );
        log("parsed");
        (a, links.join().unwrap())
    });
    // Bytes of every long key that can name a person.
    let mut long_names: HashMap<u128, &[u8]> = HashMap::new();
    for &(k, s) in &long_links {
        long_names.insert(k, s);
    }
    for st in &astates {
        for &(k, s) in &st.long {
            long_names.insert(k, s);
        }
    }
    let pmap = &pmap;
    let person_of = |u: u128| -> u128 { pmap.get(u) };
    log(&format!("links: {} ids", n));

    // -------------------------------------------------- phase C: dedup + repartition
    struct CState<'a> {
        sv: Vec<Vec<Sv>>,
        ck: Vec<Vec<Cp<'a>>>,
        rows: Vec<Ev>,
        tab: Vec<u32>,
        win: Vec<u32>,
    }
    const AHEAD: usize = 16; // prefetch distance for person lookups
    let nst = astates.len();
    // Reserve each person-partition buffer up front (about twice a thread's
    // fair share of events) so the scatter never reallocates and copies.
    let sv_reserve = ev_bytes / 64 / threads / Q * 2 + 16;
    // Same for clicks (~40 bytes a click line).
    let ck_bytes: usize = paths.iter().zip(&bufs).filter(|(p, _)| p.0 == 1).map(|(_, b)| b.len).sum();
    let ck_reserve = ck_bytes / 40 / threads / Q * 2 + 16;
    let astates_ref = &astates;
    let cstates = par(
        threads,
        p_ev + nst,
        || CState {
            sv: (0..Q).map(|_| Vec::with_capacity(sv_reserve)).collect(),
            ck: (0..Q).map(|_| Vec::with_capacity(ck_reserve)).collect(),
            rows: Vec::new(),
            tab: Vec::new(),
            win: Vec::new(),
        },
        |st, i| {
            if i < p_ev {
                let rows = &mut st.rows;
                rows.clear();
                for a in astates_ref {
                    rows.extend_from_slice(&a.ev[i]);
                }
                let cap = (rows.len().max(8) * 2).next_power_of_two();
                let mask = cap - 1;
                let tab = &mut st.tab;
                tab.clear();
                tab.resize(cap, u32::MAX);
                for j in 0..rows.len() {
                    let k = rows[j].eid;
                    let mut s = hk(k) as usize & mask;
                    loop {
                        let v = tab[s];
                        if v == u32::MAX {
                            tab[s] = j as u32;
                            break;
                        }
                        if rows[v as usize].eid == k {
                            if rows[j].ingest < rows[v as usize].ingest {
                                tab[s] = j as u32;
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                }
                let win = &mut st.win;
                win.clear();
                for &j in tab.iter() {
                    if j != u32::MAX && rows[j as usize].kind != 0 {
                        win.push(j);
                    }
                }
                for w in 0..win.len() {
                    if w + AHEAD < win.len() {
                        pmap.prefetch(rows[win[w + AHEAD] as usize].user);
                    }
                    let r = &rows[win[w] as usize];
                    let person = person_of(r.user);
                    st.sv[part_of(person, Q)].push(Sv { person, ts: r.ts, price: r.price, cat: r.cat, kind: r.kind });
                }
            } else {
                let cks = &astates_ref[i - p_ev].ck;
                for w in 0..cks.len() {
                    if w + AHEAD < cks.len() {
                        pmap.prefetch(cks[w + AHEAD].user);
                    }
                    let c = &cks[w];
                    let person = person_of(c.user);
                    st.ck[part_of(person, Q)].push(Cp { person, cid: c.cid, ts: c.ts, camp: c.camp });
                }
            }
        },
    );
    log("deduped");

    // -------------------------------------------------- phase D: sessions
    struct DState {
        users: Vec<UserRow>,
        camp: CampDays,
    }
    let cstates_ref = &cstates;
    let rank = &rank;
    let ncat = cat_names.len();
    // Session start days lie in the event window: d0 ..= day of (window_end - 1 ms).
    let d0 = ws.div_euclid(DAY) as i32;
    let nd = ((we - 1).div_euclid(DAY) - ws.div_euclid(DAY) + 1).max(1) as usize;
    // user_daily rows: about one per 10 event lines; reserve ~1.5x a thread's share.
    let users_reserve = ev_bytes / 64 / 10 / threads * 3 / 2 + 16;
    let dstates = par(
        threads,
        Q,
        || DState { users: Vec::with_capacity(users_reserve), camp: CampDays::new(ncamp, d0, nd) },
        |st, q| {
            // Group by person in linear time: dense local person ids from a flat
            // table, a counting-sort scatter, then sort each person's few rows.
            let ne: usize = cstates_ref.iter().map(|c| c.sv[q].len()).sum();
            let cap = (ne.max(8) * 2).next_power_of_two();
            let mask = cap - 1;
            let mut slot = vec![u32::MAX; cap];
            let mut pkeys: Vec<u128> = Vec::new();
            let mut ecount: Vec<u32> = Vec::new();
            let mut epid: Vec<u32> = Vec::with_capacity(ne);
            let find = |slot: &[u32], pkeys: &[u128], k: u128| -> usize {
                let mut i = hk(k) as usize & mask;
                loop {
                    let v = slot[i];
                    if v == u32::MAX || pkeys[v as usize] == k {
                        return i;
                    }
                    i = (i + 1) & mask;
                }
            };
            for c in cstates_ref {
                for e in &c.sv[q] {
                    let i = find(&slot, &pkeys, e.person);
                    let p = if slot[i] == u32::MAX {
                        slot[i] = pkeys.len() as u32;
                        pkeys.push(e.person);
                        ecount.push(0);
                        pkeys.len() as u32 - 1
                    } else {
                        slot[i]
                    };
                    ecount[p as usize] += 1;
                    epid.push(p);
                }
            }
            let np = pkeys.len();
            let mut eoff: Vec<u32> = Vec::with_capacity(np + 1);
            let mut acc = 0u32;
            for &n in &ecount {
                eoff.push(acc);
                acc += n;
            }
            eoff.push(acc);
            let mut evs: Vec<Sv> = Vec::with_capacity(ne);
            {
                let mut pos: Vec<u32> = eoff[..np].to_vec();
                let dst = evs.spare_capacity_mut();
                let mut j = 0;
                for c in cstates_ref {
                    for e in &c.sv[q] {
                        let p = epid[j] as usize;
                        dst[pos[p] as usize].write(*e);
                        pos[p] += 1;
                        j += 1;
                    }
                }
                // Safety: the counts cover every slot 0..ne exactly once.
                unsafe { evs.set_len(ne) };
            }
            drop(epid);
            let mut ckp_pid: Vec<u32> = Vec::new();
            let mut ccount = vec![0u32; np];
            for c in cstates_ref {
                for k in &c.ck[q] {
                    let i = find(&slot, &pkeys, k.person);
                    let p = slot[i];
                    if p != u32::MAX {
                        ccount[p as usize] += 1;
                    }
                    ckp_pid.push(p);
                }
            }
            let mut coff: Vec<u32> = Vec::with_capacity(np + 1);
            let mut acc = 0u32;
            for &n in &ccount {
                coff.push(acc);
                acc += n;
            }
            coff.push(acc);
            let nc = acc as usize;
            let mut cks: Vec<Cp> = Vec::with_capacity(nc);
            {
                let mut pos: Vec<u32> = coff[..np].to_vec();
                let dst = cks.spare_capacity_mut();
                let mut j = 0;
                for c in cstates_ref {
                    for k in &c.ck[q] {
                        let p = ckp_pid[j];
                        j += 1;
                        if p != u32::MAX {
                            dst[pos[p as usize] as usize].write(*k);
                            pos[p as usize] += 1;
                        }
                    }
                }
                // Safety: the counts cover every slot 0..nc exactly once.
                unsafe { cks.set_len(nc) };
            }
            drop(ckp_pid);
            drop(slot);
            for p in 0..np {
                evs[eoff[p] as usize..eoff[p + 1] as usize].sort_unstable_by_key(|e| e.ts);
                cks[coff[p] as usize..coff[p + 1] as usize].sort_unstable_by(|a, b| (a.ts, a.cid).cmp(&(b.ts, b.cid)));
            }

            let mut sess: Vec<Sess> = Vec::new();
            let mut dend: Vec<usize> = Vec::new();
            let mut catsum = vec![0i64; ncat];
            let mut touched_cats: Vec<u16> = Vec::new();
            let mut cnt = vec![0u32; ncamp];
            let mut since = vec![0usize; ncamp];
            let mut touched: Vec<u32> = Vec::new();

            let flush = |row: &mut Option<UserRow>, catsum: &mut Vec<i64>, touched_cats: &mut Vec<u16>, users: &mut Vec<UserRow>| {
                if let Some(mut r) = row.take() {
                    let mut top: Option<u16> = None;
                    for &c in touched_cats.iter() {
                        let v = catsum[c as usize];
                        if v == 0 {
                            continue;
                        }
                        top = match top {
                            None => Some(c),
                            Some(t) => {
                                let tv = catsum[t as usize];
                                if v > tv || (v == tv && rank[c as usize] < rank[t as usize]) {
                                    Some(c)
                                } else {
                                    Some(t)
                                }
                            }
                        };
                    }
                    for &c in touched_cats.iter() {
                        catsum[c as usize] = 0;
                    }
                    touched_cats.clear();
                    r.top = top;
                    users.push(r);
                }
            };

            for p in 0..np {
                let person = pkeys[p];
                let evp = &evs[eoff[p] as usize..eoff[p + 1] as usize];
                let ckp = &cks[coff[p] as usize..coff[p + 1] as usize];

                // sessions and user_daily rows
                sess.clear();
                let mut prev = i64::MIN;
                let mut row: Option<UserRow> = None;
                for e in evp {
                    let ts = ws + e.ts as i64;
                    if sess.is_empty() || ts - prev > GAP {
                        let day = ts.div_euclid(DAY) as i32;
                        sess.push(Sess { start: ts, day, revenue: 0, purch: false });
                        if row.as_ref().map(|r| r.day != day).unwrap_or(true) {
                            flush(&mut row, &mut catsum, &mut touched_cats, &mut st.users);
                            row = Some(UserRow { person, day, sessions: 0, events: 0, revenue: 0, top: None });
                        }
                        row.as_mut().unwrap().sessions += 1;
                    }
                    prev = ts;
                    let r = row.as_mut().unwrap();
                    r.events += 1;
                    if e.kind == 3 {
                        let price = e.price as i64;
                        r.revenue += price;
                        let s = sess.last_mut().unwrap();
                        s.revenue += price;
                        s.purch = true;
                        if catsum[e.cat as usize] == 0 {
                            touched_cats.push(e.cat);
                        }
                        catsum[e.cat as usize] += price;
                    }
                }
                flush(&mut row, &mut catsum, &mut touched_cats, &mut st.users);

                // credit and assists
                let n = sess.len();
                dend.clear();
                dend.resize(n, 0);
                for k in (0..n).rev() {
                    dend[k] = if k + 1 < n && sess[k + 1].day == sess[k].day { dend[k + 1] } else { k + 1 };
                }
                let (mut lo, mut hi) = (0usize, 0usize);
                for k in 0..n {
                    let s = sess[k].start;
                    while hi < ckp.len() && ckp[hi].ts <= s {
                        let c = ckp[hi].camp as usize;
                        if cnt[c] == 0 {
                            since[c] = k;
                            touched.push(c as u32);
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && ckp[lo].ts < s - WEEK {
                        let c = ckp[lo].camp as usize;
                        cnt[c] -= 1;
                        if cnt[c] == 0 {
                            assists(&mut st.camp, &sess, &dend, c as u32, since[c], k);
                        }
                        lo += 1;
                    }
                    let credited = if lo < hi {
                        let mut j = hi - 1;
                        let t = ckp[j].ts;
                        while j > lo && ckp[j - 1].ts == t {
                            j -= 1;
                        }
                        ckp[j].camp
                    } else {
                        organic
                    };
                    let a = st.camp.at(credited, sess[k].day);
                    a[0] += 1;
                    a[1] += sess[k].purch as i64;
                    a[2] += sess[k].revenue;
                }
                for &c in &touched {
                    let c = c as usize;
                    if cnt[c] > 0 {
                        assists(&mut st.camp, &sess, &dend, c as u32, since[c], n);
                        cnt[c] = 0;
                    }
                }
                touched.clear();
            }
        },
    );
    log("sessions");

    // -------------------------------------------------- phase E: write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");

    let mut camp_all = CampDays::new(ncamp, d0, nd);
    for d in &dstates {
        for (a, v) in camp_all.v.iter_mut().zip(&d.camp.v) {
            for i in 0..4 {
                a[i] += v[i];
            }
        }
    }
    let camp_rows: Vec<((u32, i32), [i64; 4])> = camp_all
        .v
        .iter()
        .enumerate()
        .filter(|(_, a)| a[0] > 0 || a[3] > 0)
        .map(|(i, a)| (((i / nd) as u32, d0 + (i % nd) as i32), *a))
        .collect();
    let camp_rows = &camp_rows;
    let dstates_ref = &dstates;
    let cat_names = &cat_names;
    let (camp_ids, camp_channels) = (&camp_ids, &camp_channels);
    let long_names = &long_names;
    let ud = &ud;
    par(threads, dstates.len() + 1, || (), |_, i| {
        if i < dstates_ref.len() {
            let rows = &dstates_ref[i].users;
            if rows.is_empty() && i > 0 {
                return;
            }
            let mut arena: Vec<u8> = Vec::with_capacity(rows.len() * 12);
            let mut spans: Vec<(usize, usize)> = Vec::with_capacity(rows.len());
            for r in rows {
                let s = arena.len();
                if is_long(r.person) {
                    arena.extend_from_slice(long_names[&r.person]);
                } else {
                    unpack(r.person, &mut arena);
                }
                spans.push((s, arena.len()));
            }
            let cols = vec![
                ("user_id", parquet::Col::Str(spans.iter().map(|&(a, b)| &arena[a..b]).collect())),
                ("day", parquet::Col::Date(rows.iter().map(|r| r.day).collect())),
                ("sessions", parquet::Col::I64(rows.iter().map(|r| r.sessions).collect())),
                ("events", parquet::Col::I64(rows.iter().map(|r| r.events).collect())),
                ("revenue_cents", parquet::Col::I64(rows.iter().map(|r| r.revenue).collect())),
                (
                    "top_category",
                    parquet::Col::OptStr(rows.iter().map(|r| r.top.map(|c| cat_names[c as usize])).collect()),
                ),
            ];
            parquet::write(&ud.join(format!("part-{i:03}.parquet")), &cols).expect("write user_daily");
        } else {
            let rows = camp_rows;
            let cols = vec![
                ("campaign_id", parquet::Col::Str(rows.iter().map(|r| camp_ids[r.0 .0 as usize]).collect())),
                ("channel", parquet::Col::Str(rows.iter().map(|r| camp_channels[r.0 .0 as usize]).collect())),
                ("day", parquet::Col::Date(rows.iter().map(|r| r.0 .1).collect())),
                ("sessions", parquet::Col::I64(rows.iter().map(|r| r.1[0]).collect())),
                ("purchasing_sessions", parquet::Col::I64(rows.iter().map(|r| r.1[1]).collect())),
                ("revenue_cents", parquet::Col::I64(rows.iter().map(|r| r.1[2]).collect())),
                ("assisted_sessions", parquet::Col::I64(rows.iter().map(|r| r.1[3]).collect())),
            ];
            parquet::write(&cd.join("part-000.parquet"), &cols).expect("write campaign_daily");
        }
    });
    log("written");
    // Every output file is written, flushed and closed (parquet::write drops
    // its writer). Exit now: freeing gigabytes of buffers one by one on this
    // thread would add ~10% to the run, and the kernel reclaims it all anyway.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The byte-by-byte reference the SWAR `datetime` replaced.
    fn datetime_ref(s: &[u8]) -> Option<i64> {
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let y = d2(s, 0)? * 100 + d2(s, 2)?;
        let mo = d2(s, 5)?;
        let d = d2(s, 8)?;
        let h = d2(s, 11)?;
        let mi = d2(s, 14)?;
        let se = d2(s, 17)?;
        if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dim = match mo {
            2 => if leap { 29 } else { 28 },
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if d > dim {
            return None;
        }
        let days = days_from_civil(y as i64, mo, d);
        Some(((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + se as i64 * 1000)
    }

    #[test]
    fn swar_datetime_matches_reference() {
        let seeds: [&[u8; 19]; 6] = [
            b"2026-10-03T23:58:10",
            b"2028-02-29T00:00:00",
            b"2027-02-29T12:30:59",
            b"0000-01-01T00:00:00",
            b"9999-12-31T23:59:59",
            b"2026-13-40T24:60:60",
        ];
        let pool: &[u8] = b"0123456789-T:tZz /\x00\xff\x2f\x3a\x40\x39\x30\xfa\xc0";
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for i in 0..2_000_000u64 {
            let mut s = *seeds[(i % seeds.len() as u64) as usize];
            for _ in 0..(next() % 4) {
                let p = (next() % 19) as usize;
                s[p] = if next() % 2 == 0 { pool[(next() % pool.len() as u64) as usize] } else { next() as u8 };
            }
            assert_eq!(datetime(&s), datetime_ref(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        // Every valid instant over a few years, day by day at several times.
        for y in [1999u32, 2000, 2024, 2025, 2028, 2100] {
            for mo in 1..=12u32 {
                for d in 1..=31u32 {
                    let s = format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}", d % 24, (d * 7) % 60, (d * 13) % 60);
                    assert_eq!(datetime(s.as_bytes()), datetime_ref(s.as_bytes()), "{s}");
                }
            }
        }
    }
}
