//! Shopping Sessions pipeline (rules.md), one native pass with no dependencies.
//!
//! Phase A  parse every CSV chunk in parallel: events (partitioned by event_id),
//!          valid clicks, link pairs.
//! Phase B  union-find over the links; each ID maps to its person's name.
//! Phase C  dedup events per event_id partition (earliest ingest_ts wins, then
//!          validity of that copy decides), map events and clicks to persons and
//!          repartition both by person.
//! Phase D  per person partition: sessionize, credit, assists, daily rows.
//! Phase E  write both tables as Parquet.

mod parquet;

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const P: usize = 256; // event_id partitions
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

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = (b.len() as u64) ^ K0;
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

/// Partition index, from bits independent of the ones hash maps use.
#[inline]
fn part(b: &[u8], n: usize) -> usize {
    ((mum(hash_bytes(b), K0) >> 32) as usize) % n
}

#[derive(Default, Clone, Copy)]
struct FastHasher(u64);
impl Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, b: &[u8]) {
        self.0 = mum(self.0 ^ hash_bytes(b), K1);
    }
    #[inline]
    fn write_u8(&mut self, v: u8) {
        self.0 = mum(self.0 ^ v as u64 ^ K2, K1);
    }
    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.0 = mum(self.0 ^ v as u64 ^ K2, K1);
    }
    #[inline]
    fn write_i32(&mut self, v: i32) {
        self.0 = mum(self.0 ^ v as u32 as u64 ^ K0, K1);
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = mum(self.0 ^ v ^ K2, K1);
    }
    #[inline]
    fn write_usize(&mut self, v: usize) {
        self.0 = mum(self.0 ^ v as u64 ^ K0, K2);
    }
}
type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;

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
#[inline]
fn datetime(s: &[u8]) -> Option<i64> {
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

#[derive(Clone, Copy)]
struct Ev<'a> {
    eid: &'a [u8],
    user: &'a [u8],
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8, // 0 invalid, 1 view, 2 cart, 3 purchase
}

#[derive(Clone, Copy)]
struct Ck<'a> {
    cid: &'a [u8],
    user: &'a [u8],
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Sv<'a> {
    person: &'a [u8],
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Cp<'a> {
    person: &'a [u8],
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

/// Campaign `c` assisted sessions a..b (in start order): add them per day.
fn assists(camp: &mut FastMap<(u32, i32), [i64; 4]>, sess: &[Sess], dend: &[usize], c: u32, a: usize, b: usize) {
    let mut i = a;
    while i < b {
        let e = dend[i].min(b);
        camp.entry((c, sess[i].day)).or_insert([0; 4])[3] += (e - i) as i64;
        i = e;
    }
}

struct UserRow<'a> {
    person: &'a [u8],
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
    let mut cat_of_name: FastMap<&[u8], u16> = FastMap::default();
    let mut product_cat: FastMap<&[u8], u16> = FastMap::default();
    {
        let b = products_buf.bytes();
        let body = memchr(b'\n', b).map(|p| &b[p + 1..]).unwrap_or(&[]);
        for line in body.split(|&c| c == b'\n') {
            let mut f = [&b""[..]; 2];
            if !fields(line, &mut f) {
                continue;
            }
            let next = cat_names.len() as u16;
            let c = *cat_of_name.entry(f[1]).or_insert_with(|| {
                cat_names.push(f[1]);
                next
            });
            product_cat.entry(f[0]).or_insert(c);
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
    let mut camp_of: FastMap<&[u8], u32> = FastMap::default();
    {
        let b = campaigns_buf.bytes();
        let body = memchr(b'\n', b).map(|p| &b[p + 1..]).unwrap_or(&[]);
        for line in body.split(|&c| c == b'\n') {
            let mut f = [&b""[..]; 2];
            if !fields(line, &mut f) || f[0].is_empty() {
                continue;
            }
            if !camp_of.contains_key(f[0]) {
                camp_of.insert(f[0], camp_ids.len() as u32);
                camp_ids.push(f[0]);
                camp_channels.push(f[1]);
            }
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
    let chunk_list: Vec<(u8, SendPtr)> = raw_chunks.into_iter().map(|(k, p, l)| (k, SendPtr(p, l))).collect();
    log(&format!("{} files, {} chunks, {} threads", paths.len(), chunk_list.len(), threads));

    // -------------------------------------------------- phase A: parse
    struct AState<'a> {
        ev: Vec<Vec<Ev<'a>>>,
        ck: Vec<Ck<'a>>,
        lk: Vec<(&'a [u8], &'a [u8])>,
    }
    let (cw_lo, cw_hi) = (ws - WEEK, we);
    let product_cat = &product_cat;
    let camp_of = &camp_of;
    let astates = par(
        threads,
        chunk_list.len(),
        || AState { ev: (0..P).map(|_| Vec::new()).collect(), ck: Vec::new(), lk: Vec::new() },
        |st, i| {
            let (kind, ref sp) = chunk_list[i];
            // The buffers live until main returns.
            let body: &[u8] = unsafe { std::slice::from_raw_parts(sp.0, sp.1) };
            match kind {
                0 => {
                    for line in body.split(|&c| c == b'\n') {
                        let mut f = [&b""[..]; 7];
                        if !fields(line, &mut f) || f[0].is_empty() {
                            continue;
                        }
                        let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                        let mut ev = Ev { eid: f[0], user: f[1], ingest, ts: 0, price: 0, cat: 0, kind: 0 };
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
                                        ev.ts = ts;
                                        ev.kind = k;
                                        ev.price = price;
                                        ev.cat = *product_cat.get(f[5]).unwrap_or(&unknown);
                                    }
                                }
                            }
                        }
                        st.ev[part(f[0], P)].push(ev);
                    }
                }
                1 => {
                    for line in body.split(|&c| c == b'\n') {
                        let mut f = [&b""[..]; 4];
                        if !fields(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                            continue;
                        }
                        let Some(ts) = parse_ts(f[3]) else { continue };
                        if ts < cw_lo || ts >= cw_hi {
                            continue;
                        }
                        let Some(&camp) = camp_of.get(f[2]) else { continue };
                        st.ck.push(Ck { cid: f[0], user: f[1], ts, camp });
                    }
                }
                _ => {
                    for line in body.split(|&c| c == b'\n') {
                        let mut f = [&b""[..]; 2];
                        if !fields(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                            continue;
                        }
                        st.lk.push((f[0], f[1]));
                    }
                }
            }
        },
    );
    log("parsed");

    // -------------------------------------------------- phase B: links
    let mut id_of: FastMap<&[u8], u32> = FastMap::default();
    let mut names: Vec<&[u8]> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for st in &astates {
        for &(a, b) in &st.lk {
            let mut id = |s: &'_ [u8]| -> u32 {
                *id_of.entry(unsafe { std::mem::transmute::<&[u8], &[u8]>(s) }).or_insert_with(|| {
                    names.push(unsafe { std::mem::transmute::<&[u8], &[u8]>(s) });
                    parent.push(parent.len() as u32);
                    (names.len() - 1) as u32
                })
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
    let mut best: Vec<u32> = (0..names.len() as u32).collect();
    for i in 0..names.len() as u32 {
        let r = find(&mut parent, i);
        if names[i as usize] < names[best[r as usize] as usize] {
            best[r as usize] = i;
        }
    }
    let mut person_of: FastMap<&[u8], &[u8]> = FastMap::default();
    person_of.reserve(names.len());
    for i in 0..names.len() as u32 {
        let r = find(&mut parent, i);
        person_of.insert(names[i as usize], names[best[r as usize] as usize]);
    }
    let person_of = &person_of;
    log(&format!("links: {} ids", names.len()));

    // -------------------------------------------------- phase C: dedup + repartition
    struct CState<'a> {
        sv: Vec<Vec<Sv<'a>>>,
        ck: Vec<Vec<Cp<'a>>>,
        rows: Vec<&'a Ev<'a>>,
        best: FastMap<&'a [u8], u32>,
    }
    let nst = astates.len();
    let astates_ref = &astates;
    let cstates = par(
        threads,
        P + nst,
        || CState {
            sv: (0..Q).map(|_| Vec::new()).collect(),
            ck: (0..Q).map(|_| Vec::new()).collect(),
            rows: Vec::new(),
            best: FastMap::default(),
        },
        |st, i| {
            fn person<'a>(m: &FastMap<&'a [u8], &'a [u8]>, u: &'a [u8]) -> &'a [u8] {
                m.get(u).copied().unwrap_or(u)
            }
            let lookup = |u| person(person_of, u);
            if i < P {
                let rows: &mut Vec<&Ev> = unsafe { std::mem::transmute(&mut st.rows) };
                rows.clear();
                for a in astates_ref {
                    rows.extend(a.ev[i].iter());
                }
                let best: &mut FastMap<&[u8], u32> = unsafe { std::mem::transmute(&mut st.best) };
                best.clear();
                best.reserve(rows.len());
                for (j, r) in rows.iter().enumerate() {
                    match best.entry(r.eid) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert(j as u32);
                        }
                        std::collections::hash_map::Entry::Occupied(mut o) => {
                            if r.ingest < rows[*o.get() as usize].ingest {
                                o.insert(j as u32);
                            }
                        }
                    }
                }
                for &j in best.values() {
                    let r = rows[j as usize];
                    if r.kind == 0 {
                        continue;
                    }
                    let person = lookup(r.user);
                    st.sv[part(person, Q)].push(Sv { person, ts: r.ts, price: r.price, cat: r.cat, kind: r.kind });
                }
            } else {
                for c in &astates_ref[i - P].ck {
                    let person = lookup(c.user);
                    st.ck[part(person, Q)].push(Cp { person, cid: c.cid, ts: c.ts, camp: c.camp });
                }
            }
        },
    );
    log("deduped");

    // -------------------------------------------------- phase D: sessions
    struct DState<'a> {
        users: Vec<UserRow<'a>>,
        camp: FastMap<(u32, i32), [i64; 4]>,
    }
    let cstates_ref = &cstates;
    let rank = &rank;
    let ncat = cat_names.len();
    let dstates = par(
        threads,
        Q,
        || DState { users: Vec::new(), camp: FastMap::default() },
        |st, q| {
            let mut pid_of: FastMap<&[u8], u32> = FastMap::default();
            let mut persons: Vec<&[u8]> = Vec::new();
            let mut evs: Vec<(u32, i64, u32, u16, u8)> = Vec::new();
            for c in cstates_ref {
                for s in &c.sv[q] {
                    let next = persons.len() as u32;
                    let id = *pid_of.entry(s.person).or_insert_with(|| {
                        persons.push(s.person);
                        next
                    });
                    evs.push((id, s.ts, s.price, s.cat, s.kind));
                }
            }
            let mut cks: Vec<(u32, i64, &[u8], u32)> = Vec::new();
            for c in cstates_ref {
                for k in &c.ck[q] {
                    if let Some(&id) = pid_of.get(k.person) {
                        cks.push((id, k.ts, k.cid, k.camp));
                    }
                }
            }
            evs.sort_unstable_by_key(|e| (e.0, e.1));
            cks.sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));

            let mut sess: Vec<Sess> = Vec::new();
            let mut dend: Vec<usize> = Vec::new();
            let mut catsum = vec![0i64; ncat];
            let mut touched_cats: Vec<u16> = Vec::new();
            let mut cnt = vec![0u32; ncamp];
            let mut since = vec![0usize; ncamp];
            let mut touched: Vec<u32> = Vec::new();

            let (mut ei, mut ci) = (0usize, 0usize);
            while ei < evs.len() {
                let pid = evs[ei].0;
                let mut ej = ei;
                while ej < evs.len() && evs[ej].0 == pid {
                    ej += 1;
                }
                while ci < cks.len() && cks[ci].0 < pid {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cks.len() && cks[cj].0 == pid {
                    cj += 1;
                }
                let person = persons[pid as usize];
                let evp = &evs[ei..ej];
                let ckp = &cks[ci..cj];
                ei = ej;
                ci = cj;

                // sessions and user_daily rows
                sess.clear();
                let mut prev = i64::MIN;
                let mut row: Option<UserRow> = None;
                let flush = |row: &mut Option<UserRow<'_>>, catsum: &mut Vec<i64>, touched_cats: &mut Vec<u16>, users: &mut Vec<UserRow<'_>>| {
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
                        // Safety: rows borrow input buffers that outlive main's work.
                        users.push(unsafe { std::mem::transmute::<UserRow<'_>, UserRow<'_>>(r) });
                    }
                };
                for &(_, ts, price, cat, kind) in evp {
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
                    if kind == 3 {
                        r.revenue += price as i64;
                        let s = sess.last_mut().unwrap();
                        s.revenue += price as i64;
                        s.purch = true;
                        if catsum[cat as usize] == 0 {
                            touched_cats.push(cat);
                        }
                        catsum[cat as usize] += price as i64;
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
                    while hi < ckp.len() && ckp[hi].1 <= s {
                        let c = ckp[hi].3 as usize;
                        if cnt[c] == 0 {
                            since[c] = k;
                            touched.push(c as u32);
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && ckp[lo].1 < s - WEEK {
                        let c = ckp[lo].3 as usize;
                        cnt[c] -= 1;
                        if cnt[c] == 0 {
                            assists(&mut st.camp, &sess, &dend, c as u32, since[c], k);
                        }
                        lo += 1;
                    }
                    let credited = if lo < hi {
                        let mut j = hi - 1;
                        let t = ckp[j].1;
                        while j > lo && ckp[j - 1].1 == t {
                            j -= 1;
                        }
                        ckp[j].3
                    } else {
                        organic
                    };
                    let a = st.camp.entry((credited, sess[k].day)).or_insert([0; 4]);
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

    let mut camp_all: FastMap<(u32, i32), [i64; 4]> = FastMap::default();
    for d in &dstates {
        for (k, v) in &d.camp {
            let a = camp_all.entry(*k).or_insert([0; 4]);
            for i in 0..4 {
                a[i] += v[i];
            }
        }
    }
    let dstates_ref = &dstates;
    let cat_names = &cat_names;
    let (camp_ids, camp_channels) = (&camp_ids, &camp_channels);
    let ud = &ud;
    par(threads, dstates.len() + 1, || (), |_, i| {
        if i < dstates_ref.len() {
            let rows = &dstates_ref[i].users;
            if rows.is_empty() && i > 0 {
                return;
            }
            let cols = vec![
                ("user_id", parquet::Col::Str(rows.iter().map(|r| r.person).collect())),
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
            let rows: Vec<(&(u32, i32), &[i64; 4])> = camp_all.iter().collect();
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
    drop(bufs);
    drop(products_buf);
    drop(campaigns_buf);
}
