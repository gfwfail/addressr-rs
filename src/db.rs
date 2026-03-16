use anyhow::Result;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

#[derive(Debug, Serialize)]
pub struct AddressResult {
    pub pid: String,
    pub sla: String,
    pub locality: String,
    pub state: String,
    pub postcode: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// A simple connection pool that reuses SQLite connections.
pub struct ConnPool {
    conns: Mutex<Vec<Connection>>,
    db_path: std::path::PathBuf,
    max_size: usize,
}

impl ConnPool {
    pub fn new(db_path: &Path, max_size: usize) -> Result<Self> {
        let mut conns = Vec::with_capacity(max_size);
        // Pre-create one connection
        conns.push(open_conn(db_path)?);
        Ok(Self {
            conns: Mutex::new(conns),
            db_path: db_path.to_path_buf(),
            max_size,
        })
    }

    pub fn get(&self) -> Result<PooledConn<'_>> {
        let conn = {
            let mut pool = self.conns.lock().unwrap();
            pool.pop()
        };
        let conn = match conn {
            Some(c) => c,
            None => open_conn(&self.db_path)?,
        };
        Ok(PooledConn { pool: self, conn: Some(conn) })
    }

    fn return_conn(&self, conn: Connection) {
        let mut pool = self.conns.lock().unwrap();
        if pool.len() < self.max_size {
            pool.push(conn);
        }
        // else drop the connection
    }
}

pub struct PooledConn<'a> {
    pool: &'a ConnPool,
    conn: Option<Connection>,
}

impl<'a> std::ops::Deref for PooledConn<'a> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

impl<'a> Drop for PooledConn<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_conn(conn);
        }
    }
}

fn open_conn(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA mmap_size=268435456;
         PRAGMA cache_size=-64000;",
    )?;
    Ok(conn)
}

pub fn open(db_path: &Path) -> Result<Connection> {
    open_conn(db_path)
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS authority_code (
            category TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            PRIMARY KEY (category, code)
        );

        CREATE TABLE IF NOT EXISTS locality (
            locality_pid TEXT PRIMARY KEY,
            locality_name TEXT NOT NULL,
            state_pid TEXT,
            primary_postcode TEXT,
            locality_class_code TEXT
        );

        CREATE TABLE IF NOT EXISTS street_locality (
            street_locality_pid TEXT PRIMARY KEY,
            street_name TEXT NOT NULL,
            street_type_code TEXT,
            street_suffix_code TEXT,
            locality_pid TEXT,
            street_class_code TEXT
        );

        CREATE TABLE IF NOT EXISTS state (
            state_pid TEXT PRIMARY KEY,
            state_name TEXT NOT NULL,
            state_abbreviation TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS address (
            address_detail_pid TEXT PRIMARY KEY,
            building_name TEXT,
            flat_type_code TEXT,
            flat_number TEXT,
            flat_number_prefix TEXT,
            flat_number_suffix TEXT,
            level_type_code TEXT,
            level_number INTEGER,
            level_number_prefix TEXT,
            level_number_suffix TEXT,
            number_first INTEGER,
            number_first_prefix TEXT,
            number_first_suffix TEXT,
            number_last INTEGER,
            number_last_prefix TEXT,
            number_last_suffix TEXT,
            lot_number TEXT,
            lot_number_prefix TEXT,
            lot_number_suffix TEXT,
            street_locality_pid TEXT,
            locality_pid TEXT,
            postcode TEXT,
            confidence INTEGER,
            state_abbreviation TEXT NOT NULL,
            -- precomputed full address text
            sla TEXT NOT NULL,
            latitude REAL,
            longitude REAL
        );

        -- FTS5 virtual table for full-text search
        CREATE VIRTUAL TABLE IF NOT EXISTS address_fts USING fts5(
            sla,
            content='address',
            content_rowid='rowid'
        );

        -- Triggers to keep FTS in sync
        CREATE TRIGGER IF NOT EXISTS address_ai AFTER INSERT ON address BEGIN
            INSERT INTO address_fts(rowid, sla) VALUES (new.rowid, new.sla);
        END;
        ",
    )?;
    Ok(())
}

pub fn search_with_pool(pool: &ConnPool, query: &str, limit: usize, offset: usize) -> Result<Vec<AddressResult>> {
    let conn = pool.get()?;
    search_with_conn(&conn, query, limit, offset)
}

pub fn search(db_path: &Path, query: &str, limit: usize, offset: usize) -> Result<Vec<AddressResult>> {
    let conn = open(db_path)?;
    search_with_conn(&conn, query, limit, offset)
}

fn search_with_conn(conn: &Connection, query: &str, limit: usize, offset: usize) -> Result<Vec<AddressResult>> {
    // Build FTS5 query: tokenize input words and join with AND
    let fts_query: String = query
        .split_whitespace()
        .map(|w| {
            let clean: String = w.chars().filter(|c| c.is_alphanumeric()).collect();
            format!("\"{}\"*", clean)
        })
        .collect::<Vec<_>>()
        .join(" ");

    let mut stmt = conn.prepare_cached(
        "SELECT a.address_detail_pid, a.sla, l.locality_name, a.state_abbreviation,
                COALESCE(a.postcode, ''), a.latitude, a.longitude
         FROM address_fts f
         JOIN address a ON a.rowid = f.rowid
         LEFT JOIN locality l ON a.locality_pid = l.locality_pid
         WHERE address_fts MATCH ?1
         ORDER BY rank
         LIMIT ?2 OFFSET ?3",
    )?;

    let results = stmt
        .query_map(params![fts_query, limit as i64, offset as i64], |row| {
            Ok(AddressResult {
                pid: row.get(0)?,
                sla: row.get(1)?,
                locality: row.get(2)?,
                state: row.get(3)?,
                postcode: row.get(4)?,
                latitude: row.get(5)?,
                longitude: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(results)
}
