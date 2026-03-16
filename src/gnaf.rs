use anyhow::{Context, Result};
use csv::ReaderBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rusqlite::params;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::db;

const STATES: &[&str] = &["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"];

pub fn load(gnaf_dir: &Path, db_path: &Path, state_filter: Option<&[String]>) -> Result<()> {
    // Find the actual G-NAF data directory
    let data_dir = find_gnaf_data_dir(gnaf_dir)?;
    let standard_dir = data_dir.join("Standard");
    let authority_dir = data_dir.join("Authority Code");

    println!("G-NAF data dir: {}", data_dir.display());
    println!("Database: {}", db_path.display());

    let conn = db::open(db_path)?;
    db::init_schema(&conn)?;

    // Load authority codes
    println!("\n=== Loading authority codes ===");
    load_authority_codes(&conn, &authority_dir)?;

    // Build lookup maps
    let flat_types = load_code_map(&conn, "FLAT_TYPE")?;
    let level_types = load_code_map(&conn, "LEVEL_TYPE")?;
    let street_types = load_code_map(&conn, "STREET_TYPE")?;
    let street_suffixes = load_code_map(&conn, "STREET_SUFFIX")?;

    let states_to_load: Vec<&str> = match state_filter {
        Some(filter) => STATES
            .iter()
            .filter(|s| filter.iter().any(|f| f == **s))
            .copied()
            .collect(),
        None => STATES.to_vec(),
    };

    println!(
        "\n=== Loading states: {} ===",
        states_to_load.join(", ")
    );

    // Use a large transaction for performance
    conn.execute_batch("PRAGMA cache_size=-2000000;")?; // 2GB cache

    for state in &states_to_load {
        println!("\n--- {} ---", state);

        // Load state info
        load_state_file(&conn, &standard_dir, state)?;

        // Load localities
        load_localities(&conn, &standard_dir, state)?;

        // Load street localities
        load_street_localities(&conn, &standard_dir, state)?;

        // Build in-memory lookups for this state
        let localities = query_localities(&conn, state)?;
        let streets = query_streets(&conn, state)?;
        let state_abbrev = state.to_string();

        // Load geocodes into memory
        let geocodes = load_geocodes(&standard_dir, state)?;

        // Load and index addresses
        load_addresses(
            &conn,
            &standard_dir,
            state,
            &state_abbrev,
            &localities,
            &streets,
            &geocodes,
            &flat_types,
            &level_types,
            &street_types,
            &street_suffixes,
        )?;
    }

    // Optimize FTS index
    println!("\n=== Optimizing search index ===");
    conn.execute("INSERT INTO address_fts(address_fts) VALUES('optimize')", [])?;
    println!("Done!");

    Ok(())
}

fn find_gnaf_data_dir(gnaf_dir: &Path) -> Result<PathBuf> {
    // Try to find the G-NAF data directory with the dated subfolder
    let gnaf_sub = gnaf_dir.join("G-NAF");
    let search_dir = if gnaf_sub.exists() { &gnaf_sub } else { gnaf_dir };

    for entry in std::fs::read_dir(search_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("G-NAF") && entry.file_type()?.is_dir() {
            // This is the dated directory like "G-NAF FEBRUARY 2026"
            return Ok(entry.path());
        }
    }

    // Check if Standard dir exists directly
    if search_dir.join("Standard").exists() {
        return Ok(search_dir.to_path_buf());
    }

    anyhow::bail!(
        "Cannot find G-NAF data directory in {}. Expected a subdirectory like 'G-NAF FEBRUARY 2026' with 'Standard' and 'Authority Code' folders.",
        gnaf_dir.display()
    )
}

fn read_psv(path: &Path) -> Result<csv::Reader<std::fs::File>> {
    let reader = ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    Ok(reader)
}

fn load_authority_codes(conn: &rusqlite::Connection, dir: &Path) -> Result<()> {
    let pattern = dir.join("Authority_Code_*_psv.psv");
    let files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
        .filter_map(|r| r.ok())
        .collect();

    conn.execute("BEGIN", [])?;
    for file in &files {
        let fname = file.file_stem().unwrap().to_string_lossy();
        // Extract category: "Authority_Code_FLAT_TYPE_AUT_psv" -> "FLAT_TYPE"
        let category = fname
            .strip_prefix("Authority_Code_")
            .and_then(|s| s.strip_suffix("_AUT_psv"))
            .unwrap_or(&fname);

        let mut rdr = read_psv(file)?;
        let mut count = 0u64;
        for result in rdr.records() {
            let record = result?;
            let code = record.get(0).unwrap_or("");
            let name = record.get(1).unwrap_or("");
            let desc = record.get(2).unwrap_or("");
            conn.execute(
                "INSERT OR REPLACE INTO authority_code (category, code, name, description) VALUES (?1, ?2, ?3, ?4)",
                params![category, code, name, desc],
            )?;
            count += 1;
        }
        println!("  {} -> {} codes", category, count);
    }
    conn.execute("COMMIT", [])?;
    Ok(())
}

fn load_code_map(conn: &rusqlite::Connection, category: &str) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT code, name FROM authority_code WHERE category = ?1")?;
    let map: HashMap<String, String> = stmt
        .query_map(params![category], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(map)
}

fn load_state_file(conn: &rusqlite::Connection, dir: &Path, state: &str) -> Result<()> {
    let file = dir.join(format!("{}_STATE_psv.psv", state));
    let mut rdr = read_psv(&file)?;
    conn.execute("BEGIN", [])?;
    for result in rdr.records() {
        let r = result?;
        conn.execute(
            "INSERT OR REPLACE INTO state (state_pid, state_name, state_abbreviation) VALUES (?1, ?2, ?3)",
            params![r.get(0).unwrap_or(""), r.get(3).unwrap_or(""), r.get(4).unwrap_or("")],
        )?;
    }
    conn.execute("COMMIT", [])?;
    Ok(())
}

fn load_localities(conn: &rusqlite::Connection, dir: &Path, state: &str) -> Result<()> {
    let file = dir.join(format!("{}_LOCALITY_psv.psv", state));
    let mut rdr = read_psv(&file)?;
    let mut count = 0u64;
    conn.execute("BEGIN", [])?;
    for result in rdr.records() {
        let r = result?;
        // LOCALITY_PID|DATE_CREATED|DATE_RETIRED|LOCALITY_NAME|PRIMARY_POSTCODE|LOCALITY_CLASS_CODE|STATE_PID|...
        conn.execute(
            "INSERT OR REPLACE INTO locality (locality_pid, locality_name, primary_postcode, locality_class_code, state_pid) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                r.get(0).unwrap_or(""),
                r.get(3).unwrap_or(""),
                r.get(4).unwrap_or(""),
                r.get(5).unwrap_or(""),
                r.get(6).unwrap_or(""),
            ],
        )?;
        count += 1;
    }
    conn.execute("COMMIT", [])?;
    println!("  Localities: {}", count);
    Ok(())
}

fn load_street_localities(conn: &rusqlite::Connection, dir: &Path, state: &str) -> Result<()> {
    let file = dir.join(format!("{}_STREET_LOCALITY_psv.psv", state));
    let mut rdr = read_psv(&file)?;
    let mut count = 0u64;
    conn.execute("BEGIN", [])?;
    for result in rdr.records() {
        let r = result?;
        // STREET_LOCALITY_PID|DATE_CREATED|DATE_RETIRED|STREET_CLASS_CODE|STREET_NAME|STREET_TYPE_CODE|STREET_SUFFIX_CODE|LOCALITY_PID|...
        conn.execute(
            "INSERT OR REPLACE INTO street_locality (street_locality_pid, street_class_code, street_name, street_type_code, street_suffix_code, locality_pid) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                r.get(0).unwrap_or(""),
                r.get(3).unwrap_or(""),
                r.get(4).unwrap_or(""),
                r.get(5).unwrap_or(""),
                r.get(6).unwrap_or(""),
                r.get(7).unwrap_or(""),
            ],
        )?;
        count += 1;
    }
    conn.execute("COMMIT", [])?;
    println!("  Streets: {}", count);
    Ok(())
}

struct LocalityInfo {
    name: String,
}

struct StreetInfo {
    name: String,
    type_code: String,
    suffix_code: String,
}

fn query_localities(
    conn: &rusqlite::Connection,
    _state: &str,
) -> Result<HashMap<String, LocalityInfo>> {
    let mut stmt = conn.prepare("SELECT locality_pid, locality_name FROM locality")?;
    let map = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                LocalityInfo {
                    name: row.get::<_, String>(1)?,
                },
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(map)
}

fn query_streets(
    conn: &rusqlite::Connection,
    _state: &str,
) -> Result<HashMap<String, StreetInfo>> {
    let mut stmt = conn.prepare(
        "SELECT street_locality_pid, street_name, COALESCE(street_type_code,''), COALESCE(street_suffix_code,'') FROM street_locality",
    )?;
    let map = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                StreetInfo {
                    name: row.get::<_, String>(1)?,
                    type_code: row.get::<_, String>(2)?,
                    suffix_code: row.get::<_, String>(3)?,
                },
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(map)
}

fn load_geocodes(dir: &Path, state: &str) -> Result<HashMap<String, (f64, f64)>> {
    let file = dir.join(format!("{}_ADDRESS_DEFAULT_GEOCODE_psv.psv", state));
    let mut rdr = read_psv(&file)?;
    let mut map = HashMap::new();
    for result in rdr.records() {
        let r = result?;
        // ADDRESS_DEFAULT_GEOCODE_PID|DATE_CREATED|DATE_RETIRED|ADDRESS_DETAIL_PID|GEOCODE_TYPE_CODE|LONGITUDE|LATITUDE
        let pid = r.get(3).unwrap_or("").to_string();
        let lon: f64 = r.get(5).unwrap_or("0").parse().unwrap_or(0.0);
        let lat: f64 = r.get(6).unwrap_or("0").parse().unwrap_or(0.0);
        if lat != 0.0 && lon != 0.0 {
            map.insert(pid, (lat, lon));
        }
    }
    println!("  Geocodes: {}", map.len());
    Ok(map)
}

#[allow(clippy::too_many_arguments)]
fn load_addresses(
    conn: &rusqlite::Connection,
    dir: &Path,
    state: &str,
    state_abbrev: &str,
    localities: &HashMap<String, LocalityInfo>,
    streets: &HashMap<String, StreetInfo>,
    geocodes: &HashMap<String, (f64, f64)>,
    flat_types: &HashMap<String, String>,
    level_types: &HashMap<String, String>,
    street_types: &HashMap<String, String>,
    street_suffixes: &HashMap<String, String>,
) -> Result<()> {
    let file = dir.join(format!("{}_ADDRESS_DETAIL_psv.psv", state));

    // Count lines for progress bar
    let line_count = count_lines(&file)?;
    let pb = ProgressBar::new(line_count as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  Addresses: [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("##-"),
    );

    let mut rdr = read_psv(&file)?;
    let mut insert_stmt = conn.prepare(
        "INSERT OR REPLACE INTO address (
            address_detail_pid, building_name,
            flat_type_code, flat_number, flat_number_prefix, flat_number_suffix,
            level_type_code, level_number, level_number_prefix, level_number_suffix,
            number_first, number_first_prefix, number_first_suffix,
            number_last, number_last_prefix, number_last_suffix,
            lot_number, lot_number_prefix, lot_number_suffix,
            street_locality_pid, locality_pid, postcode, confidence,
            state_abbreviation, sla, latitude, longitude
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27)",
    )?;

    conn.execute("BEGIN", [])?;
    let mut count = 0u64;
    let batch_size = 50_000;

    for result in rdr.records() {
        let r = result?;
        // Skip retired addresses
        let date_retired = r.get(3).unwrap_or("");
        if !date_retired.is_empty() {
            pb.inc(1);
            continue;
        }

        let pid = r.get(0).unwrap_or("");
        let building_name = r.get(4).unwrap_or("");
        let lot_number_prefix = r.get(5).unwrap_or("");
        let lot_number = r.get(6).unwrap_or("");
        let lot_number_suffix = r.get(7).unwrap_or("");
        let flat_type_code = r.get(8).unwrap_or("");
        let flat_number_prefix = r.get(9).unwrap_or("");
        let flat_number = r.get(10).unwrap_or("");
        let flat_number_suffix = r.get(11).unwrap_or("");
        let level_type_code = r.get(12).unwrap_or("");
        let level_number_prefix = r.get(13).unwrap_or("");
        let level_number: Option<i32> = r.get(14).unwrap_or("").parse().ok();
        let level_number_suffix = r.get(15).unwrap_or("");
        let number_first_prefix = r.get(16).unwrap_or("");
        let number_first: Option<i32> = r.get(17).unwrap_or("").parse().ok();
        let number_first_suffix = r.get(18).unwrap_or("");
        let number_last_prefix = r.get(19).unwrap_or("");
        let number_last: Option<i32> = r.get(20).unwrap_or("").parse().ok();
        let number_last_suffix = r.get(21).unwrap_or("");
        let street_locality_pid = r.get(22).unwrap_or("");
        let locality_pid = r.get(24).unwrap_or("");
        let postcode = r.get(26).unwrap_or("");
        let confidence: Option<i32> = r.get(29).unwrap_or("").parse().ok();

        // Build SLA (single line address)
        let sla = build_sla(
            building_name,
            flat_type_code, flat_number_prefix, flat_number, flat_number_suffix,
            level_type_code, level_number_prefix, level_number, level_number_suffix,
            number_first_prefix, number_first, number_first_suffix,
            number_last_prefix, number_last, number_last_suffix,
            lot_number_prefix, lot_number, lot_number_suffix,
            street_locality_pid, locality_pid, postcode, state_abbrev,
            localities, streets, flat_types, level_types, street_types, street_suffixes,
        );

        let (lat, lon) = geocodes
            .get(pid)
            .copied()
            .unwrap_or((0.0, 0.0));
        let lat_opt = if lat != 0.0 { Some(lat) } else { None };
        let lon_opt = if lon != 0.0 { Some(lon) } else { None };

        insert_stmt.execute(params![
            pid, building_name,
            flat_type_code, flat_number, flat_number_prefix, flat_number_suffix,
            level_type_code, level_number, level_number_prefix, level_number_suffix,
            number_first, number_first_prefix, number_first_suffix,
            number_last, number_last_prefix, number_last_suffix,
            lot_number, lot_number_prefix, lot_number_suffix,
            street_locality_pid, locality_pid, postcode, confidence,
            state_abbrev, sla, lat_opt, lon_opt,
        ])?;

        count += 1;
        if count % batch_size == 0 {
            conn.execute("COMMIT", [])?;
            conn.execute("BEGIN", [])?;
        }
        pb.inc(1);
    }
    conn.execute("COMMIT", [])?;
    pb.finish();
    println!("  Total addresses loaded: {}", count);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_sla(
    building_name: &str,
    flat_type_code: &str, flat_number_prefix: &str, flat_number: &str, flat_number_suffix: &str,
    level_type_code: &str, level_number_prefix: &str, level_number: Option<i32>, level_number_suffix: &str,
    number_first_prefix: &str, number_first: Option<i32>, number_first_suffix: &str,
    number_last_prefix: &str, number_last: Option<i32>, number_last_suffix: &str,
    lot_number_prefix: &str, lot_number: &str, lot_number_suffix: &str,
    street_locality_pid: &str, locality_pid: &str, postcode: &str, state: &str,
    localities: &HashMap<String, LocalityInfo>,
    streets: &HashMap<String, StreetInfo>,
    flat_types: &HashMap<String, String>,
    level_types: &HashMap<String, String>,
    street_types: &HashMap<String, String>,
    street_suffixes: &HashMap<String, String>,
) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Level
    if !level_type_code.is_empty() || level_number.is_some() {
        let type_name = level_types.get(level_type_code).map(|s| s.as_str()).unwrap_or("");
        let num = level_number.map(|n| n.to_string()).unwrap_or_default();
        let level_str = format!("{} {}{}{}", type_name, level_number_prefix, num, level_number_suffix).trim().to_string();
        if !level_str.is_empty() {
            parts.push(level_str);
        }
    }

    // Flat/Unit
    if !flat_type_code.is_empty() || !flat_number.is_empty() {
        let type_name = flat_types.get(flat_type_code).map(|s| s.as_str()).unwrap_or("");
        let flat_str = format!("{} {}{}{}", type_name, flat_number_prefix, flat_number, flat_number_suffix).trim().to_string();
        if !flat_str.is_empty() {
            parts.push(flat_str);
        }
    }

    // Building name
    if !building_name.is_empty() {
        parts.push(building_name.to_string());
    }

    // Street number
    let mut number = String::new();
    if !lot_number.is_empty() && number_first.is_none() {
        number = format!("LOT {}{}{}", lot_number_prefix, lot_number, lot_number_suffix);
    } else if let Some(nf) = number_first {
        number = format!("{}{}{}", number_first_prefix, nf, number_first_suffix);
        if let Some(nl) = number_last {
            number = format!("{}-{}{}{}", number, number_last_prefix, nl, number_last_suffix);
        }
    }

    // Street
    let street = streets.get(street_locality_pid);
    let street_str = if let Some(s) = street {
        let type_name = street_types.get(&s.type_code).map(|s| s.as_str()).unwrap_or("");
        let suffix_name = street_suffixes.get(&s.suffix_code).map(|s| s.as_str()).unwrap_or("");
        let mut st = s.name.clone();
        if !type_name.is_empty() {
            st = format!("{} {}", st, type_name);
        }
        if !suffix_name.is_empty() {
            st = format!("{} {}", st, suffix_name);
        }
        st
    } else {
        String::new()
    };

    if !number.is_empty() || !street_str.is_empty() {
        let street_part = if !number.is_empty() && !street_str.is_empty() {
            format!("{} {}", number, street_str)
        } else {
            format!("{}{}", number, street_str)
        };
        parts.push(street_part);
    }

    // Locality, State, Postcode
    let locality_name = localities
        .get(locality_pid)
        .map(|l| l.name.as_str())
        .unwrap_or("");
    let location = format!("{} {} {}", locality_name, state, postcode).trim().to_string();
    if !location.is_empty() {
        parts.push(location);
    }

    parts.join(", ")
}

fn count_lines(path: &Path) -> Result<usize> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(path)?;
    let reader = BufReader::with_capacity(1024 * 1024, file);
    Ok(reader.lines().count().saturating_sub(1)) // subtract header
}
