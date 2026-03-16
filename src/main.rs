mod db;
mod gnaf;
mod server;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "addressr", about = "Australian Address Search powered by G-NAF")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Load G-NAF data into SQLite database
    Load {
        /// Path to extracted G-NAF directory (containing "G-NAF" subdirectory)
        #[arg(short, long)]
        gnaf_dir: PathBuf,

        /// SQLite database path
        #[arg(short, long, default_value = "addressr.db")]
        db_path: PathBuf,

        /// Only load specific states (comma-separated, e.g. "ACT,NSW,VIC")
        #[arg(short, long)]
        states: Option<String>,
    },

    /// Start the API server
    Serve {
        /// SQLite database path
        #[arg(short, long, default_value = "addressr.db")]
        db_path: PathBuf,

        /// Listen port
        #[arg(short, long, default_value = "8080")]
        port: u16,
    },

    /// Search addresses from command line
    Search {
        /// SQLite database path
        #[arg(short, long, default_value = "addressr.db")]
        db_path: PathBuf,

        /// Search query
        query: String,

        /// Max results
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Load {
            gnaf_dir,
            db_path,
            states,
        } => {
            let state_filter: Option<Vec<String>> = states
                .map(|s| s.split(',').map(|s| s.trim().to_uppercase()).collect());
            gnaf::load(&gnaf_dir, &db_path, state_filter.as_deref())?;
        }
        Commands::Serve { db_path, port } => {
            server::serve(&db_path, port).await?;
        }
        Commands::Search {
            db_path,
            query,
            limit,
        } => {
            let results = db::search(&db_path, &query, limit, 0)?;
            if results.is_empty() {
                println!("No results found.");
            } else {
                for (i, addr) in results.iter().enumerate() {
                    println!("{}. {}", i + 1, addr.sla);
                    if let (Some(lat), Some(lon)) = (addr.latitude, addr.longitude) {
                        println!("   ({}, {})", lat, lon);
                    }
                    println!();
                }
            }
        }
    }

    Ok(())
}
