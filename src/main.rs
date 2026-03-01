mod cli;
mod dump_to_urdf;
mod fetch_snapshot_recursive;
mod types;
mod util;

use std::{env, fs, path::Path};

use clap::Parser;

use crate::cli::{Cli, Commands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = load_dotenv_file(Path::new(".env"));
    let cli = Cli::parse();

    match cli.command {
        Commands::FetchSnapshotRecursive(args) => fetch_snapshot_recursive::run(args).await?,
        Commands::DumpToUrdf(args) => dump_to_urdf::run(args)?,
    }

    Ok(())
}

fn load_dotenv_file(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let content = fs::read_to_string(path)?;
    for raw_line in content.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let without_export = trimmed.strip_prefix("export ").unwrap_or(trimmed).trim();
        let Some((key_raw, value_raw)) = without_export.split_once('=') else {
            continue;
        };

        let key = key_raw.trim();
        if key.is_empty() || env::var_os(key).is_some() {
            continue;
        }

        let mut value = value_raw.trim().to_string();
        if value.len() >= 2 {
            let quoted_double = value.starts_with('"') && value.ends_with('"');
            let quoted_single = value.starts_with('\'') && value.ends_with('\'');
            if quoted_double || quoted_single {
                value = value[1..value.len() - 1].to_string();
            }
        }

        let value = value
            .replace("\\n", "\n")
            .replace("\\r", "\r")
            .replace("\\t", "\t");

        // Safety: this runs before the async runtime starts and before any threads are spawned.
        unsafe { env::set_var(key, value) };
    }

    Ok(())
}
