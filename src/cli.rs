use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "onshape", version, about = "Onshape CLI utilities")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Recursively fetch assembly snapshots and optionally download part STL meshes.
    #[command(name = "fetch")]
    FetchSnapshotRecursive(FetchSnapshotRecursiveArgs),
    /// Convert an Onshape dump folder into a URDF using assembly mate joints when available.
    DumpToUrdf(DumpToUrdfArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WvmKind {
    W,
    V,
    M,
}

impl WvmKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::W => "w",
            Self::V => "v",
            Self::M => "m",
        }
    }
}

#[derive(Debug, Args)]
pub struct FetchSnapshotRecursiveArgs {
    /// Onshape API access key. Falls back to ONSHAPE_ACCESS_KEY.
    #[arg(long, env = "ONSHAPE_ACCESS_KEY", hide_env_values = true)]
    pub access_key: Option<String>,

    /// Onshape API secret key. Falls back to ONSHAPE_SECRET_KEY.
    #[arg(long, env = "ONSHAPE_SECRET_KEY", hide_env_values = true)]
    pub secret_key: Option<String>,

    /// Full Onshape URL: https://cad.onshape.com/documents/<did>/<w|v|m>/<wvmid>/e/<eid>
    #[arg(long)]
    pub url: Option<String>,

    #[arg(long, default_value = "https://cad.onshape.com")]
    pub stack: String,

    #[arg(long)]
    pub did: Option<String>,

    #[arg(long)]
    pub wvm: Option<WvmKind>,

    #[arg(long)]
    pub wvmid: Option<String>,

    #[arg(long)]
    pub eid: Option<String>,

    #[arg(long, default_value = "v10")]
    pub api_version: String,

    /// Output directory for snapshot files.
    #[arg(value_name = "OUTDIR")]
    pub outdir: PathBuf,

    #[arg(
        long,
        default_value = "true",
        default_missing_value = "true",
        num_args = 0..=1,
        value_parser = parse_bool
    )]
    pub download_meshes: bool,

    #[arg(long, default_value = "meshes")]
    pub mesh_dir: PathBuf,

    #[arg(long, action = ArgAction::SetTrue)]
    pub pin_microversion: bool,
}

#[derive(Debug, Args)]
pub struct DumpToUrdfArgs {
    /// Dump directory or path to assembly_root.json (positional form).
    #[arg(value_name = "DUMP_OR_ROOT_JSON")]
    pub dump_positional: Option<PathBuf>,

    /// Dump directory or path to assembly_root.json.
    #[arg(long)]
    pub dump: Option<PathBuf>,

    /// Output URDF path.
    #[arg(long)]
    pub out: Option<PathBuf>,

    #[arg(long, default_value = "onshape_robot")]
    pub robot: String,

    #[arg(long, default_value = "")]
    pub mesh_prefix: String,

    #[arg(long, default_value_t = 1.0)]
    pub mesh_scale: f64,

    #[arg(long, action = ArgAction::SetTrue)]
    pub no_collision: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    pub no_visual: bool,

    #[arg(
        long,
        default_value = "true",
        default_missing_value = "true",
        num_args = 0..=1,
        value_parser = parse_bool
    )]
    pub skip_missing_mesh: bool,
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" | "on" => Ok(true),
        "false" | "0" | "no" | "n" | "off" => Ok(false),
        _ => Err(format!("Invalid boolean value: {value}")),
    }
}
