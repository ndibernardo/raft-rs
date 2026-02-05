use std::collections::HashMap;

use clap::Parser;
use raft::server::{Config, Server};

#[derive(Parser)]
struct Args {
    /// This node's numeric ID (must be unique in the cluster).
    #[arg(long)]
    id: u64,

    /// TCP address to listen on for Raft RPCs.
    #[arg(long)]
    addr: String,

    /// A peer in the form ID=ADDR. Repeat for each peer.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Directory for persistent state (meta.json, log.jsonl).
    #[arg(long)]
    data_dir: std::path::PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut peers: HashMap<String, String> = HashMap::new();
    for p in &args.peers {
        let (id, addr) = p
            .split_once('=')
            .ok_or_else(|| format!("--peer must be ID=ADDR, got: {p}"))?;
        peers.insert(id.to_string(), addr.to_string());
    }

    Server::start(Config {
        id: args.id,
        addr: args.addr,
        peers,
        data_dir: args.data_dir,
    })?
    .run()?;

    Ok(())
}
