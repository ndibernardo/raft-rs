use std::collections::HashMap;
use std::sync::mpsc;

use clap::Parser;
use raft::client_api;
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

    /// TCP address for the HTTP client API (e.g. 127.0.0.1:8001). Optional.
    #[arg(long)]
    client_addr: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("raft=info")),
        )
        .init();

    let args = Args::parse();

    let mut peers: HashMap<String, String> = HashMap::new();
    for p in &args.peers {
        let (id, addr) = p
            .split_once('=')
            .ok_or_else(|| format!("--peer must be ID=ADDR, got: {p}"))?;
        peers.insert(id.to_string(), addr.to_string());
    }

    // Channel between the HTTP API thread and the Raft event loop.
    let (client_tx, client_rx) = mpsc::channel::<client_api::Pending>();

    let config = Config {
        id: args.id,
        addr: args.addr,
        peers,
        data_dir: args.data_dir,
        client_addr: args.client_addr.clone(),
    };

    // Start HTTP API if requested.
    if let Some(ref addr_str) = args.client_addr {
        let addr = addr_str
            .parse()
            .map_err(|e| format!("invalid --client-addr '{addr_str}': {e}"))?;
        client_api::start(addr, client_tx.clone());
    }

    // Run the Raft event loop on the main thread (sync).
    Server::start(config, client_rx)?.run()?;

    Ok(())
}
