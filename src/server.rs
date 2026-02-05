use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::command::Command;
use crate::file_storage::{FileStorage, FileStorageError};
use crate::kv::{KvCommand, KvStore};
use crate::runtime::{Event, Runtime, TimerConfig};
use crate::transport::{Transport, TransportError};
use crate::types::NodeId;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("storage: {0}")]
    Storage(#[from] FileStorageError),
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    #[error("config: {0}")]
    Config(String),
}

pub struct Config {
    pub id: u64,
    pub addr: String,
    pub peers: HashMap<String, String>,
    pub data_dir: PathBuf,
}

/// A running Raft KV node: persistent log on disk, RPCs over TCP.
pub struct Server {
    runtime: Runtime<KvCommand, KvStore, FileStorage<KvCommand>>,
    transport: Transport<KvCommand>,
}

impl Server {
    /// Open storage, bind the listener, and restore any persistent state.
    pub fn start(config: Config) -> Result<Self, ServerError> {
        let local_id = NodeId::from(config.id);

        let addr: SocketAddr = config
            .addr
            .parse()
            .map_err(|e| ServerError::Config(format!("invalid addr '{}': {e}", config.addr)))?;

        let peers = parse_peers(&config.peers)?;
        let peer_ids: Vec<NodeId> = peers.keys().copied().collect();

        let storage = FileStorage::open(&config.data_dir)?;
        let runtime = Runtime::from_storage(
            local_id,
            peer_ids,
            KvStore::new(),
            storage,
            TimerConfig::default(),
        )?;

        let transport = Transport::bind(local_id, addr, peers)?;

        eprintln!("raft: node {local_id} listening on {addr}");

        Ok(Self { runtime, transport })
    }

    /// Run the Raft event loop. Returns only on I/O error.
    pub fn run(&mut self) -> Result<(), ServerError> {
        loop {
            // Drain fired timers before blocking — back-to-back timeouts must not be skipped.
            if let Some(event) = self.runtime.poll_timers() {
                let commands = self.runtime.handle(event)?;
                self.dispatch(commands)?;
                continue;
            }

            // Block until the next timer deadline or an incoming message, whichever comes first.
            let wait = self
                .runtime
                .next_deadline()
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(5));

            if let Some((from, message)) = self.transport.recv_timeout(wait) {
                let commands = self.runtime.handle(Event::Message { from, message })?;
                self.dispatch(commands)?;
            }
        }
    }

    fn dispatch(&self, commands: Vec<Command<KvCommand>>) -> Result<(), ServerError> {
        for command in commands {
            if let Command::Send { to, message } = command {
                self.transport.send(to, message)?;
            }
        }
        Ok(())
    }
}

fn parse_peers(raw: &HashMap<String, String>) -> Result<HashMap<NodeId, SocketAddr>, ServerError> {
    raw.iter()
        .map(|(id_str, addr_str)| {
            let id: u64 = id_str
                .parse()
                .map_err(|_| ServerError::Config(format!("invalid peer id: {id_str}")))?;
            let addr: SocketAddr = addr_str.parse().map_err(|e| {
                ServerError::Config(format!("invalid peer addr '{addr_str}': {e}"))
            })?;
            Ok((NodeId::from(id), addr))
        })
        .collect()
}
