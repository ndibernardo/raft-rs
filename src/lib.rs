//! Raft consensus algorithm implementation.
//!
//! Based on:
//! - "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout)
//! - Diego Ongaro's PhD dissertation
//! - The TLA+ specification at github.com/ongardie/raft.tla

pub mod error;
pub mod log;
pub mod message;
pub mod node;
pub mod state;
pub mod types;

pub use error::{Error, Result};
pub use log::{Entry, Log};
pub use message::{AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse};
pub use node::Node;
pub use state::ServerState;
pub use types::{LogIndex, NodeId, Term};
