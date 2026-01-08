use crate::state::{Candidate, Follower, Leader};
use crate::types::{LogEntry, LogIndex, NodeId, Term};

/// Persistent state on all servers.
pub struct PersistentState<C> {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry<C>>,
}

/// Volatile state on all servers.
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

/// Server role with associated state.
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

/// A Raft node.
pub struct Node<C> {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub persistent: PersistentState<C>,
    pub volatile: VolatileState,
    pub role: Role,
}
