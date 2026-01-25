use super::log::LogEntry;
use super::primitives::{LogIndex, NodeId, Term};

/// RequestVote RPC arguments.
#[derive(Clone)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// RequestVote RPC response.
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments.
pub struct AppendEntries<Cmd> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<Cmd>>,
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

/// All possible Raft messages.
pub enum Message<Cmd> {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries<Cmd>),
    AppendEntriesResponse(AppendEntriesResponse),
}
