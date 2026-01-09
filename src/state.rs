use crate::types::{LogIndex, NodeId};

/// Follower state - passive, responds to RPCs.
pub struct Follower {
    pub leader_id: Option<NodeId>,
}

/// Candidate state - actively seeking votes.
pub struct Candidate {
    pub votes_received: Vec<NodeId>,
}

/// Leader state - manages replication.
pub struct Leader {
    pub next_index: Vec<(NodeId, LogIndex)>,
    pub match_index: Vec<(NodeId, LogIndex)>,
}
