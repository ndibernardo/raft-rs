use crate::types::{LogIndex, NodeId};

/// Follower state - passive, responds to RPCs.
pub struct Follower {
    pub leader_id: Option<NodeId>,
}

impl Follower {
    pub fn new() -> Self {
        Self { leader_id: None }
    }

    pub fn set_leader(&mut self, leader_id: NodeId) {
        self.leader_id = Some(leader_id);
    }
}

impl Default for Follower {
    fn default() -> Self {
        Self::new()
    }
}

/// Candidate state - actively seeking votes.
pub struct Candidate {
    votes_received: Vec<NodeId>,
}

impl Candidate {
    pub fn new(self_id: NodeId) -> Self {
        Self {
            votes_received: vec![self_id],
        }
    }

    pub fn record_vote(&mut self, from: NodeId) {
        self.votes_received.push(from);
    }

    pub fn has_majority(&self, cluster_size: usize) -> bool {
        let majority = cluster_size / 2 + 1;
        self.votes_received.len() >= majority
    }
}

/// Leader state - manages replication.
pub struct Leader {
    next_index: Vec<(NodeId, LogIndex)>,
    match_index: Vec<(NodeId, LogIndex)>,
}

impl Leader {
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        Self {
            next_index: peers.iter().map(|&p| (p, last_log_index.next())).collect(),
            match_index: peers.iter().map(|&p| (p, LogIndex::default())).collect(),
        }
    }

    /// Get next_index for a peer.
    pub fn next_index_for(&self, peer: NodeId) -> Option<LogIndex> {
        self.next_index
            .iter()
            .find(|(id, _)| *id == peer)
            .map(|(_, idx)| *idx)
    }

    /// Collect all follower match indices for commit calculation.
    pub fn match_indices(&self) -> impl Iterator<Item = LogIndex> + '_ {
        self.match_index.iter().map(|(_, idx)| *idx)
    }

    /// Update follower progress after successful replication.
    pub fn record_success(&mut self, from: NodeId, match_index: LogIndex) {
        if let Some((_, idx)) = self.match_index.iter_mut().find(|(id, _)| *id == from) {
            *idx = match_index;
        }
        if let Some((_, idx)) = self.next_index.iter_mut().find(|(id, _)| *id == from) {
            *idx = match_index.next();
        }
    }

    /// Decrement next_index after failed replication.
    pub fn record_failure(&mut self, from: NodeId) {
        if let Some((_, idx)) = self.next_index.iter_mut().find(|(id, _)| *id == from) {
            if let Some(prev) = idx.prev() {
                *idx = prev;
            }
        }
    }
}
