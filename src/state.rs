use crate::types::{LogIndex, NodeId};

/// §5.1: followers are passive — they issue no requests, only respond to RPCs from
/// leaders and candidates. If a follower receives no communication, it starts an election.
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

/// §5.2: a candidate requests votes from peers to win an election. It votes for itself
/// and wins if it receives votes from a majority of servers in the full cluster.
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

    // §5.2: a candidate wins the election if it receives votes from a majority
    // of the servers in the full cluster (⌊N/2⌋ + 1 out of N servers).
    pub fn has_majority(&self, cluster_size: usize) -> bool {
        let majority = cluster_size / 2 + 1;
        self.votes_received.len() >= majority
    }
}

/// §5.3, Figure 2, Volatile state on leaders (reinitialized after election).
/// The leader maintains next_index and match_index for each follower to track replication.
pub struct Leader {
    next_index: Vec<(NodeId, LogIndex)>,  // next log index to send to each server
    match_index: Vec<(NodeId, LogIndex)>, // highest log index known to be replicated
}

impl Leader {
    // nextIndex initialized to leader last log index + 1 (optimistic).
    // matchIndex initialized to 0 (conservative, increases monotonically).
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
            *idx = idx.prev().unwrap_or(*idx);
        }
    }
}
