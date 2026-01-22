use crate::command::Command;
use crate::state::{Candidate, Follower, Leader};
use crate::storage::Storage;
use crate::types::{
    AppendEntries, AppendEntriesResponse, LogEntry, LogIndex, Message, NodeId, RequestVote,
    RequestVoteResponse, Term,
};

/// Persistent state on all servers. Must survive crashes — updated on stable storage
/// before responding to RPCs. Figure 2, State (persistent state on all servers).
pub struct PersistentState<C> {
    pub current_term: Term,    // latest term server has seen (initialized to 0)
    pub voted_for: Option<NodeId>, // candidateId that received vote in current term
    pub log: Vec<LogEntry<C>>, // log entries; each entry contains command and term
}

impl<C: Clone> PersistentState<C> {
    /// Load persistent state from storage.
    pub fn load<S: Storage<C>>(storage: &S) -> Result<Self, S::Error> {
        Ok(Self {
            current_term: storage.current_term()?,
            voted_for: storage.voted_for()?,
            log: storage.entries_from(LogIndex::from(1))?,
        })
    }

    /// Save persistent state to storage.
    pub fn save<S: Storage<C>>(&self, storage: &mut S) -> Result<(), S::Error> {
        storage.set_current_term(self.current_term)?;
        storage.set_voted_for(self.voted_for)?;

        let stored_len = storage.last_log_index()?;
        let current_len = LogIndex::from_length(self.log.len());

        if stored_len > current_len {
            storage.truncate_from(current_len.next())?;
        }

        for (idx, entry) in self.log.iter().enumerate() {
            let log_index = LogIndex::from((idx + 1) as u64);
            if log_index > stored_len {
                storage.append(entry.clone())?;
            }
        }

        Ok(())
    }
}

/// Volatile state on all servers. Reinitialized after a crash.
/// Figure 2, State (volatile state on all servers).
pub struct VolatileState {
    pub commit_index: LogIndex, // index of highest log entry known to be committed
    pub last_applied: LogIndex, // index of highest log entry applied to state machine
}

/// Server role with associated state.
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

/// A committed entry ready to apply to the state machine.
#[derive(Debug, PartialEq, Eq)]
pub struct Applied<'a, C> {
    pub index: LogIndex,
    pub command: &'a C,
}

/// A Raft node.
pub struct Node<C> {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub persistent: PersistentState<C>,
    pub volatile: VolatileState,
    pub role: Role,
}

impl<C: Clone> Node<C> {
    /// Create a new node. Starts as follower with no known leader.
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,
            persistent: PersistentState {
                current_term: Term::default(),
                voted_for: None,
                log: Vec::new(),
            },
            volatile: VolatileState {
                commit_index: LogIndex::default(),
                last_applied: LogIndex::default(),
            },
            role: Role::Follower(Follower::new()),
        }
    }

    /// Create a node from persistent storage. Used for crash recovery.
    pub fn from_storage<S: Storage<C>>(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: &S,
    ) -> Result<Self, S::Error> {
        let persistent = PersistentState::load(storage)?;
        Ok(Self {
            id,
            peers,
            persistent,
            volatile: VolatileState {
                commit_index: LogIndex::default(),
                last_applied: LogIndex::default(),
            },
            role: Role::Follower(Follower::new()),
        })
    }

    /// Save current persistent state to storage.
    pub fn save<S: Storage<C>>(&self, storage: &mut S) -> Result<(), S::Error> {
        self.persistent.save(storage)
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex::from_length(self.persistent.log.len())
    }

    /// Convert to follower state, updating term.
    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        self.persistent.current_term = term;
        self.persistent.voted_for = None;
        let mut follower = Follower::new();
        if let Some(id) = leader_id {
            follower.set_leader(id);
        }
        self.role = Role::Follower(follower);
    }

    fn last_log_term(&self) -> Term {
        self.persistent
            .log
            .last()
            .map_or(Term::default(), |entry| entry.term)
    }

    /// Called when election timer fires. Follower/Candidate starts new election.
    pub fn election_timeout(&mut self) -> Vec<Command<C>> {
        match &self.role {
            Role::Leader(_) => Vec::new(),
            Role::Follower(_) | Role::Candidate(_) => self.start_election(),
        }
    }

    // §5.2: on election timeout, increment currentTerm, vote for self,
    // reset election timer, send RequestVote to all other servers.
    fn start_election(&mut self) -> Vec<Command<C>> {
        self.persistent.current_term = self.persistent.current_term.increment();
        self.persistent.voted_for = Some(self.id);
        self.role = Role::Candidate(Candidate::new(self.id));

        // Single node cluster: already have majority with own vote.
        let cluster_size = self.peers.len() + 1;
        if cluster_size == 1 {
            return self.become_leader();
        }

        let request = RequestVote {
            term: self.persistent.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        let mut commands = Vec::new();
        for &peer in &self.peers {
            commands.push(Command::Send {
                to: peer,
                message: Message::RequestVote(request.clone()),
            });
        }
        commands.push(Command::ResetElectionTimer);
        commands
    }

    /// Handle incoming RequestVote RPC. Figure 2, RequestVote RPC (receiver implementation).
    pub fn handle_request_vote(&mut self, from: NodeId, req: RequestVote) -> Vec<Command<C>> {
        if req.term > self.persistent.current_term {
            self.become_follower(req.term, None);
        }

        let vote_granted = self.should_grant_vote(&req);
        if vote_granted {
            self.persistent.voted_for = Some(req.candidate_id);
        }

        vec![Command::Send {
            to: from,
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted,
            }),
        }]
    }

    /// Grant vote only if all three conditions hold. Figure 2, RequestVote RPC §1–2:
    /// (1) candidate's term is current, (2) we haven't voted for someone else this term,
    /// (3) candidate's log is at least as up-to-date as ours (§5.4.1).
    fn should_grant_vote(&self, req: &RequestVote) -> bool {
        let term_ok = req.term >= self.persistent.current_term;
        let vote_ok = match self.persistent.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };
        let log_ok = self.is_log_up_to_date(req.last_log_term, req.last_log_index);

        term_ok && vote_ok && log_ok
    }

    /// §5.4.1: the candidate's log must be at least as up-to-date as any other log in the
    /// cluster. Determined by comparing the last entry's term, then index.
    fn is_log_up_to_date(&self, candidate_term: Term, candidate_index: LogIndex) -> bool {
        (candidate_term, candidate_index) >= (self.last_log_term(), self.last_log_index())
    }

    /// Handle incoming RequestVoteResponse RPC.
    pub fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        resp: RequestVoteResponse,
    ) -> Vec<Command<C>> {
        if resp.term < self.persistent.current_term {
            return Vec::new();
        }
        if resp.term > self.persistent.current_term {
            self.become_follower(resp.term, None);
            return Vec::new();
        }

        let dominated = match &mut self.role {
            Role::Candidate(candidate) => {
                if resp.vote_granted {
                    candidate.record_vote(from);
                }
                candidate.has_majority(self.peers.len() + 1)
            }
            Role::Follower(_) | Role::Leader(_) => return Vec::new(),
        };

        if dominated {
            self.become_leader()
        } else {
            Vec::new()
        }
    }

    /// §5.2: upon winning the election, send initial empty AppendEntries (heartbeats) to
    /// each server. Figure 2, Rules for Servers (Leaders): initialize nextIndex and matchIndex.
    fn become_leader(&mut self) -> Vec<Command<C>> {
        self.role = Role::Leader(Leader::new(&self.peers, self.last_log_index()));
        self.send_heartbeats()
    }

    /// Called when heartbeat timer fires. Leader sends AppendEntries to all peers.
    pub fn heartbeat_timeout(&mut self) -> Vec<Command<C>> {
        match &self.role {
            Role::Leader(_) => self.send_heartbeats(),
            _ => Vec::new(),
        }
    }

    // §5.3, Figure 2, AppendEntries RPC (sender): for each peer, send entries starting at
    // nextIndex. prev_log_index/term let the follower verify log consistency.
    fn send_heartbeats(&self) -> Vec<Command<C>> {
        let Role::Leader(leader) = &self.role else {
            return Vec::new();
        };

        let mut commands = Vec::new();

        for &peer in &self.peers {
            let next_index = leader
                .next_index_for(peer)
                .unwrap_or_else(|| self.last_log_index().next());

            let prev_log_index = next_index.prev().unwrap_or_default();
            let prev_log_term = self.term_at(prev_log_index);

            let entries = self.entries_from(next_index);

            commands.push(Command::Send {
                to: peer,
                message: Message::AppendEntries(AppendEntries {
                    term: self.persistent.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.volatile.commit_index,
                }),
            });
        }

        commands.push(Command::ResetHeartbeatTimer);
        commands
    }

    fn term_at(&self, index: LogIndex) -> Term {
        match index.to_array_index() {
            None => Term::default(),
            Some(idx) => self
                .persistent
                .log
                .get(idx)
                .map(|e| e.term)
                .unwrap_or_default(),
        }
    }

    fn entries_from(&self, start: LogIndex) -> Vec<LogEntry<C>> {
        match start.to_array_index() {
            None => self.persistent.log.clone(),
            Some(idx) => self.persistent.log.get(idx..).unwrap_or_default().to_vec(),
        }
    }

    /// Handle incoming AppendEntries RPC. Figure 2, AppendEntries RPC (receiver implementation).
    pub fn handle_append_entries(
        &mut self,
        from: NodeId,
        req: AppendEntries<C>,
    ) -> Vec<Command<C>> {
        if req.term > self.persistent.current_term {
            self.become_follower(req.term, Some(req.leader_id));
        }
        if req.term < self.persistent.current_term {
            return vec![Command::Send {
                to: from,
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: LogIndex::default(),
                }),
            }];
        }

        // Valid AppendEntries from current leader.
        if let Role::Follower(follower) = &mut self.role {
            follower.set_leader(req.leader_id);
        }

        let mut commands = vec![Command::ResetElectionTimer];

        if !self.check_log_consistency(req.prev_log_index, req.prev_log_term) {
            commands.push(Command::Send {
                to: from,
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: LogIndex::default(),
                }),
            });
            return commands;
        }

        self.append_entries(req.prev_log_index, req.entries);

        if req.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = std::cmp::min(req.leader_commit, self.last_log_index());
        }

        commands.push(Command::Send {
            to: from,
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.persistent.current_term,
                success: true,
                match_index: self.last_log_index(),
            }),
        });

        commands
    }

    /// §5.3 Log Matching Property: if two logs have an entry with the same index and term,
    /// they are identical in all entries up through that index. Verified by checking
    /// prev_log_index and prev_log_term before accepting new entries. Figure 2, §2.
    fn check_log_consistency(&self, prev_log_index: LogIndex, prev_log_term: Term) -> bool {
        if prev_log_index == LogIndex::default() {
            return prev_log_term == Term::default();
        }

        match prev_log_index.to_array_index() {
            Some(idx) => self
                .persistent
                .log
                .get(idx)
                .is_some_and(|entry| entry.term == prev_log_term),
            None => false,
        }
    }

    /// Handle incoming AppendEntriesResponse RPC (leader only).
    pub fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        resp: AppendEntriesResponse,
    ) -> Vec<Command<C>> {
        if resp.term > self.persistent.current_term {
            self.become_follower(resp.term, None);
            return Vec::new();
        }
        if resp.term < self.persistent.current_term {
            return Vec::new();
        }

        match &mut self.role {
            Role::Leader(leader) => {
                if resp.success {
                    leader.record_success(from, resp.match_index);
                    self.advance_commit_index();
                } else {
                    leader.record_failure(from);
                }
            }
            Role::Follower(_) | Role::Candidate(_) => {}
        }

        Vec::new()
    }

    /// §5.3: leader appends the command to its log as a new entry, then issues
    /// AppendEntries in parallel to replicate. Returns the assigned log index, or None if
    /// not leader. Figure 2, Rules for Servers (Leaders) §2.
    pub fn submit_command(&mut self, command: C) -> Option<LogIndex> {
        if !matches!(self.role, Role::Leader(_)) {
            return None;
        }

        let entry = LogEntry {
            term: self.persistent.current_term,
            command,
        };
        self.persistent.log.push(entry);

        Some(self.last_log_index())
    }

    /// Figure 2, Rules for Servers (Leaders): if there exists N > commitIndex such that a
    /// majority of matchIndex[i] >= N and log[N].term == currentTerm, set commitIndex = N.
    /// §5.4.2, Figure 8: a leader may only commit entries from its current term directly;
    /// earlier entries are committed indirectly by the Log Matching Property.
    fn advance_commit_index(&mut self) {
        let Role::Leader(leader) = &self.role else {
            return;
        };

        // Collect match indices including leader's own implicit match.
        let mut match_indices: Vec<LogIndex> = leader.match_indices().collect();
        match_indices.push(self.last_log_index());
        match_indices.sort();

        // Majority position: with N nodes, need (N/2 + 1) replicas.
        // Sorted ascending, so match_indices[len/2] is the median.
        let majority_pos = match_indices.len() / 2;
        let majority_index = match_indices[majority_pos];

        // Only commit if entry is from current term (Figure 8 safety).
        let has_higher_index = majority_index > self.volatile.commit_index;
        let is_current_term = majority_index
            .to_array_index()
            .and_then(|idx| self.persistent.log.get(idx))
            .is_some_and(|e| e.term == self.persistent.current_term);

        if has_higher_index && is_current_term {
            self.volatile.commit_index = majority_index;
        }
    }

    /// Returns true if there are committed entries waiting to be applied.
    pub fn has_pending_applies(&self) -> bool {
        self.volatile.commit_index > self.volatile.last_applied
    }

    /// Figure 2, Rules for Servers (All Servers): if commitIndex > lastApplied, increment
    /// lastApplied and apply log[lastApplied] to the state machine. §5.3.
    pub fn take_entry_to_apply(&mut self) -> Option<Applied<'_, C>> {
        if self.volatile.last_applied >= self.volatile.commit_index {
            return None;
        }

        self.volatile.last_applied = self.volatile.last_applied.next();
        let index = self.volatile.last_applied;

        self.volatile
            .last_applied
            .to_array_index()
            .and_then(|idx| self.persistent.log.get(idx))
            .map(|entry| Applied {
                index,
                command: &entry.command,
            })
    }

    /// §5.3, Figure 2, AppendEntries RPC §3–5: if an existing entry conflicts with a new one
    /// (same index, different term), delete it and all that follow, then append new entries.
    fn append_entries(&mut self, prev_log_index: LogIndex, entries: Vec<LogEntry<C>>) {
        let mut insert_index = prev_log_index.next();

        for entry in entries {
            match insert_index.to_array_index() {
                Some(idx) if idx < self.persistent.log.len() => {
                    if self.persistent.log[idx].term != entry.term {
                        self.persistent.log.truncate(idx);
                        self.persistent.log.push(entry);
                    }
                }
                _ => {
                    self.persistent.log.push(entry);
                }
            }
            insert_index = insert_index.next();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, peers: &[u64]) -> Node<String> {
        Node::new(
            NodeId::from(id),
            peers.iter().map(|&p| NodeId::from(p)).collect(),
        )
    }

    fn is_follower(node: &Node<String>) -> bool {
        matches!(node.role, Role::Follower(_))
    }

    fn is_candidate(node: &Node<String>) -> bool {
        matches!(node.role, Role::Candidate(_))
    }

    fn is_leader(node: &Node<String>) -> bool {
        matches!(node.role, Role::Leader(_))
    }

    fn extract_vote_granted(cmds: &[Command<String>]) -> bool {
        cmds.iter()
            .find_map(|c| match c {
                Command::Send {
                    message: Message::RequestVoteResponse(r),
                    ..
                } => Some(r.vote_granted),
                _ => None,
            })
            .unwrap()
    }

    fn extract_append_success(cmds: &[Command<String>]) -> bool {
        cmds.iter()
            .find_map(|c| match c {
                Command::Send {
                    message: Message::AppendEntriesResponse(r),
                    ..
                } => Some(r.success),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn new_node_is_follower() {
        let n = node(1, &[2, 3]);
        assert!(is_follower(&n));
        assert_eq!(n.persistent.current_term, Term::default());
        assert_eq!(n.persistent.voted_for, None);
    }

    #[test]
    fn election_timeout_starts_election() {
        let mut n = node(1, &[2, 3]);
        let commands = n.election_timeout();

        assert!(is_candidate(&n));
        assert_eq!(n.persistent.current_term, Term::from(1));
        assert_eq!(n.persistent.voted_for, Some(NodeId::from(1)));

        let send_count = commands
            .iter()
            .filter(|c| matches!(c, Command::Send { .. }))
            .count();
        assert_eq!(send_count, 2);
    }

    #[test]
    fn candidate_becomes_leader_with_majority() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();

        let resp = RequestVoteResponse {
            term: Term::from(1),
            vote_granted: true,
        };
        n.handle_request_vote_response(NodeId::from(2), resp);

        assert!(is_leader(&n));
    }

    #[test]
    fn candidate_stays_candidate_without_majority() {
        let mut n = node(1, &[2, 3, 4, 5]);
        n.election_timeout();

        let resp = RequestVoteResponse {
            term: Term::from(1),
            vote_granted: true,
        };
        n.handle_request_vote_response(NodeId::from(2), resp);

        assert!(is_candidate(&n));
    }

    #[test]
    fn node_rejects_vote_if_already_voted() {
        let mut n = node(1, &[2, 3]);

        let req1 = RequestVote {
            term: Term::from(1),
            candidate_id: NodeId::from(2),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(2), req1);
        assert!(extract_vote_granted(&cmds));

        let req2 = RequestVote {
            term: Term::from(1),
            candidate_id: NodeId::from(3),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(3), req2);
        assert!(!extract_vote_granted(&cmds));
    }

    #[test]
    fn node_grants_vote_in_new_term() {
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(1);
        n.persistent.voted_for = Some(NodeId::from(2));

        let req = RequestVote {
            term: Term::from(2),
            candidate_id: NodeId::from(3),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(3), req);
        assert!(extract_vote_granted(&cmds));
    }

    #[test]
    fn node_rejects_vote_with_stale_log() {
        let mut n = node(1, &[2, 3]);
        n.persistent.log.push(LogEntry {
            term: Term::from(2),
            command: "x".to_string(),
        });

        let req = RequestVote {
            term: Term::from(2),
            candidate_id: NodeId::from(2),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(2), req);
        assert!(!extract_vote_granted(&cmds));
    }

    #[test]
    fn append_entries_resets_election_timer() {
        let mut n = node(1, &[2, 3]);

        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        let cmds = n.handle_append_entries(NodeId::from(2), req);

        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::ResetElectionTimer)));
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(5);

        let req = AppendEntries {
            term: Term::from(3),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        let cmds = n.handle_append_entries(NodeId::from(2), req);
        assert!(!extract_append_success(&cmds));
    }

    #[test]
    fn append_entries_appends_new_entries() {
        let mut n = node(1, &[2, 3]);

        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![
                LogEntry {
                    term: Term::from(1),
                    command: "a".to_string(),
                },
                LogEntry {
                    term: Term::from(1),
                    command: "b".to_string(),
                },
            ],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert_eq!(n.persistent.log.len(), 2);
    }

    #[test]
    fn append_entries_truncates_on_conflict() {
        let mut n = node(1, &[2, 3]);
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            command: "old".to_string(),
        });
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            command: "conflict".to_string(),
        });

        let req = AppendEntries {
            term: Term::from(2),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::from(1),
            prev_log_term: Term::from(1),
            entries: vec![LogEntry {
                term: Term::from(2),
                command: "new".to_string(),
            }],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert_eq!(n.persistent.log.len(), 2);
        assert_eq!(n.persistent.log[1].command, "new");
        assert_eq!(n.persistent.log[1].term, Term::from(2));
    }

    #[test]
    fn higher_term_converts_to_follower() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        assert!(is_candidate(&n));

        let req = AppendEntries {
            term: Term::from(5),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert!(is_follower(&n));
        assert_eq!(n.persistent.current_term, Term::from(5));
    }

    #[test]
    fn leader_advances_commit_index_on_majority() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );
        assert!(is_leader(&n));

        // Submit a command.
        let index = n.submit_command("cmd".to_string());
        assert_eq!(index, Some(LogIndex::from(1)));

        // Simulate successful replication to one follower.
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse {
                term: Term::from(1),
                success: true,
                match_index: LogIndex::from(1),
            },
        );

        // With 3-node cluster, leader + 1 follower = majority.
        assert_eq!(n.volatile.commit_index, LogIndex::from(1));
    }

    #[test]
    fn leader_decrements_next_index_on_failure() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );
        assert!(is_leader(&n));

        // Add entries to leader's log.
        n.submit_command("a".to_string());
        n.submit_command("b".to_string());

        let Role::Leader(leader) = &n.role else {
            panic!("expected leader");
        };
        let initial_next = leader.next_index_for(NodeId::from(2)).unwrap();

        // Simulate failed replication.
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse {
                term: Term::from(1),
                success: false,
                match_index: LogIndex::default(),
            },
        );

        let Role::Leader(leader) = &n.role else {
            panic!("expected leader");
        };
        let new_next = leader.next_index_for(NodeId::from(2)).unwrap();

        assert!(new_next < initial_next);
    }

    #[test]
    fn submit_command_fails_on_non_leader() {
        let mut n = node(1, &[2, 3]);
        assert!(n.submit_command("cmd".to_string()).is_none());

        n.election_timeout();
        assert!(n.submit_command("cmd".to_string()).is_none());
    }

    #[test]
    fn leader_does_not_commit_entries_from_previous_term() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );

        // Manually add an entry from a previous term (simulating log from old leader).
        n.persistent.log.push(LogEntry {
            term: Term::from(0),
            command: "old".to_string(),
        });

        // Simulate successful replication.
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse {
                term: Term::from(1),
                success: true,
                match_index: LogIndex::from(1),
            },
        );

        // Should not commit because entry is from term 0, not current term 1.
        assert_eq!(n.volatile.commit_index, LogIndex::default());
    }

    #[test]
    fn take_entry_to_apply_returns_committed_entries() {
        let mut n = node(1, &[2, 3]);

        // Add entries to log.
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            command: "a".to_string(),
        });
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            command: "b".to_string(),
        });

        // Commit first entry.
        n.volatile.commit_index = LogIndex::from(1);

        assert!(n.has_pending_applies());
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(1));
        assert_eq!(applied.command, &"a".to_string());
        assert!(!n.has_pending_applies());
        assert!(n.take_entry_to_apply().is_none());

        // Commit second entry.
        n.volatile.commit_index = LogIndex::from(2);

        assert!(n.has_pending_applies());
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(2));
        assert_eq!(applied.command, &"b".to_string());
        assert!(!n.has_pending_applies());
    }

    #[test]
    fn take_entry_to_apply_advances_last_applied() {
        let mut n = node(1, &[2, 3]);

        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            command: "cmd".to_string(),
        });
        n.volatile.commit_index = LogIndex::from(1);

        assert_eq!(n.volatile.last_applied, LogIndex::default());
        n.take_entry_to_apply();
        assert_eq!(n.volatile.last_applied, LogIndex::from(1));
    }

    #[test]
    fn save_and_load_persistent_state() {
        use crate::storage::MemoryStorage;

        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );
        n.submit_command("cmd1".to_string());
        n.submit_command("cmd2".to_string());

        let mut storage = MemoryStorage::new();
        n.save(&mut storage).unwrap();

        let restored: Node<String> =
            Node::from_storage(NodeId::from(1), vec![NodeId::from(2), NodeId::from(3)], &storage)
                .unwrap();

        assert_eq!(
            restored.persistent.current_term,
            n.persistent.current_term
        );
        assert_eq!(restored.persistent.voted_for, n.persistent.voted_for);
        assert_eq!(restored.persistent.log.len(), n.persistent.log.len());
        assert!(is_follower(&restored));
    }
}
