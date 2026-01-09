use crate::command::Command;
use crate::state::{Candidate, Follower, Leader};
use crate::types::{
    AppendEntries, AppendEntriesResponse, LogEntry, LogIndex, Message, NodeId, RequestVote,
    RequestVoteResponse, Term,
};

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

impl<C> Node<C> {
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
            role: Role::Follower(Follower { leader_id: None }),
        }
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex::from_length(self.persistent.log.len())
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

    fn start_election(&mut self) -> Vec<Command<C>> {
        self.persistent.current_term = self.persistent.current_term.increment();
        self.persistent.voted_for = Some(self.id);
        self.role = Role::Candidate(Candidate {
            votes_received: vec![self.id],
        });

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

    /// Handle incoming RequestVote RPC.
    pub fn handle_request_vote(&mut self, from: NodeId, req: RequestVote) -> Vec<Command<C>> {
        // If RPC request contains term > currentTerm, update currentTerm and convert to follower.
        if req.term > self.persistent.current_term {
            self.persistent.current_term = req.term;
            self.persistent.voted_for = None;
            self.role = Role::Follower(Follower { leader_id: None });
        }

        let vote_granted = self.should_grant_vote(&req);

        if vote_granted {
            self.persistent.voted_for = Some(req.candidate_id);
        }

        let response = RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted,
        };

        vec![Command::Send {
            to: from,
            message: Message::RequestVoteResponse(response),
        }]
    }

    fn should_grant_vote(&self, req: &RequestVote) -> bool {
        let term_ok = req.term >= self.persistent.current_term;
        let vote_ok = match self.persistent.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };
        let log_ok = self.is_log_up_to_date(req.last_log_term, req.last_log_index);

        term_ok && vote_ok && log_ok
    }

    fn is_log_up_to_date(&self, candidate_term: Term, candidate_index: LogIndex) -> bool {
        // Compare (term, index) tuples lexicographically.
        (candidate_term, candidate_index) >= (self.last_log_term(), self.last_log_index())
    }

    /// Handle incoming RequestVoteResponse RPC.
    pub fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        resp: RequestVoteResponse,
    ) -> Vec<Command<C>> {
        use std::cmp::Ordering;

        match resp.term.cmp(&self.persistent.current_term) {
            Ordering::Less => return Vec::new(),
            Ordering::Greater => {
                self.step_down(resp.term, None);
                return Vec::new();
            }
            Ordering::Equal => {}
        }

        let Role::Candidate(ref mut candidate) = self.role else {
            return Vec::new();
        };

        if resp.vote_granted {
            candidate.votes_received.push(from);
        }

        let cluster_size = self.peers.len() + 1;
        let majority = cluster_size / 2 + 1;

        if candidate.votes_received.len() >= majority {
            self.become_leader()
        } else {
            Vec::new()
        }
    }

    fn become_leader(&mut self) -> Vec<Command<C>> {
        let last_log_index = self.last_log_index();

        self.role = Role::Leader(Leader {
            next_index: self
                .peers
                .iter()
                .map(|&peer| (peer, last_log_index.next()))
                .collect(),
            match_index: self
                .peers
                .iter()
                .map(|&peer| (peer, LogIndex::default()))
                .collect(),
        });

        // Leader sends initial empty AppendEntries (heartbeats) to establish authority.
        Vec::new()
    }

    /// Handle incoming AppendEntries RPC.
    pub fn handle_append_entries(
        &mut self,
        from: NodeId,
        req: AppendEntries<C>,
    ) -> Vec<Command<C>> {
        // If RPC request contains term > currentTerm, update and convert to follower.
        if req.term > self.persistent.current_term {
            self.persistent.current_term = req.term;
            self.persistent.voted_for = None;
            self.role = Role::Follower(Follower {
                leader_id: Some(req.leader_id),
            });
        }

        // Reply false if term < currentTerm.
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

        // Valid AppendEntries from current leader - reset election timer.
        let mut commands = vec![Command::ResetElectionTimer];

        // Update leader_id if we're a follower.
        if let Role::Follower(ref mut follower) = self.role {
            follower.leader_id = Some(req.leader_id);
        }

        // Check if log contains entry at prevLogIndex with prevLogTerm.
        let log_ok = self.check_log_consistency(req.prev_log_index, req.prev_log_term);

        if !log_ok {
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

        // Append new entries (handling conflicts).
        self.append_entries(req.prev_log_index, req.entries);

        // Update commit index.
        if req.leader_commit > self.volatile.commit_index {
            let last_new_entry = self.last_log_index();
            self.volatile.commit_index = std::cmp::min(req.leader_commit, last_new_entry);
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

    fn check_log_consistency(&self, prev_log_index: LogIndex, prev_log_term: Term) -> bool {
        // If prevLogIndex is 0, consistency check passes (empty log prefix).
        if prev_log_index == LogIndex::default() {
            return prev_log_term == Term::default();
        }

        // Check if we have an entry at prevLogIndex with matching term.
        match prev_log_index.to_array_index() {
            Some(idx) => self
                .persistent
                .log
                .get(idx)
                .map_or(false, |entry| entry.term == prev_log_term),
            None => false,
        }
    }

    fn append_entries(&mut self, prev_log_index: LogIndex, entries: Vec<LogEntry<C>>) {
        let mut insert_index = prev_log_index.next();

        for entry in entries {
            match insert_index.to_array_index() {
                Some(idx) if idx < self.persistent.log.len() => {
                    // Check for conflict.
                    if self.persistent.log[idx].term != entry.term {
                        // Delete existing entry and all that follow.
                        self.persistent.log.truncate(idx);
                        self.persistent.log.push(entry);
                    }
                    // If terms match, entry is already present - skip.
                }
                _ => {
                    // Append new entry.
                    self.persistent.log.push(entry);
                }
            }
            insert_index = insert_index.next();
        }
    }
}
